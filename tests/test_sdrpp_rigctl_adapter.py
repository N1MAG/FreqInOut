"""SDR-2 acceptance tests for the named SDR++ RigCTL receive adapter.

The server is a tiny line-oriented RigCTL double.  It deliberately fragments
responses and supports one connection per request so tests catch adapters that
assume a complete TCP read or retain a stale socket after disconnect.  No RTL-
SDR device or real SDR++ process is opened.
"""

from __future__ import annotations

from contextlib import closing
import socket
import threading
import time
from typing import Callable, Dict, List, Optional

import pytest

from freqinout.core.receiver_control import ManualReceiverControl, ReceiverCommand, ReceiverIdentity
from freqinout.core.sdrpp_rigctl_receiver import receiver_control_client_from_profile
from freqinout.radio_interface.sdrpp_rigctl import SDRPlusPlusRigctlAdapter


class _RigctlServer:
    def __init__(
        self,
        *,
        frequency_hz: int = 7_100_000,
        mode: str = "USB",
        bandwidth_hz: int = 2_400,
        mode_advertised: bool = True,
        response_fragment_size: int = 1,
        disconnect_first: bool = False,
        malformed: bool = False,
        oversized: bool = False,
        stall: bool = False,
    ) -> None:
        self.frequency_hz = int(frequency_hz)
        self.mode = mode
        self.bandwidth_hz = int(bandwidth_hz)
        self.mode_advertised = bool(mode_advertised)
        self.response_fragment_size = max(1, int(response_fragment_size))
        self.disconnect_first = bool(disconnect_first)
        self.malformed = bool(malformed)
        self.oversized = bool(oversized)
        self.stall = bool(stall)
        self.commands: List[str] = []
        self._connections = 0
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._serve, name="test-sdrpp-rigctl", daemon=True)
        self._listener: Optional[socket.socket] = None
        self._thread.start()
        assert self._ready.wait(1.0)

    @property
    def address(self) -> tuple[str, int]:
        listener = self._listener
        assert listener is not None
        host, port = listener.getsockname()[:2]
        return str(host), int(port)

    def _serve(self) -> None:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as listener:
            self._listener = listener
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(8)
            listener.settimeout(0.1)
            self._ready.set()
            while not self._stop.is_set():
                try:
                    conn, _address = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                worker = threading.Thread(target=self._handle, args=(conn,), daemon=True)
                worker.start()

    def _handle(self, conn: socket.socket) -> None:
        with closing(conn):
            self._connections += 1
            try:
                conn.settimeout(1.0)
                raw = conn.recv(1024)
            except (OSError, socket.timeout):
                return
            command = raw.decode("ascii", errors="ignore").strip()
            if not command:
                return
            self.commands.append(command)
            if self.disconnect_first and self._connections == 1:
                return
            if self.stall:
                self._stop.wait(2.0)
                return
            response = self._response(command)
            if response is None:
                return
            payload = response.encode("ascii", errors="ignore")
            if self.oversized:
                payload = (b"9" * 70_000) + b"\n"
            for offset in range(0, len(payload), self.response_fragment_size):
                try:
                    conn.sendall(payload[offset : offset + self.response_fragment_size])
                except OSError:
                    return

    def _response(self, command: str) -> Optional[str]:
        if self.malformed:
            return "not-a-rigctl-response\n"
        if command == "f":
            return f"{self.frequency_hz}\nRPRT 0\n"
        if command == "m":
            if not self.mode_advertised:
                return "RPRT -4\n"
            return f"{self.mode} {self.bandwidth_hz}\nRPRT 0\n"
        if command.startswith("F "):
            try:
                self.frequency_hz = int(command.split()[1])
            except (IndexError, ValueError):
                return "RPRT -1\n"
            return "RPRT 0\n"
        if command == "M ?":
            if not self.mode_advertised:
                return "RPRT -4\n"
            return "FM WFM AM DSB USB CW LSB RAW\n"
        if command.startswith("M "):
            if not self.mode_advertised:
                return "RPRT -4\n"
            parts = command.split()
            self.mode = parts[1]
            if len(parts) > 2:
                try:
                    self.bandwidth_hz = int(parts[2])
                except ValueError:
                    pass
            return "RPRT 0\n"
        if command == "v":
            return "VFOA\nRPRT 0\n"
        return "RPRT -4\n"

    def close(self) -> None:
        self._stop.set()
        listener = self._listener
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        self._thread.join(timeout=1.0)


def _identity(target_id: str = "vfo-a") -> ReceiverIdentity:
    return ReceiverIdentity(
        adapter_id="sdrpp_rigctl",
        receiver_id="sdrpp:rtl-sdr-0",
        display_name="RTL-SDR via SDR++",
        application_name="SDR++",
        hardware_family="RTL-SDR",
        target_id=target_id,
    )


def _adapter(server: _RigctlServer, *, identity: Optional[ReceiverIdentity] = None, timeout_s: float = 0.2):
    host, port = server.address
    return SDRPlusPlusRigctlAdapter(
        host=host,
        port=port,
        identity=identity or _identity(),
        timeout_s=timeout_s,
    )


def test_fragmented_read_set_and_verify_use_selected_target() -> None:
    server = _RigctlServer(response_fragment_size=1)
    adapter = _adapter(server)
    identity = _identity()
    try:
        state = adapter.read_state(identity, deadline=time.monotonic() + 1.0, cancel=lambda: False)
        assert state.frequency_hz == 7_100_000
        applied = adapter.set_receive_frequency(
            identity,
            7_115_000,
            deadline=time.monotonic() + 1.0,
            cancel=lambda: False,
        )
        assert applied.available is True
        verified = adapter.verify_state(
            identity,
            ReceiverCommand(target_id="vfo-a", frequency_hz=7_115_000),
            tolerance_hz=20,
            deadline=time.monotonic() + 1.0,
            cancel=lambda: False,
        )
        assert verified.verified is True
        assert server.frequency_hz == 7_115_000
        assert server.commands == ["f", "F 7115000", "f"]
    finally:
        adapter.close()
        server.close()


def test_mode_is_set_only_when_rigctl_advertises_mode_capability() -> None:
    advertised = _RigctlServer(mode_advertised=True)
    adapter = _adapter(advertised)
    identity = _identity()
    try:
        _identity_from_probe, capabilities = adapter.probe(deadline=time.monotonic() + 1.0, cancel=lambda: False)
        assert capabilities.can_set_receive_mode is True
        state = adapter.set_receive_mode(
            identity,
            "USB",
            2_400,
            deadline=time.monotonic() + 1.0,
            cancel=lambda: False,
        )
        assert state.available is True
        assert "M USB 2400" in advertised.commands
    finally:
        adapter.close()
        advertised.close()

    not_advertised = _RigctlServer(mode_advertised=False)
    adapter = _adapter(not_advertised)
    try:
        _identity_from_probe, capabilities = adapter.probe(deadline=time.monotonic() + 1.0, cancel=lambda: False)
        assert capabilities.can_set_receive_mode is False
        state = adapter.set_receive_mode(
            identity,
            "USB",
            2_400,
            deadline=time.monotonic() + 1.0,
            cancel=lambda: False,
        )
        assert state.manual is True or state.verified is False
        assert not any(
            command.startswith("M ") and command != "M ?"
            for command in not_advertised.commands
        )
    finally:
        adapter.close()
        not_advertised.close()


def test_probe_cancel_and_expired_deadline_do_not_open_socket() -> None:
    server = _RigctlServer()
    adapter = _adapter(server)
    identity = _identity()
    try:
        cancelled = adapter.probe(deadline=time.monotonic() + 1.0, cancel=lambda: True)
        assert cancelled[1].manual_only is True
        assert server.commands == []
        expired = adapter.read_state(identity, deadline=time.monotonic() - 1.0, cancel=lambda: False)
        assert expired.cancelled is True or expired.available is False
        assert server.commands == []
    finally:
        adapter.close()
        server.close()


def test_timeout_malformed_and_oversized_responses_degrade_without_verified_control() -> None:
    for kwargs in (
        {"stall": True},
        {"malformed": True},
        {"oversized": True},
    ):
        server = _RigctlServer(**kwargs)
        adapter = _adapter(server, timeout_s=0.05)
        try:
            identity, capabilities = adapter.probe(deadline=time.monotonic() + 0.5, cancel=lambda: False)
            assert identity == _identity()
            assert capabilities.manual_only is True or capabilities.can_read_state is False
            state = adapter.read_state(_identity(), deadline=time.monotonic() + 0.5, cancel=lambda: False)
            assert state.verified is False
            assert state.available is False or state.manual is True
        finally:
            adapter.close()
            server.close()


def test_disconnect_reconnects_and_close_fences_new_requests() -> None:
    server = _RigctlServer(disconnect_first=True)
    adapter = _adapter(server)
    identity = _identity()
    try:
        first = adapter.read_state(identity, deadline=time.monotonic() + 1.0, cancel=lambda: False)
        second = adapter.read_state(identity, deadline=time.monotonic() + 1.0, cancel=lambda: False)
        assert first.available is False or first.manual is True
        assert second.frequency_hz == 7_100_000
        assert server.commands[:2] == ["f", "f"]
        adapter.close()
        closed = adapter.read_state(identity, deadline=time.monotonic() + 1.0, cancel=lambda: False)
        assert closed.available is False
        assert len(server.commands) == 2
    finally:
        adapter.close()
        server.close()


def test_wrong_target_is_rejected_and_adapter_has_no_ptt_or_transmit_surface() -> None:
    server = _RigctlServer()
    adapter = _adapter(server)
    try:
        wrong = _identity("vfo-b")
        with pytest.raises(ValueError, match="target"):
            adapter.read_state(wrong, deadline=time.monotonic() + 1.0, cancel=lambda: False)
        public = {
            name
            for name, value in __import__("inspect").getmembers(type(adapter), predicate=callable)
            if not name.startswith("_")
        }
        assert not {"ptt", "set_ptt", "transmit", "send", "get_ptt"} & public
        assert not any(command[:1].lower() in {"t", "x"} for command in server.commands)
    finally:
        adapter.close()
        server.close()


def test_profile_factory_is_zero_io_and_incomplete_profiles_fall_back_to_manual() -> None:
    calls: list[object] = []

    def forbidden_socket(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("factory performed endpoint I/O")

    base = {
        "id": 81,
        "name": "RTL-SDR via SDR++",
        "device_class": "observer",
        "radio_model": "RTL-SDR",
        "sdr_application": "SDR++",
        "sdr_adapter": "sdrpp_rigctl",
        "sdr_host": "127.0.0.1",
        "sdr_port": 4532,
        "sdr_target": "selected-vfo",
    }
    client = receiver_control_client_from_profile(base, socket_factory=forbidden_socket)
    assert not isinstance(client, ManualReceiverControl)
    assert calls == []
    client.close()

    for incomplete in (
        {**base, "sdr_host": ""},
        {**base, "sdr_port": None},
        {**base, "sdr_target": ""},
        {**base, "sdr_adapter": "manual"},
        {**base, "device_class": "tx_rx"},
    ):
        fallback = receiver_control_client_from_profile(
            incomplete,
            socket_factory=forbidden_socket,
        )
        assert isinstance(fallback, ManualReceiverControl)
        assert calls == []
