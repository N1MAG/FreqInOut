from __future__ import annotations

import socketserver
import threading

from freqinout.radio_interface.rigctl_client import (
    FrequencyCommand,
    RigctldClient,
    rig_control_client_from_settings,
)


class DummySettings:
    def __init__(self, values: dict[str, object] | None = None) -> None:
        self._values = dict(values or {})

    def get(self, key: str, default=None):
        return self._values.get(key, default)


class _RigctldState:
    def __init__(self) -> None:
        self.frequency_hz = 7_100_000
        self.ptt = 0
        self.vfo = "VFOA"
        self.commands: list[str] = []
        self.lock = threading.Lock()


class _RigctldHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        data = b""
        while True:
            chunk = self.request.recv(4096)
            if not chunk:
                break
            data += chunk
        lines = [line.strip() for line in data.decode("utf-8", errors="ignore").splitlines() if line.strip()]
        responses: list[str] = []
        state: _RigctldState = self.server.state  # type: ignore[attr-defined]
        for command in lines:
            with state.lock:
                state.commands.append(command)
                if command == "f":
                    responses.append(str(state.frequency_hz))
                    continue
                if command == "t":
                    responses.append(str(state.ptt))
                    continue
                if command.startswith("F "):
                    state.frequency_hz = int(command.split()[1])
                    responses.append("RPRT 0")
                    continue
                if command.startswith("V "):
                    state.vfo = command.split()[1]
                    responses.append("RPRT 0")
                    continue
                if command.startswith("M "):
                    responses.append("RPRT 0")
                    continue
                responses.append("RPRT -1")
        self.request.sendall(("\n".join(responses) + "\n").encode("utf-8"))


class _RigctldTestServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _RigctldHandler)
        self.state = _RigctldState()
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self.server_address[1])

    def __enter__(self) -> "_RigctldTestServer":
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.shutdown()
        self.server_close()
        self._thread.join(timeout=1.0)


def test_rigctld_client_sets_and_reads_frequency() -> None:
    with _RigctldTestServer() as server:
        client = RigctldClient(host="127.0.0.1", port=server.port, timeout=0.3)

        assert client.is_available() is True
        assert client.get_vfo_frequency() == 7_100_000
        assert client.set_frequency(FrequencyCommand(rig_hz=7_078_000, mode="SSB", band="40M", vfo="A")) is True
        assert client.get_vfo_frequency() == 7_078_000
        assert "V VFOA" in server.state.commands
        assert "M LSB 0" in server.state.commands
        assert "F 7078000" in server.state.commands


def test_rig_control_client_factory_selects_rigctld() -> None:
    client = rig_control_client_from_settings(
        DummySettings(
            {
                "control_via": "RIGCTLD",
                "rig_host": "10.0.0.44",
                "rig_port": 5532,
            }
        )
    )

    assert isinstance(client, RigctldClient)
    assert client.host == "10.0.0.44"
    assert client.port == 5532
