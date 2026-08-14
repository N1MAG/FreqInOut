from __future__ import annotations

import json
import socket
import threading
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

import pytest
from PySide6.QtCore import QCoreApplication

from freqinout.radio_interface.js8_api_client import (
    JS8ApiClient,
    JS8ApiClientRegistry,
    JS8ApiConnectionError,
    JS8ApiEndpoint,
    JS8ApiError,
    JS8ApiMessage,
    classify_js8_capability_mode,
)
from freqinout.radio_interface.js8_rx_hub import JS8RxHub
from freqinout.radio_interface.js8_status import JS8ControlClient


class _FakeJs8Server:
    def __init__(self, responses: Mapping[str, Any], *, greeting: Optional[Mapping[str, Any]] = None) -> None:
        self.responses = dict(responses)
        self.greeting = dict(greeting) if greeting else None
        self.received: List[Dict[str, Any]] = []
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self.host, self.port = self._sock.getsockname()
        self._sock.listen(1)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    @property
    def endpoint(self) -> JS8ApiEndpoint:
        return JS8ApiEndpoint(self.host, int(self.port))

    def stop(self) -> None:
        self._stop.set()
        try:
            with socket.create_connection((self.host, self.port), timeout=0.2):
                pass
        except Exception:
            pass
        try:
            self._sock.close()
        except Exception:
            pass
        self._thread.join(timeout=1.0)

    def _run(self) -> None:
        try:
            conn, _addr = self._sock.accept()
        except Exception:
            return
        with conn:
            conn.settimeout(0.2)
            if self.greeting:
                self._send(conn, self.greeting)
            buffer = b""
            while not self._stop.is_set():
                try:
                    chunk = conn.recv(65536)
                except socket.timeout:
                    continue
                except Exception:
                    break
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    raw, buffer = buffer.split(b"\n", 1)
                    if not raw.strip():
                        continue
                    try:
                        message = json.loads(raw.decode("utf-8"))
                    except Exception:
                        continue
                    self.received.append(message)
                    command = str(message.get("type") or "")
                    response = self.responses.get(command)
                    if response is None:
                        continue
                    if callable(response):
                        response = response(message)
                    self._send(conn, response)

    def _send(self, conn: socket.socket, payload: Mapping[str, Any]) -> None:
        conn.sendall(json.dumps(dict(payload), separators=(",", ":")).encode("utf-8") + b"\n")


def _response(response_type: str, request: Mapping[str, Any], params: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    out_params = dict(params or {})
    req_params = request.get("params") if isinstance(request.get("params"), dict) else {}
    if "_ID" in req_params:
        out_params["_ID"] = req_params["_ID"]
    return {"type": response_type, "value": "", "params": out_params}


def test_message_parser_tolerates_missing_type_and_params() -> None:
    message = JS8ApiMessage.from_raw('{"time":1780681053.019983}')

    assert message.type == ""
    assert message.value == ""
    assert message.params == {}
    assert message.malformed is False


def test_message_parser_quarantines_bad_json() -> None:
    message = JS8ApiMessage.from_raw("{not-json")

    assert message.malformed is True
    assert message.quarantine_reason.startswith("json_parse_error")


def test_registry_returns_one_client_per_endpoint() -> None:
    JS8ApiClientRegistry.shutdown_all()
    endpoint = JS8ApiEndpoint("127.0.0.1", 2442)

    first = JS8ApiClientRegistry.get(endpoint, auto_reconnect=False)
    second = JS8ApiClientRegistry.get(endpoint, auto_reconnect=False)

    try:
        assert first is second
    finally:
        JS8ApiClientRegistry.shutdown_all()


def test_registry_status_snapshot_reports_managed_endpoints() -> None:
    JS8ApiClientRegistry.shutdown_all()
    endpoint = JS8ApiEndpoint("127.0.0.1", 2442)

    JS8ApiClientRegistry.get(endpoint, auto_reconnect=False)
    snapshots = JS8ApiClientRegistry.status_snapshot()
    rows = JS8ApiClientRegistry.status_dicts()

    try:
        assert len(snapshots) == 1
        assert snapshots[0].endpoint == endpoint
        assert snapshots[0].key == endpoint.key
        assert snapshots[0].running is False
        assert snapshots[0].connected is False
        assert snapshots[0].listener_count == 0
        assert snapshots[0].pending_request_count == 0
        assert rows == [snapshots[0].as_dict()]
        assert rows[0]["key"] == "127.0.0.1:2442"
    finally:
        JS8ApiClientRegistry.shutdown_all()


def test_registry_can_remove_one_endpoint() -> None:
    JS8ApiClientRegistry.shutdown_all()
    endpoint = JS8ApiEndpoint("127.0.0.1", 2442)

    first = JS8ApiClientRegistry.get(endpoint, auto_reconnect=False)
    JS8ApiClientRegistry.remove(endpoint)
    second = JS8ApiClientRegistry.get(endpoint, auto_reconnect=False)

    try:
        assert first is not second
    finally:
        JS8ApiClientRegistry.shutdown_all()


def test_rx_hub_can_start_two_native_api_endpoints() -> None:
    app = QCoreApplication.instance() or QCoreApplication([])
    server_a = _FakeJs8Server(
        {},
        greeting={"type": "RIG.PTT", "value": "", "params": {"PTT": False}},
    )
    server_b = _FakeJs8Server(
        {},
        greeting={"type": "RX.ACTIVITY", "value": "B", "params": {"FROM": "N0BBB"}},
    )
    seen_a: List[dict] = []
    seen_b: List[dict] = []
    hub_a = JS8RxHub.instance(server_a.host, server_a.port)
    hub_b = JS8RxHub.instance(server_b.host, server_b.port)
    try:
        hub_a.register_listener(lambda messages: seen_a.extend(messages))
        hub_b.register_listener(lambda messages: seen_b.extend(messages))

        assert hub_a.start(server_a.host, server_a.port) is True
        assert hub_b.start(server_b.host, server_b.port) is True

        deadline = time.time() + 1.0
        while time.time() < deadline and (not seen_a or not seen_b):
            hub_a._poll_queue()
            hub_b._poll_queue()
            app.processEvents()
            time.sleep(0.02)

        assert [msg["type"] for msg in seen_a] == ["RIG.PTT"]
        assert [msg["value"] for msg in seen_b] == ["B"]
        assert hub_a.ptt_active() is False
    finally:
        JS8RxHub.shutdown_all()
        JS8ApiClientRegistry.shutdown_all()
        server_a.stop()
        server_b.stop()


def test_native_client_request_matches_response_by_id() -> None:
    server = _FakeJs8Server(
        {
            "RIG.GET_FREQ": lambda request: _response(
                "RIG.FREQ",
                request,
                {"DIAL": 7078000, "FREQ": 7079950, "OFFSET": 1950},
            )
        }
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        assert client.start() is True
        response = client.request("RIG.GET_FREQ", expect_types=("RIG.FREQ",))

        assert response.type == "RIG.FREQ"
        assert response.params["DIAL"] == 7078000
        assert server.received[0]["type"] == "RIG.GET_FREQ"
        assert "_ID" in server.received[0]["params"]
    finally:
        client.stop()
        server.stop()


def test_js8_control_client_uses_native_endpoint_for_frequency_control() -> None:
    server = _FakeJs8Server(
        {
            "RIG.GET_FREQ": lambda request: _response(
                "RIG.FREQ",
                request,
                {"DIAL": 7078000, "FREQ": 7079950, "OFFSET": 1950},
            ),
        }
    )
    client = JS8ControlClient(host=server.host, port=server.port)
    try:
        assert client.set_frequency(7078000, offset_hz=1950) is True
        assert client.get_frequency() == 7078000
        assert client.get_offset() == 1950

        deadline = time.time() + 1.0
        while not any(row["type"] == "RIG.SET_FREQ" for row in server.received) and time.time() < deadline:
            time.sleep(0.02)
        set_rows = [row for row in server.received if row["type"] == "RIG.SET_FREQ"]
        assert set_rows
        assert set_rows[-1]["params"]["DIAL"] == 7078000
        assert set_rows[-1]["params"]["OFFSET"] == 1950
    finally:
        JS8ApiClientRegistry.shutdown_all()
        server.stop()


def test_native_client_surfaces_api_error() -> None:
    server = _FakeJs8Server(
        {
            "STATION.VERSION": lambda request: _response(
                "API.ERROR",
                request,
                {},
            )
            | {"value": "Connections Full"}
        }
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        assert client.start() is True
        with pytest.raises(JS8ApiError, match="Connections Full"):
            client.request("STATION.VERSION", expect_types=("STATION.VERSION",))
        assert "Connections Full" in client.last_error
    finally:
        client.stop()
        server.stop()


def test_native_client_collects_station_closing_event() -> None:
    server = _FakeJs8Server(
        {},
        greeting={
            "type": "STATION.CLOSING",
            "value": "",
            "params": {"_ID": -1, "REASON": "User closed application"},
        },
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        client.start()
        deadline = time.time() + 1.0
        event = None
        while time.time() < deadline:
            event = client.get_event_nowait()
            if event is not None:
                break
            time.sleep(0.02)

        assert event is not None
        assert event.type == "STATION.CLOSING"
        assert client.last_closing_reason == "User closed application"
        deadline = time.time() + 1.0
        while client.is_connected and time.time() < deadline:
            time.sleep(0.02)
        assert client.is_connected is False
    finally:
        client.stop()
        server.stop()


def test_native_client_station_closing_drains_pending_request() -> None:
    server = _FakeJs8Server(
        {
            "RIG.GET_FREQ": {
                "type": "STATION.CLOSING",
                "value": "",
                "params": {"REASON": "User closed application"},
            }
        }
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=2.0)
    try:
        assert client.start() is True
        started = time.time()
        with pytest.raises(JS8ApiConnectionError, match="STATION.CLOSING"):
            client.request("RIG.GET_FREQ", expect_types=("RIG.FREQ",), timeout_s=2.0)

        assert time.time() - started < 1.0
        assert client.is_connected is False
    finally:
        client.stop()
        server.stop()


def test_native_client_stop_drains_pending_request_as_connection_error() -> None:
    def slow_response(request: Mapping[str, Any]) -> Dict[str, Any]:
        time.sleep(2.0)
        return _response("RIG.FREQ", request, {"DIAL": 7078000, "FREQ": 7079950, "OFFSET": 1950})

    server = _FakeJs8Server({"RIG.GET_FREQ": slow_response})
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=2.0)
    errors: List[BaseException] = []

    def run_request() -> None:
        try:
            client.request("RIG.GET_FREQ", expect_types=("RIG.FREQ",), timeout_s=2.0)
        except BaseException as exc:
            errors.append(exc)

    try:
        assert client.start() is True
        thread = threading.Thread(target=run_request, daemon=True)
        thread.start()
        deadline = time.time() + 1.0
        while not server.received and time.time() < deadline:
            time.sleep(0.02)

        client.stop()
        thread.join(timeout=1.0)

        assert errors
        assert isinstance(errors[0], JS8ApiConnectionError)
        assert "client stopped" in str(errors[0])
    finally:
        client.stop()
        server.stop()


def test_native_client_request_fails_fast_when_not_connected() -> None:
    client = JS8ApiClient(JS8ApiEndpoint("127.0.0.1", 9), auto_reconnect=False, timeout_s=0.1)

    with pytest.raises(JS8ApiConnectionError, match="not connected"):
        client.request("RIG.GET_FREQ", expect_types=("RIG.FREQ",))


def test_native_client_send_one_way_command() -> None:
    server = _FakeJs8Server({})
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        assert client.start() is True
        client.send("TX.SET_TEXT", value="")

        deadline = time.time() + 1.0
        while not server.received and time.time() < deadline:
            time.sleep(0.02)

        assert server.received
        assert server.received[0]["type"] == "TX.SET_TEXT"
        assert "_ID" not in server.received[0].get("params", {})
    finally:
        client.stop()
        server.stop()


def test_probe_capabilities_classifies_full_api() -> None:
    def version_response(request: Mapping[str, Any]) -> Dict[str, Any]:
        return _response("STATION.VERSION", request, {"VERSION": "3.0.2"})

    server = _FakeJs8Server(
        {
            "RIG.GET_FREQ": lambda request: _response("RIG.FREQ", request, {"DIAL": 7078000, "FREQ": 7079950, "OFFSET": 1950}),
            "RIG.GET_PTT": lambda request: _response("RIG.PTT_STATUS", request, {"PTT": False, "MESSAGE": ""}),
            "TX.GET_QUEUE_DEPTH": lambda request: _response("TX.QUEUE_DEPTH", request, {"DEPTH": 0}),
            "STATION.VERSION": version_response,
            "STATION.GET_CONFIG": lambda request: _response("STATION.CONFIG", request, {"TX_ENABLED": True}),
            "RX.GET_CALL_ACTIVITY": lambda request: _response("RX.CALL_ACTIVITY", request, {}),
            "RX.GET_BAND_ACTIVITY": lambda request: _response("RX.BAND_ACTIVITY", request, {}),
            "MODE.GET_SPEED": lambda request: _response("MODE.SPEED", request, {"SPEED": 0}),
            "RX.GET_FREE_OFFSETS": lambda request: _response("RX.FREE_OFFSETS", request, {"FREE": [], "LOW": 500, "HIGH": 2500}),
        }
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        assert client.start() is True
        snapshot = client.probe_capabilities(timeout_s=0.5)

        assert snapshot.mode == "api_full"
        assert snapshot.version == "3.0.2"
        assert snapshot.supports("RIG.GET_PTT") is True
        assert snapshot.supports("TX.GET_QUEUE_DEPTH") is True
    finally:
        client.stop()
        server.stop()


def test_probe_capabilities_classifies_missing_full_command_as_basic() -> None:
    server = _FakeJs8Server(
        {
            "RIG.GET_FREQ": lambda request: _response("RIG.FREQ", request, {"DIAL": 7078000, "FREQ": 7079950, "OFFSET": 1950}),
            "STATION.VERSION": lambda request: _response("STATION.VERSION", request, {"VERSION": "2.2.0"}),
            "RX.GET_CALL_ACTIVITY": lambda request: _response("RX.CALL_ACTIVITY", request, {}),
            "MODE.GET_SPEED": lambda request: _response("MODE.SPEED", request, {"SPEED": 0}),
        }
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        assert client.start() is True
        snapshot = client.probe_capabilities(timeout_s=0.2)

        assert snapshot.mode == "api_basic"
        assert snapshot.supports("RIG.GET_PTT") is False
        assert snapshot.supports("RIG.GET_FREQ") is True
    finally:
        client.stop()
        server.stop()


def test_capability_classifier_supports_basic_and_fallback_modes() -> None:
    assert (
        classify_js8_capability_mode(
            {"RIG.GET_FREQ": True, "RX.GET_CALL_ACTIVITY": True, "MODE.GET_SPEED": True},
            connected=True,
        )
        == "api_basic"
    )
    assert classify_js8_capability_mode({"RIG.GET_FREQ": True}, connected=True) == "file_fallback"
    assert classify_js8_capability_mode({}, connected=False) == "offline"
