from __future__ import annotations

import json
import socket
import threading
import time
from typing import Any, Dict, List, Mapping, Optional

from freqinout.core.js8_send_service import (
    js8_endpoint_from_radio_profile,
    preflight_js8_send,
    send_js8_message_guarded,
)
from freqinout.radio_interface.js8_api_client import JS8ApiClient, JS8ApiEndpoint


class _FakeJs8Server:
    def __init__(self, responses: Mapping[str, Any]) -> None:
        self.responses = dict(responses)
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
                    message = json.loads(raw.decode("utf-8"))
                    self.received.append(message)
                    response = self.responses.get(str(message.get("type") or ""))
                    if response is None:
                        continue
                    if callable(response):
                        response = response(message)
                    conn.sendall(json.dumps(dict(response), separators=(",", ":")).encode("utf-8") + b"\n")


def _response(response_type: str, request: Mapping[str, Any], params: Optional[Mapping[str, Any]] = None, value: str = "") -> Dict[str, Any]:
    out_params = dict(params or {})
    req_params = request.get("params") if isinstance(request.get("params"), dict) else {}
    if "_ID" in req_params:
        out_params["_ID"] = req_params["_ID"]
    return {"type": response_type, "value": value, "params": out_params}


def _safe_server(**overrides: Any) -> _FakeJs8Server:
    responses: Dict[str, Any] = {
        "STATION.GET_CONFIG": lambda req: _response("STATION.CONFIG", req, {"TX_ENABLED": True}),
        "TX.GET_QUEUE_DEPTH": lambda req: _response("TX.QUEUE_DEPTH", req, {"DEPTH": 0}),
        "TX.GET_TEXT": lambda req: _response("TX.TEXT", req, value=""),
        "RX.GET_CALL_SELECTED": lambda req: _response("RX.CALL_SELECTED", req, value=""),
    }
    responses.update(overrides)
    return _FakeJs8Server(responses)


def test_preflight_blocks_selected_js8_target() -> None:
    server = _safe_server(
        **{"RX.GET_CALL_SELECTED": lambda req: _response("RX.CALL_SELECTED", req, value="@MAGNET")}
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        result = preflight_js8_send(client, "@MAGNET F!103 ABC", timeout_s=0.4)

        assert result.ok is False
        assert result.selected_call == "@MAGNET"
        assert [issue.code for issue in result.issues] == ["selected_target_present"]
    finally:
        client.stop()
        server.stop()


def test_preflight_requires_confirmation_when_target_state_cannot_be_verified() -> None:
    server = _safe_server()
    server.responses.pop("RX.GET_CALL_SELECTED")
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        blocked = preflight_js8_send(client, "@MAGNET F!103 ABC", timeout_s=0.1)
        confirmed = preflight_js8_send(
            client,
            "@MAGNET F!103 ABC",
            timeout_s=0.1,
            allow_uncertain_target_state=True,
        )

        assert blocked.ok is False
        assert "target_state_unknown" in [issue.code for issue in blocked.issues]
        assert confirmed.ok is True
        assert confirmed.needs_confirmation is True
    finally:
        client.stop()
        server.stop()


def test_guarded_send_clears_tx_text_then_sends_message() -> None:
    server = _safe_server()
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        result = send_js8_message_guarded(client, "@MAGNET F!103 ABC", timeout_s=0.4)

        deadline = time.time() + 1.0
        while len(server.received) < 6 and time.time() < deadline:
            time.sleep(0.02)

        assert result.sent is True
        assert [row["type"] for row in server.received[-2:]] == ["TX.SET_TEXT", "TX.SEND_MESSAGE"]
        assert server.received[-1]["value"] == "@MAGNET F!103 ABC"
    finally:
        client.stop()
        server.stop()


def test_guarded_send_clears_selected_js8_target_before_preflight() -> None:
    selected = {"value": "@OLD"}

    def selected_response(req: Mapping[str, Any]) -> Dict[str, Any]:
        return _response("RX.CALL_SELECTED", req, value=selected["value"])

    def set_selected(req: Mapping[str, Any]) -> None:
        selected["value"] = str(req.get("value") or "")
        return _response("RX.CALL_SELECTED", req, value=selected["value"])

    server = _safe_server(
        **{
            "RX.GET_CALL_SELECTED": selected_response,
            "RX.SET_SELECTED_CALL": set_selected,
            "TX.SET_SELECTED_CALL": set_selected,
            "STATION.SET_SELECTED_CALL": set_selected,
        }
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        result = send_js8_message_guarded(client, "@MAGNET F!103 ABC", timeout_s=0.4, clear_selected_target=True)

        deadline = time.time() + 1.0
        while "TX.SEND_MESSAGE" not in [row["type"] for row in server.received] and time.time() < deadline:
            time.sleep(0.02)

        assert result.sent is True
        assert selected["value"] == ""
        request_types = [row["type"] for row in server.received]
        assert "RX.SET_SELECTED_CALL" in request_types
        assert "TX.SEND_MESSAGE" in request_types
        assert request_types.index("RX.SET_SELECTED_CALL") < request_types.index("TX.SEND_MESSAGE")
    finally:
        client.stop()
        server.stop()


def test_guarded_send_does_not_transmit_when_queue_not_empty() -> None:
    server = _safe_server(
        **{"TX.GET_QUEUE_DEPTH": lambda req: _response("TX.QUEUE_DEPTH", req, {"DEPTH": 2})}
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        result = send_js8_message_guarded(client, "@MAGNET F!103 ABC", timeout_s=0.4)

        assert result.sent is False
        assert "tx_queue_not_empty" in [issue.code for issue in result.preflight.issues]
        assert "TX.SEND_MESSAGE" not in [row["type"] for row in server.received]
    finally:
        client.stop()
        server.stop()


def test_endpoint_from_radio_profile_prefers_profile_values() -> None:
    class _Settings:
        def get(self, key: str, default: object = "") -> object:
            return {"js8_host": "10.0.0.1", "js8_port": 2442}.get(key, default)

    endpoint = js8_endpoint_from_radio_profile({"js8_host": "127.0.0.2", "js8_port": "2444"}, fallback_settings=_Settings())

    assert endpoint.host == "127.0.0.2"
    assert endpoint.port == 2444
