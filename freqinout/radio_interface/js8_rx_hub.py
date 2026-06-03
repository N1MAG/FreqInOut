from __future__ import annotations

import time
import queue
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional

from PySide6.QtCore import QObject, QTimer, QCoreApplication

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService

JS8NET_PATH = Path(__file__).resolve().parents[2] / "third_party" / "js8net" / "js8net-main"
if JS8NET_PATH.exists():
    import sys

    sys.path.insert(0, str(JS8NET_PATH))
try:
    import js8net  # type: ignore
except Exception:  # pragma: no cover
    js8net = None


_JS8_HUB_TEXT_LIMIT = 8192
_JS8_HUB_FIELD_LIMIT = 256
_JS8NET_START_LOCK = threading.Lock()
_JS8NET_STARTED_ENDPOINT: Optional[tuple[str, int]] = None


def ensure_js8net_started(host: str, port: int) -> bool:
    """Start the process-global js8net connection at most once."""
    global _JS8NET_STARTED_ENDPOINT
    if js8net is None:
        return False
    endpoint = (str(host or "127.0.0.1").strip() or "127.0.0.1", int(port))
    with _JS8NET_START_LOCK:
        if _JS8NET_STARTED_ENDPOINT == endpoint:
            return True
        if _JS8NET_STARTED_ENDPOINT is not None:
            return False
        try:
            js8net.start_net(*endpoint)
        except Exception:
            return False
        _JS8NET_STARTED_ENDPOINT = endpoint
        return True


def _safe_js8_hub_text(value: object, *, limit: int = _JS8_HUB_TEXT_LIMIT) -> str:
    try:
        if value is None:
            text = ""
        elif isinstance(value, bytes):
            text = value.decode("utf-8", errors="replace")
        elif isinstance(value, bytearray):
            text = bytes(value).decode("utf-8", errors="replace")
        else:
            text = str(value)
    except Exception:
        text = ""
    if "\x00" in text:
        text = text.replace("\x00", "")
    text = "".join(ch if ch in "\t\n\r" or ord(ch) >= 32 else " " for ch in text).strip()
    if limit > 0 and len(text) > limit:
        return text[:limit]
    return text


def _safe_js8_hub_params(params: object) -> Dict[str, str]:
    if not isinstance(params, dict):
        return {}
    out: Dict[str, str] = {}
    for key, value in list(params.items())[:64]:
        key_txt = _safe_js8_hub_text(key, limit=_JS8_HUB_FIELD_LIMIT)
        if not key_txt:
            continue
        out[key_txt] = _safe_js8_hub_text(value)
    return out


def _safe_js8_hub_message(msg: object) -> Optional[dict]:
    if not isinstance(msg, dict):
        return None
    out = {
        "type": _safe_js8_hub_text(msg.get("type"), limit=_JS8_HUB_FIELD_LIMIT),
        "value": _safe_js8_hub_text(msg.get("value")),
        "params": _safe_js8_hub_params(msg.get("params")),
    }
    if "time" in msg:
        out["time"] = _safe_js8_hub_text(msg.get("time"), limit=_JS8_HUB_FIELD_LIMIT)
    if not out["type"] and not out["params"] and not out["value"]:
        return None
    return out


class JS8RxHub(QObject):
    """
    Single-consumer hub for js8net.rx_queue with listener fan-out.

    This avoids multiple tabs draining the same queue.
    """

    _instance: Optional["JS8RxHub"] = None

    def __init__(self) -> None:
        app = QCoreApplication.instance()
        super().__init__(app)
        self._listeners: List[Callable[[List[dict]], None]] = []
        self._timer = QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self._poll_queue)
        self._max_msgs = 200
        self._net_started = False
        self._host = "127.0.0.1"
        self._port = 2442
        self._last_rx_activity_ts: float = 0.0
        self._last_ptt_ts: float = 0.0
        self._ptt_active: bool = False
        self._software_status = SoftwareStatusService(SettingsManager())

    @classmethod
    def instance(cls) -> "JS8RxHub":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def is_active(self) -> bool:
        return self._timer.isActive()

    def register_listener(self, cb: Callable[[List[dict]], None]) -> None:
        if cb not in self._listeners:
            self._listeners.append(cb)

    def unregister_listener(self, cb: Callable[[List[dict]], None]) -> None:
        if cb in self._listeners:
            self._listeners.remove(cb)
        if not self._listeners and self._timer.isActive():
            self._timer.stop()

    def start(self, host: str, port: int) -> bool:
        if js8net is None:
            return False
        self._host = host
        self._port = int(port)
        if not self._net_started:
            if not self._js8call_running():
                return False
            self._net_started = ensure_js8net_started(self._host, self._port)
            if not self._net_started:
                return False
        if not self._timer.isActive():
            self._timer.start()
        return True

    def shutdown(self) -> None:
        try:
            if self._timer.isActive():
                self._timer.stop()
        except Exception:
            pass
        self._listeners.clear()
        try:
            self.deleteLater()
        except Exception:
            pass
        type(self)._instance = None

    def _js8call_running(self) -> bool:
        try:
            return bool(self._software_status.program_is_running("JS8Call"))
        except Exception:
            return True

    def _poll_queue(self) -> None:
        if js8net is None or not hasattr(js8net, "rx_queue"):
            return
        messages: List[dict] = []
        lock = getattr(js8net, "rx_lock", None)
        acquired = False
        try:
            if lock:
                lock.acquire()
                acquired = True
            while True:
                try:
                    msg = js8net.rx_queue.get_nowait()  # type: ignore[attr-defined]
                except queue.Empty:
                    break
                except Exception:
                    break
                safe_msg = _safe_js8_hub_message(msg)
                if safe_msg is not None:
                    messages.append(safe_msg)
                if len(messages) >= self._max_msgs:
                    break
        finally:
            if lock and acquired:
                try:
                    lock.release()
                except Exception:
                    pass
        if not messages:
            return
        now_ts = time.time()
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            mtype = str(msg.get("type") or "").upper()
            if mtype == "RX.ACTIVITY":
                self._last_rx_activity_ts = now_ts
            elif mtype == "RIG.PTT":
                self._last_ptt_ts = now_ts
                params = msg.get("params") or {}
                if isinstance(params, dict) and "PTT" in params:
                    self._ptt_active = bool(params.get("PTT"))
                else:
                    self._ptt_active = str(msg.get("value") or "").lower() == "on"
        for cb in list(self._listeners):
            try:
                cb(messages)
            except Exception:
                continue

    def last_rx_activity_ts(self) -> float:
        return self._last_rx_activity_ts

    def last_ptt_ts(self) -> float:
        return self._last_ptt_ts

    def ptt_active(self) -> bool:
        return self._ptt_active
