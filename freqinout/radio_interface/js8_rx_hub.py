from __future__ import annotations

import time
import queue
from pathlib import Path
from typing import Callable, List, Optional

from PySide6.QtCore import QObject, QTimer, QCoreApplication

try:
    import psutil
except Exception:  # pragma: no cover - optional dependency
    psutil = None

JS8NET_PATH = Path(__file__).resolve().parents[2] / "third_party" / "js8net" / "js8net-main"
if JS8NET_PATH.exists():
    import sys

    sys.path.insert(0, str(JS8NET_PATH))
try:
    import js8net  # type: ignore
except Exception:  # pragma: no cover
    js8net = None


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
            try:
                js8net.start_net(self._host, self._port)
                self._net_started = True
            except Exception:
                self._net_started = False
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
        if psutil is None:
            return True
        try:
            for proc in psutil.process_iter(attrs=["name", "exe"]):
                try:
                    name = (proc.info.get("name") or "").lower()
                    exe = (proc.info.get("exe") or "").lower()
                    if "js8call" in name or "js8call" in exe:
                        return True
                except Exception:
                    continue
        except Exception:
            return True
        return False

    def _poll_queue(self) -> None:
        if js8net is None or not hasattr(js8net, "rx_queue"):
            return
        messages: List[dict] = []
        lock = getattr(js8net, "rx_lock", None)
        try:
            if lock:
                lock.acquire()
            while True:
                try:
                    msg = js8net.rx_queue.get_nowait()  # type: ignore[attr-defined]
                except queue.Empty:
                    break
                if isinstance(msg, dict):
                    messages.append(msg)
                if len(messages) >= self._max_msgs:
                    break
        finally:
            if lock:
                lock.release()
        if not messages:
            return
        for cb in list(self._listeners):
            try:
                cb(messages)
            except Exception:
                continue
