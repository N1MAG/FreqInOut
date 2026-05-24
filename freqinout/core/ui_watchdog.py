from __future__ import annotations

import datetime
import faulthandler
import os
import platform
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QObject, QTimer

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


class UiEventLoopWatchdog(QObject):
    """
    Record a diagnostic thread dump if the Qt event loop stops ticking.

    The heartbeat timer belongs to the UI thread. A daemon monitor thread only
    reads timestamps and writes dumps, so it can still report when the UI thread
    is too busy to write normal performance spans.
    """

    def __init__(
        self,
        parent: Optional[QObject] = None,
        *,
        heartbeat_interval_ms: int = 1000,
        stall_threshold_sec: float = 8.0,
        check_interval_sec: float = 2.0,
        report_cooldown_sec: float = 60.0,
    ) -> None:
        super().__init__(parent)
        self._heartbeat_interval_ms = max(250, int(heartbeat_interval_ms))
        self._stall_threshold_sec = max(2.0, float(stall_threshold_sec))
        self._check_interval_sec = max(0.5, float(check_interval_sec))
        self._report_cooldown_sec = max(5.0, float(report_cooldown_sec))
        self._lock = threading.Lock()
        self._last_heartbeat = time.monotonic()
        self._last_report = 0.0
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._timer = QTimer(self)
        self._timer.setInterval(self._heartbeat_interval_ms)
        self._timer.timeout.connect(self._beat)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._beat()
        self._timer.start()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="freqinout-ui-watchdog",
            daemon=True,
        )
        self._monitor_thread.start()
        log.info(
            "UI watchdog started (threshold=%.1fs, heartbeat=%dms)",
            self._stall_threshold_sec,
            self._heartbeat_interval_ms,
        )

    def stop(self) -> None:
        self._running = False
        try:
            if self._timer.isActive():
                self._timer.stop()
        except Exception:
            pass

    def _beat(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._last_heartbeat = now

    def _monitor_loop(self) -> None:
        while self._running:
            time.sleep(self._check_interval_sec)
            now = time.monotonic()
            with self._lock:
                last_heartbeat = self._last_heartbeat
                last_report = self._last_report
            stale_for = now - last_heartbeat
            if stale_for < self._stall_threshold_sec:
                continue
            if (now - last_report) < self._report_cooldown_sec:
                continue
            with self._lock:
                self._last_report = now
            self._write_hang_dump(stale_for)

    def _write_hang_dump(self, stale_for: float) -> None:
        try:
            dump_dir = Path(get_config_dir()) / "ui_hang_dumps"
            dump_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%SZ")
            path = dump_dir / f"fio_ui_hang_{stamp}_{os.getpid()}.txt"
            with path.open("w", encoding="utf-8") as handle:
                handle.write("FreqInOut UI hang watchdog report\n")
                handle.write(f"UTC: {stamp}\n")
                handle.write(f"PID: {os.getpid()}\n")
                handle.write(f"Python: {sys.version.replace(chr(10), ' ')}\n")
                handle.write(f"Platform: {platform.platform()}\n")
                handle.write(f"UI heartbeat stale for: {stale_for:.3f} seconds\n")
                handle.write("\nThread dump:\n")
                handle.flush()
                faulthandler.dump_traceback(file=handle, all_threads=True)
            log.error("UI watchdog detected an event-loop stall; wrote %s", path)
        except Exception as e:
            log.error("UI watchdog failed to write hang dump: %s", e)
