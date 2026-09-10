from __future__ import annotations

import datetime
import faulthandler
import itertools
import json
import os
import platform
import sys
import threading
import time
from pathlib import Path
from typing import Any, Mapping, Optional

from PySide6.QtCore import QObject, QTimer

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


_DIAGNOSTIC_SECRET_KEYS = {
    "password",
    "passphrase",
    "token",
    "secret",
    "credential",
    "credentials",
    "api_key",
    "apikey",
    "access_key",
    "private_key",
    "cookie",
}


def cache_safe_diagnostic_snapshot(
    value: object,
    *,
    max_items: int = 64,
    max_depth: int = 3,
    max_text: int = 240,
) -> dict[str, object]:
    """Copy a bounded diagnostic mapping while removing credential material.

    This helper is intentionally pure: it performs no database, filesystem,
    network, Qt, or process inspection.  It is suitable for snapshots
    published by background services and later consumed by the UI watchdog.
    """

    seen: set[int] = set()

    def clean(item: object, depth: int) -> object:
        if depth > max_depth:
            return "<depth limit>"
        if item is None or isinstance(item, (bool, int, float)):
            return item
        if isinstance(item, str):
            text = item.replace("\x00", "")
            return text[:max(1, int(max_text))]
        identity = id(item)
        if identity in seen:
            return "<cycle>"
        seen.add(identity)
        try:
            if isinstance(item, Mapping):
                output: dict[str, object] = {}
                for raw_key, raw_value in itertools.islice(item.items(), max(1, int(max_items))):
                    key = str(raw_key)[:80]
                    key_norm = key.casefold().replace("-", "_")
                    if any(secret in key_norm for secret in _DIAGNOSTIC_SECRET_KEYS):
                        output[key] = "<redacted>"
                    else:
                        output[key] = clean(raw_value, depth + 1)
                return output
            if isinstance(item, (list, tuple, set, frozenset)):
                return [
                    clean(entry, depth + 1)
                    for entry in itertools.islice(iter(item), max(1, int(max_items)))
                ]
            return str(item)[:max(1, int(max_text))]
        finally:
            seen.discard(identity)

    cleaned = clean(value, 0)
    return cleaned if isinstance(cleaned, dict) else {"value": cleaned}


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
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._diagnostic_snapshot: Optional[dict[str, object]] = None
        self._timer = QTimer(self)
        self._timer.setInterval(self._heartbeat_interval_ms)
        self._timer.timeout.connect(self._beat)

    def publish_diagnostic_snapshot(self, snapshot: Mapping[str, object] | None) -> None:
        """Publish a pure cache snapshot for later watchdog consumption.

        A producer should call this after it has gathered its own state.  The
        watchdog thread then reads this bounded copy rather than invoking a
        potentially expensive service method while writing a hang dump.
        """

        safe = cache_safe_diagnostic_snapshot(snapshot or {})
        with self._lock:
            self._diagnostic_snapshot = safe

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
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
        self._stop_event.set()
        try:
            if self._timer.isActive():
                self._timer.stop()
        except Exception:
            pass
        monitor = self._monitor_thread
        if monitor is not None and monitor is not threading.current_thread():
            monitor.join(timeout=0.25)
        self._monitor_thread = None

    def _beat(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._last_heartbeat = now

    def _diagnostics_for_dump(self) -> dict[str, object]:
        with self._lock:
            cached = dict(self._diagnostic_snapshot or {})
        if cached:
            return cache_safe_diagnostic_snapshot(cached)
        # Hang capture must remain cache-only even before the first publisher
        # tick. Calling a service provider here could reproduce the lock or
        # endpoint wait that the watchdog is trying to diagnose.
        return {"state": "not_published"}

    def _monitor_loop(self) -> None:
        while self._running:
            if self._stop_event.wait(self._check_interval_sec):
                break
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
                diagnostics = self._diagnostics_for_dump()
                handle.write("\nScheduler diagnostics:\n")
                handle.write(json.dumps(diagnostics, indent=2, sort_keys=True, default=str)[:24000])
                handle.write("\n")
                handle.write("\nThread dump:\n")
                handle.flush()
                faulthandler.dump_traceback(file=handle, all_threads=True)
            log.error("UI watchdog detected an event-loop stall; wrote %s", path)
        except Exception as e:
            log.error("UI watchdog failed to write hang dump: %s", e)
