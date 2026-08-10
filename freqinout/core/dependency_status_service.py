from __future__ import annotations

import atexit
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from PySide6.QtCore import Qt, QObject, QCoreApplication, QTimer, Signal, Slot

from freqinout.core.logger import log
from freqinout.core.software_status_service import (
    STATUS_KEYS,
    SoftwareStatusService,
)


LEGACY_PRIMARY_DEPENDENCY_SCOPE = "legacy_primary"
PROCESS_STATUS_CADENCE_SEC = 10.0
PROCESS_STATUS_STALE_AFTER_SEC = 30.0


def _status_program_name(status_key: str) -> str:
    return "JS8Call" if str(status_key or "") == "JS8Call_API" else str(status_key or "")


@dataclass(frozen=True)
class DependencyStatus:
    key: str
    state: str
    value: Optional[str] = None
    checked_at: float = 0.0
    updated_at: float = 0.0
    stale_after_sec: float = PROCESS_STATUS_STALE_AFTER_SEC
    source: str = "process"
    tooltip: str = ""
    running: bool = False
    reachable: Optional[bool] = None
    last_success_at: Optional[float] = None
    last_error_at: Optional[float] = None
    last_error: str = ""
    backoff_until: Optional[float] = None
    next_check_at: Optional[float] = None
    duration_ms: Optional[float] = None
    slow: bool = False
    meta: Mapping[str, Any] = field(default_factory=dict)

    def age_sec(self, *, now: Optional[float] = None) -> Optional[float]:
        if not self.checked_at:
            return None
        return max(0.0, float(now if now is not None else time.time()) - float(self.checked_at))

    def is_fresh_enough(self, max_age_sec: Optional[float] = None, *, now: Optional[float] = None) -> bool:
        state = str(self.state or "").strip().lower()
        if state in {"stale", "unknown", "error"}:
            return False
        age = self.age_sec(now=now)
        if age is None:
            return False
        limit = float(max_age_sec if max_age_sec is not None else self.stale_after_sec)
        return age <= limit

    def to_software_status_dict(self) -> Dict[str, object]:
        out: Dict[str, object] = {
            "state": self.state,
            "tooltip": self.tooltip or ("Running" if self.running else "Not running"),
            "running": bool(self.running),
            "reachable": self.reachable,
            "checked_at": self.checked_at,
            "updated_at": self.updated_at,
            "source": self.source,
            "stale": not self.is_fresh_enough(),
            "slow": bool(self.slow),
        }
        if self.value:
            out["value"] = self.value
        if self.last_error:
            out["last_error"] = self.last_error
        if self.backoff_until:
            out["backoff_until"] = self.backoff_until
        if self.next_check_at:
            out["next_check_at"] = self.next_check_at
        if self.duration_ms is not None:
            out["duration_ms"] = self.duration_ms
        if self.meta:
            out.update(dict(self.meta))
        return out


@dataclass(frozen=True)
class DependencySnapshot:
    generated_at: float
    scope: str = LEGACY_PRIMARY_DEPENDENCY_SCOPE
    station_id: Optional[str] = None
    radio_id: Optional[str] = None
    process: Mapping[str, DependencyStatus] = field(default_factory=dict)
    reason: str = ""
    sequence: int = 0

    def is_fresh_enough(self, max_age_sec: Optional[float] = None, *, now: Optional[float] = None) -> bool:
        if not self.generated_at:
            return False
        age = max(0.0, float(now if now is not None else time.time()) - float(self.generated_at))
        return age <= float(max_age_sec if max_age_sec is not None else PROCESS_STATUS_STALE_AFTER_SEC)

    def to_software_status_snapshot(self) -> Dict[str, Dict[str, object]]:
        out: Dict[str, Dict[str, object]] = {}
        for key in STATUS_KEYS:
            status = self.process.get(key)
            if status is None:
                out[key] = _unknown_status_dict(key, self.generated_at)
                continue
            out[key] = status.to_software_status_dict()
        return out


def _unknown_status_dict(key: str, checked_at: float = 0.0) -> Dict[str, object]:
    return {
        "state": "idle",
        "tooltip": f"{key}: status not checked yet",
        "running": False,
        "reachable": None,
        "checked_at": checked_at,
        "source": "process",
        "stale": True,
    }


def _initial_snapshot() -> DependencySnapshot:
    now = time.time()
    return DependencySnapshot(
        generated_at=now,
        process={
            key: DependencyStatus(
                key=key,
                state="idle",
                checked_at=0.0,
                updated_at=now,
                tooltip=f"{key}: status not checked yet",
            )
            for key in STATUS_KEYS
        },
    )


def _logger_handlers_available() -> bool:
    for handler in getattr(log, "handlers", []) or []:
        stream = getattr(handler, "stream", None)
        if stream is not None and bool(getattr(stream, "closed", False)):
            return False
    return True


class DependencyStatusService(QObject):
    """
    Main-thread owner for shared dependency status snapshots.

    Slow or potentially blocking probes run in a plain executor worker. UI code
    reads the latest complete immutable snapshot and never performs routine
    status polling itself.
    """

    snapshot_changed = Signal(object)
    _snapshot_ready = Signal(object)

    def __init__(self, settings: Any, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self.settings = settings
        self._lock = threading.RLock()
        self._latest_snapshot = _initial_snapshot()
        self._sequence = 0
        self._worker_pending = False
        self._stopped = False
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="fio-dependency-status")
        self._snapshot_ready.connect(self._publish_snapshot, Qt.QueuedConnection)
        self._timer = QTimer(self)
        self._timer.setInterval(int(PROCESS_STATUS_CADENCE_SEC * 1000))
        self._timer.timeout.connect(lambda: self.refresh_now(reason="timer"))
        self._timer.start()
        app = QCoreApplication.instance()
        if app is not None:
            try:
                app.aboutToQuit.connect(self.stop)
            except Exception:
                pass
        QTimer.singleShot(0, lambda: self.refresh_now(reason="startup"))

    def latest_snapshot(self) -> DependencySnapshot:
        with self._lock:
            return self._latest_snapshot

    def software_status_snapshot(self) -> Dict[str, Dict[str, object]]:
        return self.latest_snapshot().to_software_status_snapshot()

    def refresh_now(self, *, reason: str = "manual", force: bool = False) -> DependencySnapshot:
        with self._lock:
            if self._stopped:
                return self._latest_snapshot
            if self._worker_pending and not force:
                return self._latest_snapshot
            self._worker_pending = True
            self._sequence += 1
            sequence = self._sequence
        future = self._executor.submit(self._build_process_snapshot, sequence, str(reason or "manual"))
        future.add_done_callback(self._on_worker_done)
        return self.latest_snapshot()

    @Slot()
    def stop(self) -> None:
        with self._lock:
            self._stopped = True
            self._worker_pending = False
        try:
            self._timer.stop()
        except Exception:
            pass
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            self._executor.shutdown(wait=False)
        except Exception:
            pass

    def _on_worker_done(self, future: Future) -> None:
        with self._lock:
            stopped = self._stopped
        if stopped:
            return
        try:
            snapshot = future.result()
        except Exception as exc:
            log.warning("DEPENDENCY_STATUS|refresh_failed|error=%s", exc)
            with self._lock:
                self._worker_pending = False
            return
        self._snapshot_ready.emit(snapshot)

    @Slot(object)
    def _publish_snapshot(self, snapshot: DependencySnapshot) -> None:
        with self._lock:
            if self._stopped:
                return
            self._latest_snapshot = snapshot
            self._worker_pending = False
        self.snapshot_changed.emit(snapshot)

    def _build_process_snapshot(self, sequence: int, reason: str) -> DependencySnapshot:
        started = time.perf_counter()
        checked_at = time.time()
        probe = SoftwareStatusService(self.settings)
        statuses: Dict[str, DependencyStatus] = {}
        for status_key in STATUS_KEYS:
            program_name = _status_program_name(status_key)
            item_started = time.perf_counter()
            try:
                running = bool(probe.program_is_running(program_name))
                capability: Dict[str, object] = {}
                if status_key == "JS8Call_API":
                    capability = probe.js8_api_capability_status(process_running=running)
                value = self._status_value(status_key, running, capability)
                state = self._status_state(status_key, running, capability)
                tooltip = self._process_tooltip(status_key, program_name, running, probe, capability)
                duration_ms = (time.perf_counter() - item_started) * 1000.0
                statuses[status_key] = DependencyStatus(
                    key=status_key,
                    state=state,
                    value=value,
                    checked_at=checked_at,
                    updated_at=checked_at,
                    stale_after_sec=PROCESS_STATUS_STALE_AFTER_SEC,
                    source="process",
                    tooltip=tooltip,
                    running=running,
                    reachable=None,
                    last_success_at=checked_at,
                    duration_ms=duration_ms,
                    slow=duration_ms > 250.0,
                    meta=self._status_meta(status_key, program_name, capability),
                )
            except Exception as exc:
                duration_ms = (time.perf_counter() - item_started) * 1000.0
                statuses[status_key] = DependencyStatus(
                    key=status_key,
                    state="error",
                    value="unknown",
                    checked_at=checked_at,
                    updated_at=checked_at,
                    stale_after_sec=PROCESS_STATUS_STALE_AFTER_SEC,
                    source="process",
                    tooltip=f"{program_name}: process status check failed",
                    running=False,
                    reachable=None,
                    last_error_at=checked_at,
                    last_error=str(exc or "process status check failed"),
                    duration_ms=duration_ms,
                    slow=duration_ms > 250.0,
                    meta={"program": program_name},
                )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with self._lock:
            stopped = self._stopped
        if elapsed_ms > 250.0 and not stopped and _logger_handlers_available():
            log.info(
                "DEPENDENCY_STATUS|slow_process_snapshot|scope=%s|duration_ms=%.1f|reason=%s",
                LEGACY_PRIMARY_DEPENDENCY_SCOPE,
                elapsed_ms,
                reason,
            )
        return DependencySnapshot(
            generated_at=checked_at,
            scope=LEGACY_PRIMARY_DEPENDENCY_SCOPE,
            station_id=None,
            radio_id=None,
            process=statuses,
            reason=reason,
            sequence=sequence,
        )

    def _process_tooltip(
        self,
        status_key: str,
        program_name: str,
        running: bool,
        probe: SoftwareStatusService,
        capability: Optional[Mapping[str, object]] = None,
    ) -> str:
        if status_key == "JS8Call_API":
            return self._js8_capability_tooltip(capability or {}, running=running)
        if status_key in {"FLRig", "FLDigi"}:
            return (
                f"{program_name} process is running. Routine UI status avoids repeated XML-RPC probes."
                if running
                else f"{program_name} is not running."
            )
        if status_key == "VarAC" and running:
            exe = probe.find_process_exe("VarAC")
            if exe:
                return f"Running: {exe}"
        return "Running" if running else "Not running"

    @staticmethod
    def _status_value(status_key: str, running: bool, capability: Mapping[str, object]) -> str:
        if status_key == "JS8Call_API":
            return str(capability.get("mode", "offline") or "offline")
        return "running" if running else "not_running"

    @staticmethod
    def _status_state(status_key: str, running: bool, capability: Mapping[str, object]) -> str:
        if status_key == "JS8Call_API":
            mode = str(capability.get("mode", "offline") or "offline")
            if mode in {"api_full", "api_basic", "file_fallback"}:
                return "ok"
            return "warn" if running else "idle"
        return "ok" if running else "idle"

    @staticmethod
    def _status_meta(status_key: str, program_name: str, capability: Mapping[str, object]) -> Dict[str, object]:
        meta: Dict[str, object] = {"program": program_name}
        if status_key == "JS8Call_API" and capability:
            meta.update(
                {
                    "capability_mode": str(capability.get("mode", "") or ""),
                    "version": str(capability.get("version", "") or ""),
                    "endpoint": str(capability.get("endpoint", "") or ""),
                    "supported": dict(capability.get("supported", {}) or {}),
                }
            )
        return meta

    @staticmethod
    def _js8_capability_tooltip(capability: Mapping[str, object], *, running: bool) -> str:
        endpoint = str(capability.get("endpoint", "") or "").strip()
        version = str(capability.get("version", "") or "").strip()
        mode = str(capability.get("mode", "offline") or "offline")
        version_part = f" Version: {version}." if version else ""
        if mode == "api_full":
            return f"JS8Call API is ready at {endpoint}.{version_part}"
        if mode == "api_basic":
            return f"JS8Call API is reachable at {endpoint}; FIO will keep compatibility fallbacks available.{version_part}"
        if mode == "file_fallback":
            return f"JS8Call API support is limited at {endpoint}; FIO will use log/database fallbacks.{version_part}"
        if running:
            return f"JS8Call is running, but FIO could not verify the TCP API at {endpoint}."
        return "JS8Call is not running."


_SERVICE_LOCK = threading.Lock()
_SERVICE: Optional[DependencyStatusService] = None


def get_dependency_status_service(settings: Any, parent: Optional[QObject] = None) -> DependencyStatusService:
    global _SERVICE
    with _SERVICE_LOCK:
        if _SERVICE is None or bool(getattr(_SERVICE, "_stopped", False)):
            _SERVICE = DependencyStatusService(settings, parent=None)
        elif settings is not None:
            _SERVICE.settings = settings
        return _SERVICE


def shutdown_dependency_status_service() -> None:
    global _SERVICE
    with _SERVICE_LOCK:
        service = _SERVICE
        _SERVICE = None
    if service is None:
        return
    try:
        service.stop()
    except Exception:
        pass
    try:
        service.deleteLater()
    except Exception:
        pass


atexit.register(shutdown_dependency_status_service)
