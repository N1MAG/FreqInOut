from __future__ import annotations

from typing import Any, Dict, Optional

from PySide6.QtCore import QObject, QThread, Signal, Slot

from freqinout.core.logger import log
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import StationRuntimeManager


class _SettingsSnapshot:
    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = dict(data or {})

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


def _snapshot_settings_payload(settings: object) -> object:
    try:
        if hasattr(settings, "all"):
            data = settings.all()
            if isinstance(data, dict):
                return dict(data)
    except Exception:
        pass
    return settings


class _ControlStatusWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)

    @Slot(object)
    def run_snapshot(self, request: object) -> None:
        try:
            payload = dict(request or {})
            settings_payload = payload.pop("settings", None)
            settings = _SettingsSnapshot(settings_payload) if isinstance(settings_payload, dict) else settings_payload
            service = SoftwareStatusService(settings)
            snapshot = service.status_snapshot(**payload)
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.finished.emit(snapshot)


class _StationSnapshotWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)

    @Slot(object)
    def run_snapshot(self, request: object) -> None:
        try:
            payload = dict(request or {})
            settings_payload = payload.get("settings")
            settings = _SettingsSnapshot(settings_payload) if isinstance(settings_payload, dict) else settings_payload
            store = payload.get("store")
            force = bool(payload.get("force", False))
            manager = StationRuntimeManager(store=store, settings=settings)
            manager.sync_with_store()
            snapshots = manager.get_runtime_snapshots(force=force)
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.finished.emit(snapshots)


class AsyncStatusBroker(QObject):
    control_snapshot_requested = Signal(object)
    station_snapshot_requested = Signal(object)

    control_snapshot_ready = Signal(object)
    control_snapshot_failed = Signal(str)
    station_snapshots_ready = Signal(object)
    station_snapshots_failed = Signal(str)

    def __init__(self, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._control_thread: QThread | None = None
        self._control_worker: _ControlStatusWorker | None = None
        self._station_thread: QThread | None = None
        self._station_worker: _StationSnapshotWorker | None = None
        self._control_inflight = False
        self._pending_control_request: Dict[str, Any] | None = None
        self._station_inflight = False
        self._pending_station_request: Dict[str, Any] | None = None
        self._setup_threads()
        self.destroyed.connect(lambda *_args: self.shutdown())

    def _setup_threads(self) -> None:
        if self._control_thread is None:
            self._control_thread = QThread(self)
            self._control_worker = _ControlStatusWorker()
            self._control_worker.moveToThread(self._control_thread)
            self.control_snapshot_requested.connect(self._control_worker.run_snapshot)
            self._control_worker.finished.connect(self._on_control_snapshot_finished)
            self._control_worker.failed.connect(self._on_control_snapshot_failed)
            self._control_thread.finished.connect(self._control_worker.deleteLater)
            self._control_thread.start()
        if self._station_thread is None:
            self._station_thread = QThread(self)
            self._station_worker = _StationSnapshotWorker()
            self._station_worker.moveToThread(self._station_thread)
            self.station_snapshot_requested.connect(self._station_worker.run_snapshot)
            self._station_worker.finished.connect(self._on_station_snapshots_finished)
            self._station_worker.failed.connect(self._on_station_snapshots_failed)
            self._station_thread.finished.connect(self._station_worker.deleteLater)
            self._station_thread.start()

    def shutdown(self) -> None:
        for thread_attr, worker_attr in (
            ("_control_thread", "_control_worker"),
            ("_station_thread", "_station_worker"),
        ):
            thread = getattr(self, thread_attr, None)
            if thread is None:
                continue
            try:
                thread.quit()
                thread.wait(1000)
            except Exception:
                pass
            setattr(self, thread_attr, None)
            setattr(self, worker_attr, None)
        self._control_inflight = False
        self._pending_control_request = None
        self._station_inflight = False
        self._pending_station_request = None

    def request_control_snapshot(
        self,
        *,
        settings: object,
        request: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = dict(request or {})
        payload["settings"] = _snapshot_settings_payload(settings)
        self._dispatch_control_request(payload)

    def request_station_snapshots(
        self,
        *,
        store: object,
        settings: object,
        force: bool = False,
    ) -> None:
        payload = {
            "store": store,
            "settings": _snapshot_settings_payload(settings),
            "force": bool(force),
        }
        self._dispatch_station_request(payload)

    def _dispatch_control_request(self, payload: Dict[str, Any]) -> None:
        if self._control_thread is None or self._control_worker is None:
            self._setup_threads()
        if self._control_thread is None or self._control_worker is None:
            return
        if self._control_inflight:
            self._pending_control_request = dict(payload)
            return
        self._control_inflight = True
        self.control_snapshot_requested.emit(dict(payload))

    def _dispatch_station_request(self, payload: Dict[str, Any]) -> None:
        if self._station_thread is None or self._station_worker is None:
            self._setup_threads()
        if self._station_thread is None or self._station_worker is None:
            return
        if self._station_inflight:
            self._pending_station_request = dict(payload)
            return
        self._station_inflight = True
        self.station_snapshot_requested.emit(dict(payload))

    @Slot(object)
    def _on_control_snapshot_finished(self, snapshot: object) -> None:
        self._control_inflight = False
        self.control_snapshot_ready.emit(snapshot)
        pending = self._pending_control_request
        self._pending_control_request = None
        if pending:
            self._dispatch_control_request(pending)

    @Slot(str)
    def _on_control_snapshot_failed(self, error_text: str) -> None:
        self._control_inflight = False
        log.debug("AsyncStatusBroker: control snapshot failed: %s", error_text)
        self.control_snapshot_failed.emit(str(error_text))
        pending = self._pending_control_request
        self._pending_control_request = None
        if pending:
            self._dispatch_control_request(pending)

    @Slot(object)
    def _on_station_snapshots_finished(self, snapshots: object) -> None:
        self._station_inflight = False
        self.station_snapshots_ready.emit(snapshots)
        pending = self._pending_station_request
        self._pending_station_request = None
        if pending:
            self._dispatch_station_request(pending)

    @Slot(str)
    def _on_station_snapshots_failed(self, error_text: str) -> None:
        self._station_inflight = False
        log.debug("AsyncStatusBroker: station snapshot request failed: %s", error_text)
        self.station_snapshots_failed.emit(str(error_text))
        pending = self._pending_station_request
        self._pending_station_request = None
        if pending:
            self._dispatch_station_request(pending)
