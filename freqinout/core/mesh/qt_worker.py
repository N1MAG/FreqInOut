from __future__ import annotations

from time import monotonic_ns
from pathlib import Path
from typing import Iterable

from PySide6.QtCore import QObject, QTimer, Signal, Slot

from freqinout.core.mesh.manager import MeshAdapterFactory, MeshConnectionManager, default_mesh_adapter_factory
from freqinout.core.mesh.models import MeshAdapterEvent, MeshHealthSnapshot
from freqinout.core.mesh.settings import MeshConnectionConfig
from freqinout.core.mesh.store import MeshEventStoreSink


class MeshConnectionWorker(QObject):
    event_ready = Signal(object)
    health_ready = Signal(object)
    error_ready = Signal(str)
    started = Signal()
    stopped = Signal()

    def __init__(
        self,
        configs: Iterable[MeshConnectionConfig],
        *,
        db_path: str | Path | None = None,
        poll_interval_ms: int = 1000,
        node_poll_interval_ms: int = 30000,
        channel_poll_interval_ms: int = 300000,
        reconnect_interval_ms: int = 15000,
        adapter_factory: MeshAdapterFactory = default_mesh_adapter_factory,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._manager = MeshConnectionManager(configs, adapter_factory=adapter_factory)
        self._poll_interval_ms = max(250, int(poll_interval_ms or 1000))
        self._node_poll_interval_ms = max(self._poll_interval_ms, int(node_poll_interval_ms or 30000))
        self._channel_poll_interval_ms = max(self._poll_interval_ms, int(channel_poll_interval_ms or 300000))
        self._reconnect_interval_ms = max(self._poll_interval_ms, int(reconnect_interval_ms or 15000))
        self._last_node_poll_ms = 0
        self._last_channel_poll_ms = 0
        self._last_reconnect_ms = 0
        self._timer: QTimer | None = None
        self._running = False
        self._stopped_emitted = False
        self._store_sink = MeshEventStoreSink(db_path) if db_path is not None else None
        self._manager.add_listener(self._handle_manager_event)

    @Slot()
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stopped_emitted = False
        try:
            self._manager.start_all()
        except Exception as exc:
            self.error_ready.emit(str(exc))
        if self._timer is None:
            self._timer = QTimer(self)
            self._timer.setInterval(self._poll_interval_ms)
            self._timer.timeout.connect(self.poll_once)
        self._timer.start()
        self.started.emit()

    @Slot()
    def stop(self) -> None:
        was_running = self._running
        self._running = False
        if self._timer is not None:
            self._timer.stop()
            self._timer.deleteLater()
            self._timer = None
        self._last_node_poll_ms = 0
        self._last_channel_poll_ms = 0
        self._last_reconnect_ms = 0
        if was_running:
            try:
                self._manager.stop_all()
            except Exception as exc:
                self.error_ready.emit(str(exc))
        if not self._stopped_emitted:
            self._stopped_emitted = True
            self.stopped.emit()

    @Slot()
    def poll_once(self) -> None:
        if not self._running:
            return
        now_ms = self._elapsed_ms()
        should_reconnect = (now_ms - self._last_reconnect_ms) >= self._reconnect_interval_ms
        should_poll_nodes = (now_ms - self._last_node_poll_ms) >= self._node_poll_interval_ms
        should_poll_channels = (now_ms - self._last_channel_poll_ms) >= self._channel_poll_interval_ms
        if should_reconnect:
            self._retry_disconnected_adapters()
            self._last_reconnect_ms = now_ms
        for adapter_id in self._manager.active_adapter_ids():
            try:
                self._manager.poll_events(adapter_id)
                if should_poll_nodes:
                    self._manager.poll_nodes(adapter_id)
                if should_poll_channels:
                    channels = self._manager.poll_channels(adapter_id)
                    if self._store_sink is not None:
                        if channels:
                            self._store_sink.stage_channels(channels)
                        self._store_sink.prune_retained_messages()
                snapshot = self._manager.health(adapter_id)
                self._handle_health(snapshot)
            except Exception as exc:
                self.error_ready.emit(str(exc))
        if should_poll_nodes:
            self._last_node_poll_ms = now_ms
        if should_poll_channels:
            self._last_channel_poll_ms = now_ms

    def manager(self) -> MeshConnectionManager:
        return self._manager

    def _handle_manager_event(self, event: MeshAdapterEvent) -> None:
        if self._store_sink is not None:
            try:
                self._store_sink(event)
            except Exception as exc:
                self.error_ready.emit(str(exc))
        if event.health is not None:
            self._handle_health(event.health)
        self.event_ready.emit(event)

    def _handle_health(self, snapshot: MeshHealthSnapshot) -> None:
        self.health_ready.emit(snapshot)

    def _retry_disconnected_adapters(self) -> None:
        for adapter_id in self._manager.configured_ids():
            try:
                snapshot = self._manager.health(adapter_id)
                if snapshot.enabled and not snapshot.connected:
                    self._manager.start_adapter(adapter_id)
            except Exception as exc:
                self.error_ready.emit(str(exc))

    @staticmethod
    def _elapsed_ms() -> int:
        return monotonic_ns() // 1_000_000
