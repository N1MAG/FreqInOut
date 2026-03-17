from __future__ import annotations

from typing import Any, Dict, List, Optional

from PySide6.QtCore import QObject, Signal

from freqinout.core.logger import log
from freqinout.core.station_runtime_manager import DeviceRuntimeSnapshot, StationRuntimeManager


class SelectedRadioContext(QObject):
    snapshots_changed = Signal(object)
    selection_changed = Signal(object)

    def __init__(self, manager: StationRuntimeManager, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._manager = manager
        self._selected_device_profile_id: Optional[int] = None
        self._snapshots: List[DeviceRuntimeSnapshot] = []

    def sync(self, *, force: bool = False) -> None:
        try:
            self._manager.sync_with_store()
        except Exception as exc:
            log.debug("SelectedRadioContext: runtime sync failed: %s", exc)
        try:
            snapshots = list(self._manager.get_runtime_snapshots(force=force))
        except Exception as exc:
            log.debug("SelectedRadioContext: snapshot load failed: %s", exc)
            snapshots = []
        self._snapshots = snapshots
        valid_ids = {int(snapshot.device_profile_id) for snapshot in snapshots}
        if self._selected_device_profile_id not in valid_ids:
            selected = next(
                (snapshot for snapshot in snapshots if snapshot.runtime_primary and snapshot.device_class != "observer"),
                None,
            )
            if selected is None:
                selected = next((snapshot for snapshot in snapshots if snapshot.device_class != "observer"), None)
            if selected is None:
                selected = next(iter(snapshots), None)
            self._selected_device_profile_id = int(selected.device_profile_id) if selected is not None else None
        self.snapshots_changed.emit(list(self._snapshots))
        self.selection_changed.emit(self.selected_snapshot())

    def snapshots(self) -> List[DeviceRuntimeSnapshot]:
        return list(self._snapshots)

    def active_txrx_snapshots(self) -> List[DeviceRuntimeSnapshot]:
        return [snapshot for snapshot in self._snapshots if snapshot.device_class != "observer"]

    def observer_snapshots(self) -> List[DeviceRuntimeSnapshot]:
        return [snapshot for snapshot in self._snapshots if snapshot.device_class == "observer"]

    def selected_device_profile_id(self) -> Optional[int]:
        return self._selected_device_profile_id

    def selected_snapshot(self) -> Optional[DeviceRuntimeSnapshot]:
        selected_id = self._selected_device_profile_id
        if selected_id is None:
            return None
        return next((snapshot for snapshot in self._snapshots if int(snapshot.device_profile_id) == int(selected_id)), None)

    def set_selected_device_profile(self, device_profile_id: int) -> bool:
        selected_id = int(device_profile_id or 0)
        if selected_id <= 0:
            return False
        current = self.selected_snapshot()
        if current is not None and int(current.device_profile_id) == selected_id and bool(current.runtime_primary):
            self._selected_device_profile_id = selected_id
            self.selection_changed.emit(current)
            return True
        try:
            self._manager.store.set_runtime_primary_device_profile(selected_id)
        except Exception as exc:
            log.warning("SelectedRadioContext: failed setting selected radio %s: %s", selected_id, exc)
            return False
        self._selected_device_profile_id = selected_id
        self.sync(force=True)
        return True

    def selected_target_context(self) -> tuple[Optional[int], Optional[int]]:
        snapshot = self.selected_snapshot()
        if snapshot is None:
            return None, None
        return int(snapshot.device_profile_id), (
            int(snapshot.assigned_operating_profile_id)
            if snapshot.assigned_operating_profile_id not in (None, "")
            else None
        )

    def summary(self) -> Dict[str, Any]:
        selected = self.selected_snapshot()
        radios = self.active_txrx_snapshots()
        observers = self.observer_snapshots()
        return {
            "active_radio_count": len(radios),
            "observer_count": len(observers),
            "selected_name": selected.name if selected is not None else "",
            "selected_device_profile_id": selected.device_profile_id if selected is not None else None,
        }
