from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication, QLabel, QPushButton

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_runtime_manager import DeviceRuntimeSnapshot
from freqinout.gui.message_viewer_tab import JS8Message, UnifiedMessage, VarACMessage
from freqinout.gui.selected_radio_context import SelectedRadioContext
from freqinout.gui.station_overview_tab import StationOverviewTab


def _snapshot(
    device_profile_id: int,
    name: str,
    *,
    runtime_primary: bool = False,
    control_backend: str = "flrig",
    endpoint_summary: str = "127.0.0.1:12345",
) -> DeviceRuntimeSnapshot:
    return DeviceRuntimeSnapshot(
        device_profile_id=device_profile_id,
        name=name,
        device_class="tx_rx",
        control_backend=control_backend,
        deployment_mode="full",
        runtime_active=True,
        runtime_primary=runtime_primary,
        scheduler_owner=runtime_primary,
        endpoint_summary=endpoint_summary,
        ptt_group="A",
        ptt_active=False,
        current_frequency_hz=14_078_000,
        current_frequency_label="14.078 MHz",
        current_band="20M",
        antenna_group="Dipole",
        frontend_group="Main",
        amplifier_group="AL-80",
        assigned_operating_profile_id=1,
        assigned_operating_profile_name="Default Operating Profile",
        assignment_state="active",
        scheduler_enabled=True,
        scheduler_mode="full",
        use_messages=True,
        use_map=True,
        use_background_ingest=True,
        use_launch_control=True,
        use_net_control_tabs=True,
        control_ready=True,
        overall_state="ok",
        status_summary="Runtime ready",
        warning_text="",
        shared_ptt_blocked=False,
        shared_ptt_owner_device_id=None,
        shared_ptt_owner_name="",
        shared_ptt_status_text="",
        observer_follow_source_device_id=None,
        observer_follow_source_name="",
        observer_follow_summary="",
        varac_cluster_name="",
        varac_cluster_id="",
        varac_instance_number=None,
        varac_gateway_handler=False,
        varac_gateway_handler_name="",
        varac_cluster_summary="",
        swap_role="",
        swap_summary="",
        service_states={"FLRig": {"state": "ok", "tooltip": "FLRig ready"}},
    )


class _StubSelectedContext(QObject):
    snapshots_changed = Signal(object)
    selection_changed = Signal(object)

    def __init__(self, snapshots: list[DeviceRuntimeSnapshot], selected_id: int) -> None:
        super().__init__()
        self._snapshots = list(snapshots)
        self._selected_id = int(selected_id)
        self.selected_calls: list[int] = []

    def active_txrx_snapshots(self) -> list[DeviceRuntimeSnapshot]:
        return list(self._snapshots)

    def selected_snapshot(self) -> DeviceRuntimeSnapshot | None:
        return next((row for row in self._snapshots if int(row.device_profile_id) == self._selected_id), None)

    def set_selected_device_profile(self, device_profile_id: int) -> bool:
        self._selected_id = int(device_profile_id)
        self.selected_calls.append(self._selected_id)
        self.selection_changed.emit(self.selected_snapshot())
        return True

    def set_external_selected_device_profile(self, device_profile_id: int) -> None:
        self._selected_id = int(device_profile_id)
        self.selection_changed.emit(self.selected_snapshot())


class _StubManager:
    def __init__(self, snapshots: list[DeviceRuntimeSnapshot]) -> None:
        self._snapshots = list(snapshots)
        self.sync_calls = 0

        class _Store:
            def __init__(self, outer) -> None:
                self._outer = outer
                self.primary_changes: list[int] = []

            def set_runtime_primary_device_profile(self, device_profile_id: int) -> None:
                self.primary_changes.append(int(device_profile_id))
                for snapshot in self._outer._snapshots:
                    snapshot.runtime_primary = int(snapshot.device_profile_id) == int(device_profile_id)

        self.store = _Store(self)

    def sync_with_store(self) -> None:
        self.sync_calls += 1

    def get_runtime_snapshots(self, *, force: bool = False) -> list[DeviceRuntimeSnapshot]:
        return list(self._snapshots)


def test_station_overview_card_shows_selected_radio_and_selects_from_card(monkeypatch):
    app = QApplication.instance() or QApplication([])
    snapshots = [_snapshot(1, "Alpha", runtime_primary=True), _snapshot(2, "Bravo")]
    context = _StubSelectedContext(snapshots, selected_id=2)
    tab = StationOverviewTab()
    try:
        tab.set_station_context(context)
        theme = {
            "success": "#0a0",
            "warning": "#aa0",
            "danger": "#a00",
            "surface_alt": "#333",
            "text_on_accent": "#fff",
            "text": "#ddd",
            "border": "#555",
            "surface": "#111",
            "info": "#09f",
            "accent": "#c90",
        }
        bravo_card = tab._build_runtime_card(snapshots[1], theme)
        bravo_labels = [label.text() for label in bravo_card.findChildren(QLabel)]
        assert "Selected Radio" in bravo_labels

        alpha_card = tab._build_runtime_card(snapshots[0], theme)
        select_btn = next(btn for btn in alpha_card.findChildren(QPushButton) if btn.text() == "Select")
        select_btn.click()
        app.processEvents()
        assert context.selected_calls[-1] == 1
    finally:
        tab.deleteLater()
        app.processEvents()


def test_message_viewer_supports_radio_and_source_filters_from_station_context(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.message_viewer_tab import MessageViewerTab

    monkeypatch.setattr(MessageViewerTab, "_initial_refresh", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_timer", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_js8_timer", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_pending_timer", lambda self: None)

    snapshots = [_snapshot(1, "Alpha"), _snapshot(2, "Bravo", runtime_primary=True, control_backend="js8call", endpoint_summary="127.0.0.1:2442")]
    context = _StubSelectedContext(snapshots, selected_id=2)

    tab = MessageViewerTab()
    try:
        tab.set_station_context(context)

        rows = [
            UnifiedMessage(
                msg_type="MSG",
                status="NEW",
                from_call="K1AAA",
                to_call="N0CALL",
                rcv_ts=1.0,
                rcv_display="2026-03-16 00:01",
                title="Alpha inbound [Alpha]",
                origin="js8",
                payload=JS8Message(
                    msg_id=1,
                    from_call="K1AAA",
                    to_call="N0CALL",
                    msg_type="MSG",
                    utc_str="2026-03-16 00:01:00",
                    utc_ts=1.0,
                    raw_text="RAW",
                    decoded_text="RAW",
                    state="UNREAD",
                    source_key="device:1",
                    source_label="Alpha",
                    device_profile_id=1,
                ),
            ),
            UnifiedMessage(
                msg_type="MSG",
                status="NEW",
                from_call="K2BBB",
                to_call="N0CALL",
                rcv_ts=2.0,
                rcv_display="2026-03-16 00:02",
                title="Bravo inbound [Bravo]",
                origin="js8",
                payload=JS8Message(
                    msg_id=2,
                    from_call="K2BBB",
                    to_call="N0CALL",
                    msg_type="MSG",
                    utc_str="2026-03-16 00:02:00",
                    utc_ts=2.0,
                    raw_text="RAW",
                    decoded_text="RAW",
                    state="UNREAD",
                    source_key="device:2",
                    source_label="Bravo",
                    device_profile_id=2,
                ),
            ),
            UnifiedMessage(
                msg_type="VarAC",
                status="READ",
                from_call="K3CCC",
                to_call="N0CALL",
                rcv_ts=3.0,
                rcv_display="2026-03-16 00:03",
                title="Cluster bulletin [Ops Cluster]",
                origin="varac",
                payload=VarACMessage(
                    msg_id=3,
                    guid="guid-3",
                    source="inbox",
                    msg_type="CHAT",
                    from_call="K3CCC",
                    to_call="N0CALL",
                    subject="Ops Cluster",
                    body="Hello",
                    ts=3.0,
                    band="40M",
                    freq_hz=7_100_000.0,
                    snr=None,
                    read_status=1,
                    folder="Inbox",
                    vmail_guid="",
                    ingest_source_label="Ops Cluster",
                    ingest_scope="cluster",
                ),
            ),
        ]

        tab._message_rows = rows
        tab._refresh_message_filters(rows)
        selected_idx = tab.radio_filter.findData("__selected__")
        assert selected_idx >= 0
        tab.radio_filter.setCurrentIndex(selected_idx)
        tab._apply_message_filters()
        filtered = tab._messages_model.rows()
        assert len(filtered) == 1
        assert filtered[0].title.startswith("Bravo inbound")

        source_idx = tab.source_filter.findData("Ops Cluster")
        assert source_idx >= 0
        tab.radio_filter.setCurrentIndex(tab.radio_filter.findData("__all__"))
        tab.source_filter.setCurrentIndex(source_idx)
        tab._apply_message_filters()
        filtered = tab._messages_model.rows()
        assert len(filtered) == 1
        assert filtered[0].origin == "varac"
        assert "Selected radio: Bravo" in tab.scope_summary_label.text()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_local_ncs_pins_session_to_selected_radio(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.local_ncs_tab import LocalNCSTab

    monkeypatch.setattr(LocalNCSTab, "_restore_context", lambda self: None)
    monkeypatch.setattr(LocalNCSTab, "reload_operator_lookup", lambda self: None)
    monkeypatch.setattr(LocalNCSTab, "_load_checkins", lambda self: None)
    monkeypatch.setattr(LocalNCSTab, "_setup_timers", lambda self: None)

    snapshots = [_snapshot(1, "Alpha", runtime_primary=True), _snapshot(2, "Bravo")]
    context = _StubSelectedContext(snapshots, selected_id=2)

    tab = LocalNCSTab()
    try:
        tab.set_station_context(context)
        tab.net_name_edit.setText("Local Net")
        tab._start_local_net()
        assert tab._session_device_profile_id == 2
        assert tab.selected_radio_combo.isEnabled() is False
        assert "Pinned" in tab.selected_radio_label.text()

        context.set_external_selected_device_profile(1)
        app.processEvents()
        assert context.selected_calls[-1] == 2
    finally:
        tab.deleteLater()
        app.processEvents()


def test_net_schedule_default_target_persists_and_new_rows_inherit(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    device = store.save_device_profile(
        {
            "name": "Field Rig",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.2",
            "flrig_port": 22345,
        }
    )

    from freqinout.gui.net_schedule_tab import NetScheduleTab

    monkeypatch.setattr(NetScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(NetScheduleTab, "_bootstrap_net_resources", lambda self: None)
    monkeypatch.setattr(NetScheduleTab, "_load_resources_from_db", lambda self: None)
    monkeypatch.setattr(NetScheduleTab, "_refresh_resource_set_combo", lambda self: None)
    monkeypatch.setattr(NetScheduleTab, "_refresh_resources_table", lambda self: None)
    monkeypatch.setattr(NetScheduleTab, "_schedule_net_sop_conflict_refresh", lambda self, **kwargs: None)
    monkeypatch.setattr(NetScheduleTab, "_enforce_net_priority_for_conflicts", lambda self, rows, **kwargs: True)
    monkeypatch.setattr(NetScheduleTab, "_save_to_db", lambda self, rows: None)
    monkeypatch.setattr(NetScheduleTab, "_bump_net_sop_conflict_scan_epoch", lambda self: None)
    monkeypatch.setattr("freqinout.gui.net_schedule_tab.QMessageBox.information", lambda *args, **kwargs: None)

    tab = NetScheduleTab()
    try:
        idx = tab.default_target_scope_combo.findData("device_profile")
        assert idx >= 0
        tab.default_target_scope_combo.setCurrentIndex(idx)
        app.processEvents()
        device_idx = tab.default_target_value_combo.findData(int(device["id"]))
        assert device_idx >= 0
        tab.default_target_value_combo.setCurrentIndex(device_idx)

        tab.table.setRowCount(0)
        tab._add_row(
            {
                "day_utc": "Monday",
                "recurrence": "Weekly",
                "group_name": "OPS-A",
                "mode": "Digi",
                "band": "40M",
                "frequency": "7.110",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "early_checkin": "5",
                "net_name": "Night Net",
                "target_scope": "inherited",
            }
        )

        scope_widget = tab.table.cellWidget(0, tab.COL_TARGET_SCOPE)
        value_widget = tab.table.cellWidget(0, tab.COL_TARGET)
        assert scope_widget.currentData() == "inherited"
        assert value_widget.isEnabled() is False

        tab._save()
    finally:
        tab.deleteLater()
        app.processEvents()

    reloaded = NetScheduleTab()
    try:
        assert reloaded.default_target_scope_combo.currentData() == "device_profile"
        assert reloaded.default_target_value_combo.currentData() == int(device["id"])
    finally:
        reloaded.deleteLater()
        app.processEvents()


def test_selected_radio_context_switches_quickly_between_active_radios():
    snapshots = [_snapshot(1, "Alpha", runtime_primary=True), _snapshot(2, "Bravo")]
    manager = _StubManager(snapshots)
    context = SelectedRadioContext(manager)

    context.sync(force=True)
    assert context.selected_snapshot() is not None
    assert int(context.selected_snapshot().device_profile_id) == 1

    changed = context.set_selected_device_profile(2)
    assert changed is True
    assert manager.store.primary_changes[-1] == 2
    assert int(context.selected_snapshot().device_profile_id) == 2
