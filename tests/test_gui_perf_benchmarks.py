from __future__ import annotations

import os
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_runtime_manager import DeviceRuntimeSnapshot
from freqinout.gui.message_viewer_tab import JS8Message, UnifiedMessage
from freqinout.gui.selected_radio_context import SelectedRadioContext


def _snapshot(device_profile_id: int, name: str, *, runtime_primary: bool = False) -> DeviceRuntimeSnapshot:
    return DeviceRuntimeSnapshot(
        device_profile_id=device_profile_id,
        name=name,
        device_class="tx_rx",
        control_backend="js8call",
        deployment_mode="full",
        runtime_active=True,
        runtime_primary=runtime_primary,
        scheduler_owner=runtime_primary,
        endpoint_summary=f"127.0.0.1:{2441 + device_profile_id}",
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
        service_states={"JS8Call": {"state": "ok", "tooltip": "JS8Call ready"}},
    )


class _StubManager:
    def __init__(self, snapshots: list[DeviceRuntimeSnapshot]) -> None:
        self._snapshots = list(snapshots)

        class _Store:
            def __init__(self, outer) -> None:
                self._outer = outer

            def set_runtime_primary_device_profile(self, device_profile_id: int) -> None:
                for snapshot in self._outer._snapshots:
                    snapshot.runtime_primary = int(snapshot.device_profile_id) == int(device_profile_id)

        self.store = _Store(self)

    def sync_with_store(self) -> None:
        return None

    def get_runtime_snapshots(self, *, force: bool = False) -> list[DeviceRuntimeSnapshot]:
        return list(self._snapshots)


def test_selected_radio_context_sync_benchmark():
    snapshots = [_snapshot(1, "Alpha", runtime_primary=True), _snapshot(2, "Bravo"), _snapshot(3, "Charlie")]
    context = SelectedRadioContext(_StubManager(snapshots))

    started_at = time.perf_counter()
    for _ in range(40):
        context.sync(force=True)
    elapsed = time.perf_counter() - started_at

    assert elapsed < 0.35


def test_message_viewer_filter_benchmark(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.message_viewer_tab import MessageViewerTab

    monkeypatch.setattr(MessageViewerTab, "_initial_refresh", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_timer", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_js8_timer", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_pending_timer", lambda self: None)

    tab = MessageViewerTab()
    try:
        rows: list[UnifiedMessage] = []
        for idx in range(1600):
            device_id = 1 if idx % 2 == 0 else 2
            label = "Alpha" if device_id == 1 else "Bravo"
            payload = JS8Message(
                msg_id=idx + 1,
                from_call=f"K{idx:04d}",
                to_call="N0CALL",
                msg_type="MSG",
                utc_str=f"2026-03-16 00:{idx % 60:02d}:00",
                utc_ts=float(idx),
                raw_text="RAW",
                decoded_text="RAW",
                state="UNREAD",
                source_key=f"device:{device_id}",
                source_label=label,
                device_profile_id=device_id,
            )
            rows.append(
                UnifiedMessage(
                    msg_type="MSG",
                    status="NEW",
                    from_call=payload.from_call,
                    to_call=payload.to_call,
                    rcv_ts=payload.utc_ts,
                    rcv_display=payload.utc_str,
                    title=f"Traffic {idx} [{label}]",
                    origin="js8",
                    payload=payload,
                )
            )

        tab._message_rows = rows
        started_at = time.perf_counter()
        tab._refresh_message_filters(rows)
        radio_idx = tab.radio_filter.findData("__all__")
        if radio_idx >= 0:
            tab.radio_filter.setCurrentIndex(radio_idx)
        tab._apply_message_filters()
        elapsed = time.perf_counter() - started_at

        assert len(tab._messages_model.rows()) == len(rows)
        assert elapsed < 0.85
    finally:
        tab.deleteLater()
        app.processEvents()


def test_net_schedule_target_widget_refresh_benchmark(monkeypatch, tmp_path):
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

    tab = NetScheduleTab()
    try:
        tab.table.setRowCount(0)
        for idx in range(80):
            tab._add_row(
                {
                    "day_utc": "Monday",
                    "recurrence": "Weekly",
                    "group_name": "OPS-A",
                    "mode": "Digi",
                    "band": "40M",
                    "frequency": "7.110",
                    "start_utc": f"{idx % 24:02d}:00",
                    "end_utc": f"{(idx + 1) % 24:02d}:00",
                    "early_checkin": "5",
                    "net_name": f"Night Net {idx}",
                    "target_scope": "inherited",
                }
            )

        started_at = time.perf_counter()
        tab.default_target_scope_combo.setCurrentIndex(tab.default_target_scope_combo.findData("device_profile"))
        tab.default_target_value_combo.setCurrentIndex(tab.default_target_value_combo.findData(int(device["id"])))
        tab._refresh_schedule_target_widgets()
        elapsed = time.perf_counter() - started_at

        assert elapsed < 0.75
    finally:
        tab.deleteLater()
        app.processEvents()
