from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QCheckBox
import pytest

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import DeviceRuntime, StationRuntimeManager
from freqinout.gui.station_overview_tab import StationOverviewTab
from freqinout.radio_interface.js8_rx_hub import JS8RxHub


def _select_device_profiles(tab, profile_ids: list[int]) -> None:
    wanted = {int(profile_id) for profile_id in profile_ids}
    found: set[int] = set()
    for row in range(tab.device_profiles_table.rowCount()):
        widget = tab.device_profiles_table.cellWidget(row, 0)
        chk = widget.findChild(QCheckBox) if widget is not None else None
        if chk is None:
            continue
        profile_id = int(chk.property("device_profile_id") or 0)
        chk.setChecked(profile_id in wanted)
        if profile_id in wanted:
            found.add(profile_id)
    assert found == wanted


def _stub_status_snapshot(self, **kwargs):
    return {
        "JS8Call_API": {"state": "idle", "tooltip": "JS8 idle"},
        "JS8Call": {"state": "idle", "tooltip": "JS8 idle"},
        "FLRig": {"state": "ok", "tooltip": "FLRig reachable"},
        "RigCtlD": {"state": "ok", "tooltip": "RigCtlD reachable"},
        "FLDigi": {"state": "idle", "tooltip": "FLDigi idle"},
        "FLMsg": {"state": "idle", "tooltip": "FLMsg idle"},
        "FLAmp": {"state": "idle", "tooltip": "FLAmp idle"},
        "VarAC": {"state": "idle", "tooltip": "VarAC idle"},
        "JS8Spotter": {"state": "idle", "tooltip": "JS8Spotter idle"},
        "CommStat": {"state": "idle", "tooltip": "CommStat idle"},
    }


def _qapplication_or_skip():
    app = QApplication.instance()
    if app is not None and not isinstance(app, QApplication):
        pytest.skip("A non-GUI QCoreApplication already exists in this test process.")
    return app or QApplication([])


def test_store_supports_multiple_active_devices_with_one_primary(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    remote_flrig = store.save_device_profile(
        {
            "name": "Remote FLRig",
            "control_backend": "flrig",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
        }
    )
    remote_rigctld = store.save_device_profile(
        {
            "name": "Remote Rigctld",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
        }
    )

    store.set_device_profile_runtime_active(int(remote_flrig["id"]), True)
    store.set_device_profile_runtime_active(int(remote_rigctld["id"]), True)

    active_ids = {int(row["id"]) for row in store.list_runtime_active_device_profiles()}
    primary_before = store.get_runtime_primary_device_profile()

    assert int(remote_flrig["id"]) in active_ids
    assert int(remote_rigctld["id"]) in active_ids
    assert primary_before is not None
    assert primary_before["system_key"] == "default_device"

    store.set_runtime_primary_device_profile(int(remote_rigctld["id"]))
    settings.reload()

    active_ids = {int(row["id"]) for row in store.list_runtime_active_device_profiles()}
    primary_after = store.get_runtime_primary_device_profile()

    assert int(remote_flrig["id"]) in active_ids
    assert int(remote_rigctld["id"]) in active_ids
    assert primary_after is not None
    assert int(primary_after["id"]) == int(remote_rigctld["id"])
    assert settings.get("control_via") == "RIGCTLD"
    assert settings.get("rig_host") == "10.0.0.44"
    assert int(settings.get("rig_port")) == 4532


def test_restart_preserves_non_default_runtime_primary(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    remote = store.save_device_profile(
        {
            "name": "Remote JS8",
            "control_backend": "js8call",
            "js8_host": "10.0.0.10",
            "js8_port": 2542,
        }
    )
    store.set_device_profile_runtime_active(int(remote["id"]), True)
    store.set_runtime_primary_device_profile(int(remote["id"]))

    restarted_settings = SettingsManager()
    restarted_store = MultiRadioStore(settings_db_path())

    primary = restarted_store.get_runtime_primary_device_profile()
    devices = restarted_store.list_device_profiles()
    default_device = next(row for row in devices if row["system_key"] == "default_device")

    assert primary is not None
    assert int(primary["id"]) == int(remote["id"])
    if int(default_device.get("id", 0) or 0) != int(remote["id"]):
        assert int(default_device.get("runtime_primary", 0) or 0) == 0
    assert int(default_device.get("runtime_active", 0) or 0) == 1
    assert restarted_settings.get("control_via") == "JS8Call"
    assert restarted_settings.get("js8_host") == "10.0.0.10"
    assert int(restarted_settings.get("js8_port")) == 2542


def test_store_blocks_primary_deactivation_until_another_primary_exists(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    default_primary = store.save_device_profile(
        {
            "name": "Primary FLRig",
            "control_backend": "flrig",
            "flrig_host": "10.0.0.7",
            "flrig_port": 12345,
        }
    )
    store.set_device_profile_runtime_active(int(default_primary["id"]), True)
    store.set_runtime_primary_device_profile(int(default_primary["id"]))

    remote = store.save_device_profile(
        {
            "name": "Remote FLRig",
            "control_backend": "flrig",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
        }
    )
    store.set_device_profile_runtime_active(int(remote["id"]), True)

    with pytest.raises(ValueError):
        store.set_device_profile_runtime_active(int(default_primary["id"]), False)

    store.set_runtime_primary_device_profile(int(remote["id"]))
    store.set_device_profile_runtime_active(int(default_primary["id"]), False)

    active_ids = {int(row["id"]) for row in store.list_runtime_active_device_profiles()}
    primary = store.get_runtime_primary_device_profile()

    assert int(default_primary["id"]) not in active_ids
    assert primary is not None
    assert int(primary["id"]) == int(remote["id"])


def test_station_runtime_manager_snapshots_multiple_active_devices(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    local = store.save_device_profile(
        {
            "name": "Local FLRig",
            "control_backend": "flrig",
            "flrig_host": "10.0.0.8",
            "flrig_port": 12345,
            "deployment_mode": "full",
        }
    )
    remote = store.save_device_profile(
        {
            "name": "Remote Rigctld",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
            "deployment_mode": "minimal",
        }
    )
    store.set_device_profile_runtime_active(int(local["id"]), True)
    store.set_device_profile_runtime_active(int(remote["id"]), True)
    store.set_runtime_primary_device_profile(int(remote["id"]))

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _stub_status_snapshot)
    monkeypatch.setattr(DeviceRuntime, "ptt_active", lambda self, force=False: False)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 7_115_000 if str(self.profile.get("control_backend", "")).lower() == "flrig" else 14_115_000,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots()

    assert len(snapshots) == 2
    assert sum(1 for snap in snapshots if snap.runtime_primary) == 1
    primary = next(snap for snap in snapshots if snap.runtime_primary)
    assert primary.device_profile_id == int(remote["id"])
    assert primary.control_ready is True
    assert primary.deployment_mode == "minimal"
    assert primary.endpoint_summary == "rigctld 10.0.0.44:4532"


def test_station_overview_tab_renders_active_runtime_cards(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = _qapplication_or_skip()

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    remote = store.save_device_profile(
        {
            "name": "Remote FLRig",
            "control_backend": "flrig",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
        }
    )
    store.set_device_profile_runtime_active(int(remote["id"]), True)

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _stub_status_snapshot)
    monkeypatch.setattr(DeviceRuntime, "ptt_active", lambda self, force=False: False)
    monkeypatch.setattr(DeviceRuntime, "current_frequency_hz", lambda self, force=False: 7_115_000)

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    tab = StationOverviewTab()
    try:
        tab.set_runtime_manager(manager)
        assert "active device profile" in tab.summary_label.text()
        assert tab.control_center_table.objectName() == "stationControlCenterTable"
        assert tab.control_center_table.rowCount() >= 1
        assert tab.control_center_table.columnCount() == 6
        assert tab.control_center_table.horizontalHeaderItem(0).text() == "Radio / SDR"
        assert tab.control_center_table.horizontalHeaderItem(2).text() == "Control State"
        assert tab.control_center_table.horizontalHeaderItem(5).text() == "Actions"
        row_text = [
            tab.control_center_table.item(row, 0).text()
            for row in range(tab.control_center_table.rowCount())
            if tab.control_center_table.item(row, 0) is not None
        ]
        assert any("Remote FLRig" in text for text in row_text)
        assert {
            tab.control_center_table.item(row, 5).text()
            for row in range(tab.control_center_table.rowCount())
            if tab.control_center_table.item(row, 5) is not None
        } == {"Read-only"}
        assert tab.cards_layout.count() >= 2
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_supports_multi_active_profiles_and_primary_selection(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = _qapplication_or_skip()

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    remote_flrig = store.save_device_profile(
        {
            "name": "Remote FLRig",
            "control_backend": "flrig",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
        }
    )
    remote_rigctld = store.save_device_profile(
        {
            "name": "Remote Rigctld",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        _select_device_profiles(tab, [int(remote_flrig["id"]), int(remote_rigctld["id"])])
        tab._activate_selected_device_profiles()

        active_ids = {int(row["id"]) for row in store.list_runtime_active_device_profiles()}
        assert int(remote_flrig["id"]) in active_ids
        assert int(remote_rigctld["id"]) in active_ids

        _select_device_profiles(tab, [int(remote_rigctld["id"])])
        tab._set_active_selected_device_profile()

        primary = store.get_runtime_primary_device_profile()
        assert primary is not None
        assert int(primary["id"]) == int(remote_rigctld["id"])
        assert tab.control_combo.currentText() == "RIGCTLD"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_runtime_command_focus_radio_is_also_active(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio_a = store.save_device_profile(
        {
            "name": "FIO-A",
            "control_backend": "flrig",
            "runtime_active": 1,
            "runtime_primary": 1,
        }
    )
    radio_b = store.save_device_profile(
        {
            "name": "FIO-B",
            "control_backend": "flrig",
            "runtime_active": 0,
            "runtime_primary": 0,
        }
    )

    focused = store.set_runtime_primary_device_profile(int(radio_b["id"]))

    assert int(focused["id"]) == int(radio_b["id"])
    assert int(focused["runtime_primary"]) == 1
    assert int(focused["runtime_active"]) == 1
    active_ids = {int(row["id"]) for row in store.list_runtime_active_device_profiles()}
    assert {int(radio_a["id"]), int(radio_b["id"])} <= active_ids


def test_settings_use_radio_refreshes_runtime_projection(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = _qapplication_or_skip()

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio_b = store.save_device_profile(
        {
            "name": "FIO-B",
            "control_backend": "flrig",
            "runtime_active": 0,
            "runtime_primary": 0,
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    refresh_calls = []
    monkeypatch.setattr(tab, "_refresh_runtime_projection_ui", lambda **kwargs: refresh_calls.append(dict(kwargs)))
    try:
        _select_device_profiles(tab, [int(radio_b["id"])])
        tab._activate_selected_device_profiles()

        refreshed = store.get_device_profile(int(radio_b["id"]))
        assert refreshed is not None
        assert int(refreshed["runtime_active"]) == 1
        assert refresh_calls == [{"refresh_multi_radio": True, "emit_saved": True}]
    finally:
        tab.deleteLater()
        app.processEvents()


def test_js8_rx_hub_instances_are_keyed_by_endpoint():
    app = _qapplication_or_skip()
    hub_a = JS8RxHub.instance("127.0.0.1", 2442)
    hub_b = JS8RxHub.instance("127.0.0.1", 2542)
    try:
        assert hub_a is not hub_b
        assert hub_a.endpoint() == ("127.0.0.1", 2442)
        assert hub_b.endpoint() == ("127.0.0.1", 2542)
    finally:
        JS8RxHub.shutdown_all()
        app.processEvents()
