from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QCheckBox

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import DeviceRuntime, StationRuntimeManager
from freqinout.gui.station_overview_tab import StationOverviewTab


def _select_device_profiles(tab, device_ids: list[int]) -> None:
    wanted = {int(device_id) for device_id in device_ids}
    found: set[int] = set()
    for row in range(tab.device_profiles_table.rowCount()):
        widget = tab.device_profiles_table.cellWidget(row, 0)
        chk = widget.findChild(QCheckBox) if widget is not None else None
        if chk is None:
            continue
        device_id = int(chk.property("device_profile_id") or 0)
        chk.setChecked(device_id in wanted)
        if device_id in wanted:
            found.add(device_id)
    assert found == wanted


def _select_assignment_devices(tab, device_ids: list[int]) -> None:
    wanted = {int(device_id) for device_id in device_ids}
    found: set[int] = set()
    for row in range(tab.device_assignments_table.rowCount()):
        widget = tab.device_assignments_table.cellWidget(row, 0)
        chk = widget.findChild(QCheckBox) if widget is not None else None
        if chk is None:
            continue
        device_id = int(chk.property("device_profile_id") or 0)
        chk.setChecked(device_id in wanted)
        if device_id in wanted:
            found.add(device_id)
    assert found == wanted


def _observer_device(store: MultiRadioStore) -> dict[str, object]:
    observer = store.save_device_profile(
        {
            "name": "North SDR",
            "device_class": "observer",
            "control_backend": "manual",
            "sdr_host": "10.0.0.50",
            "sdr_port": 7300,
            "frontend_group": "FRONT-A",
        }
    )
    store.set_device_profile_runtime_active(int(observer["id"]), True)
    return observer


def _status_snapshot(*_args, **_kwargs):
    return {
        "JS8Call_API": {"state": "idle", "tooltip": "JS8 idle"},
        "JS8Call": {"state": "idle", "tooltip": "JS8 idle"},
        "FLRig": {"state": "ok", "tooltip": "FLRig reachable"},
        "RigCtlD": {"state": "idle", "tooltip": "RigCtlD idle"},
        "FLDigi": {"state": "idle", "tooltip": "FLDigi idle"},
        "FLMsg": {"state": "idle", "tooltip": "FLMsg idle"},
        "FLAmp": {"state": "idle", "tooltip": "FLAmp idle"},
        "VarAC": {"state": "idle", "tooltip": "VarAC idle"},
        "JS8Spotter": {"state": "idle", "tooltip": "JS8Spotter idle"},
        "CommStat": {"state": "idle", "tooltip": "CommStat idle"},
    }


def test_store_derives_sdr_follow_policies_and_blocks_observer_primary(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    observer = _observer_device(store)

    policies = store.list_station_coordination_policies("sdr_follow")
    policy = next(
        row
        for row in policies
        if int(row.get("source_device_id", 0) or 0) == int(primary["id"])
        and int(row.get("target_device_id", 0) or 0) == int(observer["id"])
    )
    assert policy["action"]["guidance"] == "observer_follow_advisory"
    assert policy["action"]["park_strategy"] == "alternate_preferred_band"
    assert policy["safety_mode"] == "warn"

    with pytest.raises(ValueError):
        store.set_runtime_primary_device_profile(int(observer["id"]))

    with pytest.raises(ValueError):
        store.start_temporary_profile_swap(int(observer["id"]))


def test_station_runtime_manager_reports_observer_follow_guidance(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    observer = _observer_device(store)
    watcher = store.save_operating_profile(
        {
            "name": "Observer Watch",
            "preferred_band_set": ["40m", "80m"],
            "scheduler_enabled": False,
        }
    )
    store.set_device_operating_profile(
        int(observer["id"]),
        int(watcher["id"]),
        assignment_state="active",
        reason="Observer follow plan",
    )

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _status_snapshot)
    monkeypatch.setattr(
        SoftwareStatusService,
        "generic_endpoint_status",
        lambda self, **kwargs: {
            "state": "ok",
            "tooltip": f"Observer SDR reachable at {kwargs['host']}:{kwargs['port']}",
            "running": True,
            "reachable": True,
            "endpoint": f"{kwargs['host']}:{kwargs['port']}",
        },
    )
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 14_078_000 if int(self.profile.get("runtime_primary", 0) or 0) == 1 else None,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)

    observer_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(observer["id"]))
    assert observer_snapshot.device_class == "observer"
    assert observer_snapshot.service_states["Observer"]["state"] == "ok"
    assert observer_snapshot.observer_follow_source_name == str(primary["name"])
    assert "park on 40M" in observer_snapshot.observer_follow_summary


def test_settings_tab_persists_observer_fields_and_blocks_primary_swap_actions(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    observer = _observer_device(store)

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        row_index = next(
            row
            for row in range(tab.device_profiles_table.rowCount())
            if int(tab.device_profiles_table.item(row, 3).data(Qt.UserRole) or 0) == int(observer["id"])
        )
        assert tab.device_profiles_table.item(row_index, 6).text() == "Observer SDR 10.0.0.50:7300"
        assert tab.device_profiles_table.item(row_index, 9).text() == "Observer / SDR"

        _select_device_profiles(tab, [int(observer["id"])])
        tab._update_device_profile_action_buttons()
        assert tab.set_active_device_profile_btn.isEnabled() is False

        _select_assignment_devices(tab, [int(observer["id"])])
        tab._update_device_assignment_action_buttons()
        assert tab.temporary_profile_swap_btn.isEnabled() is False
    finally:
        tab.deleteLater()
        app.processEvents()


def test_station_overview_shows_observer_follow_guidance(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    observer = _observer_device(store)
    watcher = store.save_operating_profile(
        {
            "name": "Observer Watch",
            "preferred_band_set": ["40m", "80m"],
            "scheduler_enabled": False,
        }
    )
    store.set_device_operating_profile(
        int(observer["id"]),
        int(watcher["id"]),
        assignment_state="active",
        reason="Observer follow plan",
    )

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _status_snapshot)
    monkeypatch.setattr(
        SoftwareStatusService,
        "generic_endpoint_status",
        lambda self, **kwargs: {
            "state": "ok",
            "tooltip": f"Observer SDR reachable at {kwargs['host']}:{kwargs['port']}",
            "running": True,
            "reachable": True,
            "endpoint": f"{kwargs['host']}:{kwargs['port']}",
        },
    )
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 14_078_000 if int(self.profile.get("runtime_primary", 0) or 0) == 1 else None,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    tab = StationOverviewTab()
    try:
        tab.set_runtime_manager(manager)
        assert "Observer profiles: 1." in tab.summary_label.text()
        texts: list[str] = []
        for idx in range(tab.cards_layout.count() - 1):
            widget = tab.cards_layout.itemAt(idx).widget()
            if widget is None:
                continue
            texts.extend(label.text() for label in widget.findChildren(type(tab.summary_label)))
        assert any("park on 40M" in text for text in texts)
    finally:
        tab.deleteLater()
        app.processEvents()
