from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication
import pytest

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.radio_status_poll_coordinator import RadioStatusSnapshot
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import DeviceRuntime, StationRuntimeManager
from freqinout.gui import qsy_helper


def _idle_status_snapshot(self, **kwargs):
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


def test_store_derives_rf_conflict_policies_from_shared_resources(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    first = store.save_device_profile(
        {
            "name": "TX-A",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "antenna_group": " ant-1 ",
            "amplifier_group": "amp-main",
        }
    )
    second = store.save_device_profile(
        {
            "name": "TX-B",
            "control_backend": "rigctld",
            "rig_host": "127.0.0.1",
            "rig_port": 4532,
            "antenna_group": "ANT-1",
            "frontend_group": "front-a",
        }
    )
    store.save_device_profile(
        {
            "name": "Observer",
            "control_backend": "manual",
            "device_class": "observer",
            "antenna_group": "ANT-1",
            "amplifier_group": "AMP-MAIN",
        }
    )

    policies = store.list_station_coordination_policies("rf_conflict")
    assert len(policies) == 1
    policy = policies[0]
    assert int(policy["source_device_id"]) == min(int(first["id"]), int(second["id"]))
    assert int(policy["target_device_id"]) == max(int(first["id"]), int(second["id"]))
    assert policy["trigger"]["antenna_groups"] == ["ANT-1"]
    assert policy["trigger"]["amplifier_groups"] == []
    assert policy["trigger"]["frontend_groups"] == []
    assert policy["action"]["warning"] == "primary_runtime_rf_overlap"
    assert policy["safety_mode"] == "prompt"

    store.save_device_profile({"id": int(second["id"]), "antenna_group": ""})
    assert store.list_station_coordination_policies("rf_conflict") == []


def test_store_derives_blocking_band_overlap_policy_for_observer(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    tx = store.save_device_profile(
        {
            "name": "TX-A",
            "control_backend": "flrig",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    observer = store.save_device_profile(
        {
            "name": "SDR-A",
            "control_backend": "manual",
            "device_class": "observer",
            "band_overlap_guard_group": "north mast",
            "band_overlap_guard_mode": "warn",
        }
    )

    policies = store.list_station_coordination_policies("rf_conflict")

    assert len(policies) == 1
    policy = policies[0]
    assert int(policy["source_device_id"]) == min(int(tx["id"]), int(observer["id"]))
    assert int(policy["target_device_id"]) == max(int(tx["id"]), int(observer["id"]))
    assert policy["trigger"]["band_overlap_groups"] == ["NORTH MAST"]
    assert policy["safety_mode"] == "block"
    assert policy["action"]["guard_mode"] == "block"


def test_store_does_not_create_self_rf_policy_for_radio_with_both_overlap_guards(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    store.save_device_profile(
        {
            "name": "Protected Radio",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )

    assert store.list_station_coordination_policies("rf_conflict") == []


def test_store_derives_one_rf_policy_for_two_radios_with_both_overlap_guards(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Left Radio",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    right = store.save_device_profile(
        {
            "name": "Right Radio",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "warn",
            "advanced_frequency_guard_window_hz": 1500,
        }
    )

    policies = store.list_station_coordination_policies("rf_conflict")

    assert len(policies) == 1
    policy = policies[0]
    assert int(policy["source_device_id"]) == min(int(left["id"]), int(right["id"]))
    assert int(policy["target_device_id"]) == max(int(left["id"]), int(right["id"]))
    assert int(policy["source_device_id"]) != int(policy["target_device_id"])
    assert policy["trigger"]["band_overlap_groups"] == ["NORTH MAST"]
    assert policy["trigger"]["advanced_frequency_groups"] == ["RX FRONTEND"]
    assert policy["trigger"]["advanced_frequency_windows_hz"] == {
        str(left["id"]): 3000,
        str(right["id"]): 1500,
    }
    assert policy["safety_mode"] == "block"


def test_store_derives_warn_band_overlap_policy_when_both_radios_warn(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    first = store.save_device_profile(
        {
            "name": "TX-A",
            "control_backend": "flrig",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    second = store.save_device_profile(
        {
            "name": "SDR-A",
            "control_backend": "manual",
            "device_class": "observer",
            "band_overlap_guard_group": "north mast",
            "band_overlap_guard_mode": "warn",
        }
    )

    policies = store.list_station_coordination_policies("rf_conflict")

    assert len(policies) == 1
    policy = policies[0]
    assert int(policy["source_device_id"]) == min(int(first["id"]), int(second["id"]))
    assert int(policy["target_device_id"]) == max(int(first["id"]), int(second["id"]))
    assert policy["trigger"]["band_overlap_groups"] == ["NORTH MAST"]
    assert policy["safety_mode"] == "warn"
    assert policy["action"]["guard_mode"] == "warn"


def test_station_runtime_manager_reports_rf_conflict_context(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
            "antenna_group": "ANT-1",
            "amplifier_group": "AMP-MAIN",
        }
    )
    secondary = store.save_device_profile(
        {
            "name": "Remote Rig",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
            "antenna_group": "ant-1",
            "frontend_group": "FRONT-A",
        }
    )
    store.set_device_profile_runtime_active(int(primary["id"]), True)
    store.set_device_profile_runtime_active(int(secondary["id"]), True)
    store.set_runtime_primary_device_profile(int(primary["id"]))

    default_primary = next(
        row for row in store.list_device_profiles() if str(row.get("system_key", "") or "") == "default_device"
    )
    if int(default_primary.get("id", 0) or 0) != int(primary["id"]) and int(default_primary.get("runtime_active", 0) or 0) == 1:
        store.set_device_profile_runtime_active(int(default_primary["id"]), False)

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 7_074_000 if int(self.profile.get("id", 0) or 0) == int(secondary["id"]) else 7_078_000,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)

    primary_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(primary["id"]))
    secondary_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(secondary["id"]))

    assert primary_snapshot.antenna_group == "ANT-1"
    assert primary_snapshot.amplifier_group == "AMP-MAIN"
    assert secondary_snapshot.frontend_group == "FRONT-A"
    assert secondary_snapshot.current_frequency_label == "7.074 MHz"

    conflict = manager.evaluate_primary_rf_conflict(
        target_band="40M",
        target_frequency_hz=7_078_000,
        source="HF",
        force=True,
    )
    assert conflict is not None
    assert conflict.peer_name == "Remote Rig"
    assert conflict.same_band is True
    assert conflict.same_frequency is False
    assert conflict.shared_antenna_groups == ["ANT-1"]
    assert "RF conflict" in conflict.summary


def test_station_runtime_manager_marks_band_overlap_conflict_blocked(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "runtime_primary": 1,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    receive_only = store.save_operating_profile({"name": "Observer Model", "receive_only": 1})
    secondary = store.save_device_profile(
        {
            "name": "Observer SDR",
            "control_backend": "manual",
            "device_class": "observer",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    store.set_device_operating_profile(int(secondary["id"]), int(receive_only["id"]))
    store.set_device_profile_runtime_active(int(secondary["id"]), True)
    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 7_074_000 if int(self.profile.get("id", 0) or 0) == int(secondary["id"]) else 14_070_000,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()

    conflict = manager.evaluate_primary_rf_conflict(target_band="40M", target_frequency_hz=7_078_000, source="HF", force=True)

    assert conflict is not None
    assert conflict.blocked is True
    assert conflict.guard_mode == "block"
    assert conflict.shared_band_overlap_groups == ["NORTH MAST"]
    assert "band-overlap guard NORTH MAST" in conflict.summary


def test_station_runtime_manager_preserves_warn_band_overlap_conflict(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "runtime_primary": 1,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    receive_only = store.save_operating_profile({"name": "Observer Model", "receive_only": 1})
    secondary = store.save_device_profile(
        {
            "name": "Observer SDR",
            "control_backend": "manual",
            "device_class": "observer",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    store.set_device_operating_profile(int(secondary["id"]), int(receive_only["id"]))
    store.set_device_profile_runtime_active(int(secondary["id"]), True)
    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 7_074_000 if int(self.profile.get("id", 0) or 0) == int(secondary["id"]) else 14_070_000,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()

    conflict = manager.evaluate_primary_rf_conflict(target_band="40M", target_frequency_hz=7_078_000, source="HF", force=True)

    assert conflict is not None
    assert conflict.blocked is False
    assert conflict.guard_mode == "warn"
    assert conflict.shared_band_overlap_groups == ["NORTH MAST"]


def test_station_runtime_manager_blocks_when_shared_peer_frequency_unknown(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "runtime_primary": 1,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    secondary = store.save_device_profile(
        {
            "name": "Unverified Peer",
            "control_backend": "manual",
            "runtime_primary": 0,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    store.set_runtime_primary_device_profile(int(primary["id"]))
    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: None
        if int(self.profile.get("id", 0) or 0) == int(secondary["id"])
        else 7_078_000,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()

    conflict = manager.evaluate_primary_rf_conflict(
        target_band="40M",
        target_frequency_hz=7_078_000,
        source="HF",
        force=True,
    )

    assert conflict is not None
    assert conflict.blocked is True
    assert conflict.guard_mode == "block"
    assert conflict.peer_name == "Unverified Peer"
    assert conflict.peer_frequency_hz is None
    assert conflict.peer_status_unknown is True
    assert "unverified peer tuning" in conflict.summary
    assert "cannot verify that radio's current frequency" in conflict.detail
    assert conflict.shared_band_overlap_groups == ["NORTH MAST"]


def test_station_runtime_manager_warns_when_shared_peer_frequency_unknown_and_guard_warn(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "runtime_primary": 1,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    secondary = store.save_device_profile(
        {
            "name": "Unverified Peer",
            "control_backend": "manual",
            "runtime_primary": 0,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    store.set_runtime_primary_device_profile(int(primary["id"]))
    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: None
        if int(self.profile.get("id", 0) or 0) == int(secondary["id"])
        else 7_078_000,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()

    conflict = manager.evaluate_primary_rf_conflict(
        target_band="40M",
        target_frequency_hz=7_078_000,
        source="HF",
        force=True,
    )

    assert conflict is not None
    assert conflict.blocked is False
    assert conflict.guard_mode == "warn"
    assert conflict.peer_status_unknown is True


def test_station_runtime_manager_blocks_when_shared_peer_frequency_stale(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "runtime_primary": 1,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    secondary = store.save_device_profile(
        {
            "name": "Stale Peer",
            "control_backend": "manual",
            "runtime_primary": 0,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    store.set_runtime_primary_device_profile(int(primary["id"]))
    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 3_585_000
        if int(self.profile.get("id", 0) or 0) == int(secondary["id"])
        else 7_078_000,
    )

    def _latest_snapshot(self, key):
        if str(key) == f"device:{int(secondary['id'])}:frequency":
            return RadioStatusSnapshot(
                radio_id=str(key),
                generated_at=0.0,
                frequency_hz=3_585_000,
                stale=True,
                source="cache",
            )
        return None

    monkeypatch.setattr("freqinout.core.radio_status_poll_coordinator.RadioStatusPollCoordinator.latest_snapshot", _latest_snapshot)

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()

    conflict = manager.evaluate_primary_rf_conflict(
        target_band="40M",
        target_frequency_hz=7_078_000,
        source="HF",
        force=True,
    )

    assert conflict is not None
    assert conflict.blocked is True
    assert conflict.peer_status_unknown is True
    assert conflict.peer_status_stale is True
    assert conflict.peer_status_detail == "last peer frequency check is stale"
    assert "Peer status: last peer frequency check is stale" in conflict.detail


def test_station_runtime_manager_leads_with_known_conflict_when_another_peer_is_unknown(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "runtime_primary": 1,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    known = store.save_device_profile(
        {
            "name": "Known Peer",
            "control_backend": "manual",
            "runtime_primary": 0,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    unknown = store.save_device_profile(
        {
            "name": "A Unknown Peer",
            "control_backend": "manual",
            "runtime_primary": 0,
            "runtime_active": 1,
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    store.set_runtime_primary_device_profile(int(primary["id"]))
    store.set_device_profile_runtime_active(int(known["id"]), True)
    store.set_device_profile_runtime_active(int(unknown["id"]), True)
    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)

    def _frequency(self, force=False):
        device_id = int(self.profile.get("id", 0) or 0)
        if device_id == int(known["id"]):
            return 7_110_000
        if device_id == int(unknown["id"]):
            return None
        return 7_078_000

    monkeypatch.setattr(DeviceRuntime, "current_frequency_hz", _frequency)

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()

    conflict = manager.evaluate_primary_rf_conflict(
        target_band="40M",
        target_frequency_hz=7_078_000,
        source="HF",
        force=True,
    )

    assert conflict is not None
    assert conflict.peer_name == "Known Peer"
    assert conflict.peer_status_unknown is True
    assert "Known Peer" in conflict.summary
    assert "Other active peers: A Unknown Peer" in conflict.detail
    assert "Peer status: peer status is unknown" in conflict.detail


def test_station_runtime_manager_blocks_advanced_close_frequency_guard(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "runtime_primary": 1,
            "runtime_active": 1,
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    receive_only = store.save_operating_profile({"name": "Observer Model", "receive_only": 1})
    secondary = store.save_device_profile(
        {
            "name": "Observer SDR",
            "control_backend": "manual",
            "device_class": "observer",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "warn",
            "advanced_frequency_guard_window_hz": 1500,
        }
    )
    store.set_device_operating_profile(int(secondary["id"]), int(receive_only["id"]))
    store.set_device_profile_runtime_active(int(secondary["id"]), True)
    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 7_078_000 if int(self.profile.get("id", 0) or 0) == int(secondary["id"]) else 14_070_000,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()

    conflict = manager.evaluate_primary_rf_conflict(target_band="40M", target_frequency_hz=7_079_500, source="QSY", force=True)

    assert conflict is not None
    assert conflict.blocked is True
    assert conflict.guard_mode == "block"
    assert conflict.shared_advanced_frequency_groups == ["RX FRONTEND"]
    assert conflict.advanced_frequency_window_hz == 3000
    assert conflict.frequency_delta_hz == 1500
    assert "within 1500 Hz" in conflict.summary
    assert "advanced guard RX FRONTEND within 3000 Hz" in conflict.detail


def test_settings_tab_persists_rf_resource_groups(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = _qapplication_or_skip()

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        tab._persist_device_profile(
            {
                "name": "RF Shared Rig",
                "control_backend": "flrig",
                "flrig_host": "127.0.0.1",
                "flrig_port": 12355,
                "fldigi_host": "127.0.0.1",
                "fldigi_port": 7362,
                "js8_host": "127.0.0.1",
                "js8_port": 2442,
                "launch_enabled": True,
                "launch_path": "",
                "ptt_group": "AMP-A",
                "antenna_group": "ANT-1",
                "frontend_group": "FRONT-A",
                "amplifier_group": "AMP-MAIN",
                "notes": "Shared RF chain",
            }
        )
        saved = next(row for row in store.list_device_profiles() if row["name"] == "RF Shared Rig")
        assert saved["antenna_group"] == "ANT-1"
        assert saved["frontend_group"] == "FRONT-A"
        assert saved["amplifier_group"] == "AMP-MAIN"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_manual_qsy_prompts_before_rf_conflict_proceed(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    class _Scheduler:
        def __init__(self) -> None:
            self.apply_calls: list[bool] = []

        def evaluate_coordination_conflict(self, entry, source="QSY", force=False):
            return {
                "warning": True,
                "summary": "RF conflict: Remote Rig on same band 40M via antenna ANT-1.",
                "detail": "Target 7.078 MHz 40M overlaps Remote Rig at 7.074 MHz 40M.",
            }

        def get_status_summary(self):
            return {"shared_ptt_blocked": False}

        def apply_manual_qsy(self, entry, ignore_coordination_prompt=False):
            self.apply_calls.append(bool(ignore_coordination_prompt))

    class _Window:
        def __init__(self) -> None:
            self.scheduler = _Scheduler()

    class _FakeMessageBox:
        AcceptRole = 0
        RejectRole = 1

        def __init__(self, parent=None):
            self._clicked = None
            self._proceed = None

        @staticmethod
        def warning(*args, **kwargs):
            return None

        def setWindowTitle(self, title):
            self.title = title

        def setText(self, text):
            self.text = text

        def setInformativeText(self, text):
            self.detail = text

        def addButton(self, label, role):
            button = object()
            if label == "Proceed QSY":
                self._proceed = button
            return button

        def exec(self):
            self._clicked = self._proceed

        def clickedButton(self):
            return self._clicked

    monkeypatch.setattr(qsy_helper, "QMessageBox", _FakeMessageBox)

    window = _Window()
    result = qsy_helper.perform_qsy(window, {"freq": 7.078, "band": "40M", "mode": "Digi"})

    assert result is True
    assert window.scheduler.apply_calls == [True]


def test_scheduler_emits_coordination_conflict_prompt_once(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()

    class _Rig:
        def get_vfo_frequency(self):
            return 7_074_000

        def get_ptt(self):
            return False

    class _Manager:
        def evaluate_primary_rf_conflict(self, *, target_band="", target_frequency_hz=None, source="", force=False):
            return SimpleNamespace(
                peer_device_profile_id=22,
                peer_name="Remote Rig",
                peer_band="40M",
                peer_frequency_hz=7_074_000,
                target_band=target_band,
                target_frequency_hz=target_frequency_hz,
                same_band=True,
                same_frequency=False,
                shared_antenna_groups=["ANT-1"],
                shared_amplifier_groups=[],
                shared_frontend_groups=[],
                summary="RF conflict: Remote Rig on same band 40M via antenna ANT-1.",
                detail="Target 7.078 MHz 40M overlaps Remote Rig at 7.074 MHz 40M.",
                signature="1|HF|40M|7078000|22|ANT-1||",
            )

    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None, station_runtime_manager=_Manager())
    try:
        emitted: list[dict[str, object]] = []
        queued: list[tuple[str, int]] = []
        engine.coordination_conflict_detected.connect(lambda payload: emitted.append(dict(payload)))
        monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
        monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)

        entry = {"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"}
        engine._apply_schedule_entry(entry, "HF")
        status = engine.get_status_summary()
        engine._apply_schedule_entry(entry, "HF")

        assert len(emitted) == 1
        assert queued == []
        assert status["rf_conflict_warning"] is True
        assert "Remote Rig" in str(status["rf_conflict_summary"])
    finally:
        engine.stop()
