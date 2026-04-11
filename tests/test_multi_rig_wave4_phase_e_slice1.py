from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QMessageBox

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import DeviceRuntime, StationRuntimeManager
from freqinout.gui.qsy_helper import perform_qsy_with_hold


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


def test_store_derives_shared_ptt_policies_from_device_groups(monkeypatch, tmp_path):
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
            "ptt_group": "AMP-A",
        }
    )
    second = store.save_device_profile(
        {
            "name": "TX-B",
            "control_backend": "rigctld",
            "rig_host": "127.0.0.1",
            "rig_port": 4532,
            "ptt_group": " amp-a ",
        }
    )
    store.save_device_profile(
        {
            "name": "Observer",
            "control_backend": "manual",
            "device_class": "observer",
            "ptt_group": "AMP-A",
        }
    )

    policies = store.list_station_coordination_policies("shared_ptt")
    assert len(policies) == 1
    policy = policies[0]
    assert int(policy["source_device_id"]) == min(int(first["id"]), int(second["id"]))
    assert int(policy["target_device_id"]) == max(int(first["id"]), int(second["id"]))
    assert policy["trigger"]["ptt_group"] == "AMP-A"
    assert policy["action"]["interlock"] == "block_primary_frequency_control"
    assert policy["safety_mode"] == "auto"

    store.save_device_profile({"id": int(second["id"]), "ptt_group": ""})
    assert store.list_station_coordination_policies("shared_ptt") == []


def test_station_runtime_manager_reports_shared_ptt_lock(monkeypatch, tmp_path):
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
            "ptt_group": "AMP-A",
        }
    )
    secondary = store.save_device_profile(
        {
            "name": "Remote Rig",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
            "ptt_group": "AMP-A",
        }
    )
    store.set_device_profile_runtime_active(int(primary["id"]), True)
    store.set_device_profile_runtime_active(int(secondary["id"]), True)
    store.set_runtime_primary_device_profile(int(primary["id"]))

    default_primary = next(
        row for row in store.list_device_profiles() if str(row.get("system_key", "") or "") == "default_device"
    )
    if int(default_primary.get("runtime_active", 0) or 0) == 1:
        store.set_device_profile_runtime_active(int(default_primary["id"]), False)

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "ptt_active",
        lambda self, force=False: int(self.profile.get("id", 0) or 0) == int(secondary["id"]),
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)

    primary_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(primary["id"]))
    secondary_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(secondary["id"]))

    assert primary_snapshot.ptt_group == "AMP-A"
    assert primary_snapshot.shared_ptt_blocked is True
    assert primary_snapshot.shared_ptt_owner_name == "Remote Rig"
    assert "blocked by Remote Rig" in primary_snapshot.shared_ptt_status_text
    assert secondary_snapshot.ptt_active is True
    assert "keyed here" in secondary_snapshot.shared_ptt_status_text

    lock = manager.shared_ptt_lock_snapshot(force=True)
    assert lock.blocked is True
    assert lock.ptt_group == "AMP-A"
    assert lock.owner_name == "Remote Rig"


def test_settings_tab_persists_and_shows_ptt_group(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        tab._persist_device_profile(
            {
                "name": "Shared TX",
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
                "notes": "Shared transmitter chain",
            }
        )
        saved = next(row for row in store.list_device_profiles() if row["name"] == "Shared TX")
        assert saved["ptt_group"] == "AMP-A"

        tab._refresh_device_profiles_table()
        row_index = next(
            row
            for row in range(tab.device_profiles_table.rowCount())
            if int(tab.device_profiles_table.item(row, 3).data(Qt.UserRole) or 0) == int(saved["id"])
        )
        assert tab.device_profiles_table.item(row_index, 8).text() == "AMP-A"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_manual_qsy_with_hold_aborts_when_shared_ptt_is_blocked(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    warnings: list[str] = []

    class _Scheduler:
        def __init__(self) -> None:
            self.apply_calls = 0

        def get_status_summary(self):
            return {
                "shared_ptt_blocked": True,
                "shared_ptt_group": "AMP-A",
                "shared_ptt_owner_name": "Remote Rig",
                "shared_ptt_reason": "Shared PTT group AMP-A is in use by Remote Rig.",
            }

        def apply_manual_qsy(self, entry):
            self.apply_calls += 1

    class _Window:
        def __init__(self) -> None:
            self.scheduler = _Scheduler()

    monkeypatch.setattr(
        QMessageBox,
        "warning",
        lambda *args: warnings.append(str(args[2])) if len(args) > 2 else QMessageBox.Ok,
    )

    window = _Window()
    settings = SettingsManager()
    mins = perform_qsy_with_hold(window, settings, {"freq": 7.078, "band": "40M", "mode": "Digi"}, 30)

    assert mins == 0
    assert window.scheduler.apply_calls == 0
    assert warnings
    assert "AMP-A" in warnings[0]
