from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QCheckBox, QMessageBox

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import DeviceRuntime, StationRuntimeManager
from freqinout.gui.qsy_helper import perform_qsy, perform_qsy_with_hold


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

    default_primary = next(row for row in store.list_device_profiles() if str(row.get("system_key", "") or "") == "default_device")
    if int(default_primary.get("runtime_active", 0) or 0) == 1:
        store.set_device_profile_runtime_active(int(default_primary["id"]), False)

    monkeypatch.setattr(
        SoftwareStatusService,
        "status_snapshot",
        lambda self, **kwargs: {
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
        },
    )
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

    default_primary = next(row for row in store.list_device_profiles() if str(row.get("system_key", "") or "") == "default_device")
    if int(default_primary.get("runtime_active", 0) or 0) == 1:
        store.set_device_profile_runtime_active(int(default_primary["id"]), False)

    monkeypatch.setattr(
        SoftwareStatusService,
        "status_snapshot",
        lambda self, **kwargs: {
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
        },
    )
    freq_map = {
        int(primary["id"]): 7_078_000,
        int(secondary["id"]): 7_074_000,
    }
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: freq_map.get(int(self.profile.get("id", 0) or 0)),
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)
    primary_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(primary["id"]))
    secondary_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(secondary["id"]))

    assert primary_snapshot.current_band == "40M"
    assert primary_snapshot.antenna_group == "ANT-1"
    assert primary_snapshot.amplifier_group == "AMP-MAIN"
    assert secondary_snapshot.current_frequency_label == "7.074 MHz"
    assert secondary_snapshot.frontend_group == "FRONT-A"

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


def test_store_temporary_profile_swap_carries_primary_profile_and_restores(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    portable = store.save_operating_profile(
        {
            "name": "Portable Ops",
            "allow_profile_swap": True,
            "scheduler_enabled": False,
            "use_map": False,
            "use_messages": False,
            "use_background_ingest": False,
            "use_launch_control": False,
            "use_net_control_tabs": False,
        }
    )
    store.set_device_operating_profile(int(primary["id"]), int(portable["id"]), assignment_state="active", reason="Primary portable")

    secondary = store.save_device_profile(
        {
            "name": "Remote Rig",
            "control_backend": "rigctld",
            "rig_host": "127.0.0.1",
            "rig_port": 4532,
        }
    )
    store.set_device_profile_runtime_active(int(secondary["id"]), True)

    target_before = store.get_effective_assignment_for_device(int(secondary["id"]))
    assert target_before is not None
    target_before_id = int(target_before["operating_profile_id"])

    active_swap = store.start_temporary_profile_swap(
        int(secondary["id"]),
        mode="carry_primary_profile",
        reason="Shift to remote rig",
    )

    assert active_swap is not None
    assert active_swap["mode"] == "carry_primary_profile"
    assert active_swap["source_device_name"] == str(primary["name"])
    assert active_swap["target_device_name"] == "Remote Rig"
    assert store.get_runtime_primary_device_profile()["id"] == secondary["id"]

    target_after = store.get_effective_assignment_for_device(int(secondary["id"]))
    assert target_after is not None
    assert int(target_after["operating_profile_id"]) == int(portable["id"])
    assert target_after["assignment_state"] == "temporary_override"

    with pytest.raises(ValueError):
        store.set_device_profile_runtime_active(int(primary["id"]), False)
    with pytest.raises(ValueError):
        store.delete_operating_profile(target_before_id)

    restored = store.restore_temporary_profile_swap()
    assert int(restored["enabled"]) == 0
    assert store.get_active_profile_swap() is None
    assert int(store.get_runtime_primary_device_profile()["id"]) == int(primary["id"])

    target_restored = store.get_effective_assignment_for_device(int(secondary["id"]))
    assert target_restored is not None
    assert int(target_restored["operating_profile_id"]) == target_before_id
    assert target_restored["assignment_state"] == target_before["assignment_state"]


def test_station_runtime_manager_reports_temporary_profile_swap(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    portable = store.save_operating_profile(
        {
            "name": "Portable Ops",
            "allow_profile_swap": True,
            "scheduler_enabled": False,
            "use_map": False,
            "use_messages": False,
            "use_background_ingest": False,
            "use_launch_control": False,
            "use_net_control_tabs": False,
        }
    )
    store.set_device_operating_profile(int(primary["id"]), int(portable["id"]), assignment_state="active", reason="Portable primary")

    secondary = store.save_device_profile(
        {
            "name": "Remote Rig",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
        }
    )
    store.set_device_profile_runtime_active(int(secondary["id"]), True)
    store.start_temporary_profile_swap(int(secondary["id"]), mode="carry_primary_profile", reason="Field rotation")

    monkeypatch.setattr(
        SoftwareStatusService,
        "status_snapshot",
        lambda self, **kwargs: {
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
        },
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)
    source_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(primary["id"]))
    target_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(secondary["id"]))

    assert source_snapshot.swap_role == "source"
    assert "restore returns the primary shell" in source_snapshot.swap_summary
    assert target_snapshot.swap_role == "target"
    assert "Portable Ops" in target_snapshot.swap_summary

    policy = manager.primary_runtime_policy()
    assert policy["swap_active"] is True
    assert "Temporary swap active" in str(policy["swap_summary"])


def test_settings_tab_persists_and_shows_ptt_group(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
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
                "antenna_group": "ANT-1",
                "frontend_group": "FRONT-A",
                "amplifier_group": "AMP-MAIN",
                "notes": "Shared transmitter chain",
            }
        )
        saved = next(row for row in store.list_device_profiles() if row["name"] == "Shared TX")
        assert saved["ptt_group"] == "AMP-A"
        assert saved["antenna_group"] == "ANT-1"
        assert saved["frontend_group"] == "FRONT-A"
        assert saved["amplifier_group"] == "AMP-MAIN"

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


def test_settings_tab_starts_and_restores_temporary_profile_swap(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    portable = store.save_operating_profile(
        {
            "name": "Portable Ops",
            "allow_profile_swap": True,
            "scheduler_enabled": False,
            "use_map": False,
            "use_messages": False,
            "use_background_ingest": False,
            "use_launch_control": False,
            "use_net_control_tabs": False,
        }
    )
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    store.set_device_operating_profile(int(primary["id"]), int(portable["id"]), assignment_state="active", reason="Portable primary")

    secondary = store.save_device_profile(
        {
            "name": "Remote Rig",
            "control_backend": "rigctld",
            "rig_host": "127.0.0.1",
            "rig_port": 4532,
        }
    )
    store.set_device_profile_runtime_active(int(secondary["id"]), True)

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr(
        QMessageBox,
        "warning",
        lambda *args, **kwargs: pytest.fail(f"Unexpected warning dialog: {args[2] if len(args) > 2 else ''}"),
    )
    monkeypatch.setattr(
        QMessageBox,
        "information",
        lambda *args, **kwargs: pytest.fail(f"Unexpected information dialog: {args[2] if len(args) > 2 else ''}"),
    )

    tab = SettingsTab()
    try:
        _select_assignment_devices(tab, [int(secondary["id"])])
        monkeypatch.setattr(
            tab,
            "_open_temporary_profile_swap_dialog",
            lambda row: {
                "mode": "carry_primary_profile",
                "reason": "Swap to remote rig",
                "ends_utc": "",
            },
        )
        tab._start_temporary_profile_swap()

        active_swap = store.get_active_profile_swap()
        assert active_swap is not None
        assert active_swap["target_device_name"] == "Remote Rig"
        assert int(store.get_runtime_primary_device_profile()["id"]) == int(secondary["id"])

        tab._restore_temporary_profile_swap()

        assert store.get_active_profile_swap() is None
        assert int(store.get_runtime_primary_device_profile()["id"]) == int(primary["id"])
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

    monkeypatch.setattr(QMessageBox, "warning", lambda *args: warnings.append(str(args[2])) if len(args) > 2 else None)

    window = _Window()
    settings = SettingsManager()
    mins = perform_qsy_with_hold(window, settings, {"freq": 7.078, "band": "40M", "mode": "Digi"}, 30)

    assert mins == 0
    assert window.scheduler.apply_calls == 0
    assert warnings
    assert "AMP-A" in warnings[0]


def test_manual_qsy_prompts_before_rf_conflict_proceed(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    class _Scheduler:
        def __init__(self) -> None:
            self.apply_calls = []

        def evaluate_coordination_conflict(self, entry, source="QSY", force=False):
            return {
                "warning": True,
                "summary": "RF conflict: Remote Rig on same band 40M via antenna ANT-1.",
                "detail": "Target 7.078 MHz 40M overlaps Remote Rig at 7.074 MHz 40M.",
            }

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
            elif label == "Cancel":
                self._cancel = button
            return button

        def exec(self):
            self._clicked = self._proceed

        def clickedButton(self):
            return self._clicked

    monkeypatch.setattr("freqinout.gui.qsy_helper.QMessageBox", _FakeMessageBox)

    window = _Window()
    ok = perform_qsy(window, {"freq": 7.078, "band": "40M", "mode": "Digi"})

    assert ok is True
    assert window.scheduler.apply_calls == [True]
