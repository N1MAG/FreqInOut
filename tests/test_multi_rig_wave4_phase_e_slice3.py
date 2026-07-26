from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QCheckBox, QMessageBox
import pytest

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import StationRuntimeManager


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


def _create_active_device(store: MultiRadioStore, name: str) -> dict[str, object]:
    device = store.save_device_profile(
        {
            "name": name,
            "control_backend": "manual",
            "launch_enabled": False,
            "launch_path": "",
            "notes": f"{name} profile",
        }
    )
    store.set_device_profile_runtime_active(int(device["id"]), True)
    return device


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


def test_store_temporary_swap_use_target_profile_restores_primary_and_target_assignment(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    source = store.get_runtime_primary_device_profile()
    assert source is not None

    target_profile = store.save_operating_profile({"name": "Remote Ops"})
    target = _create_active_device(store, "Remote Rig")
    store.set_device_operating_profile(
        int(target["id"]),
        int(target_profile["id"]),
        assignment_state="active",
        reason="Remote default",
    )
    target_before = store.get_effective_assignment_for_device(int(target["id"]))
    assert target_before is not None

    policy = store.start_temporary_profile_swap(
        int(target["id"]),
        mode="use_target_profile",
        reason="Storm watch",
    )

    assert policy["mode"] == "use_target_profile"
    assert int(policy["source_device_id"]) == int(source["id"])
    assert int(policy["target_device_id"]) == int(target["id"])
    assert int(store.get_runtime_primary_device_profile()["id"]) == int(target["id"])

    target_during = store.get_effective_assignment_for_device(int(target["id"]))
    assert target_during is not None
    assert int(target_during["operating_profile_id"]) == int(target_before["operating_profile_id"])
    assert str(target_during["assignment_state"] or "") == str(target_before["assignment_state"] or "")

    restored = store.restore_temporary_profile_swap(reason="Back to base")
    target_after = store.get_effective_assignment_for_device(int(target["id"]))

    assert int(restored["enabled"]) == 0
    assert store.get_active_profile_swap() is None
    assert int(store.get_runtime_primary_device_profile()["id"]) == int(source["id"])
    assert target_after is not None
    assert int(target_after["operating_profile_id"]) == int(target_before["operating_profile_id"])
    assert str(target_after["assignment_state"] or "") == str(target_before["assignment_state"] or "")


def test_store_carry_swap_restores_target_assignment_and_blocks_unsafe_edits(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    source = store.get_runtime_primary_device_profile()
    assert source is not None

    source_profile = store.save_operating_profile({"name": "Field Carry", "allow_profile_swap": True})
    target_profile = store.save_operating_profile({"name": "Target Default"})
    store.set_device_operating_profile(
        int(source["id"]),
        int(source_profile["id"]),
        assignment_state="active",
        reason="Primary field profile",
    )
    target = _create_active_device(store, "Carry Target")
    store.set_device_operating_profile(
        int(target["id"]),
        int(target_profile["id"]),
        assignment_state="active",
        reason="Target base profile",
    )

    policy = store.start_temporary_profile_swap(
        int(target["id"]),
        mode="carry_primary_profile",
        reason="Carry the field shell",
    )
    target_during = store.get_effective_assignment_for_device(int(target["id"]))

    assert policy["mode"] == "carry_primary_profile"
    assert policy["applied_operating_profile_name"] == "Field Carry"
    assert target_during is not None
    assert int(target_during["operating_profile_id"]) == int(source_profile["id"])
    assert str(target_during["assignment_state"] or "") == "temporary_override"

    with pytest.raises(ValueError, match="Restore the active Temporary Plan Swap"):
        store.set_device_operating_profile(int(source["id"]), int(target_profile["id"]))
    with pytest.raises(ValueError, match="Restore the active Temporary Plan Swap"):
        store.set_device_profile_runtime_active(int(source["id"]), False)
    with pytest.raises(ValueError, match="Restore the active Temporary Plan Swap"):
        store.set_runtime_primary_device_profile(int(source["id"]))
    with pytest.raises(ValueError, match="restore target"):
        store.save_operating_profile({"id": int(target_profile["id"]), "enabled": False})
    with pytest.raises(ValueError, match="restore target"):
        store.delete_operating_profile(int(target_profile["id"]))

    store.restore_temporary_profile_swap()
    target_after = store.get_effective_assignment_for_device(int(target["id"]))

    assert store.get_active_profile_swap() is None
    assert int(store.get_runtime_primary_device_profile()["id"]) == int(source["id"])
    assert target_after is not None
    assert int(target_after["operating_profile_id"]) == int(target_profile["id"])
    assert str(target_after["assignment_state"] or "") == "active"


def test_station_runtime_manager_reports_active_profile_swap_annotations(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    source = store.get_runtime_primary_device_profile()
    assert source is not None

    source_profile = store.save_operating_profile({"name": "Field Carry", "allow_profile_swap": True})
    target_profile = store.save_operating_profile({"name": "Remote Default"})
    store.set_device_operating_profile(int(source["id"]), int(source_profile["id"]), assignment_state="active")
    target = _create_active_device(store, "Runtime Target")
    store.set_device_operating_profile(int(target["id"]), int(target_profile["id"]), assignment_state="active")
    store.start_temporary_profile_swap(int(target["id"]), mode="carry_primary_profile", reason="Runtime annotation")

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)

    source_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(source["id"]))
    target_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(target["id"]))
    policy = manager.primary_runtime_policy()

    assert target_snapshot.runtime_primary is True
    assert target_snapshot.swap_role == "target"
    assert "carrying Field Carry" in target_snapshot.swap_summary
    assert source_snapshot.swap_role == "source"
    assert "restore returns the primary shell" in source_snapshot.swap_summary
    assert policy["swap_active"] is True
    assert policy["swap_mode"] == "carry_primary_profile"
    assert policy["swap_target_name"] == "Runtime Target"
    assert "Temporary swap active" in str(policy["swap_summary"])


def test_settings_tab_persists_allow_profile_swap_and_handles_swap_start_restore(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    target = _create_active_device(store, "Settings Target")
    target_profile = store.save_operating_profile({"name": "Settings Target Profile"})
    store.set_device_operating_profile(int(target["id"]), int(target_profile["id"]), assignment_state="active")

    tab = SettingsTab()
    try:
        tab._persist_operating_profile({"name": "Carry Enabled", "allow_profile_swap": True})
        saved_profile = next(row for row in store.list_operating_profiles() if row["name"] == "Carry Enabled")
        assert int(saved_profile.get("allow_profile_swap", 0) or 0) == 1

        tab._refresh_multi_radio_tables()
        _select_assignment_devices(tab, [int(target["id"])])
        monkeypatch.setattr(
            tab,
            "_open_temporary_profile_swap_dialog",
            lambda target_row: {"mode": "use_target_profile", "reason": "Ops temp", "ends_utc": ""},
        )
        tab._start_temporary_profile_swap()

        active_swap = store.get_active_profile_swap()
        assert active_swap is not None
        assert int(active_swap["target_device_id"]) == int(target["id"])
        assert "Temporary plan swap active" in tab.device_assignments_hint_label.text()

        monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
        tab._restore_temporary_profile_swap()

        assert store.get_active_profile_swap() is None
    finally:
        tab.deleteLater()
        app.processEvents()
