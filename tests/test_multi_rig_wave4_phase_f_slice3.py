from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import StationRuntimeManager
import freqinout.core.station_runtime_manager as station_runtime_manager_mod


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


def _create_primary_device(store: MultiRadioStore, name: str = "Primary Rig") -> dict[str, object]:
    device = store.save_device_profile(
        {
            "name": name,
            "control_backend": "flrig",
            "device_class": "tx_rx",
            "launch_enabled": False,
            "launch_path": "",
            "runtime_active": 1,
            "runtime_primary": 1,
        }
    )
    store.set_device_profile_runtime_active(int(device["id"]), True)
    store.set_runtime_primary_device_profile(int(device["id"]))
    return store.get_runtime_primary_device_profile() or device


def _configure_varac_device(store: MultiRadioStore, device: dict[str, object], root: str) -> dict[str, object]:
    return store.save_device_profile(
        {
            "id": device["id"],
            "name": device["name"],
            "varac_install_path": root,
            "varac_db_path": f"{root}/VarAC.db",
            "varac_ini_path": f"{root}/VarAC.ini",
            "launch_cmd": f"{root}/VarAC.exe",
        }
    )


def test_varac_membership_enforces_unique_instance_and_one_enabled_membership_per_device(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = _configure_varac_device(
        store,
        store.get_runtime_primary_device_profile() or _create_primary_device(store),
        "C:/VarAC/Main",
    )
    second = _configure_varac_device(
        store,
        store.save_device_profile({"name": "Cluster Node B", "control_backend": "flrig"}),
        "C:/VarAC/B",
    )
    cluster_a = store.save_varac_cluster({"name": "Cluster A", "cluster_id": "CL-A"})
    cluster_b = store.save_varac_cluster({"name": "Cluster B", "cluster_id": "CL-B"})

    store.set_varac_cluster_member(int(cluster_a["id"]), int(primary["id"]), instance_number=1, enabled=True)

    with pytest.raises(ValueError, match="one enabled VarAC cluster membership"):
        store.set_varac_cluster_member(int(cluster_b["id"]), int(primary["id"]), instance_number=2, enabled=True)

    disabled_membership = store.set_varac_cluster_member(int(cluster_b["id"]), int(primary["id"]), instance_number=2, enabled=False)
    assert int(disabled_membership["enabled"] or 0) == 0

    with pytest.raises(ValueError, match="instance number 1"):
        store.set_varac_cluster_member(int(cluster_a["id"]), int(second["id"]), instance_number=1, enabled=True)


def test_gateway_handler_must_be_enabled_member_and_member_cannot_be_removed_or_disabled(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = _configure_varac_device(
        store,
        store.get_runtime_primary_device_profile() or _create_primary_device(store),
        "C:/VarAC/Main",
    )
    second = _configure_varac_device(
        store,
        store.save_device_profile({"name": "Cluster Node B", "control_backend": "flrig"}),
        "C:/VarAC/B",
    )
    third = _configure_varac_device(
        store,
        store.save_device_profile({"name": "Cluster Node C", "control_backend": "flrig"}),
        "C:/VarAC/C",
    )
    cluster = store.save_varac_cluster({"name": "Gateway Cluster", "cluster_id": "GW-A"})

    store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=True)
    store.set_varac_cluster_member(int(cluster["id"]), int(second["id"]), instance_number=2, enabled=True)

    with pytest.raises(ValueError, match="enabled member"):
        store.set_varac_cluster_gateway_handler(int(cluster["id"]), int(third["id"]))

    store.set_varac_cluster_gateway_handler(int(cluster["id"]), int(primary["id"]))

    with pytest.raises(ValueError, match="gateway handler"):
        store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=False)

    with pytest.raises(ValueError, match="gateway handler"):
        store.remove_varac_cluster_member(int(cluster["id"]), int(primary["id"]))

    policies = store.list_station_coordination_policies("gateway_exclusive")
    assert any(
        int(row.get("source_device_id", 0) or 0) == int(primary["id"])
        and int(row.get("target_device_id", 0) or 0) == int(second["id"])
        and row["action"]["gateway_handler_name"] == str(primary["name"])
        for row in policies
    )

    store.set_varac_cluster_gateway_handler(int(cluster["id"]), None)
    store.remove_varac_cluster_member(int(cluster["id"]), int(primary["id"]))
    remaining = store.list_varac_cluster_members(cluster_id=int(cluster["id"]))
    assert all(int(row.get("device_profile_id", 0) or 0) != int(primary["id"]) for row in remaining)


def test_station_runtime_manager_warns_for_missing_shared_db_and_missing_gateway(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = _configure_varac_device(
        store,
        store.get_runtime_primary_device_profile() or _create_primary_device(store),
        "C:/VarAC/Main",
    )
    second = _configure_varac_device(
        store,
        store.save_device_profile({"name": "Cluster Node B", "control_backend": "flrig"}),
        "C:/VarAC/B",
    )
    store.set_device_profile_runtime_active(int(second["id"]), True)

    cluster = store.save_varac_cluster({"name": "Warn Cluster", "cluster_id": "WARN-A"})
    store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=True)
    store.set_varac_cluster_member(int(cluster["id"]), int(second["id"]), instance_number=2, enabled=True)

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _status_snapshot)
    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: name == "VarAC")
    monkeypatch.setattr(
        station_runtime_manager_mod.VarACStatusClient,
        "get_status",
        lambda self: {"busy": False, "waiting_for_frequency": False, "reason": None},
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)
    primary_snapshot = next(snap for snap in snapshots if snap.runtime_primary)
    secondary_snapshot = next(snap for snap in snapshots if snap.name == "Cluster Node B")

    assert primary_snapshot.service_states["VarAC Cluster"]["state"] == "warn"
    assert "shared db path not configured" in primary_snapshot.warning_text.lower()
    assert "gateway handler not selected" in primary_snapshot.varac_cluster_summary.lower()
    assert "gateway handler not selected" in secondary_snapshot.varac_cluster_summary.lower()
