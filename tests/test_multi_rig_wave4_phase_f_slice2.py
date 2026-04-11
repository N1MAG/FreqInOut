from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QCheckBox

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import StationRuntimeManager
import freqinout.core.station_runtime_manager as station_runtime_manager_mod


def _select_varac_clusters(tab, cluster_ids: list[int]) -> None:
    wanted = {int(cluster_id) for cluster_id in cluster_ids}
    found: set[int] = set()
    for row in range(tab.varac_clusters_table.rowCount()):
        widget = tab.varac_clusters_table.cellWidget(row, 0)
        chk = widget.findChild(QCheckBox) if widget is not None else None
        if chk is None:
            continue
        cluster_id = int(chk.property("varac_cluster_id") or 0)
        chk.setChecked(cluster_id in wanted)
        if cluster_id in wanted:
            found.add(cluster_id)
    assert found == wanted


def _select_varac_memberships(tab, cluster_device_pairs: list[tuple[int, int]]) -> None:
    wanted = {(int(cluster_id), int(device_id)) for cluster_id, device_id in cluster_device_pairs}
    found: set[tuple[int, int]] = set()
    for row in range(tab.varac_members_table.rowCount()):
        widget = tab.varac_members_table.cellWidget(row, 0)
        chk = widget.findChild(QCheckBox) if widget is not None else None
        if chk is None:
            continue
        pair = (int(chk.property("varac_cluster_id") or 0), int(chk.property("device_profile_id") or 0))
        chk.setChecked(pair in wanted)
        if pair in wanted:
            found.add(pair)
    assert found == wanted


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


def test_store_persists_varac_clusters_and_memberships(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    updated_primary = store.save_device_profile(
        {
            "id": primary["id"],
            "name": primary["name"],
            "varac_install_path": "C:/VarAC/Main",
            "varac_db_path": "C:/VarAC/Main/VarAC.db",
            "varac_ini_path": "C:/VarAC/Main/VarAC.ini",
            "launch_cmd": "C:/VarAC/Main/VarAC.exe",
        }
    )
    cluster = store.save_varac_cluster(
        {
            "name": "Home Cluster",
            "cluster_id": "HOME-A",
            "shared_db_path": str(tmp_path / "shared" / "VarAC.db"),
            "counters_refresh_sec": 20,
            "ptt_lock_enabled": True,
        }
    )
    membership = store.set_varac_cluster_member(
        int(cluster["id"]),
        int(updated_primary["id"]),
        instance_number=1,
        enabled=True,
    )

    clusters = store.list_varac_clusters()
    assert clusters[0]["name"] == "Home Cluster"
    assert int(clusters[0]["enabled_member_count"] or 0) == 1

    members = store.list_varac_cluster_members(cluster_id=int(cluster["id"]))
    assert len(members) == 1
    assert members[0]["cluster_public_id"] == "HOME-A"
    assert int(members[0]["instance_number"] or 0) == 1
    assert membership["device_name"] == updated_primary["name"]

    persisted_primary = store.get_device_profile(int(updated_primary["id"]))
    assert persisted_primary is not None
    assert int(persisted_primary["varac_cluster_member_enabled"] or 0) == 1


def test_station_runtime_manager_exposes_varac_cluster_summary(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    shared_db = tmp_path / "cluster" / "VarAC.db"
    shared_db.parent.mkdir(parents=True, exist_ok=True)
    shared_db.write_text("")

    store.save_device_profile(
        {
            "id": primary["id"],
            "name": primary["name"],
            "varac_install_path": "C:/VarAC/Main",
            "varac_db_path": "C:/VarAC/Main/VarAC.db",
            "varac_ini_path": "C:/VarAC/Main/VarAC.ini",
            "launch_cmd": "C:/VarAC/Main/VarAC.exe",
        }
    )
    cluster = store.save_varac_cluster(
        {
            "name": "Home Cluster",
            "cluster_id": "HOME-A",
            "shared_db_path": str(shared_db),
            "counters_refresh_sec": 15,
        }
    )
    store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=True)
    store.set_varac_cluster_gateway_handler(int(cluster["id"]), int(primary["id"]))

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

    assert primary_snapshot.varac_cluster_name == "Home Cluster"
    assert primary_snapshot.varac_gateway_handler is True
    assert "gateway handler" in primary_snapshot.varac_cluster_summary.lower()
    assert primary_snapshot.service_states["VarAC"]["state"] == "ok"
    assert primary_snapshot.service_states["VarAC Cluster"]["state"] == "ok"


def test_settings_tab_shows_varac_cluster_and_membership_tables(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    store.save_device_profile(
        {
            "id": primary["id"],
            "name": primary["name"],
            "varac_install_path": "C:/VarAC/Main",
            "varac_db_path": "C:/VarAC/Main/VarAC.db",
            "varac_ini_path": "C:/VarAC/Main/VarAC.ini",
        }
    )
    cluster = store.save_varac_cluster(
        {
            "name": "Home Cluster",
            "cluster_id": "HOME-A",
            "shared_db_path": str(tmp_path / "shared" / "VarAC.db"),
        }
    )
    store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=True)
    store.set_varac_cluster_gateway_handler(int(cluster["id"]), int(primary["id"]))

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        assert any(
            tab.varac_clusters_table.item(row, 1).text() == "Home Cluster"
            and tab.varac_clusters_table.item(row, 5).text() == str(primary["name"])
            for row in range(tab.varac_clusters_table.rowCount())
        )
        assert any(
            tab.varac_members_table.item(row, 1).text() == "Home Cluster"
            and tab.varac_members_table.item(row, 2).text() == str(primary["name"])
            and tab.varac_members_table.item(row, 7).text() == "Handler"
            for row in range(tab.varac_members_table.rowCount())
        )

        _select_varac_clusters(tab, [int(cluster["id"])])
        tab._update_varac_cluster_action_buttons()
        assert tab.edit_varac_cluster_btn.isEnabled() is True

        _select_varac_memberships(tab, [(int(cluster["id"]), int(primary["id"]))])
        tab._update_varac_membership_action_buttons()
        assert tab.remove_varac_membership_btn.isEnabled() is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_persists_device_profile_varac_fields(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    device = store.save_device_profile({"name": "South VarAC", "control_backend": "flrig"})

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        tab._persist_device_profile(
            {
                "id": device["id"],
                "name": device["name"],
                "control_backend": str(device.get("control_backend", "flrig") or "flrig"),
                "deployment_mode": str(device.get("deployment_mode", "full") or "full"),
                "enabled": True,
                "device_class": str(device.get("device_class", "tx_rx") or "tx_rx"),
                "flrig_host": "127.0.0.1",
                "flrig_port": 12345,
                "fldigi_host": "127.0.0.1",
                "fldigi_port": 7362,
                "js8_host": "127.0.0.1",
                "js8_port": 2442,
                "varac_install_path": "C:/VarAC/South",
                "varac_db_path": "C:/VarAC/South/VarAC.db",
                "varac_ini_path": "C:/VarAC/South/VarAC.ini",
                "launch_cmd": "C:/VarAC/South/VarAC.exe",
                "launch_enabled": True,
                "launch_path": "",
                "rig_host": "",
                "rig_port": None,
                "sdr_host": "",
                "sdr_port": None,
                "ptt_group": "",
                "antenna_group": "",
                "frontend_group": "",
                "amplifier_group": "",
                "notes": "",
            },
            existing=device,
        )
        updated = store.get_device_profile(int(device["id"]))
        assert updated is not None
        assert updated["varac_install_path"] == "C:/VarAC/South"
        assert updated["varac_db_path"] == "C:/VarAC/South/VarAC.db"
        assert updated["varac_ini_path"] == "C:/VarAC/South/VarAC.ini"
        assert updated["launch_cmd"] == "C:/VarAC/South/VarAC.exe"
    finally:
        tab.deleteLater()
        app.processEvents()
