from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from pathlib import Path

from freqinout.core.multi_radio_store import (
    MultiRadioStore,
    ensure_default_multi_radio_records,
    settings_db_path,
)
from freqinout.core.settings_manager import SettingsManager


def _insert_kv(db_path: Path, values: dict[str, object]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.executemany(
            "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
            [(key, json.dumps(value)) for key, value in values.items()],
        )
        conn.commit()
    finally:
        conn.close()


def test_settings_manager_seeds_default_multi_radio_records(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    devices = store.list_device_profiles()
    operating = store.list_operating_profiles()
    assignments = store.list_assignments()

    assert settings.db_path == settings_db_path()
    assert len(devices) == 1
    assert devices[0]["system_key"] == "default_device"
    assert devices[0]["name"] == "Default Radio"
    assert devices[0]["control_backend"] == "flrig"
    assert devices[0]["runtime_active"] == 1
    assert devices[0]["flrig_port"] == 12345
    assert devices[0]["js8_port"] == 2442
    assert len(operating) == 1
    assert operating[0]["system_key"] == "default_operating"
    assert operating[0]["scheduler_enabled"] == 1
    assert len(assignments) == 1
    assert assignments[0]["assignment_state"] == "active"
    assert assignments[0]["device_profile_id"] == devices[0]["id"]
    assert assignments[0]["operating_profile_id"] == operating[0]["id"]

    js8_instances = store.list_js8_instances()
    fast_light_configs = store.list_fast_light_configs()
    varac_nodes = store.list_varac_nodes()
    assert len(js8_instances) == 1
    assert len(fast_light_configs) == 1
    assert len(varac_nodes) == 1
    assert devices[0]["js8_instance_id"] == js8_instances[0]["id"]
    assert devices[0]["fast_light_config_id"] == fast_light_configs[0]["id"]
    assert devices[0]["varac_node_id"] == varac_nodes[0]["id"]


def test_default_multi_radio_seed_reflects_legacy_js8_control_settings(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(
        db_path,
        {
            "control_via": "JS8Call",
            "js8_host": "10.0.0.9",
            "js8_port": 2542,
            "flrig_port": 22345,
            "fldigi_port": 7367,
            "launch_control_enabled": False,
            "use_scheduler": False,
            "varac_path": "C:/VarAC",
            "varac_db_path": "C:/VarAC/VarAC.db",
        },
    )

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    devices = store.list_device_profiles()
    operating = store.list_operating_profiles()

    assert len(devices) == 1
    assert devices[0]["control_backend"] == "js8call"
    assert devices[0]["runtime_active"] == 1
    assert devices[0]["js8_host"] == "10.0.0.9"
    assert devices[0]["js8_port"] == 2542
    assert devices[0]["flrig_port"] == 22345
    assert devices[0]["fldigi_port"] == 7367
    assert devices[0]["launch_enabled"] == 0
    assert devices[0]["varac_install_path"] == "C:/VarAC"
    assert devices[0]["varac_db_path"] == "C:/VarAC/VarAC.db"
    assert len(operating) == 1
    assert operating[0]["scheduler_enabled"] == 0
    assert operating[0]["use_launch_control"] == 0


def test_default_multi_radio_seed_is_idempotent(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    ensure_default_multi_radio_records(settings._conn, settings.all())  # type: ignore[arg-type]
    ensure_default_multi_radio_records(settings._conn, settings.all())  # type: ignore[arg-type]

    store = MultiRadioStore(settings_db_path())
    assert len(store.list_device_profiles()) == 1
    assert len(store.list_operating_profiles()) == 1
    assert len(store.list_assignments()) == 1
    assert len([row for row in store.list_device_profiles() if int(row.get("runtime_active", 0) or 0) == 1]) == 1


def test_db_initializer_creates_multi_radio_settings_tables(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)
    db_initializer.ensure_all_tables()

    conn = sqlite3.connect(cfg_root / "config" / "freqinout.db")
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()

    assert "kv" in tables
    assert "device_profiles" in tables
    assert "operating_profiles" in tables
    assert "operating_profile_assignments" in tables
    assert "station_coordination_policies" in tables
    assert "varac_clusters" in tables
    assert "varac_cluster_members" in tables


def test_tool_db_schema_lists_multi_radio_settings_tables(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    tools_dir = Path(__file__).resolve().parents[1] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))

    import db_schema

    db_schema = importlib.reload(db_schema)

    assert "device_profiles" in db_schema.SETTINGS_TABLES
    assert "operating_profiles" in db_schema.SETTINGS_TABLES
    assert "operating_profile_assignments" in db_schema.SETTINGS_TABLES
    assert "station_coordination_policies" in db_schema.SETTINGS_TABLES
    assert "varac_clusters" in db_schema.SETTINGS_TABLES
    assert "varac_cluster_members" in db_schema.SETTINGS_TABLES


def test_store_switches_runtime_active_device_and_projects_legacy_settings(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    remote = store.save_device_profile(
        {
            "name": "Remote FLRig",
            "control_backend": "flrig",
            "deployment_mode": "minimal",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
            "fldigi_host": "10.0.0.9",
            "fldigi_port": 7364,
            "js8_host": "10.0.0.10",
            "js8_port": 2542,
            "js8_directed_path": "C:/JS8A/DIRECTED.TXT",
            "launch_enabled": False,
            "launch_path": "C:/Apps/FLRig.exe",
            "runtime_active": True,
        }
    )

    settings.reload()
    active = store.get_runtime_active_device_profile()
    assert active is not None
    assert active["id"] == remote["id"]
    assert settings.get("control_via") == "FLRig"
    assert settings.get("flrig_host") == "10.0.0.8"
    assert settings.get("flrig_port") == 22345
    assert settings.get("fldigi_host") == "10.0.0.9"
    assert settings.get("fldigi_port") == 7364
    assert settings.get("js8_host") == "10.0.0.10"
    assert settings.get("js8_port") == 2542
    assert settings.get("js8_directed_path") == "C:/JS8A/DIRECTED.TXT"
    assert settings.get("launch_control_enabled") is False
    assert settings.get("path_flrig") == "C:/Apps/FLRig.exe"
    assert len([row for row in store.list_device_profiles() if int(row.get("runtime_active", 0) or 0) == 1]) == 1


def test_settings_manager_mirrors_flat_settings_into_runtime_active_device(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    settings.set_many(
        {
            "control_via": "JS8Call",
            "flrig_host": "10.1.1.11",
            "flrig_port": 12444,
            "fldigi_host": "10.1.1.12",
            "fldigi_port": 7368,
            "js8_host": "10.1.1.13",
            "js8_port": 2550,
            "js8_directed_path": "C:/JS8Primary/DIRECTED.TXT",
            "launch_control_enabled": False,
            "path_js8call": "C:/Apps/JS8Call",
        }
    )

    store = MultiRadioStore(settings_db_path())
    active = store.get_runtime_active_device_profile()

    assert active is not None
    assert active["control_backend"] == "js8call"
    assert active["flrig_host"] == "10.1.1.11"
    assert active["flrig_port"] == 12444
    assert active["fldigi_host"] == "10.1.1.12"
    assert active["fldigi_port"] == 7368
    assert active["js8_host"] == "10.1.1.13"
    assert active["js8_port"] == 2550
    assert active["js8_directed_path"] == "C:/JS8Primary/DIRECTED.TXT"
    assert active["launch_enabled"] == 0
    assert active["launch_path"] == "C:/Apps/JS8Call"


def test_store_projects_linked_software_records_to_legacy_settings(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    js8 = store.save_js8_instance(
        {
            "name": "Field JS8",
            "host": "10.0.0.10",
            "port": 2542,
            "offset_hz": 2050,
            "directed_path": "C:/JS8Field/DIRECTED.TXT",
            "forms_path": "C:/JS8Field/forms",
            "install_path": "C:/Apps/JS8Field",
            "spotter_launch_path": "C:/Apps/JS8Spotter.exe",
            "commstat_launch_path": "C:/Apps/CommStat.exe",
        }
    )
    fast_light = store.save_fast_light_config(
        {
            "name": "Field FL",
            "flrig_path": "C:/Apps/FLRig.exe",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
            "fldigi_path": "C:/Apps/FLDigi.exe",
            "fldigi_host": "10.0.0.9",
            "fldigi_port": 7364,
            "fldigi_checkin_dir": "C:/Logs/checkins",
        }
    )
    varac = store.save_varac_node(
        {
            "name": "Field VarAC",
            "install_path": "C:/Apps/VarAC",
            "db_path": "C:/Apps/VarAC/VarAC.db",
            "ini_path": "C:/Apps/VarAC/VarAC.ini",
            "launch_cmd": "VarAC.exe",
            "incoming_path": "C:/Apps/VarAC/incoming",
        }
    )

    store.save_device_profile(
        {
            "name": "Linked Field Device",
            "control_backend": "flrig",
            "js8_instance_id": int(js8["id"]),
            "fast_light_config_id": int(fast_light["id"]),
            "varac_node_id": int(varac["id"]),
            "runtime_active": True,
        }
    )

    settings.reload()
    assert settings.get("flrig_host") == "10.0.0.8"
    assert settings.get("flrig_port") == 22345
    assert settings.get("path_flrig") == "C:/Apps/FLRig.exe"
    assert settings.get("path_fldigi") == "C:/Apps/FLDigi.exe"
    assert settings.get("fldigi_checkin_dir") == "C:/Logs/checkins"
    assert settings.get("js8_host") == "10.0.0.10"
    assert settings.get("js8_port") == 2542
    assert settings.get("js8_offset_hz") == 2050
    assert settings.get("js8_directed_path") == "C:/JS8Field/DIRECTED.TXT"
    assert settings.get("js8_forms_path") == "C:/JS8Field/forms"
    assert settings.get("path_js8spotter") == "C:/Apps/JS8Spotter.exe"
    assert settings.get("path_commstat") == "C:/Apps/CommStat.exe"
    assert settings.get("varac_path") == "C:/Apps/VarAC"
    assert settings.get("varac_db_path") == "C:/Apps/VarAC/VarAC.db"
    assert settings.get("varac_ini_path") == "C:/Apps/VarAC/VarAC.ini"
    assert settings.get("varac_launch_cmd") == "VarAC.exe"
    assert settings.get("message_paths", {}).get("varac") == "C:/Apps/VarAC/incoming"


def test_settings_manager_mirrors_flat_settings_into_linked_runtime_records(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    js8 = store.save_js8_instance({"name": "Bound JS8"})
    fast_light = store.save_fast_light_config({"name": "Bound FL"})
    varac = store.save_varac_node({"name": "Bound VarAC"})
    linked = store.save_device_profile(
        {
            "name": "Linked Primary",
            "control_backend": "js8call",
            "js8_instance_id": int(js8["id"]),
            "fast_light_config_id": int(fast_light["id"]),
            "varac_node_id": int(varac["id"]),
        }
    )
    store.set_runtime_active_device_profile(int(linked["id"]))

    settings = SettingsManager()
    settings.set_many(
        {
            "control_via": "JS8Call",
            "flrig_host": "10.1.1.11",
            "flrig_port": 12444,
            "fldigi_host": "10.1.1.12",
            "fldigi_port": 7368,
            "fldigi_checkin_dir": "C:/Checkins",
            "path_fldigi": "C:/Apps/FLDigi.exe",
            "js8_host": "10.1.1.13",
            "js8_port": 2550,
            "js8_offset_hz": 2150,
            "js8_directed_path": "C:/JS8Primary/DIRECTED.TXT",
            "js8_forms_path": "C:/JS8Primary/forms",
            "path_js8call": "C:/Apps/JS8Call",
            "path_js8spotter": "C:/Apps/JS8Spotter.exe",
            "path_commstat": "C:/Apps/CommStat.exe",
            "varac_path": "C:/VarAC",
            "varac_db_path": "C:/VarAC/VarAC.db",
            "varac_ini_path": "C:/VarAC/VarAC.ini",
            "varac_launch_cmd": "VarAC.exe",
            "message_paths": {"varac": "C:/VarAC/incoming"},
        }
    )

    js8_after = store.get_js8_instance(int(js8["id"]))
    fast_light_after = store.get_fast_light_config(int(fast_light["id"]))
    varac_after = store.get_varac_node(int(varac["id"]))
    assert js8_after is not None
    assert fast_light_after is not None
    assert varac_after is not None
    assert js8_after["host"] == "10.1.1.13"
    assert js8_after["port"] == 2550
    assert js8_after["offset_hz"] == 2150
    assert js8_after["directed_path"] == "C:/JS8Primary/DIRECTED.TXT"
    assert js8_after["forms_path"] == "C:/JS8Primary/forms"
    assert js8_after["install_path"] == "C:/Apps/JS8Call"
    assert js8_after["spotter_launch_path"] == "C:/Apps/JS8Spotter.exe"
    assert fast_light_after["flrig_host"] == "10.1.1.11"
    assert fast_light_after["flrig_port"] == 12444
    assert fast_light_after["fldigi_path"] == "C:/Apps/FLDigi.exe"
    assert fast_light_after["fldigi_checkin_dir"] == "C:/Checkins"
    assert varac_after["install_path"] == "C:/VarAC"
    assert varac_after["db_path"] == "C:/VarAC/VarAC.db"
    assert varac_after["incoming_path"] == "C:/VarAC/incoming"


def test_store_blocks_deleting_managed_records_that_are_still_assigned(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    js8 = store.save_js8_instance({"name": "Delete Guard JS8"})
    fast_light = store.save_fast_light_config({"name": "Delete Guard FL"})
    varac = store.save_varac_node({"name": "Delete Guard VarAC"})
    store.save_device_profile(
        {
            "name": "Guarded Device",
            "js8_instance_id": int(js8["id"]),
            "fast_light_config_id": int(fast_light["id"]),
            "varac_node_id": int(varac["id"]),
        }
    )

    try:
        store.delete_js8_instance(int(js8["id"]))
        raise AssertionError("Expected JS8 delete guard.")
    except ValueError:
        pass
    try:
        store.delete_fast_light_config(int(fast_light["id"]))
        raise AssertionError("Expected Fast Light delete guard.")
    except ValueError:
        pass
    try:
        store.delete_varac_node(int(varac["id"]))
        raise AssertionError("Expected VarAC delete guard.")
    except ValueError:
        pass


def test_store_allows_rigctld_runtime_activation(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    rigctld = store.save_device_profile(
        {
            "name": "Remote Rigctld",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
        }
    )

    store.set_runtime_active_device_profile(int(rigctld["id"]))
    settings.reload()
    active = store.get_runtime_active_device_profile()
    assert active is not None
    assert int(active["id"]) == int(rigctld["id"])
    assert settings.get("control_via") == "RIGCTLD"
    assert settings.get("rig_host") == "10.0.0.44"
    assert int(settings.get("rig_port")) == 4532
