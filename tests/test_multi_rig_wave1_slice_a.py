from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from freqinout.core.multi_radio_store import (
    CURRENT_MULTI_RIG_MIGRATION_VERSION,
    FALLBACK_RADIO_NAMES,
    MULTI_RIG_MIGRATION_COMPLETED_AT_KEY,
    MULTI_RIG_MIGRATION_SUMMARY_PREFIX,
    MULTI_RIG_MIGRATION_VERSION_KEY,
    MultiRadioStore,
    SETTINGS_TABLE_SPECS,
    ensure_multi_rig_migration,
    ensure_default_multi_radio_records,
    mirror_legacy_settings_into_runtime_active_device,
    multi_rig_guardrail_warnings,
    settings_db_path,
)
from freqinout.core.multi_rig_guardrails import collect_multi_rig_guardrail_warnings
from freqinout.core.runtime_policy_selection_service import DurableRuntimePolicyStore
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
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


def _seed_default_runtime(settings: SettingsManager) -> None:
    ensure_default_multi_radio_records(settings._conn, settings.all())  # type: ignore[arg-type]
    settings.reload()


def test_settings_manager_starts_fresh_install_with_no_radios(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    devices = store.list_device_profiles()
    operating = store.list_operating_profiles()
    assignments = store.list_assignments()

    assert settings.db_path == settings_db_path()
    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert settings.get(f"{MULTI_RIG_MIGRATION_SUMMARY_PREFIX}{CURRENT_MULTI_RIG_MIGRATION_VERSION}")[
        "fresh_install_blank_slate"
    ] is True
    assert devices == []
    assert operating == []
    assert assignments == []

    js8_instances = store.list_js8_instances()
    fast_light_configs = store.list_fast_light_configs()
    varac_nodes = store.list_varac_nodes()
    assert js8_instances == []
    assert fast_light_configs == []
    assert varac_nodes == []


def test_existing_legacy_settings_wait_for_explicit_migration(monkeypatch, tmp_path):
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

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    assert store.list_device_profiles() == []
    assert store.list_operating_profiles() == []
    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) is None

    result = ensure_multi_rig_migration(settings._conn, settings.all())  # type: ignore[arg-type]
    settings.reload()

    assert result.applied is True
    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert settings.get(MULTI_RIG_MIGRATION_COMPLETED_AT_KEY)
    assert settings.get(f"{MULTI_RIG_MIGRATION_SUMMARY_PREFIX}{CURRENT_MULTI_RIG_MIGRATION_VERSION}")[
        "created_device_profile_id"
    ]

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


def test_deferred_legacy_writes_do_not_create_multi_rig_records(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig", "flrig_port": 12345})

    settings = SettingsManager()
    settings.set("js8_port", 2555)

    store = MultiRadioStore(settings_db_path())
    assert settings.get("js8_port") == 2555
    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) is None
    assert store.list_device_profiles() == []
    assert store.list_js8_instances() == []
    assert store.list_fast_light_configs() == []
    assert store.list_varac_nodes() == []


def test_timezone_only_settings_do_not_block_fresh_install_blank_slate(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"timezone": "America/Denver"})

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert store.list_device_profiles() == []
    assert store.list_operating_profiles() == []


def test_corrupt_legacy_config_starts_blank_without_schema_limbo(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "config.json").write_text("{not valid json", encoding="utf-8")

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert store.list_device_profiles() == []
    assert store.list_operating_profiles() == []


def test_empty_legacy_config_json_starts_blank_slate(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "config.json").write_text("{}", encoding="utf-8")

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert store.list_device_profiles() == []
    assert store.list_operating_profiles() == []


def test_ignored_only_legacy_config_json_starts_blank_slate(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "config.json").write_text(json.dumps({"timezone": "America/Denver"}), encoding="utf-8")

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())

    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert store.list_device_profiles() == []
    assert store.list_operating_profiles() == []


def test_explicit_migration_writes_key_map_columns(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(
        db_path,
        {
            "control_via": "JS8Call",
            "rig_host": "10.9.0.1",
            "rig_port": 4532,
            "flrig_host": "10.9.0.2",
            "flrig_port": 12346,
            "path_flrig": "/opt/flrig",
            "path_fldigi": "/opt/fldigi",
            "fldigi_host": "10.9.0.3",
            "fldigi_port": 7366,
            "fldigi_log_path": "/logs/fldigi",
            "fldigi_checkin_dir": "/checkins",
            "path_flmsg": "/opt/flmsg",
            "path_flamp": "/opt/flamp",
            "message_paths": {
                "flmsg": "/messages/flmsg",
                "flamp": "/messages/flamp",
                "varac": "/messages/varac",
            },
            "js8_host": "10.9.0.4",
            "js8_port": 2445,
            "js8_offset_hz": 1500,
            "js8_profile_path": "/js8/profile",
            "js8_directed_path": "/js8/DIRECTED.TXT",
            "js8_forms_path": "/js8/forms",
            "path_js8call": "/opt/js8call",
            "path_js8spotter": "/opt/js8spotter",
            "path_commstat": "/opt/commstat",
            "varac_path": "/opt/varac",
            "varac_db_path": "/varac/VarAC.db",
            "varac_ini_path": "/varac/VarAC.ini",
            "varac_launch_cmd": "varac --portable",
            "varac_outbox_dir": "/varac/outbox",
            "varac_bbs_dir": "/varac/bbs",
            "varac_bbs_archive_dir": "/varac/archive",
            "varac_bbs_enabled": True,
            "varac_bbs_limit_access_enabled": True,
            "varac_bbs_allowed_callsigns": "K1ABC,N0XYZ",
            "varac_bbs_announce_enabled": True,
            "launch_control_enabled": False,
            "use_scheduler": False,
        },
    )

    settings = SettingsManager()
    result = ensure_multi_rig_migration(
        settings._conn,  # type: ignore[arg-type]
        settings.all(),
        radio_name="IC-7300 Desk",
        radio_model="IC-7300",
        radio_manufacturer="Icom",
        enabled_software_roles={"js8call", "fast_light", "varac", "flamp", "flmsg", "js8spotter", "commstat"},
    )
    settings.reload()
    store = MultiRadioStore(settings_db_path())

    assert result.applied is True
    device = store.list_device_profiles()[0]
    js8 = store.list_js8_instances()[0]
    fast = store.list_fast_light_configs()[0]
    varac = store.list_varac_nodes()[0]
    operating = store.list_operating_profiles()[0]

    assert device["name"] == "IC-7300 Desk"
    assert device["radio_manufacturer"] == "Icom"
    assert device["radio_model"] == "IC-7300"
    assert device["control_backend"] == "js8call"
    assert device["rig_host"] == "10.9.0.1"
    assert device["rig_port"] == 4532
    assert device["flmsg_path"] == "/opt/flmsg"
    assert device["flmsg_message_path"] == "/messages/flmsg"
    assert device["flamp_path"] == "/opt/flamp"
    assert device["flamp_message_path"] == "/messages/flamp"
    assert device["varac_outbox_dir"] == "/varac/outbox"
    assert device["varac_bbs_dir"] == "/varac/bbs"
    assert device["varac_bbs_archive_dir"] == "/varac/archive"
    assert device["varac_bbs_enabled"] == 1
    assert device["launch_enabled"] == 0
    assert device["use_js8call"] == 1
    assert device["use_flrig"] == 1
    assert device["use_fldigi"] == 1
    assert device["use_varac"] == 1
    assert device["use_flamp"] == 1
    assert device["use_flmsg"] == 1
    assert device["use_js8spotter"] == 1
    assert device["use_commstat"] == 1

    assert fast["flrig_host"] == "10.9.0.2"
    assert fast["flrig_port"] == 12346
    assert fast["fldigi_path"] == "/opt/fldigi"
    assert fast["fldigi_host"] == "10.9.0.3"
    assert fast["fldigi_port"] == 7366
    assert fast["fldigi_log_path"] == "/logs/fldigi"
    assert fast["fldigi_checkin_dir"] == "/checkins"

    assert js8["host"] == "10.9.0.4"
    assert js8["port"] == 2445
    assert js8["offset_hz"] == 1500
    assert js8["profile_path"] == "/js8/profile"
    assert js8["directed_path"] == "/js8/DIRECTED.TXT"
    assert js8["forms_path"] == "/js8/forms"
    assert js8["install_path"] == "/opt/js8call"
    assert js8["spotter_launch_path"] == "/opt/js8spotter"
    assert js8["commstat_launch_path"] == "/opt/commstat"

    assert varac["install_path"] == "/opt/varac"
    assert varac["db_path"] == "/varac/VarAC.db"
    assert varac["ini_path"] == "/varac/VarAC.ini"
    assert varac["launch_cmd"] == "varac --portable"
    assert varac["incoming_path"] == "/messages/varac"

    assert operating["name"] == "Daily HF Schedule"
    assert operating["scheduler_enabled"] == 0
    assert operating["use_launch_control"] == 0


def test_migration_summary_records_unknown_role_warning(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig"})

    settings = SettingsManager()
    result = ensure_multi_rig_migration(
        settings._conn,  # type: ignore[arg-type]
        settings.all(),
        enabled_software_roles={"js8call", "unknown_radio_tool"},
    )
    settings.reload()
    summary = settings.get(f"{MULTI_RIG_MIGRATION_SUMMARY_PREFIX}{CURRENT_MULTI_RIG_MIGRATION_VERSION}")

    assert result.applied is True
    assert "Unknown software role ignored: unknown_radio_tool" in result.warnings
    assert summary["warnings"] == list(result.warnings)


def test_operating_profile_save_defaults_launch_control_off(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    _seed_default_runtime(settings)
    store = MultiRadioStore(settings_db_path())
    profile = store.save_operating_profile({"name": "Operator Plan"})

    assert profile["use_launch_control"] == 0


def test_mirror_legacy_settings_without_launch_key_keeps_launch_control_off(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    _seed_default_runtime(settings)
    store = MultiRadioStore(settings_db_path())
    active = store.get_runtime_active_device_profile()
    assert active is not None
    assignment = store.list_effective_assignments()[0]

    mirror_legacy_settings_into_runtime_active_device(
        settings._conn,  # type: ignore[arg-type]
        {
            "control_via": "FLRig",
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "use_scheduler": True,
        },
    )

    device = store.get_device_profile(int(active["id"]))
    operating = store.get_operating_profile(int(assignment["operating_profile_id"]))
    assert device is not None
    assert operating is not None
    assert device["launch_enabled"] == 0
    assert operating["use_launch_control"] == 0


def test_v2_migration_disables_existing_launch_enabled_rows(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    _seed_default_runtime(settings)
    store = MultiRadioStore(settings_db_path())
    device = store.list_device_profiles()[0]
    operating = store.list_operating_profiles()[0]
    with settings._conn:  # type: ignore[attr-defined]
        settings._conn.execute("UPDATE kv SET value=? WHERE key=?", (json.dumps(1), MULTI_RIG_MIGRATION_VERSION_KEY))  # type: ignore[attr-defined]
        settings._conn.execute("UPDATE device_profiles SET launch_enabled=1 WHERE id=?", (int(device["id"]),))  # type: ignore[attr-defined]
        settings._conn.execute("UPDATE device_profiles SET name='Default Radio', needs_operator_name=0 WHERE id=?", (int(device["id"]),))  # type: ignore[attr-defined]
        settings._conn.execute("UPDATE operating_profiles SET use_launch_control=1 WHERE id=?", (int(operating["id"]),))  # type: ignore[attr-defined]
        settings._conn.execute(  # type: ignore[attr-defined]
            """
            INSERT OR REPLACE INTO runtime_policies (
                radio_profile_id, scheduler_enabled, background_ingest_enabled, messages_enabled,
                map_enabled, launch_enabled, net_control_enabled, operator_suppressed,
                created_utc, updated_utc
            ) VALUES (?, 1, 1, 1, 1, 1, 1, 0, '2026-07-22T00:00:00Z', '2026-07-22T00:00:00Z')
            """,
            (int(device["id"]),),
        )

    result = ensure_multi_rig_migration(settings._conn, settings.all())  # type: ignore[arg-type]
    settings.reload()

    device = store.get_device_profile(int(device["id"]))
    operating = store.get_operating_profile(int(operating["id"]))
    policy = DurableRuntimePolicyStore(store).get_policy(radio_shared_state_id(device["id"]))
    assert result.applied is True
    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert device is not None and device["launch_enabled"] == 0
    assert device is not None and device["needs_operator_name"] == 1
    assert operating is not None and operating["use_launch_control"] == 0
    assert policy.launch_control_enabled is False


def test_radio_rename_clears_needs_operator_name(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    _seed_default_runtime(settings)
    store = MultiRadioStore(settings_db_path())
    device = store.list_device_profiles()[0]

    renamed = store.save_device_profile({"id": int(device["id"]), "name": "IC-7300 Desk"})

    assert renamed["name"] == "IC-7300 Desk"
    assert renamed["needs_operator_name"] == 0


def test_fallback_radio_name_set_drives_name_readiness() -> None:
    assert {"", "radio", "my radio", "default radio", "device profile"} <= set(FALLBACK_RADIO_NAMES)


def test_rerunning_migration_does_not_overwrite_custom_operating_plan_name(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    _seed_default_runtime(settings)
    store = MultiRadioStore(settings_db_path())
    operating = store.list_operating_profiles()[0]
    store.save_operating_profile({"id": int(operating["id"]), "name": "My Custom Plan", "use_launch_control": 0})
    with settings._conn:  # type: ignore[attr-defined]
        settings._conn.execute("UPDATE kv SET value=? WHERE key=?", (json.dumps(1), MULTI_RIG_MIGRATION_VERSION_KEY))  # type: ignore[attr-defined]

    ensure_multi_rig_migration(settings._conn, settings.all())  # type: ignore[arg-type]

    refreshed = store.get_operating_profile(int(operating["id"]))
    assert refreshed is not None
    assert refreshed["name"] == "My Custom Plan"


def test_multi_rig_guardrail_warnings_surface_duplicate_active_endpoints_and_paths(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    first = store.save_device_profile(
        {
            "name": "Desk Radio",
            "runtime_active": 1,
            "control_backend": "flrig",
            "use_flrig": 1,
            "use_fldigi": 1,
            "use_js8call": 1,
            "use_varac": 1,
            "use_flamp": 1,
            "use_flmsg": 1,
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7362,
            "js8_host": "127.0.0.1",
            "js8_port": 2442,
            "varac_db_path": "/varac/shared/VarAC.db",
            "varac_bbs_dir": "/varac/shared/bbs",
            "flamp_message_path": "/messages/shared/flamp",
            "flmsg_message_path": "/messages/shared/flmsg",
        }
    )
    second = store.save_device_profile(
        {
            "name": "Field Radio",
            "runtime_active": 1,
            "control_backend": "flrig",
            "use_flrig": 1,
            "use_fldigi": 1,
            "use_js8call": 1,
            "use_varac": 1,
            "use_flamp": 1,
            "use_flmsg": 1,
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7362,
            "js8_host": "127.0.0.1",
            "js8_port": 2442,
            "varac_db_path": "/varac/shared/VarAC.db",
            "varac_bbs_dir": "/varac/shared/bbs",
            "flamp_message_path": "/messages/shared/flamp",
            "flmsg_message_path": "/messages/shared/flmsg",
        }
    )

    assert first["launch_enabled"] == 0
    assert second["launch_enabled"] == 0

    with settings._conn:  # type: ignore[attr-defined]
        settings._conn.execute(  # type: ignore[attr-defined]
            "UPDATE device_profiles SET runtime_active=1 WHERE id IN (?, ?)",
            (int(first["id"]), int(second["id"])),
        )

    structured = collect_multi_rig_guardrail_warnings(settings._conn)  # type: ignore[arg-type]
    warnings = multi_rig_guardrail_warnings(settings._conn)  # type: ignore[arg-type]

    js8_warning = next(warning for warning in structured if warning.warning_type == "duplicate_js8_endpoint")
    assert set(js8_warning.affected_radio_names) == {"Desk Radio", "Field Radio"}
    assert js8_warning.resource_value == "127.0.0.1:2442"
    assert any("Duplicate JS8Call API endpoint" in warning for warning in warnings)
    assert any("Duplicate FLDigi XML-RPC endpoint" in warning for warning in warnings)
    assert any("Duplicate FLRig control endpoint" in warning for warning in warnings)
    assert any("Duplicate VarAC live BBS directory" in warning for warning in warnings)
    assert any("Duplicate VarAC database path" in warning for warning in warnings)
    assert any("Duplicate FLAMP message path" in warning for warning in warnings)
    assert any("Duplicate FLMSG message path" in warning for warning in warnings)


def test_migration_key_map_targets_exist_in_schema():
    expected_columns = {
        "device_profiles": {
            "control_backend",
            "needs_operator_name",
            "rig_host",
            "rig_port",
            "launch_path",
            "launch_enabled",
            "scheduler_enabled",
            "schedule_hold_minutes_default",
            "freq_enforcement_mode",
            "freq_prompt_interval",
            "fldigi_enforcement_mode",
            "fldigi_prompt_interval",
            "js8_enforcement_mode",
            "js8_prompt_interval",
            "flmsg_path",
            "flmsg_message_path",
            "flamp_path",
            "flamp_message_path",
            "varac_outbox_dir",
            "varac_bbs_dir",
            "varac_bbs_archive_dir",
        },
        "fast_light_configs": {
            "flrig_host",
            "flrig_port",
            "fldigi_path",
            "fldigi_host",
            "fldigi_port",
            "fldigi_log_path",
            "fldigi_checkin_dir",
        },
        "js8_instances": {
            "host",
            "port",
            "offset_hz",
            "profile_path",
            "directed_path",
            "forms_path",
            "install_path",
            "spotter_launch_path",
            "commstat_launch_path",
        },
        "varac_nodes": {
            "install_path",
            "db_path",
            "ini_path",
            "launch_cmd",
            "incoming_path",
        },
        "operating_profiles": {
            "scheduler_enabled",
            "use_launch_control",
        },
    }

    for table, columns in expected_columns.items():
        spec_columns = set(SETTINGS_TABLE_SPECS[table]["columns"])
        assert columns <= spec_columns


def test_device_profile_timer_policy_is_seeded_from_legacy_settings(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(
        db_path,
        {
            "control_via": "FLRig",
            "use_scheduler": False,
            "schedule_hold_minutes_default": 60,
            "freq_enforcement_mode": "Prompt",
            "freq_prompt_interval": "Every 15 minutes",
            "fldigi_enforcement_mode": "Disabled",
            "fldigi_prompt_interval": "Every 30 minutes",
            "js8_enforcement_mode": "Prompt",
            "js8_prompt_interval": "Every 5 minutes",
        },
    )

    settings = SettingsManager()
    result = ensure_multi_rig_migration(settings._conn, settings.all())  # type: ignore[arg-type]
    store = MultiRadioStore(settings_db_path())
    devices = store.list_device_profiles()

    assert result.applied is True
    assert len(devices) == 1
    device = devices[0]
    assert device["scheduler_enabled"] == 0
    assert device["schedule_hold_minutes_default"] == 60
    assert device["freq_enforcement_mode"] == "Prompt"
    assert device["freq_prompt_interval"] == "Every 15 minutes"
    assert device["fldigi_enforcement_mode"] == "Disabled"
    assert device["fldigi_prompt_interval"] == "Every 30 minutes"
    assert device["js8_enforcement_mode"] == "Prompt"
    assert device["js8_prompt_interval"] == "Every 5 minutes"


def test_timer_policy_mirrors_between_legacy_settings_and_active_device(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    _seed_default_runtime(settings)
    settings.set("use_scheduler", False)
    settings.set("schedule_hold_minutes_default", 90)
    settings.set("freq_enforcement_mode", "Prompt")
    settings.set("freq_prompt_interval", "Every 15 minutes")
    settings.set("fldigi_enforcement_mode", "Disabled")
    settings.set("fldigi_prompt_interval", "Every 30 minutes")
    settings.set("js8_enforcement_mode", "Prompt")
    settings.set("js8_prompt_interval", "Every 5 minutes")

    mirrored = mirror_legacy_settings_into_runtime_active_device(
        settings._conn,  # type: ignore[arg-type]
        settings.all(),
        keys_changed={"freq_enforcement_mode"},
    )
    assert mirrored is not None

    store = MultiRadioStore(settings_db_path())
    device = store.list_device_profiles()[0]
    assert device["scheduler_enabled"] == 0
    assert device["schedule_hold_minutes_default"] == 90
    assert device["freq_enforcement_mode"] == "Prompt"
    assert device["freq_prompt_interval"] == "Every 15 minutes"
    assert device["fldigi_enforcement_mode"] == "Disabled"
    assert device["fldigi_prompt_interval"] == "Every 30 minutes"
    assert device["js8_enforcement_mode"] == "Prompt"
    assert device["js8_prompt_interval"] == "Every 5 minutes"

    store.save_device_profile(
        {
            **device,
            "scheduler_enabled": 1,
            "schedule_hold_minutes_default": 120,
            "freq_enforcement_mode": "On Schedule Change",
            "freq_prompt_interval": "Hourly",
            "fldigi_enforcement_mode": "Prompt",
            "fldigi_prompt_interval": "Every 10 minutes",
            "js8_enforcement_mode": "Disabled",
            "js8_prompt_interval": "Hourly",
        }
    )
    projected = store.sync_runtime_active_device_to_legacy_settings(int(device["id"]))
    settings.reload()

    assert projected is not None
    assert settings.get("use_scheduler") is True
    assert settings.get("schedule_hold_minutes_default") == 120
    assert settings.get("freq_enforcement_mode") == "On Schedule Change"
    assert settings.get("freq_prompt_interval") == "Hourly"
    assert settings.get("fldigi_enforcement_mode") == "Prompt"
    assert settings.get("fldigi_prompt_interval") == "Every 10 minutes"
    assert settings.get("js8_enforcement_mode") == "Disabled"
    assert settings.get("js8_prompt_interval") == "Hourly"


def test_hold_duration_normalizes_at_store_boundaries(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(
        db_path,
        {
            "control_via": "FLRig",
            "schedule_hold_minutes_default": 45,
        },
    )

    settings = SettingsManager()
    result = ensure_multi_rig_migration(settings._conn, settings.all())  # type: ignore[arg-type]
    store = MultiRadioStore(settings_db_path())
    device = store.list_device_profiles()[0]

    assert result.applied is True
    assert device["schedule_hold_minutes_default"] == 30

    settings.set("schedule_hold_minutes_default", 45)
    mirrored = mirror_legacy_settings_into_runtime_active_device(
        settings._conn,  # type: ignore[arg-type]
        settings.all(),
        keys_changed={"schedule_hold_minutes_default"},
    )
    assert mirrored is not None
    device = store.list_device_profiles()[0]
    assert device["schedule_hold_minutes_default"] == 30

    saved = store.save_device_profile({**device, "schedule_hold_minutes_default": 45})
    assert saved["schedule_hold_minutes_default"] == 30

    projected = store.sync_runtime_active_device_to_legacy_settings(int(saved["id"]))
    settings.reload()
    assert projected is not None
    assert settings.get("schedule_hold_minutes_default") == 30


def test_default_seed_is_idempotent(monkeypatch, tmp_path):
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


def test_db_initializer_and_schema_tool_include_slice_a_tables(monkeypatch, tmp_path):
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


def test_runtime_active_projection_updates_legacy_settings(monkeypatch, tmp_path):
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
    _seed_default_runtime(settings)
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


def test_linked_records_project_and_mirror(monkeypatch, tmp_path):
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
    linked = store.save_device_profile(
        {
            "name": "Linked Field Device",
            "control_backend": "flrig",
            "js8_instance_id": int(js8["id"]),
            "fast_light_config_id": int(fast_light["id"]),
            "varac_node_id": int(varac["id"]),
        }
    )
    store.set_runtime_active_device_profile(int(linked["id"]))

    settings = SettingsManager()
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
    assert js8_after["commstat_launch_path"] == "C:/Apps/CommStat.exe"
    assert fast_light_after["flrig_host"] == "10.1.1.11"
    assert fast_light_after["flrig_port"] == 12444
    assert fast_light_after["fldigi_path"] == "C:/Apps/FLDigi.exe"
    assert fast_light_after["fldigi_checkin_dir"] == "C:/Checkins"
    assert varac_after["install_path"] == "C:/VarAC"
    assert varac_after["db_path"] == "C:/VarAC/VarAC.db"
    assert varac_after["incoming_path"] == "C:/VarAC/incoming"


def test_js8_instance_update_without_offset_preserves_existing_offset(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    store = MultiRadioStore(settings_db_path())
    js8 = store.save_js8_instance({"name": "Offset Guard", "offset_hz": 2050})
    updated = store.save_js8_instance({"id": int(js8["id"]), "name": "Offset Guard Updated", "port": 2444})

    assert updated["offset_hz"] == 2050


def test_store_delete_guards_and_rigctld_activation(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
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

    with pytest.raises(ValueError):
        store.delete_js8_instance(int(js8["id"]))
    with pytest.raises(ValueError):
        store.delete_fast_light_config(int(fast_light["id"]))
    with pytest.raises(ValueError):
        store.delete_varac_node(int(varac["id"]))

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
