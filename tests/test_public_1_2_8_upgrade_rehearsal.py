from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from freqinout.core.config_backup import create_config_backup, restore_config_backup
from freqinout.core.config_migration_preview import build_single_rig_upgrade_apply_plan
from freqinout.core.multi_radio_store import (
    CURRENT_MULTI_RIG_MIGRATION_VERSION,
    MULTI_RIG_MIGRATION_VERSION_KEY,
    MultiRadioStore,
    ensure_multi_rig_migration,
    settings_db_path,
)
from freqinout.core.settings_manager import SettingsManager


def _write_public_single_rig_settings(db_path: Path) -> None:
    values = {
        "callsign": "N1MAG",
        "control_via": "FLRig",
        "path_flrig": "/opt/fio/flrig",
        "flrig_host": "127.0.0.1",
        "flrig_port": 12345,
        "path_fldigi": "/opt/fio/fldigi",
        "fldigi_host": "127.0.0.1",
        "fldigi_port": 7362,
        "path_js8call": "/opt/fio/js8call",
        "js8_host": "127.0.0.1",
        "js8_port": 2442,
        "path_flmsg": "/opt/fio/flmsg",
        "path_flamp": "/opt/fio/flamp",
        "varac_path": "/opt/fio/VarAC.exe",
        "varac_ini_path": "/opt/fio/VarAC.ini",
        "launch_control_enabled": True,
        "use_scheduler": True,
    }
    db_path.parent.mkdir(parents=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
        conn.executemany(
            "INSERT INTO kv(key, value) VALUES(?, ?)",
            [(key, json.dumps(value)) for key, value in values.items()],
        )


def test_public_1_2_8_profile_upgrades_with_backup_and_rolls_back(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "public-1.2.8-profile"
    config_dir = profile_root / "config"
    _write_public_single_rig_settings(config_dir / "freqinout.db")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))

    settings = SettingsManager()
    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) is None
    plan = build_single_rig_upgrade_apply_plan(
        settings.all(),
        radio_name="Existing Station Radio",
        operating_plan_name="Existing Station Plan",
        config_dir=profile_root,
    )
    assert plan.can_apply is True

    backup = create_config_backup(
        plan.backup_paths,
        reason=plan.backup_reason,
        backup_root=tmp_path / "retained-backups",
    )
    assert backup.items[0].status == "backed_up"

    result = ensure_multi_rig_migration(
        settings._conn,  # type: ignore[arg-type]
        settings.all(),
        radio_name="Existing Station Radio",
        radio_manufacturer="Yaesu",
        radio_model="FTDX-10",
        operating_plan_name="Existing Station Plan",
        enabled_software_roles=("fast_light", "js8call", "flmsg", "flamp", "varac"),
    )
    settings.reload()
    store = MultiRadioStore(settings_db_path())
    radios = store.list_device_profiles()
    assert result.applied is True
    assert settings.get(MULTI_RIG_MIGRATION_VERSION_KEY) == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert len(radios) == 1
    assert radios[0]["name"] == "Existing Station Radio"
    assert radios[0]["flrig_port"] == 12345
    assert radios[0]["fldigi_port"] == 7362
    assert radios[0]["js8_port"] == 2442
    assert radios[0]["launch_enabled"] == 0
    assert len(store.list_js8_instances()) == 1
    assert len(store.list_fast_light_configs()) == 1
    assert len(store.list_varac_nodes()) == 1

    rerun = ensure_multi_rig_migration(settings._conn, settings.all())  # type: ignore[arg-type]
    assert rerun.already_current is True
    assert rerun.applied is False
    settings.close()

    restored = restore_config_backup(backup)
    assert restored.ok is True
    with sqlite3.connect(config_dir / "freqinout.db") as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "kv" in tables
        device_count = conn.execute("SELECT COUNT(*) FROM device_profiles").fetchone()[0]
        assert device_count == 0
        marker = conn.execute(
            "SELECT value FROM kv WHERE key=?",
            (MULTI_RIG_MIGRATION_VERSION_KEY,),
        ).fetchone()
        assert marker is None
