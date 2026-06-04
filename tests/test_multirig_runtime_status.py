from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from freqinout.core.background_ingest import BackgroundIngestController
import freqinout.core.station_runtime_manager as station_runtime_manager_mod
from freqinout.core.multi_radio_store import (
    MultiRadioStore,
    ensure_multi_radio_settings_schema,
    ensure_multi_rig_migration,
)
from freqinout.core.multi_rig_runtime_status import (
    SCOPE_ALL_ACTIVE_RUNTIME,
    SCOPE_NONE,
    STARTUP_DEFERRED,
    STARTUP_EXISTING_UNMIGRATED,
    STARTUP_FRESH_DEFAULT_READY,
    STARTUP_MIGRATED,
    STARTUP_MIGRATION_ERROR,
    build_multi_rig_runtime_status,
)
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.shared_state_persistence import SharedStatePersistenceAdapter
from freqinout.core.station_runtime_manager import StationRuntimeManager


def _insert_kv(db_path: Path, values: dict[str, object]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        ensure_multi_radio_settings_schema(conn)
        conn.executemany(
            "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
            [(key, json.dumps(value)) for key, value in values.items()],
        )
        conn.commit()
    finally:
        conn.close()


def test_fresh_default_ready_status(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore()
    status = build_multi_rig_runtime_status(store)

    assert status.startup_mode == STARTUP_FRESH_DEFAULT_READY
    assert status.migration_current is True
    assert status.existing_fio_usage_detected is False
    assert status.primary_device_profile_id is not None
    assert status.primary_radio_id == f"radio_{status.primary_device_profile_id}"
    assert status.active_device_profile_ids == (status.primary_device_profile_id,)
    assert status.messages_scope == SCOPE_ALL_ACTIVE_RUNTIME
    assert status.background_ingest_scope == SCOPE_ALL_ACTIVE_RUNTIME


def test_existing_unmigrated_status_does_not_create_profiles(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig", "flrig_port": 12345})

    SettingsManager()
    store = MultiRadioStore()
    status = build_multi_rig_runtime_status(store)

    assert status.startup_mode == STARTUP_EXISTING_UNMIGRATED
    assert status.migration_current is False
    assert status.primary_device_profile_id is None
    assert status.active_device_profile_ids == ()
    assert status.background_ingest_scope == SCOPE_NONE
    assert store.list_device_profiles() == []


def test_unused_pre_settings_startup_is_existing_unmigrated(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    store = MultiRadioStore()
    status = build_multi_rig_runtime_status(store)

    assert status.startup_mode == STARTUP_EXISTING_UNMIGRATED
    assert status.migration_current is False
    assert status.existing_fio_usage_detected is False
    assert status.primary_device_profile_id is None
    assert status.active_device_profile_ids == ()
    assert store.list_device_profiles() == []


def test_deferred_status(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "JS8Call"})
    settings = SettingsManager()

    result = ensure_multi_rig_migration(settings._conn, settings.all(), defer=True)  # type: ignore[arg-type]
    status = build_multi_rig_runtime_status(MultiRadioStore())

    assert result.deferred is True
    assert status.startup_mode == STARTUP_DEFERRED
    assert status.migration_deferred is True
    assert status.background_ingest_scope == SCOPE_NONE


def test_migrated_status_after_explicit_migration(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig", "flrig_port": 12345})
    settings = SettingsManager()

    ensure_multi_rig_migration(settings._conn, settings.all())  # type: ignore[arg-type]
    store = MultiRadioStore()
    status = build_multi_rig_runtime_status(store)

    assert status.startup_mode == STARTUP_MIGRATED
    assert status.migration_current is True
    assert status.existing_fio_usage_detected is True
    assert status.primary_device_profile_id is not None
    assert status.active_device_profile_ids == (status.primary_device_profile_id,)


def test_migration_error_status_uses_warnings(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig"})
    SettingsManager()

    status = build_multi_rig_runtime_status(MultiRadioStore(), migration_warnings=("Could not migrate",))

    assert status.startup_mode == STARTUP_MIGRATION_ERROR
    assert status.warnings == ("Could not migrate",)
    assert status.background_ingest_scope == SCOPE_NONE


def test_shared_state_snapshot_carries_runtime_status(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig"})
    SettingsManager()

    store = MultiRadioStore()
    snapshot = SharedStatePersistenceAdapter(store).snapshot()

    assert snapshot.startup_mode == STARTUP_EXISTING_UNMIGRATED
    assert snapshot.runtime_status.startup_mode == STARTUP_EXISTING_UNMIGRATED
    assert snapshot.selection_state.primary_runtime_radio_id is None
    assert snapshot.selection_state.active_runtime_radio_ids == ()
    assert store.list_device_profiles() == []


def test_station_runtime_manager_preserves_pending_status_without_runtimes(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig"})
    settings = SettingsManager()

    manager = StationRuntimeManager(store=MultiRadioStore(), settings=settings)
    manager.sync_with_store()

    assert manager.runtime_status() is not None
    assert manager.runtime_status().startup_mode == STARTUP_EXISTING_UNMIGRATED
    assert manager.get_runtime_primary_device_profile() is None


def test_station_runtime_manager_reuses_cached_runtime_status(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig"})
    settings = SettingsManager()

    calls = {"count": 0}
    real_builder = station_runtime_manager_mod.build_multi_rig_runtime_status

    def counting_builder(store):
        calls["count"] += 1
        return real_builder(store)

    monkeypatch.setattr(station_runtime_manager_mod, "build_multi_rig_runtime_status", counting_builder)
    manager = StationRuntimeManager(store=MultiRadioStore(), settings=settings)

    manager.sync_with_store()
    manager.sync_with_store()
    manager.sync_with_store(refresh_runtime_status=True)

    assert calls["count"] == 2


def test_background_ingest_skips_extra_multirig_jobs_for_existing_unmigrated(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    _insert_kv(db_path, {"control_via": "FLRig"})
    SettingsManager()

    service = BackgroundIngestController(SettingsManager())

    assert service._active_varac_vault_profiles() == []
