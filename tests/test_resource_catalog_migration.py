"""Bounded LN-1 migration characterization using isolated SQLite fixtures."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from freqinout.core.resource_catalog_store import ResourceCatalogStore


def _report_value(report: object, name: str, default: Any = None) -> Any:
    if isinstance(report, dict):
        return report.get(name, default)
    return getattr(report, name, default)


def _seed_nets(path: Path, rows: list[dict[str, Any]]) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE net_resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_set TEXT NOT NULL, source_type TEXT NOT NULL, source_ref TEXT,
                readonly INTEGER DEFAULT 1, day_utc TEXT NOT NULL, recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0, month_weeks TEXT, group_name TEXT,
                band TEXT NOT NULL, mode TEXT NOT NULL, frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL, end_utc TEXT NOT NULL, early_checkin INTEGER NOT NULL,
                primary_js8call_group TEXT, coverage TEXT, comment TEXT, net_name TEXT,
                fldigi_mode TEXT, fldigi_offset TEXT, updated_utc TEXT
            )
            """
        )
        columns = (
            "resource_set", "source_type", "source_ref", "readonly", "day_utc", "recurrence",
            "biweekly_offset_weeks", "month_weeks", "group_name", "band", "mode", "frequency",
            "start_utc", "end_utc", "early_checkin", "primary_js8call_group", "coverage", "comment",
            "net_name", "fldigi_mode", "fldigi_offset", "updated_utc",
        )
        conn.executemany(
            f"INSERT INTO net_resources ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            [tuple(row.get(column, "") for column in columns) for row in rows],
        )


def _seed_linked_schedule(path: Path, resource_id: int = 1) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """CREATE TABLE net_schedule_tab (
            id INTEGER PRIMARY KEY, resource_id INTEGER, day_utc TEXT, band TEXT,
            mode TEXT, frequency TEXT, start_utc TEXT, end_utc TEXT, early_checkin INTEGER)"""
        )
        conn.execute(
            """INSERT INTO net_schedule_tab
            (id,resource_id,day_utc,band,mode,frequency,start_utc,end_utc,early_checkin)
            VALUES (1,?,?,?,?,?,?,?,?)""",
            (resource_id, "Monday", "40M", "Digi", "7.078", "01:00", "02:00", 0),
        )


def _legacy_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "resource_set": "Reference Bundle", "source_type": "builtin", "source_ref": "reference.json",
        "readonly": 1, "day_utc": "Monday", "recurrence": "Weekly", "biweekly_offset_weeks": 0,
        "month_weeks": "", "group_name": "Group Alpha", "band": "40M", "mode": "Digi",
        "frequency": "7.078", "start_utc": "01:00", "end_utc": "02:00", "early_checkin": 0,
        "primary_js8call_group": "", "coverage": "National", "comment": "", "net_name": "Net Alpha",
        "fldigi_mode": "", "fldigi_offset": "", "updated_utc": "2026-09-09T00:00:00Z",
    }
    row.update(overrides)
    return row


def _seed_settings(path: Path, profiles: list[dict[str, Any]]) -> str:
    payload = json.dumps(profiles, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO kv(key,value) VALUES ('local_net_profiles', ?)", (payload,))
    return payload


def _successful_backup(path: Path, backup_root: Path):
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_file = backup_root / path.name
    backup_file.write_bytes(path.read_bytes())
    item = SimpleNamespace(original_path=str(path), backup_path=str(backup_file), status="backed_up")
    return SimpleNamespace(backup_dir=str(backup_root), items=(item,))


def _backup_factory(backup_root: Path):
    def backup(paths: Any, **_kwargs: Any) -> object:
        selected = [Path(paths)] if isinstance(paths, (str, Path)) else [Path(path) for path in paths]
        items = []
        backup_root.mkdir(parents=True, exist_ok=True)
        for path in selected:
            result = _successful_backup(path, backup_root)
            items.extend(result.items)
        return SimpleNamespace(backup_dir=str(backup_root), items=tuple(items))

    return backup


def _table_exists(path: Path, name: str) -> bool:
    with sqlite3.connect(path) as conn:
        return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _counts(path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(path) as conn:
        return tuple(
            int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            if _table_exists(path, table) else 0
            for table in ("frequency_resources", "net_directory_entries", "net_directory_sessions")
        )


def test_empty_installation_is_a_noop(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import dry_run_resource_catalog_migration

    nets = tmp_path / "freqinout_nets.db"
    report = dry_run_resource_catalog_migration(nets)
    assert _report_value(report, "total_legacy_rows") == 0
    assert _report_value(report, "migrated_frequency_count") == 0
    assert _report_value(report, "migrated_net_entry_count") == 0
    assert _report_value(report, "migrated_session_count") == 0
    assert not nets.exists()


def test_current_duplicate_malformed_and_mixed_sources_are_classified_and_deduped(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import dry_run_resource_catalog_migration

    nets = tmp_path / "freqinout_nets.db"
    rows = [
        _legacy_row(),
        _legacy_row(source_type="manual", source_ref="station-entry", readonly=0, group_name="Group Bravo", band="2M", mode="FM", frequency="146.520", net_name="Station Net"),
        _legacy_row(source_type="imported", source_ref="community.json", group_name="Group Charlie", band="GMRS", mode="FM", frequency="462.675", net_name="Imported Net"),
        _legacy_row(day_utc=" Monday ", recurrence="weekly", group_name=" group alpha ", frequency="7.0780", net_name=" net alpha "),
        _legacy_row(group_name="", band="2M", mode="FM", frequency="not-a-frequency", net_name=""),
        _legacy_row(group_name="Group Delta", band="", mode="FM", frequency="146.520", net_name="Malformed"),
        _legacy_row(group_name="Group Echo", band="2M", mode="FM", frequency="146.520", net_name=""),
    ]
    _seed_nets(nets, rows)
    report = dry_run_resource_catalog_migration(nets)
    assert _report_value(report, "total_legacy_rows") == len(rows)
    assert _report_value(report, "migrated_frequency_count") >= 4
    assert _report_value(report, "migrated_net_entry_count") >= 3
    assert _report_value(report, "migrated_session_count") >= 3
    assert _report_value(report, "review_required_count") >= 2
    classifications = _report_value(report, "classifications", {})
    assert classifications
    # Dry-run never creates canonical tables or changes the source DB.
    assert not _table_exists(nets, "frequency_resources")


def test_local_net_profiles_remain_lossless_and_never_become_schedules(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import apply_resource_catalog_migration

    nets = tmp_path / "freqinout_nets.db"
    settings = tmp_path / "freqinout.db"
    _seed_nets(nets, [_legacy_row()])
    original_profiles = _seed_settings(settings, [{"group": "Group Alpha", "resource": "2M", "mode": "FM", "target": "146.520"}])
    backup = _backup_factory(tmp_path / "backup")
    report = apply_resource_catalog_migration(nets, settings, backup_factory=backup)
    assert _report_value(report, "authority_state") in {"shadow_ready", "canonical"}
    with sqlite3.connect(settings) as conn:
        assert conn.execute("SELECT value FROM kv WHERE key='local_net_profiles'").fetchone()[0] == original_profiles
    assert not _table_exists(nets, "local_net_schedules")
    with sqlite3.connect(nets) as conn:
        assert conn.execute("SELECT COUNT(*) FROM net_resources").fetchone()[0] == 1


def test_backup_failure_leaves_legacy_database_byte_identical(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import apply_resource_catalog_migration

    nets = tmp_path / "freqinout_nets.db"
    _seed_nets(nets, [_legacy_row()])
    before = hashlib.sha256(nets.read_bytes()).digest()

    def failed_backup(*_args: Any, **_kwargs: Any) -> object:
        raise RuntimeError("backup unavailable")

    with pytest.raises((RuntimeError, OSError)):
        apply_resource_catalog_migration(nets, backup_factory=failed_backup)
    assert hashlib.sha256(nets.read_bytes()).digest() == before
    assert not _table_exists(nets, "frequency_resources")


def test_transaction_failure_rolls_back_and_retry_is_idempotent(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import apply_resource_catalog_migration

    nets = tmp_path / "freqinout_nets.db"
    _seed_nets(nets, [_legacy_row(), _legacy_row(group_name="Group Bravo", band="2M", mode="FM", frequency="146.520", net_name="Net Bravo")])
    backup = _backup_factory(tmp_path / "backup")
    with pytest.raises((RuntimeError, ValueError)):
        apply_resource_catalog_migration(nets, backup_factory=backup, fail_after_rows=1)
    with sqlite3.connect(nets) as conn:
        assert conn.execute("SELECT COUNT(*) FROM net_resources").fetchone()[0] == 2

    first = apply_resource_catalog_migration(nets, backup_factory=backup)
    first_counts = _counts(nets)
    second = apply_resource_catalog_migration(nets, backup_factory=backup)
    assert _counts(nets) == first_counts
    assert _report_value(second, "unchanged_count") >= 1
    assert _report_value(first, "migrated_frequency_count") >= 2


def test_canonical_rows_match_legacy_frequency_and_net_parity(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import apply_resource_catalog_migration

    nets = tmp_path / "freqinout_nets.db"
    _seed_nets(nets, [_legacy_row(), _legacy_row(group_name="Group Bravo", band="2M", mode="FM", frequency="146.520", net_name="Net Bravo")])
    backup = _backup_factory(tmp_path / "backup")
    apply_resource_catalog_migration(nets, backup_factory=backup)
    frequencies = ResourceCatalogStore(nets).list_frequencies(active=None, limit=200)
    entries = ResourceCatalogStore(nets).list_net_entries(active=None, limit=200)
    with sqlite3.connect(nets) as conn:
        legacy_count = int(conn.execute("SELECT COUNT(*) FROM net_resources").fetchone()[0])
        canonical_frequency_count = int(conn.execute(
            "SELECT COUNT(*) FROM frequency_resources WHERE source_key != 'us-fcc-reference-v1'"
        ).fetchone()[0])
        canonical_entry_count = int(conn.execute("SELECT COUNT(*) FROM net_directory_entries").fetchone()[0])
    assert len(frequencies) >= canonical_frequency_count
    assert len(entries) == canonical_entry_count
    assert canonical_frequency_count == legacy_count
    assert canonical_entry_count == legacy_count
    assert {resource.band for resource in frequencies if resource.source_key != "us-fcc-reference-v1"} == {"40M", "2M"}
    assert {entry.name for entry in entries} == {"Net Alpha", "Net Bravo"}


def test_linked_hf_schedule_receives_canonical_identity_and_accepted_snapshot(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import apply_resource_catalog_migration

    nets = tmp_path / "freqinout_nets.db"
    _seed_nets(nets, [_legacy_row()])
    _seed_linked_schedule(nets)
    apply_resource_catalog_migration(nets, backup_factory=_backup_factory(tmp_path / "backup"))

    with sqlite3.connect(nets) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM net_schedule_tab WHERE id=1").fetchone()
        assert row["resource_id"] == 1
        assert str(row["net_session_key"]).startswith("session_")
        assert row["accepted_session_version_hash"]
        assert row["accepted_resource_version_hash"]
        assert json.loads(row["accepted_snapshot_json"])["frequency_resource_key"]


def test_shadow_fast_path_is_read_only_and_legacy_delete_reconciles(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import (
        apply_resource_catalog_migration,
        ensure_resource_catalog_shadow,
        resource_catalog_migration_needed,
    )

    nets = tmp_path / "freqinout_nets.db"
    _seed_nets(nets, [_legacy_row(), _legacy_row(net_name="Net Bravo")])
    backup = _backup_factory(tmp_path / "backup")
    apply_resource_catalog_migration(nets, backup_factory=backup)
    assert resource_catalog_migration_needed(nets) is False
    before = nets.stat().st_mtime_ns
    report = ensure_resource_catalog_shadow(nets)
    assert report.unchanged_count == 2
    assert nets.stat().st_mtime_ns == before

    with sqlite3.connect(nets) as conn:
        conn.execute("DELETE FROM net_resources WHERE id=2")
    assert resource_catalog_migration_needed(nets) is True
    apply_resource_catalog_migration(nets, backup_factory=backup)
    with sqlite3.connect(nets) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM legacy_net_resource_map WHERE legacy_table_name='net_resources'"
        ).fetchone()[0] == 1
