from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

from freqinout.core.config_backup import ConfigBackupItem, ConfigBackupResult
from freqinout.core.varac_bbs_library_store import (
    BBS_LIBRARY_SCHEMA_VERSION,
    ensure_bbs_library_schema,
    import_legacy_station_bbs_profiles,
    list_bbs_admin_rows,
    list_bbs_location_manifest_rows,
    list_bbs_locations,
    reconcile_bbs_publications,
    set_bbs_artifact_locations,
    set_bbs_location_artifact,
    upsert_bbs_artifact_path,
    upsert_bbs_location,
)


def _create_v1_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE bbs_library_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE bbs_artifacts (
            artifact_id TEXT PRIMARY KEY,
            source_kind TEXT NOT NULL,
            source_id TEXT,
            source_path TEXT UNIQUE,
            display_name TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            mtime_ns INTEGER NOT NULL DEFAULT 0,
            content_hash TEXT,
            q_id TEXT,
            block_id TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            deleted INTEGER NOT NULL DEFAULT 0,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        );
        CREATE TABLE bbs_locations (
            location_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            source_dir TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_utc TEXT NOT NULL
        );
        CREATE TABLE bbs_location_artifacts (
            location_id TEXT NOT NULL,
            artifact_id TEXT NOT NULL,
            live_name TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0,
            visibility_rule TEXT NOT NULL DEFAULT 'public',
            retention_class TEXT NOT NULL DEFAULT 'normal',
            publish_enabled INTEGER NOT NULL DEFAULT 1,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            PRIMARY KEY(location_id, artifact_id)
        );
        INSERT INTO bbs_library_meta(key, value) VALUES('schema_version', '1');
        """
    )


def _insert_v1_row(
    conn: sqlite3.Connection,
    *,
    artifact_name: str,
    location_id: str,
    location_name: str,
    source_dir: Path,
    enabled: int = 1,
    metadata: dict[str, object] | None = None,
) -> tuple[str, str]:
    source = source_dir / artifact_name
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(f"{artifact_name}\n", encoding="utf-8")
    stat = source.stat()
    artifact_id = f"{location_id}-{artifact_name}"
    conn.execute(
        """
        INSERT INTO bbs_artifacts(
            artifact_id, source_kind, source_id, source_path, display_name,
            size, mtime_ns, content_hash, q_id, block_id, metadata_json,
            deleted, created_utc, updated_utc
        )
        VALUES(?, 'managed_location_file', ?, ?, ?, ?, ?, '', '', '', ?, 0, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
        """,
        (
            artifact_id,
            location_id,
            str(source.resolve()),
            artifact_name,
            stat.st_size,
            stat.st_mtime_ns,
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )
    conn.execute(
        """
        INSERT INTO bbs_locations(location_id, name, source_dir, enabled, metadata_json, updated_utc)
        VALUES(?, ?, ?, ?, ?, '2026-01-01T00:00:00+00:00')
        ON CONFLICT(location_id) DO UPDATE SET
            name=excluded.name,
            source_dir=excluded.source_dir,
            enabled=excluded.enabled,
            metadata_json=excluded.metadata_json,
            updated_utc=excluded.updated_utc
        """,
        (
            location_id,
            location_name,
            str(source_dir),
            enabled,
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )
    conn.execute(
        """
        INSERT INTO bbs_location_artifacts(
            location_id, artifact_id, live_name, sort_order, visibility_rule,
            retention_class, publish_enabled, created_utc, updated_utc
        )
        VALUES(?, ?, ?, 1, 'public', 'normal', 1, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
        """,
        (location_id, artifact_id, artifact_name),
    )
    return artifact_id, str(source.resolve())


def test_v1_to_v2_migration_preserves_data_and_creates_backup_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg_root = tmp_path / "cfg"
    db_path = cfg_root / "config" / "freqinout.db"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path.parent.mkdir(parents=True)

    with sqlite3.connect(db_path) as conn:
        _create_v1_schema(conn)
        artifact_id, source_path = _insert_v1_row(
            conn,
            artifact_name="status.txt",
            location_id="intel",
            location_name="Intel",
            source_dir=tmp_path / "legacy" / "intel",
            metadata={
                "retention_policy": "Archive this location by age",
                "retention_days": 7,
                "parent_location_id": "station-root",
                "access_rule": "trusted callsigns",
            },
        )
        conn.execute(
            "UPDATE bbs_locations SET metadata_json=? WHERE location_id='intel'",
            (
                json.dumps(
                    {
                        "retention_policy": "Archive this location by age",
                        "retention_days": 7,
                        "parent_location_id": "station-root",
                        "access_rule": "trusted callsigns",
                    },
                    sort_keys=True,
                ),
            ),
        )
        conn.execute(
            "INSERT INTO bbs_library_meta(key, value) VALUES('station_default_location_id', 'legacy-default')"
        )
        conn.commit()

        ensure_bbs_library_schema(conn)
        ensure_bbs_library_schema(conn)

        location = conn.execute(
            """
            SELECT parent_location_id, access_rule, retention_mode, retention_days
            FROM bbs_locations
            WHERE location_id='intel'
            """,
        ).fetchone()
        artifact = conn.execute(
            """
            SELECT source_state, missing_since_utc, last_reconciled_utc
            FROM bbs_artifacts
            WHERE artifact_id=?
            """,
            (artifact_id,),
        ).fetchone()
        version = conn.execute(
            "SELECT value FROM bbs_library_meta WHERE key='schema_version' LIMIT 1"
        ).fetchone()[0]

    backup_root = cfg_root / "backups"
    backups = sorted(backup_root.glob("bbs-library-schema-v1-to-v2-*"))
    assert version == str(BBS_LIBRARY_SCHEMA_VERSION)
    assert location == ("station-root", "trusted callsigns", "expire_after_days", 7)
    assert artifact == ("present", None, None)
    assert len(backups) == 1
    assert (backups[0] / "freqinout.db").is_file()
    assert (backups[0] / "manifest.json").is_file()
    assert source_path.endswith("status.txt")


def test_v1_to_v2_migration_backup_failure_aborts_and_leaves_v1_marker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg_root = tmp_path / "cfg"
    db_path = cfg_root / "config" / "freqinout.db"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path.parent.mkdir(parents=True)

    with sqlite3.connect(db_path) as conn:
        _create_v1_schema(conn)
        conn.execute("INSERT INTO bbs_locations(location_id, name, source_dir, enabled, metadata_json, updated_utc) VALUES('intel', 'Intel', '/legacy/intel', 1, '{}', '2026-01-01T00:00:00+00:00')")
        conn.commit()

    failure = ConfigBackupResult(
        backup_dir=str(tmp_path / "backups" / "broken"),
        reason="bbs-library-schema-v1-to-v2",
        created_at="20260101-000000",
        items=(
            ConfigBackupItem(
                original_path=str(db_path),
                backup_path="",
                kind="file",
                status="failed",
                error="simulated backup failure",
            ),
        ),
        manifest_path=str(tmp_path / "backups" / "broken" / "manifest.json"),
    )
    monkeypatch.setattr(
        "freqinout.core.varac_bbs_library_store.create_config_backup",
        lambda *args, **kwargs: failure,
    )

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(RuntimeError):
            ensure_bbs_library_schema(conn)

    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT value FROM bbs_library_meta WHERE key='schema_version' LIMIT 1"
        ).fetchone()[0]
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(bbs_artifacts)").fetchall()
        }

    assert marker == "1"
    assert "source_state" not in columns
    assert "missing_since_utc" not in columns


def test_legacy_profile_union_import_is_idempotent_and_keeps_existing_station_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_root = tmp_path / "cfg"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    db_path = cfg_root / "config" / "freqinout.db"
    db_path.parent.mkdir(parents=True)

    legacy_profiles = [
        {
            "id": 2,
            "runtime_primary": 1,
            "runtime_active": 1,
            "varac_bbs_vault_enabled": True,
            "varac_bbs_vault_managed_root": "/legacy/root",
            "varac_bbs_vault_default_location_id": "legacy-default",
            "varac_bbs_vault_global_code_policy": "Allow public locations",
            "varac_bbs_allowed_callsigns": "N1MAG, K7ETC",
            "varac_bbs_limit_access_enabled": True,
            "varac_bbs_auto_archive_days": 9,
            "varac_bbs_vault_locations_v1": [
                {
                    "id": "intel",
                    "name": "Legacy Intel",
                    "source_dir": "/legacy/intel",
                    "retention_policy": "Archive this location by age",
                    "retention_days": 7,
                    "access_rule": "trusted callsigns",
                },
                {
                    "id": "weather",
                    "name": "Weather",
                    "source_dir": "/legacy/weather",
                    "retention_mode": "manual",
                    "enabled": True,
                },
            ],
        },
        {
            "id": 1,
            "runtime_primary": 0,
            "runtime_active": 1,
            "varac_bbs_vault_locations_v1": [
                {
                    "id": "intel",
                    "name": "Secondary Intel",
                    "source_dir": "/legacy/secondary-intel",
                }
            ],
        },
    ]

    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        upsert_bbs_location(
            conn,
            location_id="intel",
            name="Station Intel",
            source_dir="/station/intel",
            enabled=True,
            parent_location_id="station-root",
            access_rule="station-only",
            retention_mode="manual",
            retention_days=0,
        )
        conn.execute(
            "INSERT INTO bbs_library_meta(key, value) VALUES('station_default_location_id', 'station-intel')"
        )
        conn.execute(
            "INSERT INTO bbs_library_meta(key, value) VALUES('global_retention_days', '42')"
        )
        conn.commit()

        imported = import_legacy_station_bbs_profiles(conn, legacy_profiles)
        rows = list_bbs_locations(conn)
        meta = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT key, value FROM bbs_library_meta WHERE key IN ('station_enabled', 'station_managed_root', 'station_default_location_id', 'station_global_code_policy', 'station_allowed_callsigns', 'station_limit_access_enabled', 'station_sweeper_rules_json', 'global_retention_days', 'legacy_profile_import_v1')"
            ).fetchall()
        }
        rerun = import_legacy_station_bbs_profiles(conn, legacy_profiles)
        intel_row = next(row for row in rows if row.location_id == "intel")
        weather_row = next(row for row in rows if row.location_id == "weather")

    assert imported == 1
    assert rerun == 0
    assert intel_row.name == "Station Intel"
    assert intel_row.source_dir == "/station/intel"
    assert intel_row.access_rule == "station-only"
    assert intel_row.retention_mode == "manual"
    assert weather_row.name == "Weather"
    assert meta["station_default_location_id"] == "station-intel"
    assert meta["station_managed_root"] == "/legacy/root"
    assert meta["station_enabled"] == "1"
    assert meta["station_allowed_callsigns"] == "N1MAG, K7ETC"
    assert meta["station_limit_access_enabled"] == "1"
    assert meta["global_retention_days"] == "42"
    assert meta["legacy_profile_import_v1"] == "complete"
    ownership_backups = sorted((cfg_root / "backups").glob("bbs-station-ownership-import-v1-*"))
    assert len(ownership_backups) == 1
    assert (ownership_backups[0] / "freqinout.db").is_file()


def test_retention_expiry_disables_publication_without_deleting_source(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    managed = tmp_path / "managed" / "intel"
    managed.mkdir(parents=True)
    source = managed / "expire.txt"
    source.write_text("keep me\n", encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        artifact_id = upsert_bbs_artifact_path(conn, source_path=source, source_kind="operator_file")
        upsert_bbs_location(
            conn,
            location_id="intel",
            name="Intel",
            source_dir=str(managed),
            enabled=True,
            retention_mode="expire_after_days",
            retention_days=1,
        )
        conn.execute(
            "UPDATE bbs_artifacts SET mtime_ns=?, updated_utc='2026-01-01T00:00:00+00:00' WHERE artifact_id=?",
            (int(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1_000_000_000), artifact_id),
        )
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id, publish_enabled=True)
        conn.commit()

        result = reconcile_bbs_publications(conn, now_utc=dt.datetime(2026, 1, 5, tzinfo=dt.timezone.utc))
        row = conn.execute(
            """
            SELECT publish_enabled, disabled_reason, expires_utc
            FROM bbs_location_artifacts
            WHERE location_id='intel' AND artifact_id=?
            """,
            (artifact_id,),
        ).fetchone()
        manifest_rows = list_bbs_location_manifest_rows(conn, "intel")
        admin_row = list_bbs_admin_rows(conn, location_id="intel", now_utc=dt.datetime(2026, 1, 5, tzinfo=dt.timezone.utc))[0]

    assert result.expired == 1
    assert row[0] == 0
    assert row[1] == "retention_expired"
    assert row[2]
    assert manifest_rows == []
    assert admin_row.publication_state == "retention_expired"
    assert source.read_text(encoding="utf-8") == "keep me\n"


def test_reconcile_bbs_publications_is_bounded_and_does_not_recount_missing_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "freqinout.db"
    managed = tmp_path / "managed" / "intel"
    managed.mkdir(parents=True)
    source = managed / "missing.txt"
    source.write_text("delete me\n", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        artifact_id = upsert_bbs_artifact_path(conn, source_path=source, source_kind="operator_file")
        upsert_bbs_location(
            conn,
            location_id="intel",
            name="Intel",
            source_dir=str(managed),
            enabled=True,
            retention_mode="manual",
            retention_days=0,
        )
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id, publish_enabled=True)
        conn.commit()

        source.unlink()
        first = reconcile_bbs_publications(conn, batch_size=1, now_utc=dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc))
        first_row = conn.execute(
            "SELECT source_state, missing_since_utc FROM bbs_artifacts WHERE artifact_id=?",
            (artifact_id,),
        ).fetchone()
        second = reconcile_bbs_publications(conn, batch_size=1, now_utc=dt.datetime(2026, 1, 3, tzinfo=dt.timezone.utc))
        second_row = conn.execute(
            "SELECT source_state, missing_since_utc FROM bbs_artifacts WHERE artifact_id=?",
            (artifact_id,),
        ).fetchone()

    assert first.checked == 1
    assert first.missing == 1
    assert first_row[0] == "missing"
    assert first_row[1]
    assert second.checked == 1
    assert second.missing == 0
    assert second_row[0] == "missing"
    assert second_row[1] == first_row[1]


def test_admin_rows_report_publication_states_and_whole_day_age(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    managed = tmp_path / "managed" / "intel"
    managed.mkdir(parents=True)
    published_source = managed / "published.txt"
    published_source.write_text("published\n", encoding="utf-8")
    disabled_source = managed / "disabled.txt"
    disabled_source.write_text("disabled\n", encoding="utf-8")
    expired_source = managed / "expired.txt"
    expired_source.write_text("expired\n", encoding="utf-8")
    missing_source = managed / "missing.txt"
    missing_source.write_text("missing\n", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        upsert_bbs_location(
            conn,
            location_id="intel",
            name="Intel",
            source_dir=str(managed),
            enabled=True,
            retention_mode="manual",
            retention_days=0,
        )
        upsert_bbs_location(
            conn,
            location_id="expired",
            name="Expired",
            source_dir=str(managed / "expired"),
            enabled=True,
            retention_mode="expire_after_days",
            retention_days=1,
        )
        published_id = upsert_bbs_artifact_path(conn, source_path=published_source, source_kind="operator_file")
        disabled_id = upsert_bbs_artifact_path(conn, source_path=disabled_source, source_kind="operator_file")
        expired_id = upsert_bbs_artifact_path(conn, source_path=expired_source, source_kind="operator_file")
        missing_id = upsert_bbs_artifact_path(conn, source_path=missing_source, source_kind="operator_file")
        conn.execute(
            "UPDATE bbs_artifacts SET mtime_ns=? WHERE artifact_id=?",
            ((dt.datetime(2026, 1, 3, 3, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000_000, published_id),
        )
        conn.execute(
            "UPDATE bbs_artifacts SET mtime_ns=? WHERE artifact_id=?",
            ((dt.datetime(2026, 1, 3, 3, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000_000, disabled_id),
        )
        conn.execute(
            "UPDATE bbs_artifacts SET mtime_ns=? WHERE artifact_id=?",
            ((dt.datetime(2025, 12, 31, 0, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000_000, expired_id),
        )
        conn.execute(
            "UPDATE bbs_artifacts SET mtime_ns=? WHERE artifact_id=?",
            ((dt.datetime(2026, 1, 4, 12, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000_000, missing_id),
        )
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=published_id, publish_enabled=True)
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=disabled_id, publish_enabled=False)
        set_bbs_location_artifact(
            conn,
            location_id="intel",
            artifact_id=expired_id,
            publish_enabled=True,
            expires_utc="2026-01-04T00:00:00+00:00",
        )
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=missing_id, publish_enabled=True)
        conn.commit()

        disabled_source.unlink()
        missing_source.unlink()
        reconcile_bbs_publications(conn, now_utc=dt.datetime(2026, 1, 5, tzinfo=dt.timezone.utc))
        rows = list_bbs_admin_rows(conn, location_id="intel", now_utc=dt.datetime(2026, 1, 5, tzinfo=dt.timezone.utc))
        by_name = {row.display_name: row for row in rows}

    assert by_name["published.txt"].publication_state == "published"
    assert by_name["published.txt"].published is True
    assert by_name["published.txt"].age_days == 1
    assert by_name["disabled.txt"].publication_state == "operator_disabled"
    assert by_name["expired.txt"].publication_state == "retention_expired"
    assert by_name["missing.txt"].publication_state == "source_missing"
    assert by_name["missing.txt"].age_days == 0


def test_set_bbs_artifact_locations_is_atomic_and_preserves_operator_intent(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    managed = tmp_path / "managed" / "intel"
    managed.mkdir(parents=True)
    source = managed / "status.txt"
    source.write_text("regional\n", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        artifact_id = upsert_bbs_artifact_path(conn, source_path=source, source_kind="operator_file")
        upsert_bbs_location(conn, location_id="intel", name="Intel", source_dir=str(managed))
        upsert_bbs_location(conn, location_id="weather", name="Weather", source_dir=str(managed / "weather"))
        set_bbs_artifact_locations(conn, artifact_id=artifact_id, location_ids=["intel", "weather"])
        selected = list(conn.execute(
            "SELECT location_id, publish_enabled, disabled_reason FROM bbs_location_artifacts WHERE artifact_id=? ORDER BY location_id",
            (artifact_id,),
        ).fetchall())
        with pytest.raises(ValueError):
            set_bbs_artifact_locations(conn, artifact_id=artifact_id, location_ids=["intel", "missing"])
        after_error = list(conn.execute(
            "SELECT location_id, publish_enabled, disabled_reason FROM bbs_location_artifacts WHERE artifact_id=? ORDER BY location_id",
            (artifact_id,),
        ).fetchall())
        set_bbs_artifact_locations(conn, artifact_id=artifact_id, location_ids=["weather"])
        manifest_rows = list_bbs_location_manifest_rows(conn, "weather")
        intel_rows = list_bbs_location_manifest_rows(conn, "intel")

    assert selected == [("intel", 1, ""), ("weather", 1, "")]
    assert after_error == selected
    assert manifest_rows and manifest_rows[0].source_path == str(source.resolve())
    assert intel_rows == []


def test_location_retention_change_recalculates_existing_mapping_expiry(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    source = tmp_path / "status.txt"
    source.write_text("status\n", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        artifact_id = upsert_bbs_artifact_path(conn, source_path=source)
        artifact_mtime = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        conn.execute(
            "UPDATE bbs_artifacts SET mtime_ns=? WHERE artifact_id=?",
            (int(artifact_mtime.timestamp() * 1_000_000_000), artifact_id),
        )
        upsert_bbs_location(
            conn,
            location_id="intel",
            name="Intel",
            retention_mode="manual",
        )
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id)
        manual_expiry = conn.execute(
            "SELECT expires_utc FROM bbs_location_artifacts WHERE location_id='intel' AND artifact_id=?",
            (artifact_id,),
        ).fetchone()[0]

        upsert_bbs_location(
            conn,
            location_id="intel",
            name="Intel",
            retention_mode="expire_after_days",
            retention_days=7,
        )
        seven_day_expiry = conn.execute(
            "SELECT expires_utc FROM bbs_location_artifacts WHERE location_id='intel' AND artifact_id=?",
            (artifact_id,),
        ).fetchone()[0]

        upsert_bbs_location(
            conn,
            location_id="intel",
            name="Intel",
            retention_mode="manual",
        )
        restored_manual_expiry = conn.execute(
            "SELECT expires_utc FROM bbs_location_artifacts WHERE location_id='intel' AND artifact_id=?",
            (artifact_id,),
        ).fetchone()[0]

    assert manual_expiry is None
    assert seven_day_expiry == "2026-01-08T00:00:00+00:00"
    assert restored_manual_expiry is None
