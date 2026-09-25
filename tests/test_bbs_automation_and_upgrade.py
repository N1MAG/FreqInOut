from __future__ import annotations

import sqlite3
import importlib
from pathlib import Path

from freqinout.core.js8_message_schema import ensure_js8_message_cache_schema
from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.varac_bbs_automation import apply_station_bbs_automation
from freqinout.core.varac_bbs_initialization import initialize_station_bbs_library
from freqinout.core.varac_bbs_library_store import (
    list_bbs_locations,
    load_station_bbs_sweeper_rules,
    save_station_bbs_sweeper_rules,
    upsert_bbs_location,
)


def test_js8_single_rig_schema_upgrade_preserves_rows_and_adds_source_identity(tmp_path):
    db_path = tmp_path / "freqinout_nets.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE js8_messages (id INTEGER PRIMARY KEY, from_call TEXT, decoded_text TEXT)"
        )
        conn.execute(
            "CREATE TABLE js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL)"
        )
        conn.execute(
            "INSERT INTO js8_messages(id, from_call, decoded_text) VALUES(7, 'W1ABC', 'legacy')"
        )
        ensure_js8_message_cache_schema(conn)
        row = conn.execute(
            "SELECT id, source_key, source_id, decoded_text FROM js8_messages WHERE id=7"
        ).fetchone()
        assert row == (7, "", 7, "legacy")
        columns = {entry[1] for entry in conn.execute("PRAGMA table_info(js8_messages)")}
        assert {"source_key", "source_id", "source_radio_id", "js8_instance_id", "source_path"} <= columns
        # Idempotence is essential because both startup and ingestion call it.
        ensure_js8_message_cache_schema(conn)


def test_first_20_startup_upgrades_legacy_js8_before_projection(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    db_path = config_dir / "freqinout_nets.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE js8_messages (
                id INTEGER PRIMARY KEY, from_call TEXT, to_call TEXT, msg_type TEXT,
                utc_str TEXT, utc_ts REAL, raw_text TEXT, decoded_text TEXT, state TEXT
            )"""
        )
        conn.execute(
            "INSERT INTO js8_messages(id, from_call, decoded_text) VALUES(11, 'W1ABC', 'upgrade')"
        )

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)
    monkeypatch.setattr(db_initializer, "_config_dir", lambda: config_dir)
    db_initializer._ensure_nets_db()

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT source_key, source_id, decoded_text FROM js8_messages WHERE id=11"
        ).fetchone()
        assert row == ("", 11, "upgrade")
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_projection'"
        ).fetchone() is not None


def test_station_bbs_initializer_is_non_destructive_and_idempotent(tmp_path):
    db_path = tmp_path / "freqinout.db"
    live_dir = tmp_path / "VarAC" / "BBS"
    live_dir.mkdir(parents=True)
    source = live_dir / "message.txt"
    source.write_text("existing traffic", encoding="utf-8")

    first = initialize_station_bbs_library(db_path, live_dir, import_existing_files=True)
    second = initialize_station_bbs_library(db_path, live_dir, import_existing_files=True)

    assert source.read_text(encoding="utf-8") == "existing traffic"
    assert first.imported_files == 1
    assert second.imported_files == 0
    assert Path(first.default_location_dir, "message.txt").exists()
    with connect_sqlite(db_path) as conn:
        locations = list_bbs_locations(conn)
        assert [(row.location_id, row.name) for row in locations] == [("default", "Default")]
        assert conn.execute(
            "SELECT value FROM bbs_library_meta WHERE key='station_enabled'"
        ).fetchone()[0] == "1"


def test_station_bbs_automation_uses_canonical_rules_and_deduplicates_force_rescans(tmp_path):
    db_path = tmp_path / "freqinout.db"
    target = tmp_path / "managed" / "Default"
    target.mkdir(parents=True)
    incoming = tmp_path / "incoming" / "W1ABC-status.k2s"
    incoming.parent.mkdir()
    incoming.write_text(":hdr_fm:\nW1ABC\nstation status green", encoding="utf-8")
    stat = incoming.stat()
    record = FileRecord(
        path=incoming,
        origin="flmsg",
        size=stat.st_size,
        mtime=stat.st_mtime,
    )
    with connect_sqlite(db_path) as conn:
        with conn:
            upsert_bbs_location(
                conn,
                location_id="default",
                name="Default",
                source_dir=str(target),
            )
            conn.execute(
                "INSERT OR REPLACE INTO bbs_library_meta(key, value) VALUES('station_enabled', '1')"
            )
            save_station_bbs_sweeper_rules(
                conn,
                [
                    {
                        "id": "status",
                        "name": "Status reports",
                        "enabled": True,
                        "source_families": ["flmsg"],
                        "from_calls": ["W1ABC"],
                        "target_location_ids": ["default"],
                        "copy_mode": "copy",
                    }
                ],
            )

    first = apply_station_bbs_automation(db_path, [record])
    second = apply_station_bbs_automation(db_path, [record])

    assert first.copied == 1 and first.error == ""
    assert second.copied == 0 and second.error == ""
    assert len(list(target.iterdir())) == 1
    with connect_sqlite(db_path) as conn:
        assert load_station_bbs_sweeper_rules(conn)[0]["id"] == "status"


def test_saved_empty_station_rules_do_not_revive_legacy_profile_rules(tmp_path):
    db_path = tmp_path / "freqinout.db"
    target = tmp_path / "managed"
    target.mkdir()
    incoming = tmp_path / "W1ABC-status.k2s"
    incoming.write_text("W1ABC status", encoding="utf-8")
    stat = incoming.stat()
    with connect_sqlite(db_path) as conn:
        with conn:
            upsert_bbs_location(
                conn,
                location_id="default",
                name="Default",
                source_dir=str(target),
            )
            save_station_bbs_sweeper_rules(conn, [])
    legacy_rule = {
        "id": "legacy",
        "name": "Legacy",
        "enabled": True,
        "source_families": ["flmsg"],
        "from_calls": ["W1ABC"],
        "target_location_ids": ["default"],
    }
    result = apply_station_bbs_automation(
        db_path,
        [FileRecord(path=incoming, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)],
        legacy_rules=[legacy_rule],
        legacy_enabled=True,
    )
    assert result.copied == 0
    assert not list(target.iterdir())
