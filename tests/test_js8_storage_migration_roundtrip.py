"""JSV-S4 persistence regressions for modern JS8Call storage identity.

These tests exercise the additive database migration boundary.  They keep the
legacy file paths deliberately real so a future schema migration cannot start
"helpfully" moving or rewriting JS8Call-owned message files.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from freqinout.core.multi_radio_store import MultiRadioStore


def _create_pre_jsv_s4_database(path: Path, *, directed_path: Path, inbox_path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """CREATE TABLE js8_instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT, system_key TEXT UNIQUE,
                name TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                host TEXT NOT NULL DEFAULT '127.0.0.1', port INTEGER NOT NULL DEFAULT 2442,
                offset_hz INTEGER NOT NULL DEFAULT 0, profile_path TEXT,
                directed_path TEXT, inbox_path TEXT, forms_path TEXT, install_path TEXT,
                spotter_launch_path TEXT, commstat_launch_path TEXT,
                created_utc TEXT NOT NULL, updated_utc TEXT NOT NULL
            )"""
        )
        conn.execute(
            """INSERT INTO js8_instances(
                system_key, name, directed_path, inbox_path, created_utc, updated_utc
            ) VALUES (?, ?, ?, ?, ?, ?)""",
            ("legacy-js8", "Legacy JS8", str(directed_path), str(inbox_path), "then", "then"),
        )
        conn.commit()
    finally:
        conn.close()


def test_jsv_s4_storage_migration_is_additive_idempotent_and_leaves_legacy_files_untouched(tmp_path: Path) -> None:
    legacy_root = tmp_path / "legacy-message-store"
    directed = legacy_root / "DIRECTED.TXT"
    inbox = legacy_root / "inbox.db3"
    all_txt = legacy_root / "ALL.TXT"
    legacy_root.mkdir()
    directed.write_bytes(b"legacy directed bytes\x00\xff")
    inbox.write_bytes(b"legacy inbox bytes\x01\xfe")
    all_txt.write_bytes(b"legacy all bytes\x02\xfd")
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (directed, inbox, all_txt)
    }
    db_path = tmp_path / "legacy.sqlite"
    _create_pre_jsv_s4_database(db_path, directed_path=directed, inbox_path=inbox)

    store = MultiRadioStore(db_path)
    with store.connect() as first_connection:
        first_columns = tuple(row[1] for row in first_connection.execute("PRAGMA table_info(js8_instances)"))
        first_row = first_connection.execute(
            """SELECT directed_path, inbox_path, variant_family, storage_mode,
                      application_data_root, all_path, save_dir, storage_evidence
                 FROM js8_instances WHERE system_key='legacy-js8'"""
        ).fetchone()
    # A second open is the migration idempotence check: it must not create a
    # different schema or mutate the pre-existing record/files.
    with store.connect() as second_connection:
        second_columns = tuple(row[1] for row in second_connection.execute("PRAGMA table_info(js8_instances)"))
        second_row = second_connection.execute(
            """SELECT directed_path, inbox_path, variant_family, storage_mode,
                      application_data_root, all_path, save_dir, storage_evidence
                 FROM js8_instances WHERE system_key='legacy-js8'"""
        ).fetchone()

    assert {
        "variant_family",
        "variant_version",
        "rig_name",
        "rig_name_source",
        "application_data_root",
        "all_path",
        "save_dir",
        "storage_mode",
        "storage_verified_utc",
        "storage_evidence",
    } <= set(first_columns)
    assert second_columns == first_columns
    assert tuple(first_row) == tuple(second_row)
    assert tuple(first_row[:4]) == (str(directed), str(inbox), "unknown", "unverified")
    assert tuple(first_row[4:]) == (None, None, None, None)
    assert {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (directed, inbox, all_txt)
    } == before


def test_jsv_s4_storage_identity_round_trips_all_paths_and_verification(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    message_root = tmp_path / "js8-application-data"
    save_dir = tmp_path / "operator-saves"
    directed = message_root / "DIRECTED.TXT"
    all_txt = message_root / "ALL.TXT"
    inbox = message_root / "inbox.db3"
    store = MultiRadioStore(db_path)

    saved = store.save_js8_instance(
        {
            "system_key": "jsv-s4-field-alpha",
            "name": "Field Alpha JS8",
            "variant_family": "improved",
            "variant_version": "3.0.3",
            "rig_name": "Field Alpha",
            "rig_name_source": "managed",
            "application_data_root": str(message_root),
            "all_path": str(all_txt),
            "directed_path": str(directed),
            "inbox_path": str(inbox),
            "save_dir": str(save_dir),
            "storage_mode": "rig_scoped",
            "storage_verified_utc": "2026-09-10T12:34:56Z",
            "storage_evidence": "runtime_verified:settings+message_files",
        }
    )
    reloaded = MultiRadioStore(db_path).get_js8_instance(int(saved["id"]))

    assert reloaded is not None
    assert reloaded["variant_family"] == "js8call_improved_3_0_3"
    assert reloaded["variant_version"] == "3.0.3"
    assert reloaded["rig_name"] == "Field Alpha"
    assert reloaded["rig_name_source"] == "managed"
    assert reloaded["application_data_root"] == str(message_root.resolve())
    assert reloaded["all_path"] == str(all_txt)
    assert reloaded["directed_path"] == str(directed)
    assert reloaded["inbox_path"] == str(inbox)
    assert reloaded["save_dir"] == str(save_dir)
    assert reloaded["save_dir"] != reloaded["application_data_root"]
    assert reloaded["storage_mode"] == "rig_scoped"
    assert reloaded["storage_verified_utc"] == "2026-09-10T12:34:56Z"
    assert reloaded["storage_evidence"] == "runtime_verified:settings+message_files"
