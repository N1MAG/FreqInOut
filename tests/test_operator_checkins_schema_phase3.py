from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

from freqinout.core.checkins_db import ensure_operator_checkins_schema


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_core_operator_checkins_schema_migrates_legacy_layout():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE operator_checkins (
                callsign TEXT PRIMARY KEY,
                name TEXT,
                state TEXT,
                grid TEXT,
                group1 TEXT,
                group2 TEXT,
                group3 TEXT,
                group_role TEXT,
                date_added TEXT,
                checkin_count INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO operator_checkins
                (callsign, name, state, grid, group1, group2, group3, group_role, date_added, checkin_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("K1ABC", "Bill", "CO", "DM79", "Alpha", "Bravo", "", "bogus", "20240301", 7),
        )

        ensure_operator_checkins_schema(conn, repair_data=True)

        cols = _table_columns(conn, "operator_checkins")
        assert {
            "first_seen_utc",
            "last_seen_utc",
            "last_net",
            "last_role",
            "groups_json",
            "trusted",
        }.issubset(cols)

        row = conn.execute(
            """
            SELECT first_seen_utc, last_seen_utc, groups_json, trusted, group_role, checkin_count
            FROM operator_checkins
            WHERE callsign='K1ABC'
            """
        ).fetchone()

        assert row == ("20240301", "", '["ALPHA", "BRAVO"]', 0, "", 7)
    finally:
        conn.close()


def test_core_operator_checkins_repair_normalizes_commstat_groups():
    conn = sqlite3.connect(":memory:")
    try:
        ensure_operator_checkins_schema(conn)
        conn.execute(
            """
            INSERT INTO operator_checkins
                (callsign, name, state, group1, group2, group3, groups_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("K1ABC", "Bill", "CO", "@MAGNET", "MAGNET", "", '["@MAGNET","MAGNET","ARES"]'),
        )

        ensure_operator_checkins_schema(conn, repair_data=True)

        row = conn.execute(
            """
            SELECT group1, group2, group3, groups_json
            FROM operator_checkins
            WHERE callsign='K1ABC'
            """
        ).fetchone()
        assert row == ("MAGNET", "ARES", "", '["MAGNET", "ARES"]')
    finally:
        conn.close()


def test_db_initializer_migrates_operator_checkins_before_ui(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)

    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    nets_db = config_dir / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        conn.execute(
            """
            CREATE TABLE operator_checkins (
                callsign TEXT PRIMARY KEY,
                name TEXT,
                state TEXT,
                grid TEXT,
                group1 TEXT,
                group2 TEXT,
                group3 TEXT,
                group_role TEXT,
                date_added TEXT,
                checkin_count INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO operator_checkins
                (callsign, name, state, grid, group1, group2, group3, group_role, date_added, checkin_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("N0CALL", "Operator", "NM", "DM65", "Hub", "", "", "NCS", "20240229", 3),
        )
        conn.commit()
    finally:
        conn.close()

    db_initializer.ensure_all_tables()

    conn = sqlite3.connect(nets_db)
    try:
        cols = _table_columns(conn, "operator_checkins")
        assert {
            "first_seen_utc",
            "last_seen_utc",
            "last_net",
            "last_role",
            "groups_json",
            "trusted",
        }.issubset(cols)

        row = conn.execute(
            """
            SELECT first_seen_utc, groups_json, trusted, group_role, checkin_count
            FROM operator_checkins
            WHERE callsign='N0CALL'
            """
        ).fetchone()
        assert row == ("20240229", '["HUB"]', 0, "NCS", 3)
    finally:
        conn.close()


def test_db_admin_init_creates_unified_operator_checkins_schema(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    tools_dir = Path(__file__).resolve().parents[1] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))

    import db_admin
    import db_schema

    db_schema = importlib.reload(db_schema)
    db_admin = importlib.reload(db_admin)

    db_admin.ensure_tables(["operator_checkins"])

    conn = sqlite3.connect(db_schema.NETS_DB)
    try:
        cols = _table_columns(conn, "operator_checkins")
        assert {
            "callsign",
            "first_seen_utc",
            "last_seen_utc",
            "last_net",
            "last_role",
            "groups_json",
            "trusted",
        }.issubset(cols)
        assert "date_added" not in cols
    finally:
        conn.close()
