"""
Ensure core SQLite tables exist before the UI starts.
This avoids runtime errors such as "no such table: js8_links".
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Set

from freqinout.core.logger import log
from freqinout.core.config_paths import get_config_dir

# Base config directory (user-writable)
CONFIG_DIR = get_config_dir() / "config"


def _ensure_settings_db() -> None:
    """
    Ensure settings DB (freqinout.db) has the kv table.
    """
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    db_path = CONFIG_DIR / "freqinout.db"
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
        conn.commit()
    finally:
        conn.close()


def _ensure_operator_checkins(conn: sqlite3.Connection) -> None:
    """
    Ensure operator_checkins has the unified columns used throughout the app.
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS operator_checkins (
            callsign TEXT PRIMARY KEY,
            name TEXT,
            state TEXT,
            grid TEXT,
            group1 TEXT,
            group2 TEXT,
            group3 TEXT,
            group_role TEXT,
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            last_net TEXT,
            last_role TEXT,
            checkin_count INTEGER DEFAULT 0,
            groups_json TEXT,
            trusted INTEGER DEFAULT 1
        )
        """
    )
    cur.execute("PRAGMA table_info(operator_checkins)")
    cols: Set[str] = {row[1] for row in cur.fetchall()}
    desired = {
        "callsign",
        "name",
        "state",
        "grid",
        "group1",
        "group2",
        "group3",
        "group_role",
        "first_seen_utc",
        "last_seen_utc",
        "last_net",
        "last_role",
        "checkin_count",
        "groups_json",
        "trusted",
    }
    if not desired.issubset(cols):
        # Recreate with the full schema and copy rows forward
        cur.execute("DROP TABLE IF EXISTS operator_checkins_new")
        cur.execute(
            """
            CREATE TABLE operator_checkins_new (
                callsign TEXT PRIMARY KEY,
                name TEXT,
                state TEXT,
                grid TEXT,
                group1 TEXT,
                group2 TEXT,
                group3 TEXT,
                group_role TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                last_net TEXT,
                last_role TEXT,
                checkin_count INTEGER DEFAULT 0,
                groups_json TEXT,
                trusted INTEGER DEFAULT 1
            )
            """
        )
        cur.execute(
            """
            INSERT OR REPLACE INTO operator_checkins_new
                (callsign, name, state, grid, group1, group2, group3, group_role,
                 first_seen_utc, last_seen_utc, last_net, last_role,
                 checkin_count, groups_json, trusted)
            SELECT
                callsign,
                name,
                state,
                grid,
                group1,
                group2,
                group3,
                group_role,
                first_seen_utc,
                last_seen_utc,
                last_net,
                last_role,
                checkin_count,
                groups_json,
                trusted
            FROM operator_checkins
            """
        )
        cur.execute("DROP TABLE operator_checkins")
        cur.execute("ALTER TABLE operator_checkins_new RENAME TO operator_checkins")


def _ensure_js8_links(conn: sqlite3.Connection) -> None:
    """
    Ensure js8_links exists with the expected columns.
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_links (
            ts REAL,
            origin TEXT,
            destination TEXT,
            snr REAL,
            band TEXT,
            freq_hz REAL,
            is_relay INTEGER DEFAULT 0,
            relay_via TEXT,
            is_spotter INTEGER DEFAULT 0
        )
        """
    )
    cur.execute("PRAGMA table_info(js8_links)")
    cols = {row[1] for row in cur.fetchall()}
    if "last_seen_utc" not in cols:
        cur.execute("ALTER TABLE js8_links ADD COLUMN last_seen_utc TEXT")


def _ensure_nets_db() -> None:
    """
    Ensure nets DB (freqinout_nets.db) has required tables.
    """
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    db_path = CONFIG_DIR / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # Daily / Net schedules
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_schedule_tab (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                day_utc TEXT NOT NULL,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                vfo TEXT,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                group_name TEXT,
                auto_tune INTEGER DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS net_schedule_tab (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                day_utc TEXT NOT NULL,
                recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                vfo TEXT,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                early_checkin INTEGER NOT NULL,
                primary_js8call_group TEXT,
                comment TEXT,
                net_name TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS net_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                day_utc TEXT NOT NULL,
                recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                early_checkin INTEGER NOT NULL,
                primary_js8call_group TEXT,
                comment TEXT,
                net_name TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS message_viewer_paths (
                origin TEXT,
                path TEXT UNIQUE
            )
            """
        )

        # Auto-query backlog for JS8 (MSG IDs / GRID requests)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS autoquery_backlog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                callsign TEXT NOT NULL,
                msg_id TEXT,
                kind TEXT NOT NULL,           -- 'MSG' or 'GRID'
                status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING / RETRIEVED / FAILED
                attempts INTEGER DEFAULT 0,
                last_attempt_ts REAL,
                created_ts REAL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_autoquery_callsign ON autoquery_backlog(callsign)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_autoquery_status ON autoquery_backlog(status)")

        # Peer HF schedule (imported from other operators)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS peer_hf_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_callsign TEXT NOT NULL,
                day_utc TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                frequency TEXT NOT NULL,
                meta_json TEXT,
                imported_at TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_peer_hf_owner ON peer_hf_schedule(owner_callsign)")

        _ensure_operator_checkins(conn)
        _ensure_js8_links(conn)

        conn.commit()
    finally:
        conn.close()


def ensure_all_tables() -> None:
    """
    Public entry point to ensure both DBs are initialized.
    """
    _ensure_settings_db()
    _ensure_nets_db()
    log.info("DB init: ensured core tables (settings and nets).")
