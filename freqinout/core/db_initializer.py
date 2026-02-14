"""
Ensure core SQLite tables exist before the UI starts.
This avoids runtime errors such as "no such table: js8_links".
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Set

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


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for name, col_type in columns.items():
        if name in existing:
            continue
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")


def _table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _ensure_prop_contact_events(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS prop_contact_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL,
            ts_utc TEXT NOT NULL,
            origin_callsign TEXT,
            origin_grid6 TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            target_callsign TEXT,
            target_grid6 TEXT,
            band TEXT NOT NULL,
            mode TEXT,
            freq_hz REAL,
            distance_km REAL,
            outcome TEXT NOT NULL,
            source TEXT NOT NULL,
            source_ref TEXT,
            inserted_utc TEXT NOT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "prop_contact_events",
        {
            "event_key": "TEXT",
            "ts_utc": "TEXT",
            "origin_callsign": "TEXT",
            "origin_grid6": "TEXT",
            "target_type": "TEXT",
            "target_id": "TEXT",
            "target_callsign": "TEXT",
            "target_grid6": "TEXT",
            "band": "TEXT",
            "mode": "TEXT",
            "freq_hz": "REAL",
            "distance_km": "REAL",
            "outcome": "TEXT",
            "source": "TEXT",
            "source_ref": "TEXT",
            "inserted_utc": "TEXT",
        },
    )
    cols = _table_columns(conn, "prop_contact_events")
    if "id" in cols and "event_key" in cols:
        # Safety: backfill missing keys in older/dev DBs before enforcing uniqueness.
        cur.execute(
            """
            UPDATE prop_contact_events
            SET event_key = 'legacy:' || id
            WHERE event_key IS NULL OR TRIM(event_key) = ''
            """
        )
    # Safety: normalize categorical fields to reduce downstream parsing edge-cases.
    cur.execute("UPDATE prop_contact_events SET target_type = UPPER(TRIM(target_type)) WHERE target_type IS NOT NULL")
    cur.execute("UPDATE prop_contact_events SET outcome = UPPER(TRIM(outcome)) WHERE outcome IS NOT NULL")
    cur.execute("UPDATE prop_contact_events SET source = UPPER(TRIM(source)) WHERE source IS NOT NULL")
    cur.execute("UPDATE prop_contact_events SET band = UPPER(TRIM(band)) WHERE band IS NOT NULL")
    cur.execute("UPDATE prop_contact_events SET origin_grid6 = UPPER(TRIM(origin_grid6)) WHERE origin_grid6 IS NOT NULL")
    cur.execute("UPDATE prop_contact_events SET target_grid6 = UPPER(TRIM(target_grid6)) WHERE target_grid6 IS NOT NULL")
    # Safety: collapse duplicate event_key rows to avoid index creation failure.
    cur.execute(
        """
        DELETE FROM prop_contact_events
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM prop_contact_events
            GROUP BY event_key
        )
        """
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_prop_contact_events_event_key ON prop_contact_events(event_key)"
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prop_contact_events_lookup
        ON prop_contact_events(origin_grid6, target_type, target_id, band, ts_utc)
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_prop_contact_events_source ON prop_contact_events(source, ts_utc)"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prop_contact_events_inserted ON prop_contact_events(inserted_utc)")


def _ensure_prop_ingest_checkpoint(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS prop_ingest_checkpoint (
            source TEXT PRIMARY KEY,
            last_ts_utc TEXT,
            last_source_ref TEXT,
            updated_utc TEXT NOT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "prop_ingest_checkpoint",
        {
            "source": "TEXT",
            "last_ts_utc": "TEXT",
            "last_source_ref": "TEXT",
            "updated_utc": "TEXT",
        },
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_prop_ingest_checkpoint_updated ON prop_ingest_checkpoint(updated_utc)"
    )


def _ensure_prop_outcome_stats(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS prop_outcome_stats (
            key_hash TEXT PRIMARY KEY,
            origin_grid6 TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            band TEXT NOT NULL,
            mode TEXT,
            month INTEGER NOT NULL,
            utc_hour_bucket INTEGER NOT NULL,
            distance_bucket TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            success_count INTEGER NOT NULL DEFAULT 0,
            weighted_attempt REAL NOT NULL DEFAULT 0,
            weighted_success REAL NOT NULL DEFAULT 0,
            last_event_utc TEXT,
            updated_utc TEXT NOT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "prop_outcome_stats",
        {
            "key_hash": "TEXT",
            "origin_grid6": "TEXT",
            "target_type": "TEXT",
            "target_id": "TEXT",
            "band": "TEXT",
            "mode": "TEXT",
            "month": "INTEGER",
            "utc_hour_bucket": "INTEGER",
            "distance_bucket": "TEXT",
            "attempt_count": "INTEGER DEFAULT 0",
            "success_count": "INTEGER DEFAULT 0",
            "weighted_attempt": "REAL DEFAULT 0",
            "weighted_success": "REAL DEFAULT 0",
            "last_event_utc": "TEXT",
            "updated_utc": "TEXT",
        },
    )
    # Safety: keep stats in valid ranges after schema drift/manual edits.
    cur.execute("UPDATE prop_outcome_stats SET month = MIN(12, MAX(1, CAST(month AS INTEGER))) WHERE month IS NOT NULL")
    cur.execute(
        """
        UPDATE prop_outcome_stats
        SET utc_hour_bucket = MIN(23, MAX(0, CAST(utc_hour_bucket AS INTEGER)))
        WHERE utc_hour_bucket IS NOT NULL
        """
    )
    cur.execute("UPDATE prop_outcome_stats SET attempt_count = MAX(0, CAST(attempt_count AS INTEGER))")
    cur.execute("UPDATE prop_outcome_stats SET success_count = MAX(0, CAST(success_count AS INTEGER))")
    cur.execute("UPDATE prop_outcome_stats SET weighted_attempt = MAX(0, weighted_attempt)")
    cur.execute("UPDATE prop_outcome_stats SET weighted_success = MAX(0, weighted_success)")
    cur.execute("UPDATE prop_outcome_stats SET band = UPPER(TRIM(band)) WHERE band IS NOT NULL")
    cur.execute("UPDATE prop_outcome_stats SET origin_grid6 = UPPER(TRIM(origin_grid6)) WHERE origin_grid6 IS NOT NULL")
    cur.execute("UPDATE prop_outcome_stats SET distance_bucket = UPPER(TRIM(distance_bucket)) WHERE distance_bucket IS NOT NULL")
    cur.execute("UPDATE prop_outcome_stats SET target_type = UPPER(TRIM(target_type)) WHERE target_type IS NOT NULL")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prop_outcome_stats_lookup
        ON prop_outcome_stats(origin_grid6, target_type, target_id, band, month, utc_hour_bucket)
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_prop_outcome_stats_updated ON prop_outcome_stats(updated_utc)"
    )


def _ensure_propagation_outcome_tables(conn: sqlite3.Connection) -> None:
    _ensure_prop_contact_events(conn)
    _ensure_prop_ingest_checkpoint(conn)
    _ensure_prop_outcome_stats(conn)


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
        _ensure_columns(
            conn,
            "daily_schedule_tab",
            {
                "day_utc": "TEXT",
                "band": "TEXT",
                "mode": "TEXT",
                "vfo": "TEXT",
                "frequency": "TEXT",
                "start_utc": "TEXT",
                "end_utc": "TEXT",
                "group_name": "TEXT",
                "auto_tune": "INTEGER DEFAULT 0",
            },
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS net_schedule_tab (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                day_utc TEXT NOT NULL,
                recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0,
                month_weeks TEXT,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                vfo TEXT,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                early_checkin INTEGER NOT NULL,
                auto_tune INTEGER DEFAULT 0,
                primary_js8call_group TEXT,
                comment TEXT,
                net_name TEXT
            )
            """
        )
        _ensure_columns(
            conn,
            "net_schedule_tab",
            {
                "day_utc": "TEXT",
                "recurrence": "TEXT",
                "biweekly_offset_weeks": "INTEGER DEFAULT 0",
                "month_weeks": "TEXT",
                "band": "TEXT",
                "mode": "TEXT",
                "vfo": "TEXT",
                "frequency": "TEXT",
                "start_utc": "TEXT",
                "end_utc": "TEXT",
                "early_checkin": "INTEGER DEFAULT 0",
                "auto_tune": "INTEGER DEFAULT 0",
                "primary_js8call_group": "TEXT",
                "comment": "TEXT",
                "net_name": "TEXT",
            },
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS net_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                day_utc TEXT NOT NULL,
                recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0,
                month_weeks TEXT,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                early_checkin INTEGER NOT NULL,
                auto_tune INTEGER DEFAULT 0,
                primary_js8call_group TEXT,
                comment TEXT,
                net_name TEXT
            )
            """
        )
        _ensure_columns(
            conn,
            "net_schedule",
            {
                "day_utc": "TEXT",
                "recurrence": "TEXT",
                "biweekly_offset_weeks": "INTEGER DEFAULT 0",
                "month_weeks": "TEXT",
                "band": "TEXT",
                "mode": "TEXT",
                "frequency": "TEXT",
                "start_utc": "TEXT",
                "end_utc": "TEXT",
                "early_checkin": "INTEGER DEFAULT 0",
                "auto_tune": "INTEGER DEFAULT 0",
                "primary_js8call_group": "TEXT",
                "comment": "TEXT",
                "net_name": "TEXT",
            },
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS message_viewer_paths (
                origin TEXT,
                path TEXT UNIQUE
            )
            """
        )
        # FLDigi check-ins (used for map tooltip mode flags)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fldigi_checkins (
                callsign TEXT PRIMARY KEY,
                last_seen_ts REAL
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
        _ensure_columns(
            conn,
            "autoquery_backlog",
            {
                "callsign": "TEXT",
                "msg_id": "TEXT",
                "kind": "TEXT",
                "status": "TEXT",
                "attempts": "INTEGER DEFAULT 0",
                "last_attempt_ts": "REAL",
                "created_ts": "REAL",
            },
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
        _ensure_columns(
            conn,
            "peer_hf_schedule",
            {
                "owner_callsign": "TEXT",
                "day_utc": "TEXT",
                "start_utc": "TEXT",
                "end_utc": "TEXT",
                "band": "TEXT",
                "mode": "TEXT",
                "frequency": "TEXT",
                "meta_json": "TEXT",
                "imported_at": "TEXT",
            },
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_peer_hf_owner ON peer_hf_schedule(owner_callsign)")

        # Propagation outcomes (offline scoring support)
        _ensure_propagation_outcome_tables(conn)

        # SOP profiles/actions/state
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sop_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                operating_group TEXT NOT NULL,
                secondary_group TEXT,
                frequency TEXT NOT NULL,
                sop_start_utc TEXT NOT NULL,
                active INTEGER DEFAULT 0,
                window_hours INTEGER DEFAULT 12,
                created_utc TEXT,
                updated_utc TEXT
            )
            """
        )
        _ensure_columns(
            conn,
            "sop_profiles",
            {
                "name": "TEXT",
                "operating_group": "TEXT",
                "secondary_group": "TEXT",
                "frequency": "TEXT",
                "sop_start_utc": "TEXT",
                "active": "INTEGER DEFAULT 0",
                "window_hours": "INTEGER DEFAULT 12",
                "created_utc": "TEXT",
                "updated_utc": "TEXT",
            },
        )
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sop_profiles_name ON sop_profiles(name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sop_profiles_active ON sop_profiles(active)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sop_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL,
                band TEXT,
                frequency TEXT,
                software TEXT NOT NULL,
                action_key TEXT NOT NULL,
                action_label TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                interval_hours INTEGER DEFAULT 3,
                interval_minutes INTEGER,
                description TEXT,
                contact_rule TEXT DEFAULT 'none',
                contact_target TEXT,
                sort_order INTEGER DEFAULT 0
            )
            """
        )
        _ensure_columns(
            conn,
            "sop_actions",
            {
                "profile_id": "INTEGER",
                "band": "TEXT",
                "frequency": "TEXT",
                "software": "TEXT",
                "action_key": "TEXT",
                "action_label": "TEXT",
                "enabled": "INTEGER DEFAULT 1",
                "interval_hours": "INTEGER DEFAULT 3",
                "interval_minutes": "INTEGER",
                "description": "TEXT",
                "contact_rule": "TEXT DEFAULT 'none'",
                "contact_target": "TEXT",
                "sort_order": "INTEGER DEFAULT 0",
            },
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sop_actions_profile ON sop_actions(profile_id)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sop_action_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL,
                action_id INTEGER NOT NULL,
                last_completed_utc TEXT,
                updated_utc TEXT
            )
            """
        )
        _ensure_columns(
            conn,
            "sop_action_state",
            {
                "profile_id": "INTEGER",
                "action_id": "INTEGER",
                "last_completed_utc": "TEXT",
                "updated_utc": "TEXT",
            },
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_sop_state_action ON sop_action_state(profile_id, action_id)"
        )

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
