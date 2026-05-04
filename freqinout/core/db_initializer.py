"""
Ensure core SQLite tables exist before the UI starts.
This avoids runtime errors such as "no such table: js8_links".
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Set

from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.commstat_artifacts import ensure_commstat_artifact_tables
from freqinout.core.logger import log
from freqinout.core.config_paths import get_config_dir
from freqinout.core.multi_radio_store import ensure_multi_radio_settings_schema
from freqinout.core.operator_activity import ensure_js8_callsign_stats
from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.varac_ingest import ensure_varac_local_tables

# Base config directory (user-writable)
CONFIG_DIR = get_config_dir() / "config"


def _ensure_settings_db() -> None:
    """
    Ensure settings DB (freqinout.db) has the compatibility kv table and
    Wave 1 multi-rig settings schema.
    """
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    db_path = CONFIG_DIR / "freqinout.db"
    conn = connect_sqlite(db_path)
    try:
        ensure_multi_radio_settings_schema(conn)
    finally:
        conn.close()


def _ensure_operator_checkins(conn: sqlite3.Connection) -> None:
    """
    Ensure operator_checkins has the unified schema and one-time data repairs
    before any UI path touches it.
    """
    ensure_operator_checkins_schema(conn, repair_data=True)


def _ensure_local_operator_tables(conn: sqlite3.Connection) -> None:
    """
    Ensure local operator and local NCS check-in tables exist.
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS local_operator_checkins (
            callsign TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            name TEXT,
            city TEXT,
            state TEXT,
            category TEXT,
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            checkin_count INTEGER DEFAULT 0,
            notes TEXT,
            sitrep_status TEXT DEFAULT 'GREEN',
            updated_utc TEXT
        )
        """
    )
    _ensure_columns(
        conn,
        "local_operator_checkins",
        {
            "callsign": "TEXT",
            "first_name": "TEXT",
            "last_name": "TEXT",
            "name": "TEXT",
            "city": "TEXT",
            "state": "TEXT",
            "category": "TEXT",
            "first_seen_utc": "TEXT",
            "last_seen_utc": "TEXT",
            "checkin_count": "INTEGER DEFAULT 0",
            "notes": "TEXT",
            "sitrep_status": "TEXT DEFAULT 'GREEN'",
            "updated_utc": "TEXT",
        },
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ops_last_seen ON local_operator_checkins(last_seen_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ops_category ON local_operator_checkins(category)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ops_status ON local_operator_checkins(sitrep_status)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS local_ncs_checkins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checkin_utc TEXT NOT NULL,
            net_name TEXT,
            channels TEXT,
            callsign TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            name TEXT,
            city TEXT,
            state TEXT,
            category TEXT,
            sitrep_status TEXT DEFAULT 'GREEN',
            notes TEXT,
            updated_utc TEXT
        )
        """
    )
    _ensure_columns(
        conn,
        "local_ncs_checkins",
        {
            "checkin_utc": "TEXT",
            "net_name": "TEXT",
            "channels": "TEXT",
            "callsign": "TEXT",
            "first_name": "TEXT",
            "last_name": "TEXT",
            "name": "TEXT",
            "city": "TEXT",
            "state": "TEXT",
            "category": "TEXT",
            "sitrep_status": "TEXT DEFAULT 'GREEN'",
            "notes": "TEXT",
            "updated_utc": "TEXT",
        },
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ncs_checkins_ts ON local_ncs_checkins(checkin_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ncs_checkins_callsign ON local_ncs_checkins(callsign)")


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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_js8_links_ts ON js8_links(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_js8_links_origin_ts ON js8_links(origin, ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_js8_links_destination_ts ON js8_links(destination, ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_js8_links_band ON js8_links(band)")
    ensure_js8_callsign_stats(conn, rebuild_if_empty=True)


def _ensure_controlfreq_support_indexes(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    tables = {
        str(row[0])
        for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if row and row[0]
    }
    if "js8_messages" in tables:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_js8_messages_utc_ts ON js8_messages(utc_ts DESC, from_call)"
        )
    if "spotter_traffic" in tables:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_spotter_traffic_utc_ts ON spotter_traffic(utc_ts DESC, from_call, id DESC)"
        )
    if "fldigi_checkins" in tables:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_fldigi_checkins_last_seen_ts ON fldigi_checkins(last_seen_ts DESC, callsign)"
        )


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


def _ensure_sitrep_ingest_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sitrep_ingest_checkpoint (
            source_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            last_id INTEGER NOT NULL DEFAULT 0,
            updated_ts REAL NOT NULL,
            last_error TEXT
        )
        """
    )
    _ensure_columns(
        conn,
        "sitrep_ingest_checkpoint",
        {
            "source_key": "TEXT",
            "source": "TEXT",
            "source_table": "TEXT",
            "source_db_path": "TEXT",
            "last_id": "INTEGER DEFAULT 0",
            "updated_ts": "REAL",
            "last_error": "TEXT",
        },
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sitrep_source_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            source_id INTEGER NOT NULL,
            subtype TEXT NOT NULL,
            from_call TEXT,
            target TEXT,
            report_group TEXT,
            grid TEXT,
            scope TEXT,
            transport_mode TEXT,
            remarks_text TEXT,
            brevity_code TEXT,
            brevity_summary TEXT,
            state_code TEXT,
            state_confidence TEXT,
            geo_confidence TEXT,
            status_payload TEXT,
            raw_payload TEXT,
            event_ts REAL,
            event_ts_utc TEXT,
            ingested_ts REAL NOT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "sitrep_source_events",
        {
            "source": "TEXT",
            "source_table": "TEXT",
            "source_db_path": "TEXT",
            "source_id": "INTEGER",
            "subtype": "TEXT",
            "from_call": "TEXT",
            "target": "TEXT",
            "report_group": "TEXT",
            "grid": "TEXT",
            "scope": "TEXT",
            "transport_mode": "TEXT",
            "remarks_text": "TEXT",
            "brevity_code": "TEXT",
            "brevity_summary": "TEXT",
            "state_code": "TEXT",
            "state_confidence": "TEXT",
            "geo_confidence": "TEXT",
            "status_payload": "TEXT",
            "raw_payload": "TEXT",
            "event_ts": "REAL",
            "event_ts_utc": "TEXT",
            "ingested_ts": "REAL",
        },
    )
    # Safety: remove duplicates before enforcing uniqueness in upgraded DBs.
    cur.execute(
        """
        DELETE FROM sitrep_source_events
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM sitrep_source_events
            GROUP BY source, source_table, source_db_path, source_id
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sitrep_source_events_source_id
            ON sitrep_source_events(source, source_table, source_db_path, source_id)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_source_events_recent
            ON sitrep_source_events(source, event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_source_events_call_recent
            ON sitrep_source_events(from_call, event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_checkpoint_updated
            ON sitrep_ingest_checkpoint(updated_ts)
        """
    )
    ensure_commstat_artifact_tables(conn)


def _ensure_sitrep_fusion_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sitrep_fusion_checkpoint (
            pipeline_key TEXT PRIMARY KEY,
            last_source_event_id INTEGER NOT NULL DEFAULT 0,
            updated_ts REAL NOT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "sitrep_fusion_checkpoint",
        {
            "pipeline_key": "TEXT",
            "last_source_event_id": "INTEGER DEFAULT 0",
            "updated_ts": "REAL",
        },
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sitrep_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_key TEXT NOT NULL UNIQUE,
            event_ts REAL,
            event_ts_utc TEXT,
            from_call TEXT NOT NULL,
            target TEXT,
            report_group TEXT,
            grid TEXT,
            state_code TEXT,
            state_confidence TEXT,
            geo_confidence TEXT,
            scope TEXT,
            overall_status TEXT NOT NULL DEFAULT 'not_reported',
            power TEXT NOT NULL DEFAULT 'not_reported',
            water TEXT NOT NULL DEFAULT 'not_reported',
            medical TEXT NOT NULL DEFAULT 'not_reported',
            communications TEXT NOT NULL DEFAULT 'not_reported',
            internet TEXT NOT NULL DEFAULT 'not_reported',
            travel TEXT NOT NULL DEFAULT 'not_reported',
            food TEXT NOT NULL DEFAULT 'not_reported',
            fuel TEXT NOT NULL DEFAULT 'not_reported',
            crime TEXT NOT NULL DEFAULT 'not_reported',
            civil_unrest TEXT NOT NULL DEFAULT 'not_reported',
            political TEXT NOT NULL DEFAULT 'not_reported',
            subtype TEXT,
            transport_mode TEXT,
            remarks_text TEXT,
            brevity_code TEXT,
            brevity_summary TEXT,
            source_first TEXT,
            source_last TEXT,
            sources_json TEXT,
            source_count INTEGER DEFAULT 1,
            source_refs_json TEXT,
            raw_payload_json TEXT,
            inserted_ts REAL NOT NULL,
            updated_ts REAL NOT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "sitrep_events",
        {
            "report_key": "TEXT",
            "event_ts": "REAL",
            "event_ts_utc": "TEXT",
            "from_call": "TEXT",
            "target": "TEXT",
            "report_group": "TEXT",
            "grid": "TEXT",
            "state_code": "TEXT",
            "state_confidence": "TEXT",
            "geo_confidence": "TEXT",
            "scope": "TEXT",
            "overall_status": "TEXT DEFAULT 'not_reported'",
            "power": "TEXT DEFAULT 'not_reported'",
            "water": "TEXT DEFAULT 'not_reported'",
            "medical": "TEXT DEFAULT 'not_reported'",
            "communications": "TEXT DEFAULT 'not_reported'",
            "internet": "TEXT DEFAULT 'not_reported'",
            "travel": "TEXT DEFAULT 'not_reported'",
            "food": "TEXT DEFAULT 'not_reported'",
            "fuel": "TEXT DEFAULT 'not_reported'",
            "crime": "TEXT DEFAULT 'not_reported'",
            "civil_unrest": "TEXT DEFAULT 'not_reported'",
            "political": "TEXT DEFAULT 'not_reported'",
            "subtype": "TEXT",
            "transport_mode": "TEXT",
            "remarks_text": "TEXT",
            "brevity_code": "TEXT",
            "brevity_summary": "TEXT",
            "source_first": "TEXT",
            "source_last": "TEXT",
            "sources_json": "TEXT",
            "source_count": "INTEGER DEFAULT 1",
            "source_refs_json": "TEXT",
            "raw_payload_json": "TEXT",
            "inserted_ts": "REAL",
            "updated_ts": "REAL",
        },
    )
    # Safety: collapse duplicate report keys before enforcing uniqueness.
    cur.execute(
        """
        DELETE FROM sitrep_events
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM sitrep_events
            GROUP BY report_key
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sitrep_events_report_key
            ON sitrep_events(report_key)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_events_recent
            ON sitrep_events(event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_events_call_recent
            ON sitrep_events(from_call, event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_events_source_last
            ON sitrep_events(source_last, event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sitrep_latest_by_callsign (
            callsign TEXT PRIMARY KEY,
            latest_event_id INTEGER NOT NULL,
            latest_event_ts REAL,
            latest_event_ts_utc TEXT,
            latest_subtype TEXT,
            latest_target TEXT,
            latest_report_group TEXT,
            latest_grid TEXT,
            latest_state_code TEXT,
            latest_state_confidence TEXT,
            latest_geo_confidence TEXT,
            latest_transport_mode TEXT,
            latest_remarks_text TEXT,
            latest_brevity_code TEXT,
            latest_brevity_summary TEXT,
            effective_status TEXT NOT NULL DEFAULT 'not_reported',
            scope TEXT,
            overall_status TEXT NOT NULL DEFAULT 'not_reported',
            power TEXT NOT NULL DEFAULT 'not_reported',
            water TEXT NOT NULL DEFAULT 'not_reported',
            medical TEXT NOT NULL DEFAULT 'not_reported',
            communications TEXT NOT NULL DEFAULT 'not_reported',
            internet TEXT NOT NULL DEFAULT 'not_reported',
            travel TEXT NOT NULL DEFAULT 'not_reported',
            food TEXT NOT NULL DEFAULT 'not_reported',
            fuel TEXT NOT NULL DEFAULT 'not_reported',
            crime TEXT NOT NULL DEFAULT 'not_reported',
            civil_unrest TEXT NOT NULL DEFAULT 'not_reported',
            political TEXT NOT NULL DEFAULT 'not_reported',
            source_summary_json TEXT,
            updated_ts REAL NOT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "sitrep_latest_by_callsign",
        {
            "callsign": "TEXT",
            "latest_event_id": "INTEGER",
            "latest_event_ts": "REAL",
            "latest_event_ts_utc": "TEXT",
            "latest_subtype": "TEXT",
            "latest_target": "TEXT",
            "latest_report_group": "TEXT",
            "latest_grid": "TEXT",
            "latest_state_code": "TEXT",
            "latest_state_confidence": "TEXT",
            "latest_geo_confidence": "TEXT",
            "latest_transport_mode": "TEXT",
            "latest_remarks_text": "TEXT",
            "latest_brevity_code": "TEXT",
            "latest_brevity_summary": "TEXT",
            "effective_status": "TEXT DEFAULT 'not_reported'",
            "scope": "TEXT",
            "overall_status": "TEXT DEFAULT 'not_reported'",
            "power": "TEXT DEFAULT 'not_reported'",
            "water": "TEXT DEFAULT 'not_reported'",
            "medical": "TEXT DEFAULT 'not_reported'",
            "communications": "TEXT DEFAULT 'not_reported'",
            "internet": "TEXT DEFAULT 'not_reported'",
            "travel": "TEXT DEFAULT 'not_reported'",
            "food": "TEXT DEFAULT 'not_reported'",
            "fuel": "TEXT DEFAULT 'not_reported'",
            "crime": "TEXT DEFAULT 'not_reported'",
            "civil_unrest": "TEXT DEFAULT 'not_reported'",
            "political": "TEXT DEFAULT 'not_reported'",
            "source_summary_json": "TEXT",
            "updated_ts": "REAL",
        },
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_latest_effective
            ON sitrep_latest_by_callsign(effective_status, latest_event_ts DESC)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sitrep_state_rollup (
            report_group TEXT NOT NULL,
            state_code TEXT NOT NULL,
            callsign_count INTEGER NOT NULL DEFAULT 0,
            red_count INTEGER NOT NULL DEFAULT 0,
            yellow_count INTEGER NOT NULL DEFAULT 0,
            green_count INTEGER NOT NULL DEFAULT 0,
            unknown_count INTEGER NOT NULL DEFAULT 0,
            js8_count INTEGER NOT NULL DEFAULT 0,
            internet_count INTEGER NOT NULL DEFAULT 0,
            mixed_transport_count INTEGER NOT NULL DEFAULT 0,
            latest_event_ts REAL,
            updated_ts REAL NOT NULL,
            PRIMARY KEY (report_group, state_code)
        )
        """
    )
    _ensure_columns(
        conn,
        "sitrep_state_rollup",
        {
            "report_group": "TEXT",
            "state_code": "TEXT",
            "callsign_count": "INTEGER DEFAULT 0",
            "red_count": "INTEGER DEFAULT 0",
            "yellow_count": "INTEGER DEFAULT 0",
            "green_count": "INTEGER DEFAULT 0",
            "unknown_count": "INTEGER DEFAULT 0",
            "js8_count": "INTEGER DEFAULT 0",
            "internet_count": "INTEGER DEFAULT 0",
            "mixed_transport_count": "INTEGER DEFAULT 0",
            "latest_event_ts": "REAL",
            "updated_ts": "REAL",
        },
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_state_rollup_group
            ON sitrep_state_rollup(report_group, latest_event_ts DESC, state_code)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_fusion_checkpoint_updated
            ON sitrep_fusion_checkpoint(updated_ts)
        """
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
    conn = connect_sqlite(db_path)
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
                auto_tune INTEGER DEFAULT 0,
                target_scope TEXT NOT NULL DEFAULT 'station',
                target_device_profile_id INTEGER,
                target_operating_profile_id INTEGER
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
                "target_scope": "TEXT NOT NULL DEFAULT 'station'",
                "target_device_profile_id": "INTEGER",
                "target_operating_profile_id": "INTEGER",
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
                net_name TEXT,
                fldigi_mode TEXT,
                fldigi_offset TEXT,
                resource_id INTEGER,
                target_scope TEXT NOT NULL DEFAULT 'station',
                target_device_profile_id INTEGER,
                target_operating_profile_id INTEGER
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
                "fldigi_mode": "TEXT",
                "fldigi_offset": "TEXT",
                "resource_id": "INTEGER",
                "target_scope": "TEXT NOT NULL DEFAULT 'station'",
                "target_device_profile_id": "INTEGER",
                "target_operating_profile_id": "INTEGER",
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
                net_name TEXT,
                fldigi_mode TEXT,
                fldigi_offset TEXT,
                target_scope TEXT NOT NULL DEFAULT 'station',
                target_device_profile_id INTEGER,
                target_operating_profile_id INTEGER
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
                "fldigi_mode": "TEXT",
                "fldigi_offset": "TEXT",
                "target_scope": "TEXT NOT NULL DEFAULT 'station'",
                "target_device_profile_id": "INTEGER",
                "target_operating_profile_id": "INTEGER",
            },
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS net_resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_set TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_ref TEXT,
                readonly INTEGER DEFAULT 1,
                day_utc TEXT NOT NULL,
                recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0,
                month_weeks TEXT,
                group_name TEXT,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                early_checkin INTEGER NOT NULL,
                primary_js8call_group TEXT,
                coverage TEXT,
                comment TEXT,
                net_name TEXT,
                fldigi_mode TEXT,
                fldigi_offset TEXT,
                updated_utc TEXT
            )
            """
        )
        _ensure_columns(
            conn,
            "net_resources",
            {
                "resource_set": "TEXT",
                "source_type": "TEXT",
                "source_ref": "TEXT",
                "readonly": "INTEGER DEFAULT 1",
                "day_utc": "TEXT",
                "recurrence": "TEXT",
                "biweekly_offset_weeks": "INTEGER DEFAULT 0",
                "month_weeks": "TEXT",
                "group_name": "TEXT",
                "band": "TEXT",
                "mode": "TEXT",
                "frequency": "TEXT",
                "start_utc": "TEXT",
                "end_utc": "TEXT",
                "early_checkin": "INTEGER DEFAULT 0",
                "primary_js8call_group": "TEXT",
                "coverage": "TEXT",
                "comment": "TEXT",
                "net_name": "TEXT",
                "fldigi_mode": "TEXT",
                "fldigi_offset": "TEXT",
                "updated_utc": "TEXT",
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
                imported_at TEXT,
                source_type TEXT DEFAULT 'IMPORTED',
                updated_at TEXT
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
                "source_type": "TEXT DEFAULT 'IMPORTED'",
                "updated_at": "TEXT",
            },
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_peer_hf_owner ON peer_hf_schedule(owner_callsign)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS peer_hf_schedule_inferred (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_callsign TEXT NOT NULL,
                day_utc TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                frequency TEXT NOT NULL,
                source TEXT,
                confidence REAL DEFAULT 0,
                sample_count INTEGER DEFAULT 0,
                weeks_seen INTEGER DEFAULT 0,
                weeks_observed INTEGER DEFAULT 0,
                last_seen_ts REAL,
                updated_ts REAL
            )
            """
        )
        _ensure_columns(
            conn,
            "peer_hf_schedule_inferred",
            {
                "owner_callsign": "TEXT",
                "day_utc": "TEXT",
                "start_utc": "TEXT",
                "end_utc": "TEXT",
                "band": "TEXT",
                "mode": "TEXT",
                "frequency": "TEXT",
                "source": "TEXT",
                "confidence": "REAL DEFAULT 0",
                "sample_count": "INTEGER DEFAULT 0",
                "weeks_seen": "INTEGER DEFAULT 0",
                "weeks_observed": "INTEGER DEFAULT 0",
                "last_seen_ts": "REAL",
                "updated_ts": "REAL",
            },
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_peer_hf_inferred_owner ON peer_hf_schedule_inferred(owner_callsign)"
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_peer_hf_inferred_owner_day
            ON peer_hf_schedule_inferred(owner_callsign, day_utc, start_utc, end_utc)
            """
        )
        # Imported schedules are authoritative; inferred rows are a fallback per callsign.
        cur.execute("DROP VIEW IF EXISTS peer_hf_schedule_effective")
        cur.execute(
            """
            CREATE VIEW IF NOT EXISTS peer_hf_schedule_effective AS
            SELECT
                owner_callsign,
                day_utc,
                start_utc,
                end_utc,
                band,
                mode,
                frequency,
                COALESCE(NULLIF(TRIM(source_type), ''), 'IMPORTED') AS source_type,
                NULL AS confidence,
                NULL AS sample_count,
                NULL AS weeks_seen,
                NULL AS weeks_observed,
                NULL AS last_seen_ts
            FROM peer_hf_schedule
            UNION ALL
            SELECT
                i.owner_callsign,
                i.day_utc,
                i.start_utc,
                i.end_utc,
                i.band,
                i.mode,
                i.frequency,
                'INFERRED' AS source_type,
                i.confidence,
                i.sample_count,
                i.weeks_seen,
                i.weeks_observed,
                i.last_seen_ts
            FROM peer_hf_schedule_inferred i
            WHERE NOT EXISTS (
                SELECT 1
                FROM peer_hf_schedule e
                WHERE UPPER(TRIM(e.owner_callsign)) = UPPER(TRIM(i.owner_callsign))
            )
            """
        )

        # Propagation outcomes (offline scoring support)
        _ensure_propagation_outcome_tables(conn)
        _ensure_sitrep_ingest_tables(conn)
        _ensure_sitrep_fusion_tables(conn)

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
                priority INTEGER DEFAULT 100,
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
                "priority": "INTEGER DEFAULT 100",
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

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sop_schedule_layer (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL,
                day_utc TEXT NOT NULL,
                recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0,
                month_weeks TEXT,
                group_name TEXT,
                band TEXT,
                mode TEXT,
                vfo TEXT,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                sort_order INTEGER DEFAULT 0,
                updated_utc TEXT
            )
            """
        )
        _ensure_columns(
            conn,
            "sop_schedule_layer",
            {
                "profile_id": "INTEGER",
                "day_utc": "TEXT",
                "recurrence": "TEXT DEFAULT 'Weekly'",
                "biweekly_offset_weeks": "INTEGER DEFAULT 0",
                "month_weeks": "TEXT",
                "group_name": "TEXT",
                "band": "TEXT",
                "mode": "TEXT",
                "vfo": "TEXT",
                "frequency": "TEXT",
                "start_utc": "TEXT",
                "end_utc": "TEXT",
                "enabled": "INTEGER DEFAULT 1",
                "sort_order": "INTEGER DEFAULT 0",
                "updated_utc": "TEXT",
            },
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sop_layer_profile ON sop_schedule_layer(profile_id)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_sop_layer_profile_day ON sop_schedule_layer(profile_id, day_utc, start_utc)"
        )

        _ensure_operator_checkins(conn)
        _ensure_local_operator_tables(conn)
        _ensure_js8_links(conn)
        ensure_varac_local_tables(conn)
        _ensure_controlfreq_support_indexes(conn)

        conn.commit()
    finally:
        conn.close()


def ensure_all_tables() -> None:
    """
    Public entry point to ensure both DBs are initialized.
    """
    ensure_settings_tables()
    ensure_nets_tables()
    log.info("DB init: ensured core tables (settings and nets).")


def ensure_settings_tables() -> None:
    """
    Public entry point to ensure settings DB tables and migrations are applied.
    """
    _ensure_settings_db()
    log.info("DB init: ensured settings tables.")


def ensure_nets_tables() -> None:
    """
    Public entry point to ensure nets DB tables and migrations are applied.
    """
    _ensure_nets_db()
    log.info("DB init: ensured nets tables.")
