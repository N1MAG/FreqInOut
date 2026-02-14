from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
SETTINGS_DB = CONFIG_DIR / "freqinout.db"
NETS_DB = CONFIG_DIR / "freqinout_nets.db"


@dataclass(frozen=True)
class TableDef:
    name: str
    db: Path
    description: str
    ddl: str = ""


SETTINGS_TABLES: Dict[str, TableDef] = {
    "kv": TableDef(
        name="kv",
        db=SETTINGS_DB,
        description="Key/value settings store (JSON encoded values).",
        ddl="""
        CREATE TABLE IF NOT EXISTS kv (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """,
    ),
    "daily_schedule_tab": TableDef(
        name="daily_schedule_tab",
        db=NETS_DB,
        description="HF schedule rows mirrored from config (day, band/mode/VFO, frequency, times, auto_tune).",
        ddl="""
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
        """,
    ),
}

NETS_TABLES: Dict[str, TableDef] = {
    "operator_checkins": TableDef(
        name="operator_checkins",
        db=NETS_DB,
        description="Operator roster with check-in counts and group metadata.",
        ddl="""
        CREATE TABLE IF NOT EXISTS operator_checkins (
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
        """,
    ),
    "net_schedule_tab": TableDef(
        name="net_schedule_tab",
        db=NETS_DB,
        description="Primary net schedule table (day, recurrence, band/mode/VFO, frequency, times, metadata).",
        ddl="""
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
        """,
    ),
    "net_schedule": TableDef(
        name="net_schedule",
        db=NETS_DB,
        description="Legacy net schedule mirror (no VFO column; kept for backward compatibility).",
        ddl="""
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
        """,
    ),
    "message_viewer_paths": TableDef(
        name="message_viewer_paths",
        db=NETS_DB,
        description="Directories watched by the Message Viewer tab (origin + path).",
        ddl="""
        CREATE TABLE IF NOT EXISTS message_viewer_paths (
            origin TEXT,
            path TEXT UNIQUE
        )
        """,
    ),
    "js8_links": TableDef(
        name="js8_links",
        db=NETS_DB,
        description="JS8 link/spot records (times, peers, SNR, band, frequency, relay info).",
        ddl="""
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
        """,
    ),
    "autoquery_backlog": TableDef(
        name="autoquery_backlog",
        db=NETS_DB,
        description="Backlog queue for JS8 auto-query processing.",
        ddl="",
    ),
    "peer_hf_schedule": TableDef(
        name="peer_hf_schedule",
        db=NETS_DB,
        description="Peer shared HF schedule rows.",
        ddl="",
    ),
    "prop_contact_events": TableDef(
        name="prop_contact_events",
        db=NETS_DB,
        description="Raw historical propagation contact outcomes used for offline forecasting.",
        ddl="""
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
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_prop_contact_events_event_key
            ON prop_contact_events(event_key);
        CREATE INDEX IF NOT EXISTS idx_prop_contact_events_lookup
            ON prop_contact_events(origin_grid6, target_type, target_id, band, ts_utc);
        CREATE INDEX IF NOT EXISTS idx_prop_contact_events_source
            ON prop_contact_events(source, ts_utc);
        CREATE INDEX IF NOT EXISTS idx_prop_contact_events_inserted
            ON prop_contact_events(inserted_utc);
        """,
    ),
    "prop_ingest_checkpoint": TableDef(
        name="prop_ingest_checkpoint",
        db=NETS_DB,
        description="Ingestion progress checkpoints for incremental propagation event backfill.",
        ddl="""
        CREATE TABLE IF NOT EXISTS prop_ingest_checkpoint (
            source TEXT PRIMARY KEY,
            last_ts_utc TEXT,
            last_source_ref TEXT,
            updated_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_prop_ingest_checkpoint_updated
            ON prop_ingest_checkpoint(updated_utc);
        """,
    ),
    "prop_outcome_stats": TableDef(
        name="prop_outcome_stats",
        db=NETS_DB,
        description="Aggregated propagation success stats by origin/target/time/band buckets.",
        ddl="""
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
        );
        CREATE INDEX IF NOT EXISTS idx_prop_outcome_stats_lookup
            ON prop_outcome_stats(origin_grid6, target_type, target_id, band, month, utc_hour_bucket);
        CREATE INDEX IF NOT EXISTS idx_prop_outcome_stats_updated
            ON prop_outcome_stats(updated_utc);
        """,
    ),
}

ALL_TABLES: Dict[str, TableDef] = {**SETTINGS_TABLES, **NETS_TABLES}

GROUPS: Dict[str, List[str]] = {
    "settings": list(SETTINGS_TABLES.keys()),
    "nets": list(NETS_TABLES.keys()),
    "all": list(ALL_TABLES.keys()),
    "settings_all": list(SETTINGS_TABLES.keys()),
    "nets_all": list(NETS_TABLES.keys()),
}


def db_for_table(table: str) -> Optional[Path]:
    table_def = ALL_TABLES.get(table)
    if not table_def:
        return None
    return table_def.db
