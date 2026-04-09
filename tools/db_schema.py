from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
APP_NAME = "FreqInOut"


def _runtime_app_root() -> Path:
    env_cfg = os.environ.get("FREQINOUT_CONFIG_DIR")
    if env_cfg:
        return Path(env_cfg)
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA") or Path.home()) / APP_NAME
    else:
        base = Path.home() / f".{APP_NAME.lower()}"
    try:
        base.mkdir(parents=True, exist_ok=True)
        return base
    except Exception:
        fallback = ROOT / "freqinout_config"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


CONFIG_DIR = _runtime_app_root() / "config"
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
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            last_net TEXT,
            last_role TEXT,
            checkin_count INTEGER DEFAULT 0,
            groups_json TEXT,
            trusted INTEGER DEFAULT 0
        )
        """,
    ),
    "local_operator_checkins": TableDef(
        name="local_operator_checkins",
        db=NETS_DB,
        description="Local operator roster for VHF/UHF/GMRS/MURS/FRS workflows.",
        ddl="""
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
        """,
    ),
    "local_ncs_checkins": TableDef(
        name="local_ncs_checkins",
        db=NETS_DB,
        description="Local NCS check-in log (single table) with SitRep and notes snapshots.",
        ddl="""
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
            group_name TEXT,
            fldigi_mode TEXT,
            fldigi_offset TEXT,
            resource_id INTEGER
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
            group_name TEXT,
            fldigi_mode TEXT,
            fldigi_offset TEXT
        )
        """,
    ),
    "net_resources": TableDef(
        name="net_resources",
        db=NETS_DB,
        description="Read-only/shareable net resource catalog used to populate active net schedules.",
        ddl="""
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
            is_spotter INTEGER DEFAULT 0,
            last_seen_utc TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_js8_links_ts
            ON js8_links(ts);
        CREATE INDEX IF NOT EXISTS idx_js8_links_origin_ts
            ON js8_links(origin, ts);
        CREATE INDEX IF NOT EXISTS idx_js8_links_destination_ts
            ON js8_links(destination, ts);
        CREATE INDEX IF NOT EXISTS idx_js8_links_band
            ON js8_links(band);
        """,
    ),
    "js8_callsign_stats": TableDef(
        name="js8_callsign_stats",
        db=NETS_DB,
        description="Compact per-callsign JS8 activity summary used for responsive map/history last-seen lookups.",
        ddl="""
        CREATE TABLE IF NOT EXISTS js8_callsign_stats (
            callsign TEXT PRIMARY KEY,
            last_seen_ts REAL,
            last_band TEXT,
            last_freq_hz REAL
        );
        CREATE INDEX IF NOT EXISTS idx_js8_callsign_stats_last_seen
            ON js8_callsign_stats(last_seen_ts);
        """,
    ),
    "varac_ingest_state": TableDef(
        name="varac_ingest_state",
        db=NETS_DB,
        description="VarAC incremental ingest checkpoints by source table.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_ingest_state (
            table_name TEXT PRIMARY KEY,
            last_id INTEGER DEFAULT 0
        )
        """,
    ),
    "varac_sync_status": TableDef(
        name="varac_sync_status",
        db=NETS_DB,
        description="VarAC ingest run-level health (timing, success/failure, row counts).",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_sync_status (
            run_started_ts REAL PRIMARY KEY,
            run_finished_ts REAL,
            varac_db_path TEXT,
            success INTEGER DEFAULT 0,
            rows_scanned INTEGER DEFAULT 0,
            rows_written INTEGER DEFAULT 0,
            error_text TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_varac_sync_status_finished
            ON varac_sync_status(run_finished_ts);
        """,
    ),
    "varac_sync_table_counts": TableDef(
        name="varac_sync_table_counts",
        db=NETS_DB,
        description="Per-table row counters and watermark snapshots for each VarAC ingest run.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_sync_table_counts (
            run_started_ts REAL,
            table_name TEXT,
            rows_scanned INTEGER DEFAULT 0,
            rows_written INTEGER DEFAULT 0,
            watermark_id INTEGER DEFAULT 0,
            PRIMARY KEY (run_started_ts, table_name)
        );
        CREATE INDEX IF NOT EXISTS idx_varac_sync_counts_table
            ON varac_sync_table_counts(table_name, run_started_ts);
        """,
    ),
    "varac_messages": TableDef(
        name="varac_messages",
        db=NETS_DB,
        description="Unified VarAC message mirror (qso/vmail/broadcast) for UI read paths.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_messages (
            id INTEGER,
            guid TEXT,
            source TEXT,
            msg_type TEXT,
            from_call TEXT,
            to_call TEXT,
            subject TEXT,
            body TEXT,
            ts REAL,
            band TEXT,
            freq_hz REAL,
            snr REAL,
            read_status INTEGER,
            folder TEXT,
            file_path TEXT,
            vmail_guid TEXT,
            is_deleted INTEGER DEFAULT 0,
            flag_state INTEGER DEFAULT 0,
            folder_label TEXT,
            urgent INTEGER DEFAULT 0,
            has_attachment INTEGER DEFAULT 0,
            via_callsign TEXT,
            PRIMARY KEY (source, id)
        );
        CREATE INDEX IF NOT EXISTS idx_varac_messages_ts
            ON varac_messages(ts);
        CREATE INDEX IF NOT EXISTS idx_varac_messages_source_ts
            ON varac_messages(source, ts);
        CREATE INDEX IF NOT EXISTS idx_varac_messages_folder_label
            ON varac_messages(folder_label);
        """,
    ),
    "varac_callsign_stats": TableDef(
        name="varac_callsign_stats",
        db=NETS_DB,
        description="Latest-seen callsign snapshot from VarAC streams.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_callsign_stats (
            callsign TEXT PRIMARY KEY,
            last_seen_ts REAL,
            last_band TEXT,
            last_freq_hz REAL,
            last_snr REAL,
            last_source TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_varac_callsign_stats_last_seen
            ON varac_callsign_stats(last_seen_ts);
        """,
    ),
    "varac_callsign_traits": TableDef(
        name="varac_callsign_traits",
        db=NETS_DB,
        description="Derived callsign traits (emcomm, bbs activity, alert hints).",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_callsign_traits (
            callsign TEXT PRIMARY KEY,
            is_emcomm INTEGER DEFAULT 0,
            bbs_seen INTEGER DEFAULT 0,
            alert_count INTEGER DEFAULT 0,
            last_alert_ts REAL,
            last_updated_ts REAL
        );
        CREATE INDEX IF NOT EXISTS idx_varac_callsign_traits_updated
            ON varac_callsign_traits(last_updated_ts);
        """,
    ),
    "varac_links": TableDef(
        name="varac_links",
        db=NETS_DB,
        description="VarAC peer links extracted from QSO traffic.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_links (
            ts REAL,
            origin TEXT,
            destination TEXT,
            snr REAL,
            band TEXT,
            freq_hz REAL,
            source TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_varac_links_ts
            ON varac_links(ts);
        """,
    ),
    "varac_vmail_folders": TableDef(
        name="varac_vmail_folders",
        db=NETS_DB,
        description="VarAC vmail folder lookup mirror (Inbox/Sent/Outbox/Parking).",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_vmail_folders (
            folder_id INTEGER PRIMARY KEY,
            folder TEXT
        )
        """,
    ),
    "varac_relay_notifications": TableDef(
        name="varac_relay_notifications",
        db=NETS_DB,
        description="VarAC relay-notification stream for relay inbox workflows.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_relay_notifications (
            id INTEGER PRIMARY KEY,
            guid TEXT,
            relay_ts REAL,
            from_call TEXT,
            freq_hz REAL,
            urgent INTEGER DEFAULT 0,
            is_deleted INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_varac_relay_ts
            ON varac_relay_notifications(relay_ts);
        """,
    ),
    "varac_broadcast_events": TableDef(
        name="varac_broadcast_events",
        db=NETS_DB,
        description="VarAC broadcast mirror including via_callsign and recency fields.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_broadcast_events (
            id INTEGER PRIMARY KEY,
            guid TEXT,
            ts REAL,
            freq_hz REAL,
            band TEXT,
            from_call TEXT,
            to_call TEXT,
            via_callsign TEXT,
            message TEXT,
            snr REAL,
            instance_id INTEGER,
            is_deleted INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_varac_broadcast_ts
            ON varac_broadcast_events(ts);
        CREATE INDEX IF NOT EXISTS idx_varac_broadcast_band_ts
            ON varac_broadcast_events(band, ts);
        CREATE INDEX IF NOT EXISTS idx_varac_broadcast_from_ts
            ON varac_broadcast_events(from_call, ts);
        """,
    ),
    "varac_cqframe_type_lut": TableDef(
        name="varac_cqframe_type_lut",
        db=NETS_DB,
        description="VarAC cqframe type lookup mirror.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_cqframe_type_lut (
            cqframe_type_id INTEGER PRIMARY KEY,
            cqframe_type TEXT
        )
        """,
    ),
    "varac_cqframe_events": TableDef(
        name="varac_cqframe_events",
        db=NETS_DB,
        description="VarAC CQ/Beacon mirror used for map density and recency heat.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_cqframe_events (
            id INTEGER PRIMARY KEY,
            guid TEXT,
            ts REAL,
            cqframe_type_id INTEGER,
            cqframe_type TEXT,
            freq_hz REAL,
            band TEXT,
            bandwidth TEXT,
            from_call TEXT,
            snr REAL,
            slot INTEGER,
            data TEXT,
            locator TEXT,
            is_emcomm INTEGER DEFAULT 0,
            instance_id INTEGER,
            is_deleted INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_varac_cqframe_ts
            ON varac_cqframe_events(ts);
        CREATE INDEX IF NOT EXISTS idx_varac_cqframe_band_ts
            ON varac_cqframe_events(band, ts);
        CREATE INDEX IF NOT EXISTS idx_varac_cqframe_from_ts
            ON varac_cqframe_events(from_call, ts);
        """,
    ),
    "varac_qso_snr_reports": TableDef(
        name="varac_qso_snr_reports",
        db=NETS_DB,
        description="VarAC qso_snr_report mirror for propagation confidence trends.",
        ddl="""
        CREATE TABLE IF NOT EXISTS varac_qso_snr_reports (
            id INTEGER PRIMARY KEY,
            guid TEXT,
            qso_guid TEXT,
            snr_direction TEXT,
            snr REAL,
            ts REAL
        );
        CREATE INDEX IF NOT EXISTS idx_varac_qso_snr_ts
            ON varac_qso_snr_reports(ts);
        CREATE INDEX IF NOT EXISTS idx_varac_qso_snr_qso
            ON varac_qso_snr_reports(qso_guid, ts);
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
    "peer_hf_schedule_inferred": TableDef(
        name="peer_hf_schedule_inferred",
        db=NETS_DB,
        description="Inferred peer HF schedule rows derived from recurring observed traffic.",
        ddl="""
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
        );
        CREATE INDEX IF NOT EXISTS idx_peer_hf_inferred_owner
            ON peer_hf_schedule_inferred(owner_callsign);
        CREATE INDEX IF NOT EXISTS idx_peer_hf_inferred_owner_day
            ON peer_hf_schedule_inferred(owner_callsign, day_utc, start_utc, end_utc);
        """,
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
    "sitrep_ingest_checkpoint": TableDef(
        name="sitrep_ingest_checkpoint",
        db=NETS_DB,
        description="Incremental SitRep adapter checkpoints by source/table.",
        ddl="""
        CREATE TABLE IF NOT EXISTS sitrep_ingest_checkpoint (
            source_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            last_id INTEGER NOT NULL DEFAULT 0,
            updated_ts REAL NOT NULL,
            last_error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sitrep_checkpoint_updated
            ON sitrep_ingest_checkpoint(updated_ts);
        """,
    ),
    "sitrep_source_events": TableDef(
        name="sitrep_source_events",
        db=NETS_DB,
        description="Raw SitRep-capable source event staging mirror (Phase 1).",
        ddl="""
        CREATE TABLE IF NOT EXISTS sitrep_source_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            source_id INTEGER NOT NULL,
            subtype TEXT NOT NULL,
            from_call TEXT,
            target TEXT,
            grid TEXT,
            scope TEXT,
            status_payload TEXT,
            raw_payload TEXT,
            event_ts REAL,
            event_ts_utc TEXT,
            ingested_ts REAL NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sitrep_source_events_source_id
            ON sitrep_source_events(source, source_table, source_db_path, source_id);
        CREATE INDEX IF NOT EXISTS idx_sitrep_source_events_recent
            ON sitrep_source_events(source, event_ts DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_sitrep_source_events_call_recent
            ON sitrep_source_events(from_call, event_ts DESC, id DESC);
        """,
    ),
    "sitrep_fusion_checkpoint": TableDef(
        name="sitrep_fusion_checkpoint",
        db=NETS_DB,
        description="Checkpoint for incremental SitRep fusion from staged source events.",
        ddl="""
        CREATE TABLE IF NOT EXISTS sitrep_fusion_checkpoint (
            pipeline_key TEXT PRIMARY KEY,
            last_source_event_id INTEGER NOT NULL DEFAULT 0,
            updated_ts REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sitrep_fusion_checkpoint_updated
            ON sitrep_fusion_checkpoint(updated_ts);
        """,
    ),
    "sitrep_events": TableDef(
        name="sitrep_events",
        db=NETS_DB,
        description="Canonical fused SitRep events collapsed across CommStat/Spotter sources.",
        ddl="""
        CREATE TABLE IF NOT EXISTS sitrep_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_key TEXT NOT NULL UNIQUE,
            event_ts REAL,
            event_ts_utc TEXT,
            from_call TEXT NOT NULL,
            target TEXT,
            grid TEXT,
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
            source_first TEXT,
            source_last TEXT,
            sources_json TEXT,
            source_count INTEGER DEFAULT 1,
            source_refs_json TEXT,
            raw_payload_json TEXT,
            inserted_ts REAL NOT NULL,
            updated_ts REAL NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sitrep_events_report_key
            ON sitrep_events(report_key);
        CREATE INDEX IF NOT EXISTS idx_sitrep_events_recent
            ON sitrep_events(event_ts DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_sitrep_events_call_recent
            ON sitrep_events(from_call, event_ts DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_sitrep_events_source_last
            ON sitrep_events(source_last, event_ts DESC, id DESC);
        """,
    ),
    "sitrep_latest_by_callsign": TableDef(
        name="sitrep_latest_by_callsign",
        db=NETS_DB,
        description="Per-callsign latest effective SitRep projection for fast Operators/Map reads.",
        ddl="""
        CREATE TABLE IF NOT EXISTS sitrep_latest_by_callsign (
            callsign TEXT PRIMARY KEY,
            latest_event_id INTEGER NOT NULL,
            latest_event_ts REAL,
            latest_event_ts_utc TEXT,
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
        );
        CREATE INDEX IF NOT EXISTS idx_sitrep_latest_effective
            ON sitrep_latest_by_callsign(effective_status, latest_event_ts DESC);
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
