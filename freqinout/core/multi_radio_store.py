from __future__ import annotations

import datetime
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


DEFAULT_DEVICE_SYSTEM_KEY = "default_device"
DEFAULT_DEVICE_NAME = "Default Radio"
DEFAULT_OPERATING_SYSTEM_KEY = "default_operating"
DEFAULT_OPERATING_NAME = "Default Operating Profile"
DEFAULT_JS8_INSTANCE_SYSTEM_KEY = "default_js8_instance"
DEFAULT_JS8_INSTANCE_NAME = "Primary JS8"
DEFAULT_FAST_LIGHT_SYSTEM_KEY = "default_fast_light"
DEFAULT_FAST_LIGHT_NAME = "Primary Fast Light"
DEFAULT_VARAC_NODE_SYSTEM_KEY = "default_varac_node"
DEFAULT_VARAC_NODE_NAME = "Primary VarAC"

SUPPORTED_DEVICE_CONTROL_BACKENDS = frozenset({"flrig", "js8call", "manual", "rigctld"})
# Keep the runtime compatibility projection aligned with 1.2.2.
SUPPORTED_RUNTIME_CONTROL_BACKENDS = frozenset({"flrig", "js8call", "manual", "rigctld"})
SUPPORTED_DEVICE_CLASSES = frozenset({"tx_rx", "observer", "gateway"})
SUPPORTED_DEPLOYMENT_MODES = frozenset({"full", "minimal"})
SUPPORTED_ASSIGNMENT_STATES = frozenset({"active", "temporary_override", "scheduled", "inactive", "superseded"})
EFFECTIVE_ASSIGNMENT_STATES = frozenset({"active", "temporary_override"})
SUPPORTED_SCHEDULER_MODES = frozenset({"full", "simple"})
SHARED_PTT_POLICY_TYPE = "shared_ptt"
SHARED_PTT_POLICY_PRIORITY = 20
RF_CONFLICT_POLICY_TYPE = "rf_conflict"
RF_CONFLICT_POLICY_PRIORITY = 30
SDR_FOLLOW_POLICY_TYPE = "sdr_follow"
SDR_FOLLOW_POLICY_PRIORITY = 60
GATEWAY_EXCLUSIVE_POLICY_TYPE = "gateway_exclusive"
GATEWAY_EXCLUSIVE_POLICY_PRIORITY = 70
PROFILE_SWAP_POLICY_TYPE = "profile_swap"
PROFILE_SWAP_POLICY_PRIORITY = 40
SUPPORTED_PROFILE_SWAP_MODES = frozenset({"use_target_profile", "carry_primary_profile"})

MIRRORED_LEGACY_KEYS = frozenset(
    {
        "control_via",
        "rig_host",
        "rig_port",
        "flrig_host",
        "flrig_port",
        "fldigi_host",
        "fldigi_port",
        "fldigi_log_path",
        "fldigi_checkin_dir",
        "js8_host",
        "js8_port",
        "js8_offset_hz",
        "js8_profile_path",
        "js8_directed_path",
        "js8_forms_path",
        "path_flrig",
        "path_fldigi",
        "path_flmsg",
        "path_flamp",
        "path_js8call",
        "path_js8spotter",
        "path_commstat",
        "varac_path",
        "varac_db_path",
        "varac_ini_path",
        "varac_launch_cmd",
        "varac_outbox_dir",
        "varac_bbs_dir",
        "varac_bbs_archive_dir",
        "varac_bbs_enabled",
        "varac_bbs_limit_access_enabled",
        "varac_bbs_allowed_callsigns",
        "varac_bbs_announce_enabled",
        "varac_bbs_vault_enabled",
        "varac_bbs_vault_managed_root",
        "varac_bbs_vault_default_location_id",
        "varac_bbs_vault_global_code_policy",
        "varac_bbs_vault_trigger_mode",
        "varac_bbs_vault_return_mode",
        "varac_bbs_vault_failed_attempt_limit",
        "varac_bbs_vault_failed_attempt_window_seconds",
        "varac_bbs_vault_cooldown_seconds",
        "varac_bbs_vault_idle_timeout_seconds",
        "varac_bbs_vault_flamp_enabled",
        "varac_bbs_vault_flamp_relay_dir",
        "varac_bbs_vault_flamp_listing_max_age_days",
        "varac_bbs_vault_locations_v1",
        "varac_bbs_vault_runtime_state_v1",
        "varac_bbs_vault_last_summary",
        "message_paths",
        "launch_control_enabled",
        "use_scheduler",
    }
)

SETTINGS_TABLE_SPECS: Dict[str, Dict[str, object]] = {
    "device_profiles": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS device_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_key TEXT UNIQUE,
            name TEXT NOT NULL,
            radio_catalog_id TEXT,
            radio_manufacturer TEXT,
            radio_model TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            runtime_active INTEGER NOT NULL DEFAULT 0,
            runtime_primary INTEGER NOT NULL DEFAULT 0,
            display_order INTEGER NOT NULL DEFAULT 0,
            device_class TEXT NOT NULL DEFAULT 'tx_rx',
            deployment_mode TEXT NOT NULL DEFAULT 'full',
            control_backend TEXT NOT NULL DEFAULT 'flrig',
            use_flrig INTEGER NOT NULL DEFAULT 0,
            use_fldigi INTEGER NOT NULL DEFAULT 0,
            use_flmsg INTEGER NOT NULL DEFAULT 0,
            use_flamp INTEGER NOT NULL DEFAULT 0,
            use_js8call INTEGER NOT NULL DEFAULT 0,
            use_js8spotter INTEGER NOT NULL DEFAULT 0,
            use_commstat INTEGER NOT NULL DEFAULT 0,
            use_varac INTEGER NOT NULL DEFAULT 0,
            rig_host TEXT,
            rig_port INTEGER,
            flrig_host TEXT,
            flrig_port INTEGER,
            fldigi_host TEXT,
            fldigi_port INTEGER,
            fldigi_log_path TEXT,
            fldigi_checkin_dir TEXT,
            flmsg_path TEXT,
            flmsg_message_path TEXT,
            flamp_path TEXT,
            flamp_message_path TEXT,
            js8_host TEXT,
            js8_port INTEGER,
            js8_instance_id INTEGER,
            js8_profile_path TEXT,
            js8_directed_path TEXT,
            js8_forms_path TEXT,
            fast_light_config_id INTEGER,
            varac_install_path TEXT,
            varac_db_path TEXT,
            varac_ini_path TEXT,
            varac_node_id INTEGER,
            varac_outbox_dir TEXT,
            varac_bbs_dir TEXT,
            varac_bbs_archive_dir TEXT,
            varac_bbs_enabled INTEGER NOT NULL DEFAULT 0,
            varac_bbs_limit_access_enabled INTEGER NOT NULL DEFAULT 0,
            varac_bbs_allowed_callsigns TEXT,
            varac_bbs_announce_enabled INTEGER NOT NULL DEFAULT 0,
            varac_bbs_auto_archive_enabled INTEGER NOT NULL DEFAULT 0,
            varac_bbs_auto_archive_days INTEGER NOT NULL DEFAULT 14,
            varac_bbs_vault_enabled INTEGER NOT NULL DEFAULT 0,
            varac_bbs_vault_managed_root TEXT,
            varac_bbs_vault_default_location_id TEXT,
            varac_bbs_vault_global_code_policy TEXT,
            varac_bbs_vault_trigger_mode TEXT,
            varac_bbs_vault_return_mode TEXT,
            varac_bbs_vault_failed_attempt_limit INTEGER NOT NULL DEFAULT 3,
            varac_bbs_vault_failed_attempt_window_seconds INTEGER NOT NULL DEFAULT 900,
            varac_bbs_vault_cooldown_seconds INTEGER NOT NULL DEFAULT 1800,
            varac_bbs_vault_idle_timeout_seconds INTEGER NOT NULL DEFAULT 600,
            varac_bbs_vault_flamp_enabled INTEGER NOT NULL DEFAULT 0,
            varac_bbs_vault_flamp_relay_dir TEXT,
            varac_bbs_vault_flamp_listing_max_age_days INTEGER NOT NULL DEFAULT 14,
            varac_bbs_vault_locations_v1 TEXT,
            varac_bbs_vault_runtime_state_v1 TEXT,
            varac_bbs_vault_last_summary TEXT,
            varac_cluster_member_enabled INTEGER DEFAULT 0,
            launch_enabled INTEGER NOT NULL DEFAULT 1,
            launch_path TEXT,
            launch_cmd TEXT,
            ptt_group TEXT,
            antenna_group TEXT,
            frontend_group TEXT,
            amplifier_group TEXT,
            sdr_host TEXT,
            sdr_port INTEGER,
            notes TEXT,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "system_key": "TEXT",
            "name": "TEXT NOT NULL",
            "radio_catalog_id": "TEXT",
            "radio_manufacturer": "TEXT",
            "radio_model": "TEXT",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "runtime_active": "INTEGER NOT NULL DEFAULT 0",
            "runtime_primary": "INTEGER NOT NULL DEFAULT 0",
            "display_order": "INTEGER NOT NULL DEFAULT 0",
            "device_class": "TEXT NOT NULL DEFAULT 'tx_rx'",
            "deployment_mode": "TEXT NOT NULL DEFAULT 'full'",
            "control_backend": "TEXT NOT NULL DEFAULT 'flrig'",
            "use_flrig": "INTEGER NOT NULL DEFAULT 0",
            "use_fldigi": "INTEGER NOT NULL DEFAULT 0",
            "use_flmsg": "INTEGER NOT NULL DEFAULT 0",
            "use_flamp": "INTEGER NOT NULL DEFAULT 0",
            "use_js8call": "INTEGER NOT NULL DEFAULT 0",
            "use_js8spotter": "INTEGER NOT NULL DEFAULT 0",
            "use_commstat": "INTEGER NOT NULL DEFAULT 0",
            "use_varac": "INTEGER NOT NULL DEFAULT 0",
            "rig_host": "TEXT",
            "rig_port": "INTEGER",
            "flrig_host": "TEXT",
            "flrig_port": "INTEGER",
            "fldigi_host": "TEXT",
            "fldigi_port": "INTEGER",
            "fldigi_log_path": "TEXT",
            "fldigi_checkin_dir": "TEXT",
            "flmsg_path": "TEXT",
            "flmsg_message_path": "TEXT",
            "flamp_path": "TEXT",
            "flamp_message_path": "TEXT",
            "js8_host": "TEXT",
            "js8_port": "INTEGER",
            "js8_instance_id": "INTEGER",
            "js8_profile_path": "TEXT",
            "js8_directed_path": "TEXT",
            "js8_forms_path": "TEXT",
            "fast_light_config_id": "INTEGER",
            "varac_install_path": "TEXT",
            "varac_db_path": "TEXT",
            "varac_ini_path": "TEXT",
            "varac_node_id": "INTEGER",
            "varac_outbox_dir": "TEXT",
            "varac_bbs_dir": "TEXT",
            "varac_bbs_archive_dir": "TEXT",
            "varac_bbs_enabled": "INTEGER NOT NULL DEFAULT 0",
            "varac_bbs_limit_access_enabled": "INTEGER NOT NULL DEFAULT 0",
            "varac_bbs_allowed_callsigns": "TEXT",
            "varac_bbs_announce_enabled": "INTEGER NOT NULL DEFAULT 0",
            "varac_bbs_auto_archive_enabled": "INTEGER NOT NULL DEFAULT 0",
            "varac_bbs_auto_archive_days": "INTEGER NOT NULL DEFAULT 14",
            "varac_bbs_vault_enabled": "INTEGER NOT NULL DEFAULT 0",
            "varac_bbs_vault_managed_root": "TEXT",
            "varac_bbs_vault_default_location_id": "TEXT",
            "varac_bbs_vault_global_code_policy": "TEXT",
            "varac_bbs_vault_trigger_mode": "TEXT",
            "varac_bbs_vault_return_mode": "TEXT",
            "varac_bbs_vault_failed_attempt_limit": "INTEGER NOT NULL DEFAULT 3",
            "varac_bbs_vault_failed_attempt_window_seconds": "INTEGER NOT NULL DEFAULT 900",
            "varac_bbs_vault_cooldown_seconds": "INTEGER NOT NULL DEFAULT 1800",
            "varac_bbs_vault_idle_timeout_seconds": "INTEGER NOT NULL DEFAULT 600",
            "varac_bbs_vault_flamp_enabled": "INTEGER NOT NULL DEFAULT 0",
            "varac_bbs_vault_flamp_relay_dir": "TEXT",
            "varac_bbs_vault_flamp_listing_max_age_days": "INTEGER NOT NULL DEFAULT 14",
            "varac_bbs_vault_locations_v1": "TEXT",
            "varac_bbs_vault_runtime_state_v1": "TEXT",
            "varac_bbs_vault_last_summary": "TEXT",
            "varac_cluster_member_enabled": "INTEGER DEFAULT 0",
            "launch_enabled": "INTEGER NOT NULL DEFAULT 1",
            "launch_path": "TEXT",
            "launch_cmd": "TEXT",
            "ptt_group": "TEXT",
            "antenna_group": "TEXT",
            "frontend_group": "TEXT",
            "amplifier_group": "TEXT",
            "sdr_host": "TEXT",
            "sdr_port": "INTEGER",
            "notes": "TEXT",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_device_profiles_system_key ON device_profiles(system_key)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_display_order ON device_profiles(display_order)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_runtime_active ON device_profiles(runtime_active)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_runtime_primary ON device_profiles(runtime_primary)",
        ),
    },
    "js8_instances": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS js8_instances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_key TEXT UNIQUE,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            host TEXT NOT NULL DEFAULT '127.0.0.1',
            port INTEGER NOT NULL DEFAULT 2442,
            offset_hz INTEGER NOT NULL DEFAULT 0,
            profile_path TEXT,
            directed_path TEXT,
            inbox_path TEXT,
            forms_path TEXT,
            install_path TEXT,
            spotter_launch_path TEXT,
            commstat_launch_path TEXT,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "system_key": "TEXT",
            "name": "TEXT NOT NULL",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "host": "TEXT NOT NULL DEFAULT '127.0.0.1'",
            "port": "INTEGER NOT NULL DEFAULT 2442",
            "offset_hz": "INTEGER NOT NULL DEFAULT 0",
            "profile_path": "TEXT",
            "directed_path": "TEXT",
            "inbox_path": "TEXT",
            "forms_path": "TEXT",
            "install_path": "TEXT",
            "spotter_launch_path": "TEXT",
            "commstat_launch_path": "TEXT",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_js8_instances_system_key ON js8_instances(system_key)",
        ),
    },
    "fast_light_configs": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS fast_light_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_key TEXT UNIQUE,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            flrig_path TEXT,
            flrig_host TEXT NOT NULL DEFAULT '127.0.0.1',
            flrig_port INTEGER NOT NULL DEFAULT 12345,
            fldigi_path TEXT,
            fldigi_host TEXT,
            fldigi_port INTEGER NOT NULL DEFAULT 7362,
            fldigi_log_path TEXT,
            fldigi_checkin_dir TEXT,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "system_key": "TEXT",
            "name": "TEXT NOT NULL",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "flrig_path": "TEXT",
            "flrig_host": "TEXT NOT NULL DEFAULT '127.0.0.1'",
            "flrig_port": "INTEGER NOT NULL DEFAULT 12345",
            "fldigi_path": "TEXT",
            "fldigi_host": "TEXT",
            "fldigi_port": "INTEGER NOT NULL DEFAULT 7362",
            "fldigi_log_path": "TEXT",
            "fldigi_checkin_dir": "TEXT",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_fast_light_configs_system_key ON fast_light_configs(system_key)",
        ),
    },
    "varac_nodes": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS varac_nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_key TEXT UNIQUE,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            install_path TEXT,
            db_path TEXT,
            ini_path TEXT,
            launch_cmd TEXT,
            incoming_path TEXT,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "system_key": "TEXT",
            "name": "TEXT NOT NULL",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "install_path": "TEXT",
            "db_path": "TEXT",
            "ini_path": "TEXT",
            "launch_cmd": "TEXT",
            "incoming_path": "TEXT",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_varac_nodes_system_key ON varac_nodes(system_key)",
        ),
    },
    "operating_profiles": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS operating_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_key TEXT UNIQUE,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            description TEXT,
            scheduler_enabled INTEGER NOT NULL DEFAULT 1,
            scheduler_mode TEXT NOT NULL DEFAULT 'full',
            preferred_band_set_json TEXT NOT NULL DEFAULT '[]',
            use_messages INTEGER NOT NULL DEFAULT 1,
            use_map INTEGER NOT NULL DEFAULT 1,
            use_background_ingest INTEGER NOT NULL DEFAULT 1,
            use_launch_control INTEGER NOT NULL DEFAULT 1,
            use_net_control_tabs INTEGER NOT NULL DEFAULT 1,
            allow_profile_swap INTEGER NOT NULL DEFAULT 0,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "system_key": "TEXT",
            "name": "TEXT NOT NULL",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "description": "TEXT",
            "scheduler_enabled": "INTEGER NOT NULL DEFAULT 1",
            "scheduler_mode": "TEXT NOT NULL DEFAULT 'full'",
            "preferred_band_set_json": "TEXT NOT NULL DEFAULT '[]'",
            "use_messages": "INTEGER NOT NULL DEFAULT 1",
            "use_map": "INTEGER NOT NULL DEFAULT 1",
            "use_background_ingest": "INTEGER NOT NULL DEFAULT 1",
            "use_launch_control": "INTEGER NOT NULL DEFAULT 1",
            "use_net_control_tabs": "INTEGER NOT NULL DEFAULT 1",
            "allow_profile_swap": "INTEGER NOT NULL DEFAULT 0",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_operating_profiles_system_key ON operating_profiles(system_key)",
        ),
    },
    "operating_profile_assignments": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS operating_profile_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_profile_id INTEGER NOT NULL,
            operating_profile_id INTEGER NOT NULL,
            assignment_state TEXT NOT NULL DEFAULT 'active',
            starts_utc TEXT,
            ends_utc TEXT,
            reason TEXT,
            created_by TEXT,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "device_profile_id": "INTEGER NOT NULL",
            "operating_profile_id": "INTEGER NOT NULL",
            "assignment_state": "TEXT NOT NULL DEFAULT 'active'",
            "starts_utc": "TEXT",
            "ends_utc": "TEXT",
            "reason": "TEXT",
            "created_by": "TEXT",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE INDEX IF NOT EXISTS idx_assignments_device_state ON operating_profile_assignments(device_profile_id, assignment_state)",
            "CREATE INDEX IF NOT EXISTS idx_assignments_operating_state ON operating_profile_assignments(operating_profile_id, assignment_state)",
        ),
    },
    "station_coordination_policies": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS station_coordination_policies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            policy_type TEXT NOT NULL,
            source_device_id INTEGER,
            target_device_id INTEGER,
            priority INTEGER NOT NULL DEFAULT 100,
            trigger_json TEXT NOT NULL DEFAULT '{}',
            action_json TEXT NOT NULL DEFAULT '{}',
            safety_mode TEXT NOT NULL DEFAULT 'warn',
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "name": "TEXT NOT NULL",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "policy_type": "TEXT NOT NULL",
            "source_device_id": "INTEGER",
            "target_device_id": "INTEGER",
            "priority": "INTEGER NOT NULL DEFAULT 100",
            "trigger_json": "TEXT NOT NULL DEFAULT '{}'",
            "action_json": "TEXT NOT NULL DEFAULT '{}'",
            "safety_mode": "TEXT NOT NULL DEFAULT 'warn'",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE INDEX IF NOT EXISTS idx_coordination_policies_enabled ON station_coordination_policies(enabled)",
            "CREATE INDEX IF NOT EXISTS idx_coordination_policies_type ON station_coordination_policies(policy_type)",
        ),
    },
    "varac_clusters": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS varac_clusters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            cluster_id TEXT NOT NULL,
            shared_db_path TEXT,
            counters_refresh_sec INTEGER NOT NULL DEFAULT 30,
            ptt_lock_enabled INTEGER NOT NULL DEFAULT 0,
            gateway_handler_device_id INTEGER,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "name": "TEXT NOT NULL",
            "cluster_id": "TEXT NOT NULL",
            "shared_db_path": "TEXT",
            "counters_refresh_sec": "INTEGER NOT NULL DEFAULT 30",
            "ptt_lock_enabled": "INTEGER NOT NULL DEFAULT 0",
            "gateway_handler_device_id": "INTEGER",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE INDEX IF NOT EXISTS idx_varac_clusters_cluster_id ON varac_clusters(cluster_id)",
        ),
    },
    "varac_cluster_members": {
        "ddl": """
        CREATE TABLE IF NOT EXISTS varac_cluster_members (
            cluster_id INTEGER NOT NULL,
            device_profile_id INTEGER NOT NULL,
            instance_number INTEGER NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            PRIMARY KEY (cluster_id, device_profile_id)
        )
        """,
        "columns": {
            "cluster_id": "INTEGER NOT NULL",
            "device_profile_id": "INTEGER NOT NULL",
            "instance_number": "INTEGER NOT NULL",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_varac_cluster_members_instance ON varac_cluster_members(cluster_id, instance_number)",
        ),
    },
}


def settings_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout.db"


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _coerce_text(value: Any, default: str = "") -> str:
    try:
        return str(value if value is not None else default).strip()
    except Exception:
        return str(default or "").strip()


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value if value not in (None, "") else default)
    except Exception:
        return int(default)


def _coerce_optional_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def _coerce_bool_int(value: Any, default: bool = False) -> int:
    if value in (None, ""):
        return 1 if default else 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if int(value) != 0 else 0
    return 1 if str(value).strip().lower() in {"1", "true", "yes", "on"} else 0


def _normalize_system_key(raw: Any, fallback: str) -> str:
    text = _coerce_text(raw, fallback).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or fallback


def _settings_text(values: Mapping[str, Any], key: str, default: str = "") -> str:
    return _coerce_text(values.get(key, default), default)


def _settings_int(values: Mapping[str, Any], key: str, default: int) -> int:
    return _coerce_int(values.get(key, default), default)


def _settings_optional_int(values: Mapping[str, Any], key: str, default: Optional[int] = None) -> Optional[int]:
    return _coerce_optional_int(values.get(key), default)


def _settings_bool(values: Mapping[str, Any], key: str, default: bool) -> bool:
    return bool(_coerce_bool_int(values.get(key), default))


def _fetchone_dict(cursor: sqlite3.Cursor) -> Optional[Dict[str, Any]]:
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return dict(row)
    columns = [str(col[0]) for col in (cursor.description or [])]
    return {columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))}


def _fetchall_dicts(conn: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    original_row_factory = conn.row_factory
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.row_factory = original_row_factory


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _load_kv_settings(conn: sqlite3.Connection) -> Dict[str, Any]:
    loaded: Dict[str, Any] = {}
    for key, value in conn.execute("SELECT key, value FROM kv").fetchall():
        try:
            loaded[str(key)] = json.loads(value)
        except Exception:
            loaded[str(key)] = value
    return loaded


def _write_kv_settings(conn: sqlite3.Connection, updates: Mapping[str, Any]) -> None:
    if not updates:
        return
    conn.executemany(
        "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
        [(str(key), json.dumps(value)) for key, value in updates.items()],
    )


def _next_system_key(conn: sqlite3.Connection, table: str, requested: Any, *, exclude_id: Optional[int] = None) -> str:
    base = _normalize_system_key(requested, "record")
    candidate = base
    suffix = 2
    while True:
        if exclude_id is None:
            row = conn.execute(f"SELECT id FROM {table} WHERE system_key=?", (candidate,)).fetchone()
        else:
            row = conn.execute(
                f"SELECT id FROM {table} WHERE system_key=? AND id<>?",
                (candidate, int(exclude_id)),
            ).fetchone()
        if not row:
            return candidate
        candidate = f"{base}_{suffix}"
        suffix += 1


def _normalize_control_backend(settings_values: Mapping[str, Any]) -> str:
    raw = _coerce_text(settings_values.get("control_via", "FLRig"), "FLRig").upper()
    if raw == "FLRIG":
        return "flrig"
    if raw == "JS8CALL":
        return "js8call"
    if raw == "RIGCTLD":
        return "rigctld"
    return "manual"


def _legacy_control_via(control_backend: str) -> str:
    backend = _coerce_text(control_backend, "manual").lower()
    if backend == "flrig":
        return "FLRig"
    if backend == "js8call":
        return "JS8Call"
    if backend == "rigctld":
        return "RIGCTLD"
    return "Manual"


def _normalize_assignment_state(value: Any, default: str = "active") -> str:
    state = _coerce_text(value, default).strip().lower() or default
    return state if state in SUPPORTED_ASSIGNMENT_STATES else default


def normalize_ptt_group(value: Any) -> str:
    text = _coerce_text(value, "")
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().upper()


def normalize_resource_group(value: Any) -> str:
    text = _coerce_text(value, "")
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().upper()


def _normalize_band_token(value: Any) -> str:
    return re.sub(r"\s+", "", _coerce_text(value, "").upper())


def _parse_string_list(value: Any) -> List[str]:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = [part.strip() for part in text.split(",") if part.strip()]
    elif isinstance(value, (list, tuple, set)):
        parsed = list(value)
    else:
        parsed = []
    out = [_normalize_band_token(item) for item in parsed if _normalize_band_token(item)]
    return list(dict.fromkeys(out))


def _coerce_json_array_text(value: Any) -> str:
    return json.dumps(_parse_string_list(value))


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if value in (None, ""):
        return []
    try:
        loaded = json.loads(str(value))
    except Exception:
        return []
    return list(loaded) if isinstance(loaded, list) else []


def _coerce_json_list_text(value: Any) -> str:
    if isinstance(value, list):
        return json.dumps(value, sort_keys=True)
    if value in (None, ""):
        return "[]"
    return json.dumps(_parse_json_list(value), sort_keys=True)


def _normalize_varac_cluster_id(value: Any, fallback: str = "CLUSTER") -> str:
    text = _coerce_text(value, fallback).upper() or fallback
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^A-Z0-9_.-]+", "-", text)
    text = text.strip("._-")
    return text or fallback


def _is_observer_device_class(value: Any) -> bool:
    raw = value.get("device_class", "tx_rx") if isinstance(value, Mapping) else value
    return _coerce_text(raw, "tx_rx").lower() == "observer"


def _normalize_profile_swap_mode(value: Any, default: str = "use_target_profile") -> str:
    mode = _coerce_text(value, default).lower() or default
    return mode if mode in SUPPORTED_PROFILE_SWAP_MODES else default


def _parse_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        loaded = json.loads(str(value))
    except Exception:
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _coerce_json_object_text(value: Any) -> str:
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    if value in (None, ""):
        return "{}"
    parsed = _parse_json_object(value)
    return json.dumps(parsed, sort_keys=True)


def _coordination_policy_from_row(row: sqlite3.Row | Mapping[str, Any] | Dict[str, Any]) -> Dict[str, Any]:
    data = dict(row)
    data["policy_type"] = _coerce_text(data.get("policy_type", SHARED_PTT_POLICY_TYPE), SHARED_PTT_POLICY_TYPE).lower()
    data["safety_mode"] = _coerce_text(data.get("safety_mode", "warn"), "warn").lower() or "warn"
    data["trigger"] = _parse_json_object(data.get("trigger_json", "{}"))
    data["action"] = _parse_json_object(data.get("action_json", "{}"))
    return data


def _active_profile_swap_policy_conn(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT *
          FROM station_coordination_policies
         WHERE policy_type=?
           AND enabled=1
      ORDER BY priority ASC, id DESC
         LIMIT 1
        """,
        (PROFILE_SWAP_POLICY_TYPE,),
    ).fetchone()
    if row is None:
        return None
    return _coordination_policy_from_row(row)


def _assignment_snapshot_from_row(row: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(row, Mapping):
        return {}
    operating_profile_id = row.get("operating_profile_id")
    return {
        "operating_profile_id": (
            int(operating_profile_id or 0) if operating_profile_id not in (None, "") else None
        ),
        "assignment_state": _normalize_assignment_state(row.get("assignment_state", "active"), "active"),
        "reason": _coerce_text(row.get("reason", ""), ""),
        "created_by": _coerce_text(row.get("created_by", "settings_ui"), "settings_ui") or "settings_ui",
        "starts_utc": _coerce_text(row.get("starts_utc", ""), ""),
        "ends_utc": _coerce_text(row.get("ends_utc", ""), ""),
    }


def _enrich_profile_swap_policy(conn: sqlite3.Connection, policy: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(policy, Mapping):
        return None
    data = dict(policy)
    trigger = dict(data.get("trigger") or {})
    action = dict(data.get("action") or {})
    source_id = int(data.get("source_device_id", 0) or 0)
    target_id = int(data.get("target_device_id", 0) or 0)
    source_device = _record_by_id(conn, "device_profiles", source_id) if source_id > 0 else None
    target_device = _record_by_id(conn, "device_profiles", target_id) if target_id > 0 else None
    data["source_device_name"] = _coerce_text((source_device or {}).get("name", ""), "")
    data["target_device_name"] = _coerce_text((target_device or {}).get("name", ""), "")
    data["mode"] = _normalize_profile_swap_mode(trigger.get("mode", "use_target_profile"))
    carried_profile_id = action.get("applied_operating_profile_id")
    if carried_profile_id not in (None, ""):
        carried_profile = _record_by_id(conn, "operating_profiles", int(carried_profile_id))
        data["applied_operating_profile_name"] = _coerce_text((carried_profile or {}).get("name", ""), "")
    else:
        data["applied_operating_profile_name"] = ""
    restore_assignment = dict(action.get("restore_target_assignment") or {})
    restore_operating_id = restore_assignment.get("operating_profile_id")
    if restore_operating_id not in (None, ""):
        restore_profile = _record_by_id(conn, "operating_profiles", int(restore_operating_id))
        data["restore_target_operating_profile_name"] = _coerce_text((restore_profile or {}).get("name", ""), "")
    else:
        data["restore_target_operating_profile_name"] = ""
    return data


def _varac_cluster_by_id(conn: sqlite3.Connection, cluster_db_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM varac_clusters WHERE id=?", (int(cluster_db_id),))
    return _fetchone_dict(cur)


def _varac_cluster_membership_row(
    conn: sqlite3.Connection,
    cluster_db_id: int,
    device_profile_id: int,
) -> Optional[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
          FROM varac_cluster_members
         WHERE cluster_id=?
           AND device_profile_id=?
        """,
        (int(cluster_db_id), int(device_profile_id)),
    )
    return _fetchone_dict(cur)


def _varac_enabled_membership_for_device(
    conn: sqlite3.Connection,
    device_profile_id: int,
    *,
    exclude_cluster_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    params: List[Any] = [int(device_profile_id)]
    where = ""
    if exclude_cluster_id is not None:
        where = " AND cluster_id<>?"
        params.append(int(exclude_cluster_id))
    cur = conn.execute(
        f"""
        SELECT *
          FROM varac_cluster_members
         WHERE device_profile_id=?
           AND enabled=1{where}
      ORDER BY cluster_id ASC
         LIMIT 1
        """,
        params,
    )
    return _fetchone_dict(cur)


def _device_has_varac_cluster_membership(conn: sqlite3.Connection, device_profile_id: int) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM varac_cluster_members WHERE device_profile_id=? LIMIT 1",
        (int(device_profile_id),),
    )
    return cur.fetchone() is not None


def _sync_varac_cluster_member_enabled_flags_conn(conn: sqlite3.Connection) -> None:
    ensure_multi_radio_settings_schema(conn)
    conn.execute(
        """
        UPDATE device_profiles
           SET varac_cluster_member_enabled = CASE
                WHEN EXISTS (
                    SELECT 1
                      FROM varac_cluster_members
                     WHERE device_profile_id=device_profiles.id
                       AND enabled=1
                ) THEN 1 ELSE 0 END
        """
    )


def _list_varac_clusters_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    rows = conn.execute(
        """
        SELECT
            c.*,
            COUNT(m.device_profile_id) AS member_count,
            COALESCE(SUM(CASE WHEN m.enabled=1 THEN 1 ELSE 0 END), 0) AS enabled_member_count,
            g.name AS gateway_handler_name
          FROM varac_clusters c
          LEFT JOIN varac_cluster_members m
            ON m.cluster_id = c.id
          LEFT JOIN device_profiles g
            ON g.id = c.gateway_handler_device_id
      GROUP BY c.id
      ORDER BY LOWER(c.name) ASC, c.id ASC
        """
    ).fetchall()
    clusters: List[Dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        cluster_id = int(data.get("id", 0) or 0)
        member_rows = conn.execute(
            """
            SELECT
                m.device_profile_id,
                m.instance_number,
                m.enabled,
                d.name AS device_name
              FROM varac_cluster_members m
              LEFT JOIN device_profiles d
                ON d.id = m.device_profile_id
             WHERE m.cluster_id=?
          ORDER BY m.enabled DESC, m.instance_number ASC, LOWER(COALESCE(d.name, '')) ASC, m.device_profile_id ASC
            """,
            (cluster_id,),
        ).fetchall()
        enabled_members = [
            dict(member)
            for member in member_rows
            if int(dict(member).get("enabled", 1) or 0) == 1
        ]
        data["member_count"] = int(data.get("member_count", 0) or 0)
        data["enabled_member_count"] = int(data.get("enabled_member_count", 0) or 0)
        data["gateway_handler_device_id"] = (
            int(data.get("gateway_handler_device_id", 0) or 0)
            if data.get("gateway_handler_device_id") not in (None, "")
            else None
        )
        data["gateway_handler_name"] = _coerce_text(data.get("gateway_handler_name", ""), "")
        data["member_device_ids"] = [int(dict(member).get("device_profile_id", 0) or 0) for member in enabled_members]
        data["member_names"] = [
            _coerce_text(dict(member).get("device_name", ""), "")
            for member in enabled_members
            if _coerce_text(dict(member).get("device_name", ""), "")
        ]
        data["gateway_handler_ready"] = (
            data.get("gateway_handler_device_id") in data["member_device_ids"]
            if data.get("gateway_handler_device_id") is not None
            else False
        )
        clusters.append(data)
    return clusters


def _list_varac_cluster_members_conn(
    conn: sqlite3.Connection,
    *,
    cluster_id: Optional[int] = None,
    device_profile_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    where: List[str] = []
    params: List[Any] = []
    if cluster_id is not None:
        where.append("m.cluster_id=?")
        params.append(int(cluster_id))
    if device_profile_id is not None:
        where.append("m.device_profile_id=?")
        params.append(int(device_profile_id))
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""
        SELECT
            m.cluster_id,
            m.device_profile_id,
            m.instance_number,
            m.enabled,
            m.created_utc,
            m.updated_utc,
            c.name AS cluster_name,
            c.cluster_id AS cluster_public_id,
            c.shared_db_path,
            c.counters_refresh_sec,
            c.ptt_lock_enabled,
            c.gateway_handler_device_id,
            d.name AS device_name,
            d.device_class,
            d.enabled AS device_enabled,
            d.runtime_active,
            d.runtime_primary
          FROM varac_cluster_members m
          JOIN varac_clusters c
            ON c.id = m.cluster_id
          JOIN device_profiles d
            ON d.id = m.device_profile_id
          {where_sql}
      ORDER BY LOWER(c.name) ASC, m.instance_number ASC, LOWER(d.name) ASC, m.device_profile_id ASC
        """,
        params,
    ).fetchall()
    members: List[Dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        gateway_id = (
            int(data.get("gateway_handler_device_id", 0) or 0)
            if data.get("gateway_handler_device_id") not in (None, "")
            else None
        )
        data["cluster_db_id"] = int(data.get("cluster_id", 0) or 0)
        data["cluster_public_id"] = _coerce_text(data.get("cluster_public_id", ""), "")
        data["device_class"] = _coerce_text(data.get("device_class", "tx_rx"), "tx_rx").lower() or "tx_rx"
        data["gateway_handler_device_id"] = gateway_id
        data["is_gateway_handler"] = gateway_id is not None and int(data.get("device_profile_id", 0) or 0) == gateway_id
        members.append(data)
    return members


def _sync_pair_coordination_policies_conn(
    conn: sqlite3.Connection,
    *,
    policy_type: str,
    expected: Mapping[tuple[int, int], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    existing_rows = _fetchall_dicts(
        conn,
        """
        SELECT *
          FROM station_coordination_policies
         WHERE policy_type=?
        """,
        (str(policy_type),),
    )
    existing_by_pair: Dict[tuple[int, int], Dict[str, Any]] = {}
    for row in existing_rows:
        data = dict(row)
        pair = (
            int(data.get("source_device_id", 0) or 0),
            int(data.get("target_device_id", 0) or 0),
        )
        existing_by_pair[pair] = data

    now_iso = _utc_now_iso()
    for pair, record in expected.items():
        current = existing_by_pair.pop(pair, None)
        if current is None:
            conn.execute(
                """
                INSERT INTO station_coordination_policies (
                    name, enabled, policy_type, source_device_id, target_device_id,
                    priority, trigger_json, action_json, safety_mode, created_utc, updated_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["name"],
                    int(record["enabled"]),
                    record["policy_type"],
                    int(record["source_device_id"]),
                    int(record["target_device_id"]),
                    int(record["priority"]),
                    record["trigger_json"],
                    record["action_json"],
                    record["safety_mode"],
                    now_iso,
                    now_iso,
                ),
            )
            continue
        conn.execute(
            """
            UPDATE station_coordination_policies
               SET name=?, enabled=?, priority=?, trigger_json=?, action_json=?, safety_mode=?, updated_utc=?
             WHERE id=?
            """,
            (
                record["name"],
                int(record["enabled"]),
                int(record["priority"]),
                record["trigger_json"],
                record["action_json"],
                record["safety_mode"],
                now_iso,
                int(current.get("id", 0) or 0),
            ),
        )

    stale_ids = [int(row.get("id", 0) or 0) for row in existing_by_pair.values() if int(row.get("id", 0) or 0) > 0]
    if stale_ids:
        placeholders = ", ".join("?" for _ in stale_ids)
        conn.execute(f"DELETE FROM station_coordination_policies WHERE id IN ({placeholders})", tuple(stale_ids))
    conn.commit()
    refreshed = _fetchall_dicts(
        conn,
        """
        SELECT *
          FROM station_coordination_policies
         WHERE policy_type=?
      ORDER BY priority ASC, source_device_id ASC, target_device_id ASC, id ASC
        """,
        (str(policy_type),),
    )
    return [_coordination_policy_from_row(row) for row in refreshed]


def _sync_shared_ptt_policies_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    rows = _fetchall_dicts(
        conn,
        """
        SELECT id, name, enabled, device_class, ptt_group
          FROM device_profiles
         WHERE enabled=1
      ORDER BY id ASC
        """
    )
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        device = dict(row)
        if _coerce_text(device.get("device_class", "tx_rx"), "tx_rx").lower() == "observer":
            continue
        group = normalize_ptt_group(device.get("ptt_group", ""))
        if not group:
            continue
        groups.setdefault(group, []).append(device)

    expected: Dict[tuple[int, int], Dict[str, Any]] = {}
    for group, members in groups.items():
        sorted_members = sorted(members, key=lambda item: (str(item.get("name", "")).lower(), int(item.get("id", 0) or 0)))
        for idx, left in enumerate(sorted_members):
            left_id = int(left.get("id", 0) or 0)
            left_name = _coerce_text(left.get("name", f"Device {left_id}"), f"Device {left_id}")
            for right in sorted_members[idx + 1 :]:
                right_id = int(right.get("id", 0) or 0)
                right_name = _coerce_text(right.get("name", f"Device {right_id}"), f"Device {right_id}")
                source_id, target_id = sorted((left_id, right_id))
                name_a, name_b = (left_name, right_name) if source_id == left_id else (right_name, left_name)
                expected[(source_id, target_id)] = {
                    "name": f"Shared PTT {group}: {name_a} <-> {name_b}",
                    "enabled": 1,
                    "policy_type": SHARED_PTT_POLICY_TYPE,
                    "source_device_id": source_id,
                    "target_device_id": target_id,
                    "priority": SHARED_PTT_POLICY_PRIORITY,
                    "trigger_json": _coerce_json_object_text({"ptt_group": group}),
                    "action_json": _coerce_json_object_text(
                        {"interlock": "block_primary_frequency_control", "scope": "primary_runtime"}
                    ),
                    "safety_mode": "auto",
                }
    return _sync_pair_coordination_policies_conn(
        conn,
        policy_type=SHARED_PTT_POLICY_TYPE,
        expected=expected,
    )


def _sync_rf_conflict_policies_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    rows = _fetchall_dicts(
        conn,
        """
        SELECT id, name, enabled, device_class, antenna_group, frontend_group, amplifier_group
          FROM device_profiles
         WHERE enabled=1
      ORDER BY id ASC
        """,
    )
    devices: List[Dict[str, Any]] = []
    for row in rows:
        device = dict(row)
        if _coerce_text(device.get("device_class", "tx_rx"), "tx_rx").lower() == "observer":
            continue
        device["name"] = _coerce_text(device.get("name", f"Device {int(device.get('id', 0) or 0)}"))
        device["antenna_group"] = normalize_resource_group(device.get("antenna_group", ""))
        device["frontend_group"] = normalize_resource_group(device.get("frontend_group", ""))
        device["amplifier_group"] = normalize_resource_group(device.get("amplifier_group", ""))
        devices.append(device)

    groups_by_field: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        "antenna_group": {},
        "frontend_group": {},
        "amplifier_group": {},
    }
    for device in devices:
        for field_name in groups_by_field.keys():
            group_value = str(device.get(field_name, "") or "").strip()
            if not group_value:
                continue
            groups_by_field[field_name].setdefault(group_value, []).append(device)

    pair_map: Dict[tuple[int, int], Dict[str, Any]] = {}
    group_columns = (
        ("antenna_group", "antenna_groups"),
        ("amplifier_group", "amplifier_groups"),
        ("frontend_group", "frontend_groups"),
    )
    for field_name, trigger_key in group_columns:
        for group_name, members in groups_by_field[field_name].items():
            sorted_members = sorted(
                members,
                key=lambda item: (str(item.get("name", "")).lower(), int(item.get("id", 0) or 0)),
            )
            for idx, left in enumerate(sorted_members):
                left_id = int(left.get("id", 0) or 0)
                left_name = _coerce_text(left.get("name", f"Device {left_id}"), f"Device {left_id}")
                for right in sorted_members[idx + 1 :]:
                    right_id = int(right.get("id", 0) or 0)
                    right_name = _coerce_text(right.get("name", f"Device {right_id}"), f"Device {right_id}")
                    source_id, target_id = sorted((left_id, right_id))
                    source_name, target_name = (
                        (left_name, right_name) if source_id == left_id else (right_name, left_name)
                    )
                    pair = (source_id, target_id)
                    pair_entry = pair_map.setdefault(
                        pair,
                        {
                            "source_id": source_id,
                            "target_id": target_id,
                            "source_name": source_name,
                            "target_name": target_name,
                            "antenna_groups": set(),
                            "frontend_groups": set(),
                            "amplifier_groups": set(),
                        },
                    )
                    pair_entry[str(trigger_key)].add(group_name)

    expected: Dict[tuple[int, int], Dict[str, Any]] = {}
    for pair, info in pair_map.items():
        trigger = {
            "antenna_groups": sorted(str(group) for group in info["antenna_groups"]),
            "frontend_groups": sorted(str(group) for group in info["frontend_groups"]),
            "amplifier_groups": sorted(str(group) for group in info["amplifier_groups"]),
        }
        if not any(trigger.values()):
            continue
        expected[pair] = {
            "name": f"RF Conflict: {info['source_name']} <-> {info['target_name']}",
            "enabled": 1,
            "policy_type": RF_CONFLICT_POLICY_TYPE,
            "source_device_id": int(info["source_id"]),
            "target_device_id": int(info["target_id"]),
            "priority": RF_CONFLICT_POLICY_PRIORITY,
            "trigger_json": _coerce_json_object_text(trigger),
            "action_json": _coerce_json_object_text(
                {"warning": "primary_runtime_rf_overlap", "scope": "primary_runtime"}
            ),
            "safety_mode": "prompt",
        }
    return _sync_pair_coordination_policies_conn(
        conn,
        policy_type=RF_CONFLICT_POLICY_TYPE,
        expected=expected,
    )


def _sync_sdr_follow_policies_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    rows = _fetchall_dicts(
        conn,
        """
        SELECT id, name, enabled, device_class, antenna_group, frontend_group, sdr_host, sdr_port
          FROM device_profiles
         WHERE enabled=1
      ORDER BY id ASC
        """,
    )
    observers: List[Dict[str, Any]] = []
    transceivers: List[Dict[str, Any]] = []
    for row in rows:
        device = dict(row)
        device["name"] = _coerce_text(device.get("name", f"Device {int(device.get('id', 0) or 0)}"))
        device["antenna_group"] = normalize_resource_group(device.get("antenna_group", ""))
        device["frontend_group"] = normalize_resource_group(device.get("frontend_group", ""))
        if _is_observer_device_class(device):
            observers.append(device)
        else:
            transceivers.append(device)

    expected: Dict[tuple[int, int], Dict[str, Any]] = {}
    for source in transceivers:
        source_id = int(source.get("id", 0) or 0)
        if source_id <= 0:
            continue
        source_name = _coerce_text(source.get("name", f"Device {source_id}"), f"Device {source_id}")
        source_antenna = str(source.get("antenna_group", "") or "").strip()
        source_frontend = str(source.get("frontend_group", "") or "").strip()
        for observer in observers:
            target_id = int(observer.get("id", 0) or 0)
            if target_id <= 0 or target_id == source_id:
                continue
            observer_name = _coerce_text(observer.get("name", f"Device {target_id}"), f"Device {target_id}")
            observer_antenna = str(observer.get("antenna_group", "") or "").strip()
            observer_frontend = str(observer.get("frontend_group", "") or "").strip()
            expected[(source_id, target_id)] = {
                "name": f"SDR Follow: {source_name} -> {observer_name}",
                "enabled": 1,
                "policy_type": SDR_FOLLOW_POLICY_TYPE,
                "source_device_id": source_id,
                "target_device_id": target_id,
                "priority": SDR_FOLLOW_POLICY_PRIORITY,
                "trigger_json": _coerce_json_object_text(
                    {
                        "source_scope": "primary_runtime",
                        "shared_antenna_groups": [source_antenna] if source_antenna and source_antenna == observer_antenna else [],
                        "shared_frontend_groups": [source_frontend] if source_frontend and source_frontend == observer_frontend else [],
                    }
                ),
                "action_json": _coerce_json_object_text(
                    {
                        "guidance": "observer_follow_advisory",
                        "park_strategy": "alternate_preferred_band",
                        "fallback": "follow_primary_band",
                    }
                ),
                "safety_mode": "warn",
            }
    return _sync_pair_coordination_policies_conn(
        conn,
        policy_type=SDR_FOLLOW_POLICY_TYPE,
        expected=expected,
    )


def _sync_gateway_exclusive_policies_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    expected: Dict[tuple[int, int], Dict[str, Any]] = {}
    for cluster in _list_varac_clusters_conn(conn):
        cluster_db_id = int(cluster.get("id", 0) or 0)
        gateway_id = (
            int(cluster.get("gateway_handler_device_id", 0) or 0)
            if cluster.get("gateway_handler_device_id") not in (None, "")
            else 0
        )
        if cluster_db_id <= 0 or gateway_id <= 0:
            continue
        gateway_row = next(
            (
                row
                for row in _list_varac_cluster_members_conn(conn, cluster_id=cluster_db_id)
                if int(row.get("device_profile_id", 0) or 0) == gateway_id and int(row.get("enabled", 1) or 0) == 1
            ),
            None,
        )
        if not gateway_row:
            continue
        gateway_name = _coerce_text(gateway_row.get("device_name", f"Device {gateway_id}"), f"Device {gateway_id}")
        cluster_name = _coerce_text(cluster.get("name", f"Cluster {cluster_db_id}"), f"Cluster {cluster_db_id}")
        cluster_public_id = _coerce_text(cluster.get("cluster_id", ""), "")
        for member in _list_varac_cluster_members_conn(conn, cluster_id=cluster_db_id):
            member_id = int(member.get("device_profile_id", 0) or 0)
            if member_id <= 0 or member_id == gateway_id or int(member.get("enabled", 1) or 0) != 1:
                continue
            member_name = _coerce_text(member.get("device_name", f"Device {member_id}"), f"Device {member_id}")
            expected[(gateway_id, member_id)] = {
                "name": f"Gateway Exclusive: {cluster_name} {gateway_name} -> {member_name}",
                "enabled": 1,
                "policy_type": GATEWAY_EXCLUSIVE_POLICY_TYPE,
                "source_device_id": gateway_id,
                "target_device_id": member_id,
                "priority": GATEWAY_EXCLUSIVE_POLICY_PRIORITY,
                "trigger_json": _coerce_json_object_text(
                    {
                        "cluster_db_id": cluster_db_id,
                        "cluster_id": cluster_public_id,
                        "cluster_name": cluster_name,
                    }
                ),
                "action_json": _coerce_json_object_text(
                    {
                        "interlock": "gateway_handler_exclusive",
                        "cluster_db_id": cluster_db_id,
                        "cluster_id": cluster_public_id,
                        "cluster_name": cluster_name,
                        "gateway_handler_device_id": gateway_id,
                        "gateway_handler_name": gateway_name,
                        "source_instance_number": int(gateway_row.get("instance_number", 0) or 0),
                        "target_instance_number": int(member.get("instance_number", 0) or 0),
                    }
                ),
                "safety_mode": "warn",
            }
    return _sync_pair_coordination_policies_conn(
        conn,
        policy_type=GATEWAY_EXCLUSIVE_POLICY_TYPE,
        expected=expected,
    )


def _sync_derived_coordination_policies_conn(conn: sqlite3.Connection) -> None:
    _sync_varac_cluster_member_enabled_flags_conn(conn)
    _sync_shared_ptt_policies_conn(conn)
    _sync_rf_conflict_policies_conn(conn)
    _sync_sdr_follow_policies_conn(conn)
    _sync_gateway_exclusive_policies_conn(conn)


def ensure_multi_radio_settings_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS kv (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    for table_name, spec in SETTINGS_TABLE_SPECS.items():
        conn.execute(str(spec["ddl"]))
        existing = _table_columns(conn, table_name)
        for column_name, column_type in dict(spec["columns"]).items():
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        for index_sql in tuple(spec["indexes"]):
            conn.execute(str(index_sql))
    conn.commit()


def _record_by_id(conn: sqlite3.Connection, table: str, record_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute(f"SELECT * FROM {table} WHERE id=?", (int(record_id),))
    return _fetchone_dict(cur)


def _record_by_system_key(conn: sqlite3.Connection, table: str, system_key: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute(f"SELECT * FROM {table} WHERE system_key=?", (str(system_key),))
    return _fetchone_dict(cur)


def _save_simple_record(
    conn: sqlite3.Connection,
    table: str,
    values: Mapping[str, Any],
    *,
    default_system_key: str,
    default_name: str,
    fields: Iterable[str],
) -> Dict[str, Any]:
    payload = dict(values)
    record_id = _coerce_optional_int(payload.get("id"))
    existing = _record_by_id(conn, table, record_id) if record_id is not None else None
    now_iso = _utc_now_iso()
    system_key = _next_system_key(
        conn,
        table,
        payload.get("system_key", (existing or {}).get("system_key", default_system_key)),
        exclude_id=record_id,
    )
    record: Dict[str, Any] = {
        "system_key": system_key,
        "name": _coerce_text(payload.get("name", (existing or {}).get("name", default_name)), default_name) or default_name,
        "enabled": _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True),
        "created_utc": (existing or {}).get("created_utc", now_iso),
        "updated_utc": now_iso,
    }
    for field in fields:
        default_value = (existing or {}).get(field)
        if field.endswith("_port") or field == "offset_hz":
            record[field] = _coerce_optional_int(payload.get(field, default_value), default_value)
        else:
            record[field] = payload.get(field, default_value)

    columns = list(record.keys())
    if existing:
        assignments = ", ".join(f"{name}=?" for name in columns)
        params = [record[name] for name in columns] + [int(record_id)]
        conn.execute(f"UPDATE {table} SET {assignments} WHERE id=?", params)
        conn.commit()
        return _record_by_id(conn, table, int(record_id)) or {}

    placeholders = ", ".join(["?"] * len(columns))
    conn.execute(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
        [record[name] for name in columns],
    )
    conn.commit()
    return _record_by_system_key(conn, table, system_key) or {}


def _save_js8_instance_conn(conn: sqlite3.Connection, values: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(values)
    payload.setdefault("host", "127.0.0.1")
    payload.setdefault("port", 2442)
    payload.setdefault("offset_hz", 0)
    return _save_simple_record(
        conn,
        "js8_instances",
        payload,
        default_system_key=DEFAULT_JS8_INSTANCE_SYSTEM_KEY,
        default_name=DEFAULT_JS8_INSTANCE_NAME,
        fields=(
            "host",
            "port",
            "offset_hz",
            "profile_path",
            "directed_path",
            "inbox_path",
            "forms_path",
            "install_path",
            "spotter_launch_path",
            "commstat_launch_path",
        ),
    )


def _save_fast_light_config_conn(conn: sqlite3.Connection, values: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(values)
    payload.setdefault("flrig_host", "127.0.0.1")
    payload.setdefault("flrig_port", 12345)
    payload.setdefault("fldigi_port", 7362)
    return _save_simple_record(
        conn,
        "fast_light_configs",
        payload,
        default_system_key=DEFAULT_FAST_LIGHT_SYSTEM_KEY,
        default_name=DEFAULT_FAST_LIGHT_NAME,
        fields=(
            "flrig_path",
            "flrig_host",
            "flrig_port",
            "fldigi_path",
            "fldigi_host",
            "fldigi_port",
            "fldigi_log_path",
            "fldigi_checkin_dir",
        ),
    )


def _save_varac_node_conn(conn: sqlite3.Connection, values: Mapping[str, Any]) -> Dict[str, Any]:
    return _save_simple_record(
        conn,
        "varac_nodes",
        values,
        default_system_key=DEFAULT_VARAC_NODE_SYSTEM_KEY,
        default_name=DEFAULT_VARAC_NODE_NAME,
        fields=("install_path", "db_path", "ini_path", "launch_cmd", "incoming_path"),
    )


def _save_operating_profile_conn(conn: sqlite3.Connection, values: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(values)
    record_id = _coerce_optional_int(payload.get("id"))
    existing = _record_by_id(conn, "operating_profiles", record_id) if record_id is not None else None
    now_iso = _utc_now_iso()
    system_key = _next_system_key(
        conn,
        "operating_profiles",
        payload.get("system_key", (existing or {}).get("system_key", DEFAULT_OPERATING_SYSTEM_KEY)),
        exclude_id=record_id,
    )
    scheduler_mode = _coerce_text(payload.get("scheduler_mode", (existing or {}).get("scheduler_mode", "full")), "full").lower() or "full"
    if scheduler_mode not in SUPPORTED_SCHEDULER_MODES:
        scheduler_mode = "full"
    record = {
        "system_key": system_key,
        "name": _coerce_text(payload.get("name", (existing or {}).get("name", DEFAULT_OPERATING_NAME)), DEFAULT_OPERATING_NAME) or DEFAULT_OPERATING_NAME,
        "enabled": _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True),
        "description": _coerce_text(payload.get("description", (existing or {}).get("description", "")), ""),
        "scheduler_enabled": _coerce_bool_int(payload.get("scheduler_enabled", (existing or {}).get("scheduler_enabled", 1)), True),
        "scheduler_mode": scheduler_mode,
        "preferred_band_set_json": _coerce_json_array_text(
            payload.get(
                "preferred_band_set",
                payload.get("preferred_band_set_json", (existing or {}).get("preferred_band_set_json", "[]")),
            )
        ),
        "use_messages": _coerce_bool_int(payload.get("use_messages", (existing or {}).get("use_messages", 1)), True),
        "use_map": _coerce_bool_int(payload.get("use_map", (existing or {}).get("use_map", 1)), True),
        "use_background_ingest": _coerce_bool_int(payload.get("use_background_ingest", (existing or {}).get("use_background_ingest", 1)), True),
        "use_launch_control": _coerce_bool_int(payload.get("use_launch_control", (existing or {}).get("use_launch_control", 1)), True),
        "use_net_control_tabs": _coerce_bool_int(payload.get("use_net_control_tabs", (existing or {}).get("use_net_control_tabs", 1)), True),
        "allow_profile_swap": _coerce_bool_int(
            payload.get("allow_profile_swap", (existing or {}).get("allow_profile_swap", 0)),
            False,
        ),
        "created_utc": (existing or {}).get("created_utc", now_iso),
        "updated_utc": now_iso,
    }
    columns = list(record.keys())
    if existing:
        assignments = ", ".join(f"{name}=?" for name in columns)
        conn.execute(
            f"UPDATE operating_profiles SET {assignments} WHERE id=?",
            [record[name] for name in columns] + [int(record_id)],
        )
        conn.commit()
        return _record_by_id(conn, "operating_profiles", int(record_id)) or {}
    conn.execute(
        f"INSERT INTO operating_profiles ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
        [record[name] for name in columns],
    )
    conn.commit()
    return _record_by_system_key(conn, "operating_profiles", system_key) or {}


def _effective_assignment_for_device(conn: sqlite3.Connection, device_profile_id: int) -> Optional[Dict[str, Any]]:
    if int(device_profile_id or 0) <= 0:
        return None
    placeholders = ", ".join(["?"] * len(EFFECTIVE_ASSIGNMENT_STATES))
    cur = conn.execute(
        f"""
        SELECT *
          FROM operating_profile_assignments
         WHERE device_profile_id=?
           AND assignment_state IN ({placeholders})
      ORDER BY id ASC
         LIMIT 1
        """,
        (int(device_profile_id), *tuple(EFFECTIVE_ASSIGNMENT_STATES)),
    )
    return _fetchone_dict(cur)


def _set_device_operating_profile_conn(
    conn: sqlite3.Connection,
    device_profile_id: int,
    operating_profile_id: int,
    *,
    assignment_state: str = "active",
    reason: str = "",
    created_by: str = "settings_ui",
    starts_utc: Optional[str] = None,
    ends_utc: Optional[str] = None,
    allow_active_swap_edit: bool = False,
) -> Dict[str, Any]:
    desired_state = _normalize_assignment_state(assignment_state, "active")
    if desired_state not in EFFECTIVE_ASSIGNMENT_STATES:
        raise ValueError("Only active or temporary_override assignments can become the effective device assignment.")

    device = _record_by_id(conn, "device_profiles", int(device_profile_id))
    if not device:
        raise KeyError(f"Unknown device profile id: {device_profile_id}")
    operating = _record_by_id(conn, "operating_profiles", int(operating_profile_id))
    if not operating:
        raise KeyError(f"Unknown operating profile id: {operating_profile_id}")
    if int(operating.get("enabled", 1) or 0) != 1:
        raise ValueError("Cannot assign a disabled operating profile.")
    active_swap = _active_profile_swap_policy_conn(conn)
    if active_swap is not None and not allow_active_swap_edit:
        source_id = int(active_swap.get("source_device_id", 0) or 0)
        target_id = int(active_swap.get("target_device_id", 0) or 0)
        if int(device_profile_id) in {source_id, target_id}:
            raise ValueError("Restore the active temporary swap before editing assignments on the swap source/target devices.")

    current = _effective_assignment_for_device(conn, int(device_profile_id))
    now_iso = _utc_now_iso()
    starts_value = _coerce_text(starts_utc, now_iso) or now_iso
    ends_value = _coerce_text(ends_utc, "")
    reason_value = _coerce_text(reason, "")
    created_by_value = _coerce_text(created_by, "settings_ui") or "settings_ui"

    if current:
        current_operating_id = int(current.get("operating_profile_id", 0) or 0)
        current_state = _normalize_assignment_state(current.get("assignment_state", "active"), "active")
        current_reason = _coerce_text(current.get("reason", ""), "")
        current_ends = _coerce_text(current.get("ends_utc", ""), "")
        if (
            current_operating_id == int(operating_profile_id)
            and current_state == desired_state
            and current_reason == reason_value
            and current_ends == ends_value
        ):
            return dict(current)
        if current_operating_id == int(operating_profile_id) and current_state == desired_state:
            conn.execute(
                """
                UPDATE operating_profile_assignments
                   SET reason=?, ends_utc=?, updated_utc=?
                 WHERE id=?
                """,
                (
                    reason_value,
                    ends_value or None,
                    now_iso,
                    int(current.get("id", 0) or 0),
                ),
            )
            conn.commit()
            return _effective_assignment_for_device(conn, int(device_profile_id)) or {}

        conn.execute(
            """
            UPDATE operating_profile_assignments
               SET assignment_state='superseded', ends_utc=?, updated_utc=?
             WHERE id=?
            """,
            (
                _coerce_text(current.get("ends_utc", ""), "") or now_iso,
                now_iso,
                int(current.get("id", 0) or 0),
            ),
        )

    conn.execute(
        """
        INSERT INTO operating_profile_assignments (
            device_profile_id,
            operating_profile_id,
            assignment_state,
            starts_utc,
            ends_utc,
            reason,
            created_by,
            created_utc,
            updated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(device_profile_id),
            int(operating_profile_id),
            desired_state,
            starts_value,
            ends_value or None,
            reason_value,
            created_by_value,
            now_iso,
            now_iso,
        ),
    )
    conn.commit()
    return _effective_assignment_for_device(conn, int(device_profile_id)) or {}


def _restore_default_operating_profile_conn(
    conn: sqlite3.Connection,
    device_profile_id: int,
    *,
    reason: str = "Restored default operating profile.",
    created_by: str = "settings_ui",
    allow_active_swap_edit: bool = False,
) -> Dict[str, Any]:
    operating = _record_by_system_key(conn, "operating_profiles", DEFAULT_OPERATING_SYSTEM_KEY)
    if not operating:
        operating = _save_operating_profile_conn(conn, _seed_operating_defaults(_load_kv_settings(conn)))
    return _set_device_operating_profile_conn(
        conn,
        int(device_profile_id),
        int(operating.get("id", 0) or 0),
        assignment_state="active",
        reason=reason,
        created_by=created_by,
        allow_active_swap_edit=allow_active_swap_edit,
    )


def _restore_assignment_snapshot_conn(
    conn: sqlite3.Connection,
    device_profile_id: int,
    snapshot: Optional[Mapping[str, Any]],
    *,
    fallback_reason: str = "Restored previous operating profile after temporary swap.",
    created_by: str = "settings_ui",
    allow_active_swap_edit: bool = False,
) -> Dict[str, Any]:
    data = dict(snapshot or {})
    operating_profile_id = data.get("operating_profile_id")
    if operating_profile_id in (None, ""):
        return _restore_default_operating_profile_conn(
            conn,
            int(device_profile_id),
            reason=fallback_reason,
            created_by=created_by,
            allow_active_swap_edit=allow_active_swap_edit,
        )
    return _set_device_operating_profile_conn(
        conn,
        int(device_profile_id),
        int(operating_profile_id),
        assignment_state=_normalize_assignment_state(data.get("assignment_state", "active"), "active"),
        reason=_coerce_text(data.get("reason", fallback_reason), fallback_reason),
        created_by=_coerce_text(data.get("created_by", created_by), created_by) or created_by,
        starts_utc=_coerce_text(data.get("starts_utc", ""), "") or None,
        ends_utc=_coerce_text(data.get("ends_utc", ""), "") or None,
        allow_active_swap_edit=allow_active_swap_edit,
    )


def _runtime_primary_device_profile(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT *
          FROM device_profiles
         WHERE runtime_active=1
           AND runtime_primary=1
      ORDER BY display_order ASC, id ASC
         LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return _resolve_device_profile_links_conn(conn, dict(row))


def _runtime_active_device_profiles(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
          FROM device_profiles
         WHERE runtime_active=1
      ORDER BY display_order ASC, id ASC
        """
    ).fetchall()
    return [_resolve_device_profile_links_conn(conn, dict(row)) for row in rows]


def _ensure_default_assignment(conn: sqlite3.Connection, device_id: int, operating_profile_id: int) -> None:
    if _effective_assignment_for_device(conn, int(device_id)):
        return
    row = conn.execute(
        """
        SELECT id
          FROM operating_profile_assignments
         WHERE device_profile_id=?
           AND operating_profile_id=?
           AND assignment_state='active'
         LIMIT 1
        """,
        (int(device_id), int(operating_profile_id)),
    ).fetchone()
    if row:
        return
    now_iso = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO operating_profile_assignments (
            device_profile_id,
            operating_profile_id,
            assignment_state,
            created_utc,
            updated_utc
        ) VALUES (?, ?, 'active', ?, ?)
        """,
        (int(device_id), int(operating_profile_id), now_iso, now_iso),
    )
    conn.commit()


def _ensure_effective_assignment_for_device(conn: sqlite3.Connection, device_id: int) -> Dict[str, Any]:
    assignment = _effective_assignment_for_device(conn, int(device_id))
    if assignment:
        return dict(assignment)
    operating = _record_by_system_key(conn, "operating_profiles", DEFAULT_OPERATING_SYSTEM_KEY)
    if not operating:
        operating = _save_operating_profile_conn(conn, _seed_operating_defaults(_load_kv_settings(conn)))
    _ensure_default_assignment(conn, int(device_id), int(operating.get("id", 0) or 0))
    return _effective_assignment_for_device(conn, int(device_id)) or {}


def _resolve_device_profile_links_conn(conn: sqlite3.Connection, profile: Mapping[str, Any]) -> Dict[str, Any]:
    data = dict(profile)
    js8_instance_id = _coerce_optional_int(data.get("js8_instance_id"))
    if js8_instance_id is not None:
        js8_row = _record_by_id(conn, "js8_instances", int(js8_instance_id))
        if js8_row:
            data["js8_host"] = _coerce_text(js8_row.get("host", ""), "127.0.0.1") or "127.0.0.1"
            data["js8_port"] = _coerce_optional_int(js8_row.get("port"), 2442)
            data["js8_offset_hz"] = _coerce_optional_int(js8_row.get("offset_hz"), 0)
            data["js8_profile_path"] = _coerce_text(js8_row.get("profile_path", ""), "")
            data["js8_directed_path"] = _coerce_text(js8_row.get("directed_path", ""), "")
            data["js8_forms_path"] = _coerce_text(js8_row.get("forms_path", ""), "")
            data["js8_install_path"] = _coerce_text(js8_row.get("install_path", ""), "")
            data["spotter_launch_path"] = _coerce_text(js8_row.get("spotter_launch_path", ""), "")
            data["commstat_launch_path"] = _coerce_text(js8_row.get("commstat_launch_path", ""), "")
            if _coerce_text(data.get("control_backend", ""), "").lower() == "js8call":
                data["launch_path"] = data["js8_install_path"]

    fast_light_config_id = _coerce_optional_int(data.get("fast_light_config_id"))
    if fast_light_config_id is not None:
        fast_light_row = _record_by_id(conn, "fast_light_configs", int(fast_light_config_id))
        if fast_light_row:
            data["flrig_host"] = _coerce_text(fast_light_row.get("flrig_host", ""), "127.0.0.1") or "127.0.0.1"
            data["flrig_port"] = _coerce_optional_int(fast_light_row.get("flrig_port"), 12345)
            data["fldigi_host"] = _coerce_text(
                fast_light_row.get("fldigi_host", ""),
                data.get("flrig_host", "127.0.0.1"),
            ) or _coerce_text(data.get("flrig_host", ""), "127.0.0.1")
            data["fldigi_port"] = _coerce_optional_int(fast_light_row.get("fldigi_port"), 7362)
            data["fldigi_log_path"] = _coerce_text(fast_light_row.get("fldigi_log_path", ""), "")
            data["fldigi_checkin_dir"] = _coerce_text(fast_light_row.get("fldigi_checkin_dir", ""), "")
            data["flrig_path"] = _coerce_text(fast_light_row.get("flrig_path", ""), "")
            data["fldigi_path"] = _coerce_text(fast_light_row.get("fldigi_path", ""), "")
            if _coerce_text(data.get("control_backend", ""), "").lower() == "flrig":
                data["launch_path"] = data["flrig_path"]

    varac_node_id = _coerce_optional_int(data.get("varac_node_id"))
    if varac_node_id is not None:
        varac_row = _record_by_id(conn, "varac_nodes", int(varac_node_id))
        if varac_row:
            if not _coerce_text(data.get("varac_install_path", ""), ""):
                data["varac_install_path"] = _coerce_text(varac_row.get("install_path", ""), "")
            if not _coerce_text(data.get("varac_db_path", ""), ""):
                data["varac_db_path"] = _coerce_text(varac_row.get("db_path", ""), "")
            if not _coerce_text(data.get("varac_ini_path", ""), ""):
                data["varac_ini_path"] = _coerce_text(varac_row.get("ini_path", ""), "")
            if not _coerce_text(data.get("launch_cmd", ""), ""):
                data["launch_cmd"] = _coerce_text(varac_row.get("launch_cmd", ""), "")
            if not _coerce_text(data.get("varac_incoming_path", ""), ""):
                data["varac_incoming_path"] = _coerce_text(varac_row.get("incoming_path", ""), "")

    assignment = _effective_assignment_for_device(conn, _coerce_int(data.get("id"), 0))
    if assignment:
        operating = _record_by_id(conn, "operating_profiles", int(assignment["operating_profile_id"]))
        if operating:
            data["operating_profile_id"] = operating["id"]
            data["scheduler_enabled"] = _coerce_bool_int(operating.get("scheduler_enabled", 1), True)
            data["use_launch_control"] = _coerce_bool_int(operating.get("use_launch_control", 1), True)
    return data


def _legacy_settings_projection_from_device(
    device_profile: Mapping[str, Any],
    existing_settings: Mapping[str, Any],
) -> Dict[str, Any]:
    control_backend = _coerce_text(device_profile.get("control_backend", "manual"), "manual").lower() or "manual"
    use_flrig = bool(_coerce_bool_int(device_profile.get("use_flrig"), control_backend == "flrig"))
    use_fldigi = bool(_coerce_bool_int(device_profile.get("use_fldigi"), False))
    use_flmsg = bool(_coerce_bool_int(device_profile.get("use_flmsg"), False))
    use_flamp = bool(_coerce_bool_int(device_profile.get("use_flamp"), False))
    use_js8call = bool(_coerce_bool_int(device_profile.get("use_js8call"), control_backend == "js8call"))
    use_js8spotter = bool(_coerce_bool_int(device_profile.get("use_js8spotter"), False))
    use_commstat = bool(_coerce_bool_int(device_profile.get("use_commstat"), False))
    use_varac = bool(_coerce_bool_int(device_profile.get("use_varac"), False))
    flrig_host = _coerce_text(device_profile.get("flrig_host", ""), "127.0.0.1") or "127.0.0.1"
    fldigi_host = _coerce_text(device_profile.get("fldigi_host", ""), "") or flrig_host or "127.0.0.1"
    js8_host = _coerce_text(device_profile.get("js8_host", ""), "127.0.0.1") or "127.0.0.1"
    message_paths = dict(existing_settings.get("message_paths", {}) or {})
    flmsg_message_path = _coerce_text(device_profile.get("flmsg_message_path", ""), "")
    if use_flmsg and flmsg_message_path:
        message_paths["flmsg"] = flmsg_message_path
    else:
        message_paths.pop("flmsg", None)
    flamp_message_path = _coerce_text(device_profile.get("flamp_message_path", ""), "")
    if use_flamp and flamp_message_path:
        message_paths["flamp"] = flamp_message_path
    else:
        message_paths.pop("flamp", None)
    varac_incoming = _coerce_text(device_profile.get("varac_incoming_path", ""), "")
    if use_varac and varac_incoming:
        message_paths["varac"] = varac_incoming
    else:
        message_paths.pop("varac", None)
    updates: Dict[str, Any] = {
        "control_via": _legacy_control_via(control_backend),
        "rig_host": _coerce_text(device_profile.get("rig_host", ""), ""),
        "rig_port": _coerce_optional_int(device_profile.get("rig_port"), 4532),
        "flrig_host": flrig_host if use_flrig else "",
        "flrig_port": _coerce_optional_int(device_profile.get("flrig_port"), 12345) if use_flrig else None,
        "fldigi_host": fldigi_host if use_fldigi else "",
        "fldigi_port": _coerce_optional_int(device_profile.get("fldigi_port"), 7362) if use_fldigi else None,
        "fldigi_log_path": _coerce_text(device_profile.get("fldigi_log_path", ""), "") if use_fldigi else "",
        "fldigi_checkin_dir": _coerce_text(device_profile.get("fldigi_checkin_dir", ""), "") if use_fldigi else "",
        "varac_outbox_dir": _coerce_text(device_profile.get("varac_outbox_dir", ""), "") if use_varac else "",
        "varac_bbs_dir": _coerce_text(device_profile.get("varac_bbs_dir", ""), "") if use_varac else "",
        "varac_bbs_archive_dir": _coerce_text(device_profile.get("varac_bbs_archive_dir", ""), "") if use_varac else "",
        "varac_bbs_enabled": bool(_coerce_bool_int(device_profile.get("varac_bbs_enabled", 0), False)) if use_varac else False,
        "varac_bbs_limit_access_enabled": bool(
            _coerce_bool_int(device_profile.get("varac_bbs_limit_access_enabled", 0), False)
        )
        if use_varac
        else False,
        "varac_bbs_allowed_callsigns": _coerce_text(device_profile.get("varac_bbs_allowed_callsigns", ""), "") if use_varac else "",
        "varac_bbs_announce_enabled": bool(
            _coerce_bool_int(device_profile.get("varac_bbs_announce_enabled", 0), False)
        )
        if use_varac
        else False,
        "varac_bbs_auto_archive_enabled": bool(
            _coerce_bool_int(device_profile.get("varac_bbs_auto_archive_enabled", 0), False)
        )
        if use_varac
        else False,
        "varac_bbs_auto_archive_days": _coerce_optional_int(device_profile.get("varac_bbs_auto_archive_days"), 14)
        if use_varac
        else 14,
        "varac_bbs_vault_enabled": bool(
            _coerce_bool_int(device_profile.get("varac_bbs_vault_enabled", 0), False)
        )
        if use_varac
        else False,
        "varac_bbs_vault_managed_root": _coerce_text(device_profile.get("varac_bbs_vault_managed_root", ""), "")
        if use_varac
        else "",
        "varac_bbs_vault_default_location_id": _coerce_text(
            device_profile.get("varac_bbs_vault_default_location_id", ""),
            "",
        )
        if use_varac
        else "",
        "varac_bbs_vault_global_code_policy": _coerce_text(
            device_profile.get("varac_bbs_vault_global_code_policy", ""),
            "",
        )
        if use_varac
        else "",
        "varac_bbs_vault_trigger_mode": _coerce_text(device_profile.get("varac_bbs_vault_trigger_mode", ""), "")
        if use_varac
        else "",
        "varac_bbs_vault_return_mode": _coerce_text(device_profile.get("varac_bbs_vault_return_mode", ""), "")
        if use_varac
        else "",
        "varac_bbs_vault_failed_attempt_limit": _coerce_optional_int(
            device_profile.get("varac_bbs_vault_failed_attempt_limit"),
            3,
        )
        if use_varac
        else 3,
        "varac_bbs_vault_failed_attempt_window_seconds": _coerce_optional_int(
            device_profile.get("varac_bbs_vault_failed_attempt_window_seconds"),
            900,
        )
        if use_varac
        else 900,
        "varac_bbs_vault_cooldown_seconds": _coerce_optional_int(
            device_profile.get("varac_bbs_vault_cooldown_seconds"),
            1800,
        )
        if use_varac
        else 1800,
        "varac_bbs_vault_idle_timeout_seconds": _coerce_optional_int(
            device_profile.get("varac_bbs_vault_idle_timeout_seconds"),
            600,
        )
        if use_varac
        else 600,
        "varac_bbs_vault_flamp_enabled": bool(
            _coerce_bool_int(device_profile.get("varac_bbs_vault_flamp_enabled", 0), False)
        )
        if use_varac
        else False,
        "varac_bbs_vault_flamp_relay_dir": _coerce_text(
            device_profile.get("varac_bbs_vault_flamp_relay_dir", ""),
            "",
        )
        if use_varac
        else "",
        "varac_bbs_vault_flamp_listing_max_age_days": _coerce_optional_int(
            device_profile.get("varac_bbs_vault_flamp_listing_max_age_days"),
            14,
        )
        if use_varac
        else 14,
        "varac_bbs_vault_locations_v1": _parse_json_list(device_profile.get("varac_bbs_vault_locations_v1", "[]"))
        if use_varac
        else [],
        "varac_bbs_vault_runtime_state_v1": _parse_json_object(
            device_profile.get("varac_bbs_vault_runtime_state_v1", "{}")
        )
        if use_varac
        else {},
        "varac_bbs_vault_last_summary": _coerce_text(device_profile.get("varac_bbs_vault_last_summary", ""), "")
        if use_varac
        else "",
        "js8_host": js8_host if use_js8call else "",
        "js8_port": _coerce_optional_int(device_profile.get("js8_port"), 2442) if use_js8call else None,
        "js8_offset_hz": (_coerce_optional_int(device_profile.get("js8_offset_hz"), 0) or 0) if use_js8call else 0,
        "js8_profile_path": _coerce_text(device_profile.get("js8_profile_path", ""), "") if use_js8call else "",
        "js8_directed_path": _coerce_text(device_profile.get("js8_directed_path", ""), "") if use_js8call else "",
        "js8_forms_path": _coerce_text(device_profile.get("js8_forms_path", ""), "") if use_js8call else "",
        "varac_path": _coerce_text(device_profile.get("varac_install_path", ""), "") if use_varac else "",
        "varac_db_path": _coerce_text(device_profile.get("varac_db_path", ""), "") if use_varac else "",
        "varac_ini_path": _coerce_text(device_profile.get("varac_ini_path", ""), "") if use_varac else "",
        "varac_launch_cmd": _coerce_text(device_profile.get("launch_cmd", ""), "") if use_varac else "",
        "message_paths": message_paths,
        "launch_control_enabled": bool(_coerce_bool_int(device_profile.get("launch_enabled", 1), True)),
    }
    flrig_path = _coerce_text(device_profile.get("flrig_path", ""), "")
    if use_flrig and flrig_path:
        updates["path_flrig"] = flrig_path
    elif use_flrig and control_backend == "flrig" and not flrig_path:
        updates["path_flrig"] = _coerce_text(device_profile.get("launch_path", ""), "")
    else:
        updates["path_flrig"] = ""
    fldigi_path = _coerce_text(device_profile.get("fldigi_path", ""), "")
    if use_fldigi and fldigi_path:
        updates["path_fldigi"] = fldigi_path
    else:
        updates["path_fldigi"] = ""
    updates["path_flmsg"] = _coerce_text(device_profile.get("flmsg_path", ""), "") if use_flmsg else ""
    updates["path_flamp"] = _coerce_text(device_profile.get("flamp_path", ""), "") if use_flamp else ""
    js8_install_path = _coerce_text(device_profile.get("js8_install_path", ""), "")
    if use_js8call and js8_install_path:
        updates["path_js8call"] = js8_install_path
    elif use_js8call and control_backend == "js8call" and not js8_install_path:
        updates["path_js8call"] = _coerce_text(device_profile.get("launch_path", ""), "")
    else:
        updates["path_js8call"] = ""
    spotter_launch = _coerce_text(device_profile.get("spotter_launch_path", ""), "")
    if use_js8spotter and spotter_launch:
        updates["path_js8spotter"] = spotter_launch
    else:
        updates["path_js8spotter"] = ""
    commstat_launch = _coerce_text(device_profile.get("commstat_launch_path", ""), "")
    if use_commstat and commstat_launch:
        updates["path_commstat"] = commstat_launch
    else:
        updates["path_commstat"] = ""
    scheduler_enabled = device_profile.get("scheduler_enabled")
    if scheduler_enabled is not None:
        updates["use_scheduler"] = bool(_coerce_bool_int(scheduler_enabled, True))
    return updates


def _normalize_runtime_primary_device(conn: sqlite3.Connection) -> Optional[int]:
    rows = conn.execute(
        """
        SELECT id, enabled, runtime_active, runtime_primary, device_class
          FROM device_profiles
      ORDER BY display_order ASC, id ASC
        """
    ).fetchall()
    if not rows:
        return None
    enabled_rows = [row for row in rows if int(row[1] or 0) == 1]
    candidates = [row for row in enabled_rows if _coerce_text(row[4], "tx_rx").lower() != "observer"]
    if not candidates:
        candidates = enabled_rows or [rows[0]]
    active_candidates = [row for row in candidates if int(row[2] or 0) == 1]
    chosen = next((row for row in active_candidates if int(row[3] or 0) == 1), None)
    if chosen is None and active_candidates:
        chosen = active_candidates[0]
    if chosen is None:
        chosen = candidates[0]
    chosen_id = int(chosen[0])
    active_ids = {int(row[0]) for row in enabled_rows if int(row[2] or 0) == 1}
    if not active_ids:
        active_ids = {chosen_id}
    else:
        active_ids.add(chosen_id)
    conn.executemany(
        "UPDATE device_profiles SET runtime_active=?, runtime_primary=? WHERE id=?",
        [
            (
                1 if int(row[0]) in active_ids else 0,
                1 if int(row[0]) == chosen_id else 0,
                int(row[0]),
            )
            for row in rows
        ],
    )
    conn.commit()
    return chosen_id


def _seed_js8_defaults(settings_values: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "system_key": DEFAULT_JS8_INSTANCE_SYSTEM_KEY,
        "name": DEFAULT_JS8_INSTANCE_NAME,
        "host": _settings_text(settings_values, "js8_host", "127.0.0.1") or "127.0.0.1",
        "port": _settings_int(settings_values, "js8_port", 2442),
        "offset_hz": _settings_int(settings_values, "js8_offset_hz", 0),
        "profile_path": _settings_text(settings_values, "js8_profile_path", ""),
        "directed_path": _settings_text(settings_values, "js8_directed_path", ""),
        "forms_path": _settings_text(settings_values, "js8_forms_path", ""),
        "install_path": _settings_text(settings_values, "path_js8call", ""),
        "spotter_launch_path": _settings_text(settings_values, "path_js8spotter", ""),
        "commstat_launch_path": _settings_text(settings_values, "path_commstat", ""),
    }


def _seed_fast_light_defaults(settings_values: Mapping[str, Any]) -> Dict[str, Any]:
    flrig_host = _settings_text(settings_values, "flrig_host", "127.0.0.1") or "127.0.0.1"
    fldigi_host = _settings_text(settings_values, "fldigi_host", "") or flrig_host or "127.0.0.1"
    return {
        "system_key": DEFAULT_FAST_LIGHT_SYSTEM_KEY,
        "name": DEFAULT_FAST_LIGHT_NAME,
        "flrig_path": _settings_text(settings_values, "path_flrig", ""),
        "flrig_host": flrig_host,
        "flrig_port": _settings_int(settings_values, "flrig_port", 12345),
        "fldigi_path": _settings_text(settings_values, "path_fldigi", ""),
        "fldigi_host": fldigi_host,
        "fldigi_port": _settings_int(settings_values, "fldigi_port", 7362),
        "fldigi_log_path": _settings_text(settings_values, "fldigi_log_path", ""),
        "fldigi_checkin_dir": _settings_text(settings_values, "fldigi_checkin_dir", ""),
    }


def _seed_varac_defaults(settings_values: Mapping[str, Any]) -> Dict[str, Any]:
    message_paths = settings_values.get("message_paths", {}) or {}
    return {
        "system_key": DEFAULT_VARAC_NODE_SYSTEM_KEY,
        "name": DEFAULT_VARAC_NODE_NAME,
        "install_path": _settings_text(settings_values, "varac_path", ""),
        "db_path": _settings_text(settings_values, "varac_db_path", ""),
        "ini_path": _settings_text(settings_values, "varac_ini_path", ""),
        "launch_cmd": _settings_text(settings_values, "varac_launch_cmd", ""),
        "incoming_path": _coerce_text(message_paths.get("varac", ""), ""),
    }


def _seed_operating_defaults(settings_values: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "system_key": DEFAULT_OPERATING_SYSTEM_KEY,
        "name": DEFAULT_OPERATING_NAME,
        "scheduler_enabled": _settings_bool(settings_values, "use_scheduler", True),
        "scheduler_mode": "full",
        "preferred_band_set_json": "[]",
        "use_messages": 1,
        "use_map": 1,
        "use_background_ingest": 1,
        "use_launch_control": _settings_bool(settings_values, "launch_control_enabled", True),
        "use_net_control_tabs": 1,
        "allow_profile_swap": 0,
    }


def _seed_device_defaults(
    settings_values: Mapping[str, Any],
    *,
    js8_instance_id: int,
    fast_light_config_id: int,
    varac_node_id: int,
) -> Dict[str, Any]:
    control_backend = _normalize_control_backend(settings_values)
    flrig_host = _settings_text(settings_values, "flrig_host", "127.0.0.1") or "127.0.0.1"
    fldigi_host = _settings_text(settings_values, "fldigi_host", "") or flrig_host or "127.0.0.1"
    js8_host = _settings_text(settings_values, "js8_host", "127.0.0.1") or "127.0.0.1"
    message_paths = settings_values.get("message_paths", {}) or {}
    launch_path = ""
    if control_backend == "flrig":
        launch_path = _settings_text(settings_values, "path_flrig", "")
    elif control_backend == "js8call":
        launch_path = _settings_text(settings_values, "path_js8call", "")
    return {
        "system_key": DEFAULT_DEVICE_SYSTEM_KEY,
        "name": DEFAULT_DEVICE_NAME,
        "enabled": 1,
        "runtime_active": 1,
        "runtime_primary": 1,
        "display_order": 0,
        "device_class": "tx_rx",
        "deployment_mode": "full",
        "control_backend": control_backend,
        "use_flrig": _coerce_bool_int(
            control_backend == "flrig" or bool(_settings_text(settings_values, "path_flrig", "")),
            False,
        ),
        "use_fldigi": _coerce_bool_int(
            bool(_settings_text(settings_values, "path_fldigi", "") or _settings_text(settings_values, "fldigi_log_path", "")),
            False,
        ),
        "use_flmsg": _coerce_bool_int(bool(_settings_text(settings_values, "path_flmsg", "")), False),
        "use_flamp": _coerce_bool_int(bool(_settings_text(settings_values, "path_flamp", "")), False),
        "use_js8call": _coerce_bool_int(
            control_backend == "js8call" or bool(_settings_text(settings_values, "path_js8call", "")),
            False,
        ),
        "use_js8spotter": _coerce_bool_int(bool(_settings_text(settings_values, "path_js8spotter", "")), False),
        "use_commstat": _coerce_bool_int(bool(_settings_text(settings_values, "path_commstat", "")), False),
        "use_varac": _coerce_bool_int(
            bool(_settings_text(settings_values, "varac_path", "") or _settings_text(settings_values, "varac_launch_cmd", "")),
            False,
        ),
        "rig_host": _settings_text(settings_values, "rig_host", ""),
        "rig_port": _settings_optional_int(settings_values, "rig_port"),
        "flrig_host": flrig_host,
        "flrig_port": _settings_int(settings_values, "flrig_port", 12345),
        "fldigi_host": fldigi_host,
        "fldigi_port": _settings_int(settings_values, "fldigi_port", 7362),
        "fldigi_log_path": _settings_text(settings_values, "fldigi_log_path", ""),
        "fldigi_checkin_dir": _settings_text(settings_values, "fldigi_checkin_dir", ""),
        "flmsg_path": _settings_text(settings_values, "path_flmsg", ""),
        "flmsg_message_path": _coerce_text(message_paths.get("flmsg", ""), ""),
        "flamp_path": _settings_text(settings_values, "path_flamp", ""),
        "flamp_message_path": _coerce_text(message_paths.get("flamp", ""), ""),
        "js8_host": js8_host,
        "js8_port": _settings_int(settings_values, "js8_port", 2442),
        "js8_instance_id": int(js8_instance_id),
        "js8_profile_path": _settings_text(settings_values, "js8_profile_path", ""),
        "js8_directed_path": _settings_text(settings_values, "js8_directed_path", ""),
        "js8_forms_path": _settings_text(settings_values, "js8_forms_path", ""),
        "fast_light_config_id": int(fast_light_config_id),
        "varac_install_path": _settings_text(settings_values, "varac_path", ""),
        "varac_db_path": _settings_text(settings_values, "varac_db_path", ""),
        "varac_ini_path": _settings_text(settings_values, "varac_ini_path", ""),
        "varac_node_id": int(varac_node_id),
        "varac_outbox_dir": _settings_text(settings_values, "varac_outbox_dir", ""),
        "varac_bbs_dir": _settings_text(settings_values, "varac_bbs_dir", ""),
        "varac_bbs_archive_dir": _settings_text(settings_values, "varac_bbs_archive_dir", ""),
        "varac_bbs_enabled": _coerce_bool_int(settings_values.get("varac_bbs_enabled"), False),
        "varac_bbs_limit_access_enabled": _coerce_bool_int(
            settings_values.get("varac_bbs_limit_access_enabled"),
            False,
        ),
        "varac_bbs_allowed_callsigns": _settings_text(settings_values, "varac_bbs_allowed_callsigns", ""),
        "varac_bbs_announce_enabled": _coerce_bool_int(
            settings_values.get("varac_bbs_announce_enabled"),
            False,
        ),
        "varac_bbs_auto_archive_enabled": _coerce_bool_int(
            settings_values.get("varac_bbs_auto_archive_enabled"),
            False,
        ),
        "varac_bbs_auto_archive_days": _settings_int(settings_values, "varac_bbs_auto_archive_days", 14),
        "varac_bbs_vault_enabled": _coerce_bool_int(settings_values.get("varac_bbs_vault_enabled"), False),
        "varac_bbs_vault_managed_root": _settings_text(settings_values, "varac_bbs_vault_managed_root", ""),
        "varac_bbs_vault_default_location_id": _settings_text(
            settings_values,
            "varac_bbs_vault_default_location_id",
            "",
        ),
        "varac_bbs_vault_global_code_policy": _settings_text(
            settings_values,
            "varac_bbs_vault_global_code_policy",
            "",
        ),
        "varac_bbs_vault_trigger_mode": _settings_text(settings_values, "varac_bbs_vault_trigger_mode", ""),
        "varac_bbs_vault_return_mode": _settings_text(settings_values, "varac_bbs_vault_return_mode", ""),
        "varac_bbs_vault_failed_attempt_limit": _settings_int(
            settings_values,
            "varac_bbs_vault_failed_attempt_limit",
            3,
        ),
        "varac_bbs_vault_failed_attempt_window_seconds": _settings_int(
            settings_values,
            "varac_bbs_vault_failed_attempt_window_seconds",
            900,
        ),
        "varac_bbs_vault_cooldown_seconds": _settings_int(
            settings_values,
            "varac_bbs_vault_cooldown_seconds",
            1800,
        ),
        "varac_bbs_vault_idle_timeout_seconds": _settings_int(
            settings_values,
            "varac_bbs_vault_idle_timeout_seconds",
            600,
        ),
        "varac_bbs_vault_flamp_enabled": _coerce_bool_int(
            settings_values.get("varac_bbs_vault_flamp_enabled"),
            False,
        ),
        "varac_bbs_vault_flamp_relay_dir": _settings_text(
            settings_values,
            "varac_bbs_vault_flamp_relay_dir",
            "",
        ),
        "varac_bbs_vault_flamp_listing_max_age_days": _settings_int(
            settings_values,
            "varac_bbs_vault_flamp_listing_max_age_days",
            14,
        ),
        "varac_bbs_vault_locations_v1": _coerce_json_list_text(
            settings_values.get("varac_bbs_vault_locations_v1", [])
        ),
        "varac_bbs_vault_runtime_state_v1": _coerce_json_object_text(
            settings_values.get("varac_bbs_vault_runtime_state_v1", {})
        ),
        "varac_bbs_vault_last_summary": _settings_text(settings_values, "varac_bbs_vault_last_summary", ""),
        "launch_enabled": _coerce_bool_int(settings_values.get("launch_control_enabled"), True),
        "launch_path": launch_path,
        "launch_cmd": _settings_text(settings_values, "varac_launch_cmd", ""),
        "sdr_host": _settings_text(settings_values, "sdr_host", ""),
        "sdr_port": _settings_optional_int(settings_values, "sdr_port"),
    }


def ensure_default_multi_radio_records(conn: sqlite3.Connection, settings_values: Mapping[str, Any]) -> None:
    ensure_multi_radio_settings_schema(conn)
    js8 = _record_by_system_key(conn, "js8_instances", DEFAULT_JS8_INSTANCE_SYSTEM_KEY)
    if not js8:
        js8 = _save_js8_instance_conn(conn, _seed_js8_defaults(settings_values))
    fast_light = _record_by_system_key(conn, "fast_light_configs", DEFAULT_FAST_LIGHT_SYSTEM_KEY)
    if not fast_light:
        fast_light = _save_fast_light_config_conn(conn, _seed_fast_light_defaults(settings_values))
    varac = _record_by_system_key(conn, "varac_nodes", DEFAULT_VARAC_NODE_SYSTEM_KEY)
    if not varac:
        varac = _save_varac_node_conn(conn, _seed_varac_defaults(settings_values))
    operating = _record_by_system_key(conn, "operating_profiles", DEFAULT_OPERATING_SYSTEM_KEY)
    if not operating:
        operating = _save_operating_profile_conn(conn, _seed_operating_defaults(settings_values))
    device = _record_by_system_key(conn, "device_profiles", DEFAULT_DEVICE_SYSTEM_KEY)
    if not device:
        device = MultiRadioStore._save_device_profile_conn(
            conn,
            _seed_device_defaults(
                settings_values,
                js8_instance_id=int(js8["id"]),
                fast_light_config_id=int(fast_light["id"]),
                varac_node_id=int(varac["id"]),
            ),
        )
    else:
        now_iso = _utc_now_iso()
        conn.execute(
            """
            UPDATE device_profiles
               SET js8_instance_id=COALESCE(js8_instance_id, ?),
                   fast_light_config_id=COALESCE(fast_light_config_id, ?),
                   varac_node_id=COALESCE(varac_node_id, ?),
                   updated_utc=?
             WHERE id=?
            """,
            (int(js8["id"]), int(fast_light["id"]), int(varac["id"]), now_iso, int(device["id"])),
        )
        conn.commit()
        device = _record_by_id(conn, "device_profiles", int(device["id"])) or device
    _ensure_default_assignment(conn, int(device["id"]), int(operating["id"]))
    _normalize_runtime_primary_device(conn)
    log.debug("MultiRadioStore: ensured default multi-radio records for the compatibility baseline.")


def project_runtime_active_device_to_legacy_settings(
    conn: sqlite3.Connection,
    device_profile_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    if device_profile_id is None:
        device_profile_id = _normalize_runtime_primary_device(conn)
    if device_profile_id is None:
        return None
    raw = _record_by_id(conn, "device_profiles", int(device_profile_id))
    if not raw:
        return None
    resolved = _resolve_device_profile_links_conn(conn, raw)
    existing_settings = _load_kv_settings(conn)
    updates = _legacy_settings_projection_from_device(resolved, existing_settings)
    _write_kv_settings(conn, updates)
    conn.commit()
    return updates


def mirror_legacy_settings_into_runtime_active_device(
    conn: sqlite3.Connection,
    settings_values: Mapping[str, Any],
    *,
    keys_changed: Optional[Iterable[str]] = None,
) -> Optional[Dict[str, Any]]:
    if keys_changed is not None and MIRRORED_LEGACY_KEYS.isdisjoint({str(key) for key in keys_changed}):
        return None
    ensure_multi_radio_settings_schema(conn)
    active_id = _normalize_runtime_primary_device(conn)
    if active_id is None:
        return None
    existing = _record_by_id(conn, "device_profiles", int(active_id))
    if not existing:
        return None

    control_backend = _normalize_control_backend(settings_values)
    flrig_host = _settings_text(settings_values, "flrig_host", "127.0.0.1") or "127.0.0.1"
    fldigi_host = _settings_text(settings_values, "fldigi_host", "") or flrig_host or "127.0.0.1"
    js8_host = _settings_text(settings_values, "js8_host", "127.0.0.1") or "127.0.0.1"
    launch_path = _coerce_text(existing.get("launch_path", ""), "")
    if control_backend == "flrig":
        launch_path = _settings_text(settings_values, "path_flrig", "")
    elif control_backend == "js8call":
        launch_path = _settings_text(settings_values, "path_js8call", "")
    updates = {
        "control_backend": control_backend,
        "rig_host": _settings_text(settings_values, "rig_host", ""),
        "rig_port": _settings_optional_int(settings_values, "rig_port"),
        "flrig_host": flrig_host,
        "flrig_port": _settings_int(settings_values, "flrig_port", 12345),
        "fldigi_host": fldigi_host,
        "fldigi_port": _settings_int(settings_values, "fldigi_port", 7362),
        "fldigi_log_path": _settings_text(settings_values, "fldigi_log_path", ""),
        "fldigi_checkin_dir": _settings_text(settings_values, "fldigi_checkin_dir", ""),
        "flmsg_path": _settings_text(settings_values, "path_flmsg", ""),
        "flmsg_message_path": _coerce_text((settings_values.get("message_paths", {}) or {}).get("flmsg", ""), ""),
        "flamp_path": _settings_text(settings_values, "path_flamp", ""),
        "flamp_message_path": _coerce_text((settings_values.get("message_paths", {}) or {}).get("flamp", ""), ""),
        "js8_host": js8_host,
        "js8_port": _settings_int(settings_values, "js8_port", 2442),
        "js8_profile_path": _settings_text(settings_values, "js8_profile_path", ""),
        "js8_directed_path": _settings_text(settings_values, "js8_directed_path", ""),
        "js8_forms_path": _settings_text(settings_values, "js8_forms_path", ""),
        "varac_install_path": _settings_text(settings_values, "varac_path", ""),
        "varac_db_path": _settings_text(settings_values, "varac_db_path", ""),
        "varac_ini_path": _settings_text(settings_values, "varac_ini_path", ""),
        "varac_outbox_dir": _settings_text(settings_values, "varac_outbox_dir", ""),
        "varac_bbs_dir": _settings_text(settings_values, "varac_bbs_dir", ""),
        "varac_bbs_archive_dir": _settings_text(settings_values, "varac_bbs_archive_dir", ""),
        "varac_bbs_enabled": _coerce_bool_int(settings_values.get("varac_bbs_enabled"), False),
        "varac_bbs_limit_access_enabled": _coerce_bool_int(
            settings_values.get("varac_bbs_limit_access_enabled"),
            False,
        ),
        "varac_bbs_allowed_callsigns": _settings_text(settings_values, "varac_bbs_allowed_callsigns", ""),
        "varac_bbs_announce_enabled": _coerce_bool_int(
            settings_values.get("varac_bbs_announce_enabled"),
            False,
        ),
        "varac_bbs_auto_archive_enabled": _coerce_bool_int(
            settings_values.get("varac_bbs_auto_archive_enabled"),
            False,
        ),
        "varac_bbs_auto_archive_days": _settings_int(settings_values, "varac_bbs_auto_archive_days", 14),
        "varac_bbs_vault_enabled": _coerce_bool_int(settings_values.get("varac_bbs_vault_enabled"), False),
        "varac_bbs_vault_managed_root": _settings_text(settings_values, "varac_bbs_vault_managed_root", ""),
        "varac_bbs_vault_default_location_id": _settings_text(
            settings_values,
            "varac_bbs_vault_default_location_id",
            "",
        ),
        "varac_bbs_vault_global_code_policy": _settings_text(
            settings_values,
            "varac_bbs_vault_global_code_policy",
            "",
        ),
        "varac_bbs_vault_trigger_mode": _settings_text(settings_values, "varac_bbs_vault_trigger_mode", ""),
        "varac_bbs_vault_return_mode": _settings_text(settings_values, "varac_bbs_vault_return_mode", ""),
        "varac_bbs_vault_failed_attempt_limit": _settings_int(
            settings_values,
            "varac_bbs_vault_failed_attempt_limit",
            3,
        ),
        "varac_bbs_vault_failed_attempt_window_seconds": _settings_int(
            settings_values,
            "varac_bbs_vault_failed_attempt_window_seconds",
            900,
        ),
        "varac_bbs_vault_cooldown_seconds": _settings_int(
            settings_values,
            "varac_bbs_vault_cooldown_seconds",
            1800,
        ),
        "varac_bbs_vault_idle_timeout_seconds": _settings_int(
            settings_values,
            "varac_bbs_vault_idle_timeout_seconds",
            600,
        ),
        "varac_bbs_vault_flamp_enabled": _coerce_bool_int(
            settings_values.get("varac_bbs_vault_flamp_enabled"),
            False,
        ),
        "varac_bbs_vault_flamp_relay_dir": _settings_text(
            settings_values,
            "varac_bbs_vault_flamp_relay_dir",
            "",
        ),
        "varac_bbs_vault_flamp_listing_max_age_days": _settings_int(
            settings_values,
            "varac_bbs_vault_flamp_listing_max_age_days",
            14,
        ),
        "varac_bbs_vault_locations_v1": _coerce_json_list_text(
            settings_values.get("varac_bbs_vault_locations_v1", [])
        ),
        "varac_bbs_vault_runtime_state_v1": _coerce_json_object_text(
            settings_values.get("varac_bbs_vault_runtime_state_v1", {})
        ),
        "varac_bbs_vault_last_summary": _settings_text(settings_values, "varac_bbs_vault_last_summary", ""),
        "launch_enabled": _coerce_bool_int(settings_values.get("launch_control_enabled"), True),
        "launch_path": launch_path,
        "launch_cmd": _settings_text(settings_values, "varac_launch_cmd", ""),
        "updated_utc": _utc_now_iso(),
    }
    assignments = ", ".join(f"{name}=?" for name in updates)
    conn.execute(
        f"UPDATE device_profiles SET {assignments} WHERE id=?",
        [updates[name] for name in updates] + [int(active_id)],
    )

    js8_instance_id = _coerce_optional_int(existing.get("js8_instance_id"))
    if js8_instance_id is not None:
        js8_existing = _record_by_id(conn, "js8_instances", int(js8_instance_id)) or {}
        _save_js8_instance_conn(
            conn,
            {
                "id": int(js8_instance_id),
                "system_key": js8_existing.get("system_key"),
                "name": js8_existing.get("name", DEFAULT_JS8_INSTANCE_NAME),
                "enabled": js8_existing.get("enabled", 1),
                "host": js8_host,
                "port": _settings_int(settings_values, "js8_port", 2442),
                "offset_hz": _settings_int(settings_values, "js8_offset_hz", 0),
                "profile_path": _settings_text(settings_values, "js8_profile_path", ""),
                "directed_path": _settings_text(settings_values, "js8_directed_path", ""),
                "forms_path": _settings_text(settings_values, "js8_forms_path", ""),
                "install_path": _settings_text(settings_values, "path_js8call", ""),
                "spotter_launch_path": _settings_text(settings_values, "path_js8spotter", ""),
                "commstat_launch_path": _settings_text(settings_values, "path_commstat", ""),
            },
        )

    fast_light_config_id = _coerce_optional_int(existing.get("fast_light_config_id"))
    if fast_light_config_id is not None:
        fast_existing = _record_by_id(conn, "fast_light_configs", int(fast_light_config_id)) or {}
        _save_fast_light_config_conn(
            conn,
            {
                "id": int(fast_light_config_id),
                "system_key": fast_existing.get("system_key"),
                "name": fast_existing.get("name", DEFAULT_FAST_LIGHT_NAME),
                "enabled": fast_existing.get("enabled", 1),
                "flrig_path": _settings_text(settings_values, "path_flrig", ""),
                "flrig_host": flrig_host,
                "flrig_port": _settings_int(settings_values, "flrig_port", 12345),
                "fldigi_path": _settings_text(settings_values, "path_fldigi", ""),
                "fldigi_host": fldigi_host,
                "fldigi_port": _settings_int(settings_values, "fldigi_port", 7362),
                "fldigi_log_path": _settings_text(settings_values, "fldigi_log_path", ""),
                "fldigi_checkin_dir": _settings_text(settings_values, "fldigi_checkin_dir", ""),
            },
        )

    varac_node_id = _coerce_optional_int(existing.get("varac_node_id"))
    if varac_node_id is not None:
        varac_existing = _record_by_id(conn, "varac_nodes", int(varac_node_id)) or {}
        message_paths = settings_values.get("message_paths", {}) or {}
        _save_varac_node_conn(
            conn,
            {
                "id": int(varac_node_id),
                "system_key": varac_existing.get("system_key"),
                "name": varac_existing.get("name", DEFAULT_VARAC_NODE_NAME),
                "enabled": varac_existing.get("enabled", 1),
                "install_path": _settings_text(settings_values, "varac_path", ""),
                "db_path": _settings_text(settings_values, "varac_db_path", ""),
                "ini_path": _settings_text(settings_values, "varac_ini_path", ""),
                "launch_cmd": _settings_text(settings_values, "varac_launch_cmd", ""),
                "incoming_path": _coerce_text(message_paths.get("varac", ""), ""),
            },
        )

    assignment = _effective_assignment_for_device(conn, int(active_id))
    if assignment:
        operating = _record_by_id(conn, "operating_profiles", int(assignment["operating_profile_id"])) or {}
        _save_operating_profile_conn(
            conn,
            {
                "id": int(assignment["operating_profile_id"]),
                "system_key": operating.get("system_key"),
                "name": operating.get("name", DEFAULT_OPERATING_NAME),
                "enabled": operating.get("enabled", 1),
                "description": operating.get("description", ""),
                "scheduler_enabled": _settings_bool(settings_values, "use_scheduler", True),
                "scheduler_mode": operating.get("scheduler_mode", "full"),
                "use_messages": operating.get("use_messages", 1),
                "use_map": operating.get("use_map", 1),
                "use_background_ingest": operating.get("use_background_ingest", 1),
                "use_launch_control": _settings_bool(settings_values, "launch_control_enabled", True),
                "use_net_control_tabs": operating.get("use_net_control_tabs", 1),
            },
        )
    conn.commit()
    return _resolve_device_profile_links_conn(conn, _record_by_id(conn, "device_profiles", int(active_id)) or existing)


class MultiRadioStore:
    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = Path(db_path) if db_path else settings_db_path()

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        ensure_multi_radio_settings_schema(conn)
        return conn

    @staticmethod
    def _save_device_profile_conn(conn: sqlite3.Connection, values: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(values)
        requested_id = _coerce_optional_int(payload.get("id"))
        existing = _record_by_id(conn, "device_profiles", requested_id) if requested_id is not None else None
        now_iso = _utc_now_iso()
        system_key = _next_system_key(
            conn,
            "device_profiles",
            payload.get("system_key", (existing or {}).get("system_key", DEFAULT_DEVICE_SYSTEM_KEY)),
            exclude_id=requested_id,
        )
        control_backend = _coerce_text(payload.get("control_backend", (existing or {}).get("control_backend", "flrig")), "flrig").lower()
        if control_backend not in SUPPORTED_DEVICE_CONTROL_BACKENDS:
            raise ValueError(f"Unsupported control backend: {control_backend}")
        device_class = _coerce_text(payload.get("device_class", (existing or {}).get("device_class", "tx_rx")), "tx_rx").lower()
        if device_class not in SUPPORTED_DEVICE_CLASSES:
            raise ValueError(f"Unsupported device class: {device_class}")
        deployment_mode = _coerce_text(payload.get("deployment_mode", (existing or {}).get("deployment_mode", "full")), "full").lower()
        if deployment_mode not in SUPPORTED_DEPLOYMENT_MODES:
            raise ValueError(f"Unsupported deployment mode: {deployment_mode}")
        if existing and int(existing.get("runtime_primary", 0) or 0) == 1 and device_class == "observer":
            raise ValueError("Observer / SDR device profiles cannot become the compatibility runtime device.")
        if device_class == "observer" and requested_id is not None and _device_has_varac_cluster_membership(conn, int(requested_id)):
            raise ValueError("Observer / SDR device profiles cannot participate in VarAC clusters.")

        js8_instance_id = _coerce_optional_int(payload.get("js8_instance_id", (existing or {}).get("js8_instance_id")))
        fast_light_config_id = _coerce_optional_int(
            payload.get("fast_light_config_id", (existing or {}).get("fast_light_config_id"))
        )
        varac_node_id = _coerce_optional_int(payload.get("varac_node_id", (existing or {}).get("varac_node_id")))
        if js8_instance_id is not None and not _record_by_id(conn, "js8_instances", int(js8_instance_id)):
            raise KeyError(f"Unknown JS8 instance id: {js8_instance_id}")
        if fast_light_config_id is not None and not _record_by_id(conn, "fast_light_configs", int(fast_light_config_id)):
            raise KeyError(f"Unknown Fast Light config id: {fast_light_config_id}")
        if varac_node_id is not None and not _record_by_id(conn, "varac_nodes", int(varac_node_id)):
            raise KeyError(f"Unknown VarAC node id: {varac_node_id}")

        flrig_host = _coerce_text(payload.get("flrig_host", (existing or {}).get("flrig_host", "127.0.0.1")), "127.0.0.1") or "127.0.0.1"
        fldigi_host = _coerce_text(payload.get("fldigi_host", (existing or {}).get("fldigi_host", "")), "") or flrig_host or "127.0.0.1"
        js8_host = _coerce_text(payload.get("js8_host", (existing or {}).get("js8_host", "127.0.0.1")), "127.0.0.1") or "127.0.0.1"
        default_use_flrig = (existing or {}).get("use_flrig")
        if default_use_flrig is None:
            default_use_flrig = control_backend == "flrig"
        default_use_fldigi = (existing or {}).get("use_fldigi")
        if default_use_fldigi is None:
            default_use_fldigi = fast_light_config_id is not None or any(
                payload.get(key)
                for key in (
                    "fldigi_path",
                    "fldigi_host",
                    "fldigi_port",
                    "fldigi_log_path",
                    "fldigi_checkin_dir",
                )
            )
        default_use_flmsg = (existing or {}).get("use_flmsg")
        if default_use_flmsg is None:
            default_use_flmsg = bool(payload.get("flmsg_path") or payload.get("flmsg_message_path"))
        default_use_flamp = (existing or {}).get("use_flamp")
        if default_use_flamp is None:
            default_use_flamp = bool(payload.get("flamp_path") or payload.get("flamp_message_path"))
        default_use_js8call = (existing or {}).get("use_js8call")
        if default_use_js8call is None:
            default_use_js8call = (
                control_backend == "js8call"
                or js8_instance_id is not None
                or any(
                    payload.get(key)
                    for key in (
                        "js8_host",
                        "js8_port",
                        "js8_profile_path",
                        "js8_directed_path",
                        "js8_forms_path",
                    )
                )
            )
        default_use_js8spotter = (existing or {}).get("use_js8spotter")
        if default_use_js8spotter is None:
            default_use_js8spotter = js8_instance_id is not None or bool(payload.get("spotter_launch_path"))
        default_use_commstat = (existing or {}).get("use_commstat")
        if default_use_commstat is None:
            default_use_commstat = js8_instance_id is not None or bool(payload.get("commstat_launch_path"))
        default_use_varac = (existing or {}).get("use_varac")
        if default_use_varac is None:
            default_use_varac = varac_node_id is not None
        record = {
            "system_key": system_key,
            "name": _coerce_text(payload.get("name", (existing or {}).get("name", "Device Profile")), "Device Profile") or "Device Profile",
            "radio_catalog_id": _coerce_text(payload.get("radio_catalog_id", (existing or {}).get("radio_catalog_id", "")), ""),
            "radio_manufacturer": _coerce_text(
                payload.get("radio_manufacturer", (existing or {}).get("radio_manufacturer", "")),
                "",
            ),
            "radio_model": _coerce_text(payload.get("radio_model", (existing or {}).get("radio_model", "")), ""),
            "enabled": _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True),
            "runtime_active": _coerce_bool_int(payload.get("runtime_active", (existing or {}).get("runtime_active", 0)), False),
            "runtime_primary": _coerce_bool_int(payload.get("runtime_primary", (existing or {}).get("runtime_primary", 0)), False),
            "display_order": _coerce_int(payload.get("display_order", (existing or {}).get("display_order", 0)), 0),
            "device_class": device_class,
            "deployment_mode": deployment_mode,
            "control_backend": control_backend,
            "use_flrig": _coerce_bool_int(payload.get("use_flrig", default_use_flrig), control_backend == "flrig"),
            "use_fldigi": _coerce_bool_int(payload.get("use_fldigi", default_use_fldigi), False),
            "use_flmsg": _coerce_bool_int(payload.get("use_flmsg", default_use_flmsg), False),
            "use_flamp": _coerce_bool_int(payload.get("use_flamp", default_use_flamp), False),
            "use_js8call": _coerce_bool_int(payload.get("use_js8call", default_use_js8call), control_backend == "js8call"),
            "use_js8spotter": _coerce_bool_int(payload.get("use_js8spotter", default_use_js8spotter), False),
            "use_commstat": _coerce_bool_int(payload.get("use_commstat", default_use_commstat), False),
            "use_varac": _coerce_bool_int(payload.get("use_varac", default_use_varac), False),
            "rig_host": _coerce_text(payload.get("rig_host", (existing or {}).get("rig_host", "")), ""),
            "rig_port": _coerce_optional_int(payload.get("rig_port", (existing or {}).get("rig_port"))),
            "flrig_host": flrig_host,
            "flrig_port": _coerce_optional_int(payload.get("flrig_port", (existing or {}).get("flrig_port")), 12345),
            "fldigi_host": fldigi_host,
            "fldigi_port": _coerce_optional_int(payload.get("fldigi_port", (existing or {}).get("fldigi_port")), 7362),
            "fldigi_log_path": _coerce_text(payload.get("fldigi_log_path", (existing or {}).get("fldigi_log_path", "")), ""),
            "fldigi_checkin_dir": _coerce_text(payload.get("fldigi_checkin_dir", (existing or {}).get("fldigi_checkin_dir", "")), ""),
            "flmsg_path": _coerce_text(payload.get("flmsg_path", (existing or {}).get("flmsg_path", "")), ""),
            "flmsg_message_path": _coerce_text(
                payload.get("flmsg_message_path", (existing or {}).get("flmsg_message_path", "")),
                "",
            ),
            "flamp_path": _coerce_text(payload.get("flamp_path", (existing or {}).get("flamp_path", "")), ""),
            "flamp_message_path": _coerce_text(
                payload.get("flamp_message_path", (existing or {}).get("flamp_message_path", "")),
                "",
            ),
            "js8_host": js8_host,
            "js8_port": _coerce_optional_int(payload.get("js8_port", (existing or {}).get("js8_port")), 2442),
            "js8_instance_id": js8_instance_id,
            "js8_profile_path": _coerce_text(payload.get("js8_profile_path", (existing or {}).get("js8_profile_path", "")), ""),
            "js8_directed_path": _coerce_text(payload.get("js8_directed_path", (existing or {}).get("js8_directed_path", "")), ""),
            "js8_forms_path": _coerce_text(payload.get("js8_forms_path", (existing or {}).get("js8_forms_path", "")), ""),
            "fast_light_config_id": fast_light_config_id,
            "varac_install_path": _coerce_text(payload.get("varac_install_path", (existing or {}).get("varac_install_path", "")), ""),
            "varac_db_path": _coerce_text(payload.get("varac_db_path", (existing or {}).get("varac_db_path", "")), ""),
            "varac_ini_path": _coerce_text(payload.get("varac_ini_path", (existing or {}).get("varac_ini_path", "")), ""),
            "varac_node_id": varac_node_id,
            "varac_outbox_dir": _coerce_text(
                payload.get("varac_outbox_dir", (existing or {}).get("varac_outbox_dir", "")),
                "",
            ),
            "varac_bbs_dir": _coerce_text(payload.get("varac_bbs_dir", (existing or {}).get("varac_bbs_dir", "")), ""),
            "varac_bbs_archive_dir": _coerce_text(
                payload.get("varac_bbs_archive_dir", (existing or {}).get("varac_bbs_archive_dir", "")),
                "",
            ),
            "varac_bbs_enabled": _coerce_bool_int(
                payload.get("varac_bbs_enabled", (existing or {}).get("varac_bbs_enabled", 0)),
                False,
            ),
            "varac_bbs_limit_access_enabled": _coerce_bool_int(
                payload.get(
                    "varac_bbs_limit_access_enabled",
                    (existing or {}).get("varac_bbs_limit_access_enabled", 0),
                ),
                False,
            ),
            "varac_bbs_allowed_callsigns": _coerce_text(
                payload.get(
                    "varac_bbs_allowed_callsigns",
                    (existing or {}).get("varac_bbs_allowed_callsigns", ""),
                ),
                "",
            ),
            "varac_bbs_announce_enabled": _coerce_bool_int(
                payload.get(
                    "varac_bbs_announce_enabled",
                    (existing or {}).get("varac_bbs_announce_enabled", 0),
                ),
                False,
            ),
            "varac_bbs_auto_archive_enabled": _coerce_bool_int(
                payload.get(
                    "varac_bbs_auto_archive_enabled",
                    (existing or {}).get("varac_bbs_auto_archive_enabled", 0),
                ),
                False,
            ),
            "varac_bbs_auto_archive_days": _coerce_optional_int(
                payload.get(
                    "varac_bbs_auto_archive_days",
                    (existing or {}).get("varac_bbs_auto_archive_days", 14),
                ),
                14,
            ),
            "varac_bbs_vault_enabled": _coerce_bool_int(
                payload.get("varac_bbs_vault_enabled", (existing or {}).get("varac_bbs_vault_enabled", 0)),
                False,
            ),
            "varac_bbs_vault_managed_root": _coerce_text(
                payload.get("varac_bbs_vault_managed_root", (existing or {}).get("varac_bbs_vault_managed_root", "")),
                "",
            ),
            "varac_bbs_vault_default_location_id": _coerce_text(
                payload.get(
                    "varac_bbs_vault_default_location_id",
                    (existing or {}).get("varac_bbs_vault_default_location_id", ""),
                ),
                "",
            ),
            "varac_bbs_vault_global_code_policy": _coerce_text(
                payload.get(
                    "varac_bbs_vault_global_code_policy",
                    (existing or {}).get("varac_bbs_vault_global_code_policy", ""),
                ),
                "",
            ),
            "varac_bbs_vault_trigger_mode": _coerce_text(
                payload.get("varac_bbs_vault_trigger_mode", (existing or {}).get("varac_bbs_vault_trigger_mode", "")),
                "",
            ),
            "varac_bbs_vault_return_mode": _coerce_text(
                payload.get("varac_bbs_vault_return_mode", (existing or {}).get("varac_bbs_vault_return_mode", "")),
                "",
            ),
            "varac_bbs_vault_failed_attempt_limit": _coerce_optional_int(
                payload.get(
                    "varac_bbs_vault_failed_attempt_limit",
                    (existing or {}).get("varac_bbs_vault_failed_attempt_limit", 3),
                ),
                3,
            ),
            "varac_bbs_vault_failed_attempt_window_seconds": _coerce_optional_int(
                payload.get(
                    "varac_bbs_vault_failed_attempt_window_seconds",
                    (existing or {}).get("varac_bbs_vault_failed_attempt_window_seconds", 900),
                ),
                900,
            ),
            "varac_bbs_vault_cooldown_seconds": _coerce_optional_int(
                payload.get(
                    "varac_bbs_vault_cooldown_seconds",
                    (existing or {}).get("varac_bbs_vault_cooldown_seconds", 1800),
                ),
                1800,
            ),
            "varac_bbs_vault_idle_timeout_seconds": _coerce_optional_int(
                payload.get(
                    "varac_bbs_vault_idle_timeout_seconds",
                    (existing or {}).get("varac_bbs_vault_idle_timeout_seconds", 600),
                ),
                600,
            ),
            "varac_bbs_vault_flamp_enabled": _coerce_bool_int(
                payload.get(
                    "varac_bbs_vault_flamp_enabled",
                    (existing or {}).get("varac_bbs_vault_flamp_enabled", 0),
                ),
                False,
            ),
            "varac_bbs_vault_flamp_relay_dir": _coerce_text(
                payload.get(
                    "varac_bbs_vault_flamp_relay_dir",
                    (existing or {}).get("varac_bbs_vault_flamp_relay_dir", ""),
                ),
                "",
            ),
            "varac_bbs_vault_flamp_listing_max_age_days": _coerce_optional_int(
                payload.get(
                    "varac_bbs_vault_flamp_listing_max_age_days",
                    (existing or {}).get("varac_bbs_vault_flamp_listing_max_age_days", 14),
                ),
                14,
            ),
            "varac_bbs_vault_locations_v1": _coerce_json_list_text(
                payload.get(
                    "varac_bbs_vault_locations_v1",
                    (existing or {}).get("varac_bbs_vault_locations_v1", "[]"),
                )
            ),
            "varac_bbs_vault_runtime_state_v1": _coerce_json_object_text(
                payload.get(
                    "varac_bbs_vault_runtime_state_v1",
                    (existing or {}).get("varac_bbs_vault_runtime_state_v1", "{}"),
                )
            ),
            "varac_bbs_vault_last_summary": _coerce_text(
                payload.get("varac_bbs_vault_last_summary", (existing or {}).get("varac_bbs_vault_last_summary", "")),
                "",
            ),
            "launch_enabled": _coerce_bool_int(payload.get("launch_enabled", (existing or {}).get("launch_enabled", 1)), True),
            "launch_path": _coerce_text(payload.get("launch_path", (existing or {}).get("launch_path", "")), ""),
            "launch_cmd": _coerce_text(payload.get("launch_cmd", (existing or {}).get("launch_cmd", "")), ""),
            "ptt_group": normalize_ptt_group(payload.get("ptt_group", (existing or {}).get("ptt_group", ""))),
            "antenna_group": normalize_resource_group(payload.get("antenna_group", (existing or {}).get("antenna_group", ""))),
            "frontend_group": normalize_resource_group(payload.get("frontend_group", (existing or {}).get("frontend_group", ""))),
            "amplifier_group": normalize_resource_group(payload.get("amplifier_group", (existing or {}).get("amplifier_group", ""))),
            "sdr_host": _coerce_text(payload.get("sdr_host", (existing or {}).get("sdr_host", "")), ""),
            "sdr_port": _coerce_optional_int(payload.get("sdr_port", (existing or {}).get("sdr_port"))),
            "notes": _coerce_text(payload.get("notes", (existing or {}).get("notes", "")), ""),
            "created_utc": (existing or {}).get("created_utc", now_iso),
            "updated_utc": now_iso,
        }
        columns = list(record.keys())
        if existing:
            assignments = ", ".join(f"{name}=?" for name in columns)
            conn.execute(
                f"UPDATE device_profiles SET {assignments} WHERE id=?",
                [record[name] for name in columns] + [int(requested_id)],
            )
            conn.commit()
            saved = _record_by_id(conn, "device_profiles", int(requested_id)) or {}
        else:
            conn.execute(
                f"INSERT INTO device_profiles ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                [record[name] for name in columns],
            )
            conn.commit()
            saved = _record_by_system_key(conn, "device_profiles", system_key) or {}

        _sync_derived_coordination_policies_conn(conn)
        if _coerce_bool_int(payload.get("runtime_active"), False):
            return MultiRadioStore._set_runtime_active_device_conn(conn, int(saved["id"]))
        _normalize_runtime_primary_device(conn)
        return _resolve_device_profile_links_conn(conn, saved)

    @staticmethod
    def _set_runtime_primary_device_conn(
        conn: sqlite3.Connection,
        device_profile_id: int,
        *,
        deactivate_others: bool = False,
    ) -> Dict[str, Any]:
        device = _record_by_id(conn, "device_profiles", int(device_profile_id))
        if not device:
            raise KeyError(f"Unknown device profile id: {device_profile_id}")
        if _coerce_text(device.get("device_class", "tx_rx"), "tx_rx").lower() == "observer":
            raise ValueError("Observer / SDR device profiles cannot become the compatibility runtime device.")
        backend = _coerce_text(device.get("control_backend", "manual"), "manual").lower()
        if backend not in SUPPORTED_RUNTIME_CONTROL_BACKENDS:
            raise ValueError(f"Cannot activate backend until runtime support exists: {backend}")
        assignment = _effective_assignment_for_device(conn, int(device_profile_id))
        if not assignment:
            operating = _record_by_system_key(conn, "operating_profiles", DEFAULT_OPERATING_SYSTEM_KEY)
            if not operating:
                operating = _save_operating_profile_conn(conn, _seed_operating_defaults(_load_kv_settings(conn)))
            _ensure_default_assignment(conn, int(device_profile_id), int(operating["id"]))
        if deactivate_others:
            conn.execute(
                "UPDATE device_profiles SET runtime_active=CASE WHEN id=? THEN 1 ELSE 0 END, runtime_primary=CASE WHEN id=? THEN 1 ELSE 0 END",
                (int(device_profile_id), int(device_profile_id)),
            )
        else:
            conn.execute(
                "UPDATE device_profiles SET runtime_primary=CASE WHEN id=? THEN 1 ELSE 0 END, runtime_active=CASE WHEN id=? THEN 1 ELSE runtime_active END",
                (int(device_profile_id), int(device_profile_id)),
            )
        conn.commit()
        _normalize_runtime_primary_device(conn)
        project_runtime_active_device_to_legacy_settings(conn, int(device_profile_id))
        return _resolve_device_profile_links_conn(conn, _record_by_id(conn, "device_profiles", int(device_profile_id)) or device)

    @staticmethod
    def _set_runtime_active_device_conn(
        conn: sqlite3.Connection,
        device_profile_id: int,
        *,
        deactivate_others: bool = True,
    ) -> Dict[str, Any]:
        return MultiRadioStore._set_runtime_primary_device_conn(
            conn,
            int(device_profile_id),
            deactivate_others=deactivate_others,
        )

    def get_runtime_active_device_profile(self) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            active_id = _normalize_runtime_primary_device(conn)
            if active_id is None:
                return None
            row = _record_by_id(conn, "device_profiles", int(active_id))
            if not row:
                return None
            return _resolve_device_profile_links_conn(conn, row)

    def get_runtime_primary_device_profile(self) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            _normalize_runtime_primary_device(conn)
            return _runtime_primary_device_profile(conn)

    def list_runtime_active_device_profiles(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            _normalize_runtime_primary_device(conn)
            return _runtime_active_device_profiles(conn)

    def list_device_profiles(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            _normalize_runtime_primary_device(conn)
            rows = conn.execute("SELECT * FROM device_profiles ORDER BY display_order ASC, id ASC").fetchall()
            return [_resolve_device_profile_links_conn(conn, dict(row)) for row in rows]

    def get_device_profile(self, device_profile_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            _normalize_runtime_primary_device(conn)
            row = _record_by_id(conn, "device_profiles", int(device_profile_id))
            if not row:
                return None
            return _resolve_device_profile_links_conn(conn, row)

    def list_operating_profiles(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM operating_profiles ORDER BY id ASC").fetchall()
            return [dict(row) for row in rows]

    def get_operating_profile(self, operating_profile_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            return _record_by_id(conn, "operating_profiles", int(operating_profile_id))

    def save_operating_profile(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(values)
        requested_id = _coerce_optional_int(payload.get("id"))
        with self._connect() as conn:
            existing = _record_by_id(conn, "operating_profiles", int(requested_id)) if requested_id is not None else None
            active_swap = _active_profile_swap_policy_conn(conn)
            enabled = _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True)
            if existing and str(existing.get("system_key", "") or "").strip() == DEFAULT_OPERATING_SYSTEM_KEY and enabled != 1:
                raise ValueError("Cannot disable the default operating profile.")
            if existing and enabled != 1:
                placeholders = ", ".join("?" for _ in EFFECTIVE_ASSIGNMENT_STATES)
                assigned = conn.execute(
                    f"""
                    SELECT id
                      FROM operating_profile_assignments
                     WHERE operating_profile_id=?
                       AND assignment_state IN ({placeholders})
                     LIMIT 1
                    """,
                    (int(requested_id), *tuple(EFFECTIVE_ASSIGNMENT_STATES)),
                ).fetchone()
                if assigned is not None:
                    raise ValueError("Cannot disable an operating profile while it is assigned to a device.")
                if active_swap is not None:
                    restore_target = dict((active_swap.get("action") or {}).get("restore_target_assignment") or {})
                    restore_target_id = restore_target.get("operating_profile_id")
                    if restore_target_id not in (None, "") and int(restore_target_id) == int(requested_id):
                        raise ValueError(
                            "Cannot disable this operating profile while it is captured as the restore target for an active temporary swap."
                        )
            return _save_operating_profile_conn(conn, payload)

    def delete_operating_profile(self, operating_profile_id: int) -> None:
        with self._connect() as conn:
            operating = _record_by_id(conn, "operating_profiles", int(operating_profile_id))
            if not operating:
                raise KeyError(f"Unknown operating profile id: {operating_profile_id}")
            if str(operating.get("system_key", "") or "").strip() == DEFAULT_OPERATING_SYSTEM_KEY:
                raise ValueError("Cannot delete the default operating profile.")
            active_swap = _active_profile_swap_policy_conn(conn)
            if active_swap is not None:
                restore_target = dict((active_swap.get("action") or {}).get("restore_target_assignment") or {})
                restore_target_id = restore_target.get("operating_profile_id")
                if restore_target_id not in (None, "") and int(restore_target_id) == int(operating_profile_id):
                    raise ValueError(
                        "Cannot delete this operating profile while it is captured as the restore target for an active temporary swap."
                    )
            placeholders = ", ".join("?" for _ in EFFECTIVE_ASSIGNMENT_STATES)
            assigned = conn.execute(
                f"""
                SELECT id
                  FROM operating_profile_assignments
                 WHERE operating_profile_id=?
                   AND assignment_state IN ({placeholders})
                 LIMIT 1
                """,
                (int(operating_profile_id), *tuple(EFFECTIVE_ASSIGNMENT_STATES)),
            ).fetchone()
            if assigned is not None:
                raise ValueError("Cannot delete an operating profile while it is assigned to a device.")
            conn.execute("DELETE FROM operating_profile_assignments WHERE operating_profile_id=?", (int(operating_profile_id),))
            conn.execute("DELETE FROM operating_profiles WHERE id=?", (int(operating_profile_id),))
            conn.commit()

    def list_assignments(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM operating_profile_assignments ORDER BY device_profile_id ASC, id ASC"
            ).fetchall()
            return [dict(row) for row in rows]

    def list_effective_assignments(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT id FROM device_profiles ORDER BY display_order ASC, id ASC").fetchall()
            assignments: List[Dict[str, Any]] = []
            for row in rows:
                assignment = _effective_assignment_for_device(conn, int(row[0]))
                if assignment:
                    assignments.append(dict(assignment))
            return assignments

    def get_effective_assignment_for_device(self, device_profile_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            assignment = _effective_assignment_for_device(conn, int(device_profile_id))
            return dict(assignment) if isinstance(assignment, dict) else None

    def set_device_operating_profile(
        self,
        device_profile_id: int,
        operating_profile_id: int,
        *,
        assignment_state: str = "active",
        reason: str = "",
        created_by: str = "settings_ui",
        starts_utc: Optional[str] = None,
        ends_utc: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            return _set_device_operating_profile_conn(
                conn,
                int(device_profile_id),
                int(operating_profile_id),
                assignment_state=assignment_state,
                reason=reason,
                created_by=created_by,
                starts_utc=starts_utc,
                ends_utc=ends_utc,
            )

    def restore_default_operating_profile(
        self,
        device_profile_id: int,
        *,
        reason: str = "Restored default operating profile.",
        created_by: str = "settings_ui",
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            return _restore_default_operating_profile_conn(
                conn,
                int(device_profile_id),
                reason=reason,
                created_by=created_by,
            )

    def get_active_profile_swap(self) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            policy = _active_profile_swap_policy_conn(conn)
            return _enrich_profile_swap_policy(conn, policy)

    def start_temporary_profile_swap(
        self,
        target_device_profile_id: int,
        *,
        mode: str = "use_target_profile",
        reason: str = "",
        created_by: str = "settings_ui",
        ends_utc: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            if _active_profile_swap_policy_conn(conn) is not None:
                raise ValueError("A temporary profile swap is already active. Restore it before starting another swap.")

            source_id = int(_normalize_runtime_primary_device(conn) or 0)
            if source_id <= 0:
                raise ValueError("A primary device profile is required before starting a temporary swap.")
            if int(target_device_profile_id) == source_id:
                raise ValueError("Select a different active device profile as the temporary swap target.")

            source_row = _record_by_id(conn, "device_profiles", source_id)
            target_row = _record_by_id(conn, "device_profiles", int(target_device_profile_id))
            if not source_row:
                raise ValueError("The current primary device profile could not be resolved.")
            if not target_row:
                raise KeyError(f"Unknown target device profile id: {target_device_profile_id}")
            if int(target_row.get("enabled", 1) or 0) != 1:
                raise ValueError("The selected temporary swap target is disabled.")
            if int(target_row.get("runtime_active", 0) or 0) != 1:
                raise ValueError("The selected temporary swap target must already be runtime-active.")
            if _coerce_text(target_row.get("device_class", "tx_rx"), "tx_rx").lower() == "observer":
                raise ValueError("Observer / SDR device profiles cannot be used as temporary-swap targets.")

            source_assignment = _ensure_effective_assignment_for_device(conn, source_id)
            target_assignment = _ensure_effective_assignment_for_device(conn, int(target_device_profile_id))
            if not source_assignment:
                raise ValueError("The current primary device does not have an effective operating profile assignment.")

            mode_value = _normalize_profile_swap_mode(mode, "use_target_profile")
            reason_value = _coerce_text(reason, "")
            if not reason_value:
                reason_value = f"Temporary swap from {str(source_row.get('name', '') or 'primary device').strip()}."
            created_by_value = _coerce_text(created_by, "settings_ui") or "settings_ui"
            ends_value = _coerce_text(ends_utc, "")

            action: Dict[str, Any] = {
                "restore_primary_device_id": source_id,
                "restore_target_assignment": _assignment_snapshot_from_row(target_assignment),
                "target_assignment_changed": False,
                "source_assignment": _assignment_snapshot_from_row(source_assignment),
            }
            if mode_value == "carry_primary_profile":
                source_operating_profile = _record_by_id(
                    conn,
                    "operating_profiles",
                    int(source_assignment.get("operating_profile_id", 0) or 0),
                )
                if not source_operating_profile:
                    raise ValueError("The current primary device does not have a valid operating profile to carry.")
                if int(source_operating_profile.get("allow_profile_swap", 0) or 0) != 1:
                    raise ValueError("The current primary operating profile does not allow profile swap coordination.")
                applied_target = _set_device_operating_profile_conn(
                    conn,
                    int(target_device_profile_id),
                    int(source_operating_profile.get("id", 0) or 0),
                    assignment_state="temporary_override",
                    reason=reason_value,
                    created_by=created_by_value,
                    ends_utc=ends_value or None,
                )
                action["target_assignment_changed"] = True
                action["applied_operating_profile_id"] = int(applied_target.get("operating_profile_id", 0) or 0)
                action["applied_assignment_state"] = str(applied_target.get("assignment_state", "") or "").strip().lower()

            self._set_runtime_primary_device_conn(conn, int(target_device_profile_id), deactivate_others=False)

            now_iso = _utc_now_iso()
            trigger = {
                "mode": mode_value,
                "reason": reason_value,
                "ends_utc": ends_value,
                "created_by": created_by_value,
            }
            conn.execute(
                """
                INSERT INTO station_coordination_policies (
                    name, enabled, policy_type, source_device_id, target_device_id,
                    priority, trigger_json, action_json, safety_mode, created_utc, updated_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"Temporary Swap: {str(source_row.get('name', '') or f'Device {source_id}')} -> {str(target_row.get('name', '') or f'Device {int(target_device_profile_id)}')}",
                    1,
                    PROFILE_SWAP_POLICY_TYPE,
                    source_id,
                    int(target_device_profile_id),
                    PROFILE_SWAP_POLICY_PRIORITY,
                    _coerce_json_object_text(trigger),
                    _coerce_json_object_text(action),
                    "prompt",
                    now_iso,
                    now_iso,
                ),
            )
            conn.commit()
            return _enrich_profile_swap_policy(conn, _active_profile_swap_policy_conn(conn)) or {}

    def restore_temporary_profile_swap(
        self,
        *,
        reason: str = "Restored temporary profile swap.",
        created_by: str = "settings_ui",
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            policy = _active_profile_swap_policy_conn(conn)
            if policy is None:
                raise ValueError("No temporary profile swap is currently active.")

            source_id = int(policy.get("source_device_id", 0) or 0)
            target_id = int(policy.get("target_device_id", 0) or 0)
            source_row = _record_by_id(conn, "device_profiles", source_id)
            if not source_row:
                raise ValueError("Cannot restore the temporary swap because the original primary device no longer exists.")
            if int(source_row.get("enabled", 1) or 0) != 1:
                raise ValueError("Cannot restore the temporary swap while the original primary device profile is disabled.")

            action = dict(policy.get("action") or {})
            created_by_value = _coerce_text(created_by, "settings_ui") or "settings_ui"
            if bool(action.get("target_assignment_changed")):
                _restore_assignment_snapshot_conn(
                    conn,
                    target_id,
                    action.get("restore_target_assignment"),
                    fallback_reason=reason,
                    created_by=created_by_value,
                    allow_active_swap_edit=True,
                )

            self._set_runtime_primary_device_conn(conn, source_id, deactivate_others=False)

            updated_action = dict(action)
            updated_action["restored_utc"] = _utc_now_iso()
            updated_action["restore_reason"] = _coerce_text(reason, "")
            updated_utc = _utc_now_iso()
            conn.execute(
                """
                UPDATE station_coordination_policies
                   SET enabled=0, action_json=?, updated_utc=?
                 WHERE id=?
                """,
                (
                    _coerce_json_object_text(updated_action),
                    updated_utc,
                    int(policy.get("id", 0) or 0),
                ),
            )
            conn.commit()
            restored = dict(policy)
            restored["enabled"] = 0
            restored["action"] = updated_action
            restored["updated_utc"] = updated_utc
            return _enrich_profile_swap_policy(conn, restored) or restored

    def list_js8_instances(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM js8_instances ORDER BY id ASC").fetchall()
            return [dict(row) for row in rows]

    def get_js8_instance(self, js8_instance_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            return _record_by_id(conn, "js8_instances", int(js8_instance_id))

    def save_js8_instance(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        with self._connect() as conn:
            return _save_js8_instance_conn(conn, values)

    def delete_js8_instance(self, js8_instance_id: int) -> None:
        with self._connect() as conn:
            row = _record_by_id(conn, "js8_instances", int(js8_instance_id))
            if not row:
                raise KeyError(f"Unknown JS8 instance id: {js8_instance_id}")
            count = conn.execute(
                "SELECT COUNT(*) FROM device_profiles WHERE js8_instance_id=?",
                (int(js8_instance_id),),
            ).fetchone()[0]
            if int(count or 0) > 0:
                raise ValueError("Cannot delete a JS8 instance that is still assigned.")
            conn.execute("DELETE FROM js8_instances WHERE id=?", (int(js8_instance_id),))
            conn.commit()

    def list_fast_light_configs(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM fast_light_configs ORDER BY id ASC").fetchall()
            return [dict(row) for row in rows]

    def get_fast_light_config(self, fast_light_config_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            return _record_by_id(conn, "fast_light_configs", int(fast_light_config_id))

    def save_fast_light_config(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        with self._connect() as conn:
            return _save_fast_light_config_conn(conn, values)

    def delete_fast_light_config(self, fast_light_config_id: int) -> None:
        with self._connect() as conn:
            row = _record_by_id(conn, "fast_light_configs", int(fast_light_config_id))
            if not row:
                raise KeyError(f"Unknown Fast Light config id: {fast_light_config_id}")
            count = conn.execute(
                "SELECT COUNT(*) FROM device_profiles WHERE fast_light_config_id=?",
                (int(fast_light_config_id),),
            ).fetchone()[0]
            if int(count or 0) > 0:
                raise ValueError("Cannot delete a Fast Light config that is still assigned.")
            conn.execute("DELETE FROM fast_light_configs WHERE id=?", (int(fast_light_config_id),))
            conn.commit()

    def list_varac_nodes(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM varac_nodes ORDER BY id ASC").fetchall()
            return [dict(row) for row in rows]

    def get_varac_node(self, varac_node_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            return _record_by_id(conn, "varac_nodes", int(varac_node_id))

    def save_varac_node(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        with self._connect() as conn:
            return _save_varac_node_conn(conn, values)

    def delete_varac_node(self, varac_node_id: int) -> None:
        with self._connect() as conn:
            row = _record_by_id(conn, "varac_nodes", int(varac_node_id))
            if not row:
                raise KeyError(f"Unknown VarAC node id: {varac_node_id}")
            count = conn.execute(
                "SELECT COUNT(*) FROM device_profiles WHERE varac_node_id=?",
                (int(varac_node_id),),
            ).fetchone()[0]
            if int(count or 0) > 0:
                raise ValueError("Cannot delete a VarAC node that is still assigned.")
            conn.execute("DELETE FROM varac_nodes WHERE id=?", (int(varac_node_id),))
            conn.commit()

    def list_varac_clusters(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            _sync_derived_coordination_policies_conn(conn)
            return _list_varac_clusters_conn(conn)

    def list_varac_cluster_members(
        self,
        *,
        cluster_id: Optional[int] = None,
        device_profile_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            _sync_derived_coordination_policies_conn(conn)
            return _list_varac_cluster_members_conn(
                conn,
                cluster_id=int(cluster_id) if cluster_id is not None else None,
                device_profile_id=int(device_profile_id) if device_profile_id is not None else None,
            )

    def save_varac_cluster(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(values or {})
        requested_id = _coerce_optional_int(payload.get("id"))
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            existing = _varac_cluster_by_id(conn, int(requested_id)) if requested_id is not None else None
            now_iso = _utc_now_iso()
            name_default = _coerce_text((existing or {}).get("name", ""), "") or "VarAC Cluster"
            cluster_id_default = _coerce_text((existing or {}).get("cluster_id", ""), "") or _normalize_varac_cluster_id(name_default)
            name = _coerce_text(payload.get("name", name_default), name_default) or name_default
            cluster_key = _normalize_varac_cluster_id(payload.get("cluster_id", cluster_id_default), _normalize_varac_cluster_id(name))
            shared_db_path = _coerce_text(payload.get("shared_db_path", (existing or {}).get("shared_db_path", "")), "")
            counters_refresh_sec = max(
                5,
                min(
                    600,
                    _coerce_int(payload.get("counters_refresh_sec", (existing or {}).get("counters_refresh_sec", 30)), 30),
                ),
            )
            ptt_lock_enabled = _coerce_bool_int(
                payload.get("ptt_lock_enabled", (existing or {}).get("ptt_lock_enabled", 0)),
                False,
            )
            duplicate_cluster = conn.execute(
                """
                SELECT id
                  FROM varac_clusters
                 WHERE UPPER(cluster_id)=?
                   AND (? IS NULL OR id<>?)
                 LIMIT 1
                """,
                (cluster_key, requested_id, requested_id),
            ).fetchone()
            if duplicate_cluster is not None:
                raise ValueError(f"VarAC cluster ID {cluster_key} is already in use.")
            gateway_handler_device_id = _coerce_optional_int(
                payload.get("gateway_handler_device_id", (existing or {}).get("gateway_handler_device_id"))
            )
            if requested_id is None and gateway_handler_device_id is not None:
                raise ValueError("Assign cluster members before selecting a VarAC gateway handler.")

            row_id = int(requested_id) if requested_id is not None else 0
            if gateway_handler_device_id is not None and row_id > 0:
                membership = _varac_cluster_membership_row(conn, row_id, int(gateway_handler_device_id))
                if membership is None or int(membership.get("enabled", 1) or 0) != 1:
                    raise ValueError("The VarAC gateway handler must be an enabled member of this cluster.")

            if existing:
                conn.execute(
                    """
                    UPDATE varac_clusters
                       SET name=?, cluster_id=?, shared_db_path=?, counters_refresh_sec=?,
                           ptt_lock_enabled=?, gateway_handler_device_id=?, updated_utc=?
                     WHERE id=?
                    """,
                    (
                        name,
                        cluster_key,
                        shared_db_path or None,
                        counters_refresh_sec,
                        ptt_lock_enabled,
                        gateway_handler_device_id,
                        now_iso,
                        int(requested_id),
                    ),
                )
                row_id = int(requested_id)
            else:
                conn.execute(
                    """
                    INSERT INTO varac_clusters (
                        name, cluster_id, shared_db_path, counters_refresh_sec,
                        ptt_lock_enabled, gateway_handler_device_id, created_utc, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        cluster_key,
                        shared_db_path or None,
                        counters_refresh_sec,
                        ptt_lock_enabled,
                        None,
                        now_iso,
                        now_iso,
                    ),
                )
                row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            conn.commit()
            _sync_derived_coordination_policies_conn(conn)
            cluster = next((row for row in _list_varac_clusters_conn(conn) if int(row.get("id", 0) or 0) == row_id), None)
            return dict(cluster or {})

    def delete_varac_cluster(self, cluster_id: int) -> None:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            cluster = _varac_cluster_by_id(conn, int(cluster_id))
            if not cluster:
                raise KeyError(f"Unknown VarAC cluster id: {cluster_id}")
            conn.execute("DELETE FROM varac_cluster_members WHERE cluster_id=?", (int(cluster_id),))
            conn.execute("DELETE FROM varac_clusters WHERE id=?", (int(cluster_id),))
            conn.commit()
            _sync_derived_coordination_policies_conn(conn)

    def set_varac_cluster_member(
        self,
        cluster_id: int,
        device_profile_id: int,
        *,
        instance_number: int,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            cluster = _varac_cluster_by_id(conn, int(cluster_id))
            if not cluster:
                raise KeyError(f"Unknown VarAC cluster id: {cluster_id}")
            device = _record_by_id(conn, "device_profiles", int(device_profile_id))
            if not device:
                raise KeyError(f"Unknown device profile id: {device_profile_id}")
            if _is_observer_device_class(device):
                raise ValueError("Observer / SDR device profiles cannot participate in VarAC clusters.")
            instance_value = _coerce_int(instance_number, 0)
            if instance_value <= 0:
                raise ValueError("VarAC cluster instance number must be a positive integer.")
            enabled_value = _coerce_bool_int(enabled, True)
            gateway_id = (
                int(cluster.get("gateway_handler_device_id", 0) or 0)
                if cluster.get("gateway_handler_device_id") not in (None, "")
                else 0
            )
            existing = _varac_cluster_membership_row(conn, int(cluster_id), int(device_profile_id))
            if enabled_value == 1:
                other_membership = _varac_enabled_membership_for_device(
                    conn,
                    int(device_profile_id),
                    exclude_cluster_id=int(cluster_id),
                )
                if other_membership is not None:
                    other_cluster = _varac_cluster_by_id(conn, int(other_membership.get("cluster_id", 0) or 0))
                    raise ValueError(
                        "Each device profile may have only one enabled VarAC cluster membership in this phase"
                        + (
                            f" ({str((other_cluster or {}).get('name', '') or '').strip()})."
                            if other_cluster
                            else "."
                        )
                    )
            if existing is not None and enabled_value != 1 and gateway_id == int(device_profile_id):
                raise ValueError("Clear or reassign the VarAC gateway handler before disabling this membership.")

            duplicate = conn.execute(
                """
                SELECT device_profile_id
                  FROM varac_cluster_members
                 WHERE cluster_id=?
                   AND instance_number=?
                   AND device_profile_id<>?
                   AND enabled=1
                 LIMIT 1
                """,
                (int(cluster_id), instance_value, int(device_profile_id)),
            ).fetchone()
            if enabled_value == 1 and duplicate is not None:
                duplicate_id = int(duplicate[0] or 0)
                duplicate_device = _record_by_id(conn, "device_profiles", duplicate_id)
                duplicate_name = _coerce_text((duplicate_device or {}).get("name", f"Device {duplicate_id}"), f"Device {duplicate_id}")
                raise ValueError(
                    f"VarAC instance number {instance_value} is already assigned to {duplicate_name} in this cluster."
                )

            now_iso = _utc_now_iso()
            if existing is None:
                try:
                    conn.execute(
                        """
                        INSERT INTO varac_cluster_members (
                            cluster_id, device_profile_id, instance_number, enabled, created_utc, updated_utc
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(cluster_id),
                            int(device_profile_id),
                            instance_value,
                            enabled_value,
                            now_iso,
                            now_iso,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError("Unable to save the VarAC cluster membership. Check cluster and instance uniqueness.") from exc
            else:
                try:
                    conn.execute(
                        """
                        UPDATE varac_cluster_members
                           SET instance_number=?, enabled=?, updated_utc=?
                         WHERE cluster_id=? AND device_profile_id=?
                        """,
                        (
                            instance_value,
                            enabled_value,
                            now_iso,
                            int(cluster_id),
                            int(device_profile_id),
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError("Unable to update the VarAC cluster membership. Check cluster and instance uniqueness.") from exc
            conn.commit()
            _sync_derived_coordination_policies_conn(conn)
            rows = _list_varac_cluster_members_conn(conn, cluster_id=int(cluster_id), device_profile_id=int(device_profile_id))
            return dict(rows[0]) if rows else {}

    def remove_varac_cluster_member(self, cluster_id: int, device_profile_id: int) -> None:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            cluster = _varac_cluster_by_id(conn, int(cluster_id))
            if not cluster:
                raise KeyError(f"Unknown VarAC cluster id: {cluster_id}")
            membership = _varac_cluster_membership_row(conn, int(cluster_id), int(device_profile_id))
            if membership is None:
                raise KeyError(f"Unknown VarAC cluster membership: cluster={cluster_id}, device={device_profile_id}")
            gateway_id = (
                int(cluster.get("gateway_handler_device_id", 0) or 0)
                if cluster.get("gateway_handler_device_id") not in (None, "")
                else 0
            )
            if gateway_id == int(device_profile_id):
                raise ValueError("Clear or reassign the VarAC gateway handler before removing this membership.")
            conn.execute(
                "DELETE FROM varac_cluster_members WHERE cluster_id=? AND device_profile_id=?",
                (int(cluster_id), int(device_profile_id)),
            )
            conn.commit()
            _sync_derived_coordination_policies_conn(conn)

    def set_varac_cluster_gateway_handler(
        self,
        cluster_id: int,
        gateway_handler_device_id: Optional[int],
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            cluster = _varac_cluster_by_id(conn, int(cluster_id))
            if not cluster:
                raise KeyError(f"Unknown VarAC cluster id: {cluster_id}")
            gateway_value = _coerce_optional_int(gateway_handler_device_id)
            if gateway_value is not None:
                membership = _varac_cluster_membership_row(conn, int(cluster_id), int(gateway_value))
                if membership is None or int(membership.get("enabled", 1) or 0) != 1:
                    raise ValueError("The VarAC gateway handler must be an enabled member of this cluster.")
            conn.execute(
                "UPDATE varac_clusters SET gateway_handler_device_id=?, updated_utc=? WHERE id=?",
                (gateway_value, _utc_now_iso(), int(cluster_id)),
            )
            conn.commit()
            _sync_derived_coordination_policies_conn(conn)
            cluster_row = next((row for row in _list_varac_clusters_conn(conn) if int(row.get("id", 0) or 0) == int(cluster_id)), None)
            return dict(cluster_row or {})

    def save_device_profile(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        with self._connect() as conn:
            return self._save_device_profile_conn(conn, values)

    def delete_device_profile(self, device_profile_id: int) -> None:
        with self._connect() as conn:
            device = _record_by_id(conn, "device_profiles", int(device_profile_id))
            if not device:
                raise KeyError(f"Unknown device profile id: {device_profile_id}")
            if int(device.get("runtime_active", 0) or 0) == 1:
                raise ValueError("Cannot delete a runtime-active device profile. Deactivate it first.")
            conn.execute(
                "UPDATE varac_clusters SET gateway_handler_device_id=NULL WHERE gateway_handler_device_id=?",
                (int(device_profile_id),),
            )
            conn.execute("DELETE FROM operating_profile_assignments WHERE device_profile_id=?", (int(device_profile_id),))
            conn.execute("DELETE FROM varac_cluster_members WHERE device_profile_id=?", (int(device_profile_id),))
            conn.execute(
                "DELETE FROM station_coordination_policies WHERE source_device_id=? OR target_device_id=?",
                (int(device_profile_id), int(device_profile_id)),
            )
            conn.execute("DELETE FROM device_profiles WHERE id=?", (int(device_profile_id),))
            conn.commit()
            _sync_derived_coordination_policies_conn(conn)
            _normalize_runtime_primary_device(conn)

    def set_runtime_active_device_profile(self, device_profile_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            active_swap = _active_profile_swap_policy_conn(conn)
            if active_swap is not None:
                current_target_id = int(active_swap.get("target_device_id", 0) or 0)
                if int(device_profile_id) != current_target_id:
                    raise ValueError("Restore the active temporary swap before changing the primary device profile.")
            return self._set_runtime_active_device_conn(conn, int(device_profile_id), deactivate_others=True)

    def set_runtime_primary_device_profile(self, device_profile_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            active_swap = _active_profile_swap_policy_conn(conn)
            if active_swap is not None:
                current_target_id = int(active_swap.get("target_device_id", 0) or 0)
                if int(device_profile_id) != current_target_id:
                    raise ValueError("Restore the active temporary swap before changing the primary device profile.")
            return self._set_runtime_primary_device_conn(conn, int(device_profile_id), deactivate_others=False)

    def sync_shared_ptt_policies(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            return _sync_shared_ptt_policies_conn(conn)

    def sync_rf_conflict_policies(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            return _sync_rf_conflict_policies_conn(conn)

    def list_station_coordination_policies(self, policy_type: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            _sync_derived_coordination_policies_conn(conn)
            params: List[Any] = []
            where = ""
            normalized_type = _coerce_text(policy_type, "").lower()
            if normalized_type:
                where = "WHERE policy_type=?"
                params.append(normalized_type)
            rows = conn.execute(
                f"""
                SELECT *
                  FROM station_coordination_policies
                  {where}
              ORDER BY priority ASC, policy_type ASC, source_device_id ASC, target_device_id ASC, id ASC
                """,
                tuple(params),
            ).fetchall()
            return [_coordination_policy_from_row(row) for row in rows]

    def set_device_profile_runtime_active(self, device_profile_id: int, active: bool) -> Dict[str, Any]:
        with self._connect() as conn:
            device = _record_by_id(conn, "device_profiles", int(device_profile_id))
            if not device:
                raise KeyError(f"Unknown device profile id: {device_profile_id}")
            active_swap = _active_profile_swap_policy_conn(conn)
            if active_swap is not None:
                source_id = int(active_swap.get("source_device_id", 0) or 0)
                target_id = int(active_swap.get("target_device_id", 0) or 0)
                if int(device_profile_id) in {source_id, target_id}:
                    raise ValueError("Restore the active temporary swap before changing runtime activation on the swap source/target devices.")
            if active:
                backend = _coerce_text(device.get("control_backend", "manual"), "manual").lower()
                if backend not in SUPPORTED_RUNTIME_CONTROL_BACKENDS:
                    raise ValueError(f"Cannot activate backend until runtime support exists: {backend}")
                conn.execute("UPDATE device_profiles SET runtime_active=1 WHERE id=?", (int(device_profile_id),))
                assignment = _effective_assignment_for_device(conn, int(device_profile_id))
                if not assignment:
                    operating = _record_by_system_key(conn, "operating_profiles", DEFAULT_OPERATING_SYSTEM_KEY)
                    if not operating:
                        operating = _save_operating_profile_conn(conn, _seed_operating_defaults(_load_kv_settings(conn)))
                    _ensure_default_assignment(conn, int(device_profile_id), int(operating["id"]))
                conn.commit()
                primary_id = _normalize_runtime_primary_device(conn)
                if primary_id == int(device_profile_id):
                    project_runtime_active_device_to_legacy_settings(conn, int(device_profile_id))
                return _resolve_device_profile_links_conn(conn, _record_by_id(conn, "device_profiles", int(device_profile_id)) or device)

            if int(device.get("runtime_primary", 0) or 0) == 1:
                raise ValueError("Cannot deactivate the primary runtime device profile. Make another device primary first.")

            active_profiles = _runtime_active_device_profiles(conn)
            if len(active_profiles) <= 1:
                raise ValueError("At least one runtime-active device profile must remain enabled.")

            conn.execute("UPDATE device_profiles SET runtime_active=0, runtime_primary=0 WHERE id=?", (int(device_profile_id),))
            conn.commit()
            primary_id = _normalize_runtime_primary_device(conn)
            if primary_id is not None:
                project_runtime_active_device_to_legacy_settings(conn, int(primary_id))
            return _resolve_device_profile_links_conn(conn, _record_by_id(conn, "device_profiles", int(device_profile_id)) or device)

    def sync_runtime_active_device_to_legacy_settings(self, device_profile_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            return project_runtime_active_device_to_legacy_settings(conn, int(device_profile_id)) or {}
