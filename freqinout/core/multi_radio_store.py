from __future__ import annotations

import datetime
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

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
SUPPORTED_RUNTIME_CONTROL_BACKENDS = frozenset({"flrig", "js8call", "manual", "rigctld"})
SUPPORTED_DEVICE_CLASSES = frozenset({"tx_rx", "observer", "gateway"})
EFFECTIVE_ASSIGNMENT_STATES = frozenset({"active", "temporary_override"})
SUPPORTED_ASSIGNMENT_STATES = frozenset({"active", "temporary_override", "scheduled", "inactive", "superseded", "expired"})
SUPPORTED_SCHEDULER_MODES = frozenset({"full", "simple"})
SUPPORTED_COORDINATION_POLICY_TYPES = frozenset(
    {"rf_conflict", "shared_ptt", "profile_swap", "sdr_follow", "observer_park", "gateway_exclusive"}
)
SUPPORTED_COORDINATION_SAFETY_MODES = frozenset({"warn", "prompt", "auto"})
RF_CONFLICT_POLICY_TYPE = "rf_conflict"
RF_CONFLICT_POLICY_PRIORITY = 30
SDR_FOLLOW_POLICY_TYPE = "sdr_follow"
SDR_FOLLOW_POLICY_PRIORITY = 60
GATEWAY_EXCLUSIVE_POLICY_TYPE = "gateway_exclusive"
GATEWAY_EXCLUSIVE_POLICY_PRIORITY = 70
PROFILE_SWAP_POLICY_TYPE = "profile_swap"
PROFILE_SWAP_POLICY_PRIORITY = 40
SHARED_PTT_POLICY_TYPE = "shared_ptt"
SHARED_PTT_POLICY_PRIORITY = 20
SUPPORTED_PROFILE_SWAP_MODES = frozenset({"use_target_profile", "carry_primary_profile"})


SETTINGS_TABLE_SPECS: Dict[str, Dict[str, object]] = {
    "device_profiles": {
        "description": "Multi-radio device profiles (control endpoints, deployment mode, and shared RF resource metadata).",
        "ddl": """
        CREATE TABLE IF NOT EXISTS device_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_key TEXT UNIQUE,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            runtime_active INTEGER NOT NULL DEFAULT 0,
            runtime_primary INTEGER NOT NULL DEFAULT 0,
            display_order INTEGER NOT NULL DEFAULT 0,
            device_class TEXT NOT NULL DEFAULT 'tx_rx',
            deployment_mode TEXT NOT NULL DEFAULT 'full',
            control_backend TEXT NOT NULL DEFAULT 'flrig',
            rig_host TEXT,
            rig_port INTEGER,
            flrig_host TEXT,
            flrig_port INTEGER,
            fldigi_host TEXT,
            fldigi_port INTEGER,
            fldigi_log_path TEXT,
            js8_host TEXT,
            js8_port INTEGER,
            js8_instance_id INTEGER,
            js8_profile_path TEXT,
            js8_directed_path TEXT,
            js8_inbox_path TEXT,
            fast_light_config_id INTEGER,
            varac_install_path TEXT,
            varac_db_path TEXT,
            varac_ini_path TEXT,
            varac_node_id INTEGER,
            varac_cluster_member_enabled INTEGER DEFAULT 0,
            sdr_host TEXT,
            sdr_port INTEGER,
            launch_enabled INTEGER DEFAULT 1,
            launch_path TEXT,
            launch_cmd TEXT,
            working_dir TEXT,
            ptt_group TEXT,
            antenna_group TEXT,
            frontend_group TEXT,
            amplifier_group TEXT,
            notes TEXT,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "system_key": "TEXT",
            "name": "TEXT",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "runtime_active": "INTEGER NOT NULL DEFAULT 0",
            "runtime_primary": "INTEGER NOT NULL DEFAULT 0",
            "display_order": "INTEGER NOT NULL DEFAULT 0",
            "device_class": "TEXT NOT NULL DEFAULT 'tx_rx'",
            "deployment_mode": "TEXT NOT NULL DEFAULT 'full'",
            "control_backend": "TEXT NOT NULL DEFAULT 'flrig'",
            "rig_host": "TEXT",
            "rig_port": "INTEGER",
            "flrig_host": "TEXT",
            "flrig_port": "INTEGER",
            "fldigi_host": "TEXT",
            "fldigi_port": "INTEGER",
            "fldigi_log_path": "TEXT",
            "js8_host": "TEXT",
            "js8_port": "INTEGER",
            "js8_instance_id": "INTEGER",
            "js8_profile_path": "TEXT",
            "js8_directed_path": "TEXT",
            "js8_inbox_path": "TEXT",
            "fast_light_config_id": "INTEGER",
            "varac_install_path": "TEXT",
            "varac_db_path": "TEXT",
            "varac_ini_path": "TEXT",
            "varac_node_id": "INTEGER",
            "varac_cluster_member_enabled": "INTEGER DEFAULT 0",
            "sdr_host": "TEXT",
            "sdr_port": "INTEGER",
            "launch_enabled": "INTEGER DEFAULT 1",
            "launch_path": "TEXT",
            "launch_cmd": "TEXT",
            "working_dir": "TEXT",
            "ptt_group": "TEXT",
            "antenna_group": "TEXT",
            "frontend_group": "TEXT",
            "amplifier_group": "TEXT",
            "notes": "TEXT",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_device_profiles_system_key ON device_profiles(system_key)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_order ON device_profiles(display_order)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_enabled ON device_profiles(enabled)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_runtime_active ON device_profiles(runtime_active)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_runtime_primary ON device_profiles(runtime_primary)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_js8_instance ON device_profiles(js8_instance_id)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_fast_light_config ON device_profiles(fast_light_config_id)",
            "CREATE INDEX IF NOT EXISTS idx_device_profiles_varac_node ON device_profiles(varac_node_id)",
        ),
    },
    "js8_instances": {
        "description": "Reusable JS8Call instance records (endpoint, file paths, and optional companion launch paths).",
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
            "CREATE INDEX IF NOT EXISTS idx_js8_instances_enabled ON js8_instances(enabled)",
        ),
    },
    "fast_light_configs": {
        "description": "Reusable Fast Light rig-control configs (FLRig/FLDigi endpoints, paths, and log/check-in paths).",
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
            "CREATE INDEX IF NOT EXISTS idx_fast_light_configs_enabled ON fast_light_configs(enabled)",
        ),
    },
    "varac_nodes": {
        "description": "Reusable VarAC node records (install/database/config paths and optional incoming directory).",
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
            "CREATE INDEX IF NOT EXISTS idx_varac_nodes_enabled ON varac_nodes(enabled)",
        ),
    },
    "operating_profiles": {
        "description": "Multi-radio operating profiles (scheduler behavior, deployment flags, and automation policy defaults).",
        "ddl": """
        CREATE TABLE IF NOT EXISTS operating_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_key TEXT UNIQUE,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            description TEXT,
            scheduler_enabled INTEGER NOT NULL DEFAULT 1,
            scheduler_mode TEXT NOT NULL DEFAULT 'full',
            preferred_antenna_group TEXT,
            preferred_band_set_json TEXT NOT NULL DEFAULT '[]',
            preferred_mode_set_json TEXT NOT NULL DEFAULT '[]',
            allow_auto_qsy INTEGER NOT NULL DEFAULT 0,
            allow_auto_band_change INTEGER NOT NULL DEFAULT 0,
            allow_profile_swap INTEGER NOT NULL DEFAULT 0,
            prompt_only INTEGER NOT NULL DEFAULT 1,
            use_messages INTEGER NOT NULL DEFAULT 1,
            use_map INTEGER NOT NULL DEFAULT 1,
            use_background_ingest INTEGER NOT NULL DEFAULT 1,
            use_launch_control INTEGER NOT NULL DEFAULT 1,
            use_net_control_tabs INTEGER NOT NULL DEFAULT 1,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """,
        "columns": {
            "system_key": "TEXT",
            "name": "TEXT",
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "description": "TEXT",
            "scheduler_enabled": "INTEGER NOT NULL DEFAULT 1",
            "scheduler_mode": "TEXT NOT NULL DEFAULT 'full'",
            "preferred_antenna_group": "TEXT",
            "preferred_band_set_json": "TEXT NOT NULL DEFAULT '[]'",
            "preferred_mode_set_json": "TEXT NOT NULL DEFAULT '[]'",
            "allow_auto_qsy": "INTEGER NOT NULL DEFAULT 0",
            "allow_auto_band_change": "INTEGER NOT NULL DEFAULT 0",
            "allow_profile_swap": "INTEGER NOT NULL DEFAULT 0",
            "prompt_only": "INTEGER NOT NULL DEFAULT 1",
            "use_messages": "INTEGER NOT NULL DEFAULT 1",
            "use_map": "INTEGER NOT NULL DEFAULT 1",
            "use_background_ingest": "INTEGER NOT NULL DEFAULT 1",
            "use_launch_control": "INTEGER NOT NULL DEFAULT 1",
            "use_net_control_tabs": "INTEGER NOT NULL DEFAULT 1",
            "created_utc": "TEXT NOT NULL",
            "updated_utc": "TEXT NOT NULL",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_operating_profiles_system_key ON operating_profiles(system_key)",
            "CREATE INDEX IF NOT EXISTS idx_operating_profiles_enabled ON operating_profiles(enabled)",
        ),
    },
    "operating_profile_assignments": {
        "description": "Assignments connecting operating profiles to device profiles, including temporary overrides and schedule-ready active state.",
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
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_assignments_active_pair ON operating_profile_assignments(device_profile_id, operating_profile_id, assignment_state)",
        ),
    },
    "station_coordination_policies": {
        "description": "Cross-device station coordination rules (RF conflicts, shared PTT, swaps, SDR follow, and safety mode).",
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
        "description": "Shared VarAC cluster definitions, including cluster ID, shared DB path, and designated gateway handler device.",
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
        "description": "Mappings between VarAC clusters and device profiles with unique instance numbering per cluster.",
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


def _coerce_bool_int(value: Any, default: bool) -> int:
    if value in (None, ""):
        return 1 if bool(default) else 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if int(value) != 0 else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "yes", "on"} else 0
    return 1 if bool(default) else 0


def _coerce_json_string_list(value: Any) -> str:
    if value in (None, ""):
        return "[]"
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return "[]"
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = [part.strip() for part in raw.split(",") if part.strip()]
        else:
            if isinstance(parsed, list):
                parsed = [str(item).strip() for item in parsed if str(item).strip()]
            elif isinstance(parsed, str):
                parsed = [part.strip() for part in parsed.split(",") if part.strip()]
            else:
                parsed = []
        return json.dumps(parsed)
    if isinstance(value, (list, tuple, set)):
        normalized = [str(item).strip() for item in value if str(item).strip()]
        return json.dumps(normalized)
    return "[]"


def _normalize_system_key(raw: Any, fallback: str = "device") -> str:
    text = _coerce_text(raw, fallback).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    return text or fallback


def settings_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout.db"


def _fetchone_dict(cursor: sqlite3.Cursor) -> Optional[Dict[str, Any]]:
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [str(col[0]) for col in (cursor.description or [])]
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return {columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))}


def _normalize_scheduler_mode(value: Any, default: str = "full") -> str:
    mode = _coerce_text(value, default).lower() or default
    return mode if mode in SUPPORTED_SCHEDULER_MODES else default


def _normalize_assignment_state(value: Any, default: str = "active") -> str:
    state = _coerce_text(value, default).lower() or default
    return state if state in SUPPORTED_ASSIGNMENT_STATES else default


def _normalize_device_class(value: Any, default: str = "tx_rx") -> str:
    device_class = _coerce_text(value, default).lower() or default
    return device_class if device_class in SUPPORTED_DEVICE_CLASSES else default


def _normalize_varac_cluster_id(value: Any, fallback: str = "CLUSTER") -> str:
    text = _coerce_text(value, fallback).upper() or fallback
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^A-Z0-9_.-]+", "-", text)
    text = text.strip("._-")
    return text or fallback


def _is_observer_device_class(value: Any) -> bool:
    raw = value.get("device_class", "tx_rx") if isinstance(value, Mapping) else value
    return _normalize_device_class(raw, "tx_rx") == "observer"


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


def _normalize_coordination_policy_type(value: Any, default: str = SHARED_PTT_POLICY_TYPE) -> str:
    policy_type = _coerce_text(value, default).lower() or default
    return policy_type if policy_type in SUPPORTED_COORDINATION_POLICY_TYPES else default


def _normalize_coordination_safety_mode(value: Any, default: str = "warn") -> str:
    safety_mode = _coerce_text(value, default).lower() or default
    return safety_mode if safety_mode in SUPPORTED_COORDINATION_SAFETY_MODES else default


def _normalize_profile_swap_mode(value: Any, default: str = "use_target_profile") -> str:
    mode = _coerce_text(value, default).lower() or default
    return mode if mode in SUPPORTED_PROFILE_SWAP_MODES else default


def _coerce_json_object_text(value: Any) -> str:
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    if value in (None, ""):
        return "{}"
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "{}"
        try:
            parsed = json.loads(text)
        except Exception:
            return "{}"
        return json.dumps(parsed if isinstance(parsed, dict) else {}, sort_keys=True)
    return "{}"


def _parse_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = _coerce_text(value, "")
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {str(row[1]) for row in cur.fetchall()}


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Mapping[str, str]) -> None:
    existing = _table_columns(conn, table)
    cur = conn.cursor()
    for name, ddl in columns.items():
        if name in existing:
            continue
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def ensure_multi_radio_settings_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for table_name, spec in SETTINGS_TABLE_SPECS.items():
        cur.execute(str(spec["ddl"]).strip())
        _ensure_columns(conn, table_name, spec["columns"])  # type: ignore[arg-type]
        for stmt in spec.get("indexes", ()):  # type: ignore[union-attr]
            cur.execute(str(stmt))
    conn.commit()


def _normalize_control_backend(settings_values: Mapping[str, Any]) -> str:
    raw = str(settings_values.get("control_via", "FLRig") or "FLRig").strip().upper()
    if raw == "FLRIG":
        return "flrig"
    if raw == "JS8CALL":
        return "js8call"
    if raw == "RIGCTLD":
        return "rigctld"
    if raw == "MANUAL":
        return "manual"
    return "manual"


def _settings_int(settings_values: Mapping[str, Any], key: str, default: int) -> int:
    try:
        value = settings_values.get(key, default)
        return int(value if value not in (None, "") else default)
    except Exception:
        return int(default)


def _settings_text(settings_values: Mapping[str, Any], key: str, default: str = "") -> str:
    try:
        return str(settings_values.get(key, default) or "").strip()
    except Exception:
        return str(default or "").strip()


def _settings_bool(settings_values: Mapping[str, Any], key: str, default: bool) -> bool:
    try:
        value = settings_values.get(key, default)
    except Exception:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(default)


def _settings_optional_int(settings_values: Mapping[str, Any], key: str, default: Optional[int] = None) -> Optional[int]:
    try:
        value = settings_values.get(key, default)
    except Exception:
        return default
    return _coerce_optional_int(value, default)


def _legacy_control_via(control_backend: str) -> str:
    backend = _coerce_text(control_backend, "manual").lower()
    if backend == "flrig":
        return "FLRig"
    if backend == "js8call":
        return "JS8Call"
    if backend == "rigctld":
        return "RIGCTLD"
    return "Manual"


def _legacy_device_projection_from_settings(settings_values: Mapping[str, Any]) -> Dict[str, Any]:
    control_backend = _normalize_control_backend(settings_values)
    flrig_host = _settings_text(settings_values, "flrig_host", "127.0.0.1") or "127.0.0.1"
    fldigi_host = _settings_text(settings_values, "fldigi_host", "") or flrig_host or "127.0.0.1"
    js8_host = _settings_text(settings_values, "js8_host", "127.0.0.1") or "127.0.0.1"
    launch_path = ""
    if control_backend == "flrig":
        launch_path = _settings_text(settings_values, "path_flrig", "")
    elif control_backend == "js8call":
        launch_path = _settings_text(settings_values, "path_js8call", "")

    return {
        "control_backend": control_backend,
        "rig_host": _settings_text(settings_values, "rig_host", ""),
        "rig_port": _settings_optional_int(settings_values, "rig_port"),
        "flrig_host": flrig_host,
        "flrig_port": _settings_int(settings_values, "flrig_port", 12345),
        "fldigi_host": fldigi_host,
        "fldigi_port": _settings_int(settings_values, "fldigi_port", 7362),
        "fldigi_log_path": _settings_text(settings_values, "fldigi_log_path", ""),
        "working_dir": "",
        "js8_host": js8_host,
        "js8_port": _settings_int(settings_values, "js8_port", 2442),
        "js8_profile_path": _settings_text(settings_values, "js8_profile_path", ""),
        "js8_directed_path": _settings_text(settings_values, "js8_directed_path", ""),
        "js8_inbox_path": "",
        "varac_install_path": _settings_text(settings_values, "varac_path", ""),
        "varac_db_path": _settings_text(settings_values, "varac_db_path", ""),
        "varac_ini_path": _settings_text(settings_values, "varac_ini_path", ""),
        "sdr_host": _settings_text(settings_values, "sdr_host", ""),
        "sdr_port": _settings_optional_int(settings_values, "sdr_port"),
        "launch_enabled": 1 if _settings_bool(settings_values, "launch_control_enabled", True) else 0,
        "launch_path": launch_path,
        "launch_cmd": _settings_text(settings_values, "varac_launch_cmd", ""),
    }


def _legacy_settings_projection_from_device(device_profile: Mapping[str, Any]) -> Dict[str, Any]:
    control_backend = _coerce_text(device_profile.get("control_backend", "manual"), "manual").lower() or "manual"
    flrig_host = _coerce_text(device_profile.get("flrig_host", ""), "127.0.0.1") or "127.0.0.1"
    fldigi_host = _coerce_text(device_profile.get("fldigi_host", ""), "") or flrig_host or "127.0.0.1"
    js8_host = _coerce_text(device_profile.get("js8_host", ""), "127.0.0.1") or "127.0.0.1"

    updates: Dict[str, Any] = {
        "control_via": _legacy_control_via(control_backend),
        "rig_host": _coerce_text(device_profile.get("rig_host", ""), ""),
        "rig_port": _coerce_optional_int(device_profile.get("rig_port"), 4532),
        "flrig_host": flrig_host,
        "flrig_port": _coerce_optional_int(device_profile.get("flrig_port"), 12345),
        "fldigi_host": fldigi_host,
        "fldigi_port": _coerce_optional_int(device_profile.get("fldigi_port"), 7362),
        "fldigi_log_path": _coerce_text(device_profile.get("fldigi_log_path", ""), ""),
        "fldigi_checkin_dir": _coerce_text(device_profile.get("fldigi_checkin_dir", ""), ""),
        "js8_host": js8_host,
        "js8_port": _coerce_optional_int(device_profile.get("js8_port"), 2442),
        "js8_offset_hz": _coerce_int(device_profile.get("js8_offset_hz", 0), 0),
        "js8_profile_path": _coerce_text(device_profile.get("js8_profile_path", ""), ""),
        "js8_directed_path": _coerce_text(device_profile.get("js8_directed_path", ""), ""),
        "js8_forms_path": _coerce_text(device_profile.get("js8_forms_path", ""), ""),
        "varac_path": _coerce_text(device_profile.get("varac_install_path", ""), ""),
        "varac_db_path": _coerce_text(device_profile.get("varac_db_path", ""), ""),
        "varac_ini_path": _coerce_text(device_profile.get("varac_ini_path", ""), ""),
        "varac_launch_cmd": _coerce_text(device_profile.get("launch_cmd", ""), ""),
        "launch_control_enabled": bool(_coerce_bool_int(device_profile.get("launch_enabled"), True)),
    }
    launch_path = _coerce_text(device_profile.get("launch_path", ""), "")
    if control_backend == "flrig":
        updates["path_flrig"] = launch_path
    elif control_backend == "js8call":
        updates["path_js8call"] = launch_path
    flrig_path = _coerce_text(device_profile.get("flrig_path", ""), "")
    if flrig_path:
        updates["path_flrig"] = flrig_path
    fldigi_path = _coerce_text(device_profile.get("fldigi_path", ""), "")
    if fldigi_path:
        updates["path_fldigi"] = fldigi_path
    js8_install_path = _coerce_text(device_profile.get("js8_install_path", ""), "")
    if js8_install_path:
        updates["path_js8call"] = js8_install_path
    js8_spotter_path = _coerce_text(device_profile.get("js8_spotter_launch_path", ""), "")
    if js8_spotter_path:
        updates["path_js8spotter"] = js8_spotter_path
    js8_commstat_path = _coerce_text(device_profile.get("js8_commstat_launch_path", ""), "")
    if js8_commstat_path:
        updates["path_commstat"] = js8_commstat_path
    return updates


def _write_kv_values(conn: sqlite3.Connection, values: Mapping[str, Any]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
        [(str(key), json.dumps(value)) for key, value in values.items()],
    )


def _resolve_device_profile_links_conn(conn: sqlite3.Connection, profile: Mapping[str, Any]) -> Dict[str, Any]:
    data = dict(profile)
    backend = _coerce_text(data.get("control_backend", "manual"), "manual").lower() or "manual"

    js8_instance_id = _coerce_optional_int(data.get("js8_instance_id"))
    data["js8_instance_id"] = js8_instance_id
    data["js8_instance_name"] = ""
    if js8_instance_id is not None:
        js8_row = _js8_instance_by_id(conn, int(js8_instance_id))
        if js8_row:
            data["js8_instance_name"] = _coerce_text(js8_row.get("name", ""), "")
            data["js8_host"] = _coerce_text(js8_row.get("host", ""), "127.0.0.1") or "127.0.0.1"
            data["js8_port"] = _coerce_optional_int(js8_row.get("port"), 2442)
            data["js8_profile_path"] = _coerce_text(js8_row.get("profile_path", ""), "")
            data["js8_directed_path"] = _coerce_text(js8_row.get("directed_path", ""), "")
            data["js8_inbox_path"] = _coerce_text(js8_row.get("inbox_path", ""), "")
            data["js8_forms_path"] = _coerce_text(js8_row.get("forms_path", ""), "")
            data["js8_install_path"] = _coerce_text(js8_row.get("install_path", ""), "")
            data["js8_spotter_launch_path"] = _coerce_text(js8_row.get("spotter_launch_path", ""), "")
            data["js8_commstat_launch_path"] = _coerce_text(js8_row.get("commstat_launch_path", ""), "")
            data["js8_offset_hz"] = _coerce_int(js8_row.get("offset_hz", 0), 0)
            if backend == "js8call":
                data["launch_path"] = _coerce_text(js8_row.get("install_path", ""), "")

    fast_light_config_id = _coerce_optional_int(data.get("fast_light_config_id"))
    data["fast_light_config_id"] = fast_light_config_id
    data["fast_light_config_name"] = ""
    if fast_light_config_id is not None:
        fast_light_row = _fast_light_config_by_id(conn, int(fast_light_config_id))
        if fast_light_row:
            data["fast_light_config_name"] = _coerce_text(fast_light_row.get("name", ""), "")
            data["flrig_host"] = _coerce_text(fast_light_row.get("flrig_host", ""), "127.0.0.1") or "127.0.0.1"
            data["flrig_port"] = _coerce_optional_int(fast_light_row.get("flrig_port"), 12345)
            data["fldigi_host"] = _coerce_text(
                fast_light_row.get("fldigi_host", ""),
                data.get("flrig_host", "127.0.0.1"),
            ) or _coerce_text(data.get("flrig_host", ""), "127.0.0.1")
            data["fldigi_port"] = _coerce_optional_int(fast_light_row.get("fldigi_port"), 7362)
            data["fldigi_log_path"] = _coerce_text(fast_light_row.get("fldigi_log_path", ""), "")
            data["flrig_path"] = _coerce_text(fast_light_row.get("flrig_path", ""), "")
            data["fldigi_path"] = _coerce_text(fast_light_row.get("fldigi_path", ""), "")
            data["fldigi_checkin_dir"] = _coerce_text(fast_light_row.get("fldigi_checkin_dir", ""), "")
            if backend == "flrig":
                data["launch_path"] = _coerce_text(fast_light_row.get("flrig_path", ""), "")

    varac_node_id = _coerce_optional_int(data.get("varac_node_id"))
    data["varac_node_id"] = varac_node_id
    data["varac_node_name"] = ""
    if varac_node_id is not None:
        varac_row = _varac_node_by_id(conn, int(varac_node_id))
        if varac_row:
            data["varac_node_name"] = _coerce_text(varac_row.get("name", ""), "")
            data["varac_install_path"] = _coerce_text(varac_row.get("install_path", ""), "")
            data["varac_db_path"] = _coerce_text(varac_row.get("db_path", ""), "")
            data["varac_ini_path"] = _coerce_text(varac_row.get("ini_path", ""), "")
            data["launch_cmd"] = _coerce_text(varac_row.get("launch_cmd", ""), "")
            data["varac_incoming_path"] = _coerce_text(varac_row.get("incoming_path", ""), "")
    return data


def _device_profile_by_id(conn: sqlite3.Connection, device_profile_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM device_profiles WHERE id=?", (int(device_profile_id),))
    row = _fetchone_dict(cur)
    if not row:
        return None
    return _resolve_device_profile_links_conn(conn, row)


def _js8_instance_by_id(conn: sqlite3.Connection, js8_instance_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM js8_instances WHERE id=?", (int(js8_instance_id),))
    return _fetchone_dict(cur)


def _fast_light_config_by_id(conn: sqlite3.Connection, fast_light_config_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM fast_light_configs WHERE id=?", (int(fast_light_config_id),))
    return _fetchone_dict(cur)


def _varac_node_by_id(conn: sqlite3.Connection, varac_node_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM varac_nodes WHERE id=?", (int(varac_node_id),))
    return _fetchone_dict(cur)


def _list_js8_instances_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
          FROM js8_instances
      ORDER BY CASE WHEN system_key=? THEN 0 ELSE 1 END, LOWER(name) ASC, id ASC
        """
        ,
        (DEFAULT_JS8_INSTANCE_SYSTEM_KEY,),
    ).fetchall()
    return [dict(row) for row in rows]


def _list_fast_light_configs_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
          FROM fast_light_configs
      ORDER BY CASE WHEN system_key=? THEN 0 ELSE 1 END, LOWER(name) ASC, id ASC
        """
        ,
        (DEFAULT_FAST_LIGHT_SYSTEM_KEY,),
    ).fetchall()
    return [dict(row) for row in rows]


def _list_varac_nodes_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
          FROM varac_nodes
      ORDER BY CASE WHEN system_key=? THEN 0 ELSE 1 END, LOWER(name) ASC, id ASC
        """
        ,
        (DEFAULT_VARAC_NODE_SYSTEM_KEY,),
    ).fetchall()
    return [dict(row) for row in rows]


def _operating_profile_by_id(conn: sqlite3.Connection, operating_profile_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM operating_profiles WHERE id=?", (int(operating_profile_id),))
    return _fetchone_dict(cur)


def _runtime_primary_device_profile(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
          FROM device_profiles
         WHERE runtime_active=1
           AND runtime_primary=1
      ORDER BY display_order ASC, id ASC
         LIMIT 1
        """
    )
    row = _fetchone_dict(cur)
    if not row:
        return None
    return _resolve_device_profile_links_conn(conn, row)


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


def _next_system_key(
    conn: sqlite3.Connection,
    table_name: str,
    raw_key: Any,
    *,
    exclude_id: Optional[int] = None,
    fallback: str = "device",
) -> str:
    base = _normalize_system_key(raw_key, fallback)
    candidate = base
    suffix = 2
    while True:
        if exclude_id is None:
            cur = conn.execute(f"SELECT id FROM {table_name} WHERE system_key=?", (candidate,))
        else:
            cur = conn.execute(
                f"SELECT id FROM {table_name} WHERE system_key=? AND id<>?",
                (candidate, int(exclude_id)),
            )
        if cur.fetchone() is None:
            return candidate
        candidate = f"{base}_{suffix}"
        suffix += 1


def _next_device_system_key(conn: sqlite3.Connection, raw_key: Any, *, exclude_id: Optional[int] = None) -> str:
    return _next_system_key(conn, "device_profiles", raw_key, exclude_id=exclude_id, fallback="device")


def _next_operating_system_key(conn: sqlite3.Connection, raw_key: Any, *, exclude_id: Optional[int] = None) -> str:
    return _next_system_key(conn, "operating_profiles", raw_key, exclude_id=exclude_id, fallback="operating")


def _next_js8_instance_system_key(conn: sqlite3.Connection, raw_key: Any, *, exclude_id: Optional[int] = None) -> str:
    return _next_system_key(conn, "js8_instances", raw_key, exclude_id=exclude_id, fallback="js8")


def _next_fast_light_system_key(conn: sqlite3.Connection, raw_key: Any, *, exclude_id: Optional[int] = None) -> str:
    return _next_system_key(conn, "fast_light_configs", raw_key, exclude_id=exclude_id, fallback="fast_light")


def _next_varac_node_system_key(conn: sqlite3.Connection, raw_key: Any, *, exclude_id: Optional[int] = None) -> str:
    return _next_system_key(conn, "varac_nodes", raw_key, exclude_id=exclude_id, fallback="varac")


def _save_js8_instance_conn(conn: sqlite3.Connection, values: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(values or {})
    requested_id = _coerce_optional_int(payload.get("id"))
    existing = _js8_instance_by_id(conn, int(requested_id)) if requested_id is not None else None
    now_iso = _utc_now_iso()
    name_default = _coerce_text((existing or {}).get("name", ""), "") or "JS8 Instance"
    system_key_default = _coerce_text((existing or {}).get("system_key", ""), "") or name_default
    name = _coerce_text(payload.get("name", name_default), name_default) or name_default
    requested_key = payload.get("system_key", system_key_default if existing else name)
    system_key = _next_js8_instance_system_key(conn, requested_key, exclude_id=requested_id)
    record = {
        "system_key": system_key,
        "name": name,
        "enabled": _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True),
        "host": _coerce_text(payload.get("host", (existing or {}).get("host", "127.0.0.1")), "127.0.0.1") or "127.0.0.1",
        "port": _coerce_int(payload.get("port", (existing or {}).get("port", 2442)), 2442),
        "offset_hz": _coerce_int(payload.get("offset_hz", (existing or {}).get("offset_hz", 0)), 0),
        "profile_path": _coerce_text(payload.get("profile_path", (existing or {}).get("profile_path", "")), ""),
        "directed_path": _coerce_text(payload.get("directed_path", (existing or {}).get("directed_path", "")), ""),
        "inbox_path": _coerce_text(payload.get("inbox_path", (existing or {}).get("inbox_path", "")), ""),
        "forms_path": _coerce_text(payload.get("forms_path", (existing or {}).get("forms_path", "")), ""),
        "install_path": _coerce_text(payload.get("install_path", (existing or {}).get("install_path", "")), ""),
        "spotter_launch_path": _coerce_text(
            payload.get("spotter_launch_path", (existing or {}).get("spotter_launch_path", "")),
            "",
        ),
        "commstat_launch_path": _coerce_text(
            payload.get("commstat_launch_path", (existing or {}).get("commstat_launch_path", "")),
            "",
        ),
    }
    columns = list(record.keys())
    if existing:
        assignments = ", ".join(f"{column}=?" for column in columns)
        conn.execute(
            f"UPDATE js8_instances SET {assignments}, updated_utc=? WHERE id=?",
            [record[column] for column in columns] + [now_iso, int(requested_id)],
        )
        row_id = int(requested_id)
    else:
        insert_columns = columns + ["created_utc", "updated_utc"]
        placeholders = ", ".join("?" for _ in insert_columns)
        conn.execute(
            f"INSERT INTO js8_instances ({', '.join(insert_columns)}) VALUES ({placeholders})",
            [record[column] for column in columns] + [now_iso, now_iso],
        )
        row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.commit()
    return _js8_instance_by_id(conn, row_id) or {}


def _save_fast_light_config_conn(conn: sqlite3.Connection, values: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(values or {})
    requested_id = _coerce_optional_int(payload.get("id"))
    existing = _fast_light_config_by_id(conn, int(requested_id)) if requested_id is not None else None
    now_iso = _utc_now_iso()
    name_default = _coerce_text((existing or {}).get("name", ""), "") or "Fast Light Config"
    system_key_default = _coerce_text((existing or {}).get("system_key", ""), "") or name_default
    name = _coerce_text(payload.get("name", name_default), name_default) or name_default
    requested_key = payload.get("system_key", system_key_default if existing else name)
    system_key = _next_fast_light_system_key(conn, requested_key, exclude_id=requested_id)
    flrig_host = _coerce_text(
        payload.get("flrig_host", (existing or {}).get("flrig_host", "127.0.0.1")),
        "127.0.0.1",
    ) or "127.0.0.1"
    record = {
        "system_key": system_key,
        "name": name,
        "enabled": _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True),
        "flrig_path": _coerce_text(payload.get("flrig_path", (existing or {}).get("flrig_path", "")), ""),
        "flrig_host": flrig_host,
        "flrig_port": _coerce_int(payload.get("flrig_port", (existing or {}).get("flrig_port", 12345)), 12345),
        "fldigi_path": _coerce_text(payload.get("fldigi_path", (existing or {}).get("fldigi_path", "")), ""),
        "fldigi_host": _coerce_text(
            payload.get("fldigi_host", (existing or {}).get("fldigi_host", "")),
            "",
        ) or flrig_host,
        "fldigi_port": _coerce_int(payload.get("fldigi_port", (existing or {}).get("fldigi_port", 7362)), 7362),
        "fldigi_log_path": _coerce_text(
            payload.get("fldigi_log_path", (existing or {}).get("fldigi_log_path", "")),
            "",
        ),
        "fldigi_checkin_dir": _coerce_text(
            payload.get("fldigi_checkin_dir", (existing or {}).get("fldigi_checkin_dir", "")),
            "",
        ),
    }
    columns = list(record.keys())
    if existing:
        assignments = ", ".join(f"{column}=?" for column in columns)
        conn.execute(
            f"UPDATE fast_light_configs SET {assignments}, updated_utc=? WHERE id=?",
            [record[column] for column in columns] + [now_iso, int(requested_id)],
        )
        row_id = int(requested_id)
    else:
        insert_columns = columns + ["created_utc", "updated_utc"]
        placeholders = ", ".join("?" for _ in insert_columns)
        conn.execute(
            f"INSERT INTO fast_light_configs ({', '.join(insert_columns)}) VALUES ({placeholders})",
            [record[column] for column in columns] + [now_iso, now_iso],
        )
        row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.commit()
    return _fast_light_config_by_id(conn, row_id) or {}


def _save_varac_node_conn(conn: sqlite3.Connection, values: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(values or {})
    requested_id = _coerce_optional_int(payload.get("id"))
    existing = _varac_node_by_id(conn, int(requested_id)) if requested_id is not None else None
    now_iso = _utc_now_iso()
    name_default = _coerce_text((existing or {}).get("name", ""), "") or "VarAC Node"
    system_key_default = _coerce_text((existing or {}).get("system_key", ""), "") or name_default
    name = _coerce_text(payload.get("name", name_default), name_default) or name_default
    requested_key = payload.get("system_key", system_key_default if existing else name)
    system_key = _next_varac_node_system_key(conn, requested_key, exclude_id=requested_id)
    record = {
        "system_key": system_key,
        "name": name,
        "enabled": _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True),
        "install_path": _coerce_text(payload.get("install_path", (existing or {}).get("install_path", "")), ""),
        "db_path": _coerce_text(payload.get("db_path", (existing or {}).get("db_path", "")), ""),
        "ini_path": _coerce_text(payload.get("ini_path", (existing or {}).get("ini_path", "")), ""),
        "launch_cmd": _coerce_text(payload.get("launch_cmd", (existing or {}).get("launch_cmd", "")), ""),
        "incoming_path": _coerce_text(payload.get("incoming_path", (existing or {}).get("incoming_path", "")), ""),
    }
    columns = list(record.keys())
    if existing:
        assignments = ", ".join(f"{column}=?" for column in columns)
        conn.execute(
            f"UPDATE varac_nodes SET {assignments}, updated_utc=? WHERE id=?",
            [record[column] for column in columns] + [now_iso, int(requested_id)],
        )
        row_id = int(requested_id)
    else:
        insert_columns = columns + ["created_utc", "updated_utc"]
        placeholders = ", ".join("?" for _ in insert_columns)
        conn.execute(
            f"INSERT INTO varac_nodes ({', '.join(insert_columns)}) VALUES ({placeholders})",
            [record[column] for column in columns] + [now_iso, now_iso],
        )
        row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.commit()
    return _varac_node_by_id(conn, row_id) or {}


def _normalize_effective_assignments(conn: sqlite3.Connection, *, device_profile_id: Optional[int] = None) -> None:
    placeholders = ", ".join("?" for _ in EFFECTIVE_ASSIGNMENT_STATES)
    params: List[Any] = list(EFFECTIVE_ASSIGNMENT_STATES)
    device_where = ""
    if device_profile_id is not None:
        device_where = " AND device_profile_id=?"
        params.append(int(device_profile_id))
    rows = conn.execute(
        f"""
        SELECT id, device_profile_id, assignment_state, ends_utc
          FROM operating_profile_assignments
         WHERE assignment_state IN ({placeholders}){device_where}
      ORDER BY device_profile_id ASC, id DESC
        """,
        params,
    ).fetchall()
    keep_by_device: Dict[int, int] = {}
    updates: List[tuple[Any, ...]] = []
    now_iso = _utc_now_iso()
    for row in rows:
        row_id = int(row[0])
        row_device_id = int(row[1])
        if row_device_id not in keep_by_device:
            keep_by_device[row_device_id] = row_id
            continue
        updates.append((row[3] or now_iso, now_iso, row_id))
    if updates:
        conn.executemany(
            """
            UPDATE operating_profile_assignments
               SET assignment_state='superseded', ends_utc=?, updated_utc=?
             WHERE id=?
            """,
            updates,
        )
        conn.commit()


def _effective_assignment_for_device(conn: sqlite3.Connection, device_profile_id: int) -> Optional[Dict[str, Any]]:
    _normalize_effective_assignments(conn, device_profile_id=int(device_profile_id))
    placeholders = ", ".join("?" for _ in EFFECTIVE_ASSIGNMENT_STATES)
    cur = conn.execute(
        f"""
        SELECT *
          FROM operating_profile_assignments
         WHERE device_profile_id=?
           AND assignment_state IN ({placeholders})
      ORDER BY id DESC
         LIMIT 1
        """,
        (int(device_profile_id), *tuple(EFFECTIVE_ASSIGNMENT_STATES)),
    )
    return _fetchone_dict(cur)


def _coordination_policy_from_row(row: sqlite3.Row | Mapping[str, Any] | Dict[str, Any]) -> Dict[str, Any]:
    data = dict(row)
    data["policy_type"] = _normalize_coordination_policy_type(data.get("policy_type", SHARED_PTT_POLICY_TYPE))
    data["safety_mode"] = _normalize_coordination_safety_mode(data.get("safety_mode", "warn"))
    data["trigger"] = _parse_json_object(data.get("trigger_json", "{}"))
    data["action"] = _parse_json_object(data.get("action_json", "{}"))
    return data


def _active_profile_swap_policy_conn(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
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

    ensure_multi_radio_settings_schema(conn)
    device = _device_profile_by_id(conn, int(device_profile_id))
    if not device:
        raise KeyError(f"Unknown device profile id: {device_profile_id}")
    operating_profile = _operating_profile_by_id(conn, int(operating_profile_id))
    if not operating_profile:
        raise KeyError(f"Unknown operating profile id: {operating_profile_id}")
    if int(operating_profile.get("enabled", 1) or 0) != 1:
        raise ValueError("Cannot assign a disabled operating profile.")

    active_swap = _active_profile_swap_policy_conn(conn)
    if active_swap is not None and not allow_active_swap_edit:
        source_id = int(active_swap.get("source_device_id", 0) or 0)
        target_id = int(active_swap.get("target_device_id", 0) or 0)
        if int(device_profile_id) in {source_id, target_id}:
            raise ValueError("Restore the active temporary swap before editing assignments on the swap source/target devices.")

    _normalize_effective_assignments(conn, device_profile_id=int(device_profile_id))
    current = _effective_assignment_for_device(conn, int(device_profile_id))
    now_iso = _utc_now_iso()
    starts_value = _coerce_text(starts_utc, now_iso) or now_iso
    ends_value = _coerce_text(ends_utc, "")
    reason_value = _coerce_text(reason, "")
    created_by_value = _coerce_text(created_by, "settings_ui") or "settings_ui"

    if current:
        current_state = _normalize_assignment_state(current.get("assignment_state", "active"), "active")
        current_operating_id = int(current.get("operating_profile_id", 0) or 0)
        current_reason = _coerce_text(current.get("reason", ""), "")
        current_ends = _coerce_text(current.get("ends_utc", ""), "")
        if (
            current_operating_id == int(operating_profile_id)
            and current_state == desired_state
            and current_reason == reason_value
            and current_ends == ends_value
        ):
            return current
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
                current_ends or now_iso,
                now_iso,
                int(current.get("id", 0) or 0),
            ),
        )

    conn.execute(
        """
        INSERT INTO operating_profile_assignments (
            device_profile_id, operating_profile_id, assignment_state, starts_utc,
            ends_utc, reason, created_by, created_utc, updated_utc
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
    _normalize_effective_assignments(conn, device_profile_id=int(device_profile_id))
    return _effective_assignment_for_device(conn, int(device_profile_id)) or {}


def _restore_default_operating_profile_conn(
    conn: sqlite3.Connection,
    device_profile_id: int,
    *,
    reason: str = "Restored default operating profile.",
    created_by: str = "settings_ui",
    allow_active_swap_edit: bool = False,
) -> Dict[str, Any]:
    ensure_multi_radio_settings_schema(conn)
    row = conn.execute(
        "SELECT id FROM operating_profiles WHERE system_key=?",
        (DEFAULT_OPERATING_SYSTEM_KEY,),
    ).fetchone()
    if row is None:
        operating_profile_id = _ensure_default_operating_profile(conn, {})
    else:
        operating_profile_id = int(row[0])
    return _set_device_operating_profile_conn(
        conn,
        int(device_profile_id),
        int(operating_profile_id),
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


def _enrich_profile_swap_policy(conn: sqlite3.Connection, policy: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(policy, Mapping):
        return None
    data = dict(policy)
    trigger = dict(data.get("trigger") or {})
    action = dict(data.get("action") or {})
    source_id = int(data.get("source_device_id", 0) or 0)
    target_id = int(data.get("target_device_id", 0) or 0)
    source_device = _device_profile_by_id(conn, source_id) if source_id > 0 else None
    target_device = _device_profile_by_id(conn, target_id) if target_id > 0 else None
    data["source_device_name"] = _coerce_text((source_device or {}).get("name", ""), "")
    data["target_device_name"] = _coerce_text((target_device or {}).get("name", ""), "")
    mode = _normalize_profile_swap_mode(trigger.get("mode", "use_target_profile"))
    data["mode"] = mode
    carried_profile_id = action.get("applied_operating_profile_id")
    if carried_profile_id not in (None, ""):
        carried_profile = _operating_profile_by_id(conn, int(carried_profile_id))
        data["applied_operating_profile_name"] = _coerce_text((carried_profile or {}).get("name", ""), "")
    else:
        data["applied_operating_profile_name"] = ""
    restore_assignment = dict(action.get("restore_target_assignment") or {})
    restore_operating_id = restore_assignment.get("operating_profile_id")
    if restore_operating_id not in (None, ""):
        restore_profile = _operating_profile_by_id(conn, int(restore_operating_id))
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
        data["device_class"] = _normalize_device_class(data.get("device_class", "tx_rx"), "tx_rx")
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
    existing_rows = conn.execute(
        """
        SELECT *
          FROM station_coordination_policies
         WHERE policy_type=?
        """,
        (policy_type,),
    ).fetchall()
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
    refreshed = conn.execute(
        """
        SELECT *
          FROM station_coordination_policies
         WHERE policy_type=?
      ORDER BY priority ASC, source_device_id ASC, target_device_id ASC, id ASC
        """,
        (policy_type,),
    ).fetchall()
    return [_coordination_policy_from_row(row) for row in refreshed]


def _sync_shared_ptt_policies_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    rows = conn.execute(
        """
        SELECT id, name, enabled, device_class, ptt_group
          FROM device_profiles
         WHERE enabled=1
      ORDER BY id ASC
        """
    ).fetchall()
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        device = dict(row)
        if _is_observer_device_class(device):
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
    rows = conn.execute(
        """
        SELECT id, name, enabled, device_class, antenna_group, frontend_group, amplifier_group
          FROM device_profiles
         WHERE enabled=1
      ORDER BY id ASC
        """
    ).fetchall()

    pair_map: Dict[tuple[int, int], Dict[str, Any]] = {}
    group_columns = (
        ("antenna_group", "antenna_groups"),
        ("amplifier_group", "amplifier_groups"),
        ("frontend_group", "frontend_groups"),
    )
    devices: List[Dict[str, Any]] = []
    for row in rows:
        device = dict(row)
        if _is_observer_device_class(device):
            continue
        devices.append(device)
    for device in devices:
        device["name"] = _coerce_text(device.get("name", f"Device {int(device.get('id', 0) or 0)}"))
        device["antenna_group"] = normalize_resource_group(device.get("antenna_group", ""))
        device["amplifier_group"] = normalize_resource_group(device.get("amplifier_group", ""))
        device["frontend_group"] = normalize_resource_group(device.get("frontend_group", ""))

    groups_by_field: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        "antenna_group": {},
        "amplifier_group": {},
        "frontend_group": {},
    }
    for device in devices:
        for field_name in groups_by_field.keys():
            group_value = str(device.get(field_name, "") or "").strip()
            if not group_value:
                continue
            groups_by_field[field_name].setdefault(group_value, []).append(device)

    for field_name, trigger_key in group_columns:
        for group_name, members in groups_by_field[field_name].items():
            sorted_members = sorted(members, key=lambda item: (str(item.get("name", "")).lower(), int(item.get("id", 0) or 0)))
            for idx, left in enumerate(sorted_members):
                left_id = int(left.get("id", 0) or 0)
                for right in sorted_members[idx + 1 :]:
                    right_id = int(right.get("id", 0) or 0)
                    source_id, target_id = sorted((left_id, right_id))
                    pair = (source_id, target_id)
                    pair_entry = pair_map.setdefault(
                        pair,
                        {
                            "source_id": source_id,
                            "target_id": target_id,
                            "source_name": _coerce_text(left.get("name", f"Device {source_id}")),
                            "target_name": _coerce_text(right.get("name", f"Device {target_id}")),
                            "antenna_groups": set(),
                            "amplifier_groups": set(),
                            "frontend_groups": set(),
                        },
                    )
                    if source_id != left_id:
                        pair_entry["source_name"] = _coerce_text(right.get("name", f"Device {source_id}"))
                        pair_entry["target_name"] = _coerce_text(left.get("name", f"Device {target_id}"))
                    pair_entry[trigger_key].add(group_name)

    expected: Dict[tuple[int, int], Dict[str, Any]] = {}
    for pair, info in pair_map.items():
        trigger = {
            "antenna_groups": sorted(str(item) for item in info["antenna_groups"]),
            "amplifier_groups": sorted(str(item) for item in info["amplifier_groups"]),
            "frontend_groups": sorted(str(item) for item in info["frontend_groups"]),
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
    rows = conn.execute(
        """
        SELECT id, name, enabled, device_class, antenna_group, frontend_group, sdr_host, sdr_port
          FROM device_profiles
         WHERE enabled=1
      ORDER BY id ASC
        """
    ).fetchall()
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
        source_name = str(source.get("name", "") or f"Device {source_id}").strip() or f"Device {source_id}"
        source_antenna = str(source.get("antenna_group", "") or "").strip()
        source_frontend = str(source.get("frontend_group", "") or "").strip()
        for observer in observers:
            target_id = int(observer.get("id", 0) or 0)
            if source_id <= 0 or target_id <= 0 or source_id == target_id:
                continue
            observer_name = str(observer.get("name", "") or f"Device {target_id}").strip() or f"Device {target_id}"
            observer_antenna = str(observer.get("antenna_group", "") or "").strip()
            observer_frontend = str(observer.get("frontend_group", "") or "").strip()
            shared_antennas = [source_antenna] if source_antenna and source_antenna == observer_antenna else []
            shared_frontends = [source_frontend] if source_frontend and source_frontend == observer_frontend else []
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
                        "shared_antenna_groups": shared_antennas,
                        "shared_frontend_groups": shared_frontends,
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
        if not isinstance(cluster, dict):
            continue
        cluster_db_id = int(cluster.get("id", 0) or 0)
        gateway_id = (
            int(cluster.get("gateway_handler_device_id", 0) or 0)
            if cluster.get("gateway_handler_device_id") not in (None, "")
            else 0
        )
        if cluster_db_id <= 0 or gateway_id <= 0:
            continue
        members = [
            row
            for row in _list_varac_cluster_members_conn(conn, cluster_id=cluster_db_id)
            if int(row.get("enabled", 1) or 0) == 1
        ]
        gateway_row = next(
            (
                row
                for row in members
                if int(row.get("device_profile_id", 0) or 0) == gateway_id
            ),
            None,
        )
        if not isinstance(gateway_row, dict):
            continue
        gateway_name = _coerce_text(
            gateway_row.get("device_name", cluster.get("gateway_handler_name", f"Device {gateway_id}")),
            f"Device {gateway_id}",
        )
        cluster_name = _coerce_text(cluster.get("name", f"Cluster {cluster_db_id}"), f"Cluster {cluster_db_id}")
        cluster_key = _coerce_text(cluster.get("cluster_id", ""), "")
        for member in members:
            target_id = int(member.get("device_profile_id", 0) or 0)
            if target_id <= 0 or target_id == gateway_id:
                continue
            target_name = _coerce_text(member.get("device_name", f"Device {target_id}"), f"Device {target_id}")
            expected[(gateway_id, target_id)] = {
                "name": f"Gateway Exclusive {cluster_name}: {gateway_name} -> {target_name}",
                "enabled": 1,
                "policy_type": GATEWAY_EXCLUSIVE_POLICY_TYPE,
                "source_device_id": gateway_id,
                "target_device_id": target_id,
                "priority": GATEWAY_EXCLUSIVE_POLICY_PRIORITY,
                "trigger_json": _coerce_json_object_text(
                    {
                        "cluster_db_id": cluster_db_id,
                        "cluster_id": cluster_key,
                        "cluster_name": cluster_name,
                    }
                ),
                "action_json": _coerce_json_object_text(
                    {
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


def _normalize_runtime_primary_device(
    conn: sqlite3.Connection,
    *,
    preferred_device_id: Optional[int] = None,
    deactivate_others: bool = False,
) -> Optional[int]:
    rows = conn.execute(
        """
        SELECT id, runtime_active, runtime_primary, enabled, system_key, device_class
          FROM device_profiles
      ORDER BY display_order ASC, id ASC
        """
    ).fetchall()
    if not rows:
        return None

    row_by_id = {int(row[0]): row for row in rows}
    def _row_enabled(row: sqlite3.Row | tuple[Any, ...]) -> bool:
        return int(row[3] or 0) == 1

    def _row_primary_candidate(row: sqlite3.Row | tuple[Any, ...]) -> bool:
        return _row_enabled(row) and _normalize_device_class(row[5] if len(row) > 5 else "tx_rx", "tx_rx") != "observer"

    active_ids = [int(row[0]) for row in rows if int(row[1] or 0) == 1 and _row_primary_candidate(row)]
    chosen_id: Optional[int] = None

    if preferred_device_id is not None:
        preferred_row = row_by_id.get(int(preferred_device_id))
        if preferred_row is not None and _row_primary_candidate(preferred_row):
            chosen_id = int(preferred_device_id)
            if int(preferred_row[1] or 0) != 1:
                conn.execute("UPDATE device_profiles SET runtime_active=1 WHERE id=?", (int(preferred_device_id),))
                conn.commit()
                active_ids = [*active_ids, int(preferred_device_id)]

    if chosen_id is None:
        primary_ids = [
            int(row[0])
            for row in rows
            if int(row[1] or 0) == 1 and int(row[2] or 0) == 1 and _row_primary_candidate(row)
        ]
        if primary_ids:
            chosen_id = primary_ids[0]

    if chosen_id is None and active_ids:
        chosen_id = active_ids[0]

    if chosen_id is None:
        for row in rows:
            if not _row_primary_candidate(row):
                continue
            if _coerce_text(row[4], "") == DEFAULT_DEVICE_SYSTEM_KEY:
                chosen_id = int(row[0])
                break

    if chosen_id is None:
        for row in rows:
            if _row_primary_candidate(row):
                chosen_id = int(row[0])
                break

    if chosen_id is None:
        active_ids = [int(row[0]) for row in rows if int(row[1] or 0) == 1 and _row_enabled(row)]
        if active_ids:
            chosen_id = active_ids[0]

    if chosen_id is None:
        for row in rows:
            if int(row[3] or 0) != 1:
                continue
            if _coerce_text(row[4], "") == DEFAULT_DEVICE_SYSTEM_KEY:
                chosen_id = int(row[0])
                break

    if chosen_id is None:
        chosen_id = int(rows[0][0])

    chosen_row = row_by_id.get(int(chosen_id))
    if chosen_row is not None and int(chosen_row[1] or 0) != 1:
        conn.execute("UPDATE device_profiles SET runtime_active=1 WHERE id=?", (int(chosen_id),))
        conn.commit()

    if deactivate_others:
        conn.execute(
            "UPDATE device_profiles SET runtime_active = CASE WHEN id=? THEN 1 ELSE 0 END, runtime_primary = CASE WHEN id=? THEN 1 ELSE 0 END",
            (int(chosen_id), int(chosen_id)),
        )
        conn.commit()
        return chosen_id

    conn.execute(
        "UPDATE device_profiles SET runtime_primary = CASE WHEN id=? THEN CASE WHEN runtime_active=1 THEN 1 ELSE 0 END ELSE 0 END",
        (int(chosen_id),),
    )
    conn.commit()
    return chosen_id


def mirror_legacy_settings_into_runtime_active_device(
    conn: sqlite3.Connection,
    settings_values: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    ensure_multi_radio_settings_schema(conn)
    active_id = _normalize_runtime_primary_device(conn)
    if active_id is None:
        return None
    existing = _device_profile_by_id(conn, int(active_id))
    if not existing:
        return None

    js8_instance_id = _coerce_optional_int(existing.get("js8_instance_id"))
    if js8_instance_id is not None:
        js8_existing = _js8_instance_by_id(conn, int(js8_instance_id)) or {}
        js8_updates = {
            "id": js8_instance_id,
            "system_key": js8_existing.get("system_key"),
            "name": js8_existing.get("name", DEFAULT_JS8_INSTANCE_NAME),
            "enabled": js8_existing.get("enabled", 1),
            "host": _settings_text(settings_values, "js8_host", "127.0.0.1") or "127.0.0.1",
            "port": _settings_int(settings_values, "js8_port", 2442),
            "offset_hz": _settings_int(settings_values, "js8_offset_hz", 0),
            "profile_path": _settings_text(settings_values, "js8_profile_path", ""),
            "directed_path": _settings_text(settings_values, "js8_directed_path", ""),
            "inbox_path": js8_existing.get("inbox_path", ""),
            "forms_path": _settings_text(settings_values, "js8_forms_path", ""),
            "install_path": _settings_text(settings_values, "path_js8call", ""),
            "spotter_launch_path": _settings_text(settings_values, "path_js8spotter", ""),
            "commstat_launch_path": _settings_text(settings_values, "path_commstat", ""),
        }
        _save_js8_instance_conn(conn, js8_updates)

    fast_light_config_id = _coerce_optional_int(existing.get("fast_light_config_id"))
    if fast_light_config_id is not None:
        fast_light_existing = _fast_light_config_by_id(conn, int(fast_light_config_id)) or {}
        _save_fast_light_config_conn(
            conn,
            {
                "id": fast_light_config_id,
                "system_key": fast_light_existing.get("system_key"),
                "name": fast_light_existing.get("name", DEFAULT_FAST_LIGHT_NAME),
                "enabled": fast_light_existing.get("enabled", 1),
                "flrig_path": _settings_text(settings_values, "path_flrig", ""),
                "flrig_host": _settings_text(settings_values, "flrig_host", "127.0.0.1") or "127.0.0.1",
                "flrig_port": _settings_int(settings_values, "flrig_port", 12345),
                "fldigi_path": _settings_text(settings_values, "path_fldigi", ""),
                "fldigi_host": _settings_text(settings_values, "fldigi_host", "") or _settings_text(
                    settings_values,
                    "flrig_host",
                    "127.0.0.1",
                ),
                "fldigi_port": _settings_int(settings_values, "fldigi_port", 7362),
                "fldigi_log_path": _settings_text(settings_values, "fldigi_log_path", ""),
                "fldigi_checkin_dir": _settings_text(settings_values, "fldigi_checkin_dir", ""),
            },
        )

    varac_node_id = _coerce_optional_int(existing.get("varac_node_id"))
    if varac_node_id is not None:
        varac_existing = _varac_node_by_id(conn, int(varac_node_id)) or {}
        message_paths = settings_values.get("message_paths", {}) if isinstance(settings_values, Mapping) else {}
        incoming_path = ""
        if isinstance(message_paths, Mapping):
            incoming_path = _coerce_text(message_paths.get("varac", ""), "")
        _save_varac_node_conn(
            conn,
            {
                "id": varac_node_id,
                "system_key": varac_existing.get("system_key"),
                "name": varac_existing.get("name", DEFAULT_VARAC_NODE_NAME),
                "enabled": varac_existing.get("enabled", 1),
                "install_path": _settings_text(settings_values, "varac_path", ""),
                "db_path": _settings_text(settings_values, "varac_db_path", ""),
                "ini_path": _settings_text(settings_values, "varac_ini_path", ""),
                "launch_cmd": _settings_text(settings_values, "varac_launch_cmd", ""),
                "incoming_path": incoming_path,
            },
        )

    updates = _legacy_device_projection_from_settings(settings_values)
    changed = any(existing.get(key) != value for key, value in updates.items())
    if not changed:
        return _device_profile_by_id(conn, int(active_id))

    assignments = ", ".join(f"{key}=?" for key in updates)
    params = [updates[key] for key in updates]
    params.extend([_utc_now_iso(), int(active_id)])
    conn.execute(
        f"UPDATE device_profiles SET {assignments}, updated_utc=? WHERE id=?",
        params,
    )
    conn.commit()
    return _device_profile_by_id(conn, int(active_id))


def project_runtime_active_device_to_legacy_settings(
    conn: sqlite3.Connection,
    device_profile_id: int,
) -> Dict[str, Any]:
    ensure_multi_radio_settings_schema(conn)
    device = _device_profile_by_id(conn, int(device_profile_id))
    if not device:
        raise KeyError(f"Unknown device profile id: {device_profile_id}")

    control_backend = _coerce_text(device.get("control_backend", "manual"), "manual").lower() or "manual"
    if control_backend not in SUPPORTED_RUNTIME_CONTROL_BACKENDS:
        raise ValueError(
            f"Unsupported runtime-active control backend for Phase B slice: {control_backend}"
        )

    updates = _legacy_settings_projection_from_device(device)
    cur = conn.execute("SELECT value FROM kv WHERE key='message_paths'")
    row = cur.fetchone()
    message_paths: Dict[str, Any] = {}
    if row and row[0] not in (None, ""):
        try:
            parsed = json.loads(row[0])
            if isinstance(parsed, dict):
                message_paths = dict(parsed)
        except Exception:
            message_paths = {}
    varac_incoming = _coerce_text(device.get("varac_incoming_path", ""), "")
    if varac_incoming:
        message_paths["varac"] = varac_incoming
        updates["message_paths"] = message_paths
    _write_kv_values(conn, updates)
    conn.commit()
    return updates


def _ensure_default_js8_instance(conn: sqlite3.Connection, settings_values: Mapping[str, Any]) -> int:
    cur = conn.execute("SELECT id FROM js8_instances WHERE system_key=?", (DEFAULT_JS8_INSTANCE_SYSTEM_KEY,))
    row = cur.fetchone()
    if row:
        return int(row[0])

    now_iso = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO js8_instances (
            system_key, name, enabled, host, port, offset_hz, profile_path, directed_path, inbox_path,
            forms_path, install_path, spotter_launch_path, commstat_launch_path, created_utc, updated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            DEFAULT_JS8_INSTANCE_SYSTEM_KEY,
            DEFAULT_JS8_INSTANCE_NAME,
            1,
            _settings_text(settings_values, "js8_host", "127.0.0.1") or "127.0.0.1",
            _settings_int(settings_values, "js8_port", 2442),
            _settings_int(settings_values, "js8_offset_hz", 0),
            _settings_text(settings_values, "js8_profile_path", ""),
            _settings_text(settings_values, "js8_directed_path", ""),
            "",
            _settings_text(settings_values, "js8_forms_path", ""),
            _settings_text(settings_values, "path_js8call", ""),
            _settings_text(settings_values, "path_js8spotter", ""),
            _settings_text(settings_values, "path_commstat", ""),
            now_iso,
            now_iso,
        ),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _ensure_default_fast_light_config(conn: sqlite3.Connection, settings_values: Mapping[str, Any]) -> int:
    cur = conn.execute("SELECT id FROM fast_light_configs WHERE system_key=?", (DEFAULT_FAST_LIGHT_SYSTEM_KEY,))
    row = cur.fetchone()
    if row:
        return int(row[0])

    now_iso = _utc_now_iso()
    flrig_host = _settings_text(settings_values, "flrig_host", "127.0.0.1") or "127.0.0.1"
    fldigi_host = _settings_text(settings_values, "fldigi_host", "") or flrig_host or "127.0.0.1"
    conn.execute(
        """
        INSERT INTO fast_light_configs (
            system_key, name, enabled, flrig_path, flrig_host, flrig_port, fldigi_path, fldigi_host,
            fldigi_port, fldigi_log_path, fldigi_checkin_dir, created_utc, updated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            DEFAULT_FAST_LIGHT_SYSTEM_KEY,
            DEFAULT_FAST_LIGHT_NAME,
            1,
            _settings_text(settings_values, "path_flrig", ""),
            flrig_host,
            _settings_int(settings_values, "flrig_port", 12345),
            _settings_text(settings_values, "path_fldigi", ""),
            fldigi_host,
            _settings_int(settings_values, "fldigi_port", 7362),
            _settings_text(settings_values, "fldigi_log_path", ""),
            _settings_text(settings_values, "fldigi_checkin_dir", ""),
            now_iso,
            now_iso,
        ),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _ensure_default_varac_node(conn: sqlite3.Connection, settings_values: Mapping[str, Any]) -> int:
    cur = conn.execute("SELECT id FROM varac_nodes WHERE system_key=?", (DEFAULT_VARAC_NODE_SYSTEM_KEY,))
    row = cur.fetchone()
    if row:
        return int(row[0])

    message_paths = settings_values.get("message_paths", {})
    incoming_path = ""
    if isinstance(message_paths, Mapping):
        incoming_path = _coerce_text(message_paths.get("varac", ""), "")
    now_iso = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO varac_nodes (
            system_key, name, enabled, install_path, db_path, ini_path, launch_cmd, incoming_path,
            created_utc, updated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            DEFAULT_VARAC_NODE_SYSTEM_KEY,
            DEFAULT_VARAC_NODE_NAME,
            1,
            _settings_text(settings_values, "varac_path", ""),
            _settings_text(settings_values, "varac_db_path", ""),
            _settings_text(settings_values, "varac_ini_path", ""),
            _settings_text(settings_values, "varac_launch_cmd", ""),
            incoming_path,
            now_iso,
            now_iso,
        ),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _ensure_default_device_profile(conn: sqlite3.Connection, settings_values: Mapping[str, Any]) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM device_profiles WHERE system_key=?", (DEFAULT_DEVICE_SYSTEM_KEY,))
    row = cur.fetchone()
    if row:
        return int(row[0])

    now_iso = _utc_now_iso()
    control_backend = _normalize_control_backend(settings_values)
    flrig_host = _settings_text(settings_values, "flrig_host", "127.0.0.1") or "127.0.0.1"
    fldigi_host = _settings_text(settings_values, "fldigi_host", "") or flrig_host or "127.0.0.1"
    js8_host = _settings_text(settings_values, "js8_host", "127.0.0.1") or "127.0.0.1"
    launch_path = ""
    if control_backend == "flrig":
        launch_path = _settings_text(settings_values, "path_flrig", "")
    elif control_backend == "js8call":
        launch_path = _settings_text(settings_values, "path_js8call", "")

    cur.execute(
        """
        INSERT INTO device_profiles (
            system_key, name, enabled, runtime_active, runtime_primary, display_order, device_class, deployment_mode,
            control_backend, rig_host, rig_port, flrig_host, flrig_port, fldigi_host,
            fldigi_port, fldigi_log_path, js8_host, js8_port, js8_profile_path, js8_directed_path, js8_inbox_path,
            varac_install_path, varac_db_path, varac_ini_path, varac_cluster_member_enabled,
            sdr_host, sdr_port, launch_enabled, launch_path, launch_cmd, working_dir,
            ptt_group, antenna_group, frontend_group, amplifier_group, notes,
            created_utc, updated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            DEFAULT_DEVICE_SYSTEM_KEY,
            DEFAULT_DEVICE_NAME,
            1,
            1,
            1,
            0,
            "tx_rx",
            "full",
            control_backend,
            _settings_text(settings_values, "rig_host", ""),
            _settings_int(settings_values, "rig_port", 4532) if control_backend == "rigctld" else None,
            flrig_host,
            _settings_int(settings_values, "flrig_port", 12345),
            fldigi_host,
            _settings_int(settings_values, "fldigi_port", 7362),
            _settings_text(settings_values, "fldigi_log_path", ""),
            js8_host,
            _settings_int(settings_values, "js8_port", 2442),
            _settings_text(settings_values, "js8_profile_path", ""),
            _settings_text(settings_values, "js8_directed_path", ""),
            "",
            _settings_text(settings_values, "varac_path", ""),
            _settings_text(settings_values, "varac_db_path", ""),
            _settings_text(settings_values, "varac_ini_path", ""),
            0,
            _settings_text(settings_values, "sdr_host", ""),
            _settings_int(settings_values, "sdr_port", 0) or None,
            1 if _settings_bool(settings_values, "launch_control_enabled", True) else 0,
            launch_path,
            _settings_text(settings_values, "varac_launch_cmd", ""),
            "",
            "default",
            "",
            "",
            "",
            "Seeded from legacy single-radio settings.",
            now_iso,
            now_iso,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _ensure_default_operating_profile(conn: sqlite3.Connection, settings_values: Mapping[str, Any]) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM operating_profiles WHERE system_key=?", (DEFAULT_OPERATING_SYSTEM_KEY,))
    row = cur.fetchone()
    if row:
        return int(row[0])

    now_iso = _utc_now_iso()
    cur.execute(
        """
        INSERT INTO operating_profiles (
            system_key, name, enabled, description, scheduler_enabled, scheduler_mode,
            preferred_antenna_group, preferred_band_set_json, preferred_mode_set_json,
            allow_auto_qsy, allow_auto_band_change, allow_profile_swap, prompt_only,
            use_messages, use_map, use_background_ingest, use_launch_control,
            use_net_control_tabs, created_utc, updated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            DEFAULT_OPERATING_SYSTEM_KEY,
            DEFAULT_OPERATING_NAME,
            1,
            "Seeded from legacy single-radio settings.",
            1 if _settings_bool(settings_values, "use_scheduler", True) else 0,
            "full",
            "",
            json.dumps([]),
            json.dumps([]),
            0,
            0,
            0,
            1,
            1,
            1,
            1,
            1 if _settings_bool(settings_values, "launch_control_enabled", True) else 0,
            1,
            now_iso,
            now_iso,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _ensure_default_assignment(conn: sqlite3.Connection, device_profile_id: int, operating_profile_id: int) -> None:
    _normalize_effective_assignments(conn, device_profile_id=int(device_profile_id))
    # Preserve an operator-selected effective assignment across app restarts.
    if _effective_assignment_for_device(conn, int(device_profile_id)) is not None:
        return
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
          FROM operating_profile_assignments
         WHERE device_profile_id=?
           AND operating_profile_id=?
           AND assignment_state='active'
        """,
        (device_profile_id, operating_profile_id),
    )
    if cur.fetchone():
        return

    now_iso = _utc_now_iso()
    cur.execute(
        """
        INSERT INTO operating_profile_assignments (
            device_profile_id, operating_profile_id, assignment_state, starts_utc,
            ends_utc, reason, created_by, created_utc, updated_utc
        ) VALUES (?, ?, 'active', ?, NULL, ?, ?, ?, ?)
        """,
        (
            int(device_profile_id),
            int(operating_profile_id),
            now_iso,
            "Seeded from legacy single-radio settings.",
            "migration",
            now_iso,
            now_iso,
        ),
    )
    conn.commit()


def _ensure_device_has_active_assignment(conn: sqlite3.Connection, device_profile_id: int) -> None:
    if _effective_assignment_for_device(conn, int(device_profile_id)) is not None:
        return
    cur = conn.execute("SELECT id FROM operating_profiles WHERE system_key=?", (DEFAULT_OPERATING_SYSTEM_KEY,))
    row = cur.fetchone()
    if row is None:
        operating_profile_id = _ensure_default_operating_profile(conn, {})
    else:
        operating_profile_id = int(row[0])
    _ensure_default_assignment(conn, int(device_profile_id), int(operating_profile_id))


def ensure_default_multi_radio_records(conn: sqlite3.Connection, settings_values: Mapping[str, Any]) -> None:
    ensure_multi_radio_settings_schema(conn)
    device_id = _ensure_default_device_profile(conn, settings_values)
    js8_instance_id = _ensure_default_js8_instance(conn, settings_values)
    fast_light_config_id = _ensure_default_fast_light_config(conn, settings_values)
    varac_node_id = _ensure_default_varac_node(conn, settings_values)
    operating_id = _ensure_default_operating_profile(conn, settings_values)
    _ensure_default_assignment(conn, device_id, operating_id)
    conn.execute(
        """
        UPDATE device_profiles
           SET js8_instance_id=COALESCE(js8_instance_id, ?),
               fast_light_config_id=COALESCE(fast_light_config_id, ?),
               varac_node_id=COALESCE(varac_node_id, ?),
               updated_utc=?
         WHERE id=?
        """,
        (int(js8_instance_id), int(fast_light_config_id), int(varac_node_id), _utc_now_iso(), int(device_id)),
    )
    conn.commit()
    # The seeded default device is only a startup fallback. If an operator has
    # already selected a different valid primary device, restart must preserve it.
    _normalize_runtime_primary_device(conn, deactivate_others=False)
    log.debug(
        "MultiRadioStore: ensured default device_profile=%s operating_profile=%s assignment and default app-instance records.",
        device_id,
        operating_id,
    )


class MultiRadioStore:
    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = Path(db_path) if db_path else settings_db_path()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_device_profile(self, device_profile_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            return _device_profile_by_id(conn, int(device_profile_id))

    def get_operating_profile(self, operating_profile_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _operating_profile_by_id(conn, int(operating_profile_id))

    def get_js8_instance(self, js8_instance_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _js8_instance_by_id(conn, int(js8_instance_id))

    def get_fast_light_config(self, fast_light_config_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _fast_light_config_by_id(conn, int(fast_light_config_id))

    def get_varac_node(self, varac_node_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _varac_node_by_id(conn, int(varac_node_id))

    def get_runtime_active_device_profile(self) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            active_id = _normalize_runtime_primary_device(conn)
            if active_id is None:
                return None
            return _device_profile_by_id(conn, int(active_id))

    def get_runtime_primary_device_profile(self) -> Optional[Dict[str, Any]]:
        return self.get_runtime_active_device_profile()

    def list_runtime_active_device_profiles(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            _normalize_runtime_primary_device(conn)
            return _runtime_active_device_profiles(conn)

    def save_device_profile(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(values)
        requested_id = _coerce_optional_int(payload.get("id"))
        wants_activation = bool(_coerce_bool_int(payload.get("runtime_active"), False))

        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            existing = _device_profile_by_id(conn, int(requested_id)) if requested_id is not None else None
            now_iso = _utc_now_iso()

            if existing:
                system_key_default = existing.get("system_key", DEFAULT_DEVICE_SYSTEM_KEY)
                name_default = existing.get("name", DEFAULT_DEVICE_NAME)
                display_order_default = _coerce_int(existing.get("display_order"), 0)
            else:
                system_key_default = DEFAULT_DEVICE_SYSTEM_KEY
                name_default = "Device Profile"
                display_order_default = _coerce_int(
                    conn.execute("SELECT COALESCE(MAX(display_order), -1) + 1 FROM device_profiles").fetchone()[0],
                    0,
                )

            name = _coerce_text(payload.get("name", name_default), name_default) or name_default
            requested_key = payload.get("system_key", system_key_default if existing else name)
            system_key = _next_device_system_key(conn, requested_key, exclude_id=requested_id)

            flrig_host = _coerce_text(
                payload.get("flrig_host", (existing or {}).get("flrig_host", "127.0.0.1")),
                "127.0.0.1",
            ) or "127.0.0.1"
            fldigi_host = _coerce_text(
                payload.get("fldigi_host", (existing or {}).get("fldigi_host", "")),
                "",
            ) or flrig_host or "127.0.0.1"
            js8_host = _coerce_text(
                payload.get("js8_host", (existing or {}).get("js8_host", "127.0.0.1")),
                "127.0.0.1",
            ) or "127.0.0.1"
            control_backend = _coerce_text(
                payload.get("control_backend", (existing or {}).get("control_backend", "flrig")),
                "flrig",
            ).lower() or "flrig"
            device_class = _normalize_device_class(
                payload.get("device_class", (existing or {}).get("device_class", "tx_rx")),
                "tx_rx",
            )
            if wants_activation and device_class == "observer":
                raise ValueError("Observer / SDR device profiles can be active, but they cannot become the primary compatibility device.")
            if wants_activation and control_backend not in SUPPORTED_RUNTIME_CONTROL_BACKENDS:
                raise ValueError(
                    f"Cannot make a device profile runtime-active until backend support exists: {control_backend}"
                )
            if existing and int(existing.get("runtime_active", 0) or 0) == 1:
                if control_backend not in SUPPORTED_RUNTIME_CONTROL_BACKENDS:
                    raise ValueError(
                        "Cannot change the runtime-active device profile to an unsupported control backend."
                    )
                if not bool(_coerce_bool_int(payload.get("enabled", existing.get("enabled", 1)), True)):
                    raise ValueError("The runtime-active device profile cannot be disabled.")
            if existing and int(existing.get("runtime_primary", 0) or 0) == 1 and device_class == "observer":
                raise ValueError("Observer / SDR device profiles cannot become the primary compatibility device.")
            if device_class == "observer":
                membership_device_id = int(requested_id) if requested_id is not None else 0
                if membership_device_id > 0 and _device_has_varac_cluster_membership(conn, membership_device_id):
                    raise ValueError("Observer / SDR device profiles cannot participate in VarAC clusters.")

            js8_instance_id = _coerce_optional_int(payload.get("js8_instance_id", (existing or {}).get("js8_instance_id")))
            if js8_instance_id is not None and not _js8_instance_by_id(conn, int(js8_instance_id)):
                raise KeyError(f"Unknown JS8 instance id: {js8_instance_id}")
            fast_light_config_id = _coerce_optional_int(
                payload.get("fast_light_config_id", (existing or {}).get("fast_light_config_id"))
            )
            if fast_light_config_id is not None and not _fast_light_config_by_id(conn, int(fast_light_config_id)):
                raise KeyError(f"Unknown Fast Light config id: {fast_light_config_id}")
            varac_node_id = _coerce_optional_int(payload.get("varac_node_id", (existing or {}).get("varac_node_id")))
            if varac_node_id is not None and not _varac_node_by_id(conn, int(varac_node_id)):
                raise KeyError(f"Unknown VarAC node id: {varac_node_id}")

            record: Dict[str, Any] = {
                "system_key": system_key,
                "name": name,
                "enabled": _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True),
                "runtime_active": _coerce_bool_int((existing or {}).get("runtime_active", 0), False),
                "runtime_primary": _coerce_bool_int((existing or {}).get("runtime_primary", 0), False),
                "display_order": _coerce_int(
                    payload.get("display_order", (existing or {}).get("display_order", display_order_default)),
                    display_order_default,
                ),
                "device_class": device_class,
                "deployment_mode": _coerce_text(
                    payload.get("deployment_mode", (existing or {}).get("deployment_mode", "full")),
                    "full",
                ) or "full",
                "control_backend": control_backend,
                "rig_host": _coerce_text(payload.get("rig_host", (existing or {}).get("rig_host", "")), ""),
                "rig_port": _coerce_optional_int(payload.get("rig_port", (existing or {}).get("rig_port"))),
                "flrig_host": flrig_host,
                "flrig_port": _coerce_optional_int(
                    payload.get("flrig_port", (existing or {}).get("flrig_port")),
                    12345,
                ),
                "fldigi_host": fldigi_host,
                "fldigi_port": _coerce_optional_int(
                    payload.get("fldigi_port", (existing or {}).get("fldigi_port")),
                    7362,
                ),
                "fldigi_log_path": _coerce_text(
                    payload.get("fldigi_log_path", (existing or {}).get("fldigi_log_path", "")),
                    "",
                ),
                "js8_host": js8_host,
                "js8_port": _coerce_optional_int(
                    payload.get("js8_port", (existing or {}).get("js8_port")),
                    2442,
                ),
                "js8_instance_id": js8_instance_id,
                "js8_profile_path": _coerce_text(
                    payload.get("js8_profile_path", (existing or {}).get("js8_profile_path", "")),
                    "",
                ),
                "js8_directed_path": _coerce_text(
                    payload.get("js8_directed_path", (existing or {}).get("js8_directed_path", "")),
                    "",
                ),
                "js8_inbox_path": _coerce_text(
                    payload.get("js8_inbox_path", (existing or {}).get("js8_inbox_path", "")),
                    "",
                ),
                "fast_light_config_id": fast_light_config_id,
                "varac_install_path": _coerce_text(
                    payload.get("varac_install_path", (existing or {}).get("varac_install_path", "")),
                    "",
                ),
                "varac_db_path": _coerce_text(
                    payload.get("varac_db_path", (existing or {}).get("varac_db_path", "")),
                    "",
                ),
                "varac_ini_path": _coerce_text(
                    payload.get("varac_ini_path", (existing or {}).get("varac_ini_path", "")),
                    "",
                ),
                "varac_node_id": varac_node_id,
                "varac_cluster_member_enabled": _coerce_bool_int(
                    payload.get(
                        "varac_cluster_member_enabled",
                        (existing or {}).get("varac_cluster_member_enabled", 0),
                    ),
                    False,
                ),
                "sdr_host": _coerce_text(payload.get("sdr_host", (existing or {}).get("sdr_host", "")), ""),
                "sdr_port": _coerce_optional_int(payload.get("sdr_port", (existing or {}).get("sdr_port"))),
                "launch_enabled": _coerce_bool_int(
                    payload.get("launch_enabled", (existing or {}).get("launch_enabled", 1)),
                    True,
                ),
                "launch_path": _coerce_text(payload.get("launch_path", (existing or {}).get("launch_path", "")), ""),
                "launch_cmd": _coerce_text(payload.get("launch_cmd", (existing or {}).get("launch_cmd", "")), ""),
                "working_dir": _coerce_text(payload.get("working_dir", (existing or {}).get("working_dir", "")), ""),
                "ptt_group": normalize_ptt_group(payload.get("ptt_group", (existing or {}).get("ptt_group", ""))),
                "antenna_group": normalize_resource_group(
                    payload.get("antenna_group", (existing or {}).get("antenna_group", "")),
                ),
                "frontend_group": normalize_resource_group(
                    payload.get("frontend_group", (existing or {}).get("frontend_group", "")),
                ),
                "amplifier_group": normalize_resource_group(
                    payload.get("amplifier_group", (existing or {}).get("amplifier_group", "")),
                ),
                "notes": _coerce_text(payload.get("notes", (existing or {}).get("notes", "")), ""),
            }

            editable_columns = [
                "system_key",
                "name",
                "enabled",
                "runtime_active",
                "runtime_primary",
                "display_order",
                "device_class",
                "deployment_mode",
                "control_backend",
                "rig_host",
                "rig_port",
                "flrig_host",
                "flrig_port",
                "fldigi_host",
                "fldigi_port",
                "fldigi_log_path",
                "js8_host",
                "js8_port",
                "js8_instance_id",
                "js8_profile_path",
                "js8_directed_path",
                "js8_inbox_path",
                "fast_light_config_id",
                "varac_install_path",
                "varac_db_path",
                "varac_ini_path",
                "varac_node_id",
                "varac_cluster_member_enabled",
                "sdr_host",
                "sdr_port",
                "launch_enabled",
                "launch_path",
                "launch_cmd",
                "working_dir",
                "ptt_group",
                "antenna_group",
                "frontend_group",
                "amplifier_group",
                "notes",
            ]

            if existing:
                assignments = ", ".join(f"{column}=?" for column in editable_columns)
                params = [record[column] for column in editable_columns]
                params.extend([now_iso, int(requested_id)])
                conn.execute(
                    f"UPDATE device_profiles SET {assignments}, updated_utc=? WHERE id=?",
                    params,
                )
                row_id = int(requested_id)
            else:
                insert_columns = editable_columns + ["created_utc", "updated_utc"]
                placeholders = ", ".join(["?"] * len(insert_columns))
                conn.execute(
                    f"INSERT INTO device_profiles ({', '.join(insert_columns)}) VALUES ({placeholders})",
                    [record[column] for column in editable_columns] + [now_iso, now_iso],
                )
                row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

            conn.commit()

            if wants_activation:
                self._set_runtime_active_device_conn(conn, row_id)
            else:
                _normalize_runtime_primary_device(conn)
            _sync_derived_coordination_policies_conn(conn)

            return _device_profile_by_id(conn, row_id) or {}

    def set_runtime_active_device_profile(self, device_profile_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            active_swap = _active_profile_swap_policy_conn(conn)
            if active_swap is not None:
                current_target_id = int(active_swap.get("target_device_id", 0) or 0)
                if int(device_profile_id) != current_target_id:
                    raise ValueError("Restore the active temporary swap before changing the primary device profile.")
            return self._set_runtime_active_device_conn(conn, int(device_profile_id))

    def set_runtime_primary_device_profile(self, device_profile_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            active_swap = _active_profile_swap_policy_conn(conn)
            if active_swap is not None:
                current_target_id = int(active_swap.get("target_device_id", 0) or 0)
                if int(device_profile_id) != current_target_id:
                    raise ValueError("Restore the active temporary swap before changing the primary device profile.")
            return self._set_runtime_primary_device_conn(conn, int(device_profile_id))

    def set_device_profile_runtime_active(self, device_profile_id: int, active: bool) -> Dict[str, Any]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            device = _device_profile_by_id(conn, int(device_profile_id))
            if not device:
                raise KeyError(f"Unknown device profile id: {device_profile_id}")
            active_swap = _active_profile_swap_policy_conn(conn)
            if active_swap is not None:
                source_id = int(active_swap.get("source_device_id", 0) or 0)
                target_id = int(active_swap.get("target_device_id", 0) or 0)
                if int(device_profile_id) in {source_id, target_id}:
                    raise ValueError("Restore the active temporary swap before changing runtime activation on the swap source/target devices.")
            if active:
                control_backend = _coerce_text(device.get("control_backend", "manual"), "manual").lower() or "manual"
                if control_backend not in SUPPORTED_RUNTIME_CONTROL_BACKENDS:
                    raise ValueError(
                        f"Cannot activate device profile {device_profile_id} until backend support exists: {control_backend}"
                    )
                conn.execute(
                    "UPDATE device_profiles SET runtime_active=1, runtime_primary=runtime_primary WHERE id=?",
                    (int(device_profile_id),),
                )
                _ensure_device_has_active_assignment(conn, int(device_profile_id))
                primary_id = _normalize_runtime_primary_device(conn)
                if primary_id == int(device_profile_id):
                    project_runtime_active_device_to_legacy_settings(conn, int(device_profile_id))
                return _device_profile_by_id(conn, int(device_profile_id)) or {}

            if int(device.get("runtime_primary", 0) or 0) == 1:
                raise ValueError("Cannot deactivate the primary runtime device profile. Make another device primary first.")

            active_profiles = _runtime_active_device_profiles(conn)
            if len(active_profiles) <= 1:
                raise ValueError("At least one runtime-active device profile must remain enabled.")

            conn.execute("UPDATE device_profiles SET runtime_active=0, runtime_primary=0 WHERE id=?", (int(device_profile_id),))
            conn.commit()
            _normalize_runtime_primary_device(conn)
            return _device_profile_by_id(conn, int(device_profile_id)) or {}

    def delete_device_profile(self, device_profile_id: int) -> None:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            device = _device_profile_by_id(conn, int(device_profile_id))
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

    def sync_runtime_active_device_to_legacy_settings(self, device_profile_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            return project_runtime_active_device_to_legacy_settings(conn, int(device_profile_id))

    def _set_runtime_active_device_conn(self, conn: sqlite3.Connection, device_profile_id: int) -> Dict[str, Any]:
        return self._set_runtime_primary_device_conn(conn, int(device_profile_id), deactivate_others=True)

    def _set_runtime_primary_device_conn(
        self,
        conn: sqlite3.Connection,
        device_profile_id: int,
        *,
        deactivate_others: bool = False,
    ) -> Dict[str, Any]:
        device = _device_profile_by_id(conn, int(device_profile_id))
        if not device:
            raise KeyError(f"Unknown device profile id: {device_profile_id}")
        if _is_observer_device_class(device):
            raise ValueError("Observer / SDR device profiles cannot become the primary compatibility device.")
        control_backend = _coerce_text(device.get("control_backend", "manual"), "manual").lower() or "manual"
        if control_backend not in SUPPORTED_RUNTIME_CONTROL_BACKENDS:
            raise ValueError(
                f"Cannot make device profile {device_profile_id} runtime-active until backend support exists: "
                f"{control_backend}"
            )
        conn.execute("UPDATE device_profiles SET runtime_active=1 WHERE id=?", (int(device_profile_id),))
        conn.commit()
        _ensure_device_has_active_assignment(conn, int(device_profile_id))
        _normalize_runtime_primary_device(
            conn,
            preferred_device_id=int(device_profile_id),
            deactivate_others=deactivate_others,
        )
        project_runtime_active_device_to_legacy_settings(conn, int(device_profile_id))
        return _device_profile_by_id(conn, int(device_profile_id)) or {}

    def list_device_profiles(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            _normalize_runtime_primary_device(conn)
            rows = conn.execute(
                """
                SELECT *
                  FROM device_profiles
                ORDER BY display_order ASC, id ASC
                """
            ).fetchall()
            return [_resolve_device_profile_links_conn(conn, dict(row)) for row in rows]

    def list_js8_instances(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _list_js8_instances_conn(conn)

    def save_js8_instance(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _save_js8_instance_conn(conn, values)

    def delete_js8_instance(self, js8_instance_id: int) -> None:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            row = _js8_instance_by_id(conn, int(js8_instance_id))
            if not row:
                raise KeyError(f"Unknown JS8 instance id: {js8_instance_id}")
            if _coerce_text(row.get("system_key", ""), "") == DEFAULT_JS8_INSTANCE_SYSTEM_KEY:
                raise ValueError("Cannot delete the default JS8 instance.")
            count = conn.execute(
                "SELECT COUNT(*) FROM device_profiles WHERE js8_instance_id=?",
                (int(js8_instance_id),),
            ).fetchone()[0]
            if int(count or 0) > 0:
                raise ValueError("Cannot delete a JS8 instance that is still assigned to one or more device profiles.")
            conn.execute("DELETE FROM js8_instances WHERE id=?", (int(js8_instance_id),))
            conn.commit()

    def list_fast_light_configs(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _list_fast_light_configs_conn(conn)

    def save_fast_light_config(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _save_fast_light_config_conn(conn, values)

    def delete_fast_light_config(self, fast_light_config_id: int) -> None:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            row = _fast_light_config_by_id(conn, int(fast_light_config_id))
            if not row:
                raise KeyError(f"Unknown Fast Light config id: {fast_light_config_id}")
            if _coerce_text(row.get("system_key", ""), "") == DEFAULT_FAST_LIGHT_SYSTEM_KEY:
                raise ValueError("Cannot delete the default Fast Light config.")
            count = conn.execute(
                "SELECT COUNT(*) FROM device_profiles WHERE fast_light_config_id=?",
                (int(fast_light_config_id),),
            ).fetchone()[0]
            if int(count or 0) > 0:
                raise ValueError("Cannot delete a Fast Light config that is still assigned to one or more device profiles.")
            conn.execute("DELETE FROM fast_light_configs WHERE id=?", (int(fast_light_config_id),))
            conn.commit()

    def list_varac_nodes(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _list_varac_nodes_conn(conn)

    def save_varac_node(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _save_varac_node_conn(conn, values)

    def delete_varac_node(self, varac_node_id: int) -> None:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            row = _varac_node_by_id(conn, int(varac_node_id))
            if not row:
                raise KeyError(f"Unknown VarAC node id: {varac_node_id}")
            if _coerce_text(row.get("system_key", ""), "") == DEFAULT_VARAC_NODE_SYSTEM_KEY:
                raise ValueError("Cannot delete the default VarAC node.")
            count = conn.execute(
                "SELECT COUNT(*) FROM device_profiles WHERE varac_node_id=?",
                (int(varac_node_id),),
            ).fetchone()[0]
            if int(count or 0) > 0:
                raise ValueError("Cannot delete a VarAC node that is still assigned to one or more device profiles.")
            conn.execute("DELETE FROM varac_nodes WHERE id=?", (int(varac_node_id),))
            conn.commit()

    def list_varac_clusters(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _list_varac_clusters_conn(conn)

    def list_varac_cluster_members(
        self,
        *,
        cluster_id: Optional[int] = None,
        device_profile_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
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
            device = _device_profile_by_id(conn, int(device_profile_id))
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
                duplicate_device = _device_profile_by_id(conn, duplicate_id)
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
                params.append(_normalize_coordination_policy_type(normalized_type, normalized_type))
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

    def save_operating_profile(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(values or {})
        requested_id = _coerce_optional_int(payload.get("id"))

        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            existing = _operating_profile_by_id(conn, int(requested_id)) if requested_id is not None else None
            now_iso = _utc_now_iso()
            active_swap = _active_profile_swap_policy_conn(conn)

            if existing:
                system_key_default = existing.get("system_key", DEFAULT_OPERATING_SYSTEM_KEY)
                name_default = existing.get("name", DEFAULT_OPERATING_NAME)
            else:
                system_key_default = DEFAULT_OPERATING_SYSTEM_KEY
                name_default = "Operating Profile"

            name = _coerce_text(payload.get("name", name_default), name_default) or name_default
            requested_key = payload.get("system_key", system_key_default if existing else name)
            system_key = _next_operating_system_key(conn, requested_key, exclude_id=requested_id)

            enabled = _coerce_bool_int(payload.get("enabled", (existing or {}).get("enabled", 1)), True)
            if existing and enabled != 1:
                placeholders = ", ".join("?" for _ in EFFECTIVE_ASSIGNMENT_STATES)
                cur = conn.execute(
                    f"""
                    SELECT id
                      FROM operating_profile_assignments
                     WHERE operating_profile_id=?
                       AND assignment_state IN ({placeholders})
                     LIMIT 1
                    """,
                    (int(requested_id), *tuple(EFFECTIVE_ASSIGNMENT_STATES)),
                )
                if cur.fetchone() is not None:
                    raise ValueError("Cannot disable an operating profile while it is assigned to a device.")
                if active_swap is not None:
                    restore_target = dict((active_swap.get("action") or {}).get("restore_target_assignment") or {})
                    restore_target_id = restore_target.get("operating_profile_id")
                    if restore_target_id not in (None, "") and int(restore_target_id) == int(requested_id):
                        raise ValueError(
                            "Cannot disable this operating profile while it is captured as the restore target for an active temporary swap."
                        )

            record: Dict[str, Any] = {
                "system_key": system_key,
                "name": name,
                "enabled": enabled,
                "description": _coerce_text(payload.get("description", (existing or {}).get("description", "")), ""),
                "scheduler_enabled": _coerce_bool_int(
                    payload.get("scheduler_enabled", (existing or {}).get("scheduler_enabled", 1)),
                    True,
                ),
                "scheduler_mode": _normalize_scheduler_mode(
                    payload.get("scheduler_mode", (existing or {}).get("scheduler_mode", "full")),
                    "full",
                ),
                "preferred_antenna_group": _coerce_text(
                    payload.get("preferred_antenna_group", (existing or {}).get("preferred_antenna_group", "")),
                    "",
                ),
                "preferred_band_set_json": _coerce_json_string_list(
                    payload.get(
                        "preferred_band_set_json",
                        payload.get("preferred_band_set", (existing or {}).get("preferred_band_set_json", "[]")),
                    )
                ),
                "preferred_mode_set_json": _coerce_json_string_list(
                    payload.get(
                        "preferred_mode_set_json",
                        payload.get("preferred_mode_set", (existing or {}).get("preferred_mode_set_json", "[]")),
                    )
                ),
                "allow_auto_qsy": _coerce_bool_int(
                    payload.get("allow_auto_qsy", (existing or {}).get("allow_auto_qsy", 0)),
                    False,
                ),
                "allow_auto_band_change": _coerce_bool_int(
                    payload.get("allow_auto_band_change", (existing or {}).get("allow_auto_band_change", 0)),
                    False,
                ),
                "allow_profile_swap": _coerce_bool_int(
                    payload.get("allow_profile_swap", (existing or {}).get("allow_profile_swap", 0)),
                    False,
                ),
                "prompt_only": _coerce_bool_int(
                    payload.get("prompt_only", (existing or {}).get("prompt_only", 1)),
                    True,
                ),
                "use_messages": _coerce_bool_int(
                    payload.get("use_messages", (existing or {}).get("use_messages", 1)),
                    True,
                ),
                "use_map": _coerce_bool_int(
                    payload.get("use_map", (existing or {}).get("use_map", 1)),
                    True,
                ),
                "use_background_ingest": _coerce_bool_int(
                    payload.get("use_background_ingest", (existing or {}).get("use_background_ingest", 1)),
                    True,
                ),
                "use_launch_control": _coerce_bool_int(
                    payload.get("use_launch_control", (existing or {}).get("use_launch_control", 1)),
                    True,
                ),
                "use_net_control_tabs": _coerce_bool_int(
                    payload.get("use_net_control_tabs", (existing or {}).get("use_net_control_tabs", 1)),
                    True,
                ),
            }

            editable_columns = [
                "system_key",
                "name",
                "enabled",
                "description",
                "scheduler_enabled",
                "scheduler_mode",
                "preferred_antenna_group",
                "preferred_band_set_json",
                "preferred_mode_set_json",
                "allow_auto_qsy",
                "allow_auto_band_change",
                "allow_profile_swap",
                "prompt_only",
                "use_messages",
                "use_map",
                "use_background_ingest",
                "use_launch_control",
                "use_net_control_tabs",
            ]

            if existing:
                assignments = ", ".join(f"{column}=?" for column in editable_columns)
                params = [record[column] for column in editable_columns]
                params.extend([now_iso, int(requested_id)])
                conn.execute(
                    f"UPDATE operating_profiles SET {assignments}, updated_utc=? WHERE id=?",
                    params,
                )
                row_id = int(requested_id)
            else:
                insert_columns = editable_columns + ["created_utc", "updated_utc"]
                placeholders = ", ".join(["?"] * len(insert_columns))
                conn.execute(
                    f"INSERT INTO operating_profiles ({', '.join(insert_columns)}) VALUES ({placeholders})",
                    [record[column] for column in editable_columns] + [now_iso, now_iso],
                )
                row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

            conn.commit()
            return _operating_profile_by_id(conn, row_id) or {}

    def list_operating_profiles(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            rows = conn.execute(
                """
                SELECT *
                  FROM operating_profiles
              ORDER BY id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def delete_operating_profile(self, operating_profile_id: int) -> None:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            profile = _operating_profile_by_id(conn, int(operating_profile_id))
            if not profile:
                raise KeyError(f"Unknown operating profile id: {operating_profile_id}")
            if _coerce_text(profile.get("system_key", ""), "") == DEFAULT_OPERATING_SYSTEM_KEY:
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
            cur = conn.execute(
                f"""
                SELECT id
                  FROM operating_profile_assignments
                 WHERE operating_profile_id=?
                   AND assignment_state IN ({placeholders})
                 LIMIT 1
                """,
                (int(operating_profile_id), *tuple(EFFECTIVE_ASSIGNMENT_STATES)),
            )
            if cur.fetchone() is not None:
                raise ValueError("Cannot delete an operating profile while it is assigned to a device.")
            conn.execute("DELETE FROM operating_profile_assignments WHERE operating_profile_id=?", (int(operating_profile_id),))
            conn.execute("DELETE FROM operating_profiles WHERE id=?", (int(operating_profile_id),))
            conn.commit()

    def get_effective_assignment_for_device(self, device_profile_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            return _effective_assignment_for_device(conn, int(device_profile_id))

    def list_effective_assignments(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            _normalize_effective_assignments(conn)
            rows = conn.execute(
                """
                SELECT *
                  FROM operating_profile_assignments
                 WHERE assignment_state IN ('active', 'temporary_override')
              ORDER BY device_profile_id ASC, id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def list_assignments(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            _normalize_effective_assignments(conn)
            rows = conn.execute(
                """
                SELECT *
                  FROM operating_profile_assignments
              ORDER BY id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

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
            ensure_multi_radio_settings_schema(conn)
            if _active_profile_swap_policy_conn(conn) is not None:
                raise ValueError("A temporary profile swap is already active. Restore it before starting another swap.")

            source_id = int(_normalize_runtime_primary_device(conn) or 0)
            if source_id <= 0:
                raise ValueError("A primary device profile is required before starting a temporary swap.")
            if int(target_device_profile_id) == source_id:
                raise ValueError("Select a different active device profile as the temporary swap target.")

            source_row = _device_profile_by_id(conn, source_id)
            target_row = _device_profile_by_id(conn, int(target_device_profile_id))
            if not source_row:
                raise ValueError("The current primary device profile could not be resolved.")
            if not target_row:
                raise KeyError(f"Unknown target device profile id: {target_device_profile_id}")
            if int(target_row.get("enabled", 1) or 0) != 1:
                raise ValueError("The selected temporary swap target is disabled.")
            if int(target_row.get("runtime_active", 0) or 0) != 1:
                raise ValueError("The selected temporary swap target must already be runtime-active.")
            if _is_observer_device_class(target_row):
                raise ValueError("Observer / SDR device profiles cannot be used as temporary-swap targets.")

            _ensure_device_has_active_assignment(conn, source_id)
            _ensure_device_has_active_assignment(conn, int(target_device_profile_id))
            source_assignment = _effective_assignment_for_device(conn, source_id)
            target_assignment = _effective_assignment_for_device(conn, int(target_device_profile_id))
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
                source_operating_profile = _operating_profile_by_id(conn, int(source_assignment.get("operating_profile_id", 0) or 0))
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

            self._set_runtime_primary_device_conn(conn, int(target_device_profile_id))

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
            ensure_multi_radio_settings_schema(conn)
            policy = _active_profile_swap_policy_conn(conn)
            if policy is None:
                raise ValueError("No temporary profile swap is currently active.")

            source_id = int(policy.get("source_device_id", 0) or 0)
            target_id = int(policy.get("target_device_id", 0) or 0)
            source_row = _device_profile_by_id(conn, source_id)
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

            self._set_runtime_primary_device_conn(conn, source_id)

            updated_action = dict(action)
            updated_action["restored_utc"] = _utc_now_iso()
            updated_action["restore_reason"] = _coerce_text(reason, "")
            conn.execute(
                """
                UPDATE station_coordination_policies
                   SET enabled=0, action_json=?, updated_utc=?
                 WHERE id=?
                """,
                (
                    _coerce_json_object_text(updated_action),
                    _utc_now_iso(),
                    int(policy.get("id", 0) or 0),
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM station_coordination_policies WHERE id=?",
                (int(policy.get("id", 0) or 0),),
            ).fetchone()
            return _enrich_profile_swap_policy(conn, _coordination_policy_from_row(row) if row is not None else None) or {}
