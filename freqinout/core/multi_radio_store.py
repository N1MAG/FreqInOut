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
SUPPORTED_RUNTIME_CONTROL_BACKENDS = frozenset({"flrig", "js8call", "manual", "rigctld"})
EFFECTIVE_ASSIGNMENT_STATES = frozenset({"active", "temporary_override"})
SUPPORTED_ASSIGNMENT_STATES = frozenset({"active", "temporary_override", "scheduled", "inactive", "superseded", "expired"})
SUPPORTED_SCHEDULER_MODES = frozenset({"full", "simple"})


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
            js8_profile_path TEXT,
            varac_install_path TEXT,
            varac_db_path TEXT,
            varac_ini_path TEXT,
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
            "js8_profile_path": "TEXT",
            "varac_install_path": "TEXT",
            "varac_db_path": "TEXT",
            "varac_ini_path": "TEXT",
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
        "js8_host": js8_host,
        "js8_port": _settings_int(settings_values, "js8_port", 2442),
        "js8_profile_path": _settings_text(settings_values, "js8_profile_path", ""),
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
        "js8_host": js8_host,
        "js8_port": _coerce_optional_int(device_profile.get("js8_port"), 2442),
        "js8_profile_path": _coerce_text(device_profile.get("js8_profile_path", ""), ""),
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
    return updates


def _write_kv_values(conn: sqlite3.Connection, values: Mapping[str, Any]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
        [(str(key), json.dumps(value)) for key, value in values.items()],
    )


def _device_profile_by_id(conn: sqlite3.Connection, device_profile_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM device_profiles WHERE id=?", (int(device_profile_id),))
    return _fetchone_dict(cur)


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
    return _fetchone_dict(cur)


def _runtime_active_device_profiles(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
          FROM device_profiles
         WHERE runtime_active=1
      ORDER BY display_order ASC, id ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


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


def _normalize_runtime_primary_device(
    conn: sqlite3.Connection,
    *,
    preferred_device_id: Optional[int] = None,
    deactivate_others: bool = False,
) -> Optional[int]:
    rows = conn.execute(
        """
        SELECT id, runtime_active, runtime_primary, enabled, system_key
          FROM device_profiles
      ORDER BY display_order ASC, id ASC
        """
    ).fetchall()
    if not rows:
        return None

    row_by_id = {int(row[0]): row for row in rows}
    active_ids = [int(row[0]) for row in rows if int(row[1] or 0) == 1 and int(row[3] or 0) == 1]
    chosen_id: Optional[int] = None

    if preferred_device_id is not None:
        preferred_row = row_by_id.get(int(preferred_device_id))
        if preferred_row is not None and int(preferred_row[3] or 0) == 1:
            chosen_id = int(preferred_device_id)
            if int(preferred_row[1] or 0) != 1:
                conn.execute("UPDATE device_profiles SET runtime_active=1 WHERE id=?", (int(preferred_device_id),))
                conn.commit()
                active_ids = [*active_ids, int(preferred_device_id)]

    if chosen_id is None:
        primary_ids = [int(row[0]) for row in rows if int(row[1] or 0) == 1 and int(row[2] or 0) == 1 and int(row[3] or 0) == 1]
        if primary_ids:
            chosen_id = primary_ids[0]

    if chosen_id is None and active_ids:
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

    updates = _legacy_device_projection_from_settings(settings_values)
    changed = any(existing.get(key) != value for key, value in updates.items())
    if not changed:
        return existing

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
    _write_kv_values(conn, updates)
    conn.commit()
    return updates


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
            fldigi_port, fldigi_log_path, js8_host, js8_port, js8_profile_path,
            varac_install_path, varac_db_path, varac_ini_path, varac_cluster_member_enabled,
            sdr_host, sdr_port, launch_enabled, launch_path, launch_cmd, working_dir,
            ptt_group, antenna_group, frontend_group, amplifier_group, notes,
            created_utc, updated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    operating_id = _ensure_default_operating_profile(conn, settings_values)
    _ensure_default_assignment(conn, device_id, operating_id)
    _normalize_runtime_primary_device(conn, preferred_device_id=device_id, deactivate_others=False)
    log.debug(
        "MultiRadioStore: ensured default device_profile=%s operating_profile=%s assignment.",
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
                "device_class": _coerce_text(
                    payload.get("device_class", (existing or {}).get("device_class", "tx_rx")),
                    "tx_rx",
                ) or "tx_rx",
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
                "js8_profile_path": _coerce_text(
                    payload.get("js8_profile_path", (existing or {}).get("js8_profile_path", "")),
                    "",
                ),
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
                "ptt_group": _coerce_text(payload.get("ptt_group", (existing or {}).get("ptt_group", "")), ""),
                "antenna_group": _coerce_text(
                    payload.get("antenna_group", (existing or {}).get("antenna_group", "")),
                    "",
                ),
                "frontend_group": _coerce_text(
                    payload.get("frontend_group", (existing or {}).get("frontend_group", "")),
                    "",
                ),
                "amplifier_group": _coerce_text(
                    payload.get("amplifier_group", (existing or {}).get("amplifier_group", "")),
                    "",
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
                "js8_profile_path",
                "varac_install_path",
                "varac_db_path",
                "varac_ini_path",
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

            return _device_profile_by_id(conn, row_id) or {}

    def set_runtime_active_device_profile(self, device_profile_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            return self._set_runtime_active_device_conn(conn, int(device_profile_id))

    def set_runtime_primary_device_profile(self, device_profile_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            return self._set_runtime_primary_device_conn(conn, int(device_profile_id))

    def set_device_profile_runtime_active(self, device_profile_id: int, active: bool) -> Dict[str, Any]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            device = _device_profile_by_id(conn, int(device_profile_id))
            if not device:
                raise KeyError(f"Unknown device profile id: {device_profile_id}")
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
            device = _device_profile_by_id(conn, int(device_profile_id))
            if not device:
                raise KeyError(f"Unknown device profile id: {device_profile_id}")
            if int(device.get("runtime_active", 0) or 0) == 1:
                raise ValueError("Cannot delete a runtime-active device profile. Deactivate it first.")
            conn.execute("DELETE FROM operating_profile_assignments WHERE device_profile_id=?", (int(device_profile_id),))
            conn.execute("DELETE FROM varac_cluster_members WHERE device_profile_id=?", (int(device_profile_id),))
            conn.execute(
                "DELETE FROM station_coordination_policies WHERE source_device_id=? OR target_device_id=?",
                (int(device_profile_id), int(device_profile_id)),
            )
            conn.execute("DELETE FROM device_profiles WHERE id=?", (int(device_profile_id),))
            conn.commit()
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
        return [dict(row) for row in rows]

    def save_operating_profile(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(values or {})
        requested_id = _coerce_optional_int(payload.get("id"))

        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            existing = _operating_profile_by_id(conn, int(requested_id)) if requested_id is not None else None
            now_iso = _utc_now_iso()

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
        desired_state = _normalize_assignment_state(assignment_state, "active")
        if desired_state not in EFFECTIVE_ASSIGNMENT_STATES:
            raise ValueError("Only active or temporary_override assignments can become the effective device assignment.")

        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            device = _device_profile_by_id(conn, int(device_profile_id))
            if not device:
                raise KeyError(f"Unknown device profile id: {device_profile_id}")
            operating_profile = _operating_profile_by_id(conn, int(operating_profile_id))
            if not operating_profile:
                raise KeyError(f"Unknown operating profile id: {operating_profile_id}")
            if int(operating_profile.get("enabled", 1) or 0) != 1:
                raise ValueError("Cannot assign a disabled operating profile.")

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

    def restore_default_operating_profile(
        self,
        device_profile_id: int,
        *,
        reason: str = "Restored default operating profile.",
        created_by: str = "settings_ui",
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            ensure_multi_radio_settings_schema(conn)
            row = conn.execute(
                "SELECT id FROM operating_profiles WHERE system_key=?",
                (DEFAULT_OPERATING_SYSTEM_KEY,),
            ).fetchone()
            if row is None:
                operating_profile_id = _ensure_default_operating_profile(conn, {})
            else:
                operating_profile_id = int(row[0])
        return self.set_device_operating_profile(
            int(device_profile_id),
            int(operating_profile_id),
            assignment_state="active",
            reason=reason,
            created_by=created_by,
        )
