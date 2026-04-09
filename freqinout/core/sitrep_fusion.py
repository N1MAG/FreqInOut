from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log
from freqinout.core.operator_activity import newer_timestamp_text
from freqinout.core.sitrep_metadata import (
    merge_transport_modes,
    normalize_transport_mode,
    source_family_key,
    source_families_from_sources,
    source_summary_by_family,
)


VALUE_RED = "red"
VALUE_YELLOW = "yellow"
VALUE_GREEN = "green"
VALUE_UNKNOWN = "unknown"
VALUE_NOT_REPORTED = "not_reported"

STATUS_ORDER = {
    VALUE_RED: 5,
    VALUE_YELLOW: 4,
    VALUE_GREEN: 3,
    VALUE_UNKNOWN: 2,
    VALUE_NOT_REPORTED: 1,
}

DIMENSIONS = (
    "overall_status",
    "power",
    "water",
    "medical",
    "communications",
    "internet",
    "travel",
    "food",
    "fuel",
    "crime",
    "civil_unrest",
    "political",
)

_FUSION_LOCK = threading.Lock()
_LAST_RUN_MONO = 0.0
_MIN_RUN_INTERVAL_SECONDS = 8.0
_ALL_GROUP_KEY = "__ALL__"
_STATE_CONFIDENCE_ORDER = {
    "explicit": 5,
    "grid6": 4,
    "grid4_remarks": 3,
    "grid4_operator": 2,
    "unknown": 1,
}
_GEO_CONFIDENCE_ORDER = {
    "grid6": 4,
    "grid4_state": 3,
    "state_only": 2,
    "unknown": 1,
}
_SUBTYPE_ORDER = {
    "COMMSTAT_12": 5,
    "COMMSTAT_FWD": 4,
    "SPOTTER_301": 3,
    "SPOTTER_304": 2,
    "SPOTTER_104": 1,
}


def fuse_sitreps(settings, *, max_rows: int = 1000) -> Dict[str, int]:
    if not _is_enabled(settings, "sitrep_unified_fusion_enabled", True):
        return {
            "rows_scanned": 0,
            "events_upserted": 0,
            "latest_updated": 0,
            "operators_synced": 0,
            "state_rollups_updated": 0,
            "errors": 0,
        }

    global _LAST_RUN_MONO
    now_mono = time.monotonic()
    if now_mono - _LAST_RUN_MONO < _MIN_RUN_INTERVAL_SECONDS:
        return {
            "rows_scanned": 0,
            "events_upserted": 0,
            "latest_updated": 0,
            "operators_synced": 0,
            "state_rollups_updated": 0,
            "errors": 0,
        }
    if not _FUSION_LOCK.acquire(blocking=False):
        return {
            "rows_scanned": 0,
            "events_upserted": 0,
            "latest_updated": 0,
            "operators_synced": 0,
            "state_rollups_updated": 0,
            "errors": 0,
        }
    _LAST_RUN_MONO = now_mono

    out = {
        "rows_scanned": 0,
        "events_upserted": 0,
        "latest_updated": 0,
        "operators_synced": 0,
        "state_rollups_updated": 0,
        "errors": 0,
    }
    conn = sqlite3.connect(_local_db_path())
    try:
        _ensure_tables(conn)
        cp = _get_checkpoint(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, source, source_table, source_db_path, source_id, subtype, from_call, target, report_group, grid,
                   scope, transport_mode, remarks_text, brevity_code, brevity_summary, state_code, state_confidence,
                   geo_confidence, status_payload, raw_payload, event_ts, event_ts_utc
            FROM sitrep_source_events
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(cp), int(max_rows)),
        )
        rows = cur.fetchall()
        if not rows:
            conn.commit()
            return out

        touched_calls: set[str] = set()
        max_seen = cp
        for row in rows:
            out["rows_scanned"] += 1
            max_seen = max(max_seen, int(row[0] or 0))
            try:
                canonical = _canonicalize_row(row)
                if not canonical:
                    continue
                changed = _upsert_event(conn, canonical)
                if changed:
                    out["events_upserted"] += 1
                    touched_calls.add(canonical["from_call"])
            except Exception as e:
                out["errors"] += 1
                log.debug("SitrepFusion: failed row id=%s: %s", row[0], e)

        _set_checkpoint(conn, max_seen)

        for call in sorted(touched_calls):
            try:
                if _refresh_latest_for_call(conn, call):
                    out["latest_updated"] += 1
            except Exception as e:
                out["errors"] += 1
                log.debug("SitrepFusion: latest refresh failed for %s: %s", call, e)

        try:
            out["operators_synced"] = _sync_operator_presence(conn)
        except Exception as e:
            out["errors"] += 1
            log.debug("SitrepFusion: operator sync failed: %s", e)
        try:
            out["state_rollups_updated"] = _refresh_state_rollups(conn)
        except Exception as e:
            out["errors"] += 1
            log.debug("SitrepFusion: state rollup refresh failed: %s", e)

        conn.commit()
        return out
    finally:
        conn.close()
        _FUSION_LOCK.release()


def _local_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout_nets.db"


def _is_enabled(settings, key: str, default: bool) -> bool:
    try:
        val = settings.get(key, default)
    except Exception:
        val = default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    txt = str(val or "").strip().lower()
    if txt in {"1", "true", "yes", "on", "enabled"}:
        return True
    if txt in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _ensure_tables(conn: sqlite3.Connection) -> None:
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
        CREATE INDEX IF NOT EXISTS idx_sitrep_latest_effective
            ON sitrep_latest_by_callsign(effective_status, latest_event_ts DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_state_rollup_group
            ON sitrep_state_rollup(report_group, latest_event_ts DESC, state_code)
        """
    )


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {str(row[1] or "").strip().lower() for row in cur.fetchall()}
    for name, ddl in columns.items():
        if str(name).strip().lower() in existing:
            continue
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _get_checkpoint(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT last_source_event_id FROM sitrep_fusion_checkpoint WHERE pipeline_key='sitrep_source_events'"
    )
    row = cur.fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def _set_checkpoint(conn: sqlite3.Connection, last_id: int) -> None:
    conn.execute(
        """
        INSERT INTO sitrep_fusion_checkpoint (pipeline_key, last_source_event_id, updated_ts)
        VALUES ('sitrep_source_events', ?, ?)
        ON CONFLICT(pipeline_key) DO UPDATE SET
            last_source_event_id=excluded.last_source_event_id,
            updated_ts=excluded.updated_ts
        """,
        (int(last_id), float(time.time())),
    )


def _safe_json_loads(text: object) -> Dict:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


def _canonicalize_value(value: object) -> str:
    txt = str(value or "").strip().lower()
    if not txt:
        return VALUE_NOT_REPORTED
    if txt in {VALUE_RED, VALUE_YELLOW, VALUE_GREEN, VALUE_UNKNOWN, VALUE_NOT_REPORTED}:
        return txt
    if txt in {"3", "r", "red"}:
        return VALUE_RED
    if txt in {"2", "y", "yellow"}:
        return VALUE_YELLOW
    if txt in {"1", "g", "green"}:
        return VALUE_GREEN
    if txt in {"4", "u", "unknown", "5"}:
        return VALUE_UNKNOWN
    return VALUE_UNKNOWN


def _canonical_scope(value: object) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    low = txt.lower()
    if low in {"1", "my location"}:
        return "My Location"
    if low in {"2", "my community"}:
        return "My Community"
    if low in {"3", "my county"}:
        return "My County"
    if low in {"4", "my region"}:
        return "My Region"
    if low in {"5", "other", "other location"}:
        return "Other Location"
    return txt


def _normalize_call(value: object) -> str:
    return str(value or "").strip().upper()


def _clean_target(value: object) -> str:
    return str(value or "").strip().upper()


def _clean_grid(value: object) -> str:
    return str(value or "").strip().upper()


def _fields_not_reported() -> Dict[str, str]:
    return {k: VALUE_NOT_REPORTED for k in DIMENSIONS}


def _canonicalize_from_status_string(status_txt: str) -> Dict[str, str]:
    status_txt = str(status_txt or "").strip()
    out = _fields_not_reported()
    if len(status_txt) < 12:
        return out
    chars = list(status_txt[:12])
    out["overall_status"] = _canonicalize_value(chars[0])
    out["power"] = _canonicalize_value(chars[1])
    out["water"] = _canonicalize_value(chars[2])
    out["medical"] = _canonicalize_value(chars[3])
    out["communications"] = _canonicalize_value(chars[4])
    out["travel"] = _canonicalize_value(chars[5])
    out["internet"] = _canonicalize_value(chars[6])
    out["fuel"] = _canonicalize_value(chars[7])
    out["food"] = _canonicalize_value(chars[8])
    out["crime"] = _canonicalize_value(chars[9])
    out["civil_unrest"] = _canonicalize_value(chars[10])
    out["political"] = _canonicalize_value(chars[11])
    return out


def _canonicalize_spotter_304_digits(digits: str) -> Dict[str, str]:
    out = _fields_not_reported()
    d = [ch for ch in str(digits or "").strip() if ch.isdigit()]
    if len(d) < 6:
        return out
    # [0]=landline,[1]=telecom,[2]=am/fm,[3]=internet,[4]=water,[5]=power,[6]=natgas,[7]=noaa
    out["communications"] = _canonicalize_value(d[1] if len(d) > 1 else "")
    out["internet"] = _canonicalize_value(d[3] if len(d) > 3 else "")
    out["water"] = _canonicalize_value(d[4] if len(d) > 4 else "")
    out["power"] = _canonicalize_value(d[5] if len(d) > 5 else "")
    return out


def _aggregate_status(values: Iterable[str]) -> str:
    best = VALUE_NOT_REPORTED
    best_score = STATUS_ORDER[VALUE_NOT_REPORTED]
    for val in values:
        v = _canonicalize_value(val)
        score = STATUS_ORDER.get(v, STATUS_ORDER[VALUE_UNKNOWN])
        if score > best_score:
            best = v
            best_score = score
    return best


def _canonicalize_row(row: Sequence) -> Optional[Dict]:
    (
        source_event_id,
        source,
        source_table,
        source_db_path,
        source_id,
        subtype,
        from_call,
        target,
        report_group,
        grid,
        scope,
        transport_mode,
        remarks_text,
        brevity_code,
        brevity_summary,
        state_code,
        state_confidence,
        geo_confidence,
        status_payload_json,
        raw_payload_json,
        event_ts,
        event_ts_utc,
    ) = row

    call = _normalize_call(from_call)
    if not call:
        return None

    subtype_txt = str(subtype or "").strip().upper()
    status_payload = _safe_json_loads(status_payload_json)
    raw_payload = _safe_json_loads(raw_payload_json)
    fields = _fields_not_reported()
    scope_txt = _canonical_scope(scope)

    if subtype_txt in {"COMMSTAT_12", "COMMSTAT_FWD"}:
        status_str = str(status_payload.get("status") or "").strip()
        if len(status_str) >= 12:
            fields = _canonicalize_from_status_string(status_str)
        else:
            fields["overall_status"] = _canonicalize_value(status_payload.get("overall_status"))
            fields["power"] = _canonicalize_value(status_payload.get("power"))
            fields["water"] = _canonicalize_value(status_payload.get("water"))
            fields["medical"] = _canonicalize_value(status_payload.get("medical"))
            fields["communications"] = _canonicalize_value(status_payload.get("communications"))
            fields["internet"] = _canonicalize_value(status_payload.get("internet"))
            fields["travel"] = _canonicalize_value(status_payload.get("travel"))
            fields["food"] = _canonicalize_value(status_payload.get("food"))
            fields["fuel"] = _canonicalize_value(status_payload.get("fuel"))
            fields["crime"] = _canonicalize_value(status_payload.get("crime"))
            fields["civil_unrest"] = _canonicalize_value(status_payload.get("civil_unrest"))
            fields["political"] = _canonicalize_value(status_payload.get("political"))
        if not scope_txt:
            scope_txt = _canonical_scope(status_payload.get("scope"))

    elif subtype_txt == "SPOTTER_104":
        responses = str(status_payload.get("responses") or "").strip()
        first = responses[:1] if responses else ""
        fields["overall_status"] = _canonicalize_value(first)

    elif subtype_txt == "SPOTTER_301":
        responses = str(status_payload.get("responses") or "").strip()
        d = [ch for ch in responses if ch.isdigit()]
        if d:
            scope_txt = _canonical_scope(d[0])
            map_fields = _canonicalize_spotter_304_digits("".join(d[1:9]))
            for key in ("communications", "internet", "water", "power"):
                fields[key] = map_fields[key]
            fields["overall_status"] = _aggregate_status(fields[k] for k in ("communications", "internet", "water", "power"))

    elif subtype_txt == "SPOTTER_304":
        responses = str(status_payload.get("responses") or "").strip()
        map_fields = _canonicalize_spotter_304_digits(responses)
        for key in ("communications", "internet", "water", "power"):
            fields[key] = map_fields[key]
        fields["overall_status"] = _aggregate_status(fields[k] for k in ("communications", "internet", "water", "power"))

    else:
        return None

    ts = float(event_ts or 0.0)
    ts_utc = str(event_ts_utc or "").strip()
    report_key = _build_report_key(
        subtype=subtype_txt,
        from_call=call,
        target=_clean_target(target),
        grid=_clean_grid(grid),
        scope=scope_txt,
        event_ts=ts,
        fields=fields,
        raw_payload=raw_payload,
    )

    return {
        "source_event_id": int(source_event_id or 0),
        "source": str(source or "").strip().upper(),
        "source_table": str(source_table or "").strip(),
        "source_db_path": str(source_db_path or "").strip(),
        "source_id": int(source_id or 0),
        "subtype": subtype_txt,
        "from_call": call,
        "target": _clean_target(target),
        "report_group": _clean_target(report_group),
        "grid": _clean_grid(grid),
        "state_code": _clean_target(state_code),
        "state_confidence": str(state_confidence or "").strip().lower(),
        "geo_confidence": str(geo_confidence or "").strip().lower(),
        "scope": scope_txt,
        "transport_mode": normalize_transport_mode(transport_mode),
        "remarks_text": str(remarks_text or "").strip(),
        "brevity_code": str(brevity_code or "").strip().upper(),
        "brevity_summary": str(brevity_summary or "").strip(),
        "event_ts": ts,
        "event_ts_utc": ts_utc,
        "fields": fields,
        "raw_payload": raw_payload,
        "report_key": report_key,
    }


def _report_external_id(raw_payload: Dict) -> str:
    for key in ("sr_id", "cssr_msgid", "msg_id"):
        val = str(raw_payload.get(key) or "").strip()
        if val:
            return val
    return ""


def _status_signature(fields: Dict[str, str]) -> str:
    return "|".join(str(fields.get(k, VALUE_NOT_REPORTED)) for k in DIMENSIONS)


def _build_report_key(
    *,
    subtype: str,
    from_call: str,
    target: str,
    grid: str,
    scope: str,
    event_ts: float,
    fields: Dict[str, str],
    raw_payload: Dict,
) -> str:
    external_id = _report_external_id(raw_payload)
    sig = _status_signature(fields)
    if external_id:
        base = "|".join([from_call, external_id.upper(), subtype, sig])
    else:
        minute_bucket = int(event_ts // 60.0) if event_ts > 0 else 0
        base = "|".join([from_call, target, grid, scope, subtype, str(minute_bucket), sig])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _safe_json_array_loads(value: object) -> List[str]:
    txt = str(value or "").strip()
    if not txt:
        return []
    try:
        obj = json.loads(txt)
        if isinstance(obj, list):
            out: List[str] = []
            for item in obj:
                s = str(item or "").strip()
                if s:
                    out.append(s)
            return out
    except Exception:
        pass
    return []


def _prefer_non_empty(existing: object, candidate: object) -> str:
    cand = str(candidate or "").strip()
    if cand:
        return cand
    return str(existing or "").strip()


def _confidence_rank(value: object, ranking: Dict[str, int]) -> int:
    key = str(value or "").strip().lower()
    return int(ranking.get(key, 0))


def _prefer_state_metadata(
    existing_state: object,
    existing_state_conf: object,
    existing_geo_conf: object,
    candidate_state: object,
    candidate_state_conf: object,
    candidate_geo_conf: object,
) -> Tuple[str, str, str]:
    existing = (
        str(existing_state or "").strip().upper(),
        str(existing_state_conf or "").strip().lower(),
        str(existing_geo_conf or "").strip().lower(),
    )
    candidate = (
        str(candidate_state or "").strip().upper(),
        str(candidate_state_conf or "").strip().lower(),
        str(candidate_geo_conf or "").strip().lower(),
    )
    existing_rank = (
        _confidence_rank(existing[1], _STATE_CONFIDENCE_ORDER),
        _confidence_rank(existing[2], _GEO_CONFIDENCE_ORDER),
        1 if existing[0] else 0,
    )
    candidate_rank = (
        _confidence_rank(candidate[1], _STATE_CONFIDENCE_ORDER),
        _confidence_rank(candidate[2], _GEO_CONFIDENCE_ORDER),
        1 if candidate[0] else 0,
    )
    if candidate_rank > existing_rank:
        return candidate
    return existing


def _subtype_rank(subtype: object) -> int:
    return int(_SUBTYPE_ORDER.get(str(subtype or "").strip().upper(), 0))


def _upsert_event(conn: sqlite3.Connection, canonical: Dict) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, event_ts, event_ts_utc, target, report_group, grid, state_code, state_confidence, geo_confidence,
               scope, subtype, transport_mode, remarks_text, brevity_code, brevity_summary,
               source_first, sources_json, source_refs_json, raw_payload_json
        FROM sitrep_events
        WHERE report_key=?
        """,
        (canonical["report_key"],),
    )
    row = cur.fetchone()
    now_ts = float(time.time())
    source_ref = f'{canonical["source_table"]}:{canonical["source_id"]}'

    fields = canonical["fields"]
    if not row:
        cur.execute(
            """
            INSERT INTO sitrep_events (
                report_key, event_ts, event_ts_utc, from_call, target, report_group, grid, state_code, state_confidence, geo_confidence, scope,
                overall_status, power, water, medical, communications, internet, travel, food, fuel, crime, civil_unrest, political,
                subtype, transport_mode, remarks_text, brevity_code, brevity_summary,
                source_first, source_last, sources_json, source_count, source_refs_json, raw_payload_json, inserted_ts, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                canonical["report_key"],
                canonical["event_ts"],
                canonical["event_ts_utc"],
                canonical["from_call"],
                canonical["target"],
                canonical["report_group"],
                canonical["grid"],
                canonical["state_code"],
                canonical["state_confidence"],
                canonical["geo_confidence"],
                canonical["scope"],
                fields["overall_status"],
                fields["power"],
                fields["water"],
                fields["medical"],
                fields["communications"],
                fields["internet"],
                fields["travel"],
                fields["food"],
                fields["fuel"],
                fields["crime"],
                fields["civil_unrest"],
                fields["political"],
                canonical["subtype"],
                canonical["transport_mode"],
                canonical["remarks_text"],
                canonical["brevity_code"],
                canonical["brevity_summary"],
                canonical["source"],
                canonical["source"],
                json.dumps([canonical["source"]], separators=(",", ":"), ensure_ascii=True),
                1,
                json.dumps([source_ref], separators=(",", ":"), ensure_ascii=True),
                json.dumps(canonical["raw_payload"], separators=(",", ":"), ensure_ascii=True),
                now_ts,
                now_ts,
            ),
        )
        return True

    (
        event_id,
        existing_ts,
        existing_ts_utc,
        existing_target,
        existing_report_group,
        existing_grid,
        existing_state_code,
        existing_state_confidence,
        existing_geo_confidence,
        existing_scope,
        existing_subtype,
        existing_transport_mode,
        existing_remarks_text,
        existing_brevity_code,
        existing_brevity_summary,
        source_first,
        sources_json,
        refs_json,
        existing_raw_payload_json,
    ) = row
    sources = set(_safe_json_array_loads(sources_json))
    refs = set(_safe_json_array_loads(refs_json))
    sources.add(canonical["source"])
    refs.add(source_ref)
    source_list = sorted(sources)
    ref_list = sorted(refs)
    if len(ref_list) > 20:
        ref_list = ref_list[-20:]
    merged_ts = float(existing_ts or 0.0)
    candidate_ts = float(canonical["event_ts"] or 0.0)
    candidate_is_newer = candidate_ts > merged_ts
    if candidate_is_newer:
        merged_ts = float(canonical["event_ts"] or 0.0)
        merged_ts_utc = canonical["event_ts_utc"]
    else:
        merged_ts_utc = str(existing_ts_utc or "").strip()
    preferred_state, preferred_state_confidence, preferred_geo_confidence = _prefer_state_metadata(
        existing_state_code,
        existing_state_confidence,
        existing_geo_confidence,
        canonical["state_code"],
        canonical["state_confidence"],
        canonical["geo_confidence"],
    )
    merged_transport = merge_transport_modes(existing_transport_mode, canonical["transport_mode"])
    richer_candidate = candidate_is_newer or (
        candidate_ts == float(existing_ts or 0.0) and _subtype_rank(canonical["subtype"]) >= _subtype_rank(existing_subtype)
    )
    target_out = canonical["target"] if richer_candidate else _prefer_non_empty(existing_target, canonical["target"])
    report_group_out = canonical["report_group"] if richer_candidate else _prefer_non_empty(existing_report_group, canonical["report_group"])
    grid_out = canonical["grid"] if richer_candidate and canonical["grid"] else _prefer_non_empty(existing_grid, canonical["grid"])
    scope_out = canonical["scope"] if richer_candidate and canonical["scope"] else _prefer_non_empty(existing_scope, canonical["scope"])
    subtype_out = canonical["subtype"] if richer_candidate else _prefer_non_empty(existing_subtype, canonical["subtype"])
    remarks_out = canonical["remarks_text"] if richer_candidate and canonical["remarks_text"] else _prefer_non_empty(existing_remarks_text, canonical["remarks_text"])
    brevity_code_out = canonical["brevity_code"] if richer_candidate and canonical["brevity_code"] else _prefer_non_empty(existing_brevity_code, canonical["brevity_code"])
    brevity_summary_out = canonical["brevity_summary"] if richer_candidate and canonical["brevity_summary"] else _prefer_non_empty(existing_brevity_summary, canonical["brevity_summary"])
    raw_payload_out = canonical["raw_payload"]
    if not richer_candidate:
        try:
            parsed_existing = json.loads(str(existing_raw_payload_json or "").strip() or "{}")
            if isinstance(parsed_existing, dict):
                raw_payload_out = parsed_existing
        except Exception:
            pass

    cur.execute(
        """
        UPDATE sitrep_events
        SET event_ts=?,
            event_ts_utc=COALESCE(NULLIF(?, ''), event_ts_utc),
            target=?,
            report_group=?,
            grid=?,
            state_code=?,
            state_confidence=?,
            geo_confidence=?,
            scope=?,
            overall_status=?,
            power=?,
            water=?,
            medical=?,
            communications=?,
            internet=?,
            travel=?,
            food=?,
            fuel=?,
            crime=?,
            civil_unrest=?,
            political=?,
            subtype=?,
            transport_mode=?,
            remarks_text=?,
            brevity_code=?,
            brevity_summary=?,
            source_first=?,
            source_last=?,
            sources_json=?,
            source_count=?,
            source_refs_json=?,
            raw_payload_json=?,
            updated_ts=?
        WHERE id=?
        """,
        (
            merged_ts,
            merged_ts_utc,
            target_out,
            report_group_out,
            grid_out,
            preferred_state,
            preferred_state_confidence,
            preferred_geo_confidence,
            scope_out,
            fields["overall_status"],
            fields["power"],
            fields["water"],
            fields["medical"],
            fields["communications"],
            fields["internet"],
            fields["travel"],
            fields["food"],
            fields["fuel"],
            fields["crime"],
            fields["civil_unrest"],
            fields["political"],
            subtype_out,
            merged_transport,
            remarks_out,
            brevity_code_out,
            brevity_summary_out,
            source_first or canonical["source"],
            canonical["source"],
            json.dumps(source_list, separators=(",", ":"), ensure_ascii=True),
            len(source_list),
            json.dumps(ref_list, separators=(",", ":"), ensure_ascii=True),
            json.dumps(raw_payload_out, separators=(",", ":"), ensure_ascii=True),
            now_ts,
            int(event_id),
        ),
    )
    return bool(cur.rowcount)


def _effective_from_values(overall: object, *values: object) -> str:
    overall = _canonicalize_value(overall)
    if overall != VALUE_NOT_REPORTED:
        return overall
    vals = [_canonicalize_value(value) for value in values]
    return _aggregate_status(vals)


def _per_source_status_for_call(conn: sqlite3.Connection, callsign: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT source_last, sources_json, overall_status, power, water, medical, communications, internet, travel, food, fuel, crime, civil_unrest, political, event_ts, id
        FROM sitrep_events
        WHERE from_call=?
        ORDER BY event_ts DESC, id DESC
        LIMIT 100
        """,
        (callsign,),
    )
    rows = cur.fetchall()
    for row in rows:
        effective = _effective_from_values(
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[11],
            row[12],
            row[13],
        )
        direct = str(row[0] or "").strip().upper()
        if direct and direct not in out:
            out[direct] = effective
        for src in _safe_json_array_loads(row[1]):
            key = str(src or "").strip().upper()
            if key and key not in out:
                out[key] = effective
        if len(out) >= 8:
            break
    return source_summary_by_family(out)


def _refresh_latest_for_call(conn: sqlite3.Connection, callsign: str) -> bool:
    call = _normalize_call(callsign)
    if not call:
        return False
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, event_ts, event_ts_utc, from_call, target, report_group, grid, state_code, state_confidence, geo_confidence, scope,
               overall_status, power, water, medical, communications, internet, travel, food, fuel, crime, civil_unrest, political
               , subtype, transport_mode, remarks_text, brevity_code, brevity_summary
        FROM sitrep_events
        WHERE from_call=?
        ORDER BY event_ts DESC,
                 CASE subtype
                    WHEN 'COMMSTAT_12' THEN 5
                    WHEN 'COMMSTAT_FWD' THEN 4
                    WHEN 'SPOTTER_301' THEN 3
                    WHEN 'SPOTTER_304' THEN 2
                    WHEN 'SPOTTER_104' THEN 1
                    ELSE 0
                 END DESC,
                 id DESC
        LIMIT 1
        """,
        (call,),
    )
    row = cur.fetchone()
    if not row:
        cur.execute("DELETE FROM sitrep_latest_by_callsign WHERE callsign=?", (call,))
        return bool(cur.rowcount)

    effective = _effective_from_values(
        row[11],
        row[12],
        row[13],
        row[14],
        row[15],
        row[16],
        row[17],
        row[18],
        row[19],
        row[20],
        row[21],
        row[22],
    )
    per_source = _per_source_status_for_call(conn, call)
    now_ts = float(time.time())
    cur.execute(
        """
        INSERT INTO sitrep_latest_by_callsign (
            callsign, latest_event_id, latest_event_ts, latest_event_ts_utc, latest_subtype, latest_target, latest_report_group,
            latest_grid, latest_state_code, latest_state_confidence, latest_geo_confidence, latest_transport_mode,
            latest_remarks_text, latest_brevity_code, latest_brevity_summary, effective_status, scope,
            overall_status, power, water, medical, communications, internet, travel, food, fuel, crime, civil_unrest, political,
            source_summary_json, updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(callsign) DO UPDATE SET
            latest_event_id=excluded.latest_event_id,
            latest_event_ts=excluded.latest_event_ts,
            latest_event_ts_utc=excluded.latest_event_ts_utc,
            latest_subtype=excluded.latest_subtype,
            latest_target=excluded.latest_target,
            latest_report_group=excluded.latest_report_group,
            latest_grid=excluded.latest_grid,
            latest_state_code=excluded.latest_state_code,
            latest_state_confidence=excluded.latest_state_confidence,
            latest_geo_confidence=excluded.latest_geo_confidence,
            latest_transport_mode=excluded.latest_transport_mode,
            latest_remarks_text=excluded.latest_remarks_text,
            latest_brevity_code=excluded.latest_brevity_code,
            latest_brevity_summary=excluded.latest_brevity_summary,
            effective_status=excluded.effective_status,
            scope=excluded.scope,
            overall_status=excluded.overall_status,
            power=excluded.power,
            water=excluded.water,
            medical=excluded.medical,
            communications=excluded.communications,
            internet=excluded.internet,
            travel=excluded.travel,
            food=excluded.food,
            fuel=excluded.fuel,
            crime=excluded.crime,
            civil_unrest=excluded.civil_unrest,
            political=excluded.political,
            source_summary_json=excluded.source_summary_json,
            updated_ts=excluded.updated_ts
        """,
        (
            call,
            int(row[0]),
            float(row[1] or 0.0),
            str(row[2] or ""),
            str(row[23] or "").strip().upper(),
            str(row[4] or "").strip().upper(),
            str(row[5] or "").strip().upper(),
            str(row[6] or "").strip().upper(),
            str(row[7] or "").strip().upper(),
            str(row[8] or "").strip().lower(),
            str(row[9] or "").strip().lower(),
            normalize_transport_mode(row[24]),
            str(row[25] or "").strip(),
            str(row[26] or "").strip().upper(),
            str(row[27] or "").strip(),
            effective,
            str(row[10] or ""),
            _canonicalize_value(row[11]),
            _canonicalize_value(row[12]),
            _canonicalize_value(row[13]),
            _canonicalize_value(row[14]),
            _canonicalize_value(row[15]),
            _canonicalize_value(row[16]),
            _canonicalize_value(row[17]),
            _canonicalize_value(row[18]),
            _canonicalize_value(row[19]),
            _canonicalize_value(row[20]),
            _canonicalize_value(row[21]),
            _canonicalize_value(row[22]),
            json.dumps(per_source, separators=(",", ":"), ensure_ascii=True),
            now_ts,
        ),
    )
    return bool(cur.rowcount)


def _sync_operator_presence(conn: sqlite3.Connection) -> int:
    ensure_operator_checkins_schema(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            callsign,
            latest_event_ts_utc,
            latest_grid,
            latest_state_code,
            latest_report_group,
            source_summary_json
        FROM sitrep_latest_by_callsign
        ORDER BY latest_event_ts DESC, callsign
        """
    )
    rows = cur.fetchall()
    synced = 0
    for callsign, last_seen_utc, grid, state_code, report_group, source_summary_json in rows:
        call = _normalize_call(callsign)
        if not call:
            continue
        source_summary = _safe_json_loads(source_summary_json)
        families = source_families_from_sources(source_summary.keys())
        if not families or all(fam in {"MANUAL", "UNKNOWN", "FUSED"} for fam in families):
            continue
        cur.execute(
            """
            SELECT name, state, grid, group1, group2, group3, group_role,
                   first_seen_utc, last_seen_utc, last_net, last_role,
                   checkin_count, groups_json, trusted
            FROM operator_checkins
            WHERE callsign=?
            """,
            (call,),
        )
        existing = cur.fetchone()
        if existing:
            (
                existing_name,
                existing_state,
                existing_grid,
                existing_g1,
                existing_g2,
                existing_g3,
                existing_role,
                existing_first_seen,
                existing_last_seen,
                existing_last_net,
                existing_last_role,
                existing_count,
                existing_groups_json,
                existing_trusted,
            ) = existing
        else:
            existing_name = ""
            existing_state = ""
            existing_grid = ""
            existing_g1 = existing_g2 = existing_g3 = ""
            existing_role = ""
            existing_first_seen = ""
            existing_last_seen = ""
            existing_last_net = ""
            existing_last_role = ""
            existing_count = 0
            existing_groups_json = ""
            existing_trusted = 0

        merged_groups: List[str] = []
        if existing_groups_json:
            try:
                parsed = json.loads(existing_groups_json)
                if isinstance(parsed, list):
                    merged_groups.extend(str(item).strip().upper() for item in parsed if str(item).strip())
            except Exception:
                pass
        for group in (existing_g1, existing_g2, existing_g3, report_group):
            txt = str(group or "").strip().upper()
            if txt and txt not in merged_groups:
                merged_groups.append(txt)
        merged_groups = merged_groups[:3]
        state_out = str(state_code or "").strip().upper() or str(existing_state or "").strip().upper()
        grid_out = str(grid or "").strip().upper() or str(existing_grid or "").strip().upper()
        last_seen_out = newer_timestamp_text(existing_last_seen, str(last_seen_utc or "").strip())
        if not last_seen_out:
            last_seen_out = str(existing_last_seen or "").strip() or str(last_seen_utc or "").strip()
        first_seen_out = str(existing_first_seen or "").strip() or last_seen_out
        trusted_out = 1 if int(existing_trusted or 0) else 0
        last_net_out = str(existing_last_net or "").strip() or "SitRep"
        cur.execute(
            """
            INSERT INTO operator_checkins (
                callsign, name, state, grid, group1, group2, group3, group_role,
                first_seen_utc, last_seen_utc, last_net, last_role, checkin_count, groups_json, trusted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(callsign) DO UPDATE SET
                name=excluded.name,
                state=excluded.state,
                grid=excluded.grid,
                group1=excluded.group1,
                group2=excluded.group2,
                group3=excluded.group3,
                group_role=excluded.group_role,
                first_seen_utc=excluded.first_seen_utc,
                last_seen_utc=excluded.last_seen_utc,
                last_net=excluded.last_net,
                last_role=excluded.last_role,
                checkin_count=excluded.checkin_count,
                groups_json=excluded.groups_json,
                trusted=excluded.trusted
            """,
            (
                call,
                str(existing_name or "").strip(),
                state_out,
                grid_out,
                merged_groups[0] if len(merged_groups) > 0 else "",
                merged_groups[1] if len(merged_groups) > 1 else "",
                merged_groups[2] if len(merged_groups) > 2 else "",
                str(existing_role or "").strip().upper(),
                first_seen_out,
                last_seen_out,
                last_net_out,
                str(existing_last_role or "").strip().upper(),
                int(existing_count or 0),
                json.dumps(merged_groups, separators=(",", ":"), ensure_ascii=True) if merged_groups else None,
                trusted_out,
            ),
        )
        synced += 1
    return synced


def _refresh_state_rollups(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM sitrep_state_rollup")
    cur.execute(
        """
        SELECT latest_report_group, latest_state_code, effective_status, latest_transport_mode, latest_event_ts
        FROM sitrep_latest_by_callsign
        """
    )
    rows = cur.fetchall()
    rollups: Dict[Tuple[str, str], Dict[str, float]] = {}
    for report_group, state_code, effective_status, transport_mode, latest_event_ts in rows:
        state = str(state_code or "").strip().upper()
        if not state:
            continue
        groups = [_ALL_GROUP_KEY]
        group_txt = str(report_group or "").strip().upper()
        if group_txt:
            groups.append(group_txt)
        status = _canonicalize_value(effective_status)
        if status == VALUE_NOT_REPORTED:
            status = VALUE_UNKNOWN
        transport = normalize_transport_mode(transport_mode)
        ts_val = float(latest_event_ts or 0.0)
        for group in groups:
            key = (group, state)
            bucket = rollups.setdefault(
                key,
                {
                    "callsign_count": 0,
                    "red_count": 0,
                    "yellow_count": 0,
                    "green_count": 0,
                    "unknown_count": 0,
                    "js8_count": 0,
                    "internet_count": 0,
                    "mixed_transport_count": 0,
                    "latest_event_ts": 0.0,
                },
            )
            bucket["callsign_count"] += 1
            bucket[f"{status}_count"] += 1
            if transport == "js8":
                bucket["js8_count"] += 1
            elif transport == "internet":
                bucket["internet_count"] += 1
            elif transport == "js8+internet":
                bucket["mixed_transport_count"] += 1
            if ts_val > float(bucket.get("latest_event_ts", 0.0) or 0.0):
                bucket["latest_event_ts"] = ts_val

    now_ts = float(time.time())
    for (report_group, state_code), bucket in sorted(rollups.items()):
        cur.execute(
            """
            INSERT INTO sitrep_state_rollup (
                report_group, state_code, callsign_count, red_count, yellow_count, green_count, unknown_count,
                js8_count, internet_count, mixed_transport_count, latest_event_ts, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report_group,
                state_code,
                int(bucket.get("callsign_count", 0)),
                int(bucket.get("red_count", 0)),
                int(bucket.get("yellow_count", 0)),
                int(bucket.get("green_count", 0)),
                int(bucket.get("unknown_count", 0)),
                int(bucket.get("js8_count", 0)),
                int(bucket.get("internet_count", 0)),
                int(bucket.get("mixed_transport_count", 0)),
                float(bucket.get("latest_event_ts", 0.0) or 0.0),
                now_ts,
            ),
        )
    return len(rollups)
