from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


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


def fuse_sitreps(settings, *, max_rows: int = 1000) -> Dict[str, int]:
    if not _is_enabled(settings, "sitrep_unified_fusion_enabled", True):
        return {
            "rows_scanned": 0,
            "events_upserted": 0,
            "latest_updated": 0,
            "errors": 0,
        }

    global _LAST_RUN_MONO
    now_mono = time.monotonic()
    if now_mono - _LAST_RUN_MONO < _MIN_RUN_INTERVAL_SECONDS:
        return {
            "rows_scanned": 0,
            "events_upserted": 0,
            "latest_updated": 0,
            "errors": 0,
        }
    if not _FUSION_LOCK.acquire(blocking=False):
        return {
            "rows_scanned": 0,
            "events_upserted": 0,
            "latest_updated": 0,
            "errors": 0,
        }
    _LAST_RUN_MONO = now_mono

    out = {"rows_scanned": 0, "events_upserted": 0, "latest_updated": 0, "errors": 0}
    conn = sqlite3.connect(_local_db_path())
    try:
        _ensure_tables(conn)
        cp = _get_checkpoint(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, source, source_table, source_db_path, source_id, subtype, from_call, target, grid, scope,
                   status_payload, raw_payload, event_ts, event_ts_utc
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
    cur.execute(
        """
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
        )
        """
    )
    cur.execute(
        """
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
        )
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
        CREATE INDEX IF NOT EXISTS idx_sitrep_latest_effective
            ON sitrep_latest_by_callsign(effective_status, latest_event_ts DESC)
        """
    )


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
        grid,
        scope,
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
        "grid": _clean_grid(grid),
        "scope": scope_txt,
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


def _upsert_event(conn: sqlite3.Connection, canonical: Dict) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, event_ts, source_first, sources_json, source_refs_json
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
                report_key, event_ts, event_ts_utc, from_call, target, grid, scope,
                overall_status, power, water, medical, communications, internet, travel, food, fuel, crime, civil_unrest, political,
                subtype, source_first, source_last, sources_json, source_count, source_refs_json, raw_payload_json, inserted_ts, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                canonical["report_key"],
                canonical["event_ts"],
                canonical["event_ts_utc"],
                canonical["from_call"],
                canonical["target"],
                canonical["grid"],
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

    event_id, existing_ts, source_first, sources_json, refs_json = row
    sources = set(_safe_json_array_loads(sources_json))
    refs = set(_safe_json_array_loads(refs_json))
    sources.add(canonical["source"])
    refs.add(source_ref)
    source_list = sorted(sources)
    ref_list = sorted(refs)
    if len(ref_list) > 20:
        ref_list = ref_list[-20:]
    merged_ts = float(existing_ts or 0.0)
    if float(canonical["event_ts"] or 0.0) > merged_ts:
        merged_ts = float(canonical["event_ts"] or 0.0)
        merged_ts_utc = canonical["event_ts_utc"]
    else:
        merged_ts_utc = ""

    cur.execute(
        """
        UPDATE sitrep_events
        SET event_ts=?,
            event_ts_utc=COALESCE(NULLIF(?, ''), event_ts_utc),
            target=?,
            grid=?,
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
            canonical["target"],
            canonical["grid"],
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
            source_first or canonical["source"],
            canonical["source"],
            json.dumps(source_list, separators=(",", ":"), ensure_ascii=True),
            len(source_list),
            json.dumps(ref_list, separators=(",", ":"), ensure_ascii=True),
            json.dumps(canonical["raw_payload"], separators=(",", ":"), ensure_ascii=True),
            now_ts,
            int(event_id),
        ),
    )
    return bool(cur.rowcount)


def _effective_from_row(row: Sequence) -> str:
    # row order uses SELECT in _refresh_latest_for_call
    overall = _canonicalize_value(row[7])
    if overall != VALUE_NOT_REPORTED:
        return overall
    vals = [_canonicalize_value(row[idx]) for idx in range(8, 19)]
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
        effective = _effective_from_row(
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
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
    return out


def _refresh_latest_for_call(conn: sqlite3.Connection, callsign: str) -> bool:
    call = _normalize_call(callsign)
    if not call:
        return False
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, event_ts, event_ts_utc, from_call, target, grid, scope,
               overall_status, power, water, medical, communications, internet, travel, food, fuel, crime, civil_unrest, political
        FROM sitrep_events
        WHERE from_call=?
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """,
        (call,),
    )
    row = cur.fetchone()
    if not row:
        cur.execute("DELETE FROM sitrep_latest_by_callsign WHERE callsign=?", (call,))
        return bool(cur.rowcount)

    effective = _effective_from_row(row)
    per_source = _per_source_status_for_call(conn, call)
    now_ts = float(time.time())
    cur.execute(
        """
        INSERT INTO sitrep_latest_by_callsign (
            callsign, latest_event_id, latest_event_ts, latest_event_ts_utc, effective_status, scope,
            overall_status, power, water, medical, communications, internet, travel, food, fuel, crime, civil_unrest, political,
            source_summary_json, updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(callsign) DO UPDATE SET
            latest_event_id=excluded.latest_event_id,
            latest_event_ts=excluded.latest_event_ts,
            latest_event_ts_utc=excluded.latest_event_ts_utc,
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
            effective,
            str(row[6] or ""),
            _canonicalize_value(row[7]),
            _canonicalize_value(row[8]),
            _canonicalize_value(row[9]),
            _canonicalize_value(row[10]),
            _canonicalize_value(row[11]),
            _canonicalize_value(row[12]),
            _canonicalize_value(row[13]),
            _canonicalize_value(row[14]),
            _canonicalize_value(row[15]),
            _canonicalize_value(row[16]),
            _canonicalize_value(row[17]),
            _canonicalize_value(row[18]),
            json.dumps(per_source, separators=(",", ":"), ensure_ascii=True),
            now_ts,
        ),
    )
    return bool(cur.rowcount)
