from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from typing import Dict, Iterable, List

from freqinout.core.group_utils import normalize_group_name
from freqinout.core.sitrep_metadata import merge_transport_modes, normalize_transport_mode


KIND_STATREP = "STATREP"
KIND_MESSAGE = "MESSAGE"
KIND_ALERT = "ALERT"

_STATUS_CANON = {
    "3": "RED",
    "R": "RED",
    "RED": "RED",
    "2": "YELLOW",
    "Y": "YELLOW",
    "YELLOW": "YELLOW",
    "1": "GREEN",
    "G": "GREEN",
    "GREEN": "GREEN",
    "4": "UNKNOWN",
    "5": "UNKNOWN",
    "U": "UNKNOWN",
    "UNKNOWN": "UNKNOWN",
    "": "NOT REPORTED",
    "NOT_REPORTED": "NOT REPORTED",
    "NOT REPORTED": "NOT REPORTED",
}


def ensure_commstat_artifact_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS commstat_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artifact_key TEXT NOT NULL UNIQUE,
            artifact_kind TEXT NOT NULL,
            subtype TEXT,
            event_ts REAL,
            event_ts_utc TEXT,
            from_call TEXT,
            target TEXT,
            report_group TEXT,
            grid TEXT,
            state_code TEXT,
            scope TEXT,
            transport_mode TEXT,
            status_label TEXT,
            alert_color TEXT,
            title TEXT,
            body_text TEXT,
            remarks_text TEXT,
            brevity_code TEXT,
            brevity_summary TEXT,
            source_first TEXT,
            source_last TEXT,
            sources_json TEXT,
            source_count INTEGER DEFAULT 1,
            source_refs_json TEXT,
            external_ids_json TEXT,
            payload_json TEXT,
            inserted_ts REAL NOT NULL,
            updated_ts REAL NOT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "commstat_artifacts",
        {
            "artifact_key": "TEXT",
            "artifact_kind": "TEXT",
            "subtype": "TEXT",
            "event_ts": "REAL",
            "event_ts_utc": "TEXT",
            "from_call": "TEXT",
            "target": "TEXT",
            "report_group": "TEXT",
            "grid": "TEXT",
            "state_code": "TEXT",
            "scope": "TEXT",
            "transport_mode": "TEXT",
            "status_label": "TEXT",
            "alert_color": "TEXT",
            "title": "TEXT",
            "body_text": "TEXT",
            "remarks_text": "TEXT",
            "brevity_code": "TEXT",
            "brevity_summary": "TEXT",
            "source_first": "TEXT",
            "source_last": "TEXT",
            "sources_json": "TEXT",
            "source_count": "INTEGER DEFAULT 1",
            "source_refs_json": "TEXT",
            "external_ids_json": "TEXT",
            "payload_json": "TEXT",
            "inserted_ts": "REAL",
            "updated_ts": "REAL",
        },
    )
    cur.execute(
        """
        DELETE FROM commstat_artifacts
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM commstat_artifacts
            GROUP BY artifact_key
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_commstat_artifacts_key
            ON commstat_artifacts(artifact_key)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_commstat_artifacts_recent
            ON commstat_artifacts(event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_commstat_artifacts_kind_recent
            ON commstat_artifacts(artifact_kind, event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_commstat_artifacts_call_recent
            ON commstat_artifacts(from_call, event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_commstat_artifacts_group_recent
            ON commstat_artifacts(report_group, event_ts DESC, id DESC)
        """
    )
    ensure_commstat_artifact_deletion_tables(conn)


def normalize_commstat_artifact_key(value: object) -> str:
    return str(value or "").strip().lower()


def ensure_commstat_artifact_deletion_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS commstat_artifact_deletions (
            artifact_key TEXT PRIMARY KEY,
            artifact_kind TEXT,
            from_call TEXT,
            target TEXT,
            title TEXT,
            event_ts REAL,
            deleted_ts REAL NOT NULL,
            reason TEXT
        )
        """
    )
    _ensure_columns(
        conn,
        "commstat_artifact_deletions",
        {
            "artifact_key": "TEXT",
            "artifact_kind": "TEXT",
            "from_call": "TEXT",
            "target": "TEXT",
            "title": "TEXT",
            "event_ts": "REAL",
            "deleted_ts": "REAL",
            "reason": "TEXT",
        },
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_commstat_artifact_deletions_ts
            ON commstat_artifact_deletions(deleted_ts)
        """
    )


def tombstone_commstat_artifact(
    conn: sqlite3.Connection,
    *,
    artifact_key: object,
    artifact_kind: object = "",
    from_call: object = "",
    target: object = "",
    title: object = "",
    event_ts: object = 0.0,
    reason: object = "message_viewer_delete",
) -> bool:
    key = normalize_commstat_artifact_key(artifact_key)
    if not key:
        return False
    ensure_commstat_artifact_deletion_tables(conn)
    try:
        event_ts_value = float(event_ts or 0.0)
    except Exception:
        event_ts_value = 0.0
    conn.execute(
        """
        INSERT OR REPLACE INTO commstat_artifact_deletions (
            artifact_key, artifact_kind, from_call, target, title, event_ts, deleted_ts, reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            str(artifact_kind or "").strip().upper(),
            str(from_call or "").strip().upper(),
            str(target or "").strip().upper(),
            str(title or "").strip(),
            event_ts_value,
            float(time.time()),
            str(reason or "").strip(),
        ),
    )
    return True


def artifact_kind_label(kind: object) -> str:
    key = str(kind or "").strip().upper()
    if key == KIND_STATREP:
        return "CommStat StatRep"
    if key == KIND_MESSAGE:
        return "CommStat Message"
    if key == KIND_ALERT:
        return "CommStat Alert"
    return "CommStat"


def artifact_filter_label(kind: object) -> str:
    key = str(kind or "").strip().upper()
    if key == KIND_STATREP:
        return "CommStat/StatRep"
    if key == KIND_MESSAGE:
        return "CommStat/Message"
    if key == KIND_ALERT:
        return "CommStat/Alert"
    return "CommStat"


def normalize_status_label(value: object) -> str:
    txt = str(value or "").strip().upper().replace("_", " ")
    if not txt:
        return "NOT REPORTED"
    return _STATUS_CANON.get(txt, txt)


def build_statrep_artifact_key(
    *,
    from_call: object,
    target: object,
    grid: object,
    scope: object,
    status_signature: object,
    event_ts: object,
    bucket_seconds: int = 60,
) -> str:
    bucket = _time_bucket(event_ts, bucket_seconds)
    base = "|".join(
        [
            str(from_call or "").strip().upper(),
            str(target or "").strip().upper(),
            str(grid or "").strip().upper(),
            _canonical_scope(scope),
            str(status_signature or "").strip().upper(),
            str(bucket),
        ]
    )
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def build_message_artifact_key(
    *,
    from_call: object,
    target: object,
    body_text: object,
    event_ts: object,
    bucket_seconds: int = 60,
) -> str:
    bucket = _time_bucket(event_ts, bucket_seconds)
    base = "|".join(
        [
            str(from_call or "").strip().upper(),
            str(target or "").strip().upper(),
            _normalize_body(body_text),
            str(bucket),
        ]
    )
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def build_alert_artifact_key(
    *,
    from_call: object,
    target: object,
    alert_color: object,
    title: object,
    body_text: object,
    event_ts: object,
    bucket_seconds: int = 60,
) -> str:
    bucket = _time_bucket(event_ts, bucket_seconds)
    base = "|".join(
        [
            str(from_call or "").strip().upper(),
            str(target or "").strip().upper(),
            str(alert_color or "").strip().upper(),
            _normalize_body(title),
            _normalize_body(body_text),
            str(bucket),
        ]
    )
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def upsert_commstat_artifact(
    conn: sqlite3.Connection,
    *,
    artifact_key: str,
    artifact_kind: str,
    subtype: str = "",
    event_ts: float = 0.0,
    event_ts_utc: str = "",
    from_call: str = "",
    target: str = "",
    report_group: str = "",
    grid: str = "",
    state_code: str = "",
    scope: str = "",
    transport_mode: str = "",
    status_label: str = "",
    alert_color: str = "",
    title: str = "",
    body_text: str = "",
    remarks_text: str = "",
    brevity_code: str = "",
    brevity_summary: str = "",
    source: str = "",
    source_ref: str = "",
    external_ids: Iterable[str] | None = None,
    payload: Dict | None = None,
) -> bool:
    key = str(artifact_key or "").strip().lower()
    if not key:
        return False
    kind = str(artifact_kind or "").strip().upper()
    if not kind:
        return False

    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, event_ts, event_ts_utc, from_call, target, report_group, grid, state_code, scope,
               transport_mode, status_label, alert_color, title, body_text, remarks_text,
               source_first, source_last, sources_json, source_refs_json, external_ids_json, payload_json, subtype,
               brevity_code, brevity_summary
        FROM commstat_artifacts
        WHERE artifact_key=?
        """,
        (key,),
    )
    row = cur.fetchone()

    now_ts = float(time.time())
    source_txt = str(source or "").strip().upper()
    source_ref_txt = str(source_ref or "").strip()
    ext_ids = _dedupe_str_list(external_ids or [])
    payload_json = json.dumps(payload or {}, separators=(",", ":"), ensure_ascii=True)
    transport_txt = normalize_transport_mode(transport_mode)
    status_txt = normalize_status_label(status_label)
    alert_color_txt = str(alert_color or "").strip().upper()
    subtype_txt = str(subtype or "").strip().upper()
    from_txt = str(from_call or "").strip().upper()
    target_txt = str(target or "").strip().upper()
    report_group_txt = normalize_group_name(report_group)
    grid_txt = str(grid or "").strip().upper()
    state_txt = str(state_code or "").strip().upper()
    scope_txt = _canonical_scope(scope)
    title_txt = str(title or "").strip()
    body_txt = str(body_text or "").strip()
    remarks_txt = str(remarks_text or "").strip()
    brevity_code_txt = str(brevity_code or "").strip().upper()
    brevity_summary_txt = str(brevity_summary or "").strip()

    if not row:
        cur.execute(
            """
            INSERT INTO commstat_artifacts (
                artifact_key, artifact_kind, subtype, event_ts, event_ts_utc, from_call, target, report_group,
                grid, state_code, scope, transport_mode, status_label, alert_color, title, body_text, remarks_text,
                brevity_code, brevity_summary,
                source_first, source_last, sources_json, source_count, source_refs_json, external_ids_json,
                payload_json, inserted_ts, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key,
                kind,
                subtype_txt,
                float(event_ts or 0.0),
                str(event_ts_utc or "").strip(),
                from_txt,
                target_txt,
                report_group_txt,
                grid_txt,
                state_txt,
                scope_txt,
                transport_txt,
                status_txt,
                alert_color_txt,
                title_txt,
                body_txt,
                remarks_txt,
                brevity_code_txt,
                brevity_summary_txt,
                source_txt,
                source_txt,
                json.dumps([source_txt] if source_txt else [], separators=(",", ":"), ensure_ascii=True),
                1 if source_txt else 0,
                json.dumps([source_ref_txt] if source_ref_txt else [], separators=(",", ":"), ensure_ascii=True),
                json.dumps(ext_ids, separators=(",", ":"), ensure_ascii=True),
                payload_json,
                now_ts,
                now_ts,
            ),
        )
        return True

    (
        row_id,
        existing_ts,
        existing_ts_utc,
        existing_from,
        existing_target,
        existing_group,
        existing_grid,
        existing_state,
        existing_scope,
        existing_transport,
        existing_status,
        existing_alert_color,
        existing_title,
        existing_body,
        existing_remarks,
        source_first,
        _source_last,
        sources_json,
        refs_json,
        ext_ids_json,
        existing_payload_json,
        existing_subtype,
        existing_brevity_code,
        existing_brevity_summary,
    ) = row

    sources = set(_safe_json_array_loads(sources_json))
    refs = set(_safe_json_array_loads(refs_json))
    known_ext_ids = set(_safe_json_array_loads(ext_ids_json))
    if source_txt:
        sources.add(source_txt)
    if source_ref_txt:
        refs.add(source_ref_txt)
    for ext in ext_ids:
        if ext:
            known_ext_ids.add(ext)
    source_list = _dedupe_str_list(sources)
    ref_list = _dedupe_str_list(refs)[-20:]
    ext_id_list = _dedupe_str_list(known_ext_ids)[-20:]

    existing_ts_val = float(existing_ts or 0.0)
    candidate_ts = float(event_ts or 0.0)
    candidate_is_newer = candidate_ts > existing_ts_val
    merged_ts = candidate_ts if candidate_is_newer else existing_ts_val
    merged_ts_utc = str(event_ts_utc or "").strip() if candidate_is_newer else str(existing_ts_utc or "").strip()
    richer = candidate_is_newer or not str(existing_title or "").strip()

    payload_out = payload_json
    if not richer and str(existing_payload_json or "").strip():
        payload_out = str(existing_payload_json or "").strip()

    cur.execute(
        """
        UPDATE commstat_artifacts
        SET subtype=?,
            event_ts=?,
            event_ts_utc=?,
            from_call=?,
            target=?,
            report_group=?,
            grid=?,
            state_code=?,
            scope=?,
            transport_mode=?,
            status_label=?,
            alert_color=?,
            title=?,
            body_text=?,
            remarks_text=?,
            brevity_code=?,
            brevity_summary=?,
            source_first=?,
            source_last=?,
            sources_json=?,
            source_count=?,
            source_refs_json=?,
            external_ids_json=?,
            payload_json=?,
            updated_ts=?
        WHERE id=?
        """,
        (
            subtype_txt if richer and subtype_txt else str(existing_subtype or "").strip().upper(),
            merged_ts,
            merged_ts_utc,
            from_txt or str(existing_from or "").strip().upper(),
            target_txt or str(existing_target or "").strip().upper(),
            report_group_txt or str(existing_group or "").strip().upper(),
            grid_txt or str(existing_grid or "").strip().upper(),
            state_txt or str(existing_state or "").strip().upper(),
            scope_txt or str(existing_scope or "").strip(),
            merge_transport_modes(existing_transport, transport_txt),
            status_txt if richer and status_txt else normalize_status_label(existing_status),
            alert_color_txt if richer and alert_color_txt else str(existing_alert_color or "").strip().upper(),
            title_txt if richer and title_txt else str(existing_title or "").strip(),
            body_txt if richer and body_txt else str(existing_body or "").strip(),
            remarks_txt if richer and remarks_txt else str(existing_remarks or "").strip(),
            brevity_code_txt if richer and brevity_code_txt else str(existing_brevity_code or "").strip().upper(),
            brevity_summary_txt if richer and brevity_summary_txt else str(existing_brevity_summary or "").strip(),
            str(source_first or source_txt or "").strip().upper(),
            source_txt or str(_source_last or source_first or "").strip().upper(),
            json.dumps(source_list, separators=(",", ":"), ensure_ascii=True),
            len(source_list),
            json.dumps(ref_list, separators=(",", ":"), ensure_ascii=True),
            json.dumps(ext_id_list, separators=(",", ":"), ensure_ascii=True),
            payload_out,
            now_ts,
            int(row_id),
        ),
    )
    return bool(cur.rowcount)


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {str(row[1] or "").strip().lower() for row in cur.fetchall()}
    for name, ddl in columns.items():
        if str(name).strip().lower() in existing:
            continue
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _safe_json_array_loads(value: object) -> List[str]:
    txt = str(value or "").strip()
    if not txt:
        return []
    try:
        data = json.loads(txt)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out: List[str] = []
    for item in data:
        normalized = str(item or "").strip()
        if normalized:
            out.append(normalized)
    return out


def _dedupe_str_list(values: Iterable[object]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _time_bucket(event_ts: object, bucket_seconds: int) -> int:
    try:
        ts = float(event_ts or 0.0)
    except Exception:
        ts = 0.0
    bucket = max(1, int(bucket_seconds or 60))
    if ts <= 0:
        return 0
    return int(ts // float(bucket))


def _normalize_body(value: object) -> str:
    text = str(value or "").strip()
    return " ".join(text.split()).upper()


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
