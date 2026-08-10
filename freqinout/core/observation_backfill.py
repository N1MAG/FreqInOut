from __future__ import annotations

import json
import sqlite3
import datetime as dt
from pathlib import Path
from typing import Any, Mapping, Sequence

from freqinout.core.message_file_scanner import FileRecord, IMAGE_EXTS
from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.observation_projection import (
    observation_from_local_report,
    observation_from_message_intelligence,
    utc_now_iso,
)
from freqinout.core.observation_store import ensure_observation_schema, upsert_observation_conn
from freqinout.core.sqlite_utils import connect_sqlite, table_exists


LOCAL_REPORT_SOURCE_KEY = "local_operator_reports"
SPOTTER_TRAFFIC_SOURCE_KEY = "spotter_traffic"
FORM_FILE_ORIGINS = {"flmsg", "flamp"}


def backfill_observations(
    db_path: str | Path,
    *,
    include_local_reports: bool = True,
    include_spotter_traffic: bool = True,
    batch_limit: int = 500,
) -> dict[str, int]:
    conn = connect_sqlite(db_path)
    try:
        ensure_observation_schema(conn)
        counts = {"local_reports": 0, "spotter_traffic": 0}
        with conn:
            if include_local_reports:
                counts["local_reports"] = _backfill_local_reports(conn, batch_limit=batch_limit)
            if include_spotter_traffic:
                counts["spotter_traffic"] = _backfill_spotter_traffic(conn, batch_limit=batch_limit)
        return counts
    finally:
        conn.close()


def project_message_file_observations(
    db_path: str | Path,
    records: Mapping[str, Sequence[FileRecord]],
    *,
    origins: Sequence[str] = ("flmsg", "flamp"),
    batch_limit: int = 100,
) -> int:
    allowed = {str(origin or "").strip().lower() for origin in origins if str(origin or "").strip()}
    allowed &= FORM_FILE_ORIGINS
    if not allowed:
        return 0
    candidates: list[FileRecord] = []
    for origin in sorted(allowed):
        candidates.extend(record for record in records.get(origin, ()) if isinstance(record, FileRecord))
    candidates = [
        record
        for record in candidates
        if str(record.path.suffix or "").strip().lower() not in IMAGE_EXTS
        and str(record.path or "").strip()
    ]
    candidates.sort(key=lambda record: float(record.mtime or 0.0), reverse=True)
    candidates = candidates[: max(1, int(batch_limit or 100))]
    if not candidates:
        return 0
    conn = connect_sqlite(db_path)
    try:
        ensure_observation_schema(conn)
        count = 0
        with conn:
            for record in candidates:
                observation = _observation_from_file_record(record)
                if observation is None:
                    continue
                upsert_observation_conn(conn, observation)
                count += 1
        return count
    finally:
        conn.close()


def _backfill_local_reports(conn: sqlite3.Connection, *, batch_limit: int) -> int:
    if not table_exists(conn, "local_operator_reports"):
        return 0
    last_id = _checkpoint_id(conn, LOCAL_REPORT_SOURCE_KEY)
    rows = conn.execute(
        """
        SELECT
            id,
            created_utc,
            updated_utc,
            source_kind,
            source_channel,
            net_session_id,
            callsign,
            operator_id,
            from_name,
            city,
            county,
            state,
            grid,
            lat,
            lon,
            location_source,
            location_confidence,
            status,
            topics_json,
            topic_evidence_json,
            subject,
            body,
            confirmed_state,
            followup_state,
            exercise_flag,
            source_radio_id,
            source_app,
            raw_reference,
            created_by,
            updated_by
        FROM local_operator_reports
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (last_id, max(1, int(batch_limit or 500))),
    ).fetchall()
    count = 0
    last_seen = last_id
    for row in rows:
        report = _local_report_row_to_dict(row)
        upsert_observation_conn(conn, observation_from_local_report(report))
        count += 1
        last_seen = max(last_seen, int(row[0] or 0))
    if count:
        _set_checkpoint(conn, LOCAL_REPORT_SOURCE_KEY, last_id=last_seen)
    return count


def _observation_from_file_record(record: FileRecord):
    origin = str(record.origin or "").strip().lower()
    if origin not in FORM_FILE_ORIGINS:
        return None
    text = _read_text_head(record.path, limit=131072)
    if not text:
        return None
    info = analyze_spotter_text(text) if text.strip().upper().startswith("F!") else None
    if info is None:
        from freqinout.core.message_intelligence import analyze_form_text

        info = analyze_form_text(
            text,
            source_type=origin,
            path=record.path,
        )
    mtime_utc = _mtime_utc(record.mtime)
    return observation_from_message_intelligence(
        info,
        source_ref=f"file:{record.path}",
        source_family=origin,
        received_utc=mtime_utc,
        event_utc=mtime_utc,
        status="NEW",
        extra_provenance={
            "file_path": str(record.path),
            "file_name": record.path.name,
            "file_mtime": float(record.mtime or 0.0),
            "file_size": int(record.size or 0),
            "projection_source": "message_file_scan",
        },
    )


def _backfill_spotter_traffic(conn: sqlite3.Connection, *, batch_limit: int) -> int:
    if not table_exists(conn, "spotter_traffic"):
        return 0
    last_id = _checkpoint_id(conn, SPOTTER_TRAFFIC_SOURCE_KEY)
    rows = conn.execute(
        """
        SELECT
            id,
            utc_str,
            from_call,
            to_call,
            form_id,
            raw_text,
            state,
            source_radio_id,
            js8_instance_id
        FROM spotter_traffic
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (last_id, max(1, int(batch_limit or 500))),
    ).fetchall()
    count = 0
    last_seen = last_id
    for row in rows:
        imported_id = int(row[0] or 0)
        raw_text = str(row[5] or "").strip()
        form_id = str(row[4] or "").strip()
        if not imported_id or not raw_text:
            last_seen = max(last_seen, imported_id)
            continue
        info = analyze_spotter_text(
            raw_text,
            form_name=f"MCF{form_id}" if form_id else "",
            from_call=row[2] or "",
            to_call=row[3] or "",
        )
        upsert_observation_conn(
            conn,
            observation_from_message_intelligence(
                info,
                source_ref=f"spotter_traffic:{imported_id}",
                source_family="spotter",
                source_radio_id=_int_or_none(row[7]),
                source_app=str(row[8] or "").strip(),
                received_utc=str(row[1] or ""),
                event_utc=str(row[1] or ""),
                status=str(row[6] or "").strip().upper() or "UNREAD",
                extra_provenance={"backfill_source": "spotter_traffic"},
            ),
        )
        count += 1
        last_seen = max(last_seen, imported_id)
    if count:
        _set_checkpoint(conn, SPOTTER_TRAFFIC_SOURCE_KEY, last_id=last_seen)
    return count


def _read_text_head(path: Path, *, limit: int) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            return fh.read(max(1, int(limit or 131072)))
    except Exception:
        return ""


def _mtime_utc(value: object) -> str:
    try:
        ts = float(value or 0.0)
    except Exception:
        ts = 0.0
    if ts <= 0:
        return ""
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).replace(microsecond=0).isoformat()


def _checkpoint_id(conn: sqlite3.Connection, source_key: str) -> int:
    row = conn.execute(
        """
        SELECT last_source_ref
        FROM observation_projection_checkpoint
        WHERE source_key=?
        """,
        (source_key,),
    ).fetchone()
    if not row:
        return 0
    text = str(row[0] or "").strip()
    if ":" in text:
        text = text.rsplit(":", 1)[-1]
    try:
        return int(text)
    except Exception:
        return 0


def _set_checkpoint(conn: sqlite3.Connection, source_key: str, *, last_id: int) -> None:
    source_ref = f"{source_key}:{int(last_id or 0)}"
    conn.execute(
        """
        INSERT INTO observation_projection_checkpoint (
            source_key,
            last_source_ref,
            last_event_utc,
            updated_utc
        )
        VALUES (?, ?, '', ?)
        ON CONFLICT(source_key) DO UPDATE SET
            last_source_ref=excluded.last_source_ref,
            updated_utc=excluded.updated_utc
        """,
        (source_key, source_ref, utc_now_iso()),
    )


def _local_report_row_to_dict(row: Sequence[Any]) -> Mapping[str, Any]:
    return {
        "id": int(row[0] or 0),
        "created_utc": row[1] or "",
        "updated_utc": row[2] or "",
        "source_kind": row[3] or "",
        "source_channel": row[4] or "",
        "net_session_id": row[5] or "",
        "callsign": row[6] or "",
        "operator_id": row[7] or "",
        "from_name": row[8] or "",
        "city": row[9] or "",
        "county": row[10] or "",
        "state": row[11] or "",
        "grid": row[12] or "",
        "lat": row[13],
        "lon": row[14],
        "location_source": row[15] or "",
        "location_confidence": row[16] or "",
        "status": row[17] or "",
        "topics": _json_list(row[18]),
        "topic_evidence": _json_mapping(row[19]),
        "subject": row[20] or "",
        "body": row[21] or "",
        "confirmed_state": row[22] or "",
        "followup_state": row[23] or "",
        "exercise_flag": bool(row[24]),
        "source_radio_id": row[25],
        "source_app": row[26] or "",
        "raw_reference": row[27] or "",
        "created_by": row[28] or "",
        "updated_by": row[29] or "",
    }


def _json_list(value: object) -> list[str]:
    try:
        loaded = json.loads(str(value or "[]"))
    except Exception:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(item or "").strip() for item in loaded if str(item or "").strip()]


def _json_mapping(value: object) -> Mapping[str, Any]:
    try:
        loaded = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _int_or_none(value: object) -> int | None:
    try:
        return int(value) if str(value or "").strip() else None
    except Exception:
        return None
