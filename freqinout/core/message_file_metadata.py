from __future__ import annotations

import datetime
from dataclasses import dataclass
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from freqinout.core.logger import log
from freqinout.core.message_file_scanner import FileRecord


FILE_METADATA_ORIGINS = {"bbs", "flamp", "flmsg", "varac"}
FILE_METADATA_PARSER_VERSION = 8


MESSAGE_FILE_METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("source_id", "TEXT"),
    ("source_label", "TEXT"),
    ("source_family", "TEXT"),
    ("msg_type", "TEXT"),
    ("display_type", "TEXT"),
    ("status", "TEXT"),
    ("from_call", "TEXT"),
    ("to_call", "TEXT"),
    ("title", "TEXT"),
    ("rcv_display", "TEXT"),
    ("report_ts", "REAL DEFAULT 0"),
    ("age_ts_source", "TEXT"),
    ("topics_json", "TEXT"),
    ("actionable", "INTEGER DEFAULT 0"),
    ("search_text", "TEXT"),
    ("parser_version", "INTEGER NOT NULL DEFAULT 1"),
    ("indexed_ts", "REAL NOT NULL DEFAULT 0"),
)


@dataclass(frozen=True)
class CachedMessageFileMetadata:
    source_id: str = ""
    source_label: str = ""
    source_family: str = ""
    msg_type: str = ""
    display_type: str = ""
    status: str = ""
    from_call: str = ""
    to_call: str = ""
    title: str = ""
    rcv_display: str = ""
    report_ts: float = 0.0
    age_ts_source: str = "received"
    topics: tuple[str, ...] = ()
    actionable: bool = False
    search_text: str = ""

    def age_timestamp_for(self, rec: FileRecord) -> float:
        return float(self.report_ts or rec.mtime or 0.0)


@dataclass(frozen=True)
class CachedMessageFileRowSummary:
    source_family: str = ""
    msg_type: str = ""
    display_type: str = ""
    status: str = ""
    from_call: str = ""
    to_call: str = ""
    title: str = ""
    rcv_ts: float = 0.0
    report_ts: float = 0.0
    age_ts_source: str = "received"
    topics: tuple[str, ...] = ()
    actionable: bool = False
    search_text: str = ""


def _cached_table_title(value: object, *, limit: int = 60) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def cached_message_file_row_summary(
    rec: FileRecord,
    raw: Mapping[str, object] | None,
    *,
    fallback_origin: str = "",
    fallback_title: str = "",
    title_limit: int = 60,
) -> CachedMessageFileRowSummary | None:
    cached = normalize_cached_message_file_metadata(raw)
    if cached is None:
        return None
    fallback_title_text = re.sub(r"\s+", " ", str(fallback_title or "").strip())
    fallback_is_operator_content = bool(fallback_title_text and fallback_title_text != rec.path.name)
    has_operator_content = any(
        (
            cached.msg_type,
            cached.display_type,
            cached.from_call,
            cached.to_call,
            cached.title,
            fallback_is_operator_content,
            cached.report_ts,
            cached.topics,
            cached.actionable,
            cached.search_text,
        )
    )
    if not has_operator_content:
        return None
    origin = str(fallback_origin or rec.origin or "").strip()
    msg_type = cached.msg_type or origin.upper()
    title = _cached_table_title(cached.title or fallback_title or rec.path.name, limit=title_limit)
    return CachedMessageFileRowSummary(
        source_family=cached.source_family,
        msg_type=msg_type,
        display_type=cached.display_type,
        status=cached.status,
        from_call=cached.from_call,
        to_call=cached.to_call,
        title=title,
        rcv_ts=cached.age_timestamp_for(rec),
        report_ts=cached.report_ts,
        age_ts_source=cached.age_ts_source,
        topics=cached.topics,
        actionable=cached.actionable,
        search_text=cached.search_text,
    )


def file_metadata_key(rec: FileRecord) -> tuple[str, str, float, int]:
    return (
        str(rec.origin or "").strip().lower(),
        str(rec.path),
        float(rec.mtime or 0.0),
        int(rec.size or 0),
    )


def normalize_cached_message_file_metadata(raw: Mapping[str, object] | None) -> CachedMessageFileMetadata | None:
    if not isinstance(raw, Mapping) or not raw:
        return None
    try:
        topics_raw = json.loads(str(raw.get("topics_json", "[]") or "[]"))
    except Exception:
        topics_raw = []
    topics = tuple(str(t) for t in topics_raw if str(t or "").strip()) if isinstance(topics_raw, list) else ()
    report_ts = float(raw.get("report_ts") or 0.0)
    age_ts_source = str(raw.get("age_ts_source") or ("report" if report_ts else "received")).strip().lower()
    if age_ts_source not in {"report", "received"}:
        age_ts_source = "report" if report_ts else "received"
    return CachedMessageFileMetadata(
        source_id=str(raw.get("source_id", "") or ""),
        source_label=str(raw.get("source_label", "") or ""),
        source_family=str(raw.get("source_family", "") or ""),
        msg_type=str(raw.get("msg_type", "") or ""),
        display_type=str(raw.get("display_type", "") or ""),
        status=str(raw.get("status", "") or ""),
        from_call=str(raw.get("from_call", "") or ""),
        to_call=str(raw.get("to_call", "") or ""),
        title=str(raw.get("title", "") or ""),
        rcv_display=str(raw.get("rcv_display", "") or ""),
        report_ts=report_ts,
        age_ts_source=age_ts_source,
        topics=topics,
        actionable=bool(int(raw.get("actionable", 0) or 0)),
        search_text=str(raw.get("search_text", "") or ""),
    )


def form_report_timestamp_from_summary(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    m = re.search(r"\b(\d{6})[-_\sT]?(\d{4,6})z?\b", text, flags=re.IGNORECASE)
    if m:
        yymmdd = m.group(1)
        hhmmss = m.group(2)
        try:
            year = 2000 + int(yymmdd[:2])
            dt = datetime.datetime(
                year,
                int(yymmdd[2:4]),
                int(yymmdd[4:6]),
                int(hhmmss[:2]),
                int(hhmmss[2:4]),
                int(hhmmss[4:6]) if len(hhmmss) >= 6 else 0,
                tzinfo=datetime.timezone.utc,
            )
            return float(dt.timestamp())
        except Exception:
            return 0.0
    m = re.search(r"\b(\d{8})[-_\sT]?(\d{4,6})z?\b", text, flags=re.IGNORECASE)
    if m:
        yyyymmdd = m.group(1)
        hhmmss = m.group(2)
        try:
            dt = datetime.datetime(
                int(yyyymmdd[:4]),
                int(yyyymmdd[4:6]),
                int(yyyymmdd[6:8]),
                int(hhmmss[:2]),
                int(hhmmss[2:4]),
                int(hhmmss[4:6]) if len(hhmmss) >= 6 else 0,
                tzinfo=datetime.timezone.utc,
            )
            return float(dt.timestamp())
        except Exception:
            return 0.0
    return 0.0


def ensure_message_file_metadata_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_file_metadata (
            origin TEXT NOT NULL,
            path TEXT NOT NULL,
            mtime REAL NOT NULL,
            size INTEGER NOT NULL,
            source_id TEXT,
            source_label TEXT,
            source_family TEXT,
            msg_type TEXT,
            display_type TEXT,
            status TEXT,
            from_call TEXT,
            to_call TEXT,
            title TEXT,
            rcv_display TEXT,
            report_ts REAL DEFAULT 0,
            age_ts_source TEXT,
            topics_json TEXT,
            actionable INTEGER DEFAULT 0,
            search_text TEXT,
            parser_version INTEGER NOT NULL DEFAULT 1,
            indexed_ts REAL NOT NULL,
            PRIMARY KEY (origin, path, mtime, size)
        )
        """
    )
    existing = {str(row[1] or "") for row in cur.execute("PRAGMA table_info(message_file_metadata)").fetchall()}
    for name, definition in MESSAGE_FILE_METADATA_COLUMNS:
        if name in existing:
            continue
        try:
            cur.execute(f"ALTER TABLE message_file_metadata ADD COLUMN {name} {definition}")
        except Exception:
            pass
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_message_file_metadata_origin_mtime ON message_file_metadata(origin, mtime DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_message_file_metadata_title ON message_file_metadata(title)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_message_file_metadata_source ON message_file_metadata(source_id, origin)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_message_file_metadata_report_ts ON message_file_metadata(origin, report_ts DESC)"
    )


def save_message_file_metadata_from_rows(
    db_path: str | Path,
    rows: Sequence[Any],
    *,
    ensure_scan_cache_table=None,
) -> None:
    payload: list[tuple[object, ...]] = []
    indexed_ts = time.time()
    for row in rows:
        rec = getattr(row, "payload", None)
        if not isinstance(rec, FileRecord):
            continue
        origin = str(getattr(rec, "origin", "") or getattr(row, "origin", "") or "").strip().lower()
        if origin not in FILE_METADATA_ORIGINS:
            continue
        topics = tuple(getattr(row, "topics", ()) or ())
        report_ts = float(getattr(row, "report_ts", 0.0) or 0.0)
        age_ts_source = str(getattr(row, "age_ts_source", "") or ("report" if report_ts else "received")).strip()
        payload.append(
            (
                origin,
                str(rec.path),
                float(rec.mtime or 0.0),
                int(rec.size or 0),
                str(getattr(rec, "source_id", "") or ""),
                str(getattr(rec, "source_label", "") or ""),
                str(getattr(row, "origin", "") or origin),
                str(getattr(row, "msg_type", "") or ""),
                str(getattr(row, "display_type", "") or ""),
                str(getattr(row, "status", "") or ""),
                str(getattr(row, "from_call", "") or ""),
                str(getattr(row, "to_call", "") or ""),
                str(getattr(row, "title", "") or ""),
                str(getattr(row, "rcv_display", "") or ""),
                report_ts,
                age_ts_source,
                json.dumps([str(t) for t in topics if str(t or "").strip()], separators=(",", ":"), ensure_ascii=True),
                1 if bool(getattr(row, "actionable", False)) else 0,
                str(getattr(row, "search_text", "") or ""),
                FILE_METADATA_PARSER_VERSION,
                indexed_ts,
            )
        )
    if not payload:
        return
    try:
        if callable(ensure_scan_cache_table):
            ensure_scan_cache_table()
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        ensure_message_file_metadata_table(conn)
        cur.executemany(
            """
            INSERT OR REPLACE INTO message_file_metadata (
                origin, path, mtime, size, source_id, source_label, source_family, msg_type, display_type, status, from_call, to_call,
                title, rcv_display, report_ts, age_ts_source, topics_json, actionable, search_text, parser_version, indexed_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        try:
            scan_count = int(cur.execute("SELECT COUNT(*) FROM message_scan_cache").fetchone()[0] or 0)
        except Exception:
            scan_count = 0
        if scan_count > 0:
            cur.execute(
                """
                DELETE FROM message_file_metadata
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM message_scan_cache sc
                    WHERE sc.origin=message_file_metadata.origin
                      AND sc.path=message_file_metadata.path
                      AND sc.mtime=message_file_metadata.mtime
                      AND sc.size=message_file_metadata.size
                )
                """
            )
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug("MessageFileMetadata: failed to save metadata cache: %s", e)


def load_message_file_metadata_map(
    db_path: str | Path,
    records: Dict[str, List[FileRecord]],
    *,
    ensure_scan_cache_table=None,
) -> Dict[tuple, Dict[str, object]]:
    db_path = Path(db_path)
    if not db_path.exists():
        return {}
    wanted: dict[str, set[tuple]] = {}
    for recs in (records or {}).values():
        for rec in recs:
            if not isinstance(rec, FileRecord):
                continue
            key = file_metadata_key(rec)
            wanted.setdefault(str(rec.path), set()).add(key)
    if not wanted:
        return {}
    out: Dict[tuple, Dict[str, object]] = {}
    paths = list(wanted.keys())
    try:
        if callable(ensure_scan_cache_table):
            ensure_scan_cache_table()
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        ensure_message_file_metadata_table(conn)
        for idx in range(0, len(paths), 200):
            chunk = paths[idx : idx + 200]
            placeholders = ",".join("?" for _ in chunk)
            cur.execute(
                f"""
                SELECT origin, path, mtime, size, source_family, msg_type, display_type, status,
                       from_call, to_call, title, rcv_display, report_ts, age_ts_source, topics_json, actionable, search_text,
                       parser_version, source_id, source_label
                FROM message_file_metadata
                WHERE path IN ({placeholders})
                  AND parser_version=?
                """,
                tuple(chunk) + (FILE_METADATA_PARSER_VERSION,),
            )
            for row in cur.fetchall():
                key = (str(row[0] or "").strip().lower(), str(row[1] or ""), float(row[2] or 0.0), int(row[3] or 0))
                if key not in wanted.get(str(row[1] or ""), set()):
                    continue
                out[key] = {
                    "source_family": str(row[4] or ""),
                    "msg_type": str(row[5] or ""),
                    "display_type": str(row[6] or ""),
                    "status": str(row[7] or ""),
                    "from_call": str(row[8] or ""),
                    "to_call": str(row[9] or ""),
                    "title": str(row[10] or ""),
                    "rcv_display": str(row[11] or ""),
                    "report_ts": float(row[12] or 0.0),
                    "age_ts_source": str(row[13] or ""),
                    "topics_json": str(row[14] or "[]"),
                    "actionable": int(row[15] or 0),
                    "search_text": str(row[16] or ""),
                    "source_id": str(row[18] or ""),
                    "source_label": str(row[19] or ""),
                }
        conn.close()
    except Exception as e:
        log.debug("MessageFileMetadata: failed to load metadata cache: %s", e)
        return {}
    return out


def load_existing_message_file_metadata_records(
    db_path: str | Path,
    *,
    limit: int = 10000,
    ensure_scan_cache_table=None,
) -> tuple[Dict[str, List[FileRecord]], Dict[tuple, Dict[str, object]]]:
    db_path = Path(db_path)
    if not db_path.exists():
        return {}, {}
    records: Dict[str, List[FileRecord]] = {origin: [] for origin in sorted(FILE_METADATA_ORIGINS)}
    metadata: Dict[tuple, Dict[str, object]] = {}
    try:
        if callable(ensure_scan_cache_table):
            ensure_scan_cache_table()
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        ensure_message_file_metadata_table(conn)
        cur.execute(
            """
            SELECT origin, path, mtime, size, source_family, msg_type, display_type, status,
                   from_call, to_call, title, rcv_display, report_ts, age_ts_source, topics_json, actionable, search_text,
                   parser_version, source_id, source_label
            FROM message_file_metadata
            WHERE parser_version=?
            ORDER BY COALESCE(report_ts, 0) DESC, mtime DESC, indexed_ts DESC
            LIMIT ?
            """,
            (FILE_METADATA_PARSER_VERSION, max(0, int(limit or 0))),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        log.debug("MessageFileMetadata: failed to load existing metadata records: %s", e)
        return {}, {}
    for row in rows:
        origin = str(row[0] or "").strip().lower()
        if origin not in FILE_METADATA_ORIGINS:
            continue
        path = Path(str(row[1] or ""))
        if not path.exists() or not path.is_file():
            continue
        try:
            stat = path.stat()
        except Exception:
            continue
        row_mtime = float(row[2] or 0.0)
        row_size = int(row[3] or 0)
        if int(stat.st_size) != row_size or abs(float(stat.st_mtime) - row_mtime) > 0.001:
            continue
        rec = FileRecord(
            path=path,
            origin=origin,
            size=row_size,
            mtime=row_mtime,
            source_id=str(row[18] or ""),
            source_label=str(row[19] or ""),
        )
        key = file_metadata_key(rec)
        metadata[key] = {
            "source_family": str(row[4] or ""),
            "msg_type": str(row[5] or ""),
            "display_type": str(row[6] or ""),
            "status": str(row[7] or ""),
            "from_call": str(row[8] or ""),
            "to_call": str(row[9] or ""),
            "title": str(row[10] or ""),
            "rcv_display": str(row[11] or ""),
            "report_ts": float(row[12] or 0.0),
            "age_ts_source": str(row[13] or ""),
            "topics_json": str(row[14] or "[]"),
            "actionable": int(row[15] or 0),
            "search_text": str(row[16] or ""),
            "source_id": str(row[18] or ""),
            "source_label": str(row[19] or ""),
        }
        records.setdefault(origin, []).append(rec)
    return {origin: recs for origin, recs in records.items() if recs}, metadata


def has_stale_message_file_metadata(
    db_path: str | Path,
    records: Dict[str, List[FileRecord]],
) -> bool:
    db_path = Path(db_path)
    if not db_path.exists():
        return False
    wanted_by_path: dict[str, set[tuple[str, str, float, int]]] = {}
    for recs in (records or {}).values():
        for rec in recs:
            if not isinstance(rec, FileRecord):
                continue
            key = file_metadata_key(rec)
            wanted_by_path.setdefault(str(rec.path), set()).add(key)
    if not wanted_by_path:
        return False
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        ensure_message_file_metadata_table(conn)
        paths = list(wanted_by_path.keys())
        for idx in range(0, len(paths), 200):
            chunk = paths[idx : idx + 200]
            placeholders = ",".join("?" for _ in chunk)
            cur.execute(
                f"""
                SELECT origin, path, mtime, size, parser_version
                FROM message_file_metadata
                WHERE path IN ({placeholders})
                  AND parser_version<>?
                """,
                tuple(chunk) + (FILE_METADATA_PARSER_VERSION,),
            )
            for row in cur.fetchall():
                key = (
                    str(row[0] or "").strip().lower(),
                    str(row[1] or ""),
                    float(row[2] or 0.0),
                    int(row[3] or 0),
                )
                if key in wanted_by_path.get(str(row[1] or ""), set()):
                    conn.close()
                    return True
        conn.close()
    except Exception as e:
        log.debug("MessageFileMetadata: failed stale metadata check: %s", e)
        return False
    return False


def delete_file_cache_entries(db_path: str | Path, rec: FileRecord) -> None:
    try:
        origin = str(rec.origin or "").strip().lower()
        path = str(rec.path)
        mtime = float(rec.mtime or 0.0)
        size = int(rec.size or 0)
    except Exception:
        return
    if not origin or not path:
        return
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        for table in ("message_file_metadata", "message_scan_cache", "message_signature_cache", "message_read_state"):
            exists = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not exists:
                continue
            cur.execute(
                f"""
                DELETE FROM {table}
                WHERE origin=? AND path=? AND mtime=? AND size=?
                """,
                (origin, path, mtime, size),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug("MessageFileMetadata: failed to delete file cache entries for %s: %s", path, e)


def remove_file_record_from_groups(
    files: Mapping[str, Sequence[FileRecord]],
    rec: FileRecord,
) -> Dict[str, List[FileRecord]]:
    """Return file groups with exactly the supplied file identity removed."""
    origin = str(getattr(rec, "origin", "") or "").strip().lower()
    rec_key = file_metadata_key(rec)
    out: Dict[str, List[FileRecord]] = {}
    for group, records in (files or {}).items():
        group_key = str(group or "").strip().lower()
        if group_key == origin:
            out[group] = [
                item
                for item in records
                if not isinstance(item, FileRecord) or file_metadata_key(item) != rec_key
            ]
        else:
            out[group] = list(records)
    return out
