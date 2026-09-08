from __future__ import annotations

"""Bounded station-owned storage and query helpers for FIO Spotter."""

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from freqinout.core.js8_expect_store import default_expect_db_path
from freqinout.core.perf_metrics import PerfSpan
from freqinout.core.sqlite_utils import connect_sqlite, connect_sqlite_readonly, table_exists


WATCH_KINDS = ("callsign", "group", "topic", "keyword", "status", "location")
MATCH_MODES = ("contains", "whole-word", "exact")
PRIORITIES = ("routine", "watch", "important", "urgent")
MAX_WATCH_ROWS = 500
MAX_ACTIVITY_ROWS = 500


@dataclass(frozen=True)
class SpotterWatchSaveResult:
    id: int
    created: bool
    enabled: bool


def _db_path(value: str | Path | None) -> Path:
    return Path(value) if value is not None else default_expect_db_path()


def _json_list(value: object) -> str:
    if isinstance(value, str):
        items = [part.strip() for part in value.replace(";", ",").split(",")]
    else:
        try:
            items = [str(part or "").strip() for part in value or ()]  # type: ignore[union-attr]
        except Exception:
            items = []
    return json.dumps(list(dict.fromkeys(item for item in items if item)), separators=(",", ":"))


def _load_list(value: object) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item or "").strip() for item in parsed if str(item or "").strip()]


def ensure_fio_spotter_schema(conn) -> None:
    """Apply the additive, idempotent Slice 3 watch schema."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fio_spotter_watches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            watch_kind TEXT NOT NULL,
            pattern TEXT NOT NULL,
            match_mode TEXT NOT NULL DEFAULT 'contains',
            priority TEXT NOT NULL DEFAULT 'watch',
            source_families_json TEXT NOT NULL DEFAULT '[]',
            source_radio_ids_json TEXT NOT NULL DEFAULT '[]',
            notification_mode TEXT NOT NULL DEFAULT 'in-app',
            expires_ts REAL NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            last_match_ts REAL NOT NULL DEFAULT 0,
            match_count INTEGER NOT NULL DEFAULT 0,
            health TEXT NOT NULL DEFAULT 'ready',
            import_source TEXT,
            notes TEXT,
            created_ts REAL NOT NULL,
            updated_ts REAL NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fio_spotter_watches_enabled_kind "
        "ON fio_spotter_watches(enabled, watch_kind, priority, updated_ts DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fio_spotter_watches_expiry "
        "ON fio_spotter_watches(expires_ts, enabled)"
    )


def save_spotter_watch(
    values: Mapping[str, Any], *, db_path: str | Path | None = None
) -> SpotterWatchSaveResult:
    name = str(values.get("name", "") or "").strip()
    pattern = str(values.get("pattern", "") or "").strip()
    kind = str(values.get("watch_kind", "keyword") or "keyword").strip().lower()
    mode = str(values.get("match_mode", "contains") or "contains").strip().lower()
    priority = str(values.get("priority", "watch") or "watch").strip().lower()
    if not name:
        raise ValueError("name is required")
    if not pattern:
        raise ValueError("pattern is required")
    if kind not in WATCH_KINDS:
        raise ValueError(f"unsupported watch kind: {kind}")
    if mode not in MATCH_MODES:
        raise ValueError(f"unsupported match mode: {mode}")
    if priority not in PRIORITIES:
        raise ValueError(f"unsupported priority: {priority}")
    now = time.time()
    path = _db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_sqlite(path, row_factory=sqlite3.Row)
    try:
        ensure_fio_spotter_schema(conn)
        row_id = int(values.get("id", 0) or 0)
        existing = conn.execute(
            "SELECT id FROM fio_spotter_watches WHERE id=?", (row_id,)
        ).fetchone() if row_id > 0 else None
        payload = (
            name,
            kind,
            pattern,
            mode,
            priority,
            _json_list(values.get("source_families")),
            _json_list(values.get("source_radio_ids")),
            str(values.get("notification_mode", "in-app") or "in-app").strip(),
            max(0.0, float(values.get("expires_ts", 0.0) or 0.0)),
            1 if bool(values.get("enabled", True)) else 0,
            str(values.get("health", "ready") or "ready").strip(),
            str(values.get("import_source", "fio-spotter") or "fio-spotter").strip(),
            str(values.get("notes", "") or "").strip(),
            now,
        )
        if existing:
            conn.execute(
                """
                UPDATE fio_spotter_watches
                   SET name=?, watch_kind=?, pattern=?, match_mode=?, priority=?,
                       source_families_json=?, source_radio_ids_json=?, notification_mode=?,
                       expires_ts=?, enabled=?, health=?, import_source=?, notes=?, updated_ts=?
                 WHERE id=?
                """,
                payload + (row_id,),
            )
            created = False
        else:
            conn.execute(
                """
                INSERT INTO fio_spotter_watches
                    (name, watch_kind, pattern, match_mode, priority, source_families_json,
                     source_radio_ids_json, notification_mode, expires_ts, enabled, health,
                     import_source, notes, created_ts, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload[:-1] + (now, now),
            )
            row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            created = True
        conn.commit()
        return SpotterWatchSaveResult(row_id, created, bool(payload[9]))
    finally:
        conn.close()


def list_spotter_watches(
    *, db_path: str | Path | None = None, enabled_only: bool = False, limit: int = 200
) -> list[dict[str, Any]]:
    path = _db_path(db_path)
    if not path.exists():
        return []
    conn = connect_sqlite_readonly(path, row_factory=sqlite3.Row)
    try:
        if not table_exists(conn, "fio_spotter_watches"):
            return []
        where = "WHERE enabled != 0" if enabled_only else ""
        rows = conn.execute(
            f"""
            SELECT * FROM fio_spotter_watches
            {where}
            ORDER BY enabled DESC,
                     CASE priority WHEN 'urgent' THEN 0 WHEN 'important' THEN 1
                                   WHEN 'watch' THEN 2 ELSE 3 END,
                     updated_ts DESC, id DESC
            LIMIT ?
            """,
            (max(1, min(MAX_WATCH_ROWS, int(limit or 200))),),
        ).fetchall()
    finally:
        conn.close()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = {key: row[key] for key in row.keys()} if hasattr(row, "keys") else {}
        item["source_families"] = _load_list(item.get("source_families_json"))
        item["source_radio_ids"] = _load_list(item.get("source_radio_ids_json"))
        item["enabled"] = bool(item.get("enabled", 0))
        result.append(item)
    return result


def delete_spotter_watch(watch_id: int, *, db_path: str | Path | None = None) -> bool:
    conn = connect_sqlite(_db_path(db_path))
    try:
        ensure_fio_spotter_schema(conn)
        cur = conn.execute("DELETE FROM fio_spotter_watches WHERE id=?", (int(watch_id),))
        conn.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        conn.close()


def watch_matches(watch: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    """Side-effect-free preview matcher used by administration and ingest wiring."""

    kind = str(watch.get("watch_kind", "keyword") or "keyword").strip().lower()
    pattern = str(watch.get("pattern", "") or "").strip()
    if not pattern:
        return False
    fields: dict[str, Sequence[object]] = {
        "callsign": (candidate.get("from_call"), candidate.get("to_call")),
        "group": (candidate.get("group_name"), candidate.get("to_call")),
        "topic": (candidate.get("topic"), candidate.get("topics")),
        "status": (candidate.get("status"), candidate.get("severity")),
        "location": (candidate.get("state_code"), candidate.get("grid")),
        "keyword": (candidate.get("summary"), candidate.get("body_text"), candidate.get("search_text")),
    }
    haystack = " ".join(str(value or "") for value in fields.get(kind, fields["keyword"])).strip()
    mode = str(watch.get("match_mode", "contains") or "contains").strip().lower()
    if mode == "exact":
        return haystack.casefold() == pattern.casefold()
    if mode == "whole-word":
        return bool(re.search(rf"(?<!\w){re.escape(pattern)}(?!\w)", haystack, re.IGNORECASE))
    return pattern.casefold() in haystack.casefold()


def record_spotter_watch_match(
    watch_id: int, *, matched_ts: float | None = None, db_path: str | Path | None = None
) -> bool:
    conn = connect_sqlite(_db_path(db_path))
    try:
        ensure_fio_spotter_schema(conn)
        cur = conn.execute(
            """
            UPDATE fio_spotter_watches
               SET last_match_ts=?, match_count=COALESCE(match_count, 0)+1,
                   health='matched', updated_ts=?
             WHERE id=? AND enabled != 0
            """,
            (float(matched_ts or time.time()), time.time(), int(watch_id)),
        )
        conn.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        conn.close()


def list_spotter_activity(
    *,
    db_path: str | Path | None = None,
    source_families: Sequence[str] = ("spotter", "js8", "js8call"),
    group_name: str = "",
    search_text: str = "",
    received_after_ts: float = 0.0,
    limit: int = 200,
) -> list[dict[str, Any]]:
    path = _db_path(db_path)
    if not path.exists():
        return []
    requested_sources = sorted({
        str(value or "").strip().lower()
        for value in source_families
        if str(value or "").strip()
    })
    clauses = ["deleted=0", "archived=0"]
    params: list[object] = []
    if len(requested_sources) == 1:
        clauses.append("source_family=?")
        params.append(requested_sources[0])
    elif requested_sources:
        clauses.append(f"source_family IN ({','.join('?' for _ in requested_sources)})")
        params.extend(requested_sources)
    if group_name:
        clauses.append("group_name=?")
        params.append(str(group_name).lstrip("@"))
    if search_text:
        clauses.append("search_text LIKE ?")
        params.append(f"%{str(search_text).lower()}%")
    if received_after_ts:
        clauses.append("COALESCE(NULLIF(received_ts, 0), event_ts, 0) >= ?")
        params.append(float(received_after_ts))
    bounded_limit = max(1, min(MAX_ACTIVITY_ROWS, int(limit or 200)))
    params.append(bounded_limit)
    with PerfSpan(
        "fio_spotter.activity_query",
        meta={"limit": bounded_limit, "sources": requested_sources},
        min_ms=10.0,
    ):
        conn = connect_sqlite_readonly(path, row_factory=sqlite3.Row)
        try:
            if not table_exists(conn, "message_projection"):
                return []
            rows = conn.execute(
                f"""
                SELECT message_id, source_family, source_label, radio_id,
                       app_instance_id, message_type, display_type, status,
                       severity, read_state, from_call, to_call, group_name,
                       state_code, grid, event_ts, received_ts, subject, summary,
                       body_preview AS preview, body_preview AS body_text,
                       actionable, operator_attention, recommended_action
                  FROM message_projection
                 WHERE {' AND '.join(clauses)}
                 ORDER BY event_ts DESC, received_ts DESC, message_id DESC
                 LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        finally:
            conn.close()
    return [{key: row[key] for key in row.keys()} for row in rows]
