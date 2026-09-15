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
from freqinout.core.fio_spotter_watch_engine import (
    SpotterWatchMatcher,
    canonical_criteria,
    compile_spotter_watch,
    parse_criteria,
)


WATCH_KINDS = (
    "callsign", "group", "source", "kind", "topic", "keyword", "status", "location", "structured"
)
MATCH_MODES = ("contains", "whole-word", "exact")
PRIORITIES = ("routine", "watch", "important", "urgent")
MAX_WATCH_ROWS = 500
MAX_ENABLED_WATCHES = 100
MAX_TOTAL_WATCHES = 500
MAX_ACTIVITY_ROWS = 500


@dataclass(frozen=True)
class SpotterWatchSaveResult:
    id: int
    created: bool
    enabled: bool


class SpotterWatchStoreError(ValueError):
    """Safe, user-facing watch persistence error with a stable reason code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = str(code)


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


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
            criteria_json TEXT NOT NULL DEFAULT '{}',
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
    _ensure_column(conn, "fio_spotter_watches", "criteria_json", "TEXT NOT NULL DEFAULT '{}'")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fio_spotter_watches_enabled_kind "
        "ON fio_spotter_watches(enabled, watch_kind, priority, updated_ts DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fio_spotter_watches_expiry "
        "ON fio_spotter_watches(expires_ts, enabled)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fio_spotter_watch_matches (
            watch_id INTEGER NOT NULL,
            message_id TEXT NOT NULL,
            matched_ts REAL NOT NULL,
            PRIMARY KEY (watch_id, message_id),
            FOREIGN KEY (watch_id) REFERENCES fio_spotter_watches(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fio_spotter_watch_matches_recent "
        "ON fio_spotter_watch_matches(matched_ts DESC, watch_id)"
    )


def _watch_signature(*, kind: str, pattern: str, mode: str, criteria_json: object,
                     source_families: object, source_radio_ids: object) -> str:
    conditions = parse_criteria(criteria_json)
    if not conditions:
        # Parsing through the engine gives legacy patterns the same case,
        # whitespace, and group/callsign normalization as structured rules.
        from freqinout.core.fio_spotter_watch_engine import WatchCondition
        conditions = (WatchCondition(kind, pattern, mode),)
    return json.dumps({
        "conditions": [
            {"kind": item.kind, "pattern": item.pattern, "match_mode": item.match_mode}
            for item in conditions
        ],
        "source_families": sorted((str(value).casefold() for value in _load_list(source_families)), key=str.casefold),
        "source_radio_ids": sorted((str(value).casefold() for value in _load_list(source_radio_ids)), key=str.casefold),
    }, sort_keys=True, separators=(",", ":"))


def _watch_signature_from_row(row: Mapping[str, Any]) -> str:
    return _watch_signature(
        kind=str(row["watch_kind"] or "keyword"),
        pattern=str(row["pattern"] or ""),
        mode=str(row["match_mode"] or "contains"),
        criteria_json=row["criteria_json"] if "criteria_json" in row.keys() else "{}",
        source_families=row["source_families_json"] or "[]",
        source_radio_ids=row["source_radio_ids_json"] or "[]",
    )


def save_spotter_watch(
    values: Mapping[str, Any], *, db_path: str | Path | None = None
) -> SpotterWatchSaveResult:
    name = str(values.get("name", "") or "").strip()
    pattern = str(values.get("pattern", "") or "").strip()
    kind = str(values.get("watch_kind", "keyword") or "keyword").strip().lower()
    mode = str(values.get("match_mode", "contains") or "contains").strip().lower().replace("_", "-")
    if mode == "word":
        mode = "whole-word"
    priority = str(values.get("priority", "watch") or "watch").strip().lower()
    if not name:
        raise SpotterWatchStoreError("invalid_name", "name is required")
    raw_criteria = values.get("criteria", values.get("criteria_json"))
    try:
        criteria = canonical_criteria(raw_criteria)
    except ValueError as exc:
        raise SpotterWatchStoreError("invalid_criteria", str(exc)) from exc
    if criteria:
        kind, pattern, mode = "structured", "", "contains"
    if not pattern and not criteria:
        raise SpotterWatchStoreError("invalid_pattern", "pattern is required")
    if kind not in WATCH_KINDS:
        raise SpotterWatchStoreError("invalid_kind", f"unsupported watch kind: {kind}")
    if mode not in MATCH_MODES:
        raise SpotterWatchStoreError("invalid_match_mode", f"unsupported watch match mode: {mode}")
    if priority not in PRIORITIES:
        raise SpotterWatchStoreError("invalid_priority", f"unsupported priority: {priority}")
    now = time.time()
    criteria_json = json.dumps(
        {"conditions": [
            {"kind": condition.kind, "pattern": condition.pattern, "match_mode": condition.match_mode}
            for condition in criteria
        ]}, sort_keys=True, separators=(",", ":")
    ) if criteria else "{}"
    path = _db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_sqlite(path, timeout=5.0, row_factory=sqlite3.Row)
    try:
        try:
            # Counts, duplicate check, and mutation are one serialized write
            # operation. BEGIN IMMEDIATE prevents concurrent saves passing
            # the caps at the same time.
            conn.execute("BEGIN IMMEDIATE")
            ensure_fio_spotter_schema(conn)
        except sqlite3.OperationalError as exc:
            conn.rollback()
            raise SpotterWatchStoreError("database_busy", "Watch storage is busy; try again.") from exc
        row_id = int(values.get("id", 0) or 0)
        existing = conn.execute("SELECT * FROM fio_spotter_watches WHERE id=?", (row_id,)).fetchone() if row_id > 0 else None
        total_count = int(conn.execute("SELECT COUNT(*) FROM fio_spotter_watches").fetchone()[0])
        enabled_count = int(conn.execute("SELECT COUNT(*) FROM fio_spotter_watches WHERE enabled != 0").fetchone()[0])
        raw_enabled = values.get("enabled", True)
        desired_enabled = not (isinstance(raw_enabled, str) and raw_enabled.strip().casefold() in {"", "0", "false", "no", "off", "disabled"}) and bool(raw_enabled)
        if existing is None and total_count >= MAX_TOTAL_WATCHES:
            raise SpotterWatchStoreError("total_cap", f"At most {MAX_TOTAL_WATCHES} watches can be stored.")
        if desired_enabled and enabled_count >= MAX_ENABLED_WATCHES and (existing is None or not bool(existing["enabled"])):
            raise SpotterWatchStoreError("enabled_cap", f"At most {MAX_ENABLED_WATCHES} watches may be enabled.")
        payload = (
            name,
            kind,
            pattern,
            mode,
            priority,
            criteria_json,
            _json_list(values.get("source_families")),
            _json_list(values.get("source_radio_ids")),
            str(values.get("notification_mode", "in-app") or "in-app").strip(),
            max(0.0, float(values.get("expires_ts", 0.0) or 0.0)),
            1 if desired_enabled else 0,
            str(values.get("health", "ready") or "ready").strip(),
            str(values.get("import_source", "fio-spotter") or "fio-spotter").strip(),
            str(values.get("notes", "") or "").strip(),
            now,
        )
        candidate_signature = _watch_signature(
            kind=kind, pattern=pattern, mode=mode, criteria_json=criteria_json,
            source_families=payload[6], source_radio_ids=payload[7],
        )
        for duplicate in conn.execute("SELECT * FROM fio_spotter_watches WHERE id != ?", (row_id,)).fetchall():
            try:
                duplicate_signature = _watch_signature_from_row(duplicate)
            except (TypeError, ValueError):
                continue
            if duplicate_signature == candidate_signature:
                raise SpotterWatchStoreError("duplicate", "An equivalent watch already exists.")
        if existing:
            conn.execute(
                """
                UPDATE fio_spotter_watches
                   SET name=?, watch_kind=?, pattern=?, match_mode=?, priority=?, criteria_json=?,
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
                    (name, watch_kind, pattern, match_mode, priority, criteria_json, source_families_json,
                     source_radio_ids_json, notification_mode, expires_ts, enabled, health,
                     import_source, notes, created_ts, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload[:-1] + (now, now),
            )
            row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            created = True
        conn.commit()
        return SpotterWatchSaveResult(row_id, created, bool(payload[10]))
    except SpotterWatchStoreError:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        raise SpotterWatchStoreError("save_failed", "Watch could not be saved.") from exc
    except sqlite3.OperationalError as exc:
        conn.rollback()
        raise SpotterWatchStoreError("database_busy", "Watch storage is busy; try again.") from exc
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
        try:
            item["criteria"] = [
                {"kind": condition.kind, "pattern": condition.pattern, "match_mode": condition.match_mode}
                for condition in parse_criteria(item.get("criteria_json"))
            ]
        except ValueError:
            # A malformed legacy/imported row is visible to administration,
            # but never becomes a broad match.
            item["criteria"] = []
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
    try:
        return compile_spotter_watch(watch).matches(candidate)
    except (TypeError, ValueError):
        return False


def load_spotter_watch_matcher(
    *, db_path: str | Path | None = None, enabled_only: bool = True,
    limit: int = MAX_ENABLED_WATCHES,
) -> SpotterWatchMatcher:
    """Load and compile one bounded snapshot for event-driven matching.

    The returned matcher performs no database reads. Callers should replace it
    after a watch configuration change rather than querying per candidate.
    """
    watches = list_spotter_watches(db_path=db_path, enabled_only=enabled_only, limit=limit)
    return SpotterWatchMatcher.from_snapshot(watches, limit=limit)


def record_spotter_watch_match(
    watch_id: int, *, matched_ts: float | None = None, db_path: str | Path | None = None
) -> bool:
    return bool(record_spotter_watch_matches({int(watch_id): 1}, matched_ts=matched_ts, db_path=db_path))


def record_spotter_watch_matches(
    matches: Mapping[int, int] | Sequence[int], *, matched_ts: float | None = None,
    db_path: str | Path | None = None,
) -> int:
    """Persist accumulated match counts in one bounded transaction.

    ``matches`` may be watch IDs (each counted once) or an ID-to-count mapping.
    The event matcher can therefore process many candidates without opening a
    write connection for each event.
    """
    if isinstance(matches, Mapping):
        counts = {int(key): int(value) for key, value in matches.items() if int(value) > 0}
    else:
        counts: dict[int, int] = {}
        for value in matches:
            key = int(value)
            counts[key] = counts.get(key, 0) + 1
    if not counts:
        return 0
    stamp = float(matched_ts if matched_ts is not None else time.time())
    conn = connect_sqlite(_db_path(db_path), timeout=5.0)
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_fio_spotter_schema(conn)
        updated = 0
        updated_stamp = time.time()
        for watch_id, count in counts.items():
            cur = conn.execute(
                """
                UPDATE fio_spotter_watches
                   SET last_match_ts=?, match_count=COALESCE(match_count, 0)+?,
                       health='matched', updated_ts=?
                 WHERE id=? AND enabled != 0
                """,
                (stamp, count, updated_stamp, watch_id),
            )
            updated += int(cur.rowcount or 0)
        conn.commit()
        return updated
    except sqlite3.OperationalError as exc:
        conn.rollback()
        raise SpotterWatchStoreError("database_busy", "Watch storage is busy; try again.") from exc
    finally:
        conn.close()


def record_spotter_watch_candidate_matches(
    matches: Mapping[str, Sequence[int]], *, matched_ts: float | None = None,
    db_path: str | Path | None = None,
) -> int:
    """Persist one idempotent, bounded event-time match batch.

    The `(watch_id, message_id)` key prevents a re-projection or retry from
    incrementing a watch twice. The matcher performs no reads; this function
    opens one short background transaction after the complete batch is known.
    """
    candidates = [
        (str(message_id or "").strip(), tuple(dict.fromkeys(int(value) for value in watch_ids if int(value) > 0)))
        for message_id, watch_ids in list(matches.items())[:200]
        if str(message_id or "").strip()
    ]
    if not candidates:
        return 0
    stamp = float(matched_ts if matched_ts is not None else time.time())
    conn = connect_sqlite(_db_path(db_path), timeout=5.0)
    try:
        conn.execute("BEGIN IMMEDIATE")
        counts: dict[int, int] = {}
        for message_id, watch_ids in candidates:
            for watch_id in watch_ids[:MAX_ENABLED_WATCHES]:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO fio_spotter_watch_matches "
                    "(watch_id, message_id, matched_ts) VALUES (?, ?, ?)",
                    (watch_id, message_id, stamp),
                )
                if int(cur.rowcount or 0) > 0:
                    counts[watch_id] = counts.get(watch_id, 0) + 1
        updated = 0
        updated_stamp = time.time()
        for watch_id, count in counts.items():
            cur = conn.execute(
                """
                UPDATE fio_spotter_watches
                   SET last_match_ts=?, match_count=COALESCE(match_count, 0)+?,
                       health='matched', updated_ts=?
                 WHERE id=? AND enabled != 0
                """,
                (stamp, count, updated_stamp, watch_id),
            )
            updated += int(cur.rowcount or 0)
        conn.commit()
        return updated
    except sqlite3.OperationalError as exc:
        conn.rollback()
        raise SpotterWatchStoreError("database_busy", "Watch storage is busy; try again.") from exc
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
    clauses = [
        "deleted=0",
        "archived=0",
        # Imported JS8Spotter history remains available to broader message
        # review, but Activity is the local RF observation surface.
        "NOT (source_family='spotter' AND source_label LIKE 'Imported JS8Spotter%')",
    ]
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
                       topics_json, actionable, operator_attention, confidence,
                       recommended_action, intelligence_version, intelligence_utc,
                       intelligence_json
                  FROM message_projection
                 WHERE {' AND '.join(clauses)}
                 ORDER BY event_ts DESC, received_ts DESC, message_id DESC
                 LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        finally:
            conn.close()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        item["topics"] = _load_list(item.get("topics_json"))
        try:
            intelligence = json.loads(str(item.get("intelligence_json") or "{}"))
        except (TypeError, ValueError):
            intelligence = {}
        item["intelligence"] = intelligence if isinstance(intelligence, dict) else {}
        result.append(item)
    return result
