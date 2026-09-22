from __future__ import annotations

import json
import datetime as dt
import re
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

from freqinout.core.group_utils import normalize_group_name
from freqinout.core.operator_identity import (
    canonical_callsign,
    callsigns_for_operator,
    ensure_operator_identity_schema,
    resolve_operator_identity,
)


FOCUS_KINDS = frozenset({"callsign", "group", "event", "topic", "geography", "band", "source"})
CALLSIGN_SHAPE_RE = re.compile(r"^[A-Z0-9]{3,12}(?:/[A-Z0-9]{1,6})?$")
GRID_RE = re.compile(r"^[A-R]{2}[0-9]{2}(?:[A-X]{2})?$", re.IGNORECASE)
BAND_RE = re.compile(r"^(?:160|80|60|40|30|20|17|15|12|10|6|2|1\.25|70CM|33CM|23CM)M?$", re.IGNORECASE)


@dataclass(frozen=True)
class OpsFocus:
    kind: str
    canonical_id: str
    display_label: str
    query_text: str = ""
    provenance: str = "index"
    operator_id: str = ""


@dataclass(frozen=True)
class OpsFocusSuggestion:
    focus: OpsFocus
    primary_text: str
    secondary_text: str
    score: int


@dataclass(frozen=True)
class OpsHistoricalSummary:
    entity_kind: str
    entity_id: str
    latest_received_at: float = 0.0
    latest_event_at: float = 0.0
    latest_message_ref: str = ""
    latest_observation_ref: str = ""
    latest_status_at_receipt: str = ""
    latest_source: str = ""
    latest_group: str = ""
    latest_summary: str = ""
    last_heard_by_source: tuple[str, ...] = ()
    retained_source_summary: tuple[str, ...] = ()
    scope_mismatch_notes: tuple[str, ...] = ()
    retention_boundary: float = 0.0


@dataclass(frozen=True)
class OpsFocusSnapshot:
    focus: OpsFocus
    generated_at: float
    current_count: int = 0
    current_unread: int = 0
    current_actionable: int = 0
    current_latest_at: float = 0.0
    current_scope_summary: str = ""
    historical_summary: OpsHistoricalSummary | None = None
    identity: Mapping[str, object] = field(default_factory=dict)
    aliases: tuple[str, ...] = ()
    actions: tuple[str, ...] = ()
    missing_data_reasons: tuple[str, ...] = ()


def _ops_focus_schema_ready(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM sqlite_master
         WHERE type='table'
           AND name IN ('ops_focus_entities','ops_focus_message_entities','ops_focus_backfill_state')
        """
    ).fetchone()
    return bool(row and int(row[0] or 0) == 3)


def ensure_ops_focus_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_focus_entities (
            kind TEXT NOT NULL,
            canonical_id TEXT NOT NULL,
            display_label TEXT NOT NULL,
            search_text TEXT NOT NULL,
            latest_received_ts REAL NOT NULL DEFAULT 0,
            latest_event_ts REAL NOT NULL DEFAULT 0,
            latest_message_id TEXT,
            latest_status TEXT,
            latest_source TEXT,
            latest_group TEXT,
            latest_summary TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_ts REAL NOT NULL DEFAULT 0,
            PRIMARY KEY(kind, canonical_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_focus_message_entities (
            message_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            canonical_id TEXT NOT NULL,
            received_ts REAL NOT NULL DEFAULT 0,
            event_ts REAL NOT NULL DEFAULT 0,
            source_family TEXT,
            group_name TEXT,
            read_state TEXT,
            actionable INTEGER NOT NULL DEFAULT 0,
            archived INTEGER NOT NULL DEFAULT 0,
            deleted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(message_id, kind, canonical_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_focus_backfill_state (
            projection TEXT PRIMARY KEY,
            last_rowid INTEGER NOT NULL DEFAULT 0,
            complete INTEGER NOT NULL DEFAULT 0,
            updated_ts REAL NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_focus_search "
        "ON ops_focus_entities(kind, search_text COLLATE NOCASE)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_focus_latest "
        "ON ops_focus_entities(latest_received_ts DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_focus_bridge_entity "
        "ON ops_focus_message_entities(kind, canonical_id, received_ts DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_focus_bridge_scope "
        "ON ops_focus_message_entities(kind, canonical_id, source_family, group_name, received_ts DESC)"
    )


def _value(record: object, name: str, default: object = "") -> object:
    if isinstance(record, Mapping):
        return record.get(name, default)
    try:
        return record[name]  # type: ignore[index]
    except Exception:
        return getattr(record, name, default)


def _safe_text(value: object) -> str:
    return str(value or "").encode("utf-8", "replace").decode("utf-8", "replace")


def _json_value(value: object, default: object) -> object:
    if isinstance(value, (dict, list, tuple)):
        return value
    try:
        parsed = json.loads(str(value or ""))
        return parsed
    except Exception:
        return default


def _tokens(value: object) -> tuple[str, ...]:
    parsed = _json_value(value, value)
    if isinstance(parsed, str):
        candidates: Iterable[object] = re.split(r"[,;|]", parsed)
    elif isinstance(parsed, Mapping):
        candidates = parsed.values()
    elif isinstance(parsed, Iterable):
        candidates = parsed
    else:
        candidates = ()
    result: list[str] = []
    for candidate in candidates:
        if isinstance(candidate, (list, tuple, set)):
            nested = candidate
        else:
            nested = (candidate,)
        for item in nested:
            text = str(item or "").strip()
            if text and text.lower() not in {entry.lower() for entry in result}:
                result.append(text)
    return tuple(result)


def _entity_values(record: object) -> tuple[tuple[str, str, str, dict[str, object]], ...]:
    received_ts = float(_value(record, "received_ts", 0.0) or _value(record, "event_ts", 0.0) or 0.0)
    event_ts = float(_value(record, "event_ts", 0.0) or received_ts)
    found: dict[tuple[str, str], tuple[str, str, str, dict[str, object]]] = {}

    for field_name in ("from_call", "to_call"):
        callsign = canonical_callsign(_value(record, field_name, ""))
        if not callsign:
            continue
        identity = None
        # Resolution is injected later in index_message_for_ops_focus where the
        # connection is available.
        found[("callsign", callsign)] = ("callsign", callsign, callsign, {"observed_callsign": callsign})

    group = normalize_group_name(_value(record, "group_name", ""))
    if group:
        found[("group", group)] = ("group", group, group, {})

    for topic in _tokens(_value(record, "topics_json", _value(record, "topics", ()))):
        canonical = topic.strip().lower()
        if canonical:
            found[("topic", canonical)] = ("topic", canonical, topic.strip().title(), {})

    state = str(_value(record, "state_code", "") or "").strip().upper()
    grid = str(_value(record, "grid", "") or "").strip().upper()
    if state:
        found[("geography", f"state:{state}")] = ("geography", f"state:{state}", state, {"type": "state"})
    if grid:
        found[("geography", f"grid:{grid}")] = ("geography", f"grid:{grid}", grid, {"type": "grid"})

    source = str(_value(record, "source_family", "") or "").strip().lower()
    source_label = str(_value(record, "source_label", "") or source).strip()
    if source:
        found[("source", source)] = ("source", source, source_label or source.title(), {})

    entities = _json_value(_value(record, "entities_json", _value(record, "entities", {})), {})
    intelligence = _json_value(
        _value(record, "intelligence_json", _value(record, "intelligence", {})), {}
    )
    for mapping in (entities, intelligence):
        if not isinstance(mapping, Mapping):
            continue
        for key in ("event", "incident", "storyline", "event_name", "incident_name"):
            for label in _tokens(mapping.get(key, ())):
                canonical = label.strip().lower()
                if canonical:
                    found[("event", canonical)] = ("event", canonical, label.strip(), {})
        for key in ("band", "bands"):
            for label in _tokens(mapping.get(key, ())):
                canonical = label.strip().upper()
                if canonical:
                    found[("band", canonical)] = ("band", canonical, canonical, {})
    return tuple(found.values())


def index_message_for_ops_focus(
    conn: sqlite3.Connection,
    record: object,
    *,
    identity_schema_ready: bool = False,
) -> None:
    """Idempotently project one message into compact focus/entity tables."""
    message_id = str(_value(record, "message_id", "") or "").strip()
    if not message_id:
        return
    received_ts = float(_value(record, "received_ts", 0.0) or _value(record, "event_ts", 0.0) or 0.0)
    event_ts = float(_value(record, "event_ts", 0.0) or received_ts)
    source = _safe_text(_value(record, "source_family", "")).strip().lower()
    group = _safe_text(normalize_group_name(_value(record, "group_name", "")))
    read_state = _safe_text(_value(record, "read_state", "")).strip().lower()
    actionable = 1 if bool(_value(record, "actionable", False)) else 0
    archived = 1 if bool(_value(record, "archived", False)) else 0
    policy_hidden = not bool(_value(record, "inbox_visible", True))
    deleted = 1 if bool(_value(record, "deleted", False)) or policy_hidden else 0
    status = _safe_text(_value(record, "status", "")).strip()
    summary = _safe_text(
        _value(record, "summary", "")
        or _value(record, "subject", "")
        or _value(record, "body_preview", "")
        or ""
    ).strip()[:280]

    conn.execute("DELETE FROM ops_focus_message_entities WHERE message_id=?", (message_id,))
    entities = list(_entity_values(record))
    resolved_entities: list[tuple[str, str, str, dict[str, object]]] = []
    for kind, canonical_id, display_label, metadata in entities:
        if kind == "callsign":
            identity = resolve_operator_identity(
                conn,
                canonical_id,
                at_utc=event_ts or received_ts,
                schema_ready=identity_schema_ready,
        )
            if identity is not None:
                metadata = dict(metadata)
                metadata["matched_callsign"] = canonical_id
                metadata["operator_id"] = identity.operator_id
                canonical_id = identity.operator_id
                display_label = identity.current_callsign
        resolved_entities.append((kind, canonical_id, display_label, metadata))

    for kind, canonical_id, display_label, metadata in resolved_entities:
        metadata = dict(metadata)
        metadata["reference_kind"] = "message"
        canonical_id = _safe_text(canonical_id)
        display_label = _safe_text(display_label)
        search_text = " ".join(
            part for part in (display_label, canonical_id, str(metadata.get("matched_callsign", ""))) if part
        ).encode("utf-8", "replace").decode("utf-8", "replace").lower()
        conn.execute(
            """
            INSERT INTO ops_focus_message_entities(
                message_id, kind, canonical_id, received_ts, event_ts,
                source_family, group_name, read_state, actionable, archived, deleted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id, kind, canonical_id, received_ts, event_ts, source,
                group, read_state, actionable, archived, deleted,
            ),
        )
        if deleted:
            previous = conn.execute(
                """
                SELECT p.received_ts, p.event_ts, p.message_id, p.status,
                       p.source_family, p.group_name,
                       COALESCE(NULLIF(p.summary,''),NULLIF(p.subject,''),p.body_preview,'')
                  FROM ops_focus_message_entities b
                  JOIN message_projection p ON p.message_id=b.message_id
                 WHERE b.kind=? AND b.canonical_id=? AND b.deleted=0 AND p.deleted=0
                 ORDER BY b.received_ts DESC, b.message_id DESC LIMIT 1
                """,
                (kind, canonical_id),
            ).fetchone()
            if previous:
                conn.execute(
                    """
                    UPDATE ops_focus_entities
                       SET latest_received_ts=?, latest_event_ts=?, latest_message_id=?,
                           latest_status=?, latest_source=?, latest_group=?, latest_summary=?,
                           metadata_json=?, updated_ts=?
                     WHERE kind=? AND canonical_id=?
                    """,
                    tuple(previous)
                    + (json.dumps(metadata, sort_keys=True), time.time(), kind, canonical_id),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM ops_focus_entities
                     WHERE kind=? AND canonical_id=? AND latest_message_id=?
                       AND metadata_json LIKE '%"reference_kind": "message"%'
                    """,
                    (kind, canonical_id, message_id),
                )
            continue
        conn.execute(
            """
            INSERT INTO ops_focus_entities(
                kind, canonical_id, display_label, search_text,
                latest_received_ts, latest_event_ts, latest_message_id,
                latest_status, latest_source, latest_group, latest_summary,
                metadata_json, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(kind, canonical_id) DO UPDATE SET
                display_label=excluded.display_label,
                search_text=excluded.search_text,
                latest_received_ts=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_received_ts ELSE ops_focus_entities.latest_received_ts END,
                latest_event_ts=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_event_ts ELSE ops_focus_entities.latest_event_ts END,
                latest_message_id=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_message_id ELSE ops_focus_entities.latest_message_id END,
                latest_status=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_status ELSE ops_focus_entities.latest_status END,
                latest_source=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_source ELSE ops_focus_entities.latest_source END,
                latest_group=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_group ELSE ops_focus_entities.latest_group END,
                latest_summary=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_summary ELSE ops_focus_entities.latest_summary END,
                metadata_json=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.metadata_json ELSE ops_focus_entities.metadata_json END,
                updated_ts=excluded.updated_ts
            """,
            (
                kind,
                canonical_id,
                display_label,
                search_text,
                received_ts,
                event_ts,
                message_id,
                status,
                source,
                group,
                summary,
                json.dumps(metadata, sort_keys=True),
                time.time(),
            ),
        )


def remove_message_from_ops_focus(conn: sqlite3.Connection, message_id: object) -> None:
    """Remove an obsolete projection id and repair affected compact summaries.

    Canonical-identity migrations can re-parent every external receipt away
    from an older source-scoped message id.  The hot projection row may then be
    removed, but the compact Ops bridge must not retain or advertise that id.
    """

    key = _safe_text(message_id).strip()
    if not key or not _ops_focus_schema_ready(conn):
        return
    affected = conn.execute(
        "SELECT DISTINCT kind, canonical_id FROM ops_focus_message_entities WHERE message_id=?",
        (key,),
    ).fetchall()
    conn.execute("DELETE FROM ops_focus_message_entities WHERE message_id=?", (key,))
    for row in affected:
        kind, canonical_id = _safe_text(row[0]), _safe_text(row[1])
        current = conn.execute(
            "SELECT latest_message_id, metadata_json FROM ops_focus_entities WHERE kind=? AND canonical_id=?",
            (kind, canonical_id),
        ).fetchone()
        if current is None or _safe_text(current[0]) != key:
            continue
        previous = conn.execute(
            """
            SELECT p.received_ts, p.event_ts, p.message_id, p.status,
                   p.source_family, p.group_name,
                   COALESCE(NULLIF(p.summary,''),NULLIF(p.subject,''),p.body_preview,'')
              FROM ops_focus_message_entities b
              JOIN message_projection p ON p.message_id=b.message_id
             WHERE b.kind=? AND b.canonical_id=? AND b.deleted=0 AND p.deleted=0
             ORDER BY b.received_ts DESC, b.message_id DESC LIMIT 1
            """,
            (kind, canonical_id),
        ).fetchone()
        if previous is None:
            conn.execute(
                "DELETE FROM ops_focus_entities WHERE kind=? AND canonical_id=? AND latest_message_id=?",
                (kind, canonical_id, key),
            )
            continue
        conn.execute(
            """
            UPDATE ops_focus_entities
               SET latest_received_ts=?, latest_event_ts=?, latest_message_id=?,
                   latest_status=?, latest_source=?, latest_group=?, latest_summary=?,
                   updated_ts=?
             WHERE kind=? AND canonical_id=? AND latest_message_id=?
            """,
            tuple(previous) + (time.time(), kind, canonical_id, key),
        )


def index_observation_for_ops_focus(conn: sqlite3.Connection, record: object) -> None:
    """Incrementally add compact discovery/Last Known facts for one observation."""
    if not _ops_focus_schema_ready(conn):
        ensure_ops_focus_schema(conn)
    observation_id = str(_value(record, "observation_id", "") or "").strip()
    if not observation_id:
        return
    received_ts = _epoch(_value(record, "received_utc", ""))
    event_ts = _epoch(_value(record, "event_utc", "")) or received_ts
    source = _safe_text(_value(record, "source_family", "")).strip().lower()
    status = _safe_text(_value(record, "status", "")).strip()
    summary = _safe_text(_value(record, "summary", "") or _value(record, "subject", "")).strip()[:280]
    groups = tuple(
        normalize_group_name(group)
        for group in _tokens(_value(record, "groups_json", _value(record, "groups", ())))
    )
    topics = _tokens(
        _value(record, "observed_topics_json", _value(record, "observed_topics", ()))
    )
    state = str(_value(record, "state", "") or "").strip().upper()
    grid = str(_value(record, "grid", "") or "").strip().upper()
    entities: dict[tuple[str, str], tuple[str, str, str, dict[str, object]]] = {}
    call = canonical_callsign(_value(record, "from_call", ""))
    if call:
        identity = resolve_operator_identity(conn, call, at_utc=event_ts or received_ts)
        if identity is not None:
            entities[("callsign", identity.operator_id)] = (
                "callsign", identity.operator_id, identity.current_callsign,
                {"matched_callsign": call, "operator_id": identity.operator_id},
            )
        else:
            entities[("callsign", call)] = ("callsign", call, call, {"observed_callsign": call})
    for group in groups:
        if group:
            entities[("group", group)] = ("group", group, group, {})
    for topic in topics:
        canonical = str(topic).strip().lower()
        if canonical:
            entities[("topic", canonical)] = ("topic", canonical, str(topic).strip().title(), {})
    if state:
        entities[("geography", f"state:{state}")] = ("geography", f"state:{state}", state, {"type": "state"})
    if grid:
        entities[("geography", f"grid:{grid}")] = ("geography", f"grid:{grid}", grid, {"type": "grid"})
    if source:
        entities[("source", source)] = ("source", source, source.title(), {})
    provenance = _json_value(
        _value(record, "provenance_json", _value(record, "provenance", {})), {}
    )
    if isinstance(provenance, Mapping):
        for key in ("event", "incident", "storyline", "event_name", "incident_name"):
            for label in _tokens(provenance.get(key, ())):
                canonical = label.strip().lower()
                if canonical:
                    entities[("event", canonical)] = ("event", canonical, label.strip(), {})
        for label in _tokens(provenance.get("band", provenance.get("bands", ()))):
            canonical = label.strip().upper()
            if canonical:
                entities[("band", canonical)] = ("band", canonical, canonical, {})
    group = next((item for item in groups if item), "")
    for kind, canonical_id, display_label, raw_metadata in entities.values():
        metadata = dict(raw_metadata)
        metadata["reference_kind"] = "observation"
        search_text = " ".join(
            part for part in (display_label, canonical_id, str(metadata.get("matched_callsign", ""))) if part
        ).lower()
        conn.execute(
            """
            INSERT INTO ops_focus_entities(
                kind, canonical_id, display_label, search_text,
                latest_received_ts, latest_event_ts, latest_message_id,
                latest_status, latest_source, latest_group, latest_summary,
                metadata_json, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(kind, canonical_id) DO UPDATE SET
                display_label=excluded.display_label,
                search_text=excluded.search_text,
                latest_received_ts=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_received_ts ELSE ops_focus_entities.latest_received_ts END,
                latest_event_ts=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_event_ts ELSE ops_focus_entities.latest_event_ts END,
                latest_message_id=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_message_id ELSE ops_focus_entities.latest_message_id END,
                latest_status=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_status ELSE ops_focus_entities.latest_status END,
                latest_source=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_source ELSE ops_focus_entities.latest_source END,
                latest_group=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_group ELSE ops_focus_entities.latest_group END,
                latest_summary=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.latest_summary ELSE ops_focus_entities.latest_summary END,
                metadata_json=CASE WHEN excluded.latest_received_ts >= ops_focus_entities.latest_received_ts THEN excluded.metadata_json ELSE ops_focus_entities.metadata_json END,
                updated_ts=excluded.updated_ts
            """,
            (
                kind, canonical_id, display_label, search_text, received_ts, event_ts,
                observation_id, status, source, group, summary,
                json.dumps(metadata, sort_keys=True), time.time(),
            ),
        )


def backfill_ops_focus_observation_index(
    conn: sqlite3.Connection, *, batch_size: int = 500
) -> tuple[int, bool]:
    """Index one resumable observation-projection batch."""
    if not _ops_focus_schema_ready(conn):
        ensure_ops_focus_schema(conn)
    if not _table_exists(conn, "observation_projection"):
        return 0, True
    state = conn.execute(
        "SELECT last_rowid, complete FROM ops_focus_backfill_state WHERE projection='observation_projection'"
    ).fetchone()
    last_rowid = int(state[0] or 0) if state else 0
    cursor = conn.execute(
        "SELECT rowid, * FROM observation_projection WHERE rowid>? ORDER BY rowid LIMIT ?",
        (last_rowid, max(1, min(5000, int(batch_size or 500)))),
    )
    rows = cursor.fetchall()
    names = [str(item[0]) for item in (cursor.description or ())]
    for row in rows:
        index_observation_for_ops_focus(
            conn,
            row if isinstance(row, sqlite3.Row) else dict(zip(names, row)),
        )
    if rows:
        last_rowid = int(rows[-1][0])
    complete = len(rows) < max(1, min(5000, int(batch_size or 500)))
    conn.execute(
        """
        INSERT INTO ops_focus_backfill_state(projection, last_rowid, complete, updated_ts)
        VALUES ('observation_projection', ?, ?, ?)
        ON CONFLICT(projection) DO UPDATE SET
            last_rowid=excluded.last_rowid,
            complete=excluded.complete,
            updated_ts=excluded.updated_ts
        """,
        (last_rowid, 1 if complete else 0, time.time()),
    )
    return len(rows), complete


def backfill_ops_focus_index(conn: sqlite3.Connection, *, batch_size: int = 500) -> tuple[int, bool]:
    """Index one resumable message-projection batch."""
    if not _ops_focus_schema_ready(conn):
        ensure_ops_focus_schema(conn)
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_projection'"
    ).fetchone()
    if table is None:
        return 0, True
    state = conn.execute(
        "SELECT last_rowid, complete FROM ops_focus_backfill_state WHERE projection='message_projection'"
    ).fetchone()
    last_rowid = int(state[0] or 0) if state else 0
    cursor = conn.execute(
        "SELECT rowid, * FROM message_projection WHERE rowid>? ORDER BY rowid LIMIT ?",
        (last_rowid, max(1, min(5000, int(batch_size or 500)))),
    )
    rows = cursor.fetchall()
    column_names = [str(item[0]) for item in (cursor.description or ())]
    for row in rows:
        index_message_for_ops_focus(
            conn,
            row if isinstance(row, sqlite3.Row) else dict(zip(column_names, row)),
            identity_schema_ready=True,
        )
    if rows:
        last_rowid = int(rows[-1][0])
    complete = len(rows) < max(1, min(5000, int(batch_size or 500)))
    conn.execute(
        """
        INSERT INTO ops_focus_backfill_state(projection, last_rowid, complete, updated_ts)
        VALUES ('message_projection', ?, ?, ?)
        ON CONFLICT(projection) DO UPDATE SET
            last_rowid=excluded.last_rowid,
            complete=excluded.complete,
            updated_ts=excluded.updated_ts
        """,
        (last_rowid, 1 if complete else 0, time.time()),
    )
    return len(rows), complete


def _operator_suggestions(conn: sqlite3.Connection, query: str) -> list[OpsFocusSuggestion]:
    if not _table_exists(conn, "operator_callsign_history"):
        ensure_operator_identity_schema(conn, backfill_operator_rows=False)
    q = query.upper()
    alias_rows = conn.execute(
        """
        SELECT h.callsign, h.operator_id, i.current_callsign,
               h.effective_from, h.effective_to
          FROM operator_callsign_history h
          JOIN operator_identities i ON i.operator_id=h.operator_id
         WHERE h.callsign LIKE ? COLLATE NOCASE
            OR i.current_callsign LIKE ? COLLATE NOCASE
         ORDER BY CASE WHEN h.callsign=? COLLATE NOCASE OR i.current_callsign=? COLLATE NOCASE THEN 0
                       WHEN h.callsign LIKE ? COLLATE NOCASE OR i.current_callsign LIKE ? COLLATE NOCASE THEN 1
                       ELSE 2 END,
                  h.effective_from DESC
         LIMIT 32
        """,
        (f"%{q}%", f"%{q}%", q, q, f"{q}%", f"{q}%"),
    ).fetchall()
    operator_ids = tuple(dict.fromkeys(str(row[1]) for row in alias_rows))
    metadata: dict[str, tuple[object, ...]] = {}
    if operator_ids:
        placeholders = ",".join("?" for _ in operator_ids)
        for row in conn.execute(
            f"""
            SELECT operator_id, COALESCE(name,''), COALESCE(group_role,''),
                   COALESCE(group1,''), COALESCE(last_seen_utc,'')
              FROM operator_checkins
             WHERE operator_id IN ({placeholders})
            """,
            operator_ids,
        ).fetchall():
            metadata[str(row[0])] = tuple(row[1:])
    seen_ids: set[str] = set()
    result: list[OpsFocusSuggestion] = []
    for alias, operator_id, current_callsign, _effective_from, _effective_to in alias_rows:
        alias = str(alias or "").upper()
        current_callsign = str(current_callsign or "").upper()
        operator_id = str(operator_id)
        if operator_id in seen_ids:
            continue
        seen_ids.add(operator_id)
        name, role, group, last_seen = metadata.get(operator_id, ("", "", "", ""))
        former = alias if alias != current_callsign else ""
        secondary = " · ".join(
            part
            for part in (
                str(name or "").strip(),
                " ".join(part for part in (str(group or "").strip(), str(role or "").strip()) if part),
                f"formerly {former}" if former else "",
                f"last seen {last_seen}" if last_seen else "",
            )
            if part
        )
        exact = q in {alias, current_callsign}
        prefix = alias.startswith(q) or current_callsign.startswith(q)
        result.append(
            OpsFocusSuggestion(
                focus=OpsFocus(
                    kind="callsign",
                    canonical_id=operator_id,
                    display_label=current_callsign,
                    query_text=query,
                    provenance="operator_history",
                    operator_id=operator_id,
                ),
                primary_text=current_callsign,
                secondary_text=secondary,
                score=1000 if exact else 900 if prefix else 700,
            )
        )
    return result


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _table_columns(conn: sqlite3.Connection, name: str) -> set[str]:
    return {str(row[1] or "") for row in conn.execute(f"PRAGMA table_info({name})").fetchall()}


def _epoch(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        if len(text) == 8 and text.isdigit():
            parsed = dt.datetime.strptime(text, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
        else:
            parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _callsign_latest_evidence(
    conn: sqlite3.Connection,
    callsigns: Sequence[str],
    *,
    operator_last_seen: object = "",
) -> tuple[tuple[object, ...] | None, tuple[str, ...]]:
    """Return one latest summary and bounded per-source heard facts."""
    clean = tuple(dict.fromkeys(canonical_callsign(call) for call in callsigns if canonical_callsign(call)))
    if not clean:
        return None, ()
    marks = ",".join("?" for _ in clean)
    candidates: list[tuple[object, ...]] = []
    heard: list[str] = []
    if _table_exists(conn, "observation_projection"):
        rows = conn.execute(
            f"""
            SELECT source_family, observation_id, received_utc, event_utc,
                   status, COALESCE(NULLIF(summary,''),NULLIF(subject,''),''),
                   COALESCE(NULLIF(to_target,''),''), state, grid
              FROM observation_projection
             WHERE UPPER(from_call) IN ({marks})
             ORDER BY COALESCE(NULLIF(received_utc,''),event_utc,'') DESC
             LIMIT 24
            """,
            clean,
        ).fetchall()
        seen_sources: set[str] = set()
        for row in rows:
            received = _epoch(row[2]) or _epoch(row[3])
            event = _epoch(row[3])
            source = str(row[0] or "observation")
            if source not in seen_sources:
                seen_sources.add(source)
                heard.append(f"{source}:{received:g}")
            candidates.append(
                (received, event, "", row[4], source, row[6], row[5], str(row[1] or ""))
            )
    if _table_exists(conn, "spotter_station_status"):
        status_columns = _table_columns(conn, "spotter_station_status")
        source_expr = "status_source" if "status_source" in status_columns else "'spotter'"
        row = conn.execute(
            f"""
            SELECT updated_utc_ts, status_key, {source_expr}, status_label, from_call
              FROM spotter_station_status
             WHERE UPPER(from_call) IN ({marks})
             ORDER BY updated_utc_ts DESC LIMIT 1
            """,
            clean,
        ).fetchone()
        if row:
            candidates.append((float(row[0] or 0), 0.0, "", row[1], row[2], "", row[3], f"status:{row[4]}"))
    if _table_exists(conn, "sitrep_latest_by_callsign"):
        sitrep_columns = _table_columns(conn, "sitrep_latest_by_callsign")
        source_expr = "source_last" if "source_last" in sitrep_columns else "'sitrep'"
        row = conn.execute(
            f"""
            SELECT latest_event_ts, effective_status, latest_report_group,
                   {source_expr}, COALESCE(NULLIF(latest_brevity_summary,''),NULLIF(latest_remarks_text,''),''), callsign
              FROM sitrep_latest_by_callsign
             WHERE UPPER(callsign) IN ({marks})
             ORDER BY latest_event_ts DESC LIMIT 1
            """,
            clean,
        ).fetchone()
        if row:
            candidates.append((float(row[0] or 0), float(row[0] or 0), "", row[1], row[3], row[2], row[4], f"sitrep:{row[5]}"))
    last_seen_ts = _epoch(operator_last_seen)
    if last_seen_ts:
        candidates.append((last_seen_ts, last_seen_ts, "", "", "operator history", "", "Last observed operator activity", "operator:last_seen"))
    latest = max(candidates, key=lambda item: float(item[0] or 0.0), default=None)
    return latest, tuple(heard)


def search_focus_suggestions(
    conn: sqlite3.Connection,
    query: object,
    *,
    limit: int = 8,
    extra_entities: Sequence[tuple[str, str, str]] = (),
) -> tuple[OpsFocusSuggestion, ...]:
    """Return bounded categorized suggestions from compact identity/entity indexes."""
    text = str(query or "").strip()
    if not text:
        return ()
    if not _ops_focus_schema_ready(conn):
        ensure_ops_focus_schema(conn)
    suggestions = _operator_suggestions(conn, text) if len(text) >= 2 else []
    lower = text.lower()
    rows = conn.execute(
        """
        SELECT kind, canonical_id, display_label, latest_received_ts, latest_source
          FROM ops_focus_entities
         WHERE search_text LIKE ? COLLATE NOCASE
            OR search_text LIKE ? COLLATE NOCASE
         ORDER BY CASE WHEN search_text=? COLLATE NOCASE THEN 0
                       WHEN search_text LIKE ? COLLATE NOCASE THEN 1 ELSE 2 END,
                  latest_received_ts DESC
         LIMIT ?
        """,
        (f"{lower}%", f"% {lower}%", lower, f"{lower}%", max(8, int(limit) * 2)),
    ).fetchall()
    for kind, canonical_id, label, latest_ts, source in rows:
        if str(kind) == "callsign" and any(
            item.focus.canonical_id == str(canonical_id) for item in suggestions
        ):
            continue
        age = _age_text(float(latest_ts or 0.0))
        secondary = " · ".join(part for part in (str(kind).title(), str(source or ""), f"latest {age}" if age else "") if part)
        exact = lower in {str(canonical_id).lower(), str(label).lower()}
        prefix = str(label).lower().startswith(lower)
        suggestions.append(
            OpsFocusSuggestion(
                focus=OpsFocus(str(kind), str(canonical_id), str(label), text, "message_index"),
                primary_text=str(label),
                secondary_text=secondary,
                score=950 if exact else 850 if prefix else 650,
            )
        )
    for kind, canonical_id, label in extra_entities:
        if kind not in FOCUS_KINDS:
            continue
        haystack = f"{canonical_id} {label}".lower()
        if lower not in haystack:
            continue
        suggestions.append(
            OpsFocusSuggestion(
                focus=OpsFocus(kind, canonical_id, label, text, "configuration"),
                primary_text=label,
                secondary_text=f"{kind.title()} · configured",
                score=925 if lower in {canonical_id.lower(), label.lower()} else 825,
            )
        )
    if CALLSIGN_SHAPE_RE.fullmatch(text.upper()) and not any(
        suggestion.focus.kind == "callsign" for suggestion in suggestions
    ):
        call = canonical_callsign(text)
        suggestions.append(
            OpsFocusSuggestion(
                focus=OpsFocus("callsign", call, call, text, "unresolved_callsign"),
                primary_text=call,
                secondary_text="Callsign · not in operator roster",
                score=500,
            )
        )
    if GRID_RE.fullmatch(text.upper()):
        grid = text.upper()
        suggestions.append(
            OpsFocusSuggestion(
                focus=OpsFocus("geography", f"grid:{grid}", grid, text, "typed_grid"),
                primary_text=grid,
                secondary_text="Geography · Maidenhead grid",
                score=760,
            )
        )
    if BAND_RE.fullmatch(text.upper()):
        band = text.upper()
        suggestions.append(
            OpsFocusSuggestion(
                focus=OpsFocus("band", band, band, text, "typed_band"),
                primary_text=band,
                secondary_text="Band · RF focus",
                score=740,
            )
        )
    deduped: dict[tuple[str, str], OpsFocusSuggestion] = {}
    for suggestion in suggestions:
        key = (suggestion.focus.kind, suggestion.focus.canonical_id)
        if key not in deduped or suggestion.score > deduped[key].score:
            deduped[key] = suggestion
    return tuple(
        sorted(deduped.values(), key=lambda item: (-item.score, item.focus.kind, item.primary_text))[
            : max(1, min(20, int(limit or 8)))
        ]
    )


def build_focus_snapshot(
    conn: sqlite3.Connection,
    focus: OpsFocus,
    *,
    age_seconds: int = 24 * 60 * 60,
    group_filter: str = "",
    source_filter: str = "",
    now: float | None = None,
) -> OpsFocusSnapshot:
    if not _ops_focus_schema_ready(conn):
        ensure_ops_focus_schema(conn)
    now_ts = float(now if now is not None else time.time())
    canonical_id = focus.canonical_id
    operator_id = focus.operator_id
    aliases: tuple[str, ...] = ()
    identity_data: dict[str, object] = {}
    if focus.kind == "callsign":
        identity = resolve_operator_identity(conn, focus.display_label)
        if identity is None and operator_id:
            row = conn.execute(
                "SELECT current_callsign FROM operator_identities WHERE operator_id=?",
                (operator_id,),
            ).fetchone()
            current_call = str(row[0]) if row else focus.display_label
        elif identity is not None:
            operator_id = identity.operator_id
            current_call = identity.current_callsign
        else:
            current_call = focus.display_label
        if operator_id:
            canonical_id = operator_id
            aliases = callsigns_for_operator(conn, operator_id)
            row = conn.execute(
                """
                SELECT COALESCE(name,''), COALESCE(state,''), COALESCE(grid,''),
                       COALESCE(group_role,''), COALESCE(groups_json,''),
                       COALESCE(trusted,0), COALESCE(last_seen_utc,'')
                  FROM operator_checkins WHERE operator_id=? LIMIT 1
                """,
                (operator_id,),
            ).fetchone()
            if row:
                identity_data = {
                    "callsign": current_call,
                    "name": row[0],
                    "state": row[1],
                    "grid": row[2],
                    "role": row[3],
                    "groups": tuple(_tokens(row[4])),
                    "trusted": bool(row[5]),
                    "last_seen": row[6],
                }

    clauses = ["kind=?", "canonical_id=?", "deleted=0", "archived=0"]
    params: list[object] = [focus.kind, canonical_id]
    if age_seconds > 0:
        clauses.append("received_ts>=?")
        params.append(now_ts - int(age_seconds))
    if group_filter:
        clauses.append("UPPER(group_name)=?")
        params.append(normalize_group_name(group_filter))
    if source_filter:
        clauses.append("LOWER(source_family)=?")
        params.append(str(source_filter).strip().lower())
    current = conn.execute(
        f"""
        SELECT COUNT(*),
               SUM(CASE WHEN read_state IN ('new','unread') THEN 1 ELSE 0 END),
               SUM(CASE WHEN actionable=1 THEN 1 ELSE 0 END),
               MAX(received_ts)
          FROM ops_focus_message_entities
         WHERE {' AND '.join(clauses)}
        """,
        tuple(params),
    ).fetchone()
    count = int(current[0] or 0)
    unread = int(current[1] or 0)
    actionable = int(current[2] or 0)
    current_latest = float(current[3] or 0.0)

    latest = conn.execute(
        """
        SELECT latest_received_ts, latest_event_ts, latest_message_id,
               latest_status, latest_source, latest_group, latest_summary,
               metadata_json
          FROM ops_focus_entities WHERE kind=? AND canonical_id=?
        """,
        (focus.kind, canonical_id),
    ).fetchone()
    if latest is not None:
        latest_metadata = _json_value(latest[7] if len(latest) > 7 else "{}", {})
        latest_ref_kind = str(
            latest_metadata.get("reference_kind", "") if isinstance(latest_metadata, Mapping) else ""
        )
        bridge_state = conn.execute(
            """
            SELECT deleted FROM ops_focus_message_entities
             WHERE message_id=? AND kind=? AND canonical_id=? LIMIT 1
            """,
            (str(latest[2] or ""), focus.kind, canonical_id),
        ).fetchone()
        if latest_ref_kind == "message" or bridge_state is not None:
            if bridge_state is not None and bool(bridge_state[0]):
                latest = None
    if latest is None and _table_exists(conn, "message_projection"):
        latest = conn.execute(
            """
            SELECT p.received_ts, p.event_ts, p.message_id, p.status,
                   p.source_family, p.group_name,
                   COALESCE(NULLIF(p.summary,''),NULLIF(p.subject,''),p.body_preview,''),
                   '{"reference_kind":"message"}'
              FROM ops_focus_message_entities b
              JOIN message_projection p ON p.message_id=b.message_id
             WHERE b.kind=? AND b.canonical_id=? AND b.deleted=0 AND p.deleted=0
             ORDER BY b.received_ts DESC, b.message_id DESC
             LIMIT 1
            """,
            (focus.kind, canonical_id),
        ).fetchone()
    if latest is None and focus.kind == "callsign" and _table_exists(conn, "message_projection"):
        probe_calls = aliases or (canonical_callsign(focus.display_label),)
        clean_calls = tuple(call for call in probe_calls if call)
        if clean_calls:
            placeholders = ",".join("?" for _ in clean_calls)
            latest = conn.execute(
                f"""
                SELECT COALESCE(NULLIF(received_ts,0),event_ts,0), event_ts,
                       message_id, status, source_family, group_name,
                       COALESCE(NULLIF(summary,''),NULLIF(subject,''),body_preview,''),
                       '{"reference_kind":"message"}'
                  FROM message_projection
                 WHERE deleted=0
                   AND (UPPER(from_call) IN ({placeholders}) OR UPPER(to_call) IN ({placeholders}))
                 ORDER BY COALESCE(NULLIF(received_ts,0),event_ts,0) DESC
                 LIMIT 1
                """,
                clean_calls + clean_calls,
            ).fetchone()
    last_heard_by_source: tuple[str, ...] = ()
    if focus.kind == "callsign":
        evidence_calls = aliases or (canonical_callsign(focus.display_label),)
        observed, last_heard_by_source = _callsign_latest_evidence(
            conn,
            evidence_calls,
            operator_last_seen=identity_data.get("last_seen", ""),
        )
        if observed is not None and (
            latest is None or float(observed[0] or 0.0) > float(latest[0] or 0.0)
        ):
            latest = observed
    source_rows = conn.execute(
        """
        SELECT source_family, COUNT(*)
          FROM ops_focus_message_entities
         WHERE kind=? AND canonical_id=? AND deleted=0
         GROUP BY source_family
         ORDER BY COUNT(*) DESC, source_family
         LIMIT 8
        """,
        (focus.kind, canonical_id),
    ).fetchall()
    retained_source_summary = tuple(
        f"{str(row[0] or 'unknown')} {int(row[1] or 0)}" for row in source_rows
    )
    historical = None
    missing: list[str] = []
    if latest:
        mismatches: list[str] = []
        if group_filter and normalize_group_name(latest[5]) != normalize_group_name(group_filter):
            mismatches.append(f"last known group is {latest[5] or 'unassigned'}")
        if source_filter and str(latest[4] or "").lower() != str(source_filter).lower():
            mismatches.append(f"last known source is {latest[4] or 'unknown'}")
        boundary = conn.execute(
            "SELECT MIN(received_ts) FROM ops_focus_message_entities WHERE kind=? AND canonical_id=?",
            (focus.kind, canonical_id),
        ).fetchone()
        raw_reference_metadata = latest[7] if len(latest) > 7 else "{}"
        metadata = _json_value(raw_reference_metadata, {})
        reference_kind = str(metadata.get("reference_kind", "") if isinstance(metadata, Mapping) else "")
        raw_reference_text = str(raw_reference_metadata or "").strip()
        fallback_observation_ref = (
            raw_reference_text if raw_reference_text and not raw_reference_text.startswith("{") else ""
        )
        latest_reference = str(latest[2] or "")
        observation_ref = latest_reference if reference_kind == "observation" else fallback_observation_ref
        message_ref = "" if observation_ref else latest_reference
        historical = OpsHistoricalSummary(
            entity_kind=focus.kind,
            entity_id=canonical_id,
            latest_received_at=float(latest[0] or 0.0),
            latest_event_at=float(latest[1] or 0.0),
            latest_message_ref=message_ref,
            latest_observation_ref=observation_ref,
            latest_status_at_receipt=str(latest[3] or ""),
            latest_source=str(latest[4] or ""),
            latest_group=str(latest[5] or ""),
            latest_summary=str(latest[6] or ""),
            last_heard_by_source=last_heard_by_source,
            retained_source_summary=retained_source_summary,
            scope_mismatch_notes=tuple(mismatches),
            retention_boundary=float(boundary[0] or 0.0) if boundary else 0.0,
        )
    elif not identity_data:
        missing.append("No retained traffic found")

    age_label = "all retained traffic" if age_seconds <= 0 else _duration_label(age_seconds)
    current_summary = (
        f"{count} message{'s' if count != 1 else ''} in selected {age_label}"
        if count
        else f"No traffic received in selected {age_label}"
    )
    return OpsFocusSnapshot(
        focus=OpsFocus(
            kind=focus.kind,
            canonical_id=canonical_id,
            display_label=str(identity_data.get("callsign") or focus.display_label),
            query_text=focus.query_text,
            provenance=focus.provenance,
            operator_id=operator_id,
        ),
        generated_at=now_ts,
        current_count=count,
        current_unread=unread,
        current_actionable=actionable,
        current_latest_at=current_latest,
        current_scope_summary=current_summary,
        historical_summary=historical,
        identity=identity_data,
        aliases=aliases,
        actions=("Inbox", "Map", "Pin", "History"),
        missing_data_reasons=tuple(missing),
    )


def _duration_label(seconds: int) -> str:
    if seconds % 86400 == 0:
        days = seconds // 86400
        return f"{days} day{'s' if days != 1 else ''}"
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''}"
    return f"{seconds // 60} minutes"


def _age_text(timestamp: float, *, now: float | None = None) -> str:
    if timestamp <= 0:
        return ""
    age = max(0, int((now if now is not None else time.time()) - timestamp))
    if age >= 86400:
        return f"{age // 86400}d ago"
    if age >= 3600:
        return f"{age // 3600}h ago"
    if age >= 60:
        return f"{age // 60}m ago"
    return "now"


def format_focus_last_known(summary: OpsHistoricalSummary | None, *, now: float | None = None) -> str:
    if summary is None:
        return "No retained traffic found"
    age = _age_text(summary.latest_received_at, now=now) or "time unknown"
    parts = [f"Last known · {age}"]
    if summary.latest_source:
        parts.append(summary.latest_source)
    if summary.latest_group:
        parts.append(summary.latest_group)
    if summary.latest_status_at_receipt:
        parts.append(f"reported {summary.latest_status_at_receipt}")
    return " · ".join(parts)
