from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from freqinout.core.sqlite_utils import connect_sqlite

PROJECTION_SCHEMA_VERSION = 1


def utc_now_iso() -> str:
    import datetime

    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def stable_message_id(*parts: object) -> str:
    text = "|".join(str(part or "").strip() for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def content_hash(*parts: object) -> str:
    text = "\n".join(str(part or "") for part in parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class MessageSourceRecord:
    source_id: str
    source_family: str
    source_label: str = ""
    radio_id: int | None = None
    app_instance_id: str = ""
    endpoint_or_path: str = ""
    capabilities: Mapping[str, object] = field(default_factory=dict)
    provenance: Mapping[str, object] = field(default_factory=dict)
    enabled: bool = True
    last_seen_utc: str = ""
    last_ingested_utc: str = ""


@dataclass(frozen=True)
class ExternalMessageRef:
    message_id: str
    source_id: str
    external_kind: str
    external_key: str
    external_path: str = ""
    external_mtime: float = 0.0
    external_size: int = 0
    external_hash: str = ""
    delete_capability: str = ""
    read_capability: str = ""
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class MessageProjectionRecord:
    message_id: str
    canonical_key: str
    content_hash: str
    primary_source_id: str
    source_family: str
    source_label: str = ""
    radio_id: int | None = None
    app_instance_id: str = ""
    message_type: str = ""
    display_type: str = ""
    status: str = "info"
    severity: str = "info"
    read_state: str = "new"
    from_call: str = ""
    to_call: str = ""
    group_name: str = ""
    scope: str = ""
    state_code: str = ""
    grid: str = ""
    lat: float | None = None
    lon: float | None = None
    event_ts: float = 0.0
    received_ts: float = 0.0
    event_utc: str = ""
    received_utc: str = ""
    subject: str = ""
    summary: str = ""
    body_preview: str = ""
    topics: Sequence[str] = field(default_factory=tuple)
    entities: Mapping[str, object] = field(default_factory=dict)
    actionable: bool = False
    operator_attention: bool = False
    confidence: float = 0.0
    recommended_action: str = ""
    intelligence_version: int = 0
    intelligence_utc: str = ""
    intelligence: Mapping[str, object] = field(default_factory=dict)
    pinned: bool = False
    archived: bool = False
    deleted: bool = False
    deleted_utc: str = ""
    retention_class: str = "normal"
    search_text: str = ""
    projection_version: int = PROJECTION_SCHEMA_VERSION
    projected_utc: str = ""


@dataclass(frozen=True)
class MessageArtifactRecord:
    artifact_id: str
    message_id: str
    artifact_type: str
    source_id: str = ""
    external_key: str = ""
    path: str = ""
    content_hash: str = ""
    q_id: str = ""
    block_id: str = ""
    transfer_id: str = ""
    block_count: int = 0
    missing_blocks_json: str = "[]"
    transfer_state: str = ""
    signature_state: str = ""
    verified_utc: str = ""
    metadata: Mapping[str, object] = field(default_factory=dict)


def _json(value: object, default: str) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except Exception:
        return default


def ensure_message_projection_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_sources (
            source_id TEXT PRIMARY KEY,
            source_family TEXT NOT NULL,
            source_label TEXT,
            radio_id INTEGER,
            app_instance_id TEXT,
            endpoint_or_path TEXT,
            capabilities_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            enabled INTEGER NOT NULL DEFAULT 1,
            last_seen_utc TEXT,
            last_ingested_utc TEXT,
            updated_utc TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_projection (
            message_id TEXT PRIMARY KEY,
            canonical_key TEXT NOT NULL UNIQUE,
            content_hash TEXT NOT NULL,
            primary_source_id TEXT NOT NULL,
            source_family TEXT NOT NULL,
            source_label TEXT,
            radio_id INTEGER,
            app_instance_id TEXT,
            message_type TEXT,
            display_type TEXT,
            status TEXT,
            severity TEXT,
            read_state TEXT,
            from_call TEXT,
            to_call TEXT,
            group_name TEXT,
            scope TEXT,
            state_code TEXT,
            grid TEXT,
            lat REAL,
            lon REAL,
            event_ts REAL,
            received_ts REAL,
            event_utc TEXT,
            received_utc TEXT,
            subject TEXT,
            summary TEXT,
            body_preview TEXT,
            topics_json TEXT NOT NULL DEFAULT '[]',
            entities_json TEXT NOT NULL DEFAULT '{}',
            actionable INTEGER NOT NULL DEFAULT 0,
            operator_attention INTEGER NOT NULL DEFAULT 0,
            confidence REAL NOT NULL DEFAULT 0,
            recommended_action TEXT,
            intelligence_version INTEGER NOT NULL DEFAULT 0,
            intelligence_utc TEXT,
            intelligence_json TEXT NOT NULL DEFAULT '{}',
            pinned INTEGER NOT NULL DEFAULT 0,
            archived INTEGER NOT NULL DEFAULT 0,
            deleted INTEGER NOT NULL DEFAULT 0,
            deleted_utc TEXT,
            retention_class TEXT NOT NULL DEFAULT 'normal',
            search_text TEXT,
            projection_version INTEGER NOT NULL DEFAULT 1,
            projected_utc TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_external_refs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            external_kind TEXT NOT NULL,
            external_key TEXT NOT NULL,
            external_path TEXT,
            external_mtime REAL,
            external_size INTEGER,
            external_hash TEXT,
            delete_capability TEXT,
            read_capability TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_utc TEXT NOT NULL,
            UNIQUE(source_id, external_kind, external_key)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_artifacts (
            artifact_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            source_id TEXT,
            external_key TEXT,
            path TEXT,
            content_hash TEXT,
            q_id TEXT,
            block_id TEXT,
            transfer_id TEXT,
            block_count INTEGER NOT NULL DEFAULT 0,
            missing_blocks_json TEXT NOT NULL DEFAULT '[]',
            transfer_state TEXT,
            signature_state TEXT,
            verified_utc TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_utc TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_delete_queue (
            delete_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL,
            requested_effect TEXT NOT NULL,
            requested_by TEXT,
            source_scope TEXT NOT NULL DEFAULT 'selected',
            state TEXT NOT NULL DEFAULT 'queued',
            requested_utc TEXT NOT NULL,
            completed_utc TEXT,
            result_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_delete_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            delete_id TEXT,
            message_id TEXT NOT NULL,
            source_id TEXT,
            external_kind TEXT,
            external_key TEXT,
            effect TEXT NOT NULL,
            state TEXT NOT NULL,
            detail TEXT,
            audit_utc TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_projection_checkpoint (
            source_id TEXT PRIMARY KEY,
            last_external_key TEXT,
            last_event_ts REAL,
            content_fingerprint TEXT,
            updated_utc TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_projection_default ON message_projection(deleted, archived, event_ts DESC, received_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_projection_source ON message_projection(source_family, event_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_projection_group ON message_projection(group_name, event_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_projection_status ON message_projection(status, severity, event_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_projection_attention ON message_projection(operator_attention, actionable, event_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_projection_calls ON message_projection(from_call, to_call, event_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_projection_geo ON message_projection(state_code, grid, event_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_projection_search ON message_projection(search_text)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_refs_message ON message_external_refs(message_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_artifacts_message ON message_artifacts(message_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_artifacts_flamp_qid ON message_artifacts(q_id, transfer_state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_delete_queue_state ON message_delete_queue(state, requested_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_delete_audit_message ON message_delete_audit(message_id, audit_utc)")


def upsert_message_source(conn: sqlite3.Connection, source: MessageSourceRecord, *, updated_utc: str | None = None) -> str:
    ensure_message_projection_schema(conn)
    stamp = updated_utc or utc_now_iso()
    conn.execute(
        """
        INSERT INTO message_sources (
            source_id, source_family, source_label, radio_id, app_instance_id,
            endpoint_or_path, capabilities_json, provenance_json, enabled,
            last_seen_utc, last_ingested_utc, updated_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            source_family=excluded.source_family,
            source_label=excluded.source_label,
            radio_id=excluded.radio_id,
            app_instance_id=excluded.app_instance_id,
            endpoint_or_path=excluded.endpoint_or_path,
            capabilities_json=excluded.capabilities_json,
            provenance_json=excluded.provenance_json,
            enabled=excluded.enabled,
            last_seen_utc=excluded.last_seen_utc,
            last_ingested_utc=excluded.last_ingested_utc,
            updated_utc=excluded.updated_utc
        """,
        (
            source.source_id,
            source.source_family,
            source.source_label,
            source.radio_id,
            source.app_instance_id,
            source.endpoint_or_path,
            _json(source.capabilities, "{}"),
            _json(source.provenance, "{}"),
            1 if source.enabled else 0,
            source.last_seen_utc,
            source.last_ingested_utc,
            stamp,
        ),
    )
    return source.source_id


def upsert_message_projection(conn: sqlite3.Connection, message: MessageProjectionRecord, *, projected_utc: str | None = None) -> str:
    ensure_message_projection_schema(conn)
    stamp = projected_utc or message.projected_utc or utc_now_iso()
    conn.execute(
        """
        INSERT INTO message_projection (
            message_id, canonical_key, content_hash, primary_source_id, source_family, source_label,
            radio_id, app_instance_id, message_type, display_type, status, severity, read_state,
            from_call, to_call, group_name, scope, state_code, grid, lat, lon, event_ts, received_ts,
            event_utc, received_utc, subject, summary, body_preview, topics_json, entities_json,
            actionable, operator_attention, confidence, recommended_action, intelligence_version,
            intelligence_utc, intelligence_json, pinned, archived, deleted, deleted_utc, retention_class,
            search_text, projection_version, projected_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(message_id) DO UPDATE SET
            canonical_key=excluded.canonical_key,
            content_hash=excluded.content_hash,
            primary_source_id=excluded.primary_source_id,
            source_family=excluded.source_family,
            source_label=excluded.source_label,
            radio_id=excluded.radio_id,
            app_instance_id=excluded.app_instance_id,
            message_type=excluded.message_type,
            display_type=excluded.display_type,
            status=excluded.status,
            severity=excluded.severity,
            read_state=excluded.read_state,
            from_call=excluded.from_call,
            to_call=excluded.to_call,
            group_name=excluded.group_name,
            scope=excluded.scope,
            state_code=excluded.state_code,
            grid=excluded.grid,
            lat=excluded.lat,
            lon=excluded.lon,
            event_ts=excluded.event_ts,
            received_ts=excluded.received_ts,
            event_utc=excluded.event_utc,
            received_utc=excluded.received_utc,
            subject=excluded.subject,
            summary=excluded.summary,
            body_preview=excluded.body_preview,
            topics_json=excluded.topics_json,
            entities_json=excluded.entities_json,
            actionable=excluded.actionable,
            operator_attention=excluded.operator_attention,
            confidence=excluded.confidence,
            recommended_action=excluded.recommended_action,
            intelligence_version=excluded.intelligence_version,
            intelligence_utc=excluded.intelligence_utc,
            intelligence_json=excluded.intelligence_json,
            pinned=excluded.pinned,
            archived=excluded.archived,
            deleted=excluded.deleted,
            deleted_utc=excluded.deleted_utc,
            retention_class=excluded.retention_class,
            search_text=excluded.search_text,
            projection_version=excluded.projection_version,
            projected_utc=excluded.projected_utc
        """,
        _message_values(message, stamp),
    )
    return message.message_id


def upsert_external_ref(conn: sqlite3.Connection, ref: ExternalMessageRef, *, updated_utc: str | None = None) -> str:
    ensure_message_projection_schema(conn)
    stamp = updated_utc or utc_now_iso()
    conn.execute(
        """
        INSERT INTO message_external_refs (
            message_id, source_id, external_kind, external_key, external_path,
            external_mtime, external_size, external_hash, delete_capability,
            read_capability, metadata_json, updated_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id, external_kind, external_key) DO UPDATE SET
            message_id=excluded.message_id,
            external_path=excluded.external_path,
            external_mtime=excluded.external_mtime,
            external_size=excluded.external_size,
            external_hash=excluded.external_hash,
            delete_capability=excluded.delete_capability,
            read_capability=excluded.read_capability,
            metadata_json=excluded.metadata_json,
            updated_utc=excluded.updated_utc
        """,
        (
            ref.message_id,
            ref.source_id,
            ref.external_kind,
            ref.external_key,
            ref.external_path,
            float(ref.external_mtime or 0.0),
            int(ref.external_size or 0),
            ref.external_hash,
            ref.delete_capability,
            ref.read_capability,
            _json(ref.metadata, "{}"),
            stamp,
        ),
    )
    return ref.message_id


def upsert_message_artifact(conn: sqlite3.Connection, artifact: MessageArtifactRecord, *, updated_utc: str | None = None) -> str:
    ensure_message_projection_schema(conn)
    stamp = updated_utc or utc_now_iso()
    conn.execute(
        """
        INSERT INTO message_artifacts (
            artifact_id, message_id, artifact_type, source_id, external_key, path,
            content_hash, q_id, block_id, transfer_id, block_count, missing_blocks_json,
            transfer_state, signature_state, verified_utc, metadata_json, updated_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(artifact_id) DO UPDATE SET
            message_id=excluded.message_id,
            artifact_type=excluded.artifact_type,
            source_id=excluded.source_id,
            external_key=excluded.external_key,
            path=excluded.path,
            content_hash=excluded.content_hash,
            q_id=excluded.q_id,
            block_id=excluded.block_id,
            transfer_id=excluded.transfer_id,
            block_count=excluded.block_count,
            missing_blocks_json=excluded.missing_blocks_json,
            transfer_state=excluded.transfer_state,
            signature_state=excluded.signature_state,
            verified_utc=excluded.verified_utc,
            metadata_json=excluded.metadata_json,
            updated_utc=excluded.updated_utc
        """,
        (
            artifact.artifact_id,
            artifact.message_id,
            artifact.artifact_type,
            artifact.source_id,
            artifact.external_key,
            artifact.path,
            artifact.content_hash,
            artifact.q_id,
            artifact.block_id,
            artifact.transfer_id,
            int(artifact.block_count or 0),
            artifact.missing_blocks_json,
            artifact.transfer_state,
            artifact.signature_state,
            artifact.verified_utc,
            _json(artifact.metadata, "{}"),
            stamp,
        ),
    )
    return artifact.artifact_id


def queue_message_delete(
    conn: sqlite3.Connection,
    *,
    message_id: str,
    requested_effect: str,
    requested_by: str = "",
    source_scope: str = "selected",
    requested_utc: str | None = None,
) -> str:
    ensure_message_projection_schema(conn)
    stamp = requested_utc or utc_now_iso()
    delete_id = stable_message_id("delete", message_id, requested_effect, source_scope, stamp)
    conn.execute(
        """
        INSERT INTO message_delete_queue (
            delete_id, message_id, requested_effect, requested_by, source_scope,
            state, requested_utc, result_json
        )
        VALUES (?, ?, ?, ?, ?, 'queued', ?, '{}')
        """,
        (delete_id, message_id, requested_effect, requested_by, source_scope, stamp),
    )
    conn.execute(
        """
        INSERT INTO message_delete_audit (
            delete_id, message_id, effect, state, detail, audit_utc
        )
        VALUES (?, ?, ?, 'queued', 'Delete request queued', ?)
        """,
        (delete_id, message_id, requested_effect, stamp),
    )
    conn.execute(
        "UPDATE message_projection SET deleted=1, deleted_utc=? WHERE message_id=?",
        (stamp, message_id),
    )
    return delete_id


def list_projected_messages(
    db_path: str | Path,
    *,
    source_family: str = "",
    group_name: str = "",
    status: str = "",
    severity: str = "",
    search_text: str = "",
    include_archived: bool = False,
    include_deleted: bool = False,
    limit: int = 500,
) -> list[sqlite3.Row]:
    clauses: list[str] = []
    params: list[Any] = []
    if not include_deleted:
        clauses.append("deleted=0")
    if not include_archived:
        clauses.append("archived=0")
    if source_family:
        clauses.append("source_family=?")
        params.append(source_family)
    if group_name:
        clauses.append("group_name=?")
        params.append(group_name.lstrip("@"))
    if status:
        clauses.append("status=?")
        params.append(status)
    if severity:
        clauses.append("severity=?")
        params.append(severity)
    if search_text:
        clauses.append("search_text LIKE ?")
        params.append(f"%{search_text.lower()}%")
    params.append(max(1, min(5000, int(limit or 500))))
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    conn = connect_sqlite(db_path, row_factory=sqlite3.Row)
    try:
        ensure_message_projection_schema(conn)
        return list(
            conn.execute(
                f"""
                SELECT *
                  FROM message_projection
                {where}
                 ORDER BY operator_attention DESC, actionable DESC, event_ts DESC, received_ts DESC
                 LIMIT ?
                """,
                tuple(params),
            )
        )
    finally:
        conn.close()


def _message_values(message: MessageProjectionRecord, projected_utc: str) -> tuple[object, ...]:
    return (
        str(message.message_id or ""),
        str(message.canonical_key or ""),
        str(message.content_hash or ""),
        str(message.primary_source_id or ""),
        str(message.source_family or ""),
        str(message.source_label or ""),
        message.radio_id,
        str(message.app_instance_id or ""),
        str(message.message_type or ""),
        str(message.display_type or ""),
        str(message.status or ""),
        str(message.severity or ""),
        str(message.read_state or ""),
        str(message.from_call or "").upper(),
        str(message.to_call or "").upper(),
        str(message.group_name or "").lstrip("@").upper(),
        str(message.scope or ""),
        str(message.state_code or "").upper(),
        str(message.grid or "").upper(),
        message.lat,
        message.lon,
        float(message.event_ts or 0.0),
        float(message.received_ts or 0.0),
        str(message.event_utc or ""),
        str(message.received_utc or ""),
        str(message.subject or ""),
        str(message.summary or ""),
        str(message.body_preview or ""),
        _json(list(message.topics), "[]"),
        _json(message.entities, "{}"),
        1 if message.actionable else 0,
        1 if message.operator_attention else 0,
        float(message.confidence or 0.0),
        str(message.recommended_action or ""),
        int(message.intelligence_version or 0),
        str(message.intelligence_utc or ""),
        _json(message.intelligence, "{}"),
        1 if message.pinned else 0,
        1 if message.archived else 0,
        1 if message.deleted else 0,
        str(message.deleted_utc or ""),
        str(message.retention_class or "normal"),
        str(message.search_text or "").lower(),
        int(message.projection_version or PROJECTION_SCHEMA_VERSION),
        projected_utc,
    )


def upsert_projected_message(
    db_path: str | Path,
    *,
    source: MessageSourceRecord,
    message: MessageProjectionRecord,
    refs: Sequence[ExternalMessageRef] = (),
    artifacts: Sequence[MessageArtifactRecord] = (),
) -> str:
    conn = connect_sqlite(db_path)
    try:
        ensure_message_projection_schema(conn)
        with conn:
            upsert_message_source(conn, source)
            message_id = upsert_message_projection(conn, message)
            for ref in refs:
                upsert_external_ref(conn, ref)
            for artifact in artifacts:
                upsert_message_artifact(conn, artifact)
        return message_id
    finally:
        conn.close()
