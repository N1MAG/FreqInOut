from __future__ import annotations

import hashlib
import datetime
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


@dataclass(frozen=True)
class MessageProjectionCheckpoint:
    source_id: str
    last_external_key: str = ""
    last_event_ts: float = 0.0
    content_fingerprint: str = ""
    updated_utc: str = ""


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
            pinned=CASE WHEN message_projection.pinned=1 THEN 1 ELSE excluded.pinned END,
            archived=CASE WHEN message_projection.archived=1 THEN 1 ELSE excluded.archived END,
            deleted=CASE WHEN message_projection.deleted=1 THEN 1 ELSE excluded.deleted END,
            deleted_utc=CASE
                WHEN message_projection.deleted=1 AND COALESCE(message_projection.deleted_utc, '') != '' THEN message_projection.deleted_utc
                ELSE excluded.deleted_utc
            END,
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


def load_projected_message_detail(db_path: str | Path, message_id: str) -> dict[str, Any]:
    clean_id = str(message_id or "").strip()
    if not clean_id:
        return {"message": None, "refs": [], "artifacts": []}
    conn = connect_sqlite(db_path, row_factory=sqlite3.Row)
    try:
        ensure_message_projection_schema(conn)
        message = conn.execute(
            "SELECT * FROM message_projection WHERE message_id=?",
            (clean_id,),
        ).fetchone()
        refs = conn.execute(
            """
            SELECT *
              FROM message_external_refs
             WHERE message_id=?
             ORDER BY source_id, external_kind, external_key
            """,
            (clean_id,),
        ).fetchall()
        artifacts = conn.execute(
            """
            SELECT *
              FROM message_artifacts
             WHERE message_id=?
             ORDER BY artifact_type, q_id, block_id, path
            """,
            (clean_id,),
        ).fetchall()
        return {"message": message, "refs": list(refs), "artifacts": list(artifacts)}
    finally:
        conn.close()


def load_projected_external_refs_for_messages(
    db_path: str | Path,
    message_ids: Sequence[str],
) -> dict[str, list[sqlite3.Row]]:
    clean_ids = [str(value or "").strip() for value in message_ids if str(value or "").strip()]
    if not clean_ids:
        return {}
    out: dict[str, list[sqlite3.Row]] = {message_id: [] for message_id in clean_ids}
    conn = connect_sqlite(db_path, row_factory=sqlite3.Row)
    try:
        ensure_message_projection_schema(conn)
        for start in range(0, len(clean_ids), 250):
            chunk = clean_ids[start : start + 250]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"""
                SELECT *
                  FROM message_external_refs
                 WHERE message_id IN ({placeholders})
                 ORDER BY source_id, external_kind, external_key
                """,
                tuple(chunk),
            ).fetchall()
            for row in rows:
                out.setdefault(str(row["message_id"] or ""), []).append(row)
        return out
    finally:
        conn.close()


def mark_projected_messages_read(db_path: str | Path, message_ids: Sequence[str]) -> int:
    clean_ids = [str(value or "").strip() for value in message_ids if str(value or "").strip()]
    if not clean_ids:
        return 0
    conn = connect_sqlite(db_path, row_factory=sqlite3.Row)
    try:
        ensure_message_projection_schema(conn)
        stamp = utc_now_iso()
        with conn:
            count = 0
            for message_id in clean_ids:
                cur = conn.execute(
                    """
                    UPDATE message_projection
                       SET read_state='read',
                           status=CASE WHEN status IN ('NEW', 'UNREAD', 'ALERT') THEN 'READ' ELSE status END,
                           projected_utc=?
                     WHERE message_id=? AND deleted=0
                    """,
                    (stamp, message_id),
                )
                _mark_source_refs_read(conn, message_id, stamp)
                count += int(cur.rowcount or 0)
            return count
    finally:
        conn.close()


def _mark_source_refs_read(conn: sqlite3.Connection, message_id: str, stamp: str) -> None:
    refs = list(
        conn.execute(
            """
            SELECT *
              FROM message_external_refs
             WHERE message_id=? AND COALESCE(read_capability, '') IN (
                'mark_read', 'js8_mark_read', 'spotter_mark_read', 'varac_mark_read'
             )
            """,
            (message_id,),
        )
    )
    read_ts = datetime_from_iso_ts(stamp)
    for ref in refs:
        kind = str(ref["external_kind"] or "").strip().lower()
        key = str(ref["external_key"] or "").strip()
        metadata = _parse_json_object(ref["metadata_json"])
        row_id = str(metadata.get("row_id") or "").strip()
        try:
            if kind == "js8_message" and _table_exists(conn, "js8_messages"):
                target = _int_text(row_id or key)
                if target > 0:
                    conn.execute(
                        "UPDATE js8_messages SET state='READ', read_ts=? WHERE id=? OR COALESCE(source_id, id)=?",
                        (read_ts, target, target),
                    )
            elif kind == "spotter_message" and _table_exists(conn, "spotter_traffic"):
                target = _int_text(row_id or key)
                if target > 0:
                    conn.execute("UPDATE spotter_traffic SET state='READ', read_ts=? WHERE id=?", (read_ts, target))
            elif kind == "varac_message" and _table_exists(conn, "varac_messages"):
                target = _int_text(row_id)
                source = str(metadata.get("source") or "").strip()
                source_key = str(metadata.get("source_key") or "").strip()
                if target > 0 and source and _table_has_column(conn, "varac_messages", "ingest_source_key"):
                    conn.execute(
                        "UPDATE varac_messages SET read_status=1 WHERE ingest_source_key=? AND source=? AND id=?",
                        (source_key, source, target),
                    )
                elif target > 0:
                    conn.execute("UPDATE varac_messages SET read_status=1 WHERE id=?", (target,))
        except Exception:
            continue


def datetime_from_iso_ts(value: str) -> float:
    try:
        return datetime.datetime.fromisoformat(str(value or "")).timestamp()
    except Exception:
        return 0.0


def get_message_projection_checkpoint(
    db_path: str | Path,
    source_id: str,
) -> MessageProjectionCheckpoint:
    clean_source = str(source_id or "").strip()
    if not clean_source:
        return MessageProjectionCheckpoint(source_id="")
    conn = connect_sqlite(db_path, row_factory=sqlite3.Row)
    try:
        ensure_message_projection_schema(conn)
        row = conn.execute(
            """
            SELECT source_id, last_external_key, last_event_ts, content_fingerprint, updated_utc
              FROM message_projection_checkpoint
             WHERE source_id=?
            """,
            (clean_source,),
        ).fetchone()
        if row is None:
            return MessageProjectionCheckpoint(source_id=clean_source)
        return MessageProjectionCheckpoint(
            source_id=str(row["source_id"] or ""),
            last_external_key=str(row["last_external_key"] or ""),
            last_event_ts=float(row["last_event_ts"] or 0.0),
            content_fingerprint=str(row["content_fingerprint"] or ""),
            updated_utc=str(row["updated_utc"] or ""),
        )
    finally:
        conn.close()


def set_message_projection_checkpoint(
    conn: sqlite3.Connection,
    checkpoint: MessageProjectionCheckpoint,
    *,
    updated_utc: str | None = None,
) -> str:
    ensure_message_projection_schema(conn)
    clean_source = str(checkpoint.source_id or "").strip()
    if not clean_source:
        return ""
    stamp = updated_utc or checkpoint.updated_utc or utc_now_iso()
    conn.execute(
        """
        INSERT INTO message_projection_checkpoint (
            source_id, last_external_key, last_event_ts, content_fingerprint, updated_utc
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            last_external_key=excluded.last_external_key,
            last_event_ts=excluded.last_event_ts,
            content_fingerprint=excluded.content_fingerprint,
            updated_utc=excluded.updated_utc
        """,
        (
            clean_source,
            str(checkpoint.last_external_key or ""),
            float(checkpoint.last_event_ts or 0.0),
            str(checkpoint.content_fingerprint or ""),
            stamp,
        ),
    )
    return clean_source


def process_message_delete_queue(db_path: str | Path, *, limit: int = 50) -> dict[str, int]:
    conn = connect_sqlite(db_path, row_factory=sqlite3.Row)
    try:
        ensure_message_projection_schema(conn)
        rows = list(
            conn.execute(
                """
                SELECT *
                  FROM message_delete_queue
                 WHERE state='queued'
                 ORDER BY requested_utc
                 LIMIT ?
                """,
                (max(1, min(500, int(limit or 50))),),
            )
        )
        counts = {"completed": 0, "failed": 0, "skipped": 0}
        for row in rows:
            result = _process_delete_queue_row(conn, row)
            counts[result] = counts.get(result, 0) + 1
        return counts
    finally:
        conn.close()


def _process_delete_queue_row(conn: sqlite3.Connection, row: sqlite3.Row) -> str:
    delete_id = str(row["delete_id"] or "")
    message_id = str(row["message_id"] or "")
    effect = str(row["requested_effect"] or "").strip().lower()
    stamp = utc_now_iso()
    if not delete_id or not message_id:
        return "skipped"
    try:
        if effect == "hide_fio":
            _complete_delete_queue_row(conn, delete_id, message_id, effect, "completed", "Projection hidden")
            return "completed"
        if effect == "source_delete":
            return _process_source_external_delete(conn, delete_id, message_id, effect)
        if effect == "audit_only":
            with conn:
                conn.execute(
                    """
                    UPDATE message_projection
                       SET body_preview='', search_text=LOWER(TRIM(COALESCE(subject, '') || ' ' || COALESCE(summary, ''))),
                           retention_class='audit_only', projected_utc=?
                     WHERE message_id=?
                    """,
                    (stamp, message_id),
                )
            _complete_delete_queue_row(conn, delete_id, message_id, effect, "completed", "Raw preview minimized")
            return "completed"
        if effect in {"delete_external", "delete_all_external_refs"}:
            return _process_file_external_delete(conn, delete_id, message_id, effect)
        _complete_delete_queue_row(conn, delete_id, message_id, effect, "failed", f"Unsupported delete effect: {effect}")
        return "failed"
    except Exception as exc:
        _complete_delete_queue_row(conn, delete_id, message_id, effect, "failed", str(exc))
        return "failed"


def _process_source_external_delete(
    conn: sqlite3.Connection,
    delete_id: str,
    message_id: str,
    effect: str,
) -> str:
    refs = list(
        conn.execute(
            """
            SELECT *
              FROM message_external_refs
             WHERE message_id=? AND COALESCE(delete_capability, '') IN (
                'delete_source', 'js8_delete', 'spotter_delete', 'varac_soft_delete',
                'sitrep_delete', 'commstat_delete'
             )
             ORDER BY source_id, external_kind, external_key
            """,
            (message_id,),
        )
    )
    if not refs:
        _complete_delete_queue_row(conn, delete_id, message_id, effect, "completed", "Projection hidden; no source-delete refs")
        return "completed"
    deleted = 0
    skipped = 0
    errors: list[str] = []
    with conn:
        for ref in refs:
            try:
                if _delete_source_ref(conn, ref):
                    deleted += 1
                else:
                    skipped += 1
            except Exception as exc:
                errors.append(f"{ref['external_kind']}:{ref['external_key']}: {exc}")
    detail = f"Deleted {deleted} source ref(s); skipped {skipped}"
    if errors:
        detail += "; errors: " + "; ".join(errors[:3])
    state = "failed" if errors and deleted == 0 else "completed"
    _complete_delete_queue_row(conn, delete_id, message_id, effect, state, detail)
    return "failed" if state == "failed" else "completed"


def _delete_source_ref(conn: sqlite3.Connection, ref: sqlite3.Row) -> bool:
    kind = str(ref["external_kind"] or "").strip().lower()
    key = str(ref["external_key"] or "").strip()
    metadata = _parse_json_object(ref["metadata_json"])
    row_id = str(metadata.get("row_id") or "").strip()
    if kind == "js8_message":
        if not _table_exists(conn, "js8_messages"):
            return False
        target = _int_text(row_id or key)
        if target <= 0:
            return False
        cur = conn.execute("DELETE FROM js8_messages WHERE id=? OR COALESCE(source_id, id)=?", (target, target))
        try:
            conn.execute("DELETE FROM js8_inbox_state WHERE id=? OR COALESCE(source_id, id)=?", (target, target))
        except Exception:
            pass
        return int(cur.rowcount or 0) > 0
    if kind == "spotter_message":
        if not _table_exists(conn, "spotter_traffic"):
            return False
        target = _int_text(row_id or key)
        if target <= 0:
            return False
        cur = conn.execute("DELETE FROM spotter_traffic WHERE id=?", (target,))
        return int(cur.rowcount or 0) > 0
    if kind == "varac_message":
        if not _table_exists(conn, "varac_messages"):
            return False
        target = _int_text(row_id)
        source = str(metadata.get("source") or "").strip()
        source_key = str(metadata.get("source_key") or "").strip()
        if target > 0 and source and _table_has_column(conn, "varac_messages", "ingest_source_key"):
            cur = conn.execute(
                "UPDATE varac_messages SET is_deleted=1 WHERE ingest_source_key=? AND source=? AND id=?",
                (source_key, source, target),
            )
            return int(cur.rowcount or 0) > 0
        if target > 0:
            cur = conn.execute("UPDATE varac_messages SET is_deleted=1 WHERE id=?", (target,))
            return int(cur.rowcount or 0) > 0
        if key:
            cur = conn.execute(
                "UPDATE varac_messages SET is_deleted=1 WHERE guid=? OR vmail_guid=?",
                (key, key),
            )
            return int(cur.rowcount or 0) > 0
        return False
    if kind == "sitrep_event":
        if not _table_exists(conn, "sitrep_events"):
            return False
        target = _int_text(row_id)
        if target > 0:
            cur = conn.execute("DELETE FROM sitrep_events WHERE id=?", (target,))
            return int(cur.rowcount or 0) > 0
        if key:
            cur = conn.execute("DELETE FROM sitrep_events WHERE report_key=?", (key,))
            return int(cur.rowcount or 0) > 0
        return False
    if kind == "commstat_artifact":
        if not _table_exists(conn, "commstat_artifacts"):
            return False
        cur = conn.execute("DELETE FROM commstat_artifacts WHERE artifact_key=?", (key,))
        return int(cur.rowcount or 0) > 0
    return False


def _process_file_external_delete(
    conn: sqlite3.Connection,
    delete_id: str,
    message_id: str,
    effect: str,
) -> str:
    refs = list(
        conn.execute(
            """
            SELECT *
              FROM message_external_refs
             WHERE message_id=? AND delete_capability='file_delete'
             ORDER BY external_path
            """,
            (message_id,),
        )
    )
    if not refs:
        _complete_delete_queue_row(conn, delete_id, message_id, effect, "failed", "No file-delete capable refs")
        return "failed"
    deleted = 0
    missing = 0
    errors: list[str] = []
    for ref in refs:
        path_text = str(ref["external_path"] or "").strip()
        if not path_text:
            missing += 1
            continue
        path = Path(path_text)
        try:
            if path.exists() and path.is_file():
                path.unlink()
                deleted += 1
            else:
                missing += 1
        except Exception as exc:
            errors.append(f"{path}: {exc}")
    detail = f"Deleted {deleted} file ref(s); missing {missing}"
    if errors:
        detail += "; errors: " + "; ".join(errors[:3])
    state = "failed" if errors and deleted == 0 else "completed"
    _complete_delete_queue_row(conn, delete_id, message_id, effect, state, detail)
    return "failed" if state == "failed" else "completed"


def _parse_json_object(value: object) -> dict[str, object]:
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _int_text(value: object) -> int:
    try:
        return int(float(str(value or "").strip()))
    except Exception:
        return 0


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        return any(str(row[1] or "") == column for row in conn.execute(f"PRAGMA table_info({table})").fetchall())
    except Exception:
        return False


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
            (str(table or "").strip(),),
        ).fetchone()
    except Exception:
        return False
    return bool(row)


def _complete_delete_queue_row(
    conn: sqlite3.Connection,
    delete_id: str,
    message_id: str,
    effect: str,
    state: str,
    detail: str,
) -> None:
    stamp = utc_now_iso()
    payload = {"detail": str(detail or ""), "completed_utc": stamp}
    with conn:
        conn.execute(
            """
            UPDATE message_delete_queue
               SET state=?, completed_utc=?, result_json=?
             WHERE delete_id=?
            """,
            (state, stamp, _json(payload, "{}"), delete_id),
        )
        conn.execute(
            """
            INSERT INTO message_delete_audit (
                delete_id, message_id, effect, state, detail, audit_utc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (delete_id, message_id, effect, state, detail, stamp),
        )


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
