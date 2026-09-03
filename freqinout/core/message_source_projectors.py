from __future__ import annotations

import datetime
import json
import re
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from freqinout.core.group_utils import normalize_group_name
from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.message_file_metadata import cached_message_file_row_summary, ensure_message_file_metadata_table
from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.message_projection_store import (
    ExternalMessageRef,
    MessageArtifactRecord,
    MessageProjectionCheckpoint,
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
    ensure_message_projection_schema,
    set_message_projection_checkpoint,
    stable_message_id,
    upsert_external_ref,
    upsert_message_artifact,
    upsert_message_projection,
    upsert_message_source,
)
from freqinout.core.sqlite_utils import connect_sqlite, table_exists

PROJECTOR_VERSION = 2
DEFAULT_SOURCE_NATIVE_LIMIT = 5000
_PROJECTION_WRITE_LOCK = threading.Lock()


def _file_metadata_key(rec: FileRecord) -> tuple[str, str, float, int]:
    return (
        _text(rec.origin).lower(),
        str(rec.path),
        float(rec.mtime or 0.0),
        int(rec.size or 0),
    )


def _load_file_projection_metadata(
    conn: sqlite3.Connection,
    records: Sequence[FileRecord],
) -> dict[tuple[str, str, float, int], dict[str, object]]:
    if not records:
        return {}
    try:
        ensure_message_file_metadata_table(conn)
    except Exception:
        return {}
    keys = {_file_metadata_key(rec) for rec in records}
    if not keys:
        return {}
    out: dict[tuple[str, str, float, int], dict[str, object]] = {}
    try:
        rows = conn.execute(
            """
            SELECT origin, path, mtime, size, source_id, source_label, source_family,
                   msg_type, display_type, status, from_call, to_call, title,
                   rcv_display, report_ts, age_ts_source, topics_json, actionable,
                   search_text
              FROM message_file_metadata
             WHERE origin IN ('flmsg', 'flamp', 'bbs', 'bbs_archive')
            """
        ).fetchall()
    except Exception:
        return {}
    for row in rows:
        try:
            key = (
                _text(row["origin"]).lower(),
                _text(row["path"]),
                float(row["mtime"] or 0.0),
                int(row["size"] or 0),
            )
        except Exception:
            continue
        if key not in keys:
            continue
        out[key] = {str(name): row[name] for name in row.keys()}
    return out


def project_native_message_sources(
    db_path: str | Path,
    *,
    sources: Sequence[str] = ("js8", "spotter", "varac", "sitrep", "commstat"),
    limit: int = DEFAULT_SOURCE_NATIVE_LIMIT,
    force: bool = False,
) -> dict[str, int]:
    """Project local source tables directly into the normalized message projection."""
    clean_sources = tuple(str(source or "").strip().lower() for source in sources if str(source or "").strip())
    if not clean_sources:
        return {}
    bounded_limit = max(1, min(int(limit or DEFAULT_SOURCE_NATIVE_LIMIT), 50000))
    with _PROJECTION_WRITE_LOCK:
        conn = connect_sqlite(db_path, timeout=15.0, row_factory=sqlite3.Row, busy_timeout_ms=15000)
        try:
            ensure_message_projection_schema(conn)
            out: dict[str, int] = {}
            projectors: dict[str, Callable[[sqlite3.Connection, int, bool], int]] = {
                "js8": _project_js8_messages,
                "spotter": _project_spotter_traffic,
                "varac": _project_varac_messages,
                "sitrep": _project_sitrep_events,
                "commstat": _project_commstat_artifacts,
            }
            for source in clean_sources:
                projector = projectors.get(source)
                if projector is None:
                    continue
                out[source] = projector(conn, bounded_limit, bool(force))
            return out
        finally:
            conn.close()


def project_native_file_records(
    db_path: str | Path,
    records: Mapping[str, Sequence[FileRecord]],
    *,
    force: bool = False,
) -> int:
    """Project file-scanner records directly into the normalized message projection."""
    flattened: list[FileRecord] = []
    for origin, values in (records or {}).items():
        origin_norm = _text(origin).lower()
        for rec in values or ():
            if not isinstance(rec, FileRecord):
                continue
            flattened.append(
                FileRecord(
                    path=Path(rec.path),
                    origin=origin_norm or _text(rec.origin).lower() or "file",
                    size=int(rec.size or 0),
                    mtime=float(rec.mtime or 0.0),
                    source_id=_text(getattr(rec, "source_id", "")),
                    source_label=_text(getattr(rec, "source_label", "")),
                )
            )
    fingerprint = content_hash(
        PROJECTOR_VERSION,
        "file_records",
        len(flattened),
        "\n".join(
            sorted(
                f"{rec.origin}|{rec.path}|{float(rec.mtime or 0.0):.6f}|{int(rec.size or 0)}|{rec.source_id}"
                for rec in flattened
            )
        ),
    )
    with _PROJECTION_WRITE_LOCK:
        conn = connect_sqlite(db_path, timeout=15.0, row_factory=sqlite3.Row, busy_timeout_ms=15000)
        try:
            ensure_message_projection_schema(conn)
            file_metadata = _load_file_projection_metadata(conn, flattened)
            fingerprint = content_hash(
                fingerprint,
                "metadata",
                "\n".join(
                    sorted(
                        "|".join(
                            (
                                key[0],
                                key[1],
                                f"{key[2]:.6f}",
                                str(key[3]),
                                _text(meta.get("title", "")),
                                _text(meta.get("report_ts", "")),
                                _text(meta.get("age_ts_source", "")),
                            )
                        )
                        for key, meta in file_metadata.items()
                    )
                ),
            )
            checkpoint_id = "native:file_records"
            if _checkpoint_matches(conn, checkpoint_id, fingerprint, force=force):
                return 0
            projected = 0
            with conn:
                for rec in sorted(flattened, key=lambda item: float(item.mtime or 0.0), reverse=True):
                    origin = _text(rec.origin).lower() or "file"
                    meta = cached_message_file_row_summary(
                        rec,
                        file_metadata.get(_file_metadata_key(rec)),
                        fallback_origin=origin,
                        fallback_title=rec.path.name,
                        title_limit=240,
                    )
                    event_ts = float(getattr(meta, "rcv_ts", 0.0) or rec.mtime or 0.0)
                    title = _text(getattr(meta, "title", "")) or rec.path.name
                    message_type = _text(getattr(meta, "msg_type", "")) or _file_message_type(origin, rec.path)
                    display_type = _text(getattr(meta, "display_type", "")) or _file_source_base_label(origin)
                    from_call = _upper(getattr(meta, "from_call", ""))
                    to_call = _upper(getattr(meta, "to_call", ""))
                    search_text = _text(getattr(meta, "search_text", "")) or _search_text(origin, title, rec.path)
                    topics = tuple(getattr(meta, "topics", ()) or ()) or _topics(title, origin)
                    source_id = _text(rec.source_id) or f"{origin}:{rec.path.parent}"
                    source_label = _text(rec.source_label) or _source_label(_file_source_base_label(origin), "")
                    external_kind = f"{origin}_file"
                    external_key = f"{rec.path}:{float(rec.mtime or 0.0):.6f}:{int(rec.size or 0)}"
                    message_id = stable_message_id(source_id, external_kind, external_key)
                    body = title
                    source = MessageSourceRecord(
                        source_id=source_id,
                        source_family=origin,
                        source_label=source_label,
                        endpoint_or_path=str(rec.path.parent),
                        capabilities={"read": True, "delete": True, "native_open": True},
                        provenance={"source": "file_scan", "origin": origin},
                        last_seen_utc=_utc_from_ts(event_ts),
                        last_ingested_utc=_utc_now(),
                    )
                    projection = MessageProjectionRecord(
                        message_id=message_id,
                        canonical_key=f"{source_id}:{external_kind}:{external_key}",
                        content_hash=content_hash(PROJECTOR_VERSION, "file", external_key),
                        primary_source_id=source_id,
                        source_family=origin,
                        source_label=source.source_label,
                        message_type=message_type,
                        display_type=display_type,
                        status="INFO",
                        severity="info",
                        read_state="info",
                        from_call=from_call,
                        to_call=to_call,
                        group_name=_group(to_call),
                        event_ts=event_ts,
                        received_ts=event_ts,
                        event_utc=_utc_from_ts(event_ts),
                        received_utc=_utc_from_ts(event_ts),
                        subject=title,
                        summary=title,
                        body_preview=body,
                        topics=topics,
                        entities={
                            "origin": origin,
                            "path": str(rec.path),
                            "extension": rec.path.suffix.lower(),
                            "q_id": _q_id_from_path(rec.path),
                            "age_ts_source": _text(getattr(meta, "age_ts_source", "")) or "received",
                        },
                        retention_class="artifact",
                        search_text=search_text,
                        projection_version=PROJECTOR_VERSION,
                    )
                    artifact_type = {"flamp": "flamp_transfer", "flmsg": "form_file", "bbs": "bbs_file"}.get(
                        origin,
                        f"{origin}_file",
                    )
                    q_id = _q_id_from_path(rec.path)
                    _upsert_bundle(
                        conn,
                        source,
                        projection,
                        ExternalMessageRef(
                            message_id=message_id,
                            source_id=source_id,
                            external_kind=external_kind,
                            external_key=external_key,
                            external_path=str(rec.path),
                            external_mtime=float(rec.mtime or 0.0),
                            external_size=int(rec.size or 0),
                            delete_capability="file_delete",
                            read_capability="fio_read_state",
                            metadata={"origin": origin, "source": "file_scan"},
                        ),
                        artifacts=(
                            MessageArtifactRecord(
                                artifact_id=stable_message_id(message_id, artifact_type, rec.path, rec.mtime, rec.size),
                                message_id=message_id,
                                artifact_type=artifact_type,
                                source_id=source_id,
                                external_key=external_key,
                                path=str(rec.path),
                                content_hash=content_hash(rec.path, rec.mtime, rec.size),
                                q_id=q_id,
                                block_id=_block_id_from_path(rec.path),
                                transfer_id=q_id,
                                transfer_state="seen" if q_id else "",
                                metadata={"mtime": float(rec.mtime or 0.0), "size": int(rec.size or 0)},
                            ),
                        ),
                    )
                    projected += 1
                _set_checkpoint(conn, checkpoint_id, fingerprint, flattened)
            return projected
        finally:
            conn.close()


def _project_js8_messages(conn: sqlite3.Connection, limit: int, force: bool) -> int:
    if not table_exists(conn, "js8_messages"):
        return 0
    checkpoint_id = "native:js8_messages"
    fingerprint = _table_fingerprint(
        conn,
        "js8_messages",
        "COUNT(*)",
        "MAX(COALESCE(id, 0))",
        "MAX(COALESCE(source_id, 0))",
        "MAX(COALESCE(utc_ts, 0))",
        "MAX(COALESCE(read_ts, 0))",
    )
    if _checkpoint_matches(conn, checkpoint_id, fingerprint, force=force):
        return 0
    rows = conn.execute(
        """
        SELECT id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text,
               state, read_ts, flag_state, source_key, source_id, source_radio_id,
               js8_instance_id, source_path
          FROM js8_messages
         ORDER BY COALESCE(utc_ts, 0) DESC, COALESCE(source_id, id) DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    projected = 0
    with conn:
        for row in rows:
            source_key = _text(row["source_key"]) or _text(row["js8_instance_id"]) or "legacy"
            source_id = f"js8:{source_key}"
            external_key = _text(row["source_id"]) or _text(row["id"])
            message_id = stable_message_id(source_id, "js8_message", external_key)
            status = _upper(row["state"]) or "UNREAD"
            raw_body = _text(row["raw_text"])
            body = _text(row["decoded_text"]) or raw_body
            analysis_body = "\n".join(part for part in (raw_body, body) if part)
            event_ts = _float(row["utc_ts"])
            form_name = _text(row["msg_type"])
            if not form_name.upper().startswith("F!"):
                form_name = ""
            intelligence = analyze_spotter_text(
                analysis_body,
                form_name=form_name,
                from_call=row["from_call"],
                to_call=row["to_call"],
                source_type="js8",
            )
            source = MessageSourceRecord(
                source_id=source_id,
                source_family="js8",
                source_label=_source_label("JS8Call", source_key),
                radio_id=_optional_int(row["source_radio_id"]),
                app_instance_id=_text(row["js8_instance_id"]),
                endpoint_or_path=_text(row["source_path"]),
                capabilities={"read": True, "delete": True, "native_open": True},
                provenance={"source_table": "js8_messages", "source_key": source_key},
                last_seen_utc=_utc_from_ts(event_ts),
                last_ingested_utc=_utc_now(),
            )
            projection = MessageProjectionRecord(
                message_id=message_id,
                canonical_key=f"{source_id}:js8_message:{external_key}",
                content_hash=content_hash(PROJECTOR_VERSION, "js8", external_key, status, body),
                primary_source_id=source_id,
                source_family="js8",
                source_label=source.source_label,
                radio_id=source.radio_id,
                app_instance_id=source.app_instance_id,
                message_type=_text(row["msg_type"]) or "MSG",
                display_type="JS8",
                status=status,
                severity="info",
                read_state=_read_state(status),
                from_call=_upper(row["from_call"]),
                to_call=_upper(row["to_call"]),
                group_name=_group(row["to_call"]),
                state_code=_upper(intelligence.state),
                grid=_upper(intelligence.grid),
                event_ts=event_ts,
                received_ts=event_ts,
                event_utc=_text(row["utc_str"]) or _utc_from_ts(event_ts),
                received_utc=_text(row["utc_str"]) or _utc_from_ts(event_ts),
                subject=intelligence.subject or _subject(body),
                summary=(intelligence.summary or body)[:240],
                body_preview=body[:1200],
                topics=tuple(intelligence.topics) or _topics(body),
                entities={
                    "from_call": _upper(row["from_call"]),
                    "to_call": _upper(row["to_call"]),
                    "state": _upper(intelligence.state),
                    "grid": _upper(intelligence.grid),
                },
                retention_class="normal",
                search_text=_search_text(row["from_call"], row["to_call"], row["msg_type"], body),
                projection_version=PROJECTOR_VERSION,
            )
            _upsert_bundle(
                conn,
                source,
                projection,
                ExternalMessageRef(
                    message_id=message_id,
                    source_id=source_id,
                    external_kind="js8_message",
                    external_key=external_key,
                    external_path=_text(row["source_path"]),
                    delete_capability="delete_source",
                    read_capability="mark_read",
                    metadata={"source_table": "js8_messages", "row_id": _text(row["id"])},
                ),
            )
            projected += 1
        _set_checkpoint(conn, checkpoint_id, fingerprint, rows)
    return projected


def _project_spotter_traffic(conn: sqlite3.Connection, limit: int, force: bool) -> int:
    if not table_exists(conn, "spotter_traffic"):
        return 0
    checkpoint_id = "native:spotter_traffic"
    fingerprint = _table_fingerprint(
        conn,
        "spotter_traffic",
        "COUNT(*)",
        "MAX(COALESCE(id, 0))",
        "MAX(COALESCE(utc_ts, 0))",
        "MAX(COALESCE(read_ts, 0))",
    )
    if _checkpoint_matches(conn, checkpoint_id, fingerprint, force=force):
        return 0
    rows = conn.execute(
        """
        SELECT id, utc_str, utc_ts, from_call, to_call, form_id, spotter_token,
               raw_text, decoded_text, state, read_ts, flag_state, relay_via,
               source_radio_id, js8_instance_id
          FROM spotter_traffic
         ORDER BY COALESCE(utc_ts, 0) DESC, id DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    projected = 0
    with conn:
        for row in rows:
            source_key = _text(row["js8_instance_id"]) or _text(row["source_radio_id"]) or "legacy"
            source_id = f"spotter:{source_key}"
            external_key = _text(row["id"])
            message_id = stable_message_id(source_id, "spotter_message", external_key)
            status = _upper(row["state"]) or "UNREAD"
            raw_body = _text(row["raw_text"])
            body = _text(row["decoded_text"]) or raw_body
            analysis_body = "\n".join(part for part in (raw_body, body) if part)
            event_ts = _float(row["utc_ts"])
            msg_type = _text(row["form_id"])
            if msg_type and not msg_type.startswith("F!"):
                msg_type = f"F!{msg_type}"
            intelligence = analyze_spotter_text(
                analysis_body,
                form_name=msg_type,
                from_call=row["from_call"],
                to_call=row["to_call"],
            )
            source = MessageSourceRecord(
                source_id=source_id,
                source_family="spotter",
                source_label=_source_label("FIOSpotter", source_key),
                radio_id=_optional_int(row["source_radio_id"]),
                app_instance_id=_text(row["js8_instance_id"]),
                capabilities={"read": True, "delete": True, "native_open": True},
                provenance={"source_table": "spotter_traffic", "source_key": source_key},
                last_seen_utc=_utc_from_ts(event_ts),
                last_ingested_utc=_utc_now(),
            )
            projection = MessageProjectionRecord(
                message_id=message_id,
                canonical_key=f"{source_id}:spotter_message:{external_key}",
                content_hash=content_hash(PROJECTOR_VERSION, "spotter", external_key, status, body),
                primary_source_id=source_id,
                source_family="spotter",
                source_label=source.source_label,
                radio_id=source.radio_id,
                app_instance_id=source.app_instance_id,
                message_type=msg_type or "F!",
                display_type="Spotter",
                status=status,
                severity="info",
                read_state=_read_state(status),
                from_call=_upper(row["from_call"]),
                to_call=_upper(row["to_call"]),
                group_name=_group(row["to_call"]),
                state_code=_upper(intelligence.state),
                grid=_upper(intelligence.grid),
                event_ts=event_ts,
                received_ts=event_ts,
                event_utc=_text(row["utc_str"]) or _utc_from_ts(event_ts),
                received_utc=_text(row["utc_str"]) or _utc_from_ts(event_ts),
                subject=intelligence.subject or _subject(body),
                summary=(intelligence.summary or body)[:240],
                body_preview=body[:1200],
                topics=tuple(intelligence.topics) or _topics(body),
                entities={
                    "relay_via": _upper(row["relay_via"]),
                    "spotter_token": _text(row["spotter_token"]),
                    "state": _upper(intelligence.state),
                    "grid": _upper(intelligence.grid),
                },
                retention_class="normal",
                search_text=_search_text(row["from_call"], row["to_call"], msg_type, body),
                projection_version=PROJECTOR_VERSION,
            )
            _upsert_bundle(
                conn,
                source,
                projection,
                ExternalMessageRef(
                    message_id=message_id,
                    source_id=source_id,
                    external_kind="spotter_message",
                    external_key=external_key,
                    delete_capability="delete_source",
                    read_capability="mark_read",
                    metadata={"source_table": "spotter_traffic", "row_id": external_key},
                ),
            )
            projected += 1
        _set_checkpoint(conn, checkpoint_id, fingerprint, rows)
    return projected


def _project_varac_messages(conn: sqlite3.Connection, limit: int, force: bool) -> int:
    if not table_exists(conn, "varac_messages"):
        return 0
    checkpoint_id = "native:varac_messages"
    fingerprint = _table_fingerprint(
        conn,
        "varac_messages",
        "COUNT(*)",
        "MAX(COALESCE(id, 0))",
        "MAX(COALESCE(ts, 0))",
        "SUM(COALESCE(is_deleted, 0))",
        "SUM(COALESCE(read_status, 0))",
    )
    if _checkpoint_matches(conn, checkpoint_id, fingerprint, force=force):
        return 0
    rows = conn.execute(
        """
        SELECT ingest_source_key, id, guid, source, msg_type, from_call, to_call,
               subject, body, ts, band, freq_hz, snr, read_status, folder,
               file_path, vmail_guid, is_deleted, folder_label, urgent,
               has_attachment, via_callsign
          FROM varac_messages
         WHERE COALESCE(is_deleted, 0) = 0
         ORDER BY COALESCE(ts, 0) DESC, id DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    projected = 0
    with conn:
        for row in rows:
            if _upper(row["msg_type"]) == "QSO":
                continue
            source_key = _text(row["ingest_source_key"]) or "legacy"
            source_name = _text(row["source"]) or "varac"
            source_id = f"varac:{source_key}:{source_name}"
            external_key = _text(row["guid"]) or _text(row["vmail_guid"]) or _text(row["id"])
            message_id = stable_message_id(source_id, "varac_message", external_key)
            status = "READ" if _int(row["read_status"]) else ("ALERT" if _int(row["urgent"]) else "UNREAD")
            body = _text(row["body"])
            subject = _text(row["subject"]) or _subject(body)
            event_ts = _float(row["ts"])
            source = MessageSourceRecord(
                source_id=source_id,
                source_family="varac",
                source_label=_source_label("VarAC", source_key),
                endpoint_or_path=_text(row["file_path"]),
                capabilities={"read": True, "delete": True, "native_open": True},
                provenance={"source_table": "varac_messages", "source_key": source_key, "source": source_name},
                last_seen_utc=_utc_from_ts(event_ts),
                last_ingested_utc=_utc_now(),
            )
            projection = MessageProjectionRecord(
                message_id=message_id,
                canonical_key=f"{source_id}:varac_message:{external_key}",
                content_hash=content_hash(PROJECTOR_VERSION, "varac", external_key, status, subject, body),
                primary_source_id=source_id,
                source_family="varac",
                source_label=source.source_label,
                message_type=_text(row["msg_type"]) or "VarAC",
                display_type="VarAC",
                status=status,
                severity="warning" if status == "ALERT" else "info",
                read_state=_read_state(status),
                from_call=_upper(row["from_call"]),
                to_call=_upper(row["to_call"]),
                group_name=_group(row["to_call"]),
                event_ts=event_ts,
                received_ts=event_ts,
                event_utc=_utc_from_ts(event_ts),
                received_utc=_utc_from_ts(event_ts),
                subject=subject,
                summary=(subject or body)[:240],
                body_preview=body[:1200],
                topics=_topics(subject, body, row["folder"], row["band"]),
                entities={
                    "folder": _text(row["folder"]),
                    "folder_label": _text(row["folder_label"]),
                    "band": _text(row["band"]),
                    "freq_hz": _float(row["freq_hz"]),
                    "snr": _float(row["snr"]),
                    "via_callsign": _upper(row["via_callsign"]),
                    "has_attachment": bool(_int(row["has_attachment"])),
                },
                actionable=status == "ALERT",
                operator_attention=status == "ALERT",
                retention_class="normal",
                search_text=_search_text(row["from_call"], row["to_call"], row["msg_type"], subject, body),
                projection_version=PROJECTOR_VERSION,
            )
            artifacts = []
            file_path = _text(row["file_path"])
            if file_path:
                artifacts.append(
                    MessageArtifactRecord(
                        artifact_id=stable_message_id(message_id, "varac_file", file_path),
                        message_id=message_id,
                        artifact_type="varac_file",
                        source_id=source_id,
                        external_key=external_key,
                        path=file_path,
                        content_hash=content_hash(file_path),
                    )
                )
            _upsert_bundle(
                conn,
                source,
                projection,
                ExternalMessageRef(
                    message_id=message_id,
                    source_id=source_id,
                    external_kind="varac_message",
                    external_key=external_key,
                    external_path=file_path,
                    delete_capability="delete_source",
                    read_capability="mark_read",
                    metadata={"source_table": "varac_messages", "source": source_name, "row_id": _text(row["id"])},
                ),
                artifacts=artifacts,
            )
            projected += 1
        _set_checkpoint(conn, checkpoint_id, fingerprint, rows)
    return projected


def _project_sitrep_events(conn: sqlite3.Connection, limit: int, force: bool) -> int:
    if not table_exists(conn, "sitrep_events"):
        return 0
    checkpoint_id = "native:sitrep_events"
    fingerprint = _table_fingerprint(
        conn,
        "sitrep_events",
        "COUNT(*)",
        "MAX(COALESCE(id, 0))",
        "MAX(COALESCE(event_ts, 0))",
        "MAX(COALESCE(updated_ts, 0))",
    )
    if _checkpoint_matches(conn, checkpoint_id, fingerprint, force=force):
        return 0
    rows = conn.execute(
        """
        SELECT id, report_key, event_ts, event_ts_utc, from_call, target, report_group,
               grid, state_code, state_confidence, geo_confidence, scope, subtype,
               overall_status, power, water, medical, communications, internet,
               travel, food, fuel, crime, civil_unrest, political, transport_mode,
               remarks_text, brevity_code, brevity_summary, source_first, source_last,
               source_count, sources_json, source_refs_json, raw_payload_json, updated_ts
          FROM sitrep_events
         ORDER BY COALESCE(event_ts, 0) DESC, id DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    projected = 0
    with conn:
        for row in rows:
            source_id = "sitrep:fused"
            external_key = _text(row["report_key"]) or _text(row["id"])
            message_id = stable_message_id(source_id, "sitrep_event", external_key)
            status = _status_from_condition(row["overall_status"])
            body = _sitrep_body(row)
            event_ts = _float(row["event_ts"])
            source = MessageSourceRecord(
                source_id=source_id,
                source_family="sitrep",
                source_label="SitRep",
                capabilities={"read": True, "delete": True, "native_open": True},
                provenance={"source_table": "sitrep_events"},
                last_seen_utc=_utc_from_ts(event_ts),
                last_ingested_utc=_utc_now(),
            )
            projection = MessageProjectionRecord(
                message_id=message_id,
                canonical_key=f"{source_id}:sitrep_event:{external_key}",
                content_hash=content_hash(PROJECTOR_VERSION, "sitrep", external_key, row["updated_ts"], body),
                primary_source_id=source_id,
                source_family="sitrep",
                source_label=source.source_label,
                message_type=_text(row["subtype"]) or "SitRep",
                display_type="SitRep",
                status=status,
                severity=_severity_from_status(status),
                read_state=_read_state(status),
                from_call=_upper(row["from_call"]),
                to_call=_upper(row["target"]),
                group_name=_group(row["report_group"] or row["target"]),
                scope=_text(row["scope"]),
                state_code=_upper(row["state_code"]),
                grid=_upper(row["grid"]),
                event_ts=event_ts,
                received_ts=event_ts,
                event_utc=_text(row["event_ts_utc"]) or _utc_from_ts(event_ts),
                received_utc=_text(row["event_ts_utc"]) or _utc_from_ts(event_ts),
                subject=_sitrep_subject(row),
                summary=body[:240],
                body_preview=body[:1200],
                topics=_topics(body, row["overall_status"], row["power"], row["water"], row["medical"], row["communications"], row["fuel"]),
                entities=_row_entities(row, ("overall_status", "power", "water", "medical", "communications", "internet", "travel", "food", "fuel", "crime", "civil_unrest", "political")),
                actionable=status in {"YELLOW", "RED", "ALERT"},
                operator_attention=status in {"YELLOW", "RED", "ALERT"},
                confidence=0.9,
                recommended_action="review" if status in {"YELLOW", "RED"} else "",
                retention_class="operational",
                search_text=_search_text(row["from_call"], row["target"], row["report_group"], body),
                projection_version=PROJECTOR_VERSION,
            )
            _upsert_bundle(
                conn,
                source,
                projection,
                ExternalMessageRef(
                    message_id=message_id,
                    source_id=source_id,
                    external_kind="sitrep_event",
                    external_key=external_key,
                    delete_capability="delete_source",
                    read_capability="mark_read",
                    metadata={"source_table": "sitrep_events", "row_id": _text(row["id"]), "source_refs": _json_array(row["source_refs_json"])},
                ),
            )
            projected += 1
        _set_checkpoint(conn, checkpoint_id, fingerprint, rows)
    return projected


def _project_commstat_artifacts(conn: sqlite3.Connection, limit: int, force: bool) -> int:
    if not table_exists(conn, "commstat_artifacts"):
        return 0
    checkpoint_id = "native:commstat_artifacts"
    fingerprint = _table_fingerprint(
        conn,
        "commstat_artifacts",
        "COUNT(*)",
        "MAX(COALESCE(id, 0))",
        "MAX(COALESCE(event_ts, 0))",
        "MAX(COALESCE(updated_ts, 0))",
    )
    if table_exists(conn, "commstat_artifact_deletions"):
        fingerprint = content_hash(fingerprint, _table_fingerprint(conn, "commstat_artifact_deletions", "COUNT(*)", "MAX(COALESCE(deleted_ts, 0))"))
    if _checkpoint_matches(conn, checkpoint_id, fingerprint, force=force):
        return 0
    deletion_join = ""
    deletion_where = ""
    if table_exists(conn, "commstat_artifact_deletions"):
        deletion_join = "LEFT JOIN commstat_artifact_deletions cad ON cad.artifact_key = ca.artifact_key"
        deletion_where = "WHERE cad.artifact_key IS NULL"
    rows = conn.execute(
        f"""
        SELECT ca.id, ca.artifact_key, ca.artifact_kind, ca.subtype, ca.event_ts,
               ca.event_ts_utc, ca.from_call, ca.target, ca.report_group, ca.grid,
               ca.state_code, ca.scope, ca.transport_mode, ca.reach_mode,
               ca.origin_path, ca.status_label, ca.alert_color, ca.title,
               ca.body_text, ca.remarks_text, ca.source_first, ca.source_last,
               ca.source_count, ca.sources_json, ca.source_refs_json,
               ca.external_ids_json, ca.payload_json, ca.updated_ts
          FROM commstat_artifacts ca
          {deletion_join}
          {deletion_where}
         ORDER BY COALESCE(ca.event_ts, 0) DESC, ca.id DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    projected = 0
    with conn:
        for row in rows:
            source_id = "commstat:artifacts"
            external_key = _text(row["artifact_key"]) or _text(row["id"])
            message_id = stable_message_id(source_id, "commstat_artifact", external_key)
            status = _upper(row["status_label"]) or _upper(row["alert_color"]) or "INFO"
            body = _text(row["body_text"]) or _text(row["remarks_text"]) or _text(row["title"])
            event_ts = _float(row["event_ts"])
            source = MessageSourceRecord(
                source_id=source_id,
                source_family="commstat",
                source_label="CommStat RF",
                endpoint_or_path=_text(row["origin_path"]),
                capabilities={"read": True, "delete": True, "native_open": True},
                provenance={"source_table": "commstat_artifacts"},
                last_seen_utc=_utc_from_ts(event_ts),
                last_ingested_utc=_utc_now(),
            )
            projection = MessageProjectionRecord(
                message_id=message_id,
                canonical_key=f"{source_id}:commstat_artifact:{external_key}",
                content_hash=content_hash(PROJECTOR_VERSION, "commstat", external_key, row["updated_ts"], status, body),
                primary_source_id=source_id,
                source_family="commstat",
                source_label=source.source_label,
                message_type=_text(row["subtype"]) or _text(row["artifact_kind"]) or "CommStat",
                display_type="CommStat",
                status=status,
                severity=_severity_from_status(status),
                read_state=_read_state(status),
                from_call=_upper(row["from_call"]),
                to_call=_upper(row["target"]),
                group_name=_group(row["report_group"] or row["target"]),
                scope=_text(row["scope"]),
                state_code=_upper(row["state_code"]),
                grid=_upper(row["grid"]),
                event_ts=event_ts,
                received_ts=event_ts,
                event_utc=_text(row["event_ts_utc"]) or _utc_from_ts(event_ts),
                received_utc=_text(row["event_ts_utc"]) or _utc_from_ts(event_ts),
                subject=_text(row["title"]) or _subject(body),
                summary=body[:240],
                body_preview=body[:1200],
                topics=_topics(body, row["status_label"], row["alert_color"], row["reach_mode"], row["transport_mode"]),
                entities=_row_entities(row, ("artifact_kind", "subtype", "reach_mode", "transport_mode", "source_count")),
                actionable=status in {"YELLOW", "RED", "ALERT", "WARNING"},
                operator_attention=status in {"YELLOW", "RED", "ALERT", "WARNING"},
                confidence=0.9,
                recommended_action="review" if status in {"YELLOW", "RED", "ALERT", "WARNING"} else "",
                retention_class="operational",
                search_text=_search_text(row["from_call"], row["target"], row["report_group"], row["title"], body),
                projection_version=PROJECTOR_VERSION,
            )
            _upsert_bundle(
                conn,
                source,
                projection,
                ExternalMessageRef(
                    message_id=message_id,
                    source_id=source_id,
                    external_kind="commstat_artifact",
                    external_key=external_key,
                    external_path=_text(row["origin_path"]),
                    delete_capability="delete_source",
                    read_capability="mark_read",
                    metadata={
                        "source_table": "commstat_artifacts",
                        "row_id": _text(row["id"]),
                        "source_refs": _json_array(row["source_refs_json"]),
                        "external_ids": _json_array(row["external_ids_json"]),
                    },
                ),
                artifacts=(
                    MessageArtifactRecord(
                        artifact_id=stable_message_id(message_id, "commstat_artifact", external_key),
                        message_id=message_id,
                        artifact_type="commstat_artifact",
                        source_id=source_id,
                        external_key=external_key,
                        path=_text(row["origin_path"]),
                        content_hash=content_hash(_text(row["payload_json"]), body),
                        metadata={"payload": _json_object(row["payload_json"])},
                    ),
                ),
            )
            projected += 1
        _set_checkpoint(conn, checkpoint_id, fingerprint, rows)
    return projected


def _upsert_bundle(
    conn: sqlite3.Connection,
    source: MessageSourceRecord,
    message: MessageProjectionRecord,
    ref: ExternalMessageRef,
    *,
    artifacts: Sequence[MessageArtifactRecord] = (),
) -> None:
    upsert_message_source(conn, source)
    upsert_message_projection(conn, message)
    upsert_external_ref(conn, ref)
    for artifact in artifacts:
        upsert_message_artifact(conn, artifact)


def _checkpoint_matches(conn: sqlite3.Connection, source_id: str, fingerprint: str, *, force: bool) -> bool:
    if force:
        return False
    try:
        row = conn.execute(
            "SELECT content_fingerprint FROM message_projection_checkpoint WHERE source_id=?",
            (str(source_id or "").strip(),),
        ).fetchone()
    except Exception:
        return False
    current = str(row["content_fingerprint"] or "") if row is not None else ""
    return bool(current and current == fingerprint)


def _set_checkpoint(conn: sqlite3.Connection, source_id: str, fingerprint: str, rows: Sequence[object]) -> None:
    last_key = ""
    last_ts = 0.0
    for row in rows:
        if isinstance(row, FileRecord):
            key = str(row.path)
            ts = float(row.mtime or 0.0)
        else:
            row_id = _text(_row_value(row, "id"))
            key = _text(_row_value(row, "artifact_key")) or _text(_row_value(row, "report_key")) or _text(_row_value(row, "guid")) or row_id
            ts = _float(_row_value(row, "event_ts")) or _float(_row_value(row, "utc_ts")) or _float(_row_value(row, "ts"))
        if ts >= last_ts:
            last_ts = ts
            last_key = key
    set_message_projection_checkpoint(
        conn,
        MessageProjectionCheckpoint(
            source_id=source_id,
            last_external_key=last_key,
            last_event_ts=last_ts,
            content_fingerprint=fingerprint,
        ),
    )


def _table_fingerprint(conn: sqlite3.Connection, table_name: str, *expressions: str) -> str:
    if not expressions:
        expressions = ("COUNT(*)",)
    try:
        row = conn.execute(f"SELECT {', '.join(expressions)} FROM {table_name}").fetchone()
    except Exception:
        return ""
    values = tuple(row or ())
    return content_hash(PROJECTOR_VERSION, table_name, values)


def _row_value(row: sqlite3.Row, key: str) -> object:
    try:
        return row[key]
    except Exception:
        return None


def _text(value: object) -> str:
    return str(value or "").strip()


def _upper(value: object) -> str:
    return _text(value).upper()


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _int(value: object) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _optional_int(value: object) -> int | None:
    parsed = _int(value)
    return parsed if parsed > 0 else None


def _group(value: object) -> str:
    return normalize_group_name(_text(value)).lstrip("@").upper()


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _utc_from_ts(value: object) -> str:
    ts = _float(value)
    if ts <= 0:
        return ""
    try:
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _source_label(base: str, source_key: str) -> str:
    return f"{base} {source_key}".strip() if source_key and source_key != "legacy" else base


def _file_source_base_label(origin: str) -> str:
    return {
        "flmsg": "FLMsg",
        "flamp": "FLAmp",
        "varac": "VarAC",
        "bbs": "BBS",
    }.get(_text(origin).lower(), "File")


def _file_message_type(origin: str, path: Path) -> str:
    base = _file_source_base_label(origin)
    suffix = path.suffix.lower().lstrip(".")
    return f"{base} {suffix.upper()}".strip() if suffix else base


def _q_id_from_path(path: Path) -> str:
    name = path.name
    for pattern in (r"\bQ[0-9A-Z]{3,}\b", r"\b[A-Z]{1,4}[0-9]{3,}\b"):
        match = re.search(pattern, name.upper())
        if match:
            return match.group(0)
    return ""


def _block_id_from_path(path: Path) -> str:
    match = re.search(r"(?:BLOCK|BLK|[-_])([0-9]{1,4})(?:\D|$)", path.name.upper())
    return match.group(1) if match else ""


def _read_state(status: str) -> str:
    clean = _upper(status)
    if clean in {"READ", "GREEN", "INFO"}:
        return "read" if clean == "READ" else "info"
    if clean in {"UNREAD", "NEW", "ALERT", "YELLOW", "RED", "WARNING"}:
        return "new"
    return "info"


def _severity_from_status(status: str) -> str:
    clean = _upper(status)
    if clean in {"RED", "ALERT", "CRITICAL"}:
        return "critical"
    if clean in {"YELLOW", "WARNING", "WARN"}:
        return "warning"
    if clean in {"WATCH"}:
        return "watch"
    return "info"


def _status_from_condition(value: object) -> str:
    clean = _text(value).lower()
    if clean in {"red", "critical", "emergency"}:
        return "RED"
    if clean in {"yellow", "warning", "degraded", "limited"}:
        return "YELLOW"
    if clean in {"green", "ok", "normal"}:
        return "GREEN"
    return clean.upper() if clean else "INFO"


def _subject(*values: object) -> str:
    for value in values:
        text = _text(value).replace("\r", " ").replace("\n", " ")
        if text:
            return text[:80]
    return ""


def _topics(*values: object) -> tuple[str, ...]:
    joined = " ".join(_text(value).lower() for value in values if _text(value))
    found: list[str] = []
    rules = (
        ("Power", ("power", "generator", "battery", "fuel")),
        ("Water", ("water",)),
        ("Medical", ("medical", "med", "injury", "health")),
        ("Comms", ("comm", "radio", "internet", "phone")),
        ("Travel", ("travel", "road", "route")),
        ("Safety", ("crime", "civil", "unrest", "security")),
        ("BBS", ("bbs", "vmail", "mailbox")),
    )
    for label, needles in rules:
        if any(needle in joined for needle in needles):
            found.append(label)
    return tuple(dict.fromkeys(found))


def _search_text(*values: object) -> str:
    return " ".join(_text(value) for value in values if _text(value))[:4000]


def _json_array(value: object) -> list[object]:
    try:
        parsed = json.loads(_text(value) or "[]")
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _json_object(value: object) -> dict[str, object]:
    try:
        parsed = json.loads(_text(value) or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _row_entities(row: sqlite3.Row, keys: Sequence[str]) -> Mapping[str, object]:
    out: dict[str, object] = {}
    for key in keys:
        value = _row_value(row, key)
        if value not in (None, ""):
            out[key] = value
    return out


def _sitrep_subject(row: sqlite3.Row) -> str:
    parts = [_upper(row["from_call"]), _text(row["overall_status"]).upper(), _text(row["brevity_code"])]
    return " ".join(part for part in parts if part)[:120]


def _sitrep_body(row: sqlite3.Row) -> str:
    fields = (
        ("Overall", row["overall_status"]),
        ("Power", row["power"]),
        ("Water", row["water"]),
        ("Medical", row["medical"]),
        ("Comms", row["communications"]),
        ("Internet", row["internet"]),
        ("Travel", row["travel"]),
        ("Food", row["food"]),
        ("Fuel", row["fuel"]),
        ("Crime", row["crime"]),
        ("Civil Unrest", row["civil_unrest"]),
        ("Political", row["political"]),
    )
    parts = [f"{label}: {_text(value)}" for label, value in fields if _text(value)]
    remarks = _text(row["remarks_text"])
    brevity = _text(row["brevity_summary"])
    if brevity:
        parts.append(f"Brevity: {brevity}")
    if remarks:
        parts.append(f"Remarks: {remarks}")
    return " | ".join(parts)
