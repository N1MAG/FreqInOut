from __future__ import annotations

import datetime
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.message_projection_store import (
    ExternalMessageRef,
    MessageArtifactRecord,
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
    ensure_message_projection_schema,
    stable_message_id,
    upsert_external_ref,
    upsert_message_artifact,
    upsert_message_projection,
    upsert_message_source,
    queue_message_delete,
)
from freqinout.core.message_summary import MessageSummary, message_summary_from_row
from freqinout.core.sqlite_utils import connect_sqlite

PROJECTOR_VERSION = 1


def project_unified_message_rows(db_path: str | Path, rows: Iterable[object]) -> int:
    """Persist unified message rows into the normalized message projection tables."""
    row_list = list(rows or [])
    if not row_list:
        return 0
    conn = connect_sqlite(db_path)
    try:
        ensure_message_projection_schema(conn)
        projected = 0
        with conn:
            for row in row_list:
                bundle = _projection_bundle(row)
                if bundle is None:
                    continue
                source, message, ref, artifacts = bundle
                upsert_message_source(conn, source)
                upsert_message_projection(conn, message)
                upsert_external_ref(conn, ref)
                for artifact in artifacts:
                    upsert_message_artifact(conn, artifact)
                projected += 1
        return projected
    finally:
        conn.close()


def projected_message_id_for_row(row: object) -> str:
    bundle = _projection_bundle(row)
    if bundle is None:
        return ""
    _source, message, _ref, _artifacts = bundle
    return message.message_id


def mark_projected_message_rows_deleted(
    db_path: str | Path,
    rows: Iterable[object],
    *,
    requested_effect: str = "delete",
    requested_by: str = "fio",
    source_scope: str = "selected",
) -> int:
    row_list = list(rows or [])
    if not row_list:
        return 0
    conn = connect_sqlite(db_path)
    try:
        ensure_message_projection_schema(conn)
        marked = 0
        with conn:
            for row in row_list:
                bundle = _projection_bundle(row)
                if bundle is None:
                    continue
                source, message, ref, artifacts = bundle
                upsert_message_source(conn, source)
                upsert_message_projection(conn, message)
                upsert_external_ref(conn, ref)
                for artifact in artifacts:
                    upsert_message_artifact(conn, artifact)
                queue_message_delete(
                    conn,
                    message_id=message.message_id,
                    requested_effect=requested_effect,
                    requested_by=requested_by,
                    source_scope=source_scope,
                )
                marked += 1
        return marked
    finally:
        conn.close()


def _projection_bundle(
    row: object,
) -> tuple[MessageSourceRecord, MessageProjectionRecord, ExternalMessageRef, tuple[MessageArtifactRecord, ...]] | None:
    payload = getattr(row, "payload", None)
    try:
        summary = getattr(row, "summary", None) or message_summary_from_row(row)  # type: ignore[arg-type]
    except Exception:
        return None
    family = _source_family(row, summary)
    source_id = _source_id(row, payload, summary, family)
    external_kind = _external_kind(row, payload, family)
    external_key = _external_key(row, payload, summary, family)
    message_id = stable_message_id(source_id, external_kind, external_key)
    canonical_key = f"{source_id}:{external_kind}:{external_key}"
    body_preview = _body_preview(row, payload, summary)
    event_ts = _float(getattr(summary, "event_ts", 0.0)) or _float(getattr(row, "rcv_ts", 0.0))
    received_ts = _float(getattr(summary, "received_ts", 0.0)) or _float(getattr(row, "rcv_ts", 0.0))
    source = MessageSourceRecord(
        source_id=source_id,
        source_family=family,
        source_label=str(getattr(summary, "source_label", "") or _source_label(family)),
        radio_id=_optional_int(getattr(payload, "source_radio_id", None)),
        app_instance_id=_first_text(payload, "js8_instance_id", "source_key", "source"),
        endpoint_or_path=_endpoint_or_path(payload),
        capabilities=_source_capabilities(row, payload, family),
        provenance=_source_provenance(row, payload, summary),
        last_seen_utc=_utc_from_ts(received_ts),
        last_ingested_utc=_utc_now(),
    )
    status = str(getattr(row, "status", "") or getattr(summary, "status", "") or "INFO").strip().upper()
    severity = _projection_severity(summary, status=status)
    projection = MessageProjectionRecord(
        message_id=message_id,
        canonical_key=canonical_key,
        content_hash=content_hash(
            family,
            status,
            getattr(row, "msg_type", ""),
            getattr(row, "from_call", ""),
            getattr(row, "to_call", ""),
            getattr(row, "title", ""),
            body_preview,
            _payload_identity_text(payload),
        ),
        primary_source_id=source_id,
        source_family=family,
        source_label=source.source_label,
        radio_id=source.radio_id,
        app_instance_id=source.app_instance_id,
        message_type=str(getattr(row, "msg_type", "") or getattr(summary, "form_type", "") or ""),
        display_type=str(getattr(row, "display_type", "") or ""),
        status=status,
        severity=severity,
        read_state=_read_state(status),
        from_call=str(getattr(summary, "from_call", "") or getattr(row, "from_call", "") or ""),
        to_call=str(getattr(summary, "to_target", "") or getattr(row, "to_call", "") or ""),
        group_name=str(getattr(summary, "group", "") or "").lstrip("@").upper(),
        scope=_first_text(payload, "scope", "report_group"),
        state_code=_map_hint_attr(summary, "state"),
        grid=_map_hint_attr(summary, "grid"),
        lat=_optional_float(_map_hint_attr(summary, "latitude")),
        lon=_optional_float(_map_hint_attr(summary, "longitude")),
        event_ts=event_ts,
        received_ts=received_ts,
        event_utc=_utc_from_ts(event_ts),
        received_utc=_utc_from_ts(received_ts),
        subject=str(getattr(summary, "subject", "") or getattr(row, "title", "") or ""),
        summary=str(getattr(summary, "summary", "") or getattr(row, "title", "") or ""),
        body_preview=body_preview,
        topics=tuple(str(t or "").strip() for t in getattr(summary, "topics", ()) if str(t or "").strip()),
        entities=_entities(row, payload, summary),
        actionable=bool(getattr(row, "actionable", False)),
        operator_attention=_operator_attention(summary, status=status),
        confidence=_confidence(summary),
        recommended_action=_recommended_action(summary, status=status),
        intelligence_version=PROJECTOR_VERSION,
        intelligence_utc=_utc_now(),
        intelligence=_intelligence(summary),
        retention_class=_retention_class(family, severity),
        search_text=_search_text(row, summary),
        projection_version=PROJECTOR_VERSION,
    )
    ref = ExternalMessageRef(
        message_id=message_id,
        source_id=source_id,
        external_kind=external_kind,
        external_key=external_key,
        external_path=_external_path(payload),
        external_mtime=_float(getattr(payload, "mtime", 0.0)),
        external_size=int(_float(getattr(payload, "size", 0))),
        external_hash=_payload_hash(payload),
        delete_capability=_delete_capability(family, payload),
        read_capability=_read_capability(family, payload),
        metadata=_external_metadata(row, payload, summary),
    )
    artifacts = _artifact_records(message_id, source_id, external_key, payload, family)
    return source, projection, ref, artifacts


def _source_family(row: object, summary: MessageSummary) -> str:
    value = str(getattr(summary, "source_family", "") or getattr(row, "origin", "") or "message").strip().lower()
    if value == "local_report":
        return "local"
    return value or "message"


def _source_id(row: object, payload: object, summary: MessageSummary, family: str) -> str:
    for attr in ("source_key", "source_id", "source_radio_id", "js8_instance_id", "source"):
        value = str(getattr(payload, attr, "") or "").strip()
        if value and value != "0":
            return f"{family}:{value}"
    if isinstance(payload, FileRecord):
        return f"{family}:{payload.path.parent}"
    ref = str(getattr(summary.provenance, "source_ref", "") if summary.provenance else "").strip()
    if ref:
        return f"{family}:{ref}"
    return f"{family}:{str(getattr(row, 'origin', '') or 'default').strip() or 'default'}"


def _external_kind(row: object, payload: object, family: str) -> str:
    cls_name = payload.__class__.__name__.lower() if payload is not None else ""
    if "js8" in cls_name:
        return "js8_message"
    if "spotter" in cls_name:
        return "spotter_message"
    if "varac" in cls_name:
        return "varac_message"
    if "sitrep" in cls_name:
        return "sitrep_event"
    if "commstat" in cls_name:
        return "commstat_artifact"
    if isinstance(payload, FileRecord):
        return f"{family}_file"
    return f"{family}_message"


def _external_key(row: object, payload: object, summary: MessageSummary, family: str) -> str:
    for attr in ("artifact_key", "report_key", "guid", "msg_id", "spotter_id", "event_id", "artifact_id", "vmail_guid"):
        value = str(getattr(payload, attr, "") or "").strip()
        if value and value != "0":
            return value
    if isinstance(payload, FileRecord):
        return f"{payload.path}:{float(payload.mtime or 0.0):.6f}:{int(payload.size or 0)}"
    stable = str(getattr(summary, "stable_id", "") or "").strip()
    if stable:
        return stable
    return stable_message_id(family, getattr(row, "from_call", ""), getattr(row, "to_call", ""), getattr(row, "rcv_ts", ""))


def _source_label(family: str) -> str:
    return {
        "js8": "JS8Call",
        "spotter": "FIOSpotter",
        "varac": "VarAC",
        "bbs": "BBS",
        "flmsg": "FLMsg",
        "flamp": "FLAmp",
        "sitrep": "SitRep",
        "commstat": "CommStat RF",
    }.get(family, family.upper() if family else "Message")


def _source_capabilities(row: object, payload: object, family: str) -> Mapping[str, object]:
    return {
        "read": bool(_read_capability(family, payload)),
        "delete": bool(_delete_capability(family, payload)),
        "native_open": isinstance(payload, FileRecord) or family in {"js8", "spotter", "varac"},
    }


def _source_provenance(row: object, payload: object, summary: MessageSummary) -> Mapping[str, object]:
    provenance = getattr(summary, "provenance", None)
    return {
        "row_origin": str(getattr(row, "origin", "") or ""),
        "adapter_label": str(getattr(provenance, "adapter_label", "") or "") if provenance else "",
        "source_ref": str(getattr(provenance, "source_ref", "") or "") if provenance else "",
        "payload_type": payload.__class__.__name__ if payload is not None else "",
    }


def _projection_severity(summary: MessageSummary, *, status: str) -> str:
    severity = str(getattr(summary, "severity", "") or "").strip().lower()
    return {
        "urgent": "critical",
        "important": "warning",
        "watch": "watch",
        "routine": "info",
    }.get(severity, "critical" if status == "ALERT" else "info")


def _operator_attention(summary: MessageSummary, *, status: str) -> bool:
    severity = str(getattr(summary, "severity", "") or "").strip().lower()
    return severity in {"urgent", "important"} or status in {"ALERT"}


def _read_state(status: str) -> str:
    if status == "READ":
        return "read"
    if status in {"UNREAD", "NEW", "ALERT"}:
        return "new"
    return "info"


def _body_preview(row: object, payload: object, summary: MessageSummary) -> str:
    for value in (
        getattr(summary, "summary", ""),
        getattr(payload, "body_text", ""),
        getattr(payload, "remarks_text", ""),
        getattr(payload, "body", ""),
        getattr(payload, "decoded_text", ""),
        getattr(payload, "raw_text", ""),
        getattr(row, "title", ""),
    ):
        text = _collapse(value)
        if text:
            return text[:1200]
    return ""


def _entities(row: object, payload: object, summary: MessageSummary) -> Mapping[str, object]:
    return {
        "from_call": str(getattr(summary, "from_call", "") or getattr(row, "from_call", "") or "").upper(),
        "to_call": str(getattr(summary, "to_target", "") or getattr(row, "to_call", "") or "").upper(),
        "target": _first_text(payload, "target"),
        "state": _map_hint_attr(summary, "state"),
        "grid": _map_hint_attr(summary, "grid"),
        "q_id": _q_id_for_payload(payload),
    }


def _confidence(summary: MessageSummary) -> float:
    label = ""
    if summary.provenance is not None:
        label = str(getattr(summary.provenance, "confidence_label", "") or "").strip().lower()
    if label in {"high", "confirmed"}:
        return 0.9
    if label in {"medium", "likely"}:
        return 0.7
    if label in {"low", "uncertain"}:
        return 0.4
    return 0.0


def _recommended_action(summary: MessageSummary, *, status: str) -> str:
    severity = str(getattr(summary, "severity", "") or "").strip().lower()
    if severity == "urgent" or status == "ALERT":
        return "review_now"
    if severity == "important":
        return "review"
    return ""


def _intelligence(summary: MessageSummary) -> Mapping[str, object]:
    hint = getattr(summary, "map_hint", None)
    provenance = getattr(summary, "provenance", None)
    return {
        "severity": str(getattr(summary, "severity", "") or ""),
        "source_label": str(getattr(summary, "source_label", "") or ""),
        "form_type": str(getattr(summary, "form_type", "") or ""),
        "visible_by_default": bool(getattr(summary, "visible_by_default", True)),
        "map": {
            "state": str(getattr(hint, "state", "") or "") if hint else "",
            "grid": str(getattr(hint, "grid", "") or "") if hint else "",
            "precision": str(getattr(hint, "precision", "") or "") if hint else "",
        },
        "provenance": {
            "trust": str(getattr(provenance, "trust_label", "") or "") if provenance else "",
            "freshness": str(getattr(provenance, "freshness_label", "") or "") if provenance else "",
            "rf_only": bool(getattr(provenance, "is_rf_only", False)) if provenance else False,
            "relayed": bool(getattr(provenance, "is_relayed", False)) if provenance else False,
        },
    }


def _retention_class(family: str, severity: str) -> str:
    if severity in {"critical", "warning"}:
        return "operator_attention"
    if family in {"flamp", "flmsg", "bbs"}:
        return "artifact"
    return "normal"


def _search_text(row: object, summary: MessageSummary) -> str:
    pieces = [
        getattr(row, "search_text", ""),
        getattr(summary, "search_text", ""),
        getattr(summary, "subject", ""),
        getattr(summary, "summary", ""),
        getattr(summary, "from_call", ""),
        getattr(summary, "to_target", ""),
        getattr(summary, "group", ""),
    ]
    return " ".join(_collapse(piece) for piece in pieces if _collapse(piece))


def _external_metadata(row: object, payload: object, summary: MessageSummary) -> Mapping[str, object]:
    metadata: dict[str, object] = {
        "row_key": _row_identity(row),
        "status": str(getattr(row, "status", "") or ""),
        "title": str(getattr(row, "title", "") or ""),
        "source_metadata": dict(getattr(summary, "source_metadata", {}) or {}),
    }
    for attr in (
        "source_key",
        "source_id",
        "source_radio_id",
        "js8_instance_id",
        "source_path",
        "read_ts",
        "flag_state",
        "folder",
        "vmail_guid",
        "source_refs_json",
        "external_ids_json",
    ):
        if hasattr(payload, attr):
            metadata[attr] = getattr(payload, attr)
    return metadata


def _artifact_records(
    message_id: str,
    source_id: str,
    external_key: str,
    payload: object,
    family: str,
) -> tuple[MessageArtifactRecord, ...]:
    if not isinstance(payload, FileRecord):
        return ()
    path = str(payload.path)
    artifact_type = {
        "flamp": "flamp_transfer",
        "flmsg": "form_file",
        "bbs": "bbs_file",
    }.get(family, f"{family}_file")
    q_id = _q_id_for_payload(payload)
    block_id = _block_id_from_path(path)
    artifact_id = stable_message_id(message_id, artifact_type, path, payload.mtime, payload.size)
    return (
        MessageArtifactRecord(
            artifact_id=artifact_id,
            message_id=message_id,
            artifact_type=artifact_type,
            source_id=source_id,
            external_key=external_key,
            path=path,
            content_hash=_payload_hash(payload),
            q_id=q_id,
            block_id=block_id,
            transfer_id=q_id,
            transfer_state=_transfer_state(payload),
            metadata={
                "origin": str(payload.origin or ""),
                "mtime": float(payload.mtime or 0.0),
                "size": int(payload.size or 0),
            },
        ),
    )


def _delete_capability(family: str, payload: object) -> str:
    if isinstance(payload, FileRecord):
        return "file_delete"
    return {
        "js8": "js8_delete",
        "spotter": "spotter_delete",
        "varac": "varac_soft_delete",
        "sitrep": "sitrep_delete",
        "commstat": "commstat_delete",
        "bbs": "file_delete",
        "flmsg": "file_delete",
        "flamp": "file_delete",
    }.get(family, "")


def _read_capability(family: str, payload: object) -> str:
    if isinstance(payload, FileRecord):
        return "fio_read_state"
    return {
        "js8": "js8_mark_read",
        "spotter": "spotter_mark_read",
        "varac": "varac_mark_read",
    }.get(family, "fio_read_state")


def _external_path(payload: object) -> str:
    if isinstance(payload, FileRecord):
        return str(payload.path)
    return _first_text(payload, "source_path", "folder")


def _endpoint_or_path(payload: object) -> str:
    if isinstance(payload, FileRecord):
        return str(payload.path.parent)
    return _first_text(payload, "source_path", "folder", "source")


def _payload_hash(payload: object) -> str:
    if isinstance(payload, FileRecord):
        return content_hash(str(payload.path), payload.mtime, payload.size)
    return content_hash(_payload_identity_text(payload))


def _payload_identity_text(payload: object) -> str:
    if payload is None:
        return ""
    if hasattr(payload, "__dict__"):
        try:
            return json.dumps(payload.__dict__, default=str, sort_keys=True)
        except Exception:
            pass
    return str(payload)


def _q_id_for_payload(payload: object) -> str:
    text_parts = []
    if isinstance(payload, FileRecord):
        text_parts.extend([str(payload.path.name), str(payload.path)])
    for attr in ("artifact_key", "guid", "vmail_guid", "subject", "title", "body_text", "body"):
        value = str(getattr(payload, attr, "") or "").strip()
        if value:
            text_parts.append(value)
    text = " ".join(text_parts)
    match = re.search(r"\b(Q[A-Z0-9][A-Z0-9_-]{2,})\b", text, re.IGNORECASE)
    if not match:
        return ""
    q_id = re.split(r"[_-](?:block|blk|part)[_-]?\d+", match.group(1), maxsplit=1, flags=re.IGNORECASE)[0]
    return q_id.upper()


def _block_id_from_path(path: str) -> str:
    name = Path(path).name
    match = re.search(r"(?:block|blk|part)[_-]?(\d{1,4})", name, re.IGNORECASE)
    if match:
        return match.group(1)
    return ""


def _transfer_state(payload: FileRecord) -> str:
    name = payload.path.name.lower()
    if any(token in name for token in ("missing", "partial", "incomplete")):
        return "partial"
    return "complete"


def _map_hint_attr(summary: MessageSummary, attr: str) -> Any:
    hint = getattr(summary, "map_hint", None)
    return getattr(hint, attr, "") if hint is not None else ""


def _first_text(payload: object, *attrs: str) -> str:
    for attr in attrs:
        value = str(getattr(payload, attr, "") or "").strip()
        if value:
            return value
    return ""


def _collapse(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _row_identity(row: object) -> str:
    try:
        from freqinout.core.message_row_identity import message_row_identity

        key = message_row_identity(row)
        return json.dumps(key, default=str, separators=(",", ":")) if key is not None else ""
    except Exception:
        return ""


def _optional_int(value: object) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        result = int(value)
        return result if result > 0 else None
    except Exception:
        return None


def _optional_float(value: object) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _utc_from_ts(ts: float) -> str:
    if ts <= 0:
        return ""
    try:
        return datetime.datetime.fromtimestamp(float(ts), tz=datetime.timezone.utc).isoformat()
    except Exception:
        return ""


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
