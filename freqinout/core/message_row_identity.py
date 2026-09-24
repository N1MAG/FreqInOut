from __future__ import annotations

from typing import Any

from freqinout.core.commstat_artifacts import normalize_commstat_artifact_key
from freqinout.core.message_file_metadata import file_metadata_key
from freqinout.core.message_file_scanner import FileRecord


def _class_name(value: Any) -> str:
    return value.__class__.__name__ if value is not None else ""


def message_payload_identity(payload: Any) -> tuple | None:
    cls = _class_name(payload)
    if cls == "JS8Message":
        msg_id = int(getattr(payload, "msg_id", 0) or 0)
        return ("js8", msg_id) if msg_id > 0 else None
    if cls == "SpotterMessage":
        msg_id = int(getattr(payload, "spotter_id", 0) or 0)
        return ("spotter", msg_id) if msg_id > 0 else None
    if cls == "VarACMessage":
        msg_id = int(getattr(payload, "msg_id", 0) or 0)
        source = str(getattr(payload, "source", "") or "")
        return ("varac", source, msg_id) if msg_id > 0 and source else None
    if isinstance(payload, FileRecord):
        return ("file",) + file_metadata_key(payload)
    if cls == "SitrepMessage":
        event_id = int(getattr(payload, "event_id", 0) or 0)
        if event_id > 0:
            return ("sitrep", event_id)
        report_key = str(getattr(payload, "report_key", "") or "").strip().lower()
        return ("sitrep", report_key) if report_key else None
    if cls == "CommStatArtifact":
        artifact_key = normalize_commstat_artifact_key(getattr(payload, "artifact_key", ""))
        return ("commstat", artifact_key) if artifact_key else None
    if cls == "ProjectedMessagePayload":
        message_id = str(getattr(payload, "message_id", "") or "").strip()
        return ("projected", message_id) if message_id else None
    return None


def message_row_identity(row: Any) -> tuple | None:
    return message_payload_identity(getattr(row, "payload", None))


def message_row_identity_set(rows: Any) -> set[tuple]:
    out: set[tuple] = set()
    try:
        iterator = iter(rows or [])
    except TypeError:
        return out
    for row in iterator:
        key = message_row_identity(row)
        if key is not None:
            out.add(key)
    return out


def filter_rows_excluding_identities(rows: Any, identities: Any) -> list[Any]:
    try:
        blocked = {key for key in identities or set() if key is not None}
    except TypeError:
        blocked = set()
    if not blocked:
        return list(rows or [])
    return [row for row in list(rows or []) if message_row_identity(row) not in blocked]
