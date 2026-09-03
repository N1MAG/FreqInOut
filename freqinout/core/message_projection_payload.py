from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Mapping, Sequence


@dataclass
class ProjectedMessagePayload:
    message_id: str
    canonical_key: str
    source_family: str
    source_label: str = ""
    source_ref: str = ""
    message_type: str = ""
    display_type: str = ""
    status: str = ""
    severity: str = ""
    read_state: str = ""
    from_call: str = ""
    to_call: str = ""
    group: str = ""
    report_group: str = ""
    target: str = ""
    scope: str = ""
    state_code: str = ""
    grid: str = ""
    subject: str = ""
    summary: str = ""
    body_preview: str = ""
    topics: tuple[str, ...] = ()
    entities: Mapping[str, object] = field(default_factory=dict)
    intelligence: Mapping[str, object] = field(default_factory=dict)
    provenance: Mapping[str, object] = field(default_factory=dict)
    external_refs: Sequence[Mapping[str, object]] = field(default_factory=tuple)
    flag_state: int = 0


def projected_payload_from_row(
    row: object,
    *,
    external_refs: Sequence[Mapping[str, object]] = (),
) -> ProjectedMessagePayload:
    message_id = _text(_get(row, "message_id"))
    group = _text(_get(row, "group_name")).lstrip("@").upper()
    family = _text(_get(row, "source_family")).lower() or "message"
    entities = _json_mapping(_get(row, "entities_json"))
    intelligence = _json_mapping(_get(row, "intelligence_json"))
    provenance = {
        "source_id": _text(_get(row, "primary_source_id")),
        "app_instance_id": _text(_get(row, "app_instance_id")),
        "radio_id": _get(row, "radio_id"),
        "retention_class": _text(_get(row, "retention_class")),
        "projection_version": _get(row, "projection_version"),
    }
    return ProjectedMessagePayload(
        message_id=message_id,
        canonical_key=_text(_get(row, "canonical_key")),
        source_family=family,
        source_label=_text(_get(row, "source_label")),
        source_ref=_text(_get(row, "primary_source_id")),
        message_type=_text(_get(row, "message_type")),
        display_type=_text(_get(row, "display_type")),
        status=_text(_get(row, "status")),
        severity=_text(_get(row, "severity")),
        read_state=_text(_get(row, "read_state")),
        from_call=_text(_get(row, "from_call")).upper(),
        to_call=_text(_get(row, "to_call")).upper(),
        group=group,
        report_group=group,
        target=_text(_get(row, "to_call")).upper(),
        scope=_text(_get(row, "scope")),
        state_code=_text(_get(row, "state_code")).upper(),
        grid=_text(_get(row, "grid")).upper(),
        subject=_text(_get(row, "subject")),
        summary=_text(_get(row, "summary")),
        body_preview=_text(_get(row, "body_preview")),
        topics=tuple(str(item or "").strip() for item in _json_list(_get(row, "topics_json")) if str(item or "").strip()),
        entities=entities,
        intelligence=intelligence,
        provenance=provenance,
        external_refs=tuple(external_refs or ()),
        flag_state=1 if bool(_get(row, "operator_attention")) else 0,
    )


def _get(row: object, key: str) -> object:
    try:
        return row[key]  # type: ignore[index]
    except Exception:
        return getattr(row, key, "")


def _text(value: object) -> str:
    return str(value or "").strip()


def _json_mapping(value: object) -> Mapping[str, object]:
    try:
        decoded = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _json_list(value: object) -> list[object]:
    try:
        decoded = json.loads(str(value or "[]"))
    except Exception:
        return []
    return list(decoded) if isinstance(decoded, list) else []
