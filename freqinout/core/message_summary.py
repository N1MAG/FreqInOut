from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Iterable, Mapping, Protocol, Sequence

from freqinout.core.message_file_scanner import FileRecord


DEFAULT_TRAFFIC_RETENTION_DAYS = 7


class MessageSummaryRowLike(Protocol):
    msg_type: str
    status: str
    from_call: str
    to_call: str
    rcv_ts: float
    rcv_display: str
    title: str
    origin: str
    payload: object
    search_text: str
    topics: tuple[str, ...]
    actionable: bool
    auth_state: str
    auth_detail: str
    auth_trusted: bool
    expect_decision: str
    expect_detail: str


@dataclass(frozen=True)
class MessageActionValidity:
    can_read: bool = True
    can_reply: bool = False
    can_map: bool = False
    can_open_native: bool = False
    can_pin: bool = True
    can_delete: bool = False
    disabled_reason: str = ""


@dataclass(frozen=True)
class MessageProvenance:
    source_family: str
    source_label: str
    source_ref: str = ""
    adapter_label: str = ""
    radio_short_name: str = ""
    freshness_label: str = ""
    trust_label: str = "Unknown"
    confidence_label: str = ""
    is_rf_only: bool = False
    is_local: bool = False
    is_relayed: bool = False
    is_imported: bool = False
    is_internet_backed: bool = False


@dataclass(frozen=True)
class MessageMapHint:
    state: str = ""
    grid: str = ""
    region: str = ""
    latitude: float | None = None
    longitude: float | None = None
    precision: str = ""
    unavailable_reason: str = ""


@dataclass(frozen=True)
class MessageSummary:
    source_family: str
    source_label: str
    stable_id: str
    received_ts: float
    event_ts: float
    from_call: str
    to_target: str
    group: str
    subject: str
    summary: str
    form_type: str
    status: str
    severity: str
    topics: tuple[str, ...] = ()
    search_text: str = ""
    provenance: MessageProvenance | None = None
    map_hint: MessageMapHint | None = None
    actions: MessageActionValidity = MessageActionValidity()
    source_metadata: Mapping[str, object] | None = None

    @property
    def visible_by_default(self) -> bool:
        if self.severity in {"urgent", "important", "watch"}:
            return True
        age_days = _age_days(self.event_ts or self.received_ts)
        return age_days is None or age_days <= DEFAULT_TRAFFIC_RETENTION_DAYS


def message_summary_from_row(
    row: MessageSummaryRowLike,
    *,
    radio_short_name: object = "",
    now_ts: float | None = None,
) -> MessageSummary:
    """Build the source-neutral inbox projection from an existing message row."""
    payload = getattr(row, "payload", None)
    source_family = normalize_message_source_family(getattr(row, "origin", ""))
    source_label = message_source_label(source_family)
    form_type = str(getattr(row, "msg_type", "") or "").strip()
    status = str(getattr(row, "status", "") or "").strip().upper()
    topics = tuple(str(t or "").strip() for t in getattr(row, "topics", ()) if str(t or "").strip())
    event_ts = _event_ts(row, payload)
    received_ts = _float(getattr(row, "rcv_ts", 0.0))
    from_call = _clean_call(getattr(row, "from_call", ""))
    to_target = _clean_target(getattr(row, "to_call", ""))
    group = _group_from_payload_or_target(payload, to_target)
    subject = _subject_from_row(row, payload)
    summary = _summary_from_row(row, payload, subject)
    map_hint = _map_hint_from_payload(payload)
    actions = _actions_for_row(row, payload, map_hint)
    provenance = MessageProvenance(
        source_family=source_family,
        source_label=source_label,
        source_ref=_source_ref(row, payload),
        adapter_label=_adapter_label(source_family, payload),
        radio_short_name=str(radio_short_name or _radio_short_name_from_payload(payload)).strip(),
        freshness_label=_freshness_label(event_ts or received_ts, now_ts=now_ts),
        trust_label=_trust_label(row, payload, source_family),
        confidence_label=_confidence_label(payload),
        is_rf_only=source_family in {"js8", "spotter", "commstat", "flmsg", "flamp"},
        is_local=source_family in {"local_report"},
        is_relayed=bool(str(getattr(payload, "relay_via", "") or "").strip()),
        is_imported=source_family in {"spotter", "sitrep", "commstat"} and bool(_source_ref(row, payload)),
        is_internet_backed=False,
    )
    severity = _severity(row, payload, status=status, actionable=bool(getattr(row, "actionable", False)))
    return MessageSummary(
        source_family=source_family,
        source_label=source_label,
        stable_id=_stable_id(row, payload, source_family),
        received_ts=received_ts,
        event_ts=event_ts or received_ts,
        from_call=from_call,
        to_target=to_target,
        group=group,
        subject=subject,
        summary=summary,
        form_type=form_type,
        status=status,
        severity=severity,
        topics=topics,
        search_text=str(getattr(row, "search_text", "") or "").strip(),
        provenance=provenance,
        map_hint=map_hint,
        actions=actions,
        source_metadata=_source_metadata(payload),
    )


def message_summaries_from_rows(
    rows: Iterable[MessageSummaryRowLike],
    *,
    radio_short_name: object = "",
    now_ts: float | None = None,
) -> tuple[MessageSummary, ...]:
    return tuple(
        message_summary_from_row(row, radio_short_name=radio_short_name, now_ts=now_ts)
        for row in rows
    )


def default_visible_message_summaries(
    summaries: Iterable[MessageSummary],
) -> tuple[MessageSummary, ...]:
    """Return summaries that belong in default operational traffic views."""
    return tuple(summary for summary in summaries if summary.visible_by_default)


def message_summary_source_counts(
    summaries: Iterable[MessageSummary],
) -> Mapping[str, int]:
    counts: dict[str, int] = {}
    for summary in summaries:
        label = summary.source_label or message_source_label(summary.source_family)
        counts[label] = counts.get(label, 0) + 1
    return dict(sorted(counts.items()))


def filter_message_summaries(
    summaries: Sequence[MessageSummary],
    *,
    source_family: object = "",
    topic: object = "",
    callsign: object = "",
    group: object = "",
    state: object = "",
    grid: object = "",
    include_hidden: bool = False,
) -> tuple[MessageSummary, ...]:
    family = normalize_message_source_family(source_family) if str(source_family or "").strip() else ""
    topic_text = str(topic or "").strip().lower()
    call_text = _clean_call(callsign)
    group_text = _clean_target(group)
    state_text = str(state or "").strip().upper()
    grid_text = str(grid or "").strip().upper()
    out: list[MessageSummary] = []
    for summary in summaries:
        if not include_hidden and not summary.visible_by_default:
            continue
        if family and normalize_message_source_family(summary.source_family) != family:
            continue
        if topic_text and topic_text not in {str(t or "").strip().lower() for t in summary.topics}:
            continue
        if call_text and call_text not in {summary.from_call, summary.to_target}:
            continue
        if group_text and group_text != summary.group:
            continue
        hint = summary.map_hint
        if state_text and (hint is None or hint.state != state_text):
            continue
        if grid_text and (hint is None or not hint.grid.startswith(grid_text)):
            continue
        out.append(summary)
    return tuple(out)


def normalize_message_source_family(value: object) -> str:
    text = str(value or "").strip().lower()
    aliases = {
        "js8call": "js8",
        "js8": "js8",
        "js8spotter": "spotter",
        "fiospotter": "spotter",
        "spotter": "spotter",
        "varac": "varac",
        "varac_bbs": "bbs",
        "bbs_archive": "bbs",
        "bbs": "bbs",
        "flmsg": "flmsg",
        "flamp": "flamp",
        "fastlight": "flmsg",
        "sitrep": "sitrep",
        "commstat": "commstat",
        "commstat_rf": "commstat",
        "local": "local_report",
        "local_report": "local_report",
    }
    return aliases.get(text, text or "message")


def message_source_label(source_family: object) -> str:
    family = normalize_message_source_family(source_family)
    return {
        "js8": "JS8Call",
        "spotter": "FIOSpotter",
        "varac": "VarAC",
        "bbs": "BBS",
        "flmsg": "FLMsg",
        "flamp": "FLAmp",
        "sitrep": "SitRep",
        "commstat": "CommStat RF",
        "local_report": "Local Report",
    }.get(family, str(source_family or "Message").strip() or "Message")


def _actions_for_row(row: MessageSummaryRowLike, payload: object, map_hint: MessageMapHint) -> MessageActionValidity:
    family = normalize_message_source_family(getattr(row, "origin", ""))
    can_open_native = isinstance(payload, FileRecord) or family in {"varac", "js8", "spotter"}
    can_reply = family in {"js8", "spotter", "varac", "commstat", "local_report"}
    can_delete = family in {"js8", "spotter", "varac", "bbs", "flmsg", "flamp", "commstat", "sitrep"}
    can_map = not bool(map_hint.unavailable_reason)
    disabled = ""
    if not can_map:
        disabled = map_hint.unavailable_reason
    return MessageActionValidity(
        can_read=True,
        can_reply=can_reply,
        can_map=can_map,
        can_open_native=can_open_native,
        can_pin=True,
        can_delete=can_delete,
        disabled_reason=disabled,
    )


def _map_hint_from_payload(payload: object) -> MessageMapHint:
    state = _state_from_payload(payload)
    grid = _grid_from_payload(payload)
    if state or grid:
        precision = "grid" if grid else "state"
        return MessageMapHint(state=state, grid=grid, precision=precision)
    return MessageMapHint(unavailable_reason="No known state, grid, or coordinates.")


def _severity(row: MessageSummaryRowLike, payload: object, *, status: str, actionable: bool) -> str:
    alert = str(getattr(payload, "alert_color", "") or "").strip().lower()
    overall = str(getattr(payload, "overall_status", "") or "").strip().lower()
    flag_state = _int(getattr(payload, "flag_state", 0))
    if flag_state == 1 or alert == "red" or overall == "red" or status == "ALERT":
        return "urgent"
    if actionable or alert == "yellow" or overall == "yellow":
        return "important"
    if status == "NEW":
        return "watch"
    return "routine"


def _trust_label(row: MessageSummaryRowLike, payload: object, source_family: str) -> str:
    auth = str(getattr(row, "auth_state", "") or "").strip().lower()
    if auth == "valid" or bool(getattr(row, "auth_trusted", False)):
        return "Trusted"
    if auth in {"invalid", "error"}:
        return "Untrusted"
    if str(getattr(row, "expect_decision", "") or "").strip():
        return "Expected"
    if source_family in {"js8", "spotter", "commstat", "flmsg", "flamp"}:
        return "RF"
    if source_family == "varac":
        return "Local VarAC"
    if source_family == "bbs":
        return "BBS"
    return "Unknown"


def _confidence_label(payload: object) -> str:
    for attr in ("geo_confidence", "state_confidence"):
        value = str(getattr(payload, attr, "") or "").strip()
        if value:
            return value
    return ""


def _event_ts(row: MessageSummaryRowLike, payload: object) -> float:
    for attr in ("event_ts", "report_ts", "utc_ts", "ts"):
        value = _float(getattr(payload, attr, 0.0))
        if value > 0:
            return value
    value = _float(getattr(row, "rcv_ts", 0.0))
    return value


def _source_ref(row: MessageSummaryRowLike, payload: object) -> str:
    for attr in ("source_key", "report_key", "artifact_key", "guid", "source_path"):
        value = str(getattr(payload, attr, "") or "").strip()
        if value:
            return value
    if isinstance(payload, FileRecord):
        return str(payload.path)
    return str(getattr(row, "origin", "") or "").strip()


def _stable_id(row: MessageSummaryRowLike, payload: object, source_family: str) -> str:
    for attr in ("source_id", "msg_id", "spotter_id", "event_id", "artifact_id", "report_key", "artifact_key", "guid"):
        value = str(getattr(payload, attr, "") or "").strip()
        if value and value != "0":
            return f"{source_family}:{_source_ref(row, payload)}:{value}"
    if isinstance(payload, FileRecord):
        return f"{source_family}:{payload.path}:{float(payload.mtime or 0.0)}:{int(payload.size or 0)}"
    return f"{source_family}:{_source_ref(row, payload)}:{_float(getattr(row, 'rcv_ts', 0.0))}"


def _adapter_label(source_family: str, payload: object) -> str:
    js8_instance = str(getattr(payload, "js8_instance_id", "") or "").strip()
    if js8_instance:
        return js8_instance
    source = str(getattr(payload, "source", "") or "").strip()
    if source:
        return source
    return message_source_label(source_family)


def _source_metadata(payload: object) -> Mapping[str, object]:
    keys = (
        "source_key",
        "source_radio_id",
        "js8_instance_id",
        "source_path",
        "relay_via",
        "transport_label",
        "reach_label",
        "source_family_label",
        "source_count",
    )
    out = {key: getattr(payload, key) for key in keys if hasattr(payload, key)}
    if isinstance(payload, FileRecord):
        out.update({"path": str(payload.path), "mtime": payload.mtime, "size": payload.size})
    return out


def _subject_from_row(row: MessageSummaryRowLike, payload: object) -> str:
    for attr in ("subject", "title", "subtype_label"):
        value = str(getattr(payload, attr, "") or "").strip()
        if value:
            return _collapse(value)
    return _collapse(getattr(row, "title", ""))


def _summary_from_row(row: MessageSummaryRowLike, payload: object, subject: str) -> str:
    for attr in ("body_text", "remarks_text", "body", "decoded_text", "raw_text", "brevity_summary"):
        value = str(getattr(payload, attr, "") or "").strip()
        if value:
            return _collapse(value, limit=180)
    return _collapse(subject or getattr(row, "title", ""), limit=180)


def _group_from_payload_or_target(payload: object, target: str) -> str:
    for attr in ("report_group", "group", "operating_group"):
        value = _clean_target(getattr(payload, attr, ""))
        if value:
            return value
    return target if target and not _looks_like_callsign(target) else ""


def _state_from_payload(payload: object) -> str:
    for attr in ("state_code", "state"):
        value = str(getattr(payload, attr, "") or "").strip().upper()
        if value:
            return value
    intelligence = getattr(payload, "intelligence", None)
    value = str(getattr(intelligence, "state", "") or "").strip().upper()
    return value


def _grid_from_payload(payload: object) -> str:
    value = str(getattr(payload, "grid", "") or "").strip().upper()
    return value


def _radio_short_name_from_payload(payload: object) -> str:
    return str(getattr(payload, "source_radio_id", "") or "").strip()


def _freshness_label(ts: float, *, now_ts: float | None = None) -> str:
    age_days = _age_days(ts, now_ts=now_ts)
    if age_days is None:
        return ""
    if age_days < 1 / 24:
        return "fresh"
    if age_days <= 1:
        return "today"
    if age_days <= DEFAULT_TRAFFIC_RETENTION_DAYS:
        return f"{int(age_days)}d"
    return "older than 7d"


def _age_days(ts: float, *, now_ts: float | None = None) -> float | None:
    value = _float(ts)
    if value <= 0:
        return None
    if now_ts is None:
        now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
    return max(0.0, (float(now_ts) - value) / 86400.0)


def _collapse(value: object, *, limit: int = 120) -> str:
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def _clean_call(value: object) -> str:
    return str(value or "").strip().upper()


def _clean_target(value: object) -> str:
    text = str(value or "").strip().upper()
    while text.startswith("@"):
        text = text[1:].strip()
    return text.rstrip(">").strip()


def _looks_like_callsign(value: object) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return False
    import re

    return bool(re.fullmatch(r"[A-Z]{1,2}\d[A-Z0-9]{1,5}(?:/[A-Z0-9]{1,4})?", text))


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _int(value: object) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0
