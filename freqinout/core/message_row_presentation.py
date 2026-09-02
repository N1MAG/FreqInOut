from __future__ import annotations

from dataclasses import dataclass
import datetime
import re
from typing import Callable, Mapping

from freqinout.core.commstat_artifacts import artifact_kind_label
from freqinout.core.js8_spotter_decode import parse_spotter_bracket_fields, summarize_spotter_form_text
from freqinout.core.message_intelligence import MessageIntelligence, analyze_commstat_fields, analyze_spotter_text


@dataclass(frozen=True)
class MessageRowPresentation:
    msg_type: str
    status: str
    from_call: str
    to_call: str
    rcv_ts: float
    title: str
    origin: str
    topics: tuple[str, ...] = ()
    actionable: bool = False
    display_type: str = ""
    search_detail: str = ""
    intelligence: MessageIntelligence | None = None


def message_row_search_text(
    msg_type: object,
    status: object,
    from_call: object,
    to_call: object,
    rcv_display: object,
    detail: object,
) -> str:
    return " ".join(
        [
            str(msg_type or ""),
            str(status or ""),
            str(from_call or ""),
            str(to_call or ""),
            str(rcv_display or ""),
            str(detail or ""),
        ]
    ).lower()


def message_display_profile_headers(profile: object) -> tuple[str, tuple[str, ...]]:
    profile_text = str(profile or "triage").strip().lower()
    if profile_text not in {"triage", "field_report", "intel_report", "form_message"}:
        profile_text = "triage"
    if profile_text == "field_report":
        return profile_text, ("", "MCF", "Status", "From", "To", "State / Grid", "Age", "")
    if profile_text == "intel_report":
        return profile_text, ("", "Kind", "Status", "From", "To", "State / Grid", "Age", "")
    if profile_text == "form_message":
        return profile_text, ("", "Message", "Type", "Status", "From", "To", "Age", "")
    return profile_text, ("", "Type", "Status", "From", "To", "Age", "Message", "")


def message_display_profile_for_focus_type(focus: object, type_sel: object) -> str:
    focus_text = str(focus or "all").strip().lower()
    if focus_text == "spotter":
        return "field_report"
    if focus_text == "forms":
        return "form_message"
    if focus_text == "commstat":
        return "intel_report"
    return message_display_profile_for_type(type_sel)


def message_display_profile_for_type(type_sel: object) -> str:
    text = str(type_sel or "").strip()
    if not text or text == "MSG Type...":
        return "triage"
    if text == "Spotter" or re.match(r"^F![0-9]{3}[A-Z]?$", text):
        return "field_report"
    if text == "SitRep" or text.startswith("SitRep/"):
        return "intel_report"
    if text == "CommStat":
        return "intel_report"
    if text == "FLMSG/FLAMP" or text.upper() in {"FLMSG", "FLAMP"}:
        return "form_message"
    return "triage"


def relative_age_label(ts: object, *, now_ts: float | None = None) -> str:
    try:
        now = float(now_ts) if now_ts is not None else datetime.datetime.now(datetime.timezone.utc).timestamp()
        age = max(0.0, now - float(ts or 0.0))
    except Exception:
        return ""
    if age < 60:
        return "now"
    minutes = int(age // 60)
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    rem_minutes = minutes % 60
    if hours < 24:
        return f"{hours}:{rem_minutes:02d} h"
    days = hours // 24
    if days < 90:
        return f"{days} day" if days == 1 else f"{days} days"
    months = max(1, days // 30)
    return f"{months} mo"


def spotter_mcf_display_label(code: object, title: object = "") -> str:
    code_text = str(code or "").strip().upper()
    if re.fullmatch(r"[0-9]{3}[A-Z]?", code_text):
        code_text = f"F!{code_text}"
    if not re.fullmatch(r"F![0-9]{3}[A-Z]?", code_text):
        code_text = ""

    first_part = str(title or "").strip().split("|", 1)[0].strip()
    first_part = re.sub(r"^MCF\s*[0-9]{3}[A-Z]?\s*[-:]?\s*", "", first_part, flags=re.IGNORECASE).strip()
    first_part = re.sub(r"^F![0-9]{3}[A-Z]?\s*[-:]?\s*", "", first_part, flags=re.IGNORECASE).strip()
    first_part = re.sub(
        r"\b(Status\s+Report|Situation\s+Report|Field\s+Report|Report|Form)\b",
        "",
        first_part,
        flags=re.IGNORECASE,
    ).strip()
    first_part = re.sub(r"\s+", " ", first_part).strip(" -:")
    if first_part and not _looks_like_route_summary_part(first_part) and not _looks_like_date_summary_part(first_part):
        return f"{first_part} | {code_text}" if code_text else first_part
    return code_text or str(code or "").strip()


def field_report_form_label(row: object) -> str:
    payload = getattr(row, "payload", None)
    kind = payload.__class__.__name__ if payload is not None else ""
    if kind == "SitrepMessage":
        return str(getattr(payload, "subtype_label", "") or getattr(payload, "subtype", "") or getattr(row, "msg_type", "") or "")
    if kind == "SpotterMessage":
        return spotter_mcf_display_label(getattr(row, "msg_type", ""), getattr(row, "title", ""))
    if kind == "CommStatArtifact":
        return "CommStat"
    return str(getattr(row, "msg_type", "") or "")


def field_report_status_label(row: object) -> str:
    payload = getattr(row, "payload", None)
    kind = payload.__class__.__name__ if payload is not None else ""
    if kind == "SitrepMessage":
        overall = str(getattr(payload, "overall_status", "") or "").strip()
        return overall.upper() if overall else str(getattr(row, "status", "") or "")
    if kind == "CommStatArtifact":
        alert = str(getattr(payload, "alert_color", "") or "").strip().upper()
        if alert:
            return alert
    return str(getattr(row, "status", "") or "")


def field_report_group_label(row: object, display_to: Callable[[object, object | None], str] | None = None) -> str:
    payload = getattr(row, "payload", None)
    kind = payload.__class__.__name__ if payload is not None else ""
    value = None
    if kind == "SitrepMessage":
        value = getattr(payload, "report_group", "") or getattr(payload, "target", "") or getattr(row, "to_call", "")
    elif kind == "CommStatArtifact":
        value = getattr(payload, "report_group", "") or getattr(payload, "target", "") or getattr(row, "to_call", "")
    if callable(display_to):
        return display_to(row, value)
    return strip_group_marker(getattr(row, "to_call", "") if value is None else value)


def field_report_area_label(row: object) -> str:
    payload = getattr(row, "payload", None)
    kind = payload.__class__.__name__ if payload is not None else ""
    if kind == "SitrepMessage":
        return _state_grid_scope_area(
            getattr(payload, "state_code", ""),
            getattr(payload, "grid", ""),
            getattr(payload, "scope", ""),
        )
    if kind == "SpotterMessage":
        fields = parse_spotter_bracket_fields(getattr(payload, "raw_text", ""))
        return _state_grid_scope_area(fields.get("ST", ""), fields.get("GR", ""), fields.get("CC", "") or fields.get("CO", ""))
    if kind == "CommStatArtifact":
        return _state_grid_scope_area(
            getattr(payload, "state_code", ""),
            getattr(payload, "grid", ""),
            getattr(payload, "scope", ""),
        )
    return ""


def js8_message_row_presentation(
    msg: object,
    *,
    form_title_lookup: Mapping[str, str] | Callable[[str], str] | None = None,
    alert_predicate: Callable[[str], bool] | None = None,
) -> MessageRowPresentation:
    raw_type = str(getattr(msg, "msg_type", "") or "")
    msg_type = raw_type if raw_type.startswith("F!") else "JS8 MSG"
    status = "READ" if str(getattr(msg, "state", "") or "").upper() == "READ" else "NEW"
    if status != "READ" and callable(alert_predicate) and alert_predicate(msg_type):
        status = "ALERT"
    title = ""
    if raw_type.startswith("F!"):
        title = _lookup_form_title(form_title_lookup, raw_type[2:].strip())
    if not title:
        title = str(getattr(msg, "decoded_text", "") or getattr(msg, "raw_text", "") or "").strip()
    title = _table_title(title)
    return MessageRowPresentation(
        msg_type=msg_type,
        status=status,
        from_call=_clean_call(getattr(msg, "from_call", "")),
        to_call=strip_group_marker(getattr(msg, "to_call", "")),
        rcv_ts=float(getattr(msg, "utc_ts", 0.0) or 0.0),
        title=title,
        origin="js8",
        search_detail=title,
    )


def spotter_message_row_presentation(
    msg: object,
    *,
    form_title_lookup: Mapping[str, str] | Callable[[str], str] | None = None,
    alert_predicate: Callable[[str], bool] | None = None,
) -> MessageRowPresentation:
    msg_type = str(getattr(msg, "msg_type", "") or "F!")
    status = "READ" if str(getattr(msg, "state", "") or "").upper() == "READ" else "NEW"
    if status != "READ" and callable(alert_predicate) and alert_predicate(msg_type):
        status = "ALERT"
    title = ""
    if msg_type.startswith("F!"):
        title = _lookup_form_title(form_title_lookup, msg_type[2:].strip())
    raw_text = str(getattr(msg, "raw_text", "") or "")
    intelligence = analyze_spotter_text(
        raw_text,
        form_name=title or msg_type,
        from_call=getattr(msg, "from_call", ""),
        to_call=getattr(msg, "to_call", ""),
    )
    title = (
        intelligence.summary
        or summarize_spotter_form_text(raw_text, form_title=title)
        or title
        or str(getattr(msg, "decoded_text", "") or raw_text).strip()
    )
    title = _table_title(strip_group_markers_in_display_text(title))
    search_detail = " ".join([title, " ".join(intelligence.topics), intelligence.state, intelligence.grid])
    return MessageRowPresentation(
        msg_type=msg_type,
        status=status,
        from_call=_clean_call(getattr(msg, "from_call", "")),
        to_call=strip_group_marker(getattr(msg, "to_call", "")),
        rcv_ts=float(getattr(msg, "utc_ts", 0.0) or 0.0),
        title=title,
        origin="spotter",
        topics=tuple(intelligence.topics),
        actionable=bool(intelligence.actionable),
        search_detail=search_detail,
        intelligence=intelligence,
    )


def varac_message_row_presentation(msg: object) -> MessageRowPresentation:
    raw_kind = str(getattr(msg, "msg_type", "") or "")
    status = "NEW" if (int(getattr(msg, "read_status", 0) or 0) == 0 and raw_kind.upper() != "QSO") else "READ"
    if raw_kind.upper() == "VMAIL":
        title_base = str(getattr(msg, "subject", "") or "").strip()
    else:
        title_base = str(getattr(msg, "subject", "") or getattr(msg, "body", "") or "").strip()
    title = f"{raw_kind}: {title_base}" if title_base else (raw_kind or "VarAC")
    title = _table_title(title)
    return MessageRowPresentation(
        msg_type="VarAC",
        status=status,
        from_call=_clean_call(getattr(msg, "from_call", "")),
        to_call=strip_group_marker(getattr(msg, "to_call", "")),
        rcv_ts=float(getattr(msg, "ts", 0.0) or 0.0),
        title=title,
        origin="varac",
        search_detail=title,
    )


def sitrep_message_row_presentation(msg: object) -> MessageRowPresentation:
    overall = str(getattr(msg, "overall_status", "") or "").strip().lower()
    scope = str(getattr(msg, "scope", "") or "").strip()
    title_parts = [str(getattr(msg, "subtype_label", "") or getattr(msg, "subtype", "") or "").strip()]
    if scope:
        title_parts.append(scope)
    if overall:
        title_parts.append(overall.upper())
    title = _table_title(" | ".join([p for p in title_parts if p]) or "SitRep")
    detail = " ".join(
        part
        for part in (
            title,
            getattr(msg, "subtype_label", ""),
            getattr(msg, "source_family_label", ""),
            getattr(msg, "transport_label", ""),
            getattr(msg, "report_group", ""),
            getattr(msg, "state_code", ""),
            getattr(msg, "remarks_text", ""),
            getattr(msg, "brevity_code", ""),
            getattr(msg, "brevity_summary", ""),
        )
        if str(part or "").strip()
    )
    return MessageRowPresentation(
        msg_type="SitRep",
        status="INFO",
        from_call=_clean_call(getattr(msg, "from_call", "")),
        to_call=strip_group_marker(getattr(msg, "target", "")),
        rcv_ts=float(getattr(msg, "event_ts", 0.0) or 0.0),
        title=title,
        origin="sitrep",
        search_detail=detail,
    )


def commstat_message_row_presentation(msg: object) -> MessageRowPresentation:
    status = str(getattr(msg, "status_label", "") or "INFO").strip().upper() or "INFO"
    intelligence = analyze_commstat_fields(
        artifact_kind=getattr(msg, "artifact_kind", ""),
        title=getattr(msg, "title", ""),
        body=getattr(msg, "body_text", ""),
        from_call=getattr(msg, "from_call", ""),
        target=getattr(msg, "target", ""),
        report_group=getattr(msg, "report_group", ""),
        state=getattr(msg, "state_code", ""),
        grid=getattr(msg, "grid", ""),
        scope=getattr(msg, "scope", ""),
        status=getattr(msg, "status_label", ""),
        alert_color=getattr(msg, "alert_color", ""),
        subtype=getattr(msg, "subtype", ""),
        remarks=getattr(msg, "remarks_text", ""),
        transport=getattr(msg, "transport_label", ""),
        reach=getattr(msg, "reach_label", ""),
        source_family=getattr(msg, "source_family_label", ""),
        event_utc=getattr(msg, "event_ts_utc", ""),
    )
    msg_type = intelligence.form_name or artifact_kind_label(getattr(msg, "artifact_kind", ""))
    title = _table_title(
        strip_group_markers_in_display_text(intelligence.summary or str(getattr(msg, "title", "") or "").strip() or msg_type)
    )
    detail = " ".join(
        part
        for part in (
            title,
            " ".join(intelligence.topics),
            intelligence.state,
            intelligence.grid,
            getattr(msg, "report_group", ""),
            getattr(msg, "transport_label", ""),
            getattr(msg, "reach_label", ""),
            getattr(msg, "source_family_label", ""),
            getattr(msg, "body_text", ""),
            getattr(msg, "remarks_text", ""),
            getattr(msg, "alert_color", ""),
            getattr(msg, "status_label", ""),
            getattr(msg, "grid", ""),
            getattr(msg, "state_code", ""),
        )
        if str(part or "").strip()
    )
    return MessageRowPresentation(
        msg_type=msg_type,
        status=status,
        from_call=_clean_call(getattr(msg, "from_call", "")),
        to_call=intelligence.to_call or strip_group_marker(getattr(msg, "target", "")),
        rcv_ts=float(getattr(msg, "event_ts", 0.0) or 0.0),
        title=title,
        origin="commstat",
        topics=tuple(intelligence.topics),
        actionable=bool(intelligence.actionable),
        search_detail=detail,
        intelligence=intelligence,
    )


def observation_message_row_presentation(observation: object) -> MessageRowPresentation:
    source_family = str(getattr(observation, "source_family", "") or "").strip().lower()
    source_label = {
        "meshcore": "MeshCore",
        "meshtastic": "Meshtastic",
        "mesh_client": "Meshtastic",
        "local_mesh": "Mesh",
    }.get(source_family, source_family.title() if source_family else "Message")
    received = str(getattr(observation, "received_utc", "") or getattr(observation, "event_utc", "") or "").strip()
    rcv_ts = _parse_observation_ts(received)
    topics = tuple(str(v or "").strip() for v in (getattr(observation, "observed_topics", ()) or ()) if str(v or "").strip())
    summary = str(getattr(observation, "summary", "") or "").strip()
    subject = str(getattr(observation, "subject", "") or "").strip()
    title = _table_title(subject or summary or source_label)
    status_raw = str(getattr(observation, "status", "") or "").strip().upper()
    status = status_raw or ("NEW" if bool(getattr(observation, "operator_attention", False)) else "INFO")
    provenance = getattr(observation, "provenance", {}) or {}
    channel_policy = provenance.get("channel_policy", {}) if isinstance(provenance, Mapping) else {}
    if not isinstance(channel_policy, Mapping):
        channel_policy = {}
    channel_name = str(channel_policy.get("channel_name", "") or "").strip()
    key_status = str(channel_policy.get("key_status", "") or "").strip()
    routing = provenance.get("routing", {}) if isinstance(provenance, Mapping) else {}
    if not isinstance(routing, Mapping):
        routing = {}
    hop_count = routing.get("hop_count", "")
    route_type = str(routing.get("route_type", "") or "").strip()
    groups = tuple(str(v or "").strip() for v in (getattr(observation, "groups", ()) or ()) if str(v or "").strip())
    search_detail = " ".join(
        part
        for part in (
            source_label,
            channel_name,
            key_status,
            route_type,
            f"{hop_count} hop" if hop_count not in ("", None) else "",
            " ".join(groups),
            " ".join(topics),
            str(getattr(observation, "state", "") or ""),
            str(getattr(observation, "grid", "") or ""),
            subject,
            summary,
        )
        if str(part or "").strip()
    )
    return MessageRowPresentation(
        msg_type=source_label,
        status=status,
        from_call=_clean_call(getattr(observation, "from_call", "")),
        to_call=_observation_display_target(observation, channel_policy=channel_policy),
        rcv_ts=rcv_ts,
        title=title,
        origin=source_family or "message",
        topics=topics,
        actionable=bool(getattr(observation, "operator_attention", False)),
        display_type=source_label,
        search_detail=search_detail,
    )


def _observation_display_target(observation: object, *, channel_policy: Mapping[str, object] | None = None) -> str:
    raw_target = strip_group_marker(getattr(observation, "to_target", ""))
    source_family = str(getattr(observation, "source_family", "") or "").strip().lower()
    if source_family not in {"meshcore", "meshtastic"}:
        return raw_target
    policy = channel_policy if isinstance(channel_policy, Mapping) else {}
    channel_name = strip_group_marker(policy.get("channel_name", ""))
    channel_id = strip_group_marker(policy.get("channel_id", ""))
    groups = tuple(str(v or "").strip() for v in (getattr(observation, "groups", ()) or ()) if str(v or "").strip())
    target_key = raw_target.strip().lower()
    if target_key in {"", "channel", "channels", "public"}:
        return channel_name or (groups[0] if groups else "") or raw_target or "Public"
    if channel_id and target_key == channel_id.strip().lower():
        return channel_name or raw_target
    return raw_target


def strip_group_marker(value: object) -> str:
    text = str(value or "").strip()
    while text.startswith("@"):
        text = text[1:].strip()
    return text


def strip_group_markers_in_display_text(value: object) -> str:
    text = str(value or "")
    return re.sub(r"(?<!\w)@([A-Za-z0-9_-]+)", r"\1", text)


def _state_grid_scope_area(state: object, grid: object, scope: object = "") -> str:
    state_text = str(state or "").strip().upper()
    grid_text = str(grid or "").strip().upper()
    scope_text = str(scope or "").strip()
    if state_text and grid_text:
        return f"{state_text} / {grid_text}"
    if state_text and scope_text:
        return f"{state_text} / {scope_text}"
    return grid_text or state_text or scope_text


def _looks_like_route_summary_part(value: object) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return False
    call = r"[A-Z]{1,2}\d[A-Z0-9]{1,5}(?:/[A-Z0-9]{1,4})?"
    target = rf"(?:{call}|@[A-Z0-9_-]{{2,}}|[A-Z0-9_-]{{2,}})"
    return bool(re.fullmatch(rf"{call}(?:\s*->\s*{target})?", text))


def _looks_like_date_summary_part(value: object) -> bool:
    text = str(value or "").strip()
    return bool(re.fullmatch(r"\d{6,8}[-_]\d{3,6}z?", text, flags=re.IGNORECASE))


def _lookup_form_title(form_title_lookup: Mapping[str, str] | Callable[[str], str] | None, form_id: str) -> str:
    key = str(form_id or "").strip()
    if not key:
        return ""
    if callable(form_title_lookup):
        return str(form_title_lookup(key) or "").strip()
    if isinstance(form_title_lookup, Mapping):
        return str(form_title_lookup.get(key, "") or "").strip()
    return ""


def _clean_call(value: object) -> str:
    return str(value or "").strip().upper()


def _parse_observation_ts(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(normalized).timestamp()
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d-%H%M%SZ", "%y%m%d-%H%MZ"):
        try:
            dt = datetime.datetime.strptime(text, fmt).replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception:
            continue
    return 0.0


def _table_title(value: object, *, limit: int = 60) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text
