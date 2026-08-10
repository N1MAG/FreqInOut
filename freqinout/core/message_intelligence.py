from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

from freqinout.core.js8_spotter_decode import parse_spotter_bracket_fields, summarize_spotter_form_text


TOPIC_TAXONOMY: tuple[str, ...] = (
    "Weather",
    "Fire",
    "Medical",
    "Power",
    "Water",
    "Fuel",
    "Food",
    "Travel/Roads",
    "Comms",
    "Security",
    "Shelter",
    "Logistics",
    "Infrastructure",
    "General Intel",
)


_TOPIC_PATTERNS: Mapping[str, tuple[str, ...]] = {
    "Weather": (
        "weather",
        "wx",
        "wefax",
        "wind",
        "rain",
        "snow",
        "storm",
        "temperature",
        "temp",
        "warning",
        "flood",
        "surge",
        "hurricane",
        "tornado",
    ),
    "Fire": ("fire", "wildfire", "evac", "smoke", "burn", "red flag"),
    "Medical": ("medical", "med", "injur", "hospital", "triage", "ems", "patient", "health"),
    "Power": ("power", "grid down", "outage", "generator", "battery", "electric"),
    "Water": ("water", "potable", "well", "sewer", "sanitation"),
    "Fuel": ("fuel", "gas", "diesel", "propane"),
    "Food": ("food", "supply", "supplies", "grocery", "ration"),
    "Travel/Roads": ("travel", "road", "route", "bridge", "traffic", "closed", "closure"),
    "Comms": ("comms", "communications", "radio", "repeater", "internet", "cell", "telecom"),
    "Security": ("security", "crime", "civil unrest", "threat", "riot", "looting", "violence"),
    "Shelter": ("shelter", "evacuation center", "refuge", "housing"),
    "Logistics": ("logistics", "transport", "delivery", "staging", "resource", "warehouse"),
    "Infrastructure": ("infrastructure", "bridge", "dam", "road", "rail", "utility", "water plant"),
    "General Intel": ("intel", "s2", "s-2", "sitrep", "statrep", "situation", "awareness", "osint"),
}


@dataclass(frozen=True)
class MessageIntelligence:
    source_type: str = ""
    form_name: str = ""
    from_call: str = ""
    to_call: str = ""
    subject: str = ""
    date_summary: str = ""
    state: str = ""
    grid: str = ""
    groups: tuple[str, ...] = ()
    body: str = ""
    topics: tuple[str, ...] = ()
    actionable: bool = False
    summary: str = ""
    confidence: float = 0.0
    metadata: Mapping[str, str] = field(default_factory=dict)


def normalize_topic_terms(text: object) -> tuple[str, ...]:
    haystack = f" {str(text or '').lower()} "
    found: list[str] = []
    for topic in TOPIC_TAXONOMY:
        patterns = _TOPIC_PATTERNS.get(topic, ())
        if any(_term_matches(haystack, term) for term in patterns):
            found.append(topic)
    return tuple(found)


def summarize_intelligence(info: MessageIntelligence) -> str:
    parts: list[str] = []
    if info.form_name:
        parts.append(info.form_name)
    if info.from_call or info.to_call:
        route = " -> ".join(part for part in (info.from_call, info.to_call) if part)
        if route:
            parts.append(route)
    if info.subject:
        parts.append(info.subject)
    if info.date_summary:
        parts.append(info.date_summary)
    summary = " | ".join(part for part in parts if part).strip()
    return summary or info.subject or info.form_name or info.body[:80].strip()


def analyze_spotter_text(
    text: object,
    *,
    form_name: object = "",
    from_call: object = "",
    to_call: object = "",
    source_type: str = "spotter",
) -> MessageIntelligence:
    raw = str(text or "").strip()
    fields = parse_spotter_bracket_fields(raw)
    subject = _first_nonempty(fields.get("NA"), fields.get("CM"), fields.get("DE"), fields.get("AV"), fields.get("NE"))
    state = str(fields.get("ST", "") or "").strip().upper()
    grid = str(fields.get("GR", "") or "").strip().upper()
    to_value = _clean_group_or_call(_first_nonempty(to_call, fields.get("TO")))
    from_value = _clean_call(_first_nonempty(from_call, fields.get("FR")))
    date_summary = _form_date_summary(fields.get("DA", ""))
    form_title = str(form_name or "").strip()
    topics = normalize_topic_terms(" ".join([raw, form_title, subject]))
    metadata = {str(k): str(v) for k, v in fields.items()}
    info = MessageIntelligence(
        source_type=source_type,
        form_name=form_title or _spotter_form_code(raw),
        from_call=from_value,
        to_call=to_value,
        subject=subject or summarize_spotter_form_text(raw, form_title=form_title),
        date_summary=date_summary,
        state=state,
        grid=grid,
        groups=_groups_from_values(to_value, raw),
        body=raw,
        topics=topics,
        actionable=bool(topics or state or grid or subject),
        confidence=0.82 if fields else 0.45,
        metadata=metadata,
    )
    return _with_summary(info)


def analyze_form_text(
    text: object,
    *,
    form_name: object = "",
    source_type: str = "form",
    path: object = None,
    fields: Mapping[str, str] | None = None,
) -> MessageIntelligence:
    raw = str(text or "").strip()
    parsed_fields = {
        str(k or "").strip(): str(v or "").strip()
        for k, v in (fields or {}).items()
        if str(v or "").strip() and not str(k or "").strip().startswith("_")
    }
    from_call = _clean_call(
        _first_nonempty(
            parsed_fields.get("from"),
            _extract_hdr_call(raw, ":hdr_fm:"),
            _field_after_label(raw, "From"),
            _call_from_filename(path),
        )
    )
    to_call = _clean_group_or_call(
        _first_nonempty(
            parsed_fields.get("to"),
            _extract_hdr_call(raw, ":hdr_to:"),
            _field_after_label(raw, "To"),
        )
    )
    subject = _first_nonempty(
        parsed_fields.get("subject"),
        _match_field(raw, r":sub:\s*(.*?)\s*(?=:)"),
        _message_subject_from_body(raw),
        _subject_from_filename(path),
    )
    date_summary = _form_date_summary(
        _first_nonempty(
            parsed_fields.get("date_summary"),
            _match_field(raw, r"\b(\d{6}[-_]\d{4}z?)\b"),
            _match_field(raw, r"\b(\d{8}[-_]\d{4,6}z?)\b"),
        )
    )
    form_title = str(form_name or parsed_fields.get("form_title", "") or "").strip()
    if not form_title:
        form_title = _infer_form_title(raw, path)
    state = _clean_state(_first_nonempty(parsed_fields.get("state"), _field_after_label(raw, "State")))
    grid = _clean_grid(_first_nonempty(parsed_fields.get("grid"), _field_after_label(raw, "Grid")))
    body = _first_nonempty(parsed_fields.get("body"), _message_body(raw), raw)
    topics = normalize_topic_terms(" ".join([str(path or ""), form_title, subject, body]))
    info = MessageIntelligence(
        source_type=source_type,
        form_name=form_title,
        from_call=from_call,
        to_call=to_call,
        subject=subject,
        date_summary=date_summary,
        state=state,
        grid=grid,
        groups=_groups_from_values(to_call, raw, str(path or "")),
        body=body,
        topics=topics,
        actionable=bool(topics or state or grid or subject),
        confidence=0.72 if (from_call or to_call or subject or form_title) else 0.35,
        metadata=dict(parsed_fields),
    )
    return _with_summary(info)


def analyze_commstat_fields(
    *,
    artifact_kind: object = "",
    title: object = "",
    body: object = "",
    from_call: object = "",
    target: object = "",
    report_group: object = "",
    state: object = "",
    grid: object = "",
    scope: object = "",
    status: object = "",
    alert_color: object = "",
    subtype: object = "",
    remarks: object = "",
    brevity_code: object = "",
    brevity_summary: object = "",
    transport: object = "",
    source_family: object = "CommStat",
    event_utc: object = "",
) -> MessageIntelligence:
    kind = _commstat_kind_label(artifact_kind)
    title_text = _first_nonempty(title, brevity_summary, remarks, body)
    body_text = _first_nonempty(body, remarks, brevity_summary, title)
    status_text = str(status or "").strip().upper()
    alert_text = str(alert_color or "").strip().upper()
    to_value = _clean_group_or_call(_first_nonempty(target, report_group))
    group_value = _clean_group_or_call(report_group)
    from_value = _clean_call(from_call)
    state_value = _clean_state(state)
    grid_value = _clean_grid(grid)
    combined = " ".join(
        part
        for part in (
            kind,
            title_text,
            body_text,
            remarks,
            brevity_code,
            brevity_summary,
            status_text,
            alert_text,
            subtype,
            scope,
            transport,
            source_family,
        )
        if str(part or "").strip()
    )
    topics = normalize_topic_terms(combined)
    if kind in {"CommStat Alert", "CommStat StatRep", "CommStat SitRep"} and "General Intel" not in topics:
        topics = tuple([*topics, "General Intel"])
    elevated = status_text not in {"", "INFO", "READ", "NEW", "GREEN", "OK", "NORMAL"} or alert_text in {
        "RED",
        "YELLOW",
        "ORANGE",
    }
    metadata = {
        "kind": kind,
        "status": status_text,
        "alert": alert_text,
        "scope": str(scope or "").strip(),
        "subtype": str(subtype or "").strip(),
        "transport": str(transport or "").strip(),
        "source": str(source_family or "").strip(),
    }
    info = MessageIntelligence(
        source_type="commstat",
        form_name=kind,
        from_call=from_value,
        to_call=to_value,
        subject=title_text[:120],
        date_summary=_form_date_summary(event_utc),
        state=state_value,
        grid=grid_value,
        groups=_groups_from_values(to_value, group_value, title_text, body_text),
        body=body_text,
        topics=topics,
        actionable=bool(elevated or topics or state_value or grid_value or title_text or body_text),
        confidence=0.78 if (from_value or to_value or title_text or body_text) else 0.5,
        metadata={k: v for k, v in metadata.items() if v},
    )
    return _with_summary(info)


def _with_summary(info: MessageIntelligence) -> MessageIntelligence:
    return MessageIntelligence(
        source_type=info.source_type,
        form_name=info.form_name,
        from_call=info.from_call,
        to_call=info.to_call,
        subject=info.subject,
        date_summary=info.date_summary,
        state=info.state,
        grid=info.grid,
        groups=info.groups,
        body=info.body,
        topics=info.topics,
        actionable=info.actionable,
        summary=summarize_intelligence(info),
        confidence=info.confidence,
        metadata=info.metadata,
    )


def _commstat_kind_label(value: object) -> str:
    text = str(value or "").strip().upper()
    if text in {"ALERT", "ALERTS"}:
        return "CommStat Alert"
    if text in {"STATREP", "STAT", "STATUS"}:
        return "CommStat StatRep"
    if text in {"SITREP", "SIT"}:
        return "CommStat SitRep"
    if text in {"CHECKIN", "CHECK-IN", "CHECK_IN"}:
        return "CommStat Check-In"
    if text in {"MESSAGE", "MSG", ""}:
        return "CommStat Message"
    return f"CommStat {text.title()}"


def _term_matches(haystack: str, term: str) -> bool:
    needle = str(term or "").strip().lower()
    if not needle:
        return False
    if re.fullmatch(r"[a-z0-9+-]{1,4}", needle):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", haystack))
    return needle in haystack


def _first_nonempty(*values: object) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return re.sub(r"\s+", " ", text)
    return ""


def _clean_call(value: object) -> str:
    text = str(value or "").strip().upper()
    match = re.search(r"\b[A-Z]{1,2}\d[A-Z0-9]{1,5}(?:/[A-Z0-9]{1,4})?\b", text)
    return match.group(0) if match else text


def _clean_group_or_call(value: object) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("@"):
        return re.sub(r"[^@A-Z0-9_-]+", "", text)
    return _clean_call(text)


def _clean_state(value: object) -> str:
    text = str(value or "").strip().upper()
    match = re.search(r"\b[A-Z]{2}\b", text)
    return match.group(0) if match else ""


def _clean_grid(value: object) -> str:
    text = str(value or "").strip().upper()
    match = re.search(r"\b[A-R]{2}[0-9]{2}(?:[A-X]{2})?\b", text)
    return match.group(0) if match else ""


def _groups_from_values(*values: object) -> tuple[str, ...]:
    groups: list[str] = []
    for value in values:
        for found in re.findall(r"@[A-Z0-9_-]{2,}", str(value or "").upper()):
            if found not in groups:
                groups.append(found)
        for found in re.findall(r"\b(MAGNET|GHOSTNET|AMRRON|MR[0-9A-Z]{2,}|S2(?:UNDERGROUND)?)\b", str(value or "").upper()):
            clean = found if found.startswith("@") else f"@{found}"
            if clean not in groups:
                groups.append(clean)
    return tuple(groups)


def _spotter_form_code(text: str) -> str:
    match = re.search(r"\bF![0-9]{3}[A-Z]?\b", text or "", flags=re.IGNORECASE)
    return match.group(0).upper() if match else ""


def _form_date_summary(value: object) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    match = re.search(r"\b(\d{6})[-_]?(\d{4})z?\b", txt, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)}-{match.group(2)}z"
    match = re.search(r"\b(\d{8})[-_]?(\d{4,6})z?\b", txt, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)}-{match.group(2)[:4]}z"
    return txt[:24]


def _extract_hdr_call(text: str, marker: str) -> str:
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for idx, line in enumerate(lines):
        if line.lower().startswith(str(marker or "").lower()):
            for next_line in lines[idx + 1 :]:
                value = _clean_group_or_call(next_line)
                if value:
                    return value
            break
    return ""


def _match_field(text: str, pattern: str) -> str:
    match = re.search(pattern, text or "", flags=re.IGNORECASE | re.DOTALL)
    return re.sub(r"\s+", " ", match.group(1).strip()) if match else ""


def _field_after_label(text: str, label: str) -> str:
    lines = [line.strip() for line in str(text or "").splitlines()]
    label_key = str(label or "").strip().lower()
    for idx, line in enumerate(lines):
        if line.lower().rstrip(":") == label_key:
            for next_line in lines[idx + 1 :]:
                if next_line:
                    return next_line
    return ""


def _block_after_label(text: str, label: str) -> str:
    lines = [line.rstrip() for line in str(text or "").splitlines()]
    label_key = str(label or "").strip().lower()
    start = -1
    for idx, line in enumerate(lines):
        if line.strip().lower().rstrip(":") == label_key:
            start = idx + 1
            break
    if start < 0:
        return ""
    collected: list[str] = []
    for line in lines[start:]:
        stripped = line.strip()
        if collected and _looks_like_form_label(stripped):
            break
        if stripped or collected:
            collected.append(stripped)
    return re.sub(r"\s+", " ", "\n".join(collected).strip())


def _looks_like_form_label(value: str) -> bool:
    text = str(value or "").strip().lower().rstrip(":")
    if not text or len(text) > 48:
        return False
    return text in {
        "date/time/msg id",
        "date time msg id",
        "to",
        "from",
        "msg precedence",
        "precedence",
        "region",
        "subject",
        "message",
        "remarks",
        "comments",
        "incident",
        "state",
        "grid",
    }


def _message_subject_from_body(text: str) -> str:
    return _first_nonempty(_field_after_label(text, "Subject"), _field_after_label(text, "Incident"))


def _message_body(text: str) -> str:
    for label in ("Message", "Comments", "Remarks", "Body"):
        value = _block_after_label(text, label)
        if value:
            return value
    return ""


def _infer_form_title(text: str, path: object = None) -> str:
    for line in str(text or "").splitlines():
        clean = re.sub(r"\s+", " ", line).strip()
        if clean and ("form" in clean.lower() or "sitrep" in clean.lower() or "statrep" in clean.lower()):
            return clean[:80]
    return _subject_from_filename(path)


def _call_from_filename(path: object) -> str:
    try:
        stem = Path(str(path)).stem
    except Exception:
        stem = str(path or "")
    for token in re.split(r"[-_\s]+", stem):
        call = _clean_call(token)
        if call:
            return call
    return ""


def _subject_from_filename(path: object) -> str:
    try:
        stem = Path(str(path)).stem
    except Exception:
        stem = str(path or "")
    if not stem:
        return ""
    tokens = [token for token in re.split(r"[-_]", stem) if token]
    if len(tokens) <= 1:
        return stem
    start = 0
    for idx, token in enumerate(tokens):
        if re.fullmatch(r"\d{6,8}", token) or re.fullmatch(r"\d{4,6}z", token, flags=re.IGNORECASE):
            start = idx + 1
            break
    return " ".join(tokens[start:] or tokens[-1:]).strip()
