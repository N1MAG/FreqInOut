from __future__ import annotations

import re
from typing import Mapping

from freqinout.core.js8_spotter_forms import normalize_form_code


FIELD_LABELS: Mapping[str, str] = {
    "TO": "To",
    "FR": "From",
    "ST": "State",
    "CC": "County / Area",
    "GR": "Grid",
    "NA": "Name / Notes",
    "DA": "Date",
    "OP": "Operator",
    "PH": "Phone",
    "EM": "Email",
    "AD": "Address",
    "CI": "City",
    "CO": "County",
    "ZI": "ZIP",
    "CM": "Comment",
    "DE": "Details",
    "LO": "Location",
    "SC": "Scope",
    "SE": "Severity",
    "NE": "Needs",
    "AV": "Available",
    "WX": "Weather",
    "WI": "Wind",
    "TE": "Temperature",
    "PR": "Priority",
}


def parse_spotter_bracket_fields(text: object) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in re.findall(r"\b([A-Z0-9]{2})\[(.*?)\]", str(text or ""), flags=re.IGNORECASE):
        clean_key = str(key or "").strip().upper()
        clean_value = re.sub(r"\s+", " ", str(value or "").strip())
        if clean_key and clean_value:
            out[clean_key] = clean_value
    return out


def split_spotter_form_text(text: object) -> tuple[str, str, str, str]:
    raw = str(text or "").strip()
    parts = raw.split()
    if not parts:
        return "", "", "", ""
    form_code = normalize_form_code(parts[0])
    if not form_code:
        return "", "", raw, ""
    response = parts[1].strip() if len(parts) > 1 and "[" not in parts[1] else ""
    start = 2 if response else 1
    remainder = " ".join(parts[start:]).strip()
    token_match = re.search(r"(#[A-Z0-9]{3,})\b", remainder, flags=re.IGNORECASE)
    token = token_match.group(1).upper() if token_match else ""
    if token:
        remainder = re.sub(r"\s*#[A-Z0-9]{3,}\b\s*", " ", remainder, flags=re.IGNORECASE).strip()
    return form_code, response, remainder, token


def summarize_spotter_form_text(text: object, *, form_title: object = "") -> str:
    form_code, response, remainder, token = split_spotter_form_text(text)
    fields = parse_spotter_bracket_fields(remainder)
    title = str(form_title or "").strip()
    subject = fields.get("NA") or fields.get("CM") or fields.get("DE") or fields.get("AV") or fields.get("NE") or title
    place = " ".join(part for part in (fields.get("ST", ""), fields.get("CC", ""), fields.get("GR", "")) if part).strip()
    parts = [part for part in (place, subject) if part]
    summary = " - ".join(parts).strip()
    if not summary:
        summary = title or form_code or str(text or "").strip()
    if token and token not in summary:
        summary = f"{summary} ({token})" if summary else token
    return summary


def decode_spotter_form_text(text: object, *, form_title: object = "") -> str:
    raw = str(text or "").strip()
    form_code, response, remainder, token = split_spotter_form_text(raw)
    if not form_code:
        return raw
    fields = parse_spotter_bracket_fields(remainder)
    title = str(form_title or "").strip()
    lines: list[str] = []
    heading = " ".join(part for part in (form_code, title) if part).strip()
    if heading:
        lines.append(heading)
    if response:
        lines.append(f"Response Code: {response}")
    if token:
        lines.append(f"Spotter Token: {token}")
    if fields:
        lines.append("")
        preferred_order = (
            "TO",
            "FR",
            "ST",
            "CC",
            "CO",
            "CI",
            "GR",
            "LO",
            "SC",
            "SE",
            "NA",
            "CM",
            "DE",
            "NE",
            "AV",
            "WX",
            "WI",
            "TE",
            "DA",
        )
        emitted: set[str] = set()
        for key in preferred_order:
            if key in fields:
                lines.append(f"{FIELD_LABELS.get(key, key)}: {fields[key]}")
                emitted.add(key)
        for key in sorted(k for k in fields if k not in emitted):
            lines.append(f"{FIELD_LABELS.get(key, key)}: {fields[key]}")
    leftover = re.sub(r"\b[A-Z0-9]{2}\[.*?\]", " ", remainder, flags=re.IGNORECASE)
    leftover = re.sub(r"\s+", " ", leftover).strip()
    if leftover:
        lines.extend(["", "Unparsed Text:", leftover])
    return "\n".join(lines).strip() or raw
