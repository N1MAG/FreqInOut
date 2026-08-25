from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass


NO_REPORT_PLACEHOLDERS = {
    "",
    "--",
    "n/a",
    "na",
    "none",
    "no report",
    "no reports",
    "not observed",
    "not reported",
    "not_reported",
    "unknown / no report",
}

NO_REPORT_STATUS_FIELDS = {
    "civil unrest",
    "communications",
    "comms",
    "crime",
    "food",
    "fuel",
    "internet",
    "medical",
    "overall",
    "political",
    "power",
    "security",
    "shelter",
    "status",
    "travel",
    "travel roads",
    "water",
}

_NO_REPORT_STATUS_LINE_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9 /_-]{0,48})\s*[:=]\s*(.*?)\s*$")


def normalize_placeholder_text(value: object) -> str:
    return re.sub(r"[\s/_-]+", " ", str(value or "").strip().casefold()).strip()


_NO_REPORT_PLACEHOLDERS_NORMALIZED = {normalize_placeholder_text(value) for value in NO_REPORT_PLACEHOLDERS}


def is_no_report_placeholder(value: object) -> bool:
    text = str(value or "").strip().casefold()
    return text in NO_REPORT_PLACEHOLDERS or normalize_placeholder_text(text) in _NO_REPORT_PLACEHOLDERS_NORMALIZED


def is_no_report_status_line(line: str) -> bool:
    match = _NO_REPORT_STATUS_LINE_RE.match(line)
    if not match:
        return False
    label, value = match.groups()
    return normalize_placeholder_text(label) in NO_REPORT_STATUS_FIELDS and is_no_report_placeholder(value)


def strip_no_report_status_lines(text: object) -> str:
    lines = str(text or "").splitlines()
    kept = [line.strip() for line in lines if line.strip() and not is_no_report_status_line(line)]
    return "\n".join(kept).strip()


def searchable_text_values(value: object, *, depth: int = 0) -> tuple[str, ...]:
    if value is None or depth > 4:
        return ()
    if isinstance(value, (str, int, float, bool)):
        text = strip_no_report_status_lines(str(value).strip())
        if not text or is_no_report_placeholder(text):
            return ()
        return (text,)
    if isinstance(value, Mapping):
        parts: list[str] = []
        for key, item in value.items():
            item_parts = searchable_text_values(item, depth=depth + 1)
            if isinstance(key, str) and item_parts:
                parts.append(str(key))
            parts.extend(item_parts)
        return tuple(parts)
    if is_dataclass(value) and not isinstance(value, type):
        parts: list[str] = []
        for field in fields(value):
            item_parts = searchable_text_values(getattr(value, field.name, None), depth=depth + 1)
            if item_parts:
                parts.append(str(field.name))
            parts.extend(item_parts)
        return tuple(parts)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        parts: list[str] = []
        for item in value:
            parts.extend(searchable_text_values(item, depth=depth + 1))
        return tuple(parts)
    text = str(value).strip()
    if is_no_report_placeholder(text):
        return ()
    return (text,)
