from __future__ import annotations

import csv
import datetime
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, TextIO

from freqinout.core.group_utils import normalize_group_name


HEADER_ALIASES: Dict[str, tuple[str, ...]] = {
    "timezone": ("timezone", "time zone", "time_zone", "tz"),
    "region": ("region", "area", "child group", "child_group", "net", "subgroup", "sub group"),
    "callsign": ("callsign", "call sign", "call", "station", "station call", "operator_callsign"),
    "name": ("name", "operator", "operator name", "operator_name", "full name", "fullname"),
    "role": ("role", "group role", "group_role", "net role", "net_role"),
    "tier": ("tier", "level", "rank"),
    "state": ("state/province", "state", "province", "st", "qth state"),
    "grid": ("grid6", "grid", "locator", "maidenhead", "maidenhead grid"),
    "groups": ("groups", "group", "operating groups", "operating_group", "operating group"),
    "group1": ("group1", "group 1"),
    "group2": ("group2", "group 2"),
    "group3": ("group3", "group 3"),
    "trusted": ("trusted", "trust"),
}

IGNORED_HEADERS = {"tg handle", "tghandle", "alt contact", "altcontact"}
GROUP_ROLE_ALIASES = {"ALT-HUB": "HUB-ALT"}
CALLSIGN_RE = re.compile(r"^[A-Z0-9]{1,3}[0-9][A-Z0-9]{1,4}(?:/[A-Z0-9]{1,4})?$")


@dataclass(frozen=True)
class RosterImportResult:
    entries: List[Dict[str, object]]
    parent_group: str
    child_groups: List[str]
    imported: int
    skipped: int
    detected_headers: Dict[str, str]
    source_headers: List[str]


def _header_key(value: object) -> str:
    return re.sub(r"[\s_\-/]+", " ", str(value or "").strip().lower()).strip()


def detect_roster_headers(fieldnames: Sequence[str] | None) -> Dict[str, str]:
    fields = [str(field or "").strip() for field in (fieldnames or []) if str(field or "").strip()]
    lower_to_original = {_header_key(field): field for field in fields}
    detected: Dict[str, str] = {}
    for canonical, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            key = _header_key(alias)
            if key in lower_to_original:
                detected[canonical] = lower_to_original[key]
                break
    return detected


def infer_parent_group_from_path(path: str | Path | None) -> str:
    if not path:
        return ""
    stem = Path(path).stem
    for token in (" roster", "_roster", "- roster", " current", " - current"):
        idx = stem.lower().find(token)
        if idx > 0:
            stem = stem[:idx]
            break
    stem = re.sub(r"\b\d{1,4}[-_]\d{1,2}[-_]\d{1,4}\b", " ", stem)
    stem = re.sub(r"[^A-Za-z0-9 ]+", " ", stem)
    words = [w for w in stem.split() if not w.isdigit()]
    return normalize_group_name(" ".join(words[:3]))


def _get(row: Mapping[str, object], detected: Mapping[str, str], key: str) -> str:
    header = detected.get(key)
    return str(row.get(header, "") or "").strip() if header else ""


def _normalize_callsign(value: object) -> str:
    text = str(value or "").strip().upper()
    text = re.sub(r"[^A-Z0-9/]+$", "", text)
    return text if CALLSIGN_RE.match(text) else ""


def _normalize_role(value: object) -> str:
    role = str(value or "").strip().upper()
    return GROUP_ROLE_ALIASES.get(role, role)


def _split_groups(value: object) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = re.split(r"[,;/|]+", raw)
    return [normalize_group_name(part) for part in parts if normalize_group_name(part)]


def _trusted_value(value: object) -> int:
    text = str(value or "").strip().lower()
    if text in {"", "0", "false", "no", "n", "untrusted"}:
        return 0
    return 1


def parse_operator_roster_csv(
    source: TextIO,
    *,
    parent_group: str = "",
    source_path: str | Path | None = None,
    default_trusted: bool = True,
    imported_at_utc: Optional[str] = None,
) -> RosterImportResult:
    reader = csv.DictReader(source)
    detected = detect_roster_headers(reader.fieldnames)
    if "callsign" not in detected:
        raise ValueError("Roster CSV must include a callsign column.")

    parent = normalize_group_name(parent_group) or infer_parent_group_from_path(source_path)
    imported_at = imported_at_utc or datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    entries: List[Dict[str, object]] = []
    child_seen: set[str] = set()
    child_groups: List[str] = []
    skipped = 0

    for row in reader:
        cs = _normalize_callsign(_get(row, detected, "callsign"))
        if not cs:
            skipped += 1
            continue

        region = normalize_group_name(_get(row, detected, "region"))
        groups: List[str] = []
        if parent:
            groups.append(parent)
        if region:
            groups.append(region)
            if region not in child_seen:
                child_seen.add(region)
                child_groups.append(region)
        groups.extend(_split_groups(_get(row, detected, "groups")))
        for key in ("group1", "group2", "group3"):
            value = normalize_group_name(_get(row, detected, key))
            if value:
                groups.append(value)
        groups = _dedupe(groups)

        trusted_raw = _get(row, detected, "trusted")
        trusted = _trusted_value(trusted_raw) if trusted_raw else (1 if default_trusted else 0)
        entry: Dict[str, object] = {
            "callsign": cs,
            "name": _get(row, detected, "name"),
            "state": _get(row, detected, "state").upper(),
            "grid": _get(row, detected, "grid").upper(),
            "group1": groups[0] if len(groups) > 0 else "",
            "group2": groups[1] if len(groups) > 1 else "",
            "group3": groups[2] if len(groups) > 2 else "",
            "groups_json": groups,
            "group_role": _normalize_role(_get(row, detected, "role")),
            "first_seen_utc": imported_at,
            "last_seen_utc": imported_at,
            "trusted": trusted,
            "timezone": _get(row, detected, "timezone"),
            "tier": _get(row, detected, "tier"),
            "roster_parent_group": parent,
            "roster_region": region,
        }
        entries.append(entry)

    return RosterImportResult(
        entries=entries,
        parent_group=parent,
        child_groups=child_groups,
        imported=len(entries),
        skipped=skipped,
        detected_headers=dict(detected),
        source_headers=[str(field or "").strip() for field in (reader.fieldnames or []) if str(field or "").strip()],
    )


def _dedupe(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for value in values:
        normalized = normalize_group_name(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out
