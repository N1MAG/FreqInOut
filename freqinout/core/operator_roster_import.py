from __future__ import annotations

import csv
import datetime
import re
from dataclasses import dataclass, field, replace
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
DIAGNOSTIC_LABELS = {
    "imported": "Imported",
    "updated": "Updated",
    "blank_ignored": "Blank ignored",
    "legend_ignored": "Section/legend ignored",
    "invalid_skipped": "Invalid skipped",
}


@dataclass(frozen=True)
class RosterImportDiagnostic:
    line: int
    classification: str
    callsign_text: str = ""
    field: str = ""
    reason: str = ""


def format_roster_diagnostic_classification(classification: str) -> str:
    return DIAGNOSTIC_LABELS.get(classification, classification.replace("_", " ").capitalize())


@dataclass(frozen=True)
class RosterImportResult:
    entries: List[Dict[str, object]]
    parent_group: str
    child_groups: List[str]
    imported: int
    skipped: int
    detected_headers: Dict[str, str]
    source_headers: List[str]
    updated: int = 0
    blank_ignored: int = 0
    legend_ignored: int = 0
    invalid_skipped: int = 0
    diagnostics: List[RosterImportDiagnostic] = field(default_factory=list)

    def diagnostics_text(self) -> str:
        lines = [
            f"Imported: {self.imported}",
            f"Updated: {self.updated}",
            f"Blank ignored: {self.blank_ignored}",
            f"Section/legend ignored: {self.legend_ignored}",
            f"Invalid skipped: {self.invalid_skipped}",
        ]
        for item in self.diagnostics:
            lines.append(
                " | ".join(
                    part for part in (
                        f"Line {item.line}", format_roster_diagnostic_classification(item.classification),
                        item.callsign_text, item.field, item.reason
                    ) if part
                )
            )
        return "\n".join(lines)


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


def _row_values(row: Mapping[str, object]) -> List[str]:
    return [str(value or "").strip() for value in row.values()]


def _is_legend_row(callsign_text: str, row: Mapping[str, object]) -> bool:
    """Return true only for recognizable roster labels, not malformed records."""
    values = [value for value in _row_values(row) if value]
    # The supplied MAGNET roster stores its trailing labels in TimeZone, leaving
    # Callsign blank.  A sole populated cell is therefore the marker to inspect.
    marker_text = callsign_text or (values[0] if len(values) == 1 else "")
    marker = re.sub(r"\s+", " ", str(marker_text or "").strip().lower())
    if marker in {"new additions", "c.s. change", "cs change", "limbo"}:
        return True
    if marker.startswith("*") and "signal" in marker:
        return True
    return False


def classify_roster_import_result(
    result: RosterImportResult,
    *,
    existing_callsigns: Iterable[object] = (),
) -> RosterImportResult:
    """Classify accepted rows as new imports or updates without writing data."""
    existing = {_normalize_callsign(value) for value in existing_callsigns}
    existing.discard("")
    updated_callsigns = {
        _normalize_callsign(entry.get("callsign"))
        for entry in result.entries
        if _normalize_callsign(entry.get("callsign")) in existing
    }
    diagnostics = [
        replace(
            item,
            classification=("updated" if _normalize_callsign(item.callsign_text) in updated_callsigns else "imported"),
            reason=("Existing operator will be updated" if _normalize_callsign(item.callsign_text) in updated_callsigns else "New operator ready to import"),
        )
        if item.classification in {"imported", "updated"}
        else item
        for item in result.diagnostics
    ]
    updated = len(updated_callsigns)
    return replace(result, imported=max(0, len(result.entries) - updated), updated=updated, diagnostics=diagnostics)


def _roster_csv_reader(source: TextIO) -> csv.DictReader:
    """Use a bounded dialect probe when the supplied stream can be rewound."""
    try:
        if not source.seekable():
            return csv.DictReader(source)
        position = source.tell()
        sample = source.read(8192)
        source.seek(position)
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        return csv.DictReader(source, dialect=dialect)
    except (AttributeError, csv.Error, OSError):
        return csv.DictReader(source)


def parse_operator_roster_csv(
    source: TextIO,
    *,
    parent_group: str = "",
    source_path: str | Path | None = None,
    default_trusted: bool = True,
    imported_at_utc: Optional[str] = None,
) -> RosterImportResult:
    reader = _roster_csv_reader(source)
    detected = detect_roster_headers(reader.fieldnames)
    if "callsign" not in detected:
        headers = ", ".join(str(header or "") for header in (reader.fieldnames or []) if header)
        raise ValueError(f"Roster CSV must include a callsign column (found: {headers or 'no headers'}).")

    parent = normalize_group_name(parent_group) or infer_parent_group_from_path(source_path)
    imported_at = imported_at_utc or datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    entries: List[Dict[str, object]] = []
    child_seen: set[str] = set()
    child_groups: List[str] = []
    diagnostics: List[RosterImportDiagnostic] = []
    blank_ignored = 0
    legend_ignored = 0
    invalid_skipped = 0
    seen_callsigns: set[str] = set()

    for row in reader:
        line = max(2, int(reader.line_num or 0))
        callsign_text = _get(row, detected, "callsign")
        populated_cells = [
            (str(field or "").strip(), str(value or "").strip())
            for field, value in row.items()
            if str(value or "").strip()
        ]
        marker_field, marker_text = (
            (populated_cells[0] if len(populated_cells) == 1 else ("callsign", callsign_text))
        )
        if not any(_row_values(row)):
            blank_ignored += 1
            diagnostics.append(RosterImportDiagnostic(line, "blank_ignored", reason="Blank or separator row"))
            continue
        if _is_legend_row(callsign_text, row):
            legend_ignored += 1
            diagnostics.append(
                RosterImportDiagnostic(line, "legend_ignored", marker_text, marker_field, "Section or legend row")
            )
            continue
        cs = _normalize_callsign(callsign_text)
        if not cs:
            invalid_skipped += 1
            diagnostics.append(RosterImportDiagnostic(line, "invalid_skipped", callsign_text, "callsign", "Invalid callsign"))
            continue
        if cs in seen_callsigns:
            invalid_skipped += 1
            diagnostics.append(RosterImportDiagnostic(line, "invalid_skipped", callsign_text, "callsign", "Duplicate callsign in CSV"))
            continue
        seen_callsigns.add(cs)
        diagnostics.append(
            RosterImportDiagnostic(line, "imported", callsign_text, "callsign", "New operator ready to import")
        )

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
        skipped=invalid_skipped,
        detected_headers=dict(detected),
        source_headers=[str(field or "").strip() for field in (reader.fieldnames or []) if str(field or "").strip()],
        blank_ignored=blank_ignored,
        legend_ignored=legend_ignored,
        invalid_skipped=invalid_skipped,
        diagnostics=diagnostics,
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
