from __future__ import annotations

import json
from typing import Mapping, Sequence

from freqinout.core.operator_groups import trusted_callsigns_for_groups

GROUP_SOURCE_EMPTY_SUMMARY = "Roster source: none. Allowed callsigns are manual or synced from VarAC.ini."


def normalize_group_source_selections(value: object) -> list[dict[str, object]]:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except Exception:
            return []
    if not isinstance(value, list):
        return []
    merged: dict[str, set[str]] = {}
    for raw_entry in value:
        if not isinstance(raw_entry, dict):
            continue
        family = str(raw_entry.get("family", "") or "").strip().upper()
        if not family:
            continue
        groups_raw = raw_entry.get("groups", [])
        if isinstance(groups_raw, str):
            groups = [groups_raw]
        elif isinstance(groups_raw, Sequence):
            groups = list(groups_raw)
        else:
            groups = []
        clean_groups = {
            str(group or "").strip().upper()
            for group in groups
            if str(group or "").strip()
        }
        if not clean_groups:
            clean_groups = {family}
        merged.setdefault(family, set()).update(clean_groups)
    return [
        {
            "family": family,
            "groups": sorted(groups),
            "mode": "trusted_callsigns",
        }
        for family, groups in sorted(merged.items())
    ]


def group_source_selections_json(value: object) -> str:
    rows = normalize_group_source_selections(value)
    if not rows:
        return ""
    return json.dumps(rows, separators=(",", ":"), sort_keys=True)


def group_source_summary_text(value: object) -> str:
    rows = normalize_group_source_selections(value)
    if not rows:
        return GROUP_SOURCE_EMPTY_SUMMARY
    parts: list[str] = []
    for row in rows:
        family = str(row.get("family", "") or "").strip().upper()
        groups = [
            str(group or "").strip().upper()
            for group in (row.get("groups", []) if isinstance(row.get("groups", []), Sequence) else [])
            if str(group or "").strip()
        ]
        if not family:
            continue
        if groups and groups != [family]:
            parts.append(f"{family} / {', '.join(groups)}")
        else:
            parts.append(family)
    if not parts:
        return GROUP_SOURCE_EMPTY_SUMMARY
    return "Roster source: " + "; ".join(parts)


def has_group_source(value: object) -> bool:
    return bool(normalize_group_source_selections(value))


def append_group_source_selection(value: object, family: object, groups: Sequence[object]) -> list[dict[str, object]]:
    current = normalize_group_source_selections(value)
    parent = str(family or "").strip().upper()
    clean_groups = [
        str(group or "").strip().upper()
        for group in groups
        if str(group or "").strip()
    ]
    if not parent and clean_groups:
        parent = clean_groups[0]
    if not parent:
        return current
    current.append({"family": parent, "groups": clean_groups or [parent], "mode": "trusted_callsigns"})
    return normalize_group_source_selections(current)


def remove_group_source_indexes(value: object, indexes: Sequence[int]) -> tuple[list[dict[str, object]], int]:
    rows = normalize_group_source_selections(value)
    remove_indexes = {int(idx) for idx in indexes if 0 <= int(idx) < len(rows)}
    if not remove_indexes:
        return rows, 0
    kept = [row for idx, row in enumerate(rows) if idx not in remove_indexes]
    return normalize_group_source_selections(kept), len(remove_indexes)


def roster_refresh_plan(
    sources_value: object,
    current_callsigns: Sequence[str],
    families: Mapping[str, object],
) -> dict[str, object]:
    sources = normalize_group_source_selections(sources_value)
    current = {
        str(callsign or "").strip().upper()
        for callsign in current_callsigns
        if str(callsign or "").strip()
    }
    desired: set[str] = set()
    missing_families: list[str] = []
    source_groups: list[str] = []
    for source in sources:
        family = str(source.get("family", "") or "").strip().upper()
        groups = [
            str(group or "").strip().upper()
            for group in (source.get("groups", []) if isinstance(source.get("groups", []), Sequence) else [])
            if str(group or "").strip()
        ]
        if not family:
            continue
        if family not in families:
            missing_families.append(family)
            continue
        selected_groups = groups or [family]
        source_groups.extend(selected_groups)
        desired.update(trusted_callsigns_for_groups(selected_groups, families))
    return {
        "sources": sources,
        "groups": sorted(set(source_groups)),
        "current": sorted(current),
        "desired": sorted(desired),
        "added": sorted(desired - current),
        "removed": sorted(current - desired),
        "unchanged": sorted(current & desired),
        "missing_families": sorted(set(missing_families)),
    }
