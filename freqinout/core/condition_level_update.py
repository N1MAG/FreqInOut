from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Sequence


@dataclass(frozen=True)
class ConditionLevelUpdateResult:
    settings_data: dict[str, Any]
    operating_group: str
    condition_level: int
    matched_rows: int = 0
    changed_rows: int = 0
    created: bool = False
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class ConditionLevelRevertResult:
    settings_data: dict[str, Any]
    operating_group: str
    restored_rows: int = 0
    warnings: tuple[str, ...] = ()


def apply_operating_group_condition_level(
    settings_data: Mapping[str, Any],
    *,
    operating_group: str,
    condition_level: int,
    create_if_missing: bool = False,
) -> ConditionLevelUpdateResult:
    """Return a copy of settings with one group's condition level applied.

    Operating-group condition level is group-scoped in FIO, while frequency
    configuration rows are band/mode-specific. This helper updates every row
    for the group consistently and is intentionally side-effect free so UI,
    ingest, and later auto-SOP paths can share the same behavior.
    """
    group = _normalize_group(operating_group)
    level = _normalize_level(condition_level)
    output = dict(settings_data or {})
    raw_rows = output.get("operating_groups", [])
    rows = [dict(row) for row in raw_rows] if isinstance(raw_rows, list) else []
    warnings: list[str] = []
    if not group:
        warnings.append("operating group missing")
        output["operating_groups"] = rows
        return ConditionLevelUpdateResult(
            settings_data=output,
            operating_group="",
            condition_level=level,
            warnings=tuple(warnings),
        )

    matched = 0
    changed = 0
    for row in rows:
        if not isinstance(row, MutableMapping):
            continue
        row_group = _normalize_group(row.get("group"))
        if row_group != group:
            continue
        matched += 1
        before_enabled = bool(row.get("use_condition_levels", False))
        before_level = _coerce_level(row.get("condition_level"))
        row["group"] = group
        row["use_condition_levels"] = True
        row["condition_level"] = level
        if not before_enabled or before_level != level:
            changed += 1

    created = False
    if matched == 0 and create_if_missing:
        rows.append(
            {
                "group": group,
                "mode": "",
                "band": "",
                "frequency": "",
                "vfo": "A",
                "fldigi_mode": "",
                "fldigi_offset": "",
                "auto_tune": False,
                "use_condition_levels": True,
                "condition_level": level,
            }
        )
        matched = 1
        changed = 1
        created = True
    elif matched == 0:
        warnings.append(f"operating group {group} is not configured")

    output["operating_groups"] = rows
    return ConditionLevelUpdateResult(
        settings_data=output,
        operating_group=group,
        condition_level=level,
        matched_rows=matched,
        changed_rows=changed,
        created=created,
        warnings=tuple(warnings),
    )


def condition_group_state_snapshot(
    settings_data: Mapping[str, Any],
    *,
    operating_group: str,
) -> tuple[dict[str, Any], ...]:
    """Capture the current condition-level fields for one operating group."""
    group = _normalize_group(operating_group)
    raw_rows = (settings_data or {}).get("operating_groups", [])
    rows = raw_rows if isinstance(raw_rows, list) else []
    snapshot: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            continue
        row_group = _normalize_group(row.get("group"))
        if row_group != group:
            continue
        snapshot.append(
            {
                "index": index,
                "group": row_group,
                "use_condition_levels": bool(row.get("use_condition_levels", False)),
                "condition_level": _coerce_level(row.get("condition_level")),
            }
        )
    return tuple(snapshot)


def revert_operating_group_condition_snapshot(
    settings_data: Mapping[str, Any],
    *,
    operating_group: str,
    snapshot: Sequence[Mapping[str, Any]],
) -> ConditionLevelRevertResult:
    """Return settings with one group's condition-level fields restored."""
    group = _normalize_group(operating_group)
    output = dict(settings_data or {})
    raw_rows = output.get("operating_groups", [])
    rows = [dict(row) for row in raw_rows] if isinstance(raw_rows, list) else []
    warnings: list[str] = []
    if not group:
        warnings.append("operating group missing")
    if not snapshot:
        warnings.append("no previous condition-level snapshot is available")
    by_index: dict[int, Mapping[str, Any]] = {}
    for item in snapshot or ():
        if not isinstance(item, Mapping):
            continue
        try:
            index = int(item.get("index"))
        except Exception:
            continue
        if _normalize_group(item.get("group")) != group:
            continue
        by_index[index] = item

    restored = 0
    for index, item in by_index.items():
        if index < 0 or index >= len(rows):
            continue
        row = rows[index]
        if _normalize_group(row.get("group")) != group:
            continue
        row["group"] = group
        row["use_condition_levels"] = bool(item.get("use_condition_levels", False))
        previous_level = _coerce_level(item.get("condition_level"))
        if previous_level is None:
            row.pop("condition_level", None)
        else:
            row["condition_level"] = previous_level
        restored += 1

    if restored == 0 and not warnings:
        warnings.append(f"operating group {group} rows could not be matched for revert")

    output["operating_groups"] = rows
    return ConditionLevelRevertResult(
        settings_data=output,
        operating_group=group,
        restored_rows=restored,
        warnings=tuple(warnings),
    )


def _normalize_group(value: object) -> str:
    text = str(value or "").strip().upper()
    return text[1:] if text.startswith("@") else text


def _normalize_level(value: int) -> int:
    try:
        level = int(value)
    except Exception:
        level = 5
    return max(1, min(5, level))


def _coerce_level(value: object) -> int | None:
    try:
        level = int(value)
    except Exception:
        return None
    if 1 <= level <= 5:
        return level
    return None
