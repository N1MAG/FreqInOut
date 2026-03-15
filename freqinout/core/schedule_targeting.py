from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

TARGET_SCOPE_STATION = "station"
TARGET_SCOPE_DEVICE_PROFILE = "device_profile"
TARGET_SCOPE_OPERATING_PROFILE = "operating_profile"

SUPPORTED_TARGET_SCOPES = (
    TARGET_SCOPE_STATION,
    TARGET_SCOPE_DEVICE_PROFILE,
    TARGET_SCOPE_OPERATING_PROFILE,
)


def _coerce_optional_int(value: Any) -> Optional[int]:
    if value in (None, "", False):
        return None
    try:
        return int(value)
    except Exception:
        return None


def normalize_target_scope(value: Any, default: str = TARGET_SCOPE_STATION) -> str:
    raw = str(value or "").strip().lower()
    if raw in SUPPORTED_TARGET_SCOPES:
        return raw
    return default if default in SUPPORTED_TARGET_SCOPES else TARGET_SCOPE_STATION


def normalize_schedule_target(
    scope_value: Any,
    *,
    target_device_profile_id: Any = None,
    target_operating_profile_id: Any = None,
) -> Tuple[str, Optional[int], Optional[int]]:
    scope = normalize_target_scope(scope_value)
    device_profile_id = _coerce_optional_int(target_device_profile_id)
    operating_profile_id = _coerce_optional_int(target_operating_profile_id)
    if scope != TARGET_SCOPE_DEVICE_PROFILE:
        device_profile_id = None
    if scope != TARGET_SCOPE_OPERATING_PROFILE:
        operating_profile_id = None
    return scope, device_profile_id, operating_profile_id


def normalize_schedule_target_fields(row: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    scope, device_profile_id, operating_profile_id = normalize_schedule_target(
        row.get("target_scope"),
        target_device_profile_id=row.get("target_device_profile_id"),
        target_operating_profile_id=row.get("target_operating_profile_id"),
    )
    out["target_scope"] = scope
    out["target_device_profile_id"] = device_profile_id
    out["target_operating_profile_id"] = operating_profile_id
    return out


def schedule_row_matches_runtime_target(
    row: Mapping[str, Any],
    *,
    primary_device_profile_id: Optional[int],
    primary_operating_profile_id: Optional[int],
) -> bool:
    scope, target_device_profile_id, target_operating_profile_id = normalize_schedule_target(
        row.get("target_scope"),
        target_device_profile_id=row.get("target_device_profile_id"),
        target_operating_profile_id=row.get("target_operating_profile_id"),
    )
    if scope == TARGET_SCOPE_STATION:
        return True
    if scope == TARGET_SCOPE_DEVICE_PROFILE:
        return (
            primary_device_profile_id is not None
            and target_device_profile_id is not None
            and int(target_device_profile_id) == int(primary_device_profile_id)
        )
    if scope == TARGET_SCOPE_OPERATING_PROFILE:
        return (
            primary_operating_profile_id is not None
            and target_operating_profile_id is not None
            and int(target_operating_profile_id) == int(primary_operating_profile_id)
        )
    return False


def schedule_targets_may_overlap(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_scope, left_device_id, left_operating_id = normalize_schedule_target(
        left.get("target_scope"),
        target_device_profile_id=left.get("target_device_profile_id"),
        target_operating_profile_id=left.get("target_operating_profile_id"),
    )
    right_scope, right_device_id, right_operating_id = normalize_schedule_target(
        right.get("target_scope"),
        target_device_profile_id=right.get("target_device_profile_id"),
        target_operating_profile_id=right.get("target_operating_profile_id"),
    )

    if left_scope == TARGET_SCOPE_STATION or right_scope == TARGET_SCOPE_STATION:
        return True
    if left_scope == TARGET_SCOPE_DEVICE_PROFILE and left_device_id is None:
        return False
    if right_scope == TARGET_SCOPE_DEVICE_PROFILE and right_device_id is None:
        return False
    if left_scope == TARGET_SCOPE_OPERATING_PROFILE and left_operating_id is None:
        return False
    if right_scope == TARGET_SCOPE_OPERATING_PROFILE and right_operating_id is None:
        return False
    if left_scope == TARGET_SCOPE_DEVICE_PROFILE and right_scope == TARGET_SCOPE_DEVICE_PROFILE:
        return int(left_device_id) == int(right_device_id)
    if left_scope == TARGET_SCOPE_OPERATING_PROFILE and right_scope == TARGET_SCOPE_OPERATING_PROFILE:
        return int(left_operating_id) == int(right_operating_id)
    return True
