from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

TARGET_SCOPE_STATION = "station"
TARGET_SCOPE_DEVICE_PROFILE = "device_profile"
TARGET_SCOPE_OPERATING_PROFILE = "operating_profile"
TARGET_SCOPE_INHERITED = "inherited"

SCHEDULE_KIND_HF = "hf"
SCHEDULE_KIND_NET = "net"

SUPPORTED_TARGET_SCOPES = (
    TARGET_SCOPE_INHERITED,
    TARGET_SCOPE_STATION,
    TARGET_SCOPE_DEVICE_PROFILE,
    TARGET_SCOPE_OPERATING_PROFILE,
)


def schedule_default_target_keys(schedule_kind: str) -> tuple[str, str, str]:
    normalized_kind = str(schedule_kind or "").strip().lower()
    prefix = "net_schedule" if normalized_kind == SCHEDULE_KIND_NET else "hf_schedule"
    return (
        f"{prefix}_default_target_scope",
        f"{prefix}_default_target_device_profile_id",
        f"{prefix}_default_target_operating_profile_id",
    )


def _coerce_optional_int(value: Any) -> Optional[int]:
    if value in (None, "", False):
        return None
    try:
        return int(value)
    except Exception:
        return None


def normalize_target_scope(value: Any, default: str = TARGET_SCOPE_STATION, *, allow_inherited: bool = True) -> str:
    raw = str(value or "").strip().lower()
    if raw == TARGET_SCOPE_INHERITED and not allow_inherited:
        raw = ""
    if raw in SUPPORTED_TARGET_SCOPES:
        return raw
    return default if default in SUPPORTED_TARGET_SCOPES else TARGET_SCOPE_STATION


def normalize_schedule_target(
    scope_value: Any,
    *,
    target_device_profile_id: Any = None,
    target_operating_profile_id: Any = None,
    allow_inherited: bool = True,
) -> Tuple[str, Optional[int], Optional[int]]:
    scope = normalize_target_scope(scope_value, allow_inherited=allow_inherited)
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


def load_schedule_default_target(settings: Any, schedule_kind: str) -> Tuple[str, Optional[int], Optional[int]]:
    scope_key, device_key, operating_key = schedule_default_target_keys(schedule_kind)
    scope_value = TARGET_SCOPE_STATION
    device_value = None
    operating_value = None
    try:
        if settings is not None and hasattr(settings, "get"):
            scope_value = settings.get(scope_key, TARGET_SCOPE_STATION)
            device_value = settings.get(device_key, None)
            operating_value = settings.get(operating_key, None)
    except Exception:
        scope_value = TARGET_SCOPE_STATION
        device_value = None
        operating_value = None
    return normalize_schedule_target(
        scope_value,
        target_device_profile_id=device_value,
        target_operating_profile_id=operating_value,
        allow_inherited=False,
    )


def save_schedule_default_target(
    settings: Any,
    schedule_kind: str,
    *,
    target_scope: Any,
    target_device_profile_id: Any = None,
    target_operating_profile_id: Any = None,
) -> Tuple[str, Optional[int], Optional[int]]:
    scope, device_profile_id, operating_profile_id = normalize_schedule_target(
        target_scope,
        target_device_profile_id=target_device_profile_id,
        target_operating_profile_id=target_operating_profile_id,
        allow_inherited=False,
    )
    scope_key, device_key, operating_key = schedule_default_target_keys(schedule_kind)
    values = {
        scope_key: scope,
        device_key: device_profile_id,
        operating_key: operating_profile_id,
    }
    if settings is not None and hasattr(settings, "set_many"):
        settings.set_many(values)
    elif settings is not None and hasattr(settings, "set"):
        for key, value in values.items():
            settings.set(key, value)
    return scope, device_profile_id, operating_profile_id


def resolve_schedule_target(
    row: Mapping[str, Any],
    *,
    default_target_scope: str = TARGET_SCOPE_STATION,
    default_target_device_profile_id: Optional[int] = None,
    default_target_operating_profile_id: Optional[int] = None,
) -> Tuple[str, Optional[int], Optional[int], bool]:
    scope, device_profile_id, operating_profile_id = normalize_schedule_target(
        row.get("target_scope"),
        target_device_profile_id=row.get("target_device_profile_id"),
        target_operating_profile_id=row.get("target_operating_profile_id"),
    )
    if scope == TARGET_SCOPE_INHERITED:
        default_scope, default_device_id, default_operating_id = normalize_schedule_target(
            default_target_scope,
            target_device_profile_id=default_target_device_profile_id,
            target_operating_profile_id=default_target_operating_profile_id,
            allow_inherited=False,
        )
        return default_scope, default_device_id, default_operating_id, True
    return scope, device_profile_id, operating_profile_id, False


def schedule_row_matches_runtime_target(
    row: Mapping[str, Any],
    *,
    primary_device_profile_id: Optional[int],
    primary_operating_profile_id: Optional[int],
    default_target_scope: str = TARGET_SCOPE_STATION,
    default_target_device_profile_id: Optional[int] = None,
    default_target_operating_profile_id: Optional[int] = None,
) -> bool:
    return schedule_row_matches_target_context(
        row,
        device_profile_id=primary_device_profile_id,
        operating_profile_id=primary_operating_profile_id,
        default_target_scope=default_target_scope,
        default_target_device_profile_id=default_target_device_profile_id,
        default_target_operating_profile_id=default_target_operating_profile_id,
    )


def schedule_row_matches_target_context(
    row: Mapping[str, Any],
    *,
    device_profile_id: Optional[int],
    operating_profile_id: Optional[int],
    default_target_scope: str = TARGET_SCOPE_STATION,
    default_target_device_profile_id: Optional[int] = None,
    default_target_operating_profile_id: Optional[int] = None,
) -> bool:
    scope, target_device_profile_id, target_operating_profile_id, _ = resolve_schedule_target(
        row,
        default_target_scope=default_target_scope,
        default_target_device_profile_id=default_target_device_profile_id,
        default_target_operating_profile_id=default_target_operating_profile_id,
    )
    if scope == TARGET_SCOPE_STATION:
        return True
    if scope == TARGET_SCOPE_DEVICE_PROFILE:
        return (
            device_profile_id is not None
            and target_device_profile_id is not None
            and int(target_device_profile_id) == int(device_profile_id)
        )
    if scope == TARGET_SCOPE_OPERATING_PROFILE:
        return (
            operating_profile_id is not None
            and target_operating_profile_id is not None
            and int(target_operating_profile_id) == int(operating_profile_id)
        )
    return False


def format_schedule_target_label(
    scope: str,
    *,
    inherited: bool = False,
    device_name: str = "",
    operating_profile_name: str = "",
) -> str:
    normalized = normalize_target_scope(scope, allow_inherited=False)
    if normalized == TARGET_SCOPE_DEVICE_PROFILE:
        base = device_name or "Device Profile"
    elif normalized == TARGET_SCOPE_OPERATING_PROFILE:
        base = operating_profile_name or "Operating Profile"
    else:
        base = "Station"
    return f"Inherited ({base})" if inherited else base


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
