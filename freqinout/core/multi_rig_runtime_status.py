from __future__ import annotations

from collections.abc import Iterable as IterableABC
from dataclasses import dataclass
from typing import Any, Iterable, Literal, Mapping, Optional

from freqinout.core.multi_radio_store import (
    FIO_EXISTING_USE_IGNORED_KEYS,
    MultiRadioStore,
)


MultiRigStartupMode = Literal[
    "fresh_default_ready",
    "migrated",
    "existing_unmigrated",
    "deferred",
    "migration_error",
]
RuntimeScope = Literal["primary_runtime", "all_active_runtime", "none"]

STARTUP_FRESH_DEFAULT_READY: MultiRigStartupMode = "fresh_default_ready"
STARTUP_MIGRATED: MultiRigStartupMode = "migrated"
STARTUP_EXISTING_UNMIGRATED: MultiRigStartupMode = "existing_unmigrated"
STARTUP_DEFERRED: MultiRigStartupMode = "deferred"
STARTUP_MIGRATION_ERROR: MultiRigStartupMode = "migration_error"

SCOPE_PRIMARY_RUNTIME: RuntimeScope = "primary_runtime"
SCOPE_ALL_ACTIVE_RUNTIME: RuntimeScope = "all_active_runtime"
SCOPE_NONE: RuntimeScope = "none"


@dataclass(frozen=True)
class MultiRigRuntimeStatus:
    startup_mode: MultiRigStartupMode
    migration_version: int
    migration_current: bool
    migration_deferred: bool
    existing_fio_usage_detected: bool
    fresh_install_default_created: bool
    primary_radio_id: Optional[str]
    active_radio_ids: tuple[str, ...]
    primary_device_profile_id: Optional[int]
    active_device_profile_ids: tuple[int, ...]
    scheduler_scope: RuntimeScope
    messages_scope: RuntimeScope
    map_scope: RuntimeScope
    controlfreq_scope: RuntimeScope
    main_window_scope: RuntimeScope
    background_ingest_scope: RuntimeScope
    compatibility_projection_allowed: bool
    legacy_write_mirroring_allowed: bool
    warnings: tuple[str, ...] = ()


def radio_shared_state_id(device_profile_id: object) -> str:
    text = str(device_profile_id if device_profile_id is not None else "").strip()
    return f"radio_{text}" if text else ""


def device_profile_id_from_radio_id(radio_id: object) -> int:
    text = str(radio_id if radio_id is not None else "").strip()
    if not text.startswith("radio_"):
        raise ValueError(f"Invalid radio id: {text or '<empty>'}")
    raw_id = text.removeprefix("radio_").strip()
    try:
        value = int(raw_id)
    except Exception as exc:
        raise ValueError(f"Invalid radio id: {text}") from exc
    if value <= 0:
        raise ValueError(f"Invalid radio id: {text}")
    return value


def _has_meaningful_legacy_kv(settings_values: Mapping[str, Any]) -> bool:
    meaningful = {str(key) for key in settings_values.keys()} - set(FIO_EXISTING_USE_IGNORED_KEYS)
    return bool(meaningful)


def _int_id(row: Optional[Mapping[str, Any]]) -> Optional[int]:
    if not row:
        return None
    try:
        value = int(row.get("id", 0) or 0)
    except Exception:
        return None
    return value if value > 0 else None


def _active_ids(rows: Iterable[Mapping[str, Any]]) -> tuple[int, ...]:
    ids: list[int] = []
    for row in rows:
        try:
            value = int(row.get("id", 0) or 0)
        except Exception:
            continue
        if value > 0:
            ids.append(value)
    return tuple(ids)


def build_multi_rig_runtime_status(
    store: Optional[MultiRadioStore] = None,
    *,
    settings_values: Optional[Mapping[str, Any]] = None,
    existing_fio_usage: Optional[bool] = None,
    migration_warnings: Iterable[str] = (),
) -> MultiRigRuntimeStatus:
    store = store or MultiRadioStore()
    warnings = tuple(str(item) for item in migration_warnings if str(item).strip())

    try:
        inputs = store.read_runtime_status_inputs(
            settings_values=settings_values,
            existing_fio_usage=existing_fio_usage,
        )
    except Exception as exc:
        warnings = warnings + (f"Runtime status read failed: {exc}",)
        inputs = {
            "settings_values": dict(settings_values or {}),
            "migration_version": 0,
            "migration_deferred": False,
            "migration_current": False,
            "existing_fio_usage": bool(existing_fio_usage),
            "primary_row": None,
            "active_rows": (),
        }

    loaded_settings = inputs.get("settings_values")
    settings_map: Mapping[str, Any] = loaded_settings if isinstance(loaded_settings, Mapping) else {}
    migration_version = int(inputs.get("migration_version", 0) or 0)
    migration_deferred = bool(inputs.get("migration_deferred", False))
    migration_current = bool(inputs.get("migration_current", False))
    existing_usage = bool(inputs.get("existing_fio_usage", False))

    if warnings and not migration_current:
        startup_mode: MultiRigStartupMode = STARTUP_MIGRATION_ERROR
    elif migration_current and not _has_meaningful_legacy_kv(settings_map):
        startup_mode = STARTUP_FRESH_DEFAULT_READY
    elif migration_current:
        startup_mode = STARTUP_MIGRATED
    elif migration_deferred:
        startup_mode = STARTUP_DEFERRED
    elif existing_usage:
        startup_mode = STARTUP_EXISTING_UNMIGRATED
    else:
        startup_mode = STARTUP_EXISTING_UNMIGRATED

    primary_row: Optional[Mapping[str, Any]] = None
    active_rows: tuple[Mapping[str, Any], ...] = ()
    if startup_mode in {STARTUP_MIGRATED, STARTUP_FRESH_DEFAULT_READY}:
        raw_primary_row = inputs.get("primary_row")
        raw_active_rows = inputs.get("active_rows")
        primary_row = raw_primary_row if isinstance(raw_primary_row, Mapping) else None
        if isinstance(raw_active_rows, IterableABC) and not isinstance(raw_active_rows, (str, bytes)):
            active_rows = tuple(row for row in raw_active_rows if isinstance(row, Mapping))

    primary_device_profile_id = _int_id(primary_row)
    active_device_profile_ids = _active_ids(active_rows)
    primary_radio_id = radio_shared_state_id(primary_device_profile_id) if primary_device_profile_id else None
    active_radio_ids = tuple(radio_shared_state_id(row_id) for row_id in active_device_profile_ids)

    scopes_enabled = startup_mode in {STARTUP_FRESH_DEFAULT_READY, STARTUP_MIGRATED}
    compatibility_allowed = scopes_enabled
    background_scope: RuntimeScope = SCOPE_ALL_ACTIVE_RUNTIME if scopes_enabled else SCOPE_NONE

    return MultiRigRuntimeStatus(
        startup_mode=startup_mode,
        migration_version=migration_version,
        migration_current=migration_current,
        migration_deferred=migration_deferred,
        existing_fio_usage_detected=existing_usage,
        fresh_install_default_created=startup_mode == STARTUP_FRESH_DEFAULT_READY,
        primary_radio_id=primary_radio_id,
        active_radio_ids=active_radio_ids,
        primary_device_profile_id=primary_device_profile_id,
        active_device_profile_ids=active_device_profile_ids,
        scheduler_scope=SCOPE_ALL_ACTIVE_RUNTIME if scopes_enabled else SCOPE_NONE,
        messages_scope=SCOPE_ALL_ACTIVE_RUNTIME if scopes_enabled else SCOPE_NONE,
        map_scope=SCOPE_ALL_ACTIVE_RUNTIME if scopes_enabled else SCOPE_NONE,
        controlfreq_scope=SCOPE_PRIMARY_RUNTIME if scopes_enabled else SCOPE_NONE,
        main_window_scope=SCOPE_PRIMARY_RUNTIME if scopes_enabled else SCOPE_NONE,
        background_ingest_scope=background_scope,
        compatibility_projection_allowed=compatibility_allowed,
        legacy_write_mirroring_allowed=compatibility_allowed,
        warnings=warnings,
    )
