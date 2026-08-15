from __future__ import annotations

from typing import Mapping


def scheduler_entry_radio_id(entry: Mapping[str, object] | None) -> int:
    if not isinstance(entry, Mapping):
        return 0
    for key in ("target_device_profile_id", "device_profile_id", "radio_id"):
        try:
            ident = int(entry.get(key) or 0)
        except Exception:
            ident = 0
        if ident > 0:
            return ident
    return 0


def manual_qsy_meta_for_radio(
    *,
    meta: Mapping[str, object] | None,
    meta_profile_id: object,
    device_profile_id: int,
) -> dict[str, object] | None:
    try:
        radio_id = int(device_profile_id or 0)
        stored_id = int(meta_profile_id or 0)
    except Exception:
        return None
    if radio_id <= 0 or stored_id != radio_id or not isinstance(meta, Mapping):
        return None
    return dict(meta)


def scheduler_manual_qsy_active_for_radio(
    *,
    device_profile_id: int,
    manual_meta: Mapping[str, object] | None,
    scheduler_source: object,
    scheduler_manual_active: object,
    scheduler_entry: Mapping[str, object] | None,
    primary_manual_radio_id: object = 0,
) -> bool:
    try:
        radio_id = int(device_profile_id or 0)
    except Exception:
        return False
    if radio_id <= 0:
        return False
    if isinstance(manual_meta, Mapping):
        return True
    source = str(scheduler_source or "").strip().upper()
    if source != "QSY" and not bool(scheduler_manual_active):
        return False
    entry_radio_id = scheduler_entry_radio_id(scheduler_entry)
    if entry_radio_id <= 0:
        try:
            entry_radio_id = int(primary_manual_radio_id or 0)
        except Exception:
            entry_radio_id = 0
    return entry_radio_id > 0 and entry_radio_id == radio_id


def scheduler_suspended_manually_for_radio(
    *,
    device_profile_id: int,
    suspended_manual: object,
    suspended_profile_id: object,
    runtime_scheduler_enabled_override: object,
    selected_profile_id: object,
) -> bool:
    try:
        radio_id = int(device_profile_id or 0)
    except Exception:
        return False
    if radio_id <= 0:
        return False
    try:
        stored_id = int(suspended_profile_id or 0)
    except Exception:
        stored_id = 0
    if bool(suspended_manual) and stored_id == radio_id:
        return True
    if runtime_scheduler_enabled_override is False:
        try:
            selected_id = int(selected_profile_id or 0)
        except Exception:
            selected_id = 0
        return selected_id == radio_id
    return False


def timed_suspend_active_for_radio(
    *,
    device_profile_id: int,
    timed_suspend_profile_id: object,
    hold_active: object,
) -> bool:
    try:
        radio_id = int(device_profile_id or 0)
        timed_id = int(timed_suspend_profile_id or 0)
    except Exception:
        return False
    return radio_id > 0 and timed_id == radio_id and bool(hold_active)
