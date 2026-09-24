"""Local Net draft helpers and catalog subscription review semantics."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, replace
from typing import Any, Iterable, Mapping

from freqinout.core.local_net_models import LocalNetSchedule
from freqinout.core.resource_catalog_models import FrequencyResource, NetDirectorySession
from freqinout.core.resource_catalog_store import ResourceCatalogStore


DAY_INDEX = {
    "MONDAY": 0, "MON": 0, "TUESDAY": 1, "TUE": 1, "WEDNESDAY": 2, "WED": 2,
    "THURSDAY": 3, "THU": 3, "FRIDAY": 4, "FRI": 4, "SATURDAY": 5, "SAT": 5,
    "SUNDAY": 6, "SUN": 6,
}


@dataclass(frozen=True, slots=True)
class LocalNetResourceStatus:
    state: str
    message: str
    changed_fields: tuple[str, ...] = ()


def new_local_net_schedule_key() -> str:
    return f"local_net_{uuid.uuid4()}"


def _recurrence(value: object) -> str:
    text = str(value or "Weekly").strip().lower().replace("-", "_").replace(" ", "_")
    return {"monthly": "periodic", "bi_weekly": "biweekly", "onetime": "one_time"}.get(text, text)


def _session_snapshot(session: object) -> dict[str, object]:
    return {
        name: getattr(session, name, None)
        for name in (
            "net_session_key", "net_entry_key", "frequency_resource_key", "service",
            "recurrence", "day_utc", "local_start_time", "duration_minutes", "timezone",
            "effective_start_date", "effective_end_date", "exception_dates", "reminder_minutes",
            "mode", "mode_details",
        )
    }


def _frequency_snapshot(frequency: object | None) -> dict[str, object] | None:
    return None if frequency is None else {
        name: getattr(frequency, name, None)
        for name in (
            "frequency_resource_key", "source_key", "resource_kind", "service", "band", "channel",
            "label", "receive_hz", "transmit_hz", "center_hz", "offset_hz", "mode", "tone",
            "locality", "coverage",
        )
    }


def _snapshot(session: object, frequency: object | None) -> str:
    return json.dumps(
        {"session": _session_snapshot(session), "frequency": _frequency_snapshot(frequency)},
        sort_keys=True,
        default=list,
    )


def schedule_with_accepted_frequency(
    schedule: LocalNetSchedule,
    frequency: FrequencyResource,
) -> LocalNetSchedule:
    """Return a draft whose selected frequency is an accepted local snapshot.

    Changing a directory-derived reminder's frequency is an explicit local
    override.  Capturing the selected resource prevents an immediate false
    "update available" state while retaining the subscribed session snapshot.
    """
    try:
        snapshot = json.loads(schedule.accepted_snapshot_json or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        snapshot = {}
    if not isinstance(snapshot, dict):
        snapshot = {}
    snapshot["frequency"] = _frequency_snapshot(frequency)
    return replace(
        schedule,
        frequency_resource_key=frequency.frequency_resource_key,
        accepted_resource_version_hash=frequency.version_hash,
        accepted_snapshot_json=json.dumps(snapshot, sort_keys=True, default=list),
    )


def schedule_from_directory_session(
    store: ResourceCatalogStore,
    net_session_key: str,
    *,
    schedule_key: str | None = None,
    operating_group_key: str | None = None,
    operating_group_name: str | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> LocalNetSchedule:
    session = store.get_session(net_session_key)
    if session is None:
        raise ValueError(f"published session not found: {net_session_key}")
    entry = store.get_net_entry(session.net_entry_key)
    frequency = store.get_frequency(session.frequency_resource_key) if session.frequency_resource_key else None
    group_links = store.net_entry_group_links(session.net_entry_key)
    default_group_key, default_group_name = group_links[0] if group_links else (None, None)
    recurrence = _recurrence(session.recurrence)
    day = str(getattr(session, "day_utc", None) or "").strip().upper()
    weekdays = () if recurrence in {"daily", "one_time"} else ((DAY_INDEX[day],) if day in DAY_INDEX else ())
    values: dict[str, Any] = {
        "local_net_schedule_key": schedule_key or new_local_net_schedule_key(),
        "name": str(getattr(entry, "name", None) or net_session_key),
        "service": session.service,
        "recurrence": recurrence,
        "local_start_time": str(session.local_start_time or ""),
        "timezone_name": str(session.timezone or "UTC"),
        "duration_minutes": int(session.duration_minutes or 60),
        "weekdays": weekdays,
        "month_weeks": (1,) if recurrence == "periodic" else (),
        "biweekly_anchor_date": session.effective_start_date if recurrence == "biweekly" else None,
        "one_time_local_date": session.effective_start_date if recurrence == "one_time" else None,
        "effective_start_date": session.effective_start_date,
        "effective_end_date": session.effective_end_date,
        "exception_dates": session.exception_dates,
        "reminder_minutes": int(session.reminder_minutes or 15),
        "net_entry_key": session.net_entry_key,
        "net_session_key": session.net_session_key,
        "frequency_resource_key": session.frequency_resource_key,
        "operating_group_key": operating_group_key or default_group_key,
        "operating_group_name": operating_group_name or default_group_name,
        "accepted_session_version_hash": session.version_hash,
        "accepted_resource_version_hash": getattr(frequency, "version_hash", None),
        "accepted_snapshot_json": _snapshot(session, frequency),
    }
    values.update(dict(overrides or {}))
    return LocalNetSchedule(**values)


def _catalog_payload(value: object) -> dict[str, object]:
    payload = asdict(value)
    for name in ("content_hash", "version_hash", "content_version", "created_utc", "updated_utc"):
        payload.pop(name, None)
    return payload


def _changed_fields(snapshot: object, current: Mapping[str, object]) -> tuple[str, ...]:
    if not isinstance(snapshot, Mapping):
        return ()
    return tuple(
        name
        for name in sorted(snapshot)
        if json.dumps(snapshot.get(name), sort_keys=True, default=list)
        != json.dumps(current.get(name), sort_keys=True, default=list)
    )


def _resource_status_from_values(
    schedule: LocalNetSchedule,
    frequency: FrequencyResource | None,
    session: NetDirectorySession | None,
) -> LocalNetResourceStatus:
    if not schedule.frequency_resource_key:
        return LocalNetResourceStatus("missing", "Choose a frequency resource before enabling this reminder.")
    if frequency is None:
        return LocalNetResourceStatus("missing", "The selected frequency resource is unavailable.")
    if frequency.retired or not frequency.active:
        return LocalNetResourceStatus("retired", "The selected frequency resource is retired; the accepted reminder remains unchanged.")
    changed: list[str] = []
    try:
        snapshot = json.loads(schedule.accepted_snapshot_json or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        snapshot = {}
    if schedule.net_session_key:
        if session is None:
            return LocalNetResourceStatus("missing", "The published session is unavailable; the accepted reminder remains unchanged.")
        if session.retired or not session.active:
            return LocalNetResourceStatus("retired", "The published session is retired; the accepted reminder remains unchanged.")
        changed.extend(_changed_fields(snapshot.get("session"), _catalog_payload(session)))
        session_changed = bool(
            schedule.accepted_session_version_hash
            and schedule.accepted_session_version_hash != session.version_hash
        )
    else:
        session_changed = False
    changed.extend(_changed_fields(snapshot.get("frequency"), _catalog_payload(frequency)))
    frequency_changed = bool(
        schedule.accepted_resource_version_hash
        and schedule.accepted_resource_version_hash != frequency.version_hash
    )
    if session_changed or frequency_changed:
        return LocalNetResourceStatus("update_available", "Catalog changes are available for review; local values were not changed.", tuple(dict.fromkeys(changed)))
    return LocalNetResourceStatus("current", "Catalog references are current.")


def resource_statuses(
    store: ResourceCatalogStore,
    schedules: Iterable[LocalNetSchedule],
) -> Mapping[str, LocalNetResourceStatus]:
    """Resolve catalog status for a bounded schedule set without N+1 reads."""
    rows = tuple(schedules)
    frequencies = store.frequencies_by_keys(
        row.frequency_resource_key for row in rows if row.frequency_resource_key
    )
    sessions = store.sessions_by_keys(
        row.net_session_key for row in rows if row.net_session_key
    )
    return {
        row.local_net_schedule_key: _resource_status_from_values(
            row,
            frequencies.get(row.frequency_resource_key or ""),
            sessions.get(row.net_session_key or ""),
        )
        for row in rows
    }


def resource_status(store: ResourceCatalogStore, schedule: LocalNetSchedule) -> LocalNetResourceStatus:
    return resource_statuses(store, (schedule,))[schedule.local_net_schedule_key]


__all__ = [
    "LocalNetResourceStatus",
    "new_local_net_schedule_key",
    "resource_status",
    "resource_statuses",
    "schedule_from_directory_session",
    "schedule_with_accepted_frequency",
]
