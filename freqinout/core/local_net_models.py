"""Immutable, Qt-free Local Net schedule and occurrence models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Mapping


LOCAL_NET_RECURRENCES = {"daily", "weekly", "periodic", "biweekly", "one_time"}


def _required(value: object, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _optional(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class LocalNetSchedule:
    local_net_schedule_key: str
    name: str
    service: str
    recurrence: str
    local_start_time: str
    timezone_name: str
    duration_minutes: int = 60
    weekdays: tuple[int, ...] = field(default_factory=tuple)
    month_weeks: tuple[int, ...] = field(default_factory=tuple)
    biweekly_anchor_date: str | None = None
    one_time_local_date: str | None = None
    effective_start_date: str | None = None
    effective_end_date: str | None = None
    exception_dates: tuple[str, ...] = field(default_factory=tuple)
    reminder_minutes: int = 15
    net_entry_key: str | None = None
    net_session_key: str | None = None
    frequency_resource_key: str | None = None
    operating_group_key: str | None = None
    operating_group_name: str | None = None
    sop_id: int | None = None
    participation_notes: str | None = None
    accepted_session_version_hash: str | None = None
    accepted_resource_version_hash: str | None = None
    accepted_snapshot_json: str | None = None
    enabled: bool = True
    next_occurrence_utc: str | None = None
    created_utc: str | None = None
    updated_utc: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "local_net_schedule_key", _required(self.local_net_schedule_key, "local_net_schedule_key"))
        object.__setattr__(self, "name", _required(self.name, "name"))
        object.__setattr__(self, "service", _required(self.service, "service").upper())
        recurrence = _required(self.recurrence, "recurrence").lower().replace("-", "_").replace(" ", "_")
        recurrence = {"bi_weekly": "biweekly", "onetime": "one_time"}.get(recurrence, recurrence)
        if recurrence not in LOCAL_NET_RECURRENCES:
            raise ValueError(f"unsupported Local Net recurrence: {recurrence}")
        object.__setattr__(self, "recurrence", recurrence)
        object.__setattr__(self, "local_start_time", _required(self.local_start_time, "local_start_time"))
        object.__setattr__(self, "timezone_name", _required(self.timezone_name, "timezone_name"))
        duration = int(self.duration_minutes)
        reminder = int(self.reminder_minutes)
        if duration <= 0:
            raise ValueError("duration_minutes must be positive")
        if reminder < 0:
            raise ValueError("reminder_minutes must not be negative")
        object.__setattr__(self, "duration_minutes", duration)
        object.__setattr__(self, "reminder_minutes", reminder)
        weekdays = tuple(sorted({int(value) for value in self.weekdays}))
        month_weeks = tuple(sorted({int(value) for value in self.month_weeks}))
        if any(value < 0 or value > 6 for value in weekdays):
            raise ValueError("weekdays must use Monday=0 through Sunday=6")
        if any(value < 1 or value > 5 for value in month_weeks):
            raise ValueError("month_weeks must be 1 through 5")
        if recurrence in {"weekly", "periodic", "biweekly"} and not weekdays:
            raise ValueError(f"{recurrence} requires at least one weekday")
        if recurrence == "periodic" and not month_weeks:
            raise ValueError("periodic requires at least one week of month")
        if recurrence == "biweekly" and not _optional(self.biweekly_anchor_date):
            raise ValueError("biweekly requires an anchor date")
        if recurrence == "one_time" and not _optional(self.one_time_local_date):
            raise ValueError("one_time requires a local date")
        object.__setattr__(self, "weekdays", weekdays)
        object.__setattr__(self, "month_weeks", month_weeks)
        object.__setattr__(self, "exception_dates", tuple(sorted({_required(value, "exception_date") for value in self.exception_dates})))
        for name in (
            "biweekly_anchor_date", "one_time_local_date", "effective_start_date",
            "effective_end_date", "net_entry_key", "net_session_key",
            "frequency_resource_key", "operating_group_key", "operating_group_name",
            "participation_notes", "accepted_session_version_hash",
            "accepted_resource_version_hash", "accepted_snapshot_json",
            "next_occurrence_utc", "created_utc", "updated_utc",
        ):
            object.__setattr__(self, name, _optional(getattr(self, name)))
        object.__setattr__(self, "sop_id", None if self.sop_id in (None, "") else int(self.sop_id))
        object.__setattr__(self, "enabled", bool(self.enabled))


@dataclass(frozen=True, slots=True)
class LocalNetOccurrence:
    occurrence_key: str
    local_net_schedule_key: str
    name: str
    start_utc: datetime
    end_utc: datetime
    local_start: datetime
    timezone_name: str
    state: str = "pending"
    dismissed: bool = False
    operator_note: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "occurrence_key", _required(self.occurrence_key, "occurrence_key"))
        object.__setattr__(self, "local_net_schedule_key", _required(self.local_net_schedule_key, "local_net_schedule_key"))
        object.__setattr__(self, "name", _required(self.name, "name"))
        object.__setattr__(self, "start_utc", _utc(self.start_utc, "start_utc"))
        object.__setattr__(self, "end_utc", _utc(self.end_utc, "end_utc"))
        if self.end_utc <= self.start_utc:
            raise ValueError("occurrence end must be after start")
        if self.local_start.tzinfo is None:
            raise ValueError("local_start must be timezone-aware")
        object.__setattr__(self, "state", _required(self.state, "state").lower())
        object.__setattr__(self, "dismissed", bool(self.dismissed))
        object.__setattr__(self, "operator_note", _optional(self.operator_note))


@dataclass(frozen=True, slots=True)
class LocalNetSummary:
    now: LocalNetOccurrence | None
    next: LocalNetOccurrence | None
    today_count: int
    upcoming_count: int
    attention_count: int = 0
    by_schedule: Mapping[str, LocalNetOccurrence] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "today_count", max(0, int(self.today_count)))
        object.__setattr__(self, "upcoming_count", max(0, int(self.upcoming_count)))
        object.__setattr__(self, "attention_count", max(0, int(self.attention_count)))
        object.__setattr__(self, "by_schedule", MappingProxyType(dict(self.by_schedule)))


__all__ = ["LOCAL_NET_RECURRENCES", "LocalNetOccurrence", "LocalNetSchedule", "LocalNetSummary"]
