"""Deterministic bounded recurrence projection for non-commandable Local Nets."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from freqinout.core.local_net_models import LocalNetOccurrence, LocalNetSchedule


MAX_HORIZON_DAYS = 90
MAX_OCCURRENCES = 500


def _date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _clock(value: str) -> time:
    try:
        parsed = time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("local_start_time must be HH:MM or HH:MM:SS") from exc
    return parsed.replace(tzinfo=None)


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown IANA timezone: {name}") from exc


def _localize(day: date, clock: time, zone: ZoneInfo) -> datetime | None:
    """Use the first fold; skip wall times that do not exist during a DST gap."""
    naive = datetime.combine(day, clock)
    candidate = naive.replace(tzinfo=zone, fold=0)
    round_trip = candidate.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None)
    return candidate if round_trip == naive else None


def _applies(schedule: LocalNetSchedule, day: date) -> bool:
    start = _date(schedule.effective_start_date)
    end = _date(schedule.effective_end_date)
    if (start and day < start) or (end and day > end) or day.isoformat() in schedule.exception_dates:
        return False
    if schedule.recurrence == "daily":
        return True
    if schedule.recurrence == "one_time":
        return day == _date(schedule.one_time_local_date)
    if day.weekday() not in schedule.weekdays:
        return False
    if schedule.recurrence == "weekly":
        return True
    if schedule.recurrence == "periodic":
        return ((day.day - 1) // 7) + 1 in schedule.month_weeks
    anchor = _date(schedule.biweekly_anchor_date)
    if anchor is None:
        return False
    anchor_monday = anchor - timedelta(days=anchor.weekday())
    day_monday = day - timedelta(days=day.weekday())
    return ((day_monday - anchor_monday).days // 7) % 2 == 0


def occurrence_key(schedule_key: str, start_utc: datetime) -> str:
    stamp = start_utc.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return f"{schedule_key}|{stamp}"


def project_occurrences(
    schedule: LocalNetSchedule,
    window_start_utc: datetime,
    *,
    horizon_days: int = MAX_HORIZON_DAYS,
    limit: int = MAX_OCCURRENCES,
) -> tuple[LocalNetOccurrence, ...]:
    """Project only the requested bounded horizon; never materialize a series."""
    if window_start_utc.tzinfo is None:
        raise ValueError("window_start_utc must be timezone-aware")
    start_utc = window_start_utc.astimezone(timezone.utc)
    days = max(0, min(MAX_HORIZON_DAYS, int(horizon_days)))
    row_limit = max(1, min(MAX_OCCURRENCES, int(limit)))
    if not schedule.enabled or days == 0:
        return ()
    end_utc = start_utc + timedelta(days=days)
    zone = _zone(schedule.timezone_name)
    clock = _clock(schedule.local_start_time)
    first_day = start_utc.astimezone(zone).date() - timedelta(days=1)
    last_day = end_utc.astimezone(zone).date() + timedelta(days=1)
    if schedule.recurrence == "one_time":
        one_time = _date(schedule.one_time_local_date)
        if one_time is None:
            return ()
        first_day = last_day = one_time
    results: list[LocalNetOccurrence] = []
    day = first_day
    while day <= last_day and len(results) < row_limit:
        if _applies(schedule, day):
            local_start = _localize(day, clock, zone)
            if local_start is not None:
                occurrence_start = local_start.astimezone(timezone.utc)
                occurrence_end = occurrence_start + timedelta(minutes=schedule.duration_minutes)
                if occurrence_end > start_utc and occurrence_start < end_utc:
                    state = "active" if occurrence_start <= start_utc < occurrence_end else "pending"
                    results.append(LocalNetOccurrence(
                        occurrence_key(schedule.local_net_schedule_key, occurrence_start),
                        schedule.local_net_schedule_key,
                        schedule.name,
                        occurrence_start,
                        occurrence_end,
                        local_start,
                        schedule.timezone_name,
                        state=state,
                    ))
        day += timedelta(days=1)
    results.sort(key=lambda item: (item.start_utc, item.local_net_schedule_key))
    return tuple(results[:row_limit])


def next_occurrence(schedule: LocalNetSchedule, now_utc: datetime, *, horizon_days: int = MAX_HORIZON_DAYS) -> LocalNetOccurrence | None:
    rows = project_occurrences(schedule, now_utc, horizon_days=horizon_days)
    return rows[0] if rows else None


__all__ = ["MAX_HORIZON_DAYS", "MAX_OCCURRENCES", "next_occurrence", "occurrence_key", "project_occurrences"]
