"""Bounded Shortwave listing queries and deterministic UTC schedule evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Mapping, Sequence

from freqinout.core.shortwave_models import ShortwaveDataset, ShortwaveEntry
from freqinout.core.shortwave_store import SHORTWAVE_MAX_RESULTS, ShortwaveStore


_FREQUENCY_RE = re.compile(r"^\s*([0-9]+(?:[.,][0-9]+)?)\s*(hz|khz|mhz)?\s*$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ShortwaveQuery:
    search: str = ""
    timing: str = "scheduled_now"
    classifications: tuple[str, ...] = ("broadcast",)
    language: str = ""
    target: str = ""
    dataset_key: str | None = None
    frequency_min_hz: int | None = None
    frequency_max_hz: int | None = None
    soon_minutes: int = 120
    limit: int = SHORTWAVE_MAX_RESULTS


@dataclass(frozen=True, slots=True)
class ShortwaveListingView:
    entry: ShortwaveEntry
    listing_state: str
    why: str
    next_start_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class ShortwaveQueryResult:
    dataset: ShortwaveDataset | None
    rows: tuple[ShortwaveListingView, ...]
    requested_at_utc: datetime
    bounded_limit: int
    examined_count: int
    dictionaries: Mapping[str, Mapping[str, str]] = field(default_factory=dict)


def normalize_frequency_search(value: str) -> int | None:
    """Interpret conventional shortwave frequency input without locale grouping.

    A decimal bare value is MHz (``9.955``); an integer bare value is kHz
    (``9955``). Explicit Hz/kHz/MHz suffixes override that convention.
    """

    match = _FREQUENCY_RE.fullmatch(str(value or ""))
    if match is None:
        return None
    raw = match.group(1).replace(",", ".")
    unit = (match.group(2) or "").lower()
    try:
        number = Decimal(raw)
        if not unit:
            unit = "mhz" if "." in raw else "khz"
        multiplier = {"hz": Decimal(1), "khz": Decimal(1000), "mhz": Decimal(1_000_000)}[unit]
        hertz = number * multiplier
        if hertz <= 0 or hertz != hertz.to_integral_value():
            return None
        return int(hertz)
    except (InvalidOperation, ValueError, KeyError):
        return None


def query_shortwave(
    store: ShortwaveStore,
    query: ShortwaveQuery,
    *,
    now_utc: datetime | None = None,
) -> ShortwaveQueryResult:
    now = _as_utc(now_utc or datetime.now(timezone.utc))
    dataset = store.get_dataset(query.dataset_key) if query.dataset_key else store.current_dataset()
    limit = max(1, min(SHORTWAVE_MAX_RESULTS, int(query.limit)))
    if dataset is None or dataset.state not in {"current", "superseded"}:
        return ShortwaveQueryResult(None, (), now, limit, 0, {})
    dictionaries = store.dictionaries(dataset.dataset_key)
    classifications = _normalized_classifications(query.classifications)
    exact_frequency = normalize_frequency_search(query.search)
    text_search = "" if exact_frequency is not None else str(query.search or "").strip()
    timing = str(query.timing or "scheduled_now").strip().lower()
    if timing == "all_listings":
        entries = store.list_entries(
            dataset_key=dataset.dataset_key,
            search=text_search,
            language=query.language,
            target=query.target,
            classifications=classifications,
            frequency_min_hz=exact_frequency if exact_frequency is not None else query.frequency_min_hz,
            frequency_max_hz=exact_frequency if exact_frequency is not None else query.frequency_max_hz,
            limit=limit,
        )
        rows = tuple(
            ShortwaveListingView(entry, "Listed", "Matches the selected source and filters; schedule state was not asserted.")
            for entry in entries
        )
        return ShortwaveQueryResult(dataset, rows, now, limit, len(entries), dictionaries)

    soon = max(1, min(24 * 60, int(query.soon_minutes)))
    window_end = now + timedelta(minutes=soon)
    candidates = store.list_time_candidates(
        dataset.dataset_key,
        start_minute=now.hour * 60 + now.minute,
        end_minute=(now.hour * 60 + now.minute) + soon,
        search=text_search,
        language=query.language,
        target=query.target,
        classifications=classifications,
        frequency_hz=exact_frequency,
        frequency_min_hz=query.frequency_min_hz,
        frequency_max_hz=query.frequency_max_hz,
        timing="now" if timing == "scheduled_now" else "soon",
    )
    rows: list[ShortwaveListingView] = []
    for entry in candidates:
        evaluated = evaluate_listing(entry, dataset, now, window_end)
        if evaluated is None:
            continue
        if timing == "scheduled_now" and evaluated.listing_state != "Scheduled now":
            continue
        if timing == "starting_soon" and evaluated.listing_state != "Starting soon":
            continue
        rows.append(evaluated)
    rows.sort(key=lambda item: (item.next_start_utc or now, item.entry.candidate.frequency_hz, item.entry.candidate.station_name.casefold()))
    return ShortwaveQueryResult(dataset, tuple(rows[:limit]), now, limit, len(candidates), dictionaries)


def evaluate_listing(
    entry: ShortwaveEntry,
    dataset: ShortwaveDataset,
    now_utc: datetime,
    window_end_utc: datetime,
) -> ShortwaveListingView | None:
    candidate = entry.candidate
    if candidate.parse_state != "complete" or candidate.start_minute_utc is None or candidate.end_minute_utc is None:
        return None
    now = _as_utc(now_utc)
    window_end = _as_utc(window_end_utc)
    season_start = _parse_utc(dataset.season_effective_from_utc)
    season_end = _parse_utc(dataset.season_effective_to_utc)
    if season_start is None or season_end is None or season_end <= season_start:
        return None
    for service_day in (now.date() - timedelta(days=1), now.date(), now.date() + timedelta(days=1)):
        if not _date_rule_allows(candidate.start_date_normalized, candidate.stop_date_normalized, service_day, season_start.date(), season_end.date()):
            continue
        if candidate.weekday_mask is None or not candidate.weekday_mask & (1 << service_day.weekday()):
            continue
        start = datetime.combine(service_day, time.min, tzinfo=timezone.utc) + timedelta(minutes=candidate.start_minute_utc)
        end = datetime.combine(service_day, time.min, tzinfo=timezone.utc) + timedelta(minutes=candidate.end_minute_utc)
        if candidate.crosses_midnight or candidate.end_minute_utc == 1440:
            if end <= start:
                end += timedelta(days=1)
        if start <= now < end:
            return ShortwaveListingView(entry, "Scheduled now", "The current UTC time, weekday, season, and parsed provider date rule match this listing.", start)
        if now < start <= window_end:
            return ShortwaveListingView(entry, "Starting soon", "The next parsed provider start falls inside the selected upcoming window.", start)
    return None


def _date_rule_allows(
    start_rule: str | None,
    stop_rule: str | None,
    service_day: date,
    season_start: date,
    season_end: date,
) -> bool:
    if not season_start <= service_day <= season_end:
        return False
    start_date = _rule_date(start_rule, season_start, season_end, after=None)
    stop_date = _rule_date(stop_rule, season_start, season_end, after=start_date)
    if start_date is not None and service_day < start_date:
        return False
    if stop_date is not None and service_day > stop_date:
        return False
    return True


def _rule_date(rule: str | None, season_start: date, season_end: date, *, after: date | None) -> date | None:
    if not rule:
        return None
    try:
        month, day = (int(part) for part in rule.removeprefix("--").split("-", 1))
    except (TypeError, ValueError):
        return None
    lower = after or season_start
    for year in range(season_start.year, season_end.year + 1):
        try:
            value = date(year, month, day)
        except ValueError:
            continue
        if lower <= value <= season_end:
            return value
    return None


def _normalized_classifications(values: Sequence[str]) -> tuple[str, ...]:
    allowed = {"broadcast", "utility", "time_standard", "other"}
    return tuple(dict.fromkeys(str(value).strip().lower() for value in values if str(value).strip().lower() in allowed))


def _parse_utc(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


__all__ = [
    "ShortwaveListingView",
    "ShortwaveQuery",
    "ShortwaveQueryResult",
    "evaluate_listing",
    "normalize_frequency_search",
    "query_shortwave",
]
