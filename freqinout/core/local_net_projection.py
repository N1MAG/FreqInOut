"""Immutable, non-commandable Local Net projection for Ops Center."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Mapping

from freqinout.core.local_net_models import LocalNetOccurrence, LocalNetSchedule
from freqinout.core.navigation_intent import NavigationIntent
from freqinout.core.local_net_service import LocalNetResourceStatus, resource_statuses
from freqinout.core.local_net_store import LocalNetStore, MAX_LOCAL_NET_SCHEDULES
from freqinout.core.resource_catalog_models import FrequencyResource
from freqinout.core.resource_catalog_store import ResourceCatalogStore


SOURCE_TYPE_LOCAL_NET = "LOCAL_NET"
MAX_LATER_ITEMS = 50
MISSED_REVIEW_MINUTES = 30


@dataclass(frozen=True, slots=True)
class LocalNetOutlookItem:
    occurrence_id: str
    local_net_schedule_id: str
    source_type: str
    commandable: bool
    net_session_id: str | None
    frequency_resource_id: str | None
    group_id: str | None
    name: str
    group_name: str
    service: str
    where_text: str
    start_utc: datetime
    end_utc: datetime
    timezone_name: str
    reminder_minutes: int
    state: str
    urgency: str
    urgency_text: str
    countdown_seconds: int
    what_text: str
    why_text: str
    source_health: str
    source_health_text: str
    source_changed_fields: tuple[str, ...] = ()
    sop_id: int | None = None
    sop_available: bool = False
    dismissed: bool = False
    action_metadata: Mapping[str, object] = field(default_factory=dict, compare=False, hash=False)

    def __post_init__(self) -> None:
        if self.source_type != SOURCE_TYPE_LOCAL_NET:
            raise ValueError("Local Net outlook source_type must be LOCAL_NET")
        if self.commandable:
            raise ValueError("Local Net outlook items cannot be commandable")
        if self.start_utc.tzinfo is None or self.end_utc.tzinfo is None:
            raise ValueError("Local Net outlook times must be timezone-aware")
        object.__setattr__(self, "action_metadata", MappingProxyType(dict(self.action_metadata)))


@dataclass(frozen=True, slots=True)
class LocalNetOutlookSnapshot:
    generated_utc: datetime
    active: LocalNetOutlookItem | None
    next: LocalNetOutlookItem | None
    later: tuple[LocalNetOutlookItem, ...] = ()
    recent_missed: tuple[LocalNetOutlookItem, ...] = ()
    total_upcoming: int = 0
    attention_count: int = 0

    def __post_init__(self) -> None:
        if self.generated_utc.tzinfo is None:
            raise ValueError("generated_utc must be timezone-aware")
        object.__setattr__(self, "later", tuple(self.later[:MAX_LATER_ITEMS]))
        object.__setattr__(self, "recent_missed", tuple(self.recent_missed[:5]))
        object.__setattr__(self, "total_upcoming", max(0, int(self.total_upcoming)))
        object.__setattr__(self, "attention_count", max(0, int(self.attention_count)))

    @property
    def visible_items(self) -> tuple[LocalNetOutlookItem, ...]:
        rows: list[LocalNetOutlookItem] = list(self.recent_missed)
        if self.active is not None:
            rows.append(self.active)
        if self.next is not None:
            rows.append(self.next)
        rows.extend(self.later)
        unique: dict[str, LocalNetOutlookItem] = {}
        for row in rows:
            unique.setdefault(row.occurrence_id, row)
        return tuple(unique.values())


def _where(resource: FrequencyResource | None) -> str:
    if resource is None:
        return "Frequency not selected"
    if resource.resource_kind == "band_range":
        return f"{resource.lower_hz or 0:,}–{resource.upper_hz or 0:,} Hz"
    receive = resource.receive_hz or resource.center_hz
    transmit = resource.transmit_hz
    if receive is None:
        return "Frequency unavailable"
    if transmit is not None and transmit != receive:
        return f"RX {receive:,} Hz · TX {transmit:,} Hz"
    return f"{receive:,} Hz"


def _urgency(occurrence: LocalNetOccurrence, now: datetime) -> tuple[str, str, int, str]:
    seconds = int((occurrence.start_utc - now).total_seconds())
    if occurrence.start_utc <= now < occurrence.end_utc:
        remaining = max(0, int((occurrence.end_utc - now).total_seconds()))
        minutes = max(1, (remaining + 59) // 60)
        return "active", "Active now", seconds, f"Active now · ends in {minutes}m"
    if occurrence.end_utc <= now:
        minutes = max(0, int((now - occurrence.end_utc).total_seconds()) // 60)
        return "recent", "Recently ended", seconds, f"Ended {minutes}m ago"
    minutes = max(0, (seconds + 59) // 60)
    if seconds <= 15 * 60:
        return "due_15", "Starts within 15 minutes", seconds, f"Starts in {minutes}m"
    if seconds <= 30 * 60:
        return "due_30", "Starts within 30 minutes", seconds, f"Starts in {minutes}m"
    return "upcoming", "Upcoming", seconds, f"Starts in {minutes}m"


def _item(
    occurrence: LocalNetOccurrence,
    schedule: LocalNetSchedule,
    resource: FrequencyResource | None,
    status: LocalNetResourceStatus,
    now: datetime,
) -> LocalNetOutlookItem:
    urgency, urgency_text, countdown, why = _urgency(occurrence, now)
    where_text = _where(resource)
    group_name = schedule.operating_group_name or "Community / Unassigned"
    what = f"{schedule.name} · {group_name} · {schedule.service} · {where_text}"
    health_text = status.message
    if status.state != "current":
        why = f"{why}. Resource review: {health_text}"
    return LocalNetOutlookItem(
        occurrence_id=occurrence.occurrence_key,
        local_net_schedule_id=schedule.local_net_schedule_key,
        source_type=SOURCE_TYPE_LOCAL_NET,
        commandable=False,
        net_session_id=schedule.net_session_key,
        frequency_resource_id=schedule.frequency_resource_key,
        group_id=schedule.operating_group_key,
        name=schedule.name,
        group_name=group_name,
        service=schedule.service,
        where_text=where_text,
        start_utc=occurrence.start_utc,
        end_utc=occurrence.end_utc,
        timezone_name=schedule.timezone_name,
        reminder_minutes=schedule.reminder_minutes,
        state=occurrence.state,
        urgency=urgency,
        urgency_text=urgency_text,
        countdown_seconds=countdown,
        what_text=what,
        why_text=why,
        source_health=status.state,
        source_health_text=health_text,
        source_changed_fields=status.changed_fields,
        sop_id=schedule.sop_id,
        sop_available=schedule.sop_id is not None,
        dismissed=occurrence.dismissed,
        # This mapping is deliberately empty. Command/QSY/radio metadata is not
        # a valid extension point for reminder projections.
        action_metadata={},
    )


def build_local_net_outlook(
    store: LocalNetStore,
    catalog: ResourceCatalogStore,
    now_utc: datetime | None = None,
    *,
    horizon_days: int = 30,
    later_limit: int = MAX_LATER_ITEMS,
    missed_review_minutes: int = MISSED_REVIEW_MINUTES,
) -> LocalNetOutlookSnapshot:
    """Build active/next/later reminder rows with bounded catalog access."""
    now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    review_minutes = max(0, min(120, int(missed_review_minutes)))
    schedules = store.list_schedules(enabled=True, limit=MAX_LOCAL_NET_SCHEDULES)
    by_key = {row.local_net_schedule_key: row for row in schedules}
    statuses = resource_statuses(catalog, schedules)
    resources = catalog.frequencies_by_keys(
        row.frequency_resource_key for row in schedules if row.frequency_resource_key
    )
    occurrences = store.outlook_occurrences(
        now - timedelta(minutes=review_minutes),
        horizon_days=max(1, min(90, int(horizon_days))),
        limit=min(55, max(5, int(later_limit) + 5)),
    )
    projected: list[LocalNetOutlookItem] = []
    for occurrence in occurrences:
        schedule = by_key.get(occurrence.local_net_schedule_key)
        if schedule is None:
            continue
        projected.append(
            _item(
                occurrence,
                schedule,
                resources.get(schedule.frequency_resource_key or ""),
                statuses[schedule.local_net_schedule_key],
                now,
            )
        )
    projected.sort(key=lambda row: (row.start_utc, row.local_net_schedule_id))
    recent = tuple(
        row for row in projected
        if row.end_utc <= now and row.end_utc >= now - timedelta(minutes=review_minutes)
    )[-5:]
    active_rows = tuple(row for row in projected if row.start_utc <= now < row.end_utc)
    future = tuple(row for row in projected if row.start_utc > now)
    active = active_rows[0] if active_rows else None
    following = future[0] if future else None
    cap = max(0, min(MAX_LATER_ITEMS, int(later_limit)))
    later = future[1 : 1 + cap]
    attention = sum(status.state != "current" for status in statuses.values())
    return LocalNetOutlookSnapshot(
        generated_utc=now,
        active=active,
        next=following,
        later=later,
        recent_missed=recent,
        total_upcoming=len(future),
        attention_count=attention,
    )


def build_local_net_sop_intent(
    *,
    local_net_schedule_key: str,
    occurrence_key: str,
    net_session_key: str | None,
    sop_id: int,
    return_route: str = "ops.schedule_outlook",
    return_scroll_y: int = 0,
) -> NavigationIntent:
    """Build a stable, review-only handoff from a reminder to SOP guidance."""
    return NavigationIntent(
        origin_surface="ops.local_nets_outlook",
        destination_route="sop.context",
        return_route=return_route,
        return_scroll_y=return_scroll_y,
        return_selection_key=occurrence_key,
        directory_session_ids=(net_session_key,) if net_session_key else (),
        local_net_schedule_id=local_net_schedule_key,
        sop_id=sop_id,
        readonly_reason="Opened from a Local Net reminder for operator review.",
        draft_snapshot={"occurrence_key": occurrence_key},
    )


__all__ = [
    "MAX_LATER_ITEMS",
    "MISSED_REVIEW_MINUTES",
    "SOURCE_TYPE_LOCAL_NET",
    "LocalNetOutlookItem",
    "LocalNetOutlookSnapshot",
    "build_local_net_outlook",
    "build_local_net_sop_intent",
]
