from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from freqinout.core.message_summary import normalize_message_source_family


DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class ComposeIntent:
    mode: str = "js8"
    transport: str = "js8"
    target: str = ""
    recipient_callsign: str = ""
    group: str = ""
    body: str = ""
    source: str = ""
    source_family: str = ""
    source_ref: str = ""
    title: str = ""
    topic: str = ""
    state: str = ""
    grid: str = ""
    radio_short_name: str = ""
    age_filter_seconds: int = DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        data = {
            "mode": self.mode,
            "transport": self.transport,
            "target": self.target,
            "recipient_callsign": self.recipient_callsign,
            "group": self.group,
            "body": self.body,
            "source": self.source,
            "source_family": self.source_family,
            "source_ref": self.source_ref,
            "title": self.title,
            "topic": self.topic,
            "state": self.state,
            "grid": self.grid,
            "radio_short_name": self.radio_short_name,
            "age_filter_seconds": self.age_filter_seconds,
        }
        data.update(dict(self.metadata or {}))
        return {key: value for key, value in data.items() if value not in ("", None, (), {})}


@dataclass(frozen=True)
class MapContextFilter:
    group_filter: str = ""
    topic_filter: str = ""
    query_filter: str = ""
    source_family: str = ""
    state_filter: str = ""
    grid_filter: str = ""
    fema_region_filter: str = ""
    age_filter_seconds: int = DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS
    concern_only: bool = False

    @property
    def has_focus(self) -> bool:
        return any(
            (
                self.group_filter,
                self.topic_filter,
                self.query_filter,
                self.source_family,
                self.state_filter,
                self.grid_filter,
                self.fema_region_filter,
                self.concern_only,
            )
        )

    def as_messages_kwargs(self) -> dict[str, Any]:
        return {
            "group_filter": self.group_filter,
            "topic_filter": self.topic_filter,
            "query_filter": self.query_filter,
            "source_family": self.source_family,
            "state_filter": self.state_filter,
            "grid_filter": self.grid_filter,
            "fema_region_filter": self.fema_region_filter,
            "age_filter_seconds": self.age_filter_seconds,
            "concern_only": self.concern_only,
        }

    def as_map_kwargs(self) -> dict[str, str]:
        return {
            "group_filter": self.group_filter,
            "topic_filter": self.topic_filter,
            "query_filter": self.query_filter,
            "state_filter": self.state_filter,
            "grid_filter": self.grid_filter,
        }


@dataclass(frozen=True)
class ScheduleWindow:
    group: str = ""
    net_name: str = ""
    band: str = ""
    frequency: str = ""
    starts_at: str = ""
    ends_at: str = ""
    state: str = ""
    grid: str = ""
    action_label: str = ""
    source_ref: str = ""

    @property
    def headline(self) -> str:
        parts = [self.starts_at, self.group or self.net_name, self.band or self.frequency]
        return " | ".join(part for part in parts if part) or "No scheduled action"

    def as_context_kwargs(self) -> dict[str, Any]:
        return {
            "group_filter": self.group,
            "query_filter": self.net_name,
            "state_filter": self.state,
            "grid_filter": self.grid,
        }


@dataclass(frozen=True)
class RfReadiness:
    summary: str = ""
    recommended_band: str = ""
    recommended_frequency: str = ""
    watch_band: str = ""
    watch_frequency: str = ""
    confidence_label: str = ""
    source_ref: str = ""
    details_available: bool = False

    @property
    def compact_label(self) -> str:
        if self.summary:
            return self.summary
        now = " ".join(part for part in (self.recommended_band, self.recommended_frequency) if part)
        watch = " ".join(part for part in (self.watch_band, self.watch_frequency) if part)
        if now and watch:
            return f"Use {now}; watch {watch}."
        if now:
            return f"Use {now}."
        return "RF readiness unavailable."


@dataclass(frozen=True)
class SetupChecklistItem:
    key: str
    label: str
    status: str = "unknown"
    required: bool = False
    source_family: str = ""
    target_screen: str = ""
    action_label: str = ""
    detail: str = ""

    @property
    def complete(self) -> bool:
        return self.status.strip().lower() in {"ok", "ready", "complete", "completed", "dismissed"}

    @property
    def blocks_operations(self) -> bool:
        return self.required and not self.complete


@dataclass(frozen=True)
class StationCommandRadio:
    short_name: str
    radio_id: int = 0
    group: str = ""
    band: str = ""
    frequency: str = ""
    health_label: str = ""
    next_label: str = ""
    plan_label: str = ""
    actions: tuple[str, ...] = ()

    @property
    def card_title(self) -> str:
        return self.short_name


def compose_intent_from_mapping(value: Mapping[str, Any] | None) -> ComposeIntent:
    data = dict(value or {})
    mode = _compose_mode(data.get("mode") or data.get("transport") or data.get("source_family"))
    transport = _compose_mode(data.get("transport") or mode)
    source_family = normalize_message_source_family(data.get("source_family") or data.get("source") or transport)
    target = _clean_target(data.get("target") or data.get("recipient_callsign") or data.get("callsign"))
    recipient = _clean_target(data.get("recipient_callsign") or target)
    age_seconds = _int_or_default(data.get("age_filter_seconds") or data.get("recency_seconds"), DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS)
    known_keys = {
        "mode",
        "transport",
        "target",
        "recipient_callsign",
        "callsign",
        "group",
        "body",
        "message",
        "source",
        "source_family",
        "source_ref",
        "title",
        "topic",
        "topic_filter",
        "state",
        "state_filter",
        "grid",
        "grid_filter",
        "radio_short_name",
        "radio_label",
        "age_filter_seconds",
        "recency_seconds",
    }
    metadata = {str(k): v for k, v in data.items() if str(k) not in known_keys and v not in ("", None, (), {})}
    return ComposeIntent(
        mode=mode,
        transport=transport,
        target=target,
        recipient_callsign=recipient,
        group=_clean_group(data.get("group")),
        body=str(data.get("body") or data.get("message") or ""),
        source=str(data.get("source") or "").strip(),
        source_family=source_family,
        source_ref=str(data.get("source_ref") or "").strip(),
        title=str(data.get("title") or "").strip(),
        topic=str(data.get("topic") or data.get("topic_filter") or "").strip(),
        state=str(data.get("state") or data.get("state_filter") or "").strip().upper(),
        grid=str(data.get("grid") or data.get("grid_filter") or "").strip().upper(),
        radio_short_name=str(data.get("radio_short_name") or data.get("radio_label") or "").strip(),
        age_filter_seconds=age_seconds,
        metadata=metadata,
    )


def map_context_from_mapping(value: Mapping[str, Any] | None) -> MapContextFilter:
    data = dict(value or {})
    source_family = normalize_message_source_family(data.get("source_family") or "")
    query = str(data.get("query_filter") or data.get("search_query") or data.get("callsign") or "").strip().lstrip("@")
    return MapContextFilter(
        group_filter=_clean_group(data.get("group_filter") or data.get("group") or data.get("to_target")),
        topic_filter=str(data.get("topic_filter") or data.get("topic") or "").strip(),
        query_filter=query,
        source_family="" if source_family == "unknown" else source_family,
        state_filter=str(data.get("state_filter") or data.get("state") or "").strip().upper(),
        grid_filter=str(data.get("grid_filter") or data.get("grid") or "").strip().upper(),
        fema_region_filter=str(data.get("fema_region_filter") or data.get("fema_region") or "").strip().upper(),
        age_filter_seconds=_int_or_default(data.get("age_filter_seconds") or data.get("recency_seconds"), DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS),
        concern_only=_bool(data.get("concern_only")),
    )


def compose_intent_from_map_context(context: MapContextFilter, *, mode: str = "", body_prefix: str = "") -> ComposeIntent:
    target = _clean_target(context.query_filter or context.group_filter)
    topic = str(context.topic_filter or "").strip()
    body = body_prefix or (f"RE {topic}: " if topic else "")
    return ComposeIntent(
        mode=_compose_mode(mode or context.source_family or "js8"),
        transport=_compose_mode(mode or context.source_family or "js8"),
        target=target,
        recipient_callsign=target,
        group=context.group_filter,
        body=body,
        source="context",
        source_family=context.source_family,
        topic=topic,
        state=context.state_filter,
        grid=context.grid_filter,
        age_filter_seconds=context.age_filter_seconds,
    )


def station_command_radio_from_mapping(value: Mapping[str, Any]) -> StationCommandRadio:
    data = dict(value or {})
    short_name = str(data.get("short_name") or data.get("label") or data.get("name") or data.get("radio_short_name") or "").strip()
    if " - " in short_name:
        short_name = short_name.split(" - ", 1)[0].strip()
    return StationCommandRadio(
        short_name=short_name,
        radio_id=_int_or_default(data.get("radio_id") or data.get("id") or data.get("profile_id"), 0),
        group=_clean_group(data.get("group") or data.get("operating_group")),
        band=str(data.get("band") or "").strip().upper(),
        frequency=str(data.get("frequency") or data.get("freq") or "").strip(),
        health_label=str(data.get("health_label") or data.get("health") or "").strip(),
        next_label=str(data.get("next_label") or data.get("next") or "").strip(),
        plan_label=str(data.get("plan_label") or data.get("plan") or "").strip(),
        actions=tuple(str(action or "").strip() for action in data.get("actions", ()) if str(action or "").strip())
        if isinstance(data.get("actions", ()), (list, tuple))
        else (),
    )


def schedule_window_from_mapping(value: Mapping[str, Any] | None) -> ScheduleWindow:
    data = dict(value or {})
    return ScheduleWindow(
        group=_clean_group(data.get("group") or data.get("operating_group")),
        net_name=str(data.get("net_name") or data.get("name") or data.get("title") or "").strip(),
        band=str(data.get("band") or "").strip().upper(),
        frequency=str(data.get("frequency") or data.get("freq") or "").strip(),
        starts_at=str(data.get("starts_at") or data.get("start") or data.get("when") or "").strip(),
        ends_at=str(data.get("ends_at") or data.get("end") or "").strip(),
        state=str(data.get("state") or data.get("state_filter") or "").strip().upper(),
        grid=str(data.get("grid") or data.get("grid_filter") or "").strip().upper(),
        action_label=str(data.get("action_label") or data.get("action") or "").strip(),
        source_ref=str(data.get("source_ref") or data.get("schedule_ref") or "").strip(),
    )


def rf_readiness_from_mapping(value: Mapping[str, Any] | None) -> RfReadiness:
    data = dict(value or {})
    return RfReadiness(
        summary=str(data.get("summary") or data.get("headline") or "").strip(),
        recommended_band=str(data.get("recommended_band") or data.get("band") or "").strip().upper(),
        recommended_frequency=str(data.get("recommended_frequency") or data.get("frequency") or data.get("freq") or "").strip(),
        watch_band=str(data.get("watch_band") or "").strip().upper(),
        watch_frequency=str(data.get("watch_frequency") or "").strip(),
        confidence_label=str(data.get("confidence_label") or data.get("confidence") or "").strip(),
        source_ref=str(data.get("source_ref") or data.get("forecast_ref") or "").strip(),
        details_available=_bool(data.get("details_available") or data.get("has_details")),
    )


def setup_checklist_item_from_mapping(value: Mapping[str, Any] | None) -> SetupChecklistItem:
    data = dict(value or {})
    key = str(data.get("key") or data.get("id") or data.get("setting") or "").strip()
    label = str(data.get("label") or data.get("title") or key or "Setup item").strip()
    return SetupChecklistItem(
        key=key,
        label=label,
        status=str(data.get("status") or "").strip().lower() or "unknown",
        required=_bool(data.get("required")),
        source_family=normalize_message_source_family(data.get("source_family") or data.get("source") or ""),
        target_screen=str(data.get("target_screen") or data.get("screen") or "").strip(),
        action_label=str(data.get("action_label") or data.get("action") or "").strip(),
        detail=str(data.get("detail") or data.get("description") or "").strip(),
    )


def _compose_mode(value: object) -> str:
    raw = str(value or "js8").strip().lower()
    normalized = normalize_message_source_family(raw)
    if raw in {"commstat", "commstat_rf"} or normalized == "commstat":
        return "commstat_rf"
    if raw in {"spotter", "js8spotter", "fiospotter"} or normalized == "spotter":
        return "spotter"
    if raw in {"flmsg", "flamp", "nbems", "forms", "fastlight"} or normalized in {"flmsg", "flamp"}:
        return "nbems"
    if raw in {"varac", "bbs"} or normalized == "varac":
        return "varac"
    return "js8"


def _clean_target(value: object) -> str:
    return str(value or "").strip().upper().lstrip("@")


def _clean_group(value: object) -> str:
    return str(value or "").strip().upper().lstrip("@")


def _int_or_default(value: object, default: int) -> int:
    try:
        parsed = int(value or 0)
    except Exception:
        parsed = 0
    return parsed if parsed > 0 else int(default)


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}
