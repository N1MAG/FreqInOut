from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

from freqinout.core.observation_projection import Observation


HIGH_VALUE_TOPICS: tuple[str, ...] = (
    "wildfire",
    "power",
    "water",
    "medical",
    "comms",
    "weather",
    "security",
    "logistics",
    "general intel",
)

_SOURCE_REPLY_MODES: Mapping[str, str] = {
    "js8call": "js8",
    "js8": "js8",
    "spotter": "spotter",
    "fiospotter": "spotter",
    "commstat": "commstat_rf",
    "commstat_rf": "commstat_rf",
    "varac": "varac",
    "local_report": "local_report",
    "flmsg": "nbems",
    "flamp": "nbems",
    "bbs": "nbems",
}


@dataclass(frozen=True)
class AwarenessAction:
    kind: str
    label: str
    context: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AwarenessPin:
    pin_type: str
    value: str
    label: str = ""


@dataclass(frozen=True)
class PinnedAwareness:
    pin_type: str
    value: str
    label: str
    matched_count: int = 0
    newest_utc: str = ""
    actions: tuple[AwarenessAction, ...] = ()


@dataclass(frozen=True)
class AttentionItem:
    id: str
    source_family: str
    source_ref: str
    callsign: str = ""
    to_target: str = ""
    subject: str = ""
    summary: str = ""
    topics: tuple[str, ...] = ()
    group: str = ""
    state: str = ""
    grid: str = ""
    age_seconds: int | None = None
    priority: int = 0
    pinned: bool = False
    reply_compose_mode: str = ""
    actions: tuple[AwarenessAction, ...] = ()


@dataclass(frozen=True)
class TopicRollup:
    topic: str
    count: int = 0
    source_count: int = 0
    callsign_count: int = 0
    newest_utc: str = ""
    geography_hint: str = ""
    severity: str = "neutral"


@dataclass(frozen=True)
class IncidentStoryline:
    topic: str
    headline: str
    count: int = 0
    source_count: int = 0
    callsign_count: int = 0
    newest_utc: str = ""
    geography_hint: str = ""
    severity: str = "neutral"
    state: str = "watching"
    actions: tuple[AwarenessAction, ...] = ()


@dataclass(frozen=True)
class NeedSummary:
    need_id: str
    source_family: str
    source_ref: str
    summary: str
    category: str = ""
    severity: str = "watch"
    location_hint: str = ""
    requested_by: str = ""
    assigned_to: str = ""
    status: str = "open"
    last_update_utc: str = ""
    actions: tuple[AwarenessAction, ...] = ()


@dataclass(frozen=True)
class SituationSummary:
    headline: str
    active_incidents: tuple[IncidentStoryline, ...] = ()
    top_needs: tuple[NeedSummary, ...] = ()
    handled: tuple[NeedSummary, ...] = ()
    where: tuple[str, ...] = ()
    who: tuple[str, ...] = ()
    next_actions: tuple[AwarenessAction, ...] = ()
    confidence: str = ""


@dataclass(frozen=True)
class SopTimelineItem:
    due_utc: str
    label: str
    group: str = ""
    band: str = ""
    frequency: str = ""
    action_kind: str = ""
    status: str = ""
    source_profile_id: int = 0


@dataclass(frozen=True)
class RfReadinessItem:
    target: str
    best_band: str = ""
    next_band: str = ""
    confidence: str = ""
    reason: str = ""
    peer_schedule_source: str = ""


@dataclass(frozen=True)
class OperationalSourceLane:
    source_id: str
    short_name: str
    source_kind: str = "radio"
    now: str = ""
    next: str = ""
    health: str = ""
    attention_count: int = 0
    attention_summary: str = ""


@dataclass(frozen=True)
class AwarenessSnapshot:
    generated_at_utc: str
    active_radios: tuple[Mapping[str, str], ...] = ()
    source_lanes: tuple[OperationalSourceLane, ...] = ()
    situation_summary: SituationSummary | None = None
    recommended_actions: tuple[AwarenessAction, ...] = ()
    needs_attention: tuple[AttentionItem, ...] = ()
    recent_traffic: tuple[AttentionItem, ...] = ()
    attention_items: tuple[AttentionItem, ...] = ()
    more_traffic: tuple[AttentionItem, ...] = ()
    topic_rollups: tuple[TopicRollup, ...] = ()
    pins: tuple[PinnedAwareness, ...] = ()
    sop_timeline_items: tuple[SopTimelineItem, ...] = ()
    rf_readiness: tuple[RfReadinessItem, ...] = ()
    filters: Mapping[str, str] = field(default_factory=dict)


def build_awareness_snapshot(
    observations: Sequence[Observation],
    *,
    local_callsign: str = "",
    active_groups: Sequence[str] = (),
    next_groups: Sequence[str] = (),
    source_lanes: Sequence[OperationalSourceLane | Mapping[str, object]] = (),
    pins: Sequence[AwarenessPin | Mapping[str, object]] = (),
    visible_attention_limit: int = 7,
    generated_at_utc: dt.datetime | str | None = None,
) -> AwarenessSnapshot:
    """Build the ControlFreq awareness projection from existing observations."""
    now = _coerce_utc(generated_at_utc) or dt.datetime.now(dt.timezone.utc)
    traffic_observations = tuple(
        observation for observation in observations if is_awareness_traffic_observation(observation)
    )
    active_group_set = {_normalize_group(value) for value in active_groups if _normalize_group(value)}
    next_group_set = {_normalize_group(value) for value in next_groups if _normalize_group(value)}
    normalized_pins = tuple(
        normalized_pin
        for pin in pins
        if (normalized_pin := _normalize_pin(pin)) is not None
    )
    visible_limit = max(1, int(visible_attention_limit or 7))

    items = tuple(
        _attention_item(
            observation,
            now=now,
            local_callsign=_normalize_call(local_callsign),
            active_groups=active_group_set,
            next_groups=next_group_set,
            pins=normalized_pins,
        )
        for observation in traffic_observations
    )
    ranked = tuple(sorted(items, key=lambda item: (item.priority, _negative_age(item)), reverse=True))
    needs_attention = tuple(item for item in ranked if _item_needs_attention(item))
    recent_traffic = tuple(item for item in ranked if not _item_needs_attention(item))
    topic_rollups = _topic_rollups(traffic_observations)
    pinned_awareness = tuple(_pinned_awareness(pin, traffic_observations) for pin in normalized_pins)
    normalized_lanes = _normalize_source_lanes(source_lanes)
    recommended_actions = _recommended_actions(needs_attention or ranked, topic_rollups, pinned_awareness)
    return AwarenessSnapshot(
        generated_at_utc=_format_utc(now),
        source_lanes=normalized_lanes,
        situation_summary=_situation_summary(
            needs_attention,
            topic_rollups,
            normalized_lanes,
            recommended_actions,
            recent_items=recent_traffic,
            now=now,
        ),
        recommended_actions=recommended_actions,
        needs_attention=needs_attention,
        recent_traffic=recent_traffic,
        attention_items=needs_attention[:visible_limit],
        more_traffic=needs_attention[visible_limit:] + recent_traffic,
        topic_rollups=topic_rollups,
        pins=pinned_awareness,
    )


def is_awareness_traffic_observation(observation: Observation) -> bool:
    """Return True when an observation belongs in Ops Center traffic attention.

    Source telemetry belongs in source health, diagnostics, or map layers. The
    attention queue is for user/operator traffic and actionable reports.
    """
    source_ref = str(observation.source_ref or observation.observation_id or "").strip().lower()
    subject = str(observation.subject or "").strip().lower()
    summary = str(observation.summary or "").strip().lower()
    provenance = dict(observation.provenance or {})
    if source_ref.startswith("mesh-node:"):
        return False
    if subject.startswith("mesh node:") or summary.startswith("mesh node:"):
        return False
    if str(provenance.get("node_id") or "").strip() and not str(provenance.get("message_id") or "").strip():
        return False
    surfaces = provenance.get("surfaces")
    if isinstance(surfaces, (list, tuple, set)):
        normalized_surfaces = {str(surface or "").strip().lower() for surface in surfaces}
        if normalized_surfaces and "ops_center" not in normalized_surfaces and "ops" not in normalized_surfaces:
            return False
    channel_policy = provenance.get("channel_policy")
    if isinstance(channel_policy, Mapping):
        ops_enabled = channel_policy.get("ops_enabled")
        if isinstance(ops_enabled, bool) and not ops_enabled:
            return False
    return True


def build_radio_source_lanes(
    radio_profiles: Sequence[Mapping[str, object]],
    *,
    current_label: str = "",
    next_label: str = "",
    attention_items: Sequence[AttentionItem] = (),
) -> tuple[OperationalSourceLane, ...]:
    """Build compact operational lanes from active radio profiles."""
    lanes: list[OperationalSourceLane] = []
    seen: set[str] = set()
    assigned_attention_ids: set[str] = set()
    for profile in radio_profiles:
        if not isinstance(profile, Mapping):
            continue
        source_id = str(profile.get("id") or profile.get("system_key") or profile.get("name") or "").strip()
        if not source_id or source_id in seen:
            continue
        seen.add(source_id)
        short_name = _short_source_name(profile)
        profile_attention = _attention_for_source(source_id, short_name, attention_items)
        assigned_attention_ids.update(str(item.id or "") for item in profile_attention if str(item.id or "").strip())
        is_primary = _truthy(profile.get("runtime_primary"))
        lanes.append(
            OperationalSourceLane(
                source_id=source_id,
                short_name=short_name,
                source_kind=str(profile.get("device_class") or "radio").strip() or "radio",
                now=str(current_label or "").strip() if is_primary else "",
                next=str(next_label or "").strip() if is_primary else "",
                health="Primary" if is_primary else "Active",
                attention_count=len(profile_attention),
                attention_summary=_lane_attention_summary(profile_attention),
            )
        )
    for family, family_items in _unassigned_attention_by_family(attention_items, assigned_attention_ids).items():
        if family in seen:
            continue
        lanes.append(
            OperationalSourceLane(
                source_id=family,
                short_name=_source_family_lane_label(family),
                source_kind=family,
                now="traffic",
                next="--",
                health="Data",
                attention_count=len(family_items),
                attention_summary=_lane_attention_summary(family_items),
            )
        )
    return tuple(lanes)


def reply_compose_mode_for_source(source_family: object) -> str:
    return _SOURCE_REPLY_MODES.get(_normalize_source(source_family), "source_family")


def _normalize_source_lanes(
    source_lanes: Sequence[OperationalSourceLane | Mapping[str, object]],
) -> tuple[OperationalSourceLane, ...]:
    lanes: list[OperationalSourceLane] = []
    for lane in source_lanes:
        if isinstance(lane, OperationalSourceLane):
            if lane.short_name:
                lanes.append(lane)
            continue
        if not isinstance(lane, Mapping):
            continue
        short_name = str(lane.get("short_name") or lane.get("name") or lane.get("source_id") or "").strip()
        if not short_name:
            continue
        lanes.append(
            OperationalSourceLane(
                source_id=str(lane.get("source_id") or lane.get("id") or short_name).strip(),
                short_name=short_name,
                source_kind=str(lane.get("source_kind") or "source").strip(),
                now=str(lane.get("now") or "").strip(),
                next=str(lane.get("next") or "").strip(),
                health=str(lane.get("health") or "").strip(),
                attention_count=int(lane.get("attention_count") or 0),
                attention_summary=str(lane.get("attention_summary") or "").strip(),
            )
        )
    return tuple(lanes)


def _short_source_name(profile: Mapping[str, object]) -> str:
    for key in ("short_name", "name", "label", "system_key"):
        text = str(profile.get(key) or "").strip()
        if text:
            return text
    return "Radio"


def _attention_for_source(
    source_id: str,
    short_name: str,
    attention_items: Sequence[AttentionItem],
) -> tuple[AttentionItem, ...]:
    source_id_norm = str(source_id or "").strip().lower()
    short_norm = str(short_name or "").strip().lower()
    matches: list[AttentionItem] = []
    for item in attention_items:
        values = tuple(
            str(value or "").strip().lower()
            for value in (item.source_ref, item.source_family, item.group, item.summary, item.subject)
            if str(value or "").strip()
        )
        haystack = " ".join(values)
        source_id_matches = bool(source_id_norm and source_id_norm in set(values))
        short_name_matches = bool(short_norm and len(short_norm) >= 4 and short_norm in haystack)
        if source_id_matches or short_name_matches:
            matches.append(item)
    return tuple(matches)


def _unassigned_attention_by_family(
    attention_items: Sequence[AttentionItem],
    assigned_attention_ids: set[str],
) -> dict[str, tuple[AttentionItem, ...]]:
    buckets: dict[str, list[AttentionItem]] = {}
    for item in attention_items:
        item_id = str(item.id or "").strip()
        if item_id and item_id in assigned_attention_ids:
            continue
        family = _normalize_source(item.source_family)
        if not family:
            continue
        buckets.setdefault(family, []).append(item)
    return {family: tuple(items) for family, items in buckets.items()}


def _source_family_lane_label(family: str) -> str:
    labels = {
        "js8call": "JS8",
        "spotter": "FIOSpotter",
        "commstat": "CommStat",
        "varac": "VarAC",
        "local_report": "Local",
        "meshcore": "MeshCore",
        "mqtt": "Mesh MQTT",
        "aprs": "APRS",
        "reticulum": "LXMF",
    }
    return labels.get(_normalize_source(family), str(family or "Source").strip() or "Source")


def _lane_attention_summary(items: Sequence[AttentionItem]) -> str:
    if not items:
        return "clear"
    first = items[0]
    callsign = str(first.callsign or "").strip()
    subject = str(first.subject or "").strip()
    topic = ", ".join(tuple(first.topics or ())[:1])
    detail = " ".join(part for part in (callsign, subject or topic) if part).strip()
    if len(items) == 1:
        return detail or "1 item"
    return f"{len(items)} items" + (f"; {detail}" if detail else "")


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    try:
        return int(value or 0) != 0
    except Exception:
        return str(value or "").strip().lower() in {"true", "yes", "on"}


def _recommended_actions(
    ranked_items: Sequence[AttentionItem],
    topic_rollups: Sequence[TopicRollup],
    pins: Sequence[PinnedAwareness],
) -> tuple[AwarenessAction, ...]:
    actions: list[AwarenessAction] = []
    first = ranked_items[0] if ranked_items else None
    if first is not None:
        context = {
            "source_family": first.source_family,
            "source_ref": first.source_ref,
            "callsign": first.callsign,
            "group": first.group or first.to_target,
            "topic": first.topics[0] if first.topics else "",
            "compose_mode": first.reply_compose_mode,
        }
        if first.priority >= 900:
            actions.append(AwarenessAction("reply", "Reply to direct traffic", context))
        elif first.priority >= 700:
            actions.append(AwarenessAction("sop", "Review alert SOP", context))
        else:
            actions.append(AwarenessAction("read", "Review top traffic", context))
    urgent_topic = next((rollup for rollup in topic_rollups if rollup.severity in {"urgent", "important"}), None)
    if urgent_topic is not None:
        actions.append(
            AwarenessAction(
                "map",
                f"Map {urgent_topic.topic}",
                {"topic": urgent_topic.topic, "geography": urgent_topic.geography_hint},
            )
        )
    watched_pin = next((pin for pin in pins if pin.matched_count > 0), None)
    if watched_pin is not None:
        actions.append(
            AwarenessAction(
                "messages",
                f"Review pinned {watched_pin.label}",
                {"pin_type": watched_pin.pin_type, "value": watched_pin.value},
            )
        )
    if not actions:
        actions.append(AwarenessAction("monitor", "Monitor traffic", {}))
    return tuple(actions[:3])


def _situation_summary(
    ranked_items: Sequence[AttentionItem],
    topic_rollups: Sequence[TopicRollup],
    source_lanes: Sequence[OperationalSourceLane],
    recommended_actions: Sequence[AwarenessAction],
    *,
    recent_items: Sequence[AttentionItem] = (),
    now: dt.datetime,
) -> SituationSummary:
    items = tuple(ranked_items or ())
    recent = tuple(recent_items or ())
    rollups = tuple(topic_rollups or ())
    source_names = tuple(lane.short_name for lane in source_lanes if str(lane.short_name or "").strip())
    handled = _need_summaries(items + recent, handled=True)
    if not items:
        monitored = ", ".join(source_names[:4])
        if recent:
            headline = f"No traffic needs attention; {len(recent)} recent item{'s' if len(recent) != 1 else ''}."
        else:
            headline = "No traffic needs attention."
        if monitored:
            headline += f" Monitoring {monitored}."
        return SituationSummary(
            headline=headline,
            handled=handled,
            next_actions=tuple(recommended_actions or ()),
            confidence="0 active incidents | 0 open needs" + (f" | {len(handled)} handled" if handled else ""),
        )

    incidents = _incident_storylines(rollups)
    open_needs = _need_summaries(items, handled=False)
    where = _unique_text(
        value
        for item in items
        for value in (item.state, item.grid, item.group, item.to_target)
        if str(value or "").strip()
    )
    who = _unique_text(item.callsign for item in items if str(item.callsign or "").strip())
    headline = _situation_headline(items, incidents, open_needs)
    confidence_parts = [
        f"{len(incidents)} active incident{'s' if len(incidents) != 1 else ''}",
        f"{len(open_needs)} open need{'s' if len(open_needs) != 1 else ''}",
    ]
    if handled:
        confidence_parts.append(f"{len(handled)} handled")
    return SituationSummary(
        headline=headline,
        active_incidents=incidents,
        top_needs=open_needs,
        handled=handled,
        where=where[:4],
        who=who[:5],
        next_actions=tuple(recommended_actions or ()),
        confidence=" | ".join(confidence_parts),
    )


def _situation_headline(
    items: Sequence[AttentionItem],
    incidents: Sequence[IncidentStoryline],
    open_needs: Sequence[NeedSummary],
) -> str:
    if open_needs:
        top = open_needs[0]
        route = " from ".join(part for part in (top.summary, top.requested_by) if part)
        location = f" near {top.location_hint}" if top.location_hint else ""
        return f"{len(open_needs)} open need{'s' if len(open_needs) != 1 else ''}: {route}{location}."
    top_item = items[0]
    top_topic = incidents[0].topic if incidents else next(iter(top_item.topics), "")
    top_text = _item_display_summary(top_item)
    route = " from ".join(part for part in (top_text, top_item.callsign) if part)
    if top_item.priority >= 700:
        return f"Top attention: {route}."
    if incidents:
        return f"{len(items)} recent item{'s' if len(items) != 1 else ''}; top storyline {top_topic}."
    return f"{len(items)} recent operator traffic item{'s' if len(items) != 1 else ''}."


def _incident_storylines(rollups: Sequence[TopicRollup], *, limit: int = 3) -> tuple[IncidentStoryline, ...]:
    stories: list[IncidentStoryline] = []
    for rollup in rollups[:limit]:
        topic = str(rollup.topic or "").strip()
        if not topic:
            continue
        geo = str(rollup.geography_hint or "").strip()
        headline = f"{topic}: {rollup.count} item{'s' if rollup.count != 1 else ''}"
        if geo:
            headline += f" near {geo}"
        context = {"topic": topic, "geography": geo}
        stories.append(
            IncidentStoryline(
                topic=topic,
                headline=headline,
                count=rollup.count,
                source_count=rollup.source_count,
                callsign_count=rollup.callsign_count,
                newest_utc=rollup.newest_utc,
                geography_hint=geo,
                severity=rollup.severity,
                state="active" if rollup.severity in {"urgent", "important"} else "watching",
                actions=(
                    AwarenessAction("messages", "Inbox", context),
                    AwarenessAction("map", "Map", context),
                    AwarenessAction("sop", "SOP", context),
                ),
            )
        )
    return tuple(stories)


def _need_summaries(
    items: Sequence[AttentionItem],
    *,
    handled: bool,
    limit: int = 3,
) -> tuple[NeedSummary, ...]:
    needs: list[NeedSummary] = []
    for item in items:
        category = _need_category(item)
        if not category:
            continue
        is_handled = _item_looks_handled(item)
        if bool(handled) != bool(is_handled):
            continue
        context = {
            "source_family": item.source_family,
            "source_ref": item.source_ref,
            "callsign": item.callsign,
            "group": item.group or item.to_target,
            "topic": category,
        }
        needs.append(
            NeedSummary(
                need_id=item.id,
                source_family=item.source_family,
                source_ref=item.source_ref,
                summary=_item_display_summary(item),
                category=category,
                severity="urgent" if item.priority >= 700 else "watch",
                location_hint=_item_location_hint(item),
                requested_by=item.callsign,
                status="handled" if handled else "open",
                last_update_utc=_format_item_relative_age(item),
                actions=(
                    AwarenessAction("read", "Inbox", context),
                    AwarenessAction("reply", "Reply", {**context, "compose_mode": item.reply_compose_mode}),
                    AwarenessAction("map", "Map", context),
                ),
            )
        )
        if len(needs) >= limit:
            break
    return tuple(needs)


def _need_category(item: AttentionItem) -> str:
    text = " ".join(
        str(value or "").lower()
        for value in (
            item.subject,
            item.summary,
            *tuple(item.topics or ()),
        )
    )
    topic_map = {
        "water": "Water",
        "medical": "Medical",
        "medic": "Medical",
        "shelter": "Shelter",
        "food": "Food",
        "fuel": "Fuel",
        "power": "Power",
        "comms": "Comms",
        "communications": "Comms",
        "relay": "Relay",
        "rescue": "Rescue",
        "evac": "Evacuation",
        "welfare": "Welfare",
    }
    need_terms = ("need", "needs", "request", "requested", "help", "assist", "shortage", "outage", "relay")
    has_need_language = any(term in text for term in need_terms)
    for needle, category in topic_map.items():
        if needle in text and (has_need_language or needle in {"relay", "rescue", "evac"}):
            return category
    if has_need_language:
        return next(iter(tuple(item.topics or ())), "General Need") or "General Need"
    return ""


def _item_looks_handled(item: AttentionItem) -> bool:
    text = " ".join(str(value or "").lower() for value in (item.subject, item.summary))
    handled_terms = ("handled", "resolved", "complete", "closed", "delivered", "filled", "met", "ack")
    return any(term in text for term in handled_terms)


def _item_location_hint(item: AttentionItem) -> str:
    for value in (item.state, item.grid, item.group, item.to_target):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _item_display_summary(item: AttentionItem) -> str:
    text = str(item.subject or item.summary or "").strip()
    if not text:
        topic = next(iter(tuple(item.topics or ())), "")
        text = topic or "Traffic item"
    return " ".join(text.split())


def _item_needs_attention(item: AttentionItem) -> bool:
    if bool(item.pinned):
        return True
    if int(item.priority or 0) >= 180:
        return True
    topics = {_normalize_topic(topic) for topic in tuple(item.topics or ()) if _normalize_topic(topic)}
    return bool(topics.intersection(HIGH_VALUE_TOPICS))


def _format_item_relative_age(item: AttentionItem) -> str:
    if item.age_seconds is None:
        return ""
    seconds = int(item.age_seconds)
    if seconds < 60:
        return "now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def _unique_text(values: Iterable[object]) -> tuple[str, ...]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = str(value or "").strip()
        key = text.upper()
        if text and key not in seen:
            seen.add(key)
            output.append(text)
    return tuple(output)


def _attention_item(
    observation: Observation,
    *,
    now: dt.datetime,
    local_callsign: str,
    active_groups: set[str],
    next_groups: set[str],
    pins: Sequence[AwarenessPin],
) -> AttentionItem:
    source = _normalize_source(observation.source_family)
    callsign = _normalize_call(observation.from_call)
    target = _normalize_group(observation.to_target)
    topics = tuple(str(topic or "").strip() for topic in observation.observed_topics if str(topic or "").strip())
    group = _first_group(observation)
    pinned = any(_pin_matches_observation(pin, observation) for pin in pins)
    priority = _priority(
        observation,
        local_callsign=local_callsign,
        active_groups=active_groups,
        next_groups=next_groups,
        pinned=pinned,
    )
    context = _action_context(observation, group=group, topic=topics[0] if topics else "")
    actions = [
        AwarenessAction("read", "Read", context),
        AwarenessAction("reply", "Reply", {**context, "compose_mode": reply_compose_mode_for_source(source)}),
    ]
    if topics or observation.grid or observation.state or observation.lat is not None:
        actions.append(AwarenessAction("map", "Map", context))
    if source == "condition_alert" or topics:
        actions.append(AwarenessAction("sop", "SOP", context))
    return AttentionItem(
        id=str(observation.observation_id or observation.source_ref or ""),
        source_family=source,
        source_ref=str(observation.source_ref or ""),
        callsign=callsign,
        to_target=target,
        subject=_subject_line(observation),
        summary=str(observation.summary or "").strip(),
        topics=topics,
        group=group,
        state=str(observation.state or "").strip().upper(),
        grid=str(observation.grid or "").strip().upper(),
        age_seconds=_age_seconds(observation, now),
        priority=priority,
        pinned=pinned,
        reply_compose_mode=reply_compose_mode_for_source(source),
        actions=tuple(actions),
    )


def _priority(
    observation: Observation,
    *,
    local_callsign: str,
    active_groups: set[str],
    next_groups: set[str],
    pinned: bool,
) -> int:
    score = 0
    source = _normalize_source(observation.source_family)
    target = _normalize_group(observation.to_target)
    groups = {_normalize_group(value) for value in observation.groups if _normalize_group(value)}
    topics = {_normalize_topic(value) for value in observation.observed_topics if _normalize_topic(value)}
    if pinned:
        score += 1000
    if local_callsign and target == local_callsign:
        score += 950
    if target and target in active_groups:
        score += 650
    if groups.intersection(active_groups):
        score += 600
    if source == "condition_alert":
        score += 700
    status_blob = " ".join(str(value or "").upper() for value in (observation.status, observation.urgency))
    if "RED" in status_blob or "LEVEL 5" in status_blob or "LEVEL 4" in status_blob:
        score += 450
    elif "YELLOW" in status_blob or "LEVEL 3" in status_blob:
        score += 300
    if bool(observation.operator_attention):
        score += 220
    if groups.intersection(next_groups) or (target and target in next_groups):
        score += 180
    if topics.intersection(HIGH_VALUE_TOPICS):
        score += 140
    if source in {"js8call", "varac"}:
        score += 60
    return score


def _topic_rollups(observations: Sequence[Observation]) -> tuple[TopicRollup, ...]:
    buckets: dict[str, dict[str, object]] = {}
    for observation in observations:
        for raw_topic in observation.observed_topics:
            topic = str(raw_topic or "").strip()
            key = _normalize_topic(topic)
            if not key:
                continue
            bucket = buckets.setdefault(
                key,
                {
                    "topic": topic,
                    "count": 0,
                    "sources": set(),
                    "callsigns": set(),
                    "newest": "",
                    "geographies": [],
                    "urgent": False,
                },
            )
            bucket["count"] = int(bucket["count"]) + 1
            sources = bucket["sources"]
            callsigns = bucket["callsigns"]
            geographies = bucket["geographies"]
            if isinstance(sources, set):
                sources.add(_normalize_source(observation.source_family))
            if isinstance(callsigns, set) and _normalize_call(observation.from_call):
                callsigns.add(_normalize_call(observation.from_call))
            if isinstance(geographies, list):
                geo = str(observation.state or observation.grid or "").strip().upper()
                if geo and geo not in geographies:
                    geographies.append(geo)
            received = str(observation.received_utc or observation.event_utc or "").strip()
            if received and received > str(bucket["newest"] or ""):
                bucket["newest"] = received
            status_blob = " ".join(str(value or "").upper() for value in (observation.status, observation.urgency))
            if "RED" in status_blob or "LEVEL 5" in status_blob or "LEVEL 4" in status_blob:
                bucket["urgent"] = True
    rollups: list[TopicRollup] = []
    for key, bucket in buckets.items():
        sources = bucket["sources"] if isinstance(bucket["sources"], set) else set()
        callsigns = bucket["callsigns"] if isinstance(bucket["callsigns"], set) else set()
        geographies = bucket["geographies"] if isinstance(bucket["geographies"], list) else []
        count = int(bucket["count"])
        severity = "urgent" if bucket["urgent"] else "important" if key in HIGH_VALUE_TOPICS or count > 1 else "neutral"
        rollups.append(
            TopicRollup(
                topic=str(bucket["topic"]),
                count=count,
                source_count=len(sources),
                callsign_count=len(callsigns),
                newest_utc=str(bucket["newest"] or ""),
                geography_hint=", ".join(geographies[:2]),
                severity=severity,
            )
        )
    return tuple(sorted(rollups, key=lambda item: (item.severity == "urgent", item.count, item.source_count, item.newest_utc), reverse=True))


def _pinned_awareness(pin: AwarenessPin, observations: Sequence[Observation]) -> PinnedAwareness:
    matches = [observation for observation in observations if _pin_matches_observation(pin, observation)]
    newest = ""
    for observation in matches:
        stamp = str(observation.received_utc or observation.event_utc or "").strip()
        if stamp > newest:
            newest = stamp
    context = {
        "pin_type": pin.pin_type,
        "value": pin.value,
        "label": pin.label or pin.value,
    }
    actions = (
        AwarenessAction("messages", "Messages", context),
        AwarenessAction("map", "Map", context),
        AwarenessAction("compose", "Compose", context),
        AwarenessAction("sop", "SOP", context),
    )
    return PinnedAwareness(
        pin_type=pin.pin_type,
        value=pin.value,
        label=pin.label or pin.value,
        matched_count=len(matches),
        newest_utc=newest,
        actions=actions,
    )


def _pin_matches_observation(pin: AwarenessPin, observation: Observation) -> bool:
    value = str(pin.value or "").strip()
    if not value:
        return False
    kind = str(pin.pin_type or "").strip().lower()
    if kind == "topic":
        wanted = _normalize_topic(value)
        return any(_normalize_topic(topic) == wanted for topic in observation.observed_topics)
    if kind == "callsign":
        wanted = _normalize_call(value)
        return wanted in {_normalize_call(observation.from_call), _normalize_call(observation.to_target)}
    if kind == "group":
        wanted = _normalize_group(value)
        return wanted in {_normalize_group(observation.to_target), *(_normalize_group(group) for group in observation.groups)}
    return False


def _normalize_pin(value: AwarenessPin | Mapping[str, object]) -> AwarenessPin | None:
    if isinstance(value, AwarenessPin):
        pin_type = str(value.pin_type or "").strip().lower()
        pin_value = str(value.value or "").strip()
        label = str(value.label or "").strip()
    elif isinstance(value, Mapping):
        pin_type = str(value.get("pin_type") or value.get("type") or "").strip().lower()
        pin_value = str(value.get("value") or "").strip()
        label = str(value.get("label") or "").strip()
    else:
        return None
    if pin_type not in {"topic", "callsign", "group"} or not pin_value:
        return None
    return AwarenessPin(pin_type=pin_type, value=pin_value, label=label)


def _action_context(observation: Observation, *, group: str, topic: str) -> Mapping[str, str]:
    return {
        "source_family": _normalize_source(observation.source_family),
        "source_ref": str(observation.source_ref or ""),
        "callsign": _normalize_call(observation.from_call),
        "group": group,
        "topic": str(topic or "").strip(),
        "state": str(observation.state or "").strip().upper(),
        "grid": str(observation.grid or "").strip().upper(),
    }


def _subject_line(observation: Observation) -> str:
    for value in (observation.subject, observation.summary, observation.status):
        text = str(value or "").strip()
        if text:
            return " ".join(text.split())
    source = _normalize_source(observation.source_family).upper() or "TRAFFIC"
    callsign = _normalize_call(observation.from_call) or "UNKNOWN"
    return f"{source} traffic from {callsign}"


def _first_group(observation: Observation) -> str:
    for value in (observation.to_target, *observation.groups):
        group = _normalize_group(value)
        if group:
            return group
    return ""


def _age_seconds(observation: Observation, now: dt.datetime) -> int | None:
    stamp = _coerce_utc(observation.received_utc or observation.event_utc)
    if stamp is None:
        return None
    return max(0, int((now - stamp).total_seconds()))


def _negative_age(item: AttentionItem) -> int:
    if item.age_seconds is None:
        return -999999999
    return -int(item.age_seconds)


def _coerce_utc(value: dt.datetime | str | None) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _format_utc(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_call(value: object) -> str:
    return str(value or "").strip().upper().lstrip("@")


def _normalize_group(value: object) -> str:
    return str(value or "").strip().upper().lstrip("@")


def _normalize_source(value: object) -> str:
    source = str(value or "").strip().lower()
    aliases = {
        "js8": "js8call",
        "js8spotter": "spotter",
        "fiospotter": "spotter",
        "spotter_traffic": "spotter",
        "commstat rf": "commstat",
        "local": "local_report",
    }
    return aliases.get(source, source)


def _normalize_topic(value: object) -> str:
    return str(value or "").strip().lower()
