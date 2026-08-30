from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Mapping, Sequence

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
    recommended_actions: tuple[AwarenessAction, ...] = ()
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
        for observation in observations
    )
    ranked = tuple(sorted(items, key=lambda item: (item.priority, _negative_age(item)), reverse=True))
    topic_rollups = _topic_rollups(observations)
    pinned_awareness = tuple(_pinned_awareness(pin, observations) for pin in normalized_pins)
    return AwarenessSnapshot(
        generated_at_utc=_format_utc(now),
        source_lanes=_normalize_source_lanes(source_lanes),
        recommended_actions=_recommended_actions(ranked, topic_rollups, pinned_awareness),
        attention_items=ranked[:visible_limit],
        more_traffic=ranked[visible_limit:],
        topic_rollups=topic_rollups,
        pins=pinned_awareness,
    )


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
    for profile in radio_profiles:
        if not isinstance(profile, Mapping):
            continue
        source_id = str(profile.get("id") or profile.get("system_key") or profile.get("name") or "").strip()
        if not source_id or source_id in seen:
            continue
        seen.add(source_id)
        short_name = _short_source_name(profile)
        profile_attention = _attention_for_source(source_id, short_name, attention_items)
        is_primary = _truthy(profile.get("runtime_primary"))
        lanes.append(
            OperationalSourceLane(
                source_id=source_id,
                short_name=short_name,
                source_kind=str(profile.get("device_class") or "radio").strip() or "radio",
                now=str(current_label or "").strip() if is_primary else "monitoring",
                next=str(next_label or "").strip() if is_primary else "",
                health="Primary" if is_primary else "Active",
                attention_count=len(profile_attention),
                attention_summary=_lane_attention_summary(profile_attention),
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
        haystack = " ".join(
            str(value or "").strip().lower()
            for value in (
                item.source_ref,
                item.source_family,
                item.group,
                item.summary,
                item.subject,
            )
        )
        if (source_id_norm and source_id_norm in haystack) or (short_norm and short_norm in haystack):
            matches.append(item)
    return tuple(matches)


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
