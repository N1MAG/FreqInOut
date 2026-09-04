from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from freqinout.core.group_utils import normalize_group_name


DISTRIBUTION_ROLES = frozenset({"HUB", "HUB-ALT", "ALT-HUB", "NCS", "ANCS"})
REPORTER_ROLES = frozenset({"PEER"})

_IMPACT_TOPICS = frozenset(
    {
        "comms",
        "communications",
        "damage",
        "evacuation",
        "fire",
        "food",
        "fuel",
        "hazard",
        "medical",
        "power",
        "public safety",
        "shelter",
        "travel/roads",
        "transportation",
        "water",
        "weather",
    }
)
_REQUEST_RE = re.compile(
    r"(?:\?|\b(?:please\s+)?(?:ack(?:nowledge)?|advise|confirm|reply|respond)\b|"
    r"\b(?:reply|response|ack(?:nowledgement)?|confirmation)\s+(?:is\s+)?requested\b|"
    r"\brequest(?:ing|ed)?\s+(?:a\s+)?(?:reply|response|ack(?:nowledgement)?|confirmation)\b)",
    re.IGNORECASE,
)
_GREEN_RE = re.compile(
    r"\b(?:green(?:\s+(?:report|status))?|all\s+clear|operations?\s+(?:steady|normal)|"
    r"no\s+significant\s+(?:issues?|activity)|overall\s+status\s*[:=-]?\s*(?:green|normal|functioning))\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class OperatorTrafficContext:
    callsign: str = ""
    groups: tuple[str, ...] = ()
    roles_by_group: tuple[tuple[str, str], ...] = ()

    @property
    def distribution_groups(self) -> tuple[str, ...]:
        return tuple(
            group
            for group, role in self.roles_by_group
            if _normalize_role(role) in DISTRIBUTION_ROLES
        )

    @property
    def has_distribution_duty(self) -> bool:
        return bool(self.distribution_groups)

    def role_for_group(self, group: object) -> str:
        wanted = _normalize_group(group)
        for candidate, role in self.roles_by_group:
            if candidate == wanted:
                return role
        return ""


@dataclass(frozen=True)
class TrafficActionItem:
    stable_id: str
    primary_bucket: str
    what: str
    why: str
    from_call: str = ""
    target: str = ""
    group: str = ""
    topic: str = ""
    severity: str = "routine"
    received_ts: float = 0.0
    event_oriented: bool = False
    reply_needed: bool = False
    relay_needed: bool = False
    review_needed: bool = False
    social: bool = False
    guidance_by_bucket: tuple[tuple[str, str, str], ...] = ()

    def matches_bucket(self, bucket: object) -> bool:
        key = str(bucket or "").strip().lower()
        if not key or key == "all":
            return True
        return {
            "reply": self.reply_needed,
            "relay": self.relay_needed,
            "review": self.review_needed,
            "social": self.social,
            "event": self.event_oriented,
        }.get(key, self.primary_bucket == key)

    def guidance_for(self, bucket: object = "") -> tuple[str, str]:
        key = str(bucket or self.primary_bucket).strip().lower()
        for candidate, what, why in self.guidance_by_bucket:
            if candidate == key:
                return what, why
        return self.what, self.why


@dataclass(frozen=True)
class TrafficActionSummary:
    items: tuple[TrafficActionItem, ...] = ()

    def count(self, bucket: object) -> int:
        return sum(1 for item in self.items if item.matches_bucket(bucket))

    def items_for(self, bucket: object) -> tuple[TrafficActionItem, ...]:
        return tuple(item for item in self.items if item.matches_bucket(bucket))

    @property
    def lead(self) -> TrafficActionItem | None:
        return self.items[0] if self.items else None


@dataclass(frozen=True)
class TrafficGroupVolume:
    group: str
    sources: tuple[tuple[str, int], ...] = ()
    is_operator_group: bool = False
    unread_count: int = 0
    current_count: int = 0
    previous_count: int = 0
    trend: str = "Steady"
    latest_ts: float = 0.0


def build_operator_traffic_context(
    *,
    callsign: object = "",
    configured_operating_groups: Iterable[object] = (),
    configured_local_groups: Iterable[object] = (),
    operator_rows: Iterable[Mapping[str, object]] = (),
) -> OperatorTrafficContext:
    """Build explicit user/group associations without expanding group hierarchy."""
    own_call = _normalize_call(callsign)
    groups = {
        group
        for group in (
            *(_normalize_group(value) for value in configured_operating_groups),
            *(_normalize_group(value) for value in configured_local_groups),
        )
        if group
    }
    roles: dict[str, str] = {}
    for row in operator_rows:
        if not own_call or _normalize_call(row.get("callsign")) != own_call:
            continue
        row_groups = _groups_from_operator_row(row)
        groups.update(row_groups)
        role = _normalize_role(row.get("group_role"))
        for group in row_groups:
            if role:
                roles[group] = role
    for group in groups:
        roles.setdefault(group, "MEMBER")
    return OperatorTrafficContext(
        callsign=own_call,
        groups=tuple(sorted(groups)),
        roles_by_group=tuple(sorted(roles.items())),
    )


def load_operator_traffic_context(
    db_path: str | Path | None,
    *,
    callsign: object = "",
    configured_operating_groups: Iterable[object] = (),
    configured_local_groups: Iterable[object] = (),
) -> OperatorTrafficContext:
    rows: list[dict[str, object]] = []
    path = Path(db_path) if db_path else None
    own_call = _normalize_call(callsign)
    if path is not None and path.exists() and own_call:
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(path), timeout=1.0)
            conn.row_factory = sqlite3.Row
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='operator_checkins'"
            ).fetchone()
            if table:
                record = conn.execute(
                    """
                    SELECT callsign, group1, group2, group3, groups_json, group_role
                      FROM operator_checkins
                     WHERE UPPER(TRIM(callsign))=?
                     LIMIT 1
                    """,
                    (own_call,),
                ).fetchone()
                if record is not None:
                    rows.append(dict(record))
        except Exception:
            rows = []
        finally:
            if conn is not None:
                conn.close()
    return build_operator_traffic_context(
        callsign=own_call,
        configured_operating_groups=configured_operating_groups,
        configured_local_groups=configured_local_groups,
        operator_rows=rows,
    )


def traffic_action_item(
    message: object,
    context: OperatorTrafficContext,
) -> TrafficActionItem | None:
    target = _normalize_target(_value(message, "to_target", "to_call"))
    group = _normalize_group(_value(message, "group", "group_name"))
    if not group and target in set(context.groups):
        group = target
    direct_to_user = bool(context.callsign and target == context.callsign)
    associated_group = group if group in set(context.groups) else ""
    group_directed = bool(associated_group and target in {"", associated_group})
    relevant = direct_to_user or bool(associated_group)
    if not relevant:
        return None

    severity = _normalize_severity(_value(message, "severity"))
    status = str(_value(message, "status") or "").strip().upper()
    topics = _topics(message)
    text = " ".join(
        str(_value(message, name) or "")
        for name in ("subject", "summary", "search_text", "body_preview", "title")
    )
    actionable = _bool_value(message, "actionable")
    event_oriented = _is_event_oriented(
        message,
        severity=severity,
        status=status,
        topics=topics,
        actionable=actionable,
    )
    request_signal = bool(_REQUEST_RE.search(text))
    explicit_green = bool(_GREEN_RE.search(text))
    if explicit_green and not request_signal and status not in {"ALERT", "RED"}:
        event_oriented = False
    can_reply = _can_reply(message)
    reply_needed = bool(
        can_reply
        and (
            (direct_to_user and event_oriented)
            or (request_signal and (direct_to_user or group_directed))
        )
    )
    duty_group = associated_group if context.role_for_group(associated_group) in DISTRIBUTION_ROLES else ""
    if not duty_group and direct_to_user and context.distribution_groups:
        duty_group = context.distribution_groups[0]
    relay_needed = bool(event_oriented and duty_group and _is_impactful(severity, topics, actionable))
    review_needed = bool(event_oriented and not reply_needed and not relay_needed)
    social = bool(direct_to_user and not event_oriented)
    if not (reply_needed or relay_needed or review_needed or social):
        return None

    sender = _normalize_call(_value(message, "from_call")) or "sender"
    topic = _lead_topic(topics)
    role = context.role_for_group(duty_group)
    guidance: list[tuple[str, str, str]] = []
    if reply_needed:
        reply_why = (
            f"Event traffic addressed to {context.callsign}."
            if event_oriented and direct_to_user
            else f"A reply was requested through associated group {associated_group}."
        )
        guidance.append(("reply", f"Reply to {sender}", reply_why))
    if relay_needed:
        guidance.append(
            (
                "relay",
                f"Distribute {topic or 'impact'} report from {sender}",
                f"{role} duty for {duty_group} includes coordinating and distributing impactful reports.",
            )
        )
    if review_needed:
        review_why = (
            f"Event traffic is addressed to {context.callsign}."
            if direct_to_user
            else f"Event traffic is addressed to associated group {associated_group}."
        )
        guidance.append(("review", f"Review {topic or 'event'} traffic from {sender}", review_why))
    if social:
        guidance.append(
            (
                "social",
                f"Review direct message from {sender}",
                f"It is addressed to {context.callsign}, but no event indicators were detected.",
            )
        )
    bucket = "reply" if reply_needed else "relay" if relay_needed else "review" if review_needed else "social"
    guidance_by_bucket = tuple(guidance)
    what, why = next(
        ((item_what, item_why) for key, item_what, item_why in guidance_by_bucket if key == bucket),
        ("Review traffic", "Relevant traffic needs operator review."),
    )

    return TrafficActionItem(
        stable_id=str(_value(message, "stable_id", "message_id") or ""),
        primary_bucket=bucket,
        what=what,
        why=why,
        from_call=sender,
        target=target,
        group=associated_group,
        topic=topic,
        severity=severity,
        received_ts=_float_value(message, "received_ts", "rcv_ts", "event_ts"),
        event_oriented=event_oriented,
        reply_needed=reply_needed,
        relay_needed=relay_needed,
        review_needed=review_needed,
        social=social,
        guidance_by_bucket=guidance_by_bucket,
    )


def build_traffic_action_summary(
    messages: Iterable[object],
    context: OperatorTrafficContext,
) -> TrafficActionSummary:
    items = [item for message in messages if (item := traffic_action_item(message, context)) is not None]
    items.sort(key=lambda item: (_priority(item), -float(item.received_ts or 0.0), item.stable_id))
    return TrafficActionSummary(tuple(items))


def filter_traffic_messages(
    messages: Iterable[object],
    *,
    age_seconds: object = 0,
    now_ts: float | None = None,
    source_family: object = "",
    group_filter: object = "",
) -> tuple[object, ...]:
    """Apply the shared Ops/Inbox time, source, and group scope to traffic rows."""
    try:
        age = max(0, int(age_seconds or 0))
    except Exception:
        age = 0
    now = float(now_ts if now_ts is not None else time.time())
    minimum_ts = now - age if age else 0.0
    source = _normalize_source(source_family)
    group = _normalize_group(group_filter)
    result: list[object] = []
    for message in messages:
        received = _float_value(message, "received_ts", "rcv_ts", "event_ts")
        if minimum_ts and (not received or received < minimum_ts):
            continue
        if source and _normalize_source(_value(message, "source_family", "origin")) != source:
            continue
        if group:
            message_group = _message_group(message)
            if message_group != group:
                continue
        result.append(message)
    return tuple(result)


def build_traffic_group_volumes(
    messages: Iterable[object],
    *,
    age_seconds: object = 24 * 60 * 60,
    now_ts: float | None = None,
    source_family: object = "",
    group_filter: object = "",
    operator_groups: Iterable[object] = (),
) -> tuple[TrafficGroupVolume, ...]:
    """Summarize current traffic and compare it with the preceding equal window."""
    rows = tuple(messages)
    try:
        age = max(0, int(age_seconds or 0))
    except Exception:
        age = 0
    now = float(now_ts if now_ts is not None else time.time())
    source = _normalize_source(source_family)
    wanted_group = _normalize_group(group_filter)
    associated_groups = {
        normalized
        for normalized in (_normalize_group(value) for value in operator_groups)
        if normalized
    }
    buckets: dict[str, dict[str, object]] = {}
    for message in rows:
        if source and _normalize_source(_value(message, "source_family", "origin")) != source:
            continue
        target = _normalize_target(_value(message, "to_target", "to_call"))
        group = _message_group(message) or ("DIRECT" if _looks_like_callsign(target) else "UNASSIGNED")
        if wanted_group and group != wanted_group:
            continue
        received = _float_value(message, "received_ts", "rcv_ts", "event_ts")
        elapsed = max(0.0, now - received) if received else 0.0
        current = not age or bool(received and elapsed <= age)
        previous = bool(age and received and age < elapsed <= age * 2)
        if not current and not previous:
            continue
        bucket = buckets.setdefault(
            group,
            {"unread": 0, "current": 0, "previous": 0, "latest": 0.0, "sources": {}},
        )
        if current:
            bucket["current"] = int(bucket["current"]) + 1
            bucket["latest"] = max(float(bucket["latest"]), received)
            sources = bucket["sources"]
            if isinstance(sources, dict):
                source_label = _source_label(_value(message, "source_family", "origin"))
                sources[source_label] = int(sources.get(source_label, 0)) + 1
            if _is_unread(message):
                bucket["unread"] = int(bucket["unread"]) + 1
        elif previous:
            bucket["previous"] = int(bucket["previous"]) + 1
    result = [
        TrafficGroupVolume(
            group=group,
            sources=tuple(
                sorted(
                    (
                        (str(source), int(count))
                        for source, count in (
                            values["sources"].items()
                            if isinstance(values.get("sources"), dict)
                            else ()
                        )
                    ),
                    key=lambda item: (-item[1], item[0]),
                )
            ),
            is_operator_group=group in associated_groups,
            unread_count=int(values["unread"]),
            current_count=int(values["current"]),
            previous_count=int(values["previous"]),
            trend=_traffic_trend(int(values["current"]), int(values["previous"]), age),
            latest_ts=float(values["latest"]),
        )
        for group, values in buckets.items()
        if int(values["current"]) > 0
    ]
    result.sort(
        key=lambda item: (
            0 if item.is_operator_group else 1,
            -_trend_rank(item.trend),
            -item.current_count,
            item.group,
        )
    )
    return tuple(result)


def message_matches_traffic_bucket(
    message: object,
    context: OperatorTrafficContext,
    bucket: object,
) -> bool:
    item = traffic_action_item(message, context)
    return bool(item is not None and item.matches_bucket(bucket))


def configured_group_names(settings: object) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Read explicit HF and local group names from a SettingsManager-like object."""
    try:
        hf_rows = settings.get("operating_groups", []) or []
    except Exception:
        hf_rows = []
    try:
        local_rows = settings.get("local_net_profiles", []) or []
    except Exception:
        local_rows = []
    hf = tuple(
        str(row.get("group", "") or "")
        for row in hf_rows
        if isinstance(row, Mapping) and str(row.get("group", "") or "").strip()
    )
    local = tuple(
        str(row.get("group", row.get("name", "")) or "")
        for row in local_rows
        if isinstance(row, Mapping) and str(row.get("group", row.get("name", "")) or "").strip()
    )
    return hf, local


def _groups_from_operator_row(row: Mapping[str, object]) -> set[str]:
    groups = {_normalize_group(row.get(name)) for name in ("group1", "group2", "group3")}
    raw_json = row.get("groups_json")
    if raw_json:
        try:
            decoded = json.loads(str(raw_json)) if isinstance(raw_json, str) else raw_json
            if isinstance(decoded, Sequence) and not isinstance(decoded, (str, bytes)):
                groups.update(_normalize_group(value) for value in decoded)
        except Exception:
            pass
    return {group for group in groups if group}


def _priority(item: TrafficActionItem) -> int:
    if item.reply_needed and item.event_oriented:
        return 0
    if item.relay_needed:
        return 1
    if item.review_needed:
        return 2
    if item.reply_needed:
        return 3
    return 4


def _is_event_oriented(
    message: object,
    *,
    severity: str,
    status: str,
    topics: tuple[str, ...],
    actionable: bool,
) -> bool:
    if severity in {"urgent", "important", "critical", "warning"} or status == "ALERT" or actionable:
        return True
    if {topic.lower() for topic in topics}.intersection(_IMPACT_TOPICS):
        return True
    family = str(_value(message, "source_family", "origin") or "").strip().lower()
    form_type = str(_value(message, "form_type", "message_type", "msg_type") or "").strip().lower()
    return family in {"sitrep", "commstat", "local_report"} or any(
        token in form_type for token in ("sitrep", "statrep", "field report", "incident")
    )


def _is_impactful(severity: str, topics: tuple[str, ...], actionable: bool) -> bool:
    return bool(
        actionable
        or severity in {"urgent", "important", "critical", "warning"}
        or {topic.lower() for topic in topics}.intersection(_IMPACT_TOPICS)
    )


def _can_reply(message: object) -> bool:
    actions = _value(message, "actions")
    if actions is not None and hasattr(actions, "can_reply"):
        return bool(getattr(actions, "can_reply", False))
    family = str(_value(message, "source_family", "origin") or "").strip().lower()
    return family in {
        "js8",
        "js8call",
        "spotter",
        "varac",
        "commstat",
        "commstat_rf",
        "local_report",
        "mesh",
        "meshcore",
        "meshtastic",
    }


def _topics(message: object) -> tuple[str, ...]:
    raw = _value(message, "topics", "topics_json")
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
            raw = decoded if isinstance(decoded, list) else [raw]
        except Exception:
            raw = [raw]
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    return tuple(str(value or "").strip() for value in raw if str(value or "").strip())


def _lead_topic(topics: tuple[str, ...]) -> str:
    for topic in topics:
        if topic.lower() in _IMPACT_TOPICS:
            return topic
    return topics[0] if topics else ""


def _value(message: object, *names: str) -> object:
    for name in names:
        if isinstance(message, Mapping):
            if name in message and message[name] not in (None, ""):
                return message[name]
        else:
            value = getattr(message, name, None)
            if name == "summary" and value is not None and not isinstance(value, str):
                continue
            if value not in (None, ""):
                return value
    payload = None if isinstance(message, Mapping) else getattr(message, "payload", None)
    if payload is not None and payload is not message:
        for name in names:
            if isinstance(payload, Mapping):
                value = payload.get(name)
            else:
                value = getattr(payload, name, None)
            if value not in (None, ""):
                return value
    nested = None if isinstance(message, Mapping) else getattr(message, "summary", None)
    if nested is not None and nested is not message and not isinstance(nested, str):
        for name in names:
            value = getattr(nested, name, None)
            if value not in (None, ""):
                return value
    return ""


def _message_group(message: object) -> str:
    group = _normalize_group(_value(message, "group", "group_name"))
    if group:
        return group
    target = _normalize_target(_value(message, "to_target", "to_call"))
    return target if target and not _looks_like_callsign(target) else ""


def _looks_like_callsign(value: object) -> bool:
    text = str(value or "").strip().upper()
    return bool(re.fullmatch(r"[A-Z]{1,3}\d[A-Z0-9]{1,4}(?:-[A-Z0-9]+)?", text))


def _is_unread(message: object) -> bool:
    state = str(_value(message, "read_state") or "").strip().lower()
    if state:
        return state in {"new", "unread", "alert"}
    status = str(_value(message, "status") or "").strip().upper()
    return status in {"NEW", "UNREAD", "ALERT", "YELLOW", "RED"}


def _traffic_trend(current: int, previous: int, age: int) -> str:
    if not age:
        return "Window total"
    if current >= 5 and current >= previous * 2 and current - previous >= 3:
        return "Spike ↑"
    if current > previous:
        return "Rising ↑"
    if current < previous:
        return "Falling ↓"
    return "Steady"


def _trend_rank(value: str) -> int:
    return {"Spike ↑": 3, "Rising ↑": 2, "Steady": 1, "Falling ↓": 0}.get(value, 0)


def _normalize_source(value: object) -> str:
    source = str(value or "").strip().lower()
    aliases = {
        "nbems": "forms",
        "flmsg": "forms",
        "flamp": "forms",
        "js8": "js8call",
        "commstat_rf": "commstat",
        "fiospotter": "spotter",
        "js8spotter": "spotter",
        "mesh": "meshcore",
        "meshtastic": "meshcore",
        "local_mesh": "meshcore",
        "bbs_archive": "bbs",
    }
    return aliases.get(source, source)


def _source_label(value: object) -> str:
    source = _normalize_source(value)
    return {
        "forms": "FLMSG/FLAMP",
        "js8call": "JS8Call",
        "spotter": "Spotter",
        "commstat": "CommStat",
        "sitrep": "SitRep",
        "local_report": "Local Report",
        "meshcore": "Mesh",
        "varac": "VarAC",
        "bbs": "BBS",
    }.get(source, str(source or "Unknown").strip().title())


def _bool_value(message: object, name: str) -> bool:
    value = _value(message, name)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _float_value(message: object, *names: str) -> float:
    try:
        return float(_value(message, *names) or 0.0)
    except Exception:
        return 0.0


def _normalize_role(value: object) -> str:
    role = str(value or "").strip().upper().replace("_", "-")
    return "HUB-ALT" if role == "ALT-HUB" else role


def _normalize_call(value: object) -> str:
    return str(value or "").strip().upper().rstrip(">").strip()


def _normalize_target(value: object) -> str:
    target = _normalize_call(value)
    return target[1:] if target.startswith("@") else target


def _normalize_group(value: object) -> str:
    return normalize_group_name(value)


def _normalize_severity(value: object) -> str:
    severity = str(value or "routine").strip().lower()
    return severity or "routine"
