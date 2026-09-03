from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol, Sequence

from freqinout.core.message_intelligence import TOPIC_TAXONOMY


STATUS_BUCKET_ORDER: tuple[str, ...] = ("red", "yellow", "green", "info")
STATUS_BUCKET_LABELS: dict[str, str] = {
    "red": "Red",
    "yellow": "Yellow",
    "green": "Green",
    "info": "Info",
}
STATUS_BUCKET_RANK: dict[str, int] = {value: index for index, value in enumerate(STATUS_BUCKET_ORDER)}
TOPIC_ORDER: dict[str, int] = {topic.casefold(): index for index, topic in enumerate(TOPIC_TAXONOMY)}


class IntelMessageLike(Protocol):
    status: str
    topics: tuple[str, ...]
    payload: object
    actionable: bool


@dataclass(frozen=True)
class IntelFilterChip:
    kind: str
    value: str
    label: str
    count: int
    status_bucket: str = ""
    active: bool = False


@dataclass(frozen=True)
class IntelFilterRollup:
    total: int
    status_chips: tuple[IntelFilterChip, ...]
    topic_chips: tuple[IntelFilterChip, ...]


def normalize_intel_topic(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for topic in TOPIC_TAXONOMY:
        if topic.casefold() == text.casefold():
            return topic
    return text


def normalize_status_bucket(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in STATUS_BUCKET_ORDER:
        return text
    aliases = {
        "critical": "red",
        "urgent": "red",
        "alert": "red",
        "danger": "red",
        "warning": "yellow",
        "important": "yellow",
        "watch": "yellow",
        "orange": "yellow",
        "caution": "yellow",
        "degraded": "yellow",
        "ok": "green",
        "normal": "green",
        "all clear": "green",
        "functioning": "green",
        "routine": "info",
        "new": "info",
        "read": "info",
    }
    return aliases.get(text, "")


def message_status_bucket(row: IntelMessageLike) -> str:
    payload = getattr(row, "payload", None)
    candidates = (
        getattr(payload, "alert_color", ""),
        getattr(payload, "overall_status", ""),
        getattr(payload, "status_label", ""),
        getattr(payload, "severity", ""),
        getattr(payload, "status", ""),
        getattr(row, "status", ""),
    )
    for candidate in candidates:
        bucket = normalize_status_bucket(candidate)
        if bucket:
            return bucket
    haystack = " ".join(str(value or "").strip().lower() for value in candidates if str(value or "").strip())
    if any(term in haystack for term in ("red", "critical", "urgent", "alert")):
        return "red"
    if any(term in haystack for term in ("yellow", "orange", "warning", "watch", "degraded", "caution")):
        return "yellow"
    if any(term in haystack for term in ("green", "ok", "normal", "all clear", "functioning")):
        return "green"
    return "info"


def message_topics(row: IntelMessageLike) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for value in tuple(getattr(row, "topics", ()) or ()):
        topic = normalize_intel_topic(value)
        key = topic.casefold()
        if topic and key not in seen:
            seen.add(key)
            out.append(topic)
    return tuple(out)


def row_matches_intel_filters(
    row: IntelMessageLike,
    *,
    status_bucket: object = "",
    topic: object = "",
) -> bool:
    wanted_status = normalize_status_bucket(status_bucket)
    if wanted_status and message_status_bucket(row) != wanted_status:
        return False
    wanted_topic = normalize_intel_topic(topic)
    if wanted_topic and wanted_topic.casefold() not in {value.casefold() for value in message_topics(row)}:
        return False
    return True


def build_intel_filter_rollup(
    rows: Iterable[IntelMessageLike],
    *,
    active_status: object = "",
    active_topic: object = "",
    topic_limit: int = 8,
) -> IntelFilterRollup:
    row_list = list(rows)
    status_counts = {bucket: 0 for bucket in STATUS_BUCKET_ORDER}
    topic_counts: dict[str, int] = {}
    topic_worst: dict[str, str] = {}
    for row in row_list:
        bucket = message_status_bucket(row)
        status_counts[bucket] = status_counts.get(bucket, 0) + 1
        for topic in message_topics(row):
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
            current = topic_worst.get(topic, "info")
            if STATUS_BUCKET_RANK.get(bucket, 99) < STATUS_BUCKET_RANK.get(current, 99):
                topic_worst[topic] = bucket

    active_status_norm = normalize_status_bucket(active_status)
    active_topic_norm = normalize_intel_topic(active_topic)
    status_chips = tuple(
        IntelFilterChip(
            kind="status",
            value=bucket,
            label=STATUS_BUCKET_LABELS[bucket],
            count=status_counts.get(bucket, 0),
            status_bucket=bucket,
            active=bucket == active_status_norm,
        )
        for bucket in STATUS_BUCKET_ORDER
        if status_counts.get(bucket, 0) > 0
    )
    ordered_topics = sorted(
        topic_counts,
        key=lambda topic: (
            STATUS_BUCKET_RANK.get(topic_worst.get(topic, "info"), 99),
            -topic_counts[topic],
            TOPIC_ORDER.get(topic.casefold(), 999),
            topic.casefold(),
        ),
    )
    topic_chips = tuple(
        IntelFilterChip(
            kind="topic",
            value=topic,
            label=topic,
            count=topic_counts[topic],
            status_bucket=topic_worst.get(topic, "info"),
            active=topic.casefold() == active_topic_norm.casefold(),
        )
        for topic in ordered_topics[: max(1, int(topic_limit or 8))]
    )
    return IntelFilterRollup(total=len(row_list), status_chips=status_chips, topic_chips=topic_chips)


def focus_source_values(focus: object) -> tuple[str, ...]:
    focus_key = str(focus or "all").strip().lower()
    mapping = {
        "forms": ("flmsg", "flamp"),
        "spotter": ("spotter", "js8"),
        "commstat": ("commstat",),
        "js8call": ("js8", "commstat", "spotter"),
        "mesh": ("mesh", "meshcore", "meshtastic"),
        "varac": ("varac",),
        "bbs": ("bbs",),
    }
    return mapping.get(focus_key, ())
