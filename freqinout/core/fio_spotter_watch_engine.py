from __future__ import annotations

"""Pure, bounded matching primitives for FIO Spotter watches.

This module deliberately has no SQLite or Qt dependencies.  Callers load a
bounded watch snapshot once, compile it, and can then evaluate projected
activity rows without a database read in the candidate/paint path.
"""

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


WATCH_KINDS = (
    "callsign", "group", "source", "kind", "topic", "keyword", "status", "location", "structured"
)
MATCH_MODES = ("contains", "whole-word", "exact")


def _text(value: object) -> str:
    return str(value or "").strip()


def _as_bool(value: object, default: bool = True) -> bool:
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"", "0", "false", "no", "off", "disabled"}:
            return False
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
    if value is None:
        return default
    return bool(value)


def _flatten(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set, frozenset)):
        result: list[str] = []
        for item in value:
            result.extend(_flatten(item))
        return result
    text = _text(value)
    return [text] if text else []


def _canonical_pattern(kind: str, pattern: object) -> str:
    value = " ".join(_text(pattern).split())
    if kind in {"callsign", "group"}:
        value = value.lstrip("@").upper()
    elif kind == "kind":
        value = " ".join(re.sub(r"[_/:-]+", " ", value).split())
    return value.casefold() if kind not in {"callsign", "group"} else value


def _source_alias(value: object) -> str:
    text = _text(value).casefold()
    aliases = {
        "js8call": "js8",
        "fiospotter": "spotter",
        "js8spotter": "spotter",
        "commstat rf": "commstat",
        "commstat_rf": "commstat",
        "meshcore": "mesh",
        "meshtastic": "mesh",
    }
    return aliases.get(text, text)


@dataclass(frozen=True)
class WatchCondition:
    kind: str
    pattern: str
    match_mode: str = "contains"

    def __post_init__(self) -> None:
        kind = self.kind.strip().lower()
        mode = self.match_mode.strip().lower().replace("_", "-")
        if mode == "word":
            mode = "whole-word"
        pattern = _canonical_pattern(kind, self.pattern)
        if kind not in WATCH_KINDS[:-1]:
            raise ValueError("unsupported watch condition kind")
        if mode not in MATCH_MODES:
            raise ValueError("unsupported watch match mode")
        if not pattern:
            raise ValueError("watch condition pattern is required")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "match_mode", mode)
        object.__setattr__(self, "pattern", pattern)


def _condition_values(kind: str, candidate: Mapping[str, Any]) -> list[str]:
    fields: dict[str, tuple[str, ...]] = {
        "callsign": ("from_call", "to_call", "callsign", "target_callsign"),
        "group": ("group_name", "group", "to_call"),
        "source": ("source_family", "source_label", "source"),
        "kind": ("source_kind", "message_type", "display_type", "form_type", "artifact_kind"),
        "topic": ("topic", "topics", "topic_names", "topic_labels"),
        "status": ("status", "severity", "confirmed_state", "followup_state"),
        "location": ("state_code", "grid", "city", "county", "location"),
        "keyword": ("summary", "body_text", "body", "preview", "subject", "search_text"),
    }
    values: list[str] = []
    for field in fields.get(kind, fields["keyword"]):
        values.extend(_flatten(candidate.get(field)))
    if kind in {"callsign", "group"}:
        values = [value.lstrip("@").upper() for value in values]
    return values


def condition_matches(condition: WatchCondition, candidate: Mapping[str, Any]) -> bool:
    """Match one condition against already-projected candidate data."""

    values = _condition_values(condition.kind, candidate)
    if not values:
        return False
    pattern = condition.pattern
    if condition.match_mode == "exact":
        return any(_canonical_pattern(condition.kind, value) == pattern for value in values)
    if condition.match_mode == "whole-word":
        expression = re.compile(rf"(?<!\w){re.escape(pattern)}(?!\w)", re.IGNORECASE)
        return any(expression.search(value) is not None for value in values)
    return any(pattern in _canonical_pattern(condition.kind, value) for value in values)


def parse_criteria(value: object) -> tuple[WatchCondition, ...]:
    """Parse supported criteria JSON/mappings into deterministic conditions.

    Accepted forms are ``{"conditions": [{"kind": ..., "pattern": ...}]}``,
    a mapping such as ``{"callsign": "W1ABC", "topic": "fire"}``, or a
    list of condition mappings.  All conditions are ANDed.  Invalid input is
    rejected rather than silently broadening a watch.
    """

    if value is None or value == "":
        return ()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("criteria_json must contain valid JSON") from exc
    if isinstance(value, Mapping):
        if "conditions" in value:
            raw = value.get("conditions")
        else:
            raw = []
            for raw_kind, spec in value.items():
                kind = _text(raw_kind).casefold()
                if kind not in WATCH_KINDS[:-1]:
                    raise ValueError(f"unsupported watch condition kind: {kind}")
                specs = spec if isinstance(spec, (list, tuple)) else [spec]
                for item in specs:
                    if isinstance(item, Mapping):
                        condition = dict(item)
                        condition.setdefault("kind", kind)
                    else:
                        condition = {"kind": kind, "pattern": item}
                    raw.append(condition)
    elif isinstance(value, (list, tuple)):
        raw = value
    else:
        raise ValueError("criteria_json must be an object or list")
    if raw is None:
        return ()
    if not isinstance(raw, (list, tuple)):
        raise ValueError("watch criteria conditions must be a list")
    conditions: list[WatchCondition] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("each watch condition must be an object")
        conditions.append(
            WatchCondition(
                str(item.get("kind", "") or ""),
                str(item.get("pattern", "") or ""),
                str(item.get("match_mode", "contains") or "contains"),
            )
        )
    # Repeated conditions are harmless but should not create duplicate rules.
    return tuple(sorted(set(conditions), key=lambda item: (item.kind, item.pattern, item.match_mode)))


def canonical_criteria(value: object) -> tuple[WatchCondition, ...]:
    return parse_criteria(value)


@dataclass(frozen=True)
class CompiledSpotterWatch:
    id: int
    conditions: tuple[WatchCondition, ...]
    source_families: tuple[str, ...] = ()
    source_radio_ids: tuple[str, ...] = ()
    enabled: bool = True
    expires_ts: float = 0.0

    def matches(self, candidate: Mapping[str, Any], *, now_ts: float | None = None) -> bool:
        if not self.enabled:
            return False
        now = time.time() if now_ts is None else float(now_ts)
        if self.expires_ts > 0 and now >= self.expires_ts:
            return False
        if self.source_families:
            candidate_sources = {
                _source_alias(candidate.get(key))
                for key in ("source_family", "source_label")
                if _source_alias(candidate.get(key))
            }
            display_source = _source_alias(candidate.get("display_type"))
            if display_source in {"js8", "spotter", "commstat", "flmsg", "flamp", "varac", "bbs", "mesh"}:
                candidate_sources.add(display_source)
            if not candidate_sources:
                fallback = _source_alias(candidate.get("source_kind"))
                if fallback:
                    candidate_sources.add(fallback)
            # Preview callers may provide only message text. Runtime
            # projected candidates always carry source identity; unknown
            # identity is therefore left to the caller's preflight policy.
            allowed_sources = {_source_alias(value) for value in self.source_families}
            if candidate_sources and candidate_sources.isdisjoint(allowed_sources):
                return False
        if self.source_radio_ids:
            radio = _text(candidate.get("source_radio_id") or candidate.get("radio_id")).casefold()
            if radio and radio not in self.source_radio_ids:
                return False
        return bool(self.conditions) and all(condition_matches(condition, candidate) for condition in self.conditions)


def _list_value(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            pass
    values = _flatten(value)
    return tuple(dict.fromkeys(_text(item).casefold() for item in values if _text(item)))


def compile_spotter_watch(watch: Mapping[str, Any]) -> CompiledSpotterWatch:
    raw_criteria = watch.get("criteria", watch.get("criteria_json"))
    conditions = parse_criteria(raw_criteria)
    if not conditions:
        kind = _text(watch.get("watch_kind", "keyword")).casefold()
        if kind == "structured":
            raise ValueError("structured watch requires criteria")
        conditions = (WatchCondition(kind, _text(watch.get("pattern")), _text(watch.get("match_mode", "contains"))),)
    return CompiledSpotterWatch(
        id=int(watch.get("id", 0) or 0),
        conditions=conditions,
        source_families=_list_value(watch.get("source_families", watch.get("source_families_json"))),
        source_radio_ids=_list_value(watch.get("source_radio_ids", watch.get("source_radio_ids_json"))),
        enabled=_as_bool(watch.get("enabled", True)),
        expires_ts=max(0.0, float(watch.get("expires_ts", 0.0) or 0.0)),
    )


def compile_spotter_watches(watches: Sequence[Mapping[str, Any]], *, limit: int = 100) -> tuple[CompiledSpotterWatch, ...]:
    """Compile a bounded snapshot; no persistence access occurs here."""

    compiled: list[CompiledSpotterWatch] = []
    for watch in list(watches)[: max(0, int(limit))]:
        try:
            item = compile_spotter_watch(watch)
        except (TypeError, ValueError):
            # A malformed imported/legacy row must not stop valid watches from
            # matching, and must never turn into a broad match.
            continue
        if item.enabled:
            compiled.append(item)
    return tuple(compiled)


class SpotterWatchMatcher:
    """Cached event-time matcher built from one immutable watch snapshot."""

    def __init__(self, watches: Sequence[CompiledSpotterWatch]):
        self._watches = tuple(watches)

    @classmethod
    def from_snapshot(cls, watches: Sequence[Mapping[str, Any]], *, limit: int = 100) -> "SpotterWatchMatcher":
        return cls(compile_spotter_watches(watches, limit=limit))

    @property
    def watches(self) -> tuple[CompiledSpotterWatch, ...]:
        return self._watches

    def matching_watch_ids(self, candidate: Mapping[str, Any], *, now_ts: float | None = None) -> tuple[int, ...]:
        return tuple(watch.id for watch in self._watches if watch.matches(candidate, now_ts=now_ts))

    def match(self, candidate: Mapping[str, Any], *, now_ts: float | None = None) -> tuple[int, ...]:
        return self.matching_watch_ids(candidate, now_ts=now_ts)

    def match_candidate(self, candidate: Mapping[str, Any], *, now_ts: float | None = None) -> tuple[int, ...]:
        """Named integration alias for event/ingest callers."""
        return self.matching_watch_ids(candidate, now_ts=now_ts)
