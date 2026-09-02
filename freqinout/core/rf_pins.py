from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from freqinout.core.message_intelligence import MessageIntelligence
from freqinout.core.observation_projection import Observation, observation_from_rf_pin
from freqinout.core.observation_store import (
    delete_observations_by_source_refs,
    list_observations,
    upsert_observation,
)


PIN_TYPES: tuple[str, ...] = (
    "hazard",
    "checkpoint",
    "supply",
    "medical",
    "comms",
    "shelter",
    "road",
    "weather",
    "welfare",
    "info",
    "custom",
)

TOPIC_PIN_TYPES: Mapping[str, str] = {
    "Fire": "hazard",
    "Weather": "weather",
    "Medical": "medical",
    "Comms": "comms",
    "Infrastructure": "comms",
    "Shelter": "shelter",
    "Travel/Roads": "road",
    "Food": "supply",
    "Water": "supply",
    "Fuel": "supply",
    "Logistics": "supply",
    "Security": "checkpoint",
}


def save_rf_pin(db_path: str | Path, pin: Mapping[str, Any]) -> Observation:
    """Save a receive/manual RF pin as an observation projection row."""
    observation = observation_from_rf_pin(pin)
    upsert_observation(db_path, observation)
    return observation


def list_rf_pins(db_path: str | Path, *, limit: int = 200) -> tuple[Observation, ...]:
    return tuple(list_observations(db_path, source_family="rf_pin", limit=limit))


def delete_rf_pins(db_path: str | Path, source_refs: Sequence[str]) -> int:
    return delete_observations_by_source_refs(db_path, source_refs, source_family="rf_pin")


def build_operational_pin(
    *,
    source_ref: object,
    source_family: object,
    label: object,
    summary: object = "",
    pin_type: object = "info",
    from_call: object = "",
    group: object = "",
    topics: Sequence[object] = (),
    grid: object = "",
    state: object = "",
    lat: object = None,
    lon: object = None,
    created_by: object = "",
    created_utc: object = "",
    expires_utc: object = "",
) -> dict[str, Any]:
    normalized_type = _normalize_pin_type(pin_type)
    source = _clean(source_family) or "message"
    ref = _clean(source_ref) or "unknown"
    clean_label = _clean(label) or _clean(summary) or f"{normalized_type.title()} pin"
    pin_ref = f"{source}:{ref}:{normalized_type}:{_clean(grid) or _clean(state) or _clean(lat) or 'unknown'}"
    return {
        "pin_id": pin_ref,
        "source_ref": pin_ref,
        "raw_reference": pin_ref,
        "source_app": source,
        "pin_type": normalized_type,
        "pin_kind": normalized_type,
        "label": clean_label,
        "summary": _clean(summary) or clean_label,
        "callsign": _clean(from_call),
        "group": _clean(group),
        "topics": tuple(_clean(value) for value in topics if _clean(value)),
        "grid": _clean(grid),
        "state": _clean(state),
        "lat": lat,
        "lon": lon,
        "created_by": _clean(created_by),
        "created_utc": _clean(created_utc),
        "expires_utc": _clean(expires_utc),
        "status": "PIN",
    }


def operational_pin_from_message_intelligence(
    info: MessageIntelligence,
    *,
    source_ref: object,
    source_family: object = "spotter",
    receive_enabled: bool = True,
    created_by: object = "",
    created_utc: object = "",
    expires_utc: object = "",
) -> dict[str, Any] | None:
    """Build a receive-side operational pin candidate from parsed traffic."""
    if not receive_enabled:
        return None
    if not (_clean(info.grid) or _clean(info.state)):
        return None
    topics = tuple(info.topics)
    pin_type = _pin_type_for_topics(topics)
    label = info.subject or info.summary or info.form_name
    group = info.to_call or (info.groups[0] if info.groups else "")
    return build_operational_pin(
        source_ref=source_ref,
        source_family=source_family,
        label=label,
        summary=info.summary,
        pin_type=pin_type,
        from_call=info.from_call,
        group=group,
        topics=topics,
        grid=info.grid,
        state=info.state,
        created_by=created_by,
        created_utc=created_utc,
        expires_utc=expires_utc,
    )


def _pin_type_for_topics(topics: Sequence[object]) -> str:
    for topic in topics:
        mapped = TOPIC_PIN_TYPES.get(_clean(topic))
        if mapped:
            return mapped
    return "info"


def _normalize_pin_type(value: object) -> str:
    text = _clean(value).lower().replace(" ", "_").replace("-", "_")
    return text if text in PIN_TYPES else "info"


def _clean(value: object) -> str:
    return str(value or "").strip()
