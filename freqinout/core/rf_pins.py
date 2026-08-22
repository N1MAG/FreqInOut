from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from freqinout.core.observation_projection import Observation, observation_from_rf_pin
from freqinout.core.observation_store import (
    delete_observations_by_source_refs,
    list_observations,
    upsert_observation,
)


def save_rf_pin(db_path: str | Path, pin: Mapping[str, Any]) -> Observation:
    """Save a receive/manual RF pin as an observation projection row."""
    observation = observation_from_rf_pin(pin)
    upsert_observation(db_path, observation)
    return observation


def list_rf_pins(db_path: str | Path, *, limit: int = 200) -> tuple[Observation, ...]:
    return tuple(list_observations(db_path, source_family="rf_pin", limit=limit))


def delete_rf_pins(db_path: str | Path, source_refs: Sequence[str]) -> int:
    return delete_observations_by_source_refs(db_path, source_refs, source_family="rf_pin")
