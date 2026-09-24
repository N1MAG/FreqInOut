from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass
from typing import Any, Mapping


CONFIDENCE_MANUAL_CONFIRMED = 1
CONFIDENCE_GPS = 2
CONFIDENCE_STRUCTURED_REPORT = 3
CONFIDENCE_GRID6 = 4
CONFIDENCE_STATE_GRID4 = 5
CONFIDENCE_GRID4 = 6
CONFIDENCE_ROUTE_DERIVED = 7
CONFIDENCE_SOURCE_METADATA = 8


@dataclass(frozen=True)
class LocationEvidence:
    location_value: str
    location_kind: str
    confidence_rank: int
    source_family: str = ""
    source_ref: str = ""
    observed_utc: str = ""
    stale_after: str = ""
    explanation: str = ""

    @property
    def legacy_confidence(self) -> str:
        if self.location_kind in {"gps", "lat_lon", "manual_lat_lon"}:
            return "explicit-latlon"
        if self.location_kind.startswith("grid"):
            return "grid"
        if self.location_kind == "state":
            return "state"
        if self.location_kind == "route_derived":
            return "route_derived"
        if self.location_kind == "sender_lookup":
            return "sender_lookup"
        if self.location_kind == "declared":
            return "declared"
        return "unknown"

    def as_provenance(self) -> dict[str, Any]:
        return asdict(self)


def location_evidence_from_values(
    *,
    lat: object = None,
    lon: object = None,
    grid: object = "",
    state: object = "",
    location_kind: object = "",
    source_family: object = "",
    source_ref: object = "",
    observed_utc: object = "",
    stale_after: object = "",
    explanation: object = "",
) -> LocationEvidence:
    kind_hint = _clean(location_kind).lower().replace("-", "_")
    family = _clean(source_family)
    ref = _clean(source_ref)
    observed = _clean(observed_utc)
    stale = _clean(stale_after)
    note = _clean(explanation)
    lat_value = _float_or_none(lat)
    lon_value = _float_or_none(lon)
    clean_grid = _clean_grid(grid)
    clean_state = _clean_state(state)

    if lat_value is not None and lon_value is not None:
        kind = "gps" if kind_hint in {"gps", "declared", "position"} else (kind_hint or "lat_lon")
        rank = CONFIDENCE_GPS
        if kind_hint in {"manual", "manual_lat_lon", "user_confirmed"}:
            kind = "manual_lat_lon"
            rank = CONFIDENCE_MANUAL_CONFIRMED
        return LocationEvidence(
            location_value=f"{lat_value:.6f},{lon_value:.6f}",
            location_kind=kind,
            confidence_rank=rank,
            source_family=family,
            source_ref=ref,
            observed_utc=observed,
            stale_after=stale,
            explanation=note or "GPS or explicit lat/lon location",
        )

    if clean_grid:
        if kind_hint in {"manual", "user_confirmed"}:
            rank = CONFIDENCE_MANUAL_CONFIRMED
            kind = "manual_grid"
        elif kind_hint in {"structured", "structured_report", "reported"}:
            rank = CONFIDENCE_STRUCTURED_REPORT
            kind = "structured_report"
        elif len(clean_grid) >= 6:
            rank = CONFIDENCE_GRID6
            kind = "grid6"
        elif clean_state and len(clean_grid) == 4:
            rank = CONFIDENCE_STATE_GRID4
            kind = "state_grid4"
        else:
            rank = CONFIDENCE_GRID4
            kind = "grid4"
        return LocationEvidence(
            location_value=clean_grid,
            location_kind=kind,
            confidence_rank=rank,
            source_family=family,
            source_ref=ref,
            observed_utc=observed,
            stale_after=stale,
            explanation=note or "Reported grid location",
        )

    if clean_state:
        return LocationEvidence(
            location_value=clean_state,
            location_kind="state",
            confidence_rank=CONFIDENCE_SOURCE_METADATA,
            source_family=family,
            source_ref=ref,
            observed_utc=observed,
            stale_after=stale,
            explanation=note or "State/province only",
        )

    if kind_hint == "route_derived":
        return LocationEvidence(
            location_value=_clean(source_ref) or "route-derived",
            location_kind="route_derived",
            confidence_rank=CONFIDENCE_ROUTE_DERIVED,
            source_family=family,
            source_ref=ref,
            observed_utc=observed,
            stale_after=stale,
            explanation=note or "Route-derived location clue",
        )

    return LocationEvidence(
        location_value="",
        location_kind="unknown",
        confidence_rank=CONFIDENCE_SOURCE_METADATA,
        source_family=family,
        source_ref=ref,
        observed_utc=observed,
        stale_after=stale,
        explanation=note or "No usable location evidence",
    )


def location_evidence_to_provenance(evidence: LocationEvidence) -> dict[str, Any]:
    return evidence.as_provenance()


def prefer_location_evidence(
    current: LocationEvidence | Mapping[str, Any] | None,
    candidate: LocationEvidence | Mapping[str, Any] | None,
    *,
    locked: bool = False,
) -> LocationEvidence | None:
    current_evidence = _coerce_evidence(current)
    candidate_evidence = _coerce_evidence(candidate)
    if locked:
        return current_evidence
    if current_evidence is None:
        return candidate_evidence
    if candidate_evidence is None:
        return current_evidence
    if candidate_evidence.confidence_rank < current_evidence.confidence_rank:
        return candidate_evidence
    if candidate_evidence.confidence_rank > current_evidence.confidence_rank:
        return current_evidence
    current_time = _parse_utc(current_evidence.observed_utc)
    candidate_time = _parse_utc(candidate_evidence.observed_utc)
    if candidate_time and (current_time is None or candidate_time > current_time):
        return candidate_evidence
    return current_evidence


def _coerce_evidence(value: LocationEvidence | Mapping[str, Any] | None) -> LocationEvidence | None:
    if value is None:
        return None
    if isinstance(value, LocationEvidence):
        return value
    try:
        return LocationEvidence(
            location_value=_clean(value.get("location_value")),
            location_kind=_clean(value.get("location_kind")),
            confidence_rank=int(value.get("confidence_rank", CONFIDENCE_SOURCE_METADATA)),
            source_family=_clean(value.get("source_family")),
            source_ref=_clean(value.get("source_ref")),
            observed_utc=_clean(value.get("observed_utc")),
            stale_after=_clean(value.get("stale_after")),
            explanation=_clean(value.get("explanation")),
        )
    except Exception:
        return None


def _parse_utc(value: object) -> dt.datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _clean(value: object) -> str:
    return str(value or "").strip()


def _clean_state(value: object) -> str:
    text = _clean(value).upper()
    return text if len(text) == 2 and text.isalpha() else ""


def _clean_grid(value: object) -> str:
    text = _clean(value).upper()
    if 4 <= len(text) <= 8 and text[:2].isalpha() and text[2:4].isdigit():
        return text
    return ""


def _float_or_none(value: object) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None
