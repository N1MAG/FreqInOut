from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from freqinout.core.observation_projection import (
    Observation,
    ObservationEligibility,
    explain_bbs_eligibility,
    explain_map_eligibility,
)
from freqinout.core.observation_store import list_observations


@dataclass(frozen=True)
class ObservationQuery:
    source_family: str = ""
    topic: str = ""
    from_call: str = ""
    to_target: str = ""
    status: str = ""
    state: str = ""
    grid: str = ""
    since_utc: str = ""
    limit: int = 200


@dataclass(frozen=True)
class ObservationViewRow:
    observation: Observation
    map_eligibility: ObservationEligibility | None = None
    bbs_eligibility: ObservationEligibility | None = None

    @property
    def summary_line(self) -> str:
        parts = [
            self.observation.source_family,
            self.observation.status,
            " -> ".join(
                part
                for part in (self.observation.from_call, self.observation.to_target)
                if part
            ),
            self.observation.subject or self.observation.summary,
        ]
        return " | ".join(part for part in parts if part)


@dataclass(frozen=True)
class OperationalActivitySnapshot:
    latest: tuple[Observation, ...] = ()
    high_attention: tuple[Observation, ...] = ()
    condition_alerts: tuple[Observation, ...] = ()
    topics: tuple[str, ...] = ()


def query_observations(
    db_path: str | Path,
    query: ObservationQuery | None = None,
) -> tuple[Observation, ...]:
    q = query or ObservationQuery()
    rows = list_observations(
        db_path,
        source_family=q.source_family,
        from_call=q.from_call,
        to_target=q.to_target,
        topic=q.topic,
        status=q.status,
        state=q.state,
        grid=q.grid,
        since_utc=q.since_utc,
        limit=q.limit,
    )
    return tuple(rows)


def map_observation_rows(
    db_path: str | Path,
    query: ObservationQuery | None = None,
    *,
    layer_enabled: bool,
    allow_unconfirmed_local: bool = False,
    exercise_layer: bool = False,
) -> tuple[ObservationViewRow, ...]:
    return tuple(
        ObservationViewRow(
            observation=observation,
            map_eligibility=explain_map_eligibility(
                observation,
                layer_enabled=layer_enabled,
                allow_unconfirmed_local=allow_unconfirmed_local,
                exercise_layer=exercise_layer,
            ),
        )
        for observation in query_observations(db_path, query)
    )


def bbs_observation_rows(
    db_path: str | Path,
    query: ObservationQuery | None = None,
    *,
    rule_enabled: bool = False,
    dry_run_reviewed: bool = False,
    destination_scope: str = "",
) -> tuple[ObservationViewRow, ...]:
    return tuple(
        ObservationViewRow(
            observation=observation,
            bbs_eligibility=explain_bbs_eligibility(
                observation,
                rule_enabled=rule_enabled,
                dry_run_reviewed=dry_run_reviewed,
                destination_scope=destination_scope,
            ),
        )
        for observation in query_observations(db_path, query)
    )


def eligible_map_observations(rows: Sequence[ObservationViewRow]) -> tuple[Observation, ...]:
    return tuple(
        row.observation
        for row in rows
        if row.map_eligibility is not None and row.map_eligibility.allowed
    )


def operational_activity_snapshot(
    db_path: str | Path,
    query: ObservationQuery | None = None,
    *,
    operating_group: str = "",
    limit: int = 50,
) -> OperationalActivitySnapshot:
    """Return a compact, UI-ready activity snapshot from projected observations."""
    q = query or ObservationQuery(limit=limit)
    q = ObservationQuery(
        source_family=q.source_family,
        topic=q.topic,
        from_call=q.from_call,
        to_target=q.to_target,
        status=q.status,
        state=q.state,
        grid=q.grid,
        since_utc=q.since_utc,
        limit=max(limit, q.limit or limit),
    )
    group = _normalize_group(operating_group)
    observations = tuple(
        observation
        for observation in query_observations(db_path, q)
        if not group or _observation_matches_group(observation, group)
    )[: max(1, int(limit or 50))]
    high_attention = tuple(observation for observation in observations if observation.operator_attention)
    condition_alerts = tuple(
        observation
        for observation in observations
        if str(observation.source_family or "").strip().lower() == "condition_alert"
    )
    topics = tuple(
        sorted(
            {
                str(topic or "").strip()
                for observation in observations
                for topic in observation.observed_topics
                if str(topic or "").strip()
            }
        )
    )
    return OperationalActivitySnapshot(
        latest=observations,
        high_attention=high_attention,
        condition_alerts=condition_alerts,
        topics=topics,
    )


def _normalize_group(value: str) -> str:
    return str(value or "").strip().upper().lstrip("@")


def _observation_matches_group(observation: Observation, group: str) -> bool:
    candidates = [
        observation.to_target,
        *observation.groups,
    ]
    return any(_normalize_group(candidate) == group for candidate in candidates)
