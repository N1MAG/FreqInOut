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
