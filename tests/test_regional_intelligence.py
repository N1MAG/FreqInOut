from __future__ import annotations

import datetime as dt
import sqlite3

from freqinout.core.observation_projection import Observation
from freqinout.core.regional_intelligence import (
    build_regional_intelligence,
    build_regional_intelligence_from_db,
)
from freqinout.core.observation_store import upsert_observation


NOW = dt.datetime(2026, 8, 25, 14, 0, tzinfo=dt.timezone.utc)


def _obs(
    observation_id: str,
    *,
    source_family: str = "flmsg",
    from_call: str = "K7ETC",
    state: str = "CA",
    topic: str = "Fire",
    hours_ago: float = 1.0,
    subject: str = "Wildfire reported",
    status: str = "NEW",
    confidence: float = 0.8,
    provenance: dict[str, object] | None = None,
) -> Observation:
    event_time = NOW - dt.timedelta(hours=hours_ago)
    metadata = {"confidence": confidence}
    if provenance:
        metadata.update(provenance)
    return Observation(
        observation_id=observation_id,
        source_family=source_family,
        source_ref=observation_id,
        received_utc=event_time.isoformat(),
        event_utc=event_time.isoformat(),
        from_call=from_call,
        to_target="@MAGNET",
        groups=("MAGNET",),
        observed_topics=(topic,),
        operator_attention=True,
        status=status,
        subject=subject,
        summary=subject,
        state=state,
        provenance=metadata,
    )


def test_regional_intelligence_weights_recent_and_context_reports() -> None:
    snapshot = build_regional_intelligence(
        [
            _obs("recent", from_call="KI6QDB", hours_ago=2.0),
            _obs("context", from_call="N7CWR", hours_ago=8.0),
        ],
        now=NOW,
        sensitivity="active",
    )

    ca = snapshot.state_rollups[0]
    assert ca.area_id == "CA"
    assert ca.fema_region == "R09"
    assert ca.level in {"orange", "red"}
    assert ca.evidence_count == 2
    assert ca.reporter_count == 2
    assert ca.trend == "increasing"
    assert ca.top_topics[0].topic == "Fire"


def test_regional_intelligence_commstat_nonlocal_scope_prefers_report_text_state() -> None:
    snapshot = build_regional_intelligence(
        [
            _obs(
                "commstat-other-location",
                source_family="commstat",
                from_call="KD9DSS",
                state="IN",
                topic="General Intel",
                subject="CommStat StatRep | COUNTY | YELLOW",
                provenance={
                    "scope": "COUNTY",
                    "body_text": "Reno-Sparks NV Evacuation Center",
                },
            )
        ],
        now=NOW,
        sensitivity="active",
    )

    assert snapshot.state_rollups[0].area_id == "NV"


def test_regional_intelligence_hard_history_does_not_linger_forever() -> None:
    snapshot = build_regional_intelligence(
        [_obs("old", hours_ago=400.0)],
        now=NOW,
        sensitivity="extended",
    )

    assert snapshot.state_rollups == ()
    assert snapshot.fema_rollups == ()


def test_regional_intelligence_normal_reports_are_low_concern_not_no_data() -> None:
    snapshot = build_regional_intelligence(
        [
            _obs(
                "green",
                source_family="commstat",
                topic="Power",
                subject="Power functioning, all clear",
                status="GREEN",
                confidence=0.9,
            )
        ],
        now=NOW,
        sensitivity="active",
    )

    ca = snapshot.state_rollups[0]
    assert ca.area_id == "CA"
    assert ca.level in {"green", "blue"}
    assert ca.evidence_count == 1
    assert ca.evidence[0].source_family == "commstat"
    assert ca.evidence[0].evidence_type == "status"


def test_regional_intelligence_repeated_green_commstat_does_not_escalate_state() -> None:
    observations = [
        _obs(
            f"green-{idx}",
            source_family="commstat",
            from_call=f"K5T{idx}X",
            state="TX",
            topic=topic,
            subject="CommStat StatRep | MY QTH | GREEN",
            status="GREEN",
            confidence=0.9,
        )
        for idx in range(6)
        for topic in ("Comms", "General Intel")
    ]

    snapshot = build_regional_intelligence(observations, now=NOW, sensitivity="active")

    tx = snapshot.state_rollups[0]
    assert tx.area_id == "TX"
    assert tx.level in {"green", "blue"}
    assert tx.score < 1.5
    assert tx.evidence_count == 12


def test_regional_intelligence_filters_topic_without_using_not_reported() -> None:
    snapshot = build_regional_intelligence(
        [
            _obs("fire", topic="Fire", subject="Wildfire reported"),
            _obs("power", topic="Power", subject="Power: Not Reported"),
        ],
        now=NOW,
        sensitivity="active",
        topic_filter="Fire",
    )

    ca = snapshot.state_rollups[0]
    assert [topic.topic for topic in ca.top_topics] == ["Fire"]
    assert ca.evidence_count == 1


def test_regional_intelligence_ignores_topic_tag_without_visible_evidence() -> None:
    snapshot = build_regional_intelligence(
        [
            _obs(
                "tag-only",
                topic="Fire",
                subject="MCF103 (#GYQV)",
            )
        ],
        now=NOW,
        sensitivity="active",
    )

    assert snapshot.state_rollups == ()
    assert snapshot.fema_rollups == ()


def test_regional_intelligence_accepts_structured_topic_evidence() -> None:
    obs = _obs(
        "structured",
        topic="Power",
        subject="CommStat",
        status="INFO",
        provenance={"topic_evidence": {"Power": ("status:Power grid down",)}},
    )

    snapshot = build_regional_intelligence([obs], now=NOW, sensitivity="active")

    ca = snapshot.state_rollups[0]
    assert ca.area_id == "CA"
    assert ca.top_topics[0].topic == "Power"


def test_regional_intelligence_js8_signal_is_capped_below_confirmed_report() -> None:
    report_snapshot = build_regional_intelligence(
        [_obs("report", source_family="flmsg", topic="Comms", subject="Comms degraded", confidence=0.8)],
        now=NOW,
        sensitivity="active",
    )
    signal_snapshot = build_regional_intelligence(
        [_obs("signal", source_family="js8call", topic="Comms", subject="Radio directed JS8 traffic elevated", confidence=0.9)],
        now=NOW,
        sensitivity="active",
    )

    assert signal_snapshot.state_rollups[0].signal_count == 1
    assert signal_snapshot.state_rollups[0].score < report_snapshot.state_rollups[0].score
    assert signal_snapshot.state_rollups[0].level in {"blue", "yellow"}


def test_regional_intelligence_can_load_from_observation_db(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    upsert_observation(db_path, _obs("db-fire", state="AZ", from_call="KC7HES", hours_ago=3.0))

    snapshot = build_regional_intelligence_from_db(db_path, now=NOW)

    assert snapshot.state_rollups[0].area_id == "AZ"
    assert snapshot.fema_rollups[0].area_id == "R09"


def test_regional_intelligence_db_loader_honors_group_search_state_and_topic(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    upsert_observation(
        db_path,
        _obs("ca-fire", state="CA", from_call="KI6QDB", topic="Fire", subject="Wildfire near county road"),
    )
    upsert_observation(
        db_path,
        _obs("az-fire", state="AZ", from_call="KC7HES", topic="Fire", subject="Wildfire near county road"),
    )
    upsert_observation(
        db_path,
        _obs("ca-power", state="CA", from_call="N7CWR", topic="Power", subject="Power grid down"),
    )

    snapshot = build_regional_intelligence_from_db(
        db_path,
        now=NOW,
        topic_filter="Fire",
        operating_group="MAGNET",
        search_text="county",
        state="CA",
    )

    assert [rollup.area_id for rollup in snapshot.state_rollups] == ["CA"]
    assert snapshot.state_rollups[0].top_topics[0].topic == "Fire"


def test_regional_intelligence_db_loader_honors_max_age_window(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    upsert_observation(db_path, _obs("recent-yellow", state="TX", hours_ago=2.0, topic="Comms", subject="Comms yellow"))
    upsert_observation(db_path, _obs("old-yellow", state="AZ", hours_ago=18.0, topic="Comms", subject="Comms yellow"))

    snapshot = build_regional_intelligence_from_db(
        db_path,
        now=NOW,
        max_age_sec=12 * 60 * 60,
    )

    assert [rollup.area_id for rollup in snapshot.state_rollups] == ["TX"]


def test_regional_intelligence_db_loader_uses_commstat_artifact_location(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    upsert_observation(
        db_path,
        _obs(
            "commstat_artifacts:568",
            source_family="commstat",
            from_call="KD9DSS",
            state="IN",
            topic="General Intel",
            subject="CommStat StatRep | COUNTY | YELLOW",
            status="YELLOW",
        ),
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE commstat_artifacts (
                id INTEGER PRIMARY KEY,
                from_call TEXT,
                target TEXT,
                report_group TEXT,
                grid TEXT,
                state_code TEXT,
                scope TEXT,
                status_label TEXT,
                alert_color TEXT,
                title TEXT,
                body_text TEXT,
                remarks_text TEXT,
                transport_mode TEXT,
                reach_mode TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO commstat_artifacts (
                id, from_call, target, report_group, grid, state_code, scope,
                status_label, alert_color, title, body_text, remarks_text,
                transport_mode, reach_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                568,
                "KD9DSS",
                "@COMMSTAT",
                "@COMMSTAT",
                "DM09CL",
                "",
                "COUNTY",
                "YELLOW",
                "",
                "CommStat StatRep | COUNTY | YELLOW",
                "Reno-Sparks NV Evacuation Center",
                "Reno-Sparks NV Evacuation Center",
                "internet",
                "internet_only",
            ),
        )

    snapshot = build_regional_intelligence_from_db(db_path, now=NOW)

    assert [rollup.area_id for rollup in snapshot.state_rollups] == ["NV"]

    nv_snapshot = build_regional_intelligence_from_db(db_path, now=NOW, state="NV")
    in_snapshot = build_regional_intelligence_from_db(db_path, now=NOW, state="IN")

    assert [rollup.area_id for rollup in nv_snapshot.state_rollups] == ["NV"]
    assert in_snapshot.state_rollups == ()


def test_regional_intelligence_uses_commstat_other_location_before_reporter_state() -> None:
    obs = _obs(
        "other-location",
        source_family="commstat",
        from_call="KD9DSS",
        state="",
        topic="Fire",
        subject="Reno-Sparks NV Hawk Fire Evacuation Center",
        status="YELLOW",
        provenance={"body_text": "Hawk Fire evacuation center open at 4590 S Virginia St Reno NV 89502."},
    )
    station_index = {"KD9DSS": {"state": "IN"}}

    snapshot = build_regional_intelligence([obs], now=NOW, station_index=station_index)

    assert [rollup.area_id for rollup in snapshot.state_rollups] == ["NV"]
    assert snapshot.state_rollups[0].evidence[0].reporter_callsign == "KD9DSS"


def test_regional_intelligence_keeps_explicit_report_state_when_text_mentions_elsewhere() -> None:
    obs = _obs(
        "explicit-state",
        source_family="commstat",
        from_call="KG6MTM",
        state="AZ",
        topic="Fire",
        subject="AZ: Hawk Fire updates",
        status="RED",
        provenance={"body_text": "Reference material also mentions Nevada fire operations."},
    )
    station_index = {"KG6MTM": {"state": "NV"}}

    snapshot = build_regional_intelligence([obs], now=NOW, station_index=station_index)

    assert [rollup.area_id for rollup in snapshot.state_rollups] == ["AZ"]
