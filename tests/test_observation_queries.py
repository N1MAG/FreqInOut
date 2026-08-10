from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.observation_projection import (
    observation_from_local_report,
    observation_from_message_intelligence,
)
from freqinout.core.observation_queries import (
    ObservationQuery,
    bbs_observation_rows,
    eligible_map_observations,
    map_observation_rows,
    query_observations,
)
from freqinout.core.observation_store import upsert_observation


def test_observation_query_facade_returns_filtered_read_only_rows(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    spotter = observation_from_message_intelligence(
        analyze_spotter_text("F!307 TO[@MR08] FR[K7ETC] ST[UT] GR[DM38ST] NA[Wildfire status] #D2NT"),
        source_ref="spotter_traffic:1",
        source_family="spotter",
        event_utc="2026-08-10T14:00:00+00:00",
    )
    local = observation_from_local_report(
        {
            "id": 1,
            "created_utc": "2026-08-10T15:00:00+00:00",
            "callsign": "K0PRA",
            "state": "CO",
            "grid": "DM79",
            "topics": ("Comms",),
            "subject": "Repeater degraded",
            "confirmed_state": "UNCONFIRMED",
        }
    )
    upsert_observation(db_path, spotter)
    upsert_observation(db_path, local)

    rows = query_observations(db_path, ObservationQuery(topic="Fire"))

    assert len(rows) == 1
    assert rows[0].source_family == "spotter"
    assert rows[0].route_eligible is False
    assert rows[0].publish_authorized is False


def test_map_query_facade_explains_allowed_and_blocked_rows(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    confirmed = observation_from_local_report(
        {
            "id": 1,
            "created_utc": "2026-08-10T15:00:00+00:00",
            "callsign": "K0PRA",
            "state": "CO",
            "grid": "DM79",
            "topics": ("Comms",),
            "subject": "Repeater degraded",
            "confirmed_state": "CONFIRMED",
        }
    )
    unconfirmed = observation_from_local_report(
        {
            "id": 2,
            "created_utc": "2026-08-10T15:05:00+00:00",
            "callsign": "N0PWR",
            "state": "CO",
            "grid": "DM79",
            "topics": ("Power",),
            "subject": "Generator needed",
            "confirmed_state": "UNCONFIRMED",
        }
    )
    upsert_observation(db_path, confirmed)
    upsert_observation(db_path, unconfirmed)

    rows = map_observation_rows(db_path, layer_enabled=True)
    eligible = eligible_map_observations(rows)

    assert {row.observation.from_call: row.map_eligibility.allowed for row in rows} == {
        "N0PWR": False,
        "K0PRA": True,
    }
    assert [row.from_call for row in eligible] == ["K0PRA"]
    blocked = next(row for row in rows if row.observation.from_call == "N0PWR")
    assert "local report is unconfirmed" in blocked.map_eligibility.reasons


def test_bbs_query_facade_stays_review_only_until_rule_context_is_explicit(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    spotter = observation_from_message_intelligence(
        analyze_spotter_text("F!307 TO[@MR08] FR[K7ETC] ST[UT] GR[DM38ST] NA[Wildfire status] #D2NT"),
        source_ref="spotter_traffic:1",
        source_family="spotter",
    )
    upsert_observation(db_path, spotter)

    blocked = bbs_observation_rows(db_path)
    allowed = bbs_observation_rows(
        db_path,
        rule_enabled=True,
        dry_run_reviewed=True,
        destination_scope="HF/MR08/FIRE",
    )

    assert blocked[0].bbs_eligibility.allowed is False
    assert "no enabled rule" in blocked[0].bbs_eligibility.reasons
    assert allowed[0].bbs_eligibility.allowed is True
    assert allowed[0].observation.publish_authorized is False
