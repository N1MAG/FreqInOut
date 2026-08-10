import sqlite3

from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.observation_projection import (
    observation_from_local_report,
    observation_from_message_intelligence,
)
from freqinout.core.observation_store import (
    get_projection_checkpoint,
    list_observations,
    set_projection_checkpoint,
    upsert_observation,
)


def test_observation_store_round_trips_indexed_message_projection(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    info = analyze_spotter_text(
        "F!307 TO[@MR08] FR[K7ETC] ST[UT] GR[DM38ST] NA[Wildfire status] DA[260810-1405Z]",
        form_name="MCF307 Wildfire Status Report",
    )
    obs = observation_from_message_intelligence(
        info,
        source_ref="forms:25",
        received_utc="2026-08-10T14:10:00+00:00",
        source_app="FIO-A",
    )

    upsert_observation(db_path, obs, projected_utc="2026-08-10T14:11:00+00:00")

    rows = list_observations(db_path, topic="Fire", state="UT")
    assert len(rows) == 1
    assert rows[0].observation_id == obs.observation_id
    assert rows[0].source_ref == "forms:25"
    assert rows[0].observed_topics == obs.observed_topics
    assert rows[0].route_eligible is False
    assert rows[0].publish_authorized is False
    assert rows[0].provenance["form_name"] == "MCF307 Wildfire Status Report"


def test_observation_store_upsert_replaces_topics_without_duplicates(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    first = observation_from_local_report(
        {
            "id": 1,
            "created_utc": "2026-08-10T14:00:00+00:00",
            "callsign": "K0PRA",
            "topics": ("Power",),
            "subject": "Power outage",
            "state": "CO",
            "grid": "DM79",
        }
    )
    second = observation_from_local_report(
        {
            "id": 1,
            "created_utc": "2026-08-10T14:00:00+00:00",
            "callsign": "K0PRA",
            "topics": ("Comms",),
            "subject": "Repeater degraded",
            "state": "CO",
            "grid": "DM79",
        }
    )

    upsert_observation(db_path, first)
    upsert_observation(db_path, second)

    assert list_observations(db_path, topic="Power") == []
    rows = list_observations(db_path, topic="Comms")
    assert len(rows) == 1
    assert rows[0].subject == "Repeater degraded"

    conn = sqlite3.connect(db_path)
    try:
        topic_rows = conn.execute("SELECT topic FROM observation_projection_topics").fetchall()
    finally:
        conn.close()
    assert topic_rows == [("Comms",)]


def test_projection_checkpoint_round_trips_incremental_source_state(tmp_path) -> None:
    db_path = tmp_path / "fio.db"

    set_projection_checkpoint(
        db_path,
        source_key="spotter:FIO-A",
        last_source_ref="forms:25",
        last_event_utc="2026-08-10T14:10:00+00:00",
        updated_utc="2026-08-10T14:11:00+00:00",
    )

    checkpoint = get_projection_checkpoint(db_path, "spotter:FIO-A")
    assert checkpoint == {
        "source_key": "spotter:FIO-A",
        "last_source_ref": "forms:25",
        "last_event_utc": "2026-08-10T14:10:00+00:00",
        "updated_utc": "2026-08-10T14:11:00+00:00",
    }
