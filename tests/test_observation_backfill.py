import sqlite3

from freqinout.core.observation_backfill import backfill_observations
from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.observation_backfill import project_message_file_observations
from freqinout.core.observation_store import get_projection_checkpoint, list_observations


def test_observation_backfill_projects_existing_spotter_traffic_in_batches(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE spotter_traffic (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utc_str TEXT,
                from_call TEXT,
                to_call TEXT,
                form_id TEXT,
                raw_text TEXT,
                state TEXT,
                source_radio_id TEXT,
                js8_instance_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO spotter_traffic(utc_str, from_call, to_call, form_id, raw_text, state, source_radio_id, js8_instance_id)
            VALUES ('2026-08-10 14:05:00', 'K7ETC', '@MR08', '307',
                    'F!307 TO[@MR08] FR[K7ETC] ST[UT] GR[DM38ST] NA[Wildfire status] #D2NT',
                    'UNREAD', '8', 'fio-b')
            """
        )
        conn.execute(
            """
            INSERT INTO spotter_traffic(utc_str, from_call, to_call, form_id, raw_text, state, source_radio_id, js8_instance_id)
            VALUES ('2026-08-10 14:10:00', 'W0IFM', '@MAGNET', '701',
                    'F!701 TO[@MAGNET] FR[W0IFM] ST[MO] GR[EM48EQ] NA[Power outage] #D2NT',
                    'READ', '8', 'fio-b')
            """
        )
        conn.commit()
    finally:
        conn.close()

    first = backfill_observations(db_path, include_local_reports=False, batch_limit=1)
    second = backfill_observations(db_path, include_local_reports=False, batch_limit=1)

    assert first == {"local_reports": 0, "spotter_traffic": 1}
    assert second == {"local_reports": 0, "spotter_traffic": 1}
    rows = list_observations(db_path, source_family="spotter")
    assert [row.source_ref for row in rows] == ["spotter_traffic:2", "spotter_traffic:1"]
    assert rows[0].from_call == "W0IFM"
    assert "Power" in rows[0].observed_topics
    assert get_projection_checkpoint(db_path, "spotter_traffic")["last_source_ref"] == "spotter_traffic:2"


def test_observation_backfill_projects_existing_local_reports_without_routing(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE local_operator_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_utc TEXT,
                updated_utc TEXT,
                source_kind TEXT,
                source_channel TEXT,
                net_session_id TEXT,
                callsign TEXT,
                operator_id TEXT,
                from_name TEXT,
                city TEXT,
                county TEXT,
                state TEXT,
                grid TEXT,
                lat REAL,
                lon REAL,
                location_source TEXT,
                location_confidence TEXT,
                status TEXT,
                topics_json TEXT,
                topic_evidence_json TEXT,
                subject TEXT,
                body TEXT,
                confirmed_state TEXT,
                followup_state TEXT,
                exercise_flag INTEGER,
                source_radio_id INTEGER,
                source_app TEXT,
                raw_reference TEXT,
                created_by TEXT,
                updated_by TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO local_operator_reports(
                created_utc, updated_utc, source_kind, source_channel, net_session_id, callsign,
                operator_id, from_name, city, county, state, grid, lat, lon, location_source,
                location_confidence, status, topics_json, topic_evidence_json, subject, body,
                confirmed_state, followup_state, exercise_flag, source_radio_id, source_app,
                raw_reference, created_by, updated_by
            )
            VALUES (
                '2026-08-10T15:00:00+00:00', '2026-08-10T15:00:00+00:00',
                'voice', 'VHF Net', '', 'K0PRA', '', 'Parker Club', 'Parker', 'Douglas',
                'CO', 'DM79', NULL, NULL, 'operator', 'grid', 'PRIORITY',
                '["Comms"]', '{}', 'Repeater degraded', 'Repeater degraded after storm.',
                'CONFIRMED', '', 0, 3, 'Local NCS', '', 'N1MAG', 'N1MAG'
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    result = backfill_observations(db_path, include_spotter_traffic=False)

    assert result == {"local_reports": 1, "spotter_traffic": 0}
    rows = list_observations(db_path, source_family="local_report", topic="Comms")
    assert len(rows) == 1
    assert rows[0].from_call == "K0PRA"
    assert rows[0].confirmed_state == "CONFIRMED"
    assert rows[0].route_eligible is False
    assert rows[0].publish_authorized is False


def test_message_file_projection_uses_existing_form_intelligence_without_authorizing(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    msg_path = tmp_path / "K7ETC-20260803-040212Z-57.k2s"
    msg_path.write_text(
        """
MAGNET General Use Form - v1.1.1
Date/Time/Msg ID
260803-0402z
To
MR08
From
K7ETC
Subject
Widemouth 2 Fire
Message
UT - Widemouth 2 Fire - DM38ST - evacuation posture updated.
""",
        encoding="utf-8",
    )
    stat = msg_path.stat()
    records = {
        "flmsg": [FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)],
        "flamp": [],
    }

    projected = project_message_file_observations(db_path, records, batch_limit=10)

    assert projected == 1
    rows = list_observations(db_path, source_family="flmsg", topic="Fire")
    assert len(rows) == 1
    assert rows[0].source_ref == f"file:{msg_path}"
    assert rows[0].from_call == "K7ETC"
    assert rows[0].to_target == "MR08"
    assert rows[0].state == "UT"
    assert rows[0].grid == "DM38ST"
    assert rows[0].route_eligible is False
    assert rows[0].publish_authorized is False
