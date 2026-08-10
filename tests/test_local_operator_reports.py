import sqlite3

from freqinout.core import local_ops_store


def _use_tmp_db(monkeypatch, tmp_path):
    db_path = tmp_path / "freqinout_nets.db"
    monkeypatch.setattr(local_ops_store, "_db_path", lambda: db_path)
    monkeypatch.setattr(local_ops_store, "_SCHEMA_READY", False)
    monkeypatch.setattr(local_ops_store, "_SCHEMA_READY_DB", "")
    return db_path


def test_record_local_report_preserves_operator_notes_and_adds_topic_evidence(monkeypatch, tmp_path) -> None:
    db_path = _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.upsert_operator("k7etc", name="Test Operator", notes="General local note")

    report_id = local_ops_store.record_local_report(
        callsign="k7etc",
        source_kind="gmrs",
        source_channel="462.675",
        net_session_id="local-net-1",
        from_name="Test Operator",
        city="Delta",
        county="Millard",
        state="ut",
        grid="dm38st",
        location_source="operator",
        location_confidence="explicit-grid",
        status="priority",
        topics=["Fire", "Travel/Roads"],
        subject="Widemouth 2 Fire",
        body="Road closure reported near the wildfire evacuation area.",
        confirmed_state="second hand",
        followup_state="needs callback",
        source_app="Local NCS",
        created_by="N1MAG",
        created_utc="2026-08-10T12:00:00+00:00",
    )

    assert report_id and report_id > 0
    rows = local_ops_store.list_local_reports(callsign="K7ETC")
    assert len(rows) == 1
    row = rows[0]
    assert row["callsign"] == "K7ETC"
    assert row["source_kind"] == "gmrs"
    assert row["source_channel"] == "462.675"
    assert row["status"] == "PRIORITY"
    assert row["topics"] == ["Fire", "Travel/Roads"]
    assert "manual:Fire" in row["topic_evidence"]["Fire"]
    assert any(item.startswith("body:wildfire") for item in row["topic_evidence"]["Fire"])
    assert row["confirmed_state"] == "SECOND_HAND"
    assert row["exercise_flag"] is False

    operator = local_ops_store.get_operator("K7ETC")
    assert operator is not None
    assert operator["notes"] == "General local note"
    assert operator["sitrep_status"] == "YELLOW"

    with sqlite3.connect(db_path) as conn:
        indexes = {r[1] for r in conn.execute("PRAGMA index_list(local_operator_reports)").fetchall()}
    assert "idx_local_reports_callsign" in indexes
    assert "idx_local_reports_status" in indexes


def test_local_report_filters_by_topic_status_and_keyword(monkeypatch, tmp_path) -> None:
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.record_local_report(
        callsign="W1ABC",
        status="watch",
        topics=["Power"],
        subject="Substation outage",
        body="Generator needed at the water plant.",
        state="CO",
        created_utc="2026-08-10T12:00:00+00:00",
    )
    local_ops_store.record_local_report(
        callsign="W2DEF",
        status="info",
        topics=["Comms"],
        subject="Repeater check",
        body="Local repeater is normal.",
        state="WY",
        created_utc="2026-08-10T13:00:00+00:00",
    )

    power = local_ops_store.list_local_reports(topic="Power")
    assert [row["callsign"] for row in power] == ["W1ABC"]

    watch = local_ops_store.list_local_reports(status="WATCH")
    assert [row["callsign"] for row in watch] == ["W1ABC"]

    water = local_ops_store.list_local_reports(query="water plant")
    assert [row["callsign"] for row in water] == ["W1ABC"]
