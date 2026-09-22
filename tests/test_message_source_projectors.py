from __future__ import annotations

import sqlite3

from freqinout.core.commstat_artifacts import ensure_commstat_artifact_tables
from freqinout.core.db_initializer import _ensure_sitrep_fusion_tables
from freqinout.core.message_projection_store import ensure_message_projection_schema
from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.message_source_projectors import (
    _analyze_local_js8_commstat,
    project_native_file_records,
    project_native_message_sources,
)
from freqinout.core.varac_ingest import ensure_varac_local_tables


def _connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_js8(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE js8_messages (
            id INTEGER PRIMARY KEY,
            from_call TEXT,
            to_call TEXT,
            msg_type TEXT,
            utc_str TEXT,
            utc_ts REAL,
            raw_text TEXT,
            decoded_text TEXT,
            state TEXT,
            read_ts REAL,
            flag_state INTEGER DEFAULT 0,
            source_key TEXT,
            source_id INTEGER,
            source_radio_id TEXT,
            js8_instance_id TEXT,
            source_path TEXT
        )
        """
    )


def _ensure_spotter(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE spotter_traffic (
            id INTEGER PRIMARY KEY,
            utc_str TEXT,
            utc_ts REAL,
            from_call TEXT,
            to_call TEXT,
            form_id TEXT,
            spotter_token TEXT,
            raw_text TEXT,
            decoded_text TEXT,
            state TEXT,
            read_ts REAL,
            flag_state INTEGER DEFAULT 0,
            relay_via TEXT,
            source_radio_id TEXT,
            js8_instance_id TEXT
        )
        """
    )


def test_source_native_projectors_populate_projection_without_qt_rows(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        _ensure_js8(conn)
        _ensure_spotter(conn)
        ensure_varac_local_tables(conn)
        _ensure_sitrep_fusion_tables(conn)
        ensure_commstat_artifact_tables(conn)
        conn.execute(
            """
            INSERT INTO js8_messages
                (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text, state,
                 read_ts, source_key, source_id, source_radio_id, js8_instance_id, source_path)
            VALUES
                (1, 'N1AAA', '@MR08', 'MSG', '2026-09-02 10:00:00', 1788352800,
                 'F!701C 100 ST[UT] GR[DN40] #ABCD',
                 'State (2-letter code): UT
Maidenhead Grid Square: DN40

Current Operational Status (QTH)
*Operations steady, no significant issues or noteworthy activity - Green',
                 'UNREAD', 0, 'radio-a', 101, '7', 'js8-a', '/tmp/js8.db')
            """
        )
        conn.execute(
            """
            INSERT INTO spotter_traffic
                (id, utc_str, utc_ts, from_call, to_call, form_id, spotter_token, raw_text,
                 decoded_text, state, read_ts, relay_via, source_radio_id, js8_instance_id)
            VALUES
                (2, '2026-09-02 10:01:00', 1788352860, 'N1BBB', '@MR08', '104',
                 'tok', 'F!104 ST[UT] GR[DN40] NA[Water outage]', 'Spotter decoded', 'UNREAD', 0, 'N1RELAY', '8', 'js8-b')
            """
        )
        conn.execute(
            """
            INSERT INTO varac_messages
                (ingest_source_key, id, guid, source, msg_type, from_call, to_call, subject, body,
                 ts, band, freq_hz, snr, read_status, folder, file_path, vmail_guid, is_deleted,
                 urgent, has_attachment, via_callsign)
            VALUES
                ('varac-a', 3, 'guid-3', 'inbox', 'VMail', 'N1CCC', 'N1MAG', 'VarAC subject',
                 'VarAC body', 1788352920, '20m', 14078000, -12, 0, 'Inbox', '/tmp/vmail.txt',
                 'vmail-3', 0, 1, 1, 'N1VIA')
            """
        )
        conn.execute(
            """
            INSERT INTO sitrep_events
                (report_key, event_ts, event_ts_utc, from_call, target, report_group, grid,
                 state_code, scope, subtype, overall_status, power, water, medical,
                 communications, internet, travel, food, fuel, crime, civil_unrest, political,
                 transport_mode, remarks_text, brevity_code, brevity_summary, source_first,
                 source_last, source_count, sources_json, source_refs_json, raw_payload_json,
                 inserted_ts, updated_ts)
            VALUES
                ('sitrep-4', 1788352980, '2026-09-02 10:03:00', 'N1DDD', '@MR08', '@MR08',
                 'DN40', 'UT', 'County', 'STATUS', 'yellow', 'yellow', 'green', 'green',
                 'yellow', 'green', 'green', 'green', 'yellow', 'green', 'green', 'green',
                 'js8', 'Fuel low', 'Y', 'Some degradation', 'JS8', 'JS8', 1, '[]', '[]', '{}',
                 1788352980, 1788352981)
            """
        )
        conn.execute(
            """
            INSERT INTO commstat_artifacts
                (artifact_key, artifact_kind, subtype, event_ts, event_ts_utc, from_call, target,
                 report_group, grid, state_code, scope, transport_mode, reach_mode, origin_path,
                 status_label, alert_color, title, body_text, remarks_text, source_first,
                 source_last, source_count, sources_json, source_refs_json, external_ids_json,
                 payload_json, inserted_ts, updated_ts)
            VALUES
                ('commstat-5', 'STATREP', 'RF', 1788353040, '2026-09-02 10:04:00',
                 'N1EEE', '@MR08', '@MR08', 'DN40', 'UT', 'County', 'js8', 'direct',
                 '/tmp/commstat.json', 'YELLOW', 'YELLOW', 'CommStat title', 'CommStat body',
                 'remarks', 'JS8', 'JS8', 1, '[]', '[]', '[]', '{}', 1788353040, 1788353041)
            """
        )
        conn.commit()
    finally:
        conn.close()

    result = project_native_message_sources(db_path, force=True)

    assert result == {"js8": 1, "spotter": 1, "varac": 1, "sitrep": 1, "commstat": 1}
    conn = _connect(db_path)
    try:
        families = {
            row["source_family"]: row["count"]
            for row in conn.execute(
                "SELECT source_family, COUNT(*) AS count FROM message_projection GROUP BY source_family"
            ).fetchall()
        }
        assert families == {"commstat": 1, "js8": 1, "sitrep": 1, "spotter": 1, "varac": 1}
        js8_row = conn.execute(
            "SELECT state_code, grid, summary FROM message_projection WHERE source_family='js8'"
        ).fetchone()
        assert dict(js8_row) == {
            "state_code": "UT",
            "grid": "DN40",
            "summary": "F!701C | N1AAA -> @MR08 | UT / DN40 | Green / No significant issues",
        }
        spotter_row = conn.execute(
            "SELECT state_code, grid, summary FROM message_projection WHERE source_family='spotter'"
        ).fetchone()
        assert dict(spotter_row) == {
            "state_code": "UT",
            "grid": "DN40",
            "summary": "F!104 | N1BBB -> @MR08 | UT / DN40 | Water outage",
        }
        assert conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0] == 5
        assert conn.execute("SELECT COUNT(*) FROM message_artifacts").fetchone()[0] == 2
    finally:
        conn.close()


def test_source_native_projectors_skip_unchanged_sources_by_checkpoint(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        _ensure_js8(conn)
        conn.execute(
            """
            INSERT INTO js8_messages
                (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text, state,
                 read_ts, source_key, source_id)
            VALUES
                (1, 'N1AAA', '@MR08', 'MSG', '2026-09-02 10:00:00', 1788352800,
                 'hello', 'hello', 'UNREAD', 0, 'radio-a', 101)
            """
        )
        conn.commit()
    finally:
        conn.close()

    assert project_native_message_sources(db_path, sources=("js8",), force=False)["js8"] == 1
    assert project_native_message_sources(db_path, sources=("js8",), force=False)["js8"] == 0


def test_local_js8_commstat_is_summarized_without_changing_rf_source(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        _ensure_js8(conn)
        conn.executemany(
            """
            INSERT INTO js8_messages
                (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text,
                 state, source_key, source_id, source_radio_id, js8_instance_id, source_path)
            VALUES (?, 'N1MAG', '@MAGNET', 'MSG', '2026-09-02 10:00:00', 1788352800,
                    ?, '', 'UNREAD', 'radio-a', ?, '7', 'js8-a', '/tmp/js8.db')
            """,
            (
                (1, "N1MAG: MAGNET ,EM12JV,1,A03,121111111111,Power intermittent,{&%}", 101),
                # The internet marker must not receive the RF CommStat treatment.
                (2, "N1MAG: MAGNET ,EM12JV,1,A04,+,Internet mirror,{&%3}", 102),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    assert project_native_message_sources(db_path, sources=("js8",), force=True) == {"js8": 2}
    conn = _connect(db_path)
    try:
        local = conn.execute(
            """
            SELECT source_family, radio_id, app_instance_id, message_type, display_type,
                   status, severity, subject, summary, entities_json
              FROM message_projection
             WHERE message_id IN (
                 SELECT message_id FROM message_external_refs WHERE external_key='101'
             )
            """
        ).fetchone()
        mirrored = conn.execute(
            """
            SELECT message_type, display_type, entities_json
              FROM message_projection
             WHERE message_id IN (
                 SELECT message_id FROM message_external_refs WHERE external_key='102'
             )
            """
        ).fetchone()
    finally:
        conn.close()

    assert dict(local) | {"entities_json": ""} == {
        "source_family": "js8",
        "radio_id": 7,
        "app_instance_id": "js8-a",
        "message_type": "CommStat/COMMSTAT_12",
        "display_type": "CommStat",
        "status": "GREEN",
        "severity": "info",
        "subject": "CommStat · Green · Power Yellow",
        "summary": "CommStat | Green · Power Yellow | EM12JV | Power intermittent",
        "entities_json": "",
    }
    assert '"commstat_raw_evidence"' in local["entities_json"]
    assert mirrored["message_type"] == "MSG"
    assert mirrored["display_type"] == "JS8"
    assert "commstat_subtype" not in mirrored["entities_json"]


def test_local_js8_commstat_helper_has_compact_status_and_rejects_internet_marker() -> None:
    classified = _analyze_local_js8_commstat(
        raw_payload="N1MAG: MAGNET ,EM12JV,1,A03,123111111111,Power and water issue,{&%}",
        decoded_payload="",
        from_call="N1MAG",
        to_call="@MAGNET",
        event_utc="2026-09-02 10:00:00",
    )

    assert classified is not None
    assert classified["form_name"] == "CommStat/COMMSTAT_12"
    assert classified["status"] == "GREEN"
    assert classified["summary"] == "CommStat | Green · Power Yellow, Water Red | EM12JV | Power and water issue"
    assert classified["entities"]["commstat_transport"] == "js8"
    assert _analyze_local_js8_commstat(
        raw_payload="N1MAG: MAGNET ,EM12JV,1,A04,+,Internet mirror,{&%3}",
        decoded_payload="",
        from_call="N1MAG",
        to_call="@MAGNET",
        event_utc="2026-09-02 10:00:00",
    ) is None


def test_local_js8_rrsr_is_a_commstat_status_receipt() -> None:
    classified = _analyze_local_js8_commstat(
        raw_payload="W4WYD: @MAGNET RRSR N6KYL,L42",
        decoded_payload="",
        from_call="W4WYD",
        to_call="@MAGNET",
        event_utc="2026-09-15 02:00:00",
    )

    assert classified is not None
    assert classified["form_name"] == "CommStat/Status receipt"
    assert classified["status"] == "INFO"
    assert classified["summary"] == "W4WYD acknowledged N6KYL status report L42"
    assert classified["entities"]["acknowledged_callsign"] == "N6KYL"
    assert classified["entities"]["acknowledged_report_id"] == "L42"


def test_spotter_projector_marks_imported_history_without_claiming_local_rf(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        _ensure_spotter(conn)
        conn.execute(
            """
            CREATE TABLE js8spotter_import_log (
                source_db TEXT, source_table TEXT, source_id TEXT,
                source_fingerprint TEXT, imported_kind TEXT,
                imported_id TEXT, imported_ts REAL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO spotter_traffic
                (id, utc_str, utc_ts, from_call, to_call, form_id, raw_text,
                 decoded_text, state, source_radio_id, js8_instance_id)
            VALUES (9, '2026-09-02 10:00:00', 1788352800, 'N1AAA', '@MR08',
                    '304', 'F!304 OK', 'F!304 OK', 'UNREAD', '7', 'js8-a')
            """
        )
        conn.execute(
            """
            INSERT INTO js8spotter_import_log
                (source_db, source_table, source_id, source_fingerprint,
                 imported_kind, imported_id, imported_ts)
            VALUES ('/tmp/import.db', 'forms', '9', 'hash', 'spotter_traffic', '9', 1)
            """
        )
        conn.commit()
    finally:
        conn.close()

    assert project_native_message_sources(db_path, sources=("spotter",), force=True) == {"spotter": 1}
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT source_label, entities_json FROM message_projection WHERE source_family='spotter'"
        ).fetchone()
    finally:
        conn.close()
    assert row["source_label"].startswith("Imported JS8Spotter")
    assert '"ingest_origin":"js8spotter-db-import"' in row["entities_json"]
    assert '"local_rf_received":false' in row["entities_json"]


def test_native_file_records_project_artifacts_and_skip_by_checkpoint(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    file_path = tmp_path / "Q123-04.k2s"
    file_path.write_text("payload", encoding="utf-8")
    st = file_path.stat()
    records = {
        "flamp": [
            FileRecord(
                path=file_path,
                origin="flamp",
                size=st.st_size,
                mtime=st.st_mtime,
                source_id="flamp:radio-a",
                source_label="Radio A FLAmp",
            )
        ]
    }

    assert project_native_file_records(db_path, records, force=False) == 1
    assert project_native_file_records(db_path, records, force=False) == 0

    conn = _connect(db_path)
    try:
        projection = conn.execute("SELECT source_family, subject FROM message_projection").fetchone()
        ref = conn.execute("SELECT external_path, delete_capability FROM message_external_refs").fetchone()
        artifact = conn.execute("SELECT artifact_type, q_id, block_id FROM message_artifacts").fetchone()
    finally:
        conn.close()

    assert projection["source_family"] == "flamp"
    assert projection["subject"] == file_path.name
    assert ref["external_path"] == str(file_path)
    assert ref["delete_capability"] == "file_delete"
    assert artifact["artifact_type"] == "flamp_transfer"
    assert artifact["q_id"] == "Q123"
    assert artifact["block_id"] == "04"


def test_js8_receipts_keep_api_and_directed_sources_but_share_one_station_message(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        _ensure_js8(conn)
        conn.executemany(
            """
            INSERT INTO js8_messages
                (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text,
                 decoded_text, state, source_key, source_id, source_radio_id,
                 js8_instance_id, source_path)
            VALUES (?, 'N1AAA', '@MR08', 'MSG', '2026-09-22 10:00:00',
                    1790071200, 'same traffic', 'same traffic', 'UNREAD', ?, ?,
                    '7', 'js8-a', ?)
            """,
            (
                (1, "radio-a-api", 101, ""),
                (2, "radio-a-directed", 202, "/radio-a/DIRECTED.TXT"),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    assert project_native_message_sources(db_path, sources=("js8",), force=True) == {"js8": 2}
    conn = _connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 1
        refs = conn.execute(
            """
            SELECT r.source_id, s.source_label, r.metadata_json
              FROM message_external_refs r JOIN message_sources s USING(source_id)
             ORDER BY r.source_id
            """
        ).fetchall()
        assert len(refs) == 2
        assert {row["source_id"] for row in refs} == {
            "js8:api:radio-a-api", "js8:directed_txt:radio-a-directed"
        }
        assert any("JS8Call API" in row["source_label"] for row in refs)
        assert any("JS8Call DIRECTED.TXT" in row["source_label"] for row in refs)
    finally:
        conn.close()


def test_same_js8_text_at_a_different_event_time_is_not_collapsed(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        _ensure_js8(conn)
        conn.executemany(
            """
            INSERT INTO js8_messages
                (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text,
                 decoded_text, state, source_key, source_id)
            VALUES (?, 'N1AAA', '@MR08', 'MSG', ?, ?, 'repeat', 'repeat',
                    'UNREAD', 'radio-a-api', ?)
            """,
            (
                (1, "2026-09-22 10:00:00", 1790071200, 101),
                (2, "2026-09-22 10:00:01", 1790071201, 102),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    project_native_message_sources(db_path, sources=("js8",), force=True)
    conn = _connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 2
    finally:
        conn.close()


def test_spotter_replay_sources_share_presentation_and_keep_both_receipts(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        _ensure_spotter(conn)
        conn.executemany(
            """
            INSERT INTO spotter_traffic
                (id, utc_str, utc_ts, from_call, to_call, form_id, spotter_token,
                 raw_text, decoded_text, state, source_radio_id, js8_instance_id)
            VALUES (?, '2026-09-22 10:00:00', 1790071200, 'N1AAA', 'N1MAG',
                    '701C', '#ABCD', 'F!701C 100 ST[UT] GR[DN40] #ABCD',
                    'decoded', 'UNREAD', ?, ?)
            """,
            ((1, "7", "legacy"), (2, "8", "qualified")),
        )
        conn.commit()
    finally:
        conn.close()

    project_native_message_sources(db_path, sources=("spotter",), force=True)
    conn = _connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM message_sources").fetchone()[0] == 2
    finally:
        conn.close()


def test_identical_flmsg_and_flamp_files_dedupe_within_family_and_keep_folder_receipts(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    records = {}
    for origin in ("flmsg", "flamp"):
        family_records = []
        for radio in ("a", "b"):
            path = tmp_path / f"{origin}-{radio}" / "message.txt"
            path.parent.mkdir()
            path.write_text("same completed message", encoding="utf-8")
            family_records.append(
                FileRecord(
                    path,
                    origin,
                    path.stat().st_size,
                    path.stat().st_mtime,
                    f"{origin}:radio-{radio}",
                    f"Radio {radio.upper()} {origin}",
                )
            )
        records[origin] = family_records

    assert project_native_file_records(db_path, records, force=True) == 4
    conn = _connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0] == 4
        assert conn.execute("SELECT COUNT(DISTINCT source_id) FROM message_external_refs").fetchone()[0] == 4
        assert dict(conn.execute(
            "SELECT source_family, COUNT(*) FROM message_projection GROUP BY source_family"
        ).fetchall()) == {"flamp": 1, "flmsg": 1}
    finally:
        conn.close()
