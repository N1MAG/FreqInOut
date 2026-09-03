from __future__ import annotations

import sqlite3

from freqinout.core.commstat_artifacts import ensure_commstat_artifact_tables
from freqinout.core.db_initializer import _ensure_sitrep_fusion_tables
from freqinout.core.message_projection_store import ensure_message_projection_schema
from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.message_source_projectors import project_native_file_records, project_native_message_sources
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
                 'Power report', 'Power report decoded', 'UNREAD', 0, 'radio-a', 101, '7', 'js8-a', '/tmp/js8.db')
            """
        )
        conn.execute(
            """
            INSERT INTO spotter_traffic
                (id, utc_str, utc_ts, from_call, to_call, form_id, spotter_token, raw_text,
                 decoded_text, state, read_ts, relay_via, source_radio_id, js8_instance_id)
            VALUES
                (2, '2026-09-02 10:01:00', 1788352860, 'N1BBB', '@MR08', '104',
                 'tok', 'F!104 GREEN', 'Spotter decoded', 'UNREAD', 0, 'N1RELAY', '8', 'js8-b')
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
