from __future__ import annotations

import sqlite3
from pathlib import Path

from freqinout.core.sitrep_fusion import _build_report_key
from freqinout.core.sitrep_ingest import _ensure_local_tables, _ingest_commstat3


def _create_commstat3_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE statrep (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                global_id INTEGER DEFAULT 0,
                pinned INTEGER DEFAULT 0,
                datetime TEXT,
                date TEXT,
                freq DOUBLE,
                db INTEGER,
                source INTEGER,
                sr_id TEXT,
                from_callsign TEXT,
                target TEXT,
                grid TEXT,
                scope TEXT,
                map TEXT,
                power TEXT,
                water TEXT,
                med TEXT,
                telecom TEXT,
                travel TEXT,
                internet TEXT,
                fuel TEXT,
                food TEXT,
                crime TEXT,
                civil TEXT,
                political TEXT,
                comments TEXT,
                memo TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime TEXT,
                date TEXT,
                freq DOUBLE,
                db INTEGER,
                source INTEGER,
                msg_id TEXT,
                from_callsign TEXT,
                target TEXT,
                message TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime TEXT,
                date TEXT,
                freq DOUBLE,
                db INTEGER,
                source INTEGER,
                alert_id TEXT,
                from_callsign TEXT,
                target TEXT,
                color TEXT,
                title TEXT,
                message TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_commstat3_plain_message_becomes_first_class_message(tmp_path: Path) -> None:
    source_db = tmp_path / "traffic.db3"
    _create_commstat3_db(source_db)
    conn = sqlite3.connect(source_db)
    try:
        conn.execute(
            """
            INSERT INTO messages(datetime, date, freq, db, source, msg_id, from_callsign, target, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-05-01 10:01:00",
                "2026-05-01",
                7110000.0,
                30,
                3,
                "M100",
                "N0DDK",
                "@MAGNET",
                "General advisory update for station staffing and local conditions.",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    local_conn = sqlite3.connect(":memory:")
    try:
        _ensure_local_tables(local_conn)
        stats = _ingest_commstat3(local_conn, source_db, max_rows=50)
        assert stats["rows_scanned"] == 1
        assert stats["events_inserted"] == 0
        row = local_conn.execute(
            """
            SELECT artifact_kind, report_group, transport_mode, status_label, body_text
            FROM commstat_artifacts
            """
        ).fetchone()
        assert row == (
            "MESSAGE",
            "MAGNET",
            "internet",
            "INFO",
            "General advisory update for station staffing and local conditions.",
        )
    finally:
        local_conn.close()


def test_commstat3_alert_becomes_first_class_alert(tmp_path: Path) -> None:
    source_db = tmp_path / "traffic.db3"
    _create_commstat3_db(source_db)
    conn = sqlite3.connect(source_db)
    try:
        conn.execute(
            """
            INSERT INTO alerts(datetime, date, freq, db, source, alert_id, from_callsign, target, color, title, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-05-01 10:02:00",
                "2026-05-01",
                7110000.0,
                30,
                3,
                "A200",
                "W8UFO",
                "@MAGNET",
                "RED",
                "Storm Surge Warning",
                "Move to elevated shelter locations immediately.",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    local_conn = sqlite3.connect(":memory:")
    try:
        _ensure_local_tables(local_conn)
        _ingest_commstat3(local_conn, source_db, max_rows=50)
        row = local_conn.execute(
            """
            SELECT artifact_kind, status_label, alert_color, title, body_text
            FROM commstat_artifacts
            """
        ).fetchone()
        assert row == (
            "ALERT",
            "RED",
            "RED",
            "RED ALERT | Storm Surge Warning",
            "Move to elevated shelter locations immediately.",
        )
    finally:
        local_conn.close()


def test_commstat3_statrep_and_message_copy_merge_into_one_artifact(tmp_path: Path) -> None:
    source_db = tmp_path / "traffic.db3"
    _create_commstat3_db(source_db)
    conn = sqlite3.connect(source_db)
    try:
        conn.execute(
            """
            INSERT INTO statrep(
                datetime, date, freq, db, source, sr_id, from_callsign, target, grid, scope,
                map, power, water, med, telecom, travel, internet, fuel, food, crime, civil, political, comments
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-05-01 10:03:15",
                "2026-05-01",
                7110000.0,
                30,
                2,
                "R300",
                "KC1UTT",
                "@MAGNET",
                "FN43AN",
                "3",
                "3",
                "2",
                "1",
                "1",
                "1",
                "1",
                "1",
                "1",
                "1",
                "1",
                "1",
                "1",
                "NTR NH 4BBGUB",
            ),
        )
        conn.execute(
            """
            INSERT INTO messages(datetime, date, freq, db, source, msg_id, from_callsign, target, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-05-01 10:03:40",
                "2026-05-01",
                7110000.0,
                30,
                3,
                "M301",
                "KC1UTT",
                "@MAGNET",
                "KC1UTT: @MAGNET ,FN43AN,3,R300,321111111111,NTR NH 4BBGUB,{&%3}",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    local_conn = sqlite3.connect(":memory:")
    try:
        _ensure_local_tables(local_conn)
        stats = _ingest_commstat3(local_conn, source_db, max_rows=50)
        assert stats["events_inserted"] == 2
        artifact_rows = local_conn.execute(
            """
            SELECT artifact_kind, from_call, report_group, source_count, source_refs_json, body_text
            FROM commstat_artifacts
            """
        ).fetchall()
        assert len(artifact_rows) == 1
        kind, from_call, group_name, source_count, refs_json, body_text = artifact_rows[0]
        assert kind == "STATREP"
        assert from_call == "KC1UTT"
        assert group_name == "MAGNET"
        assert source_count == 1
        assert "statrep:1" in refs_json
        assert "messages:1" in refs_json
        assert "NTR NH 4BBGUB" in body_text
    finally:
        local_conn.close()


def test_commstat_sitrep_report_key_is_semantic_across_external_ids() -> None:
    fields = {
        "overall_status": "red",
        "power": "yellow",
        "water": "green",
        "medical": "green",
        "communications": "green",
        "internet": "green",
        "travel": "green",
        "food": "green",
        "fuel": "green",
        "crime": "green",
        "civil_unrest": "green",
        "political": "green",
    }
    key_a = _build_report_key(
        subtype="COMMSTAT_12",
        from_call="KC1UTT",
        target="@MAGNET",
        grid="FN43AN",
        scope="My County",
        event_ts=1_777_631_420.0,
        fields=fields,
        raw_payload={"sr_id": "R300"},
    )
    key_b = _build_report_key(
        subtype="COMMSTAT_12",
        from_call="KC1UTT",
        target="@MAGNET",
        grid="FN43AN",
        scope="My County",
        event_ts=1_777_631_450.0,
        fields=fields,
        raw_payload={"msg_id": "M301"},
    )
    assert key_a == key_b
