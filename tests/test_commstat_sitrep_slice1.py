from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from freqinout.core.commstat_sitrep import parse_commstat_message
from freqinout.core.sitrep_ingest import _ensure_local_tables, _ingest_commstat3


def _write_brevity_assets(base: Path, list_id: str = "4") -> None:
    data = {
        "emergency_type": {
            "B": {"name": "Bridge Failure"},
        },
        "status_codes": {
            "B": {"name": "Pending"},
        },
        "public_reaction": {
            "U": {"name": "Unrest"},
        },
        "station_response": {
            "B": {"name": "Backup Power"},
        },
        "shared_impacts": {
            "G": {"name": "Grid Down"},
        },
    }
    path = base / f"{list_id}-Test Events.json"
    path.write_text(json.dumps(data), encoding="utf-8")


def _create_commstat3_messages_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
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
        conn.commit()
    finally:
        conn.close()


def _create_commstat3_statrep_db(path: Path) -> None:
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
        conn.commit()
    finally:
        conn.close()


def test_parse_commstat_standard_message_extracts_state_and_brevity(tmp_path: Path) -> None:
    _write_brevity_assets(tmp_path)

    parsed = parse_commstat_message(
        "N0DDK: @MAGNET ,EM83,3,T31,321311111331,NTR OR 4BBGUB,{&%}",
        target_hint="@MAGNET",
        source_value=1,
        asset_dir=tmp_path,
    )

    assert parsed is not None
    assert parsed["subtype"] == "COMMSTAT_12"
    assert parsed["grid"] == "EM83"
    assert parsed["scope"] == "My County"
    assert parsed["status_payload"]["status"] == "321311111331"
    assert parsed["metadata"]["report_group"] == "@MAGNET"
    assert parsed["metadata"]["transport_mode"] == "js8"
    assert parsed["metadata"]["remarks_text"] == "NTR OR 4BBGUB"
    assert parsed["metadata"]["brevity_code"] == "4BBGUB"
    assert "Bridge Failure" in parsed["metadata"]["brevity_summary"]
    assert parsed["metadata"]["state_code"] == "OR"
    assert parsed["metadata"]["state_confidence"] == "grid4_remarks"
    assert parsed["metadata"]["geo_confidence"] == "grid4_state"


def test_parse_commstat_internet_marker_sets_internet_transport(tmp_path: Path) -> None:
    parsed = parse_commstat_message(
        "W1ABC: @SITREP ,FN31,1,R12,+,CT,{&%3}",
        target_hint="@SITREP",
        source_value=1,
        asset_dir=tmp_path,
    )

    assert parsed is not None
    assert parsed["metadata"]["transport_mode"] == "internet"
    assert parsed["status_payload"]["status"] == "111111111111"


def test_ingest_commstat3_messages_only_populates_metadata(tmp_path: Path) -> None:
    _write_brevity_assets(tmp_path)
    source_db = tmp_path / "traffic.db3"
    _create_commstat3_messages_db(source_db)

    conn = sqlite3.connect(source_db)
    try:
        conn.execute(
            """
            INSERT INTO messages(datetime, date, freq, db, source, msg_id, from_callsign, target, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-04-09 16:32:32",
                "2026-04-09",
                7110000.0,
                30,
                3,
                "M42",
                "N0DDK",
                "@MAGNET",
                "N0DDK: @MAGNET ,EM83,3,T31,321311111331,NTR OR 4BBGUB,{&%3}",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    local_conn = sqlite3.connect(":memory:")
    try:
        _ensure_local_tables(local_conn)
        stats = _ingest_commstat3(local_conn, source_db, max_rows=50)
        assert stats["events_inserted"] == 1

        row = local_conn.execute(
            """
            SELECT subtype, report_group, grid, scope, transport_mode, remarks_text,
                   brevity_code, brevity_summary, state_code, state_confidence, geo_confidence,
                   status_payload, raw_payload
            FROM sitrep_source_events
            """
        ).fetchone()
        assert row is not None
        assert row[0] == "COMMSTAT_12"
        assert row[1] == "@MAGNET"
        assert row[2] == "EM83"
        assert row[3] == "My County"
        assert row[4] == "internet"
        assert row[5] == "NTR OR 4BBGUB"
        assert row[6] == "4BBGUB"
        assert "Grid Down" in row[7]
        assert row[8] == "OR"
        assert row[9] == "grid4_remarks"
        assert row[10] == "grid4_state"

        status_payload = json.loads(row[11])
        raw_payload = json.loads(row[12])
        assert status_payload["status"] == "321311111331"
        assert raw_payload["sr_id"] == "T31"
        assert raw_payload["msg_id"] == "M42"
    finally:
        local_conn.close()


def test_ingest_commstat3_statrep_populates_transport_and_geo(tmp_path: Path) -> None:
    source_db = tmp_path / "traffic.db3"
    _create_commstat3_statrep_db(source_db)

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
                "2026-04-09 16:40:42",
                "2026-04-09",
                7110000.0,
                30,
                2,
                "R40",
                "W8APP",
                "@AMRRON",
                "EN82ER",
                "My Location",
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
                "1",
                "1",
                "MI BEAUTIFUL SUNNY WARM DAY SO FAR",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    local_conn = sqlite3.connect(":memory:")
    try:
        _ensure_local_tables(local_conn)
        stats = _ingest_commstat3(local_conn, source_db, max_rows=50)
        assert stats["events_inserted"] == 1

        row = local_conn.execute(
            """
            SELECT report_group, transport_mode, state_code, state_confidence, geo_confidence
            FROM sitrep_source_events
            """
        ).fetchone()
        assert row == ("@AMRRON", "internet", "MI", "explicit", "grid6")
    finally:
        local_conn.close()
