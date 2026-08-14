from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from freqinout.core import sitrep_ingest
from freqinout.core.commstat_sitrep import (
    commstat_origin_path,
    commstat_reach_label,
    commstat_reach_mode,
    commstat_transport_label,
    parse_commstat_message,
    transport_mode_for_source,
)
from freqinout.core.sitrep_metadata import source_family_label
from freqinout.core.sitrep_ingest import _ensure_local_tables, _ingest_commstat3, _ingest_imported_js8spotter_archive


class DummySettings:
    def __init__(self, values: dict[str, object] | None = None):
        self._values = dict(values or {})

    def get(self, key: str, default=None):
        return self._values.get(key, default)

    def set(self, key: str, value):
        self._values[key] = value


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
    assert parsed["metadata"]["report_group"] == "MAGNET"
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


def test_commstat_reach_mode_distinguishes_rf_limited_and_maximum() -> None:
    assert commstat_origin_path(1) == "rf"
    assert commstat_reach_mode(1, global_id=0) == "rf_observed"
    assert transport_mode_for_source(1, global_id=0) == "js8"

    assert commstat_origin_path(1) == "rf"
    assert commstat_reach_mode(1, global_id=42) == "maximum_reach"
    assert transport_mode_for_source(1, global_id=42) == "js8+internet"

    assert commstat_origin_path(2) == "commstat_server"
    assert commstat_reach_mode(2) == "maximum_reach_relay"
    assert transport_mode_for_source(2) == "internet"

    assert commstat_origin_path(3) == "internet_only"
    assert commstat_reach_mode(3) == "internet_only"
    assert transport_mode_for_source(3) == "internet"


def test_commstat_reach_and_transport_labels_are_operator_readable() -> None:
    assert commstat_reach_label("rf_observed") == "Limited Reach (RF only)"
    assert commstat_reach_label("maximum_reach") == "Maximum Reach (RF + Internet)"
    assert commstat_reach_label("maximum_reach_relay") == "Maximum Reach relay"
    assert commstat_reach_label("internet_only") == "Internet only"
    assert commstat_transport_label("js8") == "JS8/RF"
    assert commstat_transport_label("js8+internet") == "JS8/RF + Internet"
    assert commstat_transport_label("internet") == "Internet"


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
        assert row[1] == "MAGNET"
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
                global_id, datetime, date, freq, db, source, sr_id, from_callsign, target, grid, scope,
                map, power, water, med, telecom, travel, internet, fuel, food, crime, civil, political, comments
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                99,
                "2026-04-09 16:40:42",
                "2026-04-09",
                7110000.0,
                30,
                1,
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
            SELECT report_group, transport_mode, reach_mode, origin_path, state_code, state_confidence, geo_confidence,
                   raw_payload
            FROM sitrep_source_events
            """
        ).fetchone()
        assert row[:7] == ("AMRRON", "js8+internet", "maximum_reach", "rf", "MI", "explicit", "grid6")
        assert json.loads(row[7])["global_id"] == 99

        artifact = local_conn.execute(
            """
            SELECT transport_mode, reach_mode, origin_path, payload_json
            FROM commstat_artifacts
            """
        ).fetchone()
        assert artifact[:3] == ("js8+internet", "maximum_reach", "rf")
        assert json.loads(artifact[3])["reach_mode"] == "maximum_reach"
    finally:
        local_conn.close()


def test_ingest_imported_js8spotter_csstatrep_archives_into_sitrep_events() -> None:
    local_conn = sqlite3.connect(":memory:")
    try:
        _ensure_local_tables(local_conn)
        local_conn.execute(
            """
            CREATE TABLE js8spotter_import_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_db TEXT NOT NULL,
                source_table TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                imported_ts REAL NOT NULL,
                UNIQUE(source_db, source_table, source_id, source_fingerprint)
            )
            """
        )
        local_conn.execute(
            """
            INSERT INTO js8spotter_import_archive
                (source_db, source_table, source_id, source_fingerprint, payload_json, imported_ts)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "/legacy/js8spotter.db",
                "csstatrep",
                "42",
                "fp-42",
                json.dumps(
                    {
                        "cssr_from": "N0DDK",
                        "cssr_group": "MAGNET",
                        "cssr_grid": "EM83",
                        "cssr_prio": "My County",
                        "cssr_msgid": "CSR-42",
                        "cssr_status": "321311111331",
                        "cssr_notes": "NTR OR 4BBGUB",
                        "cssr_timestamp": "2026-04-09 16:40:42",
                    }
                ),
                1_710_000_000.0,
            ),
        )
        local_conn.commit()

        stats = _ingest_imported_js8spotter_archive(local_conn, "freqinout_nets.db", max_rows=50)

        assert stats["rows_scanned"] == 1
        assert stats["events_inserted"] == 1
        row = local_conn.execute(
            """
            SELECT source, source_table, from_call, target, report_group, grid, transport_mode,
                   status_payload, raw_payload
            FROM sitrep_source_events
            """
        ).fetchone()
        assert row is not None
        assert row[:7] == ("JS8SPOTTER_IMPORT", "csstatrep", "N0DDK", "MAGNET", "MAGNET", "EM83", "js8")
        assert json.loads(row[7])["status"] == "321311111331"
        raw_payload = json.loads(row[8])
        assert raw_payload["source_db"] == "/legacy/js8spotter.db"
        assert raw_payload["source_id"] == "42"

        artifact = local_conn.execute(
            """
            SELECT source_first, source_last, source_refs_json, from_call, report_group, title
            FROM commstat_artifacts
            """
        ).fetchone()
        assert artifact is not None
        assert artifact[0] == "JS8SPOTTER_IMPORT"
        assert artifact[1] == "JS8SPOTTER_IMPORT"
        assert json.loads(artifact[2]) == ["csstatrep:1"]
        assert artifact[3] == "N0DDK"
        assert artifact[4] == "MAGNET"
    finally:
        local_conn.close()


def test_imported_js8spotter_source_uses_existing_family_label() -> None:
    assert source_family_label("JS8SPOTTER_IMPORT") == "JS8Spotter"


def test_imported_js8spotter_archive_errors_do_not_count_source_ok(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    try:
        _ensure_local_tables(conn)
        conn.execute(
            """
            CREATE TABLE js8spotter_import_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_table TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(sitrep_ingest, "_local_db_path", lambda: db_path)
    monkeypatch.setattr(sitrep_ingest, "_LAST_RUN_MONO", 0.0)
    settings = DummySettings(
        {
            "sitrep_unified_ingest_enabled": True,
            "sitrep_ingest_local_spotter_backfill_enabled": False,
            "sitrep_ingest_imported_js8spotter_archive_enabled": True,
            "sitrep_ingest_js8spotter_enabled": False,
            "sitrep_ingest_commstat3_enabled": False,
            "sitrep_ingest_commstat23_enabled": False,
        }
    )

    stats = sitrep_ingest.ingest_sitreps(settings, max_rows_per_source=50)

    assert stats["sources_attempted"] == 1
    assert stats["errors"] == 1
    assert stats["sources_ok"] == 0
