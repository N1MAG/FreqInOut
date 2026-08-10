from pathlib import Path
import sqlite3

from freqinout.core.js8_expect_store import list_expect_entries
from freqinout.core.js8_spotter_decode import decode_spotter_form_text, summarize_spotter_form_text
from freqinout.core.js8spotter_importer import import_js8spotter_database
from freqinout.core.observation_store import list_observations


def _make_spotter_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE forms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fromcall TEXT,
                tocall TEXT,
                typeid TEXT,
                responses TEXT,
                msgtxt TEXT,
                timesig TEXT,
                lm TIMESTAMP,
                gwtx TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO forms(fromcall, tocall, typeid, responses, msgtxt, timesig, lm, gwtx)
            VALUES ('N0CALL', '@MAGNET', 'F!304', '11111111', 'FIELD NOTE', '#HHJL', '2026-08-08 12:34:56', '')
            """
        )
        conn.execute(
            """
            CREATE TABLE expect (
                expect VARCHAR(6) PRIMARY KEY,
                reply TEXT,
                allowed TEXT,
                txlist TEXT,
                txmax INTEGER,
                lm TIMESTAMP,
                txspeed TEXT,
                autotx TEXT,
                atxtarget TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO expect(expect, reply, allowed, txlist, txmax, lm, txspeed, autotx, atxtarget)
            VALUES ('F!304', '@MAGNET F!304 11111111', '@MAGNET,N0CALL', '', 3, '2026-08-08 12:35:00', '0', '', '@MAGNET')
            """
        )
        conn.execute(
            """
            CREATE TABLE profile (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                def BOOLEAN DEFAULT 0,
                bgscan BOOLEAN DEFAULT 0,
                sort INT
            )
            """
        )
        conn.execute("INSERT INTO profile(title, def, bgscan, sort) VALUES ('Default', 1, 0, 1)")
        conn.execute(
            """
            CREATE TABLE grid (
                grid_callsign VARCHAR(64) PRIMARY KEY,
                grid_grid VARCHAR(16),
                grid_dial VARCHAR(64),
                grid_type VARCHAR(64),
                grid_snr VARCHAR(16),
                grid_timestamp TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO grid(grid_callsign, grid_grid, grid_dial, grid_type, grid_snr, grid_timestamp)
            VALUES ('N0CALL', 'DN70AA', '7078000', 'RX.SPOT', '-12', '2026-08-08 12:36:00')
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_js8spotter_importer_imports_forms_and_expect_idempotently(tmp_path: Path) -> None:
    source_db = tmp_path / "js8spotter.db"
    target_db = tmp_path / "freqinout_nets.db"
    _make_spotter_db(source_db)

    first = import_js8spotter_database(source_db, target_db=target_db, source_radio_id=7, js8_instance_id="fio-a")
    second = import_js8spotter_database(source_db, target_db=target_db, source_radio_id=7, js8_instance_id="fio-a")

    assert first.errors == []
    assert first.warnings == []
    assert first.forms_scanned == 1
    assert first.forms_imported == 1
    assert first.expect_scanned == 1
    assert first.expect_imported == 1
    assert second.forms_imported == 0
    assert second.expect_imported == 0
    assert first.archive_imported == 2
    assert first.grid_operators_updated == 1
    assert second.archive_imported == 0

    conn = sqlite3.connect(target_db)
    try:
        row = conn.execute(
            "SELECT from_call, to_call, form_id, spotter_token, raw_text, source_radio_id, js8_instance_id, decoded_text FROM spotter_traffic"
        ).fetchone()
        assert row[:7] == ("N0CALL", "@MAGNET", "304", "#HHJL", "F!304 11111111 FIELD NOTE #HHJL", "7", "fio-a")
        assert "Response Code: 11111111" in row[7]
        assert "Unparsed Text:" in row[7]
        imported = conn.execute("SELECT source_table, imported_kind FROM js8spotter_import_log ORDER BY source_table").fetchall()
        assert imported == [
            ("expect", "js8_expect_entries"),
            ("forms", "spotter_traffic"),
            ("grid", "js8spotter_import_archive"),
            ("profile", "js8spotter_import_archive"),
        ]
        archive_count = conn.execute("SELECT COUNT(*) FROM js8spotter_import_archive").fetchone()[0]
        assert archive_count == 2
        operator = conn.execute("SELECT callsign, grid, last_seen_utc FROM operator_checkins WHERE callsign='N0CALL'").fetchone()
        assert operator == ("N0CALL", "DN70AA", "2026-08-08 12:36:00")
    finally:
        conn.close()

    entries = list_expect_entries(db_path=target_db)
    assert len(entries) == 1
    assert entries[0]["expect_key"] == "F!304"
    assert entries[0]["allowed_groups"] == ["@MAGNET"]
    assert entries[0]["allowed_callsigns"] == ["N0CALL"]
    assert entries[0]["max_replies"] == 3

    observations = list_observations(target_db, source_family="spotter")
    assert len(observations) == 1
    assert observations[0].source_ref == "spotter_traffic:1"
    assert observations[0].from_call == "N0CALL"
    assert observations[0].to_target == "@MAGNET"
    assert observations[0].source_radio_id == 7
    assert observations[0].source_app == "fio-a"
    assert observations[0].route_eligible is False
    assert observations[0].publish_authorized is False
    assert observations[0].provenance["import_source"] == "js8spotter-db-import"


def test_spotter_bracket_decoder_makes_imported_mcforms_readable() -> None:
    raw = (
        "F!701 RUEHN7R TO[@MAGNET] FR[W0IFM] ST[MO] CC[USA] "
        "GR[EM48EQ] NA[FORM POSTED FOR PLACE HOLDER IN EXPECT SYSTEM] DA[260429-1839Z] #D2NT"
    )

    decoded = decode_spotter_form_text(raw, form_title="MAGNET SitRep")
    summary = summarize_spotter_form_text(raw, form_title="MAGNET SitRep")

    assert decoded.startswith("F!701 MAGNET SitRep")
    assert "Response Code: RUEHN7R" in decoded
    assert "To: @MAGNET" in decoded
    assert "From: W0IFM" in decoded
    assert "State: MO" in decoded
    assert "Grid: EM48EQ" in decoded
    assert "Name / Notes: FORM POSTED FOR PLACE HOLDER IN EXPECT SYSTEM" in decoded
    assert "Spotter Token: #D2NT" in decoded
    assert summary == "MO USA EM48EQ - FORM POSTED FOR PLACE HOLDER IN EXPECT SYSTEM (#D2NT)"


def test_js8spotter_importer_warns_when_source_has_no_inbox_history(tmp_path: Path) -> None:
    source_db = tmp_path / "blank_js8spotter.db"
    target_db = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(source_db)
    try:
        conn.execute("CREATE TABLE setting (key TEXT, value TEXT)")
        conn.execute("INSERT INTO setting(key, value) VALUES ('profile', 'Default')")
        conn.commit()
    finally:
        conn.close()

    stats = import_js8spotter_database(source_db, target_db=target_db, source_radio_id=7, js8_instance_id="fio-a")

    assert stats.forms_scanned == 0
    assert stats.expect_scanned == 0
    assert any("no form message history" in warning for warning in stats.warnings)
    assert any("no Expect rules" in warning for warning in stats.warnings)
