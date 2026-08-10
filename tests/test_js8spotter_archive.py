from pathlib import Path
import sqlite3
import time

from freqinout.core.js8spotter_archive import load_js8spotter_archive_records, spotter_archive_table_counts


def test_js8spotter_archive_loader_summarizes_searchable_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE js8spotter_import_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_db TEXT NOT NULL,
                source_table TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                imported_ts REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO js8spotter_import_archive
                (source_db, source_table, source_id, source_fingerprint, payload_json, imported_ts)
            VALUES
                ('spotter.db', 'grid', 'N0CALL', 'a', '{"grid_callsign":"N0CALL","grid_grid":"DN70AA","grid_snr":"-12","grid_timestamp":"2026-08-08 12:36:00"}', ?),
                ('spotter.db', 'search', 'MAGNET', 'b', '{"keyword":"MAGNET","comment":"watch net traffic"}', ?),
                ('spotter.db', 'setting', 'theme', 'c', '{"key":"theme","value":"legacy"}', ?)
            """,
            (time.time(), time.time() - 1, time.time() - 2),
        )
        conn.commit()
    finally:
        conn.close()

    records = load_js8spotter_archive_records(db_path=db_path)
    counts = spotter_archive_table_counts(db_path=db_path)

    assert counts == {"grid": 1, "search": 1, "setting": 1}
    assert [record.source_table for record in records] == ["grid", "search"]
    assert records[0].source_db == "spotter.db"
    assert records[0].title == "N0CALL DN70AA"
    assert "DN70AA" in records[0].keywords
    assert records[1].title == "MAGNET"


def test_js8spotter_archive_loader_is_quiet_when_table_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.db"
    sqlite3.connect(db_path).close()

    assert load_js8spotter_archive_records(db_path=db_path) == []
    assert spotter_archive_table_counts(db_path=db_path) == {}
