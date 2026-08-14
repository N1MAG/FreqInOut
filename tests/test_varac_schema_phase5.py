from __future__ import annotations

import importlib
import sqlite3

from freqinout.core.varac_ingest import ensure_varac_local_tables


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _table_pk_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row[1] or "") for row in sorted((row for row in rows if int(row[5] or 0)), key=lambda row: int(row[5] or 0))]


def test_ensure_varac_local_tables_creates_summary_tables():
    conn = sqlite3.connect(":memory:")
    try:
        ensure_varac_local_tables(conn)
        assert {"callsign", "last_seen_ts", "last_band", "last_freq_hz", "last_snr", "last_source"} == _table_columns(
            conn, "varac_callsign_stats"
        )
        assert {"ts", "origin", "destination", "snr", "band", "freq_hz", "source", "ingest_source_key"} == _table_columns(
            conn, "varac_links"
        )
        assert "ingest_source_key" in _table_columns(conn, "varac_messages")
        assert _table_pk_columns(conn, "varac_messages") == ["ingest_source_key", "source", "id"]
    finally:
        conn.close()


def test_db_initializer_ensures_varac_tables_on_cold_start(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)
    db_initializer.ensure_all_tables()

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        assert {"callsign", "last_seen_ts", "last_band", "last_freq_hz", "last_snr", "last_source"} == _table_columns(
            conn, "varac_callsign_stats"
        )
        assert {"id", "guid", "source", "msg_type", "from_call", "to_call"} <= _table_columns(conn, "varac_messages")
        assert "ingest_source_key" in _table_columns(conn, "varac_messages")
        assert "ingest_source_key" in _table_columns(conn, "varac_links")
        assert _table_pk_columns(conn, "varac_messages") == ["ingest_source_key", "source", "id"]
    finally:
        conn.close()


def test_db_initializer_migrates_legacy_varac_links_before_source_index(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    nets_db = config_dir / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        conn.execute(
            """
            CREATE TABLE varac_links (
                ts REAL,
                origin TEXT,
                destination TEXT,
                snr REAL,
                band TEXT,
                freq_hz REAL,
                source TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)
    db_initializer.ensure_nets_tables()

    conn = sqlite3.connect(nets_db)
    try:
        assert "ingest_source_key" in _table_columns(conn, "varac_links")
        indexes = {
            row[1]
            for row in conn.execute("PRAGMA index_list(varac_links)").fetchall()
        }
        assert "idx_varac_links_source_ts" in indexes
    finally:
        conn.close()
