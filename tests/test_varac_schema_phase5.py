from __future__ import annotations

import importlib
import sqlite3

from freqinout.core.varac_ingest import ensure_varac_local_tables


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_ensure_varac_local_tables_creates_summary_tables():
    conn = sqlite3.connect(":memory:")
    try:
        ensure_varac_local_tables(conn)
        assert {"callsign", "last_seen_ts", "last_band", "last_freq_hz", "last_snr", "last_source"} == _table_columns(
            conn, "varac_callsign_stats"
        )
        assert {"ts", "origin", "destination", "snr", "band", "freq_hz", "source"} == _table_columns(
            conn, "varac_links"
        )
        assert {"ingest_source_key", "table_name", "last_id"} == _table_columns(conn, "varac_ingest_state_v2")
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
        assert {
            "id",
            "guid",
            "source",
            "table_name",
            "ingest_source_key",
            "ingest_source_label",
            "cluster_name",
            "msg_type",
            "from_call",
            "to_call",
        } <= _table_columns(conn, "varac_messages")
    finally:
        conn.close()
