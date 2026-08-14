from __future__ import annotations

import sqlite3
from pathlib import Path

from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.db_initializer import (
    _ensure_prop_contact_events,
    _ensure_prop_ingest_checkpoint,
    _ensure_prop_outcome_stats,
)
from freqinout.core.propagation_outcome_ingest import ingest_propagation_outcomes


class DictSettings:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)


def _prepare_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        ensure_operator_checkins_schema(conn)
        _ensure_prop_contact_events(conn)
        _ensure_prop_ingest_checkpoint(conn)
        _ensure_prop_outcome_stats(conn)
        conn.execute(
            """
            CREATE TABLE js8_messages (
                id INTEGER PRIMARY KEY,
                source_key TEXT,
                source_id INTEGER,
                source_radio_id TEXT,
                js8_instance_id TEXT,
                from_call TEXT,
                to_call TEXT,
                msg_type TEXT,
                utc_str TEXT,
                utc_ts REAL
            )
            """
        )
        conn.execute(
            "INSERT INTO operator_checkins (callsign, state, grid) VALUES ('K1AAA', 'CO', 'DM79QJ')"
        )
        conn.execute(
            "INSERT INTO operator_checkins (callsign, state, grid) VALUES ('K2BBB', 'UT', 'DM38ST')"
        )
        conn.commit()
    finally:
        conn.close()


def test_propagation_outcome_ingest_projects_late_imported_older_rows(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    db_path = profile_root / "config" / "freqinout_nets.db"
    _prepare_db(db_path)
    settings = DictSettings({})

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO js8_messages
                (id, source_key, source_id, source_radio_id, js8_instance_id, from_call, to_call, msg_type, utc_str, utc_ts)
            VALUES (1, 'source-a', 1, 'A', 'js8-a', 'K1AAA', 'K2BBB', 'MSG', '2026-08-12 10:00:00', 1786538400)
            """
        )
        conn.commit()
    finally:
        conn.close()

    first = ingest_propagation_outcomes(settings, max_rows_per_source=50)
    assert first["events_inserted"] > 0

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO js8_messages
                (id, source_key, source_id, source_radio_id, js8_instance_id, from_call, to_call, msg_type, utc_str, utc_ts)
            VALUES (2, 'source-b', 1, 'B', 'js8-b', 'K1AAA', 'K2BBB', 'MSG', '2026-07-01 10:00:00', 1782900000)
            """
        )
        conn.commit()
    finally:
        conn.close()

    second = ingest_propagation_outcomes(settings, max_rows_per_source=50)
    assert second["events_inserted"] > 0

    conn = sqlite3.connect(db_path)
    try:
        refs = {
            row
            for row in conn.execute(
                "SELECT source_ref, source_key, app_instance_id, source_radio_id FROM prop_contact_events WHERE source_ref LIKE 'JS8_MESSAGES:%'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert ("JS8_MESSAGES:1", "source-a", "js8-a", "A") in refs
    assert ("JS8_MESSAGES:2", "source-b", "js8-b", "B") in refs


def test_propagation_outcome_ingest_resets_checkpoint_after_projection_rebuild(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    db_path = profile_root / "config" / "freqinout_nets.db"
    _prepare_db(db_path)
    settings = DictSettings({})

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO js8_messages
                (id, source_key, source_id, source_radio_id, js8_instance_id, from_call, to_call, msg_type, utc_str, utc_ts)
            VALUES (10, 'source-a', 10, 'A', 'js8-a', 'K1AAA', 'K2BBB', 'MSG', '2026-08-12 10:00:00', 1786538400)
            """
        )
        conn.commit()
    finally:
        conn.close()

    first = ingest_propagation_outcomes(settings, max_rows_per_source=50)
    assert first["events_inserted"] > 0

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM js8_messages")
        conn.execute(
            """
            INSERT INTO js8_messages
                (id, source_key, source_id, source_radio_id, js8_instance_id, from_call, to_call, msg_type, utc_str, utc_ts)
            VALUES (1, 'source-a', 1, 'A', 'js8-a', 'K1AAA', 'K2BBB', 'MSG', '2026-08-13 10:00:00', 1786624800)
            """
        )
        conn.commit()
    finally:
        conn.close()

    second = ingest_propagation_outcomes(settings, max_rows_per_source=50)
    assert second["rows_scanned"] > 0


def test_propagation_outcome_ingest_preserves_varac_link_source_key(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    db_path = profile_root / "config" / "freqinout_nets.db"
    _prepare_db(db_path)
    settings = DictSettings({})

    conn = sqlite3.connect(db_path)
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
                source TEXT,
                ingest_source_key TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO varac_links
                (ts, origin, destination, snr, band, freq_hz, source, ingest_source_key)
            VALUES (1786538400, 'K1AAA', 'K2BBB', -10, '20M', 14115000, 'qso', 'varac-a')
            """
        )
        conn.commit()
    finally:
        conn.close()

    result = ingest_propagation_outcomes(settings, max_rows_per_source=50)
    assert result["events_inserted"] > 0

    conn = sqlite3.connect(db_path)
    try:
        refs = {
            row
            for row in conn.execute(
                "SELECT source_ref, source_key FROM prop_contact_events WHERE source_ref LIKE 'VARAC_LINKS:%'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert ("VARAC_LINKS:1", "varac-a") in refs


def test_propagation_outcome_ingest_preserves_spotter_source_key(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    db_path = profile_root / "config" / "freqinout_nets.db"
    _prepare_db(db_path)
    settings = DictSettings({})

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE spotter_traffic (
                id INTEGER PRIMARY KEY,
                utc_ts REAL,
                from_call TEXT,
                to_call TEXT,
                source_key TEXT,
                js8_instance_id TEXT,
                source_radio_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO spotter_traffic
                (id, utc_ts, from_call, to_call, source_key, js8_instance_id, source_radio_id)
            VALUES (7, 1786538400, 'K1AAA', 'K2BBB', 'js8-directed-a', 'fio-a', 'A')
            """
        )
        conn.commit()
    finally:
        conn.close()

    result = ingest_propagation_outcomes(settings, max_rows_per_source=50)
    assert result["events_inserted"] > 0

    conn = sqlite3.connect(db_path)
    try:
        refs = {
            row
            for row in conn.execute(
                "SELECT source_ref, source_key, app_instance_id, source_radio_id FROM prop_contact_events WHERE source_ref LIKE 'SPOTTER_TRAFFIC:%'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert ("SPOTTER_TRAFFIC:7", "js8-directed-a", "fio-a", "A") in refs
