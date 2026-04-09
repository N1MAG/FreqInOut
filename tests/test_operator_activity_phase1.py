from __future__ import annotations

import sqlite3

from freqinout.core.operator_activity import (
    ensure_js8_callsign_stats,
    load_js8_direct_contact_summary,
    load_operator_activity_summary,
)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE js8_links (
            ts REAL,
            origin TEXT,
            destination TEXT,
            snr REAL,
            band TEXT,
            freq_hz REAL,
            is_relay INTEGER DEFAULT 0,
            relay_via TEXT,
            is_spotter INTEGER DEFAULT 0,
            last_seen_utc TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE operator_checkins (
            callsign TEXT PRIMARY KEY,
            last_seen_utc TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE varac_callsign_stats (
            callsign TEXT PRIMARY KEY,
            last_seen_ts REAL,
            last_band TEXT,
            last_freq_hz REAL
        )
        """
    )
    return conn


def test_activity_summary_uses_destination_only_rows_for_overall_last_seen() -> None:
    conn = _conn()
    conn.executemany(
        """
        INSERT INTO js8_links (ts, origin, destination, band, freq_hz)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (100.0, "N1MAG", "W9BVM", "40M", 7115000.0),
            (200.0, "KC1UTT", "W9BVM", "20M", 14115000.0),
        ],
    )

    summary = load_operator_activity_summary(conn)

    assert summary["W9BVM"]["overall_last_seen_ts"] == 200.0
    assert summary["W9BVM"]["overall_last_band"] == "20M"


def test_direct_contact_summary_ignores_outbound_only_attempts() -> None:
    conn = _conn()
    conn.execute(
        """
        INSERT INTO js8_links (ts, origin, destination, band, freq_hz)
        VALUES (?, ?, ?, ?, ?)
        """,
        (100.0, "N1MAG", "W9BVM", "20M", 14115000.0),
    )

    direct = load_js8_direct_contact_summary(conn, "N1MAG")

    assert "W9BVM" not in direct


def test_direct_contact_summary_keeps_last_inbound_contact_when_later_outbound_exists() -> None:
    conn = _conn()
    conn.executemany(
        """
        INSERT INTO js8_links (ts, origin, destination, snr, band, freq_hz)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (100.0, "W9BVM", "N1MAG", -8.0, "40M", 7115000.0),
            (200.0, "N1MAG", "W9BVM", -6.0, "20M", 14115000.0),
        ],
    )

    direct = load_js8_direct_contact_summary(conn, "N1MAG")

    assert direct["W9BVM"]["last_contact_ts"] == 100.0
    assert direct["W9BVM"]["last_contact_band"] == "40M"
    assert direct["W9BVM"]["last_contact_snr"] == -8.0


def test_activity_summary_prefers_mode_specific_data_over_legacy_operator_checkins() -> None:
    conn = _conn()
    ensure_js8_callsign_stats(conn, rebuild_if_empty=False)
    conn.execute(
        "INSERT INTO operator_checkins (callsign, last_seen_utc) VALUES (?, ?)",
        ("W1ABC", "20250401"),
    )
    conn.execute(
        "INSERT INTO varac_callsign_stats (callsign, last_seen_ts, last_band, last_freq_hz) VALUES (?, ?, ?, ?)",
        ("W1ABC", 500.0, "80M", 3580000.0),
    )

    summary = load_operator_activity_summary(conn)

    assert summary["W1ABC"]["overall_last_seen_ts"] == 500.0
    assert summary["W1ABC"]["overall_last_band"] == "80M"
