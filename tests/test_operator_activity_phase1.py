from __future__ import annotations

import json
import sqlite3

from freqinout.core.operator_activity import (
    _band_from_freq_hz,
    _freq_hz_from_value,
    ensure_js8_callsign_stats,
    load_js8_direct_contact_summary,
    load_operator_activity_summary,
    parse_utc_timestamp,
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


def test_activity_summary_uses_imported_js8spotter_archive_rows() -> None:
    conn = _conn()
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
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "/legacy/js8spotter.db",
            "signal",
            "sig-1",
            "fp-sig-1",
            json.dumps(
                {
                    "sig_callsign": "W9BVM",
                    "sig_timestamp": "2026-04-09 16:40:42",
                    "sig_freq": "14.115",
                    "sig_snr": "-8",
                }
            ),
            1_710_000_000.0,
        ),
    )

    summary = load_operator_activity_summary(conn)

    assert summary["W9BVM"]["spotter_last_seen_ts"] == 1_775_752_842.0
    assert summary["W9BVM"]["spotter_last_band"] == "20M"
    assert summary["W9BVM"]["overall_last_seen_source"] == "spotter_import"
    assert summary["W9BVM"]["overall_last_band"] == "20M"


def test_imported_spotter_frequency_values_accept_mhz_khz_and_hz() -> None:
    assert _band_from_freq_hz(_freq_hz_from_value("14.115")) == "20M"
    assert _band_from_freq_hz(_freq_hz_from_value("7115")) == "40M"
    assert _band_from_freq_hz(_freq_hz_from_value("14115000")) == "20M"


def test_compact_spotter_timestamps_parse_as_dates_not_epoch_seconds() -> None:
    assert parse_utc_timestamp("20260409164042") == 1_775_752_842.0


def test_imported_spotter_activity_summary_prefers_event_time_before_import_time() -> None:
    conn = _conn()
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
    old_rows = [
        (
            "/legacy/js8spotter.db",
            "signal",
            f"old-{idx}",
            f"fp-old-{idx}",
            json.dumps(
                {
                    "sig_callsign": f"N{idx}OLD",
                    "sig_timestamp": "2020-01-01 00:00:00",
                    "sig_freq": "7.115",
                }
            ),
            9_999_999_000.0 + idx,
        )
        for idx in range(5000)
    ]
    conn.executemany(
        """
        INSERT INTO js8spotter_import_archive
            (source_db, source_table, source_id, source_fingerprint, payload_json, imported_ts)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        old_rows,
    )
    conn.execute(
        """
        INSERT INTO js8spotter_import_archive
            (source_db, source_table, source_id, source_fingerprint, payload_json, imported_ts)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "/legacy/js8spotter.db",
            "signal",
            "new-event-old-import",
            "fp-new-event",
            json.dumps(
                {
                    "sig_callsign": "W9NEW",
                    "sig_timestamp": "2026-04-09 16:40:42",
                    "sig_freq": "14.115",
                }
            ),
            1.0,
        ),
    )

    summary = load_operator_activity_summary(conn)

    assert summary["W9NEW"]["spotter_last_seen_ts"] == 1_775_752_842.0
    assert summary["W9NEW"]["overall_last_band"] == "20M"
