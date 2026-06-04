from __future__ import annotations

import datetime
import sqlite3
from pathlib import Path

from freqinout.core.checkins_db import lookup_operator_identity
from freqinout.core.fldigi_log_checkin_parser import (
    parse_fldigi_log_payload,
    scan_fldigi_log_file,
)
from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab


FIXTURES = Path(__file__).parent / "fixtures"


class FakeSettings:
    def __init__(self, data=None):
        self._data = dict(data or {})

    def all(self):
        return self._data

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        self._data[key] = value



def test_rx_only_log_parses_high_confidence_tfc_and_qru(tmp_path, monkeypatch):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    candidate = parse_fldigi_log_payload("K1ABC / Bill / CO / 1RR")
    assert candidate is not None
    assert candidate.bucket == "TFC"
    assert candidate.confidence == "high"
    assert candidate.callsign == "K1ABC"
    assert candidate.traffic == "1RR"

    qru = parse_fldigi_log_payload("K2DEF / Mary / nm / no traffic")
    assert qru is not None
    assert qru.bucket == "QRU"
    assert qru.traffic == "QRU"



def test_mixed_rx_tx_log_ignores_tx_lines_for_inbound_candidates(tmp_path, monkeypatch):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    log_path = FIXTURES / "fldigi_log_mixed_sample.log"
    candidates, new_offset, last_tx_context = scan_fldigi_log_file(
        log_path,
        start_offset=0,
        session_start_utc=datetime.datetime(2026, 4, 19, 12, 0, tzinfo=datetime.timezone.utc),
    )

    assert new_offset == log_path.stat().st_size
    assert [candidate.callsign for candidate in candidates] == ["K5MIX", "K6MISS", "K7QRU"]
    assert "K9OWN" not in [candidate.callsign for candidate in candidates]
    assert all(candidate.rx for candidate in candidates)
    assert isinstance(last_tx_context, str)



def test_standardized_traffic_and_enrichment_from_operator_checkins(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))

    from freqinout.core.settings_manager import SettingsManager

    settings = SettingsManager()
    conn = sqlite3.connect(settings.db_path.parent / "freqinout_nets.db")
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operator_checkins (
                callsign TEXT PRIMARY KEY,
                name TEXT,
                state TEXT,
                grid TEXT,
                group1 TEXT,
                group2 TEXT,
                group3 TEXT,
                group_role TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                last_net TEXT,
                last_role TEXT,
                checkin_count INTEGER DEFAULT 0,
                groups_json TEXT,
                trusted INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO operator_checkins (callsign, name, state) VALUES (?, ?, ?)",
            ("K3GHI", "Gwen", "NM"),
        )
        conn.commit()
    finally:
        conn.close()

    candidate = parse_fldigi_log_payload("K3GHI / 1PP", lookup_identity=lookup_operator_identity)
    assert candidate is not None
    assert candidate.bucket == "TFC"
    assert candidate.name == "Gwen"
    assert candidate.state == "NM"
    assert candidate.traffic == "1PP"



def test_partial_decodes_stay_in_review_without_local_history_fallback(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    candidate = parse_fldigi_log_payload("K6MISS / 1PP", lookup_identity=lambda callsign: {})
    assert candidate is not None
    assert candidate.bucket == "REVIEW"
    assert candidate.confidence == "low"



def test_session_bounded_scan_ignores_backlog_before_start(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    log_path = tmp_path / "fldigi20260419.log"
    log_path.write_text(
        "RX 1 : FLDIGI (2026-04-19 11:59Z): K0OLD / Old / TX / 1RR\n"
        "RX 2 : FLDIGI (2026-04-19 12:01Z): K1NEW / New / CO / 1RR\n",
        encoding="utf-8",
    )
    candidates, _, _ = scan_fldigi_log_file(
        log_path,
        start_offset=0,
        session_start_utc=datetime.datetime(2026, 4, 19, 12, 0, tzinfo=datetime.timezone.utc),
    )
    assert [candidate.callsign for candidate in candidates] == ["K1NEW"]



def test_normalized_dedupe_suppresses_repeated_identical_decodes(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    log_path = tmp_path / "fldigi20260419.log"
    log_path.write_text(
        "RX 1 : FLDIGI (2026-04-19 12:01Z): K1ABC / Bill / CO / 1RR\n"
        "RX 2 : FLDIGI (2026-04-19 12:02Z): K1ABC / Bill / CO / 1RR\n",
        encoding="utf-8",
    )
    candidates, _, _ = scan_fldigi_log_file(
        log_path,
        start_offset=0,
        session_start_utc=datetime.datetime(2026, 4, 19, 12, 0, tzinfo=datetime.timezone.utc),
        seen_normalized=set(),
    )
    assert len(candidates) == 1



def test_tx_context_attaches_to_rx_in_same_scan(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    log_path = tmp_path / "fldigi20260419.log"
    log_path.write_text(
        "TX 1 : FLDIGI (2026-04-19 12:00Z): RELAYS DE W1XYZ:\n"
        "RX 2 : FLDIGI (2026-04-19 12:01Z): K1ABC / Bill / CO / 1RR\n",
        encoding="utf-8",
    )

    candidates, _, last_tx_context = scan_fldigi_log_file(
        log_path,
        start_offset=0,
        session_start_utc=datetime.datetime(2026, 4, 19, 12, 0, tzinfo=datetime.timezone.utc),
        include_tx_context=True,
    )

    assert len(candidates) == 1
    assert candidates[0].tx_context == "RELAYS DE W1XYZ:"
    assert last_tx_context == "RELAYS DE W1XYZ:"



def test_tx_context_survives_incremental_polling_across_scan_boundaries(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    log_path = tmp_path / "fldigi20260419.log"
    log_path.write_text(
        "TX 1 : FLDIGI (2026-04-19 12:00Z): NCS REQUEST FOR TRAFFIC STATUS\n",
        encoding="utf-8",
    )

    first_candidates, first_offset, last_tx_context = scan_fldigi_log_file(
        log_path,
        start_offset=0,
        session_start_utc=datetime.datetime(2026, 4, 19, 12, 0, tzinfo=datetime.timezone.utc),
        include_tx_context=True,
    )
    assert first_candidates == []
    assert first_offset > 0
    assert last_tx_context == "NCS REQUEST FOR TRAFFIC STATUS"

    log_path.write_text(
        "TX 1 : FLDIGI (2026-04-19 12:00Z): NCS REQUEST FOR TRAFFIC STATUS\n"
        "RX 2 : FLDIGI (2026-04-19 12:01Z): K2BBB / Bob / AZ / 1RR\n",
        encoding="utf-8",
    )

    second_candidates, second_offset, second_tx_context = scan_fldigi_log_file(
        log_path,
        start_offset=first_offset,
        session_start_utc=datetime.datetime(2026, 4, 19, 12, 0, tzinfo=datetime.timezone.utc),
        last_tx_context=last_tx_context,
        include_tx_context=True,
    )

    assert second_offset >= first_offset
    assert len(second_candidates) == 1
    assert second_candidates[0].tx_context == "NCS REQUEST FOR TRAFFIC STATUS"
    assert second_tx_context == "NCS REQUEST FOR TRAFFIC STATUS"



def test_tx_only_content_produces_zero_candidates(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    log_path = tmp_path / "fldigi20260419.log"
    log_path.write_text(
        "TX 1 : FLDIGI (2026-04-19 12:00Z): WORD OF THE WEEK\n",
        encoding="utf-8",
    )

    candidates, _, last_tx_context = scan_fldigi_log_file(
        log_path,
        start_offset=0,
        session_start_utc=datetime.datetime(2026, 4, 19, 12, 0, tzinfo=datetime.timezone.utc),
        include_tx_context=True,
    )

    assert candidates == []
    assert last_tx_context == "WORD OF THE WEEK"



def test_tx_disabled_leaves_context_empty_and_rx_only_behavior_unchanged(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    log_path = tmp_path / "fldigi20260419.log"
    log_path.write_text(
        "TX 1 : FLDIGI (2026-04-19 12:00Z): RELAYS DE W1XYZ:\n"
        "RX 2 : FLDIGI (2026-04-19 12:01Z): K3CCC / Carol / NM / 1PP\n",
        encoding="utf-8",
    )

    candidates, _, last_tx_context = scan_fldigi_log_file(
        log_path,
        start_offset=0,
        session_start_utc=datetime.datetime(2026, 4, 19, 12, 0, tzinfo=datetime.timezone.utc),
        include_tx_context=False,
    )

    assert len(candidates) == 1
    assert candidates[0].tx_context == ""
    assert last_tx_context == ""



def test_inline_review_context_stays_parse_safe():
    tab = FldigiNetControlTab.__new__(FldigiNetControlTab)
    candidate = type(
        "Candidate",
        (),
        {
            "callsign": "K1ABC",
            "name": "Bill",
            "state": "CO",
            "traffic": "1RR",
            "bucket": "REVIEW",
            "tx_context": "RELAYS DE W1XYZ:",
        },
    )()

    review_line = FldigiNetControlTab._log_assisted_candidate_line(tab, candidate)
    parsed_callsign, parsed_name, parsed_state = FldigiNetControlTab._parse_checkin_line(tab, review_line)

    assert review_line.endswith("[ctx: RELAYS DE W1XYZ:]")
    assert (parsed_callsign, parsed_name, parsed_state) == ("K1ABC", "Bill", "CO")



def test_mid_session_log_path_rollover_clears_context_and_state(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    log_path_a = tmp_path / "fldigi20260419_a.log"
    log_path_b = tmp_path / "fldigi20260419_b.log"
    log_path_a.write_text(
        "TX 1 : FLDIGI (2026-04-19 12:00Z): RELAYS DE W1XYZ:\n"
        "RX 2 : FLDIGI (2026-04-19 12:01Z): K1ABC / Bill / CO / 1RR\n",
        encoding="utf-8",
    )
    log_path_b.write_text(
        "RX 1 : FLDIGI (2026-04-19 12:02Z): K2BBB / Bob / AZ / 1PP\n",
        encoding="utf-8",
    )

    tab = FldigiNetControlTab()
    tab.show()
    app.processEvents()
    try:
        tab.log_assisted_enable_chk.setChecked(True)
        tab.settings.set("fldigi_log_path", str(log_path_a))
        tab._capture_log_assisted_session()

        assert tab._log_assisted_session_tx_context == ""
        assert tab._log_assisted_candidates_by_callsign == {}

        tab.settings.set("fldigi_log_path", str(log_path_b))
        tab._poll_log_assisted_intake()

        assert tab._log_assisted_session_path == str(log_path_b)
        assert tab._log_assisted_session_offset == log_path_b.stat().st_size
        assert tab._log_assisted_session_tx_context == ""
        assert tab.review_card.text() == ""
        assert tab._log_assisted_candidates_by_callsign == {}
    finally:
        tab.deleteLater()


def test_mid_session_log_path_rollover_clears_auto_ingested_buckets(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    log_path_a = tmp_path / "fldigi20260419_a.log"
    log_path_b = tmp_path / "fldigi20260419_b.log"
    log_path_a.write_text("", encoding="utf-8")

    tab = FldigiNetControlTab()
    tab.show()
    app.processEvents()

    try:
        tab.log_assisted_enable_chk.setChecked(True)
        tab.settings.set("fldigi_log_path", str(log_path_a))
        tab._capture_log_assisted_session()
        tab._net_in_progress = True

        tfc_candidate = type(
            "Candidate",
            (),
            {
                "callsign": "K1ABC",
                "name": "Bill",
                "state": "CO",
                "traffic": "1RR",
                "bucket": "TFC",
                "confidence": "high",
                "timestamp_utc": datetime.datetime(2026, 4, 19, 12, 1, tzinfo=datetime.timezone.utc),
                "tx_context": "RELAYS DE W1XYZ:",
                "completeness_score": lambda self: 3,
            },
        )()
        qru_candidate = type(
            "Candidate",
            (),
            {
                "callsign": "K2DEF",
                "name": "Mary",
                "state": "NM",
                "traffic": "QRU",
                "bucket": "QRU",
                "confidence": "high",
                "timestamp_utc": datetime.datetime(2026, 4, 19, 12, 2, tzinfo=datetime.timezone.utc),
                "tx_context": "RELAYS DE W1XYZ:",
                "completeness_score": lambda self: 3,
            },
        )()

        tab._apply_log_assisted_candidate(tfc_candidate)
        tab._apply_log_assisted_candidate(qru_candidate)

        assert "K1ABC" in tab.tfc_card.text()
        assert "K2DEF" in tab.qru_card.text()

        log_path_b.write_text("", encoding="utf-8")
        tab.settings.set("fldigi_log_path", str(log_path_b))
        tab._poll_log_assisted_intake()

        assert "K1ABC" not in tab.tfc_card.text()
        assert "K2DEF" not in tab.qru_card.text()
        assert tab._log_assisted_candidates_by_callsign == {}
        assert tab.review_card.text() == ""
    finally:
        tab.deleteLater()
