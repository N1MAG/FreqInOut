from __future__ import annotations

from pathlib import Path
import sqlite3
from types import SimpleNamespace

from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.observation_projection import (
    observation_from_local_report,
    observation_from_message_intelligence,
)
from freqinout.core.observation_store import upsert_observation
from freqinout.core.js8_log_link_indexer import JS8LogLinkIndexer
from freqinout.core.settings_manager import SettingsManager
from freqinout.gui.stations_map_tab import StationsMapTab


def _bare_tab() -> StationsMapTab:
    tab = StationsMapTab.__new__(StationsMapTab)
    tab._is_shutting_down = False
    tab._map_visible = True
    tab._map_initialized = False
    tab._map_dirty = False
    tab._map_page_loading = False
    tab._render_requested_during_load = False
    tab._render_pending = False
    tab._last_map_render_ts = 0.0
    tab._map_load_ok = False
    tab._ingest_started = False
    tab._deferred_initial_ingest_pending = False
    tab._pending_map_payload = None
    tab._last_map_payload_sig = None
    tab._now_reachable_enabled = False
    tab._observation_focus_enabled = False
    tab._query_cache_ttl_sec = 3.0
    tab._query_cache = {}
    tab._map_stack = None
    tab._map_loading_label = None
    tab.settings = None
    tab.web = object()
    return tab


def test_schedule_render_during_page_load_defers_until_load_finishes() -> None:
    tab = _bare_tab()
    tab._map_initialized = True
    tab._map_page_loading = True

    tab._schedule_render()

    assert tab._map_dirty is True
    assert tab._render_requested_during_load is True


def test_maybe_start_map_ingest_waits_for_successful_first_load() -> None:
    tab = _bare_tab()
    tab._deferred_initial_ingest_pending = True
    started: list[str] = []
    tab._start_map_ingest_lifecycle = lambda: started.append("started")

    assert tab._maybe_start_map_ingest() is False
    assert started == []

    tab._map_load_ok = True

    assert tab._maybe_start_map_ingest() is True
    assert started == ["started"]
    assert tab._deferred_initial_ingest_pending is False


def test_map_load_finished_flushes_deferred_render_request() -> None:
    tab = _bare_tab()
    tab._map_page_loading = True
    tab._render_requested_during_load = True
    tab._map_visible = True
    scheduled: list[str] = []
    tab._schedule_render = lambda: scheduled.append("render")

    tab._on_map_load_finished(True)

    assert tab._map_page_loading is False
    assert tab._map_initialized is True
    assert tab._map_load_ok is True
    assert tab._render_requested_during_load is False
    assert scheduled == ["render"]


def test_push_map_payload_queues_while_page_loading() -> None:
    tab = _bare_tab()
    tab._map_page_loading = True
    tab._map_initialized = False

    tab._push_map_payload([{"callsign": "N0CALL"}], [{"origin": "A", "destination": "B"}])

    assert tab._pending_map_payload is not None
    assert tab._pending_map_payload["markers"][0]["callsign"] == "N0CALL"
    assert tab._pending_map_payload["links"][0]["origin"] == "A"


def test_js8_log_indexer_repeated_scan_does_not_duplicate_map_or_activity_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    directed = tmp_path / "DIRECTED.TXT"
    directed.write_text(
        "2026-08-08 12:34:56\t7.115000\t0\t-10\tN0CALL: @MAGNET SNR -10\n",
        encoding="utf-8",
    )
    settings = SettingsManager()
    settings.set("js8_directed_path", str(directed))
    db_path = cfg_root / "config" / "freqinout_nets.db"
    indexer = JS8LogLinkIndexer(settings, db_path)

    assert indexer.update(since_ts=0) == 1
    settings.set("js8_links_directed_offset", 0)
    settings.set("js8_links_all_offset", 0)
    assert indexer.update(since_ts=0) == 1

    conn = sqlite3.connect(db_path)
    try:
        link_count = conn.execute("SELECT COUNT(*) FROM js8_links").fetchone()[0]
        stats = conn.execute(
            "SELECT callsign, last_band, last_freq_hz FROM js8_callsign_stats ORDER BY callsign"
        ).fetchall()
    finally:
        conn.close()

    assert link_count == 1
    assert stats == [
        ("@MAGNET", "40M", 7115000.0),
        ("N0CALL", "40M", 7115000.0),
    ]


def test_map_observation_loader_uses_read_only_eligibility(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: cfg_root)
    tab = _bare_tab()
    tab._observation_focus_enabled = True

    spotter = observation_from_message_intelligence(
        analyze_spotter_text("F!307 TO[@MR08] FR[K7ETC] ST[UT] GR[DM38ST] NA[Wildfire status] #D2NT"),
        source_ref="spotter_traffic:1",
        source_family="spotter",
        event_utc="2026-08-10T14:00:00+00:00",
    )
    confirmed_local = observation_from_local_report(
        {
            "id": 1,
            "created_utc": "2026-08-10T15:00:00+00:00",
            "callsign": "K0PRA",
            "state": "CO",
            "grid": "DM79",
            "topics": ("Comms",),
            "subject": "Repeater degraded",
            "confirmed_state": "CONFIRMED",
        }
    )
    unconfirmed_local = observation_from_local_report(
        {
            "id": 2,
            "created_utc": "2026-08-10T15:05:00+00:00",
            "callsign": "N0PWR",
            "state": "CO",
            "grid": "DM79",
            "topics": ("Power",),
            "subject": "Generator needed",
            "confirmed_state": "UNCONFIRMED",
        }
    )
    for obs in (spotter, confirmed_local, unconfirmed_local):
        upsert_observation(db_path, obs)

    alert_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="alert",
        max_age_sec=0,
    )
    infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )

    assert [row["callsign"] for row in alert_rows] == ["K7ETC"]
    assert [row["callsign"] for row in infrastructure_rows] == ["K0PRA"]
    assert all(row["callsign"] != "N0PWR" for row in alert_rows + infrastructure_rows)


def test_map_operational_events_can_place_grid_only_observations() -> None:
    tab = _bare_tab()
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="alert",
        display_label="Observation Alerts",
        reports_loader=lambda: [
            {
                "callsign": "K7ETC",
                "grid": "DM38ST",
                "summary": "Wildfire status",
                "icon": "warning",
                "severity": "severe",
                "utc_ts": 1786363200.0,
            }
        ],
    )

    assert len(events) == 1
    assert events[0]["lat"] != 0
    assert events[0]["lon"] != 0
    assert "Observation Alerts: 1" in events[0]["tooltip"]
