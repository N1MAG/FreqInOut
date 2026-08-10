from __future__ import annotations

from pathlib import Path
import sqlite3

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
