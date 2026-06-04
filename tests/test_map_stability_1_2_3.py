from __future__ import annotations

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
