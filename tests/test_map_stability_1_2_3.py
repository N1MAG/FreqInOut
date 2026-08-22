from __future__ import annotations

from pathlib import Path
import sqlite3
from types import SimpleNamespace

from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.observation_projection import (
    Observation,
    observation_from_local_report,
    observation_from_message_intelligence,
    observation_from_rf_pin,
)
from freqinout.core.observation_store import upsert_observation
from freqinout.core.js8_log_link_indexer import JS8LogLinkIndexer
from freqinout.core.settings_manager import SettingsManager
from freqinout.gui.stations_map_tab import StationPoint, StationsMapTab


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
    tab._last_js8_load_ts = 0.0
    tab._last_exit_ts = 0.0
    tab._js8_rx_hub = None
    tab._now_reachable_enabled = False
    tab._observation_focus_enabled = False
    tab._observation_focus_mode = ""
    tab._query_cache_ttl_sec = 3.0
    tab._query_cache = {}
    tab._map_stack = None
    tab._map_loading_label = None
    tab.settings = None
    tab.web = object()
    return tab


def test_map_auto_ingest_uses_background_controller_before_local_ingest() -> None:
    tab = _bare_tab()
    tab._app_active = True
    calls: list[str] = []
    tab._request_background_ingest = lambda *kinds: calls.append(f"background:{','.join(kinds)}") or True
    tab._ingest_js8_logs = lambda **_kwargs: calls.append("local-js8") or 0
    tab._schedule_render = lambda: calls.append("render")
    tab._emit_map_event = lambda *_args, **_kwargs: None

    StationsMapTab._auto_ingest_and_refresh(tab, initial=False)

    assert calls == ["background:js8_links,varac", "render"]


def test_map_auto_ingest_falls_back_to_local_ingest_without_background_controller() -> None:
    tab = _bare_tab()
    tab._app_active = True
    calls: list[str] = []
    tab._request_background_ingest = lambda *kinds: False
    tab._ingest_js8_logs = lambda **_kwargs: calls.append("local-js8") or 0
    tab._schedule_render = lambda: calls.append("render")
    tab._emit_map_event = lambda *_args, **_kwargs: None

    StationsMapTab._auto_ingest_and_refresh(tab, initial=False)

    assert calls == ["local-js8", "render"]


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


def test_map_view_status_text_names_current_review_context() -> None:
    tab = _bare_tab()

    assert StationsMapTab._map_view_status_text(tab) == "Map View: All Stations"

    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "hf_reports"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: HF Reports"

    tab._observation_focus_mode = "local_reports"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Local Reports"

    tab._observation_focus_mode = "all_reports"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Reports"

    tab._observation_focus_enabled = False
    tab._observation_focus_mode = ""
    tab._sitrep_status_only_enabled = True
    assert StationsMapTab._map_view_status_text(tab) == "Map View: SitRep Status"

    tab._now_reachable_enabled = True
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Peer Schedule Now"


def test_local_reports_focus_suppresses_legacy_hf_spotter_report_layers() -> None:
    tab = _bare_tab()

    assert StationsMapTab._include_legacy_spotter_report_layers(tab) is True

    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "local_reports"
    assert StationsMapTab._include_legacy_spotter_report_layers(tab) is False

    tab._observation_focus_mode = "hf_reports"
    assert StationsMapTab._include_legacy_spotter_report_layers(tab) is True


def test_map_control_strip_uses_operator_first_sections() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("def _build_ui") : source.index("def _add_collapsible_group")]

    assert 'QLabel("View Mode")' in build_block
    assert 'QLabel("Intelligence")' in build_block
    assert 'QPushButton("All Stations")' in build_block
    assert 'QPushButton("HF Reports")' in build_block
    assert 'QPushButton("Local Reports")' in build_block
    assert 'QPushButton("Reports")' in build_block
    assert 'QLabel("RF Pins")' in build_block
    assert 'QPushButton("Add RF Pin")' in build_block
    assert 'QCheckBox("Alerts/Intel")' in build_block
    assert 'QCheckBox("Infrastructure/Utilities")' in build_block
    assert 'QPushButton("Show Filters & Layers")' in build_block


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
    tab._observation_focus_mode = "all_reports"

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
    condition_alert = Observation(
        observation_id="condition_alert:magcon:k7etc:yellow",
        source_family="condition_alert",
        source_ref="js8:1",
        source_app="JS8Call",
        received_utc="2026-08-10T16:00:00+00:00",
        event_utc="2026-08-10T16:00:00+00:00",
        from_call="N1MAG",
        to_target="MAGNET",
        groups=("MAGNET",),
        observed_topics=("General Intel", "Comms"),
        operator_attention=True,
        status="CONDITION ALERT",
        urgency="LEVEL YELLOW",
        subject="MAGCON: Level YELLOW",
        summary="MAGNET condition level YELLOW",
        grid="DM79",
        location_confidence="grid",
    )
    rf_pin = observation_from_rf_pin(
        {
            "pin_id": "manual:relay-check",
            "label": "Relay check",
            "callsign": "N1MAG",
            "target": "MAGNET",
            "topics": ("Comms",),
            "grid": "DM79",
            "status": "PIN",
            "created_utc": "2026-08-10T16:30:00+00:00",
        }
    )
    for obs in (spotter, confirmed_local, unconfirmed_local, condition_alert, rf_pin):
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

    assert [row["callsign"] for row in alert_rows] == ["N1MAG", "K7ETC"]
    assert alert_rows[0]["source_family"] == "condition_alert"
    assert alert_rows[0]["source_label"] == "Condition Alert"
    assert alert_rows[0]["icon"] == "warning"
    assert [row["callsign"] for row in infrastructure_rows] == ["N1MAG", "K0PRA"]
    assert infrastructure_rows[0]["source_family"] == "rf_pin"
    assert infrastructure_rows[0]["source_label"] == "RF Pin"
    assert all(row["callsign"] != "N0PWR" for row in alert_rows + infrastructure_rows)

    tab._query_cache = {}
    tab._observation_focus_mode = "hf_reports"
    hf_alert_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="alert",
        max_age_sec=0,
    )
    hf_infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )

    assert [row["callsign"] for row in hf_alert_rows] == ["N1MAG", "K7ETC"]
    assert hf_infrastructure_rows == []

    tab._query_cache = {}
    tab._observation_focus_mode = "local_reports"
    local_alert_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="alert",
        max_age_sec=0,
    )
    local_infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )

    assert local_alert_rows == []
    assert [row["callsign"] for row in local_infrastructure_rows] == ["K0PRA"]
    assert all(row["callsign"] != "N0PWR" for row in local_alert_rows + local_infrastructure_rows)


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


def test_map_operational_events_can_place_condition_alerts_from_station_lookup() -> None:
    tab = _bare_tab()
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {
            "N1MAG": StationPoint(
                callsign="N1MAG",
                grid="DM79",
                lat=39.7,
                lon=-104.9,
            )
        },
        layer_name="alert",
        display_label="Observation Alerts",
        reports_loader=lambda: [
            {
                "callsign": "N1MAG",
                "to_target": "MAGNET",
                "summary": "MAGNET condition level YELLOW",
                "icon": "warning",
                "severity": "caution",
                "utc_ts": 1786363200.0,
                "source_family": "condition_alert",
                "source_label": "Condition Alert",
                "topics": ["General Intel", "Comms"],
                "location_confidence": "sender lookup",
            }
        ],
    )

    assert len(events) == 1
    assert events[0]["lat"] == 39.7
    assert "Condition Alert | N1MAG -&gt; MAGNET" in events[0]["tooltip"]
    assert "Topics: General Intel, Comms" in events[0]["tooltip"]


def test_map_report_popup_text_distinguishes_hf_and_local_contexts(monkeypatch) -> None:
    tab = _bare_tab()
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()
    now = 1786365000.0
    monkeypatch.setattr("freqinout.gui.stations_map_tab.time.time", lambda: now)

    assert StationsMapTab._map_report_age_text(now - 17 * 60, now=now) == "17 min ago"
    assert StationsMapTab._map_report_age_text(now - 102 * 60, now=now) == "1:42 h ago"
    assert StationsMapTab._map_report_age_text(now - 16 * 24 * 60 * 60, now=now) == "16 days ago"

    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="alert",
        display_label="Observation Alerts",
        reports_loader=lambda: [
            {
                "callsign": "K7ETC",
                "to_target": "MR08",
                "grid": "DM38ST",
                "state": "UT",
                "summary": "Wildfire status",
                "icon": "warning",
                "severity": "severe",
                "utc_ts": now - 17 * 60,
                "source_family": "spotter",
                "source_label": "HF JS8Spotter",
                "form_id": "F!307",
                "topics": ["Fire", "Travel/Roads"],
                "auth_state": "valid",
                "trusted_state": "trusted",
                "location_confidence": "grid",
            },
            {
                "callsign": "K0PRA",
                "to_target": "County GMRS",
                "grid": "DM38ST",
                "state": "CO",
                "summary": "Repeater degraded",
                "icon": "comms",
                "severity": "watch",
                "utc_ts": now - 102 * 60,
                "source_family": "local_report",
                "source_label": "Local Report",
                "topics": ["Comms"],
                "confirmed_state": "CONFIRMED",
                "location_confidence": "grid",
            },
        ],
    )

    tooltip = events[0]["tooltip"]
    assert events[0]["source_kind"] == "mixed"
    assert "Source Type: HF JS8Spotter 1, Local Report 1" in tooltip
    assert "HF JS8Spotter | F!307 | K7ETC -&gt; MR08 | 17 min ago" in tooltip
    assert "Topics: Fire, Travel/Roads" in tooltip
    assert "Auth: Valid, Trusted" in tooltip
    assert "Local Report | K0PRA -&gt; County GMRS | 1:42 h ago" in tooltip
    assert "Local: Confirmed" in tooltip
    assert "Area: CO / DM38ST (Grid)" in tooltip


def test_map_html_legend_and_operational_markers_distinguish_report_sources() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "Report Source:" in source
    assert "op-source-hf" in source
    assert "op-source-local" in source
    assert "op-source-pin" in source
    assert "op-source-mixed" in source
    assert "event.source_kind" in source
