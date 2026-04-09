from __future__ import annotations

import csv
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.gui import qsy_helper
from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.gui.operator_history_tab import OperatorHistoryTab
from freqinout.gui.stations_map_tab import StationsMapTab


class _MemorySettings:
    def __init__(self) -> None:
        self._data: dict[str, object] = {}

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def set(self, key: str, value) -> None:
        self._data[key] = value

    def reload(self) -> None:
        return None


class _FakeScheduler:
    def __init__(self) -> None:
        self.minutes: list[int] = []

    def suspend_schedule(self, minutes: int) -> None:
        self.minutes.append(int(minutes))


def _app():
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication([])


def test_set_active_hold_duration_rewrites_active_hold_cache():
    settings = _MemorySettings()
    scheduler = _FakeScheduler()
    window = SimpleNamespace(scheduler=scheduler)

    mins = qsy_helper.set_active_hold_duration(window, settings, 90, notify=False)

    assert mins == 90
    assert scheduler.minutes == [90]
    snapshot = qsy_helper.suspend_snapshot(settings, allow_reload=False)
    assert snapshot["active"] is True
    assert int(snapshot["remaining_minutes"] or 0) >= 89


def test_scheduler_status_summary_reports_next_transition_frequency(monkeypatch, tmp_path):
    app = _app()
    assert app is not None

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from freqinout.core.scheduler_engine import SchedulerEngine

    engine = SchedulerEngine()
    try:
        monkeypatch.setattr(engine, "_control_mode", lambda: "MANUAL")
        monkeypatch.setattr(engine, "_current_rig_frequency", lambda **_kwargs: 14_115_000)
        monkeypatch.setattr(
            engine,
            "_off_schedule_flags",
            lambda *args, **kwargs: {"frequency": False, "mode": False, "offset": False, "fldigi_offset": False},
        )
        monkeypatch.setattr(engine, "_varac_status", lambda: {"waiting_for_frequency": False, "busy": False})
        monkeypatch.setattr(engine, "_js8_running", lambda: False)
        monkeypatch.setattr(engine, "_fldigi_log_status", lambda: {"busy": False, "reason": None})

        engine.current_source = "HF"
        engine.current_schedule_entry = {"frequency": "14.115", "group_name": "ALPHA"}
        engine._next_source = "HF"
        engine._next_net_kind = "HF Schedule"
        engine._next_transition_freq_hz = 7_115_000

        summary = engine.get_status_summary()
        assert summary["next_frequency_label"] == "7.115"
        assert summary["next_frequency_mhz"] == pytest.approx(7.115)
    finally:
        engine.deleteLater()


def test_operator_history_import_accepts_utf8_bom(monkeypatch, tmp_path):
    _app()

    csv_path = tmp_path / "operators_bom.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=["Callsign", "Name", "State"])
        writer.writeheader()
        writer.writerow({"Callsign": "N0CALL", "Name": "Test Op", "State": "CO"})

    imported_rows: list[dict[str, object]] = []
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.QFileDialog.getOpenFileName",
        lambda *args, **kwargs: (str(csv_path), "CSV Files (*.csv)"),
    )
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.QMessageBox.information",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.QMessageBox.warning",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(OperatorHistoryTab, "_load_data", lambda self, show_toast=False: None)
    monkeypatch.setattr(OperatorHistoryTab, "_schedule_history_update", lambda self: None)
    monkeypatch.setattr(
        OperatorHistoryTab,
        "_upsert_record",
        lambda self, data: imported_rows.append(dict(data)) or True,
    )

    tab = OperatorHistoryTab()
    try:
        tab._import_csv()
    finally:
        tab.deleteLater()

    assert len(imported_rows) == 1
    assert imported_rows[0]["callsign"] == "N0CALL"


def test_message_viewer_treats_k2s_as_transport_form():
    rendered = MessageViewerTab._parse_b2s_form_content(
        ":hdr_fm: N0CALL :hdr_ed: 2026-04-08 12:30Z :prec: R :sub: Checkin :mg: Hello world"
    )

    assert MessageViewerTab._is_transport_form_ext(".b2s") is True
    assert MessageViewerTab._is_transport_form_ext(".k2s") is True
    assert "Routine" in rendered
    assert "Hello world" in rendered


def test_map_marker_group_filter_uses_merged_group_membership():
    dummy = SimpleNamespace(
        operator_rows=[
            {
                "callsign": "N0CALL",
                "state": "CO",
                "group1": "",
                "group2": "",
                "group3": "",
                "groups": ["AMRRON"],
            }
        ]
    )

    StationsMapTab._rebuild_operator_index(dummy)

    assert "AMRRON" in dummy.operator_index["N0CALL"]["groups"]
    assert StationsMapTab._marker_station_matches_filters(
        dummy,
        "N0CALL",
        group_filter="AMRRON",
        region_filter="",
        my_call="",
        allow_self=False,
    )
    assert not StationsMapTab._marker_station_matches_filters(
        dummy,
        "N0CALL",
        group_filter="MARS",
        region_filter="",
        my_call="",
        allow_self=False,
    )


def test_map_html_uses_bottom_docked_inline_legend_rows():
    dummy = SimpleNamespace(
        settings=_MemorySettings(),
        _now_reachable_enabled=True,
        show_grids=False,
        show_grid_labels=False,
        show_regions=False,
        show_states=False,
        show_cities=False,
        _resolve_prop_band_colors=lambda: {"20M": "#43A047", "40M": "#1E88E5"},
    )

    html = StationsMapTab._build_leaflet_html(
        dummy,
        markers=[],
        links=[],
        max_zoom=18,
        leaflet_js="leaflet.js",
        leaflet_css="leaflet.css",
        geojson_urls=[],
        cities_geojson=None,
        city_min_pop=0,
        show_city_labels=False,
        initial_view=None,
        prop_overlay_enabled=True,
        prop_region_scores=None,
        prop_state_scores=None,
    )

    assert 'id="legendDock"' in html
    assert 'id="legendBox"' in html
    assert "const legend = L.control" not in html
    assert "function updateLegend()" in html
    assert "legend-rows" in html
    assert "legend-label" in html
    assert "legend-sep" in html
    assert "Link SNR:" in html
    assert "SitRep Status:" in html
    assert "Peer Sched Now:" in html
    assert "Best Band Now:" in html
    assert 'color:\\"' not in html


def test_map_controls_keep_action_buttons_readable(monkeypatch, tmp_path):
    _app()
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))

    tab = StationsMapTab()
    try:
        assert tab.relay_target_combo.minimumWidth() == 180
        assert tab._refresh_links_button.minimumWidth() >= tab._refresh_links_button.sizeHint().width()
        assert tab._now_reachable_button.minimumWidth() >= tab._now_reachable_button.sizeHint().width()
        assert tab._sitrep_status_button.minimumWidth() >= tab._sitrep_status_button.sizeHint().width()
    finally:
        tab.deleteLater()
