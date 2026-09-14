"""Focused regressions for the native Map's operator-visible layer contracts.

These tests stay at the bounded projection/renderer boundary: no station
database, network tile, device, or visible-window work is involved.  They
describe the data that must reach an already-created native Map scene.
"""

from __future__ import annotations

import os
import time
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.gui.native_map_projection import build_native_overlay_projection
from freqinout.gui.native_map_renderer import NativeMapRenderer
from freqinout.gui.stations_map_tab import StationsMapTab
from freqinout.gui.theme import get_theme


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _fema_region_ids(projection: dict[str, object]) -> set[str]:
    polygons = projection.get("polygons", [])
    return {
        str(item.get("region") or "")
        for item in polygons
        if isinstance(item, dict) and str(item.get("kind") or "") == "fema_region"
    }


def test_regions_layer_projects_fema_regions_without_enabling_states() -> None:
    """Regions is its own FEMA overlay, not a restyled States overlay."""
    region_only = build_native_overlay_projection(
        {"show_regions": True, "show_states": False}
    )
    states_only = build_native_overlay_projection(
        {"show_regions": False, "show_states": True}
    )

    assert _fema_region_ids(region_only) == {
        f"R{number:02d}" for number in range(1, 11)
    }
    colorado = next(
        item for item in region_only["polygons"]
        if isinstance(item, dict) and item.get("area_code") == "CO"
    )
    assert colorado["payload"]["fema_region"] == "R08"
    assert all(
        str(item.get("kind") or "") == "state"
        for item in states_only["polygons"]
        if isinstance(item, dict)
    )

    _app()
    renderer = NativeMapRenderer()
    try:
        renderer.apply_projection(region_only)
        assert _fema_region_ids({"polygons": renderer.bridge.polygons}) == {
            f"R{number:02d}" for number in range(1, 11)
        }
    finally:
        renderer.shutdown()


def test_legend_is_empty_or_describes_only_the_layers_in_the_snapshot() -> None:
    """Legend entries track the active snapshot instead of a fixed palette."""
    empty = build_native_overlay_projection({})
    regions = build_native_overlay_projection(
        {"show_regions": True, "show_states": False}
    )
    stations = build_native_overlay_projection(
        {
            "markers": [{"id": "station", "lat": 39.7, "lon": -104.9}],
        }
    )

    assert [item.get("label") for item in empty["legend"]] == ["State boundaries"]

    region_labels = {
        str(item.get("label") or "").lower()
        for item in regions["legend"]
        if isinstance(item, dict)
    }
    assert any("region" in label for label in region_labels)
    assert not ({"station", "caution", "critical"} & region_labels)

    station_labels = {
        str(item.get("label") or "").lower()
        for item in stations["legend"]
        if isinstance(item, dict)
    }
    assert station_labels == {
        "state boundaries",
        "stations",
    }


def test_city_labels_are_priority_decluttered_at_low_zoom_and_return_when_separated() -> None:
    """Nearby cities cannot overlap; more zoom restores labels deterministically."""
    _app()
    renderer = NativeMapRenderer()
    cities = [
        {
            "id": "denver",
            "label": "Denver",
            "lat": 39.7392,
            "lon": -104.9903,
            "population": 715_522,
        },
        {
            "id": "aurora",
            "label": "Aurora",
            "lat": 39.7294,
            "lon": -104.8319,
            "population": 386_261,
        },
    ]
    try:
        started = time.perf_counter()
        renderer.apply_projection(
            {
                "view": {"lat": 39.73, "lon": -104.91, "zoom": 5.0},
                "city_labels": cities * 250,
                "show_cities": True,
                "show_city_labels": True,
            }
        )
        # The bridge deliberately retains the bounded source list.  QML does
        # the viewport-dependent O(n) declutter, so it can restore Aurora as
        # the operator zooms without requesting another projection.
        low_zoom_labels = [item["text"] for item in renderer.bridge.cityLabels]
        assert time.perf_counter() - started < 0.75
        assert low_zoom_labels[0] == "Denver"
        assert set(low_zoom_labels) == {"Denver", "Aurora"}
        assert len(low_zoom_labels) == 400
        assert 'function updateVisibleCityLabels()' in (
            __import__("pathlib").Path("freqinout/gui/qml/native_map_renderer.qml").read_text(encoding="utf-8")
        )

        renderer.apply_projection(
            {
                "view": {"lat": 39.73, "lon": -104.91, "zoom": 10.0},
                "city_labels": cities,
                "show_cities": True,
                "show_city_labels": True,
            }
        )
        assert [item["text"] for item in renderer.bridge.cityLabels] == [
            "Denver",
            "Aurora",
        ]
    finally:
        renderer.shutdown()


def test_paths_use_the_original_five_snr_bins_and_only_add_that_key_when_paths_exist() -> None:
    """Map link colour meaning stays compatible with the prior Leaflet map."""
    _app()
    renderer = NativeMapRenderer()
    try:
        renderer.apply_projection(
            {
                "links": [
                    {"id": "strong", "lat1": 39.0, "lon1": -105.0, "lat2": 40.0, "lon2": -104.0, "snr": 5},
                    {"id": "good", "lat1": 39.0, "lon1": -104.0, "lat2": 40.0, "lon2": -103.0, "snr": 0},
                    {"id": "fair", "lat1": 39.0, "lon1": -103.0, "lat2": 40.0, "lon2": -102.0, "snr": -5},
                    {"id": "weak", "lat1": 39.0, "lon1": -102.0, "lat2": 40.0, "lon2": -101.0, "snr": -10},
                    {"id": "poor", "lat1": 39.0, "lon1": -101.0, "lat2": 40.0, "lon2": -100.0, "snr": -11},
                ],
            }
        )
        assert [item["color"] for item in renderer.bridge.paths] == [
            "#1b5e20", "#2e7d32", "#fbc02d", "#f57c00", "#c62828",
        ]
        projected = build_native_overlay_projection(
            {"links": [{"lat1": 1, "lon1": 1, "lat2": 2, "lon2": 2, "snr": 0}]}
        )
        assert [item.get("color") for item in projected["legend"] if item.get("color")] == [
            "#1b5e20", "#2e7d32", "#fbc02d", "#f57c00", "#c62828",
        ]
        assert [item.get("label") for item in build_native_overlay_projection({})["legend"]] == [
            "State boundaries"
        ]
    finally:
        renderer.shutdown()


def test_regional_and_sitrep_semantics_remain_visible_and_distinct() -> None:
    """Gray/blue/yellow/orange/red intelligence and SitRep remain meaningful."""
    projection = build_native_overlay_projection(
        {
            "show_states": True,
            "regional_intelligence": {
                "enabled": True,
                "states": {
                    "CO": {"level": "blue"},
                    "UT": {"level": "orange"},
                    "WY": {"level": "gray"},
                },
            },
            "sitrep_summary_group": "MR08",
            "sitrep_state_summary": [
                {"state_code": "CO", "red_count": 1, "yellow_count": 2, "green_count": 3, "unknown_count": 4},
            ],
        }
    )
    by_state = {
        item["area_code"]: item
        for item in projection["polygons"]
        if isinstance(item, dict)
    }
    assert by_state["CO"]["color"] == "#1e88e5"
    assert by_state["UT"]["color"] == "#ef6c00"
    assert by_state["WY"]["color"] == "#78909c"
    assert projection["summary"] == "SitRep MR08: 1 state; 1 red, 2 yellow, 3 green, 4 unknown."
    labels = {str(item.get("label")): str(item.get("color")) for item in projection["legend"]}
    assert labels["SitRep Red"] == "#d32f2f"
    assert labels["SitRep Unknown"] == "#4fc3f7"


def test_regions_remain_visible_when_propagation_is_enabled() -> None:
    """Propagation annotates FEMA labels instead of replacing region colors."""
    projection = build_native_overlay_projection(
        {
            "show_regions": True,
            "show_states": False,
            "prop_overlay_enabled": True,
            "prop_state_scores": {"CO": {"bands": {"40m": 88}}},
            "prop_region_scores": {"R08": {"bands": {"40m": 82}}},
            "prop_band_colors": {"40m": "#009e73"},
        }
    )

    colorado = next(
        item for item in projection["polygons"]
        if isinstance(item, dict) and item.get("area_code") == "CO"
    )
    region_eight = next(
        item for item in projection["region_labels"]
        if isinstance(item, dict) and item.get("text") == "R08"
    )
    assert colorado["kind"] == "fema_region"
    assert colorado["color"] == "#5e35b1"
    assert region_eight["band"] == "40m"
    assert "propagation" in str(region_eight["tooltip"]).lower()


class _ThemeSettings:
    def __init__(self, name: str) -> None:
        self.name = name

    def get(self, key: str, default: object = None) -> object:
        return self.name if key == "ui_theme" else default


class _ThemeRenderer:
    def __init__(self) -> None:
        self.themes: list[dict[str, str]] = []

    def apply_theme(self, theme: dict[str, str]) -> None:
        self.themes.append(dict(theme))


def _theme_refresh_harness(settings: _ThemeSettings, renderer: _ThemeRenderer) -> SimpleNamespace:
    """A minimal live Map tab with a populated selection panel."""
    harness = SimpleNamespace(
        settings=settings,
        _native_map_renderer=renderer,
        _controls_button=None,
        _help_button=None,
        _paths_help_button=None,
        _refresh_links_button=None,
        _map_add_rf_pin_button=None,
        _map_manage_rf_pins_button=None,
        _now_reachable_enabled=False,
        prop_overlay_enabled=False,
        _map_selected_panel=object(),
        _map_selected_payload={"id": "station-1", "title": "Selection"},
        selection_renders=[],
    )
    harness._theme_snapshot = lambda: get_theme(settings.name)
    harness._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    harness._update_map_mode_buttons = lambda *_args, **_kwargs: None
    harness._update_map_view_status_label = lambda *_args, **_kwargs: None
    harness._sync_map_control_button_widths = lambda *_args, **_kwargs: None
    harness._update_splitter_indicator_state = lambda *_args, **_kwargs: None
    harness._reflow_map_filter_bar = lambda *_args, **_kwargs: None
    harness._prop_target_context = lambda: {"label": "National"}
    harness._update_prop_badge = lambda *_args, **_kwargs: None
    harness._update_map_support_card = lambda *_args, **_kwargs: None

    def render_selection(payload: dict[str, object]) -> None:
        theme = get_theme(settings.name)
        harness.selection_renders.append(
            (dict(payload), theme["surface"], theme["text"])
        )

    harness._show_map_selected_detail = render_selection
    return harness


def test_live_theme_switch_rethemes_native_scene_and_existing_selection_panel() -> None:
    """A selected panel must not retain light HTML after the app turns dark."""
    settings = _ThemeSettings("dark")
    renderer = _ThemeRenderer()
    tab = _theme_refresh_harness(settings, renderer)

    StationsMapTab.apply_theme(tab)
    settings.name = "light"
    StationsMapTab.apply_theme(tab)

    assert [theme["bg"] for theme in renderer.themes] == [
        get_theme("dark")["bg"],
        get_theme("light")["bg"],
    ]
    assert tab.selection_renders == [
        (tab._map_selected_payload, get_theme("dark")["surface"], get_theme("dark")["text"]),
        (tab._map_selected_payload, get_theme("light")["surface"], get_theme("light")["text"]),
    ]
