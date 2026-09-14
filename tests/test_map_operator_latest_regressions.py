"""Operator-facing regressions for the Map legend, filter bar, and first paint."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QPoint
from PySide6.QtWidgets import QApplication, QDialog, QFrame, QGridLayout, QLabel, QPushButton, QWidget

import freqinout.gui.stations_map_tab as stations_map_module
from freqinout.gui.native_map_projection import build_native_overlay_projection
from freqinout.gui.stations_map_tab import StationsMapTab


ROOT = Path(__file__).resolve().parents[1]


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _legend_colors(projection: dict[str, object]) -> dict[str, str]:
    legend = projection.get("legend", [])
    colors: dict[str, str] = {}

    def collect(items: object) -> None:
        if not isinstance(items, list):
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            colors[str(item.get("label") or "")] = str(item.get("color") or "")
            collect(item.get("items"))

    collect(legend)
    return colors


def test_default_station_pin_legend_keeps_the_sitrep_status_key() -> None:
    """Station pins need the production SitRep key even without an active overlay."""
    projection = build_native_overlay_projection(
        {
            "markers": [
                {"id": "station-1", "lat": 39.7392, "lon": -104.9903},
            ],
        }
    )

    colors = _legend_colors(projection)
    assert colors["Stations"] == ""
    assert colors["SitRep Status:"] == ""
    assert {label: colors[label] for label in (
        "Functioning",
        "Partially Functioning",
        "Not Functioning",
        "Unknown / No Report",
    )} == {
        "Functioning": "#43a047",
        "Partially Functioning": "#fbc02d",
        "Not Functioning": "#d32f2f",
        "Unknown / No Report": "#4fc3f7",
    }


def test_dynamic_legend_names_every_visible_station_context_layer() -> None:
    """Context labels supplement, rather than replace, the station-pin key."""
    projection = build_native_overlay_projection(
        {
            "markers": [{"id": "station-1", "lat": 39.7392, "lon": -104.9903}],
            "show_states": True,
            "show_regions": True,
            "show_grids": True,
            "show_cities": True,
            "show_city_labels": True,
            "city_labels": [
                {"id": "denver", "label": "Denver", "lat": 39.7392, "lon": -104.9903},
            ],
        }
    )

    labels = set(_legend_colors(projection))
    assert {"State boundaries", "Cities", "Stations"} <= labels
    assert {
        "Functioning",
        "Partially Functioning",
        "Not Functioning",
        "Unknown / No Report",
    } <= labels


def test_station_pin_fills_match_the_production_sitrep_legend() -> None:
    """Status remains a data color across both application themes."""
    statuses = ("green", "yellow", "red", "unknown")
    projection = build_native_overlay_projection(
        {
            "markers": [
                {"id": status, "lat": 39.0 + index, "lon": -105.0, "spotter_status_key": status}
                for index, status in enumerate(statuses)
            ]
        }
    )

    assert [marker["color"] for marker in projection["markers"]] == [
        "#43a047",
        "#fbc02d",
        "#d32f2f",
        "#4fc3f7",
    ]
    from freqinout.gui.native_map_renderer import NativeMapRenderer

    normalized_markers = NativeMapRenderer._normalise_markers(projection["markers"])
    normalized_legend = NativeMapRenderer._normalise_legend(
        projection["legend"], normalized_markers, ()
    )
    assert [marker["color"] for marker in normalized_markers] == [
        "#43a047", "#fbc02d", "#d32f2f", "#4fc3f7",
    ]
    station_group = next(item for item in normalized_legend if item["label"] == "Stations")
    heading = next(item for item in station_group["items"] if item["label"] == "SitRep Status:")
    assert heading["heading"] is True


def test_legend_wraps_inside_the_native_canvas() -> None:
    qml = (ROOT / "freqinout/gui/qml/native_map_renderer.qml").read_text(encoding="utf-8")
    legend_start = qml.index("id: legendPanel")
    legend_body = qml[legend_start:]

    assert "Flow {\n            id: legendRow" in legend_body
    assert "parent.width - 24" in legend_body
    assert "Math.min(1040" in legend_body
    assert "property var childItems: modelData.items || []" in legend_body


def _filter_field(parent: QWidget, name: str) -> QWidget:
    field = QWidget(parent)
    field.setObjectName(name)
    field.setMinimumWidth(175)
    return field


def _filter_reflow_harness() -> tuple[SimpleNamespace, QGridLayout, tuple[QWidget, ...], QFrame]:
    _app()
    bar = QFrame()
    grid = QGridLayout(bar)
    grid.setHorizontalSpacing(10)
    fields = tuple(
        _filter_field(bar, name)
        for name in ("View", "Type", "Group", "Age", "Topic", "Sensitivity", "Paths")
    )
    host = SimpleNamespace(
        _map_filter_bar=bar,
        _map_filter_grid=grid,
        _map_filter_fields=fields,
        _map_search_field=_filter_field(bar, "Search"),
        _map_clear_filters_button=QPushButton("Clear Filters", bar),
        _map_clear_layers_button=QPushButton("Clear Layers", bar),
        _now_reachable_label=QLabel("", bar),
        _map_filter_columns=None,
        width=lambda: bar.width(),
    )
    return host, grid, fields, bar


def test_principal_map_filters_share_the_wide_row_and_wrap_to_available_columns() -> None:
    """View, Topic, Group, and Age must not force Age's menu offscreen."""
    host, grid, fields, bar = _filter_reflow_harness()
    by_name = {field.objectName(): field for field in fields}

    bar.resize(1800, 400)
    StationsMapTab._reflow_map_filter_bar(host)

    assert host._map_filter_columns == 4
    assert {
        grid.getItemPosition(grid.indexOf(by_name[name]))[0]
        for name in ("View", "Topic", "Group", "Age")
    } == {0}

    bar.resize(620, 700)
    StationsMapTab._reflow_map_filter_bar(host)

    assert host._map_filter_columns == 2
    assert all(
        grid.getItemPosition(grid.indexOf(field))[1] in {0, 1}
        for field in fields
    )
    bar.deleteLater()
    _app().processEvents()


def test_hidden_mode_specific_filters_reserve_no_grid_cells() -> None:
    """All Stations keeps one complete primary row with actions directly below."""
    host, grid, fields, bar = _filter_reflow_harness()
    by_name = {field.objectName(): field for field in fields}
    for name in ("Type", "Sensitivity", "Paths"):
        by_name[name].hide()
    bar.resize(1800, 400)

    StationsMapTab._reflow_map_filter_bar(host)

    assert [
        grid.getItemPosition(grid.indexOf(by_name[name]))[:2]
        for name in ("View", "Topic", "Group", "Age")
    ] == [(0, 0), (0, 1), (0, 2), (0, 3)]
    assert grid.getItemPosition(grid.indexOf(host._map_search_field))[0] == 1
    bar.deleteLater()
    _app().processEvents()


class _RecordingPopover(QDialog):
    """Captures the requested popup position before the window manager can adjust it."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.requested_positions: list[QPoint] = []

    def move(self, position) -> None:  # noqa: N802 - Qt virtual method name
        self.requested_positions.append(QPoint(position))
        super().move(position)


def test_age_popover_is_positioned_fully_inside_its_map_screen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The age chooser may open beside its trigger, but never beyond the Map screen."""
    app = _app()
    monkeypatch.setattr(stations_map_module, "QDialog", _RecordingPopover)
    tab = StationsMapTab.__new__(StationsMapTab)
    QWidget.__init__(tab)
    # This focused fixture has no filter/splitter hierarchy to reflow.
    tab._is_shutting_down = True
    tab.resize(360, 120)
    tab._map_since_button = QPushButton("Age: 24h", tab)
    tab._map_since_button.setGeometry(180, 24, 160, 36)
    tab._map_since_popover = None
    tab._current_recency_options = lambda: [
        ("15m", 15 * 60), ("30m", 30 * 60), ("1h", 60 * 60), ("3h", 3 * 60),
        ("6h", 6 * 60), ("12h", 12 * 60), ("24h", 24 * 60), ("3d", 3 * 24 * 60 * 60),
        ("7d", 7 * 24 * 60 * 60), ("14d", 14 * 24 * 60 * 60), ("30d", 30 * 24 * 60 * 60),
        ("60d", 60 * 24 * 60 * 60), ("90d", 90 * 24 * 60 * 60), ("Any", None),
    ]
    tab._current_map_mode_key = lambda: "all"
    tab._theme_snapshot = lambda: {"text_muted": "#5f6b76"}
    tab._set_map_recency_from_label = lambda _label: None
    tab.show()
    app.processEvents()
    screen = tab.screen()
    assert screen is not None
    available = screen.availableGeometry()
    tab.move(
        available.right() - tab.width() + 1,
        available.bottom() - tab.height() + 1,
    )
    app.processEvents()

    StationsMapTab._show_map_since_popover(tab)
    app.processEvents()

    popover = tab._map_since_popover
    assert isinstance(popover, _RecordingPopover)
    assert popover.requested_positions
    requested = popover.requested_positions[-1]
    assert requested.x() >= available.left()
    assert requested.y() >= available.top()
    assert requested.x() + popover.sizeHint().width() <= available.right() + 1
    assert requested.y() + popover.sizeHint().height() <= available.bottom() + 1

    popover.close()
    tab.close()
    popover.deleteLater()
    tab.deleteLater()
    app.processEvents()


def test_first_popout_frame_is_built_before_showing_the_native_map_window() -> None:
    """First paint must expose the retained native hierarchy, never a handoff scene."""
    source = (ROOT / "freqinout/gui/map_window.py").read_text(encoding="utf-8")
    present_start = source.index("    def present(self) -> None:")
    present_body = source[present_start : source.index("    def showMaximized", present_start)]

    assert present_body.index("self.ensure_content()") < present_body.index("self.show()")
    assert "QTimer.singleShot" not in present_body
    assert "setCentralWidget(" not in present_body


def test_native_surface_waits_for_first_complete_projection_before_reveal() -> None:
    """A ready QML substrate is not itself a complete operator-visible map frame."""
    from freqinout.gui.native_map_renderer import NativeMapRenderer

    app = _app()
    renderer = NativeMapRenderer()
    try:
        app.processEvents()
        if renderer.quick_widget is None or renderer.renderer_state != "ready":
            pytest.skip("Qt Quick substrate is unavailable in this test environment")
        assert renderer._surface_stack.currentWidget() is renderer._status

        renderer.apply_projection({"markers": [], "links": []})
        assert renderer._surface_stack.currentWidget() is renderer.quick_widget
    finally:
        renderer.shutdown()
        renderer.deleteLater()
        app.processEvents()
