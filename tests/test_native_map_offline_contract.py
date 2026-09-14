"""Immutable offline and interaction contracts for the operator Map.

The Map is an RF field-operations surface.  Its basemap and every operator
interaction must remain useful with networking disabled, without an API key,
and without a tile-provider runtime.  These tests intentionally keep the
contract at the native renderer boundary: projection preparation may read
bundled assets on its worker, but applying an already-prepared snapshot may not
perform file or network I/O.
"""

from __future__ import annotations

import builtins
import os
from pathlib import Path
import re
import socket

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QPoint, QPointF, QRect, Qt
from PySide6.QtQml import QJSValue
from PySide6.QtQuick import QQuickItem
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QMainWindow

from freqinout.gui.map_window import PersistentMapWindow
from freqinout.gui.native_map_projection import build_native_overlay_projection
from freqinout.gui.native_map_renderer import NativeMapRenderer


ROOT = Path(__file__).resolve().parents[1]
RENDERER_SOURCE = ROOT / "freqinout/gui/native_map_renderer.py"
QML_SOURCE = ROOT / "freqinout/gui/qml/native_map_renderer.qml"


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _find_visual_item(root: QQuickItem | None, object_name: str) -> QQuickItem | None:
    """Find a QML scene item, including delegates owned by MapItemView.

    Qt Location parents generated delegates in the visual scene graph rather
    than the ordinary QObject tree, so ``findChild`` can report a false-not-
    ready result even after a marker is paintable and interactive.
    """
    pending = [root] if root is not None else []
    while pending:
        item = pending.pop()
        if item.objectName() == object_name:
            return item
        pending.extend(item.childItems())
    return None


class _Settings:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def get(self, key: str, default: object = None) -> object:
        return self.values.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.values[key] = value


def _full_projection() -> dict[str, object]:
    """One snapshot covering the prior Map's selectable overlay semantics."""
    return {
        "view": {"lat": 39.5, "lon": -104.9, "zoom": 4.0},
        "markers": [
            {"id": "station-a", "callsign": "STATION-A", "lat": 39.7, "lon": -104.9},
        ],
        "weather_events": [
            {"id": "weather-a", "title": "Weather", "lat": 39.8, "lon": -105.0},
        ],
        "alert_events": [
            {"id": "alert-a", "title": "Alert", "lat": 39.9, "lon": -105.1},
        ],
        "infrastructure_events": [
            {"id": "infra-a", "title": "Infrastructure", "lat": 40.0, "lon": -105.2},
        ],
        "paths": [
            {
                "id": "path-a",
                "points": [
                    {"lat": 39.7, "lon": -104.9},
                    {"lat": 40.1, "lon": -105.3},
                ],
            },
        ],
        "polygons": [
            {
                "id": "polygon-a",
                "kind": "state",
                "payload": {
                    "type": "regional_intelligence",
                    "state": "CO",
                    "label": "Colorado",
                    "level": "yellow",
                },
                "points": [
                    {"lat": 39.0, "lon": -106.0},
                    {"lat": 40.0, "lon": -106.0},
                    {"lat": 40.0, "lon": -105.0},
                ],
            },
        ],
        "grid_lines": [
            {
                "id": "grid-a",
                "points": [
                    {"lat": 38.0, "lon": -105.0},
                    {"lat": 41.0, "lon": -105.0},
                ],
            },
        ],
        "grid_labels": [
            {"id": "grid-label-a", "label": "Grid", "lat": 39.4, "lon": -105.0},
        ],
        "city_labels": [
            {"id": "city-a", "label": "City", "lat": 39.6, "lon": -104.8},
        ],
        "show_cities": True,
        "show_city_labels": True,
        "regional_fills": [
            {"id": "region-a", "lat": 39.5, "lon": -105.0, "radius_m": 120_000},
        ],
        "propagation_fills": [
            {"id": "prop-a", "lat": 39.5, "lon": -104.7, "radius_m": 80_000},
        ],
        "legend": [{"label": "Station", "color_role": "accent"}],
        "regional_summary": "Offline regional summary",
        "link_direction_markers": True,
    }


def test_renderer_has_no_online_provider_api_key_tile_or_network_dependency() -> None:
    """The local item-overlay scene must never initialize an online provider."""
    sources = "\n".join(
        (
            RENDERER_SOURCE.read_text(encoding="utf-8"),
            QML_SOURCE.read_text(encoding="utf-8"),
        )
    ).lower()

    forbidden = (
        'name: "osm"',
        "api key",
        "apikey",
        "tilelayer",
        "mapbox",
        "openstreetmap",
        "qnetwork",
        "xmlhttprequest",
        "https://",
        "http://",
    )
    assert not [token for token in forbidden if token in sources]
    assert "itemsoverlay" in sources


def test_offline_surface_always_has_a_local_vector_basemap_below_overlays() -> None:
    """The map outline is bundled data, not a blank/provider placeholder."""
    renderer_source = RENDERER_SOURCE.read_text(encoding="utf-8")
    qml_source = QML_SOURCE.read_text(encoding="utf-8")

    assert "def basePolygons(self)" in renderer_source
    assert "mapBridge.basePolygons" in qml_source
    assert qml_source.index("mapBridge.basePolygons") < qml_source.index("mapBridge.polygons")


def test_complete_bundled_north_america_basemap_reaches_the_renderer() -> None:
    """The operational overlay cap must not clip bundled Mexico geometry."""
    _app()
    projection = build_native_overlay_projection({})
    renderer = NativeMapRenderer()
    try:
        assert len(projection["base_polygons"]) == 207
        renderer.apply_projection(projection)
        assert len(renderer.bridge.basePolygons) == 207
        assert any(item.get("name") == "Yucatán" for item in renderer.bridge.basePolygons)
    finally:
        renderer.shutdown()


def test_offline_surface_declares_wheel_button_zoom_and_drag_pan() -> None:
    """Mouse-wheel, visible controls, and drag gestures remain first-class."""
    source = QML_SOURCE.read_text(encoding="utf-8")

    assert "WheelHandler" in source
    assert "DragHandler" in source or "onPositionChanged" in source
    assert '"accessible": "Zoom in"' in source
    assert '"accessible": "Zoom out"' in source
    assert "Accessible.name: modelData.accessible" in source
    assert "stationMap.zoomLevel =" in source
    assert "stationMap.pan(" in source
    assert source.count("TapHandler") >= 3


def test_offline_surface_keeps_markers_paths_and_polygons_selectable() -> None:
    """All selectable layer types use the direct Qt action bridge."""
    _app()
    renderer = NativeMapRenderer()
    actions: list[dict[str, object]] = []
    renderer.action_requested.connect(actions.append)
    try:
        renderer.apply_projection(_full_projection())
        bridge = renderer.bridge

        assert len(bridge.markers) == 4
        assert len(bridge.paths) == 1
        assert len(bridge.polygons) == 1
        bridge.select_marker(bridge.markers[0])
        bridge.select_path(bridge.paths[0])
        bridge.select_polygon(bridge.polygons[0])

        assert [action["action"] for action in actions] == [
            "select_marker",
            "select_path",
            "select_polygon",
        ]
        assert actions[0]["payload"]["id"] == "station-a"
        assert actions[1]["payload"]["id"] == "path-a"
        assert actions[2]["id"] == "polygon:polygon-a"
        polygon_payload = actions[2]["payload"]
        assert polygon_payload["type"] == "regional_intelligence"
        assert polygon_payload["state"] == "CO"
        assert polygon_payload["label"] == "Colorado"
        assert polygon_payload["level"] == "yellow"
        assert "payload" not in polygon_payload
    finally:
        renderer.shutdown()


def test_qml_selection_bridge_uses_stable_ids_not_nested_model_objects() -> None:
    """Delegate clicks must not recursively convert a complete QML modelData."""
    renderer_source = RENDERER_SOURCE.read_text(encoding="utf-8")
    qml_source = QML_SOURCE.read_text(encoding="utf-8")

    for item_type in ("Marker", "Path", "Polygon"):
        assert f"select{item_type}ById" in renderer_source
        assert f"select{item_type}ById" in qml_source
        assert f"select{item_type}ById(String(modelData.id" in qml_source
        assert f"select{item_type}(modelData)" not in qml_source
    assert 'selectMarkerById("fixed")' not in qml_source


def test_marker_tap_surface_is_at_least_32_pixels_without_shrinking_its_label() -> None:
    """Pins remain comfortably clickable even when their visual dot is small."""
    source = QML_SOURCE.read_text(encoding="utf-8")
    target = re.search(r"id:\s*markerTarget(?P<body>.*?)(?:\n\s*}\n\s*})", source, re.DOTALL)

    assert target is not None
    width = re.search(r"width:\s*Math\.max\((\d+)", target.group("body"))
    height = re.search(r"height:\s*([^\n]+)", target.group("body"))
    assert width is not None and int(width.group(1)) >= 32
    assert height is not None
    height_candidates = [int(value) for value in re.findall(r"\d+", height.group(1))]
    assert height_candidates and min(height_candidates) >= 32
    assert "anchors.top: markerDot.bottom" in target.group("body")
    assert "TapHandler" in target.group("body")


def test_visible_qquick_marker_click_emits_exactly_one_direct_action() -> None:
    """A real center-marker click reaches Python once without QJSValue recursion."""
    app = _app()
    renderer = NativeMapRenderer()
    renderer.resize(720, 520)
    renderer.show()
    actions: list[dict[str, object]] = []
    renderer.action_requested.connect(actions.append)
    try:
        renderer.apply_projection(
            {
                "view": {"lat": 39.5, "lon": -104.9, "zoom": 6.0},
                "markers": [
                    {
                        "id": "center-station",
                        "callsign": "CENTER-STATION",
                        "lat": 39.5,
                        "lon": -104.9,
                    }
                ],
            }
        )
        marker_target: QQuickItem | None = None
        for _attempt in range(80):
            app.processEvents()
            QTest.qWait(15)
            quick = renderer.quick_widget
            root = quick.rootObject() if quick is not None else None
            marker_target = _find_visual_item(root, "offlineMapMarker:station:center-station")
            if (
                quick is not None
                and quick.isVisible()
                and marker_target is not None
                and bool(marker_target.property("visible"))
                and float(marker_target.property("width") or 0.0) >= 32.0
                and float(marker_target.property("height") or 0.0) >= 32.0
            ):
                break

        quick = renderer.quick_widget
        assert quick is not None
        assert quick.isVisible()
        assert marker_target is not None, "center marker delegate was not ready for interaction"
        scene_center = marker_target.mapToScene(
            QPointF(
                float(marker_target.property("width")) / 2.0,
                float(marker_target.property("height")) / 2.0,
            )
        )
        click_point = QPoint(round(scene_center.x()), round(scene_center.y()))
        assert quick.rect().contains(click_point), f"marker hit target is outside the visible Map: {click_point}"
        QTest.mouseClick(quick, Qt.LeftButton, Qt.NoModifier, click_point)
        for _attempt in range(10):
            app.processEvents()
            QTest.qWait(10)
            if actions:
                break

        assert len(actions) == 1
        assert actions[0]["action"] == "select_marker"
        assert actions[0]["id"] == "station:center-station"
        assert actions[0]["payload"]["id"] == "center-station"
    finally:
        renderer.hide()
        renderer.shutdown()
        renderer.deleteLater()
        app.processEvents()


def test_adaptive_maidenhead_grid_is_viewport_bounded_and_zoom_sensitive() -> None:
    """Offline grid parity keeps 2/4/6-character detail without scene floods."""
    app = _app()
    renderer = NativeMapRenderer()
    renderer.resize(720, 520)
    renderer.show()
    try:
        renderer.apply_projection(
            {
                "view": {"lat": 39.5, "lon": -104.9, "zoom": 6.0},
                "show_grids": True,
                "show_grid_labels": True,
            }
        )
        root = renderer.quick_widget.rootObject() if renderer.quick_widget is not None else None
        assert root is not None

        def grid_values(name: str) -> list[object]:
            value = root.property(name)
            return list(value.toVariant() if isinstance(value, QJSValue) else value or [])

        for _attempt in range(30):
            app.processEvents()
            QTest.qWait(20)
            if grid_values("dynamicGridLabels"):
                break
        lines = grid_values("dynamicGridLines")
        labels = grid_values("dynamicGridLabels")
        assert 0 < len(lines) <= 320
        assert 0 < len(labels) <= 400
        assert all(len(str(item["text"])) == 4 for item in labels)

        renderer.set_view_center(39.5, -104.9, 10.0)
        for _attempt in range(30):
            app.processEvents()
            QTest.qWait(20)
            labels = grid_values("dynamicGridLabels")
            if labels and all(len(str(item["text"])) == 6 for item in labels):
                break
        lines = grid_values("dynamicGridLines")
        assert 0 < len(lines) <= 320
        assert 0 < len(labels) <= 400
        assert all(len(str(item["text"])) == 6 for item in labels)
    finally:
        renderer.hide()
        renderer.shutdown()
        renderer.deleteLater()
        app.processEvents()


def test_offline_projection_preserves_every_existing_visual_layer() -> None:
    """The provider-free base must not discard the established Map overlays."""
    _app()
    renderer = NativeMapRenderer()
    try:
        renderer.apply_projection(_full_projection())
        bridge = renderer.bridge

        assert {item["kind"] for item in bridge.markers} == {
            "station",
            "weather",
            "alert",
            "infrastructure",
        }
        assert len(bridge.paths) == 1
        assert len(bridge.polygons) == 1
        assert len(bridge.gridLines) == 1
        assert len(bridge.gridLabels) == 1
        assert len(bridge.cityLabels) == 1
        assert {item["kind"] for item in bridge.fills} == {"regional", "propagation"}
        assert len(bridge.directions) == 1
        assert bridge.legend == [{"label": "Station", "color_role": "accent"}]
        assert bridge.summary == "Offline regional summary"
    finally:
        renderer.shutdown()


def test_zoom_pan_and_selection_never_change_popout_or_main_geometry() -> None:
    """Map gestures update only its cached view, never either top-level window."""
    app = _app()
    host = QMainWindow()
    host.setGeometry(43, 71, 910, 570)
    host.show()
    map_window = PersistentMapWindow(host, _Settings(), NativeMapRenderer)
    try:
        map_window.present()
        app.processEvents()
        renderer = map_window.map_tab
        assert isinstance(renderer, NativeMapRenderer)
        renderer.apply_projection(_full_projection())
        host_before = QRect(host.geometry())
        map_before = QRect(map_window.geometry())
        view_before = renderer.view_state()

        renderer.set_view_center(39.7, -104.5, 5.0)
        renderer.bridge.report_view(39.6, -104.3, 5.0)
        renderer.bridge.select_marker(renderer.bridge.markers[0])
        app.processEvents()

        view_after = renderer.view_state()
        assert view_after["zoom"] == 5.0
        assert (view_after["lat"], view_after["lon"]) != (
            view_before["lat"],
            view_before["lon"],
        )
        assert QRect(host.geometry()) == host_before
        assert QRect(map_window.geometry()) == map_before
    finally:
        map_window.shutdown()
        map_window.deleteLater()
        host.close()
        host.deleteLater()
        app.processEvents()


def test_apply_projection_performs_no_file_or_network_io(monkeypatch) -> None:
    """GUI projection apply is a pure in-memory swap of a prepared snapshot."""
    _app()
    renderer = NativeMapRenderer()
    calls: list[str] = []

    def fail_open(*_args, **_kwargs):
        calls.append("file")
        raise AssertionError("renderer projection apply attempted file I/O")

    def fail_network(*_args, **_kwargs):
        calls.append("network")
        raise AssertionError("renderer projection apply attempted network I/O")

    monkeypatch.setattr(builtins, "open", fail_open)
    monkeypatch.setattr(socket, "socket", fail_network)
    monkeypatch.setattr(socket, "create_connection", fail_network)
    try:
        renderer.apply_projection(_full_projection())
        assert calls == []
    finally:
        renderer.shutdown()
