"""Slice 3 native Map parity and deployment contracts.

These tests describe the final native rendering boundary: advanced map layers
remain bounded value snapshots, the QML scene inherits FIO's shared theme, and
packaged builds ship Qt Location rather than a browser renderer.  They perform
no tile, station-database, or device I/O.
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication


ROOT = Path(__file__).resolve().parents[1]
RENDERER = ROOT / "freqinout/gui/native_map_renderer.py"
QML = ROOT / "freqinout/gui/qml/native_map_renderer.qml"
PROJECTION = ROOT / "freqinout/gui/native_map_projection.py"
PYINSTALLER_SPEC = ROOT / "FreqInOut.spec"
README = ROOT / "README.md"


def _normalised_windows_source(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\\\\", "/")


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_native_map_qml_uses_shared_theme_and_never_reintroduces_browser_code() -> None:
    renderer_source = RENDERER.read_text(encoding="utf-8")
    qml_source = QML.read_text(encoding="utf-8")

    assert "active_app_theme" in renderer_source
    assert "mapBridge.theme" in qml_source
    assert "QWebEngine" not in renderer_source
    assert "runJavaScript" not in renderer_source
    assert "WebView" not in qml_source
    assert "WebEngine" not in qml_source
    assert "leaflet" not in qml_source.lower()


def test_pyinstaller_packages_native_qml_and_qt_location_runtime_modules() -> None:
    """Frozen builds must contain QML plus Qt Location/Positioning imports."""
    spec_source = _normalised_windows_source(PYINSTALLER_SPEC)

    assert "('freqinout/gui/qml', 'freqinout/gui/qml')" in spec_source
    for module in (
        "PySide6.QtLocation",
        "PySide6.QtPositioning",
        "PySide6.QtQml",
        "PySide6.QtQuickControls2",
        "PySide6.QtQuickWidgets",
    ):
        assert module in spec_source
    assert '"QtQuick/Controls"' in spec_source
    assert '"QtQuick/Templates"' in spec_source
    assert '"itemsoverlay" in path.name.lower()' in spec_source


def test_readme_deployment_guidance_names_qt_location_not_qtwebengine() -> None:
    """Operators need the dependency that the shipped Map actually uses."""
    readme = README.read_text(encoding="utf-8")

    assert "Qt Location" in readme
    assert "QtWebEngine map support" not in readme


def test_native_overlay_projection_uses_canonical_typed_layers_and_worker_enrichment() -> None:
    """Advanced layers must cross the QML boundary as bounded value snapshots.

    A native repaint must never reach the map's database/read model.  States,
    regions, grids, cities, propagation, Regional Intel, and path-direction
    cues therefore share explicit typed, capped lists rather than bespoke
    renderer paths or browser-side data channels.
    """
    renderer_source = RENDERER.read_text(encoding="utf-8")
    qml_source = QML.read_text(encoding="utf-8")
    tab_source = (ROOT / "freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    projection_source = PROJECTION.read_text(encoding="utf-8")

    assert "def build_native_overlay_projection" in projection_source
    assert "from freqinout.gui.native_map_projection import build_native_overlay_projection" in tab_source
    assert "build_native_overlay_projection(" in tab_source
    for property_name in (
        "polygons",
        "gridLines",
        "gridLabels",
        "cityLabels",
        "fills",
        "directions",
        "legend",
        "summary",
    ):
        assert f"def {property_name}(self)" in renderer_source
        assert f"mapBridge.{property_name}" in qml_source
    for key in (
        "polygons",
        "grid_lines",
        "grid_labels",
        "city_labels",
        "propagation_fills",
        "regional_intelligence",
        "regional_summary",
        "legend",
        "link_direction_markers",
    ):
        assert key in renderer_source or key in projection_source
    for key in ("polygons", "grid_lines", "grid_labels", "city_labels", "regional_summary", "legend"):
        assert key in projection_source


def test_native_advanced_overlay_projection_preserves_typed_layers_with_caps() -> None:
    """The native bridge retains each supported layer and drops excess items.

    The concrete limit is renderer-owned so one large age window cannot make a
    QML repaint expensive.  Input items use only serializable map values; the
    test is intentionally independent of tile traffic and station storage.
    """
    _app()
    from freqinout.gui.native_map_renderer import NativeMapRenderer
    import freqinout.gui.native_map_renderer as native_map

    renderer = NativeMapRenderer()
    try:
        polygons = [
            {
                "id": f"state-{index}",
                "kind": "state",
                "points": [{"lat": 38.0, "lon": -105.0}, {"lat": 39.0, "lon": -105.0}, {"lat": 39.0, "lon": -104.0}],
                "color_role": "info",
                "payload": {"state": "CO"},
            }
            for index in range(native_map._MAX_POLYGONS + 3)
        ]
        paths = [
            {
                "id": f"path-{index}",
                "kind": "path",
                "points": [{"lat": 39.0, "lon": -105.0}, {"lat": 40.0, "lon": -104.0}],
                "color_role": "info",
                "payload": {"origin": "N0CALL", "destination": "K0CALL"},
            }
            for index in range(native_map._MAX_PATHS + 3)
        ]
        labels = [
            {"id": f"label-{index}", "lat": 39.0, "lon": -105.0, "text": "Denver", "color_role": "text_muted", "payload": {}}
            for index in range(native_map._MAX_LABELS + 3)
        ]
        projection = {
            "polygons": polygons,
            "grid_lines": paths,
            "grid_labels": labels,
            "city_labels": labels,
            "show_cities": True,
            "show_city_labels": True,
            "propagation_fills": [
                {"id": f"prop-{index}", "lat": 40.0, "lon": -104.0, "radius_m": 10_000, "color_role": "info", "payload": {}}
                for index in range(native_map._MAX_FILLS + 3)
            ],
            "regional_intelligence": {"enabled": True, "states": {"CO": {"lat": 39.0, "lon": -105.0, "level": "yellow"}}},
            "regional_summary": "Regional attention",
            "legend": [{"label": "Attention", "color_role": "warning"}] * (native_map._MAX_LEGEND_ITEMS + 3),
            "paths": paths,
            "link_direction_markers": True,
        }

        renderer.apply_projection(projection)
        bridge = renderer.bridge

        assert len(bridge.polygons) == native_map._MAX_POLYGONS
        assert len(bridge.gridLines) == native_map._MAX_PATHS
        assert len(bridge.gridLabels) == native_map._MAX_LABELS
        assert len(bridge.cityLabels) == native_map._MAX_LABELS
        assert len(bridge.fills) == native_map._MAX_FILLS
        assert len(bridge.directions) == native_map._MAX_PATHS
        assert len(bridge.legend) == native_map._MAX_LEGEND_ITEMS
        assert bridge.summary == "Regional attention"
        assert bridge.directionIndicators is True
        for collection in (bridge.polygons, bridge.gridLines, bridge.gridLabels, bridge.cityLabels, bridge.fills):
            assert collection
            assert {"id", "kind", "color_role", "_original_payload"}.issubset(collection[0])
        assert {"points", "kind"}.issubset(bridge.polygons[0])
        assert {"points", "kind"}.issubset(bridge.gridLines[0])
        assert {"path", "kind", "color_role"}.issubset(bridge.directions[0])
    finally:
        renderer.shutdown()


def test_hidden_prebuilt_popout_orders_content_before_first_show_without_geometry_mutation() -> None:
    """Native content is final-parented while hidden, never attached post-show."""
    source = (ROOT / "freqinout/gui/map_window.py").read_text(encoding="utf-8")
    present_start = source.index("    def present(self)")
    present_body = source[present_start : source.index("    def showMaximized", present_start)]

    assert "if self._map_tab is None:\n            self.ensure_content()" in present_body
    assert present_body.index("self.ensure_content()") < present_body.index("self.show()")
    assert "setCentralWidget(" not in present_body
    assert ".setGeometry(" not in present_body
    assert ".resize(" not in present_body
    assert ".move(" not in present_body
