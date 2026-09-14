"""Focused Slice 1 contracts for the native Qt Location Map renderer.

The Map pop-out must own one ordinary Qt widget hierarchy from its first paint.
These tests deliberately exercise no network tile traffic: they protect the
native renderer's import/availability, ownership, geometry, theme, and shutdown
contracts that make first Map launch calm on macOS, Linux, and Windows.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QRect, QSize, Qt
from PySide6.QtWidgets import QApplication, QMainWindow, QSizePolicy, QWidget

from freqinout.gui.map_window import PersistentMapWindow


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _Settings:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def get(self, key: str, default: object = None) -> object:
        return self.values.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.values[key] = value


@pytest.fixture
def map_host() -> QMainWindow:
    app = _app()
    host = QMainWindow()
    host.setGeometry(61, 83, 940, 620)
    host.show()
    app.processEvents()
    try:
        yield host
    finally:
        host.close()
        host.deleteLater()
        app.processEvents()


def _snapshot(window: QMainWindow) -> tuple[QRect, Qt.WindowState, bool, bool, object]:
    return (
        QRect(window.geometry()),
        window.windowState(),
        bool(window.isMaximized()),
        bool(window.isFullScreen()),
        window.screen(),
    )


def test_native_renderer_declares_qt_location_qquick_not_webengine_and_uses_shared_theme() -> None:
    """The replacement must not retain a Chromium/native-browser dependency."""
    source = Path("freqinout/gui/native_map_renderer.py").read_text(encoding="utf-8")

    assert "QQuickWidget" in source
    assert "QtLocation" in source
    assert "active_app_theme" in source
    assert "QWebEngine" not in source
    assert "QPalette" not in source
    assert ".setPalette(" not in source


def test_packaging_declares_the_qt_addons_needed_for_qt_location() -> None:
    """An installed native renderer must not rely on a developer-only Qt module."""
    project = Path("pyproject.toml").read_text(encoding="utf-8")
    requirements = Path("requirements.txt").read_text(encoding="utf-8")

    assert "PySide6-Addons==6.8.1.1" in project
    assert "PySide6-Addons==6.8.1.1" in requirements


def test_native_renderer_constructs_one_stable_qquick_surface_with_neutral_hints() -> None:
    """A cold renderer cannot participate in top-level size negotiation."""
    _app()
    from freqinout.gui.native_map_renderer import NativeMapRenderer

    owner = QWidget()
    renderer = NativeMapRenderer(owner)
    try:
        assert renderer.parentWidget() is owner
        assert renderer.quick_widget.parentWidget() is renderer
        assert renderer.sizePolicy().horizontalPolicy() is QSizePolicy.Ignored
        assert renderer.sizePolicy().verticalPolicy() is QSizePolicy.Ignored
        assert renderer.quick_widget.sizePolicy().horizontalPolicy() is QSizePolicy.Ignored
        assert renderer.quick_widget.sizePolicy().verticalPolicy() is QSizePolicy.Ignored
        assert renderer.sizeHint() == QSize(0, 0)
        assert renderer.minimumSizeHint() == QSize(0, 0)
        assert renderer.is_available() is True
        assert "unavailable" not in renderer.status_text().lower()
    finally:
        renderer.shutdown()
        owner.deleteLater()
        _app().processEvents()


def test_native_renderer_unavailable_construction_is_calm_and_actionable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken QML/Qt Location setup is shown in-window, never raised at launch."""
    _app()
    import freqinout.gui.native_map_renderer as native_map

    class _UnavailableQuickWidget:
        def __init__(self, *_args, **_kwargs) -> None:
            raise RuntimeError("simulated Qt Quick failure")

    monkeypatch.setattr(native_map, "QQuickWidget", _UnavailableQuickWidget)
    owner = QWidget()
    renderer = native_map.NativeMapRenderer(owner)
    try:
        assert renderer.is_available() is False
        status = renderer.status_text().lower()
        assert "unavailable" in status
        assert "retry" in status or "review" in status
        renderer.set_map_visible(True)
        renderer.apply_projection({"markers": [], "links": []})
    finally:
        renderer.shutdown()
        owner.deleteLater()
        _app().processEvents()


def test_native_renderer_first_show_never_mutates_main_or_map_window_geometry(map_host: QMainWindow) -> None:
    """Cold native content attaches inside a final-size persistent Map window."""
    app = _app()
    from freqinout.gui.native_map_renderer import NativeMapRenderer

    map_window = PersistentMapWindow(map_host, _Settings(), NativeMapRenderer)
    try:
        main_before = _snapshot(map_host)
        map_before = _snapshot(map_window)
        normal_before = QRect(map_window._normal_geometry)
        size_hint_before = QSize(map_window.sizeHint())
        minimum_size_hint_before = QSize(map_window.minimumSizeHint())

        map_window.present()
        app.processEvents()

        renderer = map_window.map_tab
        assert isinstance(renderer, NativeMapRenderer)
        assert renderer.window() is map_window
        assert map_window.centralWidget() is not renderer
        assert _snapshot(map_host) == main_before
        assert _snapshot(map_window) == map_before
        assert map_window._normal_geometry == normal_before
        assert map_window.sizeHint() == size_hint_before
        assert map_window.minimumSizeHint() == minimum_size_hint_before
    finally:
        map_window.shutdown()
        map_window.deleteLater()
        app.processEvents()


def test_native_renderer_visibility_projection_singleton_and_shutdown_are_idempotent(
    map_host: QMainWindow,
) -> None:
    """Warm re-entry reuses one renderer and teardown cannot revive native work."""
    app = _app()
    from freqinout.gui.native_map_renderer import NativeMapRenderer

    created: list[NativeMapRenderer] = []

    def factory(parent: QWidget) -> NativeMapRenderer:
        renderer = NativeMapRenderer(parent)
        created.append(renderer)
        return renderer

    map_window = PersistentMapWindow(map_host, _Settings(), factory)
    try:
        map_window.present()
        app.processEvents()
        first = map_window.map_tab
        assert first is created[0]
        assert isinstance(first, NativeMapRenderer)

        first.apply_projection(
            {
                "markers": [{"id": "station-1", "latitude": 39.7, "longitude": -104.9}],
                "links": [{"origin": "station-1", "destination": "station-2"}],
            }
        )
        map_window.close()
        app.processEvents()
        map_window.present()
        app.processEvents()

        assert map_window.map_tab is first
        assert created == [first]
        first.shutdown()
        first.shutdown()
        assert first.is_available() is False
        first.set_map_visible(True)
        first.apply_projection({"markers": [], "links": []})
    finally:
        map_window.shutdown()
        map_window.deleteLater()
        app.processEvents()


def test_native_renderer_exposes_direct_action_bridge_without_browser_title_transport() -> None:
    """Map actions are Qt signals, not page-title or JavaScript URL side channels."""
    source = Path("freqinout/gui/native_map_renderer.py").read_text(encoding="utf-8")

    assert "action_requested = Signal(object)" in source
    assert "titleChanged" not in source
    assert "runJavaScript" not in source
