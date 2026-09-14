from __future__ import annotations

import os
from types import SimpleNamespace
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QTimer, Signal
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

import freqinout.gui.stations_map_tab as stations_map_module
from freqinout.gui.stations_map_tab import StationsMapTab
from freqinout.gui.theme import get_theme


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _CountingGrid(QGridLayout):
    def __init__(self, parent: QWidget) -> None:
        super().__init__(parent)
        self.take_count = 0

    def takeAt(self, index: int):  # noqa: N802 - Qt virtual method name
        self.take_count += 1
        return super().takeAt(index)


class _Splitter:
    def __init__(self, width: int = 1200) -> None:
        self._width = width
        self.size_writes: list[list[int]] = []

    def width(self) -> int:
        return self._width

    def sizes(self) -> list[int]:
        return [self._width, 0]

    def setSizes(self, sizes: list[int]) -> None:  # noqa: N802 - Qt-compatible name
        self.size_writes.append(list(sizes))


class _ControlsScroll:
    def setMinimumWidth(self, _width: int) -> None:  # noqa: N802
        pass

    def setMaximumWidth(self, _width: int) -> None:  # noqa: N802
        pass

    def setVisible(self, _visible: bool) -> None:  # noqa: N802
        pass


class _Button:
    def setText(self, _text: str) -> None:  # noqa: N802
        pass

    def setVisible(self, _visible: bool) -> None:  # noqa: N802
        pass


class _FakeNativeMapRenderer(QWidget):
    """Small native surface stand-in used for ownership and geometry tests."""

    action_requested = Signal(object)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.projections: list[dict[str, object]] = []
        self.visibility: list[bool] = []

    def apply_projection(self, payload: dict[str, object]) -> None:
        self.projections.append(dict(payload))

    def set_map_visible(self, visible: bool) -> None:
        self.visibility.append(bool(visible))


def _filter_reflow_host() -> tuple[SimpleNamespace, _CountingGrid, QFrame]:
    _app()
    bar = QFrame()
    grid = _CountingGrid(bar)
    grid.setHorizontalSpacing(10)
    fields = tuple(QWidget(bar) for _ in range(7))
    for field in fields:
        field.setMinimumWidth(280)
    host = SimpleNamespace(
        _map_filter_bar=bar,
        _map_filter_grid=grid,
        _map_filter_fields=fields,
        _map_search_field=QWidget(bar),
        _map_clear_filters_button=QPushButton("Clear Filters", bar),
        _map_clear_layers_button=QPushButton("Clear Layers", bar),
        _now_reachable_label=QLabel("", bar),
        _map_filter_columns=None,
        width=lambda: bar.width(),
    )
    return host, grid, bar


def test_map_filter_reflow_defers_zero_width_and_is_idempotent_at_final_width() -> None:
    host, grid, bar = _filter_reflow_host()

    bar.resize(0, 200)
    StationsMapTab._reflow_map_filter_bar(host)

    assert host._map_filter_columns is None
    assert grid.count() == 0
    assert grid.take_count == 0

    bar.resize(900, 200)
    StationsMapTab._reflow_map_filter_bar(host)
    first_item_ids = tuple(id(grid.itemAt(index)) for index in range(grid.count()))
    first_take_count = grid.take_count

    assert host._map_filter_columns == 2
    assert grid.count() == 11

    StationsMapTab._reflow_map_filter_bar(host)

    assert grid.take_count == first_take_count
    assert tuple(id(grid.itemAt(index)) for index in range(grid.count())) == first_item_ids


def test_map_geometry_reflow_timer_coalesces_a_resize_storm() -> None:
    app = _app()
    runs: list[str] = []
    timer = QTimer()
    timer.setSingleShot(True)
    timer.timeout.connect(lambda: runs.append("flush"))
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_geometry_timer=timer,
        _flush_map_geometry_reflow=lambda: runs.append("fallback"),
    )

    StationsMapTab._schedule_map_geometry_reflow(host)
    StationsMapTab._schedule_map_geometry_reflow(host)
    StationsMapTab._schedule_map_geometry_reflow(host)
    app.processEvents()

    assert runs == ["flush"]


def test_map_drawer_reflow_does_not_rewrite_splitter_for_unchanged_state() -> None:
    splitter = _Splitter()
    host = SimpleNamespace(
        _main_splitter=splitter,
        _controls_drawer_open=False,
        _applied_controls_drawer_open=None,
        _controls_scroll=_ControlsScroll(),
        _controls_button=_Button(),
        width=lambda: 1200,
        _update_splitter_indicator_state=lambda: None,
        _position_splitter_indicator=lambda: None,
    )

    StationsMapTab._set_controls_drawer_open(host, False)
    StationsMapTab._set_controls_drawer_open(host, False)

    assert splitter.size_writes == [[0, 1200]]


def test_native_map_surface_is_constructed_once_in_the_existing_map_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Map owns one Qt Quick child rather than a browser loading handoff."""
    _app()
    monkeypatch.setattr(stations_map_module, "NativeMapRenderer", _FakeNativeMapRenderer)
    stack = QStackedWidget()
    loading = QLabel("Preparing native map", stack)
    stack.addWidget(loading)
    host = SimpleNamespace(
        _map_stack=stack,
        _map_loading_label=loading,
        _native_map_renderer=None,
        _map_visible=True,
        _app_active=True,
        _is_shutting_down=False,
        _on_native_map_action=lambda _payload: None,
    )

    first = StationsMapTab._ensure_native_map_renderer(host)
    second = StationsMapTab._ensure_native_map_renderer(host)

    assert first is second is host._native_map_renderer
    assert first.parentWidget() is stack
    assert stack.currentWidget() is first
    assert first.sizePolicy().horizontalPolicy() is QSizePolicy.Ignored
    assert first.sizePolicy().verticalPolicy() is QSizePolicy.Ignored
    stack.deleteLater()
    _app().processEvents()


def test_map_lifecycle_source_has_one_native_surface_and_no_browser_map_path() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "freqinout/gui/stations_map_tab.py"
    ).read_text(encoding="utf-8")

    assert "from freqinout.gui.native_map_renderer import NativeMapRenderer" in source
    assert "QWebEngine" not in source
    assert "runJavaScript" not in source
    assert "_ensure_webengine_imported" not in source
    assert "def _ensure_native_map_renderer" in source
    ensure_start = source.index("def _ensure_native_map_renderer")
    ensure_source = source[ensure_start : source.index("def _on_native_map_action", ensure_start)]
    for mutation in (
        "setGeometry(",
        ".resize(",
        ".move(",
        "showMaximized(",
        "showFullScreen(",
        "setWindowState(",
    ):
        assert mutation not in ensure_source


def _window_state_snapshot(window: QMainWindow) -> tuple:
    geometry = window.geometry()
    screen = window.screen()
    return (
        int(geometry.x()),
        int(geometry.y()),
        int(geometry.width()),
        int(geometry.height()),
        window.windowState(),
        bool(window.isFullScreen()),
        bool(window.isMaximized()),
        screen.name() if screen is not None else None,
    )


def test_warm_native_map_update_reuses_existing_visible_surface() -> None:
    """Projection updates retain one visible native surface without a blank handoff."""
    renderer = _FakeNativeMapRenderer()
    host = SimpleNamespace(
        _native_map_renderer=renderer,
        _map_visible=True,
        _app_active=True,
        _is_shutting_down=False,
        _pending_map_payload=None,
        _last_map_payload_sig=None,
    )

    StationsMapTab._apply_native_map_projection(host, {"markers": [], "links": []})
    StationsMapTab._apply_native_map_projection(host, {"markers": [{"id": "one", "lat": 39.7, "lon": -104.9}], "links": []})

    assert host._native_map_renderer is renderer
    assert len(renderer.projections) == 2


@pytest.mark.parametrize(
    "window_mode",
    ("normal", "maximized", "fullscreen"),
)
def test_first_map_native_surface_preserves_window_geometry_state_and_screen(
    monkeypatch: pytest.MonkeyPatch,
    window_mode: str,
) -> None:
    """Creating the lazy native child must not move or normalize the shell."""
    app = _app()
    monkeypatch.setattr(stations_map_module, "NativeMapRenderer", _FakeNativeMapRenderer)
    window = QMainWindow()
    stack = QStackedWidget(window)
    loading = QWidget(stack)
    stack.addWidget(loading)
    window.setCentralWidget(stack)
    window.resize(900, 560)
    host = SimpleNamespace(
        _map_stack=stack,
        _map_loading_label=None,
        _native_map_renderer=None,
        _map_visible=True,
        _app_active=True,
        _is_shutting_down=False,
        _on_native_map_action=lambda _payload: None,
    )

    window.setGeometry(37, 59, 900, 560)
    window.show()
    app.processEvents()
    if window_mode == "maximized":
        window.showMaximized()
    elif window_mode == "fullscreen":
        window.showFullScreen()
    else:
        window.showNormal()
    app.processEvents()

    before_geometry = window.geometry()
    before_state = window.windowState()
    before_screen = window.screen()
    assert before_screen is not None

    assert StationsMapTab._ensure_native_map_renderer(host) is host._native_map_renderer
    app.processEvents()

    assert window.geometry() == before_geometry
    assert window.windowState() == before_state
    assert window.screen() is before_screen
    assert host._native_map_renderer is not None
    assert host._native_map_renderer.parentWidget() is host._map_stack
    assert host._native_map_renderer.geometry().width() > 1
    assert host._native_map_renderer.geometry().height() > 1

    window.close()
    window.deleteLater()
    app.processEvents()


def test_main_shell_ignores_active_map_size_hint_to_preserve_window_geometry() -> None:
    """A lazy Map's native view must not resize or move the top-level shell."""
    source = (
        Path(__file__).resolve().parents[1] / "freqinout/gui/main_window.py"
    ).read_text(encoding="utf-8")

    assert "self.stack.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Ignored)" in source


def test_live_map_refresh_keeps_support_strip_compact() -> None:
    _app()
    card = QFrame()
    support_layout = QHBoxLayout(card)
    label = QLabel(card)
    retry = QPushButton("Retry", card)
    reload_data = QPushButton("Reload Data", card)
    copy = QPushButton("Copy Diagnostics", card)
    help_button = QPushButton("Help", card)
    for widget in (label, retry, reload_data, copy, help_button):
        support_layout.addWidget(widget)
    host = SimpleNamespace(
        _map_support_card=card,
        _map_support_layout=support_layout,
        _map_support_label=label,
        _map_retry_btn=retry,
        _map_reload_btn=reload_data,
        _map_copy_summary_btn=copy,
        _map_support_help_btn=help_button,
        _map_runtime_state="loading",
        _map_runtime_detail="Refreshing station data.",
        _map_initialized=True,
        _map_load_ok=True,
        _map_marker_count=5,
        _map_link_count=2,
        _map_link_status_detail="",
        fontMetrics=card.fontMetrics,
        _theme_snapshot=lambda: get_theme("dark"),
        _map_support_summary=lambda: "diagnostics",
    )

    StationsMapTab._update_map_support_card(host)

    expected_height = max(34, card.fontMetrics().height() + 14)
    assert card.maximumHeight() == expected_height
    assert retry.isHidden()
    assert reload_data.isHidden()
    assert copy.isHidden()
    assert help_button.isHidden()

    host._map_runtime_state = "degraded"
    host._map_load_ok = False
    StationsMapTab._update_map_support_card(host)

    assert card.maximumHeight() == 16777215
    assert not retry.isHidden()
    assert not reload_data.isHidden()
    assert not copy.isHidden()
    assert not help_button.isHidden()
    card.deleteLater()
