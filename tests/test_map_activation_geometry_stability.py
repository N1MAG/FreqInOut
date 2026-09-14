from __future__ import annotations

import os
from types import SimpleNamespace
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QTimer, Qt, Signal
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
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


class _MapPage:
    def __init__(self) -> None:
        self.scripts: list[str] = []

    def runJavaScript(self, script: str, callback) -> None:  # noqa: N802 - Qt-compatible name
        self.scripts.append(script)
        callback(True)


class _FakeWebEngineView(QWidget):
    """Small QWidget substitute that preserves WebEngine's lifecycle signals."""

    loadFinished = Signal(bool)
    titleChanged = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._page = _MapPage()
        self.set_page_calls: list[object] = []
        self.urls: list[object] = []
        self.html: list[str] = []

    def page(self):
        return self._page

    def setPage(self, page) -> None:  # noqa: N802 - Qt-compatible name
        self.set_page_calls.append(page)
        self._page = page

    def setUrl(self, url) -> None:  # noqa: N802 - Qt-compatible name
        self.urls.append(url)

    def setHtml(self, html: str) -> None:  # noqa: N802 - Qt-compatible name
        self.html.append(html)


def _map_load_host(stack: QStackedWidget, web: QWidget) -> SimpleNamespace:
    return SimpleNamespace(
        _map_page_loading=True,
        _map_initialized=False,
        _map_load_ok=False,
        _map_js_ready_retry_count=0,
        _map_stack=stack,
        _map_loading_label=None,
        web=web,
        _map_visible=True,
        _map_dirty=False,
        _render_requested_during_load=False,
        _render_requested_during_load_level=0,
        _pending_map_payload=None,
        _emit_map_event=lambda *_args, **_kwargs: None,
        _set_map_runtime_state=lambda *_args, **_kwargs: None,
        _map_ready_detail_text=lambda: "ready",
        _schedule_leaflet_viewport_settle=lambda: None,
        _maybe_start_map_ingest=lambda: False,
    )


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


def test_map_load_finished_promotes_direct_webview_without_overlay_handoff() -> None:
    """A successful direct-view load immediately owns the visible stack page."""
    app = _app()
    window = QMainWindow()
    stack = QStackedWidget(window)
    loading = QWidget(stack)
    web = _FakeWebEngineView(stack)
    stack.addWidget(loading)
    stack.addWidget(web)
    window.setCentralWidget(stack)
    window.resize(900, 560)
    stack.setCurrentWidget(loading)
    window.show()
    app.processEvents()

    host = _map_load_host(stack, web)

    StationsMapTab._on_map_load_finished(host, True)
    app.processEvents()

    assert host._map_load_ok is True
    assert stack.currentWidget() is web
    assert web.isVisible()

    window.close()
    window.deleteLater()
    app.processEvents()


def test_map_navigation_targets_visible_webview_directly_without_detached_page(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Cold file navigation must use the one WebEngineView owned by the stack."""
    app = _app()
    monkeypatch.setattr(stations_map_module, "QWebEngineView", _FakeWebEngineView)
    window = QMainWindow()
    stack = QStackedWidget(window)
    loading = QWidget(stack)
    stack.addWidget(loading)
    window.setCentralWidget(stack)
    window.resize(900, 560)
    window.show()
    app.processEvents()

    host = SimpleNamespace(
        _map_stack=stack,
        web=None,
        _map_loading_label=None,
        _map_runtime_detail="Loading map...",
        _map_page_loading=False,
        _map_load_ok=False,
        _set_map_runtime_state=lambda *_args, **_kwargs: None,
        _emit_map_event=lambda *_args, **_kwargs: None,
        _enter_map_degraded=lambda *_args, **_kwargs: None,
        _on_map_load_finished=lambda _ok: None,
        _on_map_page_title_changed=lambda _title: None,
    )

    assert StationsMapTab._ensure_web_view(host) is True
    web = host.web
    assert web is not None
    assert web.parentWidget() is stack
    assert web.page() is not None
    assert web.set_page_calls == []

    map_file = tmp_path / "map.html"
    map_file.write_text("<html></html>", encoding="utf-8")
    assert StationsMapTab._load_web_map_file(host, map_file) is True

    assert host.web is web
    assert len(web.urls) == 1
    assert web.html == []
    assert web.set_page_calls == []

    window.close()
    window.deleteLater()
    app.processEvents()


def test_map_lifecycle_source_has_direct_navigation_and_no_detached_page_path() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "freqinout/gui/stations_map_tab.py"
    ).read_text(encoding="utf-8")

    assert "self.web.setUrl(url)" in source
    assert "self.web.setHtml(html)" in source
    assert "_map_loading_overlay" not in source
    assert "_map_detached_page" not in source
    assert "_map_navigation_target" not in source

    direct_load_source = source.split("    def _load_web_map_file", 1)[1].split(
        "    def _on_map_page_title_changed", 1
    )[0]
    assert "setPage(" not in direct_load_source
    assert "QWebEnginePage" not in direct_load_source
    ensure_source = direct_load_source.split("    def _ensure_web_view", 1)[1]
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


def test_warm_map_reload_reuses_existing_visible_web_surface() -> None:
    """A warm reload must reuse the current view without a blanking handoff."""
    app = _app()
    window = QMainWindow()
    stack = QStackedWidget(window)
    loading = QWidget(stack)
    web = _FakeWebEngineView(stack)
    stack.addWidget(loading)
    stack.addWidget(web)
    window.setCentralWidget(stack)
    window.resize(900, 560)
    stack.setCurrentWidget(web)
    window.show()
    app.processEvents()

    events: list[str] = []
    host = SimpleNamespace(
        _map_stack=stack,
        web=web,
        _map_page_loading=True,
        _map_initialized=False,
        _map_load_ok=False,
        _map_js_ready_retry_count=0,
        _map_runtime_detail="Loading map...",
        _map_visible=True,
        _app_active=True,
        _is_shutting_down=False,
        _pending_map_payload=None,
        _map_dirty=False,
        _render_requested_during_load=False,
        _render_requested_during_load_level=0,
        _emit_map_event=lambda event, **_kwargs: events.append(str(event)),
        _set_map_runtime_state=lambda *_args, **_kwargs: None,
        _enter_map_degraded=lambda *_args, **_kwargs: None,
        _map_ready_detail_text=lambda: "ready",
        _schedule_leaflet_viewport_settle=lambda: None,
        _maybe_start_map_ingest=lambda: False,
    )

    before_view = host.web
    assert web.isVisible()
    StationsMapTab._load_map_html_into_webview(host, "<html>reload</html>")
    StationsMapTab._on_map_load_finished(host, True)
    app.processEvents()

    assert host.web is before_view
    assert stack.currentWidget() is web
    assert web.isVisible()
    assert web.html == ["<html>reload</html>"]
    assert host._map_load_ok is True
    assert events == ["page_load_started", "page_load_finished"]

    window.close()
    window.deleteLater()
    app.processEvents()


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
    monkeypatch.setattr(stations_map_module, "QWebEngineView", _FakeWebEngineView)
    window = QMainWindow()
    stack = QStackedWidget(window)
    loading = QWidget(stack)
    stack.addWidget(loading)
    window.setCentralWidget(stack)
    window.resize(900, 560)
    host = SimpleNamespace(
        _map_stack=stack,
        web=None,
        _map_loading_label=None,
        _on_map_load_finished=lambda _ok: None,
        _on_map_page_title_changed=lambda _title: None,
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

    assert StationsMapTab._ensure_web_view(host) is True
    app.processEvents()

    assert window.geometry() == before_geometry
    assert window.windowState() == before_state
    assert window.screen() is before_screen
    assert host.web is not None
    assert host.web.parentWidget() is host._map_stack
    assert host.web.geometry().width() > 1
    assert host.web.geometry().height() > 1
    assert host.web.set_page_calls == []

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
