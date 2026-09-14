from __future__ import annotations

import os
from types import MethodType, SimpleNamespace
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QSize, QTimer, Qt, Signal
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
from freqinout.gui.current_page_stack import CurrentPageStack
from freqinout.gui.main_window import MainWindow
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


class _ViewportTimer:
    def __init__(self) -> None:
        self.starts: list[int] = []

    def start(self, delay: int) -> None:
        self.starts.append(int(delay))


class _LifecycleTimer:
    def __init__(self, active: bool = False) -> None:
        self.active = active
        self.start_count = 0
        self.stop_count = 0

    def isActive(self) -> bool:  # noqa: N802
        return self.active

    def start(self) -> None:
        self.active = True
        self.start_count += 1

    def stop(self) -> None:
        self.active = False
        self.stop_count += 1


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


class _TestSignal:
    def __init__(self) -> None:
        self._callbacks: list[object] = []

    def connect(self, callback) -> None:
        self._callbacks.append(callback)


class _DetachedMapPage:
    """QWebEnginePage stand-in that records detached navigation and probes."""

    def __init__(self, parent=None) -> None:
        self.parent = parent
        self.loadFinished = _TestSignal()
        self.titleChanged = _TestSignal()
        self.urls: list[object] = []
        self.html: list[str] = []
        self.scripts: list[str] = []
        self.probe_callbacks: list[object] = []

    def deleteLater(self) -> None:  # noqa: N802 - Qt-compatible name
        pass

    def setUrl(self, url) -> None:  # noqa: N802 - Qt-compatible name
        self.urls.append(url)

    def setHtml(self, html: str) -> None:  # noqa: N802 - Qt-compatible name
        self.html.append(html)

    def runJavaScript(self, script: str, callback=None) -> None:  # noqa: N802
        self.scripts.append(script)
        if callback is not None:
            self.probe_callbacks.append(callback)


class _MapWebView:
    def __init__(self, width: int, height: int) -> None:
        self._size = QSize(width, height)
        self._page = _MapPage()

    def size(self) -> QSize:
        return self._size

    def page(self) -> _MapPage:
        return self._page


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


def _map_surface_window() -> tuple[QMainWindow, SimpleNamespace]:
    window = QMainWindow()
    central = QWidget(window)
    central_layout = QVBoxLayout(central)
    pages = CurrentPageStack(central)
    inbox = QLabel("Inbox", pages)
    map_page = QWidget(pages)
    map_layout = QVBoxLayout(map_page)
    canvas = QSplitter(Qt.Horizontal, map_page)
    map_stack = QStackedWidget(canvas)
    loading = QWidget(map_stack)
    map_stack.addWidget(loading)
    canvas.addWidget(map_stack)
    map_layout.addWidget(canvas)
    pages.addWidget(inbox)
    pages.addWidget(map_page)
    pages.setCurrentWidget(map_page)
    pages.setGeometry(0, 0, 900, 560)
    central_layout.addWidget(pages)
    window.setCentralWidget(central)
    window.setMinimumSize(0, 0)
    window.resize(900, 560)

    host = SimpleNamespace(
        _map_stack=map_stack,
        _map_canvas_splitter=canvas,
        _map_loading_overlay=None,
        _map_loading_overlay_label=None,
        _map_loading_label=None,
        _map_surface_ready=False,
        _map_surface_payload_applied=False,
        _map_surface_prepare_retry_count=0,
        _map_surface_reveal_retry_count=0,
        _map_surface_reveal_generation=0,
        _map_surface_reveal_probe_pending=False,
        _map_surface_presentation_started_at=0.0,
        _pending_map_payload=None,
        _map_initialized=False,
        _map_load_ok=False,
        _map_visible=True,
        _app_active=True,
        _is_shutting_down=False,
        _map_runtime_detail="Loading map...",
        web=None,
        _set_map_runtime_state=lambda *_args, **_kwargs: None,
        _map_ready_detail_text=lambda: "ready",
        _theme_snapshot=lambda: get_theme("dark"),
        _on_map_load_finished=lambda _ok: None,
        _on_map_page_title_changed=lambda _title: None,
        _emit_map_event=lambda *_args, **_kwargs: None,
        _schedule_leaflet_viewport_settle=lambda: None,
        _maybe_start_map_ingest=lambda: False,
        window=lambda: window,
    )
    host._show_map_loading_overlay = MethodType(
        StationsMapTab._show_map_loading_overlay,
        host,
    )
    host._present_loaded_map_surface = MethodType(
        StationsMapTab._present_loaded_map_surface,
        host,
    )
    host._begin_map_surface_geometry_settle = MethodType(
        StationsMapTab._begin_map_surface_geometry_settle,
        host,
    )
    host._schedule_map_surface_reveal = MethodType(
        StationsMapTab._schedule_map_surface_reveal,
        host,
    )
    host._sync_map_loading_overlay_geometry = MethodType(
        StationsMapTab._sync_map_loading_overlay_geometry,
        host,
    )
    host._finish_map_surface_reveal = MethodType(StationsMapTab._finish_map_surface_reveal, host)
    host._on_map_surface_reveal_timeout = MethodType(
        StationsMapTab._on_map_surface_reveal_timeout,
        host,
    )
    host._map_script_page = MethodType(StationsMapTab._map_script_page, host)
    host._map_navigation_target = MethodType(StationsMapTab._map_navigation_target, host)
    host._enter_map_degraded = lambda *_args, **_kwargs: None
    return window, host


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


def test_successful_map_load_queues_leaflet_viewport_settlement() -> None:
    scheduled: list[str] = []
    host = SimpleNamespace(
        _map_page_loading=True,
        _map_initialized=False,
        _map_load_ok=False,
        _map_js_ready_retry_count=4,
        _map_stack=None,
        web=object(),
        _pending_map_payload=None,
        _map_visible=False,
        _map_dirty=False,
        _render_requested_during_load=False,
        _render_requested_during_load_level=0,
        _emit_map_event=lambda *_args, **_kwargs: None,
        _set_map_runtime_state=lambda *_args, **_kwargs: None,
        _map_ready_detail_text=lambda: "ready",
        _schedule_leaflet_viewport_settle=lambda: scheduled.append("settle"),
        _maybe_start_map_ingest=lambda: False,
    )

    StationsMapTab._on_map_load_finished(host, True)

    assert host._map_page_loading is False
    assert host._map_initialized is True
    assert host._map_load_ok is True
    assert scheduled == ["settle"]


def test_map_page_stays_on_loading_surface_until_canvas_geometry_is_nonzero() -> None:
    """A cold load must not reveal a native view at the canvas fallback origin."""
    app = _app()
    shell = QWidget()
    splitter = QSplitter(Qt.Horizontal, shell)
    stack = QStackedWidget(splitter)
    loading = QWidget(stack)
    stack.addWidget(loading)
    splitter.addWidget(stack)
    shell.resize(900, 560)
    splitter.setGeometry(0, 0, 0, 0)
    stack.setGeometry(0, 0, 0, 0)
    web = _FakeWebEngineView(stack)
    stack.addWidget(web)
    stack.setCurrentIndex(0)
    host = _map_load_host(stack, web)

    shell.show()
    app.processEvents()
    StationsMapTab._on_map_load_finished(host, True)

    # loadFinished may arrive while Qt is still attaching/layouting the native
    # child.  The loading surface must remain current in that interval.
    assert stack.currentIndex() == 0
    assert not web.isVisible()

    splitter.setGeometry(0, 0, 900, 560)
    stack.setGeometry(0, 0, 900, 560)
    StationsMapTab._on_map_load_finished(host, True)
    app.processEvents()

    assert stack.currentIndex() == 1
    assert web.parentWidget() is stack
    assert web.isVisible()
    assert web.width() > 0
    assert web.height() > 0

    shell.close()
    shell.deleteLater()
    app.processEvents()


def test_first_map_webview_preparation_waits_for_final_canvas_geometry() -> None:
    """Lazy construction/switching must not place a native view at (0, 0)."""
    calls: list[str] = []

    class _Canvas:
        def __init__(self) -> None:
            self._size = QSize(0, 0)

        def size(self) -> QSize:
            return self._size

    canvas = _Canvas()
    host = SimpleNamespace(
        _app_active=True,
        _map_visible=True,
        _map_canvas_splitter=canvas,
        _map_dirty=False,
        _emit_map_event=lambda *_args, **_kwargs: None,
        _ensure_web_view=lambda: calls.append("ensure") or True,
    )

    assert StationsMapTab.prepare_webview_for_first_show(host) is False
    assert calls == []

    canvas._size = QSize(900, 560)
    assert StationsMapTab.prepare_webview_for_first_show(host) is True
    assert calls == ["ensure"]


def test_map_surface_geometry_quiescence_requires_stable_samples_and_elapsed_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A changed signature restarts settling; two samples still need 150 ms."""
    now = [10.0]
    monkeypatch.setattr(stations_map_module.time, "monotonic", lambda: now[0])
    signature = [(0, 0, 900, 560)]
    events: list[str] = []
    host = SimpleNamespace(
        _map_surface_geometry_phase="",
        _map_surface_geometry_signature_value=None,
        _map_surface_geometry_changed_at=0.0,
        _map_surface_geometry_stable_samples=0,
        _map_surface_geometry_signature=lambda: signature[0],
        _emit_map_event=lambda event, **_kwargs: events.append(str(event)),
    )
    host._begin_map_surface_geometry_settle = MethodType(
        StationsMapTab._begin_map_surface_geometry_settle,
        host,
    )

    assert StationsMapTab._map_surface_geometry_is_quiet(host, "precreate") is False
    now[0] += 0.10
    assert StationsMapTab._map_surface_geometry_is_quiet(host, "precreate") is False
    now[0] += 0.049
    assert StationsMapTab._map_surface_geometry_is_quiet(host, "precreate") is False
    now[0] += 0.002
    assert StationsMapTab._map_surface_geometry_is_quiet(host, "precreate") is True

    signature[0] = (0, 0, 901, 560)
    assert StationsMapTab._map_surface_geometry_is_quiet(host, "precreate") is False
    assert host._map_surface_geometry_stable_samples == 0
    assert events == ["surface_geometry_changed", "surface_geometry_changed"]


def test_cold_map_activation_waits_for_precreate_and_post_attach_barriers() -> None:
    """No Chromium load/render is allowed until both geometry barriers pass."""
    events: list[str] = []
    quiet_results = iter((False, True, False, True))
    host = SimpleNamespace(
        _map_visible=True,
        _is_shutting_down=False,
        _app_active=True,
        web=None,
        _map_surface_prepare_retry_count=0,
        _map_page_loading=False,
        _map_initialized=False,
        _map_dirty=False,
        _map_canvas_geometry_ready=lambda: True,
        _map_surface_geometry_is_quiet=lambda phase: events.append(f"quiet:{phase}") or next(quiet_results),
        _ensure_initial_data_loaded=lambda: events.append("data"),
        prepare_webview_for_first_show=lambda: events.append("prepare") or True,
        _schedule_map_surface_prepare=lambda delay: events.append(f"retry:{delay}"),
        _begin_map_surface_geometry_settle=lambda phase: events.append(f"begin:{phase}"),
        _request_map_refresh=lambda **_kwargs: events.append("render"),
    )

    # precreate geometry is moving: do not construct, load, or render.
    StationsMapTab._on_map_visible_deferred(host)
    assert events == ["quiet:precreate", "retry:75"]

    # precreate geometry is quiet: construct once, then restart settling after
    # the native child is attached; Chromium still must not load yet.
    host.prepare_webview_for_first_show = lambda: events.append("prepare") or setattr(host, "web", object()) or True
    StationsMapTab._on_map_visible_deferred(host)
    assert events == ["quiet:precreate", "retry:75", "quiet:precreate", "data", "prepare", "begin:attached", "retry:75"]
    assert "render" not in events

    # Native attachment changed the signature: defer the load until it settles.
    StationsMapTab._on_map_visible_deferred(host)
    assert events[-2:] == ["quiet:attached", "retry:75"]
    assert "render" not in events

    # Only after the post-attach barrier may the initial map HTML/data load run.
    StationsMapTab._on_map_visible_deferred(host)
    assert events[-3:] == ["data", "prepare", "render"]


def test_map_resize_invalidates_pending_geometry_quiescence() -> None:
    """A resize restarts settling and schedules another bounded prepare pass."""
    timer = QTimer()
    timer.setSingleShot(True)
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_surface_ready=False,
        _map_surface_geometry_signature_value=(1, 2, 900, 560),
        _map_surface_geometry_changed_at=4.0,
        _map_surface_geometry_stable_samples=3,
        _map_surface_prepare_timer=timer,
        _map_visible=True,
        _app_active=True,
        _schedule_map_surface_reveal=lambda: None,
        _flush_map_geometry_reflow=lambda: None,
    )
    host._invalidate_map_surface_geometry_settle = MethodType(
        StationsMapTab._invalidate_map_surface_geometry_settle,
        host,
    )

    StationsMapTab._schedule_map_geometry_reflow(host)

    assert host._map_surface_geometry_signature_value is None
    assert host._map_surface_geometry_stable_samples == 0
    assert timer.isActive()
    assert timer.interval() == 75
    timer.stop()


def test_map_first_visible_layout_settles_before_visibility_can_queue_native_view() -> None:
    """Map activation must settle its parent canvas before set_map_visible()."""
    _app()
    stack = CurrentPageStack()
    inbox = QLabel("Inbox")
    events: list[tuple[str, QSize]] = []

    class _MapPage(QWidget):
        def on_first_visible_layout_ready(self) -> None:
            events.append(("settled", self.size()))

        def set_map_visible(self, visible: bool) -> None:
            if visible:
                events.append(("visible", self.size()))

    map_page = _MapPage()
    stack.addWidget(inbox)
    stack.addWidget(map_page)
    stack.setGeometry(0, 0, 900, 560)
    stack.setCurrentWidget(inbox)

    shell = SimpleNamespace(
        settings={},
        _shutting_down=False,
        stack=stack,
        _screens=[("Inbox", inbox), ("Map", map_page)],
        _lazy_factories={},
        _lazy_placeholders={},
        _active_tab_index=None,
        _navigation_epoch=0,
        _pending_map_switch_index=None,
        _help_dialog_settle_until=0.0,
        _screen_is_runtime_suppressed=lambda _label: False,
        _queue_map_switch_after_webengine_warmup=lambda _index: False,
        _nav_screen_index_map={},
        _suppress_initial_nav_group_auto_expand=True,
        _expand_nav_group_for_screen=lambda _label: None,
        nav_buttons=[],
        _sync_compact_navigation_selection=lambda _label: None,
        _update_ncs_nav_button_styles=lambda: None,
        _schedule_status_refresh=lambda: None,
        _ensure_lazy_tab_loaded=lambda _label, _index: None,
        _update_map_filters_visibility=lambda index: map_page.set_map_visible(
            shell._screens[index][0] == "Map"
        ),
    )
    shell._settle_active_screen_layout = MethodType(MainWindow._settle_active_screen_layout, shell)

    MainWindow._set_screen(shell, 1)

    assert [kind for kind, _size in events] == ["settled", "visible"]
    assert all(size.width() > 1 and size.height() > 1 for _kind, size in events)

    stack.deleteLater()


def test_map_surface_reveal_requires_load_success_geometry_and_payload() -> None:
    """The opaque loading surface stays up until every readiness condition holds."""
    events: list[str] = []

    class _Geometry:
        def __init__(self) -> None:
            self._size = QSize(0, 0)

        def size(self) -> QSize:
            return self._size

        def width(self) -> int:
            return int(self._size.width())

        def height(self) -> int:
            return int(self._size.height())

        def setFocusPolicy(self, _policy) -> None:  # noqa: N802 - Qt-compatible name
            pass

    class _Overlay:
        def __init__(self) -> None:
            self.hidden = False

        def hide(self) -> None:
            self.hidden = True

    canvas = _Geometry()
    stack = _Geometry()
    web = _Geometry()
    web._size = QSize(900, 560)
    overlay = _Overlay()
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_visible=True,
        _app_active=True,
        _map_initialized=False,
        _map_load_ok=False,
        _map_surface_payload_applied=False,
        _map_surface_ready=False,
        _map_surface_reveal_retry_count=0,
        _map_canvas_splitter=canvas,
        _map_stack=stack,
        _map_loading_overlay=overlay,
        web=web,
        _schedule_map_surface_reveal=lambda: events.append("retry"),
        _sync_map_loading_overlay_geometry=lambda: True,
        _emit_map_event=lambda event, **_kwargs: events.append(str(event)),
        _schedule_leaflet_viewport_settle=lambda: None,
    )

    StationsMapTab._finish_map_surface_reveal(host)
    assert host._map_surface_ready is False
    assert overlay.hidden is False
    assert events == ["retry"]

    host._map_initialized = True
    StationsMapTab._finish_map_surface_reveal(host)
    assert host._map_surface_ready is False
    host._map_load_ok = True
    StationsMapTab._finish_map_surface_reveal(host)
    assert host._map_surface_ready is False
    host._map_surface_payload_applied = True
    StationsMapTab._finish_map_surface_reveal(host)
    assert host._map_surface_ready is False

    canvas._size = QSize(900, 560)
    stack._size = QSize(900, 560)
    StationsMapTab._finish_map_surface_reveal(host)

    assert host._map_surface_ready is True
    assert overlay.hidden is True
    assert events[-1] == "surface_revealed"


def test_macos_cold_map_load_keeps_webengine_page_noncurrent_behind_opaque_overlay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """macOS cold load keeps the final native page out of the current stack page."""
    monkeypatch.setattr(stations_map_module.sys, "platform", "darwin")
    app = _app()
    shell = QWidget()
    stack = QStackedWidget(shell)
    loading = QWidget(stack)
    web = QWidget(stack)
    stack.addWidget(loading)
    stack.addWidget(web)
    stack.setGeometry(0, 0, 900, 560)
    shell.resize(900, 560)
    host = SimpleNamespace(
        _map_stack=stack,
        _map_loading_overlay=None,
        _map_loading_overlay_label=None,
        _map_runtime_detail="Loading map...",
        _map_first_load_isolated=True,
        _map_surface_presented=False,
        _theme_snapshot=lambda: get_theme("dark"),
    )
    host._sync_map_loading_overlay_geometry = MethodType(
        StationsMapTab._sync_map_loading_overlay_geometry,
        host,
    )

    stack.setCurrentWidget(loading)
    shell.show()
    app.processEvents()
    StationsMapTab._show_map_loading_overlay(host, "Loading map...")
    app.processEvents()

    overlay = host._map_loading_overlay
    assert stack.currentWidget() is loading
    assert not web.isVisible()
    assert overlay is not None
    assert overlay.isVisible()
    assert overlay.geometry() == stack.contentsRect()
    assert "background:" in overlay.styleSheet()

    shell.close()
    shell.deleteLater()
    app.processEvents()


def test_macos_cold_navigation_targets_detached_page_not_native_view(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Cold macOS setUrl must never navigate the page owned by the visible view."""
    _app()
    monkeypatch.setattr(stations_map_module.sys, "platform", "darwin")
    monkeypatch.setattr(stations_map_module, "QWebEngineView", _FakeWebEngineView)
    monkeypatch.setattr(stations_map_module, "QWebEnginePage", _DetachedMapPage)
    window, host = _map_surface_window()
    window.show()
    _app().processEvents()

    assert StationsMapTab._ensure_web_view(host) is True
    detached = host._map_detached_page
    assert isinstance(detached, _DetachedMapPage)
    assert host.web.page() is not detached

    map_file = tmp_path / "map.html"
    map_file.write_text("<html></html>", encoding="utf-8")
    assert StationsMapTab._load_web_map_file(host, map_file) is True

    assert len(detached.urls) == 1
    assert host.web.urls == []
    assert host._map_attached_page is None

    window.close()
    window.deleteLater()
    _app().processEvents()


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


def test_macos_successful_cold_load_does_not_reveal_web_before_final_payload_paint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """loadFinished(True) is not allowed to switch the cold native page current."""
    app = _app()
    monkeypatch.setattr(stations_map_module.sys, "platform", "darwin")
    monkeypatch.setattr(stations_map_module, "QWebEngineView", _FakeWebEngineView)
    monkeypatch.setattr(stations_map_module, "QWebEnginePage", _DetachedMapPage)
    window, host = _map_surface_window()
    host._set_map_runtime_state = lambda *_args, **_kwargs: None
    host._map_ready_detail_text = lambda: "ready"
    host._schedule_leaflet_viewport_settle = lambda: None
    host._maybe_start_map_ingest = lambda: False
    host._pending_map_payload = {"markers": []}
    host._push_pending_map_payload_when_ready = lambda: None
    monkeypatch.setattr(stations_map_module.QTimer, "singleShot", lambda *_args: None)
    window.setGeometry(37, 59, 900, 560)
    window.show()
    app.processEvents()

    before = _window_state_snapshot(window)
    assert StationsMapTab._ensure_web_view(host) is True
    assert host._map_first_load_isolated is True
    assert host._map_stack.currentIndex() == 0
    assert host.web is not None
    assert not host.web.isVisible()

    StationsMapTab._on_map_load_finished(host, True)
    app.processEvents()

    assert host._map_load_ok is True
    assert host._map_surface_ready is False
    assert host._map_stack.currentIndex() == 0
    assert not host.web.isVisible()
    assert _window_state_snapshot(window) == before

    window.close()
    window.deleteLater()
    app.processEvents()


def test_macos_final_payload_paint_presents_web_once_without_top_level_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the final payload/paint handoff may switch the native page current."""
    app = _app()
    monkeypatch.setattr(stations_map_module.sys, "platform", "darwin")
    monkeypatch.setattr(stations_map_module, "QWebEngineView", _FakeWebEngineView)
    monkeypatch.setattr(stations_map_module, "QWebEnginePage", _DetachedMapPage)
    window, host = _map_surface_window()
    host._set_map_runtime_state = lambda *_args, **_kwargs: None
    host._map_ready_detail_text = lambda: "ready"
    host._map_surface_geometry_is_quiet = lambda _phase: True
    host._schedule_leaflet_viewport_settle = lambda: None
    host._maybe_start_map_ingest = lambda: False
    host._pending_map_payload = {"markers": []}
    host._push_pending_map_payload_when_ready = lambda: None
    monkeypatch.setattr(stations_map_module.QTimer, "singleShot", lambda *_args: None)
    window.setGeometry(37, 59, 900, 560)
    window.show()
    app.processEvents()

    assert StationsMapTab._ensure_web_view(host) is True
    StationsMapTab._on_map_load_finished(host, True)
    assert host._map_stack.currentIndex() == 0
    assert not host.web.isVisible()
    detached = host._map_detached_page
    assert isinstance(detached, _DetachedMapPage)
    assert host.web.set_page_calls == []

    current_changes: list[int] = []
    host._map_stack.currentChanged.connect(current_changes.append)
    before = _window_state_snapshot(window)
    host._map_surface_payload_applied = True

    StationsMapTab._schedule_map_surface_reveal(host)
    StationsMapTab._schedule_map_surface_reveal(host)

    assert host._map_stack.currentWidget() is host.web
    assert host.web.isVisible()
    assert current_changes == [1]
    assert host.web.set_page_calls == [detached]
    assert host._map_detached_page is None
    assert host._map_surface_presented is True
    assert _window_state_snapshot(window) == before

    # The paint-complete handoff can run after presentation; it must not switch
    # the stack a second time or alter the top-level shell.
    StationsMapTab._finish_map_surface_reveal(host)
    StationsMapTab._finish_map_surface_reveal(host)
    assert current_changes == [1]
    assert host.web.set_page_calls == [detached]
    assert host._map_surface_ready is True
    assert _window_state_snapshot(window) == before

    window.close()
    window.deleteLater()
    app.processEvents()


def test_stale_prepare_callback_cannot_rewind_present_phase_or_start_prepare() -> None:
    """A late visibility callback must hand off to reveal, never attachment prep."""
    events: list[str] = []
    host = SimpleNamespace(
        _map_visible=True,
        _is_shutting_down=False,
        _app_active=True,
        _map_initialized=True,
        _map_load_ok=True,
        _map_surface_geometry_phase="present",
        _schedule_map_surface_reveal=lambda: events.append("reveal"),
        _schedule_map_surface_prepare=lambda *_args: events.append("prepare"),
        _begin_map_surface_geometry_settle=lambda phase: events.append(f"begin:{phase}"),
    )

    StationsMapTab._on_map_visible_deferred(host)

    assert host._map_surface_geometry_phase == "present"
    assert events == ["reveal"]


def test_stale_reveal_probe_callback_cannot_reveal_new_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old two-frame probe is ignored after resize starts a new generation."""
    page = _DetachedMapPage()
    web = _MapWebView(1200, 700)
    web._page = page
    scheduled: list[str] = []
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_visible=True,
        _app_active=True,
        _map_surface_ready=False,
        _map_first_load_isolated=True,
        _map_surface_presented=True,
        _map_surface_presentation_started_at=0.0,
        _map_initialized=True,
        _map_load_ok=True,
        _map_surface_payload_applied=True,
        _map_surface_reveal_retry_count=0,
        _map_surface_reveal_generation=7,
        _map_surface_reveal_probe_pending=False,
        _map_canvas_splitter=web,
        _map_stack=web,
        web=web,
        _map_surface_geometry_is_quiet=lambda _phase: True,
        _schedule_map_surface_reveal=lambda: scheduled.append("reveal"),
    )
    host._map_script_page = MethodType(StationsMapTab._map_script_page, host)
    monkeypatch.setattr(stations_map_module.QTimer, "singleShot", lambda *_args: None)

    StationsMapTab._schedule_map_surface_reveal(host)
    assert len(page.probe_callbacks) == 1
    callback = page.probe_callbacks[0]

    host._map_surface_reveal_generation = 8
    callback(True)

    assert scheduled == []
    assert host._map_surface_ready is False


def test_map_ready_title_ignores_stale_generation_and_reveals_current_once() -> None:
    """Only the title acknowledgement for the active generation can reveal."""
    window, host = _map_surface_window()
    web = _FakeWebEngineView(host._map_stack)
    host._map_stack.addWidget(web)
    host._map_stack.setCurrentWidget(web)
    overlay = SimpleNamespace(hidden=False, hide=lambda: setattr(overlay, "hidden", True))
    events: list[str] = []
    host.web = web
    host._map_loading_overlay = overlay
    host._map_first_load_isolated = True
    host._map_surface_presented = True
    host._map_surface_ready = False
    host._map_surface_reveal_generation = 4
    host._map_surface_reveal_probe_pending = True
    host._map_initialized = True
    host._map_load_ok = True
    host._map_surface_payload_applied = True
    host._emit_map_event = lambda event, **_kwargs: events.append(str(event))
    host._schedule_leaflet_viewport_settle = lambda: None
    host._map_script_page = MethodType(StationsMapTab._map_script_page, host)
    host._sync_map_loading_overlay_geometry = lambda: True

    StationsMapTab._on_map_page_title_changed(host, "fio-map-ready:3")
    assert host._map_surface_ready is False
    assert events == []

    StationsMapTab._on_map_page_title_changed(host, "fio-map-ready:4")
    StationsMapTab._on_map_page_title_changed(host, "fio-map-ready:4")

    assert host._map_surface_ready is True
    assert overlay.hidden is True
    assert events.count("surface_revealed") == 1
    window.close()
    window.deleteLater()
    _app().processEvents()


def test_map_surface_reveal_timeout_enters_degraded_instead_of_retrying_forever(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A presented page that never paints must leave the opaque loading state."""
    degraded: list[tuple[str, str]] = []
    monkeypatch.setattr(stations_map_module.time, "monotonic", lambda: 20.0)
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_surface_ready=False,
        _map_surface_presented=True,
        _map_surface_presentation_started_at=1.0,
        _enter_map_degraded=lambda detail, *, reason, **_kwargs: degraded.append((detail, reason)),
    )

    StationsMapTab._on_map_surface_reveal_timeout(host)

    assert len(degraded) == 1
    assert degraded[0][1] == "surface_reveal_timeout"


def test_resize_after_presentation_stays_in_present_phase_and_schedules_reveal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-presentation geometry changes invalidate reveal, not cold preparation."""
    timer = QTimer()
    timer.setSingleShot(True)
    callbacks: list[object] = []
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_surface_ready=False,
        _map_surface_presented=True,
        _map_surface_geometry_signature_value=(1, 2, 900, 560),
        _map_surface_geometry_changed_at=4.0,
        _map_surface_geometry_stable_samples=3,
        _map_surface_geometry_phase="present",
        _map_surface_reveal_generation=9,
        _map_surface_reveal_probe_pending=True,
        _map_surface_prepare_timer=timer,
        _map_visible=True,
        _app_active=True,
        _schedule_map_surface_reveal=lambda: None,
    )
    monkeypatch.setattr(
        stations_map_module.QTimer,
        "singleShot",
        lambda _delay, callback: callbacks.append(callback),
    )
    host._invalidate_map_surface_geometry_settle = MethodType(
        StationsMapTab._invalidate_map_surface_geometry_settle,
        host,
    )

    StationsMapTab._invalidate_map_surface_geometry_settle(host)

    assert host._map_surface_geometry_phase == "present"
    assert host._map_surface_reveal_generation == 10
    assert host._map_surface_reveal_probe_pending is False
    assert timer.isActive() is False
    assert len(callbacks) == 1
    assert callbacks[0] is host._schedule_map_surface_reveal
    timer.deleteLater()


def test_macos_warm_map_reload_keeps_existing_visible_web_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A warm reload must not blank or hide the already-present map surface."""
    app = _app()
    monkeypatch.setattr(stations_map_module.sys, "platform", "darwin")
    window = QMainWindow()
    stack = QStackedWidget(window)
    loading = QWidget(stack)
    web = QWidget(stack)
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
        _map_surface_ready=True,
        _map_surface_payload_applied=True,
        _map_first_load_isolated=False,
        _map_surface_presented=True,
        _map_visible=True,
        _app_active=True,
        _is_shutting_down=False,
        _pending_map_payload=None,
        _map_dirty=False,
        _render_requested_during_load=False,
        _render_requested_during_load_level=0,
        _emit_map_event=lambda event, **_kwargs: events.append(str(event)),
        _set_map_runtime_state=lambda *_args, **_kwargs: None,
        _map_ready_detail_text=lambda: "ready",
        _schedule_leaflet_viewport_settle=lambda: None,
        _maybe_start_map_ingest=lambda: False,
    )

    assert web.isVisible()
    StationsMapTab._on_map_load_finished(host, True)
    app.processEvents()

    assert stack.currentWidget() is web
    assert web.isVisible()
    assert host._map_load_ok is True
    assert events[0] == "page_load_finished"

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
    monkeypatch.setattr(stations_map_module, "QWebEnginePage", _DetachedMapPage)
    window, host = _map_surface_window()

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

    window.close()
    window.deleteLater()
    app.processEvents()


def test_leaflet_viewport_invalidation_is_once_per_visible_geometry_and_generation() -> None:
    web = _MapWebView(1200, 700)
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_visible=True,
        _app_active=True,
        _map_load_ok=True,
        _map_has_leaflet_page=True,
        web=web,
        _leaflet_viewport_retry_count=0,
        _last_leaflet_viewport_signature=None,
        _map_payload_generation=3,
        _leaflet_viewport_timer=_ViewportTimer(),
    )
    host._map_script_page = MethodType(StationsMapTab._map_script_page, host)

    StationsMapTab._flush_leaflet_viewport_settle(host)
    StationsMapTab._flush_leaflet_viewport_settle(host)

    assert len(web.page().scripts) == 1
    assert "invalidateSize(false)" in web.page().scripts[0]
    assert host._last_leaflet_viewport_signature == (3, 1200, 700)
    assert host._leaflet_viewport_retry_count == 0

    web._size = QSize(1000, 700)
    StationsMapTab._flush_leaflet_viewport_settle(host)

    assert len(web.page().scripts) == 2
    assert host._last_leaflet_viewport_signature == (3, 1000, 700)


def test_isolated_cold_page_skips_leaflet_invalidation_until_presented() -> None:
    """Hidden first-load work must not awaken the native compositor early."""
    web = _MapWebView(1200, 700)
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_visible=True,
        _app_active=True,
        _map_first_load_isolated=True,
        _map_surface_presented=False,
        _map_load_ok=True,
        _map_has_leaflet_page=True,
        web=web,
        _leaflet_viewport_retry_count=0,
        _last_leaflet_viewport_signature=None,
        _map_payload_generation=3,
        _leaflet_viewport_timer=_ViewportTimer(),
    )
    host._map_script_page = MethodType(StationsMapTab._map_script_page, host)

    StationsMapTab._flush_leaflet_viewport_settle(host)
    assert web.page().scripts == []

    host._map_surface_presented = True
    StationsMapTab._flush_leaflet_viewport_settle(host)
    assert len(web.page().scripts) == 1
    assert "invalidateSize(false)" in web.page().scripts[0]


def test_main_shell_ignores_active_map_size_hint_to_preserve_window_geometry() -> None:
    """A lazy Map's native view must not resize or move the top-level shell."""
    source = (
        Path(__file__).resolve().parents[1] / "freqinout/gui/main_window.py"
    ).read_text(encoding="utf-8")

    assert "self.stack.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Ignored)" in source


def test_clean_warm_map_reentry_reuses_live_page_without_refresh_or_state_expansion() -> None:
    states: list[str] = []
    events: list[str] = []
    settled: list[str] = []
    timer = _LifecycleTimer(active=False)
    host = SimpleNamespace(
        _map_visible=False,
        _app_active=True,
        _is_shutting_down=False,
        _map_initialized=True,
        _map_load_ok=True,
        _map_dirty=False,
        _pending_refresh_level=0,
        _ingest_started=True,
        _deferred_initial_ingest_pending=False,
        _js8_timer=timer,
        _set_map_runtime_state=lambda state, *_args, **_kwargs: states.append(str(state)),
        _map_ready_detail_text=lambda: "ready",
        _emit_map_event=lambda event, **_kwargs: events.append(str(event)),
        _schedule_leaflet_viewport_settle=lambda: settled.append("settle"),
        _maybe_start_map_ingest=lambda: False,
    )

    StationsMapTab.set_map_visible(host, True)

    assert host._map_visible is True
    assert timer.start_count == 1
    assert states == ["ready"]
    assert events == ["activation_ready"]
    assert settled == ["settle"]


def test_application_inactivity_alone_does_not_mark_live_map_dirty() -> None:
    timer = _LifecycleTimer(active=True)
    events: list[str] = []
    states: list[str] = []
    settled: list[str] = []
    host = SimpleNamespace(
        _app_active=True,
        _map_visible=True,
        _is_shutting_down=False,
        _map_dirty=False,
        _pending_refresh_level=0,
        _map_initialized=True,
        _map_load_ok=True,
        _ingest_started=True,
        _js8_timer=timer,
        _emit_map_event=lambda event, **_kwargs: events.append(str(event)),
        _set_map_runtime_state=lambda state, *_args, **_kwargs: states.append(str(state)),
        _map_ready_detail_text=lambda: "ready",
        _schedule_leaflet_viewport_settle=lambda: settled.append("settle"),
    )

    StationsMapTab.set_app_active(host, False)

    assert host._map_dirty is False
    assert timer.stop_count == 1
    assert events == ["ui_paused_inactive"]

    StationsMapTab.set_app_active(host, True)

    assert host._map_dirty is False
    assert timer.start_count == 1
    assert states == ["ready"]
    assert events == ["ui_paused_inactive", "ui_resumed_ready"]
    assert settled == ["settle"]


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
