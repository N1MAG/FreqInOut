from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QRect, QSize, Qt
from PySide6.QtWidgets import QApplication, QMainWindow, QStackedWidget, QWidget

from freqinout.gui.main_window import MainWindow
from freqinout.gui.stations_map_tab import StationsMapTab
from freqinout.gui.stations_map_tab import _MapProjectionSnapshotResult
from freqinout.gui.map_window import (
    MAP_WINDOW_PLACEMENT_KEY,
    MapWindowPlacement,
    PersistentMapWindow,
    resolve_map_window_placement,
)
from freqinout.gui.theme import get_theme


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _Settings:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def get(self, key: str, default: object = None) -> object:
        return self.values.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.values[key] = value

    def set_many(self, values: dict[str, object]) -> None:
        self.values.update(values)


class _Screen:
    def __init__(self, name: str, geometry: QRect, serial: str = "") -> None:
        self._name = name
        self._geometry = QRect(geometry)
        self._serial = serial

    def name(self) -> str:
        return self._name

    def availableGeometry(self) -> QRect:  # noqa: N802 - Qt-compatible name
        return QRect(self._geometry)

    def serialNumber(self) -> str:  # noqa: N802 - Qt-compatible name
        return self._serial


class _SpyMapTab(QWidget):
    def __init__(self, parent: QWidget | None = None, *args, application_host=None, **kwargs) -> None:
        super().__init__(parent)
        self.constructor_parent = parent
        self.application_host = application_host
        self.reparent_calls: list[QWidget | None] = []
        self.shutdown_calls = 0
        self.refreshes: list[dict[str, object]] = []

    def setParent(self, parent) -> None:  # noqa: N802 - Qt virtual method name
        self.reparent_calls.append(parent)
        super().setParent(parent)

    def shutdown(self) -> None:
        self.shutdown_calls += 1

    def _request_map_refresh(self, **kwargs) -> None:
        self.refreshes.append(dict(kwargs))


class _NativeAttachmentSpyMapTab(_SpyMapTab):
    """Map-content stand-in that records its first native-surface attachment."""

    def __init__(self, parent: QWidget | None = None, *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        self.native_attachment_contexts: list[tuple[QWidget | None, QWidget | None, QRect, QSize, QSize]] = []

    def set_map_visible(self, visible: bool) -> None:
        if not visible:
            return
        window = self.window()
        self.native_attachment_contexts.append(
            (
                window.centralWidget() if isinstance(window, QMainWindow) else None,
                self.parentWidget(),
                QRect(window.geometry()),
                QSize(window.sizeHint()),
                QSize(window.minimumSizeHint()),
            )
        )


class _ThemeSpyMapTab(_SpyMapTab):
    def __init__(self, parent: QWidget | None = None, *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        self.themes: list[dict[str, str]] = []

    def apply_theme(self, theme: dict[str, str]) -> None:
        self.themes.append(dict(theme))


@pytest.fixture
def map_host() -> QMainWindow:
    app = _app()
    host = QMainWindow()
    host.setGeometry(43, 71, 910, 570)
    host.show()
    app.processEvents()
    try:
        yield host
    finally:
        host.close()
        host.deleteLater()
        app.processEvents()


@pytest.fixture
def map_window(map_host: QMainWindow):
    app = _app()
    settings = _Settings()
    created: list[_SpyMapTab] = []

    def factory(parent: QWidget) -> _SpyMapTab:
        tab = _SpyMapTab(parent, application_host=map_host)
        created.append(tab)
        return tab

    window = PersistentMapWindow(map_host, settings, factory)
    try:
        yield window, settings, created
    finally:
        window.shutdown()
        window.deleteLater()
        app.processEvents()


def _window_snapshot(window: QMainWindow) -> tuple[QRect, Qt.WindowState, bool, bool, object]:
    return (
        QRect(window.geometry()),
        window.windowState(),
        bool(window.isMaximized()),
        bool(window.isFullScreen()),
        window.screen(),
    )


def test_placement_accepts_valid_saved_geometry_on_connected_screen() -> None:
    primary = _Screen("primary", QRect(0, 0, 1600, 900))
    external = _Screen("external", QRect(1600, 0, 1920, 1080))
    saved = MapWindowPlacement(QRect(1780, 120, 900, 640), True, "external")

    resolved = resolve_map_window_placement(saved.as_dict(), (primary, external), primary)

    assert resolved == saved


@pytest.mark.parametrize(
    "saved",
    (
        MapWindowPlacement(QRect(-9000, 20, 900, 640), False, "primary"),
        MapWindowPlacement(QRect(1800, 20, 900, 640), True, "disconnected"),
        MapWindowPlacement(QRect(40, 40, 0, 640), False, "primary"),
    ),
)
def test_placement_rejects_invalid_or_disconnected_saved_placement(saved: MapWindowPlacement) -> None:
    primary = _Screen("primary", QRect(0, 0, 1600, 900))
    external = _Screen("external", QRect(1600, 0, 1920, 1080))

    resolved = resolve_map_window_placement(saved.as_dict(), (primary, external), primary)

    assert resolved.screen_id == "primary"
    assert resolved.maximized is False
    assert primary.availableGeometry().contains(resolved.geometry)
    assert resolved.geometry.width() > 0
    assert resolved.geometry.height() > 0


def test_placement_rejects_malformed_maximized_value() -> None:
    primary = _Screen("primary", QRect(0, 0, 1600, 900))
    saved = MapWindowPlacement(QRect(120, 100, 900, 640), False, "primary").as_dict()
    saved["maximized"] = "false"

    resolved = resolve_map_window_placement(saved, (primary,), primary)

    assert resolved.maximized is False


def test_placement_recovers_renamed_screen_by_serial() -> None:
    primary = _Screen("primary", QRect(0, 0, 1600, 900), "P-001")
    renamed = _Screen("field-display", QRect(1600, 0, 1920, 1080), "EXT-007")
    saved = MapWindowPlacement(QRect(1760, 120, 900, 640), True, "old-display").as_dict()
    saved["screen_serial"] = "EXT-007"

    resolved = resolve_map_window_placement(saved, (renamed, primary), primary)

    assert resolved.screen_id == "field-display"
    assert resolved.geometry == QRect(1760, 120, 900, 640)
    assert resolved.maximized is True


def test_persistent_map_window_constructs_content_once_with_final_parent(map_window) -> None:
    window, _settings, created = map_window
    app = _app()

    first = window.ensure_content()
    window.present()
    app.processEvents()
    second = window.ensure_content()

    assert first is second is window.map_tab
    assert created == [first]
    assert first.constructor_parent is first.parentWidget()
    assert first.window() is window
    assert first.application_host is window._application_host
    assert first.reparent_calls == []
    assert window.windowModality() == Qt.NonModal
    assert not bool(window.windowFlags() & Qt.WindowStaysOnTopHint)


def test_first_show_keeps_a_permanent_central_container_through_native_map_attachment(
    map_host: QMainWindow,
) -> None:
    """The loading-to-Map handoff must not replace a visible QMainWindow central widget.

    On macOS, replacing the central widget immediately before a native map
    surface attaches can look like the window is being torn down and
    recreated.  The queued first-show path must therefore hand the Map off
    inside the central container installed during construction, with no
    top-level size-hint or normal-geometry transition.
    """
    app = _app()
    settings = _Settings()
    created: list[_NativeAttachmentSpyMapTab] = []

    def factory(parent: QWidget) -> _NativeAttachmentSpyMapTab:
        tab = _NativeAttachmentSpyMapTab(parent, application_host=map_host)
        created.append(tab)
        return tab

    window = PersistentMapWindow(map_host, settings, factory)
    try:
        permanent_container = window.centralWidget()
        initial_geometry = QRect(window.geometry())
        initial_normal_geometry = QRect(window._normal_geometry)
        initial_size_hint = QSize(window.sizeHint())
        initial_minimum_size_hint = QSize(window.minimumSizeHint())

        # The final native hierarchy is built while the window is still hidden,
        # so its first visible frame cannot attach or replace a child surface.
        window.present()

        assert len(created) == 1
        assert window.centralWidget() is permanent_container

        app.processEvents()

        assert len(created) == 1
        tab = created[0]
        assert tab.constructor_parent is permanent_container
        assert tab.parentWidget() is permanent_container
        assert window.centralWidget() is permanent_container
        assert tab is not permanent_container
        assert window.geometry() == initial_geometry
        assert window._normal_geometry == initial_normal_geometry
        assert window.sizeHint() == initial_size_hint
        assert window.minimumSizeHint() == initial_minimum_size_hint
        assert tab.native_attachment_contexts == [
            (
                permanent_container,
                permanent_container,
                initial_geometry,
                initial_size_hint,
                initial_minimum_size_hint,
            )
        ]
    finally:
        window.shutdown()
        window.deleteLater()
        app.processEvents()


def test_live_theme_forwarding_does_not_move_resize_or_rebuild_map_window(
    map_host: QMainWindow,
) -> None:
    """The separate Map follows one shared palette without geometry churn."""
    app = _app()
    settings = _Settings()
    created: list[_ThemeSpyMapTab] = []

    def factory(parent: QWidget) -> _ThemeSpyMapTab:
        tab = _ThemeSpyMapTab(parent, application_host=map_host)
        created.append(tab)
        return tab

    window = PersistentMapWindow(map_host, settings, factory)
    try:
        window.present()
        app.processEvents()
        before = _window_snapshot(window)
        tab = created[0]

        light = get_theme("light")
        dark = get_theme("dark")
        window.apply_theme(light)
        window.apply_theme(dark)
        app.processEvents()

        assert tab.themes[-2:] == [light, dark]
        assert _window_snapshot(window) == before
        assert window._map_tab is tab
    finally:
        window.shutdown()
        window.deleteLater()
        app.processEvents()


def test_close_hides_and_present_reuses_the_same_map_window(map_window) -> None:
    window, _settings, created = map_window
    app = _app()

    window.present()
    app.processEvents()
    tab = window.map_tab
    assert window.isVisible()

    window.close()
    app.processEvents()

    assert not window.isVisible()
    assert window.map_tab is tab
    assert len(created) == 1
    assert tab.shutdown_calls == 0

    window.present()
    app.processEvents()

    assert window.isVisible()
    assert window.map_tab is tab
    assert len(created) == 1


def test_close_after_prebuilt_first_frame_keeps_hidden_content_for_stable_reopen(map_window) -> None:
    window, _settings, created = map_window
    app = _app()

    window.present()
    window.close()
    app.processEvents()

    assert not window.isVisible()
    assert window.map_tab is created[0]
    assert len(created) == 1

    window.present()
    app.processEvents()

    assert window.isVisible()
    assert window.map_tab is created[0]
    assert len(created) == 1


def test_present_hide_resize_and_maximize_never_mutate_main_window(map_host: QMainWindow) -> None:
    app = _app()
    settings = _Settings()
    window = PersistentMapWindow(
        map_host,
        settings,
        lambda parent: _SpyMapTab(parent, application_host=map_host),
    )
    try:
        before = _window_snapshot(map_host)
        window.present()
        app.processEvents()
        window.setGeometry(120, 140, 980, 660)
        app.processEvents()
        window.showMaximized()
        app.processEvents()
        window.hide()
        app.processEvents()
        window.present()
        app.processEvents()

        assert _window_snapshot(map_host) == before
    finally:
        window.shutdown()
        window.deleteLater()
        app.processEvents()


def test_window_persists_normal_geometry_and_maximized_state(map_host: QMainWindow) -> None:
    app = _app()
    settings = _Settings()
    factory = lambda parent: _SpyMapTab(parent, application_host=map_host)
    first = PersistentMapWindow(map_host, settings, factory)
    try:
        first.present()
        first.setGeometry(130, 150, 970, 650)
        app.processEvents()
        normal = QRect(first.geometry())
        first.showMaximized()
        app.processEvents()
        first.close()  # ordinary close persists then hides
        app.processEvents()
        persisted = settings.get(MAP_WINDOW_PLACEMENT_KEY)
        assert isinstance(persisted, dict)
        assert persisted["screen"] == first.screen().name()
        assert persisted["maximized"] is True
        assert QRect(
            int(persisted["x"]),
            int(persisted["y"]),
            int(persisted["width"]),
            int(persisted["height"]),
        ) == normal
    finally:
        first.shutdown()
        first.deleteLater()
        app.processEvents()

    restored = PersistentMapWindow(map_host, settings, factory)
    try:
        restored.present()
        app.processEvents()

        assert restored.isMaximized()
        expected = resolve_map_window_placement(
            settings.get(MAP_WINDOW_PLACEMENT_KEY),
            tuple(app.screens()),
            map_host.screen(),
        )
        # The offscreen screen is only 800 px wide, so the deliberately wider
        # persisted rectangle is safely clamped during the round trip.
        assert restored._normal_geometry == expected.geometry
    finally:
        restored.shutdown()
        restored.deleteLater()
        app.processEvents()


def test_shutdown_closes_window_and_stops_map_content_exactly_once(map_window) -> None:
    window, _settings, _created = map_window
    app = _app()
    window.present()
    app.processEvents()
    tab = window.map_tab

    window.shutdown()
    app.processEvents()
    window.shutdown()
    app.processEvents()

    assert tab.shutdown_calls == 1
    assert not window.isVisible()
    assert window.is_available_for_work() is False


def test_clean_warm_map_reentry_does_not_queue_a_refresh() -> None:
    states: list[tuple[str, str]] = []
    events: list[str] = []
    host = SimpleNamespace(
        _map_visible=False,
        _js8_timer=None,
        _app_active=True,
        _is_shutting_down=False,
        _ingest_started=True,
        _map_initialized=True,
        _map_load_ok=True,
        _map_dirty=False,
        _pending_refresh_level=0,
        _map_ready_detail_text=lambda: "ready",
        _set_map_runtime_state=lambda state, detail: states.append((state, detail)),
        _emit_map_event=lambda event, **_kwargs: events.append(event),
    )

    StationsMapTab.set_map_visible(host, True)

    assert states == [("ready", "ready")]
    assert events == ["activation_ready"]


def test_hidden_dirty_map_reentry_coalesces_to_one_refresh() -> None:
    refreshes: list[dict[str, object]] = []
    host = SimpleNamespace(
        _map_visible=True,
        _is_shutting_down=False,
        _app_active=True,
        _map_initialized=True,
        _map_load_ok=True,
        _map_dirty=True,
        _pending_map_payload={"markers": [{"callsign": "STALE"}]},
        _ensure_initial_data_loaded=lambda: None,
        _ensure_native_map_renderer=lambda: True,
        _request_map_refresh=lambda **kwargs: refreshes.append(dict(kwargs)),
        _set_map_runtime_state=lambda *_args, **_kwargs: None,
        _map_ready_detail_text=lambda: "ready",
    )

    StationsMapTab._on_map_visible_deferred(host)
    StationsMapTab._on_map_visible_deferred(host)

    assert refreshes == [
        {"level": "medium", "reason": "visible_dirty", "preserve_view": True}
    ]
    assert host._pending_map_payload is None


def test_projection_completion_while_hidden_defers_native_map_work_until_reopen() -> None:
    projections: list[dict[str, object]] = []
    events: list[str] = []
    pending_applies: list[str] = []
    result = _MapProjectionSnapshotResult(
        generation=3,
        payload='{"markers": []}',
        signature="current",
        pending_payload={"markers": [], "links": []},
    )
    host = SimpleNamespace(
        _is_shutting_down=False,
        _map_payload_generation=3,
        _last_map_payload_sig=None,
        _map_visible=False,
        _app_active=True,
        _pending_map_payload=None,
        _emit_map_event=lambda event, **_kwargs: events.append(event),
        _native_map_renderer=SimpleNamespace(apply_projection=lambda payload: projections.append(dict(payload))),
    )

    StationsMapTab._on_map_payload_snapshot_ready(host, result)

    assert projections == []
    assert host._pending_map_payload == {"markers": [], "links": []}
    assert events == ["payload_update_deferred_hidden"]

    host._map_visible = True
    host._map_initialized = True
    host._map_load_ok = True
    host._map_dirty = False
    host._pending_refresh_level = 0
    host._ensure_initial_data_loaded = lambda: None
    host._ensure_native_map_renderer = lambda: True
    def _apply_pending() -> None:
        pending_applies.append("apply")
        host._pending_map_payload = None

    host._apply_pending_native_map_projection = _apply_pending
    host._set_map_runtime_state = lambda *_args, **_kwargs: None
    host._map_ready_detail_text = lambda: "ready"

    StationsMapTab._on_map_visible_deferred(host)
    StationsMapTab._on_map_visible_deferred(host)

    assert pending_applies == ["apply"]


def test_main_map_navigation_is_a_direct_popout_action_not_stack_navigation() -> None:
    """The Map button leaves the active main-workspace page in place."""
    _app()
    host = QMainWindow()
    stack = QStackedWidget(host)
    current = QWidget(stack)
    map_placeholder = QWidget(stack)
    stack.addWidget(current)
    stack.addWidget(map_placeholder)
    stack.setCurrentWidget(current)
    host.setCentralWidget(stack)
    host.settings = _Settings()
    host.stack = stack
    host._screens = [("Settings", current), ("Map", map_placeholder)]
    host._active_tab_index = 0
    host._help_dialog_settle_until = 0.0
    host._screen_is_runtime_suppressed = lambda _label: False
    host._open_map_window = lambda: opened.append("map")
    host._restore_nav_selection_to_active_tab = lambda: None
    host._sync_compact_navigation_selection = lambda _label: None
    host._current_screen_label = lambda: "Settings"
    opened: list[str] = []

    try:
        MainWindow._set_screen(host, 1)

        assert opened == ["map"]
        assert stack.currentWidget() is current
        assert host._active_tab_index == 0
    finally:
        host.close()
        host.deleteLater()
        _app().processEvents()


def test_main_window_owns_one_persistent_map_window_and_open_raises_it(monkeypatch: pytest.MonkeyPatch) -> None:
    from freqinout.gui import main_window as main_window_module

    app = _app()
    host = QMainWindow()
    host.settings = _Settings()
    host.plan_context_service = None
    host.map_window = None
    host._shutting_down = False
    created: list[_SpyMapTab] = []

    class _Window(PersistentMapWindow):
        present_calls = 0
        raise_calls = 0
        activate_calls = 0

        def present(self) -> None:
            type(self).present_calls += 1
            super().present()

        def raise_(self) -> None:
            type(self).raise_calls += 1
            super().raise_()

        def activateWindow(self) -> None:  # noqa: N802 - Qt virtual method name
            type(self).activate_calls += 1
            super().activateWindow()

    def factory(parent: QWidget, *args, application_host=None, **kwargs) -> _SpyMapTab:
        tab = _SpyMapTab(parent, application_host=application_host)
        created.append(tab)
        return tab

    monkeypatch.setattr(main_window_module, "PersistentMapWindow", _Window)
    host._create_stations_map_tab = lambda parent: factory(parent, application_host=host)
    host._update_map_navigation_state = lambda *_args, **_kwargs: None
    host._show_tab_loading_notice = lambda *_args, **_kwargs: None
    host._screen_is_runtime_suppressed = lambda _label: False
    host._on_map_window_content_ready = lambda _tab: None
    host._on_map_window_content_failed = lambda _error: None
    host._on_map_window_work_visibility_changed = lambda _visible: None
    host._on_map_window_destroyed = lambda *_args: None
    host._ensure_map_window = lambda: MainWindow._ensure_map_window(host)
    try:
        first = MainWindow._ensure_map_window(host)
        second = MainWindow._ensure_map_window(host)
        MainWindow._open_map_window(host)
        app.processEvents()
        MainWindow._open_map_window(host)
        app.processEvents()

        assert first is second is host.map_window
        assert len(created) == 1
        assert _Window.present_calls == 2
        assert _Window.raise_calls == 2
        assert _Window.activate_calls == 2
        assert first.map_tab.application_host is host
    finally:
        window = getattr(host, "map_window", None)
        if window is not None:
            window.shutdown()
            window.deleteLater()
        host.close()
        host.deleteLater()
        app.processEvents()


def test_popout_source_guards_keep_map_out_of_the_stack_and_use_explicit_host() -> None:
    root = Path(__file__).resolve().parents[1]
    main_source = (root / "freqinout/gui/main_window.py").read_text(encoding="utf-8")
    map_source = (root / "freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    popout_source = (root / "freqinout/gui/map_window.py").read_text(encoding="utf-8")

    assert "def _ensure_map_window" in main_source
    assert "def _open_map_window" in main_source
    assert "map_window.shutdown()" in main_source
    set_screen = main_source[main_source.index("    def _set_screen") :]
    map_branch = set_screen[set_screen.index('if label == "Map"') : set_screen.index("self._ensure_lazy_tab_loaded(label, index)")]
    assert "self._open_map_window()" in map_branch
    assert "self.stack.setCurrentIndex(index)" not in map_branch
    assert "application_host" in map_source
    assert "QWebEngineView" not in main_source
    assert "self._map_tab_factory(self._content_stack)" in popout_source
    assert "setParent(" not in popout_source
