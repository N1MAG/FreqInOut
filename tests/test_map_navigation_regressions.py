"""Regression contracts for returning from the Map pop-out to main FIO."""

from __future__ import annotations

from pathlib import Path

import pytest
from PySide6.QtCore import QRect, Slot
from PySide6.QtWidgets import QApplication, QMainWindow

import freqinout.gui.stations_map_tab as stations_map_module
from freqinout.gui.main_window import MainWindow
from freqinout.gui.stations_map_tab import StationsMapTab


ROOT = Path(__file__).resolve().parents[1]


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _WindowSpy:
    """Platform-neutral top-level window stand-in that records focus mutations."""

    def __init__(self) -> None:
        self.geometry = (90, 120, 1100, 760)
        self.events: list[str] = []

    def raise_(self) -> None:
        self.events.append("raise")

    def activateWindow(self) -> None:  # noqa: N802 - Qt-compatible name
        self.events.append("activate")

    def move(self, *_args) -> None:
        self.events.append("move")

    def resize(self, *_args) -> None:
        self.events.append("resize")

    def close(self) -> None:
        self.events.append("close")


class _MainWindowSpy(_WindowSpy):
    def open_messages_section(self, mode: str, **_kwargs) -> None:
        self.events.append(f"workspace:{mode}")


def _tab_for_main_window(main_window: _MainWindowSpy) -> StationsMapTab:
    tab = StationsMapTab.__new__(StationsMapTab)
    tab._application_host_ref = lambda: main_window
    return tab


def test_map_toolbar_exposes_a_visible_show_fio_action_bound_to_main_window_focus() -> None:
    """The pop-out toolbar must always provide an obvious path back to FIO."""
    source = (ROOT / "freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    build_ui = source[source.index("    def _build_ui(self):") : source.index("    def _build_map_selected_detail_panel", source.index("    def _build_ui(self):"))]

    assert 'self._show_fio_button = QPushButton("Show FIO")' in build_ui
    assert "self._show_fio_button.setVisible(True)" in build_ui
    assert "self._show_fio_button.clicked.connect(self._bring_application_window_forward)" in build_ui


def test_show_fio_brings_existing_main_window_forward_without_window_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Focus the pre-existing FIO window; never recreate, close, or reposition either window."""
    main_window = _MainWindowSpy()
    map_window = _WindowSpy()
    tab = _tab_for_main_window(main_window)
    main_geometry = main_window.geometry
    map_geometry = map_window.geometry
    monkeypatch.setattr(StationsMapTab, "window", lambda _self: map_window)

    StationsMapTab._bring_application_window_forward(tab)

    assert main_window.events == ["raise", "activate"]
    assert map_window.events == []
    assert main_window.geometry == main_geometry
    assert map_window.geometry == map_geometry


def test_show_fio_queues_qt_main_window_presentation_outside_click_dispatch() -> None:
    """A toolbar click must not re-enter app lifecycle while Qt dispatches it."""
    app = _app()

    class _QueuedMainWindow(QMainWindow):
        def __init__(self) -> None:
            super().__init__()
            self.events: list[str] = []

        @Slot()
        def present_main_window(self) -> None:
            self.events.append("present")

    main_window = _QueuedMainWindow()
    tab = _tab_for_main_window(main_window)
    try:
        StationsMapTab._bring_application_window_forward(tab)
        assert main_window.events == []

        app.processEvents()
        assert main_window.events == ["present"]
    finally:
        main_window.close()
        main_window.deleteLater()
        app.processEvents()


def test_selected_station_inbox_opens_workspace_before_returning_focus_to_main_fio() -> None:
    """Inbox routing must reveal the destination rather than leaving FIO behind the Map."""
    main_window = _MainWindowSpy()
    tab = _tab_for_main_window(main_window)
    tab._map_selected_payload = {"id": "station-1"}
    tab._map_selected_message_context = lambda _payload: {
        "target": "messages",
        "group_filter": "MR08",
        "topic_filter": "Comms",
        "query_filter": "K1ABC",
        "source_family": "spotter",
        "age_filter_seconds": 86400,
        "concern_only": False,
        "state_filter": "",
        "grid_filter": "",
        "fema_region_filter": "",
    }

    StationsMapTab._open_map_selected_messages(tab)

    assert main_window.events == ["workspace:inbox", "raise", "activate"]


def test_selected_station_compose_opens_workspace_before_returning_focus_to_main_fio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compose routing must likewise surface the existing main FIO window."""
    main_window = _MainWindowSpy()
    tab = _tab_for_main_window(main_window)
    tab._map_selected_action_callsign = lambda: "K1ABC"
    tab._map_selected_station_is_self = lambda _callsign: False
    tab._map_selected_payload = {"id": "station-1"}
    main_window.message_viewer_tab = None
    monkeypatch.setattr(stations_map_module.QTimer, "singleShot", lambda _ms, callback: callback())

    StationsMapTab._compose_message_for_selected_station(tab)

    assert main_window.events == ["workspace:compose", "raise", "activate"]


@pytest.mark.parametrize("presentation", ["normal", "maximized", "fullscreen"])
def test_main_window_presentation_preserves_existing_geometry_and_non_minimized_state(
    presentation: str,
) -> None:
    """Foreground requests must not resize either persistent top-level window."""
    app = _app()
    main = QMainWindow()
    map_window = QMainWindow()
    main.setGeometry(QRect(90, 120, 1100, 760))
    map_window.setGeometry(QRect(30, 40, 900, 700))
    show_method = {
        "normal": main.showNormal,
        "maximized": main.showMaximized,
        "fullscreen": main.showFullScreen,
    }[presentation]
    show_method()
    map_window.showMaximized()
    app.processEvents()
    main_geometry = QRect(main.geometry())
    main_state = main.windowState()
    map_geometry = QRect(map_window.geometry())
    map_state = map_window.windowState()
    try:
        MainWindow.present_main_window(main)
        app.processEvents()

        assert main.geometry() == main_geometry
        assert main.windowState() == main_state
        assert map_window.geometry() == map_geometry
        assert map_window.windowState() == map_state
    finally:
        main.close()
        map_window.close()
        main.deleteLater()
        map_window.deleteLater()
        app.processEvents()


def test_main_window_presentation_removes_only_minimized_state() -> None:
    app = _app()
    main = QMainWindow()
    map_window = QMainWindow()
    main.setGeometry(QRect(90, 120, 1100, 760))
    main.showMaximized()
    map_window.showMaximized()
    app.processEvents()
    main.showMinimized()
    app.processEvents()
    main_normal_geometry = QRect(main.normalGeometry())
    map_geometry = QRect(map_window.geometry())
    map_state = map_window.windowState()
    try:
        MainWindow.present_main_window(main)
        app.processEvents()

        assert not main.isMinimized()
        assert main.isMaximized()
        assert main.normalGeometry() == main_normal_geometry
        assert map_window.geometry() == map_geometry
        assert map_window.windowState() == map_state
    finally:
        main.close()
        map_window.close()
        main.deleteLater()
        map_window.deleteLater()
        app.processEvents()
