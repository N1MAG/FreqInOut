from __future__ import annotations

import os
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QEvent
from PySide6.QtWidgets import QApplication, QWidget

from freqinout.gui import startup_surface_trace as trace_module
from freqinout.gui.startup_surface_trace import (
    StartupSurfaceTrace,
    install_windows_startup_surface_trace,
)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_trace_records_only_top_level_widget_surface_events(monkeypatch) -> None:
    app = _app()
    messages: list[tuple[str, tuple[object, ...]]] = []
    monkeypatch.setattr(
        trace_module.log,
        "info",
        lambda message, *args: messages.append((message, args)),
    )
    trace = StartupSurfaceTrace(app, started_at=time.perf_counter())
    top_level = QWidget()
    top_level.setObjectName("candidateWindow")
    top_level.setWindowTitle("Transient candidate")
    child = QWidget(top_level)
    try:
        trace.set_stage("Building station dashboard...")
        trace.eventFilter(child, QEvent(QEvent.Type.Show))
        trace.eventFilter(top_level, QEvent(QEvent.Type.Show))
        trace.stop()
    finally:
        top_level.deleteLater()

    surface_messages = [item for item in messages if "action=surface" in item[0]]
    assert len(surface_messages) == 1
    message, args = surface_messages[0]
    assert "event=%s %s" in message
    assert args[-2] == "show"
    assert "class=QWidget" in str(args[-1])
    assert "object=candidateWindow" in str(args[-1])
    assert "title=Transient candidate" in str(args[-1])
    assert "parent=-" in str(args[-1])


def test_trace_factory_is_bounded_to_windows() -> None:
    app = _app()
    assert (
        install_windows_startup_surface_trace(
            app,
            started_at=time.perf_counter(),
            platform="linux",
        )
        is None
    )


def test_settings_construction_never_shows_parentless_content(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = _app()
    messages: list[tuple[str, tuple[object, ...]]] = []
    monkeypatch.setattr(
        trace_module.log,
        "info",
        lambda message, *args: messages.append((message, args)),
    )

    from freqinout.gui.settings_tab import SettingsTab

    trace = StartupSurfaceTrace(app, started_at=time.perf_counter())
    tab = SettingsTab(defer_initial_load=True)
    try:
        trace.stop()
        shown_surfaces = [
            str(args[-1])
            for message, args in messages
            if "action=surface" in message and len(args) >= 2 and args[-2] == "show"
        ]
        assert shown_surfaces == []
    finally:
        trace.stop()
        tab.shutdown()
        tab.settings.close()
        tab.deleteLater()
        app.processEvents()


def test_station_command_refresh_hides_widgets_before_unparenting() -> None:
    from inspect import getsource

    from freqinout.gui.main_window import MainWindow

    for method in (
        MainWindow._refresh_adaptive_station_command_shell,
        MainWindow._refresh_station_command_radio_summary,
    ):
        source = getsource(method)
        assert "widget.hide()\n                widget.setParent(None)" in source
