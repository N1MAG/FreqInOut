from __future__ import annotations

from inspect import getsource
import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.gui.startup_splash import StartupSplash


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_startup_splash_includes_dedication_and_project_support_message() -> None:
    app = _app()
    splash = StartupSplash(app, version="v-test")
    try:
        assert splash.DEDICATION == (
            "Dedicated to my Dad (SK), a U.S. Navy Radioman who learned "
            "HF Digital Tri-Mode at age 86."
        )
        assert splash.SUPPORT_MESSAGE == (
            "If FIO benefits your station, please consider supporting its continued "
            "development. There's more to come."
        )
        assert splash.SUPPORT_URL == "buymeacoffee.com/n1mag"
        assert splash._splash.accessibleName() == "Starting FIO"
        description = splash._splash.accessibleDescription()
        assert splash.DEDICATION in description
        assert splash.SUPPORT_URL in description
        assert splash._splash.pixmap().size().width() >= 540
        assert splash._splash.pixmap().size().height() >= 270
    finally:
        splash.close()


def test_startup_splash_prepares_first_frame_before_showing_native_surface() -> None:
    events: list[tuple[str, str]] = []
    fake = SimpleNamespace()
    fake.update_status_without_event_pump = lambda message: events.append(("prepare", message))
    fake._splash = SimpleNamespace(show=lambda: events.append(("show", "")))
    fake._process_events = lambda: events.append(("events", ""))

    StartupSplash.show(fake, "Checking database...")

    assert events == [
        ("prepare", "Checking database..."),
        ("show", ""),
        ("events", ""),
    ]


def test_windows_main_shell_is_shielded_until_deliberate_first_show() -> None:
    from freqinout.gui.main_window import MainWindow
    from freqinout import main as main_module

    constructor_source = getsource(MainWindow.__init__)
    shield = 'self.setAttribute(Qt.WA_DontShowOnScreen, True)'
    assert shield in constructor_source
    assert constructor_source.index(shield) < constructor_source.index(
        'self.settings = _construct_startup_component("settings_manager", SettingsManager)'
    )

    entry_source = getsource(main_module.main)
    release = 'win.release_startup_surface_shield()'
    assert release in entry_source
    assert entry_source.index(release) < entry_source.index("win.show()")


def test_releasing_windows_main_shell_shield_normalizes_visibility_first() -> None:
    from freqinout.gui.main_window import MainWindow

    events: list[tuple[str, object]] = []
    fake = SimpleNamespace(
        _startup_surface_shielded=True,
        hide=lambda: events.append(("hide", None)),
        setAttribute=lambda attribute, enabled: events.append(("attribute", (attribute, enabled))),
    )

    MainWindow.release_startup_surface_shield(fake)

    assert events[0] == ("hide", None)
    assert events[1][0] == "attribute"
    assert events[1][1][1] is False
    assert fake._startup_surface_shielded is False
