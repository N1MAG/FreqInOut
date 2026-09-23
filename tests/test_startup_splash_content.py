from __future__ import annotations

import os

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
