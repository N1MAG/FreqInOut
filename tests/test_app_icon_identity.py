from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout import main as fio_main


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_linux_prefers_png_and_declares_desktop_identity_before_first_window(monkeypatch) -> None:
    monkeypatch.setattr(fio_main.sys, "platform", "linux")

    candidates = fio_main._app_icon_candidate_paths()
    app = _app()
    fio_main._apply_application_identity(app)

    assert candidates
    assert candidates[0].name == "FreqInOut-desktop.png"
    assert app.applicationName() == "FreqInOut"
    assert app.applicationDisplayName() == "FreqInOut"
    assert app.organizationName() == "N1MAG"
    assert app.desktopFileName() == "freqinout"


def test_windows_keeps_multiresolution_ico_as_first_choice(monkeypatch) -> None:
    monkeypatch.setattr(fio_main.sys, "platform", "win32")

    assert fio_main._app_icon_candidate_paths()[0].name == "FreqInOut.ico"


def test_application_icon_loads_from_source_assets(monkeypatch) -> None:
    monkeypatch.setattr(fio_main.sys, "platform", "linux")

    icon = fio_main._load_app_icon()

    assert not icon.isNull()


def test_linux_desktop_entry_declares_matching_startup_window_class() -> None:
    installer = Path(__file__).resolve().parents[1] / "install_FreqInOut_linux.sh"

    assert "StartupWMClass=FreqInOut" in installer.read_text(encoding="utf-8")
