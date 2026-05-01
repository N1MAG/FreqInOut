from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QMessageBox

from freqinout.gui import dialog_notifications as dialogs


_APP: QApplication | None = None


def _app() -> QApplication:
    global _APP
    existing = QApplication.instance()
    if existing is not None:
        return existing
    _APP = QApplication([])
    return _APP


def teardown_function() -> None:
    app = _app()
    for box in list(dialogs._ACTIVE_INFORMATION_BOXES):
        box.close()
    app.processEvents()
    dialogs._ACTIVE_INFORMATION_BOXES.clear()
    dialogs.uninstall_auto_closing_information_dialogs_for_tests()


def test_information_dialog_patch_shows_nonblocking_auto_close_notice() -> None:
    app = _app()

    dialogs.install_auto_closing_information_dialogs(default_timeout_ms=2500)
    result = QMessageBox.information(None, "Saved", "Settings saved.")
    app.processEvents()

    assert result == dialogs._standard_button("Ok")
    assert dialogs._ACTIVE_INFORMATION_BOXES

    box = dialogs._ACTIVE_INFORMATION_BOXES[-1]
    assert box.text() == "Settings saved."
    assert box.windowModality() == Qt.WindowModality.NonModal
    assert box.isModal() is False


def test_information_dialog_timeout_scales_but_stays_bounded() -> None:
    short_timeout = dialogs._timeout_for_text("Saved.", default_timeout_ms=4500)
    long_timeout = dialogs._timeout_for_text("x" * 1000, default_timeout_ms=4500)

    assert short_timeout >= 2500
    assert long_timeout > short_timeout
    assert long_timeout <= 30000


def test_information_dialog_patch_only_handles_ok_only_notices() -> None:
    assert dialogs._ok_only(dialogs._standard_button("Ok")) is True
    assert dialogs._ok_only(dialogs._standard_button("NoButton")) is True

    yes = dialogs._standard_button("Yes")
    no = dialogs._standard_button("No")
    assert dialogs._ok_only(yes | no) is False
