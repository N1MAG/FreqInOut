"""Focused real-widget presentation checks for GRS-6.5 guided save."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Callable

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
)


_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    global _QT_APP
    application = QApplication.instance()
    if isinstance(application, QApplication):
        _QT_APP = application
    elif _QT_APP is None:
        _QT_APP = QApplication([])
    return _QT_APP


def _open_add_radio(
    monkeypatch,
    tmp_path: Path,
    inspect_dialog: Callable[[QDialog], None],
):
    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)

    def fake_exec(dialog: QDialog) -> int:
        dialog.resize(760, 540)
        dialog.show()
        _app().processEvents()
        inspect_dialog(dialog)
        return dialog.result()

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    tab = SettingsTab()
    try:
        return tab._open_device_profile_dialog(existing=None)
    finally:
        tab.deleteLater()
        _app().processEvents()


def _advance_to_review(dialog: QDialog) -> None:
    name = next(
        (
            field
            for field in dialog.findChildren(QLineEdit)
            if "radio name" in field.placeholderText().casefold()
        ),
        None,
    )
    assert name is not None
    name.setText("GRS65 Test Radio")
    setup = dialog.findChild(QComboBox, "guidedSetupType")
    assert setup is not None
    setup_index = setup.findData("sdr_observer")
    assert setup_index >= 0
    setup.setCurrentIndex(setup_index)
    _app().processEvents()
    for step_id in ("model", "software", "connection", "guard", "schedule", "review"):
        button = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
        assert button is not None and button.isEnabled(), step_id
        button.click()
        _app().processEvents()


def test_review_shows_compact_inventory_reference_at_narrow_large_font(
    monkeypatch, tmp_path: Path
) -> None:
    def inspect(dialog: QDialog) -> None:
        font = dialog.font()
        font.setPointSize(max(18, font.pointSize()))
        dialog.setFont(font)
        dialog.resize(620, 500)
        _advance_to_review(dialog)

        card = dialog.findChild(QFrame, "guidedReviewedInventoryCard")
        status = dialog.findChild(QLabel, "guidedReviewedInventoryStatus")
        why = dialog.findChild(QLabel, "guidedReviewedInventoryWhy")
        scroll = dialog.findChild(QScrollArea)
        assert card is not None and card.isVisible()
        assert status is not None and status.wordWrap()
        assert why is not None and why.wordWrap()
        assert re.search(r"Review generation \d+ · Reference [0-9A-F ]{14}", status.text())
        assert not re.search(r"[0-9a-fA-F]{32,}", status.text())
        assert "checks this same reviewed inventory again" in why.text()
        assert scroll is not None
        assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        dialog.reject()

    assert _open_add_radio(monkeypatch, tmp_path, inspect) is None


def test_save_handoff_is_single_activation_and_returns_review_evidence(
    monkeypatch, tmp_path: Path
) -> None:
    accepted_count: list[int] = []

    def inspect(dialog: QDialog) -> None:
        _advance_to_review(dialog)
        button_box = dialog.findChild(QDialogButtonBox)
        save = button_box.button(QDialogButtonBox.Save) if button_box is not None else None
        progress_card = dialog.findChild(QFrame, "guidedSaveProgressCard")
        progress = dialog.findChild(QProgressBar, "guidedSaveProgressBar")
        assert save is not None and save.isEnabled()
        assert progress_card is not None and not progress_card.isVisible()
        assert progress is not None and (progress.minimum(), progress.maximum()) == (0, 0)
        dialog.accepted.connect(lambda: accepted_count.append(1))

        save.click()
        assert not save.isEnabled()
        assert save.text() == "Qualifying…"
        assert progress_card.isVisible()
        save.click()
        _app().processEvents()

    payload = _open_add_radio(monkeypatch, tmp_path, inspect)
    assert payload is not None
    assert accepted_count == [1]
    assert isinstance(payload["guided_inventory_generation"], int)
    assert re.fullmatch(r"[0-9a-f]{64}", payload["guided_inventory_fingerprint"])
    assert payload["guided_inventory_retry_step"] == "software"


def test_stale_review_recovery_states_nothing_changed_and_routes_to_software(
    monkeypatch, tmp_path: Path
) -> None:
    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)
    captured: dict[str, object] = {}

    def fake_exec(box: QMessageBox) -> int:
        box.show()
        _app().processEvents()
        captured["text"] = box.text()
        captured["detail"] = box.informativeText()
        buttons = {button.text(): button for button in box.buttons()}
        captured["buttons"] = buttons
        assert buttons["Review Software"].accessibleName()
        assert buttons["Review Again"].accessibleName()
        buttons["Review Software"].click()
        return 0

    monkeypatch.setattr(QMessageBox, "exec", fake_exec)
    tab = SettingsTab()
    try:
        route = tab._present_guided_stale_review_recovery(
            "The reviewed inventory is no longer current."
        )
    finally:
        tab.deleteLater()
        _app().processEvents()

    assert route == "software"
    assert "Nothing was changed" in str(captured["text"])
    assert "Review & Save" in str(captured["detail"])
