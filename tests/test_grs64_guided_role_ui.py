"""Focused GRS-6.4 operator-language and managed-recipe UI checks."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QComboBox, QDialog, QGroupBox, QLabel, QLineEdit, QPushButton


_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    global _QT_APP
    application = QApplication.instance()
    if isinstance(application, QApplication):
        _QT_APP = application
    elif _QT_APP is None:
        _QT_APP = QApplication([])
    return _QT_APP


def _open_add_radio(monkeypatch, tmp_path, inspect_dialog, *, existing=None) -> None:
    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)

    def fake_exec(dialog: QDialog) -> int:
        dialog.resize(900, 620)
        dialog.show()
        _app().processEvents()
        inspect_dialog(dialog)
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    tab = SettingsTab()
    try:
        assert tab._open_device_profile_dialog(existing=existing) is None
    finally:
        tab.deleteLater()
        _app().processEvents()


def _select_setup(dialog: QDialog, value: str) -> None:
    combo = dialog.findChild(QComboBox, "guidedSetupType")
    assert combo is not None
    index = combo.findData(value)
    assert index >= 0
    combo.setCurrentIndex(index)
    _app().processEvents()


def _show_step(dialog: QDialog, step_id: str) -> None:
    button = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
    assert button is not None
    button.click()
    _app().processEvents()


def test_add_radio_separates_exact_role_from_fio_behavior(monkeypatch, tmp_path) -> None:
    def inspect(dialog: QDialog) -> None:
        role = dialog.findChild(QComboBox, "guidedRadioRole")
        assert role is not None
        assert [(role.itemText(index), role.itemData(index)) for index in range(role.count())] == [
            ("Transceiver", "tx_rx"),
            ("Receive-only SDR", "observer"),
        ]
        _select_setup(dialog, "sdr_observer")
        assert role.currentText() == "Receive-only SDR"

        resolved_role = dialog.findChild(QLabel, "guidedRadioRoleResolved")
        role_why = dialog.findChild(QLabel, "guidedRadioRoleWhy")
        assert resolved_role is not None and resolved_role.text() == "Resolved role: Receive-only SDR"
        assert role_why is not None and role_why.text().startswith("Why:")

        _show_step(dialog, "model")
        step = dialog.findChild(QPushButton, "guidedWizardStep_model")
        behavior = dialog.findChild(QComboBox, "guidedOperatingModel")
        resolved = dialog.findChild(QLabel, "guidedOperatingModelCapabilities")
        why = dialog.findChild(QLabel, "guidedOperatingModelWhy")
        assert step is not None and step.text() == "2. FIO Behavior"
        assert behavior is not None and behavior.accessibleName() == "FIO Behavior for this radio"
        assert behavior.count() > 0
        assert all("schedule" not in behavior.itemText(index).casefold() for index in range(behavior.count()))
        assert all("(receive-only)" not in behavior.itemText(index).casefold() for index in range(behavior.count()))
        assert resolved is not None and resolved.text().startswith("Resolved behavior:")
        assert why is not None and why.text().startswith("Why:")
        assert any(
            group.title() == "FIO Behavior" and group.isVisible()
            for group in dialog.findChildren(QGroupBox)
        )

    _open_add_radio(monkeypatch, tmp_path, inspect)


def test_existing_legacy_role_is_preserved_without_exposing_it_for_new_radios(
    monkeypatch, tmp_path
) -> None:
    def inspect(dialog: QDialog) -> None:
        role = dialog.findChild(QComboBox, "guidedRadioRole")
        assert role is not None
        assert role.currentData() == "gateway"
        assert role.currentText() == "Existing advanced role"
        assert role.itemText(0) == "Transceiver"
        assert role.itemText(1) == "Receive-only SDR"

    _open_add_radio(
        monkeypatch,
        tmp_path,
        inspect,
        existing={"id": 71, "name": "Legacy Gateway", "device_class": "gateway"},
    )


def test_managed_recipe_paths_stay_out_of_normal_connections(monkeypatch, tmp_path) -> None:
    def inspect(dialog: QDialog) -> None:
        _select_setup(dialog, "js8_only")
        for step_id in ("model", "software", "connection"):
            _show_step(dialog, step_id)

        application = dialog.findChild(QLineEdit, "guidedJs8Application")
        profile = dialog.findChild(QLineEdit, "guidedJs8Profile")
        directed = dialog.findChild(QLineEdit, "guidedJs8Directed")
        endpoint = dialog.findChild(QLineEdit, "guidedJs8Host")
        summary = dialog.findChild(QLabel, "guidedManagedRecipeSummary")
        assert application is not None and not application.isVisible()
        assert profile is not None and not profile.isVisible()
        assert directed is not None and not directed.isVisible()
        assert endpoint is not None and endpoint.isVisible()
        assert summary is not None and summary.isVisible()
        assert "shown exactly in Review" in summary.text()
        assert "Advanced only" in summary.text()

    _open_add_radio(monkeypatch, tmp_path, inspect)


def test_qualified_instance_review_uses_resolved_recipe_not_raw_field_summary() -> None:
    from freqinout.gui.software_instance_assistant import SoftwareInstanceAssistant

    assistant = SoftwareInstanceAssistant(
        "js8call",
        initial_draft={
            "family_key": "js8call",
            "mode": "managed",
            "ownership": "fio-managed",
            "instance_name": "Desk JS8",
            "owner_label": "Desk Radio",
            "draft_instance_key": "desk-radio-js8call",
            "radio_role": "tx_rx",
            "variant": "js8call_2_2",
            "version": "2.2.0",
            "application_path": "/opt/js8call",
            "host": "127.0.0.1",
            "port": 2442,
            "udp_port": 2237,
        },
        managed_root="/fio/managed-instances",
    )
    try:
        assistant._refresh_review()
        review = assistant.review_label.text()
        assert "Launch recipe" in review
        assert "Effective command:" in review
        assert "Configuration roots:" in review
        assert "Application: /opt/js8call" not in review
        assert "Settings profile:" not in review
        assert "Message data:" not in review
    finally:
        assistant.deleteLater()
        _app().processEvents()
