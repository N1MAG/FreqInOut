"""Focused GRS-7 package-1 tests for the Add Radio prepare-first route.

These tests intentionally exercise the public Add Radio dialog contract rather
than pixel geometry.  They protect the ordering boundary between intent,
preparation, and technical correction while preserving the existing no-write
and stale-publication guarantees.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QDialog, QGroupBox, QLabel, QLineEdit, QPushButton


STEP_IDS = ("radio", "model", "software", "connection", "guard", "schedule", "review")

_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    global _QT_APP
    app = QApplication.instance()
    if isinstance(app, QApplication):
        _QT_APP = app
    elif _QT_APP is None:
        _QT_APP = QApplication([])
    return _QT_APP


def _settings_source() -> str:
    return Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")


def _dialog_source() -> str:
    source = _settings_source()
    return source[
        source.index("    def _open_device_profile_dialog") : source.index(
            "    def _apply_runtime_projection_widgets"
        )
    ]


def _open_add_radio_dialog(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    inspect_dialog: Callable[[QDialog, object], None],
) -> None:
    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)

    tab = SettingsTab()

    def fake_exec(dialog: QDialog) -> int:
        dialog.resize(900, 560)
        dialog.show()
        _app().processEvents()
        inspect_dialog(dialog, tab)
        return dialog.result()

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    try:
        assert tab._open_device_profile_dialog(existing=None) is None
    finally:
        tab.deleteLater()
        _app().processEvents()


def _select_setup_type(dialog: QDialog, setup_type: str = "custom") -> None:
    combo = dialog.findChild(QComboBox, "guidedSetupType")
    assert combo is not None
    index = combo.findData(setup_type)
    assert index >= 0
    combo.setCurrentIndex(index)
    _app().processEvents()


def _show_software_step(dialog: QDialog) -> None:
    _select_setup_type(dialog)
    model_button = dialog.findChild(QPushButton, "guidedWizardStep_model")
    software_button = dialog.findChild(QPushButton, "guidedWizardStep_software")
    assert model_button is not None and software_button is not None
    assert model_button.isEnabled()
    model_button.click()
    _app().processEvents()
    assert software_button.isEnabled()
    software_button.click()
    _app().processEvents()


def test_prepare_first_is_the_primary_add_radio_software_action() -> None:
    source = _dialog_source()
    prepare = source.index("configure_auto_btn =")
    responsibilities = source.index("software_responsibility_group =")
    editor_tasks = source.index("software_editor_tasks =")

    # Technical responsibility/editor surfaces are downstream of preparation.
    assert prepare < responsibilities
    assert prepare < editor_tasks
    assert 'QPushButton("Prepare selected software automatically")' in source


def test_before_prepare_no_generic_admin_or_technical_editor_is_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def inspect(dialog: QDialog, _tab: object) -> None:
        _show_software_step(dialog)
        js8 = dialog.findChild(QCheckBox, "guidedSoftwareUse_js8call")
        assert js8 is not None
        js8.setChecked(True)
        _app().processEvents()

        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        assert prepare is not None
        assert prepare.text() == "Prepare selected software automatically"

        visible_text = "\n".join(
            label.text().strip()
            for label in dialog.findChildren(QLabel)
            if label.isVisible() and label.text().strip()
        ).casefold()
        assert "software administration" not in visible_text

        # No detected-app editor or expanded technical review may precede the
        # explicit preparation action.
        for object_name in ("guidedDetectedAppsReview", "guidedAppConfigReviewDetails"):
            widget = dialog.findChild(QGroupBox, object_name) or dialog.findChild(QLabel, object_name)
            if widget is not None:
                assert not widget.isVisible()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_prepared_plan_status_contract_is_explicit_and_details_start_collapsed() -> None:
    source = _dialog_source()
    normalized = source.casefold()

    for status in (
        "Ready",
        "Needs attention",
        "Discovery in progress",
        "Stale — reprepare required",
        "Show Details",
    ):
        assert status.casefold() in normalized
    assert "app_setup_plan_group.setvisible(false)" in normalized
    assert "app_config_review_details.setvisible(false)" in normalized
    assert "_update_guided_app_setup_plan_review()" in normalized


def test_source_and_family_choices_survive_back_next_before_prepare(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def inspect(dialog: QDialog, _tab: object) -> None:
        _show_software_step(dialog)
        js8 = dialog.findChild(QCheckBox, "guidedSoftwareUse_js8call")
        spotter = dialog.findChild(QCheckBox, "guidedSoftwareUse_fio_spotter")
        assert js8 is not None and spotter is not None
        js8.setChecked(True)
        spotter.setChecked(True)
        _app().processEvents()

        source_combo = dialog.findChild(QComboBox, "guidedSoftwareSource_js8call")
        assert source_combo is not None
        source_combo.setCurrentIndex(source_combo.findData("create"))
        _app().processEvents()
        selected_before = (js8.isChecked(), spotter.isChecked(), source_combo.currentData())

        for step_id in ("connection", "software", "radio", "software"):
            button = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert button is not None and button.isEnabled()
            button.click()
            _app().processEvents()

        assert (js8.isChecked(), spotter.isChecked(), source_combo.currentData()) == selected_before
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_switching_js8_from_existing_to_create_drops_the_existing_profile_bundle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def inspect(dialog: QDialog, _tab: object) -> None:
        _show_software_step(dialog)
        js8 = dialog.findChild(QCheckBox, "guidedSoftwareUse_js8call")
        source = dialog.findChild(QComboBox, "guidedSoftwareSource_js8call")
        profile = dialog.findChild(QLineEdit, "guidedJs8Profile")
        directed = dialog.findChild(QLineEdit, "guidedJs8Directed")
        assert js8 is not None and source is not None
        assert profile is not None and directed is not None
        js8.setChecked(True)
        source.setCurrentIndex(source.findData("existing"))
        profile.setText("/existing/js8/save")
        directed.setText("/existing/js8/save/DIRECTED.TXT")
        source.setCurrentIndex(source.findData("create"))
        _app().processEvents()
        assert profile.text() == ""
        assert directed.text() == ""
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_cancel_is_a_no_write_boundary_for_prepare_first_route(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def inspect(dialog: QDialog, tab: object) -> None:
        monkeypatch.setattr(
            tab.multi_radio_store,
            "save_device_profile",
            lambda *_args, **_kwargs: calls.append("radio"),
        )
        monkeypatch.setattr(
            tab.launch_orchestrator,
            "set_radio_launch_bundle",
            lambda *_args, **_kwargs: calls.append("launch"),
        )
        _show_software_step(dialog)
        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        assert prepare is not None
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)
    assert calls == []


def test_receive_only_sdr_can_prepare_without_js8call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The receiver application is valid by itself; JS8 is an optional companion."""

    def inspect(dialog: QDialog, tab: object) -> None:
        setup = dialog.findChild(QComboBox, "guidedSetupType")
        role = dialog.findChild(QComboBox, "guidedRadioRole")
        assert setup is not None and role is not None
        setup.setCurrentIndex(setup.findData("sdr_observer"))
        _app().processEvents()
        assert role.currentData() == "observer"

        model_button = dialog.findChild(QPushButton, "guidedWizardStep_model")
        software_button = dialog.findChild(QPushButton, "guidedWizardStep_software")
        assert model_button is not None and software_button is not None
        model_button.click()
        software_button.click()
        _app().processEvents()

        js8 = dialog.findChild(QCheckBox, "guidedSoftwareUse_js8call")
        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        status = dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus")
        assert js8 is not None and prepare is not None and status is not None
        js8.setChecked(False)
        monkeypatch.setattr(tab._guided_software_discovery, "register_request", lambda _request: False)

        prepare.click()
        _app().processEvents()
        assert "Select JS8Call first" not in status.text()
        assert status.text() == "Preparation could not start. Existing settings were not changed."
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_add_radio_async_publication_remains_generation_fenced_and_cancelable() -> None:
    source = _dialog_source()
    finish_start = source.index("def _finish(payload: object)")
    finish_end = source.index("def _fail(detail: str)", finish_start)
    finish_block = source[finish_start:finish_end]
    close_start = source.index("def _close_guided_discovery_session")
    close_end = source.index("def _update_commstat_binding_summary", close_start)
    close_block = source[close_start:close_end]

    assert "result_request != discovery_request" in finish_block
    assert "result_is_current(discovery_request)" in finish_block
    assert "cancel(" in close_block
    assert "close_session(guided_discovery_session_key)" in close_block
    assert "Existing settings were not changed" in finish_block
