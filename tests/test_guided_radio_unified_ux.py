"""GRS-2 contracts for the unified Add Radio and Software workflow.

These tests intentionally use operator-visible behavior and the stable guided
step object names.  They do not encode screenshot pixels or a persistence
schema that belongs to a later GRS slice.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QGroupBox,
    QLineEdit,
    QLabel,
    QPushButton,
    QScrollArea,
)

from freqinout.core.guided_setup import guided_setup_wizard_view


STEP_IDS = ("radio", "model", "software", "connection", "guard", "schedule", "review")
STEP_LABELS = (
    "Radio",
    "Operating Model",
    "Software",
    "Connections",
    "Safety",
    "Schedule",
    "Review & Save",
)


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
        source.index("    def _open_device_profile_dialog")
        : source.index("    def _apply_runtime_projection_widgets")
    ]


def _open_add_radio_dialog(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    inspect_dialog: Callable[[QDialog, object], None],
) -> None:
    """Open the authoritative Settings > Radios > Add Radio route."""

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


def _select_setup_type(dialog: QDialog, setup_type: str) -> None:
    combo = dialog.findChild(QComboBox, "guidedSetupType")
    assert combo is not None
    index = combo.findData(setup_type)
    assert index >= 0
    combo.setCurrentIndex(index)
    _app().processEvents()


def _step_buttons(dialog: QDialog) -> list[QPushButton]:
    result = [
        dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
        for step_id in STEP_IDS
    ]
    assert all(button is not None for button in result)
    return [button for button in result if button is not None]


def _visible_group_titles(dialog: QDialog) -> set[str]:
    return {
        group.title().strip()
        for group in dialog.findChildren(QGroupBox)
        if group.isVisible() and group.title().strip()
    }


def _visible_operator_text(dialog: QDialog) -> str:
    parts = [
        label.text().strip()
        for label in dialog.findChildren(QLabel)
        if label.isVisible() and label.text().strip()
    ]
    parts.extend(sorted(_visible_group_titles(dialog)))
    return "\n".join(parts)


def _advance_to_step(dialog: QDialog, target_step_id: str) -> None:
    """Walk the public step controls in order so normal gating remains active."""

    target_index = STEP_IDS.index(target_step_id)
    steps = dict(zip(STEP_IDS, _step_buttons(dialog)))
    for step_id in STEP_IDS[1 : target_index + 1]:
        button = steps[step_id]
        assert button.isEnabled(), f"{step_id} should be the next reachable guided step"
        button.click()
        _app().processEvents()


def test_shared_wizard_model_defines_the_stable_seven_step_language() -> None:
    view = guided_setup_wizard_view("radio")

    assert view.steps == tuple(zip(STEP_IDS, STEP_LABELS))
    assert view.current_step_id == "radio"
    assert view.next_label == "Operating Model"


@pytest.mark.parametrize("setup_type", ("sdr_observer", "js8_only"))
def test_add_radio_keeps_stable_labels_positions_and_keyboard_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    setup_type: str,
) -> None:
    def inspect(dialog: QDialog, _tab: object) -> None:
        _select_setup_type(dialog, setup_type)
        buttons = _step_buttons(dialog)

        assert [button.text().split(".", 1)[0] for button in buttons] == [str(index) for index in range(1, 8)]
        assert [button.text().split(".", 1)[1].strip() for button in buttons] == list(STEP_LABELS)
        assert all(button.isVisible() for button in buttons)
        assert all(button.focusPolicy() != Qt.NoFocus for button in buttons)
        assert all(button.accessibleName().strip() for button in buttons)
        assert all(button.toolTip().strip() for button in buttons)

        back = dialog.findChild(QPushButton, "guidedWizardBack")
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        button_box = dialog.findChild(QDialogButtonBox)
        cancel = button_box.button(QDialogButtonBox.Cancel) if button_box is not None else None
        assert back is not None and next_button is not None and cancel is not None
        assert all(button.focusPolicy() != Qt.NoFocus for button in (back, next_button, cancel))
        assert all(button.accessibleName().strip() for button in (back, next_button, cancel))
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_observer_uses_receiver_guard_and_receive_schedule_without_hiding_steps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def inspect(dialog: QDialog, _tab: object) -> None:
        _select_setup_type(dialog, "sdr_observer")
        steps = dict(zip(STEP_IDS, _step_buttons(dialog)))

        for step_id, required_title in (
            ("guard", "Receiver Guard"),
            ("schedule", "Receive Schedule"),
        ):
            _advance_to_step(dialog, step_id)
            button = steps[step_id]
            assert button.isEnabled()
            assert button.property("guidedStepApplicable") is True
            assert required_title in _visible_operator_text(dialog)
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_transceiver_retains_rf_guard_and_radio_schedule(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def inspect(dialog: QDialog, _tab: object) -> None:
        _select_setup_type(dialog, "js8_only")
        _advance_to_step(dialog, "guard")
        assert "RF Guard" in _visible_operator_text(dialog)
        _advance_to_step(dialog, "schedule")
        assert "Radio Schedule" in _visible_group_titles(dialog)
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_software_cards_present_source_responsibility_completion_and_launch_policy() -> None:
    dialog_source = _dialog_source()
    normalized_source = dialog_source.casefold()

    for operator_concept in (
        "Instance source",
        "Configuration responsibility",
        "Completion policy",
        "Launch policy",
    ):
        assert operator_concept.casefold() in normalized_source
    assert "create a distinct instance" in normalized_source
    assert "use an existing instance" in normalized_source
    assert "connect manually or remotely" in normalized_source
    assert "required for this radio" in normalized_source
    assert "optional capability" in normalized_source
    assert "launch with fio" in normalized_source
    assert "operator starts this application" in normalized_source


def test_navigation_preserves_edits_and_cancel_crosses_no_save_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    save_calls: list[tuple[str, object]] = []

    def inspect(dialog: QDialog, tab: object) -> None:
        monkeypatch.setattr(
            tab.multi_radio_store,
            "save_device_profile",
            lambda payload: save_calls.append(("radio", dict(payload))) or dict(payload),
        )
        monkeypatch.setattr(
            tab.launch_orchestrator,
            "set_radio_launch_bundle",
            lambda *args, **kwargs: save_calls.append(("launch", (args, kwargs))),
        )
        name_edit = next(
            edit
            for edit in dialog.findChildren(QLineEdit)
            if "radio name" in edit.placeholderText().lower()
        )
        name_edit.setText("Field Observer")
        _select_setup_type(dialog, "sdr_observer")
        steps = dict(zip(STEP_IDS, _step_buttons(dialog)))
        for step_id in ("model", "software", "connection"):
            steps[step_id].click()
            _app().processEvents()
        for step_id in ("software", "radio"):
            steps[step_id].click()
            _app().processEvents()
        assert name_edit.text() == "Field Observer"
        _advance_to_step(dialog, "review")
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)
    assert save_calls == []


def test_cancel_contract_keeps_external_writers_out_of_the_guided_dialog() -> None:
    source = _dialog_source()

    # Native changes are committed only with the final reviewed transaction.
    # A pre-save "prepare" action would make a later Cancel unable to satisfy
    # the no-write contract.
    assert "apply_guided_external_app_config_plan(" not in source


def test_software_administration_handoff_is_explicit_and_resumable() -> None:
    source = _dialog_source()

    assert "Continue in Software Administration" in source
    assert "inactive setup draft" in source
    # Add Radio must reuse the mature instance workflow, not only its terminal
    # field editor.  The shared assistant owns source, atomic identity,
    # connections, files, launch, conflict, and review decisions.
    assert "SoftwareInstanceAssistant(" in source
    assert "Apply to radio draft" in source
    normalized_source = source.casefold()
    assert "draft" in normalized_source
    assert "family" in normalized_source
    assert "resume" in normalized_source


def test_review_and_save_exposes_complete_operational_and_launch_plan() -> None:
    source = _dialog_source()
    normalized_source = source.casefold()

    assert "review & save" in normalized_source
    assert "launch plan" in normalized_source
    for required_detail in (
        "Instance source",
        "Configuration responsibility",
        "Completion policy",
        "Launch policy",
        "Effective command",
        "Working directory",
        "Dependencies",
        "Readiness policy",
        "Receiver Guard",
        "RF Guard",
    ):
        assert required_detail.casefold() in normalized_source
    assert "save radio and software" in _settings_source().casefold()


def test_guided_dialog_uses_one_responsive_scroll_owner_and_no_fixed_step_geometry() -> None:
    source = _dialog_source()
    step_block = source[
        source.index("guided_wizard_buttons: Dict[str, QPushButton]")
        : source.index("guided_wizard_detail_label = QLabel")
    ]

    assert source.count("QScrollArea(") == 1
    assert "scroll.setWidgetResizable(True)" in source
    assert "scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source
    assert "layout.addWidget(scroll, 1)" in source
    assert "guided_wizard_buttons_row = QGridLayout()" in source
    assert "setMinimumWidth(0)" in step_block
    for prohibited in ("setFixedWidth", "setFixedHeight", "setMaximumWidth", "setMaximumHeight"):
        assert prohibited not in step_block


def test_step_navigation_performs_no_synchronous_discovery_or_io() -> None:
    source = _dialog_source()
    navigation_start = source.index("def _apply_guided_wizard_visibility")
    navigation_end = source.index("def _apply_dialog_autoconfigure_results")
    navigation_block = source[navigation_start:navigation_end]
    move_start = source.index("def _set_guided_wizard_step")
    move_end = source.index("def _default_instance_port")
    move_block = source[move_start:move_end]
    combined = navigation_block + move_block

    for prohibited in (
        "coordinator.discover",
        "SoftwarePathDetector",
        "discover_js8call_file_profiles",
        "build_autoconfig_proposal",
        "socket.",
        "subprocess",
        ".read_text(",
        ".write_text(",
        "save_device_profile",
    ):
        assert prohibited not in combined
