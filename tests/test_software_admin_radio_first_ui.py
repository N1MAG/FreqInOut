"""Focused radio-first UX contracts for the software administration widgets."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QComboBox, QDialog, QPushButton

from freqinout.core.software_administration_model import build_software_administration_snapshot
from freqinout.gui.software_administration_workspace import SoftwareAdministrationWorkspace
from freqinout.gui.software_instance_assistant import SoftwareInstanceAssistant


def _app() -> QApplication:
    app = QApplication.instance()
    return app if isinstance(app, QApplication) else QApplication([])


def test_chip_groups_are_exclusive_and_repeated_click_keeps_selection() -> None:
    _app()
    workspace = SoftwareAdministrationWorkspace()
    try:
        workspace.set_snapshot(
            build_software_administration_snapshot(
                [{"id": 1, "name": "North", "enabled": 1, "use_js8call": 1, "js8_instance_id": 2}],
                js8_instances=[{"id": 2, "name": "North JS8"}],
            )
        )
        workspace.select_context("js8call", 1, "overview")
        workspace._family_buttons["js8call"].click()
        workspace._radio_buttons[1].click()
        workspace._task_buttons["overview"].click()
        assert workspace.selected_family_key() == "js8call"
        assert workspace.selected_radio_id() == 1
        assert workspace.selected_task_key() == "overview"
        assert workspace._family_buttons["js8call"].isChecked()
        assert workspace._radio_buttons[1].isChecked()
        assert workspace._task_buttons["overview"].isChecked()
    finally:
        workspace.deleteLater()


def test_assistant_locks_launched_family_and_renders_available_vs_assigned() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        radios=(
            {"id": 1, "name": "North", "js8_instance_id": 20},
            {"id": 2, "name": "South"},
        ),
        existing_instances=({"id": 20, "name": "North JS8", "port": 2442},),
    )
    try:
        assert assistant.family_combo.isEnabled() is False
        labels = [assistant.radio_combo.itemText(i) for i in range(assistant.radio_combo.count())]
        assert any("North — Assigned to North JS8" in label for label in labels)
        assert any("South — Available" in label for label in labels)
        assert "Not assigned yet" not in " ".join(labels)
    finally:
        assistant.deleteLater()


def test_available_observer_radio_enables_next_for_distinct_js8_instance() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        radios=(
            {"id": 1, "name": "Primary", "device_class": "tx_rx", "js8_instance_id": 20},
            {"id": 2, "name": "RTL-SDR", "device_class": "observer", "js8_instance_id": None},
        ),
        existing_instances=({"id": 20, "name": "Primary JS8", "port": 2442},),
    )
    try:
        observer_index = assistant.radio_combo.findData(2)
        assert observer_index >= 0
        assistant.radio_combo.setCurrentIndex(observer_index)

        assert assistant.radio_combo.currentText() == "RTL-SDR — Available"
        assert assistant.next_button.isEnabled() is True
        assert assistant.draft().radio_id == 2
        assert assistant.draft().replace_existing is False
        assert assistant.draft().replacement_instance_id is None

        assistant.next_button.click()
        assert assistant._step == 1
    finally:
        assistant.deleteLater()


def test_single_available_radio_is_selected_and_all_setup_steps_are_visible() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        radios=(
            {"id": 2, "name": "RTL-SDR", "device_class": "observer", "js8_instance_id": None},
        ),
    )
    try:
        assert assistant.radio_combo.currentData() == 2
        assert assistant.radio_combo.currentText() == "RTL-SDR — Available"
        assert assistant.next_button.isEnabled() is True
        assert assistant.back_button.isEnabled() is False

        assert len(assistant.step_buttons) == len(assistant.STEP_TITLES) == 7
        assert [button.text() for button in assistant.step_buttons] == [
            "1. Purpose",
            "2. Find or create",
            "3. Identity",
            "4. Connections",
            "5. Files",
            "6. Launch",
            "7. Review",
        ]
        assert all(not button.isHidden() for button in assistant.step_buttons)
        assert assistant.step_buttons[0].isChecked() is True
        assert assistant.step_buttons[1].isEnabled() is True
        assert all(not button.isEnabled() for button in assistant.step_buttons[2:])

        assistant.step_buttons[0].click()
        assert assistant.step_buttons[0].isChecked() is True

        assistant.step_buttons[1].click()
        assert assistant._step == 1
        assert assistant.pages.currentIndex() == 1
        assert assistant.back_button.isEnabled() is True
        assert assistant.step_buttons[1].isChecked() is True
    finally:
        assistant.deleteLater()


def test_step_navigator_remains_visible_at_compact_supported_size() -> None:
    app = _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        radios=({"id": 2, "name": "RTL-SDR", "device_class": "observer"},),
    )
    try:
        assistant.resize(900, 560)
        assistant.show()
        app.processEvents()

        assert all(button.isVisible() for button in assistant.step_buttons)
        content_right = assistant.contentsRect().right()
        assert all(button.mapTo(assistant, button.rect().topRight()).x() <= content_right for button in assistant.step_buttons)
        assert assistant.next_button.isVisible() is True
    finally:
        assistant.close()
        assistant.deleteLater()


def test_multiple_available_radios_still_require_an_explicit_choice() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        radios=(
            {"id": 2, "name": "RTL-SDR A", "device_class": "observer"},
            {"id": 3, "name": "RTL-SDR B", "device_class": "observer"},
        ),
    )
    try:
        assert assistant.radio_combo.currentData() is None
        assert assistant.next_button.isEnabled() is False
        assert assistant.step_buttons[1].isEnabled() is False

        assistant.radio_combo.setCurrentIndex(assistant.radio_combo.findData(3))

        assert assistant.radio_combo.currentData() == 3
        assert assistant.next_button.isEnabled() is True
        assert assistant.step_buttons[1].isEnabled() is True
    finally:
        assistant.deleteLater()


def test_selected_observer_opens_on_purpose_step_with_radio_context_ready() -> None:
    """A radio-scoped launch must visibly start with the supplied receiver selected."""
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        radios=(
            {"id": 1, "name": "Primary", "device_class": "tx_rx", "js8_instance_id": 20},
            {"id": 2, "name": "RTL-SDR", "device_class": "observer", "js8_instance_id": None},
        ),
        existing_instances=({"id": 20, "name": "Primary JS8", "port": 2442},),
        selected_radio_id=2,
    )
    try:
        assert assistant._step == 0
        assert assistant.step_label.text() == "Step 1 of 7 · Purpose"
        assert assistant.radio_combo.currentData() == 2
        assert assistant.radio_combo.currentText() == "RTL-SDR — Available"
        # The assistant is intentionally not shown in this widget-only test;
        # assert the action is present rather than asking Qt for visibility in
        # an undisplayed parent hierarchy.
        assert assistant.next_button.isHidden() is False
        assert assistant.next_button.isEnabled() is True
        assert "distinct" in assistant.guidance_label.text().lower()
    finally:
        assistant.deleteLater()


def test_workspace_can_begin_js8_setup_for_unassigned_observer_radio() -> None:
    _app()
    workspace = SoftwareAdministrationWorkspace()
    try:
        radios = (
            {"id": 1, "name": "Primary", "device_class": "tx_rx", "js8_instance_id": 20},
            {"id": 2, "name": "RTL-SDR", "device_class": "observer", "js8_instance_id": None},
        )
        workspace.set_instance_context(
            radios=radios,
            inventory_by_family={"js8call": ({"id": 20, "name": "Primary JS8"},)},
        )
        workspace.set_snapshot(
            build_software_administration_snapshot(
                radios,
                js8_instances=[{"id": 20, "name": "Primary JS8"}],
            )
        )

        assert workspace.begin_instance_setup("js8call", 2) is True
        assistant = workspace._instance_assistant
        assert assistant is not None
        assert assistant.draft().radio_id == 2
        assert assistant.draft().replace_existing is False
        assert assistant.next_button.isEnabled() is True
    finally:
        workspace.deleteLater()


@pytest.mark.parametrize("size", [(1920, 1080), (900, 560)])
def test_workspace_add_action_opens_sdr_assistant_with_sole_radio_ready(size: tuple[int, int]) -> None:
    """The public Add action must carry a sole available SDR into the assistant."""
    app = _app()
    workspace = SoftwareAdministrationWorkspace()
    radios = ({"id": 2, "name": "RTL-SDR", "device_class": "observer", "js8_instance_id": None},)
    try:
        workspace.set_instance_context(radios=radios, inventory_by_family={"js8call": ()})
        workspace.set_snapshot(build_software_administration_snapshot(radios, js8_instances=()))
        workspace.resize(*size)
        workspace.show()
        app.processEvents()

        # Exercise the same public button route used by Settings, rather than
        # constructing the assistant directly or calling its private opener.
        workspace.add_instance_button.click()
        app.processEvents()
        assistant = workspace._instance_assistant
        assert assistant is not None
        assert assistant.step_label.text().strip()
        assert len(assistant.step_buttons) == 7
        assert all(button.isVisible() for button in assistant.step_buttons)
        assert assistant.radio_combo.currentData() == 2
        assert assistant.next_button.isEnabled() is True
    finally:
        workspace.close()
        workspace.deleteLater()


def test_occupied_radio_requires_explicit_replacement_and_review_comparison() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        radios=({"id": 1, "name": "North", "js8_instance_id": 20},),
        existing_instances=({"id": 20, "name": "Old JS8", "port": 2442},),
    )
    try:
        assistant.radio_combo.setCurrentIndex(1)
        assert assistant.next_button.isEnabled() is False
        assert "Replacement mode required" in assistant.replacement_banner.text()
        assistant.replacement_checkbox.setChecked(True)
        assistant._field_widgets["instance_name"].setText("New JS8")
        assistant._step = len(assistant.STEP_TITLES) - 1
        assistant._refresh()
        assert "Replacement comparison:" in assistant.review_label.text()
        assert "Old JS8" in assistant.review_label.text()
        assert assistant.draft().payload()["replace_existing"] is True
    finally:
        assistant.deleteLater()


def test_empty_radio_inventory_guides_to_create_radio_and_emits_route_signal() -> None:
    _app()
    assistant = SoftwareInstanceAssistant("varac")
    try:
        events: list[str] = []
        assistant.create_radio_requested.connect(lambda: events.append("create"))
        assert "No radios exist yet" in assistant.radio_guidance_label.text()
        assert assistant.next_button.isEnabled() is False
        assistant.create_radio_button.click()
        assert events == ["create"]
    finally:
        assistant.deleteLater()


def test_workspace_primary_action_routes_directly_to_create_radio_when_station_is_empty() -> None:
    _app()
    workspace = SoftwareAdministrationWorkspace()
    try:
        workspace.set_instance_context(radios=(), inventory_by_family={})
        workspace.set_snapshot(build_software_administration_snapshot([]))
        events: list[str] = []
        workspace.create_radio_requested.connect(lambda: events.append("create"))

        assert workspace.add_instance_button.text() == "Create a radio first…"
        workspace.add_instance_button.click()

        assert events == ["create"]
        assert workspace._instance_assistant is None
    finally:
        workspace.deleteLater()


@pytest.mark.parametrize("device_class", ["observer", "tx_rx"])
@pytest.mark.parametrize("size", [(1920, 1080), (900, 560)])
def test_settings_add_radio_dialog_keeps_guided_steps_available(
    monkeypatch, device_class: str, size: tuple[int, int]
) -> None:
    """Settings' Add Radio action must expose the complete guided navigator."""
    from freqinout.gui.settings_tab import SettingsTab

    app = _app()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    seen: dict[str, object] = {}

    def _inspect_dialog(dialog: QDialog) -> int:
        dialog.resize(*size)
        dialog.show()
        app.processEvents()
        if device_class == "observer":
            model_combo = next(
                (
                    combo for combo in dialog.findChildren(QComboBox)
                    if combo.isEditable()
                    and combo.lineEdit() is not None
                    and "radio model" in combo.lineEdit().placeholderText().lower()
                ),
                None,
            )
            assert model_combo is not None
            model_index = next(
                (
                    index for index in range(model_combo.count())
                    if "RTLSDR" in model_combo.itemText(index).upper()
                ),
                -1,
            )
            if model_index >= 0:
                model_combo.setCurrentIndex(model_index)
            else:
                # The local Hamlib catalog is optional. Typing the same model
                # keeps this production-route test deterministic in CI.
                model_combo.setEditText("RTLSDR")
            app.processEvents()
        if device_class == "observer":
            setup_combo = dialog.findChild(QComboBox, "guidedSetupType")
            assert setup_combo is not None
            setup_index = setup_combo.findData("sdr_observer")
            assert setup_index >= 0
            setup_combo.setCurrentIndex(setup_index)
            app.processEvents()
            role_combo = next(
                combo for combo in dialog.findChildren(QComboBox)
                if combo.findData("observer") >= 0
            )
            assert role_combo.currentData() == "observer"
        step_buttons = {
            button.objectName(): button
            for button in dialog.findChildren(QPushButton)
            if button.objectName().startswith("guidedWizardStep_")
        }
        seen["step_buttons"] = step_buttons
        seen["step_text"] = dialog.findChild(QPushButton, "guidedWizardStep_radio").text()
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", _inspect_dialog)
    tab = SettingsTab()
    try:
        tab.resize(*size)
        tab.show()
        app.processEvents()
        tab.add_device_profile_btn.click()
        app.processEvents()
        step_buttons = seen["step_buttons"]
        assert isinstance(step_buttons, dict)
        assert len(step_buttons) == 7
        assert all(button.isVisible() for button in step_buttons.values())
        assert [button.text().split(".", 1)[0] for button in sorted(step_buttons.values(), key=lambda item: int(item.text().split(".", 1)[0]))] == [str(index) for index in range(1, 8)]
        assert seen["step_text"]
        assert step_buttons["guidedWizardStep_model"].isVisible()
        assert step_buttons["guidedWizardStep_connection"].isVisible()
        # Non-applicable controls remain discoverable in the same seven-step
        # navigator and communicate their inactive state rather than vanishing.
        inactive_ids = (
            ("guidedWizardStep_model",) if device_class == "tx_rx" else
            ("guidedWizardStep_guard", "guidedWizardStep_schedule")
        )
        for step_id in inactive_ids:
            button = step_buttons[step_id]
            assert button.isEnabled() is False or any(
                marker in button.text().lower() for marker in ("n/a", "not applicable", "not used")
            )
        if device_class == "observer":
            assert "N/A" not in step_buttons["guidedWizardStep_model"].text()
            assert "N/A" not in step_buttons["guidedWizardStep_connection"].text()
            assert "N/A" in step_buttons["guidedWizardStep_guard"].text()
            assert "N/A" in step_buttons["guidedWizardStep_schedule"].text()
        navigator = step_buttons["guidedWizardStep_radio"].parentWidget()
        assert navigator is not None
        assert all(button.geometry().right() <= navigator.rect().right() for button in step_buttons.values())
        assert all(button.geometry().bottom() <= navigator.rect().bottom() for button in step_buttons.values())
        buttons = list(step_buttons.values())
        assert all(not buttons[left].geometry().intersects(buttons[right].geometry()) for left in range(len(buttons)) for right in range(left + 1, len(buttons)))
    finally:
        tab.deleteLater()
        app.processEvents()


def test_workspace_radio_chip_names_ownership_and_occupied_action_is_replace() -> None:
    _app()
    workspace = SoftwareAdministrationWorkspace()
    try:
        radios = ({"id": 1, "name": "North", "use_js8call": 1, "js8_instance_id": 20},)
        workspace.set_instance_context(
            radios=radios,
            inventory_by_family={"js8call": ({"id": 20, "name": "North JS8"},)},
        )
        workspace.set_snapshot(
            build_software_administration_snapshot(
                radios,
                js8_instances=[{"id": 20, "name": "North JS8"}],
            )
        )
        workspace.select_context("js8call", 1, "overview")

        assert workspace._radio_buttons[1].text() == "North — Assigned: North JS8"
        assert workspace.add_instance_button.text() == "Replace instance…"
    finally:
        workspace.deleteLater()


def test_each_selector_stays_exactly_one_checked_through_repeated_clicks_and_family_changes() -> None:
    """MIS-5 selectors are non-empty exclusive groups, including re-clicks."""
    _app()
    workspace = SoftwareAdministrationWorkspace()
    try:
        snapshot = build_software_administration_snapshot(
            [
                {
                    "id": 1, "name": "North", "enabled": 1,
                    "use_js8call": 1, "js8_instance_id": 11,
                    "use_flrig": 1, "use_fldigi": 1, "fast_light_config_id": 21,
                    "use_varac": 1, "varac_node_id": 31,
                },
                {"id": 2, "name": "South", "enabled": 1},
            ],
            js8_instances=[{"id": 11, "name": "North JS8"}],
            fast_light_configs=[{"id": 21, "name": "North Fast"}],
            varac_nodes=[{"id": 31, "name": "North VarAC"}],
        )
        workspace.set_snapshot(snapshot)
        for family in ("js8call", "fast_light", "varac", "js8call"):
            workspace.select_context(family, 1, "overview")
            # Re-select every active chip several times, then switch radio/task.
            for _ in range(3):
                workspace._family_buttons[family].click()
                workspace._radio_buttons[1].click()
                workspace._task_buttons["overview"].click()
            assert sum(button.isChecked() for button in workspace._family_buttons.values()) == 1
            assert sum(button.isChecked() for button in workspace._radio_buttons.values()) == 1
            assert sum(button.isChecked() for button in workspace._task_buttons.values()) == 1
            assert workspace.selected_family_key() == family
            assert workspace.selected_radio_id() == 1
            assert workspace.selected_task_key() == "overview"
    finally:
        workspace.deleteLater()


def test_assign_existing_is_family_filtered_and_replacement_is_explicit() -> None:
    """Recovery assignment exposes only unassigned rows and requires review."""
    _app()
    workspace = SoftwareAdministrationWorkspace()
    try:
        workspace.set_snapshot(
            build_software_administration_snapshot(
                [{"id": 1, "name": "North", "enabled": 1, "use_js8call": 1, "js8_instance_id": 10}],
                js8_instances=[
                    {"id": 10, "name": "Current JS8"},
                    {"id": 11, "name": "Retained JS8"},
                ],
                varac_nodes=[{"id": 12, "name": "Retained VarAC"}],
            )
        )
        workspace.set_instance_context(
            radios=({"id": 1, "name": "North", "js8_instance_id": 10},),
            inventory_by_family={
                "js8call": (
                    {"id": 10, "name": "Current JS8"},
                    {"id": 11, "name": "Retained JS8"},
                ),
                "varac": ({"id": 12, "name": "Retained VarAC"},),
            },
        )
        workspace.select_context("js8call", 1, "overview")
        events = []
        workspace.assign_existing_requested.connect(events.append)
        workspace.assign_button.click()
        assistant = workspace._instance_assistant
        assert assistant is not None
        assert events == [{"family_key": "js8call", "radio_id": 1}]
        assert assistant.discovery_list.count() == 1
        assert assistant.discovery_list.item(0).text().startswith("Retained JS8")
        assert assistant.radio_combo.currentData() == 1
        assert assistant.replacement_checkbox.isHidden() is False
        assert assistant.replacement_checkbox.isChecked() is False
        assistant.replacement_checkbox.setChecked(True)
        assert assistant.draft().replace_existing is True
        assert assistant.draft().replacement_instance_id == 10
    finally:
        workspace.deleteLater()


def test_navigation_and_repaint_are_cache_only_and_bounded() -> None:
    """Selection/repaint must not introduce I/O and remains bounded at compact size."""
    import inspect
    import freqinout.gui.software_administration_workspace as module
    from PySide6.QtWidgets import QScrollArea

    source = inspect.getsource(module)
    assert not any(token in source for token in ("subprocess", "QProcess", "socket", "sqlite3", "requests."))
    workspace = SoftwareAdministrationWorkspace()
    try:
        workspace.set_snapshot(build_software_administration_snapshot([{"id": 1, "name": "North", "enabled": 1}]))
        for size in ((1920, 1080), (1000, 700), (900, 560)):
            workspace.resize(*size)
            workspace.select_context("js8call")
            workspace.repaint()
            _app().processEvents()
            assert workspace.size().width() == size[0]
            assert workspace.minimumWidth() <= size[0]
            assert workspace.minimumHeight() <= size[1]
            assert all(
                scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
                for scroll in workspace.findChildren(QScrollArea)
            )
    finally:
        workspace.deleteLater()
