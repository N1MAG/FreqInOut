"""Focused radio-first UX contracts for the software administration widgets."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

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
