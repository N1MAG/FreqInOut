"""Bounded layout matrix for every Software Administration family/task."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QPoint, Qt
from PySide6.QtWidgets import QApplication, QAbstractButton, QLineEdit, QScrollArea

from freqinout.gui.software_administration_editor import SOFTWARE_EDITOR_TASKS, SoftwareTaskEditor
from freqinout.gui.software_administration_workspace import SoftwareAdministrationWorkspace
from freqinout.core.software_administration_model import build_software_administration_snapshot


_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    global _QT_APP
    if _QT_APP is None:
        _QT_APP = QApplication([])
    return _QT_APP


def _snapshot():
    """One loaded radio with every supported family represented."""
    profiles = [{
        "id": 1, "name": "Matrix Radio", "enabled": 1,
        "use_js8call": 1, "use_js8spotter": 1, "js8_instance_id": 11,
        "use_flrig": 1, "use_fldigi": 1, "use_flmsg": 1, "use_flamp": 1,
        "fast_light_config_id": 21, "use_varac": 1, "varac_node_id": 31,
        "use_commstat": 1,
    }]
    return build_software_administration_snapshot(
        profiles,
        js8_instances=[{"id": 11, "name": "Matrix JS8", "spotter_launch_path": "/opt/spotter"}],
        fast_light_configs=[{"id": 21, "name": "Matrix Fast Light"}],
        varac_nodes=[{"id": 31, "name": "Matrix VarAC"}],
    )


@pytest.mark.parametrize("family,tasks", sorted(SOFTWARE_EDITOR_TASKS.items()))
def test_every_task_has_content_and_reachable_bottom_actions(family, tasks):
    """Selected-radio editors keep their meaningful surface and actions visible."""
    for task_key in tasks:
        editor = SoftwareTaskEditor()
        try:
            editor.set_context(
                family_key=family,
                family_title=family.replace("_", " ").title(),
                task_key=task_key,
                radio_id=1,
                radio_name="Matrix Radio",
                state={},
            )
            editor.resize(900, 560)
            editor.show()
            _app().processEvents()
            assert editor.title_label.text().strip()
            assert editor.description_label.text().strip()
            editable = any(field.kind != "readonly" for field in tasks[task_key].fields)
            assert editor.save_button.isVisible() is editable
            assert editor.save_button.isEnabled() is editable
            for button in (editor.save_button, editor.task_action_button, editor.discover_button):
                if button.isVisible():
                    assert editor.rect().contains(button.geometry().bottomRight())
            assert editor.scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
            assert editor.minimumWidth() <= 900
        finally:
            editor.deleteLater()
    _app().processEvents()


@pytest.mark.parametrize(
    "family,task_key,task",
    [
        (family, task_key, task)
        for family, tasks in sorted(SOFTWARE_EDITOR_TASKS.items())
        for task_key, task in tasks.items()
        if not task.fields
    ],
)
def test_no_field_tasks_render_as_compact_action_surfaces(family, task_key, task):
    """Action-only tasks must not reserve a large empty form region."""
    editor = SoftwareTaskEditor()
    try:
        editor.set_context(
            family_key=family,
            family_title=family.replace("_", " ").title(),
            task_key=task_key,
            radio_id=1,
            radio_name="Matrix Radio",
            state={},
        )
        editor.resize(900, 560)
        editor.show()
        _app().processEvents()

        assert editor.scroll.isHidden()
        assert editor.root_layout.stretch(editor.root_layout.indexOf(editor.scroll)) == 0
        assert editor.form.rowCount() == 0
        assert editor.save_button.isHidden()
        assert editor.dirty_label.isHidden()
        if task.action:
            assert editor.task_action_button.isVisible()
            assert editor.task_action_button.text() == task.action_label
            assert editor.task_action_button.geometry().top() - editor.status_label.geometry().bottom() <= 12
        else:
            assert editor.task_action_button.isHidden()
        assert editor.description_label.geometry().top() - editor.title_label.geometry().bottom() <= 12
        assert editor.status_label.geometry().top() - editor.description_label.geometry().bottom() <= 12
        for button in (editor.task_action_button, editor.save_button):
            if button.isVisible():
                assert editor.rect().contains(button.geometry().bottomRight())
        # A no-field editor's size hint should be driven by its copy and
        # action row, not by a hidden scroll area's former minimum height.
        assert editor.sizeHint().height() < 240
    finally:
        editor.deleteLater()
    _app().processEvents()


def test_unverified_status_uses_plain_language_in_software_workspace():
    """Neutral status explains that verification has not happened yet."""
    workspace = SoftwareAdministrationWorkspace()
    try:
        workspace.set_snapshot(_snapshot())
        workspace.select_context("js8call", 1)
        _app().processEvents()
        assert "Not yet verified" in workspace.context_banner.text()
        assert "Not checked" not in workspace.context_banner.text()

        workspace.show_family_summary(_snapshot().family("js8call"))
        _app().processEvents()
        summary = workspace.editor_placeholder.text()
        assert "Not yet verified" in summary
        assert "Not checked" not in summary
        assert all(
            "Not checked" not in button.text()
            for button in workspace.findChildren(QAbstractButton)
        )
    finally:
        workspace.deleteLater()
        _app().processEvents()


@pytest.mark.parametrize("theme", ["light", "dark"])
@pytest.mark.parametrize("text_size", ["normal", "large"])
@pytest.mark.parametrize("size", [(1920, 1080), (1000, 700), (900, 560)])
def test_all_radio_context_is_summary_ready_and_workspace_stays_bounded(theme, text_size, size):
    """All-radio mode cannot save a radio-scoped draft and never grows the shell."""
    workspace = SoftwareAdministrationWorkspace()
    editor = SoftwareTaskEditor()
    try:
        workspace.set_snapshot(_snapshot())
        workspace.apply_theme({"theme": theme, "ui_text_size": text_size})
        workspace.resize(*size)
        workspace.select_context("js8call")
        workspace.show()
        _app().processEvents()
        assert workspace.selected_radio_id() is None
        assert "all radios" in workspace.context_banner.text().lower()
        assert workspace.minimumWidth() <= size[0]
        assert workspace.minimumHeight() <= size[1]
        assert all(scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
                   for scroll in workspace.findChildren(QScrollArea))

        workspace.show_family_summary(_snapshot().family("js8call"))
        _app().processEvents()
        assert "JS8Call across 1 assigned radio" in workspace.editor_placeholder.text()
        assert "Choose a radio" in workspace.editor_placeholder.text()
        assert all(
            not button.isEnabled()
            for key, button in workspace._task_buttons.items()
            if key != "overview"
        )
        assert workspace.task_strip.isHidden()
    finally:
        editor.deleteLater()
        workspace.deleteLater()
        _app().processEvents()


def test_compact_overflow_scrollbar_has_its_own_lane_below_family_chips():
    """Regression: the family scrollbar must not cover the selected chip."""

    workspace = SoftwareAdministrationWorkspace()
    try:
        workspace.set_snapshot(_snapshot())
        workspace.resize(900, 560)
        workspace.select_context("js8call")
        workspace.show()
        _app().processEvents()

        strip = workspace.family_strip
        scrollbar = strip.horizontalScrollBar()
        assert scrollbar.maximum() > scrollbar.minimum()
        assert scrollbar.isVisible()
        scrollbar_top = scrollbar.mapTo(strip, QPoint(0, 0)).y()
        selected = workspace._family_buttons["js8call"]
        selected_bottom = selected.mapTo(strip, selected.rect().bottomLeft()).y()
        assert selected_bottom < scrollbar_top
        assert strip.height() >= selected.height() + scrollbar.height()
    finally:
        workspace.deleteLater()
        _app().processEvents()


def test_selected_family_task_matrix_is_available_for_every_family():
    workspace = SoftwareAdministrationWorkspace()
    try:
        workspace.set_snapshot(_snapshot())
        for family, tasks in sorted(SOFTWARE_EDITOR_TASKS.items()):
            workspace.select_context(family, 1)
            _app().processEvents()
            assert workspace.selected_radio_id() == 1
            for task_key, task in tasks.items():
                workspace.select_context(family, 1, task_key)
                _app().processEvents()
                assert workspace.selected_task_key() == task_key
                assert task_key in workspace._task_buttons
                assert workspace._task_buttons[task_key].accessibleName().strip()
    finally:
        workspace.deleteLater()
        _app().processEvents()
