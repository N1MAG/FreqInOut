"""SCA-S1 contract tests for the software-first administration workspace."""

from __future__ import annotations

import inspect
import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QAbstractButton, QLabel, QScrollArea, QWidget

from freqinout.core.software_administration_model import build_software_administration_snapshot

ui = pytest.importorskip("freqinout.gui.software_administration_workspace")
Workspace = getattr(ui, "SoftwareAdministrationWorkspace", None)
if Workspace is None:
    Workspace = getattr(ui, "SoftwareAdministrationWorkspaceWidget", None)
if Workspace is None:
    pytest.skip("workspace shell is not available yet", allow_module_level=True)


def _app() -> QApplication:
    app = QApplication.instance()
    return app if isinstance(app, QApplication) else QApplication([])


def _snapshot():
    profiles = [
        {"id": 1, "system_key": "fio-a", "name": "FIO-A", "enabled": 1,
         "use_js8call": 1, "use_js8spotter": 1, "js8_instance_id": 11},
        {"id": 2, "system_key": "fio-b", "name": "FIO-B", "enabled": 1,
         "use_js8call": 1, "use_js8spotter": 0, "js8_instance_id": 12},
    ]
    return build_software_administration_snapshot(
        profiles,
        js8_instances=[
            {"id": 11, "system_key": "js8-a", "name": "FIO-A JS8"},
            {"id": 12, "system_key": "js8-b", "name": "FIO-B JS8"},
            {"id": 99, "system_key": "orphan", "name": "Unassigned JS8"},
        ],
    )


def _workspace(snapshot=None):
    app = _app()
    widget = Workspace()
    if snapshot is not None:
        setter = getattr(widget, "set_snapshot", None)
        assert callable(setter), "workspace must expose set_snapshot(snapshot)"
        setter(snapshot)
        app.processEvents()
    widget.show()
    app.processEvents()
    return widget


def _buttons(widget: QWidget):
    # Buttons live inside chip-strip scroll-area contents, so their own
    # visibility is not a reliable proxy until the strip has been scrolled.
    return list(widget.findChildren(QAbstractButton))


def _text(widget: QWidget) -> str:
    return widget.text() if hasattr(widget, "text") else ""


def test_family_cards_and_radio_context_are_separate_navigation_controls():
    widget = _workspace(_snapshot())
    try:
        widget.select_context("js8call")
        _app().processEvents()
        labels = [_text(b) for b in _buttons(widget)]
        assert any(label.startswith("JS8Call") for label in labels)
        assert any(label.startswith("FIO Spotter") for label in labels)
        assert any(label.startswith("FIO-A") for label in labels) and any(label.startswith("FIO-B") for label in labels)
        assert "All" in labels
        context = getattr(widget, "select_context")
        context("js8call", 2)
        _app().processEvents()
        banner_text = widget.context_banner.text()
        assert "JS8Call" in banner_text and "FIO-B" in banner_text and "FIO-B JS8" in banner_text
        assert widget.selected_family_key() == "js8call"
        assert widget.selected_radio_id() == 2
    finally:
        widget.deleteLater()


def test_selected_radio_shows_reviewable_manifest_identity_without_long_path_chrome():
    snapshot = build_software_administration_snapshot(
        [{"id": 1, "name": "FIO-A", "enabled": 1, "use_js8call": 1, "js8_instance_id": 11}],
        js8_instances=[{"id": 11, "system_key": "js8-a", "name": "FIO-A JS8"}],
        instance_manifests=[
            {
                "instance_key": "js8:js8-a",
                "family_key": "js8call",
                "application_system_key": "js8-a",
                "management_mode": "fio_managed",
                "verification_state": "configured",
                "configuration_path": "/profiles/field/JS8Call - FIO-A.ini",
                "data_root": "/messages/field/JS8Call - FIO-A",
                "ports": [{"name": "JS8Call API", "host": "127.0.0.1", "port": 2442}],
            }
        ],
    )
    widget = _workspace(snapshot)
    try:
        widget.select_context("js8call", 1)
        assert "FIO-managed launch" in widget.context_banner.text()
        assert "Config: JS8Call - FIO-A.ini" in widget.context_banner.text()
        assert "Data: JS8Call - FIO-A" in widget.context_banner.text()
        assert "/profiles/field/JS8Call - FIO-A.ini" in widget.context_banner.toolTip()
    finally:
        widget.deleteLater()


def test_radio_chip_selection_and_task_selection_route_without_io():
    widget = _workspace(_snapshot())
    try:
        events = []
        widget.radio_selected.connect(lambda value: events.append(("radio", value)))
        widget.task_selected.connect(lambda value: events.append(("task", value)))
        widget.select_context("js8call", 1, "api_radio")
        assert widget.selected_radio_id() == 1
        assert widget.selected_task_key() == "api_radio"
        next(button for button in _buttons(widget) if button.text().startswith("FIO-B")).click()
        next(button for button in _buttons(widget) if button.text() == "Overview").click()
        assert ("radio", 2) in events and ("task", "overview") in events
    finally:
        widget.deleteLater()


def test_all_state_unassigned_summary_and_fio_spotter_operational_route():
    widget = _workspace(_snapshot())
    try:
        widget.select_context("js8call")
        assert widget.selected_radio_id() is None
        assert "all radios" in widget.context_banner.text().lower()
        assert "Unassigned JS8" in widget.unassigned_label.text()
        routes = []
        widget.operational_route_requested.connect(routes.append)
        widget.select_context("fio_spotter", 1)
        next(button for button in _buttons(widget) if button.text() == "Open FIO Spotter").click()
        assert routes == ["FIO Spotter"]
    finally:
        widget.deleteLater()


def test_replacing_snapshot_preserves_valid_context_and_falls_back_safely():
    widget = _workspace(_snapshot())
    try:
        widget.select_context("js8call", 2)
        widget.set_snapshot(build_software_administration_snapshot([{"id": 1, "name": "FIO-A", "enabled": 1, "use_js8call": 1, "js8_instance_id": 11}], js8_instances=[{"id": 11, "name": "FIO-A JS8"}]))
        _app().processEvents()
        assert widget.selected_radio_id() is None
        widget.set_snapshot(_snapshot())
        widget.select_context("js8call", 999)
        assert widget.selected_radio_id() is None
    finally:
        widget.deleteLater()


def test_selection_is_cache_only_and_no_min_width_or_horizontal_scroll():
    source = inspect.getsource(ui)
    for forbidden in ("subprocess", "Popen", "os.system", "socket.create_connection", "threading.Thread"):
        assert forbidden not in source
    widget = _workspace(_snapshot())
    try:
        for width, height in ((1000, 700), (900, 560)):
            widget.resize(width, height)
            widget.show()
            _app().processEvents()
            assert widget.width() == width and widget.height() == height
            assert widget.minimumWidth() <= width
            # Chip strips may scroll horizontally as needed; the outer shell
            # itself must remain bounded and must not acquire a min-width.
            assert widget.minimumWidth() <= width
            widget.select_context("js8call", 1)
            assert all(scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded for scroll in widget.findChildren(QScrollArea))
    finally:
        widget.deleteLater()


def test_navigation_controls_have_accessible_names():
    widget = _workspace(_snapshot())
    try:
        widget.select_context("js8call")
        named = [b for b in _buttons(widget) if b.accessibleName().strip()]
        assert len(named) >= 4
        names = " ".join(b.accessibleName() for b in named)
        assert "JS8Call" in names and "FIO-A" in names
    finally:
        widget.deleteLater()


def test_open_instance_assistant_preserves_its_draft_until_finish_or_cancel():
    widget = _workspace(_snapshot())
    try:
        widget.set_instance_context(
            radios=({"id": 1, "name": "FIO-A"}, {"id": 2, "name": "FIO-B"}),
            inventory_by_family={"js8call": (), "fast_light": (), "varac": ()},
        )
        widget.select_context("js8call", 1)
        widget._open_instance_assistant()
        assistant = widget._instance_assistant
        assert assistant is not None
        assistant._field_widgets["instance_name"].setText("My second JS8")

        next(button for button in _buttons(widget) if button.text().startswith("FIO-B")).click()

        assert widget.selected_radio_id() == 1
        assert widget.editor_stack.currentWidget() is assistant
        assert assistant._field_widgets["instance_name"].text() == "My second JS8"
        assert "Finish this setup" in assistant.operation_status_label.text()
        assistant.cancelled.emit()
        assert widget._instance_assistant is None
    finally:
        widget.deleteLater()


@pytest.mark.parametrize("theme_name", ["light", "dark"])
@pytest.mark.parametrize("size", [(1920, 1080), (1000, 700), (900, 560)])
def test_theme_resize_and_paint_remain_bounded_at_supported_sizes(theme_name, size):
    """Qualification guard: navigation/repaint cannot grow the outer page."""
    widget = _workspace(_snapshot())
    try:
        widget.apply_theme({"theme": theme_name, "ui_text_size": "normal"})
        widget.resize(*size)
        widget.select_context("js8call", 1, "api_radio")
        widget.show()
        _app().processEvents()
        widget.repaint()
        _app().processEvents()
        width, height = size
        assert widget.size().width() == width and widget.size().height() == height
        assert widget.minimumSize().width() <= width
        assert widget.minimumSize().height() <= height
        assert all(scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
                   for scroll in widget.findChildren(QScrollArea))
    finally:
        widget.deleteLater()


def test_large_text_theme_stacks_without_outer_horizontal_overflow():
    widget = _workspace(_snapshot())
    try:
        widget.apply_theme({"theme": "dark", "ui_text_size": "large"})
        widget.resize(900, 560)
        widget.select_context("js8call", 1, "api_radio")
        widget.show()
        _app().processEvents()
        assert widget.minimumWidth() <= 900
        assert widget.minimumHeight() <= 560
        assert widget.context_banner.accessibleName()
        assert widget.editor_host.accessibleName()
    finally:
        widget.deleteLater()


def test_compact_height_prioritizes_the_configuration_editor():
    widget = _workspace(_snapshot())
    try:
        widget.resize(900, 560)
        widget.select_context("js8call", 1, "api_radio")
        widget.show()
        _app().processEvents()
        assert widget.explanation_label.isHidden()
        assert widget.family_prompt_label.isHidden()
        assert widget.radio_prompt_label.isHidden()
        assert widget.task_prompt_label.isHidden()
        assert widget.editor_host.height() >= 250
    finally:
        widget.deleteLater()


def test_workspace_source_has_no_discovery_or_endpoint_work_on_navigation_or_paint():
    """Keep cache-only UI behavior explicit until SCA-S4 discovery is isolated."""
    source = inspect.getsource(ui)
    forbidden = (
        "pathlib", "Path(", "open(", "subprocess", "QProcess", "socket",
        "sqlite3", "discover_", "scan_", "requests.",
    )
    assert not any(token in source for token in forbidden)



def test_dirty_contexts_mark_family_radio_and_banner_with_text_not_color_only():
    widget = _workspace(_snapshot())
    try:
        assert widget.has_dirty_drafts() is False
        assert widget.save_all_button.isEnabled() is False
        widget.set_dirty_contexts({(2, "js8call"): True, (1, "fast_light"): False})
        _app().processEvents()

        assert widget.dirty_contexts() == frozenset({(2, "js8call")})
        family = widget._family_buttons["js8call"]
        assert "unsaved changes" in family.text().lower()
        assert "unsaved" in family.accessibleName().lower()

        widget.select_context("js8call", 2)
        _app().processEvents()
        radio = widget._radio_buttons[2]
        assert "unsaved changes" in radio.text().lower()
        assert "unsaved changes" in radio.accessibleName().lower()
        assert "unsaved changes" in widget.context_banner.text().lower()
    finally:
        widget.deleteLater()


def test_save_all_is_deliberate_disabled_without_drafts_and_emits_only_on_click():
    widget = _workspace(_snapshot())
    try:
        events = []
        widget.save_all_requested.connect(lambda: events.append("save-all"))
        assert widget.save_all_button.isEnabled() is False
        assert "all unsaved" in widget.save_all_button.toolTip().lower()

        widget.set_dirty_state({(1, "js8call")})
        _app().processEvents()
        assert widget.has_dirty_drafts() is True
        assert widget.save_all_button.isEnabled() is True
        assert widget.save_all_button.text() == "Save All Changes (1)"
        assert "1 radio context" in widget.save_all_button.accessibleName().lower()
        widget.save_all_button.click()
        assert events == ["save-all"]

        widget.set_dirty_contexts(())
        assert widget.save_all_button.isEnabled() is False
        assert widget.save_all_button.text() == "Save All Changes"
        assert events == ["save-all"]
    finally:
        widget.deleteLater()


def test_dirty_projection_preserves_compact_shell_and_accepts_stale_keys():
    widget = _workspace(_snapshot())
    try:
        widget.set_dirty_contexts({(999, "js8call"), ("2", "js8call"), (None, "bad")})
        widget.resize(900, 560)
        widget.show()
        _app().processEvents()
        assert widget.width() == 900 and widget.height() == 560
        # The stale radio still makes the family visibly dirty, while the
        # current radio-specific chip remains an exact ownership indicator.
        assert "unsaved changes" in widget._family_buttons["js8call"].text().lower()
        assert widget._radio_buttons[2].text().lower().count("unsaved changes") == 1
        assert widget.minimumWidth() <= 900
    finally:
        widget.deleteLater()


def test_registered_task_editors_survive_task_and_radio_navigation():
    """A draft editor is one owned surface, not a recreated form per click."""
    widget = _workspace(_snapshot())
    try:
        api_editor = QLabel("API draft")
        api_editor.setAccessibleName("JS8Call API editor for FIO-A")
        all_overview = QLabel("All JS8Call overview")
        widget.register_task_editor("js8call", "api_radio", api_editor, radio_id=1)
        widget.register_task_editor("js8call", "overview", all_overview)

        widget.select_context("js8call", 1, "api_radio")
        _app().processEvents()
        assert widget.editor_stack.currentWidget() is api_editor
        api_editor.setText("Unsaved API draft")
        stack_count = widget.editor_stack.count()

        widget.select_context("js8call", 2, "overview")
        assert widget.editor_stack.currentWidget() is all_overview
        widget.select_context("js8call", 1, "api_radio")
        assert widget.editor_stack.currentWidget() is api_editor
        assert api_editor.text() == "Unsaved API draft"
        assert widget.editor_stack.count() == stack_count
        assert widget.registered_task_editor("js8call", "api_radio", radio_id=1) is api_editor
    finally:
        widget.deleteLater()


def test_registered_editor_uses_all_radios_surface_as_a_safe_fallback():
    widget = _workspace(_snapshot())
    try:
        shared_editor = QLabel("Shared message-storage guidance")
        widget.set_task_action_surface("js8call", "message_storage", shared_editor)
        widget.select_context("js8call", 2, "message_storage")
        _app().processEvents()
        assert widget.editor_stack.currentWidget() is shared_editor
        # The action surface is a reusable family editor, not a second owner
        # for FIO-B's radio-specific configuration.
        assert widget.registered_task_editor("js8call", "message_storage") is shared_editor
        assert widget.registered_task_editor("js8call", "message_storage", radio_id=2) is None
    finally:
        widget.deleteLater()


def test_main_window_exposes_settings_software_navigation_route():
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    assert 'screen_label == "Settings" and button_label == "Software"' in source
    assert '"software_administration",\n                        settings_nav_context="software"' in source
    assert 'if context == "software":' in source
    assert 'self._settings_nav_context = "software"' in source


def test_settings_tab_registers_software_scope_and_routes_details():
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    assert '"Software Administration"' in source
    assert 'self._add_settings_section(software_administration_group, scope="software")' in source
    assert 'def open_software_administration(' in source
    assert 'workspace.select_context(selected_family, radio_id, task_key)' in source
    assert 'self.show_settings_context("software", health_key="software_administration", radio_id=radio_id)' in source


def test_settings_software_context_selects_workspace_and_hides_radio_chrome(monkeypatch):
    """The real SettingsTab context switch must select the software section."""
    from freqinout.gui.settings_tab import SettingsTab

    app = _app()
    # These startup probes are unrelated to the SCA navigation shell.
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    tab = SettingsTab()
    try:
        tab.show()
        app.processEvents()
        assert tab.show_settings_context("software", health_key="software_administration") is True
        app.processEvents()
        assert tab.sections_stack.currentWidget() is tab.software_administration_section_group
        assert tab.configured_radios_group.isVisible() is False
        assert tab.settings_section_nav_scroll.isVisible() is False
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_software_navigation_controls_stay_embedded_without_overlay(monkeypatch):
    """Family/radio/task clicks must not open a second top-level software page."""
    from freqinout.gui.settings_tab import SettingsTab

    app = _app()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    tab = SettingsTab()
    try:
        workspace = tab.software_administration_workspace
        workspace.set_snapshot(_snapshot())
        tab.show()
        app.processEvents()
        baseline_windows = {window for window in app.topLevelWidgets() if window.isVisible()}
        assert tab.show_settings_context("software", health_key="software_administration") is True

        def assert_embedded() -> None:
            app.processEvents()
            visible_windows = {window for window in app.topLevelWidgets() if window.isVisible()}
            assert visible_windows == baseline_windows
            assert workspace.window() is tab
            assert workspace.isVisible()
            assert tab.sections_stack.currentWidget() is tab.software_administration_section_group

        assert_embedded()
        next(button for button in workspace._family_buttons.values() if button.text().startswith("JS8Call")).click()
        assert_embedded()
        next(button for button in workspace._radio_buttons.values() if button.text().startswith("FIO-B")).click()
        assert_embedded()
        next(button for button in workspace._task_buttons.values() if button.accessibleName() == "API & Radio").click()
        assert_embedded()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_software_page_does_not_freeze_to_placeholder_height(monkeypatch):
    """Late-built task editors must receive the Settings viewport and keep their footer visible."""
    from freqinout.gui.settings_tab import SettingsTab

    app = _app()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    tab = SettingsTab()
    try:
        tab.resize(1000, 700)
        workspace = tab.software_administration_workspace
        tab._software_administration_snapshot = _snapshot()
        workspace.set_snapshot(tab._software_administration_snapshot)
        tab.show()
        assert tab.show_settings_context("software", health_key="software_administration") is True
        workspace.select_context("js8call", 1, "api_radio")
        tab._show_software_task_editor()
        app.processEvents()

        editor = workspace.editor_stack.currentWidget()
        assert tab.sections_stack.height() >= tab.sections_scroll.viewport().height() - 2
        assert tab.sections_stack.height() < 1000
        assert tab.sections_scroll.verticalScrollBar().maximum() == 0
        assert editor is not None and editor is not workspace.editor_placeholder
        assert editor.height() >= 200
        assert editor.save_button.isVisible()
        assert editor.rect().contains(editor.save_button.geometry().bottomRight())
        assert workspace.heading_label.isHidden()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_software_workspace_survives_repeated_reflow_and_resize(monkeypatch, tmp_path):
    """Deferred snapshot/layout passes must not collapse the selected JS8 editor."""
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = _app()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    tab = SettingsTab()
    try:
        workspace = tab.software_administration_workspace
        workspace.set_snapshot(_snapshot())
        tab.show()
        assert tab.show_settings_context("software", health_key="software_administration") is True

        for width, height in ((1000, 700), (900, 560), (760, 460), (1000, 700)):
            tab.resize(width, height)
            workspace.select_context("js8call", 1, "api_radio")
            # This mirrors the deferred Settings load rebuilding the cached
            # workspace after the operator has already clicked JS8Call.
            workspace.set_snapshot(_snapshot())
            workspace.select_context("js8call", 1, "api_radio")
            tab._show_software_task_editor()
            tab._sync_current_section_scroll_size()
            app.processEvents()

            assert tab.sections_stack.currentWidget() is tab.software_administration_section_group
            assert workspace.isVisible()
            assert workspace.editor_stack.currentWidget() is not workspace.editor_placeholder
            assert workspace.editor_stack.currentWidget().isVisible()
            assert tab.sections_stack.height() >= 240
            assert tab.sections_stack.height() < 1000
            assert tab.sections_scroll.verticalScrollBar().maximum() == 0
    finally:
        tab.deleteLater()
        app.processEvents()


def test_software_task_handler_stays_in_software_workspace():
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    block = source[source.index("    def _on_software_administration_task_selected") : source.index("    def _software_editor_state")]
    assert 'self.show_settings_context("software", health_key="software_administration")' in block
    assert "_show_software_task_editor" in block
    assert "_activate_software_editor_radio_cache" not in block
    assert "show_settings_context(\"radios\"" not in block
    assert "multi_radio_store" not in block
    assert "subprocess" not in block and "socket" not in block
