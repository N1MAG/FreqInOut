"""SCA-S2 behavioral contract for task-oriented software editors."""

from __future__ import annotations

import inspect
import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtWidgets import QApplication, QLineEdit

from freqinout.gui import software_administration_editor as editor_module
from freqinout.gui.software_administration_editor import SOFTWARE_EDITOR_TASKS, SoftwareTaskEditor


def _app() -> QApplication:
    global _QT_APP
    app = QApplication.instance()
    if isinstance(app, QApplication):
        _QT_APP = app
    elif _QT_APP is None:
        _QT_APP = QApplication([])
    return _QT_APP


_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _editor(family: str, task: str, state: dict | None = None) -> SoftwareTaskEditor:
    editor = SoftwareTaskEditor()
    editor.set_context(
        family_key=family, family_title=family.replace("_", " ").title(), task_key=task,
        radio_id=7, radio_name="Test Radio", state=state or {},
    )
    _app().processEvents()
    return editor


def _field_keys(family: str) -> set[str]:
    return {field.key for task in SOFTWARE_EDITOR_TASKS[family].values() for field in task.fields}


def test_task_schemas_keep_external_tools_out_of_js8call() -> None:
    js8 = _field_keys("js8call")
    assert "path_commstat" not in js8
    assert "path_js8spotter" not in js8
    assert not any("expect" in key.lower() for key in js8)
    assert "path_commstat" not in _field_keys("commstat")
    assert {"js8_host", "js8_port"}.issubset(_field_keys("commstat"))
    assert "path_js8spotter" in _field_keys("external_spotter")
    fio = _field_keys("fio_spotter")
    assert "js8_forms_path" not in fio
    assert "js8_host" in fio and "js8_port" in fio
    assert not any("expect" in key.lower() for key in fio)
    assert "dependencies" in SOFTWARE_EDITOR_TASKS["fio_spotter"]
    assert "operational_workspace" in SOFTWARE_EDITOR_TASKS["fio_spotter"]
    assert "arq_port" in _field_keys("fast_light")


def test_fio_spotter_editor_is_dependency_mapping_and_operational_route_only() -> None:
    editor = _editor("fio_spotter", "dependencies", {"js8_forms_path": "/forms", "js8_host": "127.0.0.1", "js8_port": 2442})
    try:
        assert editor.field_widget("js8_forms_path") is None
        assert editor.field_widget("js8_host") is not None
        assert editor.field_widget("js8_port") is not None
        assert editor.task_action_button.property("software_action") == "fio_spotter"
        assert editor.field_widget("expect_rules") is None
    finally:
        editor.deleteLater()


def test_nested_state_apply_value_round_trips_without_flattening() -> None:
    editor = _editor("js8call", "api_radio", {"api": {"host": "127.0.0.1", "port": 2442}})
    try:
        editor.apply_value("api.port", 2443)
        assert editor.state() == {"api": {"host": "127.0.0.1", "port": 2443}}
        returned = editor.state()
        returned["api"]["port"] = 9000
        assert editor.state()["api"]["port"] == 2443
    finally:
        editor.deleteLater()


def test_editor_context_switch_is_explicit_and_does_not_probe_or_discover() -> None:
    source = inspect.getsource(editor_module)
    for forbidden in ("subprocess", "Popen", "os.system", "socket.create_connection", "QProcess", "SoftwarePathDetector", "discover_files", "probe_endpoint"):
        assert forbidden not in source
    editor = _editor("fast_light", "flmsg", {"message_paths.flmsg": "/inbox"})
    try:
        editor.set_context(family_key="fast_light", family_title="Fast Light", task_key="flamp_signing", radio_id=7, radio_name="Test Radio", state={"message_paths.flamp": "/amp"})
        assert editor.title_label.text() == "FLAmp & Signing"
        assert editor.field_widget("message_paths.flamp") is not None
    finally:
        editor.deleteLater()


def test_editor_is_compact_and_accessible_at_minimum_supported_size() -> None:
    editor = _editor("varac", "inbox_outbox")
    try:
        editor.resize(420, 220)
        editor.show()
        _app().processEvents()
        assert editor.minimumWidth() <= 420 and editor.minimumHeight() <= 220
        assert editor.accessibleName() == "Software task editor"
        field = editor.field_widget("message_paths.varac")
        assert isinstance(field, QLineEdit) and field.accessibleName()
    finally:
        editor.deleteLater()


def test_settings_task_selection_stays_in_software_workspace() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    block = source[source.index("    def _on_software_administration_task_selected") : source.index("    def _software_editor_state")]
    assert "_show_software_task_editor" in block
    assert "show_settings_context(\"software\"" in block or "show_settings_context('software'" in block
    assert "_activate_software_editor_radio_cache" not in block
    assert "js8_group" not in block and "fast_light_group" not in block and "varac_group" not in block
