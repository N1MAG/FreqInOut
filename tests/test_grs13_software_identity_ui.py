"""Canonical Software Administration identity controls remain review-only."""

from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtWidgets import QApplication, QLineEdit

from freqinout.gui.software_administration_editor import SoftwareTaskEditor
from freqinout.gui.settings_tab import FAST_LIGHT_COMPONENT_REPAIR_TASK_KEYS, SettingsTab


_APP: QApplication | None = QApplication.instance() or QApplication([])


def test_canonical_software_identity_disables_partial_edit_save_and_discovery() -> None:
    editor = SoftwareTaskEditor()
    try:
        editor.set_context(
            family_key="js8call",
            family_title="JS8Call",
            task_key="application_profile",
            radio_id=71,
            radio_name="FT-710",
            state={"path_js8call": "/usr/bin/js8call", "js8_profile_path": "/radio/FT-710"},
        )
        editor.show()
        _APP.processEvents()
        assert editor.form_widget.isEnabled()
        assert editor.save_button.isVisible()
        assert editor.discover_button.isVisible()
        assert any(isinstance(widget, QLineEdit) for widget in editor._field_widgets.values())

        editor.set_canonical_identity_managed(True, identity_key="js8call:ft-710")
        _APP.processEvents()

        assert not editor.form_widget.isEnabled()
        assert all(not widget.isEnabled() for widget in editor._field_widgets.values())
        assert editor.save_button.isHidden()
        assert editor.discover_button.isHidden()
        assert editor.dirty_label.isHidden()
        assert editor.identity_notice_label.isVisible()
        assert "FT-710" in editor.identity_notice_label.text()
        assert "js8call:ft-710" not in editor.identity_notice_label.text()
        assert "Add software instance" in editor.identity_notice_label.text()
    finally:
        editor.deleteLater()
        _APP.processEvents()


def test_canonical_fast_light_exposes_narrow_message_component_repair() -> None:
    editor = SoftwareTaskEditor()
    try:
        editor.set_context(
            family_key="fast_light",
            family_title="Fast Light",
            task_key="flamp",
            radio_id=72,
            radio_name="FT-710",
            state={"flamp_path": "/usr/local/bin/flamp"},
        )
        editor.set_canonical_identity_managed(
            True,
            identity_key="fast_light:ft-710",
            component_repair_available=True,
        )
        editor.show()
        _APP.processEvents()

        assert editor.task_action_button.isVisible()
        assert editor.task_action_button.text() == "Repair FLMsg / FLAmp components…"
        assert (
            editor.task_action_button.property("software_action")
            == "repair_fast_light_message_components"
        )
        assert "component-only" in editor.identity_notice_label.text()
        assert editor.save_button.isHidden()
    finally:
        editor.deleteLater()
        _APP.processEvents()


def test_component_repair_visibility_detects_stale_software_admin_message_path() -> None:
    assert {"flmsg", "flamp_signing", "message_folders", "launch"}.issubset(
        FAST_LIGHT_COMPONENT_REPAIR_TASK_KEYS
    )
    tab = SettingsTab.__new__(SettingsTab)
    tab.device_profiles = [
        {
            "id": 72,
            "name": "FT-710",
            "use_flmsg": 1,
            "use_flamp": 1,
            "flmsg_message_path": "/home/bill/.nbems/instances/FT-710/ICS/messages",
            "flamp_message_path": "/home/bill/.nbems/FLAMP/rx",
        }
    ]
    record = SimpleNamespace(
        family_key="fast_light",
        management_mode="fio_managed",
        components=(
            SimpleNamespace(
                component_id="flmsg",
                argv=(
                    "/usr/local/bin/flmsg",
                    "--flmsg-dir",
                    "/home/bill/.nbems/instances/FT-710",
                    "-title",
                    "FLMsg — FT-710",
                ),
                cwd="/home/bill/.nbems/instances/FT-710",
            ),
            SimpleNamespace(
                component_id="flamp",
                argv=(
                    "/usr/local/bin/flamp",
                    "--config-dir",
                    "/home/bill/.nbems/instances/FT-710",
                    "--arq-server-port",
                    "7323",
                    "-title",
                    "FLAmp — FT-710",
                ),
                cwd="/home/bill/.nbems/instances/FT-710",
            ),
        ),
    )
    tab.multi_radio_store = SimpleNamespace(
        list_radio_software_identity_records=lambda _radio_id: (record,),
        get_radio_launch_bundle=lambda _radio_id: {"items": []},
    )

    assert tab._fast_light_message_component_repair_needed(72) is True
    tab.device_profiles[0]["flamp_message_path"] = (
        "/home/bill/.nbems/instances/FT-710/FLAMP/rx"
    )
    assert tab._fast_light_message_component_repair_needed(72) is False
    record.components[0].argv = record.components[0].argv[:-2]
    assert tab._fast_light_message_component_repair_needed(72) is True
