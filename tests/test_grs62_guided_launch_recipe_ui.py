"""Focused UI contracts for core-resolved guided launch recipes."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QScrollArea

from freqinout.gui.software_instance_assistant import (
    SoftwareInstanceAssistant,
    normalize_instance_draft,
)


def _app() -> QApplication:
    app = QApplication.instance()
    return app if isinstance(app, QApplication) else QApplication([])


def _show_step(assistant: SoftwareInstanceAssistant, step: int) -> None:
    assistant._step = step
    assistant._refresh()
    _app().processEvents()


def test_qualified_js8_recipe_hides_raw_override_and_reviews_exact_component_facts() -> None:
    app = _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        unsaved_owner_key="radio-draft-1",
        unsaved_radio_label="Field receiver",
        managed_root="/managed",
        initial_draft={
            "family_key": "js8call",
            "mode": "managed",
            "draft_instance_key": "draft-js8call-abc123",
            "instance_name": "Field receiver JS8Call",
            "variant": "js8call_2_2",
            "version": "2.2.0",
            "application_path": "/apps/js8call",
            "host": "127.0.0.1",
            "port": 2443,
            "udp_port": 2243,
            "launch_at_startup": True,
        },
    )
    try:
        _show_step(assistant, 5)
        draft = assistant.draft()
        assert draft.launch_recipe_status == "qualified_managed"
        assert draft.launch_recipe_fingerprint
        assert assistant._field_widgets["launch_command"].isHidden()
        assert assistant._field_widgets["launch_command"].isEnabled() is False
        assert assistant._field_widgets["configuration_path"].isHidden()
        assert assistant._field_widgets["configuration_path"].isEnabled() is False
        assert assistant._field_widgets["storage_path"].isHidden()
        assert assistant._field_widgets["storage_path"].isEnabled() is False
        component_text = assistant.launch_recipe_components_label.text()
        assert "Effective command: /apps/js8call --rig-name" in component_text
        assert "Configuration roots: /managed/draft-js8call-abc123/js8call" in component_text
        assert "JS8Call API tcp://127.0.0.1:2443" in component_text
        assert "JS8Call UDP udp://127.0.0.1:2243" in component_text

        _show_step(assistant, 6)
        review = assistant.review_label.text()
        assert "Launch recipe" in review
        assert "Effective command: /apps/js8call --rig-name" in review
        assert "Dependencies: None" in review
        assert "/managed/draft-js8call-abc123/js8call/save" in review
        assert "Launch policy: Launch at FIO startup" in review
        assert "Advanced launch override:" not in review
    finally:
        assistant.deleteLater()
        app.processEvents()


def test_qualified_managed_paths_are_resolved_before_files_page_is_shown() -> None:
    app = _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        unsaved_owner_key="radio-draft-files",
        unsaved_radio_label="Field receiver",
        managed_root="/managed",
        initial_draft={
            "family_key": "js8call",
            "mode": "managed",
            "draft_instance_key": "draft-js8call-files",
            "instance_name": "Field receiver",
            "variant": "js8call_2_2",
            "version": "2.2.0",
            "application_path": "/apps/js8call",
            "host": "127.0.0.1",
            "port": 2443,
            "udp_port": 2243,
        },
    )
    try:
        _show_step(assistant, 4)
        draft = assistant.draft()
        assert draft.launch_recipe_status == "qualified_managed"
        assert draft.configuration_path == "/managed/draft-js8call-files/js8call"
        assert "fio-draft-js8call-files" in draft.storage_path
        assert draft.storage_path != "/managed/draft-js8call-files/js8call/save"
        assert assistant._field_widgets["configuration_path"].isHidden()
        assert assistant._field_widgets["storage_path"].isHidden()
        assert assistant.prepared_details_button.isChecked() is False
        assert assistant.prepared_details_group.isHidden()
    finally:
        assistant.deleteLater()
        app.processEvents()


def test_qualified_fast_light_review_preserves_component_order_and_dependencies() -> None:
    app = _app()
    assistant = SoftwareInstanceAssistant(
        "fast_light",
        unsaved_owner_key="radio-draft-2",
        unsaved_radio_label="Main radio",
        managed_root="/managed",
        initial_draft={
            "family_key": "fast_light",
            "mode": "managed",
            "draft_instance_key": "draft-fast-light-def456",
            "instance_name": "Main radio Fast Light",
            "application_path": "/apps/flrig",
            "secondary_application_path": "/apps/fldigi",
            "flmsg_application_path": "/apps/flmsg",
            "flamp_application_path": "/apps/flamp",
            "host": "127.0.0.1",
            "port": 12346,
            "secondary_port": 7363,
        },
    )
    try:
        _show_step(assistant, 6)
        review = assistant.review_label.text()
        assert review.index("FLRig\n") < review.index("FLDigi\n")
        assert review.index("FLDigi\n") < review.index("FLMsg\n")
        assert "Effective command: /apps/flrig --config-dir" in review
        assert "Effective command: /apps/fldigi --config-dir" in review
        assert "Dependencies: flrig" in review
        assert "FLDigi XML-RPC tcp://127.0.0.1:7363" in review
        assert "Configuration roots: /managed/draft-fast-light-def456/fast-light/fldigi" in review
    finally:
        assistant.deleteLater()
        app.processEvents()


def test_unknown_managed_variant_keeps_isolated_profile_with_launch_pending() -> None:
    app = _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        unsaved_owner_key="radio-draft-3",
        unsaved_radio_label="Future radio",
        managed_root="/managed",
        initial_draft={
            "family_key": "js8call",
            "mode": "managed",
            "draft_instance_key": "draft-js8call-future",
            "variant": "future-js8",
            "version": "99.0",
            "application_path": "/apps/future-js8",
            "host": "127.0.0.1",
            "port": 2449,
            "udp_port": 2249,
            "launch_command": "/apps/future-js8 --operator-reviewed",
        },
    )
    try:
        _show_step(assistant, 5)
        draft = assistant.draft()
        assert draft.launch_recipe_status == "launch_pending"
        assert draft.configuration_path.endswith("/draft-js8call-future/js8call")
        assert draft.storage_path
        assert assistant._field_widgets["launch_command"].isHidden() is True
        assert assistant._field_widgets["configuration_path"].isHidden() is True
        assert assistant._field_widgets["storage_path"].isHidden() is True
        assert "--rig-name" in assistant.launch_recipe_recovery_label.text()
        _show_step(assistant, 6)
        assert "Launch Pending" in assistant.review_label.text()
        assert "Recovery: Confirm that /apps/future-js8" in assistant.review_label.text()
    finally:
        assistant.deleteLater()
        app.processEvents()


def test_source_lock_wins_over_operator_start_raw_override() -> None:
    app = _app()
    existing = {
        "id": 41,
        "system_key": "existing-js8",
        "name": "Existing JS8",
        "family_key": "js8call",
        "mode": "discover",
        "host": "127.0.0.1",
        "port": 2442,
        "udp_port": 2242,
        "application_path": "/apps/js8call",
        "launch_command": "/apps/js8call --rig-name EXISTING",
        # Classification is supplied by the immutable inventory core; this
        # test exercises source-lock behavior after explicit import.
        "candidate_classification": "usable_existing",
        "usable_existing": True,
    }
    assistant = SoftwareInstanceAssistant(
        "js8call",
        existing_instances=(existing,),
        unsaved_owner_key="radio-draft-4",
        unsaved_radio_label="Imported radio",
    )
    try:
        assistant.source_buttons["discover"].setChecked(True)
        assistant.set_discovery_results((existing,))
        assistant.discovery_list.setCurrentRow(0)
        _show_step(assistant, 5)
        assert assistant.draft().launch_recipe_status == "operator_start"
        assert assistant._field_widgets["launch_command"].isHidden() is False
        assert assistant._field_widgets["launch_command"].isEnabled() is False
    finally:
        assistant.deleteLater()
        app.processEvents()


def test_launch_recipe_fields_round_trip_without_losing_core_fingerprint() -> None:
    resolution_mapping = {
        "family_key": "js8call",
        "status": "unsupported",
        "components": [],
        "raw_override_allowed": True,
        "recovery_action": "Choose Advanced.",
        "summary": "Operator action required.",
        "fingerprint": "external-fingerprint",
    }
    draft = normalize_instance_draft(
        {
            "family_key": "js8call",
            "launch_recipe": resolution_mapping,
            "launch_recipe_status": "unsupported",
            "launch_recipe_fingerprint": "external-fingerprint",
        }
    )
    assert draft.payload()["launch_recipe"] == resolution_mapping
    assert draft.payload()["launch_recipe_status"] == "unsupported"
    assert draft.payload()["launch_recipe_fingerprint"] == "external-fingerprint"


def test_assistant_navigation_and_scroll_contracts_remain_accessible_and_bounded() -> None:
    app = _app()
    assistant = SoftwareInstanceAssistant("js8call")
    try:
        assert assistant.cancel_button.accessibleName()
        assert assistant.back_button.accessibleName()
        assert assistant.next_button.accessibleName()
        assert all(button.accessibleName() for button in assistant.step_buttons)
        scrolls = assistant.findChildren(QScrollArea)
        assert scrolls
        assert all(
            scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
            for scroll in scrolls
        )
    finally:
        assistant.deleteLater()
        app.processEvents()
