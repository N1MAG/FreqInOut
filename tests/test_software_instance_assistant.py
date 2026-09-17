"""Focused contract tests for the cache-only multi-instance setup flow."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtWidgets import QApplication, QComboBox, QLineEdit

from freqinout.gui.software_instance_assistant import (
    SoftwareInstanceAssistant,
    SoftwareInstanceDraft,
    instance_conflicts,
    normalize_instance_draft,
)


def _app() -> QApplication:
    app = QApplication.instance()
    return app if isinstance(app, QApplication) else QApplication([])


def test_draft_payload_is_stable_and_normalizes_store_style_names() -> None:
    draft = normalize_instance_draft(
        {
            "family": "JS8Call",
            "name": "North API",
            "id": 4,
            "install_path": "/apps/js8",
            "profile_path": "/data/north",
            "data_path": "/messages/north",
            "launch_cmd": "/apps/js8 --profile north",
            "port": "2443",
        }
    )
    assert draft == SoftwareInstanceDraft(
        family_key="js8call", instance_name="North API", imported_id=4,
        host="127.0.0.1", port=2443, application_path="/apps/js8",
        configuration_path="/data/north", storage_path="/messages/north",
        launch_command="/apps/js8 --profile north",
    )
    assert tuple(draft.payload())[:6] == (
        "family_key", "instance_name", "radio_id", "mode", "ownership",
        "management_mode",
    )
    assert draft.payload()["executable_path"] == "/apps/js8"


def test_js8_draft_retains_exact_native_writer_evidence() -> None:
    draft = normalize_instance_draft(
        {
            "family": "js8call",
            "name": "Field JS8",
            "js8_variant_family": "js8call_subspace_4_1",
            "js8_variant_version": "4.1.0",
            "js8_writer_platform": "linux",
            "js8_writer_operation": "create",
        }
    )

    assert draft.variant == "js8call_subspace_4_1"
    assert draft.version == "4.1.0"
    assert draft.payload()["js8_variant_family"] == "js8call_subspace_4_1"
    assert draft.payload()["js8_variant_version"] == "4.1.0"


def test_conflicts_are_explicit_and_importing_same_row_is_safe() -> None:
    existing = [{"id": 3, "name": "North API", "host": "127.0.0.1", "port": 2443, "install_path": "/apps/js8"}]
    findings = instance_conflicts(
        {"family_key": "js8call", "name": "North API", "radio_id": 7, "rig_name": "north", "host": "127.0.0.1", "port": 2443, "path": "/apps/js8"},
        existing,
    )
    assert {item.code for item in findings} == {"duplicate_name", "duplicate_endpoint", "duplicate_path"}
    assert not instance_conflicts(
        {"family_key": "js8call", "name": "North API", "id": 3, "radio_id": 7, "rig_name": "north", "host": "127.0.0.1", "port": 2443, "path": "/apps/js8"},
        existing,
    )


def test_unsaved_radio_owner_uses_authoritative_assistant_without_fake_radio_id() -> None:
    app = _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        unsaved_owner_key="guided-radio-draft-7",
        unsaved_radio_label="Field SDR",
        initial_draft={
            "instance_name": "Field SDR JS8Call",
            "rig_name": "FIELD-SDR",
            "application_path": "/apps/js8call",
            "host": "127.0.0.1",
            "port": 2443,
        },
    )
    try:
        draft = assistant.draft()
        assert draft.radio_id is None
        assert draft.owner_draft_key == "guided-radio-draft-7"
        assert draft.owner_label == "Field SDR"
        assert assistant.radio_combo.isEnabled() is False
        assert "inactive setup draft" in assistant.radio_combo.currentText()
        assert "radio_required" not in {finding.code for finding in assistant.validation()}
        assert draft.payload()["owner_draft_key"] == "guided-radio-draft-7"
    finally:
        assistant.deleteLater()
        app.processEvents()


def test_assistant_has_family_specific_steps_and_emits_only_after_review() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "varac",
        radios=({"id": 7, "name": "Radio North"},),
        existing_instances=(),
    )
    try:
        assert assistant.pages.count() == 7
        assert assistant.family_combo.currentData() == "varac"
        assert assistant.radio_combo.itemData(1) == 7
        assistant.radio_combo.setCurrentIndex(1)
        assistant._field_widgets["instance_name"].setText("VarAC North")
        assistant._field_widgets["application_path"].setText("/apps/varac")
        for _ in range(6):
            assistant._next()
        assert assistant._step == 6
        assert "INI:" in assistant.review_label.text()
        assert assistant.next_button.text() == "Add instance"
        emitted = []
        assistant.completed.connect(emitted.append)
        assistant.next_button.click()
        assert emitted and emitted[0]["family_key"] == "varac"
        assert emitted[0]["radio_id"] == 7
    finally:
        assistant.deleteLater()


def test_remote_instance_requires_host_but_does_not_scan_on_open() -> None:
    _app()
    assistant = SoftwareInstanceAssistant("js8call")
    try:
        assistant.source_buttons["remote"].setChecked(True)
        host = assistant._field_widgets["host"]
        assert isinstance(host, QLineEdit)
        host.clear()
        assistant._step = 6
        assistant._refresh()
        assert assistant.next_button.isEnabled() is False
        assert "Remote host required" in assistant.conflict_label.text()
    finally:
        assistant.deleteLater()


def test_selected_radio_status_and_import_metadata_are_preserved() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        radios=({"id": 7, "name": "Radio North"},),
        selected_radio_id=7,
    )
    try:
        assert assistant.radio_combo.currentData() == 7
        assistant.set_operation_status("Discovery could not be completed.", error=True)
        assert assistant.operation_status_label.text().startswith("Discovery")
        assistant.set_discovery_results(
            ({"id": 18, "system_key": "js8-north", "name": "North imported", "host": "127.0.0.1", "port": 2448},)
        )
        assistant.discovery_list.setCurrentRow(0)
        draft = assistant.draft()
        assert draft.imported_id == 18
        assert draft.imported_system_key == "js8-north"
        assert draft.payload()["application_system_key"] == "js8-north"
        assert "discovery_selection_required" not in {item.code for item in assistant.validation()}
    finally:
        assistant.deleteLater()


def test_family_fields_are_scoped_and_draft_round_trips_every_field() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        varac_clusters=({"cluster_id": "cluster", "name": "Primary cluster"},),
    )
    try:
        assert assistant._field_widgets["rig_name"].isHidden() is False
        assert assistant._field_widgets["secondary_port"].isHidden()
        assistant.family_combo.setCurrentIndex(1)
        assert assistant._field_widgets["secondary_port"].isHidden() is False
        assert assistant._field_widgets["rig_name"].isHidden()
        assistant.family_combo.setCurrentIndex(2)
        assert assistant._field_widgets["cluster_id"].isHidden()
        cluster_path = assistant._field_widgets["cluster_path"]
        cluster_path.setCurrentIndex(cluster_path.findData("join_cluster"))
        assert assistant._field_widgets["cluster_id"].isHidden() is False
        assert assistant._field_widgets["udp_port"].isHidden()
        assistant._field_widgets["instance_name"].setText("VarAC North")
        cluster = assistant._field_widgets["cluster_id"]
        assert isinstance(cluster, QComboBox)
        cluster.setCurrentIndex(cluster.findData("cluster"))
        assistant._field_widgets["cluster_instance_number"].setText("2")
        assistant._field_widgets["launch_at_startup"].setChecked(True)
        draft = assistant.draft()
        assert draft.cluster_id == "cluster"
        assert draft.cluster_instance_number == 2
        assert draft.launch_at_startup is True
        assert draft.payload()["cluster_instance_number"] == 2
    finally:
        assistant.deleteLater()


def test_new_local_setup_is_default_and_fast_light_endpoints_must_be_distinct() -> None:
    _app()
    assistant = SoftwareInstanceAssistant("fast_light", radios=({"id": 3, "name": "Radio 3"},))
    try:
        assert assistant.source_buttons["managed"].isChecked()
        assistant.radio_combo.setCurrentIndex(1)
        assistant._field_widgets["instance_name"].setText("Fast Light 3")
        assistant._field_widgets["secondary_port"].setText(
            assistant._field_widgets["port"].text()
        )
        assistant._field_widgets["launch_at_startup"].setChecked(True)
        findings = assistant.validation()
        assert "fast_light_endpoint_overlap" in {item.code for item in findings}
        assert assistant._field_labels["port"].text() == "FLRig XML-RPC port"
    finally:
        assistant.deleteLater()


def test_discovery_requires_an_explicit_candidate_selection() -> None:
    _app()
    assistant = SoftwareInstanceAssistant("js8call", radios=({"id": 5, "name": "Radio 5"},))
    try:
        assistant.source_buttons["discover"].setChecked(True)
        assistant.radio_combo.setCurrentIndex(1)
        assistant._field_widgets["instance_name"].setText("Candidate")
        assert "discovery_selection_required" in {item.code for item in assistant.validation()}
        assistant.set_discovery_results(
            ({"name": "Detected JS8", "rig_name": "field", "host": "127.0.0.1", "port": 2450},)
        )
        assistant.discovery_list.setCurrentRow(0)
        assert "discovery_selection_required" not in {item.code for item in assistant.validation()}
    finally:
        assistant.deleteLater()
