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
    VarACNativePresentation,
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
            ({
                "id": 18,
                "system_key": "js8-north",
                "name": "North imported",
                "host": "127.0.0.1",
                "port": 2448,
                "candidate_classification": "usable_existing",
            },)
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


def test_varac_native_presentation_is_cache_only_and_exposes_worker_seams() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "varac",
        unsaved_owner_key="varac-native-draft",
        unsaved_radio_label="New Radio",
        initial_draft={
            "cluster_path": "create_cluster",
            "cluster_instance_number": 2,
            "existing_standalone_node_id": 7,
            "cluster_gateway": True,
        },
        varac_native_presentation=VarACNativePresentation(
            state="ready",
            arrangement="create_cluster",
            affected_radios=("Existing Radio", "New Radio"),
            shared_database_summary="Proposed shared database",
            member_numbers_summary="Existing 1 · New 2",
            ptt_lock_summary="On",
            email_gateway_sender_summary="New Radio",
            writer_version="13.2.7",
            writer_platform="linux-wine",
            writer_operation="convert-standalone",
            writer_qualified=True,
            varac_ini_path="/managed/new/VarAC.ini",
            vara_runtime_path="/managed/new/VARA",
            vara_ini_path="/managed/new/VARA/VARA.ini",
            ports_summary="Command 8310 · data 8311 · KISS 8312",
            fingerprints_summary="plan abc · source def",
        ),
    )
    try:
        assert "Ready" in assistant.varac_native_status_label.text()
        assert "13.2.7 writer qualified" in assistant.varac_native_status_label.text()
        visible = assistant.varac_native_summary_label.text()
        assert "Shared VarAC database" in visible
        assert "PTT lock: On" in visible
        assert "Email gateway sender: No email gateway" in visible
        assert "gateway handler" not in visible.lower()
        assert "/managed/new/VarAC.ini" not in visible
        sender = assistant.email_gateway_sender_combo
        assert sender.findData("none") >= 0
        assert sender.findData("existing_member") >= 0
        assert sender.findData("new_member") >= 0
        sender.setCurrentIndex(sender.findData("existing_member"))
        assert assistant.draft().email_gateway_sender_choice == "existing_member"
        assert assistant.draft().email_gateway_sender_member_id == "7"
        sender.setCurrentIndex(sender.findData("new_member"))
        assert assistant.draft().email_gateway_sender_choice == "new_member"
        assert assistant.draft().email_gateway_sender_member_id
        assert "Email gateway sender: New member" in assistant.varac_native_summary_label.text()
        # The compatibility-only legacy bit survives exactly as evidence; it
        # does not select or alter the new explicit sender choice.
        assert assistant.draft().cluster_gateway is True
        assert not assistant.prepared_details_group.isVisible()
        assistant.prepared_details_button.click()
        assert "/managed/new/VarAC.ini" in assistant.prepared_details_label.text()
        assert "/managed/new/VARA/VARA.ini" in assistant.prepared_details_label.text()

        assistant.set_varac_native_presentation({"state": "stop_varac_required"})
        assert assistant.varac_native_prepare_button.text() == "Retry after closing VarAC"
        assert assistant.varac_native_prepare_button.isEnabled()
        assistant.set_varac_native_presentation(
            {"state": "ready", "writer_version": "15.0.18", "writer_qualified": False}
        )
        assert "Manual setup required" in assistant.varac_native_status_label.text()

        prepared: list[object] = []
        applied: list[object] = []
        assistant.varac_native_prepare_requested.connect(prepared.append)
        assistant.varac_native_apply_requested.connect(applied.append)
        assistant.set_varac_native_presentation({"state": "needs_attention"})
        assistant.varac_native_prepare_button.click()
        assistant.request_varac_native_apply()
        assert prepared and prepared[0]["draft"]["family_key"] == "varac"
        assert prepared[0]["native_presentation"]["state"] == "needs_attention"
        assert applied and applied[0] == prepared[0]
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_native_managed_varac_cluster_final_review_emits_apply_with_prepared_snapshot() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "varac",
        unsaved_owner_key="varac-native-review",
        unsaved_radio_label="New Radio",
        initial_draft={
            "instance_name": "New Radio",
            "application_path": "/apps/VarAC.exe",
            "working_directory": "/managed/new",
            "cluster_path": "create_cluster",
            "cluster_id": "VARAC-NEW",
            "cluster_name": "New cluster",
            "cluster_instance_number": 2,
            "existing_standalone_node_id": 7,
        },
        varac_native_presentation={
            "state": "ready",
            "generation": 9,
            "writer_version": "13.2.7",
            "writer_qualified": True,
        },
    )
    try:
        assistant.email_gateway_sender_combo.setCurrentIndex(
            assistant.email_gateway_sender_combo.findData("new_member")
        )
        assistant._step = len(assistant.STEP_TITLES) - 1
        assistant._refresh()
        assert assistant.next_button.text() == "Review & Save"
        assert assistant.next_button.isEnabled()
        assert "cluster_launch_required" not in {item.code for item in assistant.validation()}
        completed = []
        apply_requests = []
        assistant.completed.connect(completed.append)
        assistant.varac_native_apply_requested.connect(apply_requests.append)
        assistant._next()
        assert not completed
        assert len(apply_requests) == 1
        payload = apply_requests[0]
        assert payload["draft"]["email_gateway_sender_choice"] == "new_member"
        assert payload["draft"]["varac_native_generation"] == 9
        assert payload["native_presentation"]["writer_qualified"] is True
    finally:
        assistant.deleteLater()
        _app().processEvents()


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
            ({
                "name": "Detected JS8",
                "rig_name": "field",
                "host": "127.0.0.1",
                "port": 2450,
                "candidate_classification": "usable_existing",
            },)
        )
        assistant.discovery_list.setCurrentRow(0)
        assert "discovery_selection_required" not in {item.code for item in assistant.validation()}
    finally:
        assistant.deleteLater()
