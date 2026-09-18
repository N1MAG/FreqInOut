"""GRS-6.1 contracts for Add Radio / Software Administration identity flow."""

import os
import inspect
from dataclasses import replace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtWidgets import QApplication

from freqinout.core.guided_family_completion import create_js8_instance, import_existing_js8_instance
from freqinout.core.guided_instance_inventory import (
    build_guided_instance_inventory,
    distinct_draft_seed,
)
from freqinout.core.guided_radio_software_model import InstanceSourceMode, RadioRole
from freqinout.core.guided_software_proposals import (
    DiscoveredSoftwareCandidate,
    ExistingCandidateRequest,
    Js8DistinctProposalRequest,
    ProposalInventory,
)
from freqinout.gui.software_instance_assistant import SoftwareInstanceAssistant, instance_conflicts


def _app():
    return QApplication.instance() or QApplication([])


def _js8(key="a", radio="radio-a", variant="stock"):
    return create_js8_instance(
        Js8DistinctProposalRequest(key, radio, RadioRole.TRANSCEIVER, variant, "2.2", "/managed", "/apps/js8"),
        ProposalInventory(),
    )


def test_add_radio_supplies_shared_existing_instance_inventory_to_assistant():
    _app()
    existing = {"id": 11, "radio_id": 7, "name": "North JS8", "port": 2442, "install_path": "/apps/js8", "family_key": "js8call"}
    assistant = SoftwareInstanceAssistant("js8call", radios=({"id": 7, "name": "North"},), existing_instances=(existing,))
    try:
        assert assistant._existing_instances == (existing,)
        assert any("Assigned to North JS8" in assistant.radio_combo.itemText(i) for i in range(assistant.radio_combo.count()))
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_unsaved_radio_draft_key_survives_navigation_and_name_changes():
    _app()
    assistant = SoftwareInstanceAssistant("js8call", unsaved_owner_key="draft-42", unsaved_radio_label="First name")
    try:
        assert assistant.draft().owner_draft_key == "draft-42"
        assistant._unsaved_radio_label = "Renamed radio"
        assistant._rebuild_radio_choices()
        assert assistant.draft().owner_draft_key == "draft-42"
        assert "Renamed radio" in assistant.radio_combo.currentText()
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_create_distinct_uses_new_identity_even_when_existing_bundle_is_in_inventory():
    existing = _js8()
    proposal = create_js8_instance(
        Js8DistinctProposalRequest("new", "radio-b", RadioRole.TRANSCEIVER, "stock", "2.2", "/managed", "/apps/js8"),
        ProposalInventory((existing.bundle,)),
    )
    assert proposal.bundle.source_mode is InstanceSourceMode.CREATE_DISTINCT
    assert proposal.bundle.identity_fingerprint != existing.bundle.identity_fingerprint
    assert proposal.bundle.configuration_path != existing.bundle.configuration_path
    assert proposal.bundle.endpoints[0].port == 2443


def test_import_locks_complete_identity_and_partial_edit_requires_explicit_clone():
    existing = _js8().bundle
    imported = import_existing_js8_instance(
        ExistingCandidateRequest("import", "radio-a", RadioRole.TRANSCEIVER, DiscoveredSoftwareCandidate("candidate", existing)),
        ProposalInventory((existing,)),
    ).bundle
    assert imported.source_locked and imported.identity_fingerprint == existing.identity_fingerprint
    # A clone is a new distinct proposal; changing one imported field in place
    # is not accepted as an import operation.
    with pytest.raises(Exception, match="source|identity|conflict|owned"):
        import_existing_js8_instance(
            ExistingCandidateRequest("edited", "radio-a", RadioRole.TRANSCEIVER, DiscoveredSoftwareCandidate("candidate", replace(existing, data_path="/other", identity_fingerprint=""))),
            ProposalInventory((existing,)),
        )


def test_cancel_or_stale_conflict_checks_do_not_mutate_existing_inventory():
    existing = [{"id": 3, "name": "North API", "host": "127.0.0.1", "port": 2443, "install_path": "/apps/js8"}]
    before = [dict(row) for row in existing]
    findings = instance_conflicts({"family_key": "js8call", "name": "North API", "radio_id": 7, "host": "127.0.0.1", "port": 2443, "path": "/apps/js8"}, existing)
    assert findings and existing == before


def test_retained_current_transaction_bundle_reserves_port_for_next_distinct_instance():
    retained = _js8("retained", "radio-a").bundle
    next_proposal = create_js8_instance(
        Js8DistinctProposalRequest("next", "radio-b", RadioRole.OBSERVER, "subspace", "2.2", "/managed", "/apps/js8"),
        ProposalInventory((retained,)),
    )
    assert next_proposal.allocated_port == 2443
    assert next_proposal.bundle.endpoints[0].port == next_proposal.allocated_port
    assert next_proposal.bundle.configuration_path != retained.configuration_path


def test_imported_assistant_fields_are_locked_and_clone_gets_fresh_identity():
    _app()
    existing = {
        "id": 11,
        "system_key": "north-js8",
        "name": "North JS8",
        "host": "127.0.0.1",
        "port": 2442,
        "udp_port": 2237,
        "rig_name": "NORTH",
        "install_path": "/apps/js8call",
        "profile_path": "/profiles/north",
        "application_data_root": "/data/north",
    }
    snapshot = build_guided_instance_inventory(
        {"js8call": (existing,)},
        linked_ids_by_family={"js8call": {11}},
        generation=7,
    )
    assistant = SoftwareInstanceAssistant(
        "js8call",
        existing_instances=snapshot.rows_for("js8call"),
        inventory_snapshot=snapshot,
        unsaved_owner_key="radio-draft-7",
        unsaved_radio_label="South",
    )
    try:
        assistant.source_buttons["discover"].setChecked(True)
        assistant.set_discovery_results(snapshot.rows_for("js8call"))
        assistant.discovery_list.setCurrentRow(0)
        imported = assistant.draft()
        assert imported.source_locked is True
        assert imported.source_fingerprint
        assert assistant.clone_distinct_button.isHidden() is False
        assert assistant._field_widgets["port"].isEnabled() is False
        assert assistant._field_widgets["configuration_path"].isEnabled() is False
        assert assistant._field_widgets["storage_path"].isEnabled() is False

        assistant.clone_distinct_button.click()
        cloned = assistant.draft()
        assert cloned.mode == "managed"
        assert cloned.source_locked is False
        assert cloned.imported_id is None
        assert cloned.imported_system_key == ""
        assert cloned.draft_instance_key.startswith("draft-js8call-")
        assert cloned.inventory_fingerprint == snapshot.fingerprint
        assert cloned.port == 2443
        assert cloned.udp_port == 2238
        assert cloned.rig_name == ""
        assert cloned.configuration_path == ""
        assert cloned.storage_path == ""
        assert cloned.application_path == "/apps/js8call"
        assert assistant._field_widgets["port"].isEnabled() is True
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_distinct_draft_identity_survives_navigation_and_display_name_edits():
    _app()
    snapshot = build_guided_instance_inventory({"js8call": ()}, generation=2)
    seed = distinct_draft_seed(
        "js8call",
        owner_draft_key="owner-9",
        snapshot=snapshot,
    )
    seed.update(instance_name="First name", radio_role="observer")
    assistant = SoftwareInstanceAssistant(
        "js8call",
        inventory_snapshot=snapshot,
        unsaved_owner_key="owner-9",
        unsaved_radio_label="Receiver",
        radio_role="observer",
        initial_draft=seed,
    )
    try:
        original_key = assistant.draft().draft_instance_key
        assistant._field_widgets["instance_name"].setText("Renamed display label")
        assistant._next()
        assistant._back()
        reviewed = assistant.draft()
        assert reviewed.owner_draft_key == "owner-9"
        assert reviewed.draft_instance_key == original_key
        assert reviewed.inventory_fingerprint == snapshot.fingerprint
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_switching_import_to_managed_cannot_retain_source_owned_fields():
    _app()
    existing = {
        "id": 19,
        "system_key": "existing-js8",
        "name": "Existing JS8",
        "host": "127.0.0.1",
        "port": 2442,
        "udp_port": 2237,
        "rig_name": "EXISTING",
        "install_path": "/apps/js8call",
        "profile_path": "/profiles/existing",
        "application_data_root": "/data/existing",
    }
    snapshot = build_guided_instance_inventory(
        {"js8call": (existing,)},
        linked_ids_by_family={"js8call": {19}},
        generation=3,
    )
    assistant = SoftwareInstanceAssistant(
        "js8call",
        existing_instances=snapshot.rows_for("js8call"),
        inventory_snapshot=snapshot,
        unsaved_owner_key="radio-draft-19",
        unsaved_radio_label="New radio",
    )
    try:
        assistant.source_buttons["discover"].setChecked(True)
        assistant.set_discovery_results(snapshot.rows_for("js8call"))
        assistant.discovery_list.setCurrentRow(0)

        assistant.source_buttons["managed"].click()
        draft = assistant.draft()
        assert draft.mode == "managed"
        assert draft.imported_id is None
        assert draft.source_locked is False
        assert draft.port == 2443
        assert draft.udp_port == 2238
        assert draft.rig_name == ""
        assert draft.configuration_path == ""
        assert draft.storage_path == ""
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_add_radio_uses_shared_snapshot_and_core_distinct_seed():
    from freqinout.gui.settings_tab import SettingsTab

    source = inspect.getsource(SettingsTab._open_device_profile_dialog)
    assert "build_guided_instance_inventory(" in source
    assert "distinct_draft_seed(" in source
    assert "existing_instances=tuple(" in source
    assert "inventory_snapshot=inventory_snapshot" in source
