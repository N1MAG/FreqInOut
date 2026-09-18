"""GRS-7.2 presentation boundary for classified software inventory rows."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from freqinout.core.guided_instance_inventory import build_guided_instance_inventory
from freqinout.gui.settings_tab import SettingsTab
from freqinout.gui.software_instance_assistant import SoftwareInstanceAssistant


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_diagnostic_inventory_rows_remain_visible_but_cannot_be_imported() -> None:
    _app()
    assistant = SoftwareInstanceAssistant(
        "js8call",
        unsaved_owner_key="draft-grs72",
        unsaved_radio_label="New radio",
    )
    usable = {
        "id": 11,
        "name": "Complete JS8Call",
        "host": "127.0.0.1",
        "port": 2442,
        "candidate_classification": "usable_existing",
    }
    diagnostic = {
        "id": 12,
        "name": "Incomplete JS8Call",
        "candidate_classification": "orphaned_incomplete",
        "candidate_reasons": ("No linked radio", "Duplicate API 2442 claim"),
    }
    try:
        assistant.source_buttons["discover"].setChecked(True)
        assistant.set_discovery_results((usable, diagnostic))

        assert assistant.discovery_list.count() == 2
        usable_item = assistant.discovery_list.item(0)
        diagnostic_item = assistant.discovery_list.item(1)
        assert usable_item.flags() & Qt.ItemIsEnabled
        assert usable_item.flags() & Qt.ItemIsSelectable
        assert not bool(diagnostic_item.flags() & Qt.ItemIsEnabled)
        assert not bool(diagnostic_item.flags() & Qt.ItemIsSelectable)
        assert diagnostic_item.data(Qt.UserRole) is None
        assert "Diagnostic only" in diagnostic_item.text()
        assert "Duplicate API 2442 claim" in diagnostic_item.toolTip()

        assert not assistant.discovery_diagnostics_label.isHidden()
        assert "cannot be imported, recommended, assigned, or launched" in (
            assistant.discovery_diagnostics_label.text()
        )
        assert assistant._discovery_results == (usable,)
        assert assistant._diagnostic_discovery_results == (diagnostic,)

        assistant.discovery_list.setCurrentRow(1)
        assert assistant.draft().source_locked is False
        assert assistant.draft().imported_id is None
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_unclassified_inventory_evidence_fails_closed_to_diagnostics() -> None:
    _app()
    assistant = SoftwareInstanceAssistant("js8call")
    try:
        assistant.source_buttons["discover"].setChecked(True)
        assistant.set_discovery_results(({"id": 99, "name": "Legacy row"},))

        assert assistant.discovery_list.count() == 1
        item = assistant.discovery_list.item(0)
        assert not bool(item.flags() & Qt.ItemIsEnabled)
        assert "classification is unavailable" in item.toolTip()
        assert assistant._discovery_results == ()
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_core_incomplete_snapshot_is_rendered_as_disabled_recovery_evidence() -> None:
    _app()
    snapshot = build_guided_instance_inventory(
        {
            "js8call": (
                {
                    "id": 7,
                    "name": "Legacy partial JS8",
                    "system_key": "legacy-partial-js8",
                    "port": 2442,
                },
            )
        },
        generation=1,
    )
    row = snapshot.rows_for("js8call")[0]
    assert row["diagnostic_only"] is True
    assert "missing application path" in row["completeness_reasons"]

    assistant = SoftwareInstanceAssistant("js8call")
    try:
        assistant.source_buttons["discover"].setChecked(True)
        assistant.set_discovery_results(snapshot.rows_for("js8call"))

        item = assistant.discovery_list.item(0)
        assert not bool(item.flags() & Qt.ItemIsEnabled)
        assert "missing application path" in item.toolTip()
        assert assistant._discovery_results == ()
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_recovery_candidate_is_selectable_only_in_explicit_find_import_mode() -> None:
    _app()
    recovery = {
        "id": 21,
        "name": "Unassigned complete JS8Call",
        "host": "127.0.0.1",
        "port": 2443,
        "candidate_classification": "recovery_only",
        "recovery_only": True,
        "candidate_usable": False,
        "source_fingerprint": "recovery-fingerprint",
    }
    assistant = SoftwareInstanceAssistant("js8call", unsaved_owner_key="draft-recovery")
    try:
        assistant.set_discovery_results((recovery,))
        assert assistant.discovery_list.isHidden()

        assistant.source_buttons["discover"].setChecked(True)
        assert not assistant.discovery_list.isHidden()
        item = assistant.discovery_list.item(0)
        assert item.flags() & Qt.ItemIsEnabled
        assert item.flags() & Qt.ItemIsSelectable
        assert item.data(Qt.UserRole) == recovery
        assert item.text().startswith("Recovery candidate —")
        assert "currently unassigned" in item.toolTip()
        assert assistant._recovery_discovery_results == (recovery,)

        assistant.discovery_list.setCurrentRow(0)
        assert assistant.draft().source_locked is True
        assert assistant.draft().imported_id == 21
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_linked_empty_manifest_candidate_is_usable_with_provenance_warning() -> None:
    _app()
    snapshot = build_guided_instance_inventory(
        {
            "js8call": (
                {
                    "id": 31,
                    "name": "Linked JS8Call",
                    "system_key": "linked-js8",
                    "host": "127.0.0.1",
                    "port": 2442,
                    "udp_port": 2237,
                    "rig_name": "LINKED",
                    "application_path": "/opt/js8call",
                    "configuration_path": "/srv/fio/js8/linked/profile",
                    "storage_path": "/srv/fio/js8/linked/data",
                },
            )
        },
        linked_ids_by_family={"js8call": {31}},
        generation=2,
    )
    linked = snapshot.usable_rows_for("js8call")[0]
    assert linked["provenance_unverified"] is True

    assistant = SoftwareInstanceAssistant("js8call", unsaved_owner_key="draft-linked")
    try:
        assistant.source_buttons["discover"].setChecked(True)
        assistant.set_discovery_results((linked,))

        item = assistant.discovery_list.item(0)
        assert item.flags() & Qt.ItemIsEnabled
        assert item.flags() & Qt.ItemIsSelectable
        assert "Configuration provenance unverified" in item.text()
        assert "does not claim native ownership" in item.toolTip()
        assert assistant._diagnostic_discovery_results == ()

        assistant.discovery_list.setCurrentRow(0)
        assert assistant.draft().source_locked is True
        assert assistant.draft().imported_id == 31
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_diagnostic_presentation_consumes_core_metadata_without_enabled_or_path_inference() -> None:
    source = open("freqinout/gui/software_instance_assistant.py", encoding="utf-8").read()

    assert "candidate_classification" in source
    assert "candidate_reasons" in source
    assert "Diagnostic only" in source
    assert "cannot be imported, recommended, assigned, or launched" in source
    helper = source[
        source.index("def _existing_candidate_is_usable") : source.index(
            "def _diagnostic_candidate_reason"
        )
    ]
    assert 'row.get("enabled")' not in helper
    assert 'row.get("application_path")' not in helper


def test_settings_projects_manifest_and_radio_link_evidence_into_one_snapshot() -> None:
    rows = ({
        "id": 11,
        "system_key": "north-js8",
        "name": "North JS8",
        "host": "127.0.0.1",
        "port": 2442,
        "install_path": "/opt/js8call",
        "profile_path": "/profiles/north",
        "application_data_root": "/data/north",
        "rig_name": "NORTH",
    },)
    manifests = ({
        "instance_key": "js8call:north-js8",
        "family_key": "js8call",
        "application_system_key": "north-js8",
        "management_mode": "operator",
        "provenance": "detected",
        "observed_fingerprint": "abc123",
    },)
    profiles = ({"id": 7, "js8_instance_id": 11},)

    enriched = SettingsTab._enrich_software_instance_rows("js8call", rows, manifests)
    assert enriched[0]["manifest_present"] is True
    assert enriched[0]["observed_fingerprint"] == "abc123"
    linked = SettingsTab._linked_software_instance_ids(profiles)
    assert linked["js8call"] == (11,)
    snapshot = build_guided_instance_inventory(
        {"js8call": enriched},
        linked_ids_by_family=linked,
    )
    assert snapshot.usable_rows_for("js8call")[0]["candidate_usable"] is True


def test_add_radio_and_presave_rebuild_use_durable_link_projection() -> None:
    source = open("freqinout/gui/settings_tab.py", encoding="utf-8").read()
    dialog_source = source[
        source.index("def _open_device_profile_dialog") : source.index(
            "def _apply_runtime_projection_widgets"
        )
    ]
    rebuild_source = source[
        source.index("def _current_guided_instance_inventory") : source.index(
            "def _guided_radio_review_is_current"
        )
    ]

    assert "guided_linked_instance_ids = self._linked_software_instance_ids" in dialog_source
    assert "linked_ids_by_family=guided_linked_instance_ids" in dialog_source
    assert "profiles = self.multi_radio_store.list_device_profiles()" in rebuild_source
    assert "linked_ids_by_family=self._linked_software_instance_ids(profiles)" in rebuild_source
    for forbidden_history in (
        "message",
        "traffic",
        "ingest",
        "sync_history",
        "link_history",
    ):
        assert forbidden_history not in rebuild_source.casefold()
