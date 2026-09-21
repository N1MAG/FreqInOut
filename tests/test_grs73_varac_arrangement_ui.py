"""Focused real-widget coverage for GRS-7.3 VarAC arrangement intent."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Mapping

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QDialog, QLabel, QLineEdit, QPushButton


_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    global _QT_APP
    app = QApplication.instance()
    if isinstance(app, QApplication):
        _QT_APP = app
    elif _QT_APP is None:
        _QT_APP = QApplication([])
    return _QT_APP


def _open_add_radio_dialog(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    presentation: Mapping[str, object],
    inspect_dialog: Callable[[QDialog], None],
) -> None:
    from freqinout.core.settings_manager import SettingsManager
    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)
    monkeypatch.setattr(
        settings_tab_module,
        "recommend_varac_arrangement_from_snapshots",
        lambda *_args, **_kwargs: dict(presentation),
    )
    tab = SettingsTab()

    def fake_exec(dialog: QDialog) -> int:
        dialog.resize(900, 560)
        dialog.show()
        _app().processEvents()
        inspect_dialog(dialog)
        return dialog.result()

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    try:
        assert tab._open_device_profile_dialog(existing=None) is None
    finally:
        tab.deleteLater()
        _app().processEvents()


def _show_software_step(dialog: QDialog) -> None:
    setup = dialog.findChild(QComboBox, "guidedSetupType")
    assert setup is not None
    custom_index = setup.findData("custom")
    assert custom_index >= 0
    setup.setCurrentIndex(custom_index)
    model = dialog.findChild(QPushButton, "guidedWizardStep_model")
    software = dialog.findChild(QPushButton, "guidedWizardStep_software")
    assert model is not None and software is not None
    model.click()
    _app().processEvents()
    software.click()
    _app().processEvents()
    varac = dialog.findChild(QCheckBox, "guidedSoftwareUse_varac")
    assert varac is not None
    varac.setChecked(True)
    _app().processEvents()


def _visible_text(dialog: QDialog) -> str:
    return "\n".join(
        label.text().strip()
        for label in dialog.findChildren(QLabel)
        if label.isVisible() and label.text().strip()
    )


def test_fresh_station_defaults_to_standalone_without_cluster_details(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    presentation = {
        "default_path": "standalone",
        "recommended_path": "",
        "join_choices": (),
        "standalone_candidates": (),
        "existing_setup_summary": "Existing setup: No VarAC node or cluster is configured.",
        "create_choice_label": "Create the first VarAC cluster",
        "new_radio_label": "new radio",
        "needs_attention": False,
        "why": "Existing cluster membership is never selected automatically.",
    }

    def inspect(dialog: QDialog) -> None:
        _show_software_step(dialog)
        combo = dialog.findChild(QComboBox, "guidedVaracArrangement")
        details = dialog.findChild(QPushButton, "guidedSoftwareDetails_varac")
        assert combo is not None and details is not None
        assert combo.currentData() == "standalone"
        assert combo.findData("join_cluster") < 0
        assert not details.isVisible() and not details.isEnabled()
        assert "No VarAC node or cluster is configured." in _visible_text(dialog)
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, presentation, inspect)


def test_existing_cluster_has_named_join_choice_and_safe_standalone_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    presentation = {
        "default_path": "standalone",
        "recommended_path": "",
        "join_choices": (
            {
                "cluster_id": "front range",
                "cluster_db_id": 11,
                "label": "Front Range",
                "next_instance_number": 3,
            },
        ),
        "standalone_candidates": (),
        "existing_setup_summary": "Existing setup: 1 VarAC cluster configured.",
        "create_choice_label": "Create a new VarAC cluster",
        "new_radio_label": "TriMode",
        "needs_attention": False,
        "why": "Existing cluster membership is never selected automatically.",
    }

    def inspect(dialog: QDialog) -> None:
        _show_software_step(dialog)
        combo = dialog.findChild(QComboBox, "guidedVaracArrangement")
        assert combo is not None
        assert combo.currentData() == "standalone"
        join_index = next(
            index for index in range(combo.count()) if combo.itemData(index) == "join_cluster"
        )
        assert combo.itemText(join_index) == "Join Front Range (next member 3)"
        metadata = combo.itemData(join_index, 256 + 1)
        assert metadata["cluster_id"] == "front range"
        assert metadata["cluster_instance_number"] == 3
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, presentation, inspect)


def test_one_standalone_requires_explicit_recommended_create_choice(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    presentation = {
        "default_path": "",
        "recommended_path": "create_cluster",
        "recommended_existing_node_id": 7,
        "recommended_existing_device_profile_id": 4,
        "existing_member_instance_number": 1,
        "new_member_instance_number": 2,
        "join_choices": (),
        "standalone_candidates": (
            {"node_id": 7, "device_profile_id": 4, "label": "FTDX-10 VarAC"},
        ),
        "existing_setup_summary": "Existing setup: FTDX-10 VarAC is standalone. No VarAC cluster is configured.",
        "create_choice_label": "Create a cluster with FTDX-10 VarAC and TriMode — Recommended",
        "proposed_create_cluster_name": "FTDX-10 + TriMode VarAC",
        "proposed_create_cluster_id": "VARAC-FTDX-10-TRIMODE",
        "new_radio_label": "TriMode",
        "needs_attention": False,
        "why": "Choose the recommended cluster arrangement explicitly to include the existing standalone node; otherwise keep the standalone alternative.",
    }

    def inspect(dialog: QDialog) -> None:
        _show_software_step(dialog)
        combo = dialog.findChild(QComboBox, "guidedVaracArrangement")
        details = dialog.findChild(QPushButton, "guidedSoftwareDetails_varac")
        assert combo is not None and details is not None
        assert combo.currentData() == ""
        assert combo.currentText() == "Choose VarAC arrangement…"
        create_index = next(
            index for index in range(combo.count()) if combo.itemData(index) == "create_cluster"
        )
        assert combo.itemText(create_index) == presentation["create_choice_label"]
        create_metadata = combo.itemData(create_index, 256 + 1)
        assert create_metadata["proposed_cluster_name"] == "FTDX-10 + TriMode VarAC"
        assert create_metadata["proposed_cluster_id"] == "VARAC-FTDX-10-TRIMODE"
        assert combo.itemText(combo.findData("standalone")) == "Create another standalone VarAC node"
        assert presentation["existing_setup_summary"] in _visible_text(dialog)
        combo.setCurrentIndex(create_index)
        _app().processEvents()
        assert combo.currentData() == "create_cluster"
        # Intent alone never opens technical cluster fields; preparation is
        # still the boundary before details can be reviewed.
        assert not details.isVisible() and not details.isEnabled()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, presentation, inspect)


def test_matching_partial_cluster_is_presented_as_explicit_resume_choice(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    presentation = {
        "default_path": "",
        "recommended_path": "join_cluster",
        "join_choices": (
            {
                "cluster_id": "varac-ftdx-10-ft-710",
                "cluster_db_id": 11,
                "label": "FTDX-10 + FT-710 VarAC",
                "next_instance_number": 2,
                "resume_recommended": True,
            },
        ),
        "standalone_candidates": (),
        "existing_setup_summary": "Existing setup: FTDX-10 + FT-710 VarAC already contains the first reviewed radio.",
        "create_choice_label": "Create a different new VarAC cluster",
        "new_radio_label": "FT-710",
        "needs_attention": False,
        "why": "Resume the matching reviewed cluster explicitly to add this radio as its next member.",
    }

    def inspect(dialog: QDialog) -> None:
        _show_software_step(dialog)
        combo = dialog.findChild(QComboBox, "guidedVaracArrangement")
        assert combo is not None
        assert combo.currentData() == ""
        join_index = next(
            index for index in range(combo.count()) if combo.itemData(index) == "join_cluster"
        )
        assert combo.itemText(join_index) == (
            "Resume FTDX-10 + FT-710 VarAC: add FT-710 as member 2 — Recommended"
        )
        metadata = combo.itemData(join_index, 256 + 1)
        assert metadata["cluster_id"] == "varac-ftdx-10-ft-710"
        assert metadata["cluster_instance_number"] == 2
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, presentation, inspect)


def test_varac_arrangement_metadata_survives_the_shared_assistant_round_trip() -> None:
    from freqinout.gui.software_instance_assistant import SoftwareInstanceAssistant

    assistant = SoftwareInstanceAssistant(
        "varac",
        unsaved_owner_key="guided-radio-test",
        unsaved_radio_label="TriMode",
        initial_draft={
            "family_key": "varac",
            "instance_name": "TriMode",
            "cluster_path": "create_cluster",
            "cluster_instance_number": 2,
            "existing_standalone_node_id": 7,
            "existing_standalone_device_profile_id": 4,
            "existing_standalone_member_number": 1,
        },
    )
    try:
        payload = assistant.draft().payload()
        assert payload["cluster_path"] == "create_cluster"
        assert payload["cluster_instance_number"] == 2
        assert payload["existing_standalone_node_id"] == 7
        assert payload["existing_standalone_device_profile_id"] == 4
        assert payload["existing_standalone_member_number"] == 1

        cluster_path = assistant._field_widgets["cluster_path"]
        assert isinstance(cluster_path, QComboBox)
        cluster_path.setCurrentIndex(cluster_path.findData("standalone"))
        changed_route = assistant.draft().payload()
        assert changed_route["cluster_path"] == "standalone"
        assert changed_route["existing_standalone_node_id"] == 0
        assert changed_route["existing_standalone_member_number"] == 0
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_multiple_standalone_create_choice_survives_radio_rename(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    presentation = {
        "default_path": "",
        "recommended_path": "create_cluster",
        "join_choices": (),
        "standalone_candidates": (
            {"node_id": 7, "device_profile_id": 4, "label": "North VarAC"},
            {"node_id": 8, "device_profile_id": 5, "label": "South VarAC"},
        ),
        "existing_setup_summary": "Existing setup: 2 standalone VarAC nodes are configured. No VarAC cluster is configured.",
        "create_choice_label": "Create a cluster with a selected standalone node and new radio — Needs attention",
        "new_radio_label": "new radio",
        "needs_attention": True,
        "why": "Choose a standalone node explicitly before creating a cluster.",
    }

    def inspect(dialog: QDialog) -> None:
        _show_software_step(dialog)
        combo = dialog.findChild(QComboBox, "guidedVaracArrangement")
        assert combo is not None
        selected_index = next(
            index
            for index in range(combo.count())
            if combo.itemData(index) == "create_cluster"
            and combo.itemData(index, 256 + 1)["existing_node_id"] == 8
        )
        combo.setCurrentIndex(selected_index)
        radio_name = next(
            field
            for field in dialog.findChildren(QLineEdit)
            if field.placeholderText().startswith("Enter a radio name")
        )
        radio_name.setText("TriMode")
        _app().processEvents()
        assert combo.currentData() == "create_cluster"
        assert combo.itemData(combo.currentIndex(), 256 + 1)["existing_node_id"] == 8
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, presentation, inspect)
