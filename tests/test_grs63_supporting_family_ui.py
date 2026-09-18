"""Focused GRS-6.3 UI contracts for supporting software families."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QDialog, QLabel, QPushButton


_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    global _QT_APP
    application = QApplication.instance()
    if isinstance(application, QApplication):
        _QT_APP = application
    elif _QT_APP is None:
        _QT_APP = QApplication([])
    return _QT_APP


def test_add_radio_models_spotter_and_commstat_without_external_install_prompts(
    monkeypatch, tmp_path
) -> None:
    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)

    def inspect_dialog(dialog: QDialog) -> int:
        dialog.show()
        _app().processEvents()

        spotter = dialog.findChild(QCheckBox, "guidedSoftwareUse_fio_spotter")
        commstat = dialog.findChild(QCheckBox, "guidedSoftwareUse_commstat")
        js8call = dialog.findChild(QCheckBox, "guidedSoftwareUse_js8call")
        assert spotter is not None and commstat is not None and js8call is not None

        spotter.setChecked(True)
        commstat.setChecked(True)
        _app().processEvents()
        assert js8call.isChecked()

        spotter_source = dialog.findChild(QComboBox, "guidedSoftwareSource_fio_spotter")
        commstat_source = dialog.findChild(QComboBox, "guidedSoftwareSource_commstat")
        assert spotter_source is not None and spotter_source.currentData() == "built_in"
        assert commstat_source is not None and commstat_source.currentData() == "station_shared"
        assert not spotter_source.isEnabled() and not commstat_source.isEnabled()

        binding = dialog.findChild(QLabel, "guidedCommStatJs8Binding")
        assert binding is not None
        assert "Station CommStat" in binding.text()
        assert "JS8Call TCP" in binding.text()
        assert "not duplicated" in binding.text()

        visible_labels = {
            label.text().strip()
            for label in dialog.findChildren(QLabel)
            if label.isVisible() and label.text().strip()
        }
        assert "MCF Forms Folder:" not in visible_labels
        assert "CommStat App:" not in visible_labels

        spotter_details = dialog.findChild(QPushButton, "guidedSoftwareDetails_fio_spotter")
        commstat_details = dialog.findChild(QPushButton, "guidedSoftwareDetails_commstat")
        assert spotter_details is not None and spotter_details.text() == "Show Details / mapping…"
        assert commstat_details is not None and commstat_details.text() == "Show Details / JS8 binding…"
        assert not spotter_details.isEnabled() and not commstat_details.isEnabled()
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", inspect_dialog)
    tab = SettingsTab()
    try:
        assert tab._open_device_profile_dialog(existing=None) is None
    finally:
        tab.deleteLater()
        _app().processEvents()


def test_varac_cluster_choice_is_explicit_and_defaults_standalone() -> None:
    from freqinout.gui.software_instance_assistant import SoftwareInstanceAssistant

    assistant = SoftwareInstanceAssistant(
        "varac",
        initial_draft={
            "family_key": "varac",
            "mode": "managed",
            "ownership": "fio-managed",
            "instance_name": "New VarAC",
        },
    )
    try:
        cluster = assistant.findChild(QComboBox, "softwareInstanceClusterPath")
        why = assistant.findChild(QLabel, "softwareInstanceVaracClusterWhy")
        assert cluster is not None and cluster.currentData() == "standalone"
        assert [cluster.itemData(index) for index in range(cluster.count())] == [
            "standalone",
            "create_cluster",
            "join_cluster",
        ]
        assert why is not None
        assert "Standalone is the safe default" in why.text()
        assert why.accessibleName() == "Why VarAC defaults to standalone"
    finally:
        assistant.deleteLater()
        _app().processEvents()


def test_commstat_software_administration_is_binding_first() -> None:
    from freqinout.gui.software_administration_editor import task_definition
    from freqinout.gui.software_administration_workspace import _TASKS

    assert _TASKS["commstat"] == (
        ("overview", "Overview"),
        ("transport_mapping", "JS8 Endpoint Bindings"),
        ("health", "Shared Service Health"),
        ("advanced", "Advanced"),
    )
    transport = task_definition("commstat", "transport_mapping")
    assert transport is not None
    assert transport.title == "JS8 Endpoint Bindings"
    assert [field.key for field in transport.fields] == ["js8_host", "js8_port"]
    assert task_definition("commstat", "installation") is None
    assert task_definition("commstat", "launch") is None

    spotter = task_definition("fio_spotter", "dependencies")
    assert spotter is not None
    assert [field.key for field in spotter.fields] == ["js8_host", "js8_port"]
    assert "MCF folder" in spotter.description
