from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QCheckBox, QMessageBox

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager


def _select_device_profile(tab, profile_id: int) -> None:
    for row in range(tab.device_profiles_table.rowCount()):
        widget = tab.device_profiles_table.cellWidget(row, 0)
        chk = widget.findChild(QCheckBox) if widget is not None else None
        if chk is None:
            continue
        if int(chk.property("device_profile_id") or 0) == int(profile_id):
            chk.setChecked(True)
            return
    raise AssertionError(f"Device profile row not found: {profile_id}")


def test_settings_tab_add_device_profile_persists(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        monkeypatch.setattr(
            tab,
            "_open_device_profile_dialog",
            lambda existing=None: {
                "name": "Remote JS8",
                "enabled": True,
                "control_backend": "js8call",
                "deployment_mode": "minimal",
                "flrig_host": "10.0.0.8",
                "flrig_port": 22345,
                "fldigi_host": "10.0.0.9",
                "fldigi_port": 7364,
                "js8_host": "10.0.0.10",
                "js8_port": 2542,
                "launch_enabled": False,
                "launch_path": "C:/Apps/JS8Call",
                "notes": "Field kit",
            },
        )
        tab._add_device_profile()

        store = MultiRadioStore(settings_db_path())
        devices = store.list_device_profiles()
        assert len(devices) == 2
        assert any(row["name"] == "Remote JS8" for row in devices)
        assert tab.device_profiles_table.rowCount() == 2
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_set_active_device_profile_refreshes_legacy_controls(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    remote = store.save_device_profile(
        {
            "name": "Remote JS8",
            "control_backend": "js8call",
            "deployment_mode": "minimal",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
            "fldigi_host": "10.0.0.9",
            "fldigi_port": 7364,
            "js8_host": "10.0.0.10",
            "js8_port": 2542,
            "launch_enabled": False,
            "launch_path": "C:/Apps/JS8Call",
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        _select_device_profile(tab, int(remote["id"]))
        tab._set_active_selected_device_profile()

        active = store.get_runtime_active_device_profile()
        assert active is not None
        assert int(active["id"]) == int(remote["id"])
        assert tab.control_combo.currentText() == "JS8Call"
        assert tab.js8_host_edit.text() == "10.0.0.10"
        assert tab.js8_port_edit.text() == "2542"
        assert tab.launch_all_with_startup_chk.isChecked() is False
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_can_activate_rigctld_device_profile(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    rigctld = store.save_device_profile(
        {
            "name": "Remote Rigctld",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        _select_device_profile(tab, int(rigctld["id"]))
        assert tab.set_active_device_profile_btn.isEnabled()
        tab._set_active_selected_device_profile()

        active = store.get_runtime_active_device_profile()
        assert active is not None
        assert int(active["id"]) == int(rigctld["id"])
        assert tab.control_combo.currentText() == "RIGCTLD"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_delete_device_profile_persists(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    remote = store.save_device_profile(
        {
            "name": "Remote Manual",
            "control_backend": "manual",
            "deployment_mode": "minimal",
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: QMessageBox.Ok)

    tab = SettingsTab()
    try:
        _select_device_profile(tab, int(remote["id"]))
        tab._delete_device_profiles()

        devices = store.list_device_profiles()
        assert len(devices) == 1
        assert all(int(row["id"]) != int(remote["id"]) for row in devices)
        assert tab.device_profiles_table.rowCount() == 1
    finally:
        tab.deleteLater()
        app.processEvents()
