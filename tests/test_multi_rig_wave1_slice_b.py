from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QCheckBox, QMessageBox

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager


def _select_device_profile(tab, profile_id: int) -> None:
    for row in range(tab.device_profiles_table.rowCount()):
        wrapper = tab.device_profiles_table.cellWidget(row, 0)
        chk = wrapper.findChild(QCheckBox) if wrapper is not None else None
        if chk is None:
            continue
        if int(chk.property("device_profile_id") or 0) == int(profile_id):
            chk.setChecked(True)
            return
    raise AssertionError(f"Device profile row not found: {profile_id}")


def test_settings_tab_loads_blank_device_profile_slate(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: QMessageBox.Ok)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: QMessageBox.Ok)

    tab = SettingsTab()
    try:
        assert tab.device_profiles_table.rowCount() == 0
        assert len(tab.device_profiles) == 0
        assert "Additional radios can be added" in tab.device_profiles_hint_label.text()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_add_device_profile_persists(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: QMessageBox.Ok)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: QMessageBox.Ok)

    tab = SettingsTab()
    try:
        monkeypatch.setattr(
            tab,
            "_open_device_profile_dialog",
            lambda existing=None: {
                "name": "Remote JS8",
                "control_backend": "js8call",
                "deployment_mode": "minimal",
                "js8_host": "10.0.0.10",
                "js8_port": 2542,
                "launch_enabled": False,
                "launch_path": "C:/Apps/JS8Call/JS8Call.exe",
                "notes": "Field kit",
            },
        )
        tab._add_device_profile()

        store = MultiRadioStore(settings_db_path())
        devices = store.list_device_profiles()
        assert len(devices) == 1
        assert any(str(row.get("name", "")) == "Remote JS8" for row in devices)
        assert tab.device_profiles_table.rowCount() == 1
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_set_active_device_profile_refreshes_projection_without_full_reload(monkeypatch, tmp_path):
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
            "js8_host": "10.0.0.10",
            "js8_port": 2542,
            "launch_enabled": False,
            "launch_path": "C:/Apps/JS8Call/JS8Call.exe",
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: QMessageBox.Ok)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: QMessageBox.Ok)

    tab = SettingsTab()
    try:
        monkeypatch.setattr(
            tab,
            "_load_settings",
            lambda: (_ for _ in ()).throw(AssertionError("_load_settings should not be used by Set Active.")),
        )
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


def test_settings_tab_set_active_device_profile_allows_rigctld(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    rigctld = store.save_device_profile(
        {
            "name": "Remote RIGCTLD",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: QMessageBox.Ok)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: QMessageBox.Ok)
    tab = SettingsTab()
    try:
        before = store.get_runtime_active_device_profile()
        assert before is None
        _select_device_profile(tab, int(rigctld["id"]))
        tab._set_active_selected_device_profile()

        after = store.get_runtime_active_device_profile()
        assert after is not None
        assert int(after["id"]) == int(rigctld["id"])
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
    plan = store.save_frequency_plan({"name": "Remove Radio Test Plan", "category": "normal"})
    store.set_assigned_plan(int(remote["id"]), int(plan["id"]))
    assert store.get_effective_assigned_plan_for_device(int(remote["id"])) is not None

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: QMessageBox.Ok)

    tab = SettingsTab()
    try:
        _select_device_profile(tab, int(remote["id"]))
        tab._delete_device_profiles()

        devices = store.list_device_profiles()
        assert len(devices) == 0
        assert all(int(row.get("id", 0) or 0) != int(remote["id"]) for row in devices)
        assert store.get_effective_assigned_plan_for_device(int(remote["id"])) is None
        assert all(
            int(row.get("device_profile_id", 0) or 0) != int(remote["id"])
            for row in store.list_assigned_plans()
        )
        assert tab.device_profiles_table.rowCount() == 0
    finally:
        tab.deleteLater()
        app.processEvents()
