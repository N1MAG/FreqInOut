from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QToolButton,
    QWidget,
)

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


def test_device_profile_dialog_uses_scrollable_collapsible_sections(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        def _fake_exec(self):
            root = self.layout()
            assert isinstance(root.itemAt(1).widget(), QScrollArea)
            assert isinstance(root.itemAt(2).widget(), QDialogButtonBox)

            scroll = self.findChild(QScrollArea)
            assert scroll is not None
            assert scroll.widget() is not None

            for object_name in (
                "deviceProfileBasicsSection",
                "deviceProfileEndpointsSection",
                "deviceProfileJs8Section",
                "deviceProfileVaracLaunchSection",
                "deviceProfileCoordinationSection",
            ):
                assert self.findChild(QWidget, object_name) is not None

            js8_header = self.findChild(QToolButton, "deviceProfileJs8SectionHeader")
            js8_content = self.findChild(QWidget, "deviceProfileJs8SectionContent")
            assert js8_header is not None
            assert js8_content is not None
            assert js8_header.isChecked() is False
            assert js8_content.isHidden() is True

            js8_header.click()
            app.processEvents()
            assert js8_header.isChecked() is True
            assert js8_content.isHidden() is False
            return QDialog.Rejected

        monkeypatch.setattr(QDialog, "exec", _fake_exec)
        assert tab._open_device_profile_dialog(existing=None) is None
    finally:
        tab.deleteLater()
        app.processEvents()


def test_device_profile_dialog_auto_expands_configured_advanced_sections(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        existing = {
            "name": "Field JS8",
            "js8_profile_path": "C:/JS8/Field",
            "varac_install_path": "C:/VarAC/Field",
            "ptt_group": "AMP-A",
            "notes": "Field deployment",
        }

        def _fake_exec(self):
            js8_header = self.findChild(QToolButton, "deviceProfileJs8SectionHeader")
            varac_header = self.findChild(QToolButton, "deviceProfileVaracLaunchSectionHeader")
            coordination_header = self.findChild(QToolButton, "deviceProfileCoordinationSectionHeader")
            assert js8_header is not None and js8_header.isChecked() is True
            assert varac_header is not None and varac_header.isChecked() is True
            assert coordination_header is not None and coordination_header.isChecked() is True
            return QDialog.Rejected

        monkeypatch.setattr(QDialog, "exec", _fake_exec)
        assert tab._open_device_profile_dialog(existing=existing) is None
    finally:
        tab.deleteLater()
        app.processEvents()


def test_device_profile_dialog_browse_buttons_populate_path_fields(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab
    from freqinout.gui import settings_tab as settings_tab_mod

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        chosen_profile_dir = str(tmp_path / "js8-instance")
        chosen_directed = str(tmp_path / "DIRECTED.TXT")
        chosen_launch_folder = str(tmp_path / "device-launch")

        monkeypatch.setattr(
            settings_tab_mod.QFileDialog,
            "getExistingDirectory",
            lambda *_args, **_kwargs: chosen_profile_dir,
        )
        monkeypatch.setattr(
            settings_tab_mod.QFileDialog,
            "getOpenFileName",
            lambda *_args, **_kwargs: (chosen_directed, ""),
        )

        def _fake_exec(self):
            js8_profile_btn = self.findChild(QPushButton, "deviceProfileJs8ProfileBrowse")
            js8_profile_edit = self.findChild(QLineEdit, "deviceProfileJs8ProfileEdit")
            js8_directed_btn = self.findChild(QPushButton, "deviceProfileJs8DirectedBrowse")
            js8_directed_edit = self.findChild(QLineEdit, "deviceProfileJs8DirectedEdit")
            launch_folder_btn = self.findChild(QPushButton, "deviceProfileLaunchPathFolderBrowse")
            launch_path_edit = self.findChild(QLineEdit, "deviceProfileLaunchPathEdit")

            assert js8_profile_btn is not None and js8_profile_edit is not None
            assert js8_directed_btn is not None and js8_directed_edit is not None
            assert launch_folder_btn is not None and launch_path_edit is not None

            js8_profile_btn.click()
            js8_directed_btn.click()

            monkeypatch.setattr(
                settings_tab_mod.QFileDialog,
                "getExistingDirectory",
                lambda *_args, **_kwargs: chosen_launch_folder,
            )
            launch_folder_btn.click()
            app.processEvents()

            assert js8_profile_edit.text() == chosen_profile_dir
            assert js8_directed_edit.text() == chosen_directed
            assert launch_path_edit.text() == chosen_launch_folder
            return QDialog.Rejected

        monkeypatch.setattr(QDialog, "exec", _fake_exec)
        assert tab._open_device_profile_dialog(existing=None) is None
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_js8_accordion_loads_default_and_inline_instance_cards(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    extra = store.save_js8_instance({"name": "Remote JS8 B"})
    store.save_fast_light_config({"name": "Remote FL B"})
    store.save_varac_node({"name": "Remote VarAC B"})

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        default_header = tab.findChild(QToolButton, "js8DefaultCardHeader")
        extra_header = tab.findChild(QToolButton, f"js8InstanceCard{int(extra['id'])}Header")
        assert default_header is not None
        assert extra_header is not None
        assert default_header.isChecked() is True
        assert extra_header.text().startswith("Remote JS8 B")
        assert tab.fast_light_configs_table.rowCount() == 2
        assert tab.varac_nodes_table.rowCount() == 2
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_js8_accordion_is_single_open(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    first = store.save_js8_instance({"name": "Remote JS8 A"})
    second = store.save_js8_instance({"name": "Remote JS8 B"})

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        default_header = tab.findChild(QToolButton, "js8DefaultCardHeader")
        first_header = tab.findChild(QToolButton, f"js8InstanceCard{int(first['id'])}Header")
        second_header = tab.findChild(QToolButton, f"js8InstanceCard{int(second['id'])}Header")
        default_content = tab.findChild(QWidget, "js8DefaultCardContent")
        first_content = tab.findChild(QWidget, f"js8InstanceCard{int(first['id'])}Content")
        second_content = tab.findChild(QWidget, f"js8InstanceCard{int(second['id'])}Content")

        assert default_header is not None and default_content is not None
        assert first_header is not None and first_content is not None
        assert second_header is not None and second_content is not None
        assert default_header.isChecked() is True
        assert default_content.isHidden() is False

        first_header.click()
        app.processEvents()
        assert first_header.isChecked() is True
        assert first_content.isHidden() is False
        assert default_header.isChecked() is False
        assert default_content.isHidden() is True

        second_header.click()
        app.processEvents()
        assert second_header.isChecked() is True
        assert second_content.isHidden() is False
        assert first_header.isChecked() is False
        assert first_content.isHidden() is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_js8_accordion_collapse_releases_card_height(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    extra = store.save_js8_instance({"name": "Remote JS8 C"})

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        default_header = tab.findChild(QToolButton, "js8DefaultCardHeader")
        extra_header = tab.findChild(QToolButton, f"js8InstanceCard{int(extra['id'])}Header")
        extra_group = tab.findChild(QWidget, f"js8InstanceCard{int(extra['id'])}")

        assert default_header is not None
        assert extra_header is not None
        assert extra_group is not None

        extra_header.click()
        app.processEvents()
        assert extra_header.isChecked() is True
        assert extra_group.maximumHeight() >= 100000

        default_header.click()
        app.processEvents()
        assert extra_header.isChecked() is False
        assert extra_group.maximumHeight() < 120
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_add_js8_instance_creates_inline_card_without_popup(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(
        SettingsTab,
        "_open_js8_instance_dialog",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Popup editor should not be used.")),
    )

    tab = SettingsTab()
    try:
        before = len(tab.js8_instances)
        tab._add_js8_instance()
        app.processEvents()
        after = len(tab.js8_instances)
        newest = max(tab.js8_instances, key=lambda row: int(row.get("id", 0) or 0))
        newest_header = tab.findChild(QToolButton, f"js8InstanceCard{int(newest['id'])}Header")
        assert after == before + 1
        assert newest_header is not None
        assert newest_header.isChecked() is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_device_profile_dialog_binding_selectors_disable_bound_fields(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    js8 = store.save_js8_instance({"name": "Bound JS8"})
    fast_light = store.save_fast_light_config({"name": "Bound FL"})
    varac = store.save_varac_node({"name": "Bound VarAC"})

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        existing = {
            "name": "Bound Device",
            "js8_instance_id": int(js8["id"]),
            "fast_light_config_id": int(fast_light["id"]),
            "varac_node_id": int(varac["id"]),
        }

        def _fake_exec(self):
            js8_combo = self.findChild(QComboBox, "deviceProfileJs8BindingCombo")
            fast_light_combo = self.findChild(QComboBox, "deviceProfileFastLightBindingCombo")
            varac_combo = self.findChild(QComboBox, "deviceProfileVaracBindingCombo")
            js8_host_edit = self.findChild(QLineEdit, "deviceProfileJs8HostEdit")
            flrig_host_edit = self.findChild(QLineEdit, "deviceProfileFlrigHostEdit")
            varac_install_edit = self.findChild(QLineEdit, "deviceProfileVaracInstallEdit")

            assert js8_combo is not None
            assert fast_light_combo is not None
            assert varac_combo is not None
            assert js8_host_edit is not None
            assert flrig_host_edit is not None
            assert varac_install_edit is not None

            assert js8_combo.currentData() == int(js8["id"])
            assert fast_light_combo.currentData() == int(fast_light["id"])
            assert varac_combo.currentData() == int(varac["id"])
            assert js8_host_edit.isEnabled() is False
            assert flrig_host_edit.isEnabled() is False
            assert varac_install_edit.isEnabled() is False
            return QDialog.Rejected

        monkeypatch.setattr(QDialog, "exec", _fake_exec)
        assert tab._open_device_profile_dialog(existing=existing) is None
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_backfills_js8_geo_via_background_dispatch(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    directed_path = tmp_path / "DIRECTED.TXT"
    directed_path.write_text("2026-03-16\t14.078\tRX\tK1ABC\tK1ABC: @N0CALL FN20\n", encoding="utf-8")

    from freqinout.gui.settings_tab import SettingsTab
    from freqinout.gui import settings_tab as settings_tab_mod

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(
        settings_tab_mod.JS8LogLinkIndexer,
        "backfill_geo_from_logs",
        lambda self: (_ for _ in ()).throw(AssertionError("JS8 geo backfill should not run on the UI thread.")),
    )

    tab = SettingsTab()
    try:
        captured: dict[str, object] = {}
        monkeypatch.setattr(tab, "_dispatch_js8_geo_backfill_request", lambda request: captured.update(request))

        tab.js8_directed_edit.setText(str(directed_path))
        tab._maybe_backfill_js8_geo()

        assert tab._js8_geo_backfill_inflight is True
        assert str(captured.get("db_path", "")).endswith("freqinout_nets.db")
        settings_payload = captured.get("settings")
        assert isinstance(settings_payload, dict)
        assert settings_payload.get("js8_directed_path") == str(directed_path)
    finally:
        tab._on_js8_geo_backfill_finished(0)
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_primary_switch_refreshes_projection_without_full_reload(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    remote = store.save_device_profile(
        {
            "name": "Remote JS8 Primary",
            "control_backend": "js8call",
            "js8_host": "10.0.0.15",
            "js8_port": 2642,
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        monkeypatch.setattr(
            tab,
            "_load_settings",
            lambda: (_ for _ in ()).throw(AssertionError("_load_settings should not run during primary switch projection refresh.")),
        )
        _select_device_profile(tab, int(remote["id"]))
        tab._set_active_selected_device_profile()

        assert tab.control_combo.currentText() == "JS8Call"
        assert tab.js8_host_edit.text() == "10.0.0.15"
        assert tab.js8_port_edit.text() == "2642"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_managed_js8_save_refreshes_projection_without_full_reload(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    js8 = store.save_js8_instance(
        {
            "name": "Primary JS8 Node",
            "host": "10.0.0.20",
            "port": 2542,
        }
    )
    store.save_device_profile(
        {
            "id": primary["id"],
            "name": primary["name"],
            "control_backend": "js8call",
            "js8_instance_id": int(js8["id"]),
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        monkeypatch.setattr(
            tab,
            "_load_settings",
            lambda: (_ for _ in ()).throw(AssertionError("_load_settings should not run after managed JS8 save.")),
        )

        tab._persist_js8_instance(
            {
                "id": int(js8["id"]),
                "name": "Primary JS8 Node",
                "host": "10.0.0.21",
                "port": 2642,
                "offset_hz": 15,
            }
        )

        assert tab.js8_host_edit.text() == "10.0.0.21"
        assert tab.js8_port_edit.text() == "2642"
    finally:
        tab.deleteLater()
        app.processEvents()
