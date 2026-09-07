from __future__ import annotations

import os
import time
import threading

import pytest


os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _settings_tab_or_skip(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    return app, SettingsTab()


def test_mesh_connection_name_is_visible_and_protocol_sensitive(monkeypatch, tmp_path):
    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        assert tab.mesh_connection_name_edit.text() == "meshtastic-1"
        assert tab.mesh_adapter_id_edit.placeholderText() == "Stable internal identifier (optional)"
        assert "control bar and connect menu" in tab.mesh_connection_name_edit.toolTip().lower()

        tab.mesh_protocol_combo.setCurrentIndex(1)
        app.processEvents()
        assert tab.mesh_connection_name_edit.text() == "meshcore-1"

        tab.mesh_connection_name_edit.setText("Backcountry relay")
        tab._on_mesh_connection_name_edited("Backcountry relay")
        tab.mesh_protocol_combo.setCurrentIndex(0)
        app.processEvents()
        assert tab.mesh_connection_name_edit.text() == "Backcountry relay"
        assert tab.mesh_connection_name_auto_label.text() == "Custom name"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_mesh_connection_editor_persists_name_and_optional_source(monkeypatch, tmp_path):
    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        tab.mesh_connection_name_edit.setText("Field gateway")
        tab._on_mesh_connection_name_edited("Field gateway")
        tab.mesh_adapter_id_edit.setText("mesh-adapter-42")
        tab.mesh_source_radio_id_edit.setText("radio-7")
        tab.mesh_source_role_edit.setText("gateway")

        config = tab._mesh_config_from_ui()
        assert config.connection_name == "Field gateway"
        assert config.connection_name_auto is False
        assert config.adapter_id == "mesh-adapter-42"
        assert config.source_radio_id == "radio-7"
        assert config.source_role == "gateway"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_mesh_add_device_selector_shows_explicit_new_device_row(monkeypatch, tmp_path):
    from freqinout.core.mesh import MeshConnectionConfig, MeshConnectionType, serialize_mesh_connection_library

    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        saved = MeshConnectionConfig(
            adapter_id="meshcore-field",
            protocol="meshcore",
            enabled=True,
            connection_type=MeshConnectionType.BLE,
            ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
            ble_device_name="MeshCore-N1MAG MOBL1",
        )
        tab.settings.set_many({"mesh_connection_library": serialize_mesh_connection_library((saved,))}, save=False)
        tab._load_mesh_settings_from_data(tab.settings.all())

        tab._on_mesh_add_device_clicked()
        app.processEvents()

        assert tab.mesh_saved_device_combo.itemText(0) == "New device — scan or configure below"
        assert tab.mesh_saved_device_combo.itemData(0) == ""
        assert tab.mesh_saved_device_combo.currentIndex() == 0
        assert tab.mesh_saved_device_combo.currentText() == "New device — scan or configure below"
        assert tab.mesh_saved_device_combo.itemText(1).startswith("MeshCore-N1MAG MOBL1")
        assert tab._mesh_adding_new_connection is True
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_mesh_ble_results_visibility_follows_scan_or_results(monkeypatch, tmp_path):
    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        tab.show()
        tab.mesh_protocol_combo.setCurrentIndex(1)
        tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "ble", fallback="tcp")
        tab._mesh_ble_scan_results_present = False
        tab._refresh_mesh_connection_visibility()
        assert not tab.mesh_ble_results_row.isHidden()
        assert tab.mesh_ble_results_combo.count() >= 1

        tab._on_mesh_ble_scan_finished(())
        assert not tab.mesh_ble_results_row.isHidden()
        assert tab.mesh_ble_results_combo.currentText() == "No device found — choose Scan to try again"
        assert tab.mesh_ble_use_selected_btn.isEnabled() is False
        assert "No new MeshCore BLE devices found" in tab.mesh_status_label.text()

        tab._on_mesh_ble_scan_failed("temporary scan failure")
        app.processEvents()
        assert not tab.mesh_ble_results_row.isHidden()
        assert tab.mesh_ble_results_combo.currentText() == "Scan failed — choose Scan to retry"
        assert tab.mesh_ble_use_selected_btn.isEnabled() is False

        tab._set_mesh_ble_scan_controls(True)
        assert not tab.mesh_protocol_combo.isEnabled()
        assert not tab.mesh_connection_type_combo.isEnabled()
        tab._set_mesh_ble_scan_controls(False)
        assert tab.mesh_protocol_combo.isEnabled()
        assert tab.mesh_connection_type_combo.isEnabled()

        tab.mesh_protocol_combo.setCurrentIndex(0)
        app.processEvents()
        tab._refresh_mesh_connection_visibility()
        assert tab.mesh_ble_results_row.isHidden()

        assert "Bluetooth settings for this computer" in tab.mesh_ble_guidance_label.text()
        assert tab.mesh_ble_use_selected_btn.text() == "Use Device"
        assert "save this discovered device" in tab.mesh_ble_use_selected_btn.toolTip().lower()
        assert "advertised ble name" in tab.mesh_ble_device_name_edit.toolTip().lower()
        assert "stable ble identifier" in tab.mesh_ble_device_id_edit.toolTip().lower()
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_mesh_ble_selected_action_survives_empty_or_failed_follow_up_scan(monkeypatch, tmp_path):
    from freqinout.core.mesh import MeshCoreBleAdvertisement

    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        tab.show()
        tab.mesh_protocol_combo.setCurrentIndex(1)
        tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "ble", fallback="tcp")
        device = MeshCoreBleAdvertisement("MOBL1", "device-address")
        tab._show_mesh_ble_scan_results((device,))
        tab._mesh_ble_scan_thread = None
        tab._on_mesh_ble_scan_finished(())
        assert tab._mesh_ble_scan_results_present
        assert tab.mesh_ble_use_selected_btn.isEnabled()
        tab._on_mesh_ble_scan_failed("temporary scan failure")
        assert tab.mesh_ble_use_selected_btn.isEnabled()
        assert not tab.mesh_ble_results_row.isHidden()
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_mesh_connect_saved_and_disconnect_actions_are_visible_enabled_and_emit_signals(monkeypatch, tmp_path):
    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.core.mesh import mesh_connection_config_key

    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    health_rows = [
        {
            "adapter_id": "meshcore-field",
            "device_name": "MeshCore-N1MAG MOBL1",
            "connected": False,
            "lifecycle_state": "not_connected",
            "last_error": "",
            "updated_utc": "2026-09-06T10:00:00Z",
        }
    ]
    monkeypatch.setattr(settings_tab_module, "list_mesh_health", lambda _db_path, transport=None: list(health_rows))
    try:
        tab.show()
        tab._select_settings_section_group(tab.local_mesh_section_group)
        tab.mesh_enabled_chk.setChecked(True)
        tab.mesh_protocol_combo.setCurrentIndex(1)
        tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "ble", fallback="tcp")
        tab.mesh_adapter_id_edit.setText("meshcore-field")
        tab.mesh_ble_device_id_edit.setText("97C92879-047E-FEA8-7A11-8A2EE82B381D")
        tab.mesh_ble_device_name_edit.setText("MeshCore-N1MAG MOBL1")

        connect_events: list[str] = []
        disconnect_events: list[str] = []
        tab.mesh_connect_requested.connect(connect_events.append)
        tab.mesh_disconnect_requested.connect(lambda: disconnect_events.append("disconnect"))

        tab._refresh_mesh_config_status()
        app.processEvents()

        assert tab.mesh_connect_saved_btn.isVisible()
        assert tab.mesh_disconnect_btn.isVisible()
        assert tab.mesh_connect_saved_btn.isEnabled()
        assert tab.mesh_disconnect_btn.isEnabled()

        tab.mesh_connect_saved_btn.click()
        tab.mesh_disconnect_btn.click()

        assert connect_events == [mesh_connection_config_key(tab._mesh_config_from_ui())]
        assert disconnect_events == ["disconnect"]

        health_rows[:] = [
            {
                "adapter_id": "meshcore-field",
                "device_name": "MeshCore-N1MAG MOBL1",
                "connected": True,
                "lifecycle_state": "connected",
                "last_error": "",
                "updated_utc": "2026-09-06T10:05:00Z",
            }
        ]

        tab._refresh_mesh_config_status()
        app.processEvents()

        assert not tab.mesh_connect_saved_btn.isEnabled()
        assert tab.mesh_disconnect_btn.isEnabled()
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_legacy_empty_mesh_slots_do_not_appear_or_count_as_pending(monkeypatch, tmp_path):
    from freqinout.core.mesh import MeshChannelPolicy

    _app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        phantom = MeshChannelPolicy(
            "meshcore-mobl1",
            "meshcore",
            "12",
            "Channel 12",
            review_state="pending",
            source="device",
        )
        configured_unnamed = MeshChannelPolicy(
            "meshcore-mobl1",
            "meshcore",
            "2",
            "Channel 2",
            review_state="pending",
            source="device",
        )
        fio_created = MeshChannelPolicy(
            "meshcore-mobl1",
            "meshcore",
            "19",
            "Channel 19",
            review_state="pending",
            source="operator",
        )

        assert tab._mesh_policy_is_legacy_empty_slot(phantom, {"0", "1", "2"}) is True
        assert tab._mesh_policy_is_legacy_empty_slot(configured_unnamed, {"0", "1", "2"}) is False
        assert tab._mesh_policy_is_legacy_empty_slot(fio_created, {"0", "1", "2"}) is False
    finally:
        tab.deleteLater()
        _app.processEvents()


@pytest.mark.parametrize("text_size", ["normal", "large"])
def test_mesh_ble_editor_remains_readable_at_compact_text_sizes(monkeypatch, tmp_path, text_size):
    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        tab.settings.set("ui_text_size", text_size)
        tab.apply_theme()
        tab.resize(900, 560)
        tab.show()
        tab._select_settings_section_group(tab.local_mesh_section_group)
        tab.mesh_protocol_combo.setCurrentIndex(1)
        tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "ble", fallback="tcp")
        app.processEvents()

        assert not tab.mesh_ble_row.isHidden()
        assert tab.mesh_ble_device_id_edit.height() >= tab.mesh_ble_device_id_edit.sizeHint().height()
        assert tab.mesh_ble_device_name_edit.height() >= tab.mesh_ble_device_name_edit.sizeHint().height()
        assert tab.mesh_ble_timeout_spin.height() >= tab.mesh_ble_timeout_spin.sizeHint().height()
        assert tab.mesh_ble_scan_state_label.height() >= tab.mesh_ble_scan_state_label.sizeHint().height()
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_slow_mesh_ble_scan_keeps_ui_responsive_and_cancels(monkeypatch, tmp_path):
    from freqinout.core.mesh import MeshOperationCancelled
    import freqinout.gui.settings_tab as settings_tab_module

    def slow_scan(_timeout_sec, *, cancel_event, progress_callback=None):
        started = time.monotonic()
        while time.monotonic() - started < 5.0:
            if cancel_event.wait(0.01):
                raise MeshOperationCancelled("test scan cancelled")
        return ()

    monkeypatch.setattr(settings_tab_module, "discover_meshcore_ble_devices", slow_scan)
    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        tab.show()
        tab._select_settings_section_group(tab.local_mesh_section_group)
        tab.mesh_protocol_combo.setCurrentIndex(1)
        tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "ble", fallback="tcp")
        app.processEvents()

        started = time.monotonic()
        tab.mesh_ble_scan_btn.click()
        app.processEvents()
        acknowledgement_ms = (time.monotonic() - started) * 1000.0

        assert acknowledgement_ms < 100.0
        assert tab.mesh_ble_scan_btn.text() == "Cancel Scan"
        for width, height in ((900, 560), (1100, 700), (900, 560)):
            interaction_started = time.monotonic()
            tab.resize(width, height)
            app.processEvents()
            assert (time.monotonic() - interaction_started) * 1000.0 < 100.0

        tab.mesh_ble_scan_btn.click()
        deadline = time.monotonic() + 2.0
        while tab._mesh_ble_scan_is_active() and time.monotonic() < deadline:
            app.processEvents()
            time.sleep(0.01)
        assert not tab._mesh_ble_scan_is_active()
        assert "cancelled" in tab.mesh_status_label.text().casefold()
    finally:
        tab.shutdown()
        deadline = time.monotonic() + 2.0
        while tab._mesh_ble_scan_is_active() and time.monotonic() < deadline:
            app.processEvents()
            time.sleep(0.01)
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_shutdown_during_mesh_ble_scan_cancels_promptly(monkeypatch, tmp_path):
    from freqinout.core.mesh import MeshOperationCancelled
    import freqinout.gui.settings_tab as settings_tab_module

    scan_started = time.monotonic()
    scan_ready = threading.Event()

    def slow_scan(_timeout_sec, *, cancel_event, progress_callback=None):
        scan_ready.set()
        while not cancel_event.wait(0.01):
            pass
        raise MeshOperationCancelled("test scan cancelled by shutdown")

    monkeypatch.setattr(settings_tab_module, "discover_meshcore_ble_devices", slow_scan)
    app, tab = _settings_tab_or_skip(monkeypatch, tmp_path)
    try:
        tab.show()
        tab._select_settings_section_group(tab.local_mesh_section_group)
        tab.mesh_protocol_combo.setCurrentIndex(1)
        tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "ble", fallback="tcp")
        app.processEvents()

        tab.mesh_ble_scan_btn.click()
        app.processEvents()

        assert tab.mesh_ble_scan_btn.text() == "Cancel Scan"
        assert scan_ready.wait(1.0)

        tab.shutdown()

        deadline = time.monotonic() + 2.0
        while tab._mesh_ble_scan_is_active() and time.monotonic() < deadline:
            app.processEvents()
            time.sleep(0.01)

        assert not tab._mesh_ble_scan_is_active()
        assert "cancelled" in tab.mesh_status_label.text().casefold()
        assert time.monotonic() - scan_started < 2.0
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()
