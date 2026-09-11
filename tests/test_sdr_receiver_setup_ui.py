from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest


def _application_or_skip():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is not None and not isinstance(app, QApplication):
        pytest.skip("A non-GUI QCoreApplication already exists in this test process.")
    return app or QApplication([])


def _open_receiver_dialog(
    monkeypatch,
    tmp_path,
    profile: dict[str, object],
    inspect,
    *,
    service_ready: bool = False,
    configure_tab=None,
) -> None:
    from PySide6.QtWidgets import QDialog

    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)

    def fake_exec(dialog: QDialog) -> int:
        inspect(dialog)
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    app = _application_or_skip()
    tab = SettingsTab()
    tab.set_receiver_control_test_service_ready(service_ready)
    if configure_tab is not None:
        configure_tab(tab)
    try:
        assert tab._open_device_profile_dialog(profile) is None
    finally:
        tab.deleteLater()
        app.processEvents()


def test_receiver_setup_shows_adapter_choice_and_keeps_future_control_disabled(monkeypatch, tmp_path) -> None:
    """SDR-1 setup stores intent but cannot imply a live adapter exists."""

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QCheckBox, QComboBox, QLabel, QPushButton

        adapter = dialog.findChild(QComboBox, "guidedReceiverAdapter")
        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        test_control = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        assert adapter is not None
        assert verification is not None
        assert enabled is not None
        assert test_control is not None
        assert adapter.findData("manual") >= 0
        assert adapter.findData("sdrpp_rigctl") >= 0
        assert "Manual tuning" in verification.text()
        assert not enabled.isEnabled()
        assert not enabled.isChecked()
        assert not test_control.isEnabled()
        assert "asynchronous adapter service" in test_control.toolTip()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {
            "id": 41,
            "name": "RTL-SDR receiver",
            "device_class": "observer",
            "control_backend": "manual",
            "radio_model": "RTL-SDR",
            "sdr_application": "SDR++",
        },
        inspect,
    )


def test_receiver_setup_rejects_stale_persisted_evidence(monkeypatch, tmp_path) -> None:
    """Legacy or mismatched evidence must never enable receiver control."""

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QCheckBox, QComboBox, QLabel, QPushButton

        adapter = dialog.findChild(QComboBox, "guidedReceiverAdapter")
        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        test_control = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        assert adapter is not None
        assert verification is not None
        assert enabled is not None
        assert test_control is not None
        assert adapter.currentData() == "sdrpp_rigctl"
        assert "Verification pending" in verification.text()
        assert "does not match" in verification.text()
        assert not enabled.isEnabled()
        assert not enabled.isChecked()
        assert not test_control.isEnabled()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {
            "id": 42,
            "name": "RTL-SDR receiver",
            "device_class": "observer",
            "control_backend": "manual",
            "radio_model": "RTL-SDR",
            "sdr_application": "SDR++",
            "sdr_adapter": "sdrpp_rigctl",
            "sdr_target": "VFO A",
            "sdr_host": "127.0.0.1",
            "sdr_port": 4532,
            "sdr_control_enabled": 1,
            "sdr_verification_state": "verified",
            "sdr_verification_json": '{"tested_at":"2026-09-10","readback":true}',
        },
        inspect,
    )


def test_receiver_setup_enables_opt_in_only_for_matching_reversible_evidence(monkeypatch, tmp_path) -> None:
    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QCheckBox, QLabel, QPushButton

        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        test_control = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        assert verification is not None
        assert enabled is not None
        assert test_control is not None
        assert "FIO tuning ready" in verification.text()
        assert enabled.isEnabled()
        assert enabled.isChecked()
        assert test_control.isEnabled()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {
            "id": 43,
            "name": "RTL-SDR receiver",
            "device_class": "observer",
            "control_backend": "manual",
            "radio_model": "RTL-SDR",
            "sdr_application": "SDR++",
            "sdr_adapter": "sdrpp_rigctl",
            "sdr_target": "selected-vfo",
            "sdr_host": "127.0.0.1",
            "sdr_port": 4532,
            "sdr_control_enabled": 1,
            "sdr_verification_state": "verified",
            "sdr_verification_json": (
                '{"schema_version":1,"adapter":"sdrpp_rigctl","host":"127.0.0.1","port":4532,'
                '"target":"selected-vfo","tested_at_utc":"2026-09-10T12:00:00+00:00",'
                '"tune_readback_verified":true,"restore_readback_verified":true}'
            ),
        },
        inspect,
        service_ready=True,
    )


def test_receiver_setup_control_surface_is_cache_only() -> None:
    source = (
        __import__("pathlib").Path(__file__).parents[1] / "freqinout" / "gui" / "settings_tab.py"
    ).read_text(encoding="utf-8")
    surface = source.split("def _update_receiver_manual_card", 1)[1].split("def _browse_guided_app_choice", 1)[0]
    assert "saved evidence" in surface
    assert "FIO tuning ready" in surface
    assert ".probe(" not in surface
    assert "socket" not in surface.lower()
    assert "set_receive_frequency" not in surface


def test_async_test_result_enables_opt_in_without_endpoint_io_on_ui_thread(monkeypatch, tmp_path) -> None:
    def configure(tab) -> None:
        def complete(payload) -> None:
            evidence = {
                "schema_version": 1,
                "adapter": payload["sdr_adapter"],
                "host": payload["sdr_host"],
                "port": int(payload["sdr_port"]),
                "target": payload["sdr_target"],
                "tested_at_utc": "2026-09-10T12:00:00+00:00",
                "tune_readback_verified": True,
                "restore_readback_verified": True,
            }
            tab.receiver_control_test_completed.emit(
                {
                    "profile_id": int(payload["id"]),
                    "verification_state": "verified",
                    "detail": "Frequency tuning and restoration were verified.",
                    "verification": evidence,
                }
            )

        tab.receiver_control_test_requested.connect(complete)

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QApplication, QCheckBox, QLabel, QPushButton

        button = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        assert button is not None and button.isEnabled()
        assert enabled is not None and not enabled.isEnabled()
        button.click()
        QApplication.processEvents()
        QApplication.processEvents()
        assert enabled.isEnabled()
        assert not enabled.isChecked()
        assert verification is not None and "FIO tuning ready" in verification.text()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {
            "id": 44,
            "name": "RTL-SDR receiver",
            "device_class": "observer",
            "control_backend": "manual",
            "radio_model": "RTL-SDR",
            "sdr_application": "SDR++",
            "sdr_adapter": "sdrpp_rigctl",
            "sdr_target": "selected-vfo",
            "sdr_host": "127.0.0.1",
            "sdr_port": 4532,
            "sdr_control_enabled": 0,
            "sdr_verification_state": "unverified",
        },
        inspect,
        service_ready=True,
        configure_tab=configure,
    )
