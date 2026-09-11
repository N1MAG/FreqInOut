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


def test_guided_observer_setup_keeps_receiver_manual_and_never_probes(monkeypatch, tmp_path) -> None:
    """The SDR-0 setup surface must be truthful before an adapter exists."""

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = _application_or_skip()

    from PySide6.QtWidgets import QComboBox, QDialog, QLabel, QLineEdit

    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)

    checked: dict[str, object] = {}

    def fake_exec(dialog: QDialog) -> int:
        application = dialog.findChild(QComboBox, "guidedReceiverApplication")
        target = dialog.findChild(QLineEdit, "guidedReceiverTarget")
        status = dialog.findChild(QLabel, "guidedReceiverManualStatus")
        guidance = dialog.findChild(QLabel, "guidedReceiverManualGuidance")
        assert application is not None
        assert target is not None
        assert status is not None
        assert guidance is not None
        assert application.findText("SDR++") >= 0
        assert application.findText("Other / manual") >= 0
        assert target.placeholderText() == "selected-vfo"
        assert "FIO is not controlling" in status.text()
        assert "manual tuning remains available" in guidance.text().lower()
        checked["size"] = (dialog.width(), dialog.height())
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    tab = SettingsTab()
    try:
        result = tab._open_device_profile_dialog(
            {
                "id": 44,
                "name": "Receiver",
                "device_class": "observer",
                "control_backend": "manual",
                "radio_model": "RTL-SDR",
                "sdr_application": "SDR++",
                "sdr_target": "VFO A",
                "sdr_host": "127.0.0.1",
                "sdr_port": 4532,
            }
        )
    finally:
        tab.deleteLater()
        app.processEvents()

    assert result is None
    assert checked["size"]


def test_receiver_setup_static_guard_has_no_probe_or_control_action() -> None:
    """Keep accidental Qt-thread endpoint I/O out of the SDR-0 settings card."""

    source = (
        __import__("pathlib").Path(__file__).parents[1] / "freqinout" / "gui" / "settings_tab.py"
    ).read_text(encoding="utf-8")
    setup_section = source.split('sdr_wrap = QGroupBox("Receiver setup")', 1)[1].split("port_prompt_group", 1)[0]
    renderer_section = source.split("def _update_receiver_manual_card", 1)[1].split("def _browse_guided_app_choice", 1)[0]
    assert "Manual tuning" in renderer_section
    assert "not controlling" in renderer_section
    assert "probe(" not in setup_section + renderer_section
    assert "socket" not in (setup_section + renderer_section).lower()
    assert "set_receive_frequency" not in setup_section + renderer_section
