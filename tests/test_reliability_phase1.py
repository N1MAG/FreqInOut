from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.config_paths import get_config_dir
from freqinout.core.settings_manager import SettingsManager
from freqinout.radio_interface.rigctl_client import flrig_client_from_settings


def test_logger_uses_runtime_config_dir(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.logger as logger

    logger = importlib.reload(logger)
    assert Path(logger._get_config_dir()) == get_config_dir()
    assert Path(logger._get_log_file()) == get_config_dir() / "freqinout.log"


def test_tool_db_schema_uses_runtime_config_dir(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    tools_dir = Path(__file__).resolve().parents[1] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))

    import db_schema

    db_schema = importlib.reload(db_schema)
    assert db_schema.CONFIG_DIR == cfg_root / "config"
    assert db_schema.SETTINGS_DB == cfg_root / "config" / "freqinout.db"
    assert db_schema.NETS_DB == cfg_root / "config" / "freqinout_nets.db"


def test_flrig_client_from_settings_uses_saved_port(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    settings.set("flrig_port", 23456)
    client = flrig_client_from_settings(settings)
    assert client.host == "127.0.0.1"
    assert client.port == 23456


def test_settings_tab_persists_flrig_port(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    tab = SettingsTab()
    try:
        tab.flrig_port_edit.setText("23456")
        tab._save_settings(show_message=False)
    finally:
        tab.deleteLater()
        app.processEvents()

    settings = SettingsManager()
    assert settings.get("flrig_port") == 23456


def test_settings_tab_uses_timezone_aware_utc_for_default_js8_offset(monkeypatch, tmp_path):
    import datetime as real_datetime
    import freqinout.gui.settings_tab as settings_tab_module

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    class FakeDateTime(real_datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 3, 14, 5, 0, 0, tzinfo=tz)

        @classmethod
        def utcnow(cls):
            raise AssertionError("datetime.utcnow() should not be used")

    monkeypatch.setattr(settings_tab_module.datetime, "datetime", FakeDateTime)
    monkeypatch.setattr(settings_tab_module.SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    app = QApplication.instance() or QApplication([])
    tab = settings_tab_module.SettingsTab()
    try:
        assert tab.js8_offset_edit.text() == "2150"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_tab_persists_fldigi_endpoint(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    tab = SettingsTab()
    try:
        tab.fldigi_host_edit.setText("10.10.10.9")
        tab.fldigi_port_edit.setText("7365")
        tab._save_settings(show_message=False)
    finally:
        tab.deleteLater()
        app.processEvents()

    settings = SettingsManager()
    assert settings.get("fldigi_host") == "10.10.10.9"
    assert settings.get("fldigi_port") == 7365
