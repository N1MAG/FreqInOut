from __future__ import annotations

import os
import sys

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from freqinout.core.software_status_service import SoftwareStatusService


class DummySettings:
    def __init__(self, values: dict[str, object] | None = None):
        self._values = dict(values or {})

    def get(self, key: str, default=None):
        return self._values.get(key, default)


def test_flrig_api_reachable_uses_saved_port(monkeypatch):
    import freqinout.radio_interface.rigctl_client as rigctl_client

    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, host="127.0.0.1", port=12345, timeout=0.8, **kwargs):
            seen["host"] = host
            seen["port"] = port
            seen["timeout"] = timeout

        def is_available(self) -> bool:
            return True

    SoftwareStatusService._shared_service_probe_cache.clear()
    monkeypatch.setattr(rigctl_client, "FLRigClient", FakeClient)

    service = SoftwareStatusService(DummySettings({"flrig_port": 23456}))
    assert service.flrig_api_reachable(force=True)
    assert seen == {"host": "127.0.0.1", "port": 23456, "timeout": 0.35}


def test_fldigi_api_reachable_uses_saved_endpoint(monkeypatch):
    import freqinout.radio_interface.rigctl_client as rigctl_client

    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, host="127.0.0.1", port=12345, timeout=0.8, **kwargs):
            seen["host"] = host
            seen["port"] = port
            seen["timeout"] = timeout
            seen.update(kwargs)

        def is_fldigi_available(self) -> bool:
            return True

    SoftwareStatusService._shared_service_probe_cache.clear()
    monkeypatch.setattr(rigctl_client, "FLRigClient", FakeClient)

    service = SoftwareStatusService(
        DummySettings(
            {
                "flrig_host": "10.0.0.8",
                "flrig_port": 22345,
                "fldigi_host": "10.0.0.9",
                "fldigi_port": 7365,
            }
        )
    )
    assert service.fldigi_api_reachable(force=True)
    assert seen == {
        "host": "10.0.0.8",
        "port": 22345,
        "timeout": 0.35,
        "fldigi_host": "10.0.0.9",
        "fldigi_port": 7365,
    }


def test_fldigi_probe_cache_includes_flrig_endpoint(monkeypatch):
    import freqinout.radio_interface.rigctl_client as rigctl_client

    seen_ports: list[int] = []

    class FakeClient:
        def __init__(self, host="127.0.0.1", port=12345, timeout=0.8, **kwargs):
            seen_ports.append(port)

        def is_fldigi_available(self) -> bool:
            return True

    SoftwareStatusService._shared_service_probe_cache.clear()
    monkeypatch.setattr(rigctl_client, "FLRigClient", FakeClient)

    service = SoftwareStatusService(DummySettings({"fldigi_host": "10.0.0.9", "fldigi_port": 7365}))
    assert service.fldigi_api_reachable(flrig_port_override=12345) is True
    assert service.fldigi_api_reachable(flrig_port_override=24567) is True
    assert seen_ports == [12345, 24567]


def test_status_snapshot_warns_when_js8_process_endpoint_mismatch(monkeypatch):
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2442}))

    monkeypatch.setattr(service, "program_is_running", lambda name: name == "JS8Call")
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **kwargs: False)

    snapshot = service.status_snapshot()
    info = snapshot["JS8Call_API"]

    assert info["state"] == "warn"
    assert info["running"] is True
    assert "configured tcp api unreachable" in str(info["tooltip"]).lower()
    assert "instance/port mismatch" in str(info["tooltip"]).lower()


def test_status_snapshot_marks_flrig_ok_when_configured_endpoint_is_reachable(monkeypatch):
    service = SoftwareStatusService(DummySettings({"flrig_host": "10.0.0.8", "flrig_port": 22345}))

    monkeypatch.setattr(service, "program_is_running", lambda name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: True)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **kwargs: False)

    snapshot = service.status_snapshot()
    info = snapshot["FLRig"]

    assert info["state"] == "ok"
    assert info["running"] is True
    assert str(info["endpoint"]) == "10.0.0.8:22345"
    assert "reachable" in str(info["tooltip"]).lower()


def test_status_snapshot_warns_when_fldigi_process_endpoint_mismatch(monkeypatch):
    service = SoftwareStatusService(DummySettings({"fldigi_host": "10.0.0.9", "fldigi_port": 7365}))

    monkeypatch.setattr(service, "program_is_running", lambda name: name == "FLDigi")
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **kwargs: False)

    snapshot = service.status_snapshot()
    info = snapshot["FLDigi"]

    assert info["state"] == "warn"
    assert info["running"] is True
    assert "configured xml-rpc unreachable" in str(info["tooltip"]).lower()
    assert "instance/port mismatch" in str(info["tooltip"]).lower()


def test_status_snapshot_marks_fldigi_ok_when_configured_endpoint_is_reachable(monkeypatch):
    service = SoftwareStatusService(DummySettings({"fldigi_host": "10.0.0.9", "fldigi_port": 7365}))

    monkeypatch.setattr(service, "program_is_running", lambda name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **kwargs: True)

    snapshot = service.status_snapshot()
    info = snapshot["FLDigi"]

    assert info["state"] == "ok"
    assert info["running"] is True
    assert str(info["endpoint"]) == "10.0.0.9:7365"
    assert "reachable" in str(info["tooltip"]).lower()


def test_settings_tab_refresh_running_status_uses_unsaved_flrig_port(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    captured: dict[str, object] = {}

    def fake_snapshot(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(tab._status_service, "status_snapshot", fake_snapshot)

    try:
        tab.flrig_port_edit.setText("24567")
        tab._refresh_running_status()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert captured.get("flrig_port_override") == 24567


def test_settings_tab_refresh_running_status_uses_unsaved_fldigi_endpoint(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    captured: dict[str, object] = {}

    def fake_snapshot(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(tab._status_service, "status_snapshot", fake_snapshot)

    try:
        tab.fldigi_host_edit.setText("10.1.1.7")
        tab.fldigi_port_edit.setText("7364")
        tab._refresh_running_status()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert captured.get("fldigi_host_override") == "10.1.1.7"
    assert captured.get("fldigi_port_override") == 7364
