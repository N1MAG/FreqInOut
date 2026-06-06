from __future__ import annotations

import os
import sys
import time
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core.dependency_health import get_dependency_health_registry
from freqinout.core.dependency_status_service import DependencyStatusService
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


def test_rigctld_api_reachable_uses_saved_endpoint(monkeypatch):
    import freqinout.radio_interface.rigctl_client as rigctl_client

    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, host="127.0.0.1", port=4532, timeout=0.8, **kwargs):
            seen["host"] = host
            seen["port"] = port
            seen["timeout"] = timeout

        def is_available(self) -> bool:
            return True

    SoftwareStatusService._shared_service_probe_cache.clear()
    monkeypatch.setattr(rigctl_client, "RigctldClient", FakeClient)

    service = SoftwareStatusService(DummySettings({"rig_host": "10.0.0.44", "rig_port": 5532}))
    assert service.rigctld_api_reachable(force=True)
    assert seen == {"host": "10.0.0.44", "port": 5532, "timeout": 0.35}


def test_status_snapshot_accepts_force_and_forwards_refresh(monkeypatch):
    service = SoftwareStatusService(
        DummySettings(
            {
                "control_via": "RIGCTLD",
                "js8_host": "127.0.0.1",
                "js8_port": 2442,
                "flrig_host": "127.0.0.1",
                "flrig_port": 12345,
                "rig_host": "127.0.0.1",
                "rig_port": 4532,
                "fldigi_host": "127.0.0.1",
                "fldigi_port": 7362,
            }
        )
    )

    seen: dict[str, object] = {
        "refresh": [],
        "js8": [],
        "flrig": [],
        "rigctld": [],
        "fldigi": [],
    }

    monkeypatch.setattr(
        service,
        "_refresh_process_snapshot",
        lambda *, force=False: seen["refresh"].append(bool(force)),
    )
    monkeypatch.setattr(service, "program_is_running", lambda name: False)
    monkeypatch.setattr(
        service,
        "js8_api_reachable",
        lambda **kwargs: seen["js8"].append(dict(kwargs)) or False,
    )
    monkeypatch.setattr(
        service,
        "flrig_api_reachable",
        lambda **kwargs: seen["flrig"].append(dict(kwargs)) or False,
    )
    monkeypatch.setattr(
        service,
        "rigctld_api_reachable",
        lambda **kwargs: seen["rigctld"].append(dict(kwargs)) or False,
    )
    monkeypatch.setattr(
        service,
        "fldigi_api_reachable",
        lambda **kwargs: seen["fldigi"].append(dict(kwargs)) or False,
    )

    snapshot = service.status_snapshot(force=True)

    assert seen["refresh"] == [True]
    assert seen["js8"] and seen["js8"][0]["force"] is True
    assert seen["flrig"] and seen["flrig"][0]["force"] is True
    assert seen["rigctld"] and seen["rigctld"][0]["force"] is True
    assert seen["fldigi"] and seen["fldigi"][0]["force"] is True
    assert snapshot["JS8Call_API"]["state"] == "idle"


def test_status_snapshot_warns_when_js8_process_endpoint_mismatch(monkeypatch):
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2442}))

    monkeypatch.setattr(service, "program_is_running", lambda name: name == "JS8Call")
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)

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

    snapshot = service.status_snapshot()
    info = snapshot["FLRig"]

    assert info["state"] == "ok"
    assert info["running"] is True
    assert str(info["endpoint"]) == "10.0.0.8:22345"
    assert "reachable" in str(info["tooltip"]).lower()


def test_status_snapshot_marks_rigctld_ok_when_active_endpoint_is_reachable(monkeypatch):
    service = SoftwareStatusService(
        DummySettings({"control_via": "RIGCTLD", "rig_host": "10.0.0.44", "rig_port": 5532})
    )

    monkeypatch.setattr(service, "program_is_running", lambda name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "rigctld_api_reachable", lambda **kwargs: True)

    snapshot = service.status_snapshot()
    info = snapshot["RigCtlD"]

    assert info["state"] == "ok"
    assert info["running"] is True
    assert str(info["endpoint"]) == "10.0.0.44:5532"
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


def test_js8_api_capability_status_records_health_success(monkeypatch):
    import freqinout.core.software_status_service as status_module

    class FakeJS8Client:
        last_error = ""

        def __init__(self, endpoint, **_kwargs):
            self.endpoint = endpoint

        def start(self):
            return True

        def probe_capabilities(self, **_kwargs):
            return SimpleNamespace(
                connected=True,
                mode="api_full",
                version="3.0.2",
                supported={"RIG.GET_FREQ": True, "RIG.GET_PTT": True},
                errors={},
            )

        def stop(self):
            pass

    SoftwareStatusService._shared_js8_capability_cache.clear()
    monkeypatch.setattr(status_module, "JS8ApiClient", FakeJS8Client)

    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2449}))
    status = service.js8_api_capability_status(process_running=True, force=True)

    assert status["mode"] == "api_full"
    assert status["version"] == "3.0.2"
    health = get_dependency_health_registry().snapshot("js8call:127.0.0.1:2449:capability")
    assert health["consecutive_failures"] == 0
    assert health["metadata"]["capability_mode"] == "api_full"
    assert "native FIO diagnostics" in str(health["metadata"]["action"])


def test_js8_api_capability_status_returns_dict_when_probe_blocked(monkeypatch):
    SoftwareStatusService._shared_js8_capability_cache.clear()
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2451}))
    monkeypatch.setattr(
        service._health,
        "may_run",
        lambda *_args, **_kwargs: (False, {}),
    )

    status = service.js8_api_capability_status(process_running=True, force=False)

    assert isinstance(status, dict)
    assert status["connected"] is False
    assert status["mode"] == "offline"
    assert status["endpoint"] == "127.0.0.1:2451"
    assert status["supported"] == {}
    assert "cooldown" in str(status["last_error"]).lower()


def test_js8_api_capability_status_does_not_use_positive_cache_when_local_process_stops():
    SoftwareStatusService._shared_js8_capability_cache.clear()
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2452}))
    SoftwareStatusService._shared_js8_capability_cache[("127.0.0.1", 2452)] = (
        time.monotonic(),
        {
            "connected": True,
            "mode": "api_full",
            "version": "3.0.2",
            "endpoint": "127.0.0.1:2452",
            "supported": {"RIG.GET_FREQ": True},
            "errors": {},
            "last_error": "",
        },
    )

    status = service.js8_api_capability_status(process_running=False, force=False)

    assert status["connected"] is False
    assert status["mode"] == "offline"
    assert status["last_error"] == "JS8Call is not running"


def test_dependency_status_snapshot_includes_js8_capability(monkeypatch):
    def fake_running(self, name):
        return name == "JS8Call"

    def fake_capability(self, **_kwargs):
        return {
            "connected": True,
            "mode": "api_basic",
            "version": "2.2.0",
            "endpoint": "127.0.0.1:2450",
            "supported": {"RIG.GET_FREQ": True},
            "errors": {},
            "last_error": "",
        }

    monkeypatch.setattr(SoftwareStatusService, "program_is_running", fake_running)
    monkeypatch.setattr(SoftwareStatusService, "js8_api_capability_status", fake_capability)

    service = DependencyStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2450}))
    try:
        snapshot = service._build_process_snapshot(1, "test")
    finally:
        service.stop()

    js8_status = snapshot.process["JS8Call_API"]
    assert js8_status.state == "ok"
    assert js8_status.value == "api_basic"
    assert js8_status.meta["version"] == "2.2.0"
    assert "compatibility fallbacks" in js8_status.tooltip


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
