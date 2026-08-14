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


def test_js8_api_capability_status_does_not_cache_local_not_running_status():
    SoftwareStatusService._shared_js8_capability_cache.clear()
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2453}))

    status = service.js8_api_capability_status(process_running=False, force=False)

    assert isinstance(status, dict)
    assert status["connected"] is False
    assert status["mode"] == "offline"
    assert status["endpoint"] == "127.0.0.1:2453"
    assert status["last_error"] == "JS8Call is not running"
    assert ("127.0.0.1", 2453) not in SoftwareStatusService._shared_js8_capability_cache


def test_js8_api_capability_status_probes_immediately_after_local_process_starts(monkeypatch):
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
                supported={"RIG.GET_FREQ": True, "RIG.GET_PTT": True, "TX.GET_QUEUE_DEPTH": True},
                errors={},
            )

        def stop(self):
            pass

    SoftwareStatusService._shared_js8_capability_cache.clear()
    monkeypatch.setattr(status_module, "JS8ApiClient", FakeJS8Client)

    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2454}))

    first = service.js8_api_capability_status(process_running=False, force=False)
    second = service.js8_api_capability_status(process_running=True, force=False)

    assert first["mode"] == "offline"
    assert first["last_error"] == "JS8Call is not running"
    assert second["connected"] is True
    assert second["mode"] == "api_full"
    assert second["version"] == "3.0.2"


def test_js8_shadow_comparison_status_reports_native_state(monkeypatch):
    import freqinout.core.software_status_service as status_module

    class FakeCapabilitySnapshot:
        connected = True
        mode = "api_basic"
        version = "3.0.2"
        supported = {
            "RIG.GET_FREQ": True,
            "RIG.GET_PTT": True,
            "TX.GET_QUEUE_DEPTH": True,
            "RX.GET_CALL_ACTIVITY": True,
            "MODE.GET_SPEED": True,
        }
        errors = {}

        def supports(self, command: str) -> bool:
            return bool(self.supported.get(str(command or "").strip().upper(), False))

    class FakeJS8Client:
        request_count = 0

        def __init__(self, endpoint, **_kwargs):
            self.endpoint = endpoint
            self.last_error = ""

        def start(self):
            return True

        def probe_capabilities(self, **_kwargs):
            return FakeCapabilitySnapshot()

        def request(self, command, **_kwargs):
            type(self).request_count += 1
            if command == "RIG.GET_FREQ":
                return SimpleNamespace(params={"DIAL": 7078000, "OFFSET": 1950}, value="")
            if command == "RIG.GET_PTT":
                return SimpleNamespace(params={"PTT": True}, value="")
            if command == "TX.GET_QUEUE_DEPTH":
                return SimpleNamespace(params={"DEPTH": 0}, value="")
            raise AssertionError(f"Unexpected command: {command}")

        def stop(self):
            pass

    SoftwareStatusService._shared_js8_capability_cache.clear()
    SoftwareStatusService._shared_js8_shadow_cache.clear()
    monkeypatch.setattr(status_module, "JS8ApiClient", FakeJS8Client)
    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: True)
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2455}))

    result = service.js8_shadow_comparison_status(
        legacy_readings={"busy": True, "frequency_hz": 7078000, "offset_hz": 1950},
        force=True,
    )

    assert result["connected"] is True
    assert result["mode"] == "api_basic"
    assert result["version"] == "3.0.2"
    assert result["legacy"] == {"busy": True, "frequency_hz": 7078000, "offset_hz": 1950}
    assert result["native"]["busy"] is True
    assert result["native"]["frequency_hz"] == 7078000
    assert result["native"]["offset_hz"] == 1950
    assert result["native"]["ptt_active"] is True
    assert result["native"]["queue_depth"] == 0
    assert result["differences"] == {}
    assert result["comparisons"]["busy"]["match"] is True
    assert result["comparisons"]["frequency_hz"]["match"] is True
    assert result["comparisons"]["offset_hz"]["match"] is True


def test_js8_shadow_comparison_status_reports_busy_mismatch(monkeypatch):
    import freqinout.core.software_status_service as status_module

    class FakeCapabilitySnapshot:
        connected = True
        mode = "api_basic"
        version = "3.0.2"
        supported = {"RIG.GET_FREQ": True, "RIG.GET_PTT": True, "TX.GET_QUEUE_DEPTH": True}
        errors = {}

        def supports(self, command: str) -> bool:
            return bool(self.supported.get(str(command or "").strip().upper(), False))

    class FakeJS8Client:
        def __init__(self, endpoint, **_kwargs):
            self.endpoint = endpoint
            self.last_error = ""

        def start(self):
            return True

        def probe_capabilities(self, **_kwargs):
            return FakeCapabilitySnapshot()

        def request(self, command, **_kwargs):
            if command == "RIG.GET_FREQ":
                return SimpleNamespace(params={"DIAL": 7078000, "OFFSET": 1950}, value="")
            if command == "RIG.GET_PTT":
                return SimpleNamespace(params={"PTT": False}, value="")
            if command == "TX.GET_QUEUE_DEPTH":
                return SimpleNamespace(params={"DEPTH": 0}, value="")
            raise AssertionError(f"Unexpected command: {command}")

        def stop(self):
            pass

    SoftwareStatusService._shared_js8_capability_cache.clear()
    SoftwareStatusService._shared_js8_shadow_cache.clear()
    monkeypatch.setattr(status_module, "JS8ApiClient", FakeJS8Client)
    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: True)
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2456}))

    result = service.js8_shadow_comparison_status(
        legacy_readings={"busy": True, "frequency_hz": 7078000, "offset_hz": 1950},
        force=True,
    )

    assert result["native"]["busy"] is False
    assert result["comparisons"]["busy"]["match"] is False
    assert result["differences"]["busy"] == {"legacy": True, "native": False}


def test_js8_shadow_comparison_status_captures_native_request_error(monkeypatch):
    import freqinout.core.software_status_service as status_module

    class FakeCapabilitySnapshot:
        connected = True
        mode = "api_basic"
        version = "3.0.2"
        supported = {"RIG.GET_FREQ": True, "RIG.GET_PTT": True, "TX.GET_QUEUE_DEPTH": True}
        errors = {}

        def supports(self, command: str) -> bool:
            return bool(self.supported.get(str(command or "").strip().upper(), False))

    class FakeJS8Client:
        def __init__(self, endpoint, **_kwargs):
            self.endpoint = endpoint
            self.last_error = ""

        def start(self):
            return True

        def probe_capabilities(self, **_kwargs):
            return FakeCapabilitySnapshot()

        def request(self, command, **_kwargs):
            if command == "RIG.GET_FREQ":
                raise RuntimeError("freq read failed")
            if command == "RIG.GET_PTT":
                return SimpleNamespace(params={"PTT": True}, value="")
            if command == "TX.GET_QUEUE_DEPTH":
                return SimpleNamespace(params={"DEPTH": 0}, value="")
            raise AssertionError(f"Unexpected command: {command}")

        def stop(self):
            pass

    SoftwareStatusService._shared_js8_capability_cache.clear()
    SoftwareStatusService._shared_js8_shadow_cache.clear()
    monkeypatch.setattr(status_module, "JS8ApiClient", FakeJS8Client)
    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: True)
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2458}))

    result = service.js8_shadow_comparison_status(legacy_readings={"frequency_hz": 7078000}, force=True)

    assert result["connected"] is True
    assert result["errors"]["RIG.GET_FREQ"] == "freq read failed"
    assert result["native"]["frequency_hz"] is None
    assert result["native"]["busy"] is True
    assert result["comparisons"]["frequency_hz"]["match"] is None


def test_js8_shadow_comparison_status_reuses_native_shadow_cache(monkeypatch):
    import freqinout.core.software_status_service as status_module

    class FakeCapabilitySnapshot:
        connected = True
        mode = "api_basic"
        version = "3.0.2"
        supported = {"RIG.GET_FREQ": True, "RIG.GET_PTT": True, "TX.GET_QUEUE_DEPTH": True}
        errors = {}

        def supports(self, command: str) -> bool:
            return bool(self.supported.get(str(command or "").strip().upper(), False))

    class FakeJS8Client:
        init_count = 0
        probe_count = 0
        request_count = 0

        def __init__(self, endpoint, **_kwargs):
            type(self).init_count += 1
            self.endpoint = endpoint
            self.last_error = ""

        def start(self):
            return True

        def probe_capabilities(self, **_kwargs):
            type(self).probe_count += 1
            return FakeCapabilitySnapshot()

        def request(self, command, **_kwargs):
            type(self).request_count += 1
            if command == "RIG.GET_FREQ":
                return SimpleNamespace(params={"DIAL": 7078000, "OFFSET": 1950}, value="")
            if command == "RIG.GET_PTT":
                return SimpleNamespace(params={"PTT": True}, value="")
            if command == "TX.GET_QUEUE_DEPTH":
                return SimpleNamespace(params={"DEPTH": 0}, value="")
            raise AssertionError(f"Unexpected command: {command}")

        def stop(self):
            pass

    SoftwareStatusService._shared_js8_capability_cache.clear()
    SoftwareStatusService._shared_js8_shadow_cache.clear()
    monkeypatch.setattr(status_module, "JS8ApiClient", FakeJS8Client)
    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: True)
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2457}))

    first = service.js8_shadow_comparison_status(
        legacy_readings={"busy": True, "frequency_hz": 7078000, "offset_hz": 1950},
        force=False,
    )
    second = service.js8_shadow_comparison_status(
        legacy_readings={"busy": False, "frequency_hz": 7078000, "offset_hz": 1700},
        force=False,
    )

    assert FakeJS8Client.probe_count == 1
    assert FakeJS8Client.init_count == 1
    assert first["native"]["busy"] is True
    assert second["native"]["busy"] is True
    assert second["comparisons"]["busy"]["legacy"] is False
    assert second["comparisons"]["busy"]["match"] is False


def test_js8_shadow_comparison_status_clears_stale_native_cache_when_local_js8_stops(monkeypatch):
    import freqinout.core.software_status_service as status_module

    class FakeCapabilitySnapshot:
        connected = True
        mode = "api_basic"
        version = "3.0.2"
        supported = {"RIG.GET_FREQ": True, "RIG.GET_PTT": True, "TX.GET_QUEUE_DEPTH": True}
        errors = {}

        def supports(self, command: str) -> bool:
            return bool(self.supported.get(str(command or "").strip().upper(), False))

    class FakeJS8Client:
        probe_count = 0
        request_count = 0

        def __init__(self, endpoint, **_kwargs):
            self.endpoint = endpoint
            self.last_error = ""

        def start(self):
            return True

        def probe_capabilities(self, **_kwargs):
            type(self).probe_count += 1
            return FakeCapabilitySnapshot()

        def request(self, command, **_kwargs):
            type(self).request_count += 1
            if command == "RIG.GET_FREQ":
                return SimpleNamespace(params={"DIAL": 7078000, "OFFSET": 1950}, value="")
            if command == "RIG.GET_PTT":
                return SimpleNamespace(params={"PTT": True}, value="")
            if command == "TX.GET_QUEUE_DEPTH":
                return SimpleNamespace(params={"DEPTH": 0}, value="")
            raise AssertionError(f"Unexpected command: {command}")

        def stop(self):
            pass

    SoftwareStatusService._shared_js8_capability_cache.clear()
    SoftwareStatusService._shared_js8_shadow_cache.clear()
    monkeypatch.setattr(status_module, "JS8ApiClient", FakeJS8Client)
    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: True)
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2459}))

    first = service.js8_shadow_comparison_status(
        legacy_readings={"busy": True, "frequency_hz": 7078000, "offset_hz": 1950},
        force=True,
    )
    cache_key = ("127.0.0.1", 2459)

    assert first["connected"] is True
    assert cache_key in SoftwareStatusService._shared_js8_shadow_cache

    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: False)
    offline = service._js8_shadow_native_status(force=False)

    assert offline["connected"] is False
    assert offline["last_error"] == "JS8Call is not running"
    assert cache_key not in SoftwareStatusService._shared_js8_shadow_cache

    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: True)
    second = service.js8_shadow_comparison_status(
        legacy_readings={"busy": True, "frequency_hz": 7078000, "offset_hz": 1950},
        force=False,
    )

    assert FakeJS8Client.probe_count == 1
    assert FakeJS8Client.request_count == 6
    assert second["connected"] is True
    assert second["native"]["busy"] is True


def test_status_refresh_applies_js8_shadow_comparison_on_scheduler_thread(monkeypatch):
    import freqinout.core.scheduler_engine as scheduler_module

    shadow_calls: list[dict[str, object]] = []
    queued_callbacks: list[object] = []

    class _ImmediateFuture:
        def __init__(self, value):
            self._value = value

        def done(self) -> bool:
            return True

        def add_done_callback(self, callback):
            callback(self)

        def result(self):
            return self._value

    class _ImmediateExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_calls: list[tuple[bool, bool]] = []

        def submit(self, fn, *args, **kwargs):
            return _ImmediateFuture(fn(*args, **kwargs))

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    class DummySettings:
        def __init__(self, values=None):
            self._values = dict(values or {})

        def get(self, key, default=None):
            return self._values.get(key, default)

        def close(self):
            pass

    class _FakeVarACStatusClient:
        def get_status(self, include_db_transfer: bool = True):
            return {"busy": False, "waiting_for_frequency": False, "reason": None}

    class _FakeShadowService:
        def __init__(self, settings):
            self.settings = settings

        def js8_shadow_comparison_status(self, *, legacy_readings, **_kwargs):
            shadow_calls.append(dict(legacy_readings))
            return {"connected": True, "mode": "api_basic", "version": "3.0.2"}

    class _FakeJS8Client:
        def __init__(self, *args, **kwargs):
            pass

        def is_busy(self) -> bool:
            return True

        def get_frequency(self):
            return 7078000

        def get_offset(self):
            return 1950

        def stop(self):
            pass

    monkeypatch.setattr(scheduler_module, "ThreadPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(scheduler_module, "SettingsManager", lambda: DummySettings({"control_via": "JS8Call"}))
    monkeypatch.setattr(scheduler_module, "VarACStatusClient", _FakeVarACStatusClient)
    monkeypatch.setattr(scheduler_module, "SoftwareStatusService", _FakeShadowService)
    monkeypatch.setattr(scheduler_module, "JS8ControlClient", _FakeJS8Client)

    engine = scheduler_module.SchedulerEngine(js8=_FakeJS8Client())
    try:
        monkeypatch.setattr(engine, "_queue_scheduler_thread_call", lambda callback: queued_callbacks.append(callback), raising=False)
        engine._maybe_refresh_external_status_snapshot(force=True)

        assert shadow_calls == [{"busy": True, "frequency_hz": 7078000, "offset_hz": 1950}]
        assert engine._last_js8_shadow_comparison == {}
        assert len(queued_callbacks) == 1

        queued_callbacks[0]()

        assert engine._last_js8_shadow_comparison == {"connected": True, "mode": "api_basic", "version": "3.0.2"}
    finally:
        engine.stop()


def test_js8_shadow_health_warns_only_for_real_mismatches():
    import freqinout.core.scheduler_engine as scheduler_module

    registry = get_dependency_health_registry()
    registry.record_success(
        "scheduler:js8-shadow",
        owner="SchedulerEngine",
        metadata={"action": "reset"},
    )
    engine = scheduler_module.SchedulerEngine()
    try:
        engine._update_js8_shadow_health(
            {
                "endpoint": "127.0.0.1:2443",
                "mode": "api_basic",
                "version": "3.0.2",
                "differences": {
                    "frequency_hz": {"legacy": 7078000, "native": 7079000},
                    "offset_hz": {"legacy": 1950, "native": 2500},
                },
            }
        )
        warning = registry.snapshot("scheduler:js8-shadow")
        assert warning["consecutive_failures"] == 1
        assert "diagnostic disagrees" in warning["last_error"]
        assert warning["metadata"]["diagnostic_only"] is True
        assert warning["metadata"]["endpoint"] == "127.0.0.1:2443"

        engine._update_js8_shadow_health(
            {
                "endpoint": "127.0.0.1:2443",
                "mode": "api_basic",
                "version": "3.0.2",
                "differences": {},
            }
        )
        cleared = registry.snapshot("scheduler:js8-shadow")
        assert cleared["consecutive_failures"] == 0
        assert cleared["last_error"] == ""
        assert "not reporting a mismatch" in str(cleared["metadata"]["action"])
    finally:
        engine.stop()


def test_js8_shadow_health_stays_clear_when_api_basic_lacks_busy_fields():
    import freqinout.core.scheduler_engine as scheduler_module

    registry = get_dependency_health_registry()
    registry.record_success(
        "scheduler:js8-shadow",
        owner="SchedulerEngine",
        metadata={"action": "reset"},
    )
    engine = scheduler_module.SchedulerEngine()
    try:
        engine._update_js8_shadow_health(
            {
                "endpoint": "127.0.0.1:2443",
                "mode": "api_basic",
                "version": "",
                "legacy": {"busy": True, "frequency_hz": 7078000, "offset_hz": 1950},
                "native": {
                    "busy": None,
                    "frequency_hz": 7078000,
                    "offset_hz": 1950,
                    "ptt_active": None,
                    "queue_depth": None,
                },
                "comparisons": {
                    "busy": {"legacy": True, "native": None, "match": None},
                    "frequency_hz": {"legacy": 7078000, "native": 7078000, "match": True},
                    "offset_hz": {"legacy": 1950, "native": 1950, "match": True},
                },
                "differences": {},
            }
        )

        cleared = registry.snapshot("scheduler:js8-shadow")
        assert cleared["consecutive_failures"] == 0
        assert cleared["last_error"] == ""
        assert cleared["metadata"]["diagnostic_only"] is True
        assert "not reporting a mismatch" in str(cleared["metadata"]["action"])
    finally:
        engine.stop()


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
