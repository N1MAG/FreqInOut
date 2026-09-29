from __future__ import annotations

import os
import time
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.dependency_status_service import shutdown_dependency_status_service
from freqinout.core import launch_orchestrator as launch_orchestrator_module
from freqinout.core.launch_orchestrator import LaunchOrchestrator
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_launch_planner import StationLaunchPlanner


def _profile(radio_id: int, *, display_order: int) -> dict[str, object]:
    return {
        "id": radio_id,
        "name": f"Radio {radio_id}",
        "runtime_active": 1,
        "display_order": display_order,
        "device_class": "tx_rx",
        "control_backend": "flrig",
        "flrig_host": "127.0.0.1",
        "flrig_port": 12344 + radio_id,
        "fldigi_host": "127.0.0.1",
        "fldigi_port": 7352 + radio_id,
        "js8_host": "127.0.0.1",
        "js8_port": 2432 + radio_id,
        "js8_instance_system_key": f"radio-{radio_id}-js8",
        "js8_instance_name": f"Radio {radio_id} JS8",
    }


def _item(name: str, radio_id: int, *, dependencies: list[str] | None = None) -> dict[str, object]:
    return {
        "name": name,
        "instance_key": f"radio-{radio_id}:{name.casefold()}",
        "enabled": True,
        "startup": True,
        "monitor_health": True,
        "launch_path_override": f"/apps/radio-{radio_id}/{name.casefold()}",
        "launch_command_override": "",
        "dependencies": list(dependencies or ()),
        "readiness_policy": {},
    }


def test_planner_builds_complete_radio_stages_with_control_first() -> None:
    profiles = [_profile(20, display_order=2), _profile(10, display_order=1)]
    bundles = {
        radio_id: {
            "launch_enabled": True,
            "items": [
                _item("VarAC", radio_id),
                _item("FLDigi", radio_id, dependencies=["FLRig"]),
                _item("FLRig", radio_id),
                _item("JS8Call", radio_id),
            ],
        }
        for radio_id in (10, 20)
    }

    queue = StationLaunchPlanner().plan_startup(profiles, bundles).queue()

    assert [(row["radio_ids"][0], row["name"]) for row in queue] == [
        (10, "FLRig"),
        (10, "VarAC"),
        (10, "FLDigi"),
        (10, "JS8Call"),
        (20, "FLRig"),
        (20, "VarAC"),
        (20, "FLDigi"),
        (20, "JS8Call"),
    ]
    assert all(row["radio_control_gate_required"] is True for row in queue)
    assert all(row["radio_control_app"] == "FLRig" for row in queue)
    assert [row["radio_control_port"] for row in queue[:4]] == [12354] * 4


def test_control_application_readiness_requires_physical_radio_readback() -> None:
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._program_running = lambda _item: True
    orchestrator._radio_control_probe_is_due = lambda _item: True
    item = {
        "name": "FLRig",
        "radio_ids": [1],
        "radio_control_gate_required": True,
        "radio_control_app": "FLRig",
    }
    orchestrator._radio_control_evidence_state = lambda _item, force=False: "unavailable"

    assert orchestrator._program_ready_for_sequence(item) is False

    orchestrator._radio_control_evidence_state = lambda _item, force=False: "ready"
    assert orchestrator._program_ready_for_sequence(item) is True


def test_radio_gate_requests_fresh_exact_endpoint_readback() -> None:
    calls: list[dict[str, object]] = []
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._sequence_preflight_started_wall = 100.0
    orchestrator.dependency_status = SimpleNamespace(
        status_snapshot=lambda **kwargs: calls.append(dict(kwargs))
        or {
            "FLRig": {
                "source": "endpoint",
                "checked_at": 101.0,
                "reachable": True,
                "radio_readback_ready": True,
            }
        }
    )
    item = {
        "radio_ids": [7],
        "radio_control_backend": "flrig",
        "radio_control_host": "192.0.2.7",
        "radio_control_port": 22345,
    }

    assert orchestrator._radio_control_evidence_state(item, force=True) == "ready"
    assert calls == [
        {
            "force": True,
            "force_process_snapshot": False,
            "verify_control_readback": True,
            "control_backend": "flrig",
            "control_probe_only": True,
            "flrig_host_override": "192.0.2.7",
            "flrig_port_override": 22345,
        }
    ]


def test_post_launch_readiness_reprobes_after_prelaunch_negative(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    responses = iter((False, True))

    def _status_snapshot(**kwargs):
        calls.append(dict(kwargs))
        ready = next(responses)
        return {
            "FLRig": {
                "source": "endpoint",
                "checked_at": 101.0 if not ready else 102.0,
                "reachable": ready,
                "radio_readback_ready": ready,
            }
        }

    times = iter((10.0, 11.1))
    monkeypatch.setattr(launch_orchestrator_module.time, "monotonic", lambda: next(times))
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._program_running = lambda _item: True
    orchestrator._sequence_preflight_started_wall = 100.0
    orchestrator._radio_control_probe_due_monotonic = {}
    orchestrator.dependency_status = SimpleNamespace(status_snapshot=_status_snapshot)
    item = {
        "name": "FLRig",
        "radio_ids": [7],
        "radio_control_gate_required": True,
        "radio_control_app": "FLRig",
        "radio_control_backend": "flrig",
        "radio_control_host": "127.0.0.1",
        "radio_control_port": 12345,
    }

    assert orchestrator._program_ready_for_sequence(item) is False
    assert orchestrator._program_ready_for_sequence(item) is True
    assert [call["force"] for call in calls] == [True, True]
    assert all(call["control_probe_only"] is True for call in calls)


def test_launch_control_reprobe_is_rate_limited_per_radio(monkeypatch) -> None:
    times = iter((20.0, 20.5, 21.1))
    monkeypatch.setattr(launch_orchestrator_module.time, "monotonic", lambda: next(times))
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._radio_control_probe_due_monotonic = {}
    item = {"radio_ids": [7]}

    assert orchestrator._radio_control_probe_is_due(item) is True
    assert orchestrator._radio_control_probe_is_due(item) is False
    assert orchestrator._radio_control_probe_is_due(item) is True


def test_prelaunch_endpoint_check_does_not_share_control_readback_scope() -> None:
    calls: list[dict[str, object]] = []
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator.dependency_status = SimpleNamespace(
        status_snapshot=lambda **kwargs: calls.append(dict(kwargs)) or {"FLRig": {}}
    )
    item = {
        "name": "FLRig",
        "radio_ids": [7],
        "radio_control_gate_required": True,
        "radio_control_app": "FLRig",
        "radio_control_backend": "flrig",
        "radio_control_host": "127.0.0.1",
        "radio_control_port": 12345,
        "readiness_policy": {"host": "127.0.0.1", "port": 12345},
    }

    orchestrator._cached_status_for_item(item, force=True)
    orchestrator._cached_radio_control_status(item, force=True)

    assert calls[0].get("verify_control_readback") is None
    assert calls[0].get("control_probe_only") is None
    assert calls[1]["verify_control_readback"] is True
    assert calls[1]["control_probe_only"] is True


def test_blocked_radio_skips_its_apps_and_healthy_peer_continues(monkeypatch, tmp_path) -> None:
    radio_one = {
        "name": "VarAC",
        "instance_identity": "radio-one:varac",
        "radio_ids": [1],
        "radio_names": ["Radio One"],
        "radio_control_gate_required": True,
        "radio_control_backend": "flrig",
        "radio_control_app": "FLRig",
    }
    radio_two = {
        "name": "VarAC",
        "instance_identity": "radio-two:varac",
        "radio_ids": [2],
        "radio_names": ["Radio Two"],
        "radio_control_gate_required": True,
        "radio_control_backend": "flrig",
        "radio_control_app": "FLRig",
    }
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])
    orchestrator = LaunchOrchestrator(SettingsManager())
    orchestrator._test_app = app
    orchestrator._active = True
    orchestrator._process_preflight_pending = False
    orchestrator._cancel_requested = False
    orchestrator._queue = [radio_one, radio_two]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator._radio_control_gate_states = {1: "blocked", 2: "ready"}
    orchestrator._sequence_claimed_identities = set()
    orchestrator._blocked_dependency_for = lambda _item: ""
    orchestrator._instance_launch_identity_blocker = lambda _item: ""
    orchestrator._legacy_default_js8_profile_conflict = lambda _item: ""
    orchestrator._configured_instance_process_running = lambda _item: True
    orchestrator._endpoint_preflight_key = lambda _item: ""
    orchestrator._program_ready_for_sequence = lambda _item: True
    orchestrator._schedule_advance_queue = lambda _delay=0: None
    progress: list[dict[str, object]] = []
    orchestrator.sequence_progress.connect(progress.append)

    orchestrator._advance_queue()
    orchestrator._advance_queue()

    assert [(row["radio_ids"], row["status"]) for row in orchestrator._results] == [
        ([1], "blocked_radio_control"),
        ([2], "already_running"),
    ]
    assert [row["status"] for row in progress] == [
        "blocked_radio_control",
        "already_running",
    ]
    shutdown_dependency_status_service()


def test_manual_control_profile_bypasses_radio_gate() -> None:
    profile = {
        **_profile(3, display_order=1),
        "control_backend": "manual",
    }
    queue = StationLaunchPlanner().plan_startup(
        [profile],
        {3: {"launch_enabled": True, "items": [_item("VarAC", 3)]}},
    ).queue()

    assert len(queue) == 1
    assert queue[0]["radio_control_gate_required"] is False
    assert queue[0]["radio_control_app"] == ""


def test_flrig_control_readback_metadata_requires_positive_frequency(monkeypatch) -> None:
    settings = SimpleNamespace(get=lambda _key, default=None: default)
    service = SoftwareStatusService(settings)
    monkeypatch.setattr(service, "program_is_running", lambda _name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **_kwargs: True)
    monkeypatch.setattr(service, "rigctld_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **_kwargs: False)

    from freqinout.radio_interface.rigctl_client import FLRigClient

    monkeypatch.setattr(FLRigClient, "get_vfo_frequency", lambda _self: 14_078_000)
    ready = service.status_snapshot(
        force=False,
        force_process_snapshot=False,
        flrig_host_override="127.0.0.1",
        flrig_port_override=12345,
        verify_control_readback=True,
        control_backend="flrig",
    )["FLRig"]

    assert ready["reachable"] is True
    assert ready["radio_readback_ready"] is True
    assert ready["frequency_hz"] == 14_078_000

    monkeypatch.setattr(FLRigClient, "get_vfo_frequency", lambda _self: None)
    unavailable = service.status_snapshot(
        force=False,
        force_process_snapshot=False,
        flrig_host_override="127.0.0.1",
        flrig_port_override=12345,
        verify_control_readback=True,
        control_backend="flrig",
    )["FLRig"]

    assert unavailable["reachable"] is True
    assert unavailable["radio_readback_ready"] is False
    assert unavailable["frequency_hz"] is None


def test_launch_control_probe_forces_only_the_exact_backend(monkeypatch) -> None:
    settings = SimpleNamespace(get=lambda _key, default=None: default)
    service = SoftwareStatusService(settings)
    calls: list[str] = []
    monkeypatch.setattr(service, "program_is_running", lambda _name: False)
    monkeypatch.setattr(
        service,
        "js8_api_reachable",
        lambda **_kwargs: calls.append("js8") or False,
    )
    monkeypatch.setattr(
        service,
        "flrig_api_reachable",
        lambda **_kwargs: calls.append("flrig") or True,
    )
    monkeypatch.setattr(
        service,
        "rigctld_api_reachable",
        lambda **_kwargs: calls.append("rigctld") or False,
    )
    monkeypatch.setattr(
        service,
        "fldigi_api_reachable",
        lambda **_kwargs: calls.append("fldigi") or False,
    )

    from freqinout.radio_interface.rigctl_client import FLRigClient

    monkeypatch.setattr(FLRigClient, "get_vfo_frequency", lambda _self: 14_115_000)
    row = service.status_snapshot(
        force=True,
        force_process_snapshot=False,
        flrig_host_override="127.0.0.1",
        flrig_port_override=12345,
        verify_control_readback=True,
        control_backend="flrig",
        control_probe_only=True,
    )["FLRig"]

    assert calls == ["flrig"]
    assert row["radio_readback_ready"] is True
    assert row["frequency_hz"] == 14_115_000


def test_successful_post_launch_probe_replaces_cached_endpoint_failure(monkeypatch) -> None:
    settings = SimpleNamespace(get=lambda _key, default=None: default)
    service = SoftwareStatusService(settings)
    monkeypatch.setattr(service, "program_is_running", lambda _name: False)
    cache_key = ("FLRIG", "127.0.0.1", "12999")
    monkeypatch.setitem(
        SoftwareStatusService._shared_service_probe_cache,
        cache_key,
        (time.monotonic(), False),
    )

    from freqinout.radio_interface.rigctl_client import FLRigClient

    monkeypatch.setattr(FLRigClient, "is_available", lambda _self: True)
    monkeypatch.setattr(FLRigClient, "get_vfo_frequency", lambda _self: 7_115_000)
    row = service.status_snapshot(
        force=True,
        force_process_snapshot=False,
        flrig_host_override="127.0.0.1",
        flrig_port_override=12999,
        verify_control_readback=True,
        control_backend="flrig",
        control_probe_only=True,
    )["FLRig"]

    assert row["radio_readback_ready"] is True
    assert SoftwareStatusService._shared_service_probe_cache[cache_key][1] is True


def test_rigctld_control_readback_uses_exact_configured_endpoint(monkeypatch) -> None:
    settings = SimpleNamespace(get=lambda _key, default=None: default)
    service = SoftwareStatusService(settings)
    monkeypatch.setattr(service, "program_is_running", lambda _name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "rigctld_api_reachable", lambda **_kwargs: True)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **_kwargs: False)

    from freqinout.radio_interface.rigctl_client import RigctldClient

    observed: list[tuple[str, int]] = []

    def _frequency(client) -> int:
        observed.append((client.host, client.port))
        return 7_078_000

    monkeypatch.setattr(RigctldClient, "get_vfo_frequency", _frequency)
    row = service.status_snapshot(
        force=False,
        force_process_snapshot=False,
        rigctld_host_override="192.0.2.20",
        rigctld_port_override=4533,
        verify_control_readback=True,
        control_backend="rigctld",
    )["RigCtlD"]

    assert observed == [("192.0.2.20", 4533)]
    assert row["radio_readback_ready"] is True
    assert row["frequency_hz"] == 7_078_000


def test_js8_control_readback_uses_exact_configured_endpoint(monkeypatch) -> None:
    settings = SimpleNamespace(get=lambda _key, default=None: default)
    service = SoftwareStatusService(settings)
    monkeypatch.setattr(service, "program_is_running", lambda _name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **_kwargs: True)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "rigctld_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **_kwargs: False)

    from freqinout.radio_interface.js8_status import JS8ControlClient

    observed: list[tuple[str, int]] = []

    def _frequency(client) -> int:
        observed.append((client.host, client._get_port()))
        return 14_078_000

    monkeypatch.setattr(JS8ControlClient, "get_frequency", _frequency)
    row = service.status_snapshot(
        force=False,
        force_process_snapshot=False,
        host_override="192.0.2.30",
        port_override=2443,
        verify_control_readback=True,
        control_backend="js8call",
    )["JS8Call_API"]

    assert observed == [("192.0.2.30", 2443)]
    assert row["radio_readback_ready"] is True
    assert row["frequency_hz"] == 14_078_000
