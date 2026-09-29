from __future__ import annotations

from types import SimpleNamespace

from freqinout.core import launch_orchestrator as launch_module
from freqinout.core.launch_orchestrator import LaunchOrchestrator


class _Timer:
    def __init__(self, interval: int = 2000) -> None:
        self._interval = interval
        self.stopped = False

    def interval(self) -> int:
        return self._interval

    def setInterval(self, value: int) -> None:
        self._interval = int(value)

    def stop(self) -> None:
        self.stopped = True


class _Signal:
    def __init__(self) -> None:
        self.values: list[dict[str, object]] = []

    def emit(self, value: dict[str, object]) -> None:
        self.values.append(value)


def _owned_process_orchestrator(item: dict[str, object], process: object) -> LaunchOrchestrator:
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._current_item = item
    orchestrator._current_process = process
    orchestrator._current_cmd = [f"/apps/{str(item['name']).casefold()}"]
    orchestrator._current_name = str(item["name"])
    orchestrator._current_launch_description = "radio configured path"
    orchestrator._current_started_monotonic = 100.0
    return orchestrator


def test_launched_fldigi_endpoint_is_authoritative_over_preflight_process_inventory() -> None:
    item = {
        "name": "FLDigi",
        "readiness_policy": {
            "host": "127.0.0.1",
            "port": 7362,
            "require_service": True,
        },
    }
    orchestrator = _owned_process_orchestrator(
        item,
        SimpleNamespace(poll=lambda: None),
    )
    orchestrator._program_running = lambda _item: (_ for _ in ()).throw(
        AssertionError("post-launch endpoint readiness must not use preflight process evidence")
    )
    calls: list[bool] = []
    orchestrator._cached_status_for_item = lambda _item, force=False: (
        calls.append(bool(force)) or {"reachable": True}
    )

    assert orchestrator._program_ready_for_sequence(item) is True
    assert calls == [True]


def test_launched_js8_endpoint_unblocks_commstat_after_existing_settle_delay(
    monkeypatch,
) -> None:
    js8 = {
        "name": "JS8Call",
        "radio_ids": [1],
        "readiness_policy": {
            "host": "127.0.0.1",
            "port": 2442,
            "require_api": True,
        },
    }
    commstat = {
        "name": "CommStat",
        "radio_ids": [1],
        "dependencies": ["JS8Call"],
    }
    monkeypatch.setattr(launch_module.time, "monotonic", lambda: 112.0)
    orchestrator = _owned_process_orchestrator(
        js8,
        SimpleNamespace(poll=lambda: None),
    )
    orchestrator._active = True
    orchestrator._cancel_requested = False
    orchestrator._current_phase = "readiness"
    orchestrator._queue = [js8, commstat]
    orchestrator._index = 1
    orchestrator._results = []
    orchestrator._wait_timeout_sec = 90
    orchestrator._poll_timer = _Timer()
    orchestrator.sequence_progress = _Signal()
    orchestrator.settings = SimpleNamespace(get=lambda _key, default=None: default)
    orchestrator._cached_status_for_item = lambda _item, force=False: {
        "reachable": True,
        "version": "",
    }
    orchestrator._persist_ready_js8_identity = lambda *_args, **_kwargs: None
    scheduled: list[int] = []
    orchestrator._schedule_advance_queue = scheduled.append

    orchestrator._poll_current_readiness()

    assert orchestrator._results[0]["status"] == "launched"
    assert "waiting 4.0s" in str(orchestrator._results[0]["detail"])
    assert scheduled == [4000]
    assert orchestrator._blocked_dependency_for(commstat) == ""


def test_process_only_readiness_uses_owned_child_stability_not_process_inventory(
    monkeypatch,
) -> None:
    item = {"name": "FLAmp", "readiness_policy": {"kind": "process"}}
    orchestrator = _owned_process_orchestrator(
        item,
        SimpleNamespace(poll=lambda: None),
    )
    orchestrator._program_running = lambda _item: (_ for _ in ()).throw(
        AssertionError("post-launch child readiness must not use preflight process evidence")
    )

    monkeypatch.setattr(launch_module.time, "monotonic", lambda: 102.9)
    assert orchestrator._program_ready_for_sequence(item) is False

    monkeypatch.setattr(launch_module.time, "monotonic", lambda: 103.1)
    assert orchestrator._program_ready_for_sequence(item) is True


def test_nonzero_custom_tool_exit_fails_without_waiting_for_timeout(monkeypatch) -> None:
    item = {"name": "rigctl-dx10", "radio_ids": [1]}
    monkeypatch.setattr(launch_module.time, "monotonic", lambda: 101.0)
    orchestrator = _owned_process_orchestrator(
        item,
        SimpleNamespace(poll=lambda: 1),
    )
    orchestrator._current_launch_description = "configured custom tool"
    orchestrator._active = True
    orchestrator._cancel_requested = False
    orchestrator._current_phase = "readiness"
    orchestrator._results = []
    orchestrator._wait_timeout_sec = 90
    orchestrator._poll_timer = _Timer()
    orchestrator.sequence_progress = _Signal()
    orchestrator._program_ready_for_sequence = lambda _item: (_ for _ in ()).throw(
        AssertionError("an exited child must be classified before readiness")
    )
    scheduled: list[int] = []
    orchestrator._schedule_advance_queue = scheduled.append

    orchestrator._poll_current_readiness()

    assert orchestrator._results == [
        {
            "name": "rigctl-dx10",
            "status": "failed",
            "detail": "launch process exited with code 1 before readiness",
            "radio_ids": [1],
        }
    ]
    assert scheduled == [0]


def test_successful_early_exit_is_limited_to_launchers_and_custom_tools(
    monkeypatch,
) -> None:
    item = {"name": "VarAC", "readiness_policy": {"kind": "process"}}
    monkeypatch.setattr(launch_module.time, "monotonic", lambda: 104.0)
    direct = _owned_process_orchestrator(item, SimpleNamespace(poll=lambda: 0))

    assert direct._current_launch_exit_failure(item) == (
        "launch process exited with code 0 before readiness"
    )
    assert direct._current_launch_process_is_stable(item) is False

    launcher = _owned_process_orchestrator(item, SimpleNamespace(poll=lambda: 0))
    launcher._current_cmd = ["xdg-open", "/apps/VarAC.desktop"]

    assert launcher._current_launch_exit_failure(item) == ""
    assert launcher._current_launch_process_is_stable(item) is True


def test_only_legacy_thirty_second_timeout_is_normalized() -> None:
    assert LaunchOrchestrator._normalized_readiness_timeout(30) == 90
    assert LaunchOrchestrator._normalized_readiness_timeout("30") == 90
    assert LaunchOrchestrator._normalized_readiness_timeout(45) == 45
    assert LaunchOrchestrator._normalized_readiness_timeout(120) == 120
    assert LaunchOrchestrator._normalized_readiness_timeout(0) == 90
