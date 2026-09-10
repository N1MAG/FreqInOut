"""MES-2 bounded fault and lifecycle tests for endpoint-lane ownership."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
from typing import List, Set

from freqinout.core.scheduler_coordination import EndpointKey, EndpointResult
from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry


class CooperativeGate:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release_event = threading.Event()

    def __call__(self) -> bool:
        self.started.set()
        self.release_event.wait()
        return True

    def release(self) -> None:
        self.release_event.set()


def _key(port: int) -> EndpointKey:
    return EndpointKey.network("rigctld", "localhost", port)


def test_twenty_five_registry_cycles_keep_one_owned_worker_per_active_key() -> None:
    """Repeated schedule churn must not create an unbounded worker population."""

    registry = EndpointLaneRegistry()
    gates: List[CooperativeGate] = []
    worker_names: Set[str] = set()
    try:
        for cycle in range(25):
            endpoint = _key(13_000 + (cycle % 3))
            gate = CooperativeGate()
            gates.append(gate)
            results: List[EndpointResult] = []
            submission = registry.submit(
                endpoint,
                occurrence_id="cycle-%d" % cycle,
                operation=gate,
                completion=results.append,
            )
            assert submission.accepted
            assert gate.started.wait(timeout=2.0)
            worker_names.update(
                thread.name
                for thread in threading.enumerate()
                if thread.name.startswith("freqinout-endpoint-")
            )
            gate.release()
            lane = registry.lane(endpoint)
            assert lane is not None and lane.future is not None
            lane.future.result(timeout=2.0)
            assert registry.remove(endpoint)
        assert len(worker_names) <= 3
    finally:
        for gate in gates:
            gate.release()
        registry.shutdown(wait=True)
    assert not any(
        thread.name.startswith("freqinout-endpoint-")
        for thread in threading.enumerate()
    )


def test_shutdown_with_cooperative_release_does_not_leave_owned_lane_threads() -> None:
    registry = EndpointLaneRegistry()
    gates = [CooperativeGate(), CooperativeGate(), CooperativeGate()]
    try:
        for offset, gate in enumerate(gates):
            submission = registry.submit(
                _key(13_100 + offset),
                occurrence_id="shutdown-%d" % offset,
                operation=gate,
                completion=lambda _result: None,
            )
            assert submission.accepted
            assert gate.started.wait(timeout=2.0)
    finally:
        for gate in gates:
            gate.release()
        registry.shutdown(wait=True)
    assert not any(
        thread.name.startswith("freqinout-endpoint-")
        for thread in threading.enumerate()
    )
