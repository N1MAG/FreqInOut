"""MES-2 tests for serialized, failure-isolated endpoint lanes.

These tests deliberately use only controllable events and an injected clock.
They do not exercise Qt, protocol adapters, sockets, or real hardware.  A
barrier is released explicitly in each test so a blocked endpoint can be
proven independent of its peers without sleeping for a duration.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, List

from freqinout.core.scheduler_coordination import EndpointKey, EndpointResult
from freqinout.core.scheduler_endpoint_lane import EndpointLane, EndpointLaneRegistry, EndpointWork


class FakeClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = float(value)

    def monotonic(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


class Gate:
    """An operation gate which never depends on wall-clock sleeping."""

    def __init__(self, *, released: bool = False) -> None:
        self.started = threading.Event()
        self.release_event = threading.Event()
        if released:
            self.release_event.set()

    def __call__(self) -> bool:
        self.started.set()
        self.release_event.wait()
        return True

    def release(self) -> None:
        self.release_event.set()


def key(port: int) -> EndpointKey:
    return EndpointKey.network("flrig", "127.0.0.1", port)


def wait_for(event: threading.Event) -> None:
    assert event.wait(timeout=2.0), "worker did not reach the expected barrier"


def completion_list(results: List[EndpointResult]):
    completed = threading.Event()

    def record(result: EndpointResult) -> None:
        results.append(result)
        completed.set()

    return record, completed


def submit(
    registry: EndpointLaneRegistry,
    endpoint_key: EndpointKey,
    operation: Callable[[], object],
    results: List[EndpointResult],
    *,
    timeout_s: float = 5.0,
) -> Any:
    return registry.submit(
        endpoint_key,
        occurrence_id="occurrence",
        operation=operation,
        completion=results.append,
        timeout_s=timeout_s,
    )


def test_three_separate_endpoint_keys_start_concurrently() -> None:
    gates = [Gate() for _ in range(3)]
    started = [gate.started for gate in gates]
    results: List[EndpointResult] = []
    registry = EndpointLaneRegistry()
    try:
        for index, gate in enumerate(gates):
            assert submit(registry, key(12_345 + index), gate, results).disposition == "started"
        for event in started:
            wait_for(event)
        assert all(event.is_set() for event in started)
    finally:
        for gate in gates:
            gate.release()
        registry.shutdown(wait=True)


def test_blocked_endpoint_does_not_delay_peer_endpoints() -> None:
    blocked = Gate()
    peer_a = Gate()
    peer_b = Gate()
    results: List[EndpointResult] = []
    registry = EndpointLaneRegistry()
    try:
        assert submit(registry, key(12_350), blocked, results).accepted
        wait_for(blocked.started)
        assert submit(registry, key(12_351), peer_a, results).accepted
        assert submit(registry, key(12_352), peer_b, results).accepted
        wait_for(peer_a.started)
        wait_for(peer_b.started)
        assert not blocked.release_event.is_set()
    finally:
        blocked.release()
        peer_a.release()
        peer_b.release()
        registry.shutdown(wait=True)


def test_same_endpoint_is_serialized_and_only_newest_pending_is_kept() -> None:
    first = Gate()
    second = Gate()
    third = Gate()
    results: List[EndpointResult] = []
    registry = EndpointLaneRegistry()
    endpoint = key(12_353)
    try:
        first_submission = submit(registry, endpoint, first, results)
        assert first_submission.disposition == "started"
        wait_for(first.started)
        second_submission = submit(registry, endpoint, second, results)
        third_submission = submit(registry, endpoint, third, results)
        assert second_submission.disposition == "coalesced"
        assert third_submission.disposition == "coalesced"
        assert registry.lane(endpoint).pending_occurrence_id == "occurrence"
        # The lane cannot start either pending operation while the first runs.
        assert not second.started.is_set()
        assert not third.started.is_set()
        first.release()
        wait_for(third.started)
        assert not second.started.is_set()
    finally:
        first.release()
        second.release()
        third.release()
        registry.shutdown(wait=True)


def test_completed_generation_with_newer_pending_is_classified_superseded() -> None:
    first = Gate()
    second = Gate(released=True)
    results: List[EndpointResult] = []
    registry = EndpointLaneRegistry()
    endpoint = key(12_354)
    try:
        submit(registry, endpoint, first, results)
        wait_for(first.started)
        assert submit(registry, endpoint, second, results).accepted
        first.release()
        wait_for(second.started)
        # The completion for generation one is published before generation two.
        assert any(item.status == "superseded" and item.generation == 1 for item in results)
    finally:
        first.release()
        second.release()
        registry.shutdown(wait=True)


def test_false_result_and_exception_use_backoff_without_affecting_peer() -> None:
    clock = FakeClock()
    false_results: List[EndpointResult] = []
    exception_results: List[EndpointResult] = []
    peer_results: List[EndpointResult] = []
    false_completion, false_done = completion_list(false_results)
    exception_completion, exception_done = completion_list(exception_results)
    peer_completion, peer_done = completion_list(peer_results)
    false_registry = EndpointLaneRegistry(monotonic_fn=clock.monotonic, base_backoff_s=10.0)
    exception_registry = EndpointLaneRegistry(monotonic_fn=clock.monotonic, base_backoff_s=10.0)
    peer_registry = EndpointLaneRegistry(monotonic_fn=clock.monotonic, base_backoff_s=10.0)
    try:
        false_submission = false_registry.submit(
            key(12_355), occurrence_id="false", operation=lambda: False,
            completion=false_completion,
        )
        exception_submission = exception_registry.submit(
            key(12_356), occurrence_id="exception",
            operation=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            completion=exception_completion,
        )
        peer_submission = peer_registry.submit(
            key(12_357), occurrence_id="peer", operation=lambda: True,
            completion=peer_completion,
        )
        assert false_submission.accepted and exception_submission.accepted and peer_submission.accepted
        wait_for(false_done)
        wait_for(exception_done)
        wait_for(peer_done)
        assert false_results[0].status == "failed"
        assert exception_results[0].reason_code == "operation_exception"
        assert peer_results[0].status == "applied_unverified"
        false_retry = false_registry.submit(
            key(12_355), occurrence_id="false-retry", operation=lambda: True,
            completion=false_completion,
        )
        exception_retry = exception_registry.submit(
            key(12_356), occurrence_id="exception-retry", operation=lambda: True,
            completion=exception_completion,
        )
        assert false_retry.disposition == "backoff"
        assert exception_retry.disposition == "backoff"
        assert false_registry.lane(key(12_355)).snapshot().failure_count == 1
        assert exception_registry.lane(key(12_356)).snapshot().failure_count == 1
        assert peer_registry.lane(key(12_357)).snapshot().failure_count == 0
    finally:
        false_registry.shutdown(wait=True)
        exception_registry.shutdown(wait=True)
        peer_registry.shutdown(wait=True)


def test_timeout_uses_injected_clock_and_does_not_replace_executor() -> None:
    clock = FakeClock()
    gate = Gate()
    results: List[EndpointResult] = []
    lane = EndpointLane(key(12_358), monotonic_fn=clock.monotonic, base_backoff_s=10.0)
    try:
        lane.submit(
            # The operation remains cooperative and is released during cleanup.
            work=EndpointWork(
                endpoint_key=key(12_358),
                generation=1,
                occurrence_id="timeout",
                operation=gate,
                completion=results.append,
                timeout_s=5.0,
            )
        )
        wait_for(gate.started)
        executor = lane._executor
        clock.advance(5.0)
        timeout_result = lane.poll()
        assert timeout_result is not None
        assert timeout_result.status == "timed_out"
        assert results == [timeout_result]
        assert lane._executor is executor
        assert lane.poll() is None
    finally:
        gate.release()
        lane.shutdown(wait=True)


def test_circuit_threshold_and_manual_retry_are_lane_local() -> None:
    clock = FakeClock()
    failures: List[EndpointResult] = []
    peer_results: List[EndpointResult] = []
    registry = EndpointLaneRegistry(
        monotonic_fn=clock.monotonic,
        circuit_failure_threshold=2,
        base_backoff_s=0.0,
    )
    endpoint = key(12_359)
    peer = key(12_360)
    try:
        for _ in range(2):
            done = threading.Event()

            def record_failure(result: EndpointResult) -> None:
                failures.append(result)
                done.set()

            registry.submit(
                endpoint, occurrence_id="failure", operation=lambda: False,
                completion=record_failure,
            )
            lane = registry.lane(endpoint)
            assert lane is not None
            wait_for(done)
            assert lane.snapshot().failure_count >= 1
            clock.advance(1.0)
            lane.poll()
        assert registry.lane(endpoint).snapshot().circuit_open
        retry_done = threading.Event()

        def record_retry(result: EndpointResult) -> None:
            failures.append(result)
            retry_done.set()

        blocked = registry.submit(
            endpoint, occurrence_id="retry", operation=lambda: True,
            completion=record_retry,
        )
        assert blocked.disposition == "circuit_open"
        peer_done = threading.Event()

        def record_peer(result: EndpointResult) -> None:
            peer_results.append(result)
            peer_done.set()

        assert registry.submit(
            peer, occurrence_id="peer", operation=lambda: True,
            completion=record_peer,
        ).accepted
        wait_for(peer_done)
        assert registry.retry_now(endpoint)
        # Retry is immediate and can clear its Future before observation.
        wait_for(retry_done)
        assert any(item.status == "applied_unverified" for item in failures)
    finally:
        registry.shutdown(wait=True)


def test_open_circuit_allows_one_automatic_half_open_probe_after_backoff() -> None:
    clock = FakeClock()
    results: List[EndpointResult] = []
    completed = threading.Event()
    registry = EndpointLaneRegistry(
        monotonic_fn=clock.monotonic,
        circuit_failure_threshold=1,
        base_backoff_s=10.0,
    )
    endpoint = key(12_365)
    try:
        first_done = threading.Event()

        def record_first(result: EndpointResult) -> None:
            results.append(result)
            first_done.set()

        registry.submit(
            endpoint, occurrence_id="failure", operation=lambda: False,
            completion=record_first,
        )
        wait_for(first_done)
        assert registry.lane(endpoint).snapshot().circuit_open

        registry.submit(
            endpoint, occurrence_id="recovery", operation=lambda: True,
            completion=lambda result: (results.append(result), completed.set()),
        )
        clock.advance(9.0)
        registry.poll()
        assert not completed.is_set()
        clock.advance(1.0)
        registry.poll()
        wait_for(completed)
        snapshot = registry.lane(endpoint).snapshot()
        assert snapshot.failure_count == 0
        assert not snapshot.circuit_open
        assert not snapshot.half_open
    finally:
        registry.shutdown(wait=True)


def test_stale_and_closed_completions_are_suppressed() -> None:
    gate = Gate()
    stale_results: List[EndpointResult] = []
    registry = EndpointLaneRegistry()
    endpoint = key(12_361)
    try:
        first = submit(registry, endpoint, gate, stale_results)
        assert first.accepted
        wait_for(gate.started)
        lane = registry.lane(endpoint)
        assert lane is not None
        stale_submission = lane.submit(
            EndpointWork(
                endpoint_key=endpoint,
                generation=1,
                occurrence_id="stale",
                operation=lambda: True,
                completion=stale_results.append,
            )
        )
        assert stale_submission.disposition == "stale"
        assert not stale_submission.accepted
        gate.release()
        assert lane.future.result(timeout=2.0) is True
        stale_gate = Gate()
        stale = submit(registry, endpoint, stale_gate, stale_results)
        assert stale.accepted
        assert registry.remove(endpoint)
        stale_gate.release()
        assert not any(item.generation == stale.generation for item in stale_results)
        assert registry.lane(endpoint) is None
    finally:
        gate.release()
        registry.shutdown(wait=True)


def test_remove_and_shutdown_are_idempotent_and_stop_future_submissions() -> None:
    registry = EndpointLaneRegistry()
    endpoint = key(12_362)
    try:
        assert submit(registry, endpoint, lambda: True, []).accepted
        assert registry.remove(endpoint)
        assert not registry.remove(endpoint)
        registry.shutdown(wait=True)
        stopped = submit(registry, endpoint, lambda: True, [])
        assert stopped.disposition == "stopped"
        assert not stopped.accepted
        registry.shutdown(wait=True)
    finally:
        registry.shutdown(wait=True)


def test_endpoint_result_is_passed_through_unchanged() -> None:
    results: List[EndpointResult] = []
    endpoint = key(12_363)
    expected = EndpointResult.create(
        endpoint_key=endpoint,
        generation=1,
        status="applied_and_verified",
        actual_state={"frequency_hz": 14_115_000},
        reason_code="verified",
        detail="adapter readback matched",
    )
    registry = EndpointLaneRegistry()
    try:
        done = threading.Event()

        def record(result: EndpointResult) -> None:
            results.append(result)
            done.set()

        assert registry.submit(
            endpoint, occurrence_id="result", operation=lambda: expected,
            completion=record,
        ).accepted
        lane = registry.lane(endpoint)
        assert lane is not None
        wait_for(done)
        assert results == [expected]
    finally:
        registry.shutdown(wait=True)
