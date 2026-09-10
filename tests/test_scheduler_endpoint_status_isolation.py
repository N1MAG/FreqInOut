"""Deterministic MES-3 coverage for endpoint-owned scheduler status.

These tests describe the status-registry contract independently of Qt, the
database, and protocol clients.  A blocked status source must not occupy the
worker for another endpoint, and a timeout must fence the result without
creating a replacement worker for the same still-running operation.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Callable, Dict, List, Optional

from freqinout.core.scheduler_coordination import EndpointKey
from freqinout.core.scheduler_endpoint_status import EndpointStatusRegistry


class _Clock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = float(value)

    def monotonic(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


def _key(port: int) -> EndpointKey:
    return EndpointKey.network("flrig", "127.0.0.1", port)


def _wait(event: threading.Event, description: str) -> None:
    assert event.wait(timeout=2.0), description


def _registry(clock: _Clock, executors: Optional[List[object]] = None) -> EndpointStatusRegistry:
    def factory(**kwargs: object) -> ThreadPoolExecutor:
        executor = ThreadPoolExecutor(**kwargs)
        if executors is not None:
            executors.append(executor)
        return executor

    return EndpointStatusRegistry(
        executor_factory=factory,
        monotonic_fn=clock.monotonic,
        ttl_s=5.0,
        retry_s=4.0,
    )


def _request(
    registry: EndpointStatusRegistry,
    endpoint: EndpointKey,
    poller: Callable[[], Dict[str, object]],
    results: List[object],
    finished: threading.Event,
    *,
    timeout_s: float = 8.0,
) -> object:
    return registry.request(
        endpoint,
        poller=poller,
        completion=lambda snapshot: (results.append(snapshot), finished.set()),
        timeout_s=timeout_s,
    )


def test_blocked_status_source_does_not_delay_fresh_peer_snapshot() -> None:
    clock = _Clock()
    registry = _registry(clock)
    blocked_started = threading.Event()
    blocked_release = threading.Event()
    a_results: List[object] = []
    b_results: List[object] = []
    a_done = threading.Event()
    b_done = threading.Event()

    def blocked() -> Dict[str, object]:
        blocked_started.set()
        assert blocked_release.wait(timeout=2.0)
        return {"frequency_hz": 7_100_000, "ptt_known": True, "ptt_active": False}

    try:
        _request(registry, _key(12345), blocked, a_results, a_done)
        _wait(blocked_started, "endpoint A status poll did not start")
        _request(
            registry,
            _key(12346),
            lambda: {"frequency_hz": 14_115_000, "ptt_known": True, "ptt_active": False},
            b_results,
            b_done,
        )
        _wait(b_done, "endpoint B status poll was delayed by endpoint A")
        assert len(b_results) == 1
        assert b_results[0].endpoint_key == _key(12346)
        assert b_results[0].stale is False
        assert registry.latest(_key(12346)) == b_results[0]
        assert not a_done.is_set()
    finally:
        blocked_release.set()
        registry.shutdown()


def test_failure_and_backoff_are_isolated_per_endpoint() -> None:
    clock = _Clock()
    registry = _registry(clock)
    a_calls = [0]
    b_calls = [0]
    a_results: List[object] = []
    b_results: List[object] = []
    a_done = threading.Event()
    b_done = threading.Event()

    def failing() -> Dict[str, object]:
        a_calls[0] += 1
        raise RuntimeError("endpoint A unavailable")

    def healthy() -> Dict[str, object]:
        b_calls[0] += 1
        return {"frequency_hz": 14_115_000, "ptt_known": True}

    try:
        _request(registry, _key(12347), failing, a_results, a_done)
        _request(registry, _key(12348), healthy, b_results, b_done)
        _wait(a_done, "endpoint A failure was not reported")
        _wait(b_done, "endpoint B did not complete while A failed")
        assert a_calls[0] == 1
        assert b_calls[0] == 1
        assert a_results[0].stale is True
        assert a_results[0].errors
        assert b_results[0].stale is False

        # A remains in backoff; requesting it does not churn the adapter.
        retry_results: List[object] = []
        retry_done = threading.Event()
        _request(registry, _key(12347), failing, retry_results, retry_done)
        assert a_calls[0] == 1
        assert registry.latest(_key(12347)).stale is True

        # The peer is still independently serviceable during A's backoff.
        peer_again: List[object] = []
        peer_done = threading.Event()
        _request(registry, _key(12348), healthy, peer_again, peer_done)
        _wait(peer_done, "endpoint B was incorrectly held by A backoff")
        assert b_calls[0] == 1  # fresh TTL cache, no unnecessary poll

        clock.advance(5.0)
        recovered_results: List[object] = []
        recovered_done = threading.Event()

        def recovered() -> Dict[str, object]:
            a_calls[0] += 1
            return {"frequency_hz": 14_115_000, "ptt_known": True}

        _request(registry, _key(12347), recovered, recovered_results, recovered_done)
        _wait(recovered_done, "endpoint A did not retry after its backoff")
        assert a_calls[0] == 2
        assert recovered_results[0].stale is False
    finally:
        registry.shutdown()


def test_fresh_cache_ttl_and_single_flight_prevent_duplicate_status_reads() -> None:
    clock = _Clock()
    registry = _registry(clock)
    endpoint = _key(12349)
    calls = [0]
    first_results: List[object] = []
    first_done = threading.Event()

    def poller() -> Dict[str, object]:
        calls[0] += 1
        return {"frequency_hz": 7_115_000, "ptt_known": True}

    try:
        _request(registry, endpoint, poller, first_results, first_done)
        _wait(first_done, "initial status read did not complete")
        assert calls[0] == 1

        cached_results: List[object] = []
        cached_done = threading.Event()
        _request(
            registry,
            endpoint,
            lambda: (_ for _ in ()).throw(AssertionError("fresh cache was not used")),
            cached_results,
            cached_done,
        )
        _wait(cached_done, "cached status request did not complete")
        assert calls[0] == 1
        assert cached_results[0] == first_results[0]

        clock.advance(6.0)
        slow_started = threading.Event()
        slow_release = threading.Event()
        slow_results: List[object] = []
        slow_done = threading.Event()

        def slow_poller() -> Dict[str, object]:
            calls[0] += 1
            slow_started.set()
            assert slow_release.wait(timeout=2.0)
            return {"frequency_hz": 14_115_000, "ptt_known": True}

        _request(registry, endpoint, slow_poller, slow_results, slow_done)
        _wait(slow_started, "expired status read did not start")
        pending = registry.request(
            endpoint,
            poller=slow_poller,
            completion=lambda snapshot: None,
            timeout_s=8.0,
        )
        assert getattr(pending, "accepted", True) is True
        assert calls[0] == 2
        slow_release.set()
        _wait(slow_done, "expired status read did not complete")
    finally:
        registry.shutdown()


def test_invalidate_fences_old_generation_and_preserves_new_snapshot() -> None:
    clock = _Clock()
    registry = _registry(clock)
    endpoint = _key(12350)
    old_started = threading.Event()
    old_release = threading.Event()
    old_results: List[object] = []
    old_done = threading.Event()

    def old_poller() -> Dict[str, object]:
        old_started.set()
        assert old_release.wait(timeout=2.0)
        return {"frequency_hz": 7_100_000}

    try:
        _request(registry, endpoint, old_poller, old_results, old_done)
        _wait(old_started, "old generation status read did not start")
        assert registry.invalidate(endpoint) is True
        invalidated = registry.latest(endpoint)
        assert invalidated.invalidated is True
        replacement = registry.publish(
            endpoint,
            {"frequency_hz": 14_115_000, "ptt_known": True},
            generation=invalidated.generation + 1,
            source="reconfigured",
        )
        old_release.set()
        assert old_done.wait(timeout=0.5) is False
        assert replacement.generation > invalidated.generation
        assert registry.latest(endpoint).frequency_hz == 14_115_000
    finally:
        old_release.set()
        registry.shutdown()


def test_timeout_reports_stale_without_replacing_stuck_worker() -> None:
    clock = _Clock()
    executors: List[object] = []
    registry = _registry(clock, executors)
    endpoint = _key(12351)
    started = threading.Event()
    release = threading.Event()
    results: List[object] = []
    done = threading.Event()

    def stuck() -> Dict[str, object]:
        started.set()
        assert release.wait(timeout=2.0)
        return {"frequency_hz": 7_100_000}

    try:
        _request(registry, endpoint, stuck, results, done, timeout_s=2.0)
        _wait(started, "stuck endpoint status read did not start")
        clock.advance(3.0)
        changed = registry.poll()
        assert len(changed) == 1
        timeout_snapshot = changed[0]
        assert timeout_snapshot.endpoint_key == endpoint
        assert timeout_snapshot.stale is True
        assert timeout_snapshot.timed_out is True
        assert len(executors) == 1

        # A timeout fences the result, but does not replace the still-running
        # executor or start duplicate work for the same endpoint.
        registry.poll()
        assert len(executors) == 1
        assert done.is_set()
        assert len(results) == 1
        assert results[0].timed_out is True
    finally:
        release.set()
        registry.shutdown()


def test_shutdown_fences_late_completion_and_marks_registry_closed() -> None:
    clock = _Clock()
    registry = _registry(clock)
    endpoint = _key(12352)
    started = threading.Event()
    release = threading.Event()
    results: List[object] = []
    done = threading.Event()

    def blocked() -> Dict[str, object]:
        started.set()
        assert release.wait(timeout=2.0)
        return {"frequency_hz": 14_115_000}

    _request(registry, endpoint, blocked, results, done)
    _wait(started, "shutdown-test status read did not start")
    registry.shutdown()
    release.set()
    assert done.wait(timeout=0.5) is False
    assert results == []
    assert all(snapshot.closed for snapshot in registry.snapshots())
    stopped = registry.request(endpoint, poller=lambda: {}, completion=lambda _snapshot: None)
    assert getattr(stopped, "accepted", False) is False
