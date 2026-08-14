from __future__ import annotations

import threading
import time

import pytest

from freqinout.core.radio_status_poll_coordinator import RadioStatusPollCoordinator


def test_coordinator_reuses_fresh_snapshot_until_ttl_expires() -> None:
    now = [100.0]
    calls: list[int] = []
    coordinator = RadioStatusPollCoordinator(ttl_seconds=2.0, time_fn=lambda: now[0])

    def poller():
        calls.append(1)
        return {"frequency_hz": 7_115_000, "ptt_active": False, "ptt_known": True, "vfo": "A"}

    first = coordinator.get_snapshot("radio_1", poller)
    second = coordinator.get_snapshot("radio_1", poller)
    now[0] = 103.0
    third = coordinator.get_snapshot("radio_1", poller)

    assert first is second
    assert third is not first
    assert third.frequency_hz == 7_115_000
    assert third.ptt_known is True
    assert len(calls) == 2


def test_force_refresh_bypasses_ttl() -> None:
    now = [100.0]
    values = [7_115_000, 14_115_000]
    coordinator = RadioStatusPollCoordinator(ttl_seconds=30.0, time_fn=lambda: now[0])

    def poller():
        return {"frequency_hz": values.pop(0), "ptt_active": False, "ptt_known": True}

    first = coordinator.get_snapshot("radio_1", poller)
    second = coordinator.get_snapshot("radio_1", poller, force=True)

    assert first.frequency_hz == 7_115_000
    assert second.frequency_hz == 14_115_000


def test_poll_error_preserves_cached_state_and_applies_backoff() -> None:
    now = [100.0]
    calls = 0
    coordinator = RadioStatusPollCoordinator(ttl_seconds=0.0, retry_seconds=5.0, time_fn=lambda: now[0])

    def good_poller():
        return {"frequency_hz": 7_115_000, "ptt_active": True, "ptt_known": True}

    def failing_poller():
        nonlocal calls
        calls += 1
        raise RuntimeError("FLRig timeout")

    good = coordinator.get_snapshot("radio_1", good_poller)
    now[0] = 101.0
    failed = coordinator.get_snapshot("radio_1", failing_poller)
    now[0] = 102.0
    backed_off = coordinator.get_snapshot("radio_1", failing_poller)

    assert good.frequency_hz == 7_115_000
    assert failed.frequency_hz == 7_115_000
    assert failed.ptt_active is True
    assert failed.stale is True
    assert failed.source == "error"
    assert failed.errors == {"poll": "FLRig timeout"}
    assert failed.backoff_until == pytest.approx(106.0)
    assert backed_off.source == "backoff"
    assert calls == 1


def test_invalidate_clears_cached_snapshot() -> None:
    now = [100.0]
    calls = 0
    coordinator = RadioStatusPollCoordinator(ttl_seconds=30.0, time_fn=lambda: now[0])

    def poller():
        nonlocal calls
        calls += 1
        return {"frequency_hz": 7_115_000 + calls}

    coordinator.get_snapshot("radio_1", poller)
    coordinator.invalidate("radio_1")
    refreshed = coordinator.get_snapshot("radio_1", poller)

    assert refreshed.frequency_hz == 7_115_002
    assert calls == 2


def test_concurrent_refreshes_are_single_flight_for_one_radio() -> None:
    coordinator = RadioStatusPollCoordinator(ttl_seconds=0.0)
    started = threading.Event()
    release = threading.Event()
    calls = 0

    def slow_poller():
        nonlocal calls
        calls += 1
        started.set()
        assert release.wait(timeout=2.0)
        return {"frequency_hz": 7_115_000, "ptt_active": False, "ptt_known": True}

    results = []
    thread = threading.Thread(target=lambda: results.append(coordinator.get_snapshot("radio_1", slow_poller)))
    thread.start()
    assert started.wait(timeout=2.0)

    pending = coordinator.get_snapshot("radio_1", slow_poller)
    release.set()
    thread.join(timeout=2.0)

    assert pending.stale is True
    assert pending.refresh_pending is True
    assert len(results) == 1
    assert results[0].frequency_hz == 7_115_000
    assert calls == 1


def test_concurrent_refresh_returns_stale_cached_snapshot_when_available() -> None:
    coordinator = RadioStatusPollCoordinator(ttl_seconds=0.0)
    coordinator.get_snapshot("radio_1", lambda: {"frequency_hz": 7_115_000, "ptt_known": True})
    started = threading.Event()
    release = threading.Event()

    def slow_poller():
        started.set()
        assert release.wait(timeout=2.0)
        return {"frequency_hz": 14_115_000, "ptt_known": True}

    thread = threading.Thread(target=lambda: coordinator.get_snapshot("radio_1", slow_poller, force=True))
    thread.start()
    assert started.wait(timeout=2.0)

    pending = coordinator.get_snapshot("radio_1", slow_poller, force=True)
    release.set()
    thread.join(timeout=2.0)

    assert pending.frequency_hz == 7_115_000
    assert pending.stale is True
    assert pending.refresh_pending is True


def test_empty_radio_id_is_rejected() -> None:
    coordinator = RadioStatusPollCoordinator()

    with pytest.raises(ValueError, match="radio_id"):
        coordinator.get_snapshot("", lambda: {})


def test_metrics_track_cache_poll_backoff_and_failure_paths() -> None:
    now = [100.0]
    coordinator = RadioStatusPollCoordinator(ttl_seconds=2.0, retry_seconds=5.0, time_fn=lambda: now[0])

    coordinator.get_snapshot("radio_1", lambda: {"frequency_hz": 7_115_000})
    coordinator.get_snapshot("radio_1", lambda: {"frequency_hz": 14_115_000})
    now[0] = 103.0

    def failing_poller():
        raise RuntimeError("radio offline")

    coordinator.get_snapshot("radio_1", failing_poller)
    now[0] = 104.0
    coordinator.get_snapshot("radio_1", failing_poller)

    metrics = coordinator.metrics_snapshot()

    assert metrics.snapshot_count == 1
    assert metrics.inflight_count == 0
    assert metrics.cache_hits == 1
    assert metrics.backoff_hits == 1
    assert metrics.polls_started == 2
    assert metrics.polls_succeeded == 1
    assert metrics.polls_failed == 1
    assert metrics.polls_completed == 2
    assert metrics.as_dict()["polls_completed"] == 2


def test_metrics_track_single_flight_reuse() -> None:
    coordinator = RadioStatusPollCoordinator(ttl_seconds=0.0)
    started = threading.Event()
    release = threading.Event()

    def slow_poller():
        started.set()
        assert release.wait(timeout=2.0)
        return {"frequency_hz": 7_115_000}

    thread = threading.Thread(target=lambda: coordinator.get_snapshot("radio_1", slow_poller))
    thread.start()
    assert started.wait(timeout=2.0)

    coordinator.get_snapshot("radio_1", slow_poller)
    release.set()
    thread.join(timeout=2.0)

    metrics = coordinator.metrics_snapshot()

    assert metrics.inflight_hits == 1
    assert metrics.polls_started == 1
    assert metrics.polls_succeeded == 1
