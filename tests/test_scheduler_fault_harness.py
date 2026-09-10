from __future__ import annotations

from tests.support.scheduler_fault_harness import (
    ControllableBarrier,
    CorrelationCapture,
    DeterministicClock,
    FakeEndpointAdapter,
    ResourceCounter,
    StationGlobalControlHarness,
    run_mes0_baseline,
    run_single_radio_baseline,
    run_station_global_blocking_baseline,
)


def test_station_global_blocking_baseline_reproduces_defect_and_cleans_up() -> None:
    result = run_station_global_blocking_baseline()

    assert result["defect_reproduced"] is True
    assert result["resources_stable"] is True
    assert result["cleanup"] == {
        "future_created": True,
        "completed": True,
        "error": "RuntimeError",
        "executor_shutdown": True,
        "future_done_after_shutdown": True,
    }
    assert result["healthy_peer_dispatches"] == {"radio-b": "blocked", "radio-c": "blocked"}
    assert result["healthy_peer_apply_started_while_hung"] == {"radio-b": False, "radio-c": False}
    radio_b_events = [event["phase"] for event in result["events"] if event["endpoint_key"] == "radio-b"]
    radio_c_events = [event["phase"] for event in result["events"] if event["endpoint_key"] == "radio-c"]
    assert radio_b_events == ["dispatch_blocked_global_worker", "cancel_requested"]
    assert radio_c_events == ["dispatch_blocked_global_worker", "cancel_requested"]
    assert any(event["correlation_id"] == "occurrence-a" for event in result["events"])
    assert any(event["correlation_id"] == "occurrence-b" for event in result["events"])
    assert any(event["correlation_id"] == "occurrence-c" for event in result["events"])
    timing = result["dispatch_decision_timing"]
    assert timing["count"] == 3
    assert timing["dropped"] == 0
    assert timing["p50"] <= timing["p95"] <= timing["max"]
    assert "not a production performance claim" in timing["scope"]


def test_single_radio_baseline_completes_successfully_and_cleans_up() -> None:
    result = run_single_radio_baseline()

    assert result["success_reproduced"] is True
    assert result["resources_stable"] is True
    assert result["completion"] == {"future_created": True, "completed": True, "error": None}
    assert result["cleanup"] == {
        "future_created": True,
        "completed": True,
        "error": None,
        "executor_shutdown": True,
        "future_done_after_shutdown": True,
    }
    assert [event["phase"] for event in result["events"]] == [
        "dispatch_accepted",
        "connect_started",
        "connect_finished",
        "apply_started",
        "apply_finished",
        "readback_started",
        "readback_finished",
    ]


def test_fake_adapter_barriers_are_released_without_wall_clock_sleep() -> None:
    clock = DeterministicClock()
    capture = CorrelationCapture(clock)
    apply = ControllableBarrier(label="radio-a:apply", initially_released=False)
    radio_a = FakeEndpointAdapter("radio-a", capture=capture, apply=apply)
    radio_b = FakeEndpointAdapter("radio-b", capture=capture)
    harness = StationGlobalControlHarness(capture)
    before = ResourceCounter.snapshot(thread_prefix="mes0-global-control")
    try:
        assert harness.dispatch(radio_a, {"frequency_hz": 7_100_000}, "a") == "accepted"
        assert apply.started.wait(timeout=1.0)
        assert harness.dispatch(radio_b, {"frequency_hz": 14_074_000}, "b") == "blocked"
        assert capture.phases_for("radio-b") == ["dispatch_blocked_global_worker"]
    finally:
        harness.close([radio_a, radio_b])
    after = ResourceCounter.snapshot(thread_prefix="mes0-global-control")
    assert ResourceCounter.stable(before, after)


def test_deterministic_clock_keeps_elapsed_and_wall_time_under_test_control() -> None:
    clock = DeterministicClock(monotonic_s=10.0)
    initial_utc = clock.now_utc()

    clock.advance(2.5)

    assert clock.monotonic() == 12.5
    assert (clock.now_utc() - initial_utc).total_seconds() == 2.5


def test_combined_baseline_requires_success_and_known_legacy_failure() -> None:
    result = run_mes0_baseline()

    assert result["all_checks_passed"] is True
    assert result["single_radio"]["success_reproduced"] is True
    assert result["three_radio_global_blocking"]["defect_reproduced"] is True


def test_resource_snapshot_reports_current_rss_or_clearly_named_peak_fallback() -> None:
    snapshot = ResourceCounter.snapshot()

    assert snapshot.memory_source in {"psutil.current_rss", "resource.peak_rss", "unavailable"}
    if snapshot.memory_source == "psutil.current_rss":
        assert snapshot.current_rss_bytes is not None
        assert snapshot.peak_rss_bytes is None
    elif snapshot.memory_source == "resource.peak_rss":
        assert snapshot.current_rss_bytes is None
        assert snapshot.peak_rss_bytes is not None
