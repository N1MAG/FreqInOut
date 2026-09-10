from __future__ import annotations

from tools.scheduler_multi_endpoint_soak import (
    DEFAULT_DURATION_SEC,
    ResourceCounter,
    SoakOptions,
    run_synthetic_soak,
    synthetic_endpoint_keys,
)


def test_synthetic_soak_has_fixed_eight_endpoint_matrix() -> None:
    keys = synthetic_endpoint_keys()

    assert len(keys) == 8
    assert len(set(keys)) == 8
    assert {key.adapter_family for key in keys} >= {"rigctld", "flrig", "js8call", "sdrpp", "sdrconnect", "sdrangel"}
    assert SoakOptions().duration_sec == DEFAULT_DURATION_SEC == 1800.0


def test_accelerated_smoke_runs_bounded_cycles_and_releases_all_lane_workers() -> None:
    before = ResourceCounter.snapshot()
    result = run_synthetic_soak(
        SoakOptions(
            duration_sec=0.31,
            cycle_sec=0.10,
            completion_timeout_sec=1.0,
            accelerated=True,
            sample_limit=4,
            error_limit=2,
            slow_operation_sec=0.01,
            disconnect_interval_cycles=2,
        )
    )
    after = ResourceCounter.snapshot()

    assert result.passed is True
    assert result.endpoint_count == 8
    assert result.cycles_completed == 4
    assert result.commands_submitted == result.commands_accepted == result.completions == 32
    assert result.expected_injected_failures == 6
    assert result.expected_disconnects == result.expected_reconnections == 2
    assert result.slow_operations == 4
    assert result.unexpected_failures == result.completion_timeouts == 0
    assert result.healthy_latency_samples == 4
    assert result.healthy_latency_samples_dropped == 16
    assert result.healthy_latency_p50_ms <= result.healthy_latency_p95_ms <= result.healthy_latency_max_ms
    assert result.healthy_latency_p95_budget_ms == 250.0
    assert result.healthy_latency_within_budget is True
    assert result.queue_stable is True
    assert result.queue_instability_observations == 0
    assert result.errors == ()
    assert result.resources_stable is True
    assert ResourceCounter.stable(before, after)


def test_accelerated_failure_regression_survives_more_than_1024_failures() -> None:
    """The capped backoff path must remain finite past the historical overflow."""

    result = run_synthetic_soak(
        SoakOptions(
            duration_sec=1_100.0,
            cycle_sec=1.0,
            completion_timeout_sec=1.0,
            accelerated=True,
            sample_limit=4,
            error_limit=4,
            slow_operation_sec=0.001,
            disconnect_interval_cycles=2,
        )
    )

    assert result.passed is True
    assert result.cycles_completed == 1_100
    assert result.commands_submitted == result.commands_accepted == result.completions == 8_800
    assert result.expected_injected_failures > 1_024
    assert result.unexpected_failures == 0
    assert result.completion_timeouts == 0
    assert result.queue_stable is True
    assert result.resources_stable is True
    assert result.as_dict()["passed"] is True


def test_soak_options_reject_unbounded_or_invalid_limits() -> None:
    for kwargs in (
        {"duration_sec": 0.0},
        {"cycle_sec": 0.0},
        {"completion_timeout_sec": 0.0},
        {"sample_limit": 0},
        {"error_limit": 0},
        {"rss_growth_budget_bytes": -1},
        {"slow_operation_sec": 1.0, "completion_timeout_sec": 1.0},
        {"disconnect_interval_cycles": 0},
        {"healthy_latency_p95_budget_ms": 0.0},
        {"slow_endpoint_index": 8},
        {"slow_endpoint_index": 3, "disconnect_endpoint_index": 3},
    ):
        try:
            SoakOptions(**kwargs)
        except ValueError:
            continue
        raise AssertionError("invalid soak options must fail closed: {}".format(kwargs))


def test_soak_fails_when_healthy_lane_p95_exceeds_explicit_budget() -> None:
    result = run_synthetic_soak(
        SoakOptions(
            duration_sec=0.01,
            cycle_sec=0.01,
            completion_timeout_sec=1.0,
            accelerated=True,
            slow_operation_sec=0.001,
            disconnect_interval_cycles=2,
            healthy_latency_p95_budget_ms=0.000001,
        )
    )

    assert result.healthy_latency_within_budget is False
    assert result.passed is False
