#!/usr/bin/env python3
"""Run a bounded eight-endpoint synthetic scheduler qualification soak.

This is a hardware-free MES-5 qualification tool.  It exercises FIO's real
endpoint-lane registry with eight distinct, deterministic synthetic routes; it
does not open radios, SDR applications, sockets, databases, or Qt objects.

The normal qualification duration is 30 minutes.  Use ``--accelerated`` only
for smoke/CI: it executes the configured virtual cycles immediately and is not
a substitute for the real-time resource soak.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import random
import sys
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

import psutil


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from freqinout.core.scheduler_coordination import EndpointKey
from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry


DEFAULT_DURATION_SEC = 30.0 * 60.0
DEFAULT_CYCLE_SEC = 1.0
DEFAULT_COMPLETION_TIMEOUT_SEC = 2.0
DEFAULT_SAMPLE_LIMIT = 64
DEFAULT_ERROR_LIMIT = 16
DEFAULT_RSS_GROWTH_BUDGET_BYTES = 64 * 1024 * 1024
DEFAULT_SLOW_OPERATION_SEC = 0.02
DEFAULT_DISCONNECT_INTERVAL_CYCLES = 10
DEFAULT_HEALTHY_LATENCY_P95_BUDGET_MS = 250.0
THREAD_PREFIX = "freqinout-endpoint-"


@dataclass(frozen=True)
class ResourceSnapshot:
    thread_count: int
    thread_names: Tuple[str, ...]
    file_descriptor_count: Optional[int]
    child_process_count: Optional[int]
    current_rss_bytes: Optional[int]
    peak_rss_bytes: Optional[int]
    memory_source: str

    def as_dict(self) -> Dict[str, object]:
        return {
            "thread_count": self.thread_count,
            "thread_names": list(self.thread_names),
            "file_descriptor_count": self.file_descriptor_count,
            "child_process_count": self.child_process_count,
            "current_rss_bytes": self.current_rss_bytes,
            "peak_rss_bytes": self.peak_rss_bytes,
            "memory_source": self.memory_source,
        }


class ResourceCounter:
    """Small cross-platform resource counter for leak qualification."""

    @staticmethod
    def _file_descriptor_count() -> Optional[int]:
        try:
            process = psutil.Process()
            if hasattr(process, "num_fds"):
                return int(process.num_fds())
            if hasattr(process, "num_handles"):
                return int(process.num_handles())
        except (psutil.Error, OSError, AttributeError):
            pass
        proc_fd = Path("/proc/self/fd")
        try:
            return sum(1 for _ in proc_fd.iterdir()) if proc_fd.is_dir() else None
        except OSError:
            return None

    @staticmethod
    def _child_process_count() -> Optional[int]:
        try:
            return len(psutil.Process().children(recursive=True))
        except (psutil.Error, OSError):
            return None

    @staticmethod
    def _memory_usage() -> Tuple[Optional[int], Optional[int], str]:
        try:
            current = int(psutil.Process().memory_info().rss)
        except (psutil.Error, OSError):
            current = None
        try:
            import resource

            raw = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            peak = raw if os.uname().sysname == "Darwin" else raw * 1024
        except (ImportError, AttributeError, OSError):
            peak = None
        if current is not None:
            return current, peak, "psutil.current_rss"
        if peak is not None:
            return None, peak, "resource.peak_rss"
        return None, None, "unavailable"

    @classmethod
    def snapshot(cls) -> ResourceSnapshot:
        threads = [thread for thread in threading.enumerate() if thread.name.startswith(THREAD_PREFIX)]
        current, peak, source = cls._memory_usage()
        return ResourceSnapshot(
            thread_count=len(threads),
            thread_names=tuple(sorted(thread.name for thread in threads)),
            file_descriptor_count=cls._file_descriptor_count(),
            child_process_count=cls._child_process_count(),
            current_rss_bytes=current,
            peak_rss_bytes=peak,
            memory_source=source,
        )

    @staticmethod
    def stable(before: ResourceSnapshot, after: ResourceSnapshot) -> bool:
        if before.thread_count != after.thread_count or before.thread_names != after.thread_names:
            return False
        if before.file_descriptor_count is not None and after.file_descriptor_count is not None:
            if after.file_descriptor_count > before.file_descriptor_count:
                return False
        if before.child_process_count is not None and after.child_process_count is not None:
            if after.child_process_count > before.child_process_count:
                return False
        return True


@dataclass(frozen=True)
class SoakOptions:
    duration_sec: float = DEFAULT_DURATION_SEC
    cycle_sec: float = DEFAULT_CYCLE_SEC
    completion_timeout_sec: float = DEFAULT_COMPLETION_TIMEOUT_SEC
    accelerated: bool = False
    sample_limit: int = DEFAULT_SAMPLE_LIMIT
    error_limit: int = DEFAULT_ERROR_LIMIT
    rss_growth_budget_bytes: int = DEFAULT_RSS_GROWTH_BUDGET_BYTES
    slow_endpoint_index: int = 3
    slow_operation_sec: float = DEFAULT_SLOW_OPERATION_SEC
    disconnect_endpoint_index: int = 5
    disconnect_interval_cycles: int = DEFAULT_DISCONNECT_INTERVAL_CYCLES
    failing_endpoint_index: int = 7
    healthy_latency_p95_budget_ms: float = DEFAULT_HEALTHY_LATENCY_P95_BUDGET_MS

    def __post_init__(self) -> None:
        if float(self.duration_sec) <= 0.0 or float(self.cycle_sec) <= 0.0:
            raise ValueError("duration_sec and cycle_sec must be positive")
        if float(self.completion_timeout_sec) <= 0.0:
            raise ValueError("completion_timeout_sec must be positive")
        if int(self.sample_limit) <= 0 or int(self.error_limit) <= 0:
            raise ValueError("sample_limit and error_limit must be positive")
        if int(self.rss_growth_budget_bytes) < 0:
            raise ValueError("rss_growth_budget_bytes cannot be negative")
        if float(self.slow_operation_sec) < 0.0:
            raise ValueError("slow_operation_sec cannot be negative")
        if float(self.slow_operation_sec) >= float(self.completion_timeout_sec):
            raise ValueError("slow_operation_sec must remain below completion_timeout_sec")
        endpoint_indexes = (
            int(self.slow_endpoint_index),
            int(self.disconnect_endpoint_index),
            int(self.failing_endpoint_index),
        )
        if any(index < 0 or index >= 8 for index in endpoint_indexes):
            raise ValueError("fault endpoint indexes must be within the eight-endpoint matrix")
        if len(set(endpoint_indexes)) != len(endpoint_indexes):
            raise ValueError("slow, disconnect, and failing endpoints must be distinct")
        if int(self.disconnect_interval_cycles) <= 0:
            raise ValueError("disconnect_interval_cycles must be positive")
        if float(self.healthy_latency_p95_budget_ms) <= 0.0:
            raise ValueError("healthy_latency_p95_budget_ms must be positive")


@dataclass(frozen=True)
class SoakResult:
    duration_sec: float
    cycle_sec: float
    accelerated: bool
    endpoint_count: int
    cycles_completed: int
    commands_submitted: int
    commands_accepted: int
    completions: int
    expected_injected_failures: int
    unexpected_failures: int
    completion_timeouts: int
    expected_disconnects: int
    expected_reconnections: int
    slow_operations: int
    healthy_latency_p50_ms: float
    healthy_latency_p95_ms: float
    healthy_latency_max_ms: float
    healthy_latency_samples: int
    healthy_latency_samples_dropped: int
    healthy_latency_p95_budget_ms: float
    healthy_latency_within_budget: bool
    queue_instability_observations: int
    queue_stable: bool
    errors: Tuple[str, ...]
    errors_dropped: int
    elapsed_wall_sec: float
    resources_before: ResourceSnapshot
    resources_after: ResourceSnapshot
    resources_stable: bool
    rss_growth_bytes: Optional[int]
    rss_within_budget: bool

    @property
    def passed(self) -> bool:
        return bool(
            self.cycles_completed > 0
            and self.commands_submitted == self.commands_accepted
            and self.commands_submitted == self.completions
            and self.expected_injected_failures > 0
            and self.expected_disconnects > 0
            and self.expected_reconnections > 0
            and self.slow_operations > 0
            and self.unexpected_failures == 0
            and self.completion_timeouts == 0
            and self.queue_stable
            and self.healthy_latency_within_budget
            and self.resources_stable
            and self.rss_within_budget
        )

    def as_dict(self) -> Dict[str, object]:
        return {
            "duration_sec": self.duration_sec,
            "cycle_sec": self.cycle_sec,
            "accelerated": self.accelerated,
            "endpoint_count": self.endpoint_count,
            "cycles_completed": self.cycles_completed,
            "commands_submitted": self.commands_submitted,
            "commands_accepted": self.commands_accepted,
            "completions": self.completions,
            "expected_injected_failures": self.expected_injected_failures,
            "unexpected_failures": self.unexpected_failures,
            "completion_timeouts": self.completion_timeouts,
            "expected_disconnects": self.expected_disconnects,
            "expected_reconnections": self.expected_reconnections,
            "slow_operations": self.slow_operations,
            "healthy_latency_ms": {
                "p50": self.healthy_latency_p50_ms,
                "p95": self.healthy_latency_p95_ms,
                "max": self.healthy_latency_max_ms,
                "samples": self.healthy_latency_samples,
                "dropped": self.healthy_latency_samples_dropped,
                "p95_budget_ms": self.healthy_latency_p95_budget_ms,
                "within_budget": self.healthy_latency_within_budget,
            },
            "queue_instability_observations": self.queue_instability_observations,
            "queue_stable": self.queue_stable,
            "errors": list(self.errors),
            "errors_dropped": self.errors_dropped,
            "elapsed_wall_sec": self.elapsed_wall_sec,
            "resources_before": self.resources_before.as_dict(),
            "resources_after": self.resources_after.as_dict(),
            "resources_stable": self.resources_stable,
            "rss_growth_bytes": self.rss_growth_bytes,
            "rss_within_budget": self.rss_within_budget,
            "passed": self.passed,
        }


class _BoundedCounters:
    def __init__(self, *, sample_limit: int, error_limit: int) -> None:
        self._lock = threading.Lock()
        self._sample_limit = int(sample_limit)
        self._error_limit = int(error_limit)
        self.submitted = 0
        self.accepted = 0
        self.completed = 0
        self.expected_injected_failures = 0
        self.unexpected_failures = 0
        self.expected_disconnects = 0
        self.expected_reconnections = 0
        self.slow_operations = 0
        self.healthy_latencies_ms: List[float] = []
        self.healthy_latencies_dropped = 0
        self._healthy_latency_max_ms = 0.0
        self._healthy_latency_seen = 0
        self._reservoir_random = random.Random(0xF105)
        self.errors: List[str] = []
        self.errors_dropped = 0

    def submitted_one(self, *, accepted: bool) -> None:
        with self._lock:
            self.submitted += 1
            self.accepted += int(bool(accepted))

    def completed_one(
        self,
        *,
        elapsed_ms: float,
        status: str,
        detail: str,
        fault_kind: str = "",
        healthy: bool = True,
    ) -> None:
        with self._lock:
            self.completed += 1
            if healthy:
                sample = max(0.0, float(elapsed_ms))
                self._healthy_latency_seen += 1
                self._healthy_latency_max_ms = max(self._healthy_latency_max_ms, sample)
                if len(self.healthy_latencies_ms) < self._sample_limit:
                    self.healthy_latencies_ms.append(sample)
                else:
                    self.healthy_latencies_dropped += 1
                    replacement = self._reservoir_random.randrange(self._healthy_latency_seen)
                    if replacement < self._sample_limit:
                        self.healthy_latencies_ms[replacement] = sample
            if fault_kind == "slow" and status in {"applied", "applied_unverified"}:
                self.slow_operations += 1
            elif fault_kind == "disconnect" and status not in {"applied", "applied_unverified"}:
                self.expected_injected_failures += 1
                self.expected_disconnects += 1
            elif fault_kind == "reconnect" and status in {"applied", "applied_unverified"}:
                self.expected_reconnections += 1
            elif fault_kind == "recurring_failure" and status not in {"applied", "applied_unverified"}:
                self.expected_injected_failures += 1
            elif status not in {"applied", "applied_unverified"}:
                self.unexpected_failures += 1
                self._record_error_locked(status, detail)

    def _record_error_locked(self, status: str, detail: str) -> None:
        message = "{}: {}".format(status, str(detail or "endpoint result failed").strip())
        if len(self.errors) < self._error_limit:
            self.errors.append(message)
        else:
            self.errors_dropped += 1

    def submission_rejected(self, disposition: str) -> None:
        with self._lock:
            self.unexpected_failures += 1
            self._record_error_locked(disposition, "submission was not accepted")

    def healthy_latency_summary(self) -> Tuple[float, float, float, int, int]:
        with self._lock:
            samples = sorted(self.healthy_latencies_ms)
            dropped = self.healthy_latencies_dropped
        if not samples:
            return 0.0, 0.0, 0.0, 0, dropped

        def percentile(fraction: float) -> float:
            index = (len(samples) - 1) * fraction
            lower = int(index)
            upper = min(lower + 1, len(samples) - 1)
            if lower == upper:
                return samples[lower]
            return samples[lower] + (samples[upper] - samples[lower]) * (index - lower)

        return percentile(0.50), percentile(0.95), self._healthy_latency_max_ms, len(samples), dropped

    def timeout_one(self, detail: str) -> None:
        with self._lock:
            self.unexpected_failures += 1
            self._record_error_locked("completion_timeout", detail)


def synthetic_endpoint_keys() -> Tuple[EndpointKey, ...]:
    """Return eight fixed routes spanning four transceiver and four SDR lanes."""
    specs = (
        ("rigctld", 45_320, ""),
        ("flrig", 12_345, ""),
        ("js8call", 2_442, ""),
        ("sdrpp", 5_000, "vfo-a"),
        ("sdrconnect", 5_001, "vfo-a"),
        ("rigctld", 45_321, ""),
        ("sdrpp", 5_002, "vfo-b"),
        ("sdrangel", 8_097, "deviceset-1"),
    )
    return tuple(
        EndpointKey.network(family, "127.0.0.1", port, target=target)
        for family, port, target in specs
    )


def _cycle_count(options: SoakOptions) -> int:
    return max(1, int((float(options.duration_sec) + float(options.cycle_sec) - 1e-9) / float(options.cycle_sec)))


def run_synthetic_soak(options: Optional[SoakOptions] = None) -> SoakResult:
    """Exercise eight isolated lanes and return bounded qualification evidence.

    Three deterministic fault schedules run beside the healthy lanes: one
    deliberately slow receive endpoint, one periodic disconnect/reconnect, and
    one endpoint that repeatedly returns a command failure.  These are expected
    qualification observations, not harness failures; healthy-lane latency is
    retained separately so a slow/failing peer cannot hide global blocking.
    """
    options = options or SoakOptions()
    endpoint_keys = synthetic_endpoint_keys()
    counter = _BoundedCounters(sample_limit=options.sample_limit, error_limit=options.error_limit)
    before = ResourceCounter.snapshot()
    # Keep the repeatedly failing synthetic endpoint observable on every cycle.
    # Circuit/open behavior is covered by focused lane tests; this qualification
    # run measures peer isolation and bounded resources under repeated failures.
    registry = EndpointLaneRegistry(
        base_backoff_s=0.0,
        max_backoff_s=0.0,
        circuit_failure_threshold=100_000,
    )
    start = time.monotonic()
    deadline = start + float(options.duration_sec)
    cycles = 0
    completion_timeouts = 0
    queue_instability_observations = 0
    reconnect_due = False
    stop_event = threading.Event()
    try:
        while True:
            if options.accelerated:
                if cycles >= _cycle_count(options):
                    break
            elif cycles > 0 and time.monotonic() >= deadline:
                break
            completed_events: List[threading.Event] = []
            for endpoint_index, endpoint_key in enumerate(endpoint_keys):
                completed = threading.Event()
                completed_events.append(completed)
                generation = cycles + 1
                occurrence_id = "soak-{}-{}".format(cycles, endpoint_index)
                submitted_at = time.monotonic()
                desired_state = {
                    "frequency_hz": 7_000_000 + endpoint_index * 100_000 + generation,
                    "cycle": generation,
                    "receive_only": endpoint_index in {3, 4, 6, 7},
                }

                fault_kind = ""
                if endpoint_index == int(options.failing_endpoint_index):
                    fault_kind = "recurring_failure"
                elif endpoint_index == int(options.disconnect_endpoint_index):
                    if cycles % int(options.disconnect_interval_cycles) == 0:
                        fault_kind = "disconnect"
                        reconnect_due = True
                    elif reconnect_due:
                        fault_kind = "reconnect"
                        reconnect_due = False
                elif endpoint_index == int(options.slow_endpoint_index):
                    fault_kind = "slow"
                healthy = endpoint_index not in {
                    int(options.slow_endpoint_index),
                    int(options.disconnect_endpoint_index),
                    int(options.failing_endpoint_index),
                }

                def operation(
                    state=dict(desired_state),
                    injected_fault=fault_kind,
                ) -> Dict[str, object]:
                    if injected_fault == "slow":
                        # Event.wait provides a bounded delay without an
                        # uninterruptible sleep or a per-command helper.
                        threading.Event().wait(float(options.slow_operation_sec))
                    if injected_fault == "disconnect":
                        return {
                            "ok": False,
                            "reason_code": "synthetic_disconnected",
                            "detail": "Expected synthetic disconnect; next cycle attempts reconnect.",
                        }
                    if injected_fault == "recurring_failure":
                        return {
                            "ok": False,
                            "reason_code": "synthetic_repeated_failure",
                            "detail": "Expected synthetic repeated endpoint failure.",
                        }
                    return {"ok": True, "actual_state": state}

                def completion(
                    result: object,
                    *,
                    event=completed,
                    issued_at=submitted_at,
                    injected_fault=fault_kind,
                    is_healthy=healthy,
                ) -> None:
                    status = str(getattr(result, "status", "") or "")
                    detail = str(getattr(result, "detail", "") or "")
                    counter.completed_one(
                        elapsed_ms=(time.monotonic() - issued_at) * 1000.0,
                        status=status,
                        detail=detail,
                        fault_kind=injected_fault,
                        healthy=is_healthy,
                    )
                    event.set()

                submission = registry.submit(
                    endpoint_key,
                    occurrence_id=occurrence_id,
                    operation=operation,
                    completion=completion,
                    timeout_s=float(options.completion_timeout_sec),
                )
                counter.submitted_one(accepted=submission.accepted)
                if not submission.accepted:
                    counter.submission_rejected(submission.disposition)
                    completed.set()
            for event in completed_events:
                if not event.wait(timeout=float(options.completion_timeout_sec)):
                    completion_timeouts += 1
                    counter.timeout_one("endpoint completion did not arrive before the bounded timeout")
            registry.poll()
            queue_instability_observations += sum(
                1 for snapshot in registry.snapshots() if snapshot.state != "idle"
            )
            cycles += 1
            if not options.accelerated:
                remaining = min(float(options.cycle_sec), max(0.0, deadline - time.monotonic()))
                if remaining > 0.0:
                    stop_event.wait(remaining)
    finally:
        registry.shutdown(wait=True, cancel_futures=True)
    after = ResourceCounter.snapshot()
    current_before = before.current_rss_bytes
    current_after = after.current_rss_bytes
    rss_growth = (
        max(0, current_after - current_before)
        if current_before is not None and current_after is not None
        else None
    )
    rss_within_budget = rss_growth is None or rss_growth <= int(options.rss_growth_budget_bytes)
    healthy_p50, healthy_p95, healthy_max, healthy_samples, healthy_dropped = counter.healthy_latency_summary()
    healthy_latency_within_budget = (
        healthy_p95 <= float(options.healthy_latency_p95_budget_ms)
    )
    return SoakResult(
        duration_sec=float(options.duration_sec),
        cycle_sec=float(options.cycle_sec),
        accelerated=bool(options.accelerated),
        endpoint_count=len(endpoint_keys),
        cycles_completed=cycles,
        commands_submitted=counter.submitted,
        commands_accepted=counter.accepted,
        completions=counter.completed,
        expected_injected_failures=counter.expected_injected_failures,
        unexpected_failures=counter.unexpected_failures,
        completion_timeouts=completion_timeouts,
        expected_disconnects=counter.expected_disconnects,
        expected_reconnections=counter.expected_reconnections,
        slow_operations=counter.slow_operations,
        healthy_latency_p50_ms=healthy_p50,
        healthy_latency_p95_ms=healthy_p95,
        healthy_latency_max_ms=healthy_max,
        healthy_latency_samples=healthy_samples,
        healthy_latency_samples_dropped=healthy_dropped,
        healthy_latency_p95_budget_ms=float(options.healthy_latency_p95_budget_ms),
        healthy_latency_within_budget=healthy_latency_within_budget,
        queue_instability_observations=queue_instability_observations,
        queue_stable=queue_instability_observations == 0,
        errors=tuple(counter.errors),
        errors_dropped=counter.errors_dropped,
        elapsed_wall_sec=max(0.0, time.monotonic() - start),
        resources_before=before,
        resources_after=after,
        resources_stable=ResourceCounter.stable(before, after),
        rss_growth_bytes=rss_growth,
        rss_within_budget=rss_within_budget,
    )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-sec", type=float, default=DEFAULT_DURATION_SEC)
    parser.add_argument("--cycle-sec", type=float, default=DEFAULT_CYCLE_SEC)
    parser.add_argument("--completion-timeout-sec", type=float, default=DEFAULT_COMPLETION_TIMEOUT_SEC)
    parser.add_argument("--accelerated", action="store_true", help="Run virtual cycles without real-time waits (CI smoke only).")
    parser.add_argument("--sample-limit", type=int, default=DEFAULT_SAMPLE_LIMIT)
    parser.add_argument("--error-limit", type=int, default=DEFAULT_ERROR_LIMIT)
    parser.add_argument("--rss-growth-budget-bytes", type=int, default=DEFAULT_RSS_GROWTH_BUDGET_BYTES)
    parser.add_argument("--slow-operation-sec", type=float, default=DEFAULT_SLOW_OPERATION_SEC)
    parser.add_argument("--disconnect-interval-cycles", type=int, default=DEFAULT_DISCONNECT_INTERVAL_CYCLES)
    parser.add_argument(
        "--healthy-latency-p95-budget-ms",
        type=float,
        default=DEFAULT_HEALTHY_LATENCY_P95_BUDGET_MS,
    )
    parser.add_argument("--json", action="store_true", help="Emit the bounded qualification report as JSON.")
    parser.add_argument("--json-out", default="", help="Optional JSON report path.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        result = run_synthetic_soak(
            SoakOptions(
                duration_sec=args.duration_sec,
                cycle_sec=args.cycle_sec,
                completion_timeout_sec=args.completion_timeout_sec,
                accelerated=args.accelerated,
                sample_limit=args.sample_limit,
                error_limit=args.error_limit,
                rss_growth_budget_bytes=args.rss_growth_budget_bytes,
                slow_operation_sec=args.slow_operation_sec,
                disconnect_interval_cycles=args.disconnect_interval_cycles,
                healthy_latency_p95_budget_ms=args.healthy_latency_p95_budget_ms,
            )
        )
    except ValueError as exc:
        print("Invalid soak options: {}".format(exc), file=sys.stderr)
        return 2
    report = result.as_dict()
    if args.json_out:
        Path(args.json_out).expanduser().write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print("MES-5 eight-endpoint synthetic scheduler qualification")
        print("  mode: {}".format("accelerated smoke" if result.accelerated else "real-time soak"))
        print("  endpoints / cycles / commands: {} / {} / {}".format(
            result.endpoint_count, result.cycles_completed, result.commands_submitted
        ))
        print("  completions / expected faults / unexpected / timeouts: {} / {} / {} / {}".format(
            result.completions,
            result.expected_injected_failures,
            result.unexpected_failures,
            result.completion_timeouts,
        ))
        print("  disconnects / reconnects / slow operations: {} / {} / {}".format(
            result.expected_disconnects,
            result.expected_reconnections,
            result.slow_operations,
        ))
        print("  healthy latency p50 / p95 / max ms: {:.3f} / {:.3f} / {:.3f}".format(
            result.healthy_latency_p50_ms,
            result.healthy_latency_p95_ms,
            result.healthy_latency_max_ms,
        ))
        print("  healthy p95 budget ms / within budget: {:.3f} / {}".format(
            result.healthy_latency_p95_budget_ms,
            result.healthy_latency_within_budget,
        ))
        print("  resource stable: {}; RSS within budget: {}".format(
            result.resources_stable, result.rss_within_budget
        ))
        print("  result: {}".format("PASS" if result.passed else "FAIL"))
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
