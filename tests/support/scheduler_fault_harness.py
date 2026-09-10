"""Deterministic, test-only fixtures for MES-0 scheduler characterization.

These helpers deliberately contain no FIO production imports.  They model the
current station-global, single-worker dispatch boundary closely enough to make
the known blocking behavior repeatable without hardware, wall-clock sleeps, or
Qt event-loop timing.  MES-1+ tests can reuse the fake adapters and evidence
capture while moving the implementation behind endpoint lanes.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional


@dataclass(frozen=True)
class FaultEvent:
    """One safe, correlation-bearing observation emitted by the harness."""

    sequence: int
    phase: str
    endpoint_key: str
    correlation_id: str
    monotonic_s: float
    detail: str = ""


class CorrelationCapture:
    """Thread-safe, ordered evidence for deterministic fault assertions."""

    def __init__(self, clock: "DeterministicClock") -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._events: List[FaultEvent] = []

    def record(self, phase: str, endpoint_key: str, correlation_id: str, detail: str = "") -> FaultEvent:
        with self._lock:
            event = FaultEvent(
                sequence=len(self._events) + 1,
                phase=str(phase),
                endpoint_key=str(endpoint_key),
                correlation_id=str(correlation_id),
                monotonic_s=self._clock.monotonic(),
                detail=str(detail),
            )
            self._events.append(event)
            return event

    def events(self) -> List[FaultEvent]:
        with self._lock:
            return list(self._events)

    def as_dicts(self) -> List[Dict[str, Any]]:
        return [asdict(event) for event in self.events()]

    def phases_for(self, endpoint_key: str) -> List[str]:
        return [event.phase for event in self.events() if event.endpoint_key == endpoint_key]


class DeterministicClock:
    """Controllable monotonic and UTC wall clocks with no sleeping."""

    def __init__(self, *, monotonic_s: float = 0.0, utc: Optional[datetime] = None) -> None:
        self._lock = threading.Lock()
        self._monotonic_s = float(monotonic_s)
        self._utc = utc or datetime(2026, 1, 1, tzinfo=timezone.utc)
        if self._utc.tzinfo is None:
            raise ValueError("utc must be timezone aware")

    def monotonic(self) -> float:
        with self._lock:
            return self._monotonic_s

    def now_utc(self) -> datetime:
        with self._lock:
            return self._utc

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("advance seconds must be non-negative")
        with self._lock:
            self._monotonic_s += float(seconds)
            self._utc += timedelta(seconds=float(seconds))

    def set_utc(self, utc: datetime) -> None:
        if utc.tzinfo is None:
            raise ValueError("utc must be timezone aware")
        with self._lock:
            self._utc = utc.astimezone(timezone.utc)


class ControllableBarrier:
    """A deterministic endpoint operation gate.

    Tests wait for ``started`` and then explicitly call ``release`` or
    ``fail``.  The worker never relies on a duration-based sleep, so a hung
    adapter call is both repeatable and safely releasable during cleanup.
    """

    def __init__(self, *, label: str = "operation", initially_released: bool = True) -> None:
        self.label = label
        self.started = threading.Event()
        self._released = threading.Event()
        self._lock = threading.Lock()
        self._failure: Optional[BaseException] = None
        if initially_released:
            self._released.set()

    @property
    def is_released(self) -> bool:
        return self._released.is_set()

    def wait(self, *, cancel_event: Optional[threading.Event] = None) -> None:
        self.started.set()
        while not self._released.wait(timeout=0.01):
            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError(f"{self.label} cancelled")
        if cancel_event is not None and cancel_event.is_set():
            raise RuntimeError(f"{self.label} cancelled")
        with self._lock:
            failure = self._failure
        if failure is not None:
            raise failure

    def release(self) -> None:
        self._released.set()

    def fail(self, exc: Optional[BaseException] = None) -> None:
        with self._lock:
            self._failure = exc or RuntimeError(f"{self.label} failed")
        self._released.set()


class FakeEndpointAdapter:
    """Scriptable endpoint fake with connect/apply/readback operation gates."""

    def __init__(
        self,
        endpoint_key: str,
        *,
        capture: CorrelationCapture,
        connect: Optional[ControllableBarrier] = None,
        apply: Optional[ControllableBarrier] = None,
        readback: Optional[ControllableBarrier] = None,
    ) -> None:
        self.endpoint_key = endpoint_key
        self.capture = capture
        self.connect_barrier = connect or ControllableBarrier(label=f"{endpoint_key}:connect")
        self.apply_barrier = apply or ControllableBarrier(label=f"{endpoint_key}:apply")
        self.readback_barrier = readback or ControllableBarrier(label=f"{endpoint_key}:readback")
        self.calls: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._cancelled = threading.Event()

    def _invoke(self, operation: str, barrier: ControllableBarrier, correlation_id: str, payload: Any = None) -> Any:
        self.capture.record(f"{operation}_started", self.endpoint_key, correlation_id)
        with self._lock:
            self.calls.append({"operation": operation, "correlation_id": correlation_id, "payload": payload})
        try:
            barrier.wait(cancel_event=self._cancelled)
        except BaseException as exc:
            self.capture.record(f"{operation}_failed", self.endpoint_key, correlation_id, type(exc).__name__)
            raise
        self.capture.record(f"{operation}_finished", self.endpoint_key, correlation_id)
        return payload

    def connect(self, correlation_id: str) -> None:
        self._invoke("connect", self.connect_barrier, correlation_id)

    def apply(self, desired_state: Mapping[str, Any], correlation_id: str) -> Mapping[str, Any]:
        return self._invoke("apply", self.apply_barrier, correlation_id, dict(desired_state))

    def readback(self, correlation_id: str) -> Mapping[str, Any]:
        result = self._invoke("readback", self.readback_barrier, correlation_id, {"endpoint_key": self.endpoint_key})
        return dict(result or {})

    def cancel(self) -> None:
        self._cancelled.set()
        self.capture.record("cancel_requested", self.endpoint_key, "harness")

    def release_all(self) -> None:
        self.connect_barrier.release()
        self.apply_barrier.release()
        self.readback_barrier.release()


@dataclass(frozen=True)
class ResourceSnapshot:
    thread_count: int
    thread_names: tuple[str, ...]
    file_descriptor_count: Optional[int]
    child_process_count: Optional[int]
    current_rss_bytes: Optional[int]
    peak_rss_bytes: Optional[int]
    memory_source: str

    def as_dict(self) -> Dict[str, Any]:
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
    """Portable, stable lifecycle counters for bounded test/soak assertions."""

    @staticmethod
    def _file_descriptor_count() -> Optional[int]:
        proc_fd = Path("/proc/self/fd")
        if proc_fd.is_dir():
            return sum(1 for _ in proc_fd.iterdir())
        try:
            import psutil  # type: ignore

            process = psutil.Process()
            if hasattr(process, "num_fds"):
                return int(process.num_fds())
            if hasattr(process, "num_handles"):
                return int(process.num_handles())
        except Exception:
            pass
        return None

    @staticmethod
    def _child_process_count() -> Optional[int]:
        try:
            import psutil  # type: ignore

            return len(psutil.Process().children(recursive=True))
        except Exception:
            return None

    @staticmethod
    def _memory_usage() -> tuple[Optional[int], Optional[int], str]:
        """Return current RSS when possible, then a clearly named peak fallback."""
        try:
            import psutil  # type: ignore

            return int(psutil.Process().memory_info().rss), None, "psutil.current_rss"
        except Exception:
            pass
        try:
            import resource

            value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            # macOS reports bytes; Linux reports KiB.
            peak_rss_bytes = value if os.name == "posix" and os.uname().sysname == "Darwin" else value * 1024
            return None, peak_rss_bytes, "resource.peak_rss"
        except Exception:
            return None, None, "unavailable"

    @classmethod
    def snapshot(cls, *, thread_prefix: Optional[str] = None) -> ResourceSnapshot:
        threads = [thread for thread in threading.enumerate() if thread_prefix is None or thread.name.startswith(thread_prefix)]
        current_rss_bytes, peak_rss_bytes, memory_source = cls._memory_usage()
        return ResourceSnapshot(
            thread_count=len(threads),
            thread_names=tuple(sorted(thread.name for thread in threads)),
            file_descriptor_count=cls._file_descriptor_count(),
            child_process_count=cls._child_process_count(),
            current_rss_bytes=current_rss_bytes,
            peak_rss_bytes=peak_rss_bytes,
            memory_source=memory_source,
        )

    @staticmethod
    def stable(before: ResourceSnapshot, after: ResourceSnapshot, *, fd_slack: int = 0, child_slack: int = 0) -> bool:
        if before.thread_count != after.thread_count or before.thread_names != after.thread_names:
            return False
        if before.file_descriptor_count is not None and after.file_descriptor_count is not None:
            if after.file_descriptor_count > before.file_descriptor_count + fd_slack:
                return False
        if before.child_process_count is not None and after.child_process_count is not None:
            if after.child_process_count > before.child_process_count + child_slack:
                return False
        return True


class StationGlobalControlHarness:
    """A test-only model of the current one-control-future dispatch boundary.

    It captures, rather than conceals, the MES-0 defect: once one adapter call
    is running, a different endpoint cannot be dispatched through the shared
    worker.  This must be removed by MES-2 endpoint lanes, not "fixed" here.
    """

    def __init__(self, capture: CorrelationCapture) -> None:
        self.capture = capture
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mes0-global-control")
        self._future: Optional[Future[Any]] = None
        self._lock = threading.Lock()
        self._closed = False
        self._dispatch_samples_ms: List[float] = []
        self._dispatch_sample_limit = 16
        self._dispatch_samples_dropped = 0

    def dispatch(self, adapter: FakeEndpointAdapter, desired_state: Mapping[str, Any], correlation_id: str) -> str:
        started_at = time.perf_counter()
        with self._lock:
            if self._closed:
                self.capture.record("dispatch_rejected_shutdown", adapter.endpoint_key, correlation_id)
                outcome = "shutdown"
            elif self._future is not None and not self._future.done():
                self.capture.record("dispatch_blocked_global_worker", adapter.endpoint_key, correlation_id)
                outcome = "blocked"
            else:
                self.capture.record("dispatch_accepted", adapter.endpoint_key, correlation_id)

                def _transaction() -> Mapping[str, Any]:
                    adapter.connect(correlation_id)
                    adapter.apply(desired_state, correlation_id)
                    return adapter.readback(correlation_id)

                self._future = self._executor.submit(_transaction)
                outcome = "accepted"
            elapsed_ms = (time.perf_counter() - started_at) * 1000.0
            if len(self._dispatch_samples_ms) < self._dispatch_sample_limit:
                self._dispatch_samples_ms.append(elapsed_ms)
            else:
                self._dispatch_samples_dropped += 1
            return outcome

    def dispatch_timing_summary(self) -> Dict[str, Any]:
        """Bounded in-process dispatch-decision timing; never endpoint latency."""
        with self._lock:
            samples = sorted(self._dispatch_samples_ms)
            dropped = self._dispatch_samples_dropped
        if not samples:
            return {"unit": "ms", "scope": "harness dispatch decision only", "count": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "dropped": dropped}

        def _percentile(percent: float) -> float:
            index = (len(samples) - 1) * percent
            lower = int(index)
            upper = min(lower + 1, len(samples) - 1)
            if lower == upper:
                return samples[lower]
            return samples[lower] + (samples[upper] - samples[lower]) * (index - lower)

        return {
            "unit": "ms",
            "scope": "harness dispatch decision only; excludes endpoint I/O and is not a production performance claim",
            "count": len(samples),
            "p50": _percentile(0.50),
            "p95": _percentile(0.95),
            "max": samples[-1],
            "dropped": dropped,
        }

    def wait_for_completion(self, *, timeout_s: float = 1.0) -> Dict[str, Any]:
        with self._lock:
            future = self._future
        if future is None:
            return {"future_created": False, "completed": True, "error": None}
        try:
            future.result(timeout=timeout_s)
        except TimeoutError:
            return {"future_created": True, "completed": False, "error": "timeout"}
        except BaseException as exc:
            return {"future_created": True, "completed": True, "error": type(exc).__name__}
        return {"future_created": True, "completed": True, "error": None}

    def close(
        self,
        adapters: Iterable[FakeEndpointAdapter],
        *,
        cancel_in_flight: bool = False,
        timeout_s: float = 1.0,
    ) -> Dict[str, Any]:
        with self._lock:
            self._closed = True
        adapter_list = list(adapters)
        if cancel_in_flight:
            for adapter in adapter_list:
                adapter.cancel()
        for adapter in adapter_list:
            adapter.release_all()
        completion = self.wait_for_completion(timeout_s=timeout_s)
        self._executor.shutdown(wait=True, cancel_futures=True)
        completion["executor_shutdown"] = True
        completion["future_done_after_shutdown"] = self._future is None or self._future.done()
        return completion


def run_single_radio_baseline() -> Dict[str, Any]:
    """Record the current successful one-radio control sequence and cleanup."""

    clock = DeterministicClock()
    capture = CorrelationCapture(clock)
    radio_a = FakeEndpointAdapter("radio-a", capture=capture)
    resources_before = ResourceCounter.snapshot(thread_prefix="mes0-global-control")
    harness = StationGlobalControlHarness(capture)
    try:
        dispatch = harness.dispatch(radio_a, {"frequency_hz": 7_100_000}, "single-radio-a")
        completion = harness.wait_for_completion()
    finally:
        cleanup = harness.close([radio_a])
    resources_after = ResourceCounter.snapshot(thread_prefix="mes0-global-control")
    successful = dispatch == "accepted" and completion["completed"] and completion["error"] is None
    return {
        "scenario": "single_radio_success",
        "dispatch": dispatch,
        "completion": completion,
        "cleanup": cleanup,
        "success_reproduced": successful,
        "events": capture.as_dicts(),
        "dispatch_decision_timing": harness.dispatch_timing_summary(),
        "resources_before": resources_before.as_dict(),
        "resources_after": resources_after.as_dict(),
        "resources_stable": ResourceCounter.stable(resources_before, resources_after),
    }


def run_station_global_blocking_baseline() -> Dict[str, Any]:
    """Prove that one controlled hang blocks both healthy peers in the legacy model."""

    clock = DeterministicClock()
    capture = CorrelationCapture(clock)
    hung_apply = ControllableBarrier(label="radio-a:apply", initially_released=False)
    radio_a = FakeEndpointAdapter("radio-a", capture=capture, apply=hung_apply)
    radio_b = FakeEndpointAdapter("radio-b", capture=capture)
    radio_c = FakeEndpointAdapter("radio-c", capture=capture)
    resources_before = ResourceCounter.snapshot(thread_prefix="mes0-global-control")
    harness = StationGlobalControlHarness(capture)
    try:
        first = harness.dispatch(radio_a, {"frequency_hz": 7_100_000}, "occurrence-a")
        if not hung_apply.started.wait(timeout=1.0):
            raise RuntimeError("radio-a did not enter its controlled apply barrier")
        second = harness.dispatch(radio_b, {"frequency_hz": 14_074_000}, "occurrence-b")
        third = harness.dispatch(radio_c, {"frequency_hz": 10_136_000}, "occurrence-c")
        radio_b_started = radio_b.apply_barrier.started.is_set()
        radio_c_started = radio_c.apply_barrier.started.is_set()
    finally:
        cleanup = harness.close([radio_a, radio_b, radio_c], cancel_in_flight=True)
    resources_after = ResourceCounter.snapshot(thread_prefix="mes0-global-control")
    return {
        "scenario": "station_global_blocking",
        "first_dispatch": first,
        "healthy_peer_dispatches": {"radio-b": second, "radio-c": third},
        "healthy_peer_apply_started_while_hung": {"radio-b": radio_b_started, "radio-c": radio_c_started},
        "defect_reproduced": first == "accepted" and second == "blocked" and third == "blocked" and not radio_b_started and not radio_c_started,
        "events": capture.as_dicts(),
        "dispatch_decision_timing": harness.dispatch_timing_summary(),
        "cleanup": cleanup,
        "resources_before": resources_before.as_dict(),
        "resources_after": resources_after.as_dict(),
        "resources_stable": ResourceCounter.stable(resources_before, resources_after),
    }


def run_mes0_baseline() -> Dict[str, Any]:
    """Return both required MES-0 characterizations in one bounded report."""

    single_radio = run_single_radio_baseline()
    three_radio_blocking = run_station_global_blocking_baseline()
    return {
        "baseline_version": 1,
        "single_radio": single_radio,
        "three_radio_global_blocking": three_radio_blocking,
        "all_checks_passed": bool(
            single_radio["success_reproduced"]
            and single_radio["resources_stable"]
            and single_radio["cleanup"]["executor_shutdown"]
            and single_radio["cleanup"]["future_done_after_shutdown"]
            and three_radio_blocking["defect_reproduced"]
            and three_radio_blocking["resources_stable"]
            and three_radio_blocking["cleanup"]["executor_shutdown"]
            and three_radio_blocking["cleanup"]["future_done_after_shutdown"]
        ),
    }
