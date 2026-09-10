#!/usr/bin/env python3
"""Run the MIP-5 message-ingest qualification soak.

The default run is a 30-minute, hardware-free real-time qualification.  Use
``--accelerated`` for a short preflight in CI or before a production run.  The
fixture is temporary unless ``--db-path`` is supplied; no production database
is opened by default.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from typing import Optional

from freqinout.core.message_projection_coordinator import (
    MessageProjectionCoordinator,
    reconcile_native_source_changes,
)
from freqinout.core.message_projection_maintenance import MessageProjectionMaintenanceService
from freqinout.core.message_projection_queue import ensure_source_dirty_triggers, queue_diagnostics
from freqinout.core.message_projection_store import ensure_message_projection_schema
from freqinout.core.message_projection_writer import close_projection_writers
from freqinout.core.perf_metrics import configure_perf_metrics_sink, shutdown_perf_metrics
from freqinout.core.scheduler_coordination import EndpointKey
from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry


DEFAULT_DURATION_SEC = 30.0 * 60.0


@dataclass(frozen=True)
class SoakOptions:
    duration_sec: float = DEFAULT_DURATION_SEC
    accelerated: bool = False
    burst_count: int = 500
    cycle_interval_sec: float = 1.0
    db_path: Optional[Path] = None

    def __post_init__(self) -> None:
        if self.duration_sec <= 0 or self.burst_count <= 0 or self.cycle_interval_sec <= 0:
            raise ValueError("duration, burst count, and cycle interval must be positive")


@dataclass(frozen=True)
class SoakResult:
    accelerated: bool
    duration_sec: float
    burst_count: int
    projection_committed: int
    projection_rows: int
    projection_cycles: int
    idle_checks: int
    idle_discoveries: int
    scheduler_commands: int
    scheduler_completions: int
    scheduler_failures: int
    queue_depth_after_shutdown: int
    endpoint_threads_after_shutdown: tuple[str, ...]
    rss_start_bytes: int
    rss_end_bytes: int
    rss_growth_bytes: int
    descriptors_start: int
    descriptors_end: int
    thread_count_start: int
    thread_count_end: int
    max_cpu_percent: float
    max_sustained_high_cpu_sec: float
    prepare_batch_p95_ms: float
    unchanged_reconcile_p95_ms: float
    max_projection_transaction_ms: float
    elapsed_wall_sec: float

    @property
    def passed(self) -> bool:
        return bool(
            self.projection_committed >= self.burst_count
            and self.projection_rows == self.burst_count
            and self.idle_discoveries == 0
            and self.scheduler_commands == self.scheduler_completions
            and self.scheduler_failures == 0
            and self.queue_depth_after_shutdown == 0
            and not self.endpoint_threads_after_shutdown
            and self.rss_growth_bytes <= 32 * 1024 * 1024
            and self.descriptors_end <= self.descriptors_start + 4
            and self.thread_count_end <= self.thread_count_start
            and (self.accelerated or self.max_sustained_high_cpu_sec <= 10.0)
            and self.prepare_batch_p95_ms <= 25.0
            and self.unchanged_reconcile_p95_ms <= 25.0
            and self.max_projection_transaction_ms <= 100.0
        )

    def as_dict(self) -> dict[str, object]:
        result = asdict(self)
        result["passed"] = self.passed
        return result


def _create_fixture(path: Path, count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE js8_messages (
                id INTEGER PRIMARY KEY, from_call TEXT, to_call TEXT,
                msg_type TEXT, utc_str TEXT, utc_ts REAL, raw_text TEXT,
                decoded_text TEXT, state TEXT, read_ts REAL, flag_state INTEGER,
                source_key TEXT, source_id INTEGER, source_radio_id INTEGER,
                js8_instance_id TEXT, source_path TEXT
            )
            """
        )
        ensure_message_projection_schema(conn)
        ensure_source_dirty_triggers(conn)
        conn.executemany(
            """
            INSERT INTO js8_messages (
                id, from_call, to_call, msg_type, utc_str, utc_ts,
                raw_text, decoded_text, state, read_ts, flag_state,
                source_key, source_id, source_radio_id, js8_instance_id, source_path
            ) VALUES (?, ?, '@MR08', 'MSG', ?, ?, ?, ?, 'NEW', 0, 0, 'mip5', ?, 1, 'mip5', '')
            """,
            [
                (
                    index,
                    f"N0C{index:04d}",
                    f"2026-09-10T12:{index % 60:02d}:00+00:00",
                    float(index),
                    f"message {index}",
                    f"message {index}",
                    index,
                )
                for index in range(1, count + 1)
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _run_scheduler_probe(registry: EndpointLaneRegistry, endpoint: EndpointKey) -> bool:
    done = threading.Event()
    failed = []

    def complete(result) -> None:
        if result.status not in {"succeeded", "applied_unverified"}:
            failed.append(result.status)
        done.set()

    submission = registry.submit(
        endpoint,
        occurrence_id=f"mip5-{time.monotonic_ns()}",
        operation=lambda: "healthy",
        completion=complete,
        timeout_s=1.0,
    )
    return bool(submission.accepted and done.wait(1.0) and not failed)


def _process_resource_snapshot() -> tuple[int, int, int, float]:
    """Return RSS, descriptors, threads, and point CPU from this process."""

    try:
        import psutil

        process = psutil.Process(os.getpid())
        descriptors = (
            int(process.num_fds())
            if hasattr(process, "num_fds")
            else int(process.num_handles())
        )
        return (
            int(process.memory_info().rss),
            descriptors,
            int(process.num_threads()),
            float(process.cpu_percent(interval=None)),
        )
    except Exception:
        return (0, 0, len(threading.enumerate()), 0.0)


def _percentile_95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(max(0.0, float(value)) for value in values)
    index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
    return ordered[index]


def _metric_samples(path: Path, name: str) -> list[float]:
    samples: list[float] = []
    if not path.exists():
        return samples
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "PERF|" not in line:
            continue
        try:
            payload = json.loads(line.split("PERF|", 1)[1])
        except Exception:
            continue
        if str(payload.get("name", "")) == name:
            samples.append(float(payload.get("ms", 0.0) or 0.0))
    return samples


def run_soak(options: SoakOptions = SoakOptions()) -> SoakResult:
    temp_dir = tempfile.TemporaryDirectory() if options.db_path is None else None
    db_path = options.db_path or Path(temp_dir.name) / "mip5-soak.sqlite"  # type: ignore[union-attr]
    started = time.perf_counter()
    _create_fixture(Path(db_path), options.burst_count)
    metrics_path = Path(db_path).with_suffix(".perf_metrics.log")
    configure_perf_metrics_sink(lambda: metrics_path)
    coordinator = MessageProjectionCoordinator(db_path)
    maintenance = MessageProjectionMaintenanceService(
        db_path, coordinator=coordinator, yield_seconds=0.01
    )
    scheduler = EndpointLaneRegistry()
    endpoint = EndpointKey.network("rigctld", "127.0.0.1", 4532, target="mip5")
    committed = 0
    cycles = 0
    idle_checks = 0
    idle_discoveries = 0
    scheduler_commands = 0
    scheduler_completions = 0
    scheduler_failures = 0
    rss_start = rss_end = descriptors_start = descriptors_end = 0
    thread_count_start = thread_count_end = 0
    max_cpu_percent = 0.0
    high_cpu_started = 0.0
    max_high_cpu_sec = 0.0
    reconcile_samples: list[float] = []
    try:
        catchup = maintenance.run_post_shell_catchup()
        committed = int(catchup.committed)
        cycles = int(catchup.cycles)

        rss_start, descriptors_start, thread_count_start, _ = _process_resource_snapshot()
        probe_cycles = 4 if options.accelerated else max(1, int(options.duration_sec / options.cycle_interval_sec))
        deadline = time.monotonic() + options.duration_sec
        for _ in range(probe_cycles):
            if not options.accelerated and time.monotonic() >= deadline:
                break
            reconcile_started = time.perf_counter()
            idle = reconcile_native_source_changes(db_path, sources=("js8",), limit_per_source=100)
            reconcile_samples.append((time.perf_counter() - reconcile_started) * 1000.0)
            idle_checks += 1
            idle_discoveries += sum(idle.values())
            scheduler_commands += 1
            if _run_scheduler_probe(scheduler, endpoint):
                scheduler_completions += 1
            else:
                scheduler_failures += 1
            if not options.accelerated:
                time.sleep(options.cycle_interval_sec)
            _rss, _descriptors, _threads, cpu_percent = _process_resource_snapshot()
            max_cpu_percent = max(max_cpu_percent, cpu_percent)
            if cpu_percent >= 100.0:
                if not high_cpu_started:
                    high_cpu_started = time.monotonic()
                max_high_cpu_sec = max(max_high_cpu_sec, time.monotonic() - high_cpu_started)
            else:
                high_cpu_started = 0.0
        conn = sqlite3.connect(db_path)
        try:
            projection_rows = int(conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0])
        finally:
            conn.close()
    finally:
        scheduler.shutdown(wait=True)
        maintenance.close(wait=True)
        close_projection_writers()
        shutdown_perf_metrics(timeout=2.0)
    prepare_samples = _metric_samples(metrics_path, "messages.prepare_batch")
    endpoint_threads = tuple(
        sorted(thread.name for thread in threading.enumerate() if thread.name.startswith("freqinout-endpoint-"))
    )
    queue_depth = int(queue_diagnostics(db_path)["depth"])
    rss_end, descriptors_end, thread_count_end, _ = _process_resource_snapshot()
    result = SoakResult(
        accelerated=options.accelerated,
        duration_sec=options.duration_sec,
        burst_count=options.burst_count,
        projection_committed=committed,
        projection_rows=projection_rows,
        projection_cycles=cycles,
        idle_checks=idle_checks,
        idle_discoveries=idle_discoveries,
        scheduler_commands=scheduler_commands,
        scheduler_completions=scheduler_completions,
        scheduler_failures=scheduler_failures,
        queue_depth_after_shutdown=queue_depth,
        endpoint_threads_after_shutdown=endpoint_threads,
        rss_start_bytes=rss_start,
        rss_end_bytes=rss_end,
        rss_growth_bytes=max(0, rss_end - rss_start),
        descriptors_start=descriptors_start,
        descriptors_end=descriptors_end,
        thread_count_start=thread_count_start,
        thread_count_end=thread_count_end,
        max_cpu_percent=round(max_cpu_percent, 3),
        max_sustained_high_cpu_sec=round(max_high_cpu_sec, 3),
        prepare_batch_p95_ms=round(_percentile_95(prepare_samples), 3),
        unchanged_reconcile_p95_ms=round(_percentile_95(reconcile_samples), 3),
        max_projection_transaction_ms=round(float(catchup.max_transaction_ms or 0.0), 3),
        elapsed_wall_sec=time.perf_counter() - started,
    )
    if temp_dir is not None:
        temp_dir.cleanup()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accelerated", action="store_true", help="run four immediate preflight cycles")
    parser.add_argument("--duration-sec", type=float, default=DEFAULT_DURATION_SEC, help="real-time duration (default: 1800)")
    parser.add_argument("--cycle-interval-sec", type=float, default=1.0)
    parser.add_argument("--burst-count", type=int, default=500)
    parser.add_argument("--db-path", type=Path, help="optional disposable qualification database path")
    parser.add_argument("--json-out", type=Path, help="write the result JSON to this path")
    args = parser.parse_args()
    result = run_soak(
        SoakOptions(
            accelerated=bool(args.accelerated),
            duration_sec=float(args.duration_sec),
            cycle_interval_sec=float(args.cycle_interval_sec),
            burst_count=int(args.burst_count),
            db_path=args.db_path,
        )
    )
    payload = json.dumps(result.as_dict(), indent=2, sort_keys=True)
    print(payload)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(payload + "\n", encoding="utf-8")
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
