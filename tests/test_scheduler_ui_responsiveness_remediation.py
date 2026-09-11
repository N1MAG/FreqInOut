"""Release-blocking scheduler/UI responsiveness regression coverage.

The scheduler timer is a Qt-thread boundary.  It may dispatch work and consume
immutable cached state, but it must not enter schedule/database projection or
other potentially blocking probes inline.  These tests deliberately avoid a
real Qt event loop, wall-clock sleeps, sockets, and production databases.

Message projection/catch-up acceptance remains owned by the message ingest
test package; this module only covers the scheduler-side release gate.
"""

from __future__ import annotations

import ast
import concurrent.futures
import inspect
import textwrap
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, List, Tuple

from freqinout.core.scheduler_endpoint_status import EndpointStatusRegistry
from freqinout.core.scheduler_coordination import EndpointKey
from freqinout.core.scheduler_engine import SchedulerEngine


def _called_method_names(function: Callable[..., object]) -> set[str]:
    """Return direct ``self.method(...)`` calls from a function's AST."""

    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    return {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "self"
    }


def test_scheduler_timer_is_a_nonblocking_dispatch_boundary() -> None:
    """The Qt timer must not call schedule projection/evaluation inline.

    This is intentionally a narrow architecture guard: the implementation
    may choose any worker/coordinator name, but the timer cannot directly enter
    methods that load SQLite-backed schedules or perform endpoint application.
    """

    source = inspect.getsource(SchedulerEngine._on_timer)
    tree = ast.parse(textwrap.dedent(source))
    called = _called_method_names(SchedulerEngine._on_timer)

    # Keep forbidden work out of the timer body itself as well as out of a
    # future direct helper accidentally added to it.
    forbidden_tokens = {
        "sqlite3",
        "MultiRadioStore",
        "SettingsManager",
        "process_iter",
        "subprocess",
        "Popen",
        "os.walk",
        "rglob",
        "connect_sqlite",
    }
    identifiers = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
    }
    attributes = {
        "." + node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
    }
    source_tokens = source + " " + " ".join(sorted(identifiers | attributes))
    assert not any(token in source_tokens for token in forbidden_tokens)

    # These methods are the old synchronous path: both reach SQLite-backed
    # schedule projection and/or endpoint application.  A timer callback may
    # request their worker equivalent, but must not invoke them itself.
    assert "_evaluate" not in called
    assert "_apply_active_schedule_lanes" not in called


def test_schedule_projection_worker_never_borrows_ui_settings_manager() -> None:
    """The DB projection lane receives a plain settings snapshot."""

    source = inspect.getsource(SchedulerEngine._load_active_schedule_lane_rows)
    assert "self.settings." not in source
    assert "settings_snapshot" in source


def test_fldigi_availability_read_is_cache_only_unless_worker_requests_live() -> None:
    """Status rendering cannot turn a cache miss into endpoint I/O."""

    class _ExplodingRig:
        def is_fldigi_available(self) -> bool:
            raise AssertionError("UI/cache-only caller probed FLDigi")

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = _ExplodingRig()
    scheduler._fldigi_available_cache = None
    scheduler._fldigi_available_ts = 0.0

    assert SchedulerEngine._fldigi_available(scheduler) is False


def test_scheduler_constructor_contains_no_inline_endpoint_availability_probe() -> None:
    """Creating the scheduler must not contact a radio before first paint."""

    source = inspect.getsource(SchedulerEngine.__init__)
    assert "rig.is_available()" not in source


def test_scheduler_schedule_reads_are_reused_until_snapshot_identity_changes() -> None:
    """Repeated schedule refreshes reuse the bounded immutable read cache."""

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler._schedule_cache = None
    scheduler.settings = SimpleNamespace(
        all=lambda: {
            "hf_schedule": [{"frequency": "14.115", "start_utc": "00:00", "end_utc": "23:59"}],
            "net_schedule": [],
        }
    )
    scheduler._config_dir = lambda: Path("/tmp/freqinout-test-profile")
    scheduler._db_mtime = lambda _path: 17.0
    scheduler._primary_schedule_target_context = lambda: (None, None)
    scheduler._sop_layer_enabled = lambda: False
    scheduler._load_assigned_frequency_plan_schedule_rows = lambda _profile_id: ([], [], False)

    calls = {"daily": 0, "net": 0, "sop": 0, "policy": 0}

    def daily():
        calls["daily"] += 1
        return None

    def net():
        calls["net"] += 1
        return None

    def sop():
        calls["sop"] += 1
        return None

    def policy():
        calls["policy"] += 1
        return None

    scheduler._load_daily_schedule_from_db = daily
    scheduler._load_net_schedule_from_db = net
    scheduler._load_sop_schedule_layer_from_db = sop
    scheduler._load_sop_net_conflict_policies_from_db = policy

    first = SchedulerEngine._load_schedules(scheduler)
    second = SchedulerEngine._load_schedules(scheduler)

    assert second == first
    assert calls == {"daily": 1, "net": 1, "sop": 1, "policy": 1}

    # A changed DB identity invalidates the read snapshot and permits exactly
    # one replacement load; this is the bounded refresh exit gate.
    scheduler._db_mtime = lambda _path: 18.0
    SchedulerEngine._load_schedules(scheduler)
    assert calls == {"daily": 2, "net": 2, "sop": 2, "policy": 2}


class _ManualExecutor:
    """Deterministic executor fake; queued callables run only when released."""

    def __init__(self, **_kwargs: object) -> None:
        self.pending: List[Tuple[Callable[[], object], concurrent.futures.Future]] = []
        self.closed = False

    def submit(self, function: Callable[[], object]) -> concurrent.futures.Future:
        future: concurrent.futures.Future = concurrent.futures.Future()
        self.pending.append((function, future))
        return future

    def shutdown(self, **_kwargs: object) -> None:
        self.closed = True
        for _function, future in self.pending:
            future.cancel()
        self.pending.clear()

    def run_endpoint(self, index: int) -> None:
        function, future = self.pending.pop(index)
        try:
            future.set_result(function())
        except BaseException as exc:  # pragma: no cover - exercised by registry
            future.set_exception(exc)


def _status_key(port: int) -> EndpointKey:
    return EndpointKey.network("flrig", "127.0.0.1", port)


def test_slow_status_refresh_is_singleflight_and_does_not_delay_peer_callback() -> None:
    """A pending slow endpoint cannot block a repeated timer/status request."""

    executor = _ManualExecutor()
    registry = EndpointStatusRegistry(
        executor_factory=lambda **kwargs: executor,
        monotonic_fn=lambda: 100.0,
        ttl_s=5.0,
    )
    endpoint_a = _status_key(12345)
    endpoint_b = _status_key(12346)
    completed: list[object] = []

    try:
        first = registry.request(
            endpoint_a,
            poller=lambda: {"frequency_hz": 7_100_000},
            completion=completed.append,
        )
        repeated = registry.request(
            endpoint_a,
            poller=lambda: {"frequency_hz": 7_100_000},
            completion=completed.append,
        )
        assert first.disposition == "started"
        assert repeated.disposition == "singleflight"
        assert len(executor.pending) == 1

        # The second endpoint is independently dispatchable while A remains
        # pending.  No sleep or timing assumption is needed.
        peer = registry.request(
            endpoint_b,
            poller=lambda: {"frequency_hz": 14_115_000},
            completion=completed.append,
        )
        assert peer.disposition == "started"
        assert len(executor.pending) == 2
        executor.run_endpoint(1)
        assert len(completed) == 1
        assert registry.latest(endpoint_b).frequency_hz == 14_115_000
        assert registry.latest(endpoint_a).inflight is True

        metrics = registry.metrics_snapshot().as_dict()
        assert metrics["polls_started"] == 2
        assert metrics["singleflight_hits"] == 1
    finally:
        registry.shutdown()
