"""MES-5 deterministic lifecycle and reconfiguration acceptance tests.

The tests use endpoint lanes, immutable schedule timing, and a scheduler engine
with external work suppressed.  Events fence every worker transition; no test
uses a sleep to make progress appear complete.
"""

from __future__ import annotations

import datetime
import os
import subprocess
import sys
import threading
import textwrap
from typing import Dict, List

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QCoreApplication, QEvent

from freqinout.core.scheduler_coordination import EndpointKey, EndpointResult
from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry
from freqinout.core.scheduler_endpoint_status import EndpointStatusRegistry
from freqinout.core.scheduler_engine import SchedulerEngine, compute_next_change_time
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_runtime_manager import DeviceRuntime, endpoint_operational_health


def _key(port: int, *, family: str = "rigctld", target: str = "") -> EndpointKey:
    return EndpointKey.network(family, "127.0.0.1", port, target=target)


def _shutdown_engine(engine: SchedulerEngine) -> None:
    engine.stop()
    app = QCoreApplication.instance()
    if app is not None:
        engine.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


def _isolated_engine(monkeypatch, tmp_path) -> SchedulerEngine:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    engine = SchedulerEngine(poll_interval_ms=60_000)
    # Startup lifecycle is tested here; endpoint discovery and schedule I/O
    # are separately covered by scheduler integration tests.
    engine._maybe_refresh_external_status_snapshot = lambda **_kwargs: None
    engine._apply_js8_offset_startup = lambda: None
    engine._clear_startup_manual_qsy_states = lambda: None
    engine._apply_active_schedule_lanes = lambda **_kwargs: False
    engine._evaluate = lambda **_kwargs: None
    return engine


def test_mes5_repeated_start_stop_cycles_recreate_owned_state_without_growth(monkeypatch, tmp_path) -> None:
    app = QCoreApplication.instance() or QCoreApplication([])
    engine = _isolated_engine(monkeypatch, tmp_path)
    try:
        for _cycle in range(25):
            engine.start()
            assert engine.timer.isActive()
            completed = threading.Event()
            key = _key(55_00)
            assert engine._endpoint_lanes.submit(
                key,
                occurrence_id=f"cycle-{_cycle}",
                operation=lambda: {"ok": True},
                completion=lambda _result: completed.set(),
            ).accepted
            assert completed.wait(1.0)
            engine.stop()
            assert not engine.timer.isActive()
            assert engine._shutdown_requested is True
            assert engine._endpoint_lanes.snapshots()[0].closed is True
            assert len(engine._endpoint_status.snapshots()) == 0
        assert not any(
            thread.name.startswith(("freqinout-endpoint-", "freqinout-status"))
            for thread in threading.enumerate()
        )
    finally:
        _shutdown_engine(engine)
        app.processEvents()


def test_mes5_edit_remove_and_disable_fence_old_lane_completion() -> None:
    old_key = _key(56_00)
    edited_key = _key(56_01)
    old_started = threading.Event()
    release_old = threading.Event()
    edited_done = threading.Event()
    stale_callbacks: List[EndpointResult] = []
    edited_callbacks: List[EndpointResult] = []
    registry = EndpointLaneRegistry(base_backoff_s=0.0)

    def old_operation():
        old_started.set()
        release_old.wait(2.0)
        return {"ok": True, "actual_state": {"frequency_hz": 7_056_000}}

    try:
        assert registry.submit(
            old_key,
            occurrence_id="before-edit",
            operation=old_operation,
            completion=stale_callbacks.append,
        ).accepted
        assert old_started.wait(1.0)
        old_lane = registry.lane(old_key)
        assert old_lane is not None and old_lane.future is not None

        # Editing the route retires the old lane.  This also models disable:
        # after removal no old lane remains eligible to accept work.
        assert registry.remove(old_key)
        assert registry.lane(old_key) is None
        assert all(snapshot.endpoint_key != old_key for snapshot in registry.snapshots())

        assert registry.submit(
            edited_key,
            occurrence_id="after-edit",
            operation=lambda: {"ok": True, "actual_state": {"frequency_hz": 14_056_000}},
            completion=lambda result: (edited_callbacks.append(result), edited_done.set()),
        ).accepted
        assert edited_done.wait(1.0)
        assert edited_callbacks[0].status == "applied_unverified"

        release_old.set()
        old_lane.future.result(timeout=1.0)
        assert stale_callbacks == []
    finally:
        release_old.set()
        registry.shutdown(wait=True)


def test_mes5_wall_clock_jump_recomputes_current_transition_without_replay() -> None:
    hf = {"start_utc": "10:00", "end_utc": "11:00"}
    net = {"start_utc": "12:00", "end_utc": "13:00"}
    before = datetime.datetime(2026, 9, 10, 9, 30, tzinfo=datetime.timezone.utc)
    after_forward_jump = datetime.datetime(2026, 9, 10, 12, 30, tzinfo=datetime.timezone.utc)
    after_backward_jump = datetime.datetime(2026, 9, 10, 9, 45, tzinfo=datetime.timezone.utc)

    assert compute_next_change_time(before, hf, net) == datetime.datetime(
        2026, 9, 10, 10, 0, tzinfo=datetime.timezone.utc
    )
    # A wake/forward correction selects the currently valid NET boundary; it
    # does not replay the skipped 10:00 HF start or 11:00 HF end.
    assert compute_next_change_time(after_forward_jump, hf, net) == datetime.datetime(
        2026, 9, 10, 13, 0, tzinfo=datetime.timezone.utc
    )
    # A backward correction recomputes from authoritative UTC rather than
    # retaining the forward-jump result.
    assert compute_next_change_time(after_backward_jump, hf, net) == datetime.datetime(
        2026, 9, 10, 10, 0, tzinfo=datetime.timezone.utc
    )


def test_mes5_ordinary_application_resume_preserves_usable_endpoint_state(monkeypatch, tmp_path) -> None:
    """A normal focus return must not discard a matching readback pair."""

    engine = _isolated_engine(monkeypatch, tmp_path)
    now_utc = datetime.datetime(2026, 9, 10, 15, 30, tzinfo=datetime.timezone.utc)
    endpoint = _key(57_02)
    expected = {
        "frequency_hz": 14_078_000,
        "control_mode": "RIGCTLD",
        "vfo": "A",
    }
    applied = ((1, "20M", 14_078_000), "HF")
    engine._expected_state_by_endpoint = {endpoint.canonical: dict(expected)}
    engine._last_applied_by_endpoint = {endpoint.canonical: applied}
    before_status = engine._endpoint_status.publish(
        endpoint,
        {"frequency_hz": 14_078_000, "ptt_known": True, "ptt_active": False, "vfo": "A"},
        source="ordinary_poll",
    )
    engine._utc_now = lambda: now_utc
    engine._monotonic_clock = lambda: 102.0
    engine._last_lifecycle_monotonic = 100.0
    engine._last_lifecycle_utc = now_utc - datetime.timedelta(seconds=2)

    try:
        engine.handle_resume()

        assert engine._expected_state_by_endpoint == {endpoint.canonical: expected}
        assert engine._last_applied_by_endpoint == {endpoint.canonical: applied}
        after_status = engine._endpoint_status.latest(endpoint)
        assert after_status.generation == before_status.generation
        assert after_status.frequency_hz == 14_078_000
        assert after_status.invalidated is False
        assert after_status.closed is False
    finally:
        _shutdown_engine(engine)


def test_mes5_unavailable_endpoint_is_isolated_from_healthy_peer() -> None:
    unavailable = _key(57_00, family="sdrpp", target="vfo-a")
    healthy = _key(57_01)
    unavailable_done = threading.Event()
    healthy_done = threading.Event()
    results: Dict[EndpointKey, EndpointResult] = {}
    registry = EndpointLaneRegistry(base_backoff_s=0.0)

    def record(result: EndpointResult, key: EndpointKey, event: threading.Event) -> None:
        results[key] = result
        event.set()

    try:
        assert registry.submit(
            unavailable,
            occurrence_id="startup-unavailable",
            operation=lambda: {"ok": False, "reason_code": "endpoint_unavailable"},
            completion=lambda result: record(result, unavailable, unavailable_done),
        ).accepted
        assert registry.submit(
            healthy,
            occurrence_id="startup-healthy",
            operation=lambda: {"ok": True, "actual_state": {"frequency_hz": 14_057_000}},
            completion=lambda result: record(result, healthy, healthy_done),
        ).accepted

        assert unavailable_done.wait(1.0)
        assert healthy_done.wait(1.0)
        assert results[unavailable].status == "failed"
        assert results[healthy].status == "applied_unverified"
        snapshots = {snapshot.endpoint_key: snapshot for snapshot in registry.snapshots()}
        assert snapshots[unavailable].failure_count == 1
        assert snapshots[healthy].failure_count == 0
    finally:
        registry.shutdown(wait=True)


def test_mes5_shutdown_during_inflight_endpoint_is_nonblocking_and_fenced() -> None:
    started = threading.Event()
    release = threading.Event()
    shutdown_returned = threading.Event()
    callbacks: List[EndpointResult] = []
    key = _key(58_00, family="sdrconnect", target="vfo-a")
    registry = EndpointLaneRegistry()

    def blocked_operation():
        started.set()
        release.wait(2.0)
        return {"ok": True}

    try:
        assert registry.submit(
            key,
            occurrence_id="shutdown-inflight",
            operation=blocked_operation,
            completion=callbacks.append,
        ).accepted
        assert started.wait(1.0)

        def stop_registry() -> None:
            registry.shutdown(wait=False)
            shutdown_returned.set()

        stopper = threading.Thread(target=stop_registry)
        stopper.start()
        assert shutdown_returned.wait(1.0)
        stopper.join(1.0)
        assert not stopper.is_alive()
        assert registry.snapshots()[0].closed is True

        release.set()
        lane = registry.lane(key)
        assert lane is not None and lane.future is not None
        lane.future.result(timeout=1.0)
        assert callbacks == []
    finally:
        release.set()
        registry.shutdown(wait=True)


def test_mes5_shutdown_during_endpoint_backoff_is_bounded() -> None:
    failed = threading.Event()
    key = _key(58_01)
    registry = EndpointLaneRegistry(base_backoff_s=60.0)
    try:
        assert registry.submit(
            key,
            occurrence_id="backoff",
            operation=lambda: {"ok": False, "reason_code": "unavailable"},
            completion=lambda _result: failed.set(),
        ).accepted
        assert failed.wait(1.0)
        # A retry queued while the failure backoff is active is the state that
        # shutdown must fence without waiting for the backoff interval.
        assert registry.submit(
            key,
            occurrence_id="backoff-retry",
            operation=lambda: {"ok": True},
            completion=lambda _result: None,
        ).disposition == "backoff"
        assert registry.lane(key).snapshot().state == "backoff"

        returned = threading.Event()

        def stop_registry() -> None:
            registry.shutdown(wait=False)
            returned.set()

        stopper = threading.Thread(target=stop_registry)
        stopper.start()
        assert returned.wait(1.0)
        stopper.join(1.0)
        assert not stopper.is_alive()
        assert registry.snapshots()[0].closed is True
    finally:
        registry.shutdown(wait=True)


def test_mes5_shutdown_during_connect_operation_is_bounded_and_fenced() -> None:
    started = threading.Event()
    release = threading.Event()
    callbacks: List[EndpointResult] = []
    key = _key(58_02)
    registry = EndpointLaneRegistry()

    def connect_to_endpoint():
        started.set()
        release.wait(2.0)
        return {"ok": True, "actual_state": {"connected": True}}

    try:
        assert registry.submit(
            key,
            occurrence_id="connect-inflight",
            operation=connect_to_endpoint,
            completion=callbacks.append,
        ).accepted
        assert started.wait(1.0)
        returned = threading.Event()

        def stop_registry() -> None:
            registry.shutdown(wait=False)
            returned.set()

        stopper = threading.Thread(target=stop_registry)
        stopper.start()
        assert returned.wait(1.0)
        stopper.join(1.0)
        assert not stopper.is_alive()
        release.set()
        lane = registry.lane(key)
        assert lane is not None and lane.future is not None
        lane.future.result(timeout=1.0)
        assert callbacks == []
    finally:
        release.set()
        registry.shutdown(wait=True)


def test_mes5_permanent_hung_peer_does_not_hold_process_exit() -> None:
    """A contract-violating peer is isolated in a daemon lane at process exit."""

    child = textwrap.dedent(
        """
        import threading

        from freqinout.core.scheduler_coordination import EndpointKey
        from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry

        started = threading.Event()
        never_release = threading.Event()
        key = EndpointKey.network("rigctld", "127.0.0.1", 58123)
        registry = EndpointLaneRegistry()

        def permanently_hung_peer():
            started.set()
            never_release.wait()

        assert registry.submit(
            key,
            occurrence_id="permanent-hung-peer",
            operation=permanently_hung_peer,
            completion=lambda _result: None,
        ).accepted
        assert started.wait(1.0)
        registry.shutdown(wait=False, join_timeout_s=0.01)
        assert registry.last_shutdown_survivors == (key.safe_label,)
        print("bounded shutdown", flush=True)
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", child],
        cwd=os.path.dirname(os.path.dirname(__file__)),
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=5.0,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "bounded shutdown" in completed.stdout


def test_mes5_engine_stop_closes_endpoint_lanes_without_waiting_for_adapter(monkeypatch, tmp_path) -> None:
    engine = _isolated_engine(monkeypatch, tmp_path)
    started = threading.Event()
    release = threading.Event()
    callback_results: List[EndpointResult] = []
    key = _key(59_00, family="sdrpp", target="vfo-a")

    def blocked_operation():
        started.set()
        release.wait(2.0)
        return {"ok": True}

    try:
        assert engine._endpoint_lanes.submit(
            key,
            occurrence_id="engine-stop",
            operation=blocked_operation,
            completion=callback_results.append,
        ).accepted
        assert started.wait(1.0)
        engine.stop()
        snapshot = engine._endpoint_lanes.snapshots()[0]
        assert snapshot.closed is True
        release.set()
        lane = engine._endpoint_lanes.lane(key)
        assert lane is not None and lane.future is not None
        lane.future.result(timeout=1.0)
        assert callback_results == []
    finally:
        release.set()
        _shutdown_engine(engine)


class _BlockedRig:
    host = "127.0.0.1"
    port = 60_00

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def set_frequency(self, _command) -> bool:
        self.started.set()
        self.release.wait(2.0)
        return True


def test_mes5_engine_reconfiguration_retires_only_changed_route(monkeypatch, tmp_path) -> None:
    engine = _isolated_engine(monkeypatch, tmp_path)
    engine._queue_scheduler_thread_call = lambda callback: callback()
    verification_calls: List[dict] = []
    events: List[tuple] = []
    engine._queue_post_apply_verification = lambda **kwargs: verification_calls.append(kwargs)
    engine._record_scheduler_event = lambda *args, **kwargs: events.append((args, kwargs))
    engine._record_scheduler_health_issue = lambda *_args, **_kwargs: None
    engine._clear_scheduler_health_issue = lambda *_args, **_kwargs: None
    engine._clear_fldigi_busy_check_state = lambda: None
    old_key = _key(60_00)
    peer_key = _key(60_01)
    edited_key = _key(60_02)
    old_profile = {"id": 1, "name": "Old", "control_backend": "rigctld", "rig_host": "127.0.0.1", "rig_port": 60_00}
    peer_profile = {"id": 2, "name": "Peer", "control_backend": "rigctld", "rig_host": "127.0.0.1", "rig_port": 60_01}
    edited_profile = dict(old_profile, name="Edited", rig_port=60_02)
    rig = _BlockedRig()
    try:
        engine._reconcile_endpoint_configuration(
            keys_by_profile={1: old_key, 2: peer_key},
            profiles_by_id={1: old_profile, 2: peer_profile},
        )
        peer_done = threading.Event()
        assert engine._endpoint_lanes.submit(
            peer_key,
            occurrence_id="peer",
            operation=lambda: {"ok": True},
            completion=lambda _result: peer_done.set(),
        ).accepted
        assert peer_done.wait(1.0)
        assert engine._queue_control_action(
            control_mode="RIGCTLD",
            rig_client=rig,
            js8_client=None,
            endpoint_key=old_key,
            device_profile_id=1,
            allow_global_fallback=False,
            entry_key=(1, "20M", 14_115_000),
            source="HF",
            freq_hz=14_115_000,
            band="20M",
            mode="USB",
            vfo="A",
            auto_tune=False,
            js8_offset=None,
            js8_group="",
        )
        assert rig.started.wait(1.0)
        old_lane = engine._endpoint_lanes.lane(old_key)
        assert old_lane is not None and old_lane.future is not None
        peer_lane_before = engine._endpoint_lanes.lane(peer_key)
        # Ignore setup/reconciliation telemetry; only a late completion from
        # the retired route would be allowed to add work or publish an event.
        events.clear()
        engine._reconcile_endpoint_configuration(
            keys_by_profile={1: edited_key, 2: peer_key},
            profiles_by_id={1: edited_profile, 2: peer_profile},
        )
        assert engine._endpoint_lanes.lane(old_key) is None
        assert engine._endpoint_lanes.lane(peer_key) is peer_lane_before
        assert engine._endpoint_keys_by_profile == {1: edited_key, 2: peer_key}
        rig.release.set()
        old_lane.future.result(timeout=1.0)
        assert old_key.canonical not in engine._last_applied_by_endpoint
        assert events == []
        assert verification_calls == []
    finally:
        rig.release.set()
        _shutdown_engine(engine)


def test_mes5_clock_discontinuity_invalidates_assumptions_without_replaying(monkeypatch, tmp_path) -> None:
    monotonic = {"value": 100.0}
    wall = {"value": datetime.datetime(2026, 9, 10, 10, 0, tzinfo=datetime.timezone.utc)}
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "clock-profile"))
    SettingsManager()
    engine = SchedulerEngine(
        poll_interval_ms=5_000,
        monotonic_clock=lambda: monotonic["value"],
        utc_now=lambda: wall["value"],
    )
    events: List[str] = []
    engine._record_scheduler_event = lambda _status, reason, **_kwargs: events.append(reason)
    engine._last_lifecycle_monotonic = monotonic["value"]
    engine._last_lifecycle_utc = wall["value"]
    engine._last_applied_by_endpoint = {"route": ((1,), "HF")}
    try:
        monotonic["value"] += 5.0
        wall["value"] += datetime.timedelta(hours=2)
        reason = engine._observe_lifecycle_clock(
            now_utc=wall["value"],
            now_monotonic=monotonic["value"],
        )
        assert reason == "clock_jump_forward"
        assert engine._last_applied_by_endpoint == {}
        assert events == ["clock_jump_forward"]

        monotonic["value"] += 5.0
        wall["value"] -= datetime.timedelta(hours=1)
        assert engine._observe_lifecycle_clock(
            now_utc=wall["value"],
            now_monotonic=monotonic["value"],
        ) == "clock_jump_backward"
        assert events[-1] == "clock_jump_backward"
    finally:
        _shutdown_engine(engine)


def test_mes5_monotonic_reset_invalidates_assumptions(monkeypatch, tmp_path) -> None:
    engine = _isolated_engine(monkeypatch, tmp_path)
    previous_utc = datetime.datetime(2026, 9, 10, 10, 0, tzinfo=datetime.timezone.utc)
    events: List[str] = []
    engine._last_lifecycle_monotonic = 100.0
    engine._last_lifecycle_utc = previous_utc
    engine._last_applied_by_endpoint = {"route": ((1,), "HF")}
    engine._record_scheduler_event = lambda _status, reason, **_kwargs: events.append(reason)
    try:
        reason = engine._observe_lifecycle_clock(
            now_utc=previous_utc + datetime.timedelta(seconds=5),
            now_monotonic=50.0,
        )
        assert reason == "monotonic_clock_reset"
        assert engine._last_applied_by_endpoint == {}
        assert engine._last_lifecycle_event["reason_code"] == "monotonic_clock_reset"
        assert events == ["monotonic_clock_reset"]
    finally:
        _shutdown_engine(engine)


def test_mes5_resume_reoffers_only_current_intent_after_invalidating_cache(monkeypatch, tmp_path) -> None:
    engine = _isolated_engine(monkeypatch, tmp_path)
    now_utc = datetime.datetime(2026, 9, 10, 15, 30, tzinfo=datetime.timezone.utc)
    applied: List[dict] = []
    evaluated: List[dict] = []
    engine._utc_now = lambda: now_utc
    engine._monotonic_clock = lambda: 250.0
    engine._last_lifecycle_monotonic = 100.0
    engine._last_lifecycle_utc = now_utc - datetime.timedelta(hours=2)
    engine._last_applied_by_endpoint = {"expired-route": ((1,), "HF")}
    engine._expected_state_by_endpoint = {
        "expired-route": {"frequency_hz": 14_078_000, "control_mode": "RIGCTLD"}
    }
    engine._apply_active_schedule_lanes = lambda **kwargs: (applied.append(kwargs) or True)
    engine._evaluate = lambda **kwargs: evaluated.append(kwargs)
    try:
        engine.handle_resume()
        assert engine._last_applied_by_endpoint == {}
        assert engine._expected_state_by_endpoint == {}
        assert engine._last_lifecycle_event["reason_code"] == "application_resume"
        assert applied == [{"now_utc": now_utc, "force": False}]
        assert evaluated == []
    finally:
        _shutdown_engine(engine)


def test_mes5_lifecycle_recompute_fences_pre_resume_command_completion(monkeypatch, tmp_path) -> None:
    engine = _isolated_engine(monkeypatch, tmp_path)
    key = _key(60_50)
    started = threading.Event()
    release = threading.Event()
    stale_callbacks: List[EndpointResult] = []

    def pre_resume_operation():
        started.set()
        release.wait(2.0)
        return {"ok": True, "actual_state": {"frequency_hz": 7_078_000}}

    try:
        assert engine._endpoint_lanes.submit(
            key,
            occurrence_id="pre-resume",
            operation=pre_resume_operation,
            completion=stale_callbacks.append,
        ).accepted
        assert started.wait(1.0)
        old_lane = engine._endpoint_lanes.lane(key)
        assert old_lane is not None and old_lane.future is not None

        engine._record_scheduler_event = lambda *_args, **_kwargs: None
        engine._prepare_lifecycle_recompute(
            now_utc=datetime.datetime.now(datetime.timezone.utc),
            reason_code="application_resume",
        )
        assert engine._endpoint_lanes.lane(key) is None
        assert engine._last_lifecycle_event["retired_command_lane_count"] == 1

        release.set()
        old_lane.future.result(timeout=1.0)
        assert stale_callbacks == []
    finally:
        release.set()
        _shutdown_engine(engine)


def test_mes5_startup_probe_jitter_is_nonblocking_and_force_bypasses(monkeypatch, tmp_path) -> None:
    monotonic = {"value": 50.0}
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "jitter-profile"))
    SettingsManager()
    engine = SchedulerEngine(monotonic_clock=lambda: monotonic["value"])
    first = _key(61_00)
    second = _key(61_01)
    try:
        engine._startup_probe_jitter_enabled = True
        assert engine._startup_probe_ready(first, force=False) is False
        assert engine._startup_probe_ready(second, force=False) is False
        deadlines = dict(engine._startup_probe_not_before)
        assert deadlines[first.canonical] != deadlines[second.canonical]
        assert engine._startup_probe_ready(first, force=True) is True
        monotonic["value"] = max(deadlines.values()) + 0.01
        assert engine._startup_probe_ready(first, force=False) is True
        assert engine._startup_probe_ready(second, force=False) is True
    finally:
        _shutdown_engine(engine)


def test_mes5_runtime_card_snapshot_is_cache_only(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "card-profile"))
    settings = SettingsManager()
    runtime = DeviceRuntime(
        {
            "id": 9,
            "name": "Cache Only Rig",
            "device_class": "tx_rx",
            "control_backend": "rigctld",
            "rig_host": "127.0.0.1",
            "rig_port": 4532,
            "runtime_active": 1,
        },
        is_primary=True,
        fallback_settings=settings,
    )
    assert runtime.status_service is not None
    monkeypatch.setattr(
        runtime.status_service,
        "status_snapshot",
        lambda **_kwargs: pytest.fail("card snapshot performed a software endpoint probe"),
    )
    runtime.ptt_active = lambda **_kwargs: pytest.fail("card snapshot polled PTT")
    runtime.current_frequency_hz = lambda **_kwargs: pytest.fail("card snapshot polled frequency")
    if runtime.varac_status_client is not None:
        runtime.varac_status_client.get_status = lambda: pytest.fail("card snapshot polled VarAC")
    try:
        snapshot = runtime.snapshot(force=True, cache_only=True)
        assert snapshot.name == "Cache Only Rig"
        assert snapshot.control_ready is None
        assert snapshot.overall_state == "idle"
        assert snapshot.status_summary == "Checking control status"
        assert snapshot.current_frequency_hz is None
    finally:
        runtime.stop()


@pytest.mark.parametrize(
    ("state_code", "indicator_state", "control_ready"),
    [
        ("on_schedule_verified", "ok", True),
        ("js8_verification_unavailable", "ok", True),
        ("applying_schedule", "idle", None),
        ("waiting_shared_resource", "idle", None),
        ("verification_unavailable", "idle", None),
        ("control_stalled", "warn", False),
        ("endpoint_unavailable", "warn", False),
        ("receiver_unavailable", "warn", False),
        ("readback_mismatch", "warn", True),
    ],
)
def test_mes5_operational_health_uses_operational_impact_boundary(
    state_code: str,
    indicator_state: str,
    control_ready: bool | None,
) -> None:
    projection = endpoint_operational_health(
        {
            "state": state_code,
            "label": state_code,
            "detail": f"detail for {state_code}",
            "endpoint_label": "FLRig 127.0.0.1:12345",
        }
    )

    assert projection.indicator_state == indicator_state
    assert projection.control_ready is control_ready


def test_mes5_operational_health_classification_is_endpoint_isolated() -> None:
    healthy = endpoint_operational_health(
        {"state": "on_schedule_verified", "label": "On schedule · verified"}
    )
    stalled = endpoint_operational_health(
        {"state": "control_stalled", "label": "Control stalled · other radios unaffected"}
    )

    assert (healthy.indicator_state, healthy.control_ready) == ("ok", True)
    assert (stalled.indicator_state, stalled.control_ready) == ("warn", False)


def test_mes5_operational_summary_never_labels_stale_readback_verified(monkeypatch, tmp_path) -> None:
    monotonic = {"value": 100.0}
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "stale-summary-profile"))
    SettingsManager()
    engine = SchedulerEngine(monotonic_clock=lambda: monotonic["value"])
    key = _key(62_00)
    old_registry = engine._endpoint_status
    engine._endpoint_status = EndpointStatusRegistry(
        monotonic_clock=lambda: monotonic["value"],
    )
    old_registry.shutdown(wait=False, cancel_futures=True)
    engine._endpoint_keys_by_profile = {1: key}
    engine._expected_state_by_endpoint = {
        key.canonical: {"frequency_hz": 14_078_000, "control_mode": "RIGCTLD"}
    }
    engine._endpoint_status.publish(
        key,
        {"frequency_hz": 14_078_000, "ptt_known": True, "ptt_active": False},
        source="test_readback",
    )
    try:
        assert engine.get_endpoint_operational_summaries()[1]["state"] == "on_schedule_verified"
        monotonic["value"] += 31.0
        summary = engine.get_endpoint_operational_summaries()[1]
        assert summary["state"] != "on_schedule_verified"
        assert summary["label"] == "Applied · verification unavailable"
    finally:
        _shutdown_engine(engine)


def test_mes5_fresh_ordinary_poll_must_match_active_intent_before_verified(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "mismatch-summary-profile"))
    SettingsManager()
    engine = SchedulerEngine()
    key = _key(62_01)
    engine._endpoint_keys_by_profile = {1: key}
    engine._expected_state_by_endpoint = {
        key.canonical: {"frequency_hz": 14_078_000, "control_mode": "RIGCTLD"}
    }
    engine._endpoint_status.publish(
        key,
        {"frequency_hz": 7_078_000, "ptt_known": True, "ptt_active": False},
        source="ordinary_poll",
    )
    try:
        summary = engine.get_endpoint_operational_summaries()[1]
        assert summary["state"] == "readback_mismatch"
        assert summary["label"] == "Off schedule · readback mismatch"
    finally:
        _shutdown_engine(engine)


def test_mes5_station_overview_requests_cache_only_runtime_snapshots(monkeypatch, tmp_path) -> None:
    """The dashboard refresh must remain a provider/cache read, never endpoint I/O."""

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "overview-profile"))
    child = textwrap.dedent(
        """
        import os
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
        from PySide6.QtWidgets import QApplication
        from freqinout.gui.station_overview_tab import StationOverviewTab

        app = QApplication([])
        calls = []

        class CacheOnlyManager:
            def get_runtime_snapshots(self, **kwargs):
                calls.append(dict(kwargs))
                return []

        tab = StationOverviewTab()
        tab.set_runtime_manager(CacheOnlyManager())
        assert calls == [{"force": True, "cache_only": True}]
        print("cache-only provider", flush=True)
        tab.deleteLater()
        app.processEvents()
        app.quit()
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", child],
        cwd=os.path.dirname(os.path.dirname(__file__)),
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=5.0,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "cache-only provider" in completed.stdout
