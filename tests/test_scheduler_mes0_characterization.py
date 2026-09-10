"""Scheduler characterizations established in MES-0 and advanced by MES-2.

These tests deliberately describe the current SchedulerEngine boundary.  They
remain a regression baseline. The two control-isolation assertions now capture
the MES-2 correction while the standalone MES-0 baseline tool retains evidence
of the former station-global blocking behavior.
"""

from __future__ import annotations

import datetime
import threading
import time
from types import SimpleNamespace

import pytest
from PySide6.QtCore import QCoreApplication, QEvent

from freqinout.core.scheduler_engine import (
    OffScheduleState,
    SchedulerEngine,
    StationActualState,
    compute_next_change_time,
)
from freqinout.core.shared_state import SchedulerManualControlState


class _FakeClock:
    """Deterministic wall/monotonic clock used by characterization tests."""

    def __init__(self, wall_utc: datetime.datetime) -> None:
        self.wall_utc = wall_utc
        self.monotonic_s = 100.0

    def advance(self, seconds: float) -> None:
        self.wall_utc += datetime.timedelta(seconds=seconds)
        self.monotonic_s += float(seconds)


class _BlockingRig:
    def __init__(self, *, result: bool = True) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.result = result
        self.commands = []

    def set_frequency(self, command) -> bool:
        self.started.set()
        self.release.wait()
        self.commands.append(command)
        return bool(self.result)

    def get_vfo_frequency(self) -> int:
        return 7_100_000


class _ImmediateRig:
    def __init__(self, *, result: bool = True) -> None:
        self.result = result
        self.commands = []
        self.started = threading.Event()

    def set_frequency(self, command) -> bool:
        self.started.set()
        self.commands.append(command)
        return bool(self.result)

    def get_vfo_frequency(self) -> int:
        return 7_100_000


def _shutdown_engine(engine: SchedulerEngine) -> None:
    engine.stop()
    app = QCoreApplication.instance()
    if app is not None:
        engine.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


def _queue_args(rig, *, entry_key: tuple, source: str = "HF", freq_hz: int = 14_115_000) -> dict:
    return {
        "control_mode": "FLRIG",
        "rig_client": rig,
        "js8_client": None,
        "allow_global_fallback": False,
        "entry_key": entry_key,
        "source": source,
        "freq_hz": freq_hz,
        "band": "20M",
        "mode": "USB",
        "vfo": "A",
        "auto_tune": False,
        "js8_offset": None,
        "js8_group": "",
    }


def test_mes0_fake_clock_freezes_cross_midnight_schedule_transition() -> None:
    clock = _FakeClock(datetime.datetime(2026, 9, 10, 23, 50, tzinfo=datetime.timezone.utc))
    overnight = {"start_utc": "23:00", "end_utc": "01:00"}
    daytime = None

    assert compute_next_change_time(clock.wall_utc, overnight, daytime) == datetime.datetime(
        2026, 9, 11, 1, 0, tzinfo=datetime.timezone.utc
    )
    clock.advance(80 * 60)
    assert compute_next_change_time(clock.wall_utc, overnight, daytime) is None


def test_mes0_three_radio_projection_dispatches_each_assigned_lane() -> None:
    applied: list[tuple[int, str, str]] = []

    class _ManualService:
        def get_state(self, radio_id: int):
            return SchedulerManualControlState(
                radio_profile_id=f"radio_{radio_id}", state="on_schedule"
            )

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler._manual_control_service = _ManualService()
    scheduler.settings = SimpleNamespace(get=lambda _key, default=None: default)
    scheduler.current_schedule_entry = {"target_device_profile_id": 99, "frequency": "7.000"}
    scheduler.active_schedule_lanes = lambda force=False, now_utc=None: [
        {
            "device_profile_id": radio_id,
            "current_source": "HF",
            "current_entry": {
                "target_device_profile_id": radio_id,
                "frequency": frequency,
                "band": "20M",
            },
        }
        for radio_id, frequency in ((8, "14.115"), (9, "14.110"), (10, "7.115"))
    ]
    scheduler._apply_schedule_entry = lambda entry, source, **_kwargs: applied.append(
        (int(entry["target_device_profile_id"]), str(entry["frequency"]), source)
    )

    assert SchedulerEngine._apply_active_schedule_lanes(
        scheduler,
        now_utc=datetime.datetime(2026, 9, 10, 12, 0, tzinfo=datetime.timezone.utc),
        force=True,
    ) is True
    assert applied == [(8, "14.115", "HF"), (9, "14.110", "HF"), (10, "7.115", "HF")]


def test_mes0_latest_intents_are_retained_per_radio_but_drained_station_globally() -> None:
    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.current_source = "HF"
    scheduler._latest_intent = None
    scheduler._latest_intents_by_radio = {}
    scheduler._latest_intent_ts = 0.0
    applied: list[tuple[int, str]] = []
    scheduler._primary_manual_control_radio_id = lambda: None
    scheduler._apply_schedule_entry = lambda entry, source, **_kwargs: applied.append(
        (int(entry["target_device_profile_id"]), str(entry["frequency"]))
    )

    for radio_id, frequency in ((10, "7.115"), (8, "14.115"), (10, "7.125")):
        SchedulerEngine._record_latest_intent(
            scheduler,
            {"target_device_profile_id": radio_id, "frequency": frequency, "band": "20M"},
            "HF",
        )

    # The per-radio slots coalesce radio 10 to its newest desired state, but
    # the current drain still pops one lowest-ID slot at a time.  This is the
    # measurable station-global behavior MES-2 must replace with independent
    # endpoint lanes.
    assert set(scheduler._latest_intents_by_radio) == {8, 10}
    assert SchedulerEngine._apply_latest_intent_if_any(scheduler) is True
    assert SchedulerEngine._apply_latest_intent_if_any(scheduler) is True
    assert SchedulerEngine._apply_latest_intent_if_any(scheduler) is False
    assert applied == [(8, "14.115"), (10, "7.125")]


def test_mes2_hung_endpoint_no_longer_blocks_peer_control_lane(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    rig_a = _BlockingRig()
    rig_b = _ImmediateRig()
    engine = SchedulerEngine(rig=rig_a, js8=None, varac=None, fldigi_log=None)
    engine._queue_scheduler_thread_call = lambda _callback: None
    engine._queue_post_apply_verification = lambda **_kwargs: None
    engine._record_scheduler_event = lambda *args, **kwargs: None
    try:
        assert SchedulerEngine._queue_control_action(engine, **_queue_args(
            rig_a, entry_key=("radio_8", 14_115_000)
        )) is True
        assert rig_a.started.wait(timeout=1.0)

        # MES-2 replaces the characterized global-worker defect. Endpoint B
        # starts on its own lane while endpoint A remains blocked.
        assert SchedulerEngine._queue_control_action(engine, **_queue_args(
            rig_b, entry_key=("radio_9", 14_110_000), freq_hz=14_110_000
        )) is True
        assert rig_b.started.wait(timeout=1.0)
        assert len(rig_b.commands) == 1

        rig_a.release.set()
        endpoint_a = engine._control_endpoint_key(
            "FLRIG", rig_client=rig_a, js8_client=None,
            entry_key=("radio_8", 14_115_000),
        )
        lane_a = engine._endpoint_lanes.lane(endpoint_a)
        assert lane_a is not None and lane_a.future is not None
        lane_a.future.result(timeout=2.0)
    finally:
        rig_a.release.set()
        _shutdown_engine(engine)


def test_mes2_failed_endpoint_backoff_isolated_from_peer_lane(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    failed_rig = _BlockingRig(result=False)
    peer_rig = _ImmediateRig()
    engine = SchedulerEngine(rig=failed_rig, js8=None, varac=None, fldigi_log=None)
    failure_recorded = threading.Event()
    engine._queue_scheduler_thread_call = lambda callback: callback()
    engine._queue_post_apply_verification = lambda **_kwargs: None
    engine._record_scheduler_event = lambda *args, **kwargs: None
    engine._record_scheduler_health_issue = lambda *args, **kwargs: failure_recorded.set()
    try:
        assert SchedulerEngine._queue_control_action(engine, **_queue_args(
            failed_rig, entry_key=("radio_8", 14_115_000)
        )) is True
        assert failed_rig.started.wait(timeout=1.0)
        failed_future = engine._control_future
        assert failed_future is not None
        failed_rig.release.set()
        failed_future.result(timeout=2.0)
        assert failure_recorded.wait(timeout=1.0)
        assert engine._control_fail_count == 1
        assert engine._control_backoff_until > time.time()

        assert SchedulerEngine._queue_control_action(engine, **_queue_args(
            peer_rig, entry_key=("radio_9", 14_110_000), freq_hz=14_110_000
        )) is True
        assert peer_rig.started.wait(timeout=1.0)
        assert len(peer_rig.commands) == 1
    finally:
        failed_rig.release.set()
        _shutdown_engine(engine)


def test_mes0_status_poll_uses_station_global_key_for_every_radio() -> None:
    keys: list[str] = []

    class _PollCoordinator:
        def get_snapshot(self, key, poller, *, force=False):
            keys.append(str(key))
            return SimpleNamespace(
                frequency_hz=7_100_000,
                generated_at=100.0,
                backoff_until=0.0,
            )

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = _ImmediateRig()
    scheduler._status_poll_coordinator = _PollCoordinator()
    scheduler._status_poll_ttl_s = 0.8
    scheduler._status_poll_retry_s = 4.0
    scheduler._status_flrig_freq_hz = None
    scheduler._status_flrig_freq_ts = 0.0
    scheduler._status_flrig_retry_ts = 0.0

    assert SchedulerEngine._status_poll_rig_frequency(scheduler, control_mode="FLRIG") == 7_100_000
    assert SchedulerEngine._status_poll_rig_frequency(scheduler, control_mode="FLRIG", force=True) == 7_100_000
    assert keys == ["scheduler:primary:rig_frequency", "scheduler:primary:rig_frequency"]


def _configure_safety_apply(engine: SchedulerEngine, monkeypatch, *, shared_blocked: bool, guard_blocked: bool) -> list:
    queued: list = []
    monkeypatch.setattr(engine, "_entry_with_operating_group_overrides", lambda entry: (dict(entry), {}))
    monkeypatch.setattr(engine, "_control_context_for_entry", lambda _entry: (
        engine.rig,
        None,
        None,
        SimpleNamespace(get=lambda key, default=None: "FLRig" if key == "control_via" else default),
        None,
    ))
    monkeypatch.setattr(engine, "_control_mode_for_context", lambda *_args, **_kwargs: "FLRIG")
    monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
    monkeypatch.setattr(engine, "_scheduling_suspended_for_radio", lambda *_args: (False, None))
    monkeypatch.setattr(engine, "_read_station_actual_state", lambda **_kwargs: StationActualState(
        actual_frequency_hz=7_100_000,
        actual_frequency_source="fake",
    ))
    monkeypatch.setattr(engine, "_compute_off_schedule_state", lambda *_args, **_kwargs: OffScheduleState(
        off_schedule=True,
        target_frequency_hz=14_115_000,
        actual_frequency_hz=7_100_000,
        actual_frequency_source="fake",
    ))
    monkeypatch.setattr(engine, "_hold_for_frequency_prompt", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False})
    monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **_kwargs: (False, None))
    monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
    monkeypatch.setattr(engine, "_varac_busy_ok", lambda **_kwargs: True)
    monkeypatch.setattr(engine, "_shared_ptt_lock_status", lambda **_kwargs: {
        "blocked": shared_blocked,
        "ptt_group": "AMP-A" if shared_blocked else "",
        "reason": "fake shared PTT" if shared_blocked else "",
    })
    monkeypatch.setattr(engine, "_coordination_conflict_status", lambda *_args, **_kwargs: {
        "blocked": guard_blocked,
        "guard_mode": "block" if guard_blocked else "confirm",
        "summary": "fake RF Guard conflict" if guard_blocked else "",
    })
    monkeypatch.setattr(engine, "_publish_shared_ptt_block_evidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_clear_shared_ptt_block_evidence", lambda: None)
    monkeypatch.setattr(engine, "_publish_local_ptt_busy_evidence", lambda **_kwargs: None)
    monkeypatch.setattr(engine, "_clear_local_ptt_busy_evidence", lambda: None)
    monkeypatch.setattr(engine, "_record_scheduler_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_record_scheduler_health_issue", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_clear_scheduler_health_issue", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append(kwargs) or True)
    return queued


@pytest.mark.parametrize(
    ("shared_blocked", "guard_blocked"),
    ((True, False), (False, True)),
    ids=("shared-ptt", "rf-guard"),
)
def test_mes0_safety_gate_holds_schedule_before_command(monkeypatch, tmp_path, shared_blocked, guard_blocked) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / f"profile-{shared_blocked}-{guard_blocked}"))
    rig = _ImmediateRig()
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    queued = _configure_safety_apply(
        engine,
        monkeypatch,
        shared_blocked=shared_blocked,
        guard_blocked=guard_blocked,
    )
    try:
        SchedulerEngine._apply_schedule_entry(
            engine,
            {"frequency": "14.115", "band": "20M", "mode": "USB", "vfo": "A"},
            "HF",
        )
        assert queued == []
        assert rig.commands == []
    finally:
        _shutdown_engine(engine)


def test_mes0_manual_qsy_precedes_schedule_apply(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    rig = _ImmediateRig()
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    engine._manual_qsy_active = True
    engine._manual_qsy_radio_id = None
    monkeypatch.setattr(engine, "_entry_with_operating_group_overrides", lambda entry: (dict(entry), {}))
    monkeypatch.setattr(engine, "_control_context_for_entry", lambda _entry: (
        rig, None, None,
        SimpleNamespace(get=lambda key, default=None: "FLRig" if key == "control_via" else default),
        None,
    ))
    monkeypatch.setattr(engine, "_record_scheduler_event", lambda *args, **kwargs: None)
    try:
        SchedulerEngine._apply_schedule_entry(
            engine,
            {"frequency": "14.115", "band": "20M", "mode": "USB", "vfo": "A"},
            "SOP",
        )
        assert rig.commands == []
        assert engine.current_source == "SOP"
        assert engine.current_schedule_entry["frequency"] == "14.115"
    finally:
        _shutdown_engine(engine)


def test_mes0_shutdown_invalidates_blocked_completion(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    rig = _BlockingRig()
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    engine._queue_scheduler_thread_call = lambda callback: callback()
    engine._queue_post_apply_verification = lambda **_kwargs: None
    engine._record_scheduler_event = lambda *args, **kwargs: None
    try:
        assert SchedulerEngine._queue_control_action(engine, **_queue_args(
            rig, entry_key=("radio_8", 14_115_000)
        )) is True
        assert rig.started.wait(timeout=1.0)
        blocked_future = engine._control_future
        assert blocked_future is not None
        engine.stop()
        assert engine._shutdown_requested is True

        rig.release.set()
        blocked_future.result(timeout=2.0)
        # The worker may finish, but its generation is stale after stop and
        # therefore cannot publish a late successful apply.
        assert engine._control_future is None
        assert engine._last_entry_key is None
    finally:
        rig.release.set()
        _shutdown_engine(engine)
