"""P1 regressions for target FLRig verification continuations.

These tests intentionally use endpoint-local events instead of sleeps.  A
target status read is allowed to finish asynchronously, but only a fresh,
successful, PTT-off completion for the exact current intent may release a
frequency command.
"""

from __future__ import annotations

import threading
import datetime
from types import SimpleNamespace
from typing import Dict, List, Optional

import pytest
from PySide6.QtCore import QCoreApplication, QEvent

from freqinout.core.scheduler_coordination import EndpointKey
from freqinout.core.scheduler_endpoint_status import EndpointStatusSnapshot
from freqinout.core.shared_state import ActionFeedbackService
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.gui import qsy_helper


class _Rig:
    def __init__(self, *, port: int, frequency_hz: int = 14_100_000,
                 ptt_active: bool = False, error: Optional[str] = None,
                 blocked: bool = False) -> None:
        self.host = "127.0.0.1"
        self.port = int(port)
        self.frequency_hz = int(frequency_hz)
        self.ptt_active = bool(ptt_active)
        self.error = error
        self.started = threading.Event()
        self.finished = threading.Event()
        self.release = threading.Event()
        if not blocked:
            self.release.set()

    def get_vfo_frequency(self) -> int:
        self.started.set()
        assert self.release.wait(timeout=2.0)
        self.finished.set()
        if self.error:
            raise RuntimeError(self.error)
        return self.frequency_hz

    def get_ptt(self) -> bool:
        if self.error:
            raise RuntimeError(self.error)
        return self.ptt_active

    def get_active_vfo(self) -> str:
        return "A"


class _Settings:
    def get(self, key: str, default: object = None) -> object:
        if key == "control_via":
            return "FLRig"
        return default


class _JS8:
    def __init__(self, *, offset_hz: int, error: Optional[str] = None) -> None:
        self.offset_hz = int(offset_hz)
        self.error = error
        self.offset_calls = 0
        self.offset_read = threading.Event()

    def get_offset(self) -> int:
        self.offset_calls += 1
        self.offset_read.set()
        if self.error:
            raise RuntimeError(self.error)
        return self.offset_hz


def _key(rig: _Rig) -> EndpointKey:
    return EndpointKey.network("flrig", rig.host, rig.port)


def _engine(monkeypatch, tmp_path) -> SchedulerEngine:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    if QCoreApplication.instance() is None:
        QCoreApplication([])
    engine = SchedulerEngine(poll_interval_ms=60_000)
    engine._queue_scheduler_thread_call = lambda callback: callback()
    engine._record_scheduler_health_issue = lambda *_args, **_kwargs: None
    engine._clear_scheduler_health_issue = lambda *_args, **_kwargs: None
    engine._record_scheduler_event = lambda *_args, **_kwargs: None
    engine._clear_fldigi_busy_check_state = lambda: None
    engine._flrig_running = lambda: True
    return engine


def _target_context(engine: SchedulerEngine, contexts: Dict[int, _Rig]) -> None:
    settings = _Settings()

    def resolve(entry: Optional[Dict[str, object]]):
        target_id = int((entry or {}).get("target_device_profile_id") or 0)
        rig = contexts[target_id]
        return rig, None, None, settings, target_id

    engine._control_context_for_entry = resolve
    engine._control_mode_for_context = lambda _settings, *, rig, js8: "FLRIG"


def _close(engine: SchedulerEngine) -> None:
    engine.stop()
    app = QCoreApplication.instance()
    if app is not None:
        engine.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


def _entry(target_id: int) -> Dict[str, object]:
    return {
        "target_device_profile_id": target_id,
        "band": "20M",
        "frequency": "14.115",
        "vfo": "A",
    }


def _capture_control(engine: SchedulerEngine) -> List[dict]:
    queued: List[dict] = []
    queued_event = threading.Event()
    engine._p1_queued_event = queued_event
    engine._queue_control_action = lambda **kwargs: (
        queued.append(dict(kwargs)), queued_event.set(), True
    )[-1]
    engine._should_delay_for_fldigi = lambda **_kwargs: (False, "")
    engine._coordination_conflict_status = lambda *_args, **_kwargs: {}
    engine._shared_ptt_lock_status = lambda **_kwargs: {}
    engine._update_desired_fldigi_settings = lambda *_args, **_kwargs: None
    engine._hold_for_frequency_prompt = lambda *_args, **_kwargs: False
    return queued


def _request_target(engine: SchedulerEngine, entry: Dict[str, object]) -> None:
    engine._apply_schedule_entry(
        entry,
        "QSY",
        force=True,
        ignore_wait_prompt=True,
        ignore_coordination_prompt=True,
        ignore_fldigi_busy=True,
    )


def test_cold_cache_completion_resumes_one_held_flrig_intent(monkeypatch, tmp_path) -> None:
    """A fresh target PTT-off completion releases one, and only one, command."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_481, blocked=True)
    _target_context(engine, {7: rig})
    queued = _capture_control(engine)
    try:
        _request_target(engine, _entry(7))
        assert rig.started.wait(timeout=2.0)
        # The first call sees a cold cache and must hold.  The exact endpoint's
        # completion is the continuation trigger; no timer tick is required.
        assert queued == []
        rig.release.set()
        assert rig.finished.wait(timeout=2.0)
        assert engine._p1_queued_event.wait(timeout=2.0)
        assert queued and len(queued) == 1
        assert queued[0]["endpoint_key"] == _key(rig)

        # Duplicate callbacks/completions must not create an apply loop.
        engine._request_endpoint_status_refresh(
            endpoint_key=_key(rig),
            rig_client=rig,
            js8_client=None,
            control_mode="FLRIG",
            force=True,
        )
        assert len(queued) == 1
    finally:
        _close(engine)


@pytest.mark.parametrize(
    "rig_kwargs",
    [
        {"ptt_active": True},
        {"error": "FLRig status failed"},
    ],
    ids=["ptt-on", "status-error"],
)
def test_unsafe_status_completion_never_releases_held_intent(
    monkeypatch, tmp_path, rig_kwargs: Dict[str, object]
) -> None:
    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_482, **rig_kwargs)
    _target_context(engine, {7: rig})
    queued = _capture_control(engine)
    try:
        _request_target(engine, _entry(7))
        assert rig.finished.wait(timeout=2.0)
        assert queued == []
    finally:
        _close(engine)


def test_target_endpoint_completion_cannot_release_peer_intent(monkeypatch, tmp_path) -> None:
    """A target A read must never satisfy or queue target B's held intent."""

    engine = _engine(monkeypatch, tmp_path)
    target_a = _Rig(port=12_483, ptt_active=True)
    target_b = _Rig(port=12_484)
    _target_context(engine, {7: target_a, 8: target_b})
    queued = _capture_control(engine)
    try:
        _request_target(engine, _entry(7))
        assert target_a.finished.wait(timeout=2.0)
        assert queued == []

        # B is independently checked and must receive its own continuation.
        _request_target(engine, _entry(8))
        assert target_b.finished.wait(timeout=2.0)
        assert engine._p1_queued_event.wait(timeout=2.0)
        assert len(queued) == 1
        assert queued[0]["endpoint_key"] == _key(target_b)
    finally:
        _close(engine)


def test_matching_readback_deduplicates_schedule_writes_but_manual_qsy_forces_apply(
    monkeypatch, tmp_path
) -> None:
    """A matching schedule is quiet; an explicit QSY remains an override."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_484, frequency_hz=14_115_000)
    _target_context(engine, {7: rig})
    engine._scheduler_enabled = lambda: True
    engine._maybe_refresh_external_status_snapshot = lambda **_kwargs: None
    endpoint = _key(rig)
    engine._endpoint_status.publish(
        endpoint,
        {"frequency_hz": 14_115_000, "vfo": "A"},
        source="ordinary_poll",
    )
    queued = _capture_control(engine)
    scheduled_entry = _entry(7)
    try:
        first_result = engine._apply_schedule_entry(
            scheduled_entry,
            "HF",
            ignore_wait_prompt=True,
            ignore_coordination_prompt=True,
            ignore_fldigi_busy=True,
        )
        assert first_result == "already_applied"
        assert queued == []
        assert endpoint.canonical in engine._expected_state_by_endpoint
        assert engine._expected_state_by_endpoint[endpoint.canonical]["js8_offset_hz"] is None
        assert engine.get_endpoint_operational_summaries()[7]["state"] == "on_schedule_verified"

        # A subsequent ordinary evaluation must remain quiet merely because
        # the UI or scheduler was refreshed.
        second_result = engine._apply_schedule_entry(
            scheduled_entry,
            "HF",
            ignore_wait_prompt=True,
            ignore_coordination_prompt=True,
            ignore_fldigi_busy=True,
        )
        assert second_result == "already_applied"
        assert queued == []

        # An explicit manual QSY is an operator command and must remain able
        # to change the endpoint even when the schedule cache is populated.
        engine._endpoint_status.publish(
            endpoint,
            {"frequency_hz": 14_115_000, "ptt_known": True, "ptt_active": False, "vfo": "A"},
            source="pre_qsy_safety_poll",
        )
        manual_entry = dict(scheduled_entry, frequency="7.115", band="40M")
        engine._apply_schedule_entry(
            manual_entry,
            "QSY",
            force=True,
            ignore_wait_prompt=True,
            ignore_coordination_prompt=True,
            ignore_fldigi_busy=True,
        )
        assert len(queued) == 1
        assert queued[0]["source"] == "QSY"
        assert queued[0]["freq_hz"] == 7_115_000
    finally:
        _close(engine)


def test_targeted_flrig_schedule_still_sets_configured_js8_offset(monkeypatch, tmp_path) -> None:
    """RF dedup must not suppress FIO-owned JS8 offset correction."""

    engine = _engine(monkeypatch, tmp_path)
    engine.settings.set("js8_offset_hz", 1_500)
    rig = _Rig(port=12_485, frequency_hz=14_115_000)
    js8 = _JS8(offset_hz=1_000)
    settings = _Settings()
    engine._control_context_for_entry = lambda _entry: (rig, js8, None, settings, 7)
    engine._control_mode_for_context = lambda _settings, *, rig, js8: "FLRIG"
    engine._scheduler_enabled = lambda: True
    engine._maybe_refresh_external_status_snapshot = lambda **_kwargs: None
    endpoint = _key(rig)
    engine._endpoint_status.publish(
        endpoint,
        {
            "frequency_hz": 14_115_000,
            "ptt_known": True,
            "ptt_active": False,
            "vfo": "A",
            "js8_offset_hz": 1_000,
        },
        source="ordinary_poll",
    )
    queued = _capture_control(engine)
    try:
        result = engine._apply_schedule_entry(
            _entry(7),
            "HF",
            ignore_wait_prompt=True,
            ignore_coordination_prompt=True,
            ignore_fldigi_busy=True,
        )

        assert result == "queued"
        assert len(queued) == 1
        assert queued[0]["freq_hz"] == 14_115_000
        assert queued[0]["js8_offset"] == 1_500
    finally:
        _close(engine)


def test_stale_status_does_not_release_held_intent(monkeypatch, tmp_path) -> None:
    """A stale cached read remains a safety hold until fresh evidence arrives."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_485)
    _target_context(engine, {7: rig})
    queued = _capture_control(engine)
    # Keep the first status request in flight.  Its eventual result is not a
    # usable completion while the request remains unresolved.
    rig.release.clear()
    try:
        _request_target(engine, _entry(7))
        assert rig.started.wait(timeout=2.0)
        assert queued == []
    finally:
        rig.release.set()
        _close(engine)


def _stage_continuation(engine: SchedulerEngine, rig: _Rig) -> EndpointKey:
    endpoint = _key(rig)
    engine._endpoint_keys_by_profile = {7: endpoint}
    engine._record_endpoint_status_continuation(
        endpoint_key=endpoint,
        device_profile_id=7,
        entry=_entry(7),
        source="QSY",
        now_utc=datetime.datetime.now(datetime.timezone.utc),
        force=True,
        ignore_suspend=True,
        ignore_wait_prompt=True,
        ignore_coordination_prompt=True,
        ignore_net_suppression=False,
        ignore_js8_busy=False,
        ignore_varac_busy=False,
        ignore_fldigi_busy=True,
        apply_js8_offset=True,
        apply_fldigi=True,
        scheduler_transition=False,
    )
    return endpoint


@pytest.mark.parametrize(
    "snapshot_kwargs,minimum_generation",
    [
        ({"ptt_active": True, "ptt_known": True}, 1),
        ({"ptt_known": True, "error_fields": (("poll", "offline"),)}, 1),
        ({"ptt_known": True, "stale": True}, 1),
        ({"ptt_known": True}, 2),
    ],
    ids=["ptt-on", "error", "stale", "superseded"],
)
def test_unsafe_or_superseded_status_completion_never_queues(
    monkeypatch, tmp_path, snapshot_kwargs: Dict[str, object], minimum_generation: int
) -> None:
    """Continuation gating rejects every status result that cannot authorize RF."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_486)
    _target_context(engine, {7: rig})
    queued = _capture_control(engine)
    endpoint = _stage_continuation(engine, rig)
    intent = engine._status_continuations_by_endpoint[endpoint.canonical]
    intent["minimum_generation"] = minimum_generation
    snapshot = EndpointStatusSnapshot(
        endpoint_key=endpoint,
        generation=1,
        collected_wall_time=1.0,
        collected_monotonic=1.0,
        frequency_hz=14_100_000,
        **snapshot_kwargs,
    )
    try:
        assert engine._resume_endpoint_status_continuation(endpoint, snapshot) is False
        assert queued == []
    finally:
        _close(engine)


def test_healthy_periodic_status_refresh_keeps_operational_summary_verified(monkeypatch, tmp_path) -> None:
    """Each endpoint-local cadence refresh preserves truthful verified status."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_487, frequency_hz=14_115_000)
    _target_context(engine, {7: rig})
    endpoint = _key(rig)
    engine._endpoint_keys_by_profile = {7: endpoint}
    engine._expected_state_by_endpoint = {
        endpoint.canonical: {"frequency_hz": 14_115_000, "control_mode": "FLRIG", "vfo": "A"}
    }
    completions = threading.Event()
    engine._clear_scheduler_health_issue = lambda *_args, **_kwargs: completions.set()
    try:
        for _cycle in range(3):
            completions.clear()
            engine._endpoint_status.invalidate(endpoint)
            engine._request_endpoint_status_refresh(
                endpoint_key=endpoint,
                rig_client=rig,
                js8_client=None,
                control_mode="FLRIG",
                force=True,
            )
            assert completions.wait(timeout=2.0)
            summary = engine.get_endpoint_operational_summaries()[7]
            assert summary["state"] == "on_schedule_verified"
            assert summary["label"] == "On schedule · verified"
    finally:
        _close(engine)


def test_timer_cadence_refreshes_each_runtime_endpoint_and_keeps_summary_verified(
    monkeypatch, tmp_path
) -> None:
    """Timer cadence refreshes endpoint-local runtimes without UI or DB work."""

    engine = _engine(monkeypatch, tmp_path)
    rig_a = _Rig(port=12_488, frequency_hz=14_115_000)
    rig_b = _Rig(port=12_489, frequency_hz=7_115_000)
    endpoint_a = _key(rig_a)
    endpoint_b = _key(rig_b)

    class _Manager:
        def __init__(self) -> None:
            self.calls: List[int] = []
            self.runtimes = {
                7: SimpleNamespace(rig_client=rig_a, js8_control_client=None, settings_proxy=_Settings()),
                8: SimpleNamespace(rig_client=rig_b, js8_control_client=None, settings_proxy=_Settings()),
            }

        def get_runtime_for_device(self, profile_id: int):
            self.calls.append(int(profile_id))
            return self.runtimes[int(profile_id)]

    manager = _Manager()
    engine.station_runtime_manager = manager
    engine._endpoint_keys_by_profile = {7: endpoint_a, 8: endpoint_b}
    engine._expected_state_by_endpoint = {
        endpoint_a.canonical: {"frequency_hz": 14_115_000, "control_mode": "FLRIG", "vfo": "A"},
        endpoint_b.canonical: {"frequency_hz": 7_115_000, "control_mode": "FLRIG", "vfo": "A"},
    }
    completed = {7: threading.Event(), 8: threading.Event()}

    def health_clear(issue: str, **kwargs: object) -> None:
        endpoint = str(kwargs.get("endpoint_key") or "")
        if endpoint == endpoint_a.canonical:
            completed[7].set()
        elif endpoint == endpoint_b.canonical:
            completed[8].set()

    engine._clear_scheduler_health_issue = health_clear
    # Keep the timer test focused on endpoint cadence rather than schedule
    # projection, external status, or database work.
    engine._observe_lifecycle_clock = lambda **_kwargs: None
    engine._maybe_refresh_external_status_snapshot = lambda **_kwargs: None
    engine._request_active_schedule_lane_rows_refresh = lambda: None
    engine._apply_cached_schedule_tick = lambda **_kwargs: None
    try:
        for _cycle in range(2):
            completed[7].clear()
            completed[8].clear()
            engine._endpoint_status.invalidate(endpoint_a)
            engine._endpoint_status.invalidate(endpoint_b)
            engine._on_timer()
            assert completed[7].wait(timeout=2.0)
            assert completed[8].wait(timeout=2.0)
            summaries = engine.get_endpoint_operational_summaries()
            assert summaries[7]["state"] == "on_schedule_verified"
            assert summaries[8]["state"] == "on_schedule_verified"
        assert manager.calls.count(7) == 2
        assert manager.calls.count(8) == 2
    finally:
        _close(engine)


def test_flrig_liveness_refresh_reads_mapped_js8_offset_when_expected(
    monkeypatch, tmp_path
) -> None:
    """An FLRig route with JS8 offset authority must refresh its mapped JS8 client."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_490, frequency_hz=14_115_000)
    js8 = _JS8(offset_hz=22)
    endpoint = _key(rig)

    class _Manager:
        def get_runtime_for_device(self, profile_id: int):
            assert int(profile_id) == 7
            return SimpleNamespace(
                rig_client=rig,
                js8_control_client=js8,
                settings_proxy=_Settings(),
            )

    engine.station_runtime_manager = _Manager()
    engine._endpoint_keys_by_profile = {7: endpoint}
    engine._expected_state_by_endpoint = {
        endpoint.canonical: {
            "frequency_hz": 14_115_000,
            "control_mode": "FLRIG",
            "vfo": "A",
            "js8_offset_hz": 22,
        }
    }
    completed = threading.Event()

    def health_clear(issue: str, **kwargs: object) -> None:
        if str(kwargs.get("endpoint_key") or "") == endpoint.canonical:
            completed.set()

    engine._clear_scheduler_health_issue = health_clear
    try:
        engine._request_active_endpoint_status_refreshes()
        assert rig.finished.wait(timeout=2.0)
        assert js8.offset_read.wait(timeout=2.0)
        assert completed.wait(timeout=2.0)
        assert js8.offset_calls == 1
        snapshot = engine._endpoint_status.latest(endpoint)
        assert snapshot.js8_offset_hz == 22
        summary = engine.get_endpoint_operational_summaries()[7]
        assert summary["state"] == "on_schedule_verified"
        assert summary["label"] == "On schedule · verified"
    finally:
        _close(engine)


def test_active_flrig_liveness_uses_configured_client_without_process_inventory(
    monkeypatch, tmp_path
) -> None:
    """A configured runtime client is authoritative even when process probing fails."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_494, frequency_hz=14_115_000)
    endpoint = _key(rig)

    class _Manager:
        def get_runtime_for_device(self, profile_id: int):
            assert int(profile_id) == 7
            return SimpleNamespace(
                rig_client=rig,
                js8_control_client=None,
                settings_proxy=_Settings(),
            )

    engine.station_runtime_manager = _Manager()
    engine._endpoint_keys_by_profile = {7: endpoint}
    # Liveness polling must not infer endpoint reachability from process
    # inventory; only the configured runtime/client is used.
    engine._flrig_running = lambda: pytest.fail("active endpoint polling used global FLRig process inventory")
    completed = threading.Event()

    def health_clear(issue: str, **kwargs: object) -> None:
        if str(kwargs.get("endpoint_key") or "") == endpoint.canonical:
            completed.set()

    engine._clear_scheduler_health_issue = health_clear
    try:
        engine._request_active_endpoint_status_refreshes()
        assert rig.finished.wait(timeout=2.0)
        assert completed.wait(timeout=2.0)
        snapshot = engine._endpoint_status.latest(endpoint)
        assert snapshot.known is True
        assert snapshot.frequency_hz == 14_115_000
    finally:
        _close(engine)


def test_matching_rf_without_expected_js8_offset_readback_requires_js8_verification(
    monkeypatch, tmp_path
) -> None:
    """Matching RF alone must not present an expected JS8 offset as verified."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_491, frequency_hz=14_115_000)
    endpoint = _key(rig)
    engine._endpoint_keys_by_profile = {7: endpoint}
    engine._expected_state_by_endpoint = {
        endpoint.canonical: {
            "frequency_hz": 14_115_000,
            "control_mode": "FLRIG",
            "vfo": "A",
            "js8_offset_hz": 22,
        }
    }
    engine._endpoint_status.publish(
        endpoint,
        {
            "frequency_hz": 14_115_000,
            "ptt_active": False,
            "ptt_known": True,
            "vfo": "A",
        },
        source="flrig_readback_without_js8",
    )
    try:
        summary = engine.get_endpoint_operational_summaries()[7]
        assert summary["state"] == "js8_verification_unavailable"
        assert summary["label"] == "RF verified · verify JS8Call"
        assert "JS8Call offset" in summary["detail"]
    finally:
        _close(engine)


def test_js8_offset_error_with_matching_flrig_readback_requires_js8_verification(
    monkeypatch, tmp_path
) -> None:
    """A JS8-only read error is not an unavailable FLRig endpoint."""

    engine = _engine(monkeypatch, tmp_path)
    endpoint = EndpointKey.network("flrig", "127.0.0.1", 12_495)
    engine._endpoint_keys_by_profile = {7: endpoint}
    engine._expected_state_by_endpoint = {
        endpoint.canonical: {
            "frequency_hz": 14_115_000,
            "control_mode": "FLRIG",
            "vfo": "A",
            "js8_offset_hz": 22,
        }
    }
    # Seed the matching FLRig evidence, then model the cache projection
    # produced when the mapped JS8 offset read raises.  The status registry
    # must retain the usable RF evidence while recording only the JS8 error.
    engine._endpoint_status.publish(
        endpoint,
        {
            "frequency_hz": 14_115_000,
            "ptt_active": False,
            "ptt_known": True,
            "vfo": "A",
        },
        source="active_endpoint_flrig_readback",
    )
    engine._endpoint_status.publish(
        endpoint,
        {"errors": {"js8": "JS8Call offset read failed"}},
        source="active_endpoint_js8_error",
    )
    try:
        snapshot = engine._endpoint_status.latest(endpoint)
        assert snapshot.frequency_hz == 14_115_000
        assert snapshot.errors == {"js8": "JS8Call offset read failed"}
        summary = engine.get_endpoint_operational_summaries()[7]
        assert summary["state"] == "js8_verification_unavailable"
        assert summary["label"] == "RF verified · verify JS8Call"
    finally:
        _close(engine)


def _post_apply_readback(frequency_hz: int) -> Dict[str, object]:
    return {
        "checked_ts": 100.0,
        "flrig_freq_hz": frequency_hz,
        "flrig_ptt_active": False,
        "flrig_ptt_known": True,
        "flrig_vfo": "A",
        "errors": {},
    }


def test_successful_readback_publishes_while_newer_endpoint_generation_is_running(
    monkeypatch, tmp_path
) -> None:
    """A newer current generation does not erase usable evidence from N."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_492)
    endpoint = _key(rig)
    first_done = threading.Event()
    newer_started = threading.Event()
    release_newer = threading.Event()
    try:
        first = engine._endpoint_lanes.submit(
            endpoint,
            occurrence_id="generation-1",
            operation=lambda: True,
            completion=lambda _result: first_done.set(),
        )
        assert first.accepted
        assert first_done.wait(timeout=2.0)
        assert first.generation == 1

        def newer_operation() -> bool:
            newer_started.set()
            assert release_newer.wait(timeout=2.0)
            return True

        newer = engine._endpoint_lanes.submit(
            endpoint,
            occurrence_id="generation-2",
            operation=newer_operation,
            completion=lambda _result: None,
        )
        assert newer.accepted
        assert newer_started.wait(timeout=2.0)
        assert engine._endpoint_lanes.lane(endpoint).snapshot().current_generation == 2

        engine._queue_post_apply_verification(
            control_future_token=first.generation,
            endpoint_key=endpoint,
            rig_client=rig,
            control_mode="FLRIG",
            source="HF",
            freq_hz=14_115_000,
            band="20M",
            mode="USB",
            vfo="A",
            entry_key=("target", 7),
            verification_data=_post_apply_readback(14_115_000),
        )
        snapshot = engine._endpoint_status.latest(endpoint)
        assert snapshot.frequency_hz == 14_115_000
        assert snapshot.source == "command_verification"
    finally:
        release_newer.set()
        _close(engine)


def test_older_readback_cannot_overwrite_later_successful_generation(
    monkeypatch, tmp_path
) -> None:
    """Once generation N+1 publishes, a late N readback is ignored."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _Rig(port=12_493)
    endpoint = _key(rig)
    first_done = threading.Event()
    newer_done = threading.Event()
    try:
        first = engine._endpoint_lanes.submit(
            endpoint,
            occurrence_id="generation-1",
            operation=lambda: True,
            completion=lambda _result: first_done.set(),
        )
        assert first_done.wait(timeout=2.0)
        newer = engine._endpoint_lanes.submit(
            endpoint,
            occurrence_id="generation-2",
            operation=lambda: True,
            completion=lambda _result: newer_done.set(),
        )
        assert newer_done.wait(timeout=2.0)
        assert first.generation == 1 and newer.generation == 2

        engine._queue_post_apply_verification(
            control_future_token=newer.generation,
            endpoint_key=endpoint,
            rig_client=rig,
            control_mode="FLRIG",
            source="HF",
            freq_hz=14_120_000,
            band="20M",
            mode="USB",
            vfo="A",
            entry_key=("target", 7),
            verification_data=_post_apply_readback(14_120_000),
        )
        newest = engine._endpoint_status.latest(endpoint)
        assert newest.frequency_hz == 14_120_000

        engine._queue_post_apply_verification(
            control_future_token=first.generation,
            endpoint_key=endpoint,
            rig_client=rig,
            control_mode="FLRIG",
            source="HF",
            freq_hz=14_115_000,
            band="20M",
            mode="USB",
            vfo="A",
            entry_key=("target", 7),
            verification_data=_post_apply_readback(14_115_000),
        )
        retained = engine._endpoint_status.latest(endpoint)
        assert retained.frequency_hz == 14_120_000
        assert retained.generation == newest.generation
    finally:
        _close(engine)


@pytest.mark.parametrize(
    "disposition, expected_result, expected_status, expected_summary",
    [
        (
            "pending_verification",
            True,
            "partial",
            "Checking target radio before QSY…",
        ),
        (
            "blocked",
            False,
            "blocked",
            "QSY blocked for the selected radio.",
        ),
    ],
    ids=["pending-verification", "blocked"],
)
def test_qsy_helper_reports_scheduler_disposition_feedback(
    disposition: str,
    expected_result: bool,
    expected_status: str,
    expected_summary: str,
) -> None:
    service = ActionFeedbackService()

    class _Scheduler:
        def apply_manual_qsy(self, entry, *, ignore_coordination_prompt=False):
            return disposition

    window = SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "Target FLRig"},
    )
    result = qsy_helper.perform_qsy(window, {"freq": 14.115, "band": "20M", "mode": "USB"})

    events = service.recent(scope="scheduler" if disposition == "pending_verification" else "radio")
    assert result is expected_result
    assert len(events) == 1
    assert events[0].action_type == "qsy"
    assert events[0].status == expected_status
    assert events[0].summary == expected_summary
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "Target FLRig"


def test_qsy_helper_preserves_legacy_none_scheduler_result() -> None:
    service = ActionFeedbackService()

    class _Scheduler:
        def apply_manual_qsy(self, entry, *, ignore_coordination_prompt=False):
            return None

    window = SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "Target FLRig"},
    )
    assert qsy_helper.perform_qsy(window, {"freq": 14.115, "band": "20M"}) is True
    assert service.recent(scope="scheduler") == []
