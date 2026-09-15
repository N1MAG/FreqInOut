"""MES-3 integration tests for endpoint-scoped SchedulerEngine status.

The rig doubles deliberately block on ``threading.Event`` instances.  The
tests therefore exercise the production status registry without relying on
wall-clock sleeps or real radio/application endpoints.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Dict, List, Optional

from PySide6.QtCore import QCoreApplication, QEvent

import freqinout.core.scheduler_engine as scheduler_engine_module
from freqinout.core.busy_evidence_service import BusyEvidenceService
from freqinout.core.scheduler_coordination import EndpointKey, EndpointResult
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.ptt_conflict_service import PttConflictService
from freqinout.radio_interface.rigctl_client import FrequencyCommand
from freqinout.core.settings_manager import SettingsManager


class _Rig:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        frequency_hz: int = 14_115_000,
        ptt_active: bool = False,
        block: bool = False,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.frequency_hz = int(frequency_hz)
        self.ptt_active = bool(ptt_active)
        self.block = bool(block)
        self.started = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.calls = 0
        if not self.block:
            self.release.set()

    def get_vfo_frequency(self) -> int:
        self.calls += 1
        self.started.set()
        assert self.release.wait(timeout=2.0)
        self.finished.set()
        return self.frequency_hz

    def get_ptt(self) -> bool:
        self.calls += 1
        return self.ptt_active

    def get_active_vfo(self) -> str:
        return "A"

    def release_io(self) -> None:
        self.release.set()


class _Settings:
    def get(self, key: str, default: object = None) -> object:
        if key == "control_via":
            return "FLRig"
        return default


class _StartupOverlapRig(_Rig):
    """Keep the first liveness read open while command verification proceeds."""

    def __init__(self, *, host: str, port: int, frequency_hz: int) -> None:
        super().__init__(host=host, port=port, frequency_hz=frequency_hz)
        self.release.clear()
        self._frequency_read_count = 0
        self._frequency_read_lock = threading.Lock()

    def get_vfo_frequency(self) -> int:
        with self._frequency_read_lock:
            self._frequency_read_count += 1
            call_number = self._frequency_read_count
        self.calls += 1
        if call_number == 1:
            self.started.set()
            assert self.release.wait(timeout=2.0)
            self.finished.set()
        return self.frequency_hz

    def set_frequency(self, command: FrequencyCommand) -> bool:
        self.frequency_hz = command.hz
        return True


def _key(rig: _Rig) -> EndpointKey:
    return EndpointKey.network("flrig", rig.host, rig.port)


def _wait(event: threading.Event, description: str) -> None:
    assert event.wait(timeout=2.0), description


def _engine(monkeypatch, tmp_path) -> SchedulerEngine:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QCoreApplication.instance()
    if app is None:
        QCoreApplication([])
    engine = SchedulerEngine(poll_interval_ms=60_000)
    engine._queue_scheduler_thread_call = lambda callback: callback()
    engine._record_scheduler_health_issue = lambda *_args, **_kwargs: None
    engine._clear_scheduler_health_issue = lambda *_args, **_kwargs: None
    engine._record_scheduler_event = lambda *_args, **_kwargs: None
    engine._clear_fldigi_busy_check_state = lambda: None
    engine._flrig_running = lambda: True
    return engine


def _close(engine: SchedulerEngine) -> None:
    engine.stop()
    app = QCoreApplication.instance()
    if app is not None:
        engine.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


def _target_context(engine: SchedulerEngine, contexts: Dict[int, _Rig]) -> None:
    settings = _Settings()

    def resolve(entry: Optional[Dict[str, object]]):
        target_id = int((entry or {}).get("target_device_profile_id") or 0)
        rig = contexts[target_id]
        return rig, None, None, settings, target_id

    engine._control_context_for_entry = resolve
    engine._control_mode_for_context = lambda _settings, *, rig, js8: "FLRIG"


def test_target_status_returns_unknown_promptly_and_peer_refresh_is_independent(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    blocked = _Rig(host="127.0.0.1", port=12_361, block=True)
    peer = _Rig(host="127.0.0.1", port=12_362, frequency_hz=7_115_000)
    _target_context(engine, {1: blocked, 2: peer})
    entry_a = {"target_device_profile_id": 1, "band": "20M", "frequency": "14.115"}
    entry_b = {"target_device_profile_id": 2, "band": "40M", "frequency": "7.115"}
    try:
        first_a = engine._read_target_station_actual_state(entry_a, control_mode="FLRIG")
        _wait(blocked.started, "endpoint A status read did not start")
        assert first_a.actual_frequency_hz is None
        assert first_a.actual_frequency_source == "unknown"
        assert first_a.stale is True

        first_b = engine._read_target_station_actual_state(entry_b, control_mode="FLRIG")
        _wait(peer.finished, "endpoint B status read did not complete")
        # The request itself is nonblocking; a very fast peer may complete
        # before the caller inspects the returned projection.
        assert first_b.actual_frequency_hz in (None, 7_115_000)

        second_b = engine._read_target_station_actual_state(entry_b, control_mode="FLRIG")
        assert second_b.actual_frequency_hz == 7_115_000
        assert second_b.actual_frequency_source == "Rig"
        assert second_b.stale is False

        key_a = _key(blocked)
        key_b = _key(peer)
        snapshot_a = engine._endpoint_status.latest(key_a)
        snapshot_b = engine._endpoint_status.latest(key_b)
        assert snapshot_a.stale is True
        assert snapshot_a.inflight is True
        assert snapshot_b.stale is False
        assert snapshot_b.frequency_hz == 7_115_000
    finally:
        blocked.release_io()
        _close(engine)


def test_startup_liveness_poll_cannot_replace_post_apply_verification(
    monkeypatch, tmp_path
) -> None:
    """An already-running FLRig verifies the first startup schedule apply once."""

    engine = _engine(monkeypatch, tmp_path)
    rig = _StartupOverlapRig(
        host="127.0.0.1",
        port=12_370,
        frequency_hz=14_100_000,
    )
    endpoint = _key(rig)
    engine._endpoint_keys_by_profile = {7: endpoint}
    engine._should_delay_for_fldigi = lambda **_kwargs: (False, "")
    engine._coordination_conflict_status = lambda *_args, **_kwargs: {}
    engine._shared_ptt_lock_status = lambda **_kwargs: {}
    engine._update_desired_fldigi_settings = lambda *_args, **_kwargs: None
    engine._hold_for_frequency_prompt = lambda *_args, **_kwargs: False
    applied = threading.Event()
    original_clear = engine._clear_scheduler_health_issue

    def _clear_health(name: str, **kwargs: object) -> None:
        original_clear(name, **kwargs)
        if name.startswith("control-task:"):
            applied.set()

    engine._clear_scheduler_health_issue = _clear_health
    try:
        # Startup liveness sees the already-running endpoint first and remains
        # in flight while the initial schedule command is queued.
        engine._request_endpoint_status_refresh(
            endpoint_key=endpoint,
            rig_client=rig,
            js8_client=None,
            control_mode="FLRIG",
            force=True,
        )
        _wait(rig.started, "startup liveness read did not begin")

        queued = engine._queue_control_action(
            control_mode="FLRIG",
            rig_client=rig,
            js8_client=None,
            endpoint_key=endpoint,
            device_profile_id=7,
            allow_global_fallback=False,
            entry_key=(7, "20M", 14_115_000, None, None, "A", "", "DIGI"),
            source="HF",
            freq_hz=14_115_000,
            band="20M",
            mode="DIGI",
            vfo="A",
            auto_tune=False,
            js8_offset=None,
            js8_group="",
        )
        assert queued is True
        _wait(applied, "startup schedule command did not complete")

        # The command result owns a purpose-specific, post-apply readback. It
        # must not inherit the blank in-flight liveness placeholder.
        summary = engine.get_endpoint_operational_summaries()[7]
        assert summary["state"] == "on_schedule_verified"
        assert summary["label"] == "On schedule · verified"
        assert rig._frequency_read_count == 2

        # Releasing the older liveness request cannot overwrite the command's
        # authoritative readback or trigger another schedule command.
        rig.release_io()
        _wait(rig.finished, "startup liveness read did not finish")
        assert engine.get_endpoint_operational_summaries()[7]["state"] == "on_schedule_verified"
        lane = engine._endpoint_lanes.lane(endpoint)
        assert lane is not None
        assert lane.snapshot().last_success_generation == 1
    finally:
        rig.release_io()
        _close(engine)


def test_target_ptt_does_not_inherit_primary_global_ptt_cache(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    primary = _Rig(host="127.0.0.1", port=12_363, ptt_active=True)
    target = _Rig(host="127.0.0.1", port=12_364, ptt_active=False)
    _target_context(engine, {2: target})
    engine._last_ptt_active = True
    engine._status_flrig_ptt = True
    engine._status_flrig_ptt_known = True
    engine._status_flrig_ptt_ts = 9_999_999_999.0
    entry = {"target_device_profile_id": 2, "band": "20M", "frequency": "14.115"}
    try:
        pending = engine._read_target_station_actual_state(entry, control_mode="FLRIG")
        _wait(target.finished, "target PTT status read did not complete")
        assert pending.flrig_ptt_active is False

        actual = engine._read_target_station_actual_state(entry, control_mode="FLRIG")
        assert actual.flrig_ptt_active is False
        assert actual.flrig_ptt_known is True
        assert actual.flrig_ptt_stale is False
        assert engine._last_ptt_active is True
        assert primary.calls == 0
    finally:
        _close(engine)


def test_inline_command_verification_publishes_only_matching_endpoint(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    target = _Rig(host="127.0.0.1", port=12_365)
    peer = _Rig(host="127.0.0.1", port=12_366, frequency_hz=7_115_000)
    target_key = _key(target)
    peer_key = _key(peer)
    target_snapshot_before = engine._endpoint_status.publish(
        target_key, {"frequency_hz": 7_100_000, "ptt_active": False, "ptt_known": True}, source="seed"
    )
    peer_snapshot_before = engine._endpoint_status.publish(
        peer_key, {"frequency_hz": 7_115_000, "ptt_active": True, "ptt_known": True}, source="seed"
    )
    completion: List[EndpointResult] = []
    completed = threading.Event()
    try:
        submission = engine._endpoint_lanes.submit(
            target_key,
            occurrence_id="command",
            operation=lambda: True,
            completion=lambda result: (completion.append(result), completed.set()),
        )
        _wait(completed, "target command lane did not complete")
        assert submission.accepted is True
        assert completion
        generation = completion[0].generation

        engine._queue_post_apply_verification(
            control_future_token=generation,
            endpoint_key=target_key,
            rig_client=target,
            js8_client=None,
            control_mode="FLRIG",
            source="HF",
            freq_hz=14_115_000,
            band="20M",
            mode="USB",
            vfo="A",
            entry_key=("target", 2),
            verification_data={
                "checked_ts": 100.0,
                "flrig_freq_hz": 14_115_000,
                "flrig_ptt_active": False,
                "flrig_ptt_known": True,
                "flrig_vfo": "A",
                "errors": {},
            },
        )
        target_snapshot_after = engine._endpoint_status.latest(target_key)
        peer_snapshot_after = engine._endpoint_status.latest(peer_key)
        assert target_snapshot_after.frequency_hz == 14_115_000
        assert target_snapshot_after.source == "command_verification"
        assert target_snapshot_after.generation >= target_snapshot_before.generation
        assert peer_snapshot_after == peer_snapshot_before
    finally:
        _close(engine)


def test_inline_command_readback_mismatch_is_not_reported_as_verified(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    target = _Rig(host="127.0.0.1", port=12_369)
    target_key = _key(target)
    completed = threading.Event()
    results: List[EndpointResult] = []
    try:
        submission = engine._endpoint_lanes.submit(
            target_key,
            occurrence_id="mismatch-command",
            operation=lambda: True,
            completion=lambda result: (results.append(result), completed.set()),
        )
        _wait(completed, "target command lane did not complete")
        assert submission.accepted is True
        engine._endpoint_keys_by_profile = {2: target_key}

        engine._queue_post_apply_verification(
            control_future_token=results[0].generation,
            endpoint_key=target_key,
            rig_client=target,
            js8_client=None,
            control_mode="FLRIG",
            source="HF",
            freq_hz=14_115_000,
            band="20M",
            mode="USB",
            vfo="A",
            entry_key=("target", 2),
            verification_data={
                "checked_ts": 100.0,
                "flrig_freq_hz": 14_100_000,
                "flrig_ptt_active": False,
                "flrig_ptt_known": True,
                "flrig_vfo": "A",
                "errors": {},
            },
        )

        snapshot = engine._endpoint_status.latest(target_key)
        assert "readback_mismatch" in snapshot.errors
        summary = engine.get_endpoint_operational_summaries()[2]
        assert summary["state"] == "readback_mismatch"
        assert summary["label"] == "Off schedule · readback mismatch"
    finally:
        _close(engine)


def test_endpoint_status_summaries_are_cache_only(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)

    class _ExplodingRig:
        host = "127.0.0.1"
        port = 12_367

        def get_vfo_frequency(self):
            raise AssertionError("summary lookup performed endpoint I/O")

        def get_ptt(self):
            raise AssertionError("summary lookup performed endpoint I/O")

    endpoint = EndpointKey.network("flrig", "127.0.0.1", 12_367)
    engine._endpoint_keys_by_profile = {7: endpoint}
    engine._endpoint_status.publish(
        endpoint,
        {"frequency_hz": 14_115_000, "ptt_active": False, "ptt_known": True},
        source="test",
    )
    engine.rig = _ExplodingRig()
    try:
        summaries = engine.get_endpoint_status_summaries()
        assert summaries[7]["frequency_hz"] == 14_115_000
        assert summaries[7]["endpoint_key"] == endpoint.canonical
    finally:
        _close(engine)


def test_stop_suppresses_late_status_health_and_cache_mutation(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    blocked = _Rig(host="127.0.0.1", port=12_368, block=True)
    _target_context(engine, {8: blocked})
    health_calls: List[object] = []
    engine._record_scheduler_health_issue = lambda *args, **kwargs: health_calls.append((args, kwargs))
    entry = {"target_device_profile_id": 8, "band": "20M", "frequency": "14.115"}
    try:
        engine._read_target_station_actual_state(entry, control_mode="FLRIG")
        _wait(blocked.started, "blocked status read did not start")
        key = _key(blocked)
        before = engine._endpoint_status.latest(key)
        assert before.inflight is True

        engine.stop()
        blocked.release_io()
        assert health_calls == []
        after = engine._endpoint_status.latest(key)
        assert after.closed is True
        assert after.stale is True
        assert after.inflight is False
    finally:
        blocked.release_io()
        if not engine._shutdown_requested:
            _close(engine)


def test_scheduler_event_uses_explicit_target_radio_scope(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    captured: List[Dict[str, object]] = []
    monkeypatch.setattr(
        scheduler_engine_module,
        "record_scheduler_event",
        lambda **kwargs: captured.append(dict(kwargs)),
    )
    monkeypatch.setattr(engine, "_primary_manual_control_radio_id", lambda: 1)
    try:
        SchedulerEngine._record_scheduler_event(
            engine,
            "hold",
            "ptt_state_unknown",
            source="HF",
            entry={"target_device_profile_id": 2, "band": "20M", "frequency": "14.115"},
            entry_key=(2, "20M", "14.115"),
            action="Target status is unknown",
        )
        assert captured[-1]["radio_profile_id"] == "radio_2"
        assert captured[-1]["metadata"]["radio_profile_id"] == "radio_2"

        captured.clear()
        SchedulerEngine._record_scheduler_event(
            engine,
            "status",
            "primary_status",
            source="HF",
            entry={"band": "20M", "frequency": "14.115"},
        )
        assert captured[-1]["radio_profile_id"] == "radio_1"
    finally:
        _close(engine)


def test_target_ptt_evidence_publish_and_clear_do_not_touch_peer(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    for profile_id in (1, 2, 3):
        store.save_device_profile(
            {
                "id": profile_id,
                "name": "Primary" if profile_id == 1 else f"Rig {profile_id}",
                "control_backend": "flrig",
                "device_class": "tx_rx",
            }
        )
    engine = SchedulerEngine(poll_interval_ms=60_000)
    busy = BusyEvidenceService(store)
    conflicts = PttConflictService(store)
    shared = {
        "ptt_group": "AMP-A",
        "owner_device_profile_id": 3,
        "reason": "Rig 3 is transmitting.",
    }
    try:
        engine._publish_local_ptt_busy_evidence(source="HF", radio_id=2)
        engine._publish_local_ptt_busy_evidence(source="HF", radio_id=3)
        assert busy.get(engine._local_ptt_busy_evidence_id(2)).radio_profile_id == "radio_2"
        assert busy.get(engine._local_ptt_busy_evidence_id(3)).radio_profile_id == "radio_3"
        engine._clear_local_ptt_busy_evidence(radio_id=2)
        assert busy.get(engine._local_ptt_busy_evidence_id(3)).radio_profile_id == "radio_3"
        assert busy.active_for_radio("radio_2") == ()

        engine._publish_shared_ptt_block_evidence(shared, source="HF", radio_id=2)
        engine._publish_shared_ptt_block_evidence(
            {"ptt_group": "AMP-A", "owner_device_profile_id": 2, "reason": "Rig 2 is transmitting."},
            source="HF",
            radio_id=3,
        )
        assert conflicts.active_for_radio("radio_2")
        assert conflicts.active_for_radio("radio_3")
        engine._clear_shared_ptt_block_evidence(radio_id=2)
        # The peer's conflict remains visible to radio 2 as the blocker; only
        # the requested-radio-2 evidence is removed.
        assert [item.id for item in conflicts.active_for_group("AMP-A")] == ["ptt_shared_3"]
        assert conflicts.active_for_radio("radio_2")[0].id == "ptt_shared_3"
        assert conflicts.active_for_radio("radio_3")
        assert busy.get(engine._shared_ptt_busy_evidence_id(3)).radio_profile_id == "radio_3"
    finally:
        _close(engine)


def test_target_rf_evaluator_failure_fails_closed_without_changing_primary_path(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    calls: List[str] = []

    class _Manager:
        def evaluate_rf_conflict_for_device(self, **_kwargs):
            calls.append("target")
            raise RuntimeError("peer status unavailable")

        def evaluate_primary_rf_conflict(self, **_kwargs):
            calls.append("primary")
            return None

    engine.station_runtime_manager = _Manager()
    target = {
        "target_device_profile_id": 2,
        "band": "40M",
        "frequency": "7.115",
    }
    legacy = {"band": "40M", "frequency": "7.115"}
    try:
        targeted = engine._coordination_conflict_status(target, source="HF")
        assert targeted["blocked"] is True
        assert targeted["peer_status_unknown"] is True
        assert targeted["peer_status_stale"] is True
        assert "unavailable" in targeted["summary"].lower()
        assert calls == ["target"]

        primary = engine._coordination_conflict_status(legacy, source="HF")
        assert primary == {}
        assert calls == ["target", "primary"]
    finally:
        _close(engine)
