from __future__ import annotations

from types import SimpleNamespace

import pytest
from PySide6.QtCore import QCoreApplication, QEvent

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core import scheduler_engine as scheduler_engine_module
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.scheduler_manual_control_service import SchedulerManualControlService
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.shared_state import SchedulerManualControlState, SchedulerManualTarget


def _radio(store: MultiRadioStore, name: str) -> dict:
    return store.save_device_profile(
        {
            "name": name,
            "control_backend": "manual",
            "runtime_active": 0,
            "runtime_primary": 0,
        }
    )


def _primary_radio(store: MultiRadioStore, name: str = "DX10") -> dict:
    radio = store.save_device_profile(
        {
            "name": name,
            "control_backend": "manual",
            "runtime_active": 1,
            "runtime_primary": 1,
        }
    )
    return radio


def _shutdown_engine(engine: SchedulerEngine) -> None:
    engine.stop()
    if QCoreApplication.instance() is not None:
        engine.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


def test_scheduler_manual_control_defaults_to_on_schedule_and_creates_schema(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    radio = _radio(store, "DX10")
    service = SchedulerManualControlService(store)

    state = service.get_state(radio_shared_state_id(radio["id"]))

    assert state.radio_profile_id == radio_shared_state_id(radio["id"])
    assert state.state == "on_schedule"
    assert state.manual_target is None
    assert state.hold_until_utc is None
    with store.connect() as conn:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='scheduler_manual_control_states'"
        ).fetchone()
    assert table is not None


def test_scheduler_stop_disconnects_qt_callbacks_and_start_reconnects(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    engine = SchedulerEngine(rig=None, js8=None, varac=None, fldigi_log=None, poll_interval_ms=60_000)
    try:
        assert engine._timer_connected is True
        assert engine._scheduler_thread_call_connected is True

        engine.stop()

        assert engine.timer.isActive() is False
        assert engine._timer_connected is False
        assert engine._scheduler_thread_call_connected is False

        monkeypatch.setattr(engine, "_maybe_refresh_external_status_snapshot", lambda *, force=False: None)
        monkeypatch.setattr(engine, "_apply_js8_offset_startup", lambda: None)
        monkeypatch.setattr(engine, "_evaluate", lambda **_kwargs: None)

        engine.start()

        assert engine._timer_connected is True
        assert engine._scheduler_thread_call_connected is True
    finally:
        _shutdown_engine(engine)


def test_scheduler_rig_frequency_poll_uses_coordinator_cache(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()

    class _Rig:
        def __init__(self) -> None:
            self.values = [7_115_000, 14_115_000]
            self.calls = 0

        def get_vfo_frequency(self):
            self.calls += 1
            return self.values[min(self.calls - 1, len(self.values) - 1)]

    rig = _Rig()
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    try:
        engine._status_poll_ttl_s = 30.0

        assert engine._status_poll_rig_frequency() == 7_115_000
        assert engine._status_poll_rig_frequency() == 7_115_000
        engine._status_poll_coordinator.invalidate("scheduler:primary:rig_frequency")
        assert engine._status_poll_rig_frequency() == 14_115_000
        assert rig.calls == 2
    finally:
        _shutdown_engine(engine)


def test_scheduler_rig_frequency_poll_preserves_cached_value_during_backoff(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()

    class _Rig:
        def __init__(self) -> None:
            self.calls = 0
            self.fail = False

        def get_vfo_frequency(self):
            self.calls += 1
            if self.fail:
                raise RuntimeError("rig read timeout")
            return 7_115_000

    rig = _Rig()
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    try:
        now = [100.0]
        engine._status_poll_coordinator._time_fn = lambda: now[0]
        engine._status_poll_ttl_s = 0.0
        engine._status_poll_retry_s = 5.0

        assert engine._status_poll_rig_frequency() == 7_115_000
        now[0] = 101.0
        rig.fail = True
        assert engine._status_poll_rig_frequency() == 7_115_000
        assert engine._status_flrig_retry_ts == 106.0
        now[0] = 102.0
        assert engine._status_poll_rig_frequency() == 7_115_000
        assert rig.calls == 2
    finally:
        _shutdown_engine(engine)


def test_scheduler_rig_ptt_poll_uses_coordinator_cache(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()

    class _Rig:
        def __init__(self) -> None:
            self.values = [True, False]
            self.calls = 0

        def get_ptt(self):
            self.calls += 1
            return self.values[min(self.calls - 1, len(self.values) - 1)]

    rig = _Rig()
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    try:
        engine._status_poll_ttl_s = 30.0

        assert engine._status_poll_rig_ptt() is True
        assert engine._status_poll_rig_ptt() is True
        engine._status_poll_coordinator.invalidate("scheduler:primary:rig_ptt")
        assert engine._status_poll_rig_ptt() is False
        assert rig.calls == 2
    finally:
        _shutdown_engine(engine)


def test_scheduler_rig_ptt_poll_marks_unknown_during_backoff(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()

    class _Rig:
        def __init__(self) -> None:
            self.calls = 0
            self.fail = False

        def get_ptt(self):
            self.calls += 1
            if self.fail:
                raise RuntimeError("ptt timeout")
            return True

    rig = _Rig()
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    try:
        now = [100.0]
        engine._status_poll_coordinator._time_fn = lambda: now[0]
        engine._status_poll_ttl_s = 0.0
        engine._status_poll_retry_s = 5.0

        assert engine._status_poll_rig_ptt() is True
        assert engine._status_flrig_ptt_known is True
        now[0] = 101.0
        rig.fail = True

        assert engine._status_poll_rig_ptt() is False
        assert engine._status_flrig_ptt_known is False
        assert engine._status_flrig_retry_ts == 106.0

        now[0] = 102.0
        assert engine._status_poll_rig_ptt() is False
        assert engine._status_flrig_ptt_known is False
        assert rig.calls == 2
    finally:
        _shutdown_engine(engine)


def test_scheduler_forced_actual_state_uses_coordinator_status_helpers(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()

    class _Rig:
        def __init__(self) -> None:
            self.freq_calls = 0
            self.ptt_calls = 0

        def get_vfo_frequency(self):
            self.freq_calls += 1
            return 7_115_000

        def get_ptt(self):
            self.ptt_calls += 1
            return True

    rig = _Rig()
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    try:
        state = engine._read_station_actual_state(force=True, control_mode="FLRIG")

        assert state.flrig_freq_hz == 7_115_000
        assert state.flrig_ptt_active is True
        assert state.flrig_ptt_known is True
        assert state.flrig_ptt_stale is False
        assert rig.freq_calls == 1
        assert rig.ptt_calls == 1
        assert engine._status_poll_coordinator.latest_snapshot("scheduler:primary:rig_frequency") is not None
        assert engine._status_poll_coordinator.latest_snapshot("scheduler:primary:rig_ptt") is not None
    finally:
        _shutdown_engine(engine)


def test_repeated_qsy_updates_manual_target_and_preserves_hold_for_one_radio(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    first = _radio(store, "DX10")
    second = _radio(store, "IC-7300")
    service = SchedulerManualControlService(store)
    first_id = radio_shared_state_id(first["id"])
    second_id = radio_shared_state_id(second["id"])

    service.hold(first_id, hold_until_utc="2026-07-26T21:00:00Z", reason_code="operator_hold")
    first_qsy = service.set_manual_qsy(
        first_id,
        SchedulerManualTarget(frequency_hz=7_268_000, mode="LSB", vfo="A", source_action="qsy"),
        operator_source="controlfreq",
    )
    second_state = service.get_state(second_id)
    second_qsy = service.set_manual_qsy(
        second_id,
        SchedulerManualTarget(frequency_hz=14_300_000, mode="USB", vfo="A", source_action="qsy"),
        hold_until_utc="2026-07-26T22:00:00Z",
        operator_source="main_control_center",
    )
    repeated_qsy = service.set_manual_qsy(
        first_id,
        SchedulerManualTarget(frequency_hz=3_900_000, mode="LSB", vfo="B", source_action="qsy"),
        operator_source="controlfreq",
    )

    assert first_qsy.state == "manual_qsy"
    assert first_qsy.hold_until_utc == "2026-07-26T21:00:00Z"
    assert repeated_qsy.manual_target is not None
    assert repeated_qsy.manual_target.frequency_hz == 3_900_000
    assert repeated_qsy.manual_target.vfo == "B"
    assert repeated_qsy.hold_until_utc == "2026-07-26T21:00:00Z"
    assert second_state.state == "on_schedule"
    assert second_qsy.hold_until_utc == "2026-07-26T22:00:00Z"
    assert service.get_state(second_id).manual_target is not None
    assert service.get_state(second_id).manual_target.frequency_hz == 14_300_000


def test_resume_clears_only_selected_radio_manual_control(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    first = _radio(store, "DX10")
    second = _radio(store, "IC-7300")
    service = SchedulerManualControlService(store)
    first_id = radio_shared_state_id(first["id"])
    second_id = radio_shared_state_id(second["id"])

    service.set_manual_qsy(
        first_id,
        SchedulerManualTarget(frequency_hz=7_268_000, mode="LSB", source_action="qsy"),
        hold_until_utc="2026-07-26T21:00:00Z",
    )
    service.suspend(second_id, reason_code="operator_suspend")

    resumed = service.resume(first_id, latest_event_id="event_1")

    assert resumed.state == "on_schedule"
    assert resumed.manual_target is None
    assert resumed.hold_until_utc is None
    assert resumed.latest_event_id == "event_1"
    assert service.get_state(second_id).state == "manual_suspend"
    assert tuple(state.radio_profile_id for state in service.list_active_states()) == (second_id,)


def test_scheduler_manual_control_rejects_unknown_radio(tmp_path) -> None:
    service = SchedulerManualControlService(MultiRadioStore(tmp_path / "freqinout.db"))

    with pytest.raises(KeyError, match="Unknown radio id"):
        service.get_state("radio_999")


def test_manual_qsy_requires_target_frequency_for_all_write_paths(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    radio = _radio(store, "DX10")
    radio_id = radio_shared_state_id(radio["id"])
    service = SchedulerManualControlService(store)

    with pytest.raises(ValueError, match="target frequency"):
        service.set_manual_qsy(radio_id, SchedulerManualTarget())

    with pytest.raises(ValueError, match="target frequency"):
        service.save_state(
            SchedulerManualControlState(
                radio_profile_id=radio_id,
                state="manual_qsy",
                manual_target=SchedulerManualTarget(frequency_hz=0),
            )
        )

    assert service.get_state(radio_id).state == "on_schedule"
    assert service.list_active_states() == ()


def test_scheduler_manual_qsy_persists_primary_radio_manual_state(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None
    radio_id = radio_shared_state_id(primary["id"])
    service = SchedulerManualControlService(store)
    engine = SchedulerEngine(rig=None, js8=None, varac=None, fldigi_log=None)
    try:
        engine.apply_manual_qsy({"frequency": "7.268", "mode": "LSB", "vfo": "B"})

        state = service.get_state(radio_id)
        assert state.state == "manual_qsy"
        assert state.manual_target is not None
        assert state.manual_target.frequency_hz == 7_268_000
        assert state.manual_target.mode == "LSB"
        assert state.manual_target.vfo == "B"
        assert state.reason_code == "operator_qsy"
    finally:
        _shutdown_engine(engine)


def test_scheduler_manual_qsy_waiting_on_rf_conflict_does_not_persist_manual_state(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None
    radio_id = radio_shared_state_id(primary["id"])
    service = SchedulerManualControlService(store)

    class _Rig:
        def get_vfo_frequency(self):
            return 7_074_000

        def get_ptt(self):
            return False

    class _Manager:
        def evaluate_primary_rf_conflict(self, *, target_band="", target_frequency_hz=None, source="", force=False):
            return SimpleNamespace(
                peer_device_profile_id=22,
                peer_name="Remote Rig",
                peer_band="40M",
                peer_frequency_hz=7_074_000,
                target_band=target_band,
                target_frequency_hz=target_frequency_hz,
                same_band=True,
                same_frequency=False,
                shared_antenna_groups=["ANT-1"],
                shared_amplifier_groups=[],
                shared_frontend_groups=[],
                summary="RF conflict: Remote Rig on same band 40M via antenna ANT-1.",
                detail="Target 7.078 MHz 40M overlaps Remote Rig at 7.074 MHz 40M.",
                signature="1|QSY|40M|7078000|22|ANT-1||",
            )

    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None, station_runtime_manager=_Manager())
    try:
        queued: list[tuple[str, int]] = []
        emitted: list[dict[str, object]] = []
        engine.coordination_conflict_detected.connect(lambda payload: emitted.append(dict(payload)))
        monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
        monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)

        engine.apply_manual_qsy({"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"})

        state = service.get_state(radio_id)
        assert queued == []
        assert len(emitted) == 1
        assert state.state == "on_schedule"
        assert state.manual_target is None
    finally:
        _shutdown_engine(engine)


def test_scheduler_blocks_schedule_transition_for_unsupported_antenna_band(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None
    store.save_device_profile(
        {
            "id": int(primary["id"]),
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "block",
        }
    )

    class _Rig:
        def get_vfo_frequency(self):
            return 14_070_000

        def get_ptt(self):
            return False

    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None)
    try:
        queued: list[tuple[str, int]] = []
        monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
        monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)

        engine._apply_schedule_entry({"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"}, "HF")

        assert queued == []
        status = engine.get_status_summary()
        assert status["rf_conflict_warning"] is True
        assert "antenna is not configured for 40M" in status["rf_conflict_summary"]
    finally:
        _shutdown_engine(engine)


def test_scheduler_infers_band_for_unsupported_antenna_guard_when_band_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None
    store.save_device_profile(
        {
            "id": int(primary["id"]),
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "block",
        }
    )

    class _Rig:
        def get_vfo_frequency(self):
            return 14_070_000

        def get_ptt(self):
            return False

    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None)
    try:
        queued: list[tuple[str, int]] = []
        monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
        monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)

        engine._apply_schedule_entry({"frequency": "7.078", "mode": "Digi", "vfo": "A"}, "HF")

        assert queued == []
        status = engine.get_status_summary()
        assert status["rf_conflict_warning"] is True
        assert "antenna is not configured for 40M" in status["rf_conflict_summary"]
    finally:
        _shutdown_engine(engine)


def test_scheduler_warn_only_rf_guard_continues_schedule_transition(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None

    class _Rig:
        def get_vfo_frequency(self):
            return 14_070_000

        def get_ptt(self):
            return False

    class _Manager:
        def evaluate_primary_rf_conflict(self, *, target_band="", target_frequency_hz=None, source="", force=False):
            return SimpleNamespace(
                peer_device_profile_id=22,
                peer_name="Remote Rig",
                peer_band="40M",
                peer_frequency_hz=7_074_000,
                target_band=target_band,
                target_frequency_hz=target_frequency_hz,
                same_band=True,
                same_frequency=False,
                shared_antenna_groups=[],
                shared_amplifier_groups=[],
                shared_frontend_groups=[],
                shared_band_overlap_groups=["NORTH MAST"],
                guard_mode="warn",
                blocked=False,
                summary="RF Safety Guard: Remote Rig shares 40M.",
                detail="Warn only should not hold schedule automation.",
                signature="1|HF|40M|7078000|22|NORTH-MAST|warn",
            )

    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None, station_runtime_manager=_Manager())
    try:
        emitted: list[dict[str, object]] = []
        queued: list[tuple[str, int]] = []
        events: list[dict[str, object]] = []
        engine.coordination_conflict_detected.connect(lambda payload: emitted.append(dict(payload)))
        monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
        monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)
        monkeypatch.setattr(scheduler_engine_module, "record_scheduler_event", lambda **kwargs: events.append(dict(kwargs)))

        engine._apply_schedule_entry({"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"}, "HF")

        assert emitted == []
        assert queued == [("HF", 7_078_000)]
        warning_events = [event for event in events if event.get("code") == "rf_safety_guard_warning"]
        assert len(warning_events) == 1
        assert warning_events[0]["action"] == "Continuing schedule change after RF Safety Guard warn-only notice"
        assert warning_events[0]["detail"] == "Warn only should not hold schedule automation."
    finally:
        _shutdown_engine(engine)


def test_scheduler_rf_safety_block_does_not_require_provider_signature(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None

    class _Rig:
        def get_vfo_frequency(self):
            return 14_070_000

        def get_ptt(self):
            return False

    class _Manager:
        def evaluate_primary_rf_conflict(self, *, target_band="", target_frequency_hz=None, source="", force=False):
            return SimpleNamespace(
                peer_device_profile_id=22,
                peer_name="Protected Receiver",
                peer_band="40M",
                peer_frequency_hz=7_074_000,
                target_band=target_band,
                target_frequency_hz=target_frequency_hz,
                same_band=True,
                same_frequency=False,
                shared_antenna_groups=[],
                shared_amplifier_groups=[],
                shared_frontend_groups=[],
                shared_band_overlap_groups=["NORTH MAST"],
                guard_mode="block",
                blocked=True,
                summary="RF Safety Guard: Protected Receiver blocks same-band overlap.",
                detail="Prevent Band Overlap group NORTH MAST is blocking this change.",
                signature="",
            )

    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None, station_runtime_manager=_Manager())
    try:
        queued: list[tuple[str, int]] = []
        events: list[dict[str, object]] = []
        monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
        monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)
        monkeypatch.setattr(scheduler_engine_module, "record_scheduler_event", lambda **kwargs: events.append(dict(kwargs)))

        engine._apply_schedule_entry({"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"}, "HF")

        assert queued == []
        block_events = [event for event in events if event.get("code") == "rf_safety_guard_block"]
        assert len(block_events) == 1
        assert block_events[0]["action"] == "Blocked schedule change by RF Safety Guard"
        assert block_events[0]["detail"] == "Prevent Band Overlap group NORTH MAST is blocking this change."
        assert block_events[0]["metadata"]["signature"]
    finally:
        _shutdown_engine(engine)


def test_scheduler_peer_block_wins_over_local_antenna_warning(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None
    store.save_device_profile(
        {
            "id": int(primary["id"]),
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "warn",
        }
    )

    class _Rig:
        def get_vfo_frequency(self):
            return 14_070_000

        def get_ptt(self):
            return False

    class _Manager:
        def evaluate_primary_rf_conflict(self, *, target_band="", target_frequency_hz=None, source="", force=False):
            return SimpleNamespace(
                peer_device_profile_id=22,
                peer_name="Protected Receiver",
                peer_band="40M",
                peer_frequency_hz=7_074_000,
                target_band=target_band,
                target_frequency_hz=target_frequency_hz,
                same_band=True,
                same_frequency=False,
                shared_antenna_groups=[],
                shared_amplifier_groups=[],
                shared_frontend_groups=[],
                shared_band_overlap_groups=["NORTH MAST"],
                guard_mode="block",
                blocked=True,
                summary="RF Safety Guard: Protected Receiver blocks same-band overlap.",
                detail="Prevent Band Overlap group NORTH MAST is blocking this change.",
                signature="1|HF|40M|7078000|22|NORTH-MAST|block",
            )

    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None, station_runtime_manager=_Manager())
    try:
        queued: list[tuple[str, int]] = []
        monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
        monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)

        engine._apply_schedule_entry({"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"}, "HF")

        assert queued == []
        status = engine.get_status_summary()
        assert status["rf_conflict_warning"] is True
        assert status["rf_conflict_summary"] == "RF Safety Guard: Protected Receiver blocks same-band overlap."
        assert "Prevent Band Overlap group NORTH MAST" in status["rf_conflict_detail"]
    finally:
        _shutdown_engine(engine)


def test_scheduler_hold_and_resume_update_primary_radio_manual_state(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None
    radio_id = radio_shared_state_id(primary["id"])
    service = SchedulerManualControlService(store)
    engine = SchedulerEngine(rig=None, js8=None, varac=None, fldigi_log=None)
    try:
        engine.suspend_schedule(15)

        held = service.get_state(radio_id)
        assert held.state == "manual_hold"
        assert held.hold_until_utc
        assert held.reason_code == "operator_hold"

        engine.resume_schedule()

        resumed = service.get_state(radio_id)
        assert resumed.state == "on_schedule"
        assert resumed.manual_target is None
        assert resumed.hold_until_utc is None
    finally:
        _shutdown_engine(engine)


def test_scheduler_resume_rf_safety_block_does_not_clear_hold(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None

    engine = SchedulerEngine(rig=None, js8=None, varac=None, fldigi_log=None)
    try:
        engine.current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}
        engine.current_source = "HF"
        set_calls: list[tuple[str, object]] = []
        original_set = engine.settings.set

        def _record_set(key, value):
            set_calls.append((str(key), value))
            original_set(key, value)

        monkeypatch.setattr(engine.settings, "set", _record_set)
        monkeypatch.setattr(
            engine,
            "_coordination_conflict_status",
            lambda entry, source="", force=False: {
                "warning": True,
                "blocked": True,
                "guard_mode": "block",
                "signature": "resume|north-mast|40m",
                "summary": "RF Safety Guard: Protected Receiver blocks same-band overlap.",
                "detail": "Prevent Band Overlap group NORTH MAST is blocking this resume.",
            },
        )

        result = engine.resume_schedule()

        assert result is False
        assert not any(key == "schedule_suspend_until" and value == 0 for key, value in set_calls)
    finally:
        _shutdown_engine(engine)


def test_scheduler_resume_rf_safety_block_does_not_require_provider_signature(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile() or _primary_radio(store)
    assert primary is not None

    engine = SchedulerEngine(rig=None, js8=None, varac=None, fldigi_log=None)
    try:
        engine.current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}
        engine.current_source = "HF"
        set_calls: list[tuple[str, object]] = []
        events: list[dict[str, object]] = []
        original_set = engine.settings.set

        def _record_set(key, value):
            set_calls.append((str(key), value))
            original_set(key, value)

        monkeypatch.setattr(engine.settings, "set", _record_set)
        monkeypatch.setattr(scheduler_engine_module, "record_scheduler_event", lambda **kwargs: events.append(dict(kwargs)))
        monkeypatch.setattr(
            engine,
            "_coordination_conflict_status",
            lambda entry, source="", force=False: {
                "warning": True,
                "blocked": True,
                "guard_mode": "block",
                "signature": "",
                "summary": "RF Safety Guard: Protected Receiver blocks same-band overlap.",
                "detail": "Prevent Band Overlap group NORTH MAST is blocking this resume.",
            },
        )

        result = engine.resume_schedule()

        assert result is False
        assert not any(key == "schedule_suspend_until" and value == 0 for key, value in set_calls)
        block_events = [event for event in events if event.get("code") == "rf_safety_guard_block"]
        assert len(block_events) == 1
        assert block_events[0]["action"] == "Blocked resume by RF Safety Guard"
        assert block_events[0]["detail"] == "Prevent Band Overlap group NORTH MAST is blocking this resume."
        assert block_events[0]["metadata"]["signature"]
    finally:
        _shutdown_engine(engine)
