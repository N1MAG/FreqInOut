from __future__ import annotations

import datetime
from concurrent.futures import Future
from types import SimpleNamespace


def test_scheduler_radio_scoped_suspend_ignores_legacy_global_hold() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine
    from freqinout.core.shared_state import SchedulerManualControlState

    future_ts = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)).timestamp()

    class FakeManualControlService:
        def get_state(self, radio_id: int):
            return SchedulerManualControlState(radio_profile_id=f"radio_{radio_id}", state="on_schedule")

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.settings = SimpleNamespace(get=lambda key, default=None: future_ts if key == "schedule_suspend_until" else default)
    scheduler._manual_control_service = FakeManualControlService()
    scheduler._manual_qsy_radio_id = None

    suspended, until = SchedulerEngine._scheduling_suspended_for_radio(
        scheduler,
        8,
        datetime.datetime.now(datetime.timezone.utc),
    )

    assert suspended is False
    assert until is None


def test_scheduler_radio_scoped_manual_suspend_blocks_only_that_radio() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine
    from freqinout.core.shared_state import SchedulerManualControlState

    class FakeManualControlService:
        def get_state(self, radio_id: int):
            state = "manual_suspend" if int(radio_id) == 8 else "on_schedule"
            return SchedulerManualControlState(radio_profile_id=f"radio_{radio_id}", state=state)

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.settings = SimpleNamespace(get=lambda _key, default=None: default)
    scheduler._manual_control_service = FakeManualControlService()
    scheduler._manual_qsy_radio_id = None
    now = datetime.datetime.now(datetime.timezone.utc)

    assert SchedulerEngine._scheduling_suspended_for_radio(scheduler, 8, now) == (True, None)
    assert SchedulerEngine._scheduling_suspended_for_radio(scheduler, 9, now) == (False, None)


def test_targeted_qsy_hold_does_not_update_legacy_suspend_cache(monkeypatch) -> None:
    from freqinout.gui import qsy_helper

    class FakeScheduler:
        def __init__(self) -> None:
            self.calls = []

        def suspend_schedule(self, minutes, *, target_device_profile_id=None):
            self.calls.append((minutes, target_device_profile_id))

    class FailOnLegacySettings:
        def set(self, key, value):
            raise AssertionError(f"targeted hold should not write legacy setting {key}")

    scheduler = FakeScheduler()
    window = SimpleNamespace(scheduler=scheduler)
    qsy_helper._SUSPEND_CACHE["ts"] = None
    qsy_helper._SUSPEND_CACHE["loaded_at"] = 0.0

    mins = qsy_helper.suspend_schedule_hold(
        window,
        FailOnLegacySettings(),
        30,
        warn_rf_conflict=False,
        target_device_profile_id=8,
    )

    assert mins == 30
    assert scheduler.calls == [(30, 8)]
    assert qsy_helper._SUSPEND_CACHE["ts"] is None


def test_targeted_resume_applies_target_radio_lane_not_shared_current_entry() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    class FakeManualControlService:
        def __init__(self) -> None:
            self.resumed = []

        def resume(self, radio_id: int) -> None:
            self.resumed.append(int(radio_id))

    class FakeSignal:
        def emit(self, *args, **kwargs) -> None:
            return None

    applied: list[tuple[dict[str, object], str, dict[str, object]]] = []
    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.current_source = "HF"
    scheduler.current_schedule_entry = {
        "frequency": "14.110",
        "band": "20M",
        "group_name": "AMRRON",
        "target_device_profile_id": 9,
    }
    scheduler._manual_qsy_active = False
    scheduler._manual_qsy_entry_key = None
    scheduler._manual_qsy_radio_id = None
    scheduler._manual_control_service = FakeManualControlService()
    scheduler._coordination_conflict_status = lambda *args, **kwargs: {}
    scheduler._coordination_conflict_signature = lambda _conflict: ""
    scheduler._clear_coordination_prompt = lambda: None
    scheduler._reset_prompt_timers = lambda: None
    scheduler._record_scheduler_event = lambda *args, **kwargs: None
    scheduler._reset_control_if_running = lambda _reason: None
    scheduler._control_future_stuck = lambda: False
    scheduler._maybe_apply_fldigi = lambda: None
    scheduler._schedule_forced_retry = lambda: None
    scheduler.settings = SimpleNamespace(set=lambda *args, **kwargs: None)
    scheduler.active_entry_changed = FakeSignal()
    scheduler._prompt_active = False
    scheduler._prompt_items = []
    scheduler._prompt_entry_key = None
    scheduler._latest_intent = None
    scheduler._latest_intent_ts = 0.0
    scheduler._retry_scheduled = False
    scheduler._control_backoff_until = 0.0
    scheduler._control_fail_count = 0
    scheduler._pending_entry_key = None
    scheduler._force_retry_after_control = False
    scheduler._forced_retry_attempts_left = 0
    scheduler._net_resume_apply_once = False
    scheduler.active_schedule_lanes = lambda force=False: [
        {
            "device_profile_id": 8,
            "current_source": "HF",
            "current_entry": {
                "frequency": "14.115",
                "band": "20M",
                "group_name": "MAGNET",
            },
        },
        {
            "device_profile_id": 9,
            "current_source": "HF",
            "current_entry": {
                "frequency": "14.110",
                "band": "20M",
                "group_name": "AMRRON",
            },
        },
    ]

    def fake_apply(entry, source, **kwargs):
        applied.append((dict(entry), source, dict(kwargs)))

    scheduler._apply_schedule_entry = fake_apply

    assert SchedulerEngine.resume_schedule(
        scheduler,
        target_device_profile_id=8,
        ignore_coordination_prompt=True,
    ) is True

    assert scheduler._manual_control_service.resumed == [8]
    assert len(applied) == 1
    entry, source, kwargs = applied[0]
    assert source == "HF"
    assert entry["group_name"] == "MAGNET"
    assert entry["frequency"] == "14.115"
    assert entry["target_device_profile_id"] == 8
    assert kwargs["ignore_suspend"] is True
    assert scheduler.current_schedule_entry["group_name"] == "AMRRON"


def test_active_schedule_lanes_apply_each_radio_row_without_singleton_fallback() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine
    from freqinout.core.shared_state import SchedulerManualControlState

    class FakeManualControlService:
        def get_state(self, radio_id: int):
            return SchedulerManualControlState(radio_profile_id=f"radio_{radio_id}", state="on_schedule")

    applied: list[tuple[dict[str, object], str, dict[str, object]]] = []
    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler._manual_control_service = FakeManualControlService()
    scheduler.settings = SimpleNamespace(get=lambda _key, default=None: default)
    scheduler.current_schedule_entry = {
        "group_name": "LEGACY",
        "frequency": "7.000",
        "target_device_profile_id": 99,
    }
    scheduler.active_schedule_lanes = lambda force=False, now_utc=None: [
        {
            "device_profile_id": 8,
            "current_source": "HF",
            "current_entry": {
                "group_name": "MAGNET",
                "band": "20M",
                "frequency": "14.115",
            },
        },
        {
            "device_profile_id": 9,
            "current_source": "HF",
            "current_entry": {
                "group_name": "AMRRON",
                "band": "20M",
                "frequency": "14.110",
            },
        },
    ]
    scheduler._apply_schedule_entry = lambda entry, source, **kwargs: applied.append(
        (dict(entry), source, dict(kwargs))
    )

    handled = SchedulerEngine._apply_active_schedule_lanes(
        scheduler,
        now_utc=datetime.datetime.now(datetime.timezone.utc),
        force=True,
    )

    assert handled is True
    assert [(entry["target_device_profile_id"], entry["group_name"], source) for entry, source, _kwargs in applied] == [
        (8, "MAGNET", "HF"),
        (9, "AMRRON", "HF"),
    ]
    assert all(kwargs["ignore_wait_prompt"] is True for _entry, _source, kwargs in applied)
    assert all(kwargs["ignore_coordination_prompt"] is True for _entry, _source, kwargs in applied)
    assert all(kwargs["ignore_js8_busy"] is True for _entry, _source, kwargs in applied)
    assert all(kwargs["ignore_varac_busy"] is True for _entry, _source, kwargs in applied)
    assert all(kwargs["ignore_fldigi_busy"] is True for _entry, _source, kwargs in applied)


def test_off_schedule_frequency_apply_bypasses_busy_gates_for_target_radio() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    applied: list[tuple[dict[str, object], str, dict[str, object]]] = []
    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.current_schedule_entry = {}
    scheduler.current_source = "HF"
    scheduler._prompt_active = True
    scheduler._prompt_items = ["Frequency"]
    scheduler._active_schedule_entry_for_radio = lambda _radio_id, force=False: (
        "HF",
        {"group_name": "AMRRON", "band": "20M", "frequency": "14.110"},
    )
    scheduler._reset_prompt_timers = lambda **_kwargs: None
    scheduler._apply_schedule_entry = lambda entry, source, **kwargs: applied.append(
        (dict(entry), source, dict(kwargs))
    )

    SchedulerEngine.resolve_off_schedule(
        scheduler,
        "apply",
        items=["Frequency"],
        target_device_profile_id=9,
    )

    assert len(applied) == 1
    entry, source, kwargs = applied[0]
    assert source == "HF"
    assert entry["target_device_profile_id"] == 9
    assert kwargs["force"] is True
    assert kwargs["ignore_suspend"] is True
    assert kwargs["ignore_js8_busy"] is True
    assert kwargs["ignore_varac_busy"] is True
    assert kwargs["ignore_fldigi_busy"] is True


def test_per_radio_flrig_clients_use_configured_ports() -> None:
    from freqinout.core.station_runtime_manager import DeviceSettingsProxy
    from freqinout.radio_interface.rigctl_client import rig_control_client_from_settings

    fallback = SimpleNamespace(get=lambda _key, default=None: default)
    radio_a = DeviceSettingsProxy(
        {"control_backend": "flrig", "flrig_host": "127.0.0.1", "flrig_port": 12345},
        fallback,
    )
    radio_b = DeviceSettingsProxy(
        {"control_backend": "flrig", "flrig_host": "127.0.0.1", "flrig_port": 12346},
        fallback,
    )

    client_a = rig_control_client_from_settings(radio_a)
    client_b = rig_control_client_from_settings(radio_b)

    assert client_a is not None
    assert client_b is not None
    assert client_a is not client_b
    assert client_a.port == 12345
    assert client_b.port == 12346


def test_scheduler_control_context_uses_target_radio_runtime_clients() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    class FakeManager:
        def __init__(self) -> None:
            self.runtime = SimpleNamespace(
                rig_client=SimpleNamespace(name="rig-b"),
                js8_control_client=SimpleNamespace(name="js8-b"),
                varac_status_client=SimpleNamespace(name="varac-b"),
                settings_proxy=SimpleNamespace(get=lambda key, default=None: "FLRig" if key == "control_via" else default),
            )

        def get_runtime_for_device(self, device_profile_id: int):
            return self.runtime if int(device_profile_id) == 2 else None

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = SimpleNamespace(name="rig-a")
    scheduler.js8 = SimpleNamespace(name="js8-a")
    scheduler.varac = SimpleNamespace(name="varac-a")
    scheduler.settings = SimpleNamespace(get=lambda _key, default=None: default)
    scheduler.station_runtime_manager = FakeManager()

    rig, js8, varac, settings, radio_id = SchedulerEngine._control_context_for_entry(
        scheduler,
        {"target_device_profile_id": 2, "frequency": "14.110"},
    )

    assert radio_id == 2
    assert rig.name == "rig-b"
    assert js8.name == "js8-b"
    assert varac.name == "varac-b"
    assert settings.get("control_via") == "FLRig"


def test_scheduler_control_context_does_not_fallback_when_target_runtime_missing() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    class FakeManager:
        def get_runtime_for_device(self, device_profile_id: int):
            return None

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = SimpleNamespace(name="rig-a")
    scheduler.js8 = SimpleNamespace(name="js8-a")
    scheduler.varac = SimpleNamespace(name="varac-a")
    scheduler.settings = SimpleNamespace(get=lambda _key, default=None: default)
    scheduler.station_runtime_manager = FakeManager()

    rig, js8, varac, settings, radio_id = SchedulerEngine._control_context_for_entry(
        scheduler,
        {"target_device_profile_id": 2, "frequency": "14.110"},
    )

    assert radio_id == 2
    assert rig is None
    assert js8 is None
    assert varac is None
    assert settings is scheduler.settings


def test_scheduler_control_context_builds_target_client_from_store_when_manager_missing(monkeypatch) -> None:
    import freqinout.core.scheduler_engine as scheduler_mod
    from freqinout.core.scheduler_engine import SchedulerEngine

    class FakeStore:
        def __init__(self, _path) -> None:
            pass

        def get_device_profile(self, device_profile_id: int):
            if int(device_profile_id) != 9:
                return None
            return {
                "id": 9,
                "name": "FIO-B",
                "control_backend": "flrig",
                "flrig_host": "127.0.0.1",
                "flrig_port": 12346,
            }

    def fake_client_from_settings(settings):
        return SimpleNamespace(
            name="profile-rig-b",
            host=settings.get("flrig_host"),
            port=int(settings.get("flrig_port")),
        )

    monkeypatch.setattr(scheduler_mod, "MultiRadioStore", FakeStore)
    monkeypatch.setattr(scheduler_mod, "rig_control_client_from_settings", fake_client_from_settings)

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = SimpleNamespace(name="global-rig", port=12345)
    scheduler.js8 = SimpleNamespace(name="global-js8")
    scheduler.varac = SimpleNamespace(name="global-varac")
    scheduler.settings = SimpleNamespace(get=lambda _key, default=None: default)
    scheduler.station_runtime_manager = None

    rig, js8, varac, settings, radio_id = SchedulerEngine._control_context_for_entry(
        scheduler,
        {"target_device_profile_id": 9, "frequency": "14.110"},
    )

    assert radio_id == 9
    assert rig.name == "profile-rig-b"
    assert rig.port == 12346
    assert rig is not scheduler.rig
    assert js8 is None
    assert varac is None
    assert settings.get("control_via") == "FLRig"
    assert settings.get("flrig_port") == 12346


def test_scheduler_control_context_builds_target_client_from_store_when_runtime_missing(monkeypatch) -> None:
    import freqinout.core.scheduler_engine as scheduler_mod
    from freqinout.core.scheduler_engine import SchedulerEngine

    class FakeManager:
        def get_runtime_for_device(self, device_profile_id: int):
            return None

    class FakeStore:
        def __init__(self, _path) -> None:
            pass

        def get_device_profile(self, device_profile_id: int):
            if int(device_profile_id) != 8:
                return None
            return {
                "id": 8,
                "name": "FIO-A",
                "control_backend": "flrig",
                "flrig_host": "127.0.0.1",
                "flrig_port": 12345,
            }

    def fake_client_from_settings(settings):
        return SimpleNamespace(port=int(settings.get("flrig_port")))

    monkeypatch.setattr(scheduler_mod, "MultiRadioStore", FakeStore)
    monkeypatch.setattr(scheduler_mod, "rig_control_client_from_settings", fake_client_from_settings)

    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = SimpleNamespace(name="global-rig", port=12346)
    scheduler.js8 = None
    scheduler.varac = None
    scheduler.settings = SimpleNamespace(get=lambda _key, default=None: default)
    scheduler.station_runtime_manager = FakeManager()

    rig, _js8, _varac, _settings, radio_id = SchedulerEngine._control_context_for_entry(
        scheduler,
        {"target_device_profile_id": 8, "frequency": "7.115"},
    )

    assert radio_id == 8
    assert rig.port == 12345
    assert rig is not scheduler.rig


def test_scheduler_queue_control_action_dispatches_to_target_rig_client() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    class FakeRig:
        def __init__(self, name: str) -> None:
            self.name = name
            self.commands = []

        def set_frequency(self, command) -> bool:
            self.commands.append(command)
            return True

    class ImmediateExecutor:
        def submit(self, callback):
            future: Future = Future()
            try:
                future.set_result(callback())
            except Exception as exc:
                future.set_exception(exc)
            return future

    fallback_rig = FakeRig("fallback")
    target_rig = FakeRig("target")
    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = fallback_rig
    scheduler.js8 = None
    scheduler._shutdown_requested = False
    scheduler._control_future = None
    scheduler._pending_entry_key = None
    scheduler._control_future_token = 0
    scheduler._control_fail_count = 0
    scheduler._control_backoff_until = 0.0
    scheduler._latest_intent = None
    scheduler._force_retry_after_control = False
    scheduler._control_can_attempt = lambda: True
    scheduler._control_future_stuck = lambda: False
    scheduler._reset_control_executor = lambda _reason: None
    scheduler._record_scheduler_event = lambda *args, **kwargs: None
    scheduler._record_scheduler_health_issue = lambda *args, **kwargs: None
    scheduler._clear_scheduler_health_issue = lambda *args, **kwargs: None
    scheduler._clear_fldigi_busy_check_state = lambda: None
    scheduler._queue_post_apply_verification = lambda *args, **kwargs: None
    scheduler._queue_scheduler_thread_call = lambda callback: callback()
    scheduler._control_executor = ImmediateExecutor()

    queued = SchedulerEngine._queue_control_action(
        scheduler,
        control_mode="FLRIG",
        rig_client=target_rig,
        js8_client=None,
        entry_key=("AMRRON", 14110000),
        source="QSY",
        freq_hz=14_110_000,
        band="20M",
        mode="USB",
        vfo="A",
        auto_tune=False,
        js8_offset=None,
        js8_group="",
    )

    assert queued is True
    assert len(target_rig.commands) == 1
    assert target_rig.commands[0].rig_hz == 14_110_000
    assert fallback_rig.commands == []


def test_scheduler_manual_qsy_resolves_target_runtime_before_queue(monkeypatch) -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    fallback_rig = SimpleNamespace(name="rig-a")
    target_rig = SimpleNamespace(name="rig-b")

    class FakeManager:
        def get_runtime_for_device(self, device_profile_id: int):
            if int(device_profile_id) != 2:
                return None
            return SimpleNamespace(
                rig_client=target_rig,
                js8_control_client=None,
                varac_status_client=None,
                settings_proxy=SimpleNamespace(get=lambda key, default=None: "FLRig" if key == "control_via" else default),
            )

    scheduler = SchedulerEngine(rig=fallback_rig, js8=None, varac=None, fldigi_log=None, station_runtime_manager=FakeManager())
    try:
        queued: list[dict[str, object]] = []
        monkeypatch.setattr(scheduler, "_flrig_running", lambda: True)
        monkeypatch.setattr(scheduler, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(scheduler, "_shared_ptt_lock_status", lambda force=False: {"blocked": False})
        monkeypatch.setattr(scheduler, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(scheduler, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(scheduler, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(scheduler, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(scheduler, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(scheduler, "_coordination_conflict_status", lambda *args, **kwargs: {})
        monkeypatch.setattr(scheduler, "_queue_control_action", lambda **kwargs: queued.append(dict(kwargs)) or True)

        scheduler.apply_manual_qsy(
            {
                "target_device_profile_id": 2,
                "frequency": "14.110",
                "band": "20M",
                "mode": "Digi",
            }
        )

        assert len(queued) == 1
        assert queued[0]["rig_client"] is target_rig
        assert queued[0]["rig_client"] is not fallback_rig
        assert queued[0]["allow_global_fallback"] is False
        assert queued[0]["freq_hz"] == 14_110_000
    finally:
        scheduler.stop()


def test_scheduler_manual_qsy_uses_profile_client_when_manager_missing(monkeypatch) -> None:
    import freqinout.core.scheduler_engine as scheduler_mod
    from freqinout.core.scheduler_engine import SchedulerEngine

    fallback_rig = SimpleNamespace(name="global-rig", port=12345)
    target_rig = SimpleNamespace(name="profile-rig-b", port=12346)

    class FakeStore:
        def __init__(self, _path) -> None:
            pass

        def get_device_profile(self, device_profile_id: int):
            if int(device_profile_id) != 9:
                return None
            return {
                "id": 9,
                "name": "FIO-B",
                "control_backend": "flrig",
                "flrig_host": "127.0.0.1",
                "flrig_port": 12346,
            }

    monkeypatch.setattr(scheduler_mod, "MultiRadioStore", FakeStore)
    monkeypatch.setattr(scheduler_mod, "rig_control_client_from_settings", lambda _settings: target_rig)

    scheduler = SchedulerEngine(rig=fallback_rig, js8=None, varac=None, fldigi_log=None, station_runtime_manager=None)
    try:
        queued: list[dict[str, object]] = []
        monkeypatch.setattr(scheduler, "_flrig_running", lambda: True)
        monkeypatch.setattr(scheduler, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(scheduler, "_shared_ptt_lock_status", lambda force=False: {"blocked": False})
        monkeypatch.setattr(scheduler, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(scheduler, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(scheduler, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(scheduler, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(scheduler, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(scheduler, "_coordination_conflict_status", lambda *args, **kwargs: {})
        monkeypatch.setattr(scheduler, "_queue_control_action", lambda **kwargs: queued.append(dict(kwargs)) or True)

        scheduler.apply_manual_qsy(
            {
                "target_device_profile_id": 9,
                "frequency": "14.110",
                "band": "20M",
                "mode": "Digi",
            }
        )

        assert len(queued) == 1
        assert queued[0]["rig_client"] is target_rig
        assert queued[0]["rig_client"] is not fallback_rig
        assert queued[0]["allow_global_fallback"] is False
        assert queued[0]["freq_hz"] == 14_110_000
    finally:
        scheduler.stop()


def test_scheduler_manual_qsy_target_never_allows_global_fallback_when_runtime_client_is_global(monkeypatch) -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    fallback_rig = SimpleNamespace(name="shared-rig")

    class FakeManager:
        def get_runtime_for_device(self, device_profile_id: int):
            if int(device_profile_id) != 9:
                return None
            return SimpleNamespace(
                rig_client=fallback_rig,
                js8_control_client=None,
                varac_status_client=None,
                settings_proxy=SimpleNamespace(get=lambda key, default=None: "FLRig" if key == "control_via" else default),
            )

    scheduler = SchedulerEngine(rig=fallback_rig, js8=None, varac=None, fldigi_log=None, station_runtime_manager=FakeManager())
    try:
        queued: list[dict[str, object]] = []
        monkeypatch.setattr(scheduler, "_flrig_running", lambda: True)
        monkeypatch.setattr(scheduler, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(scheduler, "_shared_ptt_lock_status", lambda force=False: {"blocked": False})
        monkeypatch.setattr(scheduler, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(scheduler, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(scheduler, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(scheduler, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(scheduler, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(scheduler, "_coordination_conflict_status", lambda *args, **kwargs: {})
        monkeypatch.setattr(scheduler, "_queue_control_action", lambda **kwargs: queued.append(dict(kwargs)) or True)

        scheduler.apply_manual_qsy(
            {
                "target_device_profile_id": 9,
                "frequency": "14.110",
                "band": "20M",
                "mode": "Digi",
            }
        )

        assert len(queued) == 1
        assert queued[0]["rig_client"] is fallback_rig
        assert queued[0]["allow_global_fallback"] is False
    finally:
        scheduler.stop()


def test_scheduler_queue_control_action_refuses_targeted_missing_rig_client() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    class FakeRig:
        def __init__(self) -> None:
            self.commands = []

        def set_frequency(self, command) -> bool:
            self.commands.append(command)
            return True

    fallback_rig = FakeRig()
    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = fallback_rig
    scheduler.js8 = None
    scheduler._shutdown_requested = False
    scheduler._control_future = None
    scheduler._pending_entry_key = None
    scheduler._control_backoff_until = 0.0
    scheduler._control_can_attempt = lambda: True
    scheduler._control_future_stuck = lambda: False
    scheduler._reset_control_executor = lambda _reason: None
    scheduler._record_scheduler_event = lambda *args, **kwargs: None

    queued = SchedulerEngine._queue_control_action(
        scheduler,
        control_mode="FLRIG",
        rig_client=None,
        js8_client=None,
        allow_global_fallback=False,
        entry_key=("AMRRON", 14110000),
        source="QSY",
        freq_hz=14_110_000,
        band="20M",
        mode="USB",
        vfo="A",
        auto_tune=False,
        js8_offset=None,
        js8_group="",
    )

    assert queued is False
    assert scheduler._pending_entry_key is None
    assert fallback_rig.commands == []


def test_scheduler_queue_control_action_does_not_update_global_js8_for_targeted_flrig() -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    class FakeRig:
        def __init__(self) -> None:
            self.commands = []

        def set_frequency(self, command) -> bool:
            self.commands.append(command)
            return True

    class FakeJS8:
        def __init__(self) -> None:
            self.commands = []

        def get_offset(self) -> int:
            return 1500

        def set_frequency(self, freq_hz: int, *, offset_hz: int | None = None) -> bool:
            self.commands.append((freq_hz, offset_hz))
            return True

    class ImmediateExecutor:
        def submit(self, callback):
            future: Future = Future()
            try:
                future.set_result(callback())
            except Exception as exc:
                future.set_exception(exc)
            return future

    target_rig = FakeRig()
    fallback_js8 = FakeJS8()
    scheduler = SchedulerEngine.__new__(SchedulerEngine)
    scheduler.rig = None
    scheduler.js8 = fallback_js8
    scheduler._shutdown_requested = False
    scheduler._control_future = None
    scheduler._pending_entry_key = None
    scheduler._control_future_token = 0
    scheduler._control_fail_count = 0
    scheduler._control_backoff_until = 0.0
    scheduler._latest_intent = None
    scheduler._force_retry_after_control = False
    scheduler._control_can_attempt = lambda: True
    scheduler._control_future_stuck = lambda: False
    scheduler._reset_control_executor = lambda _reason: None
    scheduler._record_scheduler_event = lambda *args, **kwargs: None
    scheduler._record_scheduler_health_issue = lambda *args, **kwargs: None
    scheduler._clear_scheduler_health_issue = lambda *args, **kwargs: None
    scheduler._clear_fldigi_busy_check_state = lambda: None
    scheduler._queue_post_apply_verification = lambda *args, **kwargs: None
    scheduler._queue_scheduler_thread_call = lambda callback: callback()
    scheduler._control_executor = ImmediateExecutor()

    queued = SchedulerEngine._queue_control_action(
        scheduler,
        control_mode="FLRIG",
        rig_client=target_rig,
        js8_client=None,
        allow_global_fallback=False,
        entry_key=("AMRRON", 14110000),
        source="QSY",
        freq_hz=14_110_000,
        band="20M",
        mode="USB",
        vfo="A",
        auto_tune=False,
        js8_offset=None,
        js8_group="",
    )

    assert queued is True
    assert len(target_rig.commands) == 1
    assert fallback_js8.commands == []


def test_secondary_js8call_runtime_builds_target_control_client(monkeypatch) -> None:
    import freqinout.core.station_runtime_manager as runtime_mod
    from freqinout.core.station_runtime_manager import DeviceRuntime

    built = []

    class FakeJS8Client:
        def __init__(self, *, host, port, settings) -> None:
            self.host = host
            self.port = port
            self.settings = settings
            built.append(self)

        def stop(self) -> None:
            pass

    monkeypatch.setattr(runtime_mod, "JS8ControlClient", FakeJS8Client)

    runtime = DeviceRuntime(
        profile={
            "id": 2,
            "name": "FIO-B",
            "control_backend": "js8call",
            "js8_host": "127.0.0.1",
            "js8_port": 2443,
        },
        fallback_settings=SimpleNamespace(get=lambda _key, default=None: default),
        is_primary=False,
    )

    assert runtime.js8_control_client is built[0]
    assert runtime.js8_control_client.host == "127.0.0.1"
    assert runtime.js8_control_client.port == 2443
