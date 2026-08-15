from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace


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
