from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace


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
