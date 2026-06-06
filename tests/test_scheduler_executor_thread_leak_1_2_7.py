from __future__ import annotations


class _BlockedFuture:
    def done(self) -> bool:
        return False

    def cancel(self) -> bool:
        return False


def test_status_timeout_does_not_replace_executor(monkeypatch):
    import freqinout.core.scheduler_engine as scheduler_module

    engine = scheduler_module.SchedulerEngine()
    try:
        executor = engine._status_executor
        engine._status_snapshot_future = _BlockedFuture()
        engine._status_snapshot_started_at = 1.0
        engine._status_snapshot_timeout_s = 5.0
        monkeypatch.setattr(scheduler_module.time, "time", lambda: 100.0)

        engine._maybe_refresh_external_status_snapshot(force=True)
        engine._maybe_refresh_external_status_snapshot(force=True)

        assert engine._status_executor is executor
        assert engine._status_snapshot_future is not None
        assert engine._status_snapshot_timeout_reported is True
    finally:
        engine.stop()


def test_status_refresh_invokes_js8_shadow_comparison_for_offset_only_branch(monkeypatch):
    import freqinout.core.scheduler_engine as scheduler_module

    shadow_calls: list[dict[str, object]] = []

    class _ImmediateFuture:
        def __init__(self, value):
            self._value = value

        def done(self) -> bool:
            return True

        def add_done_callback(self, callback):
            callback(self)

        def result(self):
            return self._value

    class _ImmediateExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_calls: list[tuple[bool, bool]] = []

        def submit(self, fn, *args, **kwargs):
            return _ImmediateFuture(fn(*args, **kwargs))

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    class DummySettings:
        def __init__(self, values=None):
            self._values = dict(values or {})

        def get(self, key, default=None):
            return self._values.get(key, default)

        def close(self):
            pass

    class _FakeVarACStatusClient:
        def get_status(self, include_db_transfer: bool = True):
            return {"busy": False, "waiting_for_frequency": False, "reason": None}

    class _FakeShadowService:
        def __init__(self, settings):
            self.settings = settings

        def js8_shadow_comparison_status(self, *, legacy_readings, **_kwargs):
            shadow_calls.append(dict(legacy_readings))
            return {"connected": True, "mode": "api_basic", "version": "3.0.2"}

    class _FakeJS8Client:
        def __init__(self, *args, **kwargs):
            pass

        def get_offset(self):
            return 1950

        def stop(self):
            pass

    monkeypatch.setattr(scheduler_module, "ThreadPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(scheduler_module, "SettingsManager", lambda: DummySettings({"control_via": "FLRig"}))
    monkeypatch.setattr(scheduler_module, "VarACStatusClient", _FakeVarACStatusClient)
    monkeypatch.setattr(scheduler_module, "SoftwareStatusService", _FakeShadowService)
    monkeypatch.setattr(scheduler_module, "JS8ControlClient", _FakeJS8Client)

    engine = scheduler_module.SchedulerEngine(js8=_FakeJS8Client())
    try:
        monkeypatch.setattr(engine, "_js8_offset_authority_active", lambda *args, **kwargs: True, raising=False)
        monkeypatch.setattr(engine, "_queue_scheduler_thread_call", lambda callback: callback(), raising=False)
        engine._maybe_refresh_external_status_snapshot(force=True)

        assert shadow_calls == [{"offset_hz": 1950}]
        assert engine._last_js8_shadow_comparison == {"connected": True, "mode": "api_basic", "version": "3.0.2"}
    finally:
        engine.stop()


def test_control_timeout_does_not_replace_executor(monkeypatch):
    import freqinout.core.scheduler_engine as scheduler_module

    engine = scheduler_module.SchedulerEngine()
    try:
        executor = engine._control_executor
        engine._control_future = _BlockedFuture()
        engine._control_future_started_at = 1.0
        engine._control_timeout_s = 5.0
        monkeypatch.setattr(scheduler_module.time, "time", lambda: 100.0)
        monkeypatch.setattr(engine, "_control_can_attempt", lambda: True)

        queued = engine._queue_control_action(
            control_mode="MANUAL",
            entry_key=("HF", 7_100_000),
            source="HF",
            freq_hz=7_100_000,
            band="40M",
            mode=None,
            vfo=None,
            auto_tune=False,
            js8_offset=None,
            js8_group="",
        )

        assert queued is False
        assert engine._control_executor is executor
        assert engine._control_future is not None
        assert engine._control_timeout_reported is True
    finally:
        engine.stop()


def test_status_refresh_invokes_js8_shadow_comparison(monkeypatch):
    import freqinout.core.scheduler_engine as scheduler_module

    shadow_calls: list[dict[str, object]] = []

    class _ImmediateFuture:
        def __init__(self, value):
            self._value = value

        def done(self) -> bool:
            return True

        def add_done_callback(self, callback):
            callback(self)

        def result(self):
            return self._value

    class _ImmediateExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_calls: list[tuple[bool, bool]] = []

        def submit(self, fn, *args, **kwargs):
            return _ImmediateFuture(fn(*args, **kwargs))

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    class DummySettings:
        def __init__(self, values=None):
            self._values = dict(values or {})

        def get(self, key, default=None):
            return self._values.get(key, default)

        def close(self):
            pass

    class _FakeVarACStatusClient:
        def get_status(self, include_db_transfer: bool = True):
            return {"busy": False, "waiting_for_frequency": False, "reason": None}

    class _FakeShadowService:
        def __init__(self, settings):
            self.settings = settings

        def js8_shadow_comparison_status(self, *, legacy_readings, **_kwargs):
            shadow_calls.append(dict(legacy_readings))
            return {"connected": True, "mode": "api_basic", "version": "3.0.2"}

    class _FakeJS8Client:
        def __init__(self, *args, **kwargs):
            pass

        def is_busy(self) -> bool:
            return True

        def get_frequency(self):
            return 7078000

        def get_offset(self):
            return 1950

        def stop(self):
            pass

    monkeypatch.setattr(scheduler_module, "ThreadPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(scheduler_module, "SettingsManager", lambda: DummySettings({"control_via": "JS8Call"}))
    monkeypatch.setattr(scheduler_module, "VarACStatusClient", _FakeVarACStatusClient)
    monkeypatch.setattr(scheduler_module, "SoftwareStatusService", _FakeShadowService)
    monkeypatch.setattr(scheduler_module, "JS8ControlClient", _FakeJS8Client)

    engine = scheduler_module.SchedulerEngine(js8=_FakeJS8Client())
    try:
        monkeypatch.setattr(engine, "_queue_scheduler_thread_call", lambda callback: callback(), raising=False)
        engine._maybe_refresh_external_status_snapshot(force=True)

        assert shadow_calls == [
            {"busy": True, "frequency_hz": 7078000, "offset_hz": 1950},
        ]
        assert engine._last_js8_shadow_comparison == {"connected": True, "mode": "api_basic", "version": "3.0.2"}
    finally:
        engine.stop()
