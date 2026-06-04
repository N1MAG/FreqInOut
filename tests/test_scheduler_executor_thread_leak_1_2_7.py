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
