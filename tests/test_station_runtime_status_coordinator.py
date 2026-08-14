from __future__ import annotations

from freqinout.core.radio_status_poll_coordinator import RadioStatusPollCoordinator
from freqinout.core.station_runtime_manager import DeviceRuntime, StationRuntimeManager


class _Rig:
    def __init__(self) -> None:
        self.ptt_values = [False]
        self.frequency_values = [7_115_000]
        self.ptt_calls = 0
        self.frequency_calls = 0
        self.fail_ptt = False
        self.fail_frequency = False

    def get_ptt(self) -> bool:
        self.ptt_calls += 1
        if self.fail_ptt:
            raise RuntimeError("ptt timeout")
        return bool(self.ptt_values[min(self.ptt_calls - 1, len(self.ptt_values) - 1)])

    def get_vfo_frequency(self) -> int:
        self.frequency_calls += 1
        if self.fail_frequency:
            raise RuntimeError("frequency timeout")
        return int(self.frequency_values[min(self.frequency_calls - 1, len(self.frequency_values) - 1)])


def _runtime(rig: _Rig, now: list[float]) -> DeviceRuntime:
    runtime = DeviceRuntime(
        {
            "id": 11,
            "name": "Runtime Rig",
            "control_backend": "flrig",
            "device_class": "tx_rx",
        },
        is_primary=False,
        status_poll_coordinator=RadioStatusPollCoordinator(
            ttl_seconds=2.0,
            retry_seconds=5.0,
            time_fn=lambda: now[0],
        ),
    )
    runtime.rig_client = rig
    return runtime


def test_device_runtime_status_reads_reuse_coordinator_cache() -> None:
    now = [100.0]
    rig = _Rig()
    rig.ptt_values = [True, False]
    rig.frequency_values = [7_115_000, 14_115_000]
    runtime = _runtime(rig, now)

    assert runtime.ptt_active() is True
    assert runtime.ptt_active() is True
    assert runtime.current_frequency_hz() == 7_115_000
    assert runtime.current_frequency_hz() == 7_115_000

    assert rig.ptt_calls == 1
    assert rig.frequency_calls == 1

    now[0] = 103.0

    assert runtime.ptt_active() is False
    assert runtime.current_frequency_hz() == 14_115_000
    assert rig.ptt_calls == 2
    assert rig.frequency_calls == 2


def test_device_runtime_status_force_refresh_bypasses_cache() -> None:
    now = [100.0]
    rig = _Rig()
    rig.ptt_values = [False, True]
    rig.frequency_values = [7_115_000, 14_115_000]
    runtime = _runtime(rig, now)

    assert runtime.ptt_active() is False
    assert runtime.ptt_active(force=True) is True
    assert runtime.current_frequency_hz() == 7_115_000
    assert runtime.current_frequency_hz(force=True) == 14_115_000

    assert rig.ptt_calls == 2
    assert rig.frequency_calls == 2


def test_device_runtime_status_failure_preserves_cached_value_during_backoff() -> None:
    now = [100.0]
    rig = _Rig()
    rig.ptt_values = [True]
    rig.frequency_values = [7_115_000]
    runtime = _runtime(rig, now)

    assert runtime.ptt_active() is True
    assert runtime.current_frequency_hz() == 7_115_000

    now[0] = 103.0
    rig.fail_ptt = True
    rig.fail_frequency = True

    assert runtime.ptt_active() is True
    assert runtime.current_frequency_hz() == 7_115_000
    assert runtime._ptt_retry_ts == 108.0
    assert runtime._freq_retry_ts == 108.0

    now[0] = 104.0

    assert runtime.ptt_active() is True
    assert runtime.current_frequency_hz() == 7_115_000
    assert rig.ptt_calls == 2
    assert rig.frequency_calls == 2


def test_device_runtime_stop_invalidates_coordinator_status() -> None:
    now = [100.0]
    rig = _Rig()
    runtime = _runtime(rig, now)

    assert runtime.current_frequency_hz() == 7_115_000
    runtime.stop()
    runtime.rig_client = rig
    rig.frequency_values = [14_115_000]
    now[0] = 101.0

    assert runtime.current_frequency_hz() == 14_115_000
    assert rig.frequency_calls == 2


def test_station_runtime_manager_exposes_status_poll_metrics() -> None:
    manager = StationRuntimeManager(store=object())

    manager._status_poll_coordinator.get_snapshot("device:11:frequency", lambda: {"frequency_hz": 7_115_000})
    manager._status_poll_coordinator.get_snapshot("device:11:frequency", lambda: {"frequency_hz": 14_115_000})

    metrics = manager.get_status_poll_metrics()

    assert metrics["snapshot_count"] == 1
    assert metrics["polls_started"] == 1
    assert metrics["polls_succeeded"] == 1
    assert metrics["cache_hits"] == 1
