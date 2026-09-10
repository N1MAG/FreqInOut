"""MES-2 SchedulerEngine integration coverage for endpoint-owned control lanes.

These tests intentionally exercise the production ``_queue_control_action``
boundary instead of only the lane implementation.  The rig doubles use gates
rather than elapsed-time sleeps so a blocked adapter cannot make the suite
flaky.  They also prove that the post-apply readback stays in the same endpoint
worker as its command: a slow readback for one rig must not hold another rig.
"""

from __future__ import annotations

import threading
from typing import List, Tuple

from PySide6.QtCore import QCoreApplication, QEvent

from freqinout.core.scheduler_engine import SchedulerEngine


def _wait(event: threading.Event, description: str) -> None:
    assert event.wait(timeout=2.0), description


class _Rig:
    """Controllable FLRig-shaped client with endpoint identity attributes."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        set_result: bool = True,
        block_set: bool = False,
        block_readback: bool = False,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.set_result = bool(set_result)
        self.block_set = bool(block_set)
        self.block_readback = bool(block_readback)
        self.set_started = threading.Event()
        self.set_release = threading.Event()
        self.set_finished = threading.Event()
        self.readback_started = threading.Event()
        self.readback_release = threading.Event()
        self.readback_finished = threading.Event()
        self.commands: List[object] = []
        if not self.block_set:
            self.set_release.set()
        if not self.block_readback:
            self.readback_release.set()

    def set_frequency(self, command: object) -> bool:
        self.set_started.set()
        self.set_release.wait()
        self.commands.append(command)
        self.set_finished.set()
        return self.set_result

    def get_vfo_frequency(self) -> int:
        self.readback_started.set()
        self.readback_release.wait()
        self.readback_finished.set()
        return 14_115_000

    def release(self) -> None:
        self.set_release.set()
        self.readback_release.set()


def _new_engine() -> SchedulerEngine:
    """Use normal engine initialization, then make test callbacks deterministic."""

    app = QCoreApplication.instance()
    if app is None:
        QCoreApplication([])
    engine = SchedulerEngine(poll_interval_ms=60_000)
    engine._queue_scheduler_thread_call = lambda callback: callback()
    engine._queue_post_apply_verification = lambda **_kwargs: None
    engine._record_scheduler_health_issue = lambda *_args, **_kwargs: None
    engine._clear_scheduler_health_issue = lambda *_args, **_kwargs: None
    engine._clear_fldigi_busy_check_state = lambda: None
    engine._control_timeout_s = 60.0
    return engine


def _stop(engine: SchedulerEngine) -> None:
    engine.stop()
    app = QCoreApplication.instance()
    if app is not None:
        engine.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


def _queue(engine: SchedulerEngine, rig: _Rig, *, entry: str, freq_hz: int = 14_115_000) -> bool:
    return engine._queue_control_action(
        control_mode="FLRIG",
        rig_client=rig,
        js8_client=None,
        allow_global_fallback=False,
        entry_key=(entry, freq_hz),
        source="HF",
        freq_hz=freq_hz,
        band="20M",
        mode="USB",
        vfo="A",
        auto_tune=False,
        js8_offset=None,
        js8_group="",
    )


def _endpoint_future(engine: SchedulerEngine, rig: _Rig):
    endpoint = engine._control_endpoint_key(
        "FLRIG", rig_client=rig, js8_client=None, entry_key=("test", 1)
    )
    lane = engine._endpoint_lanes.lane(endpoint)
    assert lane is not None and lane.future is not None
    return lane.future


def test_blocked_set_on_one_endpoint_does_not_hold_peer_set_or_inline_readback() -> None:
    engine = _new_engine()
    blocked = _Rig(host="127.0.0.1", port=12_345, block_set=True)
    peer = _Rig(host="127.0.0.1", port=12_346)
    try:
        assert _queue(engine, blocked, entry="A")
        _wait(blocked.set_started, "blocked endpoint did not begin set_frequency")
        assert _queue(engine, peer, entry="B")
        _wait(peer.set_finished, "peer endpoint did not set frequency")
        _wait(peer.readback_finished, "peer endpoint did not complete inline readback")
        assert not blocked.set_finished.is_set()
    finally:
        blocked.release()
        peer.release()
        _stop(engine)


def test_blocked_readback_on_one_endpoint_does_not_hold_peer_set_or_readback() -> None:
    engine = _new_engine()
    blocked = _Rig(host="127.0.0.1", port=12_347, block_readback=True)
    peer = _Rig(host="127.0.0.1", port=12_348)
    try:
        assert _queue(engine, blocked, entry="A")
        _wait(blocked.set_finished, "blocked-readback endpoint did not set frequency")
        _wait(blocked.readback_started, "blocked-readback endpoint did not begin get_vfo_frequency")
        assert _queue(engine, peer, entry="B")
        _wait(peer.set_finished, "peer endpoint did not set frequency")
        _wait(peer.readback_finished, "peer endpoint did not complete inline readback")
        assert not blocked.readback_finished.is_set()
    finally:
        blocked.release()
        peer.release()
        _stop(engine)


def test_failed_endpoint_enters_its_own_backoff_while_peer_completes() -> None:
    engine = _new_engine()
    failed = _Rig(host="127.0.0.1", port=12_349, set_result=False)
    peer = _Rig(host="127.0.0.1", port=12_350)
    failed_event = threading.Event()

    def record(event_type: str, event_name: str, **_kwargs: object) -> None:
        if event_type == "failed" and event_name == "control_action_failed":
            failed_event.set()

    engine._record_scheduler_event = record
    try:
        assert _queue(engine, failed, entry="A")
        assert _queue(engine, peer, entry="B")
        _wait(failed_event, "failed endpoint completion was not reported")
        _wait(peer.readback_finished, "peer endpoint did not complete while A backed off")
        failed_endpoint = engine._control_endpoint_key(
            "FLRIG", rig_client=failed, js8_client=None, entry_key=("A", 1)
        )
        failed_lane = engine._endpoint_lanes.lane(failed_endpoint)
        assert failed_lane is not None
        assert failed_lane.snapshot().failure_count == 1
        assert failed_lane.snapshot().backoff_until_monotonic > 0.0
        assert _queue(engine, failed, entry="A-retry")
        assert failed_lane.snapshot().state == "backoff"
    finally:
        failed.release()
        peer.release()
        _stop(engine)


def test_same_normalized_endpoint_serializes_and_coalesces_to_newest_command() -> None:
    engine = _new_engine()
    first = _Rig(host="LOCALHOST", port=12_351, block_set=True)
    middle = _Rig(host="localhost", port=12_351)
    newest = _Rig(host="localhost", port=12_351)
    try:
        assert _queue(engine, first, entry="first", freq_hz=7_100_000)
        _wait(first.set_started, "first endpoint command did not start")
        assert _queue(engine, middle, entry="middle", freq_hz=7_110_000)
        assert _queue(engine, newest, entry="newest", freq_hz=7_120_000)
        assert len(engine._endpoint_lanes) == 1
        assert not middle.set_started.is_set()
        assert not newest.set_started.is_set()
        first.release()
        _wait(newest.set_finished, "newest coalesced command did not run")
        _wait(newest.readback_finished, "newest coalesced command did not read back")
        assert not middle.set_started.is_set()
        assert len(first.commands) == 1
        assert first.commands[0].rig_hz == 7_100_000
        assert len(newest.commands) == 1
        assert newest.commands[0].rig_hz == 7_120_000
    finally:
        first.release()
        middle.release()
        newest.release()
        _stop(engine)


def test_stop_is_nonblocking_and_suppresses_late_lane_callback_event_and_verification() -> None:
    engine = _new_engine()
    first = _Rig(host="127.0.0.1", port=12_352, block_set=True)
    second = _Rig(host="127.0.0.1", port=12_353, block_set=True)
    events: List[Tuple[Tuple[object, ...], dict]] = []
    verification_called = threading.Event()
    engine._record_scheduler_event = lambda *args, **kwargs: events.append((args, kwargs))
    engine._queue_post_apply_verification = lambda **_kwargs: verification_called.set()
    engine._last_entry_key = ("before-stop",)
    try:
        assert _queue(engine, first, entry="A")
        assert _queue(engine, second, entry="B")
        _wait(first.set_started, "first stop-test endpoint did not start")
        _wait(second.set_started, "second stop-test endpoint did not start")
        events.clear()
        stopped = threading.Event()

        def stop_engine() -> None:
            engine.stop()
            stopped.set()

        stopper = threading.Thread(target=stop_engine, name="test-engine-stop")
        stopper.start()
        _wait(stopped, "SchedulerEngine.stop waited for blocked endpoint work")
        stopper.join(timeout=2.0)
        assert not stopper.is_alive()
        assert all(snapshot.closed for snapshot in engine._endpoint_lanes.snapshots())
        first.release()
        second.release()
        _endpoint_future(engine, first).result(timeout=2.0)
        _endpoint_future(engine, second).result(timeout=2.0)
        assert events == []
        assert not verification_called.is_set()
        assert engine._last_entry_key == ("before-stop",)
    finally:
        first.release()
        second.release()
        if not engine._shutdown_requested:
            _stop(engine)
