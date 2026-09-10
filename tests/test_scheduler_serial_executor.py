"""Lifecycle contract for the scheduler's daemonized serial executor."""

from __future__ import annotations

import threading

from freqinout.core.scheduler_serial_executor import DaemonSerialExecutor


def test_serial_executor_runs_work_and_joins_after_cooperative_shutdown() -> None:
    executor = DaemonSerialExecutor(thread_name_prefix="fio-test-serial")
    assert executor.submit(lambda: 42).result(timeout=1.0) == 42
    executor.shutdown(wait=True)
    assert executor.thread_alive is False


def test_serial_executor_cancels_queued_work_without_interrupting_running_work() -> None:
    executor = DaemonSerialExecutor(thread_name_prefix="fio-test-cancel")
    started = threading.Event()
    release = threading.Event()

    def blocked() -> str:
        started.set()
        release.wait(2.0)
        return "done"

    running = executor.submit(blocked)
    assert started.wait(1.0)
    queued = executor.submit(lambda: "must-not-run")
    executor.shutdown(wait=False, cancel_futures=True)
    assert queued.cancelled()
    assert executor.thread_alive is True
    release.set()
    assert running.result(timeout=1.0) == "done"
    assert executor.await_termination(1.0)
    assert executor.thread_alive is False


def test_serial_executor_rejects_submissions_after_shutdown() -> None:
    executor = DaemonSerialExecutor(thread_name_prefix="fio-test-closed")
    executor.shutdown(wait=True)
    try:
        executor.submit(lambda: None)
    except RuntimeError as exc:
        assert "after shutdown" in str(exc)
    else:
        raise AssertionError("closed executor accepted work")
