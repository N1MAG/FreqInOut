from __future__ import annotations

import threading
import time

import pytest

from freqinout.core.worker_lifecycle import (
    CancellationToken,
    OperationCancelled,
    WorkerShutdownRegistry,
)


def test_cancellation_token_is_idempotent_and_interrupts_wait() -> None:
    token = CancellationToken()
    observed: list[bool] = []
    waiter = threading.Thread(target=lambda: observed.append(token.wait(1.0)))
    waiter.start()
    token.cancel()
    token.cancel()
    waiter.join(0.5)

    assert observed == [True]
    assert token.is_cancelled is True
    with pytest.raises(OperationCancelled):
        token.checkpoint()


def test_shutdown_registry_requests_every_participant_and_reports_pending() -> None:
    registry = WorkerShutdownRegistry()
    stopped = {"ready": False, "stuck": False}
    registry.register(
        "ready",
        request_stop=lambda: stopped.__setitem__("ready", True),
        is_stopped=lambda: stopped["ready"],
    )
    unregister = registry.register(
        "stuck",
        request_stop=lambda: None,
        is_stopped=lambda: stopped["stuck"],
    )

    assert registry.request_stop_all() == ()
    assert registry.wait(0.02, poll_interval_sec=0.002) == ("stuck",)
    unregister()
    assert registry.pending() == ()


def test_shutdown_registry_bounded_wait_returns_soon() -> None:
    registry = WorkerShutdownRegistry()
    registry.register("stuck", request_stop=lambda: None, is_stopped=lambda: False)
    started = time.monotonic()
    assert registry.wait(0.03, poll_interval_sec=0.002) == ("stuck",)
    assert time.monotonic() - started < 0.2
