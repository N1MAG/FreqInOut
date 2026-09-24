"""Shared cancellation and bounded-shutdown primitives for background work."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable


class OperationCancelled(RuntimeError):
    """Raised when cooperative background work observes cancellation."""


class CancellationToken:
    """Thread-safe cooperative cancellation signal.

    Work that can block should use bounded I/O timeouts and call ``checkpoint``
    between operations or batches.  Cancelling a token is idempotent.
    """

    def __init__(self) -> None:
        self._event = threading.Event()

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        self._event.set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)

    def checkpoint(self) -> None:
        if self.is_cancelled:
            raise OperationCancelled("background operation cancelled")


@dataclass(frozen=True)
class ShutdownParticipant:
    name: str
    request_stop: Callable[[], None]
    is_stopped: Callable[[], bool]


class WorkerShutdownRegistry:
    """Registry used to request and observe cooperative worker shutdown.

    The registry never terminates threads.  It provides a single ownership
    point for cancellation and a bounded wait result so the caller can report
    the exact participants that did not stop within its deadline.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._participants: Dict[str, ShutdownParticipant] = {}

    def register(
        self,
        name: str,
        *,
        request_stop: Callable[[], None],
        is_stopped: Callable[[], bool],
    ) -> Callable[[], None]:
        key = str(name or "").strip()
        if not key:
            raise ValueError("shutdown participant name is required")
        participant = ShutdownParticipant(key, request_stop, is_stopped)
        with self._lock:
            self._participants[key] = participant

        def unregister() -> None:
            self.unregister(key)

        return unregister

    def unregister(self, name: str) -> None:
        with self._lock:
            self._participants.pop(str(name or "").strip(), None)

    def participants(self) -> tuple[ShutdownParticipant, ...]:
        with self._lock:
            return tuple(self._participants.values())

    def request_stop_all(self) -> tuple[str, ...]:
        errors: list[str] = []
        for participant in self.participants():
            try:
                participant.request_stop()
            except Exception:
                errors.append(participant.name)
        return tuple(errors)

    def pending(self) -> tuple[str, ...]:
        pending: list[str] = []
        for participant in self.participants():
            try:
                if not participant.is_stopped():
                    pending.append(participant.name)
            except Exception:
                pending.append(participant.name)
        return tuple(pending)

    def wait(self, timeout_sec: float, *, poll_interval_sec: float = 0.01) -> tuple[str, ...]:
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        while True:
            pending = self.pending()
            if not pending or time.monotonic() >= deadline:
                return pending
            time.sleep(min(max(0.001, float(poll_interval_sec)), max(0.0, deadline - time.monotonic())))

    def __len__(self) -> int:
        with self._lock:
            return len(self._participants)


def cancelled(token: CancellationToken | None) -> bool:
    """Small helper for optional-token loops."""
    return bool(token is not None and token.is_cancelled)


def checkpoint_all(tokens: Iterable[CancellationToken | None]) -> None:
    for token in tokens:
        if token is not None:
            token.checkpoint()
