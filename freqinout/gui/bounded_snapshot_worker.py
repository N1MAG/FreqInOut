"""Coalescing, generation-keyed worker for GUI snapshot reads.

The worker owns the potentially slow provider call.  A widget only receives a
result when the provider finishes; it decides on the GUI thread whether that
generation is still current before rendering it.
"""

from __future__ import annotations

from threading import Lock
from typing import Callable, Optional

from PySide6.QtCore import QObject, QThread, QTimer, Signal, Slot


SnapshotReader = Callable[[], object]


class BoundedSnapshotWorker(QObject):
    """Run at most one reader and retain only the newest queued request."""

    completed = Signal(int, object, object)
    _request_signal = Signal(int, object)

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._request_signal.connect(self._accept_request)
        self._lock = Lock()
        self._latest_generation = 0
        self._latest_reader: Optional[SnapshotReader] = None
        self._running = False
        self._stopping = False

    def request(self, generation: int, reader: SnapshotReader) -> None:
        """Queue a reader; replacing a not-yet-started request is intentional."""

        if not callable(reader):
            raise TypeError("snapshot reader must be callable")
        with self._lock:
            if self._stopping:
                return
        self._request_signal.emit(int(generation), reader)

    def stop(self) -> None:
        """Prevent future work and let an in-flight reader return naturally."""

        with self._lock:
            self._stopping = True
            self._latest_reader = None

    @Slot(int, object)
    def _accept_request(self, generation: int, reader: SnapshotReader) -> None:
        with self._lock:
            if self._stopping or generation < self._latest_generation:
                return
            self._latest_generation = generation
            self._latest_reader = reader
            should_schedule = not self._running
            if should_schedule:
                self._running = True
        if should_schedule:
            QTimer.singleShot(0, self._run_latest)

    @Slot()
    def _run_latest(self) -> None:
        with self._lock:
            if self._stopping or self._latest_reader is None:
                self._running = False
                return
            generation = self._latest_generation
            reader = self._latest_reader
            self._latest_reader = None
        result: object = None
        error: object = None
        try:
            result = reader()
        except Exception as exc:  # provider failures are data, not GUI errors
            error = exc
        with self._lock:
            stopping = self._stopping
            newer_pending = self._latest_reader is not None and self._latest_generation > generation
            if not newer_pending:
                self._running = False
        if not stopping:
            self.completed.emit(generation, result, error)
        if newer_pending and not stopping:
            QTimer.singleShot(0, self._run_latest)


class SnapshotWorkerController:
    """Own a ``QThread`` and worker with an explicit bounded shutdown hook."""

    def __init__(self, receiver: QObject, callback: Callable[[int, object, object], None]) -> None:
        self.thread = QThread(receiver)
        self.worker = BoundedSnapshotWorker()
        self._stopped = False
        self.worker.moveToThread(self.thread)
        self.worker.completed.connect(callback)
        self.thread.finished.connect(self.worker.deleteLater)
        # A QObject may be released through deleteLater() without receiving a
        # QWidget close event.  Connect to this plain controller (not a method
        # on the dying receiver) so the worker thread is stopped before Qt
        # destroys the receiver's child QThread.
        receiver.destroyed.connect(self._receiver_destroyed)
        self.thread.start()

    def _receiver_destroyed(self, *_args: object) -> None:
        self.stop()

    def request(self, generation: int, reader: SnapshotReader) -> None:
        self.worker.request(generation, reader)

    def stop(self, timeout_ms: int = 2000) -> bool:
        if self._stopped:
            return not self.thread.isRunning()
        self._stopped = True
        self.worker.stop()
        self.thread.quit()
        return self.thread.wait(max(1, int(timeout_ms)))
