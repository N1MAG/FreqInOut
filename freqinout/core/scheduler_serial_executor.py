"""Small project-owned serial executor for bounded endpoint lifecycle.

Unlike ``ThreadPoolExecutor``, this executor's worker is daemonized and is not
registered with Python's global executor shutdown hook. A contract-violating
endpoint call can therefore be reported and abandoned at process shutdown
without preventing FIO from exiting. Normal cooperative operations still drain
and join through the explicit scheduler lifecycle.
"""

from __future__ import annotations

from concurrent.futures import Future
import queue
import threading
from typing import Callable, Optional


_STOP = object()


class DaemonSerialExecutor:
    """One lazy daemon worker implementing the subset of Executor FIO needs."""

    def __init__(self, *, max_workers: int = 1, thread_name_prefix: str = "fio-endpoint") -> None:
        if int(max_workers) != 1:
            raise ValueError("DaemonSerialExecutor supports exactly one worker")
        self._name = str(thread_name_prefix or "fio-endpoint")
        self._queue: "queue.Queue[object]" = queue.Queue()
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._closed = False

    def submit(self, function: Callable[..., object], *args: object, **kwargs: object) -> Future:
        if not callable(function):
            raise TypeError("submitted endpoint operation must be callable")
        with self._lock:
            if self._closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            future: Future = Future()
            self._queue.put((future, function, args, kwargs))
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name=self._name,
                    daemon=True,
                )
                self._thread.start()
            return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                if cancel_futures:
                    self._cancel_queued_locked()
                self._queue.put(_STOP)
            thread = self._thread
        if wait and thread is not None and thread is not threading.current_thread():
            thread.join()

    def await_termination(self, timeout: float) -> bool:
        with self._lock:
            thread = self._thread
        if thread is None:
            return True
        if thread is threading.current_thread():
            return False
        thread.join(timeout=max(0.0, float(timeout)))
        return not thread.is_alive()

    @property
    def thread_alive(self) -> bool:
        with self._lock:
            thread = self._thread
        return bool(thread is not None and thread.is_alive())

    def _cancel_queued_locked(self) -> None:
        retained_stop = False
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is _STOP:
                retained_stop = True
                continue
            future = item[0] if isinstance(item, tuple) and item else None
            if isinstance(future, Future):
                future.cancel()
        if retained_stop:
            self._queue.put(_STOP)

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is _STOP:
                return
            future, function, args, kwargs = item  # type: ignore[misc]
            if not future.set_running_or_notify_cancel():
                continue
            try:
                result = function(*args, **kwargs)
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)
