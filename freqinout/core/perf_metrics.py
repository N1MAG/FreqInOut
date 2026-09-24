from __future__ import annotations

import atexit
import json
import os
import queue
import threading
import time
from contextlib import ContextDecorator
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}
_LOG_LEVELS = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
    "critical": 50,
}
_PERF_FILE_LOCK = threading.Lock()
PERF_METRICS_MAX_BYTES = 5 * 1024 * 1024
PERF_METRICS_BACKUP_COUNT = 5
PERF_METRICS_QUEUE_SIZE = 4096
PERF_METRICS_BATCH_SIZE = 128


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    return False


def is_enabled(settings: Any = None) -> bool:
    """
    Runtime gate for perf logging.

    Precedence:
    1) Env var `FREQINOUT_PERF_METRICS`
    2) Settings key `perf_metrics_enabled`
    3) Default True
    """
    env = os.getenv("FREQINOUT_PERF_METRICS")
    if env is not None and str(env).strip() != "":
        return _to_bool(env)

    if settings is not None and hasattr(settings, "get"):
        try:
            return _to_bool(settings.get("perf_metrics_enabled", 1))
        except Exception:
            return True
    return True


def _clean_meta(meta: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not meta:
        return {}
    out: Dict[str, Any] = {}
    for key, value in meta.items():
        name = str(key)
        if value is None or isinstance(value, (str, int, float, bool)):
            out[name] = value
            continue
        if isinstance(value, (list, tuple, set)):
            out[name] = [str(v) for v in value]
            continue
        if isinstance(value, dict):
            out[name] = {str(k): str(v) for k, v in value.items()}
            continue
        out[name] = str(value)
    return out


def _get_perf_log_file() -> Path:
    try:
        cfg = get_config_dir()
    except Exception:
        cfg = Path(os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA") or Path.home()) / "FreqInOut"
    return Path(cfg) / "perf_metrics.log"


def _logger_can_emit(level: str) -> bool:
    try:
        if getattr(log, "disabled", False):
            return False
        lvl = int(_LOG_LEVELS.get(str(level).lower(), 20))
        return bool(log.isEnabledFor(lvl))
    except Exception:
        return True


class BufferedPerfMetricsWriter:
    """Bounded, long-lived writer for performance metrics.

    Metric emission is deliberately non-blocking.  A small bounded queue
    absorbs bursts and a daemon writer keeps the metrics file open between
    batches.  If the queue is full, the metric is dropped and the drop count
    is exposed through :meth:`stats`; normal application work is never held up
    by diagnostics.
    """

    def __init__(
        self,
        path_provider: Callable[[], str | Path],
        *,
        max_bytes: int = PERF_METRICS_MAX_BYTES,
        backup_count: int = PERF_METRICS_BACKUP_COUNT,
        queue_size: int = PERF_METRICS_QUEUE_SIZE,
        batch_size: int = PERF_METRICS_BATCH_SIZE,
        flush_interval_sec: float = 0.25,
    ) -> None:
        self._path_provider = path_provider
        self._max_bytes = max(1, int(max_bytes))
        self._backup_count = max(0, int(backup_count))
        self._queue: queue.Queue[str] = queue.Queue(maxsize=max(1, int(queue_size)))
        self._batch_size = max(1, int(batch_size))
        self._flush_interval_sec = max(0.01, float(flush_interval_sec))
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._file_lock = threading.Lock()
        self._handle = None
        self._handle_path: Path | None = None
        self._handle_size = 0
        self._dropped = 0
        self._written = 0
        self._thread = threading.Thread(
            target=self._run,
            name="freqinout-perf-metrics",
            daemon=True,
        )
        self._thread.start()

    def emit(self, line: str) -> bool:
        """Queue one already-serialized line without waiting for the writer."""

        if self._stop.is_set():
            self._dropped += 1
            return False
        try:
            self._queue.put_nowait(str(line))
        except queue.Full:
            self._dropped += 1
            return False
        self._wake.set()
        return True

    def stats(self) -> dict[str, int]:
        return {
            "queued": self._queue.qsize(),
            "dropped": int(self._dropped),
            "written": int(self._written),
        }

    def flush(self, timeout: float = 1.0) -> None:
        """Drain queued metrics within a bounded wait."""

        deadline = time.monotonic() + max(0.0, float(timeout))
        self._wake.set()
        while self._queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(min(0.01, max(0.001, deadline - time.monotonic())))
        with self._file_lock:
            if self._handle is not None:
                try:
                    self._handle.flush()
                except Exception:
                    pass

    def close(self, timeout: float = 1.0) -> None:
        if not self._stop.is_set():
            self._stop.set()
            self._wake.set()
        if self._thread is not threading.current_thread() and self._thread.is_alive():
            self._thread.join(timeout=max(0.0, float(timeout)))
        if self._thread.is_alive():
            # Preserve the close timeout even if the filesystem stalls.  The
            # daemon owns the handle and may finish later; closing it from a
            # second thread could race the active batch.
            return
        with self._file_lock:
            self._close_handle()

    def _run(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            self._wake.wait(self._flush_interval_sec)
            self._wake.clear()
            batch: list[str] = []
            while len(batch) < self._batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            if batch:
                self._write_batch(batch)
                for _line in batch:
                    self._queue.task_done()
        self.flush(timeout=0.0)

    def _write_batch(self, lines: list[str]) -> None:
        try:
            path = Path(self._path_provider())
            path.parent.mkdir(parents=True, exist_ok=True)
            with self._file_lock:
                if self._handle is None or self._handle_path != path:
                    self._close_handle()
                    self._handle = path.open("a", encoding="utf-8")
                    self._handle_path = path
                    try:
                        self._handle_size = int(path.stat().st_size)
                    except Exception:
                        self._handle_size = 0
                for line in lines:
                    payload = f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} PERF|{line}\n"
                    encoded_size = len(payload.encode("utf-8"))
                    if self._handle_size and self._handle_size + encoded_size > self._max_bytes:
                        self._rollover(path)
                    self._handle.write(payload)
                    self._handle_size += encoded_size
                    self._written += 1
                self._handle.flush()
        except Exception:
            # Telemetry must never affect the application or spin on an error.
            self._dropped += len(lines)
            return

    def _rollover(self, path: Path) -> None:
        self._close_handle()
        if self._backup_count > 0:
            for index in range(self._backup_count, 0, -1):
                source = path.with_name(f"{path.name}.{index}")
                destination = path.with_name(f"{path.name}.{index + 1}")
                if index == self._backup_count:
                    try:
                        source.unlink(missing_ok=True)
                    except Exception:
                        pass
                elif source.exists():
                    try:
                        source.replace(destination)
                    except Exception:
                        pass
            if path.exists():
                try:
                    path.replace(path.with_name(f"{path.name}.1"))
                except Exception:
                    pass
        self._handle = path.open("a", encoding="utf-8")
        self._handle_path = path
        self._handle_size = 0

    def _close_handle(self) -> None:
        if self._handle is None:
            return
        try:
            self._handle.flush()
            self._handle.close()
        except Exception:
            pass
        self._handle = None
        self._handle_path = None
        self._handle_size = 0


_perf_sink: BufferedPerfMetricsWriter | None = None


def configure_perf_metrics_sink(
    path_provider: Callable[[], str | Path] | None = None,
    *,
    max_bytes: int = PERF_METRICS_MAX_BYTES,
    backup_count: int = PERF_METRICS_BACKUP_COUNT,
    queue_size: int = PERF_METRICS_QUEUE_SIZE,
    batch_size: int = PERF_METRICS_BATCH_SIZE,
    flush_interval_sec: float = 0.25,
) -> BufferedPerfMetricsWriter:
    """Replace the process sink, primarily for startup/profile configuration."""

    global _perf_sink
    shutdown_perf_metrics()
    _perf_sink = BufferedPerfMetricsWriter(
        path_provider or _get_perf_log_file,
        max_bytes=max_bytes,
        backup_count=backup_count,
        queue_size=queue_size,
        batch_size=batch_size,
        flush_interval_sec=flush_interval_sec,
    )
    return _perf_sink


def flush_perf_metrics(timeout: float = 1.0) -> None:
    sink = _perf_sink
    if sink is not None:
        sink.flush(timeout=timeout)


def shutdown_perf_metrics(timeout: float = 1.0) -> None:
    global _perf_sink
    sink = _perf_sink
    _perf_sink = None
    if sink is not None:
        sink.close(timeout=timeout)


def _append_perf_line(line: str) -> None:
    global _perf_sink
    try:
        sink = _perf_sink
        if sink is None:
            with _PERF_FILE_LOCK:
                sink = _perf_sink
                if sink is None:
                    sink = BufferedPerfMetricsWriter(_get_perf_log_file)
                    _perf_sink = sink
        sink.emit(line)
    except Exception:
        pass


atexit.register(shutdown_perf_metrics)


def emit_span(
    name: str,
    elapsed_ms: float,
    *,
    settings: Any = None,
    meta: Optional[Mapping[str, Any]] = None,
    min_ms: float = 0.0,
    level: str = "info",
) -> None:
    if elapsed_ms < float(min_ms):
        return
    if not is_enabled(settings=settings):
        return
    payload = {
        "name": str(name),
        "ms": round(float(elapsed_ms), 3),
    }
    details = _clean_meta(meta)
    if details:
        payload["meta"] = details
    line = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    if _logger_can_emit(level):
        try:
            writer = getattr(log, str(level).lower(), log.info)
            writer("PERF|%s", line)
        except Exception:
            pass
    _append_perf_line(line)


class PerfSpan(ContextDecorator):
    def __init__(
        self,
        name: str,
        *,
        settings: Any = None,
        meta: Optional[Mapping[str, Any]] = None,
        min_ms: float = 0.0,
        level: str = "info",
    ) -> None:
        self.name = str(name)
        self.settings = settings
        self.meta = meta
        self.min_ms = float(min_ms)
        self.level = str(level).lower()
        self._start: Optional[float] = None

    def __enter__(self) -> "PerfSpan":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._start is None:
            return False
        elapsed_ms = (time.perf_counter() - self._start) * 1000.0
        meta = dict(_clean_meta(self.meta))
        if exc_type is not None:
            meta["error"] = str(exc_type.__name__)
        emit_span(
            self.name,
            elapsed_ms,
            settings=self.settings,
            meta=meta,
            min_ms=self.min_ms,
            level=self.level,
        )
        return False


def span(
    name: str,
    *,
    settings: Any = None,
    meta: Optional[Mapping[str, Any]] = None,
    min_ms: float = 0.0,
    level: str = "info",
) -> PerfSpan:
    return PerfSpan(name, settings=settings, meta=meta, min_ms=min_ms, level=level)


def percentile(samples: Iterable[float], pct: float) -> float:
    values = sorted(float(v) for v in samples)
    if not values:
        return 0.0
    if pct <= 0:
        return values[0]
    if pct >= 100:
        return values[-1]
    index = (len(values) - 1) * (pct / 100.0)
    lo = int(index)
    hi = min(lo + 1, len(values) - 1)
    if lo == hi:
        return values[lo]
    frac = index - lo
    return values[lo] + (values[hi] - values[lo]) * frac


def summarize_samples(samples: Iterable[float]) -> Dict[str, float]:
    values = [float(v) for v in samples]
    if not values:
        return {
            "count": 0.0,
            "min": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
            "mean": 0.0,
        }
    return {
        "count": float(len(values)),
        "min": min(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values),
        "mean": sum(values) / len(values),
    }
