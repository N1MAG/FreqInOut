from __future__ import annotations

import json
import os
import time
from contextlib import ContextDecorator
from typing import Any, Dict, Iterable, Mapping, Optional

from freqinout.core.logger import log


_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


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
    writer = getattr(log, str(level).lower(), log.info)
    writer("PERF|%s", line)


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
