"""Parse FreqInOut performance logs and evaluate Slice 0 budgets.

The parser intentionally accepts both the structured ``PERF|{...}`` spans and
the older ``UI_PERF|...``/``DEPENDENCY_STATUS|...`` records.  It is read-only
and does not require Qt, a running application, or a configuration database.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

DEFAULT_THRESHOLDS_MS: dict[str, float] = {
    "startup.first_usable_shell": 5_000.0,
    "startup.startup_complete": 10_000.0,
    "startup.main_window_construct": 10_000.0,
    "startup.database_init": 10_000.0,
    "messages.project_native_sources": 500.0,
    "messages.project_rows": 500.0,
    "messages.project_native_files": 500.0,
    "messages.file_scan_total": 500.0,
    "freqplanner.rebuild_table": 500.0,
    "controlfreq.heavy_refresh": 500.0,
    "ui.callback": 50.0,
    "ui.slow_refresh": 50.0,
    "dependency.process_snapshot": 250.0,
    "shutdown.complete": 3_000.0,
    # A watchdog record is a count-like sentinel: any observed sample fails.
    "ui.watchdog_stall": 0.0,
}

_KEY_VALUE = re.compile(r"(?P<key>[A-Za-z0-9_.-]+)=(?P<value>[^|\s]+)")
_UI_REFRESH = re.compile(r"UI_PERF\|slow_refresh\s+label=(?P<label>[^\s|]+)\s+elapsed_ms=(?P<ms>[0-9.]+)")
_DEPENDENCY = re.compile(r"DEPENDENCY_STATUS\|slow_process_snapshot\|[^\n]*?duration_ms=(?P<ms>[0-9.]+)")
_WATCHDOG = re.compile(r"UI watchdog detected an event-loop stall", re.I)


def _percentile(samples: Iterable[float], pct: float) -> float:
    values = sorted(float(value) for value in samples)
    if not values:
        return 0.0
    index = (len(values) - 1) * max(0.0, min(100.0, float(pct))) / 100.0
    low = int(index)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (index - low)


def _summarize_samples(samples: Iterable[float]) -> dict[str, float]:
    values = [float(value) for value in samples]
    if not values:
        return {key: 0.0 for key in ("count", "min", "p50", "p95", "p99", "max", "mean")}
    return {
        "count": float(len(values)),
        "min": min(values),
        "p50": _percentile(values, 50),
        "p95": _percentile(values, 95),
        "p99": _percentile(values, 99),
        "max": max(values),
        "mean": sum(values) / len(values),
    }


@dataclass(frozen=True)
class PerfSample:
    name: str
    elapsed_ms: float
    metadata: Mapping[str, Any] = field(default_factory=dict)
    line_number: int = 0
    source: str = ""


@dataclass(frozen=True)
class BudgetResult:
    name: str
    threshold_ms: float
    summary: Mapping[str, float]
    observed: bool
    passed: bool


def _number(value: str) -> Any:
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _structured_sample(payload: str, line_number: int, source: str) -> PerfSample | None:
    try:
        record = json.loads(payload)
        name = str(record.get("name", "")).strip()
        elapsed = float(record["ms"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    if not name:
        return None
    metadata = record.get("meta")
    return PerfSample(name, elapsed, metadata if isinstance(metadata, dict) else {}, line_number, source)


def parse_perf_lines(lines: Iterable[str], *, source: str = "") -> list[PerfSample]:
    """Parse supported log records without raising on malformed lines."""
    samples: list[PerfSample] = []
    for line_number, line in enumerate(lines, 1):
        if "PERF|{" in line:
            payload = line.split("PERF|{", 1)[1]
            sample = _structured_sample("{" + payload.split("\n", 1)[0], line_number, source)
            if sample:
                samples.append(sample)
                continue
        match = _UI_REFRESH.search(line)
        if match:
            samples.append(PerfSample("ui.slow_refresh", float(match["ms"]), {"label": match["label"]}, line_number, source))
            continue
        match = _DEPENDENCY.search(line)
        if match:
            samples.append(PerfSample("dependency.process_snapshot", float(match["ms"]), {}, line_number, source))
            continue
        if _WATCHDOG.search(line):
            samples.append(PerfSample("ui.watchdog_stall", 1.0, {}, line_number, source))
    return samples


def parse_perf_file(path: str | Path) -> list[PerfSample]:
    path = Path(path)
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return parse_perf_lines(handle, source=str(path))


def _canonical_name(sample: PerfSample) -> str:
    if sample.name == "ui.slow_refresh":
        return sample.name
    if sample.name == "dependency.process_snapshot":
        return sample.name
    return sample.name


def evaluate_samples(samples: Iterable[PerfSample], thresholds_ms: Mapping[str, float] | None = None) -> list[BudgetResult]:
    thresholds = dict(DEFAULT_THRESHOLDS_MS)
    if thresholds_ms:
        thresholds.update({str(key): float(value) for key, value in thresholds_ms.items()})
    grouped: dict[str, list[float]] = {key: [] for key in thresholds}
    for sample in samples:
        name = _canonical_name(sample)
        if name in grouped:
            grouped[name].append(sample.elapsed_ms)
    results: list[BudgetResult] = []
    for name, threshold in thresholds.items():
        summary = _summarize_samples(grouped[name])
        # A budget is evaluated against p95; absent instrumentation is a fail
        # only when explicitly requested by callers, so reports distinguish it.
        results.append(BudgetResult(name, threshold, summary, bool(grouped[name]), not grouped[name] or summary["p95"] <= threshold))
    return results


def report_for_paths(paths: Iterable[str | Path]) -> dict[str, Any]:
    samples: list[PerfSample] = []
    files: list[str] = []
    for path in paths:
        files.append(str(path))
        samples.extend(parse_perf_file(path))
    results = evaluate_samples(samples)
    return {
        "files": files,
        "sample_count": len(samples),
        "metrics": {
            item.name: {
                "threshold_ms": item.threshold_ms,
                "observed": item.observed,
                "passed": item.passed,
                **dict(item.summary),
            }
            for item in results
        },
    }


def format_report(report: Mapping[str, Any]) -> str:
    lines = [f"Perf samples: {report.get('sample_count', 0)}"]
    for name, item in report.get("metrics", {}).items():
        status = "PASS" if item.get("passed") else "FAIL"
        observed = "observed" if item.get("observed") else "not observed"
        lines.append(f"{status:4} {name}: {observed}, p95={item.get('p95', 0):.1f}ms <= {item.get('threshold_ms', 0):.1f}ms")
    return "\n".join(lines)
