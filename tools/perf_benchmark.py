from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from freqinout.core import logger as fio_logger
from freqinout.core.config_paths import get_config_dir
from freqinout.core.perf_metrics import summarize_samples


def _candidate_log_files() -> List[Path]:
    candidates: List[Path] = []
    try:
        candidates.append(Path(fio_logger._get_log_file()))
    except Exception:
        pass
    try:
        candidates.append(get_config_dir() / "perf_metrics.log")
    except Exception:
        pass
    appdata = Path(os.environ.get("APPDATA", "")) if os.environ.get("APPDATA") else None
    localapp = Path(os.environ.get("LOCALAPPDATA", "")) if os.environ.get("LOCALAPPDATA") else None
    if appdata is not None:
        candidates.append(appdata / "FreqInOut" / "freqinout.log")
        candidates.append(appdata / "FreqInOut" / "perf_metrics.log")
    if localapp is not None:
        candidates.append(localapp / "FreqInOut" / "freqinout.log")
        candidates.append(localapp / "FreqInOut" / "perf_metrics.log")
    home = Path.home()
    candidates.append(home / "AppData" / "Roaming" / "FreqInOut" / "freqinout.log")
    candidates.append(home / "AppData" / "Local" / "FreqInOut" / "freqinout.log")
    candidates.append(home / "AppData" / "Roaming" / "FreqInOut" / "perf_metrics.log")
    candidates.append(home / "AppData" / "Local" / "FreqInOut" / "perf_metrics.log")
    # Deduplicate while preserving order.
    seen = set()
    out: List[Path] = []
    for path in candidates:
        norm = str(path.resolve()) if path.is_absolute() else str(path)
        if norm in seen:
            continue
        seen.add(norm)
        out.append(path)
    return out


def _resolve_log_path(raw: str) -> Path:
    if raw and raw.lower() != "auto":
        return Path(raw).resolve()
    existing = [p for p in _candidate_log_files() if p.exists()]
    if existing:
        existing.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return existing[0].resolve()
    candidates = _candidate_log_files()
    if candidates:
        return candidates[0].resolve()
    return (Path.cwd() / "freqinout.log").resolve()


def _resolve_log_paths(raw: str) -> List[Path]:
    if raw and raw.lower() != "auto":
        return [Path(raw).resolve()]
    existing = [p.resolve() for p in _candidate_log_files() if p.exists()]
    if existing:
        return existing
    return [_resolve_log_path("auto")]


def _load_perf_events(log_path: Path) -> List[Dict]:
    events: List[Dict] = []
    if not log_path.exists():
        return events
    marker = "PERF|"
    with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            idx = line.find(marker)
            if idx < 0:
                continue
            payload = line[idx + len(marker) :].strip()
            if not payload:
                continue
            try:
                data = json.loads(payload)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            if "name" not in data or "ms" not in data:
                continue
            try:
                data["ms"] = float(data["ms"])
            except Exception:
                continue
            events.append(data)
    return events


def _filter_events(events: Iterable[Dict], name_filter: str | None) -> List[Dict]:
    if not name_filter:
        return list(events)
    rx = re.compile(name_filter)
    return [e for e in events if rx.search(str(e.get("name", "")))]


def _group(events: Iterable[Dict]) -> Dict[str, List[float]]:
    grouped: Dict[str, List[float]] = defaultdict(list)
    for event in events:
        grouped[str(event.get("name", "unknown"))].append(float(event.get("ms", 0.0)))
    return grouped


def _format_summary(rows: List[Tuple[str, Dict[str, float]]]) -> str:
    header = f"{'Span':50} {'Count':>7} {'Min':>8} {'P50':>8} {'P95':>8} {'P99':>8} {'Max':>8} {'Mean':>8}"
    lines = [header, "-" * len(header)]
    for name, stats in rows:
        lines.append(
            f"{name:50.50} {int(stats['count']):7d} {stats['min']:8.2f} {stats['p50']:8.2f} "
            f"{stats['p95']:8.2f} {stats['p99']:8.2f} {stats['max']:8.2f} {stats['mean']:8.2f}"
        )
    return "\n".join(lines)


def _markdown_summary(rows: List[Tuple[str, Dict[str, float]]]) -> str:
    out = [
        "| Span | Count | Min (ms) | P50 (ms) | P95 (ms) | P99 (ms) | Max (ms) | Mean (ms) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, stats in rows:
        out.append(
            "| "
            + " | ".join(
                [
                    name,
                    str(int(stats["count"])),
                    f"{stats['min']:.2f}",
                    f"{stats['p50']:.2f}",
                    f"{stats['p95']:.2f}",
                    f"{stats['p99']:.2f}",
                    f"{stats['max']:.2f}",
                    f"{stats['mean']:.2f}",
                ]
            )
            + " |"
        )
    return "\n".join(out)


def summarize_command(args: argparse.Namespace) -> int:
    log_paths = _resolve_log_paths(args.log)
    all_events: List[Dict] = []
    for path in log_paths:
        all_events.extend(_load_perf_events(path))
    if not all_events:
        print("No PERF events found in:")
        for path in log_paths:
            print(f"  - {path}")
        return 0
    events = _filter_events(all_events, args.name)
    if not events:
        if args.name:
            print(f"No PERF events matched filter: {args.name}")
            print(f"Unfiltered PERF events available: {len(all_events)}")
            print("Tip: in PowerShell, quote regex with single quotes, e.g. --name '^settings\\.'")
        else:
            print("No PERF events found after filtering.")
        print("Log files:")
        for path in log_paths:
            print(f"  - {path}")
        return 0

    grouped = _group(events)
    rows: List[Tuple[str, Dict[str, float]]] = []
    for name, samples in grouped.items():
        rows.append((name, summarize_samples(samples)))
    rows.sort(key=lambda item: item[1].get(args.sort, 0.0), reverse=True)
    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    print("Log files:")
    for path in log_paths:
        print(f"  - {path}")
    print(f"Events: {len(events)}  Distinct spans: {len(grouped)}")
    print(_format_summary(rows))

    if args.markdown:
        out_path = Path(args.markdown).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(_markdown_summary(rows), encoding="utf-8")
        print(f"Wrote markdown summary: {out_path}")

    overall = [float(e["ms"]) for e in events]
    if overall:
        print(f"Overall median span: {statistics.median(overall):.2f} ms")
    return 0


def reset_command(args: argparse.Namespace) -> int:
    log_paths = _resolve_log_paths(args.log)
    for log_path in log_paths:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("", encoding="utf-8")
        print(f"Cleared log file: {log_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FreqInOut performance span helper")
    sub = parser.add_subparsers(dest="cmd", required=True)

    summary = sub.add_parser("summarize", help="Summarize PERF spans from log")
    summary.add_argument("--log", default="auto", help="Path to freqinout.log (or 'auto')")
    summary.add_argument("--name", default=None, help="Regex filter for span name")
    summary.add_argument("--sort", default="p95", choices=["mean", "max", "p50", "p95", "p99", "count"], help="Sort column")
    summary.add_argument("--limit", type=int, default=50, help="Max rows to print")
    summary.add_argument("--markdown", default="", help="Optional markdown output path")
    summary.set_defaults(func=summarize_command)

    reset = sub.add_parser("reset-log", help="Clear freqinout.log")
    reset.add_argument("--log", default="auto", help="Path to freqinout.log (or 'auto')")
    reset.set_defaults(func=reset_command)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
