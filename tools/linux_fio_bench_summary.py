from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


PIDSTAT_FILES = {
    "cpu": "pidstat_cpu.log",
    "mem": "pidstat_mem.log",
    "io": "pidstat_io.log",
    "ctx": "pidstat_ctx.log",
}

PIDSTAT_FIELD_PRIORITIES = {
    "cpu": ["%CPU", "%usr", "%system", "%wait"],
    "mem": ["RSS", "VSZ", "%MEM"],
    "io": ["kB_rd/s", "kB_wr/s", "kB_ccwr/s", "kB_dscd/s", "iodelay"],
    "ctx": ["cswch/s", "nvcswch/s"],
}

IGNORE_COMMAND_PATTERNS = [
    re.compile(pat, flags=re.IGNORECASE)
    for pat in [
        r"linux_fio_bench_capture\.sh",
        r"linux_fio_bench_summary\.py",
        r"\bpidstat\b",
        r"\bsar\b",
        r"\biostat\b",
        r"\bvmstat\b",
        r"\bsadc\b",
    ]
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize Linux FIO station benchmark capture folders created by tools/linux_fio_bench_capture.sh"
    )
    parser.add_argument("capture_dir", help="Path to capture directory")
    parser.add_argument(
        "--patterns",
        default="",
        help="Override pattern TSV file (default: capture_dir/target_process_patterns.tsv)",
    )
    parser.add_argument(
        "--top-commands",
        type=int,
        default=5,
        help="Top commands to keep per tracked app (default: 5)",
    )
    parser.add_argument(
        "--top-untracked",
        type=int,
        default=10,
        help="Top unmatched CPU commands to include (default: 10)",
    )
    parser.add_argument(
        "--stdout-only",
        action="store_true",
        help="Print summary to stdout only (do not write summary files)",
    )
    parser.add_argument("--text-out", default="", help="Optional text summary output path")
    parser.add_argument("--json-out", default="", help="Optional JSON summary output path")
    parser.add_argument("--markdown-out", default="", help="Optional Markdown summary output path")
    return parser.parse_args()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def safe_float(raw: str, default: float = 0.0) -> float:
    try:
        return float(raw)
    except Exception:
        return default


def safe_int(raw: str, default: int = 0) -> int:
    try:
        return int(raw)
    except Exception:
        return default


def parse_manifest_tsv(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    for line in read_text(path).splitlines():
        if not line.strip() or "\t" not in line:
            continue
        key, value = line.split("\t", 1)
        out[key.strip()] = value.strip()
    return out


def load_patterns(path: Path) -> List[Tuple[str, str, re.Pattern[str]]]:
    patterns: List[Tuple[str, str, re.Pattern[str]]] = []
    if not path.exists():
        raise FileNotFoundError(f"Pattern file not found: {path}")
    for lineno, line in enumerate(read_text(path).splitlines(), start=1):
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            raise ValueError(f"Invalid pattern line {lineno} in {path}: expected label<TAB>regex")
        label = parts[0].strip()
        regex = parts[1].strip()
        if not label or not regex:
            raise ValueError(f"Invalid pattern line {lineno} in {path}: empty label or regex")
        patterns.append((label, regex, re.compile(regex, flags=re.IGNORECASE)))
    return patterns


def is_int_token(token: str) -> bool:
    return token.isdigit()


def parse_pidstat_file(path: Path, metric_kind: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    if not path.exists():
        return records

    header_columns: List[str] = []
    data_field_count = 0

    for line in read_text(path).splitlines():
        if not line.strip():
            continue
        stripped = line.strip()
        if stripped.startswith("Linux "):
            continue
        tokens = line.split()
        if not tokens:
            continue
        if tokens[0].startswith("Average:"):
            continue

        if "UID" in tokens and "PID" in tokens and "Command" in tokens:
            pid_idx = tokens.index("PID")
            cmd_idx = len(tokens) - 1 - tokens[::-1].index("Command")
            if cmd_idx > pid_idx:
                header_columns = tokens[pid_idx + 1 : cmd_idx]
                data_field_count = len(header_columns)
            continue

        if not header_columns or data_field_count <= 0:
            continue

        uid_pid_idx = None
        max_start = max(0, len(tokens) - (data_field_count + 3))
        for i in range(max_start + 1):
            if i + 1 >= len(tokens):
                break
            if is_int_token(tokens[i]) and is_int_token(tokens[i + 1]):
                if len(tokens) > i + 2 + data_field_count:
                    uid_pid_idx = i
                    break
        if uid_pid_idx is None:
            continue

        uid = safe_int(tokens[uid_pid_idx])
        pid = safe_int(tokens[uid_pid_idx + 1])
        if pid <= 0:
            continue

        value_tokens = tokens[uid_pid_idx + 2 : uid_pid_idx + 2 + data_field_count]
        if len(value_tokens) != data_field_count:
            continue
        cmd_tokens = tokens[uid_pid_idx + 2 + data_field_count :]
        cmd = " ".join(cmd_tokens)
        timestamp = " ".join(tokens[:uid_pid_idx]).strip()

        values: Dict[str, float] = {}
        for name, raw_value in zip(header_columns, value_tokens):
            values[name] = safe_float(raw_value, 0.0)

        keep_fields = set(PIDSTAT_FIELD_PRIORITIES.get(metric_kind, []))
        filtered_values = {k: v for k, v in values.items() if (not keep_fields or k in keep_fields or k == "CPU")}
        if not filtered_values:
            filtered_values = values

        records.append(
            {
                "metric": metric_kind,
                "timestamp": timestamp,
                "uid": uid,
                "pid": pid,
                "cmd": cmd,
                "values": filtered_values,
            }
        )
    return records


def normalize_command(cmd: str) -> str:
    return " ".join(cmd.strip().split())


def should_ignore_command(cmd: str) -> bool:
    if not cmd:
        return True
    for pattern in IGNORE_COMMAND_PATTERNS:
        if pattern.search(cmd):
            return True
    return False


def match_label(cmd: str, patterns: List[Tuple[str, str, re.Pattern[str]]]) -> str | None:
    if should_ignore_command(cmd):
        return None
    for label, _raw, compiled in patterns:
        if compiled.search(cmd):
            return label
    return None


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (len(sorted_values) - 1) * (p / 100.0)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return float(sorted_values[lo])
    frac = rank - lo
    return float(sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac)


def summarize_distribution(values: Iterable[float]) -> Dict[str, float]:
    vals = [float(v) for v in values]
    if not vals:
        return {"count": 0, "min": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "mean": 0.0}
    vals_sorted = sorted(vals)
    return {
        "count": float(len(vals_sorted)),
        "min": float(vals_sorted[0]),
        "p50": percentile(vals_sorted, 50.0),
        "p95": percentile(vals_sorted, 95.0),
        "p99": percentile(vals_sorted, 99.0),
        "max": float(vals_sorted[-1]),
        "mean": float(statistics.fmean(vals_sorted)),
    }


def _new_app_agg() -> Dict[str, Any]:
    return {
        "cpu_total_by_ts": defaultdict(float),
        "cpu_usr_by_ts": defaultdict(float),
        "cpu_sys_by_ts": defaultdict(float),
        "cpu_wait_by_ts": defaultdict(float),
        "proc_ids_by_ts": defaultdict(set),
        "rss_kb_by_ts": defaultdict(float),
        "vsz_kb_by_ts": defaultdict(float),
        "mem_pct_by_ts": defaultdict(float),
        "rd_kb_s_by_ts": defaultdict(float),
        "wr_kb_s_by_ts": defaultdict(float),
        "io_total_kb_s_by_ts": defaultdict(float),
        "iodelay_by_ts": defaultdict(float),
        "ctx_s_by_ts": defaultdict(float),
        "cmd_stats": defaultdict(
            lambda: {
                "samples": 0,
                "cpu_sum": 0.0,
                "cpu_max": 0.0,
                "rss_max_kb": 0.0,
                "rss_sum_kb": 0.0,
                "io_total_max_kb_s": 0.0,
                "ctx_max_s": 0.0,
            }
        ),
    }


def aggregate_pidstat(
    pidstat_records: Dict[str, List[Dict[str, Any]]],
    patterns: List[Tuple[str, str, re.Pattern[str]]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, float]], Dict[str, int]]:
    app_aggs: Dict[str, Dict[str, Any]] = defaultdict(_new_app_agg)
    unmatched_cpu: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"samples": 0.0, "cpu_sum": 0.0, "cpu_max": 0.0}
    )
    metric_counts: Dict[str, int] = defaultdict(int)

    def update_cmd_stats(label: str, cmd: str, **kwargs: float) -> None:
        cmd_norm = normalize_command(cmd)
        if not cmd_norm:
            return
        stats = app_aggs[label]["cmd_stats"][cmd_norm]
        stats["samples"] += 1
        if "cpu" in kwargs:
            cpu = float(kwargs["cpu"])
            stats["cpu_sum"] += cpu
            stats["cpu_max"] = max(stats["cpu_max"], cpu)
        if "rss_kb" in kwargs:
            rss_kb = float(kwargs["rss_kb"])
            stats["rss_sum_kb"] += rss_kb
            stats["rss_max_kb"] = max(stats["rss_max_kb"], rss_kb)
        if "io_total_kb_s" in kwargs:
            io_total = float(kwargs["io_total_kb_s"])
            stats["io_total_max_kb_s"] = max(stats["io_total_max_kb_s"], io_total)
        if "ctx_s" in kwargs:
            ctx_s = float(kwargs["ctx_s"])
            stats["ctx_max_s"] = max(stats["ctx_max_s"], ctx_s)

    for rec in pidstat_records.get("cpu", []):
        cmd = normalize_command(rec.get("cmd", ""))
        values = rec.get("values", {})
        cpu_total = float(values.get("%CPU", 0.0))
        metric_counts["cpu"] += 1
        label = match_label(cmd, patterns)
        if label is None:
            if not should_ignore_command(cmd):
                st = unmatched_cpu[cmd]
                st["samples"] += 1.0
                st["cpu_sum"] += cpu_total
                st["cpu_max"] = max(st["cpu_max"], cpu_total)
            continue
        ts = rec.get("timestamp") or "unknown"
        agg = app_aggs[label]
        agg["cpu_total_by_ts"][ts] += cpu_total
        agg["cpu_usr_by_ts"][ts] += float(values.get("%usr", 0.0))
        agg["cpu_sys_by_ts"][ts] += float(values.get("%system", 0.0))
        agg["cpu_wait_by_ts"][ts] += float(values.get("%wait", 0.0))
        agg["proc_ids_by_ts"][ts].add(int(rec.get("pid", 0)))
        update_cmd_stats(label, cmd, cpu=cpu_total)

    for rec in pidstat_records.get("mem", []):
        cmd = normalize_command(rec.get("cmd", ""))
        label = match_label(cmd, patterns)
        if label is None:
            continue
        metric_counts["mem"] += 1
        values = rec.get("values", {})
        ts = rec.get("timestamp") or "unknown"
        agg = app_aggs[label]
        rss_kb = float(values.get("RSS", 0.0))
        vsz_kb = float(values.get("VSZ", 0.0))
        mem_pct = float(values.get("%MEM", 0.0))
        agg["rss_kb_by_ts"][ts] += rss_kb
        agg["vsz_kb_by_ts"][ts] += vsz_kb
        agg["mem_pct_by_ts"][ts] += mem_pct
        update_cmd_stats(label, cmd, rss_kb=rss_kb)

    for rec in pidstat_records.get("io", []):
        cmd = normalize_command(rec.get("cmd", ""))
        label = match_label(cmd, patterns)
        if label is None:
            continue
        metric_counts["io"] += 1
        values = rec.get("values", {})
        ts = rec.get("timestamp") or "unknown"
        agg = app_aggs[label]
        rd = float(values.get("kB_rd/s", 0.0))
        wr = float(values.get("kB_wr/s", 0.0))
        io_total = rd + wr
        agg["rd_kb_s_by_ts"][ts] += rd
        agg["wr_kb_s_by_ts"][ts] += wr
        agg["io_total_kb_s_by_ts"][ts] += io_total
        agg["iodelay_by_ts"][ts] += float(values.get("iodelay", 0.0))
        update_cmd_stats(label, cmd, io_total_kb_s=io_total)

    for rec in pidstat_records.get("ctx", []):
        cmd = normalize_command(rec.get("cmd", ""))
        label = match_label(cmd, patterns)
        if label is None:
            continue
        metric_counts["ctx"] += 1
        values = rec.get("values", {})
        ts = rec.get("timestamp") or "unknown"
        agg = app_aggs[label]
        ctx_total = float(values.get("cswch/s", 0.0)) + float(values.get("nvcswch/s", 0.0))
        agg["ctx_s_by_ts"][ts] += ctx_total
        update_cmd_stats(label, cmd, ctx_s=ctx_total)

    return app_aggs, unmatched_cpu, metric_counts


def build_app_summary(
    app_aggs: Dict[str, Dict[str, Any]],
    patterns: List[Tuple[str, str, re.Pattern[str]]],
    top_commands: int,
) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    ordered_labels = [label for label, _raw, _compiled in patterns]
    for label in app_aggs.keys():
        if label not in ordered_labels:
            ordered_labels.append(label)

    for label in ordered_labels:
        agg = app_aggs.get(label)
        if not agg:
            output[label] = {
                "present": False,
                "cpu_pct_total": summarize_distribution([]),
                "rss_mb_total": summarize_distribution([]),
                "io_total_kb_s": summarize_distribution([]),
                "io_read_kb_s": summarize_distribution([]),
                "io_write_kb_s": summarize_distribution([]),
                "ctx_switch_s_total": summarize_distribution([]),
                "process_count": summarize_distribution([]),
                "timestamp_counts": {"cpu": 0, "mem": 0, "io": 0, "ctx": 0},
                "top_commands": [],
            }
            continue

        cmd_rows = []
        for cmd, st in agg["cmd_stats"].items():
            samples = int(st["samples"])
            cpu_mean = (st["cpu_sum"] / samples) if samples else 0.0
            rss_mean_kb = (st["rss_sum_kb"] / samples) if samples else 0.0
            cmd_rows.append(
                {
                    "cmd": cmd,
                    "samples": samples,
                    "cpu_mean": round(cpu_mean, 3),
                    "cpu_max": round(float(st["cpu_max"]), 3),
                    "rss_mean_mb": round(rss_mean_kb / 1024.0, 3),
                    "rss_max_mb": round(float(st["rss_max_kb"]) / 1024.0, 3),
                    "io_total_max_kb_s": round(float(st["io_total_max_kb_s"]), 3),
                    "ctx_max_s": round(float(st["ctx_max_s"]), 3),
                }
            )
        cmd_rows.sort(
            key=lambda row: (row["cpu_max"], row["rss_max_mb"], row["io_total_max_kb_s"], row["samples"]),
            reverse=True,
        )

        output[label] = {
            "present": True,
            "cpu_pct_total": summarize_distribution(agg["cpu_total_by_ts"].values()),
            "cpu_usr_pct": summarize_distribution(agg["cpu_usr_by_ts"].values()),
            "cpu_sys_pct": summarize_distribution(agg["cpu_sys_by_ts"].values()),
            "cpu_wait_pct": summarize_distribution(agg["cpu_wait_by_ts"].values()),
            "rss_mb_total": summarize_distribution([v / 1024.0 for v in agg["rss_kb_by_ts"].values()]),
            "vsz_mb_total": summarize_distribution([v / 1024.0 for v in agg["vsz_kb_by_ts"].values()]),
            "mem_pct_total": summarize_distribution(agg["mem_pct_by_ts"].values()),
            "io_total_kb_s": summarize_distribution(agg["io_total_kb_s_by_ts"].values()),
            "io_read_kb_s": summarize_distribution(agg["rd_kb_s_by_ts"].values()),
            "io_write_kb_s": summarize_distribution(agg["wr_kb_s_by_ts"].values()),
            "iodelay": summarize_distribution(agg["iodelay_by_ts"].values()),
            "ctx_switch_s_total": summarize_distribution(agg["ctx_s_by_ts"].values()),
            "process_count": summarize_distribution([len(p) for p in agg["proc_ids_by_ts"].values()]),
            "timestamp_counts": {
                "cpu": len(agg["cpu_total_by_ts"]),
                "mem": len(agg["rss_kb_by_ts"]),
                "io": len(agg["io_total_kb_s_by_ts"]),
                "ctx": len(agg["ctx_s_by_ts"]),
            },
            "top_commands": cmd_rows[: max(0, top_commands)],
        }
    return output


def build_untracked_summary(unmatched_cpu: Dict[str, Dict[str, float]], limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for cmd, st in unmatched_cpu.items():
        samples = int(st["samples"])
        if samples <= 0:
            continue
        rows.append(
            {
                "cmd": cmd,
                "samples": samples,
                "cpu_mean": round(float(st["cpu_sum"]) / samples, 3),
                "cpu_max": round(float(st["cpu_max"]), 3),
            }
        )
    rows.sort(key=lambda row: (row["cpu_max"], row["cpu_mean"], row["samples"]), reverse=True)
    return rows[: max(0, limit)]


def parse_vmstat(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"present": False, "samples": 0}

    columns = ["r", "b", "swpd", "free", "buff", "cache", "si", "so", "bi", "bo", "in", "cs", "us", "sy", "id", "wa", "st"]
    samples: List[Dict[str, float]] = []

    for line in read_text(path).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("procs ") or stripped.startswith("r  b") or stripped.startswith("r b"):
            continue
        if not re.match(r"^\s*\d+", line):
            continue
        tokens = line.split()
        if len(tokens) < len(columns):
            continue
        numeric_tokens = tokens[: len(columns)]
        if not all(re.match(r"^-?\d+(?:\.\d+)?$", tok) for tok in numeric_tokens):
            continue
        samples.append({name: safe_float(tok) for name, tok in zip(columns, numeric_tokens)})

    if len(samples) > 1:
        samples = samples[1:]
    if not samples:
        return {"present": True, "samples": 0}

    def col(name: str) -> List[float]:
        return [float(row.get(name, 0.0)) for row in samples]

    swap_event_count = sum(1 for row in samples if row.get("si", 0.0) > 0 or row.get("so", 0.0) > 0)
    return {
        "present": True,
        "samples": len(samples),
        "run_queue_r": summarize_distribution(col("r")),
        "blocked_b": summarize_distribution(col("b")),
        "cpu_user_pct": summarize_distribution(col("us")),
        "cpu_sys_pct": summarize_distribution(col("sy")),
        "cpu_idle_pct": summarize_distribution(col("id")),
        "cpu_iowait_pct": summarize_distribution(col("wa")),
        "swap_in_s": summarize_distribution(col("si")),
        "swap_out_s": summarize_distribution(col("so")),
        "disk_blocks_in_s": summarize_distribution(col("bi")),
        "disk_blocks_out_s": summarize_distribution(col("bo")),
        "context_switches_s": summarize_distribution(col("cs")),
        "interrupts_s": summarize_distribution(col("in")),
        "swap_event_samples": swap_event_count,
    }


def parse_iostat(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"present": False, "samples": 0, "devices": {}}

    device_rows: List[Dict[str, Any]] = []
    current_headers: List[str] = []
    report_index = -1

    for line in read_text(path).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Linux "):
            continue
        if stripped.startswith("avg-cpu:"):
            current_headers = []
            continue
        if stripped.startswith("Device"):
            current_headers = line.split()
            report_index += 1
            continue
        if not current_headers:
            continue
        tokens = line.split()
        if len(tokens) < 2:
            continue
        device = tokens[0]
        header_cols = current_headers[1:]
        data_tokens = tokens[1:]
        if len(data_tokens) < len(header_cols):
            continue
        values = {col: safe_float(tok, 0.0) for col, tok in zip(header_cols, data_tokens)}
        device_rows.append({"report_index": report_index, "device": device, "values": values})

    if not device_rows:
        return {"present": True, "samples": 0, "devices": {}}

    report_ids = sorted({int(row["report_index"]) for row in device_rows if int(row["report_index"]) >= 0})
    if len(report_ids) > 1:
        first_report = report_ids[0]
        device_rows = [row for row in device_rows if int(row["report_index"]) != first_report]

    per_device: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    for row in device_rows:
        device = str(row["device"])
        for key, value in row["values"].items():
            per_device[device][key].append(float(value))

    def pick(values: Dict[str, List[float]], *names: str) -> List[float]:
        for name in names:
            if name in values:
                return values[name]
        return []

    devices_summary: Dict[str, Any] = {}
    for device, cols in per_device.items():
        util_vals = pick(cols, "%util", "util")
        await_vals = pick(cols, "await", "r_await")
        rkb_vals = pick(cols, "rkB/s", "rKB/s")
        wkb_vals = pick(cols, "wkB/s", "wKB/s")
        tps_vals = pick(cols, "tps")
        devices_summary[device] = {
            "samples": max((len(v) for v in cols.values()), default=0),
            "util_pct": summarize_distribution(util_vals),
            "await_ms": summarize_distribution(await_vals),
            "read_kb_s": summarize_distribution(rkb_vals),
            "write_kb_s": summarize_distribution(wkb_vals),
            "tps": summarize_distribution(tps_vals),
        }

    hotspots = sorted(
        (
            {
                "device": device,
                "util_max": summary["util_pct"]["max"],
                "await_max": summary["await_ms"]["max"],
                "rw_peak_kb_s": max(summary["read_kb_s"]["max"], summary["write_kb_s"]["max"]),
            }
            for device, summary in devices_summary.items()
        ),
        key=lambda row: (row["util_max"], row["await_max"], row["rw_peak_kb_s"]),
        reverse=True,
    )

    return {"present": True, "samples": len(device_rows), "devices": devices_summary, "hotspots": hotspots[:10]}


def fmt_num(value: float, decimals: int = 1) -> str:
    try:
        return f"{float(value):.{decimals}f}"
    except Exception:
        return "0.0"


def fmt_stat(stats: Dict[str, Any], key: str, decimals: int = 1) -> str:
    if not stats:
        return "0.0"
    return fmt_num(float(stats.get(key, 0.0)), decimals)


def format_app_table(apps: Dict[str, Any]) -> str:
    header = (
        f"{'App':15} {'CPU P95':>8} {'CPU Max':>8} {'RSS Max MB':>10} "
        f"{'IO RW Max':>10} {'Proc Max':>9} {'CPU Ts':>7}"
    )
    lines = [header, "-" * len(header)]
    for label, summary in apps.items():
        lines.append(
            f"{label:15.15} "
            f"{fmt_stat(summary['cpu_pct_total'], 'p95', 1):>8} "
            f"{fmt_stat(summary['cpu_pct_total'], 'max', 1):>8} "
            f"{fmt_stat(summary['rss_mb_total'], 'max', 1):>10} "
            f"{fmt_stat(summary['io_total_kb_s'], 'max', 1):>10} "
            f"{fmt_stat(summary['process_count'], 'max', 0):>9} "
            f"{int(summary.get('timestamp_counts', {}).get('cpu', 0)):>7d}"
        )
    return "\n".join(lines)


def render_text_summary(summary: Dict[str, Any]) -> str:
    manifest = summary.get("manifest", {})
    apps = summary.get("apps", {})
    vmstat = summary.get("vmstat", {})
    iostat = summary.get("iostat", {})
    untracked = summary.get("untracked_cpu_top", [])
    pidstat_metrics = summary.get("pidstat_metrics_present", {})

    lines: List[str] = []
    lines.append("FreqInOut Linux Station Benchmark Summary")
    lines.append("")
    lines.append(f"Capture dir: {summary.get('capture_dir')}")
    lines.append(
        f"Window: {manifest.get('capture_start_utc', 'unknown')} -> {manifest.get('capture_end_utc', 'unknown')}"
    )
    lines.append(
        f"Duration: requested={manifest.get('duration_requested_s', 'unknown')}s actual={manifest.get('duration_actual_s', 'unknown')}s interval={manifest.get('interval_s', 'unknown')}s"
    )
    lines.append(f"Stop reason: {manifest.get('stop_reason', 'unknown')}")
    lines.append(f"Host: {manifest.get('host', 'unknown')}  Kernel: {manifest.get('kernel', 'unknown')}")
    lines.append("")

    lines.append("PIDStat files present:")
    for key in ["cpu", "mem", "io", "ctx"]:
        present = pidstat_metrics.get(key, {}).get("present", False)
        count = pidstat_metrics.get(key, {}).get("records", 0)
        lines.append(f"  - {key}: {'yes' if present else 'no'} (records={count})")
    lines.append("")

    lines.append("Tracked App Summary (aggregate across matched processes)")
    lines.append(format_app_table(apps))
    lines.append("")

    lines.append("Top commands by tracked app")
    for label, app in apps.items():
        if not app.get("top_commands"):
            continue
        lines.append(f"- {label}:")
        for row in app["top_commands"]:
            cmd = row["cmd"]
            if len(cmd) > 120:
                cmd = cmd[:117] + "..."
            lines.append(
                "    "
                + f"cpu_max={row['cpu_max']:.1f}% rss_max={row['rss_max_mb']:.1f}MB "
                + f"io_peak={row['io_total_max_kb_s']:.1f}kB/s samples={row['samples']} :: {cmd}"
            )
    lines.append("")

    lines.append("VMStat summary")
    if not vmstat.get("present"):
        lines.append("  - vmstat.log not present")
    elif vmstat.get("samples", 0) == 0:
        lines.append("  - vmstat.log present but no samples parsed")
    else:
        lines.append(
            "  - CPU iowait %: "
            + f"p95={fmt_stat(vmstat['cpu_iowait_pct'], 'p95', 1)} "
            + f"max={fmt_stat(vmstat['cpu_iowait_pct'], 'max', 1)}"
        )
        lines.append(
            "  - Run queue r: "
            + f"p95={fmt_stat(vmstat['run_queue_r'], 'p95', 1)} "
            + f"max={fmt_stat(vmstat['run_queue_r'], 'max', 1)}"
        )
        lines.append(
            "  - Swap in/out blocks/s: "
            + f"si_max={fmt_stat(vmstat['swap_in_s'], 'max', 1)} so_max={fmt_stat(vmstat['swap_out_s'], 'max', 1)} "
            + f"(swap_event_samples={int(vmstat.get('swap_event_samples', 0))})"
        )
    lines.append("")

    lines.append("IOStat disk hotspots")
    if not iostat.get("present"):
        lines.append("  - iostat.log not present")
    elif not iostat.get("hotspots"):
        lines.append("  - iostat.log present but no device rows parsed")
    else:
        for row in iostat["hotspots"][:5]:
            lines.append(
                "  - "
                + f"{row['device']}: util_max={row['util_max']:.1f}% await_max={row['await_max']:.1f}ms rw_peak={row['rw_peak_kb_s']:.1f}kB/s"
            )
    lines.append("")

    lines.append("Top unmatched CPU consumers (not in tracked app patterns)")
    if not untracked:
        lines.append("  - none (or all unmatched commands filtered)")
    else:
        for row in untracked:
            cmd = row["cmd"]
            if len(cmd) > 140:
                cmd = cmd[:137] + "..."
            lines.append(
                "  - " + f"cpu_max={row['cpu_max']:.1f}% cpu_mean={row['cpu_mean']:.1f}% samples={row['samples']} :: {cmd}"
            )
    lines.append("")

    lines.append("Captured files")
    for name in [
        "pidstat_cpu.log",
        "pidstat_mem.log",
        "pidstat_io.log",
        "pidstat_ctx.log",
        "sar_system.log",
        "iostat.log",
        "vmstat.log",
        "process_snapshot_start.txt",
        "process_snapshot_end.txt",
        "operator_notes.txt",
        "warnings.txt",
    ]:
        path = Path(summary["capture_dir"]) / name
        lines.append(f"  - {name}: {'yes' if path.exists() else 'no'}")
    return "\n".join(lines)


def render_markdown_summary(summary: Dict[str, Any]) -> str:
    manifest = summary.get("manifest", {})
    apps = summary.get("apps", {})
    vmstat = summary.get("vmstat", {})
    iostat = summary.get("iostat", {})
    untracked = summary.get("untracked_cpu_top", [])

    lines: List[str] = []
    lines.append("# FIO Linux Station Benchmark Summary")
    lines.append("")
    lines.append(f"- Capture dir: `{summary.get('capture_dir')}`")
    lines.append(
        f"- Window (UTC): `{manifest.get('capture_start_utc', 'unknown')}` -> `{manifest.get('capture_end_utc', 'unknown')}`"
    )
    lines.append(
        f"- Duration: requested `{manifest.get('duration_requested_s', 'unknown')}s`, actual `{manifest.get('duration_actual_s', 'unknown')}s`, interval `{manifest.get('interval_s', 'unknown')}s`"
    )
    lines.append(f"- Stop reason: `{manifest.get('stop_reason', 'unknown')}`")
    lines.append("")
    lines.append("## Tracked apps")
    lines.append("")
    lines.append("| App | CPU P95 % | CPU Max % | RSS Max MB | IO RW Max kB/s | Proc Max | CPU Timestamp Samples |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for label, app in apps.items():
        lines.append(
            "| "
            + " | ".join(
                [
                    label,
                    f"{app['cpu_pct_total']['p95']:.1f}",
                    f"{app['cpu_pct_total']['max']:.1f}",
                    f"{app['rss_mb_total']['max']:.1f}",
                    f"{app['io_total_kb_s']['max']:.1f}",
                    f"{app['process_count']['max']:.0f}",
                    str(int(app.get("timestamp_counts", {}).get("cpu", 0))),
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("## VM / Disk clues")
    lines.append("")
    if vmstat.get("present") and vmstat.get("samples", 0) > 0:
        lines.append(
            f"- VMStat iowait %: p95 `{vmstat['cpu_iowait_pct']['p95']:.1f}`, max `{vmstat['cpu_iowait_pct']['max']:.1f}`"
        )
        lines.append(
            f"- VMStat run queue `r`: p95 `{vmstat['run_queue_r']['p95']:.1f}`, max `{vmstat['run_queue_r']['max']:.1f}`"
        )
        lines.append(f"- VMStat swap event samples: `{int(vmstat.get('swap_event_samples', 0))}`")
    else:
        lines.append("- VMStat summary unavailable")
    if iostat.get("hotspots"):
        lines.append("")
        lines.append("| Device | Max Util % | Max Await ms | Peak RW kB/s |")
        lines.append("|---|---:|---:|---:|")
        for row in iostat["hotspots"][:5]:
            lines.append(
                f"| {row['device']} | {row['util_max']:.1f} | {row['await_max']:.1f} | {row['rw_peak_kb_s']:.1f} |"
            )
    else:
        lines.append("- IOStat hotspot summary unavailable")
    lines.append("")
    lines.append("## Top unmatched CPU consumers")
    lines.append("")
    if not untracked:
        lines.append("- None (or filtered collector processes only)")
    else:
        for row in untracked:
            cmd = row["cmd"]
            if len(cmd) > 150:
                cmd = cmd[:147] + "..."
            lines.append(f"- `{row['cpu_max']:.1f}% max / {row['cpu_mean']:.1f}% mean` ({row['samples']} samples): `{cmd}`")
    return "\n".join(lines)


def build_summary(capture_dir: Path, args: argparse.Namespace) -> Dict[str, Any]:
    manifest_path = capture_dir / "manifest.tsv"
    manifest = parse_manifest_tsv(manifest_path)
    patterns_path = Path(args.patterns).resolve() if args.patterns else (capture_dir / "target_process_patterns.tsv")
    patterns = load_patterns(patterns_path)

    pidstat_records: Dict[str, List[Dict[str, Any]]] = {}
    pidstat_metrics_present: Dict[str, Any] = {}
    for metric_kind, filename in PIDSTAT_FILES.items():
        path = capture_dir / filename
        rows = parse_pidstat_file(path, metric_kind)
        pidstat_records[metric_kind] = rows
        pidstat_metrics_present[metric_kind] = {"present": path.exists(), "records": len(rows), "file": str(path)}

    app_aggs, unmatched_cpu, metric_counts = aggregate_pidstat(pidstat_records, patterns)
    apps = build_app_summary(app_aggs, patterns, args.top_commands)
    untracked_cpu_top = build_untracked_summary(unmatched_cpu, args.top_untracked)
    vmstat_summary = parse_vmstat(capture_dir / "vmstat.log")
    iostat_summary = parse_iostat(capture_dir / "iostat.log")

    warnings_path = capture_dir / "warnings.txt"
    warnings = []
    if warnings_path.exists():
        warnings = [line.strip() for line in read_text(warnings_path).splitlines() if line.strip()]

    return {
        "capture_dir": str(capture_dir),
        "manifest_path": str(manifest_path),
        "patterns_path": str(patterns_path),
        "manifest": manifest,
        "pattern_labels": [label for label, _raw, _compiled in patterns],
        "pidstat_metrics_present": pidstat_metrics_present,
        "pidstat_parsed_record_counts": {k: int(v) for k, v in metric_counts.items()},
        "apps": apps,
        "untracked_cpu_top": untracked_cpu_top,
        "vmstat": vmstat_summary,
        "iostat": iostat_summary,
        "warnings": warnings,
    }


def resolve_output_paths(capture_dir: Path, args: argparse.Namespace) -> Tuple[Path, Path, Path]:
    summary_dir = capture_dir / "summary"
    text_out = Path(args.text_out).resolve() if args.text_out else (summary_dir / "summary.txt")
    json_out = Path(args.json_out).resolve() if args.json_out else (summary_dir / "summary.json")
    md_out = Path(args.markdown_out).resolve() if args.markdown_out else (summary_dir / "summary.md")
    return text_out, json_out, md_out


def main() -> int:
    args = parse_args()
    capture_dir = Path(args.capture_dir).resolve()
    if not capture_dir.exists() or not capture_dir.is_dir():
        print(f"Capture directory not found: {capture_dir}")
        return 2

    try:
        summary = build_summary(capture_dir, args)
    except Exception as exc:
        print(f"Failed to build summary: {exc}")
        return 1

    text_summary = render_text_summary(summary)
    markdown_summary = render_markdown_summary(summary)

    if not args.stdout_only:
        text_out, json_out, md_out = resolve_output_paths(capture_dir, args)
        text_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.parent.mkdir(parents=True, exist_ok=True)
        md_out.parent.mkdir(parents=True, exist_ok=True)
        text_out.write_text(text_summary + "\n", encoding="utf-8")
        json_out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        md_out.write_text(markdown_summary + "\n", encoding="utf-8")
        print(f"Wrote text summary: {text_out}")
        print(f"Wrote JSON summary: {json_out}")
        print(f"Wrote Markdown summary: {md_out}")

    print()
    print(text_summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
