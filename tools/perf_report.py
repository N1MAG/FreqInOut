#!/usr/bin/env python3
"""Generate a deterministic Slice 0 performance report from FIO logs."""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


def _load_report_module():
    """Load the pure parser without importing FIO's runtime package hooks."""
    module_path = Path(__file__).resolve().parents[1] / "freqinout" / "core" / "perf_log_report.py"
    spec = importlib.util.spec_from_file_location("fio_perf_log_report_cli", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load performance parser from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", help="FIO log files (macOS or Linux format)")
    parser.add_argument("--json", action="store_true", dest="as_json", help="emit machine-readable JSON")
    args = parser.parse_args()
    report_module = _load_report_module()
    report = report_module.report_for_paths(args.logs)
    if args.as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(report_module.format_report(report))
    return 1 if any(not item["passed"] and item["observed"] for item in report["metrics"].values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
