#!/usr/bin/env python3
"""Run bounded, hardware-free MES-0 scheduler characterizations.

This is deliberately a characterization tool.  A successful exit means the
current one-radio success path and the legacy three-radio station-global
blocking defect were reproduced safely and workers were cleaned up.  Dispatch
timings are harness-only decision measurements, never production endpoint
performance claims.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Optional, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.support.scheduler_fault_harness import run_mes0_baseline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the complete correlation and resource evidence as JSON",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_mes0_baseline()
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        single = result["single_radio"]
        blocking = result["three_radio_global_blocking"]
        print("MES-0 scheduler characterization baseline")
        print(f"  one-radio successful control sequence: {single['success_reproduced']}")
        print(f"  one-radio worker resources stable after cleanup: {single['resources_stable']}")
        print(f"  three-radio legacy defect reproduced: {blocking['defect_reproduced']}")
        print(f"  healthy peers blocked while Radio A is hung: {blocking['healthy_peer_dispatches']}")
        print(f"  three-radio worker resources stable after cleanup: {blocking['resources_stable']}")
        print(f"  one-radio decision timing (harness only): {single['dispatch_decision_timing']}")
        print(f"  three-radio decision timing (harness only): {blocking['dispatch_decision_timing']}")
    return 0 if result["all_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
