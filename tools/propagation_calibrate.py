from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from freqinout.core.config_paths import get_config_dir
from freqinout.core.propagation_calibration import (
    apply_suggested_settings,
    calibrate_blend_parameters,
    load_contact_events,
    write_calibration_report,
)


def parse_args() -> argparse.Namespace:
    base = get_config_dir() / "config"
    parser = argparse.ArgumentParser(
        description="Calibrate offline propagation blend constants from local history."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=base / "freqinout_nets.db",
        help="Path to freqinout_nets.db (default: user config path).",
    )
    parser.add_argument(
        "--settings-db",
        type=Path,
        default=base / "freqinout.db",
        help="Path to settings DB for --apply.",
    )
    parser.add_argument(
        "--target-type",
        choices=["ALL", "OPERATOR", "STATE", "REGION"],
        default="ALL",
        help="Optional target type filter.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional max events to load.")
    parser.add_argument(
        "--validation-fraction",
        type=float,
        default=0.2,
        help="Fraction of newest events used for validation.",
    )
    parser.add_argument(
        "--max-validation",
        type=int,
        default=4000,
        help="Max validation events sampled from validation tail.",
    )
    parser.add_argument(
        "--recent-window-days",
        type=int,
        default=30,
        help="Recent-window gate horizon in days.",
    )
    parser.add_argument(
        "--history-cap-days",
        type=int,
        default=365,
        help="Ignore history older than this many days during calibration.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Use reduced candidate grid for faster calibration.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=base / "propagation" / "prop_calibration_recommendation.json",
        help="Output JSON report path.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply suggested settings into settings DB.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    events = load_contact_events(
        args.db,
        target_type=args.target_type,
        limit=args.limit,
    )
    if len(events) < 60:
        print(
            "[propagation_calibrate] Not enough events for calibration. "
            f"Loaded {len(events)}; need at least 60."
        )
        return 2

    if args.fast:
        report = calibrate_blend_parameters(
            events,
            validation_fraction=args.validation_fraction,
            max_validation=args.max_validation,
            recent_window_days=args.recent_window_days,
            history_cap_days=args.history_cap_days,
            alpha_values=[1.0, 2.0, 3.0],
            beta_values=[2.0, 3.0, 4.0],
            half_life_values=[45, 75, 90],
            gate_attempt_values=[4.0, 8.0, 12.0],
            gate_unique_days_values=[2, 3],
            max_blend_values=[0.65, 0.75, 0.85],
        )
    else:
        report = calibrate_blend_parameters(
            events,
            validation_fraction=args.validation_fraction,
            max_validation=args.max_validation,
            recent_window_days=args.recent_window_days,
            history_cap_days=args.history_cap_days,
        )

    write_calibration_report(args.output, report)
    print(f"[propagation_calibrate] Report written: {args.output}")
    print(
        "[propagation_calibrate] Best candidate: "
        + json.dumps(report["best"], sort_keys=True)
    )
    print(f"[propagation_calibrate] Recommended mode: {report.get('recommended_mode', 'UNKNOWN')}")
    print(
        "[propagation_calibrate] Suggested settings: "
        + json.dumps(report["suggested_settings"], sort_keys=True)
    )
    if args.apply:
        apply_suggested_settings(args.settings_db, report["suggested_settings"])
        print(f"[propagation_calibrate] Applied suggested settings to: {args.settings_db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
