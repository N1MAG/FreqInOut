#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path


HEIGHT_CALL_RE = re.compile(
    r"\.(set(?:Minimum|Maximum|Fixed)Height)\(\s*(?P<value>\d+)\s*\)"
)
TEXT_WIDGET_HINTS = (
    "btn",
    "button",
    "combo",
    "edit",
    "label",
    "chip",
    "row_widget",
    "selector",
    "badge",
)


def _looks_text_bearing(line: str) -> bool:
    left = line.split(".set", 1)[0].lower()
    return any(hint in left for hint in TEXT_WIDGET_HINTS)


def audit(root: Path, *, threshold: int) -> list[tuple[Path, int, str, int, str]]:
    findings: list[tuple[Path, int, str, int, str]] = []
    for path in sorted((root / "freqinout" / "gui").glob("*.py")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            lines = path.read_text(encoding="utf-8-sig").splitlines()
        for lineno, line in enumerate(lines, start=1):
            match = HEIGHT_CALL_RE.search(line)
            if not match:
                continue
            value = int(match.group("value"))
            if value > threshold:
                continue
            if not _looks_text_bearing(line):
                continue
            findings.append((path.relative_to(root), lineno, match.group(1), value, line.strip()))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List suspicious fixed/capped small heights on likely text-bearing Qt widgets."
    )
    parser.add_argument("--root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--threshold", default=48, type=int)
    args = parser.parse_args()

    findings = audit(args.root.resolve(), threshold=int(args.threshold))
    for path, lineno, call, value, line in findings:
        print(f"{path}:{lineno}: {call}({value}) :: {line}")
    print(f"\n{len(findings)} suspicious small text-control height call(s) <= {int(args.threshold)} px")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
