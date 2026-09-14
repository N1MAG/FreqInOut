#!/usr/bin/env python3
"""Audit source for UIA-0 text-size and control-geometry conformance.

This is deliberately a *static candidate* audit. It cannot know the font or
platform metrics at runtime, so it reports a small, reviewable set of literals
and hard locks instead of trying to prove a rendered layout is clipped.

Use an exception only when the literal is intentional and has been reviewed:

    widget.setHandleWidth(2)  # uia-0: ignore[narrow-splitter-handle] grip is styled and keyboard reachable

The comment may be on the preceding line. An exception needs a non-empty reason
and applies only to the named rule on the next audited declaration. The old
``suspicious small text-control height call`` wording is retained here for users
searching historical CI output; ``--threshold`` remains accepted but does not
hide larger fixed geometry, because UIA-0 audits it at any size.
"""
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal


# Public names retained for lightweight consumers of the earlier audit.
TEXT_WIDGET_HINTS = ()  # Replaced by class-based inference; do not use name hints.
ASSIGNMENT_RE = re.compile(
    r"(?P<target>(?:self\.)?[A-Za-z_]\w*)\s*=\s*(?P<class>Q[A-Za-z_]\w*)\s*\("
)
HEADER_ASSIGNMENT_RE = re.compile(
    r"(?P<target>(?:self\.)?[A-Za-z_]\w*)\s*=\s*(?:self\.)?[A-Za-z_]\w*\."
    r"(?:verticalHeader|horizontalHeader)\(\)"
)
HEIGHT_CALL_RE = re.compile(
    r"(?P<receiver>(?:self\.)?[A-Za-z_]\w*)\.(?P<call>set(?:Minimum|Maximum|Fixed)Height)"
    r"\(\s*(?P<value>\d+)\s*\)"
)
ROW_HEIGHT_RE = re.compile(
    r"(?P<receiver>(?:self\.)?[A-Za-z_]\w*(?:\.(?:verticalHeader|horizontalHeader)\(\))?)"
    r"\.(?P<call>set(?:Default)?SectionSize|setRowHeight)\([^\n]*?(?P<value>\d+)\s*\)"
)
HANDLE_RE = re.compile(r"\.(?:setHandleWidth|setHandleSize)\(\s*(?P<value>\d+)\s*\)")
FONT_CALL_RE = re.compile(r"\.set(?:PointSize|PixelSize)\(\s*\d+(?:\.\d+)?\s*\)")
FONT_STYLE_RE = re.compile(r"\bfont-size\s*:\s*[-+]?\d+(?:\.\d+)?(?:px|pt)\b", re.I)
COLOR_STYLE_RE = re.compile(
    r"\b(?:color|background(?:-color)?|border(?:-color)?)\s*:\s*"
    r"(?:#[0-9a-f]{3,8}\b|rgb(?:a)?\([^)]*\)|(?:black|white|red|green|blue|gray|grey)\b)",
    re.I,
)
EXCEPTION_RE = re.compile(
    r"#\s*uia-0:\s*ignore\[(?P<rule>[a-z0-9-]+)\]\s+(?P<reason>\S.*)$", re.I
)

TEXT_WIDGET_CLASSES = frozenset(
    {
        "QAbstractButton", "QCheckBox", "QComboBox", "QCommandLinkButton", "QDateEdit",
        "QDateTimeEdit", "QDoubleSpinBox", "QFontComboBox", "QGroupBox", "QLabel",
        "QLineEdit", "QPlainTextEdit", "QPushButton", "QRadioButton", "QSpinBox",
        "QTextBrowser", "QTextEdit", "QTimeEdit", "QToolButton",
    }
)
TABLE_CLASSES = frozenset({"QTableView", "QTableWidget", "QTreeView", "QTreeWidget"})
HEADER_CLASSES = frozenset({"QHeaderView"})
UNCONSTRAINED_HEIGHTS = frozenset({0, 16777215})


@dataclass(frozen=True)
class Finding:
    path: Path
    lineno: int
    severity: Literal["violation", "candidate"]
    rule: str
    message: str
    source: str


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8-sig").splitlines()


def _widget_classes(lines: Iterable[str]) -> dict[str, str]:
    """Infer only explicit Qt construction types; variable names are not evidence."""
    classes: dict[str, str] = {}
    for line in lines:
        match = ASSIGNMENT_RE.search(line)
        if match:
            classes[match.group("target")] = match.group("class")
        header = HEADER_ASSIGNMENT_RE.search(line)
        if header:
            classes[header.group("target")] = "QHeaderView"
    return classes


def _is_text_widget(receiver: str, classes: dict[str, str]) -> bool:
    return classes.get(receiver) in TEXT_WIDGET_CLASSES


def _is_table_or_header(receiver: str, classes: dict[str, str]) -> bool:
    base = ".".join(receiver.split(".")[:2]) if receiver.startswith("self.") else receiver.split(".", 1)[0]
    return (
        classes.get(base) in TABLE_CLASSES | HEADER_CLASSES
        or ".verticalHeader()" in receiver
        or ".horizontalHeader()" in receiver
    )


def _exception_for(lines: list[str], index: int, rule: str) -> bool:
    """Return whether this declaration has a documented, rule-specific waiver."""
    candidates = [lines[index]]
    if index and lines[index - 1].lstrip().startswith("#"):
        candidates.append(lines[index - 1])
    for candidate in candidates:
        match = EXCEPTION_RE.search(candidate)
        if match and match.group("rule").lower() == rule.lower():
            return True
    return False


def _add(
    findings: list[Finding], lines: list[str], path: Path, index: int,
    severity: Literal["violation", "candidate"], rule: str, message: str,
) -> None:
    if not _exception_for(lines, index, rule):
        findings.append(Finding(path, index + 1, severity, rule, message, lines[index].strip()))


def audit_findings(root: Path, *, threshold: int = 48) -> list[Finding]:
    """Return UIA-0 findings under ``freqinout/gui``.

    ``threshold`` is accepted for callers of the prior audit, but no longer
    filters literal geometry: a 60px hard lock is just as unsafe for Large Text
    as a 28px hard lock. It may be removed only in a future breaking release.
    """
    del threshold
    findings: list[Finding] = []
    gui_root = root / "freqinout" / "gui"
    for path in sorted(gui_root.rglob("*.py")):
        lines = _read_lines(path)
        classes = _widget_classes(lines)
        relative = path.relative_to(root)
        bounds: dict[str, dict[str, tuple[str, int]]] = {}

        for index, line in enumerate(lines):
            height = HEIGHT_CALL_RE.search(line)
            if height:
                receiver, call, raw_value = height.group("receiver", "call", "value")
                value = int(raw_value)
                # Qt uses 0 and QWIDGETSIZE_MAX as reset/unconstrain operations.
                if value not in UNCONSTRAINED_HEIGHTS and _is_text_widget(receiver, classes):
                    if call == "setFixedHeight":
                        _add(findings, lines, relative, index, "violation", "fixed-text-height",
                             "literal fixed height on an explicitly constructed text-bearing widget")
                    else:
                        _add(findings, lines, relative, index, "candidate", "literal-text-height",
                             "literal minimum/maximum height; verify it derives from font metrics")
                    bound = "minimum" if call == "setMinimumHeight" else "maximum"
                    if call != "setFixedHeight":
                        other = "maximum" if bound == "minimum" else "minimum"
                        previous = bounds.setdefault(receiver, {}).get(other)
                        if previous and previous[0] == raw_value:
                            _add(findings, lines, relative, index, "violation", "exact-text-height",
                                 "matching literal minimum and maximum heights lock text geometry")
                        bounds.setdefault(receiver, {})[bound] = (raw_value, index)

            row = ROW_HEIGHT_RE.search(line)
            if row and int(row.group("value")) and _is_table_or_header(row.group("receiver"), classes):
                _add(findings, lines, relative, index, "candidate", "fixed-table-row-height",
                     "literal table/header row height; verify readable rows at the largest text size")

            handle = HANDLE_RE.search(line)
            if handle:
                width = int(handle.group("value"))
                if width == 0:
                    _add(findings, lines, relative, index, "violation", "invisible-splitter-handle",
                         "splitter handle width is zero and cannot be grabbed")
                elif width <= 4:
                    _add(findings, lines, relative, index, "candidate", "narrow-splitter-handle",
                         "narrow splitter handle; verify visible, touchable, and keyboard-accessible")

            if FONT_CALL_RE.search(line) or FONT_STYLE_RE.search(line):
                _add(findings, lines, relative, index, "violation", "raw-font-size",
                     "raw font-size declaration bypasses the UI text-size setting")
            if COLOR_STYLE_RE.search(line):
                _add(findings, lines, relative, index, "candidate", "raw-literal-color",
                     "literal stylesheet color; verify shared theme token and contrast in both themes")

    return findings


def audit(root: Path, *, threshold: int = 48) -> list[tuple[Path, int, str, int, str]]:
    """Compatibility projection of literal text-height findings from the old API."""
    result: list[tuple[Path, int, str, int, str]] = []
    for finding in audit_findings(root, threshold=threshold):
        match = HEIGHT_CALL_RE.search(finding.source)
        if match and finding.rule in {"fixed-text-height", "literal-text-height", "exact-text-height"}:
            result.append((finding.path, finding.lineno, match.group("call"), int(match.group("value")), finding.source))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="List UIA-0 text-size/control-geometry conformance findings.")
    parser.add_argument("--root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--threshold", default=48, type=int, help="Deprecated compatibility option; does not filter findings.")
    args = parser.parse_args()

    findings = audit_findings(args.root.resolve(), threshold=int(args.threshold))
    for finding in findings:
        print(f"{finding.path}:{finding.lineno}: [{finding.severity}] {finding.rule}: {finding.message} :: {finding.source}")
    violations = sum(finding.severity == "violation" for finding in findings)
    candidates = len(findings) - violations
    print(f"\n{violations} hard UIA-0 violation(s), {candidates} informational candidate(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
