from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


TOOL_PATH = Path(__file__).resolve().parents[1] / "tools" / "audit_ui_text_size_heights.py"
SPEC = importlib.util.spec_from_file_location("audit_ui_text_size_heights", TOOL_PATH)
assert SPEC and SPEC.loader
AUDIT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = AUDIT
SPEC.loader.exec_module(AUDIT)


def _write_gui_fixture(tmp_path: Path, source: str) -> Path:
    gui = tmp_path / "freqinout" / "gui"
    gui.mkdir(parents=True)
    (gui / "fixture.py").write_text(source, encoding="utf-8")
    return tmp_path


def test_audit_classifies_seeded_uia0_geometry_and_style_findings(tmp_path: Path) -> None:
    root = _write_gui_fixture(
        tmp_path,
        """
label = QLabel()
label.setMinimumHeight(72)
label.setMaximumHeight(72)
button = QPushButton()
button.setFixedHeight(64)
table = QTableWidget()
header = table.verticalHeader()
header.setDefaultSectionSize(26)
splitter = QSplitter()
splitter.setHandleWidth(0)
splitter.setHandleWidth(2)
label.setStyleSheet("font-size: 14px; color: #112233;")
font.setPointSize(11)
""",
    )

    findings = AUDIT.audit_findings(root)
    by_rule = {finding.rule: finding for finding in findings}

    assert by_rule["literal-text-height"].severity == "candidate"
    assert by_rule["exact-text-height"].severity == "violation"
    assert by_rule["fixed-text-height"].severity == "violation"
    assert by_rule["fixed-table-row-height"].severity == "candidate"
    assert by_rule["invisible-splitter-handle"].severity == "violation"
    assert by_rule["narrow-splitter-handle"].severity == "candidate"
    assert by_rule["raw-font-size"].severity == "violation"
    assert by_rule["raw-literal-color"].severity == "candidate"
    assert all("fixture.py" in str(finding.path) and finding.source for finding in findings)


def test_audit_skips_zero_resets_and_requires_documented_rule_specific_exceptions(tmp_path: Path) -> None:
    root = _write_gui_fixture(
        tmp_path,
        """
label = QLabel()
label.setMinimumHeight(0)
label.setMaximumHeight(0)
label.setMaximumHeight(16777215)
button = QPushButton()
# uia-0: ignore[fixed-text-height] reviewed icon-only button has a scalable alternate action
button.setFixedHeight(36)
splitter = QSplitter()
# uia-0: ignore[narrow-splitter-handle] splitter grip is styled and keyboard reachable
splitter.setHandleWidth(2)
label.setStyleSheet("font-size: 12px;")  # uia-0: ignore[raw-font-size] test fixture intentional
label.setStyleSheet("font-size: 13px;")  # uia-0: ignore[raw-font-size]
label.setStyleSheet("color: #123456;")  # uia-0: ignore[wrong-rule] does not waive colors
""",
    )

    findings = AUDIT.audit_findings(root, threshold=1)
    rules = [finding.rule for finding in findings]

    assert "literal-text-height" not in rules
    assert "exact-text-height" not in rules
    assert "fixed-text-height" not in rules
    assert "narrow-splitter-handle" not in rules
    assert rules == ["raw-font-size", "raw-literal-color"]


def test_legacy_audit_api_still_projects_height_findings_at_any_literal_size(tmp_path: Path) -> None:
    root = _write_gui_fixture(
        tmp_path,
        """
control = QLineEdit()
control.setMinimumHeight(96)
""",
    )

    findings = AUDIT.audit(root, threshold=48)

    assert findings == [
        (Path("freqinout/gui/fixture.py"), 3, "setMinimumHeight", 96, "control.setMinimumHeight(96)")
    ]
