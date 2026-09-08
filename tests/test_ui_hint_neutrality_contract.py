from __future__ import annotations

import ast
from pathlib import Path
import re


GUI_ROOT = Path("freqinout/gui")
HINT_METHODS = {"setPlaceholderText", "setToolTip", "setStatusTip", "setWhatsThis"}
# A conservative amateur-radio-shaped token: one or two letters, one digit,
# then one to four letters. It catches identity-like examples without treating
# grid squares, radio models, frequencies, or protocol names as callsigns.
CALLSIGN_SHAPED_TOKEN = re.compile(r"(?<![A-Za-z0-9])[A-Z]{1,2}\d[A-Z]{1,4}(?![A-Za-z0-9])")


def _literal_text(node: ast.AST) -> str:
    return " ".join(
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    )


def test_gui_hints_do_not_embed_callsign_examples() -> None:
    offenders: list[str] = []
    for path in sorted(GUI_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in HINT_METHODS or not node.args:
                continue
            text = _literal_text(node.args[0])
            match = CALLSIGN_SHAPED_TOKEN.search(text)
            if match:
                offenders.append(f"{path}:{node.lineno}:{match.group(0)}")

    assert offenders == []


def test_hint_neutrality_rule_is_part_of_the_product_contract() -> None:
    contract = Path("docs/internal/multirig_product_ui_contract.md").read_text(encoding="utf-8")
    normalized = " ".join(contract.split())

    assert "## UI Hint Neutrality Contract" in contract
    assert "must not contain a real or plausible amateur-radio callsign" in normalized
