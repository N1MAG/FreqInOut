"""Static UI tab manifest coverage.

The test reads source only; it does not construct ``MainWindow`` and therefore
does not require runtime profiles, databases, endpoint processes, or network
I/O. Dynamic data-driven tabs are intentionally outside this principal-label
manifest and remain covered by their owning tab tests.
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.ui_audit_harness import SCREEN_KEY_MANIFEST


ROOT = Path(__file__).resolve().parents[1]

PRINCIPAL_NESTED_TAB_MANIFEST = {
    "freqinout/gui/fio_spotter_tab.py": {
        "Watches",
        "Expect",
        "Access Policies",
        "Forms",
        "Imports",
    },
    "freqinout/gui/resources_tab.py": {
        "Frequency Catalog",
        "Net Directory",
        "Import / Export",
    },
    "freqinout/gui/shortwave_tab.py": {"Explore", "Listening", "Data Sources"},
    "freqinout/gui/station_bbs_tab.py": {
        "Radio Service",
        "Locations && Access",
        "Publishing",
        "Automation",
        "Visitor Preview",
        "Visitor Helpers",
    },
    "freqinout/gui/message_viewer_tab.py": {
        "Delete Audit",
        "Hidden CommStat",
        "Message Index",
    },
    "freqinout/gui/station_health_tab.py": {
        "Issues",
        "Runtime Sources",
        "Scheduler Log",
    },
    "freqinout/gui/fldigi_net_control_tab.py": {
        "Reference",
        "Compare Results",
        "Review",
    },
    "freqinout/gui/stations_map_tab.py": {
        "Overview",
        "Status",
        "Paths",
        "Inbox",
    },
    "freqinout/gui/settings_tab.py": {
        "Radio Paths",
        "Inbound Guard",
    },
    "freqinout/gui/station_overview_tab.py": {"Overview"},
}

PRINCIPAL_SELECTOR_VIEW_MANIFEST = {
    "freqinout/gui/freq_planner_tab.py": {
        "Effective Windows",
        "Pattern Summary",
        "Radio Windows",
        "Week Grid",
        "SOP Lanes",
    },
    "freqinout/gui/main_window.py": {"Inbox", "Compose", "Main", "Radios", "Software"},
}


def _main_window_registered_screen_keys() -> tuple[str, ...]:
    source_path = ROOT / "freqinout/gui/main_window.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Attribute) and target.attr == "_screens" for target in node.targets):
            continue
        if not isinstance(node.value, ast.List):
            continue
        labels: list[str] = []
        for entry in node.value.elts:
            if not isinstance(entry, ast.Tuple) or not entry.elts:
                continue
            label = entry.elts[0]
            if isinstance(label, ast.Constant) and isinstance(label.value, str):
                labels.append(label.value)
        if labels:
            return tuple(labels)
    raise AssertionError("MainWindow._screens registration list was not found")


def _literal_add_tab_labels(source_path: Path) -> set[str]:
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    labels: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "addTab" or len(node.args) < 2:
            continue
        label = node.args[1]
        if isinstance(label, ast.Constant) and isinstance(label.value, str):
            labels.add(label.value)
    return labels


def test_main_window_screen_manifest_covers_every_registered_screen_key() -> None:
    registered = _main_window_registered_screen_keys()
    assert len(registered) == len(set(registered)), "duplicate MainWindow screen key"
    assert set(registered) == set(SCREEN_KEY_MANIFEST)
    assert registered == SCREEN_KEY_MANIFEST


def test_principal_nested_tab_manifest_covers_static_add_tab_labels() -> None:
    missing: dict[str, set[str]] = {}
    for relative_path, expected in PRINCIPAL_NESTED_TAB_MANIFEST.items():
        source_path = ROOT / relative_path
        observed = _literal_add_tab_labels(source_path)
        source = source_path.read_text(encoding="utf-8-sig")
        # Some tab shells iterate an authoritative constant/tuple and therefore
        # have no literal as addTab's second argument. Require both the literal
        # label and an addTab construction seam in those modules.
        absent = {
            label
            for label in expected
            if label not in observed and repr(label) not in source and f'"{label}"' not in source
        }
        if expected and not observed:
            assert ".addTab(" in source, f"{relative_path} has labels but no addTab seam"
        if absent:
            missing[relative_path] = absent
    assert not missing, f"principal nested tab labels missing from source: {missing!r}"


def test_principal_selector_views_are_present_without_constructing_runtime_tabs() -> None:
    missing: dict[str, set[str]] = {}
    for relative_path, expected in PRINCIPAL_SELECTOR_VIEW_MANIFEST.items():
        source = (ROOT / relative_path).read_text(encoding="utf-8-sig")
        absent = {label for label in expected if label not in source}
        if absent:
            missing[relative_path] = absent
    assert not missing, f"principal selector views missing from source: {missing!r}"
