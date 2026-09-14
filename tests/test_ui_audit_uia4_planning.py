"""Bounded UIA-4 probes for planning, SOP, and schedule workspaces.

These tests intentionally use small in-memory doubles and source-level policy
checks.  They verify geometry ownership and theme/scanner contracts without
opening the application database or requiring runtime configuration.
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QScrollArea

from freqinout.gui.local_nets_tab import LocalNetEditorDialog
from freqinout.gui.peer_sched_tab import ManualPeerScheduleDialog


ROOT = pathlib.Path(__file__).resolve().parents[1]
GUI = ROOT / "freqinout" / "gui"


class _Settings:
    def get(self, _key: str, default=None):
        return default


class _Catalog:
    def list_sessions(self, *, active=True, limit=200):
        return ()

    def net_entries_by_keys(self, _keys):
        return {}


class _LocalStore:
    pass


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_planning_pages_have_vertical_owner_and_compact_reflow_contract() -> None:
    names = (
        "freq_planner_tab.py",
        "sop_tab.py",
        "daily_schedule_tab.py",
        "net_schedule_tab.py",
        "local_nets_tab.py",
        "peer_sched_tab.py",
    )
    for name in names[1:]:
        source = (GUI / name).read_text(encoding="utf-8")
        assert "setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source, name
    # Plan Builder has no outer form scroll; its two horizontal surfaces are
    # explicitly bounded chip/toolbar data surfaces.
    planner_source = (GUI / names[0]).read_text(encoding="utf-8")
    assert planner_source.count("setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)") == 2
    for name, marker in (
        ("freq_planner_tab.py", "_apply_responsive_layout"),
        ("sop_tab.py", "_apply_sop_responsive_layout"),
        ("daily_schedule_tab.py", "_update_daily_responsive_layout"),
        ("net_schedule_tab.py", "_update_net_responsive_layout"),
        ("peer_sched_tab.py", "_update_peer_responsive_layout"),
    ):
        assert marker in (GUI / name).read_text(encoding="utf-8")
    assert "setMinimumWidth(0)" in (GUI / "daily_schedule_tab.py").read_text(encoding="utf-8")
    assert "setMinimumWidth(0)" in (GUI / "net_schedule_tab.py").read_text(encoding="utf-8")


def test_local_net_editor_is_scrollable_at_compact_large_text_sizes() -> None:
    _app()
    dialog = LocalNetEditorDialog(_LocalStore(), _Catalog(), _Settings())
    try:
        dialog.name_edit.setText("Draft reminder")
        for width, height in ((1920, 1080), (1000, 700), (900, 560)):
            dialog.resize(width, height)
            _app().processEvents()
            scroll = dialog.findChild(QScrollArea)
            assert scroll is not None
            assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
            assert dialog.name_edit.text() == "Draft reminder"
    finally:
        dialog.close()
        dialog.deleteLater()


def test_peer_manual_dialog_uses_theme_role_and_preserves_draft() -> None:
    _app()
    dialog = ManualPeerScheduleDialog(initial={"callsign": "N0CALL", "mode": "JS8"})
    try:
        assert "#b22222" not in dialog.error_label.styleSheet().lower()
        assert dialog.callsign_edit.text() == "N0CALL"
        dialog.resize(900, 560)
        _app().processEvents()
        assert dialog.callsign_edit.text() == "N0CALL"
    finally:
        dialog.close()
        dialog.deleteLater()


def test_sop_print_css_waivers_are_rule_specific_and_scanner_clean() -> None:
    source = (GUI / "sop_tab.py").read_text(encoding="utf-8")
    assert source.count("ignore[raw-font-size]") == 5
    assert "offline print/export HTML rendering surface, not an app screen" in source

    scanner_path = ROOT / "tools" / "audit_ui_text_size_heights.py"
    spec = importlib.util.spec_from_file_location("uia4_audit_scanner", scanner_path)
    assert spec is not None and spec.loader is not None
    scanner = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = scanner
    spec.loader.exec_module(scanner)
    owned = {name for name in (
        "freq_planner_tab.py",
        "sop_tab.py",
        "daily_schedule_tab.py",
        "net_schedule_tab.py",
        "local_nets_tab.py",
        "peer_sched_tab.py",
    )}
    findings = [f for f in scanner.audit_findings(GUI, threshold=48) if f.path.name in owned]
    assert not [f for f in findings if f.severity == "violation"]
