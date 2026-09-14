"""UIA-3 runtime geometry checks for net-control workspaces."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication

from freqinout.gui.js8call_net_control_tab import JS8CallNetControlTab


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _build_js8_layout_only(monkeypatch) -> JS8CallNetControlTab:
    for name in (
        "_apply_theme",
        "_load_settings",
        "_setup_timer",
        "_setup_clock_timer",
        "_setup_js8_rx_timer",
        "_update_clock_labels",
        "_update_suspend_state",
        "_refresh_auto_query_flags",
        "_refresh_qsy_options",
        "_refresh_ncs_session_context",
    ):
        monkeypatch.setattr(JS8CallNetControlTab, name, lambda self: None)
    return JS8CallNetControlTab()


def test_js8_ncs_reflows_at_audit_viewports_and_large_text(monkeypatch) -> None:
    app = _app()
    tab = _build_js8_layout_only(monkeypatch)
    try:
        tab.show()
        for width, height in ((1920, 1080), (1000, 700), (900, 560)):
            tab.resize(width, height)
            tab._reflow_ncs_layouts()
            app.processEvents()
            assert tab.js8_ncs_scroll_area.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
            assert tab.js8_ncs_scroll_area.horizontalScrollBar().maximum() == 0
            assert tab.checkin_table.isVisible() or tab.checkin_empty_label.isVisible()
            assert tab.start_btn.isVisible() and tab.end_btn.isVisible()
            assert tab._js8_setup_grid.indexOf(tab._js8_hold_label) >= 0

        target = "Field Exercise Net"
        tab.net_name_edit.setText(target)
        large = QFont(tab.font())
        large.setPointSizeF(max(large.pointSizeF(), 10.0) * 1.7)
        tab.setFont(large)
        tab.resize(900, 560)
        for _ in range(5):
            tab._reflow_ncs_layouts()
            app.processEvents()
        assert tab.net_name_edit.text() == target
        assert tab.js8_ncs_scroll_area.horizontalScrollBar().maximum() == 0
        assert tab.checkin_table.minimumHeight() >= tab.checkin_table.fontMetrics().lineSpacing()
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()
