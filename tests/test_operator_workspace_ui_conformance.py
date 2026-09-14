from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication, QBoxLayout

from freqinout.gui import local_operator_tab as local_operator_module
from freqinout.gui import local_report_history_tab as report_history_module
from freqinout.gui.local_operator_tab import LocalOperatorTab
from freqinout.gui.local_report_history_tab import LocalReportHistoryTab
from freqinout.gui.operator_history_tab import OperatorHistoryTab


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_operator_workspace_controls_follow_large_text_and_reflow(monkeypatch) -> None:
    app = _app()
    old_font = QFont(app.font())
    large_font = QFont(old_font)
    large_font.setPointSize(max(20, old_font.pointSize() + 8))
    app.setFont(large_font)
    monkeypatch.setattr(local_operator_module, "get_all_operators", lambda: [])
    monkeypatch.setattr(local_operator_module, "latest_report_summaries_for_callsigns", lambda _callsigns: {})
    monkeypatch.setattr(report_history_module, "list_local_reports", lambda **_kwargs: [])
    try:
        operators = LocalOperatorTab()
        reports = LocalReportHistoryTab()
        history = OperatorHistoryTab()
        operators.resize(600, 400)
        reports.resize(600, 400)
        history.resize(600, 400)
        app.processEvents()

        assert operators.table.verticalHeader().defaultSectionSize() >= operators.table.fontMetrics().lineSpacing()
        assert reports.detail_text.minimumHeight() >= reports.detail_text.fontMetrics().lineSpacing() * 3
        assert operators.filter_row.direction() == QBoxLayout.TopToBottom
        assert reports.filter_row.direction() == QBoxLayout.TopToBottom
        assert history.search_row.direction() == QBoxLayout.TopToBottom
    finally:
        app.setFont(old_font)


def test_report_theme_reflow_preserves_selected_detail(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(
        report_history_module,
        "list_local_reports",
        lambda **_kwargs: [
            {
                "id": 7,
                "status": "PRIORITY",
                "callsign": "K7ETC",
                "subject": "Long report",
                "body": "A detailed local report body.",
                "created_utc": "2026-08-10T12:00:00+00:00",
            }
        ],
    )
    tab = LocalReportHistoryTab()
    tab.table.selectRow(0)
    selected_id = tab._selected_report()["id"]
    detail = tab.detail_text.toPlainText()
    tab.resize(560, 360)
    tab.apply_theme()
    app.processEvents()

    assert tab._selected_report()["id"] == selected_id
    assert tab.detail_text.toPlainText() == detail
