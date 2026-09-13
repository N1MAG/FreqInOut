from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QSplitter

from freqinout.core.traffic_actionability import build_operator_traffic_context
from freqinout.gui import fio_spotter_tab as spotter_ui
from freqinout.gui.fio_spotter_tab import FioSpotterTab


class _Settings:
    def __init__(self, **values):
        self.values = values

    def get(self, key: str, default=None):
        return self.values.get(key, default)

    def set(self, _key: str, _value: object) -> None:
        pass

    def save(self) -> None:
        pass


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_activity_compact_inspector_and_actions_are_selection_aware(monkeypatch) -> None:
    app = _app()
    rows = [{
        "message_id": "activity-1", "source_family": "js8", "from_call": "CALLSIGN",
        "summary": "Status update", "received_ts": 10.0,
    }]
    calls = []
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", lambda **_kwargs: calls.append(1) or list(rows))
    monkeypatch.setattr(
        spotter_ui,
        "load_operator_traffic_context",
        lambda *_args, **_kwargs: build_operator_traffic_context(callsign=""),
    )
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.resize(900, 560)
        tab.show()
        app.processEvents()
        split = tab.findChild(QSplitter, "fioSpotterActivitySplit")
        assert split is not None
        assert split.orientation() == Qt.Vertical
        assert not tab.activity_watch.isEnabled()
        tab.activity_table.selectRow(0)
        app.processEvents()
        assert tab.activity_watch.isEnabled()
        tab.activity_table.item(0, 0).setText("changed")
        tab.activity_table.selectRow(0)
        tab.activity_table.item(0, 0).setText("changed")
        tab.activity_table.item(0, 0).setData(Qt.UserRole, rows[0])
        tab.resize(860, 540)
        tab.activity_chip_row.setFocus()
        app.processEvents()
        assert len(calls) == 1
    finally:
        tab.deleteLater()


def test_activity_refresh_preserves_selected_projection_identity(monkeypatch) -> None:
    app = _app()
    rows = [{"message_id": "activity-1", "source_family": "spotter", "summary": "First"}]
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", lambda **_kwargs: list(rows))
    monkeypatch.setattr(
        spotter_ui,
        "load_operator_traffic_context",
        lambda *_args, **_kwargs: build_operator_traffic_context(callsign=""),
    )
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.activity_table.selectRow(0)
        rows[0] = {"message_id": "activity-1", "source_family": "spotter", "summary": "Updated"}
        tab.refresh_activity()
        assert tab.activity_table.currentRow() == 0
        assert tab.activity_detail.toPlainText().find("Updated") >= 0
    finally:
        tab.deleteLater()


def test_spotter_actions_use_shared_theme_roles() -> None:
    app = _app()
    tab = FioSpotterTab(settings=_Settings(ui_theme="dark"))
    try:
        assert "#4C9BD3" in tab.activity_watch.styleSheet()
        tab.tabs.setCurrentIndex(1)
        app.processEvents()
        assert "#4C9BD3" in tab.watch_save.styleSheet()
        assert "#E05252" in tab.watch_delete.styleSheet()
        assert not hasattr(tab, "watch_toggle")
    finally:
        tab.deleteLater()
