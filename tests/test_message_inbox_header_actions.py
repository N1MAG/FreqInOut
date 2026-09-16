from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication, QTableView, QWidget

from freqinout.gui.message_viewer_tab import (
    JS8Message,
    MessageActionDelegate,
    MessageHeaderWithCheckbox,
    MessageTableModel,
    UnifiedMessage,
    _FontBoundedLineEdit,
)
from freqinout.core.js8_spotter_forms import SPOTTER_COMMENTS_MAX_LENGTH


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _row() -> UnifiedMessage:
    payload = JS8Message(
        msg_id=1,
        from_call="K7ETC",
        to_call="@MAGNET",
        msg_type="MSG",
        utc_str="2026-09-16 15:00:00",
        utc_ts=1.0,
        raw_text="TEST",
        decoded_text="TEST",
        state="UNREAD",
    )
    return UnifiedMessage("MSG", "UNREAD", "K7ETC", "MAGNET", 1.0, "", "Test", "js8", payload)


def test_inbox_header_exposes_labeled_select_all_button() -> None:
    app = _app()
    table = QTableView()
    model = MessageTableModel([_row()])
    table.setModel(model)
    header = MessageHeaderWithCheckbox(Qt.Horizontal, table)
    table.setHorizontalHeader(header)
    table.setColumnWidth(0, header.select_button_width_hint())
    table.show()
    app.processEvents()
    try:
        header.set_checkbox_state(Qt.Unchecked, enabled=True)
        assert header._select_all_button.text() == "Select all"
        assert header._select_all_button.isEnabled() is True
        assert header._select_all_button.width() > header._select_all_button.fontMetrics().horizontalAdvance("Select all")

        header.set_checkbox_state(Qt.Checked, enabled=True)
        assert header._select_all_button.text() == "Clear all"
        assert "visible messages" in header._select_all_button.accessibleName().lower()
    finally:
        table.close()
        table.deleteLater()


def test_inbox_action_column_is_one_compact_disclosure() -> None:
    model = MessageTableModel([_row()])
    index = model.index(0, 7)

    assert model.headerData(7, Qt.Horizontal, Qt.DisplayRole) == "Actions"
    assert model.data(index, Qt.DisplayRole) == "Actions…"
    assert model.data(index, Qt.ToolTipRole) == "Open actions for this message."


def test_inbox_action_disclosure_preserves_view_flag_and_delete_paths() -> None:
    _app()

    class Host(QWidget):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[str] = []

        def _on_view_message(self, _row) -> None:
            self.calls.append("view")

        def _cycle_flag_state(self, _payload) -> None:
            self.calls.append("flag")

        def _delete_js8_message(self, _payload) -> None:
            self.calls.append("delete")

    host = Host()
    delegate = MessageActionDelegate(host)
    try:
        menu = delegate._build_action_menu(_row())
        actions = [action for action in menu.actions() if not action.isSeparator()]
        assert [action.text() for action in actions] == ["View", "Mark for review", "Delete…"]

        for action in actions:
            action.trigger()
        assert host.calls == ["view", "flag", "delete"]
    finally:
        host.close()
        host.deleteLater()


def test_spotter_comment_control_is_compact_bounded_and_font_derived() -> None:
    _app()
    editor = _FontBoundedLineEdit(
        display_columns=34,
        max_length=SPOTTER_COMMENTS_MAX_LENGTH,
    )
    try:
        normal_width = editor.maximumWidth()
        assert editor.maxLength() == 50
        assert editor.minimumWidth() == editor.maximumWidth()

        font = QFont(editor.font())
        font.setPointSizeF(max(18.0, font.pointSizeF() + 6.0))
        editor.setFont(font)
        assert editor.maximumWidth() > normal_width
        assert editor.minimumWidth() == editor.maximumWidth()
    finally:
        editor.close()
        editor.deleteLater()
