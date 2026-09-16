from __future__ import annotations

import os
import inspect

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QRect, Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication, QStyleOptionViewItem, QTableView, QWidget

from freqinout.gui.message_viewer_tab import (
    FileRecord,
    JS8Message,
    MessageActionDelegate,
    MessageHeaderWithCheckbox,
    MessageRowAction,
    MessageTableModel,
    UnifiedMessage,
    _FontBoundedLineEdit,
)
from freqinout.core.message_projection_payload import ProjectedMessagePayload
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


def test_inbox_action_column_advertises_direct_actions() -> None:
    model = MessageTableModel([_row()])
    index = model.index(0, 7)

    assert model.headerData(7, Qt.Horizontal, Qt.DisplayRole) == "Actions"
    assert model.data(index, Qt.DisplayRole) == ""
    assert model.data(index, Qt.ToolTipRole) is None


def test_inbox_action_chips_preserve_view_flag_and_delete_paths() -> None:
    _app()

    class Host(QWidget):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[str] = []

        def _on_view_message(self, _row) -> None:
            self.calls.append("view")

        def _cycle_row_flag_state(self, _row) -> None:
            self.calls.append("flag")

        def _delete_js8_message(self, _payload) -> None:
            self.calls.append("delete")

    host = Host()
    delegate = MessageActionDelegate(host)
    try:
        row = _row()
        actions = delegate._action_items(row)
        assert [action.key for action in actions] == ["view", "flag", "delete"]
        assert [action.icon for action in actions] == ["view", "flag", "trash"]
        assert all(action.label == "" for action in actions)
        required_width = MessageActionDelegate.required_width(host.fontMetrics())
        assert required_width > host.fontMetrics().horizontalAdvance("+BBS") * 2
        option = QStyleOptionViewItem()
        option.initFrom(host)
        option.font = host.font()
        option.fontMetrics = host.fontMetrics()
        option.rect = QRect(0, 0, required_width, max(48, host.fontMetrics().lineSpacing() * 2))
        widest_actions = [
            MessageRowAction("view", "", "View message", "view"),
            MessageRowAction("flag", "", "Flag", "flag"),
            MessageRowAction("relay", "", "Add to Relay", "relay"),
            MessageRowAction("bbs", "+BBS", "Add to BBS", "bbs"),
            MessageRowAction("delete", "", "Delete", "trash", role="danger"),
        ]
        rects = [rect for _item, rect in delegate._chip_rects(option, widest_actions)]
        assert rects
        assert all(left.right() < right.left() for left, right in zip(rects, rects[1:]))
        assert rects[-1].right() < option.rect.right()

        paint_source = inspect.getsource(MessageActionDelegate.paint)
        assert "CE_PushButton" not in paint_source
        assert "action_chip_colors" in paint_source

        for action in actions:
            assert action.enabled is True
            assert delegate._trigger_action(action.key, row) is True
        assert host.calls == ["view", "flag", "delete"]
    finally:
        host.close()
        host.deleteLater()


def test_projected_flamp_actions_are_source_aware_and_cache_only(tmp_path) -> None:
    _app()

    class Host(QWidget):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[str] = []
            self.bbs_present = False

        def _file_record_for_message_row(self, row, *, allow_detail_lookup=False):
            assert allow_detail_lookup is False
            return FileRecord(
                tmp_path / "ABCD_report.k2s",
                "flamp",
                size=20,
                mtime=1.0,
            )

        def _cached_row_in_flamp_relay(self, _row) -> bool:
            return False

        def _cached_row_in_varac_bbs(self, _row) -> bool:
            return self.bbs_present

        def _can_copy_row_to_flamp_relay(self, _row):
            raise AssertionError("paint-time capability must not parse Relay files")

        def _can_copy_row_to_varac_bbs(self, _row):
            raise AssertionError("paint-time capability must not query BBS")

        def _on_view_message(self, _row) -> None:
            self.calls.append("view")

        def _cycle_row_flag_state(self, _row) -> None:
            self.calls.append("flag")

        def _copy_row_to_flamp_relay(self, _row) -> None:
            self.calls.append("relay")

        def _copy_row_to_varac_bbs(self, _row) -> None:
            self.calls.append("bbs")

        def _remove_row_from_varac_bbs(self, _row) -> None:
            self.calls.append("bbs_remove")

        def _delete_projected_file_message(self, _row) -> None:
            self.calls.append("delete")

    payload = ProjectedMessagePayload(
        message_id="m1",
        canonical_key="c1",
        source_family="flamp",
        external_refs=(
            {
                "external_kind": "flamp_file",
                "external_path": str(tmp_path / "ABCD_report.k2s"),
                "external_mtime": 1.0,
                "external_size": 20,
            },
        ),
    )
    row = UnifiedMessage("FLAMP", "NEW", "", "", 1.0, "", "Report", "flamp", payload)
    host = Host()
    delegate = MessageActionDelegate(host)
    try:
        actions = delegate._action_items(row)
        assert [action.key for action in actions] == ["view", "flag", "relay", "bbs", "delete"]
        assert next(action for action in actions if action.key == "bbs").label == "+BBS"
        for key in ("flag", "relay", "bbs", "delete"):
            assert delegate._trigger_action(key, row) is True
        assert host.calls == ["flag", "relay", "bbs", "delete"]

        host.bbs_present = True
        actions = delegate._action_items(row)
        bbs_action = next(action for action in actions if action.icon == "bbs")
        assert bbs_action.key == "bbs_remove"
        assert bbs_action.label == "-BBS"
        assert bbs_action.active is True
        assert delegate._trigger_action("bbs_remove", row) is True
        assert host.calls[-1] == "bbs_remove"
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
