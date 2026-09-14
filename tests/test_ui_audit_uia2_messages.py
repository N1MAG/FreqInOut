"""Bounded UIA-2 probes for Messages/Compose geometry and navigation.

These checks exercise the live widget tree at the audit viewports.  They use
scroll-range and font-derived item geometry instead of screenshot pixels, so
they remain useful across Qt styles and platforms.
"""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication

from freqinout.gui.message_viewer_tab import MessageViewerTab, UnifiedMessage


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _compose_tab(monkeypatch, tmp_path) -> MessageViewerTab:
    _app()
    for name in (
        "_setup_clock_timer",
        "_setup_timer",
        "_setup_js8_timer",
        "_setup_pending_timer",
        "_setup_bbs_auto_archive_timer",
        "_initial_refresh",
        "_refresh_compose_forms",
    ):
        monkeypatch.setattr(MessageViewerTab, name, lambda self: None)
    tab = MessageViewerTab()
    tab._db_path = lambda: tmp_path / "freqinout_nets.db"
    tab._save_settings = lambda: None
    return tab


def _settle(tab: MessageViewerTab) -> None:
    app = _app()
    tab._refresh_compose_layout_geometry_if_needed(force=True)
    app.processEvents()
    tab._refresh_compose_layout_geometry_if_needed(force=True)
    app.processEvents()


def test_compose_setup_reflows_without_horizontal_scroll_at_audit_viewports(monkeypatch, tmp_path) -> None:
    """All compose modes keep setup content in a vertical, bounded surface."""
    tab = _compose_tab(monkeypatch, tmp_path)
    try:
        tab.show()
        tab._set_messages_mode("Compose")
        for width, height in ((1920, 1080), (1000, 700), (900, 560)):
            tab.resize(width, height)
            for row in range(tab.compose_mode_selector.count()):
                tab.compose_mode_selector.setCurrentRow(row)
                _settle(tab)
                setup = tab.compose_setup_scroll
                assert setup.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
                assert setup.horizontalScrollBar().maximum() == 0, (
                    f"unexpected setup overflow at {width}x{height}, mode={row}: "
                    f"content={setup.widget().sizeHint().width()} widget={setup.widget().width()} "
                    f"min={setup.widget().minimumSizeHint().width()} viewport={setup.viewport().width()}"
                )
                assert setup.widget().width() <= setup.viewport().width(), (
                    f"setup child wider than viewport at {width}x{height}, mode={row}: "
                    f"child={setup.widget().width()} min={setup.widget().minimumSizeHint().width()} "
                    f"viewport={setup.viewport().width()}"
                )
                assert tab.compose_body_splitter.count() == 2
                for item_index in range(tab.compose_mode_selector.count()):
                    rect = tab.compose_mode_selector.visualItemRect(
                        tab.compose_mode_selector.item(item_index)
                    )
                    assert rect.isValid()
                    assert rect.left() >= 0
                    assert rect.right() < tab.compose_mode_selector.viewport().width()
    finally:
        tab.close()
        tab.deleteLater()


def test_compose_large_font_stacks_controls_and_preserves_draft_during_rapid_switching(
    monkeypatch, tmp_path
) -> None:
    """Scaled typography wraps the mode selector without losing the draft."""
    tab = _compose_tab(monkeypatch, tmp_path)
    app = _app()
    try:
        tab.resize(900, 560)
        tab.show()
        tab._set_messages_mode("Compose")
        large = QFont(tab.font())
        large.setPointSizeF(max(large.pointSizeF(), 10.0) * 1.7)
        tab.setFont(large)
        tab.compose_mode_selector.setFont(large)
        tab.compose_js8_target_edit.setText("GROUP")
        for row in (0, 1, 2, 3, 0, 2, 1, 3, 0):
            tab.compose_mode_selector.setCurrentRow(row)
            _settle(tab)
        assert tab.compose_js8_target_edit.text() == "GROUP"
        assert tab.compose_mode_selector.currentRow() == 0
        assert tab.compose_setup_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.compose_setup_scroll.horizontalScrollBar().maximum() == 0
        assert tab.compose_mode_selector.height() >= tab.compose_mode_selector.fontMetrics().lineSpacing()
        assert tab.compose_mode_selector.verticalScrollBar().maximum() == 0
        app.processEvents()
    finally:
        tab.close()
        tab.deleteLater()


def test_compose_geometry_cycles_are_idempotent_and_can_shrink(monkeypatch, tmp_path) -> None:
    """Repeated refreshes do not ratchet setup height, and floors can shrink."""
    tab = _compose_tab(monkeypatch, tmp_path)
    try:
        tab.resize(900, 560)
        tab.show()
        tab._set_messages_mode("Compose")
        tab.compose_mode_selector.setCurrentRow(2)
        _settle(tab)
        natural = tab.compose_setup_box.minimumHeight()
        for _ in range(6):
            _settle(tab)
            assert tab.compose_setup_box.minimumHeight() == natural
        tab.compose_setup_box.setMinimumHeight(natural + 300)
        _settle(tab)
        assert tab.compose_setup_box.minimumHeight() >= natural + 300
        tab.compose_setup_box.setMinimumHeight(0)
        _settle(tab)
        assert tab.compose_setup_box.minimumHeight() == natural
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_navigation_keeps_snapshot_counter_and_generation_boundary(monkeypatch, tmp_path) -> None:
    """Fast reader clicks remain snapshot-only and do not cross generations."""
    tab = _compose_tab(monkeypatch, tmp_path)
    try:
        rows = [
            UnifiedMessage("text", "Unread", "A", "B", float(index), "", f"M{index}", "test", "")
            for index in range(3)
        ]
        tab._reader_snapshot = rows
        tab._reader_index = 0
        tab._reader_open = True
        tab._reader_generation = 7
        rendered = []
        monkeypatch.setattr(tab, "_render_message_content", lambda row: rendered.append(row.title))
        monkeypatch.setattr(tab, "_scroll_reader_to_top", lambda: None)
        monkeypatch.setattr(tab, "_sync_reader_bbs_action", lambda row: None)
        monkeypatch.setattr(tab, "_sync_reader_delete_action", lambda row: None)
        tab._navigate_message_reader(1)
        # Re-entry is deliberately fenced during the paint/debounce window.
        tab._navigate_message_reader(1)
        assert rendered == ["M1"]
        assert tab._reader_index == 1
        assert tab.reader_position_label.text() == "2 of 3"
        assert tab._reader_generation == 7
    finally:
        tab.close()
        tab.deleteLater()
