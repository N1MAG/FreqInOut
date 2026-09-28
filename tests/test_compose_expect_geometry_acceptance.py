"""Responsive acceptance checks for the Expect editor and Compose workspaces.

These checks deliberately assert semantic structure and usable geometry rather
than screenshot pixels.  They are intended to catch regressions where a
responsive pass makes a field unusable, starves a setup pane, or loses a draft.
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QAbstractSpinBox, QLineEdit, QScrollArea, QTextEdit

from freqinout.gui import fio_spotter_tab as spotter_ui
from freqinout.gui.fio_spotter_tab import FioSpotterTab
from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.gui.theme import control_height_for_font


ROOT = Path(__file__).resolve().parents[1]


class _Settings:
    config_dir = "/tmp"

    def get(self, _key: str, default=None):
        return default

    def set(self, _key: str, _value: object) -> None:
        pass

    def save(self) -> None:
        pass


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _open_expect(monkeypatch) -> FioSpotterTab:
    _app()
    for name in (
        "list_expect_entries",
        "list_expect_allow_policies",
        "list_expect_operator_access_catalog",
        "list_expect_runtime_audit",
        "list_expect_dispatch_audit",
        "list_flamp_transfer_index_statuses",
    ):
        monkeypatch.setattr(spotter_ui, name, lambda **_kwargs: [])
    tab = FioSpotterTab(settings=_Settings())
    tab.resize(1800, 900)
    expect_index = next(
        index for index in range(tab.tabs.count())
        if tab.tabs.tabText(index) == "Expect"
    )
    tab.tabs.setCurrentIndex(expect_index)
    tab.show()
    _app().processEvents()
    return tab


def test_expect_reply_is_multiline_and_metadata_has_responsive_scan_path(monkeypatch) -> None:
    """Reply is comfortable to edit and metadata does not consume reply space."""
    tab = _open_expect(monkeypatch)
    try:
        reply = tab.expect_reply
        assert isinstance(reply, QTextEdit), "Reply must support comfortable multiline editing"
        reply.setPlainText("Line one\nLine two")
        assert reply.toPlainText() == "Line one\nLine two"
        assert reply.height() >= reply.fontMetrics().lineSpacing() * 2

        metadata = (tab.expect_key, tab.expect_policy, tab.expect_policy_summary, tab.expect_delivery_mode)
        # On a wide surface the identity/policy/mode summary is a compact row;
        # a reply editor remains below it rather than being squeezed beside it.
        ys = [widget.geometry().top() for widget in metadata]
        assert max(ys) - min(ys) <= reply.fontMetrics().lineSpacing() * 2
        assert reply.geometry().top() > max(widget.geometry().bottom() for widget in metadata)

        tab.resize(900, 560)
        _app().processEvents()
        scroll = tab.tabs.currentWidget().findChild(QScrollArea)
        assert scroll is not None
        assert scroll.horizontalScrollBar().maximum() == 0
        assert reply.width() >= reply.fontMetrics().horizontalAdvance("Reply text")
    finally:
        tab.close()
        tab.deleteLater()


def _open_compose(monkeypatch, tmp_path) -> MessageViewerTab:
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
    tab.resize(1500, 900)
    tab.show()
    tab._set_messages_mode("Compose")
    _app().processEvents()
    return tab


def test_compose_all_modes_have_bounded_surfaces_and_preserve_draft_on_resize(monkeypatch, tmp_path) -> None:
    """Each compose mode remains usable at desktop, medium, and compact sizes."""
    tab = _open_compose(monkeypatch, tmp_path)
    try:
        assert tab.compose_mode_selector.count() == 5
        for row in range(tab.compose_mode_selector.count()):
            tab.compose_mode_selector.setCurrentRow(row)
            _app().processEvents()
            assert tab.compose_body_splitter.count() == 2
            setup = tab.compose_setup_scroll
            assert setup.horizontalScrollBarPolicy() != Qt.ScrollBarAlwaysOn
            assert setup.width() > 0

            # The target field is shared by the RF modes.  Its value must be
            # a working draft, not a side effect of geometry changes.
            target = getattr(tab, "compose_js8_target_edit", None)
            if isinstance(target, QLineEdit):
                target.setText("GROUP")
                expected = target.text()
                for width, height in ((1100, 700), (900, 560), (1500, 900)):
                    tab.resize(width, height)
                    _app().processEvents()
                    assert target.text() == expected

            # Numeric controls are never allowed to collapse below their own
            # readable hint height during mode switches.
            for widget in tab.findChildren(QAbstractSpinBox):
                if widget.isVisible():
                    assert widget.height() >= widget.sizeHint().height()
    finally:
        tab.close()
        tab.deleteLater()


def test_compose_resize_refresh_path_is_cache_only() -> None:
    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    start = source.index("    def _refresh_compose_layout_geometry")
    end = source.index("    def _compose_embedded_needs_workbench", start)
    body = source[start:end].lower()
    assert "sqlite" not in body
    assert "path(" not in body
    assert "open(" not in body
    assert "subprocess" not in body


def test_commstat_status_grid_preserves_font_sized_rows_in_its_scroll_area(
    monkeypatch, tmp_path
) -> None:
    """CommStat status controls scroll rather than overlap at compact or large text sizes."""
    tab = _open_compose(monkeypatch, tmp_path)
    try:
        tab.compose_mode_selector.setCurrentRow(3)
        _app().processEvents()
        status_controls = tab.compose_commstat_status_widgets
        status_labels = tab.compose_commstat_status_labels
        status_columns = (
            ("overall", "water", "communications", "internet", "food", "civil_unrest"),
            ("power", "medical", "travel", "fuel", "crime", "political"),
        )

        for width, height, large_text in (
            (1500, 900, False),
            (900, 560, False),
            (900, 560, True),
        ):
            if large_text:
                large_font = tab.font()
                large_font.setPointSize(max(20, large_font.pointSize() + 8))
                for widget in (*status_controls.values(), *status_labels.values()):
                    widget.setFont(large_font)
            tab.resize(width, height)
            tab._refresh_compose_layout_geometry_if_needed(force=True)
            _app().processEvents()
            _app().processEvents()

            content = tab.compose_commstat_row_widget
            assert content.height() >= content.minimumSizeHint().height()
            assert content.minimumHeight() >= content.minimumSizeHint().height()
            for combo in status_controls.values():
                readable_height = control_height_for_font(
                    combo,
                    vertical_padding=10,
                    floor=max(28, combo.sizeHint().height(), combo.minimumSizeHint().height()),
                )
                assert combo.height() >= readable_height
            for column in status_columns:
                rects = [status_controls[key].geometry() for key in column]
                assert all(upper.bottom() < lower.top() for upper, lower in zip(rects, rects[1:]))
    finally:
        tab.close()
        tab.deleteLater()
