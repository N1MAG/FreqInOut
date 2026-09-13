"""Regression probes for the responsive FIOSpotter Compose setup pane."""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QGridLayout, QSizePolicy

from freqinout.gui.message_viewer_tab import MessageViewerTab


ROOT = Path(__file__).resolve().parents[1]


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _tab(monkeypatch, tmp_path) -> MessageViewerTab:
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


def test_spotter_wide_setup_keeps_source_form_and_guidance_readable(monkeypatch, tmp_path) -> None:
    """The wide sidebar must fit Spotter selectors while the main pane wins space."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        tab._compose_mode = "spotter"
        tab.resize(1400, 900)
        tab.show()
        tab._set_messages_mode("Compose")
        tab._update_compose_preview()
        tab._refresh_compose_layout_geometry_if_needed(force=True)
        app.processEvents()

        assert tab.compose_body_splitter.orientation() == Qt.Horizontal
        assert tab.compose_setup_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.compose_setup_scroll.width() >= 470
        sizes = tab.compose_body_splitter.sizes()
        assert sizes[0] >= 470
        assert sizes[1] > sizes[0]

        assert tab.compose_spotter_source_row_widget.isVisible()
        assert tab.compose_spotter_category_label.isVisible()
        assert tab.compose_form_label.isVisible()
        assert tab.compose_expect_hint_label.wordWrap() is True
        assert tab.compose_spotter_source_hint.wordWrap() is True
        assert tab.compose_spotter_source_hint.sizePolicy().horizontalPolicy() == QSizePolicy.Ignored
    finally:
        tab.close()
        tab.deleteLater()


def test_spotter_compact_setup_stacks_and_owns_overflow(monkeypatch, tmp_path) -> None:
    """Compact Compose uses the vertical body splitter and local setup scroll."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        tab._compose_mode = "spotter"
        tab.resize(760, 700)
        tab.show()
        tab._set_messages_mode("Compose")
        tab._update_compose_preview()
        tab._refresh_compose_layout_geometry_if_needed(force=True)
        app.processEvents()

        assert tab.compose_body_splitter.orientation() == Qt.Vertical
        assert tab.compose_setup_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.compose_setup_scroll.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.compose_setup_scroll.isVisible()
        assert tab.compose_spotter_source_row_widget.isVisible()
        assert tab.compose_spotter_category_label.isVisible()
        assert tab.compose_form_label.isVisible()
    finally:
        tab.close()
        tab.deleteLater()


def test_spotter_setup_scroll_resets_only_on_open_or_editor_change(monkeypatch, tmp_path) -> None:
    """Activation/form transitions reveal Send From; typing preserves scrolling."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        tab._compose_mode = "spotter"
        tab.resize(760, 700)
        tab.show()
        tab._set_messages_mode("Compose")
        tab._update_compose_preview()
        # Make the setup content intentionally taller than its compact pane so
        # this test exercises a real scrollbar rather than a zero-range bar.
        tab.compose_setup_box.setMinimumHeight(1400)
        tab._refresh_compose_layout_geometry_if_needed(force=True)
        app.processEvents()
        bar = tab.compose_setup_scroll.verticalScrollBar()
        assert bar.maximum() > 0

        bar.setValue(bar.maximum())
        scrolled_value = bar.value()
        tab._update_compose_preview()
        app.processEvents()
        assert bar.value() == scrolled_value

        tab.show_compose_from_navigation()
        app.processEvents()
        assert bar.value() == 0

        bar.setValue(bar.maximum())
        tab._on_compose_spotter_source_changed()
        app.processEvents()
        assert bar.value() == 0

        bar.setValue(bar.maximum())
        tab._on_compose_form_changed()
        app.processEvents()
        assert bar.value() == 0
        assert tab._compose_spotter_scroll_reset_pending is False

        # Redisplaying the same form during an unrelated refresh is not a
        # form change and must keep the operator's current scroll position.
        bar.setValue(bar.maximum())
        tab._on_compose_form_changed()
        app.processEvents()
        assert bar.value() > 0
    finally:
        tab.close()
        tab.deleteLater()


def test_spotter_layout_geometry_is_bounded_and_deferred() -> None:
    """Sizing is bounded and repeated geometry requests are coalesced safely."""

    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    layout = source[source.index("    def _refresh_compose_layout_geometry"): source.index("    def _compose_embedded_needs_workbench")]

    assert MessageViewerTab._compose_spotter_sidebar_width(1400, in_workbench=False) == 480
    assert MessageViewerTab._compose_spotter_sidebar_width(1400, in_workbench=True) == 520
    assert MessageViewerTab._compose_spotter_sidebar_width(1200, in_workbench=False) == 480
    assert MessageViewerTab._compose_spotter_sidebar_width(980, in_workbench=True) == 440
    assert "_set_compose_splitter_sizes_if_needed" in layout
    assert "setup_scroll.setHorizontalScrollBarPolicy" in layout
    assert "QTimer" not in layout
    assert "Path(" not in layout
    assert "sqlite" not in layout.lower()


def test_compose_sidebar_does_not_starve_medium_commstat_surface(monkeypatch, tmp_path) -> None:
    """Medium layouts stack setup before the form instead of using a 260px rail."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        assert tab._compose_sidebar_enabled("commstat_rf", 1000, in_workbench=False) is False
        assert tab._compose_sidebar_enabled("commstat_rf", 1400, in_workbench=False) is True
        assert tab._compose_sidebar_enabled("js8", 1400, in_workbench=False) is False
    finally:
        tab.close()
        tab.deleteLater()


def test_selected_target_evidence_has_full_width_action_row(monkeypatch, tmp_path) -> None:
    """Target evidence never shares a cramped line with its action buttons."""

    tab = _tab(monkeypatch, tmp_path)
    try:
        layout = tab.compose_js8_selected_target_row_widget.layout()
        assert isinstance(layout, QGridLayout)
        assert layout.itemAtPosition(0, 0).widget() is tab.compose_js8_selected_target_label
        assert layout.itemAtPosition(1, 0).widget() is tab.compose_js8_selected_target_use_btn
        assert layout.itemAtPosition(1, 1).widget() is tab.compose_js8_selected_target_refresh_btn
        assert tab.compose_guidance_label.wordWrap() is False
        assert tab.compose_js8_selected_target_label.wordWrap() is True
    finally:
        tab.close()
        tab.deleteLater()


def test_compose_sidebar_floor_tracks_large_text_controls(monkeypatch, tmp_path) -> None:
    """Large-text settings must not reintroduce a clipped target rail."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        default_floor = tab._compose_sidebar_readable_minimum_width(
            "commstat_rf", in_workbench=False
        )
        large_font = tab.font()
        large_font.setPointSize(max(20, large_font.pointSize() + 8))
        tab.compose_js8_selected_target_use_btn.setFont(large_font)
        tab.compose_js8_selected_target_refresh_btn.setFont(large_font)
        app.processEvents()
        large_floor = tab._compose_sidebar_readable_minimum_width(
            "commstat_rf", in_workbench=False
        )
        assert large_floor >= default_floor
    finally:
        tab.close()
        tab.deleteLater()
