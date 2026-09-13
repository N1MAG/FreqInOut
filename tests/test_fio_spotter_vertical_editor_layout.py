from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QBoxLayout, QScrollArea, QTextEdit

from freqinout.gui import fio_spotter_tab as spotter_ui
from freqinout.gui.fio_spotter_tab import FioSpotterTab


class _Settings:
    config_dir = "/tmp"

    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def get(self, key: str, default=None):
        return self.values.get(key, default)

    def set(self, _key: str, _value: object) -> None:
        pass

    def save(self) -> None:
        pass


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _page_scroll(tab: FioSpotterTab) -> QScrollArea:
    scroll = tab.tabs.currentWidget().findChild(QScrollArea)
    assert scroll is not None
    return scroll


def test_expect_and_access_policy_editors_stay_above_tables_without_resize_io(monkeypatch) -> None:
    app = _app()
    reads = {"entries": 0, "policies": 0, "catalog": 0}

    def entries(**_kwargs):
        reads["entries"] += 1
        return []

    def policies(**_kwargs):
        reads["policies"] += 1
        return []

    def catalog(**_kwargs):
        reads["catalog"] += 1
        return []

    monkeypatch.setattr(spotter_ui, "list_expect_entries", entries)
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", policies)
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", catalog)
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])

    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.resize(1200, 1200)
        tab.tabs.setCurrentIndex(2)
        tab.show()
        app.processEvents()

        expect_editor = tab.expect_editor_split.widget(0)
        expect_table_panel = tab.expect_editor_split.widget(1)
        assert tab.expect_editor_split.orientation() == Qt.Vertical
        assert expect_editor.isAncestorOf(tab.expect_key)
        assert expect_table_panel.isAncestorOf(tab.expect_entries_table)
        assert expect_editor.geometry().bottom() < expect_table_panel.geometry().top()
        assert expect_editor.height() >= expect_editor.minimumHeight()
        assert expect_table_panel.height() > expect_editor.height()
        assert 3 not in tab._built

        reads_before_resize_and_typing = dict(reads)
        tab.resize(900, 560)
        app.processEvents()
        tab.expect_filter.setText("status")
        app.processEvents()
        expect_scroll = _page_scroll(tab)
        assert tab.expect_editor_split.orientation() == Qt.Vertical
        assert expect_scroll.horizontalScrollBar().maximum() == 0
        assert expect_scroll.verticalScrollBar().maximum() > 0
        assert reads == reads_before_resize_and_typing

        tab.resize(1200, 1200)
        tab.tabs.setCurrentIndex(3)
        app.processEvents()
        policy_editor = tab.policy_editor_panel
        policy_table_panel = tab.policy_table.parentWidget()
        assert policy_editor is not None
        assert policy_table_panel is not None
        assert policy_editor.geometry().bottom() < policy_table_panel.geometry().top()
        assert policy_editor.height() >= policy_editor.minimumHeight()
        assert policy_table_panel.height() > policy_editor.height()

        reads_before_compact_policy = dict(reads)
        tab.resize(900, 560)
        app.processEvents()
        policy_scroll = _page_scroll(tab)
        assert policy_scroll.horizontalScrollBar().maximum() == 0
        assert policy_scroll.verticalScrollBar().maximum() > 0
        assert tab.policy_editor_columns.direction() == QBoxLayout.TopToBottom
        assert reads == reads_before_compact_policy
    finally:
        tab.close()
        tab.deleteLater()


def test_expect_wide_editor_packs_actions_and_metadata_above_saved_responses(monkeypatch) -> None:
    """A high-DPI desktop must leave a useful saved-response table in view."""
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])

    tab = FioSpotterTab(settings=_Settings())
    try:
        # 1595×903 is the logical size of the reported 2392×1354 Retina view.
        tab.resize(1595, 903)
        tab.tabs.setCurrentIndex(2)
        tab.show()
        app.processEvents()

        # Response identity now has a dedicated scan strip instead of sharing
        # a two-column form with the reply.  On a wide desktop all four
        # metadata values fit in one row and Reply remains a readable text
        # surface below it.
        assert tab._expect_meta_layout_mode == "wide"
        assert isinstance(tab.expect_reply, QTextEdit)
        assert tab.expect_reply.height() >= tab.expect_reply.fontMetrics().lineSpacing() * 3
        assert max(group.geometry().y() for group in tab.expect_editor_meta_groups) - min(
            group.geometry().y() for group in tab.expect_editor_meta_groups
        ) <= 1
        assert tab.expect_reply.geometry().top() > max(
            group.geometry().bottom() for group in tab.expect_editor_meta_groups
        )
        assert all(
            tab.expect_editor_actions.getItemPosition(
                tab.expect_editor_actions.indexOf(button)
            )[0] == 0
            for button in tab.expect_editor_action_buttons
        )
        # The bounded three-line Reply editor may take a little more vertical
        # space than the former single-line control; the saved-response table
        # remains a useful primary surface instead of collapsing to its row
        # minimum.
        assert tab.expect_editor_split.widget(1).height() >= 220
        assert tab.expect_entries_table.height() >= 160
    finally:
        tab.close()
        tab.deleteLater()


def test_expect_metadata_reflows_without_page_horizontal_overflow(monkeypatch) -> None:
    """Readability, rather than a widget's tiny legal minimum, drives wrapping."""
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])

    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.resize(900, 800)
        tab.tabs.setCurrentIndex(2)
        tab.show()
        app.processEvents()
        assert tab._expect_meta_layout_mode == "medium"
        assert tab.expect_editor_meta_groups[0].geometry().y() == tab.expect_editor_meta_groups[1].geometry().y()
        assert tab.expect_editor_meta_groups[2].geometry().y() > tab.expect_editor_meta_groups[0].geometry().y()
        assert _page_scroll(tab).horizontalScrollBar().maximum() == 0

        tab.resize(500, 800)
        app.processEvents()
        assert tab._expect_meta_layout_mode == "compact"
        assert _page_scroll(tab).horizontalScrollBar().maximum() == 0
    finally:
        tab.close()
        tab.deleteLater()


def test_policy_wide_summaries_have_dedicated_nonoverlapping_space(monkeypatch) -> None:
    """Lookup and usage evidence must not compete inside narrow form rows."""
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: [])
    monkeypatch.setattr(
        spotter_ui,
        "list_expect_operator_access_catalog",
        lambda **_kwargs: [
            {
                "callsign": f"CALL{index}",
                "current_callsign": f"CALL{index}",
                "trusted": index < 12,
                "groups": ["LOCAL"],
            }
            for index in range(30)
        ],
    )

    tab = FioSpotterTab(settings=_Settings())
    try:
        # Logical equivalent of the reported 2326x722 high-DPI capture.
        tab.resize(1550, 480)
        tab.tabs.setCurrentIndex(3)
        tab.show()
        app.processEvents()

        lookup = tab.policy_access_catalog_state
        usage = tab.policy_usage_state
        assert lookup.parentWidget() is usage.parentWidget()
        assert not lookup.geometry().intersects(usage.geometry())
        assert lookup.height() >= lookup.heightForWidth(lookup.width())
        assert usage.height() >= usage.heightForWidth(usage.width())
        assert tab.policy_editor_panel.geometry().bottom() < tab.policy_table.parentWidget().geometry().top()
        assert _page_scroll(tab).horizontalScrollBar().maximum() == 0
    finally:
        tab.close()
        tab.deleteLater()


def test_policy_selection_uses_cached_usage_and_known_radio_labels(monkeypatch) -> None:
    """Changing the policy selection must not query usage or expose radio IDs."""
    app = _app()
    reads = {"policies": 0, "entries": 0}

    def policies(**_kwargs):
        reads["policies"] += 1
        return [{
            "id": 7, "name": "Regional access", "enabled": True,
            "usage_count": 1, "source_scope": "radio", "source_radio_ids": ["7"],
        }]

    def entries(**_kwargs):
        reads["entries"] += 1
        return [{"id": 20, "expect_key": "INFO", "allow_policy_id": 7}]

    class _RadioStore:
        def list_device_profiles(self):
            return [{"id": 7, "name": "North radio", "enabled": 1, "use_js8call": 1}]

    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", policies)
    monkeypatch.setattr(spotter_ui, "list_expect_entries", entries)
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda **_kwargs: [])

    tab = FioSpotterTab(settings=_Settings(), radio_store=_RadioStore())
    try:
        tab.resize(1200, 900)
        tab.tabs.setCurrentIndex(3)
        tab.show()
        app.processEvents()
        loaded = dict(reads)

        tab.policy_table.selectRow(0)
        app.processEvents()

        assert reads == loaded
        assert tab.policy_radios.text() == "NORTH RADIO"
        assert "Used by 1 saved response(s): INFO." == tab.policy_usage_state.text()
        assert tab.policy_scope.currentData() == "radio"
        assert tab.policy_radios.isEnabled()
    finally:
        tab.close()
        tab.deleteLater()


def test_expect_and_policy_actions_reapply_shared_theme_roles(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])

    settings = _Settings()
    settings.values = {"ui_theme": "light"}
    tab = FioSpotterTab(settings=settings)
    try:
        tab.tabs.setCurrentIndex(2)
        app.processEvents()
        light_primary = tab.expect_save.styleSheet()
        light_danger = tab.expect_delete.styleSheet()
        assert "#2E6F9E" in light_primary
        assert "#C62828" in light_danger

        settings.values["ui_theme"] = "dark"
        tab.tabs.setCurrentIndex(3)
        tab.apply_theme()
        assert "#4C9BD3" in tab.expect_save.styleSheet()
        assert "#E05252" in tab.policy_delete.styleSheet()
    finally:
        tab.close()
        tab.deleteLater()
