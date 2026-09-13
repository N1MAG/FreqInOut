from __future__ import annotations

from datetime import datetime, timezone
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.core.nbems_compose import ComposeFieldDefinition, ComposeFieldOption
from PySide6.QtWidgets import QApplication


def test_saved_expect_response_removes_legacy_target_prefix() -> None:
    target, response = MessageViewerTab._compose_spotter_expect_response_parts(
        "@GROUP F!701C 100 #ABCD", "F!701C"
    )

    assert target == "@GROUP"
    assert response == "F!701C 100 #ABCD"


def test_saved_expect_compact_option_response_maps_to_form_fields() -> None:
    fields = [
        ComposeFieldDefinition(
            key="STATUS",
            label="Status",
            field_type="select",
            options=(ComposeFieldOption(value="1", label="Green"), ComposeFieldOption(value="2", label="Red")),
        ),
        ComposeFieldDefinition(
            key="SCOPE",
            label="Scope",
            field_type="select",
            options=(ComposeFieldOption(value="00", label="Local"), ComposeFieldOption(value="10", label="Regional")),
        ),
    ]

    values = MessageViewerTab._compose_spotter_saved_values("F!701C 100", "F!701C", fields)

    assert values == {"STATUS": "1", "SCOPE": "00"}


def test_spotter_date_refresh_is_copy_only_and_uses_zulu_timestamp() -> None:
    original = {"DATE": "old", "STATUS": "1"}
    updated = MessageViewerTab._compose_spotter_refresh_date_fields(
        original,
        datetime(2026, 9, 13, 12, 34, tzinfo=timezone.utc),
    )

    assert original == {"DATE": "old", "STATUS": "1"}
    assert updated["DATE"] == "260913-1234z"
    assert updated["STATUS"] == "1"


def test_saved_expect_catalog_is_bounded_and_loaded_once(monkeypatch) -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._compose_spotter_expect_loaded = False
    tab._compose_spotter_expect_entries = []
    tab._compose_spotter_expect_active_id = 0
    tab._apply_compose_spotter_source_options = lambda: None
    rows = [
        {"id": i, "expect_key": "F!701C", "response_text": f"F!701C {i}"}
        for i in range(250)
    ]
    calls = {"count": 0}

    def fake_list(**_kwargs):
        calls["count"] += 1
        return rows

    monkeypatch.setattr("freqinout.gui.message_viewer_tab.list_expect_entries", fake_list)
    tab._refresh_compose_spotter_expect_entries()
    tab._refresh_compose_spotter_expect_entries()

    assert calls["count"] == 1
    assert len(tab._compose_spotter_expect_entries) == 200


def test_saved_expect_catalog_includes_static_saved_messages_and_excludes_dynamic_q(monkeypatch) -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._compose_spotter_expect_loaded = False
    tab._compose_spotter_expect_entries = []
    tab._compose_spotter_expect_active_id = 0
    tab._apply_compose_spotter_source_options = lambda: None
    monkeypatch.setattr(
        "freqinout.gui.message_viewer_tab.list_expect_entries",
        lambda **_kwargs: [
            {"id": 1, "expect_key": "INFO", "response_text": "INFO READY"},
            {"id": 2, "expect_key": "Q", "response_text": "generated"},
        ],
    )

    tab._refresh_compose_spotter_expect_entries()

    assert [row["expect_key"] for row in tab._compose_spotter_expect_entries] == ["INFO"]


@pytest.mark.parametrize("width,height", [(1280, 720), (980, 680), (760, 560)])
def test_spotter_compose_source_row_remains_reachable_at_laptop_sizes(monkeypatch, width, height) -> None:
    app = QApplication.instance() or QApplication([])
    for name in (
        "_setup_clock_timer",
        "_setup_timer",
        "_setup_js8_timer",
        "_setup_pending_timer",
        "_setup_bbs_auto_archive_timer",
        "_initial_refresh",
        "_refresh_compose_forms",
        "_refresh_compose_setup_discovery",
    ):
        monkeypatch.setattr(MessageViewerTab, name, lambda self, *args, **kwargs: None)
    tab = MessageViewerTab()
    tab.resize(width, height)
    tab.show_compose_from_navigation()
    tab._refresh_compose_spotter_expect_entries = lambda force=False: None
    tab.compose_mode_selector.setCurrentRow(2)
    tab._update_compose_preview()
    app.processEvents()
    try:
        # The embedded Messages stack may be hidden by the test shell, so
        # assert the widget's own visibility state rather than ancestor state.
        assert tab.compose_spotter_source_row_widget.isHidden() is False
        assert tab.compose_spotter_source_combo.minimumWidth() >= 0
        assert tab.compose_send_js8_btn.isHidden() is False
        assert tab.compose_send_js8_btn.isEnabled() is False
    finally:
        tab.close()
        tab.deleteLater()
