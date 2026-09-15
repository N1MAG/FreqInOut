"""MIR reader/state/counter acceptance tests.

These tests intentionally describe the reader-experience contract before the
implementation lands.  They exercise the Qt surface through the same bounded
fixtures used by the existing Inbox tests; no source ingest or projection
database is required for the reader interactions.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QPushButton, QTextEdit

from freqinout.core.message_projection_store import (
    MessageProjectionFocusCounts,
    MessageProjectionPage,
)
from freqinout.gui import message_viewer_tab as viewer_module
from freqinout.gui.message_viewer_tab import (
    JS8Message,
    MessageTableModel,
    MessageViewerTab,
    UnifiedMessage,
    _ProjectedMessageQueryWorker,
)
from freqinout.core.message_semantics import (
    commstat_status_receipt,
    message_row_kind_label,
    message_row_source_label,
)
from PySide6.QtWidgets import QHeaderView


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _tab(monkeypatch, tmp_path):
    """Build the real Messages widget without starting source workers."""

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


def _row(msg_id: int, *, status: str = "NEW", received: float = 100.0) -> UnifiedMessage:
    payload = JS8Message(
        msg_id=msg_id,
        from_call=f"SRC{msg_id}",
        to_call="GROUP",
        msg_type="MSG",
        utc_str="2026-09-11 00:00:00",
        utc_ts=received,
        raw_text=f"raw-{msg_id}",
        decoded_text=f"body-{msg_id}",
        state="UNREAD" if status != "READ" else "READ",
        source_key=f"source-{msg_id}",
        source_id=msg_id,
    )
    return UnifiedMessage(
        msg_type="JS8 MSG",
        status=status,
        from_call=payload.from_call,
        to_call=payload.to_call,
        rcv_ts=received,
        rcv_display="2026-09-11 00:00:00",
        title=payload.decoded_text,
        origin="js8",
        payload=payload,
    )


def _set_rows(tab: MessageViewerTab, rows: list[UnifiedMessage]) -> None:
    tab._message_rows = list(rows)
    tab._messages_model = MessageTableModel(list(rows))
    tab.messages_table.setModel(tab._messages_model)


def _button(tab: MessageViewerTab, *labels: str) -> QPushButton:
    wanted = {label.casefold() for label in labels}
    for button in tab.findChildren(QPushButton):
        if button.text().strip().casefold() in wanted:
            return button
    pytest.fail(f"Reader button not found: {sorted(wanted)}")


def _settle_reader_paint(app: QApplication, tab: MessageViewerTab) -> None:
    QTest.qWait(120)
    app.processEvents()
    assert getattr(tab, "_reader_transitioning", False) is False


def test_default_inbox_separates_source_kind_and_status_receipt_semantics() -> None:
    payload = SimpleNamespace(
        source_family="js8",
        display_type="CommStat",
        message_type="CommStat/STATUS_RECEIPT",
        body_preview="@MAGNET RRSR N6KYL,L42",
    )
    row = UnifiedMessage(
        "CommStat/STATUS_RECEIPT", "INFO", "W4WYD", "@MAGNET", 1.0, "", "receipt", "js8", payload
    )
    model = MessageTableModel([row])

    receipt = commstat_status_receipt(payload.body_preview)
    assert receipt is not None
    assert model.headerData(1, Qt.Horizontal, Qt.DisplayRole) == "Source"
    assert model.headerData(6, Qt.Horizontal, Qt.DisplayRole) == "Kind / Message"
    assert message_row_source_label(row) == "CommStat"
    assert message_row_kind_label(row) == "Status receipt"


def test_commstat_kind_column_is_content_fit_not_full_screen_stretch(monkeypatch, tmp_path) -> None:
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        row = UnifiedMessage(
            "CommStat", "INFO", "K1ABC", "@MR08", 1.0, "now", "Power stable", "commstat",
            SimpleNamespace(artifact_kind="STATREP", state_code="CO", grid="DM79"),
        )
        _set_rows(tab, [row])
        tab._messages_model.set_display_profile("intel_report", "Age")
        tab.resize(1800, 800)
        tab.show()
        app.processEvents()
        tab._message_table_fit_signature = None
        tab._apply_message_table_profile_widths()
        assert tab.messages_table.columnWidth(1) <= 300
        assert tab.messages_table.horizontalHeader().sectionResizeMode(1) == QHeaderView.Interactive
    finally:
        tab.close()
        tab.deleteLater()


def test_inbox_installs_initial_table_geometry_before_show_and_publishes_atomically(
    monkeypatch, tmp_path
) -> None:
    _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        # Construction owns the final empty-page geometry; showing the native
        # window must not first expose the obsolete fixed-width profile.
        assert tab._message_table_fit_signature is not None
        assert tab.messages_table.horizontalHeader().sectionResizeMode(6) == QHeaderView.Stretch

        # Atomic fitting preserves a caller's update fence rather than
        # spuriously repainting a partially-updated header.
        tab.messages_table.setUpdatesEnabled(False)
        tab._message_table_fit_signature = None
        tab._apply_message_table_profile_widths()
        assert tab.messages_table.updatesEnabled() is False
    finally:
        tab.messages_table.setUpdatesEnabled(True)
        tab.close()
        tab.deleteLater()


def test_reader_context_actions_stage_navigation_from_cached_row(monkeypatch, tmp_path) -> None:
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        row = _row(7)
        row.payload.state_code = "CO"
        row.payload.grid = "DM79"
        row.summary = viewer_module.message_summary_from_row(row)
        opened = []
        tab.open_spotter_map = lambda **values: opened.append(("map", values))
        tab.open_hf_operator = lambda callsign: opened.append(("operator", callsign))
        tab.open_messages_section = lambda mode, **values: opened.append((mode, values))
        tab.open_fio_spotter_watch = lambda values: opened.append(("watch", values))
        tab._reader_snapshot = [row]
        tab._reader_index = 0
        tab._sync_reader_context_actions(row)

        assert tab.reader_map_btn.isEnabled()
        assert tab.reader_operator_btn.isEnabled()
        assert tab.reader_reply_btn.isEnabled()
        assert tab.reader_watch_btn.isEnabled()
        tab.reader_map_btn.click()
        tab.reader_operator_btn.click()
        tab.reader_reply_btn.click()
        tab.reader_watch_btn.click()
        assert [item[0] for item in opened] == ["map", "operator", "compose", "watch"]
        assert opened[2][1]["compose_intent"]["recipient_callsign"] == "SRC7"
        assert opened[3][1]["source_family"] == "js8"
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_clear_does_not_change_tab_active_lifecycle(monkeypatch, tmp_path) -> None:
    """Clearing content must not pause tab timers or deferred refreshes."""

    _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        tab.set_tab_active(True)
        assert tab._has_active_view is True
        tab._clear_message_detail_view()
        assert tab._has_active_view is True
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_uses_plain_text_edit_without_custom_paint_lifecycle(monkeypatch, tmp_path) -> None:
    """Navigation must not attach application state changes to paint events."""

    _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        assert type(tab.viewer) is QTextEdit
        assert not hasattr(tab.viewer, "documentPainted")
    finally:
        tab.close()
        tab.deleteLater()


@pytest.mark.parametrize(
    "size,target_height",
    [((1280, 720), 320), ((1000, 700), 280), ((900, 560), 220)],
)
def test_reader_is_content_first_and_keeps_positive_compact_viewport(
    monkeypatch, tmp_path, size, target_height
) -> None:
    """Opening a message replaces the list with a usable full-height reader."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        row = _row(1)
        _set_rows(tab, [row])
        tab._load_js8_content = lambda payload: tab.viewer.setPlainText(payload.decoded_text)
        tab._mark_js8_read = lambda *_args, **_kwargs: None
        tab.resize(*size)
        tab.show()
        app.processEvents()

        # Inbox mode has no empty reader competing with the list.
        assert getattr(tab, "_reader_open", None) is False
        assert tab.messages_table.isVisible()

        tab._on_view_message(row)
        app.processEvents()

        assert getattr(tab, "_reader_open", None) is True
        assert not tab.messages_table.isVisible()
        assert tab.viewer.isVisible()
        assert tab.viewer.viewport().height() >= target_height
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_previous_next_are_bounded_snapshot_navigation_without_page_query(monkeypatch, tmp_path) -> None:
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        rows = [_row(1, received=30), _row(2, received=20), _row(3, received=10)]
        _set_rows(tab, rows)
        tab._load_js8_content = lambda payload: tab.viewer.setPlainText(payload.decoded_text)
        tab._mark_js8_read = lambda *_args, **_kwargs: None
        page_queries: list[object] = []
        tab._request_projected_message_query = lambda **kwargs: page_queries.append(kwargs)
        tab.resize(1280, 720)
        tab.show()
        app.processEvents()
        tab._on_view_message(rows[1])
        app.processEvents()

        previous = _button(tab, "Previous")
        next_button = _button(tab, "Next")
        assert previous.isEnabled()
        assert next_button.isEnabled()

        next_button.click()
        _settle_reader_paint(app, tab)
        assert tab.viewer.toPlainText() == "body-3"
        assert not next_button.isEnabled()

        previous.click()
        _settle_reader_paint(app, tab)
        assert tab.viewer.toPlainText() == "body-2"
        assert page_queries == []
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_navigation_commits_position_only_after_matching_document(monkeypatch, tmp_path) -> None:
    """The toolbar must never lead the document by one message."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        rows = [_row(1, received=30), _row(2, received=20), _row(3, received=10)]
        _set_rows(tab, rows)
        observed_during_load: list[tuple[int, str]] = []

        def load(payload: JS8Message) -> None:
            observed_during_load.append((payload.msg_id, tab.reader_position_label.text()))
            tab.viewer.setPlainText(payload.decoded_text)

        tab._load_js8_content = load
        tab._mark_js8_read = lambda *_args, **_kwargs: None
        tab.resize(1280, 720)
        tab.show()
        app.processEvents()
        tab._on_view_message(rows[0])
        app.processEvents()

        assert tab.reader_position_label.text() == "1 of 3"
        tab.reader_next_btn.click()

        # The target document is synchronously repainted before its position is
        # committed. Navigation then remains fenced for a short debounce.
        assert observed_during_load[-1] == (2, "1 of 3")
        assert tab.viewer.toPlainText() == "body-2"
        assert tab.reader_position_label.text() == "2 of 3"
        assert not tab.reader_next_btn.isEnabled()
        tab.reader_next_btn.click()  # rapid second click is ignored while paint is pending
        _settle_reader_paint(app, tab)
        assert tab.reader_position_label.text() == "2 of 3"
        assert tab.viewer.toPlainText() == "body-2"
        assert tab.reader_next_btn.isEnabled()
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_single_next_click_commits_content_and_counter_together(monkeypatch, tmp_path) -> None:
    """One physical Next click must update the document and its position label."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        rows = [_row(1, received=30), _row(2, received=20), _row(3, received=10)]
        _set_rows(tab, rows)
        # Keep the real reader renderer and QTextEdit event path under test;
        # only suppress persistence, which is outside this UI contract.
        tab._mark_js8_read = lambda *_args, **_kwargs: None
        tab.resize(1280, 720)
        tab.show()
        app.processEvents()

        tab._on_view_message(rows[0])
        app.processEvents()
        assert "body-1" in tab.viewer.toPlainText()
        assert tab.reader_position_label.text() == "1 of 3"

        QTest.mouseClick(tab.reader_next_btn, Qt.LeftButton)
        app.processEvents()

        assert "body-2" in tab.viewer.toPlainText()
        assert tab.reader_position_label.text() == "2 of 3"
        assert tab._reader_current_row() is rows[1]
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_navigation_fences_reentry_until_document_is_committed(monkeypatch, tmp_path) -> None:
    """A nested or queued activation cannot make the position outrun content."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        rows = [_row(1, received=30), _row(2, received=20), _row(3, received=10)]
        _set_rows(tab, rows)

        def load(payload: JS8Message) -> None:
            if payload.msg_id == 2:
                tab._navigate_message_reader(1)
            tab.viewer.setPlainText(payload.decoded_text)

        tab._load_js8_content = load
        tab._mark_js8_read = lambda *_args, **_kwargs: None
        tab.resize(1280, 720)
        tab.show()
        app.processEvents()
        tab._on_view_message(rows[0])
        app.processEvents()

        tab.reader_next_btn.click()
        assert tab.viewer.toPlainText() == "body-2"
        assert tab.reader_position_label.text() == "2 of 3"
        _settle_reader_paint(app, tab)
        assert tab.reader_position_label.text() == "2 of 3"
        assert tab._reader_current_row() is rows[1]
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_unknown_payload_replaces_prior_document_with_explicit_fallback(monkeypatch, tmp_path) -> None:
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        first = _row(1, received=30)
        unknown = UnifiedMessage(
            msg_type="Future Format",
            status="NEW",
            from_call="",
            to_call="",
            rcv_ts=20,
            rcv_display="",
            title="",
            origin="future",
            payload=object(),
        )
        _set_rows(tab, [first, unknown])
        tab._load_js8_content = lambda payload: tab.viewer.setPlainText(payload.decoded_text)
        tab._mark_js8_read = lambda *_args, **_kwargs: None
        tab.resize(1280, 720)
        tab.show()
        app.processEvents()
        tab._on_view_message(first)
        app.processEvents()

        tab.reader_next_btn.click()

        assert tab.reader_position_label.text() == "2 of 2"
        assert "cannot display this message format" in tab.viewer.toPlainText()
        assert "body-1" not in tab.viewer.toPlainText()
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_back_restores_inbox_and_list_scroll(monkeypatch, tmp_path) -> None:
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        rows = [_row(i, received=100 - i) for i in range(1, 40)]
        _set_rows(tab, rows)
        tab._load_js8_content = lambda payload: tab.viewer.setPlainText(payload.decoded_text)
        tab._mark_js8_read = lambda *_args, **_kwargs: None
        tab.resize(1280, 720)
        tab.show()
        app.processEvents()
        tab.messages_table.verticalScrollBar().setValue(7)
        saved_scroll = tab.messages_table.verticalScrollBar().value()

        tab._on_view_message(rows[10])
        app.processEvents()
        _button(tab, "Back to Inbox", "Back").click()
        app.processEvents()

        assert getattr(tab, "_reader_open", None) is False
        assert tab.messages_table.isVisible()
        # The implementation may center the just-read row after restoring the
        # saved position; the saved position itself must remain available for
        # the next Back transition and the current row must be in the model.
        assert tab._saved_list_scroll == saved_scroll
        assert any(tab._reader_row_key(candidate) == tab._reader_message_key for candidate in rows)
    finally:
        tab.close()
        tab.deleteLater()


def test_user_focus_change_closes_reader_and_clears_stale_content(monkeypatch, tmp_path) -> None:
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        row = _row(1)
        _set_rows(tab, [row])
        tab._projection_primary_enabled = False
        tab._reload_projected_messages_for_scope_change = lambda: False
        tab._apply_message_filters_light = lambda: None
        tab._load_js8_content = lambda payload: tab.viewer.setPlainText(payload.decoded_text)
        tab._mark_js8_read = lambda *_args, **_kwargs: None
        tab.resize(1280, 720)
        tab.show()
        app.processEvents()
        tab._on_view_message(row)
        app.processEvents()
        assert tab.viewer.toPlainText() == "body-1"

        tab._inbox_focus_buttons["forms"].click()
        app.processEvents()

        assert getattr(tab, "_reader_open", None) is False
        assert tab.viewer.toPlainText() == ""
    finally:
        tab.close()
        tab.deleteLater()


def test_focus_counts_apply_worker_aggregate_payload_with_request_fence(monkeypatch) -> None:
    """A scalar worker aggregate updates every focus chip without a click."""

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._has_active_view = True
    tab._app_active = True
    tab._projected_query_request_id = 8
    tab._active_projection_generation = 2
    tab._projected_table_loading = False
    tab._message_rows = []
    tab._projected_scope_load_key = None
    tab._projected_total_count = 0
    tab._last_projection_render_ts = 0.0
    tab.settings = None
    tab._projected_rows_from_mappings = lambda *_args, **_kwargs: []
    tab._refresh_message_filters = lambda *_args, **_kwargs: None
    tab._apply_message_filters = lambda **_kwargs: None

    counts = {
        "all": 9,
        "new": 9,
        "forms": 2,
        "spotter": 1,
        "commstat": 3,
        "js8call": 6,
        "mesh": 0,
        "varac": 1,
        "bbs": 0,
    }
    MessageViewerTab._on_projected_message_query_finished(
        tab,
        {
            "request_id": 8,
            "generation": 3,
            "scope_key": ("all",),
            "rows": [],
            "refs": {},
            "total_count": 0,
            "focus_counts": counts,
            "focus_unread_counts": counts,
        },
    )
    assert tab._inbox_focus_unread_counts == counts

    # An older aggregate must not replace the committed count state.
    MessageViewerTab._on_projected_message_query_finished(
        tab,
        {
            "request_id": 7,
            "generation": 99,
            "rows": [],
            "refs": {},
            "focus_counts": {key: 0 for key in counts},
        },
    )
    assert tab._inbox_focus_unread_counts == counts


def test_projection_worker_counts_share_age_group_lane_not_active_focus(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        viewer_module,
        "query_projected_message_page",
        lambda *_args, **_kwargs: MessageProjectionPage(rows=(), generation=12, total_count=0),
    )
    monkeypatch.setattr(
        viewer_module,
        "load_projected_external_refs_for_messages",
        lambda *_args, **_kwargs: {},
    )

    def focus_counts(_db_path, **kwargs):
        captured.update(kwargs)
        return MessageProjectionFocusCounts(counts={"all": 4, "new": 4}, generation=12)

    monkeypatch.setattr(viewer_module, "query_projected_inbox_focus_counts", focus_counts)
    worker = _ProjectedMessageQueryWorker(
        db_path="projection.db",
        request_id=7,
        scope_key=("forms",),
        query={
            "source_families": ("flmsg", "flamp"),
            "group_names": ("MR08",),
            "received_after_ts": 100.0,
            "search_text": "exercise",
        },
    )
    results: list[dict[str, object]] = []
    worker.finished.connect(results.append)

    worker.run()

    assert captured == {"group_names": ("MR08",), "received_after_ts": 100.0}
    assert results[0]["focus_counts"] == {"all": 4, "new": 4}
    assert results[0]["focus_counts_generation"] == 12

def test_open_unread_message_updates_all_applicable_focus_counts_immediately(monkeypatch, tmp_path) -> None:
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        row = _row(1)
        _set_rows(tab, [row])
        tab._expanded_selected_message_groups = lambda: None
        tab._configured_message_group_names = lambda: set()
        tab.received_filter.setCurrentIndex(0)
        tab._refresh_inbox_focus_unread_counts([row], now_ts=200.0)
        assert tab._inbox_focus_unread_counts["all"] == 1
        assert tab._inbox_focus_unread_counts["js8call"] == 1

        tab._requires_full_refresh_after_read = lambda: False
        tab._update_mark_all_read_style = lambda: None
        tab._update_bulk_delete_buttons = lambda: None
        tab._refresh_table_after_read(lambda candidate: candidate is row, row_ref=row)

        assert tab._inbox_focus_unread_counts["all"] == 0
        assert tab._inbox_focus_unread_counts["js8call"] == 0
    finally:
        tab.close()
        tab.deleteLater()
