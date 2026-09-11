from __future__ import annotations

import os
import sqlite3
import time
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
import pytest
from PySide6.QtWidgets import QApplication, QMessageBox

from freqinout.gui.message_viewer_tab import MessageTableModel, MessageViewerTab, UnifiedMessage
from freqinout.gui.theme import apply_app_theme, get_theme


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _tab(monkeypatch, tmp_path):
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
    tab._backlog_db_path = lambda: tmp_path / "freqinout_nets.db"
    tab._ensure_backlog_table()
    return tab


def _pending_db(path, count=26):
    conn = sqlite3.connect(path)
    conn.executemany(
        """INSERT INTO autoquery_backlog
           (callsign, msg_id, kind, status, attempts, last_attempt_ts, created_ts,
            source_key, source_radio_id, js8_instance_id, source_path)
           VALUES (?, ?, 'MSG', 'PENDING', 0, 1.0, ?, ?, ?, ?, ?)""",
        [
            (f"K1A{i:02d}", str(i), float(i), f"js8:fio-{i % 2}", str(i % 2 + 1), f"fio-{i % 2}", "")
            for i in range(count)
        ],
    )
    conn.commit()
    conn.close()


def _mixed_rows():
    payload = SimpleNamespace()
    return [
        UnifiedMessage("JS8 MSG", "NEW", "K1AAA", "K1BBB", 10, "", "JS8 traffic", "js8", payload),
        UnifiedMessage("VarAC", "READ", "K1CCC", "K1DDD", 9, "", "VarAC traffic", "varac", payload),
        UnifiedMessage("SitRep", "INFO", "K1EEE", "", 8, "", "Situation report", "sitrep", payload),
        UnifiedMessage("CommStat", "WARNING", "K1FFF", "", 7, "", "Status", "commstat", payload),
        UnifiedMessage("BBS", "READ", "K1GGG", "", 6, "", "Bulletin", "bbs", payload),
    ]


def test_pending_js8_backlog_coexists_with_normal_mixed_source_inbox(monkeypatch, tmp_path):
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        _pending_db(tmp_path / "freqinout_nets.db")
        tab._messages_model = MessageTableModel(_mixed_rows())
        tab.messages_table.setModel(tab._messages_model)
        tab._update_pending_table()
        tab.resize(1000, 700)
        tab.show()
        app.processEvents()
        assert tab.pending_table.rowCount() == 0
        assert tab.pending_count.text() == "26 pending"
        assert tab._messages_model.rowCount() == 5
        assert {row.origin for row in tab._messages_model.rows()} == {"js8", "varac", "sitrep", "commstat", "bbs"}
        assert tab.messages_table.isVisible()
        assert tab.pending_retrieval_widget.isVisible()
        tab._open_pending_review()
        app.processEvents()
        assert tab.pending_review_dialog.isVisible()
        assert tab.pending_table.rowCount() == 26
    finally:
        tab.pending_review_dialog.close()
        tab.close()
        tab.deleteLater()


def test_pending_review_is_bounded_and_scrollable_at_compact_sizes(monkeypatch, tmp_path):
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        _pending_db(tmp_path / "freqinout_nets.db", count=40)
        tab._update_pending_table()
        tab.resize(900, 560)
        tab.show()
        tab._open_pending_review()
        app.processEvents()
        assert tab.pending_table.rowCount() == 40
        assert tab.pending_table.verticalScrollBarPolicy() in {Qt.ScrollBarAsNeeded, Qt.ScrollBarAlwaysOn}
        assert tab.pending_table.maximumHeight() <= 500
    finally:
        tab.close()
        tab.deleteLater()


def test_normal_message_table_has_positive_geometry_at_production_and_compact_sizes(monkeypatch, tmp_path):
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        tab._messages_model = MessageTableModel(_mixed_rows())
        tab.messages_table.setModel(tab._messages_model)
        for width, height, minimum_viewport_height in ((1280, 720, 240), (1000, 700, 160), (900, 560, 160)):
            tab.resize(width, height)
            tab.show()
            app.processEvents()
            assert tab.messages_table.isVisible()
            assert tab.messages_table.viewport().width() > 0
            assert tab.messages_table.viewport().height() >= minimum_viewport_height
            assert tab.inbox_body_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
    finally:
        tab.close()
        tab.deleteLater()


@pytest.mark.parametrize("size", [(1280, 720), (1000, 700), (900, 560)])
@pytest.mark.parametrize("scale", [1.0, 1.25])
@pytest.mark.parametrize("theme_name", ["light", "dark"])
def test_inbox_matrix_keeps_closed_retrieval_workbench_unmaterialized_and_primary_table_usable(
    monkeypatch, tmp_path, size, scale, theme_name
):
    app = _app()
    apply_app_theme(app, get_theme(theme_name), ui_text_scale=scale)
    tab = _tab(monkeypatch, tmp_path)
    try:
        _pending_db(tmp_path / "freqinout_nets.db", count=26)
        tab._messages_model = MessageTableModel(_mixed_rows())
        tab.messages_table.setModel(tab._messages_model)
        tab._update_pending_table()
        tab.resize(*size)
        tab.show()
        app.processEvents()
        assert tab.pending_retrieval_widget.isVisible()
        assert tab._pending_total_count == 26
        assert tab.pending_table.rowCount() == 0
        assert tab.messages_table.isVisible()
        assert tab.messages_table.viewport().width() > 0
        assert tab.messages_table.viewport().height() > 0
    finally:
        tab.close()
        tab.deleteLater()
        apply_app_theme(app, get_theme("light"), ui_text_scale=1.0)


def test_focus_all_and_js8_focus_have_distinct_source_semantics():
    from freqinout.core.message_inbox_filters import row_matches_inbox_focus

    rows = _mixed_rows()
    assert all(row_matches_inbox_focus(row, "all") for row in rows)
    assert row_matches_inbox_focus(rows[0], "js8call")
    assert not row_matches_inbox_focus(rows[1], "js8call")


def test_pending_actions_are_source_scoped_without_external_io(monkeypatch, tmp_path):
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._backlog_db_path = lambda: tmp_path / "freqinout_nets.db"
    MessageViewerTab._ensure_backlog_table(tab)
    conn = sqlite3.connect(tmp_path / "freqinout_nets.db")
    conn.executemany(
        """INSERT INTO autoquery_backlog
           (callsign, msg_id, kind, status, attempts, last_attempt_ts, created_ts, source_key)
           VALUES ('K1ABC', '42', 'MSG', 'PENDING', 0, 1, 1, ?)""",
        [("js8:fio-a",), ("js8:fio-b",)],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(MessageViewerTab, "_update_pending_table", lambda _self: None)
    MessageViewerTab._pending_set_status(tab, "K1ABC", "42", "WAITING", "js8:fio-b")
    MessageViewerTab._pending_delete(tab, "K1ABC", "42", "js8:fio-a")
    conn = sqlite3.connect(tmp_path / "freqinout_nets.db")
    try:
        assert conn.execute("SELECT source_key, status FROM autoquery_backlog").fetchall() == [("js8:fio-b", "WAITING")]
    finally:
        conn.close()


def test_pending_get_acknowledges_before_slow_endpoint_work(monkeypatch, tmp_path):
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        _pending_db(tmp_path / "freqinout_nets.db", count=1)
        tab._update_pending_table()
        tab._my_callsign = lambda: "TEST1"
        tab._minutes_to_next_change = lambda: None
        monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
        monkeypatch.setattr(
            tab,
            "_pending_js8_endpoint",
            lambda _row: (SimpleNamespace(host="127.0.0.1", port=2442), "test endpoint"),
        )

        def _slow_send(*_args, **_kwargs):
            time.sleep(0.2)
            return True

        monkeypatch.setattr(tab, "_send_js8_message_to_endpoint", _slow_send)
        started = time.perf_counter()
        tab._on_pending_get_row(
            {"callsign": "K1A00", "msg_id": "0", "source_key": "js8:fio-0"}
        )
        acknowledgement_ms = (time.perf_counter() - started) * 1000.0
        assert acknowledgement_ms < 100.0
        assert tab.pending_review_summary.text() == "Sending retrieval request…"
        assert not tab.pending_table.isEnabled()

        deadline = time.monotonic() + 2.0
        while tab._pending_action_thread is not None and time.monotonic() < deadline:
            app.processEvents()
            time.sleep(0.01)
        app.processEvents()
        assert tab._pending_action_thread is None
        assert tab.pending_table.isEnabled()
        conn = sqlite3.connect(tmp_path / "freqinout_nets.db")
        try:
            assert conn.execute(
                "SELECT status FROM autoquery_backlog WHERE callsign='K1A00'"
            ).fetchone() == ("WAITING",)
        finally:
            conn.close()
    finally:
        tab.close()
        tab.deleteLater()
