from PySide6.QtWidgets import QApplication, QMessageBox
from pathlib import Path

from freqinout.core import local_ops_store
from freqinout.gui.local_report_history_tab import LocalReportHistoryTab


def _app():
    return QApplication.instance() or QApplication([])


def _use_tmp_db(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "local_ops.sqlite"
    monkeypatch.setattr(local_ops_store, "_db_path", lambda: db_path)
    monkeypatch.setattr(local_ops_store, "_SCHEMA_READY", False)
    monkeypatch.setattr(local_ops_store, "_SCHEMA_READY_DB", "")


def test_local_report_history_renders_report_table_and_readable_detail(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.record_local_report(
        callsign="K7ETC",
        source_kind="gmrs",
        source_channel="462.675",
        net_session_id="County GMRS Net",
        from_name="Test Operator",
        city="Delta",
        county="Millard",
        state="UT",
        grid="DM38ST",
        status="PRIORITY",
        topics=["Fire"],
        subject="Wildfire update",
        body="Evac route closed near the bridge.",
        confirmed_state="SECOND_HAND",
        created_utc="2026-08-10T12:00:00+00:00",
    )

    tab = LocalReportHistoryTab()

    assert tab.table.rowCount() == 1
    assert tab.table.columnCount() == 7
    assert tab.summary_total_label.text() == "Reports: 1"
    assert tab.summary_priority_label.text() == "Priority/Emergency: 1"
    assert tab.summary_filters_label.text() == "Filters: none"
    assert tab.table.item(0, tab.COL_STATUS).text() == "PRIORITY"
    assert tab.table.item(0, tab.COL_FROM).text() == "K7ETC"
    assert tab.table.item(0, tab.COL_SOURCE).text() == "GMRS 462.675"
    assert "Fire" in tab.table.item(0, tab.COL_TOPICS).text()
    assert "Wildfire update" in tab.detail_text.toPlainText()
    assert "Evac route closed" in tab.detail_text.toPlainText()
    assert "Source:" in tab.detail_text.toPlainText()
    assert "topic_evidence" not in tab.detail_text.toPlainText()


def test_local_report_history_filters_by_free_text_topic_and_status(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.record_local_report(
        callsign="K7ETC",
        status="PRIORITY",
        topics=["Fire"],
        subject="Wildfire update",
        body="Evac route closed.",
        created_utc="2026-08-10T12:00:00+00:00",
    )
    local_ops_store.record_local_report(
        callsign="N0PWR",
        status="INFO",
        topics=["Comms"],
        subject="Repeater normal",
        body="Local repeater normal.",
        created_utc="2026-08-10T13:00:00+00:00",
    )

    tab = LocalReportHistoryTab()
    tab.search_edit.setText("fire")

    assert tab.table.rowCount() == 1
    assert tab.table.item(0, tab.COL_FROM).text() == "K7ETC"
    assert "search fire" in tab.summary_filters_label.text()

    tab.search_edit.clear()
    tab.status_combo.setCurrentText("INFO")

    assert tab.table.rowCount() == 1
    assert tab.table.item(0, tab.COL_FROM).text() == "N0PWR"
    assert tab.summary_priority_label.text() == "Priority/Emergency: 0"
    assert "status INFO" in tab.summary_filters_label.text()


def test_local_report_history_show_callsign_prefilters_reports(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.record_local_report(
        callsign="K7ETC",
        status="PRIORITY",
        topics=["Fire"],
        subject="Wildfire update",
        body="Evac route closed.",
        created_utc="2026-08-10T12:00:00+00:00",
    )
    local_ops_store.record_local_report(
        callsign="N0PWR",
        status="INFO",
        topics=["Comms"],
        subject="Repeater normal",
        body="Local repeater normal.",
        created_utc="2026-08-10T13:00:00+00:00",
    )

    tab = LocalReportHistoryTab()
    tab.show_callsign("k7etc")

    assert tab.callsign_edit.text() == "K7ETC"
    assert tab.table.rowCount() == 1
    assert tab.table.item(0, tab.COL_FROM).text() == "K7ETC"


def test_local_report_history_copies_selected_report_as_operator_text(monkeypatch, tmp_path) -> None:
    app = _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.record_local_report(
        callsign="K7ETC",
        source_kind="gmrs",
        source_channel="462.675",
        net_session_id="County GMRS Net",
        city="Delta",
        county="Millard",
        state="UT",
        grid="DM38ST",
        status="PRIORITY",
        topics=["Fire"],
        subject="Wildfire update",
        body="Evac route closed near the bridge.",
        confirmed_state="SECOND_HAND",
        created_utc="2026-08-10T12:00:00+00:00",
    )

    tab = LocalReportHistoryTab()
    tab._copy_selected_report()
    text = app.clipboard().text()

    assert text.startswith("Local Report")
    assert "From: K7ETC" in text
    assert "To: County GMRS Net" in text
    assert "Topics: Fire" in text
    assert "Wildfire update" in text
    assert "Evac route closed" in text
    assert "topic_evidence" not in text
    assert "Copied selected report" in tab.copy_status_label.text()


def test_local_report_history_copies_filtered_summary(monkeypatch, tmp_path) -> None:
    app = _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.record_local_report(
        callsign="K7ETC",
        source_kind="vhf",
        source_channel="146.520",
        status="PRIORITY",
        topics=["Fire"],
        subject="Wildfire update",
        body="Evac route closed.",
        city="Delta",
        state="UT",
        grid="DM38ST",
        created_utc="2026-08-10T12:00:00+00:00",
    )
    local_ops_store.record_local_report(
        callsign="N0PWR",
        status="INFO",
        topics=["Comms"],
        subject="Repeater normal",
        body="Local repeater normal.",
        created_utc="2026-08-10T13:00:00+00:00",
    )

    tab = LocalReportHistoryTab()
    tab.search_edit.setText("fire")
    tab._copy_filtered_summary()
    text = app.clipboard().text()

    assert text.startswith("Local Reports Summary")
    assert "Reports: 1" in text
    assert "Filters: search fire" in text
    assert "PRIORITY" in text
    assert "K7ETC" in text
    assert "Wildfire update" in text
    assert "Delta, UT / DM38ST" in text
    assert "N0PWR" not in text
    assert "Copied 1 report summary" in tab.copy_status_label.text()


def test_local_report_history_deletes_selected_reports(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.record_local_report(
        callsign="K7ETC",
        status="PRIORITY",
        topics=["Fire"],
        subject="Wildfire update",
        created_utc="2026-08-10T12:00:00+00:00",
    )
    local_ops_store.record_local_report(
        callsign="N0PWR",
        status="INFO",
        topics=["Comms"],
        subject="Repeater normal",
        created_utc="2026-08-10T13:00:00+00:00",
    )
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)

    tab = LocalReportHistoryTab()
    for row_index in range(tab.table.rowCount()):
        if tab.table.item(row_index, tab.COL_FROM).text() == "K7ETC":
            tab.table.selectRow(row_index)
            break
    tab._delete_selected_reports()

    assert tab.table.rowCount() == 1
    assert tab.table.item(0, tab.COL_FROM).text() == "N0PWR"
    assert local_ops_store.list_local_reports()[0]["callsign"] == "N0PWR"
    assert "Deleted 1 local report" in tab.copy_status_label.text()


def test_main_window_registers_local_reports_separate_from_hf_operator_history() -> None:
    text = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "from freqinout.gui.local_report_history_tab import LocalReportHistoryTab" in text
    assert '("HF Operators", self.operator_history_tab)' in text
    assert '("Local Reports", self.local_report_history_tab)' in text
    assert '("Local Reports", "Local Reports")' in text
    assert "self.local_operator_tab.local_reports_requested.connect(self.open_local_reports)" in text
