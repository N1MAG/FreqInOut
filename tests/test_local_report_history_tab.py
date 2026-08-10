from PySide6.QtWidgets import QApplication
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
    assert tab.table.item(0, tab.COL_STATUS).text() == "PRIORITY"
    assert tab.table.item(0, tab.COL_FROM).text() == "K7ETC"
    assert tab.table.item(0, tab.COL_TO).text() == "County GMRS Net"
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

    tab.search_edit.clear()
    tab.status_combo.setCurrentText("INFO")

    assert tab.table.rowCount() == 1
    assert tab.table.item(0, tab.COL_FROM).text() == "N0PWR"


def test_main_window_registers_local_reports_separate_from_hf_operator_history() -> None:
    text = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "from freqinout.gui.local_report_history_tab import LocalReportHistoryTab" in text
    assert '("HF Operators", self.operator_history_tab)' in text
    assert '("Local Reports", self.local_report_history_tab)' in text
    assert '("Local Reports", "Local Reports")' in text
