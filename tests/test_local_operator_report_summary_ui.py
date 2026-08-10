from PySide6.QtWidgets import QApplication, QCheckBox, QWidget

from freqinout.core import local_ops_store
from freqinout.gui.local_operator_tab import LocalOperatorTab


def _app():
    return QApplication.instance() or QApplication([])


def _use_tmp_db(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "local_ops.sqlite"
    monkeypatch.setattr(local_ops_store, "_db_path", lambda: db_path)
    monkeypatch.setattr(local_ops_store, "_SCHEMA_READY", False)
    monkeypatch.setattr(local_ops_store, "_SCHEMA_READY_DB", "")


def test_local_operator_table_shows_and_searches_latest_report_summary(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.upsert_operator(
        "K7ETC",
        first_name="Test",
        last_name="Operator",
        city="Delta",
        state="UT",
        category="GMRS",
        notes="General roster note",
        touch_seen=False,
    )
    local_ops_store.record_local_report(
        callsign="K7ETC",
        status="PRIORITY",
        topics=["Fire"],
        subject="Wildfire update",
        body="Road closed near bridge.",
        created_utc="2026-08-10T12:00:00+00:00",
    )

    tab = LocalOperatorTab()

    assert tab.table.columnCount() == 13
    report_item = tab.table.item(0, tab.COL_REPORT)
    assert report_item is not None
    assert "PRIORITY" in report_item.text()
    assert "Wildfire update" in report_item.text()
    assert "General roster note" in tab.table.item(0, tab.COL_NOTES).text()

    tab.search_edit.setText("wildfire")

    assert tab.table.rowCount() == 1
    assert tab.table.item(0, tab.COL_CALLSIGN).text() == "K7ETC"


def test_local_operator_view_reports_action_emits_selected_callsign(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.upsert_operator("K7ETC", first_name="Test", state="UT", touch_seen=False)

    tab = LocalOperatorTab()
    seen: list[str] = []
    tab.local_reports_requested.connect(lambda callsign: seen.append(callsign))
    wrapper = tab.table.cellWidget(0, tab.COL_SELECT)
    assert isinstance(wrapper, QWidget)
    checkbox = wrapper.findChild(QCheckBox)
    assert checkbox is not None
    checkbox.setChecked(True)

    tab._view_selected_reports()

    assert seen == ["K7ETC"]
