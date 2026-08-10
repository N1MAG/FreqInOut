from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QScrollArea

from freqinout.core import local_ops_store
from freqinout.gui.local_ncs_tab import LocalNCSTab


def _app():
    return QApplication.instance() or QApplication([])


def _use_tmp_db(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "local_ops.sqlite"
    monkeypatch.setattr(local_ops_store, "_db_path", lambda: db_path)
    monkeypatch.setattr(local_ops_store, "_SCHEMA_READY", False)
    monkeypatch.setattr(local_ops_store, "_SCHEMA_READY_DB", "")


def test_local_ncs_saves_selected_checkin_report_to_message_intelligence_store(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)
    local_ops_store.ensure_tables()
    entry_id = local_ops_store.record_checkin(
        callsign="k7etc",
        net_name="County GMRS Net",
        channels="GMRS RPT 462.650",
        name="Test Operator",
        city="Delta",
        state="UT",
        category="GMRS",
        sitrep_status="GREEN",
        notes="General check-in note",
    )

    tab = LocalNCSTab()
    tab._net_in_progress = True
    tab._session_entry_ids.add(int(entry_id or 0))
    tab.channels_edit.setText("GMRS RPT 462.650")
    tab._load_checkins(select_id=int(entry_id or 0))

    assert tab.table.columnCount() == 7
    assert tab.table.item(0, tab.COL_LOCATION).text() == "Delta, UT"

    tab.report_source_combo.setCurrentText("GMRS")
    tab.report_status_combo.setCurrentText("PRIORITY")
    tab.report_confirmed_combo.setCurrentText("Second Hand")
    tab._toggle_report_topic("Fire", True)
    tab.report_subject_edit.setText("Wildfire update")
    tab.report_body_edit.setPlainText("Evac route closed near the bridge.")

    tab._save_local_report()

    reports = local_ops_store.list_local_reports(callsign="K7ETC")
    assert len(reports) == 1
    assert reports[0]["source_kind"] == "gmrs"
    assert reports[0]["source_channel"] == "GMRS RPT 462.650"
    assert reports[0]["status"] == "PRIORITY"
    assert reports[0]["confirmed_state"] == "SECOND_HAND"
    assert {"Fire", "Travel/Roads", "Infrastructure"}.issubset(set(reports[0]["topics"]))
    assert reports[0]["raw_reference"] == f"local_ncs_checkins:{entry_id}"
    assert tab.report_subject_edit.text() == ""
    assert tab.report_topics_label.text() == "none"
    assert "Saved report #" in tab.report_save_label.text()


def test_local_ncs_topic_buttons_toggle_selected_topics(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)

    tab = LocalNCSTab()
    tab._set_editor_enabled(True)
    fire_btn = tab._report_topic_buttons["Fire"]
    comms_btn = tab._report_topic_buttons["Comms"]

    fire_btn.click()
    comms_btn.click()

    assert tab._report_topics == ["Fire", "Comms"]
    assert tab.report_topics_label.text() == "Fire, Comms"
    assert fire_btn.isChecked() is True
    assert comms_btn.isChecked() is True

    fire_btn.click()

    assert tab._report_topics == ["Comms"]
    assert tab.report_topics_label.text() == "Comms"
    assert fire_btn.isChecked() is False


def test_local_ncs_report_capture_scrolls_without_compressing_core_controls(monkeypatch, tmp_path) -> None:
    _app()
    _use_tmp_db(monkeypatch, tmp_path)

    tab = LocalNCSTab()

    assert isinstance(tab.local_ncs_scroll_area, QScrollArea)
    assert tab.local_ncs_scroll_area.widget() is tab.local_ncs_content
    assert tab.local_ncs_scroll_area.widgetResizable() is True
    assert tab.local_ncs_scroll_area.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
    assert tab.local_ncs_scroll_area.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
    assert tab.table.minimumHeight() >= 180
    assert tab.table.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
    assert tab.notes_edit.maximumHeight() <= 140
    assert tab.report_body_edit.maximumHeight() <= 150
