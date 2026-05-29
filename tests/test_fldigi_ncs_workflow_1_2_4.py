import datetime

from PySide6.QtWidgets import QApplication, QToolButton

from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab


class _SettingsStub:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return {"net_schedule": self._rows}


class _Checked:
    def isChecked(self):
        return True


def _tab():
    return FldigiNetControlTab.__new__(FldigiNetControlTab)


def _app():
    return QApplication.instance() or QApplication([])


def test_role_aware_copy_format_puts_role_before_traffic():
    tab = _tab()

    line = tab._format_roster_row_for_copy(
        {
            "callsign": "n1mag",
            "name": "Bill",
            "state": "co",
            "traffic": "1RR QST",
            "station_role": "NCS",
        }
    )

    assert line == "N1MAG / Bill / CO / NCS / 1RR QST"


def test_role_aware_copy_omits_role_for_non_ncs_sources():
    tab = _tab()

    line = tab._format_roster_row_for_copy(
        {
            "callsign": "w1abc",
            "name": "Bob",
            "state": "ca",
            "traffic": "1RR",
            "heard_by": "NCS",
        }
    )

    assert line == "W1ABC / Bob / CA / 1RR"


def test_duplicate_roster_rows_merge_corrections_without_repeating_callsign():
    tab = _tab()

    rows = tab._roster_merge_duplicate_rows(
        [
            {"callsign": "n1mag", "name": "", "state": "CO", "traffic": "", "category": "TFC", "heard_by": "NCS", "station_role": ""},
            {"callsign": "N1MAG", "name": "Bill", "state": "", "traffic": "1RR", "category": "TFC", "heard_by": "ANCS", "station_role": ""},
        ]
    )

    assert rows == [
        {
            "callsign": "N1MAG",
            "name": "Bill",
            "state": "CO",
            "traffic": "1RR",
            "category": "TFC",
            "heard_by": "Both",
            "station_role": "",
        }
    ]


def test_promote_heard_or_acked_side_to_both():
    tab = _tab()

    assert tab._roster_promote_side("", "NCS") == "NCS"
    assert tab._roster_promote_side("NCS", "ANCS") == "Both"
    assert tab._roster_promote_side("Both", "NCS") == "Both"


def test_needs_ack_text_is_role_specific_and_uses_directed_by():
    tab = _tab()
    tab._current_net_control_role = lambda: "NCS"
    tab._roster_table_rows = lambda: [
        {"callsign": "NCS1", "name": "Net", "state": "CO", "traffic": "", "heard_by": "Both", "acked_by": "Both", "station_role": "NCS"},
        {"callsign": "W1ABC", "name": "Bob", "state": "CA", "traffic": "1RR", "heard_by": "NCS", "acked_by": "", "station_role": "", "notes": "needs welfare follow-up"},
        {"callsign": "N1MAG", "name": "Bill", "state": "CO", "traffic": "", "heard_by": "NCS", "acked_by": "NCS", "station_role": ""},
        {"callsign": "K0XYZ", "name": "Lee", "state": "WY", "traffic": "", "heard_by": "Both", "acked_by": "ANCS", "station_role": ""},
        {"callsign": "K7BTH", "name": "Beth", "state": "AZ", "traffic": "", "heard_by": "ANCS", "acked_by": "", "station_role": ""},
    ]

    text = tab._needs_ack_text()

    assert text == "W1ABC / Bob / CA / 1RR\nK0XYZ / Lee / WY"
    assert "N1MAG" not in text
    assert "K7BTH" not in text
    assert "NCS1" not in text
    assert "welfare" not in text


def test_roster_clipboard_text_adds_blank_line_before_and_after():
    tab = _tab()

    assert tab._roster_clipboard_text("W1ABC / Bob / CA / 1RR") == "\nW1ABC / Bob / CA / 1RR\n"
    assert tab._roster_clipboard_text("") == ""


def test_copy_ack_needed_marks_copied_rows_acked_by_current_role():
    _app()
    tab = FldigiNetControlTab()
    tab.role_combo.setCurrentText("NCS")
    tab._roster_append_row("W1ABC", "Bob", "CA", "1RR", "TFC", "Local")
    tab._roster_append_row("N1MAG", "Bill", "CO", "", "QRU", "Local")
    tab._roster_set_side(1, tab.COL_ACKED, "ANCS")

    marked = tab._mark_needs_ack_rows_copied()
    rows = tab._roster_table_rows()

    assert marked == 2
    assert rows[0]["acked_by"] == "NCS"
    assert rows[1]["acked_by"] == "Both"


def test_roster_copy_text_excludes_operational_notes():
    tab = _tab()
    tab._roster_table_rows = lambda: [
        {"callsign": "W1ABC", "name": "Bob", "state": "CA", "traffic": "1RR", "category": "TFC", "station_role": "", "notes": "generator low"},
        {"callsign": "N1MAG", "name": "Bill", "state": "CO", "traffic": "", "category": "QRU", "station_role": "ANCS", "notes": "needs relief"},
    ]

    assert tab._roster_table_text() == "N1MAG / Bill / CO / ANCS\nW1ABC / Bob / CA / 1RR"
    assert "generator" not in tab._roster_table_text("TFC")


def test_keyword_and_slash_traffic_parse_to_separate_fields():
    _app()
    tab = FldigiNetControlTab()

    tab._roster_append_row("W1ABC", "Joe", "FL", "Psalms 4:20 / 1PP", "TFC", "Local")

    row = tab._roster_table_rows()[0]
    assert row["keyword"] == "Psalms 4:20"
    assert row["traffic"] == "1PP"
    assert tab._roster_table_text() == "W1ABC / Joe / FL / Psalms 4:20 / 1PP"


def test_roster_notes_archive_text_includes_only_noted_rows():
    tab = _tab()
    tab._roster_table_rows = lambda: [
        {"callsign": "W1ABC", "name": "Bob", "state": "CA", "traffic": "1RR", "category": "TFC", "station_role": "", "notes": "generator low"},
        {"callsign": "N1MAG", "name": "Bill", "state": "CO", "traffic": "", "category": "QRU", "station_role": "ANCS", "notes": ""},
    ]

    text = tab._roster_notes_archive_text(net_name="Weekly Net", archived_utc="20260515_210000Z")

    assert "Check-in Notes - Weekly Net" in text
    assert "Archived: 20260515_210000Z" in text
    assert "W1ABC / Bob / CA / - / TFC / generator low" in text
    assert "N1MAG" not in text


def test_roster_sort_keeps_control_rows_pinned_then_traffic_before_qru_by_sequence():
    tab = _tab()
    rows = [
        {"callsign": "W9ZZZ", "station_role": "", "traffic": "", "checkin_seq": "1"},
        {"callsign": "ANCS1", "station_role": "ANCS", "checkin_seq": "2"},
        {"callsign": "A1AAA", "station_role": "", "traffic": "1PP", "checkin_seq": "4"},
        {"callsign": "B2BBB", "station_role": "", "traffic": "1RR", "checkin_seq": "3"},
        {"callsign": "NCS1", "station_role": "NCS", "checkin_seq": "5"},
    ]

    ordered = [row["callsign"] for _rank, row in sorted(((tab._roster_role_rank(row, idx), row) for idx, row in enumerate(rows)), key=lambda item: item[0])]

    assert ordered == ["NCS1", "ANCS1", "A1AAA", "B2BBB", "W9ZZZ"]


def test_ui_column_sort_keeps_ncs_and_ancs_pinned():
    _app()
    tab = FldigiNetControlTab()
    tab._roster_append_row("W9ZZZ", "Zulu", "WY", "", "QRU", "Local")
    tab._roster_append_row("ANCS1", "Relay", "CO", "", "QRU", "ANCS - Net Control", overwrite_source=True)
    tab._roster_append_row("A1AAA", "Alpha", "AZ", "1RR", "TFC", "Local")
    tab._roster_append_row("NCS1", "Net", "CO", "", "QRU", "NCS - Net Control", overwrite_source=True)

    tab._sort_roster_table_by_column(tab.COL_CALLSIGN)
    ascending = [row["callsign"] for row in tab._roster_table_rows()]
    tab._sort_roster_table_by_column(tab.COL_CALLSIGN)
    descending = [row["callsign"] for row in tab._roster_table_rows()]

    assert ascending == ["NCS1", "ANCS1", "A1AAA", "W9ZZZ"]
    assert descending == ["NCS1", "ANCS1", "W9ZZZ", "A1AAA"]


def test_default_sort_restores_traffic_priority_after_column_sort():
    _app()
    tab = FldigiNetControlTab()
    tab._show_roster_action_status = lambda *args, **kwargs: None
    tab._roster_append_row("W1AAA", "Alpha", "CO", "1RR", "TFC", "Local")
    tab._roster_append_row("W2BBB", "Bravo", "CO", "", "QRU", "Local")
    tab._roster_append_row("W3CCC", "Charlie", "CO", "", "QRU", "Local")
    tab._roster_append_row("W4DDD", "Delta", "CO", "1PP", "TFC", "Local")

    tab._sort_roster_table_by_column(tab.COL_CALLSIGN)
    tab._sort_roster_table_by_column(tab.COL_CALLSIGN)
    assert [row["callsign"] for row in tab._roster_table_rows()] == ["W4DDD", "W3CCC", "W2BBB", "W1AAA"]

    tab._restore_default_roster_sort()

    assert [row["callsign"] for row in tab._roster_table_rows()] == ["W4DDD", "W1AAA", "W2BBB", "W3CCC"]


def test_roster_append_populates_visible_table_row_for_manual_checkin():
    _app()
    tab = FldigiNetControlTab()
    tab.role_combo.setCurrentText("ANCS")

    row = tab._roster_append_row("N1MAG", "Bill", "CO", "1RR", "TFC", "Local")

    assert row == 0
    assert tab.roster_table.rowCount() == 1
    rows = tab._roster_table_rows()
    assert rows[0]["callsign"] == "N1MAG"
    assert rows[0]["heard_by"] == "ANCS"
    assert rows[0]["station_role"] == ""
    heard_buttons = {button.text(): button.isChecked() for button in tab.roster_table.cellWidget(0, tab.COL_HEARD).findChildren(QToolButton)}
    acked_buttons = {button.text(): button.isChecked() for button in tab.roster_table.cellWidget(0, tab.COL_ACKED).findChildren(QToolButton)}
    assert heard_buttons == {"NCS": False, "ANCS": True}
    assert acked_buttons == {"NCS": False, "ANCS": False}
    heard_chip = tab.roster_table.cellWidget(0, tab.COL_HEARD).findChildren(QToolButton)[0]
    assert "QToolButton:checked" in heard_chip.styleSheet()
    assert heard_chip.autoRaise() is False
    assert not hasattr(tab, "mark_heard_btn")
    assert not hasattr(tab, "mark_acked_btn")


def test_directed_by_partner_click_switches_to_partner_before_both():
    _app()
    tab = FldigiNetControlTab()
    tab.role_combo.setCurrentText("NCS")
    tab._roster_append_row("W1ABC", "Bob", "CA", "1RR", "TFC", "Local")

    heard_buttons = tab._roster_side_buttons(0, tab.COL_HEARD)
    heard_buttons["ANCS"].click()
    assert tab._roster_table_rows()[0]["heard_by"] == "ANCS"

    heard_buttons["NCS"].click()
    assert tab._roster_table_rows()[0]["heard_by"] == "Both"


def test_role_filtered_category_copy_and_next_tfc_queue(tmp_path):
    _app()
    tab = FldigiNetControlTab()
    tab.settings.set("fldigi_checkin_dir", str(tmp_path))
    tab.role_combo.setCurrentText("NCS")
    tab._roster_append_row("NCS1", "Net", "CO", "1RR", "TFC", "NCS - Net Control", overwrite_source=True)
    tab._roster_set_side(0, tab.COL_HEARD, "Both")
    tab._roster_append_row("ANCS1", "Relay", "WY", "1RR", "TFC", "ANCS - Net Control", overwrite_source=True)
    tab._roster_set_side(1, tab.COL_HEARD, "Both")
    tab._roster_append_row("W1ABC", "Bob", "CA", "1RR", "TFC", "Local")
    tab._roster_append_row("K0XYZ", "Lee", "WY", "1RR", "TFC", "Local")
    tab._roster_set_side(tab._roster_find_row("K0XYZ"), tab.COL_HEARD, "ANCS")

    rows = tab._role_filtered_category_rows("TFC", "NCS")
    assert [row["callsign"] for row in rows] == ["NCS1", "ANCS1", "W1ABC"]

    first = tab._next_tfc_row("NCS")
    assert first["callsign"] == "ANCS1"
    tab._next_tfc_last_served["NCS"] = "ANCS1"
    tab._next_tfc_called_by_role["NCS"].add("ANCS1")
    second = tab._next_tfc_row("NCS")
    assert second["callsign"] == "W1ABC"


def test_action_scope_filters_roster_rows_by_role_and_shared():
    _app()
    tab = FldigiNetControlTab()
    tab.role_combo.setCurrentText("NCS")
    tab._roster_append_row("NCS1", "Net", "CO", "", "QRU", "NCS - Net Control", overwrite_source=True)
    tab._roster_set_side(0, tab.COL_HEARD, "Both")
    tab._roster_append_row("W1ABC", "Bob", "CA", "1RR", "TFC", "Local")
    tab._roster_append_row("K0XYZ", "Lee", "WY", "1RR", "TFC", "Local")
    tab._roster_set_side(tab._roster_find_row("K0XYZ"), tab.COL_HEARD, "ANCS")
    tab._roster_append_row("K7BTH", "Beth", "AZ", "1RR", "TFC", "Local")
    tab._roster_set_side(tab._roster_find_row("K7BTH"), tab.COL_HEARD, "Both")

    assert [row["callsign"] for row in tab._scope_filtered_rows("NCS", "TFC")] == ["W1ABC", "K7BTH"]
    assert [row["callsign"] for row in tab._scope_filtered_rows("ANCS", "TFC")] == ["K0XYZ", "K7BTH"]
    assert [row["callsign"] for row in tab._scope_filtered_rows("SHARED", "TFC")] == ["K7BTH"]
    assert [row["callsign"] for row in tab._scope_filtered_rows("ALL", "TFC")] == ["W1ABC", "K0XYZ", "K7BTH"]


def test_relay_compare_includes_qru_rows_missing_from_partner_reference():
    _app()
    tab = FldigiNetControlTab()
    tab.role_combo.setCurrentText("ANCS")
    tab._roster_append_row("W1ABC", "Bob", "CA", "1PP", "TFC", "Local")
    tab._roster_append_row("K0XYZ", "Lee", "WY", "", "QRU", "Local")
    tab._roster_append_row("N5REF", "Sue", "AZ", "1RR", "TFC", "Local")
    tab.reference_card.set_text("N5REF / Sue / AZ / 1RR")

    rows = tab._relay_entries_missing_from_reference("ANCS")

    assert [row["callsign"] for row in rows] == ["W1ABC", "K0XYZ"]
    assert tab._roster_table_text_for_rows(rows) == "W1ABC / Bob / CA / 1PP\nK0XYZ / Lee / WY"


def test_inline_compare_for_ancs_shows_copyable_relays_to_ncs():
    _app()
    tab = FldigiNetControlTab()
    tab.role_combo.setCurrentText("ANCS")
    tab._workspace_bucket_defaults = {"source_bucket_id": "roster", "target_bucket_id": "reference"}
    tab._roster_append_row("W1ABC", "Bob", "CA", "1PP", "TFC", "Local")
    tab._roster_append_row("K0XYZ", "Lee", "WY", "", "QRU", "Local")
    tab._roster_append_row("N5REF", "Sue", "AZ", "1RR", "TFC", "Local")
    tab.reference_card.set_text("N5REF / Sue / AZ / 1RR")

    tab._run_inline_compare()

    assert "Stations to Relay to NCS" in tab.compare_results_card.title()
    assert "W1ABC / Bob / CA / 1PP" in tab.compare_results_text.toPlainText()
    assert "K0XYZ / Lee / WY" in tab._compare_missing_text
    assert "N5REF" not in tab._compare_missing_text


def test_role_first_macro_files_stay_current_from_roster(tmp_path):
    _app()
    tab = FldigiNetControlTab()
    tab.settings.set("fldigi_checkin_dir", str(tmp_path))
    tab.role_combo.setCurrentText("NCS")
    tab._net_in_progress = True
    tab._ensure_checkin_files()
    tab._roster_append_row("W1ABC", "Bob", "CA", "1RR", "TFC", "Local")
    tab._roster_append_row("K0XYZ", "Lee", "WY", "", "QRU", "Local")
    tab._roster_set_side(tab._roster_find_row("K0XYZ"), tab.COL_HEARD, "ANCS")
    tab._roster_sync_legacy_buffers()

    assert (tmp_path / "CheckIns_TFC.txt").read_text(encoding="utf-8") == "W1ABC / Bob / CA / 1RR"
    assert "W1ABC / Bob / CA / 1RR" in (tmp_path / "NCS_CheckIns_TFC.txt").read_text(encoding="utf-8")
    assert (tmp_path / "ANCS_CheckIns_QRU.txt").read_text(encoding="utf-8") == "K0XYZ / Lee / WY"
    assert "W1ABC / Bob / CA / 1RR" in (tmp_path / "NCS_ACK_Pending.txt").read_text(encoding="utf-8")
    assert not (tmp_path / "ACK_Pending.txt").exists()
    assert not (tmp_path / "Next_TFC.txt").exists()


def test_tfc_status_chip_is_separate_from_callsign():
    _app()
    tab = FldigiNetControlTab()
    tab.role_combo.setCurrentText("NCS")
    tab._roster_append_row("W1ABC", "Bob", "CA", "1RR", "TFC", "Local")

    assert tab.roster_table.item(0, tab.COL_CALLSIGN).text() == "W1ABC"
    status = tab.roster_table.cellWidget(0, tab.COL_TFC_STATUS)
    assert status.text() == "Pending"


def test_live_action_layout_separates_scope_save_and_primary_actions():
    _app()
    tab = FldigiNetControlTab()

    assert tab.macro_profile_details_btn.text() == "Macro: None"
    assert not tab.setup_details_frame.isVisible()
    assert tab.save_btn.text() == "Save Check-ins"
    assert tab.copy_needs_sync_btn.text() == "ACK Needed"
    assert tab.next_tfc_btn.text() == "Next TFC"
    assert tab.copy_tfc_btn.text() == "TFC"
    assert tab.copy_roster_summary_btn.text() == "All Check-ins"
    assert set(tab.roster_scope_buttons) == {"NCS", "ANCS", "SHARED", "ALL"}


def test_next_tfc_end_of_queue_marks_last_station_called(tmp_path):
    _app()
    tab = FldigiNetControlTab()
    tab.settings.set("fldigi_checkin_dir", str(tmp_path))
    tab.role_combo.setCurrentText("NCS")
    tab._roster_append_row("W1ABC", "Bob", "CA", "1RR", "TFC", "Local")

    tab._copy_next_tfc()
    status = tab.roster_table.cellWidget(0, tab.COL_TFC_STATUS)
    assert status.text() == "Now"

    tab._copy_next_tfc()
    status = tab.roster_table.cellWidget(0, tab.COL_TFC_STATUS)
    assert status.text() == "Called"
    assert tab._next_tfc_last_served["NCS"] == ""


def test_net_control_role_rows_default_heard_and_acked_to_both():
    _app()
    tab = FldigiNetControlTab()

    row = tab._add_net_control_roster_row("NCS1", "NCS")

    assert row == 0
    rows = tab._roster_table_rows()
    assert rows[0]["callsign"] == "NCS1"
    assert rows[0]["station_role"] == "NCS"
    assert rows[0]["heard_by"] == "Both"
    assert rows[0]["acked_by"] == "Both"
    heard_buttons = {button.text(): button.isChecked() for button in tab.roster_table.cellWidget(0, tab.COL_HEARD).findChildren(QToolButton)}
    acked_buttons = {button.text(): button.isChecked() for button in tab.roster_table.cellWidget(0, tab.COL_ACKED).findChildren(QToolButton)}
    assert heard_buttons == {"NCS": True, "ANCS": True}
    assert acked_buttons == {"NCS": True, "ANCS": True}


def test_setting_net_control_role_updates_existing_role_row():
    _app()
    tab = FldigiNetControlTab()

    first = tab._add_net_control_roster_row("ANCS1", "ANCS")
    second = tab._add_net_control_roster_row("ANCS2", "ANCS")

    rows = tab._roster_table_rows()
    ancs_rows = [row for row in rows if row["station_role"] == "ANCS"]
    assert first == 0
    assert second == 0
    assert len(ancs_rows) == 1
    assert ancs_rows[0]["callsign"] == "ANCS2"
    assert "ANCS1" not in {row["callsign"] for row in rows}


def test_editing_net_control_roster_callsign_updates_set_field():
    _app()
    tab = FldigiNetControlTab()
    tab.role_combo.setCurrentText("NCS")
    tab._add_net_control_roster_row("ANCS1", "ANCS")
    row = tab._roster_find_role_row("ANCS")

    tab.roster_table.item(row, tab.COL_CALLSIGN).setText("ANCS2")

    assert tab.partner_primary_edit.text() == "ANCS2"
    assert tab._ancs_partner_call == "ANCS2"


def test_log_assisted_intake_is_disabled_while_hidden():
    tab = _tab()
    tab.LOG_ASSISTED_INTAKE_VISIBLE = False
    tab.log_assisted_enable_chk = _Checked()

    assert tab._log_assisted_enabled() is False


def test_net_schedule_occurrences_include_active_window_and_dedupe_daily_rows():
    tab = _tab()
    now = datetime.datetime.now(datetime.timezone.utc)
    start = (now - datetime.timedelta(minutes=15)).strftime("%H:%M")
    end = (now + datetime.timedelta(minutes=45)).strftime("%H:%M")
    row = {
        "net_name": "Late Friendly Net",
        "day_utc": now.strftime("%A"),
        "start_utc": start,
        "end_utc": end,
        "recurrence": "Daily",
        "mode": "MFSK",
        "band": "80m",
        "frequency": "3.583",
    }
    tab.settings = _SettingsStub([dict(row), dict(row)])

    occurrences = tab._net_schedule_occurrences(window_hours=12)

    assert len(occurrences) == 1
    assert occurrences[0]["active"] is True
    assert occurrences[0]["row"]["net_name"] == "Late Friendly Net"
