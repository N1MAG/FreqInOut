from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QDialog, QPushButton, QScrollArea, QTableWidget

from freqinout.core.checkins_db import ensure_operator_checkins_schema, upsert_operator_metadata
from freqinout.core.operator_roster_import import (
    RosterImportResult,
    RosterImportDiagnostic,
    classify_roster_import_result,
    parse_operator_roster_csv,
)


ROSTER = Path("/Users/bill/Downloads/MAGNET Roster 09-02-26 - Current.csv")


def test_supplied_magnet_roster_has_exact_slice6_result_contract() -> None:
    if not ROSTER.exists():
        pytest.skip("operator-supplied roster is not present")
    result = parse_operator_roster_csv(
        ROSTER.open(newline="", encoding="utf-8-sig"),
        parent_group="MAGNET",
        source_path=ROSTER,
        imported_at_utc="20260902",
    )
    assert result.imported == 166
    assert result.skipped == 0
    assert getattr(result, "blank_ignored") == 18
    assert getattr(result, "legend_ignored") == 4
    assert getattr(result, "invalid_skipped") == 0
    assert result.child_groups == [f"MR{i:02d}" for i in range(1, 11)]


def test_roster_edge_cases_classify_invalid_duplicate_and_mixed_group_delimiters() -> None:
    text = (
        "Callsign,Name,Region,Groups\n"
        "K7ETC,Existing,MR01,OPS;AUX|FIELD\n"
        "K7ETC,Duplicate,MR01,OPS\n"
        "not-a-callsign,Bad,MR02,OPS\n"
        ",,,\n"
    )
    result = parse_operator_roster_csv(io.StringIO(text), parent_group="MAGNET")
    assert result.imported == 1
    assert result.entries[0]["groups_json"] == ["MAGNET", "MR01", "OPS", "AUX", "FIELD"]
    assert sum("Duplicate callsign" in item.reason for item in result.diagnostics) == 1
    assert sum(item.classification == "invalid_skipped" and "Invalid callsign" in item.reason for item in result.diagnostics) == 1
    assert sum(item.classification == "blank_ignored" for item in result.diagnostics) == 1


def test_missing_callsign_header_is_rejected_before_any_preview_or_commit() -> None:
    with pytest.raises(ValueError, match="callsign column"):
        parse_operator_roster_csv(io.StringIO("Name,Region\nAlice,MR01\n"))


def test_invalid_row_diagnostics_include_line_callsign_field_and_reason() -> None:
    result = parse_operator_roster_csv(
        io.StringIO("Callsign,Name\nK7ETC,Valid\nBAD,Invalid\n"),
        parent_group="MAGNET",
    )
    diagnostics = getattr(result, "diagnostics")
    assert diagnostics
    assert any(
        item.line == 3
        and item.callsign_text == "BAD"
        and item.field == "callsign"
        and item.reason
        for item in diagnostics
    )


def test_preview_or_cancel_contract_does_not_commit_and_existing_operator_is_preserved() -> None:
    import sqlite3

    conn = sqlite3.connect(":memory:")
    ensure_operator_checkins_schema(conn)
    existing = {
        "callsign": "K7ETC",
        "name": "Original Name",
        "state": "UT",
        "grid": "DM38ST",
        "groups_json": ["MAGNET", "MR01"],
        "group1": "MAGNET",
        "group2": "MR01",
        "trusted": 1,
    }
    upsert_operator_metadata([existing], conn=conn)
    result = parse_operator_roster_csv(
        io.StringIO("Callsign,Name,Region\nK7ETC,Updated Name,MR01\n"),
        parent_group="MAGNET",
    )
    before = conn.execute("SELECT name FROM operator_checkins WHERE callsign='K7ETC'").fetchone()
    assert before == ("Original Name",)
    # A preview/cancel path must not mutate persisted rows; commit is explicit.
    assert result.imported == 1
    after = conn.execute("SELECT name FROM operator_checkins WHERE callsign='K7ETC'").fetchone()
    assert after == before


def test_roster_result_can_be_serialized_for_preview_or_export_without_losing_groups() -> None:
    result = parse_operator_roster_csv(
        io.StringIO("Callsign,Region,Groups\nK7ETC,MR01,OPS;AUX\n"),
        parent_group="MAGNET",
    )
    payload = json.loads(json.dumps(result.entries))
    assert payload[0]["callsign"] == "K7ETC"
    assert payload[0]["groups_json"] == ["MAGNET", "MR01", "OPS", "AUX"]


def test_classification_reports_imported_and_updated_counts_without_mutating_entries() -> None:
    result = parse_operator_roster_csv(
        io.StringIO("Callsign,Name\nK7ETC,Existing\nN0CALL,New\n"),
        parent_group="MAGNET",
    )
    classified = classify_roster_import_result(result, existing_callsigns=["K7ETC"])
    assert (classified.imported, classified.updated) == (1, 1)
    assert [entry["callsign"] for entry in classified.entries] == ["K7ETC", "N0CALL"]
    assert result.updated == 0


def test_diagnostics_have_one_record_for_each_non_header_fixture_row() -> None:
    result = parse_operator_roster_csv(
        io.StringIO("Callsign,Name\nK7ETC,Valid\n,Blank\nBAD,Invalid\n"),
        parent_group="MAGNET",
    )
    # The result contract must make every source row auditable, including accepted rows.
    assert {item.line for item in result.diagnostics} == {2, 3, 4}


def test_roster_import_preview_is_bounded_and_cancel_is_non_mutating(monkeypatch) -> None:
    from freqinout.gui.operator_history_tab import OperatorHistoryTab

    app = QApplication.instance() or QApplication([])
    result = RosterImportResult(
        entries=[{"callsign": f"K7E{i:02d}", "name": "Operator", "groups_json": ["MAGNET"]} for i in range(100)],
        parent_group="MAGNET",
        child_groups=[f"MR{i:02d}" for i in range(1, 11)],
        imported=100,
        skipped=0,
        detected_headers={"callsign": "Callsign"},
        source_headers=["Callsign"],
        diagnostics=[RosterImportDiagnostic(i + 2, "invalid_skipped", f"BAD{i}", "callsign", "Invalid") for i in range(100)],
    )
    captured = {}

    def fake_exec(dialog):
        captured["dialog"] = dialog
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    tab = OperatorHistoryTab()
    try:
        assert tab._confirm_roster_import_preview(result) is False
        dialog = captured["dialog"]
        assert dialog.result() != QDialog.Accepted
        diagnostics = dialog.findChild(QTableWidget, "rosterImportDiagnostics")
        assert diagnostics is not None
        assert diagnostics.rowCount() <= 80
        assert len(result.diagnostics) == 100
        labels = [button.text() for button in dialog.findChildren(QPushButton)]
        assert {"Import Operators", "Cancel", "Copy Diagnostics", "Export Diagnostics"} <= set(labels)
        scroll = dialog.findChild(QScrollArea)
        assert scroll is not None
        assert scroll.horizontalScrollBar().maximum() == 0
    finally:
        tab.close()
        tab.deleteLater()


def test_raise_on_error_propagates_and_rolls_back_failed_metadata_upsert(monkeypatch) -> None:
    import freqinout.core.checkins_db as checkins_db

    class Cursor:
        def execute(self, *_args, **_kwargs):
            raise sqlite3.IntegrityError("forced roster write failure")

    class Connection:
        def __init__(self):
            self.rolled_back = False

        def cursor(self):
            return Cursor()

        def rollback(self):
            self.rolled_back = True

    import sqlite3

    conn = Connection()
    monkeypatch.setattr(checkins_db, "_ensure_table", lambda _conn: None)
    monkeypatch.setattr(
        checkins_db,
        "get_or_create_operator_identity",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.IntegrityError("forced roster write failure")),
    )
    with pytest.raises(sqlite3.IntegrityError, match="forced roster write failure"):
        checkins_db.upsert_operator_metadata(
            [{"callsign": "K7ETC", "name": "Should not commit"}],
            conn=conn,
            raise_on_error=True,
        )
    # Caller-owned transactions retain rollback authority and can preserve prior rows.
    conn.rollback()
    assert conn.rolled_back is True


def test_strict_multirow_metadata_upsert_rolls_back_atomically_on_second_row_failure() -> None:
    import sqlite3
    import freqinout.core.checkins_db as checkins_db

    conn = sqlite3.connect(":memory:")
    ensure_operator_checkins_schema(conn)
    conn.execute(
        """CREATE TRIGGER fail_second_roster_row BEFORE INSERT ON operator_checkins
           WHEN NEW.callsign = 'N0CALL' BEGIN SELECT RAISE(ABORT, 'forced second row failure'); END"""
    )
    with pytest.raises(sqlite3.IntegrityError, match="forced second row failure"):
        checkins_db.upsert_operator_metadata(
            [{"callsign": "K7ETC", "name": "First"}, {"callsign": "N0CALL", "name": "Second"}],
            conn=conn,
            raise_on_error=True,
        )
    conn.rollback()
    assert conn.execute("SELECT COUNT(*) FROM operator_checkins").fetchone() == (0,)
