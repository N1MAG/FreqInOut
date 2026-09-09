from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QGroupBox, QLabel, QLineEdit

from freqinout.core.sop_action_model import SopActionDraftCollection
from freqinout.gui.sop_tab import SOPTab
from freqinout.gui.theme import apply_app_theme, get_theme


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _payload(index: int = 0) -> dict:
    return {
        "id": index + 1,
        "group_name": "OPS",
        "condition_levels": "2,3",
        "band": "40M",
        "frequency": "7.078",
        "software": "JS8Call",
        "mode": "DIGITAL",
        "action_key": "send_message",
        "action_label": "Send message",
        "enabled": True,
        "daily_start_utc": "02:00",
        "daily_end_utc": "03:00",
        "duration_minutes": 60,
        "interval_minutes": 180,
        "interval_phase_minutes": 15,
        "interval_hours": 3,
        "conflict_policy": "SOP_ALL",
        "daily_conflict_summary": "Daily clear",
        "net_conflict_summary": "Net clear",
        "schedule_applied": True,
        "description": "Check in and report status",
        "contact_rule": "callsign",
        "contact_target": "Callsign",
    }


def test_sop_action_collection_round_trips_order_and_preserves_fields() -> None:
    collection = SopActionDraftCollection([_payload(0), {**_payload(1), "group_name": "AUX"}])
    rows = collection.payloads()
    assert [row["group_name"] for row in rows] == ["OPS", "AUX"]
    assert [row["sort_order"] for row in rows] == [0, 1]
    assert rows[0]["description"] == "Check in and report status"
    assert rows[0]["interval_phase_minutes"] == 15


def test_sop_action_collection_duplicate_remove_and_stable_ids() -> None:
    collection = SopActionDraftCollection([_payload(0)])
    duplicate = collection.duplicate(0)
    assert duplicate is not None
    assert len(collection.rows()) == 2
    assert collection.rows()[0].id == 1
    assert collection.rows()[1].id == 0
    assert collection.rows()[1].description == collection.rows()[0].description
    assert collection.remove(0) is True
    assert collection.rows()[0].id == 0
    assert collection.remove(9) is False
    assert collection.update(0, "group_name", "NEW") is True
    assert collection.update(0, "not_a_field", "x") is False


def test_sop_cards_mirror_draft_to_advanced_table_without_field_loss() -> None:
    _app()
    tab = SOPTab()
    try:
        tab._action_drafts.replace([_payload()])
        tab._reload_advanced_table_from_drafts()
        tab._sop_cards_expanded = True
        tab._render_sop_action_cards()
        card = tab.findChild(QGroupBox, "sopActionCard")
        assert card is not None
        group = card.findChild(QLineEdit, "sopCard_group_name")
        description = card.findChild(QLineEdit, "sopCard_description")
        assert group is not None and description is not None
        group.setText("AUX")
        description.setText("Preserve this detailed instruction")
        tab._render_sop_action_cards()
        row = tab._action_drafts.payloads()[0]
        assert row["group_name"] == "AUX"
        assert row["description"] == "Preserve this detailed instruction"
        assert tab.actions_table.rowCount() == 1
        assert tab.actions_table.cellWidget(0, tab.COL_GROUP).currentText() == "AUX"
        assert tab.actions_table.cellWidget(0, tab.COL_DESC).text() == "Preserve this detailed instruction"
    finally:
        tab.close()
        tab.deleteLater()


def test_sop_bulk_editor_round_trip_preserves_card_only_fields() -> None:
    _app()
    tab = SOPTab()
    try:
        source = _payload()
        source["enabled"] = False
        source["daily_end_utc"] = "03:00"
        source["daily_conflict_summary"] = "Daily detail"
        source["net_conflict_summary"] = "Net detail"
        tab._action_drafts.replace([source])
        tab._reload_advanced_table_from_drafts()
        tab._sync_drafts_from_advanced_table()
        result = tab._action_drafts.payloads()[0]
        assert result["enabled"] is False
        assert result["daily_end_utc"] == "03:00"
        assert result["daily_conflict_summary"] == "Daily detail"
        assert result["net_conflict_summary"] == "Net detail"
    finally:
        tab.close()
        tab.deleteLater()


def test_sop_advanced_editor_is_collapsed_and_large_cards_are_bounded() -> None:
    _app()
    tab = SOPTab()
    try:
        assert not tab.advanced_table_box.isVisible()
        tab._action_drafts.replace([{**_payload(i), "id": i + 1} for i in range(20)])
        tab._render_sop_action_cards()
        cards = [
            tab.sop_action_cards_layout.itemAt(index).widget()
            for index in range(tab.sop_action_cards_layout.count())
            if isinstance(tab.sop_action_cards_layout.itemAt(index).widget(), QGroupBox)
            and tab.sop_action_cards_layout.itemAt(index).widget().objectName() == "sopActionCard"
        ]
        assert len(cards) == 12
        assert not tab.sop_cards_more_btn.isHidden()
        assert tab.sop_cards_more_btn.text() == "Show actions 13–20"
        tab.sop_cards_more_btn.click()
        _app().processEvents()
        paged_cards = [
            tab.sop_action_cards_layout.itemAt(index).widget()
            for index in range(tab.sop_action_cards_layout.count())
            if isinstance(tab.sop_action_cards_layout.itemAt(index).widget(), QGroupBox)
            and tab.sop_action_cards_layout.itemAt(index).widget().objectName() == "sopActionCard"
        ]
        assert len(paged_cards) == 8
        assert paged_cards[0].title() == "Action 13"
        assert tab.sop_cards_more_btn.text() == "Back to actions 1–12"
    finally:
        tab.close()
        tab.deleteLater()


def test_sop_compact_view_uses_vertical_scroll_and_no_page_horizontal_scroll() -> None:
    _app()
    tab = SOPTab()
    try:
        tab.resize(900, 560)
        tab.show()
        _app().processEvents()
        assert tab.sop_scroll.horizontalScrollBarPolicy().name == "ScrollBarAlwaysOff"
        assert tab.sop_scroll.verticalScrollBar().maximum() >= 0
        assert tab.sop_scroll.widgetResizable()
        assert tab.sop_scroll.horizontalScrollBar().maximum() == 0
    finally:
        tab.close()
        tab.deleteLater()


@pytest.mark.parametrize("size", [(900, 560), (1000, 700)])
@pytest.mark.parametrize("scale", [1.0, 1.25])
@pytest.mark.parametrize("theme_name", ["light", "dark"])
@pytest.mark.parametrize("state", ["empty", "populated", "invalid", "conflict"])
def test_sop_compact_acceptance_matrix_preserves_workflow_and_scrolls_vertically(
    size, scale, theme_name, state
) -> None:
    app = _app()
    apply_app_theme(app, get_theme(theme_name), ui_text_scale=scale)
    tab = SOPTab()
    try:
        if state == "empty":
            rows = []
        elif state == "invalid":
            rows = [{"group_name": "", "action_key": "", "description": "Incomplete action"}]
        elif state == "conflict":
            rows = [{**_payload(), "daily_conflict_summary": "Daily conflict requires review"}]
        else:
            rows = [{**_payload(i), "id": i + 1} for i in range(3)]
        tab._action_drafts.replace(rows)
        tab._reload_advanced_table_from_drafts()
        tab._render_sop_action_cards()
        tab.resize(*size)
        tab.show()
        app.processEvents()

        assert tab.sop_scroll.verticalScrollBar().maximum() >= 0
        assert tab.sop_scroll.horizontalScrollBarPolicy().name == "ScrollBarAlwaysOff"
        # The page itself must not have hidden horizontal overflow.
        assert tab.sop_scroll.horizontalScrollBar().maximum() == 0
        assert tab.add_row_btn.isVisible()
        assert tab.add_row_btn.geometry().bottom() <= tab.sop_scroll.widget().height() + 20

        cards = [
            tab.sop_action_cards_layout.itemAt(i).widget()
            for i in range(tab.sop_action_cards_layout.count())
            if isinstance(tab.sop_action_cards_layout.itemAt(i).widget(), QGroupBox)
            and tab.sop_action_cards_layout.itemAt(i).widget().objectName() == "sopActionCard"
        ]
        assert len(cards) <= 12
        if state == "conflict":
            assert any(
                "Daily conflict requires review" in label.text()
                for card in cards
                for label in card.findChildren(QLabel)
            )
        if state == "populated":
            assert tab.actions_table.rowCount() == 3
            assert tab._action_drafts.payloads()[1]["description"] == "Check in and report status"
    finally:
        tab.close()
        tab.deleteLater()
