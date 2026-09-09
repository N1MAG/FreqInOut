from __future__ import annotations

import pytest


def test_sop_builder_cards_are_primary_and_save_from_drafts(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))

    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.sop_tab import SOPTab

    app = QApplication.instance() or QApplication([])
    tab = SOPTab()
    try:
        tab.resize(900, 560)
        tab.show()
        app.processEvents()

        assert tab.sop_scroll.widget() is tab.sop_scroll_content
        assert tab.sop_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.advanced_table_box.isVisible() is False

        tab.category_combo.setCurrentIndex(1)  # Local Comms SOP avoids HF-group fixture setup.
        tab.name_edit.setText("Draft-backed local SOP")
        tab._action_drafts.replace(
            [
                {
                    "group_name": "COUNTY",
                    "software": "VHF",
                    "mode": "FM",
                    "action_key": "local_checkin",
                    "action_label": "Check-in",
                    "daily_start_utc": "12:00",
                    "duration_minutes": 30,
                    "interval_minutes": 60,
                    "description": "from cards",
                }
            ]
        )
        tab._render_sop_action_cards()
        assert tab.sop_action_cards_layout.count() >= 2  # one card plus the trailing stretch

        # The legacy table deliberately contains different text: Save must use
        # the draft collection rather than scrape this hidden compatibility view.
        tab._action_drafts.update(0, "description", "draft source of truth")
        _profile, actions, _layer = tab._collect_profile_payload()
        assert actions[0]["description"] == "draft source of truth"
        assert actions[0]["software"] == "Local Net"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_sop_builder_rejects_invalid_card_start_time(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))

    from PySide6.QtWidgets import QApplication

    from freqinout.gui.sop_tab import SOPTab

    app = QApplication.instance() or QApplication([])
    tab = SOPTab()
    try:
        tab.category_combo.setCurrentIndex(1)
        tab.name_edit.setText("Invalid time")
        tab._action_drafts.replace(
            [{
                "group_name": "COUNTY",
                "software": "VHF",
                "action_key": "local_checkin",
                "daily_start_utc": "not-a-time",
                "duration_minutes": 30,
            }]
        )
        with pytest.raises(ValueError, match="Daily Start must be HH:MM"):
            tab._collect_profile_payload()
    finally:
        tab.deleteLater()
        app.processEvents()
