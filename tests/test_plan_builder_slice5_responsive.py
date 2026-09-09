from __future__ import annotations

from pathlib import Path

import pytest


PLANNER = Path("freqinout/gui/freq_planner_tab.py")


def test_plan_builder_declares_responsive_compact_layout_seam() -> None:
    text = PLANNER.read_text(encoding="utf-8")

    assert "def _apply_responsive_layout" in text
    assert "mode = \"compact\" if width < 1100 or height < 700 else \"wide\"" in text
    assert "self.plan_select_layout = plan_select_row" in text
    assert "self.source_workspace_layout = source_workspace" in text
    assert "self._apply_responsive_layout(force=True)" in text
    assert "self._apply_responsive_layout()" in text


def test_plan_builder_compact_layout_reflows_every_plan_and_source_action() -> None:
    text = PLANNER.read_text(encoding="utf-8")
    compact = text[text.index('if mode == "compact":'):text.index("        else:", text.index('if mode == "compact":'))]

    for marker in (
        "self.frequency_plan_combo",
        "self.new_plan_btn",
        "self.save_plan_btn",
        "self.rename_plan_btn",
        "self.delete_plan_btn",
        "self.assign_plan_btn",
        "self.hf_daily_source_combo",
        "self.hf_net_source_combo",
        "self.sop_plan_source_combo",
        "self.save_sop_plan_btn",
        "self.build_sop_layer_btn",
    ):
        assert marker in compact


def test_plan_builder_detail_surfaces_are_progressively_bounded() -> None:
    text = PLANNER.read_text(encoding="utf-8")

    assert "self.frequency_plan_action_hint_label.setWordWrap(True)" in text
    assert "self._update_responsive_height_bounds()" in text
    assert "max(48, line * 3 + 8)" in text
    assert "max(76, line * 3 + 18)" in text
    assert "max(190, line * 7 + 36)" in text
    assert "self.rf_guard_review_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in text
    assert "MAX_PROJECTION_TABLE_ROWS = 500" in text
    assert "MAX_RF_GUARD_ROWS = 200" in text
    assert "rows[: self.MAX_PROJECTION_TABLE_ROWS]" in text
    assert "all_rows[: self.MAX_RF_GUARD_ROWS]" in text


@pytest.mark.parametrize("width,height,expected", [(900, 560, "compact"), (1000, 700, "compact"), (1920, 1080, "wide")])
def test_plan_builder_runtime_mode_matrix(width: int, height: int, expected: str) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.freq_planner_tab import FreqPlannerTab

    app = QApplication.instance() or QApplication([])
    tab = FreqPlannerTab()
    tab.resize(width, height)
    tab.show()
    app.processEvents()
    assert tab._responsive_layout_mode == expected
    assert tab.hf_daily_source_combo.isVisible()
    assert tab.hf_net_source_combo.isVisible()
    assert tab.sop_plan_source_combo.isVisible()
    assert tab.save_plan_btn.isVisible()
    assert tab.build_sop_layer_btn.isVisible()
    tab.close()
