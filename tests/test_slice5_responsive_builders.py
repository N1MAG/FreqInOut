from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QScrollArea

from freqinout.gui.freq_planner_tab import FreqPlannerTab
from freqinout.gui.theme import apply_app_theme, get_theme


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


@pytest.mark.parametrize("size", [(1920, 1080), (1000, 700), (900, 560)])
@pytest.mark.parametrize("scale", [1.0, 1.25])
@pytest.mark.parametrize("theme_name", ["light", "dark"])
def test_plan_builder_matrix_keeps_table_and_responsive_surfaces_reachable(size, scale, theme_name) -> None:
    app = _app()
    apply_app_theme(app, get_theme(theme_name), ui_text_scale=scale)
    tab = FreqPlannerTab()
    try:
        tab.resize(*size)
        tab.show()
        app.processEvents()
        assert tab.table.isVisible()
        assert tab.table.viewport().height() > 0
        assert tab.plan_ingredients_scroll.widgetResizable()
        assert tab.plan_review_toolbar_scroll.widgetResizable()
        assert tab.plan_ingredients_scroll.verticalScrollBarPolicy().name == "ScrollBarAlwaysOff"
        assert tab.plan_review_toolbar_scroll.verticalScrollBarPolicy().name == "ScrollBarAlwaysOff"
        assert tab.frequency_plan_action_hint_label.wordWrap()
        assert tab._responsive_layout_mode == ("compact" if size[0] < 1100 or size[1] < 700 else "wide")
        tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("operational"))
        app.processEvents()
        assert tab.operational_day_combo.isVisible()
        tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("effective"))
        app.processEvents()
        assert not tab.operational_day_combo.isVisible()
        assert not tab.radio_window_radio_combo.isVisible()
    finally:
        tab.close()
        tab.deleteLater()


def test_plan_builder_radio_view_only_exposes_radio_filter_and_keeps_rf_panel_bounded() -> None:
    _app()
    tab = FreqPlannerTab()
    try:
        tab.resize(900, 560)
        tab.show()
        tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("radio"))
        _app().processEvents()
        assert tab.radio_window_radio_combo.isVisible()
        assert not tab.operational_day_combo.isVisible()
        assert tab.rf_guard_review_table.maximumHeight() <= 190
        assert tab.selected_window_card.maximumHeight() <= 76
    finally:
        tab.close()
        tab.deleteLater()
