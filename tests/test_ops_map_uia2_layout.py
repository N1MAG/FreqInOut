from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication, QFrame, QGridLayout, QLabel, QPushButton, QTableWidget, QWidget

from freqinout.gui.controlfreq_tab import ControlFreqTab
from freqinout.gui.stations_map_tab import StationsMapTab


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_ops_table_defaults_follow_large_text_metrics_for_rows_and_header() -> None:
    _app()
    table = QTableWidget(2, 2)
    font = QFont(table.font())
    font.setPointSize(max(22, font.pointSize() + 10))
    table.setFont(font)

    ControlFreqTab._setup_table_defaults(table)

    assert table.verticalHeader().defaultSectionSize() >= table.fontMetrics().height() + 8
    assert table.horizontalHeader().minimumHeight() >= table.horizontalHeader().fontMetrics().height() + 10


def test_map_filter_reflow_keeps_search_and_clear_actions_in_layout_at_compact_width() -> None:
    _app()
    tab = StationsMapTab.__new__(StationsMapTab)
    bar = QFrame()
    grid = QGridLayout(bar)
    fields = tuple(QWidget(bar) for _ in range(7))
    for field in fields:
        field.setMinimumWidth(280)
    search = QWidget(bar)
    clear = QPushButton("Clear Filters", bar)
    clear_layers = QPushButton("Clear Layers", bar)
    reachable = QLabel("", bar)
    tab._map_filter_bar = bar
    tab._map_filter_grid = grid
    tab._map_filter_fields = fields
    tab._map_search_field = search
    tab._map_clear_filters_button = clear
    tab._map_clear_layers_button = clear_layers
    tab._now_reachable_label = reachable

    bar.resize(900, 200)
    StationsMapTab._reflow_map_filter_bar(tab)
    assert grid.indexOf(search) >= 0
    assert grid.indexOf(clear) >= 0
    assert grid.indexOf(clear_layers) >= 0
    assert grid.columnCount() == 2

    bar.resize(500, 400)
    StationsMapTab._reflow_map_filter_bar(tab)
    assert grid.getItemPosition(grid.indexOf(search))[1] == 0
    assert grid.getItemPosition(grid.indexOf(clear))[1] == 0
    assert grid.getItemPosition(grid.indexOf(clear_layers))[1] == 0


def test_ops_uses_shared_label_roles_without_raw_font_or_color_styles() -> None:
    source = (Path(__file__).resolve().parents[1] / "freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")

    assert "label_style(\"muted", source
    assert "label_style(\"danger" in source
    assert '"font-size: 12px; font-weight: 600; border-radius: 6px;' not in source
    assert '"color: #5b6875' not in source
