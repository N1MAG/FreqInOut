from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication, QAbstractScrollArea

from freqinout.gui.resource_picker import ResourcePicker
from freqinout.gui.resources_tab import ResourceImportExportView


class _EmptyStore:
    def list_frequencies(self, **_kwargs):
        return []

    def list_net_entries(self, **_kwargs):
        return []


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_resources_surfaces_large_text_have_font_derived_geometry_and_no_form_horizontal_scroll() -> None:
    app = _app()
    prior_font = QFont(app.font())
    large_font = QFont(prior_font)
    large_font.setPointSize(max(20, prior_font.pointSize() + 8))
    app.setFont(large_font)
    store = _EmptyStore()
    try:
        transfer = ResourceImportExportView(store)
        picker = ResourcePicker(store)
        for view in (transfer, picker):
            view.resize(900, 560)
            view.show()
        app.processEvents()

        assert transfer.content_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert transfer.diagnostics.minimumHeight() >= transfer.diagnostics.fontMetrics().lineSpacing() * 4
        assert picker.table.verticalHeader().defaultSectionSize() >= picker.table.fontMetrics().lineSpacing()
        assert picker.search_row.direction().name in {"LeftToRight", "TopToBottom"}
        assert all(
            scroll.horizontalScrollBar().maximum() == 0
            for view in (transfer, picker)
            for scroll in view.findChildren(QAbstractScrollArea)
        )
    finally:
        app.setFont(prior_font)
