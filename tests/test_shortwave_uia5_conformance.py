from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication

from freqinout.gui.hf_net_subscription_dialog import HfNetSubscriptionDialog
from freqinout.gui.shortwave_tab import (
    ShortwaveDataSourcesView,
    ShortwaveExploreView,
    ShortwaveListeningView,
)


class _EmptyCatalog:
    def list_net_entries(self, **_kwargs):
        return []

    def list_sessions(self, **_kwargs):
        return []

    def get_frequency(self, _key):
        return None


class _EmptySettings:
    def get(self, _key, default=None):
        return default


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_shortwave_and_subscription_surfaces_follow_large_text_geometry(tmp_path: Path) -> None:
    app = _app()
    prior_font = QFont(app.font())
    large_font = QFont(prior_font)
    large_font.setPointSize(max(20, prior_font.pointSize() + 8))
    app.setFont(large_font)
    views = [
        ShortwaveExploreView(tmp_path / "shortwave.db"),
        ShortwaveListeningView(tmp_path / "shortwave.db"),
        ShortwaveDataSourcesView(tmp_path / "shortwave.db"),
        HfNetSubscriptionDialog(_EmptyCatalog(), _EmptySettings()),
    ]
    try:
        for view in views:
            view.resize(900, 560)
            view.show()
        app.processEvents()

        explore = views[0]
        listening = views[1]
        sources = views[2]
        subscription = views[3]
        assert explore.detail_scroll.horizontalScrollBarPolicy().name == "ScrollBarAlwaysOff"
        assert listening.editor_scroll.horizontalScrollBarPolicy().name == "ScrollBarAlwaysOff"
        assert sources.preview_text.minimumHeight() >= sources.preview_text.fontMetrics().lineSpacing() * 4
        assert subscription.entry_table.verticalHeader().defaultSectionSize() >= subscription.entry_table.fontMetrics().lineSpacing()
    finally:
        for view in views:
            view.close()
            view.deleteLater()
        app.setFont(prior_font)
        app.processEvents()
