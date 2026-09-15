from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QListWidget

from freqinout.gui import fio_spotter_tab as spotter_ui
from freqinout.gui.fio_spotter_tab import FioSpotterTab


class _Settings:
    def __init__(self, **values):
        self.values = values

    def get(self, key: str, default=None):
        return self.values.get(key, default)

    def set(self, _key: str, _value: object) -> None:
        pass

    def save(self) -> None:
        pass


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_activity_tab_and_duplicate_catalog_are_removed(monkeypatch) -> None:
    app = _app()
    calls = []
    monkeypatch.setattr(spotter_ui, "list_spotter_watches", lambda **_kwargs: calls.append(1) or [])
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.resize(900, 560)
        tab.show()
        app.processEvents()
        assert [tab.tabs.tabText(index) for index in range(tab.tabs.count())] == [
            "Watches", "Expect", "Access Policies", "Forms", "Imports",
        ]
        assert not hasattr(tab, "activity_table")
        assert not hasattr(spotter_ui, "list_spotter_activity")
        assert calls == []
    finally:
        tab.deleteLater()


def test_spotter_actions_use_shared_theme_roles() -> None:
    app = _app()
    tab = FioSpotterTab(settings=_Settings(ui_theme="dark"))
    try:
        tab.tabs.setCurrentIndex(0)
        app.processEvents()
        assert "#4C9BD3" in tab.watch_save.styleSheet()
        assert "#E05252" in tab.watch_delete.styleSheet()
        assert not hasattr(tab, "watch_toggle")
    finally:
        tab.deleteLater()


def test_spotter_sections_are_left_aligned_wrapping_shared_theme_chips(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_spotter_watches", lambda **_kwargs: [])
    tab = FioSpotterTab(settings=_Settings(ui_theme="dark"))
    try:
        tab.resize(1200, 700)
        tab.show()
        app.processEvents()
        selector = tab.tab_selector
        assert isinstance(selector, QListWidget)
        assert tab.tabs.tabBar().isHidden()
        assert selector.flow() == QListWidget.LeftToRight
        assert selector.isWrapping()
        assert selector.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert selector.verticalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert selector.visualItemRect(selector.item(0)).left() <= selector.spacing() + 8
        assert "#4C9BD3" in selector.styleSheet()
        assert selector.item(0).sizeHint().height() >= selector.fontMetrics().lineSpacing()

        selector.setFocus()
        QTest.keyClick(selector, Qt.Key_Right)
        app.processEvents()
        assert selector.currentRow() == 1
        assert tab.tabs.currentIndex() == 1

        tab.tabs.setCurrentIndex(0)
        app.processEvents()
        assert selector.currentRow() == 0

        tab.settings.values["ui_theme"] = "light"
        tab.apply_theme()
        assert "#2E6F9E" in selector.styleSheet()
        assert "#DDE1E6" in selector.styleSheet()

        selector.setFixedWidth(320)
        tab._refresh_tab_selector_geometry()
        app.processEvents()
        assert selector.visualItemRect(selector.item(4)).top() > selector.visualItemRect(selector.item(0)).top()
    finally:
        tab.deleteLater()
