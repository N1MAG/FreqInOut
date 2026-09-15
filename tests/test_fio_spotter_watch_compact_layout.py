from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QScrollArea, QSplitter, QWidget

from freqinout.gui import fio_spotter_tab as spotter_ui
from freqinout.gui.fio_spotter_tab import FioSpotterTab


class _Settings:
    def get(self, _key: str, default=None):
        return default

    def set(self, _key: str, _value: object) -> None:
        pass

    def save(self) -> None:
        pass


def _app() -> QApplication:
    app = QApplication.instance()
    return app or QApplication([])


def test_watches_make_table_dominant_and_fold_primary_controls(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_spotter_watches", lambda **_kwargs: [])
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.show()
        tab.resize(1400, 900)
        tab.tabs.setCurrentIndex(0)
        app.processEvents()

        split = tab.findChild(QSplitter, "fioSpotterWatchesSplit")
        assert split is not None
        assert split.orientation() == Qt.Horizontal
        sizes = split.sizes()
        assert sizes[0] > sizes[1]
        actions = tab.watch_enabled.parent()
        assert actions.height() <= actions.sizeHint().height() + 2
        assert tab.watch_name.geometry().top() - actions.geometry().bottom() <= 12
        primary = tab.findChild(QWidget, "fioSpotterWatchPrimaryCondition")
        assert primary is not None
        assert tab.watch_kind.parent() is primary
        assert tab.watch_pattern.parent() is tab.watch_kind.parent()
        assert tab.watch_mode.parent() is tab.watch_kind.parent()
        assert tab.watch_enabled.parent().objectName() == "fioSpotterWatchEditorActions"
    finally:
        tab.deleteLater()


def test_watch_compact_controls_keep_payload_and_second_condition_behavior(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_spotter_watches", lambda **_kwargs: [])
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.tabs.setCurrentIndex(0)
        app.processEvents()
        tab.watch_name.setText("Two conditions")
        tab.watch_kind.setCurrentText("group")
        tab.watch_pattern.setText("SNR")
        tab.watch_mode.setCurrentText("exact")
        tab.watch_secondary_kind.setCurrentIndex(tab.watch_secondary_kind.findData("topic"))
        tab.watch_secondary_pattern.setText("Fire")
        tab.watch_secondary_mode.setCurrentText("contains")
        tab.watch_enabled.setChecked(False)

        values = tab._watch_values()
        assert values["enabled"] is False
        assert values["match_mode"] == "exact"
        assert values["criteria"] == [
            {"kind": "group", "pattern": "SNR", "match_mode": "exact"},
            {"kind": "topic", "pattern": "Fire", "match_mode": "contains"},
        ]

        tab._clear_watch()
        assert tab.watch_enabled.isChecked()
        assert tab.watch_secondary_kind.currentData() == ""
        assert tab.watch_secondary_pattern.text() == ""
    finally:
        tab.deleteLater()


def test_watches_switch_to_vertical_splitter_at_compact_width(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_spotter_watches", lambda **_kwargs: [])
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.show()
        tab.resize(800, 900)
        tab.tabs.setCurrentIndex(0)
        app.processEvents()

        split = tab.findChild(QSplitter, "fioSpotterWatchesSplit")
        assert split is not None
        assert split.orientation() == Qt.Vertical
        assert all(size > 0 for size in split.sizes())
        assert split.widget(1).width() >= 700
    finally:
        tab.deleteLater()


def test_watches_medium_width_stacks_without_page_overflow_or_action_gap(monkeypatch) -> None:
    """The screenshot-sized window keeps the editor usable and top-packed."""
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_spotter_watches", lambda **_kwargs: [])
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.show()
        tab.resize(1104, 768)
        tab.tabs.setCurrentIndex(0)
        app.processEvents()

        split = tab.findChild(QSplitter, "fioSpotterWatchesSplit")
        assert split is not None
        assert split.orientation() == Qt.Vertical
        actions = tab.watch_enabled.parent()
        assert actions.height() <= actions.sizeHint().height() + 2
        assert tab.watch_name.geometry().top() - actions.geometry().bottom() <= 12

        scroll = tab.tabs.currentWidget().findChild(QScrollArea)
        assert scroll is not None
        assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert not scroll.horizontalScrollBar().isVisible()
        assert scroll.widget().width() <= scroll.viewport().width()
    finally:
        tab.deleteLater()
