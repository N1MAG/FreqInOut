from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPoint, Qt
from PySide6.QtWidgets import QApplication

from freqinout.gui.station_bbs_tab import StationBbsTab
from freqinout.gui.theme import apply_app_theme, get_theme
from tests.test_station_bbs_tab import _seed_catalog, _wait_for_catalog


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _show_tab(tab: StationBbsTab, size: tuple[int, int], theme: str, scale: float) -> None:
    apply_app_theme(_app(), get_theme(theme), ui_text_scale=scale)
    tab.resize(*size)
    tab.show()
    _app().processEvents()


def _assert_reachable(widget, host) -> None:
    assert widget.isVisible(), f"{widget.objectName() or widget.text()} is hidden"
    origin = widget.mapTo(host, QPoint(0, 0))
    assert origin.y() + widget.height() <= host.height() + 2
    assert origin.x() + widget.width() <= host.width() + 2


@pytest.mark.parametrize("size", [(1280, 720), (1000, 700), (900, 560)])
@pytest.mark.parametrize("scale", [1.0, 1.25])
@pytest.mark.parametrize("theme", ["light", "dark"])
def test_bbs_radio_service_matrix_is_concise_and_reachable(tmp_path, size, scale, theme):
    app = _app()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    _wait_for_catalog(tab, app)
    try:
        tab.service_tabs.setCurrentWidget(tab.radio_service_page)
        _show_tab(tab, size, theme, scale)
        assert tab.radio_service_page.isVisible()
        assert tab.radio_service_selector.isVisible()
        assert not hasattr(tab, "radio_service_table")
        assert tab.radio_editor_scroll.horizontalScrollBar().maximum() == 0
        assert tab.radio_editor_scroll.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.radio_service_save_btn.isEnabled() is False
        # Radio Settings remains the recovery route when no usable VarAC
        # profile exists; only the BBS editor/save surface is disabled.
        assert tab.radio_settings_btn.isEnabled() is True
        assert tab.radio_service_save_btn.text() == "Save Radio Service"
        assert tab.radio_native_paths_label.wordWrap()
    finally:
        tab.close()
        tab.deleteLater()
        apply_app_theme(app, get_theme("light"), ui_text_scale=1.0)
        app.processEvents()


def test_bbs_location_editor_is_collapsed_and_add_cancel_is_reversible(tmp_path):
    app = _app()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    _wait_for_catalog(tab, app)
    try:
        tab.service_tabs.setCurrentWidget(tab.locations_page)
        tab.resize(900, 560)
        tab.show()
        app.processEvents()
        assert not tab.location_editor.isVisible()
        assert not tab.location_edit_btn.isChecked()
        tab.location_add_btn.click()
        app.processEvents()
        assert tab.location_editor.isVisible()
        cancel = getattr(tab, "location_cancel_btn", None)
        assert cancel is not None, "location editor needs an explicit Cancel action"
        assert cancel.isVisible()
        cancel.click()
        app.processEvents()
        assert not tab.location_editor.isVisible()
        assert not tab.location_edit_btn.isChecked()
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_bbs_long_labels_paths_have_no_page_overflow_and_one_summary(tmp_path):
    app = _app()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    _wait_for_catalog(tab, app)
    try:
        tab.service_tabs.setCurrentWidget(tab.locations_page)
        tab.resize(900, 560)
        tab.show()
        app.processEvents()
        tab.location_edit_btn.click()
        tab.location_name_edit.setText("A very long station location name " * 5)
        tab.location_source_edit.setText("/" + ("deeply-nested-folder/" * 18) + "source.txt")
        app.processEvents()
        # The control surface must not acquire page-level horizontal overflow.
        # Artifact tables may retain their own intentional internal scrollbar.
        assert tab.location_context_scroll.horizontalScrollBar().maximum() == 0
        assert tab.summary_label.text().count("Managed BBS catalog") <= 1
        assert tab.location_context_label.wordWrap()
        for button in (tab.location_add_btn, tab.location_edit_btn):
            _assert_reachable(button, tab.locations_page)
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_bbs_resize_and_theme_changes_are_geometry_only(monkeypatch, tmp_path):
    app = _app()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    _wait_for_catalog(tab, app)
    calls = {"refresh": 0, "radio": 0}
    original_refresh = tab.refresh_catalog
    original_radio = tab._refresh_radio_services

    def refresh(*args, **kwargs):
        calls["refresh"] += 1
        return original_refresh(*args, **kwargs)

    def radio(*args, **kwargs):
        calls["radio"] += 1
        return original_radio(*args, **kwargs)

    monkeypatch.setattr(tab, "refresh_catalog", refresh)
    monkeypatch.setattr(tab, "_refresh_radio_services", radio)
    try:
        for size in ((1280, 720), (1000, 700), (900, 560)):
            tab.resize(*size)
            apply_app_theme(app, get_theme("dark" if size[0] == 900 else "light"), ui_text_scale=1.25)
            tab.apply_theme()
            app.processEvents()
        assert calls == {"refresh": 0, "radio": 0}
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()
