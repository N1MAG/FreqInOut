"""Bounded UIA-3 probes for the FLDigi/SSB NCS workspaces."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QBoxLayout

from freqinout.gui.fldigi_macro_mapping_dialog import FldigiMacroMappingDialog
from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab


class _Settings:
    config_dir = "/tmp"

    def all(self):
        return {}

    def get(self, _key: str, default=None):
        return default

    def set(self, _key: str, _value: object) -> None:
        pass

    def save(self) -> None:
        pass


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _fldigi_tab(monkeypatch) -> FldigiNetControlTab:
    _app()
    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)
    tab = FldigiNetControlTab()
    tab.settings = _Settings()
    return tab


def test_fldigi_ncs_compact_reflows_dense_action_rows_and_preserves_draft(monkeypatch) -> None:
    tab = _fldigi_tab(monkeypatch)
    try:
        tab.resize(900, 560)
        tab.show()
        tab.macro_profile_edit.setText("/tmp/net.mdf")
        tab._refresh_responsive_geometry()
        _app().processEvents()
        assert tab._ncs_scroll_area.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab._ncs_scroll_area.horizontalScrollBar().maximum() == 0
        assert tab._roster_primary_actions_layout.direction() == QBoxLayout.TopToBottom
        assert tab._session_context_row.direction() == QBoxLayout.TopToBottom
        assert tab._macro_path_row.direction() == QBoxLayout.TopToBottom
        assert tab.macro_profile_edit.text() == "/tmp/net.mdf"
        assert tab.roster_table.minimumHeight() == 0

        tab.resize(1920, 1080)
        tab._refresh_responsive_geometry()
        _app().processEvents()
        assert tab._roster_primary_actions_layout.direction() == QBoxLayout.LeftToRight
        assert tab.macro_profile_edit.text() == "/tmp/net.mdf"
    finally:
        tab.close()
        tab.deleteLater()


def test_macro_mapping_dialog_fits_compact_view_without_fixed_floor(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "freqinout.gui.fldigi_macro_mapping_dialog.FldigiMacroProfileStore.scan_profile",
        lambda _store, path: {
            "profile_path": path,
            "profile_name": "",
            "detected_macros": [],
            "mappings": [],
        },
    )
    dialog = FldigiMacroMappingDialog(_Settings(), str(tmp_path / "missing.mdf"))
    try:
        assert dialog.minimumWidth() == 0
        assert dialog.minimumHeight() == 0
        dialog.resize(900, 560)
        dialog.show()
        _app().processEvents()
        assert dialog._controls_layout.direction() == QBoxLayout.TopToBottom
        assert dialog.table.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert dialog.table.width() > 0

        dialog.resize(1200, 700)
        dialog._refresh_responsive_geometry()
        _app().processEvents()
        assert dialog._controls_layout.direction() == QBoxLayout.LeftToRight
    finally:
        dialog.close()
        dialog.deleteLater()
