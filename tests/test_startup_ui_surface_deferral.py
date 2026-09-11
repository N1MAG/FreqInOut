"""Startup surface deferral contracts for the first visible FIO shell."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_controlfreq_deferred_constructor_does_not_refresh_or_backfill(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    _app()
    from freqinout.gui.controlfreq_tab import ControlFreqTab

    monkeypatch.setattr(
        ControlFreqTab,
        "_refresh_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inline dashboard refresh")),
    )
    monkeypatch.setattr(
        ControlFreqTab,
        "_schedule_focus_index_backfill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inline focus backfill")),
    )

    tab = ControlFreqTab(defer_initial_refresh=True)
    try:
        assert tab._last_refresh_ts == 0.0
        assert tab._active is False
    finally:
        tab.deleteLater()


def test_sop_deferred_constructor_waits_for_activation_before_data_projection(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    _app()
    from freqinout.gui.sop_tab import SOPTab

    calls: list[str] = []
    monkeypatch.setattr(SOPTab, "_refresh_reference_data", lambda self: calls.append("reference"))
    monkeypatch.setattr(SOPTab, "_reload_profiles", lambda self, **_kwargs: calls.append("profiles"))
    monkeypatch.setattr(SOPTab, "refresh_upcoming", lambda self: calls.append("upcoming"))
    monkeypatch.setattr(SOPTab, "_refresh_sop_workbench_contracts", lambda self: calls.append("workbench"))
    monkeypatch.setattr(SOPTab, "_schedule_layer_sync_refresh", lambda self: calls.append("layers"))

    tab = SOPTab(defer_initial_load=True)
    try:
        assert calls == []
        tab._active = True
        tab._ensure_initial_data_projection()
        assert calls[:4] == ["reference", "profiles", "upcoming", "workbench"]
        assert "layers" in calls
        assert tab._initial_data_loaded is True
    finally:
        tab.deleteLater()


def test_settings_deferred_constructor_never_autosaves_blank_controls(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    _app()
    from freqinout.gui.settings_tab import SettingsTab

    calls: list[str] = []
    monkeypatch.setattr(SettingsTab, "_load_settings", lambda self: calls.append("load"))
    monkeypatch.setattr(SettingsTab, "_save_settings", lambda self, **_kwargs: calls.append("save"))

    tab = SettingsTab(defer_initial_load=True)
    try:
        assert calls == []
        tab._save_settings_quiet()
        assert calls == []
        tab._active = True
        tab._ensure_initial_settings_loaded()
        assert calls == ["load"]
        assert tab._initial_settings_loaded is True
    finally:
        tab.deleteLater()


def test_main_shell_opts_into_deferred_non_visible_projections() -> None:
    source = open("freqinout/gui/main_window.py", encoding="utf-8").read()

    assert "defer_initial_load=True" in source
    assert "defer_initial_refresh=True" in source
    assert "local_net_profiles_changed.connect(self.sop_tab.on_local_net_profiles_updated)" in source
    assert "self.sop_tab.sop_data_changed.connect(self._on_sop_data_changed)" in source


def test_station_command_plan_render_cache_never_reads_sqlite() -> None:
    from inspect import getsource

    from freqinout.gui.main_window import MainWindow

    source = getsource(MainWindow._station_command_plan_cache)
    assert "list_effective_assigned_plans" not in source
    assert "list_frequency_plans" not in source
    assert "multi_radio_store" not in source
    assert "_station_command_active_schedule_lanes" in source
