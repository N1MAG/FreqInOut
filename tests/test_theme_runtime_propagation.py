"""Focused live-theme ownership regressions for Settings and Map surfaces."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from freqinout.gui.settings_tab import SettingsTab
from freqinout.gui.theme import get_theme


ROOT = Path(__file__).resolve().parents[1]


class _Settings:
    def get(self, key: str, default: object = None) -> object:
        return "dark" if key == "ui_theme" else default


def test_settings_theme_refresh_isolates_optional_runtime_painter_failures() -> None:
    """One unavailable status painter cannot leave explicit controls stale."""
    events: list[str] = []

    def _record(name: str):
        return lambda *_args, **_kwargs: events.append(name)

    def _broken_status(*_args, **_kwargs) -> None:
        events.append("status")
        raise RuntimeError("status source unavailable during theme switch")

    harness = SimpleNamespace(
        settings=_Settings(),
        sections_nav_list=object(),
        loading_label=None,
        _last_running_status_snapshot={"JS8Call": {"state": "unknown"}},
        _status_service=SimpleNamespace(software_status_snapshot=lambda: {}),
        _settings_dirty=False,
        _apply_sections_nav_style=_record("nav surface"),
        _refresh_settings_nav_button_styles=_record("nav buttons"),
        _refresh_section_nav_health=_record("nav health"),
        _paint_running_status_snapshot=_broken_status,
        _update_launch_selected_state=_record("launch"),
        _update_device_profile_action_buttons=_record("profiles"),
        _update_op_group_action_buttons=_record("hf groups"),
        _update_local_net_action_buttons=_record("local groups"),
        _set_save_button_state=_record("save"),
        _context_help_buttons=[],
        _refresh_contextual_autofill_buttons=_record("autofill"),
        _update_enforcement_visibility=_record("enforcement"),
        _update_logging_actions_layout=_record("logging layout"),
        _apply_accessibility_width_guards=_record("widths"),
    )

    SettingsTab.apply_theme(harness, get_theme("light"))

    assert events[:4] == ["nav surface", "nav buttons", "nav health", "status"]
    assert events[-4:] == ["autofill", "enforcement", "logging layout", "widths"]
    assert "launch" in events
    assert "save" in events


def test_main_owns_map_and_settings_theme_snapshots_explicitly() -> None:
    """Live theme changes cannot be re-resolved from a stale child cache."""
    main_source = (ROOT / "freqinout/gui/main_window.py").read_text(encoding="utf-8")
    map_source = (ROOT / "freqinout/gui/map_window.py").read_text(encoding="utf-8")
    tab_source = (ROOT / "freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "widget.apply_theme(theme)" in main_source
    assert "map_window.apply_theme(theme)" in main_source
    assert "def apply_theme(self, theme: Mapping[str, object])" in map_source
    assert "tab.apply_theme(colors)" in map_source
    assert 'shared_settings = getattr(application_host, "settings", None)' in tab_source
