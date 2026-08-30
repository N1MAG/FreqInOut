from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_theme_adds_qtoolbutton_baseline_and_font_helper() -> None:
    text = _read("freqinout/gui/theme.py")
    assert "def font_css(font: QFont) -> str:" in text
    assert '"QPushButton, QToolButton {' in text
    assert '"QPushButton:hover, QToolButton:hover {' in text
    assert '"QPushButton:disabled, QToolButton:disabled {' in text


def test_theme_has_global_large_text_accessibility_guards() -> None:
    text = _read("freqinout/gui/theme.py")

    assert "def control_height_for_font" in text
    assert "def button_height_for_font" in text
    assert "def single_line_label_height" in text
    assert "def apply_text_size_accessibility_guards" in text
    assert "fio_text_size_guard_opt_out" in text
    assert "apply_text_size_accessibility_guards(app, include_widths=False)" in text


def test_high_use_tabs_call_shared_large_text_guard() -> None:
    messages = _read("freqinout/gui/message_viewer_tab.py")
    settings = _read("freqinout/gui/settings_tab.py")
    controlfreq = _read("freqinout/gui/controlfreq_tab.py")
    fldigi_net = _read("freqinout/gui/fldigi_net_control_tab.py")
    sop = _read("freqinout/gui/sop_tab.py")
    main_window = _read("freqinout/gui/main_window.py")

    assert "apply_text_size_accessibility_guards(self, include_widths=False)" in messages
    assert "self._messages_text_size_guard_signature" in messages
    assert "guard_signature != getattr(self, \"_messages_text_size_guard_signature\", None)" in messages
    assert "button_height_for_font(self.compose_mode_selector" in messages
    assert "button_height_for_font(chip)" in messages
    assert "apply_text_size_accessibility_guards(self, include_widths=False)" in settings
    assert "device_selector_h = control_height_for_font" in settings
    assert "step_frame_h = control_height_for_font" in settings
    assert "apply_text_size_accessibility_guards(self, include_widths=False)" in controlfreq
    assert "self.macro_profile_details_btn.setMinimumHeight(button_height_for_font" in fldigi_net
    assert "apply_text_size_accessibility_guards(self, include_widths=False)" in sop
    assert "summary_h = control_height_for_font" in main_window


def test_large_text_height_audit_tool_is_available() -> None:
    text = _read("tools/audit_ui_text_size_heights.py")

    assert "HEIGHT_CALL_RE" in text
    assert "TEXT_WIDGET_HINTS" in text
    assert "suspicious small text-control height call" in text


def test_map_legend_uses_ui_text_scale() -> None:
    text = _read("freqinout/gui/stations_map_tab.py")
    assert "resolve_ui_text_scale" in text
    assert "label_font_px = max(10.0, 10.0 * float(ui_text_scale))" in text
    assert "legend_font_px = max(12.0, 12.0 * float(ui_text_scale))" in text


def test_startup_applies_saved_text_size_before_main_window() -> None:
    text = _read("freqinout/main.py")
    assert "def _apply_startup_theme(app: QApplication) -> None:" in text
    assert "apply_app_theme(app, resolve_theme(settings), ui_text_scale=resolve_ui_text_scale(settings))" in text
    assert "_apply_startup_theme(app)\n    _emit_startup_stage(\"apply_startup_theme\"" in text
    assert "app_icon = _load_app_icon()" in text
