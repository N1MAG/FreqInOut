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


def test_map_legend_uses_ui_text_scale() -> None:
    text = _read("freqinout/gui/stations_map_tab.py")
    assert "resolve_ui_text_scale" in text
    assert "label_font_px = max(10.0, 10.0 * float(ui_text_scale))" in text
    assert "legend_font_px = max(12.0, 12.0 * float(ui_text_scale))" in text


def test_startup_applies_saved_text_size_before_main_window() -> None:
    text = _read("freqinout/main.py")
    assert "def _apply_startup_theme(app: QApplication) -> None:" in text
    assert "apply_app_theme(app, resolve_theme(settings), ui_text_scale=resolve_ui_text_scale(settings))" in text
    assert "_apply_startup_theme(app)\n    app_icon = _load_app_icon()" in text
