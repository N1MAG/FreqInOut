from __future__ import annotations

from typing import Dict, Tuple

from PySide6.QtGui import QColor, QFont, QPalette


THEMES: Dict[str, Dict[str, str]] = {
    "light": {
        "bg": "#E6E8EA",
        "surface": "#F0F2F4",
        "surface_alt": "#DDE1E6",
        "text": "#1C1F21",
        "text_muted": "#5B6570",
        "border": "#D3D7DD",
        "accent": "#2E6F9E",
        "accent_hover": "#3B84B4",
        "accent_active": "#1F5A83",
        "success": "#2E7D32",
        "warning": "#C99700",
        "danger": "#C62828",
        "info": "#1565C0",
        "focus": "#7FB5FF",
    },
    "dark": {
        "bg": "#0F1216",
        "surface": "#171B21",
        "surface_alt": "#202632",
        "text": "#E7EBF0",
        "text_muted": "#A3ACB8",
        "border": "#2A313A",
        "accent": "#4C9BD3",
        "accent_hover": "#60A9DA",
        "accent_active": "#3A86BE",
        "success": "#4CAF50",
        "warning": "#D1A000",
        "danger": "#E05252",
        "info": "#5EA2FF",
        "focus": "#9AC7FF",
    },
}

UI_TEXT_SIZE_SCALES: Dict[str, float] = {
    "normal": 1.00,
    "medium": 1.10,
    "large": 1.25,
}

_APP_BASE_FONT: QFont | None = None

BAND_COLORS_LIGHT: Dict[str, str] = {
    "160m": "#7F7F7F",
    "80m": "#CC79A7",
    "60m": "#F0E442",
    "40m": "#009E73",
    "30m": "#56B4E9",
    "20m": "#E69F00",
    "17m": "#8A6B2E",
    "15m": "#D55E00",
    "12m": "#B59B00",
    "10m": "#009060",
}

BAND_COLORS_DARK: Dict[str, str] = {
    "160m": "#7F7F7F",
    "80m": "#8B4D8F",
    "60m": "#F0E442",
    "40m": "#009E73",
    "30m": "#56B4E9",
    "20m": "#F2C14E",
    "17m": "#CC79A7",
    "15m": "#D55E00",
    "12m": "#B59B00",
    "10m": "#009060",
}


def get_theme(name: str) -> Dict[str, str]:
    key = (name or "light").strip().lower()
    return dict(THEMES.get(key, THEMES["light"]))


def resolve_theme(settings) -> Dict[str, str]:
    key = (settings.get("ui_theme", "light") or "light").strip().lower()
    if key not in THEMES:
        key = "light"
    return get_theme(key)


def normalize_ui_text_size(value: object) -> str:
    txt = str(value or "normal").strip().lower()
    aliases = {
        "100": "normal",
        "100%": "normal",
        "1.0": "normal",
        "normal": "normal",
        "110": "medium",
        "110%": "medium",
        "1.1": "medium",
        "medium": "medium",
        "125": "large",
        "125%": "large",
        "1.25": "large",
        "large": "large",
    }
    normalized = aliases.get(txt, txt)
    return normalized if normalized in UI_TEXT_SIZE_SCALES else "normal"


def ui_text_scale_for_size(size_key: object) -> float:
    key = normalize_ui_text_size(size_key)
    return float(UI_TEXT_SIZE_SCALES.get(key, 1.00))


def resolve_ui_text_scale(settings) -> float:
    return ui_text_scale_for_size(settings.get("ui_text_size", "normal"))


def _hex_to_rgb(value: str) -> Tuple[int, int, int]:
    value = value.lstrip("#")
    if len(value) != 6:
        return 0, 0, 0
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)


def _rgb_to_hex(r: int, g: int, b: int) -> str:
    return f"#{r:02X}{g:02X}{b:02X}"


def _blend_hex(fg: str, bg: str, alpha: float) -> str:
    fr, fg_c, fb = _hex_to_rgb(fg)
    br, bg_c, bb = _hex_to_rgb(bg)
    r = int(fr * alpha + br * (1 - alpha))
    g = int(fg_c * alpha + bg_c * (1 - alpha))
    b = int(fb * alpha + bb * (1 - alpha))
    return _rgb_to_hex(r, g, b)


def _luminance(value: str) -> float:
    r, g, b = _hex_to_rgb(value)
    return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255.0


def _pick_text_color(bg_hex: str, light: str, dark: str) -> str:
    return dark if _luminance(bg_hex) > 0.6 else light


def _relative_luminance(value: str) -> float:
    r, g, b = _hex_to_rgb(value)
    srgb = [r / 255.0, g / 255.0, b / 255.0]

    def _channel(c: float) -> float:
        return c / 12.92 if c <= 0.03928 else pow((c + 0.055) / 1.055, 2.4)

    rr, gg, bb = (_channel(c) for c in srgb)
    return 0.2126 * rr + 0.7152 * gg + 0.0722 * bb


def _contrast_ratio(fg_hex: str, bg_hex: str) -> float:
    l1 = _relative_luminance(fg_hex)
    l2 = _relative_luminance(bg_hex)
    hi = max(l1, l2)
    lo = min(l1, l2)
    return (hi + 0.05) / (lo + 0.05)


def _best_contrast_text(bg_hex: str, candidates: Tuple[str, ...]) -> str:
    best = candidates[0]
    best_ratio = _contrast_ratio(best, bg_hex)
    for cand in candidates[1:]:
        ratio = _contrast_ratio(cand, bg_hex)
        if ratio > best_ratio:
            best = cand
            best_ratio = ratio
    return best


def _dual_button_rules(
    *,
    bg: str,
    fg: str,
    border: str,
    hover: str,
    active: str,
    disabled_bg: str,
    disabled_fg: str,
    disabled_border: str,
) -> str:
    def _rules_for(selector: str) -> str:
        return (
            f"{selector} {{"
            f" background-color: {bg}; color: {fg}; border: 1px solid {border};"
            " border-radius: 6px; padding: 4px 10px; font-weight: 600;"
            " }"
            f" {selector}:hover {{ background-color: {hover}; }}"
            f" {selector}:pressed {{ background-color: {active}; }}"
            f" {selector}:disabled {{ background-color: {disabled_bg}; color: {disabled_fg}; border-color: {disabled_border}; }}"
        )

    return _rules_for("QPushButton") + _rules_for("QToolButton")


def button_style(role: str, theme: Dict[str, str]) -> str:
    role = (role or "primary").strip().lower()
    if role == "primary":
        bg = theme["accent"]
        hover = theme["accent_hover"]
        active = theme["accent_active"]
    elif role.startswith("eligible"):
        # Soft CTA highlighting: subtle tint that signals "next eligible action"
        # without overpowering the surrounding UI.
        _, _, suffix = role.partition("_")
        key = suffix or "primary"
        base_map = {
            "primary": theme["accent"],
            "info": theme["info"],
            "success": theme["success"],
            "warning": theme["warning"],
            "danger": theme["danger"],
        }
        base = base_map.get(key, theme["accent"])
        bg = _blend_hex(base, theme["surface"], 0.38)
        hover = _blend_hex(base, theme["surface"], 0.52)
        active = _blend_hex(base, theme["surface"], 0.64)
        border = _blend_hex(base, theme["border"], 0.60)
        fg = _best_contrast_text(bg, ("#111111", "#FFFFFF"))
        disabled_bg = _blend_hex(theme["surface_alt"], theme["surface"], 0.7)
        disabled_fg = theme["text_muted"]
        return _dual_button_rules(
            bg=bg,
            fg=fg,
            border=border,
            hover=hover,
            active=active,
            disabled_bg=disabled_bg,
            disabled_fg=disabled_fg,
            disabled_border=theme["border"],
        )
    elif role == "info":
        bg = theme["info"]
        hover = _blend_hex(theme["info"], theme["surface"], 0.9)
        active = _blend_hex(theme["info"], theme["surface"], 0.8)
    elif role == "success":
        bg = theme["success"]
        hover = _blend_hex(theme["success"], theme["surface"], 0.9)
        active = _blend_hex(theme["success"], theme["surface"], 0.8)
    elif role == "success_muted":
        bg = _blend_hex(theme["success"], theme["surface"], 0.3)
        hover = _blend_hex(theme["success"], theme["surface"], 0.4)
        active = _blend_hex(theme["success"], theme["surface"], 0.5)
    elif role == "warning":
        bg = theme["warning"]
        hover = _blend_hex(theme["warning"], theme["surface"], 0.9)
        active = _blend_hex(theme["warning"], theme["surface"], 0.8)
    elif role == "danger":
        bg = theme["danger"]
        hover = _blend_hex(theme["danger"], theme["surface"], 0.9)
        active = _blend_hex(theme["danger"], theme["surface"], 0.8)
    elif role == "muted":
        bg = theme["surface_alt"]
        hover = _blend_hex(theme["surface_alt"], theme["surface"], 0.8)
        active = _blend_hex(theme["surface_alt"], theme["surface"], 0.7)
    elif role == "secondary":
        bg = theme["surface_alt"]
        hover = _blend_hex(theme["surface_alt"], theme["surface"], 0.8)
        active = _blend_hex(theme["surface_alt"], theme["surface"], 0.7)
    else:
        bg = theme["surface_alt"]
        hover = _blend_hex(theme["surface_alt"], theme["surface"], 0.8)
        active = _blend_hex(theme["surface_alt"], theme["surface"], 0.7)
    fg = _best_contrast_text(bg, ("#111111", "#FFFFFF"))
    disabled_bg = _blend_hex(theme["surface_alt"], theme["surface"], 0.7)
    disabled_fg = theme["text_muted"]
    border = theme["border"]
    return _dual_button_rules(
        bg=bg,
        fg=fg,
        border=border,
        hover=hover,
        active=active,
        disabled_bg=disabled_bg,
        disabled_fg=disabled_fg,
        disabled_border=border,
    )


def normalize_band(band: str) -> str:
    txt = (band or "").strip().lower().replace(" ", "")
    if txt.endswith("m"):
        return txt
    if txt.isdigit():
        return f"{txt}m"
    return txt


def band_cell_colors(band: str, theme: Dict[str, str]) -> Dict[str, str] | None:
    band_key = normalize_band(band)
    is_dark = theme.get("bg") == THEMES["dark"]["bg"]
    palette = BAND_COLORS_DARK if is_dark else BAND_COLORS_LIGHT
    base = palette.get(band_key)
    if not base:
        return None
    alpha = 0.28 if theme is THEMES.get("light") or theme.get("bg") == THEMES["light"]["bg"] else 0.18
    bg = _blend_hex(base, theme["surface"], alpha)
    fg = _pick_text_color(bg, theme["text"], "#111111")
    return {"bg": bg, "fg": fg, "border": base}


def qcolor(value: str) -> QColor:
    return QColor(value)


def led_style(state: str, theme: Dict[str, str]) -> str:
    state = (state or "idle").strip().lower()
    if state == "ok":
        color = theme["success"]
    elif state == "warn":
        color = theme["warning"]
    elif state == "error":
        color = theme["danger"]
    else:
        color = theme["border"]
    return f"background-color: {color}; border-radius: 7px;"


def app_stylesheet(theme: Dict[str, str]) -> str:
    return (
        "QWidget {"
        f" background-color: {theme['bg']};"
        f" color: {theme['text']};"
        "}"
        "QMainWindow, QDialog {"
        f" background-color: {theme['bg']};"
        "}"
        "QGroupBox {"
        f" border: 1px solid {theme['border']};"
        " border-radius: 6px; margin-top: 10px;"
        "}"
        "QGroupBox::title {"
        " subcontrol-origin: margin; left: 8px; padding: 0 4px;"
        f" color: {theme['text']};"
        "}"
        "QLabel {"
        f" color: {theme['text']};"
        "}"
        "QLineEdit, QTextEdit, QPlainTextEdit, QListWidget, QTableWidget, QComboBox {"
        f" background-color: {theme['surface']};"
        f" color: {theme['text']};"
        f" border: 1px solid {theme['border']};"
        " border-radius: 4px; padding: 2px 4px;"
        "}"
        "QPushButton {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text']};"
        f" border: 1px solid {theme['border']};"
        " border-radius: 6px; padding: 4px 10px;"
        "}"
        "QPushButton:hover {"
        f" background-color: {theme['surface']};"
        "}"
        "QPushButton:pressed {"
        f" background-color: {theme['surface_alt']};"
        "}"
        "QPushButton:disabled {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text_muted']};"
        f" border-color: {theme['border']};"
        "}"
        "QTableWidget::item:selected, QListWidget::item:selected {"
        f" background-color: {theme['surface_alt']};"
        "}"
        "QHeaderView::section {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text']};"
        f" border: 1px solid {theme['border']};"
        " padding: 4px;"
        "}"
        "QScrollBar:vertical, QScrollBar:horizontal {"
        f" background: {theme['surface']};"
        "}"
        "QScrollBar::handle:vertical, QScrollBar::handle:horizontal {"
        f" background: {theme['text_muted']};"
        " border-radius: 4px;"
        "}"
        "QScrollBar::handle:vertical:hover, QScrollBar::handle:horizontal:hover {"
        f" background: {theme['accent']};"
        "}"
        "QCheckBox, QRadioButton {"
        f" color: {theme['text']};"
        "}"
        "QCheckBox::indicator {"
        " width: 16px; height: 16px;"
        f" border: 1px solid {theme['text_muted']};"
        " border-radius: 3px;"
        f" background-color: {theme['surface']};"
        "}"
        "QCheckBox::indicator:checked {"
        f" background-color: {theme['accent']};"
        f" border: 1px solid {theme['accent']};"
        "}"
        "QRadioButton::indicator {"
        " width: 16px; height: 16px;"
        f" border: 1px solid {theme['text_muted']};"
        " border-radius: 8px;"
        f" background-color: {theme['surface']};"
        "}"
        "QRadioButton::indicator:checked {"
        f" background-color: {theme['accent']};"
        f" border: 1px solid {theme['accent']};"
        "}"
        "QComboBox QAbstractItemView {"
        f" background-color: {theme['surface']};"
        f" color: {theme['text']};"
        "}"
        "QComboBox QAbstractItemView::indicator {"
        " width: 16px; height: 16px;"
        f" border: 1px solid {theme['text_muted']};"
        " border-radius: 3px;"
        f" background-color: {theme['surface']};"
        "}"
        "QComboBox QAbstractItemView::indicator:checked {"
        f" background-color: {theme['accent']};"
        f" border: 1px solid {theme['accent']};"
        "}"
        "QToolTip {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text']};"
        f" border: 1px solid {theme['border']};"
        "}"
    )


def apply_app_theme(app, theme: Dict[str, str], *, ui_text_scale: float = 1.00) -> None:
    global _APP_BASE_FONT
    if app is None:
        return
    if _APP_BASE_FONT is None:
        _APP_BASE_FONT = QFont(app.font())
    scaled_font = QFont(_APP_BASE_FONT)
    try:
        scale = float(ui_text_scale)
    except Exception:
        scale = 1.00
    scale = min(1.25, max(1.00, scale))
    if scaled_font.pointSizeF() > 0:
        scaled_font.setPointSizeF(max(6.0, scaled_font.pointSizeF() * scale))
    elif scaled_font.pixelSize() > 0:
        scaled_font.setPixelSize(max(8, int(round(scaled_font.pixelSize() * scale))))
    app.setFont(scaled_font)
    pal = QPalette()
    pal.setColor(QPalette.Window, qcolor(theme["bg"]))
    pal.setColor(QPalette.WindowText, qcolor(theme["text"]))
    pal.setColor(QPalette.Base, qcolor(theme["surface"]))
    pal.setColor(QPalette.AlternateBase, qcolor(theme["surface_alt"]))
    pal.setColor(QPalette.Text, qcolor(theme["text"]))
    pal.setColor(QPalette.Button, qcolor(theme["surface_alt"]))
    pal.setColor(QPalette.ButtonText, qcolor(theme["text"]))
    pal.setColor(QPalette.ToolTipBase, qcolor(theme["surface_alt"]))
    pal.setColor(QPalette.ToolTipText, qcolor(theme["text"]))
    pal.setColor(QPalette.Link, qcolor(theme["accent"]))
    pal.setColor(QPalette.Highlight, qcolor(theme["accent"]))
    pal.setColor(QPalette.HighlightedText, qcolor(theme["text"]))
    app.setPalette(pal)
    app.setStyleSheet(app_stylesheet(theme))
