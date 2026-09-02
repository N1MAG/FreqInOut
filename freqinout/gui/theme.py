from __future__ import annotations

from typing import Dict, Tuple

from PySide6.QtGui import QColor, QFont, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QAbstractButton,
    QComboBox,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QSplitter,
    QTextEdit,
    QWidget,
)


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
        "station_control_surface": "#D7EAF8",
        "station_control_border": "#2E6F9E",
        "station_control_text": "#102A3D",
        "station_control_muted": "#3F6278",
        "station_control_tile_surface": "#EDF5FB",
        "station_control_tile_selected_surface": "#FFFFFF",
        "station_control_tile_border": "#B8D4E8",
        "station_control_tile_selected_border": "#2E6F9E",
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
        "station_control_surface": "#12324A",
        "station_control_border": "#4C9BD3",
        "station_control_text": "#F3F8FF",
        "station_control_muted": "#B9D3E8",
        "station_control_tile_surface": "#173B57",
        "station_control_tile_selected_surface": "#1C4564",
        "station_control_tile_border": "#2A5878",
        "station_control_tile_selected_border": "#74B6E5",
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
MAX_QT_HEIGHT = 16777215

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


def font_css(font: QFont) -> str:
    parts: list[str] = []
    try:
        family = str(font.family() or "").strip()
    except Exception:
        family = ""
    if family:
        family = family.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'font-family: "{family}";')
    try:
        point_size = float(font.pointSizeF())
    except Exception:
        point_size = -1.0
    if point_size > 0:
        parts.append(f"font-size: {point_size:.2f}pt;")
    else:
        try:
            pixel_size = int(font.pixelSize())
        except Exception:
            pixel_size = -1
        if pixel_size > 0:
            parts.append(f"font-size: {pixel_size}px;")
    return " ".join(parts)


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


def _combo_box_text_width(combo: QComboBox) -> Tuple[int, int]:
    metrics = combo.fontMetrics()
    longest_text = combo.currentText() or ""
    longest_width = metrics.horizontalAdvance(longest_text)
    count = combo.count()
    sample_limit = min(count, 250)
    for idx in range(sample_limit):
        text = combo.itemText(idx)
        width = metrics.horizontalAdvance(text)
        if width > longest_width:
            longest_width = width
            longest_text = text
    if count > sample_limit:
        for idx in range(max(sample_limit, count - 25), count):
            text = combo.itemText(idx)
            width = metrics.horizontalAdvance(text)
            if width > longest_width:
                longest_width = width
                longest_text = text
    return longest_width, len(longest_text)


def fit_combo_box_to_contents(combo: QComboBox) -> None:
    try:
        if combo is None or combo.property("fio_no_auto_fit"):
            return
        if combo.count() <= 0 and not combo.currentText():
            return
        combo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
        text_width, text_len = _combo_box_text_width(combo)
        closed_width = min(max(text_width + 48, 76), 360)
        popup_width = min(max(text_width + 64, closed_width), 520)
        if combo.maximumWidth() > combo.minimumWidth() and combo.minimumWidth() < closed_width:
            combo.setMinimumWidth(closed_width)
        combo.setMinimumContentsLength(min(max(combo.minimumContentsLength(), text_len), 34))
        view = combo.view()
        if view is not None and view.minimumWidth() < popup_width:
            view.setMinimumWidth(popup_width)
    except Exception:
        return


def fit_existing_combo_boxes(app) -> None:
    if app is None:
        return
    try:
        for widget in app.allWidgets():
            if isinstance(widget, QComboBox):
                fit_combo_box_to_contents(widget)
    except Exception:
        return


def fit_child_combo_boxes(container) -> None:
    if container is None:
        return
    try:
        if isinstance(container, QComboBox):
            fit_combo_box_to_contents(container)
        for widget in container.findChildren(QComboBox):
            fit_combo_box_to_contents(widget)
    except Exception:
        return


def mark_text_size_guard_opt_out(widget: QWidget | None) -> None:
    if widget is None:
        return
    try:
        widget.setProperty("fio_text_size_guard_opt_out", True)
    except Exception:
        return


def control_height_for_font(widget: QWidget | None, *, vertical_padding: int = 10, floor: int = 28) -> int:
    if widget is None:
        return floor
    try:
        line_h = int(widget.fontMetrics().lineSpacing())
    except Exception:
        line_h = 0
    return max(int(floor), line_h + int(vertical_padding))


def button_height_for_font(widget: QWidget | None, *, vertical_padding: int = 12, floor: int = 30) -> int:
    return control_height_for_font(widget, vertical_padding=vertical_padding, floor=floor)


def single_line_label_height(widget: QWidget | None, *, vertical_padding: int = 6, floor: int = 24) -> int:
    return control_height_for_font(widget, vertical_padding=vertical_padding, floor=floor)


def _iter_text_size_guard_widgets(root) -> list[QWidget]:
    if root is None:
        return []
    try:
        if isinstance(root, QApplication):
            return [widget for widget in root.allWidgets() if isinstance(widget, QWidget)]
    except Exception:
        pass
    widgets: list[QWidget] = []
    if isinstance(root, QWidget):
        widgets.append(root)
        try:
            widgets.extend([widget for widget in root.findChildren(QWidget) if isinstance(widget, QWidget)])
        except Exception:
            pass
    return widgets


def _has_text_size_guard_opt_out(widget: QWidget) -> bool:
    try:
        return bool(widget.property("fio_text_size_guard_opt_out"))
    except Exception:
        return False


def _raise_widget_height_to_font(widget: QWidget, target_h: int) -> None:
    target_h = max(0, int(target_h or 0))
    if target_h <= 0:
        return
    try:
        current_min = int(widget.minimumHeight())
    except Exception:
        current_min = 0
    try:
        current_max = int(widget.maximumHeight())
    except Exception:
        current_max = MAX_QT_HEIGHT
    try:
        if current_min < target_h:
            widget.setMinimumHeight(target_h)
        if current_max < MAX_QT_HEIGHT and current_max < target_h:
            widget.setMaximumHeight(target_h)
    except Exception:
        return


def _widget_has_visible_text(widget: QWidget) -> bool:
    try:
        if isinstance(widget, QComboBox):
            return bool(widget.currentText() or widget.count())
        if isinstance(widget, QLineEdit):
            return bool(widget.text() or widget.placeholderText())
        if isinstance(widget, QLabel):
            return bool(str(widget.text() or "").strip())
        if isinstance(widget, QAbstractButton):
            return bool(str(widget.text() or "").strip())
        if isinstance(widget, (QPlainTextEdit, QTextEdit)):
            return bool(widget.placeholderText() or widget.toPlainText())
    except Exception:
        return False
    return False


def apply_text_size_accessibility_guards(root, *, include_widths: bool = True) -> None:
    """Raise undersized text controls so the active app font does not clip."""
    for widget in _iter_text_size_guard_widgets(root):
        if _has_text_size_guard_opt_out(widget):
            continue
        if not _widget_has_visible_text(widget):
            continue
        if isinstance(widget, (QPlainTextEdit, QTextEdit)):
            _raise_widget_height_to_font(widget, control_height_for_font(widget, vertical_padding=16, floor=48))
        elif isinstance(widget, QAbstractButton):
            _raise_widget_height_to_font(widget, button_height_for_font(widget))
        elif isinstance(widget, (QComboBox, QLineEdit)):
            _raise_widget_height_to_font(widget, control_height_for_font(widget))
        elif isinstance(widget, QLabel) and not widget.wordWrap():
            _raise_widget_height_to_font(widget, single_line_label_height(widget))
        if not include_widths:
            continue
        try:
            if isinstance(widget, QAbstractButton):
                text = str(widget.text() or "").replace("&", "").strip()
                if text:
                    needed = int(widget.fontMetrics().horizontalAdvance(text) + 30)
                    if needed > int(widget.minimumWidth() or 0):
                        widget.setMinimumWidth(min(420, needed))
        except Exception:
            pass


def style_splitter_handles(splitter: QSplitter | None, theme: Dict[str, str] | None = None, *, width: int = 12) -> None:
    """Make resizable panel handles visible and easier to grab."""
    if splitter is None:
        return
    theme = theme or resolve_theme(False)
    handle_width = max(8, int(width or 0))
    try:
        splitter.setHandleWidth(handle_width)
        splitter.setChildrenCollapsible(False)
        splitter.setToolTip("")
        splitter.setStyleSheet(
            "QSplitter::handle {"
            f" background-color: {theme['border']};"
            " border-radius: 4px;"
            "}"
            "QSplitter::handle:horizontal {"
            " margin: 3px 4px;"
            " width: 8px;"
            "}"
            "QSplitter::handle:vertical {"
            " margin: 4px 3px;"
            " height: 8px;"
            "}"
            "QSplitter::handle:hover {"
            f" background-color: {theme['accent']};"
            "}"
        )
    except Exception:
        return


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
        "QLineEdit:disabled, QTextEdit:disabled, QPlainTextEdit:disabled, QListWidget:disabled, QTableWidget:disabled, QComboBox:disabled {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text_muted']};"
        f" border: 1px solid {theme['border']};"
        "}"
        "QTabWidget::pane {"
        f" border: 1px solid {theme['border']};"
        f" background-color: {theme['bg']};"
        " top: -1px;"
        "}"
        "QTabBar::tab {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text']};"
        f" border: 1px solid {theme['border']};"
        " border-bottom-color: transparent;"
        " padding: 6px 14px;"
        " min-height: 22px;"
        "}"
        "QTabBar::tab:selected {"
        f" background-color: {theme['surface']};"
        f" color: {theme['text']};"
        f" border-color: {theme['accent']};"
        "}"
        "QTabBar::tab:hover {"
        f" background-color: {theme['surface']};"
        f" color: {theme['text']};"
        "}"
        "QTabBar::tab:disabled {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text_muted']};"
        "}"
        "QPushButton, QToolButton {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text']};"
        f" border: 1px solid {theme['border']};"
        " border-radius: 6px; padding: 4px 10px;"
        "}"
        "QPushButton:hover, QToolButton:hover {"
        f" background-color: {theme['surface']};"
        "}"
        "QPushButton:pressed, QToolButton:pressed {"
        f" background-color: {theme['surface_alt']};"
        "}"
        "QPushButton:disabled, QToolButton:disabled {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text_muted']};"
        f" border-color: {theme['border']};"
        "}"
        "QTableWidget::item:selected, QListWidget::item:selected {"
        f" background-color: {theme['surface_alt']};"
        f" color: {theme['text']};"
        "}"
        "QTableWidget::item:disabled, QListWidget::item:disabled {"
        f" color: {theme['text_muted']};"
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
    pal.setColor(QPalette.Disabled, QPalette.WindowText, qcolor(theme["text_muted"]))
    pal.setColor(QPalette.Disabled, QPalette.Text, qcolor(theme["text_muted"]))
    pal.setColor(QPalette.Disabled, QPalette.ButtonText, qcolor(theme["text_muted"]))
    pal.setColor(QPalette.Disabled, QPalette.Base, qcolor(theme["surface_alt"]))
    pal.setColor(QPalette.Disabled, QPalette.Button, qcolor(theme["surface_alt"]))
    app.setPalette(pal)
    app.setStyleSheet(app_stylesheet(theme))
    fit_existing_combo_boxes(app)
    apply_text_size_accessibility_guards(app, include_widths=False)
