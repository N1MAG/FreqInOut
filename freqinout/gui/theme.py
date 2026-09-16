from __future__ import annotations

from typing import Dict, Tuple

from PySide6.QtCore import QSize, Qt
from PySide6.QtGui import QColor, QFont, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QAbstractButton,
    QAbstractItemView,
    QAbstractSpinBox,
    QComboBox,
    QGroupBox,
    QHeaderView,
    QLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QPlainTextEdit,
    QSplitter,
    QTabBar,
    QTableView,
    QTextEdit,
    QTreeView,
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


def active_app_theme(default: Dict[str, str] | None = None) -> Dict[str, str]:
    """Return the last applied shared theme without reading configuration."""

    app = QApplication.instance()
    if app is not None:
        value = app.property("fio_active_theme")
        if isinstance(value, dict) and value:
            return dict(value)
    return dict(default or THEMES["light"])


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


def contrast_text_for_background(bg_hex: str, theme: Dict[str, str]) -> str:
    """Return readable text for a theme-owned filled surface."""

    return _best_contrast_text(
        bg_hex,
        (theme.get("text", "#111111"), "#111111", "#FFFFFF"),
    )


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


def action_chip_metrics(font_metrics) -> Tuple[int, int, int, int, int]:
    """Return shared font-derived geometry for compact table action chips.

    The tuple is ``(horizontal_padding, gap, cell_margin, height, radius)``.
    Keeping this treatment in the shared theme layer prevents delegates from
    inventing platform-specific button sizes or local chip radii.
    """

    line_height = max(1, int(font_metrics.lineSpacing()))
    em_width = max(1, int(font_metrics.horizontalAdvance("M")))
    space_width = max(1, int(font_metrics.horizontalAdvance(" ")))
    horizontal_padding = max(em_width, line_height)
    gap = max(space_width, line_height // 3)
    cell_margin = max(space_width, line_height // 4)
    height = line_height + max(6, line_height // 2)
    radius = max(3, line_height // 3)
    return horizontal_padding, gap, cell_margin, height, radius


def action_chip_colors(
    role: str,
    theme: Dict[str, str],
    *,
    enabled: bool = True,
    active: bool = False,
    hovered: bool = False,
) -> Tuple[str, str, str]:
    """Return shared ``(background, foreground, border)`` chip colors.

    Painted item-view actions cannot consume a QWidget stylesheet directly,
    so delegates use this cache-only semantic companion to :func:`button_style`.
    It preserves the same theme roles without falling back to native push-button
    chrome that differs across macOS, Linux, and Windows.
    """

    role_key = str(role or "secondary").strip().lower()
    if not enabled:
        return theme["surface"], theme["text_muted"], theme["border"]

    if active:
        base = theme["success"]
        background = _blend_hex(base, theme["surface"], 0.30 if hovered else 0.22)
        return background, theme["text"], base

    if role_key == "danger":
        base = theme["danger"]
        if hovered:
            background = _blend_hex(base, theme["surface"], 0.24)
            return background, base, base
        # Dense tables must not become a red stripe.  Destructive row actions
        # use neutral chrome at rest and reveal danger semantics on hover/focus
        # before their existing confirmation step.
        return theme["surface_alt"], theme["text"], theme["border"]

    if hovered:
        background = _blend_hex(theme["accent"], theme["surface"], 0.34)
        return background, theme["text"], theme["accent"]

    return theme["surface_alt"], theme["text"], theme["border"]


def choice_chip_selector_style(object_name: str, theme: Dict[str, str]) -> str:
    """Return the shared themed treatment for wrapping peer-choice chips."""

    name = str(object_name or "fioChoiceChipSelector").strip()
    selector = f"QListWidget#{name}"
    accent_text = contrast_text_for_background(theme["accent"], theme)
    return (
        f"{selector} {{"
        f" background-color: {theme['surface']};"
        f" border: 1px solid {theme['border']};"
        " border-radius: 6px; padding: 3px; outline: 0;"
        "}"
        f" {selector}:focus {{ border-color: {theme['accent']}; }}"
        f" {selector}::item {{"
        f" background-color: {theme['surface_alt']}; color: {theme['text']};"
        f" border: 1px solid {theme['border']};"
        " border-radius: 6px; padding: 5px 16px; margin: 1px 4px 1px 0;"
        " font-weight: 600;"
        "}"
        f" {selector}::item:hover {{"
        f" background-color: {theme['accent_hover']}; color: {accent_text};"
        "}"
        f" {selector}::item:selected {{"
        f" background-color: {theme['accent']}; color: {accent_text};"
        f" border-color: {theme['accent_active']};"
        "}"
        f" {selector}::item:disabled {{"
        f" background-color: {theme['surface']}; color: {theme['text_muted']};"
        f" border-color: {theme['border']};"
        "}"
    )


def fit_wrapping_choice_chip_selector(selector: QListWidget | None) -> int:
    """Fit a left-flowing chip selector from its active font and viewport.

    The operation is geometry-only and bounded by the selector's item count.
    It returns the calculated height so an owning group may include its title.
    """

    if not isinstance(selector, QListWidget):
        return 0
    metrics = selector.fontMetrics()
    line_height = max(1, int(metrics.lineSpacing()))
    row_height = button_height_for_font(
        selector,
        vertical_padding=max(1, line_height // 2),
        floor=line_height,
    )
    item_gap = max(1, line_height // 3)
    widths: list[int] = []
    for index in range(selector.count()):
        item = selector.item(index)
        text = item.text() if item is not None else ""
        width = max(line_height, int(metrics.horizontalAdvance(text)) + 3 * line_height)
        widths.append(width)
        if item is not None:
            item.setSizeHint(QSize(width, row_height))
    selector.setTextElideMode(Qt.ElideNone)
    available = int(selector.viewport().width() or selector.width() or 0)
    if available <= 0:
        parent = selector.parentWidget()
        available = int(parent.width() or 0) if parent is not None else 0
    available = max(1, available)
    rows = 1
    flow_gap = 2 * item_gap
    used = item_gap
    for width in widths:
        trailing = item_gap
        if used > item_gap and used + flow_gap + width + trailing > available:
            rows += 1
            used = item_gap + width
        else:
            used += width if used == item_gap else flow_gap + width
    frame = max(1, int(selector.frameWidth()))
    height = rows * row_height + 2 * frame + 2 * rows * item_gap
    selector.setSpacing(item_gap)
    selector.setMinimumHeight(height)
    selector.setMaximumHeight(height)
    selector.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    selector.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    selector.updateGeometry()
    return height


def label_style(role: str, theme: Dict[str, str], *, weight: int = 400) -> str:
    """Return a small semantic label treatment from the shared palette.

    Labels often carry quiet workflow context, but they must still follow the
    active Light/Dark palette.  Keeping this here prevents each workspace from
    inventing a near-identical muted or informational text color.
    """
    key = (role or "muted").strip().lower()
    color_key = {
        "muted": "text_muted",
        "info": "info",
        "success": "success",
        "warning": "warning",
        "danger": "danger",
        "text": "text",
    }.get(key, "text_muted")
    return f"color: {theme[color_key]}; font-weight: {max(100, int(weight))};"


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


def horizontal_layout_breakpoint(layout: QLayout | None, *, reserve_controls: int = 1) -> int:
    """Return a font/content-derived width at which a control row should stack.

    The calculation is independent of the layout's current direction, so repeated
    compact/wide transitions cannot create a breakpoint feedback loop.  A small
    reserve expressed in live control widths leaves room for translated labels,
    focus rings, and platform-native subcontrols.
    """

    if layout is None:
        return 0
    widths = []
    try:
        for index in range(layout.count()):
            item = layout.itemAt(index)
            widget = item.widget() if item is not None else None
            if widget is None or widget.isHidden():
                continue
            hint = widget.sizeHint().width()
            widths.append(max(int(widget.minimumSizeHint().width()), int(hint), 0))
        margins = layout.contentsMargins()
        spacing = max(0, int(layout.spacing()))
        natural = sum(widths) + spacing * max(0, len(widths) - 1)
        natural += int(margins.left()) + int(margins.right())
        return natural + max(widths, default=0) * max(0, int(reserve_controls))
    except Exception:
        return 0


def single_line_label_height(widget: QWidget | None, *, vertical_padding: int = 6, floor: int = 24) -> int:
    return control_height_for_font(widget, vertical_padding=vertical_padding, floor=floor)


def font_derived_widget_height(
    widget: QWidget | None,
    *,
    vertical_padding: int = 10,
    floor: int = 28,
    include_size_hints: bool = True,
) -> int:
    """Return a readable one-line floor for the widget's active style/font.

    ``sizeHint`` carries platform style, indicator, icon, and subcontrol needs;
    font metrics protect text when a style reports an undersized hint.  This
    helper is deliberately geometry-only so it is safe during lazy-page theme
    publication and repeated accessibility passes.
    """
    target = control_height_for_font(
        widget,
        vertical_padding=vertical_padding,
        floor=floor,
    )
    if widget is None or not include_size_hints:
        return target
    # QHeaderView.minimumSizeHint() is the generic scroll-area viewport floor
    # (often about 88 px high), not the painted header-line requirement.  Its
    # sizeHint carries the actual orientation-specific header thickness.
    hint_names = ("sizeHint",) if isinstance(widget, QHeaderView) else ("minimumSizeHint", "sizeHint")
    for hint_name in hint_names:
        try:
            hint = getattr(widget, hint_name)()
            if hint is not None and hint.isValid():
                target = max(target, int(hint.height()))
        except Exception:
            continue
    return target


def multiline_height_for_font(
    widget: QWidget | None,
    *,
    visible_lines: int = 3,
    vertical_padding: int = 16,
) -> int:
    """Return a font-derived height for a bounded multiline text surface."""

    if widget is None:
        return max(1, int(visible_lines)) * 16 + int(vertical_padding)
    try:
        line_h = max(1, int(widget.fontMetrics().lineSpacing()))
    except Exception:
        line_h = 16
    frame_h = 0
    try:
        frame_h = max(0, int(widget.frameWidth()) * 2)
    except Exception:
        pass
    return max(
        font_derived_widget_height(
            widget,
            vertical_padding=vertical_padding,
            floor=0,
            include_size_hints=False,
        ),
        max(1, int(visible_lines)) * line_h + int(vertical_padding) + frame_h,
    )


def item_view_height_for_rows(
    view: QWidget | None,
    *,
    visible_rows: int,
    include_header: bool = True,
) -> int:
    """Return a font/style-derived height for a bounded table or list viewport."""

    if view is None:
        return max(1, int(visible_rows)) * 24
    try:
        row_h = max(
            int(view.fontMetrics().lineSpacing()) + 10,
            int(view.sizeHintForRow(0)) if hasattr(view, "sizeHintForRow") else 0,
        )
    except Exception:
        row_h = max(24, int(view.fontMetrics().lineSpacing()) + 10)
    header_h = 0
    if include_header:
        try:
            header = view.horizontalHeader()
            header_h = font_derived_widget_height(header, vertical_padding=10, floor=0)
        except Exception:
            pass
    frame_h = 0
    try:
        frame_h = max(0, int(view.frameWidth()) * 2)
    except Exception:
        pass
    return header_h + max(1, int(visible_rows)) * row_h + frame_h


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


def _apply_item_view_font_geometry(widget: QAbstractItemView) -> None:
    """Raise table/tree header and row floors from current font/style metrics."""
    # An item view's own size hint describes the entire viewport, never one row.
    row_h = font_derived_widget_height(
        widget,
        vertical_padding=8,
        floor=24,
        include_size_hints=False,
    )
    try:
        vertical_header = widget.verticalHeader() if isinstance(widget, QTableView) else None
        if vertical_header is not None:
            vertical_header.setMinimumSectionSize(max(vertical_header.minimumSectionSize(), row_h))
            if vertical_header.defaultSectionSize() < row_h:
                vertical_header.setDefaultSectionSize(row_h)
    except Exception:
        pass
    try:
        horizontal_header = widget.header() if isinstance(widget, QTreeView) else widget.horizontalHeader()
        if isinstance(horizontal_header, QHeaderView):
            header_h = font_derived_widget_height(horizontal_header, vertical_padding=10, floor=28)
            _raise_widget_height_to_font(horizontal_header, header_h)
            horizontal_header.setMinimumSectionSize(
                max(horizontal_header.minimumSectionSize(), header_h)
            )
    except Exception:
        pass


def apply_text_size_accessibility_guards(root, *, include_widths: bool = True) -> None:
    """Apply the shared font-derived safety floor to an existing widget tree.

    The pass is geometry-only and idempotent, so callers may run it after lazy
    construction and font/theme changes.  It remains a safety net: owning
    screens must still provide responsive task layout and scroll ownership.
    """
    for widget in _iter_text_size_guard_widgets(root):
        if _has_text_size_guard_opt_out(widget):
            continue
        if isinstance(widget, QAbstractItemView):
            _apply_item_view_font_geometry(widget)
        if isinstance(widget, QTabBar) and widget.count() > 0:
            _raise_widget_height_to_font(
                widget,
                font_derived_widget_height(widget, vertical_padding=12, floor=32),
            )
            continue
        if isinstance(widget, (QPlainTextEdit, QTextEdit)):
            _raise_widget_height_to_font(widget, control_height_for_font(widget, vertical_padding=16, floor=48))
        elif isinstance(widget, QAbstractButton):
            _raise_widget_height_to_font(
                widget,
                font_derived_widget_height(widget, vertical_padding=12, floor=30),
            )
        elif isinstance(widget, (QComboBox, QLineEdit, QAbstractSpinBox)):
            _raise_widget_height_to_font(
                widget,
                font_derived_widget_height(widget, vertical_padding=10, floor=28),
            )
        elif isinstance(widget, QGroupBox) and str(widget.title() or "").strip():
            # A group's size hint includes its complete child layout.  It is
            # therefore not a one-line font metric and may contain default
            # item-view viewport hints (often about 192 px).  Raising the
            # group's minimum height to that aggregate hint makes the value
            # sticky across later responsive reflow and produces large blank
            # regions.  Protect only the title line here; the owning layout
            # and the guarded child controls own content height.
            _raise_widget_height_to_font(
                widget,
                font_derived_widget_height(
                    widget,
                    vertical_padding=12,
                    floor=32,
                    include_size_hints=False,
                ),
            )
        elif isinstance(widget, QLabel) and not widget.wordWrap() and _widget_has_visible_text(widget):
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
        "QLineEdit, QTextEdit, QTextBrowser, QPlainTextEdit, QListWidget, QTableWidget, QComboBox, QSpinBox, QDoubleSpinBox, QDateEdit, QTimeEdit, QDateTimeEdit {"
        f" background-color: {theme['surface']};"
        f" color: {theme['text']};"
        f" border: 1px solid {theme['border']};"
        " border-radius: 4px; padding: 2px 4px;"
        f" selection-background-color: {theme['accent']};"
        f" selection-color: {_best_contrast_text(theme['accent'], ('#FFFFFF', '#111111', theme['text']))};"
        "}"
        "QLineEdit:focus, QTextEdit:focus, QTextBrowser:focus, QPlainTextEdit:focus, QListWidget:focus, QTableWidget:focus, QComboBox:focus, QSpinBox:focus, QDoubleSpinBox:focus, QDateEdit:focus, QTimeEdit:focus, QDateTimeEdit:focus {"
        f" border: 1px solid {theme['focus']};"
        "}"
        "QLineEdit:disabled, QTextEdit:disabled, QTextBrowser:disabled, QPlainTextEdit:disabled, QListWidget:disabled, QTableWidget:disabled, QComboBox:disabled, QSpinBox:disabled, QDoubleSpinBox:disabled, QDateEdit:disabled, QTimeEdit:disabled, QDateTimeEdit:disabled {"
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
        "QSpinBox::up-button, QSpinBox::down-button, QDoubleSpinBox::up-button, QDoubleSpinBox::down-button, QDateEdit::up-button, QDateEdit::down-button, QTimeEdit::up-button, QTimeEdit::down-button, QDateTimeEdit::up-button, QDateTimeEdit::down-button {"
        f" background-color: {theme['surface_alt']};"
        f" border-left: 1px solid {theme['border']};"
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
    app.setProperty("fio_active_theme", dict(theme))
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
