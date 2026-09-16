from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QDateTimeEdit,
    QGroupBox,
    QLineEdit,
    QListWidget,
    QPlainTextEdit,
    QRadioButton,
    QSpinBox,
    QTabBar,
    QTableWidget,
    QTreeWidget,
    QWidget,
)

from freqinout.gui.theme import (
    action_chip_colors,
    action_chip_metrics,
    active_app_theme,
    apply_app_theme,
    apply_text_size_accessibility_guards,
    contrast_text_for_background,
    choice_chip_selector_style,
    fit_wrapping_choice_chip_selector,
    font_derived_widget_height,
    get_theme,
    item_view_height_for_rows,
    mark_text_size_guard_opt_out,
    multiline_height_for_font,
)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _large_font(widget: QWidget) -> None:
    font = QFont(widget.font())
    font.setPointSize(max(22, font.pointSize() + 10))
    widget.setFont(font)


def test_shared_guard_covers_input_indicator_tab_and_group_families() -> None:
    _app()
    root = QWidget()
    controls = [
        QLineEdit(root),
        QSpinBox(root),
        QDateTimeEdit(root),
        QCheckBox("Enabled", root),
        QRadioButton("Paused", root),
        QGroupBox("Configuration", root),
    ]
    tabs = QTabBar(root)
    tabs.addTab("Activity")
    controls.append(tabs)
    try:
        for widget in controls:
            _large_font(widget)
            widget.setMinimumHeight(0)
            widget.setMaximumHeight(10)

        apply_text_size_accessibility_guards(root, include_widths=False)

        for widget in controls:
            target = font_derived_widget_height(
                widget,
                vertical_padding=12 if isinstance(widget, (QCheckBox, QRadioButton, QGroupBox, QTabBar)) else 10,
                floor=30 if isinstance(widget, (QCheckBox, QRadioButton)) else 28,
                include_size_hints=not isinstance(widget, QGroupBox),
            )
            assert widget.minimumHeight() >= target
            assert widget.maximumHeight() >= target
    finally:
        root.deleteLater()


def test_group_guard_protects_title_without_promoting_child_viewport_hint() -> None:
    """A group's aggregate child hint must not become a sticky font floor."""
    _app()
    root = QWidget()
    group = QGroupBox("Mode", root)
    child = QTableWidget(0, 1, group)
    child.setMinimumHeight(0)
    group.setMinimumHeight(0)
    try:
        aggregate_hint = group.sizeHint().height()
        apply_text_size_accessibility_guards(root, include_widths=False)
        title_floor = font_derived_widget_height(
            group,
            vertical_padding=12,
            floor=32,
            include_size_hints=False,
        )
        assert group.minimumHeight() == title_floor
        assert group.minimumHeight() < max(aggregate_hint, child.sizeHint().height())
    finally:
        root.deleteLater()


def test_shared_guard_raises_table_tree_headers_and_row_floors() -> None:
    _app()
    root = QWidget()
    table = QTableWidget(2, 2, root)
    tree = QTreeWidget(root)
    tree.setHeaderLabels(["Name", "Status"])
    try:
        for widget in (table, tree):
            _large_font(widget)
            if isinstance(widget, QTableWidget):
                widget.verticalHeader().setDefaultSectionSize(10)
            widget.horizontalHeader().setMaximumHeight(10) if isinstance(widget, QTableWidget) else widget.header().setMaximumHeight(10)

        apply_text_size_accessibility_guards(root, include_widths=False)

        table_floor = font_derived_widget_height(
            table,
            vertical_padding=8,
            floor=24,
            include_size_hints=False,
        )
        assert table.verticalHeader().defaultSectionSize() >= table_floor
        assert table.horizontalHeader().minimumHeight() >= font_derived_widget_height(
            table.horizontalHeader(), vertical_padding=10, floor=28
        )
        assert table_floor < 100, "row floor must not inherit the whole view's size hint"
        assert tree.header().minimumHeight() >= font_derived_widget_height(
            tree.header(), vertical_padding=10, floor=28
        )
    finally:
        root.deleteLater()


def test_shared_guard_is_idempotent_and_respects_explicit_opt_out() -> None:
    _app()
    root = QWidget()
    protected = QLineEdit(root)
    protected.setPlaceholderText("Intentionally bounded")
    protected.setMinimumHeight(7)
    protected.setMaximumHeight(7)
    mark_text_size_guard_opt_out(protected)
    normal = QLineEdit(root)
    _large_font(normal)
    normal.setMaximumHeight(8)
    try:
        apply_text_size_accessibility_guards(root, include_widths=False)
        snapshot = (normal.minimumHeight(), normal.maximumHeight())
        apply_text_size_accessibility_guards(root, include_widths=False)
        assert (normal.minimumHeight(), normal.maximumHeight()) == snapshot
        assert protected.minimumHeight() == 7
        assert protected.maximumHeight() == 7
    finally:
        root.deleteLater()


def test_shared_guard_source_remains_geometry_only() -> None:
    import inspect

    source = inspect.getsource(apply_text_size_accessibility_guards).lower()
    forbidden = ("sqlite", "subprocess", "socket", "requests", "path(", "open(")
    assert not [token for token in forbidden if token in source]


def test_active_theme_cache_tracks_shared_theme_without_settings_read() -> None:
    app = _app()
    dark = get_theme("dark")
    try:
        apply_app_theme(app, dark, ui_text_scale=1.0)
        assert active_app_theme()["bg"] == dark["bg"]
    finally:
        apply_app_theme(app, get_theme("light"), ui_text_scale=1.0)


def test_shared_filled_surface_contrast_follows_background() -> None:
    theme = get_theme("dark")
    assert contrast_text_for_background(theme["text"], theme) == "#111111"
    assert contrast_text_for_background(theme["bg"], theme) == "#FFFFFF"


def test_multiline_and_item_view_bounds_grow_with_the_active_font() -> None:
    _app()
    edit = QPlainTextEdit()
    table = QTableWidget(2, 2)
    try:
        normal_edit = multiline_height_for_font(edit, visible_lines=4)
        normal_table = item_view_height_for_rows(table, visible_rows=4)
        _large_font(edit)
        _large_font(table)
        assert multiline_height_for_font(edit, visible_lines=4) > normal_edit
        assert item_view_height_for_rows(table, visible_rows=4) > normal_table
    finally:
        edit.deleteLater()
        table.deleteLater()


def test_shared_choice_chip_selector_wraps_and_uses_theme_tokens() -> None:
    _app()
    selector = QListWidget()
    selector.setObjectName("testChoiceChips")
    selector.setFlow(QListWidget.LeftToRight)
    selector.setWrapping(True)
    selector.setFixedWidth(260)
    selector.addItems(["Watches", "Expect", "Access Policies", "Forms", "Imports"])
    try:
        theme = get_theme("dark")
        selector.setStyleSheet(choice_chip_selector_style(selector.objectName(), theme))
        fitted_height = fit_wrapping_choice_chip_selector(selector)
        assert theme["accent"] in selector.styleSheet()
        assert theme["surface_alt"] in selector.styleSheet()
        assert fitted_height > selector.item(0).sizeHint().height()
        assert selector.horizontalScrollBarPolicy().name == "ScrollBarAlwaysOff"
        assert selector.verticalScrollBarPolicy().name == "ScrollBarAlwaysOff"
    finally:
        selector.deleteLater()


def test_shared_table_action_chip_treatment_is_theme_and_font_derived() -> None:
    _app()
    widget = QWidget()
    try:
        normal_metrics = action_chip_metrics(widget.fontMetrics())
        _large_font(widget)
        large_metrics = action_chip_metrics(widget.fontMetrics())
        assert large_metrics[0] > normal_metrics[0]
        assert large_metrics[3] > normal_metrics[3]

        for theme_name in ("light", "dark"):
            theme = get_theme(theme_name)
            normal = action_chip_colors("secondary", theme)
            danger = action_chip_colors("danger", theme)
            danger_hover = action_chip_colors("danger", theme, hovered=True)
            disabled = action_chip_colors("secondary", theme, enabled=False)
            active = action_chip_colors("secondary", theme, active=True)
            assert normal == (theme["surface_alt"], theme["text"], theme["border"])
            assert danger == (theme["surface_alt"], theme["text"], theme["border"])
            assert danger_hover[1:] == (theme["danger"], theme["danger"])
            assert disabled == (theme["surface"], theme["text_muted"], theme["border"])
            assert active[2] == theme["success"]
    finally:
        widget.deleteLater()


def test_settings_theme_path_only_paints_cached_dependency_status() -> None:
    import inspect

    from freqinout.gui.settings_tab import SettingsTab

    source = inspect.getsource(SettingsTab.apply_theme)
    assert "_paint_running_status_snapshot" in source
    assert "_refresh_running_status" not in source
    assert "_selected_radio_status_snapshot" not in source
