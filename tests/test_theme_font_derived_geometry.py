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
    QPlainTextEdit,
    QRadioButton,
    QSpinBox,
    QTabBar,
    QTableWidget,
    QTreeWidget,
    QWidget,
)

from freqinout.gui.theme import (
    active_app_theme,
    apply_app_theme,
    apply_text_size_accessibility_guards,
    contrast_text_for_background,
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
            )
            assert widget.minimumHeight() >= target
            assert widget.maximumHeight() >= target
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


def test_settings_theme_path_only_paints_cached_dependency_status() -> None:
    import inspect

    from freqinout.gui.settings_tab import SettingsTab

    source = inspect.getsource(SettingsTab.apply_theme)
    assert "_paint_running_status_snapshot" in source
    assert "_refresh_running_status" not in source
    assert "_selected_radio_status_snapshot" not in source
