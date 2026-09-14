from __future__ import annotations

import pytest

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QScrollArea,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from tests.ui_audit_harness import (
    GeometryAuditError,
    assert_font_derived_vertical_minimum,
    assert_geometry_settles,
    assert_lazy_page_theme_lifecycle,
    assert_page_horizontal_scroll_policy,
    assert_tab_bar_font_room,
    assert_table_tree_headers_and_rows,
    ensure_qapplication,
    font_derived_minimum_height,
    settle_geometry,
)


def _app() -> QApplication:
    return ensure_qapplication()


def _show(widget: QWidget, width: int = 640, height: int = 360) -> QWidget:
    widget.resize(width, height)
    widget.show()
    settle_geometry(widget)
    return widget


def test_font_minimum_is_derived_from_active_font_and_passes() -> None:
    app = _app()
    label = _show(QLabel("Accessible text"))
    try:
        required = font_derived_minimum_height(label)
        label.resize(label.width(), required)
        assert assert_font_derived_vertical_minimum(label) == required
    finally:
        label.deleteLater()
        app.processEvents()


def test_font_minimum_reports_under_sized_widget() -> None:
    app = _app()
    label = _show(QLabel("Text"))
    try:
        required = font_derived_minimum_height(label)
        label.setFixedHeight(max(1, required - 1))
        with pytest.raises(GeometryAuditError, match="font-derived minimum"):
            assert_font_derived_vertical_minimum(label)
    finally:
        label.deleteLater()
        app.processEvents()


def test_table_headers_and_rows_pass_without_pixel_fixture() -> None:
    app = _app()
    table = QTableWidget(2, 2)
    table.setHorizontalHeaderLabels(["Name", "State"])
    table.setItem(0, 0, QTableWidgetItem("Radio"))
    table.setItem(0, 1, QTableWidgetItem("Ready"))
    table.setItem(1, 0, QTableWidgetItem("Mesh"))
    table.setItem(1, 1, QTableWidgetItem("Idle"))
    _show(table)
    try:
        assert_table_tree_headers_and_rows(table)
    finally:
        table.deleteLater()
        app.processEvents()


def test_table_row_failure_is_explicit() -> None:
    app = _app()
    table = QTableWidget(1, 1)
    table.setHorizontalHeaderLabels(["Name"])
    table.setItem(0, 0, QTableWidgetItem("Too short"))
    table.verticalHeader().setMinimumSectionSize(1)
    table.setRowHeight(0, 1)
    _show(table)
    try:
        with pytest.raises(GeometryAuditError, match="row 0"):
            assert_table_tree_headers_and_rows(table)
    finally:
        table.deleteLater()
        app.processEvents()


def test_tree_headers_and_nested_rows_pass() -> None:
    app = _app()
    tree = QTreeWidget()
    tree.setColumnCount(1)
    tree.setHeaderLabels(["Destination"])
    root = QTreeWidgetItem(tree, ["Group"])
    QTreeWidgetItem(root, ["Radio"])
    tree.expandAll()
    _show(tree)
    try:
        assert_table_tree_headers_and_rows(tree)
    finally:
        tree.deleteLater()
        app.processEvents()


def test_tab_bar_font_room_passes_and_reports_short_tabs() -> None:
    app = _app()
    tabs = QTabWidget()
    tabs.addTab(QLabel("Overview"), "Overview")
    tabs.addTab(QLabel("Settings"), "Settings")
    _show(tabs)
    try:
        assert_tab_bar_font_room(tabs, expected_labels=["Overview", "Settings"])
        with pytest.raises(GeometryAuditError, match="tab 0"):
            assert_tab_bar_font_room(tabs, minimum_tab_height=tabs.tabBar().height() + 1)
    finally:
        tabs.deleteLater()
        app.processEvents()


def test_geometry_settling_is_bounded_and_stable() -> None:
    app = _app()
    page = _show(QLabel("Stable geometry"))
    try:
        snapshot = assert_geometry_settles(page)
        assert snapshot.geometry.width() > 0
        assert snapshot.geometry.height() > 0
    finally:
        page.deleteLater()
        app.processEvents()


def test_page_horizontal_scroll_policy_distinguishes_local_overflow() -> None:
    app = _app()
    page = QWidget()
    layout = QVBoxLayout(page)
    page_scroll = QScrollArea(page)
    page_scroll.setProperty("fio_scroll_owner", "page")
    page_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    local_table_scroll = QScrollArea(page)
    local_table_scroll.setProperty("fio_scroll_owner", "local")
    local_table_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    layout.addWidget(page_scroll)
    layout.addWidget(local_table_scroll)
    _show(page)
    try:
        assert_page_horizontal_scroll_policy(page)
        page_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        with pytest.raises(GeometryAuditError, match="page scrolling"):
            assert_page_horizontal_scroll_policy(page)
    finally:
        page.deleteLater()
        app.processEvents()


def test_lazy_page_theme_lifecycle_is_injected_and_bounded() -> None:
    app = _app()
    host = QWidget()
    host_layout = QVBoxLayout(host)
    _show(host)
    seen_themes: list[str] = []

    def load() -> QWidget:
        page = QLabel("Deferred content", host)
        host_layout.addWidget(page)
        return page

    def apply_theme(page: QWidget, theme: str) -> None:
        seen_themes.append(theme)
        page.setProperty("fio_theme", theme)

    try:
        page = assert_lazy_page_theme_lifecycle(host, load, apply_theme)
        assert page.property("fio_lazy_page_loaded") is True
        assert seen_themes == ["light", "light", "light", "dark", "dark", "dark"]
    finally:
        host.deleteLater()
        app.processEvents()


def test_lazy_page_theme_lifecycle_rejects_unattached_page() -> None:
    app = _app()
    host = _show(QWidget())
    try:
        with pytest.raises(GeometryAuditError, match="not attached"):
            assert_lazy_page_theme_lifecycle(host, lambda: QLabel("orphan"), lambda *_: None)
    finally:
        host.deleteLater()
        app.processEvents()
