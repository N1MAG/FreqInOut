"""User-facing Resources contracts for the Frequency Catalog and Net Directory.

These checks intentionally distinguish presentation from the canonical catalog
schema.  Stable storage keys and hashes remain useful to services and transfer
code, but they are not part of the normal operator browsing experience.
"""

from __future__ import annotations

import ast
import os
import re
import time
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _app():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    return app or QApplication([])


def _settle_catalog_snapshot(app, *views) -> None:
    """Allow the bounded Resources snapshot worker to deliver its GUI update."""
    from PySide6.QtTest import QTest

    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        app.processEvents()
        QTest.qWait(5)
        if all((getattr(view, "table", None) or getattr(view, "entry_table", None)).rowCount() > 0 for view in views):
            return


def _catalog(tmp_path: Path):
    from freqinout.core.resource_catalog_models import (
        CatalogSource,
        FrequencyResource,
        NetDirectoryEntry,
        NetDirectorySession,
    )
    from freqinout.core.resource_catalog_store import ResourceCatalogStore

    store = ResourceCatalogStore(tmp_path / "catalog.db")
    store.create_schema()
    store.create_source(CatalogSource("source_internal", "Synthetic reference", "station"))
    frequency = store.create_frequency(
        FrequencyResource(
            "frequency_internal",
            "source_internal",
            "simplex",
            "AMATEUR",
            "Synthetic 40 meter channel",
            band="40M",
            receive_hz=7_115_000,
            transmit_hz=7_115_000,
            content_version="internal-version",
            version_hash="internal-frequency-hash",
        )
    )
    entry = store.create_net_entry(
        NetDirectoryEntry(
            "net_internal",
            "source_internal",
            "Synthetic Net",
            description="A synthetic directory entry",
            scope="Western",
            version_hash="internal-net-hash",
        )
    )
    store.create_session(
        NetDirectorySession(
            "net_schedule_internal",
            entry.net_entry_key,
            "source_internal",
            "AMATEUR",
            frequency_resource_key=frequency.frequency_resource_key,
            recurrence="Weekly",
            day_utc="Wednesday",
            local_start_time="20:00",
            timezone="UTC",
        )
    )
    return store


def _table_headers(table) -> tuple[str, ...]:
    return tuple(
        table.horizontalHeaderItem(index).text()
        for index in range(table.columnCount())
        if table.horizontalHeaderItem(index) is not None
    )


def _visible_text(widget) -> list[str]:
    """Collect visible operator-facing text, including table headers/cells."""

    from PySide6.QtWidgets import QAbstractButton, QComboBox, QLabel, QLineEdit, QTableWidget

    values: list[str] = []
    for child_type in (QAbstractButton, QComboBox, QLabel, QLineEdit):
        for child in widget.findChildren(child_type):
            if not child.isVisible():
                continue
            text = child.currentText() if isinstance(child, QComboBox) else child.text() if hasattr(child, "text") else ""
            placeholder = child.placeholderText() if isinstance(child, QLineEdit) else ""
            values.extend(value for value in (text, placeholder) if value)
    for table in widget.findChildren(QTableWidget):
        if not table.isVisible():
            continue
        values.extend(_table_headers(table))
        for row in range(table.rowCount()):
            for column in range(table.columnCount()):
                item = table.item(row, column)
                if item is not None and item.text():
                    values.append(item.text())
    return values


_INTERNAL_IDENTIFIER_LABEL = re.compile(
    r"\b(?:source|resource|net|schedule|meeting)\s*(?:key|id)\b|\bversion(?:\s+hash)?\b|\bhash\b",
    re.IGNORECASE,
)


def _assert_operator_surface_hides_catalog_internals(widget, *raw_values: str) -> None:
    visible = _visible_text(widget)
    text = " ".join(visible)
    assert not _INTERNAL_IDENTIFIER_LABEL.search(text), visible
    for value in raw_values:
        assert value not in text, (value, visible)


def test_main_navigation_has_one_resources_entry_and_compact_route() -> None:
    """Resources is a single master destination; catalog sections stay inside it."""

    from freqinout.gui.main_window import MainWindow

    source = Path(__file__).resolve().parents[1] / "freqinout" / "gui" / "main_window.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    nav_items: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Attribute) and target.attr == "_nav_specs" for target in node.targets):
            continue
        if not isinstance(node.value, ast.List):
            continue
        for item in node.value.elts:
            try:
                label, target = ast.literal_eval(item)
            except (ValueError, TypeError):
                continue
            nav_items.append((str(label), str(target)))

    resources = [(label, target) for label, target in nav_items if target == "Resources"]
    # Resources is the master group header; Frequencies is its only R-1 child.
    assert resources == [("Frequencies", "Resources")]
    assert all(label not in {"Frequency Catalog", "Net Directory", "Import / Export"} for label, _ in nav_items)

    source_text = source.read_text(encoding="utf-8")
    assert 'for resource_section in ("frequency_catalog", "net_directory", "import_export")' in source_text

    compact = MainWindow._compact_navigation_specs()
    assert [(label, target) for label, _accessible, target, _icon in compact if target == "Resources"] == [
        ("Resources", "Resources")
    ]


def test_frequency_display_uses_decimal_mhz_without_thousands_separator(tmp_path: Path) -> None:
    from freqinout.gui.frequency_catalog_view import FrequencyCatalogView
    from freqinout.gui.resource_picker import frequency_where_text

    store = _catalog(tmp_path)
    resource = store.get_frequency("frequency_internal")
    assert resource is not None
    assert frequency_where_text(resource) == "7.115 MHz"
    assert "," not in frequency_where_text(resource)

    app = _app()
    view = FrequencyCatalogView(store)
    try:
        view.show()
        app.processEvents()
        _settle_catalog_snapshot(app, view)
        assert view.table.item(0, 3).text() == "7.115 MHz"
    finally:
        view.close()
        view.deleteLater()
        app.processEvents()


def test_normal_catalog_views_hide_source_keys_and_version_hashes(tmp_path: Path) -> None:
    from freqinout.gui.frequency_catalog_view import FrequencyCatalogView
    from freqinout.gui.net_directory_view import NetDirectoryView

    store = _catalog(tmp_path)
    app = _app()
    frequency_view = FrequencyCatalogView(store)
    net_view = NetDirectoryView(store)
    try:
        for view in (frequency_view, net_view):
            view.show()
        app.processEvents()
        _settle_catalog_snapshot(app, frequency_view, net_view)

        frequency_text = " ".join(_visible_text(frequency_view))
        assert "source_internal" not in frequency_text
        assert "internal-frequency-hash" not in frequency_text
        assert "Synthetic reference" in frequency_text
        assert all("key" not in header.casefold() for header in _table_headers(frequency_view.table))
        assert "Listing" in _table_headers(frequency_view.table)
        assert any(
            frequency_view.status_filter.itemText(index).startswith("Listing: Listed")
            for index in range(frequency_view.status_filter.count())
        )
        assert "Version" not in frequency_text
        assert not any(text.strip() == "Active" for text in _visible_text(frequency_view))
        source_filter = getattr(frequency_view, "source_filter", None)
        assert source_filter is None or not source_filter.isVisible()

        net_text = " ".join(_visible_text(net_view))
        assert "source_internal" not in net_text
        assert "internal-net-hash" not in net_text
        assert "Synthetic reference" in net_text
        assert all("key" not in header.casefold() for header in _table_headers(net_view.entry_table))
        assert "Listing" in _table_headers(net_view.entry_table)
        assert "Version" not in net_text
        assert not any(text.strip() == "Active" for text in _visible_text(net_view))

        # New/edit workflows remain operator-facing. Internal identity fields
        # are retained in the model but are not shown as raw IDs or hashes.
        frequency_view.begin_edit()
        app.processEvents()
        _assert_operator_surface_hides_catalog_internals(
            frequency_view,
            "source_internal",
            "frequency_internal",
            "internal-frequency-hash",
            "internal-version",
        )
        assert not frequency_view.key_edit.isVisible()
        frequency_view.cancel_editor()
        frequency_view.begin_new()
        app.processEvents()
        _assert_operator_surface_hides_catalog_internals(frequency_view)
        assert not frequency_view.key_edit.isVisible()
        frequency_view.cancel_editor()

        net_view.begin_edit_entry()
        app.processEvents()
        _assert_operator_surface_hides_catalog_internals(
            net_view,
            "source_internal",
            "net_internal",
            "internal-net-hash",
        )
        assert not net_view.entry_key_edit.isVisible()
        net_view.entry_editor.setVisible(False)
        net_view.begin_new_entry()
        app.processEvents()
        _assert_operator_surface_hides_catalog_internals(net_view)
        assert not net_view.entry_key_edit.isVisible()
        net_view.entry_editor.setVisible(False)

        net_view.session_table.selectRow(0)
        app.processEvents()
        net_view.begin_edit_session()
        app.processEvents()
        _assert_operator_surface_hides_catalog_internals(
            net_view,
            "source_internal",
            "net_schedule_internal",
            "frequency_internal",
        )
        assert not net_view.session_key_edit.isVisible()
        assert not net_view.session_frequency_edit.isVisible()
        net_view.session_editor.setVisible(False)
        net_view.begin_new_session()
        app.processEvents()
        _assert_operator_surface_hides_catalog_internals(net_view)
        assert not net_view.session_key_edit.isVisible()
        assert not net_view.session_frequency_edit.isVisible()
    finally:
        frequency_view.close()
        net_view.close()
        frequency_view.deleteLater()
        net_view.deleteLater()
        app.processEvents()


def test_net_directory_uses_region_listing_and_net_terminology(tmp_path: Path) -> None:
    from freqinout.gui.net_directory_view import NetDirectoryView

    store = _catalog(tmp_path)
    app = _app()
    view = NetDirectoryView(store)
    try:
        view.show()
        app.processEvents()
        _settle_catalog_snapshot(app, view)
        headers = _table_headers(view.entry_table)
        assert "Region" in headers
        assert "Scope" not in headers
        assert "Listing" in headers
        status_items = [view.status_filter.itemText(index) for index in range(view.status_filter.count())]
        assert any("Listed" in item for item in status_items)
        assert all("Active" not in item for item in status_items)

        visible = _visible_text(view)
        assert not any("session" in text.casefold() for text in visible)
        assert not any("scope" in text.casefold() for text in visible)
        assert any("Western" in text for text in visible)
    finally:
        view.close()
        view.deleteLater()
        app.processEvents()


def test_resources_workspace_has_no_horizontal_overflow_in_compact_dark_mode(tmp_path: Path) -> None:
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QAbstractScrollArea, QScrollArea

    from freqinout.gui.resources_tab import ResourcesTab
    from freqinout.gui.theme import apply_app_theme, get_theme

    app = _app()
    prior_style = app.styleSheet()
    apply_app_theme(app, get_theme("dark"))
    tab = ResourcesTab(db_path=tmp_path / "resources.db")
    try:
        tab.resize(900, 560)
        tab.show()
        app.processEvents()
        for scroll in tab.findChildren(QScrollArea):
            assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        for scroll in tab.findChildren(QAbstractScrollArea):
            assert scroll.horizontalScrollBar().maximum() == 0
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()
        app.setStyleSheet(prior_style)
