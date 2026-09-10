"""LN-2 headless contract tests for Tools & Resources.

The workspace is intentionally imported lazily in these tests: the package is
allowed to land after LN-1, while the tests document the exact shell and
resource-service behavior expected once its UI module is present.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _resources_module():
    return pytest.importorskip("freqinout.gui.resources_tab")


def _resources_tab_class(module: Any):
    for name in ("ResourcesTab", "ToolsResourcesTab"):
        cls = getattr(module, name, None)
        if cls is not None:
            return cls
    pytest.fail("resources_tab must expose ResourcesTab")


def _app():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is not None and not isinstance(app, QApplication):
        pytest.skip("A non-GUI QCoreApplication is already active")
    return app or QApplication([])


def _make_tab(cls: Any, path: Path):
    """Construct against the small set of accepted Qt-free injection shapes."""

    from freqinout.core.resource_catalog_store import ResourceCatalogStore

    store = ResourceCatalogStore(path)
    attempts = (
        {"store": store},
        {"resource_store": store},
        {"db_path": path},
    )
    errors: list[Exception] = []
    for kwargs in attempts:
        try:
            return cls(**kwargs)
        except (TypeError, ValueError, RuntimeError) as exc:
            errors.append(exc)
    pytest.fail(f"could not construct ResourcesTab with supported injection shapes: {errors}")


def test_workspace_exposes_exact_functional_tabs_and_no_placeholder_callsigns() -> None:
    module = _resources_module()
    source = Path(module.__file__).read_text(encoding="utf-8")
    cls = _resources_tab_class(module)
    labels = getattr(cls, "TAB_LABELS", getattr(module, "TAB_LABELS", ()))
    if labels:
        assert tuple(labels) == ("Frequency Catalog", "Net Directory", "Import / Export")
    else:
        for label in ("Frequency Catalog", "Net Directory", "Import / Export"):
            assert label in source
    assert "Forms & Templates" not in source
    assert "K1ABC" not in source and "N0CALL" not in source


def test_query_only_opening_does_not_create_database_or_schema(tmp_path: Path) -> None:
    module = _resources_module()
    cls = _resources_tab_class(module)
    db_path = tmp_path / "not-created.db"
    _app()
    tab = _make_tab(cls, db_path)
    try:
        for method_name in ("refresh", "refresh_catalog", "load", "open"):
            method = getattr(tab, method_name, None)
            if callable(method):
                method()
                break
        assert not db_path.exists()
    finally:
        tab.deleteLater()


@pytest.mark.parametrize("size", [(900, 560), (1000, 700)])
def test_compact_workspace_has_no_page_level_horizontal_overflow(tmp_path: Path, size: tuple[int, int]) -> None:
    module = _resources_module()
    cls = _resources_tab_class(module)
    app = _app()
    tab = _make_tab(cls, tmp_path / f"resources-{size[0]}.db")
    try:
        from PySide6.QtCore import Qt
        from PySide6.QtWidgets import QAbstractScrollArea, QScrollArea

        tab.resize(*size)
        tab.show()
        app.processEvents()
        for scroll in tab.findChildren(QScrollArea):
            assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        for scroll in tab.findChildren(QAbstractScrollArea):
            assert scroll.horizontalScrollBar().maximum() == 0
    finally:
        tab.deleteLater()


def test_large_text_controls_fit_their_font_metrics(tmp_path: Path) -> None:
    module = _resources_module()
    cls = _resources_tab_class(module)
    app = _app()
    from PySide6.QtGui import QFont
    from PySide6.QtWidgets import QAbstractButton, QComboBox, QLabel, QLineEdit

    old_font = QFont(app.font())
    large_font = QFont(old_font)
    large_font.setPointSizeF(max(16.0, old_font.pointSizeF() * 1.35))
    app.setFont(large_font)
    tab = _make_tab(cls, tmp_path / "large-text.db")
    try:
        tab.resize(900, 560)
        tab.show()
        app.processEvents()
        for widget_type in (QAbstractButton, QComboBox, QLineEdit, QLabel):
            for control in tab.findChildren(widget_type):
                if not control.isVisible():
                    continue
                text = control.text() if hasattr(control, "text") else ""
                placeholder = control.placeholderText() if hasattr(control, "placeholderText") else ""
                if text or placeholder:
                    assert control.height() >= control.fontMetrics().height()
    finally:
        tab.deleteLater()
        app.setFont(old_font)


def test_results_are_bounded_and_lifecycle_actions_keep_used_by_guard(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_models import (
        CatalogSource,
        FrequencyResource,
        ReferencedResourceError,
    )
    from freqinout.core.resource_catalog_store import ResourceCatalogStore

    store = ResourceCatalogStore(tmp_path / "catalog.db")
    store.create_schema()
    store.create_source(CatalogSource("station:test", "Station", "station"))
    resource = FrequencyResource(
        "frequency_test", "station:test", "simplex", "AMATEUR", "Synthetic Channel",
        band="2M", receive_hz=146520000,
    )
    store.create_frequency(resource, group_keys=("operating_group_test",))
    assert len(store.list_frequencies(limit=10000)) <= 200
    store.update_frequency(resource, group_keys=("operating_group_test",))
    assert store.frequency_usage(resource.frequency_resource_key).total_references == 0
    # The lifecycle helper must refuse destructive removal once a consumer
    # references a resource, allowing UI Used-by details to remain truthful.
    with sqlite3.connect(tmp_path / "catalog.db") as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS local_net_schedules (frequency_resource_key TEXT)"
        )
        conn.execute("INSERT INTO local_net_schedules VALUES (?)", (resource.frequency_resource_key,))
    assert store.frequency_usage(resource.frequency_resource_key).is_referenced
    with pytest.raises(ReferencedResourceError):
        store.delete_frequency_if_unreferenced(resource.frequency_resource_key)
    assert store.retire_frequency(resource.frequency_resource_key).retired is True
