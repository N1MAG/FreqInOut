"""Focused R-1 qualification checks for Resources navigation and transfer.

The tests in this module stay on isolated temporary catalog databases.  They
cover the R-1 exit gate without importing Shortwave data or exercising any
radio/scheduler path.
"""

from __future__ import annotations

import ast
import json
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from freqinout.core.navigation_intent import NavigationIntent
from freqinout.core.resource_catalog_models import (
    CatalogSource,
    FrequencyResource,
    NetDirectoryEntry,
    NetDirectorySession,
)
from freqinout.core.resource_catalog_store import ResourceCatalogStore
from freqinout.core.resource_catalog_transfer import (
    TRANSFER_SCHEMA_VERSION,
    apply_import_preview,
    confirm_export_preview,
    export_selected_resources,
    preview_json_import,
    preview_resource_export,
    StaleExportPreviewError,
)


def _catalog(path: Path) -> ResourceCatalogStore:
    store = ResourceCatalogStore(path)
    store.create_schema()
    store.create_source(CatalogSource("station", "Synthetic Station", "station"))
    return store


def _seed_catalog(
    path: Path,
    *,
    two_frequencies: bool = False,
) -> tuple[ResourceCatalogStore, FrequencyResource, NetDirectoryEntry, NetDirectorySession]:
    store = _catalog(path)
    frequency = store.create_frequency(
        FrequencyResource(
            "frequency_r1_primary",
            "station",
            "simplex",
            "AMATEUR",
            "R-1 primary frequency",
            band="40M",
            receive_hz=7_115_000,
            transmit_hz=7_115_000,
        ),
        group_keys={"group_alpha": "Alpha Group"},
    )
    if two_frequencies:
        store.create_frequency(
            FrequencyResource(
                "frequency_r1_secondary",
                "station",
                "simplex",
                "AMATEUR",
                "R-1 secondary frequency",
                band="20M",
                receive_hz=14_110_000,
                transmit_hz=14_110_000,
            ),
            group_keys={"group_alpha": "Alpha Group"},
        )
    entry = store.create_net_entry(
        NetDirectoryEntry("net_r1", "station", "R-1 Directory Net"),
        group_keys={"group_alpha": "Alpha Group"},
    )
    session = store.create_session(
        NetDirectorySession(
            "session_r1",
            entry.net_entry_key,
            "station",
            "AMATEUR",
            frequency_resource_key=frequency.frequency_resource_key,
            recurrence="Weekly",
            local_start_time="20:00",
            timezone="UTC",
            day_utc="Wednesday",
        )
    )
    return store, frequency, entry, session


def _item_count(payload: dict[str, object]) -> int:
    return sum(len(payload.get(name, ())) for name in ("frequencies", "net_entries", "sessions"))


def test_export_dependency_closure_preserves_relationships_and_excludes_station_schedules(tmp_path: Path) -> None:
    store, frequency, entry, session = _seed_catalog(tmp_path / "source.db")

    payload = export_selected_resources(store, net_entry_keys=(entry.net_entry_key,))

    assert payload["schema_version"] == TRANSFER_SCHEMA_VERSION
    assert [row["net_entry_key"] for row in payload["net_entries"]] == [entry.net_entry_key]
    assert [row["net_session_key"] for row in payload["sessions"]] == [session.net_session_key]
    assert [row["frequency_resource_key"] for row in payload["frequencies"]] == [frequency.frequency_resource_key]
    assert payload["frequencies"][0]["group_links"] == [
        {"operating_group_key": "group_alpha", "group_name_snapshot": "Alpha Group"}
    ]
    assert payload["net_entries"][0]["group_links"] == [
        {"operating_group_key": "group_alpha", "group_name_snapshot": "Alpha Group"}
    ]
    # Station-owned HF/Local schedules are intentionally outside the transfer
    # scope; their usage belongs in the human preview, not the JSON payload.
    assert not {"hf_schedule", "local_schedule", "schedules"}.intersection(payload)


def test_export_preview_is_immutable_and_confirmation_is_the_only_write_boundary(tmp_path: Path) -> None:
    store, frequency, entry, _session = _seed_catalog(tmp_path / "catalog.db")

    preview = preview_resource_export(store, net_entry_keys=(entry.net_entry_key,))
    assert preview.total_count == 3
    assert preview.direct_count == 1
    assert preview.dependency_count == 2
    assert preview.payload_size == len(preview.payload_json)
    assert preview.payload_sha256
    assert preview.items[0].source_label == "Synthetic Station"
    assert any(item.item_key == frequency.frequency_resource_key for item in preview.items)

    with pytest.raises(FrozenInstanceError):
        preview.max_items = 1  # type: ignore[misc]
    with pytest.raises(AttributeError):
        preview.items.append(object())  # type: ignore[attr-defined]

    # Building and confirming the review token never writes a destination;
    # callers receive the exact reviewed payload and choose where to save it.
    destination = tmp_path / "not_written_until_ui_confirm.db"
    assert not destination.exists()
    confirmed = confirm_export_preview(store, preview)
    assert confirmed == preview.payload()
    assert not destination.exists()


def test_export_preview_stale_revalidation_requires_review_again(tmp_path: Path) -> None:
    store, _frequency, entry, _session = _seed_catalog(tmp_path / "catalog.db")
    preview = preview_resource_export(store, net_entry_keys=(entry.net_entry_key,))

    current = store.get_net_entry(entry.net_entry_key)
    assert current is not None
    store.update_net_entry(NetDirectoryEntry(current.net_entry_key, current.source_key, "Concurrent directory edit"))

    with pytest.raises(StaleExportPreviewError, match="changed after preview"):
        confirm_export_preview(store, preview)


def test_matching_aggregate_limit_round_trips_and_over_limit_import_is_non_mutating(tmp_path: Path) -> None:
    source, _frequency, entry, _session = _seed_catalog(tmp_path / "source.db")
    payload = export_selected_resources(source, net_entry_keys=(entry.net_entry_key,))
    assert _item_count(payload) == 3

    target = _catalog(tmp_path / "target.db")
    preview = preview_json_import(target, json.dumps(payload), target_source_key="transfer", max_items=3)
    assert preview.actionable
    assert not [item for item in preview.diagnostics if item.status in {"invalid", "ambiguous", "conflict"}]
    results = apply_import_preview(target, preview)
    assert not [item for item in results if item.status in {"invalid", "ambiguous", "conflict"}]
    assert target.get_net_entry(entry.net_entry_key) is not None

    # An aggregate limit applies to the whole bundle, not independently to
    # each collection.  Rejected previews must not partially import rows.
    broad_source, _f, broad_entry, _s = _seed_catalog(tmp_path / "broad.db", two_frequencies=True)
    broad_payload = export_selected_resources(
        broad_source,
        frequency_resource_keys=("frequency_r1_secondary",),
        net_entry_keys=(broad_entry.net_entry_key,),
    )
    assert _item_count(broad_payload) == 4
    rejected_target = _catalog(tmp_path / "rejected.db")
    rejected = preview_json_import(rejected_target, broad_payload, target_source_key="transfer", max_items=3)
    assert rejected.diagnostics[0].status == "invalid"
    assert rejected_target.get_net_entry(broad_entry.net_entry_key) is None
    assert rejected_target.list_frequencies(active=None) == ()


def test_import_preview_is_immutable_and_apply_revalidates_stale_catalog(tmp_path: Path) -> None:
    source, frequency, entry, _session = _seed_catalog(tmp_path / "source.db")
    payload = export_selected_resources(source, net_entry_keys=(entry.net_entry_key,))
    target, _target_frequency, _target_entry, _target_session = _seed_catalog(tmp_path / "target.db")

    preview = preview_json_import(target, payload, target_source_key="transfer")
    with pytest.raises(FrozenInstanceError):
        preview.target_source = CatalogSource("other", "Other", "station")  # type: ignore[misc]
    with pytest.raises(TypeError):
        preview.expected_version_hashes[frequency.frequency_resource_key] = "changed"  # type: ignore[index]

    # A concurrent catalog edit invalidates the immutable preview and cannot
    # be silently overwritten at the apply boundary.
    existing = target.get_frequency(frequency.frequency_resource_key)
    assert existing is not None
    target.update_frequency(
        FrequencyResource(
            existing.frequency_resource_key,
            existing.source_key,
            existing.resource_kind,
            existing.service,
            "Concurrent edit",
            band=existing.band,
            receive_hz=existing.receive_hz,
            transmit_hz=existing.transmit_hz,
        )
    )
    results = apply_import_preview(target, preview)
    assert any(item.status == "conflict" and item.item_key == frequency.frequency_resource_key for item in results)
    assert target.get_frequency(frequency.frequency_resource_key).label == "Concurrent edit"


def test_cancelled_export_preview_does_not_write_or_leave_apply_enabled(tmp_path: Path) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.resources_tab import ResourceImportExportView

    source, _frequency, entry, _session = _seed_catalog(tmp_path / "source.db")
    app = QApplication.instance() or QApplication([])
    view = ResourceImportExportView(source)
    try:
        # The view receives a preview object only; cancellation is the write
        # boundary's no-op path and must not touch the catalog.
        view.set_export_selection(net_entry_keys=(entry.net_entry_key,))
        view.review_export()
        assert view._export_preview is not None
        view.cancel_export_preview()
        app.processEvents()
        assert view._export_preview is None
        assert not view.save_reviewed_export_btn.isEnabled()
        assert not view.cancel_export_preview_btn.isEnabled()
        assert source.get_net_entry(entry.net_entry_key) is not None
        assert source.get_session("session_r1") is not None
    finally:
        view.close()
        view.deleteLater()
        app.processEvents()


def test_export_surface_is_preview_first_and_hides_raw_keys_by_default(tmp_path: Path) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication, QLineEdit, QPushButton

    from freqinout.gui.resources_tab import ResourceImportExportView

    store, _frequency, _entry, _session = _seed_catalog(tmp_path / "catalog.db")
    app = QApplication.instance() or QApplication([])
    view = ResourceImportExportView(store)
    try:
        view.show()
        app.processEvents()
        button_text = {button.text().replace("…", "...") for button in view.findChildren(QPushButton)}
        assert any(text.startswith("Review export") for text in button_text)
        assert not any(text.startswith("Export Selected") for text in button_text)
        # Stable-key paste is an advanced compatibility path, not the default
        # operator-facing export interaction.
        visible_placeholders = {
            edit.placeholderText()
            for edit in view.findChildren(QLineEdit)
            if edit.isVisible() and edit.placeholderText()
        }
        assert not any("keys, comma separated" in placeholder for placeholder in visible_placeholders)
    finally:
        view.close()
        view.deleteLater()
        app.processEvents()


def test_resources_master_navigation_has_frequencies_child_and_no_dead_shortwave_route() -> None:
    from freqinout.gui.main_window import MainWindow

    source = Path(__file__).resolve().parents[1] / "freqinout" / "gui" / "main_window.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    nav_specs: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Attribute) and target.attr == "_nav_specs" for target in node.targets):
            continue
        if isinstance(node.value, ast.List):
            for item in node.value.elts:
                try:
                    label, target = ast.literal_eval(item)
                except (TypeError, ValueError):
                    continue
                nav_specs.append((str(label), str(target)))

    # The visible child is Frequencies; the Resources master is rendered by
    # the group header.  Internal catalog tabs are not main-nav destinations.
    assert [(label, target) for label, target in nav_specs if target == "Resources"] == [("Frequencies", "Resources")]
    assert all(
        label not in {"Frequency Catalog", "Net Directory", "Import / Export", "Shortwave"}
        for label, _ in nav_specs
    )
    compact = MainWindow._compact_navigation_specs()
    assert [(label, target) for label, _accessible, target, _icon in compact if target == "Resources"] == [
        ("Resources", "Resources")
    ]

    text = source.read_text(encoding="utf-8")
    assert 'self._nav_group_order: list[str]' in text and '"Resources"' in text
    assert "Frequencies" in text, "Resources must expose its Frequencies child in the full/compact route"


def test_resources_contextual_deep_links_preserve_internal_tabs_and_return_intent(tmp_path: Path) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.resources_tab import ResourcesTab

    app = QApplication.instance() or QApplication([])
    tab = ResourcesTab(db_path=tmp_path / "resources.db")
    returned: list[NavigationIntent] = []
    intent = NavigationIntent(
        origin_surface="local_nets_editor",
        destination_route="resources.net_directory",
        return_route="plans.local_nets",
        return_query="synthetic",
        return_selection_key="net_r1",
    )
    tab.return_requested.connect(returned.append)
    try:
        tab.set_navigation_intent(intent)
        assert tab.open_section("net_directory") is not None
        assert tab.tabs.currentIndex() == 1
        assert tuple(tab.tabs.tabText(i) for i in range(tab.tabs.count())) == (
            "Frequency Catalog",
            "Net Directory",
            "Import / Export",
        )
        tab.return_btn.click()
        app.processEvents()
        assert returned == [intent]
        assert tab.context_bar.isVisible() is False
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_frequency_catalog_supports_multi_select_without_changing_detail_selection(tmp_path: Path) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QApplication, QAbstractScrollArea, QAbstractItemView

    from freqinout.gui.frequency_catalog_view import FrequencyCatalogView

    store, _frequency, _entry, _session = _seed_catalog(tmp_path / "catalog.db", two_frequencies=True)
    app = QApplication.instance() or QApplication([])
    view = FrequencyCatalogView(store)
    try:
        view.resize(900, 560)
        view.show()
        app.processEvents()
        assert view.table.selectionMode() == QAbstractItemView.SingleSelection
        assert view.table.horizontalHeaderItem(0).text() == "Export"
        assert view.table.item(0, 0).flags() & Qt.ItemIsUserCheckable
        view.table.item(0, 0).setCheckState(Qt.Checked)
        view.table.item(1, 0).setCheckState(Qt.Checked)
        assert view.review_export_btn.isEnabled()
        assert "2 frequencies" in view.export_selection_label.text()
        view.table.selectRow(1)
        assert len(view.table.selectionModel().selectedRows()) == 1
        assert view._selected is not None
        assert view.table.accessibleName() or view.table.toolTip()
        for scroll in view.findChildren(QAbstractScrollArea):
            assert scroll.horizontalScrollBar().maximum() == 0
    finally:
        view.close()
        view.deleteLater()
        app.processEvents()
