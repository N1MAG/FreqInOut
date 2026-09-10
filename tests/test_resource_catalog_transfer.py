from __future__ import annotations

import json

from freqinout.core.resource_catalog_models import CatalogSource, FrequencyResource, NetDirectoryEntry, NetDirectorySession
from freqinout.core.resource_catalog_store import ResourceCatalogStore
from freqinout.core.resource_catalog_transfer import (
    TRANSFER_SCHEMA_VERSION,
    apply_import_preview,
    export_selected_resources,
    preview_json_import,
)


def _store(path) -> ResourceCatalogStore:
    store = ResourceCatalogStore(path)
    store.create_schema()
    store.create_source(CatalogSource("station", "Station", "station"))
    return store


def _frequency(key="freq_2m"):
    return FrequencyResource(key, "station", "simplex", "amateur", "2m Calling", center_hz=146_520_000)


def test_export_net_includes_its_sessions_and_frequency_without_contact_or_secrets(tmp_path):
    store = _store(tmp_path / "catalog.db")
    frequency = store.create_frequency(_frequency())
    entry = store.create_net_entry(NetDirectoryEntry("net_county", "station", "County Net", contact_info="operator@example.test"))
    store.create_session(NetDirectorySession("session_county", entry.net_entry_key, "station", "amateur", frequency.frequency_resource_key, recurrence="Weekly"))

    exported = export_selected_resources(store, net_entry_keys=(entry.net_entry_key,))

    assert exported["schema_version"] == TRANSFER_SCHEMA_VERSION
    assert [item["frequency_resource_key"] for item in exported["frequencies"]] == [frequency.frequency_resource_key]
    assert [item["net_session_key"] for item in exported["sessions"]] == ["session_county"]
    assert "contact_info" not in json.dumps(exported)
    assert "password" not in json.dumps(exported).lower()


def test_preview_is_non_mutating_then_explicit_apply_creates_relationships(tmp_path):
    source = _store(tmp_path / "source.db")
    frequency = source.create_frequency(
        _frequency(), group_keys={"group_county": "County Group"}
    )
    entry = source.create_net_entry(
        NetDirectoryEntry("net_county", "station", "County Net"),
        group_keys={"group_county": "County Group"},
    )
    source.create_session(NetDirectorySession("session_county", entry.net_entry_key, "station", "amateur", frequency.frequency_resource_key))
    payload = export_selected_resources(source, net_entry_keys=(entry.net_entry_key,))
    target = _store(tmp_path / "target.db")

    preview = preview_json_import(target, json.dumps(payload), target_source_key="transfer_source")
    assert {item.status for item in preview.diagnostics} == {"new"}
    assert target.get_frequency(frequency.frequency_resource_key) is None
    results = apply_import_preview(target, preview)

    assert not [item for item in results if item.status in {"invalid", "ambiguous", "conflict"}]
    assert target.get_session("session_county").frequency_resource_key == frequency.frequency_resource_key
    assert target.frequency_group_links(frequency.frequency_resource_key) == (("group_county", "County Group"),)
    assert target.net_entry_group_links(entry.net_entry_key) == (("group_county", "County Group"),)


def test_preview_reports_duplicate_invalid_ambiguous_and_bundled_conflict_without_writes(tmp_path):
    store = _store(tmp_path / "catalog.db")
    store.create_source(CatalogSource("fcc", "FCC", "bundled", read_only=True))
    store.create_frequency(FrequencyResource("fcc_2m", "fcc", "simplex", "amateur", "Bundled", center_hz=146_520_000))
    document = {
        "schema_version": TRANSFER_SCHEMA_VERSION,
        "kind": "resource_catalog_transfer",
        "frequencies": [
            {"frequency_resource_key": "fcc_2m", "resource_kind": "simplex", "service": "amateur", "label": "attempt", "center_hz": 146_520_000},
            {"frequency_resource_key": "duplicate", "resource_kind": "simplex", "service": "gmrs", "label": "one", "center_hz": 462_550_000},
            {"frequency_resource_key": "duplicate", "resource_kind": "simplex", "service": "gmrs", "label": "two", "center_hz": 462_575_000},
            {"frequency_resource_key": "ambiguous", "resource_kind": "simplex", "service": "unknown", "label": "unknown", "center_hz": 1},
            {"frequency_resource_key": "invalid", "resource_kind": "band_range", "service": "amateur", "label": "bad"},
        ],
        "net_entries": [], "sessions": [],
    }
    preview = preview_json_import(store, document)

    assert {item.status for item in preview.diagnostics} >= {"conflict", "duplicate", "ambiguous", "invalid"}
    assert store.get_frequency("duplicate") is None


def test_preview_rejects_secrets_and_bounded_oversized_documents(tmp_path):
    store = _store(tmp_path / "catalog.db")
    secret = preview_json_import(store, {"schema_version": 1, "kind": "resource_catalog_transfer", "api_token": "nope"})
    oversized = preview_json_import(store, "{}", max_bytes=1)

    assert secret.diagnostics[0].status == "invalid"
    assert oversized.diagnostics[0].status == "invalid"


def test_preview_rejects_malformed_group_links_without_writing_item(tmp_path):
    store = _store(tmp_path / "catalog.db")
    document = {
        "schema_version": TRANSFER_SCHEMA_VERSION,
        "kind": "resource_catalog_transfer",
        "frequencies": [
            {
                "frequency_resource_key": "bad_links",
                "resource_kind": "simplex",
                "service": "amateur",
                "label": "Synthetic frequency",
                "center_hz": 146_520_000,
                "group_links": [{"group_name_snapshot": "Missing stable key"}],
            }
        ],
        "net_entries": [],
        "sessions": [],
    }

    preview = preview_json_import(store, document)
    assert any(item.status == "invalid" and item.item_key == "bad_links" for item in preview.diagnostics)
    apply_import_preview(store, preview)
    assert store.get_frequency("bad_links") is None


def test_import_without_group_links_preserves_existing_associations(tmp_path):
    store = _store(tmp_path / "catalog.db")
    current = store.create_frequency(
        _frequency(), group_keys={"group_county": "County Group"}
    )
    document = {
        "schema_version": TRANSFER_SCHEMA_VERSION,
        "kind": "resource_catalog_transfer",
        "frequencies": [
            {
                "frequency_resource_key": current.frequency_resource_key,
                "resource_kind": current.resource_kind,
                "service": current.service,
                "label": "Updated synthetic frequency",
                "center_hz": current.center_hz,
            }
        ],
        "net_entries": [],
        "sessions": [],
    }

    preview = preview_json_import(store, document)
    apply_import_preview(store, preview)

    assert store.get_frequency(current.frequency_resource_key).label == "Updated synthetic frequency"
    assert store.frequency_group_links(current.frequency_resource_key) == (
        ("group_county", "County Group"),
    )
