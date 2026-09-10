from __future__ import annotations

import dataclasses
import math
import sqlite3
import time

import pytest

from freqinout.core.resource_catalog_models import (
    CatalogSource,
    FrequencyResource,
    NetDirectoryEntry,
    NetDirectorySession,
    ReadOnlyResourceError,
    ReferencedResourceError,
)
from freqinout.core.resource_catalog_store import CATALOG_TABLES, MAX_RESULTS, ResourceCatalogStore


def _station_source(store: ResourceCatalogStore) -> None:
    store.create_source(CatalogSource("source_station", "Station", "station"))


def _frequency(key: str = "frequency_2m") -> FrequencyResource:
    return FrequencyResource(key, "source_station", "simplex", "amateur", "2m Calling", center_hz=146_520_000, locality="County")


def test_query_methods_do_not_create_a_database_or_schema(tmp_path):
    path = tmp_path / "freqinout_nets.db"
    store = ResourceCatalogStore(path)

    assert store.list_frequencies(search="calling") == ()
    assert store.get_frequency("frequency_2m") is None
    assert store.list_sources() == ()
    assert store.sources_by_keys(("source_missing",)) == {}
    assert not path.exists()

    store.create_schema()
    with sqlite3.connect(path) as conn:
        names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert set(CATALOG_TABLES) <= names
    assert "net_resources" not in names


def test_query_methods_do_not_write_an_existing_database_without_catalog_schema(tmp_path):
    path = tmp_path / "existing-but-uninitialized.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE unrelated_rows (value TEXT)")
        conn.execute("INSERT INTO unrelated_rows(value) VALUES ('preserve me')")

    before = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns
    store = ResourceCatalogStore(path)

    assert store.get_source("source_missing") is None
    assert store.list_sources() == ()
    assert store.sources_by_keys(("source_missing",)) == {}
    assert store.get_frequency("frequency_missing") is None
    assert store.list_frequencies(search="anything") == ()
    assert store.list_net_entries(search="anything") == ()
    assert store.list_sessions(net_entry_key="net_missing") == ()
    assert store.frequency_usage("frequency_missing").total_references == 0

    assert path.read_bytes() == before
    assert path.stat().st_mtime_ns == before_mtime
    assert not path.with_name(f"{path.name}-wal").exists()
    assert not path.with_name(f"{path.name}-shm").exists()


def test_frequency_crud_hash_diff_usage_retire_and_unreferenced_delete(tmp_path):
    store = ResourceCatalogStore(tmp_path / "freqinout_nets.db")
    store.create_schema()
    _station_source(store)
    created = store.create_frequency(_frequency(), group_keys=("group_county",))

    assert created.center_hz == 146_520_000
    assert store.sources_by_keys(("source_station", "missing"))["source_station"].label == "Station"
    assert [source.label for source in store.list_sources()] == ["Station"]
    created = store.update_frequency(created, group_keys={"group_county": "County Group"})
    assert store.frequency_group_links(created.frequency_resource_key) == (("group_county", "County Group"),)
    assert created.content_hash and created.version_hash
    assert store.list_frequencies(search="COUNTY", limit=999) == (created,)
    assert store.list_frequencies(limit=999).count(created) == 1

    updated = store.update_frequency(dataclasses.replace(created, label="County Call"))
    comparison = store.compare_frequency_version(created.frequency_resource_key, created.version_hash, {"label": created.label, "center_hz": created.center_hz})
    assert updated.version_hash != created.version_hash
    assert comparison.update_available is True
    assert comparison.diffs == (comparison.diffs[0],)
    assert comparison.diffs[0].field_name == "label"

    entry = store.create_net_entry(NetDirectoryEntry("net_county", "source_station", "County Net"))
    store.create_session(NetDirectorySession("session_county", entry.net_entry_key, "source_station", "amateur", updated.frequency_resource_key, recurrence="Weekly"))
    assert store.frequency_usage(updated.frequency_resource_key).by_kind == {"directory_sessions": 1}
    with pytest.raises(ReferencedResourceError):
        store.delete_frequency_if_unreferenced(updated.frequency_resource_key)
    retired = store.retire_frequency(updated.frequency_resource_key)
    assert retired.retired and not retired.active

    solo = store.create_frequency(_frequency("frequency_solo"))
    store.delete_frequency_if_unreferenced(solo.frequency_resource_key)
    assert store.get_frequency(solo.frequency_resource_key) is None


def test_bundled_records_are_created_but_not_mutable_or_deletable(tmp_path):
    store = ResourceCatalogStore(tmp_path / "freqinout_nets.db")
    store.create_schema()
    store.create_source(CatalogSource("source_fcc", "FCC reference", "bundled", read_only=True))
    bundled = store.create_frequency(FrequencyResource("frequency_gmrs", "source_fcc", "channel", "gmrs", "GMRS 20", center_hz=462_675_000))

    with pytest.raises(ReadOnlyResourceError):
        store.update_frequency(dataclasses.replace(bundled, label="Changed"))
    with pytest.raises(ReadOnlyResourceError):
        store.delete_frequency_if_unreferenced(bundled.frequency_resource_key)


def test_station_override_replacement_retires_without_losing_referenced_usage(tmp_path):
    store = ResourceCatalogStore(tmp_path / "freqinout_nets.db")
    store.create_schema()
    store.create_source(CatalogSource("source_fcc", "FCC reference", "bundled", read_only=True))
    _station_source(store)
    bundled = store.create_frequency(FrequencyResource("frequency_reference", "source_fcc", "channel", "gmrs", "GMRS 20", center_hz=462_675_000))
    override = store.create_frequency(FrequencyResource("frequency_station_override", "source_station", "repeater", "gmrs", "County GMRS 20", receive_hz=bundled.center_hz, transmit_hz=467_675_000, provenance=bundled.frequency_resource_key))
    replacement = store.create_frequency(FrequencyResource("frequency_station_replacement", "source_station", "repeater", "gmrs", "County GMRS replacement", receive_hz=462_700_000, transmit_hz=467_700_000))
    entry = store.create_net_entry(NetDirectoryEntry("net_gmrs", "source_station", "County GMRS Net"))
    store.create_session(NetDirectorySession("session_gmrs", entry.net_entry_key, "source_station", "gmrs", override.frequency_resource_key, recurrence="Weekly"))

    retired = store.retire_frequency(override.frequency_resource_key, replacement_frequency_resource_key=replacement.frequency_resource_key)

    assert bundled.active and not bundled.retired
    assert retired.retired and not retired.active
    assert retired.replacement_frequency_resource_key == replacement.frequency_resource_key
    assert store.frequency_usage(override.frequency_resource_key).total_references == 1
    with pytest.raises(ReferencedResourceError):
        store.delete_frequency_if_unreferenced(override.frequency_resource_key)


def test_directory_session_query_is_bounded_and_preserves_integer_hz(tmp_path):
    store = ResourceCatalogStore(tmp_path / "freqinout_nets.db")
    store.create_schema()
    _station_source(store)
    resource = store.create_frequency(_frequency())
    entry = store.create_net_entry(NetDirectoryEntry("net_a", "source_station", "A Net"))
    for index in range(MAX_RESULTS + 4):
        store.create_session(NetDirectorySession(f"session_{index}", entry.net_entry_key, "source_station", "amateur", resource.frequency_resource_key, recurrence="Weekly", duration_minutes=60))

    sessions = store.list_sessions(net_entry_key=entry.net_entry_key, limit=MAX_RESULTS + 99)
    assert len(sessions) == MAX_RESULTS
    assert store.get_frequency(resource.frequency_resource_key).center_hz == 146_520_000


def test_production_sized_warm_filtered_catalog_queries_meet_budget(tmp_path):
    """Exercise the required 10k/2k/5k corpus without slow API setup."""
    path = tmp_path / "freqinout_nets.db"
    store = ResourceCatalogStore(path)
    store.create_schema()
    _station_source(store)
    now = "2026-09-09T00:00:00Z"
    with sqlite3.connect(path) as conn:
        conn.executemany(
            """INSERT INTO frequency_resources(
                frequency_resource_key,source_key,resource_kind,service,label,center_hz,
                content_hash,version_hash,active,retired,created_utc,updated_utc
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (
                    f"frequency_{index:05d}", "source_station", "simplex", "AMATEUR",
                    f"Regional Catalog Frequency {index:05d}", 144_000_000 + index,
                    f"content-frequency-{index}", f"version-frequency-{index}", 1, 0, now, now,
                )
                for index in range(10_000)
            ],
        )
        conn.executemany(
            """INSERT INTO net_directory_entries(
                net_entry_key,source_key,name,content_hash,version_hash,active,retired,created_utc,updated_utc
            ) VALUES (?,?,?,?,?,?,?,?,?)""",
            [
                (
                    f"net_{index:04d}", "source_station", f"Metro Regional Net {index:04d}",
                    f"content-net-{index}", f"version-net-{index}", 1, 0, now, now,
                )
                for index in range(2_000)
            ],
        )
        conn.executemany(
            """INSERT INTO net_directory_sessions(
                net_session_key,net_entry_key,source_key,service,frequency_resource_key,
                exception_dates_json,content_hash,version_hash,active,retired,created_utc,updated_utc
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (
                    f"session_{index:05d}", f"net_{index % 2_000:04d}", "source_station", "AMATEUR",
                    f"frequency_{index % 10_000:05d}", "[]", f"content-session-{index}",
                    f"version-session-{index}", 1, 0, now, now,
                )
                for index in range(5_000)
            ],
        )

    # Prime SQLite/page cache before capturing warm samples.
    assert len(store.list_frequencies(search="frequency 042", service="amateur", limit=MAX_RESULTS)) == 100
    assert len(store.list_net_entries(search="regional net 04", limit=MAX_RESULTS)) == 100
    assert len(store.list_sessions(net_entry_key="net_0042", limit=MAX_RESULTS)) == 3

    samples = []
    for _ in range(15):
        started = time.perf_counter()
        frequencies = store.list_frequencies(search="frequency 042", service="amateur", limit=MAX_RESULTS)
        entries = store.list_net_entries(search="regional net 04", limit=MAX_RESULTS)
        sessions = store.list_sessions(net_entry_key="net_0042", limit=MAX_RESULTS)
        samples.append(time.perf_counter() - started)
        assert len(frequencies) == len(entries) == 100
        assert len(sessions) == 3

    p95 = sorted(samples)[math.ceil(len(samples) * 0.95) - 1]
    assert p95 < 0.100, f"warm filtered catalog p95 was {p95 * 1000:.1f} ms"
