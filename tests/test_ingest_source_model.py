from __future__ import annotations

import sqlite3
import json
from pathlib import Path

from freqinout.core.ingest_source_model import (
    AppInstanceDescriptor,
    IngestSourceDescriptor,
    app_instance_from_device_profile,
    build_ingest_source_inventory,
    dedupe_ingest_sources,
    file_message_sources_from_device_profile,
    js8_api_endpoint_collisions,
    js8_ingest_sources,
)
from freqinout.core.protocol_capabilities import protocol_capabilities_for
from freqinout.core.js8_source_context import resolve_js8_endpoint_context, resolve_js8_source_context
from freqinout.core.js8_log_link_indexer import JS8LogLinkIndexer
from freqinout.core.js8_runtime_ingest import ingest_js8_links_for_runtime_sources
from freqinout.core.js8_runtime_messages import inbox_path_for_directed_source, ingest_js8_messages_for_runtime_sources
from freqinout.core.js8_ncs_offsets import ncs_offset_keys_for_directed_path
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.message_file_scanner import MessageFileScanner
from freqinout.core.message_source_delete import delete_js8_inbox_row, delete_js8_local_rows, delete_varac_local_projection
from freqinout.core import sitrep_ingest
from freqinout.core.sitrep_ingest import ingest_sitreps
from freqinout.core.varac_ingest import _get_last_id, _scoped_table_key, _set_last_id, ensure_varac_local_tables, ingest_varac
from freqinout.core.varac_runtime_ingest import ingest_varac_for_runtime_sources


class DictSettings:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value


def test_sitrep_ingest_throttle_is_source_scoped(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    monkeypatch.setattr(sitrep_ingest, "_LAST_RUN_MONO", 0.0)
    monkeypatch.setattr(sitrep_ingest, "_LAST_RUN_MONO_BY_SCOPE", {})
    monkeypatch.setattr(sitrep_ingest.time, "monotonic", lambda: 100.0)
    settings = DictSettings(
        {
            "sitrep_unified_ingest_enabled": True,
            "sitrep_ingest_local_spotter_backfill_enabled": False,
            "sitrep_ingest_imported_js8spotter_archive_enabled": False,
            "sitrep_ingest_js8spotter_enabled": False,
            "sitrep_ingest_commstat3_enabled": False,
            "sitrep_ingest_commstat23_enabled": False,
        }
    )

    first = ingest_sitreps(settings, ingest_scope_key="legacy")
    second_same_scope = ingest_sitreps(settings, ingest_scope_key="legacy")
    second_other_scope = ingest_sitreps(settings, ingest_scope_key="commstat-a")

    assert first["sources_attempted"] == 0
    assert second_same_scope["sources_attempted"] == 0
    assert "legacy" in sitrep_ingest._LAST_RUN_MONO_BY_SCOPE
    assert "commstat-a" in sitrep_ingest._LAST_RUN_MONO_BY_SCOPE


def test_js8_profile_builds_file_and_api_sources(tmp_path: Path) -> None:
    directed = tmp_path / "FIO-A" / "DIRECTED.TXT"
    profile = {
        "id": 7,
        "name": "FIO-A",
        "use_js8call": 1,
        "js8_host": "127.0.0.1",
        "js8_port": 2442,
        "js8_directed_path": str(directed),
        "js8_instance_id": 3,
    }

    instance = app_instance_from_device_profile(profile, "js8call")
    assert instance is not None

    sources = js8_ingest_sources(instance)
    roles = {source.metadata.get("role"): source for source in sources}

    assert set(roles) == {"directed", "all", "api"}
    assert roles["directed"].radio_id == "7"
    assert roles["directed"].path.endswith("DIRECTED.TXT")
    assert roles["all"].path.endswith("ALL.TXT")
    assert roles["api"].endpoint == "127.0.0.1:2442"
    assert roles["directed"].checkpoint_key != roles["all"].checkpoint_key
    assert instance.capabilities["receive_links"] is True
    assert instance.capabilities["send_message"] is True
    assert instance.capabilities["frequency_control"] is False
    assert roles["directed"].capabilities["receive_links"] is True
    assert roles["directed"].capabilities["send_message"] is False
    assert roles["directed"].provenance == "rf"
    assert roles["directed"].scope_hint == "station_or_group"


def test_protocol_capability_registry_keeps_varac_read_import_only() -> None:
    capabilities = protocol_capabilities_for("varac").as_dict()

    assert capabilities["receive_messages"] is True
    assert capabilities["bbs_read"] is True
    assert capabilities["store_forward"] is True
    assert capabilities["frequency_control"] is False
    assert capabilities["config_write_supported"] is False
    assert capabilities["bbs_write"] is False
    assert capabilities["read_only"] is True


def test_ingest_descriptors_self_populate_capability_hints() -> None:
    app = AppInstanceDescriptor(source_id="app_commstat", family="commstat", label="CommStat")
    source = IngestSourceDescriptor(
        source_id="source_commstat",
        family="commstat",
        source_type="sqlite",
        label="CommStat DB",
        metadata={"scope_hint": "group"},
    )

    assert app.capabilities["internet_assisted"] is True
    assert app.provenance == "mixed"
    assert source.capabilities["receive_reports"] is True
    assert source.capabilities["topology"] is True
    assert source.scope_hint == "group"


def test_js8_profile_defaults_api_endpoint_when_enabled(tmp_path: Path) -> None:
    directed = tmp_path / "FIO-A" / "DIRECTED.TXT"
    profile = {
        "id": 7,
        "name": "FIO-A",
        "use_js8call": 1,
        "js8_directed_path": str(directed),
    }

    instance = app_instance_from_device_profile(profile, "js8call")
    assert instance is not None

    roles = {source.metadata.get("role"): source for source in js8_ingest_sources(instance)}
    assert roles["api"].endpoint == "127.0.0.1:2442"


def test_js8_api_endpoint_collisions_detect_shared_default_endpoint(tmp_path: Path) -> None:
    inventory = build_ingest_source_inventory(
        [
            {"id": "A", "name": "FIO-A", "use_js8call": True, "js8_directed_path": str(tmp_path / "a" / "DIRECTED.TXT")},
            {"id": "B", "name": "FIO-B", "use_js8call": True, "js8_directed_path": str(tmp_path / "b" / "DIRECTED.TXT")},
        ]
    )

    collisions = js8_api_endpoint_collisions(inventory)

    assert collisions == {"127.0.0.1:2442": ("FIO-A JS8Call", "FIO-B JS8Call")}


def test_js8_profile_builds_explicit_inbox_source(tmp_path: Path) -> None:
    directed = tmp_path / "FIO-A" / "DIRECTED.TXT"
    inbox = tmp_path / "FIO-A" / "inbox_v1.db"
    profile = {
        "id": 7,
        "name": "FIO-A",
        "use_js8call": 1,
        "js8_directed_path": str(directed),
        "js8_inbox_path": str(inbox),
    }

    instance = app_instance_from_device_profile(profile, "js8call")
    assert instance is not None

    sources = js8_ingest_sources(instance)
    inbox_sources = [source for source in sources if source.metadata.get("role") == "inbox"]

    assert len(inbox_sources) == 1
    assert inbox_sources[0].source_type == "sqlite"
    assert inbox_sources[0].path == str(inbox)
    assert inbox_sources[0].app_instance_id == instance.source_id


def test_js8_source_context_resolves_api_source_for_endpoint(tmp_path: Path) -> None:
    directed = tmp_path / "FIO-A" / "DIRECTED.TXT"
    inventory = build_ingest_source_inventory(
        [
            {
                "id": 7,
                "name": "FIO-A",
                "use_js8call": 1,
                "js8_host": "127.0.0.1",
                "js8_port": 2442,
                "js8_directed_path": str(directed),
                "js8_instance_id": "fio-a",
            }
        ]
    )

    context = resolve_js8_source_context(host="127.0.0.1", port=2442, inventory=inventory)

    api_source = next(source for source in inventory.sources_for_family("js8call") if source.source_type == "api")
    assert context == {
        "source_id": api_source.source_id,
        "app_instance_id": api_source.app_instance_id,
        "source_radio_id": "7",
        "js8_instance_id": "fio-a",
    }


def test_js8_endpoint_context_resolves_app_source_key_to_instance_endpoint(tmp_path: Path) -> None:
    directed_a = tmp_path / "FIO-A" / "DIRECTED.TXT"
    directed_b = tmp_path / "FIO-B" / "DIRECTED.TXT"
    inventory = build_ingest_source_inventory(
        [
            {
                "id": 7,
                "name": "FIO-A",
                "use_js8call": 1,
                "js8_host": "127.0.0.1",
                "js8_port": 2442,
                "js8_directed_path": str(directed_a),
                "js8_instance_id": "fio-a",
            },
            {
                "id": 8,
                "name": "FIO-B",
                "use_js8call": 1,
                "js8_host": "127.0.0.1",
                "js8_port": 2444,
                "js8_directed_path": str(directed_b),
                "js8_instance_id": "fio-b",
            },
        ]
    )
    instance_b = next(instance for instance in inventory.app_instances if instance.radio_id == "8")

    context = resolve_js8_endpoint_context(
        {"js8_host": "127.0.0.1", "js8_port": 2442},
        source_context={"source_key": instance_b.source_id},
        inventory=inventory,
    )

    assert context["host"] == "127.0.0.1"
    assert context["port"] == "2444"
    assert context["source_radio_id"] == "8"
    assert context["js8_instance_id"] == "fio-b"


def test_js8_endpoint_context_resolves_file_source_key_to_owning_instance(tmp_path: Path) -> None:
    directed = tmp_path / "FIO-A" / "DIRECTED.TXT"
    inventory = build_ingest_source_inventory(
        [
            {
                "id": 7,
                "name": "FIO-A",
                "use_js8call": 1,
                "js8_host": "127.0.0.1",
                "js8_port": 2448,
                "js8_directed_path": str(directed),
                "js8_instance_id": "fio-a",
            }
        ]
    )
    directed_source = next(
        source
        for source in inventory.sources_for_family("js8call")
        if source.source_type == "file" and source.metadata.get("role") == "directed"
    )

    context = resolve_js8_endpoint_context(
        source_context={"source_key": directed_source.source_id},
        inventory=inventory,
    )

    assert context["host"] == "127.0.0.1"
    assert context["port"] == "2448"
    assert context["app_instance_id"] == directed_source.app_instance_id


def test_js8_ncs_offsets_are_source_scoped_and_separate_from_background_checkpoints(tmp_path: Path) -> None:
    directed = tmp_path / "FIO-A" / "DIRECTED.TXT"
    inventory = build_ingest_source_inventory(
        [
            {
                "id": 7,
                "name": "FIO-A",
                "use_js8call": 1,
                "js8_host": "127.0.0.1",
                "js8_port": 2442,
                "js8_directed_path": str(directed),
                "js8_instance_id": "fio-a",
            }
        ]
    )
    roles = {
        source.metadata.get("role"): source
        for source in inventory.sources_for_family("js8call")
        if source.source_type == "file"
    }

    keys = ncs_offset_keys_for_directed_path(directed, inventory=inventory)

    assert keys.directed_source_id == roles["directed"].source_id
    assert keys.all_source_id == roles["all"].source_id
    assert keys.directed_offset_key == f"js8_ncs_directed_offset_{roles['directed'].source_id}"
    assert keys.all_offset_key == f"js8_ncs_all_offset_{roles['all'].source_id}"
    assert keys.directed_offset_key != roles["directed"].checkpoint_key
    assert keys.all_offset_key != roles["all"].checkpoint_key


def test_js8_ncs_offsets_fall_back_to_path_stable_keys_without_inventory(tmp_path: Path) -> None:
    directed = tmp_path / "FIO-B" / "DIRECTED.TXT"

    first = ncs_offset_keys_for_directed_path(directed)
    second = ncs_offset_keys_for_directed_path(str(directed))

    assert first == second
    assert first.directed_source_id
    assert first.all_source_id
    assert first.directed_offset_key.startswith("js8_ncs_directed_offset_")
    assert first.all_offset_key.startswith("js8_ncs_all_offset_")


def test_file_profile_builds_enabled_message_directory_sources(tmp_path: Path) -> None:
    profile = {
        "id": 9,
        "name": "FIO-B",
        "use_flmsg": True,
        "use_flamp": False,
        "flmsg_message_path": str(tmp_path / "flmsg"),
        "flamp_message_path": str(tmp_path / "flamp"),
    }

    sources = file_message_sources_from_device_profile(profile)

    assert len(sources) == 1
    assert sources[0].family == "flmsg"
    assert sources[0].source_type == "directory"
    assert sources[0].radio_id == "9"


def test_dedupe_sources_keeps_one_descriptor_per_family_type_and_path(tmp_path: Path) -> None:
    profile = {
        "id": 1,
        "name": "FIO-A",
        "use_js8call": True,
        "js8_directed_path": str(tmp_path / "DIRECTED.TXT"),
    }
    instance = app_instance_from_device_profile(profile, "js8call")
    assert instance is not None
    sources = js8_ingest_sources(instance)

    deduped = dedupe_ingest_sources([*sources, *sources])

    assert len(deduped) == len(sources)


def test_inventory_builds_sources_across_runtime_profiles(tmp_path: Path) -> None:
    profiles = [
        {
            "id": 1,
            "name": "FIO-A",
            "use_js8call": True,
            "use_flmsg": True,
            "js8_host": "127.0.0.1",
            "js8_port": 2442,
            "js8_directed_path": str(tmp_path / "a" / "DIRECTED.TXT"),
            "flmsg_message_path": str(tmp_path / "flmsg"),
        },
        {
            "id": 2,
            "name": "FIO-B",
            "use_varac": True,
            "varac_db_path": str(tmp_path / "VarAC.db"),
        },
    ]

    inventory = build_ingest_source_inventory(profiles)

    assert [instance.family for instance in inventory.app_instances] == ["js8call", "varac"]
    assert {source.family for source in inventory.ingest_sources} == {"js8call", "flmsg", "varac"}
    assert {source.source_type for source in inventory.sources_for_family("js8call")} == {"file", "api"}
    assert len(inventory.sources_for_family("varac")) == 1


def test_inventory_includes_commstat_db_source_with_group_state(tmp_path: Path) -> None:
    commstat_dir = tmp_path / "CommStat"
    commstat_dir.mkdir()
    db_path = commstat_dir / "traffic.db3"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE groups (name TEXT, is_active INTEGER)")
        conn.execute("INSERT INTO groups (name, is_active) VALUES ('MAGNET', 1)")
        conn.execute("INSERT INTO groups (name, is_active) VALUES ('MR08', 0)")
        conn.commit()
    finally:
        conn.close()
    profile = {
        "id": 5,
        "name": "FIO-A",
        "use_commstat": True,
        "commstat_launch_path": str(commstat_dir / "CommStat.exe"),
    }

    inventory = build_ingest_source_inventory([profile])

    assert [instance.family for instance in inventory.app_instances] == ["commstat"]
    source = inventory.sources_for_family("commstat")[0]
    assert source.source_type == "sqlite"
    assert source.path == str(db_path)
    assert source.metadata["configured_groups"] == ("MAGNET", "MR08")
    assert source.metadata["active_groups"] == ("MAGNET",)


def test_js8_link_indexer_offsets_are_source_scoped(tmp_path: Path) -> None:
    source_a = tmp_path / "a" / "DIRECTED.TXT"
    source_b = tmp_path / "b" / "DIRECTED.TXT"
    source_a.parent.mkdir()
    source_b.parent.mkdir()
    source_a.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7AAA SNR -12\n",
        encoding="utf-8",
    )
    source_b.write_text(
        "2026-08-12 10:01:00\t14.115000\t1500\t+04\tN2MAG: K7BBB SNR -10\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "fio.db"
    settings = DictSettings({"operating_groups": []})
    indexer = JS8LogLinkIndexer(settings, db_path)  # type: ignore[arg-type]

    inserted_a = indexer.update_from_directed_path(source_a, directed_offset_key="a_offset", all_offset_key="a_all_offset")
    inserted_b = indexer.update_from_directed_path(source_b, directed_offset_key="b_offset", all_offset_key="b_all_offset")
    inserted_a_again = indexer.update_from_directed_path(source_a, directed_offset_key="a_offset", all_offset_key="a_all_offset")

    assert inserted_a == 1
    assert inserted_b == 1
    assert inserted_a_again == 0
    assert settings.values["a_offset"] == source_a.stat().st_size
    assert settings.values["b_offset"] == source_b.stat().st_size
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT origin, destination, band FROM js8_links ORDER BY origin").fetchall()
    finally:
        conn.close()
    assert rows == [("K7AAA", "N1MAG", "40M"), ("K7BBB", "N2MAG", "20M")]


def test_js8_link_indexer_updates_from_inventory_sources_per_instance(tmp_path: Path) -> None:
    directed_a = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed_b = tmp_path / "radio-b" / "DIRECTED.TXT"
    directed_a.parent.mkdir()
    directed_b.parent.mkdir()
    all_a = directed_a.with_name("ALL.TXT")
    all_b = directed_b.with_name("ALL.TXT")
    directed_a.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7AAA SNR -12\n",
        encoding="utf-8",
    )
    directed_b.write_text(
        "2026-08-12 10:01:00\t14.115000\t1500\t+04\tN2MAG: K7BBB SNR -10\n",
        encoding="utf-8",
    )
    all_a.write_text(
        "2026-08-12 10:02:00  Transmitting 7.115 MHz  JS8:  K7AAA: N1MAG SNR -09\n",
        encoding="utf-8",
    )
    all_b.write_text(
        "2026-08-12 10:03:00  Transmitting 14.115 MHz  JS8:  K7BBB: N2MAG SNR -08\n",
        encoding="utf-8",
    )
    inventory = build_ingest_source_inventory(
        [
            {
                "id": "A",
                "name": "FIO-A",
                "use_js8call": True,
                "js8_directed_path": str(directed_a),
            },
            {
                "id": "B",
                "name": "FIO-B",
                "use_js8call": True,
                "js8_directed_path": str(directed_b),
            },
        ]
    )
    settings = DictSettings({"operating_groups": []})
    indexer = JS8LogLinkIndexer(settings, tmp_path / "fio.db")  # type: ignore[arg-type]

    first = indexer.update_from_ingest_sources(inventory.sources_for_family("js8call"))
    second = indexer.update_from_ingest_sources(inventory.sources_for_family("js8call"))

    assert sorted(first.values()) == [1, 1]
    assert sorted(second.values()) == [0, 0]
    offset_keys = {key for key in settings.values if key.endswith("_offset")}
    assert len(offset_keys) == 4
    for source in inventory.sources_for_family("js8call"):
        if source.source_type == "file":
            assert settings.values[source.checkpoint_key] == Path(source.path).stat().st_size
    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        rows = conn.execute(
            "SELECT origin, destination, band, source_id, app_instance_id, source_radio_id FROM js8_links ORDER BY band, origin"
        ).fetchall()
    finally:
        conn.close()
    directed_by_radio = {
        source.radio_id: source
        for source in inventory.sources_for_family("js8call")
        if source.metadata.get("role") == "directed"
    }
    app_by_radio = {instance.radio_id: instance for instance in inventory.app_instances}
    assert rows == [
        ("K7BBB", "N2MAG", "20M", directed_by_radio["B"].source_id, app_by_radio["B"].source_id, "B"),
        ("K7AAA", "N1MAG", "40M", directed_by_radio["A"].source_id, app_by_radio["A"].source_id, "A"),
    ]


def test_js8_runtime_link_ingest_prefers_runtime_inventory_sources(tmp_path: Path) -> None:
    directed_a = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed_b = tmp_path / "radio-b" / "DIRECTED.TXT"
    directed_a.parent.mkdir()
    directed_b.parent.mkdir()
    directed_a.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7AAA SNR -12\n",
        encoding="utf-8",
    )
    directed_b.write_text(
        "2026-08-12 10:01:00\t14.115000\t1500\t+04\tN2MAG: K7BBB SNR -10\n",
        encoding="utf-8",
    )
    inventory = build_ingest_source_inventory(
        [
            {"id": "A", "name": "FIO-A", "use_js8call": True, "js8_directed_path": str(directed_a)},
            {"id": "B", "name": "FIO-B", "use_js8call": True, "js8_directed_path": str(directed_b)},
        ]
    )
    settings = DictSettings({"operating_groups": []})

    result = ingest_js8_links_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        tmp_path / "fio.db",
        inventory=inventory,
    )

    assert result.used_runtime_sources is True
    assert result.inserted == 2
    assert sorted(result.counts_by_source.values()) == [1, 1]
    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        rows = conn.execute(
            "SELECT origin, destination, band, source_radio_id FROM js8_links ORDER BY source_radio_id"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("K7AAA", "N1MAG", "40M", "A"), ("K7BBB", "N2MAG", "20M", "B")]


def test_js8_runtime_link_ingest_rebuilds_empty_table_with_stale_offsets(tmp_path: Path) -> None:
    directed = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed.parent.mkdir()
    directed.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7AAA SNR -12\n",
        encoding="utf-8",
    )
    inventory = build_ingest_source_inventory(
        [{"id": "A", "name": "FIO-A", "use_js8call": True, "js8_directed_path": str(directed)}]
    )
    settings = DictSettings({"operating_groups": []})
    for source in inventory.sources_for_family("js8call"):
        path = Path(source.path)
        if source.source_type == "file" and path.exists():
            settings.set(source.checkpoint_key, path.stat().st_size)

    result = ingest_js8_links_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        tmp_path / "fio.db",
        since_ts=2_000_000_000.0,
        inventory=inventory,
    )

    assert result.used_runtime_sources is True
    assert result.inserted == 1
    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        rows = conn.execute("SELECT origin, destination, band FROM js8_links").fetchall()
    finally:
        conn.close()
    assert rows == [("K7AAA", "N1MAG", "40M")]


def test_js8_runtime_link_ingest_force_rebuild_uses_runtime_sources_not_stale_rows(tmp_path: Path) -> None:
    directed_a = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed_b = tmp_path / "radio-b" / "DIRECTED.TXT"
    directed_a.parent.mkdir()
    directed_b.parent.mkdir()
    directed_a.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7AAA SNR -12\n",
        encoding="utf-8",
    )
    directed_b.write_text(
        "2026-08-12 10:01:00\t14.115000\t1500\t+04\tN2MAG: K7BBB SNR -10\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "fio.db"
    seed_settings = DictSettings({"js8_directed_path": str(directed_a), "operating_groups": []})
    JS8LogLinkIndexer(seed_settings, db_path).update(force_rebuild=True)
    inventory = build_ingest_source_inventory(
        [{"id": "B", "name": "FIO-B", "use_js8call": True, "js8_directed_path": str(directed_b)}]
    )
    settings = DictSettings({"js8_directed_path": str(directed_a), "operating_groups": []})

    result = ingest_js8_links_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        db_path,
        inventory=inventory,
        force_rebuild=True,
    )

    assert result.used_runtime_sources is True
    assert result.inserted == 1
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT origin, destination, band, source_radio_id FROM js8_links ORDER BY origin"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("K7BBB", "N2MAG", "20M", "B")]


def test_js8_link_indexer_keeps_same_link_from_different_sources(tmp_path: Path) -> None:
    directed_a = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed_b = tmp_path / "radio-b" / "DIRECTED.TXT"
    directed_a.parent.mkdir()
    directed_b.parent.mkdir()
    directed_a.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7AAA SNR -12\n",
        encoding="utf-8",
    )
    directed_b.write_text(
        "2026-08-12 10:01:00\t7.115000\t1500\t+04\tN1MAG: K7AAA SNR -10\n",
        encoding="utf-8",
    )
    inventory = build_ingest_source_inventory(
        [
            {"id": "A", "name": "FIO-A", "use_js8call": True, "js8_directed_path": str(directed_a)},
            {"id": "B", "name": "FIO-B", "use_js8call": True, "js8_directed_path": str(directed_b)},
        ]
    )
    settings = DictSettings({"operating_groups": []})
    indexer = JS8LogLinkIndexer(settings, tmp_path / "fio.db")  # type: ignore[arg-type]

    indexer.update_from_ingest_sources(inventory.sources_for_family("js8call"))

    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        rows = conn.execute(
            """
            SELECT origin, destination, band, source_radio_id
              FROM js8_links
          ORDER BY source_radio_id
            """
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("K7AAA", "N1MAG", "40M", "A"), ("K7AAA", "N1MAG", "40M", "B")]


def test_js8_link_indexer_visible_and_background_same_sources_do_not_duplicate(tmp_path: Path) -> None:
    directed = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed.parent.mkdir()
    directed.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7AAA SNR -12\n",
        encoding="utf-8",
    )
    inventory = build_ingest_source_inventory(
        [{"id": "A", "name": "FIO-A", "use_js8call": True, "js8_directed_path": str(directed)}]
    )
    db_path = tmp_path / "fio.db"
    background_settings = DictSettings({"operating_groups": []})
    visible_settings = DictSettings({"operating_groups": []})
    sources = inventory.sources_for_family("js8call")

    background = JS8LogLinkIndexer(background_settings, db_path)  # type: ignore[arg-type]
    visible = JS8LogLinkIndexer(visible_settings, db_path)  # type: ignore[arg-type]
    background_counts = background.update_from_ingest_sources(sources)
    visible_counts = visible.update_from_ingest_sources(sources)

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT origin, destination, band, source_radio_id
              FROM js8_links
          ORDER BY source_radio_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert sorted(background_counts.values()) == [1]
    assert sorted(visible_counts.values()) == [1]
    assert rows == [("K7AAA", "N1MAG", "40M", "A")]


def test_js8_link_indexer_live_batch_preserves_source_provenance(tmp_path: Path) -> None:
    settings = DictSettings({"operating_groups": []})
    indexer = JS8LogLinkIndexer(settings, tmp_path / "fio.db")  # type: ignore[arg-type]

    indexer.ingest_live_batch(
        [
            (1_786_000_000.0, "K7AAA", "N1MAG", -12.0, 7_115_000.0, 0, "source-a", "app-a", "A"),
            (1_786_000_001.0, "K7AAA", "N1MAG", -10.0, 7_115_000.0, 0, "source-b", "app-b", "B"),
        ]
    )

    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        rows = conn.execute(
            """
            SELECT origin, destination, band, source_id, app_instance_id, source_radio_id
              FROM js8_links
          ORDER BY source_id
            """
        ).fetchall()
        stats = conn.execute(
            """
            SELECT callsign, last_source_id, last_app_instance_id, last_source_radio_id
              FROM js8_callsign_stats
             WHERE callsign='K7AAA'
            """
        ).fetchone()
    finally:
        conn.close()

    assert rows == [
        ("K7AAA", "N1MAG", "40M", "source-a", "app-a", "A"),
        ("K7AAA", "N1MAG", "40M", "source-b", "app-b", "B"),
    ]
    assert stats == ("K7AAA", "source-b", "app-b", "B")


def test_js8_new_source_scoped_offset_does_not_skip_older_history(tmp_path: Path) -> None:
    directed = tmp_path / "new" / "DIRECTED.TXT"
    directed.parent.mkdir()
    directed.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7AAA SNR -12\n",
        encoding="utf-8",
    )
    settings = DictSettings({"operating_groups": []})
    indexer = JS8LogLinkIndexer(settings, tmp_path / "fio.db")  # type: ignore[arg-type]

    inserted = indexer.update_from_directed_path(
        directed,
        since_ts=2_000_000_000.0,
        directed_offset_key="new_source_offset",
        all_offset_key="new_source_all_offset",
    )

    assert inserted == 1


def test_js8_link_indexer_parses_relay_route_to_final_station(tmp_path: Path) -> None:
    directed = tmp_path / "DIRECTED.TXT"
    directed.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: K7RIE>KC7WOK SNR -12\n",
        encoding="utf-8",
    )
    settings = DictSettings({"operating_groups": []})
    indexer = JS8LogLinkIndexer(settings, tmp_path / "fio.db")  # type: ignore[arg-type]

    inserted = indexer.update_from_directed_path(directed)

    assert inserted == 1
    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        rows = conn.execute(
            "SELECT origin, destination, is_relay, relay_via FROM js8_links"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("KC7WOK", "N1MAG", 1, "K7RIE")]


def test_js8_link_indexer_ignores_group_targets_but_keeps_station_relays(tmp_path: Path) -> None:
    directed = tmp_path / "DIRECTED.TXT"
    directed.write_text(
        "2026-08-12 10:00:00\t7.115000\t1500\t+05\tN1MAG: @MAGNET GRID DM79QJ\n"
        "2026-08-12 10:01:00\t7.115000\t1500\t+04\tN1MAG: K7RIE>KC7WOK SNR -12\n",
        encoding="utf-8",
    )
    settings = DictSettings({"operating_groups": []})
    indexer = JS8LogLinkIndexer(settings, tmp_path / "fio.db")  # type: ignore[arg-type]

    inserted = indexer.update_from_directed_path(directed)

    assert inserted == 1
    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        rows = conn.execute("SELECT origin, destination FROM js8_links").fetchall()
    finally:
        conn.close()
    assert rows == [("KC7WOK", "N1MAG")]


def test_js8_inbox_ingest_keeps_same_native_id_from_two_sources(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    inbox_a = tmp_path / "a" / "inbox.db3"
    inbox_b = tmp_path / "b" / "inbox.db3"
    inbox_a.parent.mkdir()
    inbox_b.parent.mkdir()
    payload_a = {"params": {"TEXT": "HELLO A", "FROM": "K1AAA", "TO": "@MAGNET", "UTC": "2026-08-12 10:00:00"}}
    payload_b = {"params": {"TEXT": "HELLO B", "FROM": "K2BBB", "TO": "@MR08", "UTC": "2026-08-12 10:01:00"}}
    for path, payload in ((inbox_a, payload_a), (inbox_b, payload_b)):
        conn = sqlite3.connect(path)
        try:
            conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, json TEXT, type TEXT, value TEXT)")
            conn.execute("INSERT INTO inbox_v1 (id, json, type, value) VALUES (1, ?, 'UNREAD', '')", (json.dumps(payload),))
            conn.commit()
        finally:
            conn.close()
    ingestor = MessageIngestor(DictSettings({"operating_groups": []}))  # type: ignore[arg-type]

    ingestor.ingest_js8_messages(inbox_path=inbox_a, source_radio_id=1, js8_instance_id="A", source_key="source_a")
    ingestor.ingest_js8_messages(inbox_path=inbox_b, source_radio_id=2, js8_instance_id="B", source_key="source_b")

    conn = sqlite3.connect(profile_root / "config" / "freqinout_nets.db")
    try:
        rows = conn.execute(
            "SELECT source_key, source_id, from_call, raw_text, source_path FROM js8_messages ORDER BY source_key"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [
        ("source_a", 1, "K1AAA", "HELLO A", str(inbox_a)),
        ("source_b", 1, "K2BBB", "HELLO B", str(inbox_b)),
    ]


def test_js8_source_inbox_resolver_requires_source_local_inbox(tmp_path: Path) -> None:
    directed = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed.parent.mkdir()
    directed.write_text("", encoding="utf-8")
    profiles = [{"id": "A", "name": "FIO-A", "use_js8call": True, "js8_directed_path": str(directed)}]
    inventory = build_ingest_source_inventory(profiles)
    directed_source = next(
        source
        for source in inventory.sources_for_family("js8call")
        if source.source_type == "file" and source.metadata.get("role") == "directed"
    )

    assert inbox_path_for_directed_source(directed_source) is None

    inbox = directed.parent / "inbox.db3"
    inbox.write_text("", encoding="utf-8")

    assert inbox_path_for_directed_source(directed_source) == inbox


def test_js8_runtime_message_ingest_uses_explicit_profile_inbox(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    directed = tmp_path / "radio-a" / "DIRECTED.TXT"
    explicit_inbox = tmp_path / "custom-inbox" / "fio-a.db3"
    directed.parent.mkdir()
    explicit_inbox.parent.mkdir()
    directed.write_text("", encoding="utf-8")
    conn = sqlite3.connect(explicit_inbox)
    try:
        conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, json TEXT, type TEXT, value TEXT)")
        conn.execute(
            "INSERT INTO inbox_v1 (id, json, type, value) VALUES (1, ?, 'UNREAD', '')",
            (json.dumps({"params": {"TEXT": "EXPLICIT INBOX", "FROM": "K1AAA", "TO": "@MAGNET", "UTC": "2026-08-12 10:00:00"}}),),
        )
        conn.commit()
    finally:
        conn.close()
    profiles = [
        {
            "id": "A",
            "name": "FIO-A",
            "use_js8call": True,
            "js8_directed_path": str(directed),
            "js8_inbox_path": str(explicit_inbox),
        }
    ]
    inventory = build_ingest_source_inventory(profiles)
    settings = DictSettings({"operating_groups": []})

    result = ingest_js8_messages_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        inventory=inventory,
        profiles=profiles,
    )

    assert result.js8_inbox_sources == 1
    conn = sqlite3.connect(profile_root / "config" / "freqinout_nets.db")
    try:
        row = conn.execute("SELECT raw_text, source_path FROM js8_messages").fetchone()
    finally:
        conn.close()
    assert row == ("EXPLICIT INBOX", str(explicit_inbox))


def test_js8_runtime_message_ingest_uses_all_runtime_sources(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    directed_a = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed_b = tmp_path / "radio-b" / "DIRECTED.TXT"
    directed_a.parent.mkdir()
    directed_b.parent.mkdir()
    directed_a.write_text("", encoding="utf-8")
    directed_b.write_text("", encoding="utf-8")
    inbox_a = directed_a.parent / "inbox.db3"
    inbox_b = directed_b.parent / "inbox.db3"
    payload_a = {"params": {"TEXT": "HELLO A", "FROM": "K1AAA", "TO": "@MAGNET", "UTC": "2026-08-12 10:00:00"}}
    payload_b = {"params": {"TEXT": "HELLO B", "FROM": "K2BBB", "TO": "@MR08", "UTC": "2026-08-12 10:01:00"}}
    for path, payload in ((inbox_a, payload_a), (inbox_b, payload_b)):
        conn = sqlite3.connect(path)
        try:
            conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, json TEXT, type TEXT, value TEXT)")
            conn.execute("INSERT INTO inbox_v1 (id, json, type, value) VALUES (1, ?, 'UNREAD', '')", (json.dumps(payload),))
            conn.commit()
        finally:
            conn.close()
    profiles = [
        {"id": "A", "name": "FIO-A", "use_js8call": True, "js8_directed_path": str(directed_a)},
        {"id": "B", "name": "FIO-B", "use_js8call": True, "js8_directed_path": str(directed_b)},
    ]
    inventory = build_ingest_source_inventory(profiles)
    settings = DictSettings({"operating_groups": []})

    result = ingest_js8_messages_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        inventory=inventory,
        profiles=profiles,
    )

    assert result.used_runtime_sources is True
    assert result.js8_inbox_sources == 2
    conn = sqlite3.connect(profile_root / "config" / "freqinout_nets.db")
    try:
        rows = conn.execute(
            "SELECT source_key, source_id, source_radio_id, from_call, raw_text FROM js8_messages ORDER BY source_radio_id"
        ).fetchall()
    finally:
        conn.close()
    instance_by_radio = {instance.radio_id: instance for instance in inventory.app_instances}
    assert rows == [
        (instance_by_radio["A"].source_id, 1, "A", "K1AAA", "HELLO A"),
        (instance_by_radio["B"].source_id, 1, "B", "K2BBB", "HELLO B"),
    ]


def test_js8_runtime_message_ingest_does_not_fallback_to_global_inbox(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    global_directed = tmp_path / "global" / "DIRECTED.TXT"
    runtime_directed = tmp_path / "runtime" / "DIRECTED.TXT"
    global_directed.parent.mkdir()
    runtime_directed.parent.mkdir()
    global_directed.write_text("", encoding="utf-8")
    runtime_directed.write_text("", encoding="utf-8")
    inbox = global_directed.parent / "inbox.db3"
    conn = sqlite3.connect(inbox)
    try:
        conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, json TEXT, type TEXT, value TEXT)")
        conn.execute(
            "INSERT INTO inbox_v1 (id, json, type, value) VALUES (1, ?, 'UNREAD', '')",
            (json.dumps({"params": {"TEXT": "GLOBAL ONLY", "FROM": "K1AAA", "TO": "@MAGNET", "UTC": "2026-08-12 10:00:00"}}),),
        )
        conn.commit()
    finally:
        conn.close()
    profiles = [
        {"id": "A", "name": "FIO-A", "use_js8call": True, "js8_directed_path": str(runtime_directed)},
    ]
    inventory = build_ingest_source_inventory(profiles)
    settings = DictSettings({"operating_groups": [], "js8_directed_path": str(global_directed)})

    result = ingest_js8_messages_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        inventory=inventory,
        profiles=profiles,
    )

    assert result.used_runtime_sources is True
    assert result.js8_inbox_sources == 0
    local_db = profile_root / "config" / "freqinout_nets.db"
    if not local_db.exists():
        return
    conn = sqlite3.connect(local_db)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='js8_messages'"
        ).fetchone()
        count = conn.execute("SELECT COUNT(*) FROM js8_messages").fetchone()[0] if exists else 0
    finally:
        conn.close()
    assert count == 0


def test_js8_source_scoped_delete_removes_only_matching_native_source(monkeypatch, tmp_path: Path) -> None:
    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    inbox_a = tmp_path / "a" / "inbox.db3"
    inbox_b = tmp_path / "b" / "inbox.db3"
    inbox_a.parent.mkdir()
    inbox_b.parent.mkdir()
    payload_a = {"params": {"TEXT": "DELETE A", "FROM": "K1AAA", "TO": "@MAGNET", "UTC": "2026-08-12 10:00:00"}}
    payload_b = {"params": {"TEXT": "KEEP B", "FROM": "K2BBB", "TO": "@MR08", "UTC": "2026-08-12 10:01:00"}}
    for path, payload in ((inbox_a, payload_a), (inbox_b, payload_b)):
        conn = sqlite3.connect(path)
        try:
            conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, json TEXT, type TEXT, value TEXT)")
            conn.execute("INSERT INTO inbox_v1 (id, json, type, value) VALUES (1, ?, 'UNREAD', '')", (json.dumps(payload),))
            conn.commit()
        finally:
            conn.close()
    ingestor = MessageIngestor(DictSettings({"operating_groups": []}))  # type: ignore[arg-type]
    ingestor.ingest_js8_messages(inbox_path=inbox_a, source_radio_id=1, js8_instance_id="A", source_key="source_a")
    ingestor.ingest_js8_messages(inbox_path=inbox_b, source_radio_id=2, js8_instance_id="B", source_key="source_b")
    local_db = profile_root / "config" / "freqinout_nets.db"

    assert delete_js8_inbox_row(inbox_a, 1) is True
    delete_js8_local_rows(local_db, 0x7FFFFFFF, source_key="source_a", source_id=1)

    conn = sqlite3.connect(local_db)
    try:
        rows = conn.execute("SELECT source_key, source_id, raw_text FROM js8_messages ORDER BY source_key").fetchall()
    finally:
        conn.close()
    assert rows == [("source_b", 1, "KEEP B")]

    conn = sqlite3.connect(inbox_b)
    try:
        assert conn.execute("SELECT COUNT(*) FROM inbox_v1 WHERE id=1").fetchone()[0] == 1
    finally:
        conn.close()


def test_file_scanner_preserves_source_metadata_across_incremental_runs(tmp_path: Path) -> None:
    msg_path = tmp_path / "messages" / "report.k2s"
    msg_path.parent.mkdir()
    msg_path.write_text("MAGNET report", encoding="utf-8")
    watch = [
        {
            "origin": "flmsg",
            "path": str(msg_path.parent),
            "source_id": "source-flmsg-a",
            "source_label": "FIO-A FLMSG",
        }
    ]

    first_records, first_mtimes, _mode = MessageFileScanner(watch, force=True).scan()
    first = first_records["flmsg"][0]
    second_records, _second_mtimes, _mode = MessageFileScanner(
        watch,
        force=False,
        base_records=first_records,
        base_dir_mtimes=first_mtimes,
    ).scan()
    second = second_records["flmsg"][0]

    assert first.source_id == "source-flmsg-a"
    assert first.source_label == "FIO-A FLMSG"
    assert second.source_id == "source-flmsg-a"
    assert second.source_label == "FIO-A FLMSG"


def test_file_scanner_preserves_source_metadata_for_nested_full_scan(tmp_path: Path) -> None:
    msg_path = tmp_path / "messages" / "nested" / "report.k2s"
    msg_path.parent.mkdir(parents=True)
    msg_path.write_text("Nested MAGNET report", encoding="utf-8")
    watch = [
        {
            "origin": "flmsg",
            "path": str(tmp_path / "messages"),
            "source_id": "source-flmsg-a",
            "source_label": "FIO-A FLMSG",
        }
    ]

    records, _mtimes, mode = MessageFileScanner(watch, force=True).scan()

    assert mode == "full"
    assert len(records["flmsg"]) == 1
    assert records["flmsg"][0].path == msg_path
    assert records["flmsg"][0].source_id == "source-flmsg-a"
    assert records["flmsg"][0].source_label == "FIO-A FLMSG"


def test_varac_watermarks_can_be_scoped_by_source(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        ensure_varac_local_tables(conn)
        _set_last_id(conn, "vmail", 12, "varac_a")
        _set_last_id(conn, "vmail", 34, "varac_b")
        _set_last_id(conn, "vmail", 5)
        conn.commit()

        assert _scoped_table_key("vmail", "varac_a") == "varac_a:vmail"
        assert _get_last_id(conn, "vmail", "varac_a") == 12
        assert _get_last_id(conn, "vmail", "varac_b") == 34
        assert _get_last_id(conn, "vmail") == 5
    finally:
        conn.close()


def test_varac_messages_table_migrates_to_source_scoped_primary_key(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE varac_messages (
                id INTEGER,
                guid TEXT,
                source TEXT,
                msg_type TEXT,
                from_call TEXT,
                to_call TEXT,
                subject TEXT,
                body TEXT,
                ts REAL,
                PRIMARY KEY (source, id)
            )
            """
        )
        conn.execute(
            "INSERT INTO varac_messages (id, guid, source, msg_type, from_call, to_call, subject, body, ts) VALUES (1, 'g1', 'vmail', 'VMAIL', 'K1AAA', 'K2BBB', 'Hello', 'Body', 10)"
        )
        ensure_varac_local_tables(conn)

        info = conn.execute("PRAGMA table_info(varac_messages)").fetchall()
        pk_columns = [
            str(row[1] or "")
            for row in sorted((row for row in info if int(row[5] or 0)), key=lambda row: int(row[5] or 0))
        ]
        assert pk_columns == ["ingest_source_key", "source", "id"]
        row = conn.execute("SELECT ingest_source_key, source, id, subject FROM varac_messages").fetchone()
        assert row == ("legacy", "vmail", 1, "Hello")
        conn.execute(
            "INSERT INTO varac_messages (ingest_source_key, id, guid, source, msg_type, subject) VALUES ('varac-b', 1, 'g2', 'vmail', 'VMAIL', 'Other')"
        )
        rows = conn.execute("SELECT ingest_source_key, source, id, subject FROM varac_messages ORDER BY ingest_source_key").fetchall()
        assert rows == [("legacy", "vmail", 1, "Hello"), ("varac-b", "vmail", 1, "Other")]
    finally:
        conn.close()


def test_varac_local_projection_delete_can_be_source_scoped(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        ensure_varac_local_tables(conn)
        conn.execute(
            "INSERT INTO varac_messages (ingest_source_key, id, guid, source, msg_type, subject) VALUES ('varac-a', 1, 'g1', 'vmail', 'VMAIL', 'A')"
        )
        conn.execute(
            "INSERT INTO varac_messages (ingest_source_key, id, guid, source, msg_type, subject) VALUES ('varac-b', 1, 'g2', 'vmail', 'VMAIL', 'B')"
        )
        conn.commit()
    finally:
        conn.close()

    delete_varac_local_projection(db_path, source="vmail", msg_id=1, ingest_source_key="varac-a")

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT ingest_source_key, source, id, subject FROM varac_messages").fetchall()
    finally:
        conn.close()
    assert rows == [("varac-b", "vmail", 1, "B")]


def test_varac_ingest_keeps_same_native_message_id_from_two_sources(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    source_a = tmp_path / "varac-a.db"
    source_b = tmp_path / "varac-b.db"
    for db_path, subject, sender in (
        (source_a, "A message", "K1AAA"),
        (source_b, "B message", "K2BBB"),
    ):
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE vmail (
                    id INTEGER PRIMARY KEY,
                    guid TEXT,
                    creation_time TEXT,
                    sent_time TEXT,
                    received_time TEXT,
                    folder_id INTEGER,
                    vmail_to TEXT,
                    vmail_from TEXT,
                    delivery_band TEXT,
                    delivery_snr TEXT,
                    subject TEXT,
                    msg TEXT,
                    read_status INTEGER,
                    is_deleted INTEGER,
                    frequency REAL,
                    vmail_via TEXT,
                    urgent INTEGER,
                    has_attachment INTEGER
                )
                """
            )
            conn.execute(
                """
                INSERT INTO vmail (
                    id, guid, creation_time, sent_time, received_time, folder_id, vmail_to, vmail_from,
                    delivery_band, delivery_snr, subject, msg, read_status, is_deleted, frequency,
                    vmail_via, urgent, has_attachment
                ) VALUES (1, ?, '2026-08-12 10:00:00', '', '', 0, 'N1MAG', ?, '20M', '-10', ?, 'Body', 0, 0, 14115000, '', 0, 0)
                """,
                (f"guid-{sender}", sender, subject),
            )
            conn.commit()
        finally:
            conn.close()

    assert ingest_varac(DictSettings({"varac_db_path": str(source_a)}), ingest_source_key="varac-a") is True  # type: ignore[arg-type]
    assert ingest_varac(DictSettings({"varac_db_path": str(source_b)}), ingest_source_key="varac-b") is True  # type: ignore[arg-type]

    local_db = tmp_path / "profile" / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(local_db)
    try:
        rows = conn.execute(
            "SELECT ingest_source_key, source, id, from_call, subject FROM varac_messages ORDER BY ingest_source_key"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [
        ("varac-a", "vmail", 1, "K1AAA", "A message"),
        ("varac-b", "vmail", 1, "K2BBB", "B message"),
    ]


def test_varac_runtime_ingest_uses_source_inventory(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    source_a = tmp_path / "varac-a.db"
    source_b = tmp_path / "varac-b.db"
    for db_path, subject, sender in (
        (source_a, "A message", "K1AAA"),
        (source_b, "B message", "K2BBB"),
    ):
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE vmail (
                    id INTEGER PRIMARY KEY,
                    guid TEXT,
                    creation_time TEXT,
                    sent_time TEXT,
                    received_time TEXT,
                    folder_id INTEGER,
                    vmail_to TEXT,
                    vmail_from TEXT,
                    delivery_band TEXT,
                    delivery_snr TEXT,
                    subject TEXT,
                    msg TEXT,
                    read_status INTEGER,
                    is_deleted INTEGER,
                    frequency REAL,
                    vmail_via TEXT,
                    urgent INTEGER,
                    has_attachment INTEGER
                )
                """
            )
            conn.execute(
                """
                INSERT INTO vmail (
                    id, guid, creation_time, sent_time, received_time, folder_id, vmail_to, vmail_from,
                    delivery_band, delivery_snr, subject, msg, read_status, is_deleted, frequency,
                    vmail_via, urgent, has_attachment
                ) VALUES (1, ?, '2026-08-12 10:00:00', '', '', 0, 'N1MAG', ?, '20M', '-10', ?, 'Body', 0, 0, 14115000, '', 0, 0)
                """,
                (f"guid-runtime-{sender}", sender, subject),
            )
            conn.commit()
        finally:
            conn.close()
    profiles = [
        {"id": "A", "name": "FIO-A", "use_varac": True, "varac_db_path": str(source_a)},
        {"id": "B", "name": "FIO-B", "use_varac": True, "varac_db_path": str(source_b)},
    ]
    inventory = build_ingest_source_inventory(profiles)
    settings = DictSettings({"operating_groups": []})

    result = ingest_varac_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        inventory=inventory,
        profiles=profiles,
    )

    assert result.used_runtime_sources is True
    assert result.sources_attempted == 2
    assert result.sources_succeeded == 2
    local_db = tmp_path / "profile" / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(local_db)
    try:
        rows = conn.execute(
            "SELECT ingest_source_key, source, id, from_call, subject FROM varac_messages ORDER BY subject"
        ).fetchall()
    finally:
        conn.close()
    source_by_radio = {
        source.radio_id: source
        for source in inventory.sources_for_family("varac")
    }
    assert rows == [
        (source_by_radio["A"].source_id, "vmail", 1, "K1AAA", "A message"),
        (source_by_radio["B"].source_id, "vmail", 1, "K2BBB", "B message"),
    ]


def test_varac_runtime_ingest_repeated_same_source_does_not_duplicate(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    source_db = tmp_path / "varac-a.db"
    conn = sqlite3.connect(source_db)
    try:
        conn.execute(
            """
            CREATE TABLE vmail (
                id INTEGER PRIMARY KEY,
                guid TEXT,
                creation_time TEXT,
                sent_time TEXT,
                received_time TEXT,
                folder_id INTEGER,
                vmail_to TEXT,
                vmail_from TEXT,
                delivery_band TEXT,
                delivery_snr TEXT,
                subject TEXT,
                msg TEXT,
                read_status INTEGER,
                is_deleted INTEGER,
                frequency REAL,
                vmail_via TEXT,
                urgent INTEGER,
                has_attachment INTEGER
            )
            """
        )
        conn.execute(
            """
            INSERT INTO vmail (
                id, guid, creation_time, sent_time, received_time, folder_id, vmail_to, vmail_from,
                delivery_band, delivery_snr, subject, msg, read_status, is_deleted, frequency,
                vmail_via, urgent, has_attachment
            ) VALUES (1, 'guid-runtime-K1AAA', '2026-08-12 10:00:00', '', '', 0, 'N1MAG', 'K1AAA',
                      '20M', '-10', 'A message', 'Body', 0, 0, 14115000, '', 0, 0)
            """
        )
        conn.commit()
    finally:
        conn.close()
    profiles = [{"id": "A", "name": "FIO-A", "use_varac": True, "varac_db_path": str(source_db)}]
    inventory = build_ingest_source_inventory(profiles)
    settings = DictSettings({"operating_groups": []})

    first = ingest_varac_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        inventory=inventory,
        profiles=profiles,
    )
    second = ingest_varac_for_runtime_sources(
        settings,  # type: ignore[arg-type]
        inventory=inventory,
        profiles=profiles,
    )

    local_db = tmp_path / "profile" / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(local_db)
    try:
        rows = conn.execute(
            "SELECT ingest_source_key, source, id, from_call, subject FROM varac_messages"
        ).fetchall()
        state_rows = conn.execute("SELECT table_name, last_id FROM varac_ingest_state").fetchall()
    finally:
        conn.close()

    source = inventory.sources_for_family("varac")[0]
    assert first.used_runtime_sources is True
    assert first.sources_succeeded == 1
    assert second.used_runtime_sources is True
    assert second.sources_succeeded == 1
    assert rows == [(source.source_id, "vmail", 1, "K1AAA", "A message")]
    assert (f"{source.source_id}:vmail", 1) in state_rows
