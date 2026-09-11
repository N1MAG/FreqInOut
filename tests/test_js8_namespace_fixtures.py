"""Contract fixtures for JSV-S1 namespace and legacy-path acceptance tests.

The resolver tests consume this data so path expectations remain explicit and
do not accidentally regress to using SaveDir as JS8 message storage.
"""

import json
import sqlite3
from pathlib import Path

from freqinout.core.js8_storage import resolve_js8_storage, storage_collisions
from freqinout.core.multi_radio_store import MultiRadioStore

FIXTURE = Path(__file__).parent / "fixtures" / "js8_namespace_cases.json"


def _cases() -> list[dict[str, object]]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]


def test_namespace_fixture_has_three_distinct_roots_and_required_message_files() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    cases = _cases()

    roots = {str(case["expected_root"]) for case in cases}
    assert len(roots) == 3
    assert payload["message_files"] == ["ALL.TXT", "DIRECTED.TXT", "inbox.db3"]


def test_resolver_maps_default_and_rig_named_fixture_roots_and_message_files(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for case in payload["cases"]:
        root = tmp_path / str(case["id"])
        resolved = resolve_js8_storage(
            {
                "variant_family": case["variant"],
                "variant_version": case["version"],
                "rig_name": case["rig_name"],
                "rig_name_source": "fixture",
                "application_data_root": str(root),
                "storage_evidence": "operator_confirmed:fixture",
                "save_dir": case["save_dir"],
            },
            platform="Linux",
            home=tmp_path,
        )
        assert resolved.data_root == str(root.resolve())
        assert resolved.all_path == str(root / "ALL.TXT")
        assert resolved.directed_path == str(root / "DIRECTED.TXT")
        assert resolved.inbox_path == str(root / "inbox.db3")
        assert resolved.save_dir == case["save_dir"]
        assert resolved.storage_mode == "rig_scoped"


def test_resolver_keeps_message_identity_when_only_save_dir_changes(tmp_path: Path) -> None:
    base = {
        "variant_family": "js8call",
        "variant_version": "2.2.0",
        "rig_name": "field-a",
        "application_data_root": str(tmp_path / "message-root"),
        "storage_evidence": "operator_confirmed:fixture",
    }
    first = resolve_js8_storage({**base, "save_dir": str(tmp_path / "save-one")})
    second = resolve_js8_storage({**base, "save_dir": str(tmp_path / "save-two")})
    assert first.data_root == second.data_root
    assert first.all_path == second.all_path
    assert first.directed_path == second.directed_path
    assert first.inbox_path == second.inbox_path


def test_storage_collision_detects_symlink_equivalent_roots_without_scan(tmp_path: Path) -> None:
    root = tmp_path / "canonical-root"
    root.mkdir()
    link = tmp_path / "equivalent-root"
    link.symlink_to(root, target_is_directory=True)
    collisions = storage_collisions(
        (
            {"id": "a", "radio_name": "Radio A", "application_data_root": str(root)},
            {"id": "b", "radio_name": "Radio B", "application_data_root": str(link)},
        )
    )
    assert len(collisions) == 1
    assert collisions[0].labels == ("Radio A", "Radio B")
    assert collisions[0].instance_ids == ("a", "b")


def test_schema_adds_storage_fields_without_rewriting_legacy_paths(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE js8_instances (
            id INTEGER PRIMARY KEY AUTOINCREMENT, system_key TEXT UNIQUE,
            name TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
            host TEXT NOT NULL DEFAULT '127.0.0.1', port INTEGER NOT NULL DEFAULT 2442,
            offset_hz INTEGER NOT NULL DEFAULT 0, profile_path TEXT,
            directed_path TEXT, inbox_path TEXT, forms_path TEXT, install_path TEXT,
            spotter_launch_path TEXT, commstat_launch_path TEXT,
            created_utc TEXT NOT NULL, updated_utc TEXT NOT NULL
        )"""
    )
    conn.execute(
        "INSERT INTO js8_instances(system_key,name,directed_path,inbox_path,created_utc,updated_utc) VALUES (?,?,?,?,?,?)",
        ("legacy", "Legacy", "/old/DIRECTED.TXT", "/old/inbox.db3", "now", "now"),
    )
    conn.commit()
    conn.close()

    with MultiRadioStore(db_path).connect() as migrated:
        columns = {row[1] for row in migrated.execute("PRAGMA table_info(js8_instances)")}
        row = migrated.execute("SELECT directed_path,inbox_path FROM js8_instances WHERE system_key='legacy'").fetchone()
    assert {"variant_family", "variant_version", "rig_name", "application_data_root", "storage_mode"} <= columns
    assert tuple(row) == ("/old/DIRECTED.TXT", "/old/inbox.db3")


def test_namespace_fixture_keeps_save_folder_independent_from_message_root() -> None:
    for case in _cases():
        assert case["save_dir"] != case["expected_root"]

    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert set(payload["identity_invariants"]) == {
        "save_dir_is_not_message_storage_root",
        "settings_name_does_not_change_message_storage_root",
        "api_port_does_not_change_message_storage_root",
    }


def test_namespace_fixture_covers_symlink_collision_and_non_destructive_legacy_path() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    assert payload["collision"]["expected"] == "collision"
    assert payload["legacy"]["expected"] == "explicit_override_needs_verification"
    assert payload["legacy"]["source_content"] == "preserve-byte-for-byte"
