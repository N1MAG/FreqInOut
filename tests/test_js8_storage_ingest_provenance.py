"""JSV-S3 contracts for JS8 message-store ingestion provenance.

These tests deliberately use the public runtime inventory and ingestion APIs.
They protect the boundary where a JS8 application's *message-storage root* is
converted into exactly one file/SQLite ingest cursor, rather than treating a
TCP port or SaveDir as storage identity.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pytest

from freqinout.core.ingest_source_model import build_ingest_source_inventory
from freqinout.core.js8_runtime_messages import ingest_js8_messages_for_runtime_sources


class DictSettings:
    def __init__(self, values: dict[str, object] | None = None) -> None:
        self.values = dict(values or {"operating_groups": []})

    def get(self, key: str, default: object = None) -> object:
        return self.values.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.values[key] = value


def _create_inbox(path: Path, text: str = "SAME TRAFFIC") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, json TEXT, type TEXT, value TEXT)")
        conn.execute(
            "INSERT INTO inbox_v1 (id, json, type, value) VALUES (1, ?, 'UNREAD', '')",
            (
                json.dumps(
                    {
                        "params": {
                            "TEXT": text,
                            "FROM": "K1AAA",
                            "TO": "@MAGNET",
                            "UTC": "2026-09-10 12:00:00",
                        }
                    }
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _profile(*, radio_id: str, root: Path, variant: str = "js8call", version: str = "2.2.0", evidence: str = "operator_confirmed:test") -> dict[str, object]:
    return {
        "id": radio_id,
        "name": f"Radio {radio_id}",
        "use_js8call": True,
        "js8_instance_id": f"instance-{radio_id}",
        "js8_host": "127.0.0.1",
        "js8_port": 2400 + int(ord(radio_id[0])),
        "variant_family": variant,
        "variant_version": version,
        "rig_name": f"rig-{radio_id}",
        "application_data_root": str(root),
        "storage_evidence": evidence,
    }


def _local_rows(profile_root: Path) -> list[tuple[str, int, str, str, str, str]]:
    conn = sqlite3.connect(profile_root / "config" / "freqinout_nets.db")
    try:
        return conn.execute(
            """
            SELECT source_key, source_id, source_radio_id, js8_instance_id, raw_text, source_path
              FROM js8_messages
             ORDER BY source_key, source_id
            """
        ).fetchall()
    finally:
        conn.close()


def test_verified_distinct_roots_keep_identical_inbox_content_as_distinct_radio_sources(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Content equality never collapses reception provenance across roots."""

    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    root_a, root_b = tmp_path / "root-a", tmp_path / "root-b"
    _create_inbox(root_a / "inbox.db3")
    _create_inbox(root_b / "inbox.db3")
    profiles = [_profile(radio_id="A", root=root_a), _profile(radio_id="B", root=root_b)]
    inventory = build_ingest_source_inventory(profiles)

    inbox_sources = [source for source in inventory.sources_for_family("js8call") if source.source_type == "sqlite"]
    assert len(inbox_sources) == 2
    assert {source.radio_id for source in inbox_sources} == {"A", "B"}
    assert {source.metadata["storage_mode"] for source in inbox_sources} == {"rig_scoped"}
    assert len({source.source_id for source in inbox_sources}) == 2

    result = ingest_js8_messages_for_runtime_sources(DictSettings(), inventory=inventory, profiles=profiles)  # type: ignore[arg-type]

    assert result.js8_inbox_sources == 2
    rows = _local_rows(profile_root)
    assert len(rows) == 2
    assert {row[2] for row in rows} == {"A", "B"}
    assert {row[3] for row in rows} == {"instance-A", "instance-B"}
    assert {row[4] for row in rows} == {"SAME TRAFFIC"}
    assert {row[5] for row in rows} == {str(root_a / "inbox.db3"), str(root_b / "inbox.db3")}
    assert len({row[0] for row in rows}) == 2


@pytest.mark.parametrize(
    ("variant", "version", "evidence", "expected_attribution"),
    (
        ("unknown", "", "platform_candidate", "unverified"),
    ),
)
def test_shared_or_unverified_root_is_coalesced_and_never_gets_radio_attribution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    variant: str,
    version: str,
    evidence: str,
    expected_attribution: str,
) -> None:
    """A canonical shared root has one cursor and explicit unattributed metadata."""

    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    root = tmp_path / "canonical-root"
    _create_inbox(root / "inbox.db3", "SHARED TRAFFIC")
    profiles = [_profile(radio_id="A", root=root, variant=variant, version=version, evidence=evidence)]
    # An unknown build is already unattributed with one canonical source,
    # before a second radio can be allowed to claim it.
    if expected_attribution == "shared":
        profiles.append(_profile(radio_id="B", root=root, variant=variant, version=version, evidence=evidence))
    inventory = build_ingest_source_inventory(profiles)

    sources = inventory.sources_for_family("js8call")
    # There is one canonical source for each root-owned artifact, not one per radio.
    assert len([source for source in sources if source.source_type == "sqlite"]) == 1
    assert len([source for source in sources if source.source_type == "file" and source.metadata.get("role") == "directed"]) == 1
    assert len([source for source in sources if source.source_type == "file" and source.metadata.get("role") == "all"]) == 1
    inbox_source = next(source for source in sources if source.source_type == "sqlite")
    assert inbox_source.radio_id == ""
    assert inbox_source.app_instance_id == ""
    assert inbox_source.metadata["attribution"] == expected_attribution
    expected_radios = ("A", "B") if expected_attribution == "shared" else ("A",)
    assert tuple(inbox_source.metadata["candidate_radio_ids"]) == expected_radios
    assert tuple(inbox_source.metadata["candidate_app_instance_ids"])

    result = ingest_js8_messages_for_runtime_sources(DictSettings(), inventory=inventory, profiles=profiles)  # type: ignore[arg-type]

    rows = _local_rows(profile_root)
    assert len(rows) == 1
    source_key, _source_id, radio_id, app_id, raw_text, source_path = rows[0]
    assert radio_id == ""
    assert app_id == ""
    assert raw_text == "SHARED TRAFFIC"
    assert source_path == str(root / "inbox.db3")
    assert expected_attribution in source_key


def test_locked_inbox_fails_promptly_and_does_not_hold_up_next_canonical_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """One writer lock is a local failure, never a cross-radio ingest stall."""

    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    locked_root, ready_root = tmp_path / "locked", tmp_path / "ready"
    _create_inbox(locked_root / "inbox.db3", "LOCKED")
    _create_inbox(ready_root / "inbox.db3", "NEXT SOURCE")
    profiles = [_profile(radio_id="A", root=locked_root), _profile(radio_id="B", root=ready_root)]
    inventory = build_ingest_source_inventory(profiles)

    lock = sqlite3.connect(locked_root / "inbox.db3", timeout=0)
    try:
        lock.execute("BEGIN EXCLUSIVE")
        started = time.monotonic()
        result = ingest_js8_messages_for_runtime_sources(DictSettings(), inventory=inventory, profiles=profiles)  # type: ignore[arg-type]
        elapsed = time.monotonic() - started
    finally:
        lock.rollback()
        lock.close()

    assert elapsed < 1.0
    rows = _local_rows(profile_root)
    assert [(row[2], row[4]) for row in rows] == [("B", "NEXT SOURCE")]


def test_restart_uses_canonical_root_checkpoint_without_duplicate_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A freshly built inventory retains the root cursor identity after restart."""

    profile_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile_root))
    root = tmp_path / "isolated"
    _create_inbox(root / "inbox.db3", "CHECKPOINTED")
    profiles = [_profile(radio_id="A", root=root)]

    first_inventory = build_ingest_source_inventory(profiles)
    first = ingest_js8_messages_for_runtime_sources(DictSettings(), inventory=first_inventory, profiles=profiles)  # type: ignore[arg-type]
    second_inventory = build_ingest_source_inventory(profiles)
    second = ingest_js8_messages_for_runtime_sources(DictSettings(), inventory=second_inventory, profiles=profiles)  # type: ignore[arg-type]

    assert first.js8_inbox_sources == 1
    assert second.js8_inbox_sources == 1
    rows = _local_rows(profile_root)
    assert len(rows) == 1
    assert rows[0][2:5] == ("A", "instance-A", "CHECKPOINTED")
    conn = sqlite3.connect(profile_root / "config" / "freqinout_nets.db")
    try:
        checkpoint_count = conn.execute("SELECT COUNT(*) FROM js8_ingest_checkpoint").fetchone()[0]
    finally:
        conn.close()
    assert checkpoint_count == 1
