from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from freqinout.core import varac_bbs_vault as vault
from freqinout.core.varac_bbs_library_store import (
    ensure_bbs_library_schema,
    set_bbs_location_artifact,
    upsert_bbs_artifact_path,
    upsert_bbs_location,
)
from freqinout.core.varac_bbs_vault import VaultLocation, publish_location_view, read_publish_manifest


def _catalog(db_path: Path, *, location_id: str, source: Path, live_name: str) -> None:
    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        artifact_id = upsert_bbs_artifact_path(conn, source_path=source, display_name=live_name)
        upsert_bbs_location(
            conn,
            location_id=location_id,
            name=location_id.title(),
            source_dir=source.parent,
        )
        set_bbs_location_artifact(
            conn,
            location_id=location_id,
            artifact_id=artifact_id,
            live_name=live_name,
        )
        conn.commit()


def test_publication_uses_explicit_catalog_identity_without_cross_runtime_leakage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_a = tmp_path / "runtime-a" / "freqinout.db"
    db_b = tmp_path / "runtime-b" / "freqinout.db"
    source_a = tmp_path / "runtime-a" / "locations" / "Intel" / "a.txt"
    source_b = tmp_path / "runtime-b" / "locations" / "Intel" / "b.txt"
    source_a.parent.mkdir(parents=True)
    source_b.parent.mkdir(parents=True)
    source_a.write_text("runtime A\n", encoding="utf-8")
    source_b.write_text("runtime B\n", encoding="utf-8")
    db_a.parent.mkdir(parents=True, exist_ok=True)
    db_b.parent.mkdir(parents=True, exist_ok=True)
    _catalog(db_a, location_id="intel", source=source_a, live_name="a.txt")
    _catalog(db_b, location_id="intel", source=source_b, live_name="b.txt")

    def fail_if_default_db_is_requested(_settings: object) -> Path:
        raise AssertionError("publication must not resolve the process-wide default BBS database")

    monkeypatch.setattr(vault, "bbs_library_db_path_from_settings", fail_if_default_db_is_requested)

    location_a = VaultLocation(id="intel", name="Intel", source_dir=str(source_a.parent), alias="INTEL")
    location_b = VaultLocation(id="intel", name="Intel", source_dir=str(source_b.parent), alias="INTEL")
    live_a = tmp_path / "runtime-a" / "live"
    live_b = tmp_path / "runtime-b" / "live"
    managed_a = tmp_path / "runtime-a" / "managed"
    managed_b = tmp_path / "runtime-b" / "managed"

    result_a = publish_location_view(
        location_a,
        live_bbs_dir=live_a,
        managed_root=managed_a,
        manifest_db_path=db_a,
    )
    result_b = publish_location_view(
        location_b,
        live_bbs_dir=live_b,
        managed_root=managed_b,
        manifest_db_path=db_b,
    )

    entries_a = read_publish_manifest(result_a.manifest_path)
    entries_b = read_publish_manifest(result_b.manifest_path)
    sources_a = {entry.source_path for entry in entries_a}
    sources_b = {entry.source_path for entry in entries_b}
    assert str(source_a.resolve()) in sources_a
    assert str(source_b.resolve()) not in sources_a
    assert str(source_b.resolve()) in sources_b
    assert str(source_a.resolve()) not in sources_b
    assert (live_a / "a.txt").read_text(encoding="utf-8") == "runtime A\n"
    assert (live_b / "b.txt").read_text(encoding="utf-8") == "runtime B\n"


def test_empty_publication_catalog_path_uses_filesystem_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_dir = tmp_path / "Intel"
    source_dir.mkdir()
    source = source_dir / "filesystem.txt"
    source.write_text("filesystem fallback\n", encoding="utf-8")

    def fail_if_default_db_is_requested(_settings: object) -> Path:
        raise AssertionError("an empty catalog path must not resolve the process-wide default BBS database")

    monkeypatch.setattr(vault, "bbs_library_db_path_from_settings", fail_if_default_db_is_requested)
    result = publish_location_view(
        VaultLocation(id="intel", name="Intel", source_dir=str(source_dir), alias="INTEL"),
        live_bbs_dir=tmp_path / "live",
        managed_root=tmp_path / "managed",
        manifest_db_path="",
    )

    entries = read_publish_manifest(result.manifest_path)
    assert any(entry.source_name == source.name for entry in entries)
    assert (tmp_path / "live" / "filesystem.txt").read_text(encoding="utf-8") == "filesystem fallback\n"
