from __future__ import annotations

import sqlite3
from pathlib import Path

from freqinout.core.varac_bbs_library_store import (
    ensure_bbs_library_schema,
    list_bbs_location_manifest_rows,
    sync_bbs_location_from_folder,
    set_bbs_location_artifact,
    upsert_bbs_artifact_path,
    upsert_bbs_location,
)
from freqinout.core.varac_bbs_vault import VaultLocation, publish_location_view, read_publish_manifest


def test_db_backed_manifest_publishes_one_artifact_to_multiple_locations(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    source = tmp_path / "canonical" / "status.txt"
    source.parent.mkdir()
    source.write_text("regional status\n", encoding="utf-8")
    live_bbs = tmp_path / "VarAC" / "BBS"
    managed_root = tmp_path / "VarAC" / "FIO_BBS_Vault"

    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        artifact_id = upsert_bbs_artifact_path(conn, source_path=source, source_kind="operator_file")
        upsert_bbs_location(conn, location_id="intel", name="Intel")
        upsert_bbs_location(conn, location_id="weather", name="Weather")
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id, live_name="intel-status.txt")
        set_bbs_location_artifact(conn, location_id="weather", artifact_id=artifact_id, live_name="weather-status.txt")
        conn.commit()

    intel = VaultLocation(id="intel", name="Intel", source_dir="", alias="INTEL")
    weather = VaultLocation(id="weather", name="Weather", source_dir="", alias="WX")

    result_intel = publish_location_view(
        intel,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        manifest_db_path=db_path,
    )
    assert result_intel.published_count == 4
    assert (live_bbs / "intel-status.txt").read_text(encoding="utf-8") == "regional status\n"
    manifest = read_publish_manifest(result_intel.manifest_path)
    assert any(entry.source_path == str(source.resolve()) for entry in manifest)

    result_weather = publish_location_view(
        weather,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        manifest_db_path=db_path,
    )
    assert result_weather.published_count == 1
    assert result_weather.removed_count == 1
    assert not (live_bbs / "intel-status.txt").exists()
    assert (live_bbs / "weather-status.txt").read_text(encoding="utf-8") == "regional status\n"

    with sqlite3.connect(db_path) as conn:
        rows = list_bbs_location_manifest_rows(conn, "intel")
    assert len(rows) == 1
    assert rows[0].source_path == str(source.resolve())


def test_db_unpublish_disables_membership_without_deleting_artifact_or_source(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    source = tmp_path / "canonical" / "report.txt"
    source.parent.mkdir()
    source.write_text("keep me\n", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        artifact_id = upsert_bbs_artifact_path(conn, source_path=source)
        upsert_bbs_location(conn, location_id="intel", name="Intel")
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id, publish_enabled=True)
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id, publish_enabled=False)
        conn.commit()

        rows = list_bbs_location_manifest_rows(conn, "intel")
        artifact_count = conn.execute("SELECT COUNT(*) FROM bbs_artifacts WHERE artifact_id=?", (artifact_id,)).fetchone()[0]

    assert rows == []
    assert artifact_count == 1
    assert source.read_text(encoding="utf-8") == "keep me\n"


def test_one_station_catalog_materializes_through_two_radio_live_directories(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    source = tmp_path / "canonical" / "shared-status.txt"
    source.parent.mkdir()
    source.write_text("shared station status\n", encoding="utf-8")
    managed_root = tmp_path / "FIO_BBS_Vault"
    live_a = tmp_path / "FIO-A" / "BBS"
    live_b = tmp_path / "FIO-B" / "BBS"

    with sqlite3.connect(db_path) as conn:
        artifact_id = upsert_bbs_artifact_path(conn, source_path=source)
        upsert_bbs_location(conn, location_id="intel", name="Intel", retention_mode="manual")
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id)
        conn.commit()

    location = VaultLocation(id="intel", name="Intel", source_dir="", alias="INTEL")
    result_a = publish_location_view(
        location,
        live_bbs_dir=live_a,
        managed_root=managed_root,
        manifest_db_path=db_path,
    )
    result_b = publish_location_view(
        location,
        live_bbs_dir=live_b,
        managed_root=managed_root,
        manifest_db_path=db_path,
    )

    assert (live_a / source.name).read_text(encoding="utf-8") == "shared station status\n"
    assert (live_b / source.name).read_text(encoding="utf-8") == "shared station status\n"
    assert result_a.manifest_path != result_b.manifest_path
    assert read_publish_manifest(result_a.manifest_path) == read_publish_manifest(result_b.manifest_path)


def test_db_folder_sync_preserves_operator_disabled_membership_and_missing_source_state(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    source_dir = tmp_path / "managed" / "Intel"
    source_dir.mkdir(parents=True)
    source = source_dir / "status.txt"
    source.write_text("regional status\n", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        ensure_bbs_library_schema(conn)
        sync_bbs_location_from_folder(conn, location_id="intel", name="Intel", source_dir=source_dir, enabled=True)
        artifact_id = conn.execute(
            "SELECT artifact_id FROM bbs_artifacts WHERE source_path=? LIMIT 1",
            (str(source.resolve()),),
        ).fetchone()[0]
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id, publish_enabled=False)
        conn.commit()

        sync_bbs_location_from_folder(conn, location_id="intel", name="Intel", source_dir=source_dir, enabled=True)
        publish_row = conn.execute(
            """
            SELECT publish_enabled
            FROM bbs_location_artifacts
            WHERE location_id=? AND artifact_id=?
            """,
            ("intel", artifact_id),
        ).fetchone()
        artifact_row = conn.execute(
            """
            SELECT deleted
            FROM bbs_artifacts
            WHERE artifact_id=?
            """,
            (artifact_id,),
        ).fetchone()
        manifest_rows = list_bbs_location_manifest_rows(conn, "intel")

        source.unlink()
        sync_bbs_location_from_folder(conn, location_id="intel", name="Intel", source_dir=source_dir, enabled=True)
        missing_row = conn.execute(
            """
            SELECT publish_enabled
            FROM bbs_location_artifacts
            WHERE location_id=? AND artifact_id=?
            """,
            ("intel", artifact_id),
        ).fetchone()
        missing_artifact_row = conn.execute(
            """
            SELECT source_state, deleted
            FROM bbs_artifacts
            WHERE artifact_id=?
            """,
            (artifact_id,),
        ).fetchone()
        missing_manifest_rows = list_bbs_location_manifest_rows(conn, "intel")

        source.write_text("regional status\n", encoding="utf-8")
        sync_bbs_location_from_folder(conn, location_id="intel", name="Intel", source_dir=source_dir, enabled=True)
        restored_row = conn.execute(
            """
            SELECT publish_enabled
            FROM bbs_location_artifacts
            WHERE location_id=? AND artifact_id=?
            """,
            ("intel", artifact_id),
        ).fetchone()
        restored_artifact_row = conn.execute(
            """
            SELECT deleted
            FROM bbs_artifacts
            WHERE artifact_id=?
            """,
            (artifact_id,),
        ).fetchone()
        restored_manifest_rows = list_bbs_location_manifest_rows(conn, "intel")

    assert publish_row == (0,)
    assert artifact_row == (0,)
    assert manifest_rows == []
    assert missing_row == (0,)
    assert missing_artifact_row == ("missing", 0)
    assert missing_manifest_rows == []
    assert restored_row == (0,)
    assert restored_artifact_row == (0,)
    assert restored_manifest_rows == []
