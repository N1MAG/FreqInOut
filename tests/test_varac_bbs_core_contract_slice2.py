from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

from freqinout.core.message_file_scanner import is_fio_bbs_helper_file_name
from freqinout.core.varac_bbs_library_store import (
    RETENTION_CLASS_KEEP,
    ensure_bbs_library_schema,
    list_bbs_admin_rows,
    list_bbs_location_manifest_rows,
    reconcile_bbs_publications,
    remove_bbs_artifact_from_all_locations,
    republish_bbs_artifact,
    set_bbs_artifact_keep,
    set_bbs_location_artifact,
    upsert_bbs_artifact_path,
    upsert_bbs_location,
)
from freqinout.core.varac_bbs_vault import (
    VaultLocation,
    _is_fio_bbs_generated_listing,
    _read_first_entry,
    _root_location_helper_entry,
    root_location_helper_filename_preview,
)


UTC = dt.timezone.utc


def _setup_catalog(tmp_path: Path, *, retention_days: int = 2) -> tuple[sqlite3.Connection, Path, str]:
    source = tmp_path / "source.txt"
    source.write_text("source remains unchanged\n", encoding="utf-8")
    conn = sqlite3.connect(tmp_path / "freqinout.db")
    ensure_bbs_library_schema(conn)
    artifact_id = upsert_bbs_artifact_path(conn, source_path=source)
    upsert_bbs_location(
        conn,
        location_id="intel",
        name="Intel",
        source_dir=str(tmp_path),
        retention_mode="expire_after_days",
        retention_days=retention_days,
    )
    upsert_bbs_location(
        conn,
        location_id="weather",
        name="Weather",
        source_dir=str(tmp_path),
        retention_mode="manual",
    )
    return conn, source, artifact_id


def test_keep_override_is_persistent_through_reconcile_and_clear_restores_normal_expiry(
    tmp_path: Path,
) -> None:
    conn, source, artifact_id = _setup_catalog(tmp_path)
    try:
        set_bbs_location_artifact(conn, location_id="intel", artifact_id=artifact_id)
        kept_expiry = set_bbs_artifact_keep(
            conn, artifact_id=artifact_id, location_id="intel", keep=True
        )
        assert kept_expiry == ""
        row = conn.execute(
            "SELECT retention_class, publish_enabled, expires_utc FROM bbs_location_artifacts"
        ).fetchone()
        assert row == (RETENTION_CLASS_KEEP, 1, None)

        reconcile_bbs_publications(
            conn, now_utc=dt.datetime(2035, 1, 1, tzinfo=UTC)
        )
        assert len(list_bbs_location_manifest_rows(conn, "intel")) == 1
        assert list_bbs_admin_rows(conn, location_id="intel")[0].retention_class == RETENTION_CLASS_KEEP

        cleared_expiry = set_bbs_artifact_keep(
            conn, artifact_id=artifact_id, location_id="intel", keep=False
        )
        assert cleared_expiry
        expected_expiry = dt.datetime.fromtimestamp(
            source.stat().st_mtime_ns / 1_000_000_000,
            tz=UTC,
        ) + dt.timedelta(days=2)
        assert abs(
            (dt.datetime.fromisoformat(cleared_expiry) - expected_expiry).total_seconds()
        ) < 0.00001
        assert conn.execute(
            "SELECT retention_class FROM bbs_location_artifacts"
        ).fetchone()[0] == "normal"
        assert source.read_text(encoding="utf-8") == "source remains unchanged\n"
    finally:
        conn.close()


def test_republish_starts_from_action_time_and_remove_preserves_source(tmp_path: Path) -> None:
    conn, source, artifact_id = _setup_catalog(tmp_path)
    try:
        set_bbs_location_artifact(
            conn,
            location_id="intel",
            artifact_id=artifact_id,
            publish_enabled=False,
        )
        source_mtime = source.stat().st_mtime_ns
        action_time = dt.datetime(2040, 4, 5, 6, 7, 8, tzinfo=UTC)
        expiry = republish_bbs_artifact(
            conn,
            artifact_id=artifact_id,
            location_id="intel",
            now_utc=action_time,
        )
        assert expiry == "2040-04-07T06:07:08+00:00"
        row = conn.execute(
            "SELECT publish_enabled, disabled_reason, retention_class, expires_utc "
            "FROM bbs_location_artifacts WHERE location_id='intel'"
        ).fetchone()
        assert row == (1, "", "normal", expiry)
        assert source.stat().st_mtime_ns == source_mtime

        set_bbs_location_artifact(conn, location_id="weather", artifact_id=artifact_id)
        assert remove_bbs_artifact_from_all_locations(conn, artifact_id=artifact_id) == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM bbs_location_artifacts WHERE artifact_id=? AND publish_enabled=1",
            (artifact_id,),
        ).fetchone()[0] == 0
        assert source.is_file()
        assert source.read_text(encoding="utf-8") == "source remains unchanged\n"
    finally:
        conn.close()


def test_helper_files_are_extensionless_but_historical_txt_names_are_recognized() -> None:
    location = VaultLocation(
        id="intel",
        name="Intel",
        source_dir="/tmp/intel",
        alias="INTEL",
        description="Latest reports",
    )
    logical = root_location_helper_filename_preview(
        location,
        default_location_id="default",
        global_code_policy="Allow public locations",
        order=20,
    )
    entry = _root_location_helper_entry(
        location,
        default_location_id="default",
        global_code_policy="Allow public locations",
        order=20,
    )
    first = _read_first_entry()

    assert logical == "20 type INTEL - open Intel - Latest reports"
    assert entry.name == logical
    assert not entry.name.lower().endswith(".txt")
    assert first.name == "00 HOW TO USE - Type command then refresh BBS"
    assert first.content == first.name + "\n"
    assert _is_fio_bbs_generated_listing("00 HOW TO USE - Type command then refresh BBS")
    assert _is_fio_bbs_generated_listing("00 READ FIRST - type command, then refresh BBS.txt")
    assert _is_fio_bbs_generated_listing("21 TYPE HUBS - open HUBS")
    assert is_fio_bbs_helper_file_name("00 HOW TO USE - Type command then refresh BBS")
    assert is_fio_bbs_helper_file_name("21 TYPE HUBS - open HUBS.txt")
    # Queue/block payload names retain their existing physical suffixes and
    # remain recognized by compatibility filtering.
    assert is_fio_bbs_helper_file_name("BBS_BLOCK_LIST_ABC.txt")
