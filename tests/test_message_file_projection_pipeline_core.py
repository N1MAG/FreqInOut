from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from freqinout.core.message_file_projection_pipeline import MessageFileProjectionPipeline
from freqinout.core.message_file_scanner import FileRecord, FileScanDelta, file_path_display, file_path_from_key, file_path_key
from freqinout.core.message_projection_store import ensure_message_projection_schema
from freqinout.core.message_projection_writer import close_projection_writers


def _record(path: Path, *, size: int, mtime: float) -> FileRecord:
    return FileRecord(path=path, origin="flamp", size=size, mtime=mtime, source_id="flamp:radio-a")


def _schema(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()


def test_path_identity_is_reversible_and_printable_for_surrogate_paths() -> None:
    raw = Path("inbox/report-\udcff.k2s")
    key = file_path_key(raw)
    assert file_path_from_key(key) == raw
    rendered = file_path_display(raw)
    assert "\udcff" not in rendered
    assert "\\xff" in rendered


def test_incremental_pipeline_keeps_canonical_message_for_metadata_only_change(tmp_path: Path) -> None:
    db_path = tmp_path / "projection.sqlite"
    data_dir = tmp_path / "messages"
    data_dir.mkdir()
    path = data_dir / "Q123-04.k2s"
    path.write_text("first", encoding="utf-8")
    _schema(db_path)
    first = _record(path, size=5, mtime=10.0)
    initial = FileScanDelta.compare({}, {"flamp": [first]})
    pipeline = MessageFileProjectionPipeline(db_path)
    try:
        outcome = pipeline.run({"flamp": [first]}, {str(data_dir): 1.0}, "watch-a", initial)
        assert outcome.state == "updated"
        assert outcome.projected == 1
        assert outcome.inventory_writes == 2

        no_change = FileScanDelta.compare({"flamp": [first]}, {"flamp": [first]})
        stable = pipeline.run({"flamp": [first]}, {str(data_dir): 1.0}, "watch-a", no_change)
        assert stable.state == "unchanged"
        assert stable.inventory_writes == stable.projected == stable.transactions == 0

        changed = _record(path, size=9, mtime=11.0)
        change_delta = FileScanDelta.compare({"flamp": [first]}, {"flamp": [changed]})
        changed_result = pipeline.run({"flamp": [changed]}, {str(data_dir): 2.0}, "watch-a", change_delta)
        assert changed_result.projected == 1
        assert changed_result.tombstoned == 0
    finally:
        close_projection_writers()

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT deleted, subject FROM message_projection ORDER BY event_ts").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 0
        receipts = conn.execute(
            "SELECT external_key, metadata_json FROM message_external_refs ORDER BY external_key"
        ).fetchall()
        assert len(receipts) == 2
        receipt_presence = {row[0].rsplit(":", 2)[-2]: json.loads(row[1]).get("source_present", True) for row in receipts}
        assert receipt_presence == {"10.000000": False, "11.000000": True}
        assert conn.execute("SELECT COUNT(*) FROM message_file_scan_inventory").fetchone()[0] == 1
    finally:
        conn.close()


def test_incremental_pipeline_tombstones_canonical_message_for_content_change(tmp_path: Path) -> None:
    db_path = tmp_path / "projection.sqlite"
    data_dir = tmp_path / "messages"
    data_dir.mkdir()
    path = data_dir / "Q123-04.k2s"
    path.write_text("first", encoding="utf-8")
    _schema(db_path)
    first = _record(path, size=5, mtime=10.0)
    pipeline = MessageFileProjectionPipeline(db_path)
    try:
        initial = FileScanDelta.compare({}, {"flamp": [first]})
        assert pipeline.run({"flamp": [first]}, {str(data_dir): 1.0}, "watch-a", initial).projected == 1

        path.write_text("second", encoding="utf-8")
        changed = _record(path, size=6, mtime=11.0)
        change_delta = FileScanDelta.compare({"flamp": [first]}, {"flamp": [changed]})
        result = pipeline.run({"flamp": [changed]}, {str(data_dir): 2.0}, "watch-a", change_delta)
        assert result.projected == 1
        assert result.tombstoned == 1
    finally:
        close_projection_writers()

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT deleted FROM message_projection ORDER BY event_ts").fetchall()
        assert len(rows) == 2
        assert sorted(row[0] for row in rows) == [0, 1]
        receipts = conn.execute(
            "SELECT metadata_json FROM message_external_refs ORDER BY external_key"
        ).fetchall()
        receipt_presence = {json.loads(row[0]).get("source_present", True) for row in receipts}
        assert receipt_presence == {True, False}
    finally:
        conn.close()


def test_pipeline_escapes_surrogate_file_paths_before_sqlite_binding(tmp_path: Path) -> None:
    db_path = tmp_path / "projection.sqlite"
    _schema(db_path)
    record = FileRecord(Path("report-\udcff.k2s"), "flamp", 7, 3.0, "flamp:radio-a")
    pipeline = MessageFileProjectionPipeline(db_path)
    try:
        result = pipeline.run({"flamp": [record]}, {"inbox": 1.0}, "watch-a", FileScanDelta.compare({}, {"flamp": [record]}))
        assert result.state == "updated"
    finally:
        close_projection_writers()
    conn = sqlite3.connect(db_path)
    try:
        persisted = conn.execute("SELECT external_path FROM message_external_refs").fetchone()[0]
        assert "\udcff" not in persisted
        assert "\\xff" in persisted
    finally:
        conn.close()


def test_first_mip3_cache_run_projects_current_records_even_with_empty_delta(tmp_path: Path) -> None:
    """A legacy GUI scan cache cannot be mistaken for projection completeness."""

    db_path = tmp_path / "projection.sqlite"
    _schema(db_path)
    record = _record(tmp_path / "already-discovered.k2s", size=4, mtime=7.0)
    no_delta = FileScanDelta.compare({"flamp": [record]}, {"flamp": [record]})
    try:
        result = MessageFileProjectionPipeline(db_path).run(
            {"flamp": [record]}, {str(tmp_path): 1.0}, "watch-a", no_delta
        )
        assert result.state == "cached"
        assert result.projected == 1
    finally:
        close_projection_writers()

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM message_file_scan_inventory").fetchone()[0] == 1
    finally:
        conn.close()
