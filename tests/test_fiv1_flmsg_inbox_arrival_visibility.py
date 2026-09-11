"""FIV-1 end-to-end contracts for FLMsg file arrival visibility.

These tests intentionally exercise the scanner, normalized file projection,
and bounded Inbox read model together.  They use filesystem arrival time as
the recent signal while retaining an old embedded/report timestamp in the
metadata cache.
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

from freqinout.core.message_file_projection_pipeline import MessageFileProjectionPipeline
from freqinout.core.message_file_metadata import ensure_message_file_metadata_table
from freqinout.core.message_file_scanner import MessageFileScanner
from freqinout.core.message_projection_store import (
    MAX_PROJECTED_MESSAGE_PAGE_SIZE,
    ensure_message_projection_schema,
    query_projected_message_page,
)
from freqinout.core.message_projection_writer import close_projection_writers
from freqinout.core.message_source_projectors import project_native_file_records


TARGET_NAMES = (
    "K8AXZ-OH-RR-20260512-2330Z_AIB__DISTRIBUTION_ETIQUETTE.k2s",
    "MAGNET_S2_RR_260907-1800Z_WEEKLY_SNAPSHOT.sig.b2s",
    "KC1VXQ-NH-RR-20260909-CA.k2s",
    "W6ZYC-CA-RR-20260908-2050Z-sit.k2s",
)


def _write_fixture(root: Path, *, competitors: int = 205) -> tuple[Path, float]:
    root.mkdir()
    now = time.time()
    for offset, name in enumerate(TARGET_NAMES):
        path = root / name
        path.write_bytes(b"NBEMS form payload\n")
        os.utime(path, (now - offset, now - offset))

    # These rows have newer embedded/event times but arrived just before the
    # target files.  Event-first bounded ordering would displace the targets.
    for index in range(competitors):
        path = root / f"competing-{index:03d}.txt"
        path.write_text("competing traffic\n", encoding="utf-8")
        arrival = now - 100 - index
        os.utime(path, (arrival, arrival))
    return root, now


def _old_report_metadata(db_path: Path, root: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        ensure_message_file_metadata_table(conn)
        with conn:
            for name in TARGET_NAMES:
                path = root / name
                stat = path.stat()
                conn.execute(
                    """
                    INSERT INTO message_file_metadata
                        (origin, path, mtime, size, source_family, msg_type,
                         display_type, title, report_ts, age_ts_source,
                         topics_json, actionable, search_text, indexed_ts)
                    VALUES ('flmsg', ?, ?, ?, 'flmsg', '', '', ?, ?,
                            'report', '[]', 0, ?, ?)
                    """,
                    (
                        str(path),
                        stat.st_mtime,
                        stat.st_size,
                        name,
                        1.0,  # deliberately old embedded/report time
                        name.lower(),
                        time.time(),
                    ),
                )
    finally:
        conn.close()


def test_fiv1_scan_projection_and_default_inbox_include_arrivals(tmp_path: Path) -> None:
    db_path = tmp_path / "projection.db"
    root, now = _write_fixture(tmp_path / "messages")
    _old_report_metadata(db_path, root)

    records, dir_mtimes, mode, delta = MessageFileScanner(
        [{"origin": "flmsg", "path": str(root)}], force=True
    ).scan_with_delta()
    assert mode == "full"
    assert {record.path.name for record in records["flmsg"]} >= set(TARGET_NAMES)

    try:
        outcome = MessageFileProjectionPipeline(db_path).run(
            records,
            dir_mtimes,
            "fiv1-flmsg-root",
            delta,
        )
        assert outcome.state == "updated"
        assert outcome.projected == len(records["flmsg"])
    finally:
        close_projection_writers()
    page = query_projected_message_page(
        db_path,
        source_families=("flmsg",),
        received_after_ts=now - 7 * 24 * 60 * 60,
        page_size=MAX_PROJECTED_MESSAGE_PAGE_SIZE,
        include_total=True,
    )
    names = {str(row["subject"]) for row in page.rows}
    assert set(TARGET_NAMES) <= names
    assert page.total_count == len(records["flmsg"])
    assert all(row["source_family"] == "flmsg" for row in page.rows)
    assert all(row["inbox_visible"] == 1 for row in page.rows)
    assert all(float(row["received_ts"]) >= now - 7 * 24 * 60 * 60 for row in page.rows)


def test_fiv1_received_first_keyset_pages_are_disjoint_and_deterministic(tmp_path: Path) -> None:
    db_path = tmp_path / "projection.db"
    root, now = _write_fixture(tmp_path / "messages")
    _old_report_metadata(db_path, root)
    records, _dir_mtimes, _mode = MessageFileScanner(
        [{"origin": "flmsg", "path": str(root)}], force=True
    ).scan()
    project_native_file_records(db_path, records, force=True)

    kwargs = {
        "source_families": ("flmsg",),
        "received_after_ts": now - 7 * 24 * 60 * 60,
        "page_size": MAX_PROJECTED_MESSAGE_PAGE_SIZE,
        "include_total": True,
    }
    first = query_projected_message_page(db_path, **kwargs)
    repeat = query_projected_message_page(db_path, **kwargs)
    second = query_projected_message_page(db_path, cursor=first.next_cursor, **kwargs)

    first_ids = [str(row["message_id"]) for row in first.rows]
    repeat_ids = [str(row["message_id"]) for row in repeat.rows]
    second_ids = [str(row["message_id"]) for row in second.rows]
    assert len(first.rows) == MAX_PROJECTED_MESSAGE_PAGE_SIZE
    assert first_ids == repeat_ids
    assert not (set(first_ids) & set(second_ids))
    assert first.total_count == len(first_ids) + len(second_ids)
    # The recent filesystem arrival is selected despite its old report time.
    assert set(TARGET_NAMES) <= {str(row["subject"]) for row in first.rows}
