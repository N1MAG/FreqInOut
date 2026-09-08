from __future__ import annotations

from pathlib import Path
import sqlite3
import time

import pytest

from freqinout.core.db_initializer import _ensure_js8_expect_tables
from freqinout.core.fio_spotter_store import (
    MAX_ACTIVITY_ROWS,
    delete_spotter_watch,
    list_spotter_activity,
    list_spotter_watches,
    record_spotter_watch_match,
    save_spotter_watch,
    watch_matches,
)
from freqinout.core.message_projection_store import (
    MessageProjectionRecord,
    MessageSourceRecord,
    upsert_projected_message,
)


def test_watch_crud_match_and_persist(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    saved = save_spotter_watch(
        {
            "name": "Regional fire",
            "watch_kind": "keyword",
            "pattern": "wildfire",
            "match_mode": "whole-word",
            "priority": "urgent",
            "source_families": ["spotter", "js8"],
            "source_radio_ids": ["7"],
        },
        db_path=db,
    )
    assert saved.created is True
    rows = list_spotter_watches(db_path=db)
    assert [row["name"] for row in rows] == ["Regional fire"]
    assert rows[0]["source_families"] == ["spotter", "js8"]
    assert watch_matches(rows[0], {"body_text": "New wildfire report"}) is True
    assert watch_matches(rows[0], {"body_text": "wildfires are possible"}) is False

    assert record_spotter_watch_match(saved.id, matched_ts=1234.0, db_path=db) is True
    restarted = list_spotter_watches(db_path=db)
    assert restarted[0]["match_count"] == 1
    assert restarted[0]["last_match_ts"] == 1234.0
    assert delete_spotter_watch(saved.id, db_path=db) is True
    assert list_spotter_watches(db_path=db) == []


def test_station_initializer_adds_watch_schema_idempotently_without_losing_rows(
    tmp_path: Path,
) -> None:
    db = tmp_path / "nets.db"
    with sqlite3.connect(db) as conn:
        _ensure_js8_expect_tables(conn)
        conn.execute(
            """
            INSERT INTO fio_spotter_watches(
                name, watch_kind, pattern, created_ts, updated_ts
            ) VALUES ('Existing', 'keyword', 'smoke', 1, 1)
            """
        )
        _ensure_js8_expect_tables(conn)
        conn.commit()
        assert conn.execute(
            "SELECT name, pattern FROM fio_spotter_watches"
        ).fetchall() == [("Existing", "smoke")]


@pytest.mark.parametrize("kind", ["unknown", "regex"])
def test_watch_rejects_unsupported_kind(kind: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        save_spotter_watch(
            {"name": "bad", "watch_kind": kind, "pattern": "x"},
            db_path=tmp_path / "nets.db",
        )


def test_activity_query_is_source_filtered_newest_first_and_bounded(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    spotter_source = MessageSourceRecord(source_id="spotter", source_family="spotter")
    for index in range(MAX_ACTIVITY_ROWS + 25):
        upsert_projected_message(
            db,
            source=spotter_source,
            message=MessageProjectionRecord(
                message_id=f"spotter:{index}",
                canonical_key=f"spotter:{index}",
                content_hash=f"hash:{index}",
                primary_source_id="spotter",
                source_family="spotter",
                from_call="W5TTA",
                to_call="MR08",
                group_name="MR08",
                summary=f"Message {index}",
                search_text=f"message {index}",
                event_ts=float(index),
                received_ts=float(index),
            ),
        )
    upsert_projected_message(
        db,
        source=MessageSourceRecord(source_id="commstat", source_family="commstat"),
        message=MessageProjectionRecord(
            message_id="commstat:ignored",
            canonical_key="commstat:ignored",
            content_hash="commstat:ignored",
            primary_source_id="commstat",
            source_family="commstat",
            from_call="OTHER",
            event_ts=9999.0,
            received_ts=9999.0,
        ),
    )
    rows = list_spotter_activity(db_path=db, group_name="MR08", limit=10000)
    assert len(rows) == MAX_ACTIVITY_ROWS
    assert rows[0]["message_id"] == f"spotter:{MAX_ACTIVITY_ROWS + 24}"
    assert all(row["source_family"] == "spotter" for row in rows)


def test_activity_read_remains_nonblocking_while_ingest_writer_is_active(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    source = MessageSourceRecord(source_id="spotter", source_family="spotter")
    upsert_projected_message(
        db,
        source=source,
        message=MessageProjectionRecord(
            message_id="spotter:one",
            canonical_key="spotter:one",
            content_hash="hash:one",
            primary_source_id="spotter",
            source_family="spotter",
            event_ts=1.0,
            received_ts=1.0,
        ),
    )
    writer = sqlite3.connect(db)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE message_projection SET summary='pending' WHERE message_id='spotter:one'")
        started = time.perf_counter()
        rows = list_spotter_activity(db_path=db)
        elapsed = time.perf_counter() - started
    finally:
        writer.rollback()
        writer.close()
    assert [row["message_id"] for row in rows] == ["spotter:one"]
    assert elapsed < 0.5
