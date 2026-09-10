"""MIP-2 durable dirty-work queue contract tests."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from freqinout.core.message_projection_store import ensure_message_projection_schema
from freqinout.core.message_projection_queue import (
    DirtyProjectionItem,
    SourceProjectionState,
    claim_ready,
    claim_ready_conn,
    complete_dirty_conn,
    enqueue_dirty,
    get_source_state,
    get_source_state_conn,
    queue_diagnostics,
    release_owner_leases_conn,
    retry_dirty_conn,
    upsert_source_state_conn,
)


def _db(tmp_path: Path) -> Path:
    path = tmp_path / "projection-queue.sqlite"
    conn = sqlite3.connect(path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()
    return path


def _item(
    key: str = "message-1",
    *,
    priority: int = 0,
    version: str = "v1",
    observed: str = "2026-09-10T12:00:00+00:00",
) -> DirtyProjectionItem:
    return DirtyProjectionItem(
        source_id="source-a",
        source_family="js8",
        external_kind="js8_message",
        external_key=key,
        priority=priority,
        source_version=version,
        first_observed_utc=observed,
        last_observed_utc=observed,
    )


def test_dirty_identity_coalesces_to_newest_version_and_highest_priority(tmp_path) -> None:
    db_path = _db(tmp_path)
    enqueue_dirty(db_path, _item(priority=1, version="v1"))
    enqueue_dirty(
        db_path,
        _item(priority=9, version="v2", observed="2026-09-10T12:01:00+00:00"),
    )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = tuple(conn.execute(
            "SELECT COUNT(*), priority, source_version, first_observed_utc, last_observed_utc "
            "FROM message_projection_dirty"
        ).fetchone())
    finally:
        conn.close()

    assert row == (1, 9, "v2", "2026-09-10T12:00:00+00:00", "2026-09-10T12:01:00+00:00")


def test_dirty_leases_are_exclusive_and_expire(tmp_path) -> None:
    db_path = _db(tmp_path)
    enqueue_dirty(db_path, _item())

    first = claim_ready(db_path, owner="worker-a", lease_seconds=0.05)
    assert len(first) == 1
    assert claim_ready(db_path, owner="worker-b") == ()

    time.sleep(0.07)
    expired = claim_ready(db_path, owner="worker-b")
    assert len(expired) == 1
    assert expired[0].lease_owner == "worker-b"


def test_dirty_completion_removes_only_owned_claim(tmp_path) -> None:
    db_path = _db(tmp_path)
    dirty_key = enqueue_dirty(db_path, _item())
    assert claim_ready(db_path, owner="worker-a")

    conn = sqlite3.connect(db_path)
    try:
        with conn:
            assert complete_dirty_conn(conn, [dirty_key], owner="worker-b") == 0
            assert complete_dirty_conn(conn, [dirty_key], owner="worker-a") == 1
    finally:
        conn.close()

    assert queue_diagnostics(db_path)["depth"] == 0


def test_dirty_retry_clears_lease_and_records_bounded_error(tmp_path) -> None:
    db_path = _db(tmp_path)
    dirty_key = enqueue_dirty(db_path, _item())
    assert claim_ready(db_path, owner="worker-a")

    conn = sqlite3.connect(db_path)
    try:
        with conn:
            assert retry_dirty_conn(
                conn,
                [dirty_key],
                owner="worker-a",
                delay_seconds=0.05,
                error_code="busy_database",
            ) == 1
        row = conn.execute(
            "SELECT attempt_count, retry_after_utc, last_error_code, lease_owner "
            "FROM message_projection_dirty WHERE dirty_key=?",
            (dirty_key,),
        ).fetchone()
    finally:
        conn.close()

    assert row[0] == 1
    assert row[1]
    assert row[2] == "busy_database"
    assert row[3] is None
    assert claim_ready(db_path, owner="worker-b") == ()


def test_dirty_work_survives_connection_restart_and_owner_release(tmp_path) -> None:
    db_path = _db(tmp_path)
    dirty_key = enqueue_dirty(db_path, _item())
    assert claim_ready(db_path, owner="old-process")

    # Simulate shutdown cleanup followed by a new process opening the same DB.
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            assert release_owner_leases_conn(conn, "old-process") == 1
    finally:
        conn.close()

    restarted = claim_ready(db_path, owner="new-process")
    assert [item.stable_key for item in restarted] == [dirty_key]
    assert queue_diagnostics(db_path)["depth"] == 1


def test_source_state_round_trips_watermark_and_diagnostics(tmp_path) -> None:
    db_path = _db(tmp_path)
    state = SourceProjectionState(
        source_id="source-a",
        source_family="commstat",
        high_water_key="artifact-42",
        high_water_ts=1_789_000_042.0,
        source_generation="generation-7",
        projector_version=3,
        classifier_version=4,
        last_reconciled_utc="2026-09-10T12:05:00+00:00",
        availability_state="available",
        diagnostics={"scanned": 42, "discovered": 1},
        updated_utc="2026-09-10T12:05:01+00:00",
    )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            upsert_source_state_conn(conn, state)
        loaded = get_source_state_conn(conn, "source-a")
    finally:
        conn.close()

    assert loaded is not None
    assert loaded.high_water_key == "artifact-42"
    assert loaded.high_water_ts == 1_789_000_042.0
    assert loaded.source_generation == "generation-7"
    assert loaded.diagnostics == {"discovered": 1, "scanned": 42}
    assert get_source_state(db_path, "source-a").availability_state == "available"


def test_empty_queue_diagnostics_are_read_only(tmp_path, monkeypatch) -> None:
    db_path = _db(tmp_path)
    import freqinout.core.message_projection_queue as queue_module

    statements: list[str] = []
    original_connect = queue_module.connect_sqlite_readonly

    def traced_connect(path, **kwargs):
        conn = original_connect(path, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(queue_module, "connect_sqlite_readonly", traced_connect)
    assert queue_diagnostics(db_path) == {
        "depth": 0,
        "oldest_utc": "",
        "leased": 0,
        "delayed": 0,
    }
    assert not any(statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE")) for statement in statements)
