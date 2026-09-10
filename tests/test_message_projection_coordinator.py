"""MIP-2 bounded coordinator acceptance tests.

These tests use a small native JS8 table in a temporary database.  The table
is representative of the production identity/version columns and has the
same additive dirty triggers installed by startup migration.  The assertions
are deliberately about bounded work, durable queue state, and projection
identity rather than timing-sensitive implementation details.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from freqinout.core import message_projection_coordinator as coordinator_module
from freqinout.core.message_projection_coordinator import (
    MessageProjectionCoordinator,
    reconcile_native_source_changes,
)
from freqinout.core.message_projection_queue import (
    claim_ready,
    ensure_source_dirty_triggers,
    queue_diagnostics,
    release_owner_leases_conn,
)
from freqinout.core.message_projection_store import ensure_message_projection_schema


def _db(tmp_path: Path) -> Path:
    path = tmp_path / "message-projection-coordinator.sqlite"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE js8_messages (
                id INTEGER PRIMARY KEY,
                from_call TEXT,
                to_call TEXT,
                msg_type TEXT,
                utc_str TEXT,
                utc_ts REAL,
                raw_text TEXT,
                decoded_text TEXT,
                state TEXT,
                read_ts REAL,
                flag_state INTEGER,
                source_key TEXT,
                source_id INTEGER,
                source_radio_id INTEGER,
                js8_instance_id TEXT,
                source_path TEXT
            )
            """
        )
        ensure_message_projection_schema(conn)
        ensure_source_dirty_triggers(conn)
        conn.commit()
    finally:
        conn.close()
    return path


def _insert_messages(db_path: Path, count: int, *, start: int = 1) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(
            """
            INSERT INTO js8_messages (
                id, from_call, to_call, msg_type, utc_str, utc_ts,
                raw_text, decoded_text, state, read_ts, flag_state,
                source_key, source_id
            ) VALUES (?, ?, '@MR08', 'MSG', ?, ?, ?, ?, 'NEW', 0, 0, 'test', ?)
            """,
            [
                (
                    row_id,
                    f"N0CALL{row_id}",
                    f"2026-09-10T12:{row_id % 60:02d}:00+00:00",
                    float(row_id),
                    f"message {row_id}",
                    f"message {row_id}",
                    row_id,
                )
                for row_id in range(start, start + count)
            ],
        )
        conn.commit()
    finally:
        conn.close()

def _projection_counts(db_path: Path) -> tuple[int, int, int]:
    conn = sqlite3.connect(db_path)
    try:
        return tuple(
            conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT message_id), SUM(deleted) "
                "FROM message_projection"
            ).fetchone()
        )
    finally:
        conn.close()


def _drain(db_path: Path, *, expected: int, coordinator: MessageProjectionCoordinator) -> list:
    cycles = []
    while queue_diagnostics(db_path)["depth"]:
        result = coordinator.run_once(reconcile=False)
        cycles.append(result)
        assert result.claimed <= 100
        assert len(cycles) <= expected // 100 + 2
    return cycles


def test_unchanged_reconciliation_has_no_dml_after_state_exists(tmp_path, monkeypatch) -> None:
    db_path = _db(tmp_path)
    _insert_messages(db_path, 1)
    assert reconcile_native_source_changes(db_path, limit_per_source=100)["js8"] == 1
    worker = MessageProjectionCoordinator(db_path)
    try:
        assert worker.run_once(reconcile=False).committed == 1

        statements: list[str] = []
        original_connect = coordinator_module.connect_sqlite_runtime_write

        def traced_connect(*args, **kwargs):
            conn = original_connect(*args, **kwargs)
            conn.set_trace_callback(statements.append)
            return conn

        monkeypatch.setattr(coordinator_module, "connect_sqlite_runtime_write", traced_connect)
        assert reconcile_native_source_changes(db_path, limit_per_source=100)["js8"] == 0
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))
            for statement in statements
        )
    finally:
        worker.close()


def test_one_new_row_projects_one_identity_without_fixed_replay(tmp_path, monkeypatch) -> None:
    db_path = _db(tmp_path)
    _insert_messages(db_path, 2)
    assert reconcile_native_source_changes(db_path, limit_per_source=100)["js8"] == 2
    worker = MessageProjectionCoordinator(db_path)
    try:
        assert worker.run_once(reconcile=False).committed == 2
        _insert_messages(db_path, 1, start=3)

        prepared_keys: list[str] = []
        original_prepare = coordinator_module.prepare_native_message_bundles

        def traced_prepare(conn, items):
            prepared_keys.extend(item.external_key for item in items)
            return original_prepare(conn, items)

        monkeypatch.setattr(coordinator_module, "prepare_native_message_bundles", traced_prepare)
        result = worker.run_once(reconcile=False)
        assert result.claimed == 1
        assert result.prepared == 1
        assert result.committed == 1
        assert prepared_keys == ["3"]
        assert _projection_counts(db_path) == (3, 3, 0)
    finally:
        worker.close()


def test_five_hundred_message_burst_drains_in_bounded_cycles(tmp_path) -> None:
    db_path = _db(tmp_path)
    _insert_messages(db_path, 500)
    assert reconcile_native_source_changes(db_path, limit_per_source=1000)["js8"] == 500
    worker = MessageProjectionCoordinator(db_path)
    try:
        cycles = _drain(db_path, expected=500, coordinator=worker)
        assert len(cycles) == 5
        assert sum(result.claimed for result in cycles) == 500
        assert all(result.committed == result.claimed for result in cycles)
        assert _projection_counts(db_path) == (500, 500, 0)
    finally:
        worker.close()


def test_restart_after_lease_release_has_no_loss_or_duplicate_projection(tmp_path) -> None:
    db_path = _db(tmp_path)
    _insert_messages(db_path, 5)
    assert reconcile_native_source_changes(db_path, limit_per_source=100)["js8"] == 5
    leased = claim_ready(db_path, owner="crashed-process", limit=100)
    assert len(leased) == 5

    conn = sqlite3.connect(db_path)
    try:
        with conn:
            assert release_owner_leases_conn(conn, "crashed-process") == 5
    finally:
        conn.close()

    restarted = MessageProjectionCoordinator(db_path)
    try:
        cycles = _drain(db_path, expected=5, coordinator=restarted)
        assert sum(result.committed for result in cycles) == 5
        assert _projection_counts(db_path) == (5, 5, 0)
    finally:
        restarted.close()


def test_source_delete_tombstones_existing_projection(tmp_path) -> None:
    db_path = _db(tmp_path)
    _insert_messages(db_path, 1)
    assert reconcile_native_source_changes(db_path, limit_per_source=100)["js8"] == 1
    worker = MessageProjectionCoordinator(db_path)
    try:
        assert worker.run_once(reconcile=False).committed == 1
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("DELETE FROM js8_messages WHERE id=1")
            conn.commit()
        finally:
            conn.close()
        result = worker.run_once(reconcile=False)
        assert result.deleted == 1
        assert queue_diagnostics(db_path)["depth"] == 0
        assert _projection_counts(db_path) == (1, 1, 1)
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT deleted, deleted_utc FROM message_projection"
            ).fetchone()[0:1] == (1,)
        finally:
            conn.close()
    finally:
        worker.close()


def test_projector_version_reset_replays_in_bounded_resumable_batches(tmp_path, monkeypatch) -> None:
    db_path = _db(tmp_path)
    _insert_messages(db_path, 250)
    assert reconcile_native_source_changes(db_path, limit_per_source=1000)["js8"] == 250
    worker = MessageProjectionCoordinator(db_path)
    try:
        _drain(db_path, expected=250, coordinator=worker)
        monkeypatch.setattr(coordinator_module, "PROJECTOR_VERSION", 99)
        monkeypatch.setattr(coordinator_module, "JS8_MESSAGE_POLICY_VERSION", 99)

        first = reconcile_native_source_changes(db_path, limit_per_source=100)
        assert first["js8"] == 100
        state = _source_state(db_path)
        assert state[0:3] == ("100", 99, 99)
        assert queue_diagnostics(db_path)["depth"] == 100
        assert worker.run_once(reconcile=False).claimed == 100

        second = reconcile_native_source_changes(db_path, limit_per_source=100)
        assert second["js8"] == 100
        state = _source_state(db_path)
        assert state[0:3] == ("200", 99, 99)
        assert queue_diagnostics(db_path)["depth"] == 100
        assert worker.run_once(reconcile=False).claimed == 100

        third = reconcile_native_source_changes(db_path, limit_per_source=100)
        assert third["js8"] == 50
        state = _source_state(db_path)
        assert state[0:3] == ("250", 99, 99)
        assert queue_diagnostics(db_path)["depth"] == 50
        assert worker.run_once(reconcile=False).claimed == 50
        assert _projection_counts(db_path) == (250, 250, 0)
    finally:
        worker.close()


def _source_state(db_path: Path) -> tuple[str, int, int]:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT high_water_key, projector_version, classifier_version "
            "FROM message_projection_source_state WHERE source_id='native:js8_messages'"
        ).fetchone()
    finally:
        conn.close()
