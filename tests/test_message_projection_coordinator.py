"""MIP-2 bounded coordinator acceptance tests.

These tests use a small native JS8 table in a temporary database.  The table
is representative of the production identity/version columns and has the
same additive dirty triggers installed by startup migration.  The assertions
are deliberately about bounded work, durable queue state, and projection
identity rather than timing-sensitive implementation details.
"""

from __future__ import annotations

import sqlite3
import threading
import time
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
from freqinout.core.fio_spotter_store import list_spotter_watches, save_spotter_watch


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


def _insert_spotter_rows(db_path: Path, count: int) -> None:
    """Add a second valid native source for global-discovery-cap coverage."""

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE spotter_traffic (
                id INTEGER PRIMARY KEY,
                read_ts REAL,
                state TEXT,
                flag_state INTEGER,
                js8_instance_id TEXT,
                source_radio_id INTEGER
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO spotter_traffic (
                id, read_ts, state, flag_state, js8_instance_id, source_radio_id
            ) VALUES (?, 0, 'NEW', 0, 'spotter-test', 1)
            """,
            [(row_id,) for row_id in range(1, count + 1)],
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
    cycle_limit = coordinator_module.MAX_CYCLE_ITEMS
    cycles = []
    while queue_diagnostics(db_path)["depth"]:
        result = coordinator.run_once(reconcile=False)
        cycles.append(result)
        assert result.claimed <= cycle_limit
        assert len(cycles) <= (expected + cycle_limit - 1) // cycle_limit + 2
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
        assert len(cycles) == 20
        assert sum(result.claimed for result in cycles) == 500
        assert all(result.committed == result.claimed for result in cycles)
        assert _projection_counts(db_path) == (500, 500, 0)
    finally:
        worker.close()


def test_queue_first_catchup_limits_discovery_and_monotonically_drains_backlog(
    tmp_path, monkeypatch
) -> None:
    """A historical scan cannot add more work than one cycle can consume."""

    db_path = _db(tmp_path)
    _insert_messages(db_path, 250)
    # Simulate a pre-trigger historical source table: reconciliation must own
    # first discovery rather than simply consuming insert-trigger work.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM message_projection_dirty")
        conn.commit()
    finally:
        conn.close()
    worker = MessageProjectionCoordinator(db_path)
    try:
        # The production coordinator passes one global cap, unlike the public
        # reconciliation helper's deliberate per-source maintenance seam.
        observed_limits: list[tuple[int, int | None]] = []
        original_reconcile = coordinator_module.reconcile_native_source_changes

        def traced_reconcile(path, **kwargs):
            observed_limits.append((kwargs["limit_per_source"], kwargs.get("max_items")))
            return original_reconcile(path, **kwargs)

        monkeypatch.setattr(coordinator_module, "reconcile_native_source_changes", traced_reconcile)
        first = worker.run_once(reconcile=True)
        assert first.discovered == first.claimed == first.committed == 25
        assert observed_limits == [(25, 25)]
        assert queue_diagnostics(db_path)["depth"] == 0

        # Seed a durable historical backlog.  Subsequent coordinator cycles
        # must drain it first instead of discovering the remaining source rows.
        assert reconcile_native_source_changes(db_path, limit_per_source=1000)["js8"] == 225
        depths = [queue_diagnostics(db_path)["depth"]]
        assert depths == [225]

        def discovery_is_forbidden(*_args, **_kwargs):
            raise AssertionError("discovery ran while durable projection work remained")

        monkeypatch.setattr(coordinator_module, "reconcile_native_source_changes", discovery_is_forbidden)
        while queue_diagnostics(db_path)["depth"]:
            result = worker.run_once(reconcile=True)
            assert result.claimed <= coordinator_module.MAX_CYCLE_ITEMS
            assert result.committed == result.claimed
            depths.append(queue_diagnostics(db_path)["depth"])
        assert depths == sorted(depths, reverse=True)
        assert depths[-1] == 0
        assert _projection_counts(db_path) == (250, 250, 0)
    finally:
        worker.close()


def test_global_discovery_cap_stops_before_a_second_native_source(tmp_path) -> None:
    """One cycle cannot enqueue 100 rows for each available source table."""

    db_path = _db(tmp_path)
    _insert_messages(db_path, 100)
    _insert_spotter_rows(db_path, 100)
    discovered = reconcile_native_source_changes(
        db_path,
        sources=("js8", "spotter"),
        limit_per_source=100,
        max_items=100,
    )
    assert discovered == {"js8": 100, "spotter": 0}
    assert queue_diagnostics(db_path)["depth"] == 100
    conn = sqlite3.connect(db_path)
    try:
        js8_watermark = conn.execute(
            "SELECT high_water_key FROM message_projection_source_state "
            "WHERE source_id='native:js8_messages'"
        ).fetchone()
        spotter_watermark = conn.execute(
            "SELECT high_water_key FROM message_projection_source_state "
            "WHERE source_id='native:spotter_traffic'"
        ).fetchone()
        assert js8_watermark == ("100",)
        assert spotter_watermark is None
    finally:
        conn.close()


def test_prepare_slices_are_cancelable_and_release_durable_leases(tmp_path, monkeypatch) -> None:
    """Cancellation stops between short prepare slices without losing work."""

    db_path = _db(tmp_path)
    _insert_messages(db_path, 100)
    worker = MessageProjectionCoordinator(db_path)
    cancelled = threading.Event()
    prepared_sizes: list[int] = []
    original_prepare = coordinator_module.prepare_native_message_bundles

    def cancel_after_first_slice(conn, items):
        prepared_sizes.append(len(items))
        result = original_prepare(conn, items)
        cancelled.set()
        return result

    monkeypatch.setattr(coordinator_module, "prepare_native_message_bundles", cancel_after_first_slice)
    try:
        result = worker.run_once(reconcile=False, cancel_event=cancelled)
        assert result.state == "cancelled"
        assert prepared_sizes == [coordinator_module.PREPARE_ITEMS_PER_SLICE]
        diagnostics = queue_diagnostics(db_path)
        assert diagnostics["depth"] == 100
        assert diagnostics["leased"] == 0
        assert _projection_counts(db_path) == (0, 0, None)
    finally:
        worker.close()


def test_nonblocking_close_cancels_an_inflight_prepare_slice_promptly(tmp_path, monkeypatch) -> None:
    """Shutdown returns promptly while the current bounded slice unwinds."""

    db_path = _db(tmp_path)
    _insert_messages(db_path, 25)
    worker = MessageProjectionCoordinator(db_path)
    entered_prepare = threading.Event()
    original_prepare = coordinator_module.prepare_native_message_bundles

    def wait_for_shutdown(conn, items):
        entered_prepare.set()
        assert worker._cancel.wait(1.0)
        return original_prepare(conn, items)

    monkeypatch.setattr(coordinator_module, "prepare_native_message_bundles", wait_for_shutdown)
    future = worker.submit_once(reconcile=False)
    assert entered_prepare.wait(1.0)
    started = time.monotonic()
    worker.close(wait=False)
    assert time.monotonic() - started < 0.25
    result = future.result(timeout=1.0)
    assert result.state == "cancelled"
    diagnostics = queue_diagnostics(db_path)
    assert diagnostics["depth"] == 25
    assert diagnostics["leased"] == 0


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


def test_projection_watch_matching_is_background_bounded_and_idempotent(tmp_path) -> None:
    db_path = _db(tmp_path)
    watch = save_spotter_watch(
        {
            "name": "Selected operator",
            "watch_kind": "callsign",
            "pattern": "N0CALL1",
            "match_mode": "exact",
            "enabled": True,
        },
        db_path=db_path,
    )
    _insert_messages(db_path, 1)
    worker = MessageProjectionCoordinator(db_path)
    try:
        result = worker.run_once(reconcile=False)
        assert result.committed == 1
        assert list_spotter_watches(db_path=db_path)[0]["match_count"] == 1

        # A changed native row is reprojected, but the same stable message ID
        # cannot increment the watch a second time.
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("UPDATE js8_messages SET read_ts=1 WHERE id=1")
            conn.commit()
        finally:
            conn.close()
        replay = worker.run_once(reconcile=False)
        assert replay.committed == 1
        row = next(item for item in list_spotter_watches(db_path=db_path) if item["id"] == watch.id)
        assert row["match_count"] == 1
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
        assert sum(
            result.claimed for result in _drain(db_path, expected=100, coordinator=worker)
        ) == 100

        second = reconcile_native_source_changes(db_path, limit_per_source=100)
        assert second["js8"] == 100
        state = _source_state(db_path)
        assert state[0:3] == ("200", 99, 99)
        assert queue_diagnostics(db_path)["depth"] == 100
        assert sum(
            result.claimed for result in _drain(db_path, expected=100, coordinator=worker)
        ) == 100

        third = reconcile_native_source_changes(db_path, limit_per_source=100)
        assert third["js8"] == 50
        state = _source_state(db_path)
        assert state[0:3] == ("250", 99, 99)
        assert queue_diagnostics(db_path)["depth"] == 50
        assert sum(
            result.claimed for result in _drain(db_path, expected=50, coordinator=worker)
        ) == 50
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
