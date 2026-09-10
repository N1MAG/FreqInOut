"""MIP-5 preflight qualification for message ingest and lifecycle isolation.

The tests use temporary SQLite databases and synthetic endpoint lanes.  They
are intentionally accelerated and deterministic; the Linux/macOS production
soak and physical endpoint gates remain separate operator runs.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

from freqinout.core import message_projection_coordinator as coordinator_module
from freqinout.core.js8_expect_store import evaluate_expect_request, save_expect_entry
from freqinout.core.mesh.lifecycle import MeshRetryPolicy, MeshRetryState
from freqinout.core.message_projection_coordinator import (
    MessageProjectionCoordinator,
    reconcile_native_source_changes,
)
from freqinout.core.message_projection_queue import (
    ensure_source_dirty_triggers,
    queue_diagnostics,
)
from freqinout.core.message_projection_store import ensure_message_projection_schema
from freqinout.core.message_projection_writer import close_projection_writers
from freqinout.core.scheduler_coordination import EndpointKey
from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry
from freqinout.core.varac_bbs_library_store import (
    ensure_bbs_library_schema,
    reconcile_bbs_publications,
)


def _create_db(tmp_path: Path, *, rows: int) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "mip5.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE js8_messages (
                id INTEGER PRIMARY KEY,
                from_call TEXT, to_call TEXT, msg_type TEXT,
                utc_str TEXT, utc_ts REAL, raw_text TEXT, decoded_text TEXT,
                state TEXT, read_ts REAL, flag_state INTEGER,
                source_key TEXT, source_id INTEGER, source_radio_id INTEGER,
                js8_instance_id TEXT, source_path TEXT
            )
            """
        )
        ensure_message_projection_schema(conn)
        ensure_source_dirty_triggers(conn)
        conn.executemany(
            """
            INSERT INTO js8_messages (
                id, from_call, to_call, msg_type, utc_str, utc_ts,
                raw_text, decoded_text, state, read_ts, flag_state,
                source_key, source_id, source_radio_id, js8_instance_id, source_path
            ) VALUES (?, ?, '@MR08', 'MSG', ?, ?, ?, ?, 'NEW', 0, 0, 'mip5', ?, 1, 'mip5', '')
            """,
            [
                (
                    index,
                    f"N0C{index:04d}",
                    f"2026-09-10T12:{index % 60:02d}:00+00:00",
                    float(index),
                    f"message {index}",
                    f"message {index}",
                    index,
                )
                for index in range(1, rows + 1)
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _drain(coordinator: MessageProjectionCoordinator, db_path: Path, *, expected: int) -> int:
    cycles = 0
    committed = 0
    while queue_diagnostics(db_path)["depth"]:
        result = coordinator.run_once(reconcile=False)
        cycles += 1
        committed += result.committed
        assert result.claimed <= 100
        assert cycles <= expected // 100 + 2
    return committed


def _close(coordinator: MessageProjectionCoordinator) -> None:
    coordinator.close()
    close_projection_writers()


def test_mip5_accelerated_500_message_burst_is_bounded_and_queryable(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path, rows=500)
    assert reconcile_native_source_changes(db_path, sources=("js8",), limit_per_source=1000) == {"js8": 500}
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        started = time.perf_counter()
        committed = _drain(coordinator, db_path, expected=500)
        elapsed = time.perf_counter() - started
        assert committed == 500
        assert elapsed < 5.0
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 500
            assert conn.execute("SELECT COUNT(DISTINCT message_id) FROM message_projection").fetchone()[0] == 500
        finally:
            conn.close()
    finally:
        _close(coordinator)


def test_mip5_scheduler_lane_remains_responsive_while_projection_prepares_burst(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _create_db(tmp_path, rows=500)
    reconcile_native_source_changes(db_path, sources=("js8",), limit_per_source=1000)
    coordinator = MessageProjectionCoordinator(db_path)
    scheduler = EndpointLaneRegistry()
    preparation_started = threading.Event()
    release_preparation = threading.Event()
    original_prepare = coordinator_module.prepare_native_message_bundles

    def blocked_prepare(conn, items):
        preparation_started.set()
        assert release_preparation.wait(2.0)
        return original_prepare(conn, items)

    monkeypatch.setattr(coordinator_module, "prepare_native_message_bundles", blocked_prepare)
    endpoint = EndpointKey.network("rigctld", "127.0.0.1", 4532, target="radio-b")
    completed = threading.Event()
    results: list[str] = []
    try:
        projection_future = coordinator.submit_once(reconcile=False)
        assert preparation_started.wait(1.0)
        submission = scheduler.submit(
            endpoint,
            occurrence_id="mip5-peer",
            operation=lambda: "healthy",
            completion=lambda result: (results.append(result.status), completed.set()),
            timeout_s=1.0,
        )
        assert submission.accepted is True
        assert completed.wait(0.5)
        assert results == ["applied_unverified"]
        release_preparation.set()
        assert projection_future.result(timeout=5.0).committed == 100
    finally:
        release_preparation.set()
        scheduler.shutdown(wait=True)
        _close(coordinator)


def test_mip5_mesh_retry_and_bbs_reconcile_remain_independent_during_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    message_db = _create_db(tmp_path / "messages", rows=500)
    reconcile_native_source_changes(message_db, sources=("js8",), limit_per_source=1000)
    coordinator = MessageProjectionCoordinator(message_db)
    preparation_started = threading.Event()
    release_preparation = threading.Event()
    original_prepare = coordinator_module.prepare_native_message_bundles

    def blocked_prepare(conn, items):
        preparation_started.set()
        assert release_preparation.wait(2.0)
        return original_prepare(conn, items)

    monkeypatch.setattr(coordinator_module, "prepare_native_message_bundles", blocked_prepare)
    bbs_db = tmp_path / "bbs.sqlite"
    try:
        projection_future = coordinator.submit_once(reconcile=False)
        assert preparation_started.wait(1.0)

        started = time.perf_counter()
        retry = MeshRetryState()
        assert retry.record_failure(1_000, MeshRetryPolicy(initial_delay_ms=250)) == 250
        assert retry.due(1_249) is False
        assert retry.due(1_250) is True
        assert time.perf_counter() - started < 0.1

        started = time.perf_counter()
        with sqlite3.connect(bbs_db) as conn:
            ensure_bbs_library_schema(conn)
            result = reconcile_bbs_publications(conn, batch_size=1)
            conn.commit()
        assert result.checked == 0
        assert time.perf_counter() - started < 0.5

        release_preparation.set()
        assert projection_future.result(timeout=5.0).committed == 100
    finally:
        release_preparation.set()
        _close(coordinator)


def test_mip5_idle_reconciliation_performs_zero_projection_dml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = _create_db(tmp_path, rows=1)
    reconcile_native_source_changes(db_path, sources=("js8",), limit_per_source=100)
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        assert coordinator.run_once(reconcile=False).committed == 1
        statements: list[str] = []
        original_connect = coordinator_module.connect_sqlite_runtime_write

        def traced_connect(*args, **kwargs):
            conn = original_connect(*args, **kwargs)
            conn.set_trace_callback(statements.append)
            return conn

        monkeypatch.setattr(coordinator_module, "connect_sqlite_runtime_write", traced_connect)
        original_reconcile = coordinator_module.reconcile_native_source_changes

        def reconcile_js8_only(path, **kwargs):
            return original_reconcile(path, sources=("js8",), **kwargs)

        monkeypatch.setattr(coordinator_module, "reconcile_native_source_changes", reconcile_js8_only)
        result = coordinator.run_once(reconcile=True)
        assert result.discovered == 0
        assert result.claimed == 0
        assert not any(statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE")) for statement in statements)
    finally:
        _close(coordinator)


def test_mip5_expect_fast_path_is_independent_of_saturated_projection_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    message_db = _create_db(tmp_path / "messages", rows=500)
    reconcile_native_source_changes(message_db, sources=("js8",), limit_per_source=1000)
    coordinator = MessageProjectionCoordinator(message_db)
    expect_db = tmp_path / "expect.sqlite"
    save_expect_entry(
        {
            "expect_key": "Q",
            "response_text": "Q YES",
            "source_scope": "all",
            "allow_any": True,
            "enabled": True,
            "auto_reply_enabled": True,
        },
        db_path=expect_db,
    )
    preparation_started = threading.Event()
    release_preparation = threading.Event()
    original_prepare = coordinator_module.prepare_native_message_bundles
    expect_metrics: list[tuple[str, dict[str, object]]] = []

    def capture_expect_metric(name, _elapsed_ms, **kwargs):
        expect_metrics.append((str(name), dict(kwargs.get("meta", {}) or {})))

    monkeypatch.setattr("freqinout.core.js8_expect_store.emit_span", capture_expect_metric)

    def blocked_prepare(conn, items):
        preparation_started.set()
        assert release_preparation.wait(2.0)
        return original_prepare(conn, items)

    monkeypatch.setattr(coordinator_module, "prepare_native_message_bundles", blocked_prepare)
    try:
        projection_future = coordinator.submit_once(reconcile=False)
        assert preparation_started.wait(1.0)
        started = time.perf_counter()
        evaluation = evaluate_expect_request(
            expect_key="Q",
            requesting_callsign="N0CALL",
            source_radio_id="1",
            js8_instance_id="mip5",
            db_path=expect_db,
            write_audit=False,
        )
        assert time.perf_counter() - started < 0.25
        assert evaluation.decision == "reply-ready"
        assert expect_metrics == [
            (
                "messages.expect_fast_path",
                {
                    "decision": "reply-ready",
                    "group_addressed": False,
                    "source_scoped": True,
                    "audit": False,
                },
            )
        ]
        release_preparation.set()
        assert projection_future.result(timeout=5.0).committed == 100
    finally:
        release_preparation.set()
        _close(coordinator)


def test_mip5_shutdown_and_restart_preserve_queued_work_without_duplicate_rows(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path, rows=205)
    reconcile_native_source_changes(db_path, sources=("js8",), limit_per_source=1000)
    first = MessageProjectionCoordinator(db_path)
    try:
        assert first.run_once(reconcile=False).committed == 100
    finally:
        first.close()
        close_projection_writers()
    assert queue_diagnostics(db_path)["depth"] == 105

    restarted = MessageProjectionCoordinator(db_path)
    try:
        assert _drain(restarted, db_path, expected=205) == 105
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 205
            assert conn.execute("SELECT COUNT(DISTINCT message_id) FROM message_projection").fetchone()[0] == 205
        finally:
            conn.close()
    finally:
        _close(restarted)


def test_mip5_qualification_tool_accelerated_preflight() -> None:
    from tools.message_ingest_mip5_soak import SoakOptions, run_soak

    result = run_soak(SoakOptions(accelerated=True, duration_sec=1.0, burst_count=500))
    assert result.passed is True
    assert result.projection_committed >= 500
    assert result.idle_discoveries == 0
    assert result.scheduler_commands == result.scheduler_completions
