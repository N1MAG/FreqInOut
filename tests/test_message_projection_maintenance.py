"""MIP-5 post-shell message-projection maintenance tests."""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import freqinout.core.message_projection_maintenance as maintenance_module
from freqinout.core.message_projection_coordinator import ProjectionCycleResult
from freqinout.core.message_projection_maintenance import (
    DEEP_REBUILD_CHECKPOINT_SOURCE,
    DeepRebuildPreview,
    MessageProjectionMaintenanceService,
)
from freqinout.core.message_projection_queue import (
    SourceProjectionState,
    get_source_state,
    upsert_source_state_conn,
)
from freqinout.core.message_projection_store import (
    ensure_message_projection_schema,
    get_message_projection_checkpoint,
)


class _BatchCoordinator:
    """Deterministic coordinator double proving the service never widens a batch."""

    def __init__(self, batches: int, *, cancel_after: int = 0, event: threading.Event | None = None) -> None:
        self.remaining = int(batches)
        self.cancel_after = int(cancel_after)
        self.event = event
        self.calls = 0

    def run_once(self, *, reconcile: bool = True, cancel_event=None) -> ProjectionCycleResult:
        assert reconcile is True
        self.calls += 1
        if self.remaining <= 0:
            return ProjectionCycleResult()
        self.remaining -= 1
        if self.cancel_after and self.calls >= self.cancel_after and self.event is not None:
            self.event.set()
        return ProjectionCycleResult(
            discovered=100,
            claimed=100,
            prepared=100,
            committed=100,
            state="committed",
        )


class _BlockedCoordinator:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def run_once(self, *, reconcile: bool = True, cancel_event=None) -> ProjectionCycleResult:
        assert reconcile is True
        self.started.set()
        assert self.release.wait(2.0)
        return ProjectionCycleResult(discovered=1, state="committed")


class _BusyOnceCoordinator:
    def __init__(self) -> None:
        self.calls = 0

    def run_once(self, *, reconcile: bool = True, cancel_event=None) -> ProjectionCycleResult:
        assert reconcile is True
        self.calls += 1
        if self.calls == 1:
            raise sqlite3.OperationalError("database is locked")
        if self.calls == 2:
            return ProjectionCycleResult(
                discovered=1,
                claimed=1,
                prepared=1,
                committed=1,
                state="committed",
            )
        return ProjectionCycleResult()


class _RepairCoordinator(_BatchCoordinator):
    def __init__(self) -> None:
        super().__init__(0)
        self.repair_calls = 0

    def repair_legacy_duplicates(self, *, cancel_event=None):
        self.repair_calls += 1
        return SimpleNamespace(
            state="committed",
            planned=3,
            repaired=3,
            transactions=1,
            max_transaction_ms=4.0,
        )


def _empty_db(tmp_path: Path, name: str = "maintenance.sqlite") -> Path:
    path = tmp_path / name
    conn = sqlite3.connect(path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()
    return path


def test_explicit_deep_rebuild_converges_legacy_presentations_before_complete(
    tmp_path,
) -> None:
    coordinator = _RepairCoordinator()
    service = MessageProjectionMaintenanceService(
        _empty_db(tmp_path), coordinator=coordinator, yield_seconds=0
    )
    try:
        result = service.run_post_shell_catchup(
            rebuild_id="repair-rebuild", source_rows_estimate=10
        )
        assert result.state == "complete"
        assert result.repaired == 3
        assert result.processed == 3
        assert coordinator.repair_calls == 1
    finally:
        service.close(wait=True)


def _native_db(tmp_path: Path) -> Path:
    path = _empty_db(tmp_path, "maintenance-native.sqlite")
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
        conn.execute(
            """
            INSERT INTO js8_messages(
                id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text,
                decoded_text, state, read_ts, flag_state, source_key, source_id
            ) VALUES (1, 'N0TEST', '@MR08', 'MSG', '2026-09-10T12:00:00+00:00',
                      1.0, 'maintenance test', 'maintenance test', 'NEW', 0, 0,
                      'test', 1)
            """
        )
        with conn:
            upsert_source_state_conn(
                conn,
                SourceProjectionState(
                    source_id="native:js8_messages",
                    source_family="js8",
                    high_water_key="1",
                    source_generation="1",
                    projector_version=3,
                    availability_state="available",
                ),
            )
    finally:
        conn.close()
    return path


def test_post_shell_catchup_drains_twelve_thousand_rows_in_100_item_cycles(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    coordinator = _BatchCoordinator(120)
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=coordinator, yield_seconds=0
    )
    try:
        result = service.run_post_shell_catchup()
        assert result.state == "complete"
        assert result.committed == 12_000
        # One final idle reconciliation confirms the durable queue has drained.
        assert result.cycles == 121
        assert coordinator.calls == 121
    finally:
        service.close()


def test_catchup_coalesces_ui_progress_but_always_publishes_terminal_state(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=_BatchCoordinator(30), yield_seconds=0
    )
    published = []
    service.set_progress_callback(published.append)
    try:
        result = service.run_post_shell_catchup()
        committed = [item.committed for item in published if item.committed]
        assert result.state == "complete"
        # The dialog polls the bounded in-memory snapshot.  Cross-thread UI
        # notifications are intentionally coalesced rather than queued for
        # every 25/100-row cycle of a large historical rebuild.
        assert committed[-1] == 3_000
        assert len(committed) < 30
    finally:
        service.close()


def test_rebuild_checkpoint_is_periodic_and_terminal_state_is_exact(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=_BatchCoordinator(23), yield_seconds=0
    )
    persisted: list[tuple[int, str]] = []
    original = service._persist_rebuild_progress

    def record(progress, *, state: str):
        persisted.append((progress.cycles, state))
        return original(progress, state=state)

    service._persist_rebuild_progress = record  # type: ignore[method-assign]
    try:
        result = service.run_post_shell_catchup(
            rebuild_id="periodic-checkpoint", source_rows_estimate=2300
        )
        assert result.state == "complete"
        assert persisted == [(10, "running"), (20, "running"), (24, "complete")]
    finally:
        service.close()


def test_deep_rebuild_retries_transient_coordinator_database_lock(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    coordinator = _BusyOnceCoordinator()
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=coordinator, yield_seconds=0
    )
    try:
        result = service.run_post_shell_catchup(
            rebuild_id="busy-once",
            source_rows_estimate=1,
        )
        assert result.state == "complete"
        assert result.committed == 1
        assert coordinator.calls == 3
    finally:
        service.close()


def test_checkpoint_lock_does_not_abort_active_deep_rebuild(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _empty_db(tmp_path)
    coordinator = _BatchCoordinator(1)
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=coordinator, yield_seconds=0
    )
    original_connect = maintenance_module.connect_sqlite_runtime_write
    calls = 0

    def busy_first_checkpoint(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise sqlite3.OperationalError("database is locked")
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(
        "freqinout.core.message_projection_maintenance.connect_sqlite_runtime_write",
        busy_first_checkpoint,
    )
    try:
        result = service.run_post_shell_catchup(
            rebuild_id="checkpoint-busy",
            source_rows_estimate=1,
        )
        assert result.state == "complete"
        assert result.committed == 100
    finally:
        service.close()


def test_rebuild_request_reports_busy_instead_of_operational_error(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=_BatchCoordinator(0), yield_seconds=0
    )
    writer = sqlite3.connect(db_path, timeout=0.1)
    try:
        writer.execute("BEGIN IMMEDIATE")
        result = service.request_deep_rebuild(
            preview=DeepRebuildPreview(source_rows=1, available_sources=1)
        )
        assert result.state == "busy"
        assert result.rebuild_id == ""
    finally:
        writer.rollback()
        writer.close()
        service.close()


def test_interrupted_rebuild_persists_checkpoint_and_resumes(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    cancel = threading.Event()
    first_coordinator = _BatchCoordinator(20, cancel_after=3, event=cancel)
    first = MessageProjectionMaintenanceService(
        db_path, coordinator=first_coordinator, yield_seconds=0
    )
    try:
        requested = first.request_deep_rebuild()
        interrupted = first.run_post_shell_catchup(
            rebuild_id=requested.rebuild_id,
            source_rows_estimate=requested.source_rows_estimate,
            cancel_event=cancel,
        )
        assert interrupted.state == "cancelled"
        assert interrupted.committed == 300
        checkpoint = get_message_projection_checkpoint(
            db_path, DEEP_REBUILD_CHECKPOINT_SOURCE
        )
        assert checkpoint.last_external_key == "3"
        assert checkpoint.last_event_ts == 300.0
        assert ":cancelled:" in checkpoint.content_fingerprint
    finally:
        first.close()

    resumed_coordinator = _BatchCoordinator(0)
    resumed = MessageProjectionMaintenanceService(
        db_path, coordinator=resumed_coordinator, yield_seconds=0
    )
    try:
        result = resumed.start_deep_rebuild().result(timeout=2.0)
        assert result.state == "complete"
        assert result.rebuild_id == requested.rebuild_id
        checkpoint = get_message_projection_checkpoint(
            db_path, DEEP_REBUILD_CHECKPOINT_SOURCE
        )
        assert ":complete:" in checkpoint.content_fingerprint
    finally:
        resumed.close()


def test_post_shell_start_does_not_implicitly_resume_rebuild_checkpoint(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    seed = MessageProjectionMaintenanceService(
        db_path, coordinator=_BatchCoordinator(0), yield_seconds=0
    )
    try:
        request = seed.request_deep_rebuild()
        assert request.state == "requested"
    finally:
        seed.close()

    coordinator = _BatchCoordinator(2)
    resumed = MessageProjectionMaintenanceService(
        db_path, coordinator=coordinator, yield_seconds=0
    )
    try:
        result = resumed.start_post_shell_catchup().result(timeout=2.0)
        assert result.state == "sliced"
        assert result.rebuild_id == ""
        assert result.committed == 100
        assert coordinator.calls == 1
        assert ":requested:" in get_message_projection_checkpoint(
            db_path, DEEP_REBUILD_CHECKPOINT_SOURCE
        ).content_fingerprint
    finally:
        resumed.close()


def test_post_shell_start_never_reads_rebuild_checkpoint(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _empty_db(tmp_path)

    def forbidden_checkpoint_read(*_args, **_kwargs):
        raise AssertionError("ordinary post-shell catch-up must not inspect rebuild state")

    monkeypatch.setattr(
        "freqinout.core.message_projection_maintenance.get_message_projection_checkpoint",
        forbidden_checkpoint_read,
    )
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=_BatchCoordinator(0), yield_seconds=0
    )
    try:
        started = time.perf_counter()
        future = service.start_post_shell_catchup()
        assert time.perf_counter() - started < 0.1
        assert future.result(timeout=2.0).state == "complete"
    finally:
        service.close()


def test_ordinary_post_shell_requests_run_one_cooperative_cycle_each(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    coordinator = _BatchCoordinator(2)
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=coordinator, yield_seconds=0
    )
    try:
        first = service.start_post_shell_catchup().result(timeout=2.0)
        second = service.start_post_shell_catchup().result(timeout=2.0)
        final = service.start_post_shell_catchup().result(timeout=2.0)
        assert first.state == "sliced"
        assert second.state == "sliced"
        assert final.state == "complete"
        assert coordinator.calls == 3
    finally:
        service.close()


def test_close_does_not_wait_for_the_current_bounded_cycle(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    coordinator = _BlockedCoordinator()
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=coordinator, yield_seconds=0
    )
    future = service.start_post_shell_catchup()
    assert coordinator.started.wait(1.0)
    started = time.perf_counter()
    service.close(wait=False)
    assert time.perf_counter() - started < 0.1
    coordinator.release.set()
    assert future.result(timeout=2.0).state == "cancelled"
    assert service.is_stopped() is True


def test_close_tracks_running_control_task_until_it_finishes(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _empty_db(tmp_path)
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=_BatchCoordinator(0), yield_seconds=0
    )
    started = threading.Event()
    release = threading.Event()

    def blocked_preview():
        started.set()
        assert release.wait(2.0)
        return "done"

    monkeypatch.setattr(service, "preview_deep_rebuild", blocked_preview)
    future = service.preview_deep_rebuild_async()
    assert started.wait(1.0)
    service.close(wait=False)
    assert service.is_stopped() is False
    release.set()
    assert future.result(timeout=2.0) == "done"
    assert service.is_stopped() is True


def test_maintenance_diagnostics_are_in_memory_and_bounded(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    service = MessageProjectionMaintenanceService(
        db_path, coordinator=_BatchCoordinator(1), yield_seconds=0
    )
    try:
        result = service.run_post_shell_catchup()
        snapshot = service.diagnostic_snapshot()
        assert snapshot == {
            "state": "complete",
            "cycles": 2,
            "discovered": 100,
            "claimed": 100,
            "committed": 100,
            "deleted": 0,
            "repaired": 0,
            "processed": 100,
            "deferred": 0,
            "max_transaction_ms": 0.0,
            "queue_depth": 0,
            "oldest_dirty_utc": "",
            "rebuild": False,
            "source_rows_estimate": 0,
        }
        assert result.processed == 100
    finally:
        service.close()
    assert service.is_stopped() is True


def test_zero_change_post_shell_startup_has_no_projection_mutation(tmp_path) -> None:
    db_path = _empty_db(tmp_path)
    # Persist the expected unavailable state first, as startup migration would.
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            for source_id, family in (
                ("native:js8_messages", "js8"),
                ("native:spotter_traffic", "spotter"),
                ("native:varac_messages", "varac"),
                ("native:sitrep_events", "sitrep"),
                ("native:commstat_artifacts", "commstat"),
                ("native:commstat_artifact_deletions", "commstat"),
            ):
                upsert_source_state_conn(
                    conn,
                    SourceProjectionState(
                        source_id=source_id,
                        source_family=family,
                        availability_state="unavailable",
                        projector_version=3,
                    ),
                )
    finally:
        conn.close()
    service = MessageProjectionMaintenanceService(db_path, yield_seconds=0)
    try:
        result = service.run_post_shell_catchup()
        assert result.state == "complete"
        assert result.processed == 0
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 0
            assert conn.execute(
                "SELECT generation FROM message_projection_generation WHERE singleton=1"
            ).fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM message_projection_dirty").fetchone()[0] == 0
        finally:
            conn.close()
    finally:
        service.close()


def test_explicit_rebuild_resets_only_derived_state_and_preserves_native_rows(tmp_path) -> None:
    db_path = _native_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        before = conn.execute("SELECT * FROM js8_messages WHERE id=1").fetchone()
    finally:
        conn.close()
    service = MessageProjectionMaintenanceService(db_path, yield_seconds=0)
    try:
        request = service.request_deep_rebuild()
        assert request.state == "requested"
        # The request clears only a derived watermark.  It does not mutate the
        # JS8 evidence or delete existing normalized projection records.
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute("SELECT * FROM js8_messages WHERE id=1").fetchone() == before
            assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 0
        finally:
            conn.close()
        state = get_source_state(db_path, "native:js8_messages")
        assert state is not None and state.high_water_key == ""

        result = service.run_post_shell_catchup(
            rebuild_id=request.rebuild_id,
            source_rows_estimate=request.source_rows_estimate,
        )
        assert result.state == "complete"
        assert result.committed == 1
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute("SELECT * FROM js8_messages WHERE id=1").fetchone() == before
            assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 1
        finally:
            conn.close()
    finally:
        service.close()
