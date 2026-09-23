"""Post-shell maintenance for the derived message projection.

This module intentionally owns no source ingestion and no projection SQL.  It
drives the existing bounded coordinator after the application shell is usable,
and persists enough derived state to safely resume after a shutdown.  Native
source tables are never cleared, rewritten, or repaired here.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
import threading
import time
import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from freqinout.core.message_delete_audit import load_message_delete_audit_rows
from freqinout.core.message_projection_coordinator import (
    MessageProjectionCoordinator,
    ProjectionCycleResult,
    native_projection_source_state_specs,
)
from freqinout.core.message_projection_queue import (
    SourceProjectionState,
    get_source_state_conn,
    queue_diagnostics,
    upsert_source_state_conn,
)
from freqinout.core.message_projection_store import (
    MessageProjectionCheckpoint,
    get_message_projection_checkpoint,
    set_message_projection_checkpoint,
)
from freqinout.core.logger import log
from freqinout.core.perf_metrics import emit_span
from freqinout.core.scheduler_serial_executor import DaemonSerialExecutor
from freqinout.core.sqlite_utils import connect_sqlite_readonly, connect_sqlite_runtime_write


DEEP_REBUILD_CHECKPOINT_SOURCE = "maintenance:message-projection:deep-rebuild"
DEFAULT_CATCHUP_YIELD_SECONDS = 0.01
MAX_CONSECUTIVE_BUSY_RETRIES = 8
BUSY_RETRY_DELAY_SECONDS = 0.1
# Rebuild cycles are deliberately small so cancellation has a short worst-case
# latency.  Publishing every small cycle to Qt, however, can queue thousands of
# cross-thread UI events during a large historical rebuild.  The dialog polls
# the in-memory snapshot directly, so callbacks are only a visible-view hint.
PROGRESS_CALLBACK_MIN_INTERVAL_SECONDS = 0.25
REBUILD_CHECKPOINT_CYCLE_INTERVAL = 10


@dataclass(frozen=True)
class DeepRebuildPreview:
    """Read-only estimate shown before an operator requests a rebuild."""

    source_rows: int = 0
    available_sources: int = 0
    unavailable_sources: int = 0


@dataclass(frozen=True)
class ProjectionCatchupProgress:
    """Aggregate of short coordinator cycles, safe to publish to the UI."""

    state: str = "idle"
    cycles: int = 0
    discovered: int = 0
    claimed: int = 0
    prepared: int = 0
    committed: int = 0
    deleted: int = 0
    repaired: int = 0
    deferred: int = 0
    max_transaction_ms: float = 0.0
    queue_depth: int = 0
    oldest_dirty_utc: str = ""
    rebuild_id: str = ""
    source_rows_estimate: int = 0

    @property
    def processed(self) -> int:
        return self.committed + self.deleted + self.repaired

    @property
    def resumable(self) -> bool:
        return self.state in {"cancelled", "deferred", "failed", "running"}


@dataclass(frozen=True)
class _NoProjectionRepair:
    """Compatibility result for narrow coordinator test doubles."""

    state: str = "committed"
    planned: int = 0
    repaired: int = 0
    transactions: int = 0
    max_transaction_ms: float = 0.0


class MessageProjectionMaintenanceService:
    """Run bounded projection catch-up after the shell is already visible.

    A service uses one coordinator instance, which in turn uses the process
    projection-writer registry.  It never opens its own projection writer or
    performs source I/O.  Cancelling can leave at most the current <=100-item
    coordinator cycle in flight; durable queue rows and watermarks make the
    remainder resumable.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        coordinator: MessageProjectionCoordinator | None = None,
        yield_seconds: float = DEFAULT_CATCHUP_YIELD_SECONDS,
    ) -> None:
        self.db_path = Path(db_path)
        self._coordinator = coordinator or MessageProjectionCoordinator(self.db_path)
        self._owns_coordinator = coordinator is None
        self._yield_seconds = max(0.0, min(0.1, float(yield_seconds)))
        self._executor = DaemonSerialExecutor(
            max_workers=1, thread_name_prefix="fio-message-catchup"
        )
        self._cancel = threading.Event()
        self._lock = threading.RLock()
        self._inflight: Future | None = None
        self._control_futures: set[Future] = set()
        self._closed = False
        self._last_progress = ProjectionCatchupProgress()
        self._progress_callback: Callable[[ProjectionCatchupProgress], None] | None = None
        self._last_progress_callback_monotonic = 0.0

    def set_progress_callback(
        self, callback: Callable[[ProjectionCatchupProgress], None] | None
    ) -> None:
        """Set a non-blocking publisher for already-computed progress state."""

        with self._lock:
            self._progress_callback = callback if callable(callback) else None

    def start_post_shell_catchup(self) -> Future:
        """Schedule one ordinary, bounded catch-up cycle.

        Application-owned pacing decides when another cycle may run.  Keeping
        this call to one cycle prevents a large or actively-growing inbox from
        monopolizing a CPU core and contending continuously with source ingest.
        Explicit deep rebuilds retain their drain-to-completion behavior.
        """

        return self._submit(
            rebuild_id="",
            source_rows_estimate=0,
            max_cycles=1,
        )

    def request_deep_rebuild(
        self, *, preview: DeepRebuildPreview | None = None
    ) -> ProjectionCatchupProgress:
        """Explicitly reset only derived watermarks/queue state for a rebuild.

        The operation is intentionally separate from ``start_post_shell_catchup``
        so opening a tab or ordinary startup can never trigger a historical
        replay.  It does not delete projection evidence either; current native
        rows are reprojected incrementally and missing source evidence remains
        stale/auditable under its existing retention policy.
        """

        with self._lock:
            if self._closed:
                return ProjectionCatchupProgress(state="closed")
            if self._inflight is not None and not self._inflight.done():
                # Do not race an ordinary catch-up's derived watermark with a
                # user-requested reset.  The caller can retry after the short
                # active cycle drains or calls cancel().
                return ProjectionCatchupProgress(state="busy")
        preview = preview or self.preview_deep_rebuild()
        rebuild_id = uuid.uuid4().hex
        conn: sqlite3.Connection | None = None
        try:
            conn = connect_sqlite_runtime_write(
                self.db_path,
                timeout=0.5,
                row_factory=sqlite3.Row,
                busy_timeout_ms=500,
            )
            with conn:
                for _adapter, family, source_id in native_projection_source_state_specs():
                    prior = get_source_state_conn(conn, source_id)
                    upsert_source_state_conn(
                        conn,
                        SourceProjectionState(
                            source_id=source_id,
                            source_family=family,
                            # A cleared derived watermark makes reconciliation
                            # rediscover current authoritative rows in <=100-row
                            # slices.  No source table is modified.
                            high_water_key="",
                            source_generation="",
                            projector_version=0,
                            classifier_version=0,
                            availability_state="unknown",
                            diagnostics={
                                "maintenance": "deep_rebuild",
                                "rebuild_id": rebuild_id,
                                "prior_high_water_key": prior.high_water_key if prior else "",
                            },
                        ),
                    )
                self._write_rebuild_checkpoint_conn(
                    conn,
                    rebuild_id=rebuild_id,
                    state="requested",
                    cycles=0,
                    processed=0,
                    source_rows_estimate=preview.source_rows,
                )
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_busy(exc):
                raise
            log.info(
                "MESSAGE_INDEX_REBUILD|request_deferred|reason=database_busy"
            )
            return ProjectionCatchupProgress(state="busy")
        finally:
            if conn is not None:
                conn.close()
        return ProjectionCatchupProgress(
            state="requested",
            rebuild_id=rebuild_id,
            source_rows_estimate=preview.source_rows,
        )

    def preview_deep_rebuild_async(self) -> Future:
        """Queue the read-only rebuild estimate away from the UI thread."""

        return self._submit_control(self.preview_deep_rebuild)

    def request_deep_rebuild_async(
        self, *, preview: DeepRebuildPreview | None = None
    ) -> Future:
        """Queue the explicit derived-state reset away from the UI thread."""

        return self._submit_control(self.request_deep_rebuild, preview=preview)

    def load_message_maintenance_rows_async(self, *, limit: int = 300) -> Future:
        """Load bounded maintenance tables on the service's read lane."""

        return self._submit_control(self._load_message_maintenance_rows, limit=limit)

    def start_deep_rebuild(
        self, *, rebuild_id: str = "", source_rows_estimate: int = 0
    ) -> Future:
        """Schedule an already explicit rebuild or resume the persisted one."""

        requested_id = str(rebuild_id or "").strip()
        if requested_id:
            return self._submit(
                rebuild_id=requested_id,
                source_rows_estimate=max(0, int(source_rows_estimate or 0)),
            )
        checkpoint = get_message_projection_checkpoint(
            self.db_path, DEEP_REBUILD_CHECKPOINT_SOURCE
        )
        active_id, estimate, checkpoint_state = _checkpoint_values(checkpoint)
        if checkpoint_state == "complete":
            active_id = ""
        requested_id = str(active_id).strip()
        if not requested_id:
            future: Future = Future()
            future.set_result(ProjectionCatchupProgress(state="not_requested"))
            return future
        return self._submit(rebuild_id=requested_id, source_rows_estimate=estimate)

    def run_post_shell_catchup(
        self,
        *,
        rebuild_id: str = "",
        source_rows_estimate: int = 0,
        max_cycles: int | None = None,
        cancel_event: threading.Event | None = None,
    ) -> ProjectionCatchupProgress:
        """Drain short cycles in a background worker until idle or cancelled.

        ``max_cycles`` exists for deterministic tests and operational slicing;
        production callers normally leave it unset.  Each cycle remains capped
        by the coordinator/writer at 100 identities, then yields before another
        source read or write attempt.
        """

        event = cancel_event or self._cancel
        limit = None if max_cycles is None else max(1, int(max_cycles))
        progress = ProjectionCatchupProgress(
            state="running",
            rebuild_id=str(rebuild_id or ""),
            source_rows_estimate=max(0, int(source_rows_estimate or 0)),
        )
        self._record_progress(progress)
        consecutive_busy = 0
        try:
            while not event.is_set() and (limit is None or progress.cycles < limit):
                try:
                    result = self._coordinator.run_once(
                        reconcile=True,
                        cancel_event=event,
                    )
                except sqlite3.OperationalError as exc:
                    if not _is_sqlite_busy(exc):
                        raise
                    consecutive_busy += 1
                    log.warning(
                        "MESSAGE_INDEX_REBUILD|database_busy|cycle=%s|retry=%s|limit=%s",
                        progress.cycles,
                        consecutive_busy,
                        MAX_CONSECUTIVE_BUSY_RETRIES,
                    )
                    if consecutive_busy >= MAX_CONSECUTIVE_BUSY_RETRIES:
                        progress = _with_state(progress, "deferred")
                        break
                    if event.wait(BUSY_RETRY_DELAY_SECONDS * consecutive_busy):
                        break
                    continue
                consecutive_busy = 0
                progress = _accumulate(progress, result)
                self._record_progress(progress)
                if progress.rebuild_id and _should_checkpoint_rebuild(progress):
                    self._persist_rebuild_progress(progress, state="running")
                if result.deferred:
                    progress = _with_state(progress, "deferred")
                    break
                if _cycle_is_idle(result):
                    if progress.rebuild_id:
                        repair_method = getattr(
                            self._coordinator, "repair_legacy_duplicates", None
                        )
                        repair = (
                            repair_method(cancel_event=event)
                            if callable(repair_method)
                            else _NoProjectionRepair()
                        )
                        progress = _with_repair(
                            progress,
                            repaired=int(repair.repaired or 0),
                            max_transaction_ms=float(repair.max_transaction_ms or 0.0),
                        )
                        self._record_progress(progress)
                        log.info(
                            "MESSAGE_INDEX_REBUILD|canonical_repair|state=%s|planned=%s|repaired=%s|transactions=%s",
                            repair.state,
                            repair.planned,
                            repair.repaired,
                            repair.transactions,
                        )
                        if repair.state != "committed":
                            progress = _with_state(progress, repair.state)
                            break
                    progress = _with_state(progress, "complete")
                    break
                # Let Qt/the interpreter and unrelated service workers run.
                if self._yield_seconds and event.wait(self._yield_seconds):
                    break
            else:
                if progress.cycles >= (limit or 0):
                    progress = _with_state(progress, "sliced")
            if event.is_set():
                progress = _with_state(progress, "cancelled")
            elif progress.state == "running":
                progress = _with_state(progress, "sliced" if limit is not None else "complete")
        except Exception:
            progress = _with_state(progress, "failed")
            if progress.rebuild_id:
                self._persist_rebuild_progress(progress, state="failed")
            log.exception(
                "MESSAGE_INDEX_REBUILD|failed|rebuild_id=%s|cycle=%s",
                progress.rebuild_id,
                progress.cycles,
            )
            raise
        finally:
            if progress.rebuild_id:
                final_state = "complete" if progress.state == "complete" else progress.state
                self._persist_rebuild_progress(progress, state=final_state)
            try:
                queue = queue_diagnostics(self.db_path)
                progress = _with_queue_state(
                    progress,
                    depth=int(queue.get("depth", 0) or 0),
                    oldest_utc=str(queue.get("oldest_utc", "") or ""),
                )
                emit_span(
                    "messages.projection_lag",
                    _projection_lag_ms(progress.oldest_dirty_utc),
                    meta={
                        "depth": progress.queue_depth,
                        "oldest_utc": progress.oldest_dirty_utc,
                        "state": progress.state,
                    },
                    level="debug",
                )
            except Exception:
                pass
            # A terminal state must always be published even if a large rebuild
            # finished within the normal callback coalescing interval.
            self._record_progress(progress, force_callback=True)
        return progress

    def preview_deep_rebuild(self) -> DeepRebuildPreview:
        """Count current native records using one read-only, bounded query set."""

        conn = connect_sqlite_readonly(self.db_path, row_factory=sqlite3.Row)
        source_rows = available = unavailable = 0
        try:
            for _adapter, _family, source_id in native_projection_source_state_specs():
                table = source_id.removeprefix("native:")
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()
                if exists is None:
                    unavailable += 1
                    continue
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                source_rows += int(row["count"] or 0)
                available += 1
        finally:
            conn.close()
        return DeepRebuildPreview(
            source_rows=source_rows,
            available_sources=available,
            unavailable_sources=unavailable,
        )

    def _load_message_maintenance_rows(self, *, limit: int) -> dict[str, object]:
        capped = max(1, min(int(limit or 300), 1000))
        audit_rows = load_message_delete_audit_rows(self.db_path, limit=capped)
        hidden_rows: list[dict[str, object]] = []
        conn = connect_sqlite_readonly(
            self.db_path, timeout=0.5, row_factory=sqlite3.Row
        )
        try:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='commstat_artifact_deletions'"
            ).fetchone()
            if exists is not None:
                rows = conn.execute(
                    """
                    SELECT deleted_ts, artifact_kind, from_call, target, title,
                           event_ts, reason, artifact_key
                      FROM commstat_artifact_deletions
                     ORDER BY deleted_ts DESC
                     LIMIT ?
                    """,
                    (capped,),
                ).fetchall()
                hidden_rows = [dict(row) for row in rows]
        finally:
            conn.close()
        return {"audit": audit_rows, "hidden": hidden_rows}

    def cancel(self) -> None:
        """Request cooperative stop; source watermarks/dirty work remain durable."""

        self._cancel.set()

    def diagnostic_snapshot(self) -> dict[str, object]:
        """Return bounded in-memory state; safe for watchdog publication."""

        with self._lock:
            progress = self._last_progress
            running = bool(self._inflight is not None and not self._inflight.done())
            closed = bool(self._closed)
        return {
            "state": "closed" if closed else ("running" if running else progress.state),
            "cycles": int(progress.cycles),
            "discovered": int(progress.discovered),
            "claimed": int(progress.claimed),
            "committed": int(progress.committed),
            "deleted": int(progress.deleted),
            "repaired": int(progress.repaired),
            "processed": int(progress.processed),
            "deferred": int(progress.deferred),
            "max_transaction_ms": round(float(progress.max_transaction_ms), 3),
            "queue_depth": int(progress.queue_depth),
            "oldest_dirty_utc": str(progress.oldest_dirty_utc or ""),
            "rebuild": bool(progress.rebuild_id),
            "source_rows_estimate": int(progress.source_rows_estimate),
        }

    def is_stopped(self) -> bool:
        with self._lock:
            return bool(
                self._closed
                and (self._inflight is None or self._inflight.done())
                and all(future.done() for future in self._control_futures)
            )

    def close(self, *, wait: bool = False) -> None:
        """Bounded lifecycle shutdown without synchronously draining history."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._cancel.set()
            inflight = self._inflight
            progress = self._last_progress
            control_futures = tuple(self._control_futures)
        emit_span(
            "messages.shutdown",
            0.0,
            meta={
                "queued_or_surviving": int(progress.queue_depth),
                "committed": int(progress.committed),
                "deleted": int(progress.deleted),
                "cancel_requested": bool(inflight is not None and not inflight.done()),
                "control_tasks": sum(1 for future in control_futures if not future.done()),
                "state": progress.state,
            },
            level="debug",
        )
        self._executor.shutdown(wait=wait, cancel_futures=True)
        if self._owns_coordinator:
            if wait or inflight is None or inflight.done():
                self._coordinator.close(wait=wait)
            else:
                # Do not release a coordinator lease while its final bounded
                # cycle is still committing.  Cleanup stays on the daemon
                # maintenance lane and performs no Qt work.
                inflight.add_done_callback(
                    lambda _future: self._coordinator.close(wait=False)
                )

    def _submit(
        self,
        *,
        rebuild_id: str,
        source_rows_estimate: int,
        max_cycles: int | None = None,
    ) -> Future:
        with self._lock:
            if self._closed:
                future: Future = Future()
                future.set_result(ProjectionCatchupProgress(state="closed"))
                return future
            if self._inflight is not None and not self._inflight.done():
                return self._inflight
            self._cancel.clear()
            self._inflight = self._executor.submit(
                self.run_post_shell_catchup,
                rebuild_id=rebuild_id,
                source_rows_estimate=source_rows_estimate,
                max_cycles=max_cycles,
                cancel_event=self._cancel,
            )
            return self._inflight

    def _submit_control(self, function: Callable[..., object], **kwargs: object) -> Future:
        with self._lock:
            if self._closed:
                future: Future = Future()
                future.set_exception(RuntimeError("message projection maintenance is closed"))
                return future
            self._cancel.clear()
            future = self._executor.submit(function, **kwargs)
            self._control_futures.add(future)

        def _finished(done_future: Future) -> None:
            with self._lock:
                self._control_futures.discard(done_future)

        future.add_done_callback(_finished)
        return future

    def _record_progress(
        self,
        progress: ProjectionCatchupProgress,
        *,
        force_callback: bool = False,
    ) -> None:
        with self._lock:
            self._last_progress = progress
            callback = self._progress_callback
            now = time.monotonic()
            publish = bool(
                force_callback
                or now - self._last_progress_callback_monotonic
                >= PROGRESS_CALLBACK_MIN_INTERVAL_SECONDS
            )
            if publish:
                self._last_progress_callback_monotonic = now
        if callback is not None and publish:
            try:
                callback(progress)
            except Exception:
                pass

    def _persist_rebuild_progress(
        self, progress: ProjectionCatchupProgress, *, state: str
    ) -> bool:
        """Persist compact progress without letting lock contention stop work.

        Source watermarks and queue rows are already durable.  This checkpoint
        is resume/display metadata, so a transient writer lock must defer only
        this metadata update; the next bounded cycle or final write retries it.
        """

        conn: sqlite3.Connection | None = None
        try:
            conn = connect_sqlite_runtime_write(
                self.db_path,
                timeout=0.5,
                busy_timeout_ms=500,
            )
            with conn:
                self._write_rebuild_checkpoint_conn(
                    conn,
                    rebuild_id=progress.rebuild_id,
                    state=state,
                    cycles=progress.cycles,
                    processed=progress.processed,
                    source_rows_estimate=progress.source_rows_estimate,
                )
            return True
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_busy(exc):
                raise
            log.warning(
                "MESSAGE_INDEX_REBUILD|checkpoint_deferred|state=%s|cycle=%s|reason=database_busy",
                state,
                progress.cycles,
            )
            return False
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def _write_rebuild_checkpoint_conn(
        conn: sqlite3.Connection,
        *,
        rebuild_id: str,
        state: str,
        cycles: int,
        processed: int,
        source_rows_estimate: int,
    ) -> None:
        # Compact metadata only: no callsigns, message content, paths, or keys.
        fingerprint = (
            f"mip5:deep-rebuild:{str(rebuild_id or '')}:{str(state or '')}:"
            f"rows={max(0, int(source_rows_estimate or 0))}"
        )
        set_message_projection_checkpoint(
            conn,
            MessageProjectionCheckpoint(
                source_id=DEEP_REBUILD_CHECKPOINT_SOURCE,
                last_external_key=str(max(0, int(cycles or 0))),
                last_event_ts=float(max(0, int(processed or 0))),
                content_fingerprint=fingerprint,
            ),
        )


def _cycle_is_idle(result: ProjectionCycleResult) -> bool:
    return not any(
        int(value or 0)
        for value in (
            result.discovered,
            result.claimed,
            result.prepared,
            result.deleted,
            result.committed,
            result.deferred,
        )
    )


def _should_checkpoint_rebuild(progress: ProjectionCatchupProgress) -> bool:
    """Persist resumability metadata periodically, with terminal state flushed.

    Native source watermarks and dirty queue rows are committed by each bounded
    coordinator cycle.  The maintenance checkpoint is operator-facing resume
    metadata, so writing it for every 25-row cycle only adds SQLite contention
    without improving recovery precision.  ``finally`` always flushes the
    exact terminal state.
    """

    return int(progress.cycles or 0) % REBUILD_CHECKPOINT_CYCLE_INTERVAL == 0


def _is_sqlite_busy(exc: sqlite3.OperationalError) -> bool:
    text = str(exc or "").strip().casefold()
    return "locked" in text or "busy" in text


def _accumulate(
    progress: ProjectionCatchupProgress, result: ProjectionCycleResult
) -> ProjectionCatchupProgress:
    return ProjectionCatchupProgress(
        state="running",
        cycles=progress.cycles + 1,
        discovered=progress.discovered + int(result.discovered or 0),
        claimed=progress.claimed + int(result.claimed or 0),
        prepared=progress.prepared + int(result.prepared or 0),
        committed=progress.committed + int(result.committed or 0),
        deleted=progress.deleted + int(result.deleted or 0),
        repaired=progress.repaired,
        deferred=progress.deferred + int(result.deferred or 0),
        max_transaction_ms=max(
            float(progress.max_transaction_ms or 0.0),
            float(result.max_transaction_ms or 0.0),
        ),
        queue_depth=progress.queue_depth,
        oldest_dirty_utc=progress.oldest_dirty_utc,
        rebuild_id=progress.rebuild_id,
        source_rows_estimate=progress.source_rows_estimate,
    )


def _with_state(progress: ProjectionCatchupProgress, state: str) -> ProjectionCatchupProgress:
    return ProjectionCatchupProgress(
        state=str(state or "idle"),
        cycles=progress.cycles,
        discovered=progress.discovered,
        claimed=progress.claimed,
        prepared=progress.prepared,
        committed=progress.committed,
        deleted=progress.deleted,
        repaired=progress.repaired,
        deferred=progress.deferred,
        max_transaction_ms=progress.max_transaction_ms,
        queue_depth=progress.queue_depth,
        oldest_dirty_utc=progress.oldest_dirty_utc,
        rebuild_id=progress.rebuild_id,
        source_rows_estimate=progress.source_rows_estimate,
    )


def _with_queue_state(
    progress: ProjectionCatchupProgress, *, depth: int, oldest_utc: str
) -> ProjectionCatchupProgress:
    return ProjectionCatchupProgress(
        state=progress.state,
        cycles=progress.cycles,
        discovered=progress.discovered,
        claimed=progress.claimed,
        prepared=progress.prepared,
        committed=progress.committed,
        deleted=progress.deleted,
        repaired=progress.repaired,
        deferred=progress.deferred,
        max_transaction_ms=progress.max_transaction_ms,
        queue_depth=max(0, int(depth or 0)),
        oldest_dirty_utc=str(oldest_utc or ""),
        rebuild_id=progress.rebuild_id,
        source_rows_estimate=progress.source_rows_estimate,
    )


def _with_repair(
    progress: ProjectionCatchupProgress,
    *,
    repaired: int,
    max_transaction_ms: float,
) -> ProjectionCatchupProgress:
    return ProjectionCatchupProgress(
        state=progress.state,
        cycles=progress.cycles,
        discovered=progress.discovered,
        claimed=progress.claimed,
        prepared=progress.prepared,
        committed=progress.committed,
        deleted=progress.deleted,
        repaired=progress.repaired + max(0, int(repaired or 0)),
        deferred=progress.deferred,
        max_transaction_ms=max(
            float(progress.max_transaction_ms or 0.0),
            float(max_transaction_ms or 0.0),
        ),
        queue_depth=progress.queue_depth,
        oldest_dirty_utc=progress.oldest_dirty_utc,
        rebuild_id=progress.rebuild_id,
        source_rows_estimate=progress.source_rows_estimate,
    )


def _projection_lag_ms(oldest_utc: str) -> float:
    text = str(oldest_utc or "").strip()
    if not text:
        return 0.0
    try:
        stamp = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=dt.timezone.utc)
        return max(
            0.0,
            (dt.datetime.now(dt.timezone.utc) - stamp.astimezone(dt.timezone.utc)).total_seconds()
            * 1000.0,
        )
    except Exception:
        return 0.0


def _checkpoint_values(checkpoint: MessageProjectionCheckpoint) -> tuple[str, int, str]:
    fingerprint = str(checkpoint.content_fingerprint or "")
    if not fingerprint.startswith("mip5:deep-rebuild:"):
        return "", 0, ""
    parts = fingerprint.split(":")
    # mip5:deep-rebuild:<id>:<state>:rows=<count>
    rebuild_id = parts[2].strip() if len(parts) >= 3 else ""
    state = parts[3].strip() if len(parts) >= 4 else ""
    estimate = 0
    if parts and parts[-1].startswith("rows="):
        try:
            estimate = max(0, int(parts[-1].split("=", 1)[1]))
        except ValueError:
            estimate = 0
    return rebuild_id, estimate, state
