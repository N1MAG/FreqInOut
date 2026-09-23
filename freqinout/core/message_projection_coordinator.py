"""Incremental native-message projection coordinator.

The coordinator detects bounded native changes, leases durable identities,
prepares immutable bundles outside a write transaction, and hands all
projection mutations to the single database writer lane.
"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from concurrent.futures import Future
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping, Sequence

from freqinout.core.js8_message_policy import JS8_MESSAGE_POLICY_VERSION
from freqinout.core.message_projection_queue import (
    DirtyProjectionItem,
    SourceProjectionState,
    claim_ready,
    complete_dirty_conn,
    enqueue_dirty_conn,
    get_source_state_conn,
    queue_diagnostics,
    release_owner_leases_conn,
    retry_dirty_conn,
    upsert_source_state_conn,
)
from freqinout.core.message_projection_writer import (
    ProjectionBundle,
    ProjectionDeleteRequest,
    ProjectionWriteResult,
    ProjectionRepairResult,
    get_projection_writer,
)
from freqinout.core.perf_metrics import emit_span
from freqinout.core.message_source_projectors import (
    PROJECTOR_VERSION,
    native_projector_version,
    prepare_native_message_bundles,
)
from freqinout.core.scheduler_serial_executor import DaemonSerialExecutor
from freqinout.core.fio_spotter_store import (
    load_spotter_watch_matcher,
    record_spotter_watch_candidate_matches,
)
from freqinout.core.fio_spotter_watch_engine import SpotterWatchMatcher
from freqinout.core.sqlite_utils import (
    connect_sqlite_readonly,
    connect_sqlite_runtime_write,
)


_SOURCE_SPECS: Mapping[str, Mapping[str, str]] = {
    "js8": {
        "family": "js8",
        "table": "js8_messages",
        "kind": "js8_message",
        "source_id": (
            "'js8:' || CASE "
            "WHEN COALESCE(source_key,'') != '' THEN "
            "CASE WHEN LOWER(source_key || ' ' || COALESCE(source_path,'')) LIKE '%directed%' THEN 'directed_txt' "
            "WHEN LOWER(source_key || ' ' || COALESCE(source_path,'')) LIKE '%inbox%' "
            "OR LOWER(source_key || ' ' || COALESCE(source_path,'')) LIKE '%.db3%' THEN 'inbox_db' "
            "WHEN LOWER(source_key) LIKE '%api%' OR COALESCE(source_path,'') = '' THEN 'api' ELSE 'file' END "
            "|| ':' || source_key "
            "WHEN COALESCE(source_path,'') != '' THEN "
            "CASE WHEN LOWER(source_path) LIKE '%directed%' THEN 'directed_txt' "
            "WHEN LOWER(source_path) LIKE '%inbox%' OR LOWER(source_path) LIKE '%.db3%' THEN 'inbox_db' "
            "ELSE 'file' END || ':legacy:' || source_path "
            "ELSE 'api:' || COALESCE(NULLIF(js8_instance_id,''), 'legacy') END"
        ),
        "watermark": "id",
        "key": "CAST(COALESCE(source_id,id) AS TEXT)",
        "version": "printf('%s:%s:%s:%s',COALESCE(id,0),COALESCE(read_ts,0),COALESCE(state,''),COALESCE(flag_state,0))",
    },
    "spotter": {
        "family": "spotter",
        "table": "spotter_traffic",
        "kind": "spotter_message",
        "source_id": "'spotter:' || COALESCE(NULLIF(js8_instance_id,''), NULLIF(CAST(source_radio_id AS TEXT),''), 'legacy')",
        "watermark": "id",
        "key": "CAST(id AS TEXT)",
        "version": "printf('%s:%s:%s:%s',COALESCE(id,0),COALESCE(read_ts,0),COALESCE(state,''),COALESCE(flag_state,0))",
    },
    "varac": {
        "family": "varac",
        "table": "varac_messages",
        "kind": "varac_message",
        "source_id": "'varac:' || COALESCE(NULLIF(ingest_source_key,''),'legacy') || ':' || COALESCE(NULLIF(source,''),'varac')",
        "watermark": "rowid",
        "key": "COALESCE(NULLIF(guid,''),NULLIF(vmail_guid,''),CAST(id AS TEXT))",
        "version": "printf('%s:%s:%s:%s',COALESCE(id,0),COALESCE(ts,0),COALESCE(read_status,0),COALESCE(is_deleted,0))",
    },
    "sitrep": {
        "family": "sitrep",
        "table": "sitrep_events",
        "kind": "sitrep_event",
        "source_id": "'sitrep:fused'",
        "watermark": "id",
        "key": "COALESCE(NULLIF(report_key,''),CAST(id AS TEXT))",
        "version": "printf('%s:%s:%s',COALESCE(id,0),COALESCE(event_ts,0),COALESCE(updated_ts,0))",
    },
    "commstat": {
        "family": "commstat",
        "table": "commstat_artifacts",
        "kind": "commstat_artifact",
        "source_id": "'commstat:artifacts'",
        "watermark": "id",
        "key": "COALESCE(NULLIF(artifact_key,''),CAST(id AS TEXT))",
        "version": "printf('%s:%s:%s',COALESCE(id,0),COALESCE(event_ts,0),COALESCE(updated_ts,0))",
    },
    "commstat_deletions": {
        "family": "commstat",
        "table": "commstat_artifact_deletions",
        "kind": "commstat_artifact",
        "source_id": "'commstat:artifacts'",
        "watermark": "rowid",
        "key": "CAST(artifact_key AS TEXT)",
        "version": "printf('%s:%s',COALESCE(artifact_key,''),COALESCE(deleted_ts,0))",
        "operation": "delete",
    },
}


# A coordinator cycle is deliberately one UI-friendly unit of historical
# catch-up.  Discovery must never fill the durable queue faster than this same
# cycle can drain it.  Preparation uses smaller chunks so parsing/classifying a
# large source row set yields the interpreter before the writer starts.
# Ordinary maintenance runs on the same SQLite database as live source ingest.
# Keep each cooperative cycle small enough to release CPU and database access
# promptly; the application schedules additional cycles with an idle gap.
MAX_CYCLE_ITEMS = 25
PREPARE_ITEMS_PER_SLICE = 10


def native_projection_source_state_specs() -> tuple[tuple[str, str, str], ...]:
    """Return stable derived-state identities for native reconcilers.

    The startup maintenance service deliberately needs only the adapter name,
    source family, and *derived* watermark identity.  Keeping that mapping
    here avoids a second, eventually divergent list of authoritative tables
    in the rebuild path.
    """

    return tuple(
        (adapter, str(spec["family"]), f"native:{spec['table']}")
        for adapter, spec in _SOURCE_SPECS.items()
    )


@dataclass(frozen=True)
class ProjectionCycleResult:
    discovered: int = 0
    claimed: int = 0
    prepared: int = 0
    deleted: int = 0
    committed: int = 0
    deferred: int = 0
    max_transaction_ms: float = 0.0
    state: str = "idle"


def reconcile_native_source_changes(
    db_path: str | Path,
    *,
    sources: Sequence[str] = tuple(_SOURCE_SPECS),
    limit_per_source: int = 100,
    max_items: int | None = None,
) -> dict[str, int]:
    """Discover only rows beyond each durable keyset watermark.

    Additive database triggers capture subsequent updates/deletes to older
    identities. Therefore an unchanged reconciliation performs SELECTs only and
    emits no projection or queue writes.
    """

    cap = max(1, min(1000, int(limit_per_source or 100)))
    remaining = None if max_items is None else max(0, int(max_items))
    conn = connect_sqlite_runtime_write(
        db_path, timeout=0.25, row_factory=sqlite3.Row, busy_timeout_ms=250
    )
    results: dict[str, int] = {}
    try:
        for adapter in (str(value).strip().lower() for value in sources):
            # ``max_items`` is the coordinator's queue budget.  Do not even
            # advance an additional source cursor once the cycle has filled
            # that budget: a later source remains durable, undiscovered work
            # for the next short cycle rather than creating a hidden backlog.
            if remaining is not None and remaining <= 0:
                results[adapter] = 0
                continue
            spec = _SOURCE_SPECS.get(adapter)
            if spec is None:
                continue
            family = spec["family"]
            # ``PROJECTOR_VERSION`` remains the compatibility override used by
            # repair tooling/tests; family versions may advance independently.
            target_projector = max(PROJECTOR_VERSION, native_projector_version(family))
            table = spec["table"]
            source_id = f"native:{table}"
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            state = get_source_state_conn(conn, source_id)
            if exists is None:
                results[adapter] = 0
                if state is None or state.availability_state != "unavailable":
                    with conn:
                        upsert_source_state_conn(
                            conn,
                            SourceProjectionState(
                                source_id=source_id,
                                source_family=family,
                                availability_state="unavailable",
                                projector_version=target_projector,
                                classifier_version=JS8_MESSAGE_POLICY_VERSION if family == "js8" else 0,
                            ),
                        )
                continue
            target_classifier = JS8_MESSAGE_POLICY_VERSION if family == "js8" else 0
            version_changed = bool(
                state
                and (
                    state.projector_version != target_projector
                    or state.classifier_version != target_classifier
                )
            )
            high_water = 0 if state is None or version_changed else _int_key(state.high_water_key)
            watermark_expr = spec["watermark"]
            source_cap = cap if remaining is None else min(cap, remaining)
            rows = conn.execute(
                f"""
                SELECT {watermark_expr} AS discovery_key,
                       {spec['source_id']} AS projection_source_id,
                       {spec['key']} AS external_key,
                       {spec['version']} AS source_version
                  FROM {table}
                 WHERE {watermark_expr}>?
                 ORDER BY {watermark_expr}
                 LIMIT ?
                """,
                (high_water, source_cap),
            ).fetchall()
            results[adapter] = len(rows)
            if remaining is not None:
                remaining -= len(rows)
            if not rows:
                if state is None or version_changed or state.availability_state != "available":
                    with conn:
                        upsert_source_state_conn(
                            conn,
                            SourceProjectionState(
                                source_id=source_id,
                                source_family=family,
                                high_water_key=str(high_water),
                                projector_version=target_projector,
                                classifier_version=target_classifier,
                                availability_state="available",
                            ),
                        )
                continue
            with conn:
                for row in rows:
                    enqueue_dirty_conn(
                        conn,
                        DirtyProjectionItem(
                            source_id=str(row["projection_source_id"] or source_id),
                            source_family=family,
                            external_kind=spec["kind"],
                            external_key=str(row["external_key"] or row["id"]),
                            source_version=str(row["source_version"] or row["discovery_key"]),
                            operation=spec.get("operation", "upsert"),
                            projector_version=target_projector,
                        ),
                    )
                last_id = int(rows[-1]["discovery_key"] or high_water)
                upsert_source_state_conn(
                    conn,
                    SourceProjectionState(
                        source_id=source_id,
                        source_family=family,
                        high_water_key=str(last_id),
                        source_generation=str(last_id),
                        projector_version=target_projector,
                        classifier_version=target_classifier,
                        availability_state="available",
                        diagnostics={"last_batch": len(rows)},
                    ),
                )
        return results
    finally:
        conn.close()


class MessageProjectionCoordinator:
    """One background preparation lane feeding the registered SQLite writer."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.owner = f"projection-{uuid.uuid4().hex}"
        self._writer = get_projection_writer(self.db_path)
        self._executor = DaemonSerialExecutor(
            max_workers=1, thread_name_prefix="fio-message-prepare"
        )
        self._lock = threading.Lock()
        self._inflight: Future | None = None
        self._closed = False
        self._cancel = threading.Event()
        self._watch_matcher = SpotterWatchMatcher(())
        self._watch_reload_due = 0.0

    def submit_once(self, *, reconcile: bool = True) -> Future:
        """Coalesce callers onto one non-UI projection cycle."""

        with self._lock:
            if self._closed:
                future: Future = Future()
                future.set_result(ProjectionCycleResult(state="closed"))
                return future
            if self._inflight is not None and not self._inflight.done():
                return self._inflight
            self._cancel.clear()
            self._inflight = self._executor.submit(self.run_once, reconcile=reconcile)
            return self._inflight

    def run_once(
        self,
        *,
        reconcile: bool = True,
        cancel_event: threading.Event | None = None,
    ) -> ProjectionCycleResult:
        """Drain one bounded durable-work cycle without blocking the UI.

        Existing dirty rows always take precedence over fresh native-table
        discovery.  This keeps historical scans from accumulating a larger
        queue than the same cycle can process and leaves all cursors durable.
        """

        event = cancel_event or self._cancel
        if event.is_set():
            return ProjectionCycleResult(state="cancelled")
        discovered = 0
        # Queue-first backpressure: when work is already durable, consume it
        # before considering any additional historical discovery.
        queued_before = int(queue_diagnostics(self.db_path).get("depth", 0) or 0)
        items = claim_ready(
            self.db_path,
            owner=self.owner,
            limit=MAX_CYCLE_ITEMS,
            lease_seconds=30.0,
        )
        if not items and not queued_before and reconcile and not event.is_set():
            reconcile_started = time.perf_counter()
            source_discoveries = reconcile_native_source_changes(
                self.db_path,
                limit_per_source=MAX_CYCLE_ITEMS,
                max_items=MAX_CYCLE_ITEMS,
            )
            discovered = sum(source_discoveries.values())
            reconcile_ms = (time.perf_counter() - reconcile_started) * 1000.0
            if discovered or reconcile_ms >= 25.0:
                emit_span(
                    "messages.reconcile_source",
                    reconcile_ms,
                    meta={"discovered": discovered, "sources": source_discoveries},
                    level="warning" if reconcile_ms >= 100.0 else "debug",
                )
            if discovered:
                emit_span(
                    "messages.dirty_detect",
                    reconcile_ms,
                    meta={"new_or_coalesced": discovered, "sources": source_discoveries},
                    level="debug",
                )
            if event.is_set():
                return ProjectionCycleResult(discovered=discovered, state="cancelled")
            items = claim_ready(
                self.db_path,
                owner=self.owner,
                limit=MAX_CYCLE_ITEMS,
                lease_seconds=30.0,
            )
        if not items:
            return ProjectionCycleResult(
                discovered=discovered,
                deferred=1 if queued_before and not event.is_set() else 0,
                state=(
                    "cancelled"
                    if event.is_set()
                    else "deferred"
                    if queued_before
                    else "idle"
                ),
            )
        if event.is_set():
            self._release_claims(items)
            return ProjectionCycleResult(
                discovered=discovered,
                claimed=len(items),
                deferred=len(items),
                state="cancelled",
            )
        upserts = tuple(item for item in items if item.operation != "delete")
        deletes = tuple(item for item in items if item.operation == "delete")
        bundles: tuple[ProjectionBundle, ...] = ()
        missing: tuple[object, ...] = ()
        if upserts:
            prepare_started = time.perf_counter()
            prepared: list[ProjectionBundle] = []
            absent: list[object] = []
            for start in range(0, len(upserts), PREPARE_ITEMS_PER_SLICE):
                if event.is_set():
                    self._release_claims(items)
                    return ProjectionCycleResult(
                        discovered=discovered,
                        claimed=len(items),
                        prepared=len(prepared),
                        deferred=len(items),
                        state="cancelled",
                    )
                conn = connect_sqlite_readonly(self.db_path, row_factory=sqlite3.Row)
                try:
                    chunk_bundles, chunk_missing = prepare_native_message_bundles(
                        conn, upserts[start : start + PREPARE_ITEMS_PER_SLICE]
                    )
                finally:
                    conn.close()
                prepared.extend(chunk_bundles)
                absent.extend(chunk_missing)
                # Keep historical parsing cooperative even when one source
                # family has a long sequence of heavyweight payloads.
                if start + PREPARE_ITEMS_PER_SLICE < len(upserts):
                    time.sleep(0)
            bundles, missing = tuple(prepared), tuple(absent)
            prepare_ms = (time.perf_counter() - prepare_started) * 1000.0
            emit_span(
                "messages.prepare_batch",
                prepare_ms,
                meta={
                    "requested": len(upserts),
                    "prepared": len(bundles),
                    "unchanged_or_missing": len(missing),
                },
                level="warning" if prepare_ms >= 100.0 else "debug",
            )
        if event.is_set():
            self._release_claims(items)
            return ProjectionCycleResult(
                discovered=discovered,
                claimed=len(items),
                prepared=len(bundles),
                deferred=len(items),
                state="cancelled",
            )
        by_identity = {
            (item.source_id, item.external_kind, item.external_key): item for item in upserts
        }
        owned_bundles: list[ProjectionBundle] = []
        for bundle in bundles:
            ref = bundle.refs[0] if bundle.refs else None
            key = (
                ref.source_id if ref else "",
                ref.external_kind if ref else "",
                ref.external_key if ref else "",
            )
            item = by_identity.get(key)
            if item is None:
                continue
            owned_bundles.append(
                replace(bundle, dirty_keys=(item.stable_key,), dirty_owner=self.owner)
            )
        missing_items = tuple(item for item in missing if isinstance(item, DirtyProjectionItem))
        stale_aliases = tuple(
            item for item in missing_items if self._obsolete_source_identity(item)
        )
        delete_items = deletes + tuple(item for item in missing_items if item not in stale_aliases)
        committed = deferred = deleted = 0
        max_transaction_ms = 0.0
        states: list[str] = []
        if stale_aliases:
            completed_stale = self._complete_stale_claims(stale_aliases)
            committed += completed_stale
            if completed_stale != len(stale_aliases):
                deferred += len(stale_aliases) - completed_stale
                self._retry(items=stale_aliases, code="stale_identity_cleanup")
            states.append("committed" if completed_stale == len(stale_aliases) else "deferred")
        if owned_bundles:
            result: ProjectionWriteResult = self._writer.submit(
                owned_bundles, cancel_event=event
            ).result()
            committed += result.committed_bundles
            deferred += result.deferred_bundles
            max_transaction_ms = max(max_transaction_ms, float(result.max_transaction_ms or 0.0))
            states.append(result.state)
            if result.committed_message_ids:
                self._record_watch_matches(
                    owned_bundles, committed_message_ids=result.committed_message_ids
                )
            if not result.completed:
                self._retry(
                    items=[
                        by_identity[
                            (
                                b.refs[0].source_id,
                                b.refs[0].external_kind,
                                b.refs[0].external_key,
                            )
                        ]
                        for b in owned_bundles
                    ],
                    code=result.state,
                )
        if delete_items:
            delete_result: ProjectionWriteResult = self._writer.submit_deletions(
                (
                    ProjectionDeleteRequest(
                        source_id=item.source_id,
                        external_kind=item.external_kind,
                        external_key=item.external_key,
                        dirty_key=item.stable_key,
                        dirty_owner=self.owner,
                    )
                    for item in delete_items
                ),
                cancel_event=event,
            ).result()
            deleted = delete_result.message_upserts
            committed += delete_result.committed_bundles
            deferred += delete_result.deferred_bundles
            max_transaction_ms = max(
                max_transaction_ms, float(delete_result.max_transaction_ms or 0.0)
            )
            states.append(delete_result.state)
            if not delete_result.completed:
                self._retry(items=delete_items, code=delete_result.state)
        handled = {item.stable_key for item in delete_items + stale_aliases}
        handled.update(
            key for bundle in owned_bundles for key in bundle.dirty_keys
        )
        unsupported = tuple(item for item in items if item.stable_key not in handled)
        if unsupported:
            self._retry(items=unsupported, code="unsupported_source")
            deferred += len(unsupported)
        state = "committed" if states and all(value == "committed" for value in states) else states[-1] if states else "deferred"
        return ProjectionCycleResult(
            discovered=discovered,
            claimed=len(items),
            prepared=len(owned_bundles),
            deleted=deleted,
            committed=committed,
            deferred=deferred,
            max_transaction_ms=max_transaction_ms,
            state=state,
        )

    def repair_legacy_duplicates(
        self, *, cancel_event: threading.Event | None = None
    ) -> ProjectionRepairResult:
        """Run explicit derived-index convergence on the serialized lane."""

        return self._writer.repair_legacy_duplicates(cancel_event=cancel_event)

    @staticmethod
    def _obsolete_source_identity(item: DirtyProjectionItem) -> bool:
        """Recognize pre-v4 JS8 dirty keys without treating them as deletes."""

        if item.operation == "delete" or item.source_family != "js8":
            return False
        source_id = str(item.source_id or "")
        if not source_id.startswith("js8:"):
            return False
        suffix = source_id[len("js8:") :]
        return not suffix.startswith(("api:", "directed_txt:", "inbox_db:", "file:"))

    def _complete_stale_claims(self, items: Sequence[DirtyProjectionItem]) -> int:
        if not items:
            return 0
        conn = connect_sqlite_runtime_write(
            self.db_path, timeout=0.25, busy_timeout_ms=250
        )
        try:
            with conn:
                return complete_dirty_conn(
                    conn,
                    [item.stable_key for item in items],
                    owner=self.owner,
                )
        finally:
            conn.close()

    def _record_watch_matches(
        self,
        bundles: Sequence[ProjectionBundle],
        *,
        committed_message_ids: Sequence[str],
    ) -> None:
        """Match committed projections from one cached snapshot off the UI thread."""
        started = time.perf_counter()
        try:
            now = time.monotonic()
            if now >= self._watch_reload_due:
                self._watch_matcher = load_spotter_watch_matcher(
                    db_path=self.db_path, enabled_only=True, limit=100
                )
                self._watch_reload_due = now + 10.0
            committed = {str(value) for value in committed_message_ids}
            matches: dict[str, tuple[int, ...]] = {}
            for bundle in bundles[:MAX_CYCLE_ITEMS]:
                message = bundle.message
                if message.message_id not in committed:
                    continue
                candidate = {
                    "source_family": message.source_family,
                    "source_label": message.source_label,
                    "source_kind": message.message_type,
                    "message_type": message.message_type,
                    "display_type": message.display_type,
                    "radio_id": message.radio_id,
                    "from_call": message.from_call,
                    "to_call": message.to_call,
                    "group_name": message.group_name,
                    "status": message.status,
                    "severity": message.severity,
                    "state_code": message.state_code,
                    "grid": message.grid,
                    "subject": message.subject,
                    "summary": message.summary,
                    "body_preview": message.body_preview,
                    "search_text": message.search_text,
                    "topics": tuple(message.topics),
                }
                watch_ids = self._watch_matcher.match_candidate(candidate)
                if watch_ids:
                    matches[message.message_id] = watch_ids
            if matches:
                record_spotter_watch_candidate_matches(matches, db_path=self.db_path)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            if matches or elapsed_ms >= 10.0:
                emit_span(
                    "fio_spotter.watch_match_batch",
                    elapsed_ms,
                    meta={"candidates": len(committed), "matched": len(matches)},
                    level="warning" if elapsed_ms >= 100.0 else "debug",
                )
        except Exception as exc:
            # Watches are advisory. A busy or malformed watch store must never
            # delay, retry, or roll back the authoritative message projection.
            self._watch_reload_due = time.monotonic() + 10.0
            emit_span(
                "fio_spotter.watch_match_batch",
                (time.perf_counter() - started) * 1000.0,
                meta={"state": "deferred", "error": type(exc).__name__},
                level="debug",
            )

    def _release_claims(self, items: Sequence[DirtyProjectionItem]) -> None:
        """Make unprocessed durable work immediately available after cancel."""

        if not items:
            return
        conn: sqlite3.Connection | None = None
        try:
            conn = connect_sqlite_runtime_write(
                self.db_path, timeout=0.25, busy_timeout_ms=250
            )
            with conn:
                release_owner_leases_conn(conn, self.owner)
        except Exception:
            # A short lease is still a safe fallback if shutdown races another
            # SQLite writer.
            pass
        finally:
            if conn is not None:
                conn.close()

    def _retry(self, *, items: Sequence[DirtyProjectionItem], code: str) -> None:
        conn = connect_sqlite_runtime_write(self.db_path, timeout=0.25, busy_timeout_ms=250)
        try:
            with conn:
                retry_dirty_conn(
                    conn,
                    [item.stable_key for item in items],
                    owner=self.owner,
                    delay_seconds=0.05,
                    error_code=code,
                )
        finally:
            conn.close()

    def close(self, *, wait: bool = True) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._cancel.set()
        self._executor.shutdown(wait=wait, cancel_futures=True)
        conn: sqlite3.Connection | None = None
        try:
            conn = connect_sqlite_runtime_write(
                self.db_path, timeout=0.25, busy_timeout_ms=250
            )
            with conn:
                release_owner_leases_conn(conn, self.owner)
        except Exception:
            # Leases are time-bounded. Shutdown must not fail or hang merely
            # because another short writer owns SQLite at this instant.
            pass
        finally:
            if conn is not None:
                conn.close()


def _int_key(value: object) -> int:
    try:
        return max(0, int(str(value or "0")))
    except Exception:
        return 0
