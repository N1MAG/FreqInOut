"""MIP-1 schema ownership and low-level projection-writer contract tests.

These tests intentionally keep the migration boundary separate from row-level
store helpers.  The startup owner is allowed to create/upgrade the projection
schema; a prepared writer batch must receive an already migrated connection and
must not perform DDL or schema introspection for each row.
"""

from __future__ import annotations

import sqlite3
import threading

from freqinout.core.message_projection_store import (
    ExternalMessageRef,
    MessageArtifactRecord,
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
    ensure_message_projection_schema,
    upsert_external_ref,
    upsert_message_artifact,
    upsert_message_projection,
    upsert_message_source,
)
from freqinout.core.message_projection_writer import (
    ProjectionBundle,
    ProjectionBundleWriter,
    get_projection_writer,
)


def _source() -> MessageSourceRecord:
    return MessageSourceRecord(
        source_id="mip1-source",
        source_family="js8",
        source_label="MIP-1 test source",
    )


def _message(message_id: str = "mip1-message") -> MessageProjectionRecord:
    return MessageProjectionRecord(
        message_id=message_id,
        canonical_key=f"js8:{message_id}",
        content_hash=content_hash(message_id, "body"),
        primary_source_id="mip1-source",
        source_family="js8",
        source_label="MIP-1 test source",
        message_type="Message",
        status="NEW",
        severity="info",
        read_state="new",
        from_call="N0CALL",
        to_call="@MR08",
        group_name="MR08",
        event_ts=100.0,
        received_ts=100.0,
        subject="MIP-1 test",
        summary="MIP-1 test",
        body_preview="body",
        search_text="mip1 test body",
    )


def _bundle(message_id: str = "mip1-message", *, source: MessageSourceRecord | None = None) -> ProjectionBundle:
    source = source or _source()
    message = _message(message_id)
    return ProjectionBundle(source=source, message=message)


def test_startup_schema_owner_is_additive_and_idempotent() -> None:
    """Schema setup may be run repeatedly without rewriting existing evidence."""

    conn = sqlite3.connect(":memory:")
    try:
        ensure_message_projection_schema(conn)
        with conn:
            upsert_message_source(conn, _source())
            upsert_message_projection(conn, _message())

        before = conn.execute(
            "SELECT message_id, canonical_key, content_hash FROM message_projection"
        ).fetchall()
        ensure_message_projection_schema(conn)
        after = conn.execute(
            "SELECT message_id, canonical_key, content_hash FROM message_projection"
        ).fetchall()

        assert before == after
        assert conn.execute("SELECT COUNT(*) FROM message_projection_dirty").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM message_projection_source_state").fetchone()[0] == 0
        assert conn.execute("SELECT generation FROM message_projection_generation WHERE singleton=1").fetchone()[0] == 0
    finally:
        conn.close()


def test_row_level_projection_helpers_do_not_assure_schema(monkeypatch) -> None:
    """Prepared row writes require startup-owned schema and perform only DML."""

    conn = sqlite3.connect(":memory:")
    try:
        # The migration is deliberately completed before the assurance hook is
        # disabled, matching the serialized writer's connection contract.
        ensure_message_projection_schema(conn)

        def fail_schema_assurance(_connection: sqlite3.Connection) -> None:
            raise AssertionError("row-level projection helper performed schema assurance")

        import freqinout.core.message_projection_store as store

        monkeypatch.setattr(store, "ensure_message_projection_schema", fail_schema_assurance)
        source = _source()
        message = _message()
        ref = ExternalMessageRef(
            message_id=message.message_id,
            source_id=source.source_id,
            external_kind="js8_message",
            external_key="external-1",
        )
        artifact = MessageArtifactRecord(
            artifact_id="artifact-1",
            message_id=message.message_id,
            artifact_type="attachment",
            source_id=source.source_id,
            external_key="external-1",
        )

        with conn:
            upsert_message_source(conn, source)
            upsert_message_projection(conn, message)
            upsert_external_ref(conn, ref)
            upsert_message_artifact(conn, artifact)

        assert conn.execute("SELECT COUNT(*) FROM message_sources").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM message_artifacts").fetchone()[0] == 1
    finally:
        conn.close()


def _prepared_writer(tmp_path):
    db_path = tmp_path / "writer.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_writer_bundle_is_atomic_when_a_later_bundle_fails(tmp_path, monkeypatch) -> None:
    db_path = _prepared_writer(tmp_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    import freqinout.core.message_projection_writer as writer_module

    original = writer_module.upsert_message_projection
    calls = 0

    def fail_second(conn, message, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected bundle failure")
        return original(conn, message, **kwargs)

    monkeypatch.setattr(writer_module, "upsert_message_projection", fail_second)
    try:
        result = writer.write_batch((_bundle("first"), _bundle("second")))
        assert result.state == "failed"
        assert result.committed_bundles == 0
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM message_sources").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 0
        finally:
            conn.close()
    finally:
        writer.close()


def test_writer_caps_cpu_and_transaction_units_at_25_bundles(tmp_path) -> None:
    db_path = _prepared_writer(tmp_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        result = writer.write_batch(tuple(_bundle(f"message-{idx}") for idx in range(101)))
        assert result.state == "committed"
        assert result.committed_bundles == 101
        assert result.transactions == 5
        assert result.deferred_bundles == 0
    finally:
        writer.close()


def test_writer_identical_bundle_performs_zero_dml(tmp_path) -> None:
    db_path = _prepared_writer(tmp_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        bundle = _bundle()
        first = writer.write_batch((bundle,))
        assert first.committed_bundles == 1

        statements: list[str] = []
        conn = sqlite3.connect(db_path)
        conn.set_trace_callback(statements.append)
        try:
            second = writer.write_batch((bundle,))
        finally:
            conn.set_trace_callback(None)
            conn.close()

        # The writer's own connection is separate; validate the observable
        # contract through its counters rather than relying on another
        # connection's trace callback.
        assert second.state == "committed"
        assert second.skipped_bundles == 1
        assert second.committed_bundles == 0
        assert second.source_upserts == 0
        assert second.message_upserts == 0
        assert second.ref_upserts == 0
        assert second.artifact_upserts == 0
        assert second.indexed_messages == 0
    finally:
        writer.close()


def test_writer_deduplicates_source_upsert_within_batch(tmp_path) -> None:
    db_path = _prepared_writer(tmp_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        result = writer.write_batch((_bundle("first"), _bundle("second")))
        assert result.state == "committed"
        assert result.committed_bundles == 2
        assert result.source_upserts == 1
        assert result.message_upserts == 2
    finally:
        writer.close()


def test_writer_updates_only_the_changed_message_component(tmp_path) -> None:
    db_path = _prepared_writer(tmp_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        original = _bundle()
        assert writer.write_batch((original,)).committed_bundles == 1
        changed_message = MessageProjectionRecord(
            **{
                **original.message.__dict__,
                "content_hash": content_hash(original.message.message_id, "changed body"),
                "body_preview": "changed body",
            }
        )
        result = writer.write_batch((ProjectionBundle(source=original.source, message=changed_message),))
        assert result.source_upserts == 0
        assert result.message_upserts == 1
        assert result.ref_upserts == 0
        assert result.artifact_upserts == 0
        assert result.indexed_messages == 1
    finally:
        writer.close()


def test_writer_detects_semantic_message_corrections_without_a_hash_change(tmp_path) -> None:
    """Projection-owned fields may change while the payload hash remains stable."""

    db_path = _prepared_writer(tmp_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        original = _bundle()
        assert writer.write_batch((original,)).committed_bundles == 1
        corrected = MessageProjectionRecord(
            **{
                **original.message.__dict__,
                # Keep content_hash deliberately unchanged: this is a
                # classification/display correction, not a new payload.
                "summary": "Corrected summary",
                "severity": "attention",
                "inbox_visible": False,
                "inbox_suppression_reason": "noise",
                "classification_version": 7,
            }
        )

        result = writer.write_batch((ProjectionBundle(source=original.source, message=corrected),))

        assert result.message_upserts == 1
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT summary, severity, inbox_visible, inbox_suppression_reason, classification_version "
                "FROM message_projection WHERE message_id=?",
                (corrected.message_id,),
            ).fetchone()
            assert row == ("Corrected summary", "attention", 0, "noise", 7)
        finally:
            conn.close()
    finally:
        writer.close()


def test_writer_ignores_volatile_source_ingested_time_but_writes_source_corrections(tmp_path) -> None:
    db_path = _prepared_writer(tmp_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        original = _bundle(source=MessageSourceRecord(**{**_source().__dict__, "last_ingested_utc": "2026-09-10T01:00:00Z"}))
        assert writer.write_batch((original,)).source_upserts == 1

        replayed = MessageSourceRecord(
            **{**original.source.__dict__, "last_ingested_utc": "2026-09-10T02:00:00Z"}
        )
        same_result = writer.write_batch((ProjectionBundle(source=replayed, message=original.message),))
        assert same_result.source_upserts == 0
        assert same_result.message_upserts == 0

        corrected = MessageSourceRecord(
            **{**replayed.__dict__, "source_label": "Corrected MIP-1 source"}
        )
        correction_result = writer.write_batch((ProjectionBundle(source=corrected, message=original.message),))
        assert correction_result.source_upserts == 1
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT source_label, last_ingested_utc FROM message_sources WHERE source_id=?",
                (corrected.source_id,),
            ).fetchone()
            assert row == ("Corrected MIP-1 source", "2026-09-10T02:00:00Z")
        finally:
            conn.close()
    finally:
        writer.close()


def test_projection_writer_registry_returns_one_writer_per_database(tmp_path) -> None:
    first_path = tmp_path / "first.sqlite"
    second_path = tmp_path / "second.sqlite"
    first = get_projection_writer(first_path)
    try:
        assert first is get_projection_writer(first_path)
        assert first is get_projection_writer(first_path.parent / "." / first_path.name)
        assert first is not get_projection_writer(second_path)
    finally:
        # Close both registry-owned writers so the test cannot leave daemon
        # worker threads behind for the rest of the suite.
        first.close()
        get_projection_writer(second_path).close()


def test_writer_busy_retry_is_bounded_and_preserves_deferred_bundle(tmp_path) -> None:
    db_path = _prepared_writer(tmp_path)
    lock_conn = sqlite3.connect(db_path, timeout=0.01)
    lock_conn.execute("BEGIN EXCLUSIVE")
    writer = ProjectionBundleWriter(db_path, busy_retry_seconds=0.10)
    try:
        result = writer.write_batch((_bundle(),))
        assert result.state == "delayed"
        assert result.busy_retries >= 1
        assert result.committed_bundles == 0
        assert result.deferred_message_ids == ("mip1-message",)
    finally:
        lock_conn.rollback()
        lock_conn.close()
        writer.close()


def test_writer_cancellation_does_not_start_a_partial_bundle(tmp_path) -> None:
    db_path = _prepared_writer(tmp_path)
    cancelled = threading.Event()
    cancelled.set()
    writer = ProjectionBundleWriter(db_path)
    try:
        result = writer.write_batch((_bundle(),), cancel_event=cancelled)
        assert result.state == "cancelled"
        assert result.committed_bundles == 0
        assert result.deferred_message_ids == ("mip1-message",)
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 0
        finally:
            conn.close()
    finally:
        writer.close()


def test_writer_commit_advances_generation_once_per_transaction(tmp_path) -> None:
    db_path = _prepared_writer(tmp_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        result = writer.write_batch((_bundle("first"), _bundle("second")))
        assert result.transactions == 1
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT generation FROM message_projection_generation WHERE singleton=1"
            ).fetchone()[0] == 1
        finally:
            conn.close()
    finally:
        writer.close()
