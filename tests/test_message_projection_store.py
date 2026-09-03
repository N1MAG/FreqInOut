from __future__ import annotations

import importlib
import sqlite3

from freqinout.core.message_projection_store import (
    ExternalMessageRef,
    MessageArtifactRecord,
    MessageProjectionCheckpoint,
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
    ensure_message_projection_schema,
    get_message_projection_checkpoint,
    list_projected_messages,
    load_projected_external_refs_for_messages,
    load_projected_message_detail,
    mark_projected_messages_read,
    process_message_delete_queue,
    queue_message_delete,
    stable_message_id,
    upsert_external_ref,
    upsert_message_artifact,
    upsert_message_projection,
    upsert_message_source,
    upsert_projected_message,
    set_message_projection_checkpoint,
)


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _source() -> MessageSourceRecord:
    return MessageSourceRecord(
        source_id="radio1-commstat",
        source_family="commstat",
        source_label="Radio 1 CommStat",
        radio_id=1,
        endpoint_or_path="/var/lib/commstat/commstat.db",
        capabilities={"receive_reports": True, "delete_external": True},
        provenance={"rf_only": True},
    )


def _message(message_id: str = "msg-1", *, group: str = "MR08", event_ts: float = 100.0) -> MessageProjectionRecord:
    return MessageProjectionRecord(
        message_id=message_id,
        canonical_key=f"commstat:artifact:{message_id}",
        content_hash=content_hash(message_id, "body"),
        primary_source_id="radio1-commstat",
        source_family="commstat",
        source_label="Radio 1 CommStat",
        radio_id=1,
        message_type="CommStat StatRep",
        status="YELLOW",
        severity="attention",
        read_state="new",
        from_call="k7etc",
        to_call="@MR08",
        group_name=group,
        state_code="ut",
        grid="dm38",
        event_ts=event_ts,
        received_ts=event_ts,
        subject="Power report",
        summary="Power report | yellow",
        body_preview="Generator fuel low",
        topics=("Power", "Fuel"),
        actionable=True,
        operator_attention=True,
        confidence=0.92,
        search_text="power report yellow generator fuel",
    )


def test_message_projection_schema_covers_contract_tables_and_indexes() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        ensure_message_projection_schema(conn)

        assert {
            "message_sources",
            "message_projection",
            "message_external_refs",
            "message_artifacts",
            "message_delete_queue",
            "message_delete_audit",
            "message_projection_checkpoint",
        } <= {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert {"message_id", "canonical_key", "content_hash", "search_text"} <= _columns(conn, "message_projection")
        assert {"q_id", "block_id", "transfer_id", "missing_blocks_json"} <= _columns(conn, "message_artifacts")
    finally:
        conn.close()


def test_projected_message_upsert_is_idempotent_and_query_is_bounded(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    source = _source()
    first_id = stable_message_id(source.source_id, "artifact", "abc")
    first = _message(first_id, event_ts=200.0)
    second = _message(stable_message_id(source.source_id, "artifact", "def"), event_ts=100.0)

    upsert_projected_message(
        db_path,
        source=source,
        message=first,
        refs=(
            ExternalMessageRef(
                message_id=first.message_id,
                source_id=source.source_id,
                external_kind="commstat_artifact",
                external_key="abc",
                delete_capability="delete_source",
            ),
        ),
    )
    upsert_projected_message(db_path, source=source, message=first)
    upsert_projected_message(db_path, source=source, message=second)

    rows = list_projected_messages(db_path, source_family="commstat", group_name="@MR08", limit=1)

    assert [row["message_id"] for row in rows] == [first.message_id]
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0] == 1
    finally:
        conn.close()


def test_flamp_artifact_preserves_qid_and_transfer_state() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        ensure_message_projection_schema(conn)
        with conn:
            upsert_message_source(conn, MessageSourceRecord(source_id="flamp-r1", source_family="flamp"))
            upsert_message_projection(
                conn,
                _message("flamp-msg", group="MAGNET", event_ts=300.0),
            )
            upsert_external_ref(
                conn,
                ExternalMessageRef(
                    message_id="flamp-msg",
                    source_id="flamp-r1",
                    external_kind="flamp_block",
                    external_key="Q123:04",
                    external_path="/traffic/Q123-04.k2s",
                ),
            )
            upsert_message_artifact(
                conn,
                MessageArtifactRecord(
                    artifact_id="artifact-q123",
                    message_id="flamp-msg",
                    artifact_type="flamp_transfer",
                    source_id="flamp-r1",
                    external_key="Q123",
                    path="/traffic/Q123",
                    q_id="Q123",
                    block_id="04",
                    transfer_id="Q123",
                    block_count=12,
                    missing_blocks_json='["07","08"]',
                    transfer_state="partial",
                ),
            )

        row = conn.execute("SELECT q_id, block_id, transfer_state FROM message_artifacts").fetchone()
        assert row == ("Q123", "04", "partial")
    finally:
        conn.close()


def test_delete_queue_tombstones_projection_without_losing_audit() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        ensure_message_projection_schema(conn)
        with conn:
            upsert_message_source(conn, _source())
            upsert_message_projection(conn, _message("msg-delete"))
            delete_id = queue_message_delete(
                conn,
                message_id="msg-delete",
                requested_effect="delete_all_external_refs",
                requested_by="operator",
                source_scope="all_refs",
                requested_utc="2026-09-02T00:00:00+00:00",
            )

        assert delete_id
        assert conn.execute("SELECT deleted FROM message_projection WHERE message_id='msg-delete'").fetchone()[0] == 1
        assert conn.execute("SELECT state FROM message_delete_queue WHERE delete_id=?", (delete_id,)).fetchone()[0] == "queued"
        assert conn.execute("SELECT COUNT(*) FROM message_delete_audit WHERE delete_id=?", (delete_id,)).fetchone()[0] == 1
    finally:
        conn.close()


def test_load_projected_message_detail_returns_refs_and_artifacts(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    source = _source()
    message = _message("msg-detail")
    upsert_projected_message(
        db_path,
        source=source,
        message=message,
        refs=(
            ExternalMessageRef(
                message_id=message.message_id,
                source_id=source.source_id,
                external_kind="commstat_artifact",
                external_key="abc",
                external_path="/tmp/abc.json",
            ),
        ),
        artifacts=(
            MessageArtifactRecord(
                artifact_id="artifact-detail",
                message_id=message.message_id,
                artifact_type="flamp_transfer",
                source_id=source.source_id,
                external_key="abc",
                path="/tmp/Q123.k2s",
                q_id="Q123",
                block_id="04",
            ),
        ),
    )

    detail = load_projected_message_detail(db_path, message.message_id)

    assert detail["message"]["message_id"] == message.message_id
    assert detail["refs"][0]["external_key"] == "abc"
    assert detail["artifacts"][0]["q_id"] == "Q123"


def test_load_projected_external_refs_for_messages_bulk_loads_by_message(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    source = _source()
    first = _message("msg-ref-1")
    second = _message("msg-ref-2")
    upsert_projected_message(
        db_path,
        source=source,
        message=first,
        refs=(
            ExternalMessageRef(
                message_id=first.message_id,
                source_id=source.source_id,
                external_kind="flmsg_file",
                external_key="first",
                external_path="/traffic/first.k2s",
            ),
        ),
    )
    upsert_projected_message(
        db_path,
        source=source,
        message=second,
        refs=(
            ExternalMessageRef(
                message_id=second.message_id,
                source_id=source.source_id,
                external_kind="commstat_artifact",
                external_key="second",
                external_path="/traffic/second.json",
            ),
        ),
    )

    refs_by_message = load_projected_external_refs_for_messages(
        db_path,
        [first.message_id, "", second.message_id],
    )

    assert refs_by_message[first.message_id][0]["external_key"] == "first"
    assert refs_by_message[second.message_id][0]["external_key"] == "second"


def test_mark_projected_messages_read_updates_status_and_read_state(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    upsert_projected_message(db_path, source=_source(), message=_message("msg-read"))

    assert mark_projected_messages_read(db_path, ["msg-read"]) == 1

    row = list_projected_messages(db_path, include_deleted=True, limit=1)[0]
    assert row["status"] == "YELLOW"
    assert row["read_state"] == "read"


def test_mark_projected_messages_read_updates_source_refs(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        conn.execute(
            """
            CREATE TABLE js8_messages (
                id INTEGER PRIMARY KEY,
                source_id INTEGER,
                state TEXT,
                read_ts REAL
            )
            """
        )
        conn.execute("INSERT INTO js8_messages (id, source_id, state, read_ts) VALUES (1, 101, 'UNREAD', 0)")
        conn.commit()
    finally:
        conn.close()
    source = MessageSourceRecord(source_id="js8:radio-a", source_family="js8")
    message = _message("msg-js8-read")
    upsert_projected_message(
        db_path,
        source=source,
        message=message,
        refs=(
            ExternalMessageRef(
                message_id=message.message_id,
                source_id=source.source_id,
                external_kind="js8_message",
                external_key="101",
                read_capability="mark_read",
                metadata={"row_id": "1"},
            ),
        ),
    )

    assert mark_projected_messages_read(db_path, [message.message_id]) == 1

    conn = sqlite3.connect(db_path)
    try:
        state, read_ts = conn.execute("SELECT state, read_ts FROM js8_messages WHERE id=1").fetchone()
    finally:
        conn.close()
    assert state == "READ"
    assert read_ts > 0


def test_process_delete_queue_completes_hide_without_external_delete(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    upsert_projected_message(db_path, source=_source(), message=_message("msg-hide"))
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            queue_message_delete(conn, message_id="msg-hide", requested_effect="hide_fio")
    finally:
        conn.close()

    assert process_message_delete_queue(db_path) == {"completed": 1, "failed": 0, "skipped": 0}

    conn = sqlite3.connect(db_path)
    try:
        state = conn.execute("SELECT state FROM message_delete_queue").fetchone()[0]
        audit_count = conn.execute("SELECT COUNT(*) FROM message_delete_audit").fetchone()[0]
    finally:
        conn.close()
    assert state == "completed"
    assert audit_count == 2


def test_process_delete_queue_deletes_source_refs(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        conn.execute(
            """
            CREATE TABLE spotter_traffic (
                id INTEGER PRIMARY KEY,
                state TEXT
            )
            """
        )
        conn.execute("INSERT INTO spotter_traffic (id, state) VALUES (42, 'UNREAD')")
        conn.commit()
    finally:
        conn.close()
    source = MessageSourceRecord(source_id="spotter:radio-a", source_family="spotter")
    message = _message("msg-spotter-delete")
    upsert_projected_message(
        db_path,
        source=source,
        message=message,
        refs=(
            ExternalMessageRef(
                message_id=message.message_id,
                source_id=source.source_id,
                external_kind="spotter_message",
                external_key="42",
                delete_capability="delete_source",
                metadata={"row_id": "42"},
            ),
        ),
    )
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            queue_message_delete(conn, message_id=message.message_id, requested_effect="source_delete")
    finally:
        conn.close()

    assert process_message_delete_queue(db_path) == {"completed": 1, "failed": 0, "skipped": 0}

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM spotter_traffic").fetchone()[0] == 0
    finally:
        conn.close()


def test_process_delete_queue_audit_only_minimizes_body_preview(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    upsert_projected_message(db_path, source=_source(), message=_message("msg-audit"))
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            queue_message_delete(conn, message_id="msg-audit", requested_effect="audit_only")
    finally:
        conn.close()

    assert process_message_delete_queue(db_path)["completed"] == 1

    row = list_projected_messages(db_path, include_deleted=True, limit=1)[0]
    assert row["body_preview"] == ""
    assert row["retention_class"] == "audit_only"


def test_process_delete_queue_deletes_file_refs(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    file_path = tmp_path / "message.k2s"
    file_path.write_text("payload", encoding="utf-8")
    source = MessageSourceRecord(source_id="flamp-r1", source_family="flamp")
    message = _message("msg-file")
    upsert_projected_message(
        db_path,
        source=source,
        message=message,
        refs=(
            ExternalMessageRef(
                message_id=message.message_id,
                source_id=source.source_id,
                external_kind="flamp_file",
                external_key="Q123",
                external_path=str(file_path),
                delete_capability="file_delete",
            ),
        ),
    )
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            queue_message_delete(conn, message_id=message.message_id, requested_effect="delete_external")
    finally:
        conn.close()

    assert process_message_delete_queue(db_path)["completed"] == 1
    assert not file_path.exists()


def test_message_projection_checkpoint_round_trips(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        with conn:
            set_message_projection_checkpoint(
                conn,
                MessageProjectionCheckpoint(
                    source_id="messages:unified_rows",
                    last_external_key="abc",
                    last_event_ts=123.4,
                    content_fingerprint="fp-1",
                    updated_utc="2026-09-02T00:00:00+00:00",
                ),
            )
    finally:
        conn.close()

    checkpoint = get_message_projection_checkpoint(db_path, "messages:unified_rows")

    assert checkpoint.source_id == "messages:unified_rows"
    assert checkpoint.last_external_key == "abc"
    assert checkpoint.last_event_ts == 123.4
    assert checkpoint.content_fingerprint == "fp-1"


def test_db_initializer_ensures_message_projection_tables(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)
    db_initializer.ensure_all_tables()

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        assert {"message_id", "canonical_key", "content_hash"} <= _columns(conn, "message_projection")
        assert {"source_id", "external_kind", "external_key"} <= _columns(conn, "message_external_refs")
    finally:
        conn.close()
