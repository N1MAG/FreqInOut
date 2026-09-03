from __future__ import annotations

import importlib
import sqlite3

from freqinout.core.message_projection_store import (
    ExternalMessageRef,
    MessageArtifactRecord,
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
    ensure_message_projection_schema,
    list_projected_messages,
    queue_message_delete,
    stable_message_id,
    upsert_external_ref,
    upsert_message_artifact,
    upsert_message_projection,
    upsert_message_source,
    upsert_projected_message,
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
