"""MIP-2 regression contracts for identity, lifecycle, and state fidelity.

These tests intentionally exercise the source-owned tables and the durable
projection queue.  They use only temporary databases; no production profile
or hardware database is opened.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from freqinout.core.commstat_artifacts import (
    ensure_commstat_artifact_tables,
    tombstone_commstat_artifact,
)
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.message_projection_coordinator import (
    MessageProjectionCoordinator,
    reconcile_native_source_changes,
)
from freqinout.core.message_projection_queue import (
    ensure_source_dirty_triggers,
    queue_diagnostics,
)
from freqinout.core.message_projection_store import (
    ExternalMessageRef,
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
    ensure_message_projection_schema,
    mark_projected_messages_read,
    stable_message_id,
)
from freqinout.core.message_projection_writer import (
    ProjectionBundle,
    ProjectionDeleteRequest,
    ProjectionBundleWriter,
)
from freqinout.core.varac_ingest import ensure_varac_local_tables


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_js8(conn: sqlite3.Connection) -> None:
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
            flag_state INTEGER DEFAULT 0,
            source_key TEXT,
            source_id INTEGER,
            source_radio_id TEXT,
            js8_instance_id TEXT,
            source_path TEXT
        )
        """
    )


def _db_with_sources(tmp_path: Path, *, js8: bool = False, varac: bool = False, commstat: bool = False) -> Path:
    path = tmp_path / "mip2-regressions.sqlite"
    conn = _connect(path)
    try:
        ensure_message_projection_schema(conn)
        if js8:
            _ensure_js8(conn)
        if varac:
            ensure_varac_local_tables(conn)
        if commstat:
            ensure_commstat_artifact_tables(conn)
        ensure_source_dirty_triggers(conn)
        conn.commit()
    finally:
        conn.close()
    return path


def _insert_varac(conn: sqlite3.Connection, *, ingest_key: str, row_id: int, guid: str, body: str) -> None:
    conn.execute(
        """
        INSERT INTO varac_messages (
            ingest_source_key, id, guid, source, msg_type, from_call, to_call,
            subject, body, ts, band, freq_hz, snr, read_status, folder,
            file_path, vmail_guid, is_deleted, flag_state, folder_label,
            urgent, has_attachment, via_callsign
        ) VALUES (?, ?, ?, 'inbox', 'VMail', 'N0CALL', 'N1MAG',
                  'Subject', ?, 1788352800, '20m', 14078000, -10, 0,
                  'Inbox', '', '', 0, 0, 'Inbox', 0, 0, '')
        """,
        (ingest_key, row_id, guid, body),
    )


def test_varac_endpoint_identity_is_scoped_for_duplicate_ids_and_guids(tmp_path) -> None:
    """Two endpoint rows may share a VarAC id/guid without projection loss."""

    db_path = _db_with_sources(tmp_path, varac=True)
    conn = _connect(db_path)
    try:
        _insert_varac(conn, ingest_key="endpoint-a", row_id=7, guid="same-guid", body="A")
        _insert_varac(conn, ingest_key="endpoint-b", row_id=7, guid="same-guid", body="B")
        conn.commit()
    finally:
        conn.close()

    # The source trigger identity must include endpoint scope. A bare guid
    # coalesces these rows and loses one endpoint's work.
    assert queue_diagnostics(db_path)["depth"] == 2
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        result = coordinator.run_once(reconcile=False)
        assert result.committed == 2
    finally:
        coordinator.close()

    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT primary_source_id, body_preview FROM message_projection "
            "WHERE source_family='varac' ORDER BY primary_source_id"
        ).fetchall()
        assert len(rows) == 2
        assert {row["body_preview"] for row in rows} == {"A", "B"}
        refs = conn.execute(
            "SELECT source_id, external_kind, external_key FROM message_external_refs "
            "WHERE external_kind='varac_message' ORDER BY source_id"
        ).fetchall()
        assert len(refs) == 2
    finally:
        conn.close()


def test_same_varac_message_keeps_endpoint_receipts_under_one_station_message(tmp_path) -> None:
    db_path = _db_with_sources(tmp_path, varac=True)
    conn = _connect(db_path)
    try:
        _insert_varac(conn, ingest_key="endpoint-a", row_id=7, guid="shared-guid", body="Same")
        _insert_varac(conn, ingest_key="endpoint-b", row_id=7, guid="shared-guid", body="Same")
        conn.commit()
    finally:
        conn.close()

    coordinator = MessageProjectionCoordinator(db_path)
    try:
        result = coordinator.run_once(reconcile=False)
        assert result.committed == 2
    finally:
        coordinator.close()

    conn = _connect(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM message_projection WHERE source_family='varac'"
        ).fetchone()[0] == 1
        refs = conn.execute(
            "SELECT source_id FROM message_external_refs WHERE external_kind='varac_message'"
        ).fetchall()
        assert {row["source_id"] for row in refs} == {
            "varac:endpoint-a:inbox", "varac:endpoint-b:inbox"
        }
    finally:
        conn.close()


def test_deleting_one_varac_receipt_does_not_hide_same_message_from_peer_source(tmp_path) -> None:
    db_path = _db_with_sources(tmp_path, varac=True)
    conn = _connect(db_path)
    try:
        _insert_varac(conn, ingest_key="endpoint-a", row_id=7, guid="shared-guid", body="Same")
        _insert_varac(conn, ingest_key="endpoint-b", row_id=7, guid="shared-guid", body="Same")
        conn.commit()
    finally:
        conn.close()
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        assert coordinator.run_once(reconcile=False).committed == 2
    finally:
        coordinator.close()

    writer = ProjectionBundleWriter(db_path)
    try:
        result = writer.write_deletions(
            [
                ProjectionDeleteRequest(
                    source_id="varac:endpoint-a:inbox",
                    external_kind="varac_message",
                    external_key="shared-guid",
                )
            ]
        )
        assert result.completed
    finally:
        writer.close()

    conn = _connect(db_path)
    try:
        projection = conn.execute(
            "SELECT deleted FROM message_projection WHERE source_family='varac'"
        ).fetchone()
        assert projection["deleted"] == 0
        refs = conn.execute(
            "SELECT source_id, metadata_json FROM message_external_refs ORDER BY source_id"
        ).fetchall()
        assert len(refs) == 2
        assert '"source_present":false' in refs[0]["metadata_json"]
        assert "source_present" not in refs[1]["metadata_json"]
    finally:
        conn.close()


def test_varac_delete_is_scoped_to_the_deleted_endpoint(tmp_path) -> None:
    """Deleting one duplicate VarAC endpoint must not tombstone its peer."""

    db_path = _db_with_sources(tmp_path, varac=True)
    conn = _connect(db_path)
    try:
        _insert_varac(conn, ingest_key="endpoint-a", row_id=7, guid="same-guid", body="A")
        _insert_varac(conn, ingest_key="endpoint-b", row_id=7, guid="same-guid", body="B")
        conn.commit()
    finally:
        conn.close()
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        assert coordinator.run_once(reconcile=False).committed == 2
    finally:
        coordinator.close()

    conn = _connect(db_path)
    try:
        target = conn.execute(
                "SELECT source_id, external_kind, external_key FROM message_external_refs "
            "WHERE source_id LIKE 'varac:endpoint-a:%'"
        ).fetchone()
    finally:
        conn.close()
    assert target is not None

    # source_id is required here: external key alone is not an endpoint scope.
    writer = ProjectionBundleWriter(db_path)
    try:
        result = writer.write_deletions(
            [
                ProjectionDeleteRequest(
                    source_id=target["source_id"],
                    external_kind=target["external_kind"],
                    external_key=target["external_key"],
                )
            ]
        )
        assert result.completed
    finally:
        writer.close()

    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT primary_source_id, deleted FROM message_projection "
            "WHERE source_family='varac' ORDER BY primary_source_id"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["deleted"] == 1
        assert rows[1]["deleted"] == 0
    finally:
        conn.close()


def test_reconciliation_discovers_repeated_varac_id_after_watermark(tmp_path) -> None:
    """VarAC discovery cannot use id alone because endpoints repeat ids."""

    db_path = _db_with_sources(tmp_path, varac=True)
    conn = _connect(db_path)
    try:
        _insert_varac(conn, ingest_key="endpoint-a", row_id=7, guid="guid-a", body="A")
        conn.commit()
    finally:
        conn.close()
    assert reconcile_native_source_changes(db_path, sources=("varac",), limit_per_source=100)["varac"] == 1
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        assert coordinator.run_once(reconcile=False).committed == 1
    finally:
        coordinator.close()

    conn = _connect(db_path)
    try:
        _insert_varac(conn, ingest_key="endpoint-b", row_id=7, guid="guid-b", body="B")
        conn.commit()
    finally:
        conn.close()

    # The second row is not greater than the first row's id watermark. A
    # rowid/compound discovery key is required in addition to the trigger path.
    assert reconcile_native_source_changes(db_path, sources=("varac",), limit_per_source=100)["varac"] == 1


def test_source_table_creation_after_main_initializer_installs_dirty_triggers(tmp_path, monkeypatch) -> None:
    """Late-created JS8 and Spotter source schemas use the lifecycle seam."""

    config_dir = tmp_path / "config-root"
    monkeypatch.setattr("freqinout.core.message_ingest.get_config_dir", lambda: config_dir)
    db_path = config_dir / "config" / "freqinout_nets.db"
    db_path.parent.mkdir(parents=True)
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()

    ingestor = MessageIngestor({"operating_groups": []})
    ingestor._ensure_local_js8_tables()
    ingestor._ensure_spotter_table()

    conn = _connect(db_path)
    try:
        names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert "trg_mip_dirty_js8_messages_insert" in names
    assert "trg_mip_dirty_spotter_traffic_insert" in names


def test_commstat_deletion_marker_queues_and_tombstones_artifact(tmp_path) -> None:
    """A deletion marker is source change work even when the artifact remains."""

    db_path = _db_with_sources(tmp_path, commstat=True)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO commstat_artifacts (
                artifact_key, artifact_kind, event_ts, event_ts_utc, from_call,
                target, report_group, status_label, alert_color, title, body_text,
                inserted_ts, updated_ts
            ) VALUES ('artifact-1', 'STATREP', 1788352800, '2026-09-10T12:00:00Z',
                      'N0CALL', '@MR08', '@MR08', 'GREEN', 'GREEN', 'Title',
                      'Body', 1788352800, 1788352800)
            """
        )
        conn.commit()
    finally:
        conn.close()
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        assert coordinator.run_once(reconcile=False).committed == 1
    finally:
        coordinator.close()

    conn = _connect(db_path)
    try:
        tombstone_commstat_artifact(conn, artifact_key="artifact-1")
        conn.commit()
    finally:
        conn.close()
    assert queue_diagnostics(db_path)["depth"] == 1

    coordinator = MessageProjectionCoordinator(db_path)
    try:
        result = coordinator.run_once(reconcile=False)
        assert result.deleted == 1
    finally:
        coordinator.close()
    conn = _connect(db_path)
    try:
        assert conn.execute(
            "SELECT deleted FROM message_projection WHERE source_family='commstat'"
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_source_field_correction_updates_projection_when_content_hash_is_unchanged(tmp_path) -> None:
    """All displayed source fields are part of incremental change semantics."""

    db_path = _db_with_sources(tmp_path, js8=True)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO js8_messages (
                id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text,
                decoded_text, state, read_ts, flag_state, source_key, source_id
            ) VALUES (1, 'N0OLD', '@MR08', 'MSG', '2026-09-10T12:00:00Z',
                      1788352800, 'same payload', 'same payload', 'UNREAD', 0,
                      0, 'radio-a', 1)
            """
        )
        conn.commit()
    finally:
        conn.close()
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        assert coordinator.run_once(reconcile=False).committed == 1
    finally:
        coordinator.close()
    conn = _connect(db_path)
    try:
        original_hash = conn.execute(
            "SELECT content_hash FROM message_projection WHERE source_family='js8'"
        ).fetchone()[0]
    finally:
        conn.close()

    conn = _connect(db_path)
    try:
        conn.execute("UPDATE js8_messages SET from_call='N0NEW' WHERE id=1")
        conn.commit()
    finally:
        conn.close()
    coordinator = MessageProjectionCoordinator(db_path)
    try:
        assert coordinator.run_once(reconcile=False).committed == 1
    finally:
        coordinator.close()
    conn = _connect(db_path)
    try:
        updated = conn.execute(
            "SELECT from_call, content_hash FROM message_projection WHERE source_family='js8'"
        ).fetchone()
        assert updated["from_call"] == "N0NEW"
        assert updated["content_hash"] == original_hash
    finally:
        conn.close()


def test_read_and_ops_indexes_remain_consistent_across_refresh_and_delete(tmp_path) -> None:
    """Persisted read state and the compact Ops bridge follow projection state."""

    db_path = _db_with_sources(tmp_path)
    message_id = stable_message_id("mip2", "state")
    source = MessageSourceRecord(source_id="js8:state", source_family="js8", source_label="State")
    message = MessageProjectionRecord(
        message_id=message_id,
        canonical_key="js8:state:1",
        content_hash=content_hash("mip2", "state", "body"),
        primary_source_id=source.source_id,
        source_family="js8",
        source_label=source.source_label,
        message_type="MSG",
        display_type="JS8",
        status="UNREAD",
        read_state="new",
        from_call="N0CALL",
        to_call="@MR08",
        group_name="MR08",
        event_ts=1788352800,
        received_ts=1788352800,
        summary="body",
        body_preview="body",
        search_text="n0call mr08 body",
    )
    ref = ExternalMessageRef(
        message_id=message_id,
        source_id=source.source_id,
        external_kind="js8_message",
        external_key="1",
    )
    writer = ProjectionBundleWriter(db_path)
    try:
        assert writer.write_batch([ProjectionBundle(source=source, message=message, refs=(ref,))]).committed_bundles == 1
    finally:
        writer.close()

    assert mark_projected_messages_read(db_path, [message_id]) == 1
    writer = ProjectionBundleWriter(db_path)
    try:
        # A stale/new refresh must not reset a user's persisted read state.
        assert writer.write_batch([ProjectionBundle(source=source, message=message, refs=(ref,))]).state == "committed"
    finally:
        writer.close()
    conn = _connect(db_path)
    try:
        projection = conn.execute(
            "SELECT read_state, status FROM message_projection WHERE message_id=?", (message_id,)
        ).fetchone()
        bridge = conn.execute(
            "SELECT read_state, deleted FROM ops_focus_message_entities WHERE message_id=? AND kind='callsign'",
            (message_id,),
        ).fetchone()
        assert tuple(projection) == ("read", "READ")
        assert bridge["read_state"] == "read"

        writer = ProjectionBundleWriter(db_path)
        result = writer.write_deletions(
            [
                ProjectionDeleteRequest(
                    source_id=source.source_id,
                    external_kind="js8_message",
                    external_key="1",
                )
            ]
        )
        writer.close()
        assert result.completed
        deleted_bridge = conn.execute(
            "SELECT deleted FROM ops_focus_message_entities WHERE message_id=? AND kind='callsign'",
            (message_id,),
        ).fetchone()
        assert deleted_bridge[0] == 1
    finally:
        conn.close()
