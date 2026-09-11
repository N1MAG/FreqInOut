from __future__ import annotations

import sqlite3

from freqinout.core import message_projection_store as projection_store
from freqinout.core.message_projection_store import (
    MAX_PROJECTED_MESSAGE_PAGE_SIZE,
    MessageProjectionCheckpoint,
    count_projected_messages,
    ensure_message_projection_schema,
    get_message_projection_checkpoint,
    get_message_projection_generation,
    list_projected_messages,
    load_projected_external_refs_for_messages,
    load_projected_message_detail,
    query_projected_inbox_focus_counts,
    query_projected_message_page,
)


def _seed_projection_rows(db_path, *, count: int = 205) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        with conn:
            for index in range(count):
                family = "commstat" if index % 2 == 0 else "sitrep"
                group = "MR08" if index % 3 == 0 else "MAGNET"
                conn.execute(
                    """
                    INSERT INTO message_projection (
                        message_id, canonical_key, content_hash, primary_source_id,
                        source_family, group_name, status, severity, deleted, archived,
                        inbox_visible, operator_attention, actionable, event_ts,
                        received_ts, search_text, projected_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 1, 0, 0, ?, ?, ?, ?)
                    """,
                    (
                        f"message-{index:04d}",
                        f"{family}:row:{index}",
                        f"hash-{index}",
                        f"{family}:source",
                        family,
                        group,
                        "NEW" if index % 5 else "READ",
                        "warning" if index % 7 == 0 else "info",
                        float(index),
                        float(index),
                        f"alpha traffic {index}",
                        "2026-09-10T00:00:00+00:00",
                    ),
                )
    finally:
        conn.close()


def test_mip4_page_is_hard_capped_keyset_paginated_and_counted(tmp_path) -> None:
    db_path = tmp_path / "projection.db"
    _seed_projection_rows(db_path)

    first = query_projected_message_page(db_path, page_size=9_999, include_total=True)

    assert len(first.rows) == MAX_PROJECTED_MESSAGE_PAGE_SIZE
    assert first.total_count == 205
    assert first.generation == 0
    assert first.next_cursor is not None
    assert [row["message_id"] for row in first.rows[:3]] == [
        "message-0204",
        "message-0203",
        "message-0202",
    ]

    second = query_projected_message_page(db_path, page_size=200, cursor=first.next_cursor, include_total=True)

    assert len(second.rows) == 5
    assert second.total_count == 205
    assert second.next_cursor is None
    assert not ({row["message_id"] for row in first.rows} & {row["message_id"] for row in second.rows})


def test_mip4_page_reads_rows_count_and_generation_in_one_readonly_snapshot(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "projection.db"
    _seed_projection_rows(db_path, count=3)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE message_projection_generation SET generation=37 WHERE singleton=1"
        )
        conn.commit()
    finally:
        conn.close()

    calls = 0
    statements: list[str] = []
    real_readonly = projection_store.connect_sqlite_readonly

    def traced_readonly(*args, **kwargs):
        nonlocal calls
        calls += 1
        conn = real_readonly(*args, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(projection_store, "connect_sqlite_readonly", traced_readonly)
    page = query_projected_message_page(db_path, include_total=True)

    assert calls == 1
    assert len(page.rows) == 3
    assert page.total_count == 3
    assert page.generation == 37
    assert get_message_projection_generation(db_path) == 37
    normalized = "\n".join(statements).upper()
    assert "BEGIN" in normalized
    assert "COUNT(*) AS COUNT FROM MESSAGE_PROJECTION" in normalized
    assert "FROM MESSAGE_PROJECTION_GENERATION" in normalized


def test_mip4_filter_count_uses_migration_indexes_and_returns_matching_page(tmp_path) -> None:
    db_path = tmp_path / "projection.db"
    _seed_projection_rows(db_path)

    page = query_projected_message_page(
        db_path,
        source_family="COMMSTAT",
        group_name="@MR08",
        status="new",
        include_total=True,
    )

    assert page.total_count == count_projected_messages(
        db_path,
        source_family="commstat",
        group_name="MR08",
        status="NEW",
    )
    assert page.rows
    assert all(row["source_family"] == "commstat" for row in page.rows)
    assert all(row["group_name"] == "MR08" for row in page.rows)
    assert all(row["status"] == "NEW" for row in page.rows)

    conn = sqlite3.connect(db_path)
    try:
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(message_projection)").fetchall()}
        plan = conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM message_projection
             WHERE deleted=0 AND archived=0 AND inbox_visible=1
               AND source_family=?
             ORDER BY COALESCE(NULLIF(received_ts, 0), event_ts, 0) DESC,
                      event_ts DESC, message_id DESC
             LIMIT 200
            """,
            ("commstat",),
        ).fetchall()
    finally:
        conn.close()

    assert "idx_msg_projection_model_default" in indexes
    assert "idx_msg_projection_model_source" in indexes
    assert "idx_msg_projection_model_group" in indexes
    assert "idx_msg_projection_model_received" in indexes
    assert "idx_msg_projection_model_received_v2" in indexes
    assert "idx_msg_projection_model_source_received_v2" in indexes
    assert any("idx_msg_projection_model_source_received_v2" in str(row) for row in plan)


def test_inbox_read_model_collapses_legacy_and_safe_identities_for_same_file_version(tmp_path) -> None:
    """A file-path identity upgrade must not create two reader positions."""

    db_path = tmp_path / "projection.db"
    _seed_projection_rows(db_path, count=0)
    path = "/home/operator/.nbems/ICS/messages/report.k2s"
    rows = (
        (
            "legacy-file",
            "flmsg:/home/operator/.nbems/ICS/messages",
            "2026-09-06T00:00:00+00:00",
        ),
        (
            "safe-file",
            "flmsg:L2hvbWUvb3BlcmF0b3IvLm5iZW1zL0lDUy9tZXNzYWdlcw",
            "2026-09-11T00:00:00+00:00",
        ),
    )
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            for message_id, source_id, projected_utc in rows:
                conn.execute(
                    """
                    INSERT INTO message_projection (
                        message_id, canonical_key, content_hash, primary_source_id,
                        source_family, message_type, status, read_state, deleted,
                        archived, inbox_visible, event_ts, received_ts, projected_utc
                    ) VALUES (?, ?, ?, ?, 'flmsg', 'FLMSG', 'NEW', 'new', 0, 0, 1, 50, 100, ?)
                    """,
                    (message_id, f"key:{message_id}", f"hash:{message_id}", source_id, projected_utc),
                )
                conn.execute(
                    """
                    INSERT INTO message_external_refs (
                        message_id, source_id, external_kind, external_key,
                        external_path, external_mtime, external_size, updated_utc
                    ) VALUES (?, ?, 'flmsg_file', ?, ?, 100, 42, ?)
                    """,
                    (message_id, source_id, f"external:{message_id}", path, projected_utc),
                )
    finally:
        conn.close()

    page = query_projected_message_page(db_path, include_total=True)
    counts = query_projected_inbox_focus_counts(db_path)

    assert [row["message_id"] for row in page.rows] == ["safe-file"]
    assert page.total_count == 1
    assert count_projected_messages(db_path) == 1
    assert counts.counts["all"] == 1
    assert counts.counts["forms"] == 1

    conn = sqlite3.connect(db_path)
    try:
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(message_external_refs)")}
    finally:
        conn.close()
    assert "idx_msg_refs_file_identity" in indexes


def test_mip4_page_applies_multi_group_identity_type_and_age_filters_before_limit(tmp_path) -> None:
    db_path = tmp_path / "projection.db"
    _seed_projection_rows(db_path, count=205)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE message_projection SET from_call='K7ABC', to_call='N1MAG', message_type='MSG'"
        )
        conn.commit()
    finally:
        conn.close()

    page = query_projected_message_page(
        db_path,
        group_names=("@MR08", "MAGNET", "MR08"),
        statuses=("NEW", "UNREAD"),
        from_call="k7abc",
        to_call="n1mag",
        message_type="msg",
        received_after_ts=10.0,
        received_before_ts=30.0,
        include_total=True,
    )

    assert page.total_count == 16
    assert len(page.rows) == 16
    assert all(10.0 <= float(row["received_ts"]) <= 30.0 for row in page.rows)
    assert all(row["status"] == "NEW" for row in page.rows)


def test_inbox_focus_counts_are_one_scoped_snapshot_independent_of_active_focus(tmp_path) -> None:
    db_path = tmp_path / "projection.db"
    _seed_projection_rows(db_path, count=0)
    rows = [
        ("form", "flmsg", "FLMSG", "NEW", "new", "MR08", 101.0),
        ("spotter", "js8", "F!104", "NEW", "new", "MR08", 102.0),
        ("commstat", "commstat", "COMMSTAT", "YELLOW", "new", "MR08", 103.0),
        ("mesh", "meshcore", "MSG", "UNREAD", "unread", "MR08", 104.0),
        ("varac", "varac", "VARAC", "ALERT", "alert", "MR08", 105.0),
        ("bbs", "bbs_archive", "BBS", "RED", "new", "MR08", 106.0),
        ("read-wins", "flamp", "FLAMP", "READ", "new", "MR08", 107.0),
        ("other-group", "flmsg", "FLMSG", "NEW", "new", "MAGNET", 108.0),
        ("too-old", "flmsg", "FLMSG", "NEW", "new", "MR08", 10.0),
    ]
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            for message_id, family, message_type, status, read_state, group, received_ts in rows:
                conn.execute(
                    """
                    INSERT INTO message_projection (
                        message_id, canonical_key, content_hash, primary_source_id,
                        source_family, message_type, group_name, status, read_state,
                        deleted, archived, inbox_visible, event_ts, received_ts, projected_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 1, ?, ?, ?)
                    """,
                    (
                        message_id,
                        f"key:{message_id}",
                        f"hash:{message_id}",
                        f"source:{family}",
                        family,
                        message_type,
                        group,
                        status,
                        read_state,
                        received_ts,
                        received_ts,
                        "2026-09-10T00:00:00+00:00",
                    ),
                )
            conn.execute(
                "UPDATE message_projection_generation SET generation=41 WHERE singleton=1"
            )
    finally:
        conn.close()

    snapshot = query_projected_inbox_focus_counts(
        db_path,
        group_names=("@MR08", "MR08"),
        received_after_ts=100.0,
    )

    assert snapshot.generation == 41
    assert dict(snapshot.counts) == {
        "all": 6,
        "new": 6,
        "forms": 1,
        "spotter": 1,
        "commstat": 1,
        "js8call": 2,
        "mesh": 1,
        "varac": 1,
        "bbs": 1,
    }


def test_mip4_read_apis_are_readonly_and_never_repair_schema(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "projection.db"
    _seed_projection_rows(db_path, count=2)
    statements: list[str] = []
    real_readonly = projection_store.connect_sqlite_readonly

    def traced_readonly(*args, **kwargs):
        conn = real_readonly(*args, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    def fail_schema_repair(_conn) -> None:
        raise AssertionError("read API attempted schema repair")

    def fail_writable(*_args, **_kwargs):
        raise AssertionError("read API attempted writable connection")

    monkeypatch.setattr(projection_store, "connect_sqlite_readonly", traced_readonly)
    monkeypatch.setattr(projection_store, "connect_sqlite", fail_writable)
    monkeypatch.setattr(projection_store, "ensure_message_projection_schema", fail_schema_repair)

    first_id = "message-0000"
    assert list_projected_messages(db_path, limit=10)
    assert count_projected_messages(db_path) == 2
    assert query_projected_message_page(db_path, include_total=True).total_count == 2
    assert load_projected_message_detail(db_path, first_id)["message"]["message_id"] == first_id
    assert load_projected_external_refs_for_messages(db_path, [first_id]) == {first_id: []}
    assert get_message_projection_checkpoint(db_path, "mip4") == MessageProjectionCheckpoint(source_id="mip4")

    mutating = ("CREATE ", "ALTER ", "INSERT ", "UPDATE ", "DELETE ", "REPLACE ", "DROP ")
    assert not any(statement.lstrip().upper().startswith(mutating) for statement in statements)
