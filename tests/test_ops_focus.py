from __future__ import annotations

import sqlite3

from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.message_projection_store import (
    MessageProjectionRecord,
    content_hash,
    upsert_message_projection,
)
from freqinout.core.operator_identity import change_operator_callsign, resolve_operator_identity
from freqinout.core.observation_projection import Observation
from freqinout.core.observation_store import upsert_observation_conn
from freqinout.core.ops_focus import (
    OpsFocus,
    backfill_ops_focus_index,
    build_focus_snapshot,
    format_focus_last_known,
    search_focus_suggestions,
)


def _message(
    message_id: str,
    *,
    from_call: str = "",
    to_call: str = "",
    group: str = "",
    received_ts: float,
    source: str = "js8call",
    topics: tuple[str, ...] = (),
    entities: dict[str, object] | None = None,
    status: str = "info",
    read_state: str = "new",
    actionable: bool = False,
    archived: bool = False,
    deleted: bool = False,
) -> MessageProjectionRecord:
    return MessageProjectionRecord(
        message_id=message_id,
        canonical_key=f"key:{message_id}",
        content_hash=content_hash(message_id),
        primary_source_id="source:test",
        source_family=source,
        from_call=from_call,
        to_call=to_call,
        group_name=group,
        event_ts=received_ts - 60,
        received_ts=received_ts,
        received_utc="2026-09-04T12:00:00+00:00",
        summary=f"Summary for {message_id}",
        topics=topics,
        entities=entities or {},
        status=status,
        read_state=read_state,
        actionable=actionable,
        archived=archived,
        deleted=deleted,
    )


def test_incremental_index_returns_categorized_suggestions() -> None:
    conn = sqlite3.connect(":memory:")
    upsert_message_projection(
        conn,
        _message(
            "m1",
            from_call="K7ETC",
            group="MR08",
            received_ts=1_725_500_000.0,
            topics=("Power",),
            entities={"event": "Pine Ridge Fire"},
        ),
    )

    assert search_focus_suggestions(conn, "power")[0].focus.kind == "topic"
    event = search_focus_suggestions(conn, "pine")
    assert event and event[0].focus.kind == "event"
    assert event[0].primary_text == "Pine Ridge Fire"
    group = search_focus_suggestions(conn, "MR08")
    assert any(item.focus.kind == "group" for item in group)


def test_snapshot_keeps_current_scope_separate_from_last_known() -> None:
    conn = sqlite3.connect(":memory:")
    now = 1_725_600_000.0
    upsert_message_projection(
        conn,
        _message(
            "old",
            from_call="K7ETC",
            group="MR08",
            received_ts=now - 35 * 86400,
            status="green",
            read_state="read",
        ),
    )
    focus = next(
        item.focus
        for item in search_focus_suggestions(conn, "K7ETC")
        if item.focus.kind == "callsign"
    )

    snapshot = build_focus_snapshot(conn, focus, age_seconds=86400, now=now)

    assert snapshot.current_count == 0
    assert snapshot.current_unread == 0
    assert snapshot.historical_summary is not None
    assert snapshot.historical_summary.latest_status_at_receipt == "green"
    assert "No traffic received in selected 1 day" == snapshot.current_scope_summary
    assert "Last known · 35d ago" in format_focus_last_known(snapshot.historical_summary, now=now)


def test_archived_evidence_supplies_last_known_but_not_current_counts() -> None:
    conn = sqlite3.connect(":memory:")
    now = 1_725_600_000.0
    upsert_message_projection(
        conn,
        _message("archived", from_call="K7ETC", received_ts=now - 60, archived=True),
    )
    focus = search_focus_suggestions(conn, "K7ETC")[0].focus

    snapshot = build_focus_snapshot(conn, focus, age_seconds=3600, now=now)

    assert snapshot.current_count == 0
    assert snapshot.historical_summary is not None
    assert snapshot.historical_summary.latest_message_ref == "archived"


def test_deleted_latest_evidence_does_not_reappear_as_last_known() -> None:
    conn = sqlite3.connect(":memory:")
    now = 1_725_600_000.0
    upsert_message_projection(conn, _message("older", group="MR08", received_ts=now - 120))
    upsert_message_projection(conn, _message("newer", group="MR08", received_ts=now - 60))
    upsert_message_projection(
        conn,
        _message("newer", group="MR08", received_ts=now - 60, deleted=True),
    )
    focus = next(item.focus for item in search_focus_suggestions(conn, "MR08") if item.focus.kind == "group")

    snapshot = build_focus_snapshot(conn, focus, age_seconds=3600, now=now)

    assert snapshot.current_count == 1
    assert snapshot.historical_summary is not None
    assert snapshot.historical_summary.latest_message_ref == "older"


def test_former_callsign_suggestion_and_evidence_resolve_to_stable_operator() -> None:
    conn = sqlite3.connect(":memory:")
    ensure_operator_checkins_schema(conn)
    conn.execute(
        "INSERT INTO operator_checkins(callsign,name,first_seen_utc) VALUES ('K1OLD','Casey','20240101')"
    )
    ensure_operator_checkins_schema(conn)
    identity = resolve_operator_identity(conn, "K1OLD")
    assert identity is not None
    change_operator_callsign(conn, "K1OLD", "K1NEW", effective_at=1_725_494_400.0)
    upsert_message_projection(
        conn,
        _message("former", from_call="K1OLD", received_ts=1_725_000_000.0),
    )

    suggestion = search_focus_suggestions(conn, "K1OLD")[0]
    assert suggestion.focus.operator_id == identity.operator_id
    assert suggestion.primary_text == "K1NEW"
    assert "formerly K1OLD" in suggestion.secondary_text
    snapshot = build_focus_snapshot(conn, suggestion.focus, age_seconds=0, now=1_725_600_000.0)
    assert snapshot.current_count == 1
    assert snapshot.aliases == ("K1NEW", "K1OLD")


def test_backfill_is_bounded_and_resumable() -> None:
    conn = sqlite3.connect(":memory:")
    # Use normal projection writes, then reset only the compact index to emulate
    # a pre-index database.
    for idx in range(3):
        upsert_message_projection(
            conn,
            _message(f"m{idx}", group="MAGNET", received_ts=1_725_000_000.0 + idx),
        )
    conn.execute("DELETE FROM ops_focus_message_entities")
    conn.execute("DELETE FROM ops_focus_entities")
    conn.execute("DELETE FROM ops_focus_backfill_state")
    conn.row_factory = sqlite3.Row

    count1, complete1 = backfill_ops_focus_index(conn, batch_size=2)
    count2, complete2 = backfill_ops_focus_index(conn, batch_size=2)

    assert (count1, complete1) == (2, False)
    assert (count2, complete2) == (1, True)
    assert conn.execute(
        "SELECT COUNT(DISTINCT message_id) FROM ops_focus_message_entities"
    ).fetchone()[0] == 3


def test_operator_last_seen_supplies_last_known_without_message_history() -> None:
    conn = sqlite3.connect(":memory:")
    ensure_operator_checkins_schema(conn)
    conn.execute(
        "INSERT INTO operator_checkins(callsign,name,last_seen_utc) "
        "VALUES ('K1ABC','Alex','2026-08-01T12:00:00+00:00')"
    )
    ensure_operator_checkins_schema(conn)
    focus = search_focus_suggestions(conn, "K1ABC")[0].focus

    snapshot = build_focus_snapshot(conn, focus, age_seconds=86400, now=1_788_523_200.0)

    assert snapshot.current_count == 0
    assert snapshot.historical_summary is not None
    assert snapshot.historical_summary.latest_observation_ref == "operator:last_seen"
    assert snapshot.historical_summary.latest_source == "operator history"
    assert "No traffic received in selected 1 day" == snapshot.current_scope_summary


def test_observation_incrementally_drives_discovery_and_last_known_not_traffic_count() -> None:
    conn = sqlite3.connect(":memory:")
    upsert_observation_conn(
        conn,
        Observation(
            observation_id="obs-1",
            source_family="spotter",
            source_ref="rf:1",
            received_utc="2026-09-01T12:00:00+00:00",
            event_utc="2026-09-01T11:58:00+00:00",
            from_call="K7ETC",
            groups=("MR08",),
            observed_topics=("Power",),
            status="yellow",
            summary="Commercial power intermittent",
            state="UT",
        ),
    )

    topic = next(item.focus for item in search_focus_suggestions(conn, "power") if item.focus.kind == "topic")
    snapshot = build_focus_snapshot(conn, topic, age_seconds=30 * 86400, now=1_788_523_200.0)

    assert snapshot.current_count == 0
    assert snapshot.historical_summary is not None
    assert snapshot.historical_summary.latest_observation_ref == "obs-1"
    assert snapshot.historical_summary.latest_message_ref == ""
    assert snapshot.historical_summary.latest_status_at_receipt == "yellow"
