from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.message_projection_projector import (
    mark_projected_message_rows_deleted,
    project_unified_message_rows,
)
from freqinout.core.message_projection_store import list_projected_messages


def _row(**overrides):
    payload = overrides.pop(
        "payload",
        SimpleNamespace(
            msg_id=123,
            source_key="rig-a",
            source_radio_id="7",
            js8_instance_id="js8-a",
            raw_text="FIO test traffic",
            decoded_text="FIO test traffic decoded",
            flag_state=0,
        ),
    )
    data = {
        "msg_type": "JS8",
        "status": "NEW",
        "from_call": "N1AAA",
        "to_call": "@FIO",
        "rcv_ts": 1_780_000_000.0,
        "rcv_display": "2026-06-01 00:00",
        "title": "Routine JS8 traffic",
        "origin": "js8",
        "payload": payload,
        "search_text": "routine js8 traffic n1aaa",
        "auth_state": "",
        "auth_detail": "",
        "auth_trusted": False,
        "expect_decision": "",
        "expect_detail": "",
        "topics": ("traffic",),
        "actionable": False,
        "display_type": "Message",
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def test_project_unified_message_rows_persists_source_message_and_ref(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    count = project_unified_message_rows(db_path, [_row()])

    assert count == 1
    rows = list_projected_messages(db_path, limit=10)
    assert len(rows) == 1
    assert rows[0]["source_family"] == "js8"
    assert rows[0]["read_state"] == "new"
    assert rows[0]["severity"] == "watch"
    assert rows[0]["operator_attention"] == 0

    conn = sqlite3.connect(db_path)
    try:
        refs = conn.execute("SELECT external_kind, external_key, read_capability FROM message_external_refs").fetchall()
    finally:
        conn.close()
    assert refs == [("js8_message", "123", "js8_mark_read")]


def test_project_unified_message_rows_extracts_flamp_artifact_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    payload = FileRecord(
        path=tmp_path / "QABC123_block_04.k2s",
        mtime=1_780_000_100.0,
        size=2048,
        origin="flamp",
    )

    count = project_unified_message_rows(
        db_path,
        [
            _row(
                msg_type="FLAMP",
                status="READ",
                from_call="N1BBB",
                to_call="@FLAMP",
                title="FLAMP QABC123 transfer",
                origin="flamp",
                payload=payload,
                topics=("artifact", "flamp"),
            )
        ],
    )

    assert count == 1
    rows = list_projected_messages(db_path, limit=10)
    assert len(rows) == 1
    assert rows[0]["source_family"] == "flamp"
    assert rows[0]["read_state"] == "read"

    conn = sqlite3.connect(db_path)
    try:
        artifacts = conn.execute(
            "SELECT artifact_type, q_id, block_id, transfer_state FROM message_artifacts"
        ).fetchall()
    finally:
        conn.close()
    assert artifacts == [("flamp_transfer", "QABC123", "04", "complete")]


def test_project_unified_message_rows_upserts_refreshes_without_duplicates(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    first = _row(title="Initial title")
    second = _row(title="Updated title")

    assert project_unified_message_rows(db_path, [first]) == 1
    assert project_unified_message_rows(db_path, [second]) == 1

    conn = sqlite3.connect(db_path)
    try:
        message_count = conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0]
        ref_count = conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0]
        subject = conn.execute("SELECT subject FROM message_projection").fetchone()[0]
    finally:
        conn.close()
    assert message_count == 1
    assert ref_count == 1
    assert subject == "Updated title"


def test_mark_projected_message_rows_deleted_hides_rows_from_default_projection(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    row = _row()

    assert project_unified_message_rows(db_path, [row]) == 1
    assert mark_projected_message_rows_deleted(db_path, [row], source_scope="test") == 1

    assert list_projected_messages(db_path, limit=10) == []
    deleted_rows = list_projected_messages(db_path, include_deleted=True, limit=10)
    assert len(deleted_rows) == 1
    assert deleted_rows[0]["deleted"] == 1

    conn = sqlite3.connect(db_path)
    try:
        queued = conn.execute(
            "SELECT requested_effect, source_scope, state FROM message_delete_queue"
        ).fetchall()
    finally:
        conn.close()
    assert queued == [("delete", "test", "queued")]


def test_projection_refresh_does_not_resurrect_deleted_row(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    row = _row(title="Before delete")
    refreshed = _row(title="After delete source refresh")

    assert project_unified_message_rows(db_path, [row]) == 1
    assert mark_projected_message_rows_deleted(db_path, [row], source_scope="test") == 1
    assert project_unified_message_rows(db_path, [refreshed]) == 1

    assert list_projected_messages(db_path, limit=10) == []
    deleted_rows = list_projected_messages(db_path, include_deleted=True, limit=10)
    assert len(deleted_rows) == 1
    assert deleted_rows[0]["deleted"] == 1
    assert deleted_rows[0]["subject"] == "After delete source refresh"
