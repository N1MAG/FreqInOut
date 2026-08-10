from __future__ import annotations

import sqlite3

from freqinout.core.db_initializer import _ensure_js8_expect_tables


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _indexes(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA index_list({table})").fetchall()}


def test_js8_expect_schema_supports_bulk_allow_policies_and_audit() -> None:
    conn = sqlite3.connect(":memory:")
    _ensure_js8_expect_tables(conn)

    policy_cols = _columns(conn, "js8_expect_allow_policies")
    assert {
        "name",
        "allowed_callsigns_json",
        "allowed_groups_json",
        "blocked_callsigns_json",
        "source_scope",
        "source_radio_ids_json",
        "enabled",
        "import_source",
    }.issubset(policy_cols)

    entry_cols = _columns(conn, "js8_expect_entries")
    assert {
        "allow_policy_id",
        "allowed_callsigns_json",
        "allowed_groups_json",
        "allow_any",
        "blocked_callsigns_json",
        "auto_reply_enabled",
        "unattended_auto_reply_enabled",
        "source_scope",
        "js8_instance_id",
    }.issubset(entry_cols)

    audit_cols = _columns(conn, "js8_expect_audit")
    assert {
        "event_id",
        "expect_entry_id",
        "source_radio_id",
        "source_js8_instance_id",
        "requesting_callsign",
        "decision",
        "reply_radio_id",
    }.issubset(audit_cols)

    dispatch_cols = _columns(conn, "js8_expect_dispatch_audit")
    assert {
        "event_id",
        "expect_entry_id",
        "expect_key",
        "source_radio_id",
        "source_js8_instance_id",
        "decision",
        "reason",
        "transmitted_text",
    }.issubset(dispatch_cols)

    assert "idx_js8_expect_allow_policies_name" in _indexes(conn, "js8_expect_allow_policies")
    assert "idx_js8_expect_entries_policy" in _indexes(conn, "js8_expect_entries")
    assert "idx_js8_expect_audit_created_call" in _indexes(conn, "js8_expect_audit")
    assert "idx_js8_expect_dispatch_audit_created" in _indexes(conn, "js8_expect_dispatch_audit")
