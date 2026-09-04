from __future__ import annotations

import sqlite3

import pytest

from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.operator_identity import (
    CallsignConflictError,
    OperatorIdentityError,
    build_operator_alias_index,
    change_operator_callsign,
    get_or_create_operator_identity,
    list_callsign_history,
    resolve_operator_identity,
)


def _seed(conn: sqlite3.Connection, callsign: str, *, name: str = "Operator") -> None:
    ensure_operator_checkins_schema(conn)
    conn.execute(
        """
        INSERT INTO operator_checkins(
            callsign, name, group1, group_role, trusted, first_seen_utc
        ) VALUES (?, ?, 'MAGNET', 'HUB', 1, '20240101')
        """,
        (callsign, name),
    )
    ensure_operator_checkins_schema(conn)


def test_schema_backfills_stable_identity_and_current_alias() -> None:
    conn = sqlite3.connect(":memory:")
    _seed(conn, "K1OLD")

    row = conn.execute(
        "SELECT operator_id, callsign FROM operator_checkins WHERE callsign='K1OLD'"
    ).fetchone()
    assert row and row[0]
    identity = resolve_operator_identity(conn, "K1OLD")
    assert identity is not None
    assert identity.operator_id == row[0]
    assert identity.current_callsign == "K1OLD"


def test_schema_allows_base_and_portable_roster_rows_to_share_identity() -> None:
    conn = sqlite3.connect(":memory:")
    _seed(conn, "K1ABC")
    original = resolve_operator_identity(conn, "K1ABC")
    assert original is not None

    # Reproduce the partially migrated production shape: the exact portable
    # row has no identity yet while its canonical base call already does.
    conn.execute(
        """
        INSERT INTO operator_checkins(
            callsign, name, group1, group_role, trusted, first_seen_utc
        ) VALUES ('K1ABC/P', 'Portable', 'MAGNET', 'PEER', 0, '20240901')
        """
    )
    ensure_operator_checkins_schema(conn)

    rows = conn.execute(
        "SELECT callsign, operator_id FROM operator_checkins ORDER BY callsign"
    ).fetchall()
    assert rows == [("K1ABC", original.operator_id), ("K1ABC/P", original.operator_id)]
    index_row = next(
        row for row in conn.execute("PRAGMA index_list(operator_checkins)").fetchall()
        if row[1] == "idx_operator_checkins_identity"
    )
    assert index_row[2] == 0  # non-unique compatibility lookup index


def test_change_callsign_preserves_variant_roster_rows() -> None:
    conn = sqlite3.connect(":memory:")
    _seed(conn, "K1ABC")
    conn.execute(
        """
        INSERT INTO operator_checkins(
            callsign, name, group1, group_role, trusted, first_seen_utc
        ) VALUES ('K1ABC/P', 'Portable', 'MAGNET', 'PEER', 0, '20240901')
        """
    )
    ensure_operator_checkins_schema(conn)

    change_operator_callsign(conn, "K1ABC", "K1NEW", effective_at=1_725_494_400.0)

    rows = conn.execute(
        "SELECT callsign FROM operator_checkins ORDER BY callsign"
    ).fetchall()
    assert rows == [("K1ABC/P",), ("K1NEW",)]


def test_change_callsign_preserves_operator_row_and_alias_history() -> None:
    conn = sqlite3.connect(":memory:")
    _seed(conn, "K1OLD", name="Casey")
    original = resolve_operator_identity(conn, "K1OLD")
    assert original is not None

    changed = change_operator_callsign(
        conn,
        "K1OLD",
        "K1NEW",
        effective_at=1_725_494_400.0,
        note="FCC change",
    )

    assert changed.operator_id == original.operator_id
    row = conn.execute(
        "SELECT callsign, name, group1, group_role, trusted, operator_id FROM operator_checkins"
    ).fetchone()
    assert row == ("K1NEW", "Casey", "MAGNET", "HUB", 1, original.operator_id)
    assert resolve_operator_identity(conn, "K1OLD").operator_id == original.operator_id
    assert resolve_operator_identity(conn, "K1NEW").operator_id == original.operator_id
    history = list_callsign_history(conn, original.operator_id)
    assert [entry.callsign for entry in history] == ["K1NEW", "K1OLD"]
    assert history[0].effective_to is None
    assert history[1].effective_to == 1_725_494_400.0
    audit = conn.execute(
        "SELECT action, old_callsign, new_callsign FROM operator_identity_audit"
    ).fetchone()
    assert audit == ("change_callsign", "K1OLD", "K1NEW")


def test_alias_index_maps_former_and_current_callsign_to_one_operator() -> None:
    conn = sqlite3.connect(":memory:")
    _seed(conn, "K1OLD")
    changed = change_operator_callsign(conn, "K1OLD", "K1NEW", effective_at=1_725_494_400.0)

    aliases = build_operator_alias_index(conn)
    assert aliases["K1OLD"].operator_id == changed.operator_id
    assert aliases["K1NEW"].operator_id == changed.operator_id
    assert aliases["K1OLD"].current_callsign == "K1NEW"


def test_change_callsign_rejects_existing_operator_and_rolls_back() -> None:
    conn = sqlite3.connect(":memory:")
    _seed(conn, "K1OLD")
    _seed(conn, "K2USED")

    with pytest.raises(CallsignConflictError):
        change_operator_callsign(conn, "K1OLD", "K2USED", effective_at=1_725_494_400.0)

    assert conn.execute(
        "SELECT COUNT(*) FROM operator_checkins WHERE callsign IN ('K1OLD','K2USED')"
    ).fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM operator_identity_audit").fetchone()[0] == 0


def test_change_callsign_rejects_invalid_or_unchanged_call() -> None:
    conn = sqlite3.connect(":memory:")
    _seed(conn, "K1OLD")
    with pytest.raises(OperatorIdentityError):
        change_operator_callsign(conn, "K1OLD", "not a call")
    with pytest.raises(OperatorIdentityError):
        change_operator_callsign(conn, "K1OLD", "K1OLD")


def test_delayed_former_callsign_resolves_by_evidence_time_without_merging_reuse() -> None:
    conn = sqlite3.connect(":memory:")
    _seed(conn, "K1OLD")
    changed = change_operator_callsign(conn, "K1OLD", "K1NEW", effective_at=1_725_494_400.0)

    delayed = get_or_create_operator_identity(conn, "K1OLD", at_utc=1_725_000_000.0)
    reused = get_or_create_operator_identity(conn, "K1OLD", at_utc=1_726_000_000.0)

    assert delayed.operator_id == changed.operator_id
    assert delayed.current_callsign == "K1NEW"
    assert reused.operator_id != changed.operator_id
    assert reused.current_callsign == "K1OLD"
