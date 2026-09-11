"""Regression tests for bounded, idempotent startup data repairs."""

from __future__ import annotations

import sqlite3
from inspect import getsource

from freqinout.core.db_initializer import _repair_group_column, _repair_sitrep_commstat_groups
from freqinout.core import checkins_db, local_ops_store


def test_group_repair_selects_and_updates_only_noncanonical_rows() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE commstat_artifacts(report_group TEXT)")
    conn.executemany(
        "INSERT INTO commstat_artifacts(report_group) VALUES (?)",
        [("MAGNET",), ("MR08",), (" @magnet ",), ("mr08",), ("",)],
    )
    traces: list[str] = []
    conn.set_trace_callback(traces.append)

    changed = _repair_group_column(conn, "commstat_artifacts", "report_group")

    assert changed == 2
    assert [row[0] for row in conn.execute("SELECT report_group FROM commstat_artifacts")] == [
        "MAGNET",
        "MR08",
        "MAGNET",
        "MR08",
        "",
    ]
    repair_select = next(statement for statement in traces if statement.startswith("SELECT rowid"))
    assert "WHERE COALESCE" in repair_select


def test_canonical_startup_repair_does_not_rebuild_sitrep_rollups(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE sitrep_latest_by_callsign(latest_report_group TEXT)")
    conn.execute("CREATE TABLE sitrep_state_rollup(dummy TEXT)")
    conn.execute("INSERT INTO sitrep_latest_by_callsign VALUES ('MAGNET')")

    from freqinout.core import sitrep_fusion

    monkeypatch.setattr(
        sitrep_fusion,
        "_refresh_state_rollups",
        lambda _conn: (_ for _ in ()).throw(AssertionError("canonical data triggered full rollup rebuild")),
    )

    _repair_sitrep_commstat_groups(conn)


def test_operator_lookup_paths_are_read_only_and_do_not_assure_schema() -> None:
    shared_source = getsource(checkins_db.get_all_operators)
    local_source = getsource(local_ops_store.get_all_operators)

    assert "connect_sqlite_readonly" in shared_source
    assert "_ensure_table" not in shared_source
    assert "ensure_tables" not in local_source
    assert "connect_sqlite_readonly" in local_source
