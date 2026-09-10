"""LN-2 compatibility-writer tests: legacy and canonical state share a transaction."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from freqinout.core.legacy_resource_projection import (
    dedupe_legacy_resources,
    delete_legacy_resources,
    replace_nonstation_set,
    update_legacy_resource_fields,
    upsert_legacy_resource,
)


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE net_resources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        resource_set TEXT NOT NULL, source_type TEXT NOT NULL, source_ref TEXT,
        readonly INTEGER DEFAULT 1, day_utc TEXT NOT NULL, recurrence TEXT DEFAULT 'Weekly',
        biweekly_offset_weeks INTEGER DEFAULT 0, month_weeks TEXT, group_name TEXT,
        band TEXT NOT NULL, mode TEXT NOT NULL, frequency TEXT NOT NULL,
        start_utc TEXT NOT NULL, end_utc TEXT NOT NULL, early_checkin INTEGER NOT NULL,
        primary_js8call_group TEXT, coverage TEXT, comment TEXT, net_name TEXT,
        fldigi_mode TEXT, fldigi_offset TEXT, updated_utc TEXT)"""
    )
    return conn


def _row(**overrides: object) -> dict[str, object]:
    result: dict[str, object] = {
        "day_utc": "Monday", "recurrence": "Weekly", "biweekly_offset_weeks": 0,
        "month_weeks": "", "group_name": "County ARES", "band": "2M", "mode": "FM",
        "frequency": "146.520", "start_utc": "19:00", "end_utc": "20:00", "early_checkin": 10,
        "primary_js8call_group": "@COUNTY", "coverage": "County", "comment": "Bring traffic",
        "net_name": "County Net", "fldigi_mode": "", "fldigi_offset": "",
    }
    result.update(overrides)
    return result


def _mapping(conn: sqlite3.Connection, resource_id: int) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM legacy_net_resource_map WHERE legacy_table_name='net_resources' AND legacy_resource_id=?",
        (str(resource_id),),
    ).fetchone()
    assert row is not None
    return row


def test_upsert_update_and_delete_commit_or_rollback_legacy_and_canonical_together(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "nets.db")
    try:
        with conn:
            resource_id = upsert_legacy_resource(
                conn, _row(), resource_set="station", source_type="manual", source_ref="operator", readonly=0
            )
        mapping = _mapping(conn, resource_id)
        frequency_key = mapping["frequency_resource_key"]
        assert conn.execute("SELECT frequency FROM net_resources WHERE id=?", (resource_id,)).fetchone()[0] == "146.520"
        assert conn.execute("SELECT center_hz FROM frequency_resources WHERE frequency_resource_key=?", (frequency_key,)).fetchone()[0] == 146_520_000

        conn.execute("BEGIN")
        assert update_legacy_resource_fields(conn, resource_id, {"frequency": "146.580", "coverage": "Changed"})
        conn.rollback()
        # One rollback reverts both the compatibility row and its canonical map/resource.
        assert tuple(conn.execute("SELECT frequency,coverage FROM net_resources WHERE id=?", (resource_id,)).fetchone()) == ("146.520", "County")
        assert conn.execute("SELECT center_hz FROM frequency_resources WHERE frequency_resource_key=?", (frequency_key,)).fetchone()[0] == 146_520_000

        with conn:
            assert update_legacy_resource_fields(conn, resource_id, {"frequency": "146.580"})
        assert conn.execute("SELECT center_hz FROM frequency_resources WHERE frequency_resource_key=?", (frequency_key,)).fetchone()[0] == 146_580_000
        with conn:
            assert delete_legacy_resources(conn, [resource_id]) == 1
        assert conn.execute("SELECT COUNT(*) FROM net_resources").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM legacy_net_resource_map WHERE legacy_table_name='net_resources'").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM frequency_resources WHERE frequency_resource_key=?", (frequency_key,)).fetchone()[0] == 0
    finally:
        conn.close()


def test_replace_preserves_station_rows_and_source_fields_are_lossless(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "nets.db")
    try:
        with conn:
            station_id = upsert_legacy_resource(conn, _row(net_name="Station Net"), resource_set="shared", source_type="manual", source_ref="mine", readonly=0)
            old_id = upsert_legacy_resource(conn, _row(net_name="Old Import"), resource_set="shared", source_type="imported", source_ref="old.json")
            inserted, removed = replace_nonstation_set(
                conn, "shared", [_row(net_name="New Import", comment="source comment", coverage="Regional")],
                source_type="imported", source_ref="new.json",
            )
        assert (inserted, removed) == (1, 1)
        rows = conn.execute("SELECT id,net_name,source_type,source_ref,readonly FROM net_resources ORDER BY id").fetchall()
        assert (station_id, "Station Net", "manual", "mine", 0) in {tuple(row) for row in rows}
        assert all(row[0] != old_id for row in rows)
        replacement_id = next(row[0] for row in rows if row[1] == "New Import")
        audit = json.loads(_mapping(conn, replacement_id)["diagnostics_json"])["legacy_row"]
        assert audit["source_type"] == "imported"
        assert audit["source_ref"] == "new.json"
        assert audit["comment"] == "source comment"
        assert audit["coverage"] == "Regional"
        authority = conn.execute(
            "SELECT authority_state FROM resource_catalog_migration_state WHERE state_key='resource_catalog'"
        ).fetchone()[0]
        assert authority == "canonical"
    finally:
        conn.close()


def test_dedupe_removes_only_duplicate_and_reconciles_canonical_mapping(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "nets.db")
    try:
        with conn:
            first = upsert_legacy_resource(conn, _row(), resource_set="station", source_type="manual", source_ref="operator", readonly=0)
            second = upsert_legacy_resource(conn, _row(net_name="Temporary Net"), resource_set="station", source_type="manual", source_ref="operator", readonly=0)
            unique = upsert_legacy_resource(conn, _row(net_name="Different Net"), resource_set="station", source_type="manual", source_ref="operator", readonly=0)
            assert update_legacy_resource_fields(conn, second, {"net_name": "County Net"})
            assert dedupe_legacy_resources(conn) == 1
        remaining = {row[0] for row in conn.execute("SELECT id FROM net_resources")}
        assert unique in remaining and len(remaining) == 2
        assert len(remaining.intersection({first, second})) == 1
        mapped = {int(row[0]) for row in conn.execute("SELECT legacy_resource_id FROM legacy_net_resource_map WHERE legacy_table_name='net_resources'")}
        assert mapped == remaining
    finally:
        conn.close()
