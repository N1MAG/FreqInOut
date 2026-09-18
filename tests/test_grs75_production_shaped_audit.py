"""Read-only production-shaped checks for the GRS-7.5 qualification gate.

The supplied production copies are evidence only.  The fixture is opened with
SQLite's immutable URI, projects only radio/software inventory rows, and never
reads message or operational-history tables.  CI and other stations can point
the test at an equivalent copy with ``FIO_PRODUCTION_DB_ROOT``.
"""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3

import pytest

from freqinout.core.guided_instance_inventory import build_guided_instance_inventory


_DEFAULT_ROOT = Path("/Users/bill/RadioTools/FIO_DB_prod/current")
_PRODUCTION_ROOT = Path(os.environ.get("FIO_PRODUCTION_DB_ROOT", str(_DEFAULT_ROOT)))
_DB_PATH = _PRODUCTION_ROOT / "freqinout.db"


def _open_immutable() -> sqlite3.Connection:
    if not _DB_PATH.is_file():
        pytest.skip(f"production-shaped database not present: {_DB_PATH}")
    return sqlite3.connect(f"file:{_DB_PATH}?mode=ro&immutable=1", uri=True)


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}


def _rows(connection: sqlite3.Connection, table: str, columns: tuple[str, ...]) -> list[dict[str, object]]:
    quoted = ", ".join(f'"{column}"' for column in columns)
    return [
        dict(zip(columns, row))
        for row in connection.execute(f'SELECT {quoted} FROM "{table}" ORDER BY 1')
    ]


def test_production_copy_is_immutable_and_has_the_expected_radio_inventory_shape() -> None:
    """The actual station copy can be inspected without changing it."""

    if not _DB_PATH.is_file():
        pytest.skip(f"production-shaped database not present: {_DB_PATH}")
    before = (_DB_PATH.stat().st_size, _DB_PATH.stat().st_mtime_ns)
    connection = _open_immutable()
    try:
        connection.execute("PRAGMA query_only=ON")
        assert connection.execute("PRAGMA query_only").fetchone() == (1,)
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        required = {
            "device_profiles",
            "js8_instances",
            "fast_light_configs",
            "varac_nodes",
            "varac_clusters",
            "varac_cluster_members",
            "software_instance_manifests",
        }
        assert required <= tables

        assert connection.execute("SELECT COUNT(*) FROM device_profiles").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM js8_instances").fetchone()[0] == 8
        assert connection.execute("SELECT COUNT(*) FROM fast_light_configs").fetchone()[0] == 8
        assert connection.execute("SELECT COUNT(*) FROM varac_nodes").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM varac_clusters").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM varac_cluster_members").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM software_instance_manifests").fetchone()[0] == 0

        # The production copy is an older schema shape: node-local VarAC
        # outbox and JS8 UDP are projected elsewhere.  The guided inventory
        # must remain compatible without treating those missing columns as a
        # reason to read operational history or mutate the schema.
        assert "incoming_path" in _columns(connection, "varac_nodes")
        assert "outbox_path" not in _columns(connection, "varac_nodes")
        assert "udp_port" not in _columns(connection, "js8_instances")
    finally:
        connection.close()
    after = (_DB_PATH.stat().st_size, _DB_PATH.stat().st_mtime_ns)
    assert after == before


def test_production_inventory_projection_is_bounded_and_classifies_legacy_rows() -> None:
    """Only saved configuration/link evidence drives classification."""

    connection = _open_immutable()
    traces: list[str] = []
    connection.set_trace_callback(traces.append)
    before = (_DB_PATH.stat().st_size, _DB_PATH.stat().st_mtime_ns)
    try:
        js8_rows = _rows(
            connection,
            "js8_instances",
            (
                "id", "system_key", "name", "host", "port", "rig_name",
                "install_path", "profile_path", "application_data_root",
                "launch_cmd",
            ),
        )
        fast_rows = _rows(
            connection,
            "fast_light_configs",
            (
                "id", "system_key", "name", "flrig_host", "flrig_port",
                "flrig_path", "fldigi_host", "fldigi_port", "fldigi_path",
                "fldigi_log_path", "fldigi_checkin_dir",
            ),
        )
        varac_rows = _rows(
            connection,
            "varac_nodes",
            (
                "id", "system_key", "name", "install_path", "ini_path",
                "db_path", "incoming_path", "launch_cmd",
            ),
        )
        links = connection.execute(
            "SELECT js8_instance_id, fast_light_config_id, varac_node_id "
            "FROM device_profiles"
        ).fetchone()
        assert links == (1, 1, 1)

        snapshot = build_guided_instance_inventory(
            {
                "js8call": js8_rows,
                "fast_light": fast_rows,
                "varac": varac_rows,
            },
            linked_ids_by_family={
                "js8call": {int(links[0])},
                "fast_light": {int(links[1])},
                "varac": {int(links[2])},
            },
            generation=75,
        )

        assert len(snapshot.usable_rows_for("js8call")) == 1
        assert len(snapshot.diagnostic_rows_for("js8call")) == 7
        assert len(snapshot.usable_rows_for("fast_light")) == 1
        assert len(snapshot.diagnostic_rows_for("fast_light")) == 7
        assert len(snapshot.usable_rows_for("varac")) == 1
        assert len(snapshot.diagnostic_rows_for("varac")) == 0
        assert snapshot.recovery_rows_for("js8call") == ()
        assert snapshot.recovery_rows_for("fast_light") == ()
        assert snapshot.recovery_rows_for("varac") == ()

        js8_duplicates = snapshot.duplicate_resource_claims_for("js8call")
        fast_duplicates = snapshot.duplicate_resource_claims_for("fast_light")
        assert ("tcp", "127.0.0.1:2442", 8) in js8_duplicates
        assert ("tcp", "127.0.0.1:12345", 8) in fast_duplicates
        assert ("tcp", "127.0.0.1:7362", 8) in fast_duplicates

        # This audit's SQL is deliberately limited to the radio-owned
        # configuration/link tables.  Message, ingest, sync, and projection
        # history are not an ownership source for guided setup.
        forbidden = (
            "message_", "js8_messages", "varac_messages", "varac_sync", "ingest",
            "traffic", "observation",
        )
        assert not any(
            any(token in statement.casefold() for token in forbidden)
            for statement in traces
        )
    finally:
        connection.close()
    assert (_DB_PATH.stat().st_size, _DB_PATH.stat().st_mtime_ns) == before
