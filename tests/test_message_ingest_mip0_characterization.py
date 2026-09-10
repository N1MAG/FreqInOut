"""MIP-0 characterization tests for the pre-optimization message pipeline.

These tests deliberately describe the current implementation.  They provide a
small, deterministic reproduction of the production failure shapes so the
later MIP packages can replace the behavior with bounded, incremental work
without losing a regression baseline.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest
from PySide6.QtCore import QCoreApplication, QTimer

from freqinout.core import message_projection_store, message_source_projectors
from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.message_source_projectors import project_native_file_records
from freqinout.core.multi_radio_store import (
    MultiRadioStore,
    set_multi_rig_migration_version,
)


def _create_js8_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE js8_messages (
            id INTEGER PRIMARY KEY,
            from_call TEXT, to_call TEXT, msg_type TEXT,
            utc_str TEXT, utc_ts REAL, raw_text TEXT, decoded_text TEXT,
            state TEXT, read_ts REAL, flag_state INTEGER DEFAULT 0,
            source_key TEXT, source_id INTEGER, source_radio_id TEXT,
            js8_instance_id TEXT, source_path TEXT
        )
        """
    )


def _insert_js8_rows(conn: sqlite3.Connection, count: int, *, start_id: int = 1) -> None:
    rows = []
    for row_id in range(start_id, start_id + count):
        rows.append(
            (
                row_id,
                f"N{row_id % 90 + 1:02d}AAA",
                "@MR08",
                "MSG",
                "2026-09-10 10:00:00",
                1_789_000_000.0 + row_id,
                f"message-{row_id}",
                f"message-{row_id}",
                "UNREAD",
                0.0,
                0,
                "radio-a",
                row_id,
                "radio-a",
                "js8-a",
                "/tmp/js8.db",
            )
        )
    conn.executemany(
        """
        INSERT INTO js8_messages
            (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text,
             decoded_text, state, read_ts, flag_state, source_key, source_id,
             source_radio_id, js8_instance_id, source_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def test_mip0_production_shaped_fixture_has_expected_source_and_file_counts(tmp_path: Path) -> None:
    """Keep a cheap source-volume fixture matching the Linux production shape."""

    db_path = tmp_path / "source-shape.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        # The characterization fixture intentionally stores only the common
        # source identity columns.  MIP adapters may add richer schemas later;
        # the volume contract is what matters at this stage.
        for table in ("commstat_artifacts", "sitrep_events", "spotter_traffic", "varac_messages"):
            conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
        _create_js8_table(conn)
        for table, count in (
            ("commstat_artifacts", 5_000),
            ("sitrep_events", 5_000),
            ("spotter_traffic", 1_000),
            ("varac_messages", 100),
        ):
            conn.executemany(
                f"INSERT INTO {table}(id, payload) VALUES (?, ?)",
                ((idx, f"fixture-{table}-{idx}") for idx in range(1, count + 1)),
            )
        # Populate JS8 with its actual shape after the generic source count
        # rows above have been created for the other source tables.
        _insert_js8_rows(conn, 100)
        conn.commit()
    finally:
        conn.close()

    records = {
        "flmsg": [
            FileRecord(
                path=tmp_path / "flmsg" / f"message-{idx:04d}.k2s",
                origin="flmsg",
                size=256,
                mtime=float(idx),
                source_id="flmsg:radio-a",
            )
            for idx in range(551)
        ]
    }
    conn = sqlite3.connect(db_path)
    try:
        counts = {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("commstat_artifacts", "sitrep_events", "spotter_traffic", "js8_messages", "varac_messages")
        }
    finally:
        conn.close()

    assert counts == {
        "commstat_artifacts": 5_000,
        "sitrep_events": 5_000,
        "spotter_traffic": 1_000,
        "js8_messages": 100,
        "varac_messages": 100,
    }
    assert len(records["flmsg"]) == 551


def test_mip0_changed_source_replays_the_current_five_thousand_row_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A changed source fingerprint currently reprojects the whole bounded window."""

    db_path = tmp_path / "replay.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_js8_table(conn)
        _insert_js8_rows(conn, 5_000)
        conn.commit()
    finally:
        conn.close()

    bundles: list[object] = []
    monkeypatch.setattr(
        message_source_projectors,
        "_upsert_bundle",
        lambda _conn, _source, message, _ref, **_kwargs: bundles.append(message),
    )
    first = message_source_projectors.project_native_message_sources(
        db_path, sources=("js8",), limit=5_000, force=False
    )
    assert first == {"js8": 5_000}
    assert len(bundles) == 5_000

    conn = sqlite3.connect(db_path)
    try:
        _insert_js8_rows(conn, 1, start_id=5_001)
        conn.commit()
    finally:
        conn.close()
    second = message_source_projectors.project_native_message_sources(
        db_path, sources=("js8",), limit=5_000, force=False
    )

    assert second == {"js8": 5_000}
    assert len(bundles) == 10_000


def test_mip1_bundle_helpers_use_only_the_outer_schema_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The compatibility projector assures schema once, never per row helper."""

    db_path = tmp_path / "schema-amplification.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_js8_table(conn)
        _insert_js8_rows(conn, 1)
        conn.commit()
    finally:
        conn.close()

    original = message_projection_store.ensure_message_projection_schema
    calls = 0

    def counted(connection: sqlite3.Connection) -> None:
        nonlocal calls
        calls += 1
        original(connection)

    monkeypatch.setattr(message_projection_store, "ensure_message_projection_schema", counted)
    monkeypatch.setattr(message_source_projectors, "ensure_message_projection_schema", counted)
    result = message_source_projectors.project_native_message_sources(
        db_path, sources=("js8",), limit=1, force=True
    )

    assert result == {"js8": 1}
    assert calls == 1


def test_mip0_file_scan_completion_regression_now_preserves_ui_heartbeat() -> None:
    """MIP-3 replaces the characterized foreground work with a state-only callback."""

    from freqinout.gui.message_viewer_tab import MessageViewerTab

    app = QCoreApplication.instance() or QCoreApplication([])
    heartbeat: list[str] = []
    QTimer.singleShot(0, lambda: heartbeat.append("heartbeat"))

    class _Settings:
        def get(self, _key: object, default: object = None) -> object:
            return default

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._refresh_files_inflight = True
    tab._file_scan_start_ts = time.time()
    tab._files_snapshot_fp = None
    tab._projection_primary_enabled = False
    tab._last_projection_render_ts = 0.0
    tab.settings = _Settings()
    tab.files = []
    tab._scan_cache_loaded = False
    calls: list[tuple[str, int]] = []
    ui_thread = threading.get_ident()

    def mark(name: str):
        def _marked(*_args, **_kwargs):
            calls.append((name, threading.get_ident()))

        return _marked

    tab._emit_message_refresh_busy = mark("busy")
    tab._files_records_fingerprint = lambda _records: "new-fingerprint"
    tab._update_fldigi_senders = mark("fldigi")
    tab._load_read_state_map = lambda: {}
    tab._save_file_scan_cache = mark("cache")
    tab._apply_bbs_sweeper_rules_after_file_scan = mark("bbs")
    tab._project_message_files_to_observations = mark("observations")
    tab._start_native_file_projection_write = mark("projection")
    tab._refresh_varac_messages = mark("varac")
    tab._populate_messages_table = mark("table")
    tab._start_signature_verification = mark("signatures")

    payload = {"records": {"flmsg": [], "flamp": [], "varac": [], "bbs": []}, "dir_mtimes": {}}
    MessageViewerTab._on_file_scan_finished(tab, payload, False)

    assert heartbeat == []
    assert {name for name, _thread in calls} == {"busy"}
    assert all(thread_id == ui_thread for _name, thread_id in calls)
    app.processEvents()
    assert heartbeat == ["heartbeat"]


def test_mip0_projection_reports_a_deterministic_sqlite_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The current monolithic projector surfaces a writer lock to its caller."""

    db_path = tmp_path / "locked.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_js8_table(conn)
        _insert_js8_rows(conn, 1)
        conn.commit()
    finally:
        conn.close()
    # Initialize the projection tables before taking the lock.
    message_source_projectors.project_native_message_sources(db_path, sources=("js8",), limit=1)

    lock_conn = sqlite3.connect(db_path, timeout=0.01)
    lock_conn.execute("BEGIN EXCLUSIVE")
    original_connect = message_source_projectors.connect_sqlite

    def short_connect(path, **kwargs):
        kwargs["timeout"] = 0.1
        kwargs["busy_timeout_ms"] = 50
        return original_connect(path, **kwargs)

    monkeypatch.setattr(message_source_projectors, "connect_sqlite", short_connect)
    try:
        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            message_source_projectors.project_native_message_sources(
                db_path, sources=("js8",), limit=1, force=True
            )
        assert time.monotonic() - started < 1.0
    finally:
        lock_conn.rollback()
        lock_conn.close()


def test_mip0_projection_rollback_is_restart_recoverable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failure during a source window rolls back and can be replayed after restart."""

    db_path = tmp_path / "restart.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_js8_table(conn)
        _insert_js8_rows(conn, 2)
        conn.commit()
    finally:
        conn.close()

    original_bundle = message_source_projectors._upsert_bundle
    bundle_calls = 0

    def fail_second(connection, source, message, ref, **kwargs):
        nonlocal bundle_calls
        bundle_calls += 1
        original_bundle(connection, source, message, ref, **kwargs)
        if bundle_calls == 2:
            raise RuntimeError("characterization crash")

    monkeypatch.setattr(message_source_projectors, "_upsert_bundle", fail_second)
    with pytest.raises(RuntimeError, match="characterization crash"):
        message_source_projectors.project_native_message_sources(db_path, sources=("js8",), limit=2, force=True)

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM message_projection_checkpoint").fetchone()[0] == 0
    finally:
        conn.close()

    monkeypatch.setattr(message_source_projectors, "_upsert_bundle", original_bundle)
    result = message_source_projectors.project_native_message_sources(
        db_path, sources=("js8",), limit=2, force=False
    )
    assert result == {"js8": 2}


def test_mip0_invalid_surrogate_file_path_is_now_safely_projected(tmp_path: Path) -> None:
    """MIP-3 retains malformed path identity without passing surrogates to SQLite."""

    record = FileRecord(
        path=tmp_path / "malformed-\udcf4.k2s",
        origin="flamp",
        size=1,
        mtime=1.0,
        source_id="flamp:radio-a",
    )
    db_path = tmp_path / "malformed.sqlite"
    assert project_native_file_records(db_path, {"flamp": [record]}, force=True) == 1
    conn = sqlite3.connect(db_path)
    try:
        path_text = conn.execute("SELECT external_path FROM message_external_refs").fetchone()[0]
        assert "\udcf4" not in path_text
        assert "\\xf4" in path_text
    finally:
        conn.close()


def test_mip0_profile_list_regression_is_now_read_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """MIP-4 keeps runtime-primary repair out of profile list/get paths."""

    store = MultiRadioStore(tmp_path / "settings.sqlite")
    profile = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    with store.connect() as conn:
        set_multi_rig_migration_version(conn)
        conn.execute(
            "UPDATE device_profiles SET runtime_active=0, runtime_primary=1 WHERE id=?",
            (int(profile["id"]),),
        )
        conn.commit()

    import freqinout.core.multi_radio_store as multi_radio_store

    updates: list[str] = []
    original = multi_radio_store._normalize_runtime_primary_device

    def traced(conn, *args, **kwargs):
        statements: list[str] = []
        conn.set_trace_callback(statements.append)
        try:
            return original(conn, *args, **kwargs)
        finally:
            conn.set_trace_callback(None)
            updates.extend(statement for statement in statements if statement.lstrip().upper().startswith("UPDATE"))

    monkeypatch.setattr(multi_radio_store, "_normalize_runtime_primary_device", traced)
    rows = store.list_device_profiles()
    assert len(rows) == 1
    assert updates == []
    with store.connect() as conn:
        persisted = conn.execute(
            "SELECT runtime_active, runtime_primary FROM device_profiles WHERE id=?",
            (int(profile["id"]),),
        ).fetchone()
    assert tuple(persisted) == (0, 1)
