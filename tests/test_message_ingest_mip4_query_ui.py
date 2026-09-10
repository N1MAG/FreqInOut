"""MIP-4 bounded projection query/model and UI dependency contracts.

These tests use temporary SQLite databases only.  They intentionally exercise
the read boundary independently from native source tables and filesystem
scans: opening a view must be a bounded indexed read of the projection, not a
repair or projection operation.
"""

from __future__ import annotations

import ast
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from freqinout.core import message_projection_store as projection_store
from freqinout.core.message_projection_store import ensure_message_projection_schema
from freqinout.core.message_projection_writer import (
    ProjectionBundle,
    ProjectionBundleWriter,
    close_projection_writers,
)
from freqinout.core.message_projection_store import (
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
)


def _schema(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()


def _insert_messages(db_path: Path, count: int) -> None:
    conn = sqlite3.connect(db_path)
    try:
        stamp = "2026-09-10T12:00:00Z"
        conn.executemany(
            """
            INSERT INTO message_projection (
                message_id, canonical_key, content_hash, primary_source_id,
                source_family, message_type, display_type, status,
                read_state, from_call, to_call, group_name, event_ts,
                received_ts, event_utc, received_utc, summary, body_preview,
                topics_json, entities_json, search_text, projected_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    f"mip4-{index}",
                    f"mip4:key:{index}",
                    f"hash-{index}",
                    "mip4-source",
                    "js8",
                    "MSG",
                    "MSG",
                    "INFO",
                    "unread",
                    f"CALL{index:03d}",
                    "N1MAG",
                    "MAGNET",
                    float(index),
                    float(index),
                    stamp,
                    stamp,
                    f"Message {index}",
                    f"Body {index}",
                    "[]",
                    "{}",
                    f"message {index} body {index}",
                    stamp,
                )
                for index in range(count)
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _bundle(index: int = 1) -> ProjectionBundle:
    source = MessageSourceRecord(
        source_id="mip4-source",
        source_family="js8",
        source_label="JS8Call",
        endpoint_or_path="test",
        provenance={"test": True},
    )
    message = MessageProjectionRecord(
        message_id=f"mip4-message-{index}",
        canonical_key=f"mip4:message:{index}",
        content_hash=content_hash("mip4", index),
        primary_source_id=source.source_id,
        source_family="js8",
        source_label="JS8Call",
        message_type="MSG",
        display_type="MSG",
        status="INFO",
        read_state="unread",
        from_call="CALL",
        to_call="N1MAG",
        group_name="MAGNET",
        event_ts=float(index),
        received_ts=float(index),
        summary=f"Message {index}",
        body_preview="body",
        search_text=f"message {index} body",
    )
    return ProjectionBundle(source=source, message=message)


def test_projected_page_is_hard_capped_at_200_rows(tmp_path: Path) -> None:
    """A caller cannot accidentally make a 1,500-row widget model."""

    db_path = tmp_path / "projection.sqlite"
    _schema(db_path)
    _insert_messages(db_path, 250)

    rows = projection_store.list_projected_messages(db_path, limit=1500)

    assert len(rows) <= 200
    assert len(rows) == 200


def test_projection_reads_do_not_run_schema_repair_or_migration(monkeypatch, tmp_path: Path) -> None:
    """List/detail/reference reads must be schema-free after startup migration."""

    db_path = tmp_path / "projection.sqlite"
    _schema(db_path)
    _insert_messages(db_path, 1)

    def fail_schema_repair(_conn):
        raise AssertionError("read-side schema assurance is forbidden")

    monkeypatch.setattr(projection_store, "ensure_message_projection_schema", fail_schema_repair)

    rows = projection_store.list_projected_messages(db_path, limit=200)
    assert len(rows) == 1
    detail = projection_store.load_projected_message_detail(db_path, "mip4-0")
    assert detail["message"] is not None
    refs = projection_store.load_projected_external_refs_for_messages(db_path, ["mip4-0"])
    assert refs == {"mip4-0": []}


def test_projection_generation_advances_only_for_committed_changes(tmp_path: Path) -> None:
    """Generation is a cheap invalidation token, not a per-row render signal."""

    db_path = tmp_path / "projection.sqlite"
    _schema(db_path)
    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        before = sqlite3.connect(db_path).execute(
            "SELECT generation FROM message_projection_generation WHERE singleton=1"
        ).fetchone()[0]
        changed = writer.write_batch((_bundle(1),))
        assert changed.committed_bundles == 1
        after_change = sqlite3.connect(db_path).execute(
            "SELECT generation FROM message_projection_generation WHERE singleton=1"
        ).fetchone()[0]
        assert after_change == before + 1

        unchanged = writer.write_batch((_bundle(1),))
        assert unchanged.message_upserts == 0
        after_noop = sqlite3.connect(db_path).execute(
            "SELECT generation FROM message_projection_generation WHERE singleton=1"
        ).fetchone()[0]
        assert after_noop == after_change
    finally:
        writer.close()
        close_projection_writers()


def test_projected_filter_query_does_not_scan_native_source_directories(monkeypatch, tmp_path: Path) -> None:
    """Changing a filter is an indexed projection query, never source discovery."""

    db_path = tmp_path / "projection.sqlite"
    _schema(db_path)
    _insert_messages(db_path, 3)
    source_root = tmp_path / "native-source"
    source_root.mkdir()

    def fail_directory_scan(*_args, **_kwargs):
        raise AssertionError("native source directory scan during filter query")

    monkeypatch.setattr(Path, "rglob", fail_directory_scan)
    monkeypatch.setattr(Path, "glob", fail_directory_scan)
    rows = projection_store.list_projected_messages(
        db_path,
        search_text="body 2",
        source_families=("js8",),
        limit=200,
    )
    assert [row["message_id"] for row in rows] == ["mip4-2"]


def _function_source(path: Path, name: str) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return ast.get_source_segment(path.read_text(encoding="utf-8"), node) or ""
    raise AssertionError(f"{name} not found in {path}")


def test_station_control_bar_refresh_is_cache_only() -> None:
    """Station Control Bar repaint cannot perform endpoint or database I/O."""

    path = Path(__file__).parents[1] / "freqinout" / "gui" / "main_window.py"
    source = _function_source(path, "_refresh_station_command_bar")
    forbidden = (
        "connect_sqlite",
        "sqlite3.connect",
        "subprocess",
        "socket",
        "requests.",
        "process.",
        "probe",
        "read_frequency",
    )
    offenders = [token for token in forbidden if token in source]
    assert offenders == [], f"cache-only control-bar refresh performs I/O: {offenders}"


def test_settings_save_refreshes_only_relevant_active_consumers() -> None:
    """Inactive Settings-save consumers stay lazy; active SOP gets a bounded refresh."""

    path = Path(__file__).parents[1] / "freqinout" / "gui" / "main_window.py"
    source = _function_source(path, "_flush_settings_saved_for_lazy_tabs")
    # The implementation may use a helper, but the callback must make an
    # active/visible decision rather than unconditionally refreshing every tab.
    assert any(token in source for token in ("isVisible", "_active", "currentWidget", "_ui_refresh_allowed"))
    assert "settings_saved.message_viewer" in source
    assert "settings_saved.controlfreq" in source


def test_message_model_has_bounded_page_and_generation_update_seam() -> None:
    """The UI model is bounded and supports invalidation without reconstruction."""

    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.message_viewer_tab import MessageTableModel, UnifiedMessage

    app = QApplication.instance() or QApplication([])
    del app
    rows = [
        UnifiedMessage(
            msg_type="MSG",
            status="INFO",
            from_call=f"CALL{index}",
            to_call="N1MAG",
            rcv_ts=float(index),
            rcv_display="now",
            title=f"Message {index}",
            origin="js8",
            payload=SimpleNamespace(),
        )
        for index in range(250)
    ]
    model = MessageTableModel(rows)
    assert model.rowCount() <= 200
    assert any(
        hasattr(model, name)
        for name in ("apply_projection_update", "set_projection_rows", "set_rows_for_generation")
    ), "model needs a generation-aware update seam"


def test_projection_refresh_policy_declares_visible_and_hidden_bounds() -> None:
    """Visible refreshes are <=500ms; hidden refreshes coalesce at <=2s."""

    candidates = (
        Path(__file__).parents[1] / "freqinout" / "gui" / "message_viewer_tab.py",
        Path(__file__).parents[1] / "freqinout" / "core" / "message_projection_ui.py",
    )
    source = "\n".join(path.read_text(encoding="utf-8") for path in candidates if path.exists())
    normalized = source.lower().replace(" ", "")
    assert any(token in normalized for token in ("500", "0.5")), "visible refresh bound is not declared"
    assert any(token in normalized for token in ("2000", "2.0", "hidden")), "hidden refresh bound is not declared"
    assert any(token in normalized for token in ("coalesc", "debounc", "invalidation")), "refresh coalescing seam is not declared"
