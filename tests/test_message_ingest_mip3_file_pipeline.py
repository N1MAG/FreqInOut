"""MIP-3 file discovery and UI completion-boundary contracts.

These tests deliberately use temporary directories/databases only.  They are
performance-shaped contracts: the unchanged path must reuse the scanner's
cached records, while a changed directory must expose only the small delta
needed by the next projection stage.  Parsing, BBS housekeeping, and UI model
work are not allowed to run from the Qt scan-completion callback.
"""

from __future__ import annotations

import ast
import os
import sqlite3
import time
from pathlib import Path

import pytest

from freqinout.core.message_file_scanner import FileRecord, MessageFileScanner
from freqinout.core.message_projection_store import (
    ExternalMessageRef,
    MessageArtifactRecord,
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
    ensure_message_projection_schema,
)
from freqinout.core.message_projection_writer import ProjectionBundle, ProjectionBundleWriter


_EMPTY_ORIGINS = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}


def _watch(root: Path) -> list[dict[str, str]]:
    return [{"origin": "flamp", "path": str(root), "source_id": "mip3-flamp"}]


def _record_map(records: dict[str, list[FileRecord]]) -> dict[str, tuple[int, float]]:
    return {
        str(rec.path): (int(rec.size), float(rec.mtime))
        for values in records.values()
        for rec in values
    }


def test_unchanged_551_file_scan_reuses_cache_without_file_work(monkeypatch, tmp_path: Path) -> None:
    """An unchanged directory does no per-file stat/read/hash/parse pass."""

    root = tmp_path / "flamp"
    root.mkdir()
    for index in range(551):
        (root / f"report-{index:04d}.txt").write_text(f"report {index}\n", encoding="utf-8")

    first_records, first_mtimes, first_mode = MessageFileScanner(
        _watch(root), force=True
    ).scan()
    assert first_mode == "full"
    assert len(first_records["flamp"]) == 551

    file_paths = {Path(rec.path) for rec in first_records["flamp"]}
    file_stat_calls = 0
    file_read_calls = 0
    real_stat = Path.stat
    real_read_text = Path.read_text

    def counted_stat(path: Path, *args, **kwargs):
        nonlocal file_stat_calls
        if path in file_paths:
            file_stat_calls += 1
        return real_stat(path, *args, **kwargs)

    def counted_read_text(path: Path, *args, **kwargs):
        nonlocal file_read_calls
        if path in file_paths:
            file_read_calls += 1
        return real_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", counted_stat)
    monkeypatch.setattr(Path, "read_text", counted_read_text)

    second_records, second_mtimes, second_mode = MessageFileScanner(
        _watch(root),
        force=False,
        base_records=first_records,
        base_dir_mtimes=first_mtimes,
    ).scan()

    assert second_mode == "incremental"
    assert second_mtimes == first_mtimes
    assert _record_map(second_records) == _record_map(first_records)
    assert file_stat_calls == 0
    assert file_read_calls == 0


def test_changed_file_scan_reports_one_add_change_and_remove_delta(tmp_path: Path) -> None:
    """A changed directory preserves unchanged records and exposes exact deltas."""

    root = tmp_path / "flamp"
    root.mkdir()
    unchanged = root / "unchanged.txt"
    changed = root / "changed.txt"
    removed = root / "removed.txt"
    unchanged.write_text("same", encoding="utf-8")
    changed.write_text("old", encoding="utf-8")
    removed.write_text("gone", encoding="utf-8")

    first_records, first_mtimes, _ = MessageFileScanner(_watch(root), force=True).scan()
    before = _record_map(first_records)
    old_changed = before[str(changed)]

    removed.unlink()
    changed.write_text("changed content with a new size", encoding="utf-8")
    # Make the metadata difference deterministic even on filesystems with a
    # coarse mtime clock.
    os.utime(changed, ns=(time.time_ns(), time.time_ns()))
    added = root / "added.txt"
    added.write_text("new", encoding="utf-8")

    current, _mtimes, mode = MessageFileScanner(
        _watch(root),
        force=False,
        base_records=first_records,
        base_dir_mtimes=first_mtimes,
    ).scan()
    after = _record_map(current)

    assert mode == "incremental"
    assert str(removed) not in after
    assert str(added) in after
    assert after[str(changed)] != old_changed
    assert after[str(unchanged)] == before[str(unchanged)]
    assert set(after) - set(before) == {str(added)}
    assert set(before) - set(after) == {str(removed)}


def test_surrogate_filename_has_stable_safe_display_and_does_not_repeat_failure(tmp_path: Path) -> None:
    """Malformed filesystem bytes remain inspectable without leaking surrogates."""

    # macOS rejects arbitrary non-UTF-8 bytes in a filename at the syscall
    # boundary (unlike Linux).  Constructing the FileRecord with Python's
    # surrogateescape spelling exercises the same presentation boundary on all
    # platforms without making the test host filesystem-dependent.
    record = FileRecord(path=Path("report-\udcff.k2s"), origin="flamp", size=9, mtime=1.0)

    first_name = record.display_name()
    second_name = record.display_name()
    first_info = record.info_line()
    second_info = record.info_line()
    assert first_name == second_name
    assert first_info == second_info
    assert "\udcff" not in first_name
    assert "\udcff" not in first_info
    # The safe representation must retain enough information to distinguish
    # this name from an ordinary replacement-character filename.
    assert "ff" in first_name.lower() or "\\x" in first_name.lower() or "�" in first_name

    direct = FileRecord(path=Path("bad-\udcff.k2s"), origin="flamp")
    assert "\udcff" not in direct.display_name()
    assert "\udcff" not in direct.info_line()


def _file_bundle(tmp_path: Path) -> ProjectionBundle:
    source = MessageSourceRecord(
        source_id="mip3-file-source",
        source_family="flamp",
        source_label="FLAMP",
        endpoint_or_path=str(tmp_path),
        provenance={"source": "file_scan"},
    )
    message = MessageProjectionRecord(
        message_id="mip3-file-message",
        canonical_key="mip3:file:report.k2s",
        content_hash=content_hash("mip3-file", "payload"),
        primary_source_id=source.source_id,
        source_family="flamp",
        source_label="FLAMP",
        message_type="FLAMP",
        display_type="FLAMP",
        status="NEW",
        subject="report.k2s",
        summary="FLAMP report",
        body_preview="payload",
        search_text="flamp report payload",
    )
    ref = ExternalMessageRef(
        message_id=message.message_id,
        source_id=source.source_id,
        external_kind="flamp_file",
        external_key="report.k2s:1:7",
        external_path=str(tmp_path / "report.k2s"),
        external_mtime=1.0,
        external_size=7,
        delete_capability="file_delete",
    )
    artifact = MessageArtifactRecord(
        artifact_id="mip3-file-artifact",
        message_id=message.message_id,
        artifact_type="flamp_transfer",
        source_id=source.source_id,
        external_key=ref.external_key,
        path=ref.external_path,
        content_hash=content_hash("payload"),
        q_id="Q-1A2B",
        transfer_state="seen",
    )
    return ProjectionBundle(source=source, message=message, refs=(ref,), artifacts=(artifact,))


def test_file_projection_writes_reference_and_artifact_atomically(tmp_path: Path) -> None:
    """One serialized bundle commits the message, ref, and artifact together."""

    db_path = tmp_path / "mip3.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()

    writer = ProjectionBundleWriter(db_path, transaction_budget_seconds=2.0)
    try:
        result = writer.write_batch((_file_bundle(tmp_path),))
        assert result.state == "committed"
        assert result.committed_bundles == 1
        assert result.message_upserts == 1
        assert result.ref_upserts == 1
        assert result.artifact_upserts == 1
    finally:
        writer.close()

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM message_artifacts").fetchone()[0] == 1
        row = conn.execute(
            "SELECT q_id, transfer_state FROM message_artifacts"
        ).fetchone()
        assert tuple(row) == ("Q-1A2B", "seen")
    finally:
        conn.close()


def _completion_method_ast() -> ast.FunctionDef:
    source_path = Path(__file__).parents[1] / "freqinout" / "gui" / "message_viewer_tab.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_on_file_scan_finished":
            return node
    raise AssertionError("MessageViewerTab._on_file_scan_finished not found")


_FORBIDDEN_COMPLETION_CALLS = {
    "_save_file_scan_cache",
    "_save_file_scan_cache_meta_only",
    "_apply_bbs_sweeper_rules_after_file_scan",
    "_project_message_files_to_observations",
    "_refresh_varac_messages",
    "_start_signature_verification",
    "_populate_messages_table",
}


def test_file_scan_completion_callback_has_no_synchronous_heavy_work() -> None:
    """Static boundary: completion only swaps state and schedules follow-up work."""

    method = _completion_method_ast()
    calls = {
        node.func.attr
        for node in ast.walk(method)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _FORBIDDEN_COMPLETION_CALLS
    }
    assert calls == set(), f"heavy work remains synchronous: {sorted(calls)}"


def test_file_scan_completion_dynamic_boundary_does_not_invoke_heavy_hooks(monkeypatch) -> None:
    """Dynamic seam: a completion callback cannot invoke heavy follow-up hooks."""

    pytest.importorskip("PySide6")
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    events: list[str] = []

    class _Settings:
        def get(self, _key, default=None):
            return default

    class _Signal:
        def emit(self, _value):
            return None

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._refresh_files_inflight = False
    tab._file_scan_start_ts = time.time()
    tab._files_snapshot_fp = ()
    tab._projection_primary_enabled = True
    tab._last_projection_render_ts = 1.0
    tab._messages_busy_state = False
    tab._scan_cache_loaded = False
    tab.settings = _Settings()
    tab.busyStateChanged = _Signal()

    records = dict(_EMPTY_ORIGINS)
    records["flamp"] = [FileRecord(path=Path("new-report.k2s"), origin="flamp", size=3, mtime=1.0)]
    for name in _FORBIDDEN_COMPLETION_CALLS:
        setattr(tab, name, lambda _name=name, **_kwargs: events.append(_name))
    tab._update_fldigi_senders = lambda *_args, **_kwargs: None
    tab._load_read_state_map = lambda *_args, **_kwargs: {}
    # Starting a worker is the permitted hand-off seam; the callback must not
    # execute the worker's projection body synchronously.
    tab._start_native_file_projection_write = lambda *_args, **_kwargs: None
    tab._emit_message_refresh_busy = lambda: None

    MessageViewerTab._on_file_scan_finished(
        tab, {"records": records, "dir_mtimes": {}, "mode": "incremental"}, False
    )
    assert events == []
