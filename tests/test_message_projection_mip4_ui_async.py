"""MIP-4 asynchronous Messages query and invalidation acceptance contracts."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from freqinout.core.message_projection_store import ensure_message_projection_schema


def _seed(db_path: Path, count: int = 205) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        with conn:
            for index in range(count):
                conn.execute(
                    """
                    INSERT INTO message_projection (
                        message_id, canonical_key, content_hash, primary_source_id,
                        source_family, group_name, status, severity, deleted, archived,
                        inbox_visible, operator_attention, actionable, event_ts,
                        received_ts, search_text, projected_utc
                    ) VALUES (?, ?, ?, ?, 'js8', 'MAGNET', 'NEW', 'info', 0, 0,
                              1, 0, 0, ?, ?, ?, ?)
                    """,
                    (
                        f"mip4-ui-{index:04d}",
                        f"mip4-ui:key:{index}",
                        f"hash-{index}",
                        "mip4-ui-source",
                        float(index),
                        float(index),
                        f"message body {index}",
                        "2026-09-10T00:00:00Z",
                    ),
                )
    finally:
        conn.close()


def _minimal_tab(*, active: bool = True):
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._projection_primary_enabled = True
    tab._has_active_view = active
    tab._app_active = active
    tab._freeze_messages_table = False
    tab._deferred_refresh = False
    tab._projected_query_request_id = 0
    tab._projected_query_pending = False
    tab._projected_query_pending_force = False
    tab._projected_query_thread = None
    tab._active_projection_generation = 0
    tab.settings = None
    return tab


class _FakeTimer:
    def __init__(self) -> None:
        self.starts: list[int] = []
        self.active = False

    def isActive(self) -> bool:
        return self.active

    def start(self, delay: int) -> None:
        self.starts.append(int(delay))
        self.active = True


def test_populate_projection_primary_never_starts_legacy_rows_build_even_force() -> None:
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    tab = _minimal_tab()
    calls: list[str] = []
    tab._request_projected_message_query = lambda **kwargs: calls.append("projection")
    tab._start_rows_build = lambda **kwargs: calls.append("legacy")

    MessageViewerTab._populate_messages_table(tab, force=True)

    assert calls == ["projection"]


def test_completed_legacy_rows_build_cannot_write_back_in_projection_primary_mode() -> None:
    """A late compatibility worker must not become a second projection writer."""

    from freqinout.gui.message_viewer_tab import MessageViewerTab

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._rows_build_generation = 4
    tab._locally_deleted_row_keys = set()
    tab._projection_primary_enabled = True
    tab._last_projection_render_ts = 1.0
    tab._message_rows = []
    tab._sender_cache = {}
    tab._cache_max_sender_entries = 10
    tab._retag_bbs_archive_rows = lambda _rows: None
    tab._save_message_file_metadata_from_rows = lambda _rows: None
    writes: list[object] = []
    tab._start_message_projection_write = lambda rows, **kwargs: writes.append((rows, kwargs))

    MessageViewerTab._on_rows_build_finished(
        tab,
        {
            "generation": 4,
            "rows": [],
            "force": True,
            "sender_cache_updates": {},
            "elapsed_ms": 0.0,
        },
    )

    assert writes == []


def test_query_worker_returns_bounded_rows_total_and_generation(tmp_path: Path) -> None:
    from freqinout.gui.message_viewer_tab import _ProjectedMessageQueryWorker

    db_path = tmp_path / "projection.sqlite"
    _seed(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("UPDATE message_projection_generation SET generation=19 WHERE singleton=1")
        conn.commit()
    finally:
        conn.close()

    payloads: list[dict] = []
    worker = _ProjectedMessageQueryWorker(
        db_path=str(db_path),
        request_id=7,
        scope_key=("all",),
        query={"source_families": ("js8",)},
    )
    worker.finished.connect(payloads.append)
    worker.run()

    assert len(payloads) == 1
    payload = payloads[0]
    assert payload["request_id"] == 7
    assert payload["total_count"] == 205
    assert payload["generation"] == 19
    assert len(payload["rows"]) <= 200
    assert not payload["error"]


def test_query_worker_carries_bbs_membership_state_without_ui_lookup(tmp_path: Path) -> None:
    from freqinout.gui.message_viewer_tab import _ProjectedMessageQueryWorker

    db_path = tmp_path / "projection.sqlite"
    _seed(db_path, count=1)
    message_path = str(tmp_path / "report.k2s")
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO message_external_refs (
                    message_id, source_id, external_kind, external_key,
                    external_path, external_mtime, external_size, updated_utc
                ) VALUES ('mip4-ui-0000', 'flamp:source', 'flamp_file',
                          'file:1', ?, 1, 20, '2026-09-16T00:00:00Z')
                """,
                (message_path,),
            )
            conn.execute("CREATE TABLE bbs_artifacts (artifact_id TEXT, source_path TEXT)")
            conn.execute(
                "CREATE TABLE bbs_location_artifacts (artifact_id TEXT, publish_enabled INTEGER)"
            )
            conn.execute("INSERT INTO bbs_artifacts VALUES ('a1', ?)", (message_path,))
            conn.execute("INSERT INTO bbs_location_artifacts VALUES ('a1', 1)")
    finally:
        conn.close()

    payloads: list[dict] = []
    worker = _ProjectedMessageQueryWorker(
        db_path=str(db_path),
        request_id=8,
        scope_key=("all",),
        query={"source_families": ("js8",)},
    )
    worker.finished.connect(payloads.append)
    worker.run()

    path_key = str(Path(message_path).absolute())
    assert payloads[0]["bbs_published_counts"] == {path_key: 1}
    assert not payloads[0]["error"]


def test_stale_request_and_generation_results_are_rejected_without_render() -> None:
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    tab = _minimal_tab()
    tab._projected_query_request_id = 8
    tab._active_projection_generation = 12
    events: list[str] = []
    tab._projected_rows_from_mappings = lambda *_args, **_kwargs: events.append("convert") or []
    tab._refresh_message_filters = lambda *_args, **_kwargs: events.append("filters")
    tab._apply_message_filters = lambda *_args, **_kwargs: events.append("render")

    # The request token is newer, but the worker response is from request 7.
    MessageViewerTab._on_projected_message_query_finished(
        tab,
        {"request_id": 7, "generation": 99, "rows": [{"message_id": "old"}]},
    )
    # The request matches, but its projection generation is older than the
    # already-rendered generation.
    MessageViewerTab._on_projected_message_query_finished(
        tab,
        {"request_id": 8, "generation": 11, "rows": [{"message_id": "stale"}]},
    )

    assert events == []
    assert tab._active_projection_generation == 12


def test_hidden_projection_invalidations_set_pending_without_starting_query() -> None:
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    tab = _minimal_tab(active=False)
    tab._projected_query_timer = _FakeTimer()

    MessageViewerTab._request_projected_message_query(tab)
    MessageViewerTab._request_projected_message_query(tab, force=True)
    MessageViewerTab._start_projected_message_query(tab)

    assert tab._projected_query_pending is True
    assert tab._projected_query_pending_force is True
    assert tab._projected_query_timer.starts == []
    assert tab._projected_query_thread is None


def test_visible_projection_invalidations_coalesce_to_one_pending_timer() -> None:
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    tab = _minimal_tab(active=True)
    tab._projected_query_timer = _FakeTimer()

    MessageViewerTab._request_projected_message_query(tab)
    MessageViewerTab._request_projected_message_query(tab)

    assert len(tab._projected_query_timer.starts) == 1
    assert tab._projected_query_timer.starts[0] <= 500
