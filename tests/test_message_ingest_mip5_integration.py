"""MIP-5 application/UI ownership and startup-boundary regressions."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).parents[1]


def _method_source(path: Path, class_name: str, method_name: str) -> str:
    text = path.read_text(encoding="utf-8-sig")
    tree = ast.parse(text, filename=str(path))
    owner = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    method = next(
        node
        for node in owner.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == method_name
    )
    return ast.get_source_segment(text, method) or ""


def test_background_ingest_starts_only_at_post_shell_boundary() -> None:
    main_window = ROOT / "freqinout" / "gui" / "main_window.py"
    constructor = _method_source(main_window, "MainWindow", "__init__")
    post_shell = _method_source(main_window, "MainWindow", "start_post_shell_services")
    assert "self.background_ingest.start()" not in constructor
    assert "background.start()" in post_shell
    assert "request_message_projection_catchup" in post_shell

    app_main = (ROOT / "freqinout" / "main.py").read_text(encoding="utf-8-sig")
    assert app_main.index('win.show()') < app_main.index('"first_usable_shell"')
    assert app_main.index('"first_usable_shell"') < app_main.index(
        "QTimer.singleShot(0, win.start_post_shell_services)"
    )


def test_inbox_has_no_private_native_projection_coordinator() -> None:
    viewer = (ROOT / "freqinout" / "gui" / "message_viewer_tab.py").read_text(
        encoding="utf-8-sig"
    )
    assert "MessageProjectionCoordinator" not in viewer
    assert "_NativeMessageProjectionWorker" not in viewer
    assert "request_message_projection_catchup" in viewer
    assert "Preview Message Index Rebuild" in viewer


def test_watchdog_is_published_precomputed_scheduler_and_projection_state() -> None:
    main_window = ROOT / "freqinout" / "gui" / "main_window.py"
    publisher = _method_source(
        main_window, "MainWindow", "_publish_watchdog_diagnostic_snapshot"
    )
    assert "publish_diagnostic_snapshot" in publisher
    assert '"scheduler"' in publisher
    assert '"message_projection"' in publisher
    assert "sqlite" not in publisher.lower()


def test_projection_batch_progress_coalesces_visible_inbox_queries() -> None:
    main_window = ROOT / "freqinout" / "gui" / "main_window.py"
    constructor = _method_source(main_window, "MainWindow", "__init__")
    progress = _method_source(
        main_window, "MainWindow", "_on_message_projection_progressed"
    )
    assert "_message_projection_progressed.connect" in constructor
    assert "_request_projected_message_query" in progress
    assert "delay_ms=500" in progress


def test_deep_rebuild_ui_states_scope_and_source_preservation() -> None:
    viewer = (ROOT / "freqinout" / "gui" / "message_viewer_tab.py").read_text(
        encoding="utf-8-sig"
    )
    method = _method_source(
        ROOT / "freqinout" / "gui" / "message_viewer_tab.py",
        "MessageViewerTab",
        "_start_message_projection_deep_rebuild",
    )
    preview_poll = _method_source(
        ROOT / "freqinout" / "gui" / "message_viewer_tab.py",
        "MessageViewerTab",
        "_poll_message_projection_rebuild_preview",
    )
    request_poll = _method_source(
        ROOT / "freqinout" / "gui" / "message_viewer_tab.py",
        "MessageViewerTab",
        "_poll_message_projection_rebuild_request",
    )
    assert "preview_deep_rebuild_async" in method
    assert "request_deep_rebuild_async" in preview_poll
    assert "native messages and received files remain untouched" in preview_poll
    assert "start_deep_rebuild" in request_poll
    assert "progress.canceled.connect(service.cancel)" in request_poll
    assert "background catch-up" in viewer
    assert "after restart" in viewer


def test_message_maintenance_dialog_loads_rows_off_the_ui_thread() -> None:
    path = ROOT / "freqinout" / "gui" / "message_viewer_tab.py"
    method = _method_source(path, "MessageViewerTab", "_open_message_maintenance")
    assert "load_message_maintenance_rows_async" in method
    assert "self._load_message_delete_audit_rows(" not in method
    assert "self._load_hidden_commstat_rows(" not in method
    assert "Loading…" in method
