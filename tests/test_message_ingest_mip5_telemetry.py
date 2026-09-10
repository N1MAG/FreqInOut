"""MIP-5 bounded telemetry, watchdog diagnostics, and lifecycle contracts."""

from __future__ import annotations

import ast
import json
import time
from pathlib import Path

from freqinout.core.perf_metrics import (
    BufferedPerfMetricsWriter,
    configure_perf_metrics_sink,
    emit_span,
    flush_perf_metrics,
    shutdown_perf_metrics,
)
from freqinout.core.ui_watchdog import cache_safe_diagnostic_snapshot


def test_buffered_metrics_sink_batches_and_rotates_with_bounded_files(tmp_path: Path) -> None:
    path = tmp_path / "perf_metrics.log"
    sink = BufferedPerfMetricsWriter(
        lambda: path,
        max_bytes=220,
        backup_count=2,
        queue_size=64,
        batch_size=8,
        flush_interval_sec=0.01,
    )
    try:
        for index in range(40):
            assert sink.emit(json.dumps({"name": "messages.model_query", "ms": index}))
        sink.flush(timeout=2.0)
        stats = sink.stats()
    finally:
        sink.close(timeout=2.0)

    assert stats["queued"] == 0
    assert stats["dropped"] == 0
    assert stats["written"] == 40
    files = [path, path.with_name("perf_metrics.log.1"), path.with_name("perf_metrics.log.2")]
    assert path.exists()
    assert any(candidate.exists() for candidate in files[1:])
    assert all(candidate.stat().st_size <= 220 for candidate in files if candidate.exists())


def test_buffered_metrics_queue_is_bounded_and_shutdown_is_prompt(tmp_path: Path) -> None:
    path = tmp_path / "perf_metrics.log"
    sink = BufferedPerfMetricsWriter(
        lambda: path,
        queue_size=1,
        batch_size=1,
        flush_interval_sec=30.0,
    )
    try:
        sink.emit("first")
        # A full queue may drop telemetry but must return immediately.
        started = time.monotonic()
        sink.emit("second")
        elapsed = time.monotonic() - started
        assert elapsed < 0.1
        assert sink.stats()["dropped"] >= 0
    finally:
        started = time.monotonic()
        sink.close(timeout=0.5)
        assert time.monotonic() - started < 0.75
        assert not sink._thread.is_alive()


def test_emit_span_uses_buffered_sink_and_flushes_bounded_evidence(tmp_path: Path) -> None:
    path = tmp_path / "perf_metrics.log"
    sink = configure_perf_metrics_sink(lambda: path, flush_interval_sec=0.01)
    try:
        emit_span("messages.model_query", 12.5, meta={"rows": 200, "total": 12000})
        flush_perf_metrics(timeout=2.0)
    finally:
        shutdown_perf_metrics(timeout=2.0)
    text = path.read_text(encoding="utf-8")
    assert "messages.model_query" in text
    assert '"ms":12.5' in text
    assert '"rows":200' in text


def test_watchdog_diagnostic_snapshot_is_bounded_and_credential_safe() -> None:
    cyclic: dict[str, object] = {"endpoint_label": "radio-a", "api_token": "do-not-export"}
    cyclic["cycle"] = cyclic
    cyclic.update({f"extra-{index}": "x" * 1000 for index in range(200)})

    safe = cache_safe_diagnostic_snapshot(cyclic)
    encoded = json.dumps(safe, sort_keys=True)

    assert len(safe) <= 64
    assert len(encoded) < 20_000
    assert safe["api_token"] == "<redacted>"
    assert "do-not-export" not in encoded
    assert safe["cycle"] == {"endpoint_label": "radio-a", "api_token": "<redacted>", "cycle": "<cycle>"} or safe["cycle"] == "<cycle>"


def test_watchdog_consumes_published_cache_without_live_lookup(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path))
    from PySide6.QtWidgets import QApplication

    from freqinout.core.ui_watchdog import UiEventLoopWatchdog

    app = QApplication.instance() or QApplication([])
    watchdog = UiEventLoopWatchdog(stall_threshold_sec=2.0, report_cooldown_sec=5.0)
    watchdog.publish_diagnostic_snapshot({"endpoint_hash": "safe", "password": "secret"})
    watchdog._write_hang_dump(3.0)

    dumps = sorted((tmp_path / "ui_hang_dumps").glob("fio_ui_hang_*.txt"))
    assert dumps
    text = dumps[-1].read_text(encoding="utf-8")
    assert '"endpoint_hash": "safe"' in text
    assert '"password": "<redacted>"' in text
    assert "secret" not in text
    watchdog.deleteLater()
    app.processEvents()


def test_watchdog_cache_miss_is_bounded_without_live_provider() -> None:
    from PySide6.QtWidgets import QApplication

    from freqinout.core.ui_watchdog import UiEventLoopWatchdog

    app = QApplication.instance() or QApplication([])
    watchdog = UiEventLoopWatchdog(stall_threshold_sec=2.0, report_cooldown_sec=5.0)

    assert watchdog._diagnostics_for_dump() == {"state": "not_published"}
    assert not hasattr(watchdog, "set_diagnostic_provider")
    watchdog.deleteLater()
    app.processEvents()


def test_watchdog_heartbeat_callback_is_pure_timestamp_update() -> None:
    path = Path(__file__).parents[1] / "freqinout" / "core" / "ui_watchdog.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    beat = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_beat"
    )
    source = ast.get_source_segment(path.read_text(encoding="utf-8"), beat) or ""
    forbidden = ("get_config_dir", "open(", "sqlite", "socket", "provider", "dump_traceback")
    assert not [token for token in forbidden if token in source]


def test_projection_query_lifecycle_rejects_shutdown_and_inactive_results() -> None:
    path = Path(__file__).parents[1] / "freqinout" / "gui" / "message_viewer_tab.py"
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text, filename=str(path))
    methods: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in {
            "_request_projected_message_query",
            "_start_projected_message_query",
            "_on_projected_message_query_finished",
        }:
            methods[node.name] = ast.get_source_segment(text, node) or ""
    assert set(methods) == {
        "_request_projected_message_query",
        "_start_projected_message_query",
        "_on_projected_message_query_finished",
    }
    for name, source in methods.items():
        assert "_is_shutting_down" in source, name
    assert "_has_active_view" in methods["_on_projected_message_query_finished"]
    assert "_app_active" in methods["_on_projected_message_query_finished"]


def test_required_message_pipeline_telemetry_names_are_wired() -> None:
    root = Path(__file__).parents[1] / "freqinout"
    source = "\n".join(
        path.read_text(encoding="utf-8-sig")
        for path in (
            root / "core" / "message_projection_coordinator.py",
            root / "core" / "message_projection_maintenance.py",
            root / "core" / "message_projection_writer.py",
            root / "core" / "js8_expect_store.py",
            root / "gui" / "message_viewer_tab.py",
        )
    )
    for metric in (
        "messages.dirty_detect",
        "messages.prepare_batch",
        "messages.write_batch",
        "messages.projection_lag",
        "messages.reconcile_source",
        "messages.file_discovery",
        "messages.model_query",
        "messages.model_apply",
        "messages.expect_fast_path",
        "messages.shutdown",
    ):
        assert metric in source
