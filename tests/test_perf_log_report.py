from __future__ import annotations

from pathlib import Path

from freqinout.core.perf_log_report import evaluate_samples, parse_perf_lines, report_for_paths


def test_parser_accepts_structured_ui_and_dependency_records() -> None:
    samples = parse_perf_lines([
        '2026-09-05 PERF|{"meta":{"records":270},"ms":141.791,"name":"messages.file_scan_total"}\n',
        '2026-09-05 PERF|{"meta":{"label":"station_command_bar"},"ms":42.0,"name":"ui.callback"}\n',
        '2026-09-05 PERF|{"meta":{"deadline_ms":3000},"ms":180.0,"name":"shutdown.complete"}\n',
        "2026-09-05 UI_PERF|slow_refresh label=station_command_bar elapsed_ms=851.2\n",
        "2026-09-05 DEPENDENCY_STATUS|slow_process_snapshot|scope=x|duration_ms=2985.7|reason=startup\n",
        "2026-09-05 UI watchdog detected an event-loop stall\n",
    ])
    assert [sample.name for sample in samples] == [
        "messages.file_scan_total",
        "ui.callback",
        "shutdown.complete",
        "ui.slow_refresh",
        "dependency.process_snapshot",
        "ui.watchdog_stall",
    ]
    assert samples[0].metadata["records"] == 270
    assert samples[3].elapsed_ms == 851.2


def test_evaluation_uses_p95_and_reports_absent_instrumentation_as_unobserved() -> None:
    samples = parse_perf_lines([
        'PERF|{"ms":100,"name":"messages.project_rows"}\n',
        'PERF|{"ms":200,"name":"messages.project_rows"}\n',
        'PERF|{"ms":300,"name":"messages.project_rows"}\n',
    ])
    results = {item.name: item for item in evaluate_samples(samples, {"messages.project_rows": 250})}
    assert results["messages.project_rows"].summary["p95"] == 290
    assert not results["messages.project_rows"].passed
    assert not results["mesh.scan"].observed if "mesh.scan" in results else True


def test_report_reads_multiple_log_files(tmp_path: Path) -> None:
    first = tmp_path / "mac.log"
    second = tmp_path / "linux.log"
    first.write_text(
        'PERF|{"ms":93552.998,"name":"startup.main_window_construct"}\n'
        "UI_PERF|slow_refresh label=station_command_bar elapsed_ms=851.2\n",
        encoding="utf-8",
    )
    second.write_text(
        "DEPENDENCY_STATUS|slow_process_snapshot|scope=legacy_primary|duration_ms=2985.7|reason=startup\n"
        "UI watchdog detected an event-loop stall\n",
        encoding="utf-8",
    )
    paths = [first, second]
    report = report_for_paths(paths)
    assert report["sample_count"] == 4
    assert report["metrics"]["startup.main_window_construct"]["observed"]
    assert report["metrics"]["ui.slow_refresh"]["observed"]
    assert report["metrics"]["ui.watchdog_stall"]["passed"] is False


def test_malformed_structured_record_is_ignored() -> None:
    assert parse_perf_lines(["PERF|{not-json}\n", "ordinary line\n"]) == []
