from __future__ import annotations

from pathlib import Path
import time

from PySide6.QtWidgets import QApplication


def test_ui_watchdog_writes_hang_dump(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path))
    app = QApplication.instance() or QApplication([])

    from freqinout.core.ui_watchdog import UiEventLoopWatchdog

    watchdog = UiEventLoopWatchdog(stall_threshold_sec=2.0, report_cooldown_sec=5.0)
    watchdog.publish_diagnostic_snapshot(
        {"endpoint_lane_count": 5, "endpoint_lanes": [{"state": "running"}]}
    )
    watchdog._write_hang_dump(9.25)

    dumps = sorted((tmp_path / "ui_hang_dumps").glob("fio_ui_hang_*.txt"))
    assert dumps
    text = dumps[-1].read_text(encoding="utf-8")
    assert "FreqInOut UI hang watchdog report" in text
    assert "UI heartbeat stale for: 9.250 seconds" in text
    assert "Scheduler diagnostics:" in text
    assert '"endpoint_lane_count": 5' in text
    assert "Thread dump:" in text
    watchdog.deleteLater()
    app.processEvents()


def test_main_window_starts_ui_watchdog() -> None:
    text = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    assert "from freqinout.core.ui_watchdog import ProcessCpuWatchdog, UiEventLoopWatchdog" in text
    assert "self._ui_watchdog = UiEventLoopWatchdog(self)" in text
    assert "self._ui_watchdog.start()" in text
    assert "self._ui_watchdog.stop()" in text
    assert "self._cpu_watchdog = ProcessCpuWatchdog()" in text
    assert "self._cpu_watchdog.start()" in text
    assert "self._cpu_watchdog.stop()" in text


def test_cpu_watchdog_captures_sustained_cpu_with_bounded_redacted_evidence(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path))
    from freqinout.core.ui_watchdog import ProcessCpuWatchdog

    watchdog = ProcessCpuWatchdog(
        high_cpu_percent=50.0,
        consecutive_samples=2,
        report_cooldown_sec=5.0,
        max_reports=2,
    )
    watchdog.publish_diagnostic_snapshot(
        {
            "scheduler": {
                "schedule_projection": {"request_count": 303},
                "api_token": "do-not-export",
            }
        }
    )

    assert not watchdog._observe_cpu(80.0, 10.0)
    assert watchdog._observe_cpu(90.0, 11.0)
    assert not watchdog._observe_cpu(90.0, 12.0)

    dumps = sorted((tmp_path / "cpu_hotspots").glob("fio_cpu_hotspot_*.txt"))
    assert len(dumps) == 1
    report = dumps[0].read_text(encoding="utf-8")
    assert "FreqInOut sustained CPU hotspot report" in report
    assert "Process CPU: 90.0% of one logical core" in report
    assert '"request_count": 303' in report
    assert '"api_token": "<redacted>"' in report
    assert "do-not-export" not in report
    assert "Bounded Python thread stacks:" in report


def test_cpu_watchdog_resets_threshold_streak() -> None:
    from freqinout.core.ui_watchdog import ProcessCpuWatchdog

    watchdog = ProcessCpuWatchdog(
        high_cpu_percent=50.0,
        consecutive_samples=3,
        report_cooldown_sec=5.0,
    )
    watchdog._write_hotspot_dump = lambda _percent: None

    assert not watchdog._observe_cpu(75.0, 10.0)
    assert not watchdog._observe_cpu(10.0, 11.0)
    assert not watchdog._observe_cpu(75.0, 12.0)
    assert not watchdog._observe_cpu(75.0, 13.0)
    assert not watchdog._observe_cpu(10.0, 14.0)


def test_cpu_watchdog_stop_interrupts_monitor_wait() -> None:
    from freqinout.core.ui_watchdog import ProcessCpuWatchdog

    watchdog = ProcessCpuWatchdog(sample_interval_sec=10.0)
    watchdog.start()
    monitor = watchdog._monitor_thread
    started = time.monotonic()
    watchdog.stop()

    assert time.monotonic() - started < 0.5
    assert monitor is not None
    assert not monitor.is_alive()


def test_ui_watchdog_stop_interrupts_monitor_wait() -> None:
    from freqinout.core.ui_watchdog import UiEventLoopWatchdog

    watchdog = UiEventLoopWatchdog(check_interval_sec=10.0)
    watchdog.start()
    monitor = watchdog._monitor_thread
    started = time.monotonic()
    watchdog.stop()

    assert time.monotonic() - started < 0.5
    assert monitor is not None
    assert not monitor.is_alive()
