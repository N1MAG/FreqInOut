from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import QApplication


def test_ui_watchdog_writes_hang_dump(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path))
    app = QApplication.instance() or QApplication([])

    from freqinout.core.ui_watchdog import UiEventLoopWatchdog

    watchdog = UiEventLoopWatchdog(stall_threshold_sec=2.0, report_cooldown_sec=5.0)
    watchdog._write_hang_dump(9.25)

    dumps = sorted((tmp_path / "ui_hang_dumps").glob("fio_ui_hang_*.txt"))
    assert dumps
    text = dumps[-1].read_text(encoding="utf-8")
    assert "FreqInOut UI hang watchdog report" in text
    assert "UI heartbeat stale for: 9.250 seconds" in text
    assert "Thread dump:" in text
    watchdog.deleteLater()
    app.processEvents()


def test_main_window_starts_ui_watchdog() -> None:
    text = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    assert "from freqinout.core.ui_watchdog import UiEventLoopWatchdog" in text
    assert "self._ui_watchdog = UiEventLoopWatchdog(self)" in text
    assert "self._ui_watchdog.start()" in text
    assert "self._ui_watchdog.stop()" in text
