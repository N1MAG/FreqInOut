from __future__ import annotations

import os
import sys

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

if sys.platform == "darwin":
    pytest.skip("PySide6 QtCore import aborts in this macOS test environment", allow_module_level=True)

from PySide6.QtCore import QCoreApplication

import freqinout.core.scheduler_engine as scheduler_engine_module
from freqinout.core.scheduler_engine import SchedulerEngine


class _RecorderFuture:
    def __init__(self) -> None:
        self.cancel_calls = 0

    def cancel(self) -> bool:
        self.cancel_calls += 1
        return True

    def done(self) -> bool:
        return False


class _RecorderExecutor:
    def __init__(self, *args, **kwargs) -> None:
        self.shutdown_calls: list[tuple[bool, bool]] = []

    def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


def test_scheduler_stop_cancels_future_and_shuts_down_executor(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    monkeypatch.setattr(scheduler_engine_module, "ThreadPoolExecutor", _RecorderExecutor)

    app = QCoreApplication.instance() or QCoreApplication([])
    engine = SchedulerEngine()
    future = _RecorderFuture()
    executor = engine._control_executor
    engine._control_future = future
    engine.start()

    try:
        assert engine.timer.isActive()

        engine.stop()

        assert not engine.timer.isActive()
        assert engine._shutdown_requested is True
        assert future.cancel_calls == 1
        assert executor.shutdown_calls == [(False, True)]
        assert engine._status_executor.shutdown_calls == [(False, True)]
        assert engine._control_future is None
        assert engine._pending_entry_key is None
    finally:
        engine.deleteLater()
        app.processEvents()
