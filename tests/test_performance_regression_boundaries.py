"""Regression contracts for the startup and guided-setup responsiveness fixes."""

from __future__ import annotations

import os
import sqlite3
from inspect import getsource
from types import SimpleNamespace

import pytest


def test_main_window_progress_callback_does_not_pump_deferred_timers() -> None:
    """Construction status updates may repaint the splash, but never drain Qt work."""
    from freqinout import main as app_main
    from freqinout.gui.startup_splash import StartupSplash

    main_source = getsource(app_main.main)
    safe_update_source = getsource(StartupSplash.update_status_without_event_pump)

    assert "splash.update_status_without_event_pump" in main_source
    assert "processEvents" not in safe_update_source
    assert "_process_events" not in safe_update_source


def test_mature_propagation_schema_does_not_rewrite_or_dedupe_rows() -> None:
    """A second startup must not normalize/dedupe an indexed, canonical DB."""
    from freqinout.core.db_initializer import _ensure_propagation_outcome_tables

    conn = sqlite3.connect(":memory:")
    try:
        _ensure_propagation_outcome_tables(conn)
        conn.execute(
            """
            INSERT INTO prop_contact_events(
                event_key, ts_utc, origin_grid6, target_type, target_id,
                band, outcome, source, target_grid6, inserted_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("canonical-event", "2026-09-16T00:00:00Z", "DN70AA", "GRID", "DN71AA", "40M", "SUCCESS", "JS8", "DN71AA", "2026-09-16T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO prop_outcome_stats(
                key_hash, origin_grid6, target_type, target_id, band, month,
                utc_hour_bucket, distance_bucket, attempt_count, success_count,
                weighted_attempt, weighted_success, updated_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("canonical-stat", "DN70AA", "GRID", "DN71AA", "40M", 9, 12, "NEAR", 1, 1, 1.0, 1.0, "2026-09-16T00:00:00Z"),
        )
        conn.commit()
        writes_before = conn.total_changes
        trace: list[str] = []
        conn.set_trace_callback(trace.append)

        _ensure_propagation_outcome_tables(conn)

        assert conn.total_changes == writes_before
        sql = "\n".join(trace).upper()
        assert "DELETE FROM PROP_CONTACT_EVENTS" not in sql
        assert "GROUP BY EVENT_KEY" not in sql
        # Normalizers remain guarded; they cannot be full-table rewrites.
        for statement in trace:
            normalized = statement.upper()
            if normalized.lstrip().startswith("UPDATE PROP_"):
                assert " WHERE " in normalized
    finally:
        conn.close()


def test_mesh_start_is_post_shell_work_and_startup_hook_is_idempotent() -> None:
    from freqinout.gui.main_window import MainWindow

    constructor = getsource(MainWindow.__init__)
    post_shell = getsource(MainWindow.start_post_shell_services)
    assert "self._start_mesh_runtime_if_enabled()" not in constructor
    assert "self._start_mesh_runtime_if_enabled()" in post_shell

    calls: list[str] = []
    shell = SimpleNamespace(
        _shutting_down=False,
        _post_shell_services_started=False,
        _background_ingest_start_pending=False,
        background_ingest=None,
        _start_mesh_runtime_if_enabled=lambda: calls.append("mesh"),
        request_message_projection_catchup=lambda **_kwargs: calls.append("projection"),
        _schedule_message_projection_reconcile=lambda: calls.append("reconcile"),
        _publish_watchdog_diagnostic_snapshot=lambda: calls.append("watchdog"),
    )

    MainWindow.start_post_shell_services(shell)
    MainWindow.start_post_shell_services(shell)

    assert calls == ["mesh", "projection", "reconcile", "watchdog"]


def test_guided_autoconfigure_starts_a_qthread_without_inline_discovery(monkeypatch, tmp_path) -> None:
    """The Prepare selected software button moves work off the GUI thread."""
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))

    from PySide6.QtCore import QObject, QEventLoop, QThread, QTimer, Signal
    from PySide6.QtWidgets import QApplication, QDialog, QPushButton
    from freqinout.gui import settings_tab as settings_tab_module
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    source = open("freqinout/gui/settings_tab.py", encoding="utf-8").read()
    start = source.index("        def _start_dialog_autoconfigure() -> None:")
    end = source.index("        configure_auto_btn.clicked.connect(_start_dialog_autoconfigure)", start)
    start_block = source[start:end]
    assert "_GuidedRadioAutofillWorker(" in start_block
    assert "QThread(self)" in start_block
    assert "discover_js8call_file_profiles" not in start_block
    assert "detect_js8" not in start_block
    assert "_guided_radio_autofill_finished.emit" in start_block
    assert "worker.finished.connect(_finish)" not in start_block

    seen: dict[str, bool] = {}

    class _FastWorker(QObject):
        finished = Signal(object)
        failed = Signal(str)

        def __init__(self, *_args, **_kwargs) -> None:
            super().__init__()

        def request_cancel(self) -> None:
            pass

        def run(self) -> None:
            seen["ran_off_gui_thread"] = QThread.currentThread() is not app.thread()
            self.finished.emit({"cancelled": True})

    monkeypatch.setattr(settings_tab_module, "_GuidedRadioAutofillWorker", _FastWorker)

    def _exercise(dialog: QDialog) -> int:
        dialog.show()
        app.processEvents()
        button = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        assert button is not None
        button.click()
        # A fast worker is allowed to finish and restore this transient UI
        # state before the GUI thread observes the button again.  The thread
        # boundary below, rather than a timing-sensitive label snapshot, is
        # the responsiveness contract.
        loop = QEventLoop()
        QTimer.singleShot(1500, loop.quit)
        QTimer.singleShot(0, lambda: _quit_when_finished(loop))
        loop.exec()
        return QDialog.Rejected

    def _quit_when_finished(loop: QEventLoop) -> None:
        if seen.get("ran_off_gui_thread"):
            loop.quit()
        else:
            QTimer.singleShot(10, lambda: _quit_when_finished(loop))

    monkeypatch.setattr(QDialog, "exec", _exercise)
    tab = SettingsTab()
    try:
        tab._open_device_profile_dialog(existing=None)
        assert seen == {"ran_off_gui_thread": True}
    finally:
        tab.deleteLater()
        app.processEvents()


def test_builtin_default_operating_model_is_reenabled_without_duplication(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    from freqinout.core.multi_radio_store import (
        DEFAULT_OPERATING_SYSTEM_KEY,
        MultiRadioStore,
        settings_db_path,
    )
    from freqinout.core.settings_manager import SettingsManager

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    initial = store.ensure_builtin_operating_profiles()
    default = next(row for row in initial if row["system_key"] == DEFAULT_OPERATING_SYSTEM_KEY)
    with store._connect() as conn:  # Regression setup for legacy disabled data.
        conn.execute("UPDATE operating_profiles SET enabled=0 WHERE id=?", (int(default["id"]),))

    repaired = store.ensure_builtin_operating_profiles()
    repaired_default = next(row for row in repaired if row["system_key"] == DEFAULT_OPERATING_SYSTEM_KEY)
    all_defaults = [row for row in store.list_operating_profiles() if row["system_key"] == DEFAULT_OPERATING_SYSTEM_KEY]

    assert int(repaired_default["id"]) == int(default["id"])
    assert int(repaired_default["enabled"]) == 1
    assert len(all_defaults) == 1
