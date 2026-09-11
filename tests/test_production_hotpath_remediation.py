from __future__ import annotations

import datetime as dt
import sqlite3
import time
from types import SimpleNamespace


def test_multi_radio_store_assures_schema_once_per_store(monkeypatch, tmp_path) -> None:
    import freqinout.core.multi_radio_store as module

    calls = 0
    real = module.ensure_multi_radio_settings_schema

    def counted(conn):
        nonlocal calls
        calls += 1
        return real(conn)

    monkeypatch.setattr(module, "ensure_multi_radio_settings_schema", counted)
    store = module.MultiRadioStore(tmp_path / "settings.db")
    with store.connect():
        pass
    with store.connect():
        pass

    assert calls == 1


def test_propagation_history_cache_is_shared_across_same_day_windows(monkeypatch) -> None:
    from freqinout.core.propagation_service import PropagationService

    service = PropagationService(default_profiles={}, outcome_db_path=None)
    calls = 0

    def history(**_kwargs):
        nonlocal calls
        calls += 1
        return {
            "weighted_attempt": 0.0,
            "weighted_success": 0.0,
            "weighted_attempt_recent": 0.0,
            "unique_days_recent": 0.0,
            "recency_factor": 0.0,
        }

    monkeypatch.setattr(service, "_weighted_history", history)
    args = {
        "modeled_score": 50.0,
        "origin_grid6": "GRID",
        "target_type": "REGION",
        "target_id": "REGION",
        "band": "40M",
    }
    service.blend_modeled_score(
        now_utc=dt.datetime(2026, 9, 11, 8, tzinfo=dt.timezone.utc), **args
    )
    service.blend_modeled_score(
        now_utc=dt.datetime(2026, 9, 11, 20, tzinfo=dt.timezone.utc), **args
    )

    # One target-specific and one pooled lookup; the second display window
    # reuses that same day's immutable empirical snapshot.
    assert calls == 2


def test_shortwave_source_lane_shutdown_is_nonblocking_and_qthread_free(tmp_path) -> None:
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.shortwave_tab import ShortwaveDataSourcesView

    app = QApplication.instance() or QApplication([])
    view = ShortwaveDataSourcesView(tmp_path / "resources.db")
    completed: list[object] = []

    def slow(cancelled):
        deadline = time.monotonic() + 0.25
        while time.monotonic() < deadline and not cancelled():
            time.sleep(0.01)
        return "done"

    view._active = True
    view._run(slow, completed.append)
    assert view._task_thread is not None and view._task_thread.isRunning()
    started = time.monotonic()
    view.shutdown()
    assert time.monotonic() - started < 0.1
    assert view._task_executor is None
    assert completed == []
    view.close()
    app.processEvents()


def test_station_command_health_uses_scheduler_lane_without_store_read() -> None:
    from freqinout.gui.main_window import MainWindow

    validation = '{"state":"warning","warnings":["Review antenna assignment."]}'
    window = MainWindow.__new__(MainWindow)
    window.scheduler = SimpleNamespace(
        active_schedule_lanes=lambda **_kwargs: [
            {
                "device_profile_id": 3,
                "assignment_validation_status_json": validation,
                "hf_rows": [],
                "net_rows": [],
                "sop_rows": [],
            }
        ]
    )
    window.multi_radio_store = SimpleNamespace(
        get_effective_assigned_plan_for_device=lambda *_args: (_ for _ in ()).throw(
            AssertionError("command-bar rendering opened SQLite")
        )
    )
    window._station_command_lane_cache_data = None
    window._station_command_lane_cache_expires = 0.0

    assert MainWindow._station_command_assignment_rf_guard_issues(
        window, {"id": 3}
    ) == [("warn", "Review antenna assignment.")]


def test_varac_status_loader_returns_only_latest_row_per_source(tmp_path) -> None:
    from freqinout.core.varac_ingest import (
        _ensure_local_tables,
        load_latest_varac_sync_status,
    )

    db_path = tmp_path / "messages.db"
    with sqlite3.connect(db_path) as conn:
        _ensure_local_tables(conn)
        conn.executemany(
            """
            INSERT INTO varac_sync_status
                (run_started_ts, run_finished_ts, varac_db_path, success,
                 rows_scanned, rows_written, error_text, ingest_source_key)
            VALUES (?, ?, ?, 1, 1, 1, '', ?)
            """,
            [
                (1.0, 3.0, "/old-a", "a"),
                (2.0, 2.5, "/new-a", "a"),
                (4.0, 4.5, "/only-b", "b"),
            ],
        )

    result = load_latest_varac_sync_status(db_path=db_path)

    assert set(result) == {"a", "b"}
    assert result["a"]["varac_db_path"] == "/new-a"
    assert result["b"]["varac_db_path"] == "/only-b"
