from __future__ import annotations

import threading
import time

from PySide6.QtWidgets import QApplication

from freqinout.gui.bounded_snapshot_worker import SnapshotWorkerController
from freqinout.gui.station_health_tab import StationHealthTab
from freqinout.gui.station_overview_tab import StationOverviewTab


def _app() -> QApplication:
    app = QApplication.instance()
    return app if isinstance(app, QApplication) else QApplication([])


def _drain_until(app: QApplication, predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        app.processEvents()
        if predicate():
            return True
        time.sleep(0.005)
    app.processEvents()
    return bool(predicate())


def test_stale_generation_is_ignored_without_replacing_last_coherent_health_snapshot() -> None:
    _app()
    tab = StationHealthTab()
    try:
        coherent = {"issue_count": 0, "severity": "ok", "items": [], "recent_scheduler_events": []}
        tab._snapshot_generation = 8
        tab._last_coherent_summary = coherent
        tab._last_summary = dict(coherent)
        stale = ({"issue_count": 99, "severity": "danger", "items": [{"state": "old"}]}, tuple())
        tab._on_snapshot_ready(7, stale, None)
        assert tab._last_coherent_summary is coherent
        assert tab._last_summary == coherent
    finally:
        tab.shutdown()
        tab.deleteLater()
        _app().processEvents()


def test_rapid_activation_runs_health_providers_off_ui_thread_and_theme_is_cache_only() -> None:
    app = _app()
    tab = StationHealthTab()
    provider_threads: list[int] = []
    main_thread = threading.get_ident()
    try:
        tab.set_runtime_item_provider(lambda: provider_threads.append(threading.get_ident()) or [])
        tab.set_runtime_source_provider(lambda: provider_threads.append(threading.get_ident()) or [])
        provider_threads.clear()
        tab.settings.get = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("theme read"))
        for _ in range(5):
            tab.set_tab_active(True)
            tab.set_tab_active(False)
            tab.apply_theme()
        assert _drain_until(app, lambda: len(provider_threads) >= 2)
        assert all(ident != main_thread for ident in provider_threads)
    finally:
        tab.shutdown()
        tab.deleteLater()
        app.processEvents()


def test_slow_health_refresh_returns_without_blocking_gui_and_can_shutdown_cleanly() -> None:
    app = _app()
    tab = StationHealthTab()
    started = threading.Event()
    release = threading.Event()
    try:
        def slow_items():
            started.set()
            release.wait(2.0)
            return []

        tab._runtime_item_provider = slow_items
        begin = time.monotonic()
        tab.set_tab_active(True)
        elapsed = time.monotonic() - begin
        assert elapsed < 0.2
        assert started.wait(1.0)
        finished = threading.Event()
        tab._snapshot_worker.worker.completed.connect(lambda *_args: finished.set())
        release.set()
        assert _drain_until(app, finished.is_set)
        assert tab.shutdown() is None
        assert tab._snapshot_worker is None
    finally:
        release.set()
        tab.shutdown()
        tab.deleteLater()
        app.processEvents()


def test_worker_coalesces_pending_generations_and_clean_shutdown() -> None:
    app = _app()
    host = StationOverviewTab()
    started = threading.Event()
    release = threading.Event()
    completed: list[int] = []
    controller = SnapshotWorkerController(host, lambda generation, _result, _error: completed.append(generation))
    try:
        def slow_reader():
            started.set()
            release.wait(2.0)
            return {"ok": True}

        controller.request(1, slow_reader)
        assert started.wait(1.0)
        begin = time.monotonic()
        controller.request(2, lambda: {"ok": 2})
        assert time.monotonic() - begin < 0.05
        release.set()
        assert _drain_until(app, lambda: completed == [1, 2])
        assert controller.stop()
        assert not controller.thread.isRunning()
    finally:
        release.set()
        controller.stop()
        host.shutdown()
        host.deleteLater()
        app.processEvents()


def test_settings_gpg_discovery_runs_off_ui_thread(monkeypatch, tmp_path) -> None:
    app = _app()
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    import freqinout.gui.settings_tab as settings_module

    SettingsTab = settings_module.SettingsTab
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    worker_threads: list[int] = []
    completed = threading.Event()

    def available(_path):
        worker_threads.append(threading.get_ident())
        return True, "ready", "/usr/bin/gpg"

    def public_keys(*, configured_path=""):
        worker_threads.append(threading.get_ident())
        return [], ""

    def secret_keys(*, configured_path=""):
        worker_threads.append(threading.get_ident())
        completed.set()
        return [], ""

    monkeypatch.setattr(settings_module, "gpg_available", available)
    monkeypatch.setattr(settings_module, "list_public_keys", public_keys)
    monkeypatch.setattr(settings_module, "list_secret_keys", secret_keys)
    tab = SettingsTab()
    try:
        begin = time.monotonic()
        tab._refresh_gpg_keys_table(show_dialog_on_error=False)
        assert time.monotonic() - begin < 0.2
        assert completed.wait(1.0)
        assert _drain_until(app, lambda: tab._gpg_probe_thread is None)
        assert worker_threads
        assert all(ident != threading.get_ident() for ident in worker_threads)
        assert tab._gpg_keys_loaded is True
    finally:
        tab.shutdown()
        tab.deleteLater()
        app.processEvents()
