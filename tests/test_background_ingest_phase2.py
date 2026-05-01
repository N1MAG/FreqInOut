from __future__ import annotations

import os
import sys
import threading
import time

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

if sys.platform == "darwin":
    pytest.skip("PySide background ingest tests abort in this macOS environment", allow_module_level=True)

import freqinout.core.background_ingest as background_ingest
from freqinout.core.background_ingest import BackgroundIngestController
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.varac_bbs_vault import VaracBbsVaultRunResult


def _wait_until(predicate, timeout: float = 2.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


def test_background_ingest_messages_runs_on_worker_thread(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    controller = BackgroundIngestController(settings)

    started = threading.Event()
    release = threading.Event()
    seen = {
        "thread_id": None,
        "settings_id": None,
        "calls": [],
    }
    main_thread_id = threading.get_ident()

    class FakeIngestor:
        def __init__(self, worker_settings):
            self.worker_settings = worker_settings

        def ingest_js8_messages(self):
            seen["thread_id"] = threading.get_ident()
            seen["settings_id"] = id(self.worker_settings)
            seen["calls"].append("js8")
            started.set()
            release.wait(timeout=1.0)

        def ingest_spotter_from_directed(self):
            seen["calls"].append("spotter")

    monkeypatch.setattr(background_ingest, "MessageIngestor", FakeIngestor)

    controller.start(initial_stagger=False)
    try:
        started_at = time.perf_counter()
        controller._ingest_messages()
        elapsed = time.perf_counter() - started_at

        assert elapsed < 0.1
        assert started.wait(timeout=1.0)

        release.set()
        assert _wait_until(lambda: "messages" not in controller._job_futures)
        assert seen["thread_id"] is not None
        assert seen["thread_id"] != main_thread_id
        assert seen["settings_id"] != id(settings)
        assert seen["calls"] == ["js8", "spotter"]
    finally:
        release.set()
        controller.stop()
        controller.deleteLater()
        app.processEvents()


def test_background_ingest_skips_duplicate_message_submission(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    controller = BackgroundIngestController(SettingsManager())

    started = threading.Event()
    release = threading.Event()
    counts = {
        "instances": 0,
        "js8": 0,
        "spotter": 0,
    }

    class FakeIngestor:
        def __init__(self, worker_settings):
            counts["instances"] += 1
            self.worker_settings = worker_settings

        def ingest_js8_messages(self):
            counts["js8"] += 1
            started.set()
            release.wait(timeout=1.0)

        def ingest_spotter_from_directed(self):
            counts["spotter"] += 1

    monkeypatch.setattr(background_ingest, "MessageIngestor", FakeIngestor)

    controller.start(initial_stagger=False)
    try:
        controller._ingest_messages()
        assert started.wait(timeout=1.0)

        controller._ingest_messages()
        time.sleep(0.05)

        assert counts["instances"] == 1
        assert counts["js8"] == 1

        release.set()
        assert _wait_until(lambda: "messages" not in controller._job_futures)
        assert counts["spotter"] == 1
    finally:
        release.set()
        controller.stop()
        controller.deleteLater()
        app.processEvents()


def test_background_ingest_varac_policy_job_runs_vault_even_when_guard_is_disabled(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    controller = BackgroundIngestController(SettingsManager())
    calls = []

    def fake_vault(worker_settings):
        calls.append(("vault", id(worker_settings)))
        return VaracBbsVaultRunResult(
            enabled=True,
            scanned_events=1,
            processed_events=1,
            published=True,
            active_location_id="default",
            current_session_callsign="W8UFO",
            summary="Managed Vault Default | Session W8UFO",
        )

    class GuardResult:
        scanned_events = 0
        summary = "VGuard disabled"

    def fake_guard(worker_settings):
        calls.append(("guard", id(worker_settings)))
        return GuardResult()

    monkeypatch.setattr(background_ingest, "run_varac_bbs_vault", fake_vault)
    monkeypatch.setattr(background_ingest, "run_varac_guard", fake_guard)

    controller.start(initial_stagger=False)
    try:
        controller._ingest_varac_guard()
        assert _wait_until(lambda: "varac_guard" not in controller._job_futures)
    finally:
        controller.stop()
        controller.deleteLater()
        app.processEvents()

    assert [name for name, _settings_id in calls] == ["vault", "guard"]
    assert calls[0][1] == calls[1][1]
