from __future__ import annotations

import sqlite3
import threading

from freqinout.core import settings_manager as settings_manager_module
from freqinout.core.settings_manager import SettingsManager


def test_settings_manager_rejects_cross_thread_use(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    seen: dict[str, object] = {}
    done = threading.Event()

    def _worker() -> None:
        try:
            settings.set("thread_guard_probe", True)
        except Exception as e:
            seen["error"] = e
        finally:
            done.set()

    thread = threading.Thread(target=_worker)
    thread.start()
    thread.join(timeout=2.0)

    assert done.is_set()
    error = seen.get("error")
    assert isinstance(error, sqlite3.ProgrammingError)
    assert "different thread" in str(error)


def test_runtime_worker_settings_skip_startup_schema_and_migrations(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    owner = SettingsManager()
    owner.set("worker_probe", "before")
    owner.close()

    monkeypatch.setattr(
        settings_manager_module,
        "ensure_multi_radio_settings_schema",
        lambda _conn: (_ for _ in ()).throw(AssertionError("worker repeated startup schema")),
    )
    monkeypatch.setattr(
        settings_manager_module,
        "ensure_multi_rig_migration",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("worker repeated migration")),
    )

    worker = SettingsManager(runtime_worker=True)
    try:
        worker._sync_system_timezone = lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("worker attempted timezone initialization")
        )
        assert worker.get("worker_probe") == "before"
        worker.get("timezone")
        worker.set("worker_probe", "after")
        assert worker.get("worker_probe") == "after"
    finally:
        worker.close()
