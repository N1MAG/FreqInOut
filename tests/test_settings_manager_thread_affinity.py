from __future__ import annotations

import sqlite3
import threading

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
