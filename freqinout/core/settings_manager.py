from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from freqinout.core.logger import log
from freqinout.core.config_paths import get_config_dir
from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.system_timezone import detect_system_timezone_name
from freqinout.core.multi_radio_store import (
    ensure_default_multi_radio_records,
    ensure_multi_radio_settings_schema,
    mirror_legacy_settings_into_runtime_active_device,
)

APP_NAME = "FreqInOut"


class SettingsManager:
    """
    SQLite-backed settings store with a simple key/value table.
    Values are JSON-encoded to preserve existing data structures.
    """

    def __init__(self) -> None:
        # Prefer a user-writable config dir (works for both source and frozen builds)
        self.config_dir = get_config_dir() / "config"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.config_dir / "freqinout.db"
        # Backwards-compat: expose `_config_path`
        self._config_path = self.db_path

        self._conn: Optional[sqlite3.Connection] = None
        self._data: Dict[str, Any] = {}
        self._thread_id = threading.get_ident()
        self._last_timezone_sync_monotonic = 0.0

        self._init_db()
        self._maybe_migrate_from_json()
        self.reload()
        self._purge_legacy_autoquery_keys()
        ensure_default_multi_radio_records(self._conn, self._data)
        self.reload()
        self._sync_system_timezone(force=True)

    # ---------- internal I/O ---------- #

    def _init_db(self) -> None:
        self._conn = connect_sqlite(self.db_path)
        ensure_multi_radio_settings_schema(self._conn)

    def _maybe_migrate_from_json(self) -> None:
        """
        If the kv table is empty and a legacy config.json exists, import it once.
        """
        cur = self._conn.execute("SELECT COUNT(*) FROM kv")
        count = cur.fetchone()[0]
        if count:
            return
        legacy = self.config_dir / "config.json"
        if not legacy.exists():
            return
        try:
            data = json.loads(legacy.read_text(encoding="utf-8") or "{}")
            self._bulk_write(data)
            log.info("SettingsManager: migrated legacy config.json into %s", self.db_path)
        except Exception as e:
            log.error("SettingsManager: migration from config.json failed: %s", e)

    def _bulk_write(self, data: Dict[str, Any]) -> None:
        payload = [(k, json.dumps(v)) for k, v in data.items()]
        with self._conn:
            self._conn.executemany(
                "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)", payload
            )

    def _purge_legacy_autoquery_keys(self) -> None:
        """
        Remove deprecated JS8 auto-query keys from the settings table.
        """
        if self._data.get("autoquery_keys_purged_v1"):
            return
        keys = ("js8_auto_query_msg_id", "js8_auto_query_grids")
        try:
            with self._conn:
                self._conn.executemany("DELETE FROM kv WHERE key=?", [(k,) for k in keys])
                self._conn.execute(
                    "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)",
                    ("autoquery_keys_purged_v1", json.dumps(True)),
                )
            for k in keys:
                self._data.pop(k, None)
            self._data["autoquery_keys_purged_v1"] = True
        except Exception as e:
            log.error("SettingsManager: failed to purge legacy autoquery keys: %s", e)

    def reload(self) -> None:
        """Reload settings from SQLite into the in-memory dict."""
        self._assert_thread_affinity()
        cur = self._conn.execute("SELECT key, value FROM kv")
        loaded: Dict[str, Any] = {}
        for key, val in cur.fetchall():
            try:
                loaded[key] = json.loads(val)
            except Exception:
                loaded[key] = val
        self._data = loaded

    # ---------- public persistence API ---------- #

    def save(self) -> None:
        """Compatibility no-op: data is written immediately on set."""
        return

    def write(self) -> None:
        """Compatibility no-op: data is written immediately on set."""
        return

    # ---------- public data API ---------- #

    def _assert_thread_affinity(self) -> None:
        current_thread_id = threading.get_ident()
        if current_thread_id == self._thread_id:
            return
        raise sqlite3.ProgrammingError(
            "SettingsManager used from a different thread than it was created on "
            f"(created={self._thread_id}, current={current_thread_id}). "
            "Create a new SettingsManager in the worker thread instead of sharing this instance."
        )

    def _sync_system_timezone(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_timezone_sync_monotonic) < 30.0:
            return
        self._last_timezone_sync_monotonic = now

        current = str(self._data.get("timezone") or "").strip() or "UTC"
        detected = detect_system_timezone_name(current)
        if detected == current:
            return

        self._data["timezone"] = detected
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)",
                    ("timezone", json.dumps(detected)),
                )
                try:
                    mirror_legacy_settings_into_runtime_active_device(
                        self._conn,
                        self._data,
                        keys_changed={"timezone"},
                    )
                except Exception as mirror_exc:
                    log.error(
                        "SettingsManager: failed to mirror timezone into multi-radio store: %s",
                        mirror_exc,
                    )
            log.info("SettingsManager: synced timezone to %s (was %s)", detected, current)
        except Exception as e:
            log.error("SettingsManager: failed to sync timezone %s: %s", detected, e)
            raise

    def get(self, key: str, default: Any = None) -> Any:
        self._assert_thread_affinity()
        if key == "timezone":
            self._sync_system_timezone()
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Set a key and immediately persist to SQLite.
        """
        self._assert_thread_affinity()
        self._data[key] = value
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)",
                    (key, json.dumps(value)),
                )
                try:
                    mirror_legacy_settings_into_runtime_active_device(
                        self._conn,
                        self._data,
                        keys_changed={key},
                    )
                except Exception as mirror_exc:
                    log.error(
                        "SettingsManager: failed to mirror key %s into multi-radio store: %s",
                        key,
                        mirror_exc,
                    )
        except Exception as e:
            log.error("SettingsManager: failed to write key %s: %s", key, e)
            raise

    def set_many(self, values: Dict[str, Any], *, save: bool = True) -> None:
        """
        Batch update multiple keys. Saves once by default to avoid repeated writes.
        """
        self._assert_thread_affinity()
        self._data.update(values)
        try:
            payload = [(k, json.dumps(v)) for k, v in values.items()]
            with self._conn:
                self._conn.executemany(
                    "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)", payload
                )
                try:
                    mirror_legacy_settings_into_runtime_active_device(
                        self._conn,
                        self._data,
                        keys_changed=set(values.keys()),
                    )
                except Exception as mirror_exc:
                    log.error(
                        "SettingsManager: failed to mirror batch settings into multi-radio store: %s",
                        mirror_exc,
                    )
        except Exception as e:
            log.error("SettingsManager: failed to batch write: %s", e)
            raise

    def all(self) -> Dict[str, Any]:
        """
        Return the in-memory dict. Callers that mutate this directly should not
        rely on it persisting unless they call set/set_many.
        """
        self._assert_thread_affinity()
        return self._data

    def close(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
