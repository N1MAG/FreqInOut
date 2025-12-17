from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from freqinout.core.logger import log

APP_NAME = "FreqInOut"


class SettingsManager:
    """
    SQLite-backed settings store with a simple key/value table.
    Values are JSON-encoded to preserve existing data structures.
    """

    def __init__(self) -> None:
        # Root of FreqInOut: .../freqinout/core/settings_manager.py -> parents[2]
        base_dir = Path(__file__).resolve().parents[2]
        self.config_dir = base_dir / "config"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.config_dir / "freqinout.db"
        # Backwards-compat: expose `_config_path`
        self._config_path = self.db_path

        self._conn: Optional[sqlite3.Connection] = None
        self._data: Dict[str, Any] = {}

        self._init_db()
        self._maybe_migrate_from_json()
        self.reload()

    # ---------- internal I/O ---------- #

    def _init_db(self) -> None:
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        self._conn.commit()

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

    def reload(self) -> None:
        """Reload settings from SQLite into the in-memory dict."""
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

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Set a key and immediately persist to SQLite.
        """
        self._data[key] = value
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)",
                    (key, json.dumps(value)),
                )
        except Exception as e:
            log.error("SettingsManager: failed to write key %s: %s", key, e)
            raise

    def set_many(self, values: Dict[str, Any], *, save: bool = True) -> None:
        """
        Batch update multiple keys. Saves once by default to avoid repeated writes.
        """
        self._data.update(values)
        try:
            payload = [(k, json.dumps(v)) for k, v in values.items()]
            with self._conn:
                self._conn.executemany(
                    "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)", payload
                )
        except Exception as e:
            log.error("SettingsManager: failed to batch write: %s", e)
            raise

    def all(self) -> Dict[str, Any]:
        """
        Return the in-memory dict. Callers that mutate this directly should not
        rely on it persisting unless they call set/set_many.
        """
        return self._data
