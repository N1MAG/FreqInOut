from __future__ import annotations

import datetime
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log
from freqinout.core.sqlite_utils import connect_sqlite


MAX_SCHEDULER_EVENTS = 500
_schema_initialized_paths: Set[str] = set()


def _db_path() -> Path:
    cfg = get_config_dir() / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    return cfg / "freqinout.db"


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    db_key = str(_db_path())
    if db_key in _schema_initialized_paths:
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduler_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            event_type TEXT NOT NULL,
            code TEXT NOT NULL,
            source TEXT,
            action TEXT,
            detail TEXT,
            frequency_hz INTEGER,
            band TEXT,
            mode TEXT,
            vfo TEXT,
            schedule_key TEXT,
            metadata_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scheduler_events_ts ON scheduler_events(ts_utc)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scheduler_events_code ON scheduler_events(code)")
    _schema_initialized_paths.add(db_key)


def record_scheduler_event(
    *,
    event_type: str,
    code: str,
    source: str = "",
    action: str = "",
    detail: str = "",
    frequency_hz: Optional[int] = None,
    band: str = "",
    mode: str = "",
    vfo: str = "",
    schedule_key: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = connect_sqlite(_db_path(), timeout=0.6, busy_timeout_ms=600)
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO scheduler_events(
                ts_utc, event_type, code, source, action, detail, frequency_hz,
                band, mode, vfo, schedule_key, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _utc_now_iso(),
                str(event_type or "").strip() or "event",
                str(code or "").strip() or "unknown",
                str(source or "").strip(),
                str(action or "").strip(),
                str(detail or "").strip(),
                int(frequency_hz) if frequency_hz is not None else None,
                str(band or "").strip(),
                str(mode or "").strip(),
                str(vfo or "").strip(),
                str(schedule_key or "").strip(),
                json.dumps(metadata or {}, sort_keys=True),
            ),
        )
        conn.execute(
            """
            DELETE FROM scheduler_events
            WHERE id NOT IN (
                SELECT id FROM scheduler_events ORDER BY id DESC LIMIT ?
            )
            """,
            (MAX_SCHEDULER_EVENTS,),
        )
        conn.commit()
    except Exception as exc:
        log.debug("Scheduler event persistence failed: %s", exc)
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def load_recent_scheduler_events(limit: int = 25) -> List[Dict[str, Any]]:
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = connect_sqlite(_db_path(), timeout=0.8, row_factory=sqlite3.Row, busy_timeout_ms=800)
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, ts_utc, event_type, code, source, action, detail, frequency_hz,
                   band, mode, vfo, schedule_key, metadata_json
            FROM scheduler_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, min(100, int(limit or 25))),),
        ).fetchall()
        events: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            try:
                item["metadata"] = json.loads(str(item.pop("metadata_json", "") or "{}"))
            except Exception:
                item["metadata"] = {}
            events.append(item)
        return events
    except Exception as exc:
        log.debug("Scheduler event load failed: %s", exc)
        return []
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
