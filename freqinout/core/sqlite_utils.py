from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

from freqinout.core.logger import log
from freqinout.core.perf_metrics import emit_span


def connect_sqlite(
    db_path: str | Path,
    *,
    timeout: float = 2.0,
    row_factory: Any = None,
    busy_timeout_ms: Optional[int] = None,
) -> sqlite3.Connection:
    path = str(db_path)
    conn = sqlite3.connect(path, timeout=max(0.1, float(timeout)))
    if row_factory is not None:
        conn.row_factory = row_factory
    busy_ms = int(busy_timeout_ms if busy_timeout_ms is not None else max(100, float(timeout) * 1000.0))
    try:
        conn.execute(f"PRAGMA busy_timeout={busy_ms}")
    except Exception:
        pass
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
            (str(table_name or "").strip(),),
        ).fetchone()
    except Exception:
        return False
    return bool(row)


def fetch_all(
    db_path: str | Path,
    query: str,
    params: Sequence[Any] = (),
    *,
    timeout: float = 2.0,
    row_factory: Any = None,
    busy_timeout_ms: Optional[int] = None,
    span_name: str = "sqlite.read",
    span_meta: Optional[dict[str, object]] = None,
    min_ms: float = 5.0,
) -> List[Any]:
    start = time.perf_counter()
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = connect_sqlite(
            db_path,
            timeout=timeout,
            row_factory=row_factory,
            busy_timeout_ms=busy_timeout_ms,
        )
        cur = conn.cursor()
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        return list(rows)
    except Exception as exc:
        log.debug("SQLite read failed (%s): %s", db_path, exc)
        raise
    finally:
        emit_span(
            span_name,
            (time.perf_counter() - start) * 1000.0,
            meta=span_meta,
            min_ms=min_ms,
        )
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def fetch_one(
    db_path: str | Path,
    query: str,
    params: Sequence[Any] = (),
    *,
    timeout: float = 2.0,
    row_factory: Any = None,
    busy_timeout_ms: Optional[int] = None,
    span_name: str = "sqlite.read",
    span_meta: Optional[dict[str, object]] = None,
    min_ms: float = 5.0,
) -> Any:
    rows = fetch_all(
        db_path,
        query,
        params,
        timeout=timeout,
        row_factory=row_factory,
        busy_timeout_ms=busy_timeout_ms,
        span_name=span_name,
        span_meta=span_meta,
        min_ms=min_ms,
    )
    return rows[0] if rows else None


def rows_to_dicts(rows: Iterable[Any]) -> List[dict[str, Any]]:
    out: List[dict[str, Any]] = []
    for row in rows:
        try:
            out.append(dict(row))
        except Exception:
            continue
    return out
