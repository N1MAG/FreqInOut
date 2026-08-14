from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List


MESSAGE_DELETE_AUDIT_MAX_ROWS = 1000


def safe_audit_text(value: object, *, limit: int = 160) -> str:
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def ensure_message_delete_audit_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS message_delete_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            audit_ts REAL NOT NULL,
            batch_id TEXT,
            source TEXT,
            action TEXT,
            result TEXT,
            row_key TEXT,
            from_call TEXT,
            to_call TEXT,
            title TEXT,
            detail TEXT
        )
        """
    )


def record_message_delete_audit(
    db_path: Path | str,
    *,
    batch_id: str = "",
    source: object = "",
    action: object = "",
    result: object = "",
    row_key: object = "",
    from_call: object = "",
    to_call: object = "",
    title: object = "",
    detail: object = "",
    audit_ts: float | None = None,
) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path, timeout=1.0) as conn:
        ensure_message_delete_audit_table(conn)
        conn.execute(
            """
            INSERT INTO message_delete_audit
                (audit_ts, batch_id, source, action, result, row_key, from_call, to_call, title, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                float(time.time() if audit_ts is None else audit_ts),
                str(batch_id or ""),
                safe_audit_text(source, limit=80),
                safe_audit_text(action, limit=120),
                safe_audit_text(str(result or "").strip().lower() or "unknown", limit=48),
                safe_audit_text(row_key, limit=240),
                safe_audit_text(from_call, limit=48),
                safe_audit_text(to_call, limit=80),
                safe_audit_text(title, limit=160),
                safe_audit_text(detail, limit=240),
            ),
        )


def load_message_delete_audit_rows(db_path: Path | str, *, limit: int = 250) -> List[Dict[str, Any]]:
    path = Path(db_path)
    if not path.exists():
        return []
    capped = max(1, min(int(limit or 250), MESSAGE_DELETE_AUDIT_MAX_ROWS))
    with sqlite3.connect(path) as conn:
        ensure_message_delete_audit_table(conn)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT audit_ts, batch_id, source, action, result, from_call, to_call, title, detail
            FROM message_delete_audit
            ORDER BY audit_ts DESC, id DESC
            LIMIT ?
            """,
            (capped,),
        ).fetchall()
    return [dict(row) for row in rows]
