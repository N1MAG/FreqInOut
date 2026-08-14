from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from freqinout.core.logger import log


def delete_spotter_store_row(db_path: str | Path | None, msg_id: int) -> bool:
    if not db_path or int(msg_id or 0) <= 0:
        return False
    path = Path(db_path)
    if not path.exists():
        return False
    try:
        with sqlite3.connect(path) as conn:
            cur = conn.execute("DELETE FROM spotter_traffic WHERE id=?", (int(msg_id),))
            return int(cur.rowcount or 0) > 0
    except Exception as e:
        log.debug("MessageSourceDelete: failed to delete spotter row %s: %s", msg_id, e)
        return False


def sitrep_message_key(msg: Any) -> tuple[str, int | str] | None:
    if msg is None:
        return None
    event_id = int(getattr(msg, "event_id", 0) or 0)
    if event_id > 0:
        return ("sitrep", event_id)
    report_key = str(getattr(msg, "report_key", "") or "").strip().lower()
    return ("sitrep", report_key) if report_key else None


def delete_sitrep_store_row(db_path: str | Path | None, msg: Any) -> bool:
    if not db_path:
        return False
    path = Path(db_path)
    if not path.exists():
        return False
    key = sitrep_message_key(msg)
    if key is None:
        return False
    try:
        with sqlite3.connect(path) as conn:
            if isinstance(key[1], int):
                cur = conn.execute("DELETE FROM sitrep_events WHERE id=?", (int(key[1]),))
            else:
                cur = conn.execute("DELETE FROM sitrep_events WHERE report_key=?", (str(key[1]),))
            return int(cur.rowcount or 0) > 0
    except Exception as e:
        log.debug("MessageSourceDelete: failed to delete SitRep %s: %s", key, e)
        return False


def delete_js8_inbox_row(inbox_path: str | Path | None, msg_id: int) -> bool:
    if not inbox_path or int(msg_id or 0) <= 0:
        return False
    path = Path(inbox_path)
    if not path.exists():
        return False
    try:
        with sqlite3.connect(path, timeout=1.0) as conn:
            conn.execute("PRAGMA busy_timeout = 1000")
            deleted = False
            for table in ("inbox_v1", "inbox"):
                try:
                    cols = {str(r[1]).lower() for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
                except Exception:
                    continue
                try:
                    if "id" in cols:
                        cur = conn.execute(f"DELETE FROM {table} WHERE id=?", (int(msg_id),))
                    else:
                        cur = conn.execute(f"DELETE FROM {table} WHERE rowid=?", (int(msg_id),))
                    if cur.rowcount:
                        deleted = True
                except Exception:
                    continue
            return deleted
    except Exception as e:
        log.debug("MessageSourceDelete: failed to delete JS8 inbox row %s: %s", msg_id, e)
        return False


def delete_js8_local_rows(
    db_path: str | Path | None,
    msg_id: int,
    *,
    source_key: object = "",
    source_id: object = 0,
) -> None:
    if not db_path or int(msg_id or 0) <= 0:
        return
    path = Path(db_path)
    if not path.exists():
        return
    try:
        with sqlite3.connect(path) as conn:
            conn.execute("DELETE FROM js8_messages WHERE id=?", (int(msg_id),))
            native_id = int(source_id or 0)
            source_key_txt = str(source_key or "").strip()
            if source_key_txt and native_id > 0:
                conn.execute(
                    "DELETE FROM js8_messages WHERE COALESCE(source_key, '')=? AND COALESCE(source_id, id)=?",
                    (source_key_txt, native_id),
                )
                try:
                    conn.execute(
                        "DELETE FROM js8_inbox_state WHERE COALESCE(source_key, '')=? AND COALESCE(source_id, id)=?",
                        (source_key_txt, native_id),
                    )
                except Exception:
                    pass
            else:
                conn.execute("DELETE FROM js8_inbox_state WHERE id=?", (int(msg_id),))
    except Exception as e:
        log.debug("MessageSourceDelete: failed to delete local JS8 row %s: %s", msg_id, e)


def delete_varac_local_projection(
    db_path: str | Path | None,
    *,
    source: object,
    msg_id: int,
    ingest_source_key: object = "",
) -> None:
    if not db_path or int(msg_id or 0) <= 0:
        return
    path = Path(db_path)
    if not path.exists():
        return
    try:
        with sqlite3.connect(path) as conn:
            key = str(ingest_source_key or "").strip()
            if key and _table_has_column(conn, "varac_messages", "ingest_source_key"):
                conn.execute(
                    "DELETE FROM varac_messages WHERE ingest_source_key=? AND source=? AND id=?",
                    (key, str(source or ""), int(msg_id)),
                )
            else:
                conn.execute(
                    "DELETE FROM varac_messages WHERE source=? AND id=?",
                    (str(source or ""), int(msg_id)),
                )
    except Exception as e:
        log.debug("MessageSourceDelete: failed to delete local VarAC row %s/%s: %s", source, msg_id, e)


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        return any(str(row[1] or "") == column for row in conn.execute(f"PRAGMA table_info({table})").fetchall())
    except Exception:
        return False


def soft_delete_varac_source_row(
    db_path: str | Path | None,
    *,
    source_table: object,
    msg_id: int,
    vmail_guid: object = "",
) -> bool:
    if not db_path or int(msg_id or 0) <= 0:
        return False
    path = Path(db_path)
    if not path.exists():
        return False
    table = str(source_table or "").strip()
    if table not in {"qso", "vmail", "broadcast"}:
        return False
    try:
        with sqlite3.connect(path) as conn:
            cur = conn.execute(f"UPDATE {table} SET is_deleted=1 WHERE id=?", (int(msg_id),))
            if table == "vmail" and str(vmail_guid or "").strip():
                try:
                    conn.execute(
                        "UPDATE vmail_attachment SET is_deleted=1 WHERE vmail_guid=?",
                        (str(vmail_guid or "").strip(),),
                    )
                except Exception:
                    pass
            return int(cur.rowcount or 0) > 0
    except Exception as e:
        log.debug("MessageSourceDelete: failed to soft delete VarAC row %s/%s: %s", table, msg_id, e)
        return False
