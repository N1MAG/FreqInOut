from __future__ import annotations

import datetime as dt
import sqlite3
from typing import Any, Dict, List, Optional

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


SITREP_ALLOWED = {"GREEN", "YELLOW", "RED"}
_SCHEMA_READY = False
_SCHEMA_READY_DB = ""


def _db_path():
    return get_config_dir() / "config" / "freqinout_nets.db"


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _norm_callsign(value: str) -> str:
    return (value or "").strip().upper()


def _norm_status(value: str) -> str:
    txt = (value or "").strip().upper()
    return txt if txt in SITREP_ALLOWED else "GREEN"


def _split_name_parts(value: str) -> tuple[str, str]:
    txt = (value or "").strip()
    if not txt:
        return "", ""
    parts = [p for p in txt.split() if p]
    if len(parts) <= 1:
        return (parts[0] if parts else ""), ""
    return parts[0], " ".join(parts[1:])


def _compose_name(first_name: str, last_name: str) -> str:
    first = (first_name or "").strip()
    last = (last_name or "").strip()
    return " ".join([p for p in (first, last) if p]).strip()


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for name, col_type in columns.items():
        if name in existing:
            continue
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")


def ensure_tables() -> None:
    global _SCHEMA_READY, _SCHEMA_READY_DB
    db_path = _db_path()
    db_key = str(db_path.resolve()) if db_path.exists() else str(db_path)
    if _SCHEMA_READY and _SCHEMA_READY_DB == db_key and db_path.exists():
        return
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS local_operator_checkins (
                callsign TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                name TEXT,
                city TEXT,
                state TEXT,
                category TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                checkin_count INTEGER DEFAULT 0,
                notes TEXT,
                sitrep_status TEXT DEFAULT 'GREEN',
                updated_utc TEXT
            )
            """
        )
        _ensure_columns(
            conn,
            "local_operator_checkins",
            {
                "first_name": "TEXT",
                "last_name": "TEXT",
                "name": "TEXT",
                "city": "TEXT",
                "state": "TEXT",
                "category": "TEXT",
                "first_seen_utc": "TEXT",
                "last_seen_utc": "TEXT",
                "checkin_count": "INTEGER DEFAULT 0",
                "notes": "TEXT",
                "sitrep_status": "TEXT DEFAULT 'GREEN'",
                "updated_utc": "TEXT",
            },
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ops_last_seen ON local_operator_checkins(last_seen_utc)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ops_category ON local_operator_checkins(category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ops_status ON local_operator_checkins(sitrep_status)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS local_ncs_checkins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                checkin_utc TEXT NOT NULL,
                net_name TEXT,
                channels TEXT,
                callsign TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                name TEXT,
                city TEXT,
                state TEXT,
                category TEXT,
                sitrep_status TEXT DEFAULT 'GREEN',
                notes TEXT,
                updated_utc TEXT
            )
            """
        )
        _ensure_columns(
            conn,
            "local_ncs_checkins",
            {
                "checkin_utc": "TEXT",
                "net_name": "TEXT",
                "channels": "TEXT",
                "callsign": "TEXT",
                "first_name": "TEXT",
                "last_name": "TEXT",
                "name": "TEXT",
                "city": "TEXT",
                "state": "TEXT",
                "category": "TEXT",
                "sitrep_status": "TEXT DEFAULT 'GREEN'",
                "notes": "TEXT",
                "updated_utc": "TEXT",
            },
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ncs_checkins_ts ON local_ncs_checkins(checkin_utc)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_local_ncs_checkins_callsign ON local_ncs_checkins(callsign)")
        conn.commit()
        _SCHEMA_READY = True
        _SCHEMA_READY_DB = db_key
    finally:
        conn.close()


def get_all_operators() -> List[Dict[str, Any]]:
    ensure_tables()
    conn = sqlite3.connect(_db_path())
    try:
        cur = conn.execute(
            """
            SELECT
                callsign,
                first_name,
                last_name,
                name,
                city,
                state,
                category,
                first_seen_utc,
                last_seen_utc,
                IFNULL(checkin_count, 0),
                notes,
                sitrep_status,
                updated_utc
            FROM local_operator_checkins
            ORDER BY callsign COLLATE NOCASE
            """
        )
        out: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            first_name = (row[1] or "").strip()
            last_name = (row[2] or "").strip()
            full_name = (row[3] or "").strip()
            if (not first_name and not last_name) and full_name:
                first_name, last_name = _split_name_parts(full_name)
            if not full_name:
                full_name = _compose_name(first_name, last_name)
            out.append(
                {
                    "callsign": row[0] or "",
                    "first_name": first_name,
                    "last_name": last_name,
                    "name": full_name,
                    "city": row[4] or "",
                    "state": row[5] or "",
                    "category": row[6] or "",
                    "first_seen_utc": row[7] or "",
                    "last_seen_utc": row[8] or "",
                    "checkin_count": int(row[9] or 0),
                    "notes": row[10] or "",
                    "sitrep_status": _norm_status(row[11] or "GREEN"),
                    "updated_utc": row[12] or "",
                }
            )
        return out
    finally:
        conn.close()


def get_operator(callsign: str) -> Optional[Dict[str, Any]]:
    cs = _norm_callsign(callsign)
    if not cs:
        return None
    ensure_tables()
    conn = sqlite3.connect(_db_path())
    try:
        cur = conn.execute(
            """
            SELECT
                callsign,
                first_name,
                last_name,
                name,
                city,
                state,
                category,
                first_seen_utc,
                last_seen_utc,
                IFNULL(checkin_count, 0),
                notes,
                sitrep_status,
                updated_utc
            FROM local_operator_checkins
            WHERE callsign=?
            """,
            (cs,),
        )
        row = cur.fetchone()
        if not row:
            return None
        first_name = (row[1] or "").strip()
        last_name = (row[2] or "").strip()
        full_name = (row[3] or "").strip()
        if (not first_name and not last_name) and full_name:
            first_name, last_name = _split_name_parts(full_name)
        if not full_name:
            full_name = _compose_name(first_name, last_name)
        return {
            "callsign": row[0] or "",
            "first_name": first_name,
            "last_name": last_name,
            "name": full_name,
            "city": row[4] or "",
            "state": row[5] or "",
            "category": row[6] or "",
            "first_seen_utc": row[7] or "",
            "last_seen_utc": row[8] or "",
            "checkin_count": int(row[9] or 0),
            "notes": row[10] or "",
            "sitrep_status": _norm_status(row[11] or "GREEN"),
            "updated_utc": row[12] or "",
        }
    finally:
        conn.close()


def upsert_operator(
    callsign: str,
    *,
    first_name: str = "",
    last_name: str = "",
    name: str = "",
    city: str = "",
    state: str = "",
    category: str = "",
    notes: Optional[str] = None,
    sitrep_status: Optional[str] = None,
    first_seen_utc: Optional[str] = None,
    last_seen_utc: Optional[str] = None,
    checkin_count: Optional[int] = None,
    touch_seen: bool = False,
    increment_checkins: bool = False,
    seen_utc: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    cs = _norm_callsign(callsign)
    if not cs:
        return None

    ensure_tables()
    now_iso = _utc_now_iso()
    seen_stamp = (seen_utc or now_iso).strip()
    conn = sqlite3.connect(_db_path())
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                first_name,
                last_name,
                name,
                city,
                state,
                category,
                first_seen_utc,
                last_seen_utc,
                IFNULL(checkin_count, 0),
                notes,
                sitrep_status
            FROM local_operator_checkins
            WHERE callsign=?
            """,
            (cs,),
        )
        row = cur.fetchone()

        ex_first_name = (row[0] or "") if row else ""
        ex_last_name = (row[1] or "") if row else ""
        ex_name = (row[2] or "") if row else ""
        ex_city = (row[3] or "") if row else ""
        ex_state = (row[4] or "") if row else ""
        ex_category = (row[5] or "") if row else ""
        ex_first_seen = (row[6] or "") if row else ""
        ex_last_seen = (row[7] or "") if row else ""
        ex_count = int(row[8] or 0) if row else 0
        ex_notes = (row[9] or "") if row else ""
        ex_status = _norm_status(row[10] or "GREEN") if row else "GREEN"

        provided_first = (first_name or "").strip()
        provided_last = (last_name or "").strip()
        provided_name = (name or "").strip()
        if provided_name and not provided_first and not provided_last:
            split_first, split_last = _split_name_parts(provided_name)
            provided_first = split_first
            provided_last = split_last

        out_first_name = provided_first or ex_first_name
        out_last_name = provided_last or ex_last_name
        out_name = provided_name or _compose_name(out_first_name, out_last_name) or ex_name
        out_city = (city or "").strip() or ex_city
        out_state = (state or "").strip().upper() or ex_state
        out_category = (category or "").strip() or ex_category
        out_notes = ex_notes if notes is None else (notes or "").strip()
        out_status = _norm_status(sitrep_status or ex_status)
        out_first_seen = (first_seen_utc or "").strip() or ex_first_seen or seen_stamp
        out_last_seen = (last_seen_utc or "").strip() or ex_last_seen
        if touch_seen:
            out_last_seen = seen_stamp
            if not out_first_seen:
                out_first_seen = seen_stamp
        elif not out_last_seen and row:
            out_last_seen = ex_last_seen or ""

        out_count = ex_count + 1 if increment_checkins else ex_count
        if not row and increment_checkins and out_count <= 0:
            out_count = 1
        if checkin_count is not None:
            try:
                out_count = max(0, int(checkin_count))
            except Exception:
                pass

        cur.execute(
            """
            INSERT INTO local_operator_checkins (
                callsign,
                first_name,
                last_name,
                name,
                city,
                state,
                category,
                first_seen_utc,
                last_seen_utc,
                checkin_count,
                notes,
                sitrep_status,
                updated_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(callsign)
            DO UPDATE SET
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                name=excluded.name,
                city=excluded.city,
                state=excluded.state,
                category=excluded.category,
                first_seen_utc=COALESCE(local_operator_checkins.first_seen_utc, excluded.first_seen_utc),
                last_seen_utc=
                    CASE
                        WHEN excluded.last_seen_utc IS NOT NULL AND excluded.last_seen_utc <> '' THEN excluded.last_seen_utc
                        ELSE local_operator_checkins.last_seen_utc
                    END,
                checkin_count=excluded.checkin_count,
                notes=excluded.notes,
                sitrep_status=excluded.sitrep_status,
                updated_utc=excluded.updated_utc
            """,
            (
                cs,
                out_first_name,
                out_last_name,
                out_name,
                out_city,
                out_state,
                out_category,
                out_first_seen,
                out_last_seen,
                out_count,
                out_notes,
                out_status,
                now_iso,
            ),
        )
        conn.commit()
    except Exception as e:
        log.error("local_ops_store.upsert_operator failed for %s: %s", cs, e)
        return None
    finally:
        conn.close()

    return get_operator(cs)


def delete_operators(callsigns: List[str]) -> int:
    keys = sorted({_norm_callsign(cs) for cs in callsigns if _norm_callsign(cs)})
    if not keys:
        return 0
    ensure_tables()
    conn = sqlite3.connect(_db_path())
    deleted = 0
    try:
        with conn:
            for cs in keys:
                cur = conn.execute("DELETE FROM local_operator_checkins WHERE callsign=?", (cs,))
                deleted += int(cur.rowcount or 0)
        return deleted
    finally:
        conn.close()


def record_checkin(
    *,
    callsign: str,
    net_name: str = "",
    channels: str = "",
    first_name: str = "",
    last_name: str = "",
    name: str = "",
    city: str = "",
    state: str = "",
    category: str = "",
    sitrep_status: str = "GREEN",
    notes: str = "",
    checkin_utc: Optional[str] = None,
) -> Optional[int]:
    cs = _norm_callsign(callsign)
    if not cs:
        return None
    stamp = (checkin_utc or _utc_now_iso()).strip()
    status = _norm_status(sitrep_status)
    op_row = upsert_operator(
        cs,
        first_name=first_name,
        last_name=last_name,
        name=name,
        city=city,
        state=state,
        category=category,
        notes=notes,
        sitrep_status=status,
        touch_seen=True,
        increment_checkins=True,
        seen_utc=stamp,
    )
    if op_row is None:
        return None

    ensure_tables()
    conn = sqlite3.connect(_db_path())
    try:
        with conn:
            cur = conn.execute(
                """
                INSERT INTO local_ncs_checkins (
                    checkin_utc,
                    net_name,
                    channels,
                    callsign,
                    first_name,
                    last_name,
                    name,
                    city,
                    state,
                    category,
                    sitrep_status,
                    notes,
                    updated_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stamp,
                    (net_name or "").strip(),
                    (channels or "").strip(),
                    cs,
                    (op_row.get("first_name") or "").strip(),
                    (op_row.get("last_name") or "").strip(),
                    (op_row.get("name") or "").strip(),
                    (op_row.get("city") or "").strip(),
                    (op_row.get("state") or "").strip().upper(),
                    (op_row.get("category") or "").strip(),
                    status,
                    (notes or "").strip(),
                    _utc_now_iso(),
                ),
            )
            return int(cur.lastrowid or 0)
    except Exception as e:
        log.error("local_ops_store.record_checkin failed for %s: %s", cs, e)
        return None
    finally:
        conn.close()


def update_checkin_entry(entry_id: int, *, sitrep_status: str, notes: str) -> bool:
    if int(entry_id or 0) <= 0:
        return False
    status = _norm_status(sitrep_status)
    note_text = (notes or "").strip()
    now_iso = _utc_now_iso()

    ensure_tables()
    conn = sqlite3.connect(_db_path())
    callsign = ""
    try:
        with conn:
            row = conn.execute(
                "SELECT callsign FROM local_ncs_checkins WHERE id=?",
                (int(entry_id),),
            ).fetchone()
            if row:
                callsign = _norm_callsign(row[0] or "")
            conn.execute(
                """
                UPDATE local_ncs_checkins
                SET sitrep_status=?, notes=?, updated_utc=?
                WHERE id=?
                """,
                (status, note_text, now_iso, int(entry_id)),
            )
    except Exception as e:
        log.error("local_ops_store.update_checkin_entry failed for id=%s: %s", entry_id, e)
        return False
    finally:
        conn.close()

    if callsign:
        upsert_operator(
            callsign,
            notes=note_text,
            sitrep_status=status,
            touch_seen=False,
            increment_checkins=False,
        )
    return True


def list_checkins(limit: int = 500) -> List[Dict[str, Any]]:
    ensure_tables()
    lim = max(1, int(limit or 500))
    conn = sqlite3.connect(_db_path())
    try:
        cur = conn.execute(
            """
            SELECT
                id,
                checkin_utc,
                net_name,
                channels,
                callsign,
                first_name,
                last_name,
                name,
                city,
                state,
                category,
                sitrep_status,
                notes
            FROM local_ncs_checkins
            ORDER BY checkin_utc DESC, id DESC
            LIMIT ?
            """,
            (lim,),
        )
        out: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            first_name = (row[5] or "").strip()
            last_name = (row[6] or "").strip()
            full_name = (row[7] or "").strip()
            if (not first_name and not last_name) and full_name:
                first_name, last_name = _split_name_parts(full_name)
            if not full_name:
                full_name = _compose_name(first_name, last_name)
            out.append(
                {
                    "id": int(row[0]),
                    "checkin_utc": row[1] or "",
                    "net_name": row[2] or "",
                    "channels": row[3] or "",
                    "callsign": row[4] or "",
                    "first_name": first_name,
                    "last_name": last_name,
                    "name": full_name,
                    "city": row[8] or "",
                    "state": row[9] or "",
                    "category": row[10] or "",
                    "sitrep_status": _norm_status(row[11] or "GREEN"),
                    "notes": row[12] or "",
                }
            )
        return out
    finally:
        conn.close()
