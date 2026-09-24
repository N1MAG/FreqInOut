from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Mapping, Optional

from freqinout.core.db_initializer import _ensure_js8_expect_tables
from freqinout.core.group_utils import normalize_group_name
from freqinout.core.js8_expect_store import default_expect_db_path
from freqinout.core.js8_msg_auth import MsgAuthKey, normalize_callsign
from freqinout.core.sqlite_utils import connect_sqlite


MSG_AUTH_SCOPE_SIGNING = "signing"
MSG_AUTH_SCOPE_VERIFY_SENDER = "verify_sender"
MSG_AUTH_SCOPE_VERIFY_ANY = "verify_any"
MSG_AUTH_ANY_SENDER = "*"
MSG_AUTH_SCOPES = {
    MSG_AUTH_SCOPE_SIGNING,
    MSG_AUTH_SCOPE_VERIFY_SENDER,
    MSG_AUTH_SCOPE_VERIFY_ANY,
}


def normalize_msg_auth_scope(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"trusted_group", "group", "verify_group", "any", "any_sender"}:
        return MSG_AUTH_SCOPE_VERIFY_ANY
    if text in {"trusted_sender", "sender", "verify", "verification"}:
        return MSG_AUTH_SCOPE_VERIFY_SENDER
    if text in MSG_AUTH_SCOPES:
        return text
    return MSG_AUTH_SCOPE_SIGNING


def save_msg_auth_key(
    values: Mapping[str, Any],
    *,
    db_path: Optional[Path] = None,
) -> int:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    group_name = normalize_group_name(values.get("group_name", ""))
    key_scope = normalize_msg_auth_scope(values.get("key_scope", values.get("scope", "")))
    callsign = normalize_callsign(values.get("callsign", ""))
    if key_scope == MSG_AUTH_SCOPE_VERIFY_ANY:
        callsign = MSG_AUTH_ANY_SENDER
    key_text = str(values.get("key_text", "") or "").strip()
    if not group_name:
        raise ValueError("group_name is required")
    if not callsign:
        raise ValueError("callsign is required")
    if not key_text:
        raise ValueError("key_text is required")
    now = time.time()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        explicit_id = int(values.get("id", 0) or 0)
        row = None
        if explicit_id > 0:
            row = conn.execute("SELECT id FROM js8_msg_auth_keys WHERE id=?", (explicit_id,)).fetchone()
        if row is None:
            row = conn.execute(
                """
                SELECT id
                FROM js8_msg_auth_keys
                WHERE group_name=? AND callsign=? AND COALESCE(label, '')=? AND COALESCE(key_scope, 'signing')=?
                ORDER BY id ASC
                LIMIT 1
                """,
                (group_name, callsign, str(values.get("label", "") or "").strip(), key_scope),
            ).fetchone()
        payload = {
            "group_name": group_name,
            "callsign": callsign,
            "label": str(values.get("label", "") or "").strip(),
            "key_text": key_text,
            "key_scope": key_scope,
            "enabled": 1 if bool(values.get("enabled", True)) else 0,
            "notes": str(values.get("notes", "") or "").strip(),
            "updated_ts": now,
        }
        if row:
            row_id = int(row[0])
            conn.execute(
                """
                UPDATE js8_msg_auth_keys
                SET group_name=?, callsign=?, label=?, key_text=?, key_scope=?, enabled=?, notes=?, updated_ts=?
                WHERE id=?
                """,
                (
                    payload["group_name"],
                    payload["callsign"],
                    payload["label"],
                    payload["key_text"],
                    payload["key_scope"],
                    payload["enabled"],
                    payload["notes"],
                    payload["updated_ts"],
                    row_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO js8_msg_auth_keys
                    (group_name, callsign, label, key_text, key_scope, enabled, notes, created_ts, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["group_name"],
                    payload["callsign"],
                    payload["label"],
                    payload["key_text"],
                    payload["key_scope"],
                    payload["enabled"],
                    payload["notes"],
                    now,
                    now,
                ),
            )
            row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.commit()
    finally:
        conn.close()
    return row_id


def load_msg_auth_keys(
    *,
    group_name: object = "",
    callsign: object = "",
    key_scope: object = MSG_AUTH_SCOPE_SIGNING,
    db_path: Optional[Path] = None,
    enabled_only: bool = True,
) -> list[MsgAuthKey]:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    group_norm = normalize_group_name(group_name)
    call_norm = normalize_callsign(callsign)
    scope_norm = normalize_msg_auth_scope(key_scope)
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        clauses = []
        params: list[Any] = []
        if group_norm:
            clauses.append("group_name=?")
            params.append(group_norm)
        if call_norm:
            clauses.append("callsign=?")
            params.append(call_norm)
        if scope_norm:
            clauses.append("COALESCE(key_scope, 'signing')=?")
            params.append(scope_norm)
        if enabled_only:
            clauses.append("COALESCE(enabled, 1) != 0")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(
            f"""
            SELECT group_name, callsign, label, key_text, enabled, COALESCE(key_scope, 'signing') AS key_scope
            FROM js8_msg_auth_keys
            {where}
            ORDER BY group_name ASC, callsign ASC, key_scope ASC, label ASC, id ASC
            """,
            tuple(params),
        ).fetchall()
    finally:
        conn.close()
    out: list[MsgAuthKey] = []
    for row in rows:
        group_value = row["group_name"] if hasattr(row, "keys") else row[0]
        call_value = row["callsign"] if hasattr(row, "keys") else row[1]
        label_value = row["label"] if hasattr(row, "keys") else row[2]
        key_value = row["key_text"] if hasattr(row, "keys") else row[3]
        enabled_value = row["enabled"] if hasattr(row, "keys") else row[4]
        scope_value = row["key_scope"] if hasattr(row, "keys") else row[5]
        label = str(label_value or "").strip() or " / ".join(
            part for part in (str(group_value or "").strip(), str(call_value or "").strip()) if part
        )
        out.append(
            MsgAuthKey(
                label=label,
                key=str(key_value or "").strip(),
                scope=normalize_msg_auth_scope(scope_value),
                scope_value=str(call_value or "").strip(),
                enabled=bool(enabled_value),
            )
        )
    return out


def load_msg_auth_verification_keys(
    *,
    group_name: object = "",
    callsign: object = "",
    db_path: Optional[Path] = None,
    enabled_only: bool = True,
) -> list[MsgAuthKey]:
    sender_keys = load_msg_auth_keys(
        group_name=group_name,
        callsign=callsign,
        key_scope=MSG_AUTH_SCOPE_VERIFY_SENDER,
        db_path=db_path,
        enabled_only=enabled_only,
    )
    group_keys = load_msg_auth_keys(
        group_name=group_name,
        callsign=MSG_AUTH_ANY_SENDER,
        key_scope=MSG_AUTH_SCOPE_VERIFY_ANY,
        db_path=db_path,
        enabled_only=enabled_only,
    )
    return sender_keys + group_keys


def list_msg_auth_key_rows(
    *,
    db_path: Optional[Path] = None,
    enabled_only: bool = False,
) -> list[dict[str, Any]]:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        where = "WHERE COALESCE(enabled, 1) != 0" if enabled_only else ""
        rows = conn.execute(
            f"""
            SELECT id, group_name, callsign, label, key_text, enabled, notes, COALESCE(key_scope, 'signing') AS key_scope
            FROM js8_msg_auth_keys
            {where}
            ORDER BY group_name ASC, key_scope ASC, callsign ASC, label ASC, id ASC
            """
        ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "keys"):
            out.append({key: row[key] for key in row.keys()})
        else:
            out.append(
                {
                    "id": row[0],
                    "group_name": row[1],
                    "callsign": row[2],
                    "label": row[3],
                    "key_text": row[4],
                    "enabled": row[5],
                    "notes": row[6],
                    "key_scope": row[7],
                }
            )
    return out


def delete_msg_auth_key(key_id: int, *, db_path: Optional[Path] = None) -> bool:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        cur = conn.execute("DELETE FROM js8_msg_auth_keys WHERE id=?", (int(key_id),))
        conn.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        conn.close()
