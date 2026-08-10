from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from freqinout.core.config_paths import get_config_dir
from freqinout.core.db_initializer import _ensure_js8_expect_tables
from freqinout.core.sqlite_utils import connect_sqlite


def default_expect_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout_nets.db"


@dataclass(frozen=True)
class ExpectEntrySaveResult:
    id: int
    created: bool
    expect_key: str
    enabled: bool
    auto_reply_enabled: bool


@dataclass(frozen=True)
class ExpectAllowPolicySaveResult:
    id: int
    created: bool
    name: str
    enabled: bool


@dataclass(frozen=True)
class ExpectEntryUpdateResult:
    id: int
    enabled: bool
    auto_reply_enabled: bool


@dataclass(frozen=True)
class ExpectEvaluationResult:
    decision: str
    reason: str
    expect_entry_id: int = 0
    expect_key: str = ""
    response_text: str = ""
    reply_radio_id: str = ""
    reply_js8_instance_id: str = ""
    auto_reply_enabled: bool = False
    unattended_auto_reply_enabled: bool = False
    msg_auth_sign_enabled: bool = False
    msg_auth_sign_callsign: str = ""
    msg_auth_include_datecode: bool = False
    msg_auth_datecode: str = ""


def _record_expect_management_audit(conn, *, entry_id: int, action: str, values: Mapping[str, Any]) -> None:
    try:
        conn.execute(
            """
            INSERT INTO js8_expect_management_audit
                (expect_entry_id, action, expect_key, source_radio_id, source_scope, js8_instance_id,
                 enabled, auto_reply_enabled, import_source, detail_json, created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(entry_id or 0),
                str(action or "").strip() or "updated",
                str(values.get("expect_key", "") or "").strip().upper(),
                str(values.get("source_radio_id", "") or "").strip(),
                str(values.get("source_scope", "") or "").strip(),
                str(values.get("js8_instance_id", "") or "").strip(),
                1 if bool(values.get("enabled", False)) else 0,
                1 if bool(values.get("auto_reply_enabled", False)) else 0,
                str(values.get("import_source", "") or "").strip(),
                json.dumps(dict(values), sort_keys=True, default=str),
                time.time(),
            ),
        )
    except Exception:
        pass


def _json_list(values: Any) -> str:
    if values is None:
        return "[]"
    if isinstance(values, str):
        items = [part.strip().upper() for part in values.replace(";", ",").split(",") if part.strip()]
    else:
        try:
            items = [str(part or "").strip().upper() for part in values if str(part or "").strip()]
        except Exception:
            items = []
    return json.dumps(list(dict.fromkeys(items)), separators=(",", ":"))


def _json_load_list(value: object) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return []
    return [str(item or "").strip().upper() for item in parsed if str(item or "").strip()]


def _norm_call(value: object) -> str:
    return str(value or "").strip().upper().lstrip("@")


def _norm_group(value: object) -> str:
    text = str(value or "").strip().upper()
    if text and not text.startswith("@"):
        text = f"@{text}"
    return text


def _matches_group(configured: object, target_group: str) -> bool:
    left = _norm_group(configured)
    right = _norm_group(target_group)
    return bool(left and right and left == right)


def _source_matches(entry: Mapping[str, Any], source_radio_id: str, js8_instance_id: str) -> bool:
    scope = str(entry.get("source_scope", "") or "all").strip().lower()
    entry_radio = str(entry.get("source_radio_id", "") or "").strip()
    entry_js8 = str(entry.get("js8_instance_id", "") or "").strip()
    if scope == "radio" and entry_radio and source_radio_id and entry_radio != source_radio_id:
        return False
    if scope == "radio" and entry_radio and not source_radio_id:
        return False
    if entry_js8 and js8_instance_id and entry_js8 != js8_instance_id:
        return False
    if entry_js8 and not js8_instance_id:
        return False
    return True


def _record_expect_runtime_audit(
    conn,
    *,
    event_id: str,
    expect_entry_id: int,
    expect_key: str,
    source_radio_id: str,
    source_js8_instance_id: str,
    requesting_callsign: str,
    target_group: str,
    decision: str,
    reason: str,
    reply_radio_id: str = "",
    reply_js8_instance_id: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO js8_expect_audit
            (event_id, expect_entry_id, expect_key, source_radio_id, source_js8_instance_id,
             requesting_callsign, target_group, decision, reason,
             reply_radio_id, reply_js8_instance_id, created_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(event_id or ""),
            int(expect_entry_id or 0),
            str(expect_key or "").strip().upper(),
            str(source_radio_id or ""),
            str(source_js8_instance_id or ""),
            _norm_call(requesting_callsign),
            _norm_group(target_group),
            str(decision or ""),
            str(reason or ""),
            str(reply_radio_id or ""),
            str(reply_js8_instance_id or ""),
            time.time(),
        ),
    )


def save_expect_allow_policy(
    values: Mapping[str, Any],
    *,
    db_path: Optional[Path] = None,
) -> ExpectAllowPolicySaveResult:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    name = str(values.get("name", "") or "").strip()
    if not name:
        raise ValueError("name is required")
    now = time.time()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        explicit_id = int(values.get("id", 0) or 0)
        row = conn.execute("SELECT id FROM js8_expect_allow_policies WHERE id=?", (explicit_id,)).fetchone() if explicit_id > 0 else None
        if row is None:
            row = conn.execute(
                "SELECT id FROM js8_expect_allow_policies WHERE name=? ORDER BY id ASC LIMIT 1",
                (name,),
            ).fetchone()
        payload = {
            "name": name,
            "allowed_callsigns_json": _json_list(values.get("allowed_callsigns")),
            "allowed_groups_json": _json_list(values.get("allowed_groups")),
            "blocked_callsigns_json": _json_list(values.get("blocked_callsigns")),
            "source_scope": str(values.get("source_scope", "") or "all").strip() or "all",
            "source_radio_ids_json": _json_list(values.get("source_radio_ids")),
            "enabled": 1 if bool(values.get("enabled", True)) else 0,
            "import_source": str(values.get("import_source", "") or "fio-settings").strip(),
            "notes": str(values.get("notes", "") or "").strip(),
            "updated_ts": now,
        }
        if row:
            row_id = int(row[0])
            conn.execute(
                """
                UPDATE js8_expect_allow_policies
                SET name=?, allowed_callsigns_json=?, allowed_groups_json=?, blocked_callsigns_json=?,
                    source_scope=?, source_radio_ids_json=?, enabled=?, import_source=?, notes=?, updated_ts=?
                WHERE id=?
                """,
                (
                    payload["name"],
                    payload["allowed_callsigns_json"],
                    payload["allowed_groups_json"],
                    payload["blocked_callsigns_json"],
                    payload["source_scope"],
                    payload["source_radio_ids_json"],
                    payload["enabled"],
                    payload["import_source"],
                    payload["notes"],
                    payload["updated_ts"],
                    row_id,
                ),
            )
            created = False
        else:
            payload["created_ts"] = now
            keys = list(payload.keys())
            placeholders = ",".join("?" for _ in keys)
            conn.execute(
                f"INSERT INTO js8_expect_allow_policies ({','.join(keys)}) VALUES ({placeholders})",
                tuple(payload[key] for key in keys),
            )
            row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            created = True
        conn.commit()
    finally:
        conn.close()
    return ExpectAllowPolicySaveResult(id=row_id, created=created, name=name, enabled=bool(payload["enabled"]))


def list_expect_allow_policies(
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
            SELECT id, name, allowed_callsigns_json, allowed_groups_json, blocked_callsigns_json,
                   source_scope, source_radio_ids_json, enabled, import_source, notes
            FROM js8_expect_allow_policies
            {where}
            ORDER BY name ASC, id ASC
            """
        ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "keys"):
            raw = {key: row[key] for key in row.keys()}
        else:
            raw = {
                "id": row[0],
                "name": row[1],
                "allowed_callsigns_json": row[2],
                "allowed_groups_json": row[3],
                "blocked_callsigns_json": row[4],
                "source_scope": row[5],
                "source_radio_ids_json": row[6],
                "enabled": row[7],
                "import_source": row[8],
                "notes": row[9],
            }
        raw["allowed_callsigns"] = _json_load_list(raw.get("allowed_callsigns_json"))
        raw["allowed_groups"] = _json_load_list(raw.get("allowed_groups_json"))
        raw["blocked_callsigns"] = _json_load_list(raw.get("blocked_callsigns_json"))
        raw["source_radio_ids"] = _json_load_list(raw.get("source_radio_ids_json"))
        out.append(raw)
    return out


def delete_expect_allow_policy(policy_id: int, *, db_path: Optional[Path] = None) -> bool:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        conn.execute("UPDATE js8_expect_entries SET allow_policy_id=NULL WHERE allow_policy_id=?", (int(policy_id),))
        cur = conn.execute("DELETE FROM js8_expect_allow_policies WHERE id=?", (int(policy_id),))
        conn.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        conn.close()


def list_expect_entries(
    *,
    db_path: Optional[Path] = None,
    enabled_only: bool = False,
) -> list[dict[str, Any]]:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        where = "WHERE COALESCE(e.enabled, 0) != 0" if enabled_only else ""
        rows = conn.execute(
            f"""
            SELECT e.id, e.source_radio_id, e.source_scope, e.js8_instance_id, e.allow_policy_id,
                   p.name AS allow_policy_name, e.expect_key, e.response_text,
                   e.msg_auth_sign_enabled, e.msg_auth_sign_callsign, e.msg_auth_include_datecode, e.msg_auth_datecode,
                   e.allowed_callsigns_json, e.allowed_groups_json, e.allow_any,
                   e.blocked_callsigns_json, e.max_replies, e.cooldown_seconds, e.tx_speed,
                   e.auto_tx_schedule, e.auto_reply_enabled, e.unattended_auto_reply_enabled,
                   e.enabled, e.import_source, e.created_ts, e.updated_ts
            FROM js8_expect_entries e
            LEFT JOIN js8_expect_allow_policies p ON p.id=e.allow_policy_id
            {where}
            ORDER BY COALESCE(e.enabled, 0) DESC, e.expect_key ASC, e.id ASC
            """
        ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "keys"):
            raw = {key: row[key] for key in row.keys()}
        else:
            raw = {
                "id": row[0],
                "source_radio_id": row[1],
                "source_scope": row[2],
                "js8_instance_id": row[3],
                "allow_policy_id": row[4],
                "allow_policy_name": row[5],
                "expect_key": row[6],
                "response_text": row[7],
                "msg_auth_sign_enabled": row[8],
                "msg_auth_sign_callsign": row[9],
                "msg_auth_include_datecode": row[10],
                "msg_auth_datecode": row[11],
                "allowed_callsigns_json": row[12],
                "allowed_groups_json": row[13],
                "allow_any": row[14],
                "blocked_callsigns_json": row[15],
                "max_replies": row[16],
                "cooldown_seconds": row[17],
                "tx_speed": row[18],
                "auto_tx_schedule": row[19],
                "auto_reply_enabled": row[20],
                "unattended_auto_reply_enabled": row[21],
                "enabled": row[22],
                "import_source": row[23],
                "created_ts": row[24],
                "updated_ts": row[25],
            }
        raw["allowed_callsigns"] = _json_load_list(raw.get("allowed_callsigns_json"))
        raw["allowed_groups"] = _json_load_list(raw.get("allowed_groups_json"))
        raw["blocked_callsigns"] = _json_load_list(raw.get("blocked_callsigns_json"))
        out.append(raw)
    return out


def update_expect_entry_controls(
    entry_id: int,
    values: Mapping[str, Any],
    *,
    db_path: Optional[Path] = None,
) -> ExpectEntryUpdateResult:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        row = conn.execute(
            """
            SELECT id, expect_key, source_radio_id, source_scope, js8_instance_id, import_source
            FROM js8_expect_entries
            WHERE id=?
            """,
            (int(entry_id),),
        ).fetchone()
        if row is None:
            raise ValueError("expect entry not found")
        if hasattr(row, "keys"):
            audit_base = {key: row[key] for key in row.keys()}
        else:
            audit_base = {
                "id": row[0],
                "expect_key": row[1],
                "source_radio_id": row[2],
                "source_scope": row[3],
                "js8_instance_id": row[4],
                "import_source": row[5],
            }
        enabled = 1 if bool(values.get("enabled", False)) else 0
        auto_reply = 1 if bool(values.get("auto_reply_enabled", False)) else 0
        unattended = 1 if bool(values.get("unattended_auto_reply_enabled", False)) else 0
        allow_policy_raw = values.get("allow_policy_id")
        allow_policy_id = int(allow_policy_raw or 0) if str(allow_policy_raw or "").strip() else None
        conn.execute(
            """
            UPDATE js8_expect_entries
            SET allow_policy_id=?, allowed_callsigns_json=?, allowed_groups_json=?, allow_any=?,
                blocked_callsigns_json=?, max_replies=?, cooldown_seconds=?,
                auto_reply_enabled=?, unattended_auto_reply_enabled=?, enabled=?, updated_ts=?
            WHERE id=?
            """,
            (
                allow_policy_id,
                _json_list(values.get("allowed_callsigns")),
                _json_list(values.get("allowed_groups")),
                1 if bool(values.get("allow_any", False)) else 0,
                _json_list(values.get("blocked_callsigns")),
                int(values.get("max_replies", 1) or 1),
                int(values.get("cooldown_seconds", 0) or 0),
                auto_reply,
                unattended,
                enabled,
                time.time(),
                int(entry_id),
            ),
        )
        audit_values = dict(audit_base)
        audit_values.update(values)
        audit_values["enabled"] = enabled
        audit_values["auto_reply_enabled"] = auto_reply
        _record_expect_management_audit(conn, entry_id=int(entry_id), action="controls-updated", values=audit_values)
        conn.commit()
    finally:
        conn.close()
    return ExpectEntryUpdateResult(id=int(entry_id), enabled=bool(enabled), auto_reply_enabled=bool(auto_reply))


def delete_expect_entry(entry_id: int, *, db_path: Optional[Path] = None) -> bool:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        row = conn.execute(
            """
            SELECT expect_key, source_radio_id, source_scope, js8_instance_id, enabled, auto_reply_enabled, import_source
            FROM js8_expect_entries
            WHERE id=?
            """,
            (int(entry_id),),
        ).fetchone()
        cur = conn.execute("DELETE FROM js8_expect_entries WHERE id=?", (int(entry_id),))
        if int(cur.rowcount or 0) > 0 and row is not None:
            if hasattr(row, "keys"):
                values = {key: row[key] for key in row.keys()}
            else:
                values = {
                    "expect_key": row[0],
                    "source_radio_id": row[1],
                    "source_scope": row[2],
                    "js8_instance_id": row[3],
                    "enabled": row[4],
                    "auto_reply_enabled": row[5],
                    "import_source": row[6],
                }
            _record_expect_management_audit(conn, entry_id=int(entry_id), action="deleted", values=values)
        conn.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        conn.close()


def save_expect_entry(
    entry: Mapping[str, Any],
    *,
    db_path: Optional[Path] = None,
) -> ExpectEntrySaveResult:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    expect_key = str(entry.get("expect_key", "") or "").strip().upper()
    if not expect_key:
        raise ValueError("expect_key is required")
    source_radio_id = str(entry.get("source_radio_id", "") or "").strip()
    source_scope = str(entry.get("source_scope", "") or "radio").strip() or "radio"
    js8_instance_id = str(entry.get("js8_instance_id", "") or "").strip()
    enabled = 1 if bool(entry.get("enabled", False)) else 0
    auto_reply = 1 if bool(entry.get("auto_reply_enabled", False)) else 0
    unattended = 1 if bool(entry.get("unattended_auto_reply_enabled", False)) else 0

    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        row = conn.execute(
            """
            SELECT id
            FROM js8_expect_entries
            WHERE expect_key=?
              AND COALESCE(source_radio_id, '')=?
              AND COALESCE(js8_instance_id, '')=?
            ORDER BY id ASC
            LIMIT 1
            """,
            (expect_key, source_radio_id, js8_instance_id),
        ).fetchone()
        values = {
            "source_radio_id": source_radio_id,
            "source_scope": source_scope,
            "js8_instance_id": js8_instance_id,
            "allow_policy_id": entry.get("allow_policy_id"),
            "expect_key": expect_key,
            "response_text": str(entry.get("response_text", "") or "").strip(),
            "msg_auth_sign_enabled": 1 if bool(entry.get("msg_auth_sign_enabled", False)) else 0,
            "msg_auth_sign_callsign": str(entry.get("msg_auth_sign_callsign", "") or "").strip().upper(),
            "msg_auth_include_datecode": 1 if bool(entry.get("msg_auth_include_datecode", False)) else 0,
            "msg_auth_datecode": str(entry.get("msg_auth_datecode", "") or "").strip().upper(),
            "allowed_callsigns_json": _json_list(entry.get("allowed_callsigns")),
            "allowed_groups_json": _json_list(entry.get("allowed_groups")),
            "allow_any": 1 if bool(entry.get("allow_any", False)) else 0,
            "blocked_callsigns_json": _json_list(entry.get("blocked_callsigns")),
            "max_replies": int(entry.get("max_replies", 1) or 1),
            "cooldown_seconds": int(entry.get("cooldown_seconds", 0) or 0),
            "tx_speed": str(entry.get("tx_speed", "") or "").strip(),
            "auto_tx_schedule": str(entry.get("auto_tx_schedule", "") or "").strip(),
            "auto_reply_enabled": auto_reply,
            "unattended_auto_reply_enabled": unattended,
            "enabled": enabled,
            "import_source": str(entry.get("import_source", "") or "fio-compose").strip(),
            "updated_ts": now,
        }
        if row:
            row_id = int(row[0])
            assignments = ", ".join(f"{key}=?" for key in values.keys())
            conn.execute(
                f"UPDATE js8_expect_entries SET {assignments} WHERE id=?",
                tuple(values.values()) + (row_id,),
            )
            created = False
        else:
            values["created_ts"] = now
            keys = list(values.keys())
            placeholders = ",".join("?" for _ in keys)
            conn.execute(
                f"INSERT INTO js8_expect_entries ({','.join(keys)}) VALUES ({placeholders})",
                tuple(values[key] for key in keys),
            )
            row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            created = True
        audit_action = "created" if created else "updated"
        if created and str(values.get("import_source", "") or "").startswith("js8spotter"):
            audit_action = "imported"
        _record_expect_management_audit(conn, entry_id=row_id, action=audit_action, values=values)
        conn.commit()
    finally:
        conn.close()
    return ExpectEntrySaveResult(
        id=row_id,
        created=created,
        expect_key=expect_key,
        enabled=bool(enabled),
        auto_reply_enabled=bool(auto_reply),
    )


def list_expect_management_audit(
    *,
    db_path: Optional[Path] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        rows = conn.execute(
            """
            SELECT id, expect_entry_id, action, expect_key, source_radio_id, source_scope,
                   js8_instance_id, enabled, auto_reply_enabled, import_source, detail_json, created_ts
            FROM js8_expect_management_audit
            ORDER BY created_ts DESC, id DESC
            LIMIT ?
            """,
            (max(1, min(500, int(limit or 50))),),
        ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "keys"):
            raw = {key: row[key] for key in row.keys()}
        else:
            raw = {
                "id": row[0],
                "expect_entry_id": row[1],
                "action": row[2],
                "expect_key": row[3],
                "source_radio_id": row[4],
                "source_scope": row[5],
                "js8_instance_id": row[6],
                "enabled": row[7],
                "auto_reply_enabled": row[8],
                "import_source": row[9],
                "detail_json": row[10],
                "created_ts": row[11],
            }
        try:
            raw["detail"] = json.loads(str(raw.get("detail_json") or "{}"))
        except Exception:
            raw["detail"] = {}
        out.append(raw)
    return out


def list_expect_runtime_audit(
    *,
    db_path: Optional[Path] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    conn = connect_sqlite(path)
    try:
        _ensure_js8_expect_tables(conn)
        rows = conn.execute(
            """
            SELECT id, event_id, expect_entry_id, expect_key, source_radio_id, source_js8_instance_id,
                   requesting_callsign, target_group, decision, reason,
                   reply_radio_id, reply_js8_instance_id, created_ts
            FROM js8_expect_audit
            ORDER BY created_ts DESC, id DESC
            LIMIT ?
            """,
            (max(1, min(500, int(limit or 50))),),
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
                    "event_id": row[1],
                    "expect_entry_id": row[2],
                    "expect_key": row[3],
                    "source_radio_id": row[4],
                    "source_js8_instance_id": row[5],
                    "requesting_callsign": row[6],
                    "target_group": row[7],
                    "decision": row[8],
                    "reason": row[9],
                    "reply_radio_id": row[10],
                    "reply_js8_instance_id": row[11],
                    "created_ts": row[12],
                }
            )
    return out


def evaluate_expect_request(
    *,
    expect_key: str,
    requesting_callsign: str,
    target_group: str = "",
    source_radio_id: object = "",
    js8_instance_id: object = "",
    event_id: str = "",
    db_path: Optional[Path] = None,
    write_audit: bool = True,
) -> ExpectEvaluationResult:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    key = str(expect_key or "").strip().upper()
    call = _norm_call(requesting_callsign)
    group = _norm_group(target_group)
    radio_id = str(source_radio_id or "").strip()
    js8_id = str(js8_instance_id or "").strip()
    if not key:
        result = ExpectEvaluationResult(decision="invalid", reason="Missing Expect form key.")
    elif not call:
        result = ExpectEvaluationResult(decision="invalid", reason="Missing requesting callsign.", expect_key=key)
    else:
        entries = [row for row in list_expect_entries(db_path=path, enabled_only=False) if str(row.get("expect_key", "") or "").upper() == key]
        policies = {int(row.get("id", 0) or 0): row for row in list_expect_allow_policies(db_path=path, enabled_only=False)}
        result = ExpectEvaluationResult(decision="no-match", reason=f"No Expect entry for {key}.", expect_key=key)
        source_mismatch_seen = False
        disabled_seen = False
        for entry in entries:
            entry_id = int(entry.get("id", 0) or 0)
            if not _source_matches(entry, radio_id, js8_id):
                source_mismatch_seen = True
                continue
            if not bool(entry.get("enabled", False)):
                disabled_seen = True
                result = ExpectEvaluationResult(
                    decision="disabled",
                    reason=f"Expect entry {key} is present but disabled.",
                    expect_entry_id=entry_id,
                    expect_key=key,
                    response_text=str(entry.get("response_text", "") or ""),
                    msg_auth_sign_enabled=bool(entry.get("msg_auth_sign_enabled", False)),
                    msg_auth_sign_callsign=str(entry.get("msg_auth_sign_callsign", "") or ""),
                    msg_auth_include_datecode=bool(entry.get("msg_auth_include_datecode", False)),
                    msg_auth_datecode=str(entry.get("msg_auth_datecode", "") or ""),
                    reply_radio_id=str(entry.get("source_radio_id", "") or ""),
                    reply_js8_instance_id=str(entry.get("js8_instance_id", "") or ""),
                    auto_reply_enabled=bool(entry.get("auto_reply_enabled", False)),
                    unattended_auto_reply_enabled=bool(entry.get("unattended_auto_reply_enabled", False)),
                )
                continue
            policy = policies.get(int(entry.get("allow_policy_id", 0) or 0), {})
            if policy and not bool(policy.get("enabled", True)):
                result = ExpectEvaluationResult(
                    decision="blocked",
                    reason=f"Allow policy {policy.get('name', '') or entry.get('allow_policy_name', '') or ''} is disabled.".strip(),
                    expect_entry_id=entry_id,
                    expect_key=key,
                )
                continue
            blocked = {_norm_call(value) for value in entry.get("blocked_callsigns", [])}
            blocked.update(_norm_call(value) for value in policy.get("blocked_callsigns", []) if policy)
            if call in blocked:
                result = ExpectEvaluationResult(decision="blocked", reason=f"{call} is blocked.", expect_entry_id=entry_id, expect_key=key)
                break
            allowed_calls = {_norm_call(value) for value in entry.get("allowed_callsigns", [])}
            allowed_calls.update(_norm_call(value) for value in policy.get("allowed_callsigns", []) if policy)
            allowed_groups = list(entry.get("allowed_groups", []) or [])
            if policy:
                allowed_groups.extend(policy.get("allowed_groups", []) or [])
            allow_any = bool(entry.get("allow_any", False))
            call_allowed = bool(call and call in allowed_calls)
            group_allowed = bool(group and any(_matches_group(value, group) for value in allowed_groups))
            if not allow_any and not call_allowed and not group_allowed:
                result = ExpectEvaluationResult(
                    decision="blocked",
                    reason="Request did not match allowed callsigns or groups.",
                    expect_entry_id=entry_id,
                    expect_key=key,
                )
                continue
            auto_reply = bool(entry.get("auto_reply_enabled", False))
            result = ExpectEvaluationResult(
                decision="reply-ready" if auto_reply else "matched-manual-review",
                reason="Matched enabled Expect entry." if auto_reply else "Matched Expect entry; auto-reply is not enabled.",
                expect_entry_id=entry_id,
                expect_key=key,
                response_text=str(entry.get("response_text", "") or ""),
                msg_auth_sign_enabled=bool(entry.get("msg_auth_sign_enabled", False)),
                msg_auth_sign_callsign=str(entry.get("msg_auth_sign_callsign", "") or ""),
                msg_auth_include_datecode=bool(entry.get("msg_auth_include_datecode", False)),
                msg_auth_datecode=str(entry.get("msg_auth_datecode", "") or ""),
                reply_radio_id=str(entry.get("source_radio_id", "") or radio_id),
                reply_js8_instance_id=str(entry.get("js8_instance_id", "") or js8_id),
                auto_reply_enabled=auto_reply,
                unattended_auto_reply_enabled=bool(entry.get("unattended_auto_reply_enabled", False)),
            )
            break
        if result.decision == "no-match" and source_mismatch_seen:
            result = ExpectEvaluationResult(decision="source-mismatch", reason=f"Expect entry for {key} exists on another source.", expect_key=key)
        elif result.decision == "no-match" and disabled_seen:
            result = ExpectEvaluationResult(decision="disabled", reason=f"Expect entry for {key} exists but is disabled.", expect_key=key)
    if write_audit:
        conn = connect_sqlite(path)
        try:
            _ensure_js8_expect_tables(conn)
            _record_expect_runtime_audit(
                conn,
                event_id=event_id,
                expect_entry_id=result.expect_entry_id,
                expect_key=result.expect_key or key,
                source_radio_id=str(source_radio_id or ""),
                source_js8_instance_id=str(js8_instance_id or ""),
                requesting_callsign=requesting_callsign,
                target_group=target_group,
                decision=result.decision,
                reason=result.reason,
                reply_radio_id=result.reply_radio_id,
                reply_js8_instance_id=result.reply_js8_instance_id,
            )
            conn.commit()
        finally:
            conn.close()
    return result
