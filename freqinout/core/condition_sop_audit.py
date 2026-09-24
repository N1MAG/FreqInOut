from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from freqinout.core.observation_projection import utc_now_iso
from freqinout.core.sqlite_utils import connect_sqlite


@dataclass(frozen=True)
class ConditionSopAuditSummary:
    latest_status: str = ""
    latest_message: str = ""
    latest_group: str = ""
    latest_condition_level: int | None = None
    latest_created_utc: str = ""
    applied_count: int = 0
    blocked_count: int = 0
    prompt_count: int = 0


@dataclass(frozen=True)
class ConditionSopAuditDisplay:
    text: str = ""
    severity: str = "none"


def condition_sop_audit_display(summary: ConditionSopAuditSummary) -> ConditionSopAuditDisplay:
    status = _text(getattr(summary, "latest_status", "")).lower()
    if not status:
        return ConditionSopAuditDisplay()
    group = _text(getattr(summary, "latest_group", "")) or "Group"
    level = getattr(summary, "latest_condition_level", None)
    level_text = f" L{level}" if level is not None else ""
    message = _text(getattr(summary, "latest_message", "")) or status
    message = " ".join(message.split())
    if len(message) > 72:
        message = f"{message[:69].rstrip()}..."
    if status == "applied":
        return ConditionSopAuditDisplay(
            text=f"SOP automation: applied {group}{level_text}. {message}",
            severity="ok",
        )
    if status in {"blocked", "failed"}:
        return ConditionSopAuditDisplay(
            text=f"SOP automation needs review: {group}{level_text}. {message}",
            severity="warning",
        )
    if status in {"prompt", "suggest", "planned"}:
        return ConditionSopAuditDisplay(
            text=f"SOP automation ready for review: {group}{level_text}. {message}",
            severity="review",
        )
    return ConditionSopAuditDisplay(
        text=f"SOP automation: {status} {group}{level_text}. {message}",
        severity="review",
    )


def ensure_condition_sop_audit_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS condition_sop_invocation_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_utc TEXT NOT NULL,
            event TEXT NOT NULL,
            decision TEXT,
            operating_group TEXT,
            condition_level INTEGER,
            sop_profile_id TEXT,
            sop_profile_name TEXT,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_condition_sop_audit_created
        ON condition_sop_invocation_audit(created_utc)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_condition_sop_audit_group_level
        ON condition_sop_invocation_audit(operating_group, condition_level, created_utc)
        """
    )


def append_condition_sop_invocation_audit(
    db_path: str | Path,
    payload: Mapping[str, Any],
    *,
    status: str = "planned",
    created_utc: str | None = None,
) -> int:
    stamp = str(created_utc or utc_now_iso()).strip()
    event = str(payload.get("event") or "condition_sop_invocation").strip()
    conn = connect_sqlite(db_path)
    try:
        ensure_condition_sop_audit_schema(conn)
        with conn:
            cur = conn.execute(
                """
                INSERT INTO condition_sop_invocation_audit (
                    created_utc, event, decision, operating_group, condition_level,
                    sop_profile_id, sop_profile_name, status, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stamp,
                    event,
                    _text(payload.get("decision")),
                    _text(payload.get("operating_group")),
                    _int_or_none(payload.get("condition_level")),
                    _text(payload.get("sop_profile_id")),
                    _text(payload.get("sop_profile_name")),
                    str(status or "planned").strip() or "planned",
                    json.dumps(dict(payload), sort_keys=True),
                ),
            )
            return int(cur.lastrowid)
    finally:
        conn.close()


def list_condition_sop_invocation_audit(
    db_path: str | Path,
    *,
    limit: int = 50,
) -> tuple[Mapping[str, Any], ...]:
    conn = connect_sqlite(db_path)
    try:
        ensure_condition_sop_audit_schema(conn)
        rows = conn.execute(
            """
            SELECT id, created_utc, event, decision, operating_group, condition_level,
                   sop_profile_id, sop_profile_name, status, payload_json
            FROM condition_sop_invocation_audit
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit or 50)),),
        ).fetchall()
        out = []
        for row in rows:
            payload: dict[str, Any]
            try:
                payload = json.loads(row[9] or "{}")
            except Exception:
                payload = {}
            out.append(
                {
                    "id": int(row[0]),
                    "created_utc": row[1],
                    "event": row[2],
                    "decision": row[3],
                    "operating_group": row[4],
                    "condition_level": row[5],
                    "sop_profile_id": row[6],
                    "sop_profile_name": row[7],
                    "status": row[8],
                    "payload": payload,
                }
            )
        return tuple(out)
    finally:
        conn.close()


def condition_sop_audit_summary(
    db_path: str | Path,
    *,
    limit: int = 25,
) -> ConditionSopAuditSummary:
    rows = list_condition_sop_invocation_audit(db_path, limit=limit)
    if not rows:
        return ConditionSopAuditSummary()
    latest = rows[0]
    status_counts: dict[str, int] = {}
    for row in rows:
        status = _text(row.get("status")).lower()
        if status:
            status_counts[status] = status_counts.get(status, 0) + 1
    payload = latest.get("payload")
    if not isinstance(payload, Mapping):
        payload = {}
    return ConditionSopAuditSummary(
        latest_status=_text(latest.get("status")),
        latest_message=_text(payload.get("execution_message") or payload.get("message")),
        latest_group=_text(latest.get("operating_group")),
        latest_condition_level=_int_or_none(latest.get("condition_level")),
        latest_created_utc=_text(latest.get("created_utc")),
        applied_count=int(status_counts.get("applied", 0)),
        blocked_count=int(status_counts.get("blocked", 0) + status_counts.get("failed", 0)),
        prompt_count=int(status_counts.get("prompt", 0) + status_counts.get("suggest", 0)),
    )


def condition_sop_audit_observability_item(db_path: str | Path) -> Mapping[str, object] | None:
    summary = condition_sop_audit_summary(db_path, limit=10)
    display = condition_sop_audit_display(summary)
    if not display.text:
        return None
    status = _text(summary.latest_status).lower()
    group = summary.latest_group or "Station-wide"
    if status in {"blocked", "failed"}:
        state = "Needs Review"
        severity = "warning"
        action = "Review SOP automation and RF Guard before applying this condition change"
        is_issue = True
    elif status == "applied":
        state = "Applied"
        severity = "ok"
        action = "No action needed"
        is_issue = False
    else:
        state = "Review"
        severity = "info"
        action = "Review the suggested SOP condition change"
        is_issue = False
    return {
        "key": f"condition-sop-automation:{summary.latest_created_utc or status}",
        "scope": group,
        "dependency": "SOP Automation",
        "state": state,
        "severity": severity,
        "action": action,
        "last_issue": display.text,
        "issue_since": "",
        "cooldown": "",
        "last_check": summary.latest_created_utc,
        "last_check_ts": 0.0,
        "last_duration": "",
        "failures": 1 if is_issue else 0,
        "slow": 0,
        "is_issue": is_issue,
        "group": "condition_sop",
    }


def _text(value: object) -> str:
    return str(value or "").strip()


def _int_or_none(value: object) -> int | None:
    try:
        return int(value)
    except Exception:
        return None
