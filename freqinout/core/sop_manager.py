from __future__ import annotations

import datetime as dt
import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from freqinout.core.config_paths import get_config_dir
from freqinout.core.local_ops_store import get_all_operators
from freqinout.core.logger import log
from freqinout.core.propagation_outcome_ingest import STATE_TO_FEMA_REGION


CONFIG_DIR = get_config_dir() / "config"
NETS_DB = CONFIG_DIR / "freqinout_nets.db"


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso_utc(value: str | None) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return None


def _parse_hhmm(value: str, default_hhmm: str = "00:00") -> Tuple[int, int]:
    text = (value or default_hhmm).strip()
    try:
        hh, mm = text.split(":")
        h = max(0, min(23, int(hh)))
        m = max(0, min(59, int(mm)))
        return h, m
    except Exception:
        return 0, 0


def _day_name_from_utc(ts: dt.datetime) -> str:
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return days[ts.weekday()]


def _table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        return {str(row[1] or "").strip().lower() for row in cur.fetchall() if row and len(row) > 1}
    except Exception:
        return set()


def _norm_state(value: Any) -> str:
    txt = str(value or "").strip().upper()
    return txt[:2] if txt else ""


def _norm_region(value: Any) -> str:
    txt = str(value or "").strip().upper()
    if not txt:
        return ""
    if txt.startswith("R") and len(txt) == 2 and txt[1].isdigit():
        txt = f"R0{txt[1]}"
    if txt.startswith("R") and len(txt) == 3 and txt[1:].isdigit():
        txt = f"R{txt[1:].zfill(2)}"
    if txt in {f"R{idx:02d}" for idx in range(1, 11)}:
        return txt
    return ""


def _norm_sitrep(raw: Any) -> str:
    txt = str(raw or "").strip().upper()
    if txt in {"GREEN", "YELLOW", "RED"}:
        return txt
    return "UNKNOWN"


def _norm_group(value: Any) -> str:
    txt = str(value or "").strip().upper()
    if not txt:
        return ""
    for ch in "\"'[]{}()":
        txt = txt.replace(ch, "")
    txt = " ".join(txt.split())
    return txt.strip()


def _norm_trusted_filter(value: Any) -> str:
    txt = str(value or "").strip().upper()
    if txt in {"TRUSTED", "UNTRUSTED"}:
        return txt
    return ""


def _is_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return int(value) != 0
    txt = str(value).strip().lower()
    if not txt:
        return False
    return txt in {"1", "true", "yes", "y", "on", "enabled"}


def _trusted_label(value: Any) -> str:
    try:
        return "TRUSTED" if int(value or 0) > 0 else "UNTRUSTED"
    except Exception:
        txt = str(value or "").strip().lower()
        return "TRUSTED" if txt in {"true", "yes", "y", "trusted"} else "UNTRUSTED"


def _group_tokens_from_json(raw: Any) -> Set[str]:
    out: Set[str] = set()
    if raw is None:
        return out
    text = str(raw).strip()
    if not text:
        return out

    def _add(val: Any) -> None:
        token = _norm_group(val)
        if token:
            out.add(token)

    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None

    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, str):
                _add(item)
            elif isinstance(item, dict):
                for k, v in item.items():
                    if isinstance(v, str):
                        _add(v)
                    if bool(v):
                        _add(k)
    elif isinstance(parsed, dict):
        for k, v in parsed.items():
            if isinstance(v, str):
                _add(v)
            if bool(v):
                _add(k)
    elif isinstance(parsed, str):
        inner = parsed.strip()
        if inner:
            try:
                nested = json.loads(inner)
            except Exception:
                nested = None
            if isinstance(nested, list):
                for item in nested:
                    if isinstance(item, str):
                        _add(item)
                    elif isinstance(item, dict):
                        for k, v in item.items():
                            if isinstance(v, str):
                                _add(v)
                            if bool(v):
                                _add(k)
            elif isinstance(nested, dict):
                for k, v in nested.items():
                    if isinstance(v, str):
                        _add(v)
                    if bool(v):
                        _add(k)
            else:
                text = inner

    for chunk in str(text).replace(";", ",").replace("|", ",").split(","):
        _add(chunk)
    return out


def _collect_group_tokens(*values: Any) -> Set[str]:
    out: Set[str] = set()
    if len(values) >= 4:
        raw_json = values[3]
    else:
        raw_json = ""
    for raw in values[:3]:
        token = _norm_group(raw)
        if token:
            out.add(token)
    out.update(_group_tokens_from_json(raw_json))
    return out


def _passes_geo_filter(state: str, *, state_filter: str, region_filter: str) -> bool:
    st = _norm_state(state)
    sf = _norm_state(state_filter)
    rf = _norm_region(region_filter)
    if sf and st != sf:
        return False
    if rf:
        if STATE_TO_FEMA_REGION.get(st, "") != rf:
            return False
    return True


class SOPManager:
    """
    Persistence and reminder logic for SOP profiles/actions.
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or NETS_DB
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_tables()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    @staticmethod
    def _normalize_day_utc(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "ALL"
        upper = raw.upper()
        if upper in {"ALL", "DAILY"}:
            return "ALL"
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        for day in days:
            if upper.startswith(day[:3].upper()):
                return day
        return "ALL"

    @staticmethod
    def _normalize_recurrence(value: Any) -> str:
        raw = str(value or "Weekly").strip().upper()
        if raw == "MONTHLY":
            raw = "PERIODIC"
        if raw in {"DAILY", "PERIODIC", "BI-WEEKLY", "WEEKLY"}:
            return "Bi-Weekly" if raw == "BI-WEEKLY" else raw.title()
        return "Weekly"

    @staticmethod
    def _normalize_month_weeks(value: Any) -> str:
        weeks: List[int] = []
        for token in str(value or "").split(","):
            token = token.strip()
            if not token:
                continue
            try:
                week = int(token)
            except Exception:
                continue
            if 1 <= week <= 5:
                weeks.append(week)
        return ",".join(str(w) for w in sorted(set(weeks)))

    @staticmethod
    def _normalize_hhmm(value: Any) -> str:
        h, m = _parse_hhmm(str(value or ""), default_hhmm="00:00")
        return f"{h:02d}:{m:02d}"

    @staticmethod
    def _normalize_frequency(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            return f"{float(text):.3f}"
        except Exception:
            return text

    def ensure_tables(self) -> None:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sop_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    operating_group TEXT NOT NULL,
                    secondary_group TEXT,
                    frequency TEXT NOT NULL,
                    sop_start_utc TEXT NOT NULL,
                    priority INTEGER DEFAULT 100,
                    active INTEGER DEFAULT 0,
                    window_hours INTEGER DEFAULT 12,
                    created_utc TEXT,
                    updated_utc TEXT
                )
                """
            )
            cur.execute("PRAGMA table_info(sop_profiles)")
            profile_cols = {str(row[1] or "").strip().lower() for row in cur.fetchall()}
            if "priority" not in profile_cols:
                cur.execute("ALTER TABLE sop_profiles ADD COLUMN priority INTEGER DEFAULT 100")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sop_profiles_name ON sop_profiles(name)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sop_profiles_active ON sop_profiles(active)")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sop_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    band TEXT,
                    frequency TEXT,
                    software TEXT NOT NULL,
                    action_key TEXT NOT NULL,
                    action_label TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    interval_hours INTEGER DEFAULT 3,
                    interval_minutes INTEGER,
                    interval_phase_minutes INTEGER DEFAULT 0,
                    description TEXT,
                    contact_rule TEXT DEFAULT 'none',
                    contact_target TEXT,
                    sort_order INTEGER DEFAULT 0
                )
                """
            )
            cur.execute("PRAGMA table_info(sop_actions)")
            action_cols = {row[1] for row in cur.fetchall()}
            if "band" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN band TEXT")
                action_cols.add("band")
            if "frequency" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN frequency TEXT")
                action_cols.add("frequency")
            if "contact_rule" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN contact_rule TEXT DEFAULT 'none'")
                action_cols.add("contact_rule")
            if "contact_target" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN contact_target TEXT")
                action_cols.add("contact_target")
            if "interval_minutes" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN interval_minutes INTEGER")
                action_cols.add("interval_minutes")
            if "interval_phase_minutes" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN interval_phase_minutes INTEGER DEFAULT 0")
                action_cols.add("interval_phase_minutes")
            if "sort_order" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN sort_order INTEGER DEFAULT 0")
                action_cols.add("sort_order")
            if "interval_hours" in action_cols and "interval_minutes" in action_cols and "interval_phase_minutes" in action_cols:
                # Legacy Local Net rows used fractional-hour intervals (e.g., 3:30) to express
                # stagger intent. Convert common quarter-hour offsets to interval+phase.
                try:
                    cur.execute(
                        """
                        UPDATE sop_actions
                        SET
                            interval_phase_minutes = (interval_minutes % 60),
                            interval_minutes = CAST(interval_minutes / 60 AS INTEGER) * 60,
                            interval_hours = CASE
                                WHEN CAST(interval_minutes / 60 AS INTEGER) < 1 THEN 1
                                ELSE CAST(interval_minutes / 60 AS INTEGER)
                            END
                        WHERE lower(trim(COALESCE(software, ''))) = 'local net'
                          AND (
                                lower(trim(COALESCE(contact_rule, ''))) = 'local_profile'
                                OR lower(trim(COALESCE(action_key, ''))) LIKE 'local_%'
                              )
                          AND COALESCE(interval_phase_minutes, 0) = 0
                          AND COALESCE(interval_minutes, 0) >= 120
                          AND (COALESCE(interval_minutes, 0) % 60) IN (15, 30, 45)
                        """
                    )
                    migrated = int(cur.rowcount or 0)
                    if migrated > 0:
                        log.info("SOP: migrated %d legacy local interval rows to interval+phase", migrated)
                except Exception as e:
                    log.debug("SOP: local interval migration skipped: %s", e)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sop_actions_profile ON sop_actions(profile_id)")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sop_action_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    action_id INTEGER NOT NULL,
                    last_completed_utc TEXT,
                    updated_utc TEXT
                )
                """
            )
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_sop_state_action ON sop_action_state(profile_id, action_id)"
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sop_schedule_layer (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    day_utc TEXT NOT NULL,
                    recurrence TEXT DEFAULT 'Weekly',
                    biweekly_offset_weeks INTEGER DEFAULT 0,
                    month_weeks TEXT,
                    band TEXT,
                    mode TEXT,
                    vfo TEXT,
                    frequency TEXT NOT NULL,
                    start_utc TEXT NOT NULL,
                    end_utc TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    updated_utc TEXT
                )
                """
            )
            cur.execute("PRAGMA table_info(sop_schedule_layer)")
            layer_cols = {str(row[1] or "").strip().lower() for row in cur.fetchall()}
            if "recurrence" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN recurrence TEXT DEFAULT 'Weekly'")
            if "biweekly_offset_weeks" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN biweekly_offset_weeks INTEGER DEFAULT 0")
            if "month_weeks" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN month_weeks TEXT")
            if "band" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN band TEXT")
            if "mode" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN mode TEXT")
            if "vfo" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN vfo TEXT")
            if "enabled" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN enabled INTEGER DEFAULT 1")
            if "sort_order" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN sort_order INTEGER DEFAULT 0")
            if "updated_utc" not in layer_cols:
                cur.execute("ALTER TABLE sop_schedule_layer ADD COLUMN updated_utc TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sop_layer_profile ON sop_schedule_layer(profile_id)")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_sop_layer_profile_day ON sop_schedule_layer(profile_id, day_utc, start_utc)"
            )
            conn.commit()
        finally:
            conn.close()

    def list_profiles(self) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT id, name, operating_group, secondary_group, frequency, sop_start_utc,
                       priority, active, window_hours, created_utc, updated_utc
                FROM sop_profiles
                ORDER BY name COLLATE NOCASE
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "id": r[0],
                    "name": r[1],
                    "operating_group": r[2] or "",
                    "secondary_group": r[3] or "",
                    "frequency": r[4] or "",
                    "sop_start_utc": r[5] or "00:00",
                    "priority": int(r[6] or 100),
                    "active": bool(r[7]),
                    "window_hours": int(r[8] or 12),
                    "created_utc": r[9] or "",
                    "updated_utc": r[10] or "",
                }
                for r in rows
            ]
        finally:
            conn.close()

    def get_profile(self, profile_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT id, name, operating_group, secondary_group, frequency, sop_start_utc,
                       priority, active, window_hours, created_utc, updated_utc
                FROM sop_profiles WHERE id = ?
                """,
                (profile_id,),
            )
            row = cur.fetchone()
            if not row:
                return None

            actions_cur = conn.execute(
                """
                SELECT a.id, a.band, a.frequency, a.software, a.action_key, a.action_label, a.enabled,
                       a.interval_hours, a.interval_minutes, a.interval_phase_minutes,
                       a.description, a.contact_rule, a.contact_target, a.sort_order,
                       s.last_completed_utc
                FROM sop_actions a
                LEFT JOIN sop_action_state s
                  ON s.profile_id = a.profile_id AND s.action_id = a.id
                WHERE a.profile_id = ?
                ORDER BY a.software, a.sort_order, a.id
                """,
                (profile_id,),
            )
            actions = []
            for ar in actions_cur.fetchall():
                actions.append(
                    {
                        "id": ar[0],
                        "band": ar[1] or "",
                        "frequency": ar[2] or "",
                        "software": ar[3],
                        "action_key": ar[4],
                        "action_label": ar[5],
                        "enabled": bool(ar[6]),
                        "interval_hours": int(ar[7] or 3),
                        "interval_minutes": int(ar[8] or (int(ar[7] or 3) * 60)),
                        "interval_phase_minutes": int(ar[9] or 0),
                        "description": ar[10] or "",
                        "contact_rule": ar[11] or "none",
                        "contact_target": ar[12] or "",
                        "sort_order": int(ar[13] or 0),
                        "last_completed_utc": ar[14] or "",
                    }
                )

            layer_cur = conn.execute(
                """
                SELECT
                    id,
                    day_utc,
                    recurrence,
                    biweekly_offset_weeks,
                    month_weeks,
                    band,
                    mode,
                    vfo,
                    frequency,
                    start_utc,
                    end_utc,
                    enabled,
                    sort_order,
                    updated_utc
                FROM sop_schedule_layer
                WHERE profile_id = ?
                ORDER BY sort_order, id
                """,
                (profile_id,),
            )
            schedule_layer = []
            for lr in layer_cur.fetchall():
                schedule_layer.append(
                    {
                        "id": int(lr[0] or 0),
                        "day_utc": str(lr[1] or "ALL"),
                        "recurrence": self._normalize_recurrence(lr[2]),
                        "biweekly_offset_weeks": int(lr[3] or 0),
                        "month_weeks": self._normalize_month_weeks(lr[4]),
                        "band": str(lr[5] or "").strip().upper(),
                        "mode": str(lr[6] or "").strip().upper(),
                        "vfo": str(lr[7] or "").strip().upper(),
                        "frequency": str(lr[8] or "").strip(),
                        "start_utc": str(lr[9] or ""),
                        "end_utc": str(lr[10] or ""),
                        "enabled": bool(lr[11]),
                        "sort_order": int(lr[12] or 0),
                        "updated_utc": str(lr[13] or ""),
                    }
                )

            return {
                "id": row[0],
                "name": row[1],
                "operating_group": row[2] or "",
                "secondary_group": row[3] or "",
                "frequency": row[4] or "",
                "sop_start_utc": row[5] or "00:00",
                "priority": int(row[6] or 100),
                "active": bool(row[7]),
                "window_hours": int(row[8] or 12),
                "created_utc": row[9] or "",
                "updated_utc": row[10] or "",
                "actions": actions,
                "schedule_layer": schedule_layer,
            }
        finally:
            conn.close()

    def save_profile(
        self,
        payload: Dict[str, Any],
        actions: List[Dict[str, Any]],
        schedule_layer: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        now_iso = _utc_now_iso()
        conn = self._connect()
        try:
            with conn:
                profile_id = int(payload.get("id") or 0)
                row = (
                    (payload.get("name") or "").strip(),
                    (payload.get("operating_group") or "").strip(),
                    (payload.get("secondary_group") or "").strip(),
                    (payload.get("frequency") or "").strip(),
                    (payload.get("sop_start_utc") or "00:00").strip(),
                    int(payload.get("priority") or 100),
                    1 if payload.get("active") else 0,
                    int(payload.get("window_hours") or 12),
                )
                if profile_id > 0:
                    conn.execute(
                        """
                        UPDATE sop_profiles
                        SET name=?, operating_group=?, secondary_group=?, frequency=?, sop_start_utc=?,
                            priority=?, active=?, window_hours=?, updated_utc=?
                        WHERE id=?
                        """,
                        (*row, now_iso, profile_id),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO sop_profiles
                            (name, operating_group, secondary_group, frequency, sop_start_utc,
                             priority, active, window_hours, created_utc, updated_utc)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (*row, now_iso, now_iso),
                    )
                    profile_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

                existing = {
                    int(r[0])
                    for r in conn.execute("SELECT id FROM sop_actions WHERE profile_id = ?", (profile_id,)).fetchall()
                }
                kept: set[int] = set()

                for idx, action in enumerate(actions):
                    action_id = int(action.get("id") or 0)
                    interval_minutes = int(action.get("interval_minutes") or 0)
                    if interval_minutes <= 0:
                        interval_minutes = int(action.get("interval_hours") or 3) * 60
                    interval_phase_minutes = max(0, int(action.get("interval_phase_minutes") or 0))
                    interval_phase_minutes = interval_phase_minutes % max(1, interval_minutes)
                    interval_hours_legacy = max(1, int((interval_minutes + 59) // 60))
                    vals = (
                        profile_id,
                        (action.get("band") or "").strip().upper(),
                        (action.get("frequency") or "").strip(),
                        (action.get("software") or "").strip(),
                        (action.get("action_key") or "").strip(),
                        (action.get("action_label") or "").strip(),
                        1 if action.get("enabled", True) else 0,
                        interval_hours_legacy,
                        interval_minutes,
                        interval_phase_minutes,
                        (action.get("description") or "").strip(),
                        (action.get("contact_rule") or "none").strip(),
                        (action.get("contact_target") or "").strip().upper(),
                        int(action.get("sort_order") if action.get("sort_order") is not None else idx),
                    )
                    if action_id > 0:
                        conn.execute(
                            """
                            UPDATE sop_actions
                            SET band=?, frequency=?, software=?, action_key=?, action_label=?, enabled=?,
                                interval_hours=?, interval_minutes=?, interval_phase_minutes=?,
                                description=?, contact_rule=?, contact_target=?, sort_order=?
                            WHERE id=? AND profile_id=?
                            """,
                            (
                                vals[1],
                                vals[2],
                                vals[3],
                                vals[4],
                                vals[5],
                                vals[6],
                                vals[7],
                                vals[8],
                                vals[9],
                                vals[10],
                                vals[11],
                                vals[12],
                                vals[13],
                                action_id,
                                profile_id,
                            ),
                        )
                        kept.add(action_id)
                    else:
                        conn.execute(
                            """
                            INSERT INTO sop_actions
                                (profile_id, band, frequency, software, action_key, action_label, enabled,
                                 interval_hours, interval_minutes, interval_phase_minutes,
                                 description, contact_rule, contact_target, sort_order)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            vals,
                        )
                        new_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                        kept.add(new_id)

                to_delete = existing - kept
                for aid in to_delete:
                    conn.execute("DELETE FROM sop_action_state WHERE profile_id=? AND action_id=?", (profile_id, aid))
                    conn.execute("DELETE FROM sop_actions WHERE id=? AND profile_id=?", (aid, profile_id))

                if schedule_layer is not None:
                    existing_layer = {
                        int(r[0])
                        for r in conn.execute(
                            "SELECT id FROM sop_schedule_layer WHERE profile_id = ?",
                            (profile_id,),
                        ).fetchall()
                    }
                    kept_layer: set[int] = set()
                    for idx, layer in enumerate(schedule_layer):
                        if not isinstance(layer, dict):
                            continue
                        layer_id = int(layer.get("id") or 0)
                        recurrence = self._normalize_recurrence(layer.get("recurrence"))
                        day_utc = self._normalize_day_utc(layer.get("day_utc"))
                        if recurrence == "Daily":
                            day_utc = "ALL"
                        vals = (
                            profile_id,
                            day_utc,
                            recurrence,
                            int(layer.get("biweekly_offset_weeks") or 0),
                            self._normalize_month_weeks(layer.get("month_weeks")),
                            str(layer.get("band") or "").strip().upper(),
                            str(layer.get("mode") or "").strip().upper(),
                            str(layer.get("vfo") or "").strip().upper(),
                            self._normalize_frequency(layer.get("frequency")),
                            self._normalize_hhmm(layer.get("start_utc")),
                            self._normalize_hhmm(layer.get("end_utc")),
                            1 if layer.get("enabled", True) else 0,
                            int(layer.get("sort_order") if layer.get("sort_order") is not None else idx),
                            now_iso,
                        )
                        if not vals[8]:
                            continue
                        if layer_id > 0:
                            cur = conn.execute(
                                """
                                UPDATE sop_schedule_layer
                                SET day_utc=?, recurrence=?, biweekly_offset_weeks=?, month_weeks=?,
                                    band=?, mode=?, vfo=?, frequency=?, start_utc=?, end_utc=?,
                                    enabled=?, sort_order=?, updated_utc=?
                                WHERE id=? AND profile_id=?
                                """,
                                (
                                    vals[1],
                                    vals[2],
                                    vals[3],
                                    vals[4],
                                    vals[5],
                                    vals[6],
                                    vals[7],
                                    vals[8],
                                    vals[9],
                                    vals[10],
                                    vals[11],
                                    vals[12],
                                    vals[13],
                                    layer_id,
                                    profile_id,
                                ),
                            )
                            if int(cur.rowcount or 0) > 0:
                                kept_layer.add(layer_id)
                                continue
                        conn.execute(
                            """
                            INSERT INTO sop_schedule_layer
                                (profile_id, day_utc, recurrence, biweekly_offset_weeks, month_weeks,
                                 band, mode, vfo, frequency, start_utc, end_utc, enabled, sort_order, updated_utc)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            vals,
                        )
                        kept_layer.add(int(conn.execute("SELECT last_insert_rowid()").fetchone()[0]))

                    to_delete_layer = existing_layer - kept_layer
                    for lid in to_delete_layer:
                        conn.execute("DELETE FROM sop_schedule_layer WHERE id=? AND profile_id=?", (lid, profile_id))

            return profile_id
        finally:
            conn.close()

    def delete_profile(self, profile_id: int) -> None:
        conn = self._connect()
        try:
            with conn:
                conn.execute("DELETE FROM sop_action_state WHERE profile_id=?", (profile_id,))
                conn.execute("DELETE FROM sop_actions WHERE profile_id=?", (profile_id,))
                conn.execute("DELETE FROM sop_schedule_layer WHERE profile_id=?", (profile_id,))
                conn.execute("DELETE FROM sop_profiles WHERE id=?", (profile_id,))
        finally:
            conn.close()

    def mark_action_complete(self, profile_id: int, action_id: int, completed_utc: Optional[str] = None) -> None:
        stamp = completed_utc or _utc_now_iso()
        conn = self._connect()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO sop_action_state (profile_id, action_id, last_completed_utc, updated_utc)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(profile_id, action_id)
                    DO UPDATE SET last_completed_utc=excluded.last_completed_utc,
                                  updated_utc=excluded.updated_utc
                    """,
                    (profile_id, action_id, stamp, stamp),
                )
        finally:
            conn.close()

    def compute_next_due(
        self,
        start_hhmm: str,
        interval_minutes: int,
        now_utc: Optional[dt.datetime] = None,
        last_completed_utc: str | None = None,
        phase_minutes: int = 0,
    ) -> dt.datetime:
        now = now_utc or dt.datetime.now(dt.timezone.utc)
        interval_m = max(1, int(interval_minutes))
        interval = interval_m * 60
        phase_m = max(0, int(phase_minutes or 0)) % interval_m
        h, m = _parse_hhmm(start_hhmm, default_hhmm="00:00")
        anchor = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if anchor > now:
            anchor -= dt.timedelta(days=1)
        if phase_m:
            anchor += dt.timedelta(minutes=phase_m)
            if anchor > now:
                anchor -= dt.timedelta(days=1)

        ref = now
        last_completed = _parse_iso_utc(last_completed_utc)
        if last_completed and last_completed > ref:
            ref = last_completed
        if last_completed and last_completed <= ref:
            ref = last_completed + dt.timedelta(seconds=1)

        delta = (ref - anchor).total_seconds()
        if delta <= 0:
            return anchor
        step_count = int(delta // interval)
        if step_count * interval < delta:
            step_count += 1
        return anchor + dt.timedelta(seconds=step_count * interval)

    def load_secondary_groups(self) -> List[str]:
        conn = self._connect()
        values: set[str] = set()
        try:
            cur = conn.execute("SELECT group1, group2, group3 FROM operator_checkins")
            for g1, g2, g3 in cur.fetchall():
                for raw in (g1, g2, g3):
                    val = (raw or "").strip().upper()
                    if val:
                        values.add(val)
        except Exception as e:
            log.debug("SOP: load_secondary_groups failed: %s", e)
        finally:
            conn.close()
        return sorted(values)

    def resolve_primary_contacts(self, operating_group: str, secondary_group: str = "") -> Dict[str, List[str]]:
        group = (operating_group or "").strip().upper()
        subgroup = (secondary_group or "").strip().upper()
        hub_primary: List[str] = []
        hub_alt: List[str] = []
        ncs_primary: List[str] = []
        ncs_alt: List[str] = []
        peer: List[str] = []
        if not group:
            return {"hub": [], "ncs": [], "peer": []}

        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT callsign, group1, group2, group3, group_role FROM operator_checkins"
            )
            for callsign, g1, g2, g3, role in cur.fetchall():
                groups = [((g or "").strip().upper()) for g in (g1, g2, g3)]
                primary_match = group in groups
                secondary_match = (not subgroup) or (subgroup in groups)
                if not (primary_match and secondary_match):
                    continue
                cs = (callsign or "").strip().upper()
                r = (role or "").strip().upper()
                if not cs:
                    continue
                if r == "HUB":
                    if cs not in hub_primary:
                        hub_primary.append(cs)
                elif r == "HUB-ALT":
                    if cs not in hub_alt:
                        hub_alt.append(cs)
                elif r == "NCS":
                    if cs not in ncs_primary:
                        ncs_primary.append(cs)
                elif r == "ANCS":
                    if cs not in ncs_alt:
                        ncs_alt.append(cs)
                elif r == "PEER":
                    if cs not in peer:
                        peer.append(cs)
        except Exception as e:
            log.debug("SOP: resolve_primary_contacts failed: %s", e)
        finally:
            conn.close()
        hub = sorted(hub_primary) + sorted(hub_alt)
        ncs = sorted(ncs_primary) + sorted(ncs_alt)
        return {"hub": hub, "ncs": ncs, "peer": sorted(peer)}

    def resolve_group_callsigns(self, operating_group: str, secondary_group: str = "") -> List[str]:
        group = (operating_group or "").strip().upper()
        subgroup = (secondary_group or "").strip().upper()
        if not group:
            return []
        calls: List[str] = []
        conn = self._connect()
        try:
            cur = conn.execute("SELECT callsign, group1, group2, group3 FROM operator_checkins")
            for callsign, g1, g2, g3 in cur.fetchall():
                groups = [((g or "").strip().upper()) for g in (g1, g2, g3)]
                primary_match = group in groups
                secondary_match = (not subgroup) or (subgroup in groups)
                if not (primary_match and secondary_match):
                    continue
                cs = (callsign or "").strip().upper()
                if cs and cs not in calls:
                    calls.append(cs)
        except Exception as e:
            log.debug("SOP: resolve_group_callsigns failed: %s", e)
        finally:
            conn.close()
        return sorted(calls)

    def _load_schedule_windows(self, operating_group: str, band: str, frequency: str) -> List[Dict[str, Any]]:
        group = (operating_group or "").strip().upper()
        band_uc = (band or "").strip().upper()
        freq = ""
        try:
            freq = f"{float(frequency):.3f}"
        except Exception:
            freq = (frequency or "").strip()
        if not group:
            return []

        windows: List[Dict[str, Any]] = []
        conn = self._connect()
        try:
            daily_sql = """
                SELECT day_utc, start_utc, end_utc,
                       'Weekly' AS recurrence, '' AS month_weeks, 0 AS biweekly_offset_weeks
                FROM daily_schedule_tab
                WHERE UPPER(COALESCE(group_name, '')) = ?
            """
            daily_params: List[Any] = [group]
            if band_uc:
                daily_sql += " AND UPPER(COALESCE(band, '')) = ?"
                daily_params.append(band_uc)
            if freq:
                daily_sql += " AND printf('%.3f', CAST(frequency AS REAL)) = ?"
                daily_params.append(freq)
            for row in conn.execute(daily_sql, tuple(daily_params)).fetchall():
                day = str(row[0] or "").strip()
                start = str(row[1] or "").strip()
                end = str(row[2] or "").strip()
                if day and start and end:
                    windows.append(
                        {
                            "day_utc": day,
                            "start_utc": start,
                            "end_utc": end,
                            "recurrence": "Weekly",
                            "month_weeks": "",
                            "biweekly_offset_weeks": 0,
                        }
                    )

            net_sql = """
                SELECT day_utc, start_utc, end_utc,
                       COALESCE(recurrence, 'Weekly') AS recurrence,
                       COALESCE(month_weeks, '') AS month_weeks,
                       COALESCE(biweekly_offset_weeks, 0) AS biweekly_offset_weeks
                FROM net_schedule_tab
                WHERE UPPER(COALESCE(group_name, '')) = ?
            """
            net_params: List[Any] = [group]
            if band_uc:
                net_sql += " AND UPPER(COALESCE(band, '')) = ?"
                net_params.append(band_uc)
            if freq:
                net_sql += " AND printf('%.3f', CAST(frequency AS REAL)) = ?"
                net_params.append(freq)
            for row in conn.execute(net_sql, tuple(net_params)).fetchall():
                day = str(row[0] or "").strip()
                start = str(row[1] or "").strip()
                end = str(row[2] or "").strip()
                if day and start and end:
                    windows.append(
                        {
                            "day_utc": day,
                            "start_utc": start,
                            "end_utc": end,
                            "recurrence": self._normalize_recurrence(row[3]),
                            "month_weeks": self._normalize_month_weeks(row[4]),
                            "biweekly_offset_weeks": int(row[5] or 0),
                        }
                    )
        except Exception as e:
            log.debug("SOP: load schedule windows failed: %s", e)
        finally:
            conn.close()
        return windows

    def _load_sop_layer_windows(
        self,
        *,
        profile_id: Optional[int],
        band: str,
        frequency: str,
    ) -> List[Dict[str, Any]]:
        band_uc = (band or "").strip().upper()
        freq = self._normalize_frequency(frequency)
        out: List[Dict[str, Any]] = []
        conn = self._connect()
        try:
            sql = """
                SELECT
                    l.day_utc,
                    l.start_utc,
                    l.end_utc,
                    COALESCE(l.recurrence, 'Weekly') AS recurrence,
                    COALESCE(l.month_weeks, '') AS month_weeks,
                    COALESCE(l.biweekly_offset_weeks, 0) AS biweekly_offset_weeks,
                    COALESCE(l.band, '') AS band,
                    COALESCE(l.frequency, '') AS frequency
                FROM sop_schedule_layer l
                JOIN sop_profiles p ON p.id = l.profile_id
                WHERE l.enabled = 1
                  AND p.active = 1
            """
            params: List[Any] = []
            if profile_id and int(profile_id) > 0:
                sql += " AND l.profile_id = ?"
                params.append(int(profile_id))
            for row in conn.execute(sql, tuple(params)).fetchall():
                row_band = str(row[6] or "").strip().upper()
                row_freq = self._normalize_frequency(row[7])
                if band_uc and row_band and row_band != band_uc:
                    continue
                if freq and row_freq and row_freq != freq:
                    continue
                day = str(row[0] or "").strip()
                start = str(row[1] or "").strip()
                end = str(row[2] or "").strip()
                if not day or not start or not end:
                    continue
                out.append(
                    {
                        "day_utc": day,
                        "start_utc": start,
                        "end_utc": end,
                        "recurrence": self._normalize_recurrence(row[3]),
                        "month_weeks": self._normalize_month_weeks(row[4]),
                        "biweekly_offset_weeks": int(row[5] or 0),
                    }
                )
            return out
        except Exception as e:
            log.debug("SOP: load sop layer windows failed: %s", e)
            return []
        finally:
            conn.close()

    @staticmethod
    def _month_week_index(date_val: dt.date) -> int:
        return 1 + ((date_val.day - 1) // 7)

    def _window_matches_due(self, window: Dict[str, Any], due_utc: dt.datetime) -> bool:
        day_raw = self._normalize_day_utc(window.get("day_utc"))
        recurrence = self._normalize_recurrence(window.get("recurrence"))
        start_h, start_m = _parse_hhmm(str(window.get("start_utc") or ""), default_hhmm="00:00")
        end_h, end_m = _parse_hhmm(str(window.get("end_utc") or ""), default_hhmm="00:00")
        smin = start_h * 60 + start_m
        emin = end_h * 60 + end_m
        due_min = due_utc.hour * 60 + due_utc.minute
        day_name = _day_name_from_utc(due_utc).upper()
        prev_day = _day_name_from_utc(due_utc - dt.timedelta(days=1)).upper()
        day = day_raw.upper()
        overnight = smin > emin

        if recurrence == "Daily":
            day = "ALL"
        if day == "ALL":
            day = day_name

        def _periodic_match(use_prev_day: bool) -> bool:
            if recurrence != "Periodic":
                return True
            weeks_txt = self._normalize_month_weeks(window.get("month_weeks"))
            weeks = [int(tok) for tok in weeks_txt.split(",") if tok.strip().isdigit()]
            if not weeks:
                weeks = [1]
            ref_date = due_utc.date() - dt.timedelta(days=1) if use_prev_day else due_utc.date()
            return self._month_week_index(ref_date) in weeks

        if day == day_name:
            if not _periodic_match(False):
                return False
            if not overnight:
                return smin <= due_min < emin
            return due_min >= smin or due_min < emin
        if overnight and day == prev_day:
            if not _periodic_match(True):
                return False
            return due_min < emin
        return False

    def is_due_aligned_with_schedule(
        self,
        operating_group: str,
        band: str,
        frequency: str,
        due_utc: dt.datetime,
        *,
        profile_id: Optional[int] = None,
    ) -> bool:
        windows = self._load_schedule_windows(operating_group, band, frequency)
        windows.extend(
            self._load_sop_layer_windows(
                profile_id=profile_id,
                band=band,
                frequency=frequency,
            )
        )
        if not windows:
            return True
        for window in windows:
            if self._window_matches_due(window, due_utc):
                return True
        return False

    def build_schedule_layer_candidates(
        self,
        *,
        operating_group: str,
        action_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        group = (operating_group or "").strip().upper()
        if not group:
            return {"rows": [], "unmatched": ["Operating Group is required."], "matched_actions": 0}
        targets: List[Tuple[str, str, str]] = []
        target_labels: Dict[Tuple[str, str], str] = {}
        unmatched: List[str] = []
        for idx, row in enumerate(action_rows):
            if not isinstance(row, dict):
                continue
            software = str(row.get("software") or "").strip()
            action_label = str(row.get("action_label") or "").strip()
            if software == "Local Net":
                continue
            band = str(row.get("band") or "").strip().upper()
            freq_raw = str(row.get("frequency") or "").strip()
            freq = self._normalize_frequency(freq_raw)
            label = f"Row {idx + 1}"
            if action_label:
                label += f" ({action_label})"
            if not band or not freq:
                unmatched.append(f"{label}: missing band/frequency.")
                continue
            key = (band, freq)
            if key not in target_labels:
                target_labels[key] = label
            targets.append((band, freq, label))
        if not targets:
            return {"rows": [], "unmatched": unmatched or ["No eligible non-local action rows found."], "matched_actions": 0}

        conn = self._connect()
        try:
            cols_daily = _table_columns(conn, "daily_schedule_tab")
            cols_net = _table_columns(conn, "net_schedule_tab")
            rows: List[Dict[str, Any]] = []
            matched_targets: Set[Tuple[str, str]] = set()

            def _fetch_daily_rows(band: str, freq: str) -> None:
                if not cols_daily:
                    return
                group_col = "group_name" if "group_name" in cols_daily else ("group" if "group" in cols_daily else "")
                if not group_col:
                    return
                if not {"day_utc", "start_utc", "end_utc", "frequency", "band"}.issubset(cols_daily):
                    return
                mode_expr = "COALESCE(mode, '')" if "mode" in cols_daily else "''"
                vfo_expr = "COALESCE(vfo, 'A')" if "vfo" in cols_daily else "'A'"
                sql = (
                    f"""
                    SELECT
                        day_utc,
                        start_utc,
                        end_utc,
                        COALESCE(band, ''),
                        COALESCE(frequency, ''),
                        {mode_expr},
                        {vfo_expr}
                    FROM daily_schedule_tab
                    WHERE UPPER(COALESCE({group_col}, '')) = ?
                      AND UPPER(COALESCE(band, '')) = ?
                      AND (
                          printf('%.3f', CAST(frequency AS REAL)) = ?
                          OR TRIM(CAST(frequency AS TEXT)) = ?
                      )
                    """
                )
                params = (group, band, freq, freq)
                for d, s, e, b, f, m, v in conn.execute(sql, params).fetchall():
                    rows.append(
                        {
                            "day_utc": "ALL",
                            "recurrence": "Daily",
                            "biweekly_offset_weeks": 0,
                            "month_weeks": "",
                            "band": str(b or "").strip().upper(),
                            "mode": str(m or "").strip().upper(),
                            "vfo": "",
                            "frequency": self._normalize_frequency(f),
                            "start_utc": self._normalize_hhmm(s),
                            "end_utc": self._normalize_hhmm(e),
                            "enabled": True,
                        }
                    )

            def _fetch_net_rows(band: str, freq: str) -> None:
                if not cols_net:
                    return
                group_col = "group_name" if "group_name" in cols_net else ("group" if "group" in cols_net else "")
                if not group_col:
                    return
                if not {"day_utc", "start_utc", "end_utc", "frequency", "band"}.issubset(cols_net):
                    return
                rec_expr = "COALESCE(recurrence, 'Weekly')" if "recurrence" in cols_net else "'Weekly'"
                biweek_expr = "COALESCE(biweekly_offset_weeks, 0)" if "biweekly_offset_weeks" in cols_net else "0"
                weeks_expr = "COALESCE(month_weeks, '')" if "month_weeks" in cols_net else "''"
                mode_expr = "COALESCE(mode, '')" if "mode" in cols_net else "''"
                vfo_expr = "COALESCE(vfo, 'A')" if "vfo" in cols_net else "'A'"
                sql = (
                    f"""
                    SELECT
                        day_utc,
                        start_utc,
                        end_utc,
                        {rec_expr},
                        {biweek_expr},
                        {weeks_expr},
                        COALESCE(band, ''),
                        COALESCE(frequency, ''),
                        {mode_expr},
                        {vfo_expr}
                    FROM net_schedule_tab
                    WHERE UPPER(COALESCE({group_col}, '')) = ?
                      AND UPPER(COALESCE(band, '')) = ?
                      AND (
                          printf('%.3f', CAST(frequency AS REAL)) = ?
                          OR TRIM(CAST(frequency AS TEXT)) = ?
                      )
                    """
                )
                params = (group, band, freq, freq)
                for d, s, e, rec, biw, weeks, b, f, m, v in conn.execute(sql, params).fetchall():
                    rows.append(
                        {
                            "day_utc": "ALL",
                            "recurrence": "Daily",
                            "biweekly_offset_weeks": 0,
                            "month_weeks": "",
                            "band": str(b or "").strip().upper(),
                            "mode": str(m or "").strip().upper(),
                            "vfo": "",
                            "frequency": self._normalize_frequency(f),
                            "start_utc": self._normalize_hhmm(s),
                            "end_utc": self._normalize_hhmm(e),
                            "enabled": True,
                        }
                    )

            for band, freq, _label in targets:
                before = len(rows)
                _fetch_daily_rows(band, freq)
                _fetch_net_rows(band, freq)
                if len(rows) > before:
                    matched_targets.add((band, freq))

            dedup: Dict[Tuple[str, str, int, str, str, str, str, str, str, str], Dict[str, Any]] = {}
            for row in rows:
                key = (
                    str(row.get("day_utc") or "ALL"),
                    str(row.get("recurrence") or "Weekly"),
                    int(row.get("biweekly_offset_weeks") or 0),
                    str(row.get("month_weeks") or ""),
                    str(row.get("band") or "").upper(),
                    str(row.get("mode") or "").upper(),
                    str(row.get("vfo") or "A").upper(),
                    str(row.get("frequency") or ""),
                    str(row.get("start_utc") or ""),
                    str(row.get("end_utc") or ""),
                )
                dedup[key] = row
            out_rows = list(dedup.values())
            out_rows.sort(
                key=lambda x: (
                    str(x.get("day_utc") or "ALL"),
                    str(x.get("start_utc") or ""),
                    str(x.get("frequency") or ""),
                    str(x.get("recurrence") or ""),
                )
            )
            for idx, row in enumerate(out_rows):
                row["sort_order"] = idx
            for band, freq, _label in targets:
                if (band, freq) not in matched_targets:
                    unmatched.append(f"{target_labels.get((band, freq), 'Action')}: no matching HF/Net schedule windows found.")
            return {
                "rows": out_rows,
                "unmatched": unmatched,
                "matched_actions": len(matched_targets),
            }
        except Exception as e:
            log.debug("SOP: build schedule layer candidates failed: %s", e)
            return {"rows": [], "unmatched": [f"Failed to build candidates: {e}"], "matched_actions": 0}
        finally:
            conn.close()

    def build_upcoming_actions(
        self,
        horizon_hours: int = 12,
        only_active: bool = True,
        now_utc: Optional[dt.datetime] = None,
    ) -> List[Dict[str, Any]]:
        now = now_utc or dt.datetime.now(dt.timezone.utc)
        horizon = now + dt.timedelta(hours=max(1, int(horizon_hours)))
        overdue_warn_end = dt.timedelta(minutes=30)
        rows: List[Dict[str, Any]] = []
        for profile in self.list_profiles():
            if only_active and not profile.get("active"):
                continue
            full = self.get_profile(int(profile["id"]))
            if not full:
                continue
            contacts = self.resolve_primary_contacts(
                full.get("operating_group", ""),
                full.get("secondary_group", ""),
            )
            for action in full.get("actions", []):
                if not _is_enabled(action.get("enabled", True)):
                    continue
                interval_m = int(action.get("interval_minutes") or 0)
                if interval_m <= 0:
                    interval_m = max(1, int(action.get("interval_hours") or 3)) * 60
                interval_phase_m = max(0, int(action.get("interval_phase_minutes") or 0)) % max(1, interval_m)
                interval_td = dt.timedelta(minutes=interval_m)
                next_due = self.compute_next_due(
                    full.get("sop_start_utc", "00:00"),
                    interval_m,
                    now_utc=now,
                    last_completed_utc=None,
                    phase_minutes=interval_phase_m,
                )
                current_due = next_due - interval_td
                last_completed = _parse_iso_utc(action.get("last_completed_utc"))
                completed_current = bool(
                    last_completed
                    and current_due <= last_completed < (current_due + interval_td)
                )

                # Show currently due/overdue item up to 30 minutes past due.
                due = None
                status = "Upcoming"
                if current_due <= now <= (current_due + overdue_warn_end):
                    due = current_due
                    if completed_current:
                        status = "Completed"
                    elif now <= (current_due + dt.timedelta(minutes=20)):
                        status = "Due Now"
                    else:
                        status = "Overdue"
                elif next_due <= horizon:
                    due = next_due
                    status = "Upcoming"
                if due is None:
                    continue

                action_band = (action.get("band") or "").strip().upper()
                action_freq = (action.get("frequency") or "").strip() or full.get("frequency", "")
                rule = (action.get("contact_rule") or "none").strip()
                aligned = True
                if rule != "local_profile":
                    aligned = self.is_due_aligned_with_schedule(
                        full.get("operating_group", ""),
                        action_band,
                        action_freq,
                        due,
                        profile_id=int(full.get("id") or 0),
                    )
                selected_target = (action.get("contact_target") or "").strip().upper()
                targets: List[str] = []
                if rule == "hub_or_hub_alt":
                    if selected_target and selected_target != "__ANY_ROLE__":
                        targets = [selected_target]
                    else:
                        targets = ["Any (Role Match)"]
                elif rule == "ncs_or_ancs":
                    if selected_target and selected_target != "__ANY_ROLE__":
                        targets = [selected_target]
                    else:
                        targets = ["Any (Role Match)"]
                elif rule == "callsign":
                    target = (action.get("contact_target") or "").strip().upper()
                    targets = [target] if target else []
                elif rule == "peer":
                    target = (action.get("contact_target") or "").strip().upper()
                    targets = [target] if target else []
                elif rule == "local_profile":
                    target = (action.get("contact_target") or "").strip().upper()
                    targets = [target] if target else []
                rows.append(
                    {
                        "profile_id": int(full["id"]),
                        "profile_name": full.get("name", ""),
                        "operating_group": full.get("operating_group", ""),
                        "band": action_band,
                        "frequency": action_freq,
                        "action_id": int(action["id"]),
                        "software": action.get("software", ""),
                        "action_key": action.get("action_key", ""),
                        "action_label": action.get("action_label", ""),
                        "description": action.get("description", ""),
                        "contact_rule": rule,
                        "contact_target": (action.get("contact_target") or "").strip().upper(),
                        "contact_targets": targets,
                        "interval_minutes": interval_m,
                        "interval_phase_minutes": interval_phase_m,
                        "next_due_utc": due,
                        "aligned": aligned,
                        "status": status,
                        "is_completed": completed_current and status == "Completed",
                    }
                )
        rows.sort(key=lambda x: x["next_due_utc"])
        return rows

    def list_export_regions(self) -> List[str]:
        return sorted({str(v).strip().upper() for v in STATE_TO_FEMA_REGION.values() if str(v).strip()})

    def list_hf_groups_for_export(self) -> List[str]:
        conn = self._connect()
        out: Set[str] = set()
        try:
            cols = _table_columns(conn, "operator_checkins")
            if not cols or "callsign" not in cols:
                return []
            group1_expr = "IFNULL(group1, '')" if "group1" in cols else "''"
            group2_expr = "IFNULL(group2, '')" if "group2" in cols else "''"
            group3_expr = "IFNULL(group3, '')" if "group3" in cols else "''"
            groups_json_expr = "IFNULL(groups_json, '')" if "groups_json" in cols else "''"
            cur = conn.execute(
                f"""
                SELECT {group1_expr}, {group2_expr}, {group3_expr}, {groups_json_expr}
                FROM operator_checkins
                """
            )
            for g1, g2, g3, groups_json in cur.fetchall():
                out.update(_collect_group_tokens(g1, g2, g3, groups_json))
            return sorted(out)
        except Exception as e:
            log.debug("SOP: list_hf_groups_for_export failed: %s", e)
            return []
        finally:
            conn.close()

    def list_local_categories_for_export(self) -> List[str]:
        out: Set[str] = set()
        try:
            for row in get_all_operators():
                category = str(row.get("category") or "").strip()
                if category:
                    out.add(category)
            return sorted(out, key=lambda x: x.upper())
        except Exception as e:
            log.debug("SOP: list_local_categories_for_export failed: %s", e)
            return []

    def _contact_display_for_export(
        self,
        *,
        rule: str,
        target: str,
        contacts: Dict[str, List[str]],
    ) -> str:
        rule_val = (rule or "none").strip().lower()
        tgt = (target or "").strip().upper()
        any_role = "__ANY_ROLE__"
        if rule_val == "hub_or_hub_alt":
            if tgt and tgt != any_role:
                return tgt
            vals = contacts.get("hub", []) or []
            return " OR ".join(vals[:4]) if vals else "Any (Role Match)"
        if rule_val == "ncs_or_ancs":
            if tgt and tgt != any_role:
                return tgt
            vals = contacts.get("ncs", []) or []
            return " OR ".join(vals[:4]) if vals else "Any (Role Match)"
        if rule_val in {"callsign", "peer", "local_profile"}:
            return tgt or "--"
        return "--"

    def build_profile_export_rows(
        self,
        profile_id: int,
        *,
        now_utc: Optional[dt.datetime] = None,
    ) -> List[Dict[str, Any]]:
        full = self.get_profile(profile_id)
        if not full:
            return []
        now = now_utc or dt.datetime.now(dt.timezone.utc)
        contacts = self.resolve_primary_contacts(
            full.get("operating_group", ""),
            full.get("secondary_group", ""),
        )
        rows: List[Dict[str, Any]] = []
        for action in full.get("actions", []):
            if not bool(action.get("enabled", True)):
                continue
            interval_m = int(action.get("interval_minutes") or 0)
            if interval_m <= 0:
                interval_m = max(1, int(action.get("interval_hours") or 3)) * 60
            interval_phase_m = max(0, int(action.get("interval_phase_minutes") or 0)) % max(1, interval_m)
            interval_td = dt.timedelta(minutes=interval_m)
            next_due = self.compute_next_due(
                full.get("sop_start_utc", "00:00"),
                interval_m,
                now_utc=now,
                last_completed_utc=action.get("last_completed_utc"),
                phase_minutes=interval_phase_m,
            )
            current_due = next_due - interval_td
            status = "Upcoming"
            if current_due <= now < next_due:
                late = now - current_due
                if late <= dt.timedelta(minutes=20):
                    status = "Due Now"
                else:
                    status = "Overdue"
            last_completed = _parse_iso_utc(action.get("last_completed_utc"))
            if last_completed and current_due <= last_completed < (current_due + interval_td):
                status = "Completed"
            rows.append(
                {
                    "profile_id": int(full.get("id") or 0),
                    "profile_name": str(full.get("name") or ""),
                    "operating_group": str(full.get("operating_group") or ""),
                    "secondary_group": str(full.get("secondary_group") or ""),
                    "sop_start_utc": str(full.get("sop_start_utc") or "00:00"),
                    "action_id": int(action.get("id") or 0),
                    "software": str(action.get("software") or ""),
                    "action_label": str(action.get("action_label") or ""),
                    "band": str(action.get("band") or "").strip().upper(),
                    "frequency": str(action.get("frequency") or "").strip() or str(full.get("frequency") or ""),
                    "description": str(action.get("description") or ""),
                    "interval_minutes": interval_m,
                    "interval_phase_minutes": interval_phase_m,
                    "status": status,
                    "next_due_utc": next_due,
                    "contact_display": self._contact_display_for_export(
                        rule=str(action.get("contact_rule") or "none"),
                        target=str(action.get("contact_target") or ""),
                        contacts=contacts,
                    ),
                }
            )
        rows.sort(key=lambda x: x["next_due_utc"])
        return rows

    def build_profile_daily_plan_rows(
        self,
        profile_id: int,
        *,
        day_start_utc: dt.datetime,
        day_end_utc: dt.datetime,
    ) -> List[Dict[str, Any]]:
        full = self.get_profile(profile_id)
        if not full:
            return []
        start = day_start_utc if isinstance(day_start_utc, dt.datetime) else dt.datetime.now(dt.timezone.utc)
        end = day_end_utc if isinstance(day_end_utc, dt.datetime) else start
        if start.tzinfo is None:
            start = start.replace(tzinfo=dt.timezone.utc)
        else:
            start = start.astimezone(dt.timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=dt.timezone.utc)
        else:
            end = end.astimezone(dt.timezone.utc)
        if end < start:
            return []

        contacts = self.resolve_primary_contacts(
            full.get("operating_group", ""),
            full.get("secondary_group", ""),
        )
        rows: List[Dict[str, Any]] = []
        total_window_minutes = max(1, int((end - start).total_seconds() // 60))
        for action in full.get("actions", []):
            if not _is_enabled(action.get("enabled", True)):
                continue
            interval_m = int(action.get("interval_minutes") or 0)
            if interval_m <= 0:
                interval_m = max(1, int(action.get("interval_hours") or 3)) * 60
            interval_m = max(1, interval_m)
            phase_m = max(0, int(action.get("interval_phase_minutes") or 0)) % interval_m
            interval_td = dt.timedelta(minutes=interval_m)
            due = self.compute_next_due(
                str(full.get("sop_start_utc") or "00:00"),
                interval_m,
                now_utc=start,
                last_completed_utc=None,
                phase_minutes=phase_m,
            )
            while due < start:
                due += interval_td
            max_iters = min(240, int(math.ceil(total_window_minutes / max(1, interval_m))) + 3)
            iters = 0
            while due <= end and iters < max_iters:
                band = str(action.get("band") or "").strip().upper()
                freq = str(action.get("frequency") or "").strip() or str(full.get("frequency") or "")
                rows.append(
                    {
                        "profile_id": int(full.get("id") or 0),
                        "profile_name": str(full.get("name") or ""),
                        "due_utc": due,
                        "resource": str(action.get("software") or ""),
                        "action_label": str(action.get("action_label") or ""),
                        "band": band,
                        "frequency": freq,
                        "band_freq": f"{band} {freq}".strip() if (band or freq) else "--",
                        "contact_display": self._contact_display_for_export(
                            rule=str(action.get("contact_rule") or "none"),
                            target=str(action.get("contact_target") or ""),
                            contacts=contacts,
                        ),
                        "description": str(action.get("description") or ""),
                    }
                )
                due += interval_td
                iters += 1
        rows.sort(
            key=lambda x: (
                x.get("due_utc") if isinstance(x.get("due_utc"), dt.datetime) else dt.datetime.max.replace(tzinfo=dt.timezone.utc),
                str(x.get("resource") or ""),
                str(x.get("action_label") or ""),
                str(x.get("band_freq") or ""),
            )
        )
        return rows

    def build_profile_periodic_action_rows(self, profile_id: int) -> List[Dict[str, Any]]:
        full = self.get_profile(profile_id)
        if not full:
            return []
        contacts = self.resolve_primary_contacts(
            full.get("operating_group", ""),
            full.get("secondary_group", ""),
        )
        periodic_layers = [
            row
            for row in (full.get("schedule_layer") or [])
            if bool(row.get("enabled", True))
            and str(row.get("recurrence") or "").strip().lower() == "periodic"
            and str(row.get("month_weeks") or "").strip()
        ]
        if not periodic_layers:
            return []

        out: List[Dict[str, Any]] = []
        seen: Set[Tuple[str, str, str, str, str, str, str]] = set()
        for layer in periodic_layers:
            weeks = self._normalize_month_weeks(layer.get("month_weeks"))
            if not weeks:
                continue
            day = self._normalize_day_utc(layer.get("day_utc"))
            layer_band = str(layer.get("band") or "").strip().upper()
            layer_freq = str(layer.get("frequency") or "").strip()
            for action in full.get("actions", []):
                if not _is_enabled(action.get("enabled", True)):
                    continue
                band = str(action.get("band") or "").strip().upper() or layer_band
                freq = str(action.get("frequency") or "").strip() or layer_freq or str(full.get("frequency") or "")
                row = {
                    "profile_id": int(full.get("id") or 0),
                    "profile_name": str(full.get("name") or ""),
                    "weeks_of_month": weeks,
                    "day_of_week": day,
                    "resource": str(action.get("software") or ""),
                    "action_label": str(action.get("action_label") or ""),
                    "band_freq": f"{band} {freq}".strip() if (band or freq) else "--",
                    "contact_display": self._contact_display_for_export(
                        rule=str(action.get("contact_rule") or "none"),
                        target=str(action.get("contact_target") or ""),
                        contacts=contacts,
                    ),
                    "description": str(action.get("description") or ""),
                }
                key = (
                    row["weeks_of_month"],
                    row["day_of_week"],
                    row["resource"],
                    row["action_label"],
                    row["band_freq"],
                    row["contact_display"],
                    row["description"],
                )
                if key in seen:
                    continue
                seen.add(key)
                out.append(row)
        out.sort(
            key=lambda x: (
                str(x.get("weeks_of_month") or ""),
                str(x.get("day_of_week") or ""),
                str(x.get("resource") or ""),
                str(x.get("action_label") or ""),
            )
        )
        return out

    def list_hf_operators_for_export(
        self,
        *,
        state_filter: str = "",
        region_filter: str = "",
        group_filters: Optional[List[str]] = None,
        trusted_filters: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        conn = self._connect()
        out: List[Dict[str, str]] = []
        try:
            cols = _table_columns(conn, "operator_checkins")
            if not cols or "callsign" not in cols:
                return []
            name_expr = "IFNULL(name, '')" if "name" in cols else "''"
            state_expr = "IFNULL(state, '')" if "state" in cols else "''"
            notes_expr = "IFNULL(notes, '')" if "notes" in cols else "''"
            group1_expr = "IFNULL(group1, '')" if "group1" in cols else "''"
            group2_expr = "IFNULL(group2, '')" if "group2" in cols else "''"
            group3_expr = "IFNULL(group3, '')" if "group3" in cols else "''"
            groups_json_expr = "IFNULL(groups_json, '')" if "groups_json" in cols else "''"
            trusted_expr = "trusted" if "trusted" in cols else "0"
            selected_groups = {_norm_group(v) for v in (group_filters or []) if _norm_group(v)}
            selected_trusted = {_norm_trusted_filter(v) for v in (trusted_filters or []) if _norm_trusted_filter(v)}
            sitrep_map: Dict[str, str] = {}
            try:
                cur = conn.execute("SELECT callsign, effective_status FROM sitrep_latest_by_callsign")
                for callsign, status in cur.fetchall():
                    cs = str(callsign or "").strip().upper()
                    if cs:
                        sitrep_map[cs] = _norm_sitrep(status)
            except Exception:
                if "sitrep_status" in cols:
                    try:
                        cur = conn.execute("SELECT callsign, sitrep_status FROM operator_checkins")
                        for callsign, status in cur.fetchall():
                            cs = str(callsign or "").strip().upper()
                            if cs:
                                sitrep_map[cs] = _norm_sitrep(status)
                    except Exception:
                        pass
            cur = conn.execute(
                f"""
                SELECT
                    IFNULL(callsign, ''),
                    {name_expr},
                    {state_expr},
                    {notes_expr},
                    {group1_expr},
                    {group2_expr},
                    {group3_expr},
                    {groups_json_expr},
                    {trusted_expr}
                FROM operator_checkins
                ORDER BY callsign COLLATE NOCASE
                """
            )
            for callsign, name, state, notes, g1, g2, g3, groups_json, trusted in cur.fetchall():
                cs = str(callsign or "").strip().upper()
                st = _norm_state(state)
                if not cs:
                    continue
                if not _passes_geo_filter(st, state_filter=state_filter, region_filter=region_filter):
                    continue
                row_groups = _collect_group_tokens(g1, g2, g3, groups_json)
                if selected_groups and not (row_groups & selected_groups):
                    continue
                row_trusted = _trusted_label(trusted)
                if selected_trusted and row_trusted not in selected_trusted:
                    continue
                out.append(
                    {
                        "callsign": cs,
                        "name": str(name or "").strip(),
                        "state": st,
                        "sitrep": sitrep_map.get(cs, "UNKNOWN"),
                        "notes": str(notes or "").strip(),
                    }
                )
            return out
        except Exception as e:
            log.debug("SOP: list_hf_operators_for_export failed: %s", e)
            return []
        finally:
            conn.close()

    def list_local_operators_for_export(
        self,
        *,
        state_filter: str = "",
        region_filter: str = "",
        category_filters: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        selected_categories = {_norm_group(v) for v in (category_filters or []) if _norm_group(v)}
        try:
            for row in get_all_operators():
                cs = str(row.get("callsign") or "").strip().upper()
                st = _norm_state(row.get("state"))
                category = str(row.get("category") or "").strip()
                if not cs:
                    continue
                if not _passes_geo_filter(st, state_filter=state_filter, region_filter=region_filter):
                    continue
                if selected_categories and _norm_group(category) not in selected_categories:
                    continue
                out.append(
                    {
                        "callsign": cs,
                        "first_name": str(row.get("first_name") or "").strip(),
                        "last_name": str(row.get("last_name") or "").strip(),
                        "city": str(row.get("city") or "").strip(),
                        "state": st,
                        "category": category,
                        "sitrep": _norm_sitrep(row.get("sitrep_status")),
                        "notes": str(row.get("notes") or "").strip(),
                    }
                )
            return out
        except Exception as e:
            log.debug("SOP: list_local_operators_for_export failed: %s", e)
            return []

    def export_profile_json(self, profile_id: int) -> Dict[str, Any]:
        profile = self.get_profile(profile_id)
        if not profile:
            raise ValueError("Profile not found")
        return {
            "schema_version": 2,
            "exported_utc": _utc_now_iso(),
            "profile": profile,
        }

    def import_profile_json(self, payload: Dict[str, Any]) -> int:
        profile = payload.get("profile")
        if not isinstance(profile, dict):
            raise ValueError("Invalid SOP import: missing profile object")

        imported = dict(profile)
        imported.pop("id", None)
        actions = imported.pop("actions", [])
        schedule_layer = imported.pop("schedule_layer", [])
        name = (imported.get("name") or "Imported SOP").strip()

        existing_names = {p.get("name", "").lower() for p in self.list_profiles()}
        base = name
        suffix = 1
        while name.lower() in existing_names:
            suffix += 1
            name = f"{base} ({suffix})"
        imported["name"] = name
        try:
            imported["priority"] = int(imported.get("priority") or 100)
        except Exception:
            imported["priority"] = 100

        normalized_actions: List[Dict[str, Any]] = []
        if isinstance(actions, list):
            for idx, a in enumerate(actions):
                if not isinstance(a, dict):
                    continue
                normalized_actions.append(
                    {
                        "band": (a.get("band") or "").strip().upper(),
                        "frequency": (a.get("frequency") or "").strip(),
                        "software": (a.get("software") or "").strip(),
                        "action_key": (a.get("action_key") or "").strip(),
                        "action_label": (a.get("action_label") or "").strip(),
                        "enabled": bool(a.get("enabled", True)),
                        "interval_hours": int(a.get("interval_hours") or 3),
                        "interval_minutes": int(a.get("interval_minutes") or (int(a.get("interval_hours") or 3) * 60)),
                        "interval_phase_minutes": int(a.get("interval_phase_minutes") or 0),
                        "description": (a.get("description") or "").strip(),
                        "contact_rule": (a.get("contact_rule") or "none").strip(),
                        "contact_target": (a.get("contact_target") or "").strip().upper(),
                        "sort_order": int(a.get("sort_order") if a.get("sort_order") is not None else idx),
                    }
                )
        normalized_layers: List[Dict[str, Any]] = []
        if isinstance(schedule_layer, list):
            for idx, row in enumerate(schedule_layer):
                if not isinstance(row, dict):
                    continue
                normalized_layers.append(
                    {
                        "day_utc": self._normalize_day_utc(row.get("day_utc")),
                        "recurrence": self._normalize_recurrence(row.get("recurrence")),
                        "biweekly_offset_weeks": int(row.get("biweekly_offset_weeks") or 0),
                        "month_weeks": self._normalize_month_weeks(row.get("month_weeks")),
                        "band": str(row.get("band") or "").strip().upper(),
                        "mode": str(row.get("mode") or "").strip().upper(),
                        "vfo": str(row.get("vfo") or "").strip().upper(),
                        "frequency": self._normalize_frequency(row.get("frequency")),
                        "start_utc": self._normalize_hhmm(row.get("start_utc")),
                        "end_utc": self._normalize_hhmm(row.get("end_utc")),
                        "enabled": bool(row.get("enabled", True)),
                        "sort_order": int(row.get("sort_order") if row.get("sort_order") is not None else idx),
                    }
                )
        return self.save_profile(imported, normalized_actions, normalized_layers)

    @staticmethod
    def dumps_json(payload: Dict[str, Any]) -> str:
        def _default(o: Any) -> Any:
            if isinstance(o, dt.datetime):
                return o.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()
            raise TypeError(f"Unsupported type: {type(o)!r}")

        return json.dumps(payload, indent=2, sort_keys=True, default=_default)
