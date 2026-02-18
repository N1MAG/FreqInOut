from __future__ import annotations

import datetime as dt
import json
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
                    active INTEGER DEFAULT 0,
                    window_hours INTEGER DEFAULT 12,
                    created_utc TEXT,
                    updated_utc TEXT
                )
                """
            )
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
            if "frequency" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN frequency TEXT")
            if "contact_rule" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN contact_rule TEXT DEFAULT 'none'")
            if "contact_target" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN contact_target TEXT")
            if "interval_minutes" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN interval_minutes INTEGER")
            if "sort_order" not in action_cols:
                cur.execute("ALTER TABLE sop_actions ADD COLUMN sort_order INTEGER DEFAULT 0")
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
            conn.commit()
        finally:
            conn.close()

    def list_profiles(self) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT id, name, operating_group, secondary_group, frequency, sop_start_utc,
                       active, window_hours, created_utc, updated_utc
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
                    "active": bool(r[6]),
                    "window_hours": int(r[7] or 12),
                    "created_utc": r[8] or "",
                    "updated_utc": r[9] or "",
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
                       active, window_hours, created_utc, updated_utc
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
                       a.interval_hours, a.interval_minutes, a.description, a.contact_rule, a.contact_target, a.sort_order,
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
                        "description": ar[9] or "",
                        "contact_rule": ar[10] or "none",
                        "contact_target": ar[11] or "",
                        "sort_order": int(ar[12] or 0),
                        "last_completed_utc": ar[13] or "",
                    }
                )

            return {
                "id": row[0],
                "name": row[1],
                "operating_group": row[2] or "",
                "secondary_group": row[3] or "",
                "frequency": row[4] or "",
                "sop_start_utc": row[5] or "00:00",
                "active": bool(row[6]),
                "window_hours": int(row[7] or 12),
                "created_utc": row[8] or "",
                "updated_utc": row[9] or "",
                "actions": actions,
            }
        finally:
            conn.close()

    def save_profile(self, payload: Dict[str, Any], actions: List[Dict[str, Any]]) -> int:
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
                    1 if payload.get("active") else 0,
                    int(payload.get("window_hours") or 12),
                )
                if profile_id > 0:
                    conn.execute(
                        """
                        UPDATE sop_profiles
                        SET name=?, operating_group=?, secondary_group=?, frequency=?, sop_start_utc=?,
                            active=?, window_hours=?, updated_utc=?
                        WHERE id=?
                        """,
                        (*row, now_iso, profile_id),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO sop_profiles
                            (name, operating_group, secondary_group, frequency, sop_start_utc,
                             active, window_hours, created_utc, updated_utc)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                                interval_hours=?, interval_minutes=?, description=?, contact_rule=?, contact_target=?, sort_order=?
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
                                 interval_hours, interval_minutes, description, contact_rule, contact_target, sort_order)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            vals,
                        )
                        new_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                        kept.add(new_id)

                to_delete = existing - kept
                for aid in to_delete:
                    conn.execute("DELETE FROM sop_action_state WHERE profile_id=? AND action_id=?", (profile_id, aid))
                    conn.execute("DELETE FROM sop_actions WHERE id=? AND profile_id=?", (aid, profile_id))

            return profile_id
        finally:
            conn.close()

    def delete_profile(self, profile_id: int) -> None:
        conn = self._connect()
        try:
            with conn:
                conn.execute("DELETE FROM sop_action_state WHERE profile_id=?", (profile_id,))
                conn.execute("DELETE FROM sop_actions WHERE profile_id=?", (profile_id,))
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
    ) -> dt.datetime:
        now = now_utc or dt.datetime.now(dt.timezone.utc)
        interval = max(1, int(interval_minutes)) * 60
        h, m = _parse_hhmm(start_hhmm, default_hhmm="00:00")
        anchor = now.replace(hour=h, minute=m, second=0, microsecond=0)
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

    def _load_schedule_windows(self, operating_group: str, band: str, frequency: str) -> List[Tuple[str, str, str]]:
        group = (operating_group or "").strip().upper()
        band_uc = (band or "").strip().upper()
        freq = ""
        try:
            freq = f"{float(frequency):.3f}"
        except Exception:
            freq = (frequency or "").strip()
        if not group:
            return []

        windows: List[Tuple[str, str, str]] = []
        conn = self._connect()
        try:
            for table in ("daily_schedule_tab", "net_schedule_tab"):
                if freq:
                    if band_uc:
                        cur = conn.execute(
                            f"""
                            SELECT day_utc, start_utc, end_utc
                            FROM {table}
                            WHERE UPPER(COALESCE(group_name, '')) = ?
                              AND UPPER(COALESCE(band, '')) = ?
                              AND printf('%.3f', CAST(frequency AS REAL)) = ?
                            """,
                            (group, band_uc, freq),
                        )
                    else:
                        cur = conn.execute(
                            f"""
                            SELECT day_utc, start_utc, end_utc
                            FROM {table}
                            WHERE UPPER(COALESCE(group_name, '')) = ?
                              AND printf('%.3f', CAST(frequency AS REAL)) = ?
                            """,
                            (group, freq),
                        )
                elif band_uc:
                    cur = conn.execute(
                        f"""
                        SELECT day_utc, start_utc, end_utc
                        FROM {table}
                        WHERE UPPER(COALESCE(group_name, '')) = ?
                          AND UPPER(COALESCE(band, '')) = ?
                        """,
                        (group, band_uc),
                    )
                else:
                    cur = conn.execute(
                        f"""
                        SELECT day_utc, start_utc, end_utc
                        FROM {table}
                        WHERE UPPER(COALESCE(group_name, '')) = ?
                        """,
                        (group,),
                    )
                for row in cur.fetchall():
                    day = (row[0] or "").strip()
                    start = (row[1] or "").strip()
                    end = (row[2] or "").strip()
                    if day and start and end:
                        windows.append((day, start, end))
        except Exception as e:
            log.debug("SOP: load schedule windows failed: %s", e)
        finally:
            conn.close()
        return windows

    def is_due_aligned_with_schedule(self, operating_group: str, band: str, frequency: str, due_utc: dt.datetime) -> bool:
        windows = self._load_schedule_windows(operating_group, band, frequency)
        if not windows:
            return True
        day_name = _day_name_from_utc(due_utc)
        due_hm = due_utc.strftime("%H:%M")
        for day, start, end in windows:
            d = day.strip()
            if d not in {day_name, "ALL"}:
                continue
            if start <= due_hm <= end:
                return True
        return False

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
                interval_m = int(action.get("interval_minutes") or 0)
                if interval_m <= 0:
                    interval_m = max(1, int(action.get("interval_hours") or 3)) * 60
                interval_td = dt.timedelta(minutes=interval_m)
                next_due = self.compute_next_due(
                    full.get("sop_start_utc", "00:00"),
                    interval_m,
                    now_utc=now,
                    last_completed_utc=None,
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
            interval_td = dt.timedelta(minutes=interval_m)
            next_due = self.compute_next_due(
                full.get("sop_start_utc", "00:00"),
                interval_m,
                now_utc=now,
                last_completed_utc=action.get("last_completed_utc"),
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
            "schema_version": 1,
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
        name = (imported.get("name") or "Imported SOP").strip()

        existing_names = {p.get("name", "").lower() for p in self.list_profiles()}
        base = name
        suffix = 1
        while name.lower() in existing_names:
            suffix += 1
            name = f"{base} ({suffix})"
        imported["name"] = name

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
                        "description": (a.get("description") or "").strip(),
                        "contact_rule": (a.get("contact_rule") or "none").strip(),
                        "contact_target": (a.get("contact_target") or "").strip().upper(),
                        "sort_order": int(a.get("sort_order") if a.get("sort_order") is not None else idx),
                    }
                )
        return self.save_profile(imported, normalized_actions)

    @staticmethod
    def dumps_json(payload: Dict[str, Any]) -> str:
        def _default(o: Any) -> Any:
            if isinstance(o, dt.datetime):
                return o.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()
            raise TypeError(f"Unsupported type: {type(o)!r}")

        return json.dumps(payload, indent=2, sort_keys=True, default=_default)
