from __future__ import annotations

import datetime
import time
import sqlite3
from typing import Any, Dict, List, Optional, Set, Tuple

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QColorDialog,
    QDialog,
)

from pathlib import Path

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.core.config_paths import get_config_dir
from freqinout.utils.timezones import get_timezone
from freqinout.gui.theme import resolve_theme, button_style, band_cell_colors, qcolor, BAND_COLORS_LIGHT, BAND_COLORS_DARK

DAY_NAMES = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]
DAY_NAMES_UPPER = [d.upper() for d in DAY_NAMES]


class FreqPlannerTab(QWidget):
    """
    Frequency planner view.

    - Rows: hours 00..23 (UTC hour buckets)
    - Columns:
        0: UTC Hour
        1: Local Time (HH:00 AM/PM TZ)
        2-8: Sunday .. Saturday

    Cell contents:
      - If only HF schedule applies at that hour: show the band (or multiple bands as "40M / 80M").
      - If one or more nets apply: show "band|net name" or "band1 / band2|net1 / net2".
      - Uses hf_schedule (or legacy daily_schedule) and net_schedule from config.json.

    Highlighting:
      - Current UTC weekday column cells are highlighted *only if* they have a net in that hour.

    Local time:
      - Uses the timezone stored in Settings ("timezone") via get_timezone(), so it is
        consistent and cross-platform.
    """

    COL_UTC = 0
    COL_LOCAL = 1
    COL_DAY_OFFSET = 2  # Sunday at column 2

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        default_mode = (self.settings.get("display_time_mode", "LOCAL") or "LOCAL").upper()
        self._show_local = default_mode != "UTC"
        self._show_band = True
        self._band_colors: Dict[str, str] = {}
        self._visible_bands: List[str] = []
        self._clock_timer: QTimer | None = None
        self._last_snapshot: str = ""
        self._last_rebuild_check_ts: float = 0.0
        self._build_ui()
        self._apply_theme()
        self.rebuild_table()

    # ------------- UI ------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>FreqPlanner</h3>"))
        header.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        self.time_toggle_btn = QPushButton("Showing: Local" if self._show_local else "Showing: UTC")
        theme = resolve_theme(self.settings)
        self.time_toggle_btn.setStyleSheet(button_style("primary", theme))
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        header.addWidget(self.time_toggle_btn)
        layout.addLayout(header)

        self.band_legend = QWidget()
        self.band_legend_layout = QHBoxLayout(self.band_legend)
        self.band_legend_layout.setContentsMargins(0, 0, 0, 0)
        self.band_legend_layout.setSpacing(6)
        self.band_toggle_btn = QPushButton("Showing Band")
        self.band_toggle_btn.setStyleSheet(button_style("info", theme))
        self.band_toggle_btn.clicked.connect(self._toggle_band_view)
        self.band_legend_layout.addWidget(self.band_toggle_btn)
        layout.addWidget(self.band_legend)

        self.table = QTableWidget()
        self.table.setRowCount(24)
        self.table.setColumnCount(9)  # UTC, Local, Sun..Sat

        # Set headers with local TZ name in Local column
        tz_name, tz_abbr = self._current_timezone_label()
        self.table.setHorizontalHeaderLabels(
            [
                "UTC Hour",
                f"Local Time ({tz_abbr})",
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]
        )

        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_UTC, QHeaderView.Stretch)
        hv.setSectionResizeMode(self.COL_LOCAL, QHeaderView.Stretch)
        for col in range(self.COL_DAY_OFFSET, 9):
            hv.setSectionResizeMode(col, QHeaderView.Stretch)
        hv.setHighlightSections(False)

        layout.addWidget(self.table)

        self._setup_clock_timer()
        self._load_band_colors()
        self._render_band_legend()

    # ------------- helpers ------------- #

    def _current_timezone(self) -> tuple[str, datetime.tzinfo]:
        """
        Returns (tz_name, tzinfo) using the Settings timezone and the
        shared get_timezone() helper so it works on all platforms.
        """
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        return tz_name, tz

    def _current_timezone_label(self) -> tuple[str, str]:
        """
        Returns (tz_name, tz_abbr) for labeling the Local column header.
        Uses tzname() when available, otherwise a short fallback (ET/CT/MT/PT/UTC).
        """
        tz_name, tz = self._current_timezone()
        now = datetime.datetime.now(tz)
        abbr = now.tzname() or self._ui_tz_abbr(tz_name, tz_name)
        if abbr and len(abbr) > 5:
            abbr = self._ui_tz_abbr(tz_name, abbr)
        return tz_name, abbr

    @staticmethod
    def _normalize_condition_levels(value: Any) -> str:
        raw = str(value or "").strip().upper()
        if not raw or raw == "ALL":
            return "ALL"
        vals: List[int] = []
        for token in raw.replace(";", ",").replace("|", ",").split(","):
            token = token.strip()
            if not token:
                continue
            if token == "ALL":
                return "ALL"
            try:
                lvl = int(token)
            except Exception:
                continue
            if 1 <= lvl <= 5:
                vals.append(lvl)
        if not vals:
            return "ALL"
        return ",".join(str(v) for v in sorted(set(vals)))

    def _condition_level_map(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        rows = self.settings.get("operating_groups", []) or []
        if not isinstance(rows, list):
            return out
        for row in rows:
            if not isinstance(row, dict):
                continue
            group = str(row.get("group", "") or "").strip().upper()
            if not group:
                continue
            if not bool(row.get("use_condition_levels", False)):
                continue
            try:
                level = int(row.get("condition_level", 0) or 0)
            except Exception:
                level = 0
            if not (1 <= level <= 5):
                continue
            prev = out.get(group)
            if prev is None or level < prev:
                out[group] = level
        return out

    @classmethod
    def _condition_level_match(cls, condition_levels: str, group_level: Optional[int]) -> bool:
        normalized = cls._normalize_condition_levels(condition_levels)
        if normalized == "ALL":
            return True
        if group_level is None:
            return True
        allowed: Set[int] = set()
        for token in normalized.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                lvl = int(token)
            except Exception:
                continue
            if 1 <= lvl <= 5:
                allowed.add(lvl)
        if not allowed:
            return True
        return int(group_level) in allowed

    def _load_schedules(self) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict[str, Any]]]:
        data = self.settings.all()

        # Try DB-backed schedules first
        hf_db = self._load_hf_from_db()
        net_db = self._load_net_from_db()
        sop_db = self._load_sop_layer_from_db()
        policy_db = self._load_net_sop_policies_from_db()

        hf = hf_db if hf_db is not None else data.get("hf_schedule") or data.get("daily_schedule") or []
        net = net_db if net_db is not None else data.get("net_schedule") or []
        sop = sop_db if sop_db is not None else []
        policies = policy_db if policy_db is not None else []
        if not isinstance(hf, list):
            hf = []
        if not isinstance(net, list):
            net = []
        if not isinstance(sop, list):
            sop = []
        if not isinstance(policies, list):
            policies = []
        return hf, net, sop, policies

    @staticmethod
    def _policy_overlap(
        a_start: datetime.datetime,
        a_end: datetime.datetime,
        b_start: datetime.datetime,
        b_end: datetime.datetime,
    ) -> bool:
        return a_start < b_end and b_start < a_end

    @staticmethod
    def _parse_iso_utc(value: str) -> Optional[datetime.datetime]:
        txt = str(value or "").strip()
        if not txt:
            return None
        try:
            parsed = datetime.datetime.fromisoformat(txt.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=datetime.timezone.utc)
            return parsed.astimezone(datetime.timezone.utc)
        except Exception:
            return None

    def _load_net_sop_policies_from_db(self) -> Optional[List[Dict[str, Any]]]:
        conn: sqlite3.Connection | None = None
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            exists = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='sop_net_conflict_policy'"
            ).fetchone()
            if not exists:
                return []
            cur.execute(
                """
                SELECT
                    policy,
                    window_start_utc,
                    window_end_utc,
                    net_row_signature,
                    sop_row_signature
                FROM sop_net_conflict_policy
                WHERE COALESCE(active, 1) = 1
                ORDER BY COALESCE(updated_utc, '') DESC, id DESC
                """
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            for policy, start_utc, end_utc, net_sig, sop_sig in rows:
                pol = str(policy or "").strip().upper()
                if pol not in {"SOP_PRIORITY", "NET_PRIORITY"}:
                    continue
                start_dt = self._parse_iso_utc(str(start_utc or ""))
                end_dt = self._parse_iso_utc(str(end_utc or ""))
                if not isinstance(start_dt, datetime.datetime) or not isinstance(end_dt, datetime.datetime):
                    continue
                if end_dt <= start_dt:
                    continue
                out.append(
                    {
                        "policy": pol,
                        "start_utc": start_dt,
                        "end_utc": end_dt,
                        "net_row_signature": str(net_sig or "").strip(),
                        "sop_row_signature": str(sop_sig or "").strip(),
                    }
                )
            return out
        except Exception:
            return None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _effective_net_sop_policy_for_window(
        self,
        *,
        cell_start_utc: datetime.datetime,
        cell_end_utc: datetime.datetime,
        net_slices: List[Tuple[int, int, str, str]],
        sop_slices: List[Tuple[int, int, str, str]],
        policy_rows: List[Dict[str, Any]],
    ) -> str:
        saw_sop = False
        for n_start, n_end, _n_label, n_sig in net_slices:
            if n_end <= n_start:
                continue
            for s_start, s_end, _s_label, s_sig in sop_slices:
                if s_end <= s_start:
                    continue
                overlap_start_min = max(n_start, s_start)
                overlap_end_min = min(n_end, s_end)
                if overlap_end_min <= overlap_start_min:
                    continue
                overlap_start_utc = cell_start_utc + datetime.timedelta(minutes=overlap_start_min)
                overlap_end_utc = cell_start_utc + datetime.timedelta(minutes=overlap_end_min)
                for row in policy_rows:
                    if str(row.get("net_row_signature") or "") != str(n_sig or ""):
                        continue
                    if str(row.get("sop_row_signature") or "") != str(s_sig or ""):
                        continue
                    policy = str(row.get("policy") or "").strip().upper()
                    start_dt = row.get("start_utc")
                    end_dt = row.get("end_utc")
                    if not isinstance(start_dt, datetime.datetime) or not isinstance(end_dt, datetime.datetime):
                        continue
                    if not self._policy_overlap(overlap_start_utc, overlap_end_utc, start_dt, end_dt):
                        continue
                    if policy == "NET_PRIORITY":
                        return "NET_PRIORITY"
                    if policy == "SOP_PRIORITY":
                        saw_sop = True
        return "SOP_PRIORITY" if saw_sop else ""

    @staticmethod
    def _normalize_day_for_signature(day: str) -> str:
        raw = str(day or "").strip()
        if not raw:
            return "ALL"
        up = raw.upper()
        if up in {"ALL", "DAILY"}:
            return "ALL"
        for opt in DAY_NAMES:
            if up.startswith(opt[:3].upper()):
                return opt
        return raw

    @staticmethod
    def _normalize_recurrence_for_signature(recurrence: str) -> str:
        raw = str(recurrence or "Weekly").strip().upper()
        if raw == "MONTHLY":
            raw = "PERIODIC"
        if raw in {"DAILY", "PERIODIC", "BI-WEEKLY", "WEEKLY"}:
            return "Bi-Weekly" if raw == "BI-WEEKLY" else raw.title()
        return "Weekly"

    @staticmethod
    def _normalize_frequency_for_signature(value: Any) -> str:
        txt = str(value or "").strip()
        if not txt:
            return ""
        try:
            return f"{float(txt):.3f}"
        except Exception:
            return txt

    def _normalize_month_weeks_for_signature(self, value: Any) -> str:
        weeks = self._parse_month_weeks(str(value or ""))
        return ",".join(str(v) for v in weeks)

    def _net_row_signature(self, row: Dict[str, Any]) -> str:
        day = self._normalize_day_for_signature(str(row.get("day_utc") or "ALL"))
        recurrence = self._normalize_recurrence_for_signature(str(row.get("recurrence") or "Weekly"))
        biweekly = int(row.get("biweekly_offset_weeks") or 0)
        weeks = self._normalize_month_weeks_for_signature(row.get("month_weeks"))
        group = str(row.get("group_name") or "").strip().upper()
        band = str(row.get("band") or "").strip().upper()
        freq = self._normalize_frequency_for_signature(row.get("frequency"))
        start = str(row.get("start_utc") or "").strip()
        end = str(row.get("end_utc") or "").strip()
        net_name = str(row.get("net_name") or row.get("name") or "").strip().upper()
        return (
            f"NET|{group}|{band}|{freq}|{day}|{recurrence}|{biweekly}|"
            f"{weeks}|{start}|{end}|{net_name}"
        )

    def _sop_row_signature(self, row: Dict[str, Any]) -> str:
        day = self._normalize_day_for_signature(str(row.get("day_utc") or "ALL"))
        recurrence = self._normalize_recurrence_for_signature(str(row.get("recurrence") or "Weekly"))
        biweekly = int(row.get("biweekly_offset_weeks") or 0)
        weeks = self._normalize_month_weeks_for_signature(row.get("month_weeks"))
        group = str(row.get("group_name") or "").strip().upper()
        band = str(row.get("band") or "").strip().upper()
        freq = self._normalize_frequency_for_signature(row.get("frequency"))
        start = str(row.get("start_utc") or "").strip()
        end = str(row.get("end_utc") or "").strip()
        profile_id = int(row.get("sop_profile_id") or 0)
        layer_id = int(row.get("sop_layer_id") or row.get("id") or 0)
        return (
            f"SOP|{profile_id}|{layer_id}|{group}|{band}|{freq}|{day}|"
            f"{recurrence}|{biweekly}|{weeks}|{start}|{end}"
        )

    def _load_hf_from_db(self) -> Optional[List[Dict]]:
        """
        Load HF/daily schedule from config/freqinout.db if available.
        """
        try:
            db_path = get_config_dir() / "config" / "freqinout.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT day_utc, band, mode, vfo, frequency, start_utc, end_utc, group_name, auto_tune
                FROM daily_schedule_tab
                """
            )
            rows = cur.fetchall()
            conn.close()
            out = []
            for day_utc, band, mode, vfo, freq, start_utc, end_utc, group_name, auto_tune in rows:
                out.append(
                    {
                        "day_utc": day_utc or "ALL",
                        "band": band or "",
                        "mode": mode or "",
                        "vfo": (vfo or "A").strip().upper() or "A",
                        "frequency": str(freq or ""),
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "group_name": group_name or "",
                        "auto_tune": bool(auto_tune),
                    }
                )
            return out
        except Exception:
            return None

    def _load_net_from_db(self) -> Optional[List[Dict]]:
        """
        Load net schedule from config/freqinout_nets.db if available.
        """
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            rows = []
            try:
                cur.execute(
                    """
                    SELECT day_utc, recurrence, biweekly_offset_weeks, month_weeks, band, mode, vfo, frequency,
                           start_utc, end_utc, early_checkin, primary_js8call_group, comment, net_name, group_name
                    FROM net_schedule_tab
                    """
                )
                rows = cur.fetchall()
            except Exception:
                rows = []
            # Fallback to legacy table if the richer table is empty/missing
            if not rows:
                try:
                    cur.execute(
                        """
                        SELECT day_utc, recurrence, biweekly_offset_weeks, band, mode, frequency,
                               start_utc, end_utc, early_checkin, primary_js8call_group, comment, net_name
                        FROM net_schedule
                        """
                    )
                    legacy = cur.fetchall()
                    # Pad legacy rows to align with expected tuple positions (insert vfo=None, group_name='')
                    rows = [
                        (
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            "",
                            band,
                            mode,
                            None,
                            freq,
                            start_utc,
                            end_utc,
                             early_checkin,
                             primary_js8call_group,
                             comment,
                             net_name,
                             "",
                         )
                         for (
                             day_utc,
                             recurrence,
                            biweekly_offset_weeks,
                            band,
                            mode,
                            freq,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name,
                        ) in legacy
                    ]
                except Exception:
                    rows = []
            conn.close()
            out = []
            for (
                day_utc,
                recurrence,
                biweekly_offset_weeks,
                month_weeks,
                band,
                mode,
                vfo,
                freq,
                start_utc,
                end_utc,
                early_checkin,
                primary_js8call_group,
                comment,
                net_name,
                group_name,
            ) in rows:
                out.append(
                    {
                        "day_utc": day_utc or "ALL",
                        "recurrence": recurrence or "Weekly",
                        "biweekly_offset_weeks": biweekly_offset_weeks or 0,
                        "month_weeks": month_weeks or "",
                        "band": band or "",
                        "mode": mode or "",
                        "vfo": (vfo or "A").strip().upper() or "A",
                        "frequency": str(freq or ""),
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "early_checkin": int(early_checkin or 0),
                        "primary_js8call_group": primary_js8call_group or "",
                        "comment": comment or "",
                        "net_name": net_name or "",
                        "group_name": group_name or primary_js8call_group or "",
                    }
                )
            return out
        except Exception:
            return None

    def _load_sop_layer_from_db(self) -> Optional[List[Dict[str, Any]]]:
        """
        Load HF SOP schedule-layer rows from config/freqinout_nets.db.
        Only active SOP profiles are considered for planner overlay rows.
        """
        conn: sqlite3.Connection | None = None
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            profile_cols: set[str] = set()
            try:
                cur.execute("PRAGMA table_info(sop_profiles)")
                profile_cols = {str(r[1] or "").strip().lower() for r in cur.fetchall() if len(r) > 1}
            except Exception:
                profile_cols = set()
            has_secondary_group = "secondary_group" in profile_cols
            layer_cols: set[str] = set()
            try:
                cur.execute("PRAGMA table_info(sop_schedule_layer)")
                layer_cols = {str(r[1] or "").strip().lower() for r in cur.fetchall() if len(r) > 1}
            except Exception:
                layer_cols = set()
            condition_expr = "COALESCE(l.condition_levels, 'ALL')" if "condition_levels" in layer_cols else "'ALL'"
            profile_group_expr = (
                "COALESCE(NULLIF(TRIM(p.operating_group), ''), NULLIF(TRIM(p.secondary_group), ''),"
                " NULLIF(TRIM(p.name), ''), '')"
                if has_secondary_group
                else "COALESCE(NULLIF(TRIM(p.operating_group), ''), NULLIF(TRIM(p.name), ''), '')"
            )
            layer_group_expr = (
                f"COALESCE(NULLIF(TRIM(l.group_name), ''), {profile_group_expr})"
                if "group_name" in layer_cols
                else profile_group_expr
            )
            base_sql = """
                SELECT
                    COALESCE(l.id, 0),
                    COALESCE(l.profile_id, 0),
                    COALESCE(l.day_utc, 'ALL'),
                    COALESCE(l.recurrence, 'Weekly'),
                    COALESCE(l.biweekly_offset_weeks, 0),
                    COALESCE(l.month_weeks, ''),
                    {condition_expr},
                    COALESCE(l.band, ''),
                    COALESCE(l.mode, ''),
                    COALESCE(l.vfo, 'A'),
                    COALESCE(l.frequency, ''),
                    COALESCE(l.start_utc, ''),
                    COALESCE(l.end_utc, ''),
                    {layer_group_expr},
                    COALESCE(p.name, '')
                FROM sop_schedule_layer l
                JOIN sop_profiles p ON p.id = l.profile_id
                WHERE COALESCE(l.enabled, 1) = 1
                  AND (
                        TRIM(COALESCE(l.band, '')) <> ''
                        OR TRIM(COALESCE(l.frequency, '')) <> ''
                  )
                  {active_clause}
                ORDER BY p.operating_group COLLATE NOCASE, l.day_utc, l.start_utc
            """
            cur.execute(
                base_sql.format(
                    layer_group_expr=layer_group_expr,
                    condition_expr=condition_expr,
                    active_clause="AND COALESCE(p.active, 0) = 1",
                )
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            cond_map = self._condition_level_map()
            for (
                layer_id,
                profile_id,
                day_utc,
                recurrence,
                biweekly_offset_weeks,
                month_weeks,
                condition_levels,
                band,
                mode,
                vfo,
                freq,
                start_utc,
                end_utc,
                operating_group,
                profile_name,
            ) in rows:
                group_name = str(operating_group or "").strip().upper()
                group_level = cond_map.get(group_name)
                if not self._condition_level_match(str(condition_levels or "ALL"), group_level):
                    continue
                out.append(
                    {
                        "id": int(layer_id or 0),
                        "sop_layer_id": int(layer_id or 0),
                        "sop_profile_id": int(profile_id or 0),
                        "day_utc": day_utc or "ALL",
                        "recurrence": recurrence or "Weekly",
                        "biweekly_offset_weeks": biweekly_offset_weeks or 0,
                        "month_weeks": month_weeks or "",
                        "condition_levels": self._normalize_condition_levels(condition_levels),
                        "band": band or "",
                        "mode": mode or "",
                        "vfo": (vfo or "A").strip().upper() or "A",
                        "frequency": str(freq or ""),
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "group_name": group_name,
                        "profile_name": str(profile_name or "").strip(),
                    }
                )
            return out
        except Exception:
            return None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _parse_hhmm(self, s: str) -> int | None:
        s = (s or "").strip()
        if not s:
            return None
        try:
            h, m = s.split(":")
            h = int(h)
            m = int(m)
            if 0 <= h <= 23 and 0 <= m <= 59:
                return h * 60 + m
        except Exception:
            return None
        return None

    def _week_start_sunday_utc(self, now_utc: datetime.datetime) -> datetime.date:
        delta = (now_utc.weekday() + 1) % 7  # Sunday=0
        return (now_utc - datetime.timedelta(days=delta)).date()

    def _month_week_index(self, date_val: datetime.date) -> int:
        return 1 + ((date_val.day - 1) // 7)

    def _parse_month_weeks(self, txt: str) -> List[int]:
        weeks: List[int] = []
        for token in (txt or "").split(","):
            token = token.strip()
            if not token:
                continue
            try:
                val = int(token)
            except Exception:
                continue
            if 1 <= val <= 5:
                weeks.append(val)
        return sorted(set(weeks))

    def _net_row_applies_this_week(
        self, row: Dict, targets: List[str], week_sunday: datetime.date
    ) -> bool:
        recurrence = (row.get("recurrence") or "Weekly").strip()
        if recurrence not in ("Periodic", "Monthly"):
            return True
        weeks = self._parse_month_weeks(row.get("month_weeks", ""))
        if not weeks:
            weeks = [1]
        for idx, day_name in enumerate(DAY_NAMES):
            if day_name not in targets:
                continue
            date_val = week_sunday + datetime.timedelta(days=idx)
            if self._month_week_index(date_val) in weeks:
                return True
        return False

    def _hour_overlaps(self, start_min: int, end_min: int, hour: int) -> bool:
        """
        Returns True if the [start_min, end_min] interval overlaps any minute in this hour bucket.
        """
        hour_start = hour * 60
        hour_end = hour * 60 + 59
        return not (end_min < hour_start or start_min > hour_end)

    def _next_day(self, day_name_upper: str) -> str:
        try:
            idx = DAY_NAMES_UPPER.index(day_name_upper)
            return DAY_NAMES[(idx + 1) % 7]
        except Exception:
            return DAY_NAMES[0]

    def _expand_hours_for_day(self, day_val: str, start_min: int, end_min: int, *, early: int = 0) -> List[tuple[str, int]]:
        """
        Expand a schedule row into (day_name, hour) tuples, handling ALL and overnight spans.
        Times are in minutes from 00:00 UTC. early applies only to net rows (already adjusted).
        """
        targets: List[str] = []
        day_txt = (day_val or "ALL").strip().upper()
        if day_txt == "ALL" or day_txt not in DAY_NAMES_UPPER:
            targets = DAY_NAMES[:]  # all days in Title case
        else:
            # Title-case version from canonical list
            targets = [DAY_NAMES[DAY_NAMES_UPPER.index(day_txt)]]

        hours: List[tuple[str, int]] = []
        smin = start_min
        emin = end_min
        overnight = smin > emin

        for day_name in targets:
            day_upper = day_name.upper()
            if not overnight:
                for h in range(24):
                    if self._hour_overlaps(smin, emin, h):
                        hours.append((day_name, h))
            else:
                # Segment 1: from start to 23:59 on current day
                for h in range(24):
                    if self._hour_overlaps(smin, 23 * 60 + 59, h):
                        hours.append((day_name, h))
                # Segment 2: from 00:00 to end on next day
                next_day = self._next_day(day_upper)
                for h in range(24):
                    if self._hour_overlaps(0, emin, h):
                        hours.append((next_day, h))

        return hours

    def _net_window_for_day(
        self, row: Dict, day_name: str, now_utc: datetime.datetime
    ) -> Optional[tuple[datetime.datetime, datetime.datetime]]:
        """
        Given a net row and target day name, compute start/end UTC datetimes for that day.
        Returns None if times are invalid; caller filters by time window.
        """
        start_m = self._parse_hhmm(row.get("start_utc", ""))
        end_m = self._parse_hhmm(row.get("end_utc", ""))
        if start_m is None or end_m is None:
            return None
        overnight = start_m > end_m
        if not overnight:
            if end_m % 60 == 0:
                end_m = min(end_m + 60, 24 * 60)

        # Map day_name to offset from current UTC day (DAY_NAMES starts with Sunday=0)
        try:
            day_idx = DAY_NAMES.index(day_name)
        except ValueError:
            return None
        now_idx = now_utc.weekday()  # Monday=0
        now_day_sun0 = (now_idx + 1) % 7  # convert to Sunday=0..Saturday=6
        offset = (day_idx - now_day_sun0) % 7

        base_date = now_utc.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=offset)
        start_dt = base_date + datetime.timedelta(minutes=start_m)
        end_dt = base_date + datetime.timedelta(minutes=end_m)
        if overnight:
            end_dt += datetime.timedelta(days=1)
        return start_dt, end_dt

    def _start_of_week_local(self, tz: datetime.tzinfo) -> datetime.datetime:
        """
        Returns a datetime for Sunday 00:00 of the current local week.
        """
        now_local = datetime.datetime.now(tz)
        # weekday: Monday=0, Sunday=6; want Sunday as start -> offset (weekday+1) % 7
        days_to_sunday = (now_local.weekday() + 1) % 7
        start_date = (now_local - datetime.timedelta(days=days_to_sunday)).date()
        return datetime.datetime.combine(start_date, datetime.time(0, 0)).replace(tzinfo=tz)

    # ------------- core rebuild ------------- #

    def rebuild_table(self):
        """
        Recompute the table based on current hf_schedule and net_schedule in config.
        """
        try:
            self.settings.reload()
        except Exception:
            pass
        self.table.clearContents()
        tz_name, tz_abbr = self._current_timezone_label()
        if not self._show_local:
            headers = [
                "UTC Hour",
                f"Local Time ({tz_abbr})",
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]
        else:
            headers = [
                f"Local Hour ({tz_abbr})",
                "UTC Time",
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]
        self.table.setHorizontalHeaderLabels(headers)

        hf_sched, net_sched, sop_sched, policy_rows = self._load_schedules()
        theme = resolve_theme(self.settings)
        self._last_snapshot = self._snapshot(hf_sched, net_sched, sop_sched, policy_rows)
        now_utc = datetime.datetime.utcnow()
        week_sunday = self._week_start_sunday_utc(now_utc)

        # Precompute net schedule coverage by (day, hour) with boundary-aware logic.
        # Slice tuple: (start_minute, end_minute, label, net_row_signature)
        net_cover: Dict[tuple, List[Tuple[int, int, str, str]]] = {}

        def add_net_slice(
            day_name: str,
            hour: int,
            start_minute: int,
            end_minute: int,
            name: str,
            row_sig: str,
        ) -> None:
            if start_minute >= end_minute:
                return
            net_cover.setdefault((day_name, hour), []).append((start_minute, end_minute, name, row_sig))

        for row in net_sched:
            try:
                day = row.get("day_utc", "")
                smin = self._parse_hhmm(row.get("start_utc", ""))
                emin = self._parse_hhmm(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue
                name = (row.get("net_name") or "Net").strip()
                row_sig = self._net_row_signature(row)
                recurrence = (row.get("recurrence") or "Weekly").strip()
                day_txt = (day or "ALL").strip().upper()
                if recurrence == "Daily":
                    day_txt = "ALL"
                targets = DAY_NAMES if day_txt == "ALL" or day_txt not in DAY_NAMES_UPPER else [
                    DAY_NAMES[DAY_NAMES_UPPER.index(day_txt)]
                ]
                if not self._net_row_applies_this_week(row, targets, week_sunday):
                    continue
                overnight = smin > emin
                intervals: List[Tuple[str, int, int]] = []
                if not overnight:
                    for dname in targets:
                        intervals.append((dname, smin, emin))
                else:
                    for dname in targets:
                        intervals.append((dname, smin, 24 * 60))
                        next_idx = (DAY_NAMES.index(dname) + 1) % 7
                        next_day = DAY_NAMES[next_idx]
                        intervals.append((next_day, 0, emin))
                for dname, seg_start, seg_end in intervals:
                    start_hour = seg_start // 60
                    end_hour = (seg_end - 1) // 60
                    for hour in range(start_hour, end_hour + 1):
                        hour_start_min = hour * 60
                        hour_end_min = hour * 60 + 60
                        overlap_start = max(seg_start, hour_start_min)
                        overlap_end = min(seg_end, hour_end_min)
                        add_net_slice(
                            dname,
                            hour % 24,
                            overlap_start - hour_start_min,
                            overlap_end - hour_start_min,
                            name,
                            row_sig,
                        )
            except Exception:
                continue

        # Precompute active SOP layer coverage by (day, hour).
        # Slice tuple: (start_minute, end_minute, label, sop_row_signature)
        sop_cover: Dict[tuple, List[Tuple[int, int, str, str]]] = {}

        def add_sop_slice(
            day_name: str,
            hour: int,
            start_minute: int,
            end_minute: int,
            label: str,
            row_sig: str,
        ) -> None:
            if start_minute >= end_minute:
                return
            sop_cover.setdefault((day_name, hour), []).append((start_minute, end_minute, label, row_sig))

        for row in sop_sched:
            try:
                day = row.get("day_utc", "")
                smin = self._parse_hhmm(row.get("start_utc", ""))
                emin = self._parse_hhmm(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue
                row_sig = self._sop_row_signature(row)
                recurrence = (row.get("recurrence") or "Weekly").strip()
                day_txt = (day or "ALL").strip().upper()
                if recurrence.upper() == "DAILY":
                    day_txt = "ALL"
                targets = DAY_NAMES if day_txt == "ALL" or day_txt not in DAY_NAMES_UPPER else [
                    DAY_NAMES[DAY_NAMES_UPPER.index(day_txt)]
                ]
                if not self._net_row_applies_this_week(row, targets, week_sunday):
                    continue
                group_name = str(row.get("group_name") or row.get("profile_name") or "").strip()
                label = f"SOP:{group_name}" if group_name else "SOP"
                overnight = smin > emin
                intervals: List[Tuple[str, int, int]] = []
                if not overnight:
                    for dname in targets:
                        intervals.append((dname, smin, emin))
                else:
                    for dname in targets:
                        intervals.append((dname, smin, 24 * 60))
                        next_idx = (DAY_NAMES.index(dname) + 1) % 7
                        next_day = DAY_NAMES[next_idx]
                        intervals.append((next_day, 0, emin))
                for dname, seg_start, seg_end in intervals:
                    start_hour = seg_start // 60
                    end_hour = (seg_end - 1) // 60
                    for hour in range(start_hour, end_hour + 1):
                        hour_start_min = hour * 60
                        hour_end_min = hour * 60 + 60
                        overlap_start = max(seg_start, hour_start_min)
                        overlap_end = min(seg_end, hour_end_min)
                        add_sop_slice(
                            dname,
                            hour % 24,
                            overlap_start - hour_start_min,
                            overlap_end - hour_start_min,
                            label,
                            row_sig,
                        )
            except Exception:
                continue

        # Precompute HF schedule coverage by (day, hour) with minute-level slices
        hf_cover: Dict[tuple, List[Tuple[int, int, str, str]]] = {}

        def add_slice(
            day_name: str, hour: int, start_minute: int, end_minute: int, band: str, freq: str
        ) -> None:
            if start_minute >= end_minute:
                return
            hf_cover.setdefault((day_name, hour), []).append((start_minute, end_minute, band, freq))

        # Collect start minutes per day to resolve boundary ownership
        starts_by_day: Dict[str, set[int]] = {d: set() for d in DAY_NAMES}
        for row in hf_sched:
            try:
                smin = self._parse_hhmm(row.get("start_utc", ""))
                if smin is None:
                    continue
                day_txt = (row.get("day_utc", "ALL") or "").strip().upper()
                targets = DAY_NAMES if day_txt == "ALL" or day_txt not in DAY_NAMES_UPPER else [
                    DAY_NAMES[DAY_NAMES_UPPER.index(day_txt)]
                ]
                for dname in targets:
                    starts_by_day[dname].add(smin)
            except Exception:
                continue

        for row in hf_sched:
            try:
                smin = self._parse_hhmm(row.get("start_utc", ""))
                emin = self._parse_hhmm(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue
                band = (row.get("band") or "").strip()
                if not band:
                    continue
                freq = (row.get("frequency") or "").strip()
                day_txt = (row.get("day_utc", "ALL") or "").strip().upper()
                # Expand into day/hour slices with minute precision
                targets = DAY_NAMES if day_txt == "ALL" or day_txt not in DAY_NAMES_UPPER else [
                    DAY_NAMES[DAY_NAMES_UPPER.index(day_txt)]
                ]
                overnight = smin > emin
                intervals: List[Tuple[str, int, int]] = []
                for dname in targets:
                    if not overnight:
                        intervals.append((dname, smin, emin))
                    else:
                        # segment 1: start -> 24h on current day
                        intervals.append((dname, smin, 24 * 60))
                        # segment 2: 0 -> end on next day
                        next_idx = (DAY_NAMES.index(dname) + 1) % 7
                        next_day = DAY_NAMES[next_idx]
                        intervals.append((next_day, 0, emin))
                for dname, seg_start, seg_end in intervals:
                    # If this segment ends exactly on an hour boundary, extend to cover that hour
                    # unless another HF row starts at that exact minute on the same day.
                    if seg_end % 60 == 0 and (seg_end % (24 * 60)) not in starts_by_day.get(dname, set()):
                        seg_end = min(seg_end + 60, 24 * 60)
                    start_hour = seg_start // 60
                    end_hour = (seg_end - 1) // 60  # inclusive end minute
                    for hour in range(start_hour, end_hour + 1):
                        hour_start_min = hour * 60
                        hour_end_min = hour * 60 + 60
                        overlap_start = max(seg_start, hour_start_min)
                        overlap_end = min(seg_end, hour_end_min)
                        add_slice(
                            dname,
                            hour % 24,
                            overlap_start - hour_start_min,
                            overlap_end - hour_start_min,
                            band,
                            freq,
                        )
            except Exception:
                continue

        # Timezone for local conversion
        tz_name_cfg, tz = self._current_timezone()

        # Current UTC day for highlighting
        now_local = datetime.datetime.now(tz)
        now_plus_24 = now_utc + datetime.timedelta(hours=24)

        # Fill rows
        today_utc = now_utc.replace(minute=0, second=0, microsecond=0)
        week_start_local = self._start_of_week_local(tz)
        visible_bands: set[str] = set()

        for hour in range(24):
            if not self._show_local:
                # Column 0: UTC hour "HH:00"
                utc_item = QTableWidgetItem(f"{hour:02d}:00")
                utc_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if hour == now_utc.hour:
                    utc_item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, self.COL_UTC, utc_item)

                # Column 1: Local time using configured timezone
                utc_dt = datetime.datetime(
                    year=today_utc.year,
                    month=today_utc.month,
                    day=today_utc.day,
                    hour=hour,
                    minute=0,
                    second=0,
                    tzinfo=datetime.timezone.utc,
                )
                local_dt = utc_dt.astimezone(tz)
                local_hour_24 = local_dt.hour
                local_str = f"{local_hour_24:02d}:00"
                local_item = QTableWidgetItem(local_str)
                local_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if local_hour_24 == now_local.hour:
                    local_item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, self.COL_LOCAL, local_item)
            else:
                # Local hour as primary
                local_dt = week_start_local + datetime.timedelta(hours=hour)
                utc_dt = local_dt.astimezone(datetime.timezone.utc)

                local_item = QTableWidgetItem(f"{local_dt.hour:02d}:00")
                local_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if local_dt.hour == now_local.hour:
                    local_item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, self.COL_UTC, local_item)

                utc_item = QTableWidgetItem(f"{utc_dt.hour:02d}:00")
                utc_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if utc_dt.hour == now_utc.hour:
                    utc_item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, self.COL_LOCAL, utc_item)

            # Day columns 2..8
            for col in range(self.COL_DAY_OFFSET, 9):
                day_name = DAY_NAMES[col - self.COL_DAY_OFFSET]
                if not self._show_local:
                    lookup_day = day_name
                    lookup_hour = hour
                    cell_utc_start = datetime.datetime.combine(
                        week_sunday + datetime.timedelta(days=(col - self.COL_DAY_OFFSET)),
                        datetime.time(hour=hour, minute=0),
                    ).replace(tzinfo=datetime.timezone.utc)
                else:
                    # Local day/hour mapped to UTC day/hour for lookup
                    cell_local_dt = week_start_local + datetime.timedelta(days=(col - self.COL_DAY_OFFSET), hours=hour)
                    cell_dt_utc = cell_local_dt.astimezone(datetime.timezone.utc)
                    lookup_day = cell_dt_utc.strftime("%A")
                    lookup_hour = cell_dt_utc.hour
                    cell_utc_start = cell_dt_utc.replace(minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)
                cell_utc_end = cell_utc_start + datetime.timedelta(hours=1)

                net_slices = net_cover.get((lookup_day, lookup_hour), [])
                sop_slices = sop_cover.get((lookup_day, lookup_hour), [])
                hf_slices = hf_cover.get((lookup_day, lookup_hour), [])
                band_label = ""
                freq_label = ""
                if hf_slices:
                    # Order by start minute and compress consecutive identical bands
                    hf_slices = sorted(hf_slices, key=lambda x: x[0])
                    bands_in_order: List[str] = []
                    freqs_in_order: List[str] = []
                    last_band = None
                    last_freq = None
                    for start_m, end_m, b, f in hf_slices:
                        if end_m <= start_m:
                            continue
                        if b:
                            visible_bands.add(b.lower())
                        if b != last_band:
                            bands_in_order.append(b)
                            last_band = b
                        if f != last_freq and f:
                            freqs_in_order.append(f)
                            last_freq = f
                    if len(bands_in_order) == 1:
                        band_label = bands_in_order[0]
                    else:
                        band_label = "/".join(bands_in_order)
                    if len(freqs_in_order) == 1:
                        freq_label = freqs_in_order[0]
                    elif freqs_in_order:
                        freq_label = "/".join(freqs_in_order)

                net_label = ""
                if net_slices:
                    net_slices = sorted(net_slices, key=lambda x: x[0])
                    net_names = []
                    last_net = None
                    for start_m, end_m, name, _sig in net_slices:
                        if end_m <= start_m:
                            continue
                        if name != last_net:
                            net_names.append(name)
                            last_net = name
                    if net_names:
                        net_label = " / ".join(net_names)

                sop_label = ""
                if sop_slices:
                    sop_slices = sorted(sop_slices, key=lambda x: x[0])
                    sop_names: List[str] = []
                    seen_sop_names: set[str] = set()
                    for start_m, end_m, label, _sig in sop_slices:
                        if end_m <= start_m:
                            continue
                        raw = str(label or "").strip()
                        if raw.upper().startswith("SOP:"):
                            raw = raw[4:].strip()
                        name = raw or "SOP"
                        key = name.upper()
                        if key in seen_sop_names:
                            continue
                        seen_sop_names.add(key)
                        sop_names.append(name)
                    if sop_names:
                        sop_label = f"SOP:{'/'.join(sop_names)}"

                policy_pref = ""
                if net_label and sop_label:
                    policy_pref = self._effective_net_sop_policy_for_window(
                        cell_start_utc=cell_utc_start,
                        cell_end_utc=cell_utc_end,
                        net_slices=net_slices,
                        sop_slices=sop_slices,
                        policy_rows=policy_rows,
                    )

                cell_text = ""
                if net_label and sop_label:
                    # Display precedence with policy-aware Net/SOP arbitration.
                    cell_text = sop_label if policy_pref == "SOP_PRIORITY" else net_label
                elif net_label:
                    cell_text = net_label
                elif sop_label:
                    cell_text = sop_label
                else:
                    cell_text = band_label if self._show_band else (freq_label or band_label)

                item = QTableWidgetItem(cell_text)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if cell_text:
                    item.setToolTip(cell_text)

                if band_label and not net_label and not sop_label:
                    primary_band = band_label.split("/")[0].strip()
                    colors = self._band_cell_colors(primary_band, theme)
                    if colors:
                        item.setBackground(qcolor(colors["bg"]))
                        item.setForeground(qcolor(colors["fg"]))

                # Highlight: net window overlaps now or starts within next 24h
                highlight = False
                if net_slices:
                    for start_m, end_m, _name, _sig in net_slices:
                        # Compute absolute times for this day/hour slice
                        try:
                            day_idx = DAY_NAMES.index(lookup_day)
                        except ValueError:
                            continue
                        now_idx = now_utc.weekday()
                        now_day_sun0 = (now_idx + 1) % 7
                        offset = (day_idx - now_day_sun0) % 7
                        base_date = now_utc.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(
                            days=offset
                        )
                        start_dt = base_date + datetime.timedelta(minutes=start_m + lookup_hour * 60)
                        end_dt = base_date + datetime.timedelta(minutes=end_m + lookup_hour * 60)
                        if start_dt <= now_utc <= end_dt or (now_utc <= start_dt <= now_plus_24):
                            highlight = True
                            break
                if highlight:
                    item.setBackground(qcolor(theme["surface_alt"]))

                self.table.setItem(hour, col, item)

        # Update clock labels
        self._update_clock_labels()
        self._visible_bands = sorted(visible_bands)
        self._render_band_legend()
        log.info("FreqPlanner table rebuilt.")

    def _snapshot(
        self,
        hf_sched: List[Dict],
        net_sched: List[Dict],
        sop_sched: List[Dict],
        policy_rows: List[Dict[str, Any]],
    ) -> str:
        """
        Deterministic snapshot of schedules and time view to avoid unnecessary rebuilds.
        """
        parts = ["LOCAL" if self._show_local else "UTC", "BAND" if self._show_band else "FREQ"]
        for s in sorted(hf_sched, key=lambda x: (x.get("day_utc", ""), x.get("start_utc", ""), x.get("group_name", ""))):
            parts.append(
                f"H|{s.get('day_utc','')}|{s.get('group_name','')}|{s.get('start_utc','')}|{s.get('end_utc','')}|{s.get('band','')}"
            )
        for n in sorted(net_sched, key=lambda x: (x.get("day_utc", ""), x.get("start_utc", ""), x.get("net_name", ""))):
            parts.append(
                f"N|{n.get('day_utc','')}|{n.get('net_name','')}|{n.get('start_utc','')}|{n.get('end_utc','')}|{n.get('recurrence','')}|{n.get('month_weeks','')}"
            )
        for s in sorted(sop_sched, key=lambda x: (x.get("group_name", ""), x.get("day_utc", ""), x.get("start_utc", ""))):
            parts.append(
                f"S|{s.get('group_name','')}|{s.get('day_utc','')}|{s.get('start_utc','')}|{s.get('end_utc','')}|{s.get('recurrence','')}|{s.get('month_weeks','')}"
            )
        for p in sorted(
            policy_rows,
            key=lambda x: (
                str(x.get("policy") or ""),
                str(x.get("start_utc") or ""),
                str(x.get("end_utc") or ""),
                str(x.get("net_row_signature") or ""),
                str(x.get("sop_row_signature") or ""),
            ),
        ):
            parts.append(
                f"P|{p.get('policy','')}|{p.get('start_utc','')}|{p.get('end_utc','')}|"
                f"{p.get('net_row_signature','')}|{p.get('sop_row_signature','')}"
            )
        return ";".join(parts)

    def _maybe_rebuild_if_changed(self):
        hf_sched, net_sched, sop_sched, policy_rows = self._load_schedules()
        snap = self._snapshot(hf_sched, net_sched, sop_sched, policy_rows)
        if snap != self._last_snapshot:
            self.rebuild_table()

    def _ui_tz_abbr(self, tz_name: str, fallback: str) -> str:
        mapping = {
            "UTC": "UTC",
            "America/New_York": "ET",
            "America/Chicago": "CT",
            "America/Denver": "MT",
            "America/Los_Angeles": "PT",
            "Mountain Standard Time": "MST",
            "Central Standard Time": "CST",
            "Eastern Standard Time": "EST",
            "Pacific Standard Time": "PST",
        }
        return mapping.get(tz_name, fallback)

    def _update_clock_labels(self):
        """
        UTC from system clock; local time derived via Settings timezone + get_timezone(),
        with a UI label like ET / CT / MT / PT / UTC.
        """
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a")
        self.utc_label.setText(now_utc.strftime(f"<b>UTC ({utc_day}):</b> %y%m%d %H:%M:%S Z"))

        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        now_local = now_utc.astimezone(tz)
        fallback = now_local.tzname() or tz_name
        abbr = self._ui_tz_abbr(tz_name, fallback)

        local_day = now_local.strftime("%a")
        self.local_label.setText(
            now_local.strftime(f"<b>Local ({local_day}):</b> %y%m%d %H:%M:%S {abbr}")
        )
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")
        self._update_toggle_button_styles()

    def _setup_clock_timer(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._on_clock_tick)
        self._clock_timer.start(1000)

    def _on_clock_tick(self):
        self._update_clock_labels()
        now_ts = time.time()
        if now_ts - self._last_rebuild_check_ts >= 2.0:
            self._last_rebuild_check_ts = now_ts
            self._maybe_rebuild_if_changed()

    def set_tab_active(self, active: bool) -> None:
        if active:
            if self._clock_timer is None:
                self._setup_clock_timer()
            elif not self._clock_timer.isActive():
                self._clock_timer.start(1000)
            self._update_clock_labels()
            self._maybe_rebuild_if_changed()
            return
        if self._clock_timer and self._clock_timer.isActive():
            self._clock_timer.stop()

    def _toggle_time_view(self):
        self._show_local = not self._show_local
        self._update_toggle_button_styles()
        self.rebuild_table()
        self._update_toggle_button_styles()

    def _toggle_band_view(self):
        self._show_band = not self._show_band
        self.band_toggle_btn.setText("Showing Band" if self._show_band else "Showing Frequency")
        self._update_toggle_button_styles()
        self.rebuild_table()
        self._update_toggle_button_styles()

    def _load_band_colors(self) -> None:
        raw = self.settings.get("band_colors", {}) or {}
        self._band_colors = {}
        for k, v in raw.items():
            if not k or not v:
                continue
            self._band_colors[str(k).lower().strip()] = str(v).strip()

    def _default_band_colors(self) -> Dict[str, str]:
        theme = resolve_theme(self.settings)
        is_dark = theme.get("bg") == "#0F1216"
        palette = BAND_COLORS_DARK if is_dark else BAND_COLORS_LIGHT
        return {k.lower(): v for k, v in palette.items()}

    def _band_cell_colors(self, band: str, theme: Dict[str, str]) -> Dict[str, str] | None:
        band_key = (band or "").strip().lower()
        if not band_key:
            return None
        base = self._band_colors.get(band_key)
        if not base:
            return band_cell_colors(band_key, theme)
        alpha = 0.18 if theme.get("bg") == "#0F1216" else 0.28
        bg = self._blend_hex(base, theme.get("surface", "#F0F2F4"), alpha)
        fg = self._pick_text_color(bg, theme.get("text", "#1C1F21"), "#111111")
        return {"bg": bg, "fg": fg, "border": base}

    def _render_band_legend(self) -> None:
        while self.band_legend_layout.count():
            item = self.band_legend_layout.takeAt(0)
            if item.widget():
                if item.widget() is self.band_toggle_btn:
                    item.widget().setParent(None)
                else:
                    item.widget().deleteLater()

        theme = resolve_theme(self.settings)
        if not hasattr(self, "band_toggle_btn") or self.band_toggle_btn is None:
            self.band_toggle_btn = QPushButton("Showing Band")
            self.band_toggle_btn.clicked.connect(self._toggle_band_view)
        self._update_toggle_button_styles(theme=theme)
        self.band_toggle_btn.setText("Showing Band" if self._show_band else "Showing Frequency")
        self.band_legend_layout.addWidget(self.band_toggle_btn)

        if not self._visible_bands:
            empty = QLabel("Band colors: none")
            empty.setStyleSheet(f"color: {theme['text_muted']};")
            self.band_legend_layout.addWidget(empty)
            self.band_legend_layout.addStretch()
            return

        label = QLabel("Band colors:")
        label.setStyleSheet(f"color: {theme['text_muted']};")
        self.band_legend_layout.addWidget(label)

        for band in self._visible_bands:
            btn = QPushButton(band.upper())
            btn.setCursor(Qt.PointingHandCursor)
            btn.setProperty("band_key", band)
            btn.clicked.connect(self._on_band_color_clicked)
            btn.setStyleSheet(self._band_chip_style(band, theme))
            self.band_legend_layout.addWidget(btn)

        self.band_legend_layout.addStretch()

    def _band_chip_style(self, band: str, theme: Dict[str, str]) -> str:
        band_key = (band or "").strip().lower()
        base = self._band_colors.get(band_key) or self._default_band_colors().get(band_key)
        if not base:
            base = theme.get("surface_alt", "#DDE1E6")
        fg = self._pick_text_color(base, theme.get("text", "#1C1F21"), "#111111")
        return (
            "QPushButton {"
            f" background-color: {base}; color: {fg}; border: 1px solid {theme['border']};"
            " border-radius: 10px; padding: 2px 10px; font-weight: 600;"
            " }"
            " QPushButton:hover { opacity: 0.9; }"
        )

    def _on_band_color_clicked(self) -> None:
        btn = self.sender()
        if not isinstance(btn, QPushButton):
            return
        band_key = (btn.property("band_key") or "").strip().lower()
        if not band_key:
            return
        current = self._band_colors.get(band_key) or self._default_band_colors().get(band_key, "#CCCCCC")
        selected = self._pick_band_color(band_key, current)
        if not selected:
            return
        self._band_colors[band_key] = selected
        self.settings.set("band_colors", dict(self._band_colors))
        self.rebuild_table()

    def _reset_band_colors(self) -> None:
        self._band_colors = {}
        self.settings.set("band_colors", {})
        self.rebuild_table()

    def _pick_band_color(self, band_key: str, current: str) -> str | None:
        dialog = QColorDialog(qcolor(current), self)
        dialog.setOption(QColorDialog.DontUseNativeDialog, True)
        dialog.setWindowTitle(f"Select {band_key.upper()} Color")
        reset_btn = QPushButton("Reset Default")
        reset_btn.setAutoDefault(False)
        reset_btn.setDefault(False)
        layout = dialog.layout()
        if layout is not None:
            if hasattr(layout, "rowCount"):
                row = layout.rowCount()
                layout.addWidget(reset_btn, row, 0, 1, layout.columnCount())
            else:
                layout.addWidget(reset_btn)

        def reset_and_accept():
            default = self._default_band_colors().get(band_key, current)
            dialog.setCurrentColor(qcolor(default))
            dialog.done(QDialog.Accepted)

        reset_btn.clicked.connect(reset_and_accept)
        if dialog.exec() != QDialog.Accepted:
            return None
        color = dialog.currentColor()
        if not color.isValid():
            return None
        return color.name().upper()

    def _hex_to_rgb(self, value: str) -> tuple[int, int, int]:
        value = (value or "").lstrip("#")
        if len(value) != 6:
            return 0, 0, 0
        return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)

    def _rgb_to_hex(self, r: int, g: int, b: int) -> str:
        return f"#{r:02X}{g:02X}{b:02X}"

    def _blend_hex(self, fg: str, bg: str, alpha: float) -> str:
        fr, fg_c, fb = self._hex_to_rgb(fg)
        br, bg_c, bb = self._hex_to_rgb(bg)
        r = int(fr * alpha + br * (1 - alpha))
        g = int(fg_c * alpha + bg_c * (1 - alpha))
        b = int(fb * alpha + bb * (1 - alpha))
        return self._rgb_to_hex(r, g, b)

    def _luminance(self, value: str) -> float:
        r, g, b = self._hex_to_rgb(value)
        return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255.0

    def _pick_text_color(self, bg_hex: str, light: str, dark: str) -> str:
        return dark if self._luminance(bg_hex) > 0.6 else light

    def _apply_theme(self):
        theme = resolve_theme(self.settings)
        self._update_toggle_button_styles(theme=theme)
        self._render_band_legend()

    def _update_toggle_button_styles(self, theme: Dict[str, str] | None = None) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        # Local + Band are defaults; highlight when user selects an alternate view.
        # Use explicit info styling so the active alternate state is clearly visible.
        time_role = "info" if not self._show_local else "muted"
        band_role = "info" if not self._show_band else "muted"
        self.time_toggle_btn.setStyleSheet(button_style(time_role, theme))
        self.band_toggle_btn.setStyleSheet(button_style(band_role, theme))

    def on_settings_saved(self):
        try:
            self.settings.reload()
        except Exception:
            pass
        self._apply_theme()
        self.rebuild_table()

    def apply_theme(self):
        self._apply_theme()

    # ------------- Qt events ------------- #

    def showEvent(self, event):
        """
        Rebuild the planner whenever the tab becomes visible, so changes from
        HF Schedule or Net Schedule are reflected immediately.
        """
        super().showEvent(event)
        try:
            self.rebuild_table()
        except Exception as e:
            log.error("Failed to rebuild FreqPlanner on showEvent: %s", e)
