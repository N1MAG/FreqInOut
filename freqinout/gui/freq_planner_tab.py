from __future__ import annotations

import datetime
import sqlite3
from typing import List, Dict, Tuple

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
)

from pathlib import Path

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.utils.timezones import get_timezone

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
        self._clock_timer: QTimer | None = None
        self._build_ui()
        self.rebuild_table()

    # ------------- UI ------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>FreqPlanner</h3>"))
        header.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        layout.addLayout(header)

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

        layout.addWidget(self.table)

        self._setup_clock_timer()

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
        """
        tz_name, tz = self._current_timezone()
        now = datetime.datetime.now(tz)
        abbr = now.tzname() or tz_name
        return tz_name, abbr

    def _load_schedules(self) -> Tuple[List[Dict], List[Dict]]:
        data = self.settings.all()

        # Try DB-backed schedules first
        hf_db = self._load_hf_from_db()
        net_db = self._load_net_from_db()

        hf = hf_db if hf_db is not None else data.get("hf_schedule") or data.get("daily_schedule") or []
        net = net_db if net_db is not None else data.get("net_schedule") or []
        if not isinstance(hf, list):
            hf = []
        if not isinstance(net, list):
            net = []
        return hf, net

    def _load_hf_from_db(self) -> Optional[List[Dict]]:
        """
        Load HF/daily schedule from config/freqinout.db if available.
        """
        try:
            db_path = Path(__file__).resolve().parents[2] / "config" / "freqinout.db"
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
            db_path = Path(__file__).resolve().parents[2] / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            rows = []
            try:
                cur.execute(
                    """
                    SELECT day_utc, recurrence, biweekly_offset_weeks, band, mode, vfo, frequency,
                           start_utc, end_utc, early_checkin, primary_js8call_group, comment, net_name
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
                    # Pad legacy rows to align with expected tuple positions (insert vfo=None)
                    rows = [
                        (
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
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
            ) in rows:
                out.append(
                    {
                        "day_utc": day_utc or "ALL",
                        "recurrence": recurrence or "Weekly",
                        "biweekly_offset_weeks": biweekly_offset_weeks or 0,
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
                    }
                )
            return out
        except Exception:
            return None

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

    # ------------- core rebuild ------------- #

    def rebuild_table(self):
        """
        Recompute the table based on current hf_schedule and net_schedule in config.
        """
        self.table.clearContents()
        try:
            # Pick up latest settings written by other tabs before reading schedules.
            self.settings.reload()
        except Exception:
            pass
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

        hf_sched, net_sched = self._load_schedules()

        # Precompute net schedule by (day_utc, hour)
        net_by_day_hour: Dict[tuple, List[Dict]] = {}
        for row in net_sched:
            try:
                day = row.get("day_utc", "")
                smin = self._parse_hhmm(row.get("start_utc", ""))
                emin = self._parse_hhmm(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue
                if emin % 60 == 0:
                    emin = min(emin + 60, 24 * 60)
                # Display times should use actual start/end (no early check-in adjustment)
                for dname, hour in self._expand_hours_for_day(day, smin, emin, early=0):
                    net_by_day_hour.setdefault((dname, hour), []).append(row)
            except Exception:
                continue

        # Precompute HF schedule coverage by (day, hour) with minute-level slices
        hf_cover: Dict[tuple, List[Tuple[int, int, str]]] = {}

        def add_slice(day_name: str, hour: int, start_minute: int, end_minute: int, band: str) -> None:
            if start_minute >= end_minute:
                return
            hf_cover.setdefault((day_name, hour), []).append((start_minute, end_minute, band))

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
                        add_slice(dname, hour % 24, overlap_start - hour_start_min, overlap_end - hour_start_min, band)
            except Exception:
                continue

        # Current UTC day for highlighting
        now_utc = datetime.datetime.utcnow()
        current_day_name = now_utc.strftime("%A")  # "Sunday" etc.
        now_plus_24 = now_utc + datetime.timedelta(hours=24)

        # Timezone for local conversion
        tz_name_cfg, tz = self._current_timezone()

        # Fill rows
        today_utc = now_utc.replace(minute=0, second=0, microsecond=0)

        for hour in range(24):
            # Column 0: UTC hour "HH:00"
            utc_item = QTableWidgetItem(f"{hour:02d}:00")
            utc_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
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
            self.table.setItem(hour, self.COL_LOCAL, local_item)

            # Day columns 2..8
            for col in range(self.COL_DAY_OFFSET, 9):
                day_name = DAY_NAMES[col - self.COL_DAY_OFFSET]
                nets_here = net_by_day_hour.get((day_name, hour), [])
                hf_slices = hf_cover.get((day_name, hour), [])
                band_label = ""
                if hf_slices:
                    # Order by start minute and compress consecutive identical bands
                    hf_slices = sorted(hf_slices, key=lambda x: x[0])
                    bands_in_order: List[str] = []
                    last_band = None
                    for start_m, end_m, b in hf_slices:
                        if end_m <= start_m:
                            continue
                        if b != last_band:
                            bands_in_order.append(b)
                            last_band = b
                    if len(bands_in_order) == 1:
                        band_label = bands_in_order[0]
                    else:
                        band_label = "/".join(bands_in_order)

                net_names = []
                for n in nets_here:
                    nn = (n.get("net_name") or "").strip()
                    if nn:
                        net_names.append(nn)
                # unique
                seen_n = set()
                nets_uniq = []
                for n in net_names:
                    if n not in seen_n:
                        seen_n.add(n)
                        nets_uniq.append(n)

                has_net = bool(nets_here)
                net_label = " / ".join(nets_uniq) if nets_uniq else ("Net" if has_net else "")

                cell_text = ""
                # If any net exists, show only the net name (no band)
                if net_label:
                    cell_text = net_label
                else:
                    cell_text = band_label

                item = QTableWidgetItem(cell_text)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)

                # Highlight: net window overlaps now or starts within next 24h
                highlight = False
                for net_row in nets_here:
                    window = self._net_window_for_day(net_row, day_name, now_utc)
                    if not window:
                        continue
                    start_dt, end_dt = window
                    # Highlight if currently in window or starts within 24h
                    if start_dt <= now_utc <= end_dt:
                        highlight = True
                        break
                    if now_utc <= start_dt <= now_plus_24:
                        highlight = True
                        break
                if highlight:
                    item.setBackground(QColor("#fff59d"))  # soft yellow highlight

                self.table.setItem(hour, col, item)

        # Update clock labels
        self._update_clock_labels()
        log.info("FreqPlanner table rebuilt.")

    def _ui_tz_abbr(self, tz_name: str, fallback: str) -> str:
        mapping = {
            "UTC": "UTC",
            "America/New_York": "ET",
            "America/Chicago": "CT",
            "America/Denver": "MT",
            "America/Los_Angeles": "PT",
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

    def _setup_clock_timer(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._update_clock_labels)
        self._clock_timer.start(1000)
        self._update_clock_labels()

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
