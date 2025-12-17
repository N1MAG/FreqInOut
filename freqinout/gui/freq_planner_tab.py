from __future__ import annotations

import datetime
from typing import List, Dict, Tuple

from PySide6.QtCore import Qt, QTimer
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
        hv.setSectionResizeMode(self.COL_UTC, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_LOCAL, QHeaderView.ResizeToContents)
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
        hf = data.get("hf_schedule") or data.get("daily_schedule") or []
        net = data.get("net_schedule") or []
        if not isinstance(hf, list):
            hf = []
        if not isinstance(net, list):
            net = []
        return hf, net

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

    # ------------- core rebuild ------------- #

    def rebuild_table(self):
        """
        Recompute the table based on current hf_schedule and net_schedule in config.
        """
        self.table.clearContents()
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
                early = int(row.get("early_checkin", "0") or 0)
                smin = max(0, smin - early)
                for dname, hour in self._expand_hours_for_day(day, smin, emin, early=early):
                    net_by_day_hour.setdefault((dname, hour), []).append(row)
            except Exception:
                continue

        # Precompute hf schedule by (day, hour) honoring overnight
        hf_by_day_hour: Dict[tuple, List[Dict]] = {}
        for row in hf_sched:
            try:
                smin = self._parse_hhmm(row.get("start_utc", ""))
                emin = self._parse_hhmm(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue
                day = row.get("day_utc", "")
                for dname, hour in self._expand_hours_for_day(day, smin, emin):
                    hf_by_day_hour.setdefault((dname, hour), []).append(row)
            except Exception:
                continue

        # Current UTC day for highlighting
        now_utc = datetime.datetime.utcnow()
        current_day_name = now_utc.strftime("%A")  # "Sunday" etc.

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
            suffix = "AM" if local_hour_24 < 12 else "PM"
            hour12 = local_hour_24 % 12
            if hour12 == 0:
                hour12 = 12
            local_str = f"{hour12:02d}:00 {suffix}"
            local_item = QTableWidgetItem(local_str)
            local_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            self.table.setItem(hour, self.COL_LOCAL, local_item)

            # Day columns 2..8
            for col in range(self.COL_DAY_OFFSET, 9):
                day_name = DAY_NAMES[col - self.COL_DAY_OFFSET]
                nets_here = net_by_day_hour.get((day_name, hour), [])
                hf_rows = hf_by_day_hour.get((day_name, hour), [])
                bands = []
                for r in hf_rows:
                    b = (r.get("band") or "").strip()
                    if b:
                        bands.append(b)
                seen = set()
                bands_uniq = []
                for b in bands:
                    if b not in seen:
                        seen.add(b)
                        bands_uniq.append(b)
                band_label = " / ".join(bands_uniq)

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

                cell_text = ""
                if band_label and nets_uniq:
                    cell_text = f"{band_label}|{' / '.join(nets_uniq)}"
                elif band_label:
                    cell_text = band_label
                elif nets_uniq:
                    cell_text = " / ".join(nets_uniq)

                item = QTableWidgetItem(cell_text)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)

                # Highlight: current UTC day + has net
                if day_name == current_day_name and nets_uniq:
                    item.setBackground(Qt.yellow)

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
