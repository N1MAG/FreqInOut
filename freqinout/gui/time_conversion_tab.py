from __future__ import annotations
import datetime
from typing import List, Dict, Any, Optional, Set
import pytz
from PySide6.QtCore import QTimer, Qt
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QCheckBox,
    QPushButton, QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox
)
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.utils import get_utc_time, get_local_time

REGION_TO_TZ = {
    "Eastern": "America/New_York",
    "Central": "America/Chicago",
    "Mountain": "America/Denver",
    "Pacific": "America/Los_Angeles",
}

class TimeConversionTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._build_ui()
        self._load_settings()
        self._start_clocks()
        self._rebuild_table()

    # ---------------- UI ----------------
    def _build_ui(self):
        layout = QVBoxLayout(self)
        header = QHBoxLayout()
        header.addWidget(QLabel("<h2>Time Conversion Chart</h2>"))
        header.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        layout.addLayout(header)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("View schedule in:"))
        self.region_combo = QComboBox()
        self.region_combo.addItems(["Eastern", "Central", "Mountain", "Pacific"])
        controls.addWidget(self.region_combo)
        self.dst_checkbox = QCheckBox("Daylight Saving Time")
        controls.addWidget(self.dst_checkbox)
        self.save_btn = QPushButton("Save Settings")
        controls.addWidget(self.save_btn)
        controls.addStretch()
        layout.addLayout(controls)

        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["UTC", "Local Time", "Band(s)", "Net(s)"])
        self.table.setRowCount(24)
        header_view = self.table.horizontalHeader()
        header_view.setSectionResizeMode(0, QHeaderView.Stretch)
        header_view.setSectionResizeMode(1, QHeaderView.Stretch)
        header_view.setSectionResizeMode(2, QHeaderView.Stretch)
        header_view.setSectionResizeMode(3, QHeaderView.Stretch)
        layout.addWidget(self.table)

        self.save_btn.clicked.connect(self._save_settings)
        self.region_combo.currentIndexChanged.connect(self._rebuild_table)
        self.dst_checkbox.stateChanged.connect(self._rebuild_table)

    # ---------------- CLOCK ----------------
    def _start_clocks(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._update_time_labels)
        self._clock_timer.start(1000)
        self._update_time_labels()

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._check_hour_rollover)
        self._refresh_timer.start(60_000)
        self._last_utc_hour = datetime.datetime.utcnow().hour

    def _update_time_labels(self):
        self.utc_label.setText(f"<b>UTC:</b> {get_utc_time()}")
        self.local_label.setText(f"<b>Local:</b> {get_local_time()}")

    def _check_hour_rollover(self):
        hour = datetime.datetime.utcnow().hour
        if hour != self._last_utc_hour:
            self._last_utc_hour = hour
            self._rebuild_table()

    # ---------------- SETTINGS ----------------
    def _load_settings(self):
        region = self.settings.get("timeconv_region", "Mountain")
        idx = self.region_combo.findText(region)
        if idx >= 0:
            self.region_combo.setCurrentIndex(idx)
        dst_enabled = bool(self.settings.get("timeconv_dst", True))
        self.dst_checkbox.setChecked(dst_enabled)
        self._rebuild_table()

    def _save_settings(self):
        self.settings.set("timeconv_region", self.region_combo.currentText())
        self.settings.set("timeconv_dst", self.dst_checkbox.isChecked())
        QMessageBox.information(self, "Saved", "Time conversion settings saved.")
        self._rebuild_table()

    # ---------------- HELPERS ----------------
    def _get_local_timezone(self):
        return pytz.timezone(REGION_TO_TZ.get(self.region_combo.currentText(), "America/Denver"))

    def _parse_hhmm_to_minutes(self, s: str) -> Optional[int]:
        try:
            h, m = [int(x) for x in s.strip().split(":")]
            return h * 60 + m
        except Exception:
            return None

    def _weekday_to_day_name(self, weekday: int) -> str:
        return ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][(weekday + 1) % 7]

    # ---------------- CORE ----------------
    def _rebuild_table(self):
        try:
            local_tz = self._get_local_timezone()
        except Exception:
            local_tz = pytz.UTC

        year = datetime.datetime.utcnow().year
        base_date = datetime.date(year, 7, 15 if self.dst_checkbox.isChecked() else 1)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        current_hour = now_utc.hour

        daily = self.settings.get("daily_schedule", [])
        nets = self.settings.get("net_schedule", [])
        day_name = self._weekday_to_day_name(now_utc.weekday())

        daily_ranges = []
        for row in daily if isinstance(daily, list) else []:
            r = {k.lower().strip(): v for k, v in row.items() if isinstance(k, str)}
            start_m = self._parse_hhmm_to_minutes(r.get("start_utc") or r.get("start"))
            end_m = self._parse_hhmm_to_minutes(r.get("end_utc") or r.get("end"))
            band = (r.get("band") or "").strip()
            if start_m is not None and end_m is not None and band:
                daily_ranges.append({"start": start_m, "end": end_m, "band": band})

        nets_today = []
        for row in nets if isinstance(nets, list) else []:
            r = {k.lower().strip(): v for k, v in row.items() if isinstance(k, str)}
            if (r.get("day_utc") or r.get("day")) != day_name:
                continue
            start_m = self._parse_hhmm_to_minutes(r.get("start_utc") or r.get("start"))
            end_m = self._parse_hhmm_to_minutes(r.get("end_utc") or r.get("end"))
            if start_m is None or end_m is None:
                continue
            early = int(r.get("early_checkin", 0) or 0)
            net_name = (
                r.get("net_name")
                or r.get("net")
                or r.get("netname")
                or r.get("name")
                or r.get("comment")
                or ""
            ).strip()
            if not net_name:
                continue
            nets_today.append({"start": max(0, start_m - early), "end": end_m, "net_name": net_name})

        self.table.setRowCount(24)
        for hour in range(24):
            start_m, end_m = hour * 60, hour * 60 + 60
            utc_str = f"{hour:02d}:00"
            self.table.setItem(hour, 0, QTableWidgetItem(utc_str))

            dt_utc = datetime.datetime(base_date.year, base_date.month, base_date.day, hour, tzinfo=datetime.timezone.utc)
            dt_local = dt_utc.astimezone(local_tz)
            h12 = dt_local.hour % 12 or 12
            ampm = "AM" if dt_local.hour < 12 else "PM"
            local_str = f"{h12}:00 {ampm}"
            self.table.setItem(hour, 1, QTableWidgetItem(local_str))

            bands = [r["band"] for r in daily_ranges if r["start"] < end_m and r["end"] > start_m]
            nets_here = [n["net_name"] for n in nets_today if n["start"] < end_m and n["end"] > start_m]
            self.table.setItem(hour, 2, QTableWidgetItem(" / ".join(sorted(set(bands))[:2])))
            self.table.setItem(hour, 3, QTableWidgetItem(" / ".join(sorted(set(nets_here))[:2])))

            if hour == current_hour:
                for col in range(4):
                    item = self.table.item(hour, col)
                    if item:
                        item.setBackground(Qt.yellow)
