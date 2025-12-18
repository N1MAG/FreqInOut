from __future__ import annotations

from typing import List, Dict, Optional

import os
import platform
import sqlite3
import subprocess
import datetime
from pathlib import Path

import psutil
from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QComboBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QMessageBox,
    QLineEdit,
    QCheckBox,
)

from freqinout.core.settings_manager import SettingsManager
    # must already exist in your project
from freqinout.core.logger import log
from freqinout.utils.timezones import get_timezone


# ---- Band / Mode metadata (keep in sync with HF tab) ----

BAND_ORDER = [
    "20M",
    "40M",
    "80M",
    "--",
    "2M",
    "6M",
    "10M",
    "12M",
    "15M",
    "17M",
    "30M",
    "60M",
]

# Updated mode list
MODES = ["Digi", "SSB"]

# For band limits, JS8 and Tri behave like Digi ranges
BAND_MODE_LIMITS = {
    ("20M", "Digi"): (14.000, 14.150),
    ("20M", "SSB"): (14.150, 14.350),
    ("40M", "Digi"): (7.000, 7.125),
    ("40M", "SSB"): (7.125, 7.300),
    ("80M", "Digi"): (3.500, 3.600),
    ("80M", "SSB"): (3.600, 4.000),
    ("2M", "Digi"): (144.000, 148.000),
    ("2M", "SSB"): (144.100, 148.000),
    ("6M", "Digi"): (50.000, 54.000),
    ("6M", "SSB"): (50.100, 54.000),
    ("10M", "Digi"): (28.000, 28.300),
    ("10M", "SSB"): (28.300, 29.700),
    ("12M", "Digi"): (24.890, 24.930),
    ("12M", "SSB"): (24.930, 24.990),
    ("15M", "Digi"): (21.000, 21.200),
    ("15M", "SSB"): (21.200, 21.450),
    ("17M", "Digi"): (18.068, 18.110),
    ("17M", "SSB"): (18.110, 18.168),
    ("30M", "Digi"): (10.100, 10.150),
    ("30M", "SSB"): (10.100, 10.150),
}

SIXTY_M_CHANNELS = {
    "5.332",
    "5.348",
    "5.3585",
    "5.373",
    "5.405",
}

DAY_NAMES = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]

# Program metadata matching SettingsTab keys
PROGRAM_META: Dict[str, Dict[str, str]] = {
    "FLRig": {"path_key": "path_flrig", "autostart_key": "autostart_flrig"},
    "FLDigi": {"path_key": "path_fldigi", "autostart_key": "autostart_fldigi"},
    "FLMsg": {"path_key": "path_flmsg", "autostart_key": "autostart_flmsg"},
    "FLAmp": {"path_key": "path_flamp", "autostart_key": "autostart_flamp"},
    "JS8Call": {"path_key": "path_js8call", "autostart_key": "autostart_js8call"},
}


class NetScheduleTab(QWidget):
    """
    Net Schedules GUI.

    Columns:
      0: Select (checkbox for delete)
      1: Day (UTC)
      2: Recurrence (Weekly / Bi-Weekly / Ad Hoc)
      3: Group Name (from Operating Groups)
      4: Band
      5: Mode (JS8 / Digi / Tri / SSB)
      6: VFO (A/B)
      7: Frequency (MHz)
      8: Start UTC (HH:MM)
      9: End UTC (HH:MM)
      10: Early Check-in (minutes: 0/5/10/15)
      11: Net Name

    Data is saved to:
      - config/config.json under key "net_schedule"
      - SQLite DB freqinout_nets.db tables "net_schedule_tab" (with VFO)
        and legacy "net_schedule" (without VFO for backward compatibility)
    """

    COL_SELECT = 0
    COL_DAY = 1
    COL_RECURRENCE = 2
    COL_GROUP = 3
    COL_MODE = 4
    COL_BAND = 5
    COL_VFO = 6
    COL_FREQ = 7
    COL_START = 8
    COL_END = 9
    COL_EARLY = 10
    COL_NETNAME = 11

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._net_name_history: List[str] = []
        self.operating_groups: List[Dict[str, str]] = []
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self._clock_timer: QTimer | None = None
        self._suppress_autostart: bool = True  # avoid auto-start during initial load
        self._biweekly_choice_cache: Dict[str, int] = {}
        self._show_local: bool = False  # view toggle
        self._raw_rows: List[Dict] = []

        self._build_ui()
        self._load()
        self._setup_clock_timer()
        self._suppress_autostart = False

    # --------- UI --------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # header with clocks
        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Net Schedules</h3>"))
        header.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        self.time_toggle_btn = QPushButton("Showing: UTC")
        self.time_toggle_btn.setStyleSheet("background-color: #28a745; color: white; font-weight: 600;")
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        header.addWidget(self.time_toggle_btn)
        layout.addLayout(header)

        # table
        self.table = QTableWidget()
        self.table.setColumnCount(12)
        self._set_headers()
        self.table.setSortingEnabled(False)
        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_SELECT, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_DAY, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_RECURRENCE, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_GROUP, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_MODE, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_BAND, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_VFO, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_FREQ, QHeaderView.Stretch)
        hv.setSectionResizeMode(self.COL_START, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_END, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_EARLY, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_NETNAME, QHeaderView.Stretch)
        layout.addWidget(self.table)

        # buttons
        btn_row = QHBoxLayout()
        self.add_btn = QPushButton("Add Row")
        self.del_btn = QPushButton("Delete Selected")
        btn_row.addWidget(self.add_btn)
        btn_row.addWidget(self.del_btn)
        btn_row.addStretch()
        self.save_btn = QPushButton("Save Net Schedule")
        btn_row.addWidget(self.save_btn)
        layout.addLayout(btn_row)

        # signals
        self.add_btn.clicked.connect(self._add_row)
        self.del_btn.clicked.connect(self._delete_rows)
        self.save_btn.clicked.connect(self._save)

        self._update_clock_labels()
        self._setup_clock_timer()

    # --------- helpers: time / primary groups --------- #
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

    def _set_headers(self):
        self.table.setHorizontalHeaderLabels(
            [
                "",
                f"Day ({'Local' if self._show_local else 'UTC'})",
                "Recurrence",
                "Group Name",
                "Mode",
                "Band",
                "VFO",
                "Freq (MHz)",
                f"Start ({'Local' if self._show_local else 'UTC'} HH:MM)",
                f"End ({'Local' if self._show_local else 'UTC'} HH:MM)",
                "Early (min)",
                "Net Name",
            ]
        )
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")

    # --------- time conversion helpers --------- #
    def _day_offset(self, day_name: str) -> int:
        try:
            return DAY_NAMES.index(day_name)
        except Exception:
            return 0

    def _anchor_utc_sunday(self) -> datetime.datetime:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        delta = (now_utc.weekday() + 1) % 7  # Sunday=0
        sunday = now_utc - datetime.timedelta(days=delta)
        return sunday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)

    def _anchor_local_sunday(self) -> datetime.datetime:
        tz = get_timezone(self.settings.get("timezone", "UTC") or "UTC")
        now_local = datetime.datetime.now(tz)
        delta = (now_local.weekday() + 1) % 7
        sunday = now_local - datetime.timedelta(days=delta)
        return sunday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=tz)

    def _convert_day_time(self, day: str, hhmm: str, to_local: bool) -> tuple[str, str]:
        """
        Convert (day, HH:MM) between UTC and local using configured timezone.
        Returns day name and hh:mm in target zone.
        """
        day = (day or "").strip()
        if not day or day not in DAY_NAMES or not hhmm:
            return day, hhmm
        try:
            h, m = hhmm.split(":")
            h_i = int(h)
            m_i = int(m)
        except Exception:
            return day, hhmm
        idx = self._day_offset(day)
        if to_local:
            anchor = self._anchor_utc_sunday()
            tz = get_timezone(self.settings.get("timezone", "UTC") or "UTC")
            dt_utc = anchor + datetime.timedelta(days=idx, hours=h_i, minutes=m_i)
            dt_loc = dt_utc.astimezone(tz)
            return dt_loc.strftime("%A"), dt_loc.strftime("%H:%M")
        anchor_loc = self._anchor_local_sunday()
        dt_loc = anchor_loc + datetime.timedelta(days=idx, hours=h_i, minutes=m_i)
        dt_utc = dt_loc.astimezone(datetime.timezone.utc)
        return dt_utc.strftime("%A"), dt_utc.strftime("%H:%M")

    def _to_view_row(self, row: Dict) -> Dict:
        """
        Convert a UTC row to current view (local if toggled), preserving other fields.
        """
        if not self._show_local:
            return dict(row)
        day, start_local = self._convert_day_time(row.get("day_utc", ""), row.get("start_utc", ""), to_local=True)
        _, end_local = self._convert_day_time(row.get("day_utc", ""), row.get("end_utc", ""), to_local=True)
        out = dict(row)
        out["day_utc"] = day
        out["start_utc"] = start_local
        out["end_utc"] = end_local
        return out

    def _toggle_time_view(self):
        """
        Flip between UTC and Local view, converting current table contents back to UTC first.
        """
        try:
            # Normalize current table to UTC before flipping
            rows_utc = self._collect_rows()
        except Exception:
            rows_utc = self._raw_rows or []
        self._raw_rows = rows_utc
        self._show_local = not self._show_local
        self._set_headers()
        self.table.setRowCount(0)
        for row in self._raw_rows:
            self._add_row(self._to_view_row(row))
        self._update_clock_labels()


    # --------- row widgets --------- #

    def _add_row(self, row_data: Dict | None = None):
        r = self.table.rowCount()
        self.table.insertRow(r)

        row_data = row_data or {}

        # Select checkbox
        sel_chk = QCheckBox()
        self.table.setCellWidget(r, self.COL_SELECT, sel_chk)

        # Day combo
        day_combo = QComboBox()
        day_combo.addItems(DAY_NAMES)
        day_val = row_data.get("day_utc", "")
        if day_val in DAY_NAMES:
            day_combo.setCurrentIndex(DAY_NAMES.index(day_val))
        self.table.setCellWidget(r, self.COL_DAY, day_combo)

        # Recurrence combo
        recur_combo = QComboBox()
        recur_combo.addItems(["Weekly", "Bi-Weekly", "Ad Hoc"])
        recur_val = row_data.get("recurrence", "Weekly")
        if recur_val not in ["Weekly", "Bi-Weekly", "Ad Hoc"]:
            recur_val = "Weekly"
        recur_combo.setCurrentText(recur_val)
        self.table.setCellWidget(r, self.COL_RECURRENCE, recur_combo)

        # Group combo
        group_combo = QComboBox()
        group_names = sorted({g.get("group", "") for g in self.operating_groups if g.get("group")})
        group_combo.addItems(group_names)
        group_val = (row_data.get("group_name") or "").strip()
        if group_val and group_val in group_names:
            group_combo.setCurrentText(group_val)
        self.table.setCellWidget(r, self.COL_GROUP, group_combo)

        # Mode combo (cascades from group+band)
        mode_combo = self._set_mode_widget(r, group_combo.currentText(), "", row_data.get("mode", ""))

        # Band combo (cascades from group; fall back to BAND_ORDER)
        band_combo = QComboBox()
        self._populate_band_combo(band_combo, group_combo.currentText())
        band_val = row_data.get("band", "")
        if band_val and band_combo.findText(band_val) >= 0:
            band_combo.setCurrentText(band_val)
        elif band_combo.count() == 0:
            band_combo.addItems(BAND_ORDER)
            idx = band_combo.findText(band_val)
            if idx >= 0:
                band_combo.setCurrentIndex(idx)
        self.table.setCellWidget(r, self.COL_BAND, band_combo)

        # VFO combo (A/B)
        vfo_combo = QComboBox()
        vfo_combo.addItems(["A", "B"])
        vfo_val = (row_data.get("vfo") or "A").strip().upper()
        if vfo_val not in ("A", "B"):
            vfo_val = "A"
        vfo_combo.setCurrentText(vfo_val)
        self.table.setCellWidget(r, self.COL_VFO, vfo_combo)

        # Early check-in
        early_combo = QComboBox()
        early_combo.addItems(["0", "5", "10", "15"])
        early_val = str(row_data.get("early_checkin", "0"))
        idx = early_combo.findText(early_val)
        if idx >= 0:
            early_combo.setCurrentIndex(idx)
        self.table.setCellWidget(r, self.COL_EARLY, early_combo)

        # Net name edit
        net_edit = QLineEdit()
        net_val = row_data.get("net_name", "")
        net_edit.setText(net_val)
        self.table.setCellWidget(r, self.COL_NETNAME, net_edit)

        # Freq / times as QTableWidgetItem
        def set_item(col: int, value: str | None):
            item = QTableWidgetItem(str(value) if value is not None else "")
            self.table.setItem(r, col, item)

        set_item(self.COL_FREQ, row_data.get("frequency", ""))
        set_item(self.COL_START, row_data.get("start_utc", ""))
        set_item(self.COL_END, row_data.get("end_utc", ""))

        # wiring for cascades
        def on_group_changed(text: str, self=self, row=r, band_combo=band_combo):
            self._populate_band_combo(band_combo, text)
            if band_combo.count() > 0:
                band_combo.setCurrentIndex(0)
            self._update_mode_freq(row)

        def on_band_changed(text: str, self=self, row=r):
            self._update_mode_freq(row)

        group_combo.currentTextChanged.connect(on_group_changed)
        band_combo.currentTextChanged.connect(on_band_changed)
        # Ensure initial mode/freq selection is synced to operating group data
        self._update_mode_freq(r)

    def _get_combo_value(self, row: int, col: int, default: str = "") -> str:
        w = self.table.cellWidget(row, col)
        if isinstance(w, QComboBox):
            return w.currentText().strip()
        item = self.table.item(row, col)
        if item is not None:
            return item.text().strip()
        return default

    def _delete_rows(self):
        selected = set()
        # Prefer checkboxes
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, self.COL_SELECT)
            if isinstance(w, QCheckBox) and w.isChecked():
                selected.add(r)
        # Fallback to selected cells if no checkboxes
        if not selected:
            selected = {i.row() for i in self.table.selectedIndexes()}
        for r in sorted(selected, reverse=True):
            self.table.removeRow(r)

    # --------- Operating group helpers (cascading selections) --------- #

    def _load_operating_groups(self) -> None:
        data = self.settings.all()
        og = data.get("operating_groups", [])
        if isinstance(og, list):
            self.operating_groups = [g for g in og if isinstance(g, dict)]
        else:
            self.operating_groups = []

    def _populate_band_combo(self, band_combo: QComboBox, group_name: str):
        band_combo.blockSignals(True)
        band_combo.clear()
        bands = sorted(
            {g.get("band") for g in self.operating_groups if g.get("group") == group_name and g.get("band")}
        )
        if bands:
            band_combo.addItems(bands)
        else:
            band_combo.addItems(BAND_ORDER)
        band_combo.blockSignals(False)

    def _matching_operating_groups(self, group: str, band: str) -> List[Dict]:
        return [
            g
            for g in self.operating_groups
            if g.get("group") == group and g.get("band") == band
        ]

    def _set_mode_widget(
        self, row: int, group: str, band: str, preferred_mode: str = "", entries: Optional[List[Dict]] = None
    ) -> QComboBox:
        if entries is None:
            entries = self._matching_operating_groups(group, band)
        modes = sorted({(e.get("mode") or "").strip() for e in entries if e.get("mode")})
        combo = QComboBox()
        if modes:
            combo.addItems(modes)
        else:
            combo.addItems(MODES)
        if preferred_mode and preferred_mode in modes:
            combo.setCurrentText(preferred_mode)
        elif modes:
            combo.setCurrentIndex(0)
        combo.setEnabled(len(modes) > 1 or not modes)
        combo.currentTextChanged.connect(lambda m, r=row: self._on_mode_changed(r, m))
        self.table.setCellWidget(row, self.COL_MODE, combo)
        return combo

    def _update_mode_freq(self, row: int):
        group = self._get_combo_value(row, self.COL_GROUP, "")
        band = self._get_combo_value(row, self.COL_BAND, "")
        entries = self._matching_operating_groups(group, band)
        if not entries:
            return
        preferred_mode = self._get_combo_value(row, self.COL_MODE, "")
        mode_combo = self._set_mode_widget(row, group, band, preferred_mode, entries)
        mode_val = mode_combo.currentText().strip() if isinstance(mode_combo, QComboBox) else preferred_mode
        entry = None
        for g in entries:
            if (g.get("mode") or "").strip() == mode_val:
                entry = g
                break
        if entry is None and entries:
            entry = entries[0]
            if isinstance(mode_combo, QComboBox):
                mode_combo.blockSignals(True)
                mode_combo.setCurrentText(entry.get("mode", ""))
                mode_combo.blockSignals(False)
            mode_val = entry.get("mode", "")
        freq_val = self._format_freq(entry.get("frequency", "")) if entry else ""
        freq_item = self.table.item(row, self.COL_FREQ)
        if freq_item is None:
            freq_item = QTableWidgetItem()
            self.table.setItem(row, self.COL_FREQ, freq_item)
        freq_item.setText(freq_val)
        # trigger autostart if mode changes
        if mode_val:
            self._on_mode_changed(row, mode_val)

    def _format_freq(self, freq: str | float) -> str:
        try:
            return f"{float(freq):.4f}".rstrip("0").rstrip(".")
        except Exception:
            try:
                return str(freq).strip()
            except Exception:
                return ""

    # --------- auto-start behavior --------- #

    def _on_mode_changed(self, row: int, mode: str):
        """
        Called whenever a Mode cell changes. Mode is one of JS8, Digi, Tri, SSB.
        Triggers appropriate auto-start behavior based on Settings autostart flags.
        """
        mode = (mode or "").strip()
        if not mode:
            return
        self._autostart_for_mode(mode)

    def _autostart_for_mode(self, mode: str):
        """
        Mode → programs mapping:

          JS8  → JS8Call
          Digi → FLDigi, FLMsg, FLAmp
          Tri  → FLRig, FLDigi, FLMsg, FLAmp, JS8Call
          SSB  → no auto-launch (RF-only)
        """
        if getattr(self, "_suppress_autostart", False):
            return

        mode = mode.strip()

        if mode == "Digi":
            programs = ["FLDigi", "FLMsg", "FLAmp"]
        else:
            # For SSB (or anything else), do nothing
            return

        allowed_autostart = {"FLDigi", "FLMsg", "FLAmp"}
        programs = [p for p in programs if p in allowed_autostart]
        if not programs:
            return

        for prog in programs:
            if not self._program_autostart_enabled(prog):
                continue
            if self._program_is_running(prog):
                continue
            self._launch_program(prog)

    def _program_autostart_enabled(self, program_name: str) -> bool:
        if program_name not in {"FLDigi", "FLMsg", "FLAmp"}:
            return False
        meta = PROGRAM_META.get(program_name)
        if not meta:
            return False
        key = meta["autostart_key"]
        try:
            val = self.settings.get(key, False)
        except Exception:
            data = self.settings.all()
            val = data.get(key, False)
        return self._is_truthy(val)

    def _get_saved_program_path(self, program_name: str) -> Optional[Path]:
        meta = PROGRAM_META.get(program_name)
        if not meta:
            return None
        key = meta["path_key"]
        try:
            path_str = self.settings.get(key)
        except Exception:
            data = self.settings.all()
            path_str = data.get(key)
        if not path_str:
            return None
        p = Path(path_str)
        return p if p.exists() else None

    def _program_is_running(self, program_name: str) -> bool:
        now_ts = datetime.datetime.now().timestamp()
        if now_ts - self._proc_snapshot_ts > 2.0:
            snap = []
            for proc in psutil.process_iter(attrs=["name", "exe"]):
                try:
                    name = (proc.info.get("name") or "").lower()
                    exe = os.path.basename(proc.info.get("exe") or "").lower()
                    if name:
                        snap.append(name)
                    if exe and exe != name:
                        snap.append(exe)
                except Exception:
                    continue
            self._proc_snapshot = snap
            self._proc_snapshot_ts = now_ts

        exe_path = self._get_saved_program_path(program_name)
        targets = [program_name.lower()]
        if exe_path:
            targets.append(exe_path.name.lower())

        return any(any(t in entry for t in targets) for entry in self._proc_snapshot)

    @staticmethod
    def _is_truthy(val) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        if isinstance(val, str):
            return val.strip().lower() in {"true", "1", "yes", "on"}
        return False

    def _launch_program(self, program_name: str) -> bool:
        exe_path = self._get_saved_program_path(program_name)

        # Try explicit path first
        if exe_path and exe_path.exists():
            try:
                if platform.system() == "Windows":
                    subprocess.Popen(
                        [str(exe_path)],
                        shell=False,
                        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
                    )
                else:
                    subprocess.Popen([str(exe_path)])
                log.info("NetSchedule: launched %s from saved path %s", program_name, exe_path)
                return True
            except Exception as e:
                log.error("NetSchedule: failed launching %s from saved path %s: %s", program_name, exe_path, e)

        # Fallback: rely on PATH
        for cand in [program_name.lower(), program_name]:
            try:
                subprocess.Popen(
                    [cand],
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
                    if platform.system() == "Windows"
                    else 0,
                )
                log.info("NetSchedule: launched %s via PATH as '%s'", program_name, cand)
                return True
            except Exception:
                continue

        log.warning("NetSchedule: unable to launch %s; no valid path or PATH command", program_name)
        return False

    # --------- parsing / validation --------- #

    def _parse_hhmm(self, txt: str):
        txt = (txt or "").strip()
        if not txt:
            return None
        try:
            parts = txt.split(":")
            if len(parts) != 2:
                return None
            h = int(parts[0])
            m = int(parts[1])
            if not (0 <= h <= 23 and 0 <= m <= 59):
                return None
            return h * 60 + m
        except Exception:
            return None

    def _collect_rows(self) -> List[Dict]:
        rows: List[Dict] = []
        net_names_seen = set()

        for r in range(self.table.rowCount()):
            def text(col: int) -> str:
                item = self.table.item(r, col)
                return item.text().strip() if item else ""

            day_combo: QComboBox = self.table.cellWidget(r, self.COL_DAY)  # type: ignore
            recur_combo: QComboBox = self.table.cellWidget(r, self.COL_RECURRENCE)  # type: ignore
            group_combo: QComboBox = self.table.cellWidget(r, self.COL_GROUP)  # type: ignore
            band_combo: QComboBox = self.table.cellWidget(r, self.COL_BAND)  # type: ignore
            mode_combo: QComboBox = self.table.cellWidget(r, self.COL_MODE)  # type: ignore
            vfo_combo: QComboBox = self.table.cellWidget(r, self.COL_VFO)  # type: ignore
            early_combo: QComboBox = self.table.cellWidget(r, self.COL_EARLY)  # type: ignore
            net_edit: QLineEdit = self.table.cellWidget(r, self.COL_NETNAME)  # type: ignore

            day = day_combo.currentText().strip() if day_combo else ""
            group_name = group_combo.currentText().strip() if group_combo else ""
            band = band_combo.currentText().strip() if band_combo else ""
            mode = mode_combo.currentText().strip() if mode_combo else ""
            vfo = vfo_combo.currentText().strip().upper() if vfo_combo else "A"
            early = early_combo.currentText().strip() if early_combo else "0"
            net_name = net_edit.text().strip() if net_edit else ""
            recurrence = recur_combo.currentText().strip() if recur_combo else "Weekly"

            freq = text(self.COL_FREQ)
            start_txt = text(self.COL_START)
            end_txt = text(self.COL_END)

            # Skip completely empty rows
            if not (day or band or freq or start_txt or end_txt or net_name):
                continue

            if not day:
                raise ValueError(f"Row {r+1}: Day is required.")
            if band == "--":
                raise ValueError(f"Row {r+1}: '--' is not a valid band.")
            if band and band not in BAND_ORDER:
                raise ValueError(f"Row {r+1}: Unknown band '{band}'.")
            if mode and mode not in MODES:
                raise ValueError(f"Row {r+1}: Unknown mode '{mode}'.")

            if vfo not in ("A", "B"):
                raise ValueError(f"Row {r+1}: VFO must be 'A' or 'B'.")

            # Frequency validation
            if not freq:
                raise ValueError(f"Row {r+1}: Frequency is required.")
            try:
                freq_norm = freq.replace(" ", "")
                if freq_norm.count(".") > 1:
                    parts = freq_norm.split(".")
                    freq_norm = parts[0] + "." + "".join(parts[1:])
                freq_mhz = float(freq_norm)
            except ValueError:
                raise ValueError(f"Row {r+1}: Invalid frequency '{freq}'.")

            # Normalize band like "40" -> "40M"
            if band and band not in BAND_ORDER:
                if not band.endswith("M"):
                    band = f"{band}M"

            if band == "60M":
                key = f"{freq_mhz:.4f}".rstrip("0").rstrip(".")
                allowed = {c.rstrip("0").rstrip(".") for c in SIXTY_M_CHANNELS}
                if key not in allowed:
                    raise ValueError(
                        "Row %d: 60M must be one of 5.332, 5.348, 5.3585, 5.373, 5.405 MHz."
                        % (r + 1)
                    )
            else:
                # Treat JS8 and Tri as Digi for band limits
                mode_for_limits = mode
                if mode_for_limits in ("JS8", "Tri"):
                    mode_for_limits = "Digi"

                limits = BAND_MODE_LIMITS.get((band, mode_for_limits))
                if limits:
                    lo, hi = limits
                    if not (lo <= freq_mhz <= hi):
                        raise ValueError(
                            "Row %d: %s %s frequency must be between %.3f and %.3f MHz."
                            % (r + 1, band, mode, lo, hi)
                        )

            smin = self._parse_hhmm(start_txt)
            emin = self._parse_hhmm(end_txt)
            if smin is None or emin is None:
                raise ValueError(f"Row {r+1}: Invalid time (use HH:MM).")

            try:
                early_int = int(early)
            except ValueError:
                raise ValueError(f"Row {r+1}: Early check-in must be 0, 5, 10, or 15.")

            if early_int not in (0, 5, 10, 15):
                raise ValueError(f"Row {r+1}: Early check-in must be 0, 5, 10, or 15.")

            recurrence = recurrence if recurrence in ("Weekly", "Bi-Weekly", "Ad Hoc") else "Weekly"
            biweekly_offset = 0
            if recurrence == "Bi-Weekly":
                # Cache prompt per unique net/day/band so we only ask once per save session
                key = f"{day}|{band}|{net_name}|{freq_mhz:.4f}"
                if key in self._biweekly_choice_cache:
                    biweekly_offset = self._biweekly_choice_cache[key]
                else:
                    resp = QMessageBox.question(
                        self,
                        "Bi-Weekly Start",
                        f"For {net_name or 'this net'} on {day}, start next {day}?"
                        "\nChoose No for the week after.",
                        QMessageBox.Yes | QMessageBox.No,
                        QMessageBox.Yes,
                    )
                    biweekly_offset = 0 if resp == QMessageBox.Yes else 1
                    self._biweekly_choice_cache[key] = biweekly_offset

            # If viewing local, convert back to UTC before storing
            if self._show_local:
                orig_day = day
                day, start_txt = self._convert_day_time(orig_day, start_txt, to_local=False)
                _, end_txt = self._convert_day_time(orig_day, end_txt, to_local=False)

            row = {
                "day_utc": day,
                "recurrence": recurrence,
                "biweekly_offset_weeks": biweekly_offset,
                "group_name": group_name,
                "band": band,
                "mode": mode,
                "vfo": vfo,
                "frequency": self._format_freq(freq_mhz),
                "start_utc": start_txt,
                "end_utc": end_txt,
                "early_checkin": str(early_int),
                "net_name": net_name,
            }
            rows.append(row)

            if net_name:
                net_names_seen.add(net_name)

        self._net_name_history = sorted(net_names_seen)
        return rows

    # --------- load/save --------- #

    def _db_path(self) -> Path:
        cfg_path = getattr(self.settings, "_config_path", None)
        if cfg_path:
            try:
                cfg = Path(cfg_path)
                return cfg.parent / "freqinout_nets.db"
            except Exception:
                pass
        return Path(__file__).resolve().parents[2] / "config" / "freqinout_nets.db"

    def _load_from_db(self) -> List[Dict]:
        db_path = self._db_path()
        if not db_path.exists():
            return []

        conn = sqlite3.connect(db_path)
        try:
            has_new = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='net_schedule_tab'"
            ).fetchone()
            has_legacy = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='net_schedule'"
            ).fetchone()
            if not has_new and not has_legacy:
                return []

            rows: List[Dict] = []
            if has_new:
                try:
                    cur = conn.execute(
                        """
                        SELECT
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            band,
                            mode,
                            vfo,
                            frequency,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name,
                            group_name
                        FROM net_schedule_tab
                        """
                    )
                except Exception:
                    cur = conn.execute(
                        """
                        SELECT
                            day_utc,
                            band,
                            mode,
                            vfo,
                            frequency,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name
                        FROM net_schedule_tab
                        """
                    )
                    for (
                        day_utc,
                        band,
                        mode,
                        vfo,
                        freq,
                        start_utc,
                        end_utc,
                        early,
                        group,
                        comment,
                        net_name,
                    ) in cur.fetchall():
                        rows.append(
                            {
                                "day_utc": day_utc or "",
                                "recurrence": "Weekly",
                                "biweekly_offset_weeks": 0,
                                "band": band or "",
                                "mode": mode or "",
                                "vfo": (vfo or "A").strip().upper(),
                                "frequency": str(freq or ""),
                                "start_utc": start_utc or "",
                                "end_utc": end_utc or "",
                                "early_checkin": str(early if early is not None else 0),
                                "primary_js8call_group": group or "",
                                "comment": comment or "",
                                "net_name": net_name or "",
                                "group_name": "",
                            }
                        )
                    return rows

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
                    early,
                    group,
                    comment,
                    net_name,
                    group_name,
                ) in cur.fetchall():
                    rows.append(
                        {
                            "day_utc": day_utc or "",
                            "recurrence": recurrence or "Weekly",
                            "biweekly_offset_weeks": int(biweekly_offset_weeks or 0),
                            "band": band or "",
                            "mode": mode or "",
                            "vfo": (vfo or "A").strip().upper(),
                            "frequency": str(freq or ""),
                            "start_utc": start_utc or "",
                            "end_utc": end_utc or "",
                            "early_checkin": str(early if early is not None else 0),
                            "primary_js8call_group": group or "",
                            "comment": comment or "",
                            "net_name": net_name or "",
                            "group_name": group_name or "",
                        }
                    )
                return rows

            if has_legacy:
                try:
                    cur = conn.execute(
                        """
                        SELECT
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            band,
                            mode,
                            frequency,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name
                        FROM net_schedule
                        """
                    )
                except Exception:
                    cur = conn.execute(
                        """
                        SELECT
                            day_utc,
                            band,
                            mode,
                            frequency,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name
                        FROM net_schedule
                        """
                    )
                    for (
                        day_utc,
                        band,
                        mode,
                        freq,
                        start_utc,
                        end_utc,
                        early,
                        group,
                        comment,
                        net_name,
                    ) in cur.fetchall():
                        rows.append(
                            {
                                "day_utc": day_utc or "",
                                "recurrence": "Weekly",
                                "biweekly_offset_weeks": 0,
                                "band": band or "",
                                "mode": mode or "",
                                "vfo": "A",
                                "frequency": str(freq or ""),
                                "start_utc": start_utc or "",
                                "end_utc": end_utc or "",
                                "early_checkin": str(early if early is not None else 0),
                                "primary_js8call_group": group or "",
                                "comment": comment or "",
                                "net_name": net_name or "",
                            }
                        )
                    return rows

                for (
                    day_utc,
                    recurrence,
                    biweekly_offset_weeks,
                    band,
                    mode,
                    freq,
                    start_utc,
                    end_utc,
                    early,
                    group,
                    comment,
                    net_name,
                ) in cur.fetchall():
                    rows.append(
                        {
                            "day_utc": day_utc or "",
                            "recurrence": recurrence or "Weekly",
                            "biweekly_offset_weeks": int(biweekly_offset_weeks or 0),
                            "band": band or "",
                            "mode": mode or "",
                            "vfo": "A",
                            "frequency": str(freq or ""),
                            "start_utc": start_utc or "",
                            "end_utc": end_utc or "",
                            "early_checkin": str(early if early is not None else 0),
                            "primary_js8call_group": group or "",
                            "comment": comment or "",
                            "net_name": net_name or "",
                            "group_name": "",
                        }
                    )
            return rows
        except Exception as e:
            log.error("NetScheduleTab: failed to load schedule from DB %s: %s", db_path, e)
            return []
        finally:
            conn.close()
        # Should not reach here
        return []

    def _load(self):
        self.table.setRowCount(0)
        self._load_operating_groups()
        data = self._load_from_db()
        loaded_from_db = bool(data)
        if not data:
            data = self.settings.get("net_schedule", [])
            if not isinstance(data, list):
                data = []
        self._raw_rows = data
        for row in self._raw_rows:
            self._add_row(self._to_view_row(row))
        self._net_name_history = sorted(
            {r.get("net_name", "") for r in data if isinstance(r, dict) and r.get("net_name")}
        )
        self._update_clock_labels()
        src = "DB" if loaded_from_db else "settings"
        log.info("Net schedule loaded from %s: %d rows", src, len(data))

    def _save(self):
        try:
            rows = self._collect_rows()
        except ValueError as e:
            QMessageBox.warning(self, "Invalid Net Schedule", str(e))
            return
        self._raw_rows = rows

        # Save to JSON config
        self.settings.set("net_schedule", rows)
        self.settings.save()
        log.info("Net schedule saved to config: %d entries", len(rows))

        # Also mirror to SQLite DB (new table with VFO plus legacy table)
        try:
            self._save_to_db(rows)
        except Exception as e:
            log.error("Failed to save net schedule to DB: %s", e)
            QMessageBox.warning(
                self,
                "DB Save Error",
                f"Net schedule saved to config.json, but DB save failed:\n{e}",
            )
            return

        QMessageBox.information(self, "Saved", "Net Schedule saved.")

    # --------- SQLite mirror --------- #

    def _ensure_db_columns(self, conn: sqlite3.Connection, table: str, columns: Dict[str, str]):
        """
        Ensure each column in `columns` exists on `table`, adding with ALTER TABLE if missing.
        """
        existing = set()
        for _, name, *_ in conn.execute(f"PRAGMA table_info({table})"):
            existing.add(name if isinstance(name, str) else str(name))
        for col, ddl in columns.items():
            if col not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")

    def _save_to_db(self, rows: List[Dict]):
        """
        Persist net schedule rows into SQLite tables in config/freqinout_nets.db.
        Writes both the richer net_schedule_tab (with VFO) and the legacy
        net_schedule table for backwards compatibility.
        """
        db_path = self._db_path()
        conn = sqlite3.connect(db_path)
        try:
            self._create_tables(conn)
            self._ensure_columns_with_recreate(conn)
            conn.execute("DELETE FROM net_schedule_tab")
            conn.execute("DELETE FROM net_schedule")
            self._insert_rows(conn, rows)
            conn.commit()
            log.info("Net schedule mirrored to DB at %s (%d entries).", db_path, len(rows))
        finally:
            conn.close()

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        """
        Create the schedule tables with the expected schema.
        """
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS net_schedule_tab (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                day_utc TEXT NOT NULL,
                recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                vfo TEXT,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                early_checkin INTEGER NOT NULL,
                primary_js8call_group TEXT,
                comment TEXT,
                net_name TEXT,
                group_name TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS net_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                day_utc TEXT NOT NULL,
                recurrence TEXT DEFAULT 'Weekly',
                biweekly_offset_weeks INTEGER DEFAULT 0,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                frequency TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                early_checkin INTEGER NOT NULL,
                primary_js8call_group TEXT,
                comment TEXT,
                net_name TEXT,
                group_name TEXT
            )
            """
        )

    def _recreate_tables(self, conn: sqlite3.Connection) -> None:
        """
        Drop and recreate schedule tables when schema drift is detected.
        """
        conn.execute("DROP TABLE IF EXISTS net_schedule_tab")
        conn.execute("DROP TABLE IF EXISTS net_schedule")
        self._create_tables(conn)

    def _ensure_columns_with_recreate(self, conn: sqlite3.Connection) -> None:
        """
        Ensure expected columns exist; recreate tables once if ALTER fails.
        """
        try:
            self._ensure_db_columns(
                conn,
                "net_schedule_tab",
                {
                    "recurrence": "TEXT DEFAULT 'Weekly'",
                    "biweekly_offset_weeks": "INTEGER DEFAULT 0",
                    "vfo": "TEXT",
                    "group_name": "TEXT",
                },
            )
            self._ensure_db_columns(
                conn,
                "net_schedule",
                {
                    "recurrence": "TEXT DEFAULT 'Weekly'",
                    "biweekly_offset_weeks": "INTEGER DEFAULT 0",
                    "group_name": "TEXT",
                },
            )
        except sqlite3.OperationalError as e:
            log.warning("Net schedule column update failed (%s); recreating tables.", e)
            self._recreate_tables(conn)
            self._ensure_db_columns(
                conn,
                "net_schedule_tab",
                {
                    "recurrence": "TEXT DEFAULT 'Weekly'",
                    "biweekly_offset_weeks": "INTEGER DEFAULT 0",
                    "vfo": "TEXT",
                    "group_name": "TEXT",
                },
            )
            self._ensure_db_columns(
                conn,
                "net_schedule",
                {
                    "recurrence": "TEXT DEFAULT 'Weekly'",
                    "biweekly_offset_weeks": "INTEGER DEFAULT 0",
                    "group_name": "TEXT",
                },
            )

    def _insert_rows(self, conn: sqlite3.Connection, rows: List[Dict]) -> None:
        """
        Insert schedule rows, recreating tables once if schema drift is detected.
        """
        try:
            self._insert_rows_inner(conn, rows)
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "no column" not in msg and "has no column" not in msg:
                raise
            log.warning("Net schedule table schema drift detected (%s); recreating tables.", e)
            self._recreate_tables(conn)
            self._insert_rows_inner(conn, rows)

    def _insert_rows_inner(self, conn: sqlite3.Connection, rows: List[Dict]) -> None:
        for row in rows:
            conn.execute(
                """
                INSERT INTO net_schedule_tab
                  (day_utc, recurrence, biweekly_offset_weeks, band, mode, vfo, frequency, start_utc, end_utc,
                   early_checkin, primary_js8call_group, comment, net_name, group_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("day_utc"),
                    row.get("recurrence", "Weekly"),
                    int(row.get("biweekly_offset_weeks", 0) or 0),
                    row.get("band"),
                    row.get("mode"),
                    row.get("vfo"),
                    row.get("frequency"),
                    row.get("start_utc"),
                    row.get("end_utc"),
                    int(row.get("early_checkin", "0") or 0),
                    row.get("primary_js8call_group"),
                    row.get("comment"),
                    row.get("net_name"),
                    row.get("group_name"),
                ),
            )
            conn.execute(
                """
                INSERT INTO net_schedule
                  (day_utc, recurrence, biweekly_offset_weeks, band, mode, frequency, start_utc, end_utc,
                   early_checkin, primary_js8call_group, comment, net_name, group_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("day_utc"),
                    row.get("recurrence", "Weekly"),
                    int(row.get("biweekly_offset_weeks", 0) or 0),
                    row.get("band"),
                    row.get("mode"),
                    row.get("frequency"),
                    row.get("start_utc"),
                    row.get("end_utc"),
                    int(row.get("early_checkin", "0") or 0),
                    row.get("primary_js8call_group"),
                    row.get("comment"),
                    row.get("net_name"),
                    row.get("group_name"),
                ),
            )
