from __future__ import annotations

import datetime
import sqlite3
import os
import platform
import subprocess
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import psutil
from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QComboBox,
    QHeaderView,
    QMessageBox,
    QCheckBox,
    QApplication,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.utils.timezones import get_timezone


DAY_OPTIONS = [
    "ALL",
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]
DAY_CANON = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

BAND_OPTIONS = [
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

MODE_OPTIONS = ["Digi", "SSB"]
VFO_OPTIONS = ["A", "B"]


# Radio program metadata (must match SettingsTab keys)
PROGRAMS = {
    "FLRig": {
        "path_key": "path_flrig",
        "autostart_key": "autostart_flrig",
        "default_cmd": "flrig",
    },
    "FLDigi": {
        "path_key": "path_fldigi",
        "autostart_key": "autostart_fldigi",
        "default_cmd": "fldigi",
    },
    "FLMsg": {
        "path_key": "path_flmsg",
        "autostart_key": "autostart_flmsg",
        "default_cmd": "flmsg",
    },
    "FLAmp": {
        "path_key": "path_flamp",
        "autostart_key": "autostart_flamp",
        "default_cmd": "flamp",
    },
    "JS8Call": {
        "path_key": "path_js8call",
        "autostart_key": "autostart_js8call",
        "default_cmd": "js8call",
    },
}


class DailyScheduleTab(QWidget):
    """
    HF Frequency Schedule tab.

    This tab is intentionally very similar to the Net Schedule tab, with
    the following differences:

      - The 'Net Name' column is renamed 'Group Name'.
      - The 'Day' column allows 'ALL' in addition to each day of week.
        ('ALL' means the entry is used every day.)
      - No limit to the number of rows.

    Data is stored in settings/DB; offsets/comments are no longer used.
    """

    # Column indices
    COL_SELECT = 0
    COL_DAY = 1
    COL_GROUP = 2
    COL_MODE = 3
    COL_BAND = 4
    COL_VFO = 5
    COL_FREQ = 6
    COL_START = 7
    COL_END = 8
    COL_AUTOTUNE = 9

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self.operating_groups: List[Dict] = self._load_operating_groups()
        self._operating_groups_sig = self._snapshot_operating_groups(self.operating_groups)
        self._show_local: bool = False  # view toggle
        self._raw_schedule: List[Dict] = []

        self._clock_timer: Optional[QTimer] = None
        self._opgroups_timer: Optional[QTimer] = None
        self._suppress_autostart: bool = True  # avoid auto-start during initial load

        self._build_ui()
        self._load_schedule()
        self._setup_clock_timer()
        self._setup_operating_groups_timer()
        self._suppress_autostart = False

    def _format_freq(self, val) -> str:
        try:
            return f"{float(val):.3f}"
        except Exception:
            return str(val) if val is not None else ""

    # ---------------- UI ---------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>HF Frequency Schedule</h3>"))
        header.addStretch()

        # UTC / Local labels like net_schedule_tab
        self.utc_label = QLabel()
        self.local_label = QLabel()
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        self.time_toggle_btn = QPushButton("View: UTC")
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        header.addWidget(self.time_toggle_btn)
        layout.addLayout(header)

        # Table
        self.table = QTableWidget()
        self._set_headers()

        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_SELECT, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_DAY, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_GROUP, QHeaderView.Stretch)
        hv.setSectionResizeMode(self.COL_MODE, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_BAND, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_VFO, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_FREQ, QHeaderView.Stretch)
        hv.setSectionResizeMode(self.COL_START, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_END, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_AUTOTUNE, QHeaderView.ResizeToContents)

        layout.addWidget(self.table)

        # Buttons row
        btn_row = QHBoxLayout()
        self.add_row_btn = QPushButton("Add Row")
        self.del_row_btn = QPushButton("Delete Selected")
        btn_row.addWidget(self.add_row_btn)
        btn_row.addWidget(self.del_row_btn)
        btn_row.addStretch()

        self.save_btn = QPushButton("Save HF Schedule")
        btn_row.addWidget(self.save_btn)
        layout.addLayout(btn_row)

        # Signals
        self.add_row_btn.clicked.connect(self._add_row)
        self.del_row_btn.clicked.connect(self._delete_selected_rows)
        self.save_btn.clicked.connect(self._save_schedule)

        # Initialize clock labels once
        self._update_clock_labels()

    def _load_operating_groups(self) -> List[Dict]:
        data = self.settings.all()
        og = data.get("operating_groups", [])
        if not isinstance(og, list):
            return []
        cleaned: List[Dict] = []
        for g in og:
            if not isinstance(g, dict):
                continue
            g = dict(g)
            g["frequency"] = self._format_freq(g.get("frequency", ""))
            cleaned.append(g)
        return cleaned

    def _snapshot_operating_groups(self, og_list: List[Dict]) -> str:
        """
        Deterministic snapshot string so we can detect changes without restart.
        """
        parts = []
        for g in sorted(
            og_list, key=lambda x: (str(x.get("group", "")).lower(), str(x.get("band", "")).lower())
        ):
            parts.append(
                f"{g.get('group','')}|{g.get('mode','')}|{g.get('band','')}|{self._format_freq(g.get('frequency',''))}"
            )
        return ";".join(parts)

    # ---------------- CLOCK / TIMEZONE (shared logic) ---------------- #

    def _setup_clock_timer(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._update_clock_labels)
        self._clock_timer.start(1000)

    def _setup_operating_groups_timer(self):
        """
        Periodically refresh operating groups from settings so edits in the Settings
        tab are reflected without restarting the app.
        """
        self._opgroups_timer = QTimer(self)
        self._opgroups_timer.timeout.connect(self._maybe_reload_operating_groups)
        self._opgroups_timer.start(2000)

    def _maybe_reload_operating_groups(self):
        self.settings.reload()
        latest = self._load_operating_groups()
        sig = self._snapshot_operating_groups(latest)
        if sig == self._operating_groups_sig:
            return
        self.operating_groups = latest
        self._operating_groups_sig = sig
        self._refresh_group_band_cells()

    def _refresh_group_band_cells(self):
        """
        Update group/band combos and mode/frequency cells in-place based on refreshed operating_groups.
        """
        for r in range(self.table.rowCount()):
            group_combo = self.table.cellWidget(r, self.COL_GROUP)
            band_combo = self.table.cellWidget(r, self.COL_BAND)
            # repopulate group options
            if isinstance(group_combo, QComboBox):
                current_group = group_combo.currentText()
                group_combo.blockSignals(True)
                group_combo.clear()
                group_names = sorted({g.get("group", "") for g in self.operating_groups if g.get("group")})
                group_combo.addItems(group_names)
                if current_group in group_names:
                    group_combo.setCurrentText(current_group)
                group_combo.blockSignals(False)
            # repopulate band options based on selected group
            if isinstance(band_combo, QComboBox):
                current_band = band_combo.currentText()
                self._populate_band_combo(band_combo, self._get_combo_value(r, self.COL_GROUP, ""))
                if current_band and band_combo.findText(current_band) >= 0:
                    band_combo.setCurrentText(current_band)
            # refresh mode/freq cells
            self._update_mode_freq(r)
        self._update_clock_labels()

    def _current_timezone(self) -> tuple[str, datetime.tzinfo]:
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        return tz_name, tz
    
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
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a")
        self.utc_label.setText(now_utc.strftime(f"<b>UTC ({utc_day}):</b> %y%m%d %H:%M:%S Z"))

        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        now_local = now_utc.astimezone(tz)
        # Prefer our short UI label, fall back to tzname or tz_name
        fallback = now_local.tzname() or tz_name
        ui_abbr = self._ui_tz_abbr(tz_name, fallback)

        local_day = now_local.strftime("%a")
        self.local_label.setText(
            now_local.strftime(f"<b>Local ({local_day}):</b> %y%m%d %H:%M:%S {ui_abbr}")
        )
        self.time_toggle_btn.setText("View: Local" if self._show_local else "View: UTC")

    def _set_headers(self):
        headers = [
            "",
            "Day",
            "Group Name",
            "Mode",
            "Band",
            "VFO",
            "Frequency (MHz)",
            f"Start ({'Local' if self._show_local else 'UTC'} HH:MM)",
            f"End ({'Local' if self._show_local else 'UTC'} HH:MM)",
            "Auto-Tune",
        ]
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)

    def _day_offset(self, day_name: str) -> int:
        """
        Return 0-6 offset for canonical day names (Sunday=0). Defaults to 0 on unknown.
        """
        try:
            return DAY_CANON.index(day_name)
        except Exception:
            return 0

    def _anchor_utc_sunday(self) -> datetime.datetime:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        delta = (now_utc.weekday() + 1) % 7  # Sunday=0, Monday=1, ...
        sunday = now_utc - datetime.timedelta(days=delta)
        return sunday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)

    def _anchor_local_sunday(self) -> datetime.datetime:
        """
        Sunday 00:00 in the configured local timezone.
        """
        _, tz = self._current_timezone()
        now_local = datetime.datetime.now(tz)
        delta = (now_local.weekday() + 1) % 7
        sunday = now_local - datetime.timedelta(days=delta)
        return sunday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=tz)

    def _convert_day_time(self, day: str, hhmm: str, to_local: bool) -> Tuple[str, str]:
        """
        Convert (day, HH:MM) between UTC and local time using current timezone.
        Returns (day_name, hh:mm) in target zone. 'ALL' passes through unchanged.
        """
        day = (day or "ALL").strip()
        if day.upper() == "ALL" or not hhmm:
            return day, hhmm
        try:
            hour, minute = hhmm.split(":")
            hour = int(hour)
            minute = int(minute)
        except Exception:
            return day, hhmm
        # Map day to canonical offset (Sunday=0)
        day_idx = self._day_offset(day)
        if to_local:
            anchor = self._anchor_utc_sunday()
            _, tz = self._current_timezone()
            dt_utc = anchor + datetime.timedelta(days=day_idx, hours=hour, minutes=minute)
            dt_loc = dt_utc.astimezone(tz)
            return dt_loc.strftime("%A"), dt_loc.strftime("%H:%M")
        else:
            anchor_loc = self._anchor_local_sunday()
            dt_loc = anchor_loc + datetime.timedelta(days=day_idx, hours=hour, minutes=minute)
            dt_utc = dt_loc.astimezone(datetime.timezone.utc)
            return dt_utc.strftime("%A"), dt_utc.strftime("%H:%M")

    # ---------------- Data load/save ---------------- #

    def _db_path(self) -> Path:
        """
        Location of the primary settings DB (freqinout.db).
        """
        cfg_path = getattr(self.settings, "_config_path", None)
        if cfg_path:
            try:
                return Path(cfg_path)
            except Exception:
                pass
        try:
            return Path(__file__).resolve().parents[2] / "config" / "freqinout.db"
        except Exception:
            return Path("freqinout.db")

    def _load_schedule_from_db(self) -> List[Dict]:
        """
        Load HF schedule rows from SQLite table daily_schedule_tab, if present.
        """
        db_path = self._db_path()
        if not db_path.exists():
            return []

        conn = sqlite3.connect(db_path)
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='daily_schedule_tab'"
            )
            if not cur.fetchone():
                return []

            # Try new schema; if fails, fall back to legacy (we'll map)
            try:
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
                        group_name,
                        auto_tune
                    FROM daily_schedule_tab
                    """
                )
                rows: List[Dict] = []
                for (
                    day_utc,
                    band,
                    mode,
                    vfo,
                    freq,
                    start_utc,
                    end_utc,
                    group_name,
                    auto_tune,
                ) in cur.fetchall():
                    rows.append(
                        {
                            "day_utc": (day_utc or "ALL").strip(),
                            "band": (band or "").strip(),
                            "mode": (mode or "Digi").strip(),
                            "vfo": (vfo or "A").strip().upper(),
                            "frequency": str(freq or ""),
                            "start_utc": start_utc or "",
                            "end_utc": end_utc or "",
                            "group_name": (group_name or "").strip(),
                            "auto_tune": bool(auto_tune),
                            "fldigi_offset": "",
                            "js8_offset": "",
                            "primary_js8call_group": "",
                            "comment": "",
                        }
                    )
                return rows
            except Exception:
                pass

            # Legacy schema fallback
            cur = conn.execute(
                """
                SELECT
                    day_utc,
                    band,
                    mode,
                    vfo,
                    frequency,
                    fldigi_offset,
                    js8_offset,
                    start_utc,
                    end_utc,
                    primary_js8call_group,
                    group_name,
                    comment,
                    auto_tune
                FROM daily_schedule_tab
                """
            )
            rows: List[Dict] = []
            for (
                day_utc,
                band,
                mode,
                vfo,
                freq,
                fldigi_offset,
                js8_offset,
                start_utc,
                end_utc,
                primary_group,
                group_name,
                comment,
                auto_tune,
            ) in cur.fetchall():
                rows.append(
                    {
                        "day_utc": (day_utc or "ALL").strip(),
                        "band": (band or "").strip(),
                        "mode": (mode or "Digi").strip(),
                        "vfo": (vfo or "A").strip().upper(),
                        "frequency": str(freq or ""),
                        "fldigi_offset": "",
                        "js8_offset": "",
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "primary_js8call_group": "",
                        "group_name": group_name or "",
                        "comment": "",
                        "auto_tune": bool(auto_tune),
                    }
                )
            return rows
        except Exception as e:
            log.error("HF Frequency Schedule: failed to load from DB %s: %s", db_path, e)
            return []
        finally:
            conn.close()

    def _save_schedule_to_db(self, rows: List[Dict]) -> None:
        """
        Persist HF schedule rows to SQLite table daily_schedule_tab.
        """
        db_path = self._db_path()
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("DROP TABLE IF EXISTS daily_schedule_tab")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_schedule_tab (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    day_utc TEXT NOT NULL,
                    band TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    vfo TEXT,
                    frequency TEXT NOT NULL,
                    start_utc TEXT NOT NULL,
                    end_utc TEXT NOT NULL,
                    group_name TEXT,
                    auto_tune INTEGER DEFAULT 0
                )
                """
            )
            conn.execute("DELETE FROM daily_schedule_tab")
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO daily_schedule_tab
                        (day_utc, band, mode, vfo, frequency,
                         start_utc, end_utc, group_name, auto_tune)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row.get("day_utc"),
                        row.get("band"),
                        row.get("mode"),
                        row.get("vfo"),
                        row.get("frequency"),
                        row.get("start_utc"),
                        row.get("end_utc"),
                        row.get("group_name"),
                        1 if row.get("auto_tune") else 0,
                    ),
                )
            conn.commit()
            log.info("HF schedule mirrored to DB at %s (%d entries).", db_path, len(rows))
        finally:
            conn.close()

    def _load_schedule(self):
        hf_sched = self._load_schedule_from_db()
        loaded_from_db = bool(hf_sched)

        if not hf_sched:
            data = self.settings.all()
            hf_sched = data.get("hf_schedule")

            # Backwards compatibility: if hf_schedule not present, try daily_schedule
            if hf_sched is None:
                hf_sched = data.get("daily_schedule", [])

            if not isinstance(hf_sched, list):
                hf_sched = []

        self.table.setRowCount(0)
        self._raw_schedule = hf_sched

        for entry in hf_sched:
            self._append_entry_row(self._entry_for_display(entry))

        if self.table.rowCount() == 0:
            # Add a single empty row to start with
            self._add_row()

        src = "DB" if loaded_from_db else "settings"
        log.info("HF Frequency Schedule loaded from %s: %d rows", src, self.table.rowCount())
        self._set_headers()
        self._update_clock_labels()

    def _save_schedule(self):
        # Ensure in-progress cell edits are committed
        fw = QApplication.focusWidget()
        if fw is not None and self.table.isAncestorOf(fw):
            fw.clearFocus()
            QApplication.processEvents()

        rows: List[Dict] = []
        errors: List[str] = []

        for r in range(self.table.rowCount()):
            if not self._get_checkbox_value(r, self.COL_SELECT):
                # still include rows, checkbox is only for deletion
                pass
            day = self._get_combo_value(r, self.COL_DAY, default="ALL")
            group_name = self._get_combo_value(r, self.COL_GROUP, default="")
            mode = self._get_combo_value(r, self.COL_MODE, default="Digi")
            band = self._get_combo_value(r, self.COL_BAND, default="")
            vfo = self._get_combo_value(r, self.COL_VFO, default="A")
            freq_text = self._get_text_value(r, self.COL_FREQ)
            start_val = self._get_text_value(r, self.COL_START)
            end_val = self._get_text_value(r, self.COL_END)
            auto_tune = self._get_checkbox_value(r, self.COL_AUTOTUNE)

            if not group_name or not band or not freq_text or not start_val or not end_val:
                continue

            # Enforce frequency validity for band/mode
            if not self._validate_frequency(band, mode, freq_text):
                return  # validation already warned the user
            freq_text = self._format_freq(freq_text)

            # Validate times
            if not self._validate_time(start_val) or not self._validate_time(end_val):
                errors.append(f"Row {r+1}: Start/End must be HH:MM (24h)")
                continue

            if self._show_local:
                day_utc, start_utc = self._convert_day_time(day, start_val, to_local=False)
                _, end_utc = self._convert_day_time(day, end_val, to_local=False)
            else:
                day_utc = day
                start_utc = start_val
                end_utc = end_val

            rows.append(
                {
                    "day_utc": day_utc,
                    "band": band,
                    "mode": mode,
                    "vfo": vfo,
                    "frequency": freq_text,
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "group_name": group_name,
                    "fldigi_offset": "",
                    "js8_offset": "",
                    "primary_js8call_group": "",
                    "comment": "",
                    "auto_tune": bool(auto_tune),
                }
            )

        if errors:
            QMessageBox.warning(
                self,
                "Partial Save",
                "Some rows were skipped:\n" + "\n".join(errors),
            )

        # Persist via SettingsManager
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("hf_schedule", rows)
                self.settings.set("daily_schedule", rows)  # keep legacy key in sync
                if hasattr(self.settings, "save"):
                    self.settings.save()
            else:
                data = self.settings.all()
                data["hf_schedule"] = rows
                data["daily_schedule"] = rows
                if hasattr(self.settings, "_data"):
                    self.settings._data = data  # type: ignore[attr-defined]
                if hasattr(self.settings, "save"):
                    self.settings.save()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Save Failed",
                f"Could not save HF schedule:\n{e}",
            )
            log.error("HF Frequency Schedule save failed: %s", e)
            return

        # Mirror to SQLite for scheduler_engine
        try:
            self._save_schedule_to_db(rows)
        except Exception as e:
            log.error("HF Frequency Schedule DB save failed: %s", e)
            QMessageBox.warning(
                self,
                "DB Save Error",
                f"HF schedule saved to settings, but DB save failed:\n{e}",
            )
            return

        QMessageBox.information(self, "Saved", "HF Frequency Schedule saved.")
        log.info("HF Frequency Schedule saved: %d rows", len(rows))
        self._raw_schedule = rows

    # ---------------- Row helpers ---------------- #

    def _entry_for_display(self, entry: Dict) -> Dict:
        d = dict(entry)
        if self._show_local:
            day_loc, start_loc = self._convert_day_time(d.get("day_utc", ""), d.get("start_utc", ""), to_local=True)
            _, end_loc = self._convert_day_time(d.get("day_utc", ""), d.get("end_utc", ""), to_local=True)
            d["day_utc"] = day_loc  # reuse column but reflects view
            d["start_utc"] = start_loc
            d["end_utc"] = end_loc
        return d

    def _rebuild_from_raw(self):
        self.table.setRowCount(0)
        for entry in self._raw_schedule:
            self._append_entry_row(self._entry_for_display(entry))
        if self.table.rowCount() == 0:
            self._add_row()
        self._set_headers()
        self._update_clock_labels()

    def _toggle_time_view(self):
        self._show_local = not self._show_local
        self._rebuild_from_raw()

    def _append_entry_row(self, entry: Dict):
        row = self.table.rowCount()
        self.table.insertRow(row)

        # Select checkbox
        sel_chk = QCheckBox()
        self.table.setCellWidget(row, self.COL_SELECT, sel_chk)

        # Day
        day_combo = QComboBox()
        day_combo.addItems(DAY_OPTIONS)
        day_val = (entry.get("day_utc") or "ALL").strip()
        if day_val not in DAY_OPTIONS:
            day_val = "ALL"
        day_combo.setCurrentText(day_val)
        self.table.setCellWidget(row, self.COL_DAY, day_combo)

        # Group (from operating groups)
        group_combo = QComboBox()
        group_names = sorted({g.get("group", "") for g in self.operating_groups if g.get("group")})
        group_combo.addItems(group_names)
        group_val = (entry.get("group_name") or "").strip()
        if group_val and group_val in group_names:
            group_combo.setCurrentText(group_val)
        self.table.setCellWidget(row, self.COL_GROUP, group_combo)

        # Band
        band_combo = QComboBox()
        self._populate_band_combo(band_combo, group_combo.currentText())
        band_val = (entry.get("band") or "").strip()
        if band_val and band_combo.findText(band_val) >= 0:
            band_combo.setCurrentText(band_val)
        self.table.setCellWidget(row, self.COL_BAND, band_combo)

        # Mode + Frequency (mode becomes selectable if multiple entries exist for the same group/band)
        freq_item = QTableWidgetItem()
        freq_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        self.table.setItem(row, self.COL_FREQ, freq_item)

        # VFO
        vfo_combo = QComboBox()
        vfo_combo.addItems(VFO_OPTIONS)
        vfo_val = (entry.get("vfo") or "A").strip().upper()
        if vfo_val not in VFO_OPTIONS:
            vfo_val = "A"
        vfo_combo.setCurrentText(vfo_val)
        self.table.setCellWidget(row, self.COL_VFO, vfo_combo)

        # Start / End
        st_item = QTableWidgetItem(entry.get("start_utc", ""))
        self._make_editable(st_item)
        self.table.setItem(row, self.COL_START, st_item)

        en_item = QTableWidgetItem(entry.get("end_utc", ""))
        self._make_editable(en_item)
        self.table.setItem(row, self.COL_END, en_item)

        # Auto-Tune
        chk = QCheckBox()
        chk.setChecked(bool(entry.get("auto_tune", False)))
        chk.setTristate(False)
        self.table.setCellWidget(row, self.COL_AUTOTUNE, chk)

        # wiring for group/band changes
        def on_group_changed(text: str, self=self, row=row, band_combo=band_combo):
            self._populate_band_combo(band_combo, text)
            # auto-select first band
            if band_combo.count() > 0:
                band_combo.setCurrentIndex(0)
            self._update_mode_freq(row)

        def on_band_changed(text: str, self=self, row=row):
            self._update_mode_freq(row)

        group_combo.currentTextChanged.connect(on_group_changed)
        band_combo.currentTextChanged.connect(on_band_changed)
        # Ensure initial mode/freq selection is synced to operating group data
        self._update_mode_freq(row)

    def _add_row(self):
        self._append_entry_row({})
        self.table.scrollToBottom()

    def _delete_selected_rows(self):
        selected = set()
        # Prefer checkbox selection
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, self.COL_SELECT)
            if isinstance(w, QCheckBox) and w.isChecked():
                selected.add(r)
        # Fallback to selected cells if no checkboxes are ticked
        if not selected:
            for idx in self.table.selectedIndexes():
                selected.add(idx.row())
        for r in sorted(selected, reverse=True):
            self.table.removeRow(r)

    # ---------------- Cell access helpers ---------------- #

    def _get_combo_value(self, row: int, col: int, default: str = "") -> str:
        w = self.table.cellWidget(row, col)
        if isinstance(w, QComboBox):
            return w.currentText().strip()
        item = self.table.item(row, col)
        if item is not None:
            return item.text().strip()
        return default

    def _get_checkbox_value(self, row: int, col: int) -> bool:
        w = self.table.cellWidget(row, col)
        if isinstance(w, QCheckBox):
            return w.isChecked()
        return False

    def _get_text_value(self, row: int, col: int) -> str:
        w = self.table.cellWidget(row, col)
        if isinstance(w, QComboBox):
            return w.currentText().strip()
        item = self.table.item(row, col)
        if item is None:
            return ""
        return item.text().strip()

    def _make_editable(self, item: QTableWidgetItem):
        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable)

    def _populate_band_combo(self, band_combo: QComboBox, group_name: str):
        band_combo.blockSignals(True)
        band_combo.clear()
        bands = sorted(
            {g.get("band") for g in self.operating_groups if g.get("group") == group_name and g.get("band")}
        )
        for b in bands:
            band_combo.addItem(b)
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
            combo.addItem("")
        if preferred_mode and preferred_mode in modes:
            combo.setCurrentText(preferred_mode)
        elif modes:
            combo.setCurrentIndex(0)
        combo.setEnabled(len(modes) > 1)
        combo.currentTextChanged.connect(lambda _m, r=row: self._update_mode_freq(r))
        self.table.setCellWidget(row, self.COL_MODE, combo)
        return combo

    def _update_mode_freq(self, row: int):
        group = self._get_combo_value(row, self.COL_GROUP, "")
        band = self._get_combo_value(row, self.COL_BAND, "")
        entries = self._matching_operating_groups(group, band)
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
            freq_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            self.table.setItem(row, self.COL_FREQ, freq_item)
        freq_item.setText(freq_val)
        # trigger autostart if mode changes
        if mode_val:
            self._auto_start_for_mode(mode_val)

    # ---------------- Auto-start radio software ---------------- #

    def _auto_start_for_mode(self, mode: str):
        """
        Start radio programs according to mode, if their Auto-Start flags
        are enabled in Settings and the programs are not already running.

        - JS8: JS8Call
        - Digi: FLDigi, FLMsg, FLAmp
        - Tri: all radio programs
        - SSB: no auto-start
        """
        if getattr(self, "_suppress_autostart", False):
            return

        mode = (mode or "").strip().upper()
        if not mode:
            return

        if mode == "DIGI":
            programs = ["FLDigi", "FLMsg", "FLAmp"]
        else:
            # SSB (or anything else): no auto-start
            return

        allowed_autostart = {"FLDigi", "FLMsg", "FLAmp"}
        programs = [p for p in programs if p in allowed_autostart]
        if not programs:
            return

        for prog in programs:
            self._launch_program_if_autostart_enabled(prog)

    def _launch_program_if_autostart_enabled(self, prog_name: str):
        if prog_name not in {"FLDigi", "FLMsg", "FLAmp"}:
            return
        meta = PROGRAMS.get(prog_name)
        if not meta:
            return

        autostart_key = meta["autostart_key"]
        autostart = self._is_truthy(self.settings.get(autostart_key, False))
        if not autostart:
            return

        if self._program_is_running(prog_name, meta):
            return

        path_str = self.settings.get(meta["path_key"], "") or ""
        if path_str:
            exe_path = Path(path_str)
            cmd = [str(exe_path)]
        else:
            cmd = [meta["default_cmd"]]

        try:
            if platform.system() == "Windows":
                subprocess.Popen(
                    cmd,
                    shell=False,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
                )
            else:
                subprocess.Popen(cmd)
            log.info("DailyScheduleTab: auto-started %s via %r", prog_name, cmd)
        except Exception as e:
            log.error("DailyScheduleTab: failed to auto-start %s via %r: %s", prog_name, cmd, e)

    def _program_is_running(self, prog_name: str, meta: Dict) -> bool:
        """
        Check if a program is already running using psutil.
        We match against:
          - default_cmd
          - GUI name (prog_name)
          - basename of configured path (if any)
        """
        default_cmd = meta.get("default_cmd", "").lower()
        path_str = self.settings.get(meta["path_key"], "") or ""
        tokens = {prog_name.lower()}
        if default_cmd:
            tokens.add(default_cmd)
        if path_str:
            tokens.add(Path(path_str).name.lower())

        for proc in psutil.process_iter(attrs=["name", "exe"]):
            try:
                name = (proc.info.get("name") or "").lower()
                exe = os.path.basename(proc.info.get("exe") or "").lower()
                if any(t in name for t in tokens) or any(t in exe for t in tokens):
                    return True
            except Exception:
                continue
        return False

    @staticmethod
    def _is_truthy(val) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        if isinstance(val, str):
            return val.strip().lower() in {"true", "1", "yes", "on"}
        return False

    # ---------------- Validation ---------------- #

    def _validate_time(self, text: str) -> bool:
        text = (text or "").strip()
        if not text:
            return False
        try:
            h, m = text.split(":")
            h = int(h)
            m = int(m)
            return 0 <= h <= 23 and 0 <= m <= 59
        except Exception:
            return False

    def _validate_frequency(self, band: str, mode: str, freq_text: str) -> bool:
        """
        Validate frequency based on band/mode constraints.

        Modes are now JS8, Digi, Tri, SSB.

        For band/mode limits, we treat:
          - JS8 as Digi
          - Tri as Digi
        """
        band = (band or "").strip().upper()
        mode_raw = (mode or "").strip().title()
        # Map JS8 and Tri to Digi for band-plan limits
        if mode_raw in ("Js8", "Tri"):
            eff_mode = "Digi"
        else:
            eff_mode = mode_raw

        freq_text = (freq_text or "").strip()
        if not freq_text:
            QMessageBox.warning(self, "Missing Frequency", "Frequency is required for all HF schedule rows.")
            return False

        # Parse frequency; handle "5.358.500" style if user types with extra dot
        try:
            normalized = freq_text.replace(",", ".").replace(" ", "")
            parts = normalized.split(".")
            if len(parts) > 2:
                normalized = parts[0] + "." + "".join(parts[1:])
            freq = float(normalized)
        except Exception:
            QMessageBox.warning(
                self,
                "Invalid Frequency",
                f"Frequency '{freq_text}' is not a valid number.",
            )
            return False

        # Special 60M handling
        if band == "60M":
            allowed = [5.332, 5.348, 5.3585, 5.373, 5.405]
            for a in allowed:
                if abs(freq - a) < 0.0005:
                    return True
            QMessageBox.warning(
                self,
                "Invalid 60M Frequency",
                "On 60M the only allowed channels are:\n"
                " 5.332, 5.348, 5.358.500, 5.373, 5.405 MHz",
            )
            return False

        # Range table
        ranges = {
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
            ("30M", "Any"): (10.100, 10.150),
        }

        key = (band, eff_mode)
        any_key = (band, "Any")

        if key in ranges:
            lo, hi = ranges[key]
        elif any_key in ranges:
            lo, hi = ranges[any_key]
        else:
            # If band not in our table, accept anything
            return True

        if not (lo <= freq <= hi):
            QMessageBox.warning(
                self,
                "Frequency out of range",
                f"{band} {mode_raw}: {freq:.3f} MHz is outside allowed range "
                f"{lo:.3f} - {hi:.3f} MHz.",
            )
            return False

        return True
