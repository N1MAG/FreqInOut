from __future__ import annotations

import datetime
import os
import platform
import subprocess
import sqlite3
from pathlib import Path
from typing import Dict, Optional, List

import psutil
from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QComboBox,
    QCheckBox,
    QFileDialog,
    QMessageBox,
    QGroupBox,
    QFormLayout,
    QApplication,
    QDialog,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QSizePolicy,
    QAbstractScrollArea,
)

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.utils.timezones import get_timezone
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer


TIMEZONE_CHOICES = [
    "UTC",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
]


class SettingsTab(QWidget):
    """
    Global settings for FreqInOut.

    - Call sign / Name / State
    - Control mode
    - JS8Call TCP port
    - Primary JS8Call groups
    - JS8Call DIRECTED.TXT path
    - Radio software paths & autostart

    Timezone is *not* user selectable here; it is auto-detected from the
    system clock and stored under the 'timezone' key in SettingsManager.
    All entries are saved to config when:
      - The Save button is clicked, OR
      - The application exits (QApplication.aboutToQuit).

    Persistence is done via SettingsManager.set(...) when available,
    or by updating SettingsManager._data as a fallback. We *do not*
    call any .write() or .save() here to avoid AttributeError.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()

        self.PROGRAMS: Dict[str, Dict[str, str]] = {
            "FLRig": {"setting_key": "path_flrig", "autostart_key": "autostart_flrig"},
            "FLDigi": {"setting_key": "path_fldigi", "autostart_key": "autostart_fldigi"},
            "FLMsg": {"setting_key": "path_flmsg", "autostart_key": "autostart_flmsg"},
            "FLAmp": {"setting_key": "path_flamp", "autostart_key": "autostart_flamp"},
            "JS8Call": {"setting_key": "path_js8call", "autostart_key": "autostart_js8call"},
        }

        self.radio_checkboxes: Dict[str, QCheckBox] = {}
        self.status_labels: Dict[str, QLabel] = {}
        self.path_edits: Dict[str, QLineEdit] = {}
        self.autostart_checks: Dict[str, QCheckBox] = {}
        self.js8_groups_edits: List[QLineEdit] = []
        self.js8_auto_query_chk: Optional[QCheckBox] = None
        self.js8_auto_query_grid_chk: Optional[QCheckBox] = None
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self.operating_groups: List[Dict[str, str]] = []

        self._build_ui()
        self._load_settings()

        # Auto-save on application exit (no popup)
        app = QApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(self._save_settings_quiet)

        # time updater (UTC + detected timezone)
        self.time_timer = QTimer(self)
        self.time_timer.setInterval(1000)
        self.time_timer.timeout.connect(self._update_clock_labels)
        self.time_timer.start()

        # process status timer
        self.status_timer = QTimer(self)
        self.status_timer.setInterval(2000)
        self.status_timer.timeout.connect(self._refresh_running_status)
        self.status_timer.start()

        self._update_clock_labels()
        self._refresh_running_status()

    # ---------- UI ---------- #

    def _build_ui(self):
        main_layout = QVBoxLayout(self)

        header_layout = QHBoxLayout()
        title_label = QLabel("<h2>Settings</h2>")
        header_layout.addWidget(title_label)
        header_layout.addStretch()

        self.utc_label = QLabel()
        self.local_label = QLabel()
        header_layout.addWidget(self.utc_label)
        header_layout.addWidget(self.local_label)
        main_layout.addLayout(header_layout)

        content_layout = QHBoxLayout()
        main_layout.addLayout(content_layout)

        # LEFT column
        left_col = QVBoxLayout()
        content_layout.addLayout(left_col, 3)

        # Identity group
        callsign_group = QGroupBox("Operator Information")
        callsign_form = QFormLayout()
        self.callsign_edit = QLineEdit()
        self.callsign_edit.setMaxLength(16)
        self.callsign_edit.setFixedWidth(150)
        self.name_edit = QLineEdit()
        self.name_edit.setFixedWidth(200)
        self.state_edit = QLineEdit()
        self.state_edit.setFixedWidth(80)
        self.grid6_edit = QLineEdit()
        self.grid6_edit.setMaxLength(6)
        self.grid6_edit.setFixedWidth(90)
        callsign_form.addRow("Call Sign:", self.callsign_edit)
        callsign_form.addRow("Name:", self.name_edit)
        callsign_form.addRow("State:", self.state_edit)
        callsign_form.addRow("Grid 6:", self.grid6_edit)
        callsign_group.setLayout(callsign_form)
        left_col.addWidget(callsign_group)

        # Operation settings (control)
        op_group = QGroupBox("Operation Settings")
        op_layout = QVBoxLayout()
        op_group.setLayout(op_layout)
        left_col.addWidget(op_group)

        # control mode (no timezone dropdown anymore)
        ctrl_row = QHBoxLayout()
        ctrl_row.addWidget(QLabel("Control frequency via:"))
        self.control_combo = QComboBox()
        self.control_combo.addItems(["FLRig", "JS8Call", "Manual"])
        ctrl_row.addWidget(self.control_combo)
        ctrl_row.addStretch()
        op_layout.addLayout(ctrl_row)

        flrig_port_row = QHBoxLayout()
        flrig_port_row.addWidget(QLabel("FLRig XMLRPC Port:"))
        self.flrig_port_edit = QLineEdit()
        self.flrig_port_edit.setFixedWidth(80)
        self.flrig_port_edit.setText("12345")
        flrig_port_row.addWidget(self.flrig_port_edit)
        flrig_port_row.addStretch()
        op_layout.addLayout(flrig_port_row)

        # RIGHT column
        right_col = QVBoxLayout()
        content_layout.addLayout(right_col, 4)

        # JS8Call status/settings (managed externally)
        js8_group = QGroupBox("JS8Call Settings")
        js8_v = QVBoxLayout()
        js8_group.setLayout(js8_v)

        js8_status_row = QHBoxLayout()
        js8_status_row.addWidget(QLabel("JS8Call status:"))
        js8_status_lbl = QLabel()
        js8_status_lbl.setFixedSize(14, 14)
        js8_status_lbl.setStyleSheet("background-color: #555; border-radius: 7px;")
        # Keep API indicator separate from the per-program row to avoid overwrites
        self.status_labels["JS8Call_API"] = js8_status_lbl
        js8_status_row.addWidget(js8_status_lbl)
        js8_status_row.addStretch()
        js8_v.addLayout(js8_status_row)

        js8_port_row = QHBoxLayout()
        js8_port_row.addWidget(QLabel("JS8Call TCP Port:"))
        self.js8_port_edit = QLineEdit()
        self.js8_port_edit.setFixedWidth(80)
        self.js8_port_edit.setText("2442")
        js8_port_row.addWidget(self.js8_port_edit)
        js8_port_row.addStretch()
        js8_v.addLayout(js8_port_row)

        js8_offset_row = QHBoxLayout()
        js8_offset_row.addWidget(QLabel("JS8 Offset (Hz):"))
        self.js8_offset_edit = QLineEdit()
        self.js8_offset_edit.setFixedWidth(80)
        self.js8_offset_edit.setText("0")
        js8_offset_row.addWidget(self.js8_offset_edit)
        js8_offset_row.addStretch()
        js8_v.addLayout(js8_offset_row)

        directed_row = QHBoxLayout()
        directed_row.addWidget(QLabel("JS8Call DIRECTED.TXT:"))
        self.js8_directed_edit = QLineEdit()
        directed_browse = QPushButton("Browse…")
        directed_browse.clicked.connect(self._choose_js8_directed_path)
        directed_row.addWidget(self.js8_directed_edit, stretch=1)
        directed_row.addWidget(directed_browse)
        js8_v.addLayout(directed_row)

        js8_auto_row = QHBoxLayout()
        self.js8_auto_query_chk = QCheckBox("Auto Query Msg ID")
        self.js8_auto_query_grid_chk = QCheckBox("Auto Query Grids")
        js8_auto_row.addSpacing(30)
        js8_auto_row.addWidget(self.js8_auto_query_chk)
        js8_auto_row.addWidget(self.js8_auto_query_grid_chk)
        js8_auto_row.addStretch()
        js8_v.addLayout(js8_auto_row)

        load_links_row = QHBoxLayout()
        self.load_js8_btn = QPushButton("Load JS8 Traffic")
        self.load_js8_btn.clicked.connect(self._load_js8_logs)
        load_links_row.addWidget(self.load_js8_btn)
        load_links_row.addStretch()
        js8_v.addLayout(load_links_row)

        left_col.addWidget(js8_group)

        # Operating Groups panel (right column)
        ops_group = QGroupBox("Operating Groups")
        ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        ops_layout = QVBoxLayout()
        ops_group.setLayout(ops_layout)
        add_row = QHBoxLayout()
        add_btn = QPushButton("➕ Add Group")
        add_btn.clicked.connect(self._add_operating_group)
        edit_btn = QPushButton("✏️ Edit Selected")
        edit_btn.clicked.connect(self._edit_operating_group)
        delete_btn = QPushButton("Delete Selected")
        delete_btn.clicked.connect(self._delete_operating_groups)
        add_row.addStretch()
        add_row.addWidget(add_btn)
        add_row.addWidget(edit_btn)
        add_row.addWidget(delete_btn)
        ops_layout.addLayout(add_row)
        self.op_groups_table = QTableWidget(0, 5)
        self.op_groups_table.setHorizontalHeaderLabels(["", "Group", "Mode", "Band", "Freq (MHz)"])
        header = self.op_groups_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setStretchLastSection(True)
        self.op_groups_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.op_groups_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.op_groups_table.setEditTriggers(QTableWidget.NoEditTriggers)
        ops_layout.addWidget(self.op_groups_table)
        right_col.addWidget(ops_group)

        # Radio software
        radio_group = QGroupBox("Radio Software")
        radio_v = QVBoxLayout()
        radio_group.setLayout(radio_v)
        left_col.addWidget(radio_group)

        for prog_name, meta in self.PROGRAMS.items():
            row = QHBoxLayout()
            chk = QCheckBox(prog_name)
            self.radio_checkboxes[prog_name] = chk
            row.addWidget(chk)

            status_lbl = QLabel()
            status_lbl.setFixedSize(14, 14)
            status_lbl.setStyleSheet("background-color: #555; border-radius: 7px;")
            self.status_labels[prog_name] = status_lbl
            row.addWidget(status_lbl)

            path_edit = QLineEdit()
            path_edit.setPlaceholderText("Path to executable")
            self.path_edits[prog_name] = path_edit
            row.addWidget(path_edit)

            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(lambda _, n=prog_name: self._choose_program_path(n))
            row.addWidget(browse_btn)

            autostart_chk = QCheckBox("Auto-start")
            self.autostart_checks[prog_name] = autostart_chk
            row.addWidget(autostart_chk)

            radio_v.addLayout(row)

        # Launch Selected
        self.launch_selected_btn = QPushButton("Launch Selected")
        self.launch_selected_btn.clicked.connect(self._launch_selected_programs)
        radio_v.addWidget(self.launch_selected_btn)

        left_col.addWidget(js8_group)
        left_col.addStretch()

        right_col.addStretch()

        # bottom save
        bottom_row = QHBoxLayout()
        bottom_row.addStretch()
        self.save_btn = QPushButton("Save Settings")
        self.save_btn.clicked.connect(self._save_settings_button)
        bottom_row.addWidget(self.save_btn)
        main_layout.addLayout(bottom_row)

    # ---------- LOAD/SAVE ---------- #

    def _load_settings(self):
        data = self.settings.all()

        self.callsign_edit.setText(data.get("operator_callsign", "") or "")
        self.name_edit.setText(data.get("operator_name", "") or "")
        self.state_edit.setText(data.get("operator_state", "") or "")
        self.grid6_edit.setText(data.get("operator_grid6", "") or "")

        # Timezone: prefer stored; otherwise detect from system clock
        tz = data.get("timezone")
        if not tz:
            tz = self._detect_system_timezone()
            data["timezone"] = tz
            # Just keep this in-memory; persistence happens on explicit save or exit.
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        elif tz not in TIMEZONE_CHOICES:
            # Normalise unexpected values back into one of our known IDs
            detected = self._detect_system_timezone()
            data["timezone"] = detected
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]

        ctrl = data.get("control_via", "FLRig") or "FLRig"
        allowed_ctrl = ["FLRig", "JS8Call", "Manual"]
        if ctrl not in allowed_ctrl:
            ctrl = "FLRig"
        self.control_combo.setCurrentText(ctrl)

        port_txt = str(data.get("js8_port", "2442") or "2442")
        self.js8_port_edit.setText(port_txt)
        offset_txt = str(data.get("js8_offset_hz", "0") or "0")
        self.js8_offset_edit.setText(offset_txt)
        flrig_port_txt = str(data.get("flrig_port", "12345") or "12345")
        self.flrig_port_edit.setText(flrig_port_txt)

        groups = data.get("primary_js8_groups", [])
        if not isinstance(groups, list):
            groups = []
        for i, le in enumerate(self.js8_groups_edits):
            le.setText(groups[i] if i < len(groups) else "")

        # Load operating groups
        try:
            og = data.get("operating_groups", [])
            if isinstance(og, list):
                self.operating_groups = [g for g in og if isinstance(g, dict)]
        except Exception:
            self.operating_groups = []
        self._refresh_operating_groups_table()

        self.js8_directed_edit.setText(data.get("js8_directed_path", "") or "")

        for prog_name, meta in self.PROGRAMS.items():
            path_key = meta["setting_key"]
            auto_key = meta["autostart_key"]
            enabled_key = f"{prog_name.lower()}_enabled"

            if path_key:
                self.path_edits[prog_name].setText(data.get(path_key, "") or "")
            if auto_key and prog_name in self.autostart_checks:
                self.autostart_checks[prog_name].setChecked(bool(data.get(auto_key, False)))
            if prog_name in self.radio_checkboxes:
                self.radio_checkboxes[prog_name].setChecked(bool(data.get(enabled_key, False)))

        if self.js8_auto_query_chk:
            self.js8_auto_query_chk.setChecked(bool(data.get("js8_auto_query_msg_id", False)))
        if self.js8_auto_query_grid_chk:
            self.js8_auto_query_grid_chk.setChecked(bool(data.get("js8_auto_query_grids", False)))

        log.info("SettingsTab: settings loaded.")

    def _save_settings_button(self):
        """Explicit save via the button (shows confirmation)."""
        self._save_settings(show_message=True)

    def _save_settings_quiet(self):
        """Auto-save on application exit (no dialog)."""
        self._save_settings(show_message=False)

    def _save_settings(self, show_message: bool = True):
        data = self.settings.all()

        data["operator_callsign"] = self.callsign_edit.text().strip()
        data["operator_name"] = self.name_edit.text().strip()
        data["operator_state"] = self.state_edit.text().strip()
        data["operator_grid6"] = self.grid6_edit.text().strip().upper()
        data["operator_grid6"] = self.grid6_edit.text().strip().upper()

        # Timezone is not user-editable; keep existing value (or detect if missing)
        tz = data.get("timezone")
        if not tz:
            tz = self._detect_system_timezone()
            data["timezone"] = tz

        data["control_via"] = self.control_combo.currentText().strip()

        try:
            port_val = int(self.js8_port_edit.text().strip() or "2442")
        except ValueError:
            port_val = 2442
            self.js8_port_edit.setText("2442")
        data["js8_port"] = port_val
        try:
            offset_val = int(self.js8_offset_edit.text().strip() or "0")
        except ValueError:
            offset_val = 0
            self.js8_offset_edit.setText("0")
        data["js8_offset_hz"] = offset_val

        groups = [le.text().strip() for le in self.js8_groups_edits if le.text().strip()]
        data["primary_js8_groups"] = groups

        data["js8_directed_path"] = self.js8_directed_edit.text().strip()

        # Radio software paths / autostart / enabled flags from UI
        for prog_name, meta in self.PROGRAMS.items():
            path_key = meta["setting_key"]
            auto_key = meta["autostart_key"]
            enabled_key = f"{prog_name.lower()}_enabled"

            if path_key:
                data[path_key] = self.path_edits[prog_name].text().strip()
            if auto_key and prog_name in self.autostart_checks:
                data[auto_key] = bool(self.autostart_checks[prog_name].isChecked())
            if prog_name in self.radio_checkboxes:
                data[enabled_key] = bool(self.radio_checkboxes[prog_name].isChecked())

        data["js8_auto_query_msg_id"] = (
            bool(self.js8_auto_query_chk.isChecked()) if self.js8_auto_query_chk else False
        )
        data["js8_auto_query_grids"] = (
            bool(self.js8_auto_query_grid_chk.isChecked()) if self.js8_auto_query_grid_chk else False
        )
        data["operating_groups"] = self._table_to_operating_groups()

        # Persist with a single write when possible.
        if hasattr(self.settings, "set_many"):
            batch = {
                "operator_callsign": data["operator_callsign"],
                "operator_name": data["operator_name"],
                "operator_state": data["operator_state"],
                "operator_grid6": data["operator_grid6"],
                "timezone": data["timezone"],
                "control_via": data["control_via"],
                "js8_port": data["js8_port"],
                "js8_offset_hz": data.get("js8_offset_hz", 0),
                "primary_js8_groups": data["primary_js8_groups"],
                "js8_directed_path": data["js8_directed_path"],
                "js8_auto_query_msg_id": data["js8_auto_query_msg_id"],
                "js8_auto_query_grids": data["js8_auto_query_grids"],
                "operating_groups": data.get("operating_groups", []),
            }
            for prog_name, meta in self.PROGRAMS.items():
                path_key = meta["setting_key"]
                auto_key = meta["autostart_key"]
                enabled_key = f"{prog_name.lower()}_enabled"
                if path_key:
                    batch[path_key] = data.get(path_key, "")
                if auto_key:
                    batch[auto_key] = data.get(auto_key, False)
                if prog_name in self.radio_checkboxes:
                    batch[enabled_key] = data.get(enabled_key, False)
            self.settings.set_many(batch, save=True)  # type: ignore[attr-defined]
        elif hasattr(self.settings, "set"):
            self.settings.set("operator_callsign", data["operator_callsign"])
            self.settings.set("operator_name", data["operator_name"])
            self.settings.set("operator_state", data["operator_state"])
            self.settings.set("operator_grid6", data["operator_grid6"])
            self.settings.set("timezone", data["timezone"])
            self.settings.set("control_via", data["control_via"])
            self.settings.set("js8_port", data["js8_port"])
            self.settings.set("js8_offset_hz", data.get("js8_offset_hz", 0))
            self.settings.set("primary_js8_groups", data["primary_js8_groups"])
            self.settings.set("js8_directed_path", data["js8_directed_path"])
            self.settings.set("js8_auto_query_grids", data.get("js8_auto_query_grids", False))
            for prog_name, meta in self.PROGRAMS.items():
                path_key = meta["setting_key"]
                auto_key = meta["autostart_key"]
                enabled_key = f"{prog_name.lower()}_enabled"
                if path_key:
                    self.settings.set(path_key, data.get(path_key, ""))
                if auto_key:
                    self.settings.set(auto_key, data.get(auto_key, False))
                if prog_name in self.radio_checkboxes:
                    self.settings.set(enabled_key, data.get(enabled_key, False))
            self.settings.set("operating_groups", data.get("operating_groups", []))
        elif hasattr(self.settings, "_data"):
            # Fallback: update the internal dict only
            self.settings._data = data  # type: ignore[attr-defined]

        log.info("SettingsTab: settings saved.")
        if show_message:
            QMessageBox.information(self, "Settings", "Settings saved.")

        # Persist operator grid into operator_checkins for map usage
        self._persist_operator_grid_to_db(
            data.get("operator_callsign", ""),
            data.get("operator_grid6", ""),
            data.get("operator_name", ""),
            data.get("operator_state", ""),
        )
        self._refresh_operator_history_views()

    # ---------- TIME / TIMEZONE ---------- #

    def _detect_system_timezone(self) -> str:
        """
        Detect a reasonable default timezone from the system clock.
        Returns a value in TIMEZONE_CHOICES when possible; otherwise 'UTC'.
        """
        try:
            local_dt = datetime.datetime.now().astimezone()
            tzinfo = local_dt.tzinfo
            if tzinfo is None:
                return "UTC"

            # zoneinfo-based tz will have .key
            tz_key = getattr(tzinfo, "key", None)
            if tz_key and tz_key in TIMEZONE_CHOICES:
                return tz_key

            # Windows-style sometimes uses 'Central Standard Time', etc.
            # We just approximate based on offset if we can.
            offset = tzinfo.utcoffset(local_dt) or datetime.timedelta(0)
            hours = int(offset.total_seconds() // 3600)

            # Simple offset-based map (approximate)
            if hours == -5:
                return "America/New_York"
            if hours == -6:
                return "America/Chicago"
            if hours == -7:
                return "America/Denver"
            if hours == -8:
                return "America/Los_Angeles"

            # Fallback
            return "UTC"
        except Exception:
            return "UTC"

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

    def _persist_operator_grid_to_db(self, callsign: str, grid6: str, name: str, state: str) -> None:
        """
        Optionally upsert the operator's own grid into operator_checkins to ensure
        stations map has a primary location for link rendering.
        """
        cs = (callsign or "").strip().upper()
        grid = (grid6 or "").strip().upper()
        if not cs or len(grid) < 4:
            return
        try:
            root = Path(__file__).resolve().parents[2]  # repo root
            db_path = root / "config" / "freqinout_nets.db"
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS operator_checkins (
                    callsign TEXT PRIMARY KEY,
                    name TEXT,
                    state TEXT,
                    grid TEXT,
                    group1 TEXT,
                    group2 TEXT,
                    group3 TEXT,
                    groups_json TEXT,
                    first_seen_utc TEXT,
                    last_seen_utc TEXT,
                    checkin_count INTEGER,
                    trusted INTEGER
                )
                """
            )
            cur.execute(
                """
                INSERT INTO operator_checkins (callsign, name, state, grid, first_seen_utc, last_seen_utc, checkin_count, trusted)
                VALUES (?, ?, ?, ?, COALESCE(first_seen_utc, strftime('%Y-%m-%d', 'now')), strftime('%Y-%m-%d', 'now'), COALESCE((SELECT checkin_count FROM operator_checkins WHERE callsign=?), 0), COALESCE((SELECT trusted FROM operator_checkins WHERE callsign=?), 0))
                ON CONFLICT(callsign) DO UPDATE SET
                    name=excluded.name,
                    state=excluded.state,
                    grid=excluded.grid,
                    last_seen_utc=excluded.last_seen_utc,
                    checkin_count=excluded.checkin_count,
                    trusted=COALESCE(operator_checkins.trusted, excluded.trusted)
                """,
                (cs, name.strip(), state.strip().upper(), grid, cs, cs),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("SettingsTab: failed to persist operator grid to DB: %s", e)

    def _refresh_operator_history_views(self) -> None:
        """
        Ask the main window to reload operator history consumers (map, history, net controls).
        """
        try:
            # Prefer top-level window; parent() may be a layout wrapper
            win = self.window()
            if win and hasattr(win, "refresh_operator_history_views"):
                win.refresh_operator_history_views()
        except Exception:
            pass

    # ---------- RADIO PROGRAMS ---------- #

    def _choose_program_path(self, program_name: str):
        fn, _ = QFileDialog.getOpenFileName(self, f"Select {program_name} Executable")
        if fn:
            self.path_edits[program_name].setText(fn)

    def _get_saved_program_path(self, program_name: str) -> Optional[Path]:
        if program_name == "JS8Call":
            return None
        meta = self.PROGRAMS.get(program_name)
        if not meta:
            return None
        path_str = self.settings.get(meta["setting_key"])
        if path_str:
            return Path(path_str)
        ui_val = self.path_edits.get(program_name)
        if ui_val:
            txt = ui_val.text().strip()
            if txt:
                return Path(txt)
        return None

    def _launch_program(self, program_name: str) -> bool:
        if program_name == "JS8Call":
            log.info("Launch request ignored for JS8Call (external management only).")
            return False
        exe_path = self._get_saved_program_path(program_name)

        creationflags = 0
        if platform.system() == "Windows":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

        if exe_path and exe_path.exists():
            try:
                subprocess.Popen([str(exe_path)], shell=False if platform.system() == "Windows" else False, creationflags=creationflags)
                log.info("Launched %s from saved path %s", program_name, exe_path)
                return True
            except Exception as e:
                log.error("Failed launching %s from saved path %s: %s", program_name, exe_path, e)

        for cand in [program_name.lower(), program_name]:
            try:
                subprocess.Popen([cand], creationflags=creationflags)
                log.info("Launched %s from system PATH as '%s'", program_name, cand)
                return True
            except Exception:
                continue

        QMessageBox.warning(
            self,
            "Launch Failed",
            f"Unable to launch {program_name}. Please set the executable path.",
        )
        return False

    def _launch_selected_programs(self):
        launched_any = False
        for name, chk in self.radio_checkboxes.items():
            if chk.isChecked():
                if self._program_is_running(name):
                    log.info("Launch Selected: %s already running; skipping.", name)
                    continue
                if self._launch_program(name):
                    launched_any = True

        if not launched_any:
            QMessageBox.information(self, "Launch", "No programs were selected.")
        else:
            QTimer.singleShot(1500, self._refresh_running_status)

    def _program_is_running(self, program_name: str) -> bool:
        # Cache process snapshot briefly to avoid multiple psutil walks
        now_ts = datetime.datetime.now().timestamp()
        if now_ts - self._proc_snapshot_ts > 2.0:
            snap: list[str] = []
            for proc in psutil.process_iter(attrs=["name", "exe", "cmdline"]):
                try:
                    name = (proc.info.get("name") or "").lower()
                    exe = os.path.basename(proc.info.get("exe") or "").lower()
                    cmdline_list = proc.info.get("cmdline") or []
                    first_arg = os.path.basename(cmdline_list[0]).lower() if cmdline_list else ""
                    for token in (name, exe, first_arg):
                        if token:
                            snap.append(token)
                except Exception:
                    continue
            self._proc_snapshot = snap
            self._proc_snapshot_ts = now_ts
        exe_path = self._get_saved_program_path(program_name)
        target_names = {program_name.lower(), f"{program_name.lower()}.exe"}
        if exe_path:
            target_names.add(exe_path.name.lower())
        return any(entry in target_names for entry in self._proc_snapshot)

    def _refresh_running_status(self):
        running_js8 = self._program_is_running("JS8Call")
        api_ok = self._js8_api_reachable()
        # Update API indicator (header)
        api_lbl = self.status_labels.get("JS8Call_API")
        if api_lbl:
            if api_ok:
                api_lbl.setStyleSheet("background-color: #4CAF50; border-radius: 7px;")
                api_lbl.setToolTip("API reachable")
            elif running_js8:
                api_lbl.setStyleSheet("background-color: #ff9800; border-radius: 7px;")
                api_lbl.setToolTip("Process running, API unreachable")
            else:
                api_lbl.setStyleSheet("background-color: #555; border-radius: 7px;")
                api_lbl.setToolTip("Not running")

        # Update all other indicators
        for program_name, lbl in self.status_labels.items():
            if program_name == "JS8Call_API":
                continue
            running = running_js8 if program_name == "JS8Call" else self._program_is_running(program_name)
            if program_name == "JS8Call":
                if api_ok:
                    lbl.setStyleSheet("background-color: #4CAF50; border-radius: 7px;")
                    lbl.setToolTip("API reachable")
                elif running:
                    lbl.setStyleSheet("background-color: #ff9800; border-radius: 7px;")
                    lbl.setToolTip("Process running, API unreachable")
                else:
                    lbl.setStyleSheet("background-color: #555; border-radius: 7px;")
                    lbl.setToolTip("Not running")
            else:
                if running:
                    lbl.setStyleSheet("background-color: #4CAF50; border-radius: 7px;")
                    lbl.setToolTip("Running")
                else:
                    lbl.setStyleSheet("background-color: #555; border-radius: 7px;")
                    lbl.setToolTip("Not Running")

    def _js8_api_reachable(self) -> bool:
        """
        Lightweight check: attempt TCP connect to JS8Call API port.
        """
        import socket
        # Prefer UI value (unsaved edits) to avoid stale settings.
        try:
            port_txt = self.js8_port_edit.text().strip()
            port = int(port_txt) if port_txt else int(self.settings.get("js8_port", 2442) or 2442)
        except Exception:
            port = 2442

        hosts = []
        try:
            host_cfg = (self.settings.get("js8_host", "") or "").strip()
            if host_cfg:
                hosts.append(host_cfg)
        except Exception:
            pass
        hosts.extend(["127.0.0.1", "localhost", "::1"])

        # First try raw socket connect
        for host in hosts:
            try:
                with socket.create_connection((host, port), timeout=1.5):
                    log.debug("SettingsTab: JS8 API connect ok host=%s port=%s", host, port)
                    return True
            except Exception as e:
                log.debug("SettingsTab: JS8 API connect failed host=%s port=%s (%s)", host, port, e)
                continue

        # Fallback: try js8net get_freq (this also implicitly connects)
        try:
            from freqinout.radio_interface.js8_status import JS8ControlClient  # lazy import to avoid cycles

            client = JS8ControlClient()
            resp = client.get_frequency()
            if resp is not None:
                log.debug("SettingsTab: JS8 API reachable via js8net get_frequency (resp=%s)", resp)
                return True
            log.debug("SettingsTab: JS8 API js8net get_frequency returned None/False")
        except Exception as e:
            log.debug("SettingsTab: js8net probe failed: %s", e)
        return False

    def _program_autostart_enabled(self, program_name: str) -> bool:
        if program_name not in {"FLDigi", "FLMsg", "FLAmp", "JS8Call"}:
            return False
        meta = self.PROGRAMS.get(program_name)
        if not meta:
            return False
        key = meta.get("autostart_key")
        try:
            val = self.settings.get(key, False)
        except Exception:
            val = False
        return self._is_truthy(val)

    @staticmethod
    def _is_truthy(val) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        if isinstance(val, str):
            return val.strip().lower() in {"true", "1", "yes", "on"}
        return False

    def _auto_start_enabled_programs(self):
        for name in self.PROGRAMS.keys():
            if not self._program_autostart_enabled(name):
                continue
            if self._program_is_running(name):
                continue
            self._launch_program(name)

    # ---------- Operating Groups ---------- #

    def _add_operating_group(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("Add Operating Group")
        form = QFormLayout(dlg)

        name_edit = QLineEdit()
        form.addRow("Group Name:", name_edit)

        mode_combo = QComboBox()
        mode_combo.addItems(["Digi", "SSB"])
        form.addRow("Mode:", mode_combo)

        band_combo = QComboBox()
        band_combo.addItems([
            "20M", "40M", "80M", "2M", "6M", "10M", "12M", "15M", "17M", "30M", "60M",
        ])
        form.addRow("Band:", band_combo)

        freq_edit = QLineEdit()
        freq_edit.setPlaceholderText("e.g., 7.115")
        form.addRow("Frequency (MHz):", freq_edit)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("OK")
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        def on_accept():
            name = name_edit.text().strip()
            mode = mode_combo.currentText()
            band = band_combo.currentText()
            freq_txt = freq_edit.text().strip()
            if not name:
                QMessageBox.warning(self, "Validation", "Group Name is required.")
                return
            if not self._validate_band_frequency(band, mode, freq_txt):
                QMessageBox.warning(self, "Validation", f"Frequency {freq_txt} invalid for {band} {mode}.")
                return
            freq_val = float(freq_txt.replace(",", "."))
            self._upsert_operating_group(name, mode, band, f"{freq_val:.3f}")
            dlg.accept()

        ok_btn.clicked.connect(on_accept)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

    def _validate_band_frequency(self, band: str, mode: str, freq_txt: str) -> bool:
        try:
            freq = float(freq_txt.replace(",", "."))
        except Exception:
            return False
        # Simple band/mode ranges (same as daily schedule)
        limits = {
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
            ("60M", "Digi"): (5.332, 5.405),
            ("60M", "SSB"): (5.332, 5.405),
        }
        key = (band, mode)
        if key not in limits:
            return False
        lo, hi = limits[key]
        return lo <= freq <= hi

    def _format_freq(self, val) -> str:
        try:
            return f"{float(val):.3f}"
        except Exception:
            return str(val) if val is not None else ""

    def _upsert_operating_group(self, name: str, mode: str, band: str, freq_mhz):
        # replace existing entry with same group+mode+band
        freq_display = self._format_freq(freq_mhz)
        updated = False
        for g in self.operating_groups:
            if g.get("group") == name and g.get("mode") == mode and g.get("band") == band:
                g["frequency"] = freq_display
                updated = True
                break
        if not updated:
            self.operating_groups.append(
                {"group": name, "mode": mode, "band": band, "frequency": freq_display}
            )
        self._refresh_operating_groups_table()
        # Persist immediately so additions survive app restarts without requiring an explicit Save click.
        try:
            self._save_settings_quiet()
        except Exception:
            log.exception("Failed to persist Operating Group; will remain in-memory only.")

    def _refresh_operating_groups_table(self):
        # Sort display by Group asc, then Band asc
        self.operating_groups = sorted(
            self.operating_groups,
            key=lambda g: (str(g.get("group", "")).lower(), str(g.get("band", "")).lower()),
        )

        table = self.op_groups_table
        table.setRowCount(0)
        for g in self.operating_groups:
            row = table.rowCount()
            table.insertRow(row)
            sel_chk = QCheckBox()
            table.setCellWidget(row, 0, sel_chk)
            table.setItem(row, 1, QTableWidgetItem(str(g.get("group", ""))))
            table.setItem(row, 2, QTableWidgetItem(str(g.get("mode", ""))))
            table.setItem(row, 3, QTableWidgetItem(str(g.get("band", ""))))
            table.setItem(row, 4, QTableWidgetItem(self._format_freq(g.get("frequency", ""))))
        table.resizeColumnsToContents()
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)

    def _table_to_operating_groups(self) -> List[Dict[str, str]]:
        result: List[Dict[str, str]] = []
        for r in range(self.op_groups_table.rowCount()):
            group = self.op_groups_table.item(r, 1).text().strip() if self.op_groups_table.item(r, 1) else ""
            mode = self.op_groups_table.item(r, 2).text().strip() if self.op_groups_table.item(r, 2) else ""
            band = self.op_groups_table.item(r, 3).text().strip() if self.op_groups_table.item(r, 3) else ""
            freq_txt = self.op_groups_table.item(r, 4).text().strip() if self.op_groups_table.item(r, 4) else ""
            try:
                freq_val = float(freq_txt)
            except Exception:
                freq_val = None
            if group and mode and band and freq_val is not None:
                result.append(
                    {
                        "group": group,
                        "mode": mode,
                        "band": band,
                        "frequency": self._format_freq(freq_val),
                    }
                )
        return result

    def _selected_op_rows(self) -> List[int]:
        rows: List[int] = []
        for r in range(self.op_groups_table.rowCount()):
            w = self.op_groups_table.cellWidget(r, 0)
            if isinstance(w, QCheckBox) and w.isChecked():
                rows.append(r)
        return rows

    def _edit_operating_group(self):
        rows = self._selected_op_rows()
        if not rows:
            QMessageBox.information(self, "Edit Group", "Select one Operating Group to edit.")
            return
        if len(rows) > 1:
            QMessageBox.warning(self, "Edit Group", "Please select only one Operating Group to edit.")
            return
        row = rows[0]
        group = self.op_groups_table.item(row, 1).text().strip() if self.op_groups_table.item(row, 1) else ""
        mode = self.op_groups_table.item(row, 2).text().strip() if self.op_groups_table.item(row, 2) else "Digi"
        band = self.op_groups_table.item(row, 3).text().strip() if self.op_groups_table.item(row, 3) else ""
        freq_txt = self.op_groups_table.item(row, 4).text().strip() if self.op_groups_table.item(row, 4) else ""

        dlg = QDialog(self)
        dlg.setWindowTitle("Edit Operating Group")
        form = QFormLayout(dlg)

        name_edit = QLineEdit(group)
        form.addRow("Group Name:", name_edit)

        mode_combo = QComboBox()
        mode_combo.addItems(["Digi", "SSB"])
        if mode in ["Digi", "SSB"]:
            mode_combo.setCurrentText(mode)
        form.addRow("Mode:", mode_combo)

        band_combo = QComboBox()
        band_combo.addItems(
            [
                "20M",
                "40M",
                "80M",
                "2M",
                "6M",
                "10M",
                "12M",
                "15M",
                "17M",
                "30M",
                "60M",
            ]
        )
        if band and band_combo.findText(band) >= 0:
            band_combo.setCurrentText(band)
        form.addRow("Band:", band_combo)

        freq_edit = QLineEdit(freq_txt)
        form.addRow("Frequency (MHz):", freq_edit)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        def on_accept():
            new_name = name_edit.text().strip()
            new_mode = mode_combo.currentText()
            new_band = band_combo.currentText()
            new_freq_txt = freq_edit.text().strip()
            if not new_name:
                QMessageBox.warning(self, "Validation", "Group Name is required.")
                return
            if not self._validate_band_frequency(new_band, new_mode, new_freq_txt):
                QMessageBox.warning(
                    self, "Validation", f"Frequency {new_freq_txt} invalid for {new_band} {new_mode}."
                )
                return
            # Remove old entry, then insert updated
            self.operating_groups = [
                g
                for g in self.operating_groups
                if not (g.get("group") == group and g.get("mode") == mode and g.get("band") == band)
            ]
            self._upsert_operating_group(new_name, new_mode, new_band, new_freq_txt)
            dlg.accept()

        ok_btn.clicked.connect(on_accept)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

    def _delete_operating_groups(self):
        rows = self._selected_op_rows()
        if not rows:
            QMessageBox.information(self, "Delete Groups", "Select one or more Operating Groups to delete.")
            return
        to_remove = set()
        for r in rows:
            group = self.op_groups_table.item(r, 1).text().strip() if self.op_groups_table.item(r, 1) else ""
            mode = self.op_groups_table.item(r, 2).text().strip() if self.op_groups_table.item(r, 2) else ""
            band = self.op_groups_table.item(r, 3).text().strip() if self.op_groups_table.item(r, 3) else ""
            if group and mode and band:
                to_remove.add((group, mode, band))
        if not to_remove:
            return
        self.operating_groups = [
            g
            for g in self.operating_groups
            if (g.get("group"), g.get("mode"), g.get("band")) not in to_remove
        ]
        self._refresh_operating_groups_table()
        try:
            self._save_settings_quiet()
        except Exception:
            log.exception("Failed to persist Operating Group deletions; will remain in-memory only.")
        QMessageBox.information(self, "Delete Groups", f"Deleted {len(to_remove)} Operating Group(s).")

    # ---------- JS8 DIRECTED PATH ---------- #

    def _choose_js8_directed_path(self):
        fn, _ = QFileDialog.getOpenFileName(
            self,
            "Select JS8Call DIRECTED.TXT",
            "",
            "Text Files (*.txt);;All Files (*)",
        )
        if not fn:
            return

        path = Path(fn)
        if not path.exists():
            QMessageBox.warning(self, "Invalid Path", "Selected file does not exist.")
            return

        if path.name.lower() != "directed.txt":
            resp = QMessageBox.question(
                self,
                "Confirm",
                "The selected file is not DIRECTED.TXT. Use it anyway?",
            )
            if resp != QMessageBox.Yes:
                return

        self.js8_directed_edit.setText(str(path))

        # Persist path without calling write/save
        if hasattr(self.settings, "set"):
            self.settings.set("js8_directed_path", str(path))
        else:
            data = self.settings.all()
            data["js8_directed_path"] = str(path)
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]

        log.info("JS8Call DIRECTED.TXT path saved: %s", path)

    def _load_js8_logs(self):
        """
        Manually ingest JS8 ALL.TXT and DIRECTED.TXT into the link index used by Stations Map.
        """
        self._refresh_operator_history_views()
        directed_path = self.js8_directed_edit.text().strip()
        if not directed_path:
            QMessageBox.warning(self, "Missing path", "Please set JS8Call DIRECTED.TXT path first.")
            return
        path = Path(directed_path)
        if not path.exists():
            QMessageBox.warning(self, "File not found", f"DIRECTED.TXT not found at:\n{path}")
            return
        db_path = Path(__file__).resolve().parents[2] / "config" / "freqinout_nets.db"
        try:
            indexer = JS8LogLinkIndexer(self.settings, db_path)
            indexer.update()
            QMessageBox.information(self, "JS8 Traffic Loaded", "JS8 logs ingested successfully.")
            self._refresh_operator_history_views()
        except Exception as e:
            log.error("SettingsTab: JS8 log ingest failed: %s", e)
            QMessageBox.critical(self, "Error", f"Failed to ingest JS8 logs:\n{e}")
            self._refresh_operator_history_views()
