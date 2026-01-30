from __future__ import annotations

import datetime
import os
import platform
import subprocess
import sqlite3
from pathlib import Path
from typing import Dict, Optional, List

import psutil
from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QIntValidator
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
    QCompleter,
    QToolButton,
)

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.config_paths import get_fldigi_checkin_dir
from freqinout.utils.timezones import get_timezone
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer
from freqinout.gui.theme import resolve_theme, led_style, button_style


TIMEZONE_CHOICES = [
    "UTC",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
]

FLDIGI_MODE_OPTIONS = [
    "Cont-4/250",
    "MFSK32",
    "SSB",
    "FSQ",
    "CW",
    "WWV",
    "WEFAX576",
    "WEFAX288",
    "Cont-4/125",
    "Cont-4/500",
    "Cont-4/1K",
    "Cont-4/2K",
    "Cont-8/125",
    "Cont-8/250",
    "Cont-8/500",
    "Cont-8/1K",
    "Cont-8/2K",
    "Cont-16/250",
    "Cont-16/500",
    "Cont-16/1K",
    "Cont-16/2K",
    "Cont-32/1K",
    "Cont-32/2K",
    "Cont-64/500",
    "Cont-64/1K",
    "Cont-64/2K",
    "DOMEXM",
    "DOMEX4",
    "DOMEX5",
    "DOMEX8",
    "DOMX11",
    "DOMX16",
    "DOMX22",
    "DOMX44",
    "DOMX88",
    "FELDHELL",
    "SLOWHELL",
    "HELLX5",
    "HELLX9",
    "FSKH245",
    "FSKH105",
    "HELL80",
    "MFSK8",
    "MFSK16",
    "MFSK4",
    "MFSK11",
    "MFSK22",
    "MFSK31",
    "MFSK64",
    "MFSK128",
    "MFSK64L",
    "MFSK128L",
    "NAVTEX",
    "SITORB",
    "MT63-500S",
    "MT63-500L",
    "MT63-1KS",
    "MT63-1KL",
    "MT63-2KS",
    "MT63-2KL",
    "BPSK31",
    "BPSK63",
    "BPSK63F",
    "BPSK125",
    "BPSK250",
    "BPSK500",
    "BPSK1000",
    "PSK125C12",
    "PSK250C6",
    "PSK500C2",
    "PSK500C4",
    "PSK800C2",
    "PSK1000C2",
    "QPSK31",
    "QPSK63",
    "QPSK125",
    "QPSK250",
    "QPSK500",
    "8PSK125",
    "8PSK125FL",
    "8PSK125F",
    "8PSK250",
    "8PSK250FL",
    "8PSK250F",
    "8PSK500",
    "8PSK500F",
    "8PSK1000",
    "8PSK1000F",
    "8PSK1200F",
    "OFDM500F",
    "OFDM750F",
    "OFDM3500",
    "OLIVIA",
    "OLIVIA-4/125",
    "OLIVIA-4/250",
    "OLIVIA-4/500",
    "OLIVIA-4/1K",
    "OLIVIA-4/2K",
    "OLIVIA-8/125",
    "OLIVIA-8/250",
    "OLIVIA-8/500",
    "OLIVIA-8/1K",
    "OLIVIA-8/2K",
    "OLIVIA-16/500",
    "OLIVIA-16/1K",
    "OLIVIA-16/2K",
    "OLIVIA-32/1K",
    "OLIVIA-32/2K",
    "OLIVIA-64/500",
    "OLIVIA-64/1K",
    "OLIVIA-64/2K",
    "RTTY",
    "THORM",
    "THOR4",
    "THOR5",
    "THOR8",
    "THOR11",
    "THOR16",
    "THOR22",
    "THOR32",
    "THOR44",
    "THOR56",
    "THOR25x4",
    "THOR50x1",
    "THOR50x2",
    "THOR100",
    "THROB1",
    "THROB2",
    "THROB4",
    "THRBX1",
    "THRBX2",
    "THRBX4",
    "PSK125R",
    "PSK250R",
    "PSK500R",
    "PSK1000R",
    "PSK63RC4",
    "PSK63RC5",
    "PSK63RC10",
    "PSK63RC20",
    "PSK63RC32",
    "PSK125RC4",
    "PSK125RC5",
    "PSK125RC10",
    "PSK125RC12",
    "PSK125RC16",
    "PSK250RC2",
    "PSK250RC3",
    "PSK250RC5",
    "PSK250RC6",
    "PSK250RC7",
    "PSK500RC2",
    "PSK500RC3",
    "PSK500RC4",
    "PSK800RC2",
    "PSK1000RC2",
    "IFKP",
]


class SettingsTab(QWidget):
    """
    Global settings for FreqInOut.

    - Call sign / Name / State
    - Control mode
    - JS8Call TCP port
    - Primary JS8Call groups
    - JS8Call DIRECTED.TXT path
    - Radio software paths

    Timezone is *not* user selectable here; it is auto-detected from the
    system clock and stored under the 'timezone' key in SettingsManager.
    All entries are saved to config when:
      - The Save button is clicked, OR
      - The application exits (QApplication.aboutToQuit).

    Persistence is done via SettingsManager.set(...) when available,
    or by updating SettingsManager._data as a fallback. We *do not*
    call any .write() or .save() here to avoid AttributeError.
    """

    settings_saved = Signal()

    settings_saved = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._settings_dirty = False
        self._loading_settings = False
        self.loading_label: QLabel | None = None

        self.PROGRAMS: Dict[str, Dict[str, str]] = {
            "FLRig": {"setting_key": "path_flrig", "autostart_key": "autostart_flrig"},
            "FLDigi": {"setting_key": "path_fldigi", "autostart_key": "autostart_fldigi"},
            "FLMsg": {"setting_key": "path_flmsg", "autostart_key": "autostart_flmsg"},
            "FLAmp": {"setting_key": "path_flamp", "autostart_key": "autostart_flamp"},
            # JS8Call is managed externally; no launch/autostart controls here.
        }

        self.radio_checkboxes: Dict[str, QCheckBox] = {}
        self.status_labels: Dict[str, QLabel] = {}
        self.path_edits: Dict[str, QLineEdit] = {}
        self.js8_groups_edits: List[QLineEdit] = []
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self.operating_groups: List[Dict[str, str]] = []
        self._accordion_groups: List[QGroupBox] = []
        self._section_meta: Dict[QGroupBox, Dict[str, object]] = {}

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
        QTimer.singleShot(0, self._maybe_backfill_js8_geo)

    # ---------- UI ---------- #

    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)

        header_layout = QHBoxLayout()
        title_label = QLabel("<h2>Settings</h2>")
        header_layout.addWidget(title_label)
        self.loading_label = QLabel("Wilco. Standby for Spectrum QSY...")
        self.loading_label.setVisible(False)
        self.loading_label.setStyleSheet("padding: 2px 6px; border-radius: 4px;")
        header_layout.addWidget(self.loading_label)
        header_layout.addStretch()

        self.utc_label = QLabel()
        self.local_label = QLabel()
        header_layout.addWidget(self.utc_label)
        header_layout.addWidget(self.local_label)
        main_layout.addLayout(header_layout)

        # Identity group
        callsign_group = QGroupBox("Operator Information")
        callsign_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        callsign_layout = QVBoxLayout()
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
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Callsign:"))
        row1.addWidget(self.callsign_edit)
        row1.addSpacing(12)
        row1.addWidget(QLabel("Name:"))
        row1.addWidget(self.name_edit)
        row1.addSpacing(12)
        row1.addWidget(QLabel("State:"))
        row1.addWidget(self.state_edit)
        row1.addSpacing(12)
        row1.addWidget(QLabel("Grid 6:"))
        row1.addWidget(self.grid6_edit)
        row1.addStretch()
        callsign_layout.addLayout(row1)
        callsign_group.setLayout(callsign_layout)
        main_layout.addWidget(callsign_group)

        # Operation settings (control)
        op_group = QGroupBox("FreqInOut Settings")
        op_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        op_layout = QVBoxLayout()
        op_group.setLayout(op_layout)
        main_layout.addWidget(op_group)

        # control mode (no timezone dropdown anymore)
        ctrl_row = QHBoxLayout()
        ctrl_row.addWidget(QLabel("Theme:"))
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["Light", "Dark"])
        self.theme_combo.currentIndexChanged.connect(self._on_theme_changed)
        ctrl_row.addWidget(self.theme_combo)
        ctrl_row.addSpacing(12)
        ctrl_row.addWidget(QLabel("Frequency Control:"))
        self.control_combo = QComboBox()
        self.control_combo.addItems(["FLRig", "JS8Call", "Manual"])
        ctrl_row.addWidget(self.control_combo)
        ctrl_row.addSpacing(12)
        self.use_scheduler_chk = QCheckBox("Use FreqInOut Scheduler")
        self.use_scheduler_chk.setToolTip("Enable automatic schedule-driven frequency changes.")
        ctrl_row.addWidget(self.use_scheduler_chk)
        ctrl_row.addSpacing(12)
        ctrl_row.addStretch()
        op_layout.addLayout(ctrl_row)

        enforcement_choices = ["On Schedule Change", "Prompt"]
        prompt_choices = [
            "Select Interval",
            "Hourly",
            "Every 5 minutes",
            "Every 10 minutes",
            "Every 15 minutes",
            "Every 30 minutes",
        ]

        freq_row = QHBoxLayout()
        self.freq_timer_label = QLabel("Frequency Timer:")
        freq_row.addWidget(self.freq_timer_label)
        self.freq_enforce_combo = QComboBox()
        self.freq_enforce_combo.addItems(enforcement_choices)
        self.freq_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        freq_row.addWidget(self.freq_enforce_combo)
        freq_row.addSpacing(12)
        self.freq_prompt_label = QLabel("Prompt Interval:")
        freq_row.addWidget(self.freq_prompt_label)
        self.freq_prompt_combo = QComboBox()
        self.freq_prompt_combo.addItems(prompt_choices)
        self.freq_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.freq_prompt_combo)
        freq_row.addWidget(self.freq_prompt_combo)
        freq_row.addStretch()
        op_layout.addLayout(freq_row)

        fldigi_row = QHBoxLayout()
        self.fldigi_timer_label = QLabel("FLDigi Mode Timer:")
        fldigi_row.addWidget(self.fldigi_timer_label)
        self.fldigi_enforce_combo = QComboBox()
        self.fldigi_enforce_combo.addItems(enforcement_choices)
        self.fldigi_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        fldigi_row.addWidget(self.fldigi_enforce_combo)
        fldigi_row.addSpacing(12)
        self.fldigi_prompt_label = QLabel("Prompt Interval:")
        fldigi_row.addWidget(self.fldigi_prompt_label)
        self.fldigi_prompt_combo = QComboBox()
        self.fldigi_prompt_combo.addItems(prompt_choices)
        self.fldigi_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.fldigi_prompt_combo)
        fldigi_row.addWidget(self.fldigi_prompt_combo)
        fldigi_row.addStretch()
        op_layout.addLayout(fldigi_row)

        js8_row = QHBoxLayout()
        self.js8_timer_label = QLabel("JS8 Offset Timer:")
        js8_row.addWidget(self.js8_timer_label)
        self.js8_enforce_combo = QComboBox()
        self.js8_enforce_combo.addItems(enforcement_choices)
        self.js8_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        js8_row.addWidget(self.js8_enforce_combo)
        js8_row.addSpacing(12)
        self.js8_prompt_label = QLabel("Prompt Interval:")
        js8_row.addWidget(self.js8_prompt_label)
        self.js8_prompt_combo = QComboBox()
        self.js8_prompt_combo.addItems(prompt_choices)
        self.js8_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.js8_prompt_combo)
        js8_row.addWidget(self.js8_prompt_combo)
        js8_row.addStretch()
        op_layout.addLayout(js8_row)

        self._align_enforcement_labels()

        # Operating status indicators (always visible)
        status_group = QGroupBox("Operating Status")
        status_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        status_layout = QHBoxLayout()
        status_group.setLayout(status_layout)

        theme = resolve_theme(self.settings)
        status_items = [
            ("JS8Call_API", "JS8"),
            ("FLRig", "FLRig"),
            ("FLDigi", "FLDigi"),
            ("FLMsg", "FLMsg"),
            ("FLAmp", "FLAmp"),
            ("VarAC", "VarAC"),
            ("JS8Spotter", "JS8Spotter"),
            ("CommStat", "CommStat"),
        ]
        for key, label in status_items:
            led = QLabel()
            led.setFixedSize(14, 14)
            led.setStyleSheet(led_style("idle", theme))
            self.status_labels[key] = led
            status_layout.addWidget(led)
            status_layout.addWidget(QLabel(label))
            status_layout.addSpacing(12)
        status_layout.addStretch()
        main_layout.addWidget(status_group)

        # Operating Groups panel
        ops_group = QGroupBox("Operating Groups")
        ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        ops_layout = QVBoxLayout()
        ops_layout.setSpacing(6)
        ops_group.setLayout(ops_layout)
        add_row = QHBoxLayout()
        self.add_group_btn = QPushButton("Add Group")
        self.add_group_btn.clicked.connect(self._add_operating_group)
        self.edit_group_btn = QPushButton("Edit Selected")
        self.edit_group_btn.clicked.connect(self._edit_operating_group)
        self.delete_group_btn = QPushButton("Delete Selected")
        self.delete_group_btn.clicked.connect(self._delete_operating_groups)
        add_row.addStretch()
        add_row.addWidget(self.add_group_btn)
        add_row.addWidget(self.edit_group_btn)
        add_row.addWidget(self.delete_group_btn)
        ops_layout.addLayout(add_row)
        self.op_groups_table = QTableWidget(0, 9)
        self.op_groups_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Group",
                "Mode",
                "Band",
                "Freq (MHz)",
                "VFO",
                "FLDigi Starting Mode",
                "FLDigi Starting Offset",
                "Auto-Tune",
            ]
        )
        header = self.op_groups_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.Fixed)
        header.setSectionResizeMode(7, QHeaderView.Stretch)
        header.setStretchLastSection(True)
        header.setMinimumSectionSize(50)
        self.op_groups_table.setColumnWidth(8, 110)
        self.op_groups_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.op_groups_table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.op_groups_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.op_groups_table.setEditTriggers(QTableWidget.NoEditTriggers)
        ops_layout.addWidget(self.op_groups_table)
        ops_container = QWidget()
        ops_container.setLayout(ops_layout)
        ops_group = self._make_collapsible_group("Operating Groups", ops_container, checked=True, fit_content=False)
        self._register_collapsible_group(ops_group, self._summary_operating_groups)
        ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        main_layout.addWidget(ops_group)

        # JS8Call status/settings (managed externally)
        js8_group = QGroupBox("JS8Call Settings")
        js8_v = QVBoxLayout()
        js8_v.setSpacing(6)
        js8_group.setLayout(js8_v)

        js8_port_row = QHBoxLayout()
        js8_port_row.addWidget(QLabel("TCP Port"))
        self.js8_port_edit = QLineEdit()
        self.js8_port_edit.setFixedWidth(80)
        self.js8_port_edit.setText("2442")
        js8_port_row.addWidget(self.js8_port_edit)
        js8_port_row.addSpacing(12)
        js8_port_row.addWidget(QLabel("Offset (Hz)"))
        self.js8_offset_edit = QLineEdit()
        self.js8_offset_edit.setFixedWidth(80)
        self.js8_offset_edit.setText("0")
        js8_port_row.addWidget(self.js8_offset_edit)
        js8_port_row.addSpacing(12)
        js8_port_row.addWidget(QLabel("Mark JS8Call MSG Read?"))
        self.js8_mark_retrieved_chk = QCheckBox()
        self.js8_mark_retrieved_chk.setToolTip(
            "When enabled, clicking 'Mark Retrieved' in Message Viewer will set JS8Call inbox entries to READ."
        )
        js8_port_row.addWidget(self.js8_mark_retrieved_chk)
        js8_port_row.addStretch()
        js8_v.addLayout(js8_port_row)

        directed_forms_row = QHBoxLayout()
        directed_forms_row.addWidget(QLabel("JS8Call DIRECTED.TXT:"))
        self.js8_directed_edit = QLineEdit()
        directed_browse = QPushButton("Browse")
        directed_browse.clicked.connect(self._choose_js8_directed_path)
        directed_forms_row.addWidget(self.js8_directed_edit, stretch=1)
        directed_forms_row.addWidget(directed_browse)
        js8_v.addLayout(directed_forms_row)
        forms_row = QHBoxLayout()
        forms_row.addWidget(QLabel("JS8Spotter forms:"))
        self.js8_forms_edit = QLineEdit()
        forms_browse = QPushButton("Browse")
        forms_browse.clicked.connect(self._choose_js8_forms_path)
        forms_row.addWidget(self.js8_forms_edit, stretch=1)
        forms_row.addWidget(forms_browse)
        js8_v.addLayout(forms_row)
        self.js8_directed_edit.textChanged.connect(self._refresh_section_titles)
        self.js8_forms_edit.textChanged.connect(self._refresh_section_titles)

        load_links_row = QHBoxLayout()
        self.load_js8_btn = QPushButton("Load JS8 Traffic")
        self.load_js8_btn.clicked.connect(self._load_js8_logs)
        load_links_row.addWidget(self.load_js8_btn)
        load_links_row.addStretch()
        js8_v.addLayout(load_links_row)

        js8_container = QWidget()
        js8_container.setLayout(js8_v)
        js8_group = self._make_collapsible_group("JS8Call Settings", js8_container, checked=False, fit_content=True)
        self._register_collapsible_group(js8_group, self._summary_js8_settings)
        js8_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        main_layout.addWidget(js8_group)

        # Radio software
        radio_group = QGroupBox("Radio Software")
        radio_v = QVBoxLayout()
        radio_v.setSpacing(6)
        radio_group.setLayout(radio_v)
        radio_grid = QHBoxLayout()

        # Left column: program rows
        prog_layout = QVBoxLayout()
        msg_layout = QVBoxLayout()
        msg_layout.setSpacing(6)

        def build_prog_row(name: str) -> QHBoxLayout:
            row = QHBoxLayout()
            chk = QCheckBox(name)
            self.radio_checkboxes[name] = chk
            chk.stateChanged.connect(self._update_launch_selected_state)
            row.addWidget(chk)

            path_edit = QLineEdit()
            path_edit.setPlaceholderText("Path to executable")
            self.path_edits[name] = path_edit
            path_edit.textChanged.connect(self._refresh_section_titles)
            row.addWidget(path_edit)

            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(lambda _, n=name: self._choose_program_path(n))
            row.addWidget(browse_btn)
            return row

        msg_label_width = 160

        def build_msg_row(label: str, edit: QLineEdit, browse_cb) -> QHBoxLayout:
            row = QHBoxLayout()
            lbl = QLabel(label)
            lbl.setFixedWidth(msg_label_width)
            row.addWidget(lbl)
            row.addWidget(edit, 1)
            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(browse_cb)
            row.addWidget(browse_btn)
            return row

        # Message path edits
        self.msg_paths_edits = {}
        varac_edit = QLineEdit()
        self.msg_paths_edits["varac"] = varac_edit
        flmsg_edit = QLineEdit()
        self.msg_paths_edits["flmsg"] = flmsg_edit
        flamp_edit = QLineEdit()
        self.msg_paths_edits["flamp"] = flamp_edit

        # FLRig row + XMLRPC port
        flrig_row = QHBoxLayout()
        flrig_chk = QCheckBox("FLRig")
        self.radio_checkboxes["FLRig"] = flrig_chk
        flrig_chk.stateChanged.connect(self._update_launch_selected_state)
        flrig_row.addWidget(flrig_chk)

        flrig_path = QLineEdit()
        flrig_path.setPlaceholderText("Path to executable")
        self.path_edits["FLRig"] = flrig_path
        flrig_path.textChanged.connect(self._refresh_section_titles)
        flrig_row.addWidget(flrig_path)

        flrig_browse = QPushButton("Browse")
        flrig_browse.setFixedWidth(70)
        flrig_browse.clicked.connect(lambda _: self._choose_program_path("FLRig"))
        flrig_row.addWidget(flrig_browse)

        prog_layout.addLayout(flrig_row)

        flrig_port_row = QHBoxLayout()
        flrig_port_label = QLabel("FLRig XMLRPC Port")
        flrig_port_label.setFixedWidth(msg_label_width)
        flrig_port_row.addWidget(flrig_port_label)
        self.flrig_port_edit = QLineEdit()
        self.flrig_port_edit.setFixedWidth(80)
        self.flrig_port_edit.setText("12345")
        flrig_port_row.addWidget(self.flrig_port_edit)
        flrig_port_row.addStretch()
        flrig_port_spacer = QWidget()
        flrig_port_spacer.setFixedWidth(70)
        flrig_port_row.addWidget(flrig_port_spacer)
        msg_layout.addLayout(flrig_port_row)

        # FLDigi row + check-in path
        prog_layout.addLayout(build_prog_row("FLDigi"))
        self.fldigi_checkin_dir_edit = QLineEdit()
        fldigi_browse = QPushButton("Browse")
        fldigi_browse.clicked.connect(self._choose_fldigi_checkin_dir)
        self.fldigi_checkin_dir_edit.textChanged.connect(self._refresh_fldigi_checkin_file_labels)
        self.fldigi_main_file_edit = QLineEdit()
        self.fldigi_main_file_edit.setReadOnly(True)
        self.fldigi_main_file_edit.hide()
        self.fldigi_late_file_edit = QLineEdit()
        self.fldigi_late_file_edit.setReadOnly(True)
        self.fldigi_late_file_edit.hide()
        fldigi_row = QHBoxLayout()
        fldigi_label = QLabel("Check-in File Path")
        fldigi_label.setFixedWidth(msg_label_width)
        fldigi_row.addWidget(fldigi_label)
        fldigi_row.addWidget(self.fldigi_checkin_dir_edit, 1)
        fldigi_row.addWidget(fldigi_browse)
        msg_layout.addLayout(fldigi_row)

        # FLMsg row + ICS/Messages path
        prog_layout.addLayout(build_prog_row("FLMsg"))
        msg_layout.addLayout(
            build_msg_row(
                "ICS/Messages",
                flmsg_edit,
                lambda: self._choose_msg_path("flmsg", flmsg_edit),
            )
        )

        # FLAmp row + FLAMP/rx path
        prog_layout.addLayout(build_prog_row("FLAmp"))
        msg_layout.addLayout(
            build_msg_row(
                "FLAMP/rx",
                flamp_edit,
                lambda: self._choose_msg_path("flamp", flamp_edit),
            )
        )

        # VarAC status row + install path
        varac_row = QHBoxLayout()
        varac_row.addWidget(QLabel("VarAC"))
        self.varac_path_edit = QLineEdit()
        self.varac_path_edit.setReadOnly(False)
        self.varac_path_edit.setPlaceholderText("VarAC install folder")
        varac_row.addWidget(self.varac_path_edit, 1)
        varac_browse = QPushButton("Browse")
        varac_browse.setFixedWidth(70)
        varac_browse.clicked.connect(self._choose_varac_install_path)
        varac_row.addWidget(varac_browse)
        prog_layout.addLayout(varac_row)

        msg_layout.addLayout(
            build_msg_row(
                "VarAC Incoming Files",
                varac_edit,
                lambda: self._choose_msg_path("varac", varac_edit),
            )
        )

        radio_grid.addLayout(prog_layout, 3)
        radio_grid.addLayout(msg_layout, 4)
        radio_v.addLayout(radio_grid)

        # Launch Selected + Check-in log file copy helpers (single row)
        self.launch_selected_btn = QPushButton("Launch Selected")
        self.launch_selected_btn.clicked.connect(self._launch_selected_programs)
        self.launch_selected_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        launch_row = QHBoxLayout()
        launch_row.addWidget(self.launch_selected_btn)
        launch_row.addStretch()
        launch_row.addWidget(QLabel("Check-in Log Paths:"))
        self.copy_main_btn = QPushButton("Copy Main")
        self.copy_main_btn.clicked.connect(lambda: self._copy_text(self.fldigi_main_file_edit))
        self.copy_late_btn = QPushButton("Copy New/Late")
        self.copy_late_btn.clicked.connect(lambda: self._copy_text(self.fldigi_late_file_edit))
        launch_row.addWidget(self.copy_main_btn)
        launch_row.addWidget(self.copy_late_btn)
        radio_v.addLayout(launch_row)

        radio_container = QWidget()
        radio_container.setLayout(radio_v)
        radio_group = self._make_collapsible_group("Radio Software", radio_container, checked=False, fit_content=True)
        self._register_collapsible_group(radio_group, self._summary_radio_settings)
        radio_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        main_layout.addWidget(radio_group)

        main_layout.addStretch(1)
        # bottom save
        bottom_row = QHBoxLayout()
        bottom_row.addStretch()
        self.save_btn = QPushButton("Save Settings")
        self.save_btn.clicked.connect(self._save_settings_button)
        bottom_row.addWidget(self.save_btn)
        main_layout.addLayout(bottom_row)
        self.launch_selected_btn.setMinimumWidth(self.launch_selected_btn.sizeHint().width() + 12)
        self._wire_dirty_tracking()
        self._set_save_button_state("success")
        self._refresh_section_titles()

    def _make_collapsible_group(
        self,
        title: str,
        content: QWidget,
        *,
        checked: bool,
        fit_content: bool,
    ) -> QGroupBox:
        group = QGroupBox()
        group.setMinimumHeight(0)
        content.setVisible(checked)

        header_btn = QToolButton()
        header_btn.setCheckable(True)
        header_btn.setChecked(checked)
        header_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        header_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        header_btn.setText(title)
        header_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        header_btn.setMinimumHeight(28)
        header_btn.setStyleSheet("QToolButton { padding: 4px 6px; font-weight: 600; }")

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.addWidget(header_btn)
        header_row.addStretch()

        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 12)
        layout.setSpacing(6)
        layout.addLayout(header_row)
        layout.addWidget(content)
        group.setLayout(layout)

        header_btn.toggled.connect(lambda state, g=group, w=content: self._on_section_toggled(g, w, state))
        self._section_meta[group] = {
            **self._section_meta.get(group, {}),
            "fit_content": fit_content,
            "title": title,
            "header_btn": header_btn,
        }
        self._apply_collapsed_state(group, content, checked)
        QTimer.singleShot(0, lambda g=group, w=content: self._apply_collapsed_state(g, w, header_btn.isChecked()))
        return group

    def _register_collapsible_group(self, group: QGroupBox, summary_fn) -> None:
        self._accordion_groups.append(group)
        meta = self._section_meta.get(group, {})
        meta.update({"summary_fn": summary_fn})
        self._section_meta[group] = meta

    def _on_section_toggled(self, group: QGroupBox, content: QWidget, checked: bool) -> None:
        self._apply_collapsed_state(group, content, checked)
        if checked:
            for other in self._accordion_groups:
                if other is not group:
                    other_btn = self._section_meta.get(other, {}).get("header_btn")
                    if other_btn and other_btn.isChecked():
                        other_btn.setChecked(False)
        self._refresh_section_titles()

    def _refresh_section_titles(self) -> None:
        for group, meta in self._section_meta.items():
            base = str(meta.get("title", ""))
            summary_fn = meta.get("summary_fn")
            header_btn = meta.get("header_btn")
            if header_btn and header_btn.isChecked():
                if header_btn:
                    header_btn.setText(base)
                continue
            summary = ""
            try:
                if summary_fn:
                    summary = str(summary_fn()).strip()
            except Exception:
                summary = ""
            if header_btn:
                header_btn.setText(f"{base} — {summary}" if summary else base)

    def _apply_collapsed_state(self, group: QGroupBox, content: QWidget, expanded: bool) -> None:
        content.setVisible(expanded)
        fit_content = bool(self._section_meta.get(group, {}).get("fit_content", False))
        header_btn = self._section_meta.get(group, {}).get("header_btn")
        if header_btn:
            header_btn.setArrowType(Qt.DownArrow if expanded else Qt.RightArrow)
        if expanded:
            if fit_content:
                header_height = 0
                if header_btn:
                    header_height = header_btn.sizeHint().height()
                margins = group.layout().contentsMargins() if group.layout() else None
                extra = 0
                if margins:
                    extra = margins.top() + margins.bottom()
                target_height = content.sizeHint().height() + header_height + extra
                group.setMinimumHeight(target_height)
                group.setMaximumHeight(target_height)
                group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            else:
                group.setMinimumHeight(0)
                group.setMaximumHeight(16777215)
                group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        else:
            collapsed = self._collapsed_height(group)
            group.setMinimumHeight(collapsed)
            group.setMaximumHeight(collapsed)
            group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        group.updateGeometry()

    def _collapsed_height(self, group: QGroupBox) -> int:
        header_btn = self._section_meta.get(group, {}).get("header_btn")
        margins = group.layout().contentsMargins() if group.layout() else None
        extra = 0
        if margins:
            extra = margins.top() + margins.bottom()
        if header_btn:
            return max(34, header_btn.sizeHint().height() + extra)
        return max(34, group.fontMetrics().height() + 24 + extra)

    def _summary_operating_groups(self) -> str:
        count = len(self.operating_groups)
        return f"{count} group{'s' if count != 1 else ''}"

    def _summary_js8_settings(self) -> str:
        directed = "set" if self.js8_directed_edit.text().strip() else "missing"
        forms = "set" if self.js8_forms_edit.text().strip() else "missing"
        return f"DIRECTED {directed}, Forms {forms}"

    def _summary_radio_settings(self) -> str:
        total = len(self.PROGRAMS)
        set_count = 0
        for name in self.PROGRAMS:
            edit = self.path_edits.get(name)
            if edit and edit.text().strip():
                set_count += 1
        return f"{set_count}/{total} program paths set"

    def _disable_prompt_hint_item(self, combo: QComboBox) -> None:
        try:
            model = combo.model()
            if model is None:
                return
            idx = model.index(0, 0)
            model.setData(idx, 0, Qt.UserRole - 1)
        except Exception:
            pass

    def _align_enforcement_labels(self) -> None:
        labels = [
            self.freq_timer_label,
            self.fldigi_timer_label,
            self.js8_timer_label,
        ]
        prompt_labels = [
            self.freq_prompt_label,
            self.fldigi_prompt_label,
            self.js8_prompt_label,
        ]
        try:
            max_timer = max(lbl.sizeHint().width() for lbl in labels)
            max_prompt = max(lbl.sizeHint().width() for lbl in prompt_labels)
            for lbl in labels:
                lbl.setFixedWidth(max_timer)
            for lbl in prompt_labels:
                lbl.setFixedWidth(max_prompt)
        except Exception:
            pass

    # ---------- LOAD/SAVE ---------- #

    def _load_settings(self):
        self._loading_settings = True
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
        self.use_scheduler_chk.setChecked(bool(data.get("use_scheduler", True)))
        freq_mode = (data.get("freq_enforcement_mode", "On Schedule Change") or "On Schedule Change").strip()
        fldigi_mode = (data.get("fldigi_enforcement_mode", "On Schedule Change") or "On Schedule Change").strip()
        js8_mode = (data.get("js8_enforcement_mode", "On Schedule Change") or "On Schedule Change").strip()
        if freq_mode not in {"On Schedule Change", "Prompt"}:
            freq_mode = "On Schedule Change"
        if fldigi_mode not in {"On Schedule Change", "Prompt"}:
            fldigi_mode = "On Schedule Change"
        if js8_mode not in {"On Schedule Change", "Prompt"}:
            js8_mode = "On Schedule Change"
        self.freq_enforce_combo.setCurrentText(freq_mode)
        self.fldigi_enforce_combo.setCurrentText(fldigi_mode)
        self.js8_enforce_combo.setCurrentText(js8_mode)

        freq_prompt = (data.get("freq_prompt_interval", "Hourly") or "Hourly").strip()
        fldigi_prompt = (data.get("fldigi_prompt_interval", "Hourly") or "Hourly").strip()
        js8_prompt = (data.get("js8_prompt_interval", "Hourly") or "Hourly").strip()
        prompt_choices = {"Hourly", "Every 5 minutes", "Every 10 minutes", "Every 15 minutes", "Every 30 minutes"}
        if freq_prompt not in prompt_choices:
            freq_prompt = ""
        if fldigi_prompt not in prompt_choices:
            fldigi_prompt = ""
        if js8_prompt not in prompt_choices:
            js8_prompt = ""
        self.freq_prompt_combo.setCurrentText(freq_prompt or "Select Interval")
        self.fldigi_prompt_combo.setCurrentText(fldigi_prompt or "Select Interval")
        self.js8_prompt_combo.setCurrentText(js8_prompt or "Select Interval")
        if freq_mode != "Prompt":
            self.freq_prompt_combo.setCurrentText("Select Interval")
        if fldigi_mode != "Prompt":
            self.fldigi_prompt_combo.setCurrentText("Select Interval")
        if js8_mode != "Prompt":
            self.js8_prompt_combo.setCurrentText("Select Interval")
        self._update_enforcement_visibility()
        theme = (data.get("ui_theme", "light") or "light").strip().lower()
        self.theme_combo.setCurrentText("Dark" if theme == "dark" else "Light")

        port_txt = str(data.get("js8_port", "2442") or "2442")
        self.js8_port_edit.setText(port_txt)
        offset_val = data.get("js8_offset_hz", None)
        try:
            offset_int = int(offset_val) if offset_val not in (None, "") else 0
        except Exception:
            offset_int = 0
        if offset_int <= 0:
            offset_int = 1900 + (datetime.datetime.utcnow().hour % 7) * 50
            if hasattr(self.settings, "set"):
                self.settings.set("js8_offset_hz", offset_int)
            else:
                data["js8_offset_hz"] = offset_int
                if hasattr(self.settings, "_data"):
                    self.settings._data = data  # type: ignore[attr-defined]
        self.js8_offset_edit.setText(str(offset_int))
        self.js8_forms_edit.setText(data.get("js8_forms_path", "") or "")
        self.js8_mark_retrieved_chk.setChecked(
            bool(data.get("js8_inbox_mark_retrieved_sync", False))
        )
        # Message paths
        msg_paths = data.get("message_paths", {})
        for origin, edit in self.msg_paths_edits.items():
            edit.setText(msg_paths.get(origin, ""))
        varac_path = (data.get("varac_path", "") or "").strip()
        if not varac_path:
            legacy_db = (data.get("varac_db_path", "") or "").strip()
            if legacy_db:
                try:
                    legacy = Path(legacy_db)
                    if legacy.is_file():
                        varac_path = str(legacy.parent)
                    elif legacy.is_dir():
                        varac_path = str(legacy)
                except Exception:
                    varac_path = legacy_db
        if hasattr(self, "varac_path_edit"):
            self.varac_path_edit.setText(varac_path)
        fldigi_dir = (data.get("fldigi_checkin_dir", "") or "").strip()
        if not fldigi_dir:
            fldigi_dir = str(get_fldigi_checkin_dir())
            if hasattr(self.settings, "set"):
                self.settings.set("fldigi_checkin_dir", fldigi_dir)
            else:
                data["fldigi_checkin_dir"] = fldigi_dir
                if hasattr(self.settings, "_data"):
                    self.settings._data = data  # type: ignore[attr-defined]
        self.fldigi_checkin_dir_edit.setText(fldigi_dir)
        self._refresh_fldigi_checkin_file_labels()
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
                self.operating_groups = []
                for g in og:
                    if not isinstance(g, dict):
                        continue
                    vfo_val = (g.get("vfo") or "A").strip().upper()
                    if vfo_val not in ("A", "B"):
                        vfo_val = "A"
                    self.operating_groups.append(
                        {
                            "group": str(g.get("group", "")).upper(),
                            "mode": g.get("mode", ""),
                            "band": g.get("band", ""),
                            "frequency": g.get("frequency", ""),
                            "vfo": vfo_val,
                            "fldigi_mode": (g.get("fldigi_mode") or "").strip(),
                            "fldigi_offset": (g.get("fldigi_offset") or "").strip(),
                            "auto_tune": bool(g.get("auto_tune", False)),
                        }
                    )
        except Exception:
            self.operating_groups = []
        self._refresh_operating_groups_table()

        self.js8_directed_edit.setText(data.get("js8_directed_path", "") or "")

        for prog_name, meta in self.PROGRAMS.items():
            path_key = meta["setting_key"]
            enabled_key = f"{prog_name.lower()}_enabled"

            if path_key:
                self.path_edits[prog_name].setText(data.get(path_key, "") or "")
            if prog_name in self.radio_checkboxes:
                self.radio_checkboxes[prog_name].setChecked(bool(data.get(enabled_key, False)))

        log.info("SettingsTab: settings loaded.")
        self._update_launch_selected_state()
        self._update_op_group_action_buttons()
        self._loading_settings = False
        self._settings_dirty = False
        self._set_save_button_state("success")
        self._refresh_section_titles()

    def _save_settings_button(self):
        """Explicit save via the button (shows confirmation)."""
        self._save_settings(show_message=True)
        try:
            self.settings_saved.emit()
        except Exception:
            pass
        try:
            self.settings_saved.emit()
        except Exception:
            pass
        QTimer.singleShot(0, self._maybe_backfill_js8_geo)
        self._settings_dirty = False
        self._set_save_button_state("success")

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
        data["use_scheduler"] = bool(self.use_scheduler_chk.isChecked())
        freq_mode = self.freq_enforce_combo.currentText().strip()
        fldigi_mode = self.fldigi_enforce_combo.currentText().strip()
        js8_mode = self.js8_enforce_combo.currentText().strip()
        freq_prompt = self.freq_prompt_combo.currentText().strip()
        fldigi_prompt = self.fldigi_prompt_combo.currentText().strip()
        js8_prompt = self.js8_prompt_combo.currentText().strip()
        missing = []
        if freq_mode == "Prompt" and freq_prompt == "Select Interval":
            missing.append("Frequency Prompt Interval")
        if fldigi_mode == "Prompt" and fldigi_prompt == "Select Interval":
            missing.append("FLDigi Prompt Interval")
        if js8_mode == "Prompt" and js8_prompt == "Select Interval":
            missing.append("JS8 Prompt Interval")
        if missing:
            QMessageBox.warning(self, "Settings", f"Please select: {', '.join(missing)}.")
            return
        data["freq_enforcement_mode"] = freq_mode
        data["freq_prompt_interval"] = freq_prompt
        data["fldigi_enforcement_mode"] = fldigi_mode
        data["fldigi_prompt_interval"] = fldigi_prompt
        data["js8_enforcement_mode"] = js8_mode
        data["js8_prompt_interval"] = js8_prompt
        data["ui_theme"] = self.theme_combo.currentText().strip().lower()

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

        data["js8_forms_path"] = self.js8_forms_edit.text().strip()
        data["js8_inbox_mark_retrieved_sync"] = bool(self.js8_mark_retrieved_chk.isChecked())
        msg_paths = {}
        for origin, edit in self.msg_paths_edits.items():
            msg_paths[origin] = edit.text().strip()
        data["message_paths"] = msg_paths
        varac_path = self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else ""
        data["varac_path"] = varac_path
        data["varac_db_path"] = str(Path(varac_path) / "VarAC.db") if varac_path else ""
        fldigi_dir = self.fldigi_checkin_dir_edit.text().strip()
        if not fldigi_dir:
            fldigi_dir = str(get_fldigi_checkin_dir())
            self.fldigi_checkin_dir_edit.setText(fldigi_dir)
        data["fldigi_checkin_dir"] = fldigi_dir

        groups = [le.text().strip().upper() for le in self.js8_groups_edits if le.text().strip()]
        data["primary_js8_groups"] = groups

        data["js8_directed_path"] = self.js8_directed_edit.text().strip()

        # Radio software paths / autostart / enabled flags from UI
        for prog_name, meta in self.PROGRAMS.items():
            path_key = meta["setting_key"]
            enabled_key = f"{prog_name.lower()}_enabled"

            if path_key:
                data[path_key] = self.path_edits[prog_name].text().strip()
            if prog_name in self.radio_checkboxes:
                data[enabled_key] = bool(self.radio_checkboxes[prog_name].isChecked())

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
                "use_scheduler": data["use_scheduler"],
                "freq_enforcement_mode": data.get("freq_enforcement_mode", "On Schedule Change"),
                "freq_prompt_interval": data.get("freq_prompt_interval", "Hourly"),
                "fldigi_enforcement_mode": data.get("fldigi_enforcement_mode", "On Schedule Change"),
                "fldigi_prompt_interval": data.get("fldigi_prompt_interval", "Hourly"),
                "js8_enforcement_mode": data.get("js8_enforcement_mode", "On Schedule Change"),
                "js8_prompt_interval": data.get("js8_prompt_interval", "Hourly"),
                "ui_theme": data.get("ui_theme", "light"),
                "js8_port": data["js8_port"],
                "js8_offset_hz": data.get("js8_offset_hz", 0),
                "primary_js8_groups": data["primary_js8_groups"],
                "js8_directed_path": data["js8_directed_path"],
                "js8_forms_path": data.get("js8_forms_path", ""),
                "js8_inbox_mark_retrieved_sync": data.get("js8_inbox_mark_retrieved_sync", False),
                "message_paths": data.get("message_paths", {}),
                "varac_path": data.get("varac_path", ""),
                "varac_db_path": data.get("varac_db_path", ""),
                "fldigi_checkin_dir": data.get("fldigi_checkin_dir", ""),
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
            self.settings.set("ui_theme", data.get("ui_theme", "light"))
            self.settings.set("freq_enforcement_mode", data.get("freq_enforcement_mode", "On Schedule Change"))
            self.settings.set("freq_prompt_interval", data.get("freq_prompt_interval", "Hourly"))
            self.settings.set("fldigi_enforcement_mode", data.get("fldigi_enforcement_mode", "On Schedule Change"))
            self.settings.set("fldigi_prompt_interval", data.get("fldigi_prompt_interval", "Hourly"))
            self.settings.set("js8_enforcement_mode", data.get("js8_enforcement_mode", "On Schedule Change"))
            self.settings.set("js8_prompt_interval", data.get("js8_prompt_interval", "Hourly"))
            self.settings.set("js8_port", data["js8_port"])
            self.settings.set("js8_offset_hz", data.get("js8_offset_hz", 0))
            self.settings.set("primary_js8_groups", data["primary_js8_groups"])
            self.settings.set("js8_directed_path", data["js8_directed_path"])
            self.settings.set("js8_forms_path", data.get("js8_forms_path", ""))
            self.settings.set(
                "js8_inbox_mark_retrieved_sync",
                data.get("js8_inbox_mark_retrieved_sync", False),
            )
            self.settings.set("message_paths", data.get("message_paths", {}))
            self.settings.set("varac_path", data.get("varac_path", ""))
            self.settings.set("varac_db_path", data.get("varac_db_path", ""))
            self.settings.set("fldigi_checkin_dir", data.get("fldigi_checkin_dir", ""))
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
        self._ensure_fldigi_checkin_files()
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
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _on_theme_changed(self):
        theme = self.theme_combo.currentText().strip().lower() or "light"
        self._set_loading(True, "Wilco. Standby for Spectrum QSY...")
        QApplication.processEvents()
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("ui_theme", theme)
                if hasattr(self.settings, "save"):
                    self.settings.save()
        except Exception:
            pass
        try:
            self.settings_saved.emit()
        except Exception:
            pass
        self._mark_settings_dirty()
        # apply_theme will clear the toast once the app theme is applied

    def _update_enforcement_visibility(self) -> None:
        freq_prompt = self.freq_enforce_combo.currentText().strip() == "Prompt"
        fldigi_prompt = self.fldigi_enforce_combo.currentText().strip() == "Prompt"
        js8_prompt = self.js8_enforce_combo.currentText().strip() == "Prompt"
        theme = resolve_theme(self.settings)
        muted_style = (
            "QComboBox {"
            f" color: {theme['text_muted']}; background-color: {theme['surface_alt']};"
            f" border: 1px solid {theme['border']};"
            "}"
        )
        warn_style = (
            "QComboBox {"
            f" color: {theme['warning']}; background-color: {theme['surface']};"
            f" border: 1px solid {theme['border']};"
            "}"
        )
        for combo, enabled in (
            (self.freq_prompt_combo, freq_prompt),
            (self.fldigi_prompt_combo, fldigi_prompt),
            (self.js8_prompt_combo, js8_prompt),
        ):
            combo.setEnabled(enabled)
            if not enabled:
                combo.setCurrentText("Select Interval")
                combo.setStyleSheet(muted_style)
                continue
            if combo.currentText().strip() == "Select Interval":
                combo.setStyleSheet(warn_style)
            else:
                combo.setStyleSheet("")

    def _on_enforcement_changed(self):
        self._update_enforcement_visibility()
        self._mark_settings_dirty()

    def _set_loading(self, active: bool, text: str = "Wilco. Standby for Spectrum QSY...") -> None:
        if not self.loading_label:
            return
        self.loading_label.setText(text)
        self.loading_label.setVisible(bool(active))

    def _wire_dirty_tracking(self) -> None:
        edits = [
            self.callsign_edit,
            self.name_edit,
            self.state_edit,
            self.grid6_edit,
            self.js8_port_edit,
            self.js8_offset_edit,
            self.js8_directed_edit,
            self.js8_forms_edit,
            self.fldigi_checkin_dir_edit,
            self.flrig_port_edit,
        ]
        edits.extend(self.msg_paths_edits.values())
        edits.extend(self.path_edits.values())
        for edit in edits:
            edit.textChanged.connect(self._mark_settings_dirty)

        combos = [
            self.control_combo,
            self.theme_combo,
            self.freq_enforce_combo,
            self.freq_prompt_combo,
            self.fldigi_enforce_combo,
            self.fldigi_prompt_combo,
            self.js8_enforce_combo,
            self.js8_prompt_combo,
        ]
        for combo in combos:
            combo.currentIndexChanged.connect(self._mark_settings_dirty)

        checks = [self.use_scheduler_chk, self.js8_mark_retrieved_chk]
        checks.extend(self.radio_checkboxes.values())
        for chk in checks:
            chk.stateChanged.connect(self._mark_settings_dirty)

    def _mark_settings_dirty(self) -> None:
        if self._loading_settings:
            return
        if not self._settings_dirty:
            self._settings_dirty = True
            self._set_save_button_state("info")

    def _set_save_button_state(self, role: str) -> None:
        theme = resolve_theme(self.settings)
        self.save_btn.setStyleSheet(button_style(role, theme))

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
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
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
            # Ensure older DBs have the expected columns.
            cur.execute("PRAGMA table_info(operator_checkins)")
            existing_cols = {row[1] for row in cur.fetchall()}
            required_cols = {
                "first_seen_utc": "TEXT",
                "last_seen_utc": "TEXT",
                "checkin_count": "INTEGER",
                "trusted": "INTEGER",
                "groups_json": "TEXT",
                "group1": "TEXT",
                "group2": "TEXT",
                "group3": "TEXT",
                "grid": "TEXT",
                "state": "TEXT",
                "name": "TEXT",
            }
            for col, col_type in required_cols.items():
                if col not in existing_cols:
                    cur.execute(f"ALTER TABLE operator_checkins ADD COLUMN {col} {col_type}")
            cur.execute("PRAGMA table_info(operator_checkins)")
            existing_cols = {row[1] for row in cur.fetchall()}
            if "first_seen_utc" not in existing_cols:
                log.debug("SettingsTab: operator_checkins missing first_seen_utc after migration")
                conn.close()
                return
            cur.execute(
                """
                INSERT INTO operator_checkins (callsign, name, state, grid, first_seen_utc, last_seen_utc, checkin_count, trusted)
                VALUES (?, ?, ?, ?, strftime('%Y-%m-%d', 'now'), strftime('%Y-%m-%d', 'now'), COALESCE((SELECT checkin_count FROM operator_checkins WHERE callsign=?), 0), COALESCE((SELECT trusted FROM operator_checkins WHERE callsign=?), 0))
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

    def _update_launch_selected_state(self):
        theme = resolve_theme(self.settings)
        any_selected = any(chk.isChecked() for chk in self.radio_checkboxes.values())
        role = "info" if any_selected else "muted"
        self.launch_selected_btn.setStyleSheet(button_style(role, theme))

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
                    second_arg = os.path.basename(cmdline_list[1]).lower() if len(cmdline_list) > 1 else ""
                    for token in (name, exe, first_arg, second_arg):
                        if token:
                            snap.append(token)
                except Exception:
                    continue
            self._proc_snapshot = snap
            self._proc_snapshot_ts = now_ts
        exe_path = self._get_saved_program_path(program_name)
        target_names = {program_name.lower(), f"{program_name.lower()}.exe"}
        if program_name in {"JS8Spotter", "CommStat"}:
            target_names.add(f"{program_name.lower()}.py")
        if exe_path:
            target_names.add(exe_path.name.lower())
        return any(entry in target_names for entry in self._proc_snapshot)

    def _find_process_exe(self, program_name: str) -> Optional[str]:
        target = (program_name or "").strip().lower()
        if not target:
            return None
        target_names = {target, f"{target}.exe"}
        for proc in psutil.process_iter(attrs=["name", "exe", "cmdline"]):
            try:
                name = (proc.info.get("name") or "").lower()
                exe = proc.info.get("exe") or ""
                exe_base = os.path.basename(exe).lower()
                cmdline_list = proc.info.get("cmdline") or []
                first_arg = os.path.basename(cmdline_list[0]).lower() if cmdline_list else ""
                if name in target_names or exe_base in target_names or first_arg in target_names:
                    if exe:
                        return exe
                    if cmdline_list:
                        return cmdline_list[0]
                    return None
            except Exception:
                continue
        return None

    def _refresh_running_status(self):
        theme = resolve_theme(self.settings)
        running_js8 = self._program_is_running("JS8Call")
        api_ok = self._js8_api_reachable()
        # Update API indicator (header)
        api_lbl = self.status_labels.get("JS8Call_API")
        if api_lbl:
            if api_ok:
                api_lbl.setStyleSheet(led_style("ok", theme))
                api_lbl.setToolTip("API reachable")
            elif running_js8:
                api_lbl.setStyleSheet(led_style("warn", theme))
                api_lbl.setToolTip("Process running, API unreachable")
            else:
                api_lbl.setStyleSheet(led_style("idle", theme))
                api_lbl.setToolTip("Not running")

        # Update VarAC status tooltip without overwriting configured install path
        if hasattr(self, "varac_path_edit"):
            varac_running = self._program_is_running("VarAC")
            exe_path = self._find_process_exe("VarAC") if varac_running else None
            if exe_path:
                self.varac_path_edit.setToolTip(f"Running: {exe_path}")
            elif varac_running:
                self.varac_path_edit.setToolTip("Running")
            else:
                self.varac_path_edit.setToolTip("Not running")

        # Update all other indicators
        for program_name, lbl in self.status_labels.items():
            if program_name == "JS8Call_API":
                continue
            running = running_js8 if program_name == "JS8Call" else self._program_is_running(program_name)
            if program_name == "JS8Call":
                if api_ok:
                    lbl.setStyleSheet(led_style("ok", theme))
                    lbl.setToolTip("API reachable")
                elif running:
                    lbl.setStyleSheet(led_style("warn", theme))
                    lbl.setToolTip("Process running, API unreachable")
                else:
                    lbl.setStyleSheet(led_style("idle", theme))
                    lbl.setToolTip("Not running")
            else:
                if running:
                    lbl.setStyleSheet(led_style("ok", theme))
                    lbl.setToolTip("Running")
                else:
                    lbl.setStyleSheet(led_style("idle", theme))
                    lbl.setToolTip("Not Running")

    def apply_theme(self):
        try:
            theme = resolve_theme(self.settings)
            if self.loading_label:
                bg = theme.get("surface_alt", theme.get("surface", "#f2f2f2"))
                fg = theme.get("accent", theme.get("text", "#222"))
                border = theme.get("border", "#ccc")
                self.loading_label.setStyleSheet(
                    f"padding: 2px 6px; border-radius: 4px; background: {bg}; color: {fg}; border: 1px solid {border};"
                )
                self.loading_label.setVisible(False)
            self._refresh_running_status()
            self._update_launch_selected_state()
            self._update_op_group_action_buttons()
            self._set_save_button_state("info" if self._settings_dirty else "success")
        except Exception:
            pass

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

        vfo_combo = QComboBox()
        vfo_combo.addItems(["A", "B"])
        form.addRow("VFO:", vfo_combo)

        fldigi_mode_combo = QComboBox()
        fldigi_mode_combo.setEditable(True)
        fldigi_mode_combo.addItems(FLDIGI_MODE_OPTIONS)
        fldigi_mode_combo.setInsertPolicy(QComboBox.NoInsert)
        completer = QCompleter(FLDIGI_MODE_OPTIONS, fldigi_mode_combo)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        fldigi_mode_combo.setCompleter(completer)
        form.addRow("FLDigi Starting Mode:", fldigi_mode_combo)

        def _sync_fldigi_mode_for_ssb(text: str):
            if (text or "").strip().upper() == "SSB":
                fldigi_mode_combo.setCurrentText("SSB")

        mode_combo.currentTextChanged.connect(_sync_fldigi_mode_for_ssb)
        _sync_fldigi_mode_for_ssb(mode_combo.currentText())

        fldigi_offset_edit = QLineEdit()
        fldigi_offset_edit.setValidator(QIntValidator(0, 99999, fldigi_offset_edit))
        fldigi_offset_edit.setPlaceholderText("e.g., 900")
        form.addRow("FLDigi Starting Offset (Hz):", fldigi_offset_edit)

        auto_tune_chk = QCheckBox("Enable Auto-Tune on QSY")
        form.addRow("", auto_tune_chk)

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
            offset_txt = fldigi_offset_edit.text().strip()
            if offset_txt:
                try:
                    int(offset_txt)
                except Exception:
                    QMessageBox.warning(self, "Validation", "FLDigi Starting Offset must be an integer.")
                    return
            fldigi_mode = fldigi_mode_combo.currentText().strip()
            vfo = vfo_combo.currentText().strip().upper() or "A"
            self._upsert_operating_group(
                name,
                mode,
                band,
                f"{freq_val:.3f}",
                auto_tune=auto_tune_chk.isChecked(),
                vfo=vfo,
                fldigi_mode=fldigi_mode,
                fldigi_offset=offset_txt,
            )
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

    def _upsert_operating_group(
        self,
        name: str,
        mode: str,
        band: str,
        freq_mhz,
        auto_tune: bool = False,
        vfo: str = "A",
        fldigi_mode: str = "",
        fldigi_offset: str = "",
    ):
        # replace existing entry with same group+mode+band
        name = name.strip().upper()
        freq_display = self._format_freq(freq_mhz)
        updated = False
        for g in self.operating_groups:
            if g.get("group") == name and g.get("mode") == mode and g.get("band") == band:
                g["frequency"] = freq_display
                g["auto_tune"] = bool(auto_tune)
                g["vfo"] = vfo
                g["fldigi_mode"] = fldigi_mode
                g["fldigi_offset"] = fldigi_offset
                updated = True
                break
        if not updated:
            self.operating_groups.append(
                {
                    "group": name,
                    "mode": mode,
                    "band": band,
                    "frequency": freq_display,
                    "vfo": vfo,
                    "fldigi_mode": fldigi_mode,
                    "fldigi_offset": fldigi_offset,
                    "auto_tune": bool(auto_tune),
                }
            )
        self._refresh_operating_groups_table()
        # Persist immediately so additions survive app restarts without requiring an explicit Save click.
        try:
            self._save_settings_quiet()
            self._settings_dirty = False
            self._set_save_button_state("success")
            try:
                self.settings_saved.emit()
            except Exception:
                pass
        except Exception:
            log.exception("Failed to persist Operating Group; will remain in-memory only.")

    def _refresh_operating_groups_table(self):
        # Sort display by Group asc, then Band asc
        self.operating_groups = sorted(
            [
                {
                    "group": str(g.get("group", "")).upper(),
                    "mode": g.get("mode", ""),
                    "band": g.get("band", ""),
                    "frequency": g.get("frequency", ""),
                    "vfo": (g.get("vfo") or "A").strip().upper() or "A",
                    "fldigi_mode": (g.get("fldigi_mode") or "").strip(),
                    "fldigi_offset": (g.get("fldigi_offset") or "").strip(),
                    "auto_tune": bool(g.get("auto_tune", False)),
                }
                for g in self.operating_groups
            ],
            key=lambda g: (str(g.get("group", "")).lower(), str(g.get("band", "")).lower()),
        )

        table = self.op_groups_table
        table.setRowCount(0)
        for g in self.operating_groups:
            row = table.rowCount()
            table.insertRow(row)
            sel_chk = QCheckBox()
            sel_chk.setFixedWidth(22)
            sel_chk.stateChanged.connect(self._update_op_group_action_buttons)
            sel_wrap = QWidget()
            sel_layout = QHBoxLayout(sel_wrap)
            sel_layout.setContentsMargins(0, 0, 0, 0)
            sel_layout.setAlignment(Qt.AlignCenter)
            sel_layout.addWidget(sel_chk)
            table.setCellWidget(row, 0, sel_wrap)
            table.setItem(row, 1, QTableWidgetItem(str(g.get("group", "")).upper()))
            table.setItem(row, 2, QTableWidgetItem(str(g.get("mode", ""))))
            table.setItem(row, 3, QTableWidgetItem(str(g.get("band", ""))))
            table.setItem(row, 4, QTableWidgetItem(self._format_freq(g.get("frequency", ""))))
            table.setItem(row, 5, QTableWidgetItem(str(g.get("vfo", "A")).upper()))
            table.setItem(row, 6, QTableWidgetItem(str(g.get("fldigi_mode", ""))))
            table.setItem(row, 7, QTableWidgetItem(str(g.get("fldigi_offset", ""))))
            auto_chk = QCheckBox()
            auto_chk.setChecked(bool(g.get("auto_tune", False)))
            auto_chk.setFixedSize(20, 20)
            auto_wrap = QWidget()
            auto_wrap.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            auto_layout = QHBoxLayout(auto_wrap)
            auto_layout.setContentsMargins(6, 0, 6, 0)
            auto_layout.setAlignment(Qt.AlignCenter)
            auto_layout.addWidget(auto_chk)
            table.setCellWidget(row, 8, auto_wrap)
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeToContents)
        self._update_op_group_action_buttons()
        self._refresh_section_titles()

    def _update_op_group_action_buttons(self):
        theme = resolve_theme(self.settings)
        has_selection = bool(self._selected_op_rows())
        role = "info" if has_selection else "muted"
        self.edit_group_btn.setEnabled(True)
        self.delete_group_btn.setEnabled(True)
        self.edit_group_btn.setStyleSheet(button_style(role, theme))
        self.delete_group_btn.setStyleSheet(button_style(role, theme))

    def _table_to_operating_groups(self) -> List[Dict[str, str]]:
        result: List[Dict[str, str]] = []
        for r in range(self.op_groups_table.rowCount()):
            group = (
                self.op_groups_table.item(r, 1).text().strip().upper() if self.op_groups_table.item(r, 1) else ""
            )
            mode = self.op_groups_table.item(r, 2).text().strip() if self.op_groups_table.item(r, 2) else ""
            band = self.op_groups_table.item(r, 3).text().strip() if self.op_groups_table.item(r, 3) else ""
            freq_txt = self.op_groups_table.item(r, 4).text().strip() if self.op_groups_table.item(r, 4) else ""
            vfo_txt = self.op_groups_table.item(r, 5).text().strip() if self.op_groups_table.item(r, 5) else "A"
            fldigi_mode = (
                self.op_groups_table.item(r, 6).text().strip() if self.op_groups_table.item(r, 6) else ""
            )
            fldigi_offset = (
                self.op_groups_table.item(r, 7).text().strip() if self.op_groups_table.item(r, 7) else ""
            )
            auto_widget = self.op_groups_table.cellWidget(r, 8)
            auto_tune = False
            if isinstance(auto_widget, QCheckBox):
                auto_tune = auto_widget.isChecked()
            elif isinstance(auto_widget, QWidget):
                chk = auto_widget.findChild(QCheckBox)
                if chk is not None:
                    auto_tune = chk.isChecked()
            try:
                freq_val = float(freq_txt)
            except Exception:
                freq_val = None
            if group and mode and band and freq_val is not None:
                vfo_val = (vfo_txt or "A").strip().upper()
                if vfo_val not in ("A", "B"):
                    vfo_val = "A"
                result.append(
                    {
                        "group": group,
                        "mode": mode,
                        "band": band,
                        "frequency": self._format_freq(freq_val),
                        "vfo": vfo_val,
                        "fldigi_mode": fldigi_mode,
                        "fldigi_offset": fldigi_offset,
                        "auto_tune": auto_tune,
                    }
                )
        return result

    def _selected_op_rows(self) -> List[int]:
        rows: List[int] = []
        for r in range(self.op_groups_table.rowCount()):
            w = self.op_groups_table.cellWidget(r, 0)
            if isinstance(w, QCheckBox) and w.isChecked():
                rows.append(r)
            elif isinstance(w, QWidget):
                chk = w.findChild(QCheckBox)
                if chk is not None and chk.isChecked():
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
        vfo_txt = self.op_groups_table.item(row, 5).text().strip() if self.op_groups_table.item(row, 5) else "A"
        fldigi_mode_txt = (
            self.op_groups_table.item(row, 6).text().strip() if self.op_groups_table.item(row, 6) else ""
        )
        fldigi_offset_txt = (
            self.op_groups_table.item(row, 7).text().strip() if self.op_groups_table.item(row, 7) else ""
        )
        auto_widget = self.op_groups_table.cellWidget(row, 8)
        auto_val = False
        if isinstance(auto_widget, QCheckBox):
            auto_val = auto_widget.isChecked()
        elif isinstance(auto_widget, QWidget):
            chk = auto_widget.findChild(QCheckBox)
            if chk is not None:
                auto_val = chk.isChecked()

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

        vfo_combo = QComboBox()
        vfo_combo.addItems(["A", "B"])
        vfo_val = vfo_txt.strip().upper()
        if vfo_val in ("A", "B"):
            vfo_combo.setCurrentText(vfo_val)
        form.addRow("VFO:", vfo_combo)

        fldigi_mode_combo = QComboBox()
        fldigi_mode_combo.setEditable(True)
        fldigi_mode_combo.addItems(FLDIGI_MODE_OPTIONS)
        fldigi_mode_combo.setInsertPolicy(QComboBox.NoInsert)
        completer = QCompleter(FLDIGI_MODE_OPTIONS, fldigi_mode_combo)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        fldigi_mode_combo.setCompleter(completer)
        if fldigi_mode_txt:
            fldigi_mode_combo.setCurrentText(fldigi_mode_txt)
        form.addRow("FLDigi Starting Mode:", fldigi_mode_combo)

        def _sync_fldigi_mode_for_ssb(text: str):
            if (text or "").strip().upper() == "SSB":
                fldigi_mode_combo.setCurrentText("SSB")

        mode_combo.currentTextChanged.connect(_sync_fldigi_mode_for_ssb)
        _sync_fldigi_mode_for_ssb(mode_combo.currentText())

        fldigi_offset_edit = QLineEdit(fldigi_offset_txt)
        fldigi_offset_edit.setValidator(QIntValidator(0, 99999, fldigi_offset_edit))
        fldigi_offset_edit.setPlaceholderText("e.g., 900")
        form.addRow("FLDigi Starting Offset (Hz):", fldigi_offset_edit)

        auto_tune_chk = QCheckBox("Enable Auto-Tune on QSY")
        auto_tune_chk.setChecked(auto_val)
        form.addRow("", auto_tune_chk)

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
            offset_txt = fldigi_offset_edit.text().strip()
            if offset_txt:
                try:
                    int(offset_txt)
                except Exception:
                    QMessageBox.warning(self, "Validation", "FLDigi Starting Offset must be an integer.")
                    return
            fldigi_mode = fldigi_mode_combo.currentText().strip()
            vfo = vfo_combo.currentText().strip().upper() or "A"
            # Remove old entry, then insert updated
            self.operating_groups = [
                g
                for g in self.operating_groups
                if not (g.get("group") == group and g.get("mode") == mode and g.get("band") == band)
            ]
            self._upsert_operating_group(
                new_name,
                new_mode,
                new_band,
                new_freq_txt,
                auto_tune=auto_tune_chk.isChecked(),
                vfo=vfo,
                fldigi_mode=fldigi_mode,
                fldigi_offset=offset_txt,
            )
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
            self._settings_dirty = False
            self._set_save_button_state("success")
            try:
                self.settings_saved.emit()
            except Exception:
                pass
        except Exception:
            log.exception("Failed to persist Operating Group deletions; will remain in-memory only.")
        QMessageBox.information(self, "Delete Groups", f"Deleted {len(to_remove)} Operating Group(s).")

    # ---------- JS8 DIRECTED PATH ---------- #

    def _choose_js8_directed_path(self):
        fn, _ = QFileDialog.getOpenFileName(
            self,
            "Select JS8Call DIRECTED.TXT",
            "",
            "All Files (*);;Text Files (*.txt)",
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
        self._settings_dirty = False
        self._set_save_button_state("success")

    # ---------- JS8 FORMS PATH ---------- #

    def _choose_js8_forms_path(self):
        """
        Prompt for JS8Spotter forms folder (MCF###.txt files).
        """
        fn = QFileDialog.getExistingDirectory(
            self,
            "Select JS8Spotter forms folder",
            "",
        )
        if not fn:
            return
        self.js8_forms_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("js8_forms_path", fn)
        else:
            data = self.settings.all()
            data["js8_forms_path"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        log.info("JS8Spotter forms path saved: %s", fn)
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_msg_path(self, origin: str, edit: QLineEdit):
        """
        Prompt for message paths used by Message Viewer (VarAC/FLMSG/FLAMP).
        """
        fn = QFileDialog.getExistingDirectory(self, f"Select {origin.upper()} folder")
        if not fn:
            return
        edit.setText(fn)
        data = self.settings.all() if hasattr(self.settings, "all") else {}
        if isinstance(data, dict):
            mp = data.get("message_paths", {}) or {}
            mp[origin] = fn
            data["message_paths"] = mp
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        if hasattr(self.settings, "set"):
            mp = self.settings.get("message_paths", {}) or {}
            mp[origin] = fn
            self.settings.set("message_paths", mp)
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_varac_install_path(self):
        """
        Prompt for VarAC install folder path.
        """
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC install folder")
        if not fn:
            return
        if hasattr(self, "varac_path_edit"):
            self.varac_path_edit.setText(fn)
        data = self.settings.all() if hasattr(self.settings, "all") else {}
        if isinstance(data, dict):
            data["varac_path"] = fn
            data["varac_db_path"] = str(Path(fn) / "VarAC.db")
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        if hasattr(self.settings, "set"):
            self.settings.set("varac_path", fn)
            self.settings.set("varac_db_path", str(Path(fn) / "VarAC.db"))
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_fldigi_checkin_dir(self):
        fn = QFileDialog.getExistingDirectory(self, "Select FLDigi check-in folder")
        if not fn:
            return
        self.fldigi_checkin_dir_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("fldigi_checkin_dir", fn)
        else:
            data = self.settings.all()
            data["fldigi_checkin_dir"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._ensure_fldigi_checkin_files()

    def _copy_text(self, edit: QLineEdit) -> None:
        txt = edit.text().strip()
        if not txt:
            return
        cb = QApplication.clipboard()
        cb.setText(txt)
        QMessageBox.information(self, "Copied", f"Copied to clipboard:\n{txt}")
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _refresh_fldigi_checkin_file_labels(self) -> None:
        base = self.fldigi_checkin_dir_edit.text().strip()
        if not base:
            base = str(get_fldigi_checkin_dir())
        main_path = str(Path(base) / "main_checkins.txt")
        late_path = str(Path(base) / "new-late_checkins.txt")
        if hasattr(self, "fldigi_main_file_edit"):
            self.fldigi_main_file_edit.setText(main_path)
        if hasattr(self, "fldigi_late_file_edit"):
            self.fldigi_late_file_edit.setText(late_path)

    def _ensure_fldigi_checkin_files(self) -> None:
        base = self.fldigi_checkin_dir_edit.text().strip()
        if not base:
            base = str(get_fldigi_checkin_dir())
            self.fldigi_checkin_dir_edit.setText(base)
        folder = Path(base)
        main_path = folder / "main_checkins.txt"
        late_path = folder / "new-late_checkins.txt"
        try:
            folder.mkdir(parents=True, exist_ok=True)
            if not main_path.exists():
                main_path.touch()
            if not late_path.exists():
                late_path.touch()
        except Exception as e:
            log.error("SettingsTab: failed to ensure FLDigi check-in files: %s", e)
        self._refresh_fldigi_checkin_file_labels()

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
        from freqinout.core.config_paths import get_config_dir

        db_path = get_config_dir() / "config" / "freqinout_nets.db"
        try:
            indexer = JS8LogLinkIndexer(self.settings, db_path)
            indexer._base_callsign = JS8LogLinkIndexer._base_callsign  # ensure suffix handling
            self._maybe_backfill_js8_geo()
            indexer.update()
            QMessageBox.information(self, "JS8 Traffic Loaded", "JS8 logs ingested successfully.")
            self._refresh_operator_history_views()
        except Exception as e:
            log.error("SettingsTab: JS8 log ingest failed: %s", e)
            QMessageBox.critical(self, "Error", f"Failed to ingest JS8 logs:\n{e}")
            self._refresh_operator_history_views()

    def _maybe_backfill_js8_geo(self) -> None:
        if self._loading_settings:
            return
        if self.settings.get("js8_geo_backfill_v1_done", False):
            return
        directed_path = (self.js8_directed_edit.text().strip() or self.settings.get("js8_directed_path", "") or "")
        if not directed_path:
            return
        path = Path(directed_path)
        if not path.exists():
            return
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            indexer = JS8LogLinkIndexer(self.settings, db_path)
            indexer._base_callsign = JS8LogLinkIndexer._base_callsign  # ensure suffix handling
            scanned = indexer.backfill_geo_from_logs()
            self.settings.set("js8_geo_backfill_v1_done", True)
            log.info("SettingsTab: JS8 geo backfill complete (lines=%s).", scanned)
        except Exception as e:
            log.debug("SettingsTab: JS8 geo backfill failed: %s", e)
