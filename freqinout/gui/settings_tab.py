from __future__ import annotations

import datetime
import platform
import subprocess
import sqlite3
import os
import sys
import time
import zipfile
import re
from pathlib import Path
from typing import Dict, Optional, List

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QIntValidator
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
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
    QDialogButtonBox,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QHeaderView,
    QSizePolicy,
    QAbstractScrollArea,
    QCompleter,
    QToolButton,
    QListWidget,
    QListWidgetItem,
    QStackedWidget,
)

from freqinout.core.logger import log, set_log_level, get_log_level, _get_log_file
from freqinout.core.perf_metrics import emit_span, span as perf_span
from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.config_paths import get_fldigi_checkin_dir, get_config_dir
from freqinout.core.launch_orchestrator import LaunchOrchestrator, LAUNCH_APP_ORDER
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.gpg_tools import (
    gpg_available,
    import_public_key_file,
    import_public_key_text,
    list_public_keys,
    local_sign_key,
    normalize_fingerprint,
)
from freqinout.core.hash_tools import (
    infer_algorithm_from_hash,
    normalize_hash_algorithm,
    normalize_hash_hex,
    normalize_trusted_hash_entries,
)
from freqinout.utils.timezones import get_timezone
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer
from freqinout.gui.theme import resolve_theme, led_style, button_style
from freqinout.version import __version__


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

LOCAL_NET_SERVICE_OPTIONS = [
    "VHF Simplex",
    "VHF Repeater",
    "UHF Simplex",
    "UHF Repeater",
    "GMRS Simplex",
    "GMRS Repeater",
    "FRS",
    "MURS",
    "Meshtastic",
    "Other",
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
    local_net_profiles_changed = Signal()
    open_logs_requested = Signal()
    log_level_changed = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._settings_dirty = False
        self._loading_settings = False
        self.loading_label: QLabel | None = None
        self._status_service = SoftwareStatusService(self.settings)
        self.launch_orchestrator = LaunchOrchestrator(self.settings, self)

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
        self.local_net_profiles: List[Dict[str, str]] = []
        self._accordion_groups: List[QGroupBox] = []
        self._section_meta: Dict[QGroupBox, Dict[str, object]] = {}
        self._section_nav_items: Dict[QGroupBox, QListWidgetItem] = {}
        self._launch_items_cache: List[Dict[str, object]] = []
        self._launch_visible_names: List[str] = []
        self._launch_table_loading = False
        self._gpg_keys_table_loading = False
        self._gpg_trusted_fingerprints: set[str] = set()
        self._trusted_hashes_table_loading = False
        self._trusted_hash_entries: List[Dict[str, object]] = []
        self._active = False
        self._last_activation_refresh_ts = 0.0
        self._activation_refresh_interval_sec = 30.0

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
        self.status_timer.setInterval(5000)
        self.status_timer.timeout.connect(self._refresh_running_status)

        self._update_clock_labels()
        QTimer.singleShot(0, self._maybe_backfill_js8_geo)

    def set_tab_active(self, active: bool) -> None:
        self._active = bool(active)
        if self._active:
            if not self.status_timer.isActive():
                self.status_timer.start()
            QTimer.singleShot(0, self.on_tab_activated)
            return
        if self.status_timer.isActive():
            self.status_timer.stop()

    def on_tab_activated(self) -> None:
        with perf_span("settings.on_tab_activated", settings=self.settings, min_ms=10.0):
            now_ts = time.time()
            if (now_ts - float(self._last_activation_refresh_ts or 0.0)) < float(
                self._activation_refresh_interval_sec
            ):
                return
            self._last_activation_refresh_ts = now_ts
            self._refresh_running_status()

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

        # Operator Information
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
        callsign_container = QWidget()
        callsign_container.setLayout(callsign_layout)
        callsign_group = QGroupBox("Operator Information")
        callsign_group_layout = QVBoxLayout()
        callsign_group_layout.setContentsMargins(10, 10, 10, 12)
        callsign_group_layout.setSpacing(6)
        callsign_group_layout.addWidget(callsign_container)
        callsign_group.setLayout(callsign_group_layout)
        callsign_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        main_layout.addWidget(callsign_group)

        # FreqInOut settings
        op_layout = QVBoxLayout()

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

        enforcement_choices = ["On Schedule Change", "Prompt"]
        prompt_choices = [
            "Select Interval",
            "Hourly",
            "Every 5 minutes",
            "Every 10 minutes",
            "Every 15 minutes",
            "Every 30 minutes",
        ]

        left_column_layout = QVBoxLayout()
        left_column_layout.setSpacing(6)
        left_column_layout.addLayout(ctrl_row)

        freq_row = QHBoxLayout()
        self.freq_timer_label = QLabel("Frequency Timer:")
        freq_row.addWidget(self.freq_timer_label)
        self.freq_enforce_combo = QComboBox()
        self.freq_enforce_combo.addItems(enforcement_choices)
        self.freq_enforce_combo.setMinimumWidth(150)
        self.freq_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        freq_row.addWidget(self.freq_enforce_combo)
        freq_row.addSpacing(12)
        self.freq_prompt_label = QLabel("Prompt Interval:")
        freq_row.addWidget(self.freq_prompt_label)
        self.freq_prompt_combo = QComboBox()
        self.freq_prompt_combo.addItems(prompt_choices)
        self.freq_prompt_combo.setMinimumWidth(170)
        self.freq_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.freq_prompt_combo)
        freq_row.addWidget(self.freq_prompt_combo)
        freq_row.addStretch()
        left_column_layout.addLayout(freq_row)

        fldigi_row = QHBoxLayout()
        self.fldigi_timer_label = QLabel("FLDigi Mode Timer:")
        fldigi_row.addWidget(self.fldigi_timer_label)
        self.fldigi_enforce_combo = QComboBox()
        self.fldigi_enforce_combo.addItems(enforcement_choices)
        self.fldigi_enforce_combo.setMinimumWidth(150)
        self.fldigi_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        fldigi_row.addWidget(self.fldigi_enforce_combo)
        fldigi_row.addSpacing(12)
        self.fldigi_prompt_label = QLabel("Prompt Interval:")
        fldigi_row.addWidget(self.fldigi_prompt_label)
        self.fldigi_prompt_combo = QComboBox()
        self.fldigi_prompt_combo.addItems(prompt_choices)
        self.fldigi_prompt_combo.setMinimumWidth(170)
        self.fldigi_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.fldigi_prompt_combo)
        fldigi_row.addWidget(self.fldigi_prompt_combo)
        fldigi_row.addStretch()
        left_column_layout.addLayout(fldigi_row)

        js8_row = QHBoxLayout()
        self.js8_timer_label = QLabel("JS8 Offset Timer:")
        js8_row.addWidget(self.js8_timer_label)
        self.js8_enforce_combo = QComboBox()
        self.js8_enforce_combo.addItems(enforcement_choices)
        self.js8_enforce_combo.setMinimumWidth(150)
        self.js8_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        js8_row.addWidget(self.js8_enforce_combo)
        js8_row.addSpacing(12)
        self.js8_prompt_label = QLabel("Prompt Interval:")
        js8_row.addWidget(self.js8_prompt_label)
        self.js8_prompt_combo = QComboBox()
        self.js8_prompt_combo.addItems(prompt_choices)
        self.js8_prompt_combo.setMinimumWidth(170)
        self.js8_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.js8_prompt_combo)
        js8_row.addWidget(self.js8_prompt_combo)
        js8_row.addStretch()
        left_column_layout.addLayout(js8_row)
        left_column_layout.addStretch()

        log_warn_tip = (
            "Logging may reduce performance and increase disk usage. "
            "Enable INFO/DEBUG only while troubleshooting."
        )
        self.logging_group = QWidget()
        self.logging_group.setToolTip(log_warn_tip)
        logging_group_layout = QVBoxLayout()
        logging_group_layout.setContentsMargins(8, 8, 8, 8)
        logging_group_layout.setSpacing(6)

        self.logging_warning_label = QLabel(
            "Verbose logging can increase disk I/O and reduce performance."
        )
        self.logging_warning_label.setWordWrap(True)
        self.logging_warning_label.setToolTip(log_warn_tip)
        logging_group_layout.addWidget(self.logging_warning_label)

        level_row = QHBoxLayout()
        level_row.addWidget(QLabel("Logging Level:"))
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"])
        self.log_level_combo.setToolTip(log_warn_tip)
        self.log_level_combo.currentTextChanged.connect(self._on_log_level_changed)
        level_row.addWidget(self.log_level_combo)
        level_row.addStretch()
        logging_group_layout.addLayout(level_row)

        timed_row = QHBoxLayout()
        self.enable_timed_debug_btn = QPushButton("Enable DEBUG For")
        self.enable_timed_debug_btn.setToolTip(log_warn_tip)
        self.enable_timed_debug_btn.clicked.connect(self._enable_timed_debug)
        timed_row.addWidget(self.enable_timed_debug_btn)

        self.debug_duration_combo = QComboBox()
        self.debug_duration_combo.addItem("15 min", 15)
        self.debug_duration_combo.addItem("30 min", 30)
        self.debug_duration_combo.addItem("60 min", 60)
        self.debug_duration_combo.setCurrentIndex(1)
        self.debug_duration_combo.setToolTip("Automatically reverts to previous logging level when timer expires.")
        timed_row.addWidget(self.debug_duration_combo)
        timed_row.addStretch()
        logging_group_layout.addLayout(timed_row)

        self.logging_actions_grid = QGridLayout()
        self.logging_actions_grid.setHorizontalSpacing(8)
        self.logging_actions_grid.setVerticalSpacing(6)

        self.open_logs_btn = QPushButton("Open Logs")
        self.open_logs_btn.setToolTip(log_warn_tip)
        self.open_logs_btn.clicked.connect(self._request_open_logs)
        self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0)

        self.open_log_folder_btn = QPushButton("Open Log Folder")
        self.open_log_folder_btn.setToolTip(log_warn_tip)
        self.open_log_folder_btn.clicked.connect(self._open_log_folder)
        self.logging_actions_grid.addWidget(self.open_log_folder_btn, 0, 1)

        self.export_diag_btn = QPushButton("Export Diagnostics")
        self.export_diag_btn.setToolTip(log_warn_tip)
        self.export_diag_btn.clicked.connect(self._export_diagnostics)
        self.logging_actions_grid.addWidget(self.export_diag_btn, 0, 2)
        self.logging_actions_grid.setColumnStretch(3, 1)
        logging_group_layout.addLayout(self.logging_actions_grid)

        self.logging_group.setLayout(logging_group_layout)

        left_widget = QWidget()
        left_widget.setLayout(left_column_layout)
        left_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        op_layout.addWidget(left_widget)

        self._align_enforcement_labels()
        self._update_logging_actions_layout()

        # Operating status indicators
        status_layout = QHBoxLayout()

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
        status_container = QWidget()
        status_container.setLayout(status_layout)
        status_group = QGroupBox("Operating Status")
        status_group_layout = QVBoxLayout()
        status_group_layout.setContentsMargins(10, 10, 10, 12)
        status_group_layout.setSpacing(6)
        status_group_layout.addWidget(status_container)
        status_group.setLayout(status_group_layout)
        status_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        main_layout.addWidget(status_group)

        sections_row = QHBoxLayout()
        sections_row.setSpacing(10)
        self.sections_nav_list = QListWidget()
        self.sections_nav_list.setMinimumWidth(170)
        self.sections_nav_list.setMaximumWidth(230)
        self.sections_nav_list.setSelectionMode(QListWidget.SingleSelection)
        self.sections_nav_list.setUniformItemSizes(True)
        self.sections_nav_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.sections_nav_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.sections_nav_list.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.sections_nav_list.currentRowChanged.connect(self._on_section_nav_changed)
        sections_row.addWidget(self.sections_nav_list, 0, Qt.AlignTop)

        self.sections_stack = QStackedWidget()
        sections_row.addWidget(self.sections_stack, 1)
        main_layout.addLayout(sections_row, 1)

        op_container = QWidget()
        op_container.setLayout(op_layout)
        op_group = self._make_collapsible_group(
            "FreqInOut Settings",
            op_container,
            checked=True,
            fit_content=True,
        )
        self._register_collapsible_group(op_group, self._summary_freqinout_settings)
        op_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(op_group)

        logging_container = QWidget()
        logging_container_layout = QVBoxLayout()
        logging_container_layout.setContentsMargins(0, 0, 0, 0)
        logging_container_layout.setSpacing(0)
        logging_container_layout.addWidget(self.logging_group)
        logging_container_layout.addStretch()
        logging_container.setLayout(logging_container_layout)
        logging_section = self._make_collapsible_group(
            "Logging & Diagnostics",
            logging_container,
            checked=True,
            fit_content=True,
        )
        self._register_collapsible_group(logging_section, self._summary_logging_settings)
        logging_section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # HF Operating Groups panel
        ops_group = QGroupBox("HF Operating Groups")
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
        self.op_groups_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.op_groups_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.op_groups_table.setEditTriggers(QTableWidget.NoEditTriggers)
        ops_layout.addWidget(self.op_groups_table)
        ops_container = QWidget()
        ops_container.setLayout(ops_layout)
        ops_group = self._make_collapsible_group("HF Operating Groups", ops_container, checked=True, fit_content=False)
        self._register_collapsible_group(ops_group, self._summary_operating_groups)
        ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(ops_group)

        # Local Net Profiles panel (non-scheduler local net metadata for SOP workflows)
        local_group = QGroupBox("Local Net Profiles")
        local_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        local_layout = QVBoxLayout()
        local_layout.setSpacing(6)
        local_group.setLayout(local_layout)
        local_hint = QLabel("Used by SOP local-net reminders only. Not used by scheduler automation.")
        local_hint.setWordWrap(True)
        local_layout.addWidget(local_hint)
        local_row = QHBoxLayout()
        self.add_local_net_btn = QPushButton("Add Profile")
        self.add_local_net_btn.clicked.connect(self._add_local_net_profile)
        self.edit_local_net_btn = QPushButton("Edit Selected")
        self.edit_local_net_btn.clicked.connect(self._edit_local_net_profile)
        self.delete_local_net_btn = QPushButton("Delete Selected")
        self.delete_local_net_btn.clicked.connect(self._delete_local_net_profiles)
        local_row.addStretch()
        local_row.addWidget(self.add_local_net_btn)
        local_row.addWidget(self.edit_local_net_btn)
        local_row.addWidget(self.delete_local_net_btn)
        local_layout.addLayout(local_row)
        self.local_net_table = QTableWidget(0, 6)
        self.local_net_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Name",
                "Service",
                "Mode",
                "Target",
                "Notes",
            ]
        )
        local_header = self.local_net_table.horizontalHeader()
        local_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(5, QHeaderView.Stretch)
        self.local_net_table.verticalHeader().setVisible(False)
        self.local_net_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.local_net_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.local_net_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.local_net_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        local_layout.addWidget(self.local_net_table)
        local_container = QWidget()
        local_container.setLayout(local_layout)
        local_group = self._make_collapsible_group("Local Net Profiles", local_container, checked=True, fit_content=False)
        self._register_collapsible_group(local_group, self._summary_local_net_profiles)
        local_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(local_group)

        # JS8Call status/settings
        js8_group = QGroupBox("JS8Call Settings")
        js8_v = QVBoxLayout()
        js8_v.setSpacing(6)
        js8_v.setAlignment(Qt.AlignTop)
        js8_group.setLayout(js8_v)
        js8_label_width = 170

        js8_port_row = QHBoxLayout()
        js8_port_row.setSpacing(8)
        js8_port_row.setContentsMargins(0, 0, 0, 0)
        js8_port_label = QLabel("TCP Port")
        js8_port_label.setFixedWidth(70)
        js8_port_row.addWidget(js8_port_label)
        self.js8_port_edit = QLineEdit()
        self.js8_port_edit.setFixedWidth(80)
        self.js8_port_edit.setText("2442")
        js8_port_row.addWidget(self.js8_port_edit)
        js8_port_row.addSpacing(8)
        js8_offset_label = QLabel("Offset (Hz)")
        js8_offset_label.setFixedWidth(78)
        js8_port_row.addWidget(js8_offset_label)
        self.js8_offset_edit = QLineEdit()
        self.js8_offset_edit.setFixedWidth(80)
        self.js8_offset_edit.setText("0")
        js8_port_row.addWidget(self.js8_offset_edit)
        js8_port_row.addSpacing(8)
        js8_mark_label = QLabel("Mark JS8Call MSG Read?")
        js8_mark_label.setFixedWidth(165)
        js8_port_row.addWidget(js8_mark_label)
        self.js8_mark_retrieved_chk = QCheckBox()
        self.js8_mark_retrieved_chk.setToolTip(
            "When enabled, clicking 'Mark Retrieved' in Message Viewer will set JS8Call inbox entries to READ."
        )
        js8_port_row.addWidget(self.js8_mark_retrieved_chk)
        js8_port_row.addStretch()
        js8_v.addLayout(js8_port_row)

        def build_js8_path_row(label: str, edit: QLineEdit, browse_cb) -> QWidget:
            row = QHBoxLayout()
            row.setSpacing(8)
            row.setContentsMargins(0, 0, 0, 0)
            lbl = QLabel(label)
            lbl.setFixedWidth(js8_label_width)
            row.addWidget(lbl)
            row.addWidget(edit, 1)
            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(browse_cb)
            row.addWidget(browse_btn)
            w = QWidget()
            w.setLayout(row)
            w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            return w

        directed_forms_row = QHBoxLayout()
        self.js8_directed_edit = QLineEdit()
        js8_v.addWidget(
            build_js8_path_row("JS8Call DIRECTED.TXT:", self.js8_directed_edit, self._choose_js8_directed_path)
        )

        forms_row = QHBoxLayout()
        self.js8_forms_edit = QLineEdit()
        js8_v.addWidget(
            build_js8_path_row("JS8Spotter forms:", self.js8_forms_edit, self._choose_js8_forms_path)
        )

        js8_exec_row = QHBoxLayout()
        self.js8call_path_edit = QLineEdit()
        self.js8call_path_edit.setPlaceholderText("Folder containing JS8Call")
        js8_v.addWidget(
            build_js8_path_row("JS8Call Install Folder:", self.js8call_path_edit, self._choose_js8call_install_path)
        )

        js8spotter_exec_row = QHBoxLayout()
        self.js8spotter_path_edit = QLineEdit()
        self.js8spotter_path_edit.setPlaceholderText("Executable/script/.desktop path")
        js8_v.addWidget(
            build_js8_path_row(
                "JS8Spotter Launch Path:",
                self.js8spotter_path_edit,
                self._choose_js8spotter_launch_path,
            )
        )

        commstat_exec_row = QHBoxLayout()
        self.commstat_path_edit = QLineEdit()
        self.commstat_path_edit.setPlaceholderText("Executable/script/.desktop path")
        js8_v.addWidget(
            build_js8_path_row("CommStat Launch Path:", self.commstat_path_edit, self._choose_commstat_launch_path)
        )

        self.js8_directed_edit.textChanged.connect(self._refresh_section_titles)
        self.js8_forms_edit.textChanged.connect(self._refresh_section_titles)
        self.js8call_path_edit.textChanged.connect(self._refresh_section_titles)
        self.js8spotter_path_edit.textChanged.connect(self._refresh_section_titles)
        self.commstat_path_edit.textChanged.connect(self._refresh_section_titles)
        self.js8call_path_edit.textChanged.connect(self._on_launch_paths_changed)
        self.js8spotter_path_edit.textChanged.connect(self._on_launch_paths_changed)
        self.commstat_path_edit.textChanged.connect(self._on_launch_paths_changed)

        load_links_row = QHBoxLayout()
        load_links_row.setSpacing(8)
        load_links_row.setContentsMargins(0, 0, 0, 0)
        load_links_label = QLabel("Tools")
        load_links_label.setFixedWidth(js8_label_width)
        load_links_row.addWidget(load_links_label)
        self.load_js8_btn = QPushButton("Load JS8 Traffic")
        self.load_js8_btn.clicked.connect(self._load_js8_logs)
        load_links_row.addWidget(self.load_js8_btn)
        load_links_row.addStretch()
        js8_v.addLayout(load_links_row)

        js8_container = QWidget()
        js8_container.setLayout(js8_v)
        js8_group = self._make_collapsible_group("JS8Call Settings", js8_container, checked=True, fit_content=True)
        self._register_collapsible_group(js8_group, self._summary_js8_settings)
        js8_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(js8_group)

        msg_label_width = 170

        def build_prog_row(name: str, label: str | None = None) -> QWidget:
            row = QHBoxLayout()
            row.setSpacing(8)
            row.setContentsMargins(0, 0, 0, 0)
            lbl = QLabel(label or name)
            lbl.setFixedWidth(msg_label_width)
            row.addWidget(lbl)

            path_edit = QLineEdit()
            path_edit.setPlaceholderText("Path to executable")
            self.path_edits[name] = path_edit
            path_edit.textChanged.connect(self._refresh_section_titles)
            path_edit.textChanged.connect(self._on_launch_paths_changed)
            row.addWidget(path_edit, 1)

            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(lambda _, n=name: self._choose_program_path(n))
            row.addWidget(browse_btn)
            w = QWidget()
            w.setLayout(row)
            w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            return w

        def build_msg_row(label: str, edit: QLineEdit, browse_cb) -> QWidget:
            row = QHBoxLayout()
            row.setSpacing(8)
            row.setContentsMargins(0, 0, 0, 0)
            lbl = QLabel(label)
            lbl.setFixedWidth(msg_label_width)
            row.addWidget(lbl)
            row.addWidget(edit, 1)
            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(browse_cb)
            row.addWidget(browse_btn)
            w = QWidget()
            w.setLayout(row)
            w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            return w

        # Message path edits
        self.msg_paths_edits = {}
        flmsg_edit = QLineEdit()
        self.msg_paths_edits["flmsg"] = flmsg_edit
        flamp_edit = QLineEdit()
        self.msg_paths_edits["flamp"] = flamp_edit
        varac_edit = QLineEdit()
        self.msg_paths_edits["varac"] = varac_edit

        # Fast Light Settings
        fast_light_group = QGroupBox("Fast Light Settings")
        fast_light_v = QVBoxLayout()
        fast_light_v.setSpacing(6)
        fast_light_v.setAlignment(Qt.AlignTop)
        fast_light_group.setLayout(fast_light_v)
        fast_light_v.addWidget(build_prog_row("FLRig", "FLRig"))

        flrig_port_row = QHBoxLayout()
        flrig_port_row.setContentsMargins(0, 0, 0, 0)
        flrig_port_row.setSpacing(8)
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
        fast_light_v.addLayout(flrig_port_row)

        fast_light_v.addWidget(build_prog_row("FLDigi", "FLDigi"))
        self.fldigi_checkin_dir_edit = QLineEdit()
        self.fldigi_checkin_dir_edit.setPlaceholderText("Directory containing check-in files")
        self.fldigi_checkin_dir_edit.textChanged.connect(self._refresh_fldigi_checkin_file_labels)
        self.fldigi_main_file_edit = QLineEdit()
        self.fldigi_main_file_edit.setReadOnly(True)
        self.fldigi_main_file_edit.hide()
        self.fldigi_late_file_edit = QLineEdit()
        self.fldigi_late_file_edit.setReadOnly(True)
        self.fldigi_late_file_edit.hide()
        fast_light_v.addWidget(
            build_msg_row("Check-in File Path", self.fldigi_checkin_dir_edit, self._choose_fldigi_checkin_dir)
        )

        self.fldigi_log_path_edit = QLineEdit()
        self.fldigi_log_path_edit.setPlaceholderText("FLDigi log folder")
        fast_light_v.addWidget(
            build_msg_row("FLDigi Log Path", self.fldigi_log_path_edit, self._choose_fldigi_log_path)
        )

        fast_light_v.addWidget(build_prog_row("FLMsg", "FLMsg"))
        fast_light_v.addWidget(
            build_msg_row(
                "ICS/Messages",
                flmsg_edit,
                lambda: self._choose_msg_path("flmsg", flmsg_edit),
            )
        )

        fast_light_v.addWidget(build_prog_row("FLAmp", "FLAmp"))
        fast_light_v.addWidget(
            build_msg_row(
                "FLAMP/rx",
                flamp_edit,
                lambda: self._choose_msg_path("flamp", flamp_edit),
            )
        )

        # Check-in log file copy helpers
        launch_row = QHBoxLayout()
        launch_row.setContentsMargins(0, 0, 0, 0)
        launch_row.setSpacing(8)
        launch_label = QLabel("Check-in Log Paths")
        launch_label.setFixedWidth(msg_label_width)
        launch_row.addWidget(launch_label)
        self.copy_main_btn = QPushButton("Copy Main")
        self.copy_main_btn.clicked.connect(lambda: self._copy_text(self.fldigi_main_file_edit))
        self.copy_late_btn = QPushButton("Copy New/Late")
        self.copy_late_btn.clicked.connect(lambda: self._copy_text(self.fldigi_late_file_edit))
        launch_row.addWidget(self.copy_main_btn)
        launch_row.addWidget(self.copy_late_btn)
        launch_row.addStretch()
        fast_light_v.addLayout(launch_row)

        fast_light_container = QWidget()
        fast_light_container.setLayout(fast_light_v)
        fast_light_group = self._make_collapsible_group(
            "Fast Light Settings",
            fast_light_container,
            checked=True,
            fit_content=True,
        )
        self._register_collapsible_group(fast_light_group, self._summary_fast_light_settings)
        fast_light_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(fast_light_group)

        # Message Authenticity (Key/Hash)
        gpg_group = QGroupBox("Message Auth (Key/Hash)")
        gpg_v = QVBoxLayout()
        gpg_v.setSpacing(6)
        gpg_v.setAlignment(Qt.AlignTop)
        gpg_group.setLayout(gpg_v)

        self.gpg_verify_enabled_chk = QCheckBox("Verify FLAMP .k2s/.b2s signatures")
        self.gpg_verify_enabled_chk.setToolTip(
            "When enabled, Message Viewer verifies detached sidecars and embedded clearsigned content "
            "for configured '-sig' filename patterns."
        )
        gpg_v.addWidget(self.gpg_verify_enabled_chk)

        self.hash_verify_enabled_chk = QCheckBox(
            "Verify FLAMP .k2s/.b2s checksum sidecars (SHA-256/SHA-512 preferred)"
        )
        self.hash_verify_enabled_chk.setToolTip(
            "When enabled, Message Viewer checks checksum sidecar files for tamper/corruption detection."
        )
        gpg_v.addWidget(self.hash_verify_enabled_chk)

        trusted_hash_row = QHBoxLayout()
        trusted_hash_row.setContentsMargins(0, 0, 0, 0)
        trusted_hash_row.setSpacing(8)
        trusted_hash_label = QLabel("Trusted Hash")
        trusted_hash_label.setFixedWidth(msg_label_width)
        trusted_hash_row.addWidget(trusted_hash_label)
        self.trusted_hash_edit = QLineEdit()
        self.trusted_hash_edit.setPlaceholderText("Paste hash (SHA-1/SHA-256/SHA-512/MD5)")
        trusted_hash_row.addWidget(self.trusted_hash_edit, 1)
        self.trusted_hash_algo_combo = QComboBox()
        self.trusted_hash_algo_combo.addItems(["Auto", "SHA-1", "SHA-256", "SHA-512", "MD5"])
        self.trusted_hash_algo_combo.setFixedWidth(110)
        trusted_hash_row.addWidget(self.trusted_hash_algo_combo)
        self.trusted_hash_label_edit = QLineEdit()
        self.trusted_hash_label_edit.setPlaceholderText("Label (optional)")
        self.trusted_hash_label_edit.setFixedWidth(180)
        trusted_hash_row.addWidget(self.trusted_hash_label_edit)
        self.trusted_hash_add_btn = QPushButton("Add")
        self.trusted_hash_add_btn.setFixedWidth(70)
        trusted_hash_row.addWidget(self.trusted_hash_add_btn)
        gpg_v.addLayout(trusted_hash_row)

        trusted_hash_actions = QHBoxLayout()
        trusted_hash_actions.setContentsMargins(0, 0, 0, 0)
        trusted_hash_actions.setSpacing(8)
        trusted_hash_spacer = QLabel("")
        trusted_hash_spacer.setFixedWidth(msg_label_width)
        trusted_hash_actions.addWidget(trusted_hash_spacer)
        self.trusted_hash_import_btn = QPushButton("Import Hash File")
        self.trusted_hash_remove_btn = QPushButton("Remove Selected")
        self.trusted_hash_remove_btn.setEnabled(False)
        trusted_hash_actions.addWidget(self.trusted_hash_import_btn)
        trusted_hash_actions.addWidget(self.trusted_hash_remove_btn)
        trusted_hash_actions.addStretch()
        gpg_v.addLayout(trusted_hash_actions)

        self.trusted_hash_table = QTableWidget(0, 4)
        self.trusted_hash_table.setHorizontalHeaderLabels(["Use", "Algorithm", "Hash", "Label"])
        self.trusted_hash_table.verticalHeader().setVisible(False)
        self.trusted_hash_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.trusted_hash_table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.trusted_hash_table.setAlternatingRowColors(True)
        self.trusted_hash_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        th_hdr = self.trusted_hash_table.horizontalHeader()
        th_hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        th_hdr.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        th_hdr.setSectionResizeMode(2, QHeaderView.Stretch)
        th_hdr.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.trusted_hash_table.setMinimumHeight(150)
        gpg_v.addWidget(self.trusted_hash_table)

        gpg_path_row = QHBoxLayout()
        gpg_path_row.setContentsMargins(0, 0, 0, 0)
        gpg_path_row.setSpacing(8)
        gpg_path_label = QLabel("GPG Executable")
        gpg_path_label.setFixedWidth(msg_label_width)
        gpg_path_row.addWidget(gpg_path_label)
        self.gpg_path_edit = QLineEdit()
        self.gpg_path_edit.setPlaceholderText("Auto-detect (gpg/gpg2)")
        gpg_path_row.addWidget(self.gpg_path_edit, 1)
        self.gpg_browse_btn = QPushButton("Browse")
        self.gpg_browse_btn.setFixedWidth(70)
        self.gpg_test_btn = QPushButton("Test")
        self.gpg_test_btn.setFixedWidth(70)
        self.gpg_refresh_keys_btn = QPushButton("Refresh Keys")
        self.gpg_refresh_keys_btn.setFixedWidth(110)
        gpg_path_row.addWidget(self.gpg_browse_btn)
        gpg_path_row.addWidget(self.gpg_test_btn)
        gpg_path_row.addWidget(self.gpg_refresh_keys_btn)
        gpg_v.addLayout(gpg_path_row)

        gpg_action_row = QHBoxLayout()
        gpg_action_row.setContentsMargins(0, 0, 0, 0)
        gpg_action_row.setSpacing(8)
        gpg_action_spacer = QLabel("")
        gpg_action_spacer.setFixedWidth(msg_label_width)
        gpg_action_row.addWidget(gpg_action_spacer)
        self.gpg_import_key_btn = QPushButton("Import Key File")
        self.gpg_import_text_btn = QPushButton("Import Armored Key")
        self.gpg_sign_key_btn = QPushButton("Local-Sign Selected")
        self.gpg_sign_key_btn.setEnabled(False)
        gpg_action_row.addWidget(self.gpg_import_key_btn)
        gpg_action_row.addWidget(self.gpg_import_text_btn)
        gpg_action_row.addWidget(self.gpg_sign_key_btn)
        gpg_action_row.addStretch()
        gpg_v.addLayout(gpg_action_row)

        gpg_status_row = QHBoxLayout()
        gpg_status_row.setContentsMargins(0, 0, 0, 0)
        gpg_status_row.setSpacing(8)
        gpg_status_spacer = QLabel("")
        gpg_status_spacer.setFixedWidth(msg_label_width)
        gpg_status_row.addWidget(gpg_status_spacer)
        self.gpg_status_label = QLabel("GPG status: not checked")
        self.gpg_status_label.setWordWrap(True)
        gpg_status_row.addWidget(self.gpg_status_label, 1)
        gpg_v.addLayout(gpg_status_row)

        self.gpg_keys_table = QTableWidget(0, 3)
        self.gpg_keys_table.setHorizontalHeaderLabels(["Trusted", "Fingerprint", "User IDs"])
        self.gpg_keys_table.verticalHeader().setVisible(False)
        self.gpg_keys_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.gpg_keys_table.setSelectionMode(QTableWidget.SingleSelection)
        self.gpg_keys_table.setAlternatingRowColors(True)
        self.gpg_keys_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        gpg_hdr = self.gpg_keys_table.horizontalHeader()
        gpg_hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        gpg_hdr.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        gpg_hdr.setSectionResizeMode(2, QHeaderView.Stretch)
        self.gpg_keys_table.setMinimumHeight(180)
        gpg_v.addWidget(self.gpg_keys_table)

        self.gpg_verify_enabled_chk.stateChanged.connect(self._mark_settings_dirty)
        self.hash_verify_enabled_chk.stateChanged.connect(self._mark_settings_dirty)
        self.trusted_hash_edit.returnPressed.connect(self._add_trusted_hash_entry)
        self.trusted_hash_add_btn.clicked.connect(self._add_trusted_hash_entry)
        self.trusted_hash_import_btn.clicked.connect(self._import_trusted_hash_file)
        self.trusted_hash_remove_btn.clicked.connect(self._remove_selected_trusted_hash_entries)
        self.trusted_hash_table.itemChanged.connect(self._on_trusted_hash_table_item_changed)
        self.trusted_hash_table.itemSelectionChanged.connect(self._update_trusted_hash_actions)
        self.gpg_path_edit.textChanged.connect(self._mark_settings_dirty)
        self.gpg_browse_btn.clicked.connect(self._choose_gpg_executable_path)
        self.gpg_test_btn.clicked.connect(self._test_gpg_executable)
        self.gpg_refresh_keys_btn.clicked.connect(self._refresh_gpg_keys_table)
        self.gpg_import_key_btn.clicked.connect(self._import_gpg_key_file)
        self.gpg_import_text_btn.clicked.connect(self._import_gpg_key_text)
        self.gpg_sign_key_btn.clicked.connect(self._local_sign_selected_gpg_key)
        self.gpg_keys_table.itemChanged.connect(self._on_gpg_keys_table_item_changed)
        self.gpg_keys_table.itemSelectionChanged.connect(self._update_gpg_sign_button_state)

        gpg_container = QWidget()
        gpg_container.setLayout(gpg_v)
        gpg_group = self._make_collapsible_group(
            "Message Auth (Key/Hash)",
            gpg_container,
            checked=True,
            fit_content=True,
        )
        self._register_collapsible_group(gpg_group, self._summary_gpg_settings)
        gpg_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # VarAC Settings
        varac_group = QGroupBox("VarAC Settings")
        varac_v = QVBoxLayout()
        varac_v.setSpacing(6)
        varac_v.setAlignment(Qt.AlignTop)
        varac_group.setLayout(varac_v)

        varac_row = QHBoxLayout()
        varac_row.setContentsMargins(0, 0, 0, 0)
        varac_row.setSpacing(8)
        varac_install_label = QLabel("VarAC Install Folder:")
        varac_install_label.setFixedWidth(msg_label_width)
        varac_row.addWidget(varac_install_label)
        self.varac_path_edit = QLineEdit()
        self.varac_path_edit.setPlaceholderText("Folder containing VarAC")
        self.varac_path_edit.textChanged.connect(self._on_launch_paths_changed)
        varac_row.addWidget(self.varac_path_edit, 1)
        varac_browse = QPushButton("Browse")
        varac_browse.setFixedWidth(70)
        varac_browse.clicked.connect(self._choose_varac_install_path)
        varac_row.addWidget(varac_browse)
        varac_v.addLayout(varac_row)

        varac_v.addWidget(
            build_msg_row(
                "VarAC Incoming Files",
                varac_edit,
                lambda: self._choose_msg_path("varac", varac_edit),
            )
        )

        bbs_dir_row = QHBoxLayout()
        bbs_dir_row.setContentsMargins(0, 0, 0, 0)
        bbs_dir_row.setSpacing(8)
        bbs_dir_label = QLabel("BBS Directory")
        bbs_dir_label.setFixedWidth(msg_label_width)
        bbs_dir_row.addWidget(bbs_dir_label)
        self.varac_bbs_dir_edit = QLineEdit()
        self.varac_bbs_dir_edit.setPlaceholderText("VarAC BBS directory")
        bbs_dir_row.addWidget(self.varac_bbs_dir_edit, 1)
        bbs_dir_browse = QPushButton("Browse")
        bbs_dir_browse.setFixedWidth(70)
        bbs_dir_browse.clicked.connect(self._choose_varac_bbs_dir)
        bbs_dir_row.addWidget(bbs_dir_browse)
        varac_v.addLayout(bbs_dir_row)

        bbs_archive_row = QHBoxLayout()
        bbs_archive_row.setContentsMargins(0, 0, 0, 0)
        bbs_archive_row.setSpacing(8)
        bbs_archive_label = QLabel("BBS Archive")
        bbs_archive_label.setFixedWidth(msg_label_width)
        bbs_archive_row.addWidget(bbs_archive_label)
        self.varac_bbs_archive_dir_edit = QLineEdit()
        self.varac_bbs_archive_dir_edit.setPlaceholderText("Archive destination directory")
        bbs_archive_row.addWidget(self.varac_bbs_archive_dir_edit, 1)
        bbs_archive_browse = QPushButton("Browse")
        bbs_archive_browse.setFixedWidth(70)
        bbs_archive_browse.clicked.connect(self._choose_varac_bbs_archive_dir)
        bbs_archive_row.addWidget(bbs_archive_browse)
        varac_v.addLayout(bbs_archive_row)

        bbs_policy_row = QHBoxLayout()
        bbs_policy_row.setContentsMargins(0, 0, 0, 0)
        bbs_policy_row.setSpacing(8)
        bbs_policy_label = QLabel("Auto-Archive Policy")
        bbs_policy_label.setFixedWidth(msg_label_width)
        bbs_policy_row.addWidget(bbs_policy_label)
        self.varac_bbs_auto_archive_chk = QCheckBox("Enable Auto-Archive")
        bbs_policy_row.addWidget(self.varac_bbs_auto_archive_chk)
        bbs_policy_row.addSpacing(12)
        bbs_policy_row.addWidget(QLabel("After"))
        self.varac_bbs_archive_days_combo = QComboBox()
        for day in (1, 3, 5, 7, 10, 14, 21, 30):
            self.varac_bbs_archive_days_combo.addItem(str(day), day)
        self.varac_bbs_archive_days_combo.setCurrentText("14")
        self.varac_bbs_archive_days_combo.setFixedWidth(80)
        bbs_policy_row.addWidget(self.varac_bbs_archive_days_combo)
        bbs_policy_row.addWidget(QLabel("days"))
        bbs_policy_row.addStretch()
        varac_hint = QLabel("Moves files older than selected days from BBS Directory to BBS Archive.")
        varac_hint.setWordWrap(True)
        varac_hint.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        policy_wrap = QVBoxLayout()
        policy_wrap.setSpacing(3)
        policy_wrap.setContentsMargins(0, 0, 0, 0)
        policy_wrap.addLayout(bbs_policy_row)
        hint_row = QHBoxLayout()
        hint_row.setContentsMargins(0, 0, 0, 0)
        hint_row.addSpacing(msg_label_width)
        hint_row.addWidget(varac_hint, 1, Qt.AlignLeft | Qt.AlignTop)
        policy_wrap.addLayout(hint_row)
        varac_v.addLayout(policy_wrap)

        varac_container = QWidget()
        varac_container.setLayout(varac_v)
        varac_group = self._make_collapsible_group("VarAC Settings", varac_container, checked=True, fit_content=True)
        self._register_collapsible_group(varac_group, self._summary_varac_settings)
        varac_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(varac_group)
        self._add_settings_section(gpg_group)

        # Launch Control
        launch_group = QGroupBox("Launch Control")
        launch_v = QVBoxLayout()
        launch_v.setSpacing(6)
        launch_group.setLayout(launch_v)

        launch_hint = QLabel("Only configured apps are shown. Launch order controls startup sequence.")
        launch_hint.setWordWrap(True)
        launch_v.addWidget(launch_hint)

        launch_global_row = QHBoxLayout()
        self.launch_all_with_startup_chk = QCheckBox("Launch All with FreqInOut")
        launch_global_row.addWidget(self.launch_all_with_startup_chk)
        launch_global_row.addStretch()
        launch_v.addLayout(launch_global_row)

        self.launch_control_table = QTableWidget(0, 3)
        self.launch_control_table.setHorizontalHeaderLabels(["Application", "Enabled", "Launch on Startup"])
        self.launch_control_table.verticalHeader().setVisible(False)
        self.launch_control_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.launch_control_table.setSelectionMode(QTableWidget.SingleSelection)
        launch_header = self.launch_control_table.horizontalHeader()
        launch_header.setSectionResizeMode(0, QHeaderView.Stretch)
        launch_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        launch_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        launch_v.addWidget(self.launch_control_table)

        launch_btn_row = QHBoxLayout()
        self.launch_order_up_btn = QPushButton("Up")
        self.launch_order_down_btn = QPushButton("Down")
        self.launch_reset_order_btn = QPushButton("Reset Default Order")
        self.launch_configured_now_btn = QPushButton("Launch Configured Now")
        self.launch_stop_btn = QPushButton("Stop Launch Sequence")
        self.launch_stop_btn.setEnabled(False)
        launch_btn_row.addWidget(self.launch_order_up_btn)
        launch_btn_row.addWidget(self.launch_order_down_btn)
        launch_btn_row.addWidget(self.launch_reset_order_btn)
        launch_btn_row.addStretch()
        launch_btn_row.addWidget(self.launch_configured_now_btn)
        launch_btn_row.addWidget(self.launch_stop_btn)
        launch_v.addLayout(launch_btn_row)

        self.launch_summary_label = QLabel("Launch status: Idle")
        launch_v.addWidget(self.launch_summary_label)

        self.launch_order_up_btn.clicked.connect(lambda: self._move_launch_row(-1))
        self.launch_order_down_btn.clicked.connect(lambda: self._move_launch_row(1))
        self.launch_reset_order_btn.clicked.connect(self._reset_launch_order)
        self.launch_configured_now_btn.clicked.connect(self._launch_configured_now)
        self.launch_stop_btn.clicked.connect(self._stop_launch_sequence)
        self.launch_all_with_startup_chk.stateChanged.connect(self._refresh_section_titles)
        self.launch_control_table.itemChanged.connect(self._on_launch_table_item_changed)
        self.launch_control_table.itemSelectionChanged.connect(self._update_launch_control_buttons)
        self.launch_orchestrator.sequence_started.connect(self._on_launch_sequence_started)
        self.launch_orchestrator.sequence_progress.connect(self._on_launch_sequence_progress)
        self.launch_orchestrator.sequence_finished.connect(self._on_launch_sequence_finished)

        launch_container = QWidget()
        launch_container.setLayout(launch_v)
        launch_group = self._make_collapsible_group("Launch Control", launch_container, checked=True, fit_content=True)
        self._register_collapsible_group(launch_group, self._summary_launch_control)
        launch_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(launch_group)
        self._add_settings_section(logging_section)

        # bottom save
        bottom_row = QHBoxLayout()
        bottom_row.addStretch()
        self.save_btn = QPushButton("Save Settings")
        self.save_btn.clicked.connect(self._save_settings_button)
        bottom_row.addWidget(self.save_btn)
        main_layout.addLayout(bottom_row)
        self._wire_dirty_tracking()
        self._refresh_launch_control_table()
        self._set_save_button_state("success")
        self._refresh_section_titles()
        if self.sections_nav_list.count() > 0:
            self.sections_nav_list.setCurrentRow(0)
        self._update_sections_nav_size()

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
            "content": content,
        }
        self._apply_collapsed_state(group, content, checked)
        QTimer.singleShot(0, lambda g=group, w=content: self._apply_collapsed_state(g, w, header_btn.isChecked()))
        return group

    def _register_collapsible_group(self, group: QGroupBox, summary_fn) -> None:
        self._accordion_groups.append(group)
        meta = self._section_meta.get(group, {})
        meta.update({"summary_fn": summary_fn})
        self._section_meta[group] = meta

    def _add_settings_section(self, group: QGroupBox) -> None:
        meta = self._section_meta.get(group, {})
        title = str(meta.get("title", group.title() if hasattr(group, "title") else "Section"))
        item = QListWidgetItem(title)
        self.sections_nav_list.addItem(item)
        self._section_nav_items[group] = item
        self.sections_stack.addWidget(group)
        content = meta.get("content")
        header_btn = meta.get("header_btn")
        if isinstance(content, QWidget):
            expanded = bool(header_btn.isChecked()) if header_btn else True
            self._apply_collapsed_state(group, content, expanded)
        self._update_sections_nav_size()

    def _update_sections_nav_size(self) -> None:
        if not hasattr(self, "sections_nav_list"):
            return
        count = self.sections_nav_list.count()
        if count <= 0:
            return
        row_h = self.sections_nav_list.sizeHintForRow(0)
        if row_h <= 0:
            row_h = 28
        frame = self.sections_nav_list.frameWidth()
        target = (row_h * count) + (frame * 2) + 6
        self.sections_nav_list.setFixedHeight(max(120, min(target, 320)))

    def _on_section_nav_changed(self, row: int) -> None:
        if row < 0:
            return
        if row >= self.sections_stack.count():
            return
        self.sections_stack.setCurrentIndex(row)
        try:
            page = self.sections_stack.currentWidget()
            if isinstance(page, QGroupBox):
                header_btn = self._section_meta.get(page, {}).get("header_btn")
                if header_btn and not header_btn.isChecked():
                    header_btn.setChecked(True)
        except Exception:
            pass

    def _on_section_toggled(self, group: QGroupBox, content: QWidget, checked: bool) -> None:
        stacked_mode = hasattr(self, "sections_stack") and self.sections_stack.count() > 0
        if stacked_mode and not checked:
            header_btn = self._section_meta.get(group, {}).get("header_btn")
            if header_btn:
                QTimer.singleShot(0, lambda btn=header_btn: btn.setChecked(True))
            return
        self._apply_collapsed_state(group, content, checked)
        if checked and not stacked_mode:
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
            nav_item = self._section_nav_items.get(group)
            if nav_item:
                nav_item.setText(base)
                nav_item.setToolTip(summary if summary else base)
        self._update_sections_nav_size()

    def _apply_collapsed_state(self, group: QGroupBox, content: QWidget, expanded: bool) -> None:
        content.setVisible(expanded)
        stacked_mode = hasattr(self, "sections_stack") and self.sections_stack.count() > 0
        fit_content = bool(self._section_meta.get(group, {}).get("fit_content", False))
        if stacked_mode:
            fit_content = False
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

    def _summary_operator_info(self) -> str:
        callsign = self.callsign_edit.text().strip().upper() if hasattr(self, "callsign_edit") else ""
        grid = self.grid6_edit.text().strip().upper() if hasattr(self, "grid6_edit") else ""
        if callsign and grid:
            return f"{callsign} / {grid}"
        if callsign:
            return callsign
        return "operator profile"

    def _summary_operating_status(self) -> str:
        return "live software indicators"

    def _summary_freqinout_settings(self) -> str:
        ctrl = self.control_combo.currentText().strip() if hasattr(self, "control_combo") else "FLRig"
        scheduler = "on" if (hasattr(self, "use_scheduler_chk") and self.use_scheduler_chk.isChecked()) else "off"
        return f"Control {ctrl}, Scheduler {scheduler}"

    def _summary_logging_settings(self) -> str:
        log_level = self.log_level_combo.currentText().strip() if hasattr(self, "log_level_combo") else "INFO"
        return f"Level {log_level}"

    def _summary_operating_groups(self) -> str:
        count = len(self.operating_groups)
        return f"{count} group{'s' if count != 1 else ''}"

    def _summary_local_net_profiles(self) -> str:
        count = len(self.local_net_profiles)
        return f"{count} profile{'s' if count != 1 else ''}"

    def _summary_js8_settings(self) -> str:
        directed = "set" if self.js8_directed_edit.text().strip() else "missing"
        forms = "set" if self.js8_forms_edit.text().strip() else "missing"
        js8call = "set" if hasattr(self, "js8call_path_edit") and self.js8call_path_edit.text().strip() else "missing"
        spotter = "set" if hasattr(self, "js8spotter_path_edit") and self.js8spotter_path_edit.text().strip() else "missing"
        commstat = "set" if hasattr(self, "commstat_path_edit") and self.commstat_path_edit.text().strip() else "missing"
        return f"JS8Call {js8call}, Spotter {spotter}, CommStat {commstat}, DIRECTED {directed}, Forms {forms}"

    def _summary_fast_light_settings(self) -> str:
        total = len(self.PROGRAMS)
        set_count = 0
        for name in self.PROGRAMS:
            edit = self.path_edits.get(name)
            if edit and edit.text().strip():
                set_count += 1
        return f"{set_count}/{total} app paths set"

    def _summary_gpg_settings(self) -> str:
        enabled = bool(hasattr(self, "gpg_verify_enabled_chk") and self.gpg_verify_enabled_chk.isChecked())
        hash_enabled = bool(hasattr(self, "hash_verify_enabled_chk") and self.hash_verify_enabled_chk.isChecked())
        path_set = bool(hasattr(self, "gpg_path_edit") and self.gpg_path_edit.text().strip())
        trusted = len(self._gpg_trusted_fingerprints)
        local_hashes = len([r for r in self._trusted_hash_entries if bool(r.get("enabled", True))])
        return (
            f"Sig {'on' if enabled else 'off'}, Hash {'on' if hash_enabled else 'off'}, "
            f"GPG {'set' if path_set else 'auto'}, {trusted} keys, {local_hashes} hashes"
        )

    def _summary_varac_settings(self) -> str:
        install_set = bool(hasattr(self, "varac_path_edit") and self.varac_path_edit.text().strip())
        incoming_set = bool(self.msg_paths_edits.get("varac") and self.msg_paths_edits["varac"].text().strip())
        bbs_set = bool(
            hasattr(self, "varac_bbs_dir_edit")
            and self.varac_bbs_dir_edit.text().strip()
            and hasattr(self, "varac_bbs_archive_dir_edit")
            and self.varac_bbs_archive_dir_edit.text().strip()
        )
        archive_on = bool(hasattr(self, "varac_bbs_auto_archive_chk") and self.varac_bbs_auto_archive_chk.isChecked())
        return (
            f"Install {'set' if install_set else 'missing'}, "
            f"Incoming {'set' if incoming_set else 'missing'}, "
            f"BBS {'set' if bbs_set else 'missing'}, "
            f"Archive {'on' if archive_on else 'off'}"
        )

    def _summary_launch_control(self) -> str:
        total = len(self._launch_items_cache)
        enabled = sum(1 for item in self._launch_items_cache if bool(item.get("enabled", False)))
        startup = sum(1 for item in self._launch_items_cache if bool(item.get("startup", False)))
        launch_all = bool(hasattr(self, "launch_all_with_startup_chk") and self.launch_all_with_startup_chk.isChecked())
        return f"{enabled}/{total} enabled, {startup} startup, launch-all {'on' if launch_all else 'off'}"

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
        _perf_t0 = time.perf_counter()
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
        log_level = (data.get("log_level", "") or "INFO").strip().upper()
        if log_level not in {"DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"}:
            log_level = "INFO"
        if hasattr(self, "log_level_combo"):
            idx = self.log_level_combo.findText(log_level)
            self.log_level_combo.setCurrentIndex(idx if idx >= 0 else self.log_level_combo.findText("INFO"))
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
        self.js8call_path_edit.setText((data.get("path_js8call", "") or "").strip())
        self.js8spotter_path_edit.setText((data.get("path_js8spotter", "") or "").strip())
        self.commstat_path_edit.setText((data.get("path_commstat", "") or "").strip())
        self.js8_mark_retrieved_chk.setChecked(
            bool(data.get("js8_inbox_mark_retrieved_sync", False))
        )
        # Message paths
        msg_paths = data.get("message_paths", {})
        for origin, edit in self.msg_paths_edits.items():
            edit.setText(msg_paths.get(origin, ""))
        gpg_enabled = bool(data.get("gpg_verify_flamp_k2s_enabled", False))
        hash_enabled = bool(data.get("hash_verify_flamp_k2s_enabled", True))
        gpg_path = str(data.get("gpg_executable_path", "") or "").strip()
        trusted = data.get("gpg_trusted_signers", [])
        if not isinstance(trusted, list):
            trusted = []
        trusted_hashes_raw = data.get("trusted_file_hashes", [])
        if not isinstance(trusted_hashes_raw, list):
            trusted_hashes_raw = []
        self._gpg_trusted_fingerprints = {normalize_fingerprint(v) for v in trusted if normalize_fingerprint(v)}
        self._trusted_hash_entries = normalize_trusted_hash_entries(trusted_hashes_raw)
        if hasattr(self, "gpg_verify_enabled_chk"):
            self.gpg_verify_enabled_chk.setChecked(gpg_enabled)
        if hasattr(self, "hash_verify_enabled_chk"):
            self.hash_verify_enabled_chk.setChecked(hash_enabled)
        if hasattr(self, "gpg_path_edit"):
            self.gpg_path_edit.setText(gpg_path)
        self._refresh_trusted_hash_table()
        self._refresh_gpg_keys_table(show_dialog_on_error=False)
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
        if hasattr(self, "varac_bbs_dir_edit"):
            self.varac_bbs_dir_edit.setText((data.get("varac_bbs_dir", "") or "").strip())
        if hasattr(self, "varac_bbs_archive_dir_edit"):
            self.varac_bbs_archive_dir_edit.setText((data.get("varac_bbs_archive_dir", "") or "").strip())
        if hasattr(self, "varac_bbs_auto_archive_chk"):
            self.varac_bbs_auto_archive_chk.setChecked(bool(data.get("varac_bbs_auto_archive_enabled", False)))
        if hasattr(self, "varac_bbs_archive_days_combo"):
            allowed_days = {"1", "3", "5", "7", "10", "14", "21", "30"}
            day_val = str(data.get("varac_bbs_auto_archive_days", 14) or "14")
            if day_val not in allowed_days:
                day_val = "14"
            self.varac_bbs_archive_days_combo.setCurrentText(day_val)
        fldigi_log_path = (data.get("fldigi_log_path", "") or "").strip()
        if hasattr(self, "fldigi_log_path_edit"):
            self.fldigi_log_path_edit.setText(fldigi_log_path)
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

        # Load local net profiles (SOP local-net reminder metadata only).
        try:
            lnp = data.get("local_net_profiles", [])
            if isinstance(lnp, list):
                self.local_net_profiles = []
                for row in lnp:
                    if not isinstance(row, dict):
                        continue
                    normalized = self._normalize_local_net_profile(row)
                    if normalized.get("name"):
                        self.local_net_profiles.append(normalized)
            else:
                self.local_net_profiles = []
        except Exception:
            self.local_net_profiles = []
        self._refresh_local_net_profiles_table()

        self.js8_directed_edit.setText(data.get("js8_directed_path", "") or "")

        for prog_name, meta in self.PROGRAMS.items():
            path_key = meta["setting_key"]

            if path_key:
                self.path_edits[prog_name].setText(data.get(path_key, "") or "")

        self._launch_items_cache = self.launch_orchestrator.get_launch_items()
        launch_all = bool(self.settings.get("launch_control_enabled", data.get("launch_control_enabled", True)))
        self.launch_all_with_startup_chk.setChecked(launch_all)
        self._refresh_launch_control_table()

        log.info("SettingsTab: settings loaded.")
        self._update_launch_control_buttons()
        self._update_op_group_action_buttons()
        self._update_local_net_action_buttons()
        self._loading_settings = False
        self._settings_dirty = False
        self._set_save_button_state("success")
        self._refresh_section_titles()
        emit_span(
            "settings.load_settings",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=10.0,
        )

    def _save_settings_button(self):
        """Explicit save via the button (shows confirmation)."""
        self._save_settings(show_message=True)
        # Defer settings fanout one tick to keep Save interaction responsive.
        QTimer.singleShot(0, self._emit_settings_saved)
        QTimer.singleShot(0, self._maybe_backfill_js8_geo)
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _emit_settings_saved(self) -> None:
        try:
            self.settings_saved.emit()
        except Exception:
            pass

    def _save_settings_quiet(self):
        """Auto-save on application exit (no dialog)."""
        self._save_settings(show_message=False)

    def _save_settings(self, show_message: bool = True):
        _perf_t0 = time.perf_counter()
        data = self.settings.all()
        prev_operator = {
            "callsign": str(data.get("operator_callsign", "") or "").strip().upper(),
            "name": str(data.get("operator_name", "") or "").strip(),
            "state": str(data.get("operator_state", "") or "").strip().upper(),
            "grid6": str(data.get("operator_grid6", "") or "").strip().upper(),
        }

        data["operator_callsign"] = self.callsign_edit.text().strip()
        data["operator_name"] = self.name_edit.text().strip()
        data["operator_state"] = self.state_edit.text().strip()
        data["operator_grid6"] = self.grid6_edit.text().strip().upper()
        data["operator_grid6"] = self.grid6_edit.text().strip().upper()
        operator_changed = (
            prev_operator["callsign"] != str(data["operator_callsign"]).strip().upper()
            or prev_operator["name"] != str(data["operator_name"]).strip()
            or prev_operator["state"] != str(data["operator_state"]).strip().upper()
            or prev_operator["grid6"] != str(data["operator_grid6"]).strip().upper()
        )

        # Timezone is not user-editable; keep existing value (or detect if missing)
        tz = data.get("timezone")
        if not tz:
            tz = self._detect_system_timezone()
            data["timezone"] = tz

        data["control_via"] = self.control_combo.currentText().strip()
        data["log_level"] = (
            self.log_level_combo.currentText().strip().upper() if hasattr(self, "log_level_combo") else "INFO"
        )
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
        data["path_js8call"] = self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else ""
        data["path_js8spotter"] = (
            self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else ""
        )
        data["path_commstat"] = self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else ""
        data["js8_inbox_mark_retrieved_sync"] = bool(self.js8_mark_retrieved_chk.isChecked())
        msg_paths = {}
        for origin, edit in self.msg_paths_edits.items():
            msg_paths[origin] = edit.text().strip()
        data["message_paths"] = msg_paths
        data["gpg_verify_flamp_k2s_enabled"] = bool(
            self.gpg_verify_enabled_chk.isChecked() if hasattr(self, "gpg_verify_enabled_chk") else False
        )
        data["hash_verify_flamp_k2s_enabled"] = bool(
            self.hash_verify_enabled_chk.isChecked() if hasattr(self, "hash_verify_enabled_chk") else True
        )
        data["gpg_executable_path"] = self.gpg_path_edit.text().strip() if hasattr(self, "gpg_path_edit") else ""
        data["gpg_trusted_signers"] = sorted(
            [fp for fp in self._gpg_trusted_fingerprints if normalize_fingerprint(fp)]
        )
        data["trusted_file_hashes"] = [
            {
                "enabled": bool(row.get("enabled", True)),
                "algorithm": normalize_hash_algorithm(str(row.get("algorithm", "") or "")),
                "hash": normalize_hash_hex(str(row.get("hash", "") or "")),
                "label": str(row.get("label", "") or "").strip(),
            }
            for row in normalize_trusted_hash_entries(self._trusted_hash_entries)
        ]
        varac_path = self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else ""
        data["varac_path"] = varac_path
        data["varac_db_path"] = str(Path(varac_path) / "VarAC.db") if varac_path else ""
        data["varac_bbs_dir"] = (
            self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        )
        data["varac_bbs_archive_dir"] = (
            self.varac_bbs_archive_dir_edit.text().strip() if hasattr(self, "varac_bbs_archive_dir_edit") else ""
        )
        days_text = (
            self.varac_bbs_archive_days_combo.currentText().strip()
            if hasattr(self, "varac_bbs_archive_days_combo")
            else "14"
        )
        if days_text not in {"1", "3", "5", "7", "10", "14", "21", "30"}:
            days_text = "14"
        data["varac_bbs_auto_archive_enabled"] = bool(
            self.varac_bbs_auto_archive_chk.isChecked() if hasattr(self, "varac_bbs_auto_archive_chk") else False
        )
        data["varac_bbs_auto_archive_days"] = int(days_text)
        if data["varac_bbs_auto_archive_enabled"]:
            bbs_dir_txt = data["varac_bbs_dir"]
            bbs_archive_txt = data["varac_bbs_archive_dir"]
            if not bbs_dir_txt or not bbs_archive_txt:
                QMessageBox.warning(
                    self,
                    "Settings",
                    "When Auto-Archive BBS Files is enabled, both BBS Directory and BBS Archive are required.",
                )
                return
            bbs_dir = Path(bbs_dir_txt)
            bbs_archive = Path(bbs_archive_txt)
            if not bbs_dir.exists() or not bbs_dir.is_dir() or not bbs_archive.exists() or not bbs_archive.is_dir():
                QMessageBox.warning(
                    self,
                    "Settings",
                    "BBS Directory and BBS Archive must both exist and be directories when auto-archive is enabled.",
                )
                return
            try:
                if bbs_dir.resolve() == bbs_archive.resolve():
                    QMessageBox.warning(
                        self,
                        "Settings",
                        "BBS Directory and BBS Archive must be different directories.",
                    )
                    return
            except Exception:
                pass
        data["fldigi_log_path"] = (
            self.fldigi_log_path_edit.text().strip() if hasattr(self, "fldigi_log_path_edit") else ""
        )
        fldigi_dir = self.fldigi_checkin_dir_edit.text().strip()
        if not fldigi_dir:
            fldigi_dir = str(get_fldigi_checkin_dir())
            self.fldigi_checkin_dir_edit.setText(fldigi_dir)
        data["fldigi_checkin_dir"] = fldigi_dir

        groups = [le.text().strip().upper() for le in self.js8_groups_edits if le.text().strip()]
        data["primary_js8_groups"] = groups

        data["js8_directed_path"] = self.js8_directed_edit.text().strip()

        # Radio software paths from UI
        for prog_name, meta in self.PROGRAMS.items():
            path_key = meta["setting_key"]

            if path_key:
                data[path_key] = self.path_edits[prog_name].text().strip()

        # Launch Control settings
        self._sync_launch_cache_from_table()
        data["launch_control_items"] = [dict(item) for item in self._launch_items_cache]
        data["launch_control_enabled"] = bool(self.launch_all_with_startup_chk.isChecked())
        data["launch_control_migrated_v1"] = True
        data["launch_readiness_timeout_sec"] = int(self.settings.get("launch_readiness_timeout_sec", 30) or 30)
        startup_by_name = {
            str(item.get("name", "")).strip(): bool(item.get("startup", False))
            for item in data["launch_control_items"]
            if isinstance(item, dict)
        }
        data["autostart_flrig"] = bool(startup_by_name.get("FLRig", False))
        data["autostart_fldigi"] = bool(startup_by_name.get("FLDigi", False))
        data["autostart_flmsg"] = bool(startup_by_name.get("FLMsg", False))
        data["autostart_flamp"] = bool(startup_by_name.get("FLAmp", False))
        data["autostart_js8call"] = bool(startup_by_name.get("JS8Call", False))

        data["operating_groups"] = self._table_to_operating_groups()
        data["local_net_profiles"] = self._table_to_local_net_profiles()
        try:
            set_log_level(str(data.get("log_level", "INFO") or "INFO"))
        except Exception:
            pass

        # Persist with a single write when possible.
        if hasattr(self.settings, "set_many"):
            batch = {
                "operator_callsign": data["operator_callsign"],
                "operator_name": data["operator_name"],
                "operator_state": data["operator_state"],
                "operator_grid6": data["operator_grid6"],
                "timezone": data["timezone"],
                "control_via": data["control_via"],
                "log_level": data.get("log_level", "INFO"),
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
                "path_js8call": data.get("path_js8call", ""),
                "path_js8spotter": data.get("path_js8spotter", ""),
                "path_commstat": data.get("path_commstat", ""),
                "js8_inbox_mark_retrieved_sync": data.get("js8_inbox_mark_retrieved_sync", False),
                "message_paths": data.get("message_paths", {}),
                "gpg_verify_flamp_k2s_enabled": data.get("gpg_verify_flamp_k2s_enabled", False),
                "hash_verify_flamp_k2s_enabled": data.get("hash_verify_flamp_k2s_enabled", True),
                "gpg_executable_path": data.get("gpg_executable_path", ""),
                "gpg_trusted_signers": data.get("gpg_trusted_signers", []),
                "trusted_file_hashes": data.get("trusted_file_hashes", []),
                "varac_path": data.get("varac_path", ""),
                "varac_db_path": data.get("varac_db_path", ""),
                "varac_bbs_dir": data.get("varac_bbs_dir", ""),
                "varac_bbs_archive_dir": data.get("varac_bbs_archive_dir", ""),
                "varac_bbs_auto_archive_enabled": data.get("varac_bbs_auto_archive_enabled", False),
                "varac_bbs_auto_archive_days": data.get("varac_bbs_auto_archive_days", 14),
                "fldigi_log_path": data.get("fldigi_log_path", ""),
                "fldigi_checkin_dir": data.get("fldigi_checkin_dir", ""),
                "launch_control_items": data.get("launch_control_items", []),
                "launch_control_enabled": data.get("launch_control_enabled", True),
                "launch_control_migrated_v1": data.get("launch_control_migrated_v1", True),
                "launch_readiness_timeout_sec": data.get("launch_readiness_timeout_sec", 30),
                "operating_groups": data.get("operating_groups", []),
                "local_net_profiles": data.get("local_net_profiles", []),
            }
            for prog_name, meta in self.PROGRAMS.items():
                path_key = meta["setting_key"]
                auto_key = meta["autostart_key"]
                if path_key:
                    batch[path_key] = data.get(path_key, "")
                if auto_key:
                    batch[auto_key] = data.get(auto_key, False)
            batch["autostart_js8call"] = data.get("autostart_js8call", False)
            self.settings.set_many(batch, save=True)  # type: ignore[attr-defined]
        elif hasattr(self.settings, "set"):
            self.settings.set("operator_callsign", data["operator_callsign"])
            self.settings.set("operator_name", data["operator_name"])
            self.settings.set("operator_state", data["operator_state"])
            self.settings.set("operator_grid6", data["operator_grid6"])
            self.settings.set("timezone", data["timezone"])
            self.settings.set("control_via", data["control_via"])
            self.settings.set("log_level", data.get("log_level", "INFO"))
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
            self.settings.set("path_js8call", data.get("path_js8call", ""))
            self.settings.set("path_js8spotter", data.get("path_js8spotter", ""))
            self.settings.set("path_commstat", data.get("path_commstat", ""))
            self.settings.set(
                "js8_inbox_mark_retrieved_sync",
                data.get("js8_inbox_mark_retrieved_sync", False),
            )
            self.settings.set("message_paths", data.get("message_paths", {}))
            self.settings.set("gpg_verify_flamp_k2s_enabled", data.get("gpg_verify_flamp_k2s_enabled", False))
            self.settings.set("hash_verify_flamp_k2s_enabled", data.get("hash_verify_flamp_k2s_enabled", True))
            self.settings.set("gpg_executable_path", data.get("gpg_executable_path", ""))
            self.settings.set("gpg_trusted_signers", data.get("gpg_trusted_signers", []))
            self.settings.set("trusted_file_hashes", data.get("trusted_file_hashes", []))
            self.settings.set("varac_path", data.get("varac_path", ""))
            self.settings.set("varac_db_path", data.get("varac_db_path", ""))
            self.settings.set("varac_bbs_dir", data.get("varac_bbs_dir", ""))
            self.settings.set("varac_bbs_archive_dir", data.get("varac_bbs_archive_dir", ""))
            self.settings.set("varac_bbs_auto_archive_enabled", data.get("varac_bbs_auto_archive_enabled", False))
            self.settings.set("varac_bbs_auto_archive_days", data.get("varac_bbs_auto_archive_days", 14))
            self.settings.set("fldigi_log_path", data.get("fldigi_log_path", ""))
            self.settings.set("fldigi_checkin_dir", data.get("fldigi_checkin_dir", ""))
            self.settings.set("launch_control_items", data.get("launch_control_items", []))
            self.settings.set("launch_control_enabled", data.get("launch_control_enabled", True))
            self.settings.set("launch_control_migrated_v1", data.get("launch_control_migrated_v1", True))
            self.settings.set("launch_readiness_timeout_sec", data.get("launch_readiness_timeout_sec", 30))
            for prog_name, meta in self.PROGRAMS.items():
                path_key = meta["setting_key"]
                auto_key = meta["autostart_key"]
                if path_key:
                    self.settings.set(path_key, data.get(path_key, ""))
                if auto_key:
                    self.settings.set(auto_key, data.get(auto_key, False))
            self.settings.set("autostart_js8call", data.get("autostart_js8call", False))
            self.settings.set("operating_groups", data.get("operating_groups", []))
            self.settings.set("local_net_profiles", data.get("local_net_profiles", []))
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
        if operator_changed:
            QTimer.singleShot(0, self._refresh_operator_history_views)
        self._settings_dirty = False
        self._set_save_button_state("success")
        emit_span(
            "settings.save_settings",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            meta={"operator_changed": operator_changed},
            min_ms=0.0,
        )

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

    def _request_open_logs(self) -> None:
        try:
            self.open_logs_requested.emit()
        except Exception:
            pass

    def _on_log_level_changed(self, level: str) -> None:
        if self._loading_settings:
            return
        level = (level or "INFO").strip().upper()
        if level not in {"DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"}:
            level = "INFO"
        prev_level = (self.settings.get("log_level", "") or "INFO").strip().upper()
        if not prev_level:
            try:
                prev_level = get_log_level().strip().upper()
            except Exception:
                prev_level = "INFO"
        if level == prev_level:
            return
        if level == "DEBUG":
            confirm = QMessageBox.question(
                self,
                "Enable DEBUG Logging",
                "DEBUG logging can impact performance and disk usage.\n\nEnable DEBUG now?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if confirm != QMessageBox.Yes:
                self.log_level_combo.blockSignals(True)
                idx = self.log_level_combo.findText(prev_level)
                self.log_level_combo.setCurrentIndex(idx if idx >= 0 else self.log_level_combo.findText("INFO"))
                self.log_level_combo.blockSignals(False)
                return
        try:
            set_log_level(level)
            self.settings.set_many(
                {
                    "log_level": level,
                    "timed_debug_until_utc": "",
                    "timed_debug_prev_level": "",
                }
            )
        except Exception:
            pass
        try:
            self.log_level_changed.emit(level)
        except Exception:
            pass
        self._refresh_section_titles()

    def _enable_timed_debug(self) -> None:
        minutes = int(self.debug_duration_combo.currentData() or 30)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        until = now_utc + datetime.timedelta(minutes=minutes)
        current = (self.settings.get("log_level", "") or "INFO").strip().upper()
        if current not in {"DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"}:
            current = "INFO"
        prev_level = current if current != "DEBUG" else "INFO"
        try:
            set_log_level("DEBUG")
            self.settings.set_many(
                {
                    "log_level": "DEBUG",
                    "timed_debug_until_utc": until.isoformat(),
                    "timed_debug_prev_level": prev_level,
                }
            )
            self.log_level_combo.blockSignals(True)
            idx = self.log_level_combo.findText("DEBUG")
            self.log_level_combo.setCurrentIndex(idx if idx >= 0 else 0)
            self.log_level_combo.blockSignals(False)
        except Exception:
            pass
        try:
            self.log_level_changed.emit("DEBUG")
        except Exception:
            pass
        self._refresh_section_titles()
        QMessageBox.information(
            self,
            "Timed DEBUG Enabled",
            f"DEBUG logging enabled for {minutes} minutes.\n"
            f"It will automatically revert at {until.astimezone():%Y-%m-%d %H:%M}.",
        )

    def _open_log_folder(self) -> None:
        log_file = _get_log_file()
        folder = str(Path(log_file).parent)
        try:
            if sys.platform.startswith("win"):
                os.startfile(folder)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", folder])
            else:
                subprocess.Popen(["xdg-open", folder])
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not open log folder:\n{e}")

    def _export_diagnostics(self) -> None:
        default_name = f"freqinout_diagnostics_{datetime.datetime.now():%Y%m%d_%H%M%S}.zip"
        out_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Diagnostics",
            default_name,
            "ZIP Files (*.zip)",
        )
        if not out_path:
            return
        if not out_path.lower().endswith(".zip"):
            out_path += ".zip"
        cfg_dir = get_config_dir() / "config"
        files = [
            Path(_get_log_file()),
            cfg_dir / "freqinout.db",
            cfg_dir / "freqinout_nets.db",
        ]
        added = 0
        try:
            with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for p in files:
                    if p.exists() and p.is_file():
                        zf.write(p, arcname=p.name)
                        added += 1
                info = (
                    f"FreqInOut version: {__version__}\n"
                    f"Exported: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n"
                    f"Platform: {platform.platform()}\n"
                    f"Log level: {(self.settings.get('log_level', 'INFO') or 'INFO')}\n"
                )
                zf.writestr("diagnostics_info.txt", info)
        except Exception as e:
            QMessageBox.critical(self, "Export Diagnostics", f"Failed to export diagnostics:\n{e}")
            return
        QMessageBox.information(
            self,
            "Export Diagnostics",
            f"Export complete.\nFiles included: {added} (+ diagnostics_info.txt).",
        )

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

    def _update_logging_actions_layout(self) -> None:
        if not (
            hasattr(self, "logging_actions_grid")
            and hasattr(self, "open_logs_btn")
            and hasattr(self, "open_log_folder_btn")
            and hasattr(self, "export_diag_btn")
        ):
            return

        width = self.logging_group.width() if hasattr(self, "logging_group") else 0
        compact = width < 640
        very_compact = width < 480

        for btn in (self.open_logs_btn, self.open_log_folder_btn, self.export_diag_btn):
            try:
                self.logging_actions_grid.removeWidget(btn)
            except Exception:
                pass

        for col in range(4):
            self.logging_actions_grid.setColumnStretch(col, 0)
        self.logging_actions_grid.setColumnStretch(3, 1)

        if very_compact:
            self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0, 1, 2)
            self.logging_actions_grid.addWidget(self.open_log_folder_btn, 1, 0)
            self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 1)
            return

        self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0)
        self.logging_actions_grid.addWidget(self.open_log_folder_btn, 0, 1)
        try:
            if compact:
                self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 0, 1, 2)
            else:
                self.logging_actions_grid.addWidget(self.export_diag_btn, 0, 2)
        except Exception:
            self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 0, 1, 2)

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
            self.js8call_path_edit,
            self.js8spotter_path_edit,
            self.commstat_path_edit,
            self.fldigi_checkin_dir_edit,
            self.fldigi_log_path_edit,
            self.varac_bbs_dir_edit,
            self.varac_bbs_archive_dir_edit,
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
            self.varac_bbs_archive_days_combo,
        ]
        for combo in combos:
            combo.currentIndexChanged.connect(self._mark_settings_dirty)

        checks = [
            self.use_scheduler_chk,
            self.js8_mark_retrieved_chk,
            self.varac_bbs_auto_archive_chk,
            self.launch_all_with_startup_chk,
        ]
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

    # ---------- Launch Control ---------- #

    def _on_launch_paths_changed(self, *_args) -> None:
        if self._loading_settings:
            return
        self._refresh_launch_control_table()

    def _is_launch_item_configured(self, name: str) -> bool:
        if name == "VarAC":
            path_val = self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else ""
            return bool(path_val)
        if name == "JS8Call":
            return bool(self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else "")
        if name == "JS8Spotter":
            return bool(self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else "")
        if name == "CommStat":
            return bool(self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else "")
        edit = self.path_edits.get(name)
        return bool(edit and edit.text().strip())

    def _sync_launch_cache_from_table(self) -> None:
        if not hasattr(self, "launch_control_table"):
            return
        for row, name in enumerate(self._launch_visible_names):
            enabled_item = self.launch_control_table.item(row, 1)
            startup_item = self.launch_control_table.item(row, 2)
            enabled = bool(enabled_item and enabled_item.checkState() == Qt.Checked)
            startup = bool(startup_item and startup_item.checkState() == Qt.Checked)
            for item in self._launch_items_cache:
                if str(item.get("name", "")).strip() == name:
                    item["enabled"] = enabled
                    item["startup"] = startup
                    break

    def _refresh_launch_control_table(self) -> None:
        _perf_t0 = time.perf_counter()
        if not hasattr(self, "launch_control_table"):
            emit_span(
                "settings.refresh_launch_control_table",
                (time.perf_counter() - _perf_t0) * 1000.0,
                settings=self.settings,
                min_ms=5.0,
            )
            return
        self._sync_launch_cache_from_table()
        existing_map: Dict[str, Dict[str, object]] = {}
        for item in self._launch_items_cache:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if name:
                existing_map[name] = item
        if not existing_map:
            for name in LAUNCH_APP_ORDER:
                existing_map[name] = {"name": name, "enabled": True, "startup": False}
        ordered: List[Dict[str, object]] = []
        seen: set[str] = set()
        for item in self._launch_items_cache:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if name not in LAUNCH_APP_ORDER or name in seen:
                continue
            seen.add(name)
            ordered.append(
                {
                    "name": name,
                    "enabled": bool(item.get("enabled", True)),
                    "startup": bool(item.get("startup", False)),
                }
            )
        for name in LAUNCH_APP_ORDER:
            if name in seen:
                continue
            item = existing_map.get(name, {"name": name, "enabled": True, "startup": False})
            ordered.append(
                {
                    "name": name,
                    "enabled": bool(item.get("enabled", True)),
                    "startup": bool(item.get("startup", False)),
                }
            )
        self._launch_items_cache = ordered

        visible_items = [item for item in self._launch_items_cache if self._is_launch_item_configured(str(item.get("name", "")))]
        self._launch_visible_names = [str(item.get("name", "")) for item in visible_items]

        self._launch_table_loading = True
        self.launch_control_table.blockSignals(True)
        self.launch_control_table.setRowCount(len(visible_items))
        for row, item in enumerate(visible_items):
            name = str(item.get("name", "")).strip()
            app_item = QTableWidgetItem(name)
            app_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.launch_control_table.setItem(row, 0, app_item)

            enabled_item = QTableWidgetItem()
            enabled_item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            enabled_item.setCheckState(Qt.Checked if bool(item.get("enabled", True)) else Qt.Unchecked)
            self.launch_control_table.setItem(row, 1, enabled_item)

            startup_item = QTableWidgetItem()
            startup_item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            startup_item.setCheckState(Qt.Checked if bool(item.get("startup", False)) else Qt.Unchecked)
            self.launch_control_table.setItem(row, 2, startup_item)
        self.launch_control_table.blockSignals(False)
        self._launch_table_loading = False
        if self.launch_control_table.rowCount() > 0 and self.launch_control_table.currentRow() < 0:
            self.launch_control_table.selectRow(0)
        self._update_launch_control_buttons()
        self._refresh_section_titles()
        emit_span(
            "settings.refresh_launch_control_table",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=5.0,
        )

    def _update_launch_control_buttons(self) -> None:
        row = self.launch_control_table.currentRow() if hasattr(self, "launch_control_table") else -1
        has_rows = bool(hasattr(self, "launch_control_table") and self.launch_control_table.rowCount() > 0)
        can_move = has_rows and row >= 0
        self.launch_order_up_btn.setEnabled(bool(can_move and row > 0))
        self.launch_order_down_btn.setEnabled(bool(can_move and row < self.launch_control_table.rowCount() - 1))
        self.launch_reset_order_btn.setEnabled(has_rows)
        self.launch_configured_now_btn.setEnabled(has_rows and not self.launch_orchestrator.is_active())
        self.launch_stop_btn.setEnabled(self.launch_orchestrator.is_active())

    def _move_launch_row(self, direction: int) -> None:
        if direction == 0:
            return
        row = self.launch_control_table.currentRow()
        if row < 0:
            return
        target_row = row + direction
        if target_row < 0 or target_row >= self.launch_control_table.rowCount():
            return
        self._sync_launch_cache_from_table()
        name_a = self._launch_visible_names[row]
        name_b = self._launch_visible_names[target_row]
        idx_a = next((i for i, item in enumerate(self._launch_items_cache) if str(item.get("name", "")) == name_a), -1)
        idx_b = next((i for i, item in enumerate(self._launch_items_cache) if str(item.get("name", "")) == name_b), -1)
        if idx_a < 0 or idx_b < 0:
            return
        self._launch_items_cache[idx_a], self._launch_items_cache[idx_b] = (
            self._launch_items_cache[idx_b],
            self._launch_items_cache[idx_a],
        )
        self._refresh_launch_control_table()
        if 0 <= target_row < self.launch_control_table.rowCount():
            self.launch_control_table.selectRow(target_row)
        self._mark_settings_dirty()

    def _reset_launch_order(self) -> None:
        self._sync_launch_cache_from_table()
        existing_map = {
            str(item.get("name", "")).strip(): item
            for item in self._launch_items_cache
            if isinstance(item, dict)
        }
        reset_items: List[Dict[str, object]] = []
        for name in LAUNCH_APP_ORDER:
            prev = existing_map.get(name, {})
            reset_items.append(
                {
                    "name": name,
                    "enabled": bool(prev.get("enabled", True)),
                    "startup": bool(prev.get("startup", False)),
                }
            )
        self._launch_items_cache = reset_items
        self._refresh_launch_control_table()
        self._mark_settings_dirty()

    def _on_launch_table_item_changed(self, _item: QTableWidgetItem) -> None:
        if self._loading_settings or self._launch_table_loading:
            return
        self._sync_launch_cache_from_table()
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _launch_configured_now(self) -> None:
        self._sync_launch_cache_from_table()
        started = self.launch_orchestrator.start_manual_sequence(self._launch_items_cache)
        if not started:
            QMessageBox.information(self, "Launch Control", "No enabled configured applications to launch.")
            return
        self._update_launch_control_buttons()

    def _stop_launch_sequence(self) -> None:
        self.launch_orchestrator.stop_sequence()
        self._update_launch_control_buttons()

    def _on_launch_sequence_started(self, payload: object) -> None:
        try:
            data = payload if isinstance(payload, dict) else {}
            trigger = str(data.get("trigger", "")).strip().capitalize() or "Launch"
            self.launch_summary_label.setText(f"Launch status: {trigger} sequence running...")
        except Exception:
            self.launch_summary_label.setText("Launch status: sequence running...")
        self._update_launch_control_buttons()

    def _on_launch_sequence_progress(self, payload: object) -> None:
        try:
            data = payload if isinstance(payload, dict) else {}
            name = str(data.get("name", "")).strip()
            status = str(data.get("status", "")).strip()
            detail = str(data.get("detail", "")).strip()
            self.launch_summary_label.setText(f"Launch status: {name} {status} ({detail})")
        except Exception:
            pass
        self._update_launch_control_buttons()

    def _on_launch_sequence_finished(self, payload: object) -> None:
        data = payload if isinstance(payload, dict) else {}
        launched = int(data.get("launched", 0) or 0)
        already_running = int(data.get("already_running", 0) or 0)
        failed = int(data.get("failed", 0) or 0)
        timeout = int(data.get("timeout", 0) or 0)
        cancelled = bool(data.get("cancelled", False))
        trigger = str(data.get("trigger", "")).strip().lower()
        status_txt = (
            f"Launch status: done (launched={launched}, running={already_running}, failed={failed}, timeout={timeout})"
        )
        if cancelled:
            status_txt = (
                f"Launch status: cancelled (launched={launched}, running={already_running}, failed={failed}, timeout={timeout})"
            )
        self.launch_summary_label.setText(status_txt)
        if trigger == "manual":
            QMessageBox.information(
                self,
                "Launch Summary",
                (
                    f"Launched: {launched}\n"
                    f"Already running: {already_running}\n"
                    f"Failed: {failed}\n"
                    f"Timeout: {timeout}\n"
                    f"Cancelled: {'Yes' if cancelled else 'No'}"
                ),
            )
        self._update_launch_control_buttons()

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
        conn = None
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path, timeout=5.0)
            conn.execute("PRAGMA busy_timeout=5000")
            ensure_operator_checkins_schema(conn)
            cur = conn.cursor()
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            cur.execute(
                """
                INSERT INTO operator_checkins
                    (callsign, name, state, grid, group1, group2, group3, group_role,
                     first_seen_utc, last_seen_utc, last_net, last_role, checkin_count, groups_json, trusted)
                VALUES (?, ?, ?, ?, '', '', '', '', ?, ?, '', '', 0, NULL, 0)
                ON CONFLICT(callsign) DO UPDATE SET
                    name=excluded.name,
                    state=excluded.state,
                    grid=excluded.grid,
                    last_seen_utc=excluded.last_seen_utc,
                    trusted=COALESCE(operator_checkins.trusted, excluded.trusted)
                """,
                (cs, name.strip(), state.strip().upper(), grid, now_iso, now_iso),
            )
            conn.commit()
        except Exception as e:
            log.debug("SettingsTab: failed to persist operator grid to DB: %s", e)
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

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
        if hasattr(self, "launch_selected_btn"):
            theme = resolve_theme(self.settings)
            any_selected = any(chk.isChecked() for chk in self.radio_checkboxes.values())
            role = "info" if any_selected else "muted"
            self.launch_selected_btn.setStyleSheet(button_style(role, theme))
        self._update_launch_control_buttons()

    def _program_is_running(self, program_name: str) -> bool:
        try:
            return bool(self._status_service.program_is_running(program_name))
        except Exception:
            return False

    def _find_process_exe(self, program_name: str) -> Optional[str]:
        try:
            return self._status_service.find_process_exe(program_name)
        except Exception:
            return None

    def _refresh_running_status(self):
        _perf_t0 = time.perf_counter()
        theme = resolve_theme(self.settings)
        port_override: Optional[int] = None
        try:
            txt = self.js8_port_edit.text().strip() if hasattr(self, "js8_port_edit") else ""
            port_override = int(txt) if txt else None
        except Exception:
            port_override = None
        snapshot = self._status_service.status_snapshot(port_override=port_override)
        for program_name, lbl in self.status_labels.items():
            info = snapshot.get(program_name, {})
            state = str(info.get("state", "idle"))
            tooltip = str(info.get("tooltip", "Not running"))
            lbl.setStyleSheet(led_style(state, theme))
            lbl.setToolTip(tooltip)

        # Keep VarAC path tooltip in sync with runtime status.
        if hasattr(self, "varac_path_edit"):
            varac_info = snapshot.get("VarAC", {})
            self.varac_path_edit.setToolTip(str(varac_info.get("tooltip", "Not running")))
        emit_span(
            "settings.refresh_running_status",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=5.0,
        )

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
            self._update_local_net_action_buttons()
            self._set_save_button_state("info" if self._settings_dirty else "success")
            if hasattr(self, "open_logs_btn"):
                self.open_logs_btn.setStyleSheet(button_style("primary", theme))
            if hasattr(self, "open_log_folder_btn"):
                self.open_log_folder_btn.setStyleSheet(button_style("secondary", theme))
            if hasattr(self, "export_diag_btn"):
                self.export_diag_btn.setStyleSheet(button_style("secondary", theme))
            if hasattr(self, "enable_timed_debug_btn"):
                self.enable_timed_debug_btn.setStyleSheet(button_style("warning", theme))
            if hasattr(self, "logging_warning_label"):
                self.logging_warning_label.setStyleSheet(f"color: {theme.get('text_muted', theme.get('text', '#666'))};")
            if hasattr(self, "sections_nav_list"):
                self.sections_nav_list.setStyleSheet(
                    "QListWidget {"
                    f" background: {theme.get('surface_alt', theme.get('surface', '#f2f2f2'))};"
                    f" border: 1px solid {theme.get('border', '#cccccc')};"
                    f" color: {theme.get('text', '#222222')};"
                    "}"
                    "QListWidget::item {"
                    " padding: 6px 8px;"
                    " border-radius: 4px;"
                    " margin: 1px 2px;"
                    "}"
                    "QListWidget::item:hover {"
                    f" background: {theme.get('surface', '#ffffff')};"
                    f" border: 1px solid {theme.get('accent', '#2a6fd3')};"
                    "}"
                    "QListWidget::item:selected {"
                    f" background: {theme.get('accent_soft', theme.get('surface', '#e6f2ff'))};"
                    f" color: {theme.get('text', '#222222')};"
                    " font-weight: 600;"
                    "}"
                )
                self._update_sections_nav_size()
            self._update_enforcement_visibility()
            self._update_logging_actions_layout()
        except Exception:
            pass

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_logging_actions_layout()

    def _js8_api_reachable(self) -> bool:
        try:
            port_txt = self.js8_port_edit.text().strip() if hasattr(self, "js8_port_edit") else ""
            port_override = int(port_txt) if port_txt else None
        except Exception:
            port_override = None
        try:
            return bool(self._status_service.js8_api_reachable(port_override=port_override))
        except Exception:
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
        _perf_t0 = time.perf_counter()
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
        emit_span(
            "settings.refresh_operating_groups_table",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=5.0,
        )

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
            QMessageBox.information(self, "Delete Groups", "Select one or more HF Operating Groups to delete.")
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
        QMessageBox.information(self, "Delete Groups", f"Deleted {len(to_remove)} HF Operating Group(s).")

    # ---------- Local Net Profiles ---------- #

    def _normalize_local_net_profile(self, row: Dict) -> Dict[str, str]:
        name = str(row.get("name", "") or "").strip()
        service = str(row.get("service", "") or "").strip()
        mode = str(row.get("mode", "") or "").strip()
        target = str(row.get("target", "") or "").strip()
        notes = str(row.get("notes", "") or "").strip()
        if service not in LOCAL_NET_SERVICE_OPTIONS:
            service = "Other" if service else LOCAL_NET_SERVICE_OPTIONS[0]
        return {
            "name": name,
            "service": service,
            "mode": mode,
            "target": target,
            "notes": notes,
        }

    def _table_to_local_net_profiles(self) -> List[Dict[str, str]]:
        cleaned: List[Dict[str, str]] = []
        seen: set[str] = set()
        for raw in self.local_net_profiles:
            if not isinstance(raw, dict):
                continue
            row = self._normalize_local_net_profile(raw)
            name = row.get("name", "")
            key = name.strip().upper()
            if not key or key in seen:
                continue
            seen.add(key)
            cleaned.append(row)
        cleaned.sort(key=lambda r: (r.get("name", "").lower(), r.get("service", "").lower()))
        self.local_net_profiles = cleaned
        return [dict(r) for r in cleaned]

    def _selected_local_net_rows(self) -> List[int]:
        rows: List[int] = []
        for r in range(self.local_net_table.rowCount()):
            w = self.local_net_table.cellWidget(r, 0)
            if isinstance(w, QCheckBox) and w.isChecked():
                rows.append(r)
            elif isinstance(w, QWidget):
                chk = w.findChild(QCheckBox)
                if chk is not None and chk.isChecked():
                    rows.append(r)
        return rows

    def _update_local_net_action_buttons(self) -> None:
        theme = resolve_theme(self.settings)
        has_selection = bool(self._selected_local_net_rows()) if hasattr(self, "local_net_table") else False
        role = "info" if has_selection else "muted"
        self.add_local_net_btn.setStyleSheet(button_style("primary", theme))
        self.edit_local_net_btn.setEnabled(True)
        self.delete_local_net_btn.setEnabled(True)
        self.edit_local_net_btn.setStyleSheet(button_style(role, theme))
        self.delete_local_net_btn.setStyleSheet(button_style(role, theme))

    def _refresh_local_net_profiles_table(self) -> None:
        rows = self._table_to_local_net_profiles()
        table = self.local_net_table
        table.setRowCount(0)
        for prof in rows:
            row = table.rowCount()
            table.insertRow(row)
            sel_chk = QCheckBox()
            sel_chk.setFixedWidth(22)
            sel_chk.stateChanged.connect(self._update_local_net_action_buttons)
            sel_wrap = QWidget()
            sel_layout = QHBoxLayout(sel_wrap)
            sel_layout.setContentsMargins(0, 0, 0, 0)
            sel_layout.setAlignment(Qt.AlignCenter)
            sel_layout.addWidget(sel_chk)
            table.setCellWidget(row, 0, sel_wrap)
            table.setItem(row, 1, QTableWidgetItem(prof.get("name", "")))
            table.setItem(row, 2, QTableWidgetItem(prof.get("service", "")))
            table.setItem(row, 3, QTableWidgetItem(prof.get("mode", "")))
            table.setItem(row, 4, QTableWidgetItem(prof.get("target", "")))
            table.setItem(row, 5, QTableWidgetItem(prof.get("notes", "")))
        self._update_local_net_action_buttons()
        self._refresh_section_titles()

    def _local_profile_from_row(self, row: int) -> Dict[str, str]:
        return {
            "name": self.local_net_table.item(row, 1).text().strip() if self.local_net_table.item(row, 1) else "",
            "service": self.local_net_table.item(row, 2).text().strip() if self.local_net_table.item(row, 2) else "",
            "mode": self.local_net_table.item(row, 3).text().strip() if self.local_net_table.item(row, 3) else "",
            "target": self.local_net_table.item(row, 4).text().strip() if self.local_net_table.item(row, 4) else "",
            "notes": self.local_net_table.item(row, 5).text().strip() if self.local_net_table.item(row, 5) else "",
        }

    def _open_local_net_profile_dialog(self, existing: Optional[Dict[str, str]] = None) -> Optional[Dict[str, str]]:
        dlg = QDialog(self)
        dlg.setWindowTitle("Edit Local Net Profile" if existing else "Add Local Net Profile")
        form = QFormLayout(dlg)

        name_edit = QLineEdit((existing or {}).get("name", ""))
        service_combo = QComboBox()
        service_combo.addItems(LOCAL_NET_SERVICE_OPTIONS)
        if existing and service_combo.findText((existing or {}).get("service", "")) >= 0:
            service_combo.setCurrentText((existing or {}).get("service", ""))
        mode_combo = QComboBox()
        mode_combo.setEditable(True)
        mode_combo.addItems(["Voice", "Data", "Mixed"])
        if existing and (existing or {}).get("mode"):
            mode_combo.setCurrentText((existing or {}).get("mode", ""))
        target_edit = QLineEdit((existing or {}).get("target", ""))
        target_edit.setPlaceholderText("e.g., 146.520, Ch 16, or repeater pair/tone")
        notes_edit = QLineEdit((existing or {}).get("notes", ""))
        notes_edit.setPlaceholderText("Optional notes for SOP reminder context")

        form.addRow("Profile Name:", name_edit)
        form.addRow("Service:", service_combo)
        form.addRow("Mode:", mode_combo)
        form.addRow("Target:", target_edit)
        form.addRow("Notes:", notes_edit)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        out: Dict[str, str] = {}

        def _accept() -> None:
            candidate = self._normalize_local_net_profile(
                {
                    "name": name_edit.text(),
                    "service": service_combo.currentText(),
                    "mode": mode_combo.currentText(),
                    "target": target_edit.text(),
                    "notes": notes_edit.text(),
                }
            )
            if not candidate.get("name"):
                QMessageBox.warning(self, "Validation", "Profile Name is required.")
                return
            out.update(candidate)
            dlg.accept()

        ok_btn.clicked.connect(_accept)
        cancel_btn.clicked.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out if out else None

    def _upsert_local_net_profile(self, profile: Dict[str, str], old_name: str = "") -> None:
        normalized = self._normalize_local_net_profile(profile)
        new_key = normalized.get("name", "").strip().upper()
        old_key = (old_name or "").strip().upper()
        if not new_key:
            return
        self.local_net_profiles = [
            r
            for r in self.local_net_profiles
            if str(r.get("name", "")).strip().upper() not in {new_key, old_key}
        ]
        self.local_net_profiles.append(normalized)
        self._refresh_local_net_profiles_table()
        try:
            # Persist Local Net Profiles directly so this workflow is not blocked
            # by unrelated full-settings validation requirements.
            if hasattr(self.settings, "set"):
                self.settings.set("local_net_profiles", self._table_to_local_net_profiles())
            elif hasattr(self.settings, "_data"):
                self.settings._data["local_net_profiles"] = self._table_to_local_net_profiles()  # type: ignore[attr-defined]
            self._settings_dirty = False
            self._set_save_button_state("success")
            try:
                self.local_net_profiles_changed.emit()
            except Exception:
                pass
        except Exception:
            log.exception("Failed to persist Local Net Profile; will remain in-memory only.")

    def _add_local_net_profile(self) -> None:
        created = self._open_local_net_profile_dialog(existing=None)
        if not created:
            return
        self._upsert_local_net_profile(created)

    def _edit_local_net_profile(self) -> None:
        rows = self._selected_local_net_rows()
        if not rows:
            QMessageBox.information(self, "Edit Profile", "Select one Local Net Profile to edit.")
            return
        if len(rows) > 1:
            QMessageBox.warning(self, "Edit Profile", "Please select only one Local Net Profile to edit.")
            return
        row = rows[0]
        existing = self._local_profile_from_row(row)
        updated = self._open_local_net_profile_dialog(existing=existing)
        if not updated:
            return
        self._upsert_local_net_profile(updated, old_name=existing.get("name", ""))

    def _delete_local_net_profiles(self) -> None:
        rows = self._selected_local_net_rows()
        if not rows:
            QMessageBox.information(self, "Delete Profiles", "Select one or more Local Net Profiles to delete.")
            return
        to_remove: set[str] = set()
        for r in rows:
            name = self.local_net_table.item(r, 1).text().strip() if self.local_net_table.item(r, 1) else ""
            if name:
                to_remove.add(name.upper())
        if not to_remove:
            return
        self.local_net_profiles = [
            row for row in self.local_net_profiles if str(row.get("name", "")).strip().upper() not in to_remove
        ]
        self._refresh_local_net_profiles_table()
        try:
            # Persist Local Net Profiles directly so this workflow is not blocked
            # by unrelated full-settings validation requirements.
            if hasattr(self.settings, "set"):
                self.settings.set("local_net_profiles", self._table_to_local_net_profiles())
            elif hasattr(self.settings, "_data"):
                self.settings._data["local_net_profiles"] = self._table_to_local_net_profiles()  # type: ignore[attr-defined]
            self._settings_dirty = False
            self._set_save_button_state("success")
            try:
                self.local_net_profiles_changed.emit()
            except Exception:
                pass
        except Exception:
            log.exception("Failed to persist Local Net Profile deletions; will remain in-memory only.")
        QMessageBox.information(self, "Delete Profiles", f"Deleted {len(to_remove)} Local Net Profile(s).")

    # ---------- GPG authenticity ---------- #

    def _current_gpg_path(self) -> str:
        return self.gpg_path_edit.text().strip() if hasattr(self, "gpg_path_edit") else ""

    def _set_gpg_status(self, text: str, *, error: bool = False) -> None:
        if not hasattr(self, "gpg_status_label"):
            return
        self.gpg_status_label.setText(str(text or "").strip() or ("GPG status: error" if error else "GPG status: ready"))

    def _refresh_gpg_keys_table(self, *, show_dialog_on_error: bool = True) -> None:
        if not hasattr(self, "gpg_keys_table"):
            return
        configured = self._current_gpg_path()
        ok, msg, resolved = gpg_available(configured)
        if not ok:
            self._set_gpg_status(f"GPG unavailable: {msg}", error=True)
            self._gpg_keys_table_loading = True
            try:
                self.gpg_keys_table.setRowCount(0)
            finally:
                self._gpg_keys_table_loading = False
            self._update_gpg_sign_button_state()
            if show_dialog_on_error:
                QMessageBox.warning(
                    self,
                    "GPG",
                    f"{msg}\n\nInstall GPG or set the executable path in Settings.",
                )
            return

        if resolved:
            self._set_gpg_status(f"GPG ready: {resolved}")
        else:
            self._set_gpg_status("GPG ready.")
        keys, err = list_public_keys(configured_path=configured)
        if err:
            self._set_gpg_status(f"GPG key list failed: {err}", error=True)
            if show_dialog_on_error:
                QMessageBox.warning(self, "GPG", err)
            return
        self._gpg_keys_table_loading = True
        try:
            self.gpg_keys_table.setRowCount(0)
            for row_idx, key in enumerate(keys):
                self.gpg_keys_table.insertRow(row_idx)
                fpr = normalize_fingerprint(key.fingerprint)
                trusted = fpr in self._gpg_trusted_fingerprints
                trusted_item = QTableWidgetItem("")
                trusted_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
                trusted_item.setCheckState(Qt.Checked if trusted else Qt.Unchecked)
                trusted_item.setData(Qt.UserRole, fpr)
                self.gpg_keys_table.setItem(row_idx, 0, trusted_item)

                fpr_item = QTableWidgetItem(fpr)
                fpr_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.gpg_keys_table.setItem(row_idx, 1, fpr_item)

                uid_text = "; ".join([u for u in key.user_ids if str(u).strip()]) or "(no user id)"
                uid_item = QTableWidgetItem(uid_text)
                uid_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.gpg_keys_table.setItem(row_idx, 2, uid_item)
        finally:
            self._gpg_keys_table_loading = False
        self._update_gpg_sign_button_state()

    def _on_gpg_keys_table_item_changed(self, item: QTableWidgetItem) -> None:
        if self._gpg_keys_table_loading:
            return
        if item is None or item.column() != 0:
            return
        fpr = normalize_fingerprint(str(item.data(Qt.UserRole) or ""))
        if not fpr and item.row() >= 0 and hasattr(self, "gpg_keys_table"):
            cell = self.gpg_keys_table.item(item.row(), 1)
            fpr = normalize_fingerprint(cell.text() if cell else "")
        if not fpr:
            return
        if item.checkState() == Qt.Checked:
            self._gpg_trusted_fingerprints.add(fpr)
        else:
            self._gpg_trusted_fingerprints.discard(fpr)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _update_gpg_sign_button_state(self) -> None:
        if not hasattr(self, "gpg_sign_key_btn"):
            return
        self.gpg_sign_key_btn.setEnabled(bool(self._selected_gpg_fingerprint()))

    def _selected_gpg_fingerprint(self) -> str:
        if not hasattr(self, "gpg_keys_table"):
            return ""
        row = self.gpg_keys_table.currentRow()
        if row < 0:
            return ""
        item = self.gpg_keys_table.item(row, 1)
        return normalize_fingerprint(item.text() if item else "")

    def _choose_gpg_executable_path(self) -> None:
        start = self._current_gpg_path()
        fn, _ = QFileDialog.getOpenFileName(self, "Select GPG executable", start)
        if not fn:
            return
        self.gpg_path_edit.setText(fn)
        self._mark_settings_dirty()

    def _test_gpg_executable(self) -> None:
        ok, msg, resolved = gpg_available(self._current_gpg_path())
        if ok:
            detail = msg
            if resolved:
                detail = f"{msg}\nPath: {resolved}"
            self._set_gpg_status(f"GPG ready: {resolved or msg}")
            QMessageBox.information(self, "GPG", detail)
            return
        self._set_gpg_status(f"GPG unavailable: {msg}", error=True)
        QMessageBox.warning(self, "GPG", msg)

    def _import_gpg_key_file(self) -> None:
        fn, _ = QFileDialog.getOpenFileName(
            self,
            "Import GPG public key",
            "",
            "Key Files (*.asc *.pgp *.gpg *.key *.txt);;All Files (*)",
        )
        if not fn:
            return
        ok, msg = import_public_key_file(fn, configured_path=self._current_gpg_path())
        if not ok:
            self._set_gpg_status(f"Key import failed: {msg}", error=True)
            QMessageBox.warning(self, "GPG Import", msg)
            return
        self._set_gpg_status("Public key imported.")
        self._refresh_gpg_keys_table(show_dialog_on_error=False)
        self._mark_settings_dirty()
        QMessageBox.information(self, "GPG Import", "Public key imported successfully.")

    def _import_gpg_key_text(self) -> None:
        dlg = QDialog(self)
        dlg.setWindowTitle("Import Armored GPG Key")
        dlg.resize(720, 460)
        layout = QVBoxLayout(dlg)
        info = QLabel("Paste an armored public key block.")
        layout.addWidget(info)
        text_edit = QTextEdit()
        text_edit.setPlaceholderText("-----BEGIN PGP PUBLIC KEY BLOCK-----")
        layout.addWidget(text_edit, 1)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        ok_btn = btns.button(QDialogButtonBox.Ok)
        if ok_btn is not None:
            ok_btn.setText("Import Key")
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)
        if dlg.exec() != QDialog.Accepted:
            return
        payload = text_edit.toPlainText().strip()
        if not payload:
            QMessageBox.warning(self, "GPG Import", "No key text provided.")
            return
        ok, msg = import_public_key_text(payload, configured_path=self._current_gpg_path())
        if not ok:
            self._set_gpg_status(f"Key import failed: {msg}", error=True)
            QMessageBox.warning(self, "GPG Import", msg)
            return
        self._set_gpg_status("Public key imported.")
        self._refresh_gpg_keys_table(show_dialog_on_error=False)
        self._mark_settings_dirty()
        QMessageBox.information(self, "GPG Import", "Public key imported successfully.")

    def _local_sign_selected_gpg_key(self) -> None:
        fpr = self._selected_gpg_fingerprint()
        if not fpr:
            QMessageBox.information(self, "GPG", "Select one key to local-sign.")
            return
        resp = QMessageBox.question(
            self,
            "Local-Sign Key",
            "This will run GPG local-sign for the selected key.\nContinue?",
        )
        if resp != QMessageBox.Yes:
            return
        ok, msg = local_sign_key(fpr, configured_path=self._current_gpg_path())
        if not ok:
            self._set_gpg_status(f"Local-sign failed: {msg}", error=True)
            QMessageBox.warning(self, "GPG", msg)
            return
        self._set_gpg_status("Key local-sign complete.")
        self._refresh_gpg_keys_table(show_dialog_on_error=False)
        QMessageBox.information(self, "GPG", "Key local-sign completed.")

    def _selected_hash_algo(self) -> str:
        if not hasattr(self, "trusted_hash_algo_combo"):
            return ""
        txt = str(self.trusted_hash_algo_combo.currentText() or "").strip().lower()
        if txt == "auto":
            return ""
        return normalize_hash_algorithm(txt)

    def _normalize_single_hash_entry(self, hash_value: str, algorithm: str = "", label: str = "", enabled: bool = True) -> dict | None:
        hash_norm = normalize_hash_hex(hash_value)
        if not hash_norm:
            return None
        algo = normalize_hash_algorithm(algorithm) or infer_algorithm_from_hash(hash_norm)
        if not algo:
            return None
        return {
            "enabled": bool(enabled),
            "algorithm": algo,
            "hash": hash_norm,
            "label": str(label or "").strip(),
        }

    def _refresh_trusted_hash_table(self) -> None:
        if not hasattr(self, "trusted_hash_table"):
            return
        self._trusted_hash_entries = normalize_trusted_hash_entries(self._trusted_hash_entries)
        self._trusted_hashes_table_loading = True
        try:
            self.trusted_hash_table.setRowCount(0)
            for idx, row in enumerate(self._trusted_hash_entries):
                self.trusted_hash_table.insertRow(idx)
                use_item = QTableWidgetItem("")
                use_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
                use_item.setCheckState(Qt.Checked if bool(row.get("enabled", True)) else Qt.Unchecked)
                self.trusted_hash_table.setItem(idx, 0, use_item)

                algo = str(row.get("algorithm", "") or "").strip().upper()
                if algo == "SHA1":
                    algo = "SHA-1"
                elif algo == "SHA256":
                    algo = "SHA-256"
                elif algo == "SHA512":
                    algo = "SHA-512"
                algo_item = QTableWidgetItem(algo)
                algo_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.trusted_hash_table.setItem(idx, 1, algo_item)

                hash_item = QTableWidgetItem(str(row.get("hash", "") or ""))
                hash_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.trusted_hash_table.setItem(idx, 2, hash_item)

                label_item = QTableWidgetItem(str(row.get("label", "") or ""))
                label_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable)
                self.trusted_hash_table.setItem(idx, 3, label_item)
        finally:
            self._trusted_hashes_table_loading = False
        self._update_trusted_hash_actions()

    def _update_trusted_hash_actions(self) -> None:
        if hasattr(self, "trusted_hash_remove_btn") and hasattr(self, "trusted_hash_table"):
            self.trusted_hash_remove_btn.setEnabled(bool(self.trusted_hash_table.selectionModel().selectedRows()))

    def _on_trusted_hash_table_item_changed(self, item: QTableWidgetItem) -> None:
        if self._trusted_hashes_table_loading:
            return
        if item is None:
            return
        row_idx = int(item.row())
        if row_idx < 0 or row_idx >= len(self._trusted_hash_entries):
            return
        if item.column() == 0:
            self._trusted_hash_entries[row_idx]["enabled"] = bool(item.checkState() == Qt.Checked)
            self._mark_settings_dirty()
            self._refresh_section_titles()
            return
        if item.column() == 3:
            self._trusted_hash_entries[row_idx]["label"] = str(item.text() or "").strip()
            self._mark_settings_dirty()
            self._refresh_section_titles()

    def _add_trusted_hash_entry(self) -> None:
        raw_hash = self.trusted_hash_edit.text().strip() if hasattr(self, "trusted_hash_edit") else ""
        if not raw_hash:
            return
        algo = self._selected_hash_algo()
        label = self.trusted_hash_label_edit.text().strip() if hasattr(self, "trusted_hash_label_edit") else ""
        entry = self._normalize_single_hash_entry(raw_hash, algorithm=algo, label=label, enabled=True)
        if not entry:
            QMessageBox.warning(
                self,
                "Trusted Hash",
                "Invalid hash value. Supported lengths are MD5, SHA-1, SHA-256, and SHA-512.",
            )
            return
        key = (str(entry.get("algorithm", "")), str(entry.get("hash", "")))
        existing_keys = {
            (str(row.get("algorithm", "")), str(row.get("hash", "")))
            for row in self._trusted_hash_entries
            if isinstance(row, dict)
        }
        if key in existing_keys:
            QMessageBox.information(self, "Trusted Hash", "That hash is already stored.")
            return
        self._trusted_hash_entries.append(entry)
        self._refresh_trusted_hash_table()
        self._mark_settings_dirty()
        self._refresh_section_titles()
        self.trusted_hash_edit.clear()
        if hasattr(self, "trusted_hash_label_edit"):
            self.trusted_hash_label_edit.clear()

    @staticmethod
    def _extract_hash_candidates_from_text(text: str) -> List[dict]:
        out: List[dict] = []
        for raw_line in str(text or "").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            algo = ""
            m = re.search(r"(?i)\b(sha-?1|sha-?256|sha-?512|md5)\b", line)
            if m:
                algo = normalize_hash_algorithm(m.group(1))
            hm = re.search(r"\b([A-Fa-f0-9]{32}|[A-Fa-f0-9]{40}|[A-Fa-f0-9]{64}|[A-Fa-f0-9]{128})\b", line)
            if not hm:
                continue
            hash_norm = normalize_hash_hex(hm.group(1))
            if not hash_norm:
                continue
            if not algo:
                algo = infer_algorithm_from_hash(hash_norm)
            if not algo:
                continue
            out.append({"enabled": True, "algorithm": algo, "hash": hash_norm, "label": ""})
        return out

    def _import_trusted_hash_file(self) -> None:
        fn, _ = QFileDialog.getOpenFileName(
            self,
            "Import Trusted Hashes",
            "",
            "Text Files (*.txt *.sha1 *.sha256 *.sha512 *.md5 *.hash *.checksum);;All Files (*)",
        )
        if not fn:
            return
        try:
            text = Path(fn).read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            QMessageBox.warning(self, "Trusted Hash", f"Failed to read file:\n{e}")
            return
        imported = self._extract_hash_candidates_from_text(text)
        if not imported:
            QMessageBox.information(self, "Trusted Hash", "No supported hashes found in that file.")
            return
        existing_keys = {
            (str(row.get("algorithm", "")), str(row.get("hash", "")))
            for row in self._trusted_hash_entries
            if isinstance(row, dict)
        }
        added = 0
        for row in imported:
            key = (str(row.get("algorithm", "")), str(row.get("hash", "")))
            if key in existing_keys:
                continue
            self._trusted_hash_entries.append(row)
            existing_keys.add(key)
            added += 1
        self._refresh_trusted_hash_table()
        if added > 0:
            self._mark_settings_dirty()
            self._refresh_section_titles()
            QMessageBox.information(self, "Trusted Hash", f"Imported {added} hash entr{'y' if added == 1 else 'ies'}.")
        else:
            QMessageBox.information(self, "Trusted Hash", "All hashes from file are already stored.")

    def _remove_selected_trusted_hash_entries(self) -> None:
        if not hasattr(self, "trusted_hash_table"):
            return
        rows = sorted({idx.row() for idx in self.trusted_hash_table.selectionModel().selectedRows()}, reverse=True)
        if not rows:
            return
        for row_idx in rows:
            if 0 <= row_idx < len(self._trusted_hash_entries):
                self._trusted_hash_entries.pop(row_idx)
        self._refresh_trusted_hash_table()
        self._mark_settings_dirty()
        self._refresh_section_titles()

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

    def _choose_js8call_install_path(self):
        start = self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select JS8Call install folder", start)
        if not fn:
            return
        self.js8call_path_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("path_js8call", fn)
        else:
            data = self.settings.all()
            data["path_js8call"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_js8spotter_launch_path(self):
        start = self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else ""
        fn, _ = QFileDialog.getOpenFileName(self, "Select JS8Spotter launch path", start)
        if not fn:
            return
        self.js8spotter_path_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("path_js8spotter", fn)
        else:
            data = self.settings.all()
            data["path_js8spotter"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_commstat_launch_path(self):
        start = self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else ""
        fn, _ = QFileDialog.getOpenFileName(self, "Select CommStat launch path", start)
        if not fn:
            return
        self.commstat_path_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("path_commstat", fn)
        else:
            data = self.settings.all()
            data["path_commstat"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
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

    def _choose_varac_bbs_dir(self):
        start = self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC BBS directory", start)
        if not fn:
            return
        self.varac_bbs_dir_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("varac_bbs_dir", fn)
        else:
            data = self.settings.all()
            data["varac_bbs_dir"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_varac_bbs_archive_dir(self):
        start = self.varac_bbs_archive_dir_edit.text().strip() if hasattr(self, "varac_bbs_archive_dir_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC BBS archive directory", start)
        if not fn:
            return
        self.varac_bbs_archive_dir_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("varac_bbs_archive_dir", fn)
        else:
            data = self.settings.all()
            data["varac_bbs_archive_dir"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
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

    def _choose_fldigi_log_path(self):
        start = self.fldigi_log_path_edit.text().strip() if hasattr(self, "fldigi_log_path_edit") else ""
        start_dir = str(Path(start)) if start else ""
        fn = QFileDialog.getExistingDirectory(
            self,
            "Select FLDigi log folder",
            start_dir,
        )
        if not fn:
            return
        if hasattr(self, "fldigi_log_path_edit"):
            self.fldigi_log_path_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("fldigi_log_path", fn)
        else:
            data = self.settings.all()
            data["fldigi_log_path"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

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
        Manually rebuild JS8 link traffic from DIRECTED.TXT and ALL.TXT.
        This is intentionally a full reload, not incremental.
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
            # Force a true full reload so swapped/replaced logs are fully re-read.
            self.settings.set_many(
                {
                    "js8_links_directed_offset": 0,
                    "js8_links_all_offset": 0,
                    "js8_links_last_load_utc": 0,
                }
            )
            conn = sqlite3.connect(db_path)
            try:
                indexer._ensure_table(conn)
                indexer._clear_table(conn)
            finally:
                conn.close()
            count = int(indexer.update(since_ts=0) or 0)
            latest_ts = float(indexer._ensure_latest_ts(last_default=0.0) or 0.0)
            self.settings.set("js8_links_last_load_utc", latest_ts)
            QMessageBox.information(
                self,
                "JS8 Traffic Loaded",
                f"JS8 logs rebuilt successfully ({count} link rows loaded).",
            )
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
