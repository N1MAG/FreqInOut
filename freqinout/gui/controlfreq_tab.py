from __future__ import annotations

import datetime as dt
import json
import math
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFontMetrics, QShortcut, QKeySequence, QColor
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QComboBox,
    QPushButton,
    QGroupBox,
    QTableWidget,
    QTableWidgetItem,
    QSizePolicy,
    QMessageBox,
    QHeaderView,
    QSplitter,
    QAbstractItemView,
    QMenu,
)

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log
from freqinout.core.propagation_service import PropagationService
from freqinout.core.software_status_service import SoftwareStatusService, PROGRAM_PATH_KEYS
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sop_manager import SOPManager
from freqinout.utils.timezones import get_timezone
from freqinout.gui.qsy_helper import (
    load_operating_groups,
    selected_qsy_meta,
    perform_qsy,
    current_scheduler_freq,
)
from freqinout.gui.stations_map_tab import (
    FEMA_REGIONS,
    LOWER48_STATES,
    PROP_BANDS,
    PROP_DEFAULT_PROFILES,
    STATE_CENTERS,
    STATE_TO_FEMA_REGION,
    US_STATE_ABBR_FROM_NAME,
    maidenhead_to_latlon,
)
from freqinout.gui.theme import resolve_theme, button_style, led_style


class ControlFreqTab(QWidget):
    """
    ControlFreq: summary/console view for activity and operational status.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._timer: Optional[QTimer] = None
        self._active = False
        self._last_refresh_ts = 0.0
        self._freq_timer: Optional[QTimer] = None
        self._status_timer: Optional[QTimer] = None
        self._show_local = True
        self._intersection_cache_ts = 0.0
        self._intersection_cache_key: Tuple[str, str] = ("", "")
        self._intersection_cache_rows: List[List[str]] = []
        self._prop_target_syncing = False
        self._prop_operator_geo: Dict[str, Dict[str, str]] = {}
        self._focus_mode = True
        self._freq_ctrl_fixed_height = 0
        self._pending_group_filter = ""
        self._saved_top_sizes: List[int] = []
        self._saved_left_sizes: List[int] = []
        self._saved_right_sizes: List[int] = []
        self._schedule_entries_by_row: Dict[int, Dict[str, Any]] = {}
        self.status_labels: Dict[str, QLabel] = {}
        self._status_checked_at: Dict[str, str] = {}
        self._status_service = SoftwareStatusService(self.settings)
        self._prop_service = PropagationService(
            default_profiles=PROP_DEFAULT_PROFILES,
            profiles_path=None,
            climatology_db_path=get_config_dir() / "config" / "propagation" / "prop_climatology.db",
            outcome_db_path=get_config_dir() / "config" / "freqinout_nets.db",
            db_index_mode="round_halfdeg",
        )
        self._prefs_timer = QTimer(self)
        self._prefs_timer.setSingleShot(True)
        self._prefs_timer.timeout.connect(self._persist_ui_state)
        self._build_ui()
        self._restore_ui_state()
        self._apply_theme()
        self._refresh_all()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel("<h3>ControlFreq</h3>")
        header.addWidget(title)

        header.addStretch(1)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter by keyword...")
        self.search_edit.textChanged.connect(self._refresh_all)
        self.search_edit.textChanged.connect(self._schedule_persist_ui_state)
        self.search_edit.setMinimumWidth(340)
        self.search_edit.setMaximumWidth(420)
        header.addWidget(self.search_edit)

        self.group_combo = QComboBox()
        self.group_combo.setMinimumWidth(180)
        self.group_combo.currentIndexChanged.connect(self._refresh_all)
        self.group_combo.currentIndexChanged.connect(self._schedule_persist_ui_state)
        header.addWidget(self.group_combo)

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self._refresh_all)
        header.addWidget(self.refresh_btn)

        self.clear_filters_btn = QPushButton("Clear Filters")
        self.clear_filters_btn.clicked.connect(self._clear_filters)
        header.addWidget(self.clear_filters_btn)

        self.time_toggle_btn = QPushButton("Showing: Local")
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        header.addWidget(self.time_toggle_btn)

        self.focus_mode_btn = QPushButton("Focus Mode: Off")
        self.focus_mode_btn.clicked.connect(self._toggle_focus_mode)
        header.addWidget(self.focus_mode_btn)

        root.addLayout(header)

        updated_row = QHBoxLayout()

        self.status_group = QGroupBox("Operating Status")
        status_layout = QHBoxLayout()
        self.status_group.setLayout(status_layout)
        theme = resolve_theme(self.settings)
        status_items = [
            ("FLRig", "FLRig"),
            ("FLDigi", "FLDigi"),
            ("FLMsg", "FLMsg"),
            ("FLAmp", "FLAmp"),
            ("JS8Call_API", "JS8"),
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
        status_layout.addStretch(1)
        updated_row.addWidget(self.status_group, 3)
        updated_row.addStretch(1)
        self.updated_label = QLabel("Last updated: --")
        self.updated_label.setStyleSheet("color: #888;")
        updated_row.addWidget(self.updated_label)
        root.addLayout(updated_row)

        # Top region: left = Activity/Intersections/Messages, right = Frequency/Schedule Outlook
        self.top_splitter = QSplitter(Qt.Horizontal)
        self.top_splitter.setChildrenCollapsible(False)

        self.left_col = QWidget()
        left_layout = QVBoxLayout(self.left_col)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)

        self.activity_box = QGroupBox("Activity")
        act_layout = QVBoxLayout(self.activity_box)
        act_header = QHBoxLayout()
        act_header.addWidget(QLabel("Window"))
        self.activity_window_combo = QComboBox()
        self.activity_window_combo.addItem("30m", 30)
        self.activity_window_combo.addItem("1h", 60)
        self.activity_window_combo.addItem("2h", 120)
        self.activity_window_combo.addItem("6h", 360)
        self.activity_window_combo.addItem("12h", 720)
        self.activity_window_combo.addItem("24h", 1440)
        self.activity_window_combo.setCurrentIndex(2)
        self.activity_window_combo.currentIndexChanged.connect(self._refresh_activity)
        self.activity_window_combo.currentIndexChanged.connect(self._schedule_persist_ui_state)
        act_header.addWidget(self.activity_window_combo)
        act_header.addStretch(1)
        act_layout.addLayout(act_header)
        self.activity_table = QTableWidget(0, 4)
        self.activity_table.setHorizontalHeaderLabels(
            ["Group", "Band/Freq", "Callsigns Seen", "Traffic"]
        )
        self._setup_table_defaults(self.activity_table)
        self.activity_table.horizontalHeader().setStretchLastSection(True)
        act_layout.addWidget(self.activity_table)

        self.intersection_box = QGroupBox("Schedule Intersections")
        intersection_layout = QVBoxLayout(self.intersection_box)
        inter_header_row = QHBoxLayout()
        self.intersection_label = QLabel("Now +2h")
        self.intersection_label.setStyleSheet("font-weight: bold;")
        inter_header_row.addWidget(self.intersection_label)
        self.intersection_info = QLabel("?")
        self.intersection_info.setToolTip(
            "Exact-frequency overlaps between your schedule and peer schedules\n"
            "for now and the next two hours."
        )
        self.intersection_info.setStyleSheet(
            "font-weight: bold; border: 1px solid #888; border-radius: 8px; padding: 0 4px;"
        )
        inter_header_row.addWidget(self.intersection_info)
        inter_header_row.addStretch(1)
        intersection_layout.addLayout(inter_header_row)
        self.intersection_table = QTableWidget(0, 3)
        self.intersection_table.setHorizontalHeaderLabels(["When", "Overlaps", "Group/Band/Freq"])
        self._setup_table_defaults(self.intersection_table)
        inter_header = self.intersection_table.horizontalHeader()
        inter_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        inter_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        inter_header.setSectionResizeMode(2, QHeaderView.Stretch)
        intersection_layout.addWidget(self.intersection_table)

        self.inbox_box = QGroupBox("Message Summary")
        inbox_layout = QVBoxLayout(self.inbox_box)
        self.inbox_table = QTableWidget(0, 3)
        self.inbox_table.setHorizontalHeaderLabels(["Type", "Count", "Details / BBS Aging Out"])
        self._setup_table_defaults(self.inbox_table)
        inbox_header = self.inbox_table.horizontalHeader()
        inbox_header.setStretchLastSection(True)
        inbox_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        inbox_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        inbox_header.setSectionResizeMode(2, QHeaderView.Stretch)
        inbox_layout.addWidget(self.inbox_table)

        self.left_splitter = QSplitter(Qt.Vertical)
        self.left_splitter.setChildrenCollapsible(False)
        self.left_splitter.addWidget(self.intersection_box)
        left_layout.addWidget(self.left_splitter)
        self.top_splitter.addWidget(self.left_col)

        self.right_col = QWidget()
        right_layout = QVBoxLayout(self.right_col)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        self.freq_ctrl_box = QGroupBox("Frequency Control")
        freq_layout = QVBoxLayout(self.freq_ctrl_box)
        self.freq_ctrl_label = QLabel("Scheduled: --")
        self.freq_ctrl_label.setWordWrap(True)
        self.freq_ctrl_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.freq_ctrl_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        freq_layout.addWidget(self.freq_ctrl_label)
        self.freq_active_label = QLabel("Active: --")
        self.freq_active_label.setWordWrap(True)
        self.freq_active_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.freq_active_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        freq_layout.addWidget(self.freq_active_label)
        status_row = QHBoxLayout()
        self.now_status_label = QLabel("Status: --")
        self.now_status_label.setStyleSheet("font-weight: 500;")
        status_row.addWidget(self.now_status_label)
        self.next_change_label = QLabel("Next: --")
        self.next_change_label.setStyleSheet("color: #888;")
        status_row.addWidget(self.next_change_label)
        self.suspend_label = QLabel("Suspend: --")
        self.suspend_label.setStyleSheet("color: #888;")
        status_row.addWidget(self.suspend_label)
        status_row.addStretch(1)
        freq_layout.addLayout(status_row)
        self.freq_combo = QComboBox()
        self.freq_combo.setMinimumWidth(180)
        self.freq_combo.setMaximumWidth(260)
        self.freq_combo.currentIndexChanged.connect(self._on_freq_selection_changed)
        freq_layout.addWidget(self.freq_combo)
        btn_row = QHBoxLayout()
        self.freq_set_btn = QPushButton("Set Frequency")
        self.freq_set_btn.clicked.connect(self._on_freq_set_clicked)
        self.freq_set_btn.setMinimumHeight(26)
        self.freq_set_btn.setMaximumWidth(140)
        btn_row.addWidget(self.freq_set_btn)
        self.freq_resume_btn = QPushButton("Resume Schedule")
        self.freq_resume_btn.clicked.connect(self._on_resume_schedule_clicked)
        self.freq_resume_btn.setMinimumHeight(26)
        self.freq_resume_btn.setMaximumWidth(160)
        btn_row.addWidget(self.freq_resume_btn)
        btn_row.addStretch(1)
        freq_layout.addLayout(btn_row)

        top_overview_row = QHBoxLayout()
        top_overview_row.addWidget(self.freq_ctrl_box, 2)
        top_overview_row.addWidget(self.activity_box, 3)
        top_overview_row.addWidget(self.inbox_box, 3)
        root.addLayout(top_overview_row)
        self._lock_frequency_control_height()

        self.schedule_box = QGroupBox("Schedule Outlook")
        schedule_layout = QVBoxLayout(self.schedule_box)
        self.schedule_table = QTableWidget(0, 5)
        self.schedule_table.setHorizontalHeaderLabels(["When/Day", "Type", "Group/Net", "Band/Freq", "Actions"])
        self._setup_table_defaults(self.schedule_table)
        self.schedule_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.schedule_table.customContextMenuRequested.connect(self._on_schedule_context_menu)
        sched_header = self.schedule_table.horizontalHeader()
        sched_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        sched_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        sched_header.setSectionResizeMode(2, QHeaderView.Stretch)
        sched_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        sched_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        schedule_layout.addWidget(self.schedule_table)
        self.schedule_action_hint = QLabel("Tip: use buttons in Actions, or right-click a row for actions.")
        self.schedule_action_hint.setWordWrap(True)
        schedule_layout.addWidget(self.schedule_action_hint)

        self.right_splitter = QSplitter(Qt.Vertical)
        self.right_splitter.setChildrenCollapsible(False)
        self.right_splitter.addWidget(self.schedule_box)
        right_layout.addWidget(self.right_splitter)
        self.top_splitter.addWidget(self.right_col)
        self.top_splitter.setStretchFactor(0, 1)
        self.top_splitter.setStretchFactor(1, 1)
        self.left_splitter.setSizes([1])
        self.right_splitter.setSizes([1])
        self.top_splitter.setSizes([480, 560])
        root.addWidget(self.top_splitter, 5)

        # Bottom region: full-width propagation forecast
        row2 = QHBoxLayout()

        self.prop_box = QGroupBox("Propagation Forecast")
        prop_layout = QVBoxLayout(self.prop_box)
        target_row = QHBoxLayout()
        target_row.addWidget(QLabel("Target"))
        self.prop_target_type_combo = QComboBox()
        self.prop_target_type_combo.addItem("Region", "REGION")
        self.prop_target_type_combo.addItem("State", "STATE")
        self.prop_target_type_combo.addItem("Operator", "OPERATOR")
        self.prop_target_type_combo.currentIndexChanged.connect(self._on_prop_target_type_changed)
        target_row.addWidget(self.prop_target_type_combo)
        self.prop_target_value_combo = QComboBox()
        self.prop_target_value_combo.setEditable(True)
        self.prop_target_value_combo.setInsertPolicy(QComboBox.NoInsert)
        self.prop_target_value_combo.setDuplicatesEnabled(False)
        self.prop_target_value_combo.currentTextChanged.connect(self._on_prop_target_value_changed)
        target_row.addWidget(self.prop_target_value_combo, 1)
        prop_layout.addLayout(target_row)
        self.prop_table = QTableWidget(0, 4)
        self.prop_table.setHorizontalHeaderLabels(
            ["Zone", "Morning (Dawn-10:00)", "Day (10:00-Sunset)", "Night (Sunset-Dawn)"]
        )
        self._setup_table_defaults(self.prop_table)
        self.prop_table.horizontalHeader().setStretchLastSection(True)
        self.prop_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        prop_layout.addWidget(self.prop_table)
        self.prop_hint = QLabel(
            "Modeled snapshot based on today's schedule bands. "
            "Morning = dawn-10:00, Day = 10:00-sunset, Night = sunset-dawn (local)."
        )
        self.prop_hint.setWordWrap(True)
        self.prop_hint.setStyleSheet("color: #666;")
        prop_layout.addWidget(self.prop_hint)
        row2.addWidget(self.prop_box, 1)
        root.addLayout(row2, 2)

        # Save user-resized layout proportions.
        self.top_splitter.splitterMoved.connect(self._schedule_persist_ui_state)
        self.left_splitter.splitterMoved.connect(self._schedule_persist_ui_state)
        self.right_splitter.splitterMoved.connect(self._schedule_persist_ui_state)

        # Keyboard shortcuts
        self.shortcut_refresh = QShortcut(QKeySequence("Ctrl+R"), self)
        self.shortcut_refresh.activated.connect(self._refresh_all)
        self.shortcut_set_frequency = QShortcut(QKeySequence("Ctrl+Return"), self)
        self.shortcut_set_frequency.activated.connect(self._on_freq_set_clicked)
        self.shortcut_resume_schedule = QShortcut(QKeySequence("Ctrl+Shift+R"), self)
        self.shortcut_resume_schedule.activated.connect(self._on_resume_schedule_clicked)
        self.refresh_btn.setToolTip("Refresh (Ctrl+R)")
        self.freq_set_btn.setToolTip("Set Frequency (Ctrl+Enter)")
        self.freq_resume_btn.setToolTip("Resume Schedule (Ctrl+Shift+R)")

    @staticmethod
    def _setup_table_defaults(table: QTableWidget) -> None:
        table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        table.setAlternatingRowColors(True)
        table.setWordWrap(False)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        vh = table.verticalHeader()
        vh.setVisible(False)
        vh.setDefaultSectionSize(24)
        hh = table.horizontalHeader()
        hh.setSectionsMovable(False)
        hh.setHighlightSections(False)

    def _lock_frequency_control_height(self) -> None:
        try:
            # Recompute from natural content height, then lock to keep stable size across modes.
            self.freq_ctrl_box.setMinimumHeight(0)
            self.freq_ctrl_box.setMaximumHeight(16777215)
            height = max(140, int(self.freq_ctrl_box.sizeHint().height()))
            self._freq_ctrl_fixed_height = height
            self.freq_ctrl_box.setMinimumHeight(height)
            self.freq_ctrl_box.setMaximumHeight(height)
            self._sync_top_panel_heights()
        except Exception:
            pass

    def _sync_top_panel_heights(self) -> None:
        try:
            h = int(self._freq_ctrl_fixed_height or 0)
            if h <= 0:
                h = max(140, int(self.freq_ctrl_box.sizeHint().height()))
            if self._focus_mode:
                self.inbox_box.setMinimumHeight(h)
                self.inbox_box.setMaximumHeight(h)
            else:
                self.activity_box.setMinimumHeight(h)
                self.activity_box.setMaximumHeight(h)
                self.inbox_box.setMinimumHeight(h)
                self.inbox_box.setMaximumHeight(h)
        except Exception:
            pass

    def _schedule_persist_ui_state(self, *_args) -> None:
        if hasattr(self, "_prefs_timer"):
            self._prefs_timer.start(300)

    def _persist_ui_state(self) -> None:
        try:
            values = {
                "controlfreq_show_local": bool(self._show_local),
                "controlfreq_focus_mode": bool(self._focus_mode),
                "controlfreq_search": (self.search_edit.text() or "").strip(),
                "controlfreq_group_filter": (self.group_combo.currentData() or "").strip().upper(),
                "controlfreq_activity_window_min": int(self.activity_window_combo.currentData() or 120),
                "controlfreq_top_splitter_sizes": (
                    list(self._saved_top_sizes)
                    if self._focus_mode and self._saved_top_sizes
                    else list(self.top_splitter.sizes())
                ),
                "controlfreq_left_splitter_sizes": list(self.left_splitter.sizes()),
                "controlfreq_right_splitter_sizes": list(self.right_splitter.sizes()),
            }
            self.settings.set_many(values)
        except Exception as e:
            log.debug("ControlFreq: failed to persist UI state: %s", e)

    def _restore_ui_state(self) -> None:
        try:
            self._show_local = bool(self.settings.get("controlfreq_show_local", self._show_local))
            # Default Focus Mode to on when opening ControlFreq.
            self._focus_mode = True
            self._pending_group_filter = (
                str(self.settings.get("controlfreq_group_filter", "") or "").strip().upper()
            )
            saved_search = str(self.settings.get("controlfreq_search", "") or "").strip()
            if saved_search:
                self.search_edit.blockSignals(True)
                self.search_edit.setText(saved_search)
                self.search_edit.blockSignals(False)
            saved_window = int(self.settings.get("controlfreq_activity_window_min", 120) or 120)
            idx = self.activity_window_combo.findData(saved_window)
            if idx >= 0:
                self.activity_window_combo.blockSignals(True)
                self.activity_window_combo.setCurrentIndex(idx)
                self.activity_window_combo.blockSignals(False)
            self._saved_top_sizes = list(self.settings.get("controlfreq_top_splitter_sizes", []) or [])
            self._saved_left_sizes = list(self.settings.get("controlfreq_left_splitter_sizes", []) or [])
            self._saved_right_sizes = list(self.settings.get("controlfreq_right_splitter_sizes", []) or [])
            self._apply_focus_mode()
            QTimer.singleShot(0, self._apply_saved_splitter_sizes)
        except Exception as e:
            log.debug("ControlFreq: failed to restore UI state: %s", e)

    def _apply_saved_splitter_sizes(self) -> None:
        try:
            if len(self._saved_top_sizes) == self.top_splitter.count():
                self.top_splitter.setSizes([max(1, int(v)) for v in self._saved_top_sizes])
            if len(self._saved_left_sizes) == self.left_splitter.count():
                self.left_splitter.setSizes([max(1, int(v)) for v in self._saved_left_sizes])
            if len(self._saved_right_sizes) == self.right_splitter.count():
                self.right_splitter.setSizes([max(1, int(v)) for v in self._saved_right_sizes])
        except Exception as e:
            log.debug("ControlFreq: failed to apply saved splitter sizes: %s", e)

    def _toggle_focus_mode(self) -> None:
        self._focus_mode = not self._focus_mode
        self._apply_focus_mode()
        self._schedule_persist_ui_state()

    def _apply_focus_mode(self) -> None:
        active = bool(self._focus_mode)
        self.focus_mode_btn.setText("Focus Mode: On" if active else "Focus Mode: Off")
        try:
            theme = resolve_theme(self.settings)
            self.focus_mode_btn.setStyleSheet(
                button_style("info" if active else "secondary", theme)
            )
        except Exception:
            pass
        # Keep operating-status LEDs visible in both focus states.
        self.status_group.setVisible(True)
        self.activity_box.setVisible(not active)
        self.inbox_box.setVisible(True)
        self.left_col.setVisible(not active)
        if active:
            current_sizes = list(self.top_splitter.sizes())
            if len(current_sizes) == self.top_splitter.count() and current_sizes[0] > 0:
                self._saved_top_sizes = current_sizes
            self.top_splitter.setSizes([0, 1])
        else:
            if self._saved_top_sizes and len(self._saved_top_sizes) == self.top_splitter.count():
                self.top_splitter.setSizes([max(1, int(v)) for v in self._saved_top_sizes])
            else:
                self.top_splitter.setSizes([1, 1])
        self._sync_top_panel_heights()

    @staticmethod
    def _hex_to_rgb(value: str) -> Tuple[int, int, int]:
        text = (value or "").strip().lstrip("#")
        if len(text) != 6:
            return (0, 0, 0)
        try:
            return (int(text[0:2], 16), int(text[2:4], 16), int(text[4:6], 16))
        except Exception:
            return (0, 0, 0)

    def _is_dark_theme(self) -> bool:
        try:
            theme = resolve_theme(self.settings)
            r, g, b = self._hex_to_rgb(str(theme.get("bg", "#FFFFFF")))
            lum = (0.2126 * r) + (0.7152 * g) + (0.0722 * b)
            return lum < 128
        except Exception:
            return False

    def _urgency_palette(self) -> Dict[str, QColor]:
        if self._is_dark_theme():
            return {
                "critical": QColor("#5A2525"),
                "soon": QColor("#5B4420"),
                "upcoming": QColor("#203A5F"),
                "positive": QColor("#1D4A2E"),
                "warn": QColor("#5B4420"),
                "text": QColor("#F2F2F2"),
                "muted_text": QColor("#C8C8C8"),
            }
        return {
            "critical": QColor("#FFE2E2"),
            "soon": QColor("#FFF4D6"),
            "upcoming": QColor("#EAF2FF"),
            "positive": QColor("#EEF7EE"),
            "warn": QColor("#FFF4D6"),
            "text": QColor("#111111"),
            "muted_text": QColor("#555555"),
        }

    def _apply_theme(self) -> None:
        try:
            theme = resolve_theme(self.settings)
            self.refresh_btn.setStyleSheet(button_style("primary", theme))
            self.clear_filters_btn.setStyleSheet(button_style("secondary", theme))
            self.freq_set_btn.setStyleSheet(button_style("secondary", theme))
            self.freq_resume_btn.setStyleSheet(button_style("secondary", theme))
            self.time_toggle_btn.setStyleSheet(button_style("primary", theme))
            self.focus_mode_btn.setStyleSheet(
                button_style("info" if self._focus_mode else "secondary", theme)
            )
            if hasattr(self, "schedule_action_hint"):
                self.schedule_action_hint.setStyleSheet(f"color: {theme.get('text_muted', '#888')};")
        except Exception:
            pass
        self._lock_frequency_control_height()
        self._update_time_toggle_text()
        self._apply_focus_mode()
        self._on_freq_selection_changed()
        self._refresh_running_status()

    def apply_theme(self) -> None:
        self._apply_theme()

    def set_tab_active(self, active: bool) -> None:
        self._active = bool(active)
        if self._active:
            if self._timer is None:
                self._timer = QTimer(self)
                self._timer.timeout.connect(self._refresh_all)
            self._timer.start(60_000)
            if self._freq_timer is None:
                self._freq_timer = QTimer(self)
                self._freq_timer.timeout.connect(self._refresh_frequency_control)
            self._freq_timer.start(2000)
            if self._status_timer is None:
                self._status_timer = QTimer(self)
                self._status_timer.timeout.connect(self._refresh_status_widgets)
            self._status_timer.start(2000)
            self._refresh_frequency_control()
            self._refresh_status_widgets()
            return
        if self._timer:
            self._timer.stop()
        if self._freq_timer:
            self._freq_timer.stop()
        if self._status_timer:
            self._status_timer.stop()

    def on_tab_activated(self) -> None:
        self._refresh_all()
        self._refresh_frequency_control()
        self._refresh_status_widgets()

    def on_settings_saved(self) -> None:
        self._apply_theme()
        self._refresh_all()

    def _update_time_toggle_text(self) -> None:
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")

    def _toggle_time_view(self) -> None:
        self._show_local = not self._show_local
        self._update_time_toggle_text()
        self._schedule_persist_ui_state()
        self._refresh_schedule_outlook()

    def _clear_filters(self) -> None:
        self.search_edit.clear()
        if self.group_combo.count() > 0:
            self.group_combo.setCurrentIndex(0)
        if self.activity_window_combo.count() > 0:
            idx = self.activity_window_combo.findData(120)
            self.activity_window_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self._schedule_persist_ui_state()
        self._refresh_all()

    def _refresh_all(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        self._refresh_frequency_control()
        self._load_group_combo()
        self._refresh_activity()
        self._refresh_schedule_outlook()
        self._refresh_message_summary()
        self._refresh_status_widgets()
        self._refresh_prop_target_controls()
        self._refresh_propagation_snapshot()
        self._last_refresh_ts = time.time()
        ts = dt.datetime.fromtimestamp(self._last_refresh_ts).strftime("%Y-%m-%d %H:%M:%S")
        self.updated_label.setText(f"Last updated: {ts}")

    def _refresh_status_widgets(self) -> None:
        self._refresh_running_status()
        self._refresh_scheduler_strip()

    def _refresh_running_status(self) -> None:
        theme = resolve_theme(self.settings)
        snapshot = self._status_service.status_snapshot()
        checked_at = dt.datetime.now().strftime("%H:%M:%S")
        for program_name, lbl in self.status_labels.items():
            info = snapshot.get(program_name, {})
            state = str(info.get("state", "idle"))
            base_tooltip = str(info.get("tooltip", "Not running"))
            configured = ""
            key = PROGRAM_PATH_KEYS.get(program_name)
            if key:
                configured = str(self.settings.get(key, "") or "").strip()
            lines = [f"{program_name}: {base_tooltip}", f"Last check: {checked_at}"]
            if configured:
                lines.append(f"Configured: {configured}")
            lbl.setStyleSheet(led_style(state, theme))
            lbl.setToolTip("\n".join(lines))
            self._status_checked_at[program_name] = checked_at

    def _refresh_scheduler_strip(self) -> None:
        try:
            sched = getattr(self.window(), "scheduler", None)
            if not sched or not hasattr(sched, "get_status_summary"):
                self.now_status_label.setText("Status: --")
                self.next_change_label.setText("Next: --")
                self.suspend_label.setText("Suspend: --")
                return
            status = sched.get_status_summary()
            off_schedule = bool(status.get("off_schedule"))
            now_state = "Off Schedule" if off_schedule else "On Schedule"
            self.now_status_label.setText(f"Status: {now_state}")
            if off_schedule:
                self.now_status_label.setStyleSheet("font-weight: 600; color: #B71C1C;")
            else:
                self.now_status_label.setStyleSheet("font-weight: 500;")

            next_change = getattr(sched, "next_change_utc", None)
            next_text = "Next: --"
            if isinstance(next_change, dt.datetime):
                if next_change.tzinfo is None:
                    next_change = next_change.replace(tzinfo=dt.timezone.utc)
                else:
                    next_change = next_change.astimezone(dt.timezone.utc)
                now_utc = dt.datetime.now(dt.timezone.utc)
                mins = int(max(0.0, (next_change - now_utc).total_seconds()) // 60)
                display_dt = next_change.astimezone(self._get_display_tz()) if self._show_local else next_change
                next_text = f"Next: {display_dt:%H:%M} ({mins}m)"
                if mins <= 15:
                    self.next_change_label.setStyleSheet("font-weight: 600; color: #B71C1C;")
                elif mins <= 60:
                    self.next_change_label.setStyleSheet("font-weight: 500; color: #8A5A00;")
                else:
                    self.next_change_label.setStyleSheet("color: #888;")
            else:
                self.next_change_label.setStyleSheet("color: #888;")
            self.next_change_label.setText(next_text)

            suspended_until = status.get("suspended_until")
            suspend_text = "Suspend: Active" if suspended_until else "Suspend: Off"
            if isinstance(suspended_until, dt.datetime):
                local_dt = suspended_until.astimezone(self._get_display_tz())
                suspend_text = f"Suspend: until {local_dt:%H:%M}"
                self.suspend_label.setStyleSheet("font-weight: 500; color: #8A5A00;")
            else:
                self.suspend_label.setStyleSheet("color: #888;")
            self.suspend_label.setText(suspend_text)
        except Exception as e:
            log.debug("ControlFreq: failed scheduler strip refresh: %s", e)

    def _normalize_state_abbr(self, value: str) -> str:
        state = (value or "").strip().upper()
        if not state:
            return ""
        if len(state) <= 2:
            return state
        return US_STATE_ABBR_FROM_NAME.get(state, "")

    def _load_prop_operator_geo(self) -> Dict[str, Dict[str, str]]:
        db_path = self._db_path()
        out: Dict[str, Dict[str, str]] = {}
        if not db_path.exists():
            return out
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    IFNULL(callsign,''),
                    IFNULL(state,''),
                    IFNULL(grid,'')
                FROM operator_checkins
                """
            )
            for callsign, state, grid in cur.fetchall():
                cs = (callsign or "").strip().upper()
                if not cs:
                    continue
                out[cs] = {
                    "state": self._normalize_state_abbr(state),
                    "grid": (grid or "").strip().upper(),
                }
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load propagation target operators: %s", e)
        return out

    def _set_prop_target_value_options(self, target_type: str, selected_value: str) -> None:
        target_type = (target_type or "REGION").strip().upper()
        selected_value = (selected_value or "").strip().upper()
        if target_type == "REGION" and selected_value == "NATIONAL":
            selected_value = "ALL"
        options: List[str] = []
        if target_type == "STATE":
            options = [s for s in LOWER48_STATES if s in STATE_CENTERS]
        elif target_type == "OPERATOR":
            options = sorted(self._prop_operator_geo.keys())
        else:
            options = ["ALL"] + sorted(FEMA_REGIONS.keys())
        self.prop_target_value_combo.blockSignals(True)
        self.prop_target_value_combo.clear()
        for value in options:
            self.prop_target_value_combo.addItem(value)
        if selected_value:
            idx = self.prop_target_value_combo.findText(selected_value, Qt.MatchFixedString)
            if idx >= 0:
                self.prop_target_value_combo.setCurrentIndex(idx)
            else:
                self.prop_target_value_combo.setEditText(selected_value)
        elif self.prop_target_value_combo.count() > 0:
            self.prop_target_value_combo.setCurrentIndex(0)
        else:
            self.prop_target_value_combo.setEditText("")
        self.prop_target_value_combo.setEditable(target_type == "OPERATOR")
        self.prop_target_value_combo.blockSignals(False)

    def _refresh_prop_target_controls(self) -> None:
        self._prop_target_syncing = True
        try:
            self._prop_operator_geo = self._load_prop_operator_geo()
            target_type = (self.settings.get("prop_target_type", "REGION") or "REGION").strip().upper()
            if target_type not in {"REGION", "STATE", "OPERATOR"}:
                target_type = "REGION"
            target_value = (self.settings.get("prop_target_value", "") or "").strip().upper()
            idx = self.prop_target_type_combo.findData(target_type)
            if idx < 0:
                idx = 0
            self.prop_target_type_combo.blockSignals(True)
            self.prop_target_type_combo.setCurrentIndex(idx)
            self.prop_target_type_combo.blockSignals(False)
            self._set_prop_target_value_options(target_type, target_value)
            current_value = (self.prop_target_value_combo.currentText() or "").strip().upper()
            existing_type = (self.settings.get("prop_target_type", "") or "").strip().upper()
            existing_value = (self.settings.get("prop_target_value", "") or "").strip().upper()
            if existing_type != target_type or existing_value != current_value:
                self.settings.set_many(
                    {
                        "prop_target_type": target_type,
                        "prop_target_value": current_value,
                    }
                )
        except Exception as e:
            log.debug("ControlFreq: failed to refresh propagation target controls: %s", e)
        finally:
            self._prop_target_syncing = False

    def _on_prop_target_type_changed(self, _idx: int) -> None:
        if self._prop_target_syncing:
            return
        target_type = (self.prop_target_type_combo.currentData() or "REGION").strip().upper()
        self._prop_target_syncing = True
        try:
            self._set_prop_target_value_options(target_type, "")
            value = (self.prop_target_value_combo.currentText() or "").strip().upper()
            self.settings.set_many(
                {
                    "prop_target_type": target_type,
                    "prop_target_value": value,
                }
            )
        except Exception as e:
            log.debug("ControlFreq: propagation target type change failed: %s", e)
        finally:
            self._prop_target_syncing = False
        self._refresh_propagation_snapshot()

    def _on_prop_target_value_changed(self, text: str) -> None:
        if self._prop_target_syncing:
            return
        target_type = (self.prop_target_type_combo.currentData() or "REGION").strip().upper()
        value = (text or "").strip().upper()
        if target_type == "REGION" and value == "NATIONAL":
            value = "ALL"
        if target_type == "STATE":
            value = self._normalize_state_abbr(value)
        try:
            self.settings.set_many(
                {
                    "prop_target_type": target_type,
                    "prop_target_value": value,
                }
            )
        except Exception as e:
            log.debug("ControlFreq: propagation target value change failed: %s", e)
        self._refresh_propagation_snapshot()

    def _db_path(self) -> Path:
        return get_config_dir() / "config" / "freqinout_nets.db"

    def _settings_db_path(self) -> Path:
        return get_config_dir() / "config" / "freqinout.db"

    def _load_group_combo(self) -> None:
        groups = self._get_operating_groups()
        current = self.group_combo.currentData()
        self.group_combo.blockSignals(True)
        self.group_combo.clear()
        self.group_combo.addItem("All Groups", "")
        for g in groups:
            self.group_combo.addItem(g, g)
        # restore
        idx = self.group_combo.findData(current)
        if idx < 0 and self._pending_group_filter:
            idx = self.group_combo.findData(self._pending_group_filter)
        if idx >= 0:
            self.group_combo.setCurrentIndex(idx)
            self._pending_group_filter = ""
        self.group_combo.blockSignals(False)

    def _get_operating_groups(self) -> List[str]:
        ops = self.settings.get("operating_groups", []) or []
        out: List[str] = []
        for row in ops:
            grp = str(row.get("group", "") or "").strip().upper()
            if grp and grp not in out:
                out.append(grp)
        return sorted(out)

    def _group_freq_map(self) -> Dict[str, List[float]]:
        ops = self.settings.get("operating_groups", []) or []
        out: Dict[str, List[float]] = {}
        for row in ops:
            grp = str(row.get("group", "") or "").strip().upper()
            if not grp:
                continue
            try:
                freq = float(row.get("frequency"))
            except Exception:
                continue
            out.setdefault(grp, []).append(freq)
        return out

    def _group_band_map(self) -> Dict[str, Set[str]]:
        ops = self.settings.get("operating_groups", []) or []
        out: Dict[str, Set[str]] = {}
        for row in ops:
            grp = str(row.get("group", "") or "").strip().upper()
            band = str(row.get("band", "") or "").strip().upper()
            if not grp or not band:
                continue
            out.setdefault(grp, set()).add(band)
        return out

    def _load_operator_group_map(self) -> Dict[str, Set[str]]:
        db_path = self._db_path()
        mapping: Dict[str, Set[str]] = {}
        if not db_path.exists():
            return mapping
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT callsign, group1, group2, group3, groups_json FROM operator_checkins"
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load operator groups: %s", e)
            return mapping
        for r in rows:
            cs = (r["callsign"] or "").strip().upper()
            if not cs:
                continue
            groups: Set[str] = set()
            for key in ("group1", "group2", "group3"):
                g = (r[key] or "").strip().upper()
                if g:
                    groups.add(g)
            try:
                if r["groups_json"]:
                    gj = json.loads(r["groups_json"])
                    for g in gj or []:
                        g = str(g).strip().upper()
                        if g:
                            groups.add(g)
            except Exception:
                pass
            if groups:
                mapping[cs] = groups
        return mapping

    def _refresh_activity(self) -> None:
        window_minutes = int(self.activity_window_combo.currentData() or 120)
        search = (self.search_edit.text() or "").strip().upper()
        group_filter = self.group_combo.currentData() or ""
        group_freqs = self._group_freq_map()
        group_bands = self._group_band_map()
        sched_freqs, sched_bands = self._scheduled_group_freqs(window_minutes)
        if sched_freqs:
            filtered_freqs: Dict[str, List[float]] = {}
            for grp, freqs in group_freqs.items():
                allowed = sched_freqs.get(grp, set())
                if not allowed:
                    continue
                filtered = [f for f in freqs if any(abs(f - a) < 0.0005 for a in allowed)]
                if filtered:
                    filtered_freqs[grp] = filtered
            group_freqs = filtered_freqs
            if sched_bands:
                filtered_bands: Dict[str, Set[str]] = {}
                for grp, bands in group_bands.items():
                    allowed_b = sched_bands.get(grp, set())
                    if not allowed_b:
                        continue
                    filtered = {b for b in bands if b in allowed_b}
                    if filtered:
                        filtered_bands[grp] = filtered
                group_bands = filtered_bands
        operator_groups = self._load_operator_group_map()
        db_path = self._db_path()
        if not db_path.exists():
            self._set_table_rows(self.activity_table, [["No activity data", "--", "--", "--"]])
            return
        now_ts = time.time()
        since_ts = now_ts - (window_minutes * 60)

        # callsigns seen by group based on js8_links + checkins
        group_seen: Dict[str, Set[str]] = {g: set() for g in group_freqs}
        group_traffic: Dict[str, int] = {g: 0 for g in group_freqs}
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT origin, destination, band, freq_hz FROM js8_links WHERE ts >= ?",
                (since_ts,),
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load js8_links: %s", e)
            rows = []
        for origin, dest, band, freq_hz in rows:
            band = (band or "").strip().upper()
            for grp, freqs in group_freqs.items():
                if group_filter and grp != group_filter:
                    continue
                if band and grp in group_bands and band not in group_bands.get(grp, set()):
                    continue
                if freq_hz is None:
                    continue
                try:
                    mhz = float(freq_hz) / 1_000_000.0
                except Exception:
                    continue
                if any(abs(mhz - f) < 0.0005 for f in freqs):
                    if origin:
                        group_seen[grp].add(str(origin).strip().upper())
                    if dest:
                        group_seen[grp].add(str(dest).strip().upper())
                    group_traffic[grp] = group_traffic.get(grp, 0) + 1

        # traffic by group (messages + observed links + checkins)

        def _add_group_traffic(cs: str):
            cs = (cs or "").strip().upper()
            if not cs:
                return
            if search and search not in cs:
                return
            groups = operator_groups.get(cs, set())
            for g in groups:
                if g in group_traffic and (not group_filter or g == group_filter):
                    group_traffic[g] += 1
                    group_seen[g].add(cs)

        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT from_call, utc_ts FROM js8_messages WHERE utc_ts >= ?",
                (since_ts,),
            )
            for cs, _ in cur.fetchall():
                _add_group_traffic(cs)
            cur.execute(
                "SELECT from_call, utc_ts FROM spotter_traffic WHERE utc_ts >= ?",
                (since_ts,),
            )
            for cs, _ in cur.fetchall():
                _add_group_traffic(cs)
            cur.execute(
                "SELECT from_call, ts FROM varac_messages WHERE ts >= ?",
                (since_ts,),
            )
            for cs, _ in cur.fetchall():
                _add_group_traffic(cs)
            cur.execute(
                "SELECT callsign, last_seen_ts FROM fldigi_checkins WHERE last_seen_ts >= ?",
                (since_ts,),
            )
            for cs, _ in cur.fetchall():
                _add_group_traffic(cs)
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load recent messages: %s", e)

        rows_out: List[List[str]] = []
        for grp in sorted(group_freqs.keys()):
            if group_filter and grp != group_filter:
                continue
            bands = sorted(group_bands.get(grp, set()))
            freqs = group_freqs.get(grp, [])
            freq_txt = ", ".join(f"{f:.3f}" for f in freqs[:2])
            if len(freqs) > 2:
                freq_txt += "…"
            band_txt = "/".join(bands[:2]) + ("…" if len(bands) > 2 else "")
            band_freq = f"{band_txt} {freq_txt}".strip()
            calls_seen = len(group_seen.get(grp, set()))
            msg_ct = int(group_traffic.get(grp, 0))
            if calls_seen == 0 and msg_ct == 0:
                continue
            if search and search not in grp and not any(search in cs for cs in group_seen.get(grp, set())):
                continue
            rows_out.append([grp, band_freq or "-", str(calls_seen), str(msg_ct)])
        if not rows_out:
            rows_out = [["No activity in selected window", "--", "--", "--"]]
        self._set_table_rows(self.activity_table, rows_out)

    def _refresh_frequency_control(self) -> None:
        # Avoid clobbering selection while user is interacting
        try:
            if self.freq_combo.view().isVisible():
                return
        except Exception:
            pass
        og_list = load_operating_groups(self.settings)
        current = selected_qsy_meta(self.freq_combo)
        current_freq = None
        try:
            if current:
                current_freq = float(current.get("freq"))
        except Exception:
            current_freq = None
        self.freq_combo.blockSignals(True)
        self.freq_combo.clear()
        self.freq_combo.addItem("Select frequency", None)
        restore_idx = -1
        for g in sorted(
            og_list,
            key=lambda x: (
                str(x.get("group", "")).upper(),
                str(x.get("band", "")).upper(),
                float(x.get("frequency", 0) or 0),
            ),
        ):
            try:
                freq_val = float(g.get("frequency", 0))
            except Exception:
                continue
            label = f"{g.get('group','').strip()} - {g.get('band','').strip()} - {freq_val:.3f} MHz"
            meta = {
                "freq": freq_val,
                "mode": g.get("mode", ""),
                "band": g.get("band", ""),
                "auto_tune": bool(g.get("auto_tune", False)),
                "vfo": (g.get("vfo") or "").strip().upper(),
            }
            self.freq_combo.addItem(label.strip(" -"), meta)
            if current_freq is not None and abs(freq_val - current_freq) < 0.0005:
                restore_idx = self.freq_combo.count() - 1
        if restore_idx >= 0:
            self.freq_combo.setCurrentIndex(restore_idx)
        self.freq_combo.blockSignals(False)
        sched_freq = current_scheduler_freq(self.window())
        sched_group = self._get_scheduled_group_name()
        if sched_freq is not None:
            grp = sched_group or "--"
            self.freq_ctrl_label.setText(f"Scheduled: {grp} {sched_freq:.3f} MHz")
        else:
            self.freq_ctrl_label.setText("Scheduled: --")
        active_freq = self._get_active_frequency_mhz()
        if active_freq is not None:
            grp = sched_group or "--"
            self.freq_active_label.setText(f"Active: {grp} {active_freq:.3f} MHz")
        else:
            self.freq_active_label.setText("Active: --")
        self._update_resume_button_style(sched_freq, active_freq)
        self._update_active_label_style(sched_freq, active_freq)
        self._refresh_scheduler_strip()
        self._refresh_intersections()

    def _refresh_intersections(self) -> None:
        now_ts = time.time()
        group_filter = (self.group_combo.currentData() or "").strip().upper()
        search = (self.search_edit.text() or "").strip().upper()
        cache_key = (group_filter, search)
        if (
            cache_key == self._intersection_cache_key
            and now_ts - self._intersection_cache_ts < 30
        ):
            self._set_table_rows(self.intersection_table, self._intersection_cache_rows)
            self._style_intersection_rows()
            return

        rows = self._compute_intersection_summary_rows(group_filter, search)
        if not rows:
            rows = [["Now", "0", "No exact-frequency overlaps"], ["Next 2 hours", "0", "--"]]
        self._intersection_cache_ts = now_ts
        self._intersection_cache_key = cache_key
        self._intersection_cache_rows = rows
        self._set_table_rows(self.intersection_table, rows)
        self._style_intersection_rows()

    def _compute_intersection_summary_rows(
        self, group_filter: str, search: str
    ) -> List[List[str]]:
        rows: List[List[str]] = []
        now_utc = dt.datetime.now(dt.timezone.utc)
        now_min = now_utc.hour * 60 + now_utc.minute
        horizon_min = min(now_min + 120, 1440)
        today_name = now_utc.strftime("%A")
        tz = self._get_display_tz()

        my_entries = self._load_my_schedule_entries(today_name)
        if not my_entries:
            return rows
        operator_groups = self._load_operator_group_map()

        db_path = self._db_path()
        if not db_path.exists():
            return rows
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                """
                SELECT owner_callsign, day_utc, start_utc, end_utc, band, frequency
                FROM peer_hf_schedule
                """
            )
            peer_rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load peer schedule: %s", e)
            return rows

        now_calls: Set[str] = set()
        next_calls: Set[str] = set()
        now_labels: Set[str] = set()
        next_labels: Set[str] = set()
        for r in peer_rows:
            day = (r["day_utc"] or "ALL").strip()
            if not self._day_matches_today(day, today_name):
                continue
            cs = (r["owner_callsign"] or "").strip().upper()
            if not cs:
                continue
            groups = operator_groups.get(cs, set())
            if group_filter:
                if group_filter not in groups:
                    continue
            if search and search not in cs and not any(search in g for g in groups):
                continue
            peer_start = self._parse_time_minutes(r["start_utc"])
            peer_end = self._parse_time_minutes(r["end_utc"])
            if peer_start is None or peer_end is None or peer_end <= peer_start:
                continue
            if peer_end <= now_min or peer_start >= horizon_min:
                continue
            try:
                peer_freq = float(str(r["frequency"]).strip())
            except Exception:
                continue

            for entry in my_entries:
                if abs(entry["freq"] - peer_freq) > 0.0001:
                    continue
                start = max(peer_start, entry["start_min"], now_min)
                end = min(peer_end, entry["end_min"], horizon_min)
                if end > start:
                    if start <= now_min < end:
                        now_calls.add(cs)
                        now_labels.add(self._format_group_band_freq_label(entry))
                    else:
                        next_calls.add(cs)
                        next_labels.add(self._format_group_band_freq_label(entry))

        rows.append(["Now", str(len(now_calls)), self._summarize_labels(now_labels)])
        rows.append(["Next 2 hours", str(len(next_calls)), self._summarize_labels(next_labels)])
        return rows

    def _format_group_band_freq_label(self, entry: Dict[str, object]) -> str:
        grp = (entry.get("group") or "--").strip().upper()
        band = (entry.get("band") or "--").strip().upper()
        freq = entry.get("freq")
        try:
            freq_txt = f"{float(freq):.3f} MHz"
        except Exception:
            freq_txt = "--"
        return f"{grp} {band} {freq_txt}"

    def _summarize_labels(self, labels: Set[str]) -> str:
        if not labels:
            return "--"
        ordered = sorted(labels)
        if len(ordered) <= 2:
            return ", ".join(ordered)
        return f"{ordered[0]}, {ordered[1]} +{len(ordered) - 2} more"

    def _style_intersection_rows(self) -> None:
        # Emphasize "Now" and de-emphasize "Next hour"
        if self.intersection_table.rowCount() < 2:
            return
        palette = self._urgency_palette()
        try:
            now_item = self.intersection_table.item(0, 0)
            now_overlaps = 0
            try:
                now_overlaps = int((self.intersection_table.item(0, 1).text() if self.intersection_table.item(0, 1) else "0") or "0")
            except Exception:
                now_overlaps = 0
            if now_item:
                now_item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                font = now_item.font()
                font.setBold(True)
                now_item.setFont(font)
            if now_overlaps > 0:
                for col in range(self.intersection_table.columnCount()):
                    it = self.intersection_table.item(0, col)
                    if it:
                        it.setBackground(palette["warn"])
                        it.setForeground(palette["text"])
            for col in range(self.intersection_table.columnCount()):
                item = self.intersection_table.item(1, col)
                if item:
                    item.setForeground(palette["muted_text"])
        except Exception:
            pass

    def _format_current_schedule_label(self) -> str:
        sched_freq = current_scheduler_freq(self.window())
        if sched_freq is None:
            return "--"
        sched_group = self._get_scheduled_group_name()
        band = self._band_for_group_freq(sched_group, sched_freq)
        grp = sched_group or "--"
        band_txt = band or "--"
        return f"{grp} {band_txt} {sched_freq:.3f} MHz"

    def _band_for_group_freq(self, group: str, freq: float) -> str:
        if not group:
            return ""
        ops = self.settings.get("operating_groups", []) or []
        for row in ops:
            grp = (row.get("group") or "").strip().upper()
            if grp != group:
                continue
            try:
                f = float(row.get("frequency", 0))
            except Exception:
                continue
            if abs(f - freq) < 0.0005:
                return (row.get("band") or "").strip().upper()
        return ""

    def _load_my_schedule_entries(self, today_name: str) -> List[Dict[str, object]]:
        entries: List[Dict[str, object]] = []

        def add_entry(day: str, start: str, end: str, band: str, freq_val, group: str) -> None:
            if not self._day_matches_today(day, today_name):
                return
            start_min = self._parse_time_minutes(start)
            end_min = self._parse_time_minutes(end)
            if start_min is None or end_min is None or end_min <= start_min:
                return
            try:
                freq_num = float(str(freq_val).strip())
            except Exception:
                return
            entries.append(
                {
                    "start_min": start_min,
                    "end_min": end_min,
                    "freq": freq_num,
                    "band": (band or "").strip().upper(),
                    "group": (group or "").strip().upper(),
                }
            )

        db_path = self._settings_db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT day_utc, start_utc, end_utc, band, frequency, group_name FROM daily_schedule_tab")
                rows = cur.fetchall()
                conn.close()
                for r in rows:
                    add_entry(
                        r["day_utc"],
                        r["start_utc"],
                        r["end_utc"],
                        r["band"],
                        r["frequency"],
                        r["group_name"],
                    )
            except Exception as e:
                log.debug("ControlFreq: failed to load daily schedule for overlaps: %s", e)

        db_path = self._db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(
                    "SELECT day_utc, start_utc, end_utc, band, frequency, group_name, net_name FROM net_schedule_tab"
                )
                rows = cur.fetchall()
                conn.close()
                for r in rows:
                    group = r["group_name"] or r["net_name"]
                    add_entry(
                        r["day_utc"],
                        r["start_utc"],
                        r["end_utc"],
                        r["band"],
                        r["frequency"],
                        group,
                    )
            except Exception as e:
                log.debug("ControlFreq: failed to load net schedule for overlaps: %s", e)

        return entries

    @staticmethod
    def _day_matches_today(day: str, today_name: str) -> bool:
        day = (day or "ALL").strip().upper()
        if day in ("ALL", "DAILY"):
            return True
        if not day:
            return False
        today = today_name.upper()
        return today.startswith(day[:3]) or day.startswith(today[:3])

    @staticmethod
    def _parse_time_minutes(value: str) -> Optional[int]:
        txt = (value or "").strip()
        if not txt:
            return None
        parts = txt.split(":")
        try:
            if len(parts) == 1 and txt.isdigit() and len(txt) in (3, 4):
                hour = int(txt[:-2])
                minute = int(txt[-2:])
            elif len(parts) == 2:
                hour = int(parts[0])
                minute = int(parts[1])
            else:
                return None
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                return None
            return hour * 60 + minute
        except Exception:
            return None

    def _format_overlap_time(self, start_min: int, end_min: int, tz: dt.tzinfo) -> str:
        base = dt.datetime.now(dt.timezone.utc).date()
        start_dt = dt.datetime(
            base.year,
            base.month,
            base.day,
            start_min // 60,
            start_min % 60,
            tzinfo=dt.timezone.utc,
        )
        end_dt = dt.datetime(
            base.year,
            base.month,
            base.day,
            end_min // 60,
            end_min % 60,
            tzinfo=dt.timezone.utc,
        )
        if self._show_local:
            start_dt = start_dt.astimezone(tz)
            end_dt = end_dt.astimezone(tz)
        return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"

    def _get_scheduled_group_name(self) -> str:
        try:
            sched = getattr(self.window(), "scheduler", None)
            entry = getattr(sched, "current_schedule_entry", {}) if sched else {}
            return (entry.get("group_name") or entry.get("group") or "").strip().upper()
        except Exception:
            return ""

    def _get_active_frequency_mhz(self) -> Optional[float]:
        try:
            sched = getattr(self.window(), "scheduler", None)
            if not sched or not hasattr(sched, "get_status_summary"):
                return None
            status = sched.get_status_summary()
            freq_label = status.get("freq_label") or ""
            return self._parse_freq_label(freq_label)
        except Exception:
            return None

    @staticmethod
    def _parse_freq_label(label: str) -> Optional[float]:
        try:
            parts = str(label).replace("MHz", "").strip().split()
            for token in parts:
                try:
                    return float(token)
                except Exception:
                    continue
        except Exception:
            return None
        return None

    def _update_resume_button_style(
        self, scheduled: Optional[float], active: Optional[float]
    ) -> None:
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        mismatch = False
        if scheduled is not None and active is not None:
            mismatch = abs(scheduled - active) > 0.0005
        if theme:
            style = "warning" if mismatch else "secondary"
            self.freq_resume_btn.setStyleSheet(button_style(style, theme))

    def _update_active_label_style(
        self, scheduled: Optional[float], active: Optional[float]
    ) -> None:
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        mismatch = False
        if scheduled is not None and active is not None:
            mismatch = abs(scheduled - active) > 0.0005
        if theme:
            color = theme["info"] if mismatch else theme["text"]
            self.freq_active_label.setStyleSheet(
                f"font-size: 14px; font-weight: bold; color: {color};"
            )

    def _on_freq_set_clicked(self) -> None:
        control_via = (self.settings.get("control_via", "") or "").strip()
        if control_via not in {"FLRig", "JS8Call"}:
            QMessageBox.information(
                self,
                "Frequency Control",
                "Frequency control is available when Control Via is FLRig or JS8Call.",
            )
            return
        meta = selected_qsy_meta(self.freq_combo)
        if not meta:
            QMessageBox.warning(self, "Frequency Control", "Select a frequency first.")
            return
        ok = perform_qsy(self.window(), meta)
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        if theme:
            self.freq_set_btn.setStyleSheet(
                button_style("success" if ok else "warning", theme)
            )
        self._refresh_frequency_control()
        if ok:
            QTimer.singleShot(800, self._refresh_frequency_control)

    def _on_freq_selection_changed(self) -> None:
        meta = selected_qsy_meta(self.freq_combo)
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        if theme:
            if meta:
                self.freq_set_btn.setStyleSheet(button_style("info", theme))
            else:
                self.freq_set_btn.setStyleSheet(button_style("secondary", theme))

    def _on_resume_schedule_clicked(self) -> None:
        try:
            sched = getattr(self.window(), "scheduler", None)
            if sched and hasattr(sched, "resume_schedule"):
                sched.resume_schedule()
                return
            if sched:
                sched.apply_current_entry(force=True, ignore_wait_prompt=True, ignore_suspend=True)
        except Exception:
            pass

    def _refresh_schedule_outlook(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        today_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        week_end = now + dt.timedelta(days=7)
        today_rows = self._collect_schedule_rows(now, today_end)
        week_rows = self._collect_schedule_rows(now, week_end, include_day=True, include_hf=False)
        self.schedule_table.setRowCount(0)
        self._schedule_entries_by_row.clear()
        self._append_section_row_to(self.schedule_table, "Today")
        if today_rows:
            for entry in today_rows:
                self._append_schedule_data_row(entry)
        else:
            self._append_schedule_data_row(
                {
                    "when_text": "--",
                    "type": "--",
                    "group": "No upcoming events today",
                    "band_freq": "--",
                    "action": "",
                    "action_kind": "",
                    "when_utc": None,
                }
            )
        self._append_section_row_to(self.schedule_table, "7 Days")
        if week_rows:
            for entry in week_rows:
                self._append_schedule_data_row(entry)
        else:
            self._append_schedule_data_row(
                {
                    "when_text": "--",
                    "type": "--",
                    "group": "No upcoming events this week",
                    "band_freq": "--",
                    "action": "",
                    "action_kind": "",
                    "when_utc": None,
                }
            )
        self._apply_elide_tooltips(self.schedule_table, 2)
        self._apply_elide_tooltips(self.schedule_table, 3)

    def _append_schedule_data_row(self, entry: Dict[str, Any]) -> None:
        row = self.schedule_table.rowCount()
        self.schedule_table.insertRow(row)
        self._schedule_entries_by_row[row] = entry
        cols = [
            str(entry.get("when_text") or "--"),
            str(entry.get("type") or "--"),
            str(entry.get("group") or "--"),
            str(entry.get("band_freq") or "--"),
        ]
        for col, value in enumerate(cols):
            item = QTableWidgetItem(value)
            item.setFlags(item.flags() ^ Qt.ItemIsEditable)
            self.schedule_table.setItem(row, col, item)
        self._style_schedule_row(row, entry)
        action_label = str(entry.get("action") or "").strip()
        action_kind = str(entry.get("action_kind") or "").strip()
        if action_label and action_kind:
            btn = QPushButton(action_label)
            btn.setCursor(Qt.PointingHandCursor)
            btn.setMinimumWidth(96)
            btn.setMaximumWidth(132)
            btn.setToolTip(self._schedule_action_tooltip(action_kind))
            btn.clicked.connect(lambda _=False, payload=entry: self._on_schedule_action_clicked(payload))
            try:
                theme = resolve_theme(self.settings)
                btn.setStyleSheet(button_style(self._schedule_action_button_role(action_kind), theme))
            except Exception:
                pass
            self.schedule_table.setCellWidget(row, 4, btn)

    @staticmethod
    def _schedule_action_button_role(action_kind: str) -> str:
        kind = (action_kind or "").strip().lower()
        if kind == "qsy":
            return "warning"
        if kind in {"open_net", "open_sop"}:
            return "primary"
        return "secondary"

    @staticmethod
    def _schedule_action_tooltip(action_kind: str) -> str:
        kind = (action_kind or "").strip().lower()
        if kind == "qsy":
            return "Tune radio to this scheduled frequency now."
        if kind == "open_net":
            return "Open Net Schedule tab."
        if kind == "open_sop":
            return "Open SOP Builder tab."
        return "Run row action."

    def _on_schedule_context_menu(self, pos) -> None:
        try:
            row = self.schedule_table.rowAt(int(pos.y()))
            if row < 0:
                return
            entry = self._schedule_entries_by_row.get(row) or {}
            action_kind = str(entry.get("action_kind") or "").strip().lower()
            action_label = str(entry.get("action") or "").strip()
            if not action_kind or not action_label:
                return
            menu = QMenu(self.schedule_table)
            act = menu.addAction(action_label)
            chosen = menu.exec(self.schedule_table.viewport().mapToGlobal(pos))
            if chosen == act:
                self._on_schedule_action_clicked(entry)
        except Exception as e:
            log.debug("ControlFreq: schedule row context-menu failed: %s", e)

    def _style_schedule_row(self, row: int, entry: Dict[str, Any]) -> None:
        when_utc = entry.get("when_utc")
        if not isinstance(when_utc, dt.datetime):
            return
        palette = self._urgency_palette()
        now_utc = dt.datetime.now(dt.timezone.utc)
        mins = int((when_utc - now_utc).total_seconds() // 60)
        bg = None
        if mins <= 15:
            bg = palette["critical"]
        elif mins <= 60:
            bg = palette["soon"]
        elif mins <= 180:
            bg = palette["upcoming"]
        if bg is not None:
            for col in range(4):
                item = self.schedule_table.item(row, col)
                if item:
                    item.setBackground(bg)
                    item.setForeground(palette["text"])
        type_item = self.schedule_table.item(row, 1)
        if type_item and mins <= 15:
            f = type_item.font()
            f.setBold(True)
            type_item.setFont(f)

    def _on_schedule_action_clicked(self, entry: Dict[str, Any]) -> None:
        kind = str(entry.get("action_kind") or "").strip().lower()
        if kind == "open_net":
            self._navigate_to_tab("Net Schedule")
            return
        if kind == "open_sop":
            self._navigate_to_tab("SOP")
            return
        if kind != "qsy":
            return
        meta = self._schedule_qsy_meta(entry)
        if not meta:
            QMessageBox.warning(self, "Frequency Control", "No matching operating-group frequency is configured.")
            return
        ok = perform_qsy(self.window(), meta)
        if ok:
            self._refresh_frequency_control()
            QTimer.singleShot(800, self._refresh_frequency_control)

    def _schedule_qsy_meta(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        group = str(entry.get("group") or "").strip().upper()
        band = str(entry.get("band") or "").strip().upper()
        try:
            freq = float(entry.get("freq_mhz"))
        except Exception:
            return None
        for row in load_operating_groups(self.settings):
            grp = str(row.get("group") or "").strip().upper()
            if grp != group:
                continue
            try:
                row_freq = float(row.get("frequency", 0))
            except Exception:
                continue
            if abs(row_freq - freq) > 0.0005:
                continue
            return {
                "freq": row_freq,
                "mode": row.get("mode", ""),
                "band": row.get("band", "") or band,
                "auto_tune": bool(row.get("auto_tune", False)),
                "vfo": (row.get("vfo") or "").strip().upper(),
            }
        return {
            "freq": freq,
            "mode": "",
            "band": band,
            "auto_tune": False,
            "vfo": "",
        }

    def _navigate_to_tab(self, label: str) -> None:
        try:
            win = self.window()
            screens = getattr(win, "_screens", [])
            for idx, (tab_label, _widget) in enumerate(screens):
                if str(tab_label) == label and hasattr(win, "_set_screen"):
                    win._set_screen(idx)
                    return
        except Exception as e:
            log.debug("ControlFreq: failed to navigate to tab %s: %s", label, e)

    def _refresh_today(self) -> None:
        # Backward-compat shim for legacy callers.
        self._refresh_schedule_outlook()

    def _refresh_week(self) -> None:
        # Backward-compat shim for legacy callers.
        self._refresh_schedule_outlook()

    def _collect_schedule_rows(
        self,
        start: dt.datetime,
        end: dt.datetime,
        include_day: bool = False,
        include_hf: bool = True,
    ) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        tz = self._get_display_tz()

        # SOP upcoming actions
        try:
            mgr = SOPManager()
            horizon_hours = max(1, int((end - start).total_seconds() // 3600))
            actions = mgr.build_upcoming_actions(
                horizon_hours=horizon_hours, only_active=True, now_utc=start
            )
            for a in actions:
                due = a.get("next_due_utc") or a.get("due_utc")
                if not isinstance(due, dt.datetime):
                    continue
                if due.tzinfo is None:
                    due = due.replace(tzinfo=dt.timezone.utc)
                else:
                    due = due.astimezone(dt.timezone.utc)
                if not (start <= due <= end):
                    continue
                grp = (a.get("operating_group") or "").strip().upper()
                if group_filter and grp != group_filter:
                    continue
                label = a.get("action_label") or a.get("action") or "SOP"
                band = (a.get("band") or "").strip().upper()
                freq = (a.get("frequency") or "").strip()
                when = self._format_display_time(due, include_day, tz)
                if search and search not in grp and search not in str(label).upper() and search not in "SOP":
                    continue
                rows_out.append(
                    {
                        "when_text": when,
                        "when_utc": due,
                        "type": "SOP",
                        "group": f"{grp} {label}".strip(),
                        "band_freq": f"{band} {freq}".strip(),
                        "band": band,
                        "freq_mhz": None,
                        "action": "Open SOP",
                        "action_kind": "open_sop",
                    }
                )
        except Exception as e:
            log.debug("ControlFreq: SOP load failed: %s", e)

        # HF + Net schedule (simple view)
        if include_hf:
            rows_out.extend(self._load_hf_schedule(start, end, include_day, tz))
        rows_out.extend(self._load_net_schedule(start, end, include_day, tz))
        rows_out.sort(
            key=lambda r: (
                r.get("when_utc") if isinstance(r.get("when_utc"), dt.datetime) else dt.datetime.max.replace(tzinfo=dt.timezone.utc),
                str(r.get("type") or ""),
                str(r.get("group") or ""),
            )
        )
        return rows_out[:200]

    def _load_hf_schedule(
        self, start: dt.datetime, end: dt.datetime, include_day: bool, tz: dt.tzinfo
    ) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        db_path = self._settings_db_path()
        if not db_path.exists():
            return rows_out
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM daily_schedule_tab")
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load HF schedule: %s", e)
            return rows_out
        today_name = start.strftime("%A")
        for r in rows:
            row = dict(r)
            day = (row.get("day") or row.get("day_utc") or row.get("day_name") or "").strip()
            if day and day not in {today_name, "ALL"} and not include_day:
                continue
            grp = (row.get("group_name") or row.get("group") or "").strip().upper()
            if group_filter and grp != group_filter:
                continue
            start_hm = (row.get("start") or row.get("start_utc") or "").strip()
            band = (row.get("band") or "").strip().upper()
            freq_raw = row.get("frequency") or row.get("freq") or ""
            when_utc = self._resolve_schedule_time_utc(start, end, day or "ALL", start_hm, include_day)
            if when_utc is None:
                continue
            try:
                freq_mhz = float(str(freq_raw).strip())
            except Exception:
                freq_mhz = None
            freq_txt = f"{freq_mhz:.3f}" if isinstance(freq_mhz, float) else str(freq_raw).strip()
            when = self._format_display_time(when_utc, include_day, tz)
            if search and search not in grp and search not in (band + freq_txt).upper() and search not in "HF":
                continue
            rows_out.append(
                {
                    "when_text": when,
                    "when_utc": when_utc,
                    "type": "HF",
                    "group": grp,
                    "band_freq": f"{band} {freq_txt}".strip(),
                    "band": band,
                    "freq_mhz": freq_mhz,
                    "action": "QSY Now" if isinstance(freq_mhz, float) else "",
                    "action_kind": "qsy" if isinstance(freq_mhz, float) else "",
                }
            )
        return rows_out

    def _scheduled_group_freqs(
        self, window_minutes: int
    ) -> Tuple[Dict[str, Set[float]], Dict[str, Set[str]]]:
        sched_freqs: Dict[str, Set[float]] = {}
        sched_bands: Dict[str, Set[str]] = {}
        db_path = self._settings_db_path()
        if not db_path.exists():
            return sched_freqs, sched_bands
        now_utc = dt.datetime.now(dt.timezone.utc)
        today_utc_name = now_utc.strftime("%A").upper()

        def parse_hhmm(value: str) -> Optional[Tuple[int, int]]:
            txt = (value or "").strip()
            if not txt:
                return None
            parts = txt.split(":")
            if len(parts) == 1 and txt.isdigit() and len(txt) in (3, 4):
                h = int(txt[:-2])
                m = int(txt[-2:])
            elif len(parts) == 2:
                h = int(parts[0])
                m = int(parts[1])
            else:
                return None
            if h < 0 or h > 23 or m < 0 or m > 59:
                return None
            return h, m

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM daily_schedule_tab")
            rows = cur.fetchall()
            conn.close()
        except Exception:
            rows = []
        for r in rows:
            row = dict(r)
            day = (row.get("day_utc") or row.get("day") or "ALL").strip().upper()
            if day not in {"ALL", today_utc_name}:
                continue
            start_hm = row.get("start_utc") or row.get("start") or ""
            hm = parse_hhmm(str(start_hm))
            if not hm:
                continue
            start_utc = now_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
            delta_min = abs((start_utc - now_utc).total_seconds()) / 60.0
            if delta_min > float(window_minutes):
                continue
            grp = (row.get("group_name") or row.get("group") or "").strip().upper()
            if not grp:
                continue
            band = (row.get("band") or "").strip().upper()
            freq_val = row.get("frequency") or row.get("freq")
            if band:
                sched_bands.setdefault(grp, set()).add(band)
            try:
                freq = float(freq_val)
                sched_freqs.setdefault(grp, set()).add(freq)
            except Exception:
                continue
        return sched_freqs, sched_bands

    def _load_net_schedule(
        self, start: dt.datetime, end: dt.datetime, include_day: bool, tz: dt.tzinfo
    ) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        db_path = self._db_path()
        if not db_path.exists():
            return rows_out
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM net_schedule_tab")
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load Net schedule: %s", e)
            return rows_out
        today_name = start.strftime("%A")
        for r in rows:
            row = dict(r)
            day = (row.get("day") or row.get("day_utc") or "").strip()
            if day and day not in {today_name, "ALL"} and not include_day:
                continue
            grp = (row.get("group_name") or row.get("group") or "").strip().upper()
            if group_filter and grp != group_filter:
                continue
            start_hm = (row.get("start") or row.get("start_utc") or "").strip()
            band = (row.get("band") or "").strip().upper()
            freq_raw = row.get("frequency") or row.get("freq") or ""
            net_name = (row.get("net_name") or "").strip()
            when_utc = self._resolve_schedule_time_utc(start, end, day or "ALL", start_hm, include_day)
            if when_utc is None:
                continue
            try:
                freq_mhz = float(str(freq_raw).strip())
            except Exception:
                freq_mhz = None
            freq_txt = f"{freq_mhz:.3f}" if isinstance(freq_mhz, float) else str(freq_raw).strip()
            when = self._format_display_time(when_utc, include_day, tz)
            if search and search not in grp and search not in net_name.upper() and search not in "NET":
                continue
            rows_out.append(
                {
                    "when_text": when,
                    "when_utc": when_utc,
                    "type": "NET",
                    "group": net_name or grp,
                    "band_freq": f"{band} {freq_txt}".strip(),
                    "band": band,
                    "freq_mhz": freq_mhz,
                    "action": "Open Net",
                    "action_kind": "open_net",
                }
            )
        return rows_out

    def _resolve_schedule_time_utc(
        self,
        start_utc: dt.datetime,
        end_utc: dt.datetime,
        day_value: str,
        hhmm: str,
        include_day: bool,
    ) -> Optional[dt.datetime]:
        hm = self._parse_hhmm(hhmm)
        if not hm:
            return None
        day_norm = (day_value or "ALL").strip().upper()
        if day_norm in {"", "ALL", "DAILY"}:
            candidate = start_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
            if candidate < start_utc and include_day:
                candidate += dt.timedelta(days=1)
            if start_utc <= candidate <= end_utc:
                return candidate
            return None
        if not include_day:
            today = start_utc.strftime("%A").upper()
            if not (today.startswith(day_norm[:3]) or day_norm.startswith(today[:3])):
                return None
            candidate = start_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
            if start_utc <= candidate <= end_utc:
                return candidate
            return None
        for i in range(0, 8):
            day_dt = (start_utc + dt.timedelta(days=i))
            day_name = day_dt.strftime("%A").upper()
            if day_name.startswith(day_norm[:3]) or day_norm.startswith(day_name[:3]):
                candidate = day_dt.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
                if start_utc <= candidate <= end_utc:
                    return candidate
                return None
        return None

    def _get_display_tz(self) -> dt.tzinfo:
        if not self._show_local:
            return dt.timezone.utc
        try:
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            return get_timezone(tz_name)
        except Exception:
            return dt.timezone.utc

    def _format_display_time(
        self, utc_dt: dt.datetime, include_day: bool, tz: dt.tzinfo
    ) -> str:
        if not isinstance(utc_dt, dt.datetime):
            return "--:--"
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=dt.timezone.utc)
        local_dt = utc_dt.astimezone(tz) if self._show_local else utc_dt.astimezone(dt.timezone.utc)
        return local_dt.strftime("%a %H:%M") if include_day else local_dt.strftime("%H:%M")

    def _parse_hhmm(self, value: str) -> Optional[Tuple[int, int]]:
        txt = (value or "").strip()
        if not txt:
            return None
        parts = txt.split(":")
        if len(parts) == 1 and txt.isdigit() and len(txt) in (3, 4):
            h = int(txt[:-2])
            m = int(txt[-2:])
        elif len(parts) == 2:
            h = int(parts[0])
            m = int(parts[1])
        else:
            return None
        if h < 0 or h > 23 or m < 0 or m > 59:
            return None
        return h, m

    def _format_hhmm_display(
        self, base_utc: dt.datetime, hhmm: str, include_day: bool, tz: dt.tzinfo
    ) -> str:
        hm = self._parse_hhmm(hhmm)
        if not hm:
            return "--:--"
        dt_utc = base_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=dt.timezone.utc)
        return self._format_display_time(dt_utc, include_day, tz)

    def _refresh_message_summary(self) -> None:
        search = (self.search_edit.text() or "").strip().upper()
        message_rows = self._collect_inbox_rows(search)
        bbs_rows = self._collect_bbs_rows(search)
        rows_out: List[List[str]] = []
        rows_out.extend(message_rows)
        rows_out.extend(bbs_rows)
        self._set_table_rows(self.inbox_table, rows_out)
        self._style_message_summary_rows()
        self._apply_elide_tooltips(self.inbox_table, 2)

    def _style_message_summary_rows(self) -> None:
        palette = self._urgency_palette()
        for row in range(self.inbox_table.rowCount()):
            type_item = self.inbox_table.item(row, 0)
            count_item = self.inbox_table.item(row, 1)
            detail_item = self.inbox_table.item(row, 2)
            if type_item is None or count_item is None:
                continue
            label = (type_item.text() or "").strip().upper()
            try:
                count_val = int((count_item.text() or "0").strip())
            except Exception:
                count_val = 0
            if label == "VARAC BBS" and detail_item and detail_item.text().strip() not in {"", "-"}:
                for c in range(self.inbox_table.columnCount()):
                    it = self.inbox_table.item(row, c)
                    if it:
                        it.setBackground(palette["warn"])
                        it.setForeground(palette["text"])
                continue
            if label == "SITREP":
                red_ct = 0
                if detail_item:
                    txt = (detail_item.text() or "").upper()
                    try:
                        red_ct = int(txt.split("R:", 1)[1].split()[0])
                    except Exception:
                        red_ct = 0
                tone = palette["warn"] if red_ct > 0 else palette["positive"]
                for c in range(self.inbox_table.columnCount()):
                    it = self.inbox_table.item(row, c)
                    if it:
                        it.setBackground(tone)
                        it.setForeground(palette["text"])
                continue
            if count_val > 0:
                for c in range(self.inbox_table.columnCount()):
                    it = self.inbox_table.item(row, c)
                    if it:
                        it.setBackground(palette["positive"])
                        it.setForeground(palette["text"])

    def _collect_inbox_rows(self, search: str) -> List[List[str]]:
        db_path = self._db_path()
        if not db_path.exists():
            return [["No data", "0", "Messages DB unavailable"]]
        counts = {"JS8": 0, "Spotter": 0, "VarAC": 0}
        top_senders: Dict[str, Dict[str, int]] = {"JS8": {}, "Spotter": {}, "VarAC": {}}
        sitrep_counts = {"red": 0, "yellow": 0, "green": 0}
        group_filter = (self.group_combo.currentData() or "").strip().upper()

        def _load_operator_groups(cur: sqlite3.Cursor) -> Dict[str, Set[str]]:
            out: Dict[str, Set[str]] = {}
            try:
                cur.execute(
                    """
                    SELECT callsign, group1, group2, group3, groups_json
                    FROM operator_checkins
                    """
                )
                for callsign, g1, g2, g3, groups_json in cur.fetchall():
                    cs = (callsign or "").strip().upper()
                    if not cs:
                        continue
                    groups: Set[str] = set()
                    for g in (g1, g2, g3):
                        gg = (g or "").strip().upper()
                        if gg:
                            groups.add(gg)
                    try:
                        parsed = json.loads(groups_json) if groups_json else []
                        if isinstance(parsed, list):
                            for g in parsed:
                                gg = str(g or "").strip().upper()
                                if gg:
                                    groups.add(gg)
                    except Exception:
                        pass
                    out[cs] = groups
            except Exception:
                return {}
            return out

        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT from_call, state FROM js8_messages")
            for cs, state in cur.fetchall():
                if (state or "").upper() == "READ":
                    continue
                cs = (cs or "").strip().upper()
                if search and search not in cs and search not in "JS8":
                    continue
                counts["JS8"] += 1
                top_senders["JS8"][cs] = top_senders["JS8"].get(cs, 0) + 1
            cur.execute("SELECT from_call, state FROM spotter_traffic")
            for cs, state in cur.fetchall():
                if (state or "").upper() == "READ":
                    continue
                cs = (cs or "").strip().upper()
                if search and search not in cs and search not in "SPOTTER":
                    continue
                counts["Spotter"] += 1
                top_senders["Spotter"][cs] = top_senders["Spotter"].get(cs, 0) + 1
            cur.execute("SELECT from_call, read_status FROM varac_messages")
            for cs, read_status in cur.fetchall():
                if int(read_status or 0) != 0:
                    continue
                cs = (cs or "").strip().upper()
                if search and search not in cs and search not in "VARAC":
                    continue
                counts["VarAC"] += 1
                top_senders["VarAC"][cs] = top_senders["VarAC"].get(cs, 0) + 1
            operator_groups: Dict[str, Set[str]] = {}
            if group_filter:
                operator_groups = _load_operator_groups(cur)
            try:
                cur.execute("SELECT from_call, status_key FROM spotter_station_status")
                for cs, status_key in cur.fetchall():
                    cs_up = (cs or "").strip().upper()
                    key = (status_key or "").strip().lower()
                    if not cs_up or key not in sitrep_counts:
                        continue
                    if group_filter:
                        if group_filter not in operator_groups.get(cs_up, set()):
                            continue
                    if search:
                        status_word = "RED" if key == "red" else ("YELLOW" if key == "yellow" else "GREEN")
                        if search not in cs_up and search not in "SITREP" and search not in status_word:
                            continue
                    sitrep_counts[key] += 1
            except Exception:
                pass
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: inbox summary load failed: %s", e)
        rows_out: List[List[str]] = []
        for key in ("JS8", "Spotter", "VarAC"):
            if search and search not in key.upper() and counts[key] == 0:
                continue
            senders = sorted(top_senders[key].items(), key=lambda kv: kv[1], reverse=True)[:3]
            sender_txt = ", ".join([f"{c}({n})" for c, n in senders]) or "-"
            rows_out.append([key, str(counts[key]), sender_txt])
        sitrep_total = sitrep_counts["red"] + sitrep_counts["yellow"] + sitrep_counts["green"]
        sitrep_details = f"R:{sitrep_counts['red']}  Y:{sitrep_counts['yellow']}  G:{sitrep_counts['green']}"
        if not search or search in "SITREP" or sitrep_total > 0:
            rows_out.append(["SitRep", str(sitrep_total), sitrep_details])
        return rows_out or [["No matches", "0", "-"]]

    def _collect_bbs_rows(self, search: str) -> List[List[str]]:
        bbs_dir_txt = (self.settings.get("varac_bbs_dir", "") or "").strip()
        bbs_dir = Path(bbs_dir_txt) if bbs_dir_txt else None
        auto_days_raw = self.settings.get("varac_bbs_auto_archive_days", 14)
        try:
            auto_days = max(1, int(auto_days_raw or 14))
        except Exception:
            auto_days = 14
        now_ts = time.time()
        aging_lower_days = max(0.0, float(auto_days - 1))
        aging_out: List[tuple[float, str]] = []
        all_names: List[str] = []
        if bbs_dir and bbs_dir.exists() and bbs_dir.is_dir():
            try:
                for child in bbs_dir.iterdir():
                    if not child.is_file():
                        continue
                    all_names.append(child.name)
                    try:
                        st = child.stat()
                    except OSError:
                        continue
                    age_days = max(0.0, (now_ts - float(st.st_mtime)) / 86400.0)
                    if aging_lower_days <= age_days < float(auto_days):
                        aging_out.append((float(st.st_mtime), child.name))
            except OSError:
                pass
            aging_out.sort(key=lambda item: item[0])
            aging_names = [name for _mtime, name in aging_out]
            if len(aging_names) > 6:
                aging_txt = ", ".join(aging_names[:6]) + f" +{len(aging_names) - 6} more"
            else:
                aging_txt = ", ".join(aging_names)
            row = ["VarAC BBS", str(len(all_names)), aging_txt or "-"]
            search_hits = search and (
                search in row[0].upper()
                or any(search in name.upper() for name in aging_names)
                or any(search in name.upper() for name in all_names)
            )
            if not search or search_hits:
                return [row]
            return [["No matches", "0", "-"]]
        note = "Not configured" if not bbs_dir_txt else "Missing directory"
        row = ["VarAC BBS", "0", note]
        if not search or search in row[0].upper() or search in note.upper():
            return [row]
        return [["No matches", "0", "-"]]

    def _refresh_inbox(self) -> None:
        self._refresh_message_summary()

    def _refresh_bbs_files(self) -> None:
        self._refresh_message_summary()

    @staticmethod
    def _append_section_row_to(
        table: QTableWidget,
        text: str,
        col1: str = "",
        col2: str = "",
    ) -> None:
        row = table.rowCount()
        table.insertRow(row)
        labels = [text, col1, col2]
        use_three_cols = bool(col1 or col2)
        for col, val in enumerate(labels):
            if not use_three_cols and col > 0:
                continue
            item = QTableWidgetItem(val if val else "")
            item.setFlags(item.flags() ^ Qt.ItemIsEditable)
            item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            item.setBackground(Qt.lightGray)
            item.setForeground(Qt.black)
            font = item.font()
            font.setBold(True)
            item.setFont(font)
            table.setItem(row, col, item)
        if not use_three_cols:
            table.setSpan(row, 0, 1, table.columnCount())

    @staticmethod
    def _append_rows_to(table: QTableWidget, rows: List[List[str]]) -> None:
        for row in rows:
            r = table.rowCount()
            table.insertRow(r)
            for c, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                table.setItem(r, c, item)

    def _set_prop_window_headers(
        self,
        dawn_local: Optional[dt.datetime] = None,
        day_start_local: Optional[dt.datetime] = None,
        sunset_local: Optional[dt.datetime] = None,
        night_end_local: Optional[dt.datetime] = None,
    ) -> None:
        if not hasattr(self, "prop_table"):
            return
        if not (dawn_local and day_start_local and sunset_local and night_end_local):
            self.prop_table.setHorizontalHeaderLabels(
                ["Zone", "Morning (Dawn-10:00)", "Day (10:00-Sunset)", "Night (Sunset-Dawn)"]
            )
            return
        morning_from = dawn_local.strftime("%H:%M")
        day_from = day_start_local.strftime("%H:%M")
        day_to = sunset_local.strftime("%H:%M")
        night_to = night_end_local.strftime("%H:%M")
        self.prop_table.setHorizontalHeaderLabels(
            [
                "Zone",
                f"Morning ({morning_from}-{day_from})",
                f"Day ({day_from}-{day_to})",
                f"Night ({day_to}-{night_to})",
            ]
        )

    def _refresh_propagation_snapshot(self) -> None:
        try:
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            tz = get_timezone(tz_name)
        except Exception:
            tz = dt.timezone.utc
        now_utc = dt.datetime.now(dt.timezone.utc)
        now_local = now_utc.astimezone(tz)

        user_grid = (
            self.settings.get("operator_grid6", "")
            or self.settings.get("operator_grid", "")
            or ""
        ).strip().upper()
        user_ll = maidenhead_to_latlon(user_grid) if user_grid else None
        if not user_ll:
            self._set_prop_window_headers()
            self._set_table_rows(self.prop_table, [])
            self.prop_hint.setText(
                "Set your Grid 6 in Settings to enable propagation snapshots."
            )
            return

        dawn_local, sunset_local = self._sunrise_sunset_local(
            now_local.date(), user_ll[0], user_ll[1], tz
        )
        if dawn_local is None or sunset_local is None:
            # Fallback for polar regions
            dawn_local = now_local.replace(hour=6, minute=0, second=0, microsecond=0)
            sunset_local = now_local.replace(hour=18, minute=0, second=0, microsecond=0)
        day_start_local = now_local.replace(hour=10, minute=0, second=0, microsecond=0)
        if dawn_local.date() != now_local.date():
            dawn_local = dawn_local.replace(
                year=now_local.year, month=now_local.month, day=now_local.day
            )
        if sunset_local.date() != now_local.date():
            sunset_local = sunset_local.replace(
                year=now_local.year, month=now_local.month, day=now_local.day
            )
        night_start = sunset_local
        night_end = dawn_local + dt.timedelta(days=1) if dawn_local <= sunset_local else dawn_local
        self._set_prop_window_headers(dawn_local, day_start_local, sunset_local, night_end)

        schedule_entries = self._load_today_schedule_local(now_local, tz)
        band_rows: Dict[str, List[Tuple[dt.datetime, str]]] = {}
        for when_local, band, label in schedule_entries:
            if not band:
                continue
            band_rows.setdefault(band, []).append((when_local, label))

        # Build window bands from schedule
        window_bands = {"morning": set(), "day": set(), "night": set()}
        for band, entries in band_rows.items():
            entries.sort(key=lambda e: e[0])
            for when_local, _label in entries:
                if dawn_local <= when_local < day_start_local:
                    window_bands["morning"].add(band)
                if day_start_local <= when_local < sunset_local:
                    window_bands["day"].add(band)
                if when_local >= night_start or when_local < dawn_local:
                    window_bands["night"].add(band)

        all_bands = sorted(band_rows.keys(), key=lambda b: (b not in PROP_BANDS, b))
        target_label, regional_points, target_type, target_id = self._resolve_prop_target_points()
        national_points = [STATE_CENTERS[s] for s in LOWER48_STATES if s in STATE_CENTERS]
        blend_settings = self._blend_settings_snapshot()

        # Compute modeled top-2 for each window
        morning_mid = dawn_local + (day_start_local - dawn_local) / 2
        day_mid = day_start_local + (sunset_local - day_start_local) / 2
        night_mid = night_start + (night_end - night_start) / 2
        window_mid = {"morning": morning_mid, "day": day_mid, "night": night_mid}

        nat_scores = {}
        reg_scores = {}
        for window, mid_local in window_mid.items():
            bands = sorted(window_bands.get(window) or all_bands)
            nat_scores[window] = self._top_bands_modeled(
                bands, mid_local, user_ll, points=national_points
            )
            reg_scores[window] = self._top_bands_modeled(
                bands,
                mid_local,
                user_ll,
                points=regional_points,
                origin_grid6=user_grid,
                target_type=target_type,
                target_id=target_id,
                blend_settings=blend_settings,
            )

        schedule_rows: List[List[str]] = []
        schedule_rows.append(
            [
                "National",
                self._format_band_list(nat_scores["morning"]) or "--",
                self._format_band_list(nat_scores["day"]) or "--",
                self._format_band_list(nat_scores["night"]) or "--",
            ]
        )
        schedule_rows.append(
            [
                "Regional",
                self._format_band_list(reg_scores["morning"]) or "--",
                self._format_band_list(reg_scores["day"]) or "--",
                self._format_band_list(reg_scores["night"]) or "--",
            ]
        )

        modeled_nat: Dict[str, List[Tuple[str, float]]] = {}
        modeled_reg: Dict[str, List[Tuple[str, float]]] = {}
        for window, mid_local in window_mid.items():
            modeled_nat[window] = self._top_bands_modeled(
                PROP_BANDS, mid_local, user_ll, points=national_points
            )
            modeled_reg[window] = self._top_bands_modeled(
                PROP_BANDS,
                mid_local,
                user_ll,
                points=regional_points,
                origin_grid6=user_grid,
                target_type=target_type,
                target_id=target_id,
                blend_settings=blend_settings,
            )

        modeled_rows: List[List[str]] = []
        modeled_rows.append(
            [
                "National",
                self._format_band_list(modeled_nat["morning"]) or "--",
                self._format_band_list(modeled_nat["day"]) or "--",
                self._format_band_list(modeled_nat["night"]) or "--",
            ]
        )
        modeled_rows.append(
            [
                "Regional",
                self._format_band_list(modeled_reg["morning"]) or "--",
                self._format_band_list(modeled_reg["day"]) or "--",
                self._format_band_list(modeled_reg["night"]) or "--",
            ]
        )
        self._set_sectioned_prop_rows("Schedule-based Forecast", schedule_rows, "Modeled Forecast", modeled_rows)
        schedule_note = ""
        if not all_bands:
            schedule_note = " No scheduled bands for today."
        self.prop_hint.setText(
            f"Modeled snapshot for {now_local.strftime('%Y-%m-%d')} "
            f"({tz.tzname(now_local)}). Origin Grid: {user_grid}. "
            f"Regional target: {target_label}.{schedule_note}"
        )

    def _points_for_region(self, region_id: str) -> List[Tuple[float, float]]:
        region_id = (region_id or "").strip().upper()
        states = FEMA_REGIONS.get(region_id, []) if region_id else []
        return [STATE_CENTERS[s] for s in states if s in STATE_CENTERS and s in LOWER48_STATES]

    def _blend_settings_snapshot(self) -> Dict[str, Any]:
        return {
            "prop_blend_enabled": self.settings.get("prop_blend_enabled", 1),
            "prop_empirical_alpha": self.settings.get("prop_empirical_alpha", 2.0),
            "prop_empirical_beta": self.settings.get("prop_empirical_beta", 3.0),
            "prop_decay_half_life_days": self.settings.get("prop_decay_half_life_days", 75),
            "prop_blend_gate_attempt_min": self.settings.get("prop_blend_gate_attempt_min", 8.0),
            "prop_blend_gate_unique_days_min": self.settings.get("prop_blend_gate_unique_days_min", 3),
            "prop_blend_max_weight": self.settings.get("prop_blend_max_weight", 0.85),
            "prop_blend_recent_window_days": self.settings.get("prop_blend_recent_window_days", 30),
            "prop_blend_history_cap_days": self.settings.get("prop_blend_history_cap_days", 365),
        }

    def _resolve_prop_target_points(self) -> Tuple[str, List[Tuple[float, float]], str, str]:
        target_type = (self.settings.get("prop_target_type", "REGION") or "REGION").strip().upper()
        target_value = (self.settings.get("prop_target_value", "") or "").strip().upper()
        if hasattr(self, "prop_target_type_combo"):
            target_type = (self.prop_target_type_combo.currentData() or target_type).strip().upper()
        if hasattr(self, "prop_target_value_combo"):
            target_value = (self.prop_target_value_combo.currentText() or target_value).strip().upper()
        if target_type not in {"REGION", "STATE", "OPERATOR"}:
            target_type = "REGION"
        if target_type == "STATE":
            target_value = self._normalize_state_abbr(target_value)
        if target_type == "OPERATOR" and not self._prop_operator_geo:
            self._prop_operator_geo = self._load_prop_operator_geo()

        # Default/fallback target from local operator state -> FEMA region.
        operator_state = self._normalize_state_abbr(self.settings.get("operator_state", "") or "")
        fallback_region = STATE_TO_FEMA_REGION.get(operator_state, "")
        fallback_label = f"Region {fallback_region}" if fallback_region else "Region --"
        fallback_points = self._points_for_region(fallback_region)
        fallback_type = "REGION"
        fallback_id = fallback_region
        national_points = [STATE_CENTERS[s] for s in LOWER48_STATES if s in STATE_CENTERS]

        if target_type == "REGION":
            if target_value in {"ALL", "NATIONAL"}:
                return "National", national_points, "REGION", "NATIONAL"
            points = self._points_for_region(target_value)
            if points:
                return f"Region {target_value}", points, "REGION", target_value
            return fallback_label, fallback_points, fallback_type, fallback_id

        if target_type == "STATE":
            state_abbr = target_value if target_value in STATE_CENTERS and target_value in LOWER48_STATES else ""
            if state_abbr:
                return state_abbr, [STATE_CENTERS[state_abbr]], "STATE", state_abbr
            return fallback_label, fallback_points, fallback_type, fallback_id

        callsign = target_value
        if callsign:
            meta = self._prop_operator_geo.get(callsign, {})
            grid = (meta.get("grid") or "").strip().upper()
            state_abbr = (meta.get("state") or "").strip().upper()
            ll = maidenhead_to_latlon(grid) if grid else None
            if ll:
                return callsign, [ll], "OPERATOR", callsign
            if state_abbr in STATE_CENTERS and state_abbr in LOWER48_STATES:
                return callsign, [STATE_CENTERS[state_abbr]], "OPERATOR", callsign
        return fallback_label, fallback_points, fallback_type, fallback_id

    def _load_today_schedule_local(
        self, now_local: dt.datetime, tz: dt.tzinfo
    ) -> List[Tuple[dt.datetime, str, str]]:
        entries: List[Tuple[dt.datetime, str, str]] = []
        today_local = now_local.date()
        now_utc = now_local.astimezone(dt.timezone.utc)
        today_utc_name = now_utc.strftime("%A")
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()

        def parse_hhmm(value: str) -> Optional[Tuple[int, int]]:
            txt = (value or "").strip()
            if not txt:
                return None
            parts = txt.split(":")
            if len(parts) == 1 and txt.isdigit() and len(txt) in (3, 4):
                h = int(txt[:-2])
                m = int(txt[-2:])
            elif len(parts) == 2:
                h = int(parts[0])
                m = int(parts[1])
            else:
                return None
            if h < 0 or h > 23 or m < 0 or m > 59:
                return None
            return h, m

        # HF schedule from settings DB
        db_path = self._settings_db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT * FROM daily_schedule_tab")
                rows = cur.fetchall()
                conn.close()
            except Exception:
                rows = []
            for r in rows:
                row = dict(r)
                day = (row.get("day_utc") or row.get("day") or "ALL").strip().upper()
                if day not in {"ALL", today_utc_name.upper()}:
                    continue
                start_hm = row.get("start_utc") or row.get("start") or ""
                hm = parse_hhmm(str(start_hm))
                if not hm:
                    continue
                band = (row.get("band") or "").strip().upper()
                label = (row.get("group_name") or row.get("group") or "").strip().upper()
                if group_filter and label != group_filter:
                    continue
                if search and search not in label and search not in band:
                    continue
                start_utc = now_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
                start_local = start_utc.astimezone(tz)
                if start_local.date() == today_local:
                    entries.append((start_local, band, label))

        # Net schedule from nets DB
        db_path = self._db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT * FROM net_schedule_tab")
                rows = cur.fetchall()
                conn.close()
            except Exception:
                rows = []
            for r in rows:
                row = dict(r)
                day = (row.get("day_utc") or row.get("day") or "ALL").strip().upper()
                if day not in {"ALL", today_utc_name.upper()}:
                    continue
                start_hm = row.get("start_utc") or row.get("start") or ""
                hm = parse_hhmm(str(start_hm))
                if not hm:
                    continue
                band = (row.get("band") or "").strip().upper()
                label = (row.get("net_name") or row.get("group_name") or "").strip().upper()
                if group_filter and label != group_filter:
                    continue
                if search and search not in label and search not in band:
                    continue
                start_utc = now_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
                start_local = start_utc.astimezone(tz)
                if start_local.date() == today_local:
                    entries.append((start_local, band, label))
        return entries

    def _top_bands_modeled(
        self,
        bands: List[str],
        mid_local: dt.datetime,
        user_ll: Tuple[float, float],
        points: List[Tuple[float, float]],
        origin_grid6: str = "",
        target_type: str = "",
        target_id: str = "",
        blend_settings: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[str, float]]:
        return self._prop_service.top_bands_modeled(
            bands=bands,
            mid_utc=mid_local.astimezone(dt.timezone.utc),
            user_ll=user_ll,
            points=points,
            origin_grid6=origin_grid6,
            target_type=target_type,
            target_id=target_id,
            blend_settings=blend_settings,
            limit=2,
        )

    def _format_band_list(self, bands: List[Tuple[str, float]]) -> str:
        if not bands:
            return ""
        out = []
        for band, score in bands:
            qual = self._score_to_qual(score)
            out.append(f"{band} ({qual})")
        return "/".join(out)

    @staticmethod
    def _score_to_qual(score: float) -> str:
        if score >= 70:
            return "HIGH"
        if score >= 40:
            return "MED"
        return "LOW"

    def _format_best_band(
        self, nat: List[Tuple[str, float]], reg: List[Tuple[str, float]]
    ) -> str:
        if reg:
            band, score = reg[0]
            return f"{band} ({self._score_to_qual(score)})"
        if nat:
            band, score = nat[0]
            return f"{band} ({self._score_to_qual(score)})"
        return ""

    def _set_sectioned_prop_rows(
        self,
        label_a: str,
        rows_a: List[List[str]],
        label_b: str,
        rows_b: List[List[str]],
    ) -> None:
        self.prop_table.setRowCount(0)
        self._append_section_row(label_a)
        self._append_rows(rows_a)
        self._append_section_row(label_b)
        self._append_rows(rows_b)
        self.prop_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

    def _append_section_row(self, text: str) -> None:
        row = self.prop_table.rowCount()
        self.prop_table.insertRow(row)
        item = QTableWidgetItem(text)
        item.setFlags(item.flags() ^ Qt.ItemIsEditable)
        item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        item.setBackground(Qt.lightGray)
        item.setForeground(Qt.black)
        self.prop_table.setItem(row, 0, item)
        self.prop_table.setSpan(row, 0, 1, self.prop_table.columnCount())

    def _append_rows(self, rows: List[List[str]]) -> None:
        for row in rows:
            r = self.prop_table.rowCount()
            self.prop_table.insertRow(r)
            for c, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                self.prop_table.setItem(r, c, item)

    def _sunrise_sunset_local(
        self, date_val: dt.date, lat: float, lon: float, tz: dt.tzinfo
    ) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
        def _calc(is_sunrise: bool) -> Optional[dt.datetime]:
            n = date_val.timetuple().tm_yday
            lng_hour = lon / 15.0
            t = n + ((6 - lng_hour) / 24.0) if is_sunrise else n + ((18 - lng_hour) / 24.0)
            m = (0.9856 * t) - 3.289
            l = m + (1.916 * math.sin(math.radians(m))) + (0.020 * math.sin(math.radians(2 * m))) + 282.634
            l = (l + 360.0) % 360.0
            ra = math.degrees(math.atan(0.91764 * math.tan(math.radians(l))))
            ra = (ra + 360.0) % 360.0
            l_quadrant = (math.floor(l / 90.0)) * 90.0
            ra_quadrant = (math.floor(ra / 90.0)) * 90.0
            ra = ra + (l_quadrant - ra_quadrant)
            ra /= 15.0
            sin_dec = 0.39782 * math.sin(math.radians(l))
            cos_dec = math.cos(math.asin(sin_dec))
            cos_h = (math.cos(math.radians(90.833)) - (sin_dec * math.sin(math.radians(lat)))) / (
                cos_dec * math.cos(math.radians(lat))
            )
            if cos_h > 1 or cos_h < -1:
                return None
            if is_sunrise:
                h = 360.0 - math.degrees(math.acos(cos_h))
            else:
                h = math.degrees(math.acos(cos_h))
            h /= 15.0
            t_local = h + ra - (0.06571 * t) - 6.622
            ut = (t_local - lng_hour) % 24.0
            dt_utc = dt.datetime(
                date_val.year, date_val.month, date_val.day, tzinfo=dt.timezone.utc
            ) + dt.timedelta(hours=ut)
            return dt_utc.astimezone(tz)

        sunrise = _calc(True)
        sunset = _calc(False)
        return sunrise, sunset

    def _modeled_band_score(
        self,
        band: str,
        user_ll: Tuple[float, float],
        dest_lat: float,
        dest_lon: float,
        now_utc: dt.datetime,
        distance_km: float,
    ) -> float:
        return self._prop_service.modeled_band_score(
            band=band,
            user_ll=user_ll,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            now_utc=now_utc,
            distance_km=distance_km,
        )

    def _load_prop_db_cache(self) -> None:
        self._prop_service.load_climatology_cache()

    def _lookup_db_score(self, band: str, lat: float, lon: float, month: int) -> Optional[float]:
        return self._prop_service.lookup_db_score(band, lat, lon, month)

    def _band_score_db(self, band: str, lat: float, lon: float, month: int) -> Optional[float]:
        return self._prop_service.band_score_db(band, lat, lon, month)

    def _band_score(self, band: str, distance_km: float, hour_utc: int) -> float:
        return self._prop_service.band_score(band, distance_km, hour_utc)

    def _diurnal_weight(self, band: str, hour_local: int) -> float:
        return self._prop_service.diurnal_weight(band, hour_local)

    def _local_hour_from_lon(self, utc_dt: dt.datetime, lon: float) -> int:
        return self._prop_service.local_hour_from_lon(utc_dt, lon)

    def _path_band_weight(self, band: str, distance_km: float, hour_local: int) -> float:
        return self._prop_service.path_band_weight(band, distance_km, hour_local)

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        return PropagationService.haversine_km(lat1, lon1, lat2, lon2)

    @staticmethod
    def _set_table_rows(table: QTableWidget, rows: List[List[str]]) -> None:
        table.setRowCount(0)
        for r, row in enumerate(rows):
            table.insertRow(r)
            for c, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                table.setItem(r, c, item)

    @staticmethod
    def _apply_elide_tooltips(table: QTableWidget, col: int) -> None:
        if col < 0:
            return
        width = table.columnWidth(col) - 10
        if width <= 0:
            return
        for r in range(table.rowCount()):
            item = table.item(r, col)
            if item is None:
                continue
            text = item.text()
            fm = QFontMetrics(item.font())
            elided = fm.elidedText(text, Qt.ElideRight, width)
            if elided != text:
                item.setText(elided)
                item.setToolTip(text)
            else:
                item.setToolTip("")
