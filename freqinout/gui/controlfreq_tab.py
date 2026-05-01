from __future__ import annotations

import datetime as dt
import json
import math
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from PySide6.QtCore import Qt, QTimer, QPropertyAnimation, QEasingCurve
from PySide6.QtGui import QFont, QFontMetrics, QShortcut, QKeySequence, QColor
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QGridLayout,
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
    QCompleter,
)

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log
from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.perf_metrics import span as perf_span
from freqinout.core.propagation_service import PropagationService
from freqinout.core.schedule_targeting import (
    normalize_schedule_target_fields,
    schedule_row_matches_target_context,
)
from freqinout.core.sqlite_utils import connect_sqlite, fetch_all, rows_to_dicts, table_exists
from freqinout.core.software_status_service import SoftwareStatusService, PROGRAM_PATH_KEYS
from freqinout.core.station_readiness import (
    build_station_readiness_report,
    format_readiness_issue,
    readiness_report_detail_text,
    readiness_report_overall_text,
    readiness_state_label,
    should_show_startup_review,
    visible_status_programs,
)
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sop_manager import SOPManager
from freqinout.utils.timezones import get_timezone
from freqinout.gui.qsy_helper import (
    load_operating_groups,
    selected_qsy_meta,
    perform_qsy,
    perform_qsy_with_hold,
    current_scheduler_freq,
    refresh_hold_duration_combo,
    selected_hold_duration,
    set_hold_duration_default,
    notify_hold_duration_default_changed,
    suspend_snapshot,
    resume_schedule_hold,
    active_hold_button_role,
    active_hold_button_text,
    active_hold_status_text,
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
from freqinout.gui.help_registry import resolve_help_host
from freqinout.gui.theme import resolve_theme, button_style, led_style
from freqinout.version import __version__


class ControlFreqTab(QWidget):
    """
    ControlFreq: summary/console view for activity and operational status.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._sop_manager = SOPManager()
        self._timer: Optional[QTimer] = None
        self._active = False
        self._last_refresh_ts = 0.0
        self._freq_timer: Optional[QTimer] = None
        self._status_timer: Optional[QTimer] = None
        self._clock_timer: Optional[QTimer] = None
        self._show_local = True
        self._intersection_cache_ts = 0.0
        self._intersection_cache_key: Tuple[str, str] = ("", "")
        self._intersection_cache_rows: List[List[str]] = []
        self._prop_target_syncing = False
        self._prop_operator_geo: Dict[str, Dict[str, str]] = {}
        self._focus_mode = False
        self._freq_ctrl_fixed_height = 0
        self._pending_group_filter = ""
        self._saved_top_sizes: List[int] = []
        self._saved_left_sizes: List[int] = []
        self._saved_right_sizes: List[int] = []
        self._schedule_entries_by_row: Dict[int, Dict[str, Any]] = {}
        self._next_schedule_outlook_preview: Optional[Dict[str, Any]] = None
        self._force_hero_resync = False
        self._message_summary_target_height = 0
        self._freq_meta_full_text = "Scheduled: -- | Active: --"
        self._hero_combo_font_px = 18
        self._primary_freq_action_mode = "none"
        self._freq_action_busy_reason_label: Optional[str] = None
        self._hold_state_snapshot: Optional[Dict[str, object]] = None
        self._freq_combo_cache_key: Tuple[Tuple[str, str, float, str, str, bool], ...] = ()
        self._last_hero_sched_freq_mhz: Optional[float] = None
        self._last_hero_display_freq_mhz: Optional[float] = None
        self._current_time_full_text = "--"
        self._operator_groups_cache: Dict[str, Set[str]] = {}
        self._operator_groups_cache_ts = 0.0
        self._operator_groups_cache_mtime = 0.0
        self._operator_groups_cache_ttl_sec = 20.0
        self._activity_cache_key: Tuple[Any, ...] = ()
        self._activity_cache_rows: List[List[str]] = []
        self._activity_cache_ts = 0.0
        self._activity_cache_ttl_sec = 10.0
        self._my_schedule_entries_cache: List[Dict[str, object]] = []
        self._my_schedule_entries_cache_ts = 0.0
        self._my_schedule_entries_cache_key: Tuple[float, float] = (0.0, 0.0)
        self._my_schedule_entries_cache_ttl_sec = 20.0
        self._daily_schedule_rows_cache: List[Dict[str, Any]] = []
        self._daily_schedule_rows_cache_ts = 0.0
        self._daily_schedule_rows_cache_mtime = 0.0
        self._daily_schedule_rows_cache_ttl_sec = 10.0
        self._net_schedule_rows_cache: List[Dict[str, Any]] = []
        self._net_schedule_rows_cache_ts = 0.0
        self._net_schedule_rows_cache_mtime = 0.0
        self._net_schedule_rows_cache_ttl_sec = 10.0
        self._peer_schedule_rows_cache: List[Dict[str, Any]] = []
        self._peer_schedule_rows_cache_ts = 0.0
        self._peer_schedule_rows_cache_mtime = 0.0
        self._peer_schedule_rows_cache_ttl_sec = 20.0
        self._view_cards: Dict[str, bool] = {
            "activity": True,
            "intersections": True,
            "schedule": True,
            "propagation": True,
        }
        self._view_preset = "All"
        self._view_syncing = False
        self._card_expanded_heights: Dict[str, int] = {}
        self._card_animations: Dict[str, QPropertyAnimation] = {}
        self.status_labels: Dict[str, QLabel] = {}
        self._status_text_labels: Dict[str, QLabel] = {}
        self._status_checked_at: Dict[str, str] = {}
        self._status_service = SoftwareStatusService(self.settings)
        self._multi_radio_store = MultiRadioStore()
        self._readiness_banner_dismissed = False
        self._readiness_banner_digest = ""
        self._readiness_suppressed_version = str(self.settings.get("readiness_review_suppressed_version", "") or "").strip()
        self._readiness_dismissed_digest = str(self.settings.get("readiness_review_dismissed_digest", "") or "").strip()
        self._sop_window_cache: Dict[Tuple[Any, ...], Tuple[float, List[Dict[str, Any]]]] = {}
        self._sop_today_cache_ttl_sec = 30.0
        self._sop_tomorrow_cache_ttl_sec = 180.0
        self._sop_cache_epoch = 0
        self._sop_outlook_refresh_pending = False
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
        self._filter_refresh_timer = QTimer(self)
        self._filter_refresh_timer.setSingleShot(True)
        self._filter_refresh_timer.timeout.connect(self._run_filter_refresh)
        self._activation_refresh_pending = False
        self._activation_refresh_interval_sec = 60.0
        self._secondary_refresh_pending = False
        self._secondary_refresh_interval_sec = 60.0
        self._last_secondary_refresh_ts = 0.0
        self._heavy_refresh_pending = False
        self._heavy_refresh_interval_sec = 120.0
        self._last_heavy_refresh_ts = 0.0
        self._build_ui()
        refresh_hold_duration_combo(self.hold_duration_combo, self.settings)
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
        self.help_btn = QPushButton("Help")
        self.help_btn.setToolTip("Open ControlFreq help.")
        self.help_btn.clicked.connect(lambda: self._open_context_help("tab.controlfreq"))
        header.addWidget(self.help_btn)

        header.addStretch(1)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter by keyword...")
        self.search_edit.textChanged.connect(self._on_filters_changed)
        self.search_edit.setMinimumWidth(340)
        self.search_edit.setMaximumWidth(420)
        header.addWidget(self.search_edit)

        self.group_combo = QComboBox()
        self.group_combo.setMinimumWidth(180)
        self.group_combo.currentIndexChanged.connect(self._on_filters_changed)
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
        self.focus_mode_btn.setVisible(False)

        root.addLayout(header)

        self.readiness_review_widget = QWidget()
        readiness_layout = QVBoxLayout(self.readiness_review_widget)
        readiness_layout.setContentsMargins(10, 8, 10, 8)
        readiness_layout.setSpacing(8)
        self.readiness_review_label = QLabel()
        self.readiness_review_label.setWordWrap(True)
        readiness_layout.addWidget(self.readiness_review_label)
        readiness_actions = QGridLayout()
        readiness_actions.setContentsMargins(0, 0, 0, 0)
        readiness_actions.setHorizontalSpacing(8)
        readiness_actions.setVerticalSpacing(8)
        self.readiness_review_now_btn = QPushButton("Review Now")
        self.readiness_review_now_btn.clicked.connect(self._review_readiness_now)
        readiness_actions.addWidget(self.readiness_review_now_btn, 0, 0)
        self.readiness_review_copy_btn = QPushButton("Copy Summary")
        self.readiness_review_copy_btn.clicked.connect(self._copy_readiness_review_summary)
        readiness_actions.addWidget(self.readiness_review_copy_btn, 0, 1)
        self.readiness_review_dismiss_btn = QPushButton("Dismiss")
        self.readiness_review_dismiss_btn.clicked.connect(self._dismiss_readiness_review)
        readiness_actions.addWidget(self.readiness_review_dismiss_btn, 0, 2)
        self.readiness_review_suppress_btn = QPushButton("Do Not Remind Again For This Version")
        self.readiness_review_suppress_btn.clicked.connect(self._suppress_readiness_review_for_version)
        readiness_actions.addWidget(self.readiness_review_suppress_btn, 1, 0, 1, 3)
        readiness_actions.setColumnStretch(3, 1)
        readiness_layout.addLayout(readiness_actions)
        self.readiness_review_widget.setVisible(False)
        root.addWidget(self.readiness_review_widget)

        updated_row = QHBoxLayout()

        self.status_group = QGroupBox("Operating Status")
        self.status_group.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
        self.status_layout = QHBoxLayout()
        self.status_group.setLayout(self.status_layout)
        self._rebuild_status_indicators()
        updated_row.addWidget(self.status_group)
        right_status_col = QVBoxLayout()
        right_status_col.setContentsMargins(0, 0, 0, 0)
        right_status_col.setSpacing(6)
        self.current_time_label = QLabel("--")
        self.current_time_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.current_time_label.setMinimumWidth(160)
        self.current_time_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.current_time_label.setStyleSheet("font-size: 14px; font-weight: 600;")
        right_status_col.addWidget(self.current_time_label)
        self.updated_label = QLabel("Last updated: --")
        self.updated_label.setVisible(False)
        updated_row.addLayout(right_status_col, 1)
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
        self._set_message_summary_visible_rows(7)

        self.left_splitter = QSplitter(Qt.Vertical)
        self.left_splitter.setChildrenCollapsible(False)
        self.left_splitter.addWidget(self.activity_box)
        self.left_splitter.addWidget(self.intersection_box)
        left_layout.addWidget(self.left_splitter)
        self.top_splitter.addWidget(self.left_col)

        self.right_col = QWidget()
        right_layout = QVBoxLayout(self.right_col)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        self.freq_ctrl_box = QGroupBox("Frequency Control")
        freq_layout = QVBoxLayout(self.freq_ctrl_box)
        freq_layout.setContentsMargins(8, 8, 8, 8)
        freq_layout.setSpacing(4)
        hero_row = QHBoxLayout()
        hero_row.setContentsMargins(0, 0, 0, 0)
        hero_row.setSpacing(6)
        hero_row.addStretch(1)
        self.freq_state_badge = QLabel("Unknown")
        self.freq_state_badge.setAlignment(Qt.AlignCenter)
        self.freq_state_badge.setMinimumWidth(108)
        self.freq_state_badge.setMinimumHeight(26)
        self.freq_state_badge.setMaximumHeight(26)
        self.freq_state_badge.setStyleSheet(
            "font-size: 12px; font-weight: 600; border-radius: 6px; padding: 0 8px;"
        )
        hero_row.addWidget(self.freq_state_badge)
        freq_layout.addLayout(hero_row)
        self.freq_combo = QComboBox()
        self.freq_combo.setMinimumHeight(40)
        self.freq_combo.setMaximumHeight(40)
        self.freq_combo.setMinimumWidth(220)
        self.freq_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.freq_combo.currentIndexChanged.connect(self._on_freq_selection_changed)
        freq_layout.addWidget(self.freq_combo)
        self.freq_meta_label = QLabel(self._freq_meta_full_text)
        self.freq_meta_label.setWordWrap(False)
        self.freq_meta_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.freq_meta_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.freq_meta_label.setStyleSheet("font-size: 12px;")
        self.freq_meta_label.setToolTip(self._freq_meta_full_text)
        freq_layout.addWidget(self.freq_meta_label)
        self.effective_source_label = QLabel("Active Source: --")
        self.effective_source_label.setWordWrap(False)
        self.effective_source_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.effective_source_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.effective_source_label.setStyleSheet("color: #888;")
        freq_layout.addWidget(self.effective_source_label)
        self.next_change_label = QLabel("Next Change: --")
        self.next_change_label.setWordWrap(False)
        self.next_change_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.next_change_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.next_change_label.setStyleSheet("color: #888;")
        freq_layout.addWidget(self.next_change_label)
        try:
            freq_layout.addSpacing(max(6, int(self.fontMetrics().height() * 0.50)))
        except Exception:
            freq_layout.addSpacing(8)
        btn_row = QHBoxLayout()
        btn_row.setContentsMargins(0, 0, 0, 0)
        btn_row.setSpacing(8)
        btn_row.addWidget(self.freq_state_badge)
        self.hold_duration_combo = QComboBox()
        self.hold_duration_combo.setMinimumWidth(88)
        self.hold_duration_combo.setMaximumWidth(104)
        self.hold_duration_combo.setToolTip("Temporary schedule hold duration after QSY.")
        self.hold_duration_combo.currentIndexChanged.connect(self._on_hold_duration_changed)
        btn_row.addWidget(self.hold_duration_combo)
        self.freq_action_btn = QPushButton("QSY + Hold")
        self.freq_action_btn.clicked.connect(self._on_primary_freq_action_clicked)
        self.freq_action_btn.setMinimumHeight(26)
        self.freq_action_btn.setMinimumWidth(132)
        self.freq_action_btn.setMaximumWidth(170)
        btn_row.addWidget(self.freq_action_btn)
        btn_row.addStretch(1)
        freq_layout.addLayout(btn_row)

        top_overview_row = QHBoxLayout()
        self.freq_ctrl_box.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Preferred)
        self.freq_ctrl_box.setMinimumWidth(380)
        self.freq_ctrl_box.setMaximumWidth(540)
        self.inbox_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        top_overview_row.addWidget(self.freq_ctrl_box, 0)
        top_overview_row.addWidget(self.inbox_box, 1)
        root.addLayout(top_overview_row)
        self._lock_frequency_control_height()

        view_row = QHBoxLayout()
        view_row.addWidget(QLabel("View"))
        self.view_preset_combo = QComboBox()
        self.view_preset_combo.addItem("Operations", "Operations")
        self.view_preset_combo.addItem("All", "All")
        self.view_preset_combo.addItem("Traffic", "Traffic")
        self.view_preset_combo.addItem("Schedule", "Schedule")
        self.view_preset_combo.addItem("Propagation", "Propagation")
        self.view_preset_combo.addItem("Custom", "Custom")
        self.view_preset_combo.setMinimumWidth(150)
        self.view_preset_combo.currentIndexChanged.connect(self._on_view_preset_changed)
        view_row.addWidget(self.view_preset_combo)
        self.view_chip_buttons: Dict[str, QPushButton] = {}
        for key, label in (
            ("activity", "Activity"),
            ("intersections", "Intersections"),
            ("schedule", "Schedule"),
            ("propagation", "Propagation"),
        ):
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.toggled.connect(lambda checked, k=key: self._on_view_chip_toggled(k, checked))
            self.view_chip_buttons[key] = btn
            view_row.addWidget(btn)
        view_row.addStretch(1)
        root.addLayout(view_row)

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
        self.left_splitter.setSizes([1, 1])
        self.right_splitter.setSizes([1])
        self.top_splitter.setSizes([480, 560])
        root.addWidget(self.top_splitter, 5)

        # Bottom region: full-width propagation forecast
        self.bottom_row = QWidget()
        row2 = QHBoxLayout(self.bottom_row)
        row2.setContentsMargins(0, 0, 0, 0)
        row2.setSpacing(0)

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
        self._prop_target_completer = QCompleter(self.prop_target_value_combo.model(), self)
        self._prop_target_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._prop_target_completer.setFilterMode(Qt.MatchContains)
        self.prop_target_value_combo.setCompleter(self._prop_target_completer)
        if self.prop_target_value_combo.lineEdit():
            self.prop_target_value_combo.lineEdit().setPlaceholderText("Type to search...")
            self.prop_target_value_combo.lineEdit().setClearButtonEnabled(True)
        self.prop_target_value_combo.currentTextChanged.connect(self._on_prop_target_value_changed)
        target_row.addWidget(self.prop_target_value_combo, 1)
        self.prop_hint = QLabel("Model uses today's schedule bands.")
        self.prop_hint.setStyleSheet("color: #666;")
        self.prop_hint.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        target_row.addWidget(self.prop_hint, 1)
        prop_layout.addLayout(target_row)
        self.prop_table = QTableWidget(0, 4)
        self.prop_table.setHorizontalHeaderLabels(
            ["Zone", "Morning (Dawn-10:00)", "Day (10:00-Sunset)", "Night (Sunset-Dawn)"]
        )
        self._setup_table_defaults(self.prop_table)
        self.prop_table.horizontalHeader().setStretchLastSection(True)
        self.prop_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        prop_layout.addWidget(self.prop_table)
        row2.addWidget(self.prop_box, 1)
        root.addWidget(self.bottom_row, 2)

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
        self.freq_action_btn.setToolTip("QSY now and pause schedule control for the selected duration (Ctrl+Enter)")

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

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._apply_freq_meta_text()

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
            self.activity_box.setMinimumHeight(0)
            self.activity_box.setMaximumHeight(16777215)
            h_freq = max(140, int(self.freq_ctrl_box.sizeHint().height()))
            h_inbox = max(
                140,
                int(self._message_summary_target_height or 0),
                int(self.inbox_box.sizeHint().height()),
            )
            top_h = max(h_freq, h_inbox)
            self._freq_ctrl_fixed_height = top_h
            self.freq_ctrl_box.setMinimumHeight(top_h)
            self.freq_ctrl_box.setMaximumHeight(top_h)
            self.inbox_box.setMinimumHeight(top_h)
            self.inbox_box.setMaximumHeight(top_h)
        except Exception:
            pass

    def _set_message_summary_visible_rows(self, rows: int = 7) -> None:
        try:
            rows = max(1, int(rows))
            header_h = max(
                int(self.inbox_table.horizontalHeader().height()),
                int(self.inbox_table.horizontalHeader().sizeHint().height()),
                22,
            )
            row_h = max(int(self.inbox_table.verticalHeader().defaultSectionSize()), 18)
            frame_h = int(self.inbox_table.frameWidth()) * 2
            target_h = header_h + (row_h * rows) + frame_h + 4
            self._message_summary_target_height = max(
                target_h,
                int(self.inbox_box.sizeHint().height()),
            )
            self.inbox_table.setMinimumHeight(target_h)
            self.inbox_table.setMaximumHeight(target_h)
            self._sync_top_panel_heights()
        except Exception:
            pass

    def _schedule_persist_ui_state(self, *_args) -> None:
        if hasattr(self, "_prefs_timer"):
            self._prefs_timer.start(300)

    def _persist_ui_state(self) -> None:
        try:
            self._saved_top_sizes = list(self.top_splitter.sizes())
            self._saved_left_sizes = list(self.left_splitter.sizes())
            self._saved_right_sizes = list(self.right_splitter.sizes())
            values = {
                "controlfreq_show_local": bool(self._show_local),
                "controlfreq_focus_mode": bool(self._focus_mode),
                "controlfreq_search": (self.search_edit.text() or "").strip(),
                "controlfreq_group_filter": (self.group_combo.currentData() or "").strip().upper(),
                "controlfreq_activity_window_min": int(self.activity_window_combo.currentData() or 120),
                "controlfreq_view_preset": str(self._view_preset or "Schedule"),
                "controlfreq_view_cards": dict(self._view_cards or {}),
                "controlfreq_top_splitter_sizes": list(self._saved_top_sizes),
                "controlfreq_left_splitter_sizes": list(self._saved_left_sizes),
                "controlfreq_right_splitter_sizes": list(self._saved_right_sizes),
            }
            self.settings.set_many(values)
        except Exception as e:
            log.debug("ControlFreq: failed to persist UI state: %s", e)

    def _restore_ui_state(self) -> None:
        try:
            self._show_local = bool(self.settings.get("controlfreq_show_local", self._show_local))
            self._focus_mode = False
            self._pending_group_filter = (
                str(self.settings.get("controlfreq_group_filter", "") or "").strip().upper()
            )
            saved_view_cards = self.settings.get("controlfreq_view_cards", None)
            if isinstance(saved_view_cards, dict) and saved_view_cards:
                self._view_cards = self._normalized_view_cards(saved_view_cards)
            else:
                self._view_cards = dict(self._view_presets().get("Schedule", {}))
            saved_preset = str(self.settings.get("controlfreq_view_preset", "Schedule") or "Schedule").strip()
            self._view_preset = self._preset_for_view_cards(self._view_cards)
            if saved_preset == "Custom":
                self._view_preset = "Custom"
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
            QTimer.singleShot(0, self._finalize_restored_ui_state)
        except Exception as e:
            log.debug("ControlFreq: failed to restore UI state: %s", e)

    def _finalize_restored_ui_state(self) -> None:
        self._apply_saved_splitter_sizes()
        self._sync_view_controls_from_state()
        self._apply_view_state(animated=False)

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
        self._focus_mode = False
        self._apply_focus_mode()

    def _apply_focus_mode(self) -> None:
        self._focus_mode = False
        self.focus_mode_btn.setText("Focus Mode: Off")
        try:
            theme = resolve_theme(self.settings)
            self.focus_mode_btn.setStyleSheet(button_style("secondary", theme))
        except Exception:
            pass
        # Legacy compatibility only; visibility is controlled by the View bar.
        self.status_group.setVisible(True)
        self.inbox_box.setVisible(True)
        self._sync_view_controls_from_state()
        self._apply_view_state(animated=False)
        self._sync_top_panel_heights()

    @staticmethod
    def _view_presets() -> Dict[str, Dict[str, bool]]:
        return {
            "Operations": {
                "activity": True,
                "intersections": True,
                "schedule": True,
                "propagation": False,
            },
            "All": {
                "activity": True,
                "intersections": True,
                "schedule": True,
                "propagation": True,
            },
            "Traffic": {
                "activity": True,
                "intersections": True,
                "schedule": False,
                "propagation": False,
            },
            "Schedule": {
                "activity": False,
                "intersections": True,
                "schedule": True,
                "propagation": False,
            },
            "Propagation": {
                "activity": False,
                "intersections": False,
                "schedule": False,
                "propagation": True,
            },
        }

    def _normalized_view_cards(self, raw: object) -> Dict[str, bool]:
        defaults = dict(self._view_presets().get("All", {}))
        if isinstance(raw, dict):
            for key in defaults:
                if key in raw:
                    defaults[key] = bool(raw.get(key))
        if not any(defaults.values()):
            defaults["activity"] = True
        return defaults

    def _preset_for_view_cards(self, cards: Dict[str, bool]) -> str:
        norm = self._normalized_view_cards(cards)
        for preset, preset_cards in self._view_presets().items():
            if all(bool(norm.get(k)) == bool(preset_cards.get(k)) for k in preset_cards):
                return preset
        return "Custom"

    def _card_widget_map(self) -> Dict[str, QWidget]:
        return {
            "activity": self.activity_box,
            "intersections": self.intersection_box,
            "schedule": self.schedule_box,
            "propagation": self.bottom_row,
        }

    def _card_target_height(self, key: str, widget: QWidget) -> int:
        min_heights = {
            "activity": 180,
            "intersections": 170,
            "schedule": 220,
            "propagation": 220,
        }
        return max(
            int(min_heights.get(key, 160)),
            int(self._card_expanded_heights.get(key, 0) or 0),
            int(widget.sizeHint().height()),
            int(widget.height()),
        )

    def _stop_card_animation(self, key: str) -> None:
        anim = self._card_animations.pop(key, None)
        if anim:
            try:
                anim.stop()
            except Exception:
                pass

    def _set_card_visibility_now(self, key: str, visible: bool) -> None:
        widget = self._card_widget_map().get(key)
        if widget is None:
            return
        self._stop_card_animation(key)
        if visible:
            widget.setVisible(True)
            widget.setMinimumHeight(0)
            widget.setMaximumHeight(16777215)
            self._card_expanded_heights[key] = self._card_target_height(key, widget)
        else:
            self._card_expanded_heights[key] = self._card_target_height(key, widget)
            widget.setVisible(False)
            widget.setMinimumHeight(0)
            widget.setMaximumHeight(16777215)

    def _on_card_animation_finished(
        self, key: str, widget: QWidget, visible: bool, target_height: int
    ) -> None:
        try:
            if visible:
                widget.setVisible(True)
                widget.setMinimumHeight(0)
                widget.setMaximumHeight(16777215)
                self._card_expanded_heights[key] = max(
                    int(self._card_expanded_heights.get(key, 0) or 0),
                    int(target_height),
                )
            else:
                widget.setVisible(False)
                widget.setMinimumHeight(0)
                widget.setMaximumHeight(16777215)
        finally:
            self._card_animations.pop(key, None)
            self._rebalance_main_card_layout()
            self._sync_top_panel_heights()

    def _animate_card_visibility(self, key: str, visible: bool) -> None:
        widget = self._card_widget_map().get(key)
        if widget is None:
            return
        if visible and widget.isVisible():
            return
        if (not visible) and (not widget.isVisible()):
            return
        self._stop_card_animation(key)
        target_height = self._card_target_height(key, widget)
        if visible:
            widget.setVisible(True)
            widget.setMinimumHeight(0)
            widget.setMaximumHeight(0)
            anim = QPropertyAnimation(widget, b"maximumHeight", self)
            anim.setDuration(170)
            anim.setEasingCurve(QEasingCurve.OutCubic)
            anim.setStartValue(0)
            anim.setEndValue(target_height)
        else:
            start_h = max(1, int(widget.height()))
            self._card_expanded_heights[key] = max(
                int(self._card_expanded_heights.get(key, 0) or 0),
                int(start_h),
            )
            widget.setMaximumHeight(start_h)
            anim = QPropertyAnimation(widget, b"maximumHeight", self)
            anim.setDuration(140)
            anim.setEasingCurve(QEasingCurve.InCubic)
            anim.setStartValue(start_h)
            anim.setEndValue(0)
        self._card_animations[key] = anim
        anim.finished.connect(
            lambda k=key, w=widget, show=visible, h=target_height: self._on_card_animation_finished(k, w, show, h)
        )
        anim.start()

    def _rebalance_main_card_layout(self) -> None:
        left_activity = bool(self._view_cards.get("activity"))
        left_intersections = bool(self._view_cards.get("intersections"))
        right_schedule = bool(self._view_cards.get("schedule"))
        left_visible = left_activity or left_intersections
        right_visible = right_schedule
        self.left_col.setVisible(left_visible)
        self.right_col.setVisible(right_visible)
        self.top_splitter.setVisible(left_visible or right_visible)
        if left_visible:
            if left_activity and left_intersections:
                self.left_splitter.setSizes([1, 1])
            elif left_activity:
                self.left_splitter.setSizes([1, 0])
            else:
                self.left_splitter.setSizes([0, 1])
        if left_visible or right_visible:
            if left_visible and right_visible:
                self.top_splitter.setSizes([1, 1])
            elif left_visible:
                self.top_splitter.setSizes([1, 0])
            else:
                self.top_splitter.setSizes([0, 1])

    def _apply_view_state(self, *, animated: bool) -> None:
        self._view_cards = self._normalized_view_cards(self._view_cards)
        if animated:
            left_target = bool(self._view_cards.get("activity")) or bool(self._view_cards.get("intersections"))
            right_target = bool(self._view_cards.get("schedule"))
            if left_target:
                self.left_col.setVisible(True)
            if right_target:
                self.right_col.setVisible(True)
            if left_target or right_target:
                self.top_splitter.setVisible(True)
        for key, visible in self._view_cards.items():
            if animated:
                self._animate_card_visibility(key, bool(visible))
            else:
                self._set_card_visibility_now(key, bool(visible))
        if not animated:
            self._rebalance_main_card_layout()
            self._sync_top_panel_heights()

    def _sync_view_controls_from_state(self) -> None:
        if not hasattr(self, "view_preset_combo") or not hasattr(self, "view_chip_buttons"):
            return
        self._view_syncing = True
        try:
            preset = self._preset_for_view_cards(self._view_cards)
            if self._view_preset != "Custom":
                self._view_preset = preset
            combo_preset = self._view_preset if self._view_preset in {"Operations", "All", "Traffic", "Schedule", "Propagation", "Custom"} else "Custom"
            idx = self.view_preset_combo.findData(combo_preset)
            if idx >= 0 and self.view_preset_combo.currentIndex() != idx:
                self.view_preset_combo.setCurrentIndex(idx)
            for key, btn in self.view_chip_buttons.items():
                btn.setChecked(bool(self._view_cards.get(key, False)))
        finally:
            self._view_syncing = False
        self._update_view_chip_styles()

    def _on_view_preset_changed(self, _idx: int) -> None:
        if self._view_syncing:
            return
        preset = str(self.view_preset_combo.currentData() or "").strip()
        if not preset or preset == "Custom":
            return
        preset_cards = self._view_presets().get(preset)
        if not isinstance(preset_cards, dict):
            return
        previous_cards = dict(self._view_cards)
        self._view_cards = self._normalized_view_cards(preset_cards)
        self._view_preset = preset
        self._sync_view_controls_from_state()
        self._apply_view_state(animated=True)
        self._refresh_newly_visible_cards(previous_cards)
        self._schedule_persist_ui_state()

    def _on_view_chip_toggled(self, key: str, checked: bool) -> None:
        if self._view_syncing:
            return
        next_cards = dict(self._view_cards)
        next_cards[key] = bool(checked)
        if not any(next_cards.values()):
            self._view_syncing = True
            try:
                btn = self.view_chip_buttons.get(key)
                if btn is not None:
                    btn.setChecked(True)
            finally:
                self._view_syncing = False
            return
        previous_cards = dict(self._view_cards)
        self._view_cards = self._normalized_view_cards(next_cards)
        self._view_preset = self._preset_for_view_cards(self._view_cards)
        self._sync_view_controls_from_state()
        self._apply_view_state(animated=True)
        self._refresh_newly_visible_cards(previous_cards)
        self._schedule_persist_ui_state()

    def _refresh_newly_visible_cards(self, previous_cards: Dict[str, bool]) -> None:
        try:
            if bool(self._view_cards.get("propagation", False)) and not bool(previous_cards.get("propagation", False)):
                # Trigger immediate target + forecast refresh when propagation is first shown.
                QTimer.singleShot(0, self._refresh_prop_target_controls)
                QTimer.singleShot(0, self._refresh_propagation_snapshot)
        except Exception as e:
            log.debug("ControlFreq: failed to refresh newly visible cards: %s", e)

    def _update_view_chip_styles(self, theme: Optional[Dict[str, str]] = None) -> None:
        if not hasattr(self, "view_chip_buttons"):
            return
        if theme is None:
            try:
                theme = resolve_theme(self.settings)
            except Exception:
                theme = {}
        for key, btn in self.view_chip_buttons.items():
            role = "info" if bool(self._view_cards.get(key)) else "muted"
            try:
                btn.setStyleSheet(button_style(role, theme))
            except Exception:
                pass

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
            self.help_btn.setStyleSheet(button_style("secondary", theme))
            self.refresh_btn.setStyleSheet(button_style("muted", theme))
            self.clear_filters_btn.setStyleSheet(button_style("muted", theme))
            self.freq_action_btn.setStyleSheet(button_style("muted", theme))
            self._update_time_toggle_style(theme)
            self.focus_mode_btn.setStyleSheet(button_style("secondary", theme))
            self._update_view_chip_styles(theme)
            self._update_clear_filters_style()
            self.current_time_label.setStyleSheet(
                f"font-size: 14px; font-weight: 600; color: {theme.get('text', '#111')};"
            )
            if hasattr(self, "schedule_action_hint"):
                self.schedule_action_hint.setStyleSheet(f"color: {theme.get('text_muted', '#888')};")
            self.effective_source_label.setStyleSheet(f"color: {theme.get('text_muted', '#888')};")
        except Exception:
            pass
        self._apply_frequency_display_style()
        self._set_message_summary_visible_rows(7)
        self._lock_frequency_control_height()
        self._update_time_toggle_text()
        self._refresh_clock_display()
        self._apply_focus_mode()
        self._on_freq_selection_changed()
        self._refresh_running_status()

    def _open_context_help(self, context_key: str) -> None:
        host = resolve_help_host(self)
        if host is not None and hasattr(host, "open_context_help"):
            try:
                host.open_context_help(context_key)
            except Exception:
                pass

    def _apply_frequency_display_style(self) -> None:
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = {}
        text_color = str(theme.get("text", "#F2F2F2" if self._is_dark_theme() else "#111111"))
        muted_color = str(theme.get("text_muted", "#888"))
        digital_font = QFont("Consolas")
        if not digital_font.exactMatch():
            digital_font = QFont("Courier New")
        if not digital_font.exactMatch():
            digital_font = QFont("Monospace")
        digital_font.setStyleHint(QFont.Monospace)
        digital_font.setPixelSize(max(14, int(self._hero_combo_font_px)))
        digital_font.setBold(True)
        self.freq_combo.setFont(digital_font)
        self.freq_combo.setStyleSheet(
            f"QComboBox {{ color: {text_color}; padding: 4px 24px 4px 8px; }}"
            f"QComboBox QAbstractItemView {{ color: {text_color}; }}"
        )
        combo_h = max(36, int(QFontMetrics(digital_font).height()) + 14)
        self.freq_combo.setMinimumHeight(combo_h)
        self.freq_combo.setMaximumHeight(combo_h)
        popup_font = QFont(self.font())
        popup_font.setBold(False)
        self._freq_popup_font = popup_font
        try:
            self.freq_combo.view().setFont(popup_font)
        except Exception:
            pass
        self.freq_meta_label.setStyleSheet(f"font-size: 12px; color: {muted_color};")
        self._apply_freq_meta_text()
        self._sync_frequency_info_row_heights()
        self._set_frequency_state_badge("unknown")

    def _apply_freq_meta_text(self) -> None:
        full = str(self._freq_meta_full_text or "").strip()
        if not full:
            full = "Scheduled: -- | Active: --"
        try:
            fm = QFontMetrics(self.freq_meta_label.font())
            avail = max(80, int(self.freq_meta_label.width()) - 6)
            display = fm.elidedText(full, Qt.ElideRight, avail)
            line_h = max(18, int(fm.height()) + 4)
            self.freq_meta_label.setMinimumHeight(line_h)
            self.freq_meta_label.setMaximumHeight(line_h)
        except Exception:
            display = full
        self.freq_meta_label.setText(display)
        self.freq_meta_label.setToolTip(full)

    def _sync_frequency_info_row_heights(self) -> None:
        labels = [
            getattr(self, "freq_meta_label", None),
            getattr(self, "effective_source_label", None),
            getattr(self, "next_change_label", None),
        ]
        for label in labels:
            if not isinstance(label, QLabel):
                continue
            try:
                fm = QFontMetrics(label.font())
                row_h = max(18, int(fm.height()) + 4)
                label.setMinimumHeight(row_h)
                label.setMaximumHeight(row_h)
            except Exception:
                continue

    def _set_frequency_state_badge(self, state: str) -> None:
        key = (state or "").strip().lower()
        if key not in {"on", "off", "blocked", "unknown"}:
            key = "unknown"
        labels = {
            "on": "On Schedule",
            "off": "Off Schedule",
            "blocked": "Blocked",
            "unknown": "Unknown",
        }
        dark = self._is_dark_theme()
        colors = {
            "on": ("#1B5E20", "#D7FFD9") if dark else ("#DFF6E4", "#1B5E20"),
            "off": ("#8A5A00", "#FFF1CC") if dark else ("#FFF3D6", "#8A5A00"),
            "blocked": ("#8B1E1E", "#FFD6D6") if dark else ("#FFE2E2", "#8B1E1E"),
            "unknown": ("#455A64", "#E6EEF2") if dark else ("#EAF2FF", "#1E3A5F"),
        }
        bg, fg = colors.get(key, colors["unknown"])
        self.freq_state_badge.setText(labels.get(key, "Unknown"))
        self.freq_state_badge.setStyleSheet(
            f"font-size: 12px; font-weight: 600; border-radius: 6px; "
            f"padding: 0 8px; background: {bg}; color: {fg};"
        )

    def apply_theme(self) -> None:
        self._apply_theme()

    def set_tab_active(self, active: bool) -> None:
        self._active = bool(active)
        if self._active:
            self._reload_sop_manager_settings()
            if self._timer is None:
                self._timer = QTimer(self)
                self._timer.timeout.connect(self._on_periodic_refresh_tick)
            self._timer.start(60_000)
            if self._freq_timer is None:
                self._freq_timer = QTimer(self)
                self._freq_timer.timeout.connect(self._refresh_frequency_control_tick)
            self._freq_timer.start(2000)
            if self._status_timer is None:
                self._status_timer = QTimer(self)
                self._status_timer.timeout.connect(self._refresh_status_widgets)
            self._status_timer.start(5000)
            if self._clock_timer is None:
                self._clock_timer = QTimer(self)
                self._clock_timer.timeout.connect(self._refresh_clock_display)
            self._clock_timer.start(1000)
            # Keep tab switch snappy: defer initial refresh work until after
            # the screen change event has returned to the UI loop.
            QTimer.singleShot(0, self._refresh_frequency_control_tick)
            # Slightly delay status probing so first paint is not blocked.
            QTimer.singleShot(150, self._refresh_status_widgets)
            QTimer.singleShot(0, self._refresh_clock_display)
            if self._sop_outlook_refresh_pending and bool(self._view_cards.get("schedule", True)):
                self._sop_outlook_refresh_pending = False
                QTimer.singleShot(0, self._refresh_schedule_outlook)
            return
        if self._timer:
            self._timer.stop()
        if self._freq_timer:
            self._freq_timer.stop()
        if self._status_timer:
            self._status_timer.stop()
        if self._clock_timer:
            self._clock_timer.stop()

    def on_tab_activated(self) -> None:
        with perf_span("controlfreq.on_tab_activated", settings=self.settings, min_ms=5.0):
            now = time.time()
            stale = (self._last_refresh_ts <= 0.0) or (
                now - float(self._last_refresh_ts) >= self._activation_refresh_interval_sec
            )
            if stale and not self._activation_refresh_pending:
                self._activation_refresh_pending = True
                QTimer.singleShot(0, self._run_activation_refresh)

    def _refresh_frequency_control_tick(self) -> None:
        self._refresh_frequency_control(include_intersections=False)

    def _on_periodic_refresh_tick(self) -> None:
        # Keep periodic refresh non-blocking; defer heavy sections.
        self._refresh_all(include_secondary=False, include_heavy=False, include_status=False)
        self._schedule_deferred_secondary_refresh(force=False)

    def _run_activation_refresh(self) -> None:
        try:
            with perf_span("controlfreq.activation_refresh", settings=self.settings, min_ms=10.0):
                # Status updates run on the dedicated status timer path.
                self._refresh_all(include_secondary=False, include_heavy=False, include_status=False)
                self._schedule_deferred_secondary_refresh(force=True)
        finally:
            self._activation_refresh_pending = False

    def _schedule_deferred_secondary_refresh(self, force: bool = False) -> None:
        if self._secondary_refresh_pending:
            return
        now = time.time()
        if not force and (now - float(self._last_secondary_refresh_ts) < self._secondary_refresh_interval_sec):
            return
        self._secondary_refresh_pending = True
        QTimer.singleShot(25, self._run_deferred_secondary_refresh)

    def _run_deferred_secondary_refresh(self) -> None:
        try:
            with perf_span("controlfreq.secondary_refresh", settings=self.settings, min_ms=10.0):
                self._load_group_combo()
                if bool(self._view_cards.get("activity", True)):
                    self._refresh_activity()
                if bool(self._view_cards.get("intersections", True)):
                    self._refresh_intersections()
                if bool(self._view_cards.get("schedule", True)):
                    self._refresh_schedule_outlook()
                if bool(self._view_cards.get("propagation", True)):
                    self._refresh_prop_target_controls()
                self._last_secondary_refresh_ts = time.time()
                self._schedule_deferred_heavy_refresh(force=True)
        finally:
            self._secondary_refresh_pending = False

    def _schedule_deferred_heavy_refresh(self, force: bool = False) -> None:
        if self._heavy_refresh_pending:
            return
        now = time.time()
        if not force and (now - float(self._last_heavy_refresh_ts) < self._heavy_refresh_interval_sec):
            return
        self._heavy_refresh_pending = True
        QTimer.singleShot(50, self._run_deferred_heavy_refresh)

    def _run_deferred_heavy_refresh(self) -> None:
        try:
            with perf_span("controlfreq.heavy_refresh", settings=self.settings, min_ms=10.0):
                self._refresh_message_summary()
                if bool(self._view_cards.get("propagation", True)):
                    self._refresh_propagation_snapshot()
                self._last_heavy_refresh_ts = time.time()
        finally:
            self._heavy_refresh_pending = False

    def on_settings_saved(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        self._reload_sop_manager_settings()
        self._invalidate_sop_window_cache()
        self._invalidate_activity_cache()
        self._invalidate_schedule_row_caches()
        self._sop_outlook_refresh_pending = True
        self._apply_theme()
        if self._active:
            self._refresh_all()
            self._sop_outlook_refresh_pending = False
            return
        # Keep inactive-path cheap; refresh on next activation.
        self._last_refresh_ts = 0.0
        self._last_secondary_refresh_ts = 0.0
        self._last_heavy_refresh_ts = 0.0

    def on_peer_schedule_data_changed(self) -> None:
        self._peer_schedule_rows_cache = []
        self._peer_schedule_rows_cache_ts = 0.0
        self._peer_schedule_rows_cache_mtime = 0.0
        self._last_secondary_refresh_ts = 0.0
        if self._active:
            QTimer.singleShot(0, self._refresh_intersections)

    def _clear_status_layout(self) -> None:
        while self.status_layout.count():
            item = self.status_layout.takeAt(0)
            child_layout = item.layout()
            widget = item.widget()
            if child_layout is not None:
                while child_layout.count():
                    child_item = child_layout.takeAt(0)
                    child_widget = child_item.widget()
                    if child_widget is not None:
                        child_widget.deleteLater()
                continue
            if widget is not None:
                widget.deleteLater()

    def _current_visible_status_items(self) -> List[Tuple[str, str]]:
        try:
            profiles = list(self._multi_radio_store.list_device_profiles())
        except Exception:
            profiles = []
        return visible_status_programs(self.settings.all(), device_profiles=profiles)

    def _rebuild_status_indicators(self) -> None:
        if not hasattr(self, "status_layout"):
            return
        self._clear_status_layout()
        self.status_labels = {}
        self._status_text_labels = {}
        theme = resolve_theme(self.settings)
        visible_items = self._current_visible_status_items()
        self.status_group.setVisible(bool(visible_items))
        for key, label in visible_items:
            led = QLabel()
            led.setFixedSize(14, 14)
            led.setStyleSheet(led_style("idle", theme))
            text_label = QLabel(label)
            self.status_labels[key] = led
            self._status_text_labels[key] = text_label
            self.status_layout.addWidget(led)
            self.status_layout.addWidget(text_label)
            self.status_layout.addSpacing(12)
        self.status_layout.addStretch(1)

    def _current_readiness_report(self):
        try:
            profiles = list(self._multi_radio_store.list_device_profiles())
        except Exception:
            profiles = []
        try:
            operating_groups = load_operating_groups(self.settings)
        except Exception:
            operating_groups = []
        return build_station_readiness_report(
            self.settings.all(),
            device_profiles=profiles,
            operating_groups=operating_groups,
        )

    def _dismiss_readiness_review(self) -> None:
        self._readiness_banner_dismissed = True
        self._readiness_dismissed_digest = str(self._readiness_banner_digest or "").strip()
        try:
            self.settings.set("readiness_review_dismissed_digest", self._readiness_dismissed_digest)
        except Exception:
            pass
        self._update_readiness_review_banner()

    def _suppress_readiness_review_for_version(self) -> None:
        self._readiness_banner_dismissed = True
        self._readiness_suppressed_version = __version__
        try:
            self.settings.set("readiness_review_suppressed_version", self._readiness_suppressed_version)
        except Exception:
            pass
        self._update_readiness_review_banner()

    def _review_readiness_now(self) -> None:
        issue = self._current_readiness_report().first_actionable_issue()
        section_key = str(issue.section_key if issue else "freqinout")
        radio_id = int(issue.radio_id or 0) if issue and issue.radio_id else None
        window = self.window()
        if hasattr(window, "open_settings_section"):
            try:
                window.open_settings_section(section_key, radio_id=radio_id)
                return
            except Exception:
                pass

    def _copy_readiness_review_summary(self) -> None:
        QApplication.clipboard().setText(
            readiness_report_detail_text(self._current_readiness_report(), title="FreqInOut Multi-Rig Setup Review")
        )
        if hasattr(self, "readiness_review_copy_btn"):
            self.readiness_review_copy_btn.setText("Copied")
            QTimer.singleShot(1500, lambda: self.readiness_review_copy_btn.setText("Copy Summary"))

    def _update_readiness_review_banner(self) -> None:
        if not hasattr(self, "readiness_review_widget"):
            return
        report = self._current_readiness_report()
        if report.digest != self._readiness_banner_digest:
            self._readiness_banner_digest = report.digest
            self._readiness_banner_dismissed = False
        if not should_show_startup_review(
            report,
            dismissed_digest=self._readiness_dismissed_digest,
            suppressed_version=self._readiness_suppressed_version,
            current_version=__version__,
        ) or self._readiness_banner_dismissed:
            self.readiness_review_widget.setVisible(False)
            return
        first_issue = report.first_actionable_issue()
        detail = f" First item: {format_readiness_issue(first_issue)}." if first_issue else ""
        theme = resolve_theme(self.settings)
        self.readiness_review_label.setText(
            f"Setup review: {readiness_report_overall_text(report)}{detail}"
        )
        border = theme.get("warning", "#C99700")
        bg = theme.get("surface_alt", theme.get("surface", "#f7f7f7"))
        fg = theme.get("text", "#222222")
        self.readiness_review_widget.setStyleSheet(
            "QWidget {"
            f" background: {bg};"
            f" border: 1px solid {border};"
            " border-radius: 6px;"
            "}"
            " QLabel {"
            f" color: {fg};"
            " border: none;"
            " background: transparent;"
            "}"
        )
        self.readiness_review_now_btn.setStyleSheet(button_style("warning", theme))
        self.readiness_review_copy_btn.setStyleSheet(button_style("secondary", theme))
        self.readiness_review_dismiss_btn.setStyleSheet(button_style("muted", theme))
        self.readiness_review_suppress_btn.setStyleSheet(button_style("muted", theme))
        self.readiness_review_widget.setToolTip(
            readiness_report_detail_text(report, title=f"Current readiness state: {readiness_state_label(report.overall_state)}")
        )
        self.readiness_review_widget.setVisible(True)

    def on_condition_levels_changed(self) -> None:
        self.on_sop_data_changed()

    def on_sop_data_changed(self) -> None:
        # SOP edits can happen while ControlFreq is inactive; invalidate refresh gates
        # so the next activation redraws immediately.
        self._reload_sop_manager_settings()
        self._invalidate_sop_window_cache()
        self._sop_outlook_refresh_pending = True
        self._last_refresh_ts = 0.0
        self._last_secondary_refresh_ts = 0.0
        self._last_heavy_refresh_ts = 0.0
        if self._active:
            self._sop_outlook_refresh_pending = False
            QTimer.singleShot(0, self._refresh_schedule_outlook)
            QTimer.singleShot(0, self._refresh_scheduler_strip)

    def _reload_sop_manager_settings(self) -> None:
        try:
            mgr_settings = getattr(self._sop_manager, "settings", None)
            if mgr_settings is not None and hasattr(mgr_settings, "reload"):
                mgr_settings.reload()
        except Exception:
            pass

    def _invalidate_sop_window_cache(self) -> None:
        self._sop_cache_epoch += 1
        self._sop_window_cache.clear()

    def _invalidate_activity_cache(self) -> None:
        self._activity_cache_key = ()
        self._activity_cache_rows = []
        self._activity_cache_ts = 0.0

    def _get_cached_sop_actions(
        self,
        *,
        key: Tuple[Any, ...],
        ttl_sec: float,
        builder: Callable[[], List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        now_ts = time.time()
        cached = self._sop_window_cache.get(key)
        if cached and (now_ts - float(cached[0])) <= max(0.0, float(ttl_sec)):
            return list(cached[1])
        try:
            rows = list(builder() or [])
        except Exception as e:
            log.debug("ControlFreq: SOP cache build failed for %s: %s", key, e)
            rows = []
        self._sop_window_cache[key] = (now_ts, rows)
        if len(self._sop_window_cache) > 16:
            oldest = sorted(self._sop_window_cache.items(), key=lambda kv: kv[1][0])[:4]
            for old_key, _ in oldest:
                self._sop_window_cache.pop(old_key, None)
        return list(rows)

    def _update_time_toggle_text(self) -> None:
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")
        self._update_time_toggle_style()

    def _update_time_toggle_style(self, theme: Optional[Dict[str, str]] = None) -> None:
        if theme is None:
            try:
                theme = resolve_theme(self.settings)
            except Exception:
                theme = {}
        # Local is default context; only highlight when user switches to UTC.
        role = "info" if not self._show_local else "muted"
        try:
            self.time_toggle_btn.setStyleSheet(button_style(role, theme))
        except Exception:
            pass

    def _format_display_datetime(self, utc_dt: dt.datetime, with_seconds: bool = True) -> str:
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=dt.timezone.utc)
        display_dt = utc_dt.astimezone(self._get_display_tz()) if self._show_local else utc_dt
        fmt = "%a %b %d, %Y %H:%M %Z" if with_seconds else "%Y-%m-%d %H:%M %Z"
        return display_dt.strftime(fmt)

    def _sync_header_time_label_widths(self) -> None:
        try:
            full = str(getattr(self, "_current_time_full_text", "") or "--")
            width = int(self.current_time_label.width())
            if width <= 20:
                self.current_time_label.setText(full)
                self.current_time_label.setToolTip("")
                return
            fm = QFontMetrics(self.current_time_label.font())
            elided = fm.elidedText(full, Qt.ElideRight, max(24, width - 6))
            self.current_time_label.setText(elided)
            self.current_time_label.setToolTip(full if elided != full else "")
        except Exception:
            pass

    def _refresh_clock_display(self) -> None:
        now_utc = dt.datetime.now(dt.timezone.utc)
        self._current_time_full_text = self._format_display_datetime(now_utc, with_seconds=True)
        self.current_time_label.setText(self._current_time_full_text)
        self._sync_header_time_label_widths()

    def _toggle_time_view(self) -> None:
        self._show_local = not self._show_local
        self._update_time_toggle_text()
        self._refresh_clock_display()
        self._schedule_persist_ui_state()
        self._refresh_schedule_outlook()

    def _on_filters_changed(self, *_args) -> None:
        self._update_clear_filters_style()
        self._schedule_persist_ui_state()
        try:
            self._filter_refresh_timer.start(220)
        except Exception:
            self._run_filter_refresh()

    def _run_filter_refresh(self) -> None:
        try:
            if bool(self._view_cards.get("activity", True)):
                self._refresh_activity()
            if bool(self._view_cards.get("intersections", True)):
                self._refresh_intersections()
            if bool(self._view_cards.get("schedule", True)):
                self._refresh_schedule_outlook()
            # Message summary is always visible in top row.
            self._refresh_message_summary()
            self._last_secondary_refresh_ts = time.time()
            self._last_heavy_refresh_ts = self._last_secondary_refresh_ts
            self._refresh_clock_display()
            self._update_clear_filters_style()
        except Exception as e:
            log.debug("ControlFreq: filter refresh failed: %s", e)

    def _clear_filters(self) -> None:
        self.search_edit.clear()
        if self.group_combo.count() > 0:
            self.group_combo.setCurrentIndex(0)
        if self.activity_window_combo.count() > 0:
            idx = self.activity_window_combo.findData(120)
            self.activity_window_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self._update_clear_filters_style()
        self._on_filters_changed()

    def _filters_active(self) -> bool:
        search_active = bool((self.search_edit.text() or "").strip())
        group_active = bool((self.group_combo.currentData() or "").strip())
        window_active = int(self.activity_window_combo.currentData() or 120) != 120
        return search_active or group_active or window_active

    def _update_clear_filters_style(self) -> None:
        try:
            theme = resolve_theme(self.settings)
            role = "eligible_warning" if self._filters_active() else "muted"
            self.clear_filters_btn.setStyleSheet(button_style(role, theme))
        except Exception:
            pass

    def _refresh_all(
        self,
        include_secondary: bool = True,
        include_heavy: bool = True,
        include_status: bool = True,
    ) -> None:
        with perf_span("controlfreq.refresh_all", settings=self.settings, min_ms=10.0):
            if include_secondary or include_heavy:
                try:
                    self.settings.reload()
                except Exception:
                    pass
            self._refresh_frequency_control(include_intersections=False)
            if include_status:
                self._refresh_status_widgets()
            if include_secondary:
                self._load_group_combo()
                if bool(self._view_cards.get("activity", True)):
                    self._refresh_activity()
                if bool(self._view_cards.get("intersections", True)):
                    self._refresh_intersections()
                if bool(self._view_cards.get("schedule", True)):
                    self._refresh_schedule_outlook()
                if bool(self._view_cards.get("propagation", True)):
                    self._refresh_prop_target_controls()
                self._last_secondary_refresh_ts = time.time()
            if include_heavy:
                self._refresh_message_summary()
                if bool(self._view_cards.get("propagation", True)):
                    self._refresh_propagation_snapshot()
                self._last_heavy_refresh_ts = time.time()
            self._last_refresh_ts = time.time()
            self._refresh_clock_display()
            self._update_clear_filters_style()

    def _refresh_status_widgets(self) -> None:
        self._refresh_running_status()
        self._refresh_scheduler_strip()

    def _refresh_running_status(self) -> None:
        theme = resolve_theme(self.settings)
        visible_keys = [key for key, _label in self._current_visible_status_items()]
        if visible_keys != list(self.status_labels.keys()):
            self._rebuild_status_indicators()
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
        self._update_readiness_review_banner()

    def _refresh_scheduler_strip(self, status: Optional[Dict[str, Any]] = None) -> None:
        try:
            theme = resolve_theme(self.settings)
            muted = str(theme.get("text_muted", "#888"))
            sched = getattr(self.window(), "scheduler", None)
            if not sched or not hasattr(sched, "get_status_summary"):
                self._set_frequency_state_badge("unknown")
                self._apply_frequency_action_busy_override(None)
                self.next_change_label.setText("Next Change: --")
                self.effective_source_label.setText("Active Source: --")
                self.effective_source_label.setStyleSheet(f"color: {muted};")
                return
            if not isinstance(status, dict):
                status = sched.get_status_summary()
            source = str(status.get("source") or "").strip().upper()
            net_kind = str(status.get("net_kind") or "").strip()
            source_reason_detail = str(status.get("source_reason_detail") or "").strip()
            sop_contention = bool(status.get("sop_contention"))
            sop_profiles = [str(x).strip() for x in (status.get("sop_contention_profiles") or []) if str(x).strip()]
            sop_selected = str(status.get("sop_selected_profile") or "").strip()
            next_source = str(status.get("next_source") or "").strip().upper()
            next_net_kind = str(status.get("next_net_kind") or "").strip()
            next_source_change = bool(status.get("next_source_change"))
            next_transition_note = str(status.get("next_transition_note") or "").strip()
            source_text = "Active Source: --"
            if source == "SOP":
                source_text = f"Active Source: {net_kind or 'SOP Layer'}"
                tip = "SOP Layer currently overrides the baseline HF schedule."
                if sop_contention:
                    others = [p for p in sop_profiles if p and p != sop_selected]
                    if others:
                        source_text += " (Contention)"
                        tip = f"SOP contention: selected {sop_selected or 'winner'} over {', '.join(others[:4])}."
                    else:
                        source_text += " (Contention)"
                        tip = "SOP contention detected across active profiles."
                if source_reason_detail:
                    tip = f"{tip}\nSelection: {source_reason_detail}"
                self.effective_source_label.setStyleSheet(
                    f"font-weight: 600; color: {theme.get('warning', '#C99700')};"
                )
                self.effective_source_label.setToolTip(tip)
            elif source == "NET":
                source_text = f"Active Source: {net_kind or 'Net Schedule'}"
                self.effective_source_label.setStyleSheet(
                    f"font-weight: 600; color: {theme.get('info', '#1E88E5')};"
                )
                tip = "Active net schedule has highest precedence."
                if source_reason_detail:
                    tip = f"{tip}\nSelection: {source_reason_detail}"
                self.effective_source_label.setToolTip(tip)
            elif source == "HF":
                source_text = f"Active Source: {net_kind or 'HF Schedule'}"
                self.effective_source_label.setStyleSheet(f"color: {theme.get('text', '#111')};")
                tip = "Baseline HF schedule is active."
                if source_reason_detail:
                    tip = f"{tip}\nSelection: {source_reason_detail}"
                self.effective_source_label.setToolTip(tip)
            else:
                self.effective_source_label.setStyleSheet(f"color: {muted};")
                self.effective_source_label.setToolTip("")
            self.effective_source_label.setText(source_text)
            off_schedule = bool(status.get("off_schedule"))
            badge_state = "off" if off_schedule else "on"
            self._set_frequency_state_badge(badge_state)
            self._apply_frequency_action_busy_override(self._frequency_action_busy_reason(status))

            next_change = getattr(sched, "next_change_utc", None)
            fallback_preview = self._next_schedule_outlook_preview if isinstance(self._next_schedule_outlook_preview, dict) else None
            fallback_freq = None
            if isinstance(fallback_preview, dict):
                fallback_freq = fallback_preview.get("freq_mhz")
                if not isinstance(fallback_freq, (int, float)):
                    fallback_freq = None
            next_text = "Next Change: --"
            if not isinstance(next_change, dt.datetime) and isinstance(fallback_preview, dict):
                preview_when = fallback_preview.get("when_utc")
                if isinstance(preview_when, dt.datetime):
                    next_change = preview_when
            if isinstance(next_change, dt.datetime):
                if next_change.tzinfo is None:
                    next_change = next_change.replace(tzinfo=dt.timezone.utc)
                else:
                    next_change = next_change.astimezone(dt.timezone.utc)
                now_utc = dt.datetime.now(dt.timezone.utc)
                mins = int(max(0.0, (next_change - now_utc).total_seconds()) // 60)
                display_dt = next_change.astimezone(self._get_display_tz()) if self._show_local else next_change
                next_freq = status.get("next_frequency_mhz")
                if not isinstance(next_freq, (int, float)) and isinstance(fallback_freq, (int, float)):
                    next_freq = float(fallback_freq)
                if isinstance(next_freq, (int, float)):
                    freq_txt = f"{float(next_freq):.3f}"
                else:
                    sched_freq = current_scheduler_freq(self.window())
                    freq_txt = f"{sched_freq:.3f}" if isinstance(sched_freq, (int, float)) else "--"
                next_text = f"Next Change: {freq_txt} {display_dt:%H:%M}"
                if next_source_change and next_source and next_source != source:
                    next_label = next_net_kind or next_source
                    next_text = f"{next_text} -> {next_label}"
                if next_transition_note:
                    self.next_change_label.setToolTip(next_transition_note)
                elif next_source_change and next_source and next_source != source:
                    next_label = next_net_kind or next_source
                    self.next_change_label.setToolTip(f"Next source transition: {source} -> {next_label}.")
                elif isinstance(fallback_preview, dict) and next_change == fallback_preview.get("when_utc"):
                    preview_type = str(fallback_preview.get("type") or "").strip().upper()
                    preview_group = str(fallback_preview.get("group") or "").strip()
                    note_parts = ["Using Schedule Outlook fallback"]
                    if preview_type:
                        note_parts.append(preview_type)
                    if preview_group:
                        note_parts.append(preview_group)
                    self.next_change_label.setToolTip(" | ".join(note_parts))
                else:
                    self.next_change_label.setToolTip("")
                if mins <= 15:
                    self.next_change_label.setStyleSheet("font-weight: 600; color: #B71C1C;")
                elif mins <= 60:
                    self.next_change_label.setStyleSheet("font-weight: 500; color: #8A5A00;")
                else:
                    self.next_change_label.setStyleSheet(f"color: {muted};")
            else:
                self.next_change_label.setStyleSheet(f"color: {muted};")
                self.next_change_label.setToolTip("")
            self.next_change_label.setText(next_text)
            self._sync_frequency_info_row_heights()
        except Exception as e:
            log.debug("ControlFreq: failed scheduler strip refresh: %s", e)

    @staticmethod
    def _frequency_action_busy_reason(status: Dict[str, Any]) -> Optional[str]:
        # Stable precedence keeps the button label from flickering between sources.
        try:
            if bool(status.get("ptt_active")):
                return "PTT active"
            if bool(status.get("shared_ptt_blocked")):
                owner = str(status.get("shared_ptt_owner_name") or "").strip()
                group = str(status.get("shared_ptt_group") or "").strip()
                if owner and group:
                    return f"Shared PTT ({group}: {owner})"
                if group:
                    return f"Shared PTT ({group})"
                return "Shared PTT"
            if bool(status.get("js8_busy")):
                return "JS8Call"
            if bool(status.get("varac_waiting")) or bool(status.get("varac_busy")):
                return "VarAC"
            if bool(status.get("fldigi_busy")):
                return "FLDigi"
        except Exception:
            return None
        return None

    def _apply_frequency_action_busy_override(self, reason_label: Optional[str]) -> None:
        reason = str(reason_label or "").strip()
        prev_reason = str(self._freq_action_busy_reason_label or "").strip()
        if not reason:
            self._freq_action_busy_reason_label = None
            if prev_reason:
                self._update_frequency_action_styles()
            return
        self._freq_action_busy_reason_label = reason
        self.freq_action_btn.setText(f"Busy: {reason}")
        self.freq_action_btn.setToolTip(
            f"Busy: {reason}. Frequency changes are blocked while traffic or PTT is active."
        )
        self.freq_action_btn.setEnabled(False)
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        if theme:
            self.freq_action_btn.setStyleSheet(button_style("warning", theme))

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
            rows = fetch_all(
                db_path,
                """
                SELECT
                    IFNULL(callsign,''),
                    IFNULL(state,''),
                    IFNULL(grid,'')
                FROM operator_checkins
                """,
                timeout=1.5,
                row_factory=sqlite3.Row,
                span_name="controlfreq.load_prop_operator_geo",
            )
            for row in rows:
                callsign = row[0]
                state = row[1]
                grid = row[2]
                cs = (callsign or "").strip().upper()
                if not cs:
                    continue
                out[cs] = {
                    "state": self._normalize_state_abbr(state),
                    "grid": (grid or "").strip().upper(),
                }
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
        elif target_type == "STATE":
            self.prop_target_value_combo.setCurrentIndex(-1)
            self.prop_target_value_combo.setEditText("")
        elif self.prop_target_value_combo.count() > 0:
            self.prop_target_value_combo.setCurrentIndex(0)
        else:
            self.prop_target_value_combo.setEditText("")
        self.prop_target_value_combo.setEditable(True)
        if self.prop_target_value_combo.lineEdit():
            self.prop_target_value_combo.lineEdit().setPlaceholderText("Type to search...")
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
        if target_type in {"REGION", "STATE"} and value:
            idx = self.prop_target_value_combo.findText(value, Qt.MatchFixedString)
            if idx < 0:
                return
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

    @staticmethod
    def _safe_db_mtime(db_path: Path) -> float:
        try:
            return float(db_path.stat().st_mtime) if db_path.exists() else 0.0
        except Exception:
            return 0.0

    def _load_schedule_rows(self, db_path: Path, table_name: str) -> List[Dict[str, Any]]:
        if not db_path.exists():
            return []
        conn = None
        try:
            conn = connect_sqlite(db_path, timeout=1.5, row_factory=sqlite3.Row, busy_timeout_ms=1500)
            cur = conn.cursor()
            if not table_exists(conn, table_name):
                return []
            cur.execute(f"SELECT * FROM {table_name}")
            return rows_to_dicts(cur.fetchall())
        except Exception as e:
            log.debug("ControlFreq: failed to load %s from %s: %s", table_name, db_path, e)
            return []
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def _invalidate_schedule_row_caches(self) -> None:
        self._daily_schedule_rows_cache = []
        self._daily_schedule_rows_cache_ts = 0.0
        self._daily_schedule_rows_cache_mtime = 0.0
        self._net_schedule_rows_cache = []
        self._net_schedule_rows_cache_ts = 0.0
        self._net_schedule_rows_cache_mtime = 0.0

    def _primary_schedule_target_context(self) -> Tuple[Optional[int], Optional[int]]:
        win = self.window()
        manager = getattr(win, "station_runtime_manager", None) if win is not None else None
        if manager is not None:
            try:
                runtime = manager.get_primary_runtime() if hasattr(manager, "get_primary_runtime") else None
            except Exception:
                runtime = None
            if runtime is not None:
                try:
                    profile = runtime.profile if isinstance(runtime.profile, dict) else {}
                    assignment = runtime.assignment if isinstance(runtime.assignment, dict) else {}
                    device_profile_id = int(profile.get("id", 0) or 0)
                    operating_profile_id = assignment.get("operating_profile_id")
                    return (
                        device_profile_id or None,
                        int(operating_profile_id) if operating_profile_id not in (None, "") else None,
                    )
                except Exception:
                    pass
        try:
            store = MultiRadioStore(settings_db_path())
            primary = store.get_runtime_primary_device_profile()
            if not primary:
                return None, None
            device_profile_id = int(primary.get("id", 0) or 0)
            assignment = store.get_effective_assignment_for_device(device_profile_id)
            operating_profile_id = assignment.get("operating_profile_id") if assignment else None
            return (
                device_profile_id or None,
                int(operating_profile_id) if operating_profile_id not in (None, "") else None,
            )
        except Exception:
            return None, None

    def _filter_schedule_rows_for_runtime_target(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        device_profile_id, operating_profile_id = self._primary_schedule_target_context()
        filtered: List[Dict[str, Any]] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            row = normalize_schedule_target_fields(raw)
            if schedule_row_matches_target_context(
                row,
                device_profile_id=device_profile_id,
                operating_profile_id=operating_profile_id,
            ):
                filtered.append(row)
        return filtered

    def _daily_schedule_rows(self) -> List[Dict[str, Any]]:
        db_path = self._settings_db_path()
        now_ts = time.time()
        db_mtime = self._safe_db_mtime(db_path)
        if (
            self._daily_schedule_rows_cache
            and (now_ts - float(self._daily_schedule_rows_cache_ts) < self._daily_schedule_rows_cache_ttl_sec)
            and abs(float(self._daily_schedule_rows_cache_mtime) - db_mtime) < 0.0001
        ):
            return self._filter_schedule_rows_for_runtime_target(self._daily_schedule_rows_cache)
        rows = self._load_schedule_rows(db_path, "daily_schedule_tab")
        self._daily_schedule_rows_cache = rows
        self._daily_schedule_rows_cache_ts = now_ts
        self._daily_schedule_rows_cache_mtime = db_mtime
        return self._filter_schedule_rows_for_runtime_target(rows)

    def _net_schedule_rows(self) -> List[Dict[str, Any]]:
        db_path = self._db_path()
        now_ts = time.time()
        db_mtime = self._safe_db_mtime(db_path)
        if (
            self._net_schedule_rows_cache
            and (now_ts - float(self._net_schedule_rows_cache_ts) < self._net_schedule_rows_cache_ttl_sec)
            and abs(float(self._net_schedule_rows_cache_mtime) - db_mtime) < 0.0001
        ):
            return self._filter_schedule_rows_for_runtime_target(self._net_schedule_rows_cache)
        rows = self._load_schedule_rows(db_path, "net_schedule_tab")
        self._net_schedule_rows_cache = rows
        self._net_schedule_rows_cache_ts = now_ts
        self._net_schedule_rows_cache_mtime = db_mtime
        return self._filter_schedule_rows_for_runtime_target(rows)

    def _peer_schedule_rows(self) -> List[Dict[str, Any]]:
        db_path = self._db_path()
        now_ts = time.time()
        db_mtime = self._safe_db_mtime(db_path)
        if (
            self._peer_schedule_rows_cache
            and (now_ts - float(self._peer_schedule_rows_cache_ts) < self._peer_schedule_rows_cache_ttl_sec)
            and abs(float(self._peer_schedule_rows_cache_mtime) - db_mtime) < 0.0001
        ):
            return self._peer_schedule_rows_cache
        if not db_path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        conn = None
        try:
            conn = connect_sqlite(db_path, timeout=1.5, row_factory=sqlite3.Row, busy_timeout_ms=1500)
            has_effective_view = table_exists(conn, "peer_hf_schedule_effective")
            if has_effective_view:
                query = """
                    SELECT owner_callsign, day_utc, start_utc, end_utc, band, frequency
                    FROM peer_hf_schedule_effective
                """
            else:
                query = """
                    SELECT owner_callsign, day_utc, start_utc, end_utc, band, frequency
                    FROM peer_hf_schedule
                """
            rows = rows_to_dicts(conn.execute(query).fetchall())
        except Exception as e:
            log.debug("ControlFreq: failed to load peer schedule rows: %s", e)
            rows = []
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        self._peer_schedule_rows_cache = rows
        self._peer_schedule_rows_cache_ts = now_ts
        self._peer_schedule_rows_cache_mtime = db_mtime
        return rows

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
        now_ts = time.time()
        try:
            db_mtime = float(db_path.stat().st_mtime) if db_path.exists() else 0.0
        except Exception:
            db_mtime = 0.0
        if (
            self._operator_groups_cache
            and (now_ts - float(self._operator_groups_cache_ts) < self._operator_groups_cache_ttl_sec)
            and abs(float(self._operator_groups_cache_mtime) - db_mtime) < 0.0001
        ):
            return self._operator_groups_cache
        mapping: Dict[str, Set[str]] = {}
        if not db_path.exists():
            return mapping
        try:
            rows = fetch_all(
                db_path,
                "SELECT callsign, group1, group2, group3, groups_json FROM operator_checkins",
                timeout=1.5,
                row_factory=sqlite3.Row,
                span_name="controlfreq.load_operator_group_map",
            )
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
        self._operator_groups_cache = mapping
        self._operator_groups_cache_ts = now_ts
        self._operator_groups_cache_mtime = db_mtime
        return mapping

    def _activity_cache_token(self) -> Tuple[float, float]:
        return (
            self._safe_db_mtime(self._settings_db_path()),
            self._safe_db_mtime(self._db_path()),
        )

    @staticmethod
    def _is_activity_callsign_token(value: object) -> bool:
        cs = str(value or "").strip().upper()
        return bool(cs) and not cs.startswith("@")

    def _compute_activity_rows(
        self,
        window_minutes: int,
        search: str,
        group_filter: str,
    ) -> List[List[str]]:
        group_freqs = self._group_freq_map()
        group_bands = self._group_band_map()
        operator_groups = self._load_operator_group_map()
        db_path = self._db_path()
        if not db_path.exists():
            return [["No activity data", "--", "--", "--"]]

        now_ts = time.time()
        since_ts = now_ts - (window_minutes * 60)

        group_seen: Dict[str, Set[str]] = {g: set() for g in group_freqs}
        group_traffic: Dict[str, int] = {g: 0 for g in group_freqs}

        def _matching_groups_for_link(band: str, mhz: float) -> List[str]:
            matched: List[str] = []
            for grp, freqs in group_freqs.items():
                if group_filter and grp != group_filter:
                    continue
                allowed_bands = group_bands.get(grp, set())
                if band and allowed_bands and band not in allowed_bands:
                    continue
                if any(abs(mhz - f) < 0.0005 for f in freqs):
                    matched.append(grp)
            return matched

        def _add_group_traffic(cs: str) -> None:
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

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT origin, destination, band, freq_hz FROM js8_links WHERE ts >= ?",
                (since_ts,),
            )
            for origin, dest, band, freq_hz in cur.fetchall():
                band = (band or "").strip().upper()
                if freq_hz is None:
                    continue
                try:
                    mhz = float(freq_hz) / 1_000_000.0
                except Exception:
                    continue
                matched_groups = _matching_groups_for_link(band, mhz)
                if not matched_groups:
                    continue
                origin_cs = str(origin or "").strip().upper()
                dest_cs = str(dest or "").strip().upper()
                for grp in matched_groups:
                    if self._is_activity_callsign_token(origin_cs):
                        group_seen[grp].add(origin_cs)
                    if self._is_activity_callsign_token(dest_cs):
                        group_seen[grp].add(dest_cs)
                    group_traffic[grp] = group_traffic.get(grp, 0) + 1

            for sql, label in (
                ("SELECT from_call, utc_ts FROM js8_messages WHERE utc_ts >= ?", "js8_messages"),
                ("SELECT from_call, utc_ts FROM spotter_traffic WHERE utc_ts >= ?", "spotter_traffic"),
                ("SELECT from_call, ts FROM varac_messages WHERE ts >= ?", "varac_messages"),
                ("SELECT callsign, last_seen_ts FROM fldigi_checkins WHERE last_seen_ts >= ?", "fldigi_checkins"),
            ):
                try:
                    cur.execute(sql, (since_ts,))
                    for cs, _ in cur.fetchall():
                        _add_group_traffic(cs)
                except Exception as e:
                    log.debug("ControlFreq: failed to load %s for activity: %s", label, e)
        except Exception as e:
            log.debug("ControlFreq: failed to load activity rows: %s", e)
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

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
        return rows_out

    def _refresh_activity(self) -> None:
        if not bool(self._view_cards.get("activity", True)):
            return
        window_minutes = int(self.activity_window_combo.currentData() or 120)
        search = (self.search_edit.text() or "").strip().upper()
        group_filter = self.group_combo.currentData() or ""
        cache_key = (
            window_minutes,
            search,
            str(group_filter).strip().upper(),
            self._activity_cache_token(),
        )
        now_ts = time.time()
        if (
            cache_key == self._activity_cache_key
            and (now_ts - float(self._activity_cache_ts) <= self._activity_cache_ttl_sec)
            and self._activity_cache_rows
        ):
            self._set_table_rows(self.activity_table, self._activity_cache_rows)
            return
        with perf_span("controlfreq.refresh_activity", settings=self.settings, min_ms=5.0):
            rows_out = self._compute_activity_rows(window_minutes, search, str(group_filter).strip().upper())
        self._activity_cache_key = cache_key
        self._activity_cache_ts = time.time()
        self._activity_cache_rows = [list(row) for row in rows_out]
        self._set_table_rows(self.activity_table, rows_out)

    def _refresh_frequency_control(self, include_intersections: bool = True) -> None:
        # Avoid clobbering selection while user is interacting
        try:
            if (
                self.freq_combo.view().isVisible()
                or self.freq_combo.hasFocus()
                or self.hold_duration_combo.view().isVisible()
                or self.hold_duration_combo.hasFocus()
            ):
                return
        except Exception:
            pass
        og_list = load_operating_groups(self.settings)
        refresh_hold_duration_combo(self.hold_duration_combo, self.settings)
        current = selected_qsy_meta(self.freq_combo)
        current_freq = None
        try:
            if current:
                current_freq = float(current.get("freq"))
        except Exception:
            current_freq = None
        combo_items: List[Tuple[str, Dict[str, Any], float]] = []
        combo_cache_rows: List[Tuple[str, str, float, str, str, bool]] = []
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
            group_txt = str(g.get("group", "")).strip().upper()
            band_txt = str(g.get("band", "")).strip().upper()
            compact_parts = [f"{freq_val:.3f}"]
            if group_txt:
                compact_parts.append(group_txt)
            if band_txt:
                compact_parts.append(band_txt)
            label = "  ".join(compact_parts)
            meta = {
                "freq": freq_val,
                "mode": g.get("mode", ""),
                "band": g.get("band", ""),
                "auto_tune": bool(g.get("auto_tune", False)),
                "vfo": (g.get("vfo") or "").strip().upper(),
                "group": group_txt,
            }
            combo_items.append((label.strip(), meta, freq_val))
            combo_cache_rows.append(
                (
                    group_txt,
                    band_txt,
                    round(float(freq_val), 6),
                    str(meta.get("mode") or "").strip().upper(),
                    str(meta.get("vfo") or "").strip().upper(),
                    bool(meta.get("auto_tune")),
                )
            )
        combo_key: Tuple[Tuple[str, str, float, str, str, bool], ...] = tuple(combo_cache_rows)
        combo_rebuilt = combo_key != self._freq_combo_cache_key
        if combo_rebuilt:
            self.freq_combo.blockSignals(True)
            self.freq_combo.clear()
            self.freq_combo.addItem("Select frequency", None)
            for label, meta, _freq_val in combo_items:
                self.freq_combo.addItem(label, meta)
            self.freq_combo.blockSignals(False)
            self._freq_combo_cache_key = combo_key

        restore_idx = -1
        if current_freq is not None:
            for idx in range(1, self.freq_combo.count()):
                meta = self.freq_combo.itemData(idx)
                try:
                    freq_val = float((meta or {}).get("freq"))
                except Exception:
                    continue
                if abs(freq_val - current_freq) < 0.0005:
                    restore_idx = idx
                    break
        force_resync = bool(self._force_hero_resync)
        status_snapshot: Optional[Dict[str, Any]] = None
        try:
            sched_obj = getattr(self.window(), "scheduler", None)
            if sched_obj and hasattr(sched_obj, "get_status_summary"):
                status_snapshot = sched_obj.get_status_summary()
        except Exception:
            status_snapshot = None
        sched_freq = current_scheduler_freq(self.window())
        scheduler_freq_changed = False
        prev_sched_freq = self._last_hero_sched_freq_mhz
        if sched_freq is None:
            scheduler_freq_changed = prev_sched_freq is not None
            self._last_hero_sched_freq_mhz = None
        else:
            try:
                sched_freq_f = float(sched_freq)
            except Exception:
                sched_freq_f = None
            if sched_freq_f is not None:
                scheduler_freq_changed = prev_sched_freq is None or abs(prev_sched_freq - sched_freq_f) > 0.0005
                self._last_hero_sched_freq_mhz = sched_freq_f
        if scheduler_freq_changed:
            force_resync = True
        sched_group = self._get_scheduled_group_name()
        active_freq = self._get_active_frequency_mhz(status_snapshot)
        display_freq = active_freq if active_freq is not None else sched_freq
        if (
            scheduler_freq_changed
            and sched_freq is not None
            and active_freq is not None
            and prev_sched_freq is not None
            and abs(active_freq - prev_sched_freq) < 0.0005
        ):
            # During a scheduler transition, a cached active-frequency poll may
            # still briefly report the old scheduled value. Prefer the new
            # scheduled target only for that narrow stale-poll case.
            display_freq = sched_freq
        current_matches_display = (
            current_freq is not None
            and display_freq is not None
            and abs(current_freq - float(display_freq)) < 0.0005
        )
        selection_is_last_hero = (
            current_freq is not None
            and self._last_hero_display_freq_mhz is not None
            and abs(current_freq - float(self._last_hero_display_freq_mhz)) < 0.0005
        )
        pending_user_selection = (
            not force_resync
            and current_freq is not None
            and not current_matches_display
            and not selection_is_last_hero
        )
        if pending_user_selection and restore_idx >= 0:
            self.freq_combo.blockSignals(True)
            self.freq_combo.setCurrentIndex(restore_idx)
            self.freq_combo.blockSignals(False)
        elif display_freq is not None:
            for idx in range(1, self.freq_combo.count()):
                meta = self.freq_combo.itemData(idx)
                try:
                    freq_val = float((meta or {}).get("freq"))
                except Exception:
                    continue
                if abs(freq_val - float(display_freq)) < 0.0005:
                    self.freq_combo.blockSignals(True)
                    self.freq_combo.setCurrentIndex(idx)
                    self.freq_combo.blockSignals(False)
                    self._last_hero_display_freq_mhz = float(display_freq)
                    break
        elif not pending_user_selection:
            self._last_hero_display_freq_mhz = None
        self._force_hero_resync = False
        sched_txt = f"{sched_freq:.3f}" if sched_freq is not None else "--"
        band_txt = self._band_for_group_freq(sched_group, sched_freq) if sched_freq is not None else "--"
        group_txt = sched_group or "--"
        line1 = f"Scheduled: {group_txt} | {band_txt} - {sched_txt}"
        self._freq_meta_full_text = line1
        self._apply_freq_meta_text()
        self._update_frequency_action_styles(sched_freq, active_freq)
        self._update_active_label_style(sched_freq, active_freq)
        self._refresh_scheduler_strip(status_snapshot)
        if include_intersections:
            self._refresh_intersections()

    def _refresh_intersections(self) -> None:
        if not bool(self._view_cards.get("intersections", True)):
            return
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
        now_day_idx = (now_utc.weekday() + 1) % 7  # Sunday=0
        now_week_min = now_day_idx * 1440 + now_min
        horizon_minutes = 120

        my_entries = self._load_my_schedule_entries()
        if not my_entries:
            return rows
        operator_groups = self._load_operator_group_map()
        peer_rows = self._peer_schedule_rows()
        if not peer_rows:
            return rows

        now_calls: Set[str] = set()
        next_calls: Set[str] = set()
        now_labels: Set[str] = set()
        next_labels: Set[str] = set()
        for r in peer_rows:
            cs = str(r.get("owner_callsign") or "").strip().upper()
            if not cs:
                continue
            groups = operator_groups.get(cs, set())
            if group_filter:
                if group_filter not in groups:
                    continue
            if search and search not in cs and not any(search in g for g in groups):
                continue
            peer_start = self._parse_time_minutes(str(r.get("start_utc") or ""))
            peer_end = self._parse_time_minutes(str(r.get("end_utc") or ""))
            if peer_start is None or peer_end is None:
                continue
            peer_segments = self._expand_week_segments(str(r.get("day_utc") or "ALL"), peer_start, peer_end)
            if not peer_segments:
                continue
            peer_freq = self._parse_frequency_mhz(r.get("frequency"))
            if peer_freq is None:
                continue

            for entry in my_entries:
                if abs(entry["freq"] - peer_freq) > 0.001:
                    continue
                overlaps = self._next_horizon_overlaps(
                    entry.get("segments", []),
                    peer_segments,
                    now_week_min=now_week_min,
                    horizon_minutes=horizon_minutes,
                )
                if not overlaps:
                    continue
                has_now = any(start <= now_week_min < end for start, end in overlaps)
                if has_now:
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

    def _load_my_schedule_entries(self) -> List[Dict[str, object]]:
        try:
            settings_mtime = (
                float(self._settings_db_path().stat().st_mtime)
                if self._settings_db_path().exists()
                else 0.0
            )
        except Exception:
            settings_mtime = 0.0
        try:
            nets_mtime = float(self._db_path().stat().st_mtime) if self._db_path().exists() else 0.0
        except Exception:
            nets_mtime = 0.0
        cache_key = (settings_mtime, nets_mtime)
        now_ts = time.time()
        if (
            self._my_schedule_entries_cache
            and (now_ts - float(self._my_schedule_entries_cache_ts) < self._my_schedule_entries_cache_ttl_sec)
            and cache_key == self._my_schedule_entries_cache_key
        ):
            return self._my_schedule_entries_cache

        entries: List[Dict[str, object]] = []
        seen: Set[Tuple[str, float, str, str]] = set()

        def add_entry(day: str, start: str, end: str, band: str, freq_val, group: str) -> None:
            start_min = self._parse_time_minutes(start)
            end_min = self._parse_time_minutes(end)
            if start_min is None or end_min is None:
                return
            freq_num = self._parse_frequency_mhz(freq_val)
            if freq_num is None:
                return
            segments = self._expand_week_segments(day, start_min, end_min)
            if not segments:
                return
            group_val = (group or "").strip().upper()
            band_val = (band or "").strip().upper()
            key = (
                f"{day}|{start_min}|{end_min}",
                round(float(freq_num), 3),
                band_val,
                group_val,
            )
            if key in seen:
                return
            seen.add(key)
            entries.append(
                {
                    "start_min": start_min,
                    "end_min": end_min,
                    "freq": freq_num,
                    "band": band_val,
                    "group": group_val,
                    "segments": segments,
                }
            )

        def _read_rows(db_path: Path, table_name: str, cols: str) -> None:
            if not db_path.exists():
                return
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                has_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,),
                ).fetchone()
                if not has_table:
                    conn.close()
                    return
                cur.execute(f"SELECT {cols} FROM {table_name}")
                rows = cur.fetchall()
                conn.close()
                for r in rows:
                    group_val = ""
                    try:
                        group_val = r["group_name"] or ""
                    except Exception:
                        group_val = ""
                    if not group_val:
                        try:
                            group_val = r["net_name"] or ""
                        except Exception:
                            group_val = ""
                    add_entry(
                        r["day_utc"],
                        r["start_utc"],
                        r["end_utc"],
                        r["band"],
                        r["frequency"],
                        group_val,
                    )
            except Exception as e:
                log.debug("ControlFreq: failed to load %s for overlaps: %s", table_name, e)

        _read_rows(
            self._settings_db_path(),
            "daily_schedule_tab",
            "day_utc, start_utc, end_utc, band, frequency, group_name",
        )
        _read_rows(
            self._db_path(),
            "daily_schedule_tab",
            "day_utc, start_utc, end_utc, band, frequency, group_name",
        )
        _read_rows(
            self._db_path(),
            "net_schedule_tab",
            "day_utc, start_utc, end_utc, band, frequency, group_name, net_name",
        )
        _read_rows(
            self._settings_db_path(),
            "net_schedule_tab",
            "day_utc, start_utc, end_utc, band, frequency, group_name, net_name",
        )
        self._my_schedule_entries_cache = entries
        self._my_schedule_entries_cache_ts = now_ts
        self._my_schedule_entries_cache_key = cache_key
        return entries

    @staticmethod
    def _day_to_index(day: str) -> Optional[int]:
        txt = (day or "").strip().lower()
        if not txt:
            return None
        names = [
            "sunday",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
        ]
        for idx, name in enumerate(names):
            if txt.startswith(name[:3]) or name.startswith(txt[:3]):
                return idx
        return None

    def _schedule_day_indices(self, day: str) -> List[int]:
        txt = (day or "ALL").strip().upper()
        if txt in {"", "ALL", "DAILY"}:
            return list(range(7))
        for delim in ("/", ";", "|"):
            txt = txt.replace(delim, ",")
        out: List[int] = []
        for part in txt.split(","):
            idx = self._day_to_index(part)
            if idx is not None and idx not in out:
                out.append(idx)
        return out

    def _expand_week_segments(self, day: str, start_min: int, end_min: int) -> List[Tuple[int, int, int]]:
        if start_min < 0 or start_min > 1439 or end_min < 0 or end_min > 1439:
            return []
        if start_min == end_min:
            return []
        day_indices = self._schedule_day_indices(day)
        if not day_indices:
            return []
        segments: List[Tuple[int, int, int]] = []
        for day_idx in day_indices:
            if start_min < end_min:
                segments.append((day_idx, start_min, end_min))
                continue
            segments.append((day_idx, start_min, 24 * 60))
            segments.append(((day_idx + 1) % 7, 0, end_min))
        return segments

    @staticmethod
    def _next_horizon_overlaps(
        seg_a: List[Tuple[int, int, int]],
        seg_b: List[Tuple[int, int, int]],
        *,
        now_week_min: int,
        horizon_minutes: int,
    ) -> List[Tuple[int, int]]:
        if not seg_a or not seg_b:
            return []
        week = 7 * 24 * 60
        window_start = int(now_week_min)
        window_end = int(now_week_min + max(1, int(horizon_minutes)))

        def _absolute_segments(segments: List[Tuple[int, int, int]]) -> List[Tuple[int, int]]:
            out: List[Tuple[int, int]] = []
            for day_idx, start_min, end_min in segments:
                base_start = int(day_idx) * 1440 + int(start_min)
                base_end = int(day_idx) * 1440 + int(end_min)
                out.append((base_start, base_end))
                out.append((base_start + week, base_end + week))
            return out

        abs_a = _absolute_segments(seg_a)
        abs_b = _absolute_segments(seg_b)
        overlaps: List[Tuple[int, int]] = []
        for a_start, a_end in abs_a:
            if a_end <= window_start or a_start >= window_end:
                continue
            for b_start, b_end in abs_b:
                if b_end <= window_start or b_start >= window_end:
                    continue
                start = max(a_start, b_start, window_start)
                end = min(a_end, b_end, window_end)
                if end > start:
                    overlaps.append((start, end))
        if not overlaps:
            return overlaps
        overlaps.sort(key=lambda it: (it[0], it[1]))
        merged: List[Tuple[int, int]] = []
        for start, end in overlaps:
            if not merged or start > merged[-1][1]:
                merged.append((start, end))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        return merged

    @staticmethod
    def _parse_frequency_mhz(value) -> Optional[float]:
        try:
            txt = str(value).strip()
            if not txt:
                return None
            match = re.search(r"[-+]?\d+(?:[.,]\d+)?", txt)
            if not match:
                return None
            return float(match.group(0).replace(",", "."))
        except Exception:
            return None

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

    def _get_active_frequency_mhz(self, status: Optional[Dict[str, Any]] = None) -> Optional[float]:
        try:
            if not isinstance(status, dict):
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

    def _update_frequency_action_styles(
        self,
        scheduled: Optional[float] = None,
        active: Optional[float] = None,
    ) -> None:
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        if not theme:
            return
        if scheduled is None:
            scheduled = current_scheduler_freq(self.window())
        if active is None:
            active = self._get_active_frequency_mhz()
        hold_snapshot = self._hold_state_snapshot if isinstance(self._hold_state_snapshot, dict) else None
        if not isinstance(hold_snapshot, dict):
            hold_snapshot = suspend_snapshot(self.settings)
        if hold_snapshot.get("active"):
            remaining_sec = hold_snapshot.get("remaining_sec")
            self._primary_freq_action_mode = "resume"
            self.freq_action_btn.setText(active_hold_button_text(remaining_sec))
            self.freq_action_btn.setToolTip(active_hold_status_text(remaining_sec))
            self.freq_action_btn.setEnabled(True)
            self.freq_action_btn.setStyleSheet(button_style(active_hold_button_role(remaining_sec), theme))
            if self._freq_action_busy_reason_label:
                self._apply_frequency_action_busy_override(self._freq_action_busy_reason_label)
            return
        mismatch = (
            scheduled is not None
            and active is not None
            and abs(scheduled - active) > 0.0005
        )
        qsy_pending = False
        meta = selected_qsy_meta(self.freq_combo)
        if meta:
            try:
                selected = float(meta.get("freq", 0.0))
                qsy_pending = active is None or abs(selected - float(active)) > 0.0005
            except Exception:
                qsy_pending = True
        if qsy_pending:
            self._primary_freq_action_mode = "qsy"
            mins = self._selected_hold_minutes()
            self.freq_action_btn.setText("QSY + Hold")
            self.freq_action_btn.setToolTip(f"QSY now and pause schedule control for {mins} minutes (Ctrl+Enter)")
            self.freq_action_btn.setEnabled(True)
            self.freq_action_btn.setStyleSheet(button_style("warning", theme))
            if self._freq_action_busy_reason_label:
                self._apply_frequency_action_busy_override(self._freq_action_busy_reason_label)
            return
        if mismatch:
            self._primary_freq_action_mode = "resume"
            self.freq_action_btn.setText("Resume Schedule")
            self.freq_action_btn.setToolTip("Resume Schedule (Ctrl+Shift+R)")
            self.freq_action_btn.setEnabled(True)
            self.freq_action_btn.setStyleSheet(button_style("info", theme))
            if self._freq_action_busy_reason_label:
                self._apply_frequency_action_busy_override(self._freq_action_busy_reason_label)
            return
        self._primary_freq_action_mode = "none"
        mins = self._selected_hold_minutes()
        self.freq_action_btn.setText("QSY + Hold")
        self.freq_action_btn.setToolTip(f"QSY now and pause schedule control for {mins} minutes (Ctrl+Enter)")
        self.freq_action_btn.setEnabled(False)
        self.freq_action_btn.setStyleSheet(button_style("muted", theme))
        if self._freq_action_busy_reason_label:
            self._apply_frequency_action_busy_override(self._freq_action_busy_reason_label)

    def _on_primary_freq_action_clicked(self) -> None:
        mode = str(self._primary_freq_action_mode or "none").strip().lower()
        if mode == "resume":
            self._on_resume_schedule_clicked()
            return
        self._on_freq_set_clicked()

    def _selected_hold_minutes(self) -> int:
        return selected_hold_duration(self.hold_duration_combo, self.settings)

    def _on_hold_duration_changed(self) -> None:
        mins = self._selected_hold_minutes()
        set_hold_duration_default(self.settings, mins)
        notify_hold_duration_default_changed(self.window())
        self._update_frequency_action_styles()

    def on_hold_state_changed(self, snapshot: Optional[Dict[str, object]] = None) -> None:
        self._hold_state_snapshot = snapshot if isinstance(snapshot, dict) else suspend_snapshot(self.settings)
        try:
            if (
                not self.hold_duration_combo.view().isVisible()
                and not self.hold_duration_combo.hasFocus()
            ):
                refresh_hold_duration_combo(self.hold_duration_combo, self.settings)
        except Exception:
            pass
        self._update_frequency_action_styles()

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
            color = theme["warning"] if mismatch else theme.get("text_muted", theme["text"])
            self.freq_meta_label.setStyleSheet(f"font-size: 12px; color: {color};")

    def _on_freq_set_clicked(self) -> None:
        control_via = (self.settings.get("control_via", "") or "").strip()
        if control_via not in {"FLRig", "RIGCTLD", "JS8Call"}:
            QMessageBox.information(
                self,
                "Frequency Control",
                "Frequency control is available when Control Via is FLRig, RIGCTLD, or JS8Call.",
            )
            return
        meta = selected_qsy_meta(self.freq_combo)
        if not meta:
            QMessageBox.warning(self, "Frequency Control", "Select a frequency first.")
            return
        mins = perform_qsy_with_hold(self.window(), self.settings, meta, self._selected_hold_minutes())
        ok = mins > 0
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        if theme:
            self.freq_action_btn.setStyleSheet(
                button_style("success" if ok else "warning", theme)
            )
        self._refresh_frequency_control()
        if ok:
            QMessageBox.information(
                self,
                "QSY Applied",
                f"Frequency changed and scheduling paused for {mins} minutes.",
            )
            QTimer.singleShot(800, self._refresh_frequency_control)

    def _on_freq_selection_changed(self, *_args) -> None:
        self._update_frequency_action_styles()

    def _on_resume_schedule_clicked(self) -> None:
        resumed = False
        try:
            sched = getattr(self.window(), "scheduler", None)
            if sched and hasattr(sched, "resume_schedule"):
                resume_schedule_hold(self.window(), self.settings)
                resumed = True
            elif sched:
                sched.apply_current_entry(force=True, ignore_wait_prompt=True, ignore_suspend=True)
                resumed = True
        except Exception:
            pass
        if resumed:
            self.on_schedule_resumed()

    def on_schedule_resumed(self) -> None:
        """
        Keep hero/status readouts responsive after resume regardless of trigger origin.
        """
        self._force_hero_resync = True
        if not self._active:
            return
        self._refresh_frequency_control(include_intersections=False)

        def _pulse_refresh() -> None:
            self._force_hero_resync = True
            self._refresh_frequency_control(include_intersections=False)

        # Short pulses absorb asynchronous scheduler/radio apply completion.
        for delay_ms in (180, 700, 1500):
            QTimer.singleShot(delay_ms, _pulse_refresh)

    def _refresh_schedule_outlook(self) -> None:
        if not bool(self._view_cards.get("schedule", True)):
            return
        self._reload_sop_manager_settings()
        now_utc = dt.datetime.now(dt.timezone.utc)
        if self._show_local:
            # "Today" should respect local day boundaries in local display mode.
            tz = self._get_display_tz()
            now_local = now_utc.astimezone(tz)
            today_end_local = now_local.replace(hour=23, minute=59, second=59, microsecond=0)
            today_end = today_end_local.astimezone(dt.timezone.utc)
            tomorrow_start = (today_end_local + dt.timedelta(seconds=1)).astimezone(dt.timezone.utc)
            tomorrow_end = (
                (today_end_local + dt.timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
            ).astimezone(dt.timezone.utc)
        else:
            today_end = now_utc.replace(hour=23, minute=59, second=59, microsecond=0)
            tomorrow_start = today_end + dt.timedelta(seconds=1)
            tomorrow_end = tomorrow_start.replace(hour=23, minute=59, second=59, microsecond=0)
        day_mode = 1 if self._show_local else 0
        today_horizon_hours = max(1, int(math.ceil(max(0.0, (today_end - now_utc).total_seconds()) / 3600.0)))
        today_key = (
            "today",
            int(now_utc.timestamp() // 60),
            int(today_end.timestamp()),
            day_mode,
            int(self._sop_cache_epoch),
        )
        sop_actions_today = self._get_cached_sop_actions(
            key=today_key,
            ttl_sec=self._sop_today_cache_ttl_sec,
            builder=lambda: self._sop_manager.build_upcoming_actions(
                horizon_hours=today_horizon_hours,
                only_active=True,
                now_utc=now_utc,
            ),
        )
        tomorrow_key = (
            "tomorrow",
            int(tomorrow_start.timestamp()),
            int(tomorrow_end.timestamp()),
            day_mode,
            int(self._sop_cache_epoch),
        )
        sop_actions_tomorrow = self._get_cached_sop_actions(
            key=tomorrow_key,
            ttl_sec=self._sop_tomorrow_cache_ttl_sec,
            builder=lambda: self._build_sop_actions_in_window(
                tomorrow_start,
                tomorrow_end,
                only_active=True,
            ),
        )
        today_rows = self._collect_schedule_rows(
            now_utc,
            today_end,
            include_day=False,
            include_hf=True,
            resolve_multiday=bool(self._show_local),
            preloaded_sop_actions=sop_actions_today,
        )
        week_rows = self._collect_schedule_rows(
            tomorrow_start,
            tomorrow_end,
            include_day=True,
            include_hf=False,
            preloaded_sop_actions=sop_actions_tomorrow,
        )
        week_rows = [
            r
            for r in week_rows
            if not isinstance(r.get("when_utc"), dt.datetime)
            or (tomorrow_start <= r.get("when_utc") <= tomorrow_end)
        ]
        self._next_schedule_outlook_preview = self._next_schedule_outlook_entry(now_utc, today_rows + week_rows)
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
        self._append_section_row_to(self.schedule_table, "Tomorrow")
        if week_rows:
            for entry in week_rows:
                self._append_schedule_data_row(entry)
        else:
            self._append_schedule_data_row(
                {
                    "when_text": "--",
                    "type": "--",
                    "group": "No upcoming events tomorrow",
                    "band_freq": "--",
                    "action": "",
                    "action_kind": "",
                    "when_utc": None,
                }
            )
        self._apply_elide_tooltips(self.schedule_table, 2)
        self._apply_elide_tooltips(self.schedule_table, 3)
        self._refresh_scheduler_strip()

    def _next_schedule_outlook_entry(
        self,
        now_utc: dt.datetime,
        rows: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        for row in rows:
            when_utc = row.get("when_utc")
            if not isinstance(when_utc, dt.datetime):
                continue
            if when_utc.tzinfo is None:
                when_utc = when_utc.replace(tzinfo=dt.timezone.utc)
            else:
                when_utc = when_utc.astimezone(dt.timezone.utc)
            if when_utc < now_utc:
                continue
            freq_mhz = row.get("freq_mhz")
            if not isinstance(freq_mhz, (int, float)):
                freq_mhz = self._parse_freq_label(str(row.get("band_freq") or ""))
            if not isinstance(freq_mhz, (int, float)):
                continue
            preview = dict(row)
            preview["when_utc"] = when_utc
            preview["freq_mhz"] = float(freq_mhz)
            return preview
        return None

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
            return "Tune radio now and pause schedule control for the selected hold duration."
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
        mins = perform_qsy_with_hold(self.window(), self.settings, meta, self._selected_hold_minutes())
        ok = mins > 0
        if ok:
            self._force_hero_resync = True
            self._refresh_frequency_control()
            QMessageBox.information(
                self,
                "QSY Applied",
                f"Frequency changed and scheduling paused for {mins} minutes.",
            )

            def _refresh_qsy_hero() -> None:
                self._force_hero_resync = True
                self._refresh_frequency_control()

            QTimer.singleShot(800, _refresh_qsy_hero)

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
                "group": grp,
            }
        return {
            "freq": freq,
            "mode": "",
            "band": band,
            "auto_tune": False,
            "vfo": "",
            "group": group,
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

    @staticmethod
    def _action_enabled(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return int(value) != 0
        txt = str(value).strip().lower()
        if not txt:
            return False
        return txt in {"1", "true", "yes", "y", "on", "enabled"}

    def _build_sop_actions_in_window(
        self,
        start: dt.datetime,
        end: dt.datetime,
        *,
        only_active: bool = True,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        try:
            if end <= start:
                return out
            max_total_rows = 400
            condition_levels = self._sop_manager._condition_level_map()  # type: ignore[attr-defined]
            profiles = self._sop_manager.list_profiles()
            for profile in profiles:
                if only_active and not bool(profile.get("active")):
                    continue
                profile_id = int(profile.get("id") or 0)
                if profile_id <= 0:
                    continue
                full = self._sop_manager.get_profile(profile_id)
                if not full:
                    continue
                operating_group = str(full.get("operating_group") or "")
                for action in list(full.get("actions") or []):
                    if not self._action_enabled(action.get("enabled", True)):
                        continue
                    action_group = str(action.get("group_name") or "").strip().upper() or str(operating_group).strip().upper()
                    group_level = condition_levels.get(action_group)
                    if not self._sop_manager._action_condition_match(  # type: ignore[attr-defined]
                        str(action.get("condition_levels") or "ALL"),
                        group_level,
                    ):
                        continue
                    interval_m = int(action.get("interval_minutes") or 0)
                    if interval_m <= 0:
                        interval_m = max(1, int(action.get("interval_hours") or 3)) * 60
                    interval_m = max(1, interval_m)
                    interval_phase_m = max(0, int(action.get("interval_phase_minutes") or 0)) % interval_m
                    action_for_occurrence = dict(action)
                    if not str(action_for_occurrence.get("daily_start_utc") or "").strip():
                        action_for_occurrence["daily_start_utc"] = str(full.get("sop_start_utc") or "00:00")
                    if not str(action_for_occurrence.get("daily_end_utc") or "").strip():
                        action_for_occurrence["daily_end_utc"] = "23:59"
                    action_for_occurrence["interval_minutes"] = interval_m
                    action_for_occurrence["interval_phase_minutes"] = interval_phase_m
                    occurrences = self._sop_manager.build_action_occurrences_in_window(
                        action_for_occurrence,
                        window_start_utc=start,
                        window_end_utc=end + dt.timedelta(seconds=1),
                    )
                    if not occurrences:
                        continue
                    max_per_action = min(
                        240,
                        int(math.ceil((end - start).total_seconds() / max(60, interval_m * 60))) + 2,
                    )
                    emitted = 0
                    rule = str(action.get("contact_rule") or "none").strip()
                    selected_target = (action.get("contact_target") or "").strip().upper()
                    targets: List[str] = []
                    if rule in {"hub_or_hub_alt", "ncs_or_ancs"}:
                        if selected_target and selected_target != "__ANY_ROLE__":
                            targets = [selected_target]
                        else:
                            targets = ["Any (Role Match)"]
                    elif rule == "group":
                        targets = [selected_target] if selected_target else ["Any (Group Match)"]
                    elif rule in {"callsign", "peer", "local_profile", "local_group"}:
                        targets = [selected_target] if selected_target else []
                    action_band = (action.get("band") or "").strip().upper()
                    action_freq = (action.get("frequency") or "").strip() or str(full.get("frequency") or "")
                    action_id = int(action.get("id") or 0)
                    software = str(action.get("software") or "")
                    mode = str(action.get("mode") or "").strip().upper()
                    action_key = str(action.get("action_key") or "")
                    action_label = str(action.get("action_label") or "")
                    description = str(action.get("description") or "")
                    for due, _due_end in occurrences:
                        if due < start or due > end:
                            continue
                        if emitted >= max_per_action:
                            break
                        if len(out) >= max_total_rows:
                            break
                        # ControlFreq outlook does not render alignment warnings for SOP rows;
                        # keep this lightweight to avoid per-occurrence DB schedule scans.
                        aligned = True
                        out.append(
                            {
                                "profile_id": profile_id,
                                "profile_name": str(full.get("name") or ""),
                                "operating_group": action_group or operating_group,
                                "band": action_band,
                                "frequency": action_freq,
                                "action_id": action_id,
                                "software": software,
                                "mode": mode,
                                "action_key": action_key,
                                "action_label": action_label,
                                "description": description,
                                "contact_rule": rule,
                                "contact_target": selected_target,
                                "contact_targets": targets,
                                "interval_minutes": interval_m,
                                "interval_phase_minutes": interval_phase_m,
                                "next_due_utc": due,
                                "aligned": aligned,
                                "status": "Upcoming",
                                "is_completed": False,
                            }
                        )
                        emitted += 1
                    if len(out) >= max_total_rows:
                        break
                if len(out) >= max_total_rows:
                    break
        except Exception as e:
            log.debug("ControlFreq: SOP window build failed: %s", e)
        out.sort(
            key=lambda x: (
                x.get("next_due_utc") if isinstance(x.get("next_due_utc"), dt.datetime) else dt.datetime.max.replace(tzinfo=dt.timezone.utc),
                str(x.get("profile_name") or ""),
                str(x.get("action_label") or ""),
            )
        )
        return out

    def _collect_schedule_rows(
        self,
        start: dt.datetime,
        end: dt.datetime,
        include_day: bool = False,
        include_hf: bool = True,
        resolve_multiday: bool = False,
        preloaded_sop_actions: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        tz = self._get_display_tz()
        scan_multi_day = bool(resolve_multiday or include_day)

        # SOP upcoming actions
        try:
            if preloaded_sop_actions is None:
                horizon_hours = max(1, int(math.ceil(max(0.0, (end - start).total_seconds()) / 3600.0)))
                actions = self._sop_manager.build_upcoming_actions(
                    horizon_hours=horizon_hours, only_active=True, now_utc=start
                )
            else:
                actions = preloaded_sop_actions
            for a in actions:
                due = a.get("next_due_utc") or a.get("due_utc")
                if not isinstance(due, dt.datetime):
                    continue
                if due.tzinfo is None:
                    due = due.replace(tzinfo=dt.timezone.utc)
                else:
                    due = due.astimezone(dt.timezone.utc)
                status_txt = str(a.get("status") or "").strip().upper()
                due_window_start = start
                if status_txt in {"DUE NOW", "OVERDUE", "COMPLETED"}:
                    # SOP due-now items are represented by the current interval start,
                    # which may be a few minutes before "now". Keep them visible.
                    due_window_start = start - dt.timedelta(minutes=30)
                if not (due_window_start <= due <= end):
                    continue
                grp = (a.get("operating_group") or "").strip().upper()
                if group_filter and grp != group_filter:
                    continue
                label = a.get("action_label") or a.get("action") or "SOP"
                band = (a.get("band") or "").strip().upper()
                freq = (a.get("frequency") or "").strip()
                software = str(a.get("software") or "").strip()
                software_norm = software.lower()
                action_key = str(a.get("action_key") or "").strip()
                action_key_norm = action_key.lower()
                contact_rule = str(a.get("contact_rule") or "").strip().lower()
                profile_name = str(a.get("profile_name") or "").strip()
                description = str(a.get("description") or "").strip()
                targets = [str(x).strip() for x in (a.get("contact_targets") or []) if str(x).strip()]
                contact_target = str(a.get("contact_target") or "").strip()
                if not contact_target and targets:
                    contact_target = targets[0]
                is_local_net_action = (
                    software_norm in {"local net", "local"}
                    or contact_rule in {"local_profile", "local_group"}
                    or action_key_norm.startswith("local_")
                )
                if is_local_net_action:
                    # Keep Local Net Group/Net text predictable for quick NCS scanning.
                    profile_part = profile_name or "Local Net"
                    target_part = contact_target or "-"
                    description_part = description or str(label).strip() or "-"
                    group_text = f"{profile_part} - {target_part} - {description_part}"
                else:
                    group_text = f"{grp} {label}".strip() or profile_name or "SOP"
                display_due = due if due >= start else start
                when = self._format_display_time(display_due, include_day, tz)
                search_blob = " ".join(
                    [
                        grp,
                        str(label),
                        group_text,
                        profile_name,
                        contact_target,
                        description,
                        "SOP",
                    ]
                ).upper()
                if search and search not in search_blob:
                    continue
                rows_out.append(
                    {
                        "when_text": when,
                        "when_utc": due,
                        "type": "SOP",
                        "group": group_text,
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
            rows_out.extend(self._load_hf_schedule(start, end, include_day, tz, scan_multi_day=scan_multi_day))
        rows_out.extend(self._load_net_schedule(start, end, include_day, tz, scan_multi_day=scan_multi_day))
        rows_out.sort(
            key=lambda r: (
                r.get("when_utc") if isinstance(r.get("when_utc"), dt.datetime) else dt.datetime.max.replace(tzinfo=dt.timezone.utc),
                str(r.get("type") or ""),
                str(r.get("group") or ""),
            )
        )
        return rows_out[:200]

    def _load_hf_schedule(
        self,
        start: dt.datetime,
        end: dt.datetime,
        include_day: bool,
        tz: dt.tzinfo,
        *,
        scan_multi_day: bool = False,
    ) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        rows = self._daily_schedule_rows()
        if not rows:
            return rows_out
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        for r in rows:
            row = r
            day = (row.get("day") or row.get("day_utc") or row.get("day_name") or "").strip()
            grp = (row.get("group_name") or row.get("group") or "").strip().upper()
            if group_filter and grp != group_filter:
                continue
            start_hm = (row.get("start") or row.get("start_utc") or "").strip()
            band = (row.get("band") or "").strip().upper()
            freq_raw = row.get("frequency") or row.get("freq") or ""
            recurrence = row.get("recurrence") or "Weekly"
            month_weeks = row.get("month_weeks") or ""
            when_utc = self._resolve_schedule_time_utc(
                start,
                end,
                day or "ALL",
                start_hm,
                scan_multi_day,
                recurrence,
                month_weeks,
            )
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
                    "action": "QSY + Hold" if isinstance(freq_mhz, float) else "",
                    "action_kind": "qsy" if isinstance(freq_mhz, float) else "",
                }
            )
        return rows_out

    def _scheduled_group_freqs(
        self, window_minutes: int
    ) -> Tuple[Dict[str, Set[float]], Dict[str, Set[str]]]:
        sched_freqs: Dict[str, Set[float]] = {}
        sched_bands: Dict[str, Set[str]] = {}
        rows = self._daily_schedule_rows()
        if not rows:
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

        for r in rows:
            row = r
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
        self,
        start: dt.datetime,
        end: dt.datetime,
        include_day: bool,
        tz: dt.tzinfo,
        *,
        scan_multi_day: bool = False,
    ) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        rows = self._net_schedule_rows()
        if not rows:
            return rows_out
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        for r in rows:
            row = r
            day = (row.get("day") or row.get("day_utc") or "").strip()
            grp = (row.get("group_name") or row.get("group") or "").strip().upper()
            if group_filter and grp != group_filter:
                continue
            start_hm = (row.get("start") or row.get("start_utc") or "").strip()
            band = (row.get("band") or "").strip().upper()
            freq_raw = row.get("frequency") or row.get("freq") or ""
            net_name = (row.get("net_name") or "").strip()
            recurrence = row.get("recurrence") or "Weekly"
            month_weeks = row.get("month_weeks") or ""
            when_utc = self._resolve_schedule_time_utc(
                start,
                end,
                day or "ALL",
                start_hm,
                scan_multi_day,
                recurrence,
                month_weeks,
            )
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
        recurrence_value: str = "Weekly",
        month_weeks_value: str = "",
    ) -> Optional[dt.datetime]:
        hm = self._parse_hhmm(hhmm)
        if not hm:
            return None
        recurrence = self._normalize_recurrence(recurrence_value)
        month_weeks = self._parse_month_weeks(month_weeks_value)
        day_norm = (day_value or "ALL").strip().upper()
        day_span = max(0, (end_utc.date() - start_utc.date()).days)
        for i in range(day_span + 1):
            day_dt = start_utc + dt.timedelta(days=i)
            candidate = day_dt.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
            if candidate < start_utc or candidate > end_utc:
                continue
            if self._row_applies_on_date(day_norm, candidate, recurrence, month_weeks):
                return candidate
            if not include_day:
                break
        return None

    def _normalize_recurrence(self, value: object) -> str:
        recurrence = str(value or "Weekly").strip().upper()
        if recurrence == "MONTHLY":
            recurrence = "PERIODIC"
        if recurrence not in {"WEEKLY", "DAILY", "PERIODIC", "BI-WEEKLY"}:
            return "WEEKLY"
        return recurrence

    def _parse_month_weeks(self, txt: object) -> List[int]:
        weeks: List[int] = []
        for token in str(txt or "").split(","):
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

    def _month_week_index(self, date_val: dt.date) -> int:
        return 1 + ((date_val.day - 1) // 7)

    def _day_matches(self, day_norm: str, candidate_utc: dt.datetime) -> bool:
        if day_norm in {"", "ALL", "DAILY"}:
            return True
        day_name = candidate_utc.strftime("%A").upper()
        day_key = day_norm[:3]
        return day_name.startswith(day_key) or day_norm.startswith(day_name[:3])

    def _row_applies_on_date(
        self,
        day_norm: str,
        candidate_utc: dt.datetime,
        recurrence: str,
        month_weeks: List[int],
    ) -> bool:
        if recurrence == "DAILY":
            return True
        if not self._day_matches(day_norm, candidate_utc):
            return False
        if recurrence == "PERIODIC":
            weeks = month_weeks or [1]
            return self._month_week_index(candidate_utc.date()) in weeks
        return True

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
        if rows_out and rows_out[0][0] not in {"No matches", "No data"}:
            rows_out.sort(key=lambda row: str(row[0]).strip().upper())
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
        if not bool(self._view_cards.get("propagation", True)):
            return
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
                "Tip: Set Grid 6 in Settings to enable forecast."
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
        schedule_note = " | no scheduled bands" if not all_bands else ""
        self.prop_hint.setText(f"Tip: {target_label} | origin {user_grid}{schedule_note}")

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
        for row in self._daily_schedule_rows():
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
        for row in self._net_schedule_rows():
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
