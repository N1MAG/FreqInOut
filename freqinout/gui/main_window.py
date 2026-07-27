from __future__ import annotations

import datetime
import sqlite3
import sys
import time
from typing import Callable

from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QStackedWidget,
    QVBoxLayout,
    QPushButton,
    QButtonGroup,
    QSizePolicy,
    QLabel,
    QCheckBox,
    QComboBox,
    QGroupBox,
    QMessageBox,
    QProgressDialog,
    QDialog,
    QLayout,
    QSpacerItem,
    QScrollArea,
    QFrame,
    QStyle,
    QToolButton,
)
from PySide6.QtGui import QPixmap, QIcon
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt, QTimer, QUrl
from pathlib import Path

from freqinout.core.logger import log
from freqinout.core.logger import set_log_level
from freqinout.core.config_paths import get_config_dir
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.perf_metrics import span as perf_span
from freqinout.core.plan_context_service import PlanContextService
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.shared_state import ActionFeedbackEvent, ActionFeedbackService
from freqinout.core.station_runtime_manager import StationRuntimeManager
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.background_ingest import BackgroundIngestController
from freqinout.core.dependency_status_service import get_dependency_status_service
from freqinout.core.station_health_summary import summarize_station_health
from freqinout.core.ui_watchdog import UiEventLoopWatchdog
from freqinout.utils.timezones import get_timezone
from freqinout.radio_interface.rigctl_client import rig_control_client_from_settings
from freqinout.radio_interface.js8_status import JS8ControlClient, VarACStatusClient
from freqinout.radio_interface.fldigi_status import FldigiLogStatusClient
from freqinout.radio_interface.js8_rx_hub import JS8RxHub
from freqinout.version import __version__

from freqinout.gui.settings_tab import SettingsTab
from freqinout.gui.daily_schedule_tab import DailyScheduleTab  # HF Frequency Schedule tab
from freqinout.gui.net_schedule_tab import NetScheduleTab
from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab
from freqinout.gui.js8call_net_control_tab import JS8CallNetControlTab
from freqinout.gui.freq_planner_tab import FreqPlannerTab
from freqinout.gui.sop_tab import SOPTab
from freqinout.gui.operator_history_tab import OperatorHistoryTab
from freqinout.gui.local_operator_tab import LocalOperatorTab
from freqinout.gui.local_ncs_tab import LocalNCSTab
from freqinout.gui.log_viewer import LogViewerTab
from freqinout.gui.stations_map_tab import (
    StationsMapTab,
    FEMA_REGIONS,
    LOWER48_STATES,
    STATE_CENTERS,
)
from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.gui.peer_sched_tab import PeerSchedTab
from freqinout.gui.context_help_dialog import ContextHelpDialog
from freqinout.gui.help_tab import HelpTab
from freqinout.gui.help_registry import get_help_context
from freqinout.gui.controlfreq_tab import ControlFreqTab
from freqinout.gui.station_overview_tab import StationOverviewTab
from freqinout.gui.station_health_tab import StationHealthTab
from freqinout.gui.qsy_helper import (
    refresh_hold_duration_combo,
    selected_hold_duration,
    suspend_snapshot,
    suspend_schedule_hold,
    set_scheduler_enabled_override,
    set_active_hold_duration,
    resume_schedule_hold,
    set_hold_duration_default,
    set_suspend_until,
    active_hold_button_role,
    active_hold_button_text,
    active_hold_status_text,
)
from freqinout.gui.theme import resolve_theme, resolve_ui_text_scale, apply_app_theme, button_style, fit_child_combo_boxes


class MainWindow(QMainWindow):
    """
    Main application window for FreqInOut.

    Tabs:
      - Settings
      - HF Frequency Schedule
      - Net Schedule
      - FLDigi Net Control
      - JS8Call Net Control
      - FreqPlanner
      - Operator History
      - Help
    """

    def __init__(self, startup_status: Callable[[str], None] | None = None):
        super().__init__()
        self._startup_status_callback = startup_status
        self._shutting_down = False
        self._app_active = True
        self._ui_resume_pending = False
        self._ui_refresh_dirty = False
        self._ui_timers_paused_for_inactive = False
        self._help_dialog_settle_until = 0.0
        self._ui_resume_settle_timer = QTimer(self)
        self._ui_resume_settle_timer.setSingleShot(True)
        self._ui_resume_settle_timer.setInterval(350)
        self._ui_resume_settle_timer.timeout.connect(self._on_ui_resume_settled)

        self.settings = SettingsManager()
        self.action_feedback_service = ActionFeedbackService()
        self.plan_context_service = PlanContextService()
        self._action_feedback_unsubscribe = None
        self._notify_startup_status("Loading application settings...")
        self.dependency_status_service = get_dependency_status_service(self.settings)
        self.multi_radio_store = MultiRadioStore()
        self.station_runtime_manager = StationRuntimeManager(store=self.multi_radio_store, settings=self.settings)
        self.station_runtime_manager.sync_with_store()
        self._runtime_profile_signature: tuple[object, ...] | None = None
        self._active_runtime_profile = self._load_runtime_active_device_profile()
        self._active_runtime_policy = self._primary_runtime_policy()
        self._suppressed_screen_labels: set[str] = set()
        self._launch_startup_suppressed = False
        self._station_health_scope_map: dict[str, str] = {}
        self.setWindowTitle(f"FreqInOut de N1MAG (v{__version__})")
        self._set_window_icon()

        # Central widget with sidebar navigation + stacked pages
        central = QWidget()
        layout = QHBoxLayout(central)
        self.setCentralWidget(central)

        # Instantiate screens (lazy-load heavy tabs to improve perceived performance)
        self.settings_tab = SettingsTab(self, action_feedback_service=self.action_feedback_service)
        self._sync_settings_runtime_status()
        self.launch_orchestrator = self.settings_tab.launch_orchestrator
        self._launch_progress_dialog: QProgressDialog | None = None
        self._launch_progress_total = 0
        self._launch_progress_done = 0
        self.hf_schedule_tab = DailyScheduleTab(self, plan_context_service=self.plan_context_service)  # this tab is labeled "HF Frequency Schedule"
        self.net_tab = NetScheduleTab(self, plan_context_service=self.plan_context_service)
        self.fldigi_tab = FldigiNetControlTab(self)
        self.js8_tab = JS8CallNetControlTab(self)
        self.sop_tab = SOPTab(self, plan_context_service=self.plan_context_service)
        self.operator_history_tab = OperatorHistoryTab(self)
        self.local_operator_tab = LocalOperatorTab(self)
        self.local_ncs_tab = LocalNCSTab(self)
        self.log_tab: LogViewerTab | None = None
        self._log_dialog: QDialog | None = None
        self.peer_sched_tab = PeerSchedTab(self)
        self.help_tab = HelpTab(self)
        self._context_help_dialog: ContextHelpDialog | None = None
        self.controlfreq_tab = ControlFreqTab(self, plan_context_service=self.plan_context_service)
        self.station_overview_tab = StationOverviewTab(self)
        self.station_overview_tab.set_runtime_manager(self.station_runtime_manager)
        self.station_health_tab = StationHealthTab(self)
        self._refresh_station_health_scope_map()
        self.station_health_tab.set_scope_resolver(self._station_health_scope_resolver)
        self.station_overview_tab.health_details_requested.connect(self._open_station_health_detail)
        self._sop_data_refresh_pending = False
        self._sop_data_refresh_timer = QTimer(self)
        self._sop_data_refresh_timer.setSingleShot(True)
        self._sop_data_refresh_timer.setInterval(90)
        self._sop_data_refresh_timer.timeout.connect(self._flush_sop_data_changed)

        self.freq_planner_tab = None
        self.message_viewer_tab = None
        # Build Map eagerly (hidden) so first click does not lazy-swap widgets.
        self.stations_map_tab = StationsMapTab(self, plan_context_service=self.plan_context_service)
        self._map_prop_target_syncing = False

        self._lazy_placeholders = {}
        self._lazy_factories = {
            "FreqPlanner": self._create_freq_planner_tab,
            "Messages": self._create_message_viewer_tab,
        }

        # Internal screen registry (stable keys used by cross-tab navigation/lazy loading)
        self._screens = [
            ("ControlFreq", self.controlfreq_tab),
            ("Station Overview", self.station_overview_tab),
            ("FreqPlanner", self._placeholder_widget("FreqPlanner")),
            ("SOP", self.sop_tab),
            ("Messages", self._placeholder_widget("Messages")),
            ("NCS-FLDigi/SSB", self.fldigi_tab),
            ("NCS-JS8", self.js8_tab),
            ("NCS-Local", self.local_ncs_tab),
            ("HF Operators", self.operator_history_tab),
            ("Local Operators", self.local_operator_tab),
            ("Map", self.stations_map_tab),
            ("HF Schedule", self.hf_schedule_tab),
            ("Net Schedule", self.net_tab),
            ("Peer Schedules", self.peer_sched_tab),
            ("Station Health", self.station_health_tab),
            ("Settings", self.settings_tab),
            ("Help", self.help_tab),
        ]
        self._notify_startup_status("Building station dashboard...")
        self._screen_index_by_label = {label: idx for idx, (label, _w) in enumerate(self._screens)}
        self._condition_levels_signature: tuple[tuple[str, int], ...] = tuple()
        self._condition_levels_refresh_pending = False
        self._scheduler_status_reason_lines_signature: tuple[str, ...] | None = None
        self._hold_state_snapshot: dict[str, object] | None = None
        self._hold_state_signature: tuple[object, ...] | None = None
        self._station_command_selected_profile_id: int | None = None
        self._station_command_bar_loading = False
        # Sidebar button order/text requested by user. Keep SOP accessible via in-app links,
        # but do not show it as a primary sidebar button.
        self._nav_specs = [
            ("ControlFreq", "ControlFreq"),
            ("Control Center", "Station Overview"),
            ("Health Details", "Station Health"),
            ("Overview", "FreqPlanner"),
            ("Messages", "Messages"),
            ("Map", "Map"),
            ("FLDigi / SSB", "NCS-FLDigi/SSB"),
            ("JS8Call", "NCS-JS8"),
            ("VHF/UHF", "NCS-Local"),
            ("HF Daily", "HF Schedule"),
            ("HF Nets", "Net Schedule"),
            ("HF Peers", "Peer Schedules"),
            ("HF Callsigns", "HF Operators"),
            ("Local Callsigns", "Local Operators"),
            ("SOP Builder", "SOP"),
            ("Settings", "Settings"),
            ("Help", "Help"),
        ]
        self._nav_screen_index_map: dict[int, int] = {}
        self._nav_base_labels: list[str] = []

        # Build sidebar with scrollable nav zone + persistent status dock.
        self.nav_widget = QWidget()
        self.nav_widget.setMinimumWidth(150)
        self.nav_widget.setMaximumWidth(280)
        nav_main_layout = QVBoxLayout(self.nav_widget)
        nav_main_layout.setContentsMargins(4, 4, 4, 4)
        nav_main_layout.setSpacing(6)

        # Logo above nav area (optional if file exists)
        self.logo_label = QLabel()
        self.logo_label.setAlignment(Qt.AlignCenter)
        nav_main_layout.addWidget(self.logo_label)
        self._set_logo_pixmap()

        # Global clock stays visible above navigation so individual tabs do not
        # need to compete for live Local/UTC clock space.
        self.ledge_clock_widget = QFrame(self.nav_widget)
        self.ledge_clock_widget.setObjectName("mainLedgeClock")
        self.ledge_clock_widget.setFrameShape(QFrame.StyledPanel)
        self.ledge_clock_widget.setAccessibleName("Local and UTC clock")
        ledge_clock_layout = QVBoxLayout(self.ledge_clock_widget)
        ledge_clock_layout.setContentsMargins(6, 5, 6, 5)
        ledge_clock_layout.setSpacing(2)
        self.ledge_local_time_label = QLabel("Local --")
        self.ledge_local_time_label.setObjectName("ledgeLocalTime")
        self.ledge_local_time_label.setAlignment(Qt.AlignCenter)
        self.ledge_local_time_label.setAccessibleName("Local time")
        self.ledge_utc_time_label = QLabel("UTC --")
        self.ledge_utc_time_label.setObjectName("ledgeUtcTime")
        self.ledge_utc_time_label.setAlignment(Qt.AlignCenter)
        self.ledge_utc_time_label.setAccessibleName("UTC time")
        ledge_clock_layout.addWidget(self.ledge_local_time_label)
        ledge_clock_layout.addWidget(self.ledge_utc_time_label)
        nav_main_layout.addWidget(self.ledge_clock_widget)

        # Scrollable navigation zone (buttons + map filters + group toggles).
        self.nav_scroll = QScrollArea(self.nav_widget)
        self.nav_scroll.setWidgetResizable(True)
        self.nav_scroll.setFrameShape(QFrame.NoFrame)
        self.nav_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.nav_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.nav_scroll.setMinimumHeight(0)
        self.nav_content = QWidget()
        nav_layout = QVBoxLayout(self.nav_content)
        nav_layout.setContentsMargins(0, 0, 0, 0)
        nav_layout.setSpacing(4)
        self.nav_scroll.setWidget(self.nav_content)
        nav_main_layout.addWidget(self.nav_scroll, 1)

        self.nav_buttons = []
        self.button_group = QButtonGroup(self)
        self.button_group.setExclusive(True)
        self._map_nav_index = None
        self._station_health_nav_index = None
        self._station_health_alert_signature: tuple[object, ...] | None = None
        self._ncs_nav_indices: dict[str, int] = {}
        self._ncs_net_active: dict[str, bool] = {"FLDIGI": False, "JS8": False, "LOCAL": False}
        self._nav_group_headers: dict[str, QPushButton] = {}
        self._nav_group_bodies: dict[str, QWidget] = {}
        self._nav_group_layouts: dict[str, QVBoxLayout] = {}
        self._nav_group_sections: dict[str, QWidget] = {}
        self._nav_group_order: list[str] = ["Station", "FreqPlanner", "NCS", "Operators"]
        self._nav_group_states: dict[str, bool] = self._load_nav_group_states()

        for nav_idx, (button_label, screen_label) in enumerate(self._nav_specs):
            screen_idx = self._screen_index_by_label.get(screen_label)
            if screen_idx is None:
                continue
            group_key = self._nav_group_for_label(button_label, screen_label)
            target_layout = nav_layout
            if group_key:
                target_layout = self._ensure_nav_group_layout(group_key, nav_layout)
            btn = QPushButton(button_label)
            btn.setCheckable(True)
            btn.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
            btn.setMinimumWidth(120)
            btn.setStyleSheet(self._nav_button_alignment_style())
            btn.clicked.connect(lambda _=False, i=screen_idx: self._set_screen(i))
            self.button_group.addButton(btn, screen_idx)
            self.nav_buttons.append(btn)
            btn_idx = len(self.nav_buttons) - 1
            self._nav_screen_index_map[screen_idx] = btn_idx
            self._nav_base_labels.append(button_label)
            target_layout.addWidget(btn)
            if screen_label == "Map":
                self._map_nav_index = nav_idx
            elif screen_label == "Station Health":
                self._station_health_nav_index = btn_idx
            elif screen_label == "NCS-FLDigi/SSB":
                self._ncs_nav_indices["FLDIGI"] = btn_idx
            elif screen_label == "NCS-JS8":
                self._ncs_nav_indices["JS8"] = btn_idx
            elif screen_label == "NCS-Local":
                self._ncs_nav_indices["LOCAL"] = btn_idx

        # Placeholder for map filters (shown only on Map view)
        self.map_filters_container = QWidget()
        self.map_filters_container.setMinimumWidth(120)
        self.map_filters_container.setMaximumWidth(240)
        self.map_filters_layout = QVBoxLayout(self.map_filters_container)
        self.map_filters_layout.setContentsMargins(0, 0, 0, 0)
        nav_layout.addWidget(self.map_filters_container)
        self._init_map_filters()
        nav_layout.addStretch(1)

        # Persistent status dock (outside nav scroll area; always visible).
        self.status_dock_widget = QWidget()
        self.status_dock_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.status_dock_widget.setMinimumHeight(0)
        status_dock_layout = QVBoxLayout(self.status_dock_widget)
        status_dock_layout.setContentsMargins(0, 0, 0, 0)
        status_dock_layout.setSpacing(6)

        # Scheduler status panel (hidden on Map view)
        self.scheduler_status_container = QGroupBox("Schedule Status")
        self.scheduler_status_container.setCheckable(False)
        self.scheduler_status_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        status_title_style = (
            "QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 4px; }"
        )
        self.scheduler_status_container.setStyleSheet(
            status_title_style
        )
        status_layout = QVBoxLayout(self.scheduler_status_container)
        status_layout.setContentsMargins(4, 4, 4, 4)
        status_layout.setSpacing(4)
        status_layout.setSizeConstraint(QLayout.SetMinimumSize)
        self.scheduler_status_header = QLabel("On Schedule")
        self.scheduler_status_header.setAlignment(Qt.AlignCenter)
        self.scheduler_status_header.setWordWrap(True)
        self.scheduler_status_header.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred)
        self.scheduler_status_reasons = QWidget()
        self.scheduler_status_reasons_layout = QVBoxLayout(self.scheduler_status_reasons)
        self.scheduler_status_reasons_layout.setContentsMargins(0, 0, 0, 0)
        self.scheduler_status_reasons_layout.setSpacing(2)
        self.resume_schedule_btn = QPushButton("Resume Schedule")
        self.resume_schedule_btn.setFixedWidth(140)
        self.resume_schedule_btn.clicked.connect(self._on_resume_schedule_clicked)
        self.suspend_schedule_btn = QPushButton("Suspend")
        self.suspend_schedule_btn.setFixedWidth(140)
        self.suspend_schedule_btn.clicked.connect(self._on_suspend_schedule_clicked)
        self.suspend_duration_combo = QComboBox()
        self.suspend_duration_combo.setMinimumWidth(96)
        self.suspend_duration_combo.setMaximumWidth(112)
        self.suspend_duration_combo.setToolTip("Temporary schedule hold duration.")
        self.suspend_duration_combo.currentIndexChanged.connect(self._on_sidebar_hold_duration_changed)
        refresh_hold_duration_combo(self.suspend_duration_combo, self.settings, self._active_runtime_profile)
        self.logs_active_btn = QPushButton("Logs Active")
        self.logs_active_btn.setFixedWidth(140)
        self.logs_active_btn.clicked.connect(self._open_logs_window)
        self.logs_active_btn.setVisible(False)
        try:
            theme = resolve_theme(self.settings)
            self.resume_schedule_btn.setStyleSheet(button_style("muted", theme))
            self.suspend_schedule_btn.setStyleSheet(button_style("warning", theme))
            self.logs_active_btn.setStyleSheet(button_style("warning", theme))
        except Exception:
            pass
        status_layout.addWidget(self.scheduler_status_header)
        status_layout.addWidget(self.scheduler_status_reasons)
        hold_row = QHBoxLayout()
        hold_row.setContentsMargins(0, 0, 0, 0)
        hold_row.setSpacing(6)
        hold_row.addStretch()
        self.suspend_duration_label = QLabel("Hold")
        hold_row.addWidget(self.suspend_duration_label)
        hold_row.addWidget(self.suspend_duration_combo)
        hold_row.addStretch()
        status_layout.addLayout(hold_row)
        status_layout.addWidget(self.suspend_schedule_btn, alignment=Qt.AlignCenter)
        status_layout.addWidget(self.resume_schedule_btn, alignment=Qt.AlignCenter)
        status_layout.addWidget(self.logs_active_btn, alignment=Qt.AlignCenter)
        self.resume_schedule_btn.setVisible(False)
        status_dock_layout.addWidget(self.scheduler_status_container)

        # Condition levels panel (global; per-HF operating group status card).
        self.condition_level_container = QGroupBox("Condition Level")
        self.condition_level_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        condition_layout = QVBoxLayout(self.condition_level_container)
        condition_layout.setContentsMargins(4, 4, 4, 4)
        condition_layout.setSpacing(4)
        self.condition_levels_rows = QWidget()
        self.condition_levels_rows_layout = QVBoxLayout(self.condition_levels_rows)
        self.condition_levels_rows_layout.setContentsMargins(0, 0, 0, 0)
        self.condition_levels_rows_layout.setSpacing(2)
        self.condition_levels_summary = QLabel("No condition levels configured.")
        self.condition_levels_summary.setWordWrap(True)
        self.condition_levels_summary.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Minimum)
        self.condition_levels_summary.setVisible(False)
        self.condition_levels_edit_btn = QToolButton(self.condition_level_container)
        self.condition_levels_edit_btn.setText("Edit Levels")
        self.condition_levels_edit_btn.setAutoRaise(True)
        self.condition_levels_edit_btn.clicked.connect(self._open_condition_levels_editor)
        condition_layout.addWidget(self.condition_levels_rows)
        condition_layout.addWidget(self.condition_levels_edit_btn, alignment=Qt.AlignLeft)
        self.condition_level_container.setStyleSheet(status_title_style)
        status_dock_layout.addWidget(self.condition_level_container)

        nav_main_layout.addWidget(self.status_dock_widget, 0)

        self._update_scheduler_action_button_widths()
        self._update_nav_layout_metrics()
        QTimer.singleShot(0, self._sync_status_box_width)

        # Stacked content
        self.stack = QStackedWidget()
        for _label, widget in self._screens:
            self.stack.addWidget(widget)

        # Right-side layout (notice bar + stacked content)
        right_container = QWidget()
        right_layout = QVBoxLayout(right_container)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(4)

        self.runtime_mode_banner = QFrame(right_container)
        self.runtime_mode_banner.setVisible(False)
        banner_layout = QHBoxLayout(self.runtime_mode_banner)
        banner_layout.setContentsMargins(10, 8, 10, 8)
        banner_layout.setSpacing(6)
        self.runtime_mode_label = QLabel("")
        self.runtime_mode_label.setWordWrap(True)
        banner_layout.addWidget(self.runtime_mode_label)
        right_layout.addWidget(self.runtime_mode_banner, 0)

        self.action_feedback_banner = QFrame(right_container)
        self.action_feedback_banner.setVisible(False)
        self.action_feedback_banner.setAccessibleName("Action feedback")
        feedback_layout = QHBoxLayout(self.action_feedback_banner)
        feedback_layout.setContentsMargins(10, 6, 8, 6)
        feedback_layout.setSpacing(8)
        self.action_feedback_label = QLabel("")
        self.action_feedback_label.setWordWrap(True)
        self.action_feedback_label.setAccessibleName("Action feedback message")
        feedback_layout.addWidget(self.action_feedback_label, 1)
        self.action_feedback_dismiss_btn = QToolButton()
        self.action_feedback_dismiss_btn.setText("x")
        self.action_feedback_dismiss_btn.setToolTip("Dismiss status")
        self.action_feedback_dismiss_btn.clicked.connect(self._hide_action_feedback_banner)
        self.action_feedback_history_btn = QToolButton()
        self.action_feedback_history_btn.setText("History")
        self.action_feedback_history_btn.setToolTip("Show recent actions")
        self.action_feedback_history_btn.setAccessibleName("Recent actions")
        self.action_feedback_history_btn.clicked.connect(self._show_recent_actions_dialog)
        feedback_layout.addWidget(self.action_feedback_history_btn, 0)
        feedback_layout.addWidget(self.action_feedback_dismiss_btn, 0)
        right_layout.addWidget(self.action_feedback_banner, 0)
        self._action_feedback_clear_timer = QTimer(self)
        self._action_feedback_clear_timer.setSingleShot(True)
        self._action_feedback_clear_timer.timeout.connect(self._hide_action_feedback_banner)
        self._action_feedback_unsubscribe = self.action_feedback_service.subscribe(self._on_action_feedback_event)
        self._recent_actions_dialog: QDialog | None = None

        self.station_command_bar = QFrame(right_container)
        self.station_command_bar.setObjectName("stationCommandBar")
        self.station_command_bar.setAccessibleName("Station command context")
        self.station_command_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        command_layout = QHBoxLayout(self.station_command_bar)
        command_layout.setContentsMargins(10, 6, 10, 6)
        command_layout.setSpacing(8)
        command_layout.addWidget(QLabel("Radio"), 0)
        self.station_command_radio_combo = QComboBox(self.station_command_bar)
        self.station_command_radio_combo.setObjectName("stationCommandRadioSelector")
        self.station_command_radio_combo.setMinimumWidth(140)
        self.station_command_radio_combo.currentIndexChanged.connect(self._on_station_command_radio_changed)
        command_layout.addWidget(self.station_command_radio_combo, 0)
        self.station_command_now_label = QLabel("Now: unavailable")
        self.station_command_now_label.setObjectName("stationCommandNow")
        self.station_command_now_label.setWordWrap(False)
        command_layout.addWidget(self.station_command_now_label, 0)
        self.station_command_state_label = QLabel("State: unknown")
        self.station_command_state_label.setObjectName("stationCommandState")
        self.station_command_state_label.setWordWrap(False)
        command_layout.addWidget(self.station_command_state_label, 0)
        self.station_command_next_label = QLabel("Next: none")
        self.station_command_next_label.setObjectName("stationCommandNext")
        self.station_command_next_label.setWordWrap(False)
        command_layout.addWidget(self.station_command_next_label, 1)
        self.station_command_qsy_btn = QPushButton("QSY...")
        self.station_command_qsy_btn.setObjectName("stationCommandQsy")
        self.station_command_hold_btn = QPushButton("Hold")
        self.station_command_hold_btn.setObjectName("stationCommandHold")
        self.station_command_suspend_btn = QPushButton("Suspend")
        self.station_command_suspend_btn.setObjectName("stationCommandSuspend")
        self.station_command_resume_btn = QPushButton("Resume")
        self.station_command_resume_btn.setObjectName("stationCommandResume")
        for btn in (
            self.station_command_qsy_btn,
            self.station_command_hold_btn,
            self.station_command_suspend_btn,
            self.station_command_resume_btn,
        ):
            btn.setEnabled(False)
            btn.setToolTip("Station command wiring is not enabled yet.")
            command_layout.addWidget(btn, 0)
        right_layout.addWidget(self.station_command_bar, 0)

        right_layout.addWidget(self.stack, stretch=1)

        # Layout composition
        layout.addWidget(self.nav_widget)
        layout.addWidget(right_container, stretch=1)
        self.stack.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # Suggest a modest minimum size
        self.setMinimumSize(900, 600)

        self._apply_app_theme()
        self._ui_watchdog = UiEventLoopWatchdog(self)
        self._ui_watchdog.start()
        self._sop_next_due_cache_ts = 0.0
        self._sop_next_due_minutes = None
        self._active_tab_index = None
        self._lazy_prewarm_labels = ["Messages", "FreqPlanner"]
        self._lazy_prewarm_index = 0
        self._webengine_warmup_widget = None
        self._webengine_warmup_done = False
        self._pending_map_switch_index: int | None = None
        self._runtime_client_signature: tuple[object, ...] | None = None

        startup_policy = self._primary_runtime_policy()
        startup_suppressed = self._suppressed_screens_for_runtime(self._active_runtime_profile, startup_policy)
        self._lazy_prewarm_labels = self._runtime_lazy_prewarm_labels(startup_suppressed)
        self._startup_webengine_prewarm_enabled = ("Map" not in startup_suppressed) and self._should_prewarm_webengine_at_startup()
        if self._startup_webengine_prewarm_enabled:
            # Kick WebEngine warmup during init so first Map activation is not the
            # first WebEngine surface/process startup path seen by users.
            self._prewarm_webengine()
        else:
            log.info("MainWindow: startup WebEngine prewarm disabled (platform default/settings)")

        # Default selection
        if self.nav_buttons:
            self.nav_buttons[0].setChecked(True)
            first_screen_index = self.button_group.id(self.nav_buttons[0])
            self._set_screen(first_screen_index if first_screen_index >= 0 else 0)
        QTimer.singleShot(600, self._start_lazy_prewarm)

        # Optional: apply callsign to tab captions if already configured
        self._apply_callsign_to_tab_titles()

        # Start scheduler engine
        primary_runtime = self.station_runtime_manager.get_primary_runtime()
        self.rig_client = (
            primary_runtime.rig_client
            if primary_runtime is not None and primary_runtime.rig_client is not None
            else rig_control_client_from_settings(self.settings)
        )
        self.js8_control = (
            primary_runtime.js8_control_client
            if primary_runtime is not None and primary_runtime.js8_control_client is not None
            else self._new_js8_control_client()
        )
        self.varac_status = (
            primary_runtime.varac_status_client
            if primary_runtime is not None and primary_runtime.varac_status_client is not None
            else self._new_varac_status_client()
        )
        self.fldigi_log_status = FldigiLogStatusClient()
        self.scheduler = SchedulerEngine(
            self,
            rig=self.rig_client,
            js8=self.js8_control,
            varac=self.varac_status,
            fldigi_log=self.fldigi_log_status,
            station_runtime_manager=self.station_runtime_manager,
        )
        self._runtime_client_signature = self._runtime_client_signature_for_settings()
        try:
            if hasattr(self.scheduler, "set_runtime_scheduler_enabled"):
                self.scheduler.set_runtime_scheduler_enabled(bool(startup_policy.get("scheduler_enabled", True)))
        except Exception:
            pass
        try:
            if hasattr(self.scheduler, "set_runtime_timer_policy"):
                self.scheduler.set_runtime_timer_policy(self._runtime_timer_policy_for(self._active_runtime_profile))
        except Exception as e:
            log.debug("MainWindow: failed to apply startup runtime timer policy: %s", e)
        try:
            set_scheduler_enabled_override(bool(startup_policy.get("scheduler_enabled", True)))
        except Exception:
            pass
        self._notify_startup_status("Starting scheduler services...")
        self.scheduler.start()
        self.background_ingest = BackgroundIngestController(self.settings)
        if self._runtime_background_ingest_enabled(self._active_runtime_profile, startup_policy):
            self.background_ingest.start()
        else:
            log.info("MainWindow: background ingest disabled for current runtime policy")
        try:
            if hasattr(self.launch_orchestrator, "set_runtime_launch_enabled"):
                self.launch_orchestrator.set_runtime_launch_enabled(
                    self._runtime_launch_enabled(self._active_runtime_profile, startup_policy),
                    reason="Launch Control is disabled by the primary frequency plan.",
                )
            self._launch_startup_suppressed = not self._runtime_launch_enabled(self._active_runtime_profile, startup_policy)
        except Exception:
            pass
        try:
            self.scheduler.off_schedule_detected.connect(self._on_off_schedule_detected)
        except Exception:
            pass
        try:
            self.scheduler.off_schedule_cleared.connect(self._dismiss_off_schedule_prompt)
        except Exception:
            pass
        try:
            self.scheduler.varac_wait_detected.connect(self._on_varac_wait_detected)
        except Exception:
            pass
        try:
            self.scheduler.varac_wait_cleared.connect(self._dismiss_varac_wait_prompt)
        except Exception:
            pass
        try:
            self.scheduler.coordination_conflict_detected.connect(self._on_coordination_conflict_detected)
        except Exception:
            pass
        try:
            self.scheduler.coordination_conflict_cleared.connect(self._dismiss_coordination_conflict_prompt)
        except Exception:
            pass
        try:
            self.scheduler.active_entry_changed.connect(self._refresh_scheduler_status_panel)
        except Exception:
            pass
        try:
            if hasattr(self.fldigi_tab, "net_status_changed"):
                self.fldigi_tab.net_status_changed.connect(self._on_ncs_net_status_changed)
            if hasattr(self.js8_tab, "net_status_changed"):
                self.js8_tab.net_status_changed.connect(self._on_ncs_net_status_changed)
            if hasattr(self.local_ncs_tab, "net_status_changed"):
                self.local_ncs_tab.net_status_changed.connect(self._on_ncs_net_status_changed)
        except Exception:
            pass

        self._status_timer = QTimer(self)
        self._status_timer.setInterval(5000)
        self._status_timer.timeout.connect(self._refresh_scheduler_status_panel)
        self._status_timer.timeout.connect(self._refresh_condition_level_panel)
        self._status_timer.timeout.connect(self._refresh_station_overview)
        self._status_timer.timeout.connect(self._refresh_station_health_alert)
        self._status_timer.timeout.connect(self._check_timed_debug_expiry)
        self._status_timer.start()
        self._ledge_clock_timer = QTimer(self)
        self._ledge_clock_timer.setInterval(1000)
        self._ledge_clock_timer.timeout.connect(self._update_ledge_clock)
        self._ledge_clock_timer.start()
        self._update_ledge_clock()
        self._condition_levels_refresh_timer = QTimer(self)
        self._condition_levels_refresh_timer.setSingleShot(True)
        self._condition_levels_refresh_timer.setInterval(90)
        self._condition_levels_refresh_timer.timeout.connect(self._apply_condition_levels_changed)
        self._hold_state_timer = QTimer(self)
        self._hold_state_timer.setInterval(1000)
        self._hold_state_timer.timeout.connect(self._on_hold_state_tick)

        app = QApplication.instance()
        if app is not None:
            try:
                self._app_active = app.applicationState() == Qt.ApplicationActive
                app.applicationStateChanged.connect(self._on_application_state_changed)
            except Exception as e:
                log.debug("MainWindow: UI lifecycle state wiring failed: %s", e)
            app.aboutToQuit.connect(self._on_app_about_to_quit)

        self.on_hold_state_changed(force_reload=True)

        # Wire settings_saved signal
        def _connect_or_log(label, signal, slot) -> None:
            try:
                signal.connect(slot)
            except Exception as e:
                log.debug("MainWindow signal wiring failed: %s: %s", label, e)

        _connect_or_log("settings_saved -> js8_tab", self.settings_tab.settings_saved, self.js8_tab.on_settings_saved)
        _connect_or_log("settings_saved -> hf_schedule_tab", self.settings_tab.settings_saved, self.hf_schedule_tab.on_settings_saved)
        _connect_or_log("settings_saved -> fldigi_tab", self.settings_tab.settings_saved, self.fldigi_tab.on_settings_saved)
        _connect_or_log("settings_saved -> net_tab", self.settings_tab.settings_saved, self.net_tab.on_settings_saved)
        self.settings_tab.settings_saved.connect(self._on_settings_saved_for_lazy_tabs)
        _connect_or_log("settings_saved -> sop_tab", self.settings_tab.settings_saved, self.sop_tab.on_settings_saved)
        try:
            if hasattr(self.settings_tab, "local_net_profiles_changed"):
                self.settings_tab.local_net_profiles_changed.connect(self.sop_tab.on_local_net_profiles_updated)
        except Exception as e:
            log.debug("MainWindow signal wiring failed: local_net_profiles_changed -> sop_tab: %s", e)
        try:
            if hasattr(self.sop_tab, "sop_data_changed"):
                self.sop_tab.sop_data_changed.connect(self._on_sop_data_changed)
        except Exception as e:
            log.debug("MainWindow signal wiring failed: sop_data_changed -> main_window: %s", e)
        _connect_or_log("settings_saved -> local_operator_tab", self.settings_tab.settings_saved, self.local_operator_tab.on_settings_saved)
        _connect_or_log("settings_saved -> local_ncs_tab", self.settings_tab.settings_saved, self.local_ncs_tab.on_settings_saved)
        # Message tab settings saved handled by _on_settings_saved_for_lazy_tabs
        try:
            if hasattr(self.operator_history_tab, "operator_history_updated"):
                self.operator_history_tab.operator_history_updated.connect(
                    self._on_operator_history_local_update
                )
        except Exception as e:
            log.debug("MainWindow signal wiring failed: operator_history_updated -> main_window: %s", e)
        try:
            if hasattr(self.local_operator_tab, "local_operator_updated"):
                self.local_operator_tab.local_operator_updated.connect(self.local_ncs_tab.reload_operator_lookup)
        except Exception as e:
            log.debug("MainWindow signal wiring failed: local_operator_updated -> local_ncs_tab: %s", e)
        try:
            if hasattr(self.local_ncs_tab, "local_data_updated"):
                self.local_ncs_tab.local_data_updated.connect(self.local_operator_tab._load_data)
                self.local_ncs_tab.local_data_updated.connect(self.local_ncs_tab.reload_operator_lookup)
        except Exception as e:
            log.debug("MainWindow signal wiring failed: local_data_updated fanout: %s", e)
        _connect_or_log("settings_saved -> apply theme", self.settings_tab.settings_saved, self._apply_app_theme)
        _connect_or_log("settings_saved -> runtime settings", self.settings_tab.settings_saved, self._on_runtime_settings_saved)
        _connect_or_log("settings_saved -> sync runtime status", self.settings_tab.settings_saved, self._sync_settings_runtime_status)
        try:
            if hasattr(self.settings_tab, "device_profiles_changed"):
                self.settings_tab.device_profiles_changed.connect(self._on_runtime_device_profiles_changed)
        except Exception as e:
            log.debug("MainWindow signal wiring failed: device_profiles_changed -> runtime profile: %s", e)
        _connect_or_log("settings_saved -> log indicator", self.settings_tab.settings_saved, self._update_log_indicator)
        _connect_or_log("settings_saved -> background ingest", self.settings_tab.settings_saved, self.background_ingest.refresh_runtime_settings)
        _connect_or_log("settings_saved -> station health", self.settings_tab.settings_saved, self._on_station_health_settings_saved)
        _connect_or_log("open_logs_requested -> log window", self.settings_tab.open_logs_requested, self._open_logs_window)
        _connect_or_log("log_level_changed -> log indicator", self.settings_tab.log_level_changed, self._update_log_indicator)
        self.hf_schedule_tab.schedule_saved.connect(self._refresh_freq_planner_if_loaded)
        self.hf_schedule_tab.schedule_saved.connect(self.scheduler.force_refresh)
        if hasattr(self.sop_tab, "on_hf_schedule_saved"):
            self.hf_schedule_tab.schedule_saved.connect(self.sop_tab.on_hf_schedule_saved)
        self.net_tab.schedule_saved.connect(self._refresh_freq_planner_if_loaded)
        self.net_tab.schedule_saved.connect(self.scheduler.force_refresh)

        log.info("Main window initialized.")
        # Sync sidebar filters initially
        self._sync_map_filters_from_tab()
        self._update_log_indicator()
        self._refresh_scheduler_status_panel()
        self._refresh_condition_level_panel()
        self._refresh_station_overview(force=True)
        self._refresh_station_health_alert()
        self._apply_runtime_profile_state(force=True)

        try:
            self.launch_orchestrator.sequence_started.connect(self._on_launch_sequence_started)
        except Exception:
            pass
        try:
            self.launch_orchestrator.sequence_progress.connect(self._on_launch_sequence_progress)
        except Exception:
            pass
        try:
            self.launch_orchestrator.sequence_finished.connect(self._on_launch_sequence_finished)
        except Exception:
            pass
        QTimer.singleShot(1200, self._start_launch_control_startup)
        self._notify_startup_status("Opening FIO...")

    def _notify_startup_status(self, message: str) -> None:
        callback = getattr(self, "_startup_status_callback", None)
        if callback is None:
            return
        try:
            callback(message)
        except Exception as e:
            log.debug("MainWindow startup status update failed: %s", e)

    @staticmethod
    def _action_feedback_banner_role(status: str) -> str:
        normalized = str(status or "").strip().lower()
        if normalized == "succeeded":
            return "success"
        if normalized in {"blocked", "failed", "partial"}:
            return "warning"
        if normalized in {"requested", "in_progress"}:
            return "info"
        return "secondary"

    def _action_feedback_banner_style(self, status: str) -> str:
        theme = resolve_theme(self.settings)
        role = self._action_feedback_banner_role(status)
        if role == "success":
            border = theme.get("success", "#2E7D32")
        elif role == "warning":
            border = theme.get("warning", "#C99700")
        elif role == "info":
            border = theme.get("accent", "#2a6fd3")
        else:
            border = theme.get("border", "#cccccc")
        bg = theme.get("surface", "#ffffff")
        text = theme.get("text", "#222222")
        return (
            "QFrame {"
            f" background: {bg};"
            f" color: {text};"
            f" border: 1px solid {border};"
            " border-radius: 6px;"
            "}"
            " QLabel { border: none; background: transparent; }"
            " QToolButton { border: none; background: transparent; padding: 2px 6px; }"
        )

    @staticmethod
    def _action_feedback_display_ms(status: str) -> int:
        normalized = str(status or "").strip().lower()
        if normalized in {"blocked", "failed", "partial"}:
            return 12000
        if normalized in {"requested", "in_progress"}:
            return 7000
        return 6000

    def _hide_action_feedback_banner(self) -> None:
        if hasattr(self, "_action_feedback_clear_timer"):
            self._action_feedback_clear_timer.stop()
        if hasattr(self, "action_feedback_banner"):
            self.action_feedback_banner.setVisible(False)

    @staticmethod
    def _action_feedback_banner_scopes() -> set[str]:
        return {"settings", "radio", "scheduler"}

    def _on_action_feedback_event(self, event: ActionFeedbackEvent) -> None:
        if str(event.scope or "").strip().lower() not in self._action_feedback_banner_scopes():
            return
        if not hasattr(self, "action_feedback_banner"):
            return
        summary = str(event.summary or "").strip()
        if not summary:
            return
        detail = str(event.detail or "").strip()
        status = str(event.status or "").strip().lower()
        self.action_feedback_label.setText(summary)
        self.action_feedback_label.setToolTip(detail or summary)
        self.action_feedback_banner.setStyleSheet(self._action_feedback_banner_style(status))
        self.action_feedback_banner.setVisible(True)
        timeout_ms = self._action_feedback_display_ms(status)
        if timeout_ms > 0:
            self._action_feedback_clear_timer.start(timeout_ms)

    @staticmethod
    def _recent_action_line(event: ActionFeedbackEvent) -> str:
        status = str(event.status or "").strip().upper() or "STATUS"
        summary = str(event.summary or "").strip() or str(event.action_type or "Action").strip() or "Action"
        target = str(event.target_label or "").strip()
        time_txt = str(event.timestamp_utc or "").strip()
        if "T" in time_txt:
            time_txt = time_txt.split("T", 1)[1].replace("Z", "")
            time_txt = time_txt[:8]
        bits = [status]
        if time_txt:
            bits.append(time_txt)
        if target:
            bits.append(target)
        return f"{' | '.join(bits)}: {summary}"

    def _show_recent_actions_dialog(self) -> None:
        existing = getattr(self, "_recent_actions_dialog", None)
        if existing is not None:
            existing.show()
            existing.raise_()
            existing.activateWindow()
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Recent Actions")
        dialog.setAccessibleName("Recent actions")
        dialog.setModal(False)
        dialog.setMinimumWidth(520)
        dialog.setMinimumHeight(260)
        dialog.setAttribute(Qt.WA_DeleteOnClose, True)
        self._recent_actions_dialog = dialog

        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)
        title = QLabel("Recent Actions")
        title.setStyleSheet("font-weight: 700;")
        root.addWidget(title)

        events = self.action_feedback_service.recent()[:20]
        if not events:
            empty = QLabel("No recent actions.")
            empty.setWordWrap(True)
            root.addWidget(empty)
        else:
            scroll = QScrollArea(dialog)
            scroll.setWidgetResizable(True)
            scroll.setFrameShape(QFrame.NoFrame)
            scroll.setAccessibleName("Recent actions list")
            rows_widget = QWidget()
            rows_widget.setAccessibleName("Recent actions")
            rows_layout = QVBoxLayout(rows_widget)
            rows_layout.setContentsMargins(0, 0, 0, 0)
            rows_layout.setSpacing(6)
            for event in events:
                line = QLabel(self._recent_action_line(event))
                line.setWordWrap(True)
                line.setAccessibleName(line.text())
                detail = str(event.detail or "").strip()
                line.setToolTip(detail or str(event.summary or ""))
                rows_layout.addWidget(line)
            rows_layout.addStretch(1)
            scroll.setWidget(rows_widget)
            root.addWidget(scroll, 1)

        close_row = QHBoxLayout()
        close_row.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dialog.close)
        close_row.addWidget(close_btn)
        root.addLayout(close_row)
        dialog.destroyed.connect(lambda *_args: setattr(self, "_recent_actions_dialog", None))
        dialog.show()

    def _sync_settings_runtime_status(self) -> None:
        try:
            self.station_runtime_manager.sync_with_store(refresh_runtime_status=True)
            status = self.station_runtime_manager.runtime_status()
        except Exception:
            status = None
        try:
            if hasattr(self, "settings_tab") and hasattr(self.settings_tab, "set_multi_rig_runtime_status"):
                self.settings_tab.set_multi_rig_runtime_status(status)
        except Exception as exc:
            log.debug("MainWindow: failed syncing runtime status to Settings: %s", exc)

    def _ui_refresh_allowed(self) -> bool:
        return bool(
            not getattr(self, "_shutting_down", False)
            and getattr(self, "_app_active", True)
            and not getattr(self, "_ui_resume_pending", False)
        )

    def _mark_ui_refresh_dirty(self, reason: str = "") -> None:
        self._ui_refresh_dirty = True
        if reason:
            log.debug("UI_LIFECYCLE|refresh_deferred reason=%s", reason)

    def _pause_noncritical_ui_timers(self) -> None:
        for timer_name in ("_status_timer", "_hold_state_timer", "_condition_levels_refresh_timer"):
            timer = getattr(self, timer_name, None)
            if isinstance(timer, QTimer) and timer.isActive():
                timer.stop()
                self._ui_timers_paused_for_inactive = True

    def _resume_noncritical_ui_timers(self) -> None:
        timer = getattr(self, "_status_timer", None)
        if isinstance(timer, QTimer) and not timer.isActive():
            timer.start()
        try:
            self.on_hold_state_changed(force_reload=False)
        except Exception:
            pass
        if bool(getattr(self, "_condition_levels_refresh_pending", False)):
            timer = getattr(self, "_condition_levels_refresh_timer", None)
            if isinstance(timer, QTimer):
                timer.start()
        self._ui_timers_paused_for_inactive = False

    def _set_child_app_active(self, active: bool) -> None:
        for label, widget in getattr(self, "_screens", []):
            try:
                if hasattr(widget, "set_app_active"):
                    widget.set_app_active(bool(active))
            except Exception as e:
                log.debug("UI_LIFECYCLE|child_state_failed label=%s err=%s", label, e)

    def _flush_visible_ui_refresh(self, reason: str = "resume") -> None:
        if not self._ui_refresh_allowed():
            self._mark_ui_refresh_dirty(reason)
            return
        self._ui_refresh_dirty = False
        log.info("UI_LIFECYCLE|visible_refresh reason=%s", reason)
        for callback in (
            self._refresh_scheduler_status_panel,
            self._refresh_condition_level_panel,
            self._refresh_station_overview,
            self._refresh_station_health_alert,
            self._check_timed_debug_expiry,
        ):
            try:
                callback()
            except Exception:
                pass
        try:
            widget = self.stack.currentWidget() if hasattr(self, "stack") else None
            if widget is not None and hasattr(widget, "on_tab_activated"):
                QTimer.singleShot(0, widget.on_tab_activated)
        except Exception:
            pass

    def _on_ui_resume_settled(self) -> None:
        self._ui_resume_pending = False
        self._resume_noncritical_ui_timers()
        self._set_child_app_active(True)
        self._flush_visible_ui_refresh("app_resume")

    def _on_application_state_changed(self, state) -> None:
        active = state == Qt.ApplicationActive
        if active == getattr(self, "_app_active", True):
            return
        self._app_active = active
        log.info("UI_LIFECYCLE|app_active=%s state=%s", active, state)
        if not active:
            self._ui_resume_pending = False
            self._ui_resume_settle_timer.stop()
            self._pause_noncritical_ui_timers()
            self._set_child_app_active(False)
            self._mark_ui_refresh_dirty("app_inactive")
            return
        self._ui_resume_pending = True
        self._pause_noncritical_ui_timers()
        self._set_child_app_active(False)
        self._ui_resume_settle_timer.start()

    def refresh_operator_history_views(self):
        """
        Reload operator history across tabs so new entries (e.g., CSV import, JS8 load)
        are visible without restarting.
        """
        try:
            if hasattr(self.operator_history_tab, "_load_data"):
                self.operator_history_tab._load_data()
        except Exception as e:
            log.debug("MainWindow: operator_history_tab refresh failed: %s", e)
        try:
            if self.stations_map_tab is not None and hasattr(self.stations_map_tab, "_load_operator_history"):
                self.stations_map_tab._load_operator_history()
                if hasattr(self.stations_map_tab, "_schedule_render"):
                    self.stations_map_tab._schedule_render()
        except Exception as e:
            log.debug("MainWindow: stations_map_tab refresh failed: %s", e)
        try:
            if hasattr(self.fldigi_tab, "_load_known_operators"):
                self.fldigi_tab._load_known_operators()
        except Exception as e:
            log.debug("MainWindow: fldigi_tab refresh failed: %s", e)

    def _on_operator_history_local_update(self) -> None:
        """
        Lightweight fanout for updates that originated inside Operators tab.
        The Operators table already has local state applied; avoid reloading it.
        """
        try:
            if self.stations_map_tab is not None and hasattr(self.stations_map_tab, "_load_operator_history"):
                self.stations_map_tab._load_operator_history()
                map_visible = bool(getattr(self.stations_map_tab, "_map_visible", False))
                if map_visible and hasattr(self.stations_map_tab, "_schedule_render"):
                    self.stations_map_tab._schedule_render()
                elif not map_visible and hasattr(self.stations_map_tab, "_map_dirty"):
                    self.stations_map_tab._map_dirty = True
        except Exception as e:
            log.debug("MainWindow: stations_map_tab local refresh failed: %s", e)
        try:
            if hasattr(self.fldigi_tab, "_load_known_operators"):
                self.fldigi_tab._load_known_operators()
        except Exception as e:
            log.debug("MainWindow: fldigi_tab local refresh failed: %s", e)

    def on_peer_schedule_data_changed(self) -> None:
        try:
            if hasattr(self, "controlfreq_tab") and self.controlfreq_tab is not None:
                if hasattr(self.controlfreq_tab, "on_peer_schedule_data_changed"):
                    self.controlfreq_tab.on_peer_schedule_data_changed()
        except Exception as e:
            log.debug("MainWindow: controlfreq peer schedule refresh failed: %s", e)
        try:
            if self.stations_map_tab is not None:
                map_visible = bool(getattr(self.stations_map_tab, "_map_visible", False))
                if map_visible and hasattr(self.stations_map_tab, "_schedule_render"):
                    self.stations_map_tab._schedule_render()
                elif hasattr(self.stations_map_tab, "_map_dirty"):
                    self.stations_map_tab._map_dirty = True
        except Exception as e:
            log.debug("MainWindow: stations_map_tab peer schedule refresh failed: %s", e)

    def _update_log_indicator(self) -> None:
        try:
            level = (self.settings.get("log_level", "") or "DISABLED").upper()
            if level == "DISABLED":
                self.logs_active_btn.setVisible(False)
            else:
                self.logs_active_btn.setVisible(True)
                self.logs_active_btn.setText(f"Logs: {level}")
                self.logs_active_btn.setToolTip(
                    "Logging is active. Disable in Settings unless you are troubleshooting."
                )
                try:
                    theme = resolve_theme(self.settings)
                    self.logs_active_btn.setStyleSheet(button_style("warning", theme))
                except Exception:
                    pass
        except Exception as e:
            log.debug("MainWindow: log indicator update failed: %s", e)

    def _open_logs_window(self) -> None:
        try:
            if self.log_tab is None:
                self.log_tab = LogViewerTab(self)
                try:
                    self.log_tab.log_level_changed.connect(self._update_log_indicator)
                except Exception:
                    pass
            if self._log_dialog is None:
                dlg = QDialog(self)
                dlg.setWindowTitle("Logs")
                dlg.resize(980, 620)
                layout = QVBoxLayout(dlg)
                layout.setContentsMargins(8, 8, 8, 8)
                layout.addWidget(self.log_tab)
                try:
                    dlg.finished.connect(lambda _=0: self.log_tab.set_tab_active(False))
                except Exception:
                    pass
                self._log_dialog = dlg
            self._log_dialog.show()
            self._log_dialog.raise_()
            self._log_dialog.activateWindow()
            try:
                self.log_tab.set_tab_active(True)
            except Exception:
                pass
            try:
                self.log_tab._refresh()  # type: ignore[attr-defined]
            except Exception:
                pass
        except Exception as e:
            log.debug("MainWindow: failed to open logs window: %s", e)

    def _check_timed_debug_expiry(self) -> None:
        try:
            until_txt = (self.settings.get("timed_debug_until_utc", "") or "").strip()
            if not until_txt:
                return
            try:
                until_dt = datetime.datetime.fromisoformat(until_txt)
            except Exception:
                until_dt = None
            if until_dt is None:
                return
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=datetime.timezone.utc)
            else:
                until_dt = until_dt.astimezone(datetime.timezone.utc)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if now_utc < until_dt:
                return
            prev = (self.settings.get("timed_debug_prev_level", "") or "DISABLED").strip().upper()
            if prev not in {"DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"}:
                prev = "DISABLED"
            self.settings.set_many(
                {
                    "log_level": prev,
                    "timed_debug_until_utc": "",
                    "timed_debug_prev_level": "",
                }
            )
            set_log_level(prev)
            if self.log_tab is not None:
                try:
                    idx = self.log_tab.level_combo.findText(prev)
                    if idx >= 0:
                        self.log_tab.level_combo.setCurrentIndex(idx)
                except Exception:
                    pass
            self._update_log_indicator()
        except Exception as e:
            log.debug("MainWindow: timed debug expiry check failed: %s", e)

    def _init_map_filters(self) -> None:
        """
        Build a static sidebar panel for map display filters (no reparenting).
        """
        box = QGroupBox("Map Layers")
        box.setCheckable(False)
        v = QVBoxLayout(box)
        v.setContentsMargins(4, 4, 4, 4)
        self.map_cb_callsigns = QCheckBox("Callsigns")
        self.map_cb_regions = QCheckBox("Regions")
        self.map_cb_grids = QCheckBox("Grids")
        self.map_cb_states = QCheckBox("States")
        self.map_cb_cities = QCheckBox("Cities")
        v.addWidget(self.map_cb_callsigns)
        grid_row1 = QHBoxLayout()
        self.map_cb_regions.setMinimumWidth(90)
        self.map_cb_states.setMinimumWidth(90)
        grid_row1.addWidget(self.map_cb_regions)
        grid_row1.addWidget(self.map_cb_grids)
        grid_row1.setAlignment(Qt.AlignLeft)
        v.addLayout(grid_row1)
        grid_row2 = QHBoxLayout()
        grid_row2.addWidget(self.map_cb_states)
        grid_row2.addWidget(self.map_cb_cities)
        grid_row2.setAlignment(Qt.AlignLeft)
        v.addLayout(grid_row2)
        for cb in (
            self.map_cb_callsigns,
            self.map_cb_states,
            self.map_cb_cities,
            self.map_cb_grids,
            self.map_cb_regions,
        ):
            cb.stateChanged.connect(self._on_sidebar_map_filter_changed)

        # Population threshold
        self.map_pop_combo = QComboBox()
        self.map_pop_options = [
            ("1M+", 1_000_000),
            ("750k+", 750_000),
            ("500k+", 500_000),
            ("250k+", 250_000),
            ("100k+", 100_000),
            ("75k+", 75_000),
            ("50k+", 50_000),
            ("25k+", 25_000),
            ("10k+", 10_000),
            ("5k+", 5_000),
            ("<5k", 0),
        ]
        for label, val in self.map_pop_options:
            self.map_pop_combo.addItem(label, val)
        self.map_pop_combo.currentIndexChanged.connect(self._on_sidebar_map_filter_changed)
        pop_row = QHBoxLayout()
        pop_row.addWidget(QLabel("City Pop."))
        pop_row.addWidget(self.map_pop_combo)
        pop_row.addStretch()
        v.addLayout(pop_row)

        v.addSpacing(8)
        v.addWidget(QLabel("Propagation"))
        self.map_cb_prop_overlay = QCheckBox("Propagation Overlay")
        v.addWidget(self.map_cb_prop_overlay)
        history_label = "History (7 days)"
        prop_combo_width = max(120, self.fontMetrics().horizontalAdvance(history_label) + 42)
        prop_label_width = 54

        def _prop_label(text: str) -> QLabel:
            lbl = QLabel(text)
            lbl.setMinimumWidth(prop_label_width)
            lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
            return lbl

        def _style_prop_combo(combo: QComboBox) -> None:
            combo.setMinimumWidth(prop_combo_width)
            combo.setMaximumWidth(prop_combo_width)
            combo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
            try:
                combo.view().setMinimumWidth(prop_combo_width)
            except Exception:
                pass

        mode_row = QHBoxLayout()
        mode_row.addWidget(_prop_label("Mode:"))
        self.map_prop_mode_combo = QComboBox()
        self.map_prop_mode_combo.addItem("Actual", "actual")
        self.map_prop_mode_combo.addItem("Blended", "blended")
        self.map_prop_mode_combo.addItem("Modeled", "model")
        _style_prop_combo(self.map_prop_mode_combo)
        mode_row.addWidget(self.map_prop_mode_combo)
        mode_row.addStretch()
        v.addLayout(mode_row)
        window_row = QHBoxLayout()
        window_row.addWidget(_prop_label("Window:"))
        self.map_prop_window_combo = QComboBox()
        self.map_prop_window_combo.addItem("1h", 1)
        self.map_prop_window_combo.addItem("3h", 3)
        self.map_prop_window_combo.addItem("6h", 6)
        self.map_prop_window_combo.addItem("12h", 12)
        self.map_prop_window_combo.addItem("24h", 24)
        self.map_prop_window_combo.addItem("7 Days", 168)
        _style_prop_combo(self.map_prop_window_combo)
        window_row.addWidget(self.map_prop_window_combo)
        window_row.addStretch()
        v.addLayout(window_row)
        target_type_row = QHBoxLayout()
        target_type_row.addWidget(_prop_label("Target:"))
        self.map_prop_target_type_combo = QComboBox()
        self.map_prop_target_type_combo.addItem("Region", "REGION")
        self.map_prop_target_type_combo.addItem("State", "STATE")
        self.map_prop_target_type_combo.addItem("Operator", "OPERATOR")
        _style_prop_combo(self.map_prop_target_type_combo)
        target_type_row.addWidget(self.map_prop_target_type_combo)
        target_type_row.addStretch()
        v.addLayout(target_type_row)
        target_value_row = QHBoxLayout()
        target_value_row.addWidget(_prop_label("Value:"))
        self.map_prop_target_value_combo = QComboBox()
        self.map_prop_target_value_combo.setEditable(True)
        self.map_prop_target_value_combo.setInsertPolicy(QComboBox.NoInsert)
        self.map_prop_target_value_combo.setDuplicatesEnabled(False)
        _style_prop_combo(self.map_prop_target_value_combo)
        target_value_row.addWidget(self.map_prop_target_value_combo)
        target_value_row.addStretch()
        v.addLayout(target_value_row)
        self.map_prop_badge = QLabel("Best Band: --")
        try:
            theme = resolve_theme(self.settings)
            self.map_prop_badge.setStyleSheet(
                f"font-weight: bold; color: {theme.get('info', theme.get('accent', '#1E88E5'))};"
            )
        except Exception:
            self.map_prop_badge.setStyleSheet("font-weight: bold; color: #1E88E5;")
        v.addWidget(self.map_prop_badge)
        self.map_cb_prop_overlay.stateChanged.connect(self._on_sidebar_prop_changed)
        self.map_prop_mode_combo.currentIndexChanged.connect(self._on_sidebar_prop_mode_changed)
        self.map_prop_window_combo.currentIndexChanged.connect(self._on_sidebar_prop_window_changed)
        self.map_prop_target_type_combo.currentIndexChanged.connect(self._on_sidebar_prop_target_type_changed)
        self.map_prop_target_value_combo.currentTextChanged.connect(self._on_sidebar_prop_target_value_changed)
        self._refresh_map_prop_target_controls()
        v.addStretch()
        self.map_filters_layout.addWidget(box)

    def _sync_map_filters_from_tab(self) -> None:
        """
        Update sidebar controls from current map tab state.
        """
        tab = getattr(self, "stations_map_tab", None)
        if tab is None:
            return
        block = [
            self.map_cb_callsigns,
            self.map_cb_states,
            self.map_cb_cities,
            self.map_cb_grids,
            self.map_cb_regions,
            self.map_cb_prop_overlay,
            self.map_prop_mode_combo,
            self.map_prop_window_combo,
        ]
        for cb in block:
            cb.blockSignals(True)
        self.map_cb_callsigns.setChecked(bool(getattr(tab, "show_callsigns", False)))
        self.map_cb_states.setChecked(bool(getattr(tab, "show_states", False)))
        self.map_cb_cities.setChecked(bool(getattr(tab, "show_cities", False)))
        self.map_cb_grids.setChecked(bool(getattr(tab, "show_grids", False)))
        self.map_cb_regions.setChecked(bool(getattr(tab, "show_regions", False)))
        self.map_cb_prop_overlay.setChecked(bool(getattr(tab, "prop_overlay_enabled", False)))
        mode = getattr(tab, "prop_mode", "blended") or "blended"
        idx = self.map_prop_mode_combo.findData(str(mode).lower())
        if idx >= 0:
            self.map_prop_mode_combo.setCurrentIndex(idx)
        try:
            window_hours = int(getattr(tab, "prop_window_hours", 6))
        except Exception:
            window_hours = 6
        idx = self.map_prop_window_combo.findData(window_hours)
        if idx >= 0:
            self.map_prop_window_combo.setCurrentIndex(idx)
        for cb in block:
            cb.blockSignals(False)
        # Pop combo sync
        try:
            current_min = int(getattr(tab, "city_pop_min", 100000))
        except Exception:
            current_min = 100000
        idx = self.map_pop_combo.findData(current_min)
        if idx < 0:
            idx = 4  # default 100k+
        self.map_pop_combo.blockSignals(True)
        self.map_pop_combo.setCurrentIndex(idx)
        self.map_pop_combo.blockSignals(False)
        self._refresh_map_prop_target_controls()

    def _load_map_prop_operator_callsigns(self) -> list[str]:
        db_path = get_config_dir() / "config" / "freqinout_nets.db"
        if not db_path.exists():
            return []
        out: list[str] = []
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT IFNULL(callsign, '')
                FROM operator_checkins
                ORDER BY callsign COLLATE NOCASE
                """
            )
            for (callsign,) in cur.fetchall():
                cs = (callsign or "").strip().upper()
                if cs and cs not in out:
                    out.append(cs)
            conn.close()
        except Exception as e:
            log.debug("MainWindow: failed to load map propagation operator options: %s", e)
        return out

    def _map_prop_target_options(self, target_type: str) -> list[str]:
        target_type = (target_type or "REGION").strip().upper()
        if target_type == "STATE":
            return [s for s in LOWER48_STATES if s in STATE_CENTERS]
        if target_type == "OPERATOR":
            return self._load_map_prop_operator_callsigns()
        return ["ALL"] + sorted(FEMA_REGIONS.keys())

    def _set_map_prop_target_value_options(self, target_type: str, selected_value: str) -> None:
        target_type = (target_type or "REGION").strip().upper()
        selected_value = (selected_value or "").strip().upper()
        if target_type == "REGION" and selected_value == "NATIONAL":
            selected_value = "ALL"
        options = self._map_prop_target_options(target_type)
        self.map_prop_target_value_combo.blockSignals(True)
        self.map_prop_target_value_combo.clear()
        for value in options:
            self.map_prop_target_value_combo.addItem(value)
        if selected_value:
            idx = self.map_prop_target_value_combo.findText(selected_value, Qt.MatchFixedString)
            if idx >= 0:
                self.map_prop_target_value_combo.setCurrentIndex(idx)
            else:
                self.map_prop_target_value_combo.setEditText(selected_value)
        elif self.map_prop_target_value_combo.count() > 0:
            self.map_prop_target_value_combo.setCurrentIndex(0)
        else:
            self.map_prop_target_value_combo.setEditText("")
        self.map_prop_target_value_combo.setEditable(target_type == "OPERATOR")
        self.map_prop_target_value_combo.blockSignals(False)

    def _refresh_map_prop_target_controls(self) -> None:
        if not hasattr(self, "map_prop_target_type_combo") or not hasattr(self, "map_prop_target_value_combo"):
            return
        self._map_prop_target_syncing = True
        try:
            self.settings.reload()
            target_type = (self.settings.get("prop_target_type", "REGION") or "REGION").strip().upper()
            if target_type not in {"REGION", "STATE", "OPERATOR"}:
                target_type = "REGION"
            target_value = (self.settings.get("prop_target_value", "") or "").strip().upper()
            idx = self.map_prop_target_type_combo.findData(target_type)
            if idx < 0:
                idx = 0
            self.map_prop_target_type_combo.blockSignals(True)
            self.map_prop_target_type_combo.setCurrentIndex(idx)
            self.map_prop_target_type_combo.blockSignals(False)
            self._set_map_prop_target_value_options(target_type, target_value)
            current_value = (self.map_prop_target_value_combo.currentText() or "").strip().upper()
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
            log.debug("MainWindow: failed to sync map propagation target controls: %s", e)
        finally:
            self._map_prop_target_syncing = False

    def _on_sidebar_map_filter_changed(self, _=None) -> None:
        """
        Push sidebar filter changes into the map tab and refresh the map.
        """
        tab = getattr(self, "stations_map_tab", None)
        if tab is None:
            return
        tab.show_callsigns = self.map_cb_callsigns.isChecked()
        tab.show_states = self.map_cb_states.isChecked()
        tab.show_cities = self.map_cb_cities.isChecked()
        tab.show_grids = self.map_cb_grids.isChecked()
        tab.show_grid_labels = tab.show_grids
        tab.show_regions = self.map_cb_regions.isChecked()
        # Pop min
        try:
            pop_val = int(self.map_pop_combo.currentData())
        except Exception:
            pop_val = 100000
        tab.city_pop_min = pop_val
        # Mirror into map tab's own combo for consistency
        if hasattr(tab, "city_pop_combo"):
            try:
                idx = tab.city_pop_combo.findData(pop_val)
                if idx >= 0:
                    tab.city_pop_combo.blockSignals(True)
                    tab.city_pop_combo.setCurrentIndex(idx)
                    tab.city_pop_combo.blockSignals(False)
            except Exception:
                pass
        # Persist and redraw
        if hasattr(tab, "_save_display_preferences"):
            tab._save_display_preferences()
        if hasattr(tab, "_request_map_refresh"):
            tab._request_map_refresh(level="medium", reason="sidebar_layers")

    def _on_sidebar_prop_changed(self, _=None) -> None:
        tab = getattr(self, "stations_map_tab", None)
        if tab is None:
            return
        tab.prop_overlay_enabled = self.map_cb_prop_overlay.isChecked()
        mode = self.map_prop_mode_combo.currentData() if hasattr(self, "map_prop_mode_combo") else None
        if mode:
            tab.prop_mode = str(mode)
        if hasattr(tab, "_save_display_preferences"):
            tab._save_display_preferences()
        if hasattr(tab, "_request_map_refresh"):
            tab._request_map_refresh(level="full", reason="sidebar_prop_overlay")

    def _on_sidebar_prop_mode_changed(self, _=None) -> None:
        tab = getattr(self, "stations_map_tab", None)
        if tab is None:
            return
        mode = self.map_prop_mode_combo.currentData()
        if mode:
            tab.prop_mode = str(mode)
        if hasattr(tab, "_save_display_preferences"):
            tab._save_display_preferences()
        if hasattr(tab, "_request_map_refresh"):
            tab._request_map_refresh(level="full", reason="sidebar_prop_mode")

    def _on_sidebar_prop_window_changed(self, _=None) -> None:
        tab = getattr(self, "stations_map_tab", None)
        if tab is None:
            return
        try:
            hours = int(self.map_prop_window_combo.currentData())
        except Exception:
            hours = 6
        tab.prop_window_hours = hours
        if hasattr(tab, "_save_display_preferences"):
            tab._save_display_preferences()
        if hasattr(tab, "_request_map_refresh"):
            tab._request_map_refresh(level="full", reason="sidebar_prop_window")

    def _on_sidebar_prop_target_type_changed(self, _=None) -> None:
        if self._map_prop_target_syncing:
            return
        target_type = (self.map_prop_target_type_combo.currentData() or "REGION").strip().upper()
        self._map_prop_target_syncing = True
        try:
            self._set_map_prop_target_value_options(target_type, "")
            value = (self.map_prop_target_value_combo.currentText() or "").strip().upper()
            self.settings.set_many(
                {
                    "prop_target_type": target_type,
                    "prop_target_value": value,
                }
            )
        except Exception as e:
            log.debug("MainWindow: propagation target type change failed: %s", e)
        finally:
            self._map_prop_target_syncing = False
        tab = getattr(self, "stations_map_tab", None)
        if tab is not None and hasattr(tab, "_request_map_refresh"):
            tab._request_map_refresh(level="full", reason="sidebar_prop_target_type")

    def _on_sidebar_prop_target_value_changed(self, text: str) -> None:
        if self._map_prop_target_syncing:
            return
        target_type = (self.map_prop_target_type_combo.currentData() or "REGION").strip().upper()
        value = (text or "").strip().upper()
        if target_type == "REGION" and value == "NATIONAL":
            value = "ALL"
        try:
            self.settings.set_many(
                {
                    "prop_target_type": target_type,
                    "prop_target_value": value,
                }
            )
        except Exception as e:
            log.debug("MainWindow: propagation target value change failed: %s", e)
        tab = getattr(self, "stations_map_tab", None)
        if tab is not None and hasattr(tab, "_request_map_refresh"):
            tab._request_map_refresh(level="full", reason="sidebar_prop_target_value")

    def _update_map_filters_visibility(self, index: int) -> None:
        """
        Keep map visibility/lifecycle in sync with the active tab.
        Sidebar layout remains stable while map-specific controls live inside Map.
        """
        is_map = 0 <= index < len(self._screens) and self._screens[index][0] == "Map"
        try:
            if self.stations_map_tab is not None and hasattr(self.stations_map_tab, "set_map_visible"):
                self.stations_map_tab.set_map_visible(is_map)
        except Exception:
            pass
        if hasattr(self, "map_filters_container"):
            self.map_filters_container.setVisible(False)

    def _schedule_feedback_target(self) -> tuple[str | None, str]:
        profile = getattr(self, "_active_runtime_profile", None)
        if isinstance(profile, dict):
            profile_id = profile.get("id")
            label = str(profile.get("name") or profile.get("label") or "").strip()
            return (str(profile_id) if profile_id not in (None, "") else None, label or "Radio")
        return None, "Radio"

    def _publish_schedule_control_feedback(
        self,
        *,
        action_type: str,
        status: str,
        summary: str,
        detail: str = "",
    ) -> None:
        service = getattr(self, "action_feedback_service", None)
        if service is None or not hasattr(service, "publish"):
            log.debug("MainWindow: action feedback service unavailable for schedule control.")
            return
        radio_profile_id, target_label = self._schedule_feedback_target()
        try:
            service.publish(
                scope="scheduler",
                action_type=action_type,
                status=status,
                summary=str(summary or "").strip(),
                radio_profile_id=radio_profile_id,
                target_label=target_label,
                detail=str(detail or "").strip(),
                source_surface="main_window_schedule_control",
            )
        except Exception as e:
            log.debug("MainWindow: failed to publish schedule control feedback: %s", e)

    def _on_resume_schedule_clicked(self) -> None:
        resumed = False
        try:
            if hasattr(self, "scheduler"):
                if hasattr(self.scheduler, "resume_schedule"):
                    resume_schedule_hold(self, self.settings)
                    resumed = True
                else:
                    try:
                        set_suspend_until(self.scheduler.settings, None)
                        self.on_hold_state_changed(force_reload=False)
                    except Exception:
                        pass
                    result = self.scheduler.apply_current_entry(
                        force=True,
                        ignore_wait_prompt=True,
                        ignore_suspend=True,
                        ignore_js8_busy=True,
                        ignore_varac_busy=True,
                        ignore_fldigi_busy=True,
                    )
                    if result is False:
                        self._publish_schedule_control_feedback(
                            action_type="resume_schedule",
                            status="failed",
                            summary="Resume failed: schedule control could not return to plan.",
                            detail="Scheduler reported that the current schedule entry could not be applied.",
                        )
                        return
                    resumed = True
        except Exception as e:
            log.debug("MainWindow: resume schedule action failed: %s", e)
            self._publish_schedule_control_feedback(
                action_type="resume_schedule",
                status="failed",
                summary="Resume failed: schedule control could not return to plan.",
                detail=str(e) or "Schedule control failed while resuming.",
            )
            return
        if not resumed:
            self._publish_schedule_control_feedback(
                action_type="resume_schedule",
                status="blocked",
                summary="Resume blocked: scheduler is unavailable.",
                detail="Schedule control could not resume because the scheduler is unavailable.",
            )
            return
        _radio_id, target_label = self._schedule_feedback_target()
        self._publish_schedule_control_feedback(
            action_type="resume_schedule",
            status="succeeded",
            summary=f"{target_label} returned to schedule.",
            detail="Schedule control resumed for the active radio.",
        )
        try:
            self._refresh_scheduler_status_panel()
        except Exception:
            pass
        try:
            if hasattr(self, "controlfreq_tab") and self.controlfreq_tab is not None:
                self.controlfreq_tab.on_schedule_resumed()
        except Exception:
            pass
        # Follow-up pulses help UI converge quickly while scheduler/radio apply completes.
        for delay_ms in (300, 1100):
            try:
                QTimer.singleShot(delay_ms, self._refresh_scheduler_status_panel)
            except Exception:
                pass

    def _on_suspend_schedule_clicked(self) -> None:
        try:
            if not hasattr(self, "scheduler"):
                self._publish_schedule_control_feedback(
                    action_type="suspend_schedule",
                    status="blocked",
                    summary="Suspend blocked: scheduler is unavailable.",
                    detail="Schedule control could not pause because the scheduler is unavailable.",
                )
                return
            hold_snapshot = suspend_snapshot(self.settings)
            if hold_snapshot.get("active"):
                self._on_resume_schedule_clicked()
                return
            mins = self._selected_sidebar_hold_minutes()
            suspend_schedule_hold(self, self.settings, mins)
            _radio_id, target_label = self._schedule_feedback_target()
            self._publish_schedule_control_feedback(
                action_type="suspend_schedule",
                status="succeeded",
                summary=f"{target_label} suspended for {mins} minutes.",
                detail=f"Schedule control paused for {mins} minutes.",
            )
        except Exception as e:
            log.debug("MainWindow: suspend schedule action failed: %s", e)
            self._publish_schedule_control_feedback(
                action_type="suspend_schedule",
                status="failed",
                summary="Suspend failed: schedule control could not pause.",
                detail=str(e) or "Schedule control failed while suspending.",
            )
            return
        self._refresh_scheduler_status_panel()

    def _selected_sidebar_hold_minutes(self) -> int:
        return selected_hold_duration(
            getattr(self, "suspend_duration_combo", None),
            self.settings,
            getattr(self, "_active_runtime_profile", None),
        )

    def _on_sidebar_hold_duration_changed(self) -> None:
        mins = self._selected_sidebar_hold_minutes()
        set_hold_duration_default(self.settings, mins)
        self.on_hold_duration_default_changed()

    def _hold_state_targets(self) -> list[object]:
        return [
            getattr(self, "controlfreq_tab", None),
            getattr(self, "hf_schedule_tab", None),
            getattr(self, "fldigi_tab", None),
            getattr(self, "js8_tab", None),
        ]

    def _hold_duration_combos(self) -> list[QComboBox]:
        combos: list[QComboBox] = []
        for combo in (
            getattr(self, "suspend_duration_combo", None),
            getattr(getattr(self, "controlfreq_tab", None), "hold_duration_combo", None),
            getattr(getattr(self, "hf_schedule_tab", None), "hold_duration_combo", None),
            getattr(getattr(self, "fldigi_tab", None), "hold_duration_combo", None),
            getattr(getattr(self, "js8_tab", None), "hold_duration_combo", None),
        ):
            if isinstance(combo, QComboBox):
                combos.append(combo)
        return combos

    @staticmethod
    def _hold_snapshot_signature(snapshot: dict[str, object] | None) -> tuple[object, ...]:
        snap = snapshot if isinstance(snapshot, dict) else {}
        return (
            bool(snap.get("active")),
            int(snap.get("remaining_minutes") or 0),
            str(snap.get("severity") or "idle"),
            int(bool(snap.get("about_to_resume"))),
        )

    def _sync_hold_duration_combos(self) -> None:
        for combo in self._hold_duration_combos():
            try:
                if combo.view().isVisible() or combo.hasFocus():
                    continue
            except Exception:
                pass
            try:
                refresh_hold_duration_combo(combo, self.settings, getattr(self, "_active_runtime_profile", None))
            except Exception:
                continue

    def _broadcast_hold_state(self, snapshot: dict[str, object]) -> None:
        for tab in self._hold_state_targets():
            if tab is None or not hasattr(tab, "on_hold_state_changed"):
                continue
            try:
                tab.on_hold_state_changed(snapshot=snapshot)
            except Exception:
                continue

    def _apply_active_hold_status_panel(self, hold_snapshot: dict[str, object]) -> None:
        suspended_until = hold_snapshot.get("until")
        if not isinstance(suspended_until, datetime.datetime):
            return
        local_dt = suspended_until.astimezone()
        remaining_sec = hold_snapshot.get("remaining_sec")
        remaining_min = hold_snapshot.get("remaining_minutes") or 0
        severity_role = active_hold_button_role(remaining_sec)
        self.scheduler_status_header.setText(
            "Resuming Soon" if hold_snapshot.get("about_to_resume") else "Schedule Paused"
        )
        self._set_scheduler_reasons(
            [
                f"Auto resume in {remaining_min} min",
                f"At {local_dt:%Y-%m-%d %H:%M}",
            ]
        )
        self.resume_schedule_btn.setVisible(False)
        self.suspend_duration_label.setVisible(True)
        self.suspend_duration_combo.setVisible(True)
        self.suspend_schedule_btn.setVisible(True)
        self.suspend_schedule_btn.setText(active_hold_button_text(remaining_sec))
        self.suspend_schedule_btn.setToolTip(active_hold_status_text(remaining_sec))
        try:
            theme = resolve_theme(self.settings)
            self.suspend_schedule_btn.setStyleSheet(button_style(severity_role, theme))
            highlight = theme.get("surface_alt", theme.get("surface", "#FFFFFF"))
            border = theme.get(
                "danger" if severity_role == "danger" else "warning",
                theme.get("border", "#CCCCCC"),
            )
            self.scheduler_status_container.setStyleSheet(
                "QGroupBox { background-color: %s; border: 1px solid %s; border-radius: 6px; }"
                "QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 4px; }"
                % (highlight, border)
            )
        except Exception:
            pass
        try:
            self._update_scheduler_action_button_widths()
            self.scheduler_status_container.adjustSize()
        except Exception:
            pass
        self._auto_collapse_inactive_nav_groups()

    def _dispatch_hold_snapshot(
        self,
        snapshot: dict[str, object],
        *,
        force: bool = False,
        sync_combos: bool = False,
    ) -> None:
        previous_snapshot = self._hold_state_snapshot if isinstance(self._hold_state_snapshot, dict) else {}
        was_active = bool(previous_snapshot.get("active"))
        signature = self._hold_snapshot_signature(snapshot)
        signature_changed = force or signature != self._hold_state_signature
        self._hold_state_snapshot = snapshot
        self._hold_state_signature = signature
        if sync_combos:
            self._sync_hold_duration_combos()
        if snapshot.get("active"):
            try:
                if not self._hold_state_timer.isActive():
                    self._hold_state_timer.start()
            except Exception:
                pass
            if signature_changed:
                self._broadcast_hold_state(snapshot)
                self._apply_active_hold_status_panel(snapshot)
            return
        try:
            if self._hold_state_timer.isActive():
                self._hold_state_timer.stop()
        except Exception:
            pass
        if signature_changed or was_active:
            self._broadcast_hold_state(snapshot)
            self._refresh_scheduler_status_panel()

    def on_hold_state_changed(self, force_reload: bool = False) -> None:
        snapshot = suspend_snapshot(self.settings, allow_reload=bool(force_reload))
        if snapshot.get("until") and not snapshot.get("active"):
            resume_schedule_hold(self, self.settings)
            return
        self._dispatch_hold_snapshot(snapshot, force=bool(force_reload))

    def _on_hold_state_tick(self) -> None:
        if not self._ui_refresh_allowed():
            self._mark_ui_refresh_dirty("hold_state_tick")
            return
        snapshot = suspend_snapshot(self.settings, allow_reload=False)
        if snapshot.get("until") and not snapshot.get("active"):
            resume_schedule_hold(self, self.settings)
            return
        self._dispatch_hold_snapshot(snapshot)

    def on_hold_duration_default_changed(self) -> None:
        snapshot = self._hold_state_snapshot if isinstance(self._hold_state_snapshot, dict) else None
        if not isinstance(snapshot, dict):
            snapshot = suspend_snapshot(self.settings)
            if snapshot.get("until") and not snapshot.get("active"):
                resume_schedule_hold(self, self.settings)
                return
        if snapshot.get("active"):
            set_active_hold_duration(
                self,
                self.settings,
                notify=False,
                profile=getattr(self, "_active_runtime_profile", None),
            )
            snapshot = suspend_snapshot(self.settings, allow_reload=False)
        self._dispatch_hold_snapshot(snapshot, force=True, sync_combos=True)

    def _set_scheduler_reasons(self, lines: list[str]) -> None:
        if not hasattr(self, "scheduler_status_reasons_layout"):
            return
        sig = tuple(str(line) for line in (lines or []))
        if sig == self._scheduler_status_reason_lines_signature:
            return
        self._scheduler_status_reason_lines_signature = sig
        layout = self.scheduler_status_reasons_layout
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        for line in sig:
            lbl = QLabel(line)
            lbl.setAlignment(Qt.AlignCenter)
            lbl.setWordWrap(True)
            layout.addWidget(lbl)

    def _refresh_scheduler_status_panel(self, *_args) -> None:
        if not self._ui_refresh_allowed():
            self._mark_ui_refresh_dirty("scheduler_status")
            return
        if not hasattr(self, "scheduler") or not hasattr(self, "scheduler_status_container"):
            return
        if not self.scheduler_status_container.isVisible():
            return
        try:
            status = self.scheduler.get_status_summary()
        except Exception:
            return
        if getattr(self, "suspend_duration_combo", None) is not None:
            try:
                if (
                    not self.suspend_duration_combo.view().isVisible()
                    and not self.suspend_duration_combo.hasFocus()
                ):
                    refresh_hold_duration_combo(
                        self.suspend_duration_combo,
                        self.settings,
                        getattr(self, "_active_runtime_profile", None),
                    )
            except Exception:
                pass
        control_mode = status.get("control_mode")
        use_scheduler = bool(status.get("use_scheduler", True))
        freq_label = status.get("freq_label") or ""
        hold_snapshot = self._hold_state_snapshot if isinstance(self._hold_state_snapshot, dict) else None
        if not isinstance(hold_snapshot, dict):
            hold_snapshot = suspend_snapshot(self.settings)
        if hold_snapshot.get("until") and not hold_snapshot.get("active"):
            try:
                resume_schedule_hold(self, self.settings)
            except Exception:
                pass
            hold_snapshot = suspend_snapshot(self.settings)
        suspended_until = hold_snapshot.get("until") if hold_snapshot.get("active") else None
        off_schedule = bool(status.get("off_schedule"))
        varac_waiting = bool(status.get("varac_waiting"))
        ptt_active = bool(status.get("ptt_active"))
        shared_ptt_blocked = bool(status.get("shared_ptt_blocked"))
        shared_ptt_reason = str(status.get("shared_ptt_reason") or "").strip()
        shared_ptt_group = str(status.get("shared_ptt_group") or "").strip()
        shared_ptt_owner_name = str(status.get("shared_ptt_owner_name") or "").strip()
        rf_conflict_warning = bool(status.get("rf_conflict_warning"))
        rf_conflict_summary = str(status.get("rf_conflict_summary") or "").strip()
        js8_busy = bool(status.get("js8_busy"))
        fldigi_busy = bool(status.get("fldigi_busy"))
        fldigi_busy_reason = (status.get("fldigi_busy_reason") or "").strip().lower()
        varac_busy = bool(status.get("varac_busy"))
        net_kind = status.get("net_kind")
        flags = status.get("off_schedule_flags") or {}
        fldigi_mode_off = bool(status.get("fldigi_mode_off"))
        fldigi_offset_off = bool(status.get("fldigi_offset_off"))
        sop_contention = bool(status.get("sop_contention"))
        sop_profiles = [str(x).strip() for x in (status.get("sop_contention_profiles") or []) if str(x).strip()]
        sop_selected_profile = str(status.get("sop_selected_profile") or "").strip()
        active_source = str(status.get("source") or "").strip().upper()
        next_change_minutes = None
        sop_next_minutes = self._get_next_sop_action_minutes()
        next_change = getattr(self.scheduler, "next_change_utc", None)
        if next_change is not None:
            try:
                if getattr(next_change, "tzinfo", None) is None:
                    next_change = next_change.replace(tzinfo=datetime.timezone.utc)
                else:
                    next_change = next_change.astimezone(datetime.timezone.utc)
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                delta = (next_change - now_utc).total_seconds()
                if delta > 0:
                    next_change_minutes = int((delta + 59) // 60)
            except Exception:
                next_change_minutes = None

        if (control_mode in {"MANUAL", "NONE"}) or not use_scheduler:
            self.scheduler_status_header.setText("Frequency")
            self._set_scheduler_reasons([freq_label or "--"])
            self.resume_schedule_btn.setVisible(False)
            self.suspend_schedule_btn.setVisible(False)
            self.suspend_duration_label.setVisible(False)
            self.suspend_duration_combo.setVisible(False)
            try:
                self.scheduler_status_container.adjustSize()
            except Exception:
                pass
            self._auto_collapse_inactive_nav_groups()
            return

        if suspended_until:
            self._apply_active_hold_status_panel(hold_snapshot)
            return

        reasons = []
        busy_sources = []
        if js8_busy:
            busy_sources.append("JS8")
        if varac_busy:
            busy_sources.append("VarAC")
        if fldigi_busy:
            if fldigi_busy_reason == "gibberish":
                busy_sources.append("FLDigi (gibberish)")
            else:
                busy_sources.append("FLDigi")
        busy_line = f"BUSY RX: {'; '.join(busy_sources)}" if busy_sources else ""

        if off_schedule:
            if flags.get("frequency"):
                reasons.append("Frequency")
            if flags.get("offset"):
                reasons.append("JS8 Offset")
            if flags.get("mode") or flags.get("fldigi_offset"):
                if fldigi_offset_off:
                    reasons.append("FLDigi Offset")
                if fldigi_mode_off:
                    reasons.append("FLDigi Mode")
                if not fldigi_mode_off and not fldigi_offset_off:
                    reasons.append("FLDigi Mode/Offset")
            if varac_waiting:
                reasons.append("Waiting to Clear")
            if ptt_active:
                reasons.append("Sending Traffic")
            if shared_ptt_blocked:
                if shared_ptt_owner_name and shared_ptt_group:
                    reasons.append(f"Shared PTT {shared_ptt_group}: {shared_ptt_owner_name}")
                elif shared_ptt_reason:
                    reasons.append(shared_ptt_reason)
                else:
                    reasons.append("Shared PTT")
            if rf_conflict_warning and rf_conflict_summary:
                reasons.append(rf_conflict_summary)
            if js8_busy or varac_busy:
                reasons.append("QSO")
            if busy_line and not net_kind:
                reasons.append(busy_line)
            if sop_contention and active_source == "SOP":
                contenders = [p for p in sop_profiles if p and p != sop_selected_profile]
                if contenders:
                    reasons.append(f"SOP Contention: {sop_selected_profile or 'Winner'} over {', '.join(contenders[:3])}")
                else:
                    reasons.append("SOP Contention")
            if next_change_minutes is not None and next_change_minutes <= 15:
                reasons.append(f"Freq Change: {next_change_minutes} min")
        else:
            if varac_waiting:
                reasons.append("Waiting to Clear")
            if ptt_active:
                reasons.append("Sending Traffic")
            if shared_ptt_blocked:
                if shared_ptt_owner_name and shared_ptt_group:
                    reasons.append(f"Shared PTT {shared_ptt_group}: {shared_ptt_owner_name}")
                elif shared_ptt_reason:
                    reasons.append(shared_ptt_reason)
                else:
                    reasons.append("Shared PTT")
            if rf_conflict_warning and rf_conflict_summary:
                reasons.append(rf_conflict_summary)
            if js8_busy or varac_busy:
                reasons.append("QSO")
            if net_kind:
                reasons.append(net_kind)
            if busy_line and not net_kind:
                reasons.append(busy_line)
            if sop_contention and active_source == "SOP":
                contenders = [p for p in sop_profiles if p and p != sop_selected_profile]
                if contenders:
                    reasons.append(f"SOP Contention: {sop_selected_profile or 'Winner'} over {', '.join(contenders[:3])}")
                else:
                    reasons.append("SOP Contention")
            if next_change_minutes is not None and next_change_minutes <= 15:
                reasons.append(f"Freq Change: {next_change_minutes} min")

        if off_schedule:
            self.scheduler_status_header.setText("Off Schedule")
            self.scheduler_status_header.setStyleSheet("font-weight: bold; color: #C62828;")
            self._set_scheduler_reasons(reasons or [""])
            self.resume_schedule_btn.setVisible(True)
            self.suspend_duration_label.setVisible(True)
            self.suspend_duration_combo.setVisible(True)
            try:
                theme = resolve_theme(self.settings)
                self.resume_schedule_btn.setStyleSheet(button_style("info", theme))
            except Exception:
                pass
            self.suspend_schedule_btn.setVisible(True)
            self.suspend_schedule_btn.setText("Suspend")
            self.suspend_schedule_btn.setToolTip(
                f"Pause schedule control for {self._selected_sidebar_hold_minutes()} minutes."
            )
            try:
                theme = resolve_theme(self.settings)
                self.suspend_schedule_btn.setStyleSheet(button_style("warning", theme))
            except Exception:
                pass
            try:
                theme = resolve_theme(self.settings)
                highlight = theme.get("surface_alt", theme.get("surface", "#FFFFFF"))
                border = theme.get("warning", theme.get("border", "#CCCCCC"))
                self.scheduler_status_container.setStyleSheet(
                    "QGroupBox { background-color: %s; border: 1px solid %s; border-radius: 6px; }"
                    "QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 4px; }"
                    % (highlight, border)
                )
            except Exception:
                pass
        else:
            if sop_next_minutes is not None and 0 <= sop_next_minutes <= 180:
                hours = sop_next_minutes // 60
                minutes = sop_next_minutes % 60
                self.scheduler_status_header.setText(f"SOP Action in: {hours}:{minutes:02d}")
            else:
                self.scheduler_status_header.setText("On Schedule")
            self.scheduler_status_header.setStyleSheet("")
            reason_lines: list[str] = []
            if active_source == "SOP" and net_kind:
                reason_lines.append(str(net_kind))
            if sop_contention and active_source == "SOP":
                contenders = [p for p in sop_profiles if p and p != sop_selected_profile]
                if contenders:
                    reason_lines.append(
                        f"SOP Contention: {sop_selected_profile or 'Winner'} over {', '.join(contenders[:3])}"
                    )
                else:
                    reason_lines.append("SOP Contention")
            self._set_scheduler_reasons(reason_lines)
            self.resume_schedule_btn.setVisible(False)
            self.suspend_duration_label.setVisible(True)
            self.suspend_duration_combo.setVisible(True)
            self.suspend_schedule_btn.setVisible(True)
            self.suspend_schedule_btn.setText("Suspend")
            self.suspend_schedule_btn.setToolTip(
                f"Pause schedule control for {self._selected_sidebar_hold_minutes()} minutes."
            )
            try:
                theme = resolve_theme(self.settings)
                self.suspend_schedule_btn.setStyleSheet(button_style("warning", theme))
            except Exception:
                pass
            try:
                self.scheduler_status_container.setStyleSheet(
                    "QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 4px; }"
                )
            except Exception:
                pass
        self._update_scheduler_action_button_widths()
        try:
            self.scheduler_status_container.adjustSize()
        except Exception:
            pass
        self._auto_collapse_inactive_nav_groups()

    def _get_next_sop_action_minutes(self):
        try:
            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            # Refresh every 30s to avoid querying SOP DB on every 2s status timer tick.
            if (now - float(self._sop_next_due_cache_ts or 0.0)) < 30:
                return self._sop_next_due_minutes
            self._sop_next_due_cache_ts = now
            self._sop_next_due_minutes = None
            if not hasattr(self, "sop_tab") or not hasattr(self.sop_tab, "manager"):
                return None
            rows = self.sop_tab.manager.build_upcoming_actions(horizon_hours=3, only_active=True)
            if not rows:
                return None
            next_due = rows[0].get("next_due_utc")
            if next_due is None:
                return None
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            delta = (next_due - now_utc).total_seconds()
            mins = max(0, int((delta + 59) // 60))
            self._sop_next_due_minutes = mins
            return mins
        except Exception:
            return None

    def _invalidate_sop_status_cache(self) -> None:
        self._sop_next_due_cache_ts = 0.0
        self._sop_next_due_minutes = None

    def _on_sop_data_changed(self) -> None:
        self._sop_data_refresh_pending = True
        timer = getattr(self, "_sop_data_refresh_timer", None)
        if isinstance(timer, QTimer):
            timer.start()
            return
        self._flush_sop_data_changed()

    def _flush_sop_data_changed(self) -> None:
        if not bool(getattr(self, "_sop_data_refresh_pending", False)):
            return
        self._sop_data_refresh_pending = False
        self._invalidate_sop_status_cache()
        with perf_span("main.sop_data_changed.flush", settings=self.settings, min_ms=8.0):
            try:
                if hasattr(self, "scheduler"):
                    self.scheduler.force_refresh()
            except Exception:
                pass
            try:
                if hasattr(self, "hf_schedule_tab") and hasattr(self.hf_schedule_tab, "on_sop_data_changed"):
                    self.hf_schedule_tab.on_sop_data_changed()
            except Exception:
                pass
            try:
                if hasattr(self, "controlfreq_tab") and hasattr(self.controlfreq_tab, "on_sop_data_changed"):
                    self.controlfreq_tab.on_sop_data_changed()
            except Exception:
                pass
            try:
                if hasattr(self, "net_tab") and hasattr(self.net_tab, "on_sop_data_changed"):
                    self.net_tab.on_sop_data_changed()
            except Exception:
                pass
            try:
                self._refresh_freq_planner_if_loaded()
            except Exception:
                pass
            try:
                self._refresh_scheduler_status_panel()
            except Exception:
                pass

    def _on_app_about_to_quit(self):
        if self._shutting_down:
            return
        self._shutting_down = True
        self._close_transient_shutdown_ui()
        try:
            if hasattr(self, "_ui_watchdog"):
                self._ui_watchdog.stop()
        except Exception:
            pass
        try:
            if hasattr(self, "_sop_data_refresh_timer"):
                self._sop_data_refresh_timer.stop()
        except Exception:
            pass
        try:
            if hasattr(self, "_hold_state_timer"):
                self._hold_state_timer.stop()
        except Exception:
            pass
        try:
            if self.stack.currentWidget() is self.stations_map_tab:
                self.stack.setCurrentWidget(self.settings_tab)
        except Exception:
            pass
        try:
            if hasattr(self, "scheduler"):
                self.scheduler.stop()
        except Exception as e:
            log.debug("MainWindow shutdown: scheduler stop failed: %s", e)
        try:
            if hasattr(self, "background_ingest"):
                self.background_ingest.stop()
        except Exception as e:
            log.debug("MainWindow shutdown: background ingest stop failed: %s", e)
        try:
            if hasattr(self, "launch_orchestrator"):
                self.launch_orchestrator.stop_sequence()
        except Exception as e:
            log.debug("MainWindow shutdown: launch orchestrator stop failed: %s", e)
        try:
            if self._launch_progress_dialog is not None:
                self._launch_progress_dialog.close()
                self._launch_progress_dialog = None
        except Exception:
            pass
        try:
            if self._log_dialog is not None:
                self._log_dialog.close()
                self._log_dialog = None
        except Exception:
            pass
        try:
            if self.log_tab is not None and hasattr(self.log_tab, "set_tab_active"):
                self.log_tab.set_tab_active(False)
        except Exception:
            pass
        try:
            if self._webengine_warmup_widget is not None:
                self._webengine_warmup_widget.deleteLater()
                self._webengine_warmup_widget = None
        except Exception:
            pass
        try:
            if hasattr(self, "js8_control"):
                self.js8_control.stop()
        except Exception as e:
            log.debug("MainWindow shutdown: JS8 control stop failed: %s", e)
        for _label, widget in self._screens:
            try:
                if hasattr(widget, "shutdown"):
                    widget.shutdown()
            except Exception as e:
                log.debug("MainWindow shutdown: widget shutdown failed for %r: %s", widget, e)
                continue
        try:
            JS8RxHub.shutdown_all()
        except Exception as e:
            log.debug("MainWindow shutdown: JS8RxHub shutdown failed: %s", e)
        try:
            if hasattr(self, "station_runtime_manager"):
                self.station_runtime_manager.stop()
        except Exception as e:
            log.debug("MainWindow shutdown: station runtime manager stop failed: %s", e)
        try:
            set_scheduler_enabled_override(None)
        except Exception:
            pass

    def _close_transient_shutdown_ui(self) -> None:
        try:
            self._dismiss_off_schedule_prompt()
            self._dismiss_varac_wait_prompt()
        except Exception:
            pass
        app = QApplication.instance()
        if app is None:
            return
        try:
            popup = app.activePopupWidget()
            if popup is not None:
                popup.close()
        except Exception as e:
            log.debug("MainWindow shutdown: active popup close failed: %s", e)
        try:
            modal = app.activeModalWidget()
            if modal is not None and modal is not self:
                if isinstance(modal, QDialog):
                    modal.reject()
                else:
                    modal.close()
        except Exception as e:
            log.debug("MainWindow shutdown: active modal close failed: %s", e)
        try:
            for widget in list(app.topLevelWidgets()):
                if widget is self or not widget.isVisible():
                    continue
                if isinstance(widget, QDialog):
                    widget.reject()
                elif widget.windowModality() != Qt.NonModal or widget.parent() is not None:
                    widget.close()
        except Exception as e:
            log.debug("MainWindow shutdown: transient widget close failed: %s", e)

    def closeEvent(self, event):
        unsubscribe = getattr(self, "_action_feedback_unsubscribe", None)
        if callable(unsubscribe):
            try:
                unsubscribe()
            except Exception:
                pass
            self._action_feedback_unsubscribe = None
        self._on_app_about_to_quit()
        super().closeEvent(event)

    def resizeEvent(self, event):
        try:
            self._sync_status_box_width()
        except Exception:
            pass
        super().resizeEvent(event)
        try:
            self._auto_collapse_inactive_nav_groups()
        except Exception:
            pass

    def _sync_status_box_width(self) -> None:
        if not hasattr(self, "scheduler_status_container"):
            return
        width = 0
        if hasattr(self, "status_dock_widget"):
            try:
                width = int(self.status_dock_widget.width())
            except Exception:
                width = 0
        if hasattr(self, "nav_buttons") and self.nav_buttons:
            try:
                nav_width = max(btn.width() for btn in self.nav_buttons)
                if width <= 10:
                    width = nav_width
                if width <= 10:
                    width = max(btn.sizeHint().width() for btn in self.nav_buttons)
            except Exception:
                if width <= 10:
                    width = 0
        if width <= 10 and hasattr(self, "nav_widget"):
            try:
                margins = self.nav_widget.layout().contentsMargins()
                width = int(self.nav_widget.width() - margins.left() - margins.right())
            except Exception:
                width = int(self.nav_widget.width())
        if width <= 10 and hasattr(self, "nav_scroll"):
            try:
                width = int(self.nav_scroll.viewport().width())
            except Exception:
                pass
        if width > 0:
            for container in (
                getattr(self, "scheduler_status_container", None),
                getattr(self, "condition_level_container", None),
            ):
                if container is None:
                    continue
                try:
                    container.setFixedWidth(width)
                except Exception:
                    pass

    def _update_ledge_clock(self) -> None:
        if not hasattr(self, "ledge_local_time_label") or not hasattr(self, "ledge_utc_time_label"):
            return
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a")
        utc_text = now_utc.strftime(f"UTC {utc_day} %H:%M:%S Z")
        try:
            tz_name = str(self.settings.get("timezone", "UTC") or "UTC")
            tz = get_timezone(tz_name)
            now_local = now_utc.astimezone(tz)
            local_day = now_local.strftime("%a")
            abbr = now_local.tzname() or tz_name
            local_text = now_local.strftime(f"Local {local_day} %H:%M:%S {abbr}")
        except Exception:
            local_text = "Local --"
        self.ledge_local_time_label.setText(local_text)
        self.ledge_utc_time_label.setText(utc_text)
        self.ledge_local_time_label.setToolTip(local_text)
        self.ledge_utc_time_label.setToolTip(utc_text)

    def _update_scheduler_action_button_widths(self) -> None:
        buttons = [
            getattr(self, "resume_schedule_btn", None),
            getattr(self, "suspend_schedule_btn", None),
            getattr(self, "logs_active_btn", None),
        ]
        valid_buttons = [btn for btn in buttons if btn is not None]
        if not valid_buttons:
            return
        width = 0
        for btn in valid_buttons:
            try:
                hint_w = int(btn.sizeHint().width())
            except Exception:
                hint_w = 0
            try:
                text_w = int(btn.fontMetrics().horizontalAdvance(btn.text()) + 28)
            except Exception:
                text_w = 0
            width = max(width, hint_w, text_w)
        width = max(140, min(width, 220))
        for btn in valid_buttons:
            try:
                btn.setFixedWidth(width)
            except Exception:
                pass

    def _update_nav_layout_metrics(self) -> None:
        if not hasattr(self, "nav_widget") or not getattr(self, "nav_buttons", None):
            return
        content_width = 0
        for btn in self.nav_buttons:
            try:
                hint_w = int(btn.sizeHint().width())
            except Exception:
                hint_w = 0
            try:
                text_w = int(btn.fontMetrics().horizontalAdvance(btn.text()) + 40)
            except Exception:
                text_w = 0
            content_width = max(content_width, hint_w, text_w)
        for header in getattr(self, "_nav_group_headers", {}).values():
            try:
                hint_w = int(header.sizeHint().width())
            except Exception:
                hint_w = 0
            try:
                text_w = int(header.fontMetrics().horizontalAdvance(header.text()) + 48)
            except Exception:
                text_w = 0
            content_width = max(content_width, hint_w, text_w)

        # Accordion child rows are indented; reserve that offset so expanded
        # child buttons do not lose right-edge pixels.
        child_indent_w = 0
        for body_layout in getattr(self, "_nav_group_layouts", {}).values():
            if body_layout is None:
                continue
            try:
                margins = body_layout.contentsMargins()
                child_indent_w = max(child_indent_w, int(margins.left() + margins.right()))
            except Exception:
                continue

        # Status cards should fully fit in the rail without horizontal clipping.
        status_hint_w = 0
        for container in (
            getattr(self, "scheduler_status_container", None),
            getattr(self, "condition_level_container", None),
            getattr(self, "ledge_clock_widget", None),
        ):
            if container is None:
                continue
            try:
                status_hint_w = max(status_hint_w, int(container.sizeHint().width()))
            except Exception:
                continue

        content_width = max(content_width + child_indent_w, status_hint_w)
        content_width = max(150, min(content_width, 300))
        for btn in self.nav_buttons:
            try:
                btn.setMinimumWidth(content_width)
            except Exception:
                pass
        for header in getattr(self, "_nav_group_headers", {}).values():
            try:
                header.setMinimumWidth(content_width)
            except Exception:
                pass
        try:
            layout = self.nav_widget.layout()
            margins = layout.contentsMargins() if layout is not None else None
            margin_w = int((margins.left() + margins.right()) if margins is not None else 8)
        except Exception:
            margin_w = 8

        # Additional reserve avoids clipping from scroll-area internals and borders.
        rail_padding_w = 20
        panel_w = max(180, min(content_width + margin_w + rail_padding_w, 340))
        try:
            self.nav_widget.setMinimumWidth(panel_w)
            self.nav_widget.setMaximumWidth(panel_w)
        except Exception:
            pass
        self._update_scheduler_action_button_widths()
        self._sync_status_box_width()
        self._auto_collapse_inactive_nav_groups()

    def _dismiss_off_schedule_prompt(self) -> None:
        if hasattr(self, "_off_schedule_prompt") and self._off_schedule_prompt is not None:
            try:
                self._off_schedule_prompt.close()
            except Exception:
                pass
            self._off_schedule_prompt = None

    def _dismiss_varac_wait_prompt(self) -> None:
        if hasattr(self, "_varac_wait_prompt") and self._varac_wait_prompt is not None:
            try:
                self._varac_wait_prompt.close()
            except Exception:
                pass
            self._varac_wait_prompt = None

    def _dismiss_coordination_conflict_prompt(self) -> None:
        if hasattr(self, "_coordination_conflict_prompt") and self._coordination_conflict_prompt is not None:
            try:
                self._coordination_conflict_prompt.close()
            except Exception:
                pass
            self._coordination_conflict_prompt = None

    def _build_prompt_hold_duration_combo(self, parent) -> QComboBox:
        combo = QComboBox(parent)
        combo.setToolTip("Temporary schedule hold duration.")
        refresh_hold_duration_combo(combo, self.settings, getattr(self, "_active_runtime_profile", None))
        return combo

    def _attach_prompt_hold_duration_row(self, msg: QMessageBox, combo: QComboBox) -> None:
        try:
            row = QWidget(msg)
            layout = QHBoxLayout(row)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(6)
            layout.addWidget(QLabel("Pause for"))
            layout.addWidget(combo)
            layout.addStretch(1)
            msg.layout().addWidget(row, msg.layout().rowCount(), 0, 1, msg.layout().columnCount())
        except Exception:
            pass

    def _on_off_schedule_detected(self, payload: dict) -> None:
        if self._shutting_down:
            return
        self._dismiss_off_schedule_prompt()
        items = payload.get("items") if isinstance(payload, dict) else None
        items = items if isinstance(items, list) else []
        if not items:
            return
        msg = QMessageBox(self)
        msg.setWindowTitle("Off Schedule")
        if len(items) == 1:
            text = f"{items[0]} Off Schedule"
        elif len(items) == 2:
            text = f"{items[0]} and {items[1]} are Off Schedule"
        else:
            text = f"{', '.join(items[:-1])}, and {items[-1]} are Off Schedule"
        msg.setText(text)
        apply_btn = msg.addButton("Resume Sched.", QMessageBox.AcceptRole)
        ignore_btn = msg.addButton("Skip Once", QMessageBox.RejectRole)
        suspend_btn = msg.addButton("Pause Schedule", QMessageBox.DestructiveRole)
        hold_combo = self._build_prompt_hold_duration_combo(msg)
        self._attach_prompt_hold_duration_row(msg, hold_combo)
        self._off_schedule_prompt = msg
        auto_applied = {"done": False}

        def _auto_apply():
            if auto_applied["done"]:
                return
            auto_applied["done"] = True
            try:
                self.scheduler.resolve_off_schedule("apply", items=items)
            except Exception:
                pass
            try:
                msg.done(0)
            except Exception:
                pass

        timer = QTimer(msg)
        timer.setSingleShot(True)
        timer.timeout.connect(_auto_apply)
        timer.start(120000)

        msg.exec()
        try:
            timer.stop()
        except Exception:
            pass
        if auto_applied["done"]:
            self._off_schedule_prompt = None
            return
        clicked = msg.clickedButton()
        if clicked == apply_btn:
            try:
                self.scheduler.resolve_off_schedule("apply", items=items)
            except Exception:
                pass
        elif clicked == ignore_btn:
            try:
                self.scheduler.resolve_off_schedule("ignore", items=items)
            except Exception:
                pass
        elif clicked == suspend_btn:
            try:
                mins = selected_hold_duration(hold_combo, self.settings, getattr(self, "_active_runtime_profile", None))
                set_hold_duration_default(self.settings, mins)
                self._sync_hold_duration_combos()
                self.scheduler.resolve_off_schedule("suspend", items=items, minutes=mins)
                self.on_hold_state_changed(force_reload=True)
            except Exception:
                pass
        self._off_schedule_prompt = None

    def _on_varac_wait_detected(self, payload: dict) -> None:
        if self._shutting_down:
            return
        self._dismiss_varac_wait_prompt()
        msg = QMessageBox(self)
        msg.setWindowTitle("Frequency Change Pending")
        msg.setText("VarAC is waiting for frequency to clear.\nChange frequency now?")
        apply_btn = msg.addButton("Resume Sched.", QMessageBox.AcceptRole)
        ignore_btn = msg.addButton("Skip Once", QMessageBox.RejectRole)
        suspend_btn = msg.addButton("Pause Schedule", QMessageBox.DestructiveRole)
        hold_combo = self._build_prompt_hold_duration_combo(msg)
        self._attach_prompt_hold_duration_row(msg, hold_combo)
        self._varac_wait_prompt = msg
        msg.exec()
        clicked = msg.clickedButton()
        if clicked == apply_btn:
            try:
                self.scheduler.resolve_varac_wait("apply")
            except Exception:
                pass
        elif clicked == suspend_btn:
            try:
                mins = selected_hold_duration(hold_combo, self.settings, getattr(self, "_active_runtime_profile", None))
                set_hold_duration_default(self.settings, mins)
                self._sync_hold_duration_combos()
                self.scheduler.resolve_varac_wait("suspend", minutes=mins)
                self.on_hold_state_changed(force_reload=True)
            except Exception:
                pass
        else:
            try:
                self.scheduler.resolve_varac_wait("ignore")
            except Exception:
                pass
        self._varac_wait_prompt = None

    def _on_coordination_conflict_detected(self, payload: dict) -> None:
        if self._shutting_down:
            return
        self._dismiss_coordination_conflict_prompt()
        summary = str((payload or {}).get("summary") or "").strip() or "RF conflict detected."
        detail = str((payload or {}).get("detail") or "").strip()
        msg = QMessageBox(self)
        msg.setWindowTitle("RF Conflict Warning")
        msg.setText(summary)
        if detail:
            msg.setInformativeText(detail)
        apply_btn = msg.addButton("Proceed Once", QMessageBox.AcceptRole)
        ignore_btn = msg.addButton("Skip Once", QMessageBox.RejectRole)
        suspend_btn = msg.addButton("Pause Schedule", QMessageBox.DestructiveRole)
        hold_combo = self._build_prompt_hold_duration_combo(msg)
        self._attach_prompt_hold_duration_row(msg, hold_combo)
        self._coordination_conflict_prompt = msg
        msg.exec()
        clicked = msg.clickedButton()
        if clicked == apply_btn:
            try:
                self.scheduler.resolve_coordination_conflict("apply")
            except Exception:
                pass
        elif clicked == suspend_btn:
            try:
                mins = selected_hold_duration(hold_combo, self.settings, getattr(self, "_active_runtime_profile", None))
                set_hold_duration_default(self.settings, mins)
                self._sync_hold_duration_combos()
                self.scheduler.resolve_coordination_conflict("suspend", minutes=mins)
                self.on_hold_state_changed(force_reload=True)
            except Exception:
                pass
        else:
            try:
                self.scheduler.resolve_coordination_conflict("ignore")
            except Exception:
                pass
        self._coordination_conflict_prompt = None

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #

    def _apply_callsign_to_tab_titles(self):
        """
        Append the configured callsign to each navigation label, if available.
        This is a helper so the Settings tab can call back into the main
        window (e.g., after saving a new callsign) by doing:
            self.parent()._apply_callsign_to_tab_titles()
        """
        data = self.settings.all()
        callsign = (data.get("callsign") or "").strip().upper()
        base_labels = getattr(self, "_nav_base_labels", [])
        if not callsign:
            # Reset to base titles if no callsign is set
            for idx, base in enumerate(base_labels):
                if idx < len(self.nav_buttons):
                    self.nav_buttons[idx].setText(base)
            self._update_nav_layout_metrics()
            self._station_health_alert_signature = None
            self._refresh_station_health_alert()
            return

        for idx, base in enumerate(base_labels):
            lbl = f"{base} [{callsign}]"
            if idx < len(self.nav_buttons):
                self.nav_buttons[idx].setText(lbl)
        self._update_nav_layout_metrics()
        self._station_health_alert_signature = None
        self._refresh_station_health_alert()

    def open_help_anchor(self, anchor: str | None, *, title: str | None = None) -> None:
        help_index = next((idx for idx, (label, _) in enumerate(self._screens) if label == "Help"), -1)
        if help_index >= 0:
            self._set_screen(help_index)
        try:
            self.help_tab.open_anchor(anchor)
        except Exception:
            pass
        if title:
            try:
                self.help_tab.setWindowTitle(str(title))
            except Exception:
                pass

    def open_context_help(self, context_key: str | None) -> None:
        context = get_help_context(context_key)
        if self._context_help_dialog is None:
            self._context_help_dialog = ContextHelpDialog(self.settings, self)
            try:
                self._context_help_dialog.finished.connect(self._on_context_help_finished)
            except Exception:
                pass
        try:
            self._context_help_dialog.apply_theme()
        except Exception:
            pass
        self._context_help_dialog.show_help_for(context.key)

    def _on_context_help_finished(self, *_args) -> None:
        self._help_dialog_settle_until = time.time() + 0.35
        log.info("UI_LIFECYCLE|context_help_closed settle_ms=350")

    def open_settings_section(self, health_key: str = "freqinout", radio_id: int | None = None) -> None:
        idx = self._screen_index_by_label.get("Settings", -1)
        if idx < 0:
            return
        self._set_screen(idx)
        if hasattr(self.settings_tab, "focus_section_by_health_key"):
            QTimer.singleShot(
                0,
                lambda key=str(health_key or "freqinout"), ident=radio_id: self.settings_tab.focus_section_by_health_key(
                    key,
                    ident,
                ),
            )

    def _open_station_health_detail(self, device_profile_id: int = 0, scope_name: str = "") -> None:
        try:
            self.station_health_tab.focus_scope(
                device_profile_id=int(device_profile_id or 0),
                scope_name=str(scope_name or "").strip(),
            )
        except Exception:
            pass
        idx = self._screen_index_by_label.get("Station Health", -1)
        if idx >= 0:
            self._set_screen(idx)

    def _style_station_command_bar(self, theme: dict) -> None:
        if not hasattr(self, "station_command_bar"):
            return
        border = theme.get("border", "#CDD6E0")
        surface = theme.get("surface", "#FFFFFF")
        text = theme.get("text", "#222222")
        muted = theme.get("text_muted", "#6A737D")
        self.station_command_bar.setStyleSheet(
            "QFrame#stationCommandBar {"
            f"background: {surface}; border: 1px solid {border}; border-radius: 6px;"
            "}"
        )
        for label in (
            getattr(self, "station_command_now_label", None),
            getattr(self, "station_command_state_label", None),
            getattr(self, "station_command_next_label", None),
        ):
            if label is not None:
                label.setStyleSheet(f"color: {text}; font-weight: 600;")
        for btn in (
            getattr(self, "station_command_qsy_btn", None),
            getattr(self, "station_command_hold_btn", None),
            getattr(self, "station_command_suspend_btn", None),
            getattr(self, "station_command_resume_btn", None),
        ):
            if btn is not None:
                btn.setStyleSheet(button_style("muted", theme) + f" color: {muted};")

    def _apply_app_theme(self):
        app = QApplication.instance()
        try:
            self.settings.reload()
        except Exception:
            pass
        theme = resolve_theme(self.settings)
        ui_text_scale = resolve_ui_text_scale(self.settings)
        apply_app_theme(app, theme, ui_text_scale=ui_text_scale)
        fit_child_combo_boxes(self)
        self._set_logo_pixmap()
        self._update_log_indicator()
        if self._context_help_dialog is not None:
            try:
                self._context_help_dialog.apply_theme()
            except Exception:
                pass
        try:
            if hasattr(self, "condition_levels_edit_btn"):
                self._style_condition_levels_edit_action(theme)
        except Exception:
            pass
        try:
            self._style_ledge_clock(theme)
        except Exception:
            pass
        try:
            self._style_station_command_bar(theme)
        except Exception:
            pass
        if hasattr(self, "map_prop_badge"):
            try:
                self.map_prop_badge.setStyleSheet(
                    f"font-weight: bold; color: {theme.get('info', theme.get('accent', '#1E88E5'))};"
                )
            except Exception:
                pass
        for widget in (
            self.freq_planner_tab,
            self.sop_tab,
            self.hf_schedule_tab,
            self.net_tab,
            self.fldigi_tab,
            self.js8_tab,
            self.message_viewer_tab,
            self.log_tab,
            self.stations_map_tab,
            self.operator_history_tab,
            self.local_operator_tab,
            self.local_ncs_tab,
            self.peer_sched_tab,
            self.settings_tab,
            self.controlfreq_tab,
            self.station_overview_tab,
            self.station_health_tab,
            self.help_tab,
        ):
            if widget is None:
                continue
            if hasattr(widget, "apply_theme"):
                try:
                    widget.apply_theme()
                except Exception:
                    pass
        self._update_ncs_nav_button_styles()
        self._update_nav_layout_metrics()
        self._refresh_condition_level_panel()

    def _placeholder_widget(self, label: str) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.addWidget(QLabel(f"Loading {label}..."))
        self._lazy_placeholders[label] = w
        return w

    def _start_lazy_prewarm(self) -> None:
        if self._shutting_down:
            return
        self._prewarm_next_lazy_tab()

    @staticmethod
    def _truthy_flag(value: object, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        try:
            raw = str(value).strip().lower()
        except Exception:
            return default
        if raw == "":
            return default
        return raw in {"1", "true", "yes", "on"}

    def _should_prewarm_webengine_at_startup(self) -> bool:
        """
        Default to startup WebEngine prewarm on Windows, where the hidden
        startup path measurably improves first Map activation. Other platforms
        stay opt-in unless explicitly overridden in settings.
        """
        default_enabled = sys.platform.startswith("win")
        try:
            raw = self.settings.get("map_webengine_startup_prewarm", None)
        except Exception:
            raw = None
        return self._truthy_flag(raw, default_enabled)

    def _show_tab_loading_notice(self, text: str) -> None:
        try:
            self.statusBar().showMessage(str(text or "Preparing..."), 2500)
        except Exception:
            pass

    def _hide_tab_loading_notice(self) -> None:
        try:
            self.statusBar().clearMessage()
        except Exception:
            pass

    def _restore_nav_selection_to_active_tab(self) -> None:
        try:
            idx = self._active_tab_index
            if idx is None:
                return
            nav_idx = self._nav_screen_index_map.get(idx)
            if nav_idx is None or not (0 <= nav_idx < len(self.nav_buttons)):
                return
            btn = self.nav_buttons[nav_idx]
            if not btn.isChecked():
                btn.setChecked(True)
        except Exception:
            pass

    def _queue_map_switch_after_webengine_warmup(self, index: int) -> bool:
        """
        On Windows, keep the current tab visible for the one-time WebEngine
        warmup so the first Map navigation does not visibly coincide with the
        helper-process startup path.
        """
        if not sys.platform.startswith("win"):
            return False
        if self._shutting_down or self._webengine_warmup_done:
            return False
        self._pending_map_switch_index = index
        # Map button click may check itself before we actually switch pages.
        self._restore_nav_selection_to_active_tab()
        if self._webengine_warmup_widget is None:
            self._prewarm_webengine()
            if self._webengine_warmup_done:
                self._pending_map_switch_index = None
                return False
            if self._webengine_warmup_widget is None:
                # Warmup unavailable (e.g., Qt WebEngine missing): proceed directly.
                self._pending_map_switch_index = None
                return False
            log.info("MainWindow: deferring first Map switch until WebEngine warmup completes")
        return True

    def _complete_pending_map_switch_after_webengine_warmup(self) -> None:
        if self._shutting_down:
            self._pending_map_switch_index = None
            return
        if not self._webengine_warmup_done:
            return
        idx = self._pending_map_switch_index
        if idx is None:
            return
        self._pending_map_switch_index = None
        try:
            if hasattr(self, "stations_map_tab") and self.stations_map_tab is not None:
                if hasattr(self.stations_map_tab, "prepare_webview_for_first_show"):
                    self.stations_map_tab.prepare_webview_for_first_show()
        except Exception as e:
            log.debug("MainWindow: hidden Map webview precreate failed: %s", e)
        QTimer.singleShot(0, lambda i=idx: self._set_screen(i))

    def _prewarm_webengine(self) -> None:
        """
        Warm up Qt WebEngine process/components and native view startup early so
        first Map activation avoids the one-time close/reopen-style visual glitch
        on some Windows systems.
        """
        if self._shutting_down:
            return
        if self._webengine_warmup_done:
            return
        if self._webengine_warmup_widget is not None:
            return
        try:
            from PySide6.QtWebEngineWidgets import QWebEngineView
        except Exception:
            return
        try:
            web = QWebEngineView(self)
            web.resize(4, 4)
            self._webengine_warmup_widget = web
            try:
                # Force an offscreen show once so WebEngine native surface/process
                # startup does not occur during first visible Map activation.
                web.setAttribute(Qt.WA_DontShowOnScreen, True)
            except Exception:
                pass

            def _cleanup() -> None:
                try:
                    if self._webengine_warmup_widget is web:
                        self._webengine_warmup_widget = None
                    self._webengine_warmup_done = True
                    try:
                        web.hide()
                    except Exception:
                        pass
                    web.deleteLater()
                    QTimer.singleShot(0, self._complete_pending_map_switch_after_webengine_warmup)
                except Exception:
                    pass

            try:
                web.loadFinished.connect(lambda _ok: _cleanup())
            except Exception:
                pass
            try:
                web.show()
            except Exception:
                pass
            web.setUrl(QUrl("about:blank"))
            QTimer.singleShot(3000, _cleanup)
        except Exception as e:
            log.debug("MainWindow: WebEngine warmup (hidden-view) skipped: %s", e)
            self._webengine_warmup_widget = None

    def _prewarm_next_lazy_tab(self) -> None:
        if self._shutting_down:
            return
        if self._lazy_prewarm_index >= len(self._lazy_prewarm_labels):
            return
        label = self._lazy_prewarm_labels[self._lazy_prewarm_index]
        self._lazy_prewarm_index += 1
        try:
            idx = next((i for i, (name, _w) in enumerate(self._screens) if name == label), -1)
            if idx >= 0:
                self._ensure_lazy_tab_loaded(label, idx)
        except Exception:
            pass
        QTimer.singleShot(1500, self._prewarm_next_lazy_tab)

    def _create_freq_planner_tab(self) -> QWidget:
        with perf_span(
            "main_window.create_freq_planner_tab",
            settings=self.settings,
            min_ms=5.0,
        ):
            self.freq_planner_tab = FreqPlannerTab(self, plan_context_service=self.plan_context_service)
            try:
                self.settings_tab.settings_saved.connect(self.freq_planner_tab.on_settings_saved)
            except Exception:
                pass
            return self.freq_planner_tab

    def _create_message_viewer_tab(self) -> QWidget:
        with perf_span(
            "main_window.create_message_viewer_tab",
            settings=self.settings,
            min_ms=5.0,
        ):
            self.message_viewer_tab = MessageViewerTab(self, plan_context_service=self.plan_context_service)
            try:
                self.settings_tab.settings_saved.connect(self.message_viewer_tab.on_settings_saved)
            except Exception:
                pass
            return self.message_viewer_tab

    def _create_stations_map_tab(self) -> QWidget:
        with perf_span(
            "main_window.create_stations_map_tab",
            settings=self.settings,
            min_ms=5.0,
        ):
            self.stations_map_tab = StationsMapTab(self, plan_context_service=self.plan_context_service)
            return self.stations_map_tab

    def _ensure_lazy_tab_loaded(self, label: str, index: int) -> None:
        with perf_span(
            "main_window.ensure_lazy_tab_loaded",
            settings=self.settings,
            meta={"label": label, "index": index},
            min_ms=5.0,
        ):
            if label not in self._lazy_factories:
                return
            existing = self._get_tab_by_label(label)
            if existing is not None and existing is not self._lazy_placeholders.get(label):
                return
            factory = self._lazy_factories[label]
            new_widget = factory()
            try:
                if hasattr(new_widget, "apply_theme"):
                    new_widget.apply_theme()
            except Exception:
                pass
            placeholder = self._lazy_placeholders.get(label)
            if placeholder is not None:
                self.stack.removeWidget(placeholder)
            self.stack.insertWidget(index, new_widget)
            self._screens[index] = (label, new_widget)

    def _get_tab_by_label(self, label: str) -> QWidget | None:
        for name, widget in self._screens:
            if name == label:
                return widget
        return None

    def _queue_condition_levels_changed(self) -> None:
        self._condition_levels_refresh_pending = True
        timer = getattr(self, "_condition_levels_refresh_timer", None)
        if isinstance(timer, QTimer):
            timer.start()
            return
        # Fallback path if timer initialization failed for any reason.
        self._apply_condition_levels_changed()

    def notify_condition_levels_changed(self) -> None:
        self._queue_condition_levels_changed()

    def _apply_condition_levels_changed(self) -> None:
        if not bool(getattr(self, "_condition_levels_refresh_pending", False)):
            return
        if not self._ui_refresh_allowed():
            self._mark_ui_refresh_dirty("condition_levels_changed")
            return
        self._condition_levels_refresh_pending = False
        try:
            self.settings.reload()
        except Exception:
            pass
        try:
            self._refresh_condition_level_panel()
        except Exception:
            pass
        try:
            self._invalidate_sop_status_cache()
        except Exception:
            pass
        try:
            if hasattr(self, "scheduler"):
                self.scheduler.force_refresh()
        except Exception:
            pass
        try:
            if hasattr(self, "sop_tab") and self.sop_tab is not None:
                if hasattr(self.sop_tab, "on_condition_levels_changed"):
                    self.sop_tab.on_condition_levels_changed()
                elif hasattr(self.sop_tab, "on_settings_saved"):
                    self.sop_tab.on_settings_saved()
        except Exception:
            pass
        try:
            if hasattr(self, "controlfreq_tab") and self.controlfreq_tab is not None:
                if hasattr(self.controlfreq_tab, "on_condition_levels_changed"):
                    self.controlfreq_tab.on_condition_levels_changed()
                elif hasattr(self.controlfreq_tab, "on_sop_data_changed"):
                    self.controlfreq_tab.on_sop_data_changed()
        except Exception:
            pass
        try:
            if self.freq_planner_tab is not None:
                if hasattr(self.freq_planner_tab, "on_condition_levels_changed"):
                    self.freq_planner_tab.on_condition_levels_changed()
                elif self.freq_planner_tab.isVisible() and hasattr(self.freq_planner_tab, "on_settings_saved"):
                    self.freq_planner_tab.on_settings_saved()
        except Exception:
            pass
        try:
            self._refresh_scheduler_status_panel()
        except Exception:
            pass

    def _on_settings_saved_for_lazy_tabs(self) -> None:
        self._refresh_plan_context_labels("settings_saved")
        try:
            if self.freq_planner_tab is not None:
                self.freq_planner_tab.on_settings_saved()
        except Exception:
            pass
        try:
            if self.message_viewer_tab is not None:
                self.message_viewer_tab.on_settings_saved()
        except Exception:
            pass
        try:
            if self.controlfreq_tab is not None:
                self.controlfreq_tab.on_settings_saved()
        except Exception:
            pass
        try:
            self._refresh_condition_level_panel()
        except Exception:
            pass
        try:
            self._refresh_map_prop_target_controls()
        except Exception:
            pass

    def _plan_context_consumer_widgets(self) -> tuple[object | None, ...]:
        return (
            getattr(self, "hf_schedule_tab", None),
            getattr(self, "net_tab", None),
            getattr(self, "freq_planner_tab", None),
            getattr(self, "sop_tab", None),
            getattr(self, "message_viewer_tab", None),
            getattr(self, "controlfreq_tab", None),
            getattr(self, "stations_map_tab", None),
        )

    def _refresh_plan_context_labels(self, reason: str = "") -> None:
        try:
            self.plan_context_service.invalidate()
        except Exception as e:
            log.debug("MainWindow: plan context invalidation failed for %s: %s", reason or "refresh", e)
        for widget in self._plan_context_consumer_widgets():
            label = getattr(widget, "plan_context_label", None)
            if label is None or not hasattr(label, "refresh_context"):
                continue
            try:
                label.refresh_context(refresh=True)
            except Exception as e:
                log.debug("MainWindow: plan context label refresh failed for %s: %s", reason or "refresh", e)

    def _load_runtime_active_device_profile(self) -> dict[str, object]:
        manager = getattr(self, "station_runtime_manager", None)
        if manager is not None and hasattr(manager, "get_runtime_primary_device_profile"):
            try:
                profile = manager.get_runtime_primary_device_profile()
            except Exception as e:
                log.debug("MainWindow: failed to load runtime-primary device profile: %s", e)
                profile = None
            if isinstance(profile, dict):
                return dict(profile)
        try:
            profile = self.multi_radio_store.get_runtime_primary_device_profile()
        except Exception as e:
            log.debug("MainWindow: failed to load runtime-primary device profile from store: %s", e)
            return {}
        return dict(profile) if isinstance(profile, dict) else {}

    @staticmethod
    def _runtime_profile_deployment_mode(profile: object) -> str:
        if not isinstance(profile, dict):
            return "full"
        mode = str(profile.get("deployment_mode", "full") or "full").strip().lower()
        return mode if mode in {"full", "minimal"} else "full"

    @staticmethod
    def _runtime_policy_enabled(policy: object, key: str, default: bool = True) -> bool:
        if not isinstance(policy, dict):
            return bool(default)
        value = policy.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return int(value) != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(default)

    @staticmethod
    def _runtime_state_signature_for(profile: object, policy: object) -> tuple[object, ...]:
        if not isinstance(profile, dict):
            profile = {}
        if not isinstance(policy, dict):
            policy = {}
        return (
            int(profile.get("id", 0) or 0),
            str(profile.get("name", "") or "").strip(),
            str(profile.get("control_backend", "") or "").strip().lower(),
            MainWindow._runtime_profile_deployment_mode(profile),
            str(policy.get("operating_profile_name", "") or "").strip(),
            str(policy.get("assignment_state", "") or "").strip().lower(),
            MainWindow._runtime_policy_enabled(policy, "scheduler_enabled", True),
            str(policy.get("scheduler_mode", "full") or "full").strip().lower(),
            MainWindow._runtime_policy_enabled(policy, "use_messages", True),
            MainWindow._runtime_policy_enabled(policy, "use_map", True),
            MainWindow._runtime_policy_enabled(policy, "use_background_ingest", True),
            MainWindow._runtime_policy_enabled(policy, "use_launch_control", True),
            MainWindow._runtime_policy_enabled(policy, "use_net_control_tabs", True),
            bool(policy.get("swap_active", False)),
            str(policy.get("swap_mode", "") or "").strip().lower(),
            str(policy.get("swap_summary", "") or "").strip(),
            str(profile.get("freq_enforcement_mode", "") or "").strip(),
            str(profile.get("freq_prompt_interval", "") or "").strip(),
            str(profile.get("fldigi_enforcement_mode", "") or "").strip(),
            str(profile.get("fldigi_prompt_interval", "") or "").strip(),
            str(profile.get("js8_enforcement_mode", "") or "").strip(),
            str(profile.get("js8_prompt_interval", "") or "").strip(),
            int(profile.get("schedule_hold_minutes_default", 0) or 0),
        )

    @staticmethod
    def _runtime_timer_policy_for(profile: object) -> dict[str, object]:
        if not isinstance(profile, dict):
            return {}
        keys = (
            "freq_enforcement_mode",
            "freq_prompt_interval",
            "fldigi_enforcement_mode",
            "fldigi_prompt_interval",
            "js8_enforcement_mode",
            "js8_prompt_interval",
        )
        return {key: profile.get(key) for key in keys if str(profile.get(key, "") or "").strip()}

    def _primary_runtime_policy(self) -> dict[str, object]:
        manager = getattr(self, "station_runtime_manager", None)
        policy: object = {}
        if manager is not None and hasattr(manager, "primary_runtime_policy"):
            try:
                policy = manager.primary_runtime_policy()
            except Exception:
                policy = {}
        data = dict(policy) if isinstance(policy, dict) else {}
        return {
            "operating_profile_name": str(data.get("operating_profile_name", "") or "").strip(),
            "assignment_state": str(data.get("assignment_state", "unassigned") or "unassigned").strip().lower(),
            "scheduler_enabled": self._runtime_policy_enabled(data, "scheduler_enabled", True),
            "scheduler_mode": str(data.get("scheduler_mode", "full") or "full").strip().lower() or "full",
            "use_messages": self._runtime_policy_enabled(data, "use_messages", True),
            "use_map": self._runtime_policy_enabled(data, "use_map", True),
            "use_background_ingest": self._runtime_policy_enabled(data, "use_background_ingest", True),
            "use_launch_control": self._runtime_policy_enabled(data, "use_launch_control", True),
            "use_net_control_tabs": self._runtime_policy_enabled(data, "use_net_control_tabs", True),
            "swap_active": bool(data.get("swap_active", False)),
            "swap_mode": str(data.get("swap_mode", "") or "").strip().lower(),
            "swap_summary": str(data.get("swap_summary", "") or "").strip(),
            "swap_source_name": str(data.get("swap_source_name", "") or "").strip(),
            "swap_target_name": str(data.get("swap_target_name", "") or "").strip(),
        }

    @staticmethod
    def _suppressed_screens_for_runtime(profile: object, policy: object) -> set[str]:
        suppressed: set[str] = set()
        if MainWindow._runtime_profile_deployment_mode(profile) == "minimal":
            suppressed.update({"Map", "Messages", "FreqPlanner"})
        if not MainWindow._runtime_policy_enabled(policy, "use_map", True):
            suppressed.add("Map")
        if not MainWindow._runtime_policy_enabled(policy, "use_messages", True):
            suppressed.add("Messages")
        if not MainWindow._runtime_policy_enabled(policy, "use_net_control_tabs", True):
            suppressed.update({"NCS-FLDigi/SSB", "NCS-JS8", "NCS-Local"})
        return suppressed

    @staticmethod
    def _runtime_background_ingest_enabled(profile: object, policy: object) -> bool:
        if MainWindow._runtime_profile_deployment_mode(profile) == "minimal":
            return False
        return MainWindow._runtime_policy_enabled(policy, "use_background_ingest", True)

    @staticmethod
    def _runtime_launch_enabled(profile: object, policy: object) -> bool:
        if MainWindow._runtime_profile_deployment_mode(profile) == "minimal":
            return False
        return MainWindow._runtime_policy_enabled(policy, "use_launch_control", True)

    @staticmethod
    def _runtime_lazy_prewarm_labels(suppressed_labels: set[str]) -> list[str]:
        return [label for label in ("Messages", "FreqPlanner") if label not in suppressed_labels]

    @staticmethod
    def _runtime_banner_text(profile: object, policy: object) -> str:
        profile_name = str(profile.get("name", "") or "").strip() if isinstance(profile, dict) else ""
        backend = str(profile.get("control_backend", "") or "").strip().upper() if isinstance(profile, dict) else ""
        operating_name = str(policy.get("operating_profile_name", "") or "").strip() if isinstance(policy, dict) else ""
        assignment_state = str(policy.get("assignment_state", "") or "").strip().lower() if isinstance(policy, dict) else ""
        swap_summary = str(policy.get("swap_summary", "") or "").strip() if isinstance(policy, dict) else ""
        profile_label = profile_name or "Primary device"
        backend_txt = f" via {backend}" if backend else ""
        if MainWindow._runtime_profile_deployment_mode(profile) == "minimal":
            return (
                f"{profile_label}{backend_txt} is running in Minimal mode. "
                "Map, Messages, FreqPlanner, startup launch, and background ingest are suppressed."
            )

        restrictions: list[str] = []
        if not MainWindow._runtime_policy_enabled(policy, "scheduler_enabled", True):
            restrictions.append("scheduler automation off")
        if not MainWindow._runtime_policy_enabled(policy, "use_map", True):
            restrictions.append("Map hidden")
        if not MainWindow._runtime_policy_enabled(policy, "use_messages", True):
            restrictions.append("Messages hidden")
        if not MainWindow._runtime_policy_enabled(policy, "use_net_control_tabs", True):
            restrictions.append("net control tabs hidden")
        if not MainWindow._runtime_policy_enabled(policy, "use_background_ingest", True):
            restrictions.append("background ingest off")
        if not MainWindow._runtime_policy_enabled(policy, "use_launch_control", True):
            restrictions.append("launch control off")
        if not restrictions:
            return swap_summary
        operating_txt = operating_name or "assigned frequency plan"
        state_txt = "temporary override" if assignment_state == "temporary_override" else "active policy"
        detail = f"{profile_label}{backend_txt} is running under {operating_txt} ({state_txt}): {'; '.join(restrictions)}."
        if swap_summary:
            return f"{swap_summary} {detail}"
        return detail

    def _style_runtime_mode_banner(self, theme: dict) -> None:
        if not hasattr(self, "runtime_mode_banner") or not hasattr(self, "runtime_mode_label"):
            return
        border = theme.get("warning", theme.get("accent", "#d97706"))
        surface = theme.get("surface_alt", theme.get("surface", "#f4f4f5"))
        text = theme.get("text", "#222222")
        try:
            self.runtime_mode_banner.setStyleSheet(
                f"QFrame {{ border: 1px solid {border}; border-radius: 6px; background: {surface}; }}"
            )
            self.runtime_mode_label.setStyleSheet(f"color: {text}; font-weight: 600;")
        except Exception:
            pass

    def _set_nav_visibility_for_screen(self, screen_label: str, visible: bool) -> None:
        screen_idx = self._screen_index_by_label.get(str(screen_label or "").strip())
        if screen_idx is None:
            return
        nav_idx = self._nav_screen_index_map.get(screen_idx)
        if nav_idx is None or not (0 <= nav_idx < len(self.nav_buttons)):
            return
        try:
            self.nav_buttons[nav_idx].setVisible(bool(visible))
        except Exception:
            pass

    def _runtime_fallback_screen_index(self) -> int:
        for label in ("ControlFreq", "Settings"):
            idx = self._screen_index_by_label.get(label)
            if idx is not None:
                return int(idx)
        return 0

    def _screen_is_runtime_suppressed(self, screen_label: str) -> bool:
        return str(screen_label or "").strip() in self._suppressed_screen_labels

    def _apply_runtime_profile_state(self, *, force: bool = False) -> None:
        profile = self._load_runtime_active_device_profile()
        policy = self._primary_runtime_policy()
        signature = self._runtime_state_signature_for(profile, policy)
        if not force and signature == self._runtime_profile_signature:
            return
        self._active_runtime_profile = profile
        self._active_runtime_policy = policy
        self._runtime_profile_signature = signature
        self._suppressed_screen_labels = self._suppressed_screens_for_runtime(profile, policy)
        for label in ("Map", "Messages", "FreqPlanner", "NCS-FLDigi/SSB", "NCS-JS8", "NCS-Local"):
            self._set_nav_visibility_for_screen(label, label not in self._suppressed_screen_labels)
        self._launch_startup_suppressed = not self._runtime_launch_enabled(profile, policy)
        try:
            if hasattr(self.launch_orchestrator, "set_runtime_launch_enabled"):
                self.launch_orchestrator.set_runtime_launch_enabled(
                    not self._launch_startup_suppressed,
                    reason="Launch Control is disabled by the primary frequency plan.",
                )
        except Exception:
            pass
        if self._launch_startup_suppressed:
            try:
                self.launch_orchestrator.stop_sequence()
            except Exception:
                pass
        try:
            if hasattr(self, "settings_tab") and hasattr(self.settings_tab, "_update_launch_control_buttons"):
                self.settings_tab._update_launch_control_buttons()
        except Exception:
            pass
        try:
            if hasattr(self, "scheduler") and self.scheduler is not None and hasattr(self.scheduler, "set_runtime_scheduler_enabled"):
                self.scheduler.set_runtime_scheduler_enabled(bool(policy.get("scheduler_enabled", True)))
        except Exception:
            pass
        try:
            if hasattr(self, "scheduler") and self.scheduler is not None and hasattr(self.scheduler, "set_runtime_timer_policy"):
                self.scheduler.set_runtime_timer_policy(self._runtime_timer_policy_for(profile))
        except Exception as e:
            log.debug("MainWindow: failed to apply runtime timer policy: %s", e)
        try:
            self._sync_hold_duration_combos()
        except Exception:
            pass
        try:
            set_scheduler_enabled_override(bool(policy.get("scheduler_enabled", True)))
        except Exception:
            pass
        self._lazy_prewarm_labels = self._runtime_lazy_prewarm_labels(self._suppressed_screen_labels)
        if not self._runtime_background_ingest_enabled(profile, policy):
            if hasattr(self, "background_ingest") and self.background_ingest is not None:
                try:
                    self.background_ingest.stop()
                except Exception:
                    pass
        else:
            if hasattr(self, "background_ingest") and self.background_ingest is not None:
                try:
                    if hasattr(self.background_ingest, "is_running"):
                        if not self.background_ingest.is_running():
                            self.background_ingest.start()
                    else:
                        self.background_ingest.start()
                except Exception:
                    pass
            if (not self._webengine_warmup_done) and ("Map" not in self._suppressed_screen_labels) and self._should_prewarm_webengine_at_startup():
                try:
                    self._prewarm_webengine()
                except Exception:
                    pass
            QTimer.singleShot(0, self._start_lazy_prewarm)

        banner_text = self._runtime_banner_text(profile, policy)
        if hasattr(self, "runtime_mode_label"):
            self.runtime_mode_label.setText(banner_text)
        if hasattr(self, "runtime_mode_banner"):
            self.runtime_mode_banner.setVisible(bool(banner_text))
        try:
            self._style_runtime_mode_banner(resolve_theme(self.settings))
        except Exception:
            pass
        try:
            current_index = self.stack.currentIndex() if hasattr(self, "stack") else -1
            if 0 <= current_index < len(self._screens):
                current_label = self._screens[current_index][0]
                if self._screen_is_runtime_suppressed(current_label):
                    self._set_screen(self._runtime_fallback_screen_index())
        except Exception:
            pass
        try:
            self._update_nav_layout_metrics()
        except Exception:
            pass
        self._sync_settings_runtime_status()

    def _refresh_station_overview(self, *, force: bool = False) -> None:
        if not self._ui_refresh_allowed():
            self._mark_ui_refresh_dirty("station_overview")
            return
        try:
            if hasattr(self, "station_overview_tab") and self.station_overview_tab is not None:
                self.station_overview_tab.refresh_from_manager(force=force)
        except Exception:
            pass
        self._refresh_station_command_bar(force=False)

    @staticmethod
    def _station_command_snapshot_name(snapshot: object) -> str:
        try:
            name = str(getattr(snapshot, "name", "") or "").strip()
            if name:
                return name
            ident = int(getattr(snapshot, "device_profile_id", 0) or 0)
            return f"Radio {ident}" if ident > 0 else "Radio"
        except Exception:
            return "Radio"

    @staticmethod
    def _station_command_snapshot_id(snapshot: object) -> int:
        try:
            return int(getattr(snapshot, "device_profile_id", 0) or 0)
        except Exception:
            return 0

    @staticmethod
    def _station_command_frequency_text(snapshot: object) -> str:
        try:
            parts = [
                str(getattr(snapshot, "current_frequency_label", "") or "").strip(),
                str(getattr(snapshot, "current_band", "") or "").strip(),
            ]
            text = " ".join(part for part in parts if part).strip()
            return text or "unavailable"
        except Exception:
            return "unavailable"

    @staticmethod
    def _station_command_state_text(snapshot: object) -> str:
        try:
            if bool(getattr(snapshot, "ptt_active", False)) or bool(getattr(snapshot, "shared_ptt_blocked", False)):
                return "Busy: PTT"
            if str(getattr(snapshot, "device_class", "") or "").strip().lower() == "observer":
                return "Monitor"
            if not bool(getattr(snapshot, "scheduler_enabled", True)):
                return "Scheduler Off"
            summary = str(getattr(snapshot, "status_summary", "") or "").strip()
            if summary:
                return summary
            state = str(getattr(snapshot, "overall_state", "") or "").strip()
            return state.title() if state else "On Schedule"
        except Exception:
            return "unknown"

    @staticmethod
    def _station_command_next_text(snapshot: object) -> str:
        try:
            if str(getattr(snapshot, "device_class", "") or "").strip().lower() == "observer":
                return str(getattr(snapshot, "observer_follow_summary", "") or "").strip() or "Receive-only monitor"
            plan = str(getattr(snapshot, "assigned_operating_profile_name", "") or "").strip()
            if not plan:
                return "No assigned plan"
            if not bool(getattr(snapshot, "scheduler_enabled", True)):
                return "Scheduler disabled"
            return f"Plan: {plan}"
        except Exception:
            return "none"

    def _station_command_selected_snapshot(self, snapshots: list[object]) -> object | None:
        selected_id = getattr(self, "_station_command_selected_profile_id", None)
        if selected_id not in (None, 0):
            for snapshot in snapshots:
                if self._station_command_snapshot_id(snapshot) == int(selected_id):
                    return snapshot
        primary = next((snapshot for snapshot in snapshots if bool(getattr(snapshot, "runtime_primary", False))), None)
        if primary is not None:
            return primary
        return snapshots[0] if snapshots else None

    def _refresh_station_command_bar(self, *, force: bool = False) -> None:
        if not hasattr(self, "station_command_radio_combo"):
            return
        manager = getattr(self, "station_runtime_manager", None)
        snapshots: list[object] = []
        if manager is not None:
            try:
                snapshots = list(manager.get_runtime_snapshots(force=force))
            except Exception:
                snapshots = []
        selected = self._station_command_selected_snapshot(snapshots)
        selected_id = self._station_command_snapshot_id(selected) if selected is not None else 0
        self._station_command_bar_loading = True
        combo = self.station_command_radio_combo
        previous_block = combo.blockSignals(True)
        try:
            combo.clear()
            for snapshot in snapshots:
                ident = self._station_command_snapshot_id(snapshot)
                role = "SDR" if str(getattr(snapshot, "device_class", "") or "").strip().lower() == "observer" else "HF"
                combo.addItem(f"{self._station_command_snapshot_name(snapshot)} ({role})", ident)
            if combo.count() <= 0:
                combo.addItem("No active radios", 0)
            for index in range(combo.count()):
                try:
                    if int(combo.itemData(index) or 0) == int(selected_id):
                        combo.setCurrentIndex(index)
                        break
                except Exception:
                    continue
        finally:
            combo.blockSignals(previous_block)
            self._station_command_bar_loading = False

        if selected is not None and selected_id > 0:
            self._station_command_selected_profile_id = int(selected_id)
            self.station_command_now_label.setText(f"Now: {self._station_command_frequency_text(selected)}")
            self.station_command_state_label.setText(f"State: {self._station_command_state_text(selected)}")
            self.station_command_next_label.setText(f"Next: {self._station_command_next_text(selected)}")
            target_name = self._station_command_snapshot_name(selected)
            tooltip = f"Command target: {target_name}. Command wiring is not enabled yet."
        else:
            self._station_command_selected_profile_id = None
            self.station_command_now_label.setText("Now: unavailable")
            self.station_command_state_label.setText("State: no active radio")
            self.station_command_next_label.setText("Next: none")
            tooltip = "No active radio is available for station commands."
        for btn in (
            self.station_command_qsy_btn,
            self.station_command_hold_btn,
            self.station_command_suspend_btn,
            self.station_command_resume_btn,
        ):
            btn.setEnabled(False)
            btn.setToolTip(tooltip)

    def _on_station_command_radio_changed(self, _index: int) -> None:
        if bool(getattr(self, "_station_command_bar_loading", False)):
            return
        try:
            ident = int(self.station_command_radio_combo.currentData() or 0)
        except Exception:
            ident = 0
        self._station_command_selected_profile_id = ident if ident > 0 else None
        self._refresh_station_command_bar(force=False)

    def _on_station_health_settings_saved(self) -> None:
        self._refresh_station_health_scope_map()
        try:
            self.station_health_tab.refresh_from_registry()
        except Exception:
            pass
        self._station_health_alert_signature = None
        self._refresh_station_health_alert()

    def _refresh_station_health_scope_map(self) -> None:
        scope_map: dict[str, str] = {}
        try:
            profiles = list(self.multi_radio_store.list_runtime_active_device_profiles())
        except Exception:
            profiles = []

        def _profile_name(profile: dict) -> str:
            name = str(profile.get("name", "") or profile.get("label", "") or "").strip()
            if name:
                return name
            ident = profile.get("id", "")
            return f"Radio {ident}" if ident not in (None, "") else "Station-wide"

        def _host(value: object, default: str = "127.0.0.1") -> str:
            return str(value or default or "127.0.0.1").strip().lower() or str(default or "127.0.0.1")

        def _port(value: object, default: int) -> int:
            try:
                return int(value if value not in (None, "") else default)
            except Exception:
                return int(default)

        def _add(service: str, host: object, port: object, name: str, *, default_port: int) -> None:
            host_text = _host(host)
            port_text = str(_port(port, default_port))
            scope_map[f"{service.lower()}:{host_text}:{port_text}"] = name
            if host_text in {"127.0.0.1", "localhost"}:
                scope_map[f"{service.lower()}:loopback:{port_text}"] = name

        for raw_profile in profiles:
            if not isinstance(raw_profile, dict):
                continue
            name = _profile_name(raw_profile)
            _add("JS8CALL", raw_profile.get("js8_host"), raw_profile.get("js8_port"), name, default_port=2442)
            _add("FLRIG", raw_profile.get("flrig_host"), raw_profile.get("flrig_port"), name, default_port=12345)
            fldigi_host = raw_profile.get("fldigi_host") or raw_profile.get("flrig_host")
            _add("FLDIGI", fldigi_host, raw_profile.get("fldigi_port"), name, default_port=7362)
            backend = str(raw_profile.get("control_backend", "") or "").strip().lower()
            if backend == "rigctld":
                _add("RIGCTLD", raw_profile.get("rig_host"), raw_profile.get("rig_port"), name, default_port=4532)
            if str(raw_profile.get("device_class", "") or "").strip().lower() == "observer":
                _add("OBSERVER", raw_profile.get("sdr_host"), raw_profile.get("sdr_port"), name, default_port=0)
        self._station_health_scope_map = scope_map

    def _station_health_scope_resolver(self, key: str, metadata: dict[str, object]) -> str:
        for meta_key in ("scope", "radio_name", "radio", "profile_name", "device_name"):
            text = str(metadata.get(meta_key, "") or "").strip()
            if text:
                return text
        normalized = str(key or "").strip().lower()
        scope_map = getattr(self, "_station_health_scope_map", {}) or {}
        if normalized in scope_map:
            return scope_map[normalized]
        parts = normalized.split(":")
        if len(parts) >= 3:
            compact = ":".join(parts[:3])
            if compact in scope_map:
                return scope_map[compact]
        return ""

    def _runtime_client_signature_for_settings(self) -> tuple[object, ...]:
        manager = getattr(self, "station_runtime_manager", None)
        if manager is not None and hasattr(manager, "primary_runtime_signature"):
            try:
                signature = manager.primary_runtime_signature()
                if signature:
                    return tuple(signature)
            except Exception:
                pass

        def _text(key: str, default: str = "") -> str:
            try:
                return str(self.settings.get(key, default) or "").strip()
            except Exception:
                return str(default or "").strip()

        def _int(key: str, default: int) -> int:
            try:
                value = self.settings.get(key, default)
                return int(value if value not in (None, "") else default)
            except Exception:
                return int(default)

        return (
            _text("control_via", "FLRig").upper(),
            _text("rig_host", "127.0.0.1"),
            _int("rig_port", 4532),
            _text("flrig_host", "127.0.0.1"),
            _int("flrig_port", 12345),
            _text("fldigi_host", ""),
            _int("fldigi_port", 7362),
            _text("js8_host", "127.0.0.1"),
            _int("js8_port", 2442),
        )

    def _new_js8_control_client(self) -> object:
        js8_host = str(self.settings.get("js8_host", "") or "").strip() or None
        js8_port = int(self.settings.get("js8_port", 2442) or 2442)
        for kwargs in (
            {"host": js8_host, "port": js8_port, "settings": self.settings},
            {"host": js8_host, "port": js8_port},
            {"host": js8_host},
            {},
        ):
            try:
                return JS8ControlClient(**kwargs)
            except TypeError:
                continue
        return JS8ControlClient()

    def _new_varac_status_client(self) -> object:
        for kwargs in ({"settings": self.settings}, {}):
            try:
                return VarACStatusClient(**kwargs)
            except TypeError:
                continue
        return VarACStatusClient()

    def _rebuild_runtime_clients(self, *, force: bool = False) -> None:
        old_signature = self._runtime_client_signature
        station_runtime_manager = getattr(self, "station_runtime_manager", None)
        try:
            self.settings.reload()
        except Exception:
            pass
        if station_runtime_manager is not None:
            try:
                station_runtime_manager.sync_with_store()
            except Exception as e:
                log.debug("MainWindow: station runtime sync failed: %s", e)
        signature = self._runtime_client_signature_for_settings()
        if not force and signature == self._runtime_client_signature:
            self._refresh_station_overview(force=False)
            return
        self._runtime_client_signature = signature

        old_js8 = getattr(self, "js8_control", None)
        if old_signature is not None and signature != old_signature:
            try:
                JS8RxHub.shutdown_all()
            except Exception:
                pass

        primary_runtime = station_runtime_manager.get_primary_runtime() if station_runtime_manager is not None else None
        try:
            self.rig_client = (
                primary_runtime.rig_client
                if primary_runtime is not None and primary_runtime.rig_client is not None
                else rig_control_client_from_settings(self.settings)
            )
        except Exception as e:
            log.debug("MainWindow: rig backend rebuild failed: %s", e)
            self.rig_client = None

        try:
            self.js8_control = (
                primary_runtime.js8_control_client
                if primary_runtime is not None and primary_runtime.js8_control_client is not None
                else self._new_js8_control_client()
            )
        except Exception as e:
            log.debug("MainWindow: JS8 control rebuild failed: %s", e)
            self.js8_control = self._new_js8_control_client()

        try:
            self.varac_status = (
                primary_runtime.varac_status_client
                if primary_runtime is not None and primary_runtime.varac_status_client is not None
                else self._new_varac_status_client()
            )
        except Exception as e:
            log.debug("MainWindow: VarAC status rebuild failed: %s", e)
            self.varac_status = self._new_varac_status_client()

        try:
            self.fldigi_log_status = FldigiLogStatusClient()
        except Exception as e:
            log.debug("MainWindow: FLDigi log status rebuild failed: %s", e)

        try:
            if old_js8 is not None and old_js8 is not self.js8_control and hasattr(old_js8, "stop"):
                old_js8.stop()
        except Exception:
            pass

        if hasattr(self, "scheduler") and self.scheduler is not None:
            try:
                self.scheduler.rig = self.rig_client
                self.scheduler.js8 = self.js8_control
                self.scheduler.varac = self.varac_status
                self.scheduler.fldigi_log = self.fldigi_log_status
            except Exception:
                pass
        self._refresh_station_overview(force=True)

    def _on_runtime_settings_saved(self) -> None:
        self._rebuild_runtime_clients()
        self._apply_runtime_profile_state()
        self._refresh_plan_context_labels("runtime_settings_saved")
        try:
            if self.stations_map_tab is not None and hasattr(self.stations_map_tab, "_start_js8_rx_listener"):
                self.stations_map_tab._start_js8_rx_listener()
        except Exception:
            pass
        try:
            if hasattr(self, "scheduler") and self.scheduler is not None:
                self.scheduler.force_refresh()
        except Exception:
            pass

    def _on_runtime_device_profiles_changed(self) -> None:
        self._rebuild_runtime_clients()
        self._apply_runtime_profile_state()
        self._refresh_plan_context_labels("runtime_device_profiles_changed")
        try:
            if self.stations_map_tab is not None and hasattr(self.stations_map_tab, "_start_js8_rx_listener"):
                self.stations_map_tab._start_js8_rx_listener()
        except Exception:
            pass
        try:
            if hasattr(self, "scheduler") and self.scheduler is not None:
                self.scheduler.force_refresh()
        except Exception:
            pass

    def _refresh_freq_planner_if_loaded(self) -> None:
        try:
            if self.freq_planner_tab is not None:
                if self.stack.currentWidget() is self.freq_planner_tab:
                    self.freq_planner_tab.rebuild_table()
                elif hasattr(self.freq_planner_tab, "mark_schedule_dirty"):
                    self.freq_planner_tab.mark_schedule_dirty()
        except Exception:
            pass

    def _start_launch_control_startup(self) -> None:
        try:
            if hasattr(self, "launch_orchestrator"):
                self.launch_orchestrator.start_startup_sequence()
        except Exception as e:
            log.debug("MainWindow: launch-control startup sequence failed to start: %s", e)

    def _on_launch_sequence_started(self, payload: object) -> None:
        data = payload if isinstance(payload, dict) else {}
        queue = data.get("queue") if isinstance(data, dict) else []
        queue_count = len(queue) if isinstance(queue, list) else 0
        trigger = str(data.get("trigger", "")).strip().capitalize() or "Launch"
        self._launch_progress_total = max(queue_count, 1)
        self._launch_progress_done = 0
        try:
            self.statusBar().showMessage(f"{trigger}: launching {queue_count} application(s)...")
        except Exception:
            pass
        try:
            if self._launch_progress_dialog is not None:
                self._launch_progress_dialog.close()
        except Exception:
            pass
        dlg = QProgressDialog(
            f"{trigger}: launching applications...",
            "Stop",
            0,
            self._launch_progress_total,
            self,
        )
        dlg.setWindowTitle("Launch Control")
        dlg.setWindowModality(Qt.NonModal)
        dlg.setAutoClose(False)
        dlg.setAutoReset(False)
        dlg.setMinimumDuration(0)
        dlg.setValue(0)
        try:
            dlg.canceled.connect(self.launch_orchestrator.stop_sequence)
        except Exception:
            pass
        dlg.show()
        self._launch_progress_dialog = dlg

    def _on_launch_sequence_progress(self, payload: object) -> None:
        data = payload if isinstance(payload, dict) else {}
        name = str(data.get("name", "")).strip() or "Application"
        status = str(data.get("status", "")).strip() or "status"
        detail = str(data.get("detail", "")).strip()
        self._launch_progress_done = min(self._launch_progress_total, self._launch_progress_done + 1)
        try:
            self.statusBar().showMessage(f"Launch: {name} {status}" + (f" ({detail})" if detail else ""))
        except Exception:
            pass
        if self._launch_progress_dialog is not None:
            try:
                label = f"{name}: {status}"
                if detail:
                    label = f"{label} ({detail})"
                self._launch_progress_dialog.setLabelText(label)
                self._launch_progress_dialog.setValue(self._launch_progress_done)
            except Exception:
                pass

    def _on_launch_sequence_finished(self, payload: object) -> None:
        try:
            data = payload if isinstance(payload, dict) else {}
            trigger = str(data.get("trigger", "")).strip().lower()
            launched = int(data.get("launched", 0) or 0)
            running = int(data.get("already_running", 0) or 0)
            failed = int(data.get("failed", 0) or 0)
            timeout = int(data.get("timeout", 0) or 0)
            blocked_self = int(data.get("blocked_self", 0) or 0)
            cancelled = bool(data.get("cancelled", False))
            summary = (
                f"Launch {trigger or 'sequence'} complete: "
                f"launched={launched}, running={running}, failed={failed}, timeout={timeout}, blocked={blocked_self}"
            )
            if cancelled:
                summary = f"{summary}, cancelled=true"
            try:
                self.statusBar().showMessage(summary, 12000)
            except Exception:
                pass
            if self._launch_progress_dialog is not None:
                try:
                    self._launch_progress_dialog.setValue(self._launch_progress_total)
                    self._launch_progress_dialog.close()
                except Exception:
                    pass
                self._launch_progress_dialog = None
            log.info(
                "LaunchControl summary (%s): launched=%s running=%s failed=%s timeout=%s cancelled=%s",
                trigger or "unknown",
                launched,
                running,
                failed,
                timeout,
                cancelled,
            )
        except Exception:
            pass

    def _set_window_icon(self):
        assets_dir = Path(__file__).resolve().parents[2] / "assets"
        icon = QIcon()
        candidates = ["FreqInOut.ico", "FreqInOut-desktop.png"] if sys.platform == "win32" else ["FreqInOut-desktop.png", "FreqInOut.ico"]
        for name in candidates:
            icon_path = assets_dir / name
            if not icon_path.exists():
                continue
            candidate = QIcon(str(icon_path))
            if candidate.isNull():
                continue
            icon = candidate
            break
        if icon.isNull():
            return
        self.setWindowIcon(icon)
        app = QApplication.instance()
        if app is not None:
            app.setWindowIcon(icon)

    def _set_logo_pixmap(self):
        if not hasattr(self, "logo_label"):
            return
        theme = resolve_theme(self.settings)
        assets_dir = Path(__file__).resolve().parents[2] / "assets"
        logo_name = "FreqInOut-dark.png" if theme.get("bg") == "#0F1216" else "FreqInOut_logo.png"
        logo_path = assets_dir / logo_name
        if not logo_path.exists():
            self.logo_label.clear()
            return
        pix = QPixmap(str(logo_path))
        if pix.isNull():
            self.logo_label.clear()
            return
        pix = pix.scaledToWidth(160, Qt.SmoothTransformation)
        self.logo_label.setPixmap(pix)

    def _load_nav_group_states(self) -> dict[str, bool]:
        # Station and FreqPlanner are primary workspaces; keep their children
        # discoverable on startup. NCS/Operators remain compact by default.
        defaults = {"Station": True, "FreqPlanner": True, "NCS": False, "Operators": False}
        try:
            raw = self.settings.get("main_nav_group_states", {}) or {}
        except Exception:
            raw = {}
        if isinstance(raw, dict):
            # Backward compatibility for prior key name.
            if "FreqPlanner" not in raw and "Schedules" in raw:
                raw["FreqPlanner"] = raw.get("Schedules")
            if "FreqPlanner" not in raw and "Schedule" in raw:
                raw["FreqPlanner"] = raw.get("Schedule")
            for key in defaults:
                if key in raw:
                    defaults[key] = bool(raw.get(key))
            # Older 7A builds could persist auto-collapsed primary workspaces.
            # Reset those so schedules and Station details do not vanish after restart.
            defaults["Station"] = True
            defaults["FreqPlanner"] = True
        return defaults

    def _persist_nav_group_states(self) -> None:
        try:
            self.settings.set("main_nav_group_states", dict(self._nav_group_states))
        except Exception:
            pass

    @staticmethod
    def _nav_group_for_label(button_label: str, screen_label: str = "") -> str:
        screen = str(screen_label or "").strip()
        if screen in {"NCS-FLDigi/SSB", "NCS-JS8", "NCS-Local"}:
            return "NCS"
        if screen in {"Station Overview", "Station Health"}:
            return "Station"
        if screen in {"FreqPlanner", "HF Schedule", "Net Schedule", "Peer Schedules"}:
            return "FreqPlanner"
        if screen in {"HF Operators", "Local Operators"}:
            return "Operators"
        txt = str(button_label or "").strip()
        if txt.startswith("NCS -"):
            return "NCS"
        if txt.startswith("Schedule -"):
            return "FreqPlanner"
        if txt.startswith("Operators -"):
            return "Operators"
        return ""

    def _nav_group_for_screen_label(self, screen_label: str) -> str:
        return self._nav_group_for_label("", screen_label)

    def _expand_nav_group_for_screen(self, screen_label: str) -> None:
        key = self._nav_group_for_screen_label(screen_label)
        if not key:
            return
        body = self._nav_group_bodies.get(key)
        header = self._nav_group_headers.get(key)
        if body is not None:
            body.setVisible(True)
        if header is not None and not header.isChecked():
            header.blockSignals(True)
            try:
                header.setChecked(True)
            finally:
                header.blockSignals(False)
            self._set_nav_group_header_visual_state(header, True)
        self._nav_group_states[key] = True

    def _ensure_nav_group_layout(self, group_key: str, nav_layout: QVBoxLayout) -> QVBoxLayout:
        key = str(group_key or "").strip()
        existing = self._nav_group_layouts.get(key)
        if existing is not None:
            return existing

        section = QWidget(self.nav_content)
        section_layout = QVBoxLayout(section)
        section_layout.setContentsMargins(0, 0, 0, 0)
        section_layout.setSpacing(2)

        header = QPushButton(section)
        header.setText(key)
        header.setCheckable(True)
        expanded = bool(self._nav_group_states.get(key, True))
        header.setChecked(expanded)
        header.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._sync_nav_group_header_font(header)
        self._set_nav_group_header_visual_state(header, expanded)
        header.toggled.connect(lambda checked, g=key: self._on_nav_group_toggled(g, checked))
        section_layout.addWidget(header)

        body = QWidget(section)
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(8, 0, 0, 0)
        body_layout.setSpacing(3)
        body.setVisible(expanded)
        section_layout.addWidget(body)

        self._nav_group_headers[key] = header
        self._nav_group_bodies[key] = body
        self._nav_group_layouts[key] = body_layout
        self._nav_group_sections[key] = section
        nav_layout.addWidget(section)
        return body_layout

    def _on_nav_group_toggled(self, group_key: str, expanded: bool) -> None:
        key = str(group_key or "").strip()
        body = self._nav_group_bodies.get(key)
        header = self._nav_group_headers.get(key)
        if body is not None:
            body.setVisible(bool(expanded))
        if header is not None:
            self._set_nav_group_header_visual_state(header, expanded)
        self._nav_group_states[key] = bool(expanded)
        self._persist_nav_group_states()
        try:
            self._update_nav_group_header_styles(resolve_theme(self.settings))
        except Exception:
            pass
        self._update_nav_layout_metrics()

    def _update_nav_group_header_styles(self, theme: dict) -> None:
        align_style = self._nav_button_alignment_style()
        for key in self._nav_group_order:
            header = self._nav_group_headers.get(key)
            if header is None:
                continue
            self._sync_nav_group_header_font(header)
            expanded = bool(self._nav_group_states.get(key, True))
            role = "secondary" if expanded else "muted"
            # If NCS group is collapsed while any net is active, keep an explicit
            # reminder on the accordion header.
            if key == "NCS" and (not expanded) and any(bool(v) for v in self._ncs_net_active.values()):
                role = "warning"
            if key == "Station" and not expanded:
                issue_count, severity = self._station_health_alert_counts()
                if issue_count > 0:
                    role = "danger" if severity == "danger" else "warning"
                    header.setToolTip(
                        f"Station Health: {issue_count} responsiveness issue"
                        f"{'s' if issue_count != 1 else ''}. Expand Station or open Health Details."
                    )
                else:
                    header.setToolTip("")
            self._set_nav_group_header_visual_state(header, expanded)
            try:
                header.setStyleSheet(button_style(role, theme) + align_style)
            except Exception:
                pass

    def _sync_nav_group_header_font(self, header: QPushButton) -> None:
        try:
            source = self.nav_buttons[0] if getattr(self, "nav_buttons", None) else self
            header.setFont(source.font())
        except Exception:
            pass

    def _set_nav_group_header_visual_state(self, header: QPushButton, expanded: bool) -> None:
        try:
            style = header.style() or QApplication.style()
            if style is not None:
                icon_kind = QStyle.SP_ArrowDown if expanded else QStyle.SP_ArrowRight
                header.setIcon(style.standardIcon(icon_kind))
        except Exception:
            pass

    def _group_has_active_nav_context(self, key: str) -> bool:
        key_txt = str(key or "").strip()
        if key_txt == "NCS":
            return any(bool(v) for v in self._ncs_net_active.values())
        if key_txt == "Station":
            issue_count, _severity = self._station_health_alert_counts()
            return issue_count > 0
        return False

    def _auto_collapse_inactive_nav_groups(self) -> None:
        if not hasattr(self, "nav_widget"):
            return
        if not hasattr(self, "status_dock_widget"):
            return
        nav_layout = self.nav_widget.layout()
        if nav_layout is None:
            return
        try:
            margins = nav_layout.contentsMargins()
            avail_h = int(self.nav_widget.height() - margins.top() - margins.bottom())
        except Exception:
            avail_h = int(self.nav_widget.height())
        if avail_h <= 0:
            return
        try:
            logo_h = int(self.logo_label.sizeHint().height()) if self.logo_label.isVisible() else 0
        except Exception:
            logo_h = 0
        try:
            status_h = int(self.status_dock_widget.sizeHint().height())
        except Exception:
            status_h = 0
        spacing = int(nav_layout.spacing()) if nav_layout is not None else 0
        # Keep a minimal nav-scroll footprint so status cards can remain visible.
        min_nav_zone_h = 24
        required_h = logo_h + status_h + min_nav_zone_h + (spacing * 2)
        if avail_h >= required_h:
            return

        # Collapse expanded groups that are currently inactive until status
        # sections can remain fully visible.
        changed = False
        collapse_order = [k for k in self._nav_group_order if k != "NCS"] + ["NCS"]
        for key in collapse_order:
            if avail_h >= required_h:
                break
            if not bool(self._nav_group_states.get(key, False)):
                continue
            if self._group_has_active_nav_context(key):
                continue
            header = self._nav_group_headers.get(key)
            if header is None:
                continue
            header.blockSignals(True)
            try:
                header.setChecked(False)
            finally:
                header.blockSignals(False)
            body = self._nav_group_bodies.get(key)
            if body is not None:
                body.setVisible(False)
            header.setArrowType(Qt.RightArrow)
            self._nav_group_states[key] = False
            changed = True
            try:
                status_h = int(self.status_dock_widget.sizeHint().height())
            except Exception:
                pass
            required_h = logo_h + status_h + min_nav_zone_h + (spacing * 2)
        if changed:
            try:
                self._update_nav_group_header_styles(resolve_theme(self.settings))
            except Exception:
                pass

    def _style_condition_levels_edit_action(self, theme: dict) -> None:
        if not hasattr(self, "condition_levels_edit_btn"):
            return
        normal = theme.get("accent", theme.get("info", "#1E88E5"))
        hover = theme.get("info", normal)
        self.condition_levels_edit_btn.setStyleSheet(
            "QToolButton {"
            f"color: {normal}; border: none; background: transparent; padding: 0px; text-align: left; "
            "text-decoration: underline;"
            "}"
            "QToolButton:hover {"
            f"color: {hover};"
            "}"
        )

    @staticmethod
    def _condition_level_palette(level: int) -> tuple[str, str]:
        palette = {
            1: ("#C62828", "#FFFFFF"),  # Red
            2: ("#EF6C00", "#111111"),  # Orange
            3: ("#F9A825", "#111111"),  # Yellow
            4: ("#1565C0", "#FFFFFF"),  # Blue
            5: ("#2E7D32", "#FFFFFF"),  # Green
        }
        return palette.get(int(level), ("#455A64", "#FFFFFF"))

    def _collect_condition_levels(self) -> list[tuple[str, int]]:
        try:
            rows = self.settings.get("operating_groups", []) or []
        except Exception:
            rows = []
        if not isinstance(rows, list):
            return []
        by_group: dict[str, int] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            group = str(row.get("group", "") or "").strip().upper()
            if not group:
                continue
            if not bool(row.get("use_condition_levels", False)):
                continue
            try:
                level = int(row.get("condition_level", 0) or 0)
            except Exception:
                level = 0
            if level < 1 or level > 5:
                continue
            prev = by_group.get(group)
            if prev is None or level < prev:
                by_group[group] = level
        out = sorted(by_group.items(), key=lambda x: x[0])
        return [(g, lvl) for g, lvl in out]

    def _clear_layout_widgets(self, layout: QVBoxLayout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

    def _refresh_condition_level_panel(self) -> None:
        if not self._ui_refresh_allowed():
            self._mark_ui_refresh_dirty("condition_level_panel")
            return
        if not hasattr(self, "condition_levels_rows_layout"):
            return
        levels = self._collect_condition_levels()
        if not levels:
            self._condition_levels_signature = tuple()
            self._clear_layout_widgets(self.condition_levels_rows_layout)
            if hasattr(self, "condition_level_container"):
                self.condition_level_container.setVisible(False)
            self._auto_collapse_inactive_nav_groups()
            return
        if hasattr(self, "condition_level_container"):
            self.condition_level_container.setVisible(True)
        signature = tuple((g, int(level)) for g, level in levels)
        if signature == getattr(self, "_condition_levels_signature", tuple()):
            # If signature is unchanged, still verify rows are rendered as button widgets.
            # This prevents stale row formats from persisting across iterative UI updates.
            rows_current = True
            try:
                if self.condition_levels_rows_layout.count() != len(levels):
                    rows_current = False
                else:
                    for i in range(self.condition_levels_rows_layout.count()):
                        item = self.condition_levels_rows_layout.itemAt(i)
                        row_widget = item.widget() if item is not None else None
                        if row_widget is None:
                            rows_current = False
                            break
                        chips = row_widget.findChildren(QPushButton)
                        # Current format is exactly one button row and no standalone labels.
                        labels = row_widget.findChildren(QLabel)
                        if len(chips) != 1:
                            rows_current = False
                            break
                        if labels:
                            rows_current = False
                            break
            except Exception:
                rows_current = False
            if rows_current:
                return
        self._condition_levels_signature = signature
        self._clear_layout_widgets(self.condition_levels_rows_layout)
        for group, level in levels:
            row = QWidget(self.condition_levels_rows)
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(6)
            chip = QPushButton(f"{group}  Level {level}")
            chip.setObjectName("conditionLevelChip")
            bg, fg = self._condition_level_palette(level)
            chip.setCheckable(False)
            chip.setEnabled(True)
            chip.setFocusPolicy(Qt.NoFocus)
            chip.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            chip.setStyleSheet(
                f"QPushButton#conditionLevelChip {{"
                f" font-weight: 600; border-radius: 6px; padding: 2px 8px;"
                f" text-align: left; background-color: {bg}; color: {fg}; border: 1px solid {bg};"
                f"}}"
                f"QPushButton#conditionLevelChip:hover {{ background-color: {bg}; color: {fg}; border: 1px solid {bg}; }}"
                f"QPushButton#conditionLevelChip:pressed {{ background-color: {bg}; color: {fg}; border: 1px solid {bg}; }}"
            )
            row_layout.addWidget(chip, 1)
            self.condition_levels_rows_layout.addWidget(row)
        self._auto_collapse_inactive_nav_groups()

    def _open_condition_levels_editor(self) -> None:
        groups_data = self.settings.get("operating_groups", []) or []
        if not isinstance(groups_data, list) or not groups_data:
            QMessageBox.information(self, "Condition Levels", "No HF Operating Groups found.")
            return
        grouped: dict[str, dict[str, object]] = {}
        for row in groups_data:
            if not isinstance(row, dict):
                continue
            group = str(row.get("group", "") or "").strip().upper()
            if not group:
                continue
            use_level = bool(row.get("use_condition_levels", False))
            try:
                level = int(row.get("condition_level", 5) or 5)
            except Exception:
                level = 5
            if level < 1 or level > 5:
                level = 5
            if group not in grouped:
                grouped[group] = {"use": use_level, "level": level}
            else:
                grouped[group]["use"] = bool(grouped[group].get("use")) or use_level
                grouped[group]["level"] = min(int(grouped[group].get("level", level)), level)
        if not grouped:
            QMessageBox.information(self, "Condition Levels", "No HF Operating Groups found.")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("Edit Condition Levels")
        dlg_layout = QVBoxLayout(dlg)
        rows_holder = QWidget(dlg)
        rows_layout = QVBoxLayout(rows_holder)
        rows_layout.setContentsMargins(0, 0, 0, 0)
        rows_layout.setSpacing(6)
        editors: dict[str, tuple[QCheckBox, QComboBox]] = {}
        for group in sorted(grouped.keys()):
            row_meta = grouped[group]
            row = QWidget(rows_holder)
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(8)
            group_lbl = QLabel(group)
            group_lbl.setMinimumWidth(120)
            use_chk = QCheckBox("Use Condition Levels")
            use_chk.setChecked(bool(row_meta.get("use", False)))
            level_combo = QComboBox()
            for n in range(1, 6):
                level_combo.addItem(str(n), n)
            level_combo.setCurrentText(str(int(row_meta.get("level", 5))))
            level_combo.setEnabled(use_chk.isChecked())
            use_chk.toggled.connect(level_combo.setEnabled)
            row_layout.addWidget(group_lbl)
            row_layout.addWidget(use_chk)
            row_layout.addWidget(QLabel("Level:"))
            row_layout.addWidget(level_combo)
            row_layout.addStretch(1)
            rows_layout.addWidget(row)
            editors[group] = (use_chk, level_combo)
        dlg_layout.addWidget(rows_holder)
        buttons = QHBoxLayout()
        save_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        buttons.addStretch(1)
        buttons.addWidget(save_btn)
        buttons.addWidget(cancel_btn)
        dlg_layout.addLayout(buttons)

        def _save() -> None:
            updated: list[dict] = []
            for raw in groups_data:
                if not isinstance(raw, dict):
                    continue
                row = dict(raw)
                group = str(row.get("group", "") or "").strip().upper()
                editor = editors.get(group)
                if editor is not None:
                    use_chk, level_combo = editor
                    row["use_condition_levels"] = bool(use_chk.isChecked())
                    try:
                        level = int(level_combo.currentData() or level_combo.currentText() or 5)
                    except Exception:
                        level = 5
                    if level < 1 or level > 5:
                        level = 5
                    row["condition_level"] = level
                updated.append(row)
            try:
                self.settings.set("operating_groups", updated)
                self.settings.reload()
            except Exception as e:
                QMessageBox.warning(self, "Condition Levels", f"Failed to save condition levels:\n{e}")
                return
            try:
                if hasattr(self, "settings_tab") and self.settings_tab is not None:
                    self.settings_tab.operating_groups = [dict(r) for r in updated if isinstance(r, dict)]
                    if hasattr(self.settings_tab, "_refresh_operating_groups_table"):
                        self.settings_tab._refresh_operating_groups_table()  # type: ignore[attr-defined]
            except Exception:
                pass
            self._refresh_condition_level_panel()
            dlg.accept()
            QTimer.singleShot(0, self._queue_condition_levels_changed)

        save_btn.clicked.connect(_save)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

    def _style_ledge_clock(self, theme: dict[str, str] | None = None) -> None:
        if not hasattr(self, "ledge_clock_widget"):
            return
        palette = theme or resolve_theme(self.settings)
        border = palette.get("border", "#D0D7DE")
        panel = palette.get("panel", palette.get("background", "#FFFFFF"))
        text = palette.get("text", "#202124")
        muted = palette.get("muted", "#5F6368")
        self.ledge_clock_widget.setStyleSheet(
            f"""
            QFrame#mainLedgeClock {{
                background: {panel};
                border: 1px solid {border};
                border-radius: 4px;
            }}
            QLabel#ledgeLocalTime {{
                color: {text};
                font-weight: 600;
            }}
            QLabel#ledgeUtcTime {{
                color: {muted};
                font-weight: 600;
            }}
            """
        )

    def _set_screen(self, index: int) -> None:
        with perf_span(
            "main_window.set_screen",
            settings=self.settings,
            meta={"index": index},
            min_ms=5.0,
        ):
            if 0 <= index < self.stack.count():
                label = self._screens[index][0]
                if self._screen_is_runtime_suppressed(label):
                    fallback_index = self._runtime_fallback_screen_index()
                    if fallback_index != index:
                        self._set_screen(fallback_index)
                    return
                if label == "Map" and time.time() < float(getattr(self, "_help_dialog_settle_until", 0.0) or 0.0):
                    remaining_ms = int(
                        max(50.0, (float(self._help_dialog_settle_until) - time.time()) * 1000.0)
                    )
                    self._show_tab_loading_notice("Preparing Map...")
                    log.info("UI_LIFECYCLE|map_switch_deferred reason=help_settle ms=%s", remaining_ms)
                    QTimer.singleShot(remaining_ms, lambda idx=index: self._set_screen(idx))
                    return
                if label != "Map" and self._pending_map_switch_index is not None:
                    self._pending_map_switch_index = None
                if label == "Map" and self._queue_map_switch_after_webengine_warmup(index):
                    return
                try:
                    nav_idx = self._nav_screen_index_map.get(index)
                    self._expand_nav_group_for_screen(label)
                    if nav_idx is not None and 0 <= nav_idx < len(self.nav_buttons):
                        btn = self.nav_buttons[nav_idx]
                        if not btn.isChecked():
                            btn.setChecked(True)
                except Exception:
                    pass
                prev_index = self._active_tab_index
                if prev_index is not None and 0 <= prev_index < self.stack.count():
                    try:
                        prev_widget = self.stack.widget(prev_index)
                        if hasattr(prev_widget, "set_tab_active"):
                            prev_widget.set_tab_active(False)
                    except Exception:
                        pass

                self._ensure_lazy_tab_loaded(label, index)
                self.stack.setCurrentIndex(index)
                self._active_tab_index = index
                try:
                    fit_child_combo_boxes(self.stack.widget(index))
                except Exception:
                    pass
                try:
                    widget_active = self.stack.widget(index)
                    if label == "Messages" and hasattr(widget_active, "show_inbox_from_navigation"):
                        widget_active.show_inbox_from_navigation()
                    if hasattr(widget_active, "set_tab_active"):
                        widget_active.set_tab_active(True)
                except Exception:
                    pass
                self._update_map_filters_visibility(index)
                self._update_ncs_nav_button_styles()
                QTimer.singleShot(0, self._refresh_scheduler_status_panel)
                try:
                    widget = self.stack.widget(index)
                    if hasattr(widget, "show_loading_toast"):
                        widget.show_loading_toast()
                    if hasattr(widget, "on_tab_activated"):
                        QTimer.singleShot(0, widget.on_tab_activated)
                except Exception:
                    pass

    def _on_ncs_net_status_changed(self, kind: str, active: bool) -> None:
        kind_key = (kind or "").strip().upper()
        if kind_key in self._ncs_net_active:
            self._ncs_net_active[kind_key] = bool(active)
        try:
            # Scheduler only tracks FLDIGI/JS8 manual net locks.
            if kind_key in {"FLDIGI", "JS8"} and hasattr(self, "scheduler"):
                self.scheduler.set_manual_net_active(kind_key, bool(active))
        except Exception:
            pass
        self._update_ncs_nav_button_styles()
        self._refresh_scheduler_status_panel()

    @staticmethod
    def _nav_button_alignment_style() -> str:
        return (
            "QPushButton { text-align: left; padding-left: 12px; }"
            "QToolButton { text-align: left; padding-left: 12px; }"
        )

    def _update_ncs_nav_button_styles(self) -> None:
        if not getattr(self, "nav_buttons", None):
            return
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = {}
        self._update_nav_group_header_styles(theme)
        align_style = self._nav_button_alignment_style()
        # Base styling for all sidebar buttons so typography/contrast is consistent.
        for btn in self.nav_buttons:
            role = "primary" if btn.isChecked() else "muted"
            try:
                btn.setStyleSheet(button_style(role, theme) + align_style)
            except Exception:
                pass
        # Overlay active-net reminder on NCS entries only.
        for kind_key, idx in self._ncs_nav_indices.items():
            if idx < 0 or idx >= len(self.nav_buttons):
                continue
            btn = self.nav_buttons[idx]
            active = bool(self._ncs_net_active.get(kind_key))
            if not active:
                continue
            try:
                btn.setStyleSheet(button_style("warning", theme) + align_style)
            except Exception:
                pass
        self._apply_station_health_nav_alert(theme, align_style)

    def _station_health_nav_label(self, issue_count: int = 0) -> str:
        idx = getattr(self, "_station_health_nav_index", None)
        if idx is None or idx < 0 or idx >= len(getattr(self, "_nav_base_labels", [])):
            return "Station Health"
        label = str(self._nav_base_labels[idx] or "Station Health")
        try:
            callsign = str(self.settings.get("callsign", "") or "").strip().upper()
        except Exception:
            callsign = ""
        if callsign:
            label = f"{label} [{callsign}]"
        if issue_count > 0:
            label = f"{label} ({issue_count})"
        return label

    def _refresh_station_health_alert(self) -> None:
        if not self._ui_refresh_allowed():
            self._mark_ui_refresh_dirty("station_health_alert")
            return
        try:
            summary = summarize_station_health(include_ok=False)
        except Exception:
            summary = {"issue_count": 0, "severity": "ok", "issue_items": []}
        issue_count = int(summary.get("issue_count", 0) or 0)
        severity = str(summary.get("severity", "ok") or "ok")
        signature = (issue_count, severity)
        if signature != getattr(self, "_station_health_alert_signature", None):
            self._station_health_alert_signature = signature
            idx = getattr(self, "_station_health_nav_index", None)
            if idx is not None and 0 <= idx < len(getattr(self, "nav_buttons", [])):
                try:
                    self.nav_buttons[idx].setText(self._station_health_nav_label(issue_count))
                    self._update_nav_layout_metrics()
                except Exception:
                    pass
        self._station_health_alert_summary = dict(summary)
        self._update_ncs_nav_button_styles()

    def _station_health_alert_counts(self) -> tuple[int, str]:
        try:
            summary = getattr(self, "_station_health_alert_summary", None)
            if not isinstance(summary, dict):
                summary = summarize_station_health(include_ok=False)
            issue_count = int(summary.get("issue_count", 0) or 0)
            severity = str(summary.get("severity", "ok") or "ok")
            return issue_count, severity
        except Exception:
            return 0, "ok"

    def _apply_station_health_nav_alert(self, theme: dict[str, str], align_style: str) -> None:
        idx = getattr(self, "_station_health_nav_index", None)
        if idx is None or idx < 0 or idx >= len(getattr(self, "nav_buttons", [])):
            return
        btn = self.nav_buttons[idx]
        try:
            issue_count, severity = self._station_health_alert_counts()
            if issue_count <= 0:
                btn.setToolTip("Station Health: no known external software responsiveness issues.")
                return
            role = "danger" if severity == "danger" else "warning"
            btn.setStyleSheet(button_style(role, theme) + align_style)
            btn.setToolTip(
                f"Station Health: {issue_count} responsiveness issue"
                f"{'s' if issue_count != 1 else ''}. Open Station Health for details."
            )
        except Exception:
            pass
