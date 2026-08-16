from __future__ import annotations

import datetime
import json
import re
import sqlite3
import sys
import time
from typing import Callable, Mapping

from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
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
    QMenu,
)
from PySide6.QtGui import QPixmap, QIcon, QFontMetrics, QAction
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt, QTimer, QUrl
from pathlib import Path

from freqinout.core.logger import log
from freqinout.core.logger import set_log_level
from freqinout.core.config_paths import get_config_dir
from freqinout.core.multi_radio_store import MultiRadioStore, SUPPORTED_RUNTIME_CONTROL_BACKENDS
from freqinout.core.perf_metrics import span as perf_span
from freqinout.core.plan_context_service import PlanContextService
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.shared_state import ActionFeedbackEvent, ActionFeedbackService
from freqinout.core.station_command_state import (
    manual_qsy_meta_for_radio as station_command_manual_qsy_meta_for_radio,
    scheduler_entry_radio_id as station_command_scheduler_entry_radio_id,
    scheduler_manual_qsy_active_for_radio,
    scheduler_suspended_manually_for_radio,
    timed_suspend_active_for_radio,
)
from freqinout.core.station_runtime_manager import StationRuntimeManager
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.background_ingest import BackgroundIngestController
from freqinout.core.dependency_status_service import get_dependency_status_service, shutdown_dependency_status_service
from freqinout.core.station_readiness import (
    build_station_readiness_report,
    format_readiness_issue,
    visible_status_programs,
)
from freqinout.core.js8spotter_archive import load_js8spotter_archive_records
from freqinout.core.js8_expect_runtime import build_expect_rf_guard_preflight
from freqinout.core.ingest_runtime_status import active_runtime_source_view_rows, runtime_source_view_rows_from_skip_reasons
from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health
from freqinout.radio_interface.js8_api_client import JS8ApiClientRegistry
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
from freqinout.gui.local_report_history_tab import LocalReportHistoryTab
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
from freqinout.gui.station_command_presenter import (
    countdown_text as station_command_countdown_text,
    qsy_action_state,
    scheduler_action_state,
    timed_qsy_text,
)
from freqinout.gui.qsy_helper import (
    build_qsy_options,
    load_operating_groups,
    parse_frequency_mhz,
    perform_qsy,
    perform_qsy_with_hold,
    refresh_hold_duration_combo,
    selected_hold_duration,
    selected_qsy_meta,
    suspend_snapshot,
    suspend_schedule_hold,
    set_scheduler_enabled_override,
    set_active_hold_duration,
    set_suspend_until,
    resume_schedule_hold,
    set_hold_duration_default,
    set_suspend_until,
    active_hold_button_role,
    active_hold_button_text,
    active_hold_status_text,
)
from freqinout.gui.theme import resolve_theme, resolve_ui_text_scale, apply_app_theme, button_style, fit_child_combo_boxes, led_style


class ElidedLabel(QLabel):
    def __init__(self, text: str = "", parent: QWidget | None = None):
        super().__init__("", parent)
        self._full_text = ""
        self.setText(text)

    def setText(self, text: str) -> None:
        self._full_text = str(text or "")
        self._update_elided_text()

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._update_elided_text()

    def _update_elided_text(self) -> None:
        width = max(24, int(self.width() or self.sizeHint().width() or 120) - 12)
        text = QFontMetrics(self.font()).elidedText(self._full_text, Qt.ElideRight, width)
        super().setText(text)
        if text != self._full_text and not self.toolTip():
            super().setToolTip(self._full_text)


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
        self._quick_search_cache: tuple[float, list[dict[str, object]]] = (0.0, [])
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
        self.local_report_history_tab = LocalReportHistoryTab(self)
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
        self.station_health_tab.set_runtime_item_provider(self._station_health_runtime_items)
        self.station_health_tab.set_runtime_source_provider(self._station_health_runtime_source_rows)
        self.station_health_tab.related_view_requested.connect(self._open_station_health_runtime_source_related_view)
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
            ("Local Reports", self.local_report_history_tab),
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
        self._station_command_radio_admin_expanded = False
        self._station_command_manual_qsy_meta: dict[str, object] | None = None
        self._station_command_manual_qsy_profile_id: int | None = None
        self._station_command_scheduler_suspended_manual = False
        self._station_command_timed_suspend_profile_id = 0
        self._station_command_lane_cache_data: dict[int, dict[str, object]] | None = None
        self._station_command_lane_cache_expires = 0.0
        self._station_command_off_schedule_by_radio: dict[int, dict[str, object]] = {}
        self._settings_nav_context = "main"
        self._settings_nav_button_indices: dict[str, int] = {}
        self._messages_nav_context = "inbox"
        self._messages_nav_button_indices: dict[str, int] = {}
        # Sidebar button order/text requested by user.
        self._nav_specs = [
            ("ControlFreq", "ControlFreq"),
            ("Control Center", "Station Overview"),
            ("Health Details", "Station Health"),
            ("Plan Manager", "FreqPlanner"),
            ("SOP Builder", "SOP"),
            ("Inbox", "Messages"),
            ("Compose", "Messages"),
            ("Map", "Map"),
            ("FLDigi / SSB", "NCS-FLDigi/SSB"),
            ("JS8Call", "NCS-JS8"),
            ("VHF/UHF", "NCS-Local"),
            ("HF Daily", "HF Schedule"),
            ("HF Nets", "Net Schedule"),
            ("HF Peer Scheds", "Peer Schedules"),
            ("HF Callsigns", "HF Operators"),
            ("Local Callsigns", "Local Operators"),
            ("Local Reports", "Local Reports"),
            ("Main", "Settings"),
            ("Radios", "Settings"),
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
        self._nav_group_order: list[str] = ["Station", "FreqPlanner", "Messages", "NCS", "Operators", "Settings"]
        self._nav_group_states: dict[str, bool] = self._load_nav_group_states()
        self._suppress_initial_nav_group_auto_expand = True

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
            if screen_label == "Settings" and button_label == "Main":
                btn.clicked.connect(lambda _=False: self.open_settings_section("operator_info", settings_nav_context="main"))
            elif screen_label == "Settings" and button_label == "Radios":
                btn.clicked.connect(lambda _=False: self.open_settings_section("radio_profiles", settings_nav_context="radios"))
            elif screen_label == "Messages" and button_label == "Inbox":
                btn.clicked.connect(lambda _=False: self.open_messages_section("inbox"))
            elif screen_label == "Messages" and button_label == "Compose":
                btn.clicked.connect(lambda _=False: self.open_messages_section("compose"))
            else:
                btn.clicked.connect(lambda _=False, i=screen_idx: self._set_screen(i))
            if screen_label in {"Settings", "Messages"}:
                self.button_group.addButton(btn)
            else:
                self.button_group.addButton(btn, screen_idx)
            self.nav_buttons.append(btn)
            btn_idx = len(self.nav_buttons) - 1
            if screen_label == "Settings" and button_label == "Main":
                self._settings_nav_button_indices["main"] = btn_idx
                self._nav_screen_index_map.setdefault(screen_idx, btn_idx)
            elif screen_label == "Settings" and button_label == "Radios":
                self._settings_nav_button_indices["radios"] = btn_idx
            elif screen_label == "Messages" and button_label == "Inbox":
                self._messages_nav_button_indices["inbox"] = btn_idx
                self._nav_screen_index_map.setdefault(screen_idx, btn_idx)
            elif screen_label == "Messages" and button_label == "Compose":
                self._messages_nav_button_indices["compose"] = btn_idx
            else:
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
        self.scheduler_status_container.setVisible(False)
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
        self.resume_schedule_btn = QPushButton("Resume Schedule", self.scheduler_status_container)
        self.resume_schedule_btn.setFixedWidth(140)
        self.resume_schedule_btn.clicked.connect(self._on_resume_schedule_clicked)
        self.suspend_schedule_btn = QPushButton("Suspend", self.scheduler_status_container)
        self.suspend_schedule_btn.setFixedWidth(140)
        self.suspend_schedule_btn.clicked.connect(self._on_suspend_schedule_clicked)
        self.suspend_duration_combo = QComboBox(self.scheduler_status_container)
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
        self.suspend_duration_label = QLabel("Hold", self.scheduler_status_container)
        self.suspend_duration_label.setVisible(False)
        self.suspend_duration_combo.setVisible(False)
        self.suspend_schedule_btn.setVisible(False)
        self.resume_schedule_btn.setVisible(False)
        status_layout.addWidget(self.logs_active_btn, alignment=Qt.AlignCenter)
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
        right_layout.setSpacing(10)

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
        self.station_command_layout = QGridLayout(self.station_command_bar)
        command_layout = self.station_command_layout
        command_layout.setContentsMargins(10, 8, 10, 8)
        command_layout.setSpacing(8)
        self._station_command_layout_mode = ""
        self.station_command_radio_label = QLabel("Radio")
        self.station_command_radio_label.setObjectName("stationCommandRadioLabel")
        self.station_command_radio_combo = QComboBox(self.station_command_bar)
        self.station_command_radio_combo.setObjectName("stationCommandRadioSelector")
        self.station_command_radio_combo.setMinimumWidth(160)
        self.station_command_radio_combo.setMaximumWidth(340)
        self.station_command_radio_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.station_command_radio_combo.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.station_command_radio_combo.currentIndexChanged.connect(self._on_station_command_radio_changed)
        self.station_command_radio_separator = QFrame(self.station_command_bar)
        self.station_command_radio_separator.setObjectName("stationCommandRadioSeparator")
        self.station_command_radio_separator.setFrameShape(QFrame.VLine)
        self.station_command_radio_separator.setFrameShadow(QFrame.Plain)
        self.station_command_now_caption = QLabel("Now")
        self.station_command_now_caption.setObjectName("stationCommandNowCaption")
        self.station_command_now_label = ElidedLabel("Now: unavailable", self.station_command_bar)
        self.station_command_now_label.setObjectName("stationCommandNow")
        self.station_command_now_label.setWordWrap(False)
        self.station_command_now_label.setMinimumWidth(0)
        self.station_command_now_label.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.station_command_now_label.setToolTip("Current frequency/control target for the selected radio.")
        self.station_command_freq_combo = QComboBox(self.station_command_bar)
        self.station_command_freq_combo.setObjectName("stationCommandFrequencySelector")
        self.station_command_freq_combo.setMinimumWidth(150)
        self.station_command_freq_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.station_command_freq_combo.setToolTip("Select the operating group and band for manual QSY.")
        self.station_command_state_label = QLabel("State: unknown")
        self.station_command_state_label.setObjectName("stationCommandState")
        self.station_command_state_label.setWordWrap(False)
        self.station_command_state_label.setMinimumWidth(0)
        self.station_command_state_label.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.station_command_now_separator = QFrame(self.station_command_bar)
        self.station_command_now_separator.setObjectName("stationCommandNowSeparator")
        self.station_command_now_separator.setFrameShape(QFrame.VLine)
        self.station_command_now_separator.setFrameShadow(QFrame.Plain)
        self.station_command_action_label = QLabel("Action")
        self.station_command_action_label.setObjectName("stationCommandActionLabel")
        self.station_command_next_label = QLabel("Next: none")
        self.station_command_next_label.setObjectName("stationCommandNext")
        self.station_command_next_label.setWordWrap(False)
        self.station_command_next_label.setMinimumWidth(0)
        self.station_command_next_label.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.station_command_health_label = QLabel("Health:")
        self.station_command_health_label.setObjectName("stationCommandHealthLabel")
        self.station_command_health_widget = QWidget(self.station_command_bar)
        self.station_command_health_widget.setObjectName("stationCommandHealth")
        self.station_command_health_widget.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.station_command_health_widget.setCursor(Qt.PointingHandCursor)
        self.station_command_health_widget.mousePressEvent = (
            lambda event, widget=self.station_command_health_widget: self._on_station_command_health_clicked(event, anchor=widget)
        )
        self.station_command_health_layout = QHBoxLayout(self.station_command_health_widget)
        self.station_command_health_layout.setContentsMargins(0, 0, 0, 0)
        self.station_command_health_layout.setSpacing(6)
        self.station_command_health_leds: dict[str, QLabel] = {}
        self.station_command_health_text_labels: dict[str, QLabel] = {}
        self.station_command_duration_combo = QComboBox(self.station_command_bar)
        self.station_command_duration_combo.setObjectName("stationCommandDuration")
        self.station_command_duration_combo.setToolTip(
            "Duration for QSY Suspend."
        )
        self.station_command_duration_combo.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.station_command_duration_combo.currentIndexChanged.connect(self._on_station_command_hold_duration_changed)
        refresh_hold_duration_combo(self.station_command_duration_combo, self.settings, self._active_runtime_profile)
        self.station_command_qsy_btn = QPushButton("QSY Now")
        self.station_command_qsy_btn.setObjectName("stationCommandQsy")
        self.station_command_hold_btn = QPushButton("QSY Suspend")
        self.station_command_hold_btn.setObjectName("stationCommandHold")
        self.station_command_suspend_btn = QPushButton("Suspend Scheduler")
        self.station_command_suspend_btn.setObjectName("stationCommandSuspend")
        self.station_command_resume_btn = QPushButton("Resume Schedule")
        self.station_command_resume_btn.setObjectName("stationCommandResume")
        self.station_command_qsy_btn.clicked.connect(self._on_station_command_qsy_now_clicked)
        self.station_command_hold_btn.clicked.connect(self._on_station_command_qsy_hold_clicked)
        self.station_command_suspend_btn.clicked.connect(self._on_station_command_pause_clicked)
        self.station_command_resume_btn.clicked.connect(self._on_station_command_resume_clicked)
        for btn in (
            self.station_command_qsy_btn,
            self.station_command_hold_btn,
            self.station_command_suspend_btn,
            self.station_command_resume_btn,
        ):
            btn.setEnabled(False)
            btn.setToolTip("Station command wiring is not enabled yet.")
            btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self._station_command_qsy_suspend_base_text = "QSY Suspend"
        self._station_command_suspend_base_text = "Suspend Scheduler"
        self.station_command_radio_summary_label = QLabel("Radios")
        self.station_command_radio_summary_label.setObjectName("stationCommandRadioSummaryLabel")
        self.station_command_radio_summary_scroll = QWidget(self.station_command_bar)
        self.station_command_radio_summary_scroll.setObjectName("stationCommandRadioSummaryScroll")
        self.station_command_radio_summary_scroll.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.station_command_radio_summary_scroll.setFixedHeight(42)
        self.station_command_radio_summary_widget = self.station_command_radio_summary_scroll
        self.station_command_radio_summary_widget.setObjectName("stationCommandRadioSummary")
        self.station_command_radio_summary_widget.setMinimumWidth(0)
        self.station_command_radio_summary_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.station_command_radio_summary_layout = QHBoxLayout(self.station_command_radio_summary_widget)
        self.station_command_radio_summary_layout.setContentsMargins(0, 0, 0, 0)
        self.station_command_radio_summary_layout.setSpacing(6)
        self.station_command_radio_prev_btn = QPushButton("Prev")
        self.station_command_radio_prev_btn.setObjectName("stationCommandRadioPagePrev")
        self.station_command_radio_prev_btn.setToolTip("Show previous radios.")
        self.station_command_radio_prev_btn.clicked.connect(lambda _checked=False: self._change_station_command_radio_page(-1))
        self.station_command_radio_prev_btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.station_command_radio_next_btn = QPushButton("Next")
        self.station_command_radio_next_btn.setObjectName("stationCommandRadioPageNext")
        self.station_command_radio_next_btn.setToolTip("Show next radios.")
        self.station_command_radio_next_btn.clicked.connect(lambda _checked=False: self._change_station_command_radio_page(1))
        self.station_command_radio_next_btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self._station_command_radio_page = 0
        self._station_command_radio_summary_signature: tuple[object, ...] | None = None
        self.station_command_radio_admin_btn = QPushButton("All Radios")
        self.station_command_radio_admin_btn.setObjectName("stationCommandRadioAdminToggle")
        self.station_command_radio_admin_btn.setToolTip("Show or hide the all-radio status and assignment panel.")
        self.station_command_radio_admin_btn.clicked.connect(self._toggle_station_command_radio_admin)
        self.station_command_radio_admin_btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.station_command_radio_admin_panel = QWidget(self.station_command_bar)
        self.station_command_radio_admin_panel.setObjectName("stationCommandRadioAdminPanel")
        self.station_command_radio_admin_layout = QVBoxLayout(self.station_command_radio_admin_panel)
        self.station_command_radio_admin_layout.setContentsMargins(0, 0, 0, 0)
        self.station_command_radio_admin_layout.setSpacing(6)
        self.station_command_radio_admin_panel.setVisible(False)
        self._apply_station_command_bar_layout(force=True)
        try:
            self.dependency_status_service.snapshot_changed.connect(lambda _snapshot: self._refresh_station_command_bar(force=False))
        except Exception:
            pass
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
        self._suppress_initial_nav_group_auto_expand = False
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
        self.background_ingest = BackgroundIngestController(
            self.settings,
            expect_guard_preflight=build_expect_rf_guard_preflight(self.station_runtime_manager),
        )
        if self._runtime_background_ingest_enabled(self._active_runtime_profile, startup_policy):
            self.background_ingest.start()
        else:
            log.info("MainWindow: background ingest disabled for current runtime policy")
        try:
            if hasattr(self.launch_orchestrator, "set_runtime_launch_enabled"):
                self.launch_orchestrator.set_runtime_launch_enabled(
                    self._runtime_launch_enabled(self._active_runtime_profile, startup_policy),
                    reason="Launch Control is disabled by the primary operating model.",
                )
            self._launch_startup_suppressed = not self._runtime_launch_enabled(self._active_runtime_profile, startup_policy)
        except Exception:
            pass
        try:
            self.scheduler.off_schedule_detected.connect(self._on_off_schedule_detected)
        except Exception:
            pass
        try:
            self.scheduler.off_schedule_cleared.connect(self._on_off_schedule_cleared)
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
        _connect_or_log("settings_saved -> local_report_history_tab", self.settings_tab.settings_saved, self.local_report_history_tab.on_settings_saved)
        if hasattr(self.local_report_history_tab, "local_reports_map_requested"):
            _connect_or_log(
                "local_report_history_tab.local_reports_map_requested -> map",
                self.local_report_history_tab.local_reports_map_requested,
                self.open_local_reports_map,
            )
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
            if hasattr(self.local_operator_tab, "local_reports_requested"):
                self.local_operator_tab.local_reports_requested.connect(self.open_local_reports)
        except Exception as e:
            log.debug("MainWindow signal wiring failed: local_reports_requested -> local reports: %s", e)
        try:
            if hasattr(self.local_ncs_tab, "local_data_updated"):
                self.local_ncs_tab.local_data_updated.connect(self.local_operator_tab._load_data)
                self.local_ncs_tab.local_data_updated.connect(self.local_report_history_tab.refresh_reports)
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
        if str(getattr(event, "source_surface", "") or "").strip().lower() == "station_command_bar":
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
                    resumed = bool(resume_schedule_hold(self, self.settings))
                    if not resumed:
                        return
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
                summary="Resume blocked: schedule control did not return to plan.",
                detail="The scheduler was unavailable or RF Safety Guard blocked the resume.",
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
        self._hide_sidebar_schedule_controls()

    def _hide_sidebar_schedule_controls(self) -> None:
        for widget in (
            getattr(self, "resume_schedule_btn", None),
            getattr(self, "suspend_schedule_btn", None),
            getattr(self, "suspend_duration_label", None),
            getattr(self, "suspend_duration_combo", None),
        ):
            if widget is not None:
                try:
                    widget.setVisible(False)
                except Exception:
                    pass

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
        card_controls = getattr(self, "_station_command_radio_tile_controls", None)
        cards_active = isinstance(card_controls, Mapping) and bool(card_controls)
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
                if cards_active and not force:
                    self._update_station_command_hold_button_labels(snapshot)
                    self._update_station_command_radio_tile_hold_controls(snapshot)
                else:
                    self._refresh_station_command_bar(force=False)
            elif snapshot.get("active"):
                self._broadcast_hold_state(snapshot)
                self._update_station_command_hold_button_labels(snapshot)
                self._update_station_command_radio_tile_hold_controls(snapshot)
            return
        try:
            if self._hold_state_timer.isActive():
                self._hold_state_timer.stop()
        except Exception:
            pass
        if signature_changed:
            self._broadcast_hold_state(snapshot)
            self._refresh_scheduler_status_panel()
            if cards_active and not force:
                self._update_station_command_hold_button_labels(snapshot)
                self._update_station_command_radio_tile_hold_controls(snapshot)
            else:
                self._refresh_station_command_bar(force=False)
        elif was_active:
            self._broadcast_hold_state(snapshot)
            self._update_station_command_hold_button_labels(snapshot)
            self._update_station_command_radio_tile_hold_controls(snapshot)

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
                widget.setParent(None)
                widget.deleteLater()
        for line in sig:
            lbl = QLabel(line)
            lbl.setAlignment(Qt.AlignCenter)
            lbl.setWordWrap(True)
            layout.addWidget(lbl)

    @staticmethod
    def _rf_guard_status_reason(status: Mapping[str, object]) -> str:
        if not bool(status.get("rf_conflict_warning")):
            return ""
        peer_name = str(status.get("rf_conflict_peer_name") or "").strip()
        if bool(status.get("rf_conflict_peer_status_unknown")) or bool(status.get("rf_conflict_peer_status_stale")):
            return f"RF Guard: verify {peer_name}" if peer_name else "RF Guard: verify peer radio"
        if peer_name:
            return f"RF Guard: {peer_name}"
        return str(status.get("rf_conflict_summary") or "").strip() or "RF Guard: review"

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
        rf_conflict_summary = self._rf_guard_status_reason(status)
        js8_status_stale = bool(status.get("js8_status_stale"))
        varac_status_stale = bool(status.get("varac_status_stale"))
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
            self._hide_sidebar_schedule_controls()
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
            if js8_status_stale:
                reasons.append("Verify JS8Call")
            if varac_status_stale:
                reasons.append("Verify VarAC")
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
            if js8_status_stale:
                reasons.append("Verify JS8Call")
            if varac_status_stale:
                reasons.append("Verify VarAC")
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
        self._hide_sidebar_schedule_controls()
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
            shutdown_dependency_status_service()
        except Exception as e:
            log.debug("MainWindow shutdown: dependency status service stop failed: %s", e)
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
            self._apply_station_command_bar_layout()
        except Exception:
            pass
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
            self._off_schedule_prompt_key = None

    def _on_off_schedule_cleared(self) -> None:
        self._dismiss_off_schedule_prompt()
        try:
            self._station_command_off_schedule_by_radio = {}
        except Exception:
            pass
        try:
            self._station_command_radio_summary_signature = None
            self._refresh_station_command_bar(force=False)
        except Exception:
            pass

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
        combo.setToolTip("How long FIO should leave this radio off schedule.")
        refresh_hold_duration_combo(combo, self.settings, getattr(self, "_active_runtime_profile", None))
        combo.addItem("Indefinite", 0)
        return combo

    def _selected_prompt_hold_duration(self, combo: QComboBox | None) -> int:
        if combo is None:
            return selected_hold_duration(combo, self.settings, getattr(self, "_active_runtime_profile", None))
        try:
            data = combo.currentData()
            if data is not None and int(data) <= 0:
                return 0
        except Exception:
            pass
        return selected_hold_duration(combo, self.settings, getattr(self, "_active_runtime_profile", None))

    def _radio_name_for_device_profile_id(self, device_profile_id: int | None) -> str:
        try:
            ident = int(device_profile_id or 0)
        except Exception:
            ident = 0
        if ident <= 0:
            return "Radio"
        try:
            for snapshot in self._station_command_radio_choices():
                if self._station_command_snapshot_id(snapshot) == ident:
                    return self._station_command_snapshot_name(snapshot)
        except Exception:
            pass
        try:
            store = getattr(self, "multi_radio_store", None)
            profiles = store.list_device_profiles() if store is not None and hasattr(store, "list_device_profiles") else []
            for profile in profiles:
                if int(profile.get("id", 0) or 0) == ident:
                    name = str(profile.get("name") or "").strip()
                    if name:
                        return name
        except Exception:
            pass
        return f"Radio {ident}"

    def _attach_prompt_hold_duration_row(self, msg: QMessageBox, combo: QComboBox) -> None:
        try:
            row = QWidget(msg)
            layout = QHBoxLayout(row)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(10)
            label = QLabel("Suspend")
            label.setMinimumWidth(80)
            layout.addWidget(label)
            layout.addWidget(combo)
            layout.addStretch(1)
            msg.layout().addWidget(row, msg.layout().rowCount(), 0, 1, msg.layout().columnCount())
        except Exception:
            pass

    def _on_off_schedule_detected(self, payload: dict) -> None:
        if self._shutting_down:
            return
        items = payload.get("items") if isinstance(payload, dict) else None
        items = items if isinstance(items, list) else []
        if not items:
            return
        try:
            target_radio_id = int((payload or {}).get("device_profile_id") or 0)
        except Exception:
            target_radio_id = 0
        radio_id = target_radio_id
        try:
            entry = payload.get("entry") if isinstance(payload, Mapping) else {}
            radio_id = target_radio_id or (self._station_command_scheduler_entry_radio_id(entry) if isinstance(entry, Mapping) else 0)
            if radio_id > 0:
                state = getattr(self, "_station_command_off_schedule_by_radio", None)
                if not isinstance(state, dict):
                    state = {}
                    self._station_command_off_schedule_by_radio = state
                state[radio_id] = {"items": list(items), "entry": dict(entry)}
                self._station_command_radio_summary_signature = None
                self._refresh_station_command_bar(force=False)
        except Exception:
            pass
        target_radio_id = target_radio_id or radio_id
        prompt_key = (int(target_radio_id or 0), tuple(str(item) for item in items))
        active_key = getattr(self, "_off_schedule_prompt_key", None)
        if getattr(self, "_off_schedule_prompt", None) is not None and active_key == prompt_key:
            return
        self._dismiss_off_schedule_prompt()
        msg = QMessageBox(self)
        radio_name = self._radio_name_for_device_profile_id(target_radio_id)
        msg.setWindowTitle(f"{radio_name} Off Schedule")
        if len(items) == 1:
            text = f"{items[0]} is off schedule."
        elif len(items) == 2:
            text = f"{items[0]} and {items[1]} are off schedule."
        else:
            text = f"{', '.join(items[:-1])}, and {items[-1]} are off schedule."
        msg.setText(text)
        try:
            entry = payload.get("entry") if isinstance(payload, Mapping) else {}
            group = str((entry or {}).get("group_name") or (entry or {}).get("group") or "").strip()
            band = str((entry or {}).get("band") or "").strip()
            freq = str((entry or {}).get("frequency") or "").strip()
            scheduled_bits = [part for part in (group, band, freq) if part]
            if scheduled_bits:
                msg.setInformativeText(f"Scheduled target for {radio_name}: {' '.join(scheduled_bits)}")
        except Exception:
            pass
        apply_btn = msg.addButton("Resume Schedule", QMessageBox.AcceptRole)
        ignore_btn = msg.addButton("Skip Once", QMessageBox.RejectRole)
        suspend_btn = msg.addButton("Suspend", QMessageBox.DestructiveRole)
        hold_combo = self._build_prompt_hold_duration_combo(msg)
        self._attach_prompt_hold_duration_row(msg, hold_combo)
        self._off_schedule_prompt = msg
        self._off_schedule_prompt_key = prompt_key
        auto_applied = {"done": False}

        def _auto_apply():
            if auto_applied["done"]:
                return
            auto_applied["done"] = True
            try:
                self.scheduler.resolve_off_schedule("apply", items=items, target_device_profile_id=target_radio_id)
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
                self.scheduler.resolve_off_schedule("apply", items=items, target_device_profile_id=target_radio_id)
            except Exception:
                pass
        elif clicked == ignore_btn:
            try:
                self.scheduler.resolve_off_schedule("ignore", items=items, target_device_profile_id=target_radio_id)
            except Exception:
                pass
        elif clicked == suspend_btn:
            try:
                mins = self._selected_prompt_hold_duration(hold_combo)
                if mins > 0:
                    set_hold_duration_default(self.settings, mins)
                    self._sync_hold_duration_combos()
                self.scheduler.resolve_off_schedule(
                    "suspend",
                    items=items,
                    minutes=mins,
                    target_device_profile_id=target_radio_id,
                )
                self.on_hold_state_changed(force_reload=True)
            except Exception:
                pass
        self._off_schedule_prompt = None
        self._off_schedule_prompt_key = None

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

    def open_settings_section(
        self,
        health_key: str = "freqinout",
        radio_id: int | None = None,
        *,
        settings_nav_context: str | None = None,
    ) -> None:
        idx = self._screen_index_by_label.get("Settings", -1)
        if idx < 0:
            return
        context = str(settings_nav_context or "").strip().lower()
        if not context:
            target = str(health_key or "").strip().lower()
            main_targets = {
                "operator_info",
                "freqinout",
                "operating_groups",
                "local_comms",
                "message_auth",
                "sop_export",
                "logging",
            }
            context = "main" if target in main_targets else "radios"
        self._settings_nav_context = "main" if context == "main" else "radios"
        self._set_screen(idx)
        if hasattr(self.settings_tab, "show_settings_context"):
            QTimer.singleShot(
                0,
                lambda key=str(health_key or "freqinout"), ident=radio_id, ctx=self._settings_nav_context: self.settings_tab.show_settings_context(
                    ctx,
                    health_key=key,
                    radio_id=ident,
                ),
            )
        elif hasattr(self.settings_tab, "focus_section_by_health_key"):
            QTimer.singleShot(
                0,
                lambda key=str(health_key or "freqinout"), ident=radio_id: self.settings_tab.focus_section_by_health_key(
                    key,
                    ident,
                ),
            )

    def _open_station_health_runtime_source_related_view(self, payload: object) -> None:
        if not isinstance(payload, Mapping):
            return
        health_key = str(payload.get("health_key", "") or "radio_profiles").strip().lower()
        context = str(payload.get("settings_nav_context", "") or "radios").strip().lower()
        radio_id = self._station_health_runtime_payload_radio_id(payload)
        self.open_settings_section(
            health_key=health_key,
            radio_id=radio_id,
            settings_nav_context="main" if context in {"main", "global"} else "radios",
        )

    def _station_health_runtime_payload_radio_id(self, payload: Mapping[str, object]) -> int | None:
        raw_radio_id = payload.get("radio_id")
        try:
            if raw_radio_id not in (None, ""):
                ident = int(raw_radio_id)
                if ident > 0:
                    return ident
        except Exception:
            pass
        label = str(raw_radio_id or "").strip().lower()
        if not label:
            return None
        try:
            profiles = list(self.multi_radio_store.list_device_profiles())
        except Exception:
            profiles = []
        for profile in profiles:
            if not isinstance(profile, Mapping):
                continue
            try:
                ident = int(profile.get("id", 0) or 0)
            except Exception:
                ident = 0
            name = str(profile.get("name", "") or "").strip().lower()
            if ident > 0 and name and name == label:
                return ident
        return None

    def open_messages_section(self, mode: str = "inbox") -> None:
        idx = self._screen_index_by_label.get("Messages", -1)
        if idx < 0:
            return
        mode_key = str(mode or "inbox").strip().lower()
        self._messages_nav_context = "compose" if mode_key == "compose" else "inbox"
        self._set_screen(idx)
        QTimer.singleShot(0, self._apply_messages_nav_context)

    def open_spotter_map(self) -> None:
        idx = self._screen_index_by_label.get("Map", -1)
        if idx < 0 or self._screen_is_runtime_suppressed("Map"):
            return
        tab = getattr(self, "stations_map_tab", None)
        if tab is not None:
            focus = getattr(tab, "focus_hf_reports", None) or getattr(tab, "focus_spotter_reports", None)
            if callable(focus):
                QTimer.singleShot(0, focus)
                QTimer.singleShot(0, self._sync_map_filters_from_tab)
        self._set_screen(idx)
        try:
            self._sync_map_filters_from_tab()
        except Exception:
            pass

    def open_local_reports_map(self) -> None:
        idx = self._screen_index_by_label.get("Map", -1)
        if idx < 0 or self._screen_is_runtime_suppressed("Map"):
            return
        tab = getattr(self, "stations_map_tab", None)
        if tab is not None and hasattr(tab, "focus_local_reports"):
            QTimer.singleShot(0, tab.focus_local_reports)
            QTimer.singleShot(0, self._sync_map_filters_from_tab)
        self._set_screen(idx)
        try:
            self._sync_map_filters_from_tab()
        except Exception:
            pass

    def open_local_reports(self, callsign: str = "") -> None:
        idx = self._screen_index_by_label.get("Local Reports", -1)
        if idx < 0:
            return
        self._set_screen(idx)
        tab = getattr(self, "local_report_history_tab", None)
        if tab is not None and hasattr(tab, "show_callsign"):
            QTimer.singleShot(0, lambda cs=str(callsign or "").strip().upper(): tab.show_callsign(cs))

    def _apply_messages_nav_context(self) -> None:
        tab = getattr(self, "message_viewer_tab", None)
        if tab is None:
            try:
                current = self.stack.currentWidget()
                if isinstance(current, MessageViewerTab):
                    tab = current
            except Exception:
                tab = None
        if tab is None:
            return
        mode = str(getattr(self, "_messages_nav_context", "inbox") or "inbox").strip().lower()
        try:
            if mode == "compose" and hasattr(tab, "show_compose_from_navigation"):
                tab.show_compose_from_navigation()
            elif hasattr(tab, "show_inbox_from_navigation"):
                tab.show_inbox_from_navigation()
        except Exception:
            pass

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

    def _on_station_command_health_clicked(self, event=None, *, anchor: QWidget | None = None) -> None:
        try:
            ident = int(getattr(self, "_station_command_selected_profile_id", 0) or 0)
        except Exception:
            ident = 0
        self._show_station_command_health_menu(device_profile_id=ident, anchor=anchor)
        try:
            if event is not None:
                event.accept()
        except Exception:
            pass

    @staticmethod
    def _quick_search_blob(record: Mapping[str, object]) -> str:
        return " ".join(
            str(record.get(key, "") or "")
            for key in ("title", "subtitle", "category", "keywords")
        ).upper()

    def _quick_search_add_record(
        self,
        records: list[dict[str, object]],
        *,
        category: str,
        title: str,
        subtitle: str = "",
        screen: str = "",
        action: str = "screen",
        keywords: str = "",
        section_key: str = "",
        message_mode: str = "",
        radio_id: int = 0,
        detail_text: str = "",
    ) -> None:
        clean_title = str(title or "").strip()
        if not clean_title:
            return
        records.append(
            {
                "category": str(category or "Result"),
                "title": clean_title,
                "subtitle": str(subtitle or "").strip(),
                "screen": str(screen or "").strip(),
                "action": str(action or "screen").strip(),
                "keywords": str(keywords or "").strip(),
                "section_key": str(section_key or "").strip(),
                "message_mode": str(message_mode or "").strip(),
                "radio_id": int(radio_id or 0),
                "detail_text": str(detail_text or "").strip(),
            }
        )

    def _build_quick_search_records(self) -> list[dict[str, object]]:
        now = time.time()
        cached_ts, cached_records = getattr(self, "_quick_search_cache", (0.0, []))
        if cached_records and (now - float(cached_ts or 0.0)) < 5.0:
            return [dict(row) for row in cached_records]
        records: list[dict[str, object]] = []
        for button_label, screen_label in getattr(self, "_nav_specs", []) or []:
            label = str(button_label or screen_label or "").strip()
            screen = str(screen_label or label).strip()
            if not label or self._screen_is_runtime_suppressed(screen):
                continue
            action = "screen"
            message_mode = ""
            section_key = ""
            if screen == "Settings":
                action = "settings"
                section_key = "radio_profiles" if label == "Radios" else "operator_info"
            elif screen == "Messages":
                action = "messages"
                message_mode = "compose" if label == "Compose" else "inbox"
            self._quick_search_add_record(
                records,
                category="Go To",
                title=label,
                subtitle=screen,
                screen=screen,
                action=action,
                section_key=section_key,
                message_mode=message_mode,
                keywords=f"{label} {screen} tab view navigation",
            )
        for title, section, keywords in (
            ("RF Guard", "radio_profiles", "safety antenna band overlap advanced frequency guard radio settings"),
            ("Operating Groups", "hf_operating_groups", "hf magnet ghostnet js8call wefax groups frequencies"),
            ("Local Groups", "local_comms_groups", "repeaters local comms groups"),
            ("Schedule Assignment", "radio_profiles", "assign frequency plan schedule radio rf guard"),
            ("Message Compose", "", "compose message outbound inbox js8 flmsg varac"),
        ):
            self._quick_search_add_record(
                records,
                category="Action" if title == "Message Compose" else "Settings",
                title=title,
                subtitle="Messages" if title == "Message Compose" else "Open related settings",
                screen="Messages" if title == "Message Compose" else "Settings",
                action="messages" if title == "Message Compose" else "settings",
                section_key=section,
                message_mode="compose" if title == "Message Compose" else "",
                keywords=keywords,
            )
        try:
            for profile in self.multi_radio_store.list_device_profiles():
                ident = int(profile.get("id", 0) or 0)
                backend = str(profile.get("control_backend", "") or "").strip()
                device_class = str(profile.get("device_class", "") or "").strip()
                assignment = str(profile.get("assigned_operating_profile_name", "") or "").strip()
                subtitle = " ".join(part for part in (device_class, backend, assignment) if part)
                self._quick_search_add_record(
                    records,
                    category="Radio",
                    title=str(profile.get("name") or f"Radio {ident}"),
                    subtitle=subtitle or "Radio profile",
                    screen="Settings",
                    action="settings",
                    section_key="radio_profiles",
                    radio_id=ident,
                    keywords=f"{backend} {device_class} {assignment} flrig rigctld js8call sdr",
                )
        except Exception:
            pass
        try:
            for plan in self.multi_radio_store.list_frequency_plans():
                category = str(plan.get("category", "") or "normal").strip()
                target_screen = "FreqPlanner"
                if category == "hf_daily_schedule":
                    target_screen = "HF Schedule"
                elif category == "hf_net_schedule":
                    target_screen = "Net Schedule"
                self._quick_search_add_record(
                    records,
                    category="Schedule",
                    title=str(plan.get("name") or "Frequency Plan"),
                    subtitle=category.replace("_", " "),
                    screen=target_screen,
                    action="screen",
                    keywords=f"{plan.get('source_refs_json', '')} {plan.get('group_refs_json', '')} {plan.get('frequency_refs_json', '')}",
                )
        except Exception:
            pass
        try:
            for row in load_operating_groups(self.settings):
                group = str(row.get("group") or row.get("group_name") or "").strip()
                band = str(row.get("band") or "").strip()
                freq = str(row.get("frequency") or row.get("freq") or "").strip()
                mode = str(row.get("mode") or "").strip()
                self._quick_search_add_record(
                    records,
                    category="Frequency",
                    title=" ".join(part for part in (group, band) if part),
                    subtitle=" ".join(part for part in (freq, mode) if part),
                    screen="ControlFreq",
                    action="screen",
                    keywords=f"{group} {band} {freq} {mode} operating group qsy schedule",
                )
        except Exception:
            pass
        try:
            for archive in load_js8spotter_archive_records(limit_per_table=12)[:72]:
                table = str(archive.source_table or "").strip().lower()
                if table in {"grid", "signal", "activity", "csstatrep"}:
                    screen = "HF Operators"
                    keywords = f"{archive.keywords} js8spotter spotter history traffic map operator callsign grid"
                else:
                    screen = "Messages"
                    keywords = f"{archive.keywords} js8spotter spotter history messages expect alert profile search"
                source_label = archive.source_db
                try:
                    if source_label:
                        source_label = Path(source_label).name
                except Exception:
                    pass
                detail_lines = [
                    "JS8Spotter History",
                    "",
                    f"Type: {archive.source_table}",
                    f"Source ID: {archive.source_id}",
                    f"Source DB: {archive.source_db or 'Unknown'}",
                    f"Imported: {datetime.datetime.fromtimestamp(archive.imported_ts).strftime('%Y-%m-%d %H:%M') if archive.imported_ts else 'Unknown'}",
                    "",
                    "Summary:",
                    archive.subtitle or archive.title,
                    "",
                    "Archived Payload:",
                    json.dumps(archive.payload, indent=2, sort_keys=True, default=str),
                ]
                self._quick_search_add_record(
                    records,
                    category="Spotter History",
                    title=archive.title,
                    subtitle=" | ".join(part for part in (f"{table}: {archive.subtitle}" if archive.subtitle else table, source_label) if part),
                    screen=screen,
                    action="spotter_archive_detail",
                    keywords=keywords,
                    detail_text="\n".join(detail_lines),
                )
        except Exception:
            pass
        try:
            report = build_station_readiness_report(
                self.settings.all(),
                device_profiles=self.multi_radio_store.list_device_profiles(),
                operating_groups=load_operating_groups(self.settings),
            )
            for issue in list(getattr(report, "issues", ()) or ())[:20]:
                self._quick_search_add_record(
                    records,
                    category="Issue",
                    title=format_readiness_issue(issue),
                    subtitle="Station Health / Settings",
                    screen="Station Health",
                    action="health",
                    section_key=str(getattr(issue, "section_key", "") or ""),
                    radio_id=int(getattr(issue, "radio_id", 0) or 0),
                    keywords=f"{getattr(issue, 'severity', '')} {getattr(issue, 'scope', '')}",
                )
        except Exception:
            pass
        self._quick_search_cache = (now, [dict(row) for row in records])
        return records

    def quick_search(self, query: str, *, limit: int = 16) -> list[dict[str, object]]:
        terms = [part.strip().upper() for part in str(query or "").split() if part.strip()]
        if not terms:
            return []
        matches: list[tuple[int, int, dict[str, object]]] = []
        for idx, record in enumerate(self._build_quick_search_records()):
            blob = self._quick_search_blob(record)
            if not all(term in blob for term in terms):
                continue
            title = str(record.get("title", "") or "").upper()
            category = str(record.get("category", "") or "")
            score = 100 if title == " ".join(terms) else 80 if title.startswith(terms[0]) else 50
            if category in {"Issue", "Action", "Radio"}:
                score += 10
            matches.append((score, idx, record))
        matches.sort(key=lambda item: (-item[0], item[1]))
        return [dict(record) for _score, _idx, record in matches[: max(1, int(limit or 16))]]

    def _activate_quick_search_result(self, record: Mapping[str, object]) -> None:
        action = str(record.get("action", "screen") or "screen").strip()
        screen = str(record.get("screen", "") or "").strip()
        if action == "settings":
            self.open_settings_section(
                str(record.get("section_key", "") or "operator_info"),
                radio_id=int(record.get("radio_id", 0) or 0),
            )
            return
        if action == "messages":
            self.open_messages_mode(str(record.get("message_mode", "") or "inbox"))
            return
        if action == "health":
            self._open_station_health_detail(
                device_profile_id=int(record.get("radio_id", 0) or 0),
                scope_name=str(record.get("section_key", "") or ""),
            )
            return
        if action == "spotter_archive_detail":
            detail = str(record.get("detail_text", "") or "").strip()
            if detail:
                QMessageBox.information(self, str(record.get("title", "") or "Spotter History"), detail)
                return
        idx = self._screen_index_by_label.get(screen, -1)
        if idx >= 0:
            self._set_screen(idx)

    def show_quick_search_results(self, query: str, anchor: QWidget | None = None) -> bool:
        results = self.quick_search(query)
        if not results:
            try:
                service = getattr(self, "action_feedback_service", None)
                if service is not None:
                    service.publish(
                        scope="global",
                        status="info",
                        summary=f"No FIO results for '{str(query or '').strip()}'.",
                        detail="Try a radio name, schedule, group, frequency, setting, message action, or setup issue.",
                        source_surface="quick_search",
                    )
            except Exception:
                pass
            return False
        anchor_widget = anchor or self
        menu = QMenu(anchor_widget)
        last_category = ""
        for record in results:
            category = str(record.get("category", "Result") or "Result")
            if category != last_category:
                header = QAction(category, menu)
                header.setEnabled(False)
                menu.addAction(header)
                last_category = category
            title = str(record.get("title", "") or "")
            subtitle = str(record.get("subtitle", "") or "")
            action = QAction(f"{title}  -  {subtitle}" if subtitle else title, menu)
            action.setData(dict(record))
            menu.addAction(action)
        selected = menu.exec(anchor_widget.mapToGlobal(anchor_widget.rect().bottomLeft()))
        if selected is None or not selected.isEnabled():
            return True
        data = selected.data()
        if isinstance(data, Mapping):
            self._activate_quick_search_result(data)
        return True

    def _style_station_command_bar(self, theme: dict) -> None:
        if not hasattr(self, "station_command_bar"):
            return
        border = theme.get("station_control_border", theme.get("accent", "#2E6F9E"))
        surface = theme.get("station_control_surface", theme.get("surface_alt", "#F6F8FA"))
        text = theme.get("station_control_text", theme.get("text", "#222222"))
        muted = theme.get("station_control_muted", theme.get("text_muted", "#6A737D"))
        tile_surface = theme.get("station_control_tile_surface", theme.get("surface", "#FFFFFF"))
        tile_selected_surface = theme.get("station_control_tile_selected_surface", theme.get("surface", "#FFFFFF"))
        tile_border = theme.get("station_control_tile_border", theme.get("border", "#D3D7DD"))
        tile_selected_border = theme.get("station_control_tile_selected_border", border)
        chevron_path = (Path(__file__).resolve().parents[2] / "assets" / "dropdown-chevron.svg").as_posix()
        station_command_bar_style = (
            "QFrame#stationCommandBar {"
            f"background: {surface}; border: 1px solid {border}; border-bottom: 2px solid {border}; border-radius: 6px;"
            "}"
            "QFrame#stationCommandBar QLabel {"
            f"background: transparent; color: {text};"
            "}"
            "QFrame#stationCommandRadioTile {"
            f"background: {tile_surface}; border: 1px solid {tile_border}; border-radius: 6px;"
            "}"
            "QFrame#stationCommandRadioTile[selected=\"true\"] {"
            f"background: {tile_selected_surface}; border: 2px solid {tile_selected_border};"
            "}"
            "QLabel#stationCommandRadioTileNow {"
            f"color: {text}; font-weight: 800;"
            "}"
            "QLabel#stationCommandRadioTileState, QLabel#stationCommandRadioTileNext {"
            f"color: {muted};"
            "}"
            "QPushButton#stationCommandRadioTileHealth {"
            "padding-left: 8px; padding-right: 8px;"
            "}"
            "QComboBox#stationCommandRadioTileFrequency {"
            f"background: {theme.get('surface', '#FFFFFF')}; color: {text};"
            f"border: 1px solid {tile_border}; border-radius: 5px; padding: 4px 34px 4px 8px; font-weight: 800;"
            "}"
            "QComboBox#stationCommandRadioTileFrequency::drop-down {"
            f"border-left: 1px solid {tile_border}; width: 28px; subcontrol-origin: padding; subcontrol-position: top right;"
            "}"
            "QComboBox#stationCommandRadioTileFrequency::down-arrow {"
            f"image: url({chevron_path}); width: 12px; height: 8px;"
            "}"
            "QComboBox#stationCommandRadioTileDuration {"
            f"background: {theme.get('surface', '#FFFFFF')}; color: {muted};"
            f"border: 1px solid {tile_border}; border-radius: 5px; padding: 4px 24px 4px 8px;"
            "}"
            "QComboBox#stationCommandRadioTileDuration::drop-down {"
            "border: none; width: 22px;"
            "}"
            "QToolButton#stationCommandRadioTileTimedSuspend::menu-button,"
            "QToolButton#stationCommandRadioTileSchedulerSuspend::menu-button {"
            f"border-left: 1px solid {tile_border}; width: 16px;"
            "}"
            "QToolButton#stationCommandRadioTileTimedSuspend::menu-indicator,"
            "QToolButton#stationCommandRadioTileSchedulerSuspend::menu-indicator {"
            f"image: url({chevron_path}); width: 8px; height: 6px; right: 4px;"
            "}"
        )
        if station_command_bar_style != getattr(self, "_station_command_bar_style_signature", ""):
            self.station_command_bar.setStyleSheet(station_command_bar_style)
            self._station_command_bar_style_signature = station_command_bar_style
        for label in (
            getattr(self, "station_command_radio_label", None),
            getattr(self, "station_command_now_caption", None),
            getattr(self, "station_command_action_label", None),
            getattr(self, "station_command_state_label", None),
            getattr(self, "station_command_next_label", None),
            getattr(self, "station_command_health_label", None),
            getattr(self, "station_command_radio_summary_label", None),
        ):
            if label is not None:
                label.setStyleSheet(f"background: transparent; color: {text}; font-weight: 600;")
        for label in getattr(self, "station_command_health_text_labels", {}).values():
            try:
                label.setStyleSheet(f"background: transparent; color: {text};")
            except Exception:
                pass
        if getattr(self, "station_command_now_label", None) is not None:
            self.station_command_now_label.setStyleSheet(
                "QLabel#stationCommandNow {"
                f"background: {theme.get('surface', '#FFFFFF')}; color: {text};"
                f"border: 1px solid {border}; border-radius: 5px; padding: 4px 10px; font-weight: 800;"
                "font-size: 20px;"
                "}"
            )
        if getattr(self, "station_command_radio_combo", None) is not None:
            self.station_command_radio_combo.setStyleSheet(
                "QComboBox#stationCommandRadioSelector {"
                f"background: {theme.get('surface', '#FFFFFF')}; color: {text};"
                f"border: 1px solid {border}; border-radius: 5px; padding: 4px 28px 4px 10px; font-weight: 800;"
                "font-size: 20px;"
                "}"
                "QComboBox#stationCommandRadioSelector::drop-down {"
                "border: none; width: 24px;"
                "}"
                "QComboBox#stationCommandRadioSelector QAbstractItemView {"
                f"background: {theme.get('surface', '#FFFFFF')}; color: {text}; selection-background-color: {border};"
                "}"
            )
        if getattr(self, "station_command_duration_combo", None) is not None:
            self.station_command_duration_combo.setStyleSheet(f"color: {muted};")
        if getattr(self, "station_command_radio_summary_scroll", None) is not None:
            self.station_command_radio_summary_scroll.setStyleSheet(
                "QScrollArea#stationCommandRadioSummaryScroll {background: transparent; border: none;}"
                "QWidget#stationCommandRadioSummary {background: transparent;}"
            )
        if getattr(self, "station_command_radio_admin_panel", None) is not None:
            self.station_command_radio_admin_panel.setStyleSheet("QWidget#stationCommandRadioAdminPanel {background: transparent;}")
        manual_qsy_active = self._station_command_scheduler_manual_qsy_active()
        scheduler_suspended_manual = self._station_command_scheduler_suspended_manually()
        try:
            timed_hold_active = bool((getattr(self, "_hold_state_snapshot", None) or {}).get("active"))
        except Exception:
            timed_hold_active = False
        button_roles = (
            (getattr(self, "station_command_qsy_btn", None), "warning" if manual_qsy_active else "info"),
            (getattr(self, "station_command_hold_btn", None), "warning" if timed_hold_active else "info"),
            (getattr(self, "station_command_suspend_btn", None), "warning" if scheduler_suspended_manual else "muted"),
            (
                getattr(self, "station_command_resume_btn", None),
                "warning" if manual_qsy_active or scheduler_suspended_manual or timed_hold_active else "muted",
            ),
            (
                getattr(self, "station_command_radio_admin_btn", None),
                "info" if bool(getattr(self, "_station_command_radio_admin_expanded", False)) else "muted",
            ),
            (getattr(self, "station_command_radio_prev_btn", None), "muted"),
            (getattr(self, "station_command_radio_next_btn", None), "muted"),
        )
        for btn, role in button_roles:
            if btn is not None:
                btn.setStyleSheet(button_style(role, theme) if btn.isEnabled() else button_style("muted", theme) + f" color: {muted};")
        for sep in (
            getattr(self, "station_command_radio_separator", None),
            getattr(self, "station_command_now_separator", None),
        ):
            if sep is not None:
                sep.setStyleSheet(f"color: {border}; background: transparent;")

    def _station_command_layout_mode_for_width(self, width: int) -> str:
        try:
            return "compact" if int(width) < 1100 else "wide"
        except Exception:
            return "wide"

    def _apply_station_command_bar_layout(self, *, force: bool = False) -> None:
        if not hasattr(self, "station_command_layout"):
            return
        width = int(getattr(self.station_command_bar, "width", lambda: 0)() or self.width() or 0)
        mode = self._station_command_layout_mode_for_width(width)
        if not force and mode == getattr(self, "_station_command_layout_mode", ""):
            return
        self._station_command_layout_mode = mode
        layout = self.station_command_layout
        for widget in (
            self.station_command_radio_label,
            self.station_command_radio_combo,
            getattr(self, "station_command_radio_separator", None),
            getattr(self, "station_command_now_caption", None),
            self.station_command_now_label,
            getattr(self, "station_command_freq_combo", None),
            self.station_command_state_label,
            getattr(self, "station_command_now_separator", None),
            getattr(self, "station_command_action_label", None),
            self.station_command_next_label,
            getattr(self, "station_command_health_label", None),
            getattr(self, "station_command_health_widget", None),
            self.station_command_duration_combo,
            self.station_command_qsy_btn,
            self.station_command_hold_btn,
            self.station_command_suspend_btn,
            self.station_command_resume_btn,
            getattr(self, "station_command_radio_summary_label", None),
            getattr(self, "station_command_radio_prev_btn", None),
            getattr(self, "station_command_radio_summary_scroll", None),
            getattr(self, "station_command_radio_next_btn", None),
            getattr(self, "station_command_radio_admin_btn", None),
            getattr(self, "station_command_radio_admin_panel", None),
        ):
            if widget is not None:
                layout.removeWidget(widget)
        for col in range(16):
            layout.setColumnStretch(col, 0)

        if mode == "compact":
            self.station_command_now_label.setWordWrap(False)
            self.station_command_state_label.setWordWrap(False)
            self.station_command_next_label.setWordWrap(False)
            self.station_command_radio_separator.setVisible(False)
            self.station_command_now_separator.setVisible(False)
            layout.addWidget(self.station_command_radio_label, 0, 0)
            layout.addWidget(self.station_command_radio_combo, 0, 1)
            layout.addWidget(self.station_command_now_caption, 0, 2)
            layout.addWidget(self.station_command_now_label, 0, 3)
            layout.addWidget(self.station_command_state_label, 0, 4)
            layout.addWidget(self.station_command_action_label, 1, 0)
            layout.addWidget(self.station_command_freq_combo, 1, 1, 1, 2)
            layout.addWidget(self.station_command_qsy_btn, 1, 3)
            layout.addWidget(self.station_command_suspend_btn, 1, 4)
            layout.addWidget(self.station_command_duration_combo, 2, 1)
            layout.addWidget(self.station_command_hold_btn, 2, 3)
            layout.addWidget(self.station_command_resume_btn, 2, 4)
            if hasattr(self, "station_command_health_label") and hasattr(self, "station_command_health_widget"):
                layout.addWidget(self.station_command_health_label, 3, 0)
                layout.addWidget(self.station_command_health_widget, 3, 1)
            layout.addWidget(self.station_command_next_label, 3, 2, 1, 3)
            if hasattr(self, "station_command_radio_summary_label") and hasattr(self, "station_command_radio_summary_scroll"):
                layout.addWidget(self.station_command_radio_summary_label, 4, 0)
                layout.addWidget(self.station_command_radio_summary_scroll, 4, 1, 1, 4)
            if hasattr(self, "station_command_radio_admin_btn"):
                layout.addWidget(self.station_command_radio_admin_btn, 4, 5)
            if hasattr(self, "station_command_radio_admin_panel"):
                layout.addWidget(self.station_command_radio_admin_panel, 5, 0, 1, 5)
            layout.setColumnStretch(4, 1)
        else:
            self.station_command_radio_separator.setVisible(True)
            self.station_command_now_separator.setVisible(True)
            layout.addWidget(self.station_command_radio_label, 0, 0)
            layout.addWidget(self.station_command_radio_combo, 0, 1)
            layout.addWidget(self.station_command_radio_separator, 0, 2, 1, 1)
            layout.addWidget(self.station_command_now_caption, 0, 3)
            layout.addWidget(self.station_command_now_label, 0, 4)
            layout.addWidget(self.station_command_state_label, 0, 5)
            layout.addWidget(self.station_command_now_separator, 0, 6, 1, 1)
            layout.addWidget(self.station_command_action_label, 0, 7)
            layout.addWidget(self.station_command_freq_combo, 0, 8, 1, 2)
            layout.addWidget(self.station_command_qsy_btn, 0, 10)
            layout.addWidget(self.station_command_suspend_btn, 0, 12)
            layout.addWidget(self.station_command_duration_combo, 1, 8, 1, 2)
            layout.addWidget(self.station_command_hold_btn, 1, 10)
            layout.addWidget(self.station_command_resume_btn, 1, 12)
            if hasattr(self, "station_command_health_label") and hasattr(self, "station_command_health_widget"):
                layout.addWidget(self.station_command_health_label, 1, 0)
                layout.addWidget(self.station_command_health_widget, 1, 1)
            layout.addWidget(self.station_command_next_label, 1, 3, 1, 7)
            if hasattr(self, "station_command_radio_summary_label") and hasattr(self, "station_command_radio_summary_scroll"):
                layout.addWidget(self.station_command_radio_summary_label, 2, 0)
                layout.addWidget(self.station_command_radio_summary_scroll, 2, 1, 1, 11)
            if hasattr(self, "station_command_radio_admin_btn"):
                layout.addWidget(self.station_command_radio_admin_btn, 2, 12)
            if hasattr(self, "station_command_radio_admin_panel"):
                layout.addWidget(self.station_command_radio_admin_panel, 3, 0, 1, 13)
            layout.setColumnStretch(9, 1)
            layout.setColumnStretch(15, 2)

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
            self.local_report_history_tab,
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
            label = self._screens[idx][0] if 0 <= idx < len(self._screens) else ""
            if label == "Settings":
                nav_idx = self._settings_nav_button_indices.get(
                    str(getattr(self, "_settings_nav_context", "main") or "main")
                )
            else:
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
        if not swap_summary and restrictions == ["launch control off"]:
            return ""
        if not restrictions:
            return swap_summary
        operating_txt = operating_name or "assigned operating model"
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
        label = str(screen_label or "").strip()
        if label == "Settings":
            indices = list(getattr(self, "_settings_nav_button_indices", {}).values())
        elif label == "Messages":
            indices = list(getattr(self, "_messages_nav_button_indices", {}).values())
        else:
            indices = []
        if indices:
            for nav_idx in indices:
                if 0 <= nav_idx < len(self.nav_buttons):
                    try:
                        self.nav_buttons[nav_idx].setVisible(bool(visible))
                    except Exception:
                        pass
            return
        screen_idx = self._screen_index_by_label.get(label)
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
                    reason="Launch Control is disabled by the primary operating model.",
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
    def _station_command_value(source: object, key: str, default: object = "") -> object:
        if isinstance(source, Mapping):
            return source.get(key, default)
        return getattr(source, key, default)

    @staticmethod
    def _station_command_bool(value: object, default: bool = False) -> bool:
        if value in (None, ""):
            return default
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @staticmethod
    def _station_command_snapshot_name(snapshot: object) -> str:
        try:
            name = str(MainWindow._station_command_value(snapshot, "name", "") or "").strip()
            if name:
                return name
            ident = int(
                MainWindow._station_command_value(
                    snapshot,
                    "device_profile_id",
                    MainWindow._station_command_value(snapshot, "id", 0),
                )
                or 0
            )
            return f"Radio {ident}" if ident > 0 else "Radio"
        except Exception:
            return "Radio"

    @staticmethod
    def _station_command_snapshot_id(snapshot: object) -> int:
        try:
            return int(
                MainWindow._station_command_value(
                    snapshot,
                    "device_profile_id",
                    MainWindow._station_command_value(snapshot, "id", 0),
                )
                or 0
            )
        except Exception:
            return 0

    @staticmethod
    def _station_command_frequency_text(snapshot: object) -> str:
        try:
            freq = MainWindow._station_command_format_frequency(
                MainWindow._station_command_value(snapshot, "current_frequency_label", ""),
                suffix="",
            )
            parts = [freq]
            band = str(MainWindow._station_command_value(snapshot, "current_band", "") or "").strip()
            if band:
                parts.append(band)
            text = " ".join(part for part in parts if part).strip()
            return text or "unavailable"
        except Exception:
            return "unavailable"

    @staticmethod
    def _station_command_parse_frequency(value: object) -> float | None:
        freq = parse_frequency_mhz(value)
        if freq is not None:
            return freq
        text = str(value or "").replace("MHz", "").strip()
        return parse_frequency_mhz(text)

    @staticmethod
    def _station_command_group_display_name(group: object) -> str:
        text = str(group or "").strip().upper()
        if text == "S2 UNDERGROUND":
            return "S2/GHOSTNET"
        return text

    def _station_command_schedule_group_band(self, snapshot: object | None = None) -> tuple[str, str]:
        try:
            if snapshot is not None:
                lane_group, lane_band = self._station_command_lane_current_group_band(
                    self._station_command_snapshot_id(snapshot)
                )
                if lane_group:
                    return lane_group, lane_band
                snapshot_group = self._station_command_group_display_name(
                    self._station_command_value(snapshot, "schedule_group", "")
                )
                snapshot_band = str(
                    self._station_command_value(snapshot, "schedule_band", "")
                    or ""
                ).strip().upper()
                if snapshot_group:
                    return snapshot_group, snapshot_band
                snapshot_group = self._station_command_group_display_name(
                    self._station_command_value(snapshot, "current_group", "")
                    or self._station_command_value(snapshot, "group", "")
                    or self._station_command_value(snapshot, "group_name", "")
                )
                snapshot_band = str(
                    self._station_command_value(snapshot, "current_band", "")
                    or self._station_command_value(snapshot, "band", "")
                    or ""
                ).strip().upper()
                if snapshot_group:
                    return snapshot_group, snapshot_band
            sched = getattr(self, "scheduler", None)
            entry = getattr(sched, "current_schedule_entry", {}) if sched is not None else {}
            if isinstance(entry, Mapping):
                source = str(getattr(sched, "current_source", "") or "").strip().upper()
                selected_radio_id = self._station_command_snapshot_id(snapshot) if snapshot is not None else 0
                if selected_radio_id > 0:
                    scheduler_radio_id = self._station_command_scheduler_entry_radio_id(entry)
                    if scheduler_radio_id <= 0 and sched is not None:
                        target_getter = getattr(sched, "_primary_manual_control_radio_id", None)
                        if callable(target_getter):
                            try:
                                scheduler_radio_id = int(target_getter() or 0)
                            except Exception:
                                scheduler_radio_id = 0
                    if scheduler_radio_id <= 0:
                        return "", ""
                    if scheduler_radio_id != selected_radio_id:
                        return "", ""
                current_raw = self._station_command_value(snapshot, "current_frequency_label", "") if snapshot is not None else ""
                entry_freq = self._station_command_parse_frequency(entry.get("frequency"))
                current_freq = (
                    self._station_command_parse_frequency(current_raw)
                    if snapshot is not None
                    else None
                )
                if source != "QSY" and snapshot is not None and not str(current_raw or "").strip():
                    return "", ""
                if (
                    source != "QSY"
                    and entry_freq is not None
                    and current_freq is not None
                    and abs(float(entry_freq) - float(current_freq)) > 0.0005
                ):
                    return "", ""
                group = self._station_command_group_display_name(entry.get("group"))
                band = str(entry.get("band") or "").strip().upper()
                if group or band:
                    return group, band
        except Exception:
            pass
        return "", ""

    @staticmethod
    def _station_command_parse_json_list(value: object) -> list[dict[str, object]]:
        if isinstance(value, list):
            raw = value
        else:
            try:
                raw = json.loads(str(value or "[]"))
            except Exception:
                raw = []
        return [dict(item) for item in raw if isinstance(item, Mapping)]

    @staticmethod
    def _station_command_parse_json_ref_items(value: object) -> list[object]:
        if isinstance(value, list):
            raw = value
        else:
            try:
                raw = json.loads(str(value or "[]"))
            except Exception:
                raw = []
        refs: list[object] = []
        for item in raw:
            if isinstance(item, Mapping):
                refs.append(dict(item))
            elif isinstance(item, str):
                text = item.strip()
                if text:
                    refs.append(text)
        return refs

    @staticmethod
    def _station_command_hhmm_to_minutes(value: object) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            if ":" in text:
                hour_text, minute_text = text.split(":", 1)
            elif len(text) in {3, 4} and text.isdigit():
                hour_text, minute_text = text[:-2], text[-2:]
            else:
                return None
            hour = int(hour_text)
            minute = int(minute_text)
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                return None
            return hour * 60 + minute
        except Exception:
            return None

    @staticmethod
    def _station_command_day_name(value: object) -> str:
        text = str(value or "").strip().upper()
        aliases = {
            "SUN": "SUNDAY",
            "MON": "MONDAY",
            "TUE": "TUESDAY",
            "TUES": "TUESDAY",
            "WED": "WEDNESDAY",
            "THU": "THURSDAY",
            "THUR": "THURSDAY",
            "THURS": "THURSDAY",
            "FRI": "FRIDAY",
            "SAT": "SATURDAY",
        }
        return aliases.get(text, text)

    @staticmethod
    def _station_command_next_day_name(day: str) -> str:
        days = ("SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY")
        try:
            return days[(days.index(day) + 1) % len(days)]
        except ValueError:
            return "SUNDAY"

    @staticmethod
    def _station_command_day_index(day: str) -> int | None:
        days = ("SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY")
        try:
            return days.index(day)
        except ValueError:
            return None

    @classmethod
    def _station_command_ref_active_now(cls, ref: Mapping[str, object], now_utc: datetime.datetime) -> bool:
        start = cls._station_command_hhmm_to_minutes(
            ref.get("start_utc") or ref.get("start") or ref.get("start_local")
        )
        end = cls._station_command_hhmm_to_minutes(
            ref.get("end_utc") or ref.get("end") or ref.get("end_local")
        )
        if start is None or end is None:
            return False
        today = now_utc.strftime("%A").upper()
        row_day = cls._station_command_day_name(ref.get("day_utc") or ref.get("day") or "ALL")
        minute = now_utc.hour * 60 + now_utc.minute
        if row_day == "ALL":
            return start <= minute < end if start <= end else (minute >= start or minute < end)
        if start <= end:
            return row_day == today and start <= minute < end
        return (row_day == today and minute >= start) or (
            cls._station_command_next_day_name(row_day) == today and minute < end
        )

    @classmethod
    def _station_command_ref_start_delta_minutes(
        cls,
        ref: Mapping[str, object],
        now_utc: datetime.datetime,
    ) -> int | None:
        start = cls._station_command_hhmm_to_minutes(
            ref.get("start_utc") or ref.get("start") or ref.get("start_local")
        )
        if start is None:
            return None
        row_day = cls._station_command_day_name(ref.get("day_utc") or ref.get("day") or "ALL")
        minute = now_utc.hour * 60 + now_utc.minute
        if row_day == "ALL":
            delta = start - minute
            return delta if delta >= 0 else delta + 1440
        row_index = cls._station_command_day_index(row_day)
        if row_index is None:
            return None
        today_index = int(now_utc.strftime("%w"))
        days_until = (row_index - today_index) % 7
        delta = days_until * 1440 + start - minute
        return delta if delta >= 0 else delta + (7 * 1440)

    def _station_command_assigned_plan_group_band(self, snapshot: object) -> tuple[str, str]:
        ident = self._station_command_snapshot_id(snapshot)
        if ident <= 0:
            return "", ""
        refs = self._station_command_assigned_plan_refs_for_radio(ident)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        for ref in refs:
            if not self._station_command_ref_active_now(ref, now_utc):
                continue
            group = self._station_command_group_display_name(ref.get("group_name") or ref.get("group"))
            band = str(ref.get("band") or "").strip().upper()
            if group:
                return group, band
        if refs:
            for ref in refs:
                group = self._station_command_group_display_name(ref.get("group_name") or ref.get("group"))
                band = str(ref.get("band") or "").strip().upper()
                if group:
                    return group, band
            return "", ""
        lane_group, lane_band = self._station_command_lane_current_group_band(ident)
        if lane_group:
            return lane_group, lane_band
        return "", ""

    def _station_command_assigned_plan_next_group_band(self, snapshot: object) -> tuple[str, str]:
        ident = self._station_command_snapshot_id(snapshot)
        if ident <= 0:
            return "", ""
        refs = self._station_command_assigned_plan_refs_for_radio(ident)
        if not refs:
            lane_group, lane_band = self._station_command_lane_next_group_band(ident)
            if lane_group:
                return lane_group, lane_band
            return "", ""
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        current_group, current_band = self._station_command_assigned_plan_group_band(snapshot)
        current_key = (current_group.strip().upper(), current_band.strip().upper())
        best_changed: tuple[int, str, str] | None = None
        best_any: tuple[int, str, str] | None = None
        for ref in refs:
            group = self._station_command_group_display_name(ref.get("group_name") or ref.get("group"))
            band = str(ref.get("band") or "").strip().upper()
            if not group:
                continue
            delta = self._station_command_ref_start_delta_minutes(ref, now_utc)
            if delta is None:
                continue
            candidate = (delta, group, band)
            if best_any is None or candidate[0] < best_any[0]:
                best_any = candidate
            if (group.strip().upper(), band.strip().upper()) != current_key:
                if best_changed is None or candidate[0] < best_changed[0]:
                    best_changed = candidate
        best = best_changed or best_any
        if best is None:
            return "", ""
        return best[1], best[2]

    def _station_command_plan_cache(self) -> tuple[dict[int, dict[str, object]], dict[int, dict[str, object]]]:
        now = time.monotonic()
        cache = getattr(self, "_station_command_plan_cache_data", None)
        expires = float(getattr(self, "_station_command_plan_cache_expires", 0.0) or 0.0)
        if isinstance(cache, tuple) and now < expires:
            return cache
        assignments_by_radio: dict[int, dict[str, object]] = {}
        plans_by_id: dict[int, dict[str, object]] = {}
        store = getattr(self, "multi_radio_store", None)
        if store is not None:
            try:
                for row in store.list_effective_assigned_plans():
                    data = dict(row)
                    radio_id = int(data.get("device_profile_id") or 0)
                    if radio_id > 0:
                        assignments_by_radio[radio_id] = data
            except Exception:
                assignments_by_radio = {}
            try:
                for row in store.list_frequency_plans():
                    data = dict(row)
                    plan_id = int(data.get("id") or 0)
                    if plan_id > 0:
                        plans_by_id[plan_id] = data
            except Exception:
                plans_by_id = {}
        cache = (assignments_by_radio, plans_by_id)
        self._station_command_plan_cache_data = cache
        self._station_command_plan_cache_expires = now + 0.75
        return cache

    def _invalidate_station_command_plan_cache(self) -> None:
        self._station_command_plan_cache_data = None
        self._station_command_plan_cache_expires = 0.0

    def _station_command_assigned_plan_for_radio(self, device_profile_id: int) -> dict[str, object] | None:
        try:
            assignments_by_radio, plans_by_id = self._station_command_plan_cache()
            assignment = assignments_by_radio.get(int(device_profile_id or 0))
            if not assignment:
                return None
            plan_id = int(assignment.get("frequency_plan_id") or 0)
            if plan_id <= 0:
                return None
            return plans_by_id.get(plan_id)
        except Exception:
            return None

    def _station_command_assigned_plan_refs_for_radio(self, device_profile_id: int) -> list[dict[str, object]]:
        plan = self._station_command_assigned_plan_for_radio(device_profile_id)
        if not isinstance(plan, Mapping):
            return []
        refs: list[dict[str, object]] = []
        for key in ("schedule_refs_json", "frequency_refs_json"):
            refs.extend(self._station_command_parse_json_ref_items(plan.get(key, "[]")))
        return refs

    def _station_command_assigned_plan_name_for_radio(self, device_profile_id: int) -> str:
        plan = self._station_command_assigned_plan_for_radio(device_profile_id)
        if isinstance(plan, Mapping):
            name = str(plan.get("name") or "").strip()
            if name:
                return name
        try:
            assignments_by_radio, _plans_by_id = self._station_command_plan_cache()
            assignment = assignments_by_radio.get(int(device_profile_id or 0), {})
            for key in ("frequency_plan_name", "plan_name", "name"):
                name = str(assignment.get(key) or "").strip() if isinstance(assignment, Mapping) else ""
                if name:
                    return name
        except Exception:
            pass
        lane = self._station_command_lane_for_radio(device_profile_id)
        if isinstance(lane, Mapping):
            name = str(lane.get("frequency_plan_name") or "").strip()
            if name:
                return name
        return ""

    def _station_command_active_schedule_lanes(self, *, force: bool = False) -> dict[int, dict[str, object]]:
        now = time.monotonic()
        cache = getattr(self, "_station_command_lane_cache_data", None)
        expires = float(getattr(self, "_station_command_lane_cache_expires", 0.0) or 0.0)
        if isinstance(cache, dict) and not force and now < expires:
            return cache
        scheduler = getattr(self, "scheduler", None)
        lanes: dict[int, dict[str, object]] = {}
        if scheduler is not None and hasattr(scheduler, "active_schedule_lanes"):
            try:
                for lane in scheduler.active_schedule_lanes(force=force):
                    if not isinstance(lane, Mapping):
                        continue
                    try:
                        radio_id = int(lane.get("device_profile_id") or 0)
                    except Exception:
                        radio_id = 0
                    if radio_id > 0:
                        lanes[radio_id] = dict(lane)
            except Exception as exc:
                log.debug("MainWindow: failed to load station command schedule lanes: %s", exc)
                lanes = {}
        self._station_command_lane_cache_data = lanes
        self._station_command_lane_cache_expires = now + 0.75
        return lanes

    def _invalidate_station_command_lane_cache(self) -> None:
        self._station_command_lane_cache_data = None
        self._station_command_lane_cache_expires = 0.0

    def _station_command_lane_for_radio(self, device_profile_id: int) -> dict[str, object] | None:
        try:
            radio_id = int(device_profile_id or 0)
        except Exception:
            radio_id = 0
        if radio_id <= 0:
            return None
        lane = self._station_command_active_schedule_lanes().get(radio_id)
        return dict(lane) if isinstance(lane, Mapping) else None

    def _station_command_entry_group_band(self, entry: Mapping[str, object] | None) -> tuple[str, str]:
        if not isinstance(entry, Mapping):
            return "", ""
        group = self._station_command_group_display_name(
            entry.get("group_name") or entry.get("group") or entry.get("net_name") or entry.get("label")
        )
        band = str(entry.get("band") or "").strip().upper()
        return group, band

    def _station_command_lane_current_group_band(self, device_profile_id: int) -> tuple[str, str]:
        lane = self._station_command_lane_for_radio(device_profile_id)
        entry = lane.get("current_entry") if isinstance(lane, Mapping) else None
        return self._station_command_entry_group_band(entry if isinstance(entry, Mapping) else None)

    def _station_command_lane_next_group_band(self, device_profile_id: int) -> tuple[str, str]:
        lane = self._station_command_lane_for_radio(device_profile_id)
        entry = lane.get("next_entry") if isinstance(lane, Mapping) else None
        return self._station_command_entry_group_band(entry if isinstance(entry, Mapping) else None)

    def _station_command_lane_schedule_rows(self, device_profile_id: int) -> list[dict[str, object]]:
        lane = self._station_command_lane_for_radio(device_profile_id)
        if not isinstance(lane, Mapping):
            return []
        rows: list[dict[str, object]] = []
        for key in ("hf_rows", "net_rows", "sop_rows"):
            raw_rows = lane.get(key)
            if not isinstance(raw_rows, list):
                continue
            rows.extend(dict(row) for row in raw_rows if isinstance(row, Mapping))
        return rows

    @staticmethod
    def _station_command_scheduler_entry_radio_id(entry: Mapping[str, object]) -> int:
        return station_command_scheduler_entry_radio_id(entry)

    def _station_command_manual_qsy_meta_for_radio(self, device_profile_id: int) -> dict[str, object] | None:
        meta = getattr(self, "_station_command_manual_qsy_meta", None)
        return station_command_manual_qsy_meta_for_radio(
            meta=meta if isinstance(meta, Mapping) else None,
            meta_profile_id=getattr(self, "_station_command_manual_qsy_profile_id", 0),
            device_profile_id=int(device_profile_id or 0),
        )

    def _station_command_manual_control_state_for_radio(self, device_profile_id: int) -> object | None:
        try:
            radio_id = int(device_profile_id or 0)
        except Exception:
            radio_id = 0
        if radio_id <= 0:
            return None
        try:
            service = getattr(getattr(self, "scheduler", None), "_manual_control_service", None)
            return service.get_state(radio_id) if service is not None and hasattr(service, "get_state") else None
        except Exception:
            return None

    def _station_command_manual_control_service_available(self) -> bool:
        service = getattr(getattr(self, "scheduler", None), "_manual_control_service", None)
        return service is not None and hasattr(service, "get_state")

    @staticmethod
    def _station_command_manual_state_has_qsy_target(state: object | None) -> bool:
        target = getattr(state, "manual_target", None)
        if target is None:
            return False
        try:
            return int(getattr(target, "frequency_hz", 0) or 0) > 0
        except Exception:
            return False

    def _station_command_scheduler_manual_qsy_active_for_radio(self, device_profile_id: int) -> bool:
        try:
            ident = int(device_profile_id or 0)
        except Exception:
            return False
        if ident <= 0:
            return False
        state = self._station_command_manual_control_state_for_radio(ident)
        if state is not None:
            state_name = str(getattr(state, "state", "") or "").strip()
            if state_name == "manual_qsy":
                return True
            if state_name == "manual_hold" and self._station_command_manual_state_has_qsy_target(state):
                return True
            return False
        if self._station_command_manual_control_service_available():
            return False
        if self._station_command_manual_qsy_meta_for_radio(ident):
            return True
        try:
            sched = getattr(self, "scheduler", None)
            entry = getattr(sched, "current_schedule_entry", {}) if sched is not None else {}
            target_getter = getattr(sched, "_primary_manual_control_radio_id", None)
            primary_manual_radio_id = target_getter() if callable(target_getter) else 0
            return scheduler_manual_qsy_active_for_radio(
                device_profile_id=int(device_profile_id or 0),
                manual_meta=None,
                scheduler_source=getattr(sched, "current_source", "") if sched is not None else "",
                scheduler_manual_active=getattr(sched, "_manual_qsy_active", False) if sched is not None else False,
                scheduler_entry=entry if isinstance(entry, Mapping) else None,
                primary_manual_radio_id=primary_manual_radio_id,
            )
        except Exception:
            return False

    def _station_command_scheduler_manual_qsy_active(self) -> bool:
        try:
            active_id = int(getattr(self, "_station_command_selected_profile_id", 0) or 0)
        except Exception:
            active_id = 0
        return self._station_command_scheduler_manual_qsy_active_for_radio(active_id)

    @staticmethod
    def _station_command_countdown_text(remaining_sec: object) -> str:
        return station_command_countdown_text(remaining_sec)

    def _update_station_command_hold_button_labels(self, hold_snapshot: Mapping[str, object]) -> None:
        hold_btn = getattr(self, "station_command_hold_btn", None)
        suspend_btn = getattr(self, "station_command_suspend_btn", None)
        qsy_btn = getattr(self, "station_command_qsy_btn", None)
        if hold_btn is None or suspend_btn is None:
            return
        base_qsy = str(getattr(self, "_station_command_qsy_suspend_base_text", "QSY Suspend") or "QSY Suspend")
        base_suspend = str(getattr(self, "_station_command_suspend_base_text", "Suspend Scheduler") or "Suspend Scheduler")
        manual_qsy_active = self._station_command_scheduler_manual_qsy_active()
        scheduler_suspended_manual = self._station_command_scheduler_suspended_manually()
        if qsy_btn is not None:
            qsy_btn.setText("Manual QSY" if manual_qsy_active else "QSY Now")
        suspend_btn.setText("Scheduler Suspended" if scheduler_suspended_manual else base_suspend)
        if isinstance(hold_snapshot, Mapping) and bool(hold_snapshot.get("active")):
            countdown = self._station_command_countdown_text(hold_snapshot.get("remaining_sec"))
            if countdown:
                hold_btn.setText(f"Suspended {countdown}")
                until = hold_snapshot.get("until")
                if isinstance(until, datetime.datetime):
                    local_dt = until.astimezone()
                    tip = f"Scheduler resumes at {local_dt:%H:%M:%S} local."
                    hold_btn.setToolTip(tip)
                return
        hold_btn.setText(base_qsy)

    def _station_command_manual_qsy_meta_for_selected(self) -> dict[str, object] | None:
        try:
            active_id = int(getattr(self, "_station_command_selected_profile_id", 0) or 0)
        except Exception:
            return None
        return self._station_command_manual_qsy_meta_for_radio(active_id)

    def _station_command_set_manual_qsy_meta(self, meta: Mapping[str, object] | None) -> None:
        if not isinstance(meta, Mapping):
            self._station_command_manual_qsy_meta = None
            self._station_command_manual_qsy_profile_id = None
            return
        self._station_command_manual_qsy_meta = dict(meta)
        try:
            self._station_command_manual_qsy_profile_id = int(
                meta.get("target_device_profile_id")
                or getattr(self, "_station_command_selected_profile_id", 0)
                or 0
            )
        except Exception:
            self._station_command_manual_qsy_profile_id = None

    def _station_command_clear_manual_qsy_meta(self, target_device_profile_id: int | None = None) -> None:
        if target_device_profile_id is not None:
            try:
                target_id = int(target_device_profile_id or 0)
                stored_id = int(getattr(self, "_station_command_manual_qsy_profile_id", 0) or 0)
            except Exception:
                target_id = 0
                stored_id = 0
            if target_id > 0 and stored_id not in {0, target_id}:
                return
        self._station_command_manual_qsy_meta = None
        self._station_command_manual_qsy_profile_id = None

    def _station_command_clear_pending_qsy_for_radio(self, device_profile_id: int | None) -> None:
        try:
            radio_id = int(device_profile_id or 0)
        except Exception:
            radio_id = 0
        if radio_id <= 0:
            return
        pending = getattr(self, "_station_command_card_qsy_pending_keys", None)
        if isinstance(pending, dict):
            pending.pop(radio_id, None)

    def _station_command_scheduler_suspended_manually_for_radio(self, device_profile_id: int) -> bool:
        try:
            state = self._station_command_manual_control_state_for_radio(int(device_profile_id or 0))
            if state is not None and getattr(state, "state", "") == "manual_suspend":
                return True
            if state is not None:
                return False
            if self._station_command_manual_control_service_available():
                return False
            return scheduler_suspended_manually_for_radio(
                device_profile_id=int(device_profile_id or 0),
                suspended_manual=getattr(self, "_station_command_scheduler_suspended_manual", False),
                suspended_profile_id=getattr(self, "_station_command_scheduler_suspended_manual_profile_id", 0),
                runtime_scheduler_enabled_override=None,
                selected_profile_id=getattr(self, "_station_command_selected_profile_id", 0),
            )
        except Exception:
            return False

    def _station_command_scheduler_suspended_manually(self) -> bool:
        try:
            active_id = int(getattr(self, "_station_command_selected_profile_id", 0) or 0)
        except Exception:
            active_id = 0
        return self._station_command_scheduler_suspended_manually_for_radio(active_id)

    def _station_command_timed_suspend_active_for_radio(self, device_profile_id: int) -> bool:
        if self._station_command_scheduler_manual_qsy_active_for_radio(device_profile_id):
            return False
        hold_snapshot = self._station_command_hold_snapshot_for_radio(device_profile_id)
        return timed_suspend_active_for_radio(
            device_profile_id=int(device_profile_id or 0),
            timed_suspend_profile_id=getattr(self, "_station_command_timed_suspend_profile_id", 0),
            hold_active=bool(hold_snapshot.get("active")),
        )

    def _station_command_hold_snapshot_for_radio(self, device_profile_id: int) -> dict[str, object]:
        try:
            radio_id = int(device_profile_id or 0)
        except Exception:
            radio_id = 0
        if radio_id <= 0:
            return {"active": False, "until": None, "remaining_sec": None}
        try:
            state = self._station_command_manual_control_state_for_radio(radio_id)
        except Exception:
            state = None
        if state is not None and getattr(state, "state", "") in {"manual_hold", "manual_qsy"}:
            text = str(getattr(state, "hold_until_utc", "") or "").strip()
            if text:
                try:
                    until = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
                    if until.tzinfo is None:
                        until = until.replace(tzinfo=datetime.timezone.utc)
                    until = until.astimezone(datetime.timezone.utc)
                    remaining = (until - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
                    if remaining > 0:
                        return {"active": True, "until": until, "remaining_sec": remaining}
                except Exception:
                    pass
        return {"active": False, "until": None, "remaining_sec": None}

    def _station_command_set_scheduler_suspended_manual(
        self,
        suspended: bool,
        *,
        target_device_profile_id: int | None = None,
    ) -> None:
        value = bool(suspended)
        try:
            target_id = int(target_device_profile_id or getattr(self, "_station_command_selected_profile_id", 0) or 0)
        except Exception:
            target_id = 0
        if target_id > 0:
            try:
                service = getattr(getattr(self, "scheduler", None), "_manual_control_service", None)
                if service is not None:
                    if value and hasattr(service, "suspend"):
                        service.suspend(
                            target_id,
                            reason_code="operator_suspend",
                            operator_source="main_control_center",
                        )
                    elif not value and hasattr(service, "resume"):
                        service.resume(target_id)
            except Exception as exc:
                log.debug("MainWindow: failed to persist radio-scoped scheduler suspend: %s", exc)
            self._station_command_scheduler_suspended_manual = value
            self._station_command_scheduler_suspended_manual_profile_id = target_id if value else 0
            try:
                set_scheduler_enabled_override(None)
            except Exception:
                pass
            return

        self._station_command_scheduler_suspended_manual = value
        if value:
            try:
                self._station_command_scheduler_suspended_manual_profile_id = int(
                    getattr(self, "_station_command_selected_profile_id", 0) or 0
                )
            except Exception:
                self._station_command_scheduler_suspended_manual_profile_id = 0
        else:
            self._station_command_scheduler_suspended_manual_profile_id = 0
        try:
            scheduler = getattr(self, "scheduler", None)
            if scheduler is not None and hasattr(scheduler, "set_runtime_scheduler_enabled"):
                scheduler.set_runtime_scheduler_enabled(False if value else True)
        except Exception as exc:
            log.debug("MainWindow: failed to update runtime scheduler enabled state: %s", exc)
        try:
            set_scheduler_enabled_override(False if value else True)
        except Exception:
            pass
        if value:
            try:
                set_suspend_until(self.settings, None)
            except Exception:
                pass

    def _station_command_group_band_for_frequency(self, snapshot: object) -> tuple[str, str]:
        try:
            snapshot_group = self._station_command_group_display_name(
                self._station_command_value(snapshot, "current_group", "")
                or self._station_command_value(snapshot, "schedule_group", "")
                or self._station_command_value(snapshot, "group", "")
                or self._station_command_value(snapshot, "group_name", "")
            )
            snapshot_band = str(self._station_command_value(snapshot, "current_band", "") or "").strip().upper()
            if snapshot_group:
                return snapshot_group, snapshot_band
            current_freq = self._station_command_parse_frequency(self._station_command_value(snapshot, "current_frequency_label", ""))
            current_band = snapshot_band
            if current_freq is None:
                return "", current_band
            meta_map = build_qsy_options(load_operating_groups(self.settings))
            for meta in meta_map.values():
                try:
                    meta_freq = parse_frequency_mhz(meta.get("freq"))
                except Exception:
                    meta_freq = None
                if meta_freq is None or abs(float(meta_freq) - float(current_freq)) > 0.0005:
                    continue
                band = str(meta.get("band") or "").strip().upper()
                if current_band and band and band != current_band:
                    continue
                return self._station_command_group_display_name(meta.get("group")), band or current_band
            return "", current_band
        except Exception:
            return "", ""

    def _station_command_now_text(self, snapshot: object) -> str:
        snapshot_id = self._station_command_snapshot_id(snapshot)
        manual_meta = (
            self._station_command_manual_qsy_meta_for_radio(snapshot_id)
            if snapshot_id > 0
            else self._station_command_manual_qsy_meta_for_selected()
        )
        if manual_meta:
            return self._station_command_qsy_label(manual_meta)
        group, band = self._station_command_assigned_plan_group_band(snapshot)
        if not group:
            group, band = self._station_command_schedule_group_band(snapshot)
        if not group:
            group, band = self._station_command_group_band_for_frequency(snapshot)
        if group:
            return " ".join(part for part in (group, band) if part).strip()
        return self._station_command_frequency_text(snapshot)

    def _station_command_now_tooltip(self, snapshot: object) -> str:
        exact = self._station_command_frequency_text(snapshot)
        snapshot_id = self._station_command_snapshot_id(snapshot)
        manual_meta = (
            self._station_command_manual_qsy_meta_for_radio(snapshot_id)
            if snapshot_id > 0
            else self._station_command_manual_qsy_meta_for_selected()
        )
        if manual_meta:
            target = self._station_command_qsy_label(manual_meta)
            return f"QSY target: {target}; radio reports: {exact}" if exact else f"QSY target: {target}"
        group, band = self._station_command_schedule_group_band(snapshot)
        if not group:
            group, band = self._station_command_group_band_for_frequency(snapshot)
        target = " ".join(part for part in (group, band) if part).strip()
        try:
            source = str(getattr(getattr(self, "scheduler", None), "current_source", "") or "").strip().upper()
        except Exception:
            source = ""
        if source == "QSY" and target and exact and target != exact:
            return f"QSY target: {target}; radio reports: {exact}"
        return f"{target}: {exact}" if target and exact and target != exact else exact

    @staticmethod
    def _station_command_format_frequency(value: object, *, suffix: str = "MHz") -> str:
        freq = parse_frequency_mhz(value)
        if freq is None:
            text = str(value or "").replace("MHz", "").strip()
            freq = parse_frequency_mhz(text)
        if freq is None:
            return ""
        total_hz = max(0, int(round(float(freq) * 1_000_000)))
        mhz = total_hz // 1_000_000
        khz = (total_hz % 1_000_000) // 1_000
        hz = total_hz % 1_000
        label = f"{mhz}.{khz:03d}.{hz:03d}"
        return f"{label} {suffix}".strip() if suffix else label

    @staticmethod
    def _station_command_state_text(snapshot: object) -> str:
        try:
            if MainWindow._station_command_bool(MainWindow._station_command_value(snapshot, "ptt_active", False)) or MainWindow._station_command_bool(
                MainWindow._station_command_value(snapshot, "shared_ptt_blocked", False)
            ):
                return "Busy: PTT"
            if str(MainWindow._station_command_value(snapshot, "device_class", "") or "").strip().lower() == "observer":
                return "Monitor"
            if not MainWindow._station_command_bool(MainWindow._station_command_value(snapshot, "runtime_active", True), default=True):
                return "Configured inactive"
            if not MainWindow._station_command_bool(
                MainWindow._station_command_value(snapshot, "scheduler_enabled", True),
                default=True,
            ):
                return "Scheduler Off"
            summary = str(MainWindow._station_command_value(snapshot, "status_summary", "") or "").strip()
            if summary and "xml-rpc" not in summary.lower():
                return summary
            state = str(MainWindow._station_command_value(snapshot, "overall_state", "") or "").strip()
            return state.title() if state else "On Schedule"
        except Exception:
            return "unknown"

    def _station_command_next_text(self, snapshot: object) -> str:
        try:
            if str(self._station_command_value(snapshot, "device_class", "") or "").strip().lower() == "observer":
                return (
                    str(self._station_command_value(snapshot, "observer_follow_summary", "") or "").strip()
                    or "Receive-only monitor"
                )
            plan = self._station_command_plan_name_for_snapshot(snapshot)
            if not plan or plan == "Unassigned":
                return "No assigned plan"
            if not self._station_command_bool(
                self._station_command_value(snapshot, "scheduler_enabled", True),
                default=True,
            ):
                return "Scheduler disabled"
            next_group = self._station_command_group_display_name(
                self._station_command_value(snapshot, "next_group", "")
                or self._station_command_value(snapshot, "next_schedule_group", "")
            )
            next_band = str(
                self._station_command_value(snapshot, "next_band", "")
                or self._station_command_value(snapshot, "next_schedule_band", "")
                or ""
            ).strip().upper()
            next_label = " ".join(part for part in (next_group, next_band) if part).strip()
            if next_label:
                return next_label
            plan_group, plan_band = self._station_command_assigned_plan_next_group_band(snapshot)
            plan_label = " ".join(part for part in (plan_group, plan_band) if part).strip()
            return plan_label or plan
        except Exception:
            return "none"

    @staticmethod
    def _station_command_qsy_label(meta: Mapping[str, object]) -> str:
        group = MainWindow._station_command_group_display_name(meta.get("group"))
        band = str(meta.get("band") or "").strip().upper()
        parts = []
        if group:
            parts.append(group)
        if band:
            parts.append(band)
        if not parts:
            freq = MainWindow._station_command_format_frequency(meta.get("freq"), suffix="")
            if freq:
                parts.append(freq)
        return " ".join(part for part in parts if part).strip() or "Frequency"

    @staticmethod
    def _station_command_qsy_tooltip(meta: Mapping[str, object]) -> str:
        label = MainWindow._station_command_qsy_label(meta)
        freq = MainWindow._station_command_format_frequency(meta.get("freq"), suffix="")
        mode = str(meta.get("mode") or "").strip()
        details = " ".join(part for part in (freq, mode) if part).strip()
        return f"{label}: {details}" if details else label

    def _station_command_preferred_qsy_key(self, selected: object | None) -> str:
        selected_id = self._station_command_snapshot_id(selected) if selected is not None else 0
        has_assigned_refs = False
        manual_meta = (
            self._station_command_manual_qsy_meta_for_radio(selected_id)
            if selected_id > 0
            else self._station_command_manual_qsy_meta_for_selected()
        )
        if manual_meta:
            try:
                freq = self._station_command_parse_frequency(manual_meta.get("freq"))
                if freq is not None:
                    return f"{freq:.6f}"
            except Exception:
                pass
        if selected_id > 0:
            refs = self._station_command_assigned_plan_refs_for_radio(selected_id)
            has_assigned_refs = bool(refs)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            for ref in refs:
                if not self._station_command_ref_active_now(ref, now_utc):
                    continue
                try:
                    freq = self._station_command_parse_frequency(
                        ref.get("frequency") or ref.get("freq") or ref.get("frequency_mhz")
                    )
                    if freq is not None:
                        return f"{freq:.6f}"
                except Exception:
                    pass
            for ref in refs:
                try:
                    freq = self._station_command_parse_frequency(
                        ref.get("frequency") or ref.get("freq") or ref.get("frequency_mhz")
                    )
                    if freq is not None:
                        return f"{freq:.6f}"
                except Exception:
                    pass
            if not refs:
                lane = self._station_command_lane_for_radio(selected_id)
                entry = lane.get("current_entry") if isinstance(lane, Mapping) else None
                if isinstance(entry, Mapping):
                    try:
                        freq = self._station_command_parse_frequency(entry.get("frequency"))
                        if freq is not None:
                            return f"{freq:.6f}"
                    except Exception:
                        pass
        try:
            sched = getattr(self, "scheduler", None)
            entry = getattr(sched, "current_schedule_entry", {}) if sched is not None else {}
            if isinstance(entry, Mapping):
                if selected_id > 0:
                    entry_id = self._station_command_scheduler_entry_radio_id(entry)
                    if entry_id <= 0 or entry_id != selected_id:
                        raise ValueError("scheduler entry belongs to another radio")
                freq = self._station_command_parse_frequency(entry.get("frequency"))
                if freq is not None:
                    return f"{freq:.6f}"
        except Exception:
            pass
        if selected is not None and (selected_id <= 0 or not has_assigned_refs):
            try:
                freq = self._station_command_parse_frequency(
                    self._station_command_value(selected, "current_frequency_label", "")
                )
                if freq is not None:
                    return f"{freq:.6f}"
            except Exception:
                pass
        return ""

    def _refresh_station_command_frequency_combo(self, selected: object | None = None) -> bool:
        combo = getattr(self, "station_command_freq_combo", None)
        if not isinstance(combo, QComboBox):
            return False
        if combo.view().isVisible() or combo.hasFocus():
            return selected_qsy_meta(combo) is not None
        current = selected_qsy_meta(combo)
        current_key = ""
        try:
            if current:
                current_key = f"{float(current.get('freq')):.6f}"
        except Exception:
            current_key = ""
        preferred_key = self._station_command_preferred_qsy_key(selected)
        try:
            meta_map = build_qsy_options(load_operating_groups(self.settings))
        except Exception:
            meta_map = {}
        previous_block = combo.blockSignals(True)
        try:
            combo.clear()
            combo.addItem("Manual QSY target", None)
            for key, meta in sorted(
                meta_map.items(),
                key=lambda item: (
                    str(item[1].get("group") or "").upper(),
                    str(item[1].get("band") or "").upper(),
                    float(item[1].get("freq") or 0.0),
                ),
            ):
                combo.addItem(self._station_command_qsy_label(meta), meta)
                combo.setItemData(combo.count() - 1, self._station_command_qsy_tooltip(meta), Qt.ToolTipRole)
            select_key = current_key or preferred_key
            if select_key:
                for index in range(combo.count()):
                    data = combo.itemData(index)
                    try:
                        if isinstance(data, Mapping) and f"{float(data.get('freq')):.6f}" == select_key:
                            combo.setCurrentIndex(index)
                            break
                    except Exception:
                        continue
        finally:
            combo.blockSignals(previous_block)
        return selected_qsy_meta(combo) is not None

    def _selected_station_command_hold_minutes(self) -> int:
        return selected_hold_duration(
            getattr(self, "station_command_duration_combo", None),
            self.settings,
            getattr(self, "_active_runtime_profile", None),
        )

    def _publish_station_command_feedback(
        self,
        *,
        action_type: str,
        status: str,
        summary: str,
        detail: str = "",
    ) -> None:
        try:
            service = getattr(self, "action_feedback_service", None)
            if service is None or not hasattr(service, "publish"):
                return
            radio_id = getattr(self, "_station_command_selected_profile_id", None)
            target_label = self.station_command_radio_combo.currentText() if hasattr(self, "station_command_radio_combo") else ""
            service.publish(
                scope="radio",
                action_type=action_type,
                status=status,
                summary=summary,
                radio_profile_id=str(radio_id) if radio_id not in (None, 0, "") else None,
                target_label=str(target_label or "").strip(),
                detail=str(detail or "").strip(),
                source_surface="station_command_bar",
            )
            if str(status or "").strip().lower() in {"blocked", "failed", "error"} and hasattr(self, "statusBar"):
                self.statusBar().showMessage(str(summary or detail or "Station command needs attention."), 6000)
        except Exception as e:
            log.debug("MainWindow: station command feedback failed: %s", e)

    def _on_station_command_hold_duration_changed(self) -> None:
        mins = self._selected_station_command_hold_minutes()
        set_hold_duration_default(self.settings, mins)
        self.on_hold_duration_default_changed()

    def _station_command_selected_qsy_meta(self) -> dict | None:
        combo = getattr(self, "station_command_freq_combo", None)
        if not isinstance(combo, QComboBox):
            return None
        meta = selected_qsy_meta(combo)
        if not isinstance(meta, Mapping):
            return None
        out = dict(meta)
        try:
            profile_id = int(getattr(self, "_station_command_selected_profile_id", 0) or 0)
        except Exception:
            profile_id = 0
        if profile_id > 0:
            out["target_device_profile_id"] = profile_id
        return out

    @staticmethod
    def _station_command_display_plan_name(plan_name: object) -> str:
        text = str(plan_name or "").strip()
        if len(text) > 5 and text.lower().endswith(" plan"):
            return text[:-5].strip()
        return text

    def _on_station_command_qsy_now_clicked(self, meta: Mapping[str, object] | None = None) -> None:
        meta = dict(meta) if isinstance(meta, Mapping) else self._station_command_selected_qsy_meta()
        if not meta:
            self._publish_station_command_feedback(
                action_type="qsy",
                status="blocked",
                summary="QSY blocked: select a manual target.",
                detail="Choose a frequency in the top command bar before using QSY Now.",
            )
            return
        ok = perform_qsy(self, meta)
        if ok:
            self._station_command_set_manual_qsy_meta(meta)
            try:
                pending = getattr(self, "_station_command_card_qsy_pending_keys", None)
                if isinstance(pending, dict):
                    pending.pop(int(meta.get("target_device_profile_id") or 0), None)
            except Exception:
                pass
        freq_label = self._station_command_qsy_label(meta)
        self._publish_station_command_feedback(
            action_type="qsy",
            status="succeeded" if ok else "blocked",
            summary=f"QSY sent: {freq_label}" if ok else f"QSY blocked: {freq_label}",
            detail="Scheduler is suspended in manual QSY state until Resume Schedule or the next explicit schedule transition." if ok else "",
        )
        self._refresh_station_command_controls_after_state_change()

    def _on_station_command_qsy_hold_clicked(
        self,
        meta: Mapping[str, object] | None = None,
        hold_minutes: int | None = None,
    ) -> None:
        meta = dict(meta) if isinstance(meta, Mapping) else self._station_command_selected_qsy_meta()
        if not meta:
            self._publish_station_command_feedback(
                action_type="qsy",
                status="blocked",
                summary="QSY Suspend blocked: select a manual target.",
                detail="Choose a frequency in the top command bar before using QSY Suspend.",
            )
            return
        mins = perform_qsy_with_hold(
            self,
            self.settings,
            meta,
            int(hold_minutes or self._selected_station_command_hold_minutes()),
        )
        if mins > 0:
            self._station_command_set_manual_qsy_meta(meta)
            try:
                pending = getattr(self, "_station_command_card_qsy_pending_keys", None)
                if isinstance(pending, dict):
                    pending.pop(int(meta.get("target_device_profile_id") or 0), None)
            except Exception:
                pass
        freq_label = self._station_command_qsy_label(meta)
        self._publish_station_command_feedback(
            action_type="qsy",
            status="succeeded" if mins > 0 else "blocked",
            summary=f"QSY sent: {freq_label}" if mins > 0 else f"QSY Suspend blocked: {freq_label}",
            detail=f"Scheduler suspended for {mins} minutes." if mins > 0 else "",
        )
        self._refresh_station_command_controls_after_state_change()

    def _on_station_command_timed_suspend_clicked(self, target_device_profile_id: int | None = None) -> None:
        try:
            target_id = int(target_device_profile_id or getattr(self, "_station_command_selected_profile_id", 0) or 0) or None
        except Exception:
            target_id = None
        try:
            mins = suspend_schedule_hold(
                self,
                self.settings,
                minutes=self._selected_station_command_hold_minutes(),
                warn_rf_conflict=True,
                target_device_profile_id=target_id,
            )
            if mins > 0:
                self._station_command_clear_manual_qsy_meta(target_id)
            try:
                self._station_command_timed_suspend_profile_id = int(target_id or 0)
            except Exception:
                self._station_command_timed_suspend_profile_id = 0
            self._publish_station_command_feedback(
                action_type="timed_suspend_schedule",
                status="succeeded" if mins > 0 else "blocked",
                summary=f"Scheduler suspended for {mins} minutes." if mins > 0 else "Timed Suspend blocked.",
                detail="Scheduled frequency changes are paused temporarily. Manual radio changes remain available." if mins > 0 else "",
            )
        except Exception as e:
            self._publish_station_command_feedback(
                action_type="timed_suspend_schedule",
                status="failed",
                summary="Timed Suspend failed.",
                detail=str(e),
            )
        self._refresh_station_command_controls_after_state_change()

    def _on_station_command_pause_clicked(self, target_device_profile_id: int | None = None) -> None:
        try:
            target_id = int(target_device_profile_id or getattr(self, "_station_command_selected_profile_id", 0) or 0) or None
        except Exception:
            target_id = None
        try:
            self._station_command_set_scheduler_suspended_manual(True, target_device_profile_id=target_id)
            self._station_command_timed_suspend_profile_id = 0
            self._publish_station_command_feedback(
                action_type="suspend_schedule",
                status="succeeded",
                summary="Scheduler suspended.",
                detail="Scheduled frequency changes are suspended manually until Resume Schedule.",
            )
        except Exception as e:
            self._publish_station_command_feedback(
                action_type="suspend_schedule",
                status="failed",
                summary="Suspend Scheduler failed.",
                detail=str(e),
            )
        self._refresh_station_command_controls_after_state_change()

    def _on_station_command_resume_clicked(self, target_device_profile_id: int | None = None) -> None:
        try:
            target_profile_id = int(target_device_profile_id or getattr(self, "_station_command_selected_profile_id", 0) or 0) or None
        except Exception:
            target_profile_id = None
        try:
            ok = resume_schedule_hold(
                self,
                self.settings,
                target_device_profile_id=target_profile_id,
            )
        except TypeError:
            ok = resume_schedule_hold(self, self.settings)
        if ok:
            self._station_command_clear_manual_qsy_meta(target_profile_id)
            self._station_command_clear_pending_qsy_for_radio(target_profile_id)
            self._invalidate_station_command_lane_cache()
            self._station_command_set_scheduler_suspended_manual(False, target_device_profile_id=target_profile_id)
            self._station_command_timed_suspend_profile_id = 0
        self._publish_station_command_feedback(
            action_type="resume_schedule",
            status="succeeded" if ok else "blocked",
            summary="Schedule resumed." if ok else "Resume blocked.",
            detail="" if ok else "RF Guard or scheduler state prevented resume.",
        )
        self._refresh_station_command_bar(force=True)

    def _refresh_station_command_controls_after_state_change(self) -> None:
        try:
            choices = getattr(self, "_station_command_last_choices", []) or []
        except Exception:
            choices = []
        if isinstance(choices, list) and choices:
            try:
                snapshot = suspend_snapshot(self.settings, allow_reload=False)
            except Exception:
                snapshot = {}
            self._update_station_command_radio_tile_hold_controls(snapshot if isinstance(snapshot, Mapping) else {})
            return
        self._refresh_station_command_bar(force=False)

    def _activate_station_command_radio(self, device_profile_id: int) -> bool:
        ident = int(device_profile_id or 0)
        if ident <= 0:
            return False
        choices = self._station_command_active_radio_choices(
            getattr(self, "_station_command_last_choices", []) or self._station_command_configured_profiles()
        )
        profile = next(
            (
                choice
                for choice in choices
                if self._station_command_snapshot_id(choice) == ident
            ),
            None,
        )
        if profile is None:
            self._publish_station_command_feedback(
                action_type="select_radio",
                status="failed",
                summary="Radio selection failed.",
                detail="Only active, controllable radios can be selected in the control bar.",
            )
            return False
        try:
            self._active_runtime_profile = dict(profile) if isinstance(profile, Mapping) else profile
        except Exception as exc:
            log.debug("MainWindow: station command radio selection cache failed: %s", exc)
            return False
        return True

    @staticmethod
    def _clear_station_command_health_layout(layout: QHBoxLayout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()

    def _station_command_health_profile(self, selected: object | None, selected_id: int) -> dict | None:
        try:
            for profile in self.multi_radio_store.list_device_profiles():
                if self._station_command_snapshot_id(profile) == int(selected_id or 0):
                    return dict(profile)
        except Exception:
            pass
        return dict(selected) if isinstance(selected, Mapping) else None

    def _station_command_health_items(self, profile: dict | None) -> list[tuple[str, str]]:
        if not isinstance(profile, dict):
            return []
        try:
            return visible_status_programs(dict(self.settings.all()), device_profiles=[profile])
        except Exception:
            return []

    @staticmethod
    def _station_command_health_state(info: Mapping[str, object]) -> str:
        state = str(info.get("state", "idle") or "idle").strip().lower()
        if state == "ok":
            return "ok"
        if state == "error":
            return "error"
        return "warn"

    @staticmethod
    def _station_command_health_summary_state(issue_states: list[str]) -> str:
        if not issue_states:
            return "ok"
        if any(state == "error" for state in issue_states):
            return "error"
        return "warn"

    def _station_command_off_schedule_payload_for_profile(self, profile: dict | None) -> dict[str, object] | None:
        radio_id = self._station_command_snapshot_id(profile) if profile is not None else 0
        if radio_id <= 0:
            return None
        state = getattr(self, "_station_command_off_schedule_by_radio", {})
        if not isinstance(state, Mapping):
            return None
        payload = state.get(radio_id)
        return dict(payload) if isinstance(payload, Mapping) else None

    def _station_command_health_summary_for_profile(self, profile: dict | None) -> dict[str, object]:
        items = self._station_command_health_items(profile)
        off_schedule_payload = self._station_command_off_schedule_payload_for_profile(profile)
        off_schedule_items = []
        if isinstance(off_schedule_payload, Mapping):
            raw_items = off_schedule_payload.get("items")
            if isinstance(raw_items, list):
                off_schedule_items = [str(item).strip() for item in raw_items if str(item).strip()]
        try:
            snapshot = self.dependency_status_service.software_status_snapshot()
        except Exception:
            snapshot = {}
        issue_items: list[tuple[str, str, str, str]] = []
        if off_schedule_items:
            issue_items.append(
                (
                    "__off_schedule__",
                    "Off Schedule",
                    "warn",
                    "Review " + ", ".join(off_schedule_items) + " before trusting scheduler position.",
                )
            )
        healthy_count = 0
        for key, label_text in items:
            info = snapshot.get(key, {})
            state = self._station_command_health_state(info)
            tooltip = str(info.get("tooltip", "Not running") or "Not running")
            if state == "ok":
                healthy_count += 1
            else:
                issue_items.append((key, label_text, state, tooltip))
        issue_states = [state for _key, _label, state, _tooltip in issue_items]
        summary_state = self._station_command_health_summary_state(issue_states)
        if off_schedule_items:
            summary_label = "Off Schedule"
            summary_tooltip = "Off Schedule: " + ", ".join(off_schedule_items)
        elif not items:
            summary_label = "No checks"
            summary_tooltip = "No configured software health items for this radio."
        elif not issue_items:
            summary_label = "Healthy"
            summary_tooltip = "All configured components for this radio are healthy."
        else:
            summary_label = "Unhealthy" if summary_state == "error" else "Needs Review"
            summary_tooltip = "; ".join(f"{label}: {tooltip}" for _key, label, _state, tooltip in issue_items)
        return {
            "items": items,
            "healthy_count": healthy_count,
            "issues": issue_items,
            "state": summary_state,
            "label": summary_label,
            "tooltip": summary_tooltip,
        }

    def _show_station_command_health_menu(
        self,
        *,
        device_profile_id: int = 0,
        snapshot: object | None = None,
        anchor: QWidget | None = None,
    ) -> None:
        ident = int(device_profile_id or 0)
        selected = snapshot
        if selected is None and ident > 0:
            try:
                for candidate in getattr(self, "_station_command_last_choices", []) or []:
                    if self._station_command_snapshot_id(candidate) == ident:
                        selected = candidate
                        break
            except Exception:
                selected = None
        if ident <= 0 and selected is not None:
            ident = self._station_command_snapshot_id(selected)
        profile = self._station_command_health_profile(selected, ident)
        summary = self._station_command_health_summary_for_profile(profile)
        radio_name = self._station_command_snapshot_name(profile or selected or {"id": ident})
        anchor_widget = anchor or getattr(self, "station_command_health_widget", None) or self
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = {}
        menu_surface = theme.get("station_control_tile_selected_surface", theme.get("surface", "#FFFFFF"))
        menu_border = theme.get("station_control_tile_selected_border", theme.get("border", "#D3D7DD"))
        menu_text = theme.get("station_control_text", theme.get("text", "#222222"))
        menu_muted = theme.get("station_control_muted", theme.get("text_muted", "#6A737D"))
        menu = QMenu(anchor_widget)
        menu.setObjectName("stationCommandHealthMenu")
        menu.setStyleSheet(
            "QMenu#stationCommandHealthMenu {"
            f"background: {menu_surface}; color: {menu_text}; border: 1px solid {menu_border};"
            "padding: 5px;"
            "}"
            "QMenu#stationCommandHealthMenu::item {"
            "padding: 5px 22px 5px 10px;"
            "}"
            "QMenu#stationCommandHealthMenu::item:disabled {"
            f"color: {menu_muted};"
            "}"
            "QMenu#stationCommandHealthMenu::item:selected {"
            f"background: {theme.get('accent', menu_border)}; color: {theme.get('surface', '#FFFFFF')};"
            "}"
        )
        title = QAction(f"{radio_name} Health: {summary.get('label')}", menu)
        title.setEnabled(False)
        menu.addAction(title)
        issues = [tuple(item) for item in summary.get("issues", []) if isinstance(item, tuple)]
        if issues:
            for _key, label, state, tooltip in issues[:6]:
                prefix = "Block" if state == "error" else "Review"
                action = QAction(f"{prefix}: {label} - {tooltip}", menu)
                action.setEnabled(False)
                menu.addAction(action)
            if len(issues) > 6:
                more = QAction(f"+{len(issues) - 6} more item(s)", menu)
                more.setEnabled(False)
                menu.addAction(more)
        else:
            detail = str(summary.get("tooltip", "") or "No health issues for this radio.")
            action = QAction(detail, menu)
            action.setEnabled(False)
            menu.addAction(action)
        menu.addSeparator()
        open_action = QAction("Open Health Details", menu)
        open_action.triggered.connect(lambda _checked=False, profile_id=ident: self._open_station_health_detail(device_profile_id=profile_id))
        menu.addAction(open_action)
        self._station_command_health_menu = menu
        try:
            menu.popup(anchor_widget.mapToGlobal(anchor_widget.rect().bottomLeft()))
        except Exception:
            self._open_station_health_detail(device_profile_id=ident)

    def _add_station_command_health_item(
        self,
        *,
        key: str,
        label_text: str,
        state: str,
        tooltip: str,
        theme: dict,
    ) -> None:
        led = QLabel(self.station_command_health_widget)
        led.setFixedSize(14, 14)
        led.setCursor(Qt.PointingHandCursor)
        led.mousePressEvent = lambda event, widget=led: self._on_station_command_health_clicked(event, anchor=widget)
        led.setStyleSheet(led_style(state, theme))
        led.setToolTip(tooltip)
        text_label = QLabel(label_text, self.station_command_health_widget)
        text_label.setCursor(Qt.PointingHandCursor)
        text_label.mousePressEvent = lambda event, widget=text_label: self._on_station_command_health_clicked(event, anchor=widget)
        text_label.setToolTip(tooltip)
        text_label.setStyleSheet(
            f"background: transparent; color: {theme.get('station_control_text', theme.get('text', '#222222'))};"
        )
        self.station_command_health_leds[key] = led
        self.station_command_health_text_labels[key] = text_label
        self.station_command_health_layout.addWidget(led)
        self.station_command_health_layout.addWidget(text_label)

    def _refresh_station_command_health(self, selected: object | None, selected_id: int) -> None:
        if not hasattr(self, "station_command_health_layout"):
            return
        layout = self.station_command_health_layout
        self._clear_station_command_health_layout(layout)
        self.station_command_health_leds = {}
        self.station_command_health_text_labels = {}
        profile = self._station_command_health_profile(selected, selected_id)
        items = self._station_command_health_items(profile)
        theme = resolve_theme(self.settings)
        summary = self._station_command_health_summary_for_profile(profile)
        issue_items = [tuple(item) for item in summary.get("issues", []) if isinstance(item, tuple)]

        if not issue_items and items:
            self._add_station_command_health_item(
                key="__summary__",
                label_text="Healthy",
                state="ok",
                tooltip=str(summary.get("tooltip", "") or "All configured components for this radio are healthy."),
                theme=theme,
            )
        else:
            self._add_station_command_health_item(
                key="__summary__",
                label_text=str(summary.get("label", "Needs Review") or "Needs Review"),
                state=str(summary.get("state", "warn") or "warn"),
                tooltip=str(summary.get("tooltip", "") or "No configured software health items for this radio."),
                theme=theme,
            )
        layout.addStretch(1)
        has_items = bool(items)
        self.station_command_health_label.setVisible(has_items)
        self.station_command_health_widget.setVisible(has_items)

    @staticmethod
    def _station_command_is_controllable_profile(profile: object) -> bool:
        device_class = str(MainWindow._station_command_value(profile, "device_class", "tx_rx") or "tx_rx").strip().lower()
        if device_class == "observer":
            return False
        backend = str(MainWindow._station_command_value(profile, "control_backend", "manual") or "manual").strip().lower()
        return backend in SUPPORTED_RUNTIME_CONTROL_BACKENDS

    def _station_command_configured_profiles(self) -> list[dict]:
        try:
            profiles = list(self.multi_radio_store.list_runtime_active_device_profiles())
        except Exception:
            profiles = []
        return [dict(profile) for profile in profiles if self._station_command_is_controllable_profile(profile)]

    def _station_command_selected_snapshot(self, choices: list[object]) -> object | None:
        selected_id = getattr(self, "_station_command_selected_profile_id", None)
        if selected_id not in (None, 0):
            for snapshot in choices:
                if self._station_command_snapshot_id(snapshot) == int(selected_id):
                    return snapshot
        primary = next(
            (
                snapshot
                for snapshot in choices
                if self._station_command_bool(self._station_command_value(snapshot, "runtime_primary", False))
            ),
            None,
        )
        if primary is not None:
            return primary
        active = next(
            (
                snapshot
                for snapshot in choices
                if self._station_command_bool(self._station_command_value(snapshot, "runtime_active", False))
            ),
            None,
        )
        if active is not None:
            return active
        return choices[0] if choices else None

    def _station_command_now_text_for_summary(self, snapshot: object, selected_id: int) -> str:
        ident = self._station_command_snapshot_id(snapshot)
        if ident > 0 and ident == int(selected_id or 0):
            return self._station_command_now_text(snapshot)
        group, band = self._station_command_assigned_plan_group_band(snapshot)
        if not group:
            group, band = self._station_command_schedule_group_band(snapshot)
        if not group:
            group, band = self._station_command_group_band_for_frequency(snapshot)
        if group:
            return " ".join(part for part in (group, band) if part).strip()
        return self._station_command_frequency_text(snapshot)

    @staticmethod
    def _station_command_compact_state_text(state_text: object) -> str:
        text = str(state_text or "").strip()
        if not text:
            return "Ready"
        if text.lower() in {"ok", "ready"}:
            return "Ready"
        if text.lower().startswith("configured"):
            return "Inactive"
        return text

    def _station_command_radio_summary_text(self, snapshot: object, selected_id: int) -> str:
        name = self._station_command_snapshot_name(snapshot)
        now = self._station_command_now_text_for_summary(snapshot, selected_id)
        state = self._station_command_compact_state_text(self._station_command_state_text(snapshot))
        parts = [name]
        if now:
            parts.append(now)
        if state and state.lower() not in {"on schedule"}:
            parts.append(state)
        return " | ".join(parts)

    def _station_command_radio_summary_tooltip(self, snapshot: object, selected_id: int) -> str:
        name = self._station_command_snapshot_name(snapshot)
        now = self._station_command_now_text_for_summary(snapshot, selected_id)
        exact = self._station_command_frequency_text(snapshot)
        state = self._station_command_compact_state_text(self._station_command_state_text(snapshot))
        next_text = self._station_command_next_text(snapshot)
        return f"{name}\nNow: {now}\nFrequency: {exact}\nState: {state}\nNext: {next_text}"

    def _station_command_active_radio_choices(self, choices: list[object]) -> list[object]:
        active = [
            snapshot
            for snapshot in choices
            if self._station_command_value(snapshot, "runtime_active", None) is None
            or self._station_command_bool(self._station_command_value(snapshot, "runtime_active", False), default=False)
        ]
        return active if active else choices

    def _station_command_radio_cards_per_page(self, total: int) -> int:
        if total <= 1:
            return max(1, int(total or 1))
        try:
            width = int(getattr(self.station_command_bar, "width", lambda: 0)() or self.width() or 0) - 80
        except Exception:
            width = 700
        if width <= 0:
            width = 700
        width = max(280, width)
        per_page = max(1, width // 330)
        if width >= 660:
            per_page = max(2, per_page)
        return max(1, min(int(total), int(per_page)))

    def _station_command_radio_card_width(self, count: int) -> int:
        try:
            width = int(getattr(self.station_command_bar, "width", lambda: 0)() or self.width() or 0) - 80
        except Exception:
            width = 660
        if width <= 0:
            width = 660
        if int(count or 1) <= 1:
            return max(430, min(900, width))
        gaps = max(0, int(count or 1) - 1) * 6
        available = max(220, width - gaps)
        return max(240, min(430, available // max(1, int(count or 1))))

    def _station_command_radio_page_slice(self, choices: list[object]) -> tuple[list[object], int, int, int]:
        total = len(choices)
        per_page = self._station_command_radio_cards_per_page(total)
        page_count = max(1, (total + per_page - 1) // per_page)
        try:
            page = int(getattr(self, "_station_command_radio_page", 0) or 0)
        except Exception:
            page = 0
        page = max(0, min(page, page_count - 1))
        self._station_command_radio_page = page
        start = page * per_page
        return choices[start : start + per_page], page, page_count, per_page

    def _change_station_command_radio_page(self, delta: int) -> None:
        try:
            current = int(getattr(self, "_station_command_radio_page", 0) or 0)
        except Exception:
            current = 0
        self._station_command_radio_page = max(0, current + int(delta or 0))
        self._station_command_radio_summary_signature = None
        self._refresh_station_command_bar(force=True)

    def _style_station_command_radio_summary_button(self, button: QPushButton, *, selected: bool, state_text: str) -> None:
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = {}
        state = str(state_text or "").strip().lower()
        role = "muted" if state in {"inactive", "configured inactive", "not enabled"} else "info"
        if state in {"ptt active", "rf guard blocked", "blocked", "error", "failed"}:
            role = "danger"
        elif state in {"manual hold", "manual qsy", "scheduler suspended", "needs review", "warning", "warn"}:
            role = "warning"
        font = button.font()
        font.setBold(True)
        button.setFont(font)
        button.setStyleSheet(button_style(role, theme))

    def _refresh_station_command_radio_summary(self, choices: list[object], selected_id: int) -> None:
        layout = getattr(self, "station_command_radio_summary_layout", None)
        if layout is None:
            return
        visible_choices = list(choices)
        if not visible_choices:
            self._station_command_radio_tile_controls = {}
            signature = ("empty",)
            if signature == getattr(self, "_station_command_radio_summary_signature", None):
                return
            self._station_command_radio_summary_signature = signature
            while layout.count():
                item = layout.takeAt(0)
                widget = item.widget()
                if widget is not None:
                    widget.setParent(None)
                    widget.deleteLater()
            empty = QLabel("No configured radios", getattr(self, "station_command_radio_summary_widget", None))
            empty.setObjectName("stationCommandRadioSummaryEmpty")
            empty.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
            layout.addWidget(empty)
            layout.addStretch(1)
            return
        if len(visible_choices) <= 2:
            self._station_command_radio_page = 0
            page_choices = visible_choices
        else:
            page_choices, _page, _page_count, _per_page = self._station_command_radio_page_slice(visible_choices)
        signature = (
            "tiles",
            int(selected_id or 0),
            int(getattr(self, "_station_command_radio_page", 0) or 0),
            len(visible_choices),
            self._station_command_radio_card_width(len(page_choices)),
            tuple(
                (
                    self._station_command_snapshot_id(snapshot),
                    self._station_command_snapshot_name(snapshot),
                    self._station_command_now_text_for_summary(snapshot, selected_id),
                    self._station_command_compact_state_text(self._station_command_state_text(snapshot)),
                    self._station_command_next_text(snapshot),
                    self._station_command_plan_name_for_snapshot(snapshot),
                    tuple(
                        sorted(
                            (
                                str(meta.get("group") or "").strip().upper(),
                                str(meta.get("band") or "").strip().upper(),
                                f"{float(meta.get('freq') or 0.0):.6f}",
                            )
                            for meta in self._station_command_plan_qsy_options(snapshot).values()
                        )
                    ),
                    str(self._station_command_health_summary_for_profile(snapshot).get("state", "")),
                    self._station_command_scheduler_manual_qsy_active_for_radio(
                        self._station_command_snapshot_id(snapshot)
                    ),
                    self._station_command_scheduler_suspended_manually_for_radio(
                        self._station_command_snapshot_id(snapshot)
                    ),
                    self._station_command_timed_suspend_active_for_radio(
                        self._station_command_snapshot_id(snapshot)
                    ),
                )
                for snapshot in page_choices
            ),
        )
        if signature == getattr(self, "_station_command_radio_summary_signature", None):
            return
        self._station_command_radio_summary_signature = signature
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()
        self._station_command_radio_tile_controls = {}
        self._refresh_station_command_radio_tiles(page_choices, selected_id)

    def _station_command_set_qsy_combo_to_meta(self, meta: Mapping[str, object] | None) -> None:
        combo = getattr(self, "station_command_freq_combo", None)
        if not isinstance(combo, QComboBox) or not isinstance(meta, Mapping):
            return
        try:
            target_key = f"{float(meta.get('freq')):.6f}"
        except Exception:
            target_key = ""
        if not target_key:
            return
        for index in range(combo.count()):
            data = combo.itemData(index)
            try:
                if isinstance(data, Mapping) and f"{float(data.get('freq')):.6f}" == target_key:
                    combo.setCurrentIndex(index)
                    return
            except Exception:
                continue

    @staticmethod
    def _station_command_combo_selected_key(combo: QComboBox) -> str:
        try:
            meta = selected_qsy_meta(combo)
            if isinstance(meta, Mapping):
                return f"{float(meta.get('freq')):.6f}"
        except Exception:
            pass
        return ""

    def _station_command_for_radio_qsy(
        self,
        device_profile_id: int,
        meta: Mapping[str, object] | QComboBox | None,
        callback: Callable[..., None],
        duration_combo: QComboBox | None = None,
    ) -> Callable[..., None]:
        def run(*_args: object) -> None:
            current_meta: Mapping[str, object] | None
            if isinstance(meta, QComboBox):
                selected_meta = selected_qsy_meta(meta)
                current_meta = selected_meta if isinstance(selected_meta, Mapping) else None
            else:
                current_meta = meta
            ident = int(device_profile_id or 0)
            direct_meta = dict(current_meta) if isinstance(current_meta, Mapping) else None
            if direct_meta is not None and ident > 0:
                direct_meta["target_device_profile_id"] = ident
            duration_minutes = None
            if isinstance(duration_combo, QComboBox) and isinstance(getattr(self, "station_command_duration_combo", None), QComboBox):
                try:
                    value = duration_combo.currentData()
                    duration_minutes = int(value)
                    for index in range(self.station_command_duration_combo.count()):
                        if self.station_command_duration_combo.itemData(index) == value:
                            self.station_command_duration_combo.setCurrentIndex(index)
                            break
                except Exception:
                    pass
            if duration_combo is not None:
                callback(direct_meta, duration_minutes)
            else:
                callback(direct_meta)

        return run

    def _station_command_populate_card_frequency_combo(self, combo: QComboBox, snapshot: object | None) -> bool:
        meta_map = self._station_command_plan_qsy_options(snapshot)
        preferred_key = self._station_command_preferred_qsy_key(snapshot)
        has_options = False
        previous_block = combo.blockSignals(True)
        try:
            combo.clear()
            for key, meta in sorted(
                meta_map.items(),
                key=lambda item: (
                    str(item[1].get("group") or "").upper(),
                    str(item[1].get("band") or "").upper(),
                    float(item[1].get("freq") or 0.0),
                ),
            ):
                combo.addItem(self._station_command_qsy_label(meta), meta)
                combo.setItemData(combo.count() - 1, self._station_command_qsy_tooltip(meta), Qt.ToolTipRole)
                has_options = True
            if not has_options:
                plan_name = ""
                if snapshot is not None:
                    plan_name = self._station_command_assigned_plan_name_for_radio(self._station_command_snapshot_id(snapshot))
                combo.addItem("No plan QSY targets" if plan_name else "No assigned plan", None)
            elif preferred_key:
                for index in range(combo.count()):
                    data = combo.itemData(index)
                    try:
                        if isinstance(data, Mapping) and f"{float(data.get('freq')):.6f}" == preferred_key:
                            combo.setCurrentIndex(index)
                            break
                    except Exception:
                        continue
        finally:
            combo.blockSignals(previous_block)
        return has_options and selected_qsy_meta(combo) is not None

    def _station_command_operating_group_qsy_lookup(self) -> dict[tuple[str, str], dict[str, object]]:
        lookup: dict[tuple[str, str], dict[str, object]] = {}
        try:
            for meta in build_qsy_options(load_operating_groups(self.settings)).values():
                if not isinstance(meta, Mapping):
                    continue
                group = self._station_command_group_display_name(meta.get("group"))
                band = str(meta.get("band") or "").strip().upper()
                if group and band:
                    lookup.setdefault((group.upper(), band), dict(meta))
        except Exception:
            pass
        return lookup

    def _station_command_plan_qsy_options(self, snapshot: object | None) -> dict[str, dict[str, object]]:
        ident = self._station_command_snapshot_id(snapshot) if snapshot is not None else 0
        lookup = self._station_command_operating_group_qsy_lookup()
        options: dict[str, dict[str, object]] = {}
        if ident > 0:
            for item in self._station_command_assigned_plan_refs_for_radio(ident):
                meta = self._station_command_qsy_meta_from_plan_ref(item, lookup)
                if not meta:
                    continue
                try:
                    freq_key = f"{float(meta.get('freq')):.6f}"
                except Exception:
                    continue
                option_key = "|".join(
                    (
                        str(meta.get("group") or "").strip().upper(),
                        str(meta.get("band") or "").strip().upper(),
                        freq_key,
                    )
                )
                options.setdefault(option_key, meta)
            if options:
                return options
        lane_rows = self._station_command_lane_schedule_rows(ident) if ident > 0 else []
        for item in lane_rows:
            meta = self._station_command_qsy_meta_from_plan_ref(item, lookup)
            if not meta:
                continue
            try:
                freq_key = f"{float(meta.get('freq')):.6f}"
            except Exception:
                continue
            option_key = "|".join(
                (
                    str(meta.get("group") or "").strip().upper(),
                    str(meta.get("band") or "").strip().upper(),
                    freq_key,
                )
            )
            options.setdefault(option_key, meta)
        return options

    def _station_command_qsy_meta_from_plan_ref(
        self,
        ref: object,
        lookup: Mapping[tuple[str, str], Mapping[str, object]],
    ) -> dict[str, object] | None:
        if isinstance(ref, Mapping):
            group = self._station_command_group_display_name(ref.get("group_name") or ref.get("group") or ref.get("label"))
            band = str(ref.get("band") or ref.get("band_name") or "").strip().upper()
            freq_value = (
                ref.get("frequency")
                or ref.get("freq")
                or ref.get("frequency_mhz")
                or ref.get("name")
            )
            mode_value = ref.get("mode")
            vfo_value = ref.get("vfo")
            fldigi_mode_value = ref.get("fldigi_mode")
            fldigi_offset_value = ref.get("fldigi_offset")
        else:
            text = str(ref or "").strip()
            group = ""
            band = ""
            freq_value = text
            mode_value = ""
            vfo_value = "A"
            fldigi_mode_value = ""
            fldigi_offset_value = ""
            band_match = re.search(r"\b(160|80|60|40|30|20|17|15|12|10|6|2)\s*M\b", text, flags=re.IGNORECASE)
            if band_match:
                band = f"{band_match.group(1).upper()}M"
            group_match = re.search(r"\b([A-Z][A-Z0-9_-]{2,})\b", text.upper())
            if group_match:
                candidate = group_match.group(1)
                if candidate != band:
                    group = self._station_command_group_display_name(candidate)
        freq = parse_frequency_mhz(freq_value)
        if not band and freq is not None:
            band = self._station_command_band_from_frequency(freq)
        resolved = dict(lookup.get((group.upper(), band), {})) if group and band else {}
        if freq is None:
            freq = parse_frequency_mhz(resolved.get("freq"))
        if not group:
            group = self._station_command_group_display_name(resolved.get("group"))
        if not band:
            band = str(resolved.get("band") or "").strip().upper()
        if freq is None:
            return None
        return {
            "group": group,
            "band": band,
            "freq": float(freq),
            "mode": str(mode_value or resolved.get("mode") or "").strip(),
            "vfo": str(vfo_value or resolved.get("vfo") or "A").strip().upper() or "A",
            "fldigi_mode": str(fldigi_mode_value or resolved.get("fldigi_mode") or "").strip(),
            "fldigi_offset": str(fldigi_offset_value or resolved.get("fldigi_offset") or "").strip(),
        }

    @staticmethod
    def _station_command_band_from_frequency(freq_mhz: object) -> str:
        try:
            value = float(freq_mhz)
        except Exception:
            return ""
        bands = (
            ("160M", 1.8, 2.0),
            ("80M", 3.5, 4.0),
            ("60M", 5.0, 5.5),
            ("40M", 7.0, 7.3),
            ("30M", 10.1, 10.15),
            ("20M", 14.0, 14.35),
            ("17M", 18.068, 18.168),
            ("15M", 21.0, 21.45),
            ("12M", 24.89, 24.99),
            ("10M", 28.0, 29.7),
            ("6M", 50.0, 54.0),
            ("2M", 144.0, 148.0),
        )
        for band, lo, hi in bands:
            if lo <= value <= hi:
                return band
        return ""

    def _refresh_station_command_radio_tiles(self, choices: list[object], selected_id: int) -> None:
        layout = getattr(self, "station_command_radio_summary_layout", None)
        parent = getattr(self, "station_command_radio_summary_widget", None)
        if layout is None or parent is None:
            return
        tile_controls: dict[int, dict[str, object]] = {}
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = {}
        pending_qsy_keys = getattr(self, "_station_command_card_qsy_pending_keys", None)
        if not isinstance(pending_qsy_keys, dict):
            pending_qsy_keys = {}
            self._station_command_card_qsy_pending_keys = pending_qsy_keys
        card_width = self._station_command_radio_card_width(len(choices))
        for snapshot in choices:
            ident = self._station_command_snapshot_id(snapshot)
            selected = ident > 0 and ident == int(selected_id or 0)
            state = self._station_command_compact_state_text(self._station_command_state_text(snapshot))
            manual_qsy_active = self._station_command_scheduler_manual_qsy_active_for_radio(ident)
            radio_hold_snapshot = self._station_command_hold_snapshot_for_radio(ident)
            radio_hold_active = bool(radio_hold_snapshot.get("active"))
            timed_qsy_active = bool(radio_hold_active and manual_qsy_active)
            timed_suspend_active = self._station_command_timed_suspend_active_for_radio(ident)
            scheduler_suspended_manual = self._station_command_scheduler_suspended_manually_for_radio(ident)
            now = self._station_command_now_text_for_summary(snapshot, selected_id)
            next_text = self._station_command_next_text(snapshot)
            plan_text = self._station_command_display_plan_name(self._station_command_plan_name_for_snapshot(snapshot))
            health_summary = self._station_command_health_summary_for_profile(snapshot)
            health_state = str(health_summary.get("state", "warn") or "warn").strip().lower()
            tile = QFrame(parent)
            tile.setObjectName("stationCommandRadioTile")
            tile.setProperty("selected", "true" if selected else "false")
            tile.setFrameShape(QFrame.StyledPanel)
            tile.setMinimumWidth(card_width)
            tile.setMaximumWidth(card_width)
            tile.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            tile_layout = QGridLayout(tile)
            tile_layout.setContentsMargins(8, 7, 8, 7)
            tile_layout.setHorizontalSpacing(8)
            tile_layout.setVerticalSpacing(5)

            name_btn = QPushButton(self._station_command_snapshot_name(snapshot), tile)
            name_btn.setObjectName("stationCommandRadioTileName")
            name_btn.setCheckable(True)
            name_btn.setChecked(selected)
            name_btn.setToolTip(self._station_command_radio_summary_tooltip(snapshot, selected_id))
            name_btn.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
            self._style_station_command_radio_summary_button(name_btn, selected=selected, state_text=state)
            if ident > 0:
                name_btn.clicked.connect(lambda _checked=False, profile_id=ident: self._on_station_command_summary_radio_clicked(profile_id))

            freq_combo = QComboBox(tile)
            freq_combo.setObjectName("stationCommandRadioTileFrequency")
            freq_combo.setToolTip("Select the operating group and band for this radio.")
            freq_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            has_card_qsy_target = self._station_command_populate_card_frequency_combo(freq_combo, snapshot)
            preferred_key_for_card = self._station_command_preferred_qsy_key(snapshot)
            pending_key = str(pending_qsy_keys.get(ident, "") or "")
            if pending_key:
                for index in range(freq_combo.count()):
                    data = freq_combo.itemData(index)
                    try:
                        if isinstance(data, Mapping) and f"{float(data.get('freq')):.6f}" == pending_key:
                            freq_combo.setCurrentIndex(index)
                            break
                    except Exception:
                        continue
            freq_combo.setProperty(
                "stationCommandSelectionDirty",
                bool(pending_key and pending_key != preferred_key_for_card),
            )
            freq_combo.setProperty("stationCommandPreferredKey", preferred_key_for_card)
            now_font = freq_combo.font()
            now_font.setBold(True)
            now_font.setPointSize(max(now_font.pointSize(), 13))
            freq_combo.setFont(now_font)
            next_label = ElidedLabel(f"Next: {next_text} | Plan: {plan_text}", tile)
            next_label.setObjectName("stationCommandRadioTileNext")
            next_label.setMinimumWidth(0)
            next_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            next_label.setToolTip(f"Next: {next_text}\nPlan: {plan_text}\nClick Change Plan to manage assignment.")

            qsy_btn = QPushButton("QSY", tile)
            qsy_btn.setMinimumWidth(76)
            qsy_btn.setMaximumWidth(96)
            qsy_btn.setToolTip(f"Select {self._station_command_snapshot_name(snapshot)} and send the selected manual QSY target.")
            qsy_btn.clicked.connect(
                self._station_command_for_radio_qsy(
                    ident,
                    freq_combo,
                    self._on_station_command_qsy_now_clicked,
                )
            )
            duration_combo = QComboBox(tile)
            duration_combo.setObjectName("stationCommandRadioTileDuration")
            refresh_hold_duration_combo(duration_combo, self.settings, getattr(self, "_active_runtime_profile", None))
            duration_combo.setVisible(False)
            timer_btn = QToolButton(tile)
            timer_btn.setObjectName("stationCommandRadioTileTimedSuspend")
            timer_btn.setText(timed_qsy_text(timed_qsy_active=timed_qsy_active))
            timer_btn.setPopupMode(QToolButton.MenuButtonPopup)
            timer_btn.setMinimumWidth(128)
            timer_btn.setMaximumWidth(150)
            timer_btn.setToolTip(f"Select {self._station_command_snapshot_name(snapshot)} and QSY with a timed scheduler suspend.")
            timer_btn.clicked.connect(
                self._station_command_for_radio_qsy(
                    ident,
                    freq_combo,
                    self._on_station_command_qsy_hold_clicked,
                    duration_combo,
                )
            )
            duration_menu = QMenu(timer_btn)
            duration_menu.setObjectName("stationCommandTimedSuspendMenu")
            manual_qsy_action = QAction("Indefinite", duration_menu)
            manual_qsy_action.setToolTip("QSY and keep scheduler control suspended until Resume.")
            manual_qsy_action.triggered.connect(
                lambda _checked=False,
                radio_id=ident,
                combo=freq_combo: self._station_command_for_radio_qsy(
                    radio_id,
                    combo,
                    self._on_station_command_qsy_now_clicked,
                )()
            )
            duration_menu.addAction(manual_qsy_action)
            duration_menu.addSeparator()
            for duration_index in range(duration_combo.count()):
                label = duration_combo.itemText(duration_index)
                value = duration_combo.itemData(duration_index)
                action = QAction(label, duration_menu)
                action.triggered.connect(
                    lambda _checked=False,
                    combo=duration_combo,
                    duration_value=value,
                    button=timer_btn: (
                        combo.setCurrentIndex(
                            next(
                                (idx for idx in range(combo.count()) if combo.itemData(idx) == duration_value),
                                combo.currentIndex(),
                            )
                        ),
                        button.click(),
                    )
                )
                duration_menu.addAction(action)
            timer_btn.setMenu(duration_menu)
            suspend_btn = QToolButton(tile)
            suspend_btn.setObjectName("stationCommandRadioTileSchedulerSuspend")
            scheduler_actions = scheduler_action_state(
                manual_qsy_active=manual_qsy_active,
                timed_qsy_active=timed_qsy_active,
                timed_suspend_active=timed_suspend_active,
                scheduler_suspended_manual=scheduler_suspended_manual,
                scheduler_state_text=state,
            )
            suspend_btn.setText(scheduler_actions.timed_suspend_text)
            suspend_btn.setPopupMode(QToolButton.MenuButtonPopup)
            suspend_btn.setMinimumWidth(128)
            suspend_btn.setMaximumWidth(150)
            suspend_btn.setToolTip(f"Suspend scheduler control for {self._station_command_snapshot_name(snapshot)}.")
            suspend_btn.setEnabled(ident > 0)
            suspend_btn.clicked.connect(
                lambda _checked=False, radio_id=ident: self._on_station_command_timed_suspend_clicked(radio_id)
            )
            suspend_menu = QMenu(suspend_btn)
            suspend_menu.setObjectName("stationCommandSchedulerSuspendMenu")
            manual_suspend_action = QAction("Indefinite", suspend_menu)
            manual_suspend_action.setToolTip("Suspend scheduler control until Resume.")
            manual_suspend_action.triggered.connect(
                lambda _checked=False, radio_id=ident: self._on_station_command_pause_clicked(radio_id)
            )
            suspend_menu.addAction(manual_suspend_action)
            suspend_menu.addSeparator()
            for duration_index in range(duration_combo.count()):
                label = duration_combo.itemText(duration_index)
                value = duration_combo.itemData(duration_index)
                action = QAction(label, suspend_menu)
                action.triggered.connect(
                    lambda _checked=False,
                    combo=duration_combo,
                    duration_value=value,
                    button=suspend_btn: (
                        combo.setCurrentIndex(
                            next(
                                (idx for idx in range(combo.count()) if combo.itemData(idx) == duration_value),
                                combo.currentIndex(),
                            )
                        ),
                        button.click(),
                    )
                )
                suspend_menu.addAction(action)
            suspend_btn.setMenu(suspend_menu)
            resume_btn = QPushButton("Resume", tile)
            resume_btn.setToolTip(f"Resume scheduled control for {self._station_command_snapshot_name(snapshot)}.")
            resume_btn.setEnabled(ident > 0)
            resume_btn.clicked.connect(
                lambda _checked=False, radio_id=ident: self._on_station_command_resume_clicked(radio_id)
            )
            health_btn = QPushButton("Health", tile)
            health_btn.setObjectName("stationCommandRadioTileHealth")
            health_btn.setToolTip(self._station_command_radio_summary_tooltip(snapshot, selected_id))
            health_btn.setEnabled(ident > 0)
            health_btn.clicked.connect(
                lambda _checked=False, profile_id=ident, snap=snapshot, button=health_btn: self._show_station_command_health_menu(
                    device_profile_id=profile_id,
                    snapshot=snap,
                    anchor=button,
                )
            )
            assign_btn = QPushButton("Change Plan", tile)
            assign_btn.setObjectName("stationCommandRadioTileAssign")
            assign_btn.setToolTip(f"Assign or change the Frequency Plan for {self._station_command_snapshot_name(snapshot)}.")
            assign_btn.setEnabled(ident > 0)
            assign_btn.clicked.connect(lambda _checked=False, profile_id=ident: self._open_schedule_assignment_for_radio(profile_id))
            def _update_card_qsy_buttons(
                _index: int = -1,
                *,
                radio_id: int = ident,
                qsy_button: QPushButton = qsy_btn,
                timer_button: QPushButton = timer_btn,
                combo: QComboBox = freq_combo,
                manual_active: bool = manual_qsy_active,
                timed_qsy: bool = timed_qsy_active,
                card_snapshot: object = snapshot,
            ) -> None:
                preferred_key = self._station_command_preferred_qsy_key(card_snapshot)
                stable_preferred_key = str(combo.property("stationCommandPreferredKey") or preferred_key or "")
                selected_key = self._station_command_combo_selected_key(combo)
                action_state = qsy_action_state(
                    selected_meta=selected_qsy_meta(combo),
                    preferred_key=stable_preferred_key,
                    radio_id=radio_id,
                    selection_changed=bool(
                        selected_key
                        and (
                            (stable_preferred_key and selected_key != stable_preferred_key)
                            or (not stable_preferred_key and combo.property("stationCommandSelectionDirty"))
                        )
                    ),
                    manual_qsy_active=manual_active,
                    timed_qsy_active=timed_qsy,
                )
                qsy_button.setText("QSY")
                qsy_button.setEnabled(action_state.qsy_enabled)
                timer_button.setEnabled(action_state.timed_qsy_enabled)
                qsy_button.setStyleSheet(button_style(action_state.qsy_role, theme))
                timer_button.setStyleSheet(button_style(action_state.timed_qsy_role, theme))

            def _on_card_frequency_changed(
                _index: int = -1,
                *,
                combo: QComboBox = freq_combo,
                radio_id: int = ident,
                update_buttons: Callable[[int], None] = _update_card_qsy_buttons,
            ) -> None:
                selected_key = self._station_command_combo_selected_key(combo)
                if selected_key:
                    pending_qsy_keys[radio_id] = selected_key
                else:
                    pending_qsy_keys.pop(radio_id, None)
                combo.setProperty("stationCommandSelectionDirty", True)
                update_buttons(_index)

            freq_combo.currentIndexChanged.connect(_on_card_frequency_changed)
            for btn, role in (
                (qsy_btn, "muted"),
                (timer_btn, "warning" if timed_qsy_active else "muted"),
                (suspend_btn, scheduler_actions.timed_suspend_role),
                (resume_btn, scheduler_actions.resume_role),
                (
                    health_btn,
                    "danger"
                    if health_state == "error"
                    else "warning"
                    if health_state in {"warn", "warning", "review"}
                    else "success",
                ),
                (assign_btn, "muted"),
            ):
                btn.setStyleSheet(button_style(role, theme))
                btn.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
            _update_card_qsy_buttons()

            tile_layout.addWidget(name_btn, 0, 0, 1, 3)
            tile_layout.addWidget(health_btn, 0, 3)
            tile_layout.addWidget(freq_combo, 1, 0, 1, 4)
            tile_layout.addWidget(qsy_btn, 2, 0)
            tile_layout.addWidget(timer_btn, 2, 1)
            tile_layout.addWidget(suspend_btn, 2, 2)
            tile_layout.addWidget(next_label, 3, 0, 1, 4)
            tile_layout.addWidget(resume_btn, 4, 0, 1, 2)
            tile_layout.addWidget(assign_btn, 4, 2, 1, 2)
            tile_layout.setColumnStretch(0, 0)
            tile_layout.setColumnStretch(1, 0)
            tile_layout.setColumnStretch(2, 0)
            tile_layout.setColumnStretch(3, 1)
            tile_layout.setColumnMinimumWidth(0, 76)
            tile_layout.setColumnMinimumWidth(1, 128)
            tile_layout.setColumnMinimumWidth(2, 128)
            if ident > 0:
                tile_controls[ident] = {
                    "qsy_btn": qsy_btn,
                    "timer_btn": timer_btn,
                    "suspend_btn": suspend_btn,
                    "resume_btn": resume_btn,
                    "freq_combo": freq_combo,
                }
            try:
                tile.style().unpolish(tile)
                tile.style().polish(tile)
            except Exception:
                pass
            layout.addWidget(tile)
        layout.addStretch(1)
        self._station_command_radio_tile_controls = tile_controls

    def _update_station_command_radio_tile_hold_controls(self, hold_snapshot: Mapping[str, object]) -> None:
        controls_by_radio = getattr(self, "_station_command_radio_tile_controls", {})
        if not isinstance(controls_by_radio, Mapping):
            return
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = {}
        for raw_radio_id, controls in list(controls_by_radio.items()):
            if not isinstance(controls, Mapping):
                continue
            try:
                radio_id = int(raw_radio_id or 0)
            except Exception:
                radio_id = 0
            radio_hold_snapshot = self._station_command_hold_snapshot_for_radio(radio_id)
            radio_hold_active = bool(radio_hold_snapshot.get("active"))
            countdown = self._station_command_countdown_text(radio_hold_snapshot.get("remaining_sec")) if radio_hold_active else ""
            timed_qsy_active = bool(radio_hold_active and self._station_command_scheduler_manual_qsy_active_for_radio(radio_id))
            timed_suspend_active = self._station_command_timed_suspend_active_for_radio(radio_id)
            scheduler_suspended_manual = self._station_command_scheduler_suspended_manually_for_radio(radio_id)
            manual_qsy_active = self._station_command_scheduler_manual_qsy_active_for_radio(radio_id)
            qsy_btn = controls.get("qsy_btn")
            timer_btn = controls.get("timer_btn")
            suspend_btn = controls.get("suspend_btn")
            resume_btn = controls.get("resume_btn")
            combo = controls.get("freq_combo")
            if isinstance(qsy_btn, QPushButton) and isinstance(combo, QComboBox):
                preferred_key = str(combo.property("stationCommandPreferredKey") or "")
                selected_key = self._station_command_combo_selected_key(combo)
                action_state = qsy_action_state(
                    selected_meta=selected_qsy_meta(combo),
                    preferred_key=preferred_key,
                    radio_id=radio_id,
                    selection_changed=bool(
                        selected_key
                        and (
                            (preferred_key and selected_key != preferred_key)
                            or (not preferred_key and combo.property("stationCommandSelectionDirty"))
                        )
                    ),
                    manual_qsy_active=manual_qsy_active,
                    timed_qsy_active=timed_qsy_active,
                )
                qsy_btn.setText("QSY")
                qsy_btn.setEnabled(action_state.qsy_enabled)
                qsy_btn.setStyleSheet(button_style(action_state.qsy_role, theme))
            if isinstance(timer_btn, QToolButton):
                timer_btn.setText(f"{countdown} | Extend" if timed_qsy_active and countdown else timed_qsy_text(timed_qsy_active=timed_qsy_active))
                if isinstance(combo, QComboBox):
                    preferred_key = str(combo.property("stationCommandPreferredKey") or "")
                    selected_key = self._station_command_combo_selected_key(combo)
                    action_state = qsy_action_state(
                        selected_meta=selected_qsy_meta(combo),
                        preferred_key=preferred_key,
                        radio_id=radio_id,
                        selection_changed=bool(
                            selected_key
                            and (
                                (preferred_key and selected_key != preferred_key)
                                or (not preferred_key and combo.property("stationCommandSelectionDirty"))
                            )
                        ),
                        manual_qsy_active=manual_qsy_active,
                        timed_qsy_active=timed_qsy_active,
                    )
                    timer_btn.setEnabled(action_state.timed_qsy_enabled)
                    timer_btn.setStyleSheet(button_style(action_state.timed_qsy_role, theme))
                else:
                    timer_btn.setStyleSheet(button_style("warning" if timed_qsy_active else "muted", theme))
            if isinstance(suspend_btn, QToolButton):
                scheduler_actions = scheduler_action_state(
                    manual_qsy_active=manual_qsy_active,
                    timed_qsy_active=timed_qsy_active,
                    timed_suspend_active=timed_suspend_active,
                    scheduler_suspended_manual=scheduler_suspended_manual,
                    scheduler_state_text="Scheduler Suspended" if scheduler_suspended_manual else "",
                )
                suspend_text = (
                    f"{countdown} | Extend"
                    if timed_suspend_active and countdown
                    else scheduler_actions.timed_suspend_text
                )
                suspend_btn.setText(suspend_text)
                suspend_btn.setStyleSheet(button_style(scheduler_actions.timed_suspend_role, theme))
            if isinstance(resume_btn, QPushButton):
                resume_active = manual_qsy_active or timed_qsy_active or timed_suspend_active or scheduler_suspended_manual
                resume_btn.setEnabled(radio_id > 0 and resume_active)
                resume_btn.setStyleSheet(button_style("warning" if resume_active else "muted", theme))

    def _clear_station_command_admin_layout(self) -> None:
        layout = getattr(self, "station_command_radio_admin_layout", None)
        if layout is None:
            return
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()

    def _station_command_plan_name_for_snapshot(self, snapshot: object) -> str:
        ident = self._station_command_snapshot_id(snapshot)
        if ident > 0:
            assigned_name = self._station_command_assigned_plan_name_for_radio(ident)
            if assigned_name:
                return assigned_name
        for key in (
            "frequency_plan_name",
            "assigned_frequency_plan_name",
            "assigned_schedule_name",
        ):
            text = str(self._station_command_value(snapshot, key, "") or "").strip()
            if text:
                return text
        return "Unassigned"

    def _refresh_station_command_radio_admin(self, choices: list[object], selected_id: int) -> None:
        panel = getattr(self, "station_command_radio_admin_panel", None)
        layout = getattr(self, "station_command_radio_admin_layout", None)
        if panel is None or layout is None:
            return
        expanded = bool(getattr(self, "_station_command_radio_admin_expanded", False))
        panel.setVisible(expanded)
        admin_btn = getattr(self, "station_command_radio_admin_btn", None)
        if admin_btn is not None:
            admin_btn.setText("Hide Radios" if expanded else "All Radios")
        self._clear_station_command_admin_layout()
        if not expanded:
            return
        if not choices:
            layout.addWidget(QLabel("No configured radios are available for station control.", panel))
            return
        for snapshot in choices:
            ident = self._station_command_snapshot_id(snapshot)
            name = self._station_command_snapshot_name(snapshot)
            now = self._station_command_now_text_for_summary(snapshot, selected_id)
            state = self._station_command_compact_state_text(self._station_command_state_text(snapshot))
            plan_name = self._station_command_display_plan_name(self._station_command_plan_name_for_snapshot(snapshot))
            row = QFrame(panel)
            row.setObjectName("stationCommandRadioAdminRow")
            row.setFrameShape(QFrame.StyledPanel)
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(8, 4, 8, 4)
            row_layout.setSpacing(8)
            title = QLabel(name, row)
            title.setMinimumWidth(110)
            title.setStyleSheet("font-weight: 800;")
            title.setToolTip(self._station_command_radio_summary_tooltip(snapshot, selected_id))
            now_label = QLabel(now, row)
            now_label.setMinimumWidth(160)
            now_label.setToolTip(self._station_command_frequency_text(snapshot))
            state_label = QLabel(state, row)
            state_label.setMinimumWidth(120)
            plan_label = QLabel(f"Plan: {plan_name}", row)
            plan_label.setMinimumWidth(180)
            plan_label.setToolTip(plan_name)
            select_btn = QPushButton("Select", row)
            select_btn.setEnabled(ident > 0 and ident != int(selected_id or 0))
            select_btn.clicked.connect(lambda _checked=False, profile_id=ident: self._on_station_command_summary_radio_clicked(profile_id))
            assign_btn = QPushButton("Assign Plan", row)
            assign_btn.setEnabled(ident > 0)
            assign_btn.clicked.connect(lambda _checked=False, profile_id=ident: self._open_schedule_assignment_for_radio(profile_id))
            health_btn = QPushButton("Health", row)
            health_btn.setEnabled(ident > 0)
            health_btn.clicked.connect(lambda _checked=False, profile_id=ident: self._open_station_health_detail(device_profile_id=profile_id))
            row_layout.addWidget(title)
            row_layout.addWidget(now_label)
            row_layout.addWidget(state_label)
            row_layout.addWidget(plan_label, 1)
            row_layout.addWidget(select_btn)
            row_layout.addWidget(assign_btn)
            row_layout.addWidget(health_btn)
            layout.addWidget(row)
        layout.addStretch(1)

    def _toggle_station_command_radio_admin(self) -> None:
        self._station_command_radio_admin_expanded = not bool(getattr(self, "_station_command_radio_admin_expanded", False))
        self._refresh_station_command_bar(force=True)

    def _open_schedule_assignment_for_radio(self, device_profile_id: int) -> None:
        ident = int(device_profile_id or 0)
        if ident <= 0:
            return
        self.open_settings_section("schedule_assignments", radio_id=ident, settings_nav_context="radios")
        settings_tab = getattr(self, "settings_tab", None)
        opener = getattr(settings_tab, "open_schedule_assignment_editor", None)
        if callable(opener):
            QTimer.singleShot(0, lambda profile_id=ident: opener(device_profile_id=profile_id))

    def _on_station_command_summary_radio_clicked(self, device_profile_id: int) -> None:
        ident = int(device_profile_id or 0)
        if ident <= 0:
            return
        activated = self._activate_station_command_radio(ident)
        if activated:
            self._station_command_selected_profile_id = ident
        self._refresh_station_command_bar(force=True)

    def _refresh_station_command_bar(self, *, force: bool = False) -> None:
        if not hasattr(self, "station_command_radio_combo"):
            return
        if force:
            self._invalidate_station_command_plan_cache()
            self._invalidate_station_command_lane_cache()
        manager = getattr(self, "station_runtime_manager", None)
        snapshots: list[object] = []
        if manager is not None:
            try:
                snapshots = list(manager.get_runtime_snapshots(force=force))
            except Exception:
                snapshots = []
        snapshot_by_id = {self._station_command_snapshot_id(snapshot): snapshot for snapshot in snapshots}
        choices: list[object] = []
        seen_ids: set[int] = set()
        for profile in self._station_command_configured_profiles():
            ident = self._station_command_snapshot_id(profile)
            if ident <= 0 or ident in seen_ids:
                continue
            choices.append(snapshot_by_id.get(ident, profile))
            seen_ids.add(ident)
        for snapshot in snapshots:
            ident = self._station_command_snapshot_id(snapshot)
            if ident > 0 and ident not in seen_ids and self._station_command_is_controllable_profile(snapshot):
                choices.append(snapshot)
                seen_ids.add(ident)
        self._station_command_last_choices = list(choices)

        selected = self._station_command_selected_snapshot(choices)
        selected_id = self._station_command_snapshot_id(selected) if selected is not None else 0
        card_mode = len(choices) >= 1
        multi_active = len(choices) >= 2
        if card_mode and not force and bool(getattr(self, "_station_command_multi_mode_active", False)):
            if selected is not None and selected_id > 0:
                self._station_command_selected_profile_id = int(selected_id)
            page_choices, page, page_count, _per_page = self._station_command_radio_page_slice(choices)
            for btn, direction, label in (
                (getattr(self, "station_command_radio_prev_btn", None), -1, "Prev"),
                (getattr(self, "station_command_radio_next_btn", None), 1, "Next"),
            ):
                if btn is None:
                    continue
                show_page_control = len(choices) > 2 and page_count > 1
                if btn.isVisible() != show_page_control:
                    btn.setVisible(show_page_control)
                btn.setEnabled((page > 0) if direction < 0 else (page < page_count - 1))
                next_text = f"{label} {page + 1}/{page_count}" if page_count > 1 else label
                if btn.text() != next_text:
                    btn.setText(next_text)
            self._refresh_station_command_radio_summary(choices, selected_id)
            try:
                snapshot = suspend_snapshot(self.settings, allow_reload=False)
            except Exception:
                snapshot = {}
            self._update_station_command_radio_tile_hold_controls(snapshot if isinstance(snapshot, Mapping) else {})
            return
        self._station_command_bar_loading = True
        combo = self.station_command_radio_combo
        previous_block = combo.blockSignals(True)
        try:
            combo.clear()
            for snapshot in choices:
                ident = self._station_command_snapshot_id(snapshot)
                role = "SDR" if str(self._station_command_value(snapshot, "device_class", "") or "").strip().lower() == "observer" else "HF"
                combo.addItem(f"{self._station_command_snapshot_name(snapshot)} ({role})", ident)
            if combo.count() <= 0:
                combo.addItem("No configured radios", 0)
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
            self.station_command_now_label.setToolTip(self._station_command_now_tooltip(selected))
            self.station_command_now_label.setText(self._station_command_now_text(selected))
            state_text = self._station_command_state_text(selected)
            if state_text.strip().lower() in {"ok", "ready", "on schedule"}:
                self.station_command_state_label.setText("")
                self.station_command_state_label.setVisible(False)
            else:
                self.station_command_state_label.setText(state_text)
                self.station_command_state_label.setVisible(True)
            self.station_command_next_label.setText(f"Next: {self._station_command_next_text(selected)}")
            target_name = self._station_command_snapshot_name(selected)
            tooltip = f"Command target: {target_name}."
        else:
            self._station_command_selected_profile_id = None
            self.station_command_now_label.setToolTip("No configured radio is available for station commands.")
            self.station_command_now_label.setText("Now: unavailable")
            self.station_command_state_label.setText("No configured radio")
            self.station_command_state_label.setVisible(True)
            self.station_command_next_label.setText("Next: none")
            tooltip = "No configured radio is available for station commands."
        self._refresh_station_command_health(selected, selected_id)
        self._station_command_multi_mode_active = card_mode
        if getattr(self, "station_command_radio_summary_label", None) is not None:
            self.station_command_radio_summary_label.setText("Active Radios" if card_mode else "Radios")
            self.station_command_radio_summary_label.setVisible(False)
        if getattr(self, "station_command_radio_summary_scroll", None) is not None:
            self.station_command_radio_summary_scroll.setFixedHeight(188 if card_mode else 42)
            self.station_command_radio_summary_scroll.setVisible(True)
        page_choices: list[object] = []
        page = 0
        page_count = 1
        if card_mode:
            page_choices, page, page_count, _per_page = self._station_command_radio_page_slice(choices)
        for btn, direction, label in (
            (getattr(self, "station_command_radio_prev_btn", None), -1, "Prev"),
            (getattr(self, "station_command_radio_next_btn", None), 1, "Next"),
        ):
            if btn is None:
                continue
            show_page_control = card_mode and len(choices) > 2 and page_count > 1
            btn.setVisible(show_page_control)
            btn.setEnabled((page > 0) if direction < 0 else (page < page_count - 1))
            btn.setText(f"{label} {page + 1}/{page_count}" if page_count > 1 else label)
        if getattr(self, "station_command_radio_admin_btn", None) is not None:
            self.station_command_radio_admin_btn.setVisible(False)
        if card_mode and getattr(self, "station_command_radio_admin_panel", None) is not None:
            self._station_command_radio_admin_expanded = False
            self.station_command_radio_admin_panel.setVisible(False)
        for widget in (
            getattr(self, "station_command_radio_label", None),
            getattr(self, "station_command_radio_combo", None),
            getattr(self, "station_command_radio_separator", None),
            getattr(self, "station_command_now_caption", None),
            getattr(self, "station_command_now_label", None),
            getattr(self, "station_command_state_label", None),
            getattr(self, "station_command_now_separator", None),
            getattr(self, "station_command_action_label", None),
            getattr(self, "station_command_freq_combo", None),
            getattr(self, "station_command_qsy_btn", None),
            getattr(self, "station_command_duration_combo", None),
            getattr(self, "station_command_hold_btn", None),
            getattr(self, "station_command_suspend_btn", None),
            getattr(self, "station_command_resume_btn", None),
            getattr(self, "station_command_health_label", None),
            getattr(self, "station_command_health_widget", None),
            getattr(self, "station_command_next_label", None),
        ):
            if widget is not None:
                widget.setVisible(not card_mode)
        self._refresh_station_command_radio_summary(choices, selected_id)
        self._refresh_station_command_radio_admin(choices, selected_id)
        try:
            refresh_hold_duration_combo(
                self.station_command_duration_combo,
                self.settings,
                getattr(self, "_active_runtime_profile", None),
            )
        except Exception:
            pass
        has_qsy_target = self._refresh_station_command_frequency_combo(selected)
        has_radio = selected is not None and selected_id > 0
        hold_snapshot = suspend_snapshot(self.settings, allow_reload=False)
        manual_qsy_active = self._station_command_scheduler_manual_qsy_active()
        scheduler_suspended_manual = self._station_command_scheduler_suspended_manually()
        can_qsy = bool(has_radio and has_qsy_target)
        self.station_command_qsy_btn.setEnabled(can_qsy)
        self.station_command_hold_btn.setEnabled(can_qsy)
        self.station_command_suspend_btn.setEnabled(bool(has_radio))
        self.station_command_resume_btn.setEnabled(
            bool(has_radio and (hold_snapshot.get("active") or manual_qsy_active or scheduler_suspended_manual))
        )
        if has_radio and manual_qsy_active:
            self.station_command_state_label.setText("Manual QSY")
            self.station_command_state_label.setVisible(True)
        elif has_radio and scheduler_suspended_manual:
            self.station_command_state_label.setText("Scheduler Suspended")
            self.station_command_state_label.setVisible(True)
        self.station_command_duration_combo.setEnabled(bool(has_radio))
        self.station_command_freq_combo.setEnabled(bool(has_radio))
        for btn in (
            self.station_command_qsy_btn,
            self.station_command_hold_btn,
            self.station_command_suspend_btn,
            self.station_command_resume_btn,
        ):
            btn.setToolTip(tooltip)
        if can_qsy:
            self.station_command_qsy_btn.setToolTip(f"{tooltip} Send the selected manual QSY now and suspend scheduled changes until Resume Schedule.")
            mins = self._selected_station_command_hold_minutes()
            self.station_command_hold_btn.setToolTip(f"{tooltip} Send the selected manual QSY and suspend the scheduler for {mins} minutes.")
        self.station_command_suspend_btn.setToolTip(f"{tooltip} Suspend scheduled frequency changes until Resume Schedule without changing the radio.")
        self.station_command_resume_btn.setToolTip(f"{tooltip} Resume scheduled frequency changes.")
        self._update_station_command_hold_button_labels(hold_snapshot)
        if card_mode:
            for widget in (
                getattr(self, "station_command_radio_label", None),
                getattr(self, "station_command_radio_combo", None),
                getattr(self, "station_command_radio_separator", None),
                getattr(self, "station_command_now_caption", None),
                getattr(self, "station_command_now_label", None),
                getattr(self, "station_command_state_label", None),
                getattr(self, "station_command_now_separator", None),
                getattr(self, "station_command_action_label", None),
                getattr(self, "station_command_freq_combo", None),
                getattr(self, "station_command_qsy_btn", None),
                getattr(self, "station_command_duration_combo", None),
                getattr(self, "station_command_hold_btn", None),
                getattr(self, "station_command_suspend_btn", None),
                getattr(self, "station_command_resume_btn", None),
                getattr(self, "station_command_health_label", None),
                getattr(self, "station_command_health_widget", None),
                getattr(self, "station_command_next_label", None),
            ):
                if widget is not None:
                    widget.setVisible(False)
        try:
            self._style_station_command_bar(resolve_theme(self.settings))
        except Exception:
            pass

    def _on_station_command_radio_changed(self, _index: int) -> None:
        if bool(getattr(self, "_station_command_bar_loading", False)):
            return
        try:
            ident = int(self.station_command_radio_combo.currentData() or 0)
        except Exception:
            ident = 0
        if ident > 0:
            activated = self._activate_station_command_radio(ident)
            self._station_command_selected_profile_id = ident if activated else getattr(self, "_station_command_selected_profile_id", None)
            self._refresh_station_command_bar(force=True)
            return
        self._station_command_selected_profile_id = None
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
                refresh_sources = getattr(self.freq_planner_tab, "on_schedule_sources_changed", None)
                if callable(refresh_sources):
                    refresh_sources()
                elif self.stack.currentWidget() is self.freq_planner_tab:
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
        # Keep main navigation compact on startup. Screen changes and Quick
        # Search expand the relevant group when navigation intent is explicit.
        defaults = {
            "Station": False,
            "FreqPlanner": False,
            "Messages": False,
            "NCS": False,
            "Operators": False,
            "Settings": False,
        }
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
        if screen in {"FreqPlanner", "SOP", "HF Schedule", "Net Schedule", "Peer Schedules"}:
            return "FreqPlanner"
        if screen == "Messages":
            return "Messages"
        if screen in {"HF Operators", "Local Operators", "Local Reports"}:
            return "Operators"
        if screen == "Settings":
            return "Settings"
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
                    if label == "Settings":
                        nav_idx = self._settings_nav_button_indices.get(
                            str(getattr(self, "_settings_nav_context", "main") or "main")
                        )
                    elif label == "Messages":
                        nav_idx = self._messages_nav_button_indices.get(
                            str(getattr(self, "_messages_nav_context", "inbox") or "inbox")
                        )
                    else:
                        nav_idx = self._nav_screen_index_map.get(index)
                    if bool(getattr(self, "_suppress_initial_nav_group_auto_expand", False)):
                        self._suppress_initial_nav_group_auto_expand = False
                    else:
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
                    if label == "Messages":
                        self._apply_messages_nav_context()
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

    def _station_health_runtime_items(self) -> list[Mapping[str, object]]:
        station_poll_metrics = None
        scheduler_poll_metrics = None
        scheduler_companion_status = None
        background_job_status = None
        assigned_schedule_status = []
        runtime_source_rows = []
        try:
            manager = getattr(self, "station_runtime_manager", None)
            if manager is not None and hasattr(manager, "get_status_poll_metrics"):
                station_poll_metrics = manager.get_status_poll_metrics()
        except Exception:
            station_poll_metrics = None
        try:
            scheduler = getattr(self, "scheduler", None)
            if scheduler is not None and hasattr(scheduler, "get_status_poll_metrics"):
                scheduler_poll_metrics = scheduler.get_status_poll_metrics()
            if scheduler is not None and hasattr(scheduler, "get_status_summary"):
                scheduler_companion_status = scheduler.get_status_summary(live=False, refresh=False)
        except Exception:
            scheduler_poll_metrics = None
            scheduler_companion_status = None
        try:
            background = getattr(self, "background_ingest", None)
            if background is not None and hasattr(background, "job_status_snapshot"):
                background_job_status = background.job_status_snapshot()
        except Exception:
            background_job_status = None
        try:
            js8_registry_status = JS8ApiClientRegistry.status_dicts()
        except Exception:
            js8_registry_status = []
        assigned_schedule_status = self._station_health_assigned_schedule_status_rows()
        runtime_source_rows = self._station_health_runtime_source_rows()
        return runtime_observability_items(
            station_poll_metrics=station_poll_metrics,
            scheduler_poll_metrics=scheduler_poll_metrics,
            scheduler_companion_status=scheduler_companion_status,
            assigned_schedule_status=assigned_schedule_status,
            background_job_status=background_job_status,
            js8_registry_status=js8_registry_status,
            runtime_source_rows=runtime_source_rows,
        )

    def _station_health_assigned_schedule_status_rows(self) -> list[Mapping[str, object]]:
        try:
            store = getattr(self, "multi_radio_store", None) or MultiRadioStore()
            assignments = [dict(row) for row in store.list_effective_assigned_plans()]
            if not assignments:
                return []
            devices = {
                int(row.get("id", 0) or 0): dict(row)
                for row in store.list_device_profiles()
                if isinstance(row, Mapping)
            }
            plans = {
                int(row.get("id", 0) or 0): dict(row)
                for row in store.list_frequency_plans()
                if isinstance(row, Mapping)
            }
        except Exception:
            return []
        rows: list[Mapping[str, object]] = []
        for assignment in assignments:
            try:
                device_id = int(assignment.get("device_profile_id", 0) or 0)
                plan_id = int(assignment.get("frequency_plan_id", 0) or 0)
            except Exception:
                continue
            device = devices.get(device_id, {})
            plan = plans.get(plan_id, {})
            row = dict(assignment)
            row["device_name"] = str(device.get("name") or row.get("device_name") or f"Radio {device_id}")
            row["frequency_plan_name"] = str(plan.get("name") or row.get("frequency_plan_name") or f"Frequency Plan {plan_id}")
            rows.append(row)
        return rows

    def _station_health_runtime_source_rows(self) -> list[Mapping[str, object]]:
        rows: list[dict[str, object]] = []
        try:
            rows = [dict(getattr(row, "__dict__", {}) or {}) for row in active_runtime_source_view_rows()]
        except Exception:
            rows = []
        known_source_ids = {str(row.get("source_id", "") or "").strip() for row in rows}
        try:
            background = getattr(self, "background_ingest", None)
            status = background.job_status_snapshot() if background is not None and hasattr(background, "job_status_snapshot") else {}
            source_skips = status.get("source_skip_reasons", {}) if isinstance(status, Mapping) else {}
            if isinstance(source_skips, Mapping) and source_skips:
                for row in runtime_source_view_rows_from_skip_reasons(source_skips):
                    payload = dict(getattr(row, "__dict__", {}) or {})
                    source_id = str(payload.get("source_id", "") or "").strip()
                    if source_id and source_id in known_source_ids:
                        continue
                    rows.append(payload)
                    if source_id:
                        known_source_ids.add(source_id)
        except Exception:
            pass
        return rows

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
