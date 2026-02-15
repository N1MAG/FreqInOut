from __future__ import annotations

import datetime
import sqlite3

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
)
from PySide6.QtGui import QPixmap, QIcon
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt, QTimer
from pathlib import Path

from freqinout.core.logger import log
from freqinout.core.logger import set_log_level
from freqinout.core.config_paths import get_config_dir
from freqinout.core.perf_metrics import span as perf_span
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.background_ingest import BackgroundIngestController
from freqinout.radio_interface.rigctl_client import FLRigClient
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
from freqinout.gui.log_viewer import LogViewerTab
from freqinout.gui.stations_map_tab import (
    StationsMapTab,
    FEMA_REGIONS,
    LOWER48_STATES,
    STATE_CENTERS,
)
from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.gui.peer_sched_tab import PeerSchedTab
from freqinout.gui.help_tab import HelpTab
from freqinout.gui.controlfreq_tab import ControlFreqTab
from freqinout.gui.theme import resolve_theme, apply_app_theme, button_style


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

    def __init__(self):
        super().__init__()
        self._shutting_down = False

        self.settings = SettingsManager()
        self.setWindowTitle(f"FreqInOut de N1MAG (v{__version__})")
        self._set_window_icon()

        # Central widget with sidebar navigation + stacked pages
        central = QWidget()
        layout = QHBoxLayout(central)
        self.setCentralWidget(central)

        # Instantiate screens (lazy-load heavy tabs to improve perceived performance)
        self.settings_tab = SettingsTab(self)
        self.launch_orchestrator = self.settings_tab.launch_orchestrator
        self._launch_progress_dialog: QProgressDialog | None = None
        self._launch_progress_total = 0
        self._launch_progress_done = 0
        self.hf_schedule_tab = DailyScheduleTab(self)  # this tab is labeled "HF Frequency Schedule"
        self.net_tab = NetScheduleTab(self)
        self.fldigi_tab = FldigiNetControlTab(self)
        self.js8_tab = JS8CallNetControlTab(self)
        self.sop_tab = SOPTab(self)
        self.operator_history_tab = OperatorHistoryTab(self)
        self.log_tab: LogViewerTab | None = None
        self._log_dialog: QDialog | None = None
        self.peer_sched_tab = PeerSchedTab(self)
        self.help_tab = HelpTab(self)
        self.controlfreq_tab = ControlFreqTab(self)

        self.freq_planner_tab = None
        self.message_viewer_tab = None
        self.stations_map_tab = None
        self._map_prop_target_syncing = False

        self._lazy_placeholders = {}
        self._lazy_factories = {
            "FreqPlanner": self._create_freq_planner_tab,
            "Messages": self._create_message_viewer_tab,
            "Map": self._create_stations_map_tab,
        }

        # Sidebar navigation order (as requested)
        self._screens = [
            ("ControlFreq", self.controlfreq_tab),
            ("FreqPlanner", self._placeholder_widget("FreqPlanner")),
            ("SOP", self.sop_tab),
            ("Messages", self._placeholder_widget("Messages")),
            ("Digi/SSB NCS", self.fldigi_tab),
            ("JS8 NCS", self.js8_tab),
            ("Operators", self.operator_history_tab),
            ("Map", self._placeholder_widget("Map")),
            ("HF Schedule", self.hf_schedule_tab),
            ("Net Schedule", self.net_tab),
            ("Peer Schedules", self.peer_sched_tab),
            ("Settings", self.settings_tab),
            ("Help", self.help_tab),
        ]

        # Build sidebar
        self.nav_widget = QWidget()
        self.nav_widget.setMinimumWidth(140)
        self.nav_widget.setMaximumWidth(200)
        nav_layout = QVBoxLayout(self.nav_widget)
        nav_layout.setContentsMargins(4, 4, 4, 4)
        nav_layout.setSpacing(4)

        # Logo above nav buttons (optional if file exists)
        self.logo_label = QLabel()
        self.logo_label.setAlignment(Qt.AlignCenter)
        nav_layout.addWidget(self.logo_label)
        self._set_logo_pixmap()

        self.nav_buttons = []
        self.button_group = QButtonGroup(self)
        self.button_group.setExclusive(True)
        self._map_nav_index = None
        for idx, (label, _w) in enumerate(self._screens):
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
            btn.setMinimumWidth(120)
            btn.clicked.connect(lambda _=False, i=idx: self._set_screen(i))
            self.button_group.addButton(btn, idx)
            self.nav_buttons.append(btn)
            nav_layout.addWidget(btn)
            if label == "Map":
                self._map_nav_index = idx
        # Placeholder for map filters (shown only on Map view)
        self.map_filters_container = QWidget()
        self.map_filters_container.setMinimumWidth(120)
        self.map_filters_container.setMaximumWidth(200)
        self.map_filters_layout = QVBoxLayout(self.map_filters_container)
        self.map_filters_layout.setContentsMargins(0, 0, 0, 0)
        nav_layout.addWidget(self.map_filters_container)
        self._init_map_filters()
        spacer_height = 0
        if self.nav_buttons:
            try:
                spacer_height = max(btn.sizeHint().height() for btn in self.nav_buttons)
            except Exception:
                spacer_height = 0
        if spacer_height > 0:
            nav_layout.addItem(QSpacerItem(0, spacer_height, QSizePolicy.Minimum, QSizePolicy.Fixed))

        # Scheduler status panel (hidden on Map view)
        self.scheduler_status_container = QGroupBox("Schedule Status")
        self.scheduler_status_container.setCheckable(False)
        self.scheduler_status_container.setMinimumWidth(140)
        self.scheduler_status_container.setMaximumWidth(200)
        self.scheduler_status_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.scheduler_status_container.setStyleSheet(
            "QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top center; padding: 0 4px; }"
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
        self.suspend_schedule_btn = QPushButton("Suspend Schedule")
        self.suspend_schedule_btn.setFixedWidth(140)
        self.suspend_schedule_btn.clicked.connect(self._on_suspend_schedule_clicked)
        self.logs_active_btn = QPushButton("Logs Active")
        self.logs_active_btn.setFixedWidth(140)
        self.logs_active_btn.clicked.connect(self._open_logs_window)
        self.logs_active_btn.setVisible(False)
        try:
            theme = resolve_theme(self.settings)
            self.resume_schedule_btn.setStyleSheet(button_style("info", theme))
            self.suspend_schedule_btn.setStyleSheet(button_style("warning", theme))
            self.logs_active_btn.setStyleSheet(button_style("warning", theme))
        except Exception:
            pass
        status_layout.addWidget(self.scheduler_status_header)
        status_layout.addWidget(self.scheduler_status_reasons)
        status_layout.addWidget(self.suspend_schedule_btn, alignment=Qt.AlignCenter)
        status_layout.addWidget(self.resume_schedule_btn, alignment=Qt.AlignCenter)
        status_layout.addWidget(self.logs_active_btn, alignment=Qt.AlignCenter)
        nav_layout.addWidget(self.scheduler_status_container)
        self.resume_schedule_btn.setVisible(False)
        nav_layout.addStretch()
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

        right_layout.addWidget(self.stack, stretch=1)

        # Layout composition
        layout.addWidget(self.nav_widget)
        layout.addWidget(right_container, stretch=1)
        self.stack.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # Suggest a modest minimum size
        self.setMinimumSize(900, 600)

        self._apply_app_theme()
        self._sop_next_due_cache_ts = 0.0
        self._sop_next_due_minutes = None
        self._active_tab_index = None
        self._lazy_prewarm_labels = ["Messages", "Map", "FreqPlanner"]
        self._lazy_prewarm_index = 0

        # Default selection
        if self.nav_buttons:
            self.nav_buttons[0].setChecked(True)
            self._set_screen(0)
        QTimer.singleShot(2500, self._start_lazy_prewarm)

        # Optional: apply callsign to tab captions if already configured
        self._apply_callsign_to_tab_titles()

        # Start scheduler engine
        self.rig_client = FLRigClient()
        self.js8_control = JS8ControlClient()
        self.varac_status = VarACStatusClient()
        self.fldigi_log_status = FldigiLogStatusClient()
        self.scheduler = SchedulerEngine(
            self,
            rig=self.rig_client,
            js8=self.js8_control,
            varac=self.varac_status,
            fldigi_log=self.fldigi_log_status,
        )
        self.scheduler.start()
        self.background_ingest = BackgroundIngestController(self.settings)
        self.background_ingest.start()
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
            self.scheduler.active_entry_changed.connect(self._refresh_scheduler_status_panel)
        except Exception:
            pass
        try:
            if hasattr(self.fldigi_tab, "net_status_changed"):
                self.fldigi_tab.net_status_changed.connect(
                    lambda kind, active: self.scheduler.set_manual_net_active(kind, active)
                )
            if hasattr(self.js8_tab, "net_status_changed"):
                self.js8_tab.net_status_changed.connect(
                    lambda kind, active: self.scheduler.set_manual_net_active(kind, active)
                )
        except Exception:
            pass

        self._status_timer = QTimer(self)
        self._status_timer.setInterval(2000)
        self._status_timer.timeout.connect(self._refresh_scheduler_status_panel)
        self._status_timer.timeout.connect(self._check_timed_debug_expiry)
        self._status_timer.start()

        app = QApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(self._on_app_about_to_quit)

        # Wire settings_saved signal
        try:
            self.settings_tab.settings_saved.connect(self.js8_tab.on_settings_saved)
        except Exception:
            pass
        try:
            self.settings_tab.settings_saved.connect(self.hf_schedule_tab.on_settings_saved)
        except Exception:
            pass
        try:
            self.settings_tab.settings_saved.connect(self.fldigi_tab.on_settings_saved)
        except Exception:
            pass
        try:
            self.settings_tab.settings_saved.connect(self.net_tab.on_settings_saved)
        except Exception:
            pass
        self.settings_tab.settings_saved.connect(self._on_settings_saved_for_lazy_tabs)
        try:
            self.settings_tab.settings_saved.connect(self.sop_tab.on_settings_saved)
        except Exception:
            pass
        # Message tab settings saved handled by _on_settings_saved_for_lazy_tabs
        try:
            if hasattr(self.operator_history_tab, "operator_history_updated"):
                self.operator_history_tab.operator_history_updated.connect(self.refresh_operator_history_views)
        except Exception:
            pass
        try:
            self.settings_tab.settings_saved.connect(self._apply_app_theme)
        except Exception:
            pass
        try:
            self.settings_tab.settings_saved.connect(self._update_log_indicator)
        except Exception:
            pass
        try:
            self.settings_tab.open_logs_requested.connect(self._open_logs_window)
        except Exception:
            pass
        try:
            self.settings_tab.log_level_changed.connect(self._update_log_indicator)
        except Exception:
            pass
        self.hf_schedule_tab.schedule_saved.connect(self._refresh_freq_planner_if_loaded)
        self.hf_schedule_tab.schedule_saved.connect(self.scheduler.force_refresh)
        self.net_tab.schedule_saved.connect(self._refresh_freq_planner_if_loaded)
        self.net_tab.schedule_saved.connect(self.scheduler.force_refresh)

        log.info("Main window initialized.")
        # Sync sidebar filters initially
        self._sync_map_filters_from_tab()
        self._update_log_indicator()
        self._refresh_scheduler_status_panel()

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

    def _update_log_indicator(self) -> None:
        try:
            try:
                self.settings.reload()
            except Exception:
                pass
            level = (self.settings.get("log_level", "") or "INFO").upper()
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
            prev = (self.settings.get("timed_debug_prev_level", "") or "INFO").strip().upper()
            if prev not in {"DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"}:
                prev = "INFO"
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
        if hasattr(tab, "_render_map"):
            tab._render_map()

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
        if hasattr(tab, "_render_map"):
            tab._render_map()

    def _on_sidebar_prop_mode_changed(self, _=None) -> None:
        tab = getattr(self, "stations_map_tab", None)
        if tab is None:
            return
        mode = self.map_prop_mode_combo.currentData()
        if mode:
            tab.prop_mode = str(mode)
        if hasattr(tab, "_save_display_preferences"):
            tab._save_display_preferences()
        if hasattr(tab, "_render_map"):
            tab._render_map()

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
        if hasattr(tab, "_render_map"):
            tab._render_map()

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
        if tab is not None and hasattr(tab, "_render_map"):
            tab._render_map()

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
        if tab is not None and hasattr(tab, "_render_map"):
            tab._render_map()

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

    def _on_resume_schedule_clicked(self) -> None:
        try:
            if hasattr(self, "scheduler"):
                if hasattr(self.scheduler, "resume_schedule"):
                    self.scheduler.resume_schedule()
                else:
                    try:
                        self.scheduler.settings.set("schedule_suspend_until", 0)
                    except Exception:
                        pass
                    self.scheduler.apply_current_entry(
                        force=True,
                        ignore_wait_prompt=True,
                        ignore_suspend=True,
                    )
        except Exception:
            pass

    def _on_suspend_schedule_clicked(self) -> None:
        try:
            if not hasattr(self, "scheduler"):
                return
            status = self.scheduler.get_status_summary() if hasattr(self.scheduler, "get_status_summary") else {}
            suspended_until = status.get("suspended_until") if isinstance(status, dict) else None
            if suspended_until:
                self._on_resume_schedule_clicked()
                return
            if hasattr(self.scheduler, "suspend_schedule"):
                self.scheduler.suspend_schedule(30)
            else:
                try:
                    until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=30)
                    self.scheduler.settings.set("schedule_suspend_until", until.timestamp())
                except Exception:
                    pass
        except Exception:
            pass
        self._refresh_scheduler_status_panel()

    def _set_scheduler_reasons(self, lines: list[str]) -> None:
        if not hasattr(self, "scheduler_status_reasons_layout"):
            return
        layout = self.scheduler_status_reasons_layout
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        for line in lines:
            lbl = QLabel(line)
            lbl.setAlignment(Qt.AlignCenter)
            lbl.setWordWrap(True)
            layout.addWidget(lbl)

    def _refresh_scheduler_status_panel(self, *_args) -> None:
        if not hasattr(self, "scheduler") or not hasattr(self, "scheduler_status_container"):
            return
        if not self.scheduler_status_container.isVisible():
            return
        try:
            status = self.scheduler.get_status_summary()
        except Exception:
            return
        control_mode = status.get("control_mode")
        use_scheduler = bool(status.get("use_scheduler", True))
        freq_label = status.get("freq_label") or ""
        suspended_until = status.get("suspended_until")
        off_schedule = bool(status.get("off_schedule"))
        varac_waiting = bool(status.get("varac_waiting"))
        ptt_active = bool(status.get("ptt_active"))
        js8_busy = bool(status.get("js8_busy"))
        fldigi_busy = bool(status.get("fldigi_busy"))
        fldigi_busy_reason = (status.get("fldigi_busy_reason") or "").strip().lower()
        varac_busy = bool(status.get("varac_busy"))
        net_kind = status.get("net_kind")
        flags = status.get("off_schedule_flags") or {}
        fldigi_mode_off = bool(status.get("fldigi_mode_off"))
        fldigi_offset_off = bool(status.get("fldigi_offset_off"))
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
            try:
                self.scheduler_status_container.adjustSize()
            except Exception:
                pass
            return

        if suspended_until:
            local_dt = suspended_until.astimezone()
            self.scheduler_status_header.setText("Suspended until")
            self._set_scheduler_reasons([f"{local_dt:%Y-%m-%d %H:%M}"])
            self.resume_schedule_btn.setVisible(False)
            self.suspend_schedule_btn.setVisible(True)
            self.suspend_schedule_btn.setText("Resume Schedule")
            try:
                theme = resolve_theme(self.settings)
                self.suspend_schedule_btn.setStyleSheet(button_style("info", theme))
            except Exception:
                pass
            try:
                self.scheduler_status_container.adjustSize()
            except Exception:
                pass
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
            if flags.get("mode"):
                if fldigi_mode_off:
                    reasons.append("FLDigi Mode")
                if fldigi_offset_off:
                    reasons.append("FLDigi Offset")
                if not fldigi_mode_off and not fldigi_offset_off:
                    reasons.append("FLDigi Mode/Offset")
            if varac_waiting:
                reasons.append("Waiting to Clear")
            if ptt_active:
                reasons.append("Sending Traffic")
            if js8_busy or varac_busy:
                reasons.append("QSO")
            if busy_line and not net_kind:
                reasons.append(busy_line)
            if next_change_minutes is not None and next_change_minutes <= 15:
                reasons.append(f"Freq Change: {next_change_minutes} min")
        else:
            if varac_waiting:
                reasons.append("Waiting to Clear")
            if ptt_active:
                reasons.append("Sending Traffic")
            if js8_busy or varac_busy:
                reasons.append("QSO")
            if net_kind:
                reasons.append(net_kind)
            if busy_line and not net_kind:
                reasons.append(busy_line)
            if next_change_minutes is not None and next_change_minutes <= 15:
                reasons.append(f"Freq Change: {next_change_minutes} min")

        if off_schedule:
            self.scheduler_status_header.setText("Off Schedule")
            self.scheduler_status_header.setStyleSheet("font-weight: bold; color: #C62828;")
            self._set_scheduler_reasons(reasons or [""])
            self.resume_schedule_btn.setVisible(True)
            self.suspend_schedule_btn.setVisible(True)
            self.suspend_schedule_btn.setText("Suspend Schedule")
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
                    "QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top center; padding: 0 4px; }"
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
            self._set_scheduler_reasons([])
            self.resume_schedule_btn.setVisible(False)
            self.suspend_schedule_btn.setVisible(True)
            self.suspend_schedule_btn.setText("Suspend Schedule")
            try:
                theme = resolve_theme(self.settings)
                self.suspend_schedule_btn.setStyleSheet(button_style("warning", theme))
            except Exception:
                pass
            try:
                self.scheduler_status_container.setStyleSheet(
                    "QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top center; padding: 0 4px; }"
                )
            except Exception:
                pass
        try:
            self.scheduler_status_container.adjustSize()
        except Exception:
            pass

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

    def _on_app_about_to_quit(self):
        if self._shutting_down:
            return
        self._shutting_down = True
        try:
            if self.stack.currentWidget() is self.stations_map_tab:
                self.stack.setCurrentWidget(self.settings_tab)
        except Exception:
            pass
        try:
            if hasattr(self, "scheduler"):
                self.scheduler.stop()
        except Exception:
            pass
        try:
            if hasattr(self, "background_ingest"):
                self.background_ingest.stop()
        except Exception:
            pass
        try:
            if hasattr(self, "launch_orchestrator"):
                self.launch_orchestrator.stop_sequence()
        except Exception:
            pass
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
            if hasattr(self, "js8_control"):
                self.js8_control.stop()
        except Exception:
            pass
        for _label, widget in self._screens:
            try:
                if hasattr(widget, "shutdown"):
                    widget.shutdown()
            except Exception:
                continue
        try:
            JS8RxHub.instance().shutdown()
        except Exception:
            pass

    def closeEvent(self, event):
        self._on_app_about_to_quit()
        super().closeEvent(event)

    def resizeEvent(self, event):
        try:
            self._sync_status_box_width()
        except Exception:
            pass
        super().resizeEvent(event)

    def _sync_status_box_width(self) -> None:
        if not hasattr(self, "scheduler_status_container"):
            return
        width = 0
        if hasattr(self, "nav_buttons") and self.nav_buttons:
            try:
                width = max(btn.width() for btn in self.nav_buttons)
                if width <= 10:
                    width = max(btn.sizeHint().width() for btn in self.nav_buttons)
            except Exception:
                width = 0
        if width <= 10 and hasattr(self, "nav_widget"):
            try:
                margins = self.nav_widget.layout().contentsMargins()
                width = int(self.nav_widget.width() - margins.left() - margins.right())
            except Exception:
                width = int(self.nav_widget.width())
        if width > 0:
            self.scheduler_status_container.setFixedWidth(width)

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
        suspend_btn = msg.addButton("Pause Sched. 30 Min", QMessageBox.DestructiveRole)
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
                self.scheduler.resolve_off_schedule("suspend", items=items)
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
        suspend_btn = msg.addButton("Pause Sched. 30 Min", QMessageBox.DestructiveRole)
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
                self.scheduler.resolve_varac_wait("suspend")
            except Exception:
                pass
        else:
            try:
                self.scheduler.resolve_varac_wait("ignore")
            except Exception:
                pass
        self._varac_wait_prompt = None

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
        if not callsign:
            # Reset to base titles if no callsign is set
            for idx, (base, _w) in enumerate(self._screens):
                if idx < len(self.nav_buttons):
                    self.nav_buttons[idx].setText(base)
            return

        def label(base: str) -> str:
            return f"{base} [{callsign}]"

        for idx, (base, _w) in enumerate(self._screens):
            lbl = label(base)
            if idx < len(self.nav_buttons):
                self.nav_buttons[idx].setText(lbl)

    def _apply_app_theme(self):
        app = QApplication.instance()
        try:
            self.settings.reload()
        except Exception:
            pass
        theme = resolve_theme(self.settings)
        apply_app_theme(app, theme)
        self._set_logo_pixmap()
        self._update_log_indicator()
        for widget in (
            self.freq_planner_tab,
            self.sop_tab,
            self.hf_schedule_tab,
            self.net_tab,
            self.fldigi_tab,
            self.js8_tab,
            self.message_viewer_tab,
            self.log_tab,
            self.operator_history_tab,
            self.settings_tab,
            self.controlfreq_tab,
        ):
            if widget is None:
                continue
            if hasattr(widget, "apply_theme"):
                try:
                    widget.apply_theme()
                except Exception:
                    pass

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
            self.freq_planner_tab = FreqPlannerTab(self)
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
            self.message_viewer_tab = MessageViewerTab(self)
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
            self.stations_map_tab = StationsMapTab(self)
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

    def _on_settings_saved_for_lazy_tabs(self) -> None:
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
            self._refresh_map_prop_target_controls()
        except Exception:
            pass

    def _refresh_freq_planner_if_loaded(self) -> None:
        try:
            if self.freq_planner_tab is not None:
                self.freq_planner_tab.rebuild_table()
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
            cancelled = bool(data.get("cancelled", False))
            summary = (
                f"Launch {trigger or 'sequence'} complete: "
                f"launched={launched}, running={running}, failed={failed}, timeout={timeout}"
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
        icon_path = assets_dir / "FreqInOut-desktop.png"
        if not icon_path.exists():
            return
        icon = QIcon(str(icon_path))
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

    def _set_screen(self, index: int) -> None:
        with perf_span(
            "main_window.set_screen",
            settings=self.settings,
            meta={"index": index},
            min_ms=5.0,
        ):
            if 0 <= index < self.stack.count():
                prev_index = self._active_tab_index
                if prev_index is not None and 0 <= prev_index < self.stack.count():
                    try:
                        prev_widget = self.stack.widget(prev_index)
                        if hasattr(prev_widget, "set_tab_active"):
                            prev_widget.set_tab_active(False)
                    except Exception:
                        pass

                label = self._screens[index][0]
                self._ensure_lazy_tab_loaded(label, index)
                self.stack.setCurrentIndex(index)
                self._active_tab_index = index
                try:
                    widget_active = self.stack.widget(index)
                    if hasattr(widget_active, "set_tab_active"):
                        widget_active.set_tab_active(True)
                except Exception:
                    pass
                self._update_map_filters_visibility(index)
                QTimer.singleShot(0, self._refresh_scheduler_status_panel)
                try:
                    widget = self.stack.widget(index)
                    if hasattr(widget, "show_loading_toast"):
                        widget.show_loading_toast()
                    if hasattr(widget, "on_tab_activated"):
                        QTimer.singleShot(0, widget.on_tab_activated)
                except Exception:
                    pass
