from __future__ import annotations

import datetime

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
    QLayout,
    QSpacerItem,
)
from PySide6.QtGui import QPixmap, QIcon
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt, QTimer
from pathlib import Path

from freqinout.core.logger import log
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
from freqinout.gui.stations_map_tab import StationsMapTab
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
      - Logs
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
        self.hf_schedule_tab = DailyScheduleTab(self)  # this tab is labeled "HF Frequency Schedule"
        self.net_tab = NetScheduleTab(self)
        self.fldigi_tab = FldigiNetControlTab(self)
        self.js8_tab = JS8CallNetControlTab(self)
        self.sop_tab = SOPTab(self)
        self.operator_history_tab = OperatorHistoryTab(self)
        self.log_tab = LogViewerTab(self)
        self.peer_sched_tab = PeerSchedTab(self)
        self.help_tab = HelpTab(self)
        self.controlfreq_tab = ControlFreqTab(self)

        self.freq_planner_tab = None
        self.message_viewer_tab = None
        self.stations_map_tab = None

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
            ("Logs", self.log_tab),
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
        self._logs_nav_index = None
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
            if label == "Logs":
                self._logs_nav_index = idx
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
        try:
            theme = resolve_theme(self.settings)
            self.resume_schedule_btn.setStyleSheet(button_style("info", theme))
        except Exception:
            pass
        status_layout.addWidget(self.scheduler_status_header)
        status_layout.addWidget(self.scheduler_status_reasons)
        status_layout.addWidget(self.resume_schedule_btn, alignment=Qt.AlignCenter)
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

        # Default selection
        if self.nav_buttons:
            self.nav_buttons[0].setChecked(True)
            self._set_screen(0)

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
            self.log_tab.log_level_changed.connect(self._update_log_indicator)
        except Exception:
            pass

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
            if self._logs_nav_index is None:
                return
            if self._logs_nav_index >= len(self.nav_buttons):
                return
            btn = self.nav_buttons[self._logs_nav_index]
            if level == "DISABLED":
                btn.setText("Logs")
                btn.setToolTip("")
                btn.setStyleSheet("")
            else:
                btn.setText("Logs Enabled")
                btn.setToolTip(
                    "Logging is active. Disable in Logs tab unless you are troubleshooting."
                )
                try:
                    theme = resolve_theme(self.settings)
                    btn.setStyleSheet(button_style("warning", theme))
                except Exception:
                    pass
        except Exception as e:
            log.debug("MainWindow: log indicator update failed: %s", e)

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
        mode_row = QHBoxLayout()
        mode_row.addWidget(QLabel("Mode:"))
        self.map_prop_mode_combo = QComboBox()
        self.map_prop_mode_combo.addItem("Actual", "actual")
        self.map_prop_mode_combo.addItem("Blended", "blended")
        self.map_prop_mode_combo.addItem("Modeled", "model")
        mode_row.addWidget(self.map_prop_mode_combo)
        mode_row.addStretch()
        v.addLayout(mode_row)
        window_row = QHBoxLayout()
        window_row.addWidget(QLabel("Window:"))
        self.map_prop_window_combo = QComboBox()
        self.map_prop_window_combo.addItem("1h", 1)
        self.map_prop_window_combo.addItem("3h", 3)
        self.map_prop_window_combo.addItem("6h", 6)
        self.map_prop_window_combo.addItem("12h", 12)
        self.map_prop_window_combo.addItem("24h", 24)
        self.map_prop_window_combo.addItem("History (7 days)", 168)
        window_row.addWidget(self.map_prop_window_combo)
        window_row.addStretch()
        v.addLayout(window_row)
        self.map_prop_badge = QLabel("Best Band: --")
        self.map_prop_badge.setStyleSheet("font-weight: bold; color: #1E88E5;")
        v.addWidget(self.map_prop_badge)
        self.map_cb_prop_overlay.stateChanged.connect(self._on_sidebar_prop_changed)
        self.map_prop_mode_combo.currentIndexChanged.connect(self._on_sidebar_prop_mode_changed)
        self.map_prop_window_combo.currentIndexChanged.connect(self._on_sidebar_prop_window_changed)
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
    def _update_map_filters_visibility(self, index: int) -> None:
        """
        Show the stations-map 'Show' filters in the sidebar only when the Map view is active.
        """
        is_map = 0 <= index < len(self._screens) and self._screens[index][0] == "Map"
        if hasattr(self, "logo_label"):
            self.logo_label.setVisible(not is_map)
        if self._map_nav_index is not None and self._map_nav_index < len(self.nav_buttons):
            self.nav_buttons[self._map_nav_index].setVisible(not is_map)
        if hasattr(self, "scheduler_status_container"):
            self.scheduler_status_container.setVisible(not is_map)
        try:
            if self.stations_map_tab is not None and hasattr(self.stations_map_tab, "set_map_visible"):
                self.stations_map_tab.set_map_visible(is_map)
        except Exception:
            pass
        if not is_map or self.map_filters_layout is None:
            self.map_filters_container.setVisible(False)
            return
        self.map_filters_container.setVisible(True)
        self._sync_map_filters_from_tab()

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
            try:
                self.scheduler_status_container.adjustSize()
            except Exception:
                pass
            return

        if suspended_until:
            local_dt = suspended_until.astimezone()
            self.scheduler_status_header.setText("Suspended until")
            self._set_scheduler_reasons([f"{local_dt:%Y-%m-%d %H:%M}"])
            self.resume_schedule_btn.setVisible(True)
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

    def _create_freq_planner_tab(self) -> QWidget:
        self.freq_planner_tab = FreqPlannerTab(self)
        try:
            self.settings_tab.settings_saved.connect(self.freq_planner_tab.on_settings_saved)
        except Exception:
            pass
        return self.freq_planner_tab

    def _create_message_viewer_tab(self) -> QWidget:
        self.message_viewer_tab = MessageViewerTab(self)
        try:
            self.settings_tab.settings_saved.connect(self.message_viewer_tab.on_settings_saved)
        except Exception:
            pass
        return self.message_viewer_tab

    def _create_stations_map_tab(self) -> QWidget:
        self.stations_map_tab = StationsMapTab(self)
        try:
            self.stations_map_tab.attach_prop_controls(
                getattr(self, "map_cb_prop_overlay", None),
                getattr(self, "map_prop_badge", None),
                getattr(self, "map_prop_mode_combo", None),
                getattr(self, "map_prop_window_combo", None),
            )
        except Exception:
            pass
        return self.stations_map_tab

    def _ensure_lazy_tab_loaded(self, label: str, index: int) -> None:
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

    def _refresh_freq_planner_if_loaded(self) -> None:
        try:
            if self.freq_planner_tab is not None:
                self.freq_planner_tab.rebuild_table()
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
            self._refresh_scheduler_status_panel()
            try:
                widget = self.stack.widget(index)
                if hasattr(widget, "show_loading_toast"):
                    widget.show_loading_toast()
                QApplication.processEvents()
                if hasattr(widget, "on_tab_activated"):
                    QTimer.singleShot(0, widget.on_tab_activated)
            except Exception:
                pass
