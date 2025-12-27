from __future__ import annotations

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
)

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.radio_interface.rigctl_client import FLRigClient
from freqinout.radio_interface.js8_status import JS8ControlClient

from freqinout.gui.settings_tab import SettingsTab
from freqinout.gui.daily_schedule_tab import DailyScheduleTab  # HF Frequency Schedule tab
from freqinout.gui.net_schedule_tab import NetScheduleTab
from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab
from freqinout.gui.js8call_net_control_tab import JS8CallNetControlTab
from freqinout.gui.freq_planner_tab import FreqPlannerTab
from freqinout.gui.operator_history_tab import OperatorHistoryTab
from freqinout.gui.log_viewer import LogViewerTab
from freqinout.gui.stations_map_tab import StationsMapTab
from freqinout.gui.message_viewer_tab import MessageViewerTab


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

        self.settings = SettingsManager()
        self.setWindowTitle("FreqInOut")

        # Central widget with sidebar navigation + stacked pages
        central = QWidget()
        layout = QHBoxLayout(central)
        self.setCentralWidget(central)

        # Instantiate screens
        self.settings_tab = SettingsTab(self)
        self.hf_schedule_tab = DailyScheduleTab(self)  # this tab is labeled "HF Frequency Schedule"
        self.net_tab = NetScheduleTab(self)
        self.fldigi_tab = FldigiNetControlTab(self)
        self.js8_tab = JS8CallNetControlTab(self)
        self.freq_planner_tab = FreqPlannerTab(self)
        self.operator_history_tab = OperatorHistoryTab(self)
        self.message_viewer_tab = MessageViewerTab(self)
        self.log_tab = LogViewerTab(self)
        self.stations_map_tab = StationsMapTab(self)

        # Sidebar navigation order (as requested)
        self._screens = [
            ("FreqPlanner", self.freq_planner_tab),
            ("Message Viewer", self.message_viewer_tab),
            ("FLDigi Net Control", self.fldigi_tab),
            ("JS8Call Net Control", self.js8_tab),
            ("Operator History", self.operator_history_tab),
            ("Stations Map", self.stations_map_tab),
            ("HF Frequency Schedule", self.hf_schedule_tab),
            ("Net Schedule", self.net_tab),
            ("Settings", self.settings_tab),
            ("Logs", self.log_tab),
        ]

        # Build sidebar
        nav_widget = QWidget()
        nav_layout = QVBoxLayout(nav_widget)
        nav_layout.setContentsMargins(0, 0, 0, 0)
        nav_layout.setSpacing(4)
        self.nav_buttons = []
        self.button_group = QButtonGroup(self)
        self.button_group.setExclusive(True)
        for idx, (label, _w) in enumerate(self._screens):
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
            btn.clicked.connect(lambda _=False, i=idx: self._set_screen(i))
            self.button_group.addButton(btn, idx)
            self.nav_buttons.append(btn)
            nav_layout.addWidget(btn)
        nav_layout.addStretch()

        # Stacked content
        self.stack = QStackedWidget()
        for _label, widget in self._screens:
            self.stack.addWidget(widget)

        # Layout composition
        layout.addWidget(nav_widget)
        layout.addWidget(self.stack, stretch=1)

        # Default selection
        if self.nav_buttons:
            self.nav_buttons[0].setChecked(True)
            self.stack.setCurrentIndex(0)

        # Optional: apply callsign to tab captions if already configured
        self._apply_callsign_to_tab_titles()

        # Start scheduler engine
        self.rig_client = FLRigClient()
        self.js8_control = JS8ControlClient()
        self.scheduler = SchedulerEngine(self, rig=self.rig_client, js8=self.js8_control)
        self.scheduler.start()

        log.info("Main window initialized.")

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
            if hasattr(self.stations_map_tab, "_load_operator_history"):
                self.stations_map_tab._load_operator_history()
                if hasattr(self.stations_map_tab, "_render_map"):
                    self.stations_map_tab._render_map(preserve_view=True)
        except Exception as e:
            log.debug("MainWindow: stations_map_tab refresh failed: %s", e)
        try:
            if hasattr(self.fldigi_tab, "_load_known_operators"):
                self.fldigi_tab._load_known_operators()
        except Exception as e:
            log.debug("MainWindow: fldigi_tab refresh failed: %s", e)

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

    def _set_screen(self, index: int) -> None:
        if 0 <= index < self.stack.count():
            self.stack.setCurrentIndex(index)
