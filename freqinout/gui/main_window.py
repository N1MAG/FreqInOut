from __future__ import annotations

from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QTabWidget

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

        # Central widget with a vertical layout containing the tab widget
        central = QWidget()
        layout = QVBoxLayout(central)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        self.setCentralWidget(central)

        # Instantiate tabs
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

        # Add tabs with user-facing labels
        self.tabs.addTab(self.settings_tab, "Settings")
        self.tabs.addTab(self.hf_schedule_tab, "HF Frequency Schedule")
        self.tabs.addTab(self.net_tab, "Net Schedule")
        self.tabs.addTab(self.fldigi_tab, "FLDigi Net Control")
        self.tabs.addTab(self.js8_tab, "JS8Call Net Control")
        self.tabs.addTab(self.freq_planner_tab, "FreqPlanner")
        self.tabs.addTab(self.operator_history_tab, "Operator History")
        self.tabs.addTab(self.message_viewer_tab, "Message Viewer")
        self.tabs.addTab(self.stations_map_tab, "Stations Map")
        self.tabs.addTab(self.log_tab, "Logs")

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
        Append the configured callsign to each tab title, if available.

        This is a helper so the Settings tab can call back into the main
        window (e.g., after saving a new callsign) by doing:

            self.parent()._apply_callsign_to_tab_titles()

        If you don't want callsign in tab titles, you can remove or ignore this.
        """
        data = self.settings.all()
        callsign = (data.get("callsign") or "").strip().upper()
        if not callsign:
            # Reset to base titles if no callsign is set
            self.tabs.setTabText(self.tabs.indexOf(self.settings_tab), "Settings")
            self.tabs.setTabText(self.tabs.indexOf(self.hf_schedule_tab), "HF Frequency Schedule")
            self.tabs.setTabText(self.tabs.indexOf(self.net_tab), "Net Schedule")
            self.tabs.setTabText(self.tabs.indexOf(self.fldigi_tab), "FLDigi Net Control")
            self.tabs.setTabText(self.tabs.indexOf(self.js8_tab), "JS8Call Net Control")
            self.tabs.setTabText(self.tabs.indexOf(self.freq_planner_tab), "FreqPlanner")
            self.tabs.setTabText(self.tabs.indexOf(self.operator_history_tab), "Operator History")
            self.tabs.setTabText(self.tabs.indexOf(self.log_tab), "Logs")
            return

        def label(base: str) -> str:
            return f"{base} [{callsign}]"

        self.tabs.setTabText(self.tabs.indexOf(self.settings_tab), label("Settings"))
        self.tabs.setTabText(self.tabs.indexOf(self.hf_schedule_tab), label("HF Frequency Schedule"))
        self.tabs.setTabText(self.tabs.indexOf(self.net_tab), label("Net Schedule"))
        self.tabs.setTabText(self.tabs.indexOf(self.fldigi_tab), label("FLDigi Net Control"))
        self.tabs.setTabText(self.tabs.indexOf(self.js8_tab), label("JS8Call Net Control"))
        self.tabs.setTabText(self.tabs.indexOf(self.freq_planner_tab), label("FreqPlanner"))
        self.tabs.setTabText(self.tabs.indexOf(self.operator_history_tab), label("Operator History"))
        self.tabs.setTabText(self.tabs.indexOf(self.log_tab), label("Logs"))
