from __future__ import annotations

from pathlib import Path
from types import MethodType

from PySide6.QtWidgets import QApplication, QPushButton


def test_phase7_main_window_has_global_ledge_clock() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "self.ledge_clock_widget = QFrame(self.nav_widget)" in source
    assert 'self.ledge_clock_widget.setObjectName("mainLedgeClock")' in source
    assert 'self.ledge_local_time_label.setObjectName("ledgeLocalTime")' in source
    assert 'self.ledge_utc_time_label.setObjectName("ledgeUtcTime")' in source
    assert "self._ledge_clock_timer = QTimer(self)" in source
    assert "self._ledge_clock_timer.timeout.connect(self._update_ledge_clock)" in source
    assert "def _update_ledge_clock(self) -> None:" in source
    assert "get_timezone(tz_name)" in source


def test_phase7_navigation_groups_station_health_and_schedule_editors() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert '("Control Center", "Station Overview")' in source
    assert '("Health Details", "Station Health")' in source
    assert '("Overview", "FreqPlanner")' in source
    assert '("HF Daily", "HF Schedule")' in source
    assert '("HF Nets", "Net Schedule")' in source
    assert '("HF Peers", "Peer Schedules")' in source
    assert 'self._nav_group_order: list[str] = ["Station", "FreqPlanner", "NCS", "Operators"]' in source
    assert 'defaults = {"Station": True, "FreqPlanner": True, "NCS": False, "Operators": False}' in source
    assert 'defaults["Station"] = True' in source
    assert 'defaults["FreqPlanner"] = True' in source
    assert 'if screen in {"Station Overview", "Station Health"}:' in source
    assert 'return "Station"' in source
    assert 'if screen in {"FreqPlanner", "HF Schedule", "Net Schedule", "Peer Schedules"}:' in source
    assert 'return "FreqPlanner"' in source
    assert '("Station Health", "Station Health")' not in source
    assert '"Schedules": False' not in source
    assert "def _expand_nav_group_for_screen" in source
    assert "self._expand_nav_group_for_screen(label)" in source
    assert "if changed:\n            self._persist_nav_group_states()" not in source
    assert 'if key == "Station" and not expanded:' in source
    assert "self._station_health_alert_counts()" in source
    assert "Expand Station or open Health Details." in source


def test_phase7_primary_nav_groups_recover_from_persisted_collapsed_state() -> None:
    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def get(self, key: str, default=None):
            if key == "main_nav_group_states":
                return {
                    "Station": False,
                    "FreqPlanner": False,
                    "Schedules": False,
                    "NCS": True,
                    "Operators": True,
                }
            return default

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()

    states = MainWindow._load_nav_group_states(window)

    assert states["Station"] is True
    assert states["FreqPlanner"] is True
    assert states["NCS"] is True
    assert states["Operators"] is True


def test_phase7_station_workspace_decisions_are_specified() -> None:
    spec = Path(
        "/Users/bill/RadioCode/WORK/MultiRig/"
        "FIO_MultiRig_Phase7_Main_Shell_Station_Workspace_UX_Spec_2026-07-26.md"
    ).read_text(encoding="utf-8")

    assert "Station Control And Station Health Consolidate Into One Station Workspace" in spec
    assert "Start With A Read-Only Station Control Center" in spec
    assert "FreqPlanner Owns Schedule Planning" in spec
    assert "Global Local/UTC Clock Lives In The Main Ledge" in spec
    assert "Settings Layout Cleanup Is Part Of Phase 7" in spec
    assert "Tables Need Stable Headers" in spec


def test_phase7_high_use_tabs_hide_duplicate_live_clocks() -> None:
    sources = {
        "settings": Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8"),
        "freqplanner": Path("freqinout/gui/freq_planner_tab.py").read_text(encoding="utf-8"),
        "controlfreq": Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8"),
        "messages": Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8"),
        "hf_daily": Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8"),
        "hf_nets": Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8"),
        "sop": Path("freqinout/gui/sop_tab.py").read_text(encoding="utf-8"),
        "fldigi_ncs": Path("freqinout/gui/fldigi_net_control_tab.py").read_text(encoding="utf-8"),
        "js8_ncs": Path("freqinout/gui/js8call_net_control_tab.py").read_text(encoding="utf-8"),
        "local_ncs": Path("freqinout/gui/local_ncs_tab.py").read_text(encoding="utf-8"),
    }

    for key in ("settings", "freqplanner", "messages", "hf_daily", "hf_nets", "sop", "fldigi_ncs", "js8_ncs", "local_ncs"):
        assert "self.utc_label.setVisible(False)" in sources[key]
        assert "self.local_label.setVisible(False)" in sources[key]

    assert "self.current_time_label.setVisible(False)" in sources["controlfreq"]


def test_phase7_table_time_toggles_use_times_wording() -> None:
    source_paths = [
        "freqinout/gui/freq_planner_tab.py",
        "freqinout/gui/controlfreq_tab.py",
        "freqinout/gui/message_viewer_tab.py",
        "freqinout/gui/daily_schedule_tab.py",
        "freqinout/gui/net_schedule_tab.py",
        "freqinout/gui/peer_sched_tab.py",
        "freqinout/gui/sop_tab.py",
    ]
    combined = "\n".join(Path(path).read_text(encoding="utf-8") for path in source_paths)

    assert "Times: Local" in combined
    assert "Times: UTC" in combined
    assert "Showing: Local" not in combined
    assert "Showing: UTC" not in combined


def test_phase7_collapsed_station_group_shows_health_alert(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow
    from freqinout.gui.theme import get_theme

    window = MainWindow.__new__(MainWindow)
    header = QPushButton("Station")
    window._nav_group_order = ["Station"]
    window._nav_group_headers = {"Station": header}
    window._nav_group_states = {"Station": False}
    window._ncs_net_active = {}
    window._station_health_alert_summary = {"issue_count": 2, "severity": "danger"}
    window._nav_button_alignment_style = MainWindow._nav_button_alignment_style
    window._sync_nav_group_header_font = MethodType(MainWindow._sync_nav_group_header_font, window)
    window._set_nav_group_header_visual_state = MethodType(MainWindow._set_nav_group_header_visual_state, window)
    window._station_health_alert_counts = MethodType(MainWindow._station_health_alert_counts, window)

    MainWindow._update_nav_group_header_styles(window, get_theme("light"))

    assert "Station Health: 2 responsiveness issues" in header.toolTip()
    assert "#C62828" in header.styleSheet()

    header.deleteLater()
    app.processEvents()


def test_phase7_settings_sections_use_bounded_fit_content_layouts() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "op_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in source
    assert "logging_section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in source
    assert "self.operating_profiles_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in source
    assert "self.operating_profiles_section_group = operating_group" in source
    assert 'self._make_collapsible_group(\n            "Frequency Plans",' in source
    assert "fit_content_in_stack=True" in source
    assert 'self._refresh_fit_content_section_height(getattr(self, "operating_profiles_section_group", None))' in source
    assert "self.trusted_hash_table.setMaximumHeight(240)" in source
    assert "self.gpg_keys_table.setMaximumHeight(300)" in source
    assert "gpg_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in source
