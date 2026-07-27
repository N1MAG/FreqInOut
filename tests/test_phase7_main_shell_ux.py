from __future__ import annotations

from pathlib import Path


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

    assert '("Station", "Station Overview")' in source
    assert '("Health Details", "Station Health")' in source
    assert '("FreqPlanner", "FreqPlanner")' in source
    assert '("HF Daily", "HF Schedule")' in source
    assert '("HF Nets", "Net Schedule")' in source
    assert '("HF Peers", "Peer Schedules")' in source
    assert 'self._nav_group_order: list[str] = ["Station", "FreqPlanner", "NCS", "Operators"]' in source
    assert 'if screen == "Station Health":' in source
    assert 'return "Station"' in source
    assert 'if screen in {"HF Schedule", "Net Schedule", "Peer Schedules"}:' in source
    assert 'return "FreqPlanner"' in source
    assert '("Station Health", "Station Health")' not in source
    assert '"Schedules": False' not in source


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
    }

    for key in ("settings", "freqplanner", "messages", "hf_daily", "hf_nets"):
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
    ]
    combined = "\n".join(Path(path).read_text(encoding="utf-8") for path in source_paths)

    assert "Times: Local" in combined
    assert "Times: UTC" in combined
    assert "Showing: Local" not in combined
    assert "Showing: UTC" not in combined
