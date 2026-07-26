from __future__ import annotations

from pathlib import Path

from freqinout.core.multi_radio_store import DEFAULT_OPERATING_SYSTEM_KEY, MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager


def test_settings_source_exposes_radio_first_schedule_assignment_controls() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "Assigned Plan" in source
    assert 'QPushButton("Assign Plan...")' in source
    assert 'QPushButton("Restore Plan")' in source
    assert "Frequency Plans" in source
    assert "Assigned Plans" in source
    assert "Restore Default Plan" in source
    assert "Assign a frequency plan if this radio should participate in Station Default schedule workflows." in source
    assert "Schedule Profiles" not in source
    assert "Radio Schedule Assignments" not in source


def test_phase5_temporary_swap_uses_assigned_plan_language() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert 'QPushButton("Temporary Plan Swap...")' in source
    assert "Temporary Plan Swap" in source
    assert "Allow assigned plan swap coordination" in source
    assert "Primary Assigned Plan:" in source
    assert "Target Assigned Plan:" in source
    assert "Use target radio assigned plan (Recommended)" in source
    assert "Carry current Station Default assigned plan" in source
    assert "Current Station Default Assigned Plan" in source
    assert "Unable to start the temporary plan swap." in source
    assert "Unable to restore the temporary plan swap." in source
    assert "No temporary plan swap is currently active." in source
    assert "Temporary Profile Swap" not in source
    assert "Temporary Swap" not in source
    assert "Primary Profile:" not in source
    assert "Target Profile:" not in source
    assert "Use target radio schedule" not in source
    assert "Carry current Station Default schedule" not in source
    assert "temporary swap" not in source
    assert "temporary-swap" not in source
    assert "current effective schedule" not in source


def test_phase5_runtime_surfaces_use_frequency_plan_language() -> None:
    settings_source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    overview_source = Path("freqinout/gui/station_overview_tab.py").read_text(encoding="utf-8")
    main_source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "def _refresh_running_status_compat(self, force: bool = False) -> None:" in settings_source
    assert "except TypeError:" in settings_source
    assert "Assigned Plan:" in overview_source
    assert "Operating Profile:" not in overview_source
    assert "Launch Control is disabled by the primary frequency plan." in main_source
    assert 'operating_txt = operating_name or "assigned frequency plan"' in main_source
    assert "Launch Control is disabled by the primary operating profile." not in main_source
    assert 'operating_txt = operating_name or "assigned operating profile"' not in main_source


def test_phase5_schedule_tabs_use_frequency_plan_target_language() -> None:
    daily_source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")
    net_source = Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8")
    planner_source = Path("freqinout/gui/freq_planner_tab.py").read_text(encoding="utf-8")
    plan_label_source = Path("freqinout/gui/plan_context_label.py").read_text(encoding="utf-8")
    guide_source = Path("docs/guide.html").read_text(encoding="utf-8")

    for source in (daily_source, net_source):
        assert '("Radio Profile", TARGET_SCOPE_DEVICE_PROFILE)' in source
        assert '("Frequency Plan", TARGET_SCOPE_OPERATING_PROFILE)' in source
        assert 'return name or f"Frequency Plan #{profile_id}"' in source
        assert "Radio Profile rows apply only when that radio is the station default." in source
        assert "Frequency Plan rows apply only when the station-default radio carries that assigned plan." in source
        assert "Device Profile rows apply only" not in source
        assert "Operating Profile rows apply only" not in source
        assert 'return name or f"Profile #{profile_id}"' not in source

    assert "Read-only Frequency Plan coverage review" in plan_label_source
    assert "def plan_context_display_text" in plan_label_source
    assert "self.plan_context_service = plan_context_service or PlanContextService()" in planner_source
    assert "service=self.plan_context_service" in planner_source
    assert "self.plan_context_label = PlanContextLabel(" in planner_source
    assert '"freqplanner",' in planner_source
    assert "self.plan_context_label.refresh_context(refresh=True)" in planner_source
    assert "Use FreqPlanner to verify where and when the saved plan expects activity." in planner_source
    assert "Frequency Plans" in guide_source
    assert "Assigned Plans" in guide_source
    assert "week-at-a-glance Frequency Plan coverage check" in guide_source
    assert "ControlFreq reflects results from Settings, assigned Frequency Plans" in guide_source
    assert "Map consumes operator data, Frequency Plan and schedule-source data" in guide_source
    assert "selected radio profile and its assigned current plan" in guide_source
    assert "Plan Context Cue" in guide_source
    assert "This cue is informational only." in guide_source
    assert "HF Frequency Schedule, Net Schedules, FreqPlanner, SOP Builder, Messages, ControlFreq, and Map" in guide_source
    assert "does not change the active radio, send QSY commands, hold or resume the scheduler" in guide_source


def test_main_window_owns_plan_context_service_for_lazy_freqplanner() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "from freqinout.core.plan_context_service import PlanContextService" in source
    assert "self.plan_context_service = PlanContextService()" in source
    assert "SOPTab(self, plan_context_service=self.plan_context_service)" in source
    assert "DailyScheduleTab(self, plan_context_service=self.plan_context_service)" in source
    assert "NetScheduleTab(self, plan_context_service=self.plan_context_service)" in source
    assert "ControlFreqTab(self, plan_context_service=self.plan_context_service)" in source
    assert "StationsMapTab(self, plan_context_service=self.plan_context_service)" in source
    assert "FreqPlannerTab(self, plan_context_service=self.plan_context_service)" in source
    assert "MessageViewerTab(self, plan_context_service=self.plan_context_service)" in source
    assert 'if "plan_context_service" not in str(exc):' not in source
    assert "except TypeError as exc:" not in source
    assert "self.sop_tab = SOPTab(self)" not in source
    assert "self.hf_schedule_tab = DailyScheduleTab(self)" not in source
    assert "self.net_tab = NetScheduleTab(self)" not in source
    assert "self.controlfreq_tab = ControlFreqTab(self)" not in source
    assert "self.stations_map_tab = StationsMapTab(self)" not in source
    assert "self.message_viewer_tab = MessageViewerTab(self)" not in source
    assert "def _plan_context_consumer_widgets(self) -> tuple[object | None, ...]:" in source
    assert "def _refresh_plan_context_labels(self, reason: str = \"\") -> None:" in source
    assert "self.plan_context_service.invalidate()" in source
    assert 'getattr(self, "hf_schedule_tab", None)' in source
    assert 'getattr(self, "net_tab", None)' in source
    assert 'getattr(self, "freq_planner_tab", None)' in source
    assert 'getattr(self, "sop_tab", None)' in source
    assert 'getattr(self, "message_viewer_tab", None)' in source
    assert 'getattr(self, "controlfreq_tab", None)' in source
    assert 'getattr(self, "stations_map_tab", None)' in source
    assert "for widget in self._plan_context_consumer_widgets():" in source
    assert "label.refresh_context(refresh=True)" in source
    assert 'self._refresh_plan_context_labels("settings_saved")' in source
    assert 'self._refresh_plan_context_labels("runtime_settings_saved")' in source
    assert 'self._refresh_plan_context_labels("runtime_device_profiles_changed")' in source


def test_sop_builder_uses_shared_plan_context_label() -> None:
    source = Path("freqinout/gui/sop_tab.py").read_text(encoding="utf-8")

    assert "from freqinout.gui.plan_context_label import PlanContextLabel" in source
    assert "self.plan_context_service = plan_context_service or PlanContextService()" in source
    assert 'self.plan_context_label = PlanContextLabel(' in source
    assert '"sop",' in source
    assert "service=self.plan_context_service" in source
    assert "self.plan_context_label.refresh_context(refresh=True)" in source


def test_messages_uses_shared_plan_context_label() -> None:
    source = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert "from freqinout.gui.plan_context_label import PlanContextLabel" in source
    assert "self.plan_context_service = plan_context_service or PlanContextService()" in source
    assert 'self.plan_context_label = PlanContextLabel(' in source
    assert '"messages",' in source
    assert "service=self.plan_context_service" in source
    assert "self.plan_context_label.refresh_context(refresh=True)" in source


def test_schedule_tabs_use_shared_plan_context_label() -> None:
    daily_source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")
    net_source = Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8")

    for source, tab_id in (
        (daily_source, "hf_schedule"),
        (net_source, "net_schedule"),
    ):
        assert "from freqinout.gui.plan_context_label import PlanContextLabel" in source
        assert "self.plan_context_service = plan_context_service or PlanContextService()" in source
        assert "self.plan_context_label = PlanContextLabel(" in source
        assert f'"{tab_id}",' in source
        assert "service=self.plan_context_service" in source
        assert "self.plan_context_label.refresh_context(refresh=True)" in source
        assert "self.plan_context_label.invalidate_context()" in source


def test_controlfreq_uses_shared_plan_context_label_without_control_behavior_changes() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")

    assert "from freqinout.gui.plan_context_label import PlanContextLabel" in source
    assert "self.plan_context_service = plan_context_service" in source
    assert "self.plan_context_label = PlanContextLabel(" in source
    assert '"controlfreq",' in source
    assert "service=self.plan_context_service" in source
    assert "create_service=self.plan_context_service is not None" in source
    assert "self.plan_context_label.refresh_context(refresh=True)" in source
    assert "self.plan_context_label.invalidate_context()" in source
    assert "ControlFreq uses the current radio and Frequency Plan context" in source


def test_map_uses_shared_plan_context_label_without_map_behavior_changes() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "from freqinout.gui.plan_context_label import PlanContextLabel" in source
    assert "self.plan_context_service = plan_context_service" in source
    assert "self.plan_context_label = PlanContextLabel(" in source
    assert '"map",' in source
    assert "service=self.plan_context_service" in source
    assert "create_service=self.plan_context_service is not None" in source
    assert "self.plan_context_label.refresh_context(refresh=True)" in source
    assert "Map uses the current radio and Frequency Plan context" in source
    assert "updateMapData" in source


def test_multi_radio_store_round_trips_schedule_assignment_for_radio(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    primary_radio = next(row for row in store.list_device_profiles() if int(row.get("runtime_primary", 0) or 0) == 1)
    created_profile = store.save_operating_profile(
        {
            "name": "Night Net Schedule",
            "enabled": 1,
            "scheduler_enabled": 1,
            "scheduler_mode": "full",
            "description": "Test frequency plan for radio assignment coverage.",
        }
    )

    assigned = store.set_device_operating_profile(
        int(primary_radio["id"]),
        int(created_profile["id"]),
        assignment_state="active",
        reason="Schedule-to-radio coverage",
    )

    assert int(assigned["device_profile_id"]) == int(primary_radio["id"])
    assert int(assigned["operating_profile_id"]) == int(created_profile["id"])
    assert str(assigned["assignment_state"]) == "active"

    effective = store.get_effective_assignment_for_device(int(primary_radio["id"]))
    assert effective is not None
    assert int(effective["operating_profile_id"]) == int(created_profile["id"])

    store.restore_default_operating_profile(int(primary_radio["id"]))

    default_profile = next(
        row for row in store.list_operating_profiles() if str(row.get("system_key", "") or "") == DEFAULT_OPERATING_SYSTEM_KEY
    )

    restored = store.get_effective_assignment_for_device(int(primary_radio["id"]))
    assert restored is not None
    assert int(restored["operating_profile_id"]) == int(default_profile["id"])
