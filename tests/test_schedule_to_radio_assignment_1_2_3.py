from __future__ import annotations

import os
from pathlib import Path

import pytest

from freqinout.core.multi_radio_store import DEFAULT_OPERATING_SYSTEM_KEY, MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


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


def test_phase5_observer_plan_assignment_source_guardrails() -> None:
    settings_source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    store_source = Path("freqinout/core/multi_radio_store.py").read_text(encoding="utf-8")

    assert "receive_only INTEGER NOT NULL DEFAULT 0" in store_source
    assert "def _validate_assignment_plan_compatibility" in store_source
    assert "Observer / SDR radios can only be assigned receive-only frequency plans." in store_source
    assert 'QCheckBox("Receive-only plan (observer / SDR compatible)")' in settings_source
    assert '"Receive-only"' in settings_source
    assert "Observer / SDR radios can only be assigned receive-only frequency plans." in settings_source


def test_phase5_frequency_plan_source_provenance_source_wiring() -> None:
    store_source = Path("freqinout/core/multi_radio_store.py").read_text(encoding="utf-8")
    projection_source = Path("freqinout/core/shared_state_persistence.py").read_text(encoding="utf-8")
    context_source = Path("freqinout/core/plan_context_service.py").read_text(encoding="utf-8")
    label_source = Path("freqinout/gui/plan_context_label.py").read_text(encoding="utf-8")

    assert "source_refs_json TEXT NOT NULL DEFAULT '[]'" in store_source
    assert "schedule_refs_json TEXT NOT NULL DEFAULT '[]'" in store_source
    assert "frequency_refs_json TEXT NOT NULL DEFAULT '[]'" in store_source
    assert "group_refs_json TEXT NOT NULL DEFAULT '[]'" in store_source
    assert "def _coerce_ref_list_json_text" in store_source
    assert "source_refs=_json_string_tuple(row.get(\"source_refs_json\"))" in projection_source
    assert "source_ref_count: int = 0" in context_source
    assert "Sources: {', '.join(ref_counts)}." in label_source


def test_phase5_freqplanner_workspace_foundation_source_wiring() -> None:
    planner_source = Path("freqinout/gui/freq_planner_tab.py").read_text(encoding="utf-8")

    assert "self.frequency_plan_combo = QComboBox()" in planner_source
    assert 'self.frequency_plan_combo.setObjectName("freqPlannerFrequencyPlanCombo")' in planner_source
    assert 'self.save_plan_btn = QPushButton("Save Plan")' in planner_source
    assert 'self.assign_plan_btn = QPushButton("Assign Plan")' in planner_source
    assert 'self.make_active_plan_btn = QPushButton("Make Active")' in planner_source
    assert 'self.use_ad_hoc_plan_btn = QPushButton("Use Ad Hoc")' in planner_source
    assert "Frequency Plan workspace action placeholder" in planner_source
    assert 'self.frequency_plan_summary_label.setObjectName("freqPlannerFrequencyPlanSummary")' in planner_source
    assert 'self.frequency_plan_action_hint_label.setObjectName("freqPlannerFrequencyPlanActionHint")' in planner_source
    assert "Plan editing actions arrive in a later update." in planner_source
    assert "def _refresh_plan_workspace_header(self) -> None:" in planner_source
    assert "self._refresh_plan_workspace_header()" in planner_source


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
        assert "Missing Frequency Plan #" in source
        assert "No Frequency Plans" in source
        assert "Frequency Plan-targeted rows require a Frequency Plan." in source
        assert "Device Profile rows apply only" not in source
        assert "Operating Profile rows apply only" not in source
        assert "Missing operating profile" not in source
        assert "No operating profiles" not in source
        assert "Operating-profile-targeted rows require an operating profile." not in source
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
    assert 'self.operating_plan_inputs_label.setObjectName("sopOperatingPlanInputsSummary")' in source
    assert "Operating Plan Inputs:" in source
    assert "def _refresh_operating_plan_inputs_summary(self) -> None:" in source
    assert "self._refresh_operating_plan_inputs_summary()" in source
    assert "context.source_ref_count" in source
    assert "context.receive_only" in source


def test_sop_builder_active_widget_renders_operating_plan_inputs_summary(monkeypatch, tmp_path) -> None:
    from PySide6.QtWidgets import QApplication, QLabel

    from freqinout.core.plan_context_service import PlanContextService
    from freqinout.gui.sop_tab import SOPTab

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    plan = store.save_operating_profile(
        {
            "name": "RX Watch SOP Plan",
            "scheduler_enabled": 0,
            "receive_only": 1,
            "source_refs": ["src_hf", "src_net"],
            "schedule_refs": ["hf:mon:1900"],
        }
    )
    primary_radio = next(row for row in store.list_device_profiles() if int(row.get("runtime_primary", 0) or 0) == 1)
    store.set_device_operating_profile(int(primary_radio["id"]), int(plan["id"]))
    store.set_device_profile_runtime_active(int(primary_radio["id"]), True)

    tab = SOPTab(plan_context_service=PlanContextService(store))
    try:
        label = tab.findChild(QLabel, "sopOperatingPlanInputsSummary")

        assert label is not None
        assert label is tab.operating_plan_inputs_label
        assert f"Operating Plan Inputs: RX Watch SOP Plan assigned to {primary_radio['name']}" in label.text()
        assert "receive-only" in label.text()
        assert "2 sources" in label.text()
        assert "1 schedule ref" in label.text()
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


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


def test_multi_radio_store_round_trips_frequency_plan_provenance_fields(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    created = store.save_operating_profile(
        {
            "name": "Source Audit Plan",
            "category": "Event",
            "status": "draft",
            "source_refs": ["src_hf", "src_net", "src_hf"],
            "schedule_refs": "hf:mon:1900, net:tue:2000",
            "frequency_refs": ["freq_40m"],
            "group_refs": ["ARES"],
            "notes": "Reviewed by operator.",
        }
    )
    loaded = store.get_operating_profile(int(created["id"]))

    assert loaded is not None
    assert loaded["category"] == "event"
    assert loaded["status"] == "draft"
    assert loaded["source_refs_json"] == '["src_hf", "src_net"]'
    assert loaded["schedule_refs_json"] == '["hf:mon:1900", "net:tue:2000"]'
    assert loaded["frequency_refs_json"] == '["freq_40m"]'
    assert loaded["group_refs_json"] == '["ARES"]'
    assert loaded["notes"] == "Reviewed by operator."


def test_multi_radio_store_requires_receive_only_plan_for_observer_radio(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    observer_radio = store.save_device_profile(
        {
            "name": "RX Observer SDR",
            "enabled": 1,
            "runtime_active": 0,
            "runtime_primary": 0,
            "device_class": "observer",
            "control_backend": "manual",
            "sdr_host": "127.0.0.1",
            "sdr_port": 8073,
        }
    )
    tx_plan = store.save_operating_profile(
        {
            "name": "Transmit-capable Plan",
            "enabled": 1,
            "receive_only": 0,
        }
    )
    rx_plan = store.save_operating_profile(
        {
            "name": "Receive-only Plan",
            "enabled": 1,
            "scheduler_enabled": 0,
            "use_launch_control": 0,
            "receive_only": 1,
        }
    )

    with pytest.raises(ValueError, match="receive-only frequency plans"):
        store.set_device_operating_profile(int(observer_radio["id"]), int(tx_plan["id"]))

    assigned = store.set_device_operating_profile(int(observer_radio["id"]), int(rx_plan["id"]))

    assert int(assigned["device_profile_id"]) == int(observer_radio["id"])
    assert int(assigned["operating_profile_id"]) == int(rx_plan["id"])


def test_multi_radio_store_blocks_receive_only_plan_edit_when_assigned_to_observer(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    observer_radio = store.save_device_profile(
        {
            "name": "RX Watch",
            "enabled": 1,
            "runtime_active": 0,
            "runtime_primary": 0,
            "device_class": "observer",
            "control_backend": "manual",
        }
    )
    rx_plan = store.save_operating_profile(
        {
            "name": "Observer Watch Plan",
            "enabled": 1,
            "scheduler_enabled": 0,
            "receive_only": 1,
        }
    )
    store.set_device_operating_profile(int(observer_radio["id"]), int(rx_plan["id"]))

    with pytest.raises(ValueError, match="keep the plan receive-only"):
        store.save_operating_profile({"id": int(rx_plan["id"]), "name": "Observer Watch Plan", "receive_only": 0})

    saved = store.get_operating_profile(int(rx_plan["id"]))
    assert saved is not None
    assert int(saved["receive_only"]) == 1


def test_multi_radio_store_blocks_observer_class_edit_with_transmit_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    radio = store.save_device_profile(
        {
            "name": "Secondary TX",
            "enabled": 1,
            "runtime_active": 0,
            "runtime_primary": 0,
            "device_class": "tx_rx",
            "control_backend": "manual",
        }
    )
    tx_plan = store.save_operating_profile(
        {
            "name": "Transmit Plan",
            "enabled": 1,
            "receive_only": 0,
        }
    )
    store.set_device_operating_profile(int(radio["id"]), int(tx_plan["id"]))

    with pytest.raises(ValueError, match="receive-only frequency plans"):
        store.save_device_profile({"id": int(radio["id"]), "name": "Secondary TX", "device_class": "observer"})

    saved = store.get_device_profile(int(radio["id"]))
    assert saved is not None
    assert saved["device_class"] == "tx_rx"


def test_multi_radio_store_does_not_auto_assign_default_plan_to_unassigned_observer(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    observer_radio = store.save_device_profile(
        {
            "name": "Unassigned RX Watch",
            "enabled": 1,
            "runtime_active": 0,
            "runtime_primary": 0,
            "device_class": "observer",
            "control_backend": "manual",
        }
    )

    with pytest.raises(ValueError, match="receive-only frequency plans"):
        store.set_device_profile_runtime_active(int(observer_radio["id"]), True)

    assert store.get_effective_assignment_for_device(int(observer_radio["id"])) is None
    refreshed = store.get_device_profile(int(observer_radio["id"]))
    assert refreshed is not None
    assert int(refreshed["runtime_active"]) == 0
