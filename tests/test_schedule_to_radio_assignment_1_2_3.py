from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from freqinout.core.multi_radio_store import DEFAULT_OPERATING_SYSTEM_KEY, MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _create_primary_radio(store: MultiRadioStore, name: str = "Primary Radio") -> dict:
    return store.save_device_profile(
        {
            "name": name,
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
        }
    )


def test_settings_source_exposes_radio_first_operating_model_assignment_controls() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "Operating Model Assignment" in source
    assert 'QPushButton("Assign Model")' in source
    assert 'QPushButton("Restore Model")' in source
    assert 'QPushButton("Assign with RF Guard")' in source
    assert 'QPushButton("Swap Plans with RF Guard")' in source
    assert 'QPushButton("Save with RF Guard")' in source
    assert "RF Guard Blocked Assignment" in source
    assert "RF Guard Blocked Plan Swap" in source
    assert "Schedule Assignment Saved" in source
    assert "Choose a radio and Frequency Plan, then save with RF Guard before the schedule changes." in source
    assert "self.schedule_assignment_radio_combo = QComboBox()" in source
    assert "def open_schedule_assignment_editor(self, *, plan_id: int = 0, device_profile_id: int = 0) -> bool:" in source
    assert "self._select_settings_section_group(group)" in source
    assert 'self.schedule_assignments_table = QTableWidget(0, 7)' in source
    assert '["Selected", "Active", "Default", "Radio", "Frequency Plan", "State", "Guard Status"]' in source
    assert "def _save_schedule_assignment_editor(self) -> None:" in source
    assert "def _update_schedule_assignment_editor_hint(self) -> None:" in source
    assert "validate_frequency_plan_for_device(device_id, plan)" in source
    assert "RF Guard warning: {warnings[0]}" in source
    assert '"RF Guard Needs Review"' in source
    assert 'assignment.get("validation_status_json", "")' in source
    assert "selection_model.selectedRows()" in source
    assert "append_assignment_for_device(device_profile_id)" in source
    assert 'self.schedule_assignment_state_combo.addItem("Inactive", "inactive")' not in source
    assert "No schedule assignments were changed." in source
    assert "self.multi_radio_store.set_assigned_plan(" in source
    assert "self.multi_radio_store.swap_assigned_frequency_plans(" in source
    assert "Operating Models" in source
    assert "Schedule Assignment" in source
    assert "Restore Default Model" in source
    assert "Assign an operating model if this radio should participate in Station Default workflows." in source
    assert "Schedule Profiles" not in source


def test_phase5_temporary_swap_uses_assigned_model_language() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert 'QPushButton("Temporary Model Swap...")' in source
    assert "Temporary Model Swap" in source
    assert "Allow assignment swap coordination" in source
    assert "Primary Assigned Model:" in source
    assert "Target Assigned Model:" in source
    assert "Use target radio assigned model (Recommended)" in source
    assert "Carry current Station Default assigned model" in source
    assert "Current Station Default Assigned Model" in source
    assert "Unable to start the temporary model swap." in source
    assert "Unable to restore the temporary model swap." in source
    assert "No temporary model swap is currently active." in source
    assert "Temporary Profile Swap" not in source
    assert "Temporary Swap" not in source
    assert "Primary Profile:" not in source
    assert "Target Profile:" not in source
    assert "Use target radio schedule" not in source
    assert "Carry current Station Default schedule" not in source
    assert "temporary swap" not in source
    assert "temporary-swap" not in source
    assert "current effective schedule" not in source


def test_source_only_schedule_cannot_be_assigned_to_radio(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = _create_primary_radio(store, "Assignment Guard Radio")
    source_plan = store.save_frequency_plan(
        {
            "name": "Named Daily Source",
            "category": "hf_daily_schedule",
            "schedule_refs": [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00"}],
        }
    )

    with pytest.raises(ValueError, match="blended into a Frequency Plan"):
        store.set_assigned_plan(int(radio["id"]), int(source_plan["id"]))


def test_phase5_runtime_surfaces_use_frequency_plan_language() -> None:
    settings_source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    overview_source = Path("freqinout/gui/station_overview_tab.py").read_text(encoding="utf-8")
    main_source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "def _refresh_running_status_compat(self, force: bool = False) -> None:" in settings_source
    assert "except TypeError:" in settings_source
    assert "Operating Model:" in overview_source
    assert "Operating Profile:" not in overview_source
    assert "Launch Control is disabled by the primary operating model." in main_source
    assert 'operating_txt = operating_name or "assigned operating model"' in main_source
    assert "Launch Control is disabled by the primary operating profile." not in main_source
    assert 'operating_txt = operating_name or "assigned frequency plan"' not in main_source


def test_phase5_observer_plan_assignment_source_guardrails() -> None:
    settings_source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    store_source = Path("freqinout/core/multi_radio_store.py").read_text(encoding="utf-8")

    assert "receive_only INTEGER NOT NULL DEFAULT 0" in store_source
    assert "def _validate_assignment_plan_compatibility" in store_source
    assert "Observer / SDR radios can only be assigned receive-only operating models." in store_source
    assert 'QCheckBox("Receive-only model (observer / SDR compatible)")' in settings_source
    assert 'QCheckBox("Receive-only plan (observer / SDR compatible)")' not in settings_source
    assert 'QCheckBox("Allow assigned plan swap coordination")' not in settings_source
    assert '"Receive-only"' in settings_source
    assert "Observer / SDR radios can only be assigned receive-only operating models." in settings_source


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

    assert 'QLabel("<h3>Plan Builder</h3>")' in planner_source
    assert "plan_workspace = QVBoxLayout()" in planner_source
    assert "plan_select_row = QHBoxLayout()" in planner_source
    assert "plan_select_row.addWidget(self.save_plan_btn)" in planner_source
    assert "source_workspace.addWidget(self.save_sop_plan_btn)" in planner_source
    assert "source_workspace.addWidget(self.build_sop_layer_btn)" in planner_source
    assert 'self.plan_ingredients_frame.setObjectName("freqPlannerPlanIngredients")' in planner_source
    assert 'self.plan_ingredients_scroll.setObjectName("freqPlannerPlanIngredientsScroll")' in planner_source
    assert "self.plan_ingredients_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in planner_source
    assert "chip.setMinimumWidth(230)" in planner_source
    assert "self.plan_layers_label.setVisible(False)" in planner_source
    assert "def _refresh_plan_ingredients" in planner_source
    assert "linked by default" in planner_source
    assert "plan_source_usage_summary" in planner_source
    assert "view_workspace.addWidget(self.review_rf_guard_btn)" in planner_source
    assert "view_workspace.addWidget(self.assign_plan_btn)" in planner_source
    assert 'self.plan_review_toolbar_scroll.setObjectName("freqPlannerReviewToolbarScroll")' in planner_source
    assert "self.plan_review_toolbar_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in planner_source
    assert "self.plan_review_controls_frame.setFrameShape(QFrame.NoFrame)" in planner_source
    assert "self.frequency_plan_combo = QComboBox()" in planner_source
    assert 'self.frequency_plan_combo.setObjectName("freqPlannerFrequencyPlanCombo")' in planner_source
    assert "self.frequency_plan_combo.setEditable(True)" in planner_source
    assert "self.frequency_plan_combo.setMinimumWidth(240)" in planner_source
    assert 'self.save_plan_btn = QPushButton("Save Plan")' in planner_source
    assert 'self.save_sop_plan_btn = QPushButton("Save SOP Plan")' in planner_source
    assert 'self.delete_plan_btn = QPushButton("Delete Plan")' in planner_source
    assert 'self.assign_plan_btn = QPushButton("Assign in Settings")' in planner_source
    assert 'self.make_active_plan_btn = QPushButton("Make Active")' not in planner_source
    assert 'self.use_ad_hoc_plan_btn = QPushButton("Use Ad Hoc")' not in planner_source
    assert "self.save_plan_btn.clicked.connect(self._on_save_plan_clicked)" in planner_source
    assert "self.save_sop_plan_btn.clicked.connect(self._on_save_sop_plan_clicked)" in planner_source
    assert "self.delete_plan_btn.clicked.connect(self._on_delete_plan_clicked)" in planner_source
    assert "self.assign_plan_btn.clicked.connect(self._on_assign_plan_clicked)" in planner_source
    assert "Save or update the visible HF Daily + HF Nets + SOP projection" in planner_source
    assert "Settings > Assign Schedule" in planner_source
    assert "Choose the radio and save with RF Guard." in planner_source
    assert 'self.frequency_plan_summary_label.setObjectName("freqPlannerFrequencyPlanSummary")' in planner_source
    assert 'self.frequency_plan_action_hint_label.setObjectName("freqPlannerFrequencyPlanActionHint")' in planner_source
    assert "self.frequency_plan_action_hint_label.setWordWrap(False)" in planner_source
    assert "Build a named plan by selecting HF Daily, HF Nets, and optional SOP layers" in planner_source
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
    assert "Build where-to-be, when-to-be-there, and what-to-do plans from HF Daily, HF Nets, and SOP layers." in planner_source
    assert "Frequency Plans" in guide_source
    assert "Assigned Plans" in guide_source
    assert "build, review, edit, and assign named Frequency Plans" in guide_source
    assert "where to be, when to be there, and what to do when you get there" in guide_source
    assert "Update Plan Only" in guide_source
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
    primary_radio = _create_primary_radio(store, "SOP Primary")
    model = store.save_operating_profile(
        {
            "name": "RX Watch SOP Model",
            "scheduler_enabled": 0,
            "receive_only": 1,
        }
    )
    plan = store.save_frequency_plan(
        {
            "name": "RX Watch SOP Plan",
            "receive_only": 1,
            "source_refs": ["src_hf", "src_net"],
            "schedule_refs": ["hf:mon:1900"],
        }
    )
    store.set_device_operating_profile(int(primary_radio["id"]), int(model["id"]))
    store.set_assigned_plan(int(primary_radio["id"]), int(plan["id"]))
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
    assert "Ops Center uses the current radio and Frequency Plan context" in source


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

    primary_radio = _create_primary_radio(store)
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


def test_multi_radio_store_round_trips_durable_schedule_assignment_foundation(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = _create_primary_radio(store, "Schedule Radio")
    plan = store.save_frequency_plan(
        {
            "name": "Night Net Schedule",
            "description": "Built schedule that can be assigned independently from an Operating Model.",
            "source_refs": ["freqplanner:overview"],
            "schedule_refs": ["hf_nets:night"],
            "frequency_refs": ["40m:7.078"],
        }
    )

    assigned = store.set_assigned_plan(
        int(radio["id"]),
        int(plan["id"]),
        reason="Schedule assignment foundation coverage",
    )

    assert int(assigned["device_profile_id"]) == int(radio["id"])
    assert int(assigned["frequency_plan_id"]) == int(plan["id"])
    assert str(assigned["assignment_state"]) == "active"

    effective = store.get_effective_assigned_plan_for_device(int(radio["id"]))
    assert effective is not None
    assert int(effective["frequency_plan_id"]) == int(plan["id"])

    validation = json.loads(str(effective["validation_status_json"]))
    assert validation["rf_guard_validation"] == "enforced"
    assert validation["state"] == "ok"


def test_multi_radio_store_swaps_assigned_frequency_plans_atomically(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    first_radio = _create_primary_radio(store, "FIO-A")
    second_radio = store.save_device_profile(
        {
            "name": "FIO-B",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 0,
            "device_class": "tx_rx",
            "control_backend": "manual",
        }
    )
    first_plan = store.save_frequency_plan(
        {
            "name": "MagNet Main Plan",
            "schedule_refs": [{"day_utc": "ALL", "start_utc": "00:00", "end_utc": "23:59", "group": "MAGNET", "band": "40M"}],
            "frequency_refs": ["40m:7.115"],
        }
    )
    second_plan = store.save_frequency_plan(
        {
            "name": "AmRRON Main Plan",
            "schedule_refs": [{"day_utc": "ALL", "start_utc": "00:00", "end_utc": "23:59", "group": "AMRRON", "band": "20M"}],
            "frequency_refs": ["20m:14.110"],
        }
    )
    store.set_assigned_plan(int(first_radio["id"]), int(first_plan["id"]))
    store.set_assigned_plan(int(second_radio["id"]), int(second_plan["id"]))

    swapped = store.swap_assigned_frequency_plans(int(first_radio["id"]), int(second_radio["id"]))

    assert len(swapped) == 2
    first_effective = store.get_effective_assigned_plan_for_device(int(first_radio["id"]))
    second_effective = store.get_effective_assigned_plan_for_device(int(second_radio["id"]))
    assert first_effective is not None
    assert second_effective is not None
    assert int(first_effective["frequency_plan_id"]) == int(second_plan["id"])
    assert int(second_effective["frequency_plan_id"]) == int(first_plan["id"])
    assert str(first_effective["created_by"]) == "settings_ui_swap"
    assert str(second_effective["created_by"]) == "settings_ui_swap"


def test_multi_radio_store_requires_receive_only_schedule_plan_for_observer_radio(monkeypatch, tmp_path) -> None:
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
        }
    )
    tx_plan = store.save_frequency_plan({"name": "Transmit Schedule", "receive_only": 0})
    rx_plan = store.save_frequency_plan({"name": "Receive-only Schedule", "receive_only": 1})

    with pytest.raises(ValueError, match="receive-only schedule plans"):
        store.set_assigned_plan(int(observer_radio["id"]), int(tx_plan["id"]))

    assigned = store.set_assigned_plan(int(observer_radio["id"]), int(rx_plan["id"]))

    assert int(assigned["device_profile_id"]) == int(observer_radio["id"])
    assert int(assigned["frequency_plan_id"]) == int(rx_plan["id"])


def test_multi_radio_store_blocks_receive_only_schedule_plan_edit_when_assigned_to_observer(
    monkeypatch, tmp_path
) -> None:
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
    rx_plan = store.save_frequency_plan({"name": "Observer Watch Schedule", "receive_only": 1})
    store.set_assigned_plan(int(observer_radio["id"]), int(rx_plan["id"]))

    with pytest.raises(ValueError, match="keep the plan receive-only"):
        store.save_frequency_plan({"id": int(rx_plan["id"]), "name": "Observer Watch Schedule", "receive_only": 0})

    saved = store.get_frequency_plan(int(rx_plan["id"]))
    assert saved is not None
    assert int(saved["receive_only"]) == 1


def test_schedule_assignment_warns_for_unsupported_antenna_band(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "Band Limited Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "warn",
        }
    )
    plan = store.save_frequency_plan({"name": "Forty Meter Schedule", "frequency_refs": ["40M:7.078"]})

    assigned = store.set_assigned_plan(int(radio["id"]), int(plan["id"]))
    validation = json.loads(str(assigned["validation_status_json"]))

    assert validation["state"] == "warning"
    assert validation["rf_guard_validation"] == "enforced"
    assert "40M" in validation["plan_bands"]
    assert "20M" in validation["supported_bands"]
    assert "does not include 40M" in validation["warnings"][0]


def test_schedule_assignment_warns_for_unsupported_band_from_schedule_rows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "Forty Twenty Fifteen Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M", "20M", "15M"],
            "antenna_band_guard_mode": "warn",
        }
    )
    plan = store.save_frequency_plan(
        {
            "name": "Plan With Eighty",
            "schedule_refs": [
                {
                    "day_utc": "ALL",
                    "start_utc": "00:00",
                    "end_utc": "06:00",
                    "group": "MAGNET",
                    "mode": "DIGI",
                    "band": "80M",
                    "frequency": "3.585",
                }
            ],
        }
    )

    validation = store.validate_frequency_plan_for_device(int(radio["id"]), plan)

    assert validation["state"] == "warning"
    assert "80M" in validation["plan_bands"]
    assert validation["supported_bands"] == ["40M", "20M", "15M"]
    assert "does not include 80M" in validation["warnings"][0]


def test_schedule_assignment_ui_summarizes_antenna_plan_band_mismatch() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "def _schedule_assignment_validation_message(" in source
    assert "Antenna and schedule mismatch: this radio supports" in source
    assert "but the selected plan uses" in source
    assert "RF Guard {tone}: {detail}" in source
    assert "RF Guard Blocked Assignment\" if warning_tone == \"blocked\" else \"RF Guard Needs Review\"" in source
    assert "warning_tone = tone or \"warning\"" in source


def test_schedule_assignment_preview_warns_for_unsaved_radio_antenna_bands(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    plan = store.save_frequency_plan(
        {
            "name": "AmRRON Plan",
            "schedule_refs": [
                {
                    "day_utc": "ALL",
                    "start_utc": "00:00",
                    "end_utc": "06:00",
                    "group": "AMRRON",
                    "mode": "DIGI",
                    "band": "80M",
                    "frequency": "3.588",
                },
                {
                    "day_utc": "ALL",
                    "start_utc": "06:00",
                    "end_utc": "16:00",
                    "group": "AMRRON",
                    "mode": "DIGI",
                    "band": "40M",
                    "frequency": "7.110",
                },
                {
                    "day_utc": "ALL",
                    "start_utc": "16:00",
                    "end_utc": "00:00",
                    "group": "AMRRON",
                    "mode": "DIGI",
                    "band": "20M",
                    "frequency": "14.110",
                },
            ],
        }
    )

    validation = store.validate_frequency_plan_for_device_payload(
        {
            "name": "TS-2000",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "flrig",
            "antenna_supported_bands": ["15M", "10M"],
            "antenna_band_guard_mode": "warn",
        },
        plan,
    )

    assert validation["state"] == "warning"
    assert validation["supported_bands"] == ["15M", "10M"]
    assert validation["plan_bands"] == ["80M", "40M", "20M"]
    joined = " ".join(validation["warnings"])
    assert "does not include 80M" in joined
    assert "does not include 40M" in joined
    assert "does not include 20M" in joined


def test_schedule_assignment_validation_refreshes_when_radio_antenna_changes(monkeypatch, tmp_path) -> None:
    from freqinout.core.station_health_summary import runtime_observability_items

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "TS-2000",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["80M", "40M", "20M"],
            "antenna_band_guard_mode": "warn",
        }
    )
    plan = store.save_frequency_plan(
        {
            "name": "AmRRON Plan",
            "schedule_refs": [
                {
                    "day_utc": "ALL",
                    "start_utc": "00:00",
                    "end_utc": "06:00",
                    "group": "AMRRON",
                    "mode": "DIGI",
                    "band": "80M",
                    "frequency": "3.588",
                },
                {
                    "day_utc": "ALL",
                    "start_utc": "06:00",
                    "end_utc": "16:00",
                    "group": "AMRRON",
                    "mode": "DIGI",
                    "band": "40M",
                    "frequency": "7.110",
                },
                {
                    "day_utc": "ALL",
                    "start_utc": "16:00",
                    "end_utc": "00:00",
                    "group": "AMRRON",
                    "mode": "DIGI",
                    "band": "20M",
                    "frequency": "14.110",
                },
            ],
        }
    )

    assigned = store.set_assigned_plan(int(radio["id"]), int(plan["id"]))
    initial_validation = json.loads(str(assigned["validation_status_json"]))
    assert initial_validation["state"] == "ok"

    store.save_device_profile(
        {
            **radio,
            "antenna_supported_bands": ["15M", "10M"],
            "antenna_band_guard_mode": "warn",
        }
    )
    refreshed = store.get_effective_assigned_plan_for_device(int(radio["id"]))
    assert refreshed is not None
    validation = json.loads(str(refreshed["validation_status_json"]))

    assert validation["state"] == "warning"
    assert validation["supported_bands"] == ["15M", "10M"]
    assert validation["plan_bands"] == ["80M", "40M", "20M"]
    joined = " ".join(validation["warnings"])
    assert "does not include 80M" in joined
    assert "does not include 40M" in joined
    assert "does not include 20M" in joined
    items = runtime_observability_items(assigned_schedule_status=[refreshed])
    assert len(items) == 1
    assert items[0]["dependency"] == "Schedule Assignment RF Guard"
    assert items[0]["severity"] == "warning"
    assert "TS-2000 / AmRRON Plan" in str(items[0]["last_issue"])


def test_schedule_assignment_blocks_unsupported_antenna_band(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "Blocked Band Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "block",
        }
    )
    plan = store.save_frequency_plan({"name": "Forty Meter Schedule", "frequency_refs": ["40M:7.078"]})

    with pytest.raises(ValueError, match="does not include 40M"):
        store.set_assigned_plan(int(radio["id"]), int(plan["id"]))

    assert store.get_effective_assigned_plan_for_device(int(radio["id"])) is None
    with store.connect() as conn:
        events = conn.execute("SELECT event_type, decision, band FROM rf_guard_events").fetchall()
    assert [(row[0], row[1], row[2]) for row in events] == [("antenna_band_support", "blocked", "40M")]


def test_schedule_assignment_warns_for_prevent_band_overlap(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Left Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    right = store.save_device_profile(
        {
            "name": "Right Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    left_plan = store.save_frequency_plan({"name": "Left 40M", "frequency_refs": ["40M:7.078"]})
    right_plan = store.save_frequency_plan({"name": "Right 40M", "frequency_refs": ["40M:7.110"]})

    store.set_assigned_plan(int(left["id"]), int(left_plan["id"]))
    assigned = store.set_assigned_plan(int(right["id"]), int(right_plan["id"]))
    validation = json.loads(str(assigned["validation_status_json"]))

    assert validation["state"] == "warning"
    assert any("Prevent Band Overlap group NORTH MAST" in warning for warning in validation["warnings"])


def test_schedule_assignment_blocks_prevent_band_overlap(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Left Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    right = store.save_device_profile(
        {
            "name": "Right Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    left_plan = store.save_frequency_plan({"name": "Left 40M", "frequency_refs": ["40M:7.078"]})
    right_plan = store.save_frequency_plan({"name": "Right 40M", "frequency_refs": ["40M:7.110"]})

    store.set_assigned_plan(int(left["id"]), int(left_plan["id"]))
    with pytest.raises(ValueError, match="would both be assigned on 40M"):
        store.set_assigned_plan(int(right["id"]), int(right_plan["id"]))

    assert store.get_effective_assigned_plan_for_device(int(right["id"])) is None


def test_schedule_assignment_blocks_prevent_band_overlap_when_peer_requires_block(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    protected = store.save_device_profile(
        {
            "name": "Protected Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    candidate = store.save_device_profile(
        {
            "name": "Candidate Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "warn",
        }
    )
    protected_plan = store.save_frequency_plan({"name": "Protected 40M", "frequency_refs": ["40M:7.078"]})
    candidate_plan = store.save_frequency_plan({"name": "Candidate 40M", "frequency_refs": ["40M:7.110"]})

    store.set_assigned_plan(int(protected["id"]), int(protected_plan["id"]))
    with pytest.raises(ValueError, match="would both be assigned on 40M"):
        store.set_assigned_plan(int(candidate["id"]), int(candidate_plan["id"]))

    assert store.get_effective_assigned_plan_for_device(int(candidate["id"])) is None
    with store.connect() as conn:
        events = conn.execute(
            """
            SELECT event_type, guard_mode, decision, band
              FROM rf_guard_events
             WHERE event_type='prevent_band_overlap'
            """
        ).fetchall()
    assert [(row[0], row[1], row[2], row[3]) for row in events] == [
        ("prevent_band_overlap", "block", "blocked", "40M")
    ]


def test_schedule_assignment_blocks_advanced_close_frequency_guard(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    protected = store.save_device_profile(
        {
            "name": "Protected Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    candidate = store.save_device_profile(
        {
            "name": "Candidate SDR",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "warn",
            "advanced_frequency_guard_window_hz": 1500,
        }
    )
    protected_plan = store.save_frequency_plan({"name": "Protected 40M", "frequency_refs": ["40M:7.078"]})
    candidate_plan = store.save_frequency_plan({"name": "Candidate 40M", "frequency_refs": ["40M:7.0795"]})

    store.set_assigned_plan(int(protected["id"]), int(protected_plan["id"]))
    with pytest.raises(ValueError, match="Advanced Guard group RX FRONTEND"):
        store.set_assigned_plan(int(candidate["id"]), int(candidate_plan["id"]))

    assert store.get_effective_assigned_plan_for_device(int(candidate["id"])) is None
    with store.connect() as conn:
        events = conn.execute(
            """
            SELECT event_type, guard_mode, decision, band, frequency
              FROM rf_guard_events
             WHERE event_type='advanced_frequency_guard'
            """
        ).fetchall()
    assert [(row[0], row[1], row[2], row[3], row[4]) for row in events] == [
        ("advanced_frequency_guard", "block", "blocked", "40M", "7079500")
    ]


def test_schedule_assignment_allows_advanced_guard_when_schedule_has_times_without_frequency(
    monkeypatch, tmp_path
) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Left Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    right = store.save_device_profile(
        {
            "name": "Right Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    left_plan = store.save_frequency_plan(
        {"name": "Timed 40M Left", "schedule_refs": ["day=MON band=40M start=14:00 end=15:00"]}
    )
    right_plan = store.save_frequency_plan(
        {"name": "Timed 40M Right", "schedule_refs": ["day=MON band=40M start=14:30 end=15:30"]}
    )

    store.set_assigned_plan(int(left["id"]), int(left_plan["id"]))
    assigned = store.set_assigned_plan(int(right["id"]), int(right_plan["id"]))
    validation = json.loads(str(assigned["validation_status_json"]))

    assert validation["state"] == "ok"
    with store.connect() as conn:
        events = conn.execute("SELECT event_type FROM rf_guard_events").fetchall()
    assert [row[0] for row in events] == []


def test_schedule_assignment_allows_advanced_close_frequency_guard_when_times_do_not_overlap(
    monkeypatch, tmp_path
) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Morning Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    right = store.save_device_profile(
        {
            "name": "Afternoon Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    morning_plan = store.save_frequency_plan(
        {
            "name": "Morning Close Frequency",
            "frequency_refs": ["40M:7.078"],
            "schedule_refs": ["day=MON band=40M start=14:00 end=15:00"],
        }
    )
    afternoon_plan = store.save_frequency_plan(
        {
            "name": "Afternoon Close Frequency",
            "frequency_refs": ["40M:7.079"],
            "schedule_refs": ["day=MON band=40M start=16:00 end=17:00"],
        }
    )

    store.set_assigned_plan(int(left["id"]), int(morning_plan["id"]))
    assigned = store.set_assigned_plan(int(right["id"]), int(afternoon_plan["id"]))
    validation = json.loads(str(assigned["validation_status_json"]))

    assert validation["state"] == "ok"
    assert validation["blocked"] == []


def test_schedule_assignment_blocks_advanced_close_frequency_guard_when_times_overlap(
    monkeypatch, tmp_path
) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Morning Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "block",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    right = store.save_device_profile(
        {
            "name": "Second Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "advanced_frequency_guard_group": "RX Frontend",
            "advanced_frequency_guard_mode": "warn",
            "advanced_frequency_guard_window_hz": 3000,
        }
    )
    first_plan = store.save_frequency_plan(
        {
            "name": "First Close Frequency",
            "frequency_refs": ["40M:7.078"],
            "schedule_refs": ["day=MON band=40M start=14:00 end=15:00"],
        }
    )
    second_plan = store.save_frequency_plan(
        {
            "name": "Second Close Frequency",
            "frequency_refs": ["40M:7.079"],
            "schedule_refs": ["day=MON band=40M start=14:30 end=15:30"],
        }
    )

    store.set_assigned_plan(int(left["id"]), int(first_plan["id"]))
    with pytest.raises(ValueError, match="Advanced Guard group RX FRONTEND"):
        store.set_assigned_plan(int(right["id"]), int(second_plan["id"]))


def test_schedule_assignment_allows_non_overlapping_same_band_schedule_windows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    morning = store.save_device_profile(
        {
            "name": "Morning Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    afternoon = store.save_device_profile(
        {
            "name": "Afternoon Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    morning_plan = store.save_frequency_plan(
        {"name": "Morning 40M", "schedule_refs": ["day=MON band=40M start=14:00 end=15:00"]}
    )
    afternoon_plan = store.save_frequency_plan(
        {"name": "Afternoon 40M", "schedule_refs": ["day=MON band=40M start=16:00 end=17:00"]}
    )

    store.set_assigned_plan(int(morning["id"]), int(morning_plan["id"]))
    assigned = store.set_assigned_plan(int(afternoon["id"]), int(afternoon_plan["id"]))
    validation = json.loads(str(assigned["validation_status_json"]))

    assert validation["state"] == "ok"
    assert validation["blocked"] == []
    assert validation["warnings"] == []


def test_schedule_assignment_blocks_overlapping_same_band_schedule_windows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Left Window Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    right = store.save_device_profile(
        {
            "name": "Right Window Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    left_plan = store.save_frequency_plan(
        {"name": "Left 40M Window", "schedule_refs": ["day=MON band=40M start=14:00 end=15:30"]}
    )
    right_plan = store.save_frequency_plan(
        {"name": "Right 40M Window", "schedule_refs": ["day=MON band=40M start=15:00 end=16:00"]}
    )

    store.set_assigned_plan(int(left["id"]), int(left_plan["id"]))
    with pytest.raises(ValueError, match="would both be assigned on 40M"):
        store.set_assigned_plan(int(right["id"]), int(right_plan["id"]))

    assert store.get_effective_assigned_plan_for_device(int(right["id"])) is None


def test_schedule_assignment_keeps_conservative_guard_for_mixed_window_and_broad_refs(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Mixed Left Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    right = store.save_device_profile(
        {
            "name": "Mixed Right Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    left_plan = store.save_frequency_plan(
        {
            "name": "Mixed Left",
            "schedule_refs": ["day=MON band=20M start=14:00 end=15:00"],
            "frequency_refs": ["40M:7.078"],
        }
    )
    right_plan = store.save_frequency_plan(
        {
            "name": "Mixed Right",
            "schedule_refs": ["day=MON band=80M start=14:00 end=15:00"],
            "frequency_refs": ["40M:7.110"],
        }
    )

    store.set_assigned_plan(int(left["id"]), int(left_plan["id"]))
    with pytest.raises(ValueError, match="would both be assigned on 40M"):
        store.set_assigned_plan(int(right["id"]), int(right_plan["id"]))

    assert store.get_effective_assigned_plan_for_device(int(right["id"])) is None


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

    with pytest.raises(ValueError, match="receive-only operating models"):
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

    with pytest.raises(ValueError, match="keep the model receive-only"):
        store.save_operating_profile({"id": int(rx_plan["id"]), "name": "Observer Watch Plan", "receive_only": 0})

    saved = store.get_operating_profile(int(rx_plan["id"]))
    assert saved is not None
    assert int(saved["receive_only"]) == 1


def test_multi_radio_store_blocks_observer_class_edit_with_transmit_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    _create_primary_radio(store)
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

    with pytest.raises(ValueError, match="receive-only operating models"):
        store.save_device_profile({"id": int(radio["id"]), "name": "Secondary TX", "device_class": "observer"})

    saved = store.get_device_profile(int(radio["id"]))
    assert saved is not None
    assert saved["device_class"] == "tx_rx"


def test_multi_radio_store_does_not_auto_assign_default_plan_to_unassigned_observer(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    _create_primary_radio(store)
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

    with pytest.raises(ValueError, match="receive-only operating models"):
        store.set_device_profile_runtime_active(int(observer_radio["id"]), True)

    assert store.get_effective_assignment_for_device(int(observer_radio["id"])) is None
    refreshed = store.get_device_profile(int(observer_radio["id"]))
    assert refreshed is not None
    assert int(refreshed["runtime_active"]) == 0


def test_multi_radio_store_rejects_blank_slate_active_observer_without_persisting(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    with pytest.raises(ValueError, match="compatibility runtime device"):
        store.save_device_profile(
            {
                "name": "First SDR",
                "enabled": 1,
                "runtime_active": 1,
                "runtime_primary": 1,
                "device_class": "observer",
                "control_backend": "manual",
            }
        )

    assert store.list_device_profiles() == []
