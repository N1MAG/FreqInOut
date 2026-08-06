import os
import sys
import types
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QWidget
import pytest

from freqinout.core.launch_orchestrator import LaunchOrchestrator
from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.shared_state import ActionFeedbackService
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import DeviceRuntime, StationRuntimeManager
from freqinout.gui import controlfreq_tab as controlfreq_mod
from freqinout.gui import qsy_helper
from freqinout.gui.controlfreq_tab import ControlFreqTab
from freqinout.gui.qsy_helper import scheduler_enabled, set_scheduler_enabled_override


def _select_assignment_devices(tab, device_ids: list[int]) -> None:
    wanted = {int(device_id) for device_id in device_ids}
    found: set[int] = set()
    for row in range(tab.device_assignments_table.rowCount()):
        widget = tab.device_assignments_table.cellWidget(row, 0)
        chk = widget.findChild(QCheckBox) if widget is not None else None
        if chk is None:
            continue
        device_id = int(chk.property("device_profile_id") or 0)
        chk.setChecked(device_id in wanted)
        if device_id in wanted:
            found.add(device_id)
    assert found == wanted


def test_store_supports_operating_profile_crud_and_effective_assignment_overrides(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    field_ops = store.save_operating_profile(
        {
            "name": "Field Ops",
            "description": "Restrictive field shell",
            "scheduler_enabled": False,
            "use_map": False,
            "use_messages": False,
            "use_background_ingest": False,
            "use_launch_control": False,
            "use_net_control_tabs": False,
        }
    )

    before = store.get_effective_assignment_for_device(int(primary["id"]))
    assert before is not None
    assert before["assignment_state"] == "active"

    override = store.set_device_operating_profile(
        int(primary["id"]),
        int(field_ops["id"]),
        assignment_state="temporary_override",
        reason="Field deployment",
    )

    assert int(override["operating_profile_id"]) == int(field_ops["id"])
    assert override["assignment_state"] == "temporary_override"

    history = store.list_assignments()
    assert any(
        int(row.get("device_profile_id", 0) or 0) == int(primary["id"])
        and str(row.get("assignment_state", "") or "").strip().lower() == "superseded"
        for row in history
    )

    with pytest.raises(ValueError):
        store.save_operating_profile({"id": int(field_ops["id"]), "enabled": False})

    restored = store.restore_default_operating_profile(int(primary["id"]))
    default_profile = next(row for row in store.list_operating_profiles() if row["system_key"] == "default_operating")

    assert restored["assignment_state"] == "active"
    assert int(restored["operating_profile_id"]) == int(default_profile["id"])


def test_settings_manager_reinit_preserves_effective_operating_assignment(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    field_ops = store.save_operating_profile(
        {
            "name": "Restart Persistence",
            "scheduler_enabled": False,
            "use_map": False,
            "use_messages": False,
            "use_background_ingest": False,
            "use_launch_control": False,
            "use_net_control_tabs": False,
        }
    )
    assigned = store.set_device_operating_profile(
        int(primary["id"]),
        int(field_ops["id"]),
        assignment_state="temporary_override",
        reason="Persist across settings reload",
    )
    assert int(assigned["operating_profile_id"]) == int(field_ops["id"])

    SettingsManager()

    reloaded_store = MultiRadioStore(settings_db_path())
    effective = reloaded_store.get_effective_assignment_for_device(int(primary["id"]))

    assert effective is not None
    assert int(effective["operating_profile_id"]) == int(field_ops["id"])
    assert str(effective["reason"] or "") == "Persist across settings reload"
    assert str(effective["assignment_state"] or "") == "temporary_override"


def test_station_runtime_manager_primary_snapshot_includes_operating_policy(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    field_ops = store.save_operating_profile(
        {
            "name": "Field Policy",
            "scheduler_enabled": False,
            "use_map": False,
            "use_messages": False,
            "use_background_ingest": False,
            "use_launch_control": False,
            "use_net_control_tabs": False,
        }
    )
    store.set_device_operating_profile(
        int(primary["id"]),
        int(field_ops["id"]),
        assignment_state="temporary_override",
        reason="Primary field test",
    )

    monkeypatch.setattr(
        SoftwareStatusService,
        "status_snapshot",
        lambda self, **kwargs: {
            "JS8Call_API": {"state": "idle", "tooltip": "JS8 idle"},
            "JS8Call": {"state": "idle", "tooltip": "JS8 idle"},
            "FLRig": {"state": "ok", "tooltip": "FLRig reachable"},
            "RigCtlD": {"state": "idle", "tooltip": "RigCtlD idle"},
            "FLDigi": {"state": "idle", "tooltip": "FLDigi idle"},
            "FLMsg": {"state": "idle", "tooltip": "FLMsg idle"},
            "FLAmp": {"state": "idle", "tooltip": "FLAmp idle"},
            "VarAC": {"state": "idle", "tooltip": "VarAC idle"},
            "JS8Spotter": {"state": "idle", "tooltip": "JS8Spotter idle"},
            "CommStat": {"state": "idle", "tooltip": "CommStat idle"},
        },
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    primary_snapshot = next(snap for snap in manager.get_runtime_snapshots() if snap.runtime_primary)
    policy = manager.primary_runtime_policy()

    assert primary_snapshot.assignment_state == "temporary_override"
    assert primary_snapshot.scheduler_enabled is False
    assert primary_snapshot.use_map is False
    assert primary_snapshot.use_messages is False
    assert primary_snapshot.use_background_ingest is False
    assert primary_snapshot.use_launch_control is False
    assert primary_snapshot.use_net_control_tabs is False
    assert policy["operating_profile_name"] == "Field Policy"
    assert policy["assignment_state"] == "temporary_override"


def test_device_runtime_snapshot_defaults_launch_policy_off_when_missing() -> None:
    runtime = DeviceRuntime(
        {
            "id": 7,
            "name": "Manual Radio",
            "control_backend": "manual",
            "runtime_active": 1,
        },
        is_primary=True,
        assignment={"assignment_state": "active"},
        operating_profile={
            "name": "Missing Launch Policy",
            "scheduler_enabled": True,
            "use_messages": True,
            "use_map": True,
            "use_background_ingest": True,
            "use_net_control_tabs": True,
        },
    )

    assert runtime.operating_policy()["use_launch_control"] is False
    assert runtime.snapshot().use_launch_control is False


def test_settings_tab_supports_operating_profiles_and_assignments(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        tab._persist_operating_profile(
            {
                "name": "Field Ops",
                "description": "Field shell",
                "scheduler_enabled": False,
                "use_map": False,
                "use_messages": False,
                "use_background_ingest": False,
                "use_launch_control": False,
                "use_net_control_tabs": False,
            }
        )

        field_ops = next(row for row in store.list_operating_profiles() if row["name"] == "Field Ops")
        assert any(row["name"] == "Field Ops" for row in tab.operating_profiles)
        assert tab.device_assignments_table.rowCount() >= 1

        primary = store.get_runtime_primary_device_profile()
        assert primary is not None
        _select_assignment_devices(tab, [int(primary["id"])])

        monkeypatch.setattr(
            tab,
            "_open_assignment_dialog",
            lambda selected: {
                "operating_profile_id": int(field_ops["id"]),
                "assignment_state": "temporary_override",
                "reason": "Field assignment",
                "ends_utc": "",
            },
        )
        tab._assign_operating_profile_to_selected_devices()

        effective = store.get_effective_assignment_for_device(int(primary["id"]))
        assert effective is not None
        assert int(effective["operating_profile_id"]) == int(field_ops["id"])
        assert effective["assignment_state"] == "temporary_override"

        _select_assignment_devices(tab, [int(primary["id"])])
        tab._restore_default_operating_profile_for_selected_devices()

        restored = store.get_effective_assignment_for_device(int(primary["id"]))
        assert restored is not None
        assert restored["assignment_state"] == "active"
        assert int(restored["operating_profile_id"]) != int(field_ops["id"])
    finally:
        tab.deleteLater()
        app.processEvents()


def test_launch_orchestrator_runtime_launch_override_blocks_sequences(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    orchestrator = LaunchOrchestrator(settings)
    orchestrator.set_runtime_launch_enabled(False, reason="Blocked by primary policy.")

    assert orchestrator.launch_allowed() is False
    assert orchestrator.launch_block_reason() == "Blocked by primary policy."
    assert orchestrator.start_startup_sequence() is False
    assert orchestrator.start_manual_sequence([]) is False


def test_scheduler_engine_runtime_scheduler_override_updates_status_and_helper(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    settings.set("use_scheduler", True)

    engine = SchedulerEngine()
    try:
        engine.set_runtime_scheduler_enabled(False)
        set_scheduler_enabled_override(False)
        status = engine.get_status_summary()
        assert status["use_scheduler"] is False
        assert scheduler_enabled(settings) is False
    finally:
        set_scheduler_enabled_override(None)


def test_scheduler_engine_runtime_timer_policy_override(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    engine = SchedulerEngine()
    engine.settings.set("freq_enforcement_mode", "On Schedule Change")
    engine.settings.set("freq_prompt_interval", "Hourly")

    engine.set_runtime_timer_policy(
        {
            "freq_enforcement_mode": "Prompt",
            "freq_prompt_interval": "Every 10 minutes",
            "fldigi_enforcement_mode": "Nope",
            "js8_prompt_interval": "Select Interval",
        }
    )

    assert engine._enforcement_mode("freq_enforcement_mode") == "Prompt"
    assert engine._prompt_interval_minutes("freq_prompt_interval") == 10
    assert engine._enforcement_mode("fldigi_enforcement_mode") == "On Schedule Change"
    assert engine._prompt_interval_minutes("js8_prompt_interval") == 60

    engine.set_runtime_timer_policy(None)
    assert engine._enforcement_mode("freq_enforcement_mode") == "On Schedule Change"
    assert engine._prompt_interval_minutes("freq_prompt_interval") == 60


def test_hold_duration_helpers_prefer_runtime_profile_default() -> None:
    QApplication.instance() or QApplication([])

    class _Settings:
        def get(self, key, default=None):
            return 60 if key == "schedule_hold_minutes_default" else default

    settings = _Settings()
    assert qsy_helper.get_hold_duration_default(settings) == 60
    assert qsy_helper.get_hold_duration_default(settings, {"schedule_hold_minutes_default": 90}) == 90
    assert qsy_helper.get_hold_duration_default(settings, {"schedule_hold_minutes_default": 45}) == 30

    combo = QComboBox()
    combo.addItem("No data")
    assert qsy_helper.selected_hold_duration(combo, settings, {"schedule_hold_minutes_default": 120}) == 120
    qsy_helper.refresh_hold_duration_combo(combo, settings, {"schedule_hold_minutes_default": 90})
    assert combo.currentData() == 90


def test_runtime_hold_duration_uses_active_radio_profile() -> None:
    main_source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    control_source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    helper_source = Path("freqinout/gui/qsy_helper.py").read_text(encoding="utf-8")

    assert "from freqinout.core.multi_radio_store import DEFAULT_HOLD_DURATION_MINUTES, SUPPORTED_HOLD_DURATION_MINUTES" in helper_source
    assert "HOLD_DURATION_PRESETS: tuple[int, ...] = tuple(sorted(SUPPORTED_HOLD_DURATION_MINUTES))" in helper_source
    assert "def get_hold_duration_default(settings, profile" in helper_source
    assert 'profile.get("schedule_hold_minutes_default")' in helper_source
    assert "def hold_duration_profile_for_window" in helper_source
    assert "profile=getattr(self, \"_active_runtime_profile\", None)" in main_source
    assert "getattr(self, \"_active_runtime_profile\", None)" in main_source[
        main_source.index("def _selected_sidebar_hold_minutes")
        : main_source.index("def _on_sidebar_hold_duration_changed")
    ]
    assert 'profile.get("schedule_hold_minutes_default"' in main_source[
        main_source.index("def _runtime_state_signature_for")
        : main_source.index("def _runtime_timer_policy_for")
    ]
    assert "self._sync_hold_duration_combos()" in main_source[
        main_source.index("def _apply_runtime_profile_state")
        : main_source.index("set_scheduler_enabled_override", main_source.index("def _apply_runtime_profile_state"))
    ]
    assert "def _runtime_hold_duration_profile" in control_source
    assert "selected_hold_duration(self.hold_duration_combo, self.settings, self._runtime_hold_duration_profile())" in control_source
    assert "refresh_hold_duration_combo(self.hold_duration_combo, self.settings, self._runtime_hold_duration_profile())" in control_source


def test_main_window_runtime_timer_policy_push_failures_are_logged() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "failed to apply startup runtime timer policy" in source
    assert "failed to apply runtime timer policy" in source
    assert "self.scheduler.set_runtime_timer_policy(self._runtime_timer_policy_for(self._active_runtime_profile))" in source
    assert "self.scheduler.set_runtime_timer_policy(self._runtime_timer_policy_for(profile))" in source


class FakeStore:
    active_profile: dict[str, object] = {}

    def get_runtime_primary_device_profile(self):
        return dict(type(self).active_profile)


class StubLaunchOrchestrator(QObject):
    sequence_started = Signal(object)
    sequence_progress = Signal(object)
    sequence_finished = Signal(object)

    def __init__(self) -> None:
        super().__init__()
        self.start_calls = 0
        self.stop_calls = 0
        self._allowed = True
        self._reason = ""

    def start_startup_sequence(self) -> bool:
        if not self._allowed:
            return False
        self.start_calls += 1
        return True

    def stop_sequence(self) -> None:
        self.stop_calls += 1

    def set_runtime_launch_enabled(self, enabled: bool | None, *, reason: str = "") -> None:
        self._allowed = True if enabled is None else bool(enabled)
        self._reason = str(reason or "").strip()

    def launch_allowed(self) -> bool:
        return self._allowed

    def launch_block_reason(self) -> str:
        return self._reason

    def is_active(self) -> bool:
        return False


class StubTab(QWidget):
    schedule_saved = Signal()
    net_status_changed = Signal(str, bool)
    operator_history_updated = Signal()
    local_operator_updated = Signal()
    local_data_updated = Signal()
    sop_data_changed = Signal()

    def __init__(self, parent=None, **_kwargs) -> None:
        super().__init__(parent)

    def on_settings_saved(self) -> None:
        return

    def set_tab_active(self, active: bool) -> None:
        self._tab_active = bool(active)

    def on_tab_activated(self) -> None:
        return

    def apply_theme(self) -> None:
        return

    def _load_data(self) -> None:
        return

    def reload_operator_lookup(self) -> None:
        return

    def on_local_net_profiles_updated(self) -> None:
        return

    def on_hf_schedule_saved(self) -> None:
        return

    def on_sop_data_changed(self) -> None:
        return

    def on_schedule_resumed(self) -> None:
        return

    def mark_schedule_dirty(self) -> None:
        return

    def rebuild_table(self) -> None:
        return

    def set_map_visible(self, visible: bool) -> None:
        self._map_visible = bool(visible)

    def show_loading_toast(self) -> None:
        return

    def set_runtime_manager(self, manager) -> None:
        self._runtime_manager = manager

    def refresh_from_manager(self, force: bool = False) -> None:
        return

    def _start_js8_rx_listener(self) -> None:
        return


class FakeQsyCombo:
    def __init__(self, data=None) -> None:
        self._data = data

    def currentData(self):
        return self._data


class FakeActionButton:
    def __init__(self) -> None:
        self.text_value = ""
        self.tooltip_value = ""
        self.enabled_value = False
        self.style_value = ""

    def setText(self, value: str) -> None:
        self.text_value = value

    def setToolTip(self, value: str) -> None:
        self.tooltip_value = value

    def setEnabled(self, value: bool) -> None:
        self.enabled_value = bool(value)

    def setStyleSheet(self, value: str) -> None:
        self.style_value = value


class StubSettingsTab(StubTab):
    settings_saved = Signal()
    device_profiles_changed = Signal()
    local_net_profiles_changed = Signal()
    open_logs_requested = Signal()
    log_level_changed = Signal(str)

    def __init__(self, parent=None, **_kwargs) -> None:
        super().__init__(parent)
        self.launch_orchestrator = StubLaunchOrchestrator()

    def _update_launch_control_buttons(self) -> None:
        return


class StubScheduler(QObject):
    active_entry_changed = Signal(dict, str)
    next_change_updated = Signal(object)
    off_schedule_detected = Signal(dict)
    off_schedule_cleared = Signal()
    varac_wait_detected = Signal(dict)
    varac_wait_cleared = Signal()

    def __init__(
        self,
        parent=None,
        rig=None,
        js8=None,
        varac=None,
        fldigi_log=None,
        station_runtime_manager=None,
        poll_interval_ms=5000,
    ):
        super().__init__(parent)
        self.rig = rig
        self.js8 = js8
        self.varac = varac
        self.fldigi_log = fldigi_log
        self.station_runtime_manager = station_runtime_manager
        self.runtime_enabled = True
        self.runtime_timer_policy = {}
        self.current_schedule_entry = {}

    def start(self) -> None:
        return

    def stop(self) -> None:
        return

    def force_refresh(self) -> None:
        return

    def set_runtime_scheduler_enabled(self, enabled: bool | None) -> None:
        self.runtime_enabled = True if enabled is None else bool(enabled)

    def set_runtime_timer_policy(self, policy) -> None:
        self.runtime_timer_policy = dict(policy or {})

    def get_status_summary(self) -> dict[str, object]:
        return {
            "control_mode": "MANUAL",
            "use_scheduler": self.runtime_enabled,
            "freq_label": "--",
            "off_schedule": False,
            "varac_waiting": False,
            "ptt_active": False,
            "js8_busy": False,
            "fldigi_busy": False,
            "fldigi_busy_reason": "",
            "varac_busy": False,
            "net_kind": "",
            "off_schedule_flags": {},
            "fldigi_mode_off": False,
            "fldigi_offset_off": False,
            "sop_contention": False,
            "sop_contention_profiles": [],
            "sop_selected_profile": "",
            "source": "NONE",
        }


class StubBackgroundIngest(QObject):
    def __init__(self, settings) -> None:
        super().__init__()
        self._running = False

    def start(self, *, initial_stagger: bool = True) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    def is_running(self) -> bool:
        return self._running

    def refresh_runtime_settings(self) -> None:
        return


class StubJS8ControlClient:
    def __init__(self, host=None, port=None, settings=None) -> None:
        self.host = host
        self.port = port
        self.settings = settings

    def stop(self) -> None:
        return


class StubStationRuntimeManager:
    active_profile: dict[str, object] = {}
    active_policy: dict[str, object] = {}

    def __init__(self, store=None, settings=None) -> None:
        self.store = store
        self.settings = settings

    def sync_with_store(self) -> None:
        return

    def get_primary_runtime(self):
        return None

    def get_runtime_primary_device_profile(self):
        return dict(type(self).active_profile)

    def primary_runtime_signature(self):
        profile = dict(type(self).active_profile)
        return (
            int(profile.get("id", 0) or 0),
            str(profile.get("name", "") or "").strip(),
            str(profile.get("control_backend", "") or "").strip().lower(),
            str(profile.get("deployment_mode", "") or "").strip().lower(),
        )

    def primary_runtime_policy(self):
        return dict(type(self).active_policy)

    def get_runtime_snapshots(self, *, force: bool = False):
        return []

    def stop(self) -> None:
        return


def _nav_button(window, text: str):
    for btn in window.nav_buttons:
        if btn.text() == text:
            return btn
    raise AssertionError(f"nav button not found: {text}")


def _make_window(monkeypatch, tmp_path, active_profile, active_policy):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    def _stub_module(name: str, **attrs):
        module = types.ModuleType(name)
        for key, value in attrs.items():
            setattr(module, key, value)
        monkeypatch.setitem(sys.modules, name, module)

    _stub_module("freqinout.gui.settings_tab", SettingsTab=StubSettingsTab)
    _stub_module("freqinout.gui.daily_schedule_tab", DailyScheduleTab=StubTab)
    _stub_module("freqinout.gui.net_schedule_tab", NetScheduleTab=StubTab)
    _stub_module("freqinout.gui.fldigi_net_control_tab", FldigiNetControlTab=StubTab)
    _stub_module("freqinout.gui.js8call_net_control_tab", JS8CallNetControlTab=StubTab)
    _stub_module("freqinout.gui.freq_planner_tab", FreqPlannerTab=StubTab)
    _stub_module("freqinout.gui.sop_tab", SOPTab=StubTab)
    _stub_module("freqinout.gui.operator_history_tab", OperatorHistoryTab=StubTab)
    _stub_module("freqinout.gui.local_operator_tab", LocalOperatorTab=StubTab)
    _stub_module("freqinout.gui.local_ncs_tab", LocalNCSTab=StubTab)
    _stub_module("freqinout.gui.log_viewer", LogViewerTab=StubTab)
    _stub_module(
        "freqinout.gui.stations_map_tab",
        StationsMapTab=StubTab,
        FEMA_REGIONS=[],
        LOWER48_STATES=[],
        STATE_CENTERS={},
        JS8LogLinkIndexer=object,
    )
    _stub_module("freqinout.gui.message_viewer_tab", MessageViewerTab=StubTab)
    _stub_module("freqinout.gui.peer_sched_tab", PeerSchedTab=StubTab)
    _stub_module("freqinout.gui.help_tab", HelpTab=StubTab)
    _stub_module("freqinout.gui.controlfreq_tab", ControlFreqTab=StubTab)
    _stub_module("freqinout.gui.station_overview_tab", StationOverviewTab=StubTab)
    sys.modules.pop("freqinout.gui.main_window", None)

    import freqinout.gui.main_window as main_window_mod

    FakeStore.active_profile = dict(active_profile)
    StubStationRuntimeManager.active_profile = dict(active_profile)
    StubStationRuntimeManager.active_policy = dict(active_policy)
    monkeypatch.setattr(main_window_mod, "MultiRadioStore", FakeStore)
    monkeypatch.setattr(main_window_mod, "StationRuntimeManager", StubStationRuntimeManager)
    monkeypatch.setattr(main_window_mod, "SchedulerEngine", StubScheduler)
    monkeypatch.setattr(main_window_mod, "BackgroundIngestController", StubBackgroundIngest)
    monkeypatch.setattr(main_window_mod, "JS8ControlClient", StubJS8ControlClient)
    monkeypatch.setattr(main_window_mod, "VarACStatusClient", lambda settings=None: object())
    monkeypatch.setattr(main_window_mod, "FldigiLogStatusClient", lambda: object())
    monkeypatch.setattr(main_window_mod, "rig_control_client_from_settings", lambda settings: object())
    monkeypatch.setattr(main_window_mod.MainWindow, "_apply_app_theme", lambda self: None)
    monkeypatch.setattr(main_window_mod.MainWindow, "_sync_map_filters_from_tab", lambda self: None)
    monkeypatch.setattr(main_window_mod.MainWindow, "_refresh_scheduler_status_panel", lambda self, *args: None)
    monkeypatch.setattr(main_window_mod.MainWindow, "_refresh_condition_level_panel", lambda self, *args: None)
    monkeypatch.setattr(main_window_mod.MainWindow, "_update_log_indicator", lambda self, *args: None)
    monkeypatch.setattr(main_window_mod.MainWindow, "_apply_callsign_to_tab_titles", lambda self: None)
    monkeypatch.setattr(main_window_mod.MainWindow, "on_hold_state_changed", lambda self, force_reload=False: None)
    monkeypatch.setattr(main_window_mod.MainWindow, "_prewarm_webengine", lambda self: None)
    monkeypatch.setattr(main_window_mod.MainWindow, "_start_lazy_prewarm", lambda self: None)

    window = main_window_mod.MainWindow()
    return app, window


def test_main_window_applies_primary_operating_profile_policy(monkeypatch, tmp_path):
    app, window = _make_window(
        monkeypatch,
        tmp_path,
        {
            "id": 1,
            "name": "Base Station",
            "control_backend": "flrig",
            "deployment_mode": "full",
            "freq_enforcement_mode": "Prompt",
            "freq_prompt_interval": "Every 10 minutes",
            "fldigi_enforcement_mode": "On Schedule Change",
            "fldigi_prompt_interval": "Hourly",
            "js8_enforcement_mode": "Prompt",
            "js8_prompt_interval": "Every 5 minutes",
        },
        {
            "operating_profile_name": "Field Policy",
            "assignment_state": "temporary_override",
            "scheduler_enabled": False,
            "scheduler_mode": "full",
            "use_messages": False,
            "use_map": False,
            "use_background_ingest": False,
            "use_launch_control": False,
            "use_net_control_tabs": False,
        },
    )

    try:
        map_idx = window._screen_index_by_label["Map"]
        assert window._suppressed_screen_labels == {"Map", "Messages", "NCS-FLDigi/SSB", "NCS-JS8", "NCS-Local"}
        assert _nav_button(window, "Map").isHidden() is True
        assert _nav_button(window, "Messages").isHidden() is True
        assert _nav_button(window, "FLDigi / SSB").isHidden() is True
        assert _nav_button(window, "JS8Call").isHidden() is True
        assert _nav_button(window, "VHF/UHF").isHidden() is True
        assert window.background_ingest.is_running() is False
        assert window.scheduler.runtime_enabled is False
        assert window.scheduler.runtime_timer_policy == {
            "freq_enforcement_mode": "Prompt",
            "freq_prompt_interval": "Every 10 minutes",
            "fldigi_enforcement_mode": "On Schedule Change",
            "fldigi_prompt_interval": "Hourly",
            "js8_enforcement_mode": "Prompt",
            "js8_prompt_interval": "Every 5 minutes",
        }
        assert "Field Policy" in window.runtime_mode_label.text()

        current_before = window.stack.currentIndex()
        window._set_screen(map_idx)
        assert window.stack.currentIndex() == current_before

        window._start_launch_control_startup()
        assert window.launch_orchestrator.start_calls == 0
    finally:
        window._on_app_about_to_quit()
        window.deleteLater()
        app.processEvents()


def test_controlfreq_active_hold_allows_new_qsy_without_resume(monkeypatch):
    monkeypatch.setattr(controlfreq_mod, "button_style", lambda role, theme: role)
    fake = types.SimpleNamespace(
        settings=types.SimpleNamespace(),
        freq_combo=FakeQsyCombo({"freq": 7.268, "band": "40M", "mode": "LSB"}),
        freq_action_btn=FakeActionButton(),
        _hold_state_snapshot={"active": True, "remaining_sec": 1800},
        _freq_action_busy_reason_label=None,
    )
    fake._theme = lambda: {"text": "#000"}
    fake.window = lambda: types.SimpleNamespace(scheduler=None)
    fake._get_active_frequency_mhz = lambda: 7.078
    fake._selected_hold_minutes = lambda: 30
    fake._apply_frequency_action_busy_override = lambda reason: None
    fake._selected_qsy_pending = lambda active: ControlFreqTab._selected_qsy_pending(fake, active)

    ControlFreqTab._update_frequency_action_styles(fake, scheduled=7.078, active=7.078)

    assert fake._primary_freq_action_mode == "qsy"
    assert fake.freq_action_btn.text_value == "QSY + Hold"
    assert fake.freq_action_btn.enabled_value is True
    assert fake.freq_action_btn.style_value == "warning"


def test_controlfreq_schedule_row_qsy_uses_hold_path_without_resume(monkeypatch):
    calls = []
    feedback_calls = []
    refresh_calls = []

    def fake_qsy_with_hold(window, settings, meta, minutes):
        calls.append((window, settings, meta, minutes))
        return 60

    monkeypatch.setattr(controlfreq_mod, "perform_qsy_with_hold", fake_qsy_with_hold)
    monkeypatch.setattr(controlfreq_mod.QTimer, "singleShot", lambda _ms, callback: callback())

    fake_window = types.SimpleNamespace()
    fake_settings = types.SimpleNamespace()
    fake = types.SimpleNamespace(
        settings=fake_settings,
        _force_hero_resync=False,
        _hold_state_snapshot={"active": True, "remaining_sec": 1800},
    )
    fake.window = lambda: fake_window
    fake._selected_hold_minutes = lambda: 60
    fake._schedule_qsy_meta = lambda entry: {"freq": 7.268, "band": "40M", "mode": "LSB"}
    fake._refresh_frequency_control = lambda *args, **kwargs: refresh_calls.append((args, kwargs))
    fake._publish_qsy_action_feedback = lambda meta, minutes, source_surface: feedback_calls.append(
        (meta, minutes, source_surface)
    )

    ControlFreqTab._on_schedule_action_clicked(
        fake,
        {"action_kind": "qsy", "freq_mhz": 7.268, "group": "DX", "band": "40M"},
    )

    assert calls == [(fake_window, fake_settings, {"freq": 7.268, "band": "40M", "mode": "LSB"}, 60)]
    assert fake._force_hero_resync is True
    assert len(refresh_calls) == 2
    assert feedback_calls == [({"freq": 7.268, "band": "40M", "mode": "LSB"}, 60, "controlfreq_schedule")]


def test_controlfreq_qsy_success_publishes_radio_feedback_event() -> None:
    service = ActionFeedbackService()
    fake_window = types.SimpleNamespace(
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    fake = types.SimpleNamespace()
    fake.window = lambda: fake_window
    fake._qsy_feedback_target = lambda: ControlFreqTab._qsy_feedback_target(fake)
    fake._qsy_feedback_frequency_label = lambda meta: ControlFreqTab._qsy_feedback_frequency_label(meta)

    ControlFreqTab._publish_qsy_action_feedback(
        fake,
        {"freq": 7.268, "mode": "LSB"},
        60,
        source_surface="controlfreq",
    )

    events = service.recent(scope="radio")
    assert len(events) == 1
    assert events[0].action_type == "qsy"
    assert events[0].status == "succeeded"
    assert events[0].summary == "QSY sent to DX10: 7.268 LSB"
    assert events[0].detail == "Frequency changed and scheduling paused for 60 minutes."
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "controlfreq"


def test_controlfreq_qsy_success_uses_feedback_instead_of_success_popup() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    freq_block = source[source.index("def _on_freq_set_clicked") : source.index("def _on_freq_selection_changed")]
    row_block = source[source.index("def _on_schedule_action_clicked") : source.index("def _schedule_qsy_meta")]

    assert "_publish_qsy_action_feedback(meta, mins, source_surface=\"controlfreq\")" in freq_block
    assert "_publish_qsy_action_feedback(meta, mins, source_surface=\"controlfreq_schedule\")" in row_block
    assert '"QSY Applied"' not in freq_block
    assert '"QSY Applied"' not in row_block


def test_controlfreq_qsy_blocked_feedback_publishes_radio_event() -> None:
    service = ActionFeedbackService()
    fake_window = types.SimpleNamespace(
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    fake = types.SimpleNamespace()
    fake.window = lambda: fake_window
    fake._qsy_feedback_target = lambda: ControlFreqTab._qsy_feedback_target(fake)

    ControlFreqTab._publish_qsy_blocked_feedback(
        fake,
        "QSY blocked: select a frequency first.",
        "Choose a ControlFreq frequency before sending QSY.",
        source_surface="controlfreq",
    )

    events = service.recent(scope="radio")
    assert len(events) == 1
    assert events[0].action_type == "qsy"
    assert events[0].status == "blocked"
    assert events[0].summary == "QSY blocked: select a frequency first."
    assert events[0].detail == "Choose a ControlFreq frequency before sending QSY."
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "controlfreq"


def test_controlfreq_qsy_validation_uses_feedback_instead_of_local_popups() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    freq_block = source[source.index("def _on_freq_set_clicked") : source.index("def _on_freq_selection_changed")]
    row_block = source[source.index("def _on_schedule_action_clicked") : source.index("def _schedule_qsy_meta")]

    assert "_publish_qsy_blocked_feedback(" in freq_block
    assert "_publish_qsy_blocked_feedback(" in row_block
    assert 'QMessageBox.information(\n                self,\n                "Frequency Control"' not in freq_block
    assert 'QMessageBox.warning(self, "Frequency Control", "Select a frequency first.")' not in freq_block
    assert 'QMessageBox.warning(self, "Frequency Control", "No matching operating-group frequency is configured.")' not in row_block


def test_controlfreq_no_selected_frequency_publishes_blocked_feedback(monkeypatch) -> None:
    feedback_calls = []
    warning_calls = []
    fake = types.SimpleNamespace(
        settings=types.SimpleNamespace(get=lambda _key, default="": "FLRig"),
        freq_combo=FakeQsyCombo(None),
    )
    fake._publish_qsy_blocked_feedback = lambda summary, detail="", source_surface="": feedback_calls.append(
        (summary, detail, source_surface)
    )
    monkeypatch.setattr(controlfreq_mod.QMessageBox, "warning", lambda *args: warning_calls.append(args))

    ControlFreqTab._on_freq_set_clicked(fake)

    assert feedback_calls == [
        (
            "QSY blocked: select a frequency first.",
            "Choose a ControlFreq frequency before sending QSY.",
            "controlfreq",
        )
    ]
    assert warning_calls == []


def test_qsy_helper_shared_ptt_block_publishes_feedback_without_warning(monkeypatch) -> None:
    service = ActionFeedbackService()
    warning_calls = []

    class _Scheduler:
        def get_status_summary(self):
            return {
                "shared_ptt_blocked": True,
                "shared_ptt_reason": "Shared PTT group AMP-A is in use by Remote Rig.",
            }

        def apply_manual_qsy(self, entry):
            raise AssertionError("QSY should be blocked before apply_manual_qsy")

    window = types.SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper.QMessageBox, "warning", lambda *args: warning_calls.append(args))

    result = qsy_helper.perform_qsy(window, {"freq": 7.268, "band": "40M", "mode": "LSB"})

    events = service.recent(scope="radio")
    assert result is False
    assert warning_calls == []
    assert len(events) == 1
    assert events[0].status == "blocked"
    assert events[0].action_type == "qsy"
    assert events[0].summary == "QSY blocked: shared PTT path is busy."
    assert events[0].detail == "Shared PTT group AMP-A is in use by Remote Rig."
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "qsy_helper"


def test_qsy_helper_blocked_feedback_falls_back_to_warning_without_service(monkeypatch) -> None:
    warning_calls = []
    window = types.SimpleNamespace(scheduler=None)
    monkeypatch.setattr(qsy_helper.QMessageBox, "warning", lambda *args: warning_calls.append(args))

    result = qsy_helper.perform_qsy(window, {"freq": 7.268})

    assert result is False
    assert warning_calls
    assert warning_calls[0][1] == "Scheduler"


def test_qsy_helper_invalid_frequency_publishes_blocked_feedback(monkeypatch) -> None:
    service = ActionFeedbackService()
    warning_calls = []
    window = types.SimpleNamespace(
        scheduler=types.SimpleNamespace(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper.QMessageBox, "warning", lambda *args: warning_calls.append(args))

    result = qsy_helper.perform_qsy(window, {"freq": object()})

    events = service.recent(scope="radio")
    assert result is False
    assert warning_calls == []
    assert len(events) == 1
    assert events[0].status == "blocked"
    assert events[0].summary == "QSY blocked: selected frequency is invalid."
    assert "could not be used for QSY" in events[0].detail


def test_qsy_helper_rf_conflict_cancel_publishes_blocked_override_feedback(monkeypatch) -> None:
    service = ActionFeedbackService()

    class _Scheduler:
        def evaluate_coordination_conflict(self, entry, source="QSY"):
            return {
                "warning": True,
                "summary": "RF conflict: Remote Rig on same band.",
                "detail": "Target 7.268 MHz overlaps Remote Rig.",
            }

        def get_status_summary(self):
            return {"shared_ptt_blocked": False}

        def apply_manual_qsy(self, entry, ignore_coordination_prompt=False):
            raise AssertionError("QSY should not proceed after RF conflict cancel")

    class _FakeMessageBox:
        AcceptRole = 0
        RejectRole = 1

        def __init__(self, parent=None):
            self._cancel = None
            self._clicked = None

        def setWindowTitle(self, title):
            self.title = title

        def setText(self, text):
            self.text = text

        def setInformativeText(self, text):
            self.detail = text

        def addButton(self, label, role):
            button = object()
            if label == "Cancel":
                self._cancel = button
            return button

        def exec(self):
            self._clicked = self._cancel

        def clickedButton(self):
            return self._clicked

    window = types.SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _FakeMessageBox)

    result = qsy_helper.perform_qsy(window, {"freq": 7.268, "band": "40M", "mode": "LSB"})

    events = service.recent(scope="radio")
    assert result is False
    assert len(events) == 1
    assert events[0].action_type == "qsy_override"
    assert events[0].status == "blocked"
    assert events[0].summary == "QSY cancelled: RF Safety Guard warning."
    assert events[0].detail == "RF Safety Guard mode: Require confirmation. Target 7.268 MHz overlaps Remote Rig."
    assert events[0].source_surface == "qsy_helper_conflict"


def test_qsy_helper_rf_safety_block_stops_without_override_prompt(monkeypatch) -> None:
    service = ActionFeedbackService()

    class _Scheduler:
        def evaluate_coordination_conflict(self, entry, source="QSY"):
            return {
                "warning": True,
                "blocked": True,
                "summary": "RF Safety Guard: DX10 antenna is not configured for 40M.",
                "detail": "Antenna Supports These Bands: 20M. Target band: 40M.",
            }

        def get_status_summary(self):
            return {"shared_ptt_blocked": False}

        def apply_manual_qsy(self, entry, ignore_coordination_prompt=False):
            raise AssertionError("Blocked QSY should not proceed")

    class _FailIfShownMessageBox:
        def __init__(self, parent=None):
            raise AssertionError("Blocked QSY should not show an override prompt")

    window = types.SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _FailIfShownMessageBox)

    result = qsy_helper.perform_qsy(window, {"freq": 7.268, "band": "40M", "mode": "LSB"})

    events = service.recent(scope="radio")
    assert result is False
    assert len(events) == 1
    assert events[0].action_type == "qsy"
    assert events[0].status == "blocked"
    assert events[0].summary == "RF Safety Guard: DX10 antenna is not configured for 40M."
    assert events[0].detail == "Antenna Supports These Bands: 20M. Target band: 40M."


def test_qsy_helper_rf_safety_warn_only_continues_without_override_prompt(monkeypatch) -> None:
    service = ActionFeedbackService()

    class _Scheduler:
        def __init__(self) -> None:
            self.apply_calls = []

        def evaluate_coordination_conflict(self, entry, source="QSY"):
            return {
                "warning": True,
                "guard_mode": "warn",
                "summary": "RF Safety Guard: DX10 may overlap Remote Rig.",
                "detail": "Both radios are using Prevent Band Overlap group NORTH MAST.",
            }

        def get_status_summary(self):
            return {"shared_ptt_blocked": False}

        def apply_manual_qsy(self, entry, ignore_coordination_prompt=False):
            self.apply_calls.append(bool(ignore_coordination_prompt))

    class _FailIfShownMessageBox:
        def __init__(self, parent=None):
            raise AssertionError("Warn-only QSY should not show an override prompt")

    scheduler = _Scheduler()
    window = types.SimpleNamespace(
        scheduler=scheduler,
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _FailIfShownMessageBox)

    result = qsy_helper.perform_qsy(window, {"freq": 7.268, "band": "40M", "mode": "LSB"})

    events = service.recent(scope="radio")
    assert result is True
    assert scheduler.apply_calls == [True]
    assert len(events) == 1
    assert events[0].action_type == "qsy"
    assert events[0].status == "partial"
    assert events[0].summary == "RF Safety Guard: DX10 may overlap Remote Rig."
    assert events[0].source_surface == "qsy_helper_warning"


def test_resume_schedule_hold_rf_safety_block_keeps_hold_active(monkeypatch) -> None:
    qsy_helper._RESUME_GUARD_FEEDBACK_CACHE.clear()
    service = ActionFeedbackService()

    class _Settings:
        def set(self, key, value):
            raise AssertionError("Blocked resume should not clear suspend state")

    class _Scheduler:
        current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}

        def evaluate_coordination_conflict(self, entry, source="RESUME", force=False):
            return {
                "warning": True,
                "blocked": True,
                "guard_mode": "block",
                "summary": "RF Safety Guard: Protected Receiver blocks same-band overlap.",
                "detail": "Prevent Band Overlap group NORTH MAST is blocking this resume.",
            }

        def resume_schedule(self, **kwargs):
            raise AssertionError("Blocked resume should not call scheduler.resume_schedule")

    class _FailIfShownMessageBox:
        def __init__(self, parent=None):
            raise AssertionError("Blocked resume should not show an override prompt")

    window = types.SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _FailIfShownMessageBox)

    result = qsy_helper.resume_schedule_hold(window, _Settings())

    events = service.recent(scope="scheduler")
    assert result is False
    assert len(events) == 1
    assert events[0].action_type == "resume_schedule"
    assert events[0].status == "blocked"
    assert events[0].summary == "RF Safety Guard: Protected Receiver blocks same-band overlap."
    assert events[0].detail == "RF Safety Guard mode: Block. Prevent Band Overlap group NORTH MAST is blocking this resume."


def test_suspend_schedule_hold_warns_when_rf_conflict_remains(monkeypatch) -> None:
    qsy_helper._SUSPEND_GUARD_FEEDBACK_CACHE.clear()
    service = ActionFeedbackService()

    class _Settings:
        pass

    class _Scheduler:
        current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}

        def __init__(self) -> None:
            self.suspend_minutes = []
            self.evaluate_calls = []

        def evaluate_coordination_conflict(self, entry, source="SUSPEND", force=False):
            self.evaluate_calls.append((source, bool(force)))
            return {
                "warning": True,
                "blocked": True,
                "guard_mode": "block",
                "signature": "suspend|north-mast|40m",
                "summary": "RF Safety Guard: Protected Receiver blocks same-band overlap.",
                "detail": "Prevent Band Overlap group NORTH MAST is active.",
            }

        def suspend_schedule(self, minutes):
            self.suspend_minutes.append(minutes)

    scheduler = _Scheduler()
    window = types.SimpleNamespace(
        scheduler=scheduler,
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )

    result = qsy_helper.suspend_schedule_hold(window, _Settings(), 30)

    events = service.recent(scope="scheduler")
    assert result == 30
    assert scheduler.suspend_minutes == [30]
    assert scheduler.evaluate_calls == [("SUSPEND", True)]
    assert len(events) == 1
    assert events[0].action_type == "suspend_schedule"
    assert events[0].status == "partial"
    assert events[0].summary == "RF Safety Guard: Protected Receiver blocks same-band overlap."
    assert "condition remains in place" in events[0].detail


def test_suspend_schedule_hold_rf_warning_does_not_repeat_for_same_conflict(monkeypatch) -> None:
    qsy_helper._SUSPEND_GUARD_FEEDBACK_CACHE.clear()
    service = ActionFeedbackService()

    class _Settings:
        pass

    class _Scheduler:
        current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}

        def evaluate_coordination_conflict(self, entry, source="SUSPEND", force=False):
            return {
                "warning": True,
                "blocked": False,
                "guard_mode": "warn",
                "signature": "suspend|north-mast|40m",
                "summary": "RF Safety Guard: DX10 may overlap Remote Rig.",
                "detail": "Both radios are using Prevent Band Overlap group NORTH MAST.",
            }

        def suspend_schedule(self, minutes):
            pass

    window = types.SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )

    first = qsy_helper.suspend_schedule_hold(window, _Settings(), 30)
    second = qsy_helper.suspend_schedule_hold(window, _Settings(), 30)

    events = service.recent(scope="scheduler")
    assert first == 30
    assert second == 30
    assert len(events) == 1
    assert events[0].summary == "RF Safety Guard: DX10 may overlap Remote Rig."


def test_resume_schedule_hold_rf_safety_warn_only_resumes_without_prompt(monkeypatch) -> None:
    qsy_helper._RESUME_GUARD_FEEDBACK_CACHE.clear()
    service = ActionFeedbackService()

    class _Settings:
        pass

    class _Scheduler:
        current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}

        def __init__(self) -> None:
            self.resume_kwargs = []

        def evaluate_coordination_conflict(self, entry, source="RESUME", force=False):
            return {
                "warning": True,
                "blocked": False,
                "guard_mode": "warn",
                "summary": "RF Safety Guard: DX10 may overlap Remote Rig.",
                "detail": "Warn only should not block resume.",
            }

        def resume_schedule(self, **kwargs):
            self.resume_kwargs.append(dict(kwargs))

    class _FailIfShownMessageBox:
        def __init__(self, parent=None):
            raise AssertionError("Warn-only resume should not show an override prompt")

    scheduler = _Scheduler()
    window = types.SimpleNamespace(
        scheduler=scheduler,
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _FailIfShownMessageBox)

    result = qsy_helper.resume_schedule_hold(window, _Settings())

    events = service.recent(scope="scheduler")
    assert result is True
    assert scheduler.resume_kwargs == [{"ignore_coordination_prompt": True}]
    assert len(events) == 1
    assert events[0].action_type == "resume_schedule"
    assert events[0].status == "partial"
    assert events[0].summary == "RF Safety Guard: DX10 may overlap Remote Rig."
    assert events[0].detail == "RF Safety Guard mode: Warn only. Warn only should not block resume."


def test_resume_schedule_hold_rf_safety_confirm_cancel_keeps_hold_active(monkeypatch) -> None:
    qsy_helper._RESUME_GUARD_FEEDBACK_CACHE.clear()
    service = ActionFeedbackService()

    class _Settings:
        pass

    class _Scheduler:
        current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}

        def evaluate_coordination_conflict(self, entry, source="RESUME", force=False):
            return {
                "warning": True,
                "blocked": False,
                "guard_mode": "confirm",
                "summary": "RF Safety Guard: resume needs review.",
                "detail": "Confirm before returning to this schedule.",
            }

        def resume_schedule(self, **kwargs):
            raise AssertionError("Cancelled resume should not call scheduler.resume_schedule")

    class _CancelMessageBox:
        AcceptRole = 0
        RejectRole = 1

        def __init__(self, parent=None):
            self._cancel = None
            self._clicked = None

        def setWindowTitle(self, title):
            self.title = title

        def setText(self, text):
            self.text = text

        def setInformativeText(self, text):
            self.detail = text

        def addButton(self, label, role):
            button = object()
            if label == "Cancel":
                self._cancel = button
            return button

        def exec(self):
            self._clicked = self._cancel

        def clickedButton(self):
            return self._clicked

    window = types.SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _CancelMessageBox)

    result = qsy_helper.resume_schedule_hold(window, _Settings())

    events = service.recent(scope="scheduler")
    assert result is False
    assert len(events) == 1
    assert events[0].action_type == "resume_schedule"
    assert events[0].status == "blocked"
    assert events[0].summary == "Resume cancelled: RF Safety Guard warning."
    assert events[0].detail == "RF Safety Guard mode: Require confirmation. Confirm before returning to this schedule."


def test_perform_qsy_with_hold_does_not_duplicate_suspend_rf_warning(monkeypatch) -> None:
    qsy_helper._SUSPEND_GUARD_FEEDBACK_CACHE.clear()
    service = ActionFeedbackService()

    class _Settings:
        pass

    class _Scheduler:
        current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}

        def __init__(self) -> None:
            self.apply_calls = []
            self.suspend_minutes = []

        def evaluate_coordination_conflict(self, entry, source="QSY", force=False):
            return {
                "warning": True,
                "blocked": False,
                "guard_mode": "warn",
                "summary": "RF Safety Guard: DX10 may overlap Remote Rig.",
                "detail": "Warn only should not block QSY.",
            }

        def get_status_summary(self):
            return {"shared_ptt_blocked": False}

        def apply_manual_qsy(self, entry, ignore_coordination_prompt=False):
            self.apply_calls.append(bool(ignore_coordination_prompt))

        def suspend_schedule(self, minutes):
            self.suspend_minutes.append(minutes)

    class _FailIfShownMessageBox:
        def __init__(self, parent=None):
            raise AssertionError("Warn-only QSY+Hold should not show an override prompt")

    scheduler = _Scheduler()
    window = types.SimpleNamespace(
        scheduler=scheduler,
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _FailIfShownMessageBox)

    result = qsy_helper.perform_qsy_with_hold(window, _Settings(), {"freq": 7.268, "band": "40M"}, 30)

    radio_events = service.recent(scope="radio")
    scheduler_events = service.recent(scope="scheduler")
    assert result == 30
    assert scheduler.apply_calls == [True]
    assert scheduler.suspend_minutes == [30]
    assert len(radio_events) == 1
    assert radio_events[0].action_type == "qsy"
    assert radio_events[0].detail == "RF Safety Guard mode: Warn only. Warn only should not block QSY."
    assert scheduler_events == []


def test_resume_schedule_hold_rf_safety_confirm_cancel_does_not_repeat_prompt(monkeypatch) -> None:
    qsy_helper._RESUME_GUARD_FEEDBACK_CACHE.clear()
    service = ActionFeedbackService()

    class _Settings:
        pass

    class _Scheduler:
        current_schedule_entry = {"frequency": "7.078", "band": "40M", "mode": "Digi"}

        def evaluate_coordination_conflict(self, entry, source="RESUME", force=False):
            return {
                "warning": True,
                "blocked": False,
                "guard_mode": "confirm",
                "signature": "resume|north-mast|40m",
                "summary": "RF Safety Guard: resume needs review.",
                "detail": "Confirm before returning to this schedule.",
            }

        def resume_schedule(self, **kwargs):
            raise AssertionError("Cancelled resume should not call scheduler.resume_schedule")

    class _CancelMessageBox:
        AcceptRole = 0
        RejectRole = 1
        shown = 0

        def __init__(self, parent=None):
            type(self).shown += 1
            self._cancel = None
            self._clicked = None

        def setWindowTitle(self, title):
            self.title = title

        def setText(self, text):
            self.text = text

        def setInformativeText(self, text):
            self.detail = text

        def addButton(self, label, role):
            button = object()
            if label == "Cancel":
                self._cancel = button
            return button

        def exec(self):
            self._clicked = self._cancel

        def clickedButton(self):
            return self._clicked

    window = types.SimpleNamespace(
        scheduler=_Scheduler(),
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _CancelMessageBox)

    first = qsy_helper.resume_schedule_hold(window, _Settings())
    second = qsy_helper.resume_schedule_hold(window, _Settings())

    events = service.recent(scope="scheduler")
    assert first is False
    assert second is False
    assert _CancelMessageBox.shown == 1
    assert len(events) == 1
    assert events[0].summary == "Resume cancelled: RF Safety Guard warning."


def test_qsy_helper_rf_conflict_proceed_publishes_override_feedback(monkeypatch) -> None:
    service = ActionFeedbackService()

    class _Scheduler:
        def __init__(self) -> None:
            self.apply_calls = []

        def evaluate_coordination_conflict(self, entry, source="QSY"):
            return {
                "warning": True,
                "summary": "RF conflict: Remote Rig on same band.",
                "detail": "Target 7.268 MHz overlaps Remote Rig.",
            }

        def get_status_summary(self):
            return {"shared_ptt_blocked": False}

        def apply_manual_qsy(self, entry, ignore_coordination_prompt=False):
            self.apply_calls.append(bool(ignore_coordination_prompt))

    class _FakeMessageBox:
        AcceptRole = 0
        RejectRole = 1

        def __init__(self, parent=None):
            self._proceed = None
            self._clicked = None

        def setWindowTitle(self, title):
            self.title = title

        def setText(self, text):
            self.text = text

        def setInformativeText(self, text):
            self.detail = text

        def addButton(self, label, role):
            button = object()
            if label == "Proceed QSY":
                self._proceed = button
            return button

        def exec(self):
            self._clicked = self._proceed

        def clickedButton(self):
            return self._clicked

    scheduler = _Scheduler()
    window = types.SimpleNamespace(
        scheduler=scheduler,
        action_feedback_service=service,
        _active_runtime_profile={"id": 7, "name": "DX10"},
    )
    monkeypatch.setattr(qsy_helper, "QMessageBox", _FakeMessageBox)

    result = qsy_helper.perform_qsy(window, {"freq": 7.268, "band": "40M", "mode": "LSB"})

    events = service.recent(scope="radio")
    assert result is True
    assert scheduler.apply_calls == [True]
    assert len(events) == 1
    assert events[0].action_type == "qsy_override"
    assert events[0].status == "succeeded"
    assert events[0].summary == "QSY allowed: RF Safety Guard warning acknowledged."
    assert events[0].detail == "RF Safety Guard mode: Require confirmation. Target 7.268 MHz overlaps Remote Rig."
    assert events[0].source_surface == "qsy_helper_conflict"


def test_controlfreq_selected_qsy_pending_logs_malformed_metadata(monkeypatch):
    messages = []
    fake = types.SimpleNamespace(freq_combo=FakeQsyCombo({"freq": object()}))
    monkeypatch.setattr(controlfreq_mod.log, "debug", lambda message, *args: messages.append(message % args))

    assert ControlFreqTab._selected_qsy_pending(fake, 7.078) is True
    assert messages
    assert "selected QSY metadata could not be compared" in messages[0]
