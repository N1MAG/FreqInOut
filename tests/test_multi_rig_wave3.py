import os
import sys
import types

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication, QCheckBox, QWidget
import pytest

from freqinout.core.launch_orchestrator import LaunchOrchestrator
from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import StationRuntimeManager
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

    def __init__(self, parent=None) -> None:
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


class StubSettingsTab(StubTab):
    settings_saved = Signal()
    device_profiles_changed = Signal()
    local_net_profiles_changed = Signal()
    open_logs_requested = Signal()
    log_level_changed = Signal(str)

    def __init__(self, parent=None) -> None:
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
        self.current_schedule_entry = {}

    def start(self) -> None:
        return

    def stop(self) -> None:
        return

    def force_refresh(self) -> None:
        return

    def set_runtime_scheduler_enabled(self, enabled: bool | None) -> None:
        self.runtime_enabled = True if enabled is None else bool(enabled)

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

