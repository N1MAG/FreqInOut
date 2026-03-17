from __future__ import annotations

import os
import sys
import types

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication, QWidget


class FakeStore:
    active_profile: dict[str, object] = {}

    def get_runtime_active_device_profile(self):
        return dict(type(self).active_profile)


class StubLaunchOrchestrator(QObject):
    sequence_started = Signal(object)
    sequence_progress = Signal(object)
    sequence_finished = Signal(object)

    def __init__(self) -> None:
        super().__init__()
        self.start_calls = 0
        self.stop_calls = 0

    def start_startup_sequence(self) -> bool:
        self.start_calls += 1
        return True

    def stop_sequence(self) -> None:
        self.stop_calls += 1


class StubTab(QWidget):
    schedule_saved = Signal()
    net_status_changed = Signal(str, bool)
    operator_history_updated = Signal()
    local_operator_updated = Signal()
    local_data_updated = Signal()
    sop_data_changed = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.show_callsigns = False
        self.show_states = False
        self.show_cities = False
        self.show_grids = False
        self.show_regions = False
        self.prop_overlay_enabled = False
        self.prop_mode = "blended"
        self.prop_window_hours = 6

    def on_settings_saved(self) -> None:
        pass

    def set_tab_active(self, active: bool) -> None:
        self._tab_active = bool(active)

    def on_tab_activated(self) -> None:
        pass

    def apply_theme(self) -> None:
        pass

    def _load_data(self) -> None:
        pass

    def reload_operator_lookup(self) -> None:
        pass

    def on_local_net_profiles_updated(self) -> None:
        pass

    def on_hf_schedule_saved(self) -> None:
        pass

    def on_sop_data_changed(self) -> None:
        pass

    def on_schedule_resumed(self) -> None:
        pass

    def mark_schedule_dirty(self) -> None:
        pass

    def rebuild_table(self) -> None:
        pass

    def set_map_visible(self, visible: bool) -> None:
        self._map_visible = bool(visible)

    def show_loading_toast(self) -> None:
        pass

    def set_runtime_manager(self, manager) -> None:
        self._runtime_manager = manager

    def refresh_from_manager(self, force: bool = False) -> None:
        pass


class StubSettingsTab(StubTab):
    settings_saved = Signal()
    device_profiles_changed = Signal()
    local_net_profiles_changed = Signal()
    open_logs_requested = Signal()
    log_level_changed = Signal(str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.launch_orchestrator = StubLaunchOrchestrator()


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
        self.next_change_utc = None
        self.started = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.started = False

    def force_refresh(self) -> None:
        pass

    def get_status_summary(self) -> dict[str, object]:
        return {
            "control_mode": "MANUAL",
            "use_scheduler": True,
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
        self.start_calls = 0
        self.stop_calls = 0

    def start(self, *, initial_stagger: bool = True) -> None:
        self._running = True
        self.start_calls += 1

    def stop(self) -> None:
        self._running = False
        self.stop_calls += 1

    def is_running(self) -> bool:
        return self._running


class StubJS8ControlClient:
    def __init__(self, host=None, port=None, settings=None) -> None:
        self.host = host
        self.port = port
        self.settings = settings
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1


class StubStationRuntimeManager:
    def __init__(self, store=None, settings=None) -> None:
        self.store = store
        self.settings = settings

    def sync_with_store(self) -> None:
        return

    def get_primary_runtime(self):
        return None

    def get_runtime_primary_device_profile(self):
        return dict(FakeStore.active_profile)

    def primary_runtime_signature(self):
        profile = dict(FakeStore.active_profile)
        return (
            int(profile.get("id", 0) or 0),
            str(profile.get("control_backend", "") or "").strip().lower(),
            str(profile.get("deployment_mode", "") or "").strip().lower(),
        )

    def get_runtime_snapshots(self, *, force: bool = False):
        return []

    def stop(self) -> None:
        return


def _nav_button(window, text: str):
    for btn in window.nav_buttons:
        if btn.text() == text:
            return btn
    raise AssertionError(f"nav button not found: {text}")


def _make_window(monkeypatch, tmp_path, active_profile):
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


def test_main_window_minimal_mode_suppresses_heavy_surfaces_and_background_ingest(monkeypatch, tmp_path):
    app, window = _make_window(
        monkeypatch,
        tmp_path,
        {
            "id": 2,
            "name": "Field Rig",
            "control_backend": "rigctld",
            "deployment_mode": "minimal",
        },
    )

    try:
        assert window._suppressed_screen_labels == {"Map", "Messages", "FreqPlanner"}
        assert _nav_button(window, "Map").isHidden() is True
        assert _nav_button(window, "Messages").isHidden() is True
        assert _nav_button(window, "FreqPlanner").isHidden() is True
        assert window.background_ingest.is_running() is False
        assert "Minimal mode" in window.runtime_mode_label.text()

        window._start_launch_control_startup()
        assert window.launch_orchestrator.start_calls == 0
    finally:
        window._on_app_about_to_quit()
        window.deleteLater()
        app.processEvents()


def test_main_window_restores_full_mode_behavior_when_active_profile_changes(monkeypatch, tmp_path):
    app, window = _make_window(
        monkeypatch,
        tmp_path,
        {
            "id": 2,
            "name": "Field Rig",
            "control_backend": "rigctld",
            "deployment_mode": "minimal",
        },
    )

    try:
        FakeStore.active_profile = {
            "id": 1,
            "name": "Base Station",
            "control_backend": "flrig",
            "deployment_mode": "full",
        }
        window._apply_runtime_profile_state(force=True)

        assert window._suppressed_screen_labels == set()
        assert _nav_button(window, "Map").isHidden() is False
        assert _nav_button(window, "Messages").isHidden() is False
        assert _nav_button(window, "FreqPlanner").isHidden() is False
        assert window.background_ingest.is_running() is True
        assert window.runtime_mode_label.text() == ""

        window._start_launch_control_startup()
        assert window.launch_orchestrator.start_calls == 1
    finally:
        window._on_app_about_to_quit()
        window.deleteLater()
        app.processEvents()
