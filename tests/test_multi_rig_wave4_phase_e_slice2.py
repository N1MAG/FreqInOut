from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import DeviceRuntime, StationRuntimeManager
from freqinout.gui import qsy_helper


def _idle_status_snapshot(self, **kwargs):
    return {
        "JS8Call_API": {"state": "idle", "tooltip": "JS8 idle"},
        "JS8Call": {"state": "idle", "tooltip": "JS8 idle"},
        "FLRig": {"state": "ok", "tooltip": "FLRig reachable"},
        "RigCtlD": {"state": "ok", "tooltip": "RigCtlD reachable"},
        "FLDigi": {"state": "idle", "tooltip": "FLDigi idle"},
        "FLMsg": {"state": "idle", "tooltip": "FLMsg idle"},
        "FLAmp": {"state": "idle", "tooltip": "FLAmp idle"},
        "VarAC": {"state": "idle", "tooltip": "VarAC idle"},
        "JS8Spotter": {"state": "idle", "tooltip": "JS8Spotter idle"},
        "CommStat": {"state": "idle", "tooltip": "CommStat idle"},
    }


def test_store_derives_rf_conflict_policies_from_shared_resources(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    first = store.save_device_profile(
        {
            "name": "TX-A",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "antenna_group": " ant-1 ",
            "amplifier_group": "amp-main",
        }
    )
    second = store.save_device_profile(
        {
            "name": "TX-B",
            "control_backend": "rigctld",
            "rig_host": "127.0.0.1",
            "rig_port": 4532,
            "antenna_group": "ANT-1",
            "frontend_group": "front-a",
        }
    )
    store.save_device_profile(
        {
            "name": "Observer",
            "control_backend": "manual",
            "device_class": "observer",
            "antenna_group": "ANT-1",
            "amplifier_group": "AMP-MAIN",
        }
    )

    policies = store.list_station_coordination_policies("rf_conflict")
    assert len(policies) == 1
    policy = policies[0]
    assert int(policy["source_device_id"]) == min(int(first["id"]), int(second["id"]))
    assert int(policy["target_device_id"]) == max(int(first["id"]), int(second["id"]))
    assert policy["trigger"]["antenna_groups"] == ["ANT-1"]
    assert policy["trigger"]["amplifier_groups"] == []
    assert policy["trigger"]["frontend_groups"] == []
    assert policy["action"]["warning"] == "primary_runtime_rf_overlap"
    assert policy["safety_mode"] == "prompt"

    store.save_device_profile({"id": int(second["id"]), "antenna_group": ""})
    assert store.list_station_coordination_policies("rf_conflict") == []


def test_station_runtime_manager_reports_rf_conflict_context(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "flrig_host": "10.0.0.8",
            "flrig_port": 22345,
            "antenna_group": "ANT-1",
            "amplifier_group": "AMP-MAIN",
        }
    )
    secondary = store.save_device_profile(
        {
            "name": "Remote Rig",
            "control_backend": "rigctld",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
            "antenna_group": "ant-1",
            "frontend_group": "FRONT-A",
        }
    )
    store.set_device_profile_runtime_active(int(primary["id"]), True)
    store.set_device_profile_runtime_active(int(secondary["id"]), True)
    store.set_runtime_primary_device_profile(int(primary["id"]))

    default_primary = next(
        row for row in store.list_device_profiles() if str(row.get("system_key", "") or "") == "default_device"
    )
    if int(default_primary.get("runtime_active", 0) or 0) == 1:
        store.set_device_profile_runtime_active(int(default_primary["id"]), False)

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _idle_status_snapshot)
    monkeypatch.setattr(
        DeviceRuntime,
        "current_frequency_hz",
        lambda self, force=False: 7_074_000 if int(self.profile.get("id", 0) or 0) == int(secondary["id"]) else 7_078_000,
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)

    primary_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(primary["id"]))
    secondary_snapshot = next(snap for snap in snapshots if snap.device_profile_id == int(secondary["id"]))

    assert primary_snapshot.antenna_group == "ANT-1"
    assert primary_snapshot.amplifier_group == "AMP-MAIN"
    assert secondary_snapshot.frontend_group == "FRONT-A"
    assert secondary_snapshot.current_frequency_label == "7.074 MHz"

    conflict = manager.evaluate_primary_rf_conflict(
        target_band="40M",
        target_frequency_hz=7_078_000,
        source="HF",
        force=True,
    )
    assert conflict is not None
    assert conflict.peer_name == "Remote Rig"
    assert conflict.same_band is True
    assert conflict.same_frequency is False
    assert conflict.shared_antenna_groups == ["ANT-1"]
    assert "RF conflict" in conflict.summary


def test_settings_tab_persists_rf_resource_groups(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        tab._persist_device_profile(
            {
                "name": "RF Shared Rig",
                "control_backend": "flrig",
                "flrig_host": "127.0.0.1",
                "flrig_port": 12355,
                "fldigi_host": "127.0.0.1",
                "fldigi_port": 7362,
                "js8_host": "127.0.0.1",
                "js8_port": 2442,
                "launch_enabled": True,
                "launch_path": "",
                "ptt_group": "AMP-A",
                "antenna_group": "ANT-1",
                "frontend_group": "FRONT-A",
                "amplifier_group": "AMP-MAIN",
                "notes": "Shared RF chain",
            }
        )
        saved = next(row for row in store.list_device_profiles() if row["name"] == "RF Shared Rig")
        assert saved["antenna_group"] == "ANT-1"
        assert saved["frontend_group"] == "FRONT-A"
        assert saved["amplifier_group"] == "AMP-MAIN"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_manual_qsy_prompts_before_rf_conflict_proceed(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    class _Scheduler:
        def __init__(self) -> None:
            self.apply_calls: list[bool] = []

        def evaluate_coordination_conflict(self, entry, source="QSY", force=False):
            return {
                "warning": True,
                "summary": "RF conflict: Remote Rig on same band 40M via antenna ANT-1.",
                "detail": "Target 7.078 MHz 40M overlaps Remote Rig at 7.074 MHz 40M.",
            }

        def get_status_summary(self):
            return {"shared_ptt_blocked": False}

        def apply_manual_qsy(self, entry, ignore_coordination_prompt=False):
            self.apply_calls.append(bool(ignore_coordination_prompt))

    class _Window:
        def __init__(self) -> None:
            self.scheduler = _Scheduler()

    class _FakeMessageBox:
        AcceptRole = 0
        RejectRole = 1

        def __init__(self, parent=None):
            self._clicked = None
            self._proceed = None

        @staticmethod
        def warning(*args, **kwargs):
            return None

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

    monkeypatch.setattr(qsy_helper, "QMessageBox", _FakeMessageBox)

    window = _Window()
    result = qsy_helper.perform_qsy(window, {"freq": 7.078, "band": "40M", "mode": "Digi"})

    assert result is True
    assert window.scheduler.apply_calls == [True]


def test_scheduler_emits_coordination_conflict_prompt_once(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()

    class _Rig:
        def get_vfo_frequency(self):
            return 7_074_000

        def get_ptt(self):
            return False

    class _Manager:
        def evaluate_primary_rf_conflict(self, *, target_band="", target_frequency_hz=None, source="", force=False):
            return SimpleNamespace(
                peer_device_profile_id=22,
                peer_name="Remote Rig",
                peer_band="40M",
                peer_frequency_hz=7_074_000,
                target_band=target_band,
                target_frequency_hz=target_frequency_hz,
                same_band=True,
                same_frequency=False,
                shared_antenna_groups=["ANT-1"],
                shared_amplifier_groups=[],
                shared_frontend_groups=[],
                summary="RF conflict: Remote Rig on same band 40M via antenna ANT-1.",
                detail="Target 7.078 MHz 40M overlaps Remote Rig at 7.074 MHz 40M.",
                signature="1|HF|40M|7078000|22|ANT-1||",
            )

    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None, station_runtime_manager=_Manager())
    try:
        emitted: list[dict[str, object]] = []
        queued: list[tuple[str, int]] = []
        engine.coordination_conflict_detected.connect(lambda payload: emitted.append(dict(payload)))
        monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
        monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
        monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
        monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
        monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
        monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
        monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
        monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)

        entry = {"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"}
        engine._apply_schedule_entry(entry, "HF")
        status = engine.get_status_summary()
        engine._apply_schedule_entry(entry, "HF")

        assert len(emitted) == 1
        assert queued == []
        assert status["rf_conflict_warning"] is True
        assert "Remote Rig" in str(status["rf_conflict_summary"])
    finally:
        engine.stop()
