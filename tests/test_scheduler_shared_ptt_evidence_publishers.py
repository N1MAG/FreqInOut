from __future__ import annotations

from types import SimpleNamespace

from freqinout.core.busy_evidence_service import BusyEvidenceService
from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
from freqinout.core.ptt_conflict_service import PttConflictService
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager


class _Rig:
    def get_vfo_frequency(self):
        return 7_074_000

    def get_ptt(self):
        return False


class _SharedPttManager:
    def __init__(self, owner_id: int) -> None:
        self.owner_id = int(owner_id)
        self.blocked = True

    def shared_ptt_lock_snapshot(self, *, force=False):
        if self.blocked:
            return SimpleNamespace(
                ptt_group="AMP-A",
                blocked=True,
                owner_device_profile_id=self.owner_id,
                owner_name="Remote Rig",
                owner_backend="flrig",
                owner_ptt_active=True,
                target_ptt_active=False,
                reason="Shared PTT group AMP-A is in use by Remote Rig.",
            )
        return SimpleNamespace(
            ptt_group="AMP-A",
            blocked=False,
            owner_device_profile_id=None,
            owner_name="",
            owner_backend="",
            owner_ptt_active=False,
            target_ptt_active=False,
            reason="Shared PTT group AMP-A is clear.",
        )


def _configure_scheduler_for_apply(monkeypatch, engine: SchedulerEngine, queued: list[tuple[str, int]]) -> None:
    monkeypatch.setattr(engine, "_control_mode", lambda: "FLRIG")
    monkeypatch.setattr(engine, "_scheduler_enabled", lambda: True)
    monkeypatch.setattr(engine, "_varac_status", lambda: {"busy": False, "waiting_for_frequency": False, "reason": None})
    monkeypatch.setattr(engine, "_js8_busy_ok", lambda: True)
    monkeypatch.setattr(engine, "_varac_busy_ok", lambda status=None: True)
    monkeypatch.setattr(engine, "_should_delay_for_fldigi", lambda **kwargs: (False, None))
    monkeypatch.setattr(engine, "_net_corrections_suppressed", lambda: False)
    monkeypatch.setattr(engine, "_queue_control_action", lambda **kwargs: queued.append((kwargs["source"], kwargs["freq_hz"])) or True)


def test_scheduler_shared_ptt_block_publishes_busy_and_conflict_evidence(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    owner = store.save_device_profile({"name": "Remote Rig", "control_backend": "flrig", "ptt_group": "AMP-A"})
    primary_id = radio_shared_state_id(primary["id"])
    owner_id = radio_shared_state_id(owner["id"])
    manager = _SharedPttManager(owner["id"])
    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None, station_runtime_manager=manager)
    queued: list[tuple[str, int]] = []
    try:
        _configure_scheduler_for_apply(monkeypatch, engine, queued)

        engine._apply_schedule_entry({"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"}, "HF")

        busy = BusyEvidenceService(store).top_busy_reason(primary_id)
        conflicts = PttConflictService(store).active_for_radio(primary_id)
        assert queued == []
        assert busy is not None
        assert busy.reason_code == "shared_ptt_interlock"
        assert busy.severity == "hard"
        assert "Remote Rig" in busy.description
        assert len(conflicts) == 1
        assert conflicts[0].requested_radio_id == primary_id
        assert conflicts[0].blocking_radio_id == owner_id
        assert conflicts[0].ptt_group == "AMP-A"
        assert conflicts[0].source == "scheduler_shared_ptt"
    finally:
        engine.stop()


def test_scheduler_shared_ptt_clear_removes_scheduler_owned_evidence(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    owner = store.save_device_profile({"name": "Remote Rig", "control_backend": "flrig", "ptt_group": "AMP-A"})
    primary_id = radio_shared_state_id(primary["id"])
    manager = _SharedPttManager(owner["id"])
    engine = SchedulerEngine(rig=_Rig(), js8=None, varac=None, fldigi_log=None, station_runtime_manager=manager)
    queued: list[tuple[str, int]] = []
    try:
        _configure_scheduler_for_apply(monkeypatch, engine, queued)
        entry = {"frequency": "7.078", "band": "40M", "mode": "Digi", "vfo": "A"}

        engine._apply_schedule_entry(entry, "HF")
        assert BusyEvidenceService(store).top_busy_reason(primary_id) is not None
        assert PttConflictService(store).active_for_radio(primary_id)

        manager.blocked = False
        engine._apply_schedule_entry(entry, "HF")

        assert BusyEvidenceService(store).top_busy_reason(primary_id) is None
        assert PttConflictService(store).active_for_radio(primary_id) == ()
    finally:
        engine.stop()

