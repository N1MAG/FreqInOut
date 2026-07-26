from __future__ import annotations

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.scheduler_events import load_recent_scheduler_events, record_scheduler_event
from freqinout.core.settings_manager import SettingsManager


def test_scheduler_events_persist_radio_profile_id(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))

    record_scheduler_event(
        event_type="hold",
        code="ptt_active",
        source="HF",
        action="Holding schedule change",
        radio_profile_id="radio_7",
        metadata={"example": True},
    )

    events = load_recent_scheduler_events(limit=1)

    assert events[0]["radio_profile_id"] == "radio_7"
    assert events[0]["metadata"]["example"] is True


def test_scheduler_engine_records_primary_radio_profile_id(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    primary_id = radio_shared_state_id(primary["id"])
    engine = SchedulerEngine(rig=None, js8=None, varac=None, fldigi_log=None)
    try:
        engine._record_scheduler_event(
            "hold",
            "manual_test",
            source="HF",
            action="Manual test event",
            throttle_sec=0.0,
        )

        events = load_recent_scheduler_events(limit=1)
        assert events[0]["radio_profile_id"] == primary_id
        assert events[0]["metadata"]["radio_profile_id"] == primary_id
    finally:
        engine.stop()

