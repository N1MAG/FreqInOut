from __future__ import annotations

from freqinout.core.busy_evidence_service import BusyEvidenceService
from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager


def _engine_with_store(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    engine = SchedulerEngine(rig=None, js8=None, varac=None, fldigi_log=object())
    return engine, store, radio_shared_state_id(primary["id"])


def _prime_fldigi_status(engine: SchedulerEngine, *, busy: bool, now_ts: float = 100.0) -> None:
    entry_key = ("40M", 7_078_000, "A", "")
    engine._fldigi_busy_entry_key = entry_key
    engine._fldigi_busy_check_source = "HF"
    engine._fldigi_busy_check_target_hz = 7_078_000
    engine._fldigi_busy_check_result = {
        "busy": busy,
        "reason": "RX activity" if busy else None,
        "checked_ts": now_ts,
        "duration_ms": 1.0,
        "error": None,
    }


def test_scheduler_fldigi_busy_result_publishes_soft_busy_evidence(monkeypatch, tmp_path) -> None:
    engine, store, radio_id = _engine_with_store(monkeypatch, tmp_path)
    try:
        _prime_fldigi_status(engine, busy=True, now_ts=100.0)

        delayed, reason = engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=101.0,
        )

        busy = BusyEvidenceService(store).top_busy_reason(radio_id)
        assert delayed is True
        assert reason == "RX activity"
        assert busy is not None
        assert busy.source_family == "fl"
        assert busy.reason_code == "receive_decode"
        assert busy.severity == "soft"
        assert busy.description == "RX activity"
    finally:
        engine.stop()


def test_scheduler_fldigi_not_busy_clears_busy_evidence(monkeypatch, tmp_path) -> None:
    engine, store, radio_id = _engine_with_store(monkeypatch, tmp_path)
    try:
        _prime_fldigi_status(engine, busy=True, now_ts=100.0)
        engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=101.0,
        )
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is not None

        _prime_fldigi_status(engine, busy=False, now_ts=102.0)
        delayed, reason = engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=103.0,
        )

        assert delayed is False
        assert reason is None
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is None
    finally:
        engine.stop()


def test_scheduler_fldigi_watchdog_breakaway_clears_busy_evidence(monkeypatch, tmp_path) -> None:
    engine, store, radio_id = _engine_with_store(monkeypatch, tmp_path)
    try:
        _prime_fldigi_status(engine, busy=True, now_ts=100.0)
        engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=101.0,
        )
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is not None

        _prime_fldigi_status(engine, busy=True, now_ts=200.0)
        engine._fldigi_busy_since_ts = 1.0
        delayed, reason = engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=200.0,
        )

        assert delayed is False
        assert reason is None
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is None
    finally:
        engine.stop()


def test_scheduler_fldigi_expired_result_clears_previous_busy_evidence(monkeypatch, tmp_path) -> None:
    engine, store, radio_id = _engine_with_store(monkeypatch, tmp_path)
    try:
        _prime_fldigi_status(engine, busy=True, now_ts=100.0)
        engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=101.0,
        )
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is not None

        delayed, reason = engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=200.0,
        )

        assert delayed is True
        assert reason == "checking FLDigi receive activity"
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is None
    finally:
        engine.stop()


def test_scheduler_fldigi_error_result_clears_previous_busy_evidence(monkeypatch, tmp_path) -> None:
    engine, store, radio_id = _engine_with_store(monkeypatch, tmp_path)
    try:
        _prime_fldigi_status(engine, busy=True, now_ts=100.0)
        engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=101.0,
        )
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is not None

        _prime_fldigi_status(engine, busy=False, now_ts=102.0)
        engine._fldigi_busy_check_result["error"] = "timeout"
        delayed, reason = engine._should_delay_for_fldigi(
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            target_frequency_hz=7_078_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=103.0,
        )

        assert delayed is False
        assert reason is None
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is None
    finally:
        engine.stop()
