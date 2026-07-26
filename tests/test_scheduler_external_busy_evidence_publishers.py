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
    engine = SchedulerEngine(rig=None, js8=None, varac=None, fldigi_log=None)
    return engine, store, radio_shared_state_id(primary["id"])


def test_scheduler_js8_busy_delay_publishes_and_clears_busy_evidence(monkeypatch, tmp_path) -> None:
    engine, store, radio_id = _engine_with_store(monkeypatch, tmp_path)
    try:
        delayed, reason = engine._should_delay_for_external_busy(
            kind="js8",
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            busy=True,
            reason="RX/TX",
            ignore_busy=False,
            now_ts=100.0,
        )

        busy = BusyEvidenceService(store).top_busy_reason(radio_id)
        assert delayed is True
        assert reason == "RX/TX"
        assert busy is not None
        assert busy.source_family == "js8"
        assert busy.reason_code == "js8_tx"
        assert busy.severity == "soft"
        assert busy.description == "RX/TX"

        delayed, reason = engine._should_delay_for_external_busy(
            kind="js8",
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            busy=False,
            reason="RX/TX",
            ignore_busy=False,
            now_ts=101.0,
        )

        assert delayed is False
        assert reason is None
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is None
    finally:
        engine.stop()


def test_scheduler_varac_protected_busy_publishes_hard_transfer_evidence(monkeypatch, tmp_path) -> None:
    engine, store, radio_id = _engine_with_store(monkeypatch, tmp_path)
    try:
        delayed, reason = engine._should_delay_for_external_busy(
            kind="varac",
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            busy=True,
            reason="transfer",
            ignore_busy=True,
            protected_busy=True,
            now_ts=100.0,
        )

        busy = BusyEvidenceService(store).top_busy_reason(radio_id)
        assert delayed is True
        assert reason == "transfer"
        assert busy is not None
        assert busy.source_family == "varac"
        assert busy.reason_code == "varac_transfer"
        assert busy.severity == "hard"
        assert busy.description == "transfer"
    finally:
        engine.stop()


def test_scheduler_external_busy_watchdog_clears_busy_evidence(monkeypatch, tmp_path) -> None:
    engine, store, radio_id = _engine_with_store(monkeypatch, tmp_path)
    try:
        first, _reason = engine._should_delay_for_external_busy(
            kind="js8",
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            busy=True,
            reason="RX/TX",
            ignore_busy=False,
            now_ts=100.0,
        )
        assert first is True
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is not None

        delayed, reason = engine._should_delay_for_external_busy(
            kind="js8",
            entry_key=("40M", 7_078_000, "A", ""),
            source="HF",
            busy=True,
            reason="RX/TX",
            ignore_busy=False,
            now_ts=200.0,
        )

        assert delayed is False
        assert reason is None
        assert BusyEvidenceService(store).top_busy_reason(radio_id) is None
    finally:
        engine.stop()

