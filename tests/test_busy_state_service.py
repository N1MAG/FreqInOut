from __future__ import annotations

from freqinout.core.busy_evidence_service import BusyEvidenceService
from freqinout.core.busy_state_service import BusyStateService
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
from freqinout.core.ptt_conflict_service import PttConflictService
from freqinout.core.shared_state import BusyEvidence, PttConflictEvidence


def _radio(store: MultiRadioStore, name: str) -> dict:
    return store.save_device_profile(
        {
            "name": name,
            "control_backend": "manual",
            "runtime_active": 0,
            "runtime_primary": 0,
        }
    )


def test_busy_state_uses_top_busy_evidence_and_preserves_detail_ids(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    radio = _radio(store, "DX10")
    radio_id = radio_shared_state_id(radio["id"])
    evidence = BusyEvidenceService(store)
    evidence.publish(
        BusyEvidence(
            id="busy_js8",
            radio_profile_id=radio_id,
            source_family="js8",
            reason_code="js8_tx",
            severity="soft",
            evidence_timestamp_utc="2026-07-26T20:00:00Z",
            description="JS8Call is busy.",
        )
    )
    evidence.publish(
        BusyEvidence(
            id="busy_ptt",
            radio_profile_id=radio_id,
            source_family="ptt",
            reason_code="ptt_active",
            severity="hard",
            evidence_timestamp_utc="2026-07-26T20:01:00Z",
            description="Rig PTT is active.",
        )
    )

    state = BusyStateService(store).state_for_radio(radio_id)

    assert state.busy is True
    assert state.severity == "hard"
    assert state.source_family == "ptt"
    assert state.reason_code == "ptt_active"
    assert state.summary == "Rig PTT is active."
    assert state.top_evidence_id == "busy_ptt"
    assert state.evidence_ids == ("busy_ptt", "busy_js8")


def test_busy_state_includes_ptt_conflicts_without_busy_evidence(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    requested = _radio(store, "DX10")
    blocking = _radio(store, "IC-7300")
    requested_id = radio_shared_state_id(requested["id"])
    blocking_id = radio_shared_state_id(blocking["id"])
    PttConflictService(store).publish(
        PttConflictEvidence(
            id="ptt_conflict",
            ptt_group="AMP-A",
            requested_radio_id=requested_id,
            blocking_radio_id=blocking_id,
            source="scheduler_shared_ptt",
        )
    )

    state = BusyStateService(store).state_for_radio(requested_id)

    assert state.busy is False
    assert state.severity == "none"
    assert state.evidence_ids == ()
    assert state.ptt_conflict_ids == ("ptt_conflict",)


def test_busy_state_active_states_includes_busy_and_conflict_radios(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    busy_radio = _radio(store, "DX10")
    conflict_requested = _radio(store, "IC-7300")
    conflict_blocking = _radio(store, "Portable")
    busy_id = radio_shared_state_id(busy_radio["id"])
    requested_id = radio_shared_state_id(conflict_requested["id"])
    blocking_id = radio_shared_state_id(conflict_blocking["id"])
    BusyEvidenceService(store).publish(
        BusyEvidence(
            id="busy_js8",
            radio_profile_id=busy_id,
            source_family="js8",
            reason_code="js8_tx",
            severity="soft",
            evidence_timestamp_utc="2026-07-26T20:00:00Z",
        )
    )
    PttConflictService(store).publish(
        PttConflictEvidence(
            id="ptt_conflict",
            ptt_group="AMP-A",
            requested_radio_id=requested_id,
            blocking_radio_id=blocking_id,
            source="scheduler_shared_ptt",
        )
    )

    states = BusyStateService(store).active_states()

    assert tuple(state.radio_profile_id for state in states) == (busy_id, requested_id, blocking_id)
    assert states[0].busy is True
    assert states[1].ptt_conflict_ids == ("ptt_conflict",)
    assert states[2].ptt_conflict_ids == ("ptt_conflict",)

