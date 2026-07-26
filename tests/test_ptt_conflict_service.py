from __future__ import annotations

import pytest

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
from freqinout.core.ptt_conflict_service import PttConflictService
from freqinout.core.shared_state import PttConflictEvidence


def _radio(store: MultiRadioStore, name: str, ptt_group: str = "") -> dict:
    return store.save_device_profile(
        {
            "name": name,
            "control_backend": "manual",
            "ptt_group": ptt_group,
            "runtime_active": 0,
            "runtime_primary": 0,
        }
    )


def test_ptt_conflict_publish_creates_schema_and_round_trips_radio_ids(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    requested = _radio(store, "DX10", " amp-a ")
    blocking = _radio(store, "IC-7300", "AMP-A")
    service = PttConflictService(store)

    saved = service.publish(
        PttConflictEvidence(
            id="ptt_conflict_1",
            ptt_group=" amp-a ",
            requested_radio_id=radio_shared_state_id(requested["id"]),
            blocking_radio_id=radio_shared_state_id(blocking["id"]),
            severity="hard",
            source="scheduler",
            created_at_utc="2026-07-26T20:00:00Z",
        )
    )

    assert saved.ptt_group == "AMP-A"
    assert saved.requested_radio_id == radio_shared_state_id(requested["id"])
    assert saved.blocking_radio_id == radio_shared_state_id(blocking["id"])
    assert saved.severity == "hard"
    assert saved.source == "scheduler"
    with store.connect() as conn:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ptt_conflict_evidence'"
        ).fetchone()
    assert table is not None


def test_ptt_conflict_lists_by_requested_blocking_radio_and_group(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    requested = _radio(store, "DX10", "AMP-A")
    blocking = _radio(store, "IC-7300", "AMP-A")
    unrelated = _radio(store, "Portable", "AMP-B")
    requested_id = radio_shared_state_id(requested["id"])
    blocking_id = radio_shared_state_id(blocking["id"])
    unrelated_id = radio_shared_state_id(unrelated["id"])
    service = PttConflictService(store)

    service.publish(
        PttConflictEvidence(
            id="ptt_conflict_1",
            ptt_group="AMP-A",
            requested_radio_id=requested_id,
            blocking_radio_id=blocking_id,
            source="scheduler",
            created_at_utc="2026-07-26T20:00:00Z",
        )
    )

    assert [item.id for item in service.active_for_radio(requested_id)] == ["ptt_conflict_1"]
    assert [item.id for item in service.active_for_radio(blocking_id)] == ["ptt_conflict_1"]
    assert service.active_for_radio(unrelated_id) == ()
    assert [item.id for item in service.active_for_group(" amp-a ")] == ["ptt_conflict_1"]


def test_ptt_conflict_clear_and_update(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    requested = _radio(store, "DX10", "AMP-A")
    blocking = _radio(store, "IC-7300", "AMP-A")
    service = PttConflictService(store)

    service.publish(
        PttConflictEvidence(
            id="ptt_conflict_1",
            ptt_group="AMP-A",
            requested_radio_id=radio_shared_state_id(requested["id"]),
            blocking_radio_id=None,
            source="scheduler",
        )
    )
    updated = service.publish(
        PttConflictEvidence(
            id="ptt_conflict_1",
            ptt_group="AMP-A",
            requested_radio_id=radio_shared_state_id(requested["id"]),
            blocking_radio_id=radio_shared_state_id(blocking["id"]),
            source="shared_ptt",
        )
    )

    assert updated.blocking_radio_id == radio_shared_state_id(blocking["id"])
    assert updated.source == "shared_ptt"
    assert service.clear("ptt_conflict_1") is True
    assert service.clear("ptt_conflict_1") is False
    assert service.active_for_group("AMP-A") == ()


def test_ptt_conflict_rejects_missing_group_and_unknown_radio(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    requested = _radio(store, "DX10", "AMP-A")
    service = PttConflictService(store)

    with pytest.raises(ValueError, match="PTT group"):
        service.publish(
            PttConflictEvidence(
                id="ptt_conflict_1",
                ptt_group="",
                requested_radio_id=radio_shared_state_id(requested["id"]),
            )
        )

    with pytest.raises(KeyError, match="Unknown radio id"):
        service.publish(
            PttConflictEvidence(
                id="ptt_conflict_2",
                ptt_group="AMP-A",
                requested_radio_id="radio_999",
            )
        )

