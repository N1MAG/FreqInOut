from __future__ import annotations

from datetime import datetime, timezone

import pytest

from freqinout.core.busy_evidence_service import BusyEvidenceService
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
from freqinout.core.shared_state import BusyEvidence


def _radio(store: MultiRadioStore, name: str) -> dict:
    return store.save_device_profile(
        {
            "name": name,
            "control_backend": "manual",
            "runtime_active": 0,
            "runtime_primary": 0,
        }
    )


def test_busy_evidence_publish_creates_schema_and_round_trips_radio_scoped_state(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    radio = _radio(store, "DX10")
    radio_id = radio_shared_state_id(radio["id"])
    service = BusyEvidenceService(store)

    saved = service.publish(
        BusyEvidence(
            id="busy_js8_tx",
            radio_profile_id=radio_id,
            source_family="js8",
            reason_code="js8_tx",
            severity="soft",
            evidence_timestamp_utc="2026-07-26T20:00:00Z",
            expiration_timestamp_utc="2026-07-26T20:02:00Z",
            description="JS8Call is transmitting.",
        )
    )

    assert saved.radio_profile_id == radio_id
    assert saved.source_family == "js8"
    assert saved.reason_code == "js8_tx"
    assert saved.severity == "soft"
    assert saved.description == "JS8Call is transmitting."
    with store.connect() as conn:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='busy_evidence'"
        ).fetchone()
    assert table is not None


def test_busy_evidence_top_reason_uses_hard_safety_priority_and_filters_expired(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    radio = _radio(store, "DX10")
    radio_id = radio_shared_state_id(radio["id"])
    service = BusyEvidenceService(store)

    service.publish(
        BusyEvidence(
            id="busy_expired_ptt",
            radio_profile_id=radio_id,
            source_family="ptt",
            reason_code="ptt_active",
            severity="hard",
            evidence_timestamp_utc="2026-07-26T19:55:00Z",
            expiration_timestamp_utc="2026-07-26T19:56:00Z",
        )
    )
    service.publish(
        BusyEvidence(
            id="busy_js8_tx",
            radio_profile_id=radio_id,
            source_family="js8",
            reason_code="js8_tx",
            severity="soft",
            evidence_timestamp_utc="2026-07-26T19:59:00Z",
            expiration_timestamp_utc="2026-07-26T20:05:00Z",
        )
    )
    service.publish(
        BusyEvidence(
            id="busy_ptt",
            radio_profile_id=radio_id,
            source_family="ptt",
            reason_code="ptt_active",
            severity="hard",
            evidence_timestamp_utc="2026-07-26T20:00:00Z",
        )
    )

    now = datetime(2026, 7, 26, 20, 0, 30, tzinfo=timezone.utc)
    active = service.active_for_radio(radio_id, now_utc=now)
    top = service.top_busy_reason(radio_id, now_utc=now)

    assert [item.id for item in active] == ["busy_ptt", "busy_js8_tx"]
    assert top is not None
    assert top.reason_code == "ptt_active"
    assert top.severity == "hard"


def test_busy_evidence_same_priority_prefers_newer_evidence(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    radio = _radio(store, "DX10")
    radio_id = radio_shared_state_id(radio["id"])
    service = BusyEvidenceService(store)

    service.publish(
        BusyEvidence(
            id="old_ptt",
            radio_profile_id=radio_id,
            source_family="ptt",
            reason_code="ptt_active",
            severity="hard",
            evidence_timestamp_utc="2026-07-26T20:00:00Z",
            description="Older PTT evidence.",
        )
    )
    service.publish(
        BusyEvidence(
            id="new_ptt",
            radio_profile_id=radio_id,
            source_family="ptt",
            reason_code="ptt_active",
            severity="hard",
            evidence_timestamp_utc="2026-07-26T20:01:00Z",
            description="Newer PTT evidence.",
        )
    )

    active = service.active_for_radio(radio_id)
    top = service.top_busy_reason(radio_id)

    assert [item.id for item in active] == ["new_ptt", "old_ptt"]
    assert top is not None
    assert top.id == "new_ptt"


def test_busy_evidence_malformed_expiration_is_not_active(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    radio = _radio(store, "DX10")
    radio_id = radio_shared_state_id(radio["id"])
    service = BusyEvidenceService(store)

    service.publish(
        BusyEvidence(
            id="bad_expiration",
            radio_profile_id=radio_id,
            source_family="js8",
            reason_code="js8_tx",
            severity="soft",
            evidence_timestamp_utc="2026-07-26T20:00:00Z",
            expiration_timestamp_utc="not-a-date",
        )
    )

    assert service.get("bad_expiration").expiration_timestamp_utc == "not-a-date"
    assert service.active_for_radio(radio_id) == ()
    assert service.top_busy_reason(radio_id) is None


def test_busy_evidence_isolated_by_radio_and_clearable(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    first = _radio(store, "DX10")
    second = _radio(store, "IC-7300")
    first_id = radio_shared_state_id(first["id"])
    second_id = radio_shared_state_id(second["id"])
    service = BusyEvidenceService(store)

    service.publish(
        BusyEvidence(
            id="busy_first",
            radio_profile_id=first_id,
            source_family="fl",
            reason_code="fldigi_tx",
            severity="soft",
            evidence_timestamp_utc="2026-07-26T20:00:00Z",
        )
    )
    service.publish(
        BusyEvidence(
            id="busy_second",
            radio_profile_id=second_id,
            source_family="varac",
            reason_code="varac_transfer",
            severity="hard",
            evidence_timestamp_utc="2026-07-26T20:01:00Z",
        )
    )

    assert [item.id for item in service.active_for_radio(first_id)] == ["busy_first"]
    assert [item.id for item in service.active_for_radio(second_id)] == ["busy_second"]
    assert service.clear("busy_first") is True
    assert service.clear("busy_first") is False
    assert service.active_for_radio(first_id) == ()
    assert [item.id for item in service.active_for_radio(second_id)] == ["busy_second"]


def test_busy_evidence_rejects_unknown_radio(tmp_path) -> None:
    service = BusyEvidenceService(MultiRadioStore(tmp_path / "freqinout.db"))

    with pytest.raises(KeyError, match="Unknown radio id"):
        service.publish(
            BusyEvidence(
                id="busy_missing",
                radio_profile_id="radio_999",
                source_family="js8",
                reason_code="js8_tx",
                severity="soft",
                evidence_timestamp_utc="2026-07-26T20:00:00Z",
            )
        )
