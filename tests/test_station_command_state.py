from __future__ import annotations

from freqinout.core.station_command_state import (
    manual_qsy_meta_for_radio,
    scheduler_entry_radio_id,
    scheduler_manual_qsy_active_for_radio,
    scheduler_suspended_manually_for_radio,
    timed_suspend_active_for_radio,
)


def test_scheduler_entry_radio_id_accepts_known_keys() -> None:
    assert scheduler_entry_radio_id({"target_device_profile_id": 4}) == 4
    assert scheduler_entry_radio_id({"device_profile_id": 5}) == 5
    assert scheduler_entry_radio_id({"radio_id": 6}) == 6
    assert scheduler_entry_radio_id({"radio_id": "bad"}) == 0


def test_manual_qsy_meta_is_radio_scoped() -> None:
    assert manual_qsy_meta_for_radio(meta={"freq": 7.115}, meta_profile_id=2, device_profile_id=2) == {"freq": 7.115}
    assert manual_qsy_meta_for_radio(meta={"freq": 7.115}, meta_profile_id=1, device_profile_id=2) is None


def test_scheduler_manual_qsy_active_uses_scheduler_radio_scope() -> None:
    assert scheduler_manual_qsy_active_for_radio(
        device_profile_id=2,
        manual_meta=None,
        scheduler_source="QSY",
        scheduler_manual_active=False,
        scheduler_entry={"target_device_profile_id": 2},
    )
    assert not scheduler_manual_qsy_active_for_radio(
        device_profile_id=3,
        manual_meta=None,
        scheduler_source="QSY",
        scheduler_manual_active=False,
        scheduler_entry={"target_device_profile_id": 2},
    )


def test_scheduler_suspended_manual_is_radio_scoped() -> None:
    assert scheduler_suspended_manually_for_radio(
        device_profile_id=2,
        suspended_manual=True,
        suspended_profile_id=2,
        runtime_scheduler_enabled_override=None,
        selected_profile_id=1,
    )
    assert scheduler_suspended_manually_for_radio(
        device_profile_id=2,
        suspended_manual=False,
        suspended_profile_id=0,
        runtime_scheduler_enabled_override=False,
        selected_profile_id=2,
    )
    assert not scheduler_suspended_manually_for_radio(
        device_profile_id=3,
        suspended_manual=False,
        suspended_profile_id=0,
        runtime_scheduler_enabled_override=False,
        selected_profile_id=2,
    )


def test_timed_suspend_active_is_radio_scoped() -> None:
    assert timed_suspend_active_for_radio(device_profile_id=2, timed_suspend_profile_id=2, hold_active=True)
    assert not timed_suspend_active_for_radio(device_profile_id=3, timed_suspend_profile_id=2, hold_active=True)
