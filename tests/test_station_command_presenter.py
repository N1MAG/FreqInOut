from __future__ import annotations

from freqinout.gui.station_command_presenter import (
    countdown_text,
    qsy_action_state,
    scheduler_action_state,
    timed_qsy_text,
)


def test_qsy_action_state_mutes_unchanged_frequency() -> None:
    state = qsy_action_state(
        selected_meta={"freq": 7.115},
        preferred_key="7.115000",
        radio_id=1,
        selection_changed=False,
        manual_qsy_active=False,
        timed_qsy_active=False,
    )

    assert state.qsy_enabled is False
    assert state.timed_qsy_enabled is False
    assert state.qsy_role == "muted"
    assert state.timed_qsy_role == "muted"


def test_qsy_action_state_enables_changed_frequency() -> None:
    state = qsy_action_state(
        selected_meta={"freq": 14.115},
        preferred_key="7.115000",
        radio_id=1,
        selection_changed=True,
        manual_qsy_active=False,
        timed_qsy_active=False,
    )

    assert state.qsy_enabled is True
    assert state.timed_qsy_enabled is True
    assert state.qsy_role == "info"
    assert state.timed_qsy_role == "info"


def test_qsy_action_state_does_not_highlight_manual_state_without_new_selection() -> None:
    state = qsy_action_state(
        selected_meta={"freq": 14.115},
        preferred_key="7.115000",
        radio_id=1,
        selection_changed=False,
        manual_qsy_active=True,
        timed_qsy_active=False,
    )

    assert state.qsy_enabled is False
    assert state.qsy_role == "muted"


def test_scheduler_action_state_labels_timed_and_indefinite_suspend() -> None:
    timed = scheduler_action_state(
        timed_qsy_active=False,
        timed_suspend_active=True,
        scheduler_suspended_manual=False,
        scheduler_state_text="On Schedule",
    )
    indefinite = scheduler_action_state(
        timed_qsy_active=False,
        timed_suspend_active=False,
        scheduler_suspended_manual=True,
        scheduler_state_text="On Schedule",
    )

    assert timed.timed_suspend_text == "Extend Suspend"
    assert timed.timed_suspend_role == "warning"
    assert timed.resume_role == "warning"
    assert indefinite.timed_suspend_text == "Indefinite Suspend"
    assert indefinite.timed_suspend_role == "warning"


def test_station_command_countdown_and_timed_qsy_text() -> None:
    assert countdown_text(75) == "01:15"
    assert countdown_text(900) == "15m"
    assert timed_qsy_text(timed_qsy_active=False) == "Timed QSY"
    assert timed_qsy_text(timed_qsy_active=True) == "Extend QSY"
