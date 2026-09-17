"""Focused GRS-5 UI-state checks for Guided Add Radio."""

from pathlib import Path

from freqinout.gui.guided_receiver_presentation import (
    guided_recovery_presentation,
    receiver_state_presentation,
    validation_messages,
)


def _receiver(**changes):
    values = {
        "application": "SDR++",
        "adapter": "sdrpp_rigctl",
        "host": "127.0.0.1",
        "port": "4532",
        "target": "selected-vfo",
        "verification_state": "verified",
        "evidence_present": True,
        "evidence_matches": True,
        "automatic_tuning_enabled": True,
        "resource_blocks": (),
    }
    values.update(changes)
    return receiver_state_presentation(**values)


def test_receiver_state_vocabulary_is_exact_and_fail_safe():
    assert _receiver().title == "Verified — automatic retune allowed"
    assert _receiver(resource_blocks=("FRONT-A is in use",)).title == "Blocked by receiver resource"
    assert _receiver(evidence_matches=False).title == "Verification expired or changed"
    assert _receiver(evidence_present=False, evidence_matches=False).title == "Verification expired or changed"
    assert _receiver(automatic_tuning_enabled=False, resource_blocks=("FRONT-A is in use",)).title == "Manual / reminders only"
    assert _receiver(automatic_tuning_enabled=False).title == "Manual / reminders only"
    assert _receiver(host="").title == "Not configured"


def test_guard_validation_preserves_every_message_and_role_language():
    payload = {"blocked": ["front end busy", "antenna unavailable"], "warnings": ["band review"]}
    assert validation_messages(payload, observer=True) == (
        "Receiver Guard blocked: front end busy",
        "Receiver Guard blocked: antenna unavailable",
        "Receiver Guard warning: band review",
    )
    assert validation_messages(payload, observer=False)[0].startswith("RF Guard blocked:")


def test_recovery_states_name_the_exact_retry_route():
    operator = guided_recovery_presentation(operator_start_apps=("FLMsg",))
    assert operator.status == "Saved — operator start required"
    assert operator.retry_route == "Settings → Software → FLMsg → Launch"

    pending = guided_recovery_presentation(verification_pending_apps=("SDR++",))
    assert pending.status == "Saved — verification pending"
    assert pending.retry_route == "Settings → Software → SDR++ → Verify"

    attention = guided_recovery_presentation(needs_attention_app="JS8Call")
    assert attention.status == "Saved — one app needs attention"
    assert attention.retry_route == "Settings → Software → JS8Call"

    bundle = guided_recovery_presentation(launch_bundle_retry_app="SDR++")
    assert bundle.status == "Saved — launch bundle retry required"
    assert bundle.retry_route == "Settings → Radios → SDR++ → Launch Control"


def test_guided_dialog_exposes_receiver_guard_schedule_cards_and_persists_observer_plan():
    source = (
        Path(__file__).parents[1] / "freqinout" / "gui" / "settings_tab.py"
    ).read_text(encoding="utf-8")
    assert 'setObjectName("guidedReceiverGuardState")' in source
    assert 'setObjectName("guidedReceiveScheduleState")' in source
    assert 'schedule_group.setTitle("Receive Schedule" if observer_mode else "Radio Schedule")' in source
    assert 'if observer_mode and int(row.get("receive_only", 0) or 0) != 1:' in source
    assert "or observer_mode" in source[source.index("schedule_choice = _selected_guided_schedule_path()") :]
    assert "guided_recovery_presentation(needs_attention_app=instance_name)" in source
    assert "save_radio_launch_bundle" in source
    assert '"Radio setup not saved"' in source
    assert "Saved — verification pending" in (
        Path(__file__).parents[1] / "freqinout" / "gui" / "guided_receiver_presentation.py"
    ).read_text(encoding="utf-8")


def test_guided_receiver_presenter_has_no_qt_or_io_dependencies():
    source = (
        Path(__file__).parents[1]
        / "freqinout"
        / "gui"
        / "guided_receiver_presentation.py"
    ).read_text(encoding="utf-8")
    assert "PyQt" not in source
    assert "Path(" not in source
    assert "open(" not in source
    assert "sqlite" not in source
