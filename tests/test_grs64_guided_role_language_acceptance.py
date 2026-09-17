"""GRS-6.4 focused role, language, and recipe presentation contracts."""

from __future__ import annotations

from freqinout.gui.settings_tab import DEVICE_CLASS_OPTIONS
from freqinout.core.guided_launch_recipes import resolve_js8_managed_recipe
from freqinout.core.guided_setup import build_guided_setup_blueprint, guided_setup_wizard_view


def test_add_radio_exposes_exactly_transceiver_and_receive_only_sdr_roles() -> None:
    assert DEVICE_CLASS_OPTIONS == [
        ("Transceiver", "tx_rx"),
        ("Receive-only SDR", "observer"),
    ]


def test_step_two_explains_radio_behavior_and_schedule_is_only_in_schedule_step() -> None:
    view = guided_setup_wizard_view("model", radio_role="observer")
    assert "schedule" not in view.detail.casefold()
    assert "radio" in view.detail.casefold() or "receive" in view.detail.casefold()
    blueprint = build_guided_setup_blueprint(lane="js8_only", receive_only=True)
    schedule = next(step for step in blueprint.steps if step.step_id == "schedule_intent")
    assert schedule.choices
    assert all(step.step_id != "schedule_intent" for step in blueprint.steps if step.step_id == "app_instance")


def test_resolved_managed_recipe_has_effective_command_while_unknown_uses_advanced_recovery() -> None:
    known = resolve_js8_managed_recipe(
        {
            "draft_instance_key": "radio-a-js8",
            "radio_role": "tx_rx",
            "variant": "js8call_2_2",
            "version": "2.2.0",
            "application_path": "/opt/js8call",
            "port": 2442,
            "udp_port": 2237,
        },
        managed_root="/fio/managed-instances",
    )
    unknown = resolve_js8_managed_recipe(
        {
            "draft_instance_key": "future",
            "radio_role": "tx_rx",
            "variant": "future",
            "version": "99",
            "application_path": "/opt/future",
            "port": 2443,
            "udp_port": 2238,
        },
        managed_root="/fio/managed-instances",
    )
    assert known.qualified and known.components[0].effective_command
    assert unknown.status == "unsupported" and unknown.raw_override_allowed
    assert "Advanced" in unknown.recovery_action or "operator" in unknown.recovery_action.casefold()
