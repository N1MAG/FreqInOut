from freqinout.core.condition_sop_invocation import (
    plan_condition_sop_invocations,
    schedule_layer_rows_for_condition_decision,
)
from freqinout.core.observation_projection import Observation


def _condition_observation(*, action: str = "prompt-to-apply", level: int = 3) -> Observation:
    return Observation(
        observation_id=f"condition_alert:magnet:{level}",
        source_family="condition_alert",
        source_ref="js8:77",
        source_app="JS8Call",
        received_utc="2026-08-21T18:30:00+00:00",
        event_utc="2026-08-21T18:30:00+00:00",
        from_call="N1MAG",
        to_target="@MAGNET",
        groups=("MAGNET",),
        observed_topics=("General Intel", "Comms"),
        operator_attention=True,
        status="CONDITION ALERT",
        urgency=f"LEVEL {level}",
        subject=f"MagNet MAGCON: Level {level}",
        summary=f"MAGNET condition level {level}",
        provenance={
            "rule_id": "magnet-magcon",
            "operating_group": "MAGNET",
            "condition_level": level,
            "action": action,
        },
    )


def test_condition_sop_invocation_plan_prepares_update_when_auto_allowed() -> None:
    plans = plan_condition_sop_invocations(
        [_condition_observation(action="auto-apply", level=4)],
        settings_data={
            "operating_groups": [
                {"group": "MAGNET", "band": "20M", "use_condition_levels": True, "condition_level": 2},
                {"group": "MAGNET", "band": "40M", "use_condition_levels": True, "condition_level": 2},
            ]
        },
        sop_profiles=[
            {
                "id": 7,
                "name": "MagNet Condition 4",
                "schedule_layer": [{"group_name": "MAGNET", "condition_levels": "4"}],
            }
        ],
        auto_apply_enabled=True,
    )

    assert len(plans) == 1
    assert plans[0].ready_to_apply is True
    assert plans[0].update is not None
    assert plans[0].update.changed_rows == 2
    assert [row["condition_level"] for row in plans[0].update.settings_data["operating_groups"]] == [4, 4]
    assert plans[0].audit["decision"] == "apply"
    assert plans[0].audit["changed_rows"] == 2


def test_condition_sop_invocation_plan_does_not_update_when_auto_gate_disabled() -> None:
    plans = plan_condition_sop_invocations(
        [_condition_observation(action="auto-apply", level=4)],
        settings_data={"operating_groups": [{"group": "MAGNET", "condition_level": 2}]},
        sop_profiles=[
            {
                "id": 7,
                "name": "MagNet Condition 4",
                "schedule_layer": [{"group_name": "MAGNET", "condition_levels": "4"}],
            }
        ],
        auto_apply_enabled=False,
    )

    assert len(plans) == 1
    assert plans[0].decision.decision == "prompt"
    assert plans[0].ready_to_apply is False
    assert plans[0].update is None
    assert plans[0].audit["requires_confirmation"] is True


def test_condition_sop_invocation_plan_respects_rf_guard_blocks() -> None:
    plans = plan_condition_sop_invocations(
        [_condition_observation(action="auto-apply", level=5)],
        settings_data={"operating_groups": [{"group": "MAGNET", "condition_level": 2}]},
        sop_profiles=[
            {
                "id": 9,
                "name": "MagNet Emergency",
                "schedule_layer": [{"group_name": "MAGNET", "condition_levels": "5"}],
            }
        ],
        auto_apply_enabled=True,
        rf_guard_state_by_profile={"9": {"state": "blocked", "messages": ["RF Guard conflict"]}},
    )

    assert len(plans) == 1
    assert plans[0].decision.decision == "blocked"
    assert plans[0].ready_to_apply is False
    assert plans[0].update is None
    assert plans[0].audit["blocked"] is True
    assert plans[0].audit["reasons"] == ["RF Guard conflict"]


def test_schedule_layer_rows_for_condition_decision_filters_group_level_and_enabled() -> None:
    plans = plan_condition_sop_invocations(
        [_condition_observation(action="auto-apply", level=4)],
        settings_data={"operating_groups": [{"group": "MAGNET", "condition_level": 2}]},
        sop_profiles=[
            {
                "id": 7,
                "name": "MagNet Condition 4",
                "schedule_layer": [
                    {"group_name": "MAGNET", "condition_levels": "4", "band": "20M"},
                    {"group_name": "MAGNET", "condition_levels": "5", "band": "80M"},
                    {"group_name": "AMRRON", "condition_levels": "4", "band": "40M"},
                    {"group_name": "MAGNET", "condition_levels": "4", "band": "15M", "enabled": False},
                ],
            }
        ],
        auto_apply_enabled=True,
    )

    rows = schedule_layer_rows_for_condition_decision(
        {
            "id": 7,
            "name": "MagNet Condition 4",
            "schedule_layer": [
                {"group_name": "MAGNET", "condition_levels": "4", "band": "20M"},
                {"group_name": "MAGNET", "condition_levels": "5", "band": "80M"},
                {"group_name": "AMRRON", "condition_levels": "4", "band": "40M"},
                {"group_name": "MAGNET", "condition_levels": "4", "band": "15M", "enabled": False},
            ],
        },
        plans[0].decision,
    )

    assert rows == ({"group_name": "MAGNET", "condition_levels": "4", "band": "20M"},)
