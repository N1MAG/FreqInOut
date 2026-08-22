from pathlib import Path

from freqinout.core.condition_sop_policy import (
    condition_levels_include,
    evaluate_condition_sop_invocation,
    evaluate_condition_sop_invocations,
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


def test_condition_sop_policy_prompts_for_prompt_rules_with_matching_layer() -> None:
    decision = evaluate_condition_sop_invocation(
        _condition_observation(action="prompt-to-apply", level=3),
        sop_layers=[
            {
                "profile_id": 12,
                "profile_name": "MagNet Yellow SOP",
                "group_name": "MAGNET",
                "condition_levels": "3",
            }
        ],
    )

    assert decision.decision == "prompt"
    assert decision.requires_confirmation is True
    assert decision.should_apply is False
    assert decision.observation_id == "condition_alert:magnet:3"
    assert decision.operating_group == "MAGNET"
    assert decision.condition_level == 3
    assert decision.sop_profile_name == "MagNet Yellow SOP"


def test_condition_sop_policy_only_applies_auto_when_operator_enabled_it() -> None:
    observation = _condition_observation(action="auto-apply", level=4)
    layers = [
        {
            "profile_id": "mag4",
            "profile_name": "MagNet Red SOP",
            "group_name": "MAGNET",
            "condition_levels": "4,5",
        }
    ]

    disabled = evaluate_condition_sop_invocation(observation, sop_layers=layers, auto_apply_enabled=False)
    enabled = evaluate_condition_sop_invocation(observation, sop_layers=layers, auto_apply_enabled=True)

    assert disabled.decision == "prompt"
    assert disabled.requires_confirmation is True
    assert enabled.decision == "apply"
    assert enabled.should_apply is True
    assert enabled.requires_confirmation is False


def test_condition_sop_policy_blocks_when_rf_guard_blocks_matching_layer() -> None:
    decision = evaluate_condition_sop_invocation(
        _condition_observation(action="auto-apply", level=5),
        sop_layers=[
            {
                "profile_id": "mag5",
                "profile_name": "MagNet Emergency SOP",
                "group_name": "MAGNET",
                "condition_levels": "5",
            }
        ],
        auto_apply_enabled=True,
        rf_guard_state={"state": "blocked", "messages": ["80M unsupported by antenna"]},
    )

    assert decision.decision == "blocked"
    assert decision.blocked is True
    assert decision.should_apply is False
    assert decision.reasons == ("80M unsupported by antenna",)


def test_condition_sop_policy_blocks_without_matching_sop_layer() -> None:
    decision = evaluate_condition_sop_invocation(
        _condition_observation(action="auto-apply", level=2),
        sop_layers=[
            {
                "profile_id": "mag4",
                "profile_name": "MagNet Red SOP",
                "group_name": "MAGNET",
                "condition_levels": "4,5",
            }
        ],
        auto_apply_enabled=True,
    )

    assert decision.decision == "blocked"
    assert decision.blocked is True
    assert "no SOP layer matches MAGNET condition level 2" in decision.reasons


def test_condition_sop_policy_ignores_non_condition_observations() -> None:
    decision = evaluate_condition_sop_invocation(
        {
            "source_family": "spotter",
            "from_call": "N1MAG",
            "to_target": "@MAGNET",
            "summary": "MAGCON+3",
        },
        sop_layers=[],
        auto_apply_enabled=True,
    )

    assert decision.decision == "blocked"
    assert decision.reasons == ("not a condition alert",)


def test_condition_sop_policy_reads_provenance_json_from_stored_observation() -> None:
    observation = _condition_observation(action="auto-apply", level=4).as_record()
    observation.pop("provenance", None)

    decision = evaluate_condition_sop_invocation(
        observation,
        sop_layers=[{"group_name": "MAGNET", "condition_levels": "4", "profile_id": 7}],
        auto_apply_enabled=True,
    )

    assert decision.decision == "apply"
    assert decision.should_apply is True
    assert decision.operating_group == "MAGNET"


def test_condition_sop_policy_batch_evaluates_profile_schedule_layers() -> None:
    decisions = evaluate_condition_sop_invocations(
        [
            _condition_observation(action="suggest", level=2),
            _condition_observation(action="auto-apply", level=5),
        ],
        sop_profiles=[
            {
                "id": 7,
                "name": "MagNet Watch",
                "schedule_layer": [
                    {"group_name": "MAGNET", "condition_levels": "2"},
                ],
            },
            {
                "id": 9,
                "name": "MagNet Emergency",
                "schedule_layer": [
                    {"group_name": "MAGNET", "condition_levels": "5"},
                ],
            },
        ],
        auto_apply_enabled=True,
    )

    assert [decision.decision for decision in decisions] == ["suggest", "apply"]
    assert [decision.sop_profile_name for decision in decisions] == ["MagNet Watch", "MagNet Emergency"]


def test_condition_sop_policy_batch_applies_rf_guard_by_profile() -> None:
    decisions = evaluate_condition_sop_invocations(
        [_condition_observation(action="auto-apply", level=5)],
        sop_profiles=[
            {
                "id": 9,
                "name": "MagNet Emergency",
                "schedule_layer": [
                    {"group_name": "MAGNET", "condition_levels": "5"},
                ],
            },
        ],
        auto_apply_enabled=True,
        rf_guard_state_by_profile={"9": {"state": "blocked", "messages": ["RF overlap"]}},
    )

    assert len(decisions) == 1
    assert decisions[0].decision == "blocked"
    assert decisions[0].reasons == ("RF overlap",)


def test_condition_levels_include_matches_all_numeric_and_level_tokens() -> None:
    assert condition_levels_include("ALL", 3) is True
    assert condition_levels_include("2, L3, LEVEL4", 3) is True
    assert condition_levels_include("2, L3, LEVEL4", 4) is True
    assert condition_levels_include("2, L3, LEVEL4", 5) is False
    assert condition_levels_include("ALL", None) is False


def test_sop_builder_surfaces_condition_alert_suggestions_without_auto_apply() -> None:
    source = Path("freqinout/gui/sop_tab.py").read_text()

    assert "Traffic Suggestions" in source
    assert "refresh_traffic_suggestions" in source
    assert "Apply Level" in source
    assert "_apply_first_traffic_suggestion" in source
    assert "operational_activity_snapshot" in source
    assert "evaluate_condition_sop_invocations" in source
    assert "AUTO_SOP_INVOCATION_SETTING_KEY" in source
    assert "apply_operating_group_condition_level" in source
    assert "condition_sop_audit_summary" in source
    assert "condition_sop_audit_display" in source
    assert "Review Automation" in source
    assert "list_condition_sop_invocation_audit" in source
    assert "revert_condition_sop_audit_row" in source
    assert "append_condition_sop_invocation_audit" in source
    assert "schedule_layer_rows_for_condition_decision" in source
    assert "_traffic_suggestion_rf_guard_impacts" in source
    assert "RF Guard Blocks Condition Level" in source
    assert "auto_apply_enabled=auto_allowed" in source
    assert "QMessageBox.Apply | QMessageBox.Cancel" in source
