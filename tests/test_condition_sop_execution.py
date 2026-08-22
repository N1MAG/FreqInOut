from __future__ import annotations

from typing import Any

from freqinout.core.condition_sop_audit import list_condition_sop_invocation_audit
from freqinout.core.condition_sop_execution import execute_condition_sop_invocation_plans
from freqinout.core.condition_sop_invocation import plan_condition_sop_invocations
from freqinout.core.observation_projection import Observation


class FakeSettings:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = dict(data)
        self.set_calls: list[tuple[str, Any]] = []
        self.reload_count = 0

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.set_calls.append((key, value))

    def reload(self) -> None:
        self.reload_count += 1


def _condition_observation(group: str = "MAGNET", *, action: str = "auto-apply", level: int = 4) -> Observation:
    return Observation(
        observation_id=f"condition_alert:{group.lower()}:{level}",
        source_family="condition_alert",
        source_ref="js8:77",
        source_app="JS8Call",
        received_utc="2026-08-21T18:30:00+00:00",
        event_utc="2026-08-21T18:30:00+00:00",
        from_call="N1MAG",
        to_target=f"@{group}",
        groups=(group,),
        observed_topics=("General Intel", "Comms"),
        operator_attention=True,
        status="CONDITION ALERT",
        urgency=f"LEVEL {level}",
        subject=f"{group} condition level {level}",
        summary=f"{group} condition level {level}",
        provenance={
            "rule_id": f"{group.lower()}-condition",
            "operating_group": group,
            "condition_level": level,
            "action": action,
        },
    )


def _profile(group: str = "MAGNET", *, level: int = 4, profile_id: int = 7) -> dict[str, Any]:
    return {
        "id": profile_id,
        "name": f"{group} Level {level}",
        "schedule_layer": [{"group_name": group, "condition_levels": str(level)}],
    }


def test_condition_sop_execution_applies_ready_plan_and_audits(tmp_path) -> None:
    settings = FakeSettings(
        {
            "operating_groups": [
                {"group": "MAGNET", "band": "20M", "condition_level": 2},
                {"group": "MAGNET", "band": "40M", "condition_level": 2},
            ]
        }
    )
    plans = plan_condition_sop_invocations(
        [_condition_observation()],
        settings_data=settings.data,
        sop_profiles=[_profile()],
        auto_apply_enabled=True,
    )

    result = execute_condition_sop_invocation_plans(settings, tmp_path / "fio.db", plans)

    assert result.applied_count == 1
    assert result.records[0].observation_id == "condition_alert:magnet:4"
    assert settings.reload_count == 1
    assert len(settings.set_calls) == 1
    assert [row["condition_level"] for row in settings.data["operating_groups"]] == [4, 4]
    audit = list_condition_sop_invocation_audit(tmp_path / "fio.db")
    assert audit[0]["status"] == "applied"
    assert audit[0]["payload"]["execution_status"] == "applied"


def test_condition_sop_execution_prompts_without_mutating_when_auto_gate_disabled(tmp_path) -> None:
    settings = FakeSettings({"operating_groups": [{"group": "MAGNET", "condition_level": 2}]})
    plans = plan_condition_sop_invocations(
        [_condition_observation()],
        settings_data=settings.data,
        sop_profiles=[_profile()],
        auto_apply_enabled=False,
    )

    result = execute_condition_sop_invocation_plans(settings, tmp_path / "fio.db", plans)

    assert result.applied_count == 0
    assert settings.set_calls == []
    assert settings.reload_count == 0
    audit = list_condition_sop_invocation_audit(tmp_path / "fio.db")
    assert audit[0]["status"] == "prompt"


def test_condition_sop_execution_audits_rf_guard_block_without_mutation(tmp_path) -> None:
    settings = FakeSettings({"operating_groups": [{"group": "MAGNET", "condition_level": 2}]})
    plans = plan_condition_sop_invocations(
        [_condition_observation()],
        settings_data=settings.data,
        sop_profiles=[_profile()],
        auto_apply_enabled=True,
        rf_guard_state_by_profile={"7": {"state": "blocked", "messages": ["antenna mismatch"]}},
    )

    result = execute_condition_sop_invocation_plans(settings, tmp_path / "fio.db", plans)

    assert result.applied_count == 0
    assert settings.set_calls == []
    audit = list_condition_sop_invocation_audit(tmp_path / "fio.db")
    assert audit[0]["status"] == "blocked"
    assert "antenna mismatch" in audit[0]["payload"]["execution_message"]


def test_condition_sop_execution_missing_group_fails_without_creating_group(tmp_path) -> None:
    settings = FakeSettings({"operating_groups": [{"group": "AMRRON", "condition_level": 2}]})
    plans = plan_condition_sop_invocations(
        [_condition_observation()],
        settings_data=settings.data,
        sop_profiles=[_profile()],
        auto_apply_enabled=True,
    )

    result = execute_condition_sop_invocation_plans(settings, tmp_path / "fio.db", plans)

    assert result.failed_count == 1
    assert settings.set_calls == []
    audit = list_condition_sop_invocation_audit(tmp_path / "fio.db")
    assert audit[0]["status"] == "failed"
    assert "not configured" in audit[0]["payload"]["execution_message"]


def test_condition_sop_execution_no_change_is_audited_but_not_saved(tmp_path) -> None:
    settings = FakeSettings({"operating_groups": [{"group": "MAGNET", "condition_level": 4, "use_condition_levels": True}]})
    plans = plan_condition_sop_invocations(
        [_condition_observation()],
        settings_data=settings.data,
        sop_profiles=[_profile()],
        auto_apply_enabled=True,
    )

    result = execute_condition_sop_invocation_plans(settings, tmp_path / "fio.db", plans)

    assert result.records[0].status == "no_change"
    assert settings.set_calls == []
    audit = list_condition_sop_invocation_audit(tmp_path / "fio.db")
    assert audit[0]["status"] == "no_change"


def test_condition_sop_execution_defers_additional_auto_plans_by_default(tmp_path) -> None:
    settings = FakeSettings(
        {
            "operating_groups": [
                {"group": "MAGNET", "condition_level": 2},
                {"group": "AMRRON", "condition_level": 2},
            ]
        }
    )
    plans = plan_condition_sop_invocations(
        [
            _condition_observation("MAGNET", level=4),
            _condition_observation("AMRRON", level=3),
        ],
        settings_data=settings.data,
        sop_profiles=[_profile("MAGNET", level=4, profile_id=7), _profile("AMRRON", level=3, profile_id=8)],
        auto_apply_enabled=True,
    )

    result = execute_condition_sop_invocation_plans(settings, tmp_path / "fio.db", plans)

    assert [record.status for record in result.records] == ["applied", "deferred"]
    assert [record.observation_id for record in result.records] == [
        "condition_alert:magnet:4",
        "condition_alert:amrron:3",
    ]
    assert settings.data["operating_groups"][0]["condition_level"] == 4
    assert settings.data["operating_groups"][1]["condition_level"] == 2
    audit = list_condition_sop_invocation_audit(tmp_path / "fio.db", limit=10)
    assert {row["status"] for row in audit} == {"applied", "deferred"}
