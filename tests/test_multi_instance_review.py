from __future__ import annotations

from freqinout.core.multi_instance_review import (
    blocking_issue_message,
    build_multi_instance_adoption_plan,
    validate_multi_instance_launch_records,
)


def test_adoption_plan_is_read_only_and_guides_subspace_like_other_js8_instances() -> None:
    plan = build_multi_instance_adoption_plan(
        "js8call",
        (
            {"id": 2, "display_order": 1, "name": "FIO-B", "use_js8call": 1},
            {"id": 1, "display_order": 0, "name": "FIO-A", "use_js8call": 1, "js8_instance_id": 8},
        ),
        discovered=("/Applications/JS8Call Subspace.app",),
    )

    assert plan.requires_explicit_apply is True
    assert [target.radio_name for target in plan.targets] == ["FIO-A", "FIO-B"]
    assert plan.targets[0].state == "assigned"
    assert plan.targets[1].state == "discovery_available"
    assert "unique API port" in " ".join(plan.targets[1].guidance)
    assert "does not modify external application files" in " ".join(plan.review_notes)


def test_launch_preflight_blocks_conflicting_fast_light_endpoints() -> None:
    issues = validate_multi_instance_launch_records(
        (
            {
                "name": "FLRig",
                "instance_identity": "fast-a",
                "radio_ids": (1,),
                "radio_names": ("FIO-A",),
                "readiness_policy": {"host": "127.0.0.1", "port": 12345},
            },
            {
                "name": "FLRig",
                "instance_identity": "fast-b",
                "radio_ids": (2,),
                "radio_names": ("FIO-B",),
                "readiness_policy": {"host": "127.0.0.1", "port": 12345},
            },
        )
    )

    assert len(issues) == 1
    assert issues[0].code == "duplicate_flrig_endpoint"
    assert issues[0].radio_names == ("FIO-A", "FIO-B")
    assert "unique host/port" in blocking_issue_message(issues)


def test_launch_preflight_allows_deduped_shared_identity_but_blocks_varac_resources() -> None:
    shared = {
        "name": "JS8Call",
        "instance_identity": "same-js8",
        "radio_ids": (1, 2),
        "radio_names": ("FIO-A", "FIO-B"),
        "readiness_policy": {"host": "127.0.0.1", "port": 2442},
    }
    issues = validate_multi_instance_launch_records(
        (
            shared,
            {
                "name": "VarAC",
                "instance_identity": "varac-a",
                "radio_ids": (1,),
                "radio_names": ("FIO-A",),
                "configuration_paths": {"ini_path": "/varac/shared/VarAC.ini"},
            },
            {
                "name": "VarAC",
                "instance_identity": "varac-b",
                "radio_ids": (2,),
                "radio_names": ("FIO-B",),
                "configuration_paths": {"ini_path": "/varac/shared/VarAC.ini"},
            },
        )
    )

    assert [issue.code for issue in issues] == ["duplicate_varac_ini_path"]
    assert "backed-up explicit action" in issues[0].remediation
