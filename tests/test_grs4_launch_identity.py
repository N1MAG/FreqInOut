"""Focused GRS-4 launch identity and station-planner contracts."""

from __future__ import annotations

from freqinout.core.launch_orchestrator import LaunchOrchestrator
from freqinout.core.js8_storage import stable_managed_rig_name
from freqinout.core.station_launch_planner import StationLaunchPlanner


def _profile(*, radio_id: int = 1, observer: bool = False) -> dict[str, object]:
    return {
        "id": radio_id,
        "name": f"Radio {radio_id}",
        "device_class": "observer" if observer else "tx_rx",
        "runtime_active": 1,
        "display_order": radio_id,
        "js8_instance_system_key": f"js8-{radio_id}",
        "js8_instance_name": f"JS8 {radio_id}",
        "js8_port": 2441 + radio_id,
    }


def _item(
    name: str,
    key: str,
    *,
    path: str = "",
    command: str = "",
    startup: bool = True,
    readiness: dict[str, object] | None = None,
    dependencies: list[str] | None = None,
    scope: str = "standard",
) -> dict[str, object]:
    return {
        "name": name,
        "instance_key": key,
        "enabled": True,
        "startup": startup,
        "monitor_health": True,
        "launch_path_override": path,
        "launch_command_override": command,
        "dependencies": list(dependencies or ()),
        "readiness_policy": {"execution_scope": scope, **dict(readiness or {})},
        "execution_scope": scope,
    }


def test_review_keeps_recipe_startup_bundle_and_operator_state_distinct() -> None:
    profile = _profile()
    operator_item = _item(
        "FLMsg",
        "fast-light:shared:flmsg",
        startup=False,
        readiness={"operator_starts": True, "readiness": "operator_confirmed"},
    )
    bundles = {1: {"launch_enabled": False, "items": [operator_item]}}

    startup = StationLaunchPlanner().plan_startup([profile], bundles)
    review = StationLaunchPlanner().plan_review([profile], bundles, scope_radio_id=1)

    assert startup.instances == ()
    assert len(review.instances) == 1
    instance = review.instances[0]
    assert instance.known_recipe is True
    assert instance.startup_included is False
    assert instance.bundle_enabled is False
    assert instance.operator_starts is True
    assert instance.effective_command == ()
    assert instance.as_queue_item()["operator_starts"] is True


def test_fast_light_components_retain_per_component_exact_launch_identity() -> None:
    profile = _profile()
    items = [
        _item(
            "FLRig",
            "fast-light:one:flrig",
            path="/apps/one/flrig",
            readiness={
                "launch_arguments": ["--config-dir", "/profiles/one/flrig"],
                "working_directory": "/apps/one",
                "profile_selector": "one",
                "environment": {"FLDIGI_PROFILE": "one"},
            },
        ),
        _item(
            "FLDigi",
            "fast-light:one:fldigi",
            path="/apps/one/fldigi",
            dependencies=["FLRig"],
        ),
        _item("FLMsg", "fast-light:one:flmsg", path="/apps/one/flmsg", dependencies=["FLDigi"]),
        _item("FLAmp", "fast-light:one:flamp", path="/apps/one/flamp", dependencies=["FLDigi"]),
    ]

    plan = StationLaunchPlanner().plan_startup(
        [profile],
        {1: {"launch_enabled": True, "items": items}},
    )

    assert [instance.name for instance in plan.instances] == ["FLRig", "FLDigi", "FLMsg", "FLAmp"]
    assert len({instance.instance_identity for instance in plan.instances}) == 4
    flrig = plan.instances[0]
    assert flrig.launch_arguments == ("--config-dir", "/profiles/one/flrig")
    assert flrig.working_directory == "/apps/one"
    assert flrig.profile_selector == "one"
    assert dict(flrig.environment) == {"FLDIGI_PROFILE": "one"}


def test_review_exposes_legacy_flmsg_and_flamp_as_explicit_operator_start() -> None:
    profile = {
        **_profile(),
        "fast_light_system_key": "fast-one",
        "use_flmsg": 1,
        "use_flamp": 1,
        "flmsg_path": "/apps/flmsg",
        "flamp_path": "/apps/flamp",
    }

    review = StationLaunchPlanner().plan_review(
        [profile],
        {1: {"launch_enabled": True, "items": []}},
        scope_radio_id=1,
    )

    assert [instance.name for instance in review.instances] == ["FLMsg", "FLAmp"]
    assert all(instance.operator_starts for instance in review.instances)
    assert all(not instance.startup_included for instance in review.instances)
    assert {instance.instance_key for instance in review.instances} == {
        "fast-light:fast-one:flmsg",
        "fast-light:fast-one:flamp",
    }


def test_intentional_shared_identity_dedupes_but_distinct_keys_do_not() -> None:
    profiles = [_profile(radio_id=1), _profile(radio_id=2)]
    shared = _item("FLMsg", "fast-light:station-shared:flmsg", path="/apps/flmsg")
    shared_plan = StationLaunchPlanner().plan_startup(
        profiles,
        {
            1: {"launch_enabled": True, "items": [shared]},
            2: {"launch_enabled": True, "items": [shared]},
        },
    )
    assert len(shared_plan.instances) == 1
    assert shared_plan.instances[0].radio_ids == (1, 2)

    distinct_plan = StationLaunchPlanner().plan_startup(
        profiles,
        {
            1: {"launch_enabled": True, "items": [dict(shared, instance_key="fast-light:one:flmsg")]},
            2: {"launch_enabled": True, "items": [dict(shared, instance_key="fast-light:two:flmsg")]},
        },
    )
    assert len(distinct_plan.instances) == 2


def test_observer_sdrpp_and_js8_keep_receive_only_identity() -> None:
    profile = _profile(observer=True)
    items = [
        _item(
            "SDR++",
            "receiver:one:sdrpp",
            path="/usr/bin/sdrpp",
            scope="receive_only",
            readiness={"readiness": "process"},
        ),
        _item(
            "JS8Call",
            "js8:one",
            path="/usr/bin/js8call",
            scope="receive_only",
            readiness={"host": "127.0.0.1", "port": 2442, "require_api": True},
        ),
    ]

    plan = StationLaunchPlanner().plan_startup(
        [profile],
        {1: {"launch_enabled": True, "items": items}},
    )

    assert [instance.name for instance in plan.instances] == ["SDR++", "JS8Call"]
    assert {instance.execution_scope for instance in plan.instances} == {"receive_only"}
    assert plan.instances[0].instance_key == "receiver:one:sdrpp"
    assert plan.instances[1].rig_name


def test_js8_launch_uses_persisted_reviewed_rig_identity_not_system_key() -> None:
    profile = {
        **_profile(),
        "js8_instance_system_key": "js8call-ft-710-opaque-db-key",
        "js8_instance_name": "FT-710",
        "js8_rig_name": "FT-710",
        "js8_message_storage_root": "/operator/.local/share/JS8Call - FT-710",
    }
    item = _item("JS8Call", "js8:ft-710", path="/usr/bin/js8call-subspace")

    plan = StationLaunchPlanner().plan_startup(
        [profile],
        {1: {"launch_enabled": True, "items": [item]}},
    )

    instance = plan.instances[0]
    assert instance.rig_name == "FT-710"
    assert instance.rig_name_source == "persisted"
    assert instance.launch_arguments == ("--rig-name", "FT-710")
    assert instance.application_data_root == "/operator/.local/share/JS8Call - FT-710"
    assert "opaque-db-key" not in " ".join(instance.effective_command)


def test_legacy_default_js8_launch_keeps_native_profile_and_uses_radio_title() -> None:
    generated = stable_managed_rig_name(
        system_key="default_js8_instance",
        name="FTDX-10 JS8",
    )
    profile = {
        **_profile(),
        "name": "FTDX-10",
        "js8_instance_system_key": "default_js8_instance",
        "js8_instance_name": "FTDX-10 JS8",
        "js8_rig_name": generated,
        "js8_rig_name_source": "persisted",
        "js8_message_storage_root": "/home/operator/.local/share/JS8Call",
        "js8_storage_evidence": "runtime_verified:message_files",
    }
    item = _item("JS8Call", "JS8Call", path="/usr/bin/js8call-subspace")

    instance = StationLaunchPlanner().plan_startup(
        [profile],
        {1: {"launch_enabled": True, "items": [item]}},
    ).instances[0]
    queue_item = instance.as_queue_item()

    assert instance.launch_arguments == ()
    assert instance.rig_name == ""
    assert instance.rig_name_source == "legacy_default"
    assert instance.application_data_root.endswith("/home/operator/.local/share/JS8Call")
    assert LaunchOrchestrator._window_title_for_item(queue_item) == "JS8Call — FTDX-10"


def test_legacy_default_js8_preserves_operator_selected_rig_name() -> None:
    profile = {
        **_profile(),
        "name": "FTDX-10",
        "js8_instance_system_key": "default_js8_instance",
        "js8_instance_name": "FTDX-10 JS8",
        "js8_rig_name": "FTDX-10",
        "js8_message_storage_root": "/home/operator/.local/share/JS8Call - FTDX-10",
    }
    item = _item("JS8Call", "JS8Call", path="/usr/bin/js8call")

    instance = StationLaunchPlanner().plan_startup(
        [profile],
        {1: {"launch_enabled": True, "items": [item]}},
    ).instances[0]

    assert instance.launch_arguments == ("--rig-name", "FTDX-10")
    assert instance.rig_name_source == "persisted"
    assert LaunchOrchestrator._window_title_for_item(instance.as_queue_item()) == ""


def test_varac_exact_command_and_working_directory_reach_execution_preview() -> None:
    profile = _profile()
    command = 'wine "/opt/VarAC A/VarAC.exe" --profile "Node A"'
    item = _item(
        "VarAC",
        "varac:node-a",
        command=command,
        readiness={"working_directory": "/opt/VarAC A", "profile_selector": "Node A"},
    )
    plan = StationLaunchPlanner().plan_startup(
        [profile],
        {1: {"launch_enabled": True, "items": [item]}},
    )
    instance = plan.instances[0]
    assert instance.launch_command_override == command
    assert instance.working_directory == "/opt/VarAC A"
    assert instance.profile_selector == "Node A"

    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator.settings = type("Settings", (), {"get": lambda _self, _key, default=None: default})()
    resolved = orchestrator._with_effective_launch_preview(plan).instances[0]
    assert resolved.effective_command == ("wine", "/opt/VarAC A/VarAC.exe", "--profile", "Node A")
    assert orchestrator._infer_launch_cwd(
        "VarAC", list(resolved.effective_command), "radio launch command", resolved.as_queue_item()
    ) == "/opt/VarAC A"


def test_manual_and_startup_use_the_same_station_plan_except_scope() -> None:
    profiles = [_profile(radio_id=1), _profile(radio_id=2)]
    profiles[0]["flrig_port"] = 12345
    profiles[1]["flrig_port"] = 12346
    bundles = {
        1: {"launch_enabled": True, "items": [_item("FLRig", "fast:one:flrig", path="/a/flrig")]},
        2: {"launch_enabled": True, "items": [_item("FLRig", "fast:two:flrig", path="/b/flrig")]},
    }
    planner = StationLaunchPlanner()

    startup = planner.plan_startup(profiles, bundles, trigger="startup")
    manual = planner.plan_startup(profiles, bundles, scope_radio_id=2, trigger="manual")

    assert manual.instances == tuple(instance for instance in startup.instances if instance.radio_ids == (2,))
    assert manual.trigger == "manual"
    assert manual.scope_radio_id == 2
