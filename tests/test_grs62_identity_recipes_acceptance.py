"""GRS-6.2 acceptance contracts for managed identities and launch recipes.

These tests deliberately exercise pure proposal/profile/planner APIs.  They
are also useful as contract tests while platform-specific writer/route seams
are completed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from freqinout.core.config_autodiscovery import build_radio_instance_proposals
from freqinout.core.config_js8_managed import build_js8call_managed_profile_plans
from freqinout.core.config_managed_profiles import build_flrig_fldigi_managed_profile_plans
from freqinout.core.guided_radio_software_model import (
    AtomicInstanceBundle,
    CompletionPolicy,
    ExecutionScope,
    InstanceSourceMode,
    LaunchComponentRecord,
    ManagementMode,
    NativeWriterCapability,
    NativeWriterOperation,
    NativeWriterRegistry,
    RadioRole,
    ResourceClaimRecord,
    SoftwareFamily,
)
from freqinout.core.guided_software_proposals import (
    CompleteBundleRequest,
    ProposalInventory,
    propose_fast_light_bundle,
)
from freqinout.core.station_launch_planner import StationLaunchPlanner


@pytest.mark.parametrize("variant", ["stock", "improved", "subspace"])
@pytest.mark.parametrize("platform", ["Linux", "Darwin", "Windows"])
def test_js8_variants_have_platform_managed_roots_and_dual_protocol_identity(
    tmp_path: Path, variant: str, platform: str
) -> None:
    radios = build_radio_instance_proposals(radio_count=2, enabled_apps=("js8call",), busy_checker=lambda *_: False)
    plans = build_js8call_managed_profile_plans(
        radios,
        config_root=tmp_path / variant,
        platform=platform,
        storage_home=tmp_path / "home",
        js8call_path="/opt/js8call",
    )
    assert len(plans) == 2
    assert len({p.settings_path for p in plans}) == 2
    assert len({p.application_data_root for p in plans}) == 2
    assert len({p.rig_name for p in plans}) == 2
    assert len({(p.tcp_host, p.tcp_port) for p in plans}) == 2
    assert len({(p.tcp_host, p.udp_port) for p in plans}) == 2
    for plan in plans:
        assert plan.settings_path.parent == plan.config_dir
        assert plan.settings_path.name == f"{plan.application_name}.ini"
        assert "managed-instances" not in str(plan.settings_path)
        assert plan.settings["TCPServerPort"] == str(plan.tcp_port)
        assert plan.settings["UDPServerPort"] == str(plan.udp_port)
        assert plan.settings["SaveDir"] == str(plan.save_dir)
        assert plan.rig_name in plan.application_name


def test_fast_light_transceiver_is_fldigi_after_flrig_and_observer_is_fldigi_only(tmp_path: Path) -> None:
    tx = build_radio_instance_proposals(radio_count=1, enabled_apps=("flrig", "fldigi"), busy_checker=lambda *_: False)[0]
    tx_plans = build_flrig_fldigi_managed_profile_plans(tx, config_root=tmp_path / "tx")
    assert [p.app_id for p in tx_plans] == ["flrig", "fldigi"]
    assert tx_plans[0].config_dir != tx_plans[1].config_dir
    assert tx_plans[1].settings["flrig_port"] == str(tx_plans[0].expected_port)
    assert "--config-dir" in tx_plans[1].launch_args

    observer = build_radio_instance_proposals(radio_count=1, enabled_apps=("fldigi",), busy_checker=lambda *_: False)[0]
    observer_plans = build_flrig_fldigi_managed_profile_plans(observer, config_root=tmp_path / "observer")
    assert [p.app_id for p in observer_plans] == ["fldigi"]
    assert observer_plans[0].config_dir != tx_plans[1].config_dir


def _shared_bundle() -> AtomicInstanceBundle:
    return AtomicInstanceBundle(
        family=SoftwareFamily.FAST_LIGHT,
        instance_key="fast-light:station:shared",
        source_mode=InstanceSourceMode.SHARED_STATION_TOOL,
        completion_policy=CompletionPolicy.REQUIRED,
        owner_radio_key="",
        management_mode=ManagementMode.OPERATOR,
        execution_scope=ExecutionScope.STATION_SHARED_UTILITY,
        launch_components=(
            LaunchComponentRecord("flmsg", "/opt/flmsg", execution_scope=ExecutionScope.STATION_SHARED_UTILITY),
            LaunchComponentRecord("flamp", "/opt/flamp", execution_scope=ExecutionScope.STATION_SHARED_UTILITY),
        ),
        resources=(
            ResourceClaimRecord("station-utility", "identity", "flmsg+flamp"),
            ResourceClaimRecord("external-tx-disabled", "capability", "true", exclusive=False),
        ),
    )


def test_flmsg_flamp_are_one_explicit_shared_station_utility() -> None:
    bundle = _shared_bundle()
    proposal = propose_fast_light_bundle(
        CompleteBundleRequest("shared", "radio-a", RadioRole.OBSERVER, bundle), ProposalInventory()
    )
    assert proposal.bundle.owner_radio_key == ""
    assert {c.component_key for c in proposal.bundle.launch_components} == {"flmsg", "flamp"}
    assert proposal.bundle.execution_scope is ExecutionScope.STATION_SHARED_UTILITY


def test_qualified_recipe_is_exact_and_does_not_need_custom_launch_command() -> None:
    capability = NativeWriterCapability(
        "js8-stock-linux",
        SoftwareFamily.JS8CALL,
        "stock",
        "2.2",
        "linux",
        NativeWriterOperation.CREATE,
        preview_supported=True,
        backup_supported=True,
        readback_supported=True,
        restore_supported=True,
    )
    registry = NativeWriterRegistry((capability,))
    recipe = registry.lookup(family="js8call", variant="stock", version="2.2", platform="linux", operation="create")
    assert recipe is capability
    assert recipe.writer_key and recipe.variant == "stock"


def test_js8_known_recipe_derives_effective_rig_argument_without_custom_command() -> None:
    profile = {
        "id": 1,
        "name": "Radio A",
        "device_class": "tx_rx",
        "runtime_active": 1,
        "display_order": 1,
        "js8_instance_system_key": "js8-radio-a",
        "js8_instance_name": "JS8 Radio A",
        "js8_port": 2442,
    }
    item = {
        "name": "JS8Call",
        "instance_key": "js8-radio-a",
        "enabled": True,
        "startup": True,
        "monitor_health": True,
        "launch_path_override": "/opt/js8call",
        "launch_command_override": "",
        "dependencies": [],
        "readiness_policy": {"execution_scope": "standard"},
        "execution_scope": "standard",
    }
    planned = StationLaunchPlanner().plan_startup([profile], {1: {"launch_enabled": True, "items": [item]}}).instances[0]
    assert planned.launch_command_override == ""
    assert planned.launch_arguments[:1] == ("--rig-name",)
    assert planned.rig_name
    assert planned.launch_arguments[1] == planned.rig_name


def test_unknown_recipe_fails_closed_to_operator_action_without_mutation(tmp_path: Path) -> None:
    target = tmp_path / "JS8Call.ini"
    target.write_text("[Configuration]\nTCPServerPort=2442\n", encoding="utf-8")
    before = target.read_bytes()
    registry = NativeWriterRegistry()
    assert registry.lookup(family="js8call", variant="future", version="99.0", platform="linux", operation="create") is None
    assert not registry.supports(family="js8call", variant="future", version="99.0", platform="linux", operation="update")
    assert target.read_bytes() == before
