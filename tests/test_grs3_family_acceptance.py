"""GRS-3 acceptance contracts for family completion and safety boundaries.

These tests intentionally exercise the pure public seams.  Persistence and
launch adapters must consume these same complete plans; no test reaches into a
database or starts an application.
"""

from dataclasses import replace

import pytest

from freqinout.core.guided_family_completion import (
    AdvancedFastLightTxAcknowledgement,
    GuidedFamilyCompletionError,
    configure_fast_light_instance,
    configure_manual_js8_instance,
    create_js8_instance,
    import_existing_js8_instance,
    validate_fast_light_final_preflight,
)
from freqinout.core.guided_radio_software_model import (
    AllowedState,
    AtomicInstanceBundle,
    CompletionPolicy,
    ExecutionScope,
    GuidedRadioSoftwareDraft,
    InstanceSourceMode,
    LaunchComponentRecord,
    ManagementMode,
    RadioRole,
    ResourceClaimRecord,
    SoftwareFamily,
    SoftwareSelection,
    atomic_bundle_from_mapping,
    atomic_bundle_to_mapping,
    capability_for,
)
from freqinout.core.guided_software_proposals import (
    CompleteBundleRequest,
    DiscoveredSoftwareCandidate,
    ExistingCandidateRequest,
    Js8DistinctProposalRequest,
    ProposalInventory,
)
from freqinout.core.guided_varac_configuration import (
    GuidedVarACConfigurationError,
    VarACClusterIdentity,
    VarACClusterMembership,
    VarACConfigurationRequest,
    VarACClusterPath,
    VarACNodeIdentity,
    VarACPlanningInventory,
    plan_varac_configuration,
)


def _component(key: str, scope: ExecutionScope, *args: str) -> LaunchComponentRecord:
    return LaunchComponentRecord(key, executable=f"/apps/{key}", arguments=args, execution_scope=scope)


def _fast_light(role=RadioRole.OBSERVER, *, source=InstanceSourceMode.CREATE_DISTINCT, owner="radio-a", components=None, resources=None):
    scope = ExecutionScope.RECEIVE_ONLY if role == RadioRole.OBSERVER else ExecutionScope.STANDARD
    if source == InstanceSourceMode.SHARED_STATION_TOOL:
        scope, owner = ExecutionScope.STATION_SHARED_UTILITY, ""
    return AtomicInstanceBundle(
        family=SoftwareFamily.FAST_LIGHT,
        instance_key="fast-light-a", source_mode=source,
        completion_policy=CompletionPolicy.REQUIRED, owner_radio_key=owner,
        management_mode=ManagementMode.FIO_MANAGED, execution_scope=scope,
        resources=resources or (ResourceClaimRecord("external-tx-disabled", "safety", "true", exclusive=False),),
        launch_components=components or (_component("fldigi", scope),),
    )


def _node(key="north", radio="radio-north", root="/varac/north", command="wine VarAC.exe"):
    return VarACNodeIdentity(
        node_key=key, radio_key=radio, install_path=f"{root}/install", launch_command=command,
        working_directory=f"{root}/work", ini_path=f"{root}/VarAC.ini", database_path=f"{root}/VarAC.db",
        incoming_path=f"{root}/incoming", outbox_path=f"{root}/outbox",
    )


def test_grs3_capability_matrix_enforces_family_role_cells():
    assert capability_for(RadioRole.OBSERVER, SoftwareFamily.JS8CALL).allowed_state is AllowedState.ALLOWED
    assert capability_for(RadioRole.OBSERVER, SoftwareFamily.FAST_LIGHT).execution_scope is ExecutionScope.RECEIVE_ONLY
    assert capability_for(RadioRole.OBSERVER, SoftwareFamily.VARAC).allowed_state is AllowedState.NOT_ALLOWED
    assert capability_for(RadioRole.OBSERVER, SoftwareFamily.VARAC_CLUSTER).allowed_state is AllowedState.NOT_ALLOWED
    assert capability_for(RadioRole.TRANSCEIVER, SoftwareFamily.VARAC).allowed_state is AllowedState.ALLOWED
    assert capability_for(RadioRole.TRANSCEIVER, SoftwareFamily.FAST_LIGHT).advanced_tx_available


def test_js8_new_import_manual_are_atomic_and_new_port_cannot_reuse_identity():
    first = create_js8_instance(
        Js8DistinctProposalRequest("first", "radio-a", RadioRole.TRANSCEIVER, "stock", "2.2", "/managed", "/apps/js8"),
        ProposalInventory(),
    ).bundle
    second_proposal = create_js8_instance(
        Js8DistinctProposalRequest("second", "radio-b", RadioRole.OBSERVER, "subspace", "2.2", "/managed", "/apps/js8"),
        ProposalInventory((first,)),
    )
    second = second_proposal.bundle
    assert second_proposal.allocated_port == 2443
    assert second.configuration_path != first.configuration_path and second.data_path != first.data_path
    assert second.endpoints[0].port == 2443 and second.execution_scope is ExecutionScope.RECEIVE_ONLY

    imported = import_existing_js8_instance(
        ExistingCandidateRequest("import", "radio-a", RadioRole.TRANSCEIVER, DiscoveredSoftwareCandidate("first", first)),
        ProposalInventory((first,)),
    ).bundle
    assert imported.source_locked and imported.identity_fingerprint == first.identity_fingerprint

    manual = replace(first, source_mode=InstanceSourceMode.MANUAL_OR_REMOTE, management_mode=ManagementMode.OPERATOR,
                     instance_key="manual", identity_fingerprint="", source_fingerprint="")
    assert configure_manual_js8_instance(
        CompleteBundleRequest("manual", "radio-a", RadioRole.TRANSCEIVER, manual), ProposalInventory()
    ).bundle.source_mode is InstanceSourceMode.MANUAL_OR_REMOTE


def test_js8_bundle_collision_rejects_mixing_new_port_with_existing_profile():
    existing = create_js8_instance(
        Js8DistinctProposalRequest("first", "radio-a", RadioRole.TRANSCEIVER, "stock", "2.2", "/managed", "/apps/js8"),
        ProposalInventory(),
    ).bundle
    mixed = replace(existing, instance_key="new", owner_radio_key="radio-b", source_mode=InstanceSourceMode.MANUAL_OR_REMOTE,
                    management_mode=ManagementMode.OPERATOR,
                    source_fingerprint="", identity_fingerprint="", endpoints=(replace(existing.endpoints[0], port=2443),))
    with pytest.raises(Exception, match="conflict|owned|available"):
        configure_manual_js8_instance(CompleteBundleRequest("mixed", "radio-b", RadioRole.TRANSCEIVER, mixed), ProposalInventory((existing,)))


def test_fast_light_observer_is_receive_only_at_scope_and_import_boundaries():
    good = _fast_light()
    assert configure_fast_light_instance(CompleteBundleRequest("good", "radio-a", RadioRole.OBSERVER, good), ProposalInventory()).bundle == good
    for bad in (
        replace(good, launch_components=(_component("flrig", ExecutionScope.RECEIVE_ONLY),), identity_fingerprint="", source_fingerprint=""),
        replace(good, resources=good.resources + (ResourceClaimRecord("cat-transmit-enabled", "setting", "true", exclusive=False),), identity_fingerprint="", source_fingerprint=""),
        replace(good, launch_components=(_component("fldigi", ExecutionScope.RECEIVE_ONLY, "--enable-ptt"),), identity_fingerprint="", source_fingerprint=""),
    ):
        with pytest.raises(GuidedFamilyCompletionError):
            configure_fast_light_instance(CompleteBundleRequest("bad", "radio-a", RadioRole.OBSERVER, bad), ProposalInventory())


def test_fast_light_transceiver_advanced_tx_requires_bound_durable_ack():
    bundle = _fast_light(RadioRole.TRANSCEIVER, owner="radio-a", components=(_component("fldigi", ExecutionScope.STANDARD), _component("flrig", ExecutionScope.STANDARD)))
    model, guard = "a" * 64, "b" * 64
    ack = AdvancedFastLightTxAcknowledgement("ack", "radio-a", bundle.identity_fingerprint, model, guard)
    assert validate_fast_light_final_preflight(radio_key="radio-a", radio_role=RadioRole.TRANSCEIVER, bundle=bundle, acknowledgement=ack, operating_model_fingerprint=model, rf_guard_fingerprint=guard) is ack
    changed_bundle = replace(bundle, data_path="/changed", identity_fingerprint="", source_fingerprint="")
    for role, candidate_bundle, changed_model in ((RadioRole.OBSERVER, bundle, model), (RadioRole.TRANSCEIVER, changed_bundle, model), (RadioRole.TRANSCEIVER, bundle, "c" * 64)):
        with pytest.raises(GuidedFamilyCompletionError, match="receive-only|stale"):
            validate_fast_light_final_preflight(radio_key="radio-a", radio_role=role, bundle=candidate_bundle, acknowledgement=ack, operating_model_fingerprint=changed_model, rf_guard_fingerprint=guard)


def test_direct_mapping_boundary_rejects_injected_observer_varac():
    injected = AtomicInstanceBundle(family=SoftwareFamily.VARAC, instance_key="v", source_mode=InstanceSourceMode.MANUAL_OR_REMOTE,
                                    completion_policy=CompletionPolicy.REQUIRED, owner_radio_key="sdr", management_mode=ManagementMode.OPERATOR,
                                    execution_scope=ExecutionScope.STANDARD)
    draft = GuidedRadioSoftwareDraft("sdr", RadioRole.OBSERVER, (SoftwareSelection(SoftwareFamily.VARAC, True, bundle=injected),))
    assert any(issue.code == "family-not-allowed" for issue in draft.validation_issues)
    restored = atomic_bundle_from_mapping(atomic_bundle_to_mapping(injected))
    assert restored.family is SoftwareFamily.VARAC


def test_varac_standalone_create_join_and_exact_launch_identity():
    standalone = plan_varac_configuration(VarACConfigurationRequest("s", "tx_rx", _node(command='wine start "/opt/VarAC.exe" --profile N'), VarACClusterPath.STANDALONE), VarACPlanningInventory())
    assert standalone.membership is None and standalone.node.launch_command == 'wine start "/opt/VarAC.exe" --profile N'
    cluster = VarACClusterIdentity("Front Range", "/varac/shared/cluster.db", counter_refresh_seconds=15, ptt_lock_enabled=True)
    created = plan_varac_configuration(VarACConfigurationRequest("c", "tx_rx", _node(), VarACClusterPath.CREATE_CLUSTER, cluster=cluster, instance_number=1, gateway_for_new_cluster=True), VarACPlanningInventory())
    assert created.cluster is cluster and created.membership.enabled and created.gateway_node_key == created.node.node_key
    joined = plan_varac_configuration(VarACConfigurationRequest("j", "tx_rx", _node("south", "radio-south", "/varac/south"), VarACClusterPath.JOIN_CLUSTER, join_cluster_id="front range", instance_number=2), VarACPlanningInventory(clusters=(cluster,), memberships=(created.membership,)))
    assert joined.membership.instance_number == 2


def test_varac_cluster_and_node_collisions_fail_before_mutation_and_replacement_is_scoped():
    old = _node()
    inventory = VarACPlanningInventory(nodes=(old,), clusters=(VarACClusterIdentity("Net A"),), memberships=(VarACClusterMembership("Net A", old.node_key, old.radio_key, 1),))
    before = inventory
    with pytest.raises(GuidedVarACConfigurationError, match="already assigned"):
        plan_varac_configuration(VarACConfigurationRequest("dup", "tx_rx", _node("south", "radio-south", "/varac/south"), VarACClusterPath.JOIN_CLUSTER, join_cluster_id="net a", instance_number=1), inventory)
    with pytest.raises(GuidedVarACConfigurationError, match="already in use"):
        plan_varac_configuration(VarACConfigurationRequest("dup", "tx_rx", _node("south", "radio-south", "/varac/south"), VarACClusterPath.CREATE_CLUSTER, cluster=VarACClusterIdentity("NET A"), instance_number=2), inventory)
    replacement = plan_varac_configuration(VarACConfigurationRequest("replace", "tx_rx", _node("new", "radio-north", "/varac/new"), VarACClusterPath.STANDALONE, replace_node_key="north"), inventory)
    assert replacement.node.radio_key == old.radio_key and inventory == before
    with pytest.raises(GuidedVarACConfigurationError, match="retain the owning radio"):
        plan_varac_configuration(VarACConfigurationRequest("bad", "tx_rx", _node("new", "other", "/varac/new"), VarACClusterPath.STANDALONE, replace_node_key="north"), inventory)


@pytest.mark.parametrize("path", [VarACClusterPath.STANDALONE, VarACClusterPath.CREATE_CLUSTER, VarACClusterPath.JOIN_CLUSTER])
def test_varac_observer_rejected_before_any_resource_or_launch_plan(path):
    kwargs = {}
    if path is VarACClusterPath.CREATE_CLUSTER:
        kwargs = {"cluster": VarACClusterIdentity("Net"), "instance_number": 1}
    elif path is VarACClusterPath.JOIN_CLUSTER:
        kwargs = {"join_cluster_id": "Net", "instance_number": 1}
    with pytest.raises(GuidedVarACConfigurationError, match="not available to observer"):
        plan_varac_configuration(VarACConfigurationRequest("observer", RadioRole.OBSERVER, _node(), path, **kwargs), VarACPlanningInventory())


def test_varac_shared_database_cannot_replace_node_local_database():
    node = _node()
    request = VarACConfigurationRequest("unsafe", "tx_rx", node, VarACClusterPath.CREATE_CLUSTER, cluster=VarACClusterIdentity("Net", node.database_path), instance_number=1)
    with pytest.raises(GuidedVarACConfigurationError, match="cannot replace a node-local path"):
        plan_varac_configuration(request, VarACPlanningInventory())
