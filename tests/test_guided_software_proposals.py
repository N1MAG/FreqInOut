from dataclasses import replace

import pytest

from freqinout.core.guided_radio_software_model import (
    AtomicInstanceBundle, CompletionPolicy, EndpointRecord, ExecutionScope,
    GuidedRadioSoftwareValidationError, InstanceSourceMode, LaunchComponentRecord,
    ManagementMode, RadioRole, ResourceClaimRecord, SoftwareFamily,
)
from freqinout.core.guided_software_proposals import (
    CandidateState, CompleteBundleRequest, DiscoveredSoftwareCandidate,
    ExistingCandidateRequest, Js8DistinctProposalRequest, ProposalInventory,
    GuidedSoftwareProposalError, propose_distinct_js8, propose_existing_candidate,
    propose_complete_bundle, propose_fast_light_bundle, propose_receiver_bundle,
    propose_varac_bundle,
)


def _component(key, scope=ExecutionScope.RECEIVE_ONLY):
    return LaunchComponentRecord(key, executable=f"/bin/{key}", execution_scope=scope)


def _bundle(family, *, role=RadioRole.OBSERVER, key="instance", scope=ExecutionScope.RECEIVE_ONLY,
            owner="radio", components=(), resources=(), source=InstanceSourceMode.CREATE_DISTINCT,
            management=ManagementMode.FIO_MANAGED):
    return AtomicInstanceBundle(family, key, source, CompletionPolicy.REQUIRED, owner_radio_key=owner,
        management_mode=management, execution_scope=scope, launch_components=tuple(components), resources=tuple(resources))


def test_distinct_js8_allocates_2443_and_complete_distinct_identity():
    existing = propose_distinct_js8(Js8DistinctProposalRequest("old", "radio", RadioRole.TRANSCEIVER, "base", "1", "/managed", "/bin/js8"), ProposalInventory()).bundle
    inventory = ProposalInventory((existing,))
    proposal = propose_distinct_js8(Js8DistinctProposalRequest("new", "radio", RadioRole.TRANSCEIVER, "new", "1", "/managed", "/bin/js8"), inventory)
    assert proposal.allocated_port == 2443
    bundle = proposal.bundle
    assert bundle.configuration_path != existing.configuration_path
    assert set(bundle.message_paths).isdisjoint(existing.message_paths)
    assert bundle.identity_fingerprint != existing.identity_fingerprint
    assert {"native-profile", "application-data", "directed-txt", "all-txt", "message-inbox", "forms-root", "launch-identity"}.issubset({r.resource_key for r in bundle.resources})


def test_existing_candidate_imports_locked_whole_bundle_and_rejects_partial_or_ambiguous():
    source = _bundle(SoftwareFamily.JS8CALL, components=(_component("js8"),), scope=ExecutionScope.STANDARD)
    candidate = DiscoveredSoftwareCandidate("candidate", source, evidence=())
    proposal = propose_existing_candidate(ExistingCandidateRequest("req", "radio", RadioRole.TRANSCEIVER, candidate), ProposalInventory())
    assert proposal.imports_existing_candidate and proposal.bundle.source_mode is InstanceSourceMode.EXISTING
    assert proposal.bundle.source_fingerprint == proposal.bundle.identity_fingerprint
    with pytest.raises(GuidedSoftwareProposalError):
        propose_existing_candidate(ExistingCandidateRequest("req", "other", RadioRole.TRANSCEIVER, candidate), ProposalInventory())
    with pytest.raises(GuidedSoftwareProposalError):
        propose_existing_candidate(ExistingCandidateRequest("req", "radio", RadioRole.TRANSCEIVER, replace(candidate, state=CandidateState.AMBIGUOUS)), ProposalInventory())


def test_observer_fast_light_and_varac_rules():
    fast = _bundle(SoftwareFamily.FAST_LIGHT, components=(_component("fldigi"),), resources=(ResourceClaimRecord("external-tx-disabled", "capability", "true", exclusive=False),))
    assert propose_fast_light_bundle(CompleteBundleRequest("f", "radio", RadioRole.OBSERVER, fast), ProposalInventory()).bundle == fast
    bad_fast = replace(fast, launch_components=(_component("fldigi"), _component("flrig")), identity_fingerprint="", source_fingerprint="")
    with pytest.raises(GuidedSoftwareProposalError):
        propose_fast_light_bundle(CompleteBundleRequest("f", "radio", RadioRole.OBSERVER, bad_fast), ProposalInventory())
    varac = _bundle(SoftwareFamily.VARAC, role=RadioRole.TRANSCEIVER, scope=ExecutionScope.STANDARD, components=(_component("varac", ExecutionScope.STANDARD),), owner="radio")
    with pytest.raises(GuidedSoftwareProposalError):
        propose_varac_bundle(CompleteBundleRequest("v", "radio", RadioRole.OBSERVER, varac), ProposalInventory())


def test_receiver_requires_sdrpp_component_and_receive_scope():
    receiver = _bundle(SoftwareFamily.SDRPP, components=(_component("sdrpp"),))
    assert propose_receiver_bundle(CompleteBundleRequest("r", "radio", RadioRole.OBSERVER, receiver), ProposalInventory()).bundle == receiver
    with pytest.raises(GuidedSoftwareProposalError):
        propose_receiver_bundle(CompleteBundleRequest("r", "radio", RadioRole.OBSERVER, replace(receiver, launch_components=(), identity_fingerprint="", source_fingerprint="")), ProposalInventory())


def test_varac_cluster_requires_cluster_identity_claims_and_source_management_rules():
    resources = (ResourceClaimRecord("cluster-id", "cluster", "main"), ResourceClaimRecord("cluster-member-number", "cluster", "1"))
    cluster = _bundle(SoftwareFamily.VARAC_CLUSTER, role=RadioRole.TRANSCEIVER, scope=ExecutionScope.STANDARD, components=(_component("varac", ExecutionScope.STANDARD),), resources=resources, owner="radio")
    assert propose_varac_bundle(CompleteBundleRequest("c", "radio", RadioRole.TRANSCEIVER, cluster), ProposalInventory()).bundle == cluster
    missing = replace(cluster, resources=(), identity_fingerprint="", source_fingerprint="")
    with pytest.raises(GuidedSoftwareProposalError):
        propose_varac_bundle(CompleteBundleRequest("c", "radio", RadioRole.TRANSCEIVER, missing), ProposalInventory())
    manual = _bundle(SoftwareFamily.JS8CALL, scope=ExecutionScope.STANDARD, source=InstanceSourceMode.MANUAL_OR_REMOTE, management=ManagementMode.FIO_MANAGED)
    with pytest.raises(GuidedSoftwareProposalError):
        propose_complete_bundle(CompleteBundleRequest("m", "radio", RadioRole.TRANSCEIVER, manual), ProposalInventory())


def test_manual_remote_and_station_shared_proposals_preserve_their_ownership_scope():
    remote = _bundle(
        SoftwareFamily.JS8CALL,
        role=RadioRole.TRANSCEIVER,
        scope=ExecutionScope.REMOTE,
        source=InstanceSourceMode.MANUAL_OR_REMOTE,
        management=ManagementMode.REMOTE,
    )
    assert propose_complete_bundle(
        CompleteBundleRequest("remote", "radio", RadioRole.TRANSCEIVER, remote),
        ProposalInventory(),
    ).bundle == remote
    shared = _bundle(
        SoftwareFamily.FAST_LIGHT,
        scope=ExecutionScope.STATION_SHARED_UTILITY,
        owner="",
        source=InstanceSourceMode.SHARED_STATION_TOOL,
        management=ManagementMode.OPERATOR,
        components=(_component("flmsg", ExecutionScope.STATION_SHARED_UTILITY),),
        resources=(ResourceClaimRecord("external-tx-disabled", "capability", "true", exclusive=False),),
    )
    assert propose_fast_light_bundle(
        CompleteBundleRequest("shared", "radio", RadioRole.OBSERVER, shared),
        ProposalInventory(),
    ).bundle == shared
