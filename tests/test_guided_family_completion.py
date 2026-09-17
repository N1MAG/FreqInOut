from dataclasses import replace

import pytest

from freqinout.core.guided_family_completion import (
    AdvancedFastLightTxAcknowledgement,
    GuidedFamilyCompletionError,
    configure_fast_light_instance,
    configure_manual_js8_instance,
    create_js8_instance,
    import_existing_fast_light_instance,
    import_existing_js8_instance,
    validate_fast_light_final_preflight,
)
from freqinout.core.guided_radio_software_model import (
    AtomicInstanceBundle,
    CompletionPolicy,
    ExecutionScope,
    InstanceSourceMode,
    LaunchComponentRecord,
    ManagementMode,
    RadioRole,
    ResourceClaimRecord,
    SoftwareFamily,
)
from freqinout.core.guided_software_proposals import (
    CompleteBundleRequest,
    DiscoveredSoftwareCandidate,
    ExistingCandidateRequest,
    Js8DistinctProposalRequest,
    ProposalInventory,
)


def _component(key, scope, *arguments):
    return LaunchComponentRecord(key, executable=f"/apps/{key}", arguments=arguments, execution_scope=scope)


def _fast_light(*, role=RadioRole.OBSERVER, source=InstanceSourceMode.CREATE_DISTINCT, owner="sdr-a", components=None, resources=None):
    scope = ExecutionScope.RECEIVE_ONLY if role == RadioRole.OBSERVER else ExecutionScope.STANDARD
    if source == InstanceSourceMode.SHARED_STATION_TOOL:
        scope, owner = ExecutionScope.STATION_SHARED_UTILITY, ""
    return AtomicInstanceBundle(
        family=SoftwareFamily.FAST_LIGHT,
        instance_key="fast-a",
        source_mode=source,
        completion_policy=CompletionPolicy.REQUIRED,
        owner_radio_key=owner,
        management_mode=ManagementMode.FIO_MANAGED,
        execution_scope=scope,
        resources=resources if resources is not None else (ResourceClaimRecord("external-tx-disabled", "safety", "true", exclusive=False),),
        launch_components=components if components is not None else (_component("fldigi", scope),),
    )


def _manual_js8():
    return AtomicInstanceBundle(
        family=SoftwareFamily.JS8CALL,
        instance_key="manual-js8",
        source_mode=InstanceSourceMode.MANUAL_OR_REMOTE,
        completion_policy=CompletionPolicy.REQUIRED,
        owner_radio_key="radio-a",
        management_mode=ManagementMode.OPERATOR,
        execution_scope=ExecutionScope.STANDARD,
        configuration_path="/profiles/manual",
        data_path="/data/manual",
        message_paths=("/data/manual/DIRECTED.TXT",),
        launch_components=(_component("js8call", ExecutionScope.STANDARD),),
    )


def test_js8_create_import_and_manual_paths_are_whole_bundle_operations():
    existing = create_js8_instance(
        Js8DistinctProposalRequest("first", "radio-a", RadioRole.TRANSCEIVER, "stock", "2.2", "/managed", "/apps/js8"),
        ProposalInventory(),
    ).bundle
    created = create_js8_instance(
        Js8DistinctProposalRequest("second", "radio-b", RadioRole.OBSERVER, "subspace", "2.2", "/managed", "/apps/js8"),
        ProposalInventory((existing,)),
    )
    assert created.allocated_port == 2443
    assert created.bundle.configuration_path != existing.configuration_path
    assert created.bundle.data_path != existing.data_path
    assert created.bundle.execution_scope == ExecutionScope.RECEIVE_ONLY

    candidate = DiscoveredSoftwareCandidate("existing-js8", existing)
    imported = import_existing_js8_instance(
        ExistingCandidateRequest("import", "radio-a", RadioRole.TRANSCEIVER, candidate), ProposalInventory((existing,))
    )
    assert imported.bundle.source_locked
    assert imported.bundle.identity_fingerprint == existing.identity_fingerprint
    assert configure_manual_js8_instance(
        CompleteBundleRequest("manual", "radio-a", RadioRole.TRANSCEIVER, _manual_js8()), ProposalInventory()
    ).bundle.source_mode == InstanceSourceMode.MANUAL_OR_REMOTE


def test_js8_manual_rejects_an_identity_source_mismatch():
    invalid = replace(_manual_js8(), source_mode=InstanceSourceMode.CREATE_DISTINCT, identity_fingerprint="", source_fingerprint="")
    with pytest.raises(GuidedFamilyCompletionError, match="manual or remote source"):
        configure_manual_js8_instance(CompleteBundleRequest("m", "radio-a", RadioRole.TRANSCEIVER, invalid), ProposalInventory())


def test_observer_fast_light_is_fail_closed_for_frig_scope_and_injected_tx_settings():
    good = _fast_light()
    assert configure_fast_light_instance(CompleteBundleRequest("good", "sdr-a", RadioRole.OBSERVER, good), ProposalInventory()).bundle == good

    flrig = replace(good, launch_components=(_component("fldigi", ExecutionScope.RECEIVE_ONLY), _component("flrig", ExecutionScope.RECEIVE_ONLY)), identity_fingerprint="", source_fingerprint="")
    with pytest.raises(GuidedFamilyCompletionError, match="FLRig/CAT/PTT"):
        configure_fast_light_instance(CompleteBundleRequest("flrig", "sdr-a", RadioRole.OBSERVER, flrig), ProposalInventory())

    injected = replace(good, resources=good.resources + (ResourceClaimRecord("cat-transmit-enabled", "setting", "true", exclusive=False),), identity_fingerprint="", source_fingerprint="")
    with pytest.raises(GuidedFamilyCompletionError, match="transmit settings"):
        configure_fast_light_instance(CompleteBundleRequest("tx", "sdr-a", RadioRole.OBSERVER, injected), ProposalInventory())

    argument = replace(good, launch_components=(_component("fldigi", ExecutionScope.RECEIVE_ONLY, "--enable-ptt"),), identity_fingerprint="", source_fingerprint="")
    with pytest.raises(GuidedFamilyCompletionError, match="transmit settings"):
        configure_fast_light_instance(CompleteBundleRequest("arg", "sdr-a", RadioRole.OBSERVER, argument), ProposalInventory())


def test_shared_fast_light_utility_has_no_radio_authority_or_automatic_tx():
    shared = _fast_light(
        source=InstanceSourceMode.SHARED_STATION_TOOL,
        components=(_component("flmsg", ExecutionScope.STATION_SHARED_UTILITY),),
    )
    assert configure_fast_light_instance(CompleteBundleRequest("shared", "sdr-a", RadioRole.OBSERVER, shared), ProposalInventory()).bundle == shared
    bad = replace(shared, resources=shared.resources + (ResourceClaimRecord("auto-send", "setting", "yes", exclusive=False),), identity_fingerprint="", source_fingerprint="")
    with pytest.raises(GuidedFamilyCompletionError, match="transmit settings"):
        configure_fast_light_instance(CompleteBundleRequest("shared-bad", "sdr-a", RadioRole.OBSERVER, bad), ProposalInventory())


def test_imported_fast_light_preserves_atomic_identity_but_observer_scope_is_checked():
    source = _fast_light(role=RadioRole.TRANSCEIVER, owner="radio-a", components=(_component("fldigi", ExecutionScope.STANDARD), _component("flrig", ExecutionScope.STANDARD)))
    candidate = DiscoveredSoftwareCandidate("fast-existing", source)
    imported = import_existing_fast_light_instance(
        ExistingCandidateRequest("imp", "radio-a", RadioRole.TRANSCEIVER, candidate), ProposalInventory((source,))
    )
    assert imported.bundle.source_locked
    with pytest.raises(GuidedFamilyCompletionError, match="FLRig/CAT/PTT"):
        import_existing_fast_light_instance(
            ExistingCandidateRequest("observer", "radio-a", RadioRole.OBSERVER, candidate), ProposalInventory((source,))
        )


def test_advanced_tx_acknowledgement_is_bound_to_role_bundle_model_and_rf_guard():
    bundle = _fast_light(
        role=RadioRole.TRANSCEIVER,
        owner="radio-a",
        components=(_component("fldigi", ExecutionScope.STANDARD), _component("flrig", ExecutionScope.STANDARD)),
    )
    model = "a" * 64
    guard = "b" * 64
    acknowledgement = AdvancedFastLightTxAcknowledgement("ack-1", "radio-a", bundle.identity_fingerprint, model, guard)
    assert validate_fast_light_final_preflight(
        radio_key="radio-a", radio_role=RadioRole.TRANSCEIVER, bundle=bundle, acknowledgement=acknowledgement,
        operating_model_fingerprint=model, rf_guard_fingerprint=guard,
    ) == acknowledgement
    with pytest.raises(GuidedFamilyCompletionError, match="stale"):
        validate_fast_light_final_preflight(
            radio_key="radio-a", radio_role=RadioRole.TRANSCEIVER,
            bundle=replace(bundle, configuration_path="/changed", identity_fingerprint="", source_fingerprint=""),
            acknowledgement=acknowledgement, operating_model_fingerprint=model, rf_guard_fingerprint=guard,
        )
    with pytest.raises(GuidedFamilyCompletionError, match="stale"):
        validate_fast_light_final_preflight(
            radio_key="radio-a", radio_role=RadioRole.TRANSCEIVER, bundle=bundle, acknowledgement=acknowledgement,
            operating_model_fingerprint="c" * 64, rf_guard_fingerprint=guard,
        )
    with pytest.raises(GuidedFamilyCompletionError, match="stale"):
        validate_fast_light_final_preflight(
            radio_key="radio-a", radio_role=RadioRole.TRANSCEIVER, bundle=bundle, acknowledgement=acknowledgement,
            operating_model_fingerprint=model, rf_guard_fingerprint="d" * 64,
        )
    with pytest.raises(GuidedFamilyCompletionError, match="receive-only"):
        validate_fast_light_final_preflight(
            radio_key="radio-a", radio_role=RadioRole.OBSERVER, bundle=bundle, acknowledgement=acknowledgement,
            operating_model_fingerprint=model, rf_guard_fingerprint=guard,
        )
    with pytest.raises(GuidedFamilyCompletionError, match="different radio"):
        validate_fast_light_final_preflight(
            radio_key="radio-b", radio_role=RadioRole.TRANSCEIVER, bundle=bundle,
            acknowledgement=AdvancedFastLightTxAcknowledgement("ack-2", "radio-b", bundle.identity_fingerprint, model, guard),
            operating_model_fingerprint=model, rf_guard_fingerprint=guard,
        )
