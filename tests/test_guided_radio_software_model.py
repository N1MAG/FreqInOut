import ast
from dataclasses import replace
from pathlib import Path

import pytest

from freqinout.core.guided_radio_software_model import (
    AllowedState,
    AtomicInstanceBundle,
    CompletionPolicy,
    EndpointRecord,
    ExecutionScope,
    GuidedRadioSoftwareDraft,
    GuidedRadioSoftwareValidationError,
    InstanceSourceMode,
    LaunchComponentRecord,
    ManagementMode,
    NativeWriterCapability,
    NativeWriterOperation,
    NativeWriterRegistry,
    RadioRole,
    ResourceClaimRecord,
    SoftwareFamily,
    SoftwareSelection,
    atomic_bundle_from_mapping,
    atomic_bundle_to_mapping,
    capability_for,
    capability_matrix,
    draft_from_mapping,
    draft_to_mapping,
    lock_existing_bundle,
    radio_role_from_persisted,
    radio_role_to_persisted,
    software_family_for_capability,
)


def _bundle(family=SoftwareFamily.JS8CALL, *, scope=ExecutionScope.RECEIVE_ONLY,
            source=InstanceSourceMode.CREATE_DISTINCT, policy=CompletionPolicy.REQUIRED,
            components=(), resources=(), endpoints=(), owner="", management=ManagementMode.OPERATOR,
            **metadata):
    return AtomicInstanceBundle(
        family=family, instance_key="instance", source_mode=source,
        completion_policy=policy, execution_scope=scope, owner_radio_key=owner,
        management_mode=management, **metadata,
        launch_components=tuple(components), resources=tuple(resources), endpoints=tuple(endpoints),
    )


def _component(key="app", *, scope=ExecutionScope.RECEIVE_ONLY, executable="/bin/app"):
    return LaunchComponentRecord(component_key=key, executable=executable, execution_scope=scope)


def _observer_selection(family, bundle=None, **kwargs):
    return SoftwareSelection(family=family, selected=True, bundle=bundle or _bundle(family), **kwargs)


def test_capability_matrix_has_every_role_family_cell_exactly_once():
    cells = [(item.role, item.family) for item in capability_matrix()]
    assert len(cells) == len(RadioRole) * len(SoftwareFamily)
    assert len(set(cells)) == len(cells)
    for role in RadioRole:
        for family in SoftwareFamily:
            assert capability_for(role, family).role == role


def test_authority_module_has_only_standard_library_imports():
    source_path = Path(__file__).parents[1] / "freqinout" / "core" / "guided_radio_software_model.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    roots = {
        (node.module or "").split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    roots.update(
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    assert roots <= {"__future__", "dataclasses", "enum", "hashlib", "json", "re", "types", "typing"}


def test_observer_has_no_fio_tx_ptt_or_transmit_scheduling_and_sdrpp_receive_schedule():
    for item in capability_matrix():
        if item.role == RadioRole.OBSERVER:
            assert not item.fio_transmit_possible
            assert not item.fio_ptt_possible
            assert not item.fio_transmit_schedule_possible
    sdrpp = capability_for(RadioRole.OBSERVER, SoftwareFamily.SDRPP)
    assert sdrpp.fio_receive_schedule_possible
    assert sdrpp.fio_tune_possible


def test_observer_js8_variants_normalize_to_one_family():
    assert software_family_for_capability("js8call_improved") is SoftwareFamily.JS8CALL
    assert software_family_for_capability("js8call_subspace") is SoftwareFamily.JS8CALL


def test_observer_fast_light_requires_receive_safe_components_claim_and_no_flrig():
    family = SoftwareFamily.FAST_LIGHT
    assert capability_for(RadioRole.OBSERVER, family).external_tx_disable_required
    good = _bundle(
        family,
        resources=(ResourceClaimRecord("external-tx-disabled", "safety", "disabled"),),
        components=(_component(),),
    )
    assert GuidedRadioSoftwareDraft("r1", RadioRole.OBSERVER, (_observer_selection(family, good),)).is_valid
    missing = _bundle(family, components=(_component(),))
    assert any(i.code == "external-tx-disable-required" for i in GuidedRadioSoftwareDraft("r1", RadioRole.OBSERVER, (_observer_selection(family, missing),)).validate())
    flrig = _component("flrig")
    bad = _bundle(family, resources=(ResourceClaimRecord("external-tx-disabled", "safety", "yes"),), components=(flrig,))
    assert any(i.code == "observer-fast-light-flrig" for i in GuidedRadioSoftwareDraft("r1", RadioRole.OBSERVER, (_observer_selection(family, bad),)).validate())


@pytest.mark.parametrize("family", [SoftwareFamily.VARAC, SoftwareFamily.VARAC_CLUSTER, SoftwareFamily.FLRIG_CONTROL])
def test_observer_varac_cluster_and_flrig_are_rejected(family):
    selection = SoftwareSelection(family=family, selected=True, bundle=_bundle(
        family, scope=ExecutionScope.STANDARD,
        components=(_component("flrig", scope=ExecutionScope.STANDARD),) if family == SoftwareFamily.FLRIG_CONTROL else (),
    ))
    assert any(i.code == "family-not-allowed" for i in GuidedRadioSoftwareDraft("r1", RadioRole.OBSERVER, (selection,)).validate())


def test_transceiver_fast_light_advanced_tx_requires_acknowledgement():
    family = SoftwareFamily.FAST_LIGHT
    pending = SoftwareSelection(family, True, bundle=_bundle(family, scope=ExecutionScope.STANDARD), advanced_tx_requested=True)
    assert any(i.code == "advanced-tx-not-acknowledged" for i in GuidedRadioSoftwareDraft("r1", RadioRole.TRANSCEIVER, (pending,)).validate())
    accepted = replace(pending, advanced_tx_acknowledged=True)
    assert GuidedRadioSoftwareDraft("r1", RadioRole.TRANSCEIVER, (accepted,)).is_valid


def test_role_persistence_adapters_round_trip_and_unknown_fail_closed():
    assert radio_role_to_persisted(radio_role_from_persisted("sdr")) == "observer"
    assert radio_role_from_persisted("tx-rx") is RadioRole.TRANSCEIVER
    with pytest.raises(GuidedRadioSoftwareValidationError):
        radio_role_from_persisted("maybe")


def test_immutable_bundle_mapping_round_trip_and_identity():
    bundle = _bundle(SoftwareFamily.JS8CALL, components=(_component(),), resources=(ResourceClaimRecord("port", "tcp", "2443"),))
    restored = atomic_bundle_from_mapping(atomic_bundle_to_mapping(bundle))
    assert restored == bundle
    assert restored.identity_fingerprint == bundle.identity_fingerprint
    with pytest.raises(TypeError):
        restored.launch_components[0].environment["x"] = "y"


def test_duplicate_endpoint_and_resource_collisions_use_actual_host_port_and_type_value():
    with pytest.raises(GuidedRadioSoftwareValidationError):
        _bundle(endpoints=(EndpointRecord("a", "tcp", "localhost", 2443), EndpointRecord("b", "tcp", "127.0.0.1", 2443)))
    with pytest.raises(GuidedRadioSoftwareValidationError):
        _bundle(resources=(ResourceClaimRecord("a", "port", "2443"), ResourceClaimRecord("b", "port", "2443")))


def test_imported_bundle_is_locked_and_partial_mutation_is_rejected_but_policy_is_not_identity():
    original = _bundle(SoftwareFamily.JS8CALL, policy=CompletionPolicy.OPTIONAL)
    locked = lock_existing_bundle(original)
    assert locked.source_locked and locked.source_mode is InstanceSourceMode.EXISTING
    assert locked.identity_fingerprint == original.identity_fingerprint
    assert replace(locked, completion_policy=CompletionPolicy.REQUIRED).identity_fingerprint == locked.identity_fingerprint
    partial = atomic_bundle_to_mapping(original)
    partial["configuration_path"] = "/changed"
    with pytest.raises(GuidedRadioSoftwareValidationError):
        lock_existing_bundle({**partial, "source_mode": "existing", "source_fingerprint": original.identity_fingerprint})


def test_required_and_optional_selection_behavior():
    required = GuidedRadioSoftwareDraft("r1", RadioRole.OBSERVER, (SoftwareSelection(SoftwareFamily.JS8CALL, True),))
    assert not required.is_valid
    optional = GuidedRadioSoftwareDraft("r1", RadioRole.OBSERVER, (SoftwareSelection(SoftwareFamily.COMMSTAT, True, CompletionPolicy.OPTIONAL),))
    assert optional.is_valid
    assert any(not i.blocking for i in optional.validation_issues)


def test_built_in_shared_and_source_scope_rules():
    built = _bundle(SoftwareFamily.FIO_SPOTTER, scope=ExecutionScope.BUILT_IN, source=InstanceSourceMode.BUILT_IN, management=ManagementMode.BUILT_IN)
    assert built.source_mode is InstanceSourceMode.BUILT_IN
    with pytest.raises(GuidedRadioSoftwareValidationError):
        _bundle(SoftwareFamily.FIO_SPOTTER)
    with pytest.raises(GuidedRadioSoftwareValidationError):
        _bundle(SoftwareFamily.JS8CALL, scope=ExecutionScope.STATION_SHARED_UTILITY)
    shared = _bundle(SoftwareFamily.FAST_LIGHT, scope=ExecutionScope.STATION_SHARED_UTILITY, source=InstanceSourceMode.SHARED_STATION_TOOL)
    assert shared.execution_scope is ExecutionScope.STATION_SHARED_UTILITY


def test_bundle_policy_metadata_round_trip_and_nested_mappings_are_immutable():
    component = _component("app", scope=ExecutionScope.STANDARD)
    bundle = _bundle(
        SoftwareFamily.JS8CALL, scope=ExecutionScope.STANDARD, management=ManagementMode.FIO_MANAGED,
        owner="radio-a", components=(component,), provenance="discovered",
        desired_fingerprint="desired-v1", observed_fingerprint="observed-v1",
        verification_evidence={"readback": "verified"}, recovery_state="ready",
    )
    restored = atomic_bundle_from_mapping(atomic_bundle_to_mapping(bundle))
    assert restored == bundle
    assert restored.owner_radio_key == "radio-a"
    assert restored.management_mode is ManagementMode.FIO_MANAGED
    assert restored.provenance == "discovered"
    assert restored.verification_evidence["readback"] == "verified"
    with pytest.raises(TypeError):
        restored.verification_evidence["new"] = "nope"
    with pytest.raises(TypeError):
        restored.launch_components[0].readiness_policy["state"] = "nope"


def test_runtime_launch_policy_does_not_change_identity_but_command_does():
    component = _component("app", scope=ExecutionScope.STANDARD)
    bundle = _bundle(SoftwareFamily.JS8CALL, scope=ExecutionScope.STANDARD, components=(component,))
    policy_component = replace(component, launch_at_startup=True, monitor_health=False, readiness_policy={"ready": "socket"})
    policy_bundle = replace(bundle, launch_components=(policy_component,))
    assert policy_bundle.identity_fingerprint == bundle.identity_fingerprint
    assert atomic_bundle_from_mapping(atomic_bundle_to_mapping(policy_bundle)) == policy_bundle
    command_component = replace(component, arguments=("--profile", "new"), profile_selector="new")
    command_bundle = replace(bundle, launch_components=(command_component,), identity_fingerprint="", source_fingerprint="")
    assert command_bundle.identity_fingerprint != bundle.identity_fingerprint


def test_bundle_owner_mismatch_is_blocking_and_source_and_management_are_distinct():
    owned = _bundle(SoftwareFamily.JS8CALL, owner="radio-a", management=ManagementMode.FIO_MANAGED)
    selection = SoftwareSelection(SoftwareFamily.JS8CALL, True, bundle=owned)
    draft = GuidedRadioSoftwareDraft("radio-b", RadioRole.OBSERVER, (selection,))
    issue = next(item for item in draft.validation_issues if item.code == "bundle-owner-mismatch")
    assert issue.blocking
    assert owned.source_mode is InstanceSourceMode.CREATE_DISTINCT
    assert owned.management_mode is ManagementMode.FIO_MANAGED
    assert owned.source_mode is not owned.management_mode
    with pytest.raises(GuidedRadioSoftwareValidationError):
        _bundle(SoftwareFamily.FIO_SPOTTER, scope=ExecutionScope.BUILT_IN, source=InstanceSourceMode.BUILT_IN, management=ManagementMode.OPERATOR)


def test_native_writer_registry_requires_exact_complete_match_and_empty_default():
    assert not NativeWriterRegistry().supports(family="js8call", variant="v", version="1", platform="linux", operation="create")
    with pytest.raises(GuidedRadioSoftwareValidationError):
        NativeWriterCapability("w", SoftwareFamily.JS8CALL, "v", "1", "linux", NativeWriterOperation.CREATE, supported=True)
    capability = NativeWriterCapability("w", SoftwareFamily.JS8CALL, "v", "1", "linux", NativeWriterOperation.CREATE, preview_supported=True, backup_supported=True, readback_supported=True, restore_supported=True)
    registry = NativeWriterRegistry((capability,))
    assert registry.lookup(family="js8call", variant="v", version="1", platform="linux", operation="create") == capability
    assert registry.lookup(family="js8call", variant="unknown", version="1", platform="linux", operation="create") is None
    assert registry.lookup(family="js8call", variant="v", version="", platform="linux", operation="create") is None


def test_bounded_collection_and_text_failures():
    with pytest.raises(GuidedRadioSoftwareValidationError):
        LaunchComponentRecord("x", executable="a", arguments=tuple("x" for _ in range(65)))
    with pytest.raises(GuidedRadioSoftwareValidationError):
        EndpointRecord("x", "tcp", host="h", port=1, target="x" * 257)
    with pytest.raises(GuidedRadioSoftwareValidationError):
        GuidedRadioSoftwareDraft("r", RadioRole.OBSERVER, tuple(SoftwareSelection(f, False) for f in list(SoftwareFamily) + [SoftwareFamily.JS8CALL]))
