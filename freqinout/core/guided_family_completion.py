"""Pure GRS-3 family-completion policy for JS8Call and Fast Light.

This module deliberately sits between GRS-1 proposal construction and the
future persistence/launch adapters.  It has no Qt, database, filesystem,
process, endpoint, or radio dependencies.  In particular, callers pass an
entire :class:`AtomicInstanceBundle`; this module never accepts a loose port,
profile, or launch path that could be combined with a different identity.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Optional, Tuple

from freqinout.core.guided_radio_software_model import (
    AtomicInstanceBundle,
    ExecutionScope,
    GuidedRadioSoftwareValidationError,
    InstanceSourceMode,
    RadioRole,
    SoftwareFamily,
    radio_role_from_persisted,
)
from freqinout.core.guided_software_proposals import (
    CompleteBundleRequest,
    ExistingCandidateRequest,
    GuidedSoftwareProposalError,
    Js8DistinctProposalRequest,
    ProposalInventory,
    SoftwareProposal,
    propose_complete_bundle,
    propose_distinct_js8,
    propose_existing_candidate,
    propose_fast_light_bundle,
)


class GuidedFamilyCompletionError(GuidedRadioSoftwareValidationError):
    """Raised when a GRS-3 family plan would weaken identity or RF safety."""


_TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enabled", "enable"})
_OBSERVER_TX_TOKENS = (
    "ptt",
    "cat",
    "transmit",
    "tx-queue",
    "auto-send",
    "autosend",
    "send-automation",
)


def _text(value: object, field_name: str, *, required: bool = False) -> str:
    result = str(value or "").strip()
    if len(result) > 256:
        raise GuidedFamilyCompletionError(f"{field_name} exceeds 256 characters")
    if required and not result:
        raise GuidedFamilyCompletionError(f"{field_name} is required")
    return result


def _fingerprint(value: object, field_name: str) -> str:
    result = _text(value, field_name, required=True).casefold()
    if len(result) != 64 or any(char not in "0123456789abcdef" for char in result):
        raise GuidedFamilyCompletionError(f"{field_name} must be a SHA-256 fingerprint")
    return result


def _radio_key(value: object) -> str:
    """Match the durable radio-key normalization used by the GRS-0 model."""

    return re.sub(r"[^a-z0-9_.-]+", "-", _text(value, "radio key", required=True).casefold()).strip("-._")


def _enabled(value: object) -> bool:
    return str(value or "").strip().casefold() in _TRUE_VALUES


def _component_names(bundle: AtomicInstanceBundle) -> set[str]:
    return {component.component_key for component in bundle.launch_components}


def _unsafe_observer_tx_settings(bundle: AtomicInstanceBundle) -> Tuple[str, ...]:
    """Return bounded human-safe labels for enabled TX-shaped configuration.

    Resource claims are the pure-model representation of imported native
    settings.  We also examine explicit launch arguments, because they are part
    of the atomic launch identity and must not be a backdoor around the stored
    receive-only scope.
    """

    unsafe: list[str] = []
    for resource in bundle.resources:
        key = resource.resource_key.casefold().replace("_", "-")
        if key == "external-tx-disabled":
            continue
        if _enabled(resource.value) and any(token in key for token in _OBSERVER_TX_TOKENS):
            unsafe.append(resource.resource_key)
    for component in bundle.launch_components:
        for argument in component.arguments:
            normalized = argument.casefold().replace("_", "-")
            if any(token in normalized for token in _OBSERVER_TX_TOKENS):
                unsafe.append(f"{component.component_key} launch argument")
    return tuple(dict.fromkeys(unsafe))


def _has_reviewed_tx_disable(bundle: AtomicInstanceBundle) -> bool:
    return any(
        resource.resource_key.casefold().replace("_", "-") == "external-tx-disabled"
        and str(resource.value).strip().casefold() in _TRUE_VALUES | {"disabled"}
        for resource in bundle.resources
    )


def validate_observer_fast_light_scope(bundle: AtomicInstanceBundle) -> AtomicInstanceBundle:
    """Fail closed unless a Fast Light bundle is receive-only by construction."""

    if bundle.family != SoftwareFamily.FAST_LIGHT:
        raise GuidedFamilyCompletionError("Fast Light receive-only validation requires a fast_light bundle")
    component_names = _component_names(bundle)
    if not component_names:
        raise GuidedFamilyCompletionError("Fast Light receive-only configuration requires a reviewed component")
    if not component_names.issubset({"fldigi", "flmsg", "flamp"}):
        raise GuidedFamilyCompletionError("Fast Light is configured receive-only; FLRig/CAT/PTT controls are unavailable")
    if bundle.source_mode == InstanceSourceMode.SHARED_STATION_TOOL:
        if not component_names.issubset({"flmsg", "flamp"}):
            raise GuidedFamilyCompletionError("A station-shared Fast Light utility may contain only FLMsg or FLAmp")
        if bundle.execution_scope != ExecutionScope.STATION_SHARED_UTILITY or bundle.owner_radio_key:
            raise GuidedFamilyCompletionError("A station-shared Fast Light utility cannot claim radio control")
        expected_scope = ExecutionScope.STATION_SHARED_UTILITY
    else:
        if "fldigi" not in component_names:
            raise GuidedFamilyCompletionError("An observer Fast Light workflow requires FLDigi receive/decoder identity")
        if bundle.execution_scope != ExecutionScope.RECEIVE_ONLY or not bundle.owner_radio_key:
            raise GuidedFamilyCompletionError("Observer Fast Light must be radio-owned and receive-only")
        expected_scope = ExecutionScope.RECEIVE_ONLY
    if any(component.execution_scope != expected_scope for component in bundle.launch_components):
        raise GuidedFamilyCompletionError("Fast Light component scope does not match its reviewed receive-only identity")
    if not _has_reviewed_tx_disable(bundle):
        raise GuidedFamilyCompletionError("Fast Light receive-only configuration requires a reviewed external TX-disable claim")
    unsafe = _unsafe_observer_tx_settings(bundle)
    if unsafe:
        raise GuidedFamilyCompletionError(
            "Fast Light is configured receive-only; transmit settings are unavailable: " + ", ".join(unsafe)
        )
    return bundle


@dataclass(frozen=True)
class AdvancedFastLightTxAcknowledgement:
    """Durable acknowledgement bound to the exact preflight input identities.

    The persistence adapter can store this small value object alongside the
    Fast Light family record.  Changing the role, atomic bundle (including a
    profile or endpoint), operating model, or RF Guard invalidates the match.
    """

    acknowledgement_key: str
    radio_key: str
    fast_light_identity_fingerprint: str
    operating_model_fingerprint: str
    rf_guard_fingerprint: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "acknowledgement_key", _text(self.acknowledgement_key, "acknowledgement key", required=True))
        object.__setattr__(self, "radio_key", _radio_key(self.radio_key))
        object.__setattr__(self, "fast_light_identity_fingerprint", _fingerprint(self.fast_light_identity_fingerprint, "Fast Light identity fingerprint"))
        object.__setattr__(self, "operating_model_fingerprint", _fingerprint(self.operating_model_fingerprint, "operating-model fingerprint"))
        object.__setattr__(self, "rf_guard_fingerprint", _fingerprint(self.rf_guard_fingerprint, "RF Guard fingerprint"))

    def matches(
        self,
        *,
        radio_key: object,
        bundle: AtomicInstanceBundle,
        operating_model_fingerprint: object,
        rf_guard_fingerprint: object,
    ) -> bool:
        return (
            self.radio_key == _radio_key(radio_key)
            and self.fast_light_identity_fingerprint == bundle.computed_identity_fingerprint
            and self.operating_model_fingerprint == _fingerprint(operating_model_fingerprint, "operating-model fingerprint")
            and self.rf_guard_fingerprint == _fingerprint(rf_guard_fingerprint, "RF Guard fingerprint")
        )


def validate_fast_light_final_preflight(
    *,
    radio_key: object,
    radio_role: object,
    bundle: AtomicInstanceBundle,
    acknowledgement: Optional[AdvancedFastLightTxAcknowledgement],
    operating_model_fingerprint: object,
    rf_guard_fingerprint: object,
) -> AdvancedFastLightTxAcknowledgement:
    """Authorize an Advanced Fast Light TX plan only at final preflight.

    A caller must invoke this only for a requested Advanced-TX path.  It is
    intentionally a validator, not an authority grant: downstream send/PTT
    paths must still enforce their own current RF Guard and role checks.
    """

    role = radio_role_from_persisted(radio_role)
    if bundle.family != SoftwareFamily.FAST_LIGHT:
        raise GuidedFamilyCompletionError("Advanced TX preflight requires a Fast Light bundle")
    if bundle.owner_radio_key != _radio_key(radio_key):
        raise GuidedFamilyCompletionError("Fast Light Advanced TX bundle belongs to a different radio")
    if role != RadioRole.TRANSCEIVER:
        # Validate the supplied acknowledgement is never silently retained as a
        # role-independent approval.  This is the observer injection boundary.
        raise GuidedFamilyCompletionError("Fast Light is configured receive-only; transmit controls are not available to an observer radio")
    if acknowledgement is None:
        raise GuidedFamilyCompletionError("Fast Light Advanced TX requires an explicit acknowledgement at final preflight")
    if not isinstance(acknowledgement, AdvancedFastLightTxAcknowledgement):
        raise GuidedFamilyCompletionError("Fast Light Advanced TX acknowledgement has an invalid type")
    if not acknowledgement.matches(
        radio_key=radio_key,
        bundle=bundle,
        operating_model_fingerprint=operating_model_fingerprint,
        rf_guard_fingerprint=rf_guard_fingerprint,
    ):
        raise GuidedFamilyCompletionError(
            "Fast Light Advanced TX acknowledgement is stale; review the radio role, profile/endpoints, operating model, and RF Guard again"
        )
    return acknowledgement


def create_js8_instance(request: Js8DistinctProposalRequest, inventory: ProposalInventory) -> SoftwareProposal:
    """Create one collision-checked JS8 identity, never loose identity fields."""

    return propose_distinct_js8(request, inventory)


def import_existing_js8_instance(request: ExistingCandidateRequest, inventory: ProposalInventory) -> SoftwareProposal:
    """Import a complete JS8 candidate unchanged and source-locked."""

    if request.candidate.bundle.family != SoftwareFamily.JS8CALL:
        raise GuidedFamilyCompletionError("JS8 import requires a JS8Call family candidate")
    return propose_existing_candidate(request, inventory)


def configure_manual_js8_instance(request: CompleteBundleRequest, inventory: ProposalInventory) -> SoftwareProposal:
    """Validate a complete manual/remote JS8 bundle as one atomic identity."""

    if request.bundle.family != SoftwareFamily.JS8CALL:
        raise GuidedFamilyCompletionError("manual JS8 configuration requires a JS8Call family bundle")
    if request.bundle.source_mode != InstanceSourceMode.MANUAL_OR_REMOTE:
        raise GuidedFamilyCompletionError("manual JS8 configuration requires the manual or remote source")
    return propose_complete_bundle(request, inventory)


def configure_fast_light_instance(
    request: CompleteBundleRequest,
    inventory: ProposalInventory,
) -> SoftwareProposal:
    """Validate a complete Fast Light create/manual/shared bundle by role."""

    if request.bundle.family != SoftwareFamily.FAST_LIGHT:
        raise GuidedFamilyCompletionError("Fast Light configuration requires a fast_light family bundle")
    role = radio_role_from_persisted(request.radio_role)
    if role == RadioRole.OBSERVER:
        validate_observer_fast_light_scope(request.bundle)
    try:
        return propose_fast_light_bundle(request, inventory)
    except GuidedSoftwareProposalError as exc:
        raise GuidedFamilyCompletionError(str(exc)) from exc


def import_existing_fast_light_instance(
    request: ExistingCandidateRequest,
    inventory: ProposalInventory,
) -> SoftwareProposal:
    """Import one complete Fast Light candidate and enforce observer scope."""

    bundle = request.candidate.bundle
    if bundle.family != SoftwareFamily.FAST_LIGHT:
        raise GuidedFamilyCompletionError("Fast Light import requires a fast_light family candidate")
    if radio_role_from_persisted(request.radio_role) == RadioRole.OBSERVER:
        validate_observer_fast_light_scope(bundle)
    return propose_existing_candidate(request, inventory)


__all__ = (
    "AdvancedFastLightTxAcknowledgement",
    "GuidedFamilyCompletionError",
    "configure_fast_light_instance",
    "configure_manual_js8_instance",
    "create_js8_instance",
    "import_existing_fast_light_instance",
    "import_existing_js8_instance",
    "validate_fast_light_final_preflight",
    "validate_observer_fast_light_scope",
)
