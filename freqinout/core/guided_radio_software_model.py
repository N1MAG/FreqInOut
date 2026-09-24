"""Pure, immutable state for guided radio/software configuration.

This module is deliberately Qt-, database-, filesystem-, process-, socket-,
and radio-free.  It is the GRS-0 authority that later discovery, persistence,
and UI slices consume.  In particular, it does not infer permissions from an
application name: unknown roles, families, scopes, or native writers fail
closed.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple, Union


class GuidedRadioSoftwareValidationError(ValueError):
    """Raised when an immutable guided-configuration model is malformed."""


class RadioRole(str, Enum):
    OBSERVER = "observer"
    TRANSCEIVER = "transceiver"


class SoftwareFamily(str, Enum):
    SDRPP = "sdrpp"
    JS8CALL = "js8call"
    FAST_LIGHT = "fast_light"
    FIO_SPOTTER = "fio_spotter"
    EXTERNAL_JS8SPOTTER = "external_js8spotter"
    COMMSTAT = "commstat"
    VARAC = "varac"
    VARAC_CLUSTER = "varac_cluster"
    FLRIG_CONTROL = "flrig_control"


class AllowedState(str, Enum):
    ALLOWED = "allowed"
    OPTIONAL = "optional"
    HIDDEN_PENDING_CONTRACT = "hidden_pending_contract"
    NOT_ALLOWED = "not_allowed"


class ExecutionScope(str, Enum):
    RECEIVE_ONLY = "receive_only"
    STANDARD = "standard"
    STATION_SHARED_UTILITY = "station_shared_utility"
    REMOTE = "remote"
    BUILT_IN = "built_in"


class AuthorityMode(str, Enum):
    RECEIVE_TUNE_NEVER_PTT = "receive_tune_never_ptt"
    RECEIVE_IMPORT_ONLY = "receive_import_only"
    TRANSCEIVER_POLICY = "transceiver_policy"
    RECEIVE_SAFE_ADVANCED_TX = "receive_safe_advanced_tx"
    BUILT_IN_MAPPING = "built_in_mapping"
    EXTERNAL_APPLICATION_POLICY = "external_application_policy"
    APPLICATION_OWNED = "application_owned"
    TRANSCEIVER_CONTROL_ONLY = "transceiver_control_only"
    NONE = "none"


class InstanceSourceMode(str, Enum):
    CREATE_DISTINCT = "create_distinct_instance"
    EXISTING = "use_existing_instance"
    MANUAL_OR_REMOTE = "manual_or_remote"
    SHARED_STATION_TOOL = "shared_station_tool"
    BUILT_IN = "built_into_fio"


class CompletionPolicy(str, Enum):
    REQUIRED = "required"
    OPTIONAL = "optional"


class ManagementMode(str, Enum):
    """Who owns lifecycle/configuration changes for an instance."""

    OPERATOR = "operator"
    FIO_MANAGED = "fio_managed"
    REMOTE = "remote"
    BUILT_IN = "built_in"


class NativeWriterOperation(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    VERIFY = "verify"


_ID_RE = re.compile(r"[^a-z0-9_.-]+")
_MAX_TEXT = 4096
_MAX_SHORT_TEXT = 256
_MAX_COLLECTION = 64
_MAX_COMPONENTS = 16
_MAX_ENDPOINTS = 32
_MAX_RESOURCES = 64


def _text(value: object, field_name: str, *, required: bool = False, maximum: int = _MAX_TEXT) -> str:
    result = str(value or "").strip()
    if len(result) > maximum:
        raise GuidedRadioSoftwareValidationError(f"{field_name} exceeds {maximum} characters")
    if required and not result:
        raise GuidedRadioSoftwareValidationError(f"{field_name} is required")
    return result


def _key(value: object, field_name: str, *, required: bool = True) -> str:
    result = _ID_RE.sub("-", _text(value, field_name, required=required, maximum=_MAX_SHORT_TEXT).casefold()).strip("-._")
    if required and not result:
        raise GuidedRadioSoftwareValidationError(f"{field_name} is required")
    return result


def _enum(enum_type: Any, value: object, field_name: str) -> Any:
    if isinstance(value, enum_type):
        return value
    normalized = _text(value, field_name, required=True, maximum=_MAX_SHORT_TEXT).casefold()
    try:
        return enum_type(normalized)
    except ValueError as exc:
        raise GuidedRadioSoftwareValidationError(f"Unknown {field_name}: {value}") from exc


def _bool(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on"}
    return bool(value)


def _bounded_tuple(values: Iterable[Any], field_name: str, maximum: int) -> Tuple[Any, ...]:
    result = tuple(values or ())
    if len(result) > maximum:
        raise GuidedRadioSoftwareValidationError(f"{field_name} has more than {maximum} values")
    return result


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise GuidedRadioSoftwareValidationError(f"{field_name} must be a mapping")
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def radio_role_from_persisted(value: object) -> RadioRole:
    """Map the current device-profile vocabulary to the guided role."""

    if isinstance(value, RadioRole):
        return value
    normalized = _text(value, "radio role", required=True, maximum=_MAX_SHORT_TEXT).casefold()
    aliases = {
        "observer": RadioRole.OBSERVER,
        "sdr": RadioRole.OBSERVER,
        "receive_only": RadioRole.OBSERVER,
        "receive-only": RadioRole.OBSERVER,
        "transceiver": RadioRole.TRANSCEIVER,
        "tx_rx": RadioRole.TRANSCEIVER,
        "tx-rx": RadioRole.TRANSCEIVER,
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise GuidedRadioSoftwareValidationError(f"Unknown radio role: {value}") from exc


def radio_role_to_persisted(value: object) -> str:
    role = radio_role_from_persisted(value)
    return "observer" if role == RadioRole.OBSERVER else "tx_rx"


def software_family_for_capability(value: object) -> SoftwareFamily:
    """Normalize known JS8 variants without making them separate families."""

    if isinstance(value, SoftwareFamily):
        return value
    normalized = _text(value, "family", required=True, maximum=_MAX_SHORT_TEXT).casefold()
    if normalized in {"js8call_improved", "js8call_subspace"}:
        normalized = SoftwareFamily.JS8CALL.value
    try:
        return SoftwareFamily(normalized)
    except ValueError as exc:
        raise GuidedRadioSoftwareValidationError(f"Unknown family: {value}") from exc


@dataclass(frozen=True)
class FamilyCapability:
    """Exact role/family authority; booleans are permissions, never intent."""

    role: RadioRole
    family: SoftwareFamily
    allowed_state: AllowedState
    execution_scope: ExecutionScope
    authority_mode: AuthorityMode
    fio_tune_possible: bool = False
    fio_transmit_possible: bool = False
    fio_ptt_possible: bool = False
    fio_receive_schedule_possible: bool = False
    fio_transmit_schedule_possible: bool = False
    external_tx_disable_required: bool = False
    advanced_tx_available: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "role", radio_role_from_persisted(self.role))
        object.__setattr__(self, "family", _enum(SoftwareFamily, self.family, "family"))
        object.__setattr__(self, "allowed_state", _enum(AllowedState, self.allowed_state, "allowed_state"))
        object.__setattr__(self, "execution_scope", _enum(ExecutionScope, self.execution_scope, "execution_scope"))
        object.__setattr__(self, "authority_mode", _enum(AuthorityMode, self.authority_mode, "authority_mode"))
        for name in (
            "fio_tune_possible", "fio_transmit_possible", "fio_ptt_possible",
            "fio_receive_schedule_possible", "fio_transmit_schedule_possible",
            "external_tx_disable_required", "advanced_tx_available",
        ):
            object.__setattr__(self, name, _bool(getattr(self, name)))
        if self.allowed_state == AllowedState.NOT_ALLOWED and any(
            (self.fio_tune_possible, self.fio_transmit_possible, self.fio_ptt_possible,
             self.fio_receive_schedule_possible, self.fio_transmit_schedule_possible,
             self.advanced_tx_available)
        ):
            raise GuidedRadioSoftwareValidationError("not_allowed capability cannot grant authority")
        if self.role == RadioRole.OBSERVER and any((self.fio_transmit_possible, self.fio_ptt_possible)):
            raise GuidedRadioSoftwareValidationError("observer capability cannot grant transmit or PTT")
        if self.role == RadioRole.OBSERVER and self.advanced_tx_available:
            raise GuidedRadioSoftwareValidationError("observer capability cannot expose advanced TX")


def _capability(
    role: RadioRole, family: SoftwareFamily, state: AllowedState, scope: ExecutionScope,
    authority: AuthorityMode, *, tune: bool = False, transmit: bool = False,
    ptt: bool = False, receive_schedule: bool = False,
    transmit_schedule: bool = False, tx_disable: bool = False,
    advanced_tx: bool = False,
) -> FamilyCapability:
    return FamilyCapability(
        role=role, family=family, allowed_state=state, execution_scope=scope,
        authority_mode=authority, fio_tune_possible=tune,
        fio_transmit_possible=transmit, fio_ptt_possible=ptt,
        fio_receive_schedule_possible=receive_schedule,
        fio_transmit_schedule_possible=transmit_schedule,
        external_tx_disable_required=tx_disable,
        advanced_tx_available=advanced_tx,
    )


_CAPABILITIES: Tuple[FamilyCapability, ...] = (
    # Observer / SDR matrix.
    _capability(RadioRole.OBSERVER, SoftwareFamily.SDRPP, AllowedState.ALLOWED, ExecutionScope.RECEIVE_ONLY, AuthorityMode.RECEIVE_TUNE_NEVER_PTT, tune=True, receive_schedule=True),
    _capability(RadioRole.OBSERVER, SoftwareFamily.JS8CALL, AllowedState.ALLOWED, ExecutionScope.RECEIVE_ONLY, AuthorityMode.RECEIVE_IMPORT_ONLY, tx_disable=True),
    _capability(RadioRole.OBSERVER, SoftwareFamily.FAST_LIGHT, AllowedState.ALLOWED, ExecutionScope.RECEIVE_ONLY, AuthorityMode.RECEIVE_SAFE_ADVANCED_TX, tx_disable=True),
    _capability(RadioRole.OBSERVER, SoftwareFamily.FIO_SPOTTER, AllowedState.ALLOWED, ExecutionScope.BUILT_IN, AuthorityMode.BUILT_IN_MAPPING),
    _capability(RadioRole.OBSERVER, SoftwareFamily.EXTERNAL_JS8SPOTTER, AllowedState.HIDDEN_PENDING_CONTRACT, ExecutionScope.RECEIVE_ONLY, AuthorityMode.NONE),
    _capability(RadioRole.OBSERVER, SoftwareFamily.COMMSTAT, AllowedState.OPTIONAL, ExecutionScope.RECEIVE_ONLY, AuthorityMode.RECEIVE_IMPORT_ONLY, tx_disable=True),
    _capability(RadioRole.OBSERVER, SoftwareFamily.VARAC, AllowedState.NOT_ALLOWED, ExecutionScope.RECEIVE_ONLY, AuthorityMode.NONE),
    _capability(RadioRole.OBSERVER, SoftwareFamily.VARAC_CLUSTER, AllowedState.NOT_ALLOWED, ExecutionScope.RECEIVE_ONLY, AuthorityMode.NONE),
    _capability(RadioRole.OBSERVER, SoftwareFamily.FLRIG_CONTROL, AllowedState.NOT_ALLOWED, ExecutionScope.RECEIVE_ONLY, AuthorityMode.NONE),
    # Transceiver matrix.
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.SDRPP, AllowedState.OPTIONAL, ExecutionScope.RECEIVE_ONLY, AuthorityMode.RECEIVE_TUNE_NEVER_PTT, tune=True, receive_schedule=True),
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.JS8CALL, AllowedState.ALLOWED, ExecutionScope.STANDARD, AuthorityMode.TRANSCEIVER_POLICY, tune=True, transmit=True, ptt=True, receive_schedule=True, transmit_schedule=True),
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.FAST_LIGHT, AllowedState.ALLOWED, ExecutionScope.STANDARD, AuthorityMode.RECEIVE_SAFE_ADVANCED_TX, tune=True, transmit=True, ptt=True, receive_schedule=True, transmit_schedule=True, advanced_tx=True),
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.FIO_SPOTTER, AllowedState.ALLOWED, ExecutionScope.BUILT_IN, AuthorityMode.BUILT_IN_MAPPING),
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.EXTERNAL_JS8SPOTTER, AllowedState.ALLOWED, ExecutionScope.STANDARD, AuthorityMode.EXTERNAL_APPLICATION_POLICY),
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.COMMSTAT, AllowedState.ALLOWED, ExecutionScope.STANDARD, AuthorityMode.EXTERNAL_APPLICATION_POLICY),
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.VARAC, AllowedState.ALLOWED, ExecutionScope.STANDARD, AuthorityMode.APPLICATION_OWNED),
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.VARAC_CLUSTER, AllowedState.ALLOWED, ExecutionScope.STANDARD, AuthorityMode.APPLICATION_OWNED),
    _capability(RadioRole.TRANSCEIVER, SoftwareFamily.FLRIG_CONTROL, AllowedState.ALLOWED, ExecutionScope.STANDARD, AuthorityMode.TRANSCEIVER_CONTROL_ONLY, tune=True, ptt=True, receive_schedule=True, transmit_schedule=True),
)
_CAPABILITY_BY_KEY = MappingProxyType({(item.role, item.family): item for item in _CAPABILITIES})


def capability_for(role: object, family: object) -> FamilyCapability:
    """Return the exact capability or fail closed for an unrecognized key."""
    parsed_role = radio_role_from_persisted(role)
    parsed_family = software_family_for_capability(family)
    capability = _CAPABILITY_BY_KEY.get((parsed_role, parsed_family))
    if capability is None:
        raise GuidedRadioSoftwareValidationError("No reviewed capability for role/family")
    return capability


def capability_matrix() -> Tuple[FamilyCapability, ...]:
    return _CAPABILITIES


@dataclass(frozen=True)
class EndpointRecord:
    endpoint_key: str
    transport: str
    host: str = ""
    port: Optional[int] = None
    target: str = ""
    exclusive: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "endpoint_key", _key(self.endpoint_key, "endpoint_key"))
        object.__setattr__(self, "transport", _key(self.transport, "transport"))
        object.__setattr__(self, "host", _text(self.host, "host", maximum=_MAX_SHORT_TEXT).casefold())
        object.__setattr__(self, "target", _text(self.target, "target", maximum=_MAX_SHORT_TEXT))
        if self.port in (None, ""):
            object.__setattr__(self, "port", None)
        else:
            if isinstance(self.port, bool):
                raise GuidedRadioSoftwareValidationError("port must be an integer")
            try:
                port = int(self.port)
            except (TypeError, ValueError) as exc:
                raise GuidedRadioSoftwareValidationError("port must be an integer") from exc
            if not 1 <= port <= 65535:
                raise GuidedRadioSoftwareValidationError("port must be between 1 and 65535")
            object.__setattr__(self, "port", port)
        object.__setattr__(self, "exclusive", _bool(self.exclusive))
        if self.port is not None and not self.host:
            raise GuidedRadioSoftwareValidationError("endpoint host is required when port is set")

    @property
    def identity(self) -> Tuple[str, str, str, Optional[int], str]:
        return (self.endpoint_key, self.transport, self.host, self.port, self.target)

    @property
    def collision_identity(self) -> Tuple[str, str, Optional[int], str]:
        host = "local" if self.host in {"", "127.0.0.1", "localhost", "::1"} else self.host
        return (self.transport, host, self.port, self.target.casefold())


@dataclass(frozen=True)
class ResourceClaimRecord:
    resource_key: str
    resource_type: str
    value: str
    exclusive: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "resource_key", _key(self.resource_key, "resource_key"))
        object.__setattr__(self, "resource_type", _key(self.resource_type, "resource_type"))
        object.__setattr__(self, "value", _text(self.value, "resource value", required=True))
        object.__setattr__(self, "exclusive", _bool(self.exclusive))

    @property
    def identity(self) -> Tuple[str, str, str]:
        return (self.resource_key, self.resource_type, self.value.casefold())

    @property
    def collision_identity(self) -> Tuple[str, str]:
        return (self.resource_type, self.value.casefold())


@dataclass(frozen=True)
class LaunchComponentRecord:
    component_key: str
    executable: str = ""
    arguments: Tuple[str, ...] = field(default_factory=tuple)
    working_directory: str = ""
    environment: Mapping[str, str] = field(default_factory=dict)
    profile_selector: str = ""
    dependencies: Tuple[str, ...] = field(default_factory=tuple)
    execution_scope: ExecutionScope = ExecutionScope.STANDARD
    operator_starts: bool = False
    launch_at_startup: bool = False
    monitor_health: bool = True
    readiness_policy: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "component_key", _key(self.component_key, "component_key"))
        object.__setattr__(self, "executable", _text(self.executable, "executable"))
        arguments = tuple(_text(item, "launch argument", maximum=_MAX_TEXT) for item in _bounded_tuple(self.arguments, "arguments", _MAX_COLLECTION))
        if any(not item for item in arguments):
            raise GuidedRadioSoftwareValidationError("launch arguments cannot be blank")
        object.__setattr__(self, "arguments", arguments)
        object.__setattr__(self, "working_directory", _text(self.working_directory, "working_directory"))
        object.__setattr__(self, "profile_selector", _text(self.profile_selector, "profile_selector"))
        dependencies = tuple(_key(item, "dependency") for item in _bounded_tuple(self.dependencies, "dependencies", _MAX_COLLECTION))
        if len(set(dependencies)) != len(dependencies):
            raise GuidedRadioSoftwareValidationError("duplicate launch dependency")
        if self.component_key in dependencies:
            raise GuidedRadioSoftwareValidationError("component cannot depend on itself")
        object.__setattr__(self, "dependencies", dependencies)
        raw_environment = _mapping(self.environment, "environment")
        if len(raw_environment) > _MAX_COLLECTION:
            raise GuidedRadioSoftwareValidationError("environment has too many values")
        environment: dict[str, str] = {}
        for raw_key, raw_value in raw_environment.items():
            key = _text(raw_key, "environment key", required=True, maximum=_MAX_SHORT_TEXT)
            if "\x00" in key or "=" in key:
                raise GuidedRadioSoftwareValidationError("environment key contains an invalid character")
            if key in environment:
                raise GuidedRadioSoftwareValidationError("duplicate environment key")
            environment[key] = _text(raw_value, "environment value", maximum=_MAX_TEXT)
        object.__setattr__(self, "environment", MappingProxyType(environment))
        raw_readiness = _mapping(self.readiness_policy, "readiness_policy")
        if len(raw_readiness) > _MAX_COLLECTION:
            raise GuidedRadioSoftwareValidationError("readiness_policy has too many values")
        readiness: dict[str, str] = {}
        for raw_key, raw_value in raw_readiness.items():
            key = _key(raw_key, "readiness policy key")
            if key in readiness:
                raise GuidedRadioSoftwareValidationError("duplicate readiness policy key")
            readiness[key] = _text(raw_value, "readiness policy value", maximum=_MAX_TEXT)
        object.__setattr__(self, "readiness_policy", MappingProxyType(readiness))
        object.__setattr__(self, "execution_scope", _enum(ExecutionScope, self.execution_scope, "execution_scope"))
        object.__setattr__(self, "operator_starts", _bool(self.operator_starts))
        object.__setattr__(self, "launch_at_startup", _bool(self.launch_at_startup))
        object.__setattr__(self, "monitor_health", _bool(self.monitor_health))
        if not self.executable and not self.operator_starts:
            raise GuidedRadioSoftwareValidationError("launch component needs executable or operator_starts")
        if self.operator_starts and self.launch_at_startup:
            raise GuidedRadioSoftwareValidationError("operator-start component cannot launch at startup")


@dataclass(frozen=True)
class AtomicInstanceBundle:
    """All externally meaningful identity fields move together as one bundle."""

    family: SoftwareFamily
    instance_key: str
    source_mode: InstanceSourceMode
    completion_policy: CompletionPolicy
    owner_radio_key: str = ""
    management_mode: ManagementMode = ManagementMode.OPERATOR
    variant: str = ""
    version: str = ""
    configuration_path: str = ""
    data_path: str = ""
    message_paths: Tuple[str, ...] = field(default_factory=tuple)
    execution_scope: ExecutionScope = ExecutionScope.STANDARD
    endpoints: Tuple[EndpointRecord, ...] = field(default_factory=tuple)
    resources: Tuple[ResourceClaimRecord, ...] = field(default_factory=tuple)
    launch_components: Tuple[LaunchComponentRecord, ...] = field(default_factory=tuple)
    provenance: str = ""
    desired_fingerprint: str = ""
    observed_fingerprint: str = ""
    verification_evidence: Mapping[str, str] = field(default_factory=dict)
    recovery_state: str = ""
    identity_fingerprint: str = ""
    source_fingerprint: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "family", _enum(SoftwareFamily, self.family, "family"))
        object.__setattr__(self, "instance_key", _key(self.instance_key, "instance_key"))
        object.__setattr__(self, "source_mode", _enum(InstanceSourceMode, self.source_mode, "source_mode"))
        object.__setattr__(self, "completion_policy", _enum(CompletionPolicy, self.completion_policy, "completion_policy"))
        object.__setattr__(self, "owner_radio_key", _key(self.owner_radio_key, "owner_radio_key", required=False))
        object.__setattr__(self, "management_mode", _enum(ManagementMode, self.management_mode, "management_mode"))
        object.__setattr__(self, "variant", _text(self.variant, "variant", maximum=_MAX_SHORT_TEXT))
        object.__setattr__(self, "version", _text(self.version, "version", maximum=_MAX_SHORT_TEXT))
        object.__setattr__(self, "configuration_path", _text(self.configuration_path, "configuration_path"))
        object.__setattr__(self, "data_path", _text(self.data_path, "data_path"))
        message_paths = tuple(_text(item, "message_path", required=True) for item in _bounded_tuple(self.message_paths, "message_paths", _MAX_COLLECTION))
        if len({item.casefold() for item in message_paths}) != len(message_paths):
            raise GuidedRadioSoftwareValidationError("duplicate message path")
        object.__setattr__(self, "message_paths", message_paths)
        object.__setattr__(self, "execution_scope", _enum(ExecutionScope, self.execution_scope, "execution_scope"))
        endpoints = tuple(item if isinstance(item, EndpointRecord) else endpoint_from_mapping(item) for item in _bounded_tuple(self.endpoints, "endpoints", _MAX_ENDPOINTS))
        if len({item.endpoint_key for item in endpoints}) != len(endpoints):
            raise GuidedRadioSoftwareValidationError("duplicate endpoint key")
        if len({item.collision_identity for item in endpoints if item.exclusive}) != len(
            [item for item in endpoints if item.exclusive]
        ):
            raise GuidedRadioSoftwareValidationError("duplicate exclusive endpoint")
        object.__setattr__(self, "endpoints", endpoints)
        resources = tuple(item if isinstance(item, ResourceClaimRecord) else resource_claim_from_mapping(item) for item in _bounded_tuple(self.resources, "resources", _MAX_RESOURCES))
        if len({item.resource_key for item in resources}) != len(resources):
            raise GuidedRadioSoftwareValidationError("duplicate resource key")
        if len({item.collision_identity for item in resources if item.exclusive}) != len(
            [item for item in resources if item.exclusive]
        ):
            raise GuidedRadioSoftwareValidationError("duplicate exclusive resource")
        object.__setattr__(self, "resources", resources)
        components = tuple(item if isinstance(item, LaunchComponentRecord) else launch_component_from_mapping(item) for item in _bounded_tuple(self.launch_components, "launch_components", _MAX_COMPONENTS))
        component_keys = {item.component_key for item in components}
        if len(component_keys) != len(components):
            raise GuidedRadioSoftwareValidationError("duplicate launch component key")
        unknown_dependencies = {dependency for item in components for dependency in item.dependencies if dependency not in component_keys}
        if unknown_dependencies:
            raise GuidedRadioSoftwareValidationError("launch component dependency is not in this bundle")
        object.__setattr__(self, "launch_components", components)
        object.__setattr__(self, "provenance", _text(self.provenance, "provenance", maximum=_MAX_SHORT_TEXT))
        object.__setattr__(self, "desired_fingerprint", _text(self.desired_fingerprint, "desired_fingerprint", maximum=_MAX_SHORT_TEXT).casefold())
        object.__setattr__(self, "observed_fingerprint", _text(self.observed_fingerprint, "observed_fingerprint", maximum=_MAX_SHORT_TEXT).casefold())
        raw_evidence = _mapping(self.verification_evidence, "verification_evidence")
        if len(raw_evidence) > _MAX_COLLECTION:
            raise GuidedRadioSoftwareValidationError("verification_evidence has too many values")
        evidence: dict[str, str] = {}
        for raw_key, raw_value in raw_evidence.items():
            key = _key(raw_key, "verification evidence key")
            if key in evidence:
                raise GuidedRadioSoftwareValidationError("duplicate verification evidence key")
            evidence[key] = _text(raw_value, "verification evidence value", maximum=_MAX_TEXT)
        object.__setattr__(self, "verification_evidence", MappingProxyType(evidence))
        object.__setattr__(self, "recovery_state", _text(self.recovery_state, "recovery_state", maximum=_MAX_SHORT_TEXT))
        self._validate_source_and_scope()
        computed = self.computed_identity_fingerprint
        supplied = _text(self.identity_fingerprint, "identity_fingerprint", maximum=_MAX_SHORT_TEXT).casefold()
        if supplied and supplied != computed:
            raise GuidedRadioSoftwareValidationError("identity_fingerprint does not match the complete bundle")
        object.__setattr__(self, "identity_fingerprint", computed)
        source = _text(self.source_fingerprint, "source_fingerprint", maximum=_MAX_SHORT_TEXT).casefold()
        if self.source_mode == InstanceSourceMode.EXISTING:
            if not source:
                raise GuidedRadioSoftwareValidationError("existing bundle requires source_fingerprint")
            if source != computed:
                raise GuidedRadioSoftwareValidationError("existing bundle source_fingerprint does not match complete bundle")
        elif source and source != computed:
            raise GuidedRadioSoftwareValidationError("source_fingerprint does not match complete bundle")
        object.__setattr__(self, "source_fingerprint", source or computed)

    def _validate_source_and_scope(self) -> None:
        if self.source_mode == InstanceSourceMode.BUILT_IN:
            if self.family != SoftwareFamily.FIO_SPOTTER:
                raise GuidedRadioSoftwareValidationError("only FIO Spotter may use built-in source mode")
            if self.launch_components or self.endpoints or self.resources:
                raise GuidedRadioSoftwareValidationError("built-in bundle cannot claim external launch/resources/endpoints")
            if self.execution_scope != ExecutionScope.BUILT_IN:
                raise GuidedRadioSoftwareValidationError("built-in bundle requires built_in execution scope")
            if self.management_mode != ManagementMode.BUILT_IN:
                raise GuidedRadioSoftwareValidationError("built-in bundle requires built_in management mode")
        elif self.family == SoftwareFamily.FIO_SPOTTER:
            raise GuidedRadioSoftwareValidationError("FIO Spotter requires built-in source mode")
        if self.management_mode == ManagementMode.BUILT_IN and self.source_mode != InstanceSourceMode.BUILT_IN:
            raise GuidedRadioSoftwareValidationError("built_in management mode requires built-in source")
        if self.management_mode == ManagementMode.REMOTE and self.source_mode != InstanceSourceMode.MANUAL_OR_REMOTE:
            raise GuidedRadioSoftwareValidationError("remote management mode requires manual or remote source")
        if self.source_mode == InstanceSourceMode.SHARED_STATION_TOOL and self.execution_scope != ExecutionScope.STATION_SHARED_UTILITY:
            raise GuidedRadioSoftwareValidationError("shared station tool requires station_shared_utility scope")
        if self.source_mode == InstanceSourceMode.SHARED_STATION_TOOL and self.family not in (
            SoftwareFamily.FAST_LIGHT,
            SoftwareFamily.COMMSTAT,
        ):
            raise GuidedRadioSoftwareValidationError(
                "only Fast Light or CommStat station tools may use shared station source mode"
            )
        if self.execution_scope == ExecutionScope.STATION_SHARED_UTILITY and self.source_mode != InstanceSourceMode.SHARED_STATION_TOOL:
            raise GuidedRadioSoftwareValidationError("station_shared_utility scope requires shared station tool source")
        if self.source_mode == InstanceSourceMode.MANUAL_OR_REMOTE and self.execution_scope not in (ExecutionScope.REMOTE, ExecutionScope.RECEIVE_ONLY, ExecutionScope.STANDARD):
            raise GuidedRadioSoftwareValidationError("manual or remote bundle has incompatible execution scope")
        if self.execution_scope == ExecutionScope.BUILT_IN and self.source_mode != InstanceSourceMode.BUILT_IN:
            raise GuidedRadioSoftwareValidationError("built_in execution scope requires built-in source")
        if self.execution_scope == ExecutionScope.REMOTE and self.source_mode != InstanceSourceMode.MANUAL_OR_REMOTE:
            raise GuidedRadioSoftwareValidationError("remote execution scope requires manual or remote source")
        if self.execution_scope == ExecutionScope.REMOTE and self.management_mode != ManagementMode.REMOTE:
            raise GuidedRadioSoftwareValidationError("remote execution scope requires remote management mode")
        if self.management_mode == ManagementMode.REMOTE and self.execution_scope != ExecutionScope.REMOTE:
            raise GuidedRadioSoftwareValidationError("remote management mode requires remote execution scope")
        if self.family == SoftwareFamily.SDRPP and self.execution_scope not in (
            ExecutionScope.RECEIVE_ONLY,
            ExecutionScope.REMOTE,
        ):
            raise GuidedRadioSoftwareValidationError("SDR++ bundle must remain receive-only or remote")
        if self.family in (SoftwareFamily.VARAC, SoftwareFamily.VARAC_CLUSTER) and self.execution_scope not in (
            ExecutionScope.STANDARD,
            ExecutionScope.REMOTE,
        ):
            raise GuidedRadioSoftwareValidationError("VarAC bundle requires standard or remote scope")
        if self.family == SoftwareFamily.FLRIG_CONTROL and not any(component.component_key == "flrig" for component in self.launch_components):
            raise GuidedRadioSoftwareValidationError("FLRig control bundle requires flrig component")

    def identity_payload(self) -> Mapping[str, Any]:
        return {
            "family": self.family.value, "instance_key": self.instance_key,
            "owner_radio_key": self.owner_radio_key,
            "variant": self.variant, "version": self.version,
            "configuration_path": self.configuration_path, "data_path": self.data_path,
            "message_paths": list(self.message_paths), "execution_scope": self.execution_scope.value,
            "endpoints": [endpoint_to_mapping(item) for item in self.endpoints],
            "resources": [resource_claim_to_mapping(item) for item in self.resources],
            "launch_components": [launch_component_identity_mapping(item) for item in self.launch_components],
        }

    @property
    def computed_identity_fingerprint(self) -> str:
        return _fingerprint(self.identity_payload())

    @property
    def source_locked(self) -> bool:
        return self.source_mode == InstanceSourceMode.EXISTING


@dataclass(frozen=True)
class SoftwareSelection:
    family: SoftwareFamily
    selected: bool
    completion_policy: CompletionPolicy = CompletionPolicy.REQUIRED
    bundle: Optional[AtomicInstanceBundle] = None
    advanced_tx_requested: bool = False
    advanced_tx_acknowledged: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "family", _enum(SoftwareFamily, self.family, "family"))
        object.__setattr__(self, "selected", _bool(self.selected))
        object.__setattr__(self, "completion_policy", _enum(CompletionPolicy, self.completion_policy, "completion_policy"))
        if self.bundle is not None and not isinstance(self.bundle, AtomicInstanceBundle):
            object.__setattr__(self, "bundle", atomic_bundle_from_mapping(self.bundle))
        if self.bundle is not None and self.bundle.family != self.family:
            raise GuidedRadioSoftwareValidationError("selection family and bundle family do not match")
        object.__setattr__(self, "advanced_tx_requested", _bool(self.advanced_tx_requested))
        object.__setattr__(self, "advanced_tx_acknowledged", _bool(self.advanced_tx_acknowledged))
        if not self.advanced_tx_requested and self.advanced_tx_acknowledged:
            raise GuidedRadioSoftwareValidationError("advanced TX acknowledgement requires an advanced TX request")


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    field: str
    message: str
    blocking: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", _key(self.code, "issue code"))
        object.__setattr__(self, "field", _text(self.field, "issue field", maximum=_MAX_SHORT_TEXT))
        object.__setattr__(self, "message", _text(self.message, "issue message", required=True))
        object.__setattr__(self, "blocking", _bool(self.blocking))


@dataclass(frozen=True)
class GuidedRadioSoftwareDraft:
    radio_key: str
    radio_role: RadioRole
    selections: Tuple[SoftwareSelection, ...] = field(default_factory=tuple)
    radio_name: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "radio_key", _key(self.radio_key, "radio_key"))
        object.__setattr__(self, "radio_role", radio_role_from_persisted(self.radio_role))
        object.__setattr__(self, "radio_name", _text(self.radio_name, "radio_name", maximum=_MAX_SHORT_TEXT))
        selections = tuple(item if isinstance(item, SoftwareSelection) else selection_from_mapping(item) for item in _bounded_tuple(self.selections, "selections", len(SoftwareFamily)))
        if len({item.family for item in selections}) != len(selections):
            raise GuidedRadioSoftwareValidationError("duplicate software family selection")
        object.__setattr__(self, "selections", selections)

    @property
    def selected_families(self) -> Tuple[SoftwareFamily, ...]:
        return tuple(item.family for item in self.selections if item.selected)

    def selection_for(self, family: object) -> Optional[SoftwareSelection]:
        parsed = _enum(SoftwareFamily, family, "family")
        return next((item for item in self.selections if item.family == parsed), None)

    def validate(self) -> Tuple[ValidationIssue, ...]:
        issues: list[ValidationIssue] = []
        for selection in self.selections:
            capability = capability_for(self.radio_role, selection.family)
            field = f"selection:{selection.family.value}"
            if selection.selected and capability.allowed_state in (AllowedState.NOT_ALLOWED, AllowedState.HIDDEN_PENDING_CONTRACT):
                issues.append(ValidationIssue("family-not-allowed", field, f"{selection.family.value} is not available for {self.radio_role.value}."))
            if not selection.selected:
                if selection.bundle is not None:
                    issues.append(ValidationIssue("unselected-has-bundle", field, "An unselected family cannot retain an instance bundle."))
                if selection.advanced_tx_requested:
                    issues.append(ValidationIssue("unselected-advanced-tx", field, "An unselected family cannot request Advanced TX."))
                continue
            if selection.bundle is None:
                if selection.completion_policy == CompletionPolicy.REQUIRED:
                    issues.append(ValidationIssue("required-bundle-missing", field, "A required selected family needs a complete identity bundle."))
                else:
                    issues.append(ValidationIssue("optional-bundle-pending", field, "Optional capability is selected but can be completed later.", blocking=False))
            else:
                if selection.bundle.completion_policy != selection.completion_policy:
                    issues.append(ValidationIssue("completion-policy-mismatch", field, "Selection and atomic bundle must use the same completion policy."))
                if selection.bundle.owner_radio_key and selection.bundle.owner_radio_key != self.radio_key:
                    issues.append(ValidationIssue("bundle-owner-mismatch", field, "The selected instance bundle belongs to a different radio draft."))
                self._validate_bundle_scope(selection, capability, issues)
            if selection.advanced_tx_requested:
                if selection.family != SoftwareFamily.FAST_LIGHT or not capability.advanced_tx_available:
                    issues.append(ValidationIssue("advanced-tx-not-available", field, "Advanced TX is not available for this radio role and software family."))
                elif not selection.advanced_tx_acknowledged:
                    issues.append(ValidationIssue("advanced-tx-not-acknowledged", field, "Advanced TX requires explicit acknowledgement."))
        return tuple(issues)

    def _validate_bundle_scope(self, selection: SoftwareSelection, capability: FamilyCapability, issues: list[ValidationIssue]) -> None:
        bundle = selection.bundle
        assert bundle is not None
        field = f"selection:{selection.family.value}"
        if self.radio_role == RadioRole.OBSERVER:
            if bundle.execution_scope not in (ExecutionScope.RECEIVE_ONLY, ExecutionScope.STATION_SHARED_UTILITY, ExecutionScope.BUILT_IN):
                issues.append(ValidationIssue("observer-scope", field, "Observer bundle must use receive-only, station-shared utility, or built-in scope."))
            for component in bundle.launch_components:
                if component.execution_scope not in (ExecutionScope.RECEIVE_ONLY, ExecutionScope.STATION_SHARED_UTILITY):
                    issues.append(ValidationIssue("observer-component-scope", field, "Observer launch components must be receive-only or station-shared utilities."))
                if selection.family == SoftwareFamily.FAST_LIGHT and component.component_key == "flrig":
                    issues.append(ValidationIssue("observer-fast-light-flrig", field, "Fast Light observer bundle cannot include FLRig/CAT control."))
            if capability.external_tx_disable_required and not _resource_flag(bundle.resources, "external-tx-disabled"):
                issues.append(ValidationIssue("external-tx-disable-required", field, "Observer software requires a reviewed external TX-disable claim."))
        elif bundle.execution_scope == ExecutionScope.RECEIVE_ONLY and capability.execution_scope == ExecutionScope.STANDARD:
            # Receive-only use on a transceiver is safe and remains valid.
            pass
        if selection.family in (SoftwareFamily.VARAC, SoftwareFamily.VARAC_CLUSTER) and any(
            component.execution_scope == ExecutionScope.RECEIVE_ONLY for component in bundle.launch_components
        ):
            issues.append(ValidationIssue("varac-receive-only-scope", field, "VarAC cannot use an observer receive-only launch component."))

    @property
    def validation_issues(self) -> Tuple[ValidationIssue, ...]:
        return self.validate()

    @property
    def is_valid(self) -> bool:
        return not any(issue.blocking for issue in self.validate())


def _resource_flag(resources: Sequence[ResourceClaimRecord], key: str) -> bool:
    wanted = _key(key, "resource flag").replace("_", "-")
    return any(
        item.resource_key.replace("_", "-") == wanted
        and item.value.casefold() in {"1", "true", "yes", "on", "disabled"}
        for item in resources
    )


@dataclass(frozen=True)
class NativeWriterCapability:
    """A writer is supported only by an exact, complete lookup key."""

    writer_key: str
    family: SoftwareFamily
    variant: str
    version: str
    platform: str
    operation: NativeWriterOperation
    supported: bool = True
    preview_supported: bool = False
    backup_supported: bool = False
    readback_supported: bool = False
    restore_supported: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "writer_key", _key(self.writer_key, "writer_key"))
        object.__setattr__(self, "family", _enum(SoftwareFamily, self.family, "family"))
        object.__setattr__(self, "variant", _text(self.variant, "variant", required=True, maximum=_MAX_SHORT_TEXT).casefold())
        object.__setattr__(self, "version", _text(self.version, "version", required=True, maximum=_MAX_SHORT_TEXT).casefold())
        object.__setattr__(self, "platform", _text(self.platform, "platform", required=True, maximum=_MAX_SHORT_TEXT).casefold())
        object.__setattr__(self, "operation", _enum(NativeWriterOperation, self.operation, "operation"))
        object.__setattr__(self, "supported", _bool(self.supported))
        for field_name in (
            "preview_supported",
            "backup_supported",
            "readback_supported",
            "restore_supported",
        ):
            object.__setattr__(self, field_name, _bool(getattr(self, field_name)))
        if any(value in {"*", "any", "unknown"} for value in (self.variant, self.version, self.platform)):
            raise GuidedRadioSoftwareValidationError("writer capability requires exact family, variant, version, and platform")
        if self.supported and not all(
            (
                self.preview_supported,
                self.backup_supported,
                self.readback_supported,
                self.restore_supported,
            )
        ):
            raise GuidedRadioSoftwareValidationError(
                "supported native writer requires preview, backup, readback, and restore"
            )

    @property
    def key(self) -> Tuple[SoftwareFamily, str, str, str, NativeWriterOperation]:
        return (self.family, self.variant, self.version, self.platform, self.operation)


@dataclass(frozen=True)
class NativeWriterRegistry:
    capabilities: Tuple[NativeWriterCapability, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        capabilities = tuple(item if isinstance(item, NativeWriterCapability) else native_writer_capability_from_mapping(item) for item in _bounded_tuple(self.capabilities, "writer capabilities", _MAX_COLLECTION))
        if len({item.key for item in capabilities}) != len(capabilities):
            raise GuidedRadioSoftwareValidationError("duplicate native writer capability")
        object.__setattr__(self, "capabilities", capabilities)

    def lookup(self, *, family: object, variant: object, version: object, platform: object, operation: object) -> Optional[NativeWriterCapability]:
        """Return an exact supported record; partial/unknown keys never match."""
        try:
            parsed_family = _enum(SoftwareFamily, family, "family")
            parsed_operation = _enum(NativeWriterOperation, operation, "operation")
            parsed_variant = _text(variant, "variant", required=True, maximum=_MAX_SHORT_TEXT).casefold()
            parsed_version = _text(version, "version", required=True, maximum=_MAX_SHORT_TEXT).casefold()
            parsed_platform = _text(platform, "platform", required=True, maximum=_MAX_SHORT_TEXT).casefold()
        except GuidedRadioSoftwareValidationError:
            return None
        if any(value in {"*", "any", "unknown"} for value in (parsed_variant, parsed_version, parsed_platform)):
            return None
        candidate = next((item for item in self.capabilities if item.key == (parsed_family, parsed_variant, parsed_version, parsed_platform, parsed_operation)), None)
        return candidate if candidate is not None and candidate.supported else None

    def supports(self, **lookup: object) -> bool:
        return self.lookup(**lookup) is not None


DEFAULT_NATIVE_WRITER_REGISTRY = NativeWriterRegistry()


# Mapping helpers intentionally use JSON-ready primitives and reconstruct every
# nested record, so persistence adapters cannot accidentally keep mutable input.
def family_capability_to_mapping(value: FamilyCapability) -> Mapping[str, Any]:
    return {
        "role": value.role.value, "family": value.family.value, "allowed_state": value.allowed_state.value,
        "execution_scope": value.execution_scope.value, "authority_mode": value.authority_mode.value,
        "fio_tune_possible": value.fio_tune_possible, "fio_transmit_possible": value.fio_transmit_possible,
        "fio_ptt_possible": value.fio_ptt_possible,
        "fio_receive_schedule_possible": value.fio_receive_schedule_possible,
        "fio_transmit_schedule_possible": value.fio_transmit_schedule_possible,
        "external_tx_disable_required": value.external_tx_disable_required,
        "advanced_tx_available": value.advanced_tx_available,
    }


def family_capability_from_mapping(value: Mapping[str, Any]) -> FamilyCapability:
    raw = _mapping(value, "family capability")
    return FamilyCapability(**dict(raw))


def endpoint_to_mapping(value: EndpointRecord) -> Mapping[str, Any]:
    return {"endpoint_key": value.endpoint_key, "transport": value.transport, "host": value.host, "port": value.port, "target": value.target, "exclusive": value.exclusive}


def endpoint_from_mapping(value: Mapping[str, Any]) -> EndpointRecord:
    return EndpointRecord(**dict(_mapping(value, "endpoint")))


def resource_claim_to_mapping(value: ResourceClaimRecord) -> Mapping[str, Any]:
    return {"resource_key": value.resource_key, "resource_type": value.resource_type, "value": value.value, "exclusive": value.exclusive}


def resource_claim_from_mapping(value: Mapping[str, Any]) -> ResourceClaimRecord:
    return ResourceClaimRecord(**dict(_mapping(value, "resource claim")))


def launch_component_to_mapping(value: LaunchComponentRecord) -> Mapping[str, Any]:
    return {
        "component_key": value.component_key, "executable": value.executable, "arguments": list(value.arguments),
        "working_directory": value.working_directory, "environment": dict(value.environment),
        "profile_selector": value.profile_selector, "dependencies": list(value.dependencies),
        "execution_scope": value.execution_scope.value, "operator_starts": value.operator_starts,
        "launch_at_startup": value.launch_at_startup, "monitor_health": value.monitor_health,
        "readiness_policy": dict(value.readiness_policy),
    }


def launch_component_identity_mapping(value: LaunchComponentRecord) -> Mapping[str, Any]:
    """Return only external launch identity, excluding operator runtime policy."""

    result = dict(launch_component_to_mapping(value))
    result.pop("launch_at_startup", None)
    result.pop("monitor_health", None)
    result.pop("readiness_policy", None)
    return result


def launch_component_from_mapping(value: Mapping[str, Any]) -> LaunchComponentRecord:
    return LaunchComponentRecord(**dict(_mapping(value, "launch component")))


def atomic_bundle_to_mapping(value: AtomicInstanceBundle) -> Mapping[str, Any]:
    result = dict(value.identity_payload())
    result["source_mode"] = value.source_mode.value
    result["completion_policy"] = value.completion_policy.value
    result["management_mode"] = value.management_mode.value
    result["launch_components"] = [launch_component_to_mapping(item) for item in value.launch_components]
    result["provenance"] = value.provenance
    result["desired_fingerprint"] = value.desired_fingerprint
    result["observed_fingerprint"] = value.observed_fingerprint
    result["verification_evidence"] = dict(value.verification_evidence)
    result["recovery_state"] = value.recovery_state
    result["identity_fingerprint"] = value.identity_fingerprint
    result["source_fingerprint"] = value.source_fingerprint
    return result


def atomic_bundle_from_mapping(value: Mapping[str, Any]) -> AtomicInstanceBundle:
    raw = dict(_mapping(value, "atomic bundle"))
    return AtomicInstanceBundle(**raw)


def lock_existing_bundle(
    value: Union[AtomicInstanceBundle, Mapping[str, Any]],
) -> AtomicInstanceBundle:
    """Return a complete imported candidate locked against partial mutation."""

    bundle = value if isinstance(value, AtomicInstanceBundle) else atomic_bundle_from_mapping(value)
    raw = dict(atomic_bundle_to_mapping(bundle))
    raw["source_mode"] = InstanceSourceMode.EXISTING.value
    raw["source_fingerprint"] = bundle.computed_identity_fingerprint
    raw["identity_fingerprint"] = bundle.computed_identity_fingerprint
    return atomic_bundle_from_mapping(raw)


def selection_to_mapping(value: SoftwareSelection) -> Mapping[str, Any]:
    return {
        "family": value.family.value, "selected": value.selected, "completion_policy": value.completion_policy.value,
        "bundle": atomic_bundle_to_mapping(value.bundle) if value.bundle is not None else None,
        "advanced_tx_requested": value.advanced_tx_requested, "advanced_tx_acknowledged": value.advanced_tx_acknowledged,
    }


def selection_from_mapping(value: Mapping[str, Any]) -> SoftwareSelection:
    raw = dict(_mapping(value, "selection"))
    if raw.get("bundle") is not None:
        raw["bundle"] = atomic_bundle_from_mapping(raw["bundle"])
    return SoftwareSelection(**raw)


def validation_issue_to_mapping(value: ValidationIssue) -> Mapping[str, Any]:
    return {"code": value.code, "field": value.field, "message": value.message, "blocking": value.blocking}


def validation_issue_from_mapping(value: Mapping[str, Any]) -> ValidationIssue:
    return ValidationIssue(**dict(_mapping(value, "validation issue")))


def draft_to_mapping(value: GuidedRadioSoftwareDraft) -> Mapping[str, Any]:
    return {"radio_key": value.radio_key, "radio_role": value.radio_role.value, "radio_name": value.radio_name, "selections": [selection_to_mapping(item) for item in value.selections]}


def draft_from_mapping(value: Mapping[str, Any]) -> GuidedRadioSoftwareDraft:
    raw = dict(_mapping(value, "guided draft"))
    raw["selections"] = tuple(selection_from_mapping(item) for item in raw.get("selections", ()))
    return GuidedRadioSoftwareDraft(**raw)


def native_writer_capability_to_mapping(value: NativeWriterCapability) -> Mapping[str, Any]:
    return {
        "writer_key": value.writer_key,
        "family": value.family.value,
        "variant": value.variant,
        "version": value.version,
        "platform": value.platform,
        "operation": value.operation.value,
        "supported": value.supported,
        "preview_supported": value.preview_supported,
        "backup_supported": value.backup_supported,
        "readback_supported": value.readback_supported,
        "restore_supported": value.restore_supported,
    }


def native_writer_capability_from_mapping(value: Mapping[str, Any]) -> NativeWriterCapability:
    return NativeWriterCapability(**dict(_mapping(value, "native writer capability")))


def native_writer_registry_to_mapping(value: NativeWriterRegistry) -> Mapping[str, Any]:
    return {"capabilities": [native_writer_capability_to_mapping(item) for item in value.capabilities]}


def native_writer_registry_from_mapping(value: Mapping[str, Any]) -> NativeWriterRegistry:
    raw = dict(_mapping(value, "native writer registry"))
    raw["capabilities"] = tuple(native_writer_capability_from_mapping(item) for item in raw.get("capabilities", ()))
    return NativeWriterRegistry(**raw)


__all__ = (
    "AllowedState", "AtomicInstanceBundle", "AuthorityMode", "CompletionPolicy",
    "DEFAULT_NATIVE_WRITER_REGISTRY", "EndpointRecord", "ExecutionScope", "FamilyCapability",
    "GuidedRadioSoftwareDraft", "GuidedRadioSoftwareValidationError", "InstanceSourceMode",
    "LaunchComponentRecord", "NativeWriterCapability", "NativeWriterOperation", "NativeWriterRegistry",
    "ManagementMode", "RadioRole", "ResourceClaimRecord", "SoftwareFamily", "SoftwareSelection", "ValidationIssue",
    "atomic_bundle_from_mapping", "atomic_bundle_to_mapping", "capability_for", "capability_matrix",
    "draft_from_mapping", "draft_to_mapping", "endpoint_from_mapping", "endpoint_to_mapping",
    "family_capability_from_mapping", "family_capability_to_mapping", "launch_component_from_mapping",
    "launch_component_identity_mapping", "launch_component_to_mapping", "lock_existing_bundle",
    "native_writer_capability_from_mapping", "native_writer_capability_to_mapping",
    "native_writer_registry_from_mapping", "native_writer_registry_to_mapping", "resource_claim_from_mapping",
    "radio_role_from_persisted", "radio_role_to_persisted",
    "resource_claim_to_mapping", "selection_from_mapping", "selection_to_mapping",
    "software_family_for_capability",
    "validation_issue_from_mapping", "validation_issue_to_mapping",
)
