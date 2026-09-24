"""Qt-free desired-state coordination for multi-endpoint scheduling.

This module deliberately owns no database, socket, process, timer, adapter, or
Qt object.  It accepts immutable snapshots prepared by the scheduler integration
layer and returns immutable decisions.  Endpoint execution is introduced by the
later endpoint-lane package; nothing here performs endpoint I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import datetime
import hashlib
import ipaddress
import json
import math
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from freqinout.core.receiver_control import receiver_control_verification_matches


@dataclass(frozen=True)
class FrozenMap:
    items: Tuple[Tuple[str, "FrozenValue"], ...]


@dataclass(frozen=True)
class FrozenList:
    items: Tuple["FrozenValue", ...]


@dataclass(frozen=True)
class FrozenTuple:
    items: Tuple["FrozenValue", ...]


@dataclass(frozen=True)
class FrozenSet:
    items: Tuple["FrozenValue", ...]


FrozenValue = Any
FrozenFields = Tuple[Tuple[str, FrozenValue], ...]


class EndpointIdentityError(ValueError):
    """Raised when an endpoint identity is incomplete or unsafe to normalize."""


def bounded_exponential_backoff(
    base_seconds: float,
    maximum_seconds: float,
    failure_count: int,
) -> float:
    """Return capped exponential backoff without constructing huge integers."""

    base = max(0.0, float(base_seconds))
    maximum = max(base, float(maximum_seconds))
    if not math.isfinite(base) or not math.isfinite(maximum):
        raise ValueError("backoff bounds must be finite")
    if base <= 0.0:
        return 0.0
    exponent = max(0, int(failure_count) - 1)
    if maximum <= base:
        return maximum
    # Subtract logarithms rather than dividing first so finite extreme bounds
    # cannot overflow while calculating their ratio.
    exponent_to_cap = math.log2(maximum) - math.log2(base)
    if exponent >= exponent_to_cap:
        return maximum
    return min(maximum, math.ldexp(base, exponent))


def _normalized_token(value: object, *, field_name: str) -> str:
    token = str(value or "").strip().lower().replace("_", "-")
    if not token:
        raise EndpointIdentityError(f"{field_name} is required")
    if any(char.isspace() for char in token):
        raise EndpointIdentityError(f"{field_name} cannot contain whitespace")
    return token


def normalize_host(value: object) -> str:
    """Return a stable, non-secret host identity without performing DNS I/O."""

    host = str(value or "").strip()
    if not host:
        host = "127.0.0.1"
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1].strip()
    try:
        return ipaddress.ip_address(host).compressed.lower()
    except ValueError:
        normalized = host.rstrip(".").lower()
        # A host field is an endpoint identity, not a URL. Restricting it to
        # DNS/mDNS-safe characters prevents credentials, paths, or query
        # parameters from leaking through safe labels and diagnostics.
        if not normalized or re.fullmatch(r"[a-z0-9._-]+", normalized) is None:
            raise EndpointIdentityError("host is invalid")
        return normalized


def normalize_port(value: object) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise EndpointIdentityError("port must be an integer") from exc
    if not 1 <= port <= 65_535:
        raise EndpointIdentityError("port must be between 1 and 65535")
    return port


def _freeze_value(value: object) -> FrozenValue:
    if value is None or isinstance(value, (bool, int, str, datetime.datetime)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("desired-state values must be finite")
        return value
    if isinstance(value, Mapping):
        return FrozenMap(tuple(
            sorted(
                ((str(key), _freeze_value(item)) for key, item in value.items()),
                key=lambda pair: pair[0],
            )
        ))
    if isinstance(value, list):
        return FrozenList(tuple(_freeze_value(item) for item in value))
    if isinstance(value, tuple):
        return FrozenTuple(tuple(_freeze_value(item) for item in value))
    if isinstance(value, (set, frozenset)):
        frozen = [_freeze_value(item) for item in value]
        return FrozenSet(tuple(sorted(frozen, key=repr)))
    raise TypeError(f"unsupported snapshot value: {type(value).__name__}")


def freeze_fields(values: Optional[Mapping[str, object]]) -> FrozenFields:
    """Deep-freeze a string-keyed mapping into deterministic immutable fields."""

    if not values:
        return ()
    return tuple(
        sorted(
            ((str(key), _freeze_value(value)) for key, value in values.items()),
            key=lambda pair: pair[0],
        )
    )


def _thaw_value(value: FrozenValue) -> object:
    if isinstance(value, FrozenMap):
        return {key: _thaw_value(item) for key, item in value.items}
    if isinstance(value, FrozenList):
        return [_thaw_value(item) for item in value.items]
    if isinstance(value, FrozenTuple):
        return tuple(_thaw_value(item) for item in value.items)
    if isinstance(value, FrozenSet):
        return {_thaw_value(item) for item in value.items}
    return value


def thaw_fields(values: FrozenFields) -> Dict[str, object]:
    return {key: _thaw_value(value) for key, value in values}


_ADAPTER_ALIASES = {
    "hamlib": "rigctld",
    "rigctl": "rigctld",
    "rig-ctld": "rigctld",
    "fl-rig": "flrig",
    "js8": "js8call",
    "sdr++": "sdrpp",
    "sdr-plus-plus": "sdrpp",
    "sdrpp-rigctl": "sdrpp-rigctl",
}


def _boolish(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


@dataclass(frozen=True, order=True)
class EndpointKey:
    """Normalized command ownership identity for one physical endpoint route."""

    adapter_family: str
    transport: str
    address: str
    target: str = ""

    def __post_init__(self) -> None:
        family = _normalized_token(self.adapter_family, field_name="adapter_family")
        family = _ADAPTER_ALIASES.get(family, family)
        transport = _normalized_token(self.transport, field_name="transport")
        address = str(self.address or "").strip().lower()
        target = str(self.target or "").strip().lower()
        if not address:
            raise EndpointIdentityError("address is required")
        if any(char.isspace() for char in address):
            raise EndpointIdentityError("address cannot contain whitespace")
        object.__setattr__(self, "adapter_family", family)
        object.__setattr__(self, "transport", transport)
        object.__setattr__(self, "address", address)
        object.__setattr__(self, "target", target)

    @classmethod
    def network(
        cls,
        adapter_family: object,
        host: object,
        port: object,
        *,
        transport: object = "tcp",
        target: object = "",
    ) -> "EndpointKey":
        normalized_host = normalize_host(host)
        normalized_port = normalize_port(port)
        if ":" in normalized_host:
            address = f"[{normalized_host}]:{normalized_port}"
        else:
            address = f"{normalized_host}:{normalized_port}"
        return cls(
            adapter_family=str(adapter_family or ""),
            transport=str(transport or ""),
            address=address,
            target=str(target or ""),
        )

    @classmethod
    def manual(cls, device_profile_id: object) -> "EndpointKey":
        try:
            profile_id = int(device_profile_id)
        except (TypeError, ValueError) as exc:
            raise EndpointIdentityError("manual endpoint requires a device profile ID") from exc
        if profile_id <= 0:
            raise EndpointIdentityError("manual endpoint requires a positive device profile ID")
        return cls("manual", "none", f"device-profile:{profile_id}")

    @property
    def canonical(self) -> str:
        value = f"{self.adapter_family}|{self.transport}|{self.address}"
        return f"{value}|{self.target}" if self.target else value

    @property
    def safe_label(self) -> str:
        return self.canonical


@dataclass(frozen=True)
class EndpointBinding:
    """Configuration projection binding one profile to command ownership."""

    endpoint_key: EndpointKey
    device_profile_id: int
    device_name: str
    receive_only: bool = False
    automated: bool = True

    def __post_init__(self) -> None:
        if int(self.device_profile_id) <= 0:
            raise ValueError("device_profile_id must be positive")
        object.__setattr__(self, "device_profile_id", int(self.device_profile_id))
        object.__setattr__(self, "device_name", str(self.device_name or "").strip())
        if self.endpoint_key.adapter_family == "manual" and self.automated:
            raise ValueError("manual endpoints cannot be automated")


def endpoint_binding_from_resolved_profile(
    profile: Mapping[str, object],
) -> EndpointBinding:
    """Project an already-resolved device profile into a safe route identity.

    The caller must supply the linked/resolved profile produced by the existing
    multi-radio configuration layer. This function intentionally performs no
    database lookup and does not infer cross-protocol physical-radio ownership.
    """

    try:
        profile_id = int(profile.get("id") or profile.get("device_profile_id"))
    except (TypeError, ValueError) as exc:
        raise EndpointIdentityError("resolved profile requires a positive ID") from exc
    if profile_id <= 0:
        raise EndpointIdentityError("resolved profile requires a positive ID")
    name = str(profile.get("name") or profile.get("device_name") or f"Radio {profile_id}").strip()
    device_class = str(profile.get("device_class") or "tx_rx").strip().lower()
    receive_only = device_class == "observer"
    backend = str(profile.get("control_backend") or "manual").strip().lower()

    if receive_only:
        receiver_adapter = str(profile.get("sdr_adapter") or "manual").strip().lower()
        receiver_enabled = _boolish(profile.get("sdr_control_enabled"))
        verification_state = str(
            profile.get("sdr_verification_state") or "manual"
        ).strip().lower()
        if (
            not receiver_enabled
            or receiver_adapter in {"", "manual", "none"}
            or verification_state != "verified"
            or not receiver_control_verification_matches(profile)
        ):
            return EndpointBinding(
                endpoint_key=EndpointKey.manual(profile_id),
                device_profile_id=profile_id,
                device_name=name,
                receive_only=True,
                automated=False,
            )
        target = str(profile.get("sdr_target") or "").strip().lower()
        if not target:
            raise EndpointIdentityError("automated receiver requires a selected application target")
        if any(char.isspace() for char in target) or "|" in target:
            raise EndpointIdentityError("receiver target must be a canonical token without whitespace")
        return EndpointBinding(
            endpoint_key=EndpointKey.network(
                receiver_adapter,
                profile.get("sdr_host") or "127.0.0.1",
                profile.get("sdr_port") or 4532,
                target=target,
            ),
            device_profile_id=profile_id,
            device_name=name,
            receive_only=True,
            automated=True,
        )

    if backend in {"", "manual", "none"}:
        return EndpointBinding(
            endpoint_key=EndpointKey.manual(profile_id),
            device_profile_id=profile_id,
            device_name=name,
            receive_only=False,
            automated=False,
        )

    route_fields = {
        "flrig": ("flrig_host", "flrig_port", 12345),
        "rigctld": ("rig_host", "rig_port", 4532),
        "rigctl": ("rig_host", "rig_port", 4532),
        "hamlib": ("rig_host", "rig_port", 4532),
        "js8call": ("js8_host", "js8_port", 2442),
        "js8": ("js8_host", "js8_port", 2442),
    }
    route = route_fields.get(backend)
    if route is None:
        raise EndpointIdentityError(
            f"unsupported automated control backend: {backend or 'empty'}"
        )
    host_field, port_field, default_port = route
    key = EndpointKey.network(
        backend,
        profile.get(host_field) or "127.0.0.1",
        profile.get(port_field) or default_port,
    )
    return EndpointBinding(
        endpoint_key=key,
        device_profile_id=profile_id,
        device_name=name,
        receive_only=False,
        automated=True,
    )


@dataclass(frozen=True)
class ScheduleLaneSnapshot:
    """Already-authorized current scheduler output for one configured profile."""

    binding: EndpointBinding
    source: str
    occurrence_id: str
    desired_fields: FrozenFields = field(default_factory=tuple)
    force: bool = False

    @classmethod
    def create(
        cls,
        *,
        binding: EndpointBinding,
        source: object,
        occurrence_id: object,
        desired_state: Optional[Mapping[str, object]],
        force: bool = False,
    ) -> "ScheduleLaneSnapshot":
        return cls(
            binding=binding,
            source=str(source or "NONE").strip().upper() or "NONE",
            occurrence_id=str(occurrence_id or "").strip(),
            desired_fields=freeze_fields(desired_state),
            force=bool(force),
        )

    @property
    def has_desired_state(self) -> bool:
        return bool(self.desired_fields) and self.source != "NONE"

    def desired_state(self) -> Dict[str, object]:
        return thaw_fields(self.desired_fields)


@dataclass(frozen=True)
class StationScheduleSnapshot:
    """Immutable complete input for one coordinator evaluation."""

    revision: int
    now_utc: datetime.datetime
    monotonic_s: float
    lanes: Tuple[ScheduleLaneSnapshot, ...]

    def __post_init__(self) -> None:
        if int(self.revision) < 0:
            raise ValueError("revision cannot be negative")
        if self.now_utc.tzinfo is None:
            raise ValueError("now_utc must be timezone aware")
        if not math.isfinite(float(self.monotonic_s)):
            raise ValueError("monotonic_s must be finite")
        object.__setattr__(self, "revision", int(self.revision))
        object.__setattr__(self, "now_utc", self.now_utc.astimezone(datetime.timezone.utc))
        object.__setattr__(self, "monotonic_s", float(self.monotonic_s))
        object.__setattr__(self, "lanes", tuple(self.lanes))


def _occurrence_identity(
    *,
    source: str,
    desired_fields: FrozenFields,
) -> str:
    # Compatible aliases for one route must derive the same occurrence ID.
    material = repr((str(source), desired_fields)).encode("utf-8")
    return hashlib.sha256(material).hexdigest()[:24]


def snapshot_from_resolved_lanes(
    lane_rows: Iterable[Mapping[str, object]],
    *,
    bindings_by_profile: Mapping[int, EndpointBinding],
    revision: int,
    now_utc: datetime.datetime,
    monotonic_s: float,
    force: bool = False,
) -> StationScheduleSnapshot:
    """Freeze the existing scheduler's already-resolved lane projection.

    Source selection, NET/SOP/HF precedence, manual authority, and RF safety are
    deliberately upstream responsibilities. This boundary preserves the chosen
    ``current_entry`` mapping exactly while making later coordination independent
    from mutable database/UI state.
    """

    snapshots: List[ScheduleLaneSnapshot] = []
    for row in lane_rows:
        try:
            profile_id = int(row.get("device_profile_id"))
        except (TypeError, ValueError) as exc:
            raise ValueError("resolved lane requires device_profile_id") from exc
        binding = bindings_by_profile.get(profile_id)
        if binding is None:
            raise ValueError(f"no endpoint binding for device profile {profile_id}")
        source = str(row.get("current_source") or "NONE").strip().upper() or "NONE"
        entry = row.get("current_entry")
        if entry is None:
            desired_state: Mapping[str, object] = {}
        elif isinstance(entry, Mapping):
            desired_state = entry
        else:
            raise TypeError("resolved lane current_entry must be a mapping or None")
        desired_fields = freeze_fields(desired_state)
        explicit_occurrence = str(row.get("occurrence_id") or "").strip()
        occurrence_id = explicit_occurrence or _occurrence_identity(
            source=source,
            desired_fields=desired_fields,
        )
        snapshots.append(
            ScheduleLaneSnapshot(
                binding=binding,
                source=source,
                occurrence_id=occurrence_id,
                desired_fields=desired_fields,
                force=bool(force or row.get("force")),
            )
        )
    return StationScheduleSnapshot(
        revision=revision,
        now_utc=now_utc,
        monotonic_s=monotonic_s,
        lanes=tuple(snapshots),
    )


@dataclass(frozen=True)
class EndpointIntent:
    endpoint_key: EndpointKey
    device_profile_ids: Tuple[int, ...]
    device_names: Tuple[str, ...]
    source: str
    occurrence_id: str
    desired_fields: FrozenFields
    generation: int
    created_utc: datetime.datetime
    created_monotonic_s: float
    automated: bool
    receive_only: bool
    forced: bool = False

    def desired_state(self) -> Dict[str, object]:
        return thaw_fields(self.desired_fields)


@dataclass(frozen=True)
class EndpointResult:
    endpoint_key: EndpointKey
    generation: int
    status: str
    actual_fields: FrozenFields = field(default_factory=tuple)
    reason_code: str = ""
    detail: str = ""

    def actual_state(self) -> Dict[str, object]:
        return thaw_fields(self.actual_fields)

    @classmethod
    def create(
        cls,
        *,
        endpoint_key: EndpointKey,
        generation: int,
        status: object,
        actual_state: Optional[Mapping[str, object]] = None,
        reason_code: object = "",
        detail: object = "",
    ) -> "EndpointResult":
        normalized_status = str(status or "").strip().lower().replace(" ", "_")
        allowed = {
            "applied_and_verified",
            "applied_unverified",
            "readback_mismatch",
            "rejected",
            "superseded",
            "timed_out",
            "failed",
            "cancelled",
        }
        if normalized_status not in allowed:
            raise ValueError(f"unsupported endpoint result status: {normalized_status or 'empty'}")
        return cls(
            endpoint_key=endpoint_key,
            generation=int(generation),
            status=normalized_status,
            actual_fields=freeze_fields(actual_state),
            reason_code=str(reason_code or "").strip().lower(),
            detail=str(detail or "").strip(),
        )


@dataclass(frozen=True)
class EndpointConflict:
    endpoint_key: EndpointKey
    device_profile_ids: Tuple[int, ...]
    code: str
    detail: str


@dataclass(frozen=True)
class CoordinatorState:
    """Immutable state returned to the caller for the next pure evaluation."""

    generations: Tuple[Tuple[str, int], ...] = field(default_factory=tuple)
    signatures: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)

    def generation_map(self) -> Dict[str, int]:
        return dict(self.generations)

    def signature_map(self) -> Dict[str, str]:
        return dict(self.signatures)


@dataclass(frozen=True)
class CoordinatorDecision:
    snapshot_revision: int
    intents: Tuple[EndpointIntent, ...]
    conflicts: Tuple[EndpointConflict, ...]
    idle_endpoints: Tuple[EndpointKey, ...]
    unchanged_endpoints: Tuple[EndpointKey, ...]


def _intent_signature(lane: ScheduleLaneSnapshot) -> str:
    payload = {
        "source": lane.source,
        "occurrence_id": lane.occurrence_id,
        "receive_only": bool(lane.binding.receive_only),
        "automated": bool(lane.binding.automated),
        "desired": repr(lane.desired_fields),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _compatible_aliases(lanes: Sequence[ScheduleLaneSnapshot]) -> bool:
    if not lanes:
        return True
    first = lanes[0]
    first_signature = _intent_signature(first)
    return all(_intent_signature(lane) == first_signature for lane in lanes[1:])


def validate_endpoint_bindings(
    bindings: Iterable[EndpointBinding],
) -> Tuple[EndpointConflict, ...]:
    """Return deterministic ownership conflicts without performing external I/O."""

    grouped: Dict[EndpointKey, List[EndpointBinding]] = {}
    profile_keys: Dict[int, set[EndpointKey]] = {}
    conflicts: List[EndpointConflict] = []
    for binding in bindings:
        profile_keys.setdefault(binding.device_profile_id, set()).add(binding.endpoint_key)
        grouped.setdefault(binding.endpoint_key, []).append(binding)
    for profile_id, keys in sorted(profile_keys.items()):
        if len(keys) > 1:
            canonical_keys = ", ".join(sorted(key.canonical for key in keys))
            for key in sorted(keys, key=lambda item: item.canonical):
                conflicts.append(
                    EndpointConflict(
                        endpoint_key=key,
                        device_profile_ids=(profile_id,),
                        code="profile_has_multiple_endpoint_keys",
                        detail=f"One device profile cannot own multiple endpoint keys: {canonical_keys}.",
                    )
                )
    for key, members in sorted(grouped.items(), key=lambda item: item[0].canonical):
        receive_modes = {bool(member.receive_only) for member in members}
        automation_modes = {bool(member.automated) for member in members}
        if len(receive_modes) > 1 or len(automation_modes) > 1:
            conflicts.append(
                EndpointConflict(
                    endpoint_key=key,
                    device_profile_ids=tuple(sorted(member.device_profile_id for member in members)),
                    code="endpoint_capability_conflict",
                    detail="Aliases for one endpoint disagree about receive-only or automated control capability.",
                )
            )
    return tuple(
        sorted(
            conflicts,
            key=lambda conflict: (
                conflict.endpoint_key.canonical,
                conflict.code,
                conflict.device_profile_ids,
            ),
        )
    )


def coordinate_schedule_snapshot(
    snapshot: StationScheduleSnapshot,
    prior_state: Optional[CoordinatorState] = None,
) -> Tuple[CoordinatorDecision, CoordinatorState]:
    """Purely derive endpoint intents and the next immutable coordinator state."""

    state = prior_state or CoordinatorState()
    generations = state.generation_map()
    signatures = state.signature_map()
    grouped: Dict[EndpointKey, List[ScheduleLaneSnapshot]] = {}
    for lane in snapshot.lanes:
        grouped.setdefault(lane.binding.endpoint_key, []).append(lane)

    binding_conflicts = validate_endpoint_bindings(lane.binding for lane in snapshot.lanes)
    conflict_keys = {conflict.endpoint_key for conflict in binding_conflicts}
    conflicts: List[EndpointConflict] = list(binding_conflicts)
    intents: List[EndpointIntent] = []
    idle: List[EndpointKey] = []
    unchanged: List[EndpointKey] = []

    # A conflict invalidates the last accepted signature so resolving it always
    # creates a fresh generation instead of appearing unchanged.
    for conflict_key in conflict_keys:
        signatures.pop(conflict_key.canonical, None)

    for endpoint_key, lanes in sorted(grouped.items(), key=lambda item: item[0].canonical):
        if endpoint_key in conflict_keys:
            continue
        profile_ids = tuple(sorted({lane.binding.device_profile_id for lane in lanes}))
        if not _compatible_aliases(lanes):
            conflicts.append(
                EndpointConflict(
                    endpoint_key=endpoint_key,
                    device_profile_ids=profile_ids,
                    code="competing_endpoint_intents",
                    detail="Profiles sharing one endpoint produced different desired states or authority.",
                )
            )
            continue
        lane = sorted(lanes, key=lambda item: item.binding.device_profile_id)[0]
        if not lane.has_desired_state:
            idle.append(endpoint_key)
            signatures.pop(endpoint_key.canonical, None)
            continue
        signature = _intent_signature(lane)
        previous_signature = signatures.get(endpoint_key.canonical)
        forced = any(item.force for item in lanes)
        if previous_signature == signature and not forced:
            unchanged.append(endpoint_key)
            continue
        generation = int(generations.get(endpoint_key.canonical, 0)) + 1
        generations[endpoint_key.canonical] = generation
        signatures[endpoint_key.canonical] = signature
        aliases = sorted(lanes, key=lambda item: item.binding.device_profile_id)
        intents.append(
            EndpointIntent(
                endpoint_key=endpoint_key,
                device_profile_ids=tuple(item.binding.device_profile_id for item in aliases),
                device_names=tuple(item.binding.device_name for item in aliases),
                source=lane.source,
                occurrence_id=lane.occurrence_id,
                desired_fields=lane.desired_fields,
                generation=generation,
                created_utc=snapshot.now_utc,
                created_monotonic_s=snapshot.monotonic_s,
                automated=lane.binding.automated,
                receive_only=lane.binding.receive_only,
                forced=forced,
            )
        )

    active_keys = {key.canonical for key in grouped}
    next_generations = tuple(
        sorted((key, value) for key, value in generations.items() if key in active_keys)
    )
    next_signatures = tuple(
        sorted((key, value) for key, value in signatures.items() if key in active_keys)
    )
    decision = CoordinatorDecision(
        snapshot_revision=snapshot.revision,
        intents=tuple(intents),
        conflicts=tuple(
            sorted(
                conflicts,
                key=lambda conflict: (
                    conflict.endpoint_key.canonical,
                    conflict.code,
                    conflict.device_profile_ids,
                ),
            )
        ),
        idle_endpoints=tuple(sorted(idle, key=lambda key: key.canonical)),
        unchanged_endpoints=tuple(sorted(unchanged, key=lambda key: key.canonical)),
    )
    return decision, CoordinatorState(next_generations, next_signatures)


class StationScheduleCoordinator:
    """Small stateful facade over the pure coordination function."""

    def __init__(self) -> None:
        self._state = CoordinatorState()

    @property
    def state(self) -> CoordinatorState:
        return self._state

    def evaluate(self, snapshot: StationScheduleSnapshot) -> CoordinatorDecision:
        decision, next_state = coordinate_schedule_snapshot(snapshot, self._state)
        self._state = next_state
        return decision

    def reset(self) -> None:
        self._state = CoordinatorState()
