"""Pure, atomic planning for the guided VarAC node and cluster branches.

This module is intentionally below Qt, discovery, SQLite, and launch execution.
It accepts the complete reviewed state plus a bounded persisted inventory, either
returns one immutable transaction plan, or raises before a caller can mutate
anything.  It does not manufacture command-line arguments: ``launch_command``
and ``working_directory`` are retained exactly as supplied.
"""

from __future__ import annotations

import ntpath
import posixpath
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Tuple

from freqinout.core.guided_radio_software_model import RadioRole, radio_role_from_persisted


class GuidedVarACConfigurationError(ValueError):
    """A complete VarAC plan cannot safely be persisted."""


class VarACClusterPath(str, Enum):
    STANDALONE = "standalone"
    CREATE_CLUSTER = "create_cluster"
    JOIN_CLUSTER = "join_cluster"


_KEY_RE = re.compile(r"[^a-z0-9_.-]+")
_MAX_TEXT = 4096
_MAX_ITEMS = 256


def _text(value: object, name: str, *, required: bool = False, maximum: int = _MAX_TEXT) -> str:
    text = str(value or "").strip()
    if len(text) > maximum:
        raise GuidedVarACConfigurationError(f"{name} exceeds {maximum} characters")
    if required and not text:
        raise GuidedVarACConfigurationError(f"{name} is required")
    return text


def _key(value: object, name: str, *, required: bool = True) -> str:
    key = _KEY_RE.sub("-", _text(value, name, required=required, maximum=256).casefold()).strip("-._")
    if required and not key:
        raise GuidedVarACConfigurationError(f"{name} is required")
    return key


def _flag(value: object) -> bool:
    return value.strip().casefold() in {"1", "true", "yes", "on"} if isinstance(value, str) else bool(value)


def normalize_cluster_id(value: object) -> str:
    """Return the public cluster key used for case-insensitive uniqueness."""

    text = " ".join(_text(value, "VarAC cluster ID", required=True, maximum=256).split())
    return text.casefold()


def normalize_varac_path(value: object, name: str = "VarAC path") -> str:
    """Canonicalize a path for collision checks without resolving or reading it."""

    path = _text(value, name, required=True)
    path = ntpath.normpath(path) if "\\" in path and "/" not in path else posixpath.normpath(path)
    return path.casefold()


def _positive(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise GuidedVarACConfigurationError(f"{name} must be a positive integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise GuidedVarACConfigurationError(f"{name} must be a positive integer") from exc
    if number <= 0:
        raise GuidedVarACConfigurationError(f"{name} must be a positive integer")
    return number


def _enum_path(value: object) -> VarACClusterPath:
    if isinstance(value, VarACClusterPath):
        return value
    try:
        return VarACClusterPath(_text(value, "VarAC cluster path", required=True, maximum=256).casefold())
    except ValueError as exc:
        raise GuidedVarACConfigurationError(f"Unknown VarAC cluster path: {value}") from exc


@dataclass(frozen=True)
class VarACNodeIdentity:
    """All node-local VarAC identity must be supplied together and remains exact."""

    node_key: str
    radio_key: str
    install_path: str
    launch_command: str
    working_directory: str
    ini_path: str
    database_path: str
    incoming_path: str
    outbox_path: str
    operator_starts_remotely: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_key", _key(self.node_key, "VarAC node key"))
        object.__setattr__(self, "radio_key", _key(self.radio_key, "VarAC radio key"))
        for name in ("install_path", "ini_path", "database_path", "incoming_path", "outbox_path"):
            object.__setattr__(self, name, _text(getattr(self, name), f"VarAC {name.replace('_', ' ')}", required=True))
        object.__setattr__(self, "launch_command", _text(self.launch_command, "VarAC launch command"))
        object.__setattr__(self, "working_directory", _text(self.working_directory, "VarAC working directory"))
        object.__setattr__(self, "operator_starts_remotely", _flag(self.operator_starts_remotely))
        if self.operator_starts_remotely:
            if self.launch_command or self.working_directory:
                raise GuidedVarACConfigurationError("remote VarAC node cannot define a local launch command or working directory")
        elif not self.launch_command or not self.working_directory:
            raise GuidedVarACConfigurationError("local VarAC node requires its exact launch command and working directory")

    @property
    def local_paths(self) -> Tuple[Tuple[str, str], ...]:
        return (
            ("install", normalize_varac_path(self.install_path, "VarAC install path")),
            ("ini", normalize_varac_path(self.ini_path, "VarAC INI path")),
            ("database", normalize_varac_path(self.database_path, "VarAC database path")),
            ("incoming", normalize_varac_path(self.incoming_path, "VarAC incoming path")),
            ("outbox", normalize_varac_path(self.outbox_path, "VarAC outbox path")),
        )

    @property
    def launch_identity(self) -> str:
        if self.operator_starts_remotely:
            return "remote:" + self.node_key
        # Command text is deliberately not parsed or rewritten; the working
        # directory is part of identity for Wine wrappers and shell launchers.
        return "local:" + self.launch_command + "\x00" + normalize_varac_path(self.working_directory, "VarAC working directory")


@dataclass(frozen=True)
class VarACClusterIdentity:
    """Cluster-owned fields.  A shared database belongs to this cluster only."""

    public_id: str
    shared_database_path: str = ""
    counter_refresh_seconds: int = 30
    ptt_lock_enabled: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "public_id", " ".join(_text(self.public_id, "VarAC cluster ID", required=True, maximum=256).split()))
        object.__setattr__(self, "shared_database_path", _text(self.shared_database_path, "VarAC shared database path"))
        if isinstance(self.counter_refresh_seconds, bool):
            raise GuidedVarACConfigurationError("VarAC counter refresh must be an integer")
        try:
            refresh = int(self.counter_refresh_seconds)
        except (TypeError, ValueError) as exc:
            raise GuidedVarACConfigurationError("VarAC counter refresh must be an integer") from exc
        if not 5 <= refresh <= 600:
            raise GuidedVarACConfigurationError("VarAC counter refresh must be between 5 and 600 seconds")
        object.__setattr__(self, "counter_refresh_seconds", refresh)
        object.__setattr__(self, "ptt_lock_enabled", _flag(self.ptt_lock_enabled))

    @property
    def normalized_id(self) -> str:
        return normalize_cluster_id(self.public_id)

    @property
    def normalized_shared_database_path(self) -> str:
        return normalize_varac_path(self.shared_database_path, "VarAC shared database path") if self.shared_database_path else ""


@dataclass(frozen=True)
class VarACClusterMembership:
    cluster_id: str
    node_key: str
    radio_key: str
    instance_number: int
    enabled: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "cluster_id", normalize_cluster_id(self.cluster_id))
        object.__setattr__(self, "node_key", _key(self.node_key, "VarAC membership node key"))
        object.__setattr__(self, "radio_key", _key(self.radio_key, "VarAC membership radio key"))
        object.__setattr__(self, "instance_number", _positive(self.instance_number, "VarAC cluster instance number"))
        object.__setattr__(self, "enabled", _flag(self.enabled))


@dataclass(frozen=True)
class VarACPlanningInventory:
    """Persisted facts only; this object owns no database connection or mutation."""

    nodes: Tuple[VarACNodeIdentity, ...] = field(default_factory=tuple)
    clusters: Tuple[VarACClusterIdentity, ...] = field(default_factory=tuple)
    memberships: Tuple[VarACClusterMembership, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        nodes = tuple(self.nodes or ())
        clusters = tuple(self.clusters or ())
        memberships = tuple(self.memberships or ())
        if len(nodes) > _MAX_ITEMS or len(clusters) > _MAX_ITEMS or len(memberships) > _MAX_ITEMS:
            raise GuidedVarACConfigurationError("VarAC planning inventory exceeds its bounded size")
        if not all(isinstance(item, VarACNodeIdentity) for item in nodes):
            raise GuidedVarACConfigurationError("VarAC planning nodes must be VarACNodeIdentity values")
        if not all(isinstance(item, VarACClusterIdentity) for item in clusters):
            raise GuidedVarACConfigurationError("VarAC planning clusters must be VarACClusterIdentity values")
        if not all(isinstance(item, VarACClusterMembership) for item in memberships):
            raise GuidedVarACConfigurationError("VarAC planning memberships must be VarACClusterMembership values")
        if len({item.node_key for item in nodes}) != len(nodes):
            raise GuidedVarACConfigurationError("duplicate persisted VarAC node key")
        if len({item.normalized_id for item in clusters}) != len(clusters):
            raise GuidedVarACConfigurationError("duplicate persisted VarAC cluster ID")
        object.__setattr__(self, "nodes", nodes)
        object.__setattr__(self, "clusters", clusters)
        object.__setattr__(self, "memberships", memberships)


@dataclass(frozen=True)
class VarACConfigurationRequest:
    request_key: str
    radio_role: RadioRole
    node: VarACNodeIdentity
    cluster_path: VarACClusterPath
    cluster: Optional[VarACClusterIdentity] = None
    join_cluster_id: str = ""
    instance_number: Optional[int] = None
    enabled: bool = True
    gateway_for_new_cluster: bool = False
    replace_node_key: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_key", _key(self.request_key, "VarAC request key"))
        object.__setattr__(self, "radio_role", radio_role_from_persisted(self.radio_role))
        if not isinstance(self.node, VarACNodeIdentity):
            raise GuidedVarACConfigurationError("VarAC request needs a complete VarACNodeIdentity")
        object.__setattr__(self, "cluster_path", _enum_path(self.cluster_path))
        object.__setattr__(self, "join_cluster_id", _text(self.join_cluster_id, "joined VarAC cluster ID", maximum=256))
        object.__setattr__(self, "replace_node_key", _key(self.replace_node_key, "replaced VarAC node key", required=False))
        object.__setattr__(self, "enabled", _flag(self.enabled))
        object.__setattr__(self, "gateway_for_new_cluster", _flag(self.gateway_for_new_cluster))
        if self.instance_number is not None:
            object.__setattr__(self, "instance_number", _positive(self.instance_number, "VarAC cluster instance number"))
        if self.cluster_path == VarACClusterPath.STANDALONE:
            if self.cluster is not None or self.join_cluster_id or self.instance_number is not None or self.gateway_for_new_cluster:
                raise GuidedVarACConfigurationError("standalone VarAC cannot contain cluster membership fields")
        elif self.cluster_path == VarACClusterPath.CREATE_CLUSTER:
            if not isinstance(self.cluster, VarACClusterIdentity) or self.join_cluster_id or self.instance_number is None:
                raise GuidedVarACConfigurationError("create-cluster VarAC requires cluster identity and instance number")
            if not self.enabled:
                raise GuidedVarACConfigurationError("the first created VarAC cluster member must be enabled")
        else:
            if self.cluster is not None or not self.join_cluster_id or self.instance_number is None:
                raise GuidedVarACConfigurationError("join-cluster VarAC requires an existing cluster ID and instance number")
            if self.gateway_for_new_cluster:
                raise GuidedVarACConfigurationError("only a newly created cluster may select its first gateway")


@dataclass(frozen=True)
class VarACResourceClaim:
    owner_key: str
    kind: str
    value: str
    exclusive: bool


@dataclass(frozen=True)
class VarACConfigurationPlan:
    """A complete no-side-effect persistence plan, never a partial draft."""

    request_key: str
    node: VarACNodeIdentity
    cluster_path: VarACClusterPath
    cluster: Optional[VarACClusterIdentity]
    membership: Optional[VarACClusterMembership]
    gateway_node_key: str
    resource_claims: Tuple[VarACResourceClaim, ...]


def _without_replaced(inventory: VarACPlanningInventory, request: VarACConfigurationRequest) -> VarACPlanningInventory:
    if not request.replace_node_key:
        return inventory
    replaced = next((node for node in inventory.nodes if node.node_key == request.replace_node_key), None)
    if replaced is None:
        raise GuidedVarACConfigurationError("VarAC replacement node does not exist")
    if replaced.radio_key != request.node.radio_key:
        raise GuidedVarACConfigurationError("VarAC replacement must retain the owning radio")
    return VarACPlanningInventory(
        nodes=tuple(item for item in inventory.nodes if item.node_key != request.replace_node_key),
        clusters=inventory.clusters,
        memberships=tuple(item for item in inventory.memberships if item.node_key != request.replace_node_key),
    )


def _node_claims(node: VarACNodeIdentity) -> Tuple[VarACResourceClaim, ...]:
    path_claims = tuple(VarACResourceClaim(node.node_key, f"node-local-{kind}-path", path, True) for kind, path in node.local_paths)
    return path_claims + (VarACResourceClaim(node.node_key, "launch-identity", node.launch_identity, True),)


def _validate_node_collisions(node: VarACNodeIdentity, inventory: VarACPlanningInventory) -> None:
    if any(item.node_key == node.node_key for item in inventory.nodes):
        raise GuidedVarACConfigurationError("VarAC node identity is already registered; use an explicit replacement")
    if any(item.radio_key == node.radio_key for item in inventory.nodes):
        raise GuidedVarACConfigurationError("This radio already owns a VarAC node; use an explicit replacement")
    proposed = _node_claims(node)
    for existing in inventory.nodes:
        for left in proposed:
            for right in _node_claims(existing):
                if left.kind == "launch-identity" and right.kind == "launch-identity" and left.value == right.value:
                    raise GuidedVarACConfigurationError(f"VarAC launch identity is already owned by node {existing.node_key}")
                if left.kind != "launch-identity" and right.kind != "launch-identity" and left.value == right.value:
                    label = "launch identity" if left.kind == "launch-identity" else "node-local path"
                    raise GuidedVarACConfigurationError(f"VarAC {label} is already owned by node {existing.node_key}")


def _validate_shared_database(cluster: VarACClusterIdentity, node: VarACNodeIdentity, inventory: VarACPlanningInventory) -> None:
    shared = cluster.normalized_shared_database_path
    if not shared:
        return
    if shared in {path for _, path in node.local_paths}:
        raise GuidedVarACConfigurationError("VarAC shared cluster database cannot replace a node-local path")
    for existing in inventory.clusters:
        if existing.normalized_shared_database_path == shared and existing.normalized_id != cluster.normalized_id:
            raise GuidedVarACConfigurationError("VarAC shared database is already owned by another cluster")
    for existing in inventory.nodes:
        if shared in {path for _, path in existing.local_paths}:
            raise GuidedVarACConfigurationError("VarAC shared cluster database conflicts with a node-local path")


def _validate_membership(membership: VarACClusterMembership, inventory: VarACPlanningInventory) -> None:
    for existing in inventory.memberships:
        if not existing.enabled or not membership.enabled:
            continue
        if existing.cluster_id == membership.cluster_id and existing.instance_number == membership.instance_number:
            raise GuidedVarACConfigurationError(
                f"VarAC cluster instance {membership.instance_number} is already assigned. Choose another instance number before saving."
            )
        if existing.radio_key == membership.radio_key:
            raise GuidedVarACConfigurationError("This radio is already an enabled member of a VarAC cluster")


def plan_varac_configuration(request: VarACConfigurationRequest, inventory: VarACPlanningInventory) -> VarACConfigurationPlan:
    """Build the standalone, create-cluster, or join-cluster plan atomically.

    The function has no write capability.  Callers must persist *all* fields in
    the returned plan in one transaction after their own stale-draft checks.
    """

    if not isinstance(request, VarACConfigurationRequest):
        raise GuidedVarACConfigurationError("VarAC request must be a VarACConfigurationRequest")
    if not isinstance(inventory, VarACPlanningInventory):
        raise GuidedVarACConfigurationError("VarAC inventory must be a VarACPlanningInventory")
    # This is deliberately the first policy decision: injected/imported VarAC
    # values cannot make an observer reach resource or launch planning.
    if request.radio_role != RadioRole.TRANSCEIVER:
        raise GuidedVarACConfigurationError("VarAC and VarAC Cluster are not available to observer / SDR radios.")

    effective_inventory = _without_replaced(inventory, request)
    _validate_node_collisions(request.node, effective_inventory)
    base_claims = _node_claims(request.node)
    if request.cluster_path == VarACClusterPath.STANDALONE:
        return VarACConfigurationPlan(request.request_key, request.node, request.cluster_path, None, None, "", base_claims)

    if request.cluster_path == VarACClusterPath.CREATE_CLUSTER:
        assert request.cluster is not None and request.instance_number is not None
        if any(item.normalized_id == request.cluster.normalized_id for item in effective_inventory.clusters):
            raise GuidedVarACConfigurationError(f"VarAC cluster ID {request.cluster.public_id} is already in use.")
        _validate_shared_database(request.cluster, request.node, effective_inventory)
        membership = VarACClusterMembership(request.cluster.public_id, request.node.node_key, request.node.radio_key, request.instance_number, request.enabled)
        _validate_membership(membership, effective_inventory)
        claims = base_claims + ((VarACResourceClaim(request.cluster.normalized_id, "cluster-shared-database", request.cluster.normalized_shared_database_path, False),) if request.cluster.normalized_shared_database_path else ())
        gateway = request.node.node_key if request.gateway_for_new_cluster else ""
        return VarACConfigurationPlan(request.request_key, request.node, request.cluster_path, request.cluster, membership, gateway, claims)

    assert request.instance_number is not None
    wanted_cluster_id = normalize_cluster_id(request.join_cluster_id)
    cluster = next((item for item in effective_inventory.clusters if item.normalized_id == wanted_cluster_id), None)
    if cluster is None:
        raise GuidedVarACConfigurationError(f"Unknown VarAC cluster ID: {request.join_cluster_id}")
    _validate_shared_database(cluster, request.node, effective_inventory)
    membership = VarACClusterMembership(cluster.public_id, request.node.node_key, request.node.radio_key, request.instance_number, request.enabled)
    _validate_membership(membership, effective_inventory)
    claims = base_claims + ((VarACResourceClaim(cluster.normalized_id, "cluster-shared-database", cluster.normalized_shared_database_path, False),) if cluster.normalized_shared_database_path else ())
    return VarACConfigurationPlan(request.request_key, request.node, request.cluster_path, cluster, membership, "", claims)


__all__ = [
    "GuidedVarACConfigurationError", "VarACClusterIdentity", "VarACClusterMembership",
    "VarACClusterPath", "VarACConfigurationPlan", "VarACConfigurationRequest",
    "VarACNodeIdentity", "VarACPlanningInventory", "VarACResourceClaim",
    "normalize_cluster_id", "normalize_varac_path", "plan_varac_configuration",
]
