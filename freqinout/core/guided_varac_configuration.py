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
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional, Tuple

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
    display_name: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_key", _key(self.node_key, "VarAC node key"))
        object.__setattr__(self, "radio_key", _key(self.radio_key, "VarAC radio key"))
        for name in ("install_path", "ini_path", "database_path", "incoming_path", "outbox_path"):
            object.__setattr__(self, name, _text(getattr(self, name), f"VarAC {name.replace('_', ' ')}", required=True))
        object.__setattr__(self, "launch_command", _text(self.launch_command, "VarAC launch command"))
        object.__setattr__(self, "working_directory", _text(self.working_directory, "VarAC working directory"))
        object.__setattr__(self, "operator_starts_remotely", _flag(self.operator_starts_remotely))
        object.__setattr__(self, "display_name", _text(self.display_name, "VarAC node display name", maximum=256))
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
class VarACJoinChoice:
    """A named, non-selected join option with its next safe member number."""

    cluster_id: str
    label: str
    next_instance_number: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "cluster_id", normalize_cluster_id(self.cluster_id))
        object.__setattr__(self, "label", _text(self.label, "VarAC cluster label", required=True, maximum=256))
        object.__setattr__(self, "next_instance_number", _positive(self.next_instance_number, "next VarAC cluster instance number"))


@dataclass(frozen=True)
class VarACArrangementRecommendation:
    """Pure topology guidance shown before VarAC node detail fields.

    ``default_path`` is standalone for fresh stations and when joining an
    existing cluster. When multiple standalone nodes make cluster creation
    ambiguous it is ``None`` so the UI can require an explicit choice. A
    recommendation is explanatory state only; callers must copy a choice into
    a reviewed request before any store transaction can mutate topology.
    """

    default_path: Optional[VarACClusterPath]
    recommended_path: Optional[VarACClusterPath]
    recommended_existing_node_key: str
    join_choices: Tuple[VarACJoinChoice, ...]
    standalone_node_keys: Tuple[str, ...]
    requires_explicit_selection: bool
    why: str
    create_choice_label: str = ""
    existing_setup_summary: str = ""
    needs_attention: bool = False
    ambiguity: bool = False
    standalone_node_choices: Tuple[Tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.default_path is not None:
            object.__setattr__(self, "default_path", _enum_path(self.default_path))
        if self.recommended_path is not None:
            object.__setattr__(self, "recommended_path", _enum_path(self.recommended_path))
        object.__setattr__(self, "recommended_existing_node_key", _key(self.recommended_existing_node_key, "recommended VarAC node key", required=False))
        object.__setattr__(self, "join_choices", tuple(self.join_choices or ()))
        object.__setattr__(self, "standalone_node_keys", tuple(_key(key, "standalone VarAC node key") for key in (self.standalone_node_keys or ())))
        object.__setattr__(self, "requires_explicit_selection", _flag(self.requires_explicit_selection))
        object.__setattr__(self, "why", _text(self.why, "VarAC arrangement explanation", required=True, maximum=1024))
        object.__setattr__(self, "create_choice_label", _text(self.create_choice_label, "VarAC create choice label", maximum=256))
        object.__setattr__(self, "existing_setup_summary", _text(self.existing_setup_summary, "VarAC existing setup summary", maximum=512))
        object.__setattr__(self, "needs_attention", _flag(self.needs_attention))
        object.__setattr__(self, "ambiguity", _flag(self.ambiguity))
        choices = tuple((str(key).strip(), str(label).strip()) for key, label in (self.standalone_node_choices or ()))
        object.__setattr__(self, "standalone_node_choices", choices)

    @property
    def has_existing_clusters(self) -> bool:
        return bool(self.join_choices)

    @property
    def has_standalone_nodes(self) -> bool:
        return bool(self.standalone_node_keys)

    def to_mapping(self) -> dict[str, object]:
        return {
            "default_path": self.default_path.value if self.default_path else "",
            "recommended_path": self.recommended_path.value if self.recommended_path else "",
            "recommended_existing_node_key": self.recommended_existing_node_key,
            "join_choices": tuple(
                {
                    "cluster_id": choice.cluster_id,
                    "label": choice.label,
                    "next_instance_number": choice.next_instance_number,
                }
                for choice in self.join_choices
            ),
            "standalone_node_keys": self.standalone_node_keys,
            "why": self.why,
            "create_choice_label": self.create_choice_label,
            "existing_setup_summary": self.existing_setup_summary,
            "needs_attention": self.needs_attention,
            "ambiguity": self.ambiguity,
            "standalone_node_choices": self.standalone_node_choices,
        }


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


def recommend_varac_arrangement(inventory: VarACPlanningInventory) -> VarACArrangementRecommendation:
    """Return conditional VarAC choices without selecting or mutating one.

    Disabled memberships do not reserve a member number for the next
    proposal, matching the persistence validator. A node with any membership
    is not treated as standalone, even when that membership is disabled: the
    operator must explicitly review that topology first.
    """

    if not isinstance(inventory, VarACPlanningInventory):
        raise GuidedVarACConfigurationError("VarAC inventory must be a VarACPlanningInventory")
    membership_node_keys = {membership.node_key for membership in inventory.memberships}
    standalone_nodes = tuple(
        sorted(node.node_key for node in inventory.nodes if node.node_key not in membership_node_keys)
    )
    standalone_choices = tuple(
        (node.node_key, node.display_name or node.node_key)
        for node in sorted(
            (node for node in inventory.nodes if node.node_key not in membership_node_keys),
            key=lambda item: item.node_key,
        )
    )
    choices = []
    for cluster in sorted(inventory.clusters, key=lambda item: (item.normalized_id, item.public_id)):
        occupied = {
            membership.instance_number
            for membership in inventory.memberships
            if membership.cluster_id == cluster.normalized_id and membership.enabled
        }
        next_number = 1
        while next_number in occupied:
            next_number += 1
        choices.append(VarACJoinChoice(cluster.normalized_id, cluster.public_id, next_number))

    recommendation: Optional[VarACClusterPath] = None
    recommended_node = ""
    default_path: Optional[VarACClusterPath] = VarACClusterPath.STANDALONE
    requires_selection = False
    create_choice_label = "Create a new VarAC cluster"
    existing_setup_summary = "No existing VarAC node or cluster is configured."
    if standalone_nodes and not choices:
        recommendation = VarACClusterPath.CREATE_CLUSTER
        if len(standalone_nodes) == 1:
            recommended_node = standalone_nodes[0]
            default_path = None
            requires_selection = True
            create_choice_label = f"Create a cluster using {standalone_choices[0][1]}"
            existing_setup_summary = f"One standalone VarAC node is configured: {standalone_choices[0][1]}."
            why = (
                "An existing standalone VarAC node was found. Standalone remains an explicit alternative; "
                "choose Create a new cluster only if this new node should join that station topology."
            )
        else:
            default_path = None
            requires_selection = True
            create_choice_label = "Create a cluster using a selected standalone node"
            existing_setup_summary = f"{len(standalone_nodes)} standalone VarAC nodes are configured."
            why = (
                "Multiple standalone VarAC nodes were found. Choose which existing node to include before "
                "creating a cluster; no topology choice is preselected."
            )
    elif choices:
        existing_setup_summary = f"{len(choices)} existing VarAC cluster{'s' if len(choices) != 1 else ''} is configured."
        why = (
            "Existing VarAC clusters are available as explicit join choices. "
            "Standalone remains the safe default and no cluster membership is preselected."
        )
    else:
        why = (
            "No existing VarAC node or cluster was found. Standalone is the normal default; "
            "creating a cluster remains an explicit reviewed choice."
        )
    return VarACArrangementRecommendation(
        default_path=default_path,
        recommended_path=recommendation,
        recommended_existing_node_key=recommended_node,
        join_choices=tuple(choices),
        standalone_node_keys=standalone_nodes,
        requires_explicit_selection=requires_selection,
        why=why,
        create_choice_label=create_choice_label,
        existing_setup_summary=existing_setup_summary,
        needs_attention=requires_selection,
        ambiguity=requires_selection,
        standalone_node_choices=standalone_choices,
    )


# Clear alias for callers that describe this as a topology decision.
varac_arrangement_recommendation = recommend_varac_arrangement


def recommend_varac_arrangement_from_snapshots(
    instance_rows: Iterable[Mapping[str, Any]],
    cluster_rows: Iterable[Mapping[str, Any]],
    membership_rows: Iterable[Mapping[str, Any]],
    profile_rows: Iterable[Mapping[str, Any]] = (),
    *,
    new_radio_label: str = "new radio",
) -> Mapping[str, Any]:
    """Adapt already-loaded classified store projections for guided UI.

    This boundary deliberately accepts projections rather than opening a
    store or reconstructing missing VarAC paths. Complete, durably linked rows
    are immediately usable. A durably linked but incomplete row still proves
    that a topology node exists and remains selectable; native preparation owns
    path qualification and reports the exact missing fact. Unlinked diagnostic
    and recovery rows never influence topology.
    """

    def positive(value: object) -> int:
        try:
            number = int(value or 0)
        except (TypeError, ValueError):
            return 0
        return number if number > 0 else 0

    def text(value: object) -> str:
        return str(value or "").strip()

    new_label = text(new_radio_label) or "new radio"

    def cluster_identity_base(existing_label: str = "") -> tuple[str, str]:
        """Return the unsuffixed identity for the reviewed member labels."""

        labels = [part for part in (text(existing_label), new_label) if part]
        display_name = " + ".join(labels) or "VarAC"
        if "varac" not in display_name.casefold():
            display_name = f"{display_name} VarAC"

        slug_parts = []
        for part in labels or ["cluster"]:
            slug = re.sub(r"[^A-Z0-9]+", "-", part.upper()).strip("-")
            if slug:
                slug_parts.append(slug)
        base_id = "-".join(("VARAC", *slug_parts))[:240].strip("-") or "VARAC-CLUSTER"
        return display_name[:256], base_id

    def proposed_cluster_identity(existing_label: str = "") -> tuple[str, str]:
        """Return a readable, collision-free identity for a prepared cluster.

        The proposal is configuration owned by FIO, not a claim that FIO can
        write VarAC's native settings.  It is derived from the already-loaded
        snapshot so the normal guided path never opens a blank cluster-name
        field merely to complete a plan.
        """

        display_name, base_id = cluster_identity_base(existing_label)
        occupied = {str(row["cluster_id"]) for row in choices}
        public_id = base_id
        suffix = 2
        while normalize_cluster_id(public_id) in occupied:
            suffix_text = f"-{suffix}"
            public_id = f"{base_id[: 256 - len(suffix_text)]}{suffix_text}"
            suffix += 1
        return display_name[:256], public_id

    profile_by_node_id = {}
    profile_by_id = {}
    for raw_profile in tuple(profile_rows or ()):
        if not isinstance(raw_profile, Mapping):
            continue
        node_id = positive(raw_profile.get("varac_node_id"))
        profile_id = positive(raw_profile.get("id"))
        if node_id > 0 and profile_id > 0:
            profile_by_node_id[node_id] = {
                "device_profile_id": profile_id,
                "device_profile_name": text(raw_profile.get("name") or raw_profile.get("system_key") or f"Radio {profile_id}"),
            }
        if profile_id > 0:
            profile_by_id[profile_id] = text(
                raw_profile.get("name") or raw_profile.get("system_key") or f"Radio {profile_id}"
            )

    usable_rows = []
    for raw in tuple(instance_rows or ()):
        if not isinstance(raw, Mapping):
            continue
        classification = text(raw.get("candidate_classification")).casefold()
        linked = raw.get("linked_to_radio") is True
        immediately_usable = (
            classification == "usable_existing"
            and raw.get("candidate_usable") is True
        )
        if not immediately_usable and not linked:
            continue
        node_id = positive(raw.get("id"))
        if node_id <= 0:
            continue
        device_id = positive(
            raw.get("device_profile_id")
            or raw.get("assigned_device_profile_id")
            or raw.get("radio_profile_id")
            or raw.get("owner_radio_id")
        ) or None
        profile_evidence = profile_by_node_id.get(node_id, {})
        if device_id is None:
            device_id = profile_evidence.get("device_profile_id")
        node_key = text(raw.get("system_key") or raw.get("instance_key"))
        if not node_key:
            continue
        usable_rows.append(
            {
                "node_id": node_id,
                "device_profile_id": device_id,
                "node_key": node_key,
                "label": text(raw.get("name") or raw.get("instance_name") or node_key),
                "device_profile_name": profile_evidence.get("device_profile_name", ""),
                "candidate_classification": classification,
                "candidate_usable": immediately_usable,
                "configuration_review_required": not immediately_usable,
                "candidate_reasons": tuple(raw.get("candidate_reasons") or ()),
            }
        )
    usable_rows.sort(key=lambda item: (str(item["label"]).casefold(), item["node_id"]))

    memberships = tuple(row for row in (membership_rows or ()) if isinstance(row, Mapping))
    member_node_ids = {positive(row.get("varac_node_id")) for row in memberships if positive(row.get("varac_node_id"))}
    # The store projection normally carries device_profile_id, so support that
    # canonical key and the node-id alias used by older snapshots.
    member_device_ids = {positive(row.get("device_profile_id")) for row in memberships if positive(row.get("device_profile_id"))}
    standalone = [
        row for row in usable_rows
        if row["node_id"] not in member_node_ids
        and (row["device_profile_id"] is None or row["device_profile_id"] not in member_device_ids)
    ]

    choices = []
    for raw in tuple(cluster_rows or ()):
        if not isinstance(raw, Mapping):
            continue
        cluster_db_id = positive(raw.get("id"))
        cluster_id = text(raw.get("cluster_id") or raw.get("public_id"))
        if cluster_db_id <= 0 or not cluster_id:
            continue
        normalized_cluster = normalize_cluster_id(cluster_id)
        occupied = {
            positive(member.get("instance_number"))
            for member in memberships
            if (
                positive(member.get("cluster_id")) == cluster_db_id
                or (
                    text(member.get("cluster_public_id"))
                    and normalize_cluster_id(text(member.get("cluster_public_id"))) == normalized_cluster
                )
            )
            and _flag(member.get("enabled", True))
        }
        next_number = 1
        while next_number in occupied:
            next_number += 1
        label = text(raw.get("name") or cluster_id)
        cluster_members = tuple(
            member
            for member in memberships
            if (
                positive(member.get("cluster_id")) == cluster_db_id
                or (
                    text(member.get("cluster_public_id"))
                    and normalize_cluster_id(text(member.get("cluster_public_id"))) == normalized_cluster
                )
            )
            and _flag(member.get("enabled", True))
        )
        member_device_ids = tuple(
            sorted(
                positive(member.get("device_profile_id"))
                for member in cluster_members
                if positive(member.get("device_profile_id"))
            )
        )
        member_labels = tuple(
            profile_by_id[device_id]
            for device_id in member_device_ids
            if profile_by_id.get(device_id)
        )
        resume_recommended = False
        if len(member_device_ids) == 1 and len(member_labels) == 1:
            _expected_name, expected_id = cluster_identity_base(member_labels[0])
            resume_recommended = normalize_cluster_id(expected_id) == normalized_cluster
        choices.append(
            {
                "cluster_db_id": cluster_db_id,
                "cluster_id": normalized_cluster,
                "label": label,
                "next_instance_number": next_number,
                "member_device_profile_ids": member_device_ids,
                "resume_recommended": resume_recommended,
            }
        )
    choices.sort(key=lambda item: (str(item["label"]).casefold(), str(item["cluster_id"])))

    enriched_standalone = []
    for row in standalone:
        cluster_label = text(row.get("device_profile_name") or row.get("label"))
        proposed_name, proposed_id = proposed_cluster_identity(cluster_label)
        enriched_standalone.append(
            {
                **row,
                "proposed_cluster_name": proposed_name,
                "proposed_cluster_id": proposed_id,
            }
        )
    standalone = enriched_standalone
    proposed_create_cluster_name, proposed_create_cluster_id = proposed_cluster_identity()
    default_path = VarACClusterPath.STANDALONE.value
    recommended_path = ""
    recommended_node_id = None
    recommended_device_id = None
    existing_member_instance_number = None
    new_member_instance_number = 1
    needs_attention = False
    ambiguity = False
    requires_selection = False
    if standalone and not choices:
        recommended_path = VarACClusterPath.CREATE_CLUSTER.value
        if len(standalone) == 1:
            candidate = standalone[0]
            recommended_node_id = candidate["node_id"]
            recommended_device_id = candidate["device_profile_id"]
            existing_member_instance_number = 1
            new_member_instance_number = 2
            default_path = ""
            requires_selection = True
            existing_setup_summary = f"Existing setup: {candidate['label']} is standalone. No VarAC cluster is configured."
            create_choice_label = f"Create a cluster with {candidate['label']} and {new_label} — Recommended"
            proposed_create_cluster_name = str(candidate["proposed_cluster_name"])
            proposed_create_cluster_id = str(candidate["proposed_cluster_id"])
        else:
            default_path = ""
            needs_attention = True
            ambiguity = True
            requires_selection = True
            existing_member_instance_number = 1
            new_member_instance_number = 2
            existing_setup_summary = f"Existing setup: {len(standalone)} standalone VarAC nodes are configured. No VarAC cluster is configured."
            create_choice_label = f"Create a cluster with a selected standalone node and {new_label} — Needs attention"
    elif choices:
        resume_choices = [choice for choice in choices if choice.get("resume_recommended")]
        if len(resume_choices) == 1:
            resume_choice = resume_choices[0]
            default_path = ""
            recommended_path = VarACClusterPath.JOIN_CLUSTER.value
            requires_selection = True
            new_member_instance_number = int(resume_choice["next_instance_number"])
            existing_setup_summary = (
                f"Existing setup: {resume_choice['label']} already contains the first reviewed radio."
            )
            create_choice_label = "Create a different new VarAC cluster"
        else:
            existing_setup_summary = f"Existing setup: {len(choices)} VarAC cluster{'s' if len(choices) != 1 else ''} configured."
            create_choice_label = "Create a new VarAC cluster"
    else:
        existing_setup_summary = "Existing setup: No VarAC node or cluster is configured."
        create_choice_label = "Create a new VarAC cluster"
    return MappingProxyType(
        {
            "default_path": default_path,
            "recommended_path": recommended_path,
            "recommended_existing_node_id": recommended_node_id,
            "recommended_existing_device_profile_id": recommended_device_id,
            "existing_member_instance_number": existing_member_instance_number,
            "new_member_instance_number": new_member_instance_number,
            "standalone_candidates": tuple(MappingProxyType(dict(row)) for row in standalone),
            "join_choices": tuple(MappingProxyType(dict(row)) for row in choices),
            "existing_setup_summary": existing_setup_summary,
            "create_choice_label": create_choice_label,
            "proposed_create_cluster_name": proposed_create_cluster_name,
            "proposed_create_cluster_id": proposed_create_cluster_id,
            "needs_attention": needs_attention,
            "ambiguity": ambiguity,
            "requires_explicit_selection": requires_selection,
            "why": (
                "Choose the recommended cluster arrangement explicitly to include the existing standalone node; "
                "otherwise keep the standalone alternative."
                if recommended_path == VarACClusterPath.CREATE_CLUSTER.value and not needs_attention
                else "Resume the matching reviewed cluster explicitly to add this radio as its next member."
                if recommended_path == VarACClusterPath.JOIN_CLUSTER.value
                else "Choose a standalone node explicitly before creating a cluster."
                if needs_attention
                else "Existing cluster membership is never selected automatically."
            ),
            "new_radio_label": new_label,
        }
    )


__all__ = [
    "GuidedVarACConfigurationError", "VarACClusterIdentity", "VarACClusterMembership",
    "VarACClusterPath", "VarACConfigurationPlan", "VarACConfigurationRequest",
    "VarACNodeIdentity", "VarACPlanningInventory", "VarACResourceClaim",
    "VarACArrangementRecommendation", "VarACJoinChoice", "recommend_varac_arrangement",
    "varac_arrangement_recommendation", "recommend_varac_arrangement_from_snapshots",
    "normalize_cluster_id", "normalize_varac_path",
    "plan_varac_configuration",
]
