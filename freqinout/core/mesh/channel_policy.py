from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from freqinout.core.mesh.models import MeshChannel, MeshMessage

PUBLIC_CHANNEL_NAMES = {"public", "default", "primary", "longfast", "long fast", "channel 0"}
DIRECT_CHANNEL_ROLES = {"direct", "dm", "private-message"}
PRIVATE_CHANNEL_ROLES = {"private", "encrypted", "team", "group"}
TELEMETRY_CHANNEL_ROLES = {"admin", "telemetry", "position", "device", "health"}
KNOWN_RETENTION_WINDOWS = {"24h", "7d", "30d", "keep pinned", "none"}
CHANNEL_REVIEW_STATES = {"pending", "accepted", "ignored"}
CHANNEL_KEY_STATES = {"not_required", "device_configured", "needed", "saved"}
CHANNEL_DEFAULT_CATEGORIES = {"auto", "social", "ignore"}


@dataclass(frozen=True)
class MeshChannelPolicy:
    adapter_id: str
    transport: str
    channel_id: str
    channel_name: str
    channel_role: str = "unknown"
    channel_privacy: str = "unknown"
    mapped_groups: tuple[str, ...] = ()
    retention_window: str = "7d"
    inbox_enabled: bool = True
    ops_enabled: bool = True
    map_enabled: bool = True
    topic_scan_enabled: bool = True
    default_category: str = "auto"
    review_state: str = "pending"
    key_state: str = "not_required"
    key_hint: str = ""
    source: str = "device"
    updated_utc: str = ""

    @property
    def is_accepted(self) -> bool:
        return self.review_state == "accepted"

    @property
    def display_name(self) -> str:
        return self.channel_name or self.channel_id or "Channel"

    @property
    def requires_key(self) -> bool:
        return self.channel_role == "private" or self.channel_privacy == "encrypted"

    @property
    def key_available(self) -> bool:
        return not self.requires_key or self.key_state in {"device_configured", "saved"}

    @property
    def key_display_text(self) -> str:
        if not self.requires_key:
            return "Not needed"
        if self.key_state == "device_configured":
            return "Joined"
        if self.key_state == "saved":
            return self.key_hint or "Saved"
        return "Key needed"

    def applies_to_message(self, message: MeshMessage) -> bool:
        if self.adapter_id and self.adapter_id != message.adapter_id:
            return False
        if self.transport and self.transport != message.transport:
            return False
        message_channel = normalize_channel_id(message.channel)
        if message_channel == self.channel_id:
            return True
        if not message_channel and self.channel_role == "public" and self.channel_id in {"0", "public", "primary"}:
            return True
        if self.channel_role == "direct" and _message_looks_direct(message):
            return True
        return False


def normalize_channel_id(value: object) -> str:
    return str(value or "").strip()


def normalize_channel_role(value: object, *, channel_name: object = "", channel_id: object = "") -> str:
    role = str(value or "").strip().lower()
    name = str(channel_name or "").strip().lower()
    ident = str(channel_id or "").strip().lower()
    if role in DIRECT_CHANNEL_ROLES:
        return "direct"
    if role in TELEMETRY_CHANNEL_ROLES:
        return "telemetry"
    if role in PRIVATE_CHANNEL_ROLES:
        return "private"
    if role == "public" or name in PUBLIC_CHANNEL_NAMES or ident in {"0", "public", "primary"}:
        return "public"
    return role or "unknown"


def normalize_channel_privacy(value: object, *, role: object = "") -> str:
    privacy = str(value or "").strip().lower()
    if privacy in {"public", "encrypted", "direct", "unknown"}:
        return privacy
    normalized_role = normalize_channel_role(role)
    if normalized_role == "public":
        return "public"
    if normalized_role == "direct":
        return "direct"
    if normalized_role == "private":
        return "encrypted"
    return "unknown"


def normalize_retention_window(value: object, *, fallback: str = "7d") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in KNOWN_RETENTION_WINDOWS:
        return normalized
    return fallback


def normalize_default_category(value: object, *, fallback: str = "auto") -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in CHANNEL_DEFAULT_CATEGORIES else fallback


def normalize_review_state(value: object, *, fallback: str = "pending") -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in CHANNEL_REVIEW_STATES else fallback


def normalize_channel_key_state(
    value: object,
    *,
    role: object = "",
    privacy: object = "",
    source: object = "",
) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in CHANNEL_KEY_STATES:
        return normalized
    channel_role = normalize_channel_role(role)
    channel_privacy = normalize_channel_privacy(privacy, role=channel_role)
    if channel_role == "private" or channel_privacy == "encrypted":
        return "device_configured" if str(source or "").strip().lower() == "device" else "needed"
    return "not_required"


def default_policy_for_channel(
    *,
    adapter_id: str,
    transport: str,
    channel_id: object,
    channel_name: object = "",
    channel_role: object = "",
    channel_privacy: object = "",
    source: str = "device",
    review_state: str = "pending",
    key_state: object = "",
    key_hint: object = "",
) -> MeshChannelPolicy:
    ident = normalize_channel_id(channel_id)
    name = str(channel_name or "").strip() or (f"Channel {ident}" if ident else "Channel")
    role = normalize_channel_role(channel_role, channel_name=name, channel_id=ident)
    privacy = normalize_channel_privacy(channel_privacy, role=role)
    normalized_source = str(source or "device").strip().lower() or "device"
    normalized_key_state = normalize_channel_key_state(
        key_state,
        role=role,
        privacy=privacy,
        source=normalized_source,
    )
    normalized_key_hint = str(key_hint or "").strip()
    if role == "public":
        return MeshChannelPolicy(
            adapter_id=adapter_id,
            transport=transport,
            channel_id=ident,
            channel_name=name,
            channel_role=role,
            channel_privacy=privacy,
            retention_window="24h",
            inbox_enabled=True,
            ops_enabled=True,
            map_enabled=True,
            topic_scan_enabled=True,
            review_state=normalize_review_state(review_state),
            key_state=normalized_key_state,
            key_hint=normalized_key_hint,
            source=normalized_source,
        )
    if role == "direct":
        return MeshChannelPolicy(
            adapter_id=adapter_id,
            transport=transport,
            channel_id=ident,
            channel_name=name,
            channel_role=role,
            channel_privacy=privacy,
            retention_window="30d",
            inbox_enabled=True,
            ops_enabled=True,
            map_enabled=True,
            topic_scan_enabled=True,
            review_state=normalize_review_state(review_state),
            key_state=normalized_key_state,
            key_hint=normalized_key_hint,
            source=normalized_source,
        )
    if role == "telemetry":
        return MeshChannelPolicy(
            adapter_id=adapter_id,
            transport=transport,
            channel_id=ident,
            channel_name=name,
            channel_role=role,
            channel_privacy=privacy,
            retention_window="24h",
            inbox_enabled=False,
            ops_enabled=True,
            map_enabled=True,
            topic_scan_enabled=False,
            default_category="ignore",
            review_state=normalize_review_state(review_state),
            key_state=normalized_key_state,
            key_hint=normalized_key_hint,
            source=normalized_source,
        )
    return MeshChannelPolicy(
        adapter_id=adapter_id,
        transport=transport,
        channel_id=ident,
        channel_name=name,
        channel_role=role,
        channel_privacy=privacy,
        retention_window="7d",
        inbox_enabled=True,
        ops_enabled=True,
        map_enabled=True,
        topic_scan_enabled=True,
        review_state=normalize_review_state(review_state),
        key_state=normalized_key_state,
        key_hint=normalized_key_hint,
        source=normalized_source,
    )


def policy_from_channel(channel: MeshChannel, *, review_state: str = "pending") -> MeshChannelPolicy:
    channel_id = channel.channel_id or str(channel.index)
    return default_policy_for_channel(
        adapter_id=channel.adapter_id,
        transport=channel.transport,
        channel_id=channel_id,
        channel_name=channel.name,
        channel_role=channel.role,
        channel_privacy=channel.privacy,
        source="device",
        review_state=review_state,
        key_hint=channel.psk_hint,
    )


def policy_from_mapping(values: Mapping[str, object]) -> MeshChannelPolicy:
    mapped_groups = values.get("mapped_groups", ())
    if isinstance(mapped_groups, str):
        groups = tuple(part.strip().upper() for part in mapped_groups.split(",") if part.strip())
    elif isinstance(mapped_groups, Sequence):
        groups = tuple(str(part).strip().upper() for part in mapped_groups if str(part).strip())
    else:
        groups = ()
    fallback = default_policy_for_channel(
        adapter_id=str(values.get("adapter_id") or "").strip(),
        transport=str(values.get("transport") or "").strip().lower(),
        channel_id=values.get("channel_id"),
        channel_name=values.get("channel_name"),
        channel_role=values.get("channel_role"),
        channel_privacy=values.get("channel_privacy"),
        source=str(values.get("source") or "device").strip().lower() or "device",
        review_state=str(values.get("review_state") or "pending"),
        key_state=values.get("key_state"),
        key_hint=values.get("key_hint"),
    )

    def as_bool(name: str, default: bool) -> bool:
        value = values.get(name, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}

    return MeshChannelPolicy(
        adapter_id=fallback.adapter_id,
        transport=fallback.transport,
        channel_id=fallback.channel_id,
        channel_name=fallback.channel_name,
        channel_role=fallback.channel_role,
        channel_privacy=fallback.channel_privacy,
        mapped_groups=groups,
        retention_window=normalize_retention_window(values.get("retention_window"), fallback=fallback.retention_window),
        inbox_enabled=as_bool("inbox_enabled", fallback.inbox_enabled),
        ops_enabled=as_bool("ops_enabled", fallback.ops_enabled),
        map_enabled=as_bool("map_enabled", fallback.map_enabled),
        topic_scan_enabled=as_bool("topic_scan_enabled", fallback.topic_scan_enabled),
        default_category=normalize_default_category(
            values.get("default_category"),
            fallback=fallback.default_category,
        ),
        review_state=normalize_review_state(values.get("review_state"), fallback=fallback.review_state),
        key_state=normalize_channel_key_state(
            values.get("key_state"),
            role=fallback.channel_role,
            privacy=fallback.channel_privacy,
            source=values.get("source") or fallback.source,
        ),
        key_hint=str(values.get("key_hint") or fallback.key_hint).strip(),
        source=str(values.get("source") or fallback.source).strip().lower() or fallback.source,
        updated_utc=str(values.get("updated_utc") or "").strip(),
    )


def policy_allows_surface(policy: MeshChannelPolicy, surface: str) -> bool:
    if policy.review_state == "ignored":
        return False
    if policy.channel_role != "direct" and policy.review_state != "accepted":
        return False
    if not policy.key_available:
        return False
    normalized = str(surface or "").strip().lower()
    if normalized == "inbox":
        return policy.inbox_enabled
    if normalized in {"ops", "ops_center", "controlfreq"}:
        return policy.ops_enabled
    if normalized == "map":
        return policy.map_enabled
    if normalized in {"topic", "topic_scan", "topics"}:
        return policy.topic_scan_enabled
    return False


def message_allowed_for_surface(
    message: MeshMessage,
    policies: Sequence[MeshChannelPolicy],
    surface: str,
) -> bool:
    policy = policy_for_message(message, policies)
    return policy_allows_surface(policy, surface) if policy is not None else False


def policy_for_message(
    message: MeshMessage,
    policies: Sequence[MeshChannelPolicy],
) -> MeshChannelPolicy | None:
    for policy in policies:
        if policy.applies_to_message(message):
            return policy
    return None


def message_allowed_surfaces(
    message: MeshMessage,
    policies: Sequence[MeshChannelPolicy],
) -> tuple[str, ...]:
    policy = policy_for_message(message, policies)
    if policy is None:
        return ()
    surfaces: list[str] = []
    for surface in ("inbox", "ops_center", "map", "topic_scan"):
        if policy_allows_surface(policy, surface):
            surfaces.append(surface)
    return tuple(surfaces)


def _message_looks_direct(message: MeshMessage) -> bool:
    channel = normalize_channel_id(message.channel).lower()
    if channel in DIRECT_CHANNEL_ROLES:
        return True
    if channel:
        return False
    target = str(message.to_node or "").strip().lower()
    return bool(target and target not in {"^all", "all", "broadcast", "public", "channel"})
