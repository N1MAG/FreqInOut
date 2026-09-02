from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Mapping, Sequence

from freqinout.core.mesh.settings import MeshConnectionConfig


SOURCE_CONTROL_KIND_RADIO = "radio"
SOURCE_CONTROL_KIND_MESH = "mesh"


@dataclass(frozen=True)
class SourceControlAction:
    key: str
    label: str
    enabled: bool = True
    role: str = "muted"
    tooltip: str = ""


@dataclass(frozen=True)
class SourceControlItem:
    key: str
    kind: str
    label: str
    role: str = "muted"
    tooltip: str = ""
    focus_id: int = 0
    actions: tuple[SourceControlAction, ...] = ()


def source_control_mesh_item_from_configs(
    configs: Sequence[MeshConnectionConfig],
    health_rows: Sequence[Mapping[str, object]] = (),
) -> SourceControlItem | None:
    """Build the first configured mesh protocol control for legacy callers."""

    items = source_control_mesh_items_from_configs(configs, health_rows)
    return items[0] if items else None


def source_control_mesh_items_from_configs(
    configs: Sequence[MeshConnectionConfig],
    health_rows: Sequence[Mapping[str, object]] = (),
) -> tuple[SourceControlItem, ...]:
    """Build top-rail mesh controls from saved configs only, grouped by protocol."""

    saved = tuple(config for config in configs if _mesh_config_has_saved_endpoint(config))
    if not saved:
        return ()
    rows = tuple(row for row in health_rows if isinstance(row, Mapping))
    by_protocol: dict[str, list[MeshConnectionConfig]] = {}
    for config in saved:
        protocol = str(config.protocol or "mesh").strip().lower() or "mesh"
        by_protocol.setdefault(protocol, []).append(config)
    return tuple(
        _source_control_mesh_item_for_protocol(protocol, tuple(protocol_configs), rows)
        for protocol, protocol_configs in sorted(by_protocol.items(), key=lambda item: _mesh_protocol_label(item[0]).lower())
    )


def _source_control_mesh_item_for_protocol(
    protocol: str,
    saved: Sequence[MeshConnectionConfig],
    rows: Sequence[Mapping[str, object]],
) -> SourceControlItem:
    actions: list[SourceControlAction] = []
    connected = False
    warning = False
    names: list[str] = []
    for config in saved:
        name = _mesh_config_display_name(config)
        if name and name not in names:
            names.append(name)
        row = _best_health_row_for_config(config, rows)
        row_connected = bool(row.get("connected")) if row else False
        connected = connected or row_connected
        last_error = str(row.get("last_error") or "").strip() if row else ""
        lifecycle = str(row.get("lifecycle_state") or "").strip().lower() if row else ""
        warning = warning or bool(last_error or lifecycle == "config_error")
        label = f"Connect: {name}" if name else "Connect saved mesh"
        actions.append(
            SourceControlAction(
                key=f"connect:{config.adapter_id}",
                label=label,
                enabled=True,
                role="eligible_success" if row_connected else "muted",
                tooltip=f"Connect to saved mesh device {name}.",
            )
        )
    role = "eligible_success" if connected else ("warning" if warning else "muted")
    status = "connected" if connected else ("needs attention" if warning else "saved")
    suffix = f" ({len(saved)})" if len(saved) > 1 else ""
    base_label = _mesh_protocol_label(protocol)
    tooltip_names = ", ".join(names[:4])
    if len(names) > 4:
        tooltip_names = f"{tooltip_names}, +{len(names) - 4} more"
    return SourceControlItem(
        key=f"mesh:{protocol}",
        kind=SOURCE_CONTROL_KIND_MESH,
        label=f"{base_label}{suffix}",
        role=role,
        tooltip=f"{base_label} sources: {status}" + (f"\nSaved: {tooltip_names}" if tooltip_names else ""),
        actions=tuple(actions),
    )


def _mesh_config_has_saved_endpoint(config: MeshConnectionConfig) -> bool:
    return bool(
        str(config.ble_device_id or "").strip()
        or str(config.ble_device_name or "").strip()
        or str(config.tcp_host or "").strip()
        or str(config.serial_port or "").strip()
        or str(config.http_base_url or "").strip()
        or str(config.mqtt_broker or "").strip()
    )


def _mesh_config_display_name(config: MeshConnectionConfig) -> str:
    for value in (
        config.ble_device_name,
        config.adapter_id,
        config.ble_device_id,
        config.tcp_host,
        config.serial_port,
        config.http_base_url,
        config.mqtt_broker,
    ):
        text = str(value or "").strip()
        if text and not _looks_raw(text):
            return _strip_mesh_prefix(text)
    return "saved device"


def _mesh_protocol_label(protocol: str) -> str:
    normalized = str(protocol or "").strip().lower()
    if normalized == "meshcore":
        return "MeshCore"
    if normalized == "meshtastic":
        return "Meshtastic"
    return "Mesh"


def _best_health_row_for_config(
    config: MeshConnectionConfig,
    rows: Sequence[Mapping[str, object]],
) -> Mapping[str, object] | None:
    keys = {
        _normalize_identity(config.adapter_id),
        _normalize_identity(config.ble_device_id),
        _normalize_identity(config.ble_device_name),
    }
    keys.discard("")
    if not keys:
        return None
    matches = []
    for row in rows:
        row_keys = {
            _normalize_identity(row.get("adapter_id")),
            _normalize_identity(row.get("device_name")),
        }
        if keys.intersection(row_keys):
            matches.append(row)
    if not matches:
        return None
    return max(
        matches,
        key=lambda row: (
            2 if bool(row.get("connected")) else 0,
            0 if str(row.get("last_error") or "").strip() else 1,
            str(row.get("updated_utc") or ""),
        ),
    )


def _normalize_identity(value: object) -> str:
    text = _strip_mesh_prefix(str(value or "").strip().lower())
    return re.sub(r"[^a-z0-9]+", "", text)


def _strip_mesh_prefix(value: str) -> str:
    return re.sub(r"^(meshcore|mesh)[\s:_-]+", "", value, flags=re.IGNORECASE).strip()


def _looks_raw(value: str) -> bool:
    text = _strip_mesh_prefix(value)
    if re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", text):
        return True
    if re.fullmatch(r"(?:ble|scan)[:_-][0-9a-fA-F:-]{6,}", text):
        return True
    if re.fullmatch(r"[0-9a-fA-F:-]{12,}", text):
        return True
    return False
