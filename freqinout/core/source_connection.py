from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping, Sequence


SOURCE_CONNECTION_CONNECTED = "connected"
SOURCE_CONNECTION_RECONNECTING = "reconnecting"
SOURCE_CONNECTION_AWAY = "away"
SOURCE_CONNECTION_DISABLED = "disabled"
SOURCE_CONNECTION_CONFIG_ERROR = "config_error"


@dataclass(frozen=True)
class SourceConnectionSnapshot:
    """Protocol-neutral lifecycle state for local data sources."""

    source_id: str
    source_family: str
    display_name: str
    enabled: bool
    connected: bool
    lifecycle_state: str
    guidance: str = ""
    attention: bool = False
    required: bool = False
    last_rx_utc: str = ""
    last_tx_utc: str = ""
    updated_utc: str = ""
    last_error: str = ""
    warnings: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "source_family": self.source_family,
            "display_name": self.display_name,
            "enabled": self.enabled,
            "connected": self.connected,
            "lifecycle_state": self.lifecycle_state,
            "guidance": self.guidance,
            "attention": self.attention,
            "required": self.required,
            "last_rx_utc": self.last_rx_utc,
            "last_tx_utc": self.last_tx_utc,
            "updated_utc": self.updated_utc,
            "last_error": self.last_error,
            "warnings": self.warnings,
            "capabilities": self.capabilities,
        }


def source_connection_from_mesh_health(
    row: Mapping[str, object],
    *,
    now_utc: datetime | None = None,
    reconnecting_seconds: int = 90,
    away_seconds: int = 15 * 60,
) -> SourceConnectionSnapshot:
    """Project a retained Mesh health row into the shared source lifecycle contract."""

    now = now_utc or datetime.now(timezone.utc)
    source_id = _clean(row.get("adapter_id"))
    family = _clean(row.get("transport")) or "mesh"
    display_name = _clean(row.get("device_name")) or source_id or "Local Mesh"
    enabled = _truthy(row.get("enabled"))
    connected = _truthy(row.get("connected"))
    updated_utc = _clean(row.get("updated_utc"))
    last_rx_utc = _clean(row.get("last_rx_utc"))
    last_tx_utc = _clean(row.get("last_tx_utc"))
    last_error = _clean(row.get("last_error"))
    warnings = _warnings(row.get("warnings", row.get("warnings_json")))
    required = _truthy(row.get("required"))

    if not enabled:
        state = SOURCE_CONNECTION_DISABLED
        guidance = "Local mesh ingest is off."
    elif connected:
        state = SOURCE_CONNECTION_CONNECTED
        guidance = "Connected."
    elif _looks_like_config_error(last_error):
        state = SOURCE_CONNECTION_CONFIG_ERROR
        guidance = "Open Settings > Local Mesh to fix the connection."
    else:
        age = _age_seconds(updated_utc, now)
        if age is not None and age <= max(5, reconnecting_seconds):
            state = SOURCE_CONNECTION_RECONNECTING
            guidance = "Reconnecting."
        elif age is not None and age >= max(reconnecting_seconds, away_seconds):
            state = SOURCE_CONNECTION_AWAY
            guidance = "Device away. Retained mesh traffic and map data remain available."
        else:
            state = SOURCE_CONNECTION_RECONNECTING
            guidance = "Waiting for the mesh device."
    attention = bool(required and state not in {SOURCE_CONNECTION_CONNECTED, SOURCE_CONNECTION_DISABLED})
    if attention and state != SOURCE_CONNECTION_CONFIG_ERROR:
        guidance = guidance or "This source is required for the active view."
    return SourceConnectionSnapshot(
        source_id=source_id,
        source_family=family,
        display_name=display_name,
        enabled=enabled,
        connected=connected,
        lifecycle_state=state,
        guidance=guidance,
        attention=attention,
        required=required,
        last_rx_utc=last_rx_utc,
        last_tx_utc=last_tx_utc,
        updated_utc=updated_utc,
        last_error=last_error,
        warnings=warnings,
        capabilities=("inbox", "ops", "map", "topics"),
    )


def _clean(value: object) -> str:
    return str(value or "").strip()


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled", "connected"}


def _warnings(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        text = value.strip()
        return (text,) if text and not text.startswith("[") else ()
    if isinstance(value, Sequence):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


def _age_seconds(value: str, now: datetime) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (now - parsed.astimezone(timezone.utc)).total_seconds())


def _looks_like_config_error(message: str) -> bool:
    text = message.lower()
    return any(
        needle in text
        for needle in (
            "not configured",
            "missing",
            "invalid",
            "unsupported",
            "not available",
            "permission",
            "pair",
            "pin",
            "characteristic",
            "companion",
        )
    )
