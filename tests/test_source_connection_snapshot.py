from __future__ import annotations

from datetime import datetime, timezone

from freqinout.core.source_connection import (
    SOURCE_CONNECTION_AWAY,
    SOURCE_CONNECTION_CONFIG_ERROR,
    SOURCE_CONNECTION_CONNECTED,
    SOURCE_CONNECTION_DISABLED,
    SOURCE_CONNECTION_RECONNECTING,
    source_connection_from_mesh_health,
)


NOW = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def test_mesh_health_connected_projects_connected_lifecycle() -> None:
    snapshot = source_connection_from_mesh_health(
        {
            "adapter_id": "meshcore-mobl1",
            "transport": "meshcore",
            "device_name": "MeshCore-N1MAG MOBL1",
            "enabled": True,
            "connected": True,
            "updated_utc": "2026-09-01T11:59:50+00:00",
        },
        now_utc=NOW,
    )

    assert snapshot.lifecycle_state == SOURCE_CONNECTION_CONNECTED
    assert snapshot.connected is True
    assert snapshot.attention is False


def test_mesh_health_recent_disconnect_projects_reconnecting_lifecycle() -> None:
    snapshot = source_connection_from_mesh_health(
        {
            "adapter_id": "meshcore-mobl1",
            "transport": "meshcore",
            "enabled": True,
            "connected": False,
            "updated_utc": "2026-09-01T11:59:40+00:00",
        },
        now_utc=NOW,
    )

    assert snapshot.lifecycle_state == SOURCE_CONNECTION_RECONNECTING
    assert "Reconnecting" in snapshot.guidance


def test_mesh_health_old_disconnect_projects_away_lifecycle() -> None:
    snapshot = source_connection_from_mesh_health(
        {
            "adapter_id": "meshcore-mobl1",
            "transport": "meshcore",
            "enabled": True,
            "connected": False,
            "updated_utc": "2026-09-01T11:30:00+00:00",
        },
        now_utc=NOW,
    )

    assert snapshot.lifecycle_state == SOURCE_CONNECTION_AWAY
    assert "Retained mesh traffic" in snapshot.guidance


def test_mesh_health_config_error_projects_actionable_lifecycle() -> None:
    snapshot = source_connection_from_mesh_health(
        {
            "adapter_id": "meshcore-mobl1",
            "transport": "meshcore",
            "enabled": True,
            "connected": False,
            "last_error": "BLE device missing",
            "updated_utc": "2026-09-01T11:59:40+00:00",
        },
        now_utc=NOW,
    )

    assert snapshot.lifecycle_state == SOURCE_CONNECTION_CONFIG_ERROR
    assert "Settings > Local Mesh" in snapshot.guidance


def test_mesh_health_disabled_projects_disabled_lifecycle() -> None:
    snapshot = source_connection_from_mesh_health(
        {
            "adapter_id": "meshcore-mobl1",
            "transport": "meshcore",
            "enabled": False,
            "connected": False,
        },
        now_utc=NOW,
    )

    assert snapshot.lifecycle_state == SOURCE_CONNECTION_DISABLED
    assert snapshot.attention is False
