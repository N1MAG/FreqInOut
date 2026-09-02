from __future__ import annotations

from freqinout.core.mesh.settings import MeshConnectionConfig, MeshConnectionType
from freqinout.core.source_control_rail import (
    source_control_mesh_item_from_configs,
    source_control_mesh_items_from_configs,
)


def test_mesh_source_control_hides_unsaved_ble_discoveries() -> None:
    item = source_control_mesh_item_from_configs(
        (),
        (
            {
                "adapter_id": "97C92879-047E-FEA8-7A11-8A2EE82B381D",
                "device_name": "MeshCore-N1MAG MOBL1",
                "connected": True,
            },
        ),
    )

    assert item is None


def test_mesh_source_control_uses_saved_device_names_not_raw_ids() -> None:
    item = source_control_mesh_item_from_configs(
        (
            MeshConnectionConfig(
                adapter_id="meshcore-mobl1",
                protocol="meshcore",
                enabled=True,
                connection_type=MeshConnectionType.BLE,
                ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
                ble_device_name="MeshCore-N1MAG MOBL1",
            ),
        ),
        (
            {
                "adapter_id": "meshcore-mobl1",
                "device_name": "MeshCore-N1MAG MOBL1",
                "connected": True,
                "updated_utc": "2026-09-01T12:00:00+00:00",
            },
            {
                "adapter_id": "97C92879-047E-FEA8-7A11-8A2EE82B381D",
                "device_name": "",
                "connected": True,
                "updated_utc": "2026-09-01T12:00:01+00:00",
            },
        ),
    )

    assert item is not None
    assert item.label == "MeshCore"
    assert item.role == "eligible_success"
    assert "97C92879" not in item.tooltip
    assert tuple(action.label for action in item.actions) == ("Connect: N1MAG MOBL1",)


def test_mesh_source_control_aggregates_multiple_saved_sources() -> None:
    item = source_control_mesh_item_from_configs(
        (
            MeshConnectionConfig(
                adapter_id="meshcore-mobl1",
                protocol="meshcore",
                enabled=True,
                connection_type=MeshConnectionType.BLE,
                ble_device_name="MeshCore-N1MAG MOBL1",
            ),
            MeshConnectionConfig(
                adapter_id="meshcore-mobl2",
                protocol="meshcore",
                enabled=True,
                connection_type=MeshConnectionType.BLE,
                ble_device_name="MeshCore-N1MAG MOBL2",
            ),
        ),
        (
            {
                "adapter_id": "meshcore-mobl2",
                "device_name": "MeshCore-N1MAG MOBL2",
                "connected": False,
                "last_error": "Disconnected",
                "lifecycle_state": "away",
            },
        ),
    )

    assert item is not None
    assert item.label == "MeshCore (2)"
    assert item.role == "warning"
    assert tuple(action.label for action in item.actions) == (
        "Connect: N1MAG MOBL1",
        "Connect: N1MAG MOBL2",
    )


def test_mesh_source_control_groups_configured_devices_by_protocol() -> None:
    items = source_control_mesh_items_from_configs(
        (
            MeshConnectionConfig(
                adapter_id="meshcore-mobl1",
                protocol="meshcore",
                enabled=True,
                connection_type=MeshConnectionType.BLE,
                ble_device_name="MeshCore-N1MAG MOBL1",
            ),
            MeshConnectionConfig(
                adapter_id="meshtastic-main",
                protocol="meshtastic",
                enabled=True,
                connection_type=MeshConnectionType.TCP,
                tcp_host="192.0.2.10",
            ),
        )
    )

    assert tuple(item.label for item in items) == ("MeshCore", "Meshtastic")
    assert tuple(action.label for item in items for action in item.actions) == (
        "Connect: N1MAG MOBL1",
        "Connect: meshtastic-main",
    )
