from __future__ import annotations

import asyncio
import os
import sys
import types
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.mesh import (
    discover_meshcore_ble_devices,
    default_mesh_db_path,
    default_policy_for_channel,
    MESHCORE_COMPANION_DECODER_WARNING,
    MeshConnectionConfig,
    MeshConnectionManager,
    MeshConnectionType,
    MeshConnectionWorker,
    MeshChannel,
    MeshChannelPolicy,
    MeshCoreBleAdvertisement,
    MeshEventStoreSink,
    list_mesh_channel_policies,
    load_mesh_connection_configs,
    list_mesh_health,
    list_mesh_messages,
    list_mesh_nodes,
    list_mesh_source_connection_snapshots,
    message_allowed_for_surface,
    mesh_ingest_readiness,
    set_mesh_message_topic_override,
    MeshNode,
    mesh_node_source_ref,
    observation_from_mesh_message,
    policy_from_channel,
    prune_mesh_messages_by_channel_policy,
    stage_mesh_channel_policies_from_channels,
    mesh_source_ref,
    project_mesh_node_to_observation,
    project_mesh_message_to_observation,
    store_mesh_event,
    store_mesh_message_with_channel_policy,
    upsert_mesh_channel_policy,
    upsert_mesh_health,
    upsert_mesh_message,
    upsert_mesh_node,
    validate_mesh_connection_config,
)
from freqinout.core.mesh.adapter_base import MeshAdapter
from freqinout.core.mesh.meshcore_codec import (
    decode_meshcore_contact_frame,
    decode_meshcore_new_advert_frame,
    meshcore_companion_path_len_to_hops,
    normalize_meshcore_channels,
    normalize_meshcore_nodes,
    normalize_meshcore_waiting_messages,
)
from freqinout.core.mesh.models import MeshAdapterEvent, MeshHealthSnapshot, MeshMessage
from freqinout.core.mesh.settings import discover_serial_ports, serialize_mesh_connection_library
from freqinout.core.mesh.meshtastic_adapter import MeshConnectionError, MeshtasticLocalAdapter
from freqinout.core.mesh.meshcore_adapter import MeshCoreBleAdapter, MeshCoreBleCompanionClient, _AsyncioLoopRunner
from freqinout.core.controlfreq_awareness import is_awareness_traffic_observation
from freqinout.core.message_inbox_filters import mesh_row_is_inbox_message
from freqinout.core.observation_store import list_observations
from freqinout.core.sitrep_metadata import source_family_label, source_short_label


def test_usb_ble_and_tcp_config_validation_is_explicit() -> None:
    assert validate_mesh_connection_config(MeshConnectionConfig(enabled=False)) == ()

    serial_config = MeshConnectionConfig(enabled=True, connection_type=MeshConnectionType.SERIAL)
    ble_config = MeshConnectionConfig(enabled=True, connection_type=MeshConnectionType.BLE)
    tcp_config = MeshConnectionConfig(enabled=True, connection_type=MeshConnectionType.TCP)

    assert [issue.field for issue in validate_mesh_connection_config(serial_config)] == ["serial_port"]
    assert [issue.field for issue in validate_mesh_connection_config(ble_config)] == ["ble_device"]
    assert [issue.field for issue in validate_mesh_connection_config(tcp_config)] == ["tcp_host"]


def test_settings_mapping_uses_meshtastic_prefix_and_safe_defaults() -> None:
    config = MeshConnectionConfig.from_mapping(
        {
            "meshtastic_enabled": "true",
            "meshtastic_connection_type": "serial",
            "meshtastic_serial_port": "/dev/cu.usbmodem123",
            "meshtastic_send_enabled": "false",
        }
    )

    assert config.enabled is True
    assert config.connection_type is MeshConnectionType.SERIAL
    assert config.serial_port == "/dev/cu.usbmodem123"
    assert config.send_enabled is False
    assert config.store_messages_enabled is True


def test_mesh_config_loader_returns_only_enabled_connections() -> None:
    assert load_mesh_connection_configs({"meshtastic_enabled": False}) == ()

    configs = load_mesh_connection_configs(
        {
            "meshtastic_enabled": True,
            "meshtastic_adapter_id": "field-node",
            "meshtastic_connection_type": "tcp",
            "meshtastic_tcp_host": "192.0.2.20",
        }
    )

    assert len(configs) == 1
    assert configs[0].adapter_id == "field-node"
    assert configs[0].tcp_host == "192.0.2.20"


def test_mesh_config_loader_includes_saved_device_library() -> None:
    library = serialize_mesh_connection_library(
        (
            MeshConnectionConfig(
                adapter_id="meshcore-mobl1",
                protocol="meshcore",
                enabled=True,
                connection_type=MeshConnectionType.BLE,
                ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
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

    configs = load_mesh_connection_configs({"mesh_connection_library": library})

    assert tuple(config.protocol for config in configs) == ("meshcore", "meshtastic")
    assert tuple(config.adapter_id for config in configs) == ("meshcore-mobl1", "meshtastic-main")


def test_mesh_source_family_labels_are_user_facing() -> None:
    assert source_family_label("mesh_client") == "Meshtastic"
    assert source_short_label("meshcore") == "MCR"


def test_meshtastic_adapter_does_not_require_package_until_connect() -> None:
    adapter = MeshtasticLocalAdapter(MeshConnectionConfig(enabled=True, tcp_host="127.0.0.1"))

    with pytest.raises(MeshConnectionError, match="Meshtastic Python package"):
        adapter.connect()


def test_meshcore_ble_adapter_does_not_require_bleak_until_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    import freqinout.core.mesh.meshcore_adapter as meshcore_adapter

    monkeypatch.setattr(meshcore_adapter, "meshcore_ble_available", lambda: False)
    adapter = MeshCoreBleAdapter(
        MeshConnectionConfig(
            protocol="meshcore",
            enabled=True,
            connection_type=MeshConnectionType.BLE,
            ble_device_name="MeshCore Field",
        )
    )

    with pytest.raises(MeshConnectionError, match="bleak"):
        adapter.connect()


def test_meshcore_adapter_requires_ble_connection_type() -> None:
    adapter = MeshCoreBleAdapter(
        MeshConnectionConfig(protocol="meshcore", enabled=True, connection_type=MeshConnectionType.TCP, tcp_host="127.0.0.1")
    )

    with pytest.raises(MeshConnectionError, match="Bluetooth LE"):
        adapter.connect()


def test_meshcore_ble_pairing_errors_are_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    bleak_module = types.ModuleType("bleak")

    class FakeBleakClient:
        is_connected = False

        def __init__(self, address: str, timeout: int = 20) -> None:
            self.address = address
            self.timeout = timeout

        async def connect(self) -> None:
            raise RuntimeError("not authorized by bluetooth stack")

        async def disconnect(self) -> None:
            pass

    bleak_module.BleakClient = FakeBleakClient
    monkeypatch.setitem(sys.modules, "bleak", bleak_module)

    adapter = MeshCoreBleAdapter(
        MeshConnectionConfig(
            protocol="meshcore",
            enabled=True,
            connection_type=MeshConnectionType.BLE,
            ble_device_id="AA:BB:CC:DD:EE:FF",
        )
    )

    with pytest.raises(MeshConnectionError, match="PIN shown on the device"):
        adapter.connect()
    assert "pairing" in adapter.health().last_error.lower()


def test_meshcore_ble_connects_with_saved_device_id(monkeypatch: pytest.MonkeyPatch) -> None:
    bleak_module = types.ModuleType("bleak")
    captured: dict[str, object] = {}

    class FakeCharacteristic:
        def __init__(self, uuid: str) -> None:
            self.uuid = uuid

    class FakeService:
        uuid = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
        characteristics = (
            FakeCharacteristic("6e400002-b5a3-f393-e0a9-e50e24dcca9e"),
            FakeCharacteristic("6e400003-b5a3-f393-e0a9-e50e24dcca9e"),
        )

    class FakeBleakClient:
        def __init__(self, address: str, timeout: int = 20) -> None:
            captured["address"] = address
            captured["timeout"] = timeout
            self.is_connected = False

        async def connect(self) -> None:
            self.is_connected = True

        async def disconnect(self) -> None:
            self.is_connected = False
            captured["disconnected"] = True

        async def get_services(self) -> list[FakeService]:
            return [FakeService()]

    bleak_module.BleakClient = FakeBleakClient
    monkeypatch.setitem(sys.modules, "bleak", bleak_module)

    adapter = MeshCoreBleAdapter(
        MeshConnectionConfig(
            adapter_id="meshcore-field",
            protocol="meshcore",
            enabled=True,
            connection_type=MeshConnectionType.BLE,
            ble_device_id="AA:BB:CC:DD:EE:FF",
            ble_scan_timeout_sec=12,
        )
    )

    adapter.connect()

    assert captured["address"] == "AA:BB:CC:DD:EE:FF"
    assert captured["timeout"] == 12
    health = adapter.health()
    assert health.connected is True
    assert health.warnings == (MESHCORE_COMPANION_DECODER_WARNING,)

    adapter.disconnect()
    assert captured["disconnected"] is True


def test_meshcore_ble_companion_client_decodes_channels_and_waiting_messages() -> None:
    written: list[bytes] = []

    class FakeBleClient:
        is_connected = True

        def __init__(self) -> None:
            self.callback = None
            self.sync_count = 0

        async def start_notify(self, uuid: str, callback: object) -> None:
            self.callback = callback

        async def stop_notify(self, uuid: str) -> None:
            pass

        async def disconnect(self) -> None:
            self.is_connected = False

        async def write_gatt_char(self, uuid: str, payload: bytes, response: bool = False) -> None:
            written.append(bytes(payload))
            if self.callback is None:
                return
            if payload == bytes([31, 0]):
                self.callback(None, _meshcore_channel_frame(0, "Public", b"\x00" * 16))
            elif payload == bytes([31, 1]):
                self.callback(None, _meshcore_channel_frame(1, "Neighborhood", bytes(range(16))))
            elif payload == bytes([31, 2]):
                self.callback(None, bytes([1, 2]))
            elif payload == bytes([10]):
                self.sync_count += 1
                if self.sync_count == 1:
                    self.callback(None, _meshcore_channel_message_frame(1, 0x42, "road closure"))
                elif self.sync_count == 2:
                    self.callback(None, _meshcore_contact_message_frame(b"\xaa\xbb\xcc\xdd\xee\xff", 0xFF, "direct ping"))
                else:
                    self.callback(None, bytes([10]))

    raw = FakeBleClient()
    client = MeshCoreBleCompanionClient(raw, command_timeout_sec=0.5)

    async def run() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        await client.initialize()
        channels = await client.getChannels()
        messages = await client.getWaitingMessages()
        return channels, messages

    channels, messages = asyncio.run(run())

    assert written[0] == bytes([22, 3])
    assert [channel["name"] for channel in channels] == ["Public", "Neighborhood"]
    normalized_channels = normalize_meshcore_channels(channels, adapter_id="meshcore-field")
    assert normalized_channels[1].role == "private"
    assert normalized_channels[1].psk_hint == "device key"

    normalized_messages = normalize_meshcore_waiting_messages(messages, adapter_id="meshcore-field")
    assert [message.text for message in normalized_messages] == ["road closure", "direct ping"]
    assert normalized_messages[0].channel == "1"
    assert normalized_messages[0].hop_count == 2
    assert normalized_messages[1].channel == "direct"
    assert normalized_messages[1].direct_receive is True


def test_meshcore_channel_message_direct_route_keeps_channel_and_extracts_sender() -> None:
    normalized_messages = normalize_meshcore_waiting_messages(
        [
            {
                "channelMessage": {
                    "channelIdx": 2,
                    "pathLen": 0xFF,
                    "senderTimestamp": 1_788_202_024,
                    "text": "N1MAG MOBL2: Test",
                }
            }
        ],
        adapter_id="meshcore-mobl1",
    )

    assert len(normalized_messages) == 1
    message = normalized_messages[0]
    assert message.channel == "2"
    assert message.to_node == "channel"
    assert message.from_node == "N1MAG MOBL2"
    assert message.text == "Test"
    assert message.route_type == "direct"
    assert message.direct_receive is True
    assert message.hop_count == 0


def test_mesh_channel_zero_message_displays_as_public_without_policy() -> None:
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="public-no-policy",
        from_node="MOBILE1",
        to_node="channel",
        channel="0",
        text="testing public channel",
    )

    observation = observation_from_mesh_message(message)

    assert observation.to_target == "Public"
    assert observation.groups == ("Public",)


def test_meshcore_contact_frame_becomes_located_node() -> None:
    public_key = bytes.fromhex("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
    frame = bytearray(148)
    frame[0] = 3
    frame[1:33] = public_key
    frame[33] = 2
    frame[34] = 0
    frame[35] = 0xFF
    frame[36:100] = bytes(range(1, 65))
    frame[100:100 + len(b"K7MESH R1")] = b"K7MESH R1"
    frame[132:136] = (1_788_202_024).to_bytes(4, "little", signed=False)
    frame[136:140] = int(39.7392 * 1_000_000).to_bytes(4, "little", signed=True)
    frame[140:144] = int(-104.9903 * 1_000_000).to_bytes(4, "little", signed=True)
    frame[144:148] = (1_788_202_999).to_bytes(4, "little", signed=False)

    decoded = decode_meshcore_contact_frame(frame)
    assert decoded is not None
    nodes = normalize_meshcore_nodes([decoded], adapter_id="meshcore-field")

    assert len(nodes) == 1
    node = nodes[0]
    assert node.node_id == "aabbccddeeff"
    assert node.public_key_or_hash == public_key.hex()
    assert node.callsign == "K7MESH"
    assert node.display_name == "K7MESH"
    assert node.lat == pytest.approx(39.7392)
    assert node.lon == pytest.approx(-104.9903)
    assert node.hop_count == 0
    assert node.route_type == "direct"
    assert node.direct_receive is True


def test_meshcore_new_advert_push_becomes_located_node_event() -> None:
    public_key = bytes.fromhex("bbccddeeff00112233445566778899aabbccddeeff00112233445566778899aa")
    frame = bytearray(148)
    frame[0] = 0x8A
    frame[1:33] = public_key
    frame[33] = 2
    frame[34] = 0
    frame[35] = 2
    frame[100:100 + len(b"K7RTR EAST")] = b"K7RTR EAST"
    frame[132:136] = (1_788_202_024).to_bytes(4, "little", signed=False)
    frame[136:140] = int(40.015 * 1_000_000).to_bytes(4, "little", signed=True)
    frame[140:144] = int(-105.2705 * 1_000_000).to_bytes(4, "little", signed=True)

    decoded = decode_meshcore_new_advert_frame(frame)
    assert decoded is not None
    assert decoded["name"] == "K7RTR EAST"

    class CompanionClient:
        def raw_frames_pending(self):
            return (bytes(frame),)

        def getWaitingMessages(self):
            return []

    config = MeshConnectionConfig(
        adapter_id="meshcore-field",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="AA:BB",
    )
    adapter = MeshCoreBleAdapter(config)
    adapter._client = CompanionClient()

    events = list(adapter.receive_events())

    assert [event.event_type for event in events] == ["node"]
    assert events[0].node is not None
    assert events[0].node.node_id == "bbccddeeff00"
    assert events[0].node.callsign == "K7RTR"
    assert events[0].node.lat == pytest.approx(40.015)
    assert events[0].node.lon == pytest.approx(-105.2705)
    assert events[0].node.hop_count == 2


def test_meshcore_ble_adapter_lists_companion_contacts_as_nodes() -> None:
    class CompanionClient:
        def getContacts(self):
            return [
                {
                    "publicKey": "aabbccddeeff00112233445566778899",
                    "pubKeyPrefix": "aabbccddeeff",
                    "name": "N1MAG RTR",
                    "lat": 39.1,
                    "lon": -105.2,
                    "outPathLen": 2,
                    "rssi": -74,
                }
            ]

    config = MeshConnectionConfig(
        adapter_id="meshcore-field",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="AA:BB",
    )
    adapter = MeshCoreBleAdapter(config)
    adapter._client = CompanionClient()

    nodes = adapter.list_nodes()

    assert len(nodes) == 1
    assert nodes[0].node_id == "aabbccddeeff"
    assert nodes[0].callsign == "N1MAG"
    assert nodes[0].hop_count == 2
    assert nodes[0].route_type == "mesh"
    assert nodes[0].rssi == -74


def test_meshcore_ble_adapter_wraps_notify_capable_client_without_decoder_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    bleak_module = types.ModuleType("bleak")

    class FakeCharacteristic:
        def __init__(self, uuid: str) -> None:
            self.uuid = uuid

    class FakeService:
        uuid = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
        characteristics = (
            FakeCharacteristic("6e400002-b5a3-f393-e0a9-e50e24dcca9e"),
            FakeCharacteristic("6e400003-b5a3-f393-e0a9-e50e24dcca9e"),
        )

    class FakeBleakClient:
        def __init__(self, address: str, timeout: int = 20) -> None:
            self.address = address
            self.timeout = timeout
            self.is_connected = False

        async def connect(self) -> None:
            self.is_connected = True

        async def disconnect(self) -> None:
            self.is_connected = False

        async def get_services(self) -> list[FakeService]:
            return [FakeService()]

        async def start_notify(self, uuid: str, callback: object) -> None:
            pass

        async def stop_notify(self, uuid: str) -> None:
            pass

        async def write_gatt_char(self, uuid: str, payload: bytes, response: bool = False) -> None:
            pass

    bleak_module.BleakClient = FakeBleakClient
    monkeypatch.setitem(sys.modules, "bleak", bleak_module)

    adapter = MeshCoreBleAdapter(
        MeshConnectionConfig(
            adapter_id="meshcore-field",
            protocol="meshcore",
            enabled=True,
            connection_type=MeshConnectionType.BLE,
            ble_device_id="AA:BB:CC:DD:EE:FF",
        )
    )

    adapter.connect()

    health = adapter.health()
    assert health.connected is True
    assert MESHCORE_COMPANION_DECODER_WARNING not in health.warnings


def test_mesh_ingest_readiness_guides_empty_channel_review() -> None:
    readiness = mesh_ingest_readiness(policies=())

    assert readiness.state == "needs_channels"
    assert readiness.accepted_count == 0
    assert "Stage Public + Direct" in readiness.summary()


def test_mesh_ingest_readiness_blocks_private_channel_without_key() -> None:
    policy = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="neighborhood",
        channel_name="Neighborhood",
        channel_role="private",
        channel_privacy="encrypted",
        source="manual",
        review_state="pending",
    )

    readiness = mesh_ingest_readiness(policies=(policy,))

    assert readiness.state == "needs_key"
    assert readiness.key_needed_count == 1
    assert "Private key needed for Neighborhood" in readiness.summary()
    assert "Mark Joined" in readiness.summary()


def test_mesh_ingest_readiness_reports_decoder_boundary_after_channel_acceptance() -> None:
    policy = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        channel_privacy="public",
        source="default",
        review_state="accepted",
    )

    readiness = mesh_ingest_readiness(
        policies=(policy,),
        health_rows=({"warnings": [MESHCORE_COMPANION_DECODER_WARNING]},),
    )

    assert readiness.state == "decoder_needed"
    assert readiness.accepted_count == 1
    assert "Companion decoder" in readiness.summary()


def test_mesh_ingest_readiness_ready_when_accepted_without_ingest_warning() -> None:
    policy = default_policy_for_channel(
        adapter_id="local-mesh",
        transport="meshtastic",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        channel_privacy="public",
        source="default",
        review_state="accepted",
    )

    readiness = mesh_ingest_readiness(policies=(policy,), health_rows=({"warnings": []},))

    assert readiness.state == "ready"
    assert readiness.is_ready is True
    assert readiness.accepted_count == 1


def test_meshcore_companion_channel_discovery_marks_private_as_joined() -> None:
    class CompanionClient:
        def getChannels(self):
            return [
                {"index": 0, "name": "Public", "secret": []},
                {"index": 2, "name": "Neighborhood", "secret": [1, 2, 3, 4]},
            ]

    config = MeshConnectionConfig(
        adapter_id="meshcore-field",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="AA:BB",
    )
    adapter = MeshCoreBleAdapter(config)
    adapter._client = CompanionClient()

    channels = adapter.list_channels()
    policies = [policy_from_channel(channel) for channel in channels]
    by_name = {policy.display_name: policy for policy in policies}

    assert by_name["Public"].channel_role == "public"
    assert by_name["Neighborhood"].channel_role == "private"
    assert by_name["Neighborhood"].key_state == "device_configured"
    assert by_name["Neighborhood"].key_available is True
    assert "Direct" in by_name


def test_meshcore_companion_waiting_messages_become_events() -> None:
    class CompanionClient:
        def getWaitingMessages(self):
            return [
                {
                    "channelMessage": {
                        "channelIdx": 2,
                        "senderTimestamp": 1_788_120_000,
                        "text": "road closure near bridge",
                        "pathLen": 0x42,
                    }
                },
                {
                    "contactMessage": {
                        "pubKeyPrefix": [0xAA, 0xBB, 0xCC],
                        "senderTimestamp": 1_788_120_010,
                        "text": "direct check",
                        "pathLen": 0xFF,
                    }
                },
            ]

    config = MeshConnectionConfig(
        adapter_id="meshcore-field",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="AA:BB",
    )
    adapter = MeshCoreBleAdapter(config)
    adapter._client = CompanionClient()

    events = list(adapter.receive_events())

    assert [event.event_type for event in events] == ["message", "message"]
    assert events[0].message is not None
    assert events[0].message.channel == "2"
    assert events[0].message.text == "road closure near bridge"
    assert events[0].message.hop_count == 2
    assert events[0].message.direct_receive is False
    assert events[1].message is not None
    assert events[1].message.channel == "direct"
    assert events[1].message.from_node == "aabbcc"
    assert events[1].message.hop_count == 0
    assert events[1].message.direct_receive is True


def test_meshcore_companion_path_len_to_hops() -> None:
    assert meshcore_companion_path_len_to_hops(0xFF) == 0
    assert meshcore_companion_path_len_to_hops(0x42) == 2
    assert meshcore_companion_path_len_to_hops(-1) is None
    assert meshcore_companion_path_len_to_hops(256) is None


def test_meshcore_ble_scans_for_saved_device_name(monkeypatch: pytest.MonkeyPatch) -> None:
    bleak_module = types.ModuleType("bleak")
    captured: dict[str, object] = {}

    class FakeDevice:
        name = "MeshCore Field"
        address = "11:22:33:44:55:66"

    class FakeScanner:
        @staticmethod
        async def discover(timeout: int) -> list[FakeDevice]:
            captured["scan_timeout"] = timeout
            return [FakeDevice()]

    class FakeBleakClient:
        def __init__(self, address: str, timeout: int = 20) -> None:
            captured["address"] = address
            self.is_connected = False

        async def connect(self) -> None:
            self.is_connected = True

        async def disconnect(self) -> None:
            self.is_connected = False

        async def get_services(self) -> list:
            return []

    bleak_module.BleakClient = FakeBleakClient
    bleak_module.BleakScanner = FakeScanner
    monkeypatch.setitem(sys.modules, "bleak", bleak_module)

    adapter = MeshCoreBleAdapter(
        MeshConnectionConfig(
            protocol="meshcore",
            enabled=True,
            connection_type=MeshConnectionType.BLE,
            ble_device_name="meshcore field",
        )
    )

    adapter.connect()

    assert captured["scan_timeout"] == 20
    assert captured["address"] == "11:22:33:44:55:66"
    assert adapter.health().device_name == "meshcore field"


def test_meshcore_ble_discovery_filters_companion_advertisements(monkeypatch: pytest.MonkeyPatch) -> None:
    import freqinout.core.mesh.meshcore_adapter as meshcore_adapter

    bleak_module = types.ModuleType("bleak")
    captured: dict[str, object] = {}

    class FakeDevice:
        def __init__(self, name: str, address: str) -> None:
            self.name = name
            self.address = address

    class FakeAdvertisement:
        def __init__(self, local_name: str, rssi: int, uuids: list[str]) -> None:
            self.local_name = local_name
            self.rssi = rssi
            self.service_uuids = uuids

    class FakeScanner:
        @staticmethod
        async def discover(timeout: int, return_adv: bool = False) -> dict[str, tuple[FakeDevice, FakeAdvertisement]]:
            captured["timeout"] = timeout
            captured["return_adv"] = return_adv
            return {
                "mesh": (
                    FakeDevice("", "97C92879-047E-FEA8-7A11-8A2EE82B381D"),
                    FakeAdvertisement(
                        "MeshCore-N1MAG MOBL1",
                        -61,
                        ["6e400001-b5a3-f393-e0a9-e50e24dcca9e"],
                    ),
                ),
                "noise": (
                    FakeDevice("Keyboard", "AA"),
                    FakeAdvertisement("Keyboard", -20, ["180f"]),
                ),
            }

    bleak_module.BleakScanner = FakeScanner
    monkeypatch.setitem(sys.modules, "bleak", bleak_module)
    monkeypatch.setattr(meshcore_adapter, "meshcore_ble_available", lambda: True)

    devices = discover_meshcore_ble_devices(timeout_sec=7)

    assert captured == {"timeout": 7, "return_adv": True}
    assert len(devices) == 1
    assert devices[0].name == "MeshCore-N1MAG MOBL1"
    assert devices[0].address == "97C92879-047E-FEA8-7A11-8A2EE82B381D"
    assert devices[0].rssi == -61


def test_meshtastic_text_packet_normalizes_to_view_context() -> None:
    adapter = MeshtasticLocalAdapter(MeshConnectionConfig(adapter_id="local-mesh"))
    message = adapter.normalize_text_packet(
        {
            "id": 123,
            "fromId": "!abc",
            "toId": "^all",
            "channel": 0,
            "rxTime": 1_788_122_400,
            "rxSnr": 4.5,
            "rxRssi": -91,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "Fire spotting near the ridge"},
        }
    )

    assert message is not None
    assert message.transport == "meshtastic"
    assert message.text == "Fire spotting near the ridge"
    assert message.rx_time == datetime.fromtimestamp(1_788_122_400, tz=timezone.utc)

    context = message.as_traffic_context()
    assert context["source_family"] == "meshtastic"
    assert context["source_ref"] == "123"
    assert context["from_actor"] == "!abc"
    assert context["summary"] == "Fire spotting near the ridge"


def test_meshtastic_tcp_connect_is_lazy_and_mockable(monkeypatch: pytest.MonkeyPatch) -> None:
    meshtastic = types.ModuleType("meshtastic")
    tcp_module = types.ModuleType("meshtastic.tcp_interface")
    captured: dict[str, object] = {}

    class FakeTCPInterface:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        def close(self) -> None:
            captured["closed"] = True

    tcp_module.TCPInterface = FakeTCPInterface
    monkeypatch.setitem(sys.modules, "meshtastic", meshtastic)
    monkeypatch.setitem(sys.modules, "meshtastic.tcp_interface", tcp_module)

    adapter = MeshtasticLocalAdapter(MeshConnectionConfig(enabled=True, tcp_host="192.0.2.10"))
    adapter.connect()

    assert captured == {"hostname": "192.0.2.10"}
    assert adapter.health().connected is True

    adapter.disconnect()
    assert captured["closed"] is True


def test_meshtastic_receive_events_are_drained_once() -> None:
    adapter = MeshtasticLocalAdapter(MeshConnectionConfig(adapter_id="local-mesh"))
    adapter.ingest_packet({"id": "1", "decoded": {"text": "first"}})

    first_poll = list(adapter.receive_events())
    second_poll = list(adapter.receive_events())

    assert len(first_poll) == 1
    assert first_poll[0].message is not None
    assert first_poll[0].message.text == "first"
    assert second_poll == []


def test_meshtastic_connect_subscribes_and_disconnect_unsubscribes_pubsub(monkeypatch: pytest.MonkeyPatch) -> None:
    meshtastic = types.ModuleType("meshtastic")
    tcp_module = types.ModuleType("meshtastic.tcp_interface")
    pubsub_module = types.ModuleType("pubsub")
    subscriptions: list[tuple[object, str]] = []
    unsubscriptions: list[tuple[object, str]] = []

    class FakeTCPInterface:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def close(self) -> None:
            pass

    class FakePub:
        def subscribe(self, callback: object, topic: str) -> None:
            subscriptions.append((callback, topic))

        def unsubscribe(self, callback: object, topic: str) -> None:
            unsubscriptions.append((callback, topic))

    tcp_module.TCPInterface = FakeTCPInterface
    pubsub_module.pub = FakePub()
    monkeypatch.setitem(sys.modules, "meshtastic", meshtastic)
    monkeypatch.setitem(sys.modules, "meshtastic.tcp_interface", tcp_module)
    monkeypatch.setitem(sys.modules, "pubsub", pubsub_module)

    adapter = MeshtasticLocalAdapter(MeshConnectionConfig(enabled=True, tcp_host="192.0.2.10"))
    adapter.connect()
    subscriptions[0][0]({"id": "packet", "decoded": {"text": "hello mesh"}})
    events = list(adapter.receive_events())
    adapter.disconnect()

    assert [topic for _callback, topic in subscriptions] == ["meshtastic.receive.text", "meshtastic.receive"]
    assert [topic for _callback, topic in unsubscriptions] == ["meshtastic.receive.text", "meshtastic.receive"]
    assert events[0].message is not None
    assert events[0].message.text == "hello mesh"


def test_serial_port_discovery_is_optional_when_pyserial_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    import freqinout.core.mesh.settings as mesh_settings

    def missing_import(name: str):
        raise ImportError("serial unavailable")

    monkeypatch.setattr(mesh_settings, "import_module", missing_import)

    assert mesh_settings.discover_serial_ports() == ()


def test_settings_local_mesh_panel_builds_and_persists_payload(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "fio-config"))

    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    tab = SettingsTab()

    tab.mesh_enabled_chk.setChecked(True)
    tab._set_combo_data_if_present(tab.mesh_protocol_combo, "meshcore", fallback="meshtastic")
    tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "serial", fallback="tcp")
    tab.mesh_adapter_id_edit.setText("meshcore-field")
    tab.mesh_serial_port_combo.setCurrentText("/dev/cu.usbmesh")
    tab.mesh_send_enabled_chk.setChecked(False)
    app.processEvents()

    payload = tab._mesh_settings_payload_from_ui()

    assert payload["meshtastic_protocol"] == "meshcore"
    assert payload["meshtastic_enabled"] is True
    assert payload["meshtastic_connection_type"] == "serial"
    assert payload["meshtastic_serial_port"] == "/dev/cu.usbmesh"
    assert payload["meshtastic_send_enabled"] is False
    assert "Ready to configure Meshcore over SERIAL" in tab.mesh_status_label.text()
    assert hasattr(tab, "mesh_ble_scan_btn")
    assert hasattr(tab, "mesh_ble_results_combo")


def test_settings_meshcore_ble_scan_selection_populates_identity(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "fio-config"))

    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    tab = SettingsTab()
    tab.mesh_enabled_chk.setChecked(True)
    tab._set_combo_data_if_present(tab.mesh_protocol_combo, "meshcore", fallback="meshtastic")
    tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "ble", fallback="tcp")
    app.processEvents()

    tab._on_mesh_ble_scan_finished(
        (
            MeshCoreBleAdvertisement(
                name="MeshCore-N1MAG MOBL2",
                address="B8752C73-007D-D8AF-D938-AE58CB78A414",
                rssi=-48,
            ),
        )
    )
    tab._on_mesh_ble_use_selected_clicked()

    assert tab.mesh_ble_device_id_edit.text() == "B8752C73-007D-D8AF-D938-AE58CB78A414"
    assert tab.mesh_ble_device_name_edit.text() == "MeshCore-N1MAG MOBL2"
    assert "Using MeshCore BLE device MeshCore-N1MAG MOBL2" in tab.mesh_status_label.text()


def test_settings_mesh_channel_review_stages_accepts_and_ignores_defaults(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "fio-config"))

    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    tab = SettingsTab()
    tab.mesh_enabled_chk.setChecked(True)
    tab._set_combo_data_if_present(tab.mesh_protocol_combo, "meshcore", fallback="meshtastic")
    tab.mesh_adapter_id_edit.setText("meshcore-field")
    app.processEvents()

    tab._stage_mesh_default_channels()

    policies = list_mesh_channel_policies(default_mesh_db_path(), adapter_id="meshcore-field", transport="meshcore")
    assert [policy.display_name for policy in policies] == ["Direct", "Public"]
    assert {policy.review_state for policy in policies} == {"pending"}
    assert tab.mesh_channel_policy_table.rowCount() == 2

    tab.mesh_channel_policy_table.selectRow(0)
    tab._set_selected_mesh_channel_review_state("accepted")
    tab.mesh_channel_policy_table.clearSelection()
    tab.mesh_channel_policy_table.selectRow(1)
    tab._set_selected_mesh_channel_review_state("ignored")

    policies_by_name = {
        policy.display_name: policy
        for policy in list_mesh_channel_policies(default_mesh_db_path(), adapter_id="meshcore-field", transport="meshcore")
    }
    assert policies_by_name["Direct"].review_state == "accepted"
    assert policies_by_name["Public"].review_state == "ignored"
    assert policies_by_name["Public"].inbox_enabled is False
    assert policies_by_name["Public"].topic_scan_enabled is False


def test_settings_mesh_channel_review_orders_named_feeds_before_generated_pending_channels() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    policies = [
        default_policy_for_channel(
            adapter_id="meshcore-mobl1",
            transport="meshcore",
            channel_id="10",
            channel_name="Channel 10",
            channel_role="private",
            source="device",
            review_state="pending",
        ),
        default_policy_for_channel(
            adapter_id="meshcore-mobl1",
            transport="meshcore",
            channel_id="2",
            channel_name="COMAGNET",
            channel_role="private",
            source="device",
            review_state="accepted",
        ),
        default_policy_for_channel(
            adapter_id="meshcore-mobl1",
            transport="meshcore",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            source="device",
            review_state="accepted",
        ),
        default_policy_for_channel(
            adapter_id="meshcore-mobl1",
            transport="meshcore",
            channel_id="9",
            channel_name="Channel 9",
            channel_role="private",
            source="device",
            review_state="pending",
        ),
    ]

    ordered = sorted(policies, key=SettingsTab._mesh_channel_policy_sort_key)

    assert [policy.display_name for policy in ordered] == ["COMAGNET", "Public", "Channel 9", "Channel 10"]
    assert SettingsTab._mesh_channel_key_table_text(ordered[2]) == "On device"
    assert SettingsTab._mesh_channel_key_table_text(ordered[0]) == "Joined"


def test_settings_mesh_channel_review_dark_theme_uses_readable_semantic_colors() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class FakeSettings:
        def get(self, key: str, default=None):
            if key == "ui_theme":
                return "dark"
            return default

    tab = SettingsTab.__new__(SettingsTab)
    tab.settings = FakeSettings()

    accepted_bg, accepted_fg = SettingsTab._mesh_channel_policy_brushes(tab, "accepted")
    pending_bg, pending_fg = SettingsTab._mesh_channel_policy_brushes(tab, "pending")

    assert accepted_bg.color().name().lower() == "#173822"
    assert accepted_fg.color().name().lower() == "#e8f6ea"
    assert pending_bg.color().name().lower() == "#3a3015"
    assert pending_fg.color().name().lower() == "#fff0be"


def test_settings_theme_refresh_rebuilds_mesh_channel_table_brushes() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8-sig")
    apply_start = source.index("def apply_theme(self):")
    apply_block = source[apply_start : source.index("def resizeEvent", apply_start)]

    assert 'hasattr(self, "mesh_channel_policy_table")' in apply_block
    assert "self._refresh_mesh_channel_table()" in apply_block


def test_settings_mesh_display_device_prefers_human_name_over_raw_ble_identifier() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    config = MeshConnectionConfig(
        enabled=True,
        protocol="meshcore",
        connection_type="ble",
        adapter_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_name="MeshCore-N1MAG MOBL1",
    )

    row = {
        "adapter_id": "97C92879-047E-FEA8-7A11-8A2EE82B381D",
        "device_name": "",
    }

    assert SettingsTab._mesh_display_device_name(row, config) == "MeshCore-N1MAG MOBL1"


def _meshcore_channel_frame(index: int, name: str, secret: bytes) -> bytes:
    encoded_name = name.encode("utf-8")[:31]
    return bytes([18, index]) + encoded_name + b"\x00" * (32 - len(encoded_name)) + secret[:16].ljust(16, b"\x00")


def _meshcore_channel_message_frame(channel_idx: int, path_len: int, text: str) -> bytes:
    return (
        bytes([8, channel_idx & 0xFF, path_len & 0xFF, 0])
        + int(1_788_120_000).to_bytes(4, "little")
        + text.encode("utf-8")
    )


def _meshcore_contact_message_frame(prefix: bytes, path_len: int, text: str) -> bytes:
    return (
        bytes([7])
        + prefix[:6].ljust(6, b"\x00")
        + bytes([path_len & 0xFF, 0])
        + int(1_788_120_010).to_bytes(4, "little")
        + text.encode("utf-8")
    )


class FakeMeshAdapter:
    transport_name = "meshtastic"

    def __init__(self, config: MeshConnectionConfig) -> None:
        self.config = config
        self.adapter_id = config.adapter_id
        self.connected = False
        self.disconnected = False
        self.messages: list[MeshMessage] = []
        self.nodes: list[MeshNode] = []
        self.channels: list[MeshChannel] = []

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False
        self.disconnected = True

    def health(self) -> MeshHealthSnapshot:
        return MeshHealthSnapshot(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            enabled=self.config.enabled,
            connected=self.connected,
            connection_type=self.config.connection_type.value,
            device_name=self.config.endpoint_address,
        )

    def list_nodes(self) -> list[MeshNode]:
        return list(self.nodes)

    def list_channels(self) -> list[MeshChannel]:
        return list(self.channels)

    def get_recent_messages(self) -> list[MeshMessage]:
        return list(self.messages)

    def receive_events(self):
        messages = tuple(self.messages)
        self.messages.clear()
        for message in messages:
            yield MeshAdapterEvent(
                event_type="message",
                adapter_id=self.adapter_id,
                transport=self.transport_name,
                message=message,
            )

    def ingest_packet(self, packet):
        text = str(packet.get("text") or "").strip()
        if not text:
            return None
        message = MeshMessage(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            message_id=str(packet.get("id") or len(self.messages) + 1),
            text=text,
        )
        self.messages.append(message)
        return message


def test_mesh_connection_manager_starts_enabled_adapters_and_publishes_health() -> None:
    created: list[str] = []
    events: list[MeshAdapterEvent] = []

    def factory(config: MeshConnectionConfig) -> MeshAdapter:
        created.append(config.adapter_id)
        return FakeMeshAdapter(config)

    manager = MeshConnectionManager(
        [
            MeshConnectionConfig(adapter_id="off", enabled=False),
            MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2"),
        ],
        adapter_factory=factory,
    )
    manager.add_listener(events.append)

    snapshots = manager.start_all()

    assert tuple(snapshots) == ("local",)
    assert created == ["local"]
    assert snapshots["local"].connected is True
    assert [event.event_type for event in events] == ["health"]


def test_mesh_connection_manager_reports_factory_errors_without_crashing() -> None:
    def failing_factory(config: MeshConnectionConfig) -> MeshAdapter:
        raise MeshConnectionError("missing dependency")

    manager = MeshConnectionManager(
        [MeshConnectionConfig(adapter_id="meshcore", protocol="meshcore", enabled=True, tcp_host="192.0.2.4")],
        adapter_factory=failing_factory,
    )

    snapshot = manager.start_adapter("meshcore")

    assert snapshot.connected is False
    assert snapshot.transport == "meshcore"
    assert snapshot.last_error == "missing dependency"


def test_mesh_connection_manager_ingests_packets_as_events() -> None:
    manager = MeshConnectionManager(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=FakeMeshAdapter,
    )
    events: list[MeshAdapterEvent] = []
    manager.add_listener(events.append)
    manager.start_adapter("local")

    message = manager.ingest_packet("local", {"id": "abc", "text": "mesh check-in"})

    assert message is not None
    assert message.as_traffic_context()["summary"] == "mesh check-in"
    assert [event.event_type for event in events] == ["health", "message"]


def test_mesh_connection_manager_polls_adapter_channels() -> None:
    manager = MeshConnectionManager(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=FakeMeshAdapter,
    )
    manager.start_adapter("local")
    adapter = manager._adapters["local"]
    adapter.channels.append(MeshChannel(adapter_id="local", transport="meshtastic", index=0, name="Public"))

    channels = manager.poll_channels("local")

    assert len(channels) == 1
    assert channels[0].name == "Public"


def test_mesh_connection_manager_replaces_adapter_when_config_changes() -> None:
    created: list[str] = []

    def factory(config: MeshConnectionConfig) -> MeshAdapter:
        created.append(config.endpoint_address)
        return FakeMeshAdapter(config)

    manager = MeshConnectionManager(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=factory,
    )
    assert manager.start_adapter("local").device_name == "192.0.2.2"

    manager.upsert_config(MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.3"))
    assert manager.start_adapter("local").device_name == "192.0.2.3"

    assert created == ["192.0.2.2", "192.0.2.3"]


def test_mesh_message_store_upserts_and_filters_messages(tmp_path) -> None:
    db_path = tmp_path / "mesh.db"
    message = MeshMessage(
        adapter_id="local",
        transport="meshtastic",
        message_id="abc",
        from_node="NODE1",
        to_node="^all",
        channel="0",
        portnum="TEXT_MESSAGE_APP",
        text="Road closure at bridge",
        rx_time=datetime.fromtimestamp(1_788_122_400, tz=timezone.utc),
        topics=("Travel",),
        severity="watch",
        raw={"id": "abc"},
    )

    source_ref = upsert_mesh_message(db_path, message)
    rows = list_mesh_messages(db_path, adapter_id="local", channel="0")

    assert source_ref == mesh_source_ref(message)
    assert len(rows) == 1
    assert rows[0]["text"] == "Road closure at bridge"
    assert set(rows[0]["topics"]) == {"Travel", "Travel/Roads", "Infrastructure"}
    assert rows[0]["raw_payload"] == {"id": "abc"}


def test_mesh_message_store_preserves_route_context(tmp_path) -> None:
    db_path = tmp_path / "mesh-route.db"
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="route-msg",
        text="direct neighborhood check",
        from_node="K7MESH",
        channel="Public",
        hop_count=2,
        route_type="flood",
        direct_receive=False,
        via_node="relay-1",
        path_hops=("relay-1", "relay-2"),
        snr=7.5,
        rssi=-82,
    )

    upsert_mesh_message(db_path, message)

    row = list_mesh_messages(db_path, transport="meshcore")[0]
    assert row["route_type"] == "flood"
    assert row["direct_receive"] is False
    assert row["via_node"] == "relay-1"
    assert row["path_hops"] == ("relay-1", "relay-2")


def test_mesh_channel_policy_defaults_match_public_private_direct_and_telemetry() -> None:
    public = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
    )
    private = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="team",
        channel_name="County Team",
        channel_role="private",
    )
    direct = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="dm",
        channel_name="Direct",
        channel_role="direct",
    )
    telemetry = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="telemetry",
        channel_name="Telemetry",
        channel_role="telemetry",
    )

    assert public.retention_window == "24h"
    assert public.inbox_enabled is True
    assert public.topic_scan_enabled is True
    assert private.retention_window == "7d"
    assert direct.retention_window == "30d"
    assert telemetry.inbox_enabled is False
    assert telemetry.ops_enabled is True
    assert telemetry.topic_scan_enabled is False


def test_mesh_channel_policy_review_state_gates_surfaces() -> None:
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="m1",
        text="public smoke report",
        channel="0",
    )
    pending = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="pending",
    )
    accepted = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )

    assert message_allowed_for_surface(message, [pending], "inbox") is False
    assert message_allowed_for_surface(message, [accepted], "inbox") is True
    assert message_allowed_for_surface(message, [accepted], "ops_center") is True
    assert message_allowed_for_surface(message, [accepted], "topic_scan") is True


def test_mesh_channel_policy_persists_reviewed_channel_choices(tmp_path) -> None:
    db_path = tmp_path / "mesh-channel-policy.db"
    policy = MeshChannelPolicy(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="public",
        channel_name="Public",
        channel_role="public",
        channel_privacy="public",
        mapped_groups=("MAGNET", "COMMUNITY"),
        retention_window="24h",
        inbox_enabled=True,
        ops_enabled=True,
        map_enabled=True,
        topic_scan_enabled=True,
        review_state="accepted",
        source="device",
    )

    upsert_mesh_channel_policy(db_path, policy, updated_utc="2026-08-31T12:00:00+00:00")
    rows = list_mesh_channel_policies(db_path, transport="meshcore", review_state="accepted")

    assert len(rows) == 1
    assert rows[0].channel_name == "Public"
    assert rows[0].mapped_groups == ("MAGNET", "COMMUNITY")
    assert rows[0].retention_window == "24h"
    assert rows[0].is_accepted is True


def test_mesh_policy_can_be_staged_from_discovered_device_channel() -> None:
    channel = MeshChannel(
        adapter_id="meshcore-field",
        transport="meshcore",
        index=0,
        name="Public",
        role="public",
        privacy="public",
    )

    policy = policy_from_channel(channel)

    assert policy.channel_id == "0"
    assert policy.channel_name == "Public"
    assert policy.review_state == "pending"
    assert policy.retention_window == "24h"


def test_private_mesh_policy_requires_join_key_before_surface_use() -> None:
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="private-1",
        from_node="NODE1",
        to_node="NODE2",
        text="private channel report",
        channel="neighborhood",
    )
    manual_private = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="neighborhood",
        channel_name="Neighborhood",
        channel_role="private",
        channel_privacy="encrypted",
        review_state="accepted",
        source="manual",
    )
    joined_private = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="neighborhood",
        channel_name="Neighborhood",
        channel_role="private",
        channel_privacy="encrypted",
        review_state="accepted",
        source="device",
    )

    assert manual_private.requires_key is True
    assert manual_private.key_available is False
    assert manual_private.key_display_text == "Key needed"
    assert message_allowed_for_surface(message, [manual_private], "inbox") is False
    assert joined_private.key_available is True
    assert joined_private.key_display_text == "Joined"
    assert message_allowed_for_surface(message, [joined_private], "inbox") is True


def test_mesh_channel_policy_persists_private_key_state(tmp_path) -> None:
    db_path = tmp_path / "mesh-private-channel-policy.db"
    policy = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="neighborhood",
        channel_name="Neighborhood",
        channel_role="private",
        channel_privacy="encrypted",
        review_state="pending",
        source="manual",
    )
    upsert_mesh_channel_policy(db_path, policy, updated_utc="2026-08-31T12:00:00+00:00")

    [stored] = list_mesh_channel_policies(db_path, adapter_id="meshcore-field", transport="meshcore")

    assert stored.requires_key is True
    assert stored.key_state == "needed"
    assert stored.key_available is False


def test_mesh_stage_channels_preserves_reviewed_user_choices(tmp_path) -> None:
    db_path = tmp_path / "mesh-stage.db"
    accepted = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, accepted)

    staged = stage_mesh_channel_policies_from_channels(
        db_path,
        (
            MeshChannel(adapter_id="meshcore-field", transport="meshcore", index=0, name="Public", role="public"),
            MeshChannel(
                adapter_id="meshcore-field",
                transport="meshcore",
                index=3,
                name="Neighborhood",
                role="private",
                channel_id="neighborhood",
            ),
        ),
    )

    policies = {
        policy.channel_id: policy
        for policy in list_mesh_channel_policies(db_path, adapter_id="meshcore-field", transport="meshcore")
    }
    assert staged == 1
    assert policies["0"].review_state == "accepted"
    assert policies["neighborhood"].review_state == "pending"


def test_mesh_message_projects_to_observation_pipeline(tmp_path) -> None:
    db_path = tmp_path / "mesh-observations.db"
    message = MeshMessage(
        adapter_id="local",
        transport="meshtastic",
        message_id="geo",
        from_node="N0CALL",
        to_node="MAGNET",
        channel="MAGNET",
        text="Fire activity reported near ridge",
        rx_time=datetime.fromtimestamp(1_788_122_400, tz=timezone.utc),
        lat=39.7392,
        lon=-104.9903,
        grid="DM79QJ",
        topics=("Fire", "General Intel"),
        severity="urgent",
        route_type="direct",
        direct_receive=True,
    )

    observation = project_mesh_message_to_observation(db_path, message)
    rows = list_observations(db_path, source_family="meshtastic")

    assert observation.source_family == "meshtastic"
    assert observation.summary == "Fire activity reported near ridge"
    assert observation.location_confidence == "declared"
    assert len(rows) == 1
    assert rows[0].from_call == "N0CALL"
    assert rows[0].groups == ("MAGNET",)
    assert rows[0].observed_topics == ("Fire", "General Intel")
    assert rows[0].lat == 39.7392
    assert rows[0].provenance["routing"]["route_type"] == "direct"
    assert rows[0].provenance["routing"]["direct_receive"] is True


def test_mesh_event_store_requires_accepted_channel_before_projection(tmp_path) -> None:
    db_path = tmp_path / "mesh-policy-gate.db"
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="public-1",
        from_node="K7MESH",
        channel="0",
        text="public bridge check",
        topics=("Comms",),
    )

    store_mesh_event(
        db_path,
        MeshAdapterEvent(event_type="message", adapter_id="meshcore-field", transport="meshcore", message=message),
    )

    assert list_mesh_messages(db_path, transport="meshcore")[0]["text"] == "public bridge check"
    assert list_observations(db_path, source_family="meshcore") == []

    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="meshcore-field",
            transport="meshcore",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )
    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].summary == "public bridge check"
    assert rows[0].provenance["surfaces"] == ["inbox", "ops_center", "map", "topic_scan"]


def test_mesh_channel_message_with_direct_route_stays_on_channel_policy(tmp_path) -> None:
    db_path = tmp_path / "mesh-direct-route-channel.db"
    public = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    direct = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="direct",
        channel_name="Direct",
        channel_role="direct",
        channel_privacy="direct",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, direct)
    upsert_mesh_channel_policy(db_path, public)
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="public-direct-route",
        from_node="N1MAG MOBL2",
        to_node="channel",
        channel="0",
        text="public direct-route test",
        hop_count=0,
        route_type="direct",
        direct_receive=True,
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].to_target == "Public"
    assert rows[0].groups == ("Public",)
    assert rows[0].provenance["channel_policy"]["channel_name"] == "Public"
    assert rows[0].provenance["routing"]["direct_receive"] is True


def test_mesh_private_channel_message_is_not_hijacked_by_direct_policy(tmp_path) -> None:
    db_path = tmp_path / "mesh-private-route-channel.db"
    private = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="2",
        channel_name="COMAGNET",
        channel_role="private",
        channel_privacy="encrypted",
        review_state="accepted",
        key_state="device_configured",
    )
    direct = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="direct",
        channel_name="Direct",
        channel_role="direct",
        channel_privacy="direct",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, direct)
    upsert_mesh_channel_policy(db_path, private)
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="private-direct-route",
        from_node="N1MAG MOBL2",
        to_node="channel",
        channel="2",
        text="CoMagnet direct-route test",
        hop_count=0,
        route_type="direct",
        direct_receive=True,
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].to_target == "COMAGNET"
    assert rows[0].groups == ("COMAGNET",)
    assert rows[0].provenance["channel_policy"]["channel_name"] == "COMAGNET"


def test_mesh_message_without_location_uses_first_located_route_node(tmp_path) -> None:
    db_path = tmp_path / "mesh-route-derived-location.db"
    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="meshcore-field",
            transport="meshcore",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )
    upsert_mesh_node(
        db_path,
        MeshNode(
            adapter_id="meshcore-field",
            transport="meshcore",
            node_id="relay-1",
            short_name="RLY1",
            lat=39.7392,
            lon=-104.9903,
            grid="DM79QJ",
        ),
    )
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="route-derived",
        from_node="MOBILE1",
        to_node="channel",
        channel="0",
        text="road closure near bridge",
        via_node="relay-1",
        path_hops=("relay-1", "relay-2"),
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].lat == 39.7392
    assert rows[0].lon == -104.9903
    assert rows[0].grid == "DM79QJ"
    assert rows[0].location_confidence == "route_derived"
    assert rows[0].provenance["location_source"]["type"] == "route_derived"
    assert rows[0].provenance["location_source"]["label"] == "RLY1"


def test_mesh_message_without_location_uses_known_sender_node_before_route(tmp_path) -> None:
    db_path = tmp_path / "mesh-sender-location.db"
    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="meshcore-field",
            transport="meshcore",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )
    upsert_mesh_node(
        db_path,
        MeshNode(
            adapter_id="meshcore-field",
            transport="meshcore",
            node_id="MOBILE1",
            callsign="K7MOB",
            lat=40.015,
            lon=-105.2705,
            grid="DN70",
        ),
    )
    upsert_mesh_node(
        db_path,
        MeshNode(
            adapter_id="meshcore-field",
            transport="meshcore",
            node_id="relay-1",
            short_name="RLY1",
            lat=39.7392,
            lon=-104.9903,
            grid="DM79QJ",
        ),
    )
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="sender-location",
        from_node="MOBILE1",
        to_node="channel",
        channel="0",
        text="checking road status",
        via_node="relay-1",
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].lat == 40.015
    assert rows[0].lon == -105.2705
    assert rows[0].grid == "DN70"
    assert rows[0].location_confidence == "sender_lookup"
    assert rows[0].provenance["location_source"]["type"] == "sender_node"
    assert rows[0].provenance["location_source"]["label"] == "K7MOB"


def test_mesh_message_declared_location_wins_over_route_derived_location(tmp_path) -> None:
    db_path = tmp_path / "mesh-declared-location.db"
    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="meshcore-field",
            transport="meshcore",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )
    upsert_mesh_node(
        db_path,
        MeshNode(
            adapter_id="meshcore-field",
            transport="meshcore",
            node_id="relay-1",
            short_name="RLY1",
            lat=39.7392,
            lon=-104.9903,
        ),
    )
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="declared-location",
        from_node="MOBILE1",
        to_node="channel",
        channel="0",
        text="operator has GPS",
        via_node="relay-1",
        lat=38.8339,
        lon=-104.8214,
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].lat == 38.8339
    assert rows[0].lon == -104.8214
    assert rows[0].location_confidence == "declared"
    assert "location_source" not in rows[0].provenance


def test_mesh_message_topics_are_inferred_from_text(tmp_path) -> None:
    db_path = tmp_path / "mesh-topic-inference.db"
    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="meshcore-field",
            transport="meshcore",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="topic-from-text",
        from_node="K7MESH",
        to_node="channel",
        channel="0",
        text="Fire and road closure near the ridge",
    )

    store_mesh_message_with_channel_policy(db_path, message)

    raw = list_mesh_messages(db_path, transport="meshcore")
    rows = list_observations(db_path, source_family="meshcore")
    assert "Fire" in raw[0]["topics"]
    assert "Fire" in rows[0].observed_topics
    assert rows[0].operator_attention is True


def test_mesh_message_without_topic_hit_is_categorized_social_not_attention(tmp_path) -> None:
    db_path = tmp_path / "mesh-social-fallback.db"
    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="meshcore-field",
            transport="meshcore",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="social",
        from_node="K7MESH",
        to_node="channel",
        channel="0",
        text="good signal from the park today",
    )

    store_mesh_message_with_channel_policy(db_path, message)

    raw = list_mesh_messages(db_path, transport="meshcore")
    rows = list_observations(db_path, source_family="meshcore")
    assert raw[0]["topics"] == ("Social",)
    assert rows[0].observed_topics == ("Social",)
    assert rows[0].operator_attention is False


def test_mesh_policy_can_disable_topic_projection_without_hiding_message(tmp_path) -> None:
    db_path = tmp_path / "mesh-topic-policy.db"
    policy = default_policy_for_channel(
        adapter_id="local",
        transport="meshtastic",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, policy.__class__(**{**policy.__dict__, "topic_scan_enabled": False}))
    message = MeshMessage(
        adapter_id="local",
        transport="meshtastic",
        message_id="topic-off",
        channel="0",
        text="fire drill on public",
        topics=("Fire", "Comms"),
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshtastic")
    assert len(rows) == 1
    assert rows[0].observed_topics == ()
    assert "topic_scan" not in rows[0].provenance["surfaces"]


def test_mesh_policy_can_disable_ops_projection_without_hiding_inbox_message(tmp_path) -> None:
    db_path = tmp_path / "mesh-ops-policy.db"
    policy = default_policy_for_channel(
        adapter_id="local",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, replace(policy, ops_enabled=False))
    message = MeshMessage(
        adapter_id="local",
        transport="meshcore",
        message_id="ops-off",
        channel="0",
        text="water issue at store",
        topics=("Water",),
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert "inbox" in rows[0].provenance["surfaces"]
    assert "ops_center" not in rows[0].provenance["surfaces"]
    assert rows[0].operator_attention is False
    assert is_awareness_traffic_observation(rows[0]) is False


def test_mesh_public_flood_advertisement_is_not_weather_attention(tmp_path) -> None:
    db_path = tmp_path / "mesh-flood-advertisement.db"
    policy = default_policy_for_channel(
        adapter_id="local",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, policy)
    message = MeshMessage(
        adapter_id="local",
        transport="meshcore",
        message_id="flood-advert",
        from_node="ROUTER1",
        channel="0",
        text="meshcore router flood advertisement",
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert "Weather" not in rows[0].observed_topics
    assert rows[0].observed_topics == ("Social",)
    assert rows[0].operator_attention is False


def test_mesh_policy_default_category_social_overrides_topic_attention(tmp_path) -> None:
    db_path = tmp_path / "mesh-social-category.db"
    policy = default_policy_for_channel(
        adapter_id="local",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, replace(policy, default_category="social"))
    message = MeshMessage(
        adapter_id="local",
        transport="meshcore",
        message_id="social-topic-override",
        channel="0",
        text="fire word used casually in public chat",
        topics=("Fire",),
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].observed_topics == ("Social",)
    assert rows[0].operator_attention is False


def test_mesh_message_topic_override_survives_reprojection(tmp_path) -> None:
    db_path = tmp_path / "mesh-topic-override.db"
    policy = default_policy_for_channel(
        adapter_id="local",
        transport="meshcore",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, policy)
    message = MeshMessage(
        adapter_id="local",
        transport="meshcore",
        message_id="operator-corrected-topic",
        from_node="ALEX HQ",
        channel="0",
        text="flood gives you multiple opportunities to hear them",
        topics=("Weather",),
    )

    source_ref = store_mesh_message_with_channel_policy(db_path, message)
    set_mesh_message_topic_override(db_path, source_ref, ("Social",), operator_attention=False)
    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].observed_topics == ("Social",)
    assert rows[0].operator_attention is False
    assert rows[0].provenance["topic_override"]["topics"] == ["Social"]


def test_mesh_node_projection_is_not_an_inbox_message(tmp_path) -> None:
    db_path = tmp_path / "mesh-node-inbox-exclusion.db"
    node = MeshNode(
        adapter_id="local",
        transport="meshcore",
        node_id="ROUTER1",
        long_name="Repeater Ridge",
    )

    observation = project_mesh_node_to_observation(db_path, node)

    assert mesh_row_is_inbox_message(observation) is False


def test_mesh_private_channel_projects_after_device_join(tmp_path) -> None:
    db_path = tmp_path / "mesh-private-joined.db"
    policy = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="2",
        channel_name="County Ops",
        channel_role="private",
        channel_privacy="encrypted",
        review_state="accepted",
        key_state="device_configured",
    )
    policy = policy.__class__(**{**policy.__dict__, "mapped_groups": ("COUNTY", "ARES")})
    upsert_mesh_channel_policy(db_path, policy)
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="private-joined-1",
        from_node="K7MESH",
        channel="2",
        text="county ops water outage",
        topics=("Water", "Comms"),
        hop_count=2,
        route_type="mesh",
    )

    store_mesh_message_with_channel_policy(db_path, message)

    rows = list_observations(db_path, source_family="meshcore")
    assert len(rows) == 1
    assert rows[0].groups == ("COUNTY", "ARES")
    assert rows[0].to_target == "County Ops"
    assert rows[0].provenance["channel_policy"]["channel_name"] == "County Ops"
    assert rows[0].provenance["channel_policy"]["key_status"] == "Joined"
    assert rows[0].provenance["routing"]["hop_count"] == 2


def test_mesh_private_channel_key_needed_stays_raw_only(tmp_path) -> None:
    db_path = tmp_path / "mesh-private-key-needed.db"
    policy = default_policy_for_channel(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="2",
        channel_name="County Ops",
        channel_role="private",
        channel_privacy="encrypted",
        source="manual",
        review_state="accepted",
        key_state="needed",
    )
    upsert_mesh_channel_policy(db_path, policy)
    message = MeshMessage(
        adapter_id="meshcore-field",
        transport="meshcore",
        message_id="private-key-needed-1",
        from_node="K7MESH",
        channel="2",
        text="private channel should not surface yet",
    )

    store_mesh_message_with_channel_policy(db_path, message)

    assert list_mesh_messages(db_path, transport="meshcore")[0]["text"] == "private channel should not surface yet"
    assert list_observations(db_path, source_family="meshcore") == []


def test_mesh_retention_prunes_raw_messages_and_observations(tmp_path) -> None:
    db_path = tmp_path / "mesh-retention.db"
    policy = default_policy_for_channel(
        adapter_id="local",
        transport="meshtastic",
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(db_path, policy)
    old_message = MeshMessage(
        adapter_id="local",
        transport="meshtastic",
        message_id="old",
        channel="0",
        text="old public traffic",
        rx_time=datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc),
    )
    new_message = MeshMessage(
        adapter_id="local",
        transport="meshtastic",
        message_id="new",
        channel="0",
        text="new public traffic",
        rx_time=datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc),
    )
    store_mesh_message_with_channel_policy(db_path, old_message)
    store_mesh_message_with_channel_policy(db_path, new_message)

    removed = prune_mesh_messages_by_channel_policy(db_path, now_utc=datetime(2026, 8, 31, 13, 0, tzinfo=timezone.utc))

    assert removed == 1
    assert [row["message_id"] for row in list_mesh_messages(db_path, transport="meshtastic")] == ["new"]
    assert [obs.summary for obs in list_observations(db_path, source_family="meshtastic")] == ["new public traffic"]


def test_mesh_store_persists_health_and_event_stream(tmp_path) -> None:
    db_path = tmp_path / "mesh-health.db"
    snapshot = MeshHealthSnapshot(
        adapter_id="local",
        transport="meshtastic",
        enabled=True,
        connected=False,
        connection_type="tcp",
        device_name="192.0.2.2",
        last_error="connection refused",
        warnings=("send disabled",),
    )
    message = MeshMessage(
        adapter_id="local",
        transport="meshtastic",
        message_id="event-msg",
        text="mesh status check",
    )

    upsert_mesh_health(db_path, snapshot)
    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="local",
            transport="meshtastic",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )
    store_mesh_event(
        db_path,
        MeshAdapterEvent(
            event_type="message",
            adapter_id="local",
            transport="meshtastic",
            message=message,
        ),
    )

    health_rows = list_mesh_health(db_path)
    observations = list_observations(db_path, source_family="meshtastic")

    assert len(health_rows) == 1
    assert health_rows[0]["connected"] is False
    assert health_rows[0]["lifecycle_state"] == "reconnecting"
    assert health_rows[0]["source_connection"]["lifecycle_state"] == "reconnecting"
    assert health_rows[0]["last_error"] == "connection refused"
    assert health_rows[0]["warnings"] == ("send disabled",)
    assert observations[0].summary == "mesh status check"


def test_mesh_health_projects_source_connection_lifecycle(tmp_path) -> None:
    db_path = tmp_path / "mesh-lifecycle.db"

    upsert_mesh_health(
        db_path,
        MeshHealthSnapshot(
            adapter_id="meshcore-mobl1",
            transport="meshcore",
            enabled=True,
            connected=True,
            connection_type="ble",
            device_name="MeshCore-N1MAG MOBL1",
        ),
        updated_utc="2026-09-01T12:00:00+00:00",
    )
    connected = list_mesh_health(db_path)[0]
    assert connected["lifecycle_state"] == "connected"
    assert connected["guidance"] == "Connected."

    upsert_mesh_health(
        db_path,
        MeshHealthSnapshot(
            adapter_id="meshcore-mobl1",
            transport="meshcore",
            enabled=True,
            connected=False,
            connection_type="ble",
            device_name="MeshCore-N1MAG MOBL1",
        ),
        updated_utc="2026-09-01T12:00:00+00:00",
    )
    away = list_mesh_health(db_path)[0]
    assert away["lifecycle_state"] in {"reconnecting", "away"}
    source = list_mesh_source_connection_snapshots(db_path)[0]
    assert source["source_family"] == "meshcore"
    assert source["display_name"] == "MeshCore-N1MAG MOBL1"


def test_mesh_manager_can_publish_directly_to_store_sink(tmp_path) -> None:
    db_path = tmp_path / "mesh-sink.db"
    manager = MeshConnectionManager(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=FakeMeshAdapter,
    )
    manager.add_listener(MeshEventStoreSink(db_path))
    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="local",
            transport="meshtastic",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )

    manager.start_adapter("local")
    manager.ingest_packet("local", {"id": "sink-msg", "text": "operator check-in"})

    assert list_mesh_health(db_path)[0]["adapter_id"] == "local"
    assert list_mesh_messages(db_path)[0]["text"] == "operator check-in"
    assert list_observations(db_path, source_family="meshtastic")[0].summary == "operator check-in"


def test_mesh_manager_manual_ingest_does_not_replay_on_next_poll() -> None:
    manager = MeshConnectionManager(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=FakeMeshAdapter,
    )
    events: list[MeshAdapterEvent] = []
    manager.add_listener(events.append)
    manager.start_adapter("local")

    manager.ingest_packet("local", {"id": "one", "text": "single receive"})
    manager.poll_events("local")

    message_events = [event for event in events if event.event_type == "message"]
    assert len(message_events) == 1
    assert message_events[0].message and message_events[0].message.text == "single receive"


def test_mesh_node_store_projects_location_observation(tmp_path) -> None:
    db_path = tmp_path / "mesh-node.db"
    node = MeshNode(
        adapter_id="local",
        transport="meshtastic",
        node_id="!abc123",
        short_name="K7M",
        callsign="K7MESH",
        last_heard=datetime(2026, 8, 30, 18, 0, tzinfo=timezone.utc),
        lat=39.7392,
        lon=-104.9903,
        grid="DM79",
        hop_count=1,
        direct_receive=True,
    )

    observation = project_mesh_node_to_observation(db_path, node)

    assert mesh_node_source_ref(node) == "mesh-node:meshtastic:local:!abc123"
    assert list_mesh_nodes(db_path)[0]["callsign"] == "K7MESH"
    assert observation.lat == 39.7392
    assert observation.lon == -104.9903
    assert observation.observed_topics == ("Comms",)
    assert observation.provenance["routing"]["direct_receive"] is True
    assert list_observations(db_path, source_family="meshtastic")[0].summary == "K7MESH | DM79 | 1 hop"


def test_mesh_node_upsert_preserves_location_when_refresh_is_sparse(tmp_path) -> None:
    db_path = tmp_path / "mesh-node-preserve-location.db"
    upsert_mesh_node(
        db_path,
        MeshNode(
            adapter_id="meshcore-field",
            transport="meshcore",
            node_id="aabbccddeeff",
            long_name="K7MESH R1",
            callsign="K7MESH",
            lat=39.7392,
            lon=-104.9903,
            grid="DM79QJ",
            via_node="relay-1",
            path_hops=("relay-1",),
        ),
    )

    upsert_mesh_node(
        db_path,
        MeshNode(
            adapter_id="meshcore-field",
            transport="meshcore",
            node_id="aabbccddeeff",
            long_name="",
            callsign="",
            lat=None,
            lon=None,
            grid="",
        ),
    )

    row = list_mesh_nodes(db_path)[0]
    assert row["long_name"] == "K7MESH R1"
    assert row["callsign"] == "K7MESH"
    assert row["lat"] == 39.7392
    assert row["lon"] == -104.9903
    assert row["grid"] == "DM79QJ"
    assert row["via_node"] == "relay-1"


def test_mesh_manager_publishes_node_events() -> None:
    manager = MeshConnectionManager(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=FakeMeshAdapter,
    )
    events: list[MeshAdapterEvent] = []
    manager.add_listener(events.append)
    manager.start_adapter("local")
    adapter = manager._adapters["local"]
    adapter.nodes.append(MeshNode(adapter_id="local", transport="meshtastic", node_id="!node", short_name="N1"))

    nodes = manager.poll_nodes("local")

    assert nodes[0].node_id == "!node"
    assert any(event.event_type == "node" and event.node and event.node.node_id == "!node" for event in events)


def test_mesh_worker_starts_polls_and_stops_without_hardware(tmp_path) -> None:
    app = QApplication.instance() or QApplication([])
    db_path = tmp_path / "mesh-worker.db"
    health_events: list[MeshHealthSnapshot] = []
    ready_events: list[MeshAdapterEvent] = []
    upsert_mesh_channel_policy(
        db_path,
        default_policy_for_channel(
            adapter_id="local",
            transport="meshtastic",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            review_state="accepted",
        ),
    )
    worker = MeshConnectionWorker(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        db_path=db_path,
        poll_interval_ms=250,
        channel_poll_interval_ms=250,
        adapter_factory=FakeMeshAdapter,
    )
    worker.health_ready.connect(health_events.append)
    worker.event_ready.connect(ready_events.append)

    worker.start()
    worker.manager().ingest_packet("local", {"id": "worker-msg", "text": "worker passive receive"})
    worker.manager()._adapters["local"].nodes.append(
        MeshNode(adapter_id="local", transport="meshtastic", node_id="!worker", short_name="WK")
    )
    worker.poll_once()
    worker.stop()
    app.processEvents()

    assert any(snapshot.adapter_id == "local" for snapshot in health_events)
    assert any(event.message and event.message.text == "worker passive receive" for event in ready_events)
    assert any(event.node and event.node.node_id == "!worker" for event in ready_events)
    assert list_mesh_messages(db_path)[0]["text"] == "worker passive receive"
    assert list_mesh_nodes(db_path)[0]["node_id"] == "!worker"
    summaries = {obs.summary for obs in list_observations(db_path, source_family="meshtastic")}
    assert "worker passive receive" in summaries
    assert "WK" in summaries


def test_mesh_worker_stages_discovered_channels_for_review(tmp_path) -> None:
    app = QApplication.instance() or QApplication([])
    db_path = tmp_path / "mesh-worker-channels.db"
    worker = MeshConnectionWorker(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        db_path=db_path,
        poll_interval_ms=250,
        channel_poll_interval_ms=250,
        adapter_factory=FakeMeshAdapter,
    )

    worker.start()
    worker.manager()._adapters["local"].channels.append(
        MeshChannel(adapter_id="local", transport="meshtastic", index=0, name="Public", role="public")
    )
    worker.poll_once()
    worker.stop()
    app.processEvents()

    policies = list_mesh_channel_policies(db_path, adapter_id="local", transport="meshtastic")
    assert len(policies) == 1
    assert policies[0].channel_name == "Public"
    assert policies[0].review_state == "pending"


def test_main_window_wires_mesh_runtime_lifecycle() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    stop_block = source[
        source.index("    def _stop_mesh_runtime(self) -> None:") :
        source.index("    def _on_mesh_runtime_thread_finished(self) -> None:")
    ]

    assert "MeshConnectionWorker" in source
    assert "load_mesh_connection_configs(self.settings)" in source
    assert "self._start_mesh_runtime_if_enabled()" in source
    assert "settings_saved -> local mesh runtime" in source
    assert "self._stop_mesh_runtime()" in source
    assert "Qt.BlockingQueuedConnection" not in source
    assert 'QMetaObject.invokeMethod(worker, "stop", Qt.QueuedConnection)' in source
    assert "stop_requested = True" in stop_block
    assert "if not stop_requested:" in stop_block
    assert "thread.wait(200)" in stop_block


def test_mesh_worker_stop_cleans_timer_in_worker_lifecycle() -> None:
    source = Path("freqinout/core/mesh/qt_worker.py").read_text(encoding="utf-8")
    stop_block = source[source.index("    def stop(self) -> None:") : source.index("    @Slot()\n    def poll_once")]

    assert "self._timer.stop()" in stop_block
    assert "self._timer.deleteLater()" in stop_block
    assert "self._timer = None" in stop_block
    assert "if not self._stopped_emitted:" in stop_block


def test_meshcore_ble_runner_times_out_blocked_operation() -> None:
    runner = _AsyncioLoopRunner()

    async def slow_operation() -> str:
        await asyncio.sleep(1)
        return "done"

    try:
        with pytest.raises(MeshConnectionError, match="timed out"):
            runner.run(slow_operation(), timeout_sec=0.01)
    finally:
        runner.stop()
