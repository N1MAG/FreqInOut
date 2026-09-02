from __future__ import annotations

import sys
from datetime import datetime, timezone
from importlib import import_module, util
from typing import Iterator, Mapping

from freqinout.core.mesh.models import MeshAdapterEvent, MeshChannel, MeshHealthSnapshot, MeshMessage, MeshNode
from freqinout.core.mesh.settings import MeshConnectionConfig, MeshConnectionType, validate_mesh_connection_config


class MeshConnectionError(RuntimeError):
    pass


def meshtastic_python_available() -> bool:
    if "meshtastic" in sys.modules:
        return True
    try:
        return util.find_spec("meshtastic") is not None
    except ValueError:
        return False


class MeshtasticLocalAdapter:
    transport_name = "meshtastic"

    def __init__(self, config: MeshConnectionConfig) -> None:
        self.config = config
        self.adapter_id = config.adapter_id
        self._interface: object | None = None
        self._last_error = ""
        self._messages: list[MeshMessage] = []
        self._events: list[MeshAdapterEvent] = []
        self._pub_module: object | None = None
        self._subscribed_topics: tuple[str, ...] = ()

    def connect(self) -> None:
        if not self.config.enabled:
            raise MeshConnectionError("Meshtastic adapter is disabled.")
        issues = tuple(issue for issue in validate_mesh_connection_config(self.config) if issue.severity == "error")
        if issues:
            raise MeshConnectionError("; ".join(issue.message for issue in issues))
        if not meshtastic_python_available():
            raise MeshConnectionError("Meshtastic Python package is not installed; configure the dependency before connecting.")

        module_path, class_name, kwargs, positional = self._interface_spec()
        module = import_module(module_path)
        factory = getattr(module, class_name)
        try:
            self._interface = factory(**kwargs)
        except TypeError:
            self._interface = factory(*positional)
        self._subscribe_receive_events()
        self._last_error = ""

    def disconnect(self) -> None:
        interface = self._interface
        self._unsubscribe_receive_events()
        self._interface = None
        if interface is None:
            return
        close = getattr(interface, "close", None)
        if callable(close):
            close()

    def health(self) -> MeshHealthSnapshot:
        return MeshHealthSnapshot(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            enabled=self.config.enabled,
            connected=self._interface is not None,
            connection_type=self.config.connection_type.value,
            device_name=self.config.endpoint_address,
            last_error=self._last_error,
            warnings=tuple(issue.message for issue in validate_mesh_connection_config(self.config) if issue.severity != "error"),
        )

    def list_nodes(self) -> list[MeshNode]:
        interface = self._interface
        nodes = getattr(interface, "nodes", {}) if interface is not None else {}
        if not isinstance(nodes, Mapping):
            return []
        return [self._node_from_mapping(node_id, details) for node_id, details in nodes.items()]

    def list_channels(self) -> list[MeshChannel]:
        local_node = getattr(self._interface, "localNode", None)
        channels = getattr(local_node, "channels", None)
        if not channels:
            return []
        result: list[MeshChannel] = []
        for index, channel in enumerate(channels):
            name = str(getattr(getattr(channel, "settings", None), "name", "") or f"Channel {index}")
            role = str(getattr(channel, "role", "") or "")
            privacy = "public" if index == 0 else ("encrypted" if role else "unknown")
            result.append(
                MeshChannel(
                    self.adapter_id,
                    self.transport_name,
                    index,
                    name,
                    role=role,
                    channel_id=str(index),
                    privacy=privacy,
                )
            )
        return result

    def ingest_packet(self, packet: Mapping[str, object]) -> MeshMessage | None:
        message = self.normalize_text_packet(packet)
        if message is not None:
            self._messages.append(message)
            self._events.append(
                MeshAdapterEvent(
                    event_type="message",
                    adapter_id=self.adapter_id,
                    transport=self.transport_name,
                    message=message,
                    raw=message.raw,
                )
            )
        return message

    def get_recent_messages(self) -> list[MeshMessage]:
        return list(self._messages)

    def receive_events(self) -> Iterator[MeshAdapterEvent]:
        events = tuple(self._events)
        self._events.clear()
        yield from events

    def _on_receive(self, packet: Mapping[str, object] | object, interface: object | None = None) -> None:
        if isinstance(packet, Mapping):
            self.ingest_packet(packet)

    def _subscribe_receive_events(self) -> None:
        try:
            pub = import_module("pubsub").pub
        except Exception:
            return
        subscribed: list[str] = []
        for topic in ("meshtastic.receive.text", "meshtastic.receive"):
            try:
                pub.subscribe(self._on_receive, topic)
                subscribed.append(topic)
            except Exception:
                continue
        self._pub_module = pub
        self._subscribed_topics = tuple(subscribed)

    def _unsubscribe_receive_events(self) -> None:
        pub = self._pub_module
        if pub is None:
            self._subscribed_topics = ()
            return
        for topic in self._subscribed_topics:
            try:
                pub.unsubscribe(self._on_receive, topic)
            except Exception:
                continue
        self._pub_module = None
        self._subscribed_topics = ()

    def normalize_text_packet(self, packet: Mapping[str, object]) -> MeshMessage | None:
        decoded = packet.get("decoded")
        decoded_map = decoded if isinstance(decoded, Mapping) else {}
        text = str(decoded_map.get("text") or packet.get("text") or "").strip()
        if not text:
            return None
        rx_time = _datetime_from_packet_time(packet.get("rxTime") or packet.get("rx_time"))
        packet_id = str(packet.get("id") or packet.get("packetId") or f"{packet.get('fromId', '')}:{rx_time.timestamp() if rx_time else ''}")
        return MeshMessage(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            message_id=packet_id,
            text=text,
            from_node=str(packet.get("fromId") or packet.get("from") or ""),
            to_node=str(packet.get("toId") or packet.get("to") or ""),
            channel=str(packet.get("channel") or ""),
            portnum=str(decoded_map.get("portnum") or decoded_map.get("portNum") or ""),
            rx_time=rx_time,
            hop_count=_optional_int(packet.get("hopLimit") or packet.get("hop_count")),
            route_type=str(packet.get("route_type") or packet.get("rxRoute") or ""),
            direct_receive=_optional_bool(packet.get("direct_receive") or packet.get("directReceive")),
            via_node=str(packet.get("via_node") or packet.get("viaNode") or ""),
            path_hops=_string_tuple(packet.get("path_hops") or packet.get("pathHops")),
            snr=_optional_float(packet.get("rxSnr") or packet.get("snr")),
            rssi=_optional_float(packet.get("rxRssi") or packet.get("rssi")),
            lat=_optional_float(packet.get("lat")),
            lon=_optional_float(packet.get("lon")),
            grid=str(packet.get("grid") or ""),
            raw=packet,
        )

    def _interface_spec(self) -> tuple[str, str, dict[str, object], tuple[object, ...]]:
        connection_type = self.config.connection_type
        if connection_type is MeshConnectionType.SERIAL:
            return (
                "meshtastic.serial_interface",
                "SerialInterface",
                {"devPath": self.config.serial_port},
                (self.config.serial_port,),
            )
        if connection_type is MeshConnectionType.BLE:
            device = self.config.ble_device_id or self.config.ble_device_name
            return (
                "meshtastic.ble_interface",
                "BLEInterface",
                {"address": device},
                (device,),
            )
        if connection_type is MeshConnectionType.TCP:
            return (
                "meshtastic.tcp_interface",
                "TCPInterface",
                {"hostname": self.config.tcp_host},
                (self.config.tcp_host,),
            )
        raise MeshConnectionError(f"Meshtastic {connection_type.value} connections are configured for a later adapter slice.")

    def _node_from_mapping(self, node_id: object, details: object) -> MeshNode:
        data = details if isinstance(details, Mapping) else {}
        user = data.get("user") if isinstance(data.get("user"), Mapping) else {}
        position = data.get("position") if isinstance(data.get("position"), Mapping) else {}
        metrics = data.get("deviceMetrics") if isinstance(data.get("deviceMetrics"), Mapping) else {}
        return MeshNode(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            node_id=str(node_id),
            long_name=str(user.get("longName") or ""),
            short_name=str(user.get("shortName") or ""),
            public_key_or_hash=str(user.get("publicKey") or ""),
            role=str(user.get("role") or ""),
            last_heard=_datetime_from_packet_time(data.get("lastHeard")),
            hop_count=_optional_int(data.get("hopsAway")),
            route_type=str(data.get("route_type") or data.get("routeType") or ""),
            direct_receive=_optional_bool(data.get("direct_receive") or data.get("directReceive")),
            via_node=str(data.get("via_node") or data.get("viaNode") or ""),
            path_hops=_string_tuple(data.get("path_hops") or data.get("pathHops")),
            snr=_optional_float(data.get("snr")),
            battery_percent=_optional_float(metrics.get("batteryLevel")),
            lat=_optional_float(position.get("latitude")),
            lon=_optional_float(position.get("longitude")),
            raw=data,
        )


def _datetime_from_packet_time(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _optional_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_bool(value: object) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        normalized = str(value).strip().lower()
        if normalized in {"true", "yes", "on"}:
            return True
        if normalized in {"false", "no", "off"}:
            return False
    return None


def _string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    if isinstance(value, (tuple, list)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()
