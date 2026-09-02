from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from importlib import import_module
import json
from pathlib import Path
from typing import Mapping, Sequence

from freqinout.core.config_paths import get_config_dir


class MeshConnectionType(str, Enum):
    TCP = "tcp"
    SERIAL = "serial"
    BLE = "ble"
    HTTP = "http"
    MQTT = "mqtt"

    @classmethod
    def from_value(cls, value: object) -> "MeshConnectionType":
        normalized = str(value or "").strip().lower()
        for member in cls:
            if member.value == normalized:
                return member
        return cls.TCP


@dataclass(frozen=True)
class MeshConnectionConfig:
    adapter_id: str = "meshtastic-main"
    protocol: str = "meshtastic"
    enabled: bool = False
    connection_type: MeshConnectionType = MeshConnectionType.TCP
    tcp_host: str = ""
    tcp_port: int = 4403
    serial_port: str = ""
    serial_baud: int = 115200
    ble_device_id: str = ""
    ble_device_name: str = ""
    ble_scan_timeout_sec: int = 20
    http_base_url: str = ""
    mqtt_enabled: bool = False
    mqtt_broker: str = ""
    mqtt_topic_root: str = ""
    send_enabled: bool = False
    store_messages_enabled: bool = True
    map_positions_enabled: bool = True
    bridge_to_reticulum_enabled: bool = False

    @classmethod
    def from_mapping(cls, values: Mapping[str, object], prefix: str = "meshtastic") -> "MeshConnectionConfig":
        def get(name: str, default: object = "") -> object:
            return values.get(f"{prefix}_{name}", values.get(name, default))

        def as_bool(value: object, default: bool = False) -> bool:
            if value is None:
                return default
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}

        def as_int(value: object, default: int) -> int:
            try:
                return int(str(value).strip())
            except (TypeError, ValueError):
                return default

        return cls(
            adapter_id=str(get("adapter_id", cls.adapter_id) or cls.adapter_id),
            protocol=str(get("protocol", cls.protocol) or cls.protocol).strip().lower(),
            enabled=as_bool(get("enabled", cls.enabled), cls.enabled),
            connection_type=MeshConnectionType.from_value(get("connection_type", cls.connection_type.value)),
            tcp_host=str(get("tcp_host", cls.tcp_host) or "").strip(),
            tcp_port=as_int(get("tcp_port", cls.tcp_port), cls.tcp_port),
            serial_port=str(get("serial_port", cls.serial_port) or "").strip(),
            serial_baud=as_int(get("serial_baud", cls.serial_baud), cls.serial_baud),
            ble_device_id=str(get("ble_device_id", cls.ble_device_id) or "").strip(),
            ble_device_name=str(get("ble_device_name", cls.ble_device_name) or "").strip(),
            ble_scan_timeout_sec=as_int(get("ble_scan_timeout_sec", cls.ble_scan_timeout_sec), cls.ble_scan_timeout_sec),
            http_base_url=str(get("http_base_url", cls.http_base_url) or "").strip(),
            mqtt_enabled=as_bool(get("mqtt_enabled", cls.mqtt_enabled), cls.mqtt_enabled),
            mqtt_broker=str(get("mqtt_broker", cls.mqtt_broker) or "").strip(),
            mqtt_topic_root=str(get("mqtt_topic_root", cls.mqtt_topic_root) or "").strip(),
            send_enabled=as_bool(get("send_enabled", cls.send_enabled), cls.send_enabled),
            store_messages_enabled=as_bool(
                get("store_messages_enabled", cls.store_messages_enabled), cls.store_messages_enabled
            ),
            map_positions_enabled=as_bool(
                get("map_positions_enabled", cls.map_positions_enabled), cls.map_positions_enabled
            ),
            bridge_to_reticulum_enabled=as_bool(
                get("bridge_to_reticulum_enabled", cls.bridge_to_reticulum_enabled),
                cls.bridge_to_reticulum_enabled,
            ),
        )

    @property
    def endpoint_address(self) -> str:
        if self.connection_type is MeshConnectionType.SERIAL:
            return self.serial_port
        if self.connection_type is MeshConnectionType.BLE:
            return self.ble_device_id or self.ble_device_name
        if self.connection_type is MeshConnectionType.HTTP:
            return self.http_base_url
        if self.connection_type is MeshConnectionType.MQTT:
            return self.mqtt_broker
        return self.tcp_host

    def to_mapping(self) -> dict[str, object]:
        return {
            "adapter_id": self.adapter_id,
            "protocol": self.protocol,
            "enabled": self.enabled,
            "connection_type": self.connection_type.value,
            "tcp_host": self.tcp_host,
            "tcp_port": self.tcp_port,
            "serial_port": self.serial_port,
            "serial_baud": self.serial_baud,
            "ble_device_id": self.ble_device_id,
            "ble_device_name": self.ble_device_name,
            "ble_scan_timeout_sec": self.ble_scan_timeout_sec,
            "http_base_url": self.http_base_url,
            "mqtt_enabled": self.mqtt_enabled,
            "mqtt_broker": self.mqtt_broker,
            "mqtt_topic_root": self.mqtt_topic_root,
            "send_enabled": self.send_enabled,
            "store_messages_enabled": self.store_messages_enabled,
            "map_positions_enabled": self.map_positions_enabled,
            "bridge_to_reticulum_enabled": self.bridge_to_reticulum_enabled,
        }


@dataclass(frozen=True)
class MeshConfigIssue:
    field: str
    message: str
    severity: str = "error"


def validate_mesh_connection_config(config: MeshConnectionConfig) -> tuple[MeshConfigIssue, ...]:
    issues: list[MeshConfigIssue] = []
    if not config.enabled:
        return ()
    if config.connection_type is MeshConnectionType.TCP and not config.tcp_host:
        issues.append(MeshConfigIssue("tcp_host", "TCP mesh connections need a host name or IP address."))
    if config.connection_type is MeshConnectionType.SERIAL and not config.serial_port:
        issues.append(MeshConfigIssue("serial_port", "USB serial mesh connections need a serial device path."))
    if config.connection_type is MeshConnectionType.BLE and not (config.ble_device_id or config.ble_device_name):
        issues.append(MeshConfigIssue("ble_device", "BLE mesh connections need a saved device id or name."))
    if config.connection_type is MeshConnectionType.HTTP and not config.http_base_url:
        issues.append(MeshConfigIssue("http_base_url", "HTTP mesh connections need the device base URL."))
    if config.connection_type is MeshConnectionType.MQTT and not config.mqtt_broker:
        issues.append(MeshConfigIssue("mqtt_broker", "MQTT mesh connections need a broker."))
    if config.connection_type is MeshConnectionType.MQTT and not config.mqtt_enabled:
        issues.append(MeshConfigIssue("mqtt_enabled", "MQTT must be explicitly enabled before use.", severity="warning"))
    if config.tcp_port <= 0 or config.tcp_port > 65535:
        issues.append(MeshConfigIssue("tcp_port", "TCP port must be between 1 and 65535."))
    if config.serial_baud <= 0:
        issues.append(MeshConfigIssue("serial_baud", "Serial baud rate must be greater than zero."))
    if config.ble_scan_timeout_sec < 5:
        issues.append(MeshConfigIssue("ble_scan_timeout_sec", "BLE scan timeout should be at least 5 seconds."))
    return tuple(issues)


def discover_serial_ports() -> tuple[str, ...]:
    try:
        list_ports = import_module("serial.tools.list_ports")
    except Exception:
        return ()
    try:
        ports = list_ports.comports()
    except Exception:
        return ()
    names: list[str] = []
    for port in ports:
        device = str(getattr(port, "device", "") or "").strip()
        if device:
            names.append(device)
    return tuple(dict.fromkeys(names))


def default_mesh_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout_nets.db"


def load_mesh_connection_configs(settings_or_values: object, prefix: str = "meshtastic") -> tuple[MeshConnectionConfig, ...]:
    values = _settings_values(settings_or_values)
    configs: list[MeshConnectionConfig] = []
    configs.extend(_load_mesh_connection_library(values))
    config = MeshConnectionConfig.from_mapping(values, prefix=prefix)
    if config.enabled:
        configs.append(config)
    return tuple(_dedupe_mesh_connection_configs(configs))


def serialize_mesh_connection_library(configs: Sequence[MeshConnectionConfig]) -> str:
    return json.dumps([config.to_mapping() for config in configs], sort_keys=True)


def merge_mesh_connection_library(
    existing_values: Mapping[str, object],
    selected_config: MeshConnectionConfig,
) -> tuple[MeshConnectionConfig, ...]:
    configs = list(_load_mesh_connection_library(existing_values))
    if selected_config.enabled:
        configs.append(selected_config)
    return tuple(_dedupe_mesh_connection_configs(configs))


def _load_mesh_connection_library(values: Mapping[str, object]) -> tuple[MeshConnectionConfig, ...]:
    raw = values.get("mesh_connection_library", ())
    parsed: object = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            parsed = ()
        else:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = ()
    if not isinstance(parsed, Sequence) or isinstance(parsed, (str, bytes, bytearray)):
        return ()
    configs: list[MeshConnectionConfig] = []
    for item in parsed:
        if not isinstance(item, Mapping):
            continue
        config = MeshConnectionConfig.from_mapping(item, prefix="")
        if config.enabled:
            configs.append(config)
    return tuple(configs)


def _dedupe_mesh_connection_configs(configs: Sequence[MeshConnectionConfig]) -> tuple[MeshConnectionConfig, ...]:
    by_key: dict[tuple[str, str, str], MeshConnectionConfig] = {}
    for config in configs:
        key = (
            str(config.protocol or "").strip().lower(),
            str(config.connection_type.value or "").strip().lower(),
            str(config.endpoint_address or config.adapter_id or "").strip().lower(),
        )
        by_key[key] = config
    return tuple(by_key.values())


def _settings_values(settings_or_values: object) -> Mapping[str, object]:
    if isinstance(settings_or_values, Mapping):
        return settings_or_values
    all_values = getattr(settings_or_values, "all", None)
    if callable(all_values):
        loaded = all_values()
        if isinstance(loaded, Mapping):
            return loaded
    get_value = getattr(settings_or_values, "get", None)
    if callable(get_value):
        return {
            f"meshtastic_{name}": get_value(f"meshtastic_{name}", default)
            for name, default in (
                ("adapter_id", MeshConnectionConfig.adapter_id),
                ("protocol", MeshConnectionConfig.protocol),
                ("enabled", MeshConnectionConfig.enabled),
                ("connection_type", MeshConnectionConfig.connection_type.value),
                ("tcp_host", MeshConnectionConfig.tcp_host),
                ("tcp_port", MeshConnectionConfig.tcp_port),
                ("serial_port", MeshConnectionConfig.serial_port),
                ("serial_baud", MeshConnectionConfig.serial_baud),
                ("ble_device_id", MeshConnectionConfig.ble_device_id),
                ("ble_device_name", MeshConnectionConfig.ble_device_name),
                ("ble_scan_timeout_sec", MeshConnectionConfig.ble_scan_timeout_sec),
                ("http_base_url", MeshConnectionConfig.http_base_url),
                ("mqtt_enabled", MeshConnectionConfig.mqtt_enabled),
                ("mqtt_broker", MeshConnectionConfig.mqtt_broker),
                ("mqtt_topic_root", MeshConnectionConfig.mqtt_topic_root),
                ("send_enabled", MeshConnectionConfig.send_enabled),
                ("store_messages_enabled", MeshConnectionConfig.store_messages_enabled),
                ("map_positions_enabled", MeshConnectionConfig.map_positions_enabled),
                ("bridge_to_reticulum_enabled", MeshConnectionConfig.bridge_to_reticulum_enabled),
            )
        }
    return {}
