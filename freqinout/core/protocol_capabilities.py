from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Mapping


@dataclass(frozen=True)
class ProtocolCapabilities:
    receive_messages: bool = False
    receive_reports: bool = False
    receive_links: bool = False
    receive_nodes: bool = False
    send_message: bool = False
    send_form: bool = False
    frequency_control: bool = False
    launch_control: bool = False
    bbs_read: bool = False
    bbs_write: bool = False
    store_forward: bool = False
    topology: bool = False
    location: bool = False
    authenticated_identity: bool = False
    rf_only: bool = False
    internet_assisted: bool = False
    read_only: bool = False
    config_write_supported: bool = False

    def as_dict(self) -> dict[str, bool]:
        return {field.name: bool(getattr(self, field.name)) for field in fields(self)}

    def enabled_names(self) -> tuple[str, ...]:
        return tuple(name for name, enabled in self.as_dict().items() if enabled)


_CAPABILITIES_BY_FAMILY: dict[str, ProtocolCapabilities] = {
    "js8call": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        receive_links=True,
        receive_nodes=True,
        send_message=True,
        send_form=True,
        launch_control=True,
        topology=True,
        location=True,
        authenticated_identity=True,
        rf_only=True,
        config_write_supported=True,
    ),
    "flmsg": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        send_form=True,
        launch_control=True,
        location=True,
        authenticated_identity=True,
        rf_only=True,
        config_write_supported=True,
    ),
    "flamp": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        send_form=True,
        launch_control=True,
        authenticated_identity=True,
        rf_only=True,
        config_write_supported=True,
    ),
    "commstat": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        receive_links=True,
        receive_nodes=True,
        topology=True,
        location=True,
        authenticated_identity=True,
        rf_only=True,
        internet_assisted=True,
        read_only=True,
    ),
    "varac": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        receive_links=True,
        receive_nodes=True,
        launch_control=True,
        bbs_read=True,
        store_forward=True,
        topology=True,
        location=True,
        authenticated_identity=True,
        rf_only=True,
        read_only=True,
    ),
    "local": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        send_message=True,
        location=True,
        authenticated_identity=True,
    ),
    "meshcore": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        receive_links=True,
        receive_nodes=True,
        send_message=True,
        send_form=True,
        store_forward=True,
        topology=True,
        location=True,
        authenticated_identity=True,
        rf_only=True,
        read_only=True,
    ),
    "meshtastic": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        receive_links=True,
        receive_nodes=True,
        send_message=True,
        send_form=True,
        store_forward=True,
        topology=True,
        location=True,
        authenticated_identity=True,
        rf_only=True,
        read_only=True,
    ),
    "reticulum": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        receive_links=True,
        receive_nodes=True,
        send_message=True,
        send_form=True,
        bbs_read=True,
        bbs_write=True,
        store_forward=True,
        topology=True,
        location=True,
        authenticated_identity=True,
        read_only=True,
    ),
    "mqtt": ProtocolCapabilities(
        receive_messages=True,
        receive_reports=True,
        receive_links=True,
        receive_nodes=True,
        send_message=True,
        topology=True,
        location=True,
        authenticated_identity=True,
        internet_assisted=True,
        read_only=True,
    ),
}


_PROVENANCE_BY_FAMILY = {
    "js8call": "rf",
    "flmsg": "rf",
    "flamp": "rf",
    "commstat": "mixed",
    "varac": "rf",
    "local": "manual",
    "meshcore": "rf",
    "meshtastic": "rf",
    "reticulum": "mesh",
    "mqtt": "internet",
}


_SCOPE_BY_FAMILY = {
    "js8call": "station_or_group",
    "flmsg": "station_or_group",
    "flamp": "station_or_group",
    "commstat": "station_or_group",
    "varac": "station_or_group",
    "local": "local_group",
    "meshcore": "node_or_group",
    "meshtastic": "node_or_group",
    "reticulum": "node_or_peer",
    "mqtt": "topic_or_group",
}


def protocol_capabilities_for(family: object, source_type: object = "", metadata: Mapping[str, object] | None = None) -> ProtocolCapabilities:
    family_key = str(family or "").strip().lower()
    source_type_key = str(source_type or "").strip().lower()
    base = _CAPABILITIES_BY_FAMILY.get(family_key, ProtocolCapabilities(read_only=True))
    if family_key == "js8call" and source_type_key in {"file", "sqlite"}:
        return ProtocolCapabilities(
            **{
                **base.as_dict(),
                "send_message": False,
                "send_form": False,
                "config_write_supported": False,
            }
        )
    if family_key in {"flmsg", "flamp"} and source_type_key == "directory":
        return ProtocolCapabilities(
            **{
                **base.as_dict(),
                "send_form": False,
                "config_write_supported": False,
            }
        )
    if metadata and bool(metadata.get("read_only")):
        return ProtocolCapabilities(**{**base.as_dict(), "read_only": True})
    return base


def capabilities_dict_for(family: object, source_type: object = "", metadata: Mapping[str, object] | None = None) -> dict[str, bool]:
    return protocol_capabilities_for(family, source_type, metadata).as_dict()


def provenance_hint_for(family: object, metadata: Mapping[str, object] | None = None) -> str:
    if metadata:
        explicit = str(metadata.get("provenance") or "").strip().lower()
        if explicit:
            return explicit
    return _PROVENANCE_BY_FAMILY.get(str(family or "").strip().lower(), "unknown")


def scope_hint_for(family: object, metadata: Mapping[str, object] | None = None) -> str:
    if metadata:
        explicit = str(metadata.get("scope_hint") or "").strip().lower()
        if explicit:
            return explicit
    return _SCOPE_BY_FAMILY.get(str(family or "").strip().lower(), "unknown")
