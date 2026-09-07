from __future__ import annotations

from typing import Iterator, Mapping, Protocol

from freqinout.core.mesh.models import (
    MeshAdapterEvent,
    MeshChannel,
    MeshChannelCapabilities,
    MeshHealthSnapshot,
    MeshMessage,
    MeshNode,
)


class MeshAdapter(Protocol):
    adapter_id: str
    transport_name: str

    def connect(self) -> None:
        ...

    def disconnect(self) -> None:
        ...

    def health(self) -> MeshHealthSnapshot:
        ...

    def list_nodes(self) -> list[MeshNode]:
        ...

    def list_channels(self) -> list[MeshChannel]:
        ...

    def get_recent_messages(self) -> list[MeshMessage]:
        ...

    def receive_events(self) -> Iterator[MeshAdapterEvent]:
        ...

    def cancel_pending_operation(self) -> None:
        ...

    def channel_capabilities(self) -> MeshChannelCapabilities:
        ...

    def configure_channel(self, channel_id: str, updates: Mapping[str, object]) -> MeshChannel:
        ...

    def remove_channel(self, channel_id: str) -> None:
        ...
