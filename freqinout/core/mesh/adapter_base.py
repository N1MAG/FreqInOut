from __future__ import annotations

from typing import Iterator, Protocol

from freqinout.core.mesh.models import MeshAdapterEvent, MeshChannel, MeshHealthSnapshot, MeshMessage, MeshNode


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
