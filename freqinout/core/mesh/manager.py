from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping

from freqinout.core.mesh.adapter_base import MeshAdapter
from freqinout.core.mesh.meshcore_adapter import MeshCoreBleAdapter
from freqinout.core.mesh.meshtastic_adapter import MeshConnectionError, MeshtasticLocalAdapter
from freqinout.core.mesh.models import MeshAdapterEvent, MeshChannel, MeshHealthSnapshot, MeshMessage, MeshNode
from freqinout.core.mesh.settings import MeshConnectionConfig

MeshEventListener = Callable[[MeshAdapterEvent], None]
MeshAdapterFactory = Callable[[MeshConnectionConfig], MeshAdapter]


def default_mesh_adapter_factory(config: MeshConnectionConfig) -> MeshAdapter:
    protocol = config.protocol.strip().lower()
    if protocol == "meshtastic":
        return MeshtasticLocalAdapter(config)
    if protocol == "meshcore":
        return MeshCoreBleAdapter(config)
    raise MeshConnectionError(f"{config.protocol or 'Mesh'} adapters are configured for a later implementation slice.")


class MeshConnectionManager:
    """Protocol-neutral mesh adapter lifecycle without Qt dependencies."""

    def __init__(
        self,
        configs: Iterable[MeshConnectionConfig] = (),
        *,
        adapter_factory: MeshAdapterFactory = default_mesh_adapter_factory,
    ) -> None:
        self._configs: dict[str, MeshConnectionConfig] = {}
        self._adapters: dict[str, MeshAdapter] = {}
        self._last_errors: dict[str, str] = {}
        self._listeners: list[MeshEventListener] = []
        self._adapter_factory = adapter_factory
        for config in configs:
            self.upsert_config(config)

    def upsert_config(self, config: MeshConnectionConfig) -> None:
        existing = self._configs.get(config.adapter_id)
        self._configs[config.adapter_id] = config
        if existing is not None and existing != config:
            self.stop_adapter(config.adapter_id)
            self._adapters.pop(config.adapter_id, None)

    def remove_config(self, adapter_id: str) -> None:
        self.stop_adapter(adapter_id)
        self._configs.pop(adapter_id, None)
        self._adapters.pop(adapter_id, None)
        self._last_errors.pop(adapter_id, None)

    def configured_ids(self) -> tuple[str, ...]:
        return tuple(self._configs)

    def active_adapter_ids(self) -> tuple[str, ...]:
        return tuple(self._adapters)

    def add_listener(self, listener: MeshEventListener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener: MeshEventListener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    def start_all(self) -> dict[str, MeshHealthSnapshot]:
        return {
            adapter_id: self.start_adapter(adapter_id)
            for adapter_id, config in self._configs.items()
            if config.enabled
        }

    def start_adapter(self, adapter_id: str) -> MeshHealthSnapshot:
        config = self._require_config(adapter_id)
        if not config.enabled:
            snapshot = self._snapshot_for_config(config, connected=False, last_error="Mesh connection is disabled.")
            self._publish_health(snapshot)
            return snapshot

        adapter = self._adapters.get(adapter_id)
        if adapter is None:
            try:
                adapter = self._adapter_factory(config)
                self._adapters[adapter_id] = adapter
            except Exception as exc:
                snapshot = self._snapshot_for_config(config, connected=False, last_error=str(exc))
                self._last_errors[adapter_id] = snapshot.last_error
                self._publish_health(snapshot)
                return snapshot

        try:
            adapter.connect()
            self._last_errors.pop(adapter_id, None)
            snapshot = adapter.health()
        except Exception as exc:
            snapshot = self._snapshot_for_config(config, connected=False, last_error=str(exc))
            self._last_errors[adapter_id] = snapshot.last_error
        self._publish_health(snapshot)
        return snapshot

    def stop_all(self) -> dict[str, MeshHealthSnapshot]:
        return {adapter_id: self.stop_adapter(adapter_id) for adapter_id in tuple(self._configs)}

    def stop_adapter(self, adapter_id: str) -> MeshHealthSnapshot:
        config = self._require_config(adapter_id)
        adapter = self._adapters.get(adapter_id)
        if adapter is not None:
            try:
                adapter.disconnect()
            except Exception as exc:
                self._last_errors[adapter_id] = str(exc)
        snapshot = self.health(adapter_id)
        self._publish_health(snapshot)
        return snapshot

    def health(self, adapter_id: str) -> MeshHealthSnapshot:
        config = self._require_config(adapter_id)
        adapter = self._adapters.get(adapter_id)
        if adapter is not None:
            try:
                snapshot = adapter.health()
                last_error = self._last_errors.get(adapter_id, "")
                if last_error and not snapshot.last_error:
                    return MeshHealthSnapshot(
                        adapter_id=snapshot.adapter_id,
                        transport=snapshot.transport,
                        enabled=snapshot.enabled,
                        connected=snapshot.connected,
                        connection_type=snapshot.connection_type,
                        device_name=snapshot.device_name,
                        firmware_version=snapshot.firmware_version,
                        battery_percent=snapshot.battery_percent,
                        battery_voltage=snapshot.battery_voltage,
                        last_rx=snapshot.last_rx,
                        last_tx=snapshot.last_tx,
                        last_error=last_error,
                        warnings=snapshot.warnings,
                    )
                return snapshot
            except Exception as exc:
                self._last_errors[adapter_id] = str(exc)
        return self._snapshot_for_config(config, connected=False, last_error=self._last_errors.get(adapter_id, ""))

    def health_snapshots(self) -> tuple[MeshHealthSnapshot, ...]:
        return tuple(self.health(adapter_id) for adapter_id in self._configs)

    def ingest_packet(self, adapter_id: str, packet: Mapping[str, object]) -> MeshMessage | None:
        adapter = self._require_adapter(adapter_id)
        ingest = getattr(adapter, "ingest_packet", None)
        if not callable(ingest):
            raise MeshConnectionError(f"{adapter_id} does not support packet ingestion.")
        message = ingest(packet)
        if message is not None:
            events = tuple(adapter.receive_events())
            if events:
                for event in events:
                    self._publish(event)
            else:
                self._publish(
                    MeshAdapterEvent(
                        event_type="message",
                        adapter_id=adapter_id,
                        transport=message.transport,
                        message=message,
                        raw=message.raw,
                    )
                )
        return message

    def poll_events(self, adapter_id: str) -> tuple[MeshAdapterEvent, ...]:
        adapter = self._require_adapter(adapter_id)
        events = tuple(adapter.receive_events())
        for event in events:
            self._publish(event)
        return events

    def poll_nodes(self, adapter_id: str) -> tuple[MeshNode, ...]:
        adapter = self._require_adapter(adapter_id)
        nodes = tuple(adapter.list_nodes())
        for node in nodes:
            self._publish(
                MeshAdapterEvent(
                    event_type="node",
                    adapter_id=adapter_id,
                    transport=node.transport,
                    node=node,
                    raw=node.raw,
                )
            )
        return nodes

    def poll_channels(self, adapter_id: str) -> tuple[MeshChannel, ...]:
        adapter = self._require_adapter(adapter_id)
        return tuple(adapter.list_channels())

    def _require_config(self, adapter_id: str) -> MeshConnectionConfig:
        try:
            return self._configs[adapter_id]
        except KeyError as exc:
            raise KeyError(f"Unknown mesh adapter: {adapter_id}") from exc

    def _require_adapter(self, adapter_id: str) -> MeshAdapter:
        try:
            return self._adapters[adapter_id]
        except KeyError as exc:
            raise MeshConnectionError(f"Mesh adapter {adapter_id} is not started.") from exc

    def _snapshot_for_config(
        self,
        config: MeshConnectionConfig,
        *,
        connected: bool,
        last_error: str = "",
    ) -> MeshHealthSnapshot:
        return MeshHealthSnapshot(
            adapter_id=config.adapter_id,
            transport=config.protocol or "mesh",
            enabled=config.enabled,
            connected=connected,
            connection_type=config.connection_type.value,
            device_name=config.endpoint_address,
            last_error=last_error,
        )

    def _publish_health(self, snapshot: MeshHealthSnapshot) -> None:
        self._publish(
            MeshAdapterEvent(
                event_type="health",
                adapter_id=snapshot.adapter_id,
                transport=snapshot.transport,
                health=snapshot,
            )
        )

    def _publish(self, event: MeshAdapterEvent) -> None:
        for listener in tuple(self._listeners):
            listener(event)
