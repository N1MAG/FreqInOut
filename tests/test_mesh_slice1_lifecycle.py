from __future__ import annotations

import asyncio
import json
import sys
import threading
import types

import pytest

from PySide6.QtWidgets import QApplication

from freqinout.core.mesh import (
    MeshChannel,
    MeshChannelCapabilities,
    MeshChannelPolicy,
    MeshConnectionConfig,
    MeshConnectionManager,
    MeshConnectionType,
    MeshConnectionWorker,
    MeshHealthSnapshot,
    MeshRetryPolicy,
    MeshRetryState,
    mesh_error_requires_operator_action,
    MeshOperationCancelled,
    archive_mesh_channel_policy,
    discover_meshcore_ble_devices,
    list_mesh_channel_policies,
    next_mesh_connection_name,
    update_automatic_connection_name,
)
from freqinout.core.mesh import meshcore_adapter
from freqinout.core.mesh.models import MeshAdapterEvent


class FakeLifecycleAdapter:
    transport_name = "meshtastic"

    def __init__(self, config: MeshConnectionConfig, *, fail_connects: int = 0) -> None:
        self.config = config
        self.adapter_id = config.adapter_id
        self.connected = False
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.cancel_calls = 0
        self.capability_calls = 0
        self.list_nodes_calls = 0
        self.list_channels_calls = 0
        self.configure_calls: list[tuple[str, dict[str, object]]] = []
        self.removed_channels: list[str] = []
        self.fail_connects = fail_connects
        self.capabilities = MeshChannelCapabilities(
            can_configure=True,
            can_remove_from_device=True,
            guidance="Use the companion app for unsupported changes.",
        )
        self.channels = [
            MeshChannel(
                adapter_id=self.adapter_id,
                transport=self.transport_name,
                index=0,
                name="Public",
                role="public",
                channel_id="0",
            ),
            MeshChannel(
                adapter_id=self.adapter_id,
                transport=self.transport_name,
                index=1,
                name="Ops",
                role="private",
                channel_id="1",
            ),
        ]

    def connect(self) -> None:
        self.connect_calls += 1
        if self.fail_connects > 0:
            self.fail_connects -= 1
            raise RuntimeError("transient connect failure")
        self.connected = True

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False

    def health(self) -> MeshHealthSnapshot:
        return MeshHealthSnapshot(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            enabled=self.config.enabled,
            connected=self.connected,
            connection_type=self.config.connection_type.value,
            device_name=self.config.display_name,
        )

    def list_nodes(self) -> list[object]:
        self.list_nodes_calls += 1
        return []

    def list_channels(self) -> list[MeshChannel]:
        self.list_channels_calls += 1
        return list(self.channels)

    def get_recent_messages(self) -> list[object]:
        return []

    def receive_events(self):
        return iter(())

    def cancel_pending_operation(self) -> None:
        self.cancel_calls += 1

    def channel_capabilities(self) -> MeshChannelCapabilities:
        self.capability_calls += 1
        return self.capabilities

    def configure_channel(self, channel_id: str, updates: dict[str, object]) -> MeshChannel:
        normalized_channel_id = str(channel_id)
        self.configure_calls.append((normalized_channel_id, dict(updates)))
        name = str(updates.get("name") or "").strip() or f"Channel {normalized_channel_id}"
        role = str(updates.get("role") or "").strip() or "unknown"
        channel = MeshChannel(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            index=int(normalized_channel_id) if normalized_channel_id.isdigit() else 0,
            name=name,
            role=role,
            channel_id=normalized_channel_id,
        )
        self.channels = [existing for existing in self.channels if str(existing.channel_id) != normalized_channel_id]
        self.channels.append(channel)
        self.channels.sort(key=lambda item: item.index)
        return channel

    def remove_channel(self, channel_id: str) -> None:
        normalized_channel_id = str(channel_id)
        self.removed_channels.append(normalized_channel_id)
        self.channels = [existing for existing in self.channels if str(existing.channel_id) != normalized_channel_id]


def test_mesh_connection_config_round_trip_keeps_identity_and_legacy_defaults() -> None:
    config = MeshConnectionConfig(
        adapter_id="meshcore-mobl1",
        protocol="meshcore",
        connection_name="Harbor mesh",
        connection_name_auto=False,
        source_radio_id="radio-7",
        source_role="primary",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_name="MeshCore-N1MAG MOBL1",
        ble_scan_timeout_sec=25,
        send_enabled=True,
    )

    restored = MeshConnectionConfig.from_mapping(json.loads(json.dumps(config.to_mapping())))
    legacy = MeshConnectionConfig.from_mapping(
        {
            "adapter_id": "meshcore-legacy",
            "protocol": "meshcore",
            "enabled": True,
            "connection_type": "ble",
            "ble_device_id": "97C92879-047E-FEA8-7A11-8A2EE82B381D",
            "ble_device_name": "MeshCore-N1MAG MOBL1",
        }
    )

    assert restored == config
    assert legacy.adapter_id == "meshcore-legacy"
    assert legacy.connection_name == "meshcore-legacy"
    assert legacy.connection_name_auto is False
    assert legacy.source_radio_id == ""
    assert legacy.source_role == ""


def test_mesh_connection_name_helpers_skip_taken_names_and_preserve_manual_edits() -> None:
    taken_configs = (
        MeshConnectionConfig(adapter_id="meshcore-1", protocol="meshcore", connection_name="meshcore-1"),
        MeshConnectionConfig(adapter_id="meshcore-2", protocol="meshcore"),
    )
    free_configs = (MeshConnectionConfig(adapter_id="meshcore-2", protocol="meshcore"),)

    assert next_mesh_connection_name("meshcore", taken_configs) == "meshcore-3"
    assert update_automatic_connection_name("meshtastic-1", "meshcore", is_auto=True, configs=free_configs) == "meshcore-1"
    assert update_automatic_connection_name("Harbor mesh", "meshcore", is_auto=False, configs=taken_configs) == "Harbor mesh"


def test_mesh_retry_policy_caps_exponential_delay_and_supports_manual_retry() -> None:
    policy = MeshRetryPolicy(initial_delay_ms=500, maximum_delay_ms=1200, multiplier=2.0)
    state = MeshRetryState()

    assert state.due(0) is True
    assert state.record_failure(1_000, policy) == 500
    assert state.record_failure(1_500, policy) == 1_000
    assert state.record_failure(2_500, policy) == 1_200
    assert state.remaining_ms(3_000) == 700

    state.retry_now()

    assert state.due(3_000) is True
    assert state.remaining_ms(3_000) == 0

    state.record_success()

    assert state.failure_count == 0
    assert state.next_retry_ms == 0


def test_mesh_retry_state_operator_action_contract_requires_manual_retry() -> None:
    state = MeshRetryState()

    assert mesh_error_requires_operator_action('CBErrorDomain Code=14 "Peer removed pairing information"') is True

    state.record_operator_action_required()

    assert state.failure_count == 1
    assert state.operator_action_required is True
    assert state.due(10_000) is False
    assert state.remaining_ms(10_000) == 0

    state.retry_now()

    assert state.failure_count == 1
    assert state.operator_action_required is False
    assert state.due(10_000) is True
    assert state.remaining_ms(10_000) == 0

    state.record_operator_action_required()

    assert state.failure_count == 2
    assert state.operator_action_required is True
    assert state.due(10_000) is False


def test_mesh_worker_blocks_code14_auto_retry_until_manual_retry() -> None:
    app = QApplication.instance() or QApplication([])
    operations: list[tuple[str, str, int]] = []

    class TerminalBondAdapter(FakeLifecycleAdapter):
        def connect(self) -> None:
            self.connect_calls += 1
            raise RuntimeError('CBErrorDomain Code=14 "Peer removed pairing information"')

    worker = MeshConnectionWorker(
        [MeshConnectionConfig(adapter_id="meshcore-field", enabled=True, tcp_host="192.0.2.2")],
        poll_interval_ms=250,
        reconnect_interval_ms=250,
        reconnect_max_interval_ms=1200,
        adapter_factory=TerminalBondAdapter,
    )
    worker.operation_state.connect(lambda adapter_id, state, value: operations.append((adapter_id, state, value)))

    try:
        worker.start()
        adapter = worker.manager()._adapters["meshcore-field"]
        assert adapter.connect_calls == 1
        assert worker._retry_states["meshcore-field"].operator_action_required is True
        assert operations[-1] == ("meshcore-field", "needs-attention", 0)

        worker.poll_once()
        assert adapter.connect_calls == 1

        worker.retry_now("meshcore-field")
        assert adapter.connect_calls == 2
        assert worker._retry_states["meshcore-field"].operator_action_required is True

        worker.poll_once()
        assert adapter.connect_calls == 2
    finally:
        worker.stop()
        worker.deleteLater()
        app.processEvents()


def test_mesh_connection_manager_cancels_and_delegates_channel_management() -> None:
    created: list[FakeLifecycleAdapter] = []

    def factory(config: MeshConnectionConfig) -> FakeLifecycleAdapter:
        adapter = FakeLifecycleAdapter(config)
        created.append(adapter)
        return adapter

    manager = MeshConnectionManager(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=factory,
    )
    manager.start_adapter("local")
    adapter = created[0]

    capabilities = manager.channel_capabilities("local")
    channel = manager.configure_channel(
        "local",
        "7",
        {
            "name": "Ops",
            "role": "private",
            "uplink_enabled": False,
            "downlink_enabled": True,
            "ignored": "drop-me",
        },
    )
    manager.remove_channel_from_device("local", "7")
    manager.cancel_pending_operations()

    assert capabilities.can_configure is True
    assert capabilities.can_remove_from_device is True
    assert adapter.capability_calls >= 1
    assert channel.channel_id == "7"
    assert adapter.configure_calls == [
        ("7", {"name": "Ops", "role": "private", "uplink_enabled": False, "downlink_enabled": True})
    ]
    assert adapter.removed_channels == ["7"]
    assert adapter.cancel_calls == 1


def test_mesh_connection_worker_retries_refreshes_and_honors_stop_cancel(tmp_path) -> None:
    app = QApplication.instance() or QApplication([])
    created: list[FakeLifecycleAdapter] = []
    operations: list[tuple[str, str, int]] = []
    channel_events: list[tuple[str, tuple[MeshChannel, ...]]] = []
    capability_events: list[tuple[str, MeshChannelCapabilities]] = []
    operation_snapshots: list[object] = []

    def factory(config: MeshConnectionConfig) -> FakeLifecycleAdapter:
        adapter = FakeLifecycleAdapter(config, fail_connects=1)
        created.append(adapter)
        return adapter

    worker = MeshConnectionWorker(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        poll_interval_ms=250,
        node_poll_interval_ms=250,
        channel_poll_interval_ms=250,
        reconnect_interval_ms=250,
        reconnect_max_interval_ms=1200,
        adapter_factory=factory,
    )
    worker.operation_state.connect(lambda adapter_id, state, value: operations.append((adapter_id, state, value)))
    worker.channels_ready.connect(lambda adapter_id, channels: channel_events.append((adapter_id, channels)))
    worker.channel_capabilities_ready.connect(
        lambda adapter_id, capabilities: capability_events.append((adapter_id, capabilities))
    )
    worker.operation_ready.connect(operation_snapshots.append)

    worker.start()
    assert created[0].connect_calls == 1
    assert operations[0] == ("local", "retrying", 250)

    worker.retry_now("local")
    assert created[0].connect_calls == 2
    assert ("local", "connecting", 0) in operations
    assert ("local", "connected", 0) in operations

    refresh_start = len(channel_events)
    worker.refresh_channels("local")
    refreshed_channels = [
        channel
        for _, emitted_channels in channel_events[refresh_start:]
        for channel in emitted_channels
    ]
    assert [channel.name for channel in refreshed_channels] == ["Public", "Ops"]
    assert capability_events[-1][0] == "local"
    assert capability_events[-1][1].can_configure is True
    channel_sync_snapshots = [
        snapshot
        for snapshot in operation_snapshots
        if snapshot.source_id == "local" and snapshot.request_class == "channel-sync"
    ]
    assert channel_sync_snapshots[0].state == "running"
    assert channel_sync_snapshots[-1].state == "complete"
    assert channel_sync_snapshots[-1].progress_current == 2
    assert channel_sync_snapshots[-1].completed_monotonic_ms >= channel_sync_snapshots[-1].started_monotonic_ms

    remove_start = len(channel_events)
    worker.remove_channel_from_device("local", "0")
    assert created[0].removed_channels == ["0"]
    removed_refresh_channels = [
        channel
        for _, emitted_channels in channel_events[remove_start:]
        for channel in emitted_channels
    ]
    assert [channel.channel_id for channel in removed_refresh_channels] == ["1"]

    worker.request_stop()
    assert created[0].cancel_calls == 1
    connect_calls_before_stop = created[0].connect_calls
    worker.retry_now("local")
    assert created[0].connect_calls == connect_calls_before_stop

    worker.stop()
    app.processEvents()


def test_mesh_connection_worker_skips_default_channel_polling_but_explicit_refresh_still_works() -> None:
    app = QApplication.instance() or QApplication([])
    created: list[FakeLifecycleAdapter] = []
    channel_events: list[tuple[str, tuple[MeshChannel, ...]]] = []

    def factory(config: MeshConnectionConfig) -> FakeLifecycleAdapter:
        adapter = FakeLifecycleAdapter(config)
        created.append(adapter)
        return adapter

    worker = MeshConnectionWorker(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=factory,
    )
    worker.channels_ready.connect(lambda adapter_id, channels: channel_events.append((adapter_id, channels)))
    worker._elapsed_ms = lambda: 1_000  # type: ignore[method-assign]

    worker.start()
    worker.poll_once()
    worker.poll_once()

    assert created[0].list_nodes_calls == 0
    assert created[0].list_channels_calls == 0
    assert channel_events == []

    worker._elapsed_ms = lambda: 300_999  # type: ignore[method-assign]
    worker.poll_once()
    assert created[0].list_nodes_calls == 0
    worker._elapsed_ms = lambda: 301_000  # type: ignore[method-assign]
    worker.poll_once()
    assert created[0].list_nodes_calls == 1
    assert created[0].list_channels_calls == 0

    worker.refresh_channels("local")

    assert created[0].list_channels_calls == 1
    assert [channel.channel_id for _, channels in channel_events for channel in channels] == ["0", "1"]

    worker.stop()
    app.processEvents()


def test_mesh_connection_worker_cancels_channel_refresh_during_shutdown() -> None:
    app = QApplication.instance() or QApplication([])
    created: list[FakeLifecycleAdapter] = []
    staged_channels: list[str] = []
    errors: list[str] = []
    capability_events: list[object] = []
    refresh_started = threading.Event()

    class SlowChannelAdapter(FakeLifecycleAdapter):
        def list_channels_incremental(self, on_channel, cancel_event=None):
            self.list_channels_calls += 1
            channel = self.channels[0]
            staged_channels.append(str(channel.channel_id))
            on_channel(channel)
            refresh_started.set()
            if cancel_event is None:
                raise AssertionError("channel refresh cancellation must supply a cancel event")
            while not cancel_event.wait(0.01):
                pass
            raise MeshOperationCancelled("channel refresh cancelled")

    def factory(config: MeshConnectionConfig) -> SlowChannelAdapter:
        adapter = SlowChannelAdapter(config)
        created.append(adapter)
        return adapter

    worker = MeshConnectionWorker(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        adapter_factory=factory,
    )
    worker.error_ready.connect(errors.append)
    worker.channel_capabilities_ready.connect(lambda *_args: capability_events.append(_args))
    worker.start()

    thread = threading.Thread(target=lambda: worker.refresh_channels("local"), daemon=True)
    thread.start()
    assert refresh_started.wait(1.0)

    worker.request_stop()
    thread.join(2.0)

    assert not thread.is_alive()
    assert created[0].cancel_calls == 1
    assert staged_channels == ["0"]
    operation = worker._active_operations[("local", "channel-sync")]
    assert operation.state == "cancelled"
    assert operation.progress_current == 1
    assert errors == []
    assert capability_events == []

    worker.stop()
    app.processEvents()


def test_mesh_connection_worker_reuses_adapter_state_across_restart() -> None:
    app = QApplication.instance() or QApplication([])
    created: list[FakeLifecycleAdapter] = []

    def factory(config: MeshConnectionConfig) -> FakeLifecycleAdapter:
        adapter = FakeLifecycleAdapter(config)
        created.append(adapter)
        return adapter

    worker = MeshConnectionWorker(
        [MeshConnectionConfig(adapter_id="local", enabled=True, tcp_host="192.0.2.2")],
        poll_interval_ms=250,
        node_poll_interval_ms=250,
        channel_poll_interval_ms=250,
        adapter_factory=factory,
    )

    worker.start()
    first_adapter = created[0]
    assert first_adapter.connect_calls == 1

    worker.stop()
    worker.start()

    assert created == [first_adapter]
    assert first_adapter.connect_calls == 2
    assert first_adapter.disconnect_calls == 1

    worker.stop()
    app.processEvents()


def test_archive_mesh_channel_policy_keeps_an_auditable_ignored_row(tmp_path) -> None:
    db_path = tmp_path / "mesh-channel-policy.db"
    policy = MeshChannelPolicy(
        adapter_id="meshcore-field",
        transport="meshcore",
        channel_id="public",
        channel_name="Public",
        channel_role="public",
        channel_privacy="public",
        inbox_enabled=True,
        ops_enabled=True,
        map_enabled=True,
        topic_scan_enabled=True,
        review_state="accepted",
        source="device",
    )

    archive_mesh_channel_policy(db_path, policy)
    rows = list_mesh_channel_policies(db_path, adapter_id="meshcore-field", transport="meshcore")

    assert len(rows) == 1
    assert rows[0].review_state == "ignored"
    assert rows[0].inbox_enabled is False
    assert rows[0].ops_enabled is False
    assert rows[0].map_enabled is False
    assert rows[0].topic_scan_enabled is False
    assert rows[0].source == "archived"


def test_discover_meshcore_ble_devices_honors_cancellation(monkeypatch) -> None:
    scan_started = threading.Event()
    cancel_event = threading.Event()
    outcome: list[BaseException] = []

    bleak_module = types.ModuleType("bleak")

    class FakeScanner:
        @staticmethod
        async def discover(timeout: int, return_adv: bool = False) -> dict[str, tuple[object, object]]:
            scan_started.set()
            await asyncio.sleep(60)
            return {}

    bleak_module.BleakScanner = FakeScanner
    monkeypatch.setitem(sys.modules, "bleak", bleak_module)
    monkeypatch.setattr(meshcore_adapter, "meshcore_ble_available", lambda: True)

    def run_scan() -> None:
        try:
            discover_meshcore_ble_devices(timeout_sec=7, cancel_event=cancel_event)
        except BaseException as exc:  # pragma: no cover - thread handoff
            outcome.append(exc)

    thread = threading.Thread(target=run_scan, daemon=True)
    thread.start()
    assert scan_started.wait(1.0)

    cancel_event.set()
    thread.join(2.0)

    assert not thread.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], MeshOperationCancelled)
    assert "scan cancelled" in str(outcome[0]).casefold()


def test_meshcore_encryption_timeout_explains_stale_bond_recovery() -> None:
    message = meshcore_adapter._pairing_error_message(
        RuntimeError('CBErrorDomain Code=15 "Failed to encrypt the connection, the connection has timed out unexpectedly."')
    )

    assert "saved encryption keys" in message
    assert "Disconnect phone/tablet clients" in message
    assert "Normal disconnect and restart must not require re-pairing" in message
    assert "last-resort recovery" in message
    assert "scan again" in message.lower()


def test_meshcore_service_discovery_failure_preserves_pairing_and_guides_one_retry() -> None:
    message = meshcore_adapter._pairing_error_message(
        RuntimeError("failed to discover services, device disconnected")
    )

    assert "Companion service setup failed" in message
    assert "Keep the saved Bluetooth pairing" in message
    assert "restart the card if needed" in message
    assert "choose Connect once" in message
    assert "Re-pair only" in message
