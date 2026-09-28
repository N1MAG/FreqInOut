from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from PySide6.QtWidgets import QApplication

from freqinout.core.mesh import (
    MeshConnectionConfig,
    MeshConnectionManager,
    MeshConnectionWorker,
    MeshConnectionType,
    MeshChannelPolicy,
    MeshHealthSnapshot,
    MeshNode,
    MeshSendCapabilities,
    MeshSendRequest,
    MeshSendResult,
    append_mesh_send_audit,
    list_mesh_nodes,
    list_mesh_send_audit,
    mesh_outbound_capability,
    mesh_send_policy_issue,
    upsert_mesh_node,
    upsert_mesh_channel_policy,
)
from freqinout.core.mesh.meshcore_adapter import MeshCoreBleAdapter, MeshCorePythonAdapter
from freqinout.core.mesh.meshtastic_adapter import MeshConnectionError, MeshtasticLocalAdapter


class _Packet:
    id = 7123


class _MeshtasticInterface:
    def __init__(self, *, response: object | None = None) -> None:
        self.response = response
        self.calls: list[tuple[bytes, dict[str, object]]] = []
        self.responseHandlers: dict[object, object] = {}

    def sendData(self, payload: bytes, **kwargs: object) -> _Packet:
        self.calls.append((payload, dict(kwargs)))
        callback = kwargs.get("onResponse")
        if callable(callback) and self.response is not None:
            callback(self.response)
        return _Packet()


class _CoroutineRunner:
    def run(self, awaitable: object, *, timeout_sec: float) -> object:
        del timeout_sec
        return asyncio.run(awaitable)


class _MeshCoreClient:
    is_connected = True

    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    async def send_channel_message(self, channel: int, text: str) -> object:
        self.calls.append(("channel", channel, text))
        return SimpleNamespace(type=SimpleNamespace(name="OK"), payload={"code": 1})

    async def send_direct_message(
        self,
        destination: str,
        text: str,
        *,
        require_ack: bool,
        timeout_sec: float,
    ) -> object:
        self.calls.append(("direct", destination, text, require_ack, timeout_sec))
        return SimpleNamespace(type=SimpleNamespace(name="MSG_SENT"), payload={"expected_ack": "a1"})


class _ManagerAdapter:
    transport_name = "meshtastic"

    def __init__(self, config: MeshConnectionConfig) -> None:
        self.config = config
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def health(self) -> MeshHealthSnapshot:
        return MeshHealthSnapshot(
            adapter_id=self.config.adapter_id,
            transport=self.transport_name,
            enabled=self.config.enabled,
            connected=self.connected,
            connection_type=self.config.connection_type.value,
        )

    def send_capabilities(self) -> MeshSendCapabilities:
        return MeshSendCapabilities(supported=True, channel_send=True, direct_send=True, max_text_bytes=220)

    def send_message(self, request: MeshSendRequest) -> MeshSendResult:
        return MeshSendResult(
            request_id=request.request_id,
            adapter_id=request.adapter_id,
            transport=self.transport_name,
            destination_kind=request.destination_kind,
            destination_id=request.destination_id,
            channel_id=request.channel_id,
            state="accepted",
            requested_at=request.requested_at,
            evidence="api_accepted",
        )

    def list_nodes(self) -> list[object]:
        return []

    def list_channels(self) -> list[object]:
        return []

    def get_recent_messages(self) -> list[object]:
        return []

    def receive_events(self):
        return iter(())


def _config(
    *,
    protocol: str = "meshtastic",
    connection_type: MeshConnectionType = MeshConnectionType.TCP,
    send_enabled: bool = True,
) -> MeshConnectionConfig:
    return MeshConnectionConfig(
        adapter_id=f"{protocol}-outbound",
        protocol=protocol,
        enabled=True,
        send_enabled=send_enabled,
        connection_type=connection_type,
        tcp_host="127.0.0.1",
        serial_port="/dev/null",
        ble_device_id="test-device",
    )


def test_outbound_capability_matrix_includes_official_meshcore_ble_client() -> None:
    assert mesh_outbound_capability("meshtastic", MeshConnectionType.TCP).supported
    assert mesh_outbound_capability("meshtastic", MeshConnectionType.SERIAL).supported
    assert mesh_outbound_capability("meshtastic", MeshConnectionType.BLE).supported
    assert mesh_outbound_capability("meshcore", MeshConnectionType.TCP).supported
    assert mesh_outbound_capability("meshcore", MeshConnectionType.SERIAL).supported
    assert mesh_outbound_capability("meshcore", MeshConnectionType.BLE).supported


def test_manager_enforces_operator_send_enable_and_connected_session() -> None:
    disabled = _config(send_enabled=False)
    manager = MeshConnectionManager((disabled,), adapter_factory=_ManagerAdapter)
    manager.start_adapter(disabled.adapter_id)
    request = MeshSendRequest(
        adapter_id=disabled.adapter_id,
        text="status",
        destination_kind="channel",
        channel_id="0",
    )
    with pytest.raises(MeshConnectionError, match="not enabled"):
        manager.send_message(request)

    enabled = _config(send_enabled=True)
    manager = MeshConnectionManager((enabled,), adapter_factory=_ManagerAdapter)
    with pytest.raises(MeshConnectionError, match="not started"):
        manager.send_message(
            MeshSendRequest(
                adapter_id=enabled.adapter_id,
                text="status",
                destination_kind="channel",
                channel_id="0",
            )
        )
    manager.start_adapter(enabled.adapter_id)
    with pytest.raises(MeshConnectionError, match="between 0.1 and 30"):
        manager.send_message(
            MeshSendRequest(
                adapter_id=enabled.adapter_id,
                text="status",
                destination_kind="channel",
                channel_id="0",
                timeout_sec=float("inf"),
            )
        )
    result = manager.send_message(
        MeshSendRequest(
            adapter_id=enabled.adapter_id,
            text="status",
            destination_kind="channel",
            channel_id="0",
        )
    )
    assert result.state == "accepted"
    assert result.evidence == "api_accepted"


def test_meshtastic_channel_and_direct_sends_preserve_protocol_addressing_and_evidence() -> None:
    adapter = MeshtasticLocalAdapter(_config())
    channel_interface = _MeshtasticInterface()
    adapter._interface = channel_interface
    channel_result = adapter.send_message(
        MeshSendRequest(
            adapter_id=adapter.adapter_id,
            text="channel status",
            destination_kind="channel",
            channel_id="2",
        )
    )
    assert channel_result.state == "accepted"
    assert channel_result.evidence == "api_accepted"
    payload, kwargs = channel_interface.calls[0]
    assert payload == b"channel status"
    assert kwargs["destinationId"] == "^all"
    assert kwargs["channelIndex"] == 2
    assert kwargs["wantAck"] is False

    direct_interface = _MeshtasticInterface(response={"decoded": {"routing": {"errorReason": "NONE"}}})
    adapter._interface = direct_interface
    direct_result = adapter.send_message(
        MeshSendRequest(
            adapter_id=adapter.adapter_id,
            text="direct status",
            destination_kind="direct",
            destination_id="!11223344",
            channel_id="0",
            require_ack=True,
        )
    )
    assert direct_result.state == "acked"
    assert direct_result.evidence == "routing_ack"
    assert direct_interface.calls[0][1]["destinationId"] == "!11223344"
    assert direct_interface.calls[0][1]["wantAck"] is True


def test_meshtastic_ack_timeout_remains_accepted_without_claiming_delivery() -> None:
    adapter = MeshtasticLocalAdapter(_config())
    adapter._interface = _MeshtasticInterface(response=None)
    result = adapter.send_message(
        MeshSendRequest(
            adapter_id=adapter.adapter_id,
            text="direct status",
            destination_kind="direct",
            destination_id="!11223344",
            channel_id="0",
            require_ack=True,
            timeout_sec=0.01,
        )
    )
    assert result.state == "accepted"
    assert result.evidence == "api_accepted_ack_timeout"
    assert result.acknowledged_at is None
    assert result.retryable is False


def test_adapters_reject_oversize_utf8_payloads_before_transport_use() -> None:
    meshtastic = MeshtasticLocalAdapter(_config())
    meshtastic._interface = _MeshtasticInterface()
    with pytest.raises(MeshConnectionError, match="220-byte"):
        meshtastic.send_message(
            MeshSendRequest(
                adapter_id=meshtastic.adapter_id,
                text="é" * 111,
                destination_kind="channel",
                channel_id="0",
            )
        )

    meshcore = MeshCorePythonAdapter(_config(protocol="meshcore"))
    meshcore._client = _MeshCoreClient()
    meshcore._ble_loop = _CoroutineRunner()
    with pytest.raises(MeshConnectionError, match="160-byte"):
        meshcore.send_message(
            MeshSendRequest(
                adapter_id=meshcore.adapter_id,
                text="é" * 81,
                destination_kind="channel",
                channel_id="0",
            )
        )


def test_meshcore_python_send_reports_command_completion_and_node_ack() -> None:
    adapter = MeshCorePythonAdapter(_config(protocol="meshcore"))
    client = _MeshCoreClient()
    adapter._client = client
    adapter._ble_loop = _CoroutineRunner()

    channel_result = adapter.send_message(
        MeshSendRequest(
            adapter_id=adapter.adapter_id,
            text="ops status",
            destination_kind="channel",
            channel_id="3",
        )
    )
    assert channel_result.state == "sent"
    assert channel_result.evidence == "companion_command_complete"
    assert client.calls[0] == ("channel", 3, "ops status")

    direct_result = adapter.send_message(
        MeshSendRequest(
            adapter_id=adapter.adapter_id,
            text="private status",
            destination_kind="direct",
            destination_id="001122aabbcc",
            require_ack=True,
        )
    )
    assert direct_result.state == "acked"
    assert direct_result.evidence == "node_ack"


def test_legacy_raw_meshcore_ble_adapter_send_is_explicitly_blocked() -> None:
    adapter = MeshCoreBleAdapter(_config(protocol="meshcore", connection_type=MeshConnectionType.BLE))
    assert not adapter.send_capabilities().supported
    with pytest.raises(MeshConnectionError, match="receive-only"):
        adapter.send_message(
            MeshSendRequest(
                adapter_id=adapter.adapter_id,
                text="unsafe path",
                destination_kind="channel",
                channel_id="0",
            )
        )


def test_send_audit_is_append_only_and_does_not_store_message_plaintext(tmp_path) -> None:
    db_path = tmp_path / "mesh.db"
    request = MeshSendRequest(
        adapter_id="mesh-a",
        text="private operational plaintext",
        destination_kind="direct",
        destination_id="node-7",
    )
    append_mesh_send_audit(db_path, request, transport="meshtastic", state="requested")
    append_mesh_send_audit(
        db_path,
        request,
        result=MeshSendResult(
            request_id=request.request_id,
            adapter_id=request.adapter_id,
            transport="meshtastic",
            destination_kind="direct",
            destination_id="node-7",
            state="acked",
            evidence="routing_ack",
        ),
    )
    rows = list_mesh_send_audit(db_path, request_id=request.request_id)
    assert [row["state"] for row in rows] == ["requested", "acked"]
    assert rows[0]["payload_bytes"] == len(request.text.encode("utf-8"))
    assert request.text not in db_path.read_bytes().decode("utf-8", errors="ignore")


def test_outbound_policy_requires_an_accepted_channel_or_known_direct_node(tmp_path) -> None:
    db_path = tmp_path / "mesh.db"
    channel_request = MeshSendRequest(
        adapter_id="mesh-a",
        text="status",
        destination_kind="channel",
        channel_id="2",
    )
    assert "review" in mesh_send_policy_issue(db_path, channel_request).lower()
    upsert_mesh_channel_policy(
        db_path,
        MeshChannelPolicy(
            adapter_id="mesh-a",
            transport="meshtastic",
            channel_id="2",
            channel_name="Ops",
            channel_role="private",
            channel_privacy="encrypted",
            review_state="accepted",
            key_state="device_configured",
        ),
    )
    assert mesh_send_policy_issue(db_path, channel_request) == ""

    direct_request = MeshSendRequest(
        adapter_id="mesh-a",
        text="status",
        destination_kind="direct",
        destination_id="!11223344",
        channel_id="0",
    )
    assert "known nodes" in mesh_send_policy_issue(db_path, direct_request).lower()
    upsert_mesh_node(
        db_path,
        MeshNode(adapter_id="mesh-a", transport="meshtastic", node_id="!11223344"),
    )
    assert mesh_send_policy_issue(db_path, direct_request) == ""
    assert "review" in mesh_send_policy_issue(db_path, direct_request, transport="meshtastic").lower()
    upsert_mesh_channel_policy(
        db_path,
        MeshChannelPolicy(
            adapter_id="mesh-a",
            transport="meshtastic",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            channel_privacy="public",
            review_state="accepted",
        ),
    )
    assert mesh_send_policy_issue(db_path, direct_request, transport="meshtastic") == ""


def test_worker_owns_send_session_and_appends_requested_and_final_evidence(tmp_path) -> None:
    _app = QApplication.instance() or QApplication([])
    db_path = tmp_path / "mesh.db"
    config = _config()
    upsert_mesh_channel_policy(
        db_path,
        MeshChannelPolicy(
            adapter_id=config.adapter_id,
            transport="meshtastic",
            channel_id="0",
            channel_name="Public",
            channel_role="public",
            channel_privacy="public",
            review_state="accepted",
        ),
    )
    worker = MeshConnectionWorker((config,), db_path=db_path, adapter_factory=_ManagerAdapter)
    results = []
    worker.send_ready.connect(results.append)
    worker.start()
    request = MeshSendRequest(
        adapter_id=config.adapter_id,
        text="worker-owned send",
        destination_kind="channel",
        channel_id="0",
    )
    try:
        worker.send_message(request)
    finally:
        worker.stop()

    assert [result.state for result in results] == ["accepted"]
    audit = list_mesh_send_audit(db_path, request_id=request.request_id)
    assert [row["state"] for row in audit] == ["requested", "accepted"]
    assert audit[-1]["evidence"] == "api_accepted"


def test_meshcore_public_key_survives_node_persistence_for_direct_compose(tmp_path) -> None:
    db_path = tmp_path / "mesh.db"
    node = MeshNode(
        adapter_id="meshcore-a",
        transport="meshcore",
        node_id="node-short",
        public_key_or_hash="00112233445566778899aabbccddeeff",
        long_name="Operations Node",
    )
    upsert_mesh_node(db_path, node)
    rows = list_mesh_nodes(db_path, adapter_id="meshcore-a")
    assert rows[0]["public_key_or_hash"] == node.public_key_or_hash
