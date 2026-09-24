from __future__ import annotations

import asyncio
import inspect
import sys
import threading
import time
from concurrent.futures import CancelledError as FutureCancelledError, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from importlib import import_module, util
from typing import Callable, Iterator, Mapping

from freqinout.core.logger import log
from freqinout.core.mesh.ingest_status import MESHCORE_COMPANION_DECODER_WARNING
from freqinout.core.mesh.lifecycle import MeshOperationCancelled
from freqinout.core.mesh.meshcore_codec import (
    MESHCORE_CMD_GET_CONTACTS,
    MESHCORE_CMD_DEVICE_QUERY,
    MESHCORE_CMD_GET_CHANNEL,
    MESHCORE_CMD_SYNC_NEXT_MESSAGE,
    MESHCORE_RESP_CHANNEL_INFO,
    MESHCORE_RESP_CHANNEL_MSG_RECV,
    MESHCORE_RESP_CHANNEL_MSG_RECV_V3,
    MESHCORE_RESP_CONTACT,
    MESHCORE_RESP_CONTACTS_END,
    MESHCORE_RESP_CONTACTS_START,
    MESHCORE_RESP_CONTACT_MSG_RECV,
    MESHCORE_RESP_CONTACT_MSG_RECV_V3,
    MESHCORE_RESP_ERR,
    MESHCORE_RESP_NO_MORE_MESSAGES,
    MESHCORE_PUSH_NEW_ADVERT,
    decode_meshcore_channel_info_frame,
    decode_meshcore_contact_frame,
    decode_meshcore_new_advert_frame,
    decode_meshcore_waiting_message_frame,
    meshcore_frame_is_msg_waiting,
    meshcore_frame_is_no_more_messages,
    normalize_meshcore_channels,
    normalize_meshcore_nodes,
    normalize_meshcore_waiting_messages,
)
from freqinout.core.mesh.meshtastic_adapter import MeshConnectionError
from freqinout.core.mesh.models import (
    MeshAdapterEvent,
    MeshChannel,
    MeshChannelCapabilities,
    MeshHealthSnapshot,
    MeshMessage,
    MeshNode,
)
from freqinout.core.mesh.settings import MeshConnectionConfig, MeshConnectionType, validate_mesh_connection_config

MESHCORE_NUS_SERVICE_UUID = "6e400001b5a3f393e0a9e50e24dcca9e"
MESHCORE_NUS_RX_UUID = "6e400002b5a3f393e0a9e50e24dcca9e"
MESHCORE_NUS_TX_UUID = "6e400003b5a3f393e0a9e50e24dcca9e"

PAIRING_GUIDANCE = (
    "Open Bluetooth settings for this computer if pairing is requested, use the PIN shown on the device, "
    "then retry Local Mesh."
)
STALE_BOND_GUIDANCE = (
    "Disconnect phone/tablet clients, restart the card, and retry the saved device once. "
    "Normal disconnect and restart must not require re-pairing. "
    "If the card continues to report that it removed pairing information, the computer and card no longer share "
    "the same Bluetooth keys; re-pairing in system Bluetooth settings is the last-resort recovery because macOS "
    "does not provide applications a standard unpair API. Scan again after that recovery."
)
COMPANION_SERVICE_RECOVERY_GUIDANCE = (
    "Keep the saved Bluetooth pairing, restart the card if needed, wait for it to advertise, then choose Connect once. "
    "Re-pair only when the operating system reports an authentication, PIN, passkey, or removed-key error."
)
MESHCORE_RECEIVE_PENDING_WARNING = MESHCORE_COMPANION_DECODER_WARNING
MESHCORE_BLE_DISCONNECTING_MESSAGE = (
    "MeshCore Bluetooth is still disconnecting. Wait for it to finish before reconnecting."
)


class _MeshCoreBleSessionGate:
    """Serialize native BLE ownership across retiring and replacement workers."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._owner: object | None = None

    def acquire(self, owner: object, *, timeout_sec: float) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        with self._condition:
            while self._owner is not None and self._owner is not owner:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            self._owner = owner
            return True

    def release(self, owner: object) -> None:
        with self._condition:
            if self._owner is not owner:
                return
            self._owner = None
            self._condition.notify_all()


_MESHCORE_BLE_SESSION_GATE = _MeshCoreBleSessionGate()


@dataclass(frozen=True)
class MeshCoreBleAdvertisement:
    name: str
    address: str
    rssi: int | None = None
    service_uuids: tuple[str, ...] = ()


class MeshCoreBleCompanionClient:
    """Small MeshCore Companion bridge over a Bleak Nordic UART connection."""

    companion_receive_enabled = True

    def __init__(
        self,
        client: object,
        *,
        rx_uuid: str = MESHCORE_NUS_RX_UUID,
        tx_uuid: str = MESHCORE_NUS_TX_UUID,
        command_timeout_sec: float = 3.0,
        max_waiting_messages: int = 100,
    ) -> None:
        self._client = client
        self._rx_uuid = rx_uuid
        self._tx_uuid = tx_uuid
        self._command_timeout_sec = max(0.5, float(command_timeout_sec))
        self._max_waiting_messages = max(1, int(max_waiting_messages))
        self._response_queue: asyncio.Queue[bytes] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._notification_started = False
        self._raw_frames: list[bytes] = []
        self._msg_waiting_seen = False
        self.last_error = ""

    @property
    def is_connected(self) -> bool:
        return bool(getattr(self._client, "is_connected", False))

    async def initialize(self) -> None:
        start_notify = getattr(self._client, "start_notify", None)
        write_gatt_char = getattr(self._client, "write_gatt_char", None)
        if not callable(start_notify) or not callable(write_gatt_char):
            raise MeshConnectionError("MeshCore BLE connected, but this BLE client cannot receive Companion frames.")
        self._loop = asyncio.get_running_loop()
        await start_notify(self._tx_uuid, self._on_notification)
        self._notification_started = True
        try:
            await self._write_command(bytes([MESHCORE_CMD_DEVICE_QUERY, 3]))
        except Exception:
            # DeviceQuery is useful but not required for passive receive bring-up.
            pass

    async def disconnect(self) -> None:
        stop_notify = getattr(self._client, "stop_notify", None)
        if self._notification_started and callable(stop_notify):
            try:
                await stop_notify(self._tx_uuid)
            except Exception:
                pass
        disconnect = getattr(self._client, "disconnect", None)
        if callable(disconnect):
            await disconnect()

    async def getChannels(
        self,
        on_channel: Callable[[dict[str, object]], None] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[dict[str, object]]:
        channels: list[dict[str, object]] = []
        # Companion firmware exposes eight channel slots (0-7). Empty slots
        # are capacity, not feeds, but configured slots can be sparse, so scan
        # the bounded range and skip empties instead of treating the first one
        # as an end marker.
        for channel_idx in range(0, 8):
            if cancel_event is not None and cancel_event.is_set():
                raise MeshOperationCancelled("MeshCore channel refresh cancelled.")
            try:
                frame = await self._request(
                    bytes([MESHCORE_CMD_GET_CHANNEL, channel_idx]),
                    {MESHCORE_RESP_CHANNEL_INFO},
                )
            except (TimeoutError, MeshConnectionError) as exc:
                self.last_error = str(exc)
                break
            parsed = decode_meshcore_channel_info_frame(frame)
            if parsed is None:
                break
            # Empty CHANNEL_INFO responses represent unused capacity. They
            # must not become synthetic ``Channel N`` policies.
            if _meshcore_channel_slot_is_unused(parsed):
                continue
            channels.append(parsed)
            if on_channel is not None:
                on_channel(parsed)
        return channels

    async def getContacts(self) -> list[dict[str, object]]:
        if not self.is_connected:
            raise MeshConnectionError("MeshCore BLE device is not connected.")
        previous_queue = self._response_queue
        queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._response_queue = queue
        contacts: list[dict[str, object]] = []
        try:
            await self._write_command(bytes([MESHCORE_CMD_GET_CONTACTS]))
            while True:
                frame = await asyncio.wait_for(queue.get(), timeout=self._command_timeout_sec)
                if not frame:
                    continue
                if frame[0] == MESHCORE_RESP_ERR:
                    raise MeshConnectionError("MeshCore Companion contact sync returned an error response.")
                if frame[0] == MESHCORE_RESP_CONTACTS_START:
                    continue
                if frame[0] == MESHCORE_RESP_CONTACTS_END:
                    return contacts
                if frame[0] != MESHCORE_RESP_CONTACT:
                    continue
                parsed = decode_meshcore_contact_frame(frame)
                if parsed is not None:
                    contacts.append(parsed)
        finally:
            self._response_queue = previous_queue

    async def getWaitingMessages(self) -> list[dict[str, object]]:
        messages: list[dict[str, object]] = []
        for _ in range(self._max_waiting_messages):
            try:
                item = await self.syncNextMessage()
            except (TimeoutError, MeshConnectionError) as exc:
                self.last_error = str(exc)
                break
            if item is None:
                break
            messages.append(item)
        self._msg_waiting_seen = False
        return messages

    async def syncNextMessage(self) -> dict[str, object] | None:
        frame = await self._request(
            bytes([MESHCORE_CMD_SYNC_NEXT_MESSAGE]),
            {
                MESHCORE_RESP_CONTACT_MSG_RECV,
                MESHCORE_RESP_CHANNEL_MSG_RECV,
                MESHCORE_RESP_NO_MORE_MESSAGES,
                MESHCORE_RESP_CONTACT_MSG_RECV_V3,
                MESHCORE_RESP_CHANNEL_MSG_RECV_V3,
            },
        )
        if meshcore_frame_is_no_more_messages(frame):
            return None
        return decode_meshcore_waiting_message_frame(frame)

    def raw_frames_pending(self) -> tuple[bytes, ...]:
        frames = tuple(self._raw_frames)
        self._raw_frames.clear()
        return frames

    def waiting_messages_pending(self) -> bool:
        """Return whether firmware announced queued traffic.

        Sync-next is a command, not a harmless status poll. Sending it on every
        one-second worker tick can monopolize the BLE link and delay shutdown.
        """

        return self._msg_waiting_seen

    def inject_frame_for_test(self, frame: bytes | bytearray | memoryview) -> None:
        self._on_notification(None, frame)

    def _on_notification(self, sender: object, data: object) -> None:
        frame = bytes(data or b"")
        if not frame:
            return
        if meshcore_frame_is_msg_waiting(frame):
            self._msg_waiting_seen = True
        self._raw_frames.append(frame)
        queue = self._response_queue
        if queue is not None:
            def enqueue() -> None:
                try:
                    queue.put_nowait(frame)
                except asyncio.QueueFull:
                    pass

            loop = self._loop
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None
            if loop is not None and loop.is_running() and running_loop is not loop:
                loop.call_soon_threadsafe(enqueue)
            else:
                enqueue()

    async def _request(self, command: bytes, response_codes: set[int]) -> bytes:
        if not self.is_connected:
            raise MeshConnectionError("MeshCore BLE device is not connected.")
        previous_queue = self._response_queue
        queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._response_queue = queue
        try:
            await self._write_command(command)
            while True:
                frame = await asyncio.wait_for(queue.get(), timeout=self._command_timeout_sec)
                if frame and frame[0] == MESHCORE_RESP_ERR:
                    raise MeshConnectionError("MeshCore Companion command returned an error response.")
                if frame and frame[0] in response_codes:
                    return frame
        finally:
            self._response_queue = previous_queue

    async def _write_command(self, command: bytes) -> None:
        write_gatt_char = getattr(self._client, "write_gatt_char", None)
        if not callable(write_gatt_char):
            raise MeshConnectionError("MeshCore BLE client cannot write Companion commands.")
        try:
            await write_gatt_char(self._rx_uuid, command, response=False)
        except TypeError:
            await write_gatt_char(self._rx_uuid, command)


def meshcore_ble_available() -> bool:
    if "bleak" in sys.modules:
        return True
    try:
        return util.find_spec("bleak") is not None
    except ValueError:
        return False


def meshcore_python_available() -> bool:
    if "meshcore" in sys.modules:
        return True
    try:
        return util.find_spec("meshcore") is not None
    except ValueError:
        return False


class MeshCoreBleAdapter:
    transport_name = "meshcore"

    def __init__(self, config: MeshConnectionConfig) -> None:
        self.config = config
        self.adapter_id = config.adapter_id
        self._client: object | None = None
        self._ble_loop: _AsyncioLoopRunner | None = None
        self._pending_events: list[MeshAdapterEvent] = []
        self._last_error = ""
        self._last_rx = None
        self._device_name = config.endpoint_address
        self._saved_device_scan_fallback_attempted = False
        self._session_token = object()
        self._session_gate_owned = False
        self._session_teardown_pending = False
        self._session_state_lock = threading.Lock()

    def connect(self) -> None:
        if not self.config.enabled:
            raise MeshConnectionError("MeshCore adapter is disabled.")
        if self.config.connection_type is not MeshConnectionType.BLE:
            raise MeshConnectionError("MeshCore is currently implemented for Bluetooth LE Companion connections only.")
        issues = tuple(issue for issue in validate_mesh_connection_config(self.config) if issue.severity == "error")
        if issues:
            raise MeshConnectionError("; ".join(issue.message for issue in issues))
        if not meshcore_ble_available():
            raise MeshConnectionError(
                "The Python BLE package 'bleak' is not installed. Install it before using MeshCore BLE."
            )
        with self._session_state_lock:
            teardown_pending = self._session_teardown_pending
        if teardown_pending:
            self._last_error = MESHCORE_BLE_DISCONNECTING_MESSAGE
            raise MeshConnectionError(self._last_error)
        if self._client is not None:
            if bool(getattr(self._client, "is_connected", False)):
                return
            # A passive link loss leaves the Companion wrapper and its native
            # event loop in place. Retire that entire session before retrying.
            self.disconnect()
            with self._session_state_lock:
                teardown_pending = self._session_teardown_pending
            if teardown_pending:
                raise MeshConnectionError(MESHCORE_BLE_DISCONNECTING_MESSAGE)
        if not self._session_gate_owned:
            if not _MESHCORE_BLE_SESSION_GATE.acquire(self._session_token, timeout_sec=6.0):
                self._last_error = MESHCORE_BLE_DISCONNECTING_MESSAGE
                log.warning("MeshCore BLE connect deferred adapter=%s: prior session teardown is incomplete.", self.adapter_id)
                raise MeshConnectionError(self._last_error)
            self._session_gate_owned = True
            log.info("MeshCore BLE session acquired adapter=%s device=%s.", self.adapter_id, self._device_name)
        try:
            if self._ble_loop is None:
                self._ble_loop = _AsyncioLoopRunner()
            self._ble_loop.run(self._connect_ble(), timeout_sec=30.0)
            log.info("MeshCore BLE Companion session ready adapter=%s device=%s.", self.adapter_id, self._device_name)
        except MeshOperationCancelled:
            self._finish_ble_session()
            raise
        except MeshConnectionError as exc:
            self._last_error = str(exc)
            self._finish_ble_session()
            raise
        except Exception as exc:
            self._last_error = _pairing_error_message(exc)
            self._finish_ble_session()
            raise MeshConnectionError(self._last_error) from exc

    def disconnect(self) -> None:
        started_at = time.monotonic()
        client = self._client
        self._client = None
        log.info("MeshCore BLE disconnect requested adapter=%s.", self.adapter_id)
        try:
            if client is None:
                return
            disconnect = getattr(client, "disconnect", None)
            if not callable(disconnect):
                return
            result = disconnect()
            if _is_awaitable(result):
                runner = self._ble_loop
                if runner is not None:
                    runner.run(result, timeout_sec=5.0)
                else:
                    asyncio.run(result)
        except Exception as exc:
            self._last_error = str(exc)
            log.warning("MeshCore BLE disconnect failed adapter=%s raw=%s", self.adapter_id, str(exc))
        finally:
            stopped = self._finish_ble_session()
            elapsed_ms = (time.monotonic() - started_at) * 1000.0
            log.info(
                "MeshCore BLE disconnect teardown adapter=%s complete=%s elapsed_ms=%.1f.",
                self.adapter_id,
                stopped,
                elapsed_ms,
            )

    def cancel_pending_operation(self) -> None:
        runner = self._ble_loop
        if runner is not None:
            runner.cancel_current()

    def channel_capabilities(self) -> MeshChannelCapabilities:
        return MeshChannelCapabilities(
            guidance="Use the MeshCore companion application to configure or remove device channels.",
        )

    def _stop_ble_loop(self) -> tuple[_AsyncioLoopRunner | None, bool]:
        runner = self._ble_loop
        self._ble_loop = None
        if runner is not None:
            return runner, runner.stop()
        return None, True

    def _finish_ble_session(self) -> bool:
        runner, stopped = self._stop_ble_loop()
        if stopped:
            with self._session_state_lock:
                self._session_teardown_pending = False
            self._release_session_gate()
            return True
        self._last_error = MESHCORE_BLE_DISCONNECTING_MESSAGE
        with self._session_state_lock:
            self._session_teardown_pending = True
        log.warning("MeshCore BLE event loop is still stopping adapter=%s; retaining session ownership.", self.adapter_id)
        if runner is not None:
            threading.Thread(
                target=self._release_session_gate_after_runner,
                args=(runner,),
                name="FIO MeshCore BLE teardown",
                daemon=True,
            ).start()
        return False

    def _release_session_gate_after_runner(self, runner: _AsyncioLoopRunner) -> None:
        runner.wait_until_stopped()
        with self._session_state_lock:
            self._session_teardown_pending = False
        self._release_session_gate()
        log.info("MeshCore BLE delayed teardown completed adapter=%s.", self.adapter_id)

    def _release_session_gate(self) -> None:
        if not self._session_gate_owned:
            return
        _MESHCORE_BLE_SESSION_GATE.release(self._session_token)
        self._session_gate_owned = False

    def _run_adapter_awaitable(self, awaitable: object, *, timeout_sec: float = 10.0) -> object:
        if not _is_awaitable(awaitable):
            return awaitable
        runner = self._ble_loop
        if runner is not None:
            return runner.run(awaitable, timeout_sec=timeout_sec)
        return asyncio.run(awaitable)

    def _has_ble_loop(self) -> bool:
        return self._ble_loop is not None

    def _disconnect_without_stopping_loop(self, client: object) -> None:
        disconnect = getattr(client, "disconnect", None)
        if not callable(disconnect):
            return
        result = disconnect()
        if _is_awaitable(result):
            self._run_adapter_awaitable(result)

    def health(self) -> MeshHealthSnapshot:
        connected = bool(self._client is not None and getattr(self._client, "is_connected", False))
        warnings = [issue.message for issue in validate_mesh_connection_config(self.config) if issue.severity != "error"]
        if connected and not _client_has_companion_receive(self._client):
            warnings.append(MESHCORE_RECEIVE_PENDING_WARNING)
        return MeshHealthSnapshot(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            enabled=self.config.enabled,
            connected=connected,
            connection_type=self.config.connection_type.value,
            device_name=self._device_name,
            last_rx=self._last_rx,
            last_error=self._last_error,
            warnings=tuple(warnings),
        )

    def list_nodes(self) -> list[MeshNode]:
        for method_name in ("getContacts", "getNodes", "getKnownNodes", "getNodeDB", "listContacts", "listNodes"):
            nodes = normalize_meshcore_nodes(
                self._call_client_collection(method_name),
                adapter_id=self.adapter_id,
                transport=self.transport_name,
            )
            if nodes:
                return list(nodes)
        return []

    def list_channels(self) -> list[MeshChannel]:
        client_channels = self._call_client_collection("getChannels")
        channels = normalize_meshcore_channels(
            client_channels,
            adapter_id=self.adapter_id,
            transport=self.transport_name,
        )
        if channels:
            return list(channels)
        return [
            MeshChannel(
                adapter_id=self.adapter_id,
                transport=self.transport_name,
                index=0,
                name="Public",
                role="public",
                channel_id="0",
                privacy="public",
            ),
        ]

    def list_channels_incremental(
        self,
        on_channel: Callable[[MeshChannel], None],
        cancel_event: threading.Event | None = None,
    ) -> list[MeshChannel]:
        client = self._client
        method = getattr(client, "getChannels", None)
        if not callable(method):
            channels = self.list_channels()
            for channel in channels:
                on_channel(channel)
            return channels

        collected: list[MeshChannel] = []

        def _stage(raw: dict[str, object]) -> None:
            normalized = normalize_meshcore_channels(
                (raw,),
                adapter_id=self.adapter_id,
                transport=self.transport_name,
            )
            for channel in normalized:
                collected.append(channel)
                on_channel(channel)

        try:
            result = method(on_channel=_stage, cancel_event=cancel_event)
        except TypeError:
            result = method()
        raw_channels = self._run_adapter_awaitable(result, timeout_sec=120.0)
        if not collected:
            for channel in normalize_meshcore_channels(
                raw_channels,
                adapter_id=self.adapter_id,
                transport=self.transport_name,
            ):
                collected.append(channel)
                on_channel(channel)
        return collected

    def get_recent_messages(self) -> list[MeshMessage]:
        client = self._client
        pending = getattr(client, "waiting_messages_pending", None)
        if callable(pending) and not bool(pending()):
            return []
        raw_messages = self._call_client_collection("getWaitingMessages")
        messages = list(
            normalize_meshcore_waiting_messages(
                raw_messages,
                adapter_id=self.adapter_id,
                transport=self.transport_name,
            )
        )
        if messages:
            self._last_rx = max((message.rx_time for message in messages if message.rx_time is not None), default=None)
        return messages

    def receive_events(self) -> Iterator[MeshAdapterEvent]:
        for event in tuple(self._pending_events):
            yield event
        self._pending_events.clear()
        for frame in self._client_raw_frames_pending():
            node = self._node_from_push_frame(frame)
            if node is not None:
                yield MeshAdapterEvent(
                    event_type="node",
                    adapter_id=self.adapter_id,
                    transport=self.transport_name,
                    node=node,
                    raw=node.raw,
                )
        for message in self.get_recent_messages():
            yield MeshAdapterEvent(
                event_type="message",
                adapter_id=self.adapter_id,
                transport=self.transport_name,
                message=message,
                raw=message.raw,
            )

    def _client_raw_frames_pending(self) -> tuple[bytes, ...]:
        client = self._client
        if client is None:
            return ()
        method = getattr(client, "raw_frames_pending", None)
        if not callable(method):
            return ()
        try:
            frames = method()
        except Exception as exc:
            self._last_error = str(exc)
            return ()
        if frames is None:
            return ()
        if isinstance(frames, (bytes, bytearray, memoryview)):
            return (bytes(frames),)
        try:
            return tuple(bytes(frame) for frame in frames if frame)
        except TypeError:
            return ()

    def _node_from_push_frame(self, frame: bytes | bytearray | memoryview) -> MeshNode | None:
        data = bytes(frame)
        if not data or data[0] != MESHCORE_PUSH_NEW_ADVERT:
            return None
        parsed = decode_meshcore_new_advert_frame(data)
        if parsed is None:
            return None
        nodes = normalize_meshcore_nodes(
            [parsed],
            adapter_id=self.adapter_id,
            transport=self.transport_name,
        )
        return nodes[0] if nodes else None

    def _call_client_collection(self, method_name: str) -> tuple[object, ...]:
        client = self._client
        if client is None:
            return ()
        method = getattr(client, method_name, None)
        if not callable(method):
            return ()
        try:
            result = method()
            if _is_awaitable(result):
                result = self._run_adapter_awaitable(result)
        except Exception as exc:
            self._last_error = str(exc)
            return ()
        if result is None:
            return ()
        if isinstance(result, tuple):
            return result
        if isinstance(result, list):
            return tuple(result)
        return (result,)

    async def _connect_ble(self) -> None:
        bleak = import_module("bleak")
        saved_id = self.config.ble_device_id.strip()
        address = saved_id or await self._find_device_address(bleak)
        try:
            companion = await self._open_ble_target(bleak, address)
        except Exception as exc:
            log.warning(
                "MeshCore BLE saved-device open failed adapter=%s device=%s raw=%s",
                self.adapter_id,
                self.config.ble_device_name or saved_id or "unspecified",
                str(exc),
            )
            can_scan_fallback = bool(
                saved_id
                and not self._saved_device_scan_fallback_attempted
                and not _peer_removed_pairing_information(exc)
            )
            if not can_scan_fallback:
                raise MeshConnectionError(_pairing_error_message(exc)) from exc
            self._saved_device_scan_fallback_attempted = True
            discovered = await self._find_saved_device(bleak)
            if discovered is None:
                raise MeshConnectionError(_pairing_error_message(exc)) from exc
            discovered_device, discovered_name = discovered
            try:
                companion = await self._open_ble_target(bleak, discovered_device)
            except Exception as fallback_exc:
                log.warning(
                    "MeshCore BLE discovered-device retry failed adapter=%s device=%s raw=%s",
                    self.adapter_id,
                    discovered_name or self.config.ble_device_name or saved_id,
                    str(fallback_exc),
                )
                raise MeshConnectionError(_pairing_error_message(fallback_exc)) from fallback_exc
            self._device_name = discovered_name or self.config.ble_device_name or saved_id
        self._client = companion
        if not self._device_name or self._device_name == self.config.endpoint_address:
            self._device_name = self.config.ble_device_name or str(getattr(address, "address", address))
        self._saved_device_scan_fallback_attempted = False
        self._last_error = ""

    async def _open_ble_target(self, bleak: object, target: object) -> MeshCoreBleCompanionClient:
        started_at = time.monotonic()
        target_label = str(getattr(target, "address", "") or target or "unspecified")
        client = self._make_client(bleak, target)
        stage = "link_connect"
        try:
            log.info(
                "MeshCore BLE stage adapter=%s stage=link_connect target=%s.",
                self.adapter_id,
                target_label,
            )
            await client.connect()
            if not getattr(client, "is_connected", False):
                raise MeshConnectionError(f"MeshCore BLE device did not report connected. {PAIRING_GUIDANCE}")
            log.info(
                "MeshCore BLE stage adapter=%s stage=link_connected target=%s elapsed_ms=%.1f.",
                self.adapter_id,
                target_label,
                (time.monotonic() - started_at) * 1000.0,
            )
            stage = "service_discovery"
            await self._verify_meshcore_characteristics(client)
            log.info(
                "MeshCore BLE stage adapter=%s stage=services_verified target=%s elapsed_ms=%.1f.",
                self.adapter_id,
                target_label,
                (time.monotonic() - started_at) * 1000.0,
            )
            stage = "companion_initialize"
            companion = MeshCoreBleCompanionClient(client)
            await companion.initialize()
            log.info(
                "MeshCore BLE stage adapter=%s stage=companion_initialized target=%s elapsed_ms=%.1f.",
                self.adapter_id,
                target_label,
                (time.monotonic() - started_at) * 1000.0,
            )
            return companion
        except Exception as exc:
            log.warning(
                "MeshCore BLE stage failed adapter=%s stage=%s target=%s elapsed_ms=%.1f raw=%s",
                self.adapter_id,
                stage,
                target_label,
                (time.monotonic() - started_at) * 1000.0,
                str(exc),
            )
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                try:
                    await disconnect()
                except Exception:
                    pass
            raise

    async def _find_saved_device(self, bleak: object) -> tuple[object, str] | None:
        scanner = getattr(bleak, "BleakScanner", None)
        discover = getattr(scanner, "discover", None)
        if not callable(discover):
            return None
        try:
            discovered = await discover(timeout=max(5, int(self.config.ble_scan_timeout_sec)), return_adv=True)
        except TypeError:
            discovered = await discover(timeout=max(5, int(self.config.ble_scan_timeout_sec)))
        candidates: list[tuple[object, str, str]] = []
        values = discovered.values() if isinstance(discovered, dict) else (discovered or ())
        for item in values:
            device = item[0] if isinstance(item, tuple) and item else item
            adv = item[1] if isinstance(item, tuple) and len(item) > 1 else None
            address = str(getattr(device, "address", "") or "").strip()
            name = str(getattr(adv, "local_name", "") or getattr(device, "name", "") or "").strip()
            candidates.append((device, address, name))
        saved_id = self.config.ble_device_id.strip().casefold()
        saved_name = self.config.ble_device_name.strip().casefold()
        for device, address, name in candidates:
            if saved_id and address.casefold() == saved_id:
                return device, name
        for device, _address, name in candidates:
            if saved_name and name.casefold() == saved_name:
                return device, name
        return None

    async def _find_device_address(self, bleak: object) -> str:
        target_name = self.config.ble_device_name.strip()
        scanner = getattr(bleak, "BleakScanner", None)
        discover = getattr(scanner, "discover", None)
        if not target_name or not callable(discover):
            raise MeshConnectionError(f"MeshCore BLE needs a saved device id or advertised name. {PAIRING_GUIDANCE}")
        devices = await discover(timeout=max(5, int(self.config.ble_scan_timeout_sec)))
        for device in devices or ():
            name = str(getattr(device, "name", "") or "").strip()
            if name.casefold() == target_name.casefold():
                self._device_name = name
                return str(getattr(device, "address", "") or "").strip()
        raise MeshConnectionError(f"Could not find MeshCore BLE device named '{target_name}'. {PAIRING_GUIDANCE}")

    def _make_client(self, bleak: object, address: object) -> object:
        client_cls = getattr(bleak, "BleakClient", None)
        if client_cls is None:
            raise MeshConnectionError("The Python BLE package 'bleak' does not provide BleakClient.")
        timeout = max(5, int(self.config.ble_scan_timeout_sec))
        try:
            return client_cls(address, timeout=timeout)
        except TypeError:
            return client_cls(address)

    async def _verify_meshcore_characteristics(self, client: object) -> None:
        get_services = getattr(client, "get_services", None)
        if not callable(get_services):
            return
        try:
            services = await get_services()
        except Exception as exc:
            raise MeshConnectionError(_pairing_error_message(exc)) from exc
        uuids = {_normalize_uuid(getattr(service, "uuid", "")) for service in services or ()}
        for service in services or ():
            for characteristic in getattr(service, "characteristics", ()) or ():
                uuids.add(_normalize_uuid(getattr(characteristic, "uuid", "")))
        if uuids and not ({MESHCORE_NUS_RX_UUID, MESHCORE_NUS_TX_UUID} <= uuids):
            raise MeshConnectionError(
                "Connected over BLE, but MeshCore Companion characteristics were not available. "
                f"Verify this is the MeshCore device and pair it first. {PAIRING_GUIDANCE}"
            )


def _normalize_uuid(value: object) -> str:
    return str(value or "").replace("-", "").strip().lower()


def _meshcore_channel_slot_is_unused(channel: object) -> bool:
    if not isinstance(channel, dict):
        return False
    name = str(channel.get("name") or "").strip()
    secret = channel.get("secret")
    try:
        secret_bytes = bytes(secret or b"")
    except (TypeError, ValueError):
        secret_bytes = b""
    return not name and (not secret_bytes or not any(secret_bytes))


def _is_awaitable(value: object) -> bool:
    return inspect.isawaitable(value)


def _client_has_companion_receive(client: object | None) -> bool:
    if client is None:
        return False
    if bool(getattr(client, "companion_receive_enabled", False)):
        return callable(getattr(client, "getWaitingMessages", None))
    return bool(callable(getattr(client, "getWaitingMessages", None)) and callable(getattr(client, "getChannels", None)))


class _AsyncioLoopRunner:
    """Owns the long-lived asyncio loop used by one MeshCore client."""

    def __init__(self, operation_label: str = "MeshCore BLE") -> None:
        self._operation_label = str(operation_label or "MeshCore").strip()
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._current_future: object | None = None
        self._future_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name=f"FIO {self._operation_label}", daemon=True)
        self._thread.start()
        self._ready.wait(timeout=2)

    def run(self, awaitable: object, *, timeout_sec: float = 30.0) -> object:
        if not _is_awaitable(awaitable):
            return awaitable
        future = asyncio.run_coroutine_threadsafe(awaitable, self._loop)
        with self._future_lock:
            self._current_future = future
        try:
            return future.result(timeout=max(0.1, float(timeout_sec or 30.0)))
        except FutureCancelledError as exc:
            raise MeshOperationCancelled(f"{self._operation_label} operation cancelled.") from exc
        except FutureTimeoutError as exc:
            future.cancel()
            try:
                future.result(timeout=0.5)
            except (FutureCancelledError, FutureTimeoutError):
                pass
            raise MeshConnectionError(
                f"{self._operation_label} operation timed out. Check the selected device and connection."
            ) from exc
        finally:
            with self._future_lock:
                if self._current_future is future:
                    self._current_future = None

    def cancel_current(self) -> None:
        with self._future_lock:
            future = self._current_future
        cancel = getattr(future, "cancel", None)
        if callable(cancel):
            cancel()

    def stop(self) -> bool:
        self.cancel_current()
        if not self._thread.is_alive() or self._loop.is_closed():
            return True
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except RuntimeError:
            # The loop can close between the state check and the threadsafe
            # callback when disconnect completion and shutdown coincide.
            pass
        self._thread.join(timeout=2)
        return not self._thread.is_alive()

    def wait_until_stopped(self) -> None:
        if self._thread is threading.current_thread():
            return
        self._thread.join()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            pending = [task for task in asyncio.all_tasks(self._loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()


class MeshCorePythonCompanionClient:
    """Normalize meshcore_py serial/TCP sessions to FIO's Companion surface."""

    companion_receive_enabled = True

    def __init__(self, client: object, event_type: object) -> None:
        self._client = client
        self._event_type = event_type
        self._subscriptions: list[object] = []
        self._messages: list[dict[str, object]] = []
        self._message_lock = threading.Lock()
        self._auto_fetch_started = False

    @property
    def is_connected(self) -> bool:
        return bool(getattr(self._client, "is_connected", False))

    async def initialize(self) -> None:
        subscribe = getattr(self._client, "subscribe", None)
        start_auto_fetch = getattr(self._client, "start_auto_message_fetching", None)
        if not callable(subscribe) or not callable(start_auto_fetch):
            raise MeshConnectionError(
                "The installed MeshCore client cannot provide Companion receive events. Update the meshcore package."
            )
        channel_event = getattr(self._event_type, "CHANNEL_MSG_RECV", None)
        contact_event = getattr(self._event_type, "CONTACT_MSG_RECV", None)
        if channel_event is None or contact_event is None:
            raise MeshConnectionError(
                "The installed MeshCore client does not expose channel/contact receive events."
            )
        self._subscriptions.append(subscribe(channel_event, self._on_channel_message))
        self._subscriptions.append(subscribe(contact_event, self._on_contact_message))
        await start_auto_fetch()
        self._auto_fetch_started = True

    async def disconnect(self) -> None:
        if self._auto_fetch_started:
            stop_auto_fetch = getattr(self._client, "stop_auto_message_fetching", None)
            if callable(stop_auto_fetch):
                try:
                    await stop_auto_fetch()
                except Exception:
                    pass
            self._auto_fetch_started = False
        unsubscribe = getattr(self._client, "unsubscribe", None)
        if callable(unsubscribe):
            for subscription in self._subscriptions:
                try:
                    unsubscribe(subscription)
                except Exception:
                    continue
        self._subscriptions.clear()
        disconnect = getattr(self._client, "disconnect", None)
        if callable(disconnect):
            await disconnect()

    async def getChannels(
        self,
        on_channel: Callable[[dict[str, object]], None] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[dict[str, object]]:
        commands = getattr(self._client, "commands", None)
        get_channel = getattr(commands, "get_channel", None)
        if not callable(get_channel):
            return []
        channels: list[dict[str, object]] = []
        for channel_idx in range(8):
            if cancel_event is not None and cancel_event.is_set():
                raise MeshOperationCancelled("MeshCore channel refresh cancelled.")
            result = await get_channel(channel_idx)
            if _meshcore_event_is_error(result):
                continue
            payload = getattr(result, "payload", None)
            if not isinstance(payload, Mapping):
                continue
            raw = dict(payload)
            raw.setdefault("channel_idx", channel_idx)
            raw.setdefault("name", raw.get("channel_name"))
            raw.setdefault("secret", raw.get("channel_secret"))
            if _meshcore_channel_slot_is_unused(raw):
                continue
            channels.append(raw)
            if on_channel is not None:
                on_channel(raw)
        return channels

    async def getContacts(self) -> list[dict[str, object]]:
        ensure_contacts = getattr(self._client, "ensure_contacts", None)
        if callable(ensure_contacts):
            await ensure_contacts(follow=True)
        contacts = getattr(self._client, "contacts", {})
        if not isinstance(contacts, Mapping):
            return []
        return [dict(value) for value in contacts.values() if isinstance(value, Mapping)]

    def getWaitingMessages(self) -> list[dict[str, object]]:
        with self._message_lock:
            messages = list(self._messages)
            self._messages.clear()
        return messages

    def waiting_messages_pending(self) -> bool:
        with self._message_lock:
            return bool(self._messages)

    def raw_frames_pending(self) -> tuple[bytes, ...]:
        return ()

    def _on_channel_message(self, event: object) -> None:
        payload = getattr(event, "payload", None)
        if isinstance(payload, Mapping):
            with self._message_lock:
                self._messages.append({"channelMessage": dict(payload)})

    def _on_contact_message(self, event: object) -> None:
        payload = getattr(event, "payload", None)
        if isinstance(payload, Mapping):
            with self._message_lock:
                self._messages.append({"contactMessage": dict(payload)})


class MeshCorePythonAdapter(MeshCoreBleAdapter):
    """MeshCore Companion adapter for official meshcore_py serial/TCP clients."""

    def connect(self) -> None:
        if not self.config.enabled:
            raise MeshConnectionError("MeshCore adapter is disabled.")
        if self.config.connection_type not in {MeshConnectionType.SERIAL, MeshConnectionType.TCP}:
            raise MeshConnectionError("The official MeshCore client adapter supports USB serial and TCP only.")
        issues = tuple(issue for issue in validate_mesh_connection_config(self.config) if issue.severity == "error")
        if issues:
            raise MeshConnectionError("; ".join(issue.message for issue in issues))
        if not meshcore_python_available():
            raise MeshConnectionError(
                "The Python package 'meshcore' is not installed. Install the supported FIO mesh dependencies first."
            )
        if self._client is not None and bool(getattr(self._client, "is_connected", False)):
            return
        if self._client is not None:
            self.disconnect()
        label = f"MeshCore {self.config.connection_type.value.upper()}"
        self._ble_loop = _AsyncioLoopRunner(label)
        try:
            self._ble_loop.run(self._connect_python_client(), timeout_sec=30.0)
            self._last_error = ""
        except (MeshOperationCancelled, MeshConnectionError) as exc:
            self._last_error = str(exc)
            self._stop_python_loop()
            raise
        except Exception as exc:
            self._last_error = _meshcore_python_error_message(self.config, exc)
            self._stop_python_loop()
            raise MeshConnectionError(self._last_error) from exc

    def disconnect(self) -> None:
        client = self._client
        self._client = None
        try:
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                result = disconnect()
                if _is_awaitable(result) and self._ble_loop is not None:
                    self._ble_loop.run(result, timeout_sec=8.0)
        except Exception as exc:
            self._last_error = str(exc)
        finally:
            self._stop_python_loop()

    def cancel_pending_operation(self) -> None:
        runner = self._ble_loop
        if runner is not None:
            runner.cancel_current()

    async def _connect_python_client(self) -> None:
        module = import_module("meshcore")
        meshcore_class = getattr(module, "MeshCore", None)
        event_type = getattr(module, "EventType", None)
        if meshcore_class is None or event_type is None:
            raise MeshConnectionError("The installed meshcore package does not expose MeshCore and EventType.")
        if self.config.connection_type is MeshConnectionType.SERIAL:
            factory = getattr(meshcore_class, "create_serial", None)
            args = (self.config.serial_port, self.config.serial_baud)
        else:
            factory = getattr(meshcore_class, "create_tcp", None)
            args = (self.config.tcp_host, self.config.tcp_port)
        if not callable(factory):
            raise MeshConnectionError(
                f"The installed meshcore package does not support {self.config.connection_type.value.upper()}."
            )
        client = await factory(*args, auto_reconnect=False, default_timeout=5)
        if client is None:
            raise MeshConnectionError(_meshcore_no_handshake_message(self.config))
        wrapper = MeshCorePythonCompanionClient(client, event_type)
        try:
            await wrapper.initialize()
        except Exception:
            await wrapper.disconnect()
            raise
        self._client = wrapper
        self._device_name = self.config.display_name

    def _stop_python_loop(self) -> None:
        runner = self._ble_loop
        self._ble_loop = None
        if runner is not None:
            runner.stop()


def _meshcore_event_is_error(event: object) -> bool:
    event_type = getattr(event, "type", None)
    return str(getattr(event_type, "name", event_type) or "").strip().upper() == "ERROR"


def _meshcore_no_handshake_message(config: MeshConnectionConfig) -> str:
    if config.connection_type is MeshConnectionType.SERIAL:
        return (
            "MeshCore opened the serial port but the device did not complete the Companion handshake. "
            "Verify the port, baud rate, and Companion USB firmware mode."
        )
    return (
        "MeshCore reached the TCP endpoint but the device did not complete the Companion handshake. "
        "Verify the host, port, and Companion TCP/WiFi firmware mode."
    )


def _meshcore_python_error_message(config: MeshConnectionConfig, exc: BaseException) -> str:
    detail = str(exc or "").strip()
    if isinstance(exc, PermissionError) or "permission" in detail.casefold() or "busy" in detail.casefold():
        return (
            f"MeshCore could not open {config.serial_port or 'the serial port'} because it is busy or access was denied. "
            "Close other mesh applications, check device permissions, and retry."
        )
    endpoint = (
        f"{config.tcp_host}:{config.tcp_port}"
        if config.connection_type is MeshConnectionType.TCP
        else config.serial_port
    )
    return f"MeshCore {config.connection_type.value.upper()} connection to {endpoint or 'the selected device'} failed: {detail or 'unknown error'}"


def discover_meshcore_ble_devices(
    timeout_sec: int = 10,
    *,
    cancel_event: threading.Event | None = None,
    progress_callback: Callable[[tuple[MeshCoreBleAdvertisement, ...]], None] | None = None,
) -> tuple[MeshCoreBleAdvertisement, ...]:
    if not meshcore_ble_available():
        raise MeshConnectionError(
            "The Python BLE package 'bleak' is not installed. Install it before scanning for MeshCore BLE."
        )
    try:
        return tuple(
            asyncio.run(
                _discover_meshcore_ble_devices(
                    timeout_sec,
                    cancel_event=cancel_event,
                    progress_callback=progress_callback,
                )
            )
        )
    except (MeshConnectionError, MeshOperationCancelled):
        raise
    except Exception as exc:
        raise MeshConnectionError(_pairing_error_message(exc)) from exc


async def _discover_meshcore_ble_devices(
    timeout_sec: int,
    *,
    cancel_event: threading.Event | None = None,
    progress_callback: Callable[[tuple[MeshCoreBleAdvertisement, ...]], None] | None = None,
) -> tuple[MeshCoreBleAdvertisement, ...]:
    bleak = import_module("bleak")
    scanner = getattr(bleak, "BleakScanner", None)
    discover = getattr(scanner, "discover", None)
    if not callable(discover):
        raise MeshConnectionError("The Python BLE package 'bleak' does not provide BleakScanner.discover.")
    timeout = max(5, int(timeout_sec))
    if cancel_event is not None and cancel_event.is_set():
        raise MeshOperationCancelled("MeshCore BLE scan cancelled.")
    try:
        task = asyncio.create_task(discover(timeout=timeout, return_adv=True))
    except TypeError:
        task = asyncio.create_task(discover(timeout=timeout))
    while not task.done():
        if cancel_event is not None and cancel_event.is_set():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise MeshOperationCancelled("MeshCore BLE scan cancelled.")
        await asyncio.sleep(0.1)
    discovered = await task
    advertisements: list[MeshCoreBleAdvertisement] = []
    if isinstance(discovered, dict):
        values = discovered.values()
        for item in values:
            if isinstance(item, tuple) and len(item) >= 2:
                device, adv = item[0], item[1]
            else:
                device, adv = item, None
            advertisement = _advertisement_from_bleak(device, adv)
            if _looks_like_meshcore(advertisement):
                advertisements.append(advertisement)
    else:
        for device in discovered or ():
            advertisement = _advertisement_from_bleak(device, None)
            if _looks_like_meshcore(advertisement):
                advertisements.append(advertisement)
    result = tuple(_dedupe_advertisements(advertisements))
    if progress_callback is not None and result:
        progress_callback(result)
    return result


def _advertisement_from_bleak(device: object, advertisement_data: object | None) -> MeshCoreBleAdvertisement:
    name = str(
        getattr(advertisement_data, "local_name", "")
        or getattr(device, "name", "")
        or ""
    ).strip()
    address = str(getattr(device, "address", "") or "").strip()
    rssi = getattr(advertisement_data, "rssi", None)
    if rssi is None:
        rssi = getattr(device, "rssi", None)
    try:
        rssi_value = int(rssi) if rssi is not None else None
    except (TypeError, ValueError):
        rssi_value = None
    raw_service_uuids = getattr(advertisement_data, "service_uuids", None)
    if raw_service_uuids is None:
        raw_service_uuids = getattr(device, "metadata", {}).get("uuids", ())
    service_uuids = tuple(sorted({_normalize_uuid(uuid) for uuid in raw_service_uuids or () if str(uuid or "").strip()}))
    return MeshCoreBleAdvertisement(name=name, address=address, rssi=rssi_value, service_uuids=service_uuids)


def _looks_like_meshcore(advertisement: MeshCoreBleAdvertisement) -> bool:
    if not advertisement.address:
        return False
    if MESHCORE_NUS_SERVICE_UUID in advertisement.service_uuids:
        return True
    return "meshcore" in advertisement.name.casefold()


def _dedupe_advertisements(
    advertisements: list[MeshCoreBleAdvertisement],
) -> list[MeshCoreBleAdvertisement]:
    by_address: dict[str, MeshCoreBleAdvertisement] = {}
    for advertisement in advertisements:
        previous = by_address.get(advertisement.address)
        if previous is None:
            by_address[advertisement.address] = advertisement
            continue
        if previous.name:
            continue
        by_address[advertisement.address] = advertisement
    return sorted(
        by_address.values(),
        key=lambda item: (item.rssi is None, -(item.rssi or -999), item.name.casefold(), item.address),
    )


def _pairing_error_message(exc: object) -> str:
    text = str(exc)
    if isinstance(exc, MeshConnectionError):
        # Adapter-raised errors are already operator-facing. Reclassifying
        # them from keywords such as "pair" can hide the specific failure.
        return text
    lowered = text.casefold()
    if _peer_removed_pairing_information(exc):
        return f"The MeshCore card removed its saved Bluetooth pairing information. {STALE_BOND_GUIDANCE}"
    if any(term in lowered for term in ("failed to encrypt", "encryption timeout", "encrypt the connection")):
        return f"MeshCore BLE could not use the saved encryption keys. {STALE_BOND_GUIDANCE}"
    if any(term in lowered for term in ("pair", "pin", "passkey", "authenticate", "not authorized", "permission")):
        return f"MeshCore BLE pairing is required or incomplete. {PAIRING_GUIDANCE}"
    if any(term in lowered for term in ("characteristic", "service", "gatt", "subscribe", "notify")):
        return f"MeshCore BLE connected but Companion service setup failed. {COMPANION_SERVICE_RECOVERY_GUIDANCE}"
    return f"MeshCore BLE connection failed: {text}. {PAIRING_GUIDANCE}"


def _peer_removed_pairing_information(exc: object) -> bool:
    text = str(exc).casefold()
    return "peer removed pairing information" in text or "cberrordomain code=14" in text
