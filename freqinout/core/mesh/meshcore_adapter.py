from __future__ import annotations

import asyncio
import inspect
import sys
import threading
from concurrent.futures import CancelledError as FutureCancelledError, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from importlib import import_module, util
from typing import Iterator

from freqinout.core.mesh.ingest_status import MESHCORE_COMPANION_DECODER_WARNING
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
from freqinout.core.mesh.models import MeshAdapterEvent, MeshChannel, MeshHealthSnapshot, MeshMessage, MeshNode
from freqinout.core.mesh.settings import MeshConnectionConfig, MeshConnectionType, validate_mesh_connection_config

MESHCORE_NUS_SERVICE_UUID = "6e400001b5a3f393e0a9e50e24dcca9e"
MESHCORE_NUS_RX_UUID = "6e400002b5a3f393e0a9e50e24dcca9e"
MESHCORE_NUS_TX_UUID = "6e400003b5a3f393e0a9e50e24dcca9e"

PAIRING_GUIDANCE = (
    "Pair the MeshCore device in macOS Bluetooth Settings first if prompted, "
    "using the PIN shown on the device, then retry Local Mesh."
)
MESHCORE_RECEIVE_PENDING_WARNING = MESHCORE_COMPANION_DECODER_WARNING


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

    async def getChannels(self) -> list[dict[str, object]]:
        channels: list[dict[str, object]] = []
        for channel_idx in range(0, 32):
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
            channels.append(parsed)
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
        if self._ble_loop is None:
            self._ble_loop = _AsyncioLoopRunner()
        try:
            self._ble_loop.run(self._connect_ble(), timeout_sec=30.0)
        except MeshConnectionError as exc:
            self._last_error = str(exc)
            self._stop_ble_loop()
            raise
        except Exception as exc:
            self._last_error = _pairing_error_message(exc)
            self._stop_ble_loop()
            raise MeshConnectionError(self._last_error) from exc

    def disconnect(self) -> None:
        client = self._client
        self._client = None
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
        finally:
            self._stop_ble_loop()

    def _stop_ble_loop(self) -> None:
        runner = self._ble_loop
        self._ble_loop = None
        if runner is not None:
            runner.stop()

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
            return list(channels) + [self._direct_channel()]
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
            self._direct_channel(),
        ]

    def get_recent_messages(self) -> list[MeshMessage]:
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

    def _direct_channel(self) -> MeshChannel:
        return MeshChannel(
            adapter_id=self.adapter_id,
            transport=self.transport_name,
            index=-1,
            name="Direct",
            role="direct",
            channel_id="direct",
            privacy="direct",
        )

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
        address = self.config.ble_device_id or await self._find_device_address(bleak)
        client = self._make_client(bleak, address)
        try:
            await client.connect()
            if not getattr(client, "is_connected", False):
                raise MeshConnectionError(f"MeshCore BLE device did not report connected. {PAIRING_GUIDANCE}")
            await self._verify_meshcore_characteristics(client)
        except Exception:
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                try:
                    await disconnect()
                except Exception:
                    pass
            raise
        companion_client: object = client
        try:
            companion = MeshCoreBleCompanionClient(client)
            await companion.initialize()
            companion_client = companion
        except Exception as exc:
            self._last_error = str(exc)
        self._client = companion_client
        self._device_name = self.config.ble_device_name or str(address)
        if _client_has_companion_receive(self._client):
            self._last_error = ""

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

    def _make_client(self, bleak: object, address: str) -> object:
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


def _is_awaitable(value: object) -> bool:
    return inspect.isawaitable(value)


def _client_has_companion_receive(client: object | None) -> bool:
    if client is None:
        return False
    if bool(getattr(client, "companion_receive_enabled", False)):
        return callable(getattr(client, "getWaitingMessages", None))
    return bool(callable(getattr(client, "getWaitingMessages", None)) and callable(getattr(client, "getChannels", None)))


class _AsyncioLoopRunner:
    """Owns the long-lived asyncio loop used by a live BLE client."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="FIO MeshCore BLE", daemon=True)
        self._thread.start()
        self._ready.wait(timeout=2)

    def run(self, awaitable: object, *, timeout_sec: float = 30.0) -> object:
        if not _is_awaitable(awaitable):
            return awaitable
        future = asyncio.run_coroutine_threadsafe(awaitable, self._loop)
        try:
            return future.result(timeout=max(0.1, float(timeout_sec or 30.0)))
        except FutureTimeoutError as exc:
            future.cancel()
            try:
                future.result(timeout=0.5)
            except (FutureCancelledError, FutureTimeoutError):
                pass
            raise MeshConnectionError(
                "MeshCore BLE operation timed out. Check that the device is awake, nearby, and still paired."
            ) from exc

    def stop(self) -> None:
        if self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2)

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


def discover_meshcore_ble_devices(timeout_sec: int = 10) -> tuple[MeshCoreBleAdvertisement, ...]:
    if not meshcore_ble_available():
        raise MeshConnectionError(
            "The Python BLE package 'bleak' is not installed. Install it before scanning for MeshCore BLE."
        )
    try:
        return tuple(asyncio.run(_discover_meshcore_ble_devices(timeout_sec)))
    except MeshConnectionError:
        raise
    except Exception as exc:
        raise MeshConnectionError(_pairing_error_message(exc)) from exc


async def _discover_meshcore_ble_devices(timeout_sec: int) -> tuple[MeshCoreBleAdvertisement, ...]:
    bleak = import_module("bleak")
    scanner = getattr(bleak, "BleakScanner", None)
    discover = getattr(scanner, "discover", None)
    if not callable(discover):
        raise MeshConnectionError("The Python BLE package 'bleak' does not provide BleakScanner.discover.")
    timeout = max(5, int(timeout_sec))
    try:
        discovered = await discover(timeout=timeout, return_adv=True)
    except TypeError:
        discovered = await discover(timeout=timeout)
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
    return tuple(_dedupe_advertisements(advertisements))


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
    lowered = text.casefold()
    if any(term in lowered for term in ("pair", "pin", "passkey", "authenticate", "not authorized", "permission")):
        return f"MeshCore BLE pairing is required or incomplete. {PAIRING_GUIDANCE}"
    if any(term in lowered for term in ("characteristic", "service", "gatt", "subscribe", "notify")):
        return f"MeshCore BLE connected but Companion service setup failed. {PAIRING_GUIDANCE}"
    return f"MeshCore BLE connection failed: {text}. {PAIRING_GUIDANCE}"
