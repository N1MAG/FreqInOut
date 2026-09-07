from __future__ import annotations

import asyncio
import os
import types

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.mesh import MeshConnectionConfig, MeshConnectionType, MeshCoreBleAdvertisement, MeshHealthSnapshot, MeshMessage
from freqinout.core.mesh.meshcore_adapter import MeshCoreBleAdapter, MeshCoreBleCompanionClient
from freqinout.core.mesh.meshcore_codec import MESHCORE_PUSH_MSG_WAITING, MESHCORE_RESP_NO_MORE_MESSAGES
from freqinout.gui import main_window as main_window_module
from freqinout.gui.main_window import MainWindow
from freqinout.gui.settings_tab import SettingsTab


def _settings_tab(monkeypatch, tmp_path) -> tuple[QApplication, SettingsTab]:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])
    tab = SettingsTab()
    tab.mesh_enabled_chk.setChecked(True)
    tab._set_combo_data_if_present(tab.mesh_protocol_combo, "meshcore", fallback="meshtastic")
    tab._set_combo_data_if_present(tab.mesh_connection_type_combo, "ble", fallback="tcp")
    app.processEvents()
    return app, tab


def test_main_window_disconnect_then_restart_keeps_saved_config_and_schedules_worker_start(monkeypatch) -> None:
    saved_config = MeshConnectionConfig(
        adapter_id="meshcore-mobl1",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_name="MeshCore-N1MAG MOBL1",
    )

    scheduled: list[int] = []

    class DummyWindow:
        def __init__(self) -> None:
            self._mesh_runtime_restart_pending = True
            self._shutdown_close_pending = False
            self._mesh_worker_thread = None
            self._mesh_runtime_signature = ()
            self.stop_calls = 0
            self.start_calls = 0

        def _mesh_runtime_configs(self):
            return (saved_config,)

        def _mesh_runtime_signature_from_configs(self, configs):
            return tuple((cfg.adapter_id, cfg.protocol, cfg.enabled, cfg.connection_type.value) for cfg in configs)

        def _stop_mesh_runtime(self):
            self.stop_calls += 1

        def _start_mesh_runtime_if_enabled(self):
            self.start_calls += 1

    def single_shot(delay_ms: int, callback) -> None:
        scheduled.append(delay_ms)
        callback()

    dummy = DummyWindow()
    monkeypatch.setattr(main_window_module, "QTimer", types.SimpleNamespace(singleShot=single_shot))

    MainWindow._disconnect_mesh_runtime(dummy)
    assert dummy._mesh_runtime_restart_pending is False
    assert dummy.stop_calls == 1
    assert dummy._mesh_runtime_configs() == (saved_config,)

    MainWindow._restart_mesh_runtime_now(dummy)
    assert scheduled == [0]
    assert dummy.start_calls == 1
    assert dummy._mesh_runtime_signature == dummy._mesh_runtime_signature_from_configs((saved_config,))


def test_station_command_connect_restarts_saved_mesh_runtime_after_disconnect(monkeypatch) -> None:
    saved_config = MeshConnectionConfig(
        adapter_id="meshcore-field",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_name="MeshCore-N1MAG MOBL1",
    )

    scheduled: list[int] = []
    activated: dict[str, object] = {}

    class DummySettings:
        def __init__(self) -> None:
            self.saved_payloads: list[tuple[dict[str, object], bool]] = []

        def all(self):
            return {"meshcore-field": saved_config.to_mapping()}

        def set_many(self, payload, save=True):
            self.saved_payloads.append((dict(payload), bool(save)))

    class DummyWindow:
        def __init__(self) -> None:
            self.settings = DummySettings()
            self._mesh_runtime_restart_pending = False
            self._shutdown_close_pending = False
            self._mesh_worker_thread = None
            self._mesh_runtime_signature = self._mesh_runtime_signature_from_configs((saved_config,))
            self._mesh_runtime_stopping = False
            self.start_calls = 0
            self.refresh_calls = 0

        def _mesh_runtime_configs(self):
            return (saved_config,)

        def _mesh_runtime_signature_from_configs(self, configs):
            return tuple((cfg.adapter_id, cfg.protocol, cfg.enabled, cfg.connection_type.value) for cfg in configs)

        def _start_mesh_runtime_if_enabled(self):
            self.start_calls += 1

        def _stop_mesh_runtime(self):
            raise AssertionError("Disconnect should not be required for explicit Connect.")

        def _refresh_station_command_bar(self, force=False):
            self.refresh_calls += 1

        _station_command_mesh_protocol_prefix = staticmethod(MainWindow._station_command_mesh_protocol_prefix)

        def _restart_mesh_runtime_now(self):
            return MainWindow._restart_mesh_runtime_now(self)

    def single_shot(delay_ms: int, callback) -> None:
        scheduled.append(delay_ms)
        callback()

    def activate_mesh_connection_config(all_settings, selected_key, prefix=""):
        activated["args"] = (dict(all_settings), selected_key, prefix)
        return {"mesh_enabled": True, "mesh_protocol": "meshcore", "mesh_adapter_id": "meshcore-field"}

    dummy = DummyWindow()
    monkeypatch.setattr(main_window_module, "QTimer", types.SimpleNamespace(singleShot=single_shot))
    monkeypatch.setattr(main_window_module, "activate_mesh_connection_config", activate_mesh_connection_config)

    MainWindow._connect_saved_mesh_from_station_command(dummy, "connect:meshcore-field")

    assert activated["args"] == (
        {"meshcore-field": saved_config.to_mapping()},
        "meshcore-field",
        "meshcore",
    )
    assert dummy.settings.saved_payloads == [({"mesh_enabled": True, "mesh_protocol": "meshcore", "mesh_adapter_id": "meshcore-field"}, True)]
    assert scheduled == [0]
    assert dummy.start_calls == 1
    assert dummy.refresh_calls == 1
    assert dummy._mesh_runtime_signature == dummy._mesh_runtime_signature_from_configs((saved_config,))


def test_station_command_connect_reloads_settings_before_resolving_saved_endpoint(monkeypatch) -> None:
    saved_config = MeshConnectionConfig(
        adapter_id="meshcore-field",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_name="MeshCore-N1MAG MOBL1",
    )
    call_order: list[str] = []

    class DummySettings:
        def __init__(self) -> None:
            self.reloaded = False
            self.saved_payloads: list[tuple[dict[str, object], bool]] = []

        def reload(self) -> None:
            call_order.append("reload")
            self.reloaded = True

        def all(self):
            call_order.append("all")
            assert self.reloaded is True
            return {"meshcore-field": saved_config.to_mapping()}

        def set_many(self, payload, save=True):
            call_order.append("set_many")
            self.saved_payloads.append((dict(payload), bool(save)))

    class DummyWindow:
        def __init__(self) -> None:
            self.settings = DummySettings()
            self._mesh_runtime_restart_pending = False
            self._shutdown_close_pending = False
            self._mesh_worker_thread = None
            self._mesh_runtime_signature = self._mesh_runtime_signature_from_configs((saved_config,))
            self._mesh_runtime_stopping = False
            self.start_calls = 0
            self.refresh_calls = 0

        def _mesh_runtime_configs(self):
            return (saved_config,)

        def _mesh_runtime_signature_from_configs(self, configs):
            return tuple((cfg.adapter_id, cfg.protocol, cfg.enabled, cfg.connection_type.value) for cfg in configs)

        def _start_mesh_runtime_if_enabled(self):
            self.start_calls += 1

        def _stop_mesh_runtime(self):
            raise AssertionError("Disconnect should not be required for explicit Connect.")

        def _refresh_station_command_bar(self, force=False):
            self.refresh_calls += 1

        _station_command_mesh_protocol_prefix = staticmethod(MainWindow._station_command_mesh_protocol_prefix)

        def _restart_mesh_runtime_now(self):
            return MainWindow._restart_mesh_runtime_now(self)

    def activate_mesh_connection_config(all_settings, selected_key, prefix=""):
        call_order.append("activate")
        assert selected_key == "meshcore-field"
        assert prefix == "meshcore"
        assert "meshcore-field" in all_settings
        return {"mesh_enabled": True, "mesh_protocol": "meshcore", "mesh_adapter_id": "meshcore-field"}

    scheduled: list[int] = []

    def single_shot(delay_ms: int, callback) -> None:
        scheduled.append(delay_ms)
        callback()

    dummy = DummyWindow()
    monkeypatch.setattr(main_window_module, "QTimer", types.SimpleNamespace(singleShot=single_shot))
    monkeypatch.setattr(main_window_module, "activate_mesh_connection_config", activate_mesh_connection_config)

    MainWindow._connect_saved_mesh_from_station_command(dummy, "connect:meshcore-field")

    assert call_order[:3] == ["reload", "all", "activate"]
    assert dummy.settings.saved_payloads == [({"mesh_enabled": True, "mesh_protocol": "meshcore", "mesh_adapter_id": "meshcore-field"}, True)]
    assert scheduled == [0]
    assert dummy.start_calls == 1
    assert dummy.refresh_calls == 1


def test_mesh_runtime_restart_waits_for_thread_teardown_before_connecting(monkeypatch) -> None:
    saved_config = MeshConnectionConfig(
        adapter_id="meshcore-field",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_name="MeshCore-N1MAG MOBL1",
    )

    scheduled: list[int] = []
    stop_invocations: list[tuple[str, object, object]] = []
    start_calls: list[str] = []

    class DummySignal:
        def __init__(self) -> None:
            self.callbacks: list[object] = []

        def connect(self, callback: object) -> None:
            self.callbacks.append(callback)

    class DummyThread:
        def __init__(self) -> None:
            self.finished = DummySignal()
            self.wait_calls: list[int] = []
            self.parent = object()

        def isRunning(self) -> bool:
            return True

        def quit(self) -> None:
            pass

        def wait(self, timeout_ms: int) -> bool:
            self.wait_calls.append(timeout_ms)
            return False

        def setParent(self, parent: object | None) -> None:
            self.parent = parent

    class DummyWorker:
        def request_stop(self) -> None:
            stop_invocations.append(("request_stop", None, None))

        def stop(self) -> None:
            stop_invocations.append(("stop", None, None))

    def invoke_method(worker: object, method_name: str, connection_type: object) -> bool:
        stop_invocations.append(("invokeMethod", method_name, connection_type))
        return True

    def single_shot(delay_ms: int, callback) -> None:
        scheduled.append(delay_ms)
        callback()

    thread = DummyThread()
    worker = DummyWorker()

    class DummyWindow:
        settings = {}
        _mesh_runtime_restart_pending = False
        _shutdown_close_pending = False
        _mesh_runtime_stopping = False
        _mesh_worker_thread = thread
        _mesh_worker = worker
        _mesh_runtime_signature = ()

        def _mesh_runtime_configs(self):
            return (saved_config,)

        _mesh_runtime_signature_from_configs = staticmethod(MainWindow._mesh_runtime_signature_from_configs)

        def _start_mesh_runtime_if_enabled(self):
            start_calls.append("start")

        _stop_mesh_runtime = MainWindow._stop_mesh_runtime
        _guard_mesh_runtime_shutdown = MainWindow._guard_mesh_runtime_shutdown

    dummy = DummyWindow()
    monkeypatch.setattr(main_window_module, "QMetaObject", types.SimpleNamespace(invokeMethod=invoke_method))
    monkeypatch.setattr(main_window_module, "QTimer", types.SimpleNamespace(singleShot=single_shot))

    MainWindow._restart_mesh_runtime_now(dummy)

    assert dummy._mesh_runtime_restart_pending is True
    assert start_calls == []
    assert scheduled == []
    assert thread.wait_calls == [200]
    assert thread.parent is None
    assert len(thread.finished.callbacks) == 1

    MainWindow._on_mesh_runtime_thread_finished(dummy, runtime_thread=thread, runtime_worker=worker)

    assert scheduled == [0]
    assert start_calls == ["start"]
    assert dummy._mesh_runtime_restart_pending is False
    assert stop_invocations[0][0] == "request_stop"
    assert any(entry[0] == "invokeMethod" and entry[1] == "stop" for entry in stop_invocations)
    thread.finished.callbacks[0]()


def test_mesh_connection_indicator_refreshes_from_live_health_snapshot(monkeypatch, tmp_path) -> None:
    import freqinout.gui.settings_tab as settings_tab_module

    app, tab = _settings_tab(monkeypatch, tmp_path)
    health_rows = [
        {
            "adapter_id": "meshcore-field",
            "device_name": "MeshCore-N1MAG MOBL1",
            "connected": False,
            "lifecycle_state": "config_error",
            "last_error": "connection refused",
            "updated_utc": "2026-09-06T10:00:00Z",
        }
    ]
    monkeypatch.setattr(settings_tab_module, "list_mesh_health", lambda _db_path, transport=None: list(health_rows))
    try:
        tab.mesh_adapter_id_edit.setText("meshcore-field")
        tab.mesh_ble_device_id_edit.setText("97C92879-047E-FEA8-7A11-8A2EE82B381D")
        tab.mesh_ble_device_name_edit.setText("MeshCore-N1MAG MOBL1")

        tab._refresh_mesh_config_status()
        assert tab.mesh_connection_state_label.text() == "Needs attention"
        assert "Saved connection:" in tab.mesh_connection_state_label.toolTip()

        health_rows[:] = [
            {
                "adapter_id": "meshcore-field",
                "device_name": "MeshCore-N1MAG MOBL1",
                "connected": True,
                "lifecycle_state": "connected",
                "last_error": "",
                "updated_utc": "2026-09-06T10:05:00Z",
            }
        ]

        tab.on_mesh_health_ready(
            MeshHealthSnapshot(
                adapter_id="meshcore-field",
                transport="meshcore",
                enabled=True,
                connected=True,
                connection_type=MeshConnectionType.BLE.value,
                device_name="MeshCore-N1MAG MOBL1",
            )
        )
        app.processEvents()

        assert tab.mesh_connection_state_label.text() == "Connected: meshcore-1"
        assert "Device: MeshCore-N1MAG MOBL1" in tab.mesh_connection_state_label.toolTip()
    finally:
        tab.shutdown()
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_meshcore_default_policy_surfaces_do_not_synthesize_direct_feed(monkeypatch, tmp_path) -> None:
    app, tab = _settings_tab(monkeypatch, tmp_path)
    try:
        tab.mesh_adapter_id_edit.setText("meshcore-field")
        policies = tab._mesh_default_channel_policies()

        assert all(policy.channel_role != "direct" for policy in policies)

        route_message = MeshMessage(
            adapter_id="meshcore-field",
            transport="meshcore",
            message_id="meshcore-route-metadata",
            text="route metadata stays supported",
            channel="0",
            route_type="direct",
            direct_receive=True,
        )
        assert route_message.routing_context()["route_type"] == "direct"
        assert route_message.routing_context()["direct_receive"] is True
    finally:
        tab.shutdown()
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_meshcore_scan_results_make_select_device_available_after_failed_connection(monkeypatch, tmp_path) -> None:
    app, tab = _settings_tab(monkeypatch, tmp_path)
    try:
        tab._on_mesh_ble_scan_failed("Failed to connect to the saved device.")
        assert tab.mesh_ble_use_selected_btn.isEnabled() is False

        tab._on_mesh_ble_scan_finished(
            (
                MeshCoreBleAdvertisement(
                    name="MeshCore-N1MAG MOBL1",
                    address="97C92879-047E-FEA8-7A11-8A2EE82B381D",
                    rssi=-66,
                ),
            )
        )

        assert tab.mesh_ble_use_selected_btn.isEnabled() is True
        assert tab.mesh_ble_results_combo.count() == 1
        assert tab.mesh_ble_results_combo.currentData().address == "97C92879-047E-FEA8-7A11-8A2EE82B381D"

        tab.mesh_ble_use_selected_btn.click()
        app.processEvents()

        assert tab.mesh_ble_device_id_edit.text() == "97C92879-047E-FEA8-7A11-8A2EE82B381D"
        assert tab.mesh_ble_device_name_edit.text() == "MeshCore-N1MAG MOBL1"
        assert "Connecting to MeshCore-N1MAG MOBL1" in tab.mesh_status_label.text()
    finally:
        tab.shutdown()
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_station_command_mesh_source_chips_drop_stale_error_once_connected(monkeypatch) -> None:
    saved_config = MeshConnectionConfig(
        adapter_id="meshcore-mobl1",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_name="MeshCore-N1MAG MOBL1",
    )
    stale_row = {
        "adapter_id": "meshcore-mobl1",
        "device_name": "MeshCore-N1MAG MOBL1",
        "connected": False,
        "lifecycle_state": "config_error",
        "last_error": "connection refused",
        "updated_utc": "2026-09-05T10:00:00Z",
    }
    healthy_row = {
        "adapter_id": "meshcore-mobl1",
        "device_name": "MeshCore-N1MAG MOBL1",
        "connected": True,
        "lifecycle_state": "connected",
        "last_error": "",
        "updated_utc": "2026-09-06T10:00:00Z",
    }

    monkeypatch.setattr(main_window_module, "load_saved_mesh_connection_configs", lambda _settings: (saved_config,))
    monkeypatch.setattr(main_window_module, "list_mesh_health", lambda _db_path: [stale_row, healthy_row])

    class DummyWindow:
        settings = {}
        _station_command_mesh_config_chip_label = staticmethod(MainWindow._station_command_mesh_config_chip_label)
        _station_command_best_mesh_health_for_config = staticmethod(MainWindow._station_command_best_mesh_health_for_config)

    chips = MainWindow._station_command_mesh_source_chips(DummyWindow())

    assert chips == [
        {
            "name": "N1MAG MOBL1",
            "role": "eligible_success",
            "tooltip": "Mesh source: N1MAG MOBL1\nStatus: connected",
        }
    ]


def test_station_command_mesh_source_chip_does_not_keep_stale_success_green(monkeypatch) -> None:
    saved_config = MeshConnectionConfig(
        adapter_id="meshcore-mobl1",
        protocol="meshcore",
        enabled=True,
        connection_type=MeshConnectionType.BLE,
        ble_device_id="97C92879-047E-FEA8-7A11-8A2EE82B381D",
        ble_device_name="MeshCore-N1MAG MOBL1",
    )
    old_success = {
        "adapter_id": "meshcore-main",
        "device_name": "MeshCore-N1MAG MOBL1",
        "connected": True,
        "lifecycle_state": "connected",
        "last_error": "",
        "updated_utc": "2026-09-06T10:00:00Z",
    }
    current_failure = {
        "adapter_id": "meshcore-mobl1",
        "device_name": "MeshCore-N1MAG MOBL1",
        "connected": False,
        "lifecycle_state": "config_error",
        "last_error": "encryption timed out",
        "updated_utc": "2026-09-06T10:05:00Z",
    }

    monkeypatch.setattr(main_window_module, "load_saved_mesh_connection_configs", lambda _settings: (saved_config,))
    monkeypatch.setattr(main_window_module, "list_mesh_health", lambda _db_path: [old_success, current_failure])

    class DummyWindow:
        settings = {}
        _station_command_mesh_config_chip_label = staticmethod(MainWindow._station_command_mesh_config_chip_label)
        _station_command_best_mesh_health_for_config = staticmethod(MainWindow._station_command_best_mesh_health_for_config)

    chips = MainWindow._station_command_mesh_source_chips(DummyWindow())

    assert chips[0]["role"] == "warning"
    assert "encryption timed out" in chips[0]["tooltip"]


def test_meshcore_idle_receive_events_stays_silent_until_waiting_frame_arrives() -> None:
    class FakeBleClient:
        def __init__(self) -> None:
            self.callback = None
            self.respond_to_writes = False
            self.is_connected = True
            self.writes: list[bytes] = []

        async def start_notify(self, _uuid: str, callback) -> None:
            self.callback = callback

        async def stop_notify(self, _uuid: str) -> None:
            pass

        async def disconnect(self) -> None:
            self.is_connected = False

        async def write_gatt_char(self, _uuid: str, payload: bytes, response: bool = False) -> None:
            self.writes.append(bytes(payload))
            if self.respond_to_writes and self.callback is not None:
                self.callback(None, bytes([MESHCORE_RESP_NO_MORE_MESSAGES]))

    async def build_companion() -> tuple[FakeBleClient, MeshCoreBleCompanionClient]:
        raw = FakeBleClient()
        companion = MeshCoreBleCompanionClient(raw, command_timeout_sec=0.5)
        await companion.initialize()
        return raw, companion

    raw, companion = asyncio.run(build_companion())
    adapter = MeshCoreBleAdapter(
        MeshConnectionConfig(
            adapter_id="meshcore-mobl1",
            protocol="meshcore",
            enabled=True,
            connection_type=MeshConnectionType.BLE,
            ble_device_name="MeshCore-N1MAG MOBL1",
        )
    )
    adapter._client = companion

    idle_events = tuple(adapter.receive_events())
    assert idle_events == ()
    assert len(raw.writes) == 1

    raw.respond_to_writes = True
    companion.inject_frame_for_test(bytes([MESHCORE_PUSH_MSG_WAITING]))

    drained_events = tuple(adapter.receive_events())
    assert len(raw.writes) == 2
    assert drained_events == ()
    assert companion.waiting_messages_pending() is False
