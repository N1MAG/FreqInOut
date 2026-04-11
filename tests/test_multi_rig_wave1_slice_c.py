from __future__ import annotations

import os
import socketserver
import sys
import threading
import types
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QCoreApplication

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.radio_interface.rigctl_client import (
    FrequencyCommand,
    RigctldClient,
    rig_control_client_from_settings,
)


class DummySettings:
    def __init__(self, values: dict[str, object] | None = None) -> None:
        self._values = dict(values or {})

    def get(self, key: str, default=None):
        return self._values.get(key, default)

    def reload(self) -> None:
        return None


class _RigctldState:
    def __init__(self) -> None:
        self.frequency_hz = 7_100_000
        self.ptt = 0
        self.vfo = "VFOA"
        self.commands: list[str] = []
        self.lock = threading.Lock()


class _RigctldHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        data = b""
        while True:
            chunk = self.request.recv(4096)
            if not chunk:
                break
            data += chunk
        lines = [line.strip() for line in data.decode("utf-8", errors="ignore").splitlines() if line.strip()]
        responses: list[str] = []
        state: _RigctldState = self.server.state  # type: ignore[attr-defined]
        for command in lines:
            with state.lock:
                state.commands.append(command)
                if command == "f":
                    responses.append(str(state.frequency_hz))
                    continue
                if command == "t":
                    responses.append(str(state.ptt))
                    continue
                if command.startswith("F "):
                    state.frequency_hz = int(command.split()[1])
                    responses.append("RPRT 0")
                    continue
                if command.startswith("V "):
                    state.vfo = command.split()[1]
                    responses.append("RPRT 0")
                    continue
                if command.startswith("M "):
                    responses.append("RPRT 0")
                    continue
                responses.append("RPRT -1")
        self.request.sendall(("\n".join(responses) + "\n").encode("utf-8"))


class _RigctldTestServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _RigctldHandler)
        self.state = _RigctldState()
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self.server_address[1])

    def __enter__(self) -> "_RigctldTestServer":
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.shutdown()
        self.server_close()
        self._thread.join(timeout=1.0)


class _DummyRig:
    def __init__(self, freq_hz: int = 7_078_000, ptt: bool = False) -> None:
        self.freq_hz = int(freq_hz)
        self.ptt = bool(ptt)
        self.commands: list[FrequencyCommand] = []

    def is_available(self) -> bool:
        return True

    def get_ptt(self) -> bool:
        return self.ptt

    def get_vfo_frequency(self) -> int:
        return self.freq_hz

    def set_frequency(self, cmd: FrequencyCommand) -> bool:
        self.commands.append(cmd)
        self.freq_hz = cmd.hz
        return True


def test_rigctld_client_sets_and_reads_frequency() -> None:
    with _RigctldTestServer() as server:
        client = RigctldClient(host="127.0.0.1", port=server.port, timeout=0.3)

        assert client.is_available() is True
        assert client.get_vfo_frequency() == 7_100_000
        assert client.set_frequency(FrequencyCommand(rig_hz=7_078_000, mode="SSB", band="40M", vfo="A")) is True
        assert client.get_vfo_frequency() == 7_078_000
        assert "V VFOA" in server.state.commands
        assert "M LSB 0" in server.state.commands
        assert "F 7078000" in server.state.commands


def test_rig_control_client_factory_selects_rigctld() -> None:
    client = rig_control_client_from_settings(
        DummySettings(
            {
                "control_via": "RIGCTLD",
                "rig_host": "10.0.0.44",
                "rig_port": 5532,
            }
        )
    )

    assert isinstance(client, RigctldClient)
    assert client.host == "10.0.0.44"
    assert client.port == 5532


def test_scheduler_engine_treats_rigctld_as_supported_control_mode(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QCoreApplication.instance() or QCoreApplication([])

    settings = SettingsManager()
    settings.set_many(
        {
            "control_via": "RIGCTLD",
            "rig_host": "10.0.0.44",
            "rig_port": 4532,
        }
    )

    rig = _DummyRig(freq_hz=7_078_000, ptt=True)
    engine = SchedulerEngine(rig=rig, js8=None, varac=None, fldigi_log=None)
    try:
        assert engine._control_mode() == "RIGCTLD"
        assert engine._current_rig_frequency(control_mode="RIGCTLD") == 7_078_000
        assert engine._status_poll_rig_ptt() is True
    finally:
        engine.stop()
        app.processEvents()


def test_main_window_rebuild_runtime_clients_switches_to_rigctld_backend(monkeypatch) -> None:
    reportlab_mod = sys.modules.setdefault("reportlab", types.ModuleType("reportlab"))
    lib_mod = sys.modules.setdefault("reportlab.lib", types.ModuleType("reportlab.lib"))
    pagesizes_mod = sys.modules.setdefault("reportlab.lib.pagesizes", types.ModuleType("reportlab.lib.pagesizes"))
    pdfgen_mod = sys.modules.setdefault("reportlab.pdfgen", types.ModuleType("reportlab.pdfgen"))
    canvas_mod = sys.modules.setdefault("reportlab.pdfgen.canvas", types.ModuleType("reportlab.pdfgen.canvas"))
    pagesizes_mod.letter = (612.0, 792.0)
    pdfgen_mod.canvas = canvas_mod
    reportlab_mod.lib = lib_mod
    reportlab_mod.pdfgen = pdfgen_mod

    from freqinout.gui.main_window import MainWindow
    import freqinout.gui.main_window as main_window_mod

    sentinel_rig = object()
    created: dict[str, object] = {}

    class FakeJS8:
        def __init__(self, host=None):
            created["js8_host"] = host

        def stop(self) -> None:
            created["stopped"] = True

    class FakeVarAC:
        pass

    class FakeFldigiLog:
        pass

    monkeypatch.setattr(main_window_mod, "rig_control_client_from_settings", lambda settings: sentinel_rig)
    monkeypatch.setattr(main_window_mod, "JS8ControlClient", FakeJS8)
    monkeypatch.setattr(main_window_mod, "VarACStatusClient", FakeVarAC)
    monkeypatch.setattr(main_window_mod, "FldigiLogStatusClient", FakeFldigiLog)

    window = MainWindow.__new__(MainWindow)
    window.settings = DummySettings(
        {
            "control_via": "RIGCTLD",
            "rig_host": "10.0.0.44",
            "rig_port": 5532,
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7362,
            "js8_host": "10.0.0.12",
            "js8_port": 2442,
        }
    )
    window._runtime_client_signature = None
    window.js8_control = SimpleNamespace(stop=lambda: created.setdefault("old_js8_stopped", True))
    window.scheduler = SimpleNamespace(rig=None, js8=None, varac=None, fldigi_log=None)

    MainWindow._rebuild_runtime_clients(window, force=True)

    assert window.rig_client is sentinel_rig
    assert window.scheduler.rig is sentinel_rig
    assert isinstance(window.js8_control, FakeJS8)
    assert created.get("js8_host") == "10.0.0.12"
    assert isinstance(window.varac_status, FakeVarAC)
    assert isinstance(window.fldigi_log_status, FakeFldigiLog)
