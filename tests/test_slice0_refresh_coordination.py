from __future__ import annotations

import os
import time
import threading

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.dependency_status_service import DependencyStatusService
from freqinout.core.software_status_service import SoftwareStatusService


class _Settings:
    def get(self, _key: str, default=None):
        return default


_APP: QApplication | None = None


def _app() -> QApplication:
    global _APP
    _APP = QApplication.instance() or QApplication([])
    return _APP


def _status_rows() -> dict[str, dict[str, object]]:
    return {
        "JS8Call_API": {
            "state": "ok",
            "tooltip": "ready",
            "running": True,
            "reachable": True,
            "endpoint": "127.0.0.1:2442",
        }
    }


def test_endpoint_status_request_returns_cached_state_without_blocking_gui(monkeypatch) -> None:
    app = _app()
    calls: list[dict[str, object]] = []

    def slow_snapshot(self, **kwargs):
        calls.append(dict(kwargs))
        time.sleep(0.08)
        return _status_rows()

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", slow_snapshot)
    service = DependencyStatusService(_Settings())
    service._timer.stop()
    service.refresh_now = lambda **_kwargs: service.latest_snapshot()  # type: ignore[method-assign]
    try:
        started = time.perf_counter()
        first = service.status_snapshot(host_override="127.0.0.1", port_override=2442)
        elapsed_ms = (time.perf_counter() - started) * 1000.0

        assert elapsed_ms < 50.0
        assert first["JS8Call_API"]["stale"] is True

        deadline = time.monotonic() + 2.0
        latest = first
        while time.monotonic() < deadline:
            app.processEvents()
            latest = service.status_snapshot(host_override="127.0.0.1", port_override=2442)
            if latest["JS8Call_API"].get("endpoint") == "127.0.0.1:2442":
                break
            time.sleep(0.005)

        assert latest["JS8Call_API"]["endpoint"] == "127.0.0.1:2442"
        matching_calls = [
            call
            for call in calls
            if call.get("host_override") == "127.0.0.1"
            and call.get("port_override") == 2442
            and call.get("flrig_port_override") is None
        ]
        assert len(matching_calls) == 1
    finally:
        service.stop()


def test_endpoint_status_requests_coalesce_while_scope_is_pending(monkeypatch) -> None:
    _app()
    calls = 0

    def slow_snapshot(self, **_kwargs):
        nonlocal calls
        calls += 1
        time.sleep(0.08)
        return _status_rows()

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", slow_snapshot)
    service = DependencyStatusService(_Settings())
    service._timer.stop()
    service.refresh_now = lambda **_kwargs: service.latest_snapshot()  # type: ignore[method-assign]
    try:
        for _ in range(10):
            service.status_snapshot(host_override="127.0.0.1", port_override=2442, force=True)
        assert calls <= 1
    finally:
        service.stop()


def test_forced_process_refresh_still_obeys_single_flight() -> None:
    _app()
    service = DependencyStatusService(_Settings())
    service._timer.stop()
    started = threading.Event()
    release = threading.Event()
    calls = 0

    def slow_build(_sequence: int, _reason: str):
        nonlocal calls
        calls += 1
        started.set()
        release.wait(1.0)
        return service.latest_snapshot()

    service._build_process_snapshot = slow_build  # type: ignore[method-assign]
    try:
        service.refresh_now(reason="test", force=True)
        assert started.wait(0.5)
        for _ in range(10):
            service.refresh_now(reason="forced", force=True)
        assert calls == 1
    finally:
        release.set()
        deadline = time.monotonic() + 1.0
        while not service.is_stopped() and time.monotonic() < deadline:
            service.stop()
            time.sleep(0.005)
        service.stop()


def test_process_inventory_avoids_expensive_details_for_unrelated_processes(monkeypatch) -> None:
    class _Process:
        def __init__(self, name: str, cmdline: list[str] | None = None):
            self.info = {"name": name}
            self._cmdline = list(cmdline or [])
            self.exe_calls = 0
            self.cmdline_calls = 0

        def exe(self) -> str:
            self.exe_calls += 1
            return f"/usr/bin/{self.info['name']}"

        def cmdline(self) -> list[str]:
            self.cmdline_calls += 1
            return list(self._cmdline)

    unrelated = _Process("unrelated-daemon")
    direct = _Process("fldigi")
    wrapper = _Process("python3", ["python3", "/opt/tools/commstat.py"])
    processes = [unrelated, direct, wrapper]

    monkeypatch.setattr(
        "freqinout.core.software_status_service.psutil.process_iter",
        lambda attrs: processes,
    )
    SoftwareStatusService._shared_proc_snapshot_ts = 0.0
    service = SoftwareStatusService(_Settings())
    service._refresh_process_snapshot(force=True)

    assert unrelated.exe_calls == 0
    assert unrelated.cmdline_calls == 0
    assert direct.exe_calls == 1
    assert direct.cmdline_calls == 0
    assert wrapper.exe_calls == 0
    assert wrapper.cmdline_calls == 1
    assert service.program_is_running("FLDigi") is True
    assert service.program_is_running("CommStat") is True
