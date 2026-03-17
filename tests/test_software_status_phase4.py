from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.software_status_service import SoftwareStatusService


class DummySettings:
    def __init__(self, values: dict[str, object] | None = None):
        self._values = dict(values or {})

    def get(self, key: str, default=None):
        return self._values.get(key, default)


def test_flrig_api_reachable_uses_saved_port(monkeypatch):
    import freqinout.radio_interface.rigctl_client as rigctl_client

    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, host="127.0.0.1", port=12345, timeout=0.8, **kwargs):
            seen["host"] = host
            seen["port"] = port
            seen["timeout"] = timeout

        def is_available(self) -> bool:
            return True

    SoftwareStatusService._shared_service_probe_cache.clear()
    monkeypatch.setattr(rigctl_client, "FLRigClient", FakeClient)

    service = SoftwareStatusService(DummySettings({"flrig_port": 23456}))
    assert service.flrig_api_reachable(force=True)
    assert seen == {"host": "127.0.0.1", "port": 23456, "timeout": 0.35}


def test_fldigi_api_reachable_uses_saved_endpoint(monkeypatch):
    import freqinout.radio_interface.rigctl_client as rigctl_client

    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, host="127.0.0.1", port=12345, timeout=0.8, **kwargs):
            seen["host"] = host
            seen["port"] = port
            seen["timeout"] = timeout
            seen.update(kwargs)

        def is_fldigi_available(self) -> bool:
            return True

    SoftwareStatusService._shared_service_probe_cache.clear()
    monkeypatch.setattr(rigctl_client, "FLRigClient", FakeClient)

    service = SoftwareStatusService(
        DummySettings(
            {
                "flrig_host": "10.0.0.8",
                "flrig_port": 22345,
                "fldigi_host": "10.0.0.9",
                "fldigi_port": 7365,
            }
        )
    )
    assert service.fldigi_api_reachable(force=True)
    assert seen == {
        "host": "10.0.0.8",
        "port": 22345,
        "timeout": 0.35,
        "fldigi_host": "10.0.0.9",
        "fldigi_port": 7365,
    }


def test_rigctld_api_reachable_uses_saved_endpoint(monkeypatch):
    import freqinout.radio_interface.rigctl_client as rigctl_client

    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, host="127.0.0.1", port=4532, timeout=0.8, **kwargs):
            seen["host"] = host
            seen["port"] = port
            seen["timeout"] = timeout

        def is_available(self) -> bool:
            return True

    SoftwareStatusService._shared_service_probe_cache.clear()
    monkeypatch.setattr(rigctl_client, "RigctldClient", FakeClient)

    service = SoftwareStatusService(DummySettings({"rig_host": "10.0.0.44", "rig_port": 5532}))
    assert service.rigctld_api_reachable(force=True)
    assert seen == {"host": "10.0.0.44", "port": 5532, "timeout": 0.35}


def test_status_snapshot_warns_when_js8_process_endpoint_mismatch(monkeypatch):
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2442}))

    monkeypatch.setattr(service, "program_is_running", lambda name: name == "JS8Call")
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)

    snapshot = service.status_snapshot()
    info = snapshot["JS8Call_API"]

    assert info["state"] == "warn"
    assert info["running"] is True
    assert "configured tcp api unreachable" in str(info["tooltip"]).lower()
    assert "instance/port mismatch" in str(info["tooltip"]).lower()


def test_status_snapshot_marks_flrig_ok_when_configured_endpoint_is_reachable(monkeypatch):
    service = SoftwareStatusService(DummySettings({"flrig_host": "10.0.0.8", "flrig_port": 22345}))

    monkeypatch.setattr(service, "program_is_running", lambda name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: True)

    snapshot = service.status_snapshot()
    info = snapshot["FLRig"]

    assert info["state"] == "ok"
    assert info["running"] is True
    assert str(info["endpoint"]) == "10.0.0.8:22345"
    assert "reachable" in str(info["tooltip"]).lower()


def test_status_snapshot_marks_rigctld_ok_when_active_endpoint_is_reachable(monkeypatch):
    service = SoftwareStatusService(
        DummySettings({"control_via": "RIGCTLD", "rig_host": "10.0.0.44", "rig_port": 5532})
    )

    monkeypatch.setattr(service, "program_is_running", lambda name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "rigctld_api_reachable", lambda **kwargs: True)

    snapshot = service.status_snapshot()
    info = snapshot["RigCtlD"]

    assert info["state"] == "ok"
    assert info["running"] is True
    assert str(info["endpoint"]) == "10.0.0.44:5532"
    assert "reachable" in str(info["tooltip"]).lower()


def test_status_snapshot_warns_when_fldigi_process_endpoint_mismatch(monkeypatch):
    service = SoftwareStatusService(DummySettings({"fldigi_host": "10.0.0.9", "fldigi_port": 7365}))

    monkeypatch.setattr(service, "program_is_running", lambda name: name == "FLDigi")
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **kwargs: False)

    snapshot = service.status_snapshot()
    info = snapshot["FLDigi"]

    assert info["state"] == "warn"
    assert info["running"] is True
    assert "configured xml-rpc unreachable" in str(info["tooltip"]).lower()
    assert "instance/port mismatch" in str(info["tooltip"]).lower()


def test_status_snapshot_marks_fldigi_ok_when_configured_endpoint_is_reachable(monkeypatch):
    service = SoftwareStatusService(DummySettings({"fldigi_host": "10.0.0.9", "fldigi_port": 7365}))

    monkeypatch.setattr(service, "program_is_running", lambda name: False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **kwargs: True)

    snapshot = service.status_snapshot()
    info = snapshot["FLDigi"]

    assert info["state"] == "ok"
    assert info["running"] is True
    assert str(info["endpoint"]) == "10.0.0.9:7365"
    assert "reachable" in str(info["tooltip"]).lower()


def test_js8_api_reachable_does_not_fallback_to_js8net_by_default(monkeypatch):
    import freqinout.core.software_status_service as status_mod
    import freqinout.radio_interface.js8_status as js8_status_mod

    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2442}))

    def _raise_refused(*_args, **_kwargs):
        raise ConnectionRefusedError("refused")

    class FailIfConstructed:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("JS8ControlClient fallback should not be constructed")

    monkeypatch.setattr(status_mod.socket, "create_connection", _raise_refused)
    monkeypatch.setattr(js8_status_mod, "JS8ControlClient", FailIfConstructed)

    assert service.js8_api_reachable(force=True) is False


def test_js8_control_client_skips_js8net_start_when_endpoint_unreachable(monkeypatch):
    import freqinout.radio_interface.js8_status as js8_status_mod

    started: dict[str, object] = {}

    class DummyJs8Net:
        def start_net(self, *_args, **_kwargs):
            started["called"] = True

        def get_freq(self):
            return {"dial": 7078000}

    monkeypatch.setattr(js8_status_mod, "js8net", DummyJs8Net())
    monkeypatch.setattr(js8_status_mod.JS8ControlClient, "_js8call_running", staticmethod(lambda: True))

    attempts = {"count": 0}

    def _raise_refused(*_args, **_kwargs):
        attempts["count"] += 1
        raise ConnectionRefusedError("refused")

    monkeypatch.setattr(js8_status_mod.socket, "create_connection", _raise_refused)

    client = js8_status_mod.JS8ControlClient(host="127.0.0.1", port=2442, settings=DummySettings())
    assert client.get_frequency() is None
    assert client.get_frequency() is None
    assert started == {}
    assert attempts["count"] == 1


def test_settings_tab_refresh_running_status_uses_unsaved_flrig_port(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        tab,
        "_dispatch_status_refresh_request",
        lambda request: captured.update(request),
    )

    try:
        tab.flrig_port_edit.setText("24567")
        tab._refresh_running_status()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert captured.get("flrig_port_override") == 24567


def test_settings_tab_refresh_running_status_uses_unsaved_fldigi_endpoint(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        tab,
        "_dispatch_status_refresh_request",
        lambda request: captured.update(request),
    )

    try:
        tab.fldigi_host_edit.setText("10.1.1.7")
        tab.fldigi_port_edit.setText("7364")
        tab._refresh_running_status()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert captured.get("fldigi_host_override") == "10.1.1.7"
    assert captured.get("fldigi_port_override") == 7364


def test_settings_tab_refresh_running_status_dispatches_without_sync_snapshot(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        tab,
        "_dispatch_status_refresh_request",
        lambda request: captured.update(request),
    )
    monkeypatch.setattr(
        tab._status_service,
        "status_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("status_snapshot should not run on the UI thread")),
    )

    try:
        tab.js8_host_edit.setText("10.2.2.20")
        tab.js8_port_edit.setText("2542")
        tab._refresh_running_status()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert captured.get("host_override") == "10.2.2.20"
    assert captured.get("port_override") == 2542
