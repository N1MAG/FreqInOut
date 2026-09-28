from __future__ import annotations

import os
import sys

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from freqinout.core.software_status_service import SoftwareStatusService


class DummySettings:
    def __init__(self, values: dict[str, object] | None = None):
        self._values = dict(values or {})

    def get(self, key: str, default=None):
        return self._values.get(key, default)


def test_process_identity_uses_launch_arguments_when_radios_share_one_binary(monkeypatch):
    record = {
        "name": "flrig",
        "exe": "flrig",
        "exe_path": "/usr/local/bin/flrig",
        "cmd_tokens": ("flrig",),
        "cmd_paths": ("/usr/local/bin/flrig", "/profiles/FTDX-10"),
        "cmdline": (
            "/usr/local/bin/flrig",
            "--config-dir",
            "/profiles/FTDX-10",
        ),
    }
    monkeypatch.setattr(SoftwareStatusService, "_shared_proc_records", [record])
    service = SoftwareStatusService(DummySettings())

    assert service.cached_program_instance_running(
        "FLRig",
        "/usr/local/bin/flrig",
        ("--config-dir", "/profiles/FTDX-10"),
    )
    assert not service.cached_program_instance_running(
        "FLRig",
        "/usr/local/bin/flrig",
        ("--config-dir", "/profiles/FT-710"),
    )


def test_default_js8_process_identity_excludes_named_sibling(monkeypatch):
    named_record = {
        "name": "js8call-subspace",
        "exe": "js8call-subspace",
        "exe_path": "/usr/bin/js8call-subspace",
        "cmd_tokens": ("js8call-subspace",),
        "cmd_paths": ("/usr/bin/js8call-subspace",),
        "cmdline": ("/usr/bin/js8call-subspace", "--rig-name", "FT-710"),
    }
    monkeypatch.setattr(SoftwareStatusService, "_shared_proc_records", [named_record])
    service = SoftwareStatusService(DummySettings())

    assert not service.cached_program_instance_running(
        "JS8Call",
        "/usr/bin/js8call-subspace",
        excluded_options=("-r", "--rig-name"),
    )

    native_default = {**named_record, "cmdline": ("/usr/bin/js8call-subspace",)}
    monkeypatch.setattr(SoftwareStatusService, "_shared_proc_records", [native_default])
    assert service.cached_program_instance_running(
        "JS8Call",
        "/usr/bin/js8call-subspace",
        excluded_options=("-r", "--rig-name"),
    )


def test_selected_default_js8_status_does_not_credit_named_sibling(monkeypatch):
    service = SoftwareStatusService(DummySettings())
    seen: dict[str, object] = {}

    def running(_name, _target, arguments=(), *, excluded_options=()):
        seen["arguments"] = tuple(arguments)
        seen["excluded_options"] = tuple(excluded_options)
        return False

    monkeypatch.setattr(service, "program_instance_running", running)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **_kwargs: False)

    snapshot = service.status_snapshot(
        instance_identities={
            "JS8Call": {
                "target": "/usr/bin/js8call-subspace",
                "arguments": (),
                "instance_key": "JS8Call",
            }
        }
    )

    assert seen == {
        "arguments": (),
        "excluded_options": ("-r", "--rig-name"),
    }
    assert snapshot["JS8Call_API"]["running"] is False


def test_js8_api_fallback_keeps_requested_port(monkeypatch):
    import freqinout.core.software_status_service as status_module
    import freqinout.radio_interface.js8_status as js8_status_module

    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs):
            seen.update(kwargs)

        def get_frequency(self):
            return None

    def unavailable(*_args, **_kwargs):
        raise OSError("offline")

    service = SoftwareStatusService(DummySettings())
    monkeypatch.setattr(status_module.socket, "create_connection", unavailable)
    monkeypatch.setattr(js8_status_module, "JS8ControlClient", FakeClient)

    assert service.js8_api_reachable(
        host_override="127.0.0.1",
        port_override=2443,
        allow_fallback=True,
        force=True,
    ) is False
    assert seen["host"] == "127.0.0.1"
    assert seen["port"] == 2443


def test_flmsg_process_identity_uses_radio_scoped_nbems_arguments(monkeypatch):
    record = {
        "name": "flmsg",
        "exe": "flmsg",
        "exe_path": "/usr/local/bin/flmsg",
        "cmd_tokens": ("flmsg",),
        "cmd_paths": ("/usr/local/bin/flmsg", "/home/bill/.nbems/instances/FTDX-10"),
        "cmdline": (
            "/usr/local/bin/flmsg",
            "--flmsg-dir",
            "/home/bill/.nbems/instances/FTDX-10",
        ),
    }
    monkeypatch.setattr(SoftwareStatusService, "_shared_proc_records", [record])
    service = SoftwareStatusService(DummySettings())

    assert service.cached_program_instance_running(
        "FLMsg",
        "/usr/local/bin/flmsg",
        (
            "--flmsg-dir",
            "/home/bill/.nbems/instances/FTDX-10",
        ),
    )
    assert not service.cached_program_instance_running(
        "FLMsg",
        "/usr/local/bin/flmsg",
        (
            "--flmsg-dir",
            "/home/bill/.nbems/instances/FT-710",
        ),
    )


def test_flamp_process_identity_requires_the_selected_radio_root_and_ports(monkeypatch):
    record = {
        "name": "flamp",
        "exe": "flamp",
        "exe_path": "/usr/local/bin/flamp",
        "cmd_tokens": ("flamp",),
        "cmd_paths": ("/usr/local/bin/flamp", "/home/bill/.nbems/instances/FTDX-10"),
        "cmdline": (
            "/usr/local/bin/flamp",
            "--config-dir",
            "/home/bill/.nbems/instances/FTDX-10",
            "--arq-server-address",
            "127.0.0.1",
            "--arq-server-port",
            "7322",
            "--xmlrpc-server-address",
            "127.0.0.1",
            "--xmlrpc-server-port",
            "7362",
        ),
    }
    monkeypatch.setattr(SoftwareStatusService, "_shared_proc_records", [record])
    service = SoftwareStatusService(DummySettings())

    expected = record["cmdline"][1:]
    assert service.cached_program_instance_running("FLAmp", "/usr/local/bin/flamp", expected)
    assert not service.cached_program_instance_running(
        "FLAmp",
        "/usr/local/bin/flamp",
        tuple("7323" if value == "7322" else value for value in expected),
    )


@pytest.mark.parametrize(
    ("program", "process_name"),
    [
        ("FLMsg", "flmsg-4.0.24"),
        ("FLAmp", "flamp-2.2.14"),
    ],
)
def test_process_count_recognizes_version_qualified_fast_light_names(
    program: str,
    process_name: str,
) -> None:
    service = SoftwareStatusService(DummySettings())
    record = {
        "name": process_name,
        "exe": process_name,
        "exe_path": "",
        "cmd_tokens": (),
        "cmd_paths": (),
        "cmdline": (),
    }

    assert service.cached_program_process_count(
        program,
        process_records=(record,),
    ) == 1


@pytest.mark.parametrize(
    ("program", "target", "arguments", "record"),
    [
        (
            "FLAmp",
            r"C:\Program Files\W1HKJ\flamp.exe",
            (
                "--config-dir",
                r"C:\Users\Bill\NBEMS.files\instances\FT-710",
                "--arq-server-port",
                "7323",
                "--xmlrpc-server-port",
                "7363",
            ),
            {
                "name": "flamp.exe",
                "exe": "flamp.exe",
                "exe_path": r"C:\Program Files\W1HKJ\flamp.exe",
                "cmd_tokens": ("flamp.exe",),
                "cmd_paths": (r"C:\Program Files\W1HKJ\flamp.exe",),
                "cmdline": (
                    r"C:\Program Files\W1HKJ\flamp.exe",
                    "--config-dir",
                    r"C:\Users\Bill\NBEMS.files\instances\FT-710",
                    "--arq-server-port",
                    "7323",
                    "--xmlrpc-server-port",
                    "7363",
                ),
            },
        ),
        (
            "FLMsg",
            r"C:\Program Files\W1HKJ\flmsg.exe",
            (
                "--flmsg-dir",
                r"C:\Users\Bill\NBEMS.files\instances\FT-710",
            ),
            {
                "name": "flmsg.exe",
                "exe": "flmsg.exe",
                "exe_path": r"C:\Program Files\W1HKJ\flmsg.exe",
                "cmd_tokens": ("flmsg.exe",),
                "cmd_paths": (r"C:\Program Files\W1HKJ\flmsg.exe",),
                "cmdline": (
                    r"C:\Program Files\W1HKJ\flmsg.exe",
                    "--flmsg-dir",
                    r"C:\Users\Bill\NBEMS.files\instances\FT-710",
                ),
            },
        ),
        (
            "VarAC",
            r"C:\VarAC\VarAC.exe",
            (r"C:\VarAC\VarAC-ft-710.ini",),
            {
                "name": "varac.exe",
                "exe": "varac.exe",
                "exe_path": r"C:\VarAC\VarAC.exe",
                "cmd_tokens": ("varac.exe", "varac-ft-710.ini"),
                "cmd_paths": (
                    r"C:\VarAC\VarAC.exe",
                    r"C:\VarAC\VarAC-ft-710.ini",
                ),
                "cmdline": (
                    r"C:\VarAC\VarAC.exe",
                    r"C:\VarAC\VarAC-ft-710.ini",
                ),
            },
        ),
        (
            "FLAmp",
            "/Applications/FLAmp.app",
            (
                "--config-dir",
                "/Users/bill/Library/Application Support/NBEMS/instances/FT-710",
                "--arq-server-port",
                "7323",
                "--xmlrpc-server-port",
                "7363",
            ),
            {
                "name": "flamp",
                "exe": "flamp",
                "exe_path": "/Applications/FLAmp.app/Contents/MacOS/flamp",
                "cmd_tokens": ("flamp",),
                "cmd_paths": (
                    "/Applications/FLAmp.app/Contents/MacOS/flamp",
                    "/Users/bill/Library/Application Support/NBEMS/instances/FT-710",
                ),
                "cmdline": (
                    "/Applications/FLAmp.app/Contents/MacOS/flamp",
                    "--config-dir",
                    "/Users/bill/Library/Application Support/NBEMS/instances/FT-710",
                    "--arq-server-port",
                    "7323",
                    "--xmlrpc-server-port",
                    "7363",
                ),
            },
        ),
        (
            "FLMsg",
            "/Applications/FLMsg.app",
            (
                "--flmsg-dir",
                "/Users/bill/Library/Application Support/NBEMS/instances/FT-710",
            ),
            {
                "name": "flmsg",
                "exe": "flmsg",
                "exe_path": "/Applications/FLMsg.app/Contents/MacOS/flmsg",
                "cmd_tokens": ("flmsg",),
                "cmd_paths": (
                    "/Applications/FLMsg.app/Contents/MacOS/flmsg",
                    "/Users/bill/Library/Application Support/NBEMS/instances/FT-710",
                ),
                "cmdline": (
                    "/Applications/FLMsg.app/Contents/MacOS/flmsg",
                    "--flmsg-dir",
                    "/Users/bill/Library/Application Support/NBEMS/instances/FT-710",
                ),
            },
        ),
        (
            "VarAC",
            "/opt/homebrew/bin/wine64",
            (
                "/Users/bill/.wine/drive_c/VarAC/VarAC.exe",
                r"C:\VarAC\VarAC-ft-710.ini",
            ),
            {
                "name": "wine64-preloader",
                "exe": "",
                "exe_path": "",
                "cmd_tokens": ("wine64-preloader", "varac.exe", "varac-ft-710.ini"),
                "cmd_paths": (
                    "/opt/homebrew/bin/wine64",
                    "/Users/bill/.wine/drive_c/VarAC/VarAC.exe",
                    r"C:\VarAC\VarAC-ft-710.ini",
                ),
                "cmdline": (
                    "/opt/homebrew/bin/wine64",
                    "/Users/bill/.wine/drive_c/VarAC/VarAC.exe",
                    r"C:\VarAC\VarAC-ft-710.ini",
                ),
            },
        ),
    ],
)
def test_cross_platform_process_identity_uses_native_instance_selectors(
    program: str,
    target: str,
    arguments: tuple[str, ...],
    record: dict[str, object],
) -> None:
    service = SoftwareStatusService(DummySettings())

    assert service.cached_program_instance_running(
        program,
        target,
        arguments,
        process_records=(record,),
    )
    changed = tuple(
        "9999"
        if value in {"7323", "7363"}
        else value.replace("710", "999")
        for value in arguments
    )
    assert not service.cached_program_instance_running(
        program,
        target,
        changed,
        process_records=(record,),
    )


def test_status_snapshot_does_not_credit_other_radio_family_process(monkeypatch):
    service = SoftwareStatusService(DummySettings())
    monkeypatch.setattr(service, "program_is_running", lambda _name: True)
    monkeypatch.setattr(service, "program_instance_running", lambda _name, _target, _args=(): False)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **_kwargs: False)

    snapshot = service.status_snapshot(
        instance_identities={
            "FLRig": {
                "target": "/usr/local/bin/flrig",
                "arguments": ("--config-dir", "/profiles/FT-710"),
            }
        }
    )

    assert snapshot["FLRig"]["running"] is False
    assert snapshot["FLRig"]["state"] == "idle"


def test_forced_endpoint_snapshot_reuses_launch_preflight_process_inventory(monkeypatch):
    service = SoftwareStatusService(DummySettings())
    refresh_modes: list[bool] = []

    def record_refresh(*, force: bool = False) -> None:
        refresh_modes.append(bool(force))
        service._proc_snapshot = []
        service._proc_records = []

    monkeypatch.setattr(service, "_refresh_process_snapshot", record_refresh)
    monkeypatch.setattr(service, "js8_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **_kwargs: False)

    service.status_snapshot(force=True, force_process_snapshot=False)

    assert refresh_modes
    assert not any(refresh_modes)


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


def test_fldigi_probe_cache_includes_flrig_endpoint(monkeypatch):
    import freqinout.radio_interface.rigctl_client as rigctl_client

    seen_ports: list[int] = []

    class FakeClient:
        def __init__(self, host="127.0.0.1", port=12345, timeout=0.8, **kwargs):
            seen_ports.append(port)

        def is_fldigi_available(self) -> bool:
            return True

    SoftwareStatusService._shared_service_probe_cache.clear()
    monkeypatch.setattr(rigctl_client, "FLRigClient", FakeClient)

    service = SoftwareStatusService(DummySettings({"fldigi_host": "10.0.0.9", "fldigi_port": 7365}))
    assert service.fldigi_api_reachable(flrig_port_override=12345) is True
    assert service.fldigi_api_reachable(flrig_port_override=24567) is True
    assert seen_ports == [12345, 24567]


def test_status_snapshot_warns_when_js8_process_endpoint_mismatch(monkeypatch):
    service = SoftwareStatusService(DummySettings({"js8_host": "127.0.0.1", "js8_port": 2442}))

    monkeypatch.setattr(service, "program_is_running", lambda name: name == "JS8Call")
    monkeypatch.setattr(service, "js8_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "flrig_api_reachable", lambda **kwargs: False)
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **kwargs: False)

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
    monkeypatch.setattr(service, "fldigi_api_reachable", lambda **kwargs: False)

    snapshot = service.status_snapshot()
    info = snapshot["FLRig"]

    assert info["state"] == "ok"
    assert info["running"] is True
    assert str(info["endpoint"]) == "10.0.0.8:22345"
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


def test_settings_tab_refresh_running_status_uses_unsaved_flrig_port(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    captured: dict[str, object] = {}

    def fake_snapshot(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(tab._software_status_probe, "status_snapshot", fake_snapshot)

    try:
        tab.flrig_port_edit.setText("24567")
        tab._refresh_running_status()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert captured.get("flrig_port_override") == 24567


def test_settings_tab_refresh_running_status_uses_unsaved_fldigi_endpoint(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    captured: dict[str, object] = {}

    def fake_snapshot(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(tab._software_status_probe, "status_snapshot", fake_snapshot)

    try:
        tab.fldigi_host_edit.setText("10.1.1.7")
        tab.fldigi_port_edit.setText("7364")
        tab._refresh_running_status()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert captured.get("fldigi_host_override") == "10.1.1.7"
    assert captured.get("fldigi_port_override") == 7364


def test_settings_tab_selected_radio_status_uses_js8_edit_endpoint() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Edit:
        def __init__(self, value: str) -> None:
            self._value = value

        def text(self) -> str:
            return self._value

    class Probe:
        def __init__(self) -> None:
            self.captured: dict[str, object] = {}

        def status_snapshot(self, **kwargs):
            self.captured.update(kwargs)
            return {
                "JS8Call_API": {
                    "state": "warn",
                    "tooltip": "Process running, configured TCP API unreachable at 127.0.0.1:2242",
                }
            }

    probe = Probe()
    tab = SettingsTab.__new__(SettingsTab)
    tab._software_status_probe = probe
    tab.js8_host_edit = Edit("127.0.0.1")
    tab.js8_port_edit = Edit("2242")
    tab.flrig_port_edit = Edit("12345")
    tab.fldigi_host_edit = Edit("127.0.0.1")
    tab.fldigi_port_edit = Edit("7362")

    snapshot = tab._selected_radio_status_snapshot(force=True)

    assert probe.captured["host_override"] == "127.0.0.1"
    assert probe.captured["port_override"] == 2242
    assert snapshot["JS8Call_API"]["tooltip"].endswith("127.0.0.1:2242")


def test_settings_tab_status_refresh_not_throttled_when_endpoint_changes() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Edit:
        def __init__(self, value: str) -> None:
            self._value = value

        def text(self) -> str:
            return self._value

    class Label:
        def __init__(self) -> None:
            self.tooltip = ""

        def setStyleSheet(self, _style: str) -> None:
            pass

        def setToolTip(self, value: str) -> None:
            self.tooltip = value

    class Probe:
        def __init__(self) -> None:
            self.calls = 0

        def status_snapshot(self, **_kwargs):
            self.calls += 1
            return {
                "JS8Call_API": {
                    "state": "warn",
                    "tooltip": "Process running, configured TCP API unreachable at 127.0.0.1:2242",
                }
            }

    probe = Probe()
    label = Label()
    tab = SettingsTab.__new__(SettingsTab)
    tab.settings = DummySettings({})
    tab.status_labels = {"JS8Call_API": label}
    tab._current_visible_status_items = lambda: [("JS8Call_API", "JS8")]
    tab._software_status_probe = probe
    tab._status_service = None
    tab._last_running_status_refresh_ts = 9_999_999_999.0
    tab._running_status_refresh_interval_sec = 10.0
    tab._last_running_status_sig = (("JS8Call_API",), ("127.0.0.1", 2442, 12345, "127.0.0.1", 7362))
    tab.js8_host_edit = Edit("127.0.0.1")
    tab.js8_port_edit = Edit("2242")
    tab.flrig_port_edit = Edit("12345")
    tab.fldigi_host_edit = Edit("127.0.0.1")
    tab.fldigi_port_edit = Edit("7362")

    tab._refresh_running_status(force=False)

    assert probe.calls == 1
    assert label.tooltip.endswith("127.0.0.1:2242")
