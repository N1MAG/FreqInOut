"""Focused GRS-10/VNC-6 VarAC structured launch round-trip contracts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest
from PySide6.QtCore import QObject

from freqinout.core.launch_bundle_store import LaunchBundleStore
from freqinout.core.launch_orchestrator import LaunchOrchestrator
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.station_launch_planner import StationLaunchPlanner


def _profile(store: MultiRadioStore) -> dict[str, object]:
    return store.save_device_profile(
        {
            "system_key": "radio-a",
            "name": "Radio A",
            "runtime_active": 1,
            "display_order": 1,
            "launch_cmd": "legacy-varac --wrong-profile",
        }
    )


def _structured_item(*, executable: str, arguments: list[str], cwd: str, environment: dict[str, str]) -> dict[str, object]:
    return {
        "name": "VarAC",
        "instance_key": "varac:radio-a:varac",
        "enabled": True,
        "startup": True,
        "monitor_health": True,
        # This stale value must never win over the structured recipe.
        "launch_command_override": "legacy-varac --wrong-profile",
        "launch_path_override": "legacy-varac-directory",
        "dependencies": [],
        "readiness_policy": {
            "structured_launch": True,
            "executable": executable,
            "launch_arguments": arguments,
            "working_directory": cwd,
            "environment": environment,
            "readiness": "process",
            "window_title": "VarAC — Radio A",
        },
    }


@pytest.mark.parametrize(
    ("executable", "arguments", "cwd", "environment"),
    [
        (
            r"C:\Program Files\VarAC\VarAC.exe",
            [r"C:\Program Files\VarAC\VarAC-Radio A.ini", "--profile", "Radio A"],
            r"C:\Program Files\VarAC",
            {},
        ),
        (
            "wine",
            ["/home/bill/VarAC Station/VarAC.exe", r"C:\VarAC\VarAC-Radio A.ini"],
            "/home/bill/VarAC Station",
            {"WINEPREFIX": "/home/bill/.wine-radio-a"},
        ),
    ],
)
def test_varac_structured_recipe_store_reload_planner_and_orchestrator_are_byte_exact(
    tmp_path, executable, arguments, cwd, environment
) -> None:
    db_path = tmp_path / "freqinout.db"
    store = MultiRadioStore(db_path)
    radio = _profile(store)
    item = _structured_item(
        executable=executable,
        arguments=arguments,
        cwd=cwd,
        environment=environment,
    )

    LaunchBundleStore(db_path).save_bundle(radio["id"], True, [item])
    reopened = LaunchBundleStore(db_path).get_bundle(radio["id"])
    saved_item = reopened["items"][0]
    assert saved_item["launch_command_override"] == "legacy-varac --wrong-profile"
    assert saved_item["readiness_policy"]["executable"] == executable
    assert saved_item["readiness_policy"]["launch_arguments"] == arguments
    assert saved_item["readiness_policy"]["working_directory"] == cwd
    assert saved_item["readiness_policy"]["environment"] == environment
    assert saved_item["readiness_policy"]["window_title"] == "VarAC — Radio A"

    profile = store.get_device_profile(radio["id"])
    planned = StationLaunchPlanner().plan_startup([profile], {radio["id"]: reopened}).instances[0]
    assert planned.launch_command_override == ""
    assert planned.launch_path_override == executable
    assert planned.launch_arguments == tuple(arguments)
    assert planned.working_directory == cwd
    assert dict(planned.environment) == environment
    assert dict(planned.readiness_policy)["window_title"] == "VarAC — Radio A"

    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    command, description = orchestrator._resolve_launch_command(planned.as_queue_item())
    assert description == "structured VarAC launch recipe"
    assert command == [executable, *arguments]


def test_varac_qualified_managed_component_uses_existing_readiness_json_seam(tmp_path) -> None:
    db_path = tmp_path / "freqinout.db"
    store = MultiRadioStore(db_path)
    radio = _profile(store)
    recipe = {
        "status": "qualified_managed",
        "components": [
            {
                "component_key": "varac",
                "executable": "wine",
                "arguments": ["/home/bill/VarAC Station/VarAC.exe", r"C:\VarAC\VarAC-Radio A.ini"],
                "working_directory": "/home/bill/VarAC Station",
                "environment": {"WINEPREFIX": "/home/bill/.wine-radio-a"},
                "dependencies": ["VARA"],
                "readiness": {"kind": "process"},
            }
        ],
    }
    saved = store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=radio["id"],
        application_values={
            "system_key": "varac-a",
            "name": "VarAC A",
            "install_path": "/home/bill/VarAC Station",
            "ini_path": "/home/bill/VarAC Station/VarAC-Radio A.ini",
            "db_path": "/tmp/varac-a.db",
            "incoming_path": "/tmp/varac-a-incoming",
            "outbox_path": "/tmp/varac-a-outbox",
            "native_management_state": "managed",
        },
        manifest_values={
            "instance_key": "varac:varac-a",
            "launch_command": "legacy-varac --wrong-profile",
            "evidence": {"launch_recipe": recipe},
        },
        launch_at_startup=True,
    )
    assert saved["radio"]["varac_node_id"]

    with store.connect_readonly() as conn:
        row = conn.execute(
            "SELECT command_override, path_override, dependencies_json, readiness_json "
            "FROM radio_launch_bundle_items WHERE radio_profile_id=?",
            (radio["id"],),
        ).fetchone()
    assert row[0] == ""
    assert row[1] == "wine"
    assert json.loads(row[2]) == ["VARA"]
    readiness = json.loads(row[3])
    assert readiness["structured_launch"] is True
    assert readiness["executable"] == "wine"
    assert readiness["launch_arguments"] == recipe["components"][0]["arguments"]
    assert readiness["working_directory"] == "/home/bill/VarAC Station"
    assert readiness["environment"] == {"WINEPREFIX": "/home/bill/.wine-radio-a"}


def test_process_runner_receives_structured_varac_argv_cwd_and_environment_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import freqinout.core.launch_orchestrator as launch_module

    executable = "wine"
    arguments = ["/home/bill/VarAC Station/VarAC.exe", r"C:\VarAC\VarAC-Radio A.ini"]
    cwd = "/home/bill/VarAC Station"
    environment = {"WINEPREFIX": "/home/bill/.wine-radio-a"}
    stored_item = _structured_item(
        executable=executable,
        arguments=arguments,
        cwd=cwd,
        environment=environment,
    )
    planned = StationLaunchPlanner().plan_startup(
        [{"id": 1, "name": "Radio A", "runtime_active": 1, "display_order": 1}],
        {1: {"launch_enabled": True, "items": [stored_item]}},
    ).instances[0]
    queue_item = planned.as_queue_item()
    captured: dict[str, object] = {}

    def fake_popen(command, **kwargs):
        captured["command"] = list(command)
        captured.update(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(launch_module.subprocess, "Popen", fake_popen)
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    QObject.__init__(orchestrator)
    orchestrator._active = True
    orchestrator._cancel_requested = False
    orchestrator._queue = [queue_item]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator._current_name = None
    orchestrator._current_item = None
    orchestrator._current_cmd = None
    orchestrator._current_started_monotonic = 0.0
    orchestrator._blocked_dependency_for = lambda _item: None
    orchestrator._program_running = lambda _item: False
    orchestrator._is_self_launch_command = lambda _cmd: False
    orchestrator._schedule_advance_queue = lambda _delay=0: None
    orchestrator.dependency_status = SimpleNamespace(refresh_now=lambda **_kwargs: None)
    orchestrator._poll_timer = SimpleNamespace(setInterval=lambda _value: None, start=lambda: None)

    orchestrator._advance_queue()

    assert captured["command"] == [executable, *arguments]
    assert captured["shell"] is False
    assert captured["cwd"] == cwd
    assert captured["env"]["WINEPREFIX"] == environment["WINEPREFIX"]


def test_varac_window_title_is_pid_scoped_retried_and_never_blocks_launch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import freqinout.core.launch_orchestrator as launch_module

    scheduled: list[tuple[int, object]] = []
    attempts: list[tuple[int, str]] = []
    outcomes = iter((False, True))
    monkeypatch.setattr(
        launch_module,
        "QTimer",
        SimpleNamespace(
            singleShot=lambda delay, callback: scheduled.append((delay, callback))
        ),
    )
    monkeypatch.setattr(
        launch_module,
        "set_process_window_title",
        lambda pid, title: attempts.append((pid, title)) or next(outcomes),
    )
    item = _structured_item(
        executable="wine",
        arguments=["/opt/VarAC/VarAC.exe", r"C:\VarAC\VarAC-Radio A.ini"],
        cwd="/opt/VarAC",
        environment={"WINEPREFIX": "/home/bill/.wine"},
    )
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)

    orchestrator._schedule_process_window_title(item, SimpleNamespace(pid=4321))

    assert scheduled[0][0] == 250
    scheduled.pop(0)[1]()
    assert attempts == [(4321, "VarAC — Radio A")]
    assert scheduled[0][0] == 750
    scheduled.pop(0)[1]()
    assert attempts == [
        (4321, "VarAC — Radio A"),
        (4321, "VarAC — Radio A"),
    ]


def test_existing_structured_varac_row_derives_title_from_one_radio_context() -> None:
    assert LaunchOrchestrator._window_title_for_item(
        {
            "name": "VarAC",
            "radio_names": ["FT-710"],
            "readiness_policy": {"structured_launch": True},
        }
    ) == "VarAC — FT-710"
    assert LaunchOrchestrator._window_title_for_item(
        {
            "name": "VarAC",
            "radio_names": ["FT-710", "FTDX-10"],
            "readiness_policy": {"structured_launch": True},
        }
    ) == ""


def test_varac_legacy_launch_command_remains_compatibility_fallback() -> None:
    profile = {
        "id": 1,
        "name": "Radio A",
        "runtime_active": 1,
        "display_order": 1,
        "launch_cmd": 'wine "/opt/VarAC A/VarAC.exe" --profile "Node A"',
    }
    item = {
        "name": "VarAC",
        "instance_key": "varac:legacy",
        "enabled": True,
        "startup": True,
        "monitor_health": True,
        "launch_command_override": "",
        "launch_path_override": "",
        "dependencies": [],
        "readiness_policy": {},
    }
    planned = StationLaunchPlanner().plan_startup(
        [profile], {1: {"launch_enabled": True, "items": [item]}}
    ).instances[0]
    assert planned.launch_command_override == profile["launch_cmd"]

    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    command, description = orchestrator._resolve_launch_command(planned.as_queue_item())
    assert description == "radio launch command"
    assert command == ["wine", "/opt/VarAC A/VarAC.exe", "--profile", "Node A"]


def test_production_database_copy_migrates_and_round_trips_structured_varac_launch(
    tmp_path: Path,
) -> None:
    source = Path("/Users/bill/RadioTools/FIO_DB_prod/current/freqinout.db")
    if not source.is_file():
        pytest.skip(f"production-shaped database not present: {source}")
    before = (
        source.stat().st_size,
        source.stat().st_mtime_ns,
        hashlib.sha256(source.read_bytes()).hexdigest(),
    )
    copied = tmp_path / "freqinout.db"
    shutil.copy2(source, copied)
    store = MultiRadioStore(copied)
    radio = store.save_device_profile(
        {
            "system_key": "grs10-copy-radio",
            "name": "GRS10 Copy Radio",
            "runtime_active": 1,
            "display_order": 99,
        }
    )
    arguments = ["/home/bill/VarAC Station/VarAC.exe", r"C:\VarAC\VarAC-GRS10.ini"]
    store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=radio["id"],
        application_values={
            "system_key": "grs10-copy-varac",
            "name": "GRS10 Copy VarAC",
            "install_path": "/home/bill/VarAC Station/VarAC.exe",
            "ini_path": "/home/bill/VarAC Station/VarAC-GRS10.ini",
            "db_path": "/tmp/grs10-copy-varac.db",
            "incoming_path": "/tmp/grs10-copy-incoming",
            "outbox_path": "/tmp/grs10-copy-outbox",
            "native_management_state": "managed",
        },
        manifest_values={
            "instance_key": "varac:grs10-copy-varac",
            "evidence": {
                "launch_recipe": {
                    "status": "qualified_managed",
                    "components": [
                        {
                            "component_key": "varac",
                            "executable": "wine",
                            "arguments": arguments,
                            "working_directory": "/home/bill/VarAC Station",
                            "environment": {"WINEPREFIX": "/home/bill/.wine-grs10"},
                            "dependencies": [],
                            "readiness": {"kind": "process"},
                        }
                    ],
                }
            },
        },
        launch_at_startup=True,
    )
    reopened = LaunchBundleStore(copied).get_bundle(radio["id"])
    reloaded_profile = MultiRadioStore(copied).get_device_profile(radio["id"])
    planned = StationLaunchPlanner().plan_startup(
        [reloaded_profile],
        {radio["id"]: reopened},
    ).instances[0]
    assert planned.launch_path_override == "wine"
    assert planned.launch_arguments == tuple(arguments)
    assert planned.working_directory == "/home/bill/VarAC Station"
    assert dict(planned.environment) == {"WINEPREFIX": "/home/bill/.wine-grs10"}
    after = (
        source.stat().st_size,
        source.stat().st_mtime_ns,
        hashlib.sha256(source.read_bytes()).hexdigest(),
    )
    assert after == before
