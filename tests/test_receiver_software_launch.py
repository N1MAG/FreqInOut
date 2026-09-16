"""Qt-free contract tests for observer-safe receiver application launch."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from freqinout.core.config_autodiscovery import find_app_candidates
from freqinout.core.launch_bundle_store import LaunchBundleStore
from freqinout.core.launch_orchestrator import LAUNCH_APP_ORDER, LaunchOrchestrator
from freqinout.core.receiver_software_stack import (
    RECEIVE_ONLY_EXECUTION_SCOPE,
    build_receiver_launch_items,
)
from freqinout.core.software_status_service import PROGRAM_TOKENS, SoftwareStatusService
from freqinout.core.station_launch_planner import StationLaunchPlanner


def _observer_profile(*, radio_id: int = 7) -> dict[str, object]:
    return {
        "id": radio_id,
        "name": "RTL-SDR",
        "device_class": "observer",
        "runtime_active": 1,
        "display_order": 0,
        "sdr_application": "SDR++",
    }


def _seed_radio(db_path: Path, radio_id: int = 7) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE device_profiles (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                runtime_active INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            "INSERT INTO device_profiles (id, name, runtime_active) VALUES (?, ?, 1)",
            (radio_id, "RTL-SDR"),
        )


def _make_executable(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | 0o111)


def test_receiver_stack_builds_a_durable_sdrpp_launch_item() -> None:
    items = build_receiver_launch_items(
        _observer_profile(),
        [{"application": "SDR++", "launch_path": "/opt/sdrpp/sdrpp", "launch_at_startup": True}],
    )

    assert items == [
        {
            "name": "SDR++",
            "instance_key": "receiver:sdrpp",
            "enabled": True,
            "startup": True,
            "monitor_health": True,
            "launch_path_override": "/opt/sdrpp/sdrpp",
            "launch_command_override": "",
            "dependencies": [],
            "readiness_policy": {
                "readiness": "process",
                "execution_scope": RECEIVE_ONLY_EXECUTION_SCOPE,
            },
            "execution_scope": RECEIVE_ONLY_EXECUTION_SCOPE,
        }
    ]
    assert "SDR++" not in LAUNCH_APP_ORDER


@pytest.mark.parametrize(
    ("profile", "selection", "match"),
    [
        ({"device_class": "tx_rx"}, {"application": "SDR++", "launch_path": "/opt/sdrpp/sdrpp"}, "observer"),
        (_observer_profile(), {"application": "JS8Call", "launch_path": "/opt/js8call/js8call"}, "Unsupported"),
        (_observer_profile(), {"application": "SDR++", "launch_at_startup": True}, "needs an application path"),
        (
            _observer_profile(),
            {"application": "SDR++", "launch_path": "/usr/bin/python3", "launch_at_startup": True},
            "not another executable",
        ),
    ],
)
def test_receiver_stack_rejects_unsafe_or_incomplete_selection(
    profile: dict[str, object], selection: dict[str, object], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        build_receiver_launch_items(profile, [selection])


@pytest.mark.parametrize(
    "target",
    (
        "sdrpp",
        "/usr/bin/sdrpp",
        "/Applications/SDR++.app",
        r"C:\Program Files\SDR++\sdrpp.exe",
        "open -a SDR++",
    ),
)
def test_receiver_stack_accepts_reviewed_cross_platform_sdrpp_targets(target: str) -> None:
    items = build_receiver_launch_items(
        _observer_profile(),
        [{"application": "SDR++", "launch_path": target, "launch_at_startup": True}],
    )
    assert items[0]["launch_path_override"] == target


def test_receiver_bundle_roundtrip_and_planner_keep_receive_only_scope(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    _seed_radio(db_path)
    store = LaunchBundleStore(db_path)
    profile = _observer_profile()
    created = build_receiver_launch_items(
        profile,
        [{"application": "SDR++", "launch_command": "sdrpp --server", "startup": True}],
    )

    store.save_bundle(7, True, created)
    saved = LaunchBundleStore(db_path).get_bundle(7)
    persisted = saved["items"][0]
    assert persisted["execution_scope"] == RECEIVE_ONLY_EXECUTION_SCOPE
    assert persisted["readiness_policy"]["execution_scope"] == RECEIVE_ONLY_EXECUTION_SCOPE

    plan = StationLaunchPlanner().plan_startup([profile], {7: saved})
    assert [instance.name for instance in plan.instances] == ["SDR++"]
    assert plan.instances[0].execution_scope == RECEIVE_ONLY_EXECUTION_SCOPE
    assert plan.queue()[0]["execution_scope"] == RECEIVE_ONLY_EXECUTION_SCOPE


def test_observer_launch_plan_rejects_conventional_application() -> None:
    with pytest.raises(ValueError, match="not approved for a receive-only SDR launch stack"):
        StationLaunchPlanner().plan_startup(
            [_observer_profile()],
            {
                7: {
                    "launch_enabled": True,
                    "items": [
                        {
                            "name": "JS8Call",
                            "instance_key": "js8:unsafe",
                            "enabled": True,
                            "startup": True,
                            "launch_path_override": "/opt/js8call/js8call",
                        }
                    ],
                }
            },
        )


def test_transceiver_launch_plan_retains_existing_standard_behavior() -> None:
    profile = {
        "id": 8,
        "name": "HF Radio",
        "device_class": "tx_rx",
        "runtime_active": 1,
        "display_order": 0,
    }
    plan = StationLaunchPlanner().plan_startup(
        [profile],
        {
            8: {
                "launch_enabled": True,
                "items": [
                    {
                        "name": "FLDigi",
                        "instance_key": "fldigi:hf-radio",
                        "enabled": True,
                        "startup": True,
                        "launch_path_override": "/opt/fldigi/fldigi",
                    }
                ],
            }
        },
    )

    assert [instance.name for instance in plan.instances] == ["FLDigi"]
    assert plan.instances[0].execution_scope == "standard"


def test_sdrpp_path_detection_launch_resolution_and_process_tokens(tmp_path: Path) -> None:
    executable = tmp_path / "sdrpp"
    _make_executable(executable)

    candidates = find_app_candidates(
        apps=("sdrpp",),
        platform="Linux",
        app_search_paths={"sdrpp": (executable,)},
    )
    assert [(candidate.app_id, candidate.path, candidate.executable) for candidate in candidates] == [
        ("sdrpp", str(executable), True)
    ]
    assert {"sdrpp", "sdrpp.exe"} <= {token.lower() for token in PROGRAM_TOKENS["SDR++"]}

    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    command, source = orchestrator._resolve_launch_command(
        {"name": "SDR++", "launch_path_override": str(executable)}
    )
    assert command == [str(executable)]
    assert source == "radio configured path"

    class _Status:
        def cached_program_instance_running(self, name: str, target: str) -> bool:
            return name == "SDR++" and target == str(executable)

    class _DependencyStatus:
        def status_snapshot(self, **_kwargs: object) -> dict[str, object]:
            return {}

    orchestrator.status = _Status()
    orchestrator.dependency_status = _DependencyStatus()
    assert orchestrator._program_ready_for_sequence(
        {
            "name": "SDR++",
            "instance_identity": "receiver-sdrpp",
            "launch_path_override": str(executable),
            "readiness_policy": {"readiness": "process"},
        }
    )


def test_sdrpp_path_resolved_command_uses_cached_program_tokens() -> None:
    previous_snapshot = SoftwareStatusService._shared_proc_snapshot
    previous_records = SoftwareStatusService._shared_proc_records
    previous_timestamp = SoftwareStatusService._shared_proc_snapshot_ts
    try:
        SoftwareStatusService._shared_proc_snapshot = {"sdrpp"}
        SoftwareStatusService._shared_proc_records = []
        SoftwareStatusService._shared_proc_snapshot_ts = 1.0
        service = SoftwareStatusService.__new__(SoftwareStatusService)
        service.settings = None

        assert service.cached_program_instance_running("SDR++", "sdrpp") is True
        assert service.cached_program_instance_running("SDR++", "open -a SDR++") is True
    finally:
        SoftwareStatusService._shared_proc_snapshot = previous_snapshot
        SoftwareStatusService._shared_proc_records = previous_records
        SoftwareStatusService._shared_proc_snapshot_ts = previous_timestamp
