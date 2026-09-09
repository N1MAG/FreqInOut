"""Qt-free Slice 4 contract tests for radio launch bundles and planning.

The launch planner is deliberately exercised with plain dictionaries so the
ownership, migration, deduplication, and scope rules stay testable without
constructing Settings or Qt widgets.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from freqinout.core.config_backup import ConfigBackupItem, ConfigBackupResult
from freqinout.core.launch_bundle_store import LaunchBundleStore
from freqinout.core.station_launch_planner import LaunchPlan, PlannedInstance, StationLaunchPlanner


def _item(
    name: str,
    *,
    startup: bool = True,
    enabled: bool = True,
    instance_key: str | None = None,
    path: str = "",
    command: str = "",
    api_port: int | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "name": name,
        "enabled": enabled,
        "startup": startup,
        "monitor_health": True,
        "dependencies": [],
        "readiness_policy": {"readiness": "process"},
    }
    if instance_key:
        row["instance_key"] = instance_key
    if path:
        row["launch_path_override"] = path
    if command:
        row["launch_command_override"] = command
    if api_port is not None:
        row["api_port"] = api_port
    return row


def _bundle_items(bundle: object) -> list[dict[str, object]]:
    if isinstance(bundle, dict):
        rows = bundle.get("items", [])
    else:
        rows = getattr(bundle, "items", [])
    return [dict(row) for row in rows]


def _bundle_enabled(bundle: object) -> bool:
    if isinstance(bundle, dict):
        return bool(bundle.get("launch_enabled", False))
    return bool(getattr(bundle, "launch_enabled", False))


def _audit_rows(store: LaunchBundleStore) -> list[dict[str, object]]:
    rows = store.list_migration_audit()
    return [dict(row) for row in rows]


def _seed_legacy_kv(db_path: Path, values: dict[str, object]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
        conn.executemany(
            "INSERT OR REPLACE INTO kv(key, value) VALUES (?, ?)",
            [(key, json.dumps(value)) for key, value in values.items()],
        )


def _seed_radios(db_path: Path) -> None:
    """Create the minimal device-profile parent rows required by the FK."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_profiles (
                id INTEGER PRIMARY KEY,
                system_key TEXT,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                runtime_active INTEGER NOT NULL DEFAULT 0,
                runtime_primary INTEGER NOT NULL DEFAULT 0,
                display_order INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.executemany(
            """
            INSERT OR REPLACE INTO device_profiles
                (id, system_key, name, enabled, runtime_active, runtime_primary, display_order)
            VALUES (?, ?, ?, 1, 1, ?, ?)
            """,
            [(1, "radio_a", "Alpha", 1, 0), (2, "radio_b", "Bravo", 0, 1)],
        )


def test_radio_bundle_roundtrip_preserves_order_and_isolation(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    _seed_radios(db_path)
    store = LaunchBundleStore(db_path)
    radio_a = [
        _item("JS8Call", path="/apps/a/js8call", instance_key="js8:a"),
        _item("CommStat", path="/apps/a/commstat", startup=False),
    ]
    radio_b = [
        _item("CommStat", path="/apps/b/commstat"),
        _item("JS8Call", path="/apps/b/js8call", startup=False),
    ]

    store.save_bundle(1, True, radio_a)
    store.save_bundle(2, False, radio_b)

    reopened = LaunchBundleStore(db_path)
    saved_a = reopened.get_bundle(1)
    saved_b = reopened.get_bundle(2)

    assert _bundle_enabled(saved_a) is True
    assert [row["name"] for row in _bundle_items(saved_a)] == ["JS8Call", "CommStat"]
    assert _bundle_items(saved_a)[0]["launch_path_override"] == "/apps/a/js8call"
    assert _bundle_enabled(saved_b) is False
    assert [row["name"] for row in _bundle_items(saved_b)] == ["CommStat", "JS8Call"]
    assert _bundle_items(saved_b)[0]["launch_path_override"] == "/apps/b/commstat"

    # Mutating/re-saving one radio must not alter the other radio's bundle.
    store.save_bundle(1, False, [_item("CommStat", path="/apps/a/new-commstat")])
    isolated_b = LaunchBundleStore(db_path).get_bundle(2)
    assert _bundle_enabled(isolated_b) is False
    assert [row["name"] for row in _bundle_items(isolated_b)] == ["CommStat", "JS8Call"]


def test_legacy_migration_is_additive_idempotent_and_leaves_kv_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    _seed_radios(db_path)
    legacy_items = [
        _item("JS8Call", path="/legacy/js8call", instance_key="legacy-js8"),
        _item("CommStat", startup=False, command="/legacy/commstat"),
    ]
    legacy_values: dict[str, object] = {
        "launch_control_items": legacy_items,
        "launch_control_enabled": True,
        "autostart_js8call": True,
        "custom_tool_items": [{"name": "Field Helper", "command": "/legacy/helper"}],
    }
    _seed_legacy_kv(db_path, legacy_values)

    store = LaunchBundleStore(db_path)
    before = {
        key: json.dumps(value, sort_keys=True)
        for key, value in legacy_values.items()
    }
    backup = ConfigBackupResult(
        backup_dir=str(tmp_path / "backup"),
        reason="launch-bundle-v1",
        created_at="20260908-000000",
        items=(
            ConfigBackupItem(
                original_path=str(db_path),
                backup_path=str(tmp_path / "backup" / "freqinout.db"),
                kind="file",
                status="backed_up",
            ),
        ),
        manifest_path=str(tmp_path / "backup" / "manifest.json"),
    )
    first = store.migrate_legacy(
        legacy_values,
        backup_factory=lambda *_args, **_kwargs: backup,
    )
    migrated = store.get_bundle(1)
    assert _bundle_enabled(migrated) is True
    migrated_rows = _bundle_items(migrated)
    migrated_names = [row["name"] for row in migrated_rows]
    assert migrated_names[:2] == ["JS8Call", "CommStat"]
    assert {
        "FLRig",
        "FLDigi",
        "FLAmp",
        "FLMsg",
        "VarAC",
        "JS8Call",
        "JS8Spotter",
        "CommStat",
        "Field Helper",
    } == set(migrated_names)
    assert _bundle_items(migrated)[0]["launch_path_override"] == "/legacy/js8call"
    helper = next(row for row in migrated_rows if row["name"] == "Field Helper")
    assert helper["launch_command_override"] == "/legacy/helper"
    assert helper["monitor_health"] is False
    assert first is not None
    assert store.get_bundle(2, legacy_items=legacy_items)["legacy_fallback"] is False
    assert store.get_bundle(2, legacy_items=legacy_items)["items"] == []

    # The source kv payload remains available as a compatibility fallback.
    with sqlite3.connect(db_path) as conn:
        rows = dict(conn.execute("SELECT key, value FROM kv"))
    for key, expected in before.items():
        assert key in rows
        assert json.dumps(json.loads(rows[key]), sort_keys=True) == expected

    audit_after_first = _audit_rows(store)
    store.migrate_legacy(legacy_values, backup_factory=lambda *_args, **_kwargs: backup, dry_run=False)
    audit_after_second = _audit_rows(LaunchBundleStore(db_path))
    assert audit_after_second == audit_after_first


def test_legacy_migration_imports_flags_and_custom_tools_without_item_list(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    _seed_radios(db_path)
    settings_values: dict[str, object] = {
        "launch_control_enabled": True,
        "autostart_flrig": True,
        "autostart_fldigi": False,
        "autostart_flamp": True,
        "autostart_flmsg": False,
        "autostart_js8call": True,
        "custom_tool_items": [{"name": "Beacon Helper", "command": "/tools/beacon-helper"}],
    }
    _seed_legacy_kv(db_path, settings_values)
    backup = ConfigBackupResult(
        backup_dir=str(tmp_path / "backup"),
        reason="launch-bundle-v1",
        created_at="20260908-000000",
        items=(
            ConfigBackupItem(
                original_path=str(db_path),
                backup_path=str(tmp_path / "backup" / "freqinout.db"),
                kind="file",
                status="backed_up",
            ),
        ),
        manifest_path=str(tmp_path / "backup" / "manifest.json"),
    )

    store = LaunchBundleStore(db_path)
    store.migrate_legacy(settings_values, backup_factory=lambda *_args, **_kwargs: backup)
    rows = _bundle_items(store.get_bundle(1))
    by_name = {str(row["name"]): row for row in rows}

    assert [row["name"] for row in rows[:-1]] == [
        "FLRig",
        "FLDigi",
        "FLAmp",
        "FLMsg",
        "VarAC",
        "JS8Call",
        "JS8Spotter",
        "CommStat",
    ]
    assert by_name["FLRig"]["startup"] is True
    assert by_name["FLDigi"]["startup"] is False
    assert by_name["FLAmp"]["startup"] is True
    assert by_name["JS8Call"]["startup"] is True
    assert by_name["Beacon Helper"]["launch_command_override"] == "/tools/beacon-helper"
    assert by_name["Beacon Helper"]["monitor_health"] is False


def test_legacy_dry_run_does_not_create_bundle_or_audit(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    _seed_radios(db_path)
    store = LaunchBundleStore(db_path)
    values = {
        "launch_control_items": [_item("JS8Call", path="/legacy/js8call")],
        "launch_control_enabled": True,
    }

    result = store.migrate_legacy(values, dry_run=True)

    assert result is not None
    assert _bundle_items(store.get_bundle(1, legacy_items=None)) == []
    assert _audit_rows(store) == []


def test_legacy_migration_backup_failure_changes_nothing(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    _seed_radios(db_path)
    store = LaunchBundleStore(db_path)
    failed = ConfigBackupResult(
        backup_dir=str(tmp_path / "backup"),
        reason="launch-bundle-v1",
        created_at="20260908-000000",
        items=(
            ConfigBackupItem(
                original_path=str(db_path),
                backup_path="",
                kind="file",
                status="failed",
                error="simulated failure",
            ),
        ),
        manifest_path=str(tmp_path / "backup" / "manifest.json"),
    )

    with pytest.raises(RuntimeError, match="backup did not complete"):
        store.migrate_legacy(
            {"launch_control_enabled": True, "autostart_js8call": True},
            backup_factory=lambda *_args, **_kwargs: failed,
        )

    assert store.get_bundle(1, legacy_items=None)["items"] == []
    assert store.list_migration_audit() == []


def test_startup_planner_deduplicates_exact_shared_identity() -> None:
    profiles = [
        {"id": 1, "name": "Alpha", "runtime_active": 1, "launch_enabled": True},
        {"id": 2, "name": "Bravo", "runtime_active": 1, "launch_enabled": True},
    ]
    shared_a = _item("CommStat", path="/apps/shared/commstat")
    shared_b = _item("CommStat", path="/apps/shared/commstat")
    planner = StationLaunchPlanner()

    plan = planner.plan_startup(
        profiles,
        {1: {"launch_enabled": True, "items": [shared_a]}, 2: {"launch_enabled": True, "items": [shared_b]}},
    )

    assert len(plan.instances) == 1
    instance = plan.instances[0]
    assert instance.name == "CommStat"
    assert set(instance.radio_ids) == {1, 2}
    assert set(instance.radio_names) == {"Alpha", "Bravo"}
    assert instance.instance_identity
    assert instance.launch_path_override == "/apps/shared/commstat"


def test_startup_planner_keeps_distinct_js8_ports_as_separate_instances() -> None:
    profiles = [
        {"id": 1, "name": "Alpha", "runtime_active": 1, "launch_enabled": True, "js8_port": 2442},
        {"id": 2, "name": "Bravo", "runtime_active": 1, "launch_enabled": True, "js8_port": 2443},
    ]
    planner = StationLaunchPlanner()
    plan = planner.plan_startup(
        profiles,
        {
            1: {"launch_enabled": True, "items": [_item("JS8Call", path="/apps/js8call")]},
            2: {"launch_enabled": True, "items": [_item("JS8Call", path="/apps/js8call")]},
        },
    )

    assert len(plan.instances) == 2
    assert len({instance.instance_identity for instance in plan.instances}) == 2
    assert {instance.launch_path_override for instance in plan.instances} == {"/apps/js8call"}
    assert all(len(instance.radio_ids) == 1 for instance in plan.instances)


def test_startup_planner_keeps_distinct_flrig_endpoints_and_varac_commands() -> None:
    profiles = [
        {"id": 1, "name": "Alpha", "runtime_active": 1, "flrig_port": 12345},
        {"id": 2, "name": "Bravo", "runtime_active": 1, "flrig_port": 12346},
    ]
    planner = StationLaunchPlanner()
    flrig_plan = planner.plan_startup(
        profiles,
        {
            1: {"launch_enabled": True, "items": [_item("FLRig", path="/apps/flrig")]},
            2: {"launch_enabled": True, "items": [_item("FLRig", path="/apps/flrig")]},
        },
    )
    varac_plan = planner.plan_startup(
        profiles,
        {
            1: {"launch_enabled": True, "items": [_item("VarAC", command="wine /a/VarAC.exe")]},
            2: {"launch_enabled": True, "items": [_item("VarAC", command="wine /b/VarAC.exe")]},
        },
    )

    assert len(flrig_plan.instances) == 2
    assert len({instance.instance_identity for instance in flrig_plan.instances}) == 2
    assert len(varac_plan.instances) == 2
    assert len({instance.instance_identity for instance in varac_plan.instances}) == 2


def test_startup_planner_scope_limits_to_selected_radio() -> None:
    profiles = [
        {"id": 1, "name": "Alpha", "runtime_active": 1, "launch_enabled": True},
        {"id": 2, "name": "Bravo", "runtime_active": 1, "launch_enabled": True},
    ]
    bundles = {
        1: {"launch_enabled": True, "items": [_item("CommStat", path="/apps/a/commstat")]},
        2: {"launch_enabled": True, "items": [_item("JS8Call", path="/apps/b/js8call")]},
    }

    plan = StationLaunchPlanner().plan_startup(profiles, bundles, scope_radio_id=2)

    assert [instance.name for instance in plan.instances] == ["JS8Call"]
    assert plan.instances[0].radio_ids == (2,)
    assert plan.instances[0].radio_names == ("Bravo",)


def test_startup_planner_rejects_dependency_cycles() -> None:
    profiles = [{"id": 1, "name": "Alpha", "runtime_active": 1}]
    items = [
        {**_item("One"), "dependencies": ["Two"]},
        {**_item("Two"), "dependencies": ["One"]},
    ]

    with pytest.raises(ValueError, match="dependency cycle"):
        StationLaunchPlanner().plan_startup(
            profiles,
            {1: {"launch_enabled": True, "items": items}},
        )


def test_orchestrator_executes_the_exact_preview_queue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    from freqinout.core.launch_orchestrator import LaunchOrchestrator
    from freqinout.core.settings_manager import SettingsManager

    planned = PlannedInstance(
        name="CommStat",
        instance_key="CommStat",
        instance_identity="shared-commstat",
        radio_ids=(1, 2),
        radio_names=("Alpha", "Bravo"),
        launch_path_override="/apps/commstat",
    )
    plan = LaunchPlan(trigger="startup", scope_radio_id=None, instances=(planned,))
    orchestrator = LaunchOrchestrator(SettingsManager())
    captured: dict[str, object] = {}
    monkeypatch.setattr(orchestrator, "preview_startup_plan", lambda **_kwargs: plan)
    monkeypatch.setattr(
        orchestrator,
        "_start_sequence",
        lambda trigger, queue: captured.update(trigger=trigger, queue=queue) or True,
    )

    assert orchestrator.start_startup_sequence() is True
    assert captured == {"trigger": "startup", "queue": plan.queue()}


def test_executor_does_not_use_family_status_for_distinct_instance() -> None:
    from freqinout.core.launch_orchestrator import LaunchOrchestrator

    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator.status = type(
        "CachedStatus",
        (),
        {"cached_program_instance_running": lambda _self, _name, target: target == "/apps/alpha/flrig"},
    )()
    orchestrator._cached_status_for_item = lambda _item: {"running": True}

    assert orchestrator._program_running(
        {
            "name": "FLRig",
            "instance_identity": "alpha",
            "launch_path_override": "/apps/alpha/flrig",
        }
    ) is True
    assert orchestrator._program_running(
        {
            "name": "FLRig",
            "instance_identity": "bravo",
            "launch_path_override": "/apps/bravo/flrig",
        }
    ) is False


def test_executor_requires_dependency_success_for_every_shared_radio() -> None:
    from freqinout.core.launch_orchestrator import LaunchOrchestrator

    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._queue = [
        {"name": "JS8Call", "radio_ids": [1]},
        {"name": "JS8Call", "radio_ids": [2]},
        {"name": "CommStat", "radio_ids": [1, 2], "dependencies": ["JS8Call"]},
    ]
    orchestrator._results = [
        {"name": "JS8Call", "radio_ids": [1], "status": "launched"},
        {"name": "JS8Call", "radio_ids": [2], "status": "timeout"},
    ]

    assert orchestrator._blocked_dependency_for(orchestrator._queue[-1]) == "JS8Call"
