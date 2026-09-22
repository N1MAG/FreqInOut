"""Qt-free Slice 4 contract tests for radio launch bundles and planning.

The launch planner is deliberately exercised with plain dictionaries so the
ownership, migration, deduplication, and scope rules stay testable without
constructing Settings or Qt widgets.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from freqinout.core.config_backup import ConfigBackupItem, ConfigBackupResult
from freqinout.core.launch_bundle_store import LaunchBundleStore, normalize_launch_items
from freqinout.core.station_launch_planner import LaunchPlan, PlannedInstance, StationLaunchPlanner
from freqinout.core.launch_orchestrator import LaunchOrchestrator
from freqinout.core import launch_orchestrator as launch_module
from freqinout.core import js8_storage as js8_storage_module


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


def test_bundle_reads_do_not_repeat_schema_or_journal_initialization(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "settings.db"
    _seed_radios(db_path)
    store = LaunchBundleStore(db_path)
    store.save_bundle(1, True, [_item("FLRig")])

    monkeypatch.setattr(
        store,
        "_ensure_schema",
        lambda _conn: (_ for _ in ()).throw(AssertionError("read repeated schema initialization")),
    )

    bundle = store.get_bundle(1)

    assert _bundle_enabled(bundle) is True
    assert [row["name"] for row in _bundle_items(bundle)] == ["FLRig"]


@pytest.mark.parametrize(
    "catalog_action,custom_tools",
    [
        ("add", [{"name": "Tool A", "command": "/tools/a"}, {"name": "Tool B", "command": "/tools/b"}]),
        ("edit", [{"name": "Tool A", "command": "/tools/a-edited"}, {"name": "Tool B", "command": "/tools/b"}]),
        ("reorder", [{"name": "Tool B", "command": "/tools/b"}, {"name": "Tool A", "command": "/tools/a"}]),
    ],
)
def test_custom_tool_catalog_reconciliation_preserves_all_existing_launch_row_fields(
    catalog_action: str,
    custom_tools: list[dict[str, str]],
) -> None:
    """Adding/editing/reordering definitions must not rebuild existing recipes."""
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator.settings = SimpleNamespace(get=lambda _key, default=None: default)
    launch_rows = [
        {
            "name": "FLDigi",
            "instance_key": "fldigi:alpha-custom",
            "enabled": True,
            "startup": False,
            "monitor_health": False,
            "dependencies": ["FLRig", "Tool A"],
            "readiness_policy": {"readiness": "api", "port": 7362},
            "launch_path_override": "/apps/alpha/fldigi",
            "launch_command_override": "python /scripts/alpha-fldigi.py --profile alpha",
        },
        {
            "name": "Tool A",
            "instance_key": "custom:tool-a-alpha",
            "enabled": True,
            "startup": True,
            "monitor_health": False,
            "dependencies": ["FLDigi"],
            "readiness_policy": {"readiness": "process", "timeout": 8},
            "launch_path_override": "/apps/alpha/tool-a",
            "launch_command_override": "python /scripts/alpha-a.py --radio alpha",
        },
        {
            "name": "Tool B",
            "instance_key": "custom:tool-b-alpha",
            "enabled": False,
            "startup": False,
            "monitor_health": True,
            "dependencies": ["Tool A"],
            "readiness_policy": {"readiness": "process", "timeout": 13},
            "launch_path_override": "/apps/alpha/tool-b",
            "launch_command_override": "python /scripts/alpha-b.py --radio alpha",
        },
        {
            "name": "VARA",
            "instance_key": "varac:alpha:vara",
            "enabled": True,
            "startup": False,
            "monitor_health": True,
            "dependencies": [],
            "readiness_policy": {
                "structured_launch": True,
                "launch_arguments": ["C:\\VARA\\VARA.exe"],
            },
            "launch_path_override": "/usr/bin/wine",
            "launch_command_override": "",
        },
    ]
    existing = [dict(item) for item in launch_rows]

    reconciled = orchestrator.build_default_items(existing, custom_tools=custom_tools)

    by_name = {item["name"]: item for item in reconciled}
    assert {"FLDigi", "Tool A", "Tool B", "VARA"}.issubset(by_name)
    for expected in launch_rows:
        assert by_name[expected["name"]] == expected, f"{catalog_action} changed {expected['name']}"


def test_custom_tool_rows_keep_distinct_commands_and_radio_checkbox_state(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    _seed_radios(db_path)
    store = LaunchBundleStore(db_path)
    alpha_items = [
        {
            **_item("Field Backup", startup=True, command="/scripts/alpha-backup"),
            "instance_key": "custom:field-backup",
            "monitor_health": False,
        },
        {
            **_item("Net Helper", startup=False, command="/scripts/alpha-net"),
            "instance_key": "custom:net-helper",
            "monitor_health": True,
        },
    ]
    bravo_items = [
        {
            **_item("Field Backup", startup=False, command="/scripts/bravo-backup"),
            "instance_key": "custom:field-backup",
            "monitor_health": True,
        },
        {
            **_item("Net Helper", startup=True, command="/scripts/bravo-net"),
            "instance_key": "custom:net-helper",
            "monitor_health": False,
        },
    ]

    store.save_bundle(1, True, alpha_items)
    store.save_bundle(2, False, bravo_items)
    reopened = LaunchBundleStore(db_path)
    alpha = reopened.get_bundle(1)
    bravo = reopened.get_bundle(2)

    assert _bundle_items(alpha) == normalize_launch_items(alpha_items)
    assert _bundle_items(bravo) == normalize_launch_items(bravo_items)

    plan = StationLaunchPlanner().plan_startup(
        [
            {"id": 1, "name": "Alpha", "runtime_active": 1},
            {"id": 2, "name": "Bravo", "runtime_active": 1},
        ],
        {
            1: {**alpha, "launch_enabled": True},
            2: {**bravo, "launch_enabled": True},
        },
    )
    assert {(item.name, item.launch_command_override, item.radio_names) for item in plan.instances} == {
        ("Field Backup", "/scripts/alpha-backup", ("Alpha",)),
        ("Net Helper", "/scripts/bravo-net", ("Bravo",)),
    }


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
        {"id": 1, "name": "Alpha", "runtime_active": 1, "launch_enabled": True, "js8_port": 2442, "js8_instance_system_key": "js8-alpha", "js8_instance_name": "JS8 Alpha"},
        {"id": 2, "name": "Bravo", "runtime_active": 1, "launch_enabled": True, "js8_port": 2443, "js8_instance_system_key": "js8-bravo", "js8_instance_name": "JS8 Bravo"},
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


def test_startup_planner_allows_multiple_local_subspace_instances_with_distinct_rig_names() -> None:
    profiles = [
        {"id": 1, "name": "Alpha", "runtime_active": 1, "js8_port": 2442, "js8_instance_system_key": "js8-alpha", "js8_instance_name": "JS8 Alpha"},
        {"id": 2, "name": "Bravo", "runtime_active": 1, "js8_port": 2443, "js8_instance_system_key": "js8-bravo", "js8_instance_name": "JS8 Bravo"},
    ]
    bundles = {
        1: {"launch_enabled": True, "items": [_item("JS8Call", path="/usr/bin/js8call-subspace")]},
        2: {"launch_enabled": True, "items": [_item("JS8Call", path="/usr/bin/js8call-subspace")]},
    }

    plan = StationLaunchPlanner().plan_startup(profiles, bundles)

    assert len(plan.instances) == 2
    assert {item.expected_storage_mode for item in plan.instances} == {"rig_scoped"}
    assert len({item.rig_name for item in plan.instances}) == 2
    assert len({item.application_data_root for item in plan.instances}) == 2


def test_startup_planner_allows_one_scoped_subspace_instance() -> None:
    profiles = [
        {"id": 1, "name": "Alpha", "runtime_active": 1, "js8_port": 2442, "js8_instance_system_key": "js8-alpha", "js8_instance_name": "JS8 Alpha"},
        {"id": 2, "name": "Bravo", "runtime_active": 1, "js8_port": 2443, "js8_instance_system_key": "js8-bravo", "js8_instance_name": "JS8 Bravo"},
    ]
    bundles = {
        1: {"launch_enabled": True, "items": [_item("JS8Call", path="/usr/bin/js8call-subspace")]},
        2: {"launch_enabled": True, "items": [_item("JS8Call", path="/usr/bin/js8call-subspace")]},
    }

    plan = StationLaunchPlanner().plan_startup(profiles, bundles, scope_radio_id=1)

    assert len(plan.instances) == 1
    assert plan.instances[0].radio_names == ("Alpha",)


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
        {"id": 2, "name": "Bravo", "runtime_active": 1, "launch_enabled": True, "js8_instance_system_key": "js8-bravo", "js8_instance_name": "JS8 Bravo"},
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


def test_selected_radio_manual_start_can_override_only_automatic_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._active = False
    orchestrator.launch_allowed = lambda: False
    captured: dict[str, object] = {}
    plan = LaunchPlan(
        trigger="manual",
        scope_radio_id=7,
        instances=(
            PlannedInstance(
                name="JS8Call",
                instance_key="js8:field",
                instance_identity="js8:field",
                radio_ids=(7,),
                radio_names=("Field",),
            ),
        ),
    )
    orchestrator.preview_manual_plan = lambda radio_id, **kwargs: (
        captured.update(radio_id=radio_id, bundle_override=kwargs.get("bundle_override")) or plan
    )
    orchestrator._start_sequence = lambda trigger, queue: (
        captured.update(trigger=trigger, queue=queue) or True
    )
    override = {"launch_enabled": True, "items": [_item("JS8Call", instance_key="js8:field")]}

    assert orchestrator.start_radio_startup_sequence(7, bundle_override=override) is True
    assert captured["radio_id"] == 7
    assert captured["bundle_override"] == override
    assert captured["trigger"] == "manual"
    assert captured["queue"] == plan.queue()


def test_manual_selected_radio_plan_includes_inactive_radio_but_startup_does_not() -> None:
    profiles = [{"id": 7, "name": "Field", "runtime_active": 0}]
    bundles = {
        7: {
            "launch_enabled": True,
            "items": [_item("FLRig", instance_key="flrig:field", path="/usr/local/bin/flrig")],
        }
    }
    planner = StationLaunchPlanner()

    manual = planner.plan_startup(profiles, bundles, scope_radio_id=7, trigger="manual")
    unattended = planner.plan_startup(profiles, bundles, scope_radio_id=7, trigger="startup")

    assert [instance.name for instance in manual.instances] == ["FLRig"]
    assert unattended.instances == ()


def test_explicit_manual_sequence_preserves_structured_instance_recipe() -> None:
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._active = False
    orchestrator.launch_allowed = lambda: True
    captured: dict[str, object] = {}
    orchestrator._build_queue = lambda items, startup_only: (
        captured.update(items=items, startup_only=startup_only) or list(items)
    )
    orchestrator._start_sequence = lambda trigger, queue: (
        captured.update(trigger=trigger, queue=queue) or True
    )
    item = {
        **_item("VarAC", instance_key="varac:field"),
        "monitor_health": False,
        "dependencies": ["FLRig"],
        "readiness_policy": {
            "working_directory": "/radio/field",
            "launch_arguments": ["C:\\VarAC\\Field.ini"],
        },
        "execution_scope": "standard",
    }

    assert orchestrator.start_manual_sequence([item]) is True
    normalized = captured["items"][0]
    assert normalized["instance_key"] == "varac:field"
    assert normalized["monitor_health"] is False
    assert normalized["dependencies"] == ["FLRig"]
    assert normalized["readiness_policy"]["working_directory"] == "/radio/field"
    assert normalized["readiness_policy"]["launch_arguments"] == ["C:\\VarAC\\Field.ini"]
    assert captured["startup_only"] is False


def test_executor_does_not_use_family_status_for_distinct_instance() -> None:
    from freqinout.core.launch_orchestrator import LaunchOrchestrator

    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator.status = type(
        "CachedStatus",
        (),
        {
            "cached_program_instance_running": lambda _self, _name, target, arguments=(): (
                target == "/usr/local/bin/flrig"
                and tuple(arguments) == ("--config-dir", "/profiles/alpha")
            )
        },
    )()
    orchestrator._cached_status_for_item = lambda _item: {"running": True}

    assert orchestrator._program_running(
        {
            "name": "FLRig",
            "instance_identity": "alpha",
            "launch_path_override": "/usr/local/bin/flrig",
            "launch_arguments": ["--config-dir", "/profiles/alpha"],
        }
    ) is True
    assert orchestrator._program_running(
        {
            "name": "FLRig",
            "instance_identity": "bravo",
            "launch_path_override": "/usr/local/bin/flrig",
            "launch_arguments": ["--config-dir", "/profiles/bravo"],
        }
    ) is False


@pytest.mark.parametrize("name", ["FLRig", "FLDigi", "JS8Call"])
def test_selected_radio_endpoint_identity_launches_when_only_other_radio_process_is_running(
    monkeypatch,
    name: str,
) -> None:
    from types import SimpleNamespace

    captured: dict[str, object] = {}

    def fake_popen(command, **kwargs):
        captured["command"] = list(command)
        captured.update(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(launch_module.subprocess, "Popen", fake_popen)
    item = {
        "name": name,
        "instance_identity": f"ft-710:{name.casefold()}",
        "readiness_policy": {
            "host": "127.0.0.1",
            "port": {"FLRig": 12346, "FLDigi": 7363, "JS8Call": 2443}[name],
        },
    }
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._active = True
    orchestrator._cancel_requested = False
    orchestrator._queue = [item]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator._blocked_dependency_for = lambda _item: None
    orchestrator._program_running = lambda _item: True
    orchestrator._program_ready_for_sequence = lambda _item: False
    orchestrator._configured_instance_process_running = lambda _item: None
    orchestrator._sequence_preflight_started_wall = 100.0
    orchestrator._cached_status_for_item = lambda _item, force=False: {
        "source": "endpoint",
        "checked_at": 101.0,
        "reachable": False,
    }
    orchestrator._resolve_launch_command = lambda _item: ([f"/usr/local/bin/{name.casefold()}"], "test")
    orchestrator._is_self_launch_command = lambda _cmd: False
    orchestrator._materialize_item_managed_directories = lambda _item: ()
    orchestrator._infer_launch_cwd = lambda *_args: None
    orchestrator._schedule_advance_queue = lambda _delay=0: None
    orchestrator.dependency_status = SimpleNamespace(refresh_now=lambda **_kwargs: None)
    orchestrator._poll_timer = SimpleNamespace(setInterval=lambda _value: None, start=lambda: None)

    orchestrator._advance_queue()

    assert captured["command"] == [f"/usr/local/bin/{name.casefold()}"]
    assert orchestrator._current_item is item


def test_selected_radio_endpoint_identity_verifies_port_before_crediting_exact_process(monkeypatch) -> None:
    from types import SimpleNamespace

    started: list[bool] = []
    monkeypatch.setattr(
        launch_module.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("must not duplicate an exact running process"),
    )
    item = {
        "name": "FLDigi",
        "instance_identity": "ft-710:fldigi",
        "launch_path_override": "/usr/local/bin/fldigi",
        "launch_arguments": ["--config-dir", "/home/bill/.fldigi/instances/FT-710"],
        "readiness_policy": {"host": "127.0.0.1", "port": 7363, "require_service": True},
    }
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._active = True
    orchestrator._cancel_requested = False
    orchestrator._queue = [item]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator._blocked_dependency_for = lambda _item: None
    orchestrator._program_running = lambda _item: True
    orchestrator._program_ready_for_sequence = lambda _item: False
    orchestrator._configured_instance_process_running = lambda _item: True
    orchestrator._sequence_preflight_started_wall = 100.0
    orchestrator._cached_status_for_item = lambda _item, force=False: {
        "source": "process",
        "checked_at": 0.0,
        "stale": True,
        "reachable": False,
    }
    orchestrator._schedule_advance_queue = lambda _delay=0: None
    orchestrator._poll_timer = SimpleNamespace(
        setInterval=lambda _value: None,
        start=lambda: started.append(True),
    )

    orchestrator._advance_queue()

    assert started == [True]
    assert orchestrator._current_item is item
    assert orchestrator._current_phase == "endpoint_preflight"


def test_executor_never_spawns_before_fresh_process_preflight(monkeypatch) -> None:
    monkeypatch.setattr(
        launch_module.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("cold process evidence must never authorize launch"),
    )
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._active = True
    orchestrator._process_preflight_pending = True
    orchestrator._cancel_requested = False
    orchestrator._queue = [{"name": "FLMsg", "instance_identity": "fast:alpha:flmsg"}]
    orchestrator._index = 0
    orchestrator._results = []

    orchestrator._advance_queue()

    assert orchestrator._index == 0
    assert orchestrator._results == []


def test_start_sequence_forces_and_waits_for_new_process_snapshot(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    from freqinout.core.settings_manager import SettingsManager

    snapshots = [SimpleNamespace(sequence=4, scope="legacy_primary")]
    refresh_calls: list[dict[str, object]] = []
    scheduled: list[int] = []
    orchestrator = LaunchOrchestrator(SettingsManager())
    orchestrator.dependency_status = SimpleNamespace(
        latest_snapshot=lambda: snapshots[-1],
        refresh_now=lambda **kwargs: refresh_calls.append(dict(kwargs)) or snapshots[-1],
    )
    orchestrator._schedule_advance_queue = lambda delay=0: scheduled.append(int(delay))

    assert orchestrator._start_sequence(
        "startup",
        [{"name": "FLMsg", "instance_identity": "fast:alpha:flmsg"}],
    ) is True
    assert refresh_calls == [{"reason": "launch-preflight:startup", "force": True}]
    assert scheduled == []

    snapshots.append(SimpleNamespace(sequence=5, scope="legacy_primary"))
    orchestrator._on_launch_preflight_snapshot_changed(snapshots[-1])

    assert orchestrator._process_preflight_pending is False
    assert scheduled == [0]


@pytest.mark.parametrize("name", ["FLRig", "FLDigi", "JS8Call"])
def test_endpoint_owner_waits_for_current_port_evidence_before_spawn(
    monkeypatch,
    name: str,
) -> None:
    monkeypatch.setattr(
        launch_module.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("pending endpoint evidence must never authorize launch"),
    )
    started: list[bool] = []
    item = {
        "name": name,
        "instance_identity": f"radio-a:{name.casefold()}",
        "launch_path_override": f"/usr/local/bin/{name.casefold()}",
        "readiness_policy": {
            "host": "127.0.0.1",
            "port": {"FLRig": 12345, "FLDigi": 7362, "JS8Call": 2442}[name],
        },
    }
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._active = True
    orchestrator._process_preflight_pending = False
    orchestrator._cancel_requested = False
    orchestrator._queue = [item]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator._sequence_preflight_started_wall = 100.0
    orchestrator._blocked_dependency_for = lambda _item: None
    orchestrator._configured_instance_process_running = lambda _item: False
    orchestrator._cached_status_for_item = lambda _item, force=False: {
        "source": "process",
        "checked_at": 0.0,
        "stale": True,
        "reachable": False,
    }
    orchestrator._schedule_advance_queue = lambda _delay=0: None
    orchestrator._poll_timer = SimpleNamespace(
        setInterval=lambda _value: None,
        start=lambda: started.append(True),
    )

    orchestrator._advance_queue()

    assert started == [True]
    assert orchestrator._current_phase == "endpoint_preflight"
    assert orchestrator._current_item is item


def test_current_configured_endpoint_is_treated_as_already_running(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    monkeypatch.setattr(
        launch_module.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("an occupied configured endpoint must not be relaunched"),
    )
    from freqinout.core.settings_manager import SettingsManager

    item = {
        "name": "FLRig",
        "instance_identity": "radio-a:flrig",
        "launch_path_override": "/usr/local/bin/flrig",
        "readiness_policy": {"host": "127.0.0.1", "port": 12345},
    }
    orchestrator = LaunchOrchestrator(SettingsManager())
    orchestrator._active = True
    orchestrator._process_preflight_pending = False
    orchestrator._cancel_requested = False
    orchestrator._queue = [item]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator._sequence_preflight_started_wall = 100.0
    orchestrator._blocked_dependency_for = lambda _item: None
    orchestrator._configured_instance_process_running = lambda _item: False
    orchestrator._cached_status_for_item = lambda _item, force=False: {
        "source": "endpoint",
        "checked_at": 101.0,
        "reachable": True,
    }
    orchestrator._schedule_advance_queue = lambda _delay=0: None

    orchestrator._advance_queue()

    assert orchestrator._results[0]["status"] == "already_running"
    assert orchestrator._results[0]["detail"] == "configured endpoint is already active"


def test_exact_process_with_unready_port_is_not_duplicated_or_held(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    monkeypatch.setattr(
        launch_module.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("a process/port mismatch must not spawn another copy"),
    )
    from freqinout.core.settings_manager import SettingsManager

    item = {
        "name": "FLDigi",
        "instance_identity": "radio-a:fldigi",
        "launch_path_override": "/usr/local/bin/fldigi",
        "launch_arguments": ["--config-dir", "/profiles/radio-a"],
        "readiness_policy": {"host": "127.0.0.1", "port": 7362},
    }
    orchestrator = LaunchOrchestrator(SettingsManager())
    orchestrator._active = True
    orchestrator._process_preflight_pending = False
    orchestrator._cancel_requested = False
    orchestrator._queue = [item]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator._sequence_preflight_started_wall = 100.0
    orchestrator._blocked_dependency_for = lambda _item: None
    orchestrator._configured_instance_process_running = lambda _item: True
    orchestrator._cached_status_for_item = lambda _item, force=False: {
        "source": "endpoint",
        "checked_at": 101.0,
        "reachable": False,
    }
    orchestrator._schedule_advance_queue = lambda _delay=0: None

    orchestrator._advance_queue()

    assert orchestrator._current_item is None
    assert orchestrator._results[0]["status"] == "failed"
    assert "duplicate launch skipped" in orchestrator._results[0]["detail"]


@pytest.mark.parametrize("name", ["FLMsg", "FLAmp"])
def test_non_endpoint_fast_light_tools_do_not_use_endpoint_relaunch_recovery(name: str) -> None:
    assert not LaunchOrchestrator._has_persisted_endpoint_identity(
        {
            "name": name,
            "instance_identity": f"ft-710:{name.casefold()}",
            "readiness_policy": {"host": "127.0.0.1", "port": 7000},
        }
    )


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


def test_js8_planner_manages_stable_rig_names_and_storage_preview_without_numeric_ids(tmp_path: Path) -> None:
    profiles = [
        {"id": 91, "name": "Alpha", "runtime_active": 1, "js8_port": 2442, "js8_instance_system_key": "field-alpha", "js8_instance_name": "Alpha Instance", "js8_variant_family": "js8call", "js8_variant_version": "2.2.0", "js8_message_storage_root": str(tmp_path / "alpha"), "js8_storage_evidence": "operator_confirmed:fixture"},
        {"id": 4, "name": "Bravo", "runtime_active": 1, "js8_port": 2443, "js8_instance_system_key": "field-bravo", "js8_instance_name": "Bravo Instance", "js8_variant_family": "js8call_improved", "js8_variant_version": "3.0.3", "js8_message_storage_root": str(tmp_path / "bravo"), "js8_storage_evidence": "operator_confirmed:fixture"},
    ]
    bundles = {
        91: {"launch_enabled": True, "items": [_item("JS8Call", path="/apps/JS8Call")]},
        4: {"launch_enabled": True, "items": [_item("JS8Call", path="/apps/JS8Call")]},
    }

    queue = StationLaunchPlanner().plan_startup(profiles, bundles).queue()

    assert [row["launch_arguments"][0] for row in queue] == ["--rig-name", "--rig-name"]
    assert all("91" not in str(row["rig_name"]) for row in queue)
    assert len({row["rig_name"] for row in queue}) == 2
    assert {row["application_data_root"] for row in queue} == {str(tmp_path / "alpha"), str(tmp_path / "bravo")}


def test_fast_light_planner_preserves_instance_specific_native_arguments() -> None:
    profiles = [
        {
            "id": 7,
            "name": "Field Radio",
            "runtime_active": 1,
            "flrig_host": "127.0.0.1",
            "flrig_port": 12445,
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7462,
        }
    ]
    flrig = _item("FLRig", path="/apps/flrig", instance_key="fast:field:flrig")
    flrig["readiness_policy"] = {
        "host": "127.0.0.1",
        "port": 12445,
        "launch_arguments": ["--config-dir", "/profiles/flrig-field"],
    }
    fldigi = _item("FLDigi", path="/apps/fldigi", instance_key="fast:field:fldigi")
    fldigi["dependencies"] = ["FLRig"]
    fldigi["readiness_policy"] = {
        "host": "127.0.0.1",
        "port": 7462,
        "launch_arguments": [
            "--config-dir",
            "/profiles/fldigi-field",
            "--xmlrpc-server-address",
            "127.0.0.1",
            "--xmlrpc-server-port",
            "7462",
        ],
    }

    queue = StationLaunchPlanner().plan_startup(
        profiles,
        {7: {"launch_enabled": True, "items": [flrig, fldigi]}},
    ).queue()

    assert queue[0]["launch_arguments"] == ["--config-dir", "/profiles/flrig-field"]
    assert queue[1]["launch_arguments"][-2:] == ["--xmlrpc-server-port", "7462"]
    assert "launch_arguments" not in queue[0]["readiness_policy"]


def test_manual_flrig_launch_uses_selected_radio_config_dir_when_other_instance_runs(monkeypatch) -> None:
    """FT-710 manual Start must not inherit or suppress its FLRig recipe."""
    from types import SimpleNamespace

    profiles = [
        {"id": 1, "name": "FTDX-10", "runtime_active": 1, "flrig_host": "127.0.0.1", "flrig_port": 12345},
        {"id": 9, "name": "FT-710", "runtime_active": 1, "flrig_host": "127.0.0.1", "flrig_port": 12346},
    ]
    first = _item("FLRig", path="/usr/local/bin/flrig", instance_key="fast:ftdx10:flrig")
    first["readiness_policy"] = {
        "host": "127.0.0.1", "port": 12345,
        "launch_arguments": ["--config-dir", "/profiles/ftdx10/flrig"],
    }
    selected = _item("FLRig", path="/usr/local/bin/flrig", instance_key="fast:ft710:flrig")
    selected["readiness_policy"] = {
        "host": "127.0.0.1", "port": 12346,
        "launch_arguments": ["--config-dir", "/profiles/ft710/flrig"],
    }
    queue_item = StationLaunchPlanner().plan_startup(
        profiles,
        {1: {"launch_enabled": True, "items": [first]}, 9: {"launch_enabled": True, "items": [selected]}},
        scope_radio_id=9,
        trigger="manual",
    ).queue()[0]

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        launch_module.subprocess,
        "Popen",
        lambda command, **kwargs: captured.update(command=list(command), **kwargs) or SimpleNamespace(),
    )
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._active = True
    orchestrator._cancel_requested = False
    orchestrator._queue = [queue_item]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator._blocked_dependency_for = lambda _item: None
    # A process named FLRig exists for FTDX-10, but the exact FT-710 recipe
    # (including its --config-dir selector) is not running.
    orchestrator._program_running = lambda _item: True
    orchestrator._configured_instance_process_running = lambda _item: False
    orchestrator._program_ready_for_sequence = lambda _item: False
    orchestrator._sequence_preflight_started_wall = 100.0
    orchestrator._cached_status_for_item = lambda _item, force=False: {
        "source": "endpoint",
        "checked_at": 101.0,
        "reachable": False,
    }
    orchestrator._is_self_launch_command = lambda _cmd: False
    orchestrator._materialize_item_managed_directories = lambda _item: ()
    orchestrator._infer_launch_cwd = lambda *_args: None
    orchestrator._schedule_advance_queue = lambda _delay=0: None
    orchestrator.dependency_status = SimpleNamespace(refresh_now=lambda **_kwargs: None)
    orchestrator._poll_timer = SimpleNamespace(setInterval=lambda _value: None, start=lambda: None)

    orchestrator._advance_queue()

    assert queue_item["radio_ids"] == [9]
    assert queue_item["launch_arguments"] == ["--config-dir", "/profiles/ft710/flrig"]
    assert captured["command"] == ["/usr/local/bin/flrig", "--config-dir", "/profiles/ft710/flrig"]


def test_canonical_identity_recovers_recipe_fields_from_damaged_saved_launch_row() -> None:
    component = SimpleNamespace(
        component_id="flrig",
        argv=("/usr/local/bin/flrig", "--config-dir", "/profiles/ft710/flrig"),
        cwd="/profiles/ft710/flrig",
        env={"FIO_RADIO": "FT-710"},
        dependencies=(),
        launch={"at_startup": False, "monitor_health": True},
        readiness={"host": "127.0.0.1", "port": 12346},
    )
    record = SimpleNamespace(
        bundle_id="fast-light:ft-710",
        family_key="fast_light",
        scope="radio_scoped",
        components=(component,),
        launch={},
    )
    damaged = {
        "name": "FLRig",
        "instance_key": "FLRig",
        "enabled": True,
        "startup": True,
        "monitor_health": False,
        "launch_path_override": "/usr/local/bin/flrig",
        "dependencies": [],
        "readiness_policy": {},
    }
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator.settings = SimpleNamespace(get=lambda _key, default=None: default)
    orchestrator.bundle_store = SimpleNamespace(
        get_bundle=lambda _radio_id, legacy_items=None: {
            "radio_profile_id": 9,
            "launch_enabled": True,
            "items": [damaged],
        }
    )
    orchestrator.multi_radio_store = SimpleNamespace(
        radio_software_identity_generation=lambda _radio_id: 1,
        list_radio_software_identity_records=lambda _radio_id: (record,),
        get_software_instance_manifest=lambda _bundle_id: {
            "evidence": {
                "launch_recipe": {
                    "components": [{
                        "component_key": "flrig",
                        "executable": "/usr/local/bin/flrig",
                        "arguments": ["--config-dir", "/profiles/ft710/flrig"],
                        "working_directory": "/profiles/ft710/flrig",
                        "managed_directories": ["/profiles/ft710/flrig"],
                        "execution_scope": "radio_scoped",
                    }]
                }
            }
        },
    )

    bundle = orchestrator.get_radio_launch_bundle(9)
    recovered = bundle["items"][0]

    assert bundle["canonical_recovery"] is True
    assert recovered["instance_key"] == "fast-light:ft-710:flrig"
    assert recovered["startup"] is True
    assert recovered["monitor_health"] is False
    assert recovered["readiness_policy"]["launch_arguments"] == [
        "--config-dir", "/profiles/ft710/flrig",
    ]
    assert recovered["readiness_policy"]["working_directory"] == "/profiles/ft710/flrig"
    assert recovered["readiness_policy"]["managed_directories"] == ["/profiles/ft710/flrig"]


def test_manual_bundle_override_recovers_exact_flamp_identity_before_planning() -> None:
    arguments = (
        "--config-dir", "/home/bill/.nbems/instances/FT-710",
        "--arq-server-address", "127.0.0.1",
        "--arq-server-port", "7323",
        "--xmlrpc-server-address", "127.0.0.1",
        "--xmlrpc-server-port", "7363",
    )
    component = SimpleNamespace(
        component_id="flamp",
        argv=("/usr/local/bin/flamp", *arguments),
        cwd="/home/bill/.nbems/instances/FT-710",
        env={},
        dependencies=("fldigi",),
        launch={"at_startup": False, "monitor_health": True},
        readiness={"kind": "process"},
    )
    record = SimpleNamespace(
        bundle_id="fast-light:ft-710",
        family_key="fast_light",
        scope="radio_scoped",
        components=(component,),
        launch={},
    )
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator.multi_radio_store = SimpleNamespace(
        radio_software_identity_generation=lambda _radio_id: 1,
        list_radio_software_identity_records=lambda _radio_id: (record,),
        get_software_instance_manifest=lambda _bundle_id: {
            "evidence": {
                "launch_recipe": {
                    "components": [{
                        "component_key": "flamp",
                        "executable": "/usr/local/bin/flamp",
                        "arguments": list(arguments),
                        "working_directory": "/home/bill/.nbems/instances/FT-710",
                        "profile_selector": "/home/bill/.nbems/instances/FT-710",
                        "managed_directories": [
                            "/home/bill/.nbems/instances/FT-710",
                            "/home/bill/.nbems/instances/FT-710/FLAMP/rx",
                        ],
                        "evidence": {
                            "source": "flamp_config_dir_and_endpoint_pair",
                            "confidence": "isolated",
                        },
                        "execution_scope": "radio_scoped",
                    }]
                }
            }
        },
    )
    override = {
        "radio_profile_id": 9,
        "launch_enabled": True,
        "items": [{
            "name": "FLAmp",
            "instance_key": "FLAmp",
            "enabled": True,
            "startup": True,
            "monitor_health": False,
            "launch_path_override": "/usr/local/bin/flamp",
            "dependencies": [],
            "readiness_policy": {},
        }],
    }

    restored = orchestrator._restore_canonical_bundle_override(9, override)
    row = restored["items"][0]

    assert restored["canonical_recovery"] is True
    assert row["instance_key"] == "fast-light:ft-710:flamp"
    assert row["startup"] is True
    assert row["monitor_health"] is False
    assert row["dependencies"] == ["fldigi"]
    assert row["readiness_policy"]["launch_arguments"] == list(arguments)
    assert row["readiness_policy"]["profile_selector"] == "/home/bill/.nbems/instances/FT-710"


def test_manual_override_does_not_enable_canonical_component_absent_from_draft() -> None:
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._restore_canonical_launch_items = lambda _radio_id, items: [
        {
            **items[0],
            "instance_key": "fast-light:ft-710:flamp",
            "readiness_policy": {"launch_arguments": ["--config-dir", "/profiles/ft710"]},
        },
        {
            "name": "FLDigi",
            "instance_key": "fast-light:ft-710:fldigi",
            "enabled": True,
            "startup": True,
            "readiness_policy": {"launch_arguments": ["--config-dir", "/profiles/ft710"]},
        },
    ]

    restored = orchestrator._restore_canonical_bundle_override(
        9,
        {
            "launch_enabled": True,
            "items": [{
                "name": "FLAmp",
                "enabled": True,
                "startup": True,
                "monitor_health": False,
            }],
        },
    )

    flamp, fldigi = restored["items"]
    assert flamp["enabled"] is True
    assert flamp["startup"] is True
    assert fldigi["enabled"] is False
    assert fldigi["startup"] is False


def test_selected_radio_flamp_launch_ignores_other_radio_process(monkeypatch) -> None:
    arguments = [
        "--config-dir", "/home/bill/.nbems/instances/FT-710",
        "--arq-server-address", "127.0.0.1",
        "--arq-server-port", "7323",
        "--xmlrpc-server-address", "127.0.0.1",
        "--xmlrpc-server-port", "7363",
    ]
    item = {
        "name": "FLAmp",
        "instance_key": "fast-light:ft-710:flamp",
        "instance_identity": "fast-light:ft-710:flamp",
        "radio_ids": [9],
        "launch_path_override": "/usr/local/bin/flamp",
        "launch_arguments": arguments,
        "working_directory": "/home/bill/.nbems/instances/FT-710",
        "profile_selector": "/home/bill/.nbems/instances/FT-710",
        "dependencies": [],
        "readiness_policy": {
            "kind": "process",
            "evidence": {"source": "flamp_config_dir_and_endpoint_pair"},
        },
        "execution_scope": "radio_scoped",
    }
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        launch_module.subprocess,
        "Popen",
        lambda command, **kwargs: captured.update(command=list(command), **kwargs) or SimpleNamespace(),
    )
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator._active = True
    orchestrator._cancel_requested = False
    orchestrator._queue = [item]
    orchestrator._index = 0
    orchestrator._results = []
    orchestrator.status = SimpleNamespace(
        cached_program_instance_running=lambda _name, _target, exact_args=(): (
            tuple(exact_args)
            == tuple(value.replace("FT-710", "FTDX-10") for value in arguments)
        )
    )
    orchestrator._cached_status_for_item = lambda _item: {"running": True}
    orchestrator._program_ready_for_sequence = lambda _item: False
    orchestrator._materialize_item_managed_directories = lambda _item: ()
    orchestrator._is_self_launch_command = lambda _cmd: False
    orchestrator._infer_launch_cwd = lambda *_args: None
    orchestrator._schedule_advance_queue = lambda _delay=0: None
    orchestrator.dependency_status = SimpleNamespace(refresh_now=lambda **_kwargs: None)
    orchestrator._poll_timer = SimpleNamespace(setInterval=lambda _value: None, start=lambda: None)

    orchestrator._advance_queue()

    assert captured["command"] == ["/usr/local/bin/flamp", *arguments]
    assert orchestrator._current_item is item


def test_radio_scoped_flamp_without_exact_native_identity_fails_closed() -> None:
    item = {
        "name": "FLAmp",
        "instance_identity": "fast-light:ft-710:flamp",
        "launch_arguments": [],
        "execution_scope": "radio_scoped",
        "operator_starts": False,
    }

    reason = LaunchOrchestrator._instance_launch_identity_blocker(item)

    assert "radio-scoped launch identity is incomplete" in reason
    assert "--config-dir" in reason
    assert "--arq-server-port" in reason


def test_canonical_lowercase_component_dependency_still_orders_launch_apps() -> None:
    profiles = [{"id": 9, "name": "FT-710", "runtime_active": 1}]
    fldigi = _item("FLDigi", path="/apps/fldigi", instance_key="fast:ft710:fldigi")
    fldigi["dependencies"] = ["flrig"]
    flrig = _item("FLRig", path="/apps/flrig", instance_key="fast:ft710:flrig")

    queue = StationLaunchPlanner().plan_startup(
        profiles,
        {9: {"launch_enabled": True, "items": [fldigi, flrig]}},
    ).queue()

    assert [item["name"] for item in queue] == ["FLRig", "FLDigi"]


def test_js8_command_override_preserves_one_rig_name_and_rejects_duplicates() -> None:
    profile = {"id": 1, "name": "Alpha", "runtime_active": 1, "js8_instance_system_key": "field-alpha", "js8_instance_name": "Alpha Instance"}
    bundle = {1: {"launch_enabled": True, "items": [_item("JS8Call", command="/apps/JS8Call --rig-name 'Manual Alpha'")]}}

    row = StationLaunchPlanner().plan_startup([profile], bundle).queue()[0]
    assert row["rig_name"] == "Manual Alpha"
    assert row["rig_name_source"] == "command_override"
    assert row["launch_arguments"] == []

    duplicate = {1: {"launch_enabled": True, "items": [_item("JS8Call", command="/apps/JS8Call -r alpha --rig-name bravo")]}}
    with pytest.raises(ValueError, match="duplicate or conflicting"):
        StationLaunchPlanner().plan_startup([profile], duplicate)


def test_js8_changed_rig_name_does_not_reuse_prior_verified_storage_root(tmp_path: Path) -> None:
    prior_root = tmp_path / "prior-rig"
    profile = {
        "id": 1,
        "name": "Alpha",
        "runtime_active": 1,
        "js8_port": 2442,
        "js8_instance_system_key": "field-alpha",
        "js8_instance_name": "Alpha Instance",
        "js8_variant_family": "js8call",
        "js8_variant_version": "2.2.0",
        "js8_rig_name": "prior",
        "js8_message_storage_root": str(prior_root),
        "js8_storage_evidence": "runtime_verified:message_files",
    }
    bundle = {
        1: {
            "launch_enabled": True,
            "items": [_item("JS8Call", command="/apps/JS8Call --rig-name replacement")],
        }
    }

    row = StationLaunchPlanner().plan_startup([profile], bundle).queue()[0]

    assert row["rig_name"] == "replacement"
    assert row["application_data_root"] != str(prior_root)
    assert str(row["application_data_root"]).endswith("JS8Call - replacement")


def test_subspace_specific_launch_target_uses_rig_scoped_root() -> None:
    profile = {
        "id": 1,
        "name": "Alpha",
        "runtime_active": 1,
        "js8_port": 2442,
        "js8_instance_system_key": "field-alpha",
        "js8_instance_name": "Alpha Instance",
        "js8_variant_family": "unknown",
    }
    bundle = {
        1: {
            "launch_enabled": True,
            "items": [_item("JS8Call", path="/usr/bin/js8call-subspace")],
        }
    }

    row = StationLaunchPlanner().plan_startup([profile], bundle).queue()[0]

    assert row["expected_storage_mode"] == "rig_scoped"
    assert row["storage_mode"] == "unverified"
    assert "JS8Call - fio-field-alpha" in str(row["application_data_root"])


def test_js8_launch_planning_does_not_probe_filesystem_message_evidence(monkeypatch) -> None:
    monkeypatch.setattr(
        js8_storage_module,
        "_has_message_evidence",
        lambda _path: (_ for _ in ()).throw(AssertionError("launch preview performed filesystem I/O")),
    )
    profile = {
        "id": 1,
        "name": "Alpha",
        "runtime_active": 1,
        "js8_instance_system_key": "field-alpha",
        "js8_instance_name": "Alpha Instance",
        "js8_variant_family": "js8call",
        "js8_variant_version": "2.2.0",
    }
    bundle = {1: {"launch_enabled": True, "items": [_item("JS8Call", path="/apps/JS8Call")]}}

    row = StationLaunchPlanner().plan_startup([profile], bundle).queue()[0]

    assert row["application_data_root"]


def test_js8_planner_blocks_duplicate_rigs_and_rig_scoped_storage_roots(tmp_path: Path) -> None:
    base = {"runtime_active": 1, "js8_variant_family": "js8call", "js8_variant_version": "2.2.0", "js8_storage_evidence": "operator_confirmed:fixture"}
    duplicate_rigs = [
        {**base, "id": 1, "name": "Alpha", "js8_port": 2442, "js8_instance_system_key": "alpha", "js8_message_storage_root": str(tmp_path / "a")},
        {**base, "id": 2, "name": "Bravo", "js8_port": 2443, "js8_instance_system_key": "bravo", "js8_message_storage_root": str(tmp_path / "b")},
    ]
    same_rig_bundles = {
        1: {"launch_enabled": True, "items": [_item("JS8Call", command="/apps/JS8Call -r shared")]},
        2: {"launch_enabled": True, "items": [_item("JS8Call", command="/apps/JS8Call --rig-name shared")]},
    }
    with pytest.raises(ValueError, match="same effective rig name"):
        StationLaunchPlanner().plan_startup(duplicate_rigs, same_rig_bundles)

    same_root_profiles = [
        {**base, "id": 1, "name": "Alpha", "js8_port": 2442, "js8_instance_system_key": "alpha", "js8_message_storage_root": str(tmp_path / "shared")},
        {**base, "id": 2, "name": "Bravo", "js8_port": 2443, "js8_instance_system_key": "bravo", "js8_message_storage_root": str(tmp_path / "shared")},
    ]
    managed_bundles = {
        1: {"launch_enabled": True, "items": [_item("JS8Call", path="/apps/JS8Call")]},
        2: {"launch_enabled": True, "items": [_item("JS8Call", path="/apps/JS8Call")]},
    }
    with pytest.raises(ValueError, match="same message-storage root"):
        StationLaunchPlanner().plan_startup(same_root_profiles, managed_bundles)


def test_js8_bundle_command_appends_planned_arguments_after_bundle_resolution(tmp_path: Path, monkeypatch) -> None:
    executable = tmp_path / "JS8Call.app" / "Contents" / "MacOS" / "JS8Call"
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    monkeypatch.setattr(launch_module.platform, "system", lambda: "Darwin")

    command, _description = orchestrator._resolve_launch_command({"name": "JS8Call", "launch_path_override": str(tmp_path / "JS8Call.app"), "launch_arguments": ["--rig-name", "field-alpha"]})

    assert command == [str(executable), "--rig-name", "field-alpha"]


def test_js8_preview_queue_exposes_the_exact_effective_command() -> None:
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    plan = LaunchPlan(
        trigger="startup",
        scope_radio_id=None,
        instances=(
            PlannedInstance(
                name="JS8Call",
                instance_key="JS8Call",
                instance_identity="alpha",
                radio_ids=(1,),
                radio_names=("Alpha",),
                launch_command_override="/apps/JS8Call",
                launch_arguments=("--rig-name", "field-alpha"),
                rig_name="field-alpha",
                application_data_root="/storage/field-alpha",
                storage_mode="rig_scoped",
            ),
        ),
    )

    queue = orchestrator._with_effective_launch_preview(plan).queue()

    assert queue[0]["effective_command"] == ["/apps/JS8Call", "--rig-name", "field-alpha"]
    assert queue[0]["rig_name"] == "field-alpha"
    assert queue[0]["application_data_root"] == "/storage/field-alpha"
