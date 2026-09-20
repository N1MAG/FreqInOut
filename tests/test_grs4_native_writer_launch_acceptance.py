"""GRS-4 acceptance tests for qualified writers and launch integration."""

from pathlib import Path

import pytest

from freqinout.core.config_autodiscovery import build_lab_radio_proposals
from freqinout.core.guided_app_config_plan import (
    apply_guided_external_app_config_plan,
    build_guided_external_app_config_plan,
    rollback_guided_external_app_config_apply,
)
from freqinout.core.guided_radio_software_model import (
    GuidedRadioSoftwareValidationError,
    NativeWriterCapability,
    NativeWriterOperation,
    NativeWriterRegistry,
    SoftwareFamily,
)
from freqinout.core.station_launch_planner import StationLaunchPlanner


def _proposals(count=1):
    return build_lab_radio_proposals(radio_count=count, busy_checker=lambda _host, _port: False)


def _qualified_js8_paths(ini: Path) -> dict[str, str]:
    return {
        "js8call": "/apps/js8",
        "js8call_ini_path": str(ini),
        "js8_storage_home": str(ini.parent),
        "js8_variant_family": "js8call_2_2",
        "js8_variant_version": "2.2.0",
        "js8_writer_platform": "linux",
        "js8_writer_operation": "create",
    }


def _managed_js8_target(root: Path) -> Path:
    return root / ".config" / "JS8Call - Radio-A.ini"


def _items(*, startup=True, enabled=True, name="JS8Call", command="", path="/apps/js8call", key="js8-a"):
    return [{
        "name": name, "instance_key": key, "enabled": enabled, "startup": startup,
        "monitor_health": True, "launch_command_override": command,
        "launch_path_override": path, "dependencies": [], "readiness_policy": {},
    }]


def test_native_writer_capability_requires_exact_qualified_full_lifecycle():
    record = NativeWriterCapability(
        "js8-stock-22-macos", SoftwareFamily.JS8CALL, "stock", "2.2", "macos",
        NativeWriterOperation.UPDATE, preview_supported=True, backup_supported=True,
        readback_supported=True, restore_supported=True,
    )
    registry = NativeWriterRegistry((record,))
    assert registry.lookup(family="js8call", variant="stock", version="2.2", platform="macos", operation="update") is record
    assert not registry.supports(family="js8call", variant="stock", version="2.2.1", platform="macos", operation="update")
    assert not registry.supports(family="js8call", variant="*", version="2.2", platform="macos", operation="update")
    with pytest.raises(GuidedRadioSoftwareValidationError, match="preview, backup, readback, and restore"):
        NativeWriterCapability("incomplete", SoftwareFamily.JS8CALL, "stock", "2.2", "macos", NativeWriterOperation.CREATE, preview_supported=True)


def test_writer_plan_is_preview_only_and_external_apply_is_backup_gated(tmp_path):
    ini = tmp_path / "JS8Call.ini"
    ini.write_text("[Configuration]\nMyCall=OLD\n", encoding="utf-8")
    plan = build_guided_external_app_config_plan(
        _proposals(), config_root=tmp_path / "fio", app_paths={"js8call": "/Applications/JS8Call.app", "js8call_ini_path": str(ini), "js8_storage_home": str(tmp_path)}, callsign="n1mag", grid="dm79",
    )
    assert not (tmp_path / "fio").exists()
    result = apply_guided_external_app_config_plan(plan)
    assert result.backup is None and not result.external_writes_applied
    assert ini.read_text(encoding="utf-8") == "[Configuration]\nMyCall=OLD\n"


def test_backup_failure_blocks_every_external_write(monkeypatch, tmp_path):
    import freqinout.core.guided_app_config_plan as module
    from freqinout.core.config_backup import ConfigBackupResult, ConfigBackupItem

    ini = tmp_path / "JS8Call.ini"
    original = "[Configuration]\nMyCall=OLD\n"
    ini.write_text(original, encoding="utf-8")
    plan = build_guided_external_app_config_plan(
        _proposals(),
        config_root=tmp_path / "fio",
        app_paths=_qualified_js8_paths(ini),
        callsign="N1",
    )
    failed = ConfigBackupResult(
        backup_dir=str(tmp_path / "backup"), reason="injected", created_at="now",
        items=(ConfigBackupItem(str(ini), "", "file", "failed", "injected"),), manifest_path="",
    )
    monkeypatch.setattr(module, "create_config_backup", lambda *args, **kwargs: failed)
    result = apply_guided_external_app_config_plan(plan, allow_external_writes=True, backup_root=tmp_path / "backup")
    assert any(item.status == "failed" and "Backup failed" in item.detail for item in result.items)
    assert ini.read_text(encoding="utf-8") == original
    assert not _managed_js8_target(tmp_path).exists()


def test_post_apply_failure_restores_backup_and_reports_rollback(monkeypatch, tmp_path):
    """A write/readback fault must never leave a partially applied native file."""
    import freqinout.core.guided_app_config_plan as module

    ini = tmp_path / "JS8Call.ini"
    original = "[Configuration]\nMyCall=OLD\n"
    ini.write_text(original, encoding="utf-8")
    plan = build_guided_external_app_config_plan(
        _proposals(), config_root=tmp_path / "fio",
        app_paths=_qualified_js8_paths(ini), callsign="N1",
    )

    def partially_apply_then_fail(js8_plan, *, ini_path):
        Path(ini_path).write_text("[Configuration]\nMyCall=PARTIAL\n", encoding="utf-8")
        raise OSError("injected readback failure")

    monkeypatch.setattr(module, "apply_js8call_multisettings_plan", partially_apply_then_fail)
    result = apply_guided_external_app_config_plan(plan, allow_external_writes=True, backup_root=tmp_path / "backup")
    assert result.ok is False
    assert ini.read_text(encoding="utf-8") == original
    assert not _managed_js8_target(tmp_path).exists()
    assert any("restore" in item.detail.casefold() or item.status == "rolled_back" for item in result.items)


def test_later_persistence_failure_can_restore_a_successful_native_apply(tmp_path):
    ini = tmp_path / "JS8Call.ini"
    original = "[Configuration]\nMyCall=OLD\n"
    ini.write_text(original, encoding="utf-8")
    plan = build_guided_external_app_config_plan(
        _proposals(),
        config_root=tmp_path / "fio",
        app_paths=_qualified_js8_paths(ini),
        callsign="N1",
    )

    applied = apply_guided_external_app_config_plan(
        plan,
        allow_external_writes=True,
        backup_root=tmp_path / "backup",
    )
    assert applied.ok and applied.external_writes_applied
    target = _managed_js8_target(tmp_path)
    assert ini.read_text(encoding="utf-8") == original
    assert "MyCall = N1" in target.read_text(encoding="utf-8")

    restored = rollback_guided_external_app_config_apply(applied)
    assert restored.restore is not None and restored.restore.ok
    assert restored.ok is False
    assert ini.read_text(encoding="utf-8") == original
    assert not target.exists()
    assert any(item.status == "rolled_back" for item in restored.items)


def test_unsupported_writer_falls_back_to_operator_review_without_external_mutation(tmp_path):
    ini = tmp_path / "JS8Call.ini"
    original = "[Configuration]\nMyCall=OLD\n"
    ini.write_text(original, encoding="utf-8")
    plan = build_guided_external_app_config_plan(_proposals(), config_root=tmp_path / "fio", app_paths={"js8call": "/apps/js8", "js8call_ini_path": str(ini), "js8_storage_home": str(tmp_path)}, callsign="N1")
    result = apply_guided_external_app_config_plan(plan, allow_external_writes=True, backup_root=tmp_path / "backup")
    assert ini.read_text(encoding="utf-8") == original
    assert any(item.status == "operator_action_required" for item in result.items)
    assert all(item.status != "failed" for item in result.items)


def test_settings_runs_native_apply_and_restore_off_the_gui_thread() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    worker = source[source.index("class _GuidedNativeConfigWorker"):source.index("_GUIDED_DISCOVERY_INPUT_KEYS")]
    add_flow = source[source.index("def _start_guided_native_config_job"):source.index("def _set_active_selected_device_profile")]

    assert "apply_guided_external_app_config_plan(" in worker
    assert "rollback_guided_external_app_config_apply(" in worker
    assert "worker.moveToThread(thread)" in add_flow
    assert "thread.started.connect(worker.run)" in add_flow
    assert "self._rollback_guided_native_config(native_result)" in add_flow


@pytest.mark.parametrize("platform,command,expected", [
    ("macos", 'wine "/Applications/VarAC.app/Contents/MacOS/VarAC" --profile "A B"', ("wine", "/Applications/VarAC.app/Contents/MacOS/VarAC", "--profile", "A B")),
    ("linux", 'wine "/opt/VarAC/VarAC.exe" --profile "A B"', ("wine", "/opt/VarAC/VarAC.exe", "--profile", "A B")),
    ("windows", '"C:\\Program Files\\VarAC\\VarAC.exe" --profile "A B"', ("C:\\Program Files\\VarAC\\VarAC.exe", "--profile", "A B")),
])
def test_launch_planner_preserves_platform_command_quoting_and_effective_identity(platform, command, expected):
    profile = {"id": 1, "name": "Radio", "runtime_active": 1, "display_order": 1, "js8_instance_system_key": "sys-a", "js8_instance_name": "A", "js8_install_path": "/apps/js8"}
    item = _items(name="VarAC", command=command, path="/varac", key="varac-a")[0]
    plan = StationLaunchPlanner().plan_startup([profile], {1: {"launch_enabled": True, "items": [item]}}, trigger=platform)
    assert plan.instances[0].launch_command_override == command
    assert plan.instances[0].launch_path_override == "/varac"
    assert plan.instances[0].effective_command == ()
    assert expected[0] in command


def test_operator_start_is_not_startup_and_recipe_is_still_reviewable():
    profile = {"id": 1, "name": "Radio", "runtime_active": 1, "display_order": 1, "js8_instance_system_key": "sys-a", "js8_instance_name": "A"}
    item = _items(startup=False, command='js8call --rig-name "A B"')[0]
    startup = StationLaunchPlanner().plan_startup([profile], {1: {"launch_enabled": True, "items": [item]}}, trigger="startup")
    assert startup.instances == ()
    manual = StationLaunchPlanner().plan_startup([profile], {1: {"launch_enabled": True, "items": [dict(item, startup=True)]}}, scope_radio_id=1, trigger="manual")
    assert len(manual.instances) == 1 and manual.instances[0].launch_command_override == item["launch_command_override"]


def test_manual_and_startup_planner_outputs_are_identical_except_requested_scope():
    profile = {"id": 1, "name": "Radio", "runtime_active": 1, "display_order": 1, "js8_instance_system_key": "sys-a", "js8_instance_name": "A"}
    item = _items(startup=True, command='js8call --rig-name "A"')[0]
    bundles = {1: {"launch_enabled": True, "items": [item]}}
    startup = StationLaunchPlanner().plan_startup([profile], bundles, trigger="startup")
    manual = StationLaunchPlanner().plan_startup([profile], bundles, scope_radio_id=1, trigger="manual")
    assert startup.instances == manual.instances


def test_dependencies_and_distinct_instances_are_preserved_in_launch_plan():
    profile = {"id": 1, "name": "Radio", "runtime_active": 1, "display_order": 1, "js8_instance_system_key": "sys-a", "js8_instance_name": "A"}
    items = [
        dict(_items(name="FLDigi", path="/apps/fldigi", key="fldigi-a")[0], dependencies=["FLRig"]),
        dict(_items(name="FLRig", path="/apps/flrig", key="flrig-a")[0]),
    ]
    plan = StationLaunchPlanner().plan_startup([profile], {1: {"launch_enabled": True, "items": items}})
    assert [item.name for item in plan.instances] == ["FLRig", "FLDigi"]
    assert plan.instances[1].dependencies == ("FLRig",)
