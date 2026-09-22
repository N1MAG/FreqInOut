from __future__ import annotations

from pathlib import Path

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.software_identity_bundle import (
    SoftwareIdentityComponent,
    SoftwareIdentityRecord,
)
from freqinout.core.varac_native_preparation import prepare_managed_varac_runtime_repair
from freqinout.core.varac_runtime_repair import repair_managed_varac_wine_runtime_paths


def _legacy_managed_state(tmp_path: Path):
    drive = tmp_path / "prefix" / "drive_c"
    varac = drive / "VarAC"
    legacy_vara = tmp_path / ".freqinout" / "managed-instances" / "radio-a" / "varac-native" / "VARA"
    varac.mkdir(parents=True)
    legacy_vara.mkdir(parents=True)
    (varac / "VarAC.exe").write_bytes(b"fixture VarAC version 13.2.7")
    (varac / "VarAC.db").write_bytes(b"")
    (legacy_vara / "VARA.exe").write_bytes(b"vara")
    (legacy_vara / "VARA.ini").write_text(
        "[Setup]\r\nTCP Command Port=8300\r\nEnable KISS=1\r\nKISS Port=8302\r\n"
        "[Monitor]\r\nMonitor Mode=1\r\n",
        encoding="utf-8",
    )
    ini = varac / "VarAC.ini"
    legacy_executable = str(legacy_vara / "VARA.exe").lstrip("/").replace("/", "\\")
    ini.write_text(
        f"[OTHER]\r\nDBCustomFilePath=C:\\VarAC\\VarAC.db\r\n"
        "[VARAHF_CONFIG]\r\n"
        f"VarahfMainPath=Z:\\{legacy_executable}\r\n"
        "VarahfMainPort=8300\r\nVarahfMainHost=127.0.0.1\r\n"
        "VarahfEnableKissInterface=ON\r\nVarahfMainKissPort=8302\r\n"
        f"VarahfMonitorPath=Z:\\{legacy_executable}\r\n"
        "VarahfMonitorPort=8303\r\nVarahfLaunchOnModemConnect=OFF\r\n",
        encoding="utf-8",
    )
    store = MultiRadioStore(tmp_path / "freqinout.db")
    node = store.save_varac_node(
        {
            "system_key": "varac-radio-a",
            "name": "Radio A VarAC",
            "install_path": str(varac),
            "ini_path": str(ini),
            "db_path": str(varac / "VarAC.db"),
            "incoming_path": str(tmp_path / "mail" / "in"),
            "vara_runtime_path": str(legacy_vara),
            "vara_ini_path": str(legacy_vara / "VARA.ini"),
            "native_management_state": "managed",
            "native_writer_key": "varac:13.2.7:linux-wine:create-member",
        }
    )
    profile = store.save_device_profile(
        {
            "system_key": "radio-a",
            "name": "Radio A",
            "varac_node_id": node["id"],
            "varac_outbox_dir": str(tmp_path / "mail" / "out"),
            "varac_bbs_dir": str(tmp_path / "mail" / "BBS"),
            "varac_bbs_archive_dir": str(tmp_path / "mail" / "BBS" / "Archive"),
        }
    )
    return store, node, profile, ini, legacy_vara


def test_legacy_managed_z_runtime_is_transactionally_moved_into_wine_drive(tmp_path) -> None:
    store, node, profile, ini, legacy_vara = _legacy_managed_state(tmp_path)

    prepared = prepare_managed_varac_runtime_repair(
        node, profile, platform_override="linux-wine"
    )
    assert prepared.state == "ready"
    assert prepared.plan is not None
    assert prepared.target_runtime == tmp_path / "prefix" / "drive_c" / "VARA-radio-a"

    results = repair_managed_varac_wine_runtime_paths(
        store,
        backup_root=tmp_path / "backups",
        platform_override="linux-wine",
    )

    assert results == ({"node_id": node["id"], "state": "repaired", "detail": ""},)
    saved = store.get_varac_node(node["id"])
    assert saved["vara_runtime_path"] == str(prepared.target_runtime)
    assert saved["vara_ini_path"] == str(prepared.target_runtime / "VARA.ini")
    assert (prepared.target_runtime / "VARA.exe").read_bytes() == b"vara"
    rendered = ini.read_text(encoding="utf-8")
    assert r"VarahfMainPath=C:\VARA-radio-a\VARA.exe" in rendered
    assert "Z:\\" not in rendered
    assert legacy_vara.is_dir()  # Old data is retained; repair never deletes it.


def test_windows_does_not_run_the_linux_wine_legacy_migration(tmp_path) -> None:
    store, node, _profile, _ini, _legacy_vara = _legacy_managed_state(tmp_path)

    assert repair_managed_varac_wine_runtime_paths(
        store,
        backup_root=tmp_path / "backups",
        platform_override="windows",
    ) == ()
    assert store.get_varac_node(node["id"])["vara_runtime_path"] == node["vara_runtime_path"]


def test_running_varac_defers_automatic_runtime_repair_without_writes(tmp_path) -> None:
    store, node, _profile, ini, legacy_vara = _legacy_managed_state(tmp_path)
    original_ini = ini.read_bytes()

    results = repair_managed_varac_wine_runtime_paths(
        store,
        backup_root=tmp_path / "backups",
        process_running=True,
        platform_override="linux-wine",
    )

    assert results == (
        {"node_id": node["id"], "state": "deferred", "detail": "VarAC or VARA is running."},
    )
    assert ini.read_bytes() == original_ini
    assert store.get_varac_node(node["id"])["vara_runtime_path"] == str(legacy_vara)
    assert not (tmp_path / "prefix" / "drive_c" / "VARA-radio-a").exists()
    assert not (tmp_path / "backups").exists()


def test_correct_native_ini_reconciles_stale_fio_projection_without_copying_or_deleting(tmp_path) -> None:
    store, node, _profile, ini, legacy_vara = _legacy_managed_state(tmp_path)
    native_vara = tmp_path / "prefix" / "drive_c" / "VARA"
    native_vara.mkdir()
    (native_vara / "VARA.exe").write_bytes(b"operator vara")
    (native_vara / "VARA.ini").write_text(
        "[Setup]\r\nTCP Command Port=8300\r\nEnable KISS=1\r\nKISS Port=8302\r\n"
        "[Monitor]\r\nMonitor Mode=1\r\n",
        encoding="utf-8",
    )
    rendered = ini.read_text(encoding="utf-8")
    start = rendered.index("[VARAHF_CONFIG]")
    prefix = rendered[:start]
    ini.write_text(
        prefix
        + "[VARAHF_CONFIG]\r\n"
        + r"VarahfMainPath=C:\VARA\VARA.exe" + "\r\n"
        + "VarahfMainPort=8300\r\nVarahfMainHost=127.0.0.1\r\n"
        + "VarahfEnableKissInterface=ON\r\nVarahfMainKissPort=8302\r\n"
        + r"VarahfMonitorPath=C:\VARA\VARA.exe" + "\r\n"
        + "VarahfMonitorPort=8303\r\nVarahfLaunchOnModemConnect=OFF\r\n",
        encoding="utf-8",
    )

    results = repair_managed_varac_wine_runtime_paths(
        store,
        backup_root=tmp_path / "backups",
        platform_override="linux-wine",
    )

    assert results[0]["state"] == "reconciled"
    saved = store.get_varac_node(node["id"])
    assert saved["vara_runtime_path"] == str(native_vara)
    assert saved["vara_ini_path"] == str(native_vara / "VARA.ini")
    assert (native_vara / "VARA.exe").read_bytes() == b"operator vara"
    assert legacy_vara.is_dir()
    assert not (tmp_path / "backups").exists()


def test_runtime_repair_updates_manifest_canonical_identity_and_launch_bundle(tmp_path) -> None:
    store, node, profile, _ini, legacy_vara = _legacy_managed_state(tmp_path)
    old_executable = str(legacy_vara / "VARA.exe")
    manifest_key = "varac:varac-radio-a"
    store.save_software_instance_manifest(
        {
            "instance_key": manifest_key,
            "family_key": "varac",
            "application_system_key": node["system_key"],
            "configuration_path": node["ini_path"],
            "data_root": node["db_path"],
            "resource_claims": [
                {"kind": "vara_runtime", "value": str(legacy_vara), "exclusive": True},
                {"kind": "vara_ini", "value": str(legacy_vara / "VARA.ini"), "exclusive": True},
            ],
            "evidence": {
                "launch_recipe": {
                    "status": "qualified_managed",
                    "components": [
                        {
                            "component_key": "vara",
                            "executable": "wine",
                            "arguments": [old_executable],
                            "effective_command": ["wine", old_executable],
                            "working_directory": str(legacy_vara),
                            "environment": {"WINEPREFIX": str(tmp_path / "prefix")},
                            "dependencies": [],
                            "readiness": {"kind": "process"},
                        }
                    ],
                }
            },
        }
    )
    store.save_radio_software_identity_records(
        profile["id"],
        (
            SoftwareIdentityRecord(
                bundle_id=manifest_key,
                identity_key="radio-a:varac",
                family_key="varac",
                owner="radio-a",
                scope="standard",
                source_mode="create_distinct_instance",
                management_mode="fio_managed",
                completion_policy="required",
                provenance="managed",
                resources=(
                    {"resource_key": "vara_runtime", "resource_type": "vara_runtime", "value": str(legacy_vara), "exclusive": True},
                    {"resource_key": "vara_ini", "resource_type": "vara_ini", "value": str(legacy_vara / "VARA.ini"), "exclusive": True},
                ),
                components=(
                    SoftwareIdentityComponent(
                        component_id="vara",
                        argv=("wine", old_executable),
                        cwd=str(legacy_vara),
                        env={"WINEPREFIX": str(tmp_path / "prefix")},
                    ),
                ),
                launch={
                    "argv": {"vara": ["wine", old_executable]},
                    "cwd": {"vara": str(legacy_vara)},
                    "env": {"vara": {"WINEPREFIX": str(tmp_path / "prefix")}},
                },
            ),
        ),
        expected_generation=0,
    )
    store.save_radio_launch_bundle(
        profile["id"],
        launch_enabled=True,
        items=(
            {
                "name": "VARA",
                "instance_key": f"{manifest_key}:vara",
                "enabled": True,
                "startup": True,
                "monitor_health": True,
                "launch_path_override": "wine",
                "readiness_policy": {
                    "structured_launch": True,
                    "executable": "wine",
                    "launch_arguments": [old_executable],
                    "working_directory": str(legacy_vara),
                },
            },
        ),
    )

    result = repair_managed_varac_wine_runtime_paths(
        store,
        backup_root=tmp_path / "backups",
        platform_override="linux-wine",
    )
    assert result[0]["state"] == "repaired"
    target = tmp_path / "prefix" / "drive_c" / "VARA-radio-a"
    target_executable = str(target / "VARA.exe")

    manifest = store.get_software_instance_manifest(manifest_key)
    claims = {row["kind"]: row["value"] for row in manifest["resource_claims"]}
    assert claims["vara_runtime"] == str(target)
    assert claims["vara_ini"] == str(target / "VARA.ini")
    recipe = manifest["evidence"]["launch_recipe"]["components"][0]
    assert recipe["arguments"] == [target_executable]
    assert recipe["working_directory"] == str(target)

    canonical = store.list_radio_software_identity_records(profile["id"])[0]
    assert canonical.components[0].argv == ("wine", target_executable)
    assert canonical.components[0].cwd == str(target)
    resources = {row["resource_type"]: row["value"] for row in canonical.resources}
    assert resources["vara_runtime"] == str(target)
    assert resources["vara_ini"] == str(target / "VARA.ini")

    launch = store.get_radio_launch_bundle(profile["id"])["items"][0]
    assert launch["launch_at_startup"] == 1
    assert launch["monitor_health"] == 1
    assert launch["readiness"]["launch_arguments"] == [target_executable]
    assert launch["readiness"]["working_directory"] == str(target)
