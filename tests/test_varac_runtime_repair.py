from __future__ import annotations

from pathlib import Path

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.software_identity_bundle import (
    SoftwareIdentityComponent,
    SoftwareIdentityRecord,
)
from freqinout.core.varac_native_preparation import prepare_managed_varac_runtime_repair
from freqinout.core.varac_runtime_repair import (
    managed_varac_process_attribution,
    repair_managed_varac_cluster_launch_policy,
    repair_managed_varac_wine_runtime_paths,
    running_managed_varac_node_ids,
)


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


def _clustered_managed_state(tmp_path: Path):
    store, node, profile, ini, legacy_vara = _legacy_managed_state(tmp_path)
    cluster = store.save_varac_cluster(
        {
            "name": "Field Cluster",
            "cluster_id": "FIELD",
            "native_management_state": "managed",
        }
    )
    store.set_varac_cluster_member(
        cluster["id"],
        profile["id"],
        instance_number=1,
        enabled=True,
    )
    return store, node, profile, ini, legacy_vara


def test_managed_cluster_launch_policy_repairs_only_vara_autostart_key(tmp_path) -> None:
    store, node, _profile, ini, _legacy_vara = _clustered_managed_state(tmp_path)
    before = ini.read_text(encoding="utf-8")

    result = repair_managed_varac_cluster_launch_policy(
        store,
        backup_root=tmp_path / "backups",
        platform_override="linux-wine",
    )

    assert result == (
        {
            "node_id": node["id"],
            "state": "launch-policy-repaired",
            "detail": "VarAC modem launch enabled and verified.",
        },
    )
    after = ini.read_text(encoding="utf-8")
    assert "VarahfLaunchOnModemConnect=OFF" in before
    assert "VarahfLaunchOnModemConnect=ON" in after
    assert after.replace(
        "VarahfLaunchOnModemConnect=ON",
        "VarahfLaunchOnModemConnect=OFF",
    ) == before
    assert "launches its node-local VARA modem" in store.get_varac_node(
        node["id"]
    )["native_verification_summary"]


def test_windows_managed_cluster_launch_policy_uses_same_qualified_repair(tmp_path) -> None:
    store, node, _profile, ini, _legacy_vara = _clustered_managed_state(tmp_path)
    saved = store.get_varac_node(node["id"])
    store.save_varac_node(
        {
            **saved,
            "native_writer_key": "varac:13.2.7:windows:create-member",
        }
    )

    result = repair_managed_varac_cluster_launch_policy(
        store,
        backup_root=tmp_path / "backups",
        platform_override="windows",
    )

    assert result[0]["state"] == "launch-policy-repaired"
    assert "VarahfLaunchOnModemConnect=ON" in ini.read_text(encoding="utf-8")


def test_running_cluster_member_defers_launch_policy_repair_without_writes(tmp_path) -> None:
    store, node, _profile, ini, _legacy_vara = _clustered_managed_state(tmp_path)
    before = ini.read_bytes()

    result = repair_managed_varac_cluster_launch_policy(
        store,
        backup_root=tmp_path / "backups",
        running_node_ids=(node["id"],),
        platform_override="linux-wine",
    )

    assert result[0]["state"] == "deferred"
    assert ini.read_bytes() == before
    assert not (tmp_path / "backups").exists()


def test_exact_running_node_detection_does_not_defer_idle_cluster_sibling(tmp_path) -> None:
    store, node, profile, _ini, _legacy_vara = _clustered_managed_state(tmp_path)
    store.save_radio_launch_bundle(
        profile["id"],
        launch_enabled=True,
        items=(
            {
                "name": "VarAC",
                "instance_key": "varac:radio-a:varac",
                "enabled": True,
                "startup": True,
                "monitor_health": True,
                "launch_path_override": "wine",
                "dependencies": (),
                "readiness_policy": {
                    "structured_launch": True,
                    "executable": "wine",
                    "launch_arguments": [
                        str(Path(node["install_path"]) / "VarAC.exe"),
                        r"C:\VarAC\VarAC.ini",
                    ],
                },
            },
        ),
    )

    class _Status:
        def program_instance_running(self, name, target, arguments):
            assert name == "VarAC"
            assert target == "wine"
            assert arguments[-1] == r"C:\VarAC\VarAC.ini"
            return True

        def cached_program_process_count(self, name):
            return 1 if name == "VarAC" else 0

    assert running_managed_varac_node_ids(store, _Status()) == (node["id"],)


def test_cluster_shared_database_does_not_abort_process_attribution(tmp_path) -> None:
    store, first_node, first_profile, _ini, _legacy_vara = _clustered_managed_state(
        tmp_path
    )
    second_root = tmp_path / "prefix" / "drive_c" / "VarAC-FT710"
    second_root.mkdir(parents=True)
    second_node = store.save_varac_node(
        {
            "system_key": "varac-radio-b",
            "name": "Radio B VarAC",
            "install_path": str(second_root),
            "ini_path": str(second_root / "VarAC-ft-710.ini"),
            # Cluster members intentionally share this database.
            "db_path": first_node["db_path"],
            "incoming_path": str(tmp_path / "mail" / "radio-b-in"),
            "outbox_path": str(tmp_path / "mail" / "radio-b-out"),
            "vara_runtime_path": str(tmp_path / "prefix" / "drive_c" / "VARA-radio-b"),
            "vara_ini_path": str(
                tmp_path / "prefix" / "drive_c" / "VARA-radio-b" / "VARA.ini"
            ),
            "native_management_state": "managed",
            "native_writer_key": "varac:13.2.7:linux-wine:create-member",
        }
    )
    second_profile = store.save_device_profile(
        {
            "system_key": "radio-b",
            "name": "Radio B",
            "varac_node_id": second_node["id"],
            "varac_outbox_dir": str(tmp_path / "mail" / "radio-b-out"),
        }
    )
    membership = store.list_varac_cluster_members(
        device_profile_id=first_profile["id"]
    )[0]
    store.set_varac_cluster_member(
        membership["cluster_id"],
        second_profile["id"],
        instance_number=2,
        enabled=True,
    )

    first_arguments = [
        str(Path(first_node["install_path"]) / "VarAC.exe"),
        r"C:\VarAC\VarAC.ini",
    ]
    second_arguments = [
        str(Path(second_node["install_path"]) / "VarAC.exe"),
        r"C:\VarAC-FT710\VarAC-ft-710.ini",
    ]
    for profile, key, arguments in (
        (first_profile, "radio-a", first_arguments),
        (second_profile, "radio-b", second_arguments),
    ):
        store.save_radio_launch_bundle(
            profile["id"],
            launch_enabled=True,
            items=(
                {
                    "name": "VarAC",
                    "instance_key": f"varac:{key}:varac",
                    "enabled": True,
                    "startup": True,
                    "monitor_health": True,
                    "launch_path_override": "wine",
                    "dependencies": (),
                    "readiness_policy": {
                        "structured_launch": True,
                        "executable": "wine",
                        "launch_arguments": arguments,
                    },
                },
            ),
        )

    class _Status:
        def program_instance_running(self, name, target, arguments):
            assert name == "VarAC"
            assert target == "wine"
            return list(arguments) == first_arguments

        def cached_program_process_count(self, name):
            return 1 if name == "VarAC" else 0

    attribution = managed_varac_process_attribution(store, _Status())

    assert attribution["catalog_complete"] is True
    assert attribution["complete"] is True
    assert attribution["running_node_ids"] == (first_node["id"],)
    assert attribution["attributed_process_count"] == 1


def test_unknown_varac_process_keeps_automatic_policy_repair_fail_closed(tmp_path) -> None:
    store, _node, profile, _ini, _legacy_vara = _clustered_managed_state(tmp_path)
    store.save_radio_launch_bundle(
        profile["id"],
        launch_enabled=True,
        items=(
            {
                "name": "VarAC",
                "instance_key": "varac:radio-a:varac",
                "enabled": True,
                "startup": True,
                "monitor_health": True,
                "launch_path_override": "wine",
                "dependencies": (),
                "readiness_policy": {
                    "structured_launch": True,
                    "executable": "wine",
                    "launch_arguments": ["/managed/VarAC.exe", r"C:\VarAC\VarAC.ini"],
                },
            },
        ),
    )

    class _Status:
        def program_instance_running(self, _name, _target, _arguments):
            return False

        def cached_program_process_count(self, name):
            return 1 if name == "VarAC" else 0

    attribution = managed_varac_process_attribution(store, _Status())

    assert attribution["running_node_ids"] == ()
    assert attribution["observed_process_count"] == 1
    assert attribution["attributed_process_count"] == 0
    assert attribution["complete"] is False


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
