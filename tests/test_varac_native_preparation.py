from __future__ import annotations

from pathlib import Path

from freqinout.core.guided_instance_inventory import build_guided_instance_inventory
from freqinout.core.guided_varac_configuration import (
    recommend_varac_arrangement_from_snapshots,
)
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.varac_native_preparation import (
    native_draft_fingerprint,
    prepare_varac_native_configuration,
)


def _evidence(tmp_path: Path):
    varac_root = tmp_path / "VarAC"
    vara_root = tmp_path / "VARA"
    varac_root.mkdir()
    vara_root.mkdir()
    executable = varac_root / "VarAC.exe"
    executable.write_bytes(b"fixture VarAC version 13.2.7")
    (vara_root / "VARA.exe").write_bytes(b"fixture VARA executable")
    (vara_root / "VARA.ini").write_text(
        "[Setup]\r\nTCP Command Port=8300\r\nEnable KISS=1\r\nKISS Port=8302\r\n"
        "[Monitor]\r\nMonitor Mode=1\r\n",
        encoding="utf-8",
    )
    ini = varac_root / "VarAC.ini"
    ini.write_text(
        f"[OTHER]\r\nDBCustomFilePath={varac_root / 'VarAC.db'}\r\n"
        "[VARAHF_CONFIG]\r\n"
        f"VarahfMainPath={vara_root / 'VARA.exe'}\r\n"
        "VarahfMainPort=8300\r\nVarahfMainHost=127.0.0.1\r\n"
        "VarahfEnableKissInterface=ON\r\nVarahfMainKissPort=8302\r\n"
        f"VarahfMonitorPath={vara_root / 'VARA.exe'}\r\n"
        "VarahfMonitorPort=8303\r\nVarahfLaunchOnModemConnect=OFF\r\n",
        encoding="utf-8",
    )
    node = {
        "id": 11,
        "name": "Existing VarAC",
        "install_path": str(varac_root),
        "ini_path": str(ini),
        "db_path": str(varac_root / "VarAC.db"),
        "vara_runtime_path": str(vara_root),
    }
    profile = {"id": 5, "name": "Existing Radio", "varac_node_id": 11, "device_class": "tx_rx"}
    return node, profile


def _draft() -> dict[str, object]:
    return {
        "family_key": "varac",
        "mode": "managed",
        "instance_name": "New Radio",
        "owner_label": "New Radio",
        "draft_instance_key": "draft-new-radio",
        "cluster_path": "create_cluster",
        "cluster_name": "Field Cluster",
        "cluster_id": "FIELD",
        "cluster_instance_number": 2,
        "existing_standalone_node_id": 11,
        "existing_standalone_member_number": 1,
        "email_gateway_sender_choice": "existing_member",
        "cluster_ptt_lock": True,
    }


def test_prepare_existing_standalone_and_new_member_is_immutable_and_ready(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    draft = _draft()
    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=7,
        platform_override="linux-wine",
    )
    assert result.ready
    assert result.draft_fingerprint == native_draft_fingerprint(draft)
    assert result.presentation["generation"] == 7
    assert result.presentation["state"] == "ready"
    assert result.plan is not None and len(result.plan.members) == 2
    assert result.plan.email_gateway_sender_member_id == "node:11"
    assert result.plan.members[0].target_path == Path(node["ini_path"])
    assert result.plan.members[1].target_path == Path(node["install_path"]) / "VarAC-new-radio.ini"
    assert result.plan.members[0].vara_target_runtime_folder != result.plan.members[1].vara_target_runtime_folder
    assert result.plan.members[0].vara_changes["Setup"]["TCP Command Port"] == "8300"
    assert result.plan.members[1].vara_changes["Setup"]["TCP Command Port"] == "8310"
    assert result.plan.native_shared_db_path.startswith("Z:\\")
    assert (
        result.plan.members[1].changes["OTHER"]["DBCustomFilePath"]
        == result.plan.native_shared_db_path
    )
    assert result.plan.members[1].changes["VARAHF_CONFIG"]["VarahfMainPath"].startswith(
        "Z:\\"
    )
    assert result.plan.members[1].launch_command[2].startswith("Z:\\")
    member = result.plan.members[1]
    assert result.presentation["application_path"] == str(
        Path(node["install_path"]) / "VarAC.exe"
    )
    assert result.presentation["launch_argv"] == result.plan.members[1].launch_command
    assert result.presentation["working_directory"] == str(Path(node["install_path"]))
    assert result.presentation["configuration_path"] == str(member.target_path)
    assert result.presentation["storage_path"] == str(result.plan.shared_db_path)
    assert result.presentation["secondary_storage_path"].endswith("/incoming")
    assert result.presentation["outbox_path"].endswith("/outbox")
    assert result.presentation["secondary_storage_path"] != result.presentation["outbox_path"]
    assert result.presentation["working_directory"] == str(member.working_directory)
    assert result.presentation["vara_runtime_path"] == str(member.vara_target_runtime_folder)
    assert result.presentation["vara_ini_path"] == str(member.vara_target_path)
    assert result.presentation["port"] == 8310
    assert result.presentation["secondary_port"] == 8312
    assert not result.plan.members[1].target_path.exists()


def test_prepare_create_cluster_recovers_unique_linked_standalone_when_ui_id_is_absent(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    draft = _draft()
    draft.pop("existing_standalone_node_id")

    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=8,
        platform_override="linux-wine",
    )

    assert result.ready
    assert result.plan is not None
    assert result.plan.members[0].member_id == "node:11"
    assert result.plan.members[0].target_path == Path(node["ini_path"])
    assert result.presentation["configuration_path"] == str(
        Path(node["install_path"]) / "VarAC-new-radio.ini"
    )
    assert result.presentation["storage_path"] == node["db_path"]


def test_prepare_create_cluster_does_not_replace_explicit_stale_node_id(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    draft = _draft()
    draft["existing_standalone_node_id"] = 999

    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=9,
        platform_override="linux-wine",
    )

    assert not result.ready
    assert "no longer available" in result.error
    assert "refresh discovery" not in result.error.casefold()


def test_prepare_create_cluster_requires_choice_when_multiple_linked_standalones_exist(tmp_path) -> None:
    first_node, first_profile = _evidence(tmp_path)
    second_root = tmp_path / "SecondVarAC"
    second_root.mkdir()
    second_ini = second_root / "VarAC.ini"
    second_ini.write_bytes(Path(first_node["ini_path"]).read_bytes())
    second_node = {
        **first_node,
        "id": 12,
        "name": "Second VarAC",
        "install_path": str(second_root),
        "ini_path": str(second_ini),
    }
    second_profile = {
        **first_profile,
        "id": 6,
        "name": "Second Radio",
        "varac_node_id": 12,
    }
    draft = _draft()
    draft.pop("existing_standalone_node_id")

    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(first_node, second_node),
        device_profiles=(first_profile, second_profile),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=10,
        platform_override="linux-wine",
    )

    assert not result.ready
    assert "More than one standalone VarAC node" in result.error
    assert "Choose the node in the VarAC arrangement" in result.error


def test_real_store_incomplete_linked_node_reaches_native_preparation_without_ui_id(tmp_path) -> None:
    evidence_node, _evidence_profile = _evidence(tmp_path)
    store = MultiRadioStore(tmp_path / "freqinout.db")
    radio = store.save_device_profile(
        {"system_key": "existing-radio", "name": "Existing Radio", "enabled": 1}
    )
    adoption = store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=radio["id"],
        application_values={
            "system_key": "existing-varac",
            "name": "Existing VarAC",
            "install_path": evidence_node["install_path"],
            "ini_path": evidence_node["ini_path"],
            "db_path": evidence_node["db_path"],
            "vara_runtime_path": evidence_node["vara_runtime_path"],
            # Deliberately incomplete for inventory classification. The durable
            # radio link still proves topology; native preparation owns path
            # qualification and derives the new member's mailbox paths.
            "incoming_path": "",
        },
        manifest_values={"instance_key": "varac:existing-varac"},
    )
    saved_node_id = int(adoption["application"]["id"])
    profiles = store.list_device_profiles()
    snapshot = build_guided_instance_inventory(
        {"varac": store.list_varac_nodes()},
        linked_ids_by_family={"varac": {saved_node_id}},
    )
    classified = snapshot.rows_for("varac")[0]
    assert classified["candidate_usable"] is False
    assert classified["linked_to_radio"] is True

    arrangement = recommend_varac_arrangement_from_snapshots(
        snapshot.rows_for("varac"),
        store.list_varac_clusters(),
        store.list_varac_cluster_members(),
        profiles,
        new_radio_label="New Radio",
    )
    assert arrangement["recommended_existing_node_id"] == saved_node_id

    draft = _draft()
    draft.pop("existing_standalone_node_id")
    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=store.list_varac_nodes(),
        device_profiles=profiles,
        varac_clusters=store.list_varac_clusters(),
        varac_members=store.list_varac_cluster_members(),
        managed_root=tmp_path / "managed",
        generation=11,
        platform_override="linux-wine",
    )

    assert result.ready
    assert result.plan is not None
    assert result.plan.members[0].member_id == f"node:{saved_node_id}"
    assert result.presentation["application_path"]
    assert result.presentation["configuration_path"]
    assert result.presentation["storage_path"]
    assert result.presentation["secondary_storage_path"]
    assert result.presentation["outbox_path"]
    assert result.presentation["launch_command"]


def test_create_cluster_inherits_shared_bbs_paths_and_keeps_member_mail_paths_local(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    profile.update(
        varac_bbs_dir=str(tmp_path / "existing-bbs"),
        varac_bbs_archive_dir=str(tmp_path / "existing-bbs-archive"),
    )
    result = prepare_varac_native_configuration(
        _draft(),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=8,
        platform_override="linux-wine",
    )
    assert result.ready
    assert result.presentation["bbs_path"] == profile["varac_bbs_dir"]
    assert result.presentation["bbs_archive_path"] == profile["varac_bbs_archive_dir"]
    assert result.presentation["secondary_storage_path"] != result.presentation["outbox_path"]
    assert str(tmp_path / "existing-bbs") not in result.presentation["secondary_storage_path"]
    assert str(tmp_path / "existing-bbs") not in result.presentation["outbox_path"]


def test_create_cluster_places_new_mailboxes_beside_reviewed_existing_mailboxes(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    station_data = tmp_path / "wine" / "drive_c" / "users" / "bill" / "Desktop" / "VaraFiles"
    profile.update(
        varac_incoming_path=str(station_data / "FTDX-10_In"),
        varac_outbox_dir=str(station_data / "FTDX-10_Out"),
        varac_bbs_dir=str(station_data / "BBS"),
        varac_bbs_archive_dir=str(station_data / "BBS" / "Archive"),
    )
    draft = _draft()
    draft["instance_name"] = "FT-710"
    draft["owner_label"] = "FT-710"

    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=13,
        platform_override="linux-wine",
    )

    assert result.ready
    assert result.presentation["secondary_storage_path"] == str(station_data / "FT-710_In")
    assert result.presentation["outbox_path"] == str(station_data / "FT-710_Out")
    assert result.presentation["bbs_path"] == str(station_data / "BBS")


def test_windows_preparation_uses_the_reviewed_member_mailbox_parent(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    station_data = tmp_path / "VaraFiles"
    profile.update(
        varac_incoming_path=str(station_data / "Existing_In"),
        varac_outbox_dir=str(station_data / "Existing_Out"),
    )
    draft = _draft()
    draft["instance_name"] = "Portable Radio"
    draft["owner_label"] = "Portable Radio"

    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=17,
        platform_override="windows",
    )

    assert result.ready
    assert result.plan is not None and result.plan.platform == "windows"
    assert result.presentation["secondary_storage_path"] == str(
        station_data / "Portable-Radio_In"
    )
    assert result.presentation["outbox_path"] == str(station_data / "Portable-Radio_Out")


def test_automatic_member_mailboxes_avoid_reviewed_profile_collisions(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    station_data = tmp_path / "VaraFiles"
    profile.update(
        varac_incoming_path=str(station_data / "FTDX-10_In"),
        varac_outbox_dir=str(station_data / "FTDX-10_Out"),
    )
    occupied = {
        "id": 8,
        "name": "Earlier FT-710",
        "varac_incoming_path": str(station_data / "FT-710_In"),
        "varac_outbox_dir": str(station_data / "FT-710_Out"),
    }
    draft = _draft()
    draft["instance_name"] = "FT-710"
    draft["owner_label"] = "FT-710"

    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile, occupied),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=14,
        platform_override="linux-wine",
    )

    assert result.ready
    assert result.presentation["secondary_storage_path"] == str(station_data / "FT-710-2_In")
    assert result.presentation["outbox_path"] == str(station_data / "FT-710-2_Out")


def test_reprepare_replaces_older_generated_mailboxes_but_keeps_operator_correction(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    station_data = tmp_path / "VaraFiles"
    profile.update(
        varac_incoming_path=str(station_data / "FTDX-10_In"),
        varac_outbox_dir=str(station_data / "FTDX-10_Out"),
    )
    draft = _draft()
    old_incoming = str(tmp_path / "managed" / "new-radio" / "varac-native" / "incoming")
    old_outbox = str(tmp_path / "managed" / "new-radio" / "varac-native" / "outbox")
    draft.update(
        secondary_storage_path=old_incoming,
        outbox_path=old_outbox,
        varac_native_presentation={
            "secondary_storage_path": old_incoming,
            "outbox_path": old_outbox,
        },
    )

    regenerated = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=15,
        platform_override="linux-wine",
    )
    assert regenerated.ready
    assert regenerated.presentation["secondary_storage_path"] == str(station_data / "New-Radio_In")
    assert regenerated.presentation["outbox_path"] == str(station_data / "New-Radio_Out")

    corrected = dict(draft)
    corrected["secondary_storage_path"] = str(station_data / "Portable_In")
    corrected_result = prepare_varac_native_configuration(
        corrected,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=16,
        platform_override="linux-wine",
    )
    assert corrected_result.ready
    assert corrected_result.presentation["secondary_storage_path"] == str(
        station_data / "Portable_In"
    )


def test_create_cluster_accepts_bbs_below_stable_wine_desktop_alias(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    host_desktop = tmp_path / "home" / "bill" / "Desktop"
    host_desktop.mkdir(parents=True)
    wine_user = tmp_path / "prefix" / "drive_c" / "users" / "bill"
    wine_user.mkdir(parents=True)
    desktop_alias = wine_user / "Desktop"
    desktop_alias.symlink_to(host_desktop, target_is_directory=True)
    bbs = desktop_alias / "VaraFile" / "BBS"
    archive = bbs / "Archive"
    profile.update(
        varac_incoming_path=str(desktop_alias / "VaraFile" / "FTDX-10_In"),
        varac_outbox_dir=str(desktop_alias / "VaraFile" / "FTDX-10_Out"),
        varac_bbs_dir=str(bbs),
        varac_bbs_archive_dir=str(archive),
    )

    result = prepare_varac_native_configuration(
        _draft(),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=12,
        platform_override="linux-wine",
    )

    assert result.ready
    assert result.presentation["bbs_path"] == str(bbs)
    assert result.presentation["bbs_archive_path"] == str(archive)
    assert result.presentation["secondary_storage_path"] == str(
        desktop_alias / "VaraFile" / "New-Radio_In"
    )
    assert result.presentation["outbox_path"] == str(
        desktop_alias / "VaraFile" / "New-Radio_Out"
    )
    assert result.plan is not None
    assert result.plan.managed_directory_resolved_paths[:2] == (
        host_desktop / "VaraFile" / "New-Radio_In",
        host_desktop / "VaraFile" / "New-Radio_Out",
    )
    assert result.plan.managed_directory_resolved_paths[-2:] == (
        host_desktop / "VaraFile" / "BBS",
        host_desktop / "VaraFile" / "BBS" / "Archive",
    )


def test_create_cluster_derives_shared_bbs_defaults_under_varac_install(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    result = prepare_varac_native_configuration(
        _draft(),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=9,
        platform_override="linux-wine",
    )
    assert result.ready
    assert result.presentation["bbs_path"] == str(Path(node["install_path"]) / "BBS")
    assert result.presentation["bbs_archive_path"] == str(Path(node["install_path"]) / "BBS" / "Archive")


def test_prepare_rejects_member_mailbox_overlap_with_cluster_shared_bbs(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    draft = _draft()
    draft["bbs_path"] = str(tmp_path / "shared-bbs")
    draft["secondary_storage_path"] = str(tmp_path / "shared-bbs" / "incoming")

    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=10,
        platform_override="linux-wine",
    )

    assert not result.ready
    assert "node-local" in result.error
    assert "cluster-shared BBS" in result.error


def test_join_cluster_inherits_durable_shared_bbs_paths(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    member_data = tmp_path / "VaraFiles"
    profile.update(
        varac_incoming_path=str(member_data / "Existing-Radio_In"),
        varac_outbox_dir=str(member_data / "Existing-Radio_Out"),
    )
    draft = _draft()
    draft.update(
        cluster_path="join_cluster",
        cluster_id="FIELD",
        email_gateway_sender_choice="none",
    )
    draft.pop("existing_standalone_node_id")
    draft.pop("existing_standalone_member_number")
    shared_db = str(tmp_path / "shared" / "VarAC.db")
    bbs = str(tmp_path / "shared" / "BBS")
    archive = str(tmp_path / "shared" / "BBS" / "Archive")

    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(
            {
                "id": 19,
                "name": "Field Cluster",
                "cluster_id": "FIELD",
                "shared_db_path": shared_db,
                "shared_bbs_path": bbs,
                "shared_bbs_archive_path": archive,
            },
        ),
        varac_members=(
            {"cluster_id": 19, "device_profile_id": 5, "enabled": 1},
        ),
        managed_root=tmp_path / "managed",
        generation=11,
        platform_override="linux-wine",
    )

    assert result.ready
    assert result.presentation["storage_path"] == shared_db
    assert result.presentation["bbs_path"] == bbs
    assert result.presentation["bbs_archive_path"] == archive
    assert result.presentation["secondary_storage_path"] != bbs
    assert result.presentation["outbox_path"] != archive
    assert result.presentation["secondary_storage_path"] == str(member_data / "New-Radio_In")
    assert result.presentation["outbox_path"] == str(member_data / "New-Radio_Out")


def test_prepare_warns_for_running_process_and_still_requires_exact_qualified_version(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    running = prepare_varac_native_configuration(
        _draft(),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=2,
        platform_override="linux-wine",
        process_running=True,
    )
    assert running.ready
    assert running.presentation["apply_requires_stopped_process"] is True
    assert "close both applications before final Save" in running.presentation["why"]

    Path(node["install_path"]).joinpath("VarAC.exe").write_bytes(b"fixture VarAC version 15.0.18")
    unsupported = prepare_varac_native_configuration(
        _draft(),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=3,
        platform_override="linux-wine",
    )
    assert unsupported.state == "manual setup required"
    assert unsupported.plan is None


def test_native_fingerprint_ignores_host_presentation_but_not_operator_intent() -> None:
    draft = _draft()
    first = native_draft_fingerprint(draft)
    draft["varac_native_presentation"] = {"state": "preparing"}
    draft["varac_native_generation"] = 99
    assert native_draft_fingerprint(draft) == first
    draft.update(
        configuration_path="/managed/VarAC.ini",
        storage_path="/managed/cluster.db",
        secondary_storage_path="/managed/incoming",
        outbox_path="/managed/outbox",
        working_directory="/managed",
        launch_command="wine VarAC.exe Z:\\managed\\VarAC.ini",
        vara_runtime_path="/managed/VARA",
        vara_ini_path="/managed/VARA/VARA.ini",
        port=8310,
        secondary_port=8312,
    )
    assert native_draft_fingerprint(draft) == first
    draft["cluster_instance_number"] = 3
    assert native_draft_fingerprint(draft) != first
