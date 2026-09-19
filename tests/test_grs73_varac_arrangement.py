"""GRS-7.3 pure VarAC arrangement and atomic store gates."""

import pytest

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.guided_varac_configuration import (
    VarACClusterIdentity,
    VarACClusterMembership,
    VarACNodeIdentity,
    VarACPlanningInventory,
    VarACClusterPath,
    recommend_varac_arrangement,
    recommend_varac_arrangement_from_snapshots,
)


def _node(key: str, radio: str, *, display_name: str = "") -> VarACNodeIdentity:
    root = f"/varac/{key}"
    return VarACNodeIdentity(
        node_key=key,
        radio_key=radio,
        install_path=f"{root}/install",
        launch_command=f"/opt/varac --profile {key}",
        working_directory=f"{root}/work",
        ini_path=f"{root}/VarAC.ini",
        database_path=f"{root}/VarAC.db",
        incoming_path=f"{root}/incoming",
        outbox_path=f"{root}/outbox",
        display_name=display_name,
    )


def test_fresh_inventory_defaults_standalone_without_mutating_recommendation():
    recommendation = recommend_varac_arrangement(VarACPlanningInventory())
    assert recommendation.default_path is VarACClusterPath.STANDALONE
    assert recommendation.recommended_path is None
    assert recommendation.recommended_existing_node_key == ""
    assert recommendation.join_choices == ()
    assert recommendation.requires_explicit_selection is False


def test_existing_cluster_is_named_join_choice_with_next_enabled_member_number():
    cluster = VarACClusterIdentity("Front Range")
    inventory = VarACPlanningInventory(
        nodes=(_node("one", "radio-one"), _node("two", "radio-two")),
        clusters=(cluster,),
        memberships=(
            VarACClusterMembership("front range", "one", "radio-one", 1),
            VarACClusterMembership("front range", "two", "radio-two", 3),
            VarACClusterMembership("front range", "retired", "radio-old", 2, enabled=False),
        ),
    )
    recommendation = recommend_varac_arrangement(inventory)
    assert recommendation.default_path is VarACClusterPath.STANDALONE
    assert recommendation.recommended_path is None
    assert recommendation.join_choices[0].cluster_id == "front range"
    assert recommendation.join_choices[0].label == "Front Range"
    assert recommendation.join_choices[0].next_instance_number == 2
    assert recommendation.standalone_node_keys == ()


def test_one_standalone_node_recommends_create_cluster_but_does_not_select_it():
    inventory = VarACPlanningInventory(nodes=(_node("existing", "radio-existing", display_name="Existing Station"),))
    recommendation = recommend_varac_arrangement(inventory)
    assert recommendation.default_path is None
    assert recommendation.recommended_path is VarACClusterPath.CREATE_CLUSTER
    assert recommendation.recommended_existing_node_key == "existing"
    assert recommendation.create_choice_label == "Create a cluster using Existing Station"
    assert recommendation.existing_setup_summary == "One standalone VarAC node is configured: Existing Station."
    assert recommendation.standalone_node_choices == (("existing", "Existing Station"),)
    assert recommendation.requires_explicit_selection is True
    assert recommendation.join_choices == ()


def test_multiple_standalone_nodes_require_explicit_existing_node_selection():
    inventory = VarACPlanningInventory(
        nodes=(_node("zulu", "radio-z"), _node("alpha", "radio-a")),
    )
    recommendation = recommend_varac_arrangement(inventory)
    assert recommendation.default_path is None
    assert recommendation.recommended_path is VarACClusterPath.CREATE_CLUSTER
    assert recommendation.recommended_existing_node_key == ""
    assert recommendation.requires_explicit_selection is True
    assert recommendation.standalone_node_keys == ("alpha", "zulu")


def test_snapshot_adapter_emits_display_ready_ids_and_exact_single_standalone_copy():
    result = recommend_varac_arrangement_from_snapshots(
        (
            {
                "id": 41,
                "system_key": "ftdx-10-varac",
                "name": "FTDX-10 VarAC",
                "candidate_classification": "usable_existing",
                "candidate_usable": True,
            },
            {
                "id": 42,
                "system_key": "legacy-varac",
                "name": "Legacy diagnostic",
                "candidate_classification": "diagnostic_only",
                "candidate_usable": False,
                "device_profile_id": 8,
            },
        ),
        (),
        (),
        ({"id": 7, "name": "FTDX-10", "varac_node_id": 41},),
        new_radio_label="New Radio",
    )
    assert result["recommended_existing_node_id"] == 41
    assert result["recommended_existing_device_profile_id"] == 7
    assert result["existing_member_instance_number"] == 1
    assert result["new_member_instance_number"] == 2
    assert result["default_path"] == ""
    assert result["requires_explicit_selection"] is True
    assert result["existing_setup_summary"] == "Existing setup: FTDX-10 VarAC is standalone. No VarAC cluster is configured."
    assert result["create_choice_label"] == "Create a cluster with FTDX-10 VarAC and New Radio — Recommended"
    assert result["proposed_create_cluster_name"] == "FTDX-10 + New Radio VarAC"
    assert result["proposed_create_cluster_id"] == "VARAC-FTDX-10-NEW-RADIO"
    assert tuple(result["standalone_candidates"])[0]["node_id"] == 41
    assert tuple(result["standalone_candidates"])[0]["proposed_cluster_id"] == "VARAC-FTDX-10-NEW-RADIO"
    assert len(result["standalone_candidates"]) == 1


def test_snapshot_adapter_generates_collision_free_first_cluster_identity():
    result = recommend_varac_arrangement_from_snapshots(
        (),
        ({"id": 9, "cluster_id": "VARAC-TRIMODE", "name": "Retained cluster"},),
        (),
        new_radio_label="TriMode",
    )
    assert result["proposed_create_cluster_name"] == "TriMode VarAC"
    assert result["proposed_create_cluster_id"] == "VARAC-TRIMODE-2"


def test_snapshot_adapter_excludes_diagnostics_and_requires_choice_for_ambiguous_standalones():
    result = recommend_varac_arrangement_from_snapshots(
        (
            {"id": 1, "system_key": "a", "name": "Station A", "candidate_classification": "usable_existing", "candidate_usable": True, "device_profile_id": 11},
            {"id": 2, "system_key": "b", "name": "Station B", "candidate_classification": "usable_existing", "candidate_usable": True, "device_profile_id": 12},
            {"id": 3, "system_key": "diag", "name": "Diagnostic", "candidate_classification": "diagnostic_only", "candidate_usable": False, "device_profile_id": 13},
        ),
        (),
        (),
        new_radio_label="New Radio",
    )
    assert result["default_path"] == ""
    assert result["recommended_path"] == "create_cluster"
    assert result["recommended_existing_node_id"] is None
    assert result["existing_member_instance_number"] == 1
    assert result["new_member_instance_number"] == 2
    assert result["needs_attention"] is True
    assert result["ambiguity"] is True
    assert [row["node_id"] for row in result["standalone_candidates"]] == [1, 2]


def test_snapshot_adapter_provides_named_join_choice_and_next_number():
    result = recommend_varac_arrangement_from_snapshots(
        ({"id": 1, "system_key": "new", "name": "New", "candidate_classification": "usable_existing", "candidate_usable": True, "device_profile_id": 11},),
        ({"id": 9, "cluster_id": "Relief", "name": "Relief Cluster"},),
        (
            {"cluster_id": 9, "device_profile_id": 21, "instance_number": 1, "enabled": 1},
            {"cluster_id": 9, "device_profile_id": 22, "instance_number": 3, "enabled": 1},
            {"cluster_id": 9, "device_profile_id": 23, "instance_number": 2, "enabled": 0},
        ),
    )
    assert result["default_path"] == "standalone"
    assert result["recommended_path"] == ""
    assert result["existing_member_instance_number"] is None
    assert result["new_member_instance_number"] == 1
    assert tuple(result["join_choices"])[0]["cluster_db_id"] == 9
    assert tuple(result["join_choices"])[0]["next_instance_number"] == 2


def _save_standalone(store: MultiRadioStore, radio_key: str, node_key: str) -> tuple[dict, dict]:
    radio = store.save_device_profile({"system_key": radio_key, "name": radio_key.title()})
    result = store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=radio["id"],
        application_values={
            "system_key": node_key,
            "name": node_key.title(),
            "install_path": f"/opt/{node_key}",
            "ini_path": f"/varac/{node_key}.ini",
            "db_path": f"/varac/{node_key}.db",
            "incoming_path": f"/varac/{node_key}/incoming",
            "launch_cmd": f"/opt/{node_key} --node {node_key}",
        },
        manifest_values={"instance_key": f"varac:{node_key}"},
    )
    return radio, result["application"]


def _new_varac_values(node_key: str) -> tuple[dict, dict]:
    return (
        {
            "system_key": node_key,
            "name": node_key.title(),
            "install_path": f"/opt/{node_key}",
            "ini_path": f"/varac/{node_key}.ini",
            "db_path": f"/varac/{node_key}.db",
            "incoming_path": f"/varac/{node_key}/incoming",
            "launch_cmd": f"/opt/{node_key} --node {node_key}",
        },
        {"instance_key": f"varac:{node_key}"},
    )


def test_create_cluster_with_existing_standalone_commits_both_members_and_gateway_atomically(tmp_path):
    store = MultiRadioStore(tmp_path / "varac-existing-standalone.db")
    old_radio, old_node = _save_standalone(store, "radio-old", "varac-old")
    new_radio = store.save_device_profile({"system_key": "radio-new", "name": "Radio New"})
    app_values, manifest_values = _new_varac_values("varac-new")
    result = store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=new_radio["id"],
        application_values=app_values,
        manifest_values=manifest_values,
        varac_create_cluster_values={
            "name": "Relief Cluster",
            "cluster_id": "relief",
            "shared_db_path": "/varac/shared/relief.db",
            "shared_bbs_path": "/varac/shared/bbs",
            "shared_bbs_archive_path": "/varac/shared/bbs/archive",
            "ptt_lock_enabled": True,
            "existing_standalone_node_id": old_node["id"],
            "existing_standalone_instance_number": 1,
            "gateway_existing_standalone": True,
        },
        varac_cluster_instance_number=2,
        launch_at_startup=True,
    )

    clusters = store.list_varac_clusters()
    assert len(clusters) == 1
    cluster = clusters[0]
    assert cluster["cluster_id"] == "RELIEF"
    assert cluster["ptt_lock_enabled"] == 1
    assert cluster["gateway_handler_device_id"] == old_radio["id"]
    assert cluster["shared_bbs_path"] == "/varac/shared/bbs"
    assert cluster["shared_bbs_archive_path"] == "/varac/shared/bbs/archive"
    members = store.list_varac_cluster_members(cluster_id=cluster["id"])
    assert {(row["device_profile_id"], row["instance_number"]) for row in members} == {
        (old_radio["id"], 1),
        (new_radio["id"], 2),
    }
    assert all(row["shared_bbs_path"] == cluster["shared_bbs_path"] for row in members)
    assert all(
        row["shared_bbs_archive_path"] == cluster["shared_bbs_archive_path"]
        for row in members
    )
    profiles = {int(row["id"]): row for row in store.list_device_profiles()}
    for profile_id in (old_radio["id"], new_radio["id"]):
        assert profiles[int(profile_id)]["varac_bbs_dir"] == cluster["shared_bbs_path"]
        assert (
            profiles[int(profile_id)]["varac_bbs_archive_dir"]
            == cluster["shared_bbs_archive_path"]
        )
    assert result["radio"]["varac_node_id"] == result["application"]["id"]
    assert store.get_varac_node(old_node["id"])["name"] == old_node["name"]


def test_native_cluster_reuses_standalone_db_and_selects_email_sender_without_legacy_gateway(tmp_path):
    store = MultiRadioStore(tmp_path / "varac-native-email.db")
    old_radio, old_node = _save_standalone(store, "radio-old", "varac-old")
    new_radio = store.save_device_profile({"system_key": "radio-new", "name": "Radio New"})
    app_values, manifest_values = _new_varac_values("varac-new")
    app_values.update(
        db_path=old_node["db_path"],
        native_management_state="managed",
        native_writer_key="varac:13.2.7:linux-wine:convert-standalone",
        desired_fingerprint="desired",
        observed_fingerprint="observed",
    )
    store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=new_radio["id"],
        application_values=app_values,
        manifest_values=manifest_values,
        varac_create_cluster_values={
            "name": "Native Cluster",
            "cluster_id": "native",
            "shared_db_path": old_node["db_path"],
            "ptt_lock_enabled": True,
            "existing_standalone_node_id": old_node["id"],
            "existing_standalone_instance_number": 1,
            "email_gateway_sender_choice": "existing_member",
            "native_management_state": "managed",
            "native_writer_key": "varac:13.2.7:linux-wine:convert-standalone",
            "desired_fingerprint": "desired",
            "observed_fingerprint": "observed",
        },
        varac_cluster_instance_number=2,
    )
    cluster = store.list_varac_clusters()[0]
    assert cluster["shared_db_path"] == old_node["db_path"]
    assert cluster["email_gateway_sender_device_id"] == old_radio["id"]
    assert cluster["gateway_handler_device_id"] is None
    assert cluster["native_management_state"] == "managed"
    assert '"exclusive": false' in cluster["resource_claims_json"]
    new_node = next(
        row for row in store.list_varac_nodes() if int(row["id"]) != int(old_node["id"])
    )
    assert new_node["db_path"] == cluster["shared_db_path"]


def test_cluster_bbs_paths_save_list_and_membership_projection(tmp_path):
    store = MultiRadioStore(tmp_path / "varac-bbs-cluster.db")
    radio = store.save_device_profile({"system_key": "radio-bbs", "name": "Radio BBS"})
    cluster = store.save_varac_cluster(
        {
            "name": "BBS Shared",
            "cluster_id": "bbs-shared",
            "shared_db_path": "/varac/shared/cluster.db",
            "shared_bbs_path": "/varac/shared/bbs",
            "shared_bbs_archive_path": "/varac/shared/bbs/archive",
        }
    )

    listed = next(row for row in store.list_varac_clusters() if row["id"] == cluster["id"])
    assert listed["shared_bbs_path"] == "/varac/shared/bbs"
    assert listed["shared_bbs_archive_path"] == "/varac/shared/bbs/archive"

    store.set_varac_cluster_member(cluster["id"], radio["id"], instance_number=1)
    membership = store.list_varac_cluster_members(
        cluster_id=cluster["id"], device_profile_id=radio["id"]
    )[0]
    assert membership["shared_bbs_path"] == listed["shared_bbs_path"]
    assert membership["shared_bbs_archive_path"] == listed["shared_bbs_archive_path"]
    assigned_profile = next(
        row for row in store.list_device_profiles() if row["id"] == radio["id"]
    )
    assert assigned_profile["varac_bbs_dir"] == listed["shared_bbs_path"]
    assert assigned_profile["varac_bbs_archive_dir"] == listed["shared_bbs_archive_path"]

    updated = store.save_varac_cluster(
        {
            "id": cluster["id"],
            "shared_bbs_path": "/varac/shared/bbs-v2",
            "shared_bbs_archive_path": "/varac/shared/bbs-v2/archive",
        }
    )
    profile = next(row for row in store.list_device_profiles() if row["id"] == radio["id"])
    assert profile["varac_bbs_dir"] == updated["shared_bbs_path"]
    assert profile["varac_bbs_archive_dir"] == updated["shared_bbs_archive_path"]


def test_native_managed_join_uses_the_existing_cluster_shared_database(tmp_path):
    store = MultiRadioStore(tmp_path / "varac-native-join.db")
    old_radio, old_node = _save_standalone(store, "radio-old", "varac-old")
    cluster = store.save_varac_cluster(
        {
            "name": "Native Cluster",
            "cluster_id": "native",
            "shared_db_path": old_node["db_path"],
            "native_management_state": "managed",
        }
    )
    store.set_varac_cluster_member(cluster["id"], old_radio["id"], instance_number=1)
    new_radio = store.save_device_profile({"system_key": "radio-new", "name": "Radio New"})
    app_values, manifest_values = _new_varac_values("varac-new")
    app_values.update(
        db_path=cluster["shared_db_path"],
        native_management_state="managed",
        native_writer_key="varac:13.2.7:linux-wine:create-member",
    )

    result = store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=new_radio["id"],
        application_values=app_values,
        manifest_values=manifest_values,
        varac_cluster_db_id=cluster["id"],
        varac_cluster_instance_number=2,
    )

    assert result["application"]["db_path"] == cluster["shared_db_path"]
    members = store.list_varac_cluster_members(cluster_id=cluster["id"])
    assert {(row["device_profile_id"], row["instance_number"]) for row in members} == {
        (old_radio["id"], 1),
        (new_radio["id"], 2),
    }


def test_existing_standalone_cluster_failure_rolls_back_every_new_row(monkeypatch, tmp_path):
    store = MultiRadioStore(tmp_path / "varac-existing-failure.db")
    old_radio, old_node = _save_standalone(store, "radio-old", "varac-old")
    new_radio = store.save_device_profile({"system_key": "radio-new", "name": "Radio New"})
    app_values, manifest_values = _new_varac_values("varac-new")
    before_nodes = store.list_varac_nodes()
    before_manifests = store.list_software_instance_manifests()

    from freqinout.core import multi_radio_store as store_module

    def fail_manifest(*_args, **_kwargs):
        raise RuntimeError("injected manifest failure")

    monkeypatch.setattr(store_module, "_save_software_instance_manifest_conn", fail_manifest)
    with pytest.raises(RuntimeError, match="injected manifest failure"):
        store.adopt_software_instance(
            family_key="varac",
            radio_profile_id=new_radio["id"],
            application_values=app_values,
            manifest_values=manifest_values,
            varac_create_cluster_values={
                "name": "Relief Cluster",
                "cluster_id": "relief",
                "existing_standalone_node_id": old_node["id"],
            },
            varac_cluster_instance_number=2,
        )
    assert store.list_varac_nodes() == before_nodes
    assert store.list_software_instance_manifests() == before_manifests
    assert store.list_varac_clusters() == []
    assert store.list_varac_cluster_members() == []
    assert store.get_device_profile(new_radio["id"])["varac_node_id"] is None
    assert store.get_device_profile(old_radio["id"])["varac_node_id"] == old_node["id"]


def test_existing_standalone_cluster_rejects_conflicting_gateway_policies_before_write(tmp_path):
    store = MultiRadioStore(tmp_path / "varac-gateway-policy.db")
    _old_radio, old_node = _save_standalone(store, "radio-old", "varac-old")
    new_radio = store.save_device_profile({"system_key": "radio-new", "name": "Radio New"})
    app_values, manifest_values = _new_varac_values("varac-new")

    with pytest.raises(ValueError, match="exactly one VarAC cluster gateway policy"):
        store.adopt_software_instance(
            family_key="varac",
            radio_profile_id=new_radio["id"],
            application_values=app_values,
            manifest_values=manifest_values,
            varac_create_cluster_values={
                "name": "Relief Cluster",
                "cluster_id": "relief",
                "existing_standalone_node_id": old_node["id"],
                "gateway_existing_standalone": True,
                "gateway_for_new_cluster": True,
            },
            varac_cluster_instance_number=2,
        )

    assert store.list_varac_clusters() == []
    assert store.list_varac_cluster_members() == []
    assert store.get_device_profile(new_radio["id"])["varac_node_id"] is None


def test_existing_standalone_cluster_duplicate_member_number_and_stale_assignment_write_nothing(tmp_path):
    store = MultiRadioStore(tmp_path / "varac-existing-guards.db")
    old_radio, old_node = _save_standalone(store, "radio-old", "varac-old")
    new_radio = store.save_device_profile({"system_key": "radio-new", "name": "Radio New"})
    app_values, manifest_values = _new_varac_values("varac-new")
    with pytest.raises(ValueError, match="already assigned"):
        store.adopt_software_instance(
            family_key="varac",
            radio_profile_id=new_radio["id"],
            application_values=app_values,
            manifest_values=manifest_values,
            varac_create_cluster_values={
                "name": "Relief Cluster",
                "cluster_id": "relief",
                "existing_standalone_node_id": old_node["id"],
            },
            varac_cluster_instance_number=1,
        )
    assert store.list_varac_clusters() == []
    assert store.list_varac_cluster_members() == []
    assert store.get_device_profile(new_radio["id"])["varac_node_id"] is None

    app_values, manifest_values = _new_varac_values("varac-new-2")
    with pytest.raises(ValueError, match="assignment changed"):
        store.adopt_software_instance(
            family_key="varac",
            radio_profile_id=new_radio["id"],
            application_values=app_values,
            manifest_values=manifest_values,
            expected_current_instance_id=999,
            varac_create_cluster_values={
                "name": "Relief Cluster",
                "cluster_id": "relief",
                "existing_standalone_node_id": old_node["id"],
            },
            varac_cluster_instance_number=2,
        )
    assert len(store.list_varac_nodes()) == 1
    assert store.list_varac_clusters() == []
    assert store.list_varac_cluster_members() == []
