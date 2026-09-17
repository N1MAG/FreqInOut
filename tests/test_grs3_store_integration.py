from __future__ import annotations

import json

import pytest

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.launch_bundle_store import LaunchBundleStore
from freqinout.core.station_launch_planner import StationLaunchPlanner


def _observer(store: MultiRadioStore, key: str = "observer-a") -> dict:
    return store.save_device_profile(
        {
            "system_key": key,
            "name": "Receive-only radio",
            "device_class": "observer",
            "control_backend": "manual",
        }
    )


def test_observer_fast_light_is_receive_only_at_store_and_launch_boundaries(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "observer-fast-light.db")
    radio = _observer(store)

    result = store.adopt_observer_fast_light_instance(
        radio_profile_id=radio["id"],
        application_values={
            "system_key": "fast-rx",
            "name": "Fast Light RX",
            "flrig_path": "",
            "fldigi_path": "/opt/fldigi",
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7363,
        },
        manifest_values={
            "instance_key": "fast_light:fast-rx",
            "executable_path": "/opt/fldigi",
            "resource_claims": [
                {"kind": "fldigi_configuration", "value": "/profiles/fldigi-rx", "exclusive": True},
                {"kind": "flmsg_application", "value": "/opt/flmsg", "exclusive": True},
                {"kind": "flamp_application", "value": "/opt/flamp", "exclusive": True},
            ],
        },
        launch_at_startup=True,
    )

    saved = result["radio"]
    assert saved["use_flrig"] == 0
    assert saved["use_fldigi"] == 1
    assert saved["use_flmsg"] == 1
    assert saved["use_flamp"] == 1
    assert saved["flmsg_path"] == "/opt/flmsg"
    assert saved["flamp_path"] == "/opt/flamp"
    assert saved["fast_light_config_id"] == result["application"]["id"]
    assert result["manifest"]["evidence"] == {
        "execution_scope": "receive_only",
        "receive_only_ingest": True,
        "transmit_authority": False,
    }
    with store.connect_readonly() as conn:
        items = conn.execute(
            "SELECT app_name, readiness_json FROM radio_launch_bundle_items WHERE radio_profile_id=?",
            (radio["id"],),
        ).fetchall()
    assert [row[0] for row in items] == ["FLDigi"]
    readiness = json.loads(items[0][1])
    assert readiness["execution_scope"] == "receive_only"
    assert readiness["transmit_authority"] is False

    bundle = LaunchBundleStore(tmp_path / "observer-fast-light.db").get_bundle(radio["id"])
    review = StationLaunchPlanner().plan_review([saved], {radio["id"]: bundle})
    assert [item.name for item in review.instances] == ["FLDigi", "FLMsg", "FLAmp"]
    assert all(
        item.operator_starts
        for item in review.instances
        if item.name in {"FLMsg", "FLAmp"}
    )

    with pytest.raises(ValueError, match="cannot enable FLRig"):
        store.save_device_profile({"id": radio["id"], "use_flrig": 1})
    assert store.get_device_profile(radio["id"])["use_flrig"] == 0


def test_observer_varac_rejection_writes_nothing(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "observer-varac.db")
    radio = _observer(store)

    with pytest.raises(ValueError, match="cannot use VarAC"):
        store.adopt_software_instance(
            family_key="varac",
            radio_profile_id=radio["id"],
            application_values={
                "system_key": "varac-rx",
                "name": "Invalid observer VarAC",
                "ini_path": "/varac/rx.ini",
            },
            manifest_values={"instance_key": "varac:rx"},
        )

    assert store.list_varac_nodes() == []
    assert store.list_software_instance_manifests() == []
    assert store.get_device_profile(radio["id"])["varac_node_id"] is None


def test_advanced_fast_light_tx_requires_explicit_acknowledgement(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "fast-light-tx.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    application = {
        "system_key": "fast-tx",
        "name": "Fast Light TX",
        "flrig_path": "/opt/flrig",
        "fldigi_path": "/opt/fldigi",
        "flrig_port": 12346,
        "fldigi_port": 7363,
    }
    manifest = {
        "instance_key": "fast_light:fast-tx",
        "evidence": {
            "advanced_tx_requested": True,
            "advanced_tx_acknowledged": False,
        },
    }

    with pytest.raises(ValueError, match="explicit acknowledgement"):
        store.adopt_software_instance(
            family_key="fast_light",
            radio_profile_id=radio["id"],
            application_values=application,
            manifest_values=manifest,
        )
    assert store.list_fast_light_configs() == []

    manifest["evidence"]["advanced_tx_acknowledged"] = True
    result = store.adopt_software_instance(
        family_key="fast_light",
        radio_profile_id=radio["id"],
        application_values=application,
        manifest_values=manifest,
    )
    assert result["manifest"]["evidence"]["advanced_tx_acknowledged"] is True


def test_varac_create_cluster_is_atomic_and_keeps_exact_launch_identity(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "varac-create.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})

    result = store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=radio["id"],
        application_values={
            "system_key": "varac-a",
            "name": "VarAC A",
            "install_path": "/opt/varac/varac",
            "ini_path": "/varac/a/config.ini",
            "db_path": "/varac/a/local.db",
            "incoming_path": "/varac/a/incoming",
            "outbox_path": "/varac/a/outbox",
            "launch_cmd": "/opt/varac/varac --instance 2",
        },
        manifest_values={
            "instance_key": "varac:a",
            "launch_command": "/opt/varac/varac --instance 2",
            "resource_claims": [
                {"kind": "working_directory", "value": "/varac/a", "exclusive": True}
            ],
        },
        varac_create_cluster_values={
            "name": "Relief Cluster",
            "cluster_id": "relief",
            "shared_db_path": "/varac/shared/relief.db",
            "ptt_lock_enabled": True,
            "gateway_for_new_cluster": True,
        },
        varac_cluster_instance_number=2,
        launch_at_startup=True,
    )

    clusters = store.list_varac_clusters()
    assert len(clusters) == 1
    assert clusters[0]["cluster_id"] == "RELIEF"
    assert clusters[0]["gateway_handler_device_id"] == radio["id"]
    members = store.list_varac_cluster_members(cluster_id=clusters[0]["id"])
    assert [(row["device_profile_id"], row["instance_number"]) for row in members] == [(radio["id"], 2)]
    assert result["radio"]["varac_node_id"] == result["application"]["id"]
    with store.connect_readonly() as conn:
        item = conn.execute(
            "SELECT command_override, readiness_json FROM radio_launch_bundle_items WHERE radio_profile_id=?",
            (radio["id"],),
        ).fetchone()
    assert item[0] == "/opt/varac/varac --instance 2"
    assert json.loads(item[1])["working_directory"] == "/varac/a"


def test_varac_create_failure_rolls_back_cluster_node_manifest_and_link(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "varac-create-rollback.db")
    first_radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    second_radio = store.save_device_profile({"system_key": "radio-b", "name": "Radio B"})
    store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=first_radio["id"],
        application_values={"system_key": "varac-a", "name": "A", "ini_path": "/varac/a.ini"},
        manifest_values={"instance_key": "varac:a"},
        varac_create_cluster_values={"name": "Relief", "cluster_id": "relief"},
        varac_cluster_instance_number=1,
    )

    with pytest.raises(ValueError, match="already in use"):
        store.adopt_software_instance(
            family_key="varac",
            radio_profile_id=second_radio["id"],
            application_values={"system_key": "varac-b", "name": "B", "ini_path": "/varac/b.ini"},
            manifest_values={"instance_key": "varac:b"},
            varac_create_cluster_values={"name": "Duplicate", "cluster_id": "RELIEF"},
            varac_cluster_instance_number=2,
        )

    assert len(store.list_varac_clusters()) == 1
    assert [row["system_key"] for row in store.list_varac_nodes()] == ["varac_a"]
    assert [row["instance_key"] for row in store.list_software_instance_manifests()] == ["varac:a"]
    assert store.get_device_profile(second_radio["id"])["varac_node_id"] is None


def test_varac_shared_database_path_alias_cannot_replace_node_local_database(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "varac-path-alias.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})

    with pytest.raises(ValueError, match="cannot replace a node-local"):
        store.adopt_software_instance(
            family_key="varac",
            radio_profile_id=radio["id"],
            application_values={
                "system_key": "varac-a",
                "name": "A",
                "ini_path": "/varac/a/config.ini",
                "db_path": "/varac/a/local.db",
            },
            manifest_values={"instance_key": "varac:a"},
            varac_create_cluster_values={
                "name": "Relief",
                "cluster_id": "relief",
                "shared_db_path": "/varac/a/../a/local.db",
            },
            varac_cluster_instance_number=1,
        )

    assert store.list_varac_nodes() == []
    assert store.list_varac_clusters() == []
    assert store.list_software_instance_manifests() == []
