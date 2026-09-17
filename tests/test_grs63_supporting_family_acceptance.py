"""GRS-6.3 acceptance contracts for supporting software families."""

from __future__ import annotations

from pathlib import Path

import pytest

from freqinout.core.config_autodiscovery import RadioInstanceProposal
from freqinout.core.guided_app_config_plan import build_guided_external_app_config_plan
from freqinout.core.guided_instance_inventory import build_guided_instance_inventory, distinct_draft_seed
from freqinout.core.guided_radio_software_model import (
    AtomicInstanceBundle,
    CompletionPolicy,
    ExecutionScope,
    InstanceSourceMode,
    ManagementMode,
    RadioRole,
    SoftwareFamily,
)
from freqinout.core.guided_setup import (
    APP_INSTANCE_MANAGED,
    LANE_VARAC,
    build_guided_setup_blueprint,
    guided_setup_selected_apps_from_flags,
)
from freqinout.core.js8_spotter_forms import discover_spotter_forms, resolve_spotter_forms_dir
from freqinout.core.launch_bundle_store import LaunchBundleStore
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.station_launch_planner import StationLaunchPlanner


def test_builtin_fio_spotter_is_catalog_family_without_external_path_or_launch() -> None:
    bundle = AtomicInstanceBundle(
        family=SoftwareFamily.FIO_SPOTTER,
        instance_key="fio-spotter",
        source_mode=InstanceSourceMode.BUILT_IN,
        completion_policy=CompletionPolicy.REQUIRED,
        owner_radio_key="radio-a",
        management_mode=ManagementMode.BUILT_IN,
        execution_scope=ExecutionScope.BUILT_IN,
    )
    assert bundle.launch_components == ()
    assert bundle.endpoints == ()
    assert bundle.configuration_path == ""
    assert bundle.data_path == ""


def test_commstat_selection_is_radio_bound_alongside_js8() -> None:
    apps = guided_setup_selected_apps_from_flags(use_commstat=True)
    assert apps == ("js8call", "commstat")

    # This is the desired single-process route contract: one shared CommStat
    # launch item can carry more than one radio-owned JS8 binding.
    proposals = [
        RadioInstanceProposal("A", "radio-a", 0, ("js8call", "commstat"), ()),
        RadioInstanceProposal("B", "radio-b", 1, ("js8call", "commstat"), ()),
    ]
    plan = build_guided_external_app_config_plan(proposals, config_root=Path("/tmp/grs63"), allow_external_writes=False)
    commstat = [action for action in plan.actions if action.app_id == "commstat"]
    assert len(commstat) == 1
    assert "radio-a" in str(commstat[0].details) and "radio-b" in str(commstat[0].details)


def test_new_varac_draft_defaults_standalone_even_with_existing_nodes_and_clusters() -> None:
    snapshot = build_guided_instance_inventory(
        {"varac": ({"instance_key": "old", "cluster_id": "cluster-a", "cluster_instance_number": 1},)},
        generation=7,
    )
    draft = distinct_draft_seed(
        "varac", owner_draft_key="new-radio", snapshot=snapshot, source={"instance_name": "New VarAC"}
    )
    assert draft["cluster_path"] == "standalone"
    assert draft["cluster_id"] == ""
    assert draft["cluster_instance_number"] == 0
    blueprint = build_guided_setup_blueprint(lane=LANE_VARAC, setup_mode="managed")
    assert APP_INSTANCE_MANAGED in {choice.choice_id for choice in blueprint.steps[2].choices}


def test_cancel_or_read_only_plan_does_not_write_external_files(tmp_path: Path) -> None:
    proposal = RadioInstanceProposal("Radio A", "radio-a", 0, ("commstat",), ())
    plan = build_guided_external_app_config_plan(
        (proposal,), config_root=tmp_path / "managed", allow_external_writes=False
    )
    assert not (tmp_path / "managed").exists()
    assert plan.actions == ()
    assert plan.review_items


def test_inventory_snapshot_is_bounded_immutable_and_fingerprintable() -> None:
    rows = tuple({"instance_key": f"varac-{index}", "cluster_path": "standalone"} for index in range(1000))
    snapshot = build_guided_instance_inventory({"varac": rows}, generation=1)
    assert len(snapshot.rows_for("varac")) == 1000
    assert snapshot.fingerprint
    with pytest.raises(TypeError):
        snapshot.rows_for("varac")[0]["instance_key"] = "mutated"  # type: ignore[index]


def _commstat_item() -> dict[str, object]:
    return {
        "name": "CommStat",
        "instance_key": "commstat:station-shared",
        "enabled": True,
        "startup": True,
        "monitor_health": True,
        "launch_path_override": "/opt/CommStat",
        "launch_command_override": "",
        "dependencies": ["JS8Call"],
        "readiness_policy": {"execution_scope": "station_shared_utility"},
        "execution_scope": "station_shared_utility",
    }


def test_two_radio_commstat_bindings_dedupe_to_one_process_and_disable_one_preserves_other() -> None:
    profiles = [
        {"id": 1, "name": "A", "device_class": "tx_rx", "runtime_active": 1, "display_order": 1},
        {"id": 2, "name": "B", "device_class": "tx_rx", "runtime_active": 1, "display_order": 2},
    ]
    item = _commstat_item()
    bundles = {1: {"launch_enabled": True, "items": [item]}, 2: {"launch_enabled": True, "items": [item]}}
    plan = StationLaunchPlanner().plan_startup(profiles, bundles)
    commstat = [entry for entry in plan.instances if entry.name == "CommStat"]
    assert len(commstat) == 1
    assert commstat[0].radio_ids == (1, 2)
    remaining = StationLaunchPlanner().plan_startup(profiles, {1: bundles[1], 2: {"launch_enabled": True, "items": []}})
    commstat_remaining = [entry for entry in remaining.instances if entry.name == "CommStat"]
    assert len(commstat_remaining) == 1
    assert commstat_remaining[0].radio_ids == (1,)


def test_commstat_store_projects_one_shared_identity_with_distinct_js8_bindings(tmp_path: Path) -> None:
    db_path = tmp_path / "commstat.db"
    store = MultiRadioStore(db_path)
    js8_a = store.save_js8_instance(
        {
            "system_key": "js8-a",
            "name": "JS8 A",
            "host": "127.0.0.1",
            "port": 2442,
            "commstat_launch_path": "/opt/commstat",
        }
    )
    js8_b = store.save_js8_instance(
        {"system_key": "js8-b", "name": "JS8 B", "host": "127.0.0.1", "port": 2443}
    )
    radio_a = store.save_device_profile(
        {
            "system_key": "radio-a",
            "name": "Radio A",
            "js8_instance_id": js8_a["id"],
            "use_js8call": True,
            "use_commstat": True,
        }
    )
    radio_b = store.save_device_profile(
        {
            "system_key": "radio-b",
            "name": "Radio B",
            "js8_instance_id": js8_b["id"],
            "use_js8call": True,
            "use_commstat": True,
        }
    )
    bundles = {
        int(radio_a["id"]): LaunchBundleStore(db_path).get_bundle(int(radio_a["id"])),
        int(radio_b["id"]): LaunchBundleStore(db_path).get_bundle(int(radio_b["id"])),
    }
    rows = [
        item
        for bundle in bundles.values()
        for item in bundle["items"]
        if item["name"] == "CommStat"
    ]
    assert len(rows) == 2
    assert {item["instance_key"] for item in rows} == {"commstat:station-shared"}
    assert {item["launch_path_override"] for item in rows} == {"/opt/commstat"}
    assert {
        item["readiness_policy"]["bound_js8_port"] for item in rows
    } == {2442, 2443}

    store.save_device_profile({**radio_b, "use_commstat": False})
    assert not any(
        item["name"] == "CommStat"
        for item in LaunchBundleStore(db_path).get_bundle(int(radio_b["id"]))["items"]
    )
    assert any(
        item["name"] == "CommStat"
        for item in LaunchBundleStore(db_path).get_bundle(int(radio_a["id"]))["items"]
    )


def test_blank_spotter_catalog_uses_packaged_forms_and_has_thirty_definitions() -> None:
    root = resolve_spotter_forms_dir("")
    forms = discover_spotter_forms("")
    assert root == resolve_spotter_forms_dir(None)
    assert root.name == "spotter_forms"
    assert len(forms) == 30
