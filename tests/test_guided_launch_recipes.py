import json
from pathlib import Path

import pytest

from freqinout.core.guided_launch_recipes import (
    recipe_draft_updates,
    resolve_fast_light_managed_recipe,
    resolve_js8_managed_recipe,
)
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.config_autodiscovery import find_app_candidates


@pytest.mark.parametrize(
    ("variant", "version"),
    (
        ("js8call_2_2", "2.2.0"),
        ("js8call_improved_3_0_3", "3.0.3"),
        ("js8call_subspace_4_1", "4.1.0.478"),
    ),
)
def test_js8_managed_recipe_is_exact_and_uses_dedicated_identity(tmp_path, variant, version):
    storage_home = tmp_path / "home"
    resolution = resolve_js8_managed_recipe(
        {
            "family_key": "js8call",
            "mode": "managed",
            "draft_instance_key": "draft-js8-south",
            "owner_label": "South Rig",
            "radio_role": "tx_rx",
            "variant": variant,
            "version": version,
            "application_path": "/opt/js8call",
            "host": "127.0.0.1",
            "port": 2443,
            "udp_port": 2238,
            "launch_at_startup": True,
        },
        managed_root="/fio/managed-instances",
        platform="linux",
        storage_home=storage_home,
    )
    assert resolution.qualified
    component = resolution.components[0]
    assert component.effective_command[0] == "/opt/js8call"
    assert component.effective_command[1] == "--rig-name"
    assert component.profile_selector == component.effective_command[2]
    assert component.configuration_roots == (
        "/fio/managed-instances/draft-js8-south/js8call",
    )
    assert component.data_roots == (
        str(storage_home / ".local" / "share" / f"JS8Call - {component.profile_selector}"),
        "/fio/managed-instances/draft-js8-south/js8call/save",
        "/fio/managed-instances/draft-js8-south/js8call/forms",
    )
    assert {item["protocol"] for item in component.endpoints} == {"tcp", "udp"}
    updates = recipe_draft_updates(resolution)
    assert updates["launch_command"] == ""
    assert updates["configuration_path"] != "/old/profile"
    assert updates["storage_path"] != "/old/data"


def test_managed_recipe_refuses_relative_roots_when_settings_context_is_missing():
    resolution = resolve_js8_managed_recipe(
        {
            "draft_instance_key": "draft-js8",
            "owner_label": "Radio",
            "variant": "js8call_2_2",
            "version": "2.2.0",
            "application_path": "/opt/js8call",
            "port": 2443,
            "udp_port": 2238,
        },
        managed_root="",
    )
    assert resolution.status == "blocked_for_safety"
    assert resolution.blocker_code == "missing_managed_root"
    assert not resolution.components


def test_unknown_js8_variant_saves_isolated_profile_with_launch_pending():
    resolution = resolve_js8_managed_recipe(
        {
            "draft_instance_key": "draft-future",
            "owner_label": "Future",
            "variant": "future",
            "version": "99.0",
            "application_path": "/opt/future",
            "host": "127.0.0.1",
            "port": 2444,
            "udp_port": 2239,
        },
        managed_root="/fio/managed-instances",
    )
    assert resolution.status == "launch_pending"
    assert resolution.persistable and not resolution.launch_ready
    assert resolution.components
    assert resolution.components[0].operator_starts is True
    assert resolution.components[0].configuration_roots[0].endswith("/draft-future/js8call")
    assert "--rig-name" in resolution.recovery_action


def test_plausible_js8_executable_without_exact_version_is_warning_ready_and_persistable():
    resolution = resolve_js8_managed_recipe(
        {
            "family_key": "js8call",
            "mode": "managed",
            "draft_instance_key": "draft-warning",
            "owner_label": "Warning radio",
            "radio_role": "tx_rx",
            "variant": "js8call_2_2",
            "version": "",
            "application_path": "/usr/bin/js8call",
            "host": "127.0.0.1",
            "port": 2448,
            "udp_port": 2248,
        },
        managed_root="/fio/managed-instances",
    )
    assert resolution.status == "ready_with_warnings"
    assert resolution.outcome == "Ready with warnings"
    assert resolution.persistable and resolution.launch_ready
    assert resolution.components[0].configuration_roots
    assert resolution.components[0].evidence["version"]["confidence"] == "unverified"
    assert "version" in resolution.recovery_action.casefold()


@pytest.mark.parametrize(
    ("executable", "expected_variant"),
    (
        ("/usr/bin/js8call", "js8call_2_2"),
        ("/usr/bin/js8call-subspace", "js8call_subspace_4_1"),
        ("/opt/JS8Call-Improved.exe", "js8call_improved_3_0_3"),
    ),
)
def test_js8_executable_name_selects_recipe_family_without_inventing_version(
    executable,
    expected_variant,
):
    resolution = resolve_js8_managed_recipe(
        {
            "draft_instance_key": "draft-path-family",
            "owner_label": "Path family",
            "application_path": executable,
            "port": 2450,
            "udp_port": 2250,
        },
        managed_root="/fio/managed-instances",
    )
    assert resolution.status == "ready_with_warnings"
    assert resolution.evidence["version"]["variant"] == expected_variant
    assert resolution.evidence["version"]["version"] == ""


def test_arbitrary_browse_target_without_identity_keeps_launch_pending():
    resolution = resolve_js8_managed_recipe(
        {
            "draft_instance_key": "draft-unknown-browse",
            "owner_label": "Unknown browse",
            "application_path": "/opt/helper-tool",
            "port": 2451,
            "udp_port": 2251,
        },
        managed_root="/fio/managed-instances",
    )
    assert resolution.status == "launch_pending"
    assert resolution.components[0].operator_starts is True


def test_missing_js8_executable_is_launch_pending_but_keeps_isolated_plan():
    resolution = resolve_js8_managed_recipe(
        {
            "family_key": "js8call",
            "mode": "managed",
            "draft_instance_key": "draft-pending",
            "owner_label": "Pending radio",
            "radio_role": "tx_rx",
            "variant": "js8call_2_2",
            "version": "2.2.0",
            "application_path": "",
            "host": "127.0.0.1",
            "port": 2449,
            "udp_port": 2249,
        },
        managed_root="/fio/managed-instances",
    )
    assert resolution.status == "launch_pending"
    assert resolution.persistable and not resolution.launch_ready
    component = resolution.components[0]
    assert component.configuration_roots == ("/fio/managed-instances/draft-pending/js8call",)
    assert component.effective_command == ()
    assert "Browse" in resolution.recovery_action
    updates = recipe_draft_updates(resolution)
    assert updates["launch_setup_pending"] is True
    assert updates["configuration_path"] == component.configuration_roots[0]


def test_explicit_collision_stays_blocked_while_empty_inventory_is_not_a_block():
    common = {
        "family_key": "js8call",
        "mode": "managed",
        "draft_instance_key": "draft-distinct",
        "owner_label": "Distinct radio",
        "radio_role": "tx_rx",
        "variant": "js8call_2_2",
        "version": "2.2.0",
        "application_path": "/usr/bin/js8call",
        "host": "127.0.0.1",
        "port": 2450,
        "udp_port": 2250,
    }
    clean = resolve_js8_managed_recipe(common, managed_root="/fio/managed-instances")
    blocked = resolve_js8_managed_recipe({**common, "resource_collision": True}, managed_root="/fio/managed-instances")
    assert clean.persistable and not clean.blocked_for_safety
    assert blocked.status == "blocked_for_safety"
    assert blocked.blocker_code == "resource_collision"
    assert not blocked.persistable


def test_fast_light_generated_commands_roots_working_dirs_and_evidence_are_stable():
    draft = {
        "family_key": "fast_light",
        "mode": "managed",
        "draft_instance_key": "draft-fast-stable",
        "radio_role": "tx_rx",
        "application_path": "/usr/bin/flrig",
        "secondary_application_path": "/usr/bin/fldigi",
        "host": "127.0.0.1",
        "port": 12500,
        "secondary_port": 7500,
    }
    first = resolve_fast_light_managed_recipe(draft, managed_root="/fio/managed-instances")
    second = resolve_fast_light_managed_recipe(draft, managed_root="/fio/managed-instances")
    assert first.fingerprint == second.fingerprint
    assert [component.component_key for component in first.components] == ["flrig", "fldigi"]
    assert all(component.working_directory for component in first.components)
    assert first.components[0].effective_command[1:] == ("--config-dir", first.components[0].configuration_roots[0])
    assert first.components[1].effective_command[1] == "--config-dir"
    assert first.components[0].evidence["executable"]["path"] == "/usr/bin/flrig"
    updates = recipe_draft_updates(first)
    assert updates["launch_component_recipes"]["fldigi"]["effective_command_text"]
    assert updates["launch_component_recipes"]["fldigi"]["working_directory"].endswith("/fldigi")


def test_saved_linux_executable_identity_precedes_bounded_well_known_candidates(tmp_path, monkeypatch):
    saved = tmp_path / "saved" / "js8call"
    fallback = tmp_path / "well-known" / "js8call"
    for path in (saved, fallback):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/sh\n", encoding="utf-8")
        path.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))
    candidates = find_app_candidates(
        apps=("js8call",),
        platform="Linux",
        home=tmp_path / "home",
        extra_paths=(saved,),
        app_search_paths={"js8call": (fallback,)},
    )
    assert Path(candidates[0].path) == saved
    assert candidates[0].source == "saved"
    assert Path(candidates[1].path) == fallback
    assert candidates[1].source == "known_path"


def test_fast_light_transceiver_recipe_orders_distinct_components():
    resolution = resolve_fast_light_managed_recipe(
        {
            "draft_instance_key": "draft-fast-south",
            "radio_role": "tx_rx",
            "application_path": "/opt/flrig",
            "secondary_application_path": "/opt/fldigi",
            "flmsg_application_path": "/opt/flmsg",
            "flamp_application_path": "/opt/flamp",
            "host": "127.0.0.1",
            "port": 12346,
            "secondary_port": 7363,
            "launch_at_startup": True,
        },
        managed_root="/fio/managed-instances",
    )
    assert resolution.qualified
    assert [item.component_key for item in resolution.components] == [
        "flrig",
        "fldigi",
        "flmsg",
        "flamp",
    ]
    assert resolution.components[1].dependencies == ("flrig",)
    assert resolution.components[2].execution_scope == "station_shared_utility"
    assert resolution.components[0].configuration_roots != resolution.components[1].configuration_roots
    updates = recipe_draft_updates(resolution)
    assert updates["configuration_path"].endswith("/flrig")
    assert updates["secondary_configuration_path"].endswith("/fldigi")
    assert updates["storage_path"].endswith("/fldigi/logs")


def test_fast_light_observer_recipe_has_no_flrig_or_transmit_scope():
    resolution = resolve_fast_light_managed_recipe(
        {
            "draft_instance_key": "draft-fast-rx",
            "radio_role": "observer",
            "secondary_application_path": "/opt/fldigi",
            "host": "127.0.0.1",
            "secondary_port": 7364,
        },
        managed_root="/fio/managed-instances",
    )
    assert resolution.qualified
    assert [item.component_key for item in resolution.components] == ["fldigi"]
    assert resolution.components[0].execution_scope == "receive_only"
    assert resolution.components[0].dependencies == ()


def test_atomic_store_projects_qualified_recipe_to_component_launch_rows(tmp_path):
    store = MultiRadioStore(tmp_path / "recipes.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    resolution = resolve_fast_light_managed_recipe(
        {
            "draft_instance_key": "draft-fast-a",
            "radio_role": "tx_rx",
            "application_path": "/opt/flrig",
            "secondary_application_path": "/opt/fldigi",
            "flmsg_application_path": "/opt/flmsg",
            "host": "127.0.0.1",
            "port": 12346,
            "secondary_port": 7363,
            "launch_at_startup": True,
        },
        managed_root=str(tmp_path / "managed-instances"),
    )
    updates = recipe_draft_updates(resolution)
    result = store.adopt_software_instance(
        family_key="fast_light",
        radio_profile_id=radio["id"],
        application_values={
            "system_key": "fast-a",
            "name": "Radio A",
            "flrig_path": "/opt/flrig",
            "flrig_host": "127.0.0.1",
            "flrig_port": 12346,
            "fldigi_path": "/opt/fldigi",
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7363,
            "fldigi_log_path": updates["storage_path"],
            "fldigi_checkin_dir": updates["secondary_storage_path"],
        },
        manifest_values={
            "instance_key": "fast_light:fast-a",
            "resource_claims": [
                {"kind": "flrig_configuration", "value": updates["configuration_path"]},
                {"kind": "fldigi_configuration", "value": updates["secondary_configuration_path"]},
            ],
            "evidence": {"launch_recipe": resolution.to_mapping()},
        },
        launch_at_startup=True,
    )
    assert result["manifest"]["evidence"]["launch_recipe"]["fingerprint"] == resolution.fingerprint
    with store.connect_readonly() as conn:
        rows = conn.execute(
            "SELECT app_name, path_override, dependencies_json, readiness_json "
            "FROM radio_launch_bundle_items WHERE radio_profile_id=? ORDER BY display_order",
            (radio["id"],),
        ).fetchall()
    assert [row[0] for row in rows] == ["FLRig", "FLDigi", "FLMsg"]
    assert [row[1] for row in rows] == ["/opt/flrig", "/opt/fldigi", "/opt/flmsg"]
    assert json.loads(rows[1][2]) == ["flrig"]
    assert json.loads(rows[1][3])["launch_arguments"][0] == "--config-dir"


@pytest.mark.parametrize(
    ("executable", "version", "expected_status", "expected_enabled", "expected_operator_starts"),
    (
        ("/usr/bin/js8call", "", "ready_with_warnings", 1, False),
        ("", "2.2.0", "launch_pending", 0, True),
    ),
)
def test_store_retains_warning_and_pending_js8_plans_without_enabling_unsafe_launch(
    tmp_path,
    executable,
    version,
    expected_status,
    expected_enabled,
    expected_operator_starts,
):
    store = MultiRadioStore(tmp_path / f"{expected_status}.db")
    radio = store.save_device_profile(
        {"system_key": f"radio-{expected_status}", "name": "Radio"}
    )
    resolution = resolve_js8_managed_recipe(
        {
            "family_key": "js8call",
            "mode": "managed",
            "draft_instance_key": f"draft-{expected_status}",
            "owner_label": "Radio",
            "variant": "js8call_2_2",
            "version": version,
            "application_path": executable,
            "host": "127.0.0.1",
            "port": 2480,
            "udp_port": 2280,
            "launch_at_startup": True,
        },
        managed_root=str(tmp_path / "managed-instances"),
    )
    assert resolution.status == expected_status
    updates = recipe_draft_updates(resolution)
    store.adopt_software_instance(
        family_key="js8call",
        radio_profile_id=radio["id"],
        application_values={
            "system_key": f"js8-{expected_status}",
            "name": "Radio",
            "install_path": executable,
            "port": 2480,
            "udp_port": 2280,
            "profile_path": updates["configuration_path"],
        },
        manifest_values={
            "instance_key": f"js8call:{expected_status}",
            "resource_claims": [
                {"kind": "js8_profile", "value": updates["configuration_path"]}
            ],
            "evidence": {"launch_recipe": resolution.to_mapping()},
        },
        launch_at_startup=True,
    )
    with store.connect_readonly() as conn:
        bundle = conn.execute(
            "SELECT launch_enabled, updated_utc FROM radio_launch_bundles WHERE radio_profile_id=?",
            (radio["id"],),
        ).fetchone()
        item = conn.execute(
            "SELECT path_override, readiness_json FROM radio_launch_bundle_items "
            "WHERE radio_profile_id=?",
            (radio["id"],),
        ).fetchone()
    assert bundle[0] == expected_enabled
    assert isinstance(bundle[1], str) and "T" in bundle[1]
    readiness = json.loads(item[1])
    assert item[0] == executable
    assert readiness["operator_starts"] is expected_operator_starts
    assert readiness["profile_selector"]
    assert readiness["configuration_roots"] == [updates["configuration_path"]]
