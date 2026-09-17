import json

import pytest

from freqinout.core.guided_launch_recipes import (
    recipe_draft_updates,
    resolve_fast_light_managed_recipe,
    resolve_js8_managed_recipe,
)
from freqinout.core.multi_radio_store import MultiRadioStore


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
    assert resolution.status == "unsupported"
    assert not resolution.components


def test_unknown_js8_version_requires_operator_setup():
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
    assert resolution.status == "unsupported"
    assert resolution.raw_override_allowed
    assert not resolution.components


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
