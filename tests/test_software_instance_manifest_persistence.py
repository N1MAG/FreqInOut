from __future__ import annotations

import json
import sqlite3

import pytest

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.software_instance_manifest import (
    find_manifest_conflicts,
    manifest_from_mapping,
    manifest_to_record,
    next_available_port,
    normalize_endpoint_claims,
    normalize_resource_claims,
)


def _manifest(**overrides):
    values = {
        "instance_key": "js8:a",
        "family_key": "js8call",
        "application_system_key": "js8-a",
        "ports": [{"name": "API", "protocol": "TCP", "host": "localhost", "port": 2442}],
        "resource_claims": [{"kind": "config_root", "value": "/tmp/js8-a"}],
    }
    values.update(overrides)
    return manifest_from_mapping(values)


def test_manifest_normalization_is_bounded_and_canonical() -> None:
    endpoints = normalize_endpoint_claims(
        [{"name": " API ", "protocol": "TCP", "host": " localhost ", "port": "2442"}]
    )
    resources = normalize_resource_claims(
        [{"kind": "CONFIG_ROOT", "value": " /tmp/One ", "exclusive": 0},
         {"kind": "config_root", "value": "/tmp/One"}]
    )
    assert endpoints[0].protocol == "tcp"
    assert endpoints[0].host == "localhost"
    assert endpoints[0].port == 2442
    assert len(resources) == 1
    assert resources[0].kind == "config_root"
    assert resources[0].value == "/tmp/One"
    with pytest.raises(ValueError, match="Duplicate endpoint"):
        normalize_endpoint_claims([{"port": 2442}, {"port": 2442}])


def test_manifest_conflicts_cover_identity_endpoint_and_exclusive_resource() -> None:
    existing = _manifest()
    proposed = _manifest(
        instance_key="js8:b",
        application_system_key="js8-a",
        resource_claims=[{"kind": "config_root", "value": "/tmp/js8-a"}],
    )
    codes = {issue.code for issue in find_manifest_conflicts(proposed, [existing])}
    assert codes == {"duplicate_application_identity", "endpoint_collision", "resource_collision"}
    assert next_available_port(2442, protocol="tcp", host="127.0.0.1", manifests=[existing]) == 2443
    assert next_available_port(2442, protocol="udp", host="127.0.0.1", manifests=[existing]) == 2442


def test_manifest_path_claims_are_canonical_and_cross_role_exclusive(tmp_path) -> None:
    root = tmp_path / "profiles"
    existing = _manifest(
        instance_key="fast:a",
        family_key="fast_light",
        application_system_key="fast-a",
        ports=[],
        resource_claims=[{"kind": "flrig_configuration", "value": str(root / "one" / ".." / "shared")}],
    )
    proposed = _manifest(
        instance_key="fast:b",
        family_key="fast_light",
        application_system_key="fast-b",
        ports=[],
        resource_claims=[{"kind": "fldigi_configuration", "value": str(root / "shared")}],
    )

    assert {issue.code for issue in find_manifest_conflicts(proposed, [existing])} == {"resource_collision"}


def test_manifest_record_round_trip_and_additive_upgrade_preserve_existing_rows(tmp_path) -> None:
    db = tmp_path / "settings.db"
    store = MultiRadioStore(db)
    app = store.save_js8_instance({"system_key": "js8-existing", "name": "Existing", "port": 2442})
    before = dict(app)

    # Simulate an older settings database where this additive table has not yet existed.
    with sqlite3.connect(db) as conn:
        conn.execute("DROP TABLE software_instance_manifests")
        conn.commit()
    reopened = MultiRadioStore(db)
    manifest = reopened.save_software_instance_manifest(
        {
            "instance_key": "js8:existing",
            "family_key": "js8call",
            "application_system_key": app["system_key"],
            "ports": [{"name": "API", "host": "127.0.0.1", "port": 2442}],
            "evidence": {"source": "upgrade-test"},
        }
    )
    assert manifest["instance_key"] == "js8:existing"
    assert manifest["ports"] == [{"host": "127.0.0.1", "name": "API", "port": 2442, "protocol": "tcp"}]
    assert manifest["evidence"] == {"source": "upgrade-test"}
    assert reopened.get_js8_instance(app["id"]) == before
    record = manifest_to_record(manifest_from_mapping(manifest))
    assert json.loads(record["evidence_json"]) == {"source": "upgrade-test"}


@pytest.mark.parametrize(
    ("family", "app_values", "port", "item_names"),
    [
        ("js8call", {"system_key": "js8-second", "name": "JS8 second", "port": 2452, "install_path": "/opt/js8"}, 2452, ["JS8Call"]),
        ("fast_light", {"system_key": "fast-second", "name": "Fast second", "flrig_port": 12445, "fldigi_port": 7462}, 12445, ["FLRig", "FLDigi"]),
        ("varac", {"system_key": "varac-second", "name": "VarAC second", "ini_path": "/varac/second.ini", "launch_cmd": "/opt/varac"}, None, ["VarAC"]),
    ],
)
def test_adopt_persists_link_manifest_and_launch_items(tmp_path, family, app_values, port, item_names) -> None:
    store = MultiRadioStore(tmp_path / f"{family}.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    manifest = {
        "instance_key": f"{family}:second",
        "management_mode": "operator",
        "launch_command": "launch-second",
        "ports": ([{"name": "API", "port": port}] if port else []),
        "resource_claims": ([{"kind": "ini_path", "value": app_values["ini_path"]}] if family == "varac" else []),
    }
    result = store.adopt_software_instance(
        family_key=family,
        radio_profile_id=radio["id"],
        application_values=app_values,
        manifest_values=manifest,
    )
    assert result["manifest"]["application_system_key"] == result["application"]["system_key"]
    assert result["radio"][{"js8call": "js8_instance_id", "fast_light": "fast_light_config_id", "varac": "varac_node_id"}[family]]
    with store.connect_readonly() as conn:
        rows = conn.execute(
            "SELECT app_name, launch_at_startup, enabled FROM radio_launch_bundle_items WHERE radio_profile_id=? ORDER BY display_order",
            (radio["id"],),
        ).fetchall()
    assert [row[0] for row in rows] == item_names
    assert all(row[1] == 0 and row[2] == 1 for row in rows)


def test_rejected_adoption_rolls_back_application_and_radio_profile(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "rollback.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    first = store.adopt_software_instance(
        family_key="js8call",
        radio_profile_id=radio["id"],
        application_values={"system_key": "js8-a", "name": "First", "port": 2442},
        manifest_values={"instance_key": "js8:first", "ports": [{"name": "API", "port": 2442}]},
    )
    original_app = store.get_js8_instance(first["application"]["id"])
    original_radio = store.get_device_profile(radio["id"])
    with pytest.raises(ValueError, match="already registered|conflicts"):
        store.adopt_software_instance(
            family_key="js8call",
            radio_profile_id=radio["id"],
            application_values={"id": first["application"]["id"], "system_key": "js8-a", "name": "Mutated", "port": 2442},
            manifest_values={"instance_key": "js8:second", "ports": [{"name": "API", "port": 2442}]},
        )
    assert store.get_js8_instance(first["application"]["id"]) == original_app
    assert store.get_device_profile(radio["id"]) == original_radio
    assert [m["instance_key"] for m in store.list_software_instance_manifests()] == ["js8:first"]


@pytest.mark.parametrize(
    ("family", "first_values", "second_values"),
    [
        ("js8call", {"system_key": "js8-one", "name": "One", "port": 2442}, {"system_key": "js8-two", "name": "Two", "port": 2442}),
        ("fast_light", {"system_key": "fast-one", "name": "One", "flrig_port": 12345}, {"system_key": "fast-two", "name": "Two", "flrig_port": 12345}),
        ("varac", {"system_key": "varac-one", "name": "One", "ini_path": "/varac/shared.ini"}, {"system_key": "varac-two", "name": "Two", "ini_path": "/varac/shared.ini"}),
    ],
)
def test_adoption_rejects_legacy_app_table_claim_collision_without_manifest(tmp_path, family, first_values, second_values) -> None:
    store = MultiRadioStore(tmp_path / f"legacy-{family}.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    save_method = {
        "js8call": store.save_js8_instance,
        "fast_light": store.save_fast_light_config,
        "varac": store.save_varac_node,
    }[family]
    first_app = save_method(first_values)
    with store.connect() as conn:
        conn.execute("DELETE FROM software_instance_manifests")
        conn.commit()
    with pytest.raises(ValueError, match="already used|already assigned"):
        store.adopt_software_instance(
            family_key=family,
            radio_profile_id=radio["id"],
            application_values=second_values,
            manifest_values={"instance_key": f"{family}:collision"},
        )
    assert store.list_device_profiles()[0][{"js8call": "js8_instance_id", "fast_light": "fast_light_config_id", "varac": "varac_node_id"}[family]] is None
    assert len({row["id"] for row in ({"js8call": store.list_js8_instances, "fast_light": store.list_fast_light_configs, "varac": store.list_varac_nodes}[family]())}) == 1
    assert first_app["system_key"] in {row["system_key"] for row in ({"js8call": store.list_js8_instances, "fast_light": store.list_fast_light_configs, "varac": store.list_varac_nodes}[family]())}


def test_adoption_persists_explicit_launch_at_startup_on_bundle_and_item(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "startup.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    result = store.adopt_software_instance(
        family_key="js8call",
        radio_profile_id=radio["id"],
        application_values={"system_key": "js8-a", "name": "JS8", "port": 2452},
        manifest_values={"instance_key": "js8:startup", "ports": [{"name": "API", "port": 2452}]},
        launch_at_startup=True,
    )
    with store.connect_readonly() as conn:
        bundle = conn.execute("SELECT launch_enabled FROM radio_launch_bundles WHERE radio_profile_id=?", (radio["id"],)).fetchone()
        item = conn.execute("SELECT launch_at_startup FROM radio_launch_bundle_items WHERE instance_key=?", ("js8:startup:js8call",)).fetchone()
    assert bundle[0] == 1
    assert item[0] == 1
    assert result["manifest"]["instance_key"] == "js8:startup"


def test_replacement_requires_confirmation_and_successfully_changes_radio_link(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "replace.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    first = store.adopt_software_instance(
        family_key="js8call", radio_profile_id=radio["id"],
        application_values={"system_key": "js8-a", "name": "First", "port": 2442},
        manifest_values={"instance_key": "js8:first", "ports": [{"name": "API", "port": 2442}]},
    )
    with pytest.raises(ValueError, match="Confirm replacement"):
        store.adopt_software_instance(
            family_key="js8call", radio_profile_id=radio["id"],
            application_values={"system_key": "js8-b", "name": "Second", "port": 2452},
            manifest_values={"instance_key": "js8:second", "ports": [{"name": "API", "port": 2452}]},
        )
    assert store.get_device_profile(radio["id"])["js8_instance_id"] == first["application"]["id"]
    second = store.adopt_software_instance(
        family_key="js8call", radio_profile_id=radio["id"],
        application_values={"system_key": "js8-b", "name": "Second", "port": 2452},
        manifest_values={"instance_key": "js8:second", "ports": [{"name": "API", "port": 2452}]},
        replace_existing=True,
    )
    assert store.get_device_profile(radio["id"])["js8_instance_id"] == second["application"]["id"]
    assert {row["instance_key"] for row in store.list_software_instance_manifests()} == {"js8:first", "js8:second"}


def test_varac_cluster_membership_and_instance_collision_roll_back_adoption(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "cluster.db")
    radio_a = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    radio_b = store.save_device_profile({"system_key": "radio-b", "name": "Radio B"})
    cluster = store.save_varac_cluster({"system_key": "cluster-a", "name": "Cluster A"})
    store.adopt_software_instance(
        family_key="varac", radio_profile_id=radio_a["id"],
        application_values={"system_key": "varac-a", "name": "A", "ini_path": "/varac/a.ini"},
        manifest_values={"instance_key": "varac:a"}, varac_cluster_db_id=cluster["id"], varac_cluster_instance_number=1,
    )
    with pytest.raises(ValueError, match="already assigned"):
        store.adopt_software_instance(
            family_key="varac", radio_profile_id=radio_b["id"],
            application_values={"system_key": "varac-b", "name": "B", "ini_path": "/varac/b.ini"},
            manifest_values={"instance_key": "varac:b"}, varac_cluster_db_id=cluster["id"], varac_cluster_instance_number=1,
        )
    assert store.get_device_profile(radio_b["id"])["varac_node_id"] is None
    assert store.list_varac_nodes() and len(store.list_varac_nodes()) == 1
    assert [row["instance_number"] for row in store.list_varac_cluster_members(cluster_id=cluster["id"]) ] == [1]


def test_one_application_instance_cannot_be_silently_shared_across_radios(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "sharing.db")
    radio_a = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})
    radio_b = store.save_device_profile({"system_key": "radio-b", "name": "Radio B"})
    first = store.adopt_software_instance(
        family_key="js8call", radio_profile_id=radio_a["id"],
        application_values={"system_key": "js8-a", "name": "Shared", "port": 2442},
        manifest_values={"instance_key": "js8:shared", "ports": [{"name": "API", "port": 2442}]},
    )
    with pytest.raises(ValueError, match="already assigned"):
        store.adopt_software_instance(
            family_key="js8call", radio_profile_id=radio_b["id"],
            application_values={"id": first["application"]["id"], "system_key": "js8-a", "name": "Shared", "port": 2442},
            manifest_values={"instance_key": "js8:shared-copy", "ports": [{"name": "API", "port": 2442}]},
        )
    assert store.get_device_profile(radio_b["id"])["js8_instance_id"] is None
    assert len(store.list_software_instance_manifests()) == 1


def test_fast_light_same_process_endpoint_is_rejected_before_any_write(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "fast-self-collision.db")
    radio = store.save_device_profile({"system_key": "radio-a", "name": "Radio A"})

    with pytest.raises(ValueError, match="different local TCP endpoints"):
        store.adopt_software_instance(
            family_key="fast_light",
            radio_profile_id=radio["id"],
            application_values={
                "system_key": "fast-a",
                "name": "Fast A",
                "flrig_host": "127.0.0.1",
                "flrig_port": 12345,
                "fldigi_host": "127.0.0.1",
                "fldigi_port": 12345,
            },
            manifest_values={"instance_key": "fast:a"},
        )

    assert store.list_fast_light_configs() == []
    assert store.get_device_profile(radio["id"])["fast_light_config_id"] is None
