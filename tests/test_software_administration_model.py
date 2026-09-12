from __future__ import annotations

from copy import deepcopy

from freqinout.core.software_administration_model import (
    build_software_administration_snapshot,
)


def _value(record, name):
    """Read the public field from either a mapping or a frozen record."""
    if isinstance(record, dict):
        return record[name]
    return getattr(record, name)


def _profile(key, name, **values):
    return {
        "id": values.pop("id", 1),
        "system_key": key,
        "name": name,
        "enabled": values.pop("enabled", 1),
        "js8_instance_id": values.pop("js8_instance_id", None),
        "fast_light_config_id": values.pop("fast_light_config_id", None),
        "varac_node_id": values.pop("varac_node_id", None),
        **values,
    }


def test_reverse_radio_mapping_is_deterministic_and_keeps_disabled_flags() -> None:
    profiles = [
        _profile("radio-b", "Bravo", js8_instance_id=22, use_js8call=0, use_commstat=1),
        _profile("radio-a", "Alpha", js8_instance_id=11, use_js8call=1, use_commstat=0),
    ]
    instances = [
        {"id": 22, "system_key": "js8-b", "name": "js8-b", "profile_path": "/b"},
        {"id": 11, "system_key": "js8-a", "name": "js8-a", "profile_path": "/a"},
    ]

    profiles[0]["id"], profiles[1]["id"] = 20, 10
    snapshot = build_software_administration_snapshot(profiles, js8_instances=instances)
    family = snapshot.family("js8call")

    assert [_value(item, "radio_id") for item in family.assignments] == [10, 20]
    by_radio = {_value(item, "radio_id"): item for item in family.assignments}
    assert _value(by_radio[10], "instance_id") == 11
    assert _value(by_radio[10], "instance_name") == "js8-a"
    assert _value(by_radio[10], "software_enabled") is True
    assert _value(by_radio[20], "software_enabled") is False


def test_shared_instance_is_disclosed_on_each_radio_assignment() -> None:
    profiles = [
        _profile("radio-a", "Alpha", id=10, js8_instance_id=7, use_js8call=1),
        _profile("radio-b", "Bravo", id=20, js8_instance_id=7, use_js8call=1),
    ]
    snapshot = build_software_administration_snapshot(
        profiles,
        js8_instances=[{"id": 7, "system_key": "shared-js8", "name": "Shared JS8"}],
    )

    assignments = snapshot.family("js8call").assignments
    assert len(assignments) == 2
    assert {_value(item, "instance_id") for item in assignments} == {7}
    assert all(_value(item, "is_shared") is True for item in assignments)
    assert all(set(_value(item, "shared_radio_names")) == {"Alpha", "Bravo"} for item in assignments)


def test_unassigned_instances_are_retained_by_family() -> None:
    profiles = [_profile("radio-a", "Alpha", id=10)]
    snapshot = build_software_administration_snapshot(
        profiles,
        js8_instances=[{"id": 9, "system_key": "orphan-js8", "name": "Orphan JS8"}],
        fast_light_configs=[{"id": 8, "system_key": "orphan-fast", "name": "Orphan Fast"}],
        varac_nodes=[{"id": 7, "system_key": "orphan-varac", "name": "Orphan VarAC"}],
    )

    assert [_value(item, "instance_id") for item in snapshot.family("js8call").unassigned_instances] == [9]
    assert [_value(item, "instance_name") for item in snapshot.family("fast_light").unassigned_instances] == ["Orphan Fast"]
    assert [_value(item, "instance_id") for item in snapshot.family("varac").unassigned_instances] == [7]
    assert all(_value(item, "enabled") is True for item in snapshot.family("js8call").unassigned_instances)


def test_js8_family_derives_fio_external_spotter_and_commstat_roles() -> None:
    profile = _profile(
        "radio-a",
        "Alpha",
        id=10,
        js8_instance_id=3,
        use_js8call=1,
        use_js8spotter=1,
        use_commstat=1,
    )
    instance = {
        "id": 3,
        "system_key": "js8-a",
        "name": "JS8 Alpha",
        "spotter_launch_path": "/opt/js8spotter",
        "commstat_launch_path": "/opt/commstat",
    }
    snapshot = build_software_administration_snapshot([profile], js8_instances=[instance])
    assert snapshot.family("js8call").assignments[0]
    assert snapshot.family("fio_spotter").assignments[0]
    assert snapshot.family("external_spotter").assignments[0]
    assert snapshot.family("commstat").assignments[0]


def test_readiness_absence_is_neutral_and_inputs_are_not_mutated() -> None:
    profiles = [_profile("radio-a", "Alpha", id=10, js8_instance_id=3, use_js8call=1)]
    instances = [{"id": 3, "system_key": "js8-a"}]
    original_profiles, original_instances = deepcopy(profiles), deepcopy(instances)

    snapshot = build_software_administration_snapshot(profiles, js8_instances=instances)
    assignment = snapshot.family("js8call").assignments[0]

    # No probe evidence is a neutral, explicitly unprobed status rather than
    # an error or warning.  The raw readiness field remains empty; the public
    # status presentation is the neutral ``Not yet verified`` label.
    assert _value(assignment, "readiness") == ""
    assert _value(assignment, "status_text") == "Not yet verified"
    assert profiles == original_profiles
    assert instances == original_instances


def test_readiness_is_applied_by_radio_without_changing_radio_order() -> None:
    profiles = [_profile("radio-b", "Bravo", id=20), _profile("radio-a", "Alpha", id=10, use_varac=1, varac_node_id=4)]
    readiness = {10: {"varac": "ready"}}
    snapshot = build_software_administration_snapshot(
        profiles,
        varac_nodes=[{"id": 4, "system_key": "varac-a"}],
        readiness_by_radio=readiness,
    )
    assignments = snapshot.family("varac").assignments

    assert [_value(item, "radio_id") for item in assignments] == [10]
    assert _value(assignments[0], "readiness") == "ready"

    tuple_key_snapshot = build_software_administration_snapshot(
        profiles,
        varac_nodes=[{"id": 4, "system_key": "varac-a"}],
        readiness_by_radio={(10, "varac"): "ready"},
    )
    assert _value(tuple_key_snapshot.family("varac").assignments[0], "readiness") == "ready"
