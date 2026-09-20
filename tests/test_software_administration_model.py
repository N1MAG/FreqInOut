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


def test_fio_spotter_is_selected_only_by_its_explicit_radio_flag() -> None:
    profiles = [
        _profile("radio-js8-only", "JS8 only", id=10, js8_instance_id=3, use_js8call=1),
        _profile("radio-spotter", "Spotter", id=20, js8_instance_id=4,
                 use_js8call=1, use_js8spotter=1),
    ]
    snapshot = build_software_administration_snapshot(
        profiles,
        js8_instances=[
            {"id": 3, "system_key": "js8-only", "name": "JS8 only"},
            {"id": 4, "system_key": "js8-spotter", "name": "JS8 with Spotter"},
        ],
    )

    assignments = snapshot.family("fio_spotter").assignments
    assert [item.radio_id for item in assignments] == [20]


def _canonical_record(
    family: str,
    *,
    owner: str,
    bundle_id: str,
    components: tuple[str, ...] = (),
    bindings: tuple[tuple[str, str], ...] = (),
) -> dict[str, object]:
    if family == "fio_spotter":
        components = components or ("fio-spotter",)
    if family == "commstat" and not any(kind == "station-process" for _key, kind in bindings):
        bindings = (("commstat:station", "station-process"), *bindings)
    return {
        "bundle_id": bundle_id,
        "identity_key": f"{owner}:{family}:{bundle_id}",
        "family_key": family,
        "owner": owner,
        "scope": (
            "built_in" if family == "fio_spotter"
            else "station_shared_utility" if family == "commstat"
            else "standard"
        ),
        "source_mode": (
            "built_into_fio" if family == "fio_spotter"
            else "shared_station_tool" if family == "commstat"
            else "create_distinct_instance"
        ),
        "management_mode": (
            "built_in" if family == "fio_spotter"
            else "operator" if family == "commstat"
            else "fio_managed"
        ),
        "completion_policy": "required",
        "provenance": "guided",
        "verification": {"state": "reviewed"},
        "paths": {"configuration_path": f"/exact/{bundle_id}"},
        "resources": [],
        "endpoints": [],
        "components": [{"component_id": key} for key in components],
        "bindings": [
            {"binding_id": binding_id, "kind": kind, "radio_key": radio_key}
            for binding_id, kind in bindings
            for radio_key in (("radio-a" if kind in {"built-in-radio", "radio-js8-endpoint"} else ""),)
        ],
        "launch": {"argv": {key: [f"/exact/{key}"] for key in components}},
        "readiness": {"state": "reviewed"},
    }


def test_canonical_fast_light_and_station_bindings_project_exact_identity_once() -> None:
    profile = _profile(
        "radio-a", "Alpha", id=10, js8_instance_id=3, use_js8call=1,
        use_flrig=1, use_flamp=1, fast_light_config_id=7,
        use_js8spotter=1, use_commstat=1,
    )
    records = (
        _canonical_record(
            "fast_light", owner="radio-a", bundle_id="fast-light:radio-a",
            components=("flrig:radio-a", "fldigi:radio-a", "flmsg:station", "flamp:radio-a"),
        ),
        _canonical_record(
            "fio_spotter", owner="station", bundle_id="fio-spotter:radio-a",
            bindings=(("fio-spotter:radio-a", "built-in-radio"),),
        ),
        _canonical_record(
            "commstat", owner="station", bundle_id="commstat:station",
            components=("commstat",),
            bindings=(("commstat:radio-a:js8-endpoint", "radio-js8-endpoint"),),
        ),
    )
    snapshot = build_software_administration_snapshot(
        [profile],
        js8_instances=[{"id": 3, "system_key": "js8-a", "name": "JS8 Alpha"}],
        fast_light_configs=[{"id": 7, "system_key": "fast-light:radio-a", "name": "Fast Alpha"}],
        identity_records=records,
    )

    fast = snapshot.family("fast_light").assignments
    spotter = snapshot.family("fio_spotter").assignments
    commstat = snapshot.family("commstat").assignments
    assert len(fast) == len(spotter) == len(commstat) == 1
    assert fast[0].canonical_bundle_id == "fast-light:radio-a"
    assert fast[0].canonical_fingerprint
    assert fast[0].canonical_component_ids == (
        "flrig:radio-a", "fldigi:radio-a", "flmsg:station", "flamp:radio-a"
    )
    assert fast[0].canonical_parity_state == "verified"
    assert spotter[0].canonical_bundle_id == "fio-spotter:radio-a"
    assert spotter[0].canonical_binding_ids == ("fio-spotter:radio-a",)
    assert commstat[0].canonical_bundle_id == "commstat:station"
    assert commstat[0].canonical_component_ids == ("commstat",)
    assert commstat[0].canonical_binding_ids == (
        "commstat:station",
        "commstat:radio-a:js8-endpoint",
    )
    assert all(item.canonical_parity_state == "verified" for item in (spotter[0], commstat[0]))


def test_canonical_external_js8spotter_maps_to_external_spotter_family() -> None:
    profile = _profile("radio-a", "Alpha", id=10)
    record = _canonical_record(
        "external_js8spotter", owner="radio-a", bundle_id="external-js8spotter:radio-a",
        components=("external-js8spotter:radio-a",),
    )
    snapshot = build_software_administration_snapshot([profile], identity_records=(record,))

    assignments = snapshot.family("external_spotter").assignments
    assert len(assignments) == 1
    assert assignments[0].canonical_bundle_id == "external-js8spotter:radio-a"
    assert assignments[0].canonical_component_ids == ("external-js8spotter:radio-a",)


def test_receiver_software_projects_sdrpp_identity_for_observers_only() -> None:
    observer = _profile(
        "receiver-a", "Receiver A", id=10, device_class="observer",
        sdr_application="SDR++", sdr_adapter="sdrpp_rigctl",
    )
    transceiver = _profile(
        "radio-b", "Radio B", id=20, device_class="tx_rx",
        sdr_application="", sdr_adapter="",
    )
    record = _canonical_record(
        "sdrpp", owner="receiver-a", bundle_id="receiver:sdrpp",
        components=("sdrpp",),
    )
    snapshot = build_software_administration_snapshot(
        [observer, transceiver], identity_records=(record,)
    )

    receiver = snapshot.family("receiver")
    assert receiver is not None
    assert [item.radio_id for item in receiver.assignments] == [10]
    assert receiver.assignments[0].canonical_bundle_id == "receiver:sdrpp"
    assert receiver.assignments[0].canonical_identity_key == "receiver-a:sdrpp:receiver:sdrpp"
    assert receiver.assignments[0].canonical_component_ids == ("sdrpp",)
    assert receiver.assignments[0].canonical_fingerprint
    assert receiver.assignments[0].canonical_parity_state == "verified"
    assert receiver.unassigned_instances == ()


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
