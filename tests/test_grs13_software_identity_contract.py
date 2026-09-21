from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace

import pytest

from freqinout.core.guided_radio_software_model import (
    AtomicInstanceBundle,
    CompletionPolicy,
    EndpointRecord,
    ExecutionScope,
    InstanceSourceMode,
    LaunchComponentRecord,
    ManagementMode,
    ResourceClaimRecord,
    SoftwareFamily,
)
from freqinout.core.guided_launch_recipes import (
    recipe_draft_updates,
    resolve_fast_light_managed_recipe,
    resolve_js8_managed_recipe,
)
from freqinout.core.guided_instance_inventory import (
    build_guided_instance_inventory,
    distinct_draft_seed,
)
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.software_administration_model import build_software_administration_snapshot
from freqinout.core.software_identity_bundle import (
    build_guided_identity_records,
    identity_record_from_mapping,
    identity_record_to_mapping,
    validate_identity_parity,
)
from freqinout.gui.settings_tab import SettingsTab


def test_real_js8_recipe_round_trips_radio_identity_into_software_administration(tmp_path) -> None:
    """The production resolver, persistence row, manifest, and SA view agree."""

    operator_home = tmp_path / "operator-home"
    seed = distinct_draft_seed(
        "js8call",
        owner_draft_key="add-radio-transaction-123",
        snapshot=build_guided_instance_inventory({}),
    )
    application_system_key = str(seed["application_system_key"])
    canonical_instance_key = f"js8call:{application_system_key}"
    guided_draft = {
        **seed,
        "instance_name": "FT-710",
        "owner_label": "FT-710",
        "rig_name": "FT-710",
        "variant": "js8call_subspace_4_1",
        "version": "4.1.0.478",
        "application_path": "/usr/bin/js8call-subspace",
        "host": "127.0.0.1",
        "port": 2443,
        "udp_port": 2243,
        "launch_at_startup": True,
    }
    resolution = resolve_js8_managed_recipe(
        guided_draft,
        managed_root=str(tmp_path / ".freqinout" / "managed-instances"),
        platform="linux",
        storage_home=operator_home,
    )
    updates = recipe_draft_updates(resolution)
    native_settings = str(operator_home / ".config" / "JS8Call - FT-710.ini")
    native_data = str(operator_home / ".local" / "share" / "JS8Call - FT-710")
    assert updates["rig_name"] == "FT-710"
    assert updates["configuration_path"] == native_settings
    assert updates["storage_path"] == native_data
    assert updates["launch_arguments"] == ["--rig-name", "FT-710"]

    store = MultiRadioStore(tmp_path / "settings.sqlite")
    radio = store.save_device_profile({
        "system_key": "ft-710",
        "name": "FT-710",
        "use_flrig": 0,
        "use_fldigi": 0,
        "use_flmsg": 0,
        "use_flamp": 0,
        "use_js8call": 1,
        "use_js8spotter": 0,
        "use_commstat": 0,
        "use_varac": 0,
    })
    adopted = store.adopt_software_instance(
        family_key="js8call",
        radio_profile_id=int(radio["id"]),
        application_values={
            "system_key": application_system_key,
            "name": "FT-710",
            "host": "127.0.0.1",
            "port": 2443,
            "profile_path": native_settings,
            "install_path": "/usr/bin/js8call-subspace",
            "rig_name": "FT-710",
            "rig_name_source": "managed",
            "variant_family": "js8call_subspace_4_1",
            "variant_version": "4.1.0.478",
            "application_data_root": native_data,
            "directed_path": f"{native_data}/DIRECTED.TXT",
            "all_path": f"{native_data}/ALL.TXT",
            "inbox_path": f"{native_data}/inbox.db3",
            "storage_mode": "rig_scoped",
        },
        manifest_values={
            "instance_key": canonical_instance_key,
            "management_mode": "fio_managed",
            "provenance": "managed",
            "configuration_path": native_settings,
            "configuration_root": native_settings,
            "data_root": native_data,
            "ports": [{"name": "api", "host": "127.0.0.1", "port": 2443}],
            "evidence": {"launch_recipe": updates["launch_recipe"]},
        },
        launch_at_startup=True,
    )
    assert adopted["application"]["rig_name"] == "FT-710"
    canonical_draft = {
        **guided_draft,
        **updates,
        "instance_key": canonical_instance_key,
        "ports": [{"name": "api", "protocol": "tcp", "host": "127.0.0.1", "port": 2443}],
    }
    records = build_guided_identity_records(radio, {"js8call": canonical_draft}, ("js8call",))
    store.save_radio_software_identity_records(int(radio["id"]), records)
    assert records[0].bundle_id == canonical_instance_key
    assert store.validate_radio_software_identity_projections(int(radio["id"])) == {}

    snapshot = build_software_administration_snapshot(
        store.list_device_profiles(),
        js8_instances=store.list_js8_instances(),
        instance_manifests=store.list_software_instance_manifests(),
    )
    assignment = snapshot.family("js8call").assignments[0]
    assert assignment.instance_name == "FT-710"
    assert assignment.configuration_summary == native_settings
    assert assignment.data_summary == native_data
    persisted_text = " ".join(
        (
            adopted["application"]["rig_name"],
            assignment.configuration_summary,
            assignment.data_summary,
            str(updates["effective_launch_command"]),
        )
    )
    assert str(seed["draft_instance_key"]) not in persisted_text
    assert "managed-instances" not in persisted_text


def test_guided_fast_light_save_round_trips_all_native_paths_and_components(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "settings.sqlite")
    radio = store.save_device_profile(
        {
            "system_key": "ft-710",
            "name": "FT-710",
            "device_class": "tx_rx",
            "use_flrig": 0,
            "use_fldigi": 0,
            "use_flmsg": 0,
            "use_flamp": 0,
        }
    )
    seed = distinct_draft_seed(
        "fast_light",
        owner_draft_key="add-radio-fast-light-123",
        snapshot=build_guided_instance_inventory({}),
        source={
            "application_path": "/Applications/flrig.app",
            "secondary_application_path": "/Applications/fldigi.app",
            "flmsg_application_path": "/Applications/flmsg.app",
            "flamp_application_path": "/Applications/flamp.app",
        },
    )
    draft = {
        **seed,
        "instance_name": "FT-710",
        "owner_label": "FT-710",
        "radio_role": "tx_rx",
        "host": "127.0.0.1",
        "port": 12346,
        "secondary_port": 7363,
        "launch_at_startup": True,
    }
    resolution = resolve_fast_light_managed_recipe(
        draft,
        managed_root=str(tmp_path / ".freqinout" / "managed-instances"),
        platform="darwin",
        storage_home=tmp_path / "operator-home",
    )
    draft.update(recipe_draft_updates(resolution))

    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = store
    tab._refresh_multi_radio_tables = lambda: None
    assert tab._adopt_guided_software_drafts(
        radio,
        {"fast_light": draft},
        expected_identity_generation=0,
    )

    saved = store.get_device_profile(int(radio["id"]))
    assert saved["use_flrig"] == saved["use_fldigi"] == 1
    assert saved["use_flmsg"] == saved["use_flamp"] == 1
    assert saved["flmsg_path"] == "/Applications/flmsg.app"
    assert saved["flamp_path"] == "/Applications/flamp.app"
    assert saved["flmsg_message_path"].endswith("/ICS/messages")
    assert saved["flamp_message_path"].endswith("/.nbems/FLAMP/rx")
    manifest = store.list_software_instance_manifests()[0]
    assert manifest["management_mode"] == "fio_managed"
    claims = {item["kind"]: item["value"] for item in manifest["resource_claims"]}
    assert claims["flmsg_messages"] == saved["flmsg_message_path"]
    assert claims["flamp_receive"] == saved["flamp_message_path"]
    launch = store.get_radio_launch_bundle(int(radio["id"]))
    items = {item["app_name"]: item for item in launch["items"]}
    assert set(items) >= {"FLRig", "FLDigi", "FLMsg", "FLAmp"}
    assert items["FLMsg"]["readiness"]["launch_arguments"][0] == "--flmsg-dir"
    assert items["FLAmp"]["launch_at_startup"] == 0
    assert items["FLAmp"]["readiness"]["operator_starts"] is True
    persisted = " ".join(
        (
            saved["fldigi_log_path"],
            saved["fldigi_checkin_dir"],
            saved["flmsg_message_path"],
            saved["flamp_message_path"],
        )
    )
    assert "draft-fast_light" not in persisted
    assert ".freqinout" not in persisted
    assert store.validate_radio_software_identity_projections(int(radio["id"])) == {}


def test_converted_varac_member_is_mirrored_without_losing_other_identities(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "converted-varac-identity.sqlite")
    old_radio = store.save_device_profile({
        "system_key": "ftdx10", "name": "FTDX-10", "use_flrig": 0,
        "use_fldigi": 0, "use_flmsg": 0, "use_flamp": 0,
        "use_js8spotter": 1,
    })
    new_radio = store.save_device_profile({
        "system_key": "ft710", "name": "FT-710", "use_flrig": 0,
        "use_fldigi": 0, "use_flmsg": 0, "use_flamp": 0,
    })
    install = "/opt/VarAC/VarAC.exe"
    shared_db = "/opt/VarAC/VarAC.db"
    old = store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=int(old_radio["id"]),
        application_values={
            "system_key": "varac-ftdx10", "name": "FTDX-10", "install_path": install,
            "ini_path": "/opt/VarAC/VarAC-ftdx10.ini", "db_path": shared_db,
            "vara_runtime_path": "/managed/ftdx10/VARA",
            "vara_ini_path": "/managed/ftdx10/VARA/VARA.ini",
            "incoming_path": "/files/FTDX10_In", "outbox_path": "/files/FTDX10_Out",
            "native_management_state": "managed",
        },
        manifest_values={
            "instance_key": "varac:varac-ftdx10",
            "resource_claims": [
                {"kind": "varac_executable", "value": install, "exclusive": True},
                {"kind": "working_directory", "value": "/opt/VarAC", "exclusive": True},
                {"kind": "varac_database", "value": shared_db, "exclusive": True},
            ],
        },
    )
    old_profile = store.get_device_profile(int(old_radio["id"]))
    initial = build_guided_identity_records(
        old_profile,
        {
            "varac": {
                "instance_key": "varac:varac-ftdx10",
                "management_mode": "fio_managed",
                "application_path": install,
                "configuration_path": "/opt/VarAC/VarAC-ftdx10.ini",
                "storage_path": shared_db,
                "secondary_storage_path": "/files/FTDX10_In",
                "outbox_path": "/files/FTDX10_Out",
                "resource_claims": old["manifest"]["resource_claims"],
            }
        },
        ("varac", "fio_spotter"),
    )
    store.save_radio_software_identity_records(int(old_radio["id"]), initial)
    added = store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=int(new_radio["id"]),
        application_values={
            "system_key": "varac-ft710", "name": "FT-710", "install_path": install,
            "ini_path": "/opt/VarAC/VarAC-ft710.ini", "db_path": shared_db,
            "vara_runtime_path": "/managed/ft710/VARA",
            "vara_ini_path": "/managed/ft710/VARA/VARA.ini",
            "incoming_path": "/files/FT710_In", "outbox_path": "/files/FT710_Out",
            "native_management_state": "managed",
        },
        manifest_values={"instance_key": "varac:varac-ft710"},
        varac_cluster_instance_number=2,
        varac_create_cluster_values={
            "name": "Home", "cluster_id": "HOME", "shared_db_path": shared_db,
            "shared_bbs_path": "/files/BBS", "shared_bbs_archive_path": "/files/BBS/Archive",
            "existing_standalone_node_id": old["application"]["id"],
            "existing_standalone_instance_number": 1,
            "native_management_state": "managed",
            "existing_standalone_application_values": {
                "launch_cmd": r"wine /opt/VarAC/VarAC.exe C:\\VarAC\\VarAC-ftdx10.ini",
                "native_management_state": "managed",
            },
            "existing_standalone_manifest_values": {
                "launch_command": r"wine /opt/VarAC/VarAC.exe C:\\VarAC\\VarAC-ftdx10.ini",
                "evidence": {
                    "launch_recipe": {
                        "status": "qualified_managed",
                        "components": ({
                            "component_key": "varac", "executable": "wine",
                            "arguments": (install, r"C:\\VarAC\\VarAC-ftdx10.ini"),
                            "working_directory": "/opt/VarAC",
                            "environment": {"WINEPREFIX": "/home/operator/.wine"},
                            "operator_starts": False,
                            "readiness": {"kind": "process"},
                        },),
                    }
                },
            },
        },
    )
    assert added["radio"]["varac_node_id"]
    existing_application = store.get_varac_node(int(old["application"]["id"]))
    existing_manifest = store.get_software_instance_manifest("varac:varac-ftdx10")
    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = store
    tab._sync_converted_varac_identity(
        radio_id=int(old_radio["id"]),
        member=SimpleNamespace(
            launch_command=("wine", install, r"C:\\VarAC\\VarAC-ftdx10.ini"),
            working_directory="/opt/VarAC",
            wine_prefix="/home/operator/.wine",
        ),
        manifest=existing_manifest,
        application=existing_application,
    )

    records = store.list_radio_software_identity_records(int(old_radio["id"]))
    assert {record.family_key for record in records} == {"varac", "fio_spotter"}
    varac = next(record for record in records if record.family_key == "varac")
    claims = {item["resource_type"]: item for item in varac.resources}
    assert claims["working_directory"]["exclusive"] is False
    assert claims["varac_database"]["exclusive"] is False
    assert varac.components[0].argv[:2] == ("wine", install)
    assert varac.components[0].cwd == "/opt/VarAC"
    assert store.validate_radio_software_identity_projections(int(old_radio["id"])) == {}


def _component(key: str, executable: str, *arguments: str, depends_on: tuple[str, ...] = ()) -> LaunchComponentRecord:
    return LaunchComponentRecord(
        component_key=key,
        executable=executable,
        arguments=arguments,
        working_directory=f"/operator/radios/ft-710/{key}",
        environment={"FIO_RADIO": "FT-710"},
        dependencies=depends_on,
        launch_at_startup=True,
        readiness_policy={"kind": "tcp", "host": "127.0.0.1"},
    )


def _bundle(family: SoftwareFamily, key: str, *, components: tuple[LaunchComponentRecord, ...] = (), endpoints: tuple[EndpointRecord, ...] = ()) -> AtomicInstanceBundle:
    return AtomicInstanceBundle(
        family=family,
        instance_key=key,
        source_mode=InstanceSourceMode.CREATE_DISTINCT,
        completion_policy=CompletionPolicy.REQUIRED,
        owner_radio_key="ft-710",
        management_mode=ManagementMode.FIO_MANAGED,
        configuration_path=f"/operator/radios/ft-710/{family.value}/config",
        data_path=f"/operator/radios/ft-710/{family.value}/data",
        message_paths=(f"/operator/messages/ft-710/{family.value}/inbox",),
        endpoints=endpoints,
        resources=(ResourceClaimRecord("profile", "directory", f"/operator/radios/ft-710/{family.value}"),),
        launch_components=components,
        provenance="guided",
        desired_fingerprint=f"desired-{family.value}",
        observed_fingerprint=f"observed-{family.value}",
        verification_evidence={"state": "reviewed"},
    )


def _selected_stack() -> tuple[dict[str, object], dict[str, AtomicInstanceBundle], tuple[str, ...]]:
    radio = {"id": 71, "system_key": "ft-710", "name": "FT-710", "device_class": "tx_rx"}
    drafts = {
        "js8call": _bundle(
            SoftwareFamily.JS8CALL,
            "js8call:ft-710",
            components=(_component("js8call", "/usr/bin/js8call-subspace", "--profile", "FT-710"),),
            endpoints=(EndpointRecord("api", "tcp", "127.0.0.1", 2443),),
        ),
        "fast_light": _bundle(
            SoftwareFamily.FAST_LIGHT,
            "fast-light:ft-710",
            components=(
                _component("flrig", "/usr/local/bin/flrig", "--config-dir", "/operator/radios/ft-710/flrig"),
                _component("fldigi", "/usr/local/bin/fldigi", "--config-dir", "/operator/radios/ft-710/fldigi", depends_on=("flrig",)),
                _component("flmsg", "/usr/local/bin/flmsg", depends_on=("fldigi",)),
                _component("flamp", "/usr/local/bin/flamp", depends_on=("fldigi",)),
            ),
            endpoints=(EndpointRecord("flrig", "tcp", "127.0.0.1", 12346), EndpointRecord("fldigi", "tcp", "127.0.0.1", 7363)),
        ),
        "varac": _bundle(
            SoftwareFamily.VARAC,
            "varac:ft-710",
            components=(
                _component("vara", "/opt/vara/Vara.exe"),
                _component("varac", "/opt/varac/VarAC.exe", "--ini", "/operator/radios/ft-710/varac/VarAC.ini", depends_on=("vara",)),
            ),
            endpoints=(EndpointRecord("command", "tcp", "127.0.0.1", 8310),),
        ),
        # FIO Spotter and CommStat are selected capabilities, not inferred from JS8Call.
        "fio_spotter": AtomicInstanceBundle(
            family=SoftwareFamily.FIO_SPOTTER,
            instance_key="fio-spotter:ft-710",
            source_mode=InstanceSourceMode.BUILT_IN,
            completion_policy=CompletionPolicy.REQUIRED,
            owner_radio_key="ft-710",
            management_mode=ManagementMode.BUILT_IN,
            execution_scope=ExecutionScope.BUILT_IN,
            provenance="guided",
        ),
        "commstat": _bundle(
            SoftwareFamily.COMMSTAT,
            "commstat:station",
            components=(_component("commstat", "/usr/bin/commstat"),),
        ),
    }
    return radio, drafts, tuple(drafts)


def test_guided_identity_records_cover_every_selected_family_and_exact_launch_identity() -> None:
    radio, drafts, selected = _selected_stack()

    records = build_guided_identity_records(radio, drafts, selected)
    mappings = tuple(identity_record_to_mapping(record) for record in records)

    assert len(records) == len({mapping["identity_key"] for mapping in mappings})
    assert {mapping["family_key"] for mapping in mappings} == set(selected)
    assert {component["component_id"] for mapping in mappings for component in mapping["components"]} >= {
        "js8call", "flrig", "fldigi", "flmsg", "flamp", "vara", "varac", "commstat",
        "fio-spotter",
    }
    assert all(
        {
            "bundle_id", "identity_key", "owner", "scope", "source_mode", "management_mode",
            "completion_policy", "provenance", "verification", "paths", "resources", "endpoints",
            "components", "launch", "readiness", "fingerprint",
        } <= set(mapping)
        for mapping in mappings
    )
    assert next(mapping for mapping in mappings if mapping["family_key"] == "fio_spotter")["bindings"] == [
        {"binding_id": "fio-spotter:ft-710", "kind": "built-in-radio", "radio_key": "ft-710"}
    ]
    commstat = next(mapping for mapping in mappings if mapping["family_key"] == "commstat")
    assert {binding["kind"] for binding in commstat["bindings"]} == {"station-process", "radio-js8-endpoint"}
    assert identity_record_from_mapping(mappings[0]) == records[0]
    with pytest.raises((FrozenInstanceError, AttributeError)):
        records[0].bundle_id = "mutated"  # type: ignore[misc]


def test_identity_parity_reports_selected_component_omission_duplicate_and_changed_recipe() -> None:
    radio, drafts, selected = _selected_stack()
    expected = build_guided_identity_records(radio, drafts, selected)
    fast_light = next(record for record in expected if record.family_key == "fast_light")
    altered = dict(identity_record_to_mapping(fast_light))
    altered["components"] = [
        item for item in altered["components"] if item["component_id"] != "flamp"
    ]
    altered["launch"] = {**dict(altered["launch"]), "argv": ["/wrong/executable"]}
    actual = tuple(
        identity_record_from_mapping(altered) if record is fast_light else record
        for record in expected
    )

    issues = validate_identity_parity(expected, actual)

    assert any("missing component flamp" in issue for issue in issues)
    assert any("launch" in issue for issue in issues)


def test_malformed_identity_records_are_rejected_before_persistence() -> None:
    radio, drafts, selected = _selected_stack()
    records = build_guided_identity_records(radio, drafts, selected)
    fast_light = next(record for record in records if record.family_key == "fast_light")
    duplicate_component = dict(identity_record_to_mapping(fast_light))
    duplicate_component["components"] = [
        *duplicate_component["components"],
        dict(duplicate_component["components"][0]),
    ]
    with pytest.raises(ValueError, match="duplicate software identity component"):
        identity_record_from_mapping(duplicate_component)

    spotter = next(record for record in records if record.family_key == "fio_spotter")
    missing_spotter_binding = dict(identity_record_to_mapping(spotter))
    missing_spotter_binding["bindings"] = []
    with pytest.raises(ValueError, match="FIO Spotter identity requires"):
        identity_record_from_mapping(missing_spotter_binding)

    invalid_owner = dict(identity_record_to_mapping(fast_light))
    invalid_owner["owner"] = "station"
    with pytest.raises(ValueError, match="only station services"):
        identity_record_from_mapping(invalid_owner)


def test_commstat_uses_one_station_service_identity_and_per_radio_bindings() -> None:
    radio, drafts, _ = _selected_stack()
    second_radio = {**radio, "id": 72, "system_key": "ic-705", "name": "IC-705"}
    first = build_guided_identity_records(radio, {"js8call": drafts["js8call"]}, ("commstat",))[0]
    second = build_guided_identity_records(second_radio, {"js8call": drafts["js8call"]}, ("commstat",))[0]

    assert first.family_key == second.family_key == "commstat"
    assert first.bundle_id == second.bundle_id == "commstat:station"
    first_station = next(item for item in first.bindings if item.kind == "station-process")
    second_station = next(item for item in second.bindings if item.kind == "station-process")
    assert first_station.binding_id == second_station.binding_id == "commstat:station"
    first_radio = next(item for item in first.bindings if item.kind == "radio-js8-endpoint")
    second_radio_binding = next(item for item in second.bindings if item.kind == "radio-js8-endpoint")
    assert first_radio.binding_id != second_radio_binding.binding_id
    assert (first_radio.radio_key, second_radio_binding.radio_key) == ("ft-710", "ic-705")
    assert dict(first.launch)["dependencies"] == {"commstat": ()}
    assert [dict(item) for item in dict(first.launch)["cross_family_dependencies"]["commstat"]] == [{
            "family_key": "js8call",
            "binding_id": first_radio.binding_id,
            "radio_key": "ft-710",
        }]


def test_store_replaces_a_radio_identity_set_atomically_and_rejects_stale_generation(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "settings.sqlite")
    radio = store.save_device_profile({"system_key": "ft-710", "name": "FT-710"})
    _, drafts, selected = _selected_stack()
    records = build_guided_identity_records(radio, drafts, selected)

    store.save_radio_software_identity_records(int(radio["id"]), records)
    saved = store.list_radio_software_identity_records(int(radio["id"]))
    assert validate_identity_parity(records, saved) == ()

    # A replacement removes records no longer selected; it cannot leave stale siblings behind.
    js8_only = tuple(record for record in records if record.family_key == "js8call")
    store.save_radio_software_identity_records(int(radio["id"]), js8_only, expected_generation=1)
    assert tuple(record.family_key for record in store.list_radio_software_identity_records(int(radio["id"]))) == ("js8call",)
    with pytest.raises(ValueError, match="stale.*generation"):
        store.save_radio_software_identity_records(int(radio["id"]), records, expected_generation=1)


def test_saved_canonical_identity_records_round_trip_into_software_administration(tmp_path) -> None:
    """Save/reload parity includes radio software and station-service bindings."""

    store = MultiRadioStore(tmp_path / "settings.sqlite")
    radio, drafts, selected = _selected_stack()
    radio["spotter_launch_path"] = "/opt/js8spotter"
    radio["device_class"] = "transceiver"
    radio["name"] = "FT-710"
    radio["system_key"] = "ft-710"
    js8 = store.save_js8_instance({
        "system_key": "js8:ft-710", "name": "FT-710 JS8",
        "spotter_launch_path": "/opt/js8spotter", "commstat_launch_path": "/opt/commstat",
    })
    fast = store.save_fast_light_config({"system_key": "fast-light:ft-710", "name": "FT-710 Fast Light"})
    varac = store.save_varac_node({"system_key": "varac:ft-710", "name": "FT-710 VarAC"})
    saved_profile = store.save_device_profile({
        "system_key": "ft-710", "name": "FT-710", "device_class": "tx_rx",
        "control_backend": "js8call",
        "js8_instance_id": int(js8["id"]), "fast_light_config_id": int(fast["id"]),
        "varac_node_id": int(varac["id"]),
        "use_js8call": 1, "use_js8spotter": 1, "use_commstat": 1,
        "use_flrig": 1, "use_fldigi": 1, "use_flmsg": 1, "use_flamp": 1,
        "use_varac": 1,
    })
    radio.update(saved_profile)
    drafts = {
        family: replace(
            bundle,
            owner_radio_key=str(radio["system_key"]),
            identity_fingerprint="",
            source_fingerprint="",
        ) if family not in {"fio_spotter", "commstat"}
        else bundle
        for family, bundle in drafts.items()
    }

    receiver_radio = store.save_device_profile({
        "system_key": "rtl-sdr", "name": "RTL-SDR", "device_class": "observer",
        "control_backend": "manual", "sdr_application": "SDR++", "sdr_adapter": "manual",
    })
    receiver_drafts = {
        "sdrpp": AtomicInstanceBundle(
            family=SoftwareFamily.SDRPP,
            instance_key="receiver:sdrpp:rtl-sdr",
            source_mode=InstanceSourceMode.CREATE_DISTINCT,
            completion_policy=CompletionPolicy.REQUIRED,
            owner_radio_key=str(receiver_radio["system_key"]),
            management_mode=ManagementMode.FIO_MANAGED,
            execution_scope=ExecutionScope.RECEIVE_ONLY,
            launch_components=(_component("sdrpp", "/usr/bin/sdrpp"),),
            provenance="guided",
        )
    }
    receiver_records = build_guided_identity_records(
        receiver_radio, receiver_drafts, ("sdrpp",)
    )
    expected = build_guided_identity_records(
        radio,
        drafts,
        (*selected, "external_js8spotter"),
    ) + receiver_records

    # Add Radio cancel/no-save does not create either canonical identity table.
    with store.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM radio_software_identity_sets").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM radio_software_identity_records").fetchone()[0] == 0

    store.save_radio_software_identity_records(int(radio["id"]), expected[:-1])
    store.save_radio_software_identity_records(int(receiver_radio["id"]), receiver_records)
    reloaded_main = store.list_radio_software_identity_records(int(radio["id"]))
    reloaded_receiver = store.list_radio_software_identity_records(int(receiver_radio["id"]))
    reloaded = reloaded_main + reloaded_receiver
    assert validate_identity_parity(expected, reloaded) == ()

    # A stale replacement is rejected atomically and preserves the reviewed set.
    before_stale = store.list_radio_software_identity_records(int(radio["id"]))
    with pytest.raises(ValueError, match="stale.*generation"):
        store.save_radio_software_identity_records(
            int(radio["id"]), before_stale[:-1], expected_generation=0
        )
    assert validate_identity_parity(before_stale, store.list_radio_software_identity_records(int(radio["id"]))) == ()

    snapshot = build_software_administration_snapshot(
        store.list_device_profiles(),
        js8_instances=store.list_js8_instances(),
        fast_light_configs=store.list_fast_light_configs(),
        varac_nodes=store.list_varac_nodes(),
        identity_records=reloaded,
    )
    family_by_canonical = {
        "sdrpp": "receiver",
        "external_js8spotter": "external_spotter",
    }
    expected_by_radio = {
        (int(radio["id"]), record.family_key): record for record in expected[:-1]
    }
    expected_by_radio[(int(receiver_radio["id"]), "sdrpp")] = receiver_records[0]
    for (target_radio_id, canonical_family), record in expected_by_radio.items():
        ui_family = family_by_canonical.get(canonical_family, canonical_family)
        assignments = snapshot.family(ui_family).assignments
        matching = [item for item in assignments if item.radio_id == target_radio_id]
        assert len(matching) == 1
        assignment = matching[0]
        assert assignment.canonical_bundle_id == record.bundle_id
        assert assignment.canonical_fingerprint == record.fingerprint
        assert assignment.canonical_component_ids == tuple(
            component.component_id for component in record.components
        )
        assert assignment.canonical_binding_ids == tuple(
            binding.binding_id for binding in record.bindings
        )


def test_production_guided_draft_mapping_synthesizes_selected_support_identity() -> None:
    """The assistant's persisted draft shape is a first-class identity input."""

    radio = {
        "id": 71,
        "system_key": "ft-710",
        "name": "FT-710",
        "device_class": "tx_rx",
        "use_fio_spotter": True,
        "use_commstat": True,
        "commstat_launch_path": "/usr/bin/commstat",
    }
    guided_drafts = {
        "js8call": {
            "family_key": "js8call",
            "mode": "create",
            "management_mode": "fio_managed",
            "draft_instance_key": "draft-js8-ft-710",
            "configuration_path": "/home/bill/.config/JS8Call/FT-710.ini",
            "storage_path": "/home/bill/.local/share/JS8Call/FT-710",
            "ports": [{"name": "api", "host": "127.0.0.1", "port": 2443}],
            "resource_claims": [{"resource_key": "profile", "resource_type": "directory", "value": "/home/bill/.local/share/JS8Call/FT-710"}],
            "launch_recipe": {
                "components": [{
                    "component_key": "js8call",
                    "executable": "/usr/bin/js8call-subspace",
                    "arguments": ("--profile", "FT-710"),
                    "working_directory": "/home/bill/Radio",
                    "environment": {"FIO_RADIO": "FT-710"},
                    "readiness": {"kind": "tcp", "host": "127.0.0.1", "port": "2443"},
                    "launch_at_startup": True,
                }]
            },
        },
        "fast_light": {
            "family_key": "fast_light",
            "mode": "create",
            "management_mode": "fio_managed",
            "draft_instance_key": "draft-fast-light-ft-710",
            "configuration_path": "/home/bill/.config/fldigi/FT-710",
            "storage_path": "/home/bill/.local/share/fldigi/FT-710",
            "ports": [{"name": "flrig", "port": 12346}, {"name": "fldigi", "port": 7363}],
            "resource_claims": [{"resource_key": "profile", "resource_type": "directory", "value": "/home/bill/.config/fldigi/FT-710"}],
            "launch_recipe": {
                "components": [{
                    "component_key": "flrig",
                    "executable": "/usr/local/bin/flrig",
                    "arguments": ("--config-dir", "/home/bill/.config/flrig/FT-710"),
                    "working_directory": "/home/bill/Radio",
                    "readiness_policy": {"kind": "tcp", "port": "12346"},
                }, {
                    "component_key": "fldigi",
                    "executable": "/usr/local/bin/fldigi",
                    "arguments": ("--config-dir", "/home/bill/.config/fldigi/FT-710"),
                    "working_directory": "/home/bill/Radio",
                    "dependencies": ("flrig",),
                    "readiness_policy": {"kind": "tcp", "port": "7363"},
                }]
            },
        },
    }

    records = build_guided_identity_records(
        radio,
        guided_drafts,
        ("js8call", "fast_light", "fio_spotter", "commstat"),
    )

    by_family = {record.family_key: record for record in records}
    assert set(by_family) == {"js8call", "fast_light", "fio_spotter", "commstat"}
    assert "external_js8spotter" not in by_family
    js8_component = by_family["js8call"].components[0]
    assert js8_component.argv == ("/usr/bin/js8call-subspace", "--profile", "FT-710")
    assert js8_component.cwd == "/home/bill/Radio"
    assert dict(js8_component.readiness) == {"kind": "tcp", "host": "127.0.0.1", "port": "2443"}
    assert {item.component_id for item in by_family["fast_light"].components} == {"flrig", "fldigi"}
    assert dict(by_family["fast_light"].readiness)["fldigi"] == {"kind": "tcp", "port": "7363"}
    assert by_family["fio_spotter"].owner == "station"
    assert by_family["commstat"].owner == "station"
    assert {item.kind for item in by_family["commstat"].bindings} == {"station-process", "radio-js8-endpoint"}


def _persist_js8_projection(store: MultiRadioStore, radio_name: str = "FT-710") -> tuple[dict[str, object], object]:
    """Persist the same JS8 fields Add Radio writes across its three stores."""

    radio_key = radio_name.casefold().replace("-", "_").replace(" ", "_")
    api_port = 2443 if radio_key == "ft_710" else 2444
    config_path = f"/operator/radios/{radio_key}/js8call/config"
    data_path = f"/operator/radios/{radio_key}/js8call/data"
    message_path = f"/operator/messages/{radio_key}/js8call/inbox"
    app = store.save_js8_instance({
        "system_key": f"js8-app:{radio_key}",
        "name": f"{radio_name} JS8Call",
        "profile_path": config_path,
        "application_data_root": data_path,
        "save_dir": data_path,
        "directed_path": message_path,
        "install_path": "/usr/bin/js8call",
        "host": "127.0.0.1",
        "port": api_port,
    })
    radio = store.save_device_profile({
        "system_key": radio_key,
        "name": radio_name,
        "device_class": "tx_rx",
        "runtime_active": 1,
        "js8_instance_id": int(app["id"]),
        "use_js8call": 1,
        "use_js8spotter": 0,
        "use_commstat": 0,
        "control_backend": "js8call",
    })
    bundle = replace(
        _bundle(
            SoftwareFamily.JS8CALL,
            f"js8call:{radio_key}",
            components=(
                LaunchComponentRecord(
                    component_key="js8call",
                    executable="/usr/bin/js8call",
                    arguments=("--profile", radio_name),
                    working_directory=f"/operator/radios/{radio_key}",
                    environment={"FIO_RADIO": radio_name},
                    readiness_policy={"kind": "tcp", "host": "127.0.0.1", "port": api_port},
                ),
            ),
            endpoints=(EndpointRecord("api", "tcp", "127.0.0.1", api_port),),
        ),
        owner_radio_key=radio_key,
        configuration_path=config_path,
        data_path=data_path,
        message_paths=(message_path,),
        identity_fingerprint="",
        source_fingerprint="",
    )
    record = build_guided_identity_records(
        {**radio, "system_key": radio_key},
        {"js8call": bundle},
        ("js8call",),
    )[0]
    store.save_software_instance_manifest({
        "instance_key": record.bundle_id,
        "family_key": "js8call",
        "application_system_key": str(app["system_key"]),
        "management_mode": "fio_managed",
        "provenance": "guided",
        "configuration_path": config_path,
        "configuration_root": config_path,
        "data_root": data_path,
        "executable_path": "/usr/bin/js8call",
        "ports": [{"name": "api", "host": "127.0.0.1", "port": api_port}],
    })
    store.save_radio_launch_bundle(
        int(radio["id"]),
        launch_enabled=True,
        items=[{
            "name": "JS8Call",
            "instance_key": f"{radio_key}:js8call",
            "enabled": True,
            "startup": bool(record.components[0].launch["at_startup"]),
            "path": "/usr/bin/js8call",
            "readiness_policy": {
                "executable": "/usr/bin/js8call",
                "launch_arguments": ["--profile", radio_name],
                "working_directory": f"/operator/radios/{radio_key}",
                "environment": {"FIO_RADIO": radio_name},
                "kind": "tcp",
                "host": "127.0.0.1",
                "port": api_port,
            },
        }],
    )
    store.save_radio_software_identity_records(int(radio["id"]), (record,))
    return radio, record


def test_saved_guided_identity_projection_is_clean_and_linked_path_drift_is_family_specific(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "projection.db")
    radio, _record = _persist_js8_projection(store)

    assert store.validate_radio_software_identity_projections(int(radio["id"])) == {}

    app = store.list_js8_instances()[0]
    store.save_js8_instance({**app, "profile_path": "/operator/radios/ft-710/changed/config"})

    issues = store.validate_radio_software_identity_projections(int(radio["id"]))
    assert set(issues) == {"js8call"}
    assert any("configuration path" in issue for issue in issues["js8call"])


def test_projection_validation_detects_a_selected_family_missing_from_committed_identity_set(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "missing-family.db")
    radio, _record = _persist_js8_projection(store)
    with store.connect() as conn:
        conn.execute(
            "DELETE FROM radio_software_identity_records WHERE radio_profile_id=? AND family_key='js8call'",
            (int(radio["id"]),),
        )
        conn.commit()

    assert store.radio_software_identity_generation(int(radio["id"])) == 1
    assert store.validate_radio_software_identity_projections(int(radio["id"])) == {
        "js8call": ("canonical identity record is missing",)
    }


def test_js8_assignment_does_not_implicitly_select_station_services(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "explicit-station-services.db")
    js8 = store.save_js8_instance({
        "system_key": "js8:ft-710",
        "name": "FT-710 JS8Call",
        "spotter_launch_path": "/opt/js8spotter",
        "commstat_launch_path": "/opt/commstat",
    })

    radio = store.save_device_profile({
        "system_key": "ft-710",
        "name": "FT-710",
        "js8_instance_id": int(js8["id"]),
        "use_js8call": 1,
    })

    assert int(radio["use_js8spotter"]) == 0
    assert int(radio["use_commstat"]) == 0


def test_explicit_fio_spotter_and_commstat_project_without_false_launch_drift(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "station-service-projection.db")
    radio, js8_record = _persist_js8_projection(store)
    js8 = store.list_js8_instances()[0]
    store.save_js8_instance({**js8, "commstat_launch_path": "/usr/bin/commstat"})
    store.save_device_profile({
        "id": int(radio["id"]),
        "use_js8spotter": 1,
        "use_commstat": 1,
    })
    profile = next(
        item for item in store.list_device_profiles() if int(item["id"]) == int(radio["id"])
    )
    support_records = build_guided_identity_records(
        profile,
        {
            "js8call": {
                "family_key": "js8call",
                "ports": [{"name": "api", "host": "127.0.0.1", "port": 2443}],
            }
        },
        ("fio_spotter", "commstat"),
    )
    store.save_radio_software_identity_records(
        int(radio["id"]),
        (js8_record, *support_records),
        expected_generation=1,
    )

    assert store.validate_radio_software_identity_projections(int(radio["id"])) == {}


def test_canonical_identity_rejects_cross_radio_owner_and_binding(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "identity-ownership.db")
    target = store.save_device_profile({"system_key": "ic-705", "name": "IC-705"})
    _radio, drafts, _selected = _selected_stack()
    js8_record = build_guided_identity_records(
        {"system_key": "ft-710", "name": "FT-710"},
        {"js8call": drafts["js8call"]},
        ("js8call",),
    )[0]
    with pytest.raises(ValueError, match="owner does not match"):
        store.save_radio_software_identity_records(int(target["id"]), (js8_record,))

    spotter = build_guided_identity_records(
        {"system_key": "ft-710", "name": "FT-710"},
        {},
        ("fio_spotter",),
    )[0]
    with pytest.raises(ValueError, match="binding does not match"):
        store.save_radio_software_identity_records(int(target["id"]), (spotter,))


def test_projection_validation_uses_readonly_connection(tmp_path, monkeypatch) -> None:
    store = MultiRadioStore(tmp_path / "readonly-projection.db")
    radio, _record = _persist_js8_projection(store)

    monkeypatch.setattr(
        store,
        "_connect",
        lambda: (_ for _ in ()).throw(AssertionError("write-capable connection used")),
    )

    assert store.validate_radio_software_identity_projections(int(radio["id"])) == {}


def test_production_shaped_all_family_projections_validate_after_reload(tmp_path) -> None:
    """Exercise the app/manifest/launch rows behind every selectable family."""

    store = MultiRadioStore(tmp_path / "all-family-projections.db")
    radio = store.save_device_profile({"system_key": "ft-710", "name": "FT-710"})
    radio_key = str(radio["system_key"])

    js8_draft = {
        "family_key": "js8call",
        "draft_instance_key": "js8call:ft-710",
        "instance_key": "js8call:ft-710",
        "management_mode": "fio_managed",
        "configuration_path": "/operator/JS8Call/FT-710.ini",
        "storage_path": "/operator/JS8Call/FT-710",
        "secondary_storage_path": "/operator/JS8Call/FT-710/DIRECTED.TXT",
        "ports": [{"name": "api", "host": "127.0.0.1", "port": 2443}],
        "external_spotter_launch_at_startup": True,
        "launch_recipe": {
            "status": "qualified_managed",
            "components": [{
                "component_key": "js8call",
                "executable": "/usr/bin/js8call-subspace",
                "arguments": ["--profile", "FT-710"],
                "working_directory": "/operator/JS8Call",
                "environment": {"FIO_RADIO": "FT-710"},
                "launch_at_startup": True,
                "readiness": {"kind": "tcp", "host": "127.0.0.1", "port": 2443},
            }],
        },
    }
    store.adopt_software_instance(
        family_key="js8call",
        radio_profile_id=int(radio["id"]),
        application_values={
            "system_key": "js8-ft-710",
            "name": "FT-710",
            "install_path": "/usr/bin/js8call-subspace",
            "profile_path": js8_draft["configuration_path"],
            "application_data_root": js8_draft["storage_path"],
            "save_dir": js8_draft["storage_path"],
            "directed_path": js8_draft["secondary_storage_path"],
            "host": "127.0.0.1",
            "port": 2443,
            "spotter_launch_path": "/opt/JS8Spotter/js8spotter",
            "commstat_launch_path": "/opt/CommStat/commstat",
        },
        manifest_values={
            "instance_key": "js8call:ft-710",
            "configuration_path": js8_draft["configuration_path"],
            "configuration_root": js8_draft["configuration_path"],
            "data_root": js8_draft["storage_path"],
            "executable_path": "/usr/bin/js8call-subspace",
            "ports": js8_draft["ports"],
            "evidence": {
                "launch_recipe": js8_draft["launch_recipe"],
                "external_spotter_launch_at_startup": True,
            },
        },
        launch_at_startup=True,
    )

    fast_draft = {
        "family_key": "fast_light",
        "draft_instance_key": "fast-light:ft-710",
        "instance_key": "fast-light:ft-710",
        "management_mode": "fio_managed",
        "configuration_path": "/operator/flrig/FT-710",
        "storage_path": "/operator/fldigi/FT-710/logs",
        "secondary_storage_path": "/operator/fldigi/FT-710/checkins",
        "ports": [
            {"name": "flrig", "host": "127.0.0.1", "port": 12346},
            {"name": "fldigi", "host": "127.0.0.1", "port": 7363},
        ],
        "launch_recipe": {
            "status": "qualified_managed",
            "components": [
                {
                    "component_key": "flrig", "executable": "/usr/local/bin/flrig",
                    "arguments": ["--config-dir", "/operator/flrig/FT-710"],
                    "working_directory": "/operator/FastLight", "launch_at_startup": True,
                    "readiness": {"kind": "tcp", "host": "127.0.0.1", "port": 12346},
                },
                {
                    "component_key": "fldigi", "executable": "/usr/local/bin/fldigi",
                    "arguments": ["--config-dir", "/operator/fldigi/FT-710"],
                    "working_directory": "/operator/FastLight", "dependencies": ["flrig"],
                    "launch_at_startup": True,
                    "readiness": {"kind": "tcp", "host": "127.0.0.1", "port": 7363},
                },
                {
                    "component_key": "flmsg", "executable": "/usr/local/bin/flmsg",
                    "working_directory": "/operator/FastLight", "dependencies": ["fldigi"],
                    "launch_at_startup": True, "readiness": {"kind": "process"},
                },
                {
                    "component_key": "flamp", "executable": "/usr/local/bin/flamp",
                    "working_directory": "/operator/FastLight", "dependencies": ["fldigi"],
                    "launch_at_startup": True, "readiness": {"kind": "process"},
                },
            ],
        },
    }
    store.adopt_software_instance(
        family_key="fast_light",
        radio_profile_id=int(radio["id"]),
        application_values={
            "system_key": "fast-light-ft-710", "name": "FT-710",
            "flrig_path": "/usr/local/bin/flrig", "flrig_profile_dir": "/operator/flrig/FT-710",
            "flrig_host": "127.0.0.1", "flrig_port": 12346,
            "fldigi_path": "/usr/local/bin/fldigi", "fldigi_profile_dir": "/operator/fldigi/FT-710",
            "fldigi_host": "127.0.0.1", "fldigi_port": 7363,
            "flmsg_path": "/usr/local/bin/flmsg", "flamp_path": "/usr/local/bin/flamp",
            "fldigi_log_path": fast_draft["storage_path"],
            "fldigi_checkin_dir": fast_draft["secondary_storage_path"],
        },
        manifest_values={
            "instance_key": "fast-light:ft-710",
            "configuration_path": fast_draft["configuration_path"],
            "configuration_root": fast_draft["configuration_path"],
            "data_root": fast_draft["storage_path"],
            "executable_path": "/usr/local/bin/flrig",
            "ports": fast_draft["ports"],
            "resource_claims": [
                {"kind": "flmsg_application", "value": "/usr/local/bin/flmsg"},
                {"kind": "flamp_application", "value": "/usr/local/bin/flamp"},
            ],
            "evidence": {"launch_recipe": fast_draft["launch_recipe"]},
        },
        launch_at_startup=True,
    )

    varac_draft = {
        "family_key": "varac",
        "draft_instance_key": "varac:ft-710",
        "instance_key": "varac:ft-710",
        "management_mode": "fio_managed",
        "configuration_path": "/operator/VarAC/FT-710/VarAC.ini",
        "storage_path": "/operator/VarAC/VarAC.db",
        "secondary_storage_path": "/operator/VaraFiles/FT-710_In",
        "outbox_path": "/operator/VaraFiles/FT-710_Out",
        "bbs_path": "/operator/VaraFiles/BBS",
        "bbs_archive_path": "/operator/VaraFiles/BBS/Archive",
        "ports": [
            {"name": "command", "host": "127.0.0.1", "port": 8310},
            {"name": "kiss", "host": "127.0.0.1", "port": 8312},
        ],
        "launch_recipe": {
            "status": "qualified_managed",
            "components": [
                {
                    "component_key": "vara", "executable": "wine",
                    "arguments": ["/operator/VARA-FT-710/VARA.exe"],
                    "working_directory": "/operator/VARA-FT-710", "launch_at_startup": True,
                    "readiness": {"kind": "process"},
                },
                {
                    "component_key": "varac", "executable": "wine",
                    "arguments": ["/operator/VarAC/VarAC.exe", r"Z:\\operator\\VarAC\\FT-710\\VarAC.ini"],
                    "working_directory": "/operator/VarAC", "dependencies": ["vara"],
                    "launch_at_startup": True, "readiness": {"kind": "process"},
                },
            ],
        },
    }
    store.adopt_software_instance(
        family_key="varac",
        radio_profile_id=int(radio["id"]),
        application_values={
            "system_key": "varac-ft-710", "name": "FT-710",
            "install_path": "/operator/VarAC", "ini_path": varac_draft["configuration_path"],
            "db_path": varac_draft["storage_path"], "incoming_path": varac_draft["secondary_storage_path"],
            "outbox_path": varac_draft["outbox_path"], "bbs_path": varac_draft["bbs_path"],
            "bbs_archive_path": varac_draft["bbs_archive_path"],
        },
        manifest_values={
            "instance_key": "varac:ft-710",
            "configuration_path": varac_draft["configuration_path"],
            "configuration_root": varac_draft["configuration_path"],
            "data_root": varac_draft["storage_path"],
            "executable_path": "wine", "ports": varac_draft["ports"],
            "evidence": {"launch_recipe": varac_draft["launch_recipe"]},
        },
        launch_at_startup=True,
    )

    store.save_device_profile({
        "id": int(radio["id"]), "use_js8spotter": 1, "use_commstat": 1,
    })
    profile = next(item for item in store.list_device_profiles() if int(item["id"]) == int(radio["id"]))
    records = build_guided_identity_records(
        profile,
        {"js8call": js8_draft, "fast_light": fast_draft, "varac": varac_draft},
        (
            "js8call", "fast_light", "varac", "fio_spotter",
            "external_js8spotter", "commstat",
        ),
    )
    store.save_radio_software_identity_records(int(radio["id"]), records)

    assert {
        record.bundle_id for record in records if record.family_key in {"js8call", "fast_light", "varac"}
    } == {
        item["instance_key"] for item in store.list_software_instance_manifests()
    }
    assert store.validate_radio_software_identity_projections(int(radio["id"])) == {}
    assert validate_identity_parity(
        records,
        store.list_radio_software_identity_records(int(radio["id"])),
    ) == ()

    receiver = store.save_device_profile({
        "system_key": "rtl-sdr", "name": "RTL-SDR", "device_class": "observer",
        "sdr_application": "SDR++", "receiver_launch_enabled": 1,
        "use_flrig": 0, "use_fldigi": 0, "use_flmsg": 0, "use_flamp": 0,
        "use_js8call": 0, "use_js8spotter": 0, "use_commstat": 0, "use_varac": 0,
    })
    receiver_bundle = AtomicInstanceBundle(
        family=SoftwareFamily.SDRPP,
        instance_key="receiver:sdrpp:rtl-sdr",
        source_mode=InstanceSourceMode.CREATE_DISTINCT,
        completion_policy=CompletionPolicy.REQUIRED,
        owner_radio_key=str(receiver["system_key"]),
        management_mode=ManagementMode.FIO_MANAGED,
        execution_scope=ExecutionScope.RECEIVE_ONLY,
        launch_components=(
            LaunchComponentRecord(
                component_key="sdrpp", executable="/usr/bin/sdrpp",
                launch_at_startup=True, readiness_policy={"kind": "process"},
                execution_scope=ExecutionScope.RECEIVE_ONLY,
            ),
        ),
        provenance="guided",
    )
    store.save_radio_launch_bundle(
        int(receiver["id"]), launch_enabled=True,
        items=[{
            "name": "SDR++", "instance_key": "receiver:sdrpp:rtl-sdr", "enabled": True,
            "startup": True, "path": "/usr/bin/sdrpp",
            "readiness_policy": {"executable": "/usr/bin/sdrpp", "kind": "process"},
        }],
    )
    receiver_record = build_guided_identity_records(
        receiver, {"sdrpp": receiver_bundle}, ("sdrpp",)
    )[0]
    store.save_radio_software_identity_records(int(receiver["id"]), (receiver_record,))
    assert store.validate_radio_software_identity_projections(int(receiver["id"])) == {}


def test_launch_recipe_drift_warns_without_blocking_scoped_radio(tmp_path, monkeypatch) -> None:
    from types import SimpleNamespace

    from freqinout.core.launch_orchestrator import LaunchOrchestrator
    from freqinout.core.station_launch_planner import LaunchPlan

    store = MultiRadioStore(tmp_path / "launch-projection.db")
    affected, _ = _persist_js8_projection(store, "FT-710")
    unrelated, _ = _persist_js8_projection(store, "IC-705")
    items = store.get_radio_launch_bundle(int(affected["id"]))["items"]
    items[0]["readiness"]["executable"] = "/usr/bin/replaced-js8call"
    store.save_radio_launch_bundle(
        int(affected["id"]),
        launch_enabled=True,
        items=[{
            "name": items[0]["app_name"],
            "instance_key": items[0]["instance_key"],
            "enabled": bool(items[0]["enabled"]),
            "startup": bool(items[0]["launch_at_startup"]),
            "path": items[0]["path_override"],
            "command": items[0]["command_override"],
            "dependencies": items[0]["dependencies"],
            "readiness_policy": items[0]["readiness"],
        }],
    )

    issues = store.validate_radio_software_identity_projections(int(affected["id"]))
    assert set(issues) == {"js8call"}
    assert any("executable differs" in issue for issue in issues["js8call"])
    assert store.validate_radio_software_identity_projections(int(unrelated["id"])) == {}

    # Projection drift remains visible, but the exact reviewed launch bundle
    # stays eligible.  Launch-time recipe/resource validation remains the
    # blocking boundary.
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    eligible: list[int] = []
    profiles = [
            {"id": int(affected["id"]), "runtime_active": 1},
            {"id": int(unrelated["id"]), "runtime_active": 1},
    ]
    orchestrator.multi_radio_store = SimpleNamespace(
        list_device_profiles=lambda: list(profiles),
        list_runtime_active_device_profiles=lambda: list(profiles),
        validate_radio_software_identity_projections=lambda radio_id: store.validate_radio_software_identity_projections(radio_id),
        radio_software_identity_generation=lambda _radio_id: 1,
        varac_native_launch_blockers=lambda: [],
    )
    orchestrator.get_radio_launch_bundle = lambda radio_id: {"launch_enabled": False, "items": []}
    def plan_startup(profiles, _bundles, **kwargs):
        scope = kwargs.get("scope_radio_id")
        eligible.extend(
            int(profile["id"])
            for profile in profiles
            if scope is None or int(profile["id"]) == int(scope)
        )
        return LaunchPlan(kwargs.get("trigger", "startup"), scope, ())

    orchestrator.planner = SimpleNamespace(
        plan_startup=plan_startup
    )
    orchestrator._with_effective_launch_preview = lambda plan: plan

    plan = orchestrator.preview_manual_plan(int(affected["id"]))
    assert plan.scope_radio_id == int(affected["id"])
    assert eligible == [int(affected["id"])]
    warning = orchestrator.projection_warning_detail(int(affected["id"]))
    assert "js8call" in warning
    assert "executable differs" in warning

    eligible.clear()
    plan = orchestrator.preview_manual_plan(int(unrelated["id"]))
    assert plan.scope_radio_id == int(unrelated["id"])
    assert eligible == [int(unrelated["id"])]
    assert orchestrator.projection_warning_detail(int(unrelated["id"])) == ""


def test_launch_control_preferences_are_not_canonical_recipe_drift(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "launch-preferences.db")
    radio, _record = _persist_js8_projection(store)
    item = store.get_radio_launch_bundle(int(radio["id"]))["items"][0]

    store.save_radio_launch_bundle(
        int(radio["id"]),
        launch_enabled=True,
        items=[{
            "name": item["app_name"],
            "instance_key": item["instance_key"],
            "enabled": bool(item["enabled"]),
            "startup": not bool(item["launch_at_startup"]),
            "monitor_health": not bool(item["monitor_health"]),
            "path": item["path_override"],
            "command": item["command_override"],
            "dependencies": item["dependencies"],
            "readiness_policy": item["readiness"],
        }],
    )

    assert store.validate_radio_software_identity_projections(int(radio["id"])) == {}


@pytest.mark.parametrize(
    ("drift", "expected_issue"),
    (
        ("arguments", "arguments differ"),
        ("working_directory", "working directory differs"),
        ("environment", "environment differs"),
        ("dependencies", "dependencies differ"),
        ("readiness", "readiness differs"),
        ("missing_executable", "executable differs"),
    ),
)
def test_launch_projection_detects_each_exact_recipe_drift(tmp_path, drift, expected_issue) -> None:
    store = MultiRadioStore(tmp_path / f"{drift}.db")
    radio, _record = _persist_js8_projection(store)
    item = store.get_radio_launch_bundle(int(radio["id"]))["items"][0]
    readiness = dict(item["readiness"])
    path_override = str(item["path_override"] or "")
    command_override = str(item["command_override"] or "")
    dependencies = list(item["dependencies"])
    if drift == "arguments":
        readiness["launch_arguments"] = ["--profile", "wrong-radio"]
    elif drift == "working_directory":
        readiness["working_directory"] = "/operator/wrong-radio"
    elif drift == "environment":
        readiness["environment"] = {"FIO_RADIO": "wrong-radio"}
    elif drift == "dependencies":
        dependencies = ["unrelated-service"]
    elif drift == "readiness":
        readiness["port"] = int(readiness["port"]) + 1
    elif drift == "missing_executable":
        readiness["executable"] = ""
        path_override = ""
        command_override = ""
    store.save_radio_launch_bundle(
        int(radio["id"]),
        launch_enabled=True,
        items=[{
            "name": item["app_name"],
            "instance_key": item["instance_key"],
            "enabled": bool(item["enabled"]),
            "startup": bool(item["launch_at_startup"]),
            "path": path_override,
            "command": command_override,
            "dependencies": dependencies,
            "readiness_policy": readiness,
        }],
    )

    issues = store.validate_radio_software_identity_projections(int(radio["id"]))
    assert set(issues) == {"js8call"}
    assert any(expected_issue in issue for issue in issues["js8call"])


def test_launch_projection_does_not_match_an_unrelated_radios_launch_row(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "radio-scope.db")
    target, _ = _persist_js8_projection(store, "FT-710")
    unrelated, _ = _persist_js8_projection(store, "IC-705")
    target_id = int(target["id"])
    unrelated_items = store.get_radio_launch_bundle(int(unrelated["id"]))["items"]
    assert len(unrelated_items) == 1

    # The target identity has no launch row; the other radio has the same
    # application label and a complete, valid row that must not satisfy it.
    store.save_radio_launch_bundle(target_id, launch_enabled=True, items=[])

    issues = store.validate_radio_software_identity_projections(target_id)
    assert set(issues) == {"js8call"}
    assert any("missing or duplicated" in issue for issue in issues["js8call"])
