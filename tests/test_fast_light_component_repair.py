from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from freqinout.core.guided_launch_recipes import (
    recipe_draft_updates,
    recipe_resolution_from_mapping,
    resolve_fast_light_managed_recipe,
)
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.software_identity_bundle import (
    build_guided_identity_records,
    identity_record_from_mapping,
    identity_record_to_mapping,
)


def _without_pairs(arguments: list[str], flags: set[str]) -> list[str]:
    result: list[str] = []
    index = 0
    while index < len(arguments):
        if arguments[index] in flags:
            index += 2
            continue
        result.append(arguments[index])
        index += 1
    return result


def _launch_item(component: dict[str, object], bundle_id: str) -> dict[str, object]:
    key = str(component["component_key"])
    executable = str(component.get("executable") or "")
    arguments = list(component.get("arguments") or ())
    readiness = {
        **dict(component.get("readiness") or {}),
        "executable": executable,
        "launch_arguments": arguments,
        "working_directory": str(component.get("working_directory") or ""),
        "profile_selector": str(component.get("profile_selector") or ""),
        "configuration_roots": list(component.get("configuration_roots") or ()),
        "data_roots": list(component.get("data_roots") or ()),
        "endpoints": list(component.get("endpoints") or ()),
        "evidence": dict(component.get("evidence") or {}),
        "confidence": str(component.get("confidence") or "verified"),
        "operator_starts": bool(component.get("operator_starts", False)),
        "effective_command": [executable, *arguments] if executable and arguments else [],
        "effective_command_text": " ".join([executable, *arguments]).strip() if arguments else "",
        "environment": {},
        "execution_scope": str(component.get("execution_scope") or "standard"),
    }
    return {
        "name": {"flrig": "FLRig", "fldigi": "FLDigi", "flmsg": "FLMsg", "flamp": "FLAmp"}[key],
        "instance_key": "fast-light:station-shared:flamp" if key == "flamp" else f"{bundle_id}:{key}",
        "enabled": True,
        "startup": key in {"flrig", "fldigi", "flamp"},
        "monitor_health": True,
        "launch_path_override": executable,
        "dependencies": list(component.get("dependencies") or ()),
        "readiness_policy": readiness,
    }


def test_legacy_flmsg_flamp_repair_is_component_scoped_and_preserves_preferences(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    store = MultiRadioStore(tmp_path / "settings.sqlite")

    # An earlier radio with legacy FLAmp reserves the conventional 7322 slot.
    first_fast = store.save_fast_light_config({"system_key": "fast-one", "name": "Radio One"})
    store.save_device_profile({
        "system_key": "radio-one",
        "name": "Radio One",
        "fast_light_config_id": first_fast["id"],
        "use_flamp": 1,
    })

    fast = store.save_fast_light_config({
        "system_key": "fast-ft710",
        "name": "FT-710 Fast Light",
        "flrig_path": "/usr/local/bin/flrig",
        "flrig_host": "127.0.0.1",
        "flrig_port": 12346,
        "fldigi_path": "/usr/local/bin/fldigi",
        "fldigi_host": "127.0.0.1",
        "fldigi_port": 7363,
        "fldigi_log_path": str(home / ".fldigi/instances/FT-710/logs"),
        "fldigi_checkin_dir": str(home / ".nbems/instances/FT-710/WRAP/auto"),
    })
    radio = store.save_device_profile({
        "system_key": "ft-710",
        "name": "FT-710",
        "fast_light_config_id": fast["id"],
        "use_flrig": 1,
        "use_fldigi": 1,
        "use_flmsg": 1,
        "use_flamp": 1,
        "use_js8spotter": 1,
        "flrig_host": "127.0.0.1",
        "flrig_port": 12346,
        "fldigi_host": "127.0.0.1",
        "fldigi_port": 7363,
        "fldigi_log_path": str(home / ".fldigi/instances/FT-710/logs"),
        "fldigi_checkin_dir": str(home / ".nbems/instances/FT-710/WRAP/auto"),
        "flmsg_path": "/usr/local/bin/flmsg",
        "flmsg_message_path": str(home / ".nbems/instances/FT-710/ICS/messages"),
        "flamp_path": "/usr/local/bin/flamp",
        "flamp_message_path": str(home / ".nbems/FLAMP/rx"),
    })
    bundle_id = "fast_light:fast-ft710"
    draft = {
        "family_key": "fast_light",
        "mode": "managed",
        "draft_instance_key": bundle_id,
        "instance_key": bundle_id,
        "application_system_key": fast["system_key"],
        "instance_name": "FT-710",
        "owner_label": "FT-710",
        "radio_role": "transceiver",
        "application_path": "/usr/local/bin/flrig",
        "secondary_application_path": "/usr/local/bin/fldigi",
        "flmsg_application_path": "/usr/local/bin/flmsg",
        "flamp_application_path": "/usr/local/bin/flamp",
        "use_flmsg": True,
        "use_flamp": True,
        "host": "127.0.0.1",
        "port": 12346,
        "secondary_port": 7363,
        "arq_port": 7323,
    }
    resolution = resolve_fast_light_managed_recipe(
        draft,
        managed_root="",
        platform="linux",
        storage_home=home,
    )
    updates = recipe_draft_updates(resolution)
    modern_recipe = deepcopy(updates["launch_recipe"])
    legacy_recipe = deepcopy(modern_recipe)
    by_key = {item["component_key"]: item for item in legacy_recipe["components"]}
    fldigi_args = _without_pairs(
        list(by_key["fldigi"]["arguments"]),
        {"--arq-server-address", "--arq-server-port", "--flmsg-dir"},
    )
    by_key["fldigi"]["arguments"] = fldigi_args
    by_key["fldigi"]["effective_command"] = ["/usr/local/bin/fldigi", *fldigi_args]
    by_key["fldigi"]["effective_command_text"] = " ".join(by_key["fldigi"]["effective_command"])
    auto_path = str(home / ".nbems/instances/FT-710/WRAP/auto")
    by_key["flmsg"]["arguments"].extend(["--auto-dir", auto_path])
    by_key["flmsg"]["effective_command"] = ["/usr/local/bin/flmsg", *by_key["flmsg"]["arguments"]]
    by_key["flmsg"]["effective_command_text"] = " ".join(by_key["flmsg"]["effective_command"])
    by_key["flamp"].update({
        "arguments": [],
        "effective_command": [],
        "effective_command_text": "",
        "working_directory": "",
        "profile_selector": "",
        "configuration_roots": [],
        "managed_directories": [],
        "data_roots": [str(home / ".nbems/FLAMP/rx"), str(home / ".nbems/FLAMP/tx")],
        "endpoints": [],
        "readiness": {},
        "operator_starts": True,
    })
    legacy_recipe = recipe_resolution_from_mapping(legacy_recipe).to_mapping()

    draft.update(updates)
    draft.update({
        "launch_recipe": legacy_recipe,
        "launch_recipe_fingerprint": legacy_recipe["fingerprint"],
        "configuration_path": str(home / ".flrig/instances/FT-710"),
        "storage_path": str(home / ".fldigi/instances/FT-710/logs"),
        "secondary_storage_path": auto_path,
        "flmsg_message_path": str(home / ".nbems/instances/FT-710/ICS/messages"),
        "flamp_receive_path": str(home / ".nbems/FLAMP/rx"),
        "flamp_outgoing_path": str(home / ".nbems/FLAMP/tx"),
        "ports": [item for item in updates["ports"] if "ARQ" not in str(item.get("name"))],
        "resource_claims": [
            item for item in updates["resource_claims"] if not str(item.get("kind", "")).startswith("flamp")
        ] + [
            {"kind": "flamp_application", "value": "/usr/local/bin/flamp", "exclusive": False},
            {"kind": "flamp_receive", "value": str(home / ".nbems/FLAMP/rx"), "exclusive": False},
            {"kind": "flamp_outgoing", "value": str(home / ".nbems/FLAMP/tx"), "exclusive": False},
        ],
    })
    records = build_guided_identity_records(
        radio,
        {"fast_light": draft},
        ("fast_light", "fio_spotter"),
    )
    store.save_software_instance_manifest({
        "instance_key": bundle_id,
        "family_key": "fast_light",
        "application_system_key": fast["system_key"],
        "management_mode": "fio_managed",
        "provenance": "managed",
        "executable_path": "/usr/local/bin/flrig",
        "configuration_path": draft["configuration_path"],
        "configuration_root": draft["configuration_path"],
        "data_root": draft["storage_path"],
        "ports": draft["ports"],
        "resource_claims": draft["resource_claims"],
        "evidence": {"launch_recipe": legacy_recipe, "launch_recipe_fingerprint": legacy_recipe["fingerprint"]},
    })
    store.save_radio_software_identity_records(int(radio["id"]), records)
    legacy_items = [_launch_item(item, bundle_id) for item in legacy_recipe["components"]]
    legacy_items.append({
        "name": "Station Tool",
        "instance_key": "custom:station-tool",
        "enabled": True,
        "startup": False,
        "monitor_health": False,
        "launch_path_override": "/opt/station/tool",
        "dependencies": [],
        "readiness_policy": {"execution_scope": "standard"},
    })
    store.save_radio_launch_bundle(int(radio["id"]), launch_enabled=True, items=legacy_items)

    before = {
        record.family_key: identity_record_to_mapping(record)
        for record in store.list_radio_software_identity_records(int(radio["id"]))
    }
    before_fast_components = {
        item["component_id"]: item for item in before["fast_light"]["components"]
    }
    plan = store.prepare_fast_light_message_component_repair(
        int(radio["id"]), platform="linux", storage_home=home
    )
    assert plan.arq_port == 7323
    assert not (home / ".nbems/instances/FT-710").exists()

    original_save_launch = store.save_radio_launch_bundle

    def _fail_launch_save(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected launch persistence failure")

    monkeypatch.setattr(store, "save_radio_launch_bundle", _fail_launch_save)
    with pytest.raises(RuntimeError, match="injected launch persistence failure"):
        store.apply_fast_light_message_component_repair(plan)
    assert store.radio_software_identity_generation(int(radio["id"])) == plan.identity_generation
    assert {
        record.family_key: identity_record_to_mapping(record)
        for record in store.list_radio_software_identity_records(int(radio["id"]))
    } == before
    assert store.get_device_profile(int(radio["id"]))["flamp_message_path"] == str(
        home / ".nbems/FLAMP/rx"
    )
    monkeypatch.setattr(store, "save_radio_launch_bundle", original_save_launch)

    store.apply_fast_light_message_component_repair(plan)

    after = {
        record.family_key: identity_record_to_mapping(record)
        for record in store.list_radio_software_identity_records(int(radio["id"]))
    }
    after_fast_components = {
        item["component_id"]: item for item in after["fast_light"]["components"]
    }
    assert after["fio_spotter"] == before["fio_spotter"]
    assert after_fast_components["flrig"] == before_fast_components["flrig"]
    before_fldigi = dict(before_fast_components["fldigi"])
    after_fldigi = dict(after_fast_components["fldigi"])
    before_fldigi.pop("argv")
    after_fldigi.pop("argv")
    assert after_fldigi == before_fldigi
    assert "--arq-server-port" in after_fast_components["fldigi"]["argv"]
    assert "--flmsg-dir" in after_fast_components["fldigi"]["argv"]
    assert "--auto-dir" not in after_fast_components["flmsg"]["argv"]
    assert after_fast_components["flamp"]["argv"][-1] == "7363"
    assert after_fast_components["flamp"]["cwd"] == str(home / ".nbems/instances/FT-710")
    assert store.validate_radio_software_identity_projections(int(radio["id"])).get("fast_light") is None
    saved_profile = store.get_device_profile(int(radio["id"]))
    assert saved_profile["flamp_message_path"] == str(home / ".nbems/instances/FT-710/FLAMP/rx")
    launch = store.get_radio_launch_bundle(int(radio["id"]))
    items = {item["app_name"]: item for item in launch["items"]}
    assert items["FLMsg"]["launch_at_startup"] == 0
    assert items["FLAmp"]["launch_at_startup"] == 1
    assert items["FLAmp"]["instance_key"] == f"{bundle_id}:flamp"
    assert items["Station Tool"]["monitor_health"] == 0
    assert not (home / ".nbems/instances/FT-710").exists()

    with pytest.raises(ValueError, match="changed after review"):
        store.apply_fast_light_message_component_repair(plan)
