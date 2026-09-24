from __future__ import annotations

from pathlib import Path

from freqinout.core.config_lab_preset import (
    LAB_OPERATING_PLAN_NAME,
    apply_lab_radio_preset_to_store,
    build_lab_radio_profile_values,
)
from freqinout.core.config_autodiscovery import build_radio_instance_proposals
from freqinout.core.config_js8_managed import build_js8call_managed_profile_plans
from freqinout.core.multi_radio_store import MultiRadioStore


def test_lab_profile_values_are_varac_off_and_ported_from_proposal(tmp_path) -> None:
    proposal = build_radio_instance_proposals(radio_count=1, busy_checker=lambda _host, _port: False)[0]

    values = build_lab_radio_profile_values(
        proposal,
        app_paths={
            "flrig": "/Applications/RadioApps/flrig.app",
            "fldigi": "/Applications/RadioApps/fldigi.app",
            "js8call": "/Applications/JS8Call.app",
        },
        config_root=tmp_path / "fio-config",
        existing_device_id=7,
        js8_instance_id=8,
        fast_light_config_id=9,
    )

    assert values["id"] == 7
    assert values["system_key"] == "lab_radio_a"
    assert values["name"] == "Radio A"
    assert values["runtime_active"] == 1
    assert values["runtime_primary"] == 1
    assert values["control_backend"] == "flrig"
    assert values["use_flrig"] == 1
    assert values["use_fldigi"] == 1
    assert values["use_js8call"] == 1
    assert values["use_varac"] == 0
    assert values["flrig_port"] == 12345
    assert values["fldigi_port"] == 7362
    assert values["js8_port"] == 2442
    assert values["flrig_path"] == "/Applications/RadioApps/flrig.app"
    assert values["fldigi_path"] == "/Applications/RadioApps/fldigi.app"
    assert values["js8_install_path"] == "/Applications/JS8Call.app"
    assert values["js8_instance_id"] == 8
    assert values["fast_light_config_id"] == 9
    js8_plan = build_js8call_managed_profile_plans(
        (proposal,),
        config_root=tmp_path / "fio-config",
        js8call_path="/Applications/JS8Call.app",
    )[0]
    assert values["js8_directed_path"] == str(js8_plan.directed_path)
    assert values["js8_forms_path"] == str(js8_plan.forms_dir)


def test_apply_lab_radio_preset_creates_three_idempotent_runtime_radios(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    app_paths = {
        "flrig": "/Applications/RadioApps/flrig.app",
        "fldigi": "/Applications/RadioApps/fldigi.app",
        "js8call": "/Applications/JS8Call.app",
    }

    first = apply_lab_radio_preset_to_store(
        store,
        radio_count=3,
        app_paths=app_paths,
        config_root=tmp_path / "fio-config",
        busy_checker=lambda _host, _port: False,
    )
    second = apply_lab_radio_preset_to_store(
        store,
        radio_count=3,
        app_paths=app_paths,
        config_root=tmp_path / "fio-config",
        busy_checker=lambda _host, _port: False,
    )

    radios = store.list_device_profiles()
    by_key = {str(row["system_key"]): row for row in radios}
    js8_instances = store.list_js8_instances()
    js8_by_key = {str(row["system_key"]): row for row in js8_instances}
    fast_light_configs = store.list_fast_light_configs()
    assignments = store.list_effective_assignments()
    plans_by_id = {int(row["id"]): row for row in store.list_operating_profiles()}

    assert first.summary == "Created or updated 3 lab radio profile(s)."
    assert second.radio_profile_ids == first.radio_profile_ids
    assert first.managed_paths == second.managed_paths
    assert all(path.is_dir() for path in first.managed_paths)
    assert tmp_path / "fio-config" / "managed-instances" / "fio-a" / "flrig" in first.managed_paths
    assert tmp_path / "fio-config" / "managed-instances" / "fio-b" / "fldigi" / "logs" in first.managed_paths
    js8_plans = build_js8call_managed_profile_plans(
        build_radio_instance_proposals(radio_count=3, busy_checker=lambda _host, _port: False),
        config_root=tmp_path / "fio-config",
        js8call_path=app_paths["js8call"],
    )
    radio_c_js8 = js8_plans[2]
    assert radio_c_js8.save_dir in first.managed_paths
    assert radio_c_js8.forms_dir in first.managed_paths
    assert set(by_key) == {"lab_radio_a", "lab_radio_b", "lab_radio_c"}
    assert [by_key[key]["name"] for key in ("lab_radio_a", "lab_radio_b", "lab_radio_c")] == [
        "Radio A",
        "Radio B",
        "Radio C",
    ]
    assert by_key["lab_radio_a"]["runtime_primary"] == 1
    assert all(int(by_key[key]["runtime_active"]) == 1 for key in by_key)
    assert all(int(by_key[key]["use_varac"]) == 0 for key in by_key)
    assert by_key["lab_radio_b"]["flrig_port"] == 12346
    assert by_key["lab_radio_c"]["fldigi_port"] == 7364
    assert by_key["lab_radio_c"]["js8_port"] == 2444
    assert len(js8_instances) == 3
    assert len(fast_light_configs) == 3
    assert {row["system_key"] for row in js8_instances} == {
        "lab_js8_fio_a",
        "lab_js8_fio_b",
        "lab_js8_fio_c",
    }
    assert js8_by_key["lab_js8_fio_c"]["profile_path"] == str(radio_c_js8.save_dir)
    assert js8_by_key["lab_js8_fio_c"]["save_dir"] == str(radio_c_js8.save_dir)
    assert js8_by_key["lab_js8_fio_c"]["directed_path"] == str(radio_c_js8.directed_path)
    assert js8_by_key["lab_js8_fio_c"]["all_path"] == str(radio_c_js8.all_path)
    assert js8_by_key["lab_js8_fio_c"]["inbox_path"] == str(radio_c_js8.inbox_path)
    assert js8_by_key["lab_js8_fio_c"]["forms_path"] == str(radio_c_js8.forms_dir)
    assert js8_by_key["lab_js8_fio_c"]["application_data_root"] == str(radio_c_js8.application_data_root)
    assert js8_by_key["lab_js8_fio_c"]["rig_name"] == radio_c_js8.rig_name
    assert js8_by_key["lab_js8_fio_c"]["storage_mode"] == "rig_scoped"
    assert by_key["lab_radio_c"]["js8_profile_path"] == str(radio_c_js8.save_dir)
    assert by_key["lab_radio_c"]["js8_directed_path"] == str(radio_c_js8.directed_path)
    assert by_key["lab_radio_c"]["js8_forms_path"] == str(radio_c_js8.forms_dir)
    assert {row["system_key"] for row in fast_light_configs} == {
        "lab_fast_light_fio_a",
        "lab_fast_light_fio_b",
        "lab_fast_light_fio_c",
    }
    assert {plans_by_id[int(row["operating_profile_id"])]["name"] for row in assignments} == {LAB_OPERATING_PLAN_NAME}


def test_apply_lab_radio_preset_moves_busy_tcp_ports_forward(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    busy_ports = {12345, 12355, 2442}

    apply_lab_radio_preset_to_store(
        store,
        radio_count=1,
        config_root=tmp_path / "fio-config",
        busy_checker=lambda _host, port: port in busy_ports,
    )

    radio = store.list_device_profiles()[0]
    assert radio["flrig_port"] == 12356
    assert radio["js8_port"] == 2452
