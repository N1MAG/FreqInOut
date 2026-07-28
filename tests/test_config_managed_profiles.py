from __future__ import annotations

from freqinout.core.config_autodiscovery import build_lab_radio_proposals
from freqinout.core.config_managed_profiles import (
    build_flrig_fldigi_managed_profile_plans,
    create_managed_profile_directories,
)


def test_managed_flrig_fldigi_profile_plans_use_fio_owned_dirs_and_ports(tmp_path) -> None:
    proposal = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)[0]

    plans = build_flrig_fldigi_managed_profile_plans(
        proposal,
        config_root=tmp_path / "fio-config",
        app_paths={
            "flrig": "/Applications/RadioApps/flrig.app",
            "fldigi": "/Applications/RadioApps/fldigi.app",
        },
    )
    by_app = {plan.app_id: plan for plan in plans}

    assert set(by_app) == {"flrig", "fldigi"}
    assert by_app["flrig"].config_dir == tmp_path / "fio-config" / "managed-instances" / "fio-a" / "flrig"
    assert by_app["flrig"].executable_path == "/Applications/RadioApps/flrig.app"
    assert by_app["flrig"].launch_args == ("--config-dir", str(by_app["flrig"].config_dir))
    assert by_app["flrig"].expected_host == "127.0.0.1"
    assert by_app["flrig"].expected_port == 12345
    assert by_app["flrig"].settings["xmlrpc_port"] == "12345"
    assert "later config writer" in " ".join(by_app["flrig"].notes)

    fldigi = by_app["fldigi"]
    assert fldigi.config_dir == tmp_path / "fio-config" / "managed-instances" / "fio-a" / "fldigi"
    assert fldigi.expected_port == 7362
    assert fldigi.launch_args == (
        "--config-dir",
        str(fldigi.config_dir),
        "--xmlrpc-server-address",
        "127.0.0.1",
        "--xmlrpc-server-port",
        "7362",
    )
    assert fldigi.settings["flrig_port"] == "12345"
    assert fldigi.settings["log_dir"].endswith("managed-instances/fio-a/fldigi/logs")
    assert fldigi.settings["checkin_dir"].endswith("managed-instances/fio-a/fldigi/checkins")


def test_managed_profile_directories_are_created_idempotently(tmp_path) -> None:
    proposal = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)[0]
    plans = build_flrig_fldigi_managed_profile_plans(proposal, config_root=tmp_path / "fio-config")

    first = create_managed_profile_directories(plans)
    second = create_managed_profile_directories(plans)

    assert first == second
    assert all(path.is_dir() for path in first)
    assert tmp_path / "fio-config" / "managed-instances" / "fio-a" / "flrig" in first
    assert tmp_path / "fio-config" / "managed-instances" / "fio-a" / "fldigi" / "logs" in first
    assert tmp_path / "fio-config" / "managed-instances" / "fio-a" / "fldigi" / "checkins" in first


def test_managed_profile_plans_honor_alternate_busy_port_assignments(tmp_path) -> None:
    busy_ports = {12345, 12355, 7362}
    proposal = build_lab_radio_proposals(
        radio_count=1,
        busy_checker=lambda _host, port: port in busy_ports,
    )[0]

    plans = build_flrig_fldigi_managed_profile_plans(proposal, config_root=tmp_path / "fio-config")
    by_app = {plan.app_id: plan for plan in plans}

    assert by_app["flrig"].expected_port == 12356
    assert by_app["flrig"].settings["xmlrpc_port"] == "12356"
    assert by_app["fldigi"].expected_port == 7372
    assert by_app["fldigi"].launch_args[-1] == "7372"
    assert by_app["fldigi"].settings["flrig_port"] == "12356"
