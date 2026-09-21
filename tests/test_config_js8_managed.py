from __future__ import annotations

import pytest

from freqinout.core.config_autodiscovery import build_lab_radio_proposals
from freqinout.core.config_js8_managed import (
    build_js8call_managed_profile_plans,
    create_js8call_managed_directories,
    render_js8call_multisettings_ini,
)


def test_js8call_managed_profile_plans_map_each_radio_to_flrig_and_api_ports(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=3, busy_checker=lambda _host, _port: False)

    plans = build_js8call_managed_profile_plans(
        proposals,
        config_root=tmp_path / "fio-config",
        js8call_path="/Applications/JS8Call.app",
        callsign="n1mag",
        grid="dm79",
        platform="Linux",
        storage_home=tmp_path,
    )

    assert [plan.profile_name for plan in plans] == ["Radio-A", "Radio-B", "Radio-C"]
    assert plans[0].executable_path == "/Applications/JS8Call.app"
    assert plans[0].config_dir == tmp_path / ".config"
    assert plans[0].settings_path == tmp_path / ".config" / "JS8Call - Radio-A.ini"
    assert plans[0].directed_path.name == "DIRECTED.TXT"
    assert plans[0].directed_path.parent == tmp_path / ".local" / "share" / plans[0].application_name
    assert plans[0].all_path.parent == plans[0].application_data_root
    assert plans[0].inbox_path.parent == plans[0].application_data_root
    assert len({plan.rig_name for plan in plans}) == 3
    assert len({plan.application_data_root for plan in plans}) == 3
    assert plans[0].settings["Rig"] == "FLRig FLRig"
    assert plans[0].settings["CATNetworkPort"] == "127.0.0.1:12345"
    assert plans[0].settings["TCPServerPort"] == "2442"
    assert plans[0].settings["AcceptTCPRequests"] == "true"
    assert plans[0].settings["UDPServerPort"] == "2242"
    assert plans[0].settings["MyCall"] == "N1MAG"
    assert plans[0].settings["MyGrid"] == "DM79"
    assert plans[1].settings["CATNetworkPort"] == "127.0.0.1:12346"
    assert plans[1].settings["TCPServerPort"] == "2443"
    assert plans[1].settings["UDPServerPort"] == "2243"
    assert plans[2].settings["CATNetworkPort"] == "127.0.0.1:12347"
    assert plans[2].settings["TCPServerPort"] == "2444"


def test_js8call_managed_profiles_honor_busy_port_assignments(tmp_path) -> None:
    busy_ports = {12345, 12355, 2442, 2452}
    proposals = build_lab_radio_proposals(
        radio_count=1,
        busy_checker=lambda _host, port: port in busy_ports,
    )

    plan = build_js8call_managed_profile_plans(
        proposals,
        config_root=tmp_path / "fio-config",
        platform="Linux",
        storage_home=tmp_path,
    )[0]

    assert plan.flrig_port == 12356
    assert plan.tcp_port == 2453
    assert plan.settings["CATNetworkPort"] == "127.0.0.1:12356"
    assert plan.settings["TCPServerPort"] == "2453"


def test_js8call_managed_profile_can_leave_radio_control_to_js8call(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)

    plan = build_js8call_managed_profile_plans(
        proposals,
        config_root=tmp_path / "fio-config",
        control_route="js8call",
        radio_label="TS-2000",
        platform="Linux",
        storage_home=tmp_path,
    )[0]

    assert plan.control_route == "js8call"
    assert plan.rig_summary == "JS8Call controls TS-2000; confirm the radio in JS8Call."
    assert "Rig" not in plan.settings
    assert "CATNetworkPort" not in plan.settings
    assert plan.settings["TCPServerPort"] == "2442"
    assert plan.settings["SaveDir"] == str(tmp_path / ".local" / "share" / "JS8Call - Radio-A" / "save")


def test_windows_js8call_managed_profile_uses_application_specific_qt_config_location(tmp_path) -> None:
    proposal = build_lab_radio_proposals(
        radio_count=1,
        busy_checker=lambda _host, _port: False,
    )
    plan = build_js8call_managed_profile_plans(
        proposal,
        config_root=tmp_path / "fio-config",
        platform="Windows",
        storage_home=tmp_path,
    )[0]

    expected_root = tmp_path / "AppData" / "Local" / "JS8Call - Radio-A"
    assert plan.settings_path == expected_root / "JS8Call - Radio-A.ini"
    assert plan.application_data_root == expected_root


def test_render_js8call_native_settings_preserves_unrelated_sections_and_updates_active_configuration(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=2, busy_checker=lambda _host, _port: False)
    plans = build_js8call_managed_profile_plans(
        proposals,
        config_root=tmp_path / "fio-config",
        platform="Linux",
        storage_home=tmp_path,
    )
    existing = "\n".join(
        [
            "[Configuration]",
            "MyCall=OLD",
            "",
            "[MultiSettings/manual]",
            "TCPServerPort=2999",
            "",
            "[MultiSettings/fio-a]",
            "TCPServerPort=1111",
            "ObscureExistingKey=keep",
        ]
    )

    rendered = render_js8call_multisettings_ini(existing, (plans[0],))

    assert "[Configuration]" in rendered
    assert "MyCall = OLD" in rendered
    assert "[MultiSettings/manual]" in rendered
    assert "TCPServerPort = 2999" in rendered
    assert "[MultiSettings/fio-a]" in rendered
    assert "ObscureExistingKey = keep" in rendered
    assert "CATNetworkPort = 127.0.0.1:12345" in rendered
    assert "TCPServerPort = 2442" in rendered
    assert "AcceptTCPRequests = true" in rendered
    assert "[MultiSettings/Radio-A]" not in rendered
    assert "[MultiSettings/Radio-B]" not in rendered

    with pytest.raises(ValueError, match="one native settings file"):
        render_js8call_multisettings_ini(existing, plans)


def test_js8call_managed_directories_are_created_idempotently(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)
    plans = build_js8call_managed_profile_plans(
        proposals,
        config_root=tmp_path / "fio-config",
        platform="Linux",
        storage_home=tmp_path,
    )

    first = create_js8call_managed_directories(plans)
    second = create_js8call_managed_directories(plans)

    assert first == second
    assert all(path.is_dir() for path in first)
    assert tmp_path / ".config" in first
    assert tmp_path / ".local" / "share" / "JS8Call - Radio-A" / "save" in first
    assert tmp_path / ".local" / "share" / "JS8Call - Radio-A" / "forms" in first


def test_js8call_materializes_canonical_qt_and_data_dirs_without_touching_operator_config_root(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)
    operator_root = tmp_path / "operator-owned-config-root"
    plan = build_js8call_managed_profile_plans(
        proposals,
        config_root=operator_root,
        platform="Linux",
        storage_home=tmp_path,
    )[0]
    plan.save_dir.mkdir(parents=True)
    sentinel = plan.save_dir / "retain.txt"
    sentinel.write_text("keep", encoding="utf-8")

    materialized = create_js8call_managed_directories((plan,))

    assert materialized == (plan.config_dir, plan.save_dir, plan.forms_dir)
    assert plan.config_dir == tmp_path / ".config"
    assert plan.settings_path.parent == plan.config_dir
    assert not plan.settings_path.exists()
    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert not operator_root.exists()


def test_js8call_never_converts_settings_file_or_managed_directory_file_target(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)
    plan = build_js8call_managed_profile_plans(
        proposals,
        config_root=tmp_path / "fio-config",
        platform="Linux",
        storage_home=tmp_path,
    )[0]
    plan.config_dir.mkdir(parents=True)
    plan.forms_dir.parent.mkdir(parents=True)
    plan.forms_dir.write_text("operator file", encoding="utf-8")

    with pytest.raises(FileExistsError):
        create_js8call_managed_directories((plan,))

    assert plan.forms_dir.is_file()
    assert plan.forms_dir.read_text(encoding="utf-8") == "operator file"
    assert not plan.settings_path.exists()
