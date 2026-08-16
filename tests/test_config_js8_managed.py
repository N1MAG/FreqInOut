from __future__ import annotations

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
    )

    assert [plan.profile_name for plan in plans] == ["fio-a", "fio-b", "fio-c"]
    assert plans[0].executable_path == "/Applications/JS8Call.app"
    assert plans[0].config_dir == tmp_path / "fio-config" / "managed-instances" / "fio-a" / "js8call"
    assert plans[0].directed_path.name == "DIRECTED.TXT"
    assert plans[0].settings["Rig"] == "FLRig FLRig"
    assert plans[0].settings["CATNetworkPort"] == "127.0.0.1:12345"
    assert plans[0].settings["TCPServerPort"] == "2442"
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

    plan = build_js8call_managed_profile_plans(proposals, config_root=tmp_path / "fio-config")[0]

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
    )[0]

    assert plan.control_route == "js8call"
    assert plan.rig_summary == "JS8Call controls TS-2000; confirm the radio in JS8Call."
    assert "Rig" not in plan.settings
    assert "CATNetworkPort" not in plan.settings
    assert plan.settings["TCPServerPort"] == "2442"
    assert plan.settings["SaveDir"].endswith("managed-instances/fio-a/js8call/save")


def test_render_js8call_multisettings_preserves_existing_sections_and_updates_managed_profiles(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=2, busy_checker=lambda _host, _port: False)
    plans = build_js8call_managed_profile_plans(proposals, config_root=tmp_path / "fio-config")
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

    rendered = render_js8call_multisettings_ini(existing, plans)

    assert "[Configuration]" in rendered
    assert "MyCall = OLD" in rendered
    assert "[MultiSettings/manual]" in rendered
    assert "TCPServerPort = 2999" in rendered
    assert "[MultiSettings/fio-a]" in rendered
    assert "ObscureExistingKey = keep" in rendered
    assert "CATNetworkPort = 127.0.0.1:12345" in rendered
    assert "TCPServerPort = 2442" in rendered
    assert "[MultiSettings/fio-b]" in rendered
    assert "CATNetworkPort = 127.0.0.1:12346" in rendered
    assert "TCPServerPort = 2443" in rendered


def test_js8call_managed_directories_are_created_idempotently(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)
    plans = build_js8call_managed_profile_plans(proposals, config_root=tmp_path / "fio-config")

    first = create_js8call_managed_directories(plans)
    second = create_js8call_managed_directories(plans)

    assert first == second
    assert all(path.is_dir() for path in first)
    assert tmp_path / "fio-config" / "managed-instances" / "fio-a" / "js8call" in first
    assert tmp_path / "fio-config" / "managed-instances" / "fio-a" / "js8call" / "save" in first
    assert tmp_path / "fio-config" / "managed-instances" / "fio-a" / "js8call" / "forms" in first
