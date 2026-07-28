from __future__ import annotations

import os
from pathlib import Path

from freqinout.core.config_autodiscovery import (
    build_autoconfig_proposal,
    build_lab_radio_proposals,
    default_app_search_paths,
    find_app_candidates,
    read_js8call_multisettings,
)


def _make_executable(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | 0o111)


def test_macos_fast_scan_finds_known_app_bundles(tmp_path) -> None:
    home = tmp_path / "home"
    flrig_exe = home / "Applications" / "flrig-2.0.10.app" / "Contents" / "MacOS" / "flrig"
    js8_exe = home / "RadioTools" / "Programs" / "JS8Call-2.2.0.app" / "Contents" / "MacOS" / "JS8Call"
    _make_executable(flrig_exe)
    _make_executable(js8_exe)

    candidates = find_app_candidates(apps=("flrig", "js8call"), platform="Darwin", home=home)

    by_app = {candidate.app_id: candidate for candidate in candidates}
    assert by_app["flrig"].path.lower().endswith("flrig-2.0.10.app")
    assert by_app["flrig"].target_type == "app_bundle"
    assert by_app["flrig"].executable is True
    assert by_app["js8call"].path.lower().endswith("js8call-2.2.0.app")
    assert by_app["js8call"].confidence == "verified"


def test_default_app_search_paths_are_os_specific_and_bounded(tmp_path) -> None:
    mac_paths = default_app_search_paths(platform="Darwin", home=tmp_path)
    linux_paths = default_app_search_paths(platform="Linux", home=tmp_path)

    assert tmp_path / "RadioTools" / "Programs" / "JS8Call.app" in mac_paths["js8call"]
    assert Path("/usr/bin/flrig") in linux_paths["flrig"]
    assert all("*" not in str(path) for paths in mac_paths.values() for path in paths)


def test_lab_radio_proposal_assigns_expected_ports_and_leaves_varac_off() -> None:
    proposals = build_lab_radio_proposals(radio_count=3, busy_checker=lambda _host, _port: False)

    assert [proposal.instance_name for proposal in proposals] == ["fio-a", "fio-b", "fio-c"]
    assert proposals[0].varac_enabled is False
    assert proposals[0].enabled_apps == ("flrig", "fldigi", "js8call")
    first_ports = {assignment.service: assignment.assigned_port for assignment in proposals[0].ports}
    second_ports = {assignment.service: assignment.assigned_port for assignment in proposals[1].ports}
    assert first_ports["flrig"] == 12345
    assert first_ports["fldigi"] == 7362
    assert first_ports["js8call"] == 2442
    assert second_ports["flrig"] == 12346
    assert second_ports["js8call"] == 2443
    js8_udp = next(assignment for assignment in proposals[0].ports if assignment.service == "js8call_udp")
    assert js8_udp.protocol == "udp"
    assert js8_udp.conflict_checked is False
    assert "UDP conflict probing is deferred" in js8_udp.note


def test_port_proposal_uses_clear_alternate_when_preferred_port_is_busy() -> None:
    busy = {2442, 2452}

    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, port: port in busy)
    js8_assignment = next(assignment for assignment in proposals[0].ports if assignment.service == "js8call")

    assert js8_assignment.preferred_port == 2442
    assert js8_assignment.assigned_port == 2453
    assert js8_assignment.conflict is True
    assert "preferred port 2442 is busy" in js8_assignment.note


def test_js8call_multisettings_reader_extracts_operator_relevant_keys(tmp_path) -> None:
    ini_path = tmp_path / "JS8Call.ini"
    ini_path.write_text(
        "\n".join(
            [
                "[Configuration]",
                "MyCall=N1MAG",
                "MyGrid=DM79",
                "IrrelevantKey=ignore",
                "",
                "[MultiSettings/fio-a]",
                "Rig=FLRig FLRig",
                "CATNetworkPort=127.0.0.1:12345",
                "TCPEnabled=true",
                "TCPServer=127.0.0.1",
                "TCPServerPort=2442",
                "UDPServerPort=2242",
                "SaveDir=/tmp/fio-a/save",
            ]
        ),
        encoding="utf-8",
    )

    profiles = read_js8call_multisettings(ini_path)

    assert [profile.name for profile in profiles] == ["Default", "fio-a"]
    assert profiles[0].settings == {"MyCall": "N1MAG", "MyGrid": "DM79"}
    fio_a = profiles[1].settings
    assert fio_a["Rig"] == "FLRig FLRig"
    assert fio_a["CATNetworkPort"] == "127.0.0.1:12345"
    assert fio_a["TCPServerPort"] == "2442"


def test_autoconfig_proposal_reports_missing_apps_without_enabling_varac(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PATH", os.pathsep.join([str(tmp_path / "empty-bin")]))
    proposal = build_autoconfig_proposal(
        radio_count=2,
        platform="Darwin",
        home=tmp_path / "home",
        app_search_paths={"flrig": (), "fldigi": (), "js8call": ()},
        busy_checker=lambda _host, _port: False,
    )

    assert proposal.platform == "Darwin"
    assert proposal.missing_apps == ("flrig", "fldigi", "js8call")
    assert "VarAC is optional" in " ".join(proposal.warnings)
    assert len(proposal.radios) == 2
    assert all(radio.varac_enabled is False for radio in proposal.radios)


def test_autoconfig_proposal_does_not_count_non_executable_paths_as_found(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PATH", os.pathsep.join([str(tmp_path / "empty-bin")]))
    flrig_bundle = tmp_path / "home" / "Applications" / "FLRig.app"
    flrig_bundle.mkdir(parents=True)

    proposal = build_autoconfig_proposal(
        radio_count=1,
        platform="Darwin",
        home=tmp_path / "home",
        app_search_paths={"flrig": (flrig_bundle,), "fldigi": (), "js8call": ()},
        busy_checker=lambda _host, _port: False,
    )

    assert proposal.candidates[0].app_id == "flrig"
    assert proposal.candidates[0].executable is False
    assert proposal.missing_apps == ("flrig", "fldigi", "js8call")


def test_manual_extra_path_only_counts_for_matching_app_identity(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PATH", os.pathsep.join([str(tmp_path / "empty-bin")]))
    flrig_binary = tmp_path / "tools" / "flrig"
    _make_executable(flrig_binary)

    proposal = build_autoconfig_proposal(
        radio_count=1,
        platform="Linux",
        home=tmp_path / "home",
        app_search_paths={"flrig": (), "fldigi": (), "js8call": ()},
        extra_app_paths=(flrig_binary,),
        busy_checker=lambda _host, _port: False,
    )

    assert [candidate.app_id for candidate in proposal.candidates] == ["flrig"]
    assert proposal.candidates[0].executable is True
    assert proposal.missing_apps == ("fldigi", "js8call")


def test_broken_known_path_does_not_block_valid_path_command(tmp_path, monkeypatch) -> None:
    home = tmp_path / "home"
    broken_bundle = home / "Applications" / "FLRig.app"
    broken_bundle.mkdir(parents=True)
    bin_dir = tmp_path / "bin"
    valid_flrig = bin_dir / "flrig"
    _make_executable(valid_flrig)
    monkeypatch.setenv("PATH", str(bin_dir))

    candidates = find_app_candidates(
        apps=("flrig",),
        platform="Darwin",
        home=home,
        app_search_paths={"flrig": (broken_bundle,)},
    )

    assert [candidate.executable for candidate in candidates] == [False, True]
    assert candidates[-1].source == "path"
    assert candidates[-1].path.lower() == str(valid_flrig).lower()
