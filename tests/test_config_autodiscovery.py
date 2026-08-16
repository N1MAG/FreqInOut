from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from freqinout.core.config_autodiscovery import (
    JS8CallFileProfile,
    build_autoconfig_proposal,
    build_lab_radio_proposals,
    default_app_search_paths,
    default_js8call_ini_paths,
    discover_js8call_file_profiles,
    discover_varac_local_assets,
    find_app_candidates,
    js8call_file_profile_operator_label,
    js8call_ini_family_label,
    read_js8call_multisettings,
    select_js8call_file_profile,
)
from freqinout.core.software_path_detector import SoftwarePathDetector


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


def test_macos_js8_scan_finds_improved_subspace_and_source_tree_binary(tmp_path) -> None:
    home = tmp_path / "home"
    improved_exe = (
        home
        / "RadioTools"
        / "Programs"
        / "JS8Call_Improved_Code"
        / "JS8Call-improved"
        / "build-codex-ptt-gate"
        / "JS8Call.app"
        / "Contents"
        / "MacOS"
        / "JS8Call"
    )
    subspace_exe = (
        home
        / "RadioTools"
        / "Programs"
        / "Subspace-Edition"
        / "build-trimode-baseline"
        / "JS8Call.app"
        / "Contents"
        / "MacOS"
        / "JS8Call"
    )
    js8_22_binary = home / "RadioTools" / "Programs" / "js8_22" / "js8call"
    _make_executable(improved_exe)
    _make_executable(subspace_exe)
    _make_executable(js8_22_binary)

    candidates = find_app_candidates(apps=("js8call",), platform="Darwin", home=home)
    paths = {Path(candidate.path) for candidate in candidates}

    assert improved_exe.parents[2] in paths
    assert subspace_exe.parents[2] in paths
    assert js8_22_binary in paths
    by_path = {Path(candidate.path): candidate for candidate in candidates}
    assert by_path[improved_exe.parents[2]].executable is True
    assert by_path[subspace_exe.parents[2]].executable is True
    assert by_path[js8_22_binary].executable is True


def test_default_app_search_paths_are_os_specific_and_bounded(tmp_path) -> None:
    mac_paths = default_app_search_paths(platform="Darwin", home=tmp_path)
    linux_paths = default_app_search_paths(platform="Linux", home=tmp_path)

    assert tmp_path / "RadioTools" / "Programs" / "JS8Call.app" in mac_paths["js8call"]
    assert (
        tmp_path
        / "RadioTools"
        / "Programs"
        / "Subspace-Edition"
        / "build-trimode-baseline"
        / "JS8Call.app"
    ) in mac_paths["js8call"]
    assert tmp_path / "RadioTools" / "Programs" / "js8_22" / "js8call" in mac_paths["js8call"]
    assert tmp_path / "RadioTools" / "Programs" / "VarAC_files" in mac_paths["varac"]
    assert Path("/usr/bin/flrig") in linux_paths["flrig"]
    assert Path("/opt/js8call-improved/js8call") in linux_paths["js8call"]
    assert all("*" not in str(path) for paths in mac_paths.values() for path in paths)


def test_settings_detector_searches_macos_radioapps_folder(tmp_path) -> None:
    detector = SoftwarePathDetector(settings={})
    detector.home = tmp_path

    paths = detector._macos_bundle_candidates("fldigi")

    assert Path("/Applications/RadioApps/fldigi.app") in paths
    assert tmp_path / "Applications" / "RadioApps" / "fldigi.app" in paths
    assert tmp_path / "RadioTools" / "Programs" / "fldigi.app" in paths


def test_settings_detector_finds_js8_subspace_nested_bundle(tmp_path) -> None:
    detector = SoftwarePathDetector(settings={})
    detector.home = tmp_path
    detector.system = "Darwin"
    subspace_exe = (
        tmp_path
        / "RadioTools"
        / "Programs"
        / "Subspace-Edition"
        / "build-trimode-baseline"
        / "JS8Call.app"
        / "Contents"
        / "MacOS"
        / "JS8Call"
    )
    _make_executable(subspace_exe)

    result = detector._detect_install_target(
        key="path_js8call",
        label="JS8Call install folder",
        tokens=("JS8Call", "js8call"),
        bundle_names=("Subspace-Edition/build-trimode-baseline/JS8Call",),
        windows_files=(),
        linux_files=(),
        prefer_bundle_dir=True,
    )

    assert Path(result.path) == subspace_exe.parents[2]
    assert result.confidence == "verified"
    assert result.target_type == "app_bundle"


def test_settings_detector_can_use_varac_production_fixture_folder(tmp_path) -> None:
    varac_fixture = tmp_path / "RadioTools" / "Programs" / "VarAC_files"
    (varac_fixture / "VarAC.db").parent.mkdir(parents=True)
    (varac_fixture / "VarAC.db").write_bytes(b"")
    (varac_fixture / "VarAC.ini").write_text("[VarAC]\n", encoding="utf-8")

    detector = SoftwarePathDetector(settings={})
    detector.home = tmp_path
    detector.system = "Darwin"

    results = detector.detect_varac()

    assert Path(results["varac_path"].path) == varac_fixture
    assert Path(results["varac_db_path"].path) == varac_fixture / "VarAC.db"
    assert Path(results["varac_ini_path"].path) == varac_fixture / "VarAC.ini"


def test_varac_local_asset_discovery_inspects_known_db_and_log_assets(tmp_path) -> None:
    varac_dir = tmp_path / "RadioTools" / "Programs" / "VarAC_files"
    bbs_dir = varac_dir / "BBS"
    incoming_dir = varac_dir / "INCOMING"
    outbox_dir = varac_dir / "OUTGOING"
    bbs_dir.mkdir(parents=True)
    incoming_dir.mkdir()
    outbox_dir.mkdir()
    (varac_dir / "VarAC.ini").write_text("[VARAC]\n", encoding="utf-8")
    (varac_dir / "VarAC_traffic.log").write_text("CONNECTED TO N1MAG\n", encoding="utf-8")
    (varac_dir / "VarAC.log").write_text("Database switched to DELETE mode.\n", encoding="utf-8")
    (varac_dir / "VarAC_qso_log.adi").write_text("<CALL:5>N1MAG\n", encoding="utf-8")
    (varac_dir / "VarAC_callsign_tags.conf").write_text("N1MAG=MagNet\n", encoding="utf-8")
    (varac_dir / "VarAC_alert_tags.conf").write_text("FIRE\n", encoding="utf-8")
    (varac_dir / "VarAC_templates.ini").write_text("[Templates]\n", encoding="utf-8")
    conn = sqlite3.connect(varac_dir / "VarAC.db")
    try:
        for table in ("qso", "vmail", "broadcast", "datastream"):
            conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()

    assets = discover_varac_local_assets(install_path=varac_dir)
    by_id = {asset.asset_id: asset for asset in assets}

    assert by_id["install"].confidence == "verified"
    assert by_id["ini"].path == str(varac_dir / "VarAC.ini")
    assert by_id["db"].confidence == "verified"
    assert "qso, vmail, broadcast, datastream" in by_id["db"].detail
    assert by_id["traffic_log"].exists is True
    assert by_id["app_log"].exists is True
    assert by_id["qso_log"].exists is True
    assert by_id["callsign_tags"].exists is True
    assert by_id["alert_tags"].exists is True
    assert by_id["templates"].exists is True
    assert by_id["bbs"].exists is True
    assert by_id["incoming"].path == str(incoming_dir)
    assert by_id["incoming"].exists is True
    assert by_id["outbox"].path == str(outbox_dir)
    assert by_id["outbox"].exists is True


def test_varac_local_asset_discovery_prefers_existing_ini_bbs_path(tmp_path) -> None:
    varac_dir = tmp_path / "VarAC"
    configured_bbs = tmp_path / "Configured BBS"
    varac_dir.mkdir()
    configured_bbs.mkdir()
    (varac_dir / "BBS").mkdir()
    (varac_dir / "VarAC.ini").write_text(
        "[BBS]\n"
        f"BBSDirectory={configured_bbs}\n"
        "[FILES]\n"
        "IncomingFilesDir=C:\\missing\\Incoming\n",
        encoding="utf-8",
    )
    (varac_dir / "INCOMING").mkdir()

    assets = discover_varac_local_assets(install_path=varac_dir)
    by_id = {asset.asset_id: asset for asset in assets}

    assert by_id["bbs"].path == str(configured_bbs)
    assert by_id["incoming"].path == str(varac_dir / "INCOMING")


def test_varac_local_asset_discovery_uses_latest_timestamped_app_log(tmp_path) -> None:
    varac_dir = tmp_path / "RadioTools" / "Programs" / "VarAC_files"
    varac_dir.mkdir(parents=True)
    older_log = varac_dir / "VarAC_20260101000000.log"
    newer_log = varac_dir / "VarAC_20260201000000.log"
    traffic_log = varac_dir / "VarAC_traffic.log"
    older_log.write_text("older", encoding="utf-8")
    newer_log.write_text("newer", encoding="utf-8")
    traffic_log.write_text("traffic", encoding="utf-8")
    os.utime(older_log, (1000, 1000))
    os.utime(newer_log, (2000, 2000))
    os.utime(traffic_log, (3000, 3000))

    assets = discover_varac_local_assets(install_path=varac_dir)
    by_id = {asset.asset_id: asset for asset in assets}

    assert by_id["app_log"].path == str(newer_log)
    assert by_id["app_log"].exists is True
    assert by_id["traffic_log"].path == str(traffic_log)


def test_varac_local_asset_discovery_uses_explicit_paths_without_external_writes(tmp_path) -> None:
    db_path = tmp_path / "CustomVarAC.db"
    sqlite3.connect(db_path).close()

    assets = discover_varac_local_assets(
        app_paths={
            "varac_db_path": str(db_path),
            "varac_ini_path": str(tmp_path / "missing.ini"),
        }
    )
    by_id = {asset.asset_id: asset for asset in assets}

    assert by_id["db"].path == str(db_path)
    assert by_id["db"].exists is True
    assert by_id["db"].confidence == "partial"
    assert by_id["ini"].exists is False
    assert all(asset.kind != "write" for asset in assets)


def test_varac_local_asset_discovery_matches_available_production_fixture() -> None:
    fixture = Path("/Users/bill/RadioTools/Programs/VarAC_files")
    if not fixture.exists():
        pytest.skip("Local VarAC production-style fixture is not available.")

    assets = discover_varac_local_assets(install_path=fixture)
    by_id = {asset.asset_id: asset for asset in assets}

    assert by_id["install"].exists is True
    assert by_id["db"].confidence == "verified"
    assert "qso" in by_id["db"].detail
    assert by_id["ini"].exists is True
    assert "station N1MAG DM79QJ" in by_id["ini"].detail
    assert by_id["qso_log"].exists is True
    assert by_id["callsign_tags"].exists is True
    assert by_id["incoming"].exists is True
    assert by_id["incoming"].path.endswith("INCOMING")
    assert by_id["outbox"].exists is True
    assert by_id["outbox"].path.endswith("OUTGOING")
    assert by_id["alert_tags"].exists is True
    assert by_id["templates"].exists is True
    assert by_id["traffic_log"].exists is True
    assert by_id["app_log"].exists is True


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


def test_js8call_multisettings_reader_handles_qsettings_escaped_keys(tmp_path) -> None:
    ini_path = tmp_path / "JS8Call.ini"
    save_dir = tmp_path / "fio-c" / "save"
    ini_path.write_text(
        "\n".join(
            [
                "Configuration\\MyCall=N1MAG",
                "Configuration\\MyGrid=DM79",
                "",
                "[MultiSettings/fio-c]",
                "Configuration\\SaveDir=/ignored/default/save",
                "Rig=FLRig FLRig",
                "CATNetworkPort=127.0.0.1:12347",
                "TCPServerPort=2444",
                f"SaveDir={save_dir}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = read_js8call_multisettings(ini_path)

    assert [profile.name for profile in profiles] == ["Default", "fio-c"]
    assert profiles[0].settings == {"MyCall": "N1MAG", "MyGrid": "DM79"}
    fio_c = profiles[1].settings
    assert fio_c["CATNetworkPort"] == "127.0.0.1:12347"
    assert fio_c["TCPServerPort"] == "2444"
    assert fio_c["SaveDir"] == str(save_dir)


def test_js8call_multisettings_reader_merges_parseable_qsettings_profiles(tmp_path) -> None:
    ini_path = tmp_path / "JS8Call.ini"
    save_dir = tmp_path / "fio-c" / "save"
    ini_path.write_text(
        "\n".join(
            [
                "[Configuration]",
                "MyCall=N1MAG",
                "MyGrid=DM79",
                "",
                "[MultiSettings/fio-c]",
                "Configuration\\SaveDir=/ignored/default/save",
                "Configuration\\TCPServerPort=2444",
                f"Configuration\\SaveDir={save_dir}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = read_js8call_multisettings(ini_path)

    assert [profile.name for profile in profiles] == ["Default", "fio-c"]
    fio_c = profiles[1].settings
    assert fio_c["TCPServerPort"] == "2444"
    assert fio_c["SaveDir"] == str(save_dir)


def test_js8call_multisettings_reader_handles_fully_qualified_qsettings_profile_keys(tmp_path) -> None:
    ini_path = tmp_path / "JS8Call.ini"
    save_dir = tmp_path / "fio-b" / "save"
    ini_path.write_text(
        "\n".join(
            [
                "Configuration\\MyCall=N1MAG",
                "Configuration\\MyGrid=DM79QJ",
                f"MultiSettings/fio-b\\Configuration\\SaveDir={save_dir}",
                "MultiSettings/fio-b\\Configuration\\TCPServerPort=2443",
                "MultiSettings/fio-b\\Configuration\\CATNetworkPort=127.0.0.1:12346",
            ]
        ),
        encoding="utf-8",
    )

    profiles = read_js8call_multisettings(ini_path)

    assert [profile.name for profile in profiles] == ["Default", "fio-b"]
    fio_b = profiles[1].settings
    assert fio_b["SaveDir"] == str(save_dir)
    assert fio_b["TCPServerPort"] == "2443"
    assert fio_b["CATNetworkPort"] == "127.0.0.1:12346"


def test_js8call_file_discovery_reads_profile_savedir_logs(tmp_path) -> None:
    save_dir = tmp_path / "js8" / "fio-c"
    save_dir.mkdir(parents=True)
    directed = save_dir / "DIRECTED.TXT"
    all_txt = save_dir / "ALL.TXT"
    directed.write_text("directed traffic\n", encoding="utf-8")
    all_txt.write_text("all traffic\n", encoding="utf-8")
    ini_path = tmp_path / "JS8Call.ini"
    ini_path.write_text(
        "\n".join(
            [
                "[MultiSettings/FIO-C]",
                "TCPServerPort=2444",
                f"SaveDir={save_dir}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = discover_js8call_file_profiles(ini_path=ini_path)

    assert len(profiles) == 1
    assert profiles[0].name == "FIO-C"
    assert profiles[0].tcp_server_port == "2444"
    assert profiles[0].directed_path == str(directed)
    assert profiles[0].all_path == str(all_txt)
    assert profiles[0].confidence == "verified"
    assert "SaveDir" in profiles[0].reason


def test_js8call_file_discovery_suggests_directed_path_when_save_dir_exists(tmp_path) -> None:
    save_dir = tmp_path / "js8" / "fio-b"
    save_dir.mkdir(parents=True)
    ini_path = tmp_path / "JS8Call.ini"
    ini_path.write_text(
        "\n".join(
            [
                "[MultiSettings/FIO-B]",
                "TCPServerPort=2443",
                f"SaveDir={save_dir}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = discover_js8call_file_profiles(ini_path=ini_path)
    selected = select_js8call_file_profile(profiles, tcp_port="2443")

    assert selected is not None
    assert selected.confidence == "partial"
    assert selected.directed_path == str(save_dir / "DIRECTED.TXT")


def test_js8call_file_profile_selection_prefers_matching_tcp_port(tmp_path) -> None:
    save_a = tmp_path / "a"
    save_c = tmp_path / "c"
    save_a.mkdir()
    save_c.mkdir()
    (save_a / "DIRECTED.TXT").write_text("a\n", encoding="utf-8")
    (save_c / "DIRECTED.TXT").write_text("c\n", encoding="utf-8")
    ini_path = tmp_path / "JS8Call.ini"
    ini_path.write_text(
        "\n".join(
            [
                "[MultiSettings/FIO-A]",
                "TCPServerPort=2442",
                f"SaveDir={save_a}",
                "",
                "[MultiSettings/FIO-C]",
                "TCPServerPort=2444",
                f"SaveDir={save_c}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = discover_js8call_file_profiles(ini_path=ini_path)
    selected = select_js8call_file_profile(profiles, tcp_port="2444")

    assert selected is not None
    assert selected.name == "FIO-C"
    assert selected.directed_path == str(save_c / "DIRECTED.TXT")
    assert select_js8call_file_profile(profiles) is None
    fallback_selected = select_js8call_file_profile(profiles, tcp_port="2445", profile_name="FIO-C")
    assert fallback_selected is not None
    assert fallback_selected.name == "FIO-C"


def test_js8call_file_profile_selection_falls_back_to_name_when_port_drifted(tmp_path) -> None:
    save_b = tmp_path / "fio-b"
    save_c = tmp_path / "fio-c"
    save_b.mkdir()
    save_c.mkdir()
    (save_b / "DIRECTED.TXT").write_text("b\n", encoding="utf-8")
    (save_c / "DIRECTED.TXT").write_text("c\n", encoding="utf-8")
    ini_path = tmp_path / "JS8Call.ini"
    ini_path.write_text(
        "\n".join(
            [
                "[MultiSettings/FIO-B]",
                "TCPServerPort=2443",
                f"SaveDir={save_b}",
                "",
                "[MultiSettings/FIO-C]",
                "TCPServerPort=2444",
                f"SaveDir={save_c}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = discover_js8call_file_profiles(ini_path=ini_path)
    selected = select_js8call_file_profile(profiles, tcp_port="2243", profile_name="FIO-B")

    assert selected is not None
    assert selected.name == "FIO-B"
    assert selected.directed_path == str(save_b / "DIRECTED.TXT")


def test_js8call_file_profile_selection_prefers_exact_name_over_conflicting_port(tmp_path) -> None:
    save_a = tmp_path / "fio-a"
    save_b = tmp_path / "fio-b"
    save_a.mkdir()
    save_b.mkdir()
    (save_a / "DIRECTED.TXT").write_text("a\n", encoding="utf-8")
    (save_b / "DIRECTED.TXT").write_text("b\n", encoding="utf-8")
    ini_path = tmp_path / "JS8Call.ini"
    ini_path.write_text(
        "\n".join(
            [
                "[MultiSettings/FIO-A]",
                "TCPServerPort=2442",
                f"SaveDir={save_a}",
                "",
                "[MultiSettings/FIO-B]",
                "TCPServerPort=2443",
                f"SaveDir={save_b}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = discover_js8call_file_profiles(ini_path=ini_path)
    selected = select_js8call_file_profile(profiles, tcp_port="2443", profile_name="FIO-A")

    assert selected is not None
    assert selected.name == "FIO-A"
    assert selected.tcp_server_port == "2442"
    assert selected.directed_path == str(save_a / "DIRECTED.TXT")


def test_js8call_file_profile_selection_does_not_use_partial_name_when_port_drifted(tmp_path) -> None:
    save_b = tmp_path / "fio-b"
    save_b.mkdir()
    (save_b / "DIRECTED.TXT").write_text("b\n", encoding="utf-8")
    ini_path = tmp_path / "JS8Call.ini"
    ini_path.write_text(
        "\n".join(
            [
                "[MultiSettings/FIO-B]",
                "TCPServerPort=2443",
                f"SaveDir={save_b}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = discover_js8call_file_profiles(ini_path=ini_path)

    assert select_js8call_file_profile(profiles, tcp_port="2243", profile_name="FIO-B Backup") is None


def test_js8call_file_profile_selection_uses_name_when_port_matches_multiple_profiles(tmp_path) -> None:
    default_save = tmp_path / "default"
    save_b = tmp_path / "b"
    default_save.mkdir()
    save_b.mkdir()
    (default_save / "DIRECTED.TXT").write_text("default\n", encoding="utf-8")
    (save_b / "DIRECTED.TXT").write_text("b\n", encoding="utf-8")
    ini_path = tmp_path / "JS8Call.ini"
    ini_path.write_text(
        "\n".join(
            [
                "[Configuration]",
                "TCPServerPort=2443",
                f"SaveDir={default_save}",
                "",
                "[MultiSettings/FIO-B]",
                "TCPServerPort=2443",
                f"SaveDir={save_b}",
            ]
        ),
        encoding="utf-8",
    )

    profiles = discover_js8call_file_profiles(ini_path=ini_path)
    selected = select_js8call_file_profile(profiles, tcp_port="2443", profile_name="FIO-B")

    assert selected is not None
    assert selected.name == "FIO-B"
    assert selected.directed_path == str(save_b / "DIRECTED.TXT")
    assert select_js8call_file_profile(profiles, tcp_port="2443") is None


def test_default_js8call_ini_paths_are_os_specific_and_bounded(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    monkeypatch.delenv("APPDATA", raising=False)
    mac_paths = default_js8call_ini_paths(platform="Darwin", home=tmp_path)
    linux_paths = default_js8call_ini_paths(platform="Linux", home=tmp_path)
    windows_paths = default_js8call_ini_paths(platform="Windows", home=tmp_path)

    assert tmp_path / "Library" / "Preferences" / "JS8Call.ini" in mac_paths
    assert tmp_path / ".config" / "JS8Call.ini" in linux_paths
    assert tmp_path / ".config" / "JS8Call-improved.ini" in linux_paths
    assert tmp_path / ".config" / "JS8Call Subspace.ini" in linux_paths
    assert tmp_path / "AppData" / "Local" / "JS8Call" / "JS8Call.ini" in windows_paths
    assert tmp_path / "AppData" / "Local" / "JS8Call" / "JS8Call-improved.ini" in windows_paths
    assert tmp_path / "AppData" / "Local" / "JS8Call" / "Subspace.ini" in windows_paths
    assert all(path.is_absolute() for path in mac_paths + linux_paths + windows_paths)
    assert all("*" not in str(path) for path in mac_paths + linux_paths + windows_paths)
    assert len(windows_paths) < 16


def test_default_js8call_ini_paths_include_named_macos_instances(tmp_path) -> None:
    prefs = tmp_path / "Library" / "Preferences"
    prefs.mkdir(parents=True)
    named_ini = prefs / "JS8Call - fio-b.ini"
    named_ini.write_text("[Configuration]\n", encoding="utf-8")
    improved_ini = prefs / "JS8Call-improved.ini"
    improved_ini.write_text("[Configuration]\n", encoding="utf-8")
    unrelated_ini = prefs / "NotJS8.ini"
    unrelated_ini.write_text("[Configuration]\n", encoding="utf-8")

    paths = default_js8call_ini_paths(platform="Darwin", home=tmp_path)

    assert prefs / "JS8Call.ini" in paths
    assert named_ini in paths
    assert improved_ini in paths
    assert unrelated_ini not in paths
    assert all("*" not in str(path) for path in paths)


def test_js8call_file_profile_labels_are_operator_readable_for_supported_variants(tmp_path) -> None:
    named = tmp_path / "JS8Call - fio-b.ini"
    improved = tmp_path / "JS8Call-improved.ini"
    subspace = tmp_path / "Subspace.ini"
    js8_subspace = tmp_path / "JS8Call Subspace.ini"
    profile = discover_js8call_file_profiles(ini_path=named)

    assert js8call_ini_family_label(named) == "JS8Call fio-b"
    assert js8call_ini_family_label(improved) == "JS8Call-improved"
    assert js8call_ini_family_label(subspace) == "JS8Call Subspace"
    assert js8call_ini_family_label(js8_subspace) == "JS8Call Subspace"
    assert js8call_file_profile_operator_label(
        JS8CallFileProfile(
            name="FIO-B",
            ini_path=str(named),
            save_dir="",
            tcp_server_port="2443",
            directed_path="",
            all_path="",
            confidence="not_found",
            reason="",
        )
    ) == "JS8Call fio-b | FIO-B | API 2443"
    assert profile == tuple()


def test_autoconfig_proposal_includes_default_js8_file_profiles(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PATH", os.pathsep.join([str(tmp_path / "empty-bin")]))
    home = tmp_path / "home"
    save_dir = home / "Radio" / "JS8Call" / "FIO-C"
    save_dir.mkdir(parents=True)
    directed = save_dir / "DIRECTED.TXT"
    directed.write_text("directed traffic\n", encoding="utf-8")
    ini_path = home / "Library" / "Preferences" / "JS8Call.ini"
    ini_path.parent.mkdir(parents=True)
    ini_path.write_text(
        "\n".join(
            [
                "[MultiSettings/FIO-C]",
                "TCPServerPort=2444",
                f"SaveDir={save_dir}",
            ]
        ),
        encoding="utf-8",
    )

    proposal = build_autoconfig_proposal(
        radio_count=1,
        platform="Darwin",
        home=home,
        app_search_paths={"flrig": (), "fldigi": (), "js8call": ()},
        busy_checker=lambda _host, _port: False,
    )

    assert [profile.name for profile in proposal.js8_file_profiles] == ["FIO-C"]
    assert proposal.js8_file_profiles[0].directed_path == str(directed)


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


def test_valid_known_path_does_not_hide_distinct_path_command(tmp_path, monkeypatch) -> None:
    known_flrig = tmp_path / "known" / "flrig"
    path_flrig = tmp_path / "bin" / "flrig"
    _make_executable(known_flrig)
    _make_executable(path_flrig)
    monkeypatch.setenv("PATH", str(path_flrig.parent))

    candidates = find_app_candidates(
        apps=("flrig",),
        platform="Linux",
        home=tmp_path / "home",
        app_search_paths={"flrig": (known_flrig,)},
    )

    assert [candidate.source for candidate in candidates] == ["known_path", "path"]
    assert [candidate.path.lower() for candidate in candidates] == [str(known_flrig).lower(), str(path_flrig).lower()]


def test_duplicate_first_command_alias_does_not_hide_distinct_later_alias(tmp_path, monkeypatch) -> None:
    known_js8 = tmp_path / "known" / "JS8Call"
    path_js8_upper = tmp_path / "bin" / "JS8Call"
    path_js8_lower = tmp_path / "bin" / "js8call"
    _make_executable(known_js8)
    _make_executable(path_js8_upper)
    _make_executable(path_js8_lower)
    monkeypatch.setenv("PATH", str(path_js8_lower.parent))

    candidates = find_app_candidates(
        apps=("js8call",),
        platform="Linux",
        home=tmp_path / "home",
        app_search_paths={"js8call": (path_js8_upper,)},
    )

    assert [candidate.source for candidate in candidates] == ["known_path", "path"]
    assert [candidate.path.lower() for candidate in candidates] == [
        str(path_js8_upper).lower(),
        str(path_js8_lower).lower(),
    ]


def test_js8call_command_discovery_accepts_improved_and_subspace_aliases(tmp_path, monkeypatch) -> None:
    improved_bin = tmp_path / "improved-bin"
    subspace_bin = tmp_path / "subspace-bin"
    improved = improved_bin / "js8call-improved"
    subspace = subspace_bin / "js8call-subspace"
    _make_executable(improved)
    _make_executable(subspace)

    monkeypatch.setenv("PATH", str(improved_bin))

    candidates = find_app_candidates(
        apps=("js8call",),
        platform="Linux",
        home=tmp_path / "home",
        app_search_paths={"js8call": ()},
    )

    assert len(candidates) == 1
    assert Path(candidates[0].path).name.casefold() == improved.name.casefold()
    assert candidates[0].source == "path"
    assert candidates[0].executable is True

    monkeypatch.setenv("PATH", str(subspace_bin))

    candidates = find_app_candidates(
        apps=("js8call",),
        platform="Linux",
        home=tmp_path / "home",
        app_search_paths={"js8call": ()},
    )

    assert len(candidates) == 1
    assert Path(candidates[0].path).name.casefold() == subspace.name.casefold()
    assert candidates[0].source == "path"
    assert candidates[0].executable is True
