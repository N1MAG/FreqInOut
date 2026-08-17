from __future__ import annotations

import types
from pathlib import Path

from freqinout.core.config_autodiscovery import JS8CallFileProfile
from freqinout.core.guided_radio_autofill import (
    guided_app_candidate_choices,
    guided_js8_profile_choices,
    guided_js8_profile_review_text,
    guided_port_prompt_keys,
    guided_radio_autofill_suggestions,
    guided_single_install_path,
    next_default_instance_port,
)
from freqinout.core.software_path_detector import PathDetectionResult


def test_js8_profile_choices_include_port_profile_and_directed(tmp_path: Path) -> None:
    directed_path = tmp_path / "FIO-C" / "DIRECTED.TXT"
    directed_path.parent.mkdir()
    directed_path.write_text("", encoding="utf-8")

    choices = guided_js8_profile_choices(
        (
            JS8CallFileProfile(
                name="FIO-C",
                ini_path="/tmp/JS8Call.ini",
                save_dir=str(directed_path.parent),
                tcp_server_port="2444",
                directed_path=str(directed_path),
                all_path="",
                confidence="verified",
                reason="C",
            ),
            JS8CallFileProfile(
                name="No directed",
                ini_path="/tmp/JS8Call.ini",
                save_dir=str(tmp_path / "missing"),
                tcp_server_port="2445",
                directed_path="",
                all_path="",
                confidence="not_found",
                reason="missing",
            ),
        )
    )

    assert choices == (
        (
            f"JS8Call | FIO-C | API 2444 - {directed_path.parent}",
            {
                "port": "2444",
                "profile_path": str(directed_path.parent),
                "directed_path": str(directed_path),
            },
        ),
    )


def test_port_prompt_keys_follow_selected_apps_and_backend() -> None:
    assert guided_port_prompt_keys(
        current={"flrig_port": "", "fldigi_port": "7362", "js8_port": ""},
        selected={"flrig": False, "fldigi": True, "js8call": True},
        backend="flrig",
        observer_mode=False,
    ) == ("flrig_port", "js8_port")
    assert guided_port_prompt_keys(
        current={"flrig_port": "", "fldigi_port": "", "js8_port": ""},
        selected={"flrig": True, "fldigi": True, "js8call": True},
        backend="flrig",
        observer_mode=True,
    ) == ()
    assert guided_port_prompt_keys(
        current={"js8_port": ""},
        selected={"js8call": False},
        backend="js8call",
        observer_mode=False,
    ) == ("js8_port",)


def test_app_candidate_choices_dedupe_and_ignore_non_executables() -> None:
    choices = guided_app_candidate_choices(
        (
            types.SimpleNamespace(app_id="js8call", executable=True, path="/Applications/JS8Call.app", source="known_path"),
            types.SimpleNamespace(app_id="js8call", executable=True, path="/Applications/JS8Call.app", source="path"),
            types.SimpleNamespace(app_id="js8call", executable=False, path="/tmp/JS8Call.txt", source="known_path"),
            types.SimpleNamespace(app_id="js8call", executable=True, path="/opt/js8call/bin/js8call", source="path"),
            types.SimpleNamespace(app_id="flrig", executable=True, path="/Applications/FLRig.app", source="known_path"),
        ),
        "js8call",
    )

    assert choices == (
        ("/Applications/JS8Call.app (known_path)", "/Applications/JS8Call.app"),
        ("/opt/js8call/bin/js8call (path)", "/opt/js8call/bin/js8call"),
    )


def test_app_candidate_choices_treat_bundle_and_shim_as_one_install(tmp_path: Path) -> None:
    bundle_exe = tmp_path / "Applications" / "JS8Call.app" / "Contents" / "MacOS" / "JS8Call"
    bundle_exe.parent.mkdir(parents=True)
    bundle_exe.write_text("#!/bin/sh\n", encoding="utf-8")
    shim = tmp_path / "bin" / "js8call"
    shim.parent.mkdir()
    shim.symlink_to(bundle_exe)
    bundle = bundle_exe.parents[2]
    candidates = (
        types.SimpleNamespace(app_id="js8call", executable=True, path=str(bundle), source="known_path"),
        types.SimpleNamespace(app_id="js8call", executable=True, path=str(shim), source="path"),
    )

    choices = guided_app_candidate_choices(candidates, "js8call")
    review: list[str] = []
    selected = guided_single_install_path(
        candidates,
        "js8call",
        {},
        "path_js8call",
        "JS8Call",
        review,
    )

    assert choices == ((f"{bundle} (known_path)", str(bundle)),)
    assert selected == str(bundle)
    assert review == []


def test_app_candidate_choices_label_js8call_supported_variants() -> None:
    choices = guided_app_candidate_choices(
        (
            types.SimpleNamespace(
                app_id="js8call",
                executable=True,
                path="/Applications/JS8Call-improved.app",
                source="known_path",
            ),
            types.SimpleNamespace(
                app_id="js8call",
                executable=True,
                path="/Users/bill/RadioTools/Programs/Subspace-Edition/build-trimode-baseline/JS8Call.app",
                source="known_path",
            ),
            types.SimpleNamespace(
                app_id="js8call",
                executable=True,
                path="/Users/bill/RadioTools/Programs/js8_22/js8call",
                source="known_path",
            ),
        ),
        "js8call",
    )

    labels = [label for label, _path in choices]
    assert labels == [
        "JS8Call-improved - /Applications/JS8Call-improved.app (known_path)",
        "JS8Call Subspace - /Users/bill/RadioTools/Programs/Subspace-Edition/build-trimode-baseline/JS8Call.app (known_path)",
        "JS8Call 2.2.0 - /Users/bill/RadioTools/Programs/js8_22/js8call (known_path)",
    ]


def test_default_ports_use_unused_service_defaults() -> None:
    profiles = [
        {"id": 1, "flrig_port": 12345, "fldigi_port": 7362, "js8_port": 2442},
        {"id": 2, "flrig_port": 12346, "fldigi_port": 7363, "js8_port": 2443},
        {"id": 3, "device_class": "observer"},
    ]

    assert next_default_instance_port("flrig", profiles) == "12347"
    assert next_default_instance_port("fldigi", profiles) == "7364"
    assert next_default_instance_port("js8call", profiles) == "2444"
    assert next_default_instance_port("js8call", profiles, existing_profile_id=2) == "2443"


def test_js8_profile_review_text_is_operator_clear(tmp_path: Path) -> None:
    profile_a = JS8CallFileProfile(
        name="FIO-A",
        ini_path="/tmp/JS8Call.ini",
        save_dir=str(tmp_path / "a"),
        tcp_server_port="2442",
        directed_path=str(tmp_path / "a" / "DIRECTED.TXT"),
        all_path="",
        confidence="verified",
        reason="A",
    )
    profile_b = JS8CallFileProfile(
        name="FIO-B",
        ini_path="/tmp/JS8Call.ini",
        save_dir=str(tmp_path / "b"),
        tcp_server_port="2443",
        directed_path=str(tmp_path / "b" / "DIRECTED.TXT"),
        all_path="",
        confidence="verified",
        reason="B",
    )

    assert guided_js8_profile_review_text([]) == "No JS8Call profile with DIRECTED.TXT was found."
    assert (
        guided_js8_profile_review_text([profile_a, profile_b])
        == "Multiple JS8Call profiles have DIRECTED.TXT. Enter the JS8Call TCP port to choose the correct one."
    )
    assert (
        guided_js8_profile_review_text([profile_a], tcp_port="2444")
        == "No JS8Call profile with DIRECTED.TXT matched TCP port 2444."
    )


def test_radio_autofill_uses_js8_profile_port_before_default(tmp_path: Path) -> None:
    directed_path = tmp_path / "FIO-C" / "DIRECTED.TXT"
    directed_path.parent.mkdir()
    directed_path.write_text("", encoding="utf-8")
    suggestions, review = guided_radio_autofill_suggestions(
        current={"js8_port": ""},
        selected={"js8call": True},
        backend="manual",
        observer_mode=False,
        install_candidates=(),
        fast_results={},
        js8_results={},
        varac_results={},
        js8_file_profiles=(
            JS8CallFileProfile(
                name="FIO-C",
                ini_path="/tmp/JS8Call.ini",
                save_dir=str(directed_path.parent),
                tcp_server_port="2444",
                directed_path=str(directed_path),
                all_path="",
                confidence="verified",
                reason="Matched profile",
            ),
        ),
        default_ports={"js8call": "2442"},
        profile_name="FIO-C",
    )

    assert suggestions["js8_port"] == "2444"
    assert suggestions["js8_directed_path"] == str(directed_path)
    assert suggestions["js8_profile_path"] == str(directed_path.parent)
    assert not review


def test_radio_autofill_keeps_js8_directed_manual_when_profiles_are_ambiguous(tmp_path: Path) -> None:
    path_a = tmp_path / "FIO-A" / "DIRECTED.TXT"
    path_c = tmp_path / "FIO-C" / "DIRECTED.TXT"
    path_a.parent.mkdir()
    path_c.parent.mkdir()
    path_a.write_text("", encoding="utf-8")
    path_c.write_text("", encoding="utf-8")
    suggestions, review = guided_radio_autofill_suggestions(
        current={"js8_port": ""},
        selected={"js8call": True},
        backend="manual",
        observer_mode=False,
        install_candidates=(),
        fast_results={},
        js8_results={},
        varac_results={},
        js8_file_profiles=(
            JS8CallFileProfile(
                name="FIO-A",
                ini_path="/tmp/JS8Call.ini",
                save_dir=str(path_a.parent),
                tcp_server_port="2442",
                directed_path=str(path_a),
                all_path="",
                confidence="verified",
                reason="A",
            ),
            JS8CallFileProfile(
                name="FIO-C",
                ini_path="/tmp/JS8Call.ini",
                save_dir=str(path_c.parent),
                tcp_server_port="2444",
                directed_path=str(path_c),
                all_path="",
                confidence="verified",
                reason="C",
            ),
        ),
        default_ports={"js8call": "2443"},
        profile_name="FIO-X",
    )

    assert "js8_port" not in suggestions
    assert "js8_directed_path" not in suggestions
    assert "js8_profile_path" not in suggestions
    assert review == (
        "Multiple JS8Call profiles have DIRECTED.TXT for FIO-X. Enter the JS8Call TCP port to choose the correct one.",
    )


def test_radio_autofill_requires_manual_choice_for_multiple_installs() -> None:
    suggestions, review = guided_radio_autofill_suggestions(
        current={},
        selected={"js8call": True},
        backend="manual",
        observer_mode=False,
        install_candidates=(
            types.SimpleNamespace(app_id="js8call", executable=True, path="/Applications/JS8Call.app"),
            types.SimpleNamespace(app_id="js8call", executable=True, path="/opt/js8call/bin/js8call"),
        ),
        fast_results={},
        js8_results={
            "path_js8call": PathDetectionResult(
                key="path_js8call",
                label="JS8Call",
                path="/Applications/JS8Call.app",
                confidence="probable",
                reason="Fallback path",
                exists=True,
                target_type="file",
            )
        },
        varac_results={},
        js8_file_profiles=(),
        default_ports={"js8call": "2442"},
        profile_name="FIO-C",
    )

    assert "js8_install_path" not in suggestions
    assert "Multiple JS8Call installs found. Choose the correct app path manually." in review


def test_radio_autofill_leaves_varac_db_and_cluster_manual() -> None:
    varac_results = {
        "varac_path": PathDetectionResult(
            key="varac_path",
            label="VarAC install",
            path="/Applications/VarAC",
            confidence="verified",
            reason="Found install",
            exists=True,
            target_type="directory",
        ),
        "varac_ini_path": PathDetectionResult(
            key="varac_ini_path",
            label="VarAC.ini",
            path="/Applications/VarAC/VarAC.ini",
            confidence="verified",
            reason="Found ini",
            exists=True,
            target_type="file",
        ),
        "message_paths.varac": PathDetectionResult(
            key="message_paths.varac",
            label="VarAC incoming",
            path="/Applications/VarAC/RX Files",
            confidence="verified",
            reason="Found incoming",
            exists=True,
            target_type="directory",
        ),
        "varac_db_path": PathDetectionResult(
            key="varac_db_path",
            label="VarAC database",
            path="/Applications/VarAC/VarAC.db",
            confidence="verified",
            reason="Found db",
            exists=True,
            target_type="file",
        ),
        "varac_outbox_dir": PathDetectionResult(
            key="varac_outbox_dir",
            label="VarAC outbox",
            path="/Applications/VarAC/Outbox",
            confidence="verified",
            reason="Found outbox",
            exists=True,
            target_type="directory",
        ),
        "varac_bbs_dir": PathDetectionResult(
            key="varac_bbs_dir",
            label="VarAC BBS",
            path="/Applications/VarAC/BBS",
            confidence="verified",
            reason="Found BBS",
            exists=True,
            target_type="directory",
        ),
        "varac_bbs_archive_dir": PathDetectionResult(
            key="varac_bbs_archive_dir",
            label="VarAC BBS archive",
            path="/Applications/VarAC/BBS/Archive",
            confidence="verified",
            reason="Found BBS archive",
            exists=True,
            target_type="directory",
        ),
    }
    suggestions, review = guided_radio_autofill_suggestions(
        current={},
        selected={"varac": True},
        backend="manual",
        observer_mode=False,
        install_candidates=(),
        fast_results={},
        js8_results={},
        varac_results=varac_results,
        js8_file_profiles=(),
        default_ports={},
        profile_name="FIO-C",
    )

    assert suggestions["varac_install_path"] == "/Applications/VarAC"
    assert suggestions["varac_db_path"] == "/Applications/VarAC/VarAC.db"
    assert suggestions["varac_ini_path"] == "/Applications/VarAC/VarAC.ini"
    assert suggestions["varac_incoming_path"] == "/Applications/VarAC/RX Files"
    assert suggestions["varac_outbox_dir"] == "/Applications/VarAC/Outbox"
    assert suggestions["varac_bbs_dir"] == "/Applications/VarAC/BBS"
    assert suggestions["varac_bbs_archive_dir"] == "/Applications/VarAC/BBS/Archive"
    assert review == (
        "VarAC database and cluster membership were not changed. "
        "BBS settings were not changed. Review VarAC cluster settings separately.",
    )
    assert "control_backend" not in suggestions
    assert "use_flrig" not in suggestions
    assert "use_js8call" not in suggestions
    assert "frequency_plan" not in suggestions
    assert "schedule_assignment_id" not in suggestions
    assert "flrig_host" not in suggestions
    assert "flrig_port" not in suggestions
    assert "fldigi_host" not in suggestions
    assert "fldigi_port" not in suggestions
    assert "js8_host" not in suggestions
    assert "js8_port" not in suggestions
