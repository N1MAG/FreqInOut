from __future__ import annotations

from freqinout.core.config_migration_preview import (
    build_single_rig_upgrade_preview,
    collect_referenced_data_paths_not_backed_up,
    collect_single_rig_upgrade_backup_paths,
)


def test_single_rig_upgrade_preview_ports_existing_fast_light_and_js8_settings(tmp_path) -> None:
    settings = {
        "control_via": "FLRig",
        "path_flrig": "/Applications/FLRig.app",
        "flrig_host": "127.0.0.1",
        "flrig_port": 12345,
        "path_fldigi": "/Applications/FLDigi.app",
        "fldigi_host": "127.0.0.1",
        "fldigi_port": 7362,
        "path_js8call": "/Applications/JS8Call.app",
        "js8_host": "127.0.0.1",
        "js8_port": 2442,
        "path_js8spotter": "/Applications/JS8Spotter.app",
        "message_paths": {},
    }

    preview = build_single_rig_upgrade_preview(
        settings,
        radio_name="Icom 7300",
        operating_plan_name="Daily HF",
        config_dir=tmp_path / "fio-config",
    )

    assert preview.radio_profile["name"] == "Icom 7300"
    assert preview.radio_profile["control_backend"] == "flrig"
    assert preview.radio_profile["flrig_port"] == 12345
    assert preview.radio_profile["fldigi_port"] == 7362
    assert preview.radio_profile["js8_port"] == 2442
    assert preview.radio_profile["use_flrig"] == 1
    assert preview.radio_profile["use_fldigi"] == 1
    assert preview.radio_profile["use_js8call"] == 1
    assert preview.radio_profile["use_varac"] == 0
    assert preview.enabled_software_roles == ("fast_light", "js8call", "js8spotter")
    assert preview.operating_profile["name"] == "Daily HF"
    assert preview.backup_paths == (str(tmp_path / "fio-config"),)
    assert preview.referenced_paths_not_backed_up == ()
    assert "Icom 7300" in preview.summary
    assert "using FLRig with FLRig/FLDigi, JS8Call, JS8Spotter" in preview.summary
    assert "VarAC will remain disabled" in " ".join(preview.warnings)


def test_single_rig_upgrade_preview_defaults_to_manual_when_no_control_app_configured(tmp_path) -> None:
    preview = build_single_rig_upgrade_preview(
        {"control_via": "Manual", "message_paths": {}},
        config_dir=tmp_path / "fio-config",
    )

    assert preview.radio_profile["control_backend"] == "manual"
    assert preview.enabled_software_roles == ()
    assert preview.summary.endswith("using Manual control.")
    assert "No radio control app is configured yet" in " ".join(preview.warnings)


def test_single_rig_upgrade_preview_labels_rigctld_as_control_not_manual(tmp_path) -> None:
    preview = build_single_rig_upgrade_preview(
        {"control_via": "RIGCTLD", "rig_host": "127.0.0.1", "rig_port": 4532, "message_paths": {}},
        config_dir=tmp_path / "fio-config",
    )

    assert preview.radio_profile["control_backend"] == "rigctld"
    assert preview.summary.endswith("using RigCtlD.")
    assert "Manual control" not in preview.summary


def test_single_rig_upgrade_backup_paths_include_config_and_external_profile_hints(tmp_path) -> None:
    settings = {
        "js8_profile_path": str(tmp_path / "JS8Call.ini"),
        "js8_directed_path": str(tmp_path / "DIRECTED.TXT"),
        "varac_ini_path": str(tmp_path / "VarAC.ini"),
    }

    paths = collect_single_rig_upgrade_backup_paths(
        settings,
        config_dir=tmp_path / "fio-config",
        extra_backup_paths=[tmp_path / "JS8Call.ini"],
    )

    assert paths == (
        str(tmp_path / "fio-config"),
        str(tmp_path / "JS8Call.ini"),
        str(tmp_path / "DIRECTED.TXT"),
        str(tmp_path / "VarAC.ini"),
    )


def test_single_rig_upgrade_preview_reports_referenced_data_paths_not_backed_up(tmp_path) -> None:
    settings = {
        "control_via": "FLRig",
        "fldigi_log_path": str(tmp_path / "fldigi-logs"),
        "fldigi_checkin_dir": str(tmp_path / "checkins"),
        "varac_bbs_dir": str(tmp_path / "bbs"),
        "message_paths": {
            "flmsg": str(tmp_path / "flmsg-messages"),
            "flamp": str(tmp_path / "flamp-rx"),
        },
    }

    preview = build_single_rig_upgrade_preview(settings, config_dir=tmp_path / "fio-config")

    assert collect_referenced_data_paths_not_backed_up(settings) == (
        str(tmp_path / "fldigi-logs"),
        str(tmp_path / "checkins"),
        str(tmp_path / "bbs"),
        str(tmp_path / "flmsg-messages"),
        str(tmp_path / "flamp-rx"),
    )
    assert preview.referenced_paths_not_backed_up == collect_referenced_data_paths_not_backed_up(settings)
    assert "referenced but not backed up" in " ".join(preview.warnings)
