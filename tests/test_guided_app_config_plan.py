from __future__ import annotations

from freqinout.core.config_autodiscovery import build_lab_radio_proposals
from freqinout.core.guided_app_config_plan import build_guided_external_app_config_plan


def test_guided_external_app_config_plan_is_review_only_and_backup_gated(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)

    plan = build_guided_external_app_config_plan(
        proposals,
        config_root=tmp_path / "fio-config",
        app_paths={
            "flrig": "/Applications/RadioApps/FLRig.app",
            "fldigi": "/Applications/RadioApps/FLDigi.app",
            "js8call": "/Applications/JS8Call.app",
        },
        callsign="n1mag",
        grid="dm79",
    )

    assert not any((tmp_path / "fio-config").glob("**/*"))
    assert plan.backup_required is True
    assert plan.blocked is False
    directory_actions = [action for action in plan.actions if action.action_type == "create_directory"]
    write_actions = [action for action in plan.actions if action.writes_external_config]
    assert directory_actions
    assert all(not action.requires_backup for action in directory_actions)
    assert all(action.requires_backup for action in write_actions)
    assert {action.app_id for action in write_actions} == {"flrig", "fldigi", "js8call"}


def test_guided_external_app_config_plan_describes_fast_light_instances(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)

    plan = build_guided_external_app_config_plan(
        proposals,
        config_root=tmp_path / "fio-config",
        app_paths={"flrig": "/apps/flrig", "fldigi": "/apps/fldigi"},
    )
    actions = {action.action_id: action for action in plan.actions}

    flrig = actions["fio-a:flrig:write-managed-config"]
    assert flrig.summary == "Prepare FLRig profile fio-a on 127.0.0.1:12345."
    assert flrig.details["executable_path"] == "/apps/flrig"
    assert flrig.details["expected_port"] == "12345"
    assert flrig.details["launch_args"].endswith("managed-instances/fio-a/flrig")

    fldigi = actions["fio-a:fldigi:write-managed-config"]
    assert fldigi.summary == "Prepare FLDigi profile fio-a on 127.0.0.1:7362."
    assert fldigi.details["executable_path"] == "/apps/fldigi"
    assert fldigi.details["expected_port"] == "7362"
    assert "--xmlrpc-server-port 7362" in fldigi.details["launch_args"]


def test_guided_external_app_config_plan_describes_js8_profile_and_ports(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=2, busy_checker=lambda _host, _port: False)

    plan = build_guided_external_app_config_plan(
        proposals,
        config_root=tmp_path / "fio-config",
        app_paths={"js8call": "/apps/js8call"},
        callsign="n1mag",
        grid="dm79",
    )
    js8_actions = [action for action in plan.actions if action.action_type == "update_js8_multisettings"]

    assert [action.instance_name for action in js8_actions] == ["fio-a", "fio-b"]
    assert js8_actions[0].summary == "Prepare JS8Call profile fio-a with FLRig 127.0.0.1:12345 and API port 2442."
    assert js8_actions[0].details["executable_path"] == "/apps/js8call"
    assert js8_actions[0].details["directed_path"].endswith("managed-instances/fio-a/js8call/DIRECTED.TXT")
    assert js8_actions[1].details["flrig_port"] == "12346"
    assert js8_actions[1].details["tcp_port"] == "2443"
    assert "backup of the existing JS8Call.ini" in " ".join(js8_actions[0].notes)


def test_guided_external_app_config_plan_keeps_varac_cluster_manual(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)

    plan = build_guided_external_app_config_plan(
        proposals,
        config_root=tmp_path / "fio-config",
        include_varac=True,
        allow_external_writes=False,
    )

    assert plan.manual_review_required is True
    assert any("without changing external app configuration" in item for item in plan.review_items)
    assert "VarAC guided setup is read/import only: FIO remembers paths and monitors VarAC data without rewriting VarAC.ini or VarAC DB." in plan.review_items
    assert "Use the dedicated VarAC BBS settings workflow for explicit [BBS] section sync; cluster membership remains read-only in this release." in plan.review_items
    assert not any(action.app_id == "varac" and action.writes_external_config for action in plan.actions)


def test_guided_external_app_config_plan_varac_remembers_integration_without_writes(tmp_path) -> None:
    proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)
    varac_dir = tmp_path / "VarAC"
    bbs_dir = varac_dir / "BBS"
    bbs_archive_dir = bbs_dir / "Archive"
    bbs_dir.mkdir(parents=True)
    bbs_archive_dir.mkdir(parents=True)
    (varac_dir / "VarAC.ini").write_text(
        "[MY_INFO]\nMycall=N1MAG\nMyLocator=DM79QJ\n[RIG_CONTROL]\nRigFreqControlType=FLRIG\n",
        encoding="utf-8",
    )
    (varac_dir / "VarAC.db").write_bytes(b"")
    (varac_dir / "VarAC_traffic.log").write_text("", encoding="utf-8")
    (varac_dir / "VarAC.log").write_text("", encoding="utf-8")
    (varac_dir / "VarAC_qso_log.adi").write_text("", encoding="utf-8")
    (varac_dir / "VarAC_callsign_tags.conf").write_text("", encoding="utf-8")
    (varac_dir / "VarAC_alert_tags.conf").write_text("", encoding="utf-8")
    (varac_dir / "VarAC_templates.ini").write_text("", encoding="utf-8")

    plan = build_guided_external_app_config_plan(
        proposals,
        config_root=tmp_path / "fio-config",
        app_paths={
            "varac": str(varac_dir),
            "varac_ini_path": str(varac_dir / "VarAC.ini"),
            "varac_db_path": str(varac_dir / "VarAC.db"),
            "varac_incoming_dir": str(varac_dir / "Incoming"),
            "varac_bbs_dir": str(bbs_dir),
            "varac_bbs_archive_dir": str(bbs_archive_dir),
            "varac_traffic_log": str(varac_dir / "VarAC_traffic.log"),
            "varac_log": str(varac_dir / "VarAC.log"),
        },
        include_varac=True,
        allow_external_writes=False,
    )

    varac_actions = [action for action in plan.actions if action.app_id == "varac"]
    assert len(varac_actions) == 1
    action = varac_actions[0]
    assert action.action_type == "remember_integration"
    assert action.requires_backup is False
    assert action.writes_external_config is False
    assert action.manual_review_required is True
    assert action.details["install_path"] == str(varac_dir)
    assert action.details["ini_path"] == str(varac_dir / "VarAC.ini")
    assert action.details["db_path"] == str(varac_dir / "VarAC.db")
    assert action.details["incoming_dir"] == str(varac_dir / "Incoming")
    assert action.details["bbs_dir"] == str(bbs_dir)
    assert action.details["bbs_archive_dir"] == str(bbs_archive_dir)
    assert action.details["bbs_archive_path"] == str(bbs_archive_dir)
    assert action.details["traffic_log_path"] == str(varac_dir / "VarAC_traffic.log")
    assert action.details["app_log_path"] == str(varac_dir / "VarAC.log")
    assert action.details["qso_log_path"] == str(varac_dir / "VarAC_qso_log.adi")
    assert action.details["callsign_tags_path"] == str(varac_dir / "VarAC_callsign_tags.conf")
    assert action.details["alert_tags_path"] == str(varac_dir / "VarAC_alert_tags.conf")
    assert action.details["templates_path"] == str(varac_dir / "VarAC_templates.ini")
    assert "VarAC.ini" in action.details["readable_assets"]
    assert "station N1MAG DM79QJ" in action.details["ini_detail"]
    assert "does not write VarAC.ini" in " ".join(action.notes)
    assert plan.backup_required is False
