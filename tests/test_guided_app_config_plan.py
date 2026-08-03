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
    )

    assert plan.manual_review_required is True
    assert plan.review_items == (
        "VarAC app paths may be remembered, but VarAC database, BBS, and cluster membership require separate review.",
    )
    assert not any(action.app_id == "varac" and action.writes_external_config for action in plan.actions)
