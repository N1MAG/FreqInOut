from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from freqinout.core.guided_setup import (
    CONTROL_FLRIG,
    CONTROL_JS8CALL,
    CONTROL_NONE,
    CONTROL_RIGCTLD,
    LANE_JS8_ONLY,
    LANE_VARAC_CLUSTER,
    LANE_VARAC,
    SCHEDULE_NONE,
    SETUP_MODE_MANAGED,
    SETUP_MODE_READ_ONLY,
    answer_guided_setup_step,
    allow_external_app_writes,
    build_guided_setup_blueprint,
    build_app_config_plan_for_blueprint,
    build_guided_setup_preview,
    generated_radio_label,
    guided_setup_capability_policy,
    guided_setup_schedule_summary,
    guided_setup_review_items,
    guided_setup_apps_for_lane,
    infer_guided_control_route,
    infer_guided_setup_lane,
    normalize_guided_radio_profile_payload,
    radio_proposal_for_blueprint,
    radio_proposals_for_blueprint,
    selected_app_map_for_blueprint,
    start_guided_setup_session,
)
from freqinout.core.config_autodiscovery import build_lab_radio_proposals
from freqinout.core.guided_app_config_plan import build_guided_external_app_config_plan


def test_js8call_only_radio_or_sdr_does_not_include_fast_light_apps() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8call",
        hamlib_short_name="TS-2000",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_JS8CALL,
        receive_only=True,
    )

    assert blueprint.lane == LANE_JS8_ONLY
    assert blueprint.radio_label == "TS-2000"
    assert blueprint.selected_apps == ("js8call",)
    assert "fldigi" not in blueprint.selected_apps
    assert "flmsg" not in blueprint.selected_apps
    assert "flamp" not in blueprint.selected_apps
    assert any("Fast Light fields are not part" in note for note in blueprint.notes)
    assert any(item.item_id == "no_fast_light" and item.status == "skipped" for item in blueprint.proposal_items)


def test_core_infers_guided_setup_lane_from_selected_apps() -> None:
    assert infer_guided_setup_lane(("js8call",)) == LANE_JS8_ONLY
    assert infer_guided_setup_lane(("js8call", "js8spotter", "commstat")) == LANE_JS8_ONLY
    assert infer_guided_setup_lane(("flrig", "fldigi", "flmsg", "flamp")) == "fast_light"
    assert infer_guided_setup_lane(("flrig", "fldigi", "js8call")) == "tri_mode"
    assert infer_guided_setup_lane(("varac",), varac_selected=True) == LANE_VARAC
    assert infer_guided_setup_lane((), varac_selected=True) == LANE_VARAC
    assert infer_guided_setup_lane((), receive_only=True) == "sdr_observer"


def test_core_normalizes_guided_control_routes_from_ui_backend_values() -> None:
    assert infer_guided_control_route("flrig") == CONTROL_FLRIG
    assert infer_guided_control_route("rigctl") == CONTROL_RIGCTLD
    assert infer_guided_control_route("hamlib") == CONTROL_RIGCTLD
    assert infer_guided_control_route("js8call_cat") == CONTROL_JS8CALL
    assert infer_guided_control_route("manual") == CONTROL_NONE
    assert infer_guided_control_route("varac") == CONTROL_NONE
    assert infer_guided_control_route("") == "later"


def test_js8call_only_capability_policy_hides_fast_light_fields_but_allows_js8_control() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8call",
        hamlib_short_name="SDRplay",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_JS8CALL,
        include_spotter=True,
        include_commstat=True,
    )

    policy = guided_setup_capability_policy(blueprint)

    assert policy.visible_apps == ("js8call", "js8spotter", "commstat")
    assert {"fldigi", "flmsg", "flamp"}.issubset(set(policy.hidden_apps))
    assert policy.default_control_route == CONTROL_JS8CALL
    assert policy.control_routes[:3] == (CONTROL_JS8CALL, CONTROL_FLRIG, CONTROL_RIGCTLD)
    assert policy.fio_frequency_control_allowed is True
    assert policy.scheduler_assignment_allowed is True
    assert policy.qsy_controls_visible is True
    assert policy.external_writes_allowed is True
    assert policy.external_writes_require_backup is True


def test_generated_radio_label_uses_hamlib_short_name_and_increments() -> None:
    assert generated_radio_label("TS-2000", []) == "TS-2000"
    assert generated_radio_label("TS-2000", ["TS-2000"]) == "TS-2000 1"
    assert generated_radio_label("TS-2000", ["TS-2000", "TS-2000 1"]) == "TS-2000 2"
    assert generated_radio_label("", ["Radio"]) == "Radio 1"


def test_read_only_setup_does_not_plan_external_app_writes() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_JS8CALL,
    )

    assert blueprint.writes_external_config is False
    assert blueprint.backup_required is False
    assert allow_external_app_writes(blueprint) is False
    assert any("does not change external app configuration" in note for note in blueprint.notes)


def test_managed_js8call_setup_is_backup_gated() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_JS8CALL,
    )

    assert blueprint.writes_external_config is True
    assert blueprint.backup_required is True
    assert allow_external_app_writes(blueprint) is True
    assert any(item.item_id == "apps" and item.requires_backup for item in blueprint.proposal_items)


def test_read_only_blueprint_blocks_external_app_config_plan_writes(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_JS8CALL,
    )
    base_proposal = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)[0]
    proposal = radio_proposal_for_blueprint(blueprint, base_proposal)

    plan = build_guided_external_app_config_plan(
        (proposal,),
        config_root=tmp_path / "fio-config",
        app_paths={"js8call": "/apps/js8call"},
        allow_external_writes=allow_external_app_writes(blueprint),
    )

    assert plan.actions == tuple()
    assert plan.backup_required is False
    assert plan.review_items == (
        "Read-only setup will remember JS8Call references in FIO without changing external app configuration.",
    )


def test_js8call_only_blueprint_scopes_autofill_and_planner_apps() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        include_spotter=True,
        include_commstat=True,
    )
    selected = selected_app_map_for_blueprint(blueprint)
    base_proposal = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)[0]
    proposal = radio_proposal_for_blueprint(blueprint, base_proposal)

    assert selected["js8call"] is True
    assert selected["js8spotter"] is True
    assert selected["commstat"] is True
    assert selected["flrig"] is False
    assert selected["fldigi"] is False
    assert selected["flmsg"] is False
    assert selected["flamp"] is False
    assert proposal.enabled_apps == ("js8call",)


def test_blueprint_bridge_keeps_read_only_plan_from_writing_external_configs(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_JS8CALL,
    )
    base_proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)

    plan = build_app_config_plan_for_blueprint(
        blueprint,
        base_proposals,
        config_root=tmp_path / "fio-config",
        app_paths={"js8call": "/apps/js8call"},
    )

    assert plan.actions == tuple()
    assert plan.backup_required is False
    assert "without changing external app configuration" in plan.review_items[0]


def test_blueprint_bridge_scopes_managed_js8_only_plan_to_js8call(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_JS8CALL,
    )
    base_proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)

    plan = build_app_config_plan_for_blueprint(
        blueprint,
        base_proposals,
        config_root=tmp_path / "fio-config",
        app_paths={
            "flrig": "/apps/flrig",
            "fldigi": "/apps/fldigi",
            "js8call": "/apps/js8call",
        },
    )

    write_actions = [action for action in plan.actions if action.writes_external_config]
    assert [action.app_id for action in write_actions] == ["js8call"]
    assert plan.backup_required is True
    assert all(action.app_id not in {"flrig", "fldigi", "flmsg", "flamp"} for action in plan.actions)
    js8_action = write_actions[0]
    assert js8_action.details["control_route"] == CONTROL_JS8CALL
    assert "FLRig" not in js8_action.summary
    assert "Confirm JS8Call's radio/CAT selection" in " ".join(js8_action.notes)


def test_blueprint_bridge_keeps_varac_read_import_action_in_mixed_managed_plan(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="tri_mode",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_FLRIG,
        include_varac=True,
    )
    base_proposals = build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False)
    base_proposals = (replace(base_proposals[0], varac_enabled=True),)

    plan = build_app_config_plan_for_blueprint(
        blueprint,
        base_proposals,
        config_root=tmp_path / "fio-config",
        app_paths={
            "flrig": "/apps/flrig",
            "fldigi": "/apps/fldigi",
            "js8call": "/apps/js8call",
            "varac": "/apps/VarAC",
            "varac_ini_path": "/apps/VarAC/VarAC.ini",
        },
    )

    varac_actions = [action for action in plan.actions if action.app_id == "varac"]
    write_actions = [action for action in plan.actions if action.writes_external_config]
    policy = guided_setup_capability_policy(blueprint)
    assert "varac" in blueprint.selected_apps
    assert "varac" in policy.visible_apps
    assert any("VarAC is included as a read/import integration" in note for note in blueprint.notes)
    assert len(varac_actions) == 1
    assert varac_actions[0].action_type == "remember_integration"
    assert varac_actions[0].writes_external_config is False
    assert {action.app_id for action in write_actions} == {"flrig", "fldigi", "js8call"}


def test_guided_setup_preview_keeps_ui_summary_in_core_for_js8(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_JS8CALL,
    )
    plan = build_app_config_plan_for_blueprint(
        blueprint,
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=tmp_path / "fio-config",
        app_paths={"js8call": "/apps/js8call"},
    )

    preview = build_guided_setup_preview(blueprint, plan)

    assert preview.qsy_controls_visible is True
    assert preview.scheduler_assignment_allowed is True
    assert preview.backup_required is True
    assert preview.control_summary == "Control: JS8Call owns the radio/CAT route for FIO scheduler and QSY actions."
    assert preview.guided_path == "Guided path: Station Use -> Frequency Control -> Schedule."
    assert preview.schedule_summary == (
        "Schedule choices: existing Frequency Plan, JS8Call Standard, Daily with No Nets, Daily + Nets, "
        "SOP condition plan, monitor only."
    )
    assert preview.schedule_summary in preview.lines
    assert "Backup required before FIO writes app profiles." in preview.lines
    assert any("Prepare JS8Call profile" in line for line in preview.lines)


def test_guided_setup_preview_blocks_varac_scheduler_controls(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="FT-891",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_FLRIG,
    )
    plan = build_app_config_plan_for_blueprint(
        blueprint,
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=tmp_path / "fio-config",
        app_paths={
            "varac": "/apps/VarAC",
            "varac_ini_path": "/apps/VarAC/VarAC.ini",
            "varac_db_path": "/apps/VarAC/VarAC.db",
            "varac_incoming_dir": "/apps/VarAC/Incoming",
            "varac_outbox_dir": "/apps/VarAC/Outbox",
            "varac_bbs_dir": "/apps/VarAC/BBS",
            "varac_bbs_archive_dir": "/apps/VarAC/BBS/Archive",
        },
    )

    preview = build_guided_setup_preview(blueprint, plan)

    assert preview.qsy_controls_visible is False
    assert preview.scheduler_assignment_allowed is False
    assert preview.backup_required is False
    assert preview.control_summary == "Control: monitor/import only. FIO will not show scheduler/QSY controls for this radio."
    assert preview.schedule_summary == "Schedule: monitor only. VarAC keeps its own scheduler and FIO will not offer QSY controls."
    assert preview.schedule_summary in preview.lines
    assert preview.lines[0] == "VarAC-only radio: FIO supports BBS and message monitoring, but VarAC handles frequency scheduling."
    assert "  VarAC references: install, INI, DB, incoming, outbox, BBS, BBS archive." in preview.lines


def test_varac_only_payload_normalization_clears_stale_flrig_when_manual() -> None:
    normalized = normalize_guided_radio_profile_payload(
        {
            "name": "VarAC Radio",
            "control_backend": "manual",
            "use_varac": True,
            "use_flrig": True,
            "use_fldigi": False,
            "use_flmsg": False,
            "use_flamp": False,
            "use_js8call": False,
            "use_js8spotter": False,
            "use_commstat": False,
        }
    )

    assert normalized["control_backend"] == "manual"
    assert normalized["use_varac"] is True
    assert normalized["use_flrig"] is False
    assert normalized["use_fldigi"] is False
    assert normalized["use_js8call"] is False


def test_varac_payload_normalization_keeps_explicit_flrig_mixed_route() -> None:
    normalized = normalize_guided_radio_profile_payload(
        {
            "name": "VarAC Mixed Radio",
            "control_backend": "flrig",
            "use_varac": True,
            "use_flrig": True,
            "use_fldigi": False,
            "use_flmsg": False,
            "use_flamp": False,
            "use_js8call": False,
            "use_js8spotter": False,
            "use_commstat": False,
        }
    )

    assert normalized["control_backend"] == "flrig"
    assert normalized["use_varac"] is True
    assert normalized["use_flrig"] is True


def test_guided_setup_preview_describes_rigctld_control_route(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="SDRplay",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_RIGCTLD,
    )
    plan = build_app_config_plan_for_blueprint(
        blueprint,
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=tmp_path / "fio-config",
        app_paths={"js8call": "/apps/js8call"},
    )

    preview = build_guided_setup_preview(blueprint, plan)

    assert preview.control_summary == "Control: RigCtlD owns the radio route for FIO scheduler and QSY actions."
    assert preview.qsy_controls_visible is True
    assert preview.backup_required is False


def test_radio_proposals_for_blueprint_scopes_each_radio_independently() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="tri_mode",
        hamlib_short_name="TS-2000",
        setup_mode=SETUP_MODE_MANAGED,
    )
    base_proposals = build_lab_radio_proposals(radio_count=2, busy_checker=lambda _host, _port: False)

    scoped = radio_proposals_for_blueprint(blueprint, base_proposals)

    assert len(scoped) == 2
    assert [proposal.instance_name for proposal in scoped] == ["fio-a", "fio-b"]
    assert all(proposal.enabled_apps == ("flrig", "fldigi", "js8call") for proposal in scoped)


def test_js8call_only_inherits_flrig_route_when_js8_config_uses_flrig() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="TS-2000",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_JS8CALL,
        js8call_uses_flrig=True,
    )

    assert blueprint.control_route == CONTROL_FLRIG
    control_step = next(step for step in blueprint.steps if step.step_id == "frequency_control")
    recommended = [choice.choice_id for choice in control_step.choices if choice.recommended]
    assert recommended == [CONTROL_FLRIG]
    assert any(choice.label == "FLRig controls the TS-2000" for choice in control_step.choices)


def test_js8call_standard_frequencies_are_visible_without_named_plan() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="SDRplay",
        receive_only=True,
    )

    schedule_step = next(step for step in blueprint.steps if step.step_id == "schedule_intent")
    assert any(choice.choice_id == "js8_standard" for choice in schedule_step.choices)
    assert any(item.item_id == "js8_standard_frequencies" for item in blueprint.proposal_items)
    assert guided_setup_schedule_summary(blueprint) == (
        "Schedule choices: existing Frequency Plan, JS8Call Standard, Daily with No Nets, Daily + Nets, "
        "SOP condition plan, monitor only."
    )


def test_varac_cluster_setup_remains_read_only_in_initial_blueprint() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac_cluster",
        hamlib_short_name="IC-705",
        setup_mode=SETUP_MODE_READ_ONLY,
    )

    assert blueprint.lane == LANE_VARAC_CLUSTER
    assert guided_setup_apps_for_lane("varac_cluster") == ("varac",)
    assert blueprint.writes_external_config is False
    assert any(item.item_id == "varac_cluster_read_only" for item in blueprint.proposal_items)


def test_rigctl_choice_uses_rigctld_core_value() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="TS-2000",
        control_route="rigctl",
    )

    assert blueprint.control_route == CONTROL_RIGCTLD
    control_step = next(step for step in blueprint.steps if step.step_id == "frequency_control")
    recommended = [choice for choice in control_step.choices if choice.recommended]
    assert len(recommended) == 1
    assert recommended[0].choice_id == CONTROL_RIGCTLD
    assert recommended[0].label == "RigCtlD controls the TS-2000"


def test_varac_managed_blueprint_does_not_claim_external_config_writes() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="IC-705",
        setup_mode=SETUP_MODE_MANAGED,
    )

    assert blueprint.lane == LANE_VARAC
    assert blueprint.selected_apps == ("varac",)
    assert allow_external_app_writes(blueprint) is False
    assert blueprint.writes_external_config is False
    assert blueprint.backup_required is False
    assert any(item.item_id == "varac_read_import_only" for item in blueprint.proposal_items)
    assert any("does not write VarAC.ini or VarAC DB" in note for note in blueprint.notes)
    assert any("limited to FIO-side integration settings" in note for note in blueprint.notes)
    assert not any("may write reviewed app configuration" in note for note in blueprint.notes)


def test_varac_standalone_uses_radio_name_without_fast_light_or_js8_fields() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="FT-891",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_NONE,
    )
    selected = selected_app_map_for_blueprint(blueprint)

    assert blueprint.lane == LANE_VARAC
    assert blueprint.radio_label == "FT-891"
    assert blueprint.selected_apps == ("varac",)
    assert selected["varac"] is True
    assert selected["js8call"] is False
    assert selected["flrig"] is False
    assert selected["fldigi"] is False
    assert selected["flmsg"] is False
    assert selected["flamp"] is False
    assert any("radio name directly" in note for note in blueprint.notes)
    assert any("does not show scheduler/QSY" in note for note in blueprint.notes)


def test_varac_only_never_offers_fio_frequency_control() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="IC-705",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route="varac_cat",
    )

    assert blueprint.control_route == CONTROL_NONE
    control_step = next(step for step in blueprint.steps if step.step_id == "frequency_control")
    recommended = [choice for choice in control_step.choices if choice.recommended]
    assert len(recommended) == 1
    assert recommended[0].choice_id == CONTROL_NONE
    assert [choice.choice_id for choice in control_step.choices] == [CONTROL_NONE]
    assert control_step.choices[0].label == "VarAC controls its own frequency"
    assert any(item.item_id == "varac_no_fio_frequency_control" for item in blueprint.proposal_items)
    assert any(item.item_id == "varac_read_import_only" for item in blueprint.proposal_items)
    assert any("VarAC's scheduler" in note for note in blueprint.notes)
    assert any("does not write VarAC.ini or VarAC DB" in note for note in blueprint.notes)


def test_varac_only_profile_payload_normalizes_to_manual_monitoring() -> None:
    payload = normalize_guided_radio_profile_payload(
        {
            "name": "VarAC Portable",
            "control_backend": "flrig",
            "use_varac": True,
            "use_flrig": False,
            "use_fldigi": False,
            "use_flmsg": False,
            "use_flamp": False,
            "use_js8call": False,
            "use_js8spotter": False,
            "use_commstat": False,
        }
    )

    assert payload["control_backend"] == "manual"
    assert payload["use_varac"] is True
    assert payload["use_flrig"] is False
    assert payload["use_js8call"] is False


def test_mixed_varac_profile_payload_keeps_explicit_control_route() -> None:
    payload = normalize_guided_radio_profile_payload(
        {
            "name": "Shared Radio",
            "control_backend": "js8call",
            "use_varac": True,
            "use_js8call": True,
            "use_flrig": False,
        }
    )

    assert payload["control_backend"] == "js8call"
    assert payload["use_varac"] is True
    assert payload["use_js8call"] is True


def test_varac_capability_policy_blocks_scheduler_and_qsy_controls() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="IC-705",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_FLRIG,
    )

    policy = guided_setup_capability_policy(blueprint)

    assert policy.visible_apps == ("varac",)
    assert "js8call" in policy.hidden_apps
    assert policy.control_routes == (CONTROL_NONE,)
    assert policy.default_control_route == CONTROL_NONE
    assert policy.fio_frequency_control_allowed is False
    assert policy.scheduler_assignment_allowed is False
    assert policy.qsy_controls_visible is False
    assert policy.external_writes_allowed is False
    assert policy.external_writes_require_backup is False
    assert any("VarAC owns scheduler" in note for note in policy.read_only_notes)
    assert [choice.choice_id for choice in blueprint.schedule_choices] == [SCHEDULE_NONE]
    assert blueprint.schedule_choices[0].recommended is True


def test_varac_cluster_frequency_control_choices_are_read_only() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac_cluster",
        hamlib_short_name="FTDX10",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_FLRIG,
    )

    policy = guided_setup_capability_policy(blueprint)
    control_step = next(step for step in blueprint.steps if step.step_id == "frequency_control")

    assert blueprint.control_route == CONTROL_NONE
    assert [choice.choice_id for choice in control_step.choices] == [CONTROL_NONE]
    assert [choice.choice_id for choice in blueprint.schedule_choices] == [SCHEDULE_NONE]
    assert policy.control_routes == (CONTROL_NONE,)
    assert policy.qsy_controls_visible is False


def test_observer_capability_policy_is_monitor_only() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="sdr_observer",
        hamlib_short_name="Airspy HF",
        receive_only=True,
        control_route=CONTROL_NONE,
    )

    policy = guided_setup_capability_policy(blueprint)

    assert policy.visible_apps == tuple()
    assert policy.control_routes == (CONTROL_NONE, "later")
    assert policy.fio_frequency_control_allowed is False
    assert policy.scheduler_assignment_allowed is False
    assert policy.qsy_controls_visible is False
    assert any("Receive-only observer" in note for note in policy.read_only_notes)


def test_guided_setup_session_advances_to_review_only_after_required_steps() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        control_route=CONTROL_JS8CALL,
    )
    session = start_guided_setup_session(blueprint)

    assert session.current_step is not None
    assert session.current_step.step_id == "station_use"
    assert guided_setup_review_items(session) == tuple()

    session = answer_guided_setup_step(session, "station_use", LANE_JS8_ONLY)
    assert session.current_step is not None
    assert session.current_step.step_id == "frequency_control"

    session = answer_guided_setup_step(session, "frequency_control", CONTROL_JS8CALL)
    assert session.current_step is not None
    assert session.current_step.step_id == "schedule_intent"

    session = answer_guided_setup_step(session, "schedule_intent", "js8_standard")
    assert session.current_step is None
    assert session.ready_for_review is True
    assert guided_setup_review_items(session) == blueprint.proposal_items


def test_guided_setup_session_rejects_invalid_choices() -> None:
    blueprint = build_guided_setup_blueprint(lane="js8_only", hamlib_short_name="IC-7300")
    session = start_guided_setup_session(blueprint)

    try:
        answer_guided_setup_step(session, "station_use", "flamp_only")
    except ValueError as exc:
        assert "Invalid choice" in str(exc)
    else:
        raise AssertionError("invalid guided setup choice should fail")

    try:
        answer_guided_setup_step(session, "not_a_step", LANE_JS8_ONLY)
    except ValueError as exc:
        assert "Unknown guided setup step" in str(exc)
    else:
        raise AssertionError("unknown guided setup step should fail")


def test_settings_guided_add_radio_uses_setup_type_selector_as_ui_shell() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    dialog_block = source[
        source.index("def _open_device_profile_dialog")
        : source.index("def _apply_runtime_projection_widgets")
    ]

    assert 'setup_type_combo.setObjectName("guidedSetupType")' in dialog_block
    assert 'setup_type_combo.addItem("JS8Call only", LANE_JS8_ONLY)' in dialog_block
    assert 'setup_type_combo.addItem("VarAC only", LANE_VARAC)' in dialog_block
    assert 'setup_type_combo.addItem("Receive-only SDR", LANE_SDR_OBSERVER)' in dialog_block
    assert 'setup_type_combo.addItem("Custom software mix", "custom")' in dialog_block
    assert "def _apply_setup_type_choice()" in dialog_block
    assert "_set_combo_data(backend_combo, CONTROL_JS8CALL)" in dialog_block
    assert "_set_combo_data(backend_combo, CONTROL_FLRIG)" in dialog_block
    assert '_set_combo_data(backend_combo, "manual")' in dialog_block
    assert "_checkbox_set_checked(use_fldigi_chk, False)" in dialog_block
    assert "_checkbox_set_checked(use_varac_chk, True)" in dialog_block
    assert 'apps.append("flmsg")' in dialog_block
    assert 'apps.append("flamp")' in dialog_block
    assert 'apps.append("js8spotter")' in dialog_block
    assert 'apps.append("commstat")' in dialog_block
    assert "include_spotter=use_js8spotter_chk.isChecked()" in dialog_block
    assert "include_commstat=use_commstat_chk.isChecked()" in dialog_block
    assert "lane = _current_guided_lane()" in dialog_block
    assert "Setup type: {setup_type_label}" in dialog_block
