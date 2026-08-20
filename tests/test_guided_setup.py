from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from freqinout.core.guided_setup import (
    CONTROL_FLRIG,
    CONTROL_JS8CALL,
    CONTROL_NONE,
    CONTROL_RIGCTLD,
    APP_INSTANCE_EXISTING,
    APP_INSTANCE_MANAGED,
    APP_INSTANCE_MANUAL,
    LANE_FAST_LIGHT,
    LANE_JS8_ONLY,
    LANE_SDR_OBSERVER,
    LANE_TRI_MODE,
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
    guided_radio_label_base,
    guided_setup_autofill_review,
    guided_setup_capability_policy,
    guided_setup_field_visibility,
    guided_setup_flow_items,
    guided_setup_next_action_text,
    guided_setup_next_flow_item,
    guided_setup_flow_summary_lines,
    guided_setup_lane_preset,
    guided_setup_operator_guidance_lines,
    guided_setup_role_hint,
    guided_setup_selected_apps_from_flags,
    guided_setup_software_hint,
    guided_setup_schedule_decision,
    guided_setup_schedule_summary,
    guided_setup_review_items,
    guided_setup_apps_for_lane,
    guided_setup_app_label,
    infer_guided_control_route,
    infer_guided_setup_lane,
    normalize_guided_radio_profile_payload,
    radio_proposal_for_blueprint,
    radio_proposals_for_blueprint,
    selected_app_map_for_blueprint,
    start_guided_setup_session,
    GuidedSetupFieldVisibilityInput,
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


def test_core_normalizes_guided_app_flags_from_backend_and_js8_adjacent_tools() -> None:
    assert guided_setup_selected_apps_from_flags(control_backend=CONTROL_JS8CALL) == ("js8call",)
    assert guided_setup_selected_apps_from_flags(use_js8spotter=True) == ("js8call", "js8spotter")
    assert guided_setup_selected_apps_from_flags(use_commstat=True) == ("js8call", "commstat")
    assert guided_setup_selected_apps_from_flags(use_varac=True) == ("varac",)
    assert infer_guided_setup_lane((), varac_selected=True) == LANE_VARAC
    assert infer_guided_setup_lane((), receive_only=True) == "sdr_observer"
    assert guided_setup_app_label("js8spotter") == "FIO Spotter"


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


def test_guided_setup_field_visibility_blank_state_hides_connection_fields() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8call",
        hamlib_short_name="TS-2000",
        setup_mode=SETUP_MODE_READ_ONLY,
        control_route=CONTROL_JS8CALL,
    )

    visibility = guided_setup_field_visibility(
        blueprint,
        GuidedSetupFieldVisibilityInput(
            setup_type_choice="",
            backend=CONTROL_JS8CALL,
            use_js8call=True,
        ),
    )

    assert visibility.setup_started is False
    assert visibility.connection_group is False
    assert visibility.configure_automatically is False
    assert visibility.software_choices is False
    assert visibility.js8_fields is False
    assert visibility.flrig_fields is False
    assert visibility.technical_identity_fields is False


def test_guided_setup_field_visibility_js8_only_shows_only_js8_connection_fields() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8call",
        hamlib_short_name="TS-2000",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_JS8CALL,
        include_spotter=True,
        include_commstat=True,
    )

    visibility = guided_setup_field_visibility(
        blueprint,
        GuidedSetupFieldVisibilityInput(
            setup_type_choice=LANE_JS8_ONLY,
            backend=CONTROL_JS8CALL,
            use_js8call=True,
            use_fldigi=True,
            use_flmsg=True,
            use_flamp=True,
        ),
    )

    assert visibility.connection_group is True
    assert visibility.js8_fields is True
    assert visibility.flrig_fields is False
    assert visibility.fldigi_fields is False
    assert visibility.flmsg_fields is False
    assert visibility.flamp_fields is False
    assert visibility.varac_fields is False
    assert visibility.configure_automatically is True


def test_generated_radio_label_uses_hamlib_short_name_and_increments() -> None:
    assert generated_radio_label("TS-2000", []) == "TS-2000"
    assert generated_radio_label("TS-2000", ["TS-2000"]) == "TS-2000 1"
    assert generated_radio_label("TS-2000", ["TS-2000", "TS-2000 1"]) == "TS-2000 2"
    assert generated_radio_label("", ["Radio"]) == "Radio 1"


def test_guided_radio_label_base_prefers_model_short_name() -> None:
    assert (
        guided_radio_label_base(
            {
                "manufacturer": "Kenwood",
                "model_name": "TS-2000",
                "display_name": "Kenwood TS-2000",
            }
        )
        == "TS-2000"
    )
    assert (
        guided_radio_label_base(
            {
                "manufacturer": "Icom",
                "model_name": "",
                "display_name": "Icom IC-7300",
            }
        )
        == "IC-7300"
    )
    assert guided_radio_label_base({"display_name": "Airspy HF+"}) == "Airspy HF+"
    assert guided_radio_label_base({}) == "Radio"


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


def test_guided_setup_lane_preset_maps_setup_type_to_ui_defaults() -> None:
    js8 = guided_setup_lane_preset(LANE_JS8_ONLY)
    fast_light = guided_setup_lane_preset(LANE_FAST_LIGHT)
    tri_mode = guided_setup_lane_preset(LANE_TRI_MODE)
    varac = guided_setup_lane_preset(LANE_VARAC)
    observer = guided_setup_lane_preset(LANE_SDR_OBSERVER)

    assert js8.device_class == "tx_rx"
    assert js8.backend == CONTROL_JS8CALL
    assert js8.app_map["js8call"] is True
    assert js8.app_map["fldigi"] is False

    assert fast_light.backend == CONTROL_FLRIG
    assert fast_light.selected_apps == ("flrig", "fldigi", "flmsg", "flamp")

    assert tri_mode.backend == CONTROL_FLRIG
    assert tri_mode.selected_apps == ("flrig", "fldigi", "flmsg", "flamp", "js8call", "varac")

    assert varac.backend == "manual"
    assert varac.selected_apps == ("varac",)

    assert observer.device_class == "observer"
    assert observer.backend == "manual"
    assert observer.selected_apps == tuple()


def test_guided_setup_software_hint_is_core_generated_for_setup_surfaces() -> None:
    assert guided_setup_software_hint() == (
        "Choose a setup type above. FIO will then show only the fields needed for that radio path."
    )

    assert guided_setup_software_hint(
        setup_type_choice=LANE_TRI_MODE,
        setup_type_label="TriMode - FastLight/JS8Call/VarAC",
        selected_apps=("flrig", "fldigi", "flmsg", "flamp", "js8call", "varac"),
    ) == (
        "Setup type: TriMode - FastLight/JS8Call/VarAC. This setup uses: FLRig, FLDigi, FLMsg, FLAmp, JS8Call, VarAC."
    )

    assert guided_setup_software_hint(
        setup_type_choice="custom",
        setup_type_label="Custom software mix",
        selected_apps=("js8call", "varac"),
    ) == "Setup type: Custom software mix. Custom software mix is selected. Choose only the applications that belong to this radio."


def test_guided_setup_role_hint_is_core_generated_for_setup_surfaces() -> None:
    js8 = build_guided_setup_blueprint(
        lane=LANE_JS8_ONLY,
        hamlib_short_name="IC-7300",
        control_route=CONTROL_JS8CALL,
    )
    varac = build_guided_setup_blueprint(
        lane=LANE_VARAC,
        hamlib_short_name="FT-891",
        control_route=CONTROL_FLRIG,
    )
    observer = build_guided_setup_blueprint(
        lane=LANE_SDR_OBSERVER,
        hamlib_short_name="SDRplay",
        control_route=CONTROL_NONE,
    )

    assert guided_setup_role_hint(
        js8,
        GuidedSetupFieldVisibilityInput(
            setup_type_choice=LANE_JS8_ONLY,
            backend=CONTROL_JS8CALL,
            use_js8call=True,
        ),
    ).startswith("JS8Call is the frequency-control route")
    assert "VarAC owns frequency scheduling" in guided_setup_role_hint(
        varac,
        GuidedSetupFieldVisibilityInput(
            setup_type_choice=LANE_VARAC,
            backend="manual",
            use_varac=True,
        ),
    )
    assert "Observer radios track or monitor RF activity" in guided_setup_role_hint(
        observer,
        GuidedSetupFieldVisibilityInput(
            setup_type_choice=LANE_SDR_OBSERVER,
            device_class="observer",
        ),
    )


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


def test_blueprint_bridge_js8_only_flrig_route_does_not_enable_fast_light(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_JS8CALL,
        js8call_uses_flrig=True,
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
    assert all(action.app_id not in {"flrig", "fldigi", "flmsg", "flamp"} for action in plan.actions)
    js8_action = write_actions[0]
    assert js8_action.details["control_route"] == CONTROL_FLRIG
    assert "with FLRig" in js8_action.summary


def test_blueprint_bridge_keeps_varac_only_plan_read_import_only(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_FLRIG,
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
            "varac": "/apps/VarAC",
        },
    )

    assert plan.backup_required is False
    assert [action.app_id for action in plan.actions] == ["varac"]
    assert [action.action_type for action in plan.actions] == ["remember_integration"]
    assert all(not action.writes_external_config for action in plan.actions)
    assert any("read/import only" in item for item in plan.review_items)


def test_blueprint_bridge_keeps_varac_read_import_action_in_trimode_managed_plan(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="tri_mode",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_FLRIG,
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
    assert preview.guided_path == "Guided path: Station Use -> Frequency Control -> JS8Call Instance -> Schedule."
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
            "use_scheduler": True,
            "guided_frequency_plan_id": 12,
            "guided_open_plan_manager_after_save": True,
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
    assert normalized["use_scheduler"] is False
    assert normalized["scheduler_enabled"] is False
    assert "guided_frequency_plan_id" not in normalized
    assert "guided_open_plan_manager_after_save" not in normalized
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


def test_guided_payload_normalization_preserves_reviewed_save_fields() -> None:
    reviewed_values = {
        "name": "TriMode Radio",
        "control_backend": "flrig",
        "use_flrig": True,
        "use_fldigi": True,
        "use_flmsg": True,
        "use_flamp": True,
        "use_js8call": True,
        "use_js8spotter": True,
        "use_commstat": True,
        "use_varac": True,
        "launch_enabled": True,
        "launch_path": "/apps/start-station.sh",
        "sdr_host": "127.0.0.1",
        "sdr_port": "7355",
        "ptt_group": "hf-a",
        "antenna_group": "wire-1",
        "antenna_supported_bands": ["20M", "40M"],
        "antenna_band_guard_mode": "block",
        "band_overlap_guard_group": "shared-wire",
        "band_overlap_guard_mode": "warn",
        "advanced_frequency_guard_group": "front-end",
        "advanced_frequency_guard_mode": "block",
        "advanced_frequency_guard_window_hz": 2500,
        "frontend_group": "sdr-front-end",
        "amplifier_group": "amp-1",
        "varac_outbox_dir": "/VarAC/Outbox",
        "varac_bbs_dir": "/VarAC/BBS",
        "varac_bbs_archive_dir": "/VarAC/BBS/Archive",
        "notes": "Operator verified.",
    }

    normalized = normalize_guided_radio_profile_payload(reviewed_values)

    for key, value in reviewed_values.items():
        assert normalized[key] == value


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


def test_guided_setup_includes_app_instance_step_for_js8call() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="IC-7300",
    )
    app_step = next(step for step in blueprint.steps if step.step_id == "app_instance")

    assert app_step.title == "JS8Call Instance"
    assert app_step.prompt == "Which JS8Call belongs to IC-7300?"
    assert [choice.choice_id for choice in app_step.choices] == [
        APP_INSTANCE_EXISTING,
        APP_INSTANCE_MANAGED,
        APP_INSTANCE_MANUAL,
    ]


def test_guided_setup_includes_read_only_varac_app_instance_step() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="IC-705",
    )
    app_step = next(step for step in blueprint.steps if step.step_id == "app_instance")

    assert app_step.title == "VarAC Setup"
    assert app_step.prompt == "Which VarAC setup belongs to IC-705?"
    assert [choice.choice_id for choice in app_step.choices] == [APP_INSTANCE_EXISTING, APP_INSTANCE_MANUAL]
    assert "VarAC keeps frequency scheduling" in app_step.hint


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


def test_guided_setup_flow_summary_is_human_readable_for_js8_only(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="TS-2000",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_JS8CALL,
    )
    plan = build_app_config_plan_for_blueprint(
        blueprint,
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=tmp_path / "fio-config",
        app_paths={"js8call": "/apps/js8call"},
    )

    items = guided_setup_flow_items(blueprint, plan)
    lines = guided_setup_flow_summary_lines(blueprint, plan)

    assert [item.title for item in items] == ["Radio", "Software", "Connection", "Schedule", "Review"]
    assert items[0].detail == "TS-2000"
    assert items[1].detail == "JS8Call"
    assert "JS8Call owns the radio/CAT route" in items[2].detail
    assert items[3].status == "needs_input"
    assert "Frequency Plan" in items[3].detail
    assert items[4].status == "backup"
    assert lines[0].startswith("1. Radio: TS-2000")
    assert "4. Schedule:" in lines[3]
    assert "(Needs Input)" in lines[3]
    assert "5. Review:" in lines[-1]


def test_guided_station_use_choices_prioritize_trimode_then_custom() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="tri_mode",
        hamlib_short_name="TS-2000",
        setup_mode=SETUP_MODE_MANAGED,
    )

    station_step = next(step for step in blueprint.steps if step.step_id == "station_use")

    assert [choice.label for choice in station_step.choices[:3]] == [
        "TriMode - FastLight/JS8Call/VarAC",
        "Custom software mix",
        "JS8Call",
    ]


def test_guided_setup_operator_guidance_is_clear_for_js8_only(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="js8_only",
        hamlib_short_name="TS-2000",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_JS8CALL,
        include_spotter=True,
        include_commstat=True,
    )
    plan = build_app_config_plan_for_blueprint(
        blueprint,
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=tmp_path / "fio-config",
        app_paths={"js8call": "/apps/js8call"},
    )

    lines = guided_setup_operator_guidance_lines(blueprint, plan)

    assert any("JS8Call, FIO Spotter, CommStat" in line for line in lines)
    assert any("hides FLDigi, FLMsg, and FLAmp fields" in line for line in lines)
    assert any("JS8Call profile, API port, and message files" in line for line in lines)
    assert any("RF Guard" in line for line in lines)
    assert any("backup" in line.lower() for line in lines)


def test_guided_setup_flow_summary_keeps_varac_monitor_only(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_FLRIG,
    )
    plan = build_app_config_plan_for_blueprint(
        blueprint,
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=tmp_path / "fio-config",
        app_paths={"varac": "/apps/VarAC"},
    )

    items = guided_setup_flow_items(blueprint, plan)

    assert items[1].detail == "VarAC"
    assert "monitor/import only" in items[2].detail
    assert "monitor only" in items[3].detail
    assert items[4].status == "review"


def test_guided_setup_operator_guidance_keeps_varac_frequency_out_of_fio(tmp_path) -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="IC-7300",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_FLRIG,
    )
    plan = build_app_config_plan_for_blueprint(
        blueprint,
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=tmp_path / "fio-config",
        app_paths={"varac": "/apps/VarAC"},
    )

    lines = guided_setup_operator_guidance_lines(blueprint, plan)

    assert any("VarAC" in line for line in lines)
    assert any("monitor/import only" in line for line in lines)
    assert any("will not show QSY or scheduler controls" in line for line in lines)
    assert not any("RF Guard" in line for line in lines)


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
    assert payload["scheduler_enabled"] is False
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


def test_guided_setup_field_visibility_varac_only_shows_varac_without_frequency_control() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="varac",
        hamlib_short_name="IC-705",
        setup_mode=SETUP_MODE_MANAGED,
        control_route=CONTROL_FLRIG,
    )

    visibility = guided_setup_field_visibility(
        blueprint,
        GuidedSetupFieldVisibilityInput(
            setup_type_choice=LANE_VARAC,
            backend=CONTROL_FLRIG,
            use_flrig=True,
            use_varac=True,
        ),
    )

    assert visibility.connection_group is True
    assert visibility.varac_fields is True
    assert visibility.flrig_fields is False
    assert visibility.rigctld_fields is False
    assert visibility.js8_fields is False
    assert visibility.configure_automatically is True
    assert visibility.software_choices is False


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


def test_guided_setup_field_visibility_observer_shows_only_observer_fields() -> None:
    blueprint = build_guided_setup_blueprint(
        lane="sdr_observer",
        hamlib_short_name="Airspy HF",
        receive_only=True,
        control_route=CONTROL_NONE,
    )

    visibility = guided_setup_field_visibility(
        blueprint,
        GuidedSetupFieldVisibilityInput(
            setup_type_choice="sdr_observer",
            backend=CONTROL_NONE,
            device_class="observer",
            use_js8call=True,
            use_varac=True,
        ),
    )

    assert visibility.observer_mode is True
    assert visibility.connection_group is True
    assert visibility.observer_fields is True
    assert visibility.js8_fields is False
    assert visibility.varac_fields is False
    assert visibility.configure_automatically is False
    assert visibility.setup_steps is False


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
    assert session.current_step.step_id == "app_instance"

    session = answer_guided_setup_step(session, "app_instance", APP_INSTANCE_EXISTING)
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


def test_guided_setup_session_rejects_skipped_steps() -> None:
    blueprint = build_guided_setup_blueprint(lane="js8_only", hamlib_short_name="IC-7300")
    session = start_guided_setup_session(blueprint)

    try:
        answer_guided_setup_step(session, "app_instance", APP_INSTANCE_EXISTING)
    except ValueError as exc:
        assert "cannot be answered before current step 'station_use'" in str(exc)
    else:
        raise AssertionError("guided setup should not allow future steps before current step")


def test_guided_setup_session_revising_earlier_answer_clears_later_answers() -> None:
    blueprint = build_guided_setup_blueprint(lane="js8_only", hamlib_short_name="IC-7300")
    session = start_guided_setup_session(blueprint)
    session = answer_guided_setup_step(session, "station_use", LANE_JS8_ONLY)
    session = answer_guided_setup_step(session, "frequency_control", CONTROL_JS8CALL)
    session = answer_guided_setup_step(session, "app_instance", APP_INSTANCE_EXISTING)

    revised = answer_guided_setup_step(session, "frequency_control", CONTROL_NONE)

    assert revised.answer_map == {
        "station_use": LANE_JS8_ONLY,
        "frequency_control": CONTROL_NONE,
    }
    assert revised.current_step is not None
    assert revised.current_step.step_id == "app_instance"


def test_guided_setup_next_action_comes_from_flow_status() -> None:
    blueprint = build_guided_setup_blueprint(
        lane=LANE_JS8_ONLY,
        hamlib_short_name="IC-7300",
        control_route=CONTROL_JS8CALL,
        setup_mode=SETUP_MODE_MANAGED,
    )
    plan = build_guided_external_app_config_plan(
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=Path("/tmp/fio-test"),
        allow_external_writes=True,
    )

    next_item = guided_setup_next_flow_item(blueprint, plan)

    assert next_item.item_id in {"connection", "schedule", "review"}
    assert guided_setup_next_action_text(blueprint, plan).startswith(f"Next: {next_item.title} - ")


def test_guided_setup_next_action_uses_selected_schedule_decision() -> None:
    blueprint = build_guided_setup_blueprint(
        lane=LANE_JS8_ONLY,
        hamlib_short_name="IC-7300",
        control_route=CONTROL_JS8CALL,
        setup_mode=SETUP_MODE_MANAGED,
    )
    plan = build_guided_external_app_config_plan(
        build_lab_radio_proposals(radio_count=1, busy_checker=lambda _host, _port: False),
        config_root=Path("/tmp/fio-test"),
        allow_external_writes=True,
    )
    decision = guided_setup_schedule_decision(
        scheduler_assignment_allowed=True,
        selected_plan_name="JS8 Standard",
        plan_count=1,
        open_plan_manager=False,
    )

    items = guided_setup_flow_items(blueprint, plan, schedule_decision=decision)
    schedule_item = next(item for item in items if item.item_id == "schedule")

    assert schedule_item.status == "ready"
    assert schedule_item.detail == "Selected plan: JS8 Standard. FIO assigns it after save with RF Guard."
    assert guided_setup_next_action_text(blueprint, plan, schedule_decision=decision).startswith("Next: Review - ")


def test_guided_setup_wizard_view_returns_ui_ready_navigation_state() -> None:
    from freqinout.core.guided_setup import guided_setup_wizard_view

    radio = guided_setup_wizard_view("radio")
    assert radio.current_index == 0
    assert radio.previous_label == ""
    assert radio.next_label == "Software"
    assert radio.can_go_back is False
    assert radio.can_go_next is True
    assert radio.visible_sections == ("radio",)

    schedule = guided_setup_wizard_view("schedule")
    assert schedule.previous_label == "Connection"
    assert schedule.next_label == "Review"
    assert schedule.visible_sections == ("schedule",)
    assert "RF Guard" in schedule.detail

    review = guided_setup_wizard_view("review")
    assert review.can_go_next is False
    assert review.visible_sections == ("review", "launch", "optional")


def test_guided_setup_wizard_view_handles_hidden_connection_step() -> None:
    from freqinout.core.guided_setup import guided_setup_wizard_view

    view = guided_setup_wizard_view("connection", connection_visible=False)

    assert view.current_step_id == "schedule"
    assert view.visible_sections == ("schedule",)
    assert view.previous_label == "Software"
    assert view.next_label == "Review"

    software = guided_setup_wizard_view("software", connection_visible=False)
    assert software.next_label == "Schedule"
    assert [step_id for step_id, _label in software.steps] == ["radio", "software", "schedule", "review"]


def test_guided_setup_schedule_decision_is_single_source_for_wizard_copy() -> None:
    selected = guided_setup_schedule_decision(
        scheduler_assignment_allowed=True,
        selected_plan_name="Magnet Main",
        plan_count=3,
        open_plan_manager=False,
    )
    assert selected.status == "ready"
    assert selected.step_detail == "Selected plan: Magnet Main. FIO assigns it after save with RF Guard."
    assert selected.status_text == "After saving, FIO will assign 'Magnet Main' to this radio with RF Guard."
    assert selected.review_text == "Assign 'Magnet Main' after save with RF Guard."

    handoff = guided_setup_schedule_decision(
        scheduler_assignment_allowed=True,
        selected_plan_name="",
        plan_count=0,
        open_plan_manager=True,
    )
    assert handoff.status == "needs_input"
    assert "Plan Manager will open" in handoff.step_detail
    assert handoff.review_text == "Open Plan Manager after save so a Frequency Plan can be built and assigned."

    no_nets = guided_setup_schedule_decision(
        scheduler_assignment_allowed=True,
        selected_plan_name="Ignored hidden combo",
        plan_count=3,
        open_plan_manager=False,
        selected_schedule_choice="daily_no_nets",
    )
    assert no_nets.status == "needs_input"
    assert no_nets.step_detail == "Plan Manager will open after save to build or choose a Daily with No Nets plan."
    assert no_nets.review_text == "Open Plan Manager after save for Daily with No Nets."

    no_schedule = guided_setup_schedule_decision(
        scheduler_assignment_allowed=True,
        selected_plan_name="Ignored hidden combo",
        plan_count=3,
        open_plan_manager=False,
        selected_schedule_choice=SCHEDULE_NONE,
    )
    assert no_schedule.status == "ready"
    assert no_schedule.review_text == "No Frequency Plan assigned during setup."

    monitor_only = guided_setup_schedule_decision(
        scheduler_assignment_allowed=False,
        selected_plan_name="Ignored",
        plan_count=2,
        open_plan_manager=True,
    )
    assert monitor_only.status == "ready"
    assert "monitor/import" in monitor_only.status_text
    assert monitor_only.review_text == "No FIO-controlled schedule or QSY controls will be saved for this radio."


def test_guided_setup_autofill_review_is_bounded_and_operator_readable() -> None:
    review = guided_setup_autofill_review(
        filled=("FLRig port", "JS8Call port", "MCF forms", "VarAC DB", "VarAC outbox"),
        preserved=("JS8Call host", "Radio name"),
        detection_notes=("Detected JS8Call profile FIO-A.", "External JS8Spotter not required for FIO Spotter."),
        radio_apps_base_used=True,
        visible_limit=4,
    )

    assert review.status_text == "Configure Automatically found settings. Review before Save."
    assert review.visible_lines == (
        "Filled 5: FLRig port, JS8Call port, MCF forms, +2 more",
        "Kept existing: 2 field(s) unchanged.",
        "Used Radio Apps Base Folder.",
        "Detected JS8Call profile FIO-A.",
        "1 more detail item(s) available.",
    )
    assert "Filled: FLRig port, JS8Call port, MCF forms, VarAC DB, VarAC outbox" in review.detail_lines
    assert "Kept existing: JS8Call host, Radio name" in review.detail_lines
    assert "External JS8Spotter not required for FIO Spotter." in review.detail_lines


def test_settings_guided_add_radio_uses_setup_type_selector_as_ui_shell() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    dialog_block = source[
        source.index("def _open_device_profile_dialog")
        : source.index("def _apply_runtime_projection_widgets")
    ]

    assert 'setup_type_combo.setObjectName("guidedSetupType")' in dialog_block
    assert "dlg.setSizeGripEnabled(True)" in dialog_block
    assert "dlg.setMinimumSize(640, 520)" in dialog_block
    assert "scroll = QScrollArea(dlg)" in dialog_block
    assert "scroll.setWidgetResizable(True)" in dialog_block
    assert "setFixedWidth" not in dialog_block
    assert "setRowWrapPolicy(QFormLayout.WrapLongRows)" in dialog_block
    assert "AdjustToMinimumContentsLengthWithIcon" in dialog_block
    assert "combo.setMinimumWidth" not in dialog_block
    assert "combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)" in dialog_block
    assert dialog_block.index('setup_type_combo.addItem("TriMode - FastLight/JS8Call/VarAC", LANE_TRI_MODE)') < dialog_block.index(
        'setup_type_combo.addItem("Custom software mix", "custom")'
    )
    assert dialog_block.index('setup_type_combo.addItem("Custom software mix", "custom")') < dialog_block.index(
        'setup_type_combo.addItem("JS8Call only", LANE_JS8_ONLY)'
    )
    assert 'setup_type_combo.addItem("JS8Call only", LANE_JS8_ONLY)' in dialog_block
    assert 'setup_type_combo.addItem("VarAC only", LANE_VARAC)' in dialog_block
    assert 'setup_type_combo.addItem("Receive-only SDR", LANE_SDR_OBSERVER)' in dialog_block
    assert 'setup_type_combo.addItem("Custom software mix", "custom")' in dialog_block
    assert "def _apply_setup_type_choice()" in dialog_block
    assert "def _current_guided_blueprint()" in dialog_block
    assert "guided_setup_field_visibility(" in dialog_block
    assert "GuidedSetupFieldVisibilityInput(" in dialog_block
    assert "_set_row_visible(configure_auto_wrap, visibility.configure_automatically)" in dialog_block
    assert 'radio_apps_base_edit.setObjectName("guidedRadioAppsBaseFolder")' in dialog_block
    assert '"Radio Apps Base Folder:"' in dialog_block
    assert "_set_row_visible(radio_apps_base_wrap, visibility.configure_automatically)" in dialog_block
    assert "def _persist_radio_apps_base_folder() -> None:" in dialog_block
    assert "app_search_paths_with_radio_apps_base(" in dialog_block
    assert "guided_setup_autofill_review(" in dialog_block
    assert "Used Radio Apps Base Folder for app detection." in Path("freqinout/core/guided_setup.py").read_text(
        encoding="utf-8"
    )
    assert "guided_setup_software_hint(" in dialog_block
    assert "preset = guided_setup_lane_preset(lane)" in dialog_block
    assert "_set_combo_data(device_class_combo, preset.device_class)" in dialog_block
    assert "_set_combo_data(backend_combo, preset.backend)" in dialog_block
    assert "selected_apps = preset.app_map" in dialog_block
    assert '_checkbox_set_checked(use_fldigi_chk, selected_apps.get("fldigi", False))' in dialog_block
    assert '_checkbox_set_checked(use_varac_chk, selected_apps.get("varac", False))' in dialog_block
    assert '_set_row_visible(software_wrap, visibility.software_choices and setup_type_choice == "custom")' in dialog_block
    assert "_set_row_visible(widget, visibility.fldigi_fields)" in dialog_block
    assert "_set_row_visible(widget, visibility.flmsg_fields)" in dialog_block
    assert "_set_row_visible(widget, visibility.flamp_fields)" in dialog_block
    assert "_set_row_visible(widget, visibility.varac_fields)" in dialog_block
    assert "def _existing_radio_labels_for_name_generation()" in dialog_block
    assert "generated_radio_label(" in dialog_block
    assert "guided_radio_label_base(selected)" in dialog_block
    assert "display_name, _existing_radio_labels_for_name_generation()" not in dialog_block
    assert "guided_setup_selected_apps_from_flags(" in dialog_block
    assert "use_flmsg=use_flmsg_chk.isChecked()" in dialog_block
    assert "use_flamp=use_flamp_chk.isChecked()" in dialog_block
    assert "Hidden app sections are not part of this setup type unless you select Custom software mix." not in dialog_block
    assert "Hidden sections stay unchanged" not in dialog_block
    assert "use_js8spotter=use_js8spotter_chk.isChecked()" in dialog_block
    assert "use_commstat=use_commstat_chk.isChecked()" in dialog_block
    assert "include_spotter=use_js8spotter_chk.isChecked()" in dialog_block
    assert "include_commstat=use_commstat_chk.isChecked()" in dialog_block
    assert "lane = _current_guided_lane()" in dialog_block
    assert "schedule_decision = _current_guided_schedule_decision()" in dialog_block
    assert "guided_setup_flow_summary_lines(blueprint, plan, schedule_decision=schedule_decision)" in dialog_block
    assert "guided_setup_flow_items(blueprint, plan, schedule_decision=schedule_decision)" in dialog_block
    assert "guided_setup_next_action_text(blueprint, plan, schedule_decision=schedule_decision)" in dialog_block
    assert 'guided_wizard_group = QGroupBox("Guided Setup")' in dialog_block
    assert 'guided_wizard_group.setObjectName("guidedSetupWizard")' in dialog_block
    assert 'btn.setObjectName(f"guidedWizardStep_{step_id}")' in dialog_block
    assert 'guided_wizard_back_btn.setObjectName("guidedWizardBack")' in dialog_block
    assert 'guided_wizard_next_btn.setObjectName("guidedWizardNext")' in dialog_block
    assert 'schedule_group, schedule_form = _make_section(' in dialog_block
    assert 'connection_status_label.setObjectName("guidedConnectionStatus")' in dialog_block
    assert "No FIO frequency-control endpoint is required for this setup type." in dialog_block
    assert '"Schedule Assignment"' in dialog_block
    assert 'schedule_status_label.setObjectName("guidedScheduleAssignmentStatus")' in dialog_block
    assert 'schedule_path_combo.setObjectName("guidedSchedulePathCombo")' in dialog_block
    assert '"Schedule Path:"' in dialog_block
    assert 'schedule_plan_combo.setObjectName("guidedSchedulePlanCombo")' in dialog_block
    assert 'schedule_plan_combo.addItem("Assign later", 0)' in dialog_block
    assert 'schedule_open_plan_manager_chk.setObjectName("guidedScheduleOpenPlanManager")' in dialog_block
    assert "def _selected_guided_schedule_path() -> str:" in dialog_block
    assert "def _refresh_guided_schedule_path_combo() -> None:" in dialog_block
    assert "selected_schedule_choice=_selected_guided_schedule_path()" in dialog_block
    assert "schedule_choice == SCHEDULE_EXISTING_PLAN" in dialog_block
    assert "guided_setup_schedule_decision(" in dialog_block
    assert "Plan Manager will open after save" in Path("freqinout/core/guided_setup.py").read_text(encoding="utf-8")
    assert "guided_initial_frequency_plan_id" in dialog_block
    assert "get_effective_assigned_plan_for_device" in dialog_block
    assert "self.multi_radio_store.list_frequency_plans()" in dialog_block
    assert "SOURCE_ONLY_FREQUENCY_PLAN_CATEGORIES" in dialog_block
    assert 'save_review_group, save_review_form = _make_section(' in dialog_block
    assert '"Save Review"' in dialog_block
    assert 'save_review_label.setObjectName("guidedSaveReview")' in dialog_block
    assert "def _update_guided_save_review() -> None:" in dialog_block
    assert "VarAC monitor/import: FIO will not control VarAC frequency" in dialog_block
    assert "FIO Spotter + external JS8Spotter" in dialog_block
    assert "app_labels.append(\"FIO Spotter\")" in dialog_block
    assert '"External JS8Spotter app"' in dialog_block
    assert '"js8spotter": "External JS8Spotter"' in dialog_block
    launch_allowed_block = source[
        source.index("def _launch_item_allowed_for_profile")
        : source.index("def _is_launch_item_configured")
    ]
    assert 'if app_name == "JS8Spotter":' in launch_allowed_block
    assert 'profile.get("spotter_launch_path", "")' in launch_allowed_block
    assert 'self._radio_software_enabled(profile, software_key)' in launch_allowed_block
    assert '"What FIO will save:"' in dialog_block
    assert "Use in FIO: {enabled_text}; {active_text}" in dialog_block
    assert "Frequency Control: {frequency_line}" in dialog_block
    assert "Message/Forms Files: " in dialog_block
    assert "Monitor/import only. FIO will not offer scheduler or QSY controls." in dialog_block
    assert "Manual/external control. FIO will not tune this radio until a control endpoint is selected." in dialog_block
    assert "controls scheduler and QSY actions." in dialog_block
    assert "VarAC monitor/import: FIO will not control VarAC frequency" in dialog_block
    assert "Radio Apps Base Folder" in dialog_block
    assert '_path_summary("VarAC outbox", varac_outbox_edit.text())' in dialog_block
    assert '_path_summary("VarAC BBS", varac_bbs_edit.text())' in dialog_block
    assert '_path_summary("VarAC BBS archive", varac_bbs_archive_edit.text())' in dialog_block
    assert "payload = _draft_radio_profile()" in dialog_block
    assert 'payload["name"] = name' in dialog_block
    assert "out.update(normalize_guided_radio_profile_payload(payload))" in dialog_block
    assert "Assign '{selected}' after save with RF Guard." in Path("freqinout/core/guided_setup.py").read_text(
        encoding="utf-8"
    )
    assert "out[\"guided_frequency_plan_id\"] = guided_plan_id" in dialog_block
    assert "out[\"guided_open_plan_manager_after_save\"] = True" in dialog_block
    assert "def _apply_guided_wizard_visibility(connection_visible: bool) -> None:" in dialog_block
    assert "def _guided_wizard_step_label(index: int) -> str:" in dialog_block
    assert "def _guided_connection_step_visible() -> bool:" in dialog_block
    assert "def _guided_visible_wizard_steps() -> Tuple[Tuple[str, str], ...]:" in dialog_block
    assert 'guided_wizard_next_btn.setText(f"Next: {next_label}" if next_label else "Next")' in dialog_block
    assert 'guided_wizard_back_btn.setText(f"Back: {previous_label}" if previous_label else "Back")' in dialog_block
    assert "btn.setVisible(step_id in visible_step_ids)" in dialog_block
    assert 'identity_group.setVisible(guided_wizard_step_id == "radio")' in dialog_block
    assert 'software_group.setVisible(guided_wizard_step_id == "software")' in dialog_block
    assert 'connection_group.setVisible(guided_wizard_step_id == "connection")' in dialog_block
    assert 'schedule_group.setVisible(guided_wizard_step_id == "schedule")' in dialog_block
    assert 'save_review_group.setVisible(guided_wizard_step_id == "review")' in dialog_block
    assert 'launch_group.setVisible(guided_wizard_step_id == "review")' in dialog_block
    assert 'guided_next_action_label.setObjectName("guidedSetupNextAction")' in dialog_block
    assert 'guided_step_widgets: Dict[str, Tuple[QFrame, QLabel, QLabel, QLabel]] = {}' in dialog_block
    assert 'step_frame.setObjectName(f"guidedSetupStep_{step_id}")' in dialog_block
    assert 'status_label.setObjectName(f"guidedSetupStepStatus_{step_id}")' in dialog_block
    assert "guided_wizard_layout.addWidget(app_setup_plan_group)" in dialog_block
    assert "_add_full_width_row(software_form, app_setup_plan_group)" not in dialog_block
    assert 'if guided_wizard_step_id != "review":' in dialog_block
    assert '_set_guided_wizard_step("connection")' in dialog_block
    assert "guided_setup_operator_guidance_lines(blueprint, plan)" not in dialog_block
    assert "build_guided_setup_preview(blueprint, plan)" not in dialog_block


def test_settings_preferences_expose_radio_apps_base_folder() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert 'self.radio_apps_base_folder_edit.setObjectName("radioAppsBaseFolder")' in source
    assert '"Radio Apps Base Folder:"' in source
    assert "self.radio_apps_base_folder_browse_btn.clicked.connect(self._choose_radio_apps_base_folder)" in source
    assert 'data["radio_apps_base_folder"] = (' in source
    assert 'self.radio_apps_base_folder_edit.setText(str(data.get("radio_apps_base_folder", "") or "").strip())' in source
    assert 'QFileDialog.getExistingDirectory(self, "Select Radio Apps Base Folder", start)' in source


def test_guided_add_radio_reviewed_fields_are_in_single_draft_save_payload() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    dialog_block = source.split("    def _open_device_profile_dialog", 1)[1].split(
        "    def _apply_runtime_projection_widgets", 1
    )[0]
    draft_block = dialog_block.split("def _draft_radio_profile() -> Dict[str, Any]:", 1)[1].split(
        "def _default_instance_port", 1
    )[0]
    save_block = dialog_block.split("def _save() -> None:", 1)[1].split("buttons.accepted.connect", 1)[0]

    reviewed_keys = (
        "advanced_frequency_guard_group",
        "advanced_frequency_guard_mode",
        "advanced_frequency_guard_window_hz",
        "antenna_supported_bands",
        "launch_enabled",
        "launch_path",
        "sdr_host",
        "sdr_port",
        "varac_outbox_dir",
        "varac_bbs_dir",
        "varac_bbs_archive_dir",
    )
    for key in reviewed_keys:
        assert f'"{key}"' in draft_block

    assert "payload = _draft_radio_profile()" in save_block
    assert "out.update(normalize_guided_radio_profile_payload(payload))" in save_block
    for key in reviewed_keys:
        assert f'"{key}"' not in save_block
    assert "app_setup_plan_label.setVisible(False)" in dialog_block
    assert '"spotter_launch_path": (js8spotter_launch_edit, "External JS8Spotter app")' in dialog_block
    assert 'QGroupBox("Setup Steps")' in dialog_block
    assert dialog_block.index('setup_type_combo.addItem("TriMode - FastLight/JS8Call/VarAC", LANE_TRI_MODE)') < dialog_block.index(
        'setup_type_combo.addItem("Custom software mix", "custom")'
    )
    assert dialog_block.index('setup_type_combo.addItem("Custom software mix", "custom")') < dialog_block.index(
        'setup_type_combo.addItem("JS8Call only", LANE_JS8_ONLY)'
    )


def test_guided_add_radio_assigns_selected_plan_after_profile_save() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    planner_source = Path("freqinout/gui/freq_planner_tab.py").read_text(encoding="utf-8")
    assert "def _assign_guided_frequency_plan_after_profile_save(" in source
    assert "def _open_plan_manager_after_guided_profile_save(" in source
    assert 'screen_map.get("FreqPlanner", -1)' in source
    assert "begin_guided_radio_plan_handoff" in source
    assert "handoff(device_profile or {}, schedule_choice=schedule_choice)" in source
    assert "def begin_guided_radio_plan_handoff(" in planner_source
    assert "schedule_choice: str = \"\"" in planner_source
    assert "SCHEDULE_DAILY_NO_NETS" in planner_source
    assert "SCHEDULE_DAILY_PLUS_NETS" in planner_source
    assert "SCHEDULE_JS8_STANDARD" in planner_source
    assert "SCHEDULE_SOP_CONDITION" in planner_source
    assert "schedule_choice in {SCHEDULE_JS8_STANDARD, SCHEDULE_DAILY_NO_NETS}" in planner_source
    assert "NO_NET_SOURCE_SET_ID" in planner_source
    assert "SELECTED_HF_NET_SOURCE_SET_KEY" in planner_source
    assert "self._guided_plan_handoff_device_profile_id" in planner_source
    assert "device_profile_id=int(getattr(self, \"_guided_plan_handoff_device_profile_id\"" in planner_source
    assert "radio_name = str((device_profile or {}).get(\"name\")" in planner_source
    assert "Name the Frequency Plan for {radio_name}" in planner_source
    assert "saved without a Frequency Plan" in planner_source
    assert "Build its first Frequency Plan here" in planner_source
    assert "Build a Daily with No Nets plan" in planner_source
    assert "Build a Daily + Nets plan" in planner_source
    assert "Build an SOP condition plan" in planner_source
    assert "self.multi_radio_store.set_assigned_plan(" in source
    assert 'reason="Initial Frequency Plan selected during guided radio setup."' in source
    assert "guided_plan_id = int(created.pop(\"guided_frequency_plan_id\", 0) or 0)" in source
    assert "guided_plan_id = int(updated.pop(\"guided_frequency_plan_id\", 0) or 0)" in source
    assert "created.pop(\"guided_open_plan_manager_after_save\", False)" in source
    assert "updated.pop(\"guided_open_plan_manager_after_save\", False)" in source
    assert "created.pop(\"guided_schedule_choice\", \"\")" in source
    assert "updated.pop(\"guided_schedule_choice\", \"\")" in source
    assert "self._last_persisted_device_profile = dict(saved)" in source
    assert '"RF Guard Blocked Schedule Assignment"' in source
