from __future__ import annotations

import pytest

from freqinout.core.shared_state import (
    ActionFeedbackService,
    AssignedPlanService,
    BusyEvidence,
    FormPurposeMapping,
    FormPurposeRouter,
    Frequency,
    FrequencyPlanStore,
    MapEvent,
    MapLayerService,
    MessageSourceIndex,
    MessageSourceRecord,
    PlanContextService,
    PttConflictEvidence,
    RadioProfileStore,
    RuntimePolicy,
    RuntimePolicyStore,
    RuntimeTransientState,
    RuntimeTransientStateStore,
    SchedulerManualControlState,
    SchedulerManualTarget,
    RuntimeSelectionService,
    SchedulerState,
    SchedulerStateService,
    SchedulerTarget,
    SelectionWriteError,
    StationHealthIssue,
    StationHealthService,
)


def test_frequency_plan_can_be_created_saved_and_copied() -> None:
    store = FrequencyPlanStore()
    freq = Frequency(id="freq_1", frequency="14.300")

    plan = store.create("Home HF Daily", category="normal")
    plan = type(plan)(
        id=plan.id,
        name=plan.name,
        frequencies=(freq,),
        schedule_source_ids=("source_1",),
    )
    saved = store.save(plan)
    copied = store.copy(saved.id, name="Portable Event Plan")

    assert saved.saved is True
    assert saved.draft is False
    assert saved.status == "saved"
    assert saved.created_at_utc == saved.created_utc
    assert saved.updated_at_utc == saved.updated_utc
    assert store.get(saved.id).name == "Home HF Daily"
    assert copied.id != saved.id
    assert copied.name == "Portable Event Plan"
    assert copied.saved is False
    assert copied.draft is True
    assert copied.status == "draft"
    assert saved.frequency_refs == ("freq_1",)
    assert copied.frequency_refs == ("freq_1",)
    assert saved.source_refs == ("source_1",)


def test_canonical_frequency_fields_coexist_with_legacy_fields() -> None:
    freq = Frequency(
        id="freq_1",
        frequency="14.300",
        frequency_hz=14300000,
        mode="USB",
        source_refs=("source_1",),
    )

    assert freq.frequency == "14.300"
    assert freq.frequency_hz == 14300000
    assert freq.source_refs == ("source_1",)


def test_assigned_plan_links_one_plan_to_multiple_radios() -> None:
    plans = FrequencyPlanStore()
    radios = RadioProfileStore()
    assignments = AssignedPlanService()

    plan = plans.save(plans.create("Storm Watch"))
    left = radios.create("IC-7300")
    right = radios.create("Flex 6400")

    left_assignment = assignments.assign(left.id, plan.id)
    right_assignment = assignments.assign(right.id, plan.id, receive_only=True)

    assert left_assignment.frequency_plan_id == plan.id
    assert right_assignment.frequency_plan_id == plan.id
    assert assignments.for_radio(left.id) == left_assignment
    assert assignments.for_radio(right.id) == right_assignment
    assert left_assignment.scheduler_mode == "full"
    assert left_assignment.is_active is True
    assert left_assignment.is_default is False


def test_assigned_plan_uses_active_temporary_then_default_and_ignores_expired_override() -> None:
    assignments = AssignedPlanService()

    default = assignments.assign("radio_1", "plan_default", default=True)
    expired = assignments.assign(
        "radio_1",
        "plan_old_temp",
        assignment_category="temporary",
        temporary_override_until_utc="2020-01-01T00:00:00Z",
    )
    temporary = assignments.assign(
        "radio_1",
        "plan_temp",
        assignment_category="temporary",
        temporary_override_until_utc="2999-01-01T00:00:00Z",
    )

    assert expired.temporary_override is True
    assert assignments.for_radio("radio_1") == temporary

    assignments.unassign_radio("radio_1")
    assignments.assign("radio_1", default.frequency_plan_id, default=True)
    assignments.assign(
        "radio_1",
        expired.frequency_plan_id,
        assignment_category="temporary",
        temporary_override_until_utc="2020-01-01T00:00:00Z",
    )

    assert assignments.for_radio("radio_1").frequency_plan_id == "plan_default"


def test_runtime_policy_restart_preserves_operator_intent() -> None:
    store = RuntimePolicyStore()
    policy = RuntimePolicy(
        radio_profile_id="radio_1",
        messages_enabled=False,
        operator_suppressed=True,
    )

    store.save(policy)
    store.clean_for_restart()
    restarted = store.get("radio_1")

    assert restarted.messages_enabled is False
    assert restarted.operator_suppressed is True
    assert restarted == policy
    assert restarted.scheduler_control == "enabled"
    assert restarted.message_view_enabled is False
    assert RuntimePolicy(radio_profile_id="radio_2").launch_control_enabled is False
    assert RuntimePolicy(radio_profile_id="radio_2", launch_enabled=True).launch_control_enabled is False
    assert RuntimePolicy(
        radio_profile_id="radio_2",
        launch_enabled=True,
        launch_control_participation=True,
    ).launch_control_enabled is True


def test_runtime_transient_state_resets_temporary_conditions_on_restart() -> None:
    store = RuntimeTransientStateStore()
    saved = store.save(
        RuntimeTransientState(
            radio_profile_id="radio_1",
            temporary_paused=True,
            manual_hold=True,
            transient_error="busy",
            latest_event_id="event_1",
        )
    )

    assert saved.has_transient_condition is True

    cleared = store.clear("radio_1")

    assert cleared.temporary_paused is False
    assert cleared.manual_hold is False
    assert cleared.transient_error == ""
    assert cleared.latest_event_id is None

    store.save(saved)

    store.clean_for_restart()
    restarted = store.get("radio_1")

    assert restarted.temporary_paused is False
    assert restarted.manual_hold is False
    assert restarted.transient_error == ""
    assert restarted.latest_event_id is None
    assert restarted.has_transient_condition is False


def test_runtime_selection_service_enforces_write_authority() -> None:
    service = RuntimeSelectionService()
    first_timestamp = service.state.updated_at_utc

    with pytest.raises(SelectionWriteError):
        service.set_settings_radio("radio_1", source="scheduler")

    service.set_settings_radio("radio_1", source="settings")
    service.set_tab_radio("controlfreq", "radio_1", source_tab_id="controlfreq")

    with pytest.raises(SelectionWriteError):
        service.set_tab_radio("controlfreq", "radio_2", source_tab_id="messages")

    with pytest.raises(SelectionWriteError):
        service.set_primary_runtime_radio("radio_1", source="scheduler")

    service.set_primary_runtime_radio("radio_1", source="settings")

    with pytest.raises(SelectionWriteError):
        service.set_active_runtime_radios(["radio_1"], source="settings")

    service.set_active_runtime_radios(["radio_1", "radio_1"], source="scheduler")

    assert service.state.settings_radio_id == "radio_1"
    assert service.state.tab_radio_ids["controlfreq"] == "radio_1"
    assert service.state.tab_radio_ids_json["controlfreq"] == "radio_1"
    assert service.state.primary_runtime_radio_id == "radio_1"
    assert service.state.active_runtime_radio_ids == ("radio_1",)
    assert service.state.updated_at_utc >= first_timestamp


def test_flrig_auto_discovery_respects_operator_suppression() -> None:
    radios = RadioProfileStore()
    policies = RuntimePolicyStore()

    active = radios.create("Active FLRig", control_backend="flrig", flrig_connected=True)
    suppressed = radios.create("Suppressed FLRig", control_backend="flrig", flrig_connected=True)
    offline = radios.create("Offline FLRig", control_backend="flrig", flrig_connected=False)
    observer = radios.create("Observer FLRig", radio_class="observer", control_backend="flrig", flrig_connected=True)
    manual = radios.create("Manual Radio", control_backend="manual", flrig_connected=True)

    policies.save(RuntimePolicy(radio_profile_id=suppressed.id, operator_suppressed=True))
    policies.save(RuntimePolicy(radio_profile_id=manual.id, explicitly_suppressed=False))

    discovered = policies.discover_active_radios(
        radios.list(),
        flrig_health={active.id: True, suppressed.id: True, offline.id: False, observer.id: True},
    )

    assert discovered == (active.id,)


def test_radio_profile_canonical_fields_and_display_name_alias() -> None:
    radios = RadioProfileStore()

    profile = radios.create(
        "Portable",
        radio_class="portable",
        deployment_mode="field",
        uses_js8call=True,
        uses_flrig=True,
    )

    assert profile.display_name == "Portable"
    assert profile.radio_class == "portable"
    assert profile.deployment_mode == "field"
    assert profile.uses_js8call is True
    assert profile.uses_flrig is True

    manual_default = radios.create("Manual Default")
    assert manual_default.control_backend == "manual"


def test_scheduler_state_is_current_truth_and_events_are_accountability_trail() -> None:
    service = SchedulerStateService()
    expected = SchedulerTarget(frequency="14.300", mode="USB", offset="0")

    state = service.set_state(
        SchedulerState(
            radio_profile_id="radio_1",
            assigned_plan_id="assignment_1",
            current_target=expected,
            state="active",
            schedule_status="on_schedule",
        )
    )
    event = service.record_event(
        radio_profile_id="radio_1",
        assigned_plan_id="assignment_1",
        expected=expected,
        attempted_action="resume_schedule",
        result="success",
        explanation="Radio resumed schedule.",
    )

    assert service.get_state("radio_1") == state
    assert service.events_for_radio("radio_1") == [event]
    assert event.result == "success"


def test_scheduler_manual_control_state_stores_manual_target_for_repeated_qsy() -> None:
    target = SchedulerManualTarget(
        frequency_hz=7268000,
        mode="LSB",
        vfo="A",
        source_action="qsy",
    )
    state = SchedulerManualControlState(
        radio_profile_id="radio_1",
        state="manual_qsy",
        manual_target=target,
        hold_until_utc="2026-07-22T21:00:00Z",
        operator_source="controlfreq",
    )

    assert state.manual_target == target
    assert state.manual_target.frequency_hz == 7268000
    assert state.state == "manual_qsy"


def test_busy_and_ptt_evidence_are_radio_scoped() -> None:
    busy = BusyEvidence(
        id="busy_1",
        radio_profile_id="radio_1",
        source_family="js8",
        reason_code="js8_tx",
        severity="soft",
        evidence_timestamp_utc="2026-07-22T20:00:00Z",
        expiration_timestamp_utc="2026-07-22T20:02:00Z",
    )
    conflict = PttConflictEvidence(
        id="ptt_1",
        ptt_group="main",
        requested_radio_id="radio_2",
        blocking_radio_id="radio_1",
        source="scheduler",
    )

    assert busy.radio_profile_id == "radio_1"
    assert busy.severity == "soft"
    assert conflict.severity == "hard"
    assert conflict.blocking_radio_id == "radio_1"


def test_station_health_summarizes_scheduler_blocker_by_radio() -> None:
    scheduler = SchedulerStateService()
    health = StationHealthService()
    event = scheduler.record_event(
        radio_profile_id="radio_1",
        assigned_plan_id="assignment_1",
        attempted_action="schedule_transition",
        blocker_state="JS8Call busy",
        explanation="IC-7300 held because JS8Call is busy.",
    )

    issue = health.summarize_scheduler_blocker(event)

    assert issue.radio_profile_id == "radio_1"
    assert issue.scheduler_event_id == event.id
    assert issue.family == "scheduler"
    assert issue.summary == "IC-7300 held because JS8Call is busy."
    assert issue.evidence_ref == event.id
    assert health.top_issue("radio_1") == issue


def test_station_health_orders_blocked_error_warning_info() -> None:
    health = StationHealthService()

    info = health.add_issue(StationHealthIssue(id="info", scope="radio", severity="info", explanation="FYI"))
    warning = health.add_issue(StationHealthIssue(id="warning", scope="radio", severity="warning", explanation="Warn"))
    blocked = health.add_issue(StationHealthIssue(id="blocked", scope="radio", severity="blocked", explanation="Blocked"))
    error = health.add_issue(StationHealthIssue(id="error", scope="radio", severity="error", explanation="Error"))

    assert health.top_issue() == blocked
    health.clear_issue(blocked.id)
    assert health.top_issue() == error
    health.clear_issue(error.id)
    assert health.top_issue() == warning
    health.clear_issue(warning.id)
    assert health.top_issue() == info


def test_message_source_unknown_radio_is_preserved_not_reassigned() -> None:
    index = MessageSourceIndex()
    record = MessageSourceRecord(
        id="msg_1",
        source_software="VarAC",
        received_utc="2026-06-02T00:00:00Z",
        source_radio_id=None,
        summary="Unknown source radio",
    )

    index.add(record)

    assert index.list(radio_ids=["radio_1"], include_unknown=True) == [record]
    assert index.list(radio_ids=["radio_1"], include_unknown=False) == []


def test_unknown_form_routes_to_archive_without_map_marker_or_alert() -> None:
    router = FormPurposeRouter()
    record = MessageSourceRecord(
        id="msg_1",
        source_software="JS8Call",
        received_utc="2026-06-02T00:00:00Z",
        form_id="F!999",
        summary="Unknown form",
    )

    routed, mapping = router.route(record)
    layers = MapLayerService().from_routed_messages([routed], {})

    assert mapping is None
    assert routed.purpose == "general/archive"
    assert layers == []


def test_map_layer_service_consumes_routed_records_without_parsing_raw_forms() -> None:
    mapping = FormPurposeMapping(
        form_id="F!504",
        purpose="weather",
        destination="map_weather",
        icon="cloud-rain",
        alert=False,
    )
    router = FormPurposeRouter([mapping])
    record = MessageSourceRecord(
        id="msg_1",
        source_software="JS8Call",
        source_radio_id="radio_1",
        received_utc="2026-06-02T00:00:00Z",
        form_id="F!504",
        summary="Rain and wind",
    )

    routed, resolved = router.route(record)
    layers = MapLayerService().from_routed_messages(
        [routed],
        {mapping.form_id.upper(): mapping},
    )

    assert resolved == mapping
    assert len(layers) == 1
    assert layers[0].purpose == "weather"
    assert layers[0].source_message_id == "msg_1"
    assert layers[0].source_radio_id == "radio_1"


def test_map_event_preserves_source_and_location_trust() -> None:
    source = MessageSourceRecord(
        id="msg_1",
        source_software="JS8Call",
        source_radio_id="radio_2",
        received_utc="2026-07-22T20:14:00Z",
        trust_state="on_air",
        summary="K7ABC grid report",
    )
    event = MapEvent(
        id=f"{source.message_id}:K7ABC:station:2026-07-22T20",
        event_type="station",
        source_kind=source.source_kind,
        source_device_profile_id=source.source_device_profile_id,
        source_provider_type=source.decoder_origin,
        callsign_or_node_id="K7ABC",
        lat=39.0,
        lon=-105.0,
        location_precision="grid6",
        location_trust="on_air",
        event_timestamp_utc="2026-07-22T20:14:00Z",
        last_updated_utc="2026-07-22T20:14:05Z",
        transport_badges=("JS8", "SDRangel"),
    )

    assert event.id.startswith(source.message_id)
    assert event.source_kind == "radio"
    assert event.source_device_profile_id == "radio_2"
    assert source.received_at_utc == "2026-07-22T20:14:00Z"
    assert event.location_precision == "grid6"
    assert event.location_trust == "on_air"


def test_plan_context_service_uses_cache_until_invalidated() -> None:
    selection = RuntimeSelectionService()
    assignments = AssignedPlanService()
    scheduler = SchedulerStateService()
    health = StationHealthService()
    messages = MessageSourceIndex()

    selection.set_primary_runtime_radio("radio_1", source="settings")
    selection.set_active_runtime_radios(["radio_1"], source="scheduler")
    assignment = assignments.assign("radio_1", "plan_1")
    scheduler.set_state(
        SchedulerState(
            radio_profile_id="radio_1",
            assigned_plan_id=assignment.id,
            state="active",
            schedule_status="on_schedule",
        )
    )
    messages.add(
        MessageSourceRecord(
            id="msg_1",
            source_software="JS8Call",
            source_radio_id="radio_1",
            received_utc="2026-06-02T00:00:00Z",
        )
    )

    context_service = PlanContextService(
        selection_service=selection,
        assigned_plan_service=assignments,
        scheduler_state_service=scheduler,
        station_health_service=health,
        message_source_index=messages,
    )

    first = context_service.get_context("controlfreq")
    second = context_service.get_context("controlfreq")
    context_service.invalidate("controlfreq")
    third = context_service.get_context("controlfreq")

    assert first is second
    assert third is not first
    assert context_service.rebuild_count == 2
    assert first.selected_radio_id == "radio_1"
    assert first.assigned_plan_id == assignment.id
    assert first.message_count == 1


def test_action_feedback_service_records_recent_events_and_notifies_subscribers() -> None:
    service = ActionFeedbackService(max_recent=2)
    observed = []
    service.subscribe(observed.append)

    first = service.publish(
        scope="radio",
        radio_profile_id="radio_1",
        action_type="qsy",
        target_label="DX10",
        status="requested",
        summary="QSY sent to DX10: 7.268 LSB",
        source_surface="control_center",
    )
    second = service.publish(
        scope="radio",
        radio_profile_id="radio_1",
        action_type="qsy",
        target_label="DX10",
        status="succeeded",
        summary="DX10 moved to 7.268 LSB",
    )
    service.publish(
        scope="settings",
        action_type="save",
        target_label="Portable",
        status="succeeded",
        summary="Settings saved for Portable",
    )

    assert observed[0] == first
    assert observed[1] == second
    assert [event.summary for event in service.recent()] == [
        "Settings saved for Portable",
        "DX10 moved to 7.268 LSB",
    ]
    assert service.recent(radio_profile_id="radio_1") == [second]
    assert [event.summary for event in service.recent(newest_first=False)] == [
        "DX10 moved to 7.268 LSB",
        "Settings saved for Portable",
    ]


def test_action_feedback_rejects_unknown_status() -> None:
    service = ActionFeedbackService()

    with pytest.raises(ValueError):
        service.publish(
            scope="system",
            action_type="save",
            status="mystery",
            summary="Bad status",
        )


def test_action_feedback_rejects_unknown_scope_and_isolates_subscriber_errors() -> None:
    service = ActionFeedbackService()
    observed = []

    def broken(_event):
        raise RuntimeError("subscriber failed")

    service.subscribe(broken)
    service.subscribe(observed.append)

    with pytest.raises(ValueError):
        service.publish(scope="popup", action_type="save", status="succeeded", summary="Bad scope")

    event = service.publish(scope="system", action_type="save", status="succeeded", summary="Saved")

    assert observed == [event]
    assert len(service.subscriber_errors) == 1
