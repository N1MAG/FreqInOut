from __future__ import annotations

import pytest

from freqinout.core.shared_state import (
    AssignedPlanService,
    FormPurposeMapping,
    FormPurposeRouter,
    FrequencyPlanStore,
    MapLayerService,
    MessageSourceIndex,
    MessageSourceRecord,
    PlanContextService,
    RadioProfileStore,
    RuntimePolicy,
    RuntimePolicyStore,
    RuntimeSelectionService,
    SchedulerState,
    SchedulerStateService,
    SchedulerTarget,
    SelectionWriteError,
    StationHealthService,
)


def test_frequency_plan_can_be_created_saved_and_copied() -> None:
    store = FrequencyPlanStore()

    plan = store.create("Home HF Daily", category="normal")
    saved = store.save(plan)
    copied = store.copy(saved.id, name="Portable Event Plan")

    assert saved.saved is True
    assert saved.draft is False
    assert store.get(saved.id).name == "Home HF Daily"
    assert copied.id != saved.id
    assert copied.name == "Portable Event Plan"
    assert copied.saved is False
    assert copied.draft is True


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


def test_runtime_policy_restart_preserves_operator_intent_and_resets_temporary_state() -> None:
    store = RuntimePolicyStore()
    policy = RuntimePolicy(
        radio_profile_id="radio_1",
        messages_enabled=False,
        operator_suppressed=True,
        temporary_paused=True,
        manual_hold=True,
        transient_error="busy",
    )

    store.save(policy)
    store.clean_for_restart()
    restarted = store.get("radio_1")

    assert restarted.messages_enabled is False
    assert restarted.operator_suppressed is True
    assert restarted.temporary_paused is False
    assert restarted.manual_hold is False
    assert restarted.transient_error == ""


def test_runtime_selection_service_enforces_write_authority() -> None:
    service = RuntimeSelectionService()

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
    assert service.state.primary_runtime_radio_id == "radio_1"
    assert service.state.active_runtime_radio_ids == ("radio_1",)


def test_flrig_auto_discovery_respects_operator_suppression() -> None:
    radios = RadioProfileStore()
    policies = RuntimePolicyStore()

    active = radios.create("Active FLRig", control_backend="flrig", flrig_connected=True)
    suppressed = radios.create("Suppressed FLRig", control_backend="flrig", flrig_connected=True)
    offline = radios.create("Offline FLRig", control_backend="flrig", flrig_connected=False)

    policies.save(RuntimePolicy(radio_profile_id=suppressed.id, operator_suppressed=True))

    discovered = policies.discover_active_radios(
        radios.list(),
        flrig_health={active.id: True, suppressed.id: True, offline.id: False},
    )

    assert discovered == (active.id,)


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
    assert health.top_issue("radio_1") == issue


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

