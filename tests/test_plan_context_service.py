from __future__ import annotations

from pathlib import Path

from freqinout.core.multi_radio_store import MultiRadioStore, set_multi_rig_migration_version
from freqinout.core.multi_rig_runtime_status import radio_shared_state_id
from freqinout.core.plan_context_service import PlanContextService
from freqinout.core.runtime_policy_selection_service import (
    SOURCE_SETTINGS,
    DurableRuntimePolicyStore,
    DurableRuntimeSelectionService,
)
from freqinout.gui.plan_context_label import (
    PLAN_CONTEXT_FALLBACK_TEXT,
    plan_context_display_text,
)


def _store(tmp_path: Path) -> MultiRadioStore:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    with store.connect() as conn:
        set_multi_rig_migration_version(conn)
    return store


def _radio(
    store: MultiRadioStore,
    key: str,
    name: str,
    *,
    active: bool = True,
    primary: bool = False,
    display_order: int = 0,
    device_class: str = "tx_rx",
) -> dict:
    radio = store.save_device_profile(
        {
            "system_key": key,
            "name": name,
            "control_backend": "flrig",
            "device_class": device_class,
            "runtime_active": 0,
            "runtime_primary": 0,
            "display_order": display_order,
        }
    )
    if active:
        store.set_device_profile_runtime_active(int(radio["id"]), True)
    if primary:
        store.set_runtime_primary_device_profile(int(radio["id"]))
    return store.get_device_profile(int(radio["id"])) or radio


def _assign_behavior_and_schedule(
    store: MultiRadioStore,
    radio_id: int,
    *,
    model_values: dict,
    plan_values: dict,
) -> tuple[dict, dict]:
    model = store.save_operating_profile(model_values)
    plan = store.save_frequency_plan(plan_values)
    store.set_device_operating_profile(int(radio_id), int(model["id"]))
    store.set_assigned_plan(int(radio_id), int(plan["id"]))
    return model, plan


def test_plan_context_joins_primary_radio_to_assigned_frequency_plan(tmp_path: Path) -> None:
    store = _store(tmp_path)
    radio = _radio(store, "radio_a", "Radio A", active=True, primary=True)
    radio_id = radio_shared_state_id(radio["id"])
    _, plan = _assign_behavior_and_schedule(
        store,
        int(radio["id"]),
        model_values={
            "system_key": "field_ops_model",
            "name": "Field Ops Model",
            "scheduler_enabled": 1,
            "use_messages": 1,
            "use_map": 1,
            "use_launch_control": 1,
            "use_net_control_tabs": 1,
        },
        plan_values={"system_key": "field_ops_schedule", "name": "Field Ops"},
    )
    store.save_device_profile({"id": int(radio["id"]), "launch_enabled": 1})

    context = PlanContextService(store).primary_context()

    assert context is not None
    assert context.radio_profile_id == radio_id
    assert context.radio_label == "Radio A"
    assert context.frequency_plan_id == f"plan_{plan['id']}"
    assert context.plan_label == "Field Ops"
    assert context.summary_label == "Radio A - Field Ops"
    assert context.runtime_active is True
    assert context.runtime_primary is True
    assert context.scheduler_participating is True
    assert context.messages_enabled is True
    assert context.map_enabled is True
    assert context.launch_enabled is True
    assert context.net_control_enabled is True
    assert context.top_blocker == ""


def test_plan_context_active_contexts_preserve_runtime_selection_order(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True, display_order=1)
    second = _radio(store, "radio_b", "Radio B", active=False, display_order=2, device_class="observer")
    _assign_behavior_and_schedule(
        store,
        int(first["id"]),
        model_values={"system_key": "home_model", "name": "Home Model"},
        plan_values={"system_key": "home_plan", "name": "Home Plan"},
    )
    _assign_behavior_and_schedule(
        store,
        int(second["id"]),
        model_values={"system_key": "watch_model", "name": "Watch Model", "scheduler_enabled": 0, "receive_only": 1},
        plan_values={"system_key": "watch_plan", "name": "Watch Plan", "receive_only": 1},
    )
    store.set_device_profile_runtime_active(int(second["id"]), True)

    snapshot = PlanContextService(store).snapshot()

    assert snapshot.primary_runtime_radio_id == radio_shared_state_id(first["id"])
    assert snapshot.active_runtime_radio_ids == (
        radio_shared_state_id(first["id"]),
        radio_shared_state_id(second["id"]),
    )
    assert [context.radio_profile_id for context in snapshot.active_contexts] == list(snapshot.active_runtime_radio_ids)
    assert [context.plan_label for context in snapshot.active_contexts] == ["Home Plan", "Watch Plan"]
    assert [context.receive_only for context in snapshot.active_contexts] == [False, True]


def test_plan_context_tab_selection_uses_injected_selection_service(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True, display_order=1)
    second = _radio(store, "radio_b", "Radio B", active=True, display_order=2)
    first_id = radio_shared_state_id(first["id"])
    second_id = radio_shared_state_id(second["id"])
    _assign_behavior_and_schedule(
        store,
        int(first["id"]),
        model_values={"system_key": "primary_model", "name": "Primary Model"},
        plan_values={"system_key": "primary_plan", "name": "Primary Plan"},
    )
    _assign_behavior_and_schedule(
        store,
        int(second["id"]),
        model_values={"system_key": "map_model", "name": "Map Model"},
        plan_values={"system_key": "map_plan", "name": "Map Plan"},
    )
    policy_store = DurableRuntimePolicyStore(store)
    selection = DurableRuntimeSelectionService(store, policy_store=policy_store)
    selection.set_tab_radio("map", second_id, source_tab_id="map")
    selection.set_settings_radio(first_id, source=SOURCE_SETTINGS)

    service = PlanContextService(store, selection_service=selection)

    assert service.context_for_tab("map").radio_profile_id == second_id
    assert service.context_for_tab("map").plan_label == "Map Plan"
    assert service.context_for_tab("messages").radio_profile_id == first_id
    assert service.context_for_tab("messages").plan_label == "Primary Plan"


def test_plan_context_marks_operator_suppressed_radio_as_blocked(tmp_path: Path) -> None:
    store = _store(tmp_path)
    radio = _radio(store, "radio_a", "Radio A", active=True, primary=True)
    radio_id = radio_shared_state_id(radio["id"])
    _assign_behavior_and_schedule(
        store,
        int(radio["id"]),
        model_values={"system_key": "quiet_model", "name": "Quiet Model"},
        plan_values={"system_key": "quiet_plan", "name": "Quiet Plan"},
    )
    DurableRuntimePolicyStore(store).set_operator_suppressed(radio_id, True, source=SOURCE_SETTINGS)

    context = PlanContextService(store).context_for_radio(radio_id)

    assert context is not None
    assert context.operator_suppressed is True
    assert context.scheduler_participating is False
    assert context.top_blocker == "Radio is suppressed from runtime participation."


def test_freqplanner_context_text_formats_current_radio_and_plan(tmp_path: Path) -> None:
    store = _store(tmp_path)
    radio = _radio(store, "radio_a", "Radio A", active=True, primary=True)
    _assign_behavior_and_schedule(
        store,
        int(radio["id"]),
        model_values={"system_key": "field_model", "name": "Field Model", "use_messages": 1, "use_map": 0},
        plan_values={"system_key": "field_plan", "name": "Field Plan"},
    )

    context = PlanContextService(store).primary_context()

    assert plan_context_display_text(None) == PLAN_CONTEXT_FALLBACK_TEXT
    text = plan_context_display_text(context)
    assert "Reviewing Field Plan for Radio A" in text
    assert "primary runtime" in text
    assert "Scheduler: on." in text
    assert "Messages: on." in text
    assert "Map: off." in text
    assert "Receive-only plan." not in text


def test_plan_context_text_marks_receive_only_assigned_plan(tmp_path: Path) -> None:
    store = _store(tmp_path)
    radio = _radio(store, "radio_rx", "RX Observer", active=False, device_class="observer")
    _assign_behavior_and_schedule(
        store,
        int(radio["id"]),
        model_values={"system_key": "rx_watch_model", "name": "RX Watch Model", "scheduler_enabled": 0, "receive_only": 1},
        plan_values={"system_key": "rx_watch_plan", "name": "RX Watch", "receive_only": 1},
    )
    store.set_device_profile_runtime_active(int(radio["id"]), True)

    context = PlanContextService(store).context_for_radio(radio_shared_state_id(radio["id"]))

    assert context is not None
    assert context.receive_only is True
    assert "Receive-only plan." in plan_context_display_text(context)


def test_plan_context_text_summarizes_frequency_plan_provenance_refs(tmp_path: Path) -> None:
    store = _store(tmp_path)
    radio = _radio(store, "radio_source", "Source Radio", active=True, primary=True)
    _assign_behavior_and_schedule(
        store,
        int(radio["id"]),
        model_values={"system_key": "source_model", "name": "Source Model"},
        plan_values={
            "system_key": "source_rich",
            "name": "Source Rich Plan",
            "source_refs": ["src_hf", "src_net"],
            "schedule_refs": ["hf:mon:1900"],
            "frequency_refs": ["freq_40m", "freq_80m"],
            "group_refs": ["ARES"],
        },
    )

    context = PlanContextService(store).primary_context()

    assert context is not None
    assert context.source_ref_count == 2
    assert context.schedule_ref_count == 1
    assert context.frequency_ref_count == 2
    assert context.group_ref_count == 1
    text = plan_context_display_text(context)
    assert "Sources: 2 sources, 1 schedule ref, 2 frequency refs, 1 group ref." in text
