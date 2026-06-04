from __future__ import annotations

from pathlib import Path

import pytest

from freqinout.core.multi_radio_store import MultiRadioStore, set_multi_rig_migration_version
from freqinout.core.multi_rig_runtime_status import (
    STARTUP_EXISTING_UNMIGRATED,
    build_multi_rig_runtime_status,
    device_profile_id_from_radio_id,
    radio_shared_state_id,
)
from freqinout.core.runtime_policy_selection_service import (
    CAPABILITY_BACKGROUND_INGEST,
    CAPABILITY_LAUNCH,
    CAPABILITY_MAP,
    CAPABILITY_MESSAGES,
    CAPABILITY_NET_CONTROL,
    CAPABILITY_SCHEDULER,
    SOURCE_LAUNCH_ORCHESTRATOR,
    SOURCE_RUNTIME_POLICY,
    SOURCE_SCHEDULER,
    SOURCE_SETTINGS,
    DurableRuntimePolicyStore,
    DurableRuntimeSelectionService,
)
from freqinout.core.shared_state import SelectionWriteError
from freqinout.core.shared_state_persistence import build_shared_state_snapshot


def _store(tmp_path: Path, *, migrated: bool = True) -> MultiRadioStore:
    store = MultiRadioStore(tmp_path / "freqinout.db")
    if migrated:
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
    control_backend: str = "flrig",
    device_class: str = "tx_rx",
    enabled: bool = True,
) -> dict:
    radio = store.save_device_profile(
        {
            "system_key": key,
            "name": name,
            "control_backend": control_backend,
            "device_class": device_class,
            "runtime_active": 0,
            "runtime_primary": 0,
            "display_order": display_order,
            "enabled": 1 if enabled else 0,
        }
    )
    if not active:
        with store.connect() as conn:
            conn.execute("UPDATE device_profiles SET runtime_active=0, runtime_primary=0 WHERE id=?", (int(radio["id"]),))
            conn.commit()
    if active:
        store.set_device_profile_runtime_active(int(radio["id"]), True)
    if primary:
        store.set_runtime_primary_device_profile(int(radio["id"]))
    return store.get_device_profile(int(radio["id"])) or radio


def test_radio_id_inverse_helper():
    assert radio_shared_state_id(7) == "radio_7"
    assert device_profile_id_from_radio_id("radio_7") == 7
    with pytest.raises(ValueError):
        device_profile_id_from_radio_id("device_7")


def test_pre_migration_runtime_writes_are_rejected_without_creating_profiles(tmp_path):
    store = _store(tmp_path, migrated=False)
    policy_store = DurableRuntimePolicyStore(store)
    selection = DurableRuntimeSelectionService(store, policy_store=policy_store)
    status = build_multi_rig_runtime_status(store)

    assert status.startup_mode == STARTUP_EXISTING_UNMIGRATED
    with pytest.raises(SelectionWriteError):
        selection.set_active_runtime_radios(("radio_1",), source=SOURCE_SCHEDULER, runtime_status=status)

    assert store.list_device_profiles() == []


def test_pre_migration_policy_read_does_not_create_policy_rows(tmp_path):
    store = _store(tmp_path, migrated=False)
    radio = store.save_device_profile({"system_key": "radio_a", "name": "Radio A"})
    status = build_multi_rig_runtime_status(store)
    policy_store = DurableRuntimePolicyStore(store)

    policy = policy_store.get_policy(radio_shared_state_id(radio["id"]), runtime_status=status)

    assert policy.operator_suppressed is True
    assert policy_store.is_radio_allowed(
        radio_shared_state_id(radio["id"]),
        CAPABILITY_MESSAGES,
        runtime_status=status,
    ) is False
    with store.connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM runtime_policies").fetchone()[0]
    assert count == 0


def test_runtime_policies_seed_from_existing_plan_flags(tmp_path):
    store = _store(tmp_path)
    plan = store.save_operating_profile(
        {
            "system_key": "quiet",
            "name": "Quiet Plan",
            "scheduler_enabled": 0,
            "use_background_ingest": 0,
            "use_messages": 1,
            "use_map": 0,
            "use_launch_control": 0,
            "use_net_control_tabs": 0,
        }
    )
    radio = _radio(store, "radio_a", "Radio A")
    store.set_device_operating_profile(int(radio["id"]), int(plan["id"]))

    policy = DurableRuntimePolicyStore(store).get_policy(radio_shared_state_id(radio["id"]))

    assert policy.scheduler_enabled is False
    assert policy.background_ingest_enabled is False
    assert policy.messages_enabled is True
    assert policy.map_enabled is False
    assert policy.launch_enabled is False
    assert policy.net_control_enabled is False
    assert policy.operator_suppressed is False


def test_participating_read_persists_seeded_policy_from_plan_flags(tmp_path):
    store = _store(tmp_path)
    plan = store.save_operating_profile(
        {
            "system_key": "messages_only",
            "name": "Messages Only",
            "scheduler_enabled": 0,
            "use_background_ingest": 0,
            "use_messages": 1,
            "use_map": 0,
            "use_launch_control": 0,
            "use_net_control_tabs": 0,
        }
    )
    radio = _radio(store, "radio_a", "Radio A", active=True, primary=True)
    radio_id = radio_shared_state_id(radio["id"])
    store.set_device_operating_profile(int(radio["id"]), int(plan["id"]))
    policy_store = DurableRuntimePolicyStore(store)

    assert policy_store.list_participating_radios(CAPABILITY_MESSAGES) == (radio_id,)

    with store.connect() as conn:
        row = conn.execute("SELECT * FROM runtime_policies WHERE radio_profile_id=?", (int(radio["id"]),)).fetchone()
    assert row is not None
    assert row["scheduler_enabled"] == 0
    assert row["background_ingest_enabled"] == 0
    assert row["messages_enabled"] == 1
    assert row["map_enabled"] == 0
    assert row["launch_enabled"] == 0
    assert row["net_control_enabled"] == 0
    assert row["created_utc"].endswith("Z")
    assert "T" in row["created_utc"]


def test_operator_suppression_blocks_participation_without_deleting_profile(tmp_path):
    store = _store(tmp_path)
    radio = _radio(store, "radio_a", "Radio A", active=True, primary=True)
    radio_id = radio_shared_state_id(radio["id"])
    policy_store = DurableRuntimePolicyStore(store)

    policy_store.set_operator_suppressed(radio_id, True, source=SOURCE_SETTINGS)

    assert store.get_device_profile(int(radio["id"])) is not None
    for capability in (
        CAPABILITY_SCHEDULER,
        CAPABILITY_BACKGROUND_INGEST,
        CAPABILITY_MESSAGES,
        CAPABILITY_MAP,
        CAPABILITY_LAUNCH,
        CAPABILITY_NET_CONTROL,
    ):
        assert policy_store.list_participating_radios(capability) == ()


def test_messages_and_map_default_to_all_active_allowed_radios(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True, display_order=1)
    second = _radio(store, "radio_b", "Radio B", active=True, display_order=2)
    policy_store = DurableRuntimePolicyStore(store)

    assert policy_store.list_participating_radios(CAPABILITY_MESSAGES) == (
        radio_shared_state_id(first["id"]),
        radio_shared_state_id(second["id"]),
    )
    assert policy_store.list_participating_radios(CAPABILITY_MAP) == (
        radio_shared_state_id(first["id"]),
        radio_shared_state_id(second["id"]),
    )


def test_controlfreq_and_main_window_use_primary_selection(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True, display_order=1)
    _radio(store, "radio_b", "Radio B", active=True, display_order=2)
    selection = DurableRuntimeSelectionService(store)

    assert selection.primary_runtime_radio_id() == radio_shared_state_id(first["id"])


def test_write_authority_rules_are_enforced(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True)
    radio_id = radio_shared_state_id(first["id"])
    policy_store = DurableRuntimePolicyStore(store)
    selection = DurableRuntimeSelectionService(store, policy_store=policy_store)

    with pytest.raises(SelectionWriteError):
        selection.set_settings_radio(radio_id, source=SOURCE_SCHEDULER)
    with pytest.raises(SelectionWriteError):
        selection.set_tab_radio("messages", radio_id, source_tab_id="map")
    with pytest.raises(SelectionWriteError):
        selection.set_primary_runtime_radio(radio_id, source=SOURCE_SCHEDULER)
    with pytest.raises(SelectionWriteError):
        selection.set_active_runtime_radios((radio_id,), source=SOURCE_SETTINGS)
    with pytest.raises(SelectionWriteError):
        policy_store.set_capability(radio_id, CAPABILITY_MESSAGES, False, source=SOURCE_SCHEDULER)


def test_suppressed_radios_cannot_be_activated(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True, display_order=1)
    second = _radio(store, "radio_b", "Radio B", active=True, display_order=2)
    policy_store = DurableRuntimePolicyStore(store)
    selection = DurableRuntimeSelectionService(store, policy_store=policy_store)
    second_id = radio_shared_state_id(second["id"])

    policy_store.set_operator_suppressed(second_id, True, source=SOURCE_SETTINGS)

    with pytest.raises(SelectionWriteError):
        selection.set_active_runtime_radios((radio_shared_state_id(first["id"]), second_id), source=SOURCE_SCHEDULER)


def test_suppressing_primary_promotes_lowest_order_remaining_primary(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True, display_order=10)
    second = _radio(store, "radio_b", "Radio B", active=True, display_order=2)
    third = _radio(store, "radio_c", "Radio C", active=True, display_order=3)
    policy_store = DurableRuntimePolicyStore(store)
    selection = DurableRuntimeSelectionService(store, policy_store=policy_store)

    policy_store.set_operator_suppressed(radio_shared_state_id(first["id"]), True, source=SOURCE_SETTINGS)

    assert selection.primary_runtime_radio_id() == radio_shared_state_id(second["id"])
    assert selection.active_runtime_radio_ids() == (
        radio_shared_state_id(second["id"]),
        radio_shared_state_id(third["id"]),
    )


def test_suppressing_only_active_radio_clears_primary(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True)
    policy_store = DurableRuntimePolicyStore(store)
    selection = DurableRuntimeSelectionService(store, policy_store=policy_store)

    policy_store.set_operator_suppressed(radio_shared_state_id(first["id"]), True, source=SOURCE_SETTINGS)

    assert selection.primary_runtime_radio_id() is None
    assert selection.active_runtime_radio_ids() == ()


def test_active_runtime_replacement_promotes_by_display_order_not_input_order(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True, display_order=1)
    second = _radio(store, "radio_b", "Radio B", active=True, display_order=2)
    selection = DurableRuntimeSelectionService(store)

    selection.set_primary_runtime_radio(None, source=SOURCE_RUNTIME_POLICY)
    state = selection.set_active_runtime_radios(
        (radio_shared_state_id(second["id"]), radio_shared_state_id(first["id"])),
        source=SOURCE_SCHEDULER,
    )

    assert state.primary_runtime_radio_id == radio_shared_state_id(first["id"])


def test_scheduler_remove_from_active_runtime_promotes_remaining_primary(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True, display_order=1)
    second = _radio(store, "radio_b", "Radio B", active=True, display_order=2)
    selection = DurableRuntimeSelectionService(store)

    state = selection.remove_from_active_runtime(
        radio_shared_state_id(first["id"]),
        source=SOURCE_SCHEDULER,
    )

    assert state.primary_runtime_radio_id == radio_shared_state_id(second["id"])
    assert state.active_runtime_radio_ids == (radio_shared_state_id(second["id"]),)


def test_capability_lookup_rejects_ui_scopes(tmp_path):
    store = _store(tmp_path)
    _radio(store, "radio_a", "Radio A", active=True, primary=True)

    with pytest.raises(ValueError):
        DurableRuntimePolicyStore(store).list_participating_radios("controlfreq")


def test_auto_discovery_uses_flrig_health_and_respects_suppression(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=False, display_order=1)
    second = _radio(store, "radio_b", "Radio B", active=False, display_order=2)
    first_id = radio_shared_state_id(first["id"])
    second_id = radio_shared_state_id(second["id"])
    policy_store = DurableRuntimePolicyStore(store)
    policy_store.set_operator_suppressed(second_id, True, source=SOURCE_SETTINGS)

    assert policy_store.discover_runtime_candidates({first_id: True, second_id: True}) == (first_id,)


def test_snapshot_uses_durable_runtime_policy_and_selection_services(tmp_path):
    store = _store(tmp_path)
    first = _radio(store, "radio_a", "Radio A", active=True, primary=True)
    second = _radio(store, "radio_b", "Radio B", active=True)
    policy_store = DurableRuntimePolicyStore(store)
    second_id = radio_shared_state_id(second["id"])
    policy_store.set_operator_suppressed(second_id, True, source=SOURCE_SETTINGS)

    snapshot = build_shared_state_snapshot(store)
    suppressed = next(policy for policy in snapshot.runtime_policies if policy.radio_profile_id == second_id)

    assert suppressed.operator_suppressed is True
    assert snapshot.selection_state.primary_runtime_radio_id == radio_shared_state_id(first["id"])
    assert snapshot.selection_state.active_runtime_radio_ids == (radio_shared_state_id(first["id"]),)
