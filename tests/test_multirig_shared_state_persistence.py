from pathlib import Path

from freqinout.core.multi_radio_store import MultiRadioStore, set_multi_rig_migration_version
from freqinout.core.shared_state_persistence import (
    SHARED_STATE_BRIDGE_VERSION,
    build_shared_state_snapshot,
)


def _store(tmp_path: Path) -> MultiRadioStore:
    return MultiRadioStore(tmp_path / "freqinout.db")


def test_snapshot_projects_store_rows_into_shared_state(tmp_path):
    store = _store(tmp_path)
    with store._connect() as conn:
        set_multi_rig_migration_version(conn)
    primary_plan = store.save_operating_profile(
        {
            "system_key": "home_hf",
            "name": "Home HF Daily",
            "description": "Daily HF operating plan",
            "scheduler_enabled": 1,
            "use_messages": 1,
            "use_map": 1,
        }
    )
    observer_plan = store.save_operating_profile(
        {
            "system_key": "rx_watch",
            "name": "RX Watch",
            "scheduler_enabled": 0,
            "use_messages": 1,
            "use_map": 1,
        }
    )
    primary = store.save_device_profile(
        {
            "system_key": "radio_a",
            "name": "Radio A",
            "control_backend": "flrig",
            "device_class": "tx_rx",
            "runtime_active": 1,
            "ptt_group": "main-ptt",
        }
    )
    observer = store.save_device_profile(
        {
            "system_key": "radio_b",
            "name": "Radio B",
            "control_backend": "manual",
            "device_class": "observer",
            "runtime_active": 0,
        }
    )
    store.set_device_operating_profile(primary["id"], primary_plan["id"])
    store.set_device_operating_profile(observer["id"], observer_plan["id"])
    store.set_device_profile_runtime_active(observer["id"], True)
    store.set_runtime_primary_device_profile(primary["id"])

    snapshot = build_shared_state_snapshot(store)

    assert snapshot.schema_version == SHARED_STATE_BRIDGE_VERSION
    assert {profile.name for profile in snapshot.radio_profiles} == {"Radio A", "Radio B"}
    assert {plan.name for plan in snapshot.frequency_plans} >= {"Home HF Daily", "RX Watch"}
    assert snapshot.selection_state.primary_runtime_radio_id == f"radio_{primary['id']}"
    assert snapshot.selection_state.active_runtime_radio_ids == (
        f"radio_{primary['id']}",
        f"radio_{observer['id']}",
    )

    assigned = {
        (assignment.radio_profile_id, assignment.frequency_plan_id)
        for assignment in snapshot.assigned_plans
    }
    assert (f"radio_{primary['id']}", f"plan_{primary_plan['id']}") in assigned
    assert (f"radio_{observer['id']}", f"plan_{observer_plan['id']}") in assigned


def test_runtime_policy_keeps_operator_intent_separate_from_temporary_state(tmp_path):
    store = _store(tmp_path)
    with store._connect() as conn:
        set_multi_rig_migration_version(conn)
    plan = store.save_operating_profile(
        {
            "system_key": "quiet_rx",
            "name": "Quiet RX",
            "scheduler_enabled": 0,
            "use_background_ingest": 0,
            "use_messages": 1,
            "use_map": 0,
            "use_launch_control": 0,
            "use_net_control_tabs": 0,
        }
    )
    radio = store.save_device_profile(
        {
            "system_key": "rx_radio",
            "name": "RX Radio",
            "control_backend": "manual",
            "runtime_active": 1,
            "launch_enabled": 0,
        }
    )
    store.set_device_operating_profile(radio["id"], plan["id"])
    snapshot = build_shared_state_snapshot(store)

    policy = next(item for item in snapshot.runtime_policies if item.radio_profile_id == f"radio_{radio['id']}")

    assert policy.scheduler_enabled is False
    assert policy.background_ingest_enabled is False
    assert policy.messages_enabled is True
    assert policy.map_enabled is False
    assert policy.launch_enabled is False
    assert policy.net_control_enabled is False
    assert policy.operator_suppressed is False
    assert policy.restart_clean().temporary_paused is False
    assert policy.restart_clean().manual_hold is False
    assert policy.restart_clean().transient_error == ""


def test_disabled_or_inactive_radio_projects_as_operator_suppressed(tmp_path):
    store = _store(tmp_path)
    with store._connect() as conn:
        set_multi_rig_migration_version(conn)
    disabled = store.save_device_profile(
        {
            "system_key": "disabled_radio",
            "name": "Disabled Radio",
            "enabled": 0,
            "control_backend": "manual",
            "runtime_active": 0,
        }
    )
    snapshot = build_shared_state_snapshot(store)

    policy = next(item for item in snapshot.runtime_policies if item.radio_profile_id == f"radio_{disabled['id']}")

    assert policy.operator_suppressed is True
