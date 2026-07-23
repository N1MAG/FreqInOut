from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.multi_rig_runtime_status import (
    MultiRigRuntimeStatus,
    build_multi_rig_runtime_status,
)
from freqinout.core.runtime_policy_selection_service import (
    DurableRuntimePolicyStore,
    DurableRuntimeSelectionService,
)
from freqinout.core.shared_state import AssignedPlan, FrequencyPlan, RadioProfile, RuntimePolicy, RuntimeSelectionState


SHARED_STATE_SCHEMA_MARKER_KEY = "multi_rig_shared_state_schema_version"
SHARED_STATE_BRIDGE_VERSION = 1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class SharedStateSnapshot:
    radio_profiles: tuple[RadioProfile, ...]
    frequency_plans: tuple[FrequencyPlan, ...]
    assigned_plans: tuple[AssignedPlan, ...]
    runtime_policies: tuple[RuntimePolicy, ...]
    selection_state: RuntimeSelectionState
    runtime_status: MultiRigRuntimeStatus
    startup_mode: str
    warnings: tuple[str, ...] = ()
    schema_version: int = SHARED_STATE_BRIDGE_VERSION


def _boolish(value: object, default: bool = False) -> bool:
    if value in (None, ""):
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _text(value: object, default: str = "") -> str:
    try:
        return str(value if value is not None else default).strip()
    except Exception:
        return str(default or "").strip()


def _id(prefix: str, value: object) -> str:
    text = _text(value)
    return f"{prefix}_{text}" if text else ""


def radio_profile_from_device_row(row: Mapping[str, Any]) -> RadioProfile:
    device_class = _text(row.get("device_class"), "tx_rx").lower() or "tx_rx"
    control_backend = _text(row.get("control_backend"), "manual").lower() or "manual"
    return RadioProfile(
        id=_id("radio", row.get("id")),
        name=_text(row.get("name"), "Radio") or "Radio",
        radio_class=device_class,
        deployment_mode=_text(row.get("deployment_mode"), "fixed") or "fixed",
        control_backend=control_backend,
        needs_operator_name=_boolish(row.get("needs_operator_name"), False),
        transmit_capable=device_class != "observer",
        ptt_group=_text(row.get("ptt_group")),
        assigned_plan_id=_id("plan", row.get("assigned_plan_id")) or None,
        uses_flrig=_boolish(row.get("use_flrig"), control_backend == "flrig"),
        uses_fldigi=_boolish(row.get("use_fldigi"), False),
        uses_flmsg=_boolish(row.get("use_flmsg"), False),
        uses_flamp=_boolish(row.get("use_flamp"), False),
        uses_js8call=_boolish(row.get("use_js8call"), control_backend == "js8call"),
        uses_js8spotter=_boolish(row.get("use_js8spotter"), False),
        uses_commstat=_boolish(row.get("use_commstat"), False),
        uses_varac=_boolish(row.get("use_varac"), False),
        uses_wsjtx=_boolish(row.get("use_wsjtx"), False),
        uses_mesh=_boolish(row.get("use_mesh"), False),
        flrig_connected=control_backend == "flrig" and _boolish(row.get("runtime_active"), False),
        enabled=_boolish(row.get("enabled"), True),
        notes=_text(row.get("notes")),
    )


def frequency_plan_from_operating_row(row: Mapping[str, Any]) -> FrequencyPlan:
    return FrequencyPlan(
        id=_id("plan", row.get("id")),
        name=_text(row.get("name"), "Operating Plan") or "Operating Plan",
        description=_text(row.get("description")),
        category="normal",
        status="saved",
        draft=False,
        saved=True,
        notes=_text(row.get("notes")),
        created_utc=_text(row.get("created_utc")) or _utc_now_iso(),
        updated_utc=_text(row.get("updated_utc")) or _utc_now_iso(),
    )


def assigned_plan_from_assignment_row(row: Mapping[str, Any]) -> AssignedPlan:
    state = _text(row.get("assignment_state"), "active").lower() or "active"
    return AssignedPlan(
        id=_id("assignment", row.get("id")),
        radio_profile_id=_id("radio", row.get("device_profile_id")),
        frequency_plan_id=_id("plan", row.get("operating_profile_id")),
        assignment_category="temporary" if state == "temporary_override" else "normal",
        active=state in {"active", "temporary_override"},
        default=False,
        temporary_override=state == "temporary_override",
        temporary_override_until_utc=_text(row.get("temporary_override_until_utc")) or None,
        receive_only=False,
        scheduler_enforcement="enabled",
        scheduler_mode=_text(row.get("scheduler_mode"), "full_fio_workflow") or "full_fio_workflow",
        created_utc=_text(row.get("created_utc")) or _utc_now_iso(),
        updated_utc=_text(row.get("updated_utc")) or _utc_now_iso(),
    )


class SharedStatePersistenceAdapter:
    def __init__(self, store: MultiRadioStore) -> None:
        self.store = store

    def snapshot(self, runtime_status: Optional[MultiRigRuntimeStatus] = None) -> SharedStateSnapshot:
        runtime_status = runtime_status or build_multi_rig_runtime_status(self.store)
        policy_store = DurableRuntimePolicyStore(self.store)
        selection_service = DurableRuntimeSelectionService(self.store, policy_store=policy_store)
        device_rows = self.store.list_device_profiles()
        operating_rows = self.store.list_operating_profiles()
        assignment_rows = self.store.list_effective_assignments()

        policies = list(policy_store.list_policies(runtime_status=runtime_status))
        selection = selection_service.state(runtime_status=runtime_status)

        return SharedStateSnapshot(
            radio_profiles=tuple(radio_profile_from_device_row(row) for row in device_rows),
            frequency_plans=tuple(frequency_plan_from_operating_row(row) for row in operating_rows),
            assigned_plans=tuple(assigned_plan_from_assignment_row(row) for row in assignment_rows),
            runtime_policies=tuple(policies),
            selection_state=selection,
            runtime_status=runtime_status,
            startup_mode=runtime_status.startup_mode,
            warnings=runtime_status.warnings,
        )

    def active_runtime_radio_ids(self) -> tuple[str, ...]:
        return self.snapshot().selection_state.active_runtime_radio_ids

    def primary_runtime_radio_id(self) -> Optional[str]:
        return self.snapshot().selection_state.primary_runtime_radio_id


def build_shared_state_snapshot(store: MultiRadioStore) -> SharedStateSnapshot:
    return SharedStatePersistenceAdapter(store).snapshot()
