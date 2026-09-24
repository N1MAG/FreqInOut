from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from freqinout.core.condition_level_update import (
    ConditionLevelUpdateResult,
    apply_operating_group_condition_level,
)
from freqinout.core.condition_sop_policy import (
    ConditionSopInvocationDecision,
    condition_levels_include,
    evaluate_condition_sop_invocations,
)
from freqinout.core.observation_projection import Observation


@dataclass(frozen=True)
class ConditionSopInvocationPlan:
    decision: ConditionSopInvocationDecision
    update: ConditionLevelUpdateResult | None = None
    audit: Mapping[str, Any] | None = None

    @property
    def ready_to_apply(self) -> bool:
        return bool(self.decision.should_apply and self.update is not None and not self.update.warnings)


def plan_condition_sop_invocations(
    observations: Sequence[Observation | Mapping[str, object]],
    *,
    settings_data: Mapping[str, Any],
    sop_profiles: Sequence[Mapping[str, object]] = (),
    auto_apply_enabled: bool = False,
    rf_guard_state_by_profile: Mapping[str, Mapping[str, object]] | None = None,
) -> tuple[ConditionSopInvocationPlan, ...]:
    """Build side-effect-free SOP invocation plans from condition alerts.

    The returned plans are intentionally not persisted. A caller must still
    decide to save the returned `update.settings_data`, record `audit`, and
    refresh scheduler projections. This keeps ingest fast and keeps all
    unattended behavior behind an explicit orchestration layer.
    """
    decisions = evaluate_condition_sop_invocations(
        observations,
        sop_profiles=sop_profiles,
        auto_apply_enabled=auto_apply_enabled,
        rf_guard_state_by_profile=rf_guard_state_by_profile,
    )
    plans: list[ConditionSopInvocationPlan] = []
    for decision in decisions:
        update: ConditionLevelUpdateResult | None = None
        if decision.should_apply and not decision.blocked and decision.operating_group and decision.condition_level is not None:
            update = apply_operating_group_condition_level(
                settings_data,
                operating_group=decision.operating_group,
                condition_level=int(decision.condition_level),
                create_if_missing=False,
            )
        plans.append(
            ConditionSopInvocationPlan(
                decision=decision,
                update=update,
                audit=_audit_payload(decision, update),
            )
        )
    return tuple(plans)


def schedule_layer_rows_for_condition_decision(
    profile: Mapping[str, Any],
    decision: ConditionSopInvocationDecision,
) -> tuple[dict[str, Any], ...]:
    """Return the enabled SOP schedule rows affected by a condition decision.

    This is used by unattended invocation preflight so RF Guard validates the
    actual condition layer being invoked instead of every row in a profile.
    """
    group = _normalize_group(decision.operating_group)
    level = decision.condition_level
    raw_rows = profile.get("schedule_layer") or profile.get("layers") or ()
    if isinstance(raw_rows, (str, bytes)) or not isinstance(raw_rows, Sequence):
        return ()
    rows: list[dict[str, Any]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, Mapping):
            continue
        if not _enabled(raw_row.get("enabled", True)):
            continue
        row_group = _normalize_group(raw_row.get("group_name") or raw_row.get("operating_group") or raw_row.get("group"))
        if row_group and group and row_group != group:
            continue
        if not condition_levels_include(raw_row.get("condition_levels") or raw_row.get("condition_level"), level):
            continue
        rows.append(dict(raw_row))
    return tuple(rows)


def _audit_payload(
    decision: ConditionSopInvocationDecision,
    update: ConditionLevelUpdateResult | None,
) -> Mapping[str, Any]:
    return {
        "event": "condition_sop_invocation",
        "decision": decision.decision,
        "observation_id": decision.observation_id,
        "operating_group": decision.operating_group,
        "condition_level": decision.condition_level,
        "sop_profile_id": decision.sop_profile_id,
        "sop_profile_name": decision.sop_profile_name,
        "should_apply": bool(decision.should_apply),
        "requires_confirmation": bool(decision.requires_confirmation),
        "blocked": bool(decision.blocked),
        "reasons": list(decision.reasons),
        "matched_rows": int(update.matched_rows) if update is not None else 0,
        "changed_rows": int(update.changed_rows) if update is not None else 0,
        "warnings": list(update.warnings) if update is not None else [],
    }


def _enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return True


def _normalize_group(value: Any) -> str:
    return str(value or "").strip().upper().lstrip("@").rstrip(">")
