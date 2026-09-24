from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from freqinout.core.condition_sop_audit import append_condition_sop_invocation_audit
from freqinout.core.condition_sop_invocation import ConditionSopInvocationPlan
from freqinout.core.condition_level_update import condition_group_state_snapshot


class ConditionSopSettingsStore(Protocol):
    def get(self, key: str, default: Any = None) -> Any: ...

    def set(self, key: str, value: Any) -> None: ...

    def reload(self) -> None: ...


@dataclass(frozen=True)
class ConditionSopExecutionRecord:
    status: str
    observation_id: str = ""
    operating_group: str = ""
    condition_level: int | None = None
    audit_id: int | None = None
    message: str = ""


@dataclass(frozen=True)
class ConditionSopExecutionResult:
    records: tuple[ConditionSopExecutionRecord, ...] = ()

    @property
    def applied_count(self) -> int:
        return sum(1 for record in self.records if record.status == "applied")

    @property
    def audited_count(self) -> int:
        return sum(1 for record in self.records if record.audit_id is not None)

    @property
    def failed_count(self) -> int:
        return sum(1 for record in self.records if record.status == "failed")


def execute_condition_sop_invocation_plans(
    settings: ConditionSopSettingsStore,
    db_path: str | Path,
    plans: Sequence[ConditionSopInvocationPlan],
    *,
    apply_limit: int = 1,
) -> ConditionSopExecutionResult:
    """Persist approved condition-SOP plans and audit every outcome.

    This helper is the narrow mutation point for unattended condition-level SOP
    invocation. It is intentionally UI-free and conservative: only ready
    auto-apply plans can update settings, only one change is applied by default,
    and all prompt/suggest/blocked/deferred outcomes are recorded for later
    operator review.
    """
    max_apply = max(0, int(apply_limit))
    applied = 0
    records: list[ConditionSopExecutionRecord] = []
    settings_changed = False

    for plan in tuple(plans or ()):
        status = _execution_status(plan)
        if status == "applied" and applied >= max_apply:
            status = "deferred"

        message = _execution_message(plan, status)
        payload = _execution_payload(plan, status, message)

        if status == "applied":
            try:
                assert plan.update is not None
                previous_snapshot = condition_group_state_snapshot(
                    {"operating_groups": settings.get("operating_groups", [])},
                    operating_group=plan.decision.operating_group,
                )
                payload = dict(payload)
                payload["previous_condition_group_state"] = [dict(row) for row in previous_snapshot]
                payload["new_condition_level"] = plan.decision.condition_level
                settings.set("operating_groups", plan.update.settings_data.get("operating_groups", []))
                applied += 1
                settings_changed = True
            except Exception as exc:
                status = "failed"
                message = f"failed to apply condition level: {exc}"
                payload = _execution_payload(plan, status, message)

        audit_id = append_condition_sop_invocation_audit(db_path, payload, status=status)
        records.append(
            ConditionSopExecutionRecord(
                status=status,
                observation_id=str(payload.get("observation_id") or plan.decision.observation_id or ""),
                operating_group=plan.decision.operating_group,
                condition_level=plan.decision.condition_level,
                audit_id=audit_id,
                message=message,
            )
        )

    if settings_changed and hasattr(settings, "reload"):
        try:
            settings.reload()
        except Exception:
            pass

    return ConditionSopExecutionResult(records=tuple(records))


def _execution_status(plan: ConditionSopInvocationPlan) -> str:
    decision = plan.decision
    update = plan.update
    if decision.blocked or decision.decision == "blocked":
        return "blocked"
    if not decision.should_apply:
        if decision.requires_confirmation or decision.decision == "prompt":
            return "prompt"
        if decision.decision == "suggest":
            return "suggest"
        return "ignored"
    if update is None:
        return "failed"
    if update.warnings:
        return "failed"
    if update.changed_rows <= 0:
        return "no_change"
    return "applied"


def _execution_message(plan: ConditionSopInvocationPlan, status: str) -> str:
    group = plan.decision.operating_group or "unknown group"
    level = plan.decision.condition_level
    if status == "applied":
        return f"applied {group} condition level {level}"
    if status == "no_change":
        return f"{group} condition level {level} was already current"
    if status == "deferred":
        return f"deferred {group} condition level {level}; apply limit reached"
    if status == "prompt":
        return "operator confirmation required"
    if status == "suggest":
        return "suggestion only"
    if status == "blocked":
        return "; ".join(plan.decision.reasons) or "blocked"
    if plan.update is not None and plan.update.warnings:
        return "; ".join(plan.update.warnings)
    return status


def _execution_payload(plan: ConditionSopInvocationPlan, status: str, message: str) -> Mapping[str, Any]:
    payload = dict(plan.audit or {})
    payload["execution_status"] = status
    payload["execution_message"] = message
    return payload
