from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from freqinout.core.condition_level_update import (
    ConditionLevelRevertResult,
    revert_operating_group_condition_snapshot,
)


@dataclass(frozen=True)
class ConditionSopRevertResult:
    settings_data: dict[str, Any]
    operating_group: str = ""
    restored_rows: int = 0
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return bool(self.restored_rows > 0 and not self.warnings)


def revert_condition_sop_audit_row(
    settings_data: Mapping[str, Any],
    audit_row: Mapping[str, Any],
) -> ConditionSopRevertResult:
    """Return settings with an applied condition-SOP audit row reverted.

    Only rows that include a captured `previous_condition_group_state` are
    reversible. Older audit rows remain review-only.
    """
    payload = audit_row.get("payload")
    if not isinstance(payload, Mapping):
        payload = {}
    status = str(audit_row.get("status") or payload.get("execution_status") or "").strip().lower()
    if status != "applied":
        return ConditionSopRevertResult(
            settings_data=dict(settings_data or {}),
            operating_group=str(audit_row.get("operating_group") or payload.get("operating_group") or "").strip(),
            warnings=(f"only applied SOP automation rows can be reverted; this row is {status or 'unknown'}",),
        )
    group = str(audit_row.get("operating_group") or payload.get("operating_group") or "").strip()
    snapshot = payload.get("previous_condition_group_state")
    if not isinstance(snapshot, (list, tuple)):
        snapshot = ()
    result: ConditionLevelRevertResult = revert_operating_group_condition_snapshot(
        settings_data,
        operating_group=group,
        snapshot=tuple(item for item in snapshot if isinstance(item, Mapping)),
    )
    return ConditionSopRevertResult(
        settings_data=result.settings_data,
        operating_group=result.operating_group,
        restored_rows=result.restored_rows,
        warnings=result.warnings,
    )
