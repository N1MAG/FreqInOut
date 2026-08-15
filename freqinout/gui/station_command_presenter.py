from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class QsyActionState:
    qsy_enabled: bool
    timed_qsy_enabled: bool
    qsy_role: str
    timed_qsy_role: str


@dataclass(frozen=True)
class SchedulerActionState:
    timed_suspend_text: str
    timed_suspend_role: str
    resume_role: str


def qsy_key(meta: Mapping[str, object] | None) -> str:
    if not isinstance(meta, Mapping):
        return ""
    try:
        return f"{float(meta.get('freq')):.6f}"
    except Exception:
        return ""


def qsy_action_state(
    *,
    selected_meta: Mapping[str, object] | None,
    preferred_key: str,
    radio_id: int,
    selection_changed: bool,
    manual_qsy_active: bool,
    timed_qsy_active: bool,
) -> QsyActionState:
    selected_key = qsy_key(selected_meta)
    armed = bool(selected_key)
    changed = bool(selection_changed and selected_key)
    enabled_base = int(radio_id or 0) > 0 and armed
    qsy_enabled = enabled_base and changed
    qsy_active = bool(manual_qsy_active or timed_qsy_active)
    timed_qsy_enabled = enabled_base and (changed or qsy_active)
    return QsyActionState(
        qsy_enabled=qsy_enabled,
        timed_qsy_enabled=timed_qsy_enabled,
        qsy_role="info" if changed else "muted",
        timed_qsy_role="warning" if qsy_active else ("info" if changed else "muted"),
    )


def scheduler_action_state(
    *,
    manual_qsy_active: bool = False,
    timed_qsy_active: bool,
    timed_suspend_active: bool,
    scheduler_suspended_manual: bool,
    scheduler_state_text: str,
) -> SchedulerActionState:
    state = str(scheduler_state_text or "").strip().lower()
    suspend_active = timed_suspend_active or scheduler_suspended_manual or state == "scheduler suspended"
    resume_active = (
        manual_qsy_active
        or timed_qsy_active
        or timed_suspend_active
        or scheduler_suspended_manual
        or state in {"manual hold", "manual qsy", "scheduler suspended"}
    )
    return SchedulerActionState(
        timed_suspend_text=(
            "Indefinite Suspend"
            if scheduler_suspended_manual
            else "Extend Suspend"
            if timed_suspend_active
            else "Timed Suspend"
        ),
        timed_suspend_role="warning" if suspend_active else "muted",
        resume_role="warning" if resume_active else "muted",
    )


def timed_qsy_text(*, timed_qsy_active: bool) -> str:
    return "Extend QSY" if timed_qsy_active else "Timed QSY"


def countdown_text(remaining_sec: object) -> str:
    try:
        seconds = max(0, int(float(remaining_sec)))
    except Exception:
        return ""
    if seconds < 10 * 60:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes:02d}:{secs:02d}"
    minutes = max(1, int((seconds + 59) // 60))
    return f"{minutes}m"
