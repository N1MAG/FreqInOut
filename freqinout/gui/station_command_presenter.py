from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping


ShellDensity = Literal["condensed", "compact", "roomy"]
ScheduleProminence = Literal["calm", "upcoming", "urgent", "overdue"]


@dataclass(frozen=True)
class ShellLayoutState:
    density: ShellDensity
    show_source_context: bool
    show_clock_seconds: bool
    stack_primary_context: bool


@dataclass(frozen=True)
class NextActionState:
    text: str
    prominence: ScheduleProminence
    role: str


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


def shell_layout_state(
    width: object,
    *,
    source_count: int = 1,
    large_text: bool = False,
) -> ShellLayoutState:
    """Return a deterministic shell density without inspecting Qt widgets."""
    try:
        available = max(0, int(width))
    except Exception:
        available = 0
    pressure = max(0, int(source_count or 0) - 1) * 90
    if large_text:
        pressure += 140
    effective = available - pressure
    if effective < 720:
        return ShellLayoutState("condensed", False, False, True)
    if effective < 1120:
        return ShellLayoutState("compact", False, False, False)
    return ShellLayoutState("roomy", True, True, False)


def schedule_prominence(remaining_minutes: object | None) -> ScheduleProminence:
    if remaining_minutes is None:
        return "calm"
    try:
        minutes = float(remaining_minutes)
    except Exception:
        return "calm"
    if minutes < 0:
        return "overdue"
    if minutes <= 15:
        return "urgent"
    if minutes <= 30:
        return "upcoming"
    return "calm"


def next_action_state(label: object, remaining_minutes: object | None) -> NextActionState:
    text = str(label or "").strip() or "No scheduled action"
    prominence = schedule_prominence(remaining_minutes)
    if remaining_minutes is not None:
        try:
            minutes = int(float(remaining_minutes))
        except Exception:
            minutes = -1
        if minutes >= 0:
            text = f"{text} in {minutes}m"
    role = {
        "calm": "muted",
        "upcoming": "info",
        "urgent": "warning",
        "overdue": "danger",
    }[prominence]
    return NextActionState(text=text, prominence=prominence, role=role)


def source_chip_text(
    name: object,
    now_text: object,
    *,
    density: ShellDensity,
) -> str:
    radio_name = str(name or "Radio").strip() or "Radio"
    context = str(now_text or "").strip()
    if density == "roomy" and context:
        return f"{radio_name}  ·  {context}"
    return radio_name


def primary_context_text(radio_name: object, now_text: object) -> str:
    name = str(radio_name or "Radio").strip() or "Radio"
    destination = str(now_text or "Unavailable").strip() or "Unavailable"
    return f"{name}  ·  {destination}"


def qsy_key(meta: Mapping[str, object] | None) -> str:
    if not isinstance(meta, Mapping):
        return ""
    try:
        return f"{float(meta.get('freq')):.6f}"
    except Exception:
        return ""


def frequency_controls_available(profile: Mapping[str, object] | None) -> bool:
    if not isinstance(profile, Mapping):
        return False
    backend = str(profile.get("control_backend", "") or "").strip().lower()
    use_varac = _truthy(profile.get("use_varac", profile.get("uses_varac", False)))
    use_flrig = _truthy(profile.get("use_flrig", profile.get("uses_flrig", False)))
    use_js8call = _truthy(profile.get("use_js8call", profile.get("uses_js8call", False)))
    use_fldigi = _truthy(profile.get("use_fldigi", profile.get("uses_fldigi", False)))
    varac_only = use_varac and not any((use_flrig, use_js8call, use_fldigi))
    if varac_only:
        return False
    return backend in {"flrig", "rigctld", "js8call"}


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


def _truthy(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)
