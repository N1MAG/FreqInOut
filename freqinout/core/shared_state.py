from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from itertools import count
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_iso(value: Optional[str]) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_expired_utc(value: Optional[str]) -> bool:
    parsed = _parse_utc_iso(value)
    if parsed is None:
        return False
    return parsed <= datetime.now(timezone.utc)


class SelectionWriteError(ValueError):
    """Raised when code tries to mutate selection state outside its authority."""


class NotFoundError(KeyError):
    """Raised when a shared-state object id is unknown."""


_id_counter = count(1)


def _next_id(prefix: str) -> str:
    return f"{prefix}_{next(_id_counter)}"


@dataclass(frozen=True)
class Frequency:
    id: str
    frequency: str
    frequency_hz: int = 0
    band: str = ""
    mode: str = ""
    offset: str = ""
    offset_hz: Optional[int] = None
    label: str = ""
    group: str = ""
    region: str = ""
    source_id: str = ""
    source_refs: tuple[str, ...] = ()
    confidence: str = ""
    notes: str = ""
    created_at_utc: str = field(default_factory=_utc_now_iso)
    updated_at_utc: str = field(default_factory=_utc_now_iso)


@dataclass(frozen=True)
class ScheduleSource:
    id: str
    name: str
    source_type: str = "operator"
    source_name: str = ""
    source_uri: str = ""
    url_or_reference: str = ""
    imported_utc: str = ""
    last_verified_utc: str = ""
    last_seen_utc: str = ""
    effective_range: str = ""
    region: str = ""
    license_notes: str = ""
    confidence: str = ""
    trust_level: str = "normal"
    metadata_json: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FrequencyPlan:
    id: str
    name: str
    description: str = ""
    category: str = "normal"
    status: str = "draft"
    frequencies: tuple[Frequency, ...] = ()
    source_refs: tuple[str, ...] = ()
    schedule_source_ids: tuple[str, ...] = ()
    schedule_refs: tuple[str, ...] = ()
    frequency_refs: tuple[str, ...] = ()
    group_refs: tuple[str, ...] = ()
    draft: bool = True
    saved: bool = False
    notes: str = ""
    created_utc: str = field(default_factory=_utc_now_iso)
    updated_utc: str = field(default_factory=_utc_now_iso)

    @property
    def created_at_utc(self) -> str:
        return self.created_utc

    @property
    def updated_at_utc(self) -> str:
        return self.updated_utc

    def __post_init__(self) -> None:
        normalized_status = str(self.status or "").strip().lower()
        if normalized_status not in {"draft", "saved", "archived"}:
            normalized_status = "saved" if self.saved and not self.draft else "draft"
        object.__setattr__(self, "status", normalized_status)
        object.__setattr__(self, "draft", normalized_status == "draft")
        object.__setattr__(self, "saved", normalized_status == "saved")
        if not self.frequency_refs and self.frequencies:
            object.__setattr__(self, "frequency_refs", tuple(frequency.id for frequency in self.frequencies))
        if not self.source_refs and self.schedule_source_ids:
            object.__setattr__(self, "source_refs", tuple(self.schedule_source_ids))


@dataclass(frozen=True)
class AssignedPlan:
    id: str
    radio_profile_id: str
    frequency_plan_id: str
    assignment_category: str = "normal"
    scheduler_mode: str = "full_fio_workflow"
    active: bool = True
    default: bool = False
    temporary_override: bool = False
    temporary_override_until_utc: Optional[str] = None
    receive_only: bool = False
    scheduler_enforcement: str = "enabled"
    created_utc: str = field(default_factory=_utc_now_iso)
    updated_utc: str = field(default_factory=_utc_now_iso)

    @property
    def is_active(self) -> bool:
        return self.active

    @property
    def is_default(self) -> bool:
        return self.default

    @property
    def created_at_utc(self) -> str:
        return self.created_utc

    @property
    def updated_at_utc(self) -> str:
        return self.updated_utc

    @property
    def is_temporary(self) -> bool:
        return self.assignment_category == "temporary" or self.temporary_override or bool(self.temporary_override_until_utc)

    @property
    def temporary_override_active(self) -> bool:
        return self.is_temporary and not _is_expired_utc(self.temporary_override_until_utc)

    def __post_init__(self) -> None:
        category = str(self.assignment_category or "normal").strip().lower() or "normal"
        if self.temporary_override or self.temporary_override_until_utc:
            category = "temporary"
        object.__setattr__(self, "assignment_category", category)
        object.__setattr__(self, "temporary_override", category == "temporary")
        object.__setattr__(self, "scheduler_mode", str(self.scheduler_mode or "full_fio_workflow").strip() or "full_fio_workflow")


@dataclass(frozen=True)
class RadioProfile:
    id: str
    name: str
    radio_class: str = "tx_rx"
    deployment_mode: str = "fixed"
    control_backend: str = "manual"
    needs_operator_name: bool = False
    transmit_capable: bool = True
    ptt_group: str = ""
    assigned_plan_id: Optional[str] = None
    uses_flrig: bool = False
    uses_fldigi: bool = False
    uses_flmsg: bool = False
    uses_flamp: bool = False
    uses_js8call: bool = False
    uses_js8spotter: bool = False
    uses_commstat: bool = False
    uses_varac: bool = False
    uses_wsjtx: bool = False
    uses_mesh: bool = False
    flrig_connected: bool = False
    enabled: bool = True
    notes: str = ""

    @property
    def display_name(self) -> str:
        return self.name

    def __post_init__(self) -> None:
        radio_class = str(self.radio_class or "tx_rx").strip().lower() or "tx_rx"
        object.__setattr__(self, "radio_class", radio_class)
        if radio_class == "observer":
            object.__setattr__(self, "transmit_capable", False)


@dataclass(frozen=True)
class RuntimePolicy:
    radio_profile_id: str
    scheduler_control: str = "enabled"
    scheduler_enabled: bool = True
    background_ingest_enabled: bool = True
    messages_enabled: bool = True
    map_enabled: bool = True
    launch_enabled: bool = False
    net_control_enabled: bool = True
    message_view_participation: Optional[bool] = None
    map_link_contribution: Optional[bool] = None
    launch_control_participation: Optional[bool] = None
    net_control_participation: Optional[bool] = None
    operator_suppressed: bool = False
    explicitly_suppressed: Optional[bool] = None
    updated_at_utc: str = field(default_factory=_utc_now_iso)

    def restart_clean(self) -> "RuntimePolicy":
        """Stable policy survives restart unchanged."""
        return self

    @property
    def runtime_available(self) -> bool:
        return not self.operator_suppressed and not bool(self.explicitly_suppressed)

    @property
    def background_ingest(self) -> bool:
        return self.background_ingest_enabled

    @property
    def message_view_enabled(self) -> bool:
        return self.messages_enabled if self.message_view_participation is None else self.message_view_participation

    @property
    def map_link_enabled(self) -> bool:
        return self.map_enabled if self.map_link_contribution is None else self.map_link_contribution

    @property
    def launch_control_enabled(self) -> bool:
        return False if self.launch_control_participation is None else self.launch_control_participation

    @property
    def net_control_participation_enabled(self) -> bool:
        return self.net_control_enabled if self.net_control_participation is None else self.net_control_participation

    @property
    def stable_policy_only(self) -> Mapping[str, bool | str | None]:
        return {
            "radio_profile_id": self.radio_profile_id,
            "scheduler_control": self.scheduler_control,
            "scheduler_enabled": self.scheduler_enabled,
            "background_ingest_enabled": self.background_ingest_enabled,
            "messages_enabled": self.messages_enabled,
            "map_enabled": self.map_enabled,
            "launch_control_participation": self.launch_control_participation,
            "net_control_enabled": self.net_control_enabled,
            "operator_suppressed": self.operator_suppressed,
            "explicitly_suppressed": self.explicitly_suppressed,
        }


@dataclass(frozen=True)
class RuntimeTransientState:
    radio_profile_id: str
    temporary_paused: bool = False
    manual_hold: bool = False
    transient_error: str = ""
    latest_event_id: Optional[str] = None
    updated_at_utc: str = field(default_factory=_utc_now_iso)

    @property
    def has_transient_condition(self) -> bool:
        return self.temporary_paused or self.manual_hold or bool(self.transient_error)

    def restart_clean(self) -> "RuntimeTransientState":
        return replace(
            self,
            temporary_paused=False,
            manual_hold=False,
            transient_error="",
            latest_event_id=None,
            updated_at_utc=_utc_now_iso(),
        )


@dataclass(frozen=True)
class RuntimeSelectionState:
    settings_radio_id: Optional[str] = None
    tab_radio_ids: Mapping[str, str] = field(default_factory=dict)
    primary_runtime_radio_id: Optional[str] = None
    active_runtime_radio_ids: tuple[str, ...] = ()
    updated_at_utc: str = field(default_factory=_utc_now_iso)

    @property
    def tab_radio_ids_json(self) -> Mapping[str, str]:
        return dict(self.tab_radio_ids)


@dataclass(frozen=True)
class SchedulerTarget:
    frequency: str = ""
    mode: str = ""
    offset: str = ""
    starts_utc: str = ""
    source: str = ""


@dataclass(frozen=True)
class SchedulerState:
    radio_profile_id: str
    assigned_plan_id: Optional[str] = None
    current_target: SchedulerTarget = field(default_factory=SchedulerTarget)
    next_target: SchedulerTarget = field(default_factory=SchedulerTarget)
    state: str = "disabled"
    schedule_status: str = "unknown"
    js8_offset_status: str = "unknown"
    current_blocker: str = ""
    last_transition_result: str = ""
    updated_utc: str = field(default_factory=_utc_now_iso)


@dataclass(frozen=True)
class SchedulerManualTarget:
    frequency_hz: int = 0
    mode: str = ""
    vfo: str = ""
    offset_hz: Optional[int] = None
    source_action: str = ""
    set_at_utc: str = field(default_factory=_utc_now_iso)


@dataclass(frozen=True)
class SchedulerManualControlState:
    radio_profile_id: str
    state: str = "on_schedule"
    manual_target: Optional[SchedulerManualTarget] = None
    hold_until_utc: Optional[str] = None
    reason_code: str = ""
    operator_source: str = "scheduler"
    latest_event_id: Optional[str] = None
    created_at_utc: str = field(default_factory=_utc_now_iso)
    updated_at_utc: str = field(default_factory=_utc_now_iso)


@dataclass(frozen=True)
class BusyEvidence:
    id: str
    radio_profile_id: str
    source_family: str
    reason_code: str
    severity: str
    evidence_timestamp_utc: str
    expiration_timestamp_utc: Optional[str] = None
    description: str = ""
    latest_event_id: Optional[str] = None


@dataclass(frozen=True)
class SchedulerEvent:
    id: str
    event_utc: str
    radio_profile_id: str
    assigned_plan_id: Optional[str]
    expected: SchedulerTarget
    attempted_action: str
    blocker_state: str = ""
    breakaway_state: str = ""
    result: str = ""
    explanation: str = ""
    event_type: str = ""
    source: str = ""
    target_json: Mapping[str, Any] = field(default_factory=dict)
    message: str = ""
    created_at_utc: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", self.event_type or self.attempted_action)
        object.__setattr__(self, "message", self.message or self.explanation)
        object.__setattr__(self, "created_at_utc", self.created_at_utc or self.event_utc)


@dataclass(frozen=True)
class PttConflictEvidence:
    id: str
    ptt_group: str
    requested_radio_id: str
    blocking_radio_id: Optional[str] = None
    severity: str = "hard"
    source: str = ""
    created_at_utc: str = field(default_factory=_utc_now_iso)


@dataclass(frozen=True)
class StationHealthIssue:
    id: str
    scope: str
    severity: str
    explanation: str
    family: str = ""
    summary: str = ""
    detail: str = ""
    evidence_ref: Optional[str] = None
    radio_profile_id: Optional[str] = None
    dependency: str = ""
    action_target: str = ""
    scheduler_event_id: Optional[str] = None
    created_utc: str = field(default_factory=_utc_now_iso)
    cleared_at_utc: Optional[str] = None

    @property
    def created_at_utc(self) -> str:
        return self.created_utc

    def __post_init__(self) -> None:
        object.__setattr__(self, "family", self.family or self.scope)
        object.__setattr__(self, "summary", self.summary or self.explanation)
        object.__setattr__(self, "detail", self.detail or self.explanation)
        object.__setattr__(self, "evidence_ref", self.evidence_ref or self.scheduler_event_id)


@dataclass(frozen=True)
class MessageSourceRecord:
    id: str
    source_software: str
    received_utc: str
    sender: str = ""
    source_radio_id: Optional[str] = None
    source_path: str = ""
    message_type: str = "message"
    trust_state: str = "unknown"
    plan_id: Optional[str] = None
    group: str = ""
    summary: str = ""
    can_render: bool = True
    quarantine_state: str = ""
    form_id: str = ""
    purpose: str = ""
    message_id: str = ""
    source_kind: str = ""
    source_device_profile_id: Optional[str] = None
    received_at_utc: str = ""
    decoder_origin: str = ""
    transport: str = ""
    attribution_summary: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "message_id", self.message_id or self.id)
        object.__setattr__(self, "source_device_profile_id", self.source_device_profile_id or self.source_radio_id)
        object.__setattr__(self, "received_at_utc", self.received_at_utc or self.received_utc)
        object.__setattr__(self, "decoder_origin", self.decoder_origin or self.source_software)
        object.__setattr__(self, "transport", self.transport or self.source_software)
        object.__setattr__(self, "attribution_summary", self.attribution_summary or self.summary)
        if not self.source_kind:
            object.__setattr__(self, "source_kind", "radio" if self.source_radio_id else "unknown")


@dataclass(frozen=True)
class ActionFeedbackEvent:
    id: str
    timestamp_utc: str
    scope: str
    action_type: str
    status: str
    summary: str
    radio_profile_id: Optional[str] = None
    target_label: str = ""
    detail: str = ""
    undo_command: Optional[str] = None
    source_surface: str = ""
    related_event_id: Optional[str] = None


@dataclass(frozen=True)
class FormPurposeMapping:
    form_id: str
    purpose: str
    destination: str = "messages"
    icon: str = ""
    alert: bool = False


@dataclass(frozen=True)
class OperationalContext:
    tab_id: str
    selected_radio_id: Optional[str]
    primary_runtime_radio_id: Optional[str]
    active_runtime_radio_ids: tuple[str, ...]
    assigned_plan_id: Optional[str] = None
    frequency_plan_id: Optional[str] = None
    scheduler_state: Optional[SchedulerState] = None
    top_issue: Optional[StationHealthIssue] = None
    message_count: int = 0
    map_layer_count: int = 0
    updated_utc: str = field(default_factory=_utc_now_iso)


@dataclass(frozen=True)
class MapLayerRecord:
    id: str
    purpose: str
    source_message_id: str
    source_radio_id: Optional[str]
    summary: str
    icon: str = ""
    alert: bool = False


@dataclass(frozen=True)
class MapEvent:
    id: str
    event_type: str
    source_kind: str
    event_timestamp_utc: str
    last_updated_utc: str
    source_device_profile_id: Optional[str] = None
    source_provider_type: Optional[str] = None
    callsign_or_node_id: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    grid: Optional[str] = None
    location_precision: str = "unknown"
    location_trust: str = "unknown"
    transport_badges: tuple[str, ...] = ()
    payload_json: Mapping[str, Any] = field(default_factory=dict)


class FrequencyPlanStore:
    def __init__(self) -> None:
        self._plans: Dict[str, FrequencyPlan] = {}

    def save(self, plan: FrequencyPlan) -> FrequencyPlan:
        now = _utc_now_iso()
        saved = replace(plan, saved=True, draft=False, status="saved", updated_utc=now)
        self._plans[saved.id] = saved
        return saved

    def create(self, name: str, *, category: str = "normal") -> FrequencyPlan:
        plan = FrequencyPlan(id=_next_id("plan"), name=name, category=category)
        self._plans[plan.id] = plan
        return plan

    def copy(self, plan_id: str, *, name: Optional[str] = None) -> FrequencyPlan:
        plan = self.get(plan_id)
        copied = replace(
            plan,
            id=_next_id("plan"),
            name=name or f"{plan.name} Copy",
            status="draft",
            draft=True,
            saved=False,
            created_utc=_utc_now_iso(),
            updated_utc=_utc_now_iso(),
        )
        self._plans[copied.id] = copied
        return copied

    def get(self, plan_id: str) -> FrequencyPlan:
        try:
            return self._plans[plan_id]
        except KeyError as exc:
            raise NotFoundError(plan_id) from exc

    def list(self) -> List[FrequencyPlan]:
        return list(self._plans.values())


class RadioProfileStore:
    def __init__(self) -> None:
        self._profiles: Dict[str, RadioProfile] = {}

    def save(self, profile: RadioProfile) -> RadioProfile:
        self._profiles[profile.id] = profile
        return profile

    def create(self, name: str, **kwargs: Any) -> RadioProfile:
        profile = RadioProfile(id=_next_id("radio"), name=name, **kwargs)
        self._profiles[profile.id] = profile
        return profile

    def get(self, radio_profile_id: str) -> RadioProfile:
        try:
            return self._profiles[radio_profile_id]
        except KeyError as exc:
            raise NotFoundError(radio_profile_id) from exc

    def list(self) -> List[RadioProfile]:
        return list(self._profiles.values())


class RuntimePolicyStore:
    def __init__(self) -> None:
        self._policies: Dict[str, RuntimePolicy] = {}

    def get(self, radio_profile_id: str) -> RuntimePolicy:
        return self._policies.get(radio_profile_id, RuntimePolicy(radio_profile_id=radio_profile_id))

    def save(self, policy: RuntimePolicy) -> RuntimePolicy:
        self._policies[policy.radio_profile_id] = policy
        return policy

    def clean_for_restart(self) -> None:
        # Stable policy is intentionally restart-safe; transient conditions live elsewhere.
        return None

    def discover_active_radios(
        self,
        profiles: Iterable[RadioProfile],
        *,
        flrig_health: Optional[Mapping[str, bool]] = None,
    ) -> tuple[str, ...]:
        healthy = dict(flrig_health or {})
        active: List[str] = []
        for profile in profiles:
            policy = self.get(profile.id)
            if not profile.enabled or profile.radio_class == "observer" or not policy.runtime_available:
                continue
            if profile.control_backend.lower() == "flrig":
                if healthy.get(profile.id, profile.flrig_connected):
                    active.append(profile.id)
                continue
        return tuple(active)


class RuntimeTransientStateStore:
    def __init__(self) -> None:
        self._states: Dict[str, RuntimeTransientState] = {}

    def get(self, radio_profile_id: str) -> RuntimeTransientState:
        return self._states.get(radio_profile_id, RuntimeTransientState(radio_profile_id=radio_profile_id))

    def save(self, state: RuntimeTransientState) -> RuntimeTransientState:
        updated = replace(state, updated_at_utc=_utc_now_iso())
        self._states[updated.radio_profile_id] = updated
        return updated

    def clear(self, radio_profile_id: str) -> RuntimeTransientState:
        cleared = self.get(radio_profile_id).restart_clean()
        self._states[cleared.radio_profile_id] = cleared
        return cleared

    def clean_for_restart(self) -> None:
        self._states = {
            radio_id: state.restart_clean()
            for radio_id, state in self._states.items()
        }


class RuntimeSelectionService:
    SETTINGS_SOURCE = "settings"
    MIGRATION_SOURCE = "migration"
    RUNTIME_POLICY_SOURCE = "runtime_policy"
    SCHEDULER_SOURCE = "scheduler"
    LAUNCH_SOURCE = "launch_orchestrator"

    def __init__(self) -> None:
        self._state = RuntimeSelectionState()

    @property
    def state(self) -> RuntimeSelectionState:
        return RuntimeSelectionState(
            settings_radio_id=self._state.settings_radio_id,
            tab_radio_ids=dict(self._state.tab_radio_ids),
            primary_runtime_radio_id=self._state.primary_runtime_radio_id,
            active_runtime_radio_ids=tuple(self._state.active_runtime_radio_ids),
            updated_at_utc=self._state.updated_at_utc,
        )

    def set_settings_radio(self, radio_id: str, *, source: str) -> None:
        if source != self.SETTINGS_SOURCE:
            raise SelectionWriteError("Only Settings may update the settings radio.")
        self._state = replace(self._state, settings_radio_id=radio_id, updated_at_utc=_utc_now_iso())

    def set_tab_radio(self, tab_id: str, radio_id: str, *, source_tab_id: str) -> None:
        if source_tab_id != tab_id:
            raise SelectionWriteError("A tab may update only its own selected radio.")
        tab_radios = dict(self._state.tab_radio_ids)
        tab_radios[tab_id] = radio_id
        self._state = replace(self._state, tab_radio_ids=tab_radios, updated_at_utc=_utc_now_iso())

    def set_primary_runtime_radio(self, radio_id: Optional[str], *, source: str) -> None:
        if source not in {self.SETTINGS_SOURCE, self.MIGRATION_SOURCE, self.RUNTIME_POLICY_SOURCE}:
            raise SelectionWriteError("Primary runtime radio requires explicit settings, migration, or runtime-policy authority.")
        self._state = replace(self._state, primary_runtime_radio_id=radio_id, updated_at_utc=_utc_now_iso())

    def set_active_runtime_radios(self, radio_ids: Sequence[str], *, source: str) -> None:
        if source not in {self.SCHEDULER_SOURCE, self.LAUNCH_SOURCE}:
            raise SelectionWriteError("Active runtime radios may be set only by scheduler or launch orchestration.")
        unique = tuple(dict.fromkeys(str(radio_id) for radio_id in radio_ids if str(radio_id or "").strip()))
        self._state = replace(self._state, active_runtime_radio_ids=unique, updated_at_utc=_utc_now_iso())


class AssignedPlanService:
    def __init__(self) -> None:
        self._assignments: Dict[str, AssignedPlan] = {}

    def assign(self, radio_profile_id: str, frequency_plan_id: str, **kwargs: Any) -> AssignedPlan:
        assignment = AssignedPlan(
            id=_next_id("assignment"),
            radio_profile_id=radio_profile_id,
            frequency_plan_id=frequency_plan_id,
            **kwargs,
        )
        self._assignments[assignment.id] = assignment
        return assignment

    def for_radio(self, radio_profile_id: str) -> Optional[AssignedPlan]:
        active = [
            item
            for item in self._assignments.values()
            if item.radio_profile_id == radio_profile_id and item.active
        ]
        if not active:
            return None
        temporary = [item for item in active if item.temporary_override_active]
        if temporary:
            return temporary[-1]
        default = [item for item in active if item.default or item.assignment_category == "default"]
        return default[-1] if default else active[-1]

    def unassign_radio(self, radio_profile_id: str) -> None:
        for assignment in list(self._assignments.values()):
            if assignment.radio_profile_id == radio_profile_id and assignment.active:
                self._assignments[assignment.id] = replace(assignment, active=False)

    def list(self) -> List[AssignedPlan]:
        return list(self._assignments.values())


class SchedulerStateService:
    def __init__(self) -> None:
        self._states: Dict[str, SchedulerState] = {}
        self._events: List[SchedulerEvent] = []

    def set_state(self, state: SchedulerState) -> SchedulerState:
        updated = replace(state, updated_utc=_utc_now_iso())
        self._states[updated.radio_profile_id] = updated
        return updated

    def get_state(self, radio_profile_id: str) -> Optional[SchedulerState]:
        return self._states.get(radio_profile_id)

    def record_event(
        self,
        *,
        radio_profile_id: str,
        assigned_plan_id: Optional[str],
        expected: Optional[SchedulerTarget] = None,
        attempted_action: str,
        blocker_state: str = "",
        breakaway_state: str = "",
        result: str = "",
        explanation: str = "",
    ) -> SchedulerEvent:
        event = SchedulerEvent(
            id=_next_id("sched_event"),
            event_utc=_utc_now_iso(),
            radio_profile_id=radio_profile_id,
            assigned_plan_id=assigned_plan_id,
            expected=expected or SchedulerTarget(),
            attempted_action=attempted_action,
            blocker_state=blocker_state,
            breakaway_state=breakaway_state,
            result=result,
            explanation=explanation,
        )
        self._events.append(event)
        return event

    def events_for_radio(self, radio_profile_id: str) -> List[SchedulerEvent]:
        return [event for event in self._events if event.radio_profile_id == radio_profile_id]


class StationHealthService:
    SEVERITY_ORDER = {"blocked": 0, "error": 1, "warning": 2, "info": 3, "critical": 0}

    def __init__(self) -> None:
        self._issues: Dict[str, StationHealthIssue] = {}

    def add_issue(self, issue: StationHealthIssue) -> StationHealthIssue:
        self._issues[issue.id] = issue
        return issue

    def clear_issue(self, issue_id: str) -> None:
        self._issues.pop(issue_id, None)

    def top_issue(self, radio_profile_id: Optional[str] = None) -> Optional[StationHealthIssue]:
        issues = [
            issue
            for issue in self._issues.values()
            if radio_profile_id is None or issue.radio_profile_id in {None, radio_profile_id}
        ]
        if not issues:
            return None
        return sorted(
            issues,
            key=lambda issue: (
                self.SEVERITY_ORDER.get(issue.severity, 99),
                issue.created_utc,
            ),
        )[0]

    def summarize_scheduler_blocker(self, event: SchedulerEvent, *, severity: str = "warning") -> StationHealthIssue:
        explanation = event.explanation or event.blocker_state or "Scheduler action was blocked."
        issue = StationHealthIssue(
            id=_next_id("health"),
            scope="scheduler",
            severity=severity,
            radio_profile_id=event.radio_profile_id,
            scheduler_event_id=event.id,
            explanation=explanation,
        )
        return self.add_issue(issue)


class MessageSourceIndex:
    def __init__(self) -> None:
        self._records: Dict[str, MessageSourceRecord] = {}

    def add(self, record: MessageSourceRecord) -> MessageSourceRecord:
        self._records[record.id] = record
        return record

    def list(
        self,
        *,
        radio_ids: Optional[Iterable[str]] = None,
        include_unknown: bool = True,
    ) -> List[MessageSourceRecord]:
        allowed = set(radio_ids or [])
        if not allowed:
            return list(self._records.values())
        result: List[MessageSourceRecord] = []
        for record in self._records.values():
            if record.source_radio_id in allowed:
                result.append(record)
            elif include_unknown and record.source_radio_id is None:
                result.append(record)
        return result


class FormPurposeRouter:
    UNKNOWN_PURPOSE = "general/archive"

    def __init__(self, mappings: Optional[Iterable[FormPurposeMapping]] = None) -> None:
        self._mappings: Dict[str, FormPurposeMapping] = {}
        for mapping in mappings or ():
            self.add_mapping(mapping)

    def add_mapping(self, mapping: FormPurposeMapping) -> None:
        self._mappings[mapping.form_id.upper()] = mapping

    def route(self, record: MessageSourceRecord) -> tuple[MessageSourceRecord, Optional[FormPurposeMapping]]:
        form_id = str(record.form_id or "").upper()
        mapping = self._mappings.get(form_id)
        if mapping is None:
            routed = replace(record, purpose=self.UNKNOWN_PURPOSE)
            return routed, None
        routed = replace(record, purpose=mapping.purpose)
        return routed, mapping


class MapLayerService:
    def from_routed_messages(
        self,
        records: Iterable[MessageSourceRecord],
        mappings: Mapping[str, FormPurposeMapping],
    ) -> List[MapLayerRecord]:
        layers: List[MapLayerRecord] = []
        for record in records:
            purpose = str(record.purpose or "")
            if not purpose or purpose == FormPurposeRouter.UNKNOWN_PURPOSE:
                continue
            mapping = mappings.get(str(record.form_id or "").upper())
            if mapping is None or not mapping.destination.startswith("map"):
                continue
            layers.append(
                MapLayerRecord(
                    id=_next_id("map_layer"),
                    purpose=purpose,
                    source_message_id=record.id,
                    source_radio_id=record.source_radio_id,
                    summary=record.summary,
                    icon=mapping.icon,
                    alert=mapping.alert,
                )
            )
        return layers


class ActionFeedbackService:
    VALID_STATUSES = frozenset(
        {
            "requested",
            "in_progress",
            "succeeded",
            "failed",
            "blocked",
            "partial",
            "undone",
            "expired",
        }
    )
    VALID_SCOPES = frozenset(
        {
            "radio",
            "settings",
            "scheduler",
            "messages",
            "map",
            "bbs",
            "system",
        }
    )

    def __init__(self, *, max_recent: int = 100) -> None:
        self.max_recent = max(1, int(max_recent))
        self._events: List[ActionFeedbackEvent] = []
        self._subscribers: List[Callable[[ActionFeedbackEvent], None]] = []
        self.subscriber_errors: List[Exception] = []

    def subscribe(self, callback: Callable[[ActionFeedbackEvent], None]) -> Callable[[], None]:
        self._subscribers.append(callback)
        return lambda: self._subscribers.remove(callback) if callback in self._subscribers else None

    def publish(
        self,
        *,
        scope: str,
        action_type: str,
        status: str,
        summary: str,
        radio_profile_id: Optional[str] = None,
        target_label: str = "",
        detail: str = "",
        undo_command: Optional[str] = None,
        source_surface: str = "",
        related_event_id: Optional[str] = None,
    ) -> ActionFeedbackEvent:
        normalized_status = str(status or "").strip().lower()
        if normalized_status not in self.VALID_STATUSES:
            raise ValueError(f"Unknown action feedback status: {status}")
        normalized_scope = str(scope or "system").strip().lower() or "system"
        if normalized_scope not in self.VALID_SCOPES:
            raise ValueError(f"Unknown action feedback scope: {scope}")
        event = ActionFeedbackEvent(
            id=_next_id("feedback"),
            timestamp_utc=_utc_now_iso(),
            radio_profile_id=radio_profile_id,
            scope=normalized_scope,
            action_type=str(action_type or "").strip().lower(),
            target_label=str(target_label or ""),
            status=normalized_status,
            summary=str(summary or "").strip(),
            detail=str(detail or ""),
            undo_command=undo_command,
            source_surface=str(source_surface or ""),
            related_event_id=related_event_id,
        )
        self._events.append(event)
        if len(self._events) > self.max_recent:
            self._events = self._events[-self.max_recent :]
        for callback in list(self._subscribers):
            try:
                callback(event)
            except Exception as exc:
                self.subscriber_errors.append(exc)
        return event

    def recent(
        self,
        *,
        radio_profile_id: Optional[str] = None,
        scope: Optional[str] = None,
        newest_first: bool = True,
    ) -> List[ActionFeedbackEvent]:
        events = list(reversed(self._events)) if newest_first else list(self._events)
        if radio_profile_id is not None:
            events = [event for event in events if event.radio_profile_id == radio_profile_id]
        if scope is not None:
            normalized_scope = str(scope or "").strip().lower()
            events = [event for event in events if event.scope == normalized_scope]
        return events


class PlanContextService:
    def __init__(
        self,
        *,
        selection_service: RuntimeSelectionService,
        assigned_plan_service: AssignedPlanService,
        scheduler_state_service: SchedulerStateService,
        station_health_service: StationHealthService,
        message_source_index: MessageSourceIndex,
        map_layer_provider: Callable[[], Sequence[MapLayerRecord]] | None = None,
    ) -> None:
        self._selection_service = selection_service
        self._assigned_plan_service = assigned_plan_service
        self._scheduler_state_service = scheduler_state_service
        self._station_health_service = station_health_service
        self._message_source_index = message_source_index
        self._map_layer_provider = map_layer_provider or (lambda: ())
        self._cache: Dict[str, OperationalContext] = {}
        self.rebuild_count = 0

    def invalidate(self, tab_id: Optional[str] = None) -> None:
        if tab_id is None:
            self._cache.clear()
        else:
            self._cache.pop(tab_id, None)

    def get_context(self, tab_id: str) -> OperationalContext:
        cached = self._cache.get(tab_id)
        if cached is not None:
            return cached
        self.rebuild_count += 1
        state = self._selection_service.state
        selected_radio_id = state.tab_radio_ids.get(tab_id) or state.primary_runtime_radio_id
        assignment = self._assigned_plan_service.for_radio(selected_radio_id) if selected_radio_id else None
        scheduler_state = self._scheduler_state_service.get_state(selected_radio_id) if selected_radio_id else None
        context = OperationalContext(
            tab_id=tab_id,
            selected_radio_id=selected_radio_id,
            primary_runtime_radio_id=state.primary_runtime_radio_id,
            active_runtime_radio_ids=state.active_runtime_radio_ids,
            assigned_plan_id=assignment.id if assignment else None,
            frequency_plan_id=assignment.frequency_plan_id if assignment else None,
            scheduler_state=scheduler_state,
            top_issue=self._station_health_service.top_issue(selected_radio_id),
            message_count=len(
                self._message_source_index.list(radio_ids=state.active_runtime_radio_ids)
            ),
            map_layer_count=len(tuple(self._map_layer_provider())),
        )
        self._cache[tab_id] = context
        return context
