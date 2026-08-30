from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from freqinout.core.source_view_contracts import (
    VIEW_ATTENTION,
    VIEW_COMPOSE,
    VIEW_MAP_CONTEXT,
    VIEW_OPERATOR_DIRECTORY,
    VIEW_RF_READINESS,
    VIEW_SCHEDULE,
    VIEW_SETUP,
    VIEW_STATION_COMMAND,
    VIEW_TRAFFIC_INBOX,
    SourceViewContract,
    contracts_for_view,
    normalize_source_family,
    normalize_view_name,
    source_contract_for,
)


VIEW_TEMPLATE_ATTENTION_QUEUE = "attention_queue"
VIEW_TEMPLATE_TRAFFIC_INBOX = "traffic_inbox"
VIEW_TEMPLATE_MAP_CONTEXT = "map_context"
VIEW_TEMPLATE_COMPOSE_WORKBENCH = "compose_workbench"
VIEW_TEMPLATE_SCHEDULE_OUTLOOK = "schedule_outlook"
VIEW_TEMPLATE_RF_READINESS = "rf_readiness"
VIEW_TEMPLATE_SETUP_CHECKLIST = "setup_checklist"
VIEW_TEMPLATE_STATION_COMMAND = "station_command"
VIEW_TEMPLATE_OPERATOR_DIRECTORY = "operator_directory"


@dataclass(frozen=True)
class OperationalViewDefinition:
    key: str
    label: str
    template: str
    default_tab: str
    default_enabled: bool
    supports_user_selection: bool
    allowed_source_families: tuple[str, ...]
    required_gates: tuple[str, ...]
    action_kinds: tuple[str, ...]
    max_default_rows: int
    notes: str = ""

    def supports_source(self, family: object) -> bool:
        normalized = normalize_source_family(family)
        return normalized in self.allowed_source_families

    def source_contracts(self) -> tuple[SourceViewContract, ...]:
        return tuple(source_contract_for(family) for family in self.allowed_source_families)


@dataclass(frozen=True)
class ViewGateReport:
    view_key: str
    source_family: str
    failures: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.failures


MANDATORY_VIEW_GATES = (
    "source_meaning",
    "volume_retention",
    "provenance_trust",
    "constrained_customization",
    "map_scaling",
    "action_validity",
)


CONTROLFREQ_PRESETS: Mapping[str, Mapping[str, bool]] = {
    "Operations": {
        "activity": True,
        "intersections": True,
        "schedule": True,
        "propagation": True,
    },
    "All": {
        "activity": True,
        "intersections": True,
        "schedule": True,
        "propagation": True,
    },
    "Traffic": {
        "activity": True,
        "intersections": True,
        "schedule": False,
        "propagation": False,
    },
    "Schedule": {
        "activity": False,
        "intersections": True,
        "schedule": True,
        "propagation": False,
    },
    "Propagation": {
        "activity": False,
        "intersections": False,
        "schedule": False,
        "propagation": True,
    },
}


CONTROLFREQ_VIEW_KEYS = ("activity", "intersections", "schedule", "propagation")


_VIEW_DEFINITIONS: dict[str, OperationalViewDefinition] = {
    "activity": OperationalViewDefinition(
        key="activity",
        label="Activity",
        template=VIEW_TEMPLATE_ATTENTION_QUEUE,
        default_tab="ControlFreq",
        default_enabled=True,
        supports_user_selection=True,
        allowed_source_families=tuple(contract.family for contract in contracts_for_view(VIEW_ATTENTION)),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("read", "reply", "map", "pin"),
        max_default_rows=12,
        notes="Global attention queue for high-value traffic, pins, and direct operator context.",
    ),
    "intersections": OperationalViewDefinition(
        key="intersections",
        label="Intersections",
        template=VIEW_TEMPLATE_SCHEDULE_OUTLOOK,
        default_tab="ControlFreq",
        default_enabled=True,
        supports_user_selection=True,
        allowed_source_families=("local",),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("tune", "compose"),
        max_default_rows=2,
        notes="Schedule intersections are displayed as part of schedule outlook, not as a separate mental model.",
    ),
    "schedule": OperationalViewDefinition(
        key="schedule",
        label="Schedule",
        template=VIEW_TEMPLATE_SCHEDULE_OUTLOOK,
        default_tab="ControlFreq",
        default_enabled=True,
        supports_user_selection=True,
        allowed_source_families=("local",),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("tune", "compose"),
        max_default_rows=4,
        notes="Current and next SOP/frequency-plan guidance.",
    ),
    "propagation": OperationalViewDefinition(
        key="propagation",
        label="Propagation",
        template=VIEW_TEMPLATE_RF_READINESS,
        default_tab="ControlFreq",
        default_enabled=True,
        supports_user_selection=True,
        allowed_source_families=("local",),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("map", "tune"),
        max_default_rows=3,
        notes="Compact RF readiness recommendation with opt-in details.",
    ),
    "traffic_inbox": OperationalViewDefinition(
        key="traffic_inbox",
        label="Traffic Inbox",
        template=VIEW_TEMPLATE_TRAFFIC_INBOX,
        default_tab="Messages",
        default_enabled=True,
        supports_user_selection=True,
        allowed_source_families=tuple(contract.family for contract in contracts_for_view(VIEW_TRAFFIC_INBOX)),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("read", "reply", "map", "pin", "open_source"),
        max_default_rows=200,
        notes="Normalized inbox view across RF, store-forward, file, and future mesh sources.",
    ),
    "compose_workbench": OperationalViewDefinition(
        key="compose_workbench",
        label="Compose",
        template=VIEW_TEMPLATE_COMPOSE_WORKBENCH,
        default_tab="Messages",
        default_enabled=True,
        supports_user_selection=False,
        allowed_source_families=tuple(contract.family for contract in contracts_for_view(VIEW_COMPOSE)),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("reply", "compose", "open_source"),
        max_default_rows=0,
        notes="Source-appropriate outbound workbench driven by ComposeIntent.",
    ),
    "map_context": OperationalViewDefinition(
        key="map_context",
        label="Map",
        template=VIEW_TEMPLATE_MAP_CONTEXT,
        default_tab="Map",
        default_enabled=True,
        supports_user_selection=True,
        allowed_source_families=tuple(contract.family for contract in contracts_for_view(VIEW_MAP_CONTEXT)),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("read", "reply", "map", "pin"),
        max_default_rows=250,
        notes="Geographic focus view with source-aware clustering and handoff filters.",
    ),
    "station_command": OperationalViewDefinition(
        key="station_command",
        label="Station Command",
        template=VIEW_TEMPLATE_STATION_COMMAND,
        default_tab="Shell",
        default_enabled=True,
        supports_user_selection=False,
        allowed_source_families=("local",),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("tune", "open_source"),
        max_default_rows=0,
        notes="Always-visible short-name radio command cards.",
    ),
    "setup_checklist": OperationalViewDefinition(
        key="setup_checklist",
        label="Setup",
        template=VIEW_TEMPLATE_SETUP_CHECKLIST,
        default_tab="Shell",
        default_enabled=True,
        supports_user_selection=False,
        allowed_source_families=("local",),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("open_source", "acknowledge"),
        max_default_rows=8,
        notes="Required/degraded setup issues that block useful operations.",
    ),
    "operator_directory": OperationalViewDefinition(
        key="operator_directory",
        label="Operators",
        template=VIEW_TEMPLATE_OPERATOR_DIRECTORY,
        default_tab="Operators",
        default_enabled=True,
        supports_user_selection=True,
        allowed_source_families=tuple(contract.family for contract in contracts_for_view(VIEW_OPERATOR_DIRECTORY)),
        required_gates=MANDATORY_VIEW_GATES,
        action_kinds=("read", "reply", "map", "pin"),
        max_default_rows=200,
        notes="Operator network awareness fed by callsigns, nodes, groups, and direct-message context.",
    ),
}


def all_operational_views() -> tuple[OperationalViewDefinition, ...]:
    return tuple(_VIEW_DEFINITIONS.values())


def operational_view_for(key: object) -> OperationalViewDefinition:
    normalized = normalize_view_name(key)
    if normalized in _VIEW_DEFINITIONS:
        return _VIEW_DEFINITIONS[normalized]
    raise KeyError(f"Unknown operational view: {key!r}")


def operational_views_for_source(family: object) -> tuple[OperationalViewDefinition, ...]:
    normalized = normalize_source_family(family)
    return tuple(view for view in all_operational_views() if normalized in view.allowed_source_families)


def operational_views_for_tab(tab_name: object, *, selectable_only: bool = False) -> tuple[OperationalViewDefinition, ...]:
    wanted = str(tab_name or "").strip().lower()
    views = tuple(view for view in all_operational_views() if view.default_tab.lower() == wanted)
    if selectable_only:
        views = tuple(view for view in views if view.supports_user_selection)
    return views


def controlfreq_view_presets() -> dict[str, dict[str, bool]]:
    return {preset: dict(cards) for preset, cards in CONTROLFREQ_PRESETS.items()}


def controlfreq_preset_names(*, include_custom: bool = True) -> tuple[str, ...]:
    names = tuple(CONTROLFREQ_PRESETS)
    return names + (("Custom",) if include_custom else ())


def controlfreq_view_labels() -> tuple[tuple[str, str], ...]:
    return tuple((key, operational_view_for(key).label) for key in CONTROLFREQ_VIEW_KEYS)


def validate_view_sources(
    view_key: object,
    source_families: Sequence[object] | None = None,
) -> tuple[ViewGateReport, ...]:
    view = operational_view_for(view_key)
    families = tuple(normalize_source_family(family) for family in (source_families or view.allowed_source_families))
    reports: list[ViewGateReport] = []
    for family in families:
        if family not in view.allowed_source_families:
            reports.append(ViewGateReport(view.key, family, ("source_not_allowed",)))
            continue
        contract = source_contract_for(family)
        gate_summary = contract.gate_summary()
        failures = tuple(gate for gate in view.required_gates if not bool(gate_summary.get(gate, False)))
        reports.append(ViewGateReport(view.key, family, failures))
    return tuple(reports)
