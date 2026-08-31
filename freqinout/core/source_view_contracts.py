from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

from freqinout.core.protocol_capabilities import protocol_capabilities_for
from freqinout.core.view_contracts import DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS


@dataclass(frozen=True)
class RetentionContract:
    default_window_seconds: int = DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS
    default_visible_limit: int = 50
    archive_allowed: bool = True
    active_storyline_override: bool = True
    rollup_required: bool = False


@dataclass(frozen=True)
class ProvenanceContract:
    label: str = "unknown"
    show_age: bool = True
    show_confidence: bool = False
    show_relay_path: bool = False
    conflict_policy: str = "show_source_and_age"


@dataclass(frozen=True)
class MapScalingContract:
    geometry: tuple[str, ...] = ()
    auto_fit: bool = False
    clustering_required: bool = False
    default_marker_limit: int = 100
    broad_location_policy: str = "show_filtered_context"


@dataclass(frozen=True)
class ActionContract:
    read: bool = True
    reply: bool = False
    map: bool = False
    compose: bool = False
    pin: bool = True
    tune: bool = False
    open_source: bool = False
    acknowledge: bool = False

    def enabled_names(self) -> tuple[str, ...]:
        return tuple(name for name in self.__dataclass_fields__ if bool(getattr(self, name)))


@dataclass(frozen=True)
class SourceViewContract:
    family: str
    display_name: str
    source_specific_fields: tuple[str, ...]
    common_fields: tuple[str, ...]
    drilldown_fields: tuple[str, ...] = ()
    retention: RetentionContract = field(default_factory=RetentionContract)
    provenance: ProvenanceContract = field(default_factory=ProvenanceContract)
    map_scaling: MapScalingContract = field(default_factory=MapScalingContract)
    actions: ActionContract = field(default_factory=ActionContract)
    allowed_views: tuple[str, ...] = ()
    default_view: str = ""
    notes: str = ""

    def supports_view(self, view_name: object) -> bool:
        wanted = normalize_view_name(view_name)
        return wanted in {normalize_view_name(view) for view in self.allowed_views}

    def gate_summary(self) -> Mapping[str, bool]:
        return {
            "source_meaning": bool(self.source_specific_fields and self.common_fields),
            "volume_retention": self.retention.default_window_seconds > 0 and self.retention.default_visible_limit > 0,
            "provenance_trust": bool(self.provenance.label),
            "constrained_customization": bool(self.allowed_views and self.default_view),
            "map_scaling": bool(self.map_scaling.geometry) or not self.actions.map,
            "action_validity": bool(self.actions.enabled_names()),
        }

    @property
    def complete(self) -> bool:
        return all(self.gate_summary().values())


COMMON_TRAFFIC_FIELDS = (
    "source_family",
    "source_ref",
    "received_time",
    "event_time",
    "from_actor",
    "to_target",
    "group",
    "subject",
    "summary",
    "topics",
    "severity",
)


VIEW_ATTENTION = "attention_queue"
VIEW_TRAFFIC_INBOX = "traffic_inbox"
VIEW_MAP_CONTEXT = "map_context"
VIEW_COMPOSE = "compose_workbench"
VIEW_SCHEDULE = "schedule_outlook"
VIEW_RF_READINESS = "rf_readiness"
VIEW_SETUP = "setup_checklist"
VIEW_STATION_COMMAND = "station_command"
VIEW_STATION_CONTROL_CENTER = "station_control_center"
VIEW_OPERATOR_DIRECTORY = "operator_directory"
VIEW_NET_CONTROL = "net_control"


def normalize_source_family(value: object) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_").replace("/", "_")
    aliases = {
        "js8": "js8call",
        "js8call": "js8call",
        "js8spotter": "fiospotter",
        "spotter": "fiospotter",
        "fiospotter": "fiospotter",
        "commstat_rf": "commstat",
        "commstat": "commstat",
        "varac_bbs": "varac",
        "bbs": "varac",
        "local_report": "local",
        "local": "local",
        "mesh": "meshcore",
        "meshcore": "meshcore",
        "mesh_mqtt": "mqtt",
        "meshmqtt": "mqtt",
        "meshtastic_mqtt": "mqtt",
        "mqtt": "mqtt",
        "lxmf": "reticulum",
        "reticulum_lxmf": "reticulum",
        "reticulum": "reticulum",
        "aprs": "aprs",
    }
    return aliases.get(raw, raw or "unknown")


def normalize_view_name(value: object) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def source_contract_for(family: object) -> SourceViewContract:
    key = normalize_source_family(family)
    return _SOURCE_CONTRACTS.get(key, _unknown_contract(key))


def all_source_contracts() -> tuple[SourceViewContract, ...]:
    priority = ("meshcore", "mqtt", "aprs", "reticulum")
    ordered = [source_contract_for(key) for key in priority if key in _SOURCE_CONTRACTS]
    ordered.extend(contract for key, contract in _SOURCE_CONTRACTS.items() if key not in priority)
    return tuple(ordered)


def contracts_for_view(view_name: object) -> tuple[SourceViewContract, ...]:
    wanted = normalize_view_name(view_name)
    return tuple(contract for contract in all_source_contracts() if contract.supports_view(wanted))


def contract_gate_failures(contract: SourceViewContract) -> tuple[str, ...]:
    return tuple(name for name, passed in contract.gate_summary().items() if not passed)


def source_contracts_missing_gates(contracts: Sequence[SourceViewContract] | None = None) -> Mapping[str, tuple[str, ...]]:
    result: dict[str, tuple[str, ...]] = {}
    for contract in contracts or all_source_contracts():
        failures = contract_gate_failures(contract)
        if failures:
            result[contract.family] = failures
    return result


def _actions_for_family(family: str) -> ActionContract:
    caps = protocol_capabilities_for(family)
    return ActionContract(
        read=bool(caps.receive_messages or caps.receive_reports or caps.bbs_read),
        reply=bool(caps.send_message),
        map=bool(caps.location or caps.topology),
        compose=bool(caps.send_message or caps.send_form),
        pin=True,
        tune=bool(caps.frequency_control),
        open_source=bool(caps.launch_control or caps.bbs_read),
        acknowledge=family in {"meshcore", "mqtt", "aprs", "reticulum", "commstat", "fiospotter"},
    )


def _unknown_contract(family: str) -> SourceViewContract:
    return SourceViewContract(
        family=family or "unknown",
        display_name=family or "Unknown",
        source_specific_fields=(),
        common_fields=COMMON_TRAFFIC_FIELDS,
        provenance=ProvenanceContract(label="unknown", show_confidence=True),
        actions=ActionContract(read=True, pin=True),
        allowed_views=(VIEW_TRAFFIC_INBOX,),
        default_view=VIEW_TRAFFIC_INBOX,
        notes="Unknown sources must complete source-specific gates before default operational display.",
    )


_SOURCE_CONTRACTS: dict[str, SourceViewContract] = {
    "meshcore": SourceViewContract(
        family="meshcore",
        display_name="MeshCore",
        source_specific_fields=("room", "node_id", "hop_count", "ack_state", "channel", "path"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("raw_packet", "node_capabilities", "store_forward_state"),
        retention=RetentionContract(default_visible_limit=75, rollup_required=True),
        provenance=ProvenanceContract(label="rf", show_confidence=True, show_relay_path=True),
        map_scaling=MapScalingContract(geometry=("lat_lon", "grid", "node", "path"), auto_fit=True, clustering_required=True),
        actions=_actions_for_family("meshcore"),
        allowed_views=(
            VIEW_ATTENTION,
            VIEW_TRAFFIC_INBOX,
            VIEW_MAP_CONTEXT,
            VIEW_COMPOSE,
            VIEW_OPERATOR_DIRECTORY,
            VIEW_STATION_CONTROL_CENTER,
        ),
        default_view=VIEW_ATTENTION,
        notes="Highest-priority future source for offline operator network awareness.",
    ),
    "mqtt": SourceViewContract(
        family="mqtt",
        display_name="Mesh MQTT",
        source_specific_fields=("topic_path", "broker", "node_id", "payload_type", "bridge_id"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("raw_payload", "broker_metadata", "bridge_policy"),
        retention=RetentionContract(default_visible_limit=60, rollup_required=True),
        provenance=ProvenanceContract(label="internet_or_bridge", show_confidence=True, show_relay_path=True),
        map_scaling=MapScalingContract(geometry=("lat_lon", "grid", "topic_region"), auto_fit=True, clustering_required=True),
        actions=_actions_for_family("mqtt"),
        allowed_views=(VIEW_ATTENTION, VIEW_TRAFFIC_INBOX, VIEW_MAP_CONTEXT, VIEW_COMPOSE, VIEW_STATION_CONTROL_CENTER),
        default_view=VIEW_ATTENTION,
        notes="Requires explicit bridge/trust labeling because data may be internet-backed.",
    ),
    "aprs": SourceViewContract(
        family="aprs",
        display_name="APRS",
        source_specific_fields=("packet_type", "object_name", "symbol", "path", "digipeaters", "weather"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("raw_packet", "telemetry", "weather_fields", "path_detail"),
        retention=RetentionContract(default_visible_limit=100, rollup_required=True),
        provenance=ProvenanceContract(label="rf_or_is", show_confidence=True, show_relay_path=True),
        map_scaling=MapScalingContract(
            geometry=("lat_lon", "object", "weather", "path"),
            auto_fit=True,
            clustering_required=True,
            default_marker_limit=250,
        ),
        actions=ActionContract(read=True, reply=False, map=True, compose=False, pin=True, open_source=True, acknowledge=True),
        allowed_views=(VIEW_ATTENTION, VIEW_TRAFFIC_INBOX, VIEW_MAP_CONTEXT, VIEW_STATION_CONTROL_CENTER),
        default_view=VIEW_MAP_CONTEXT,
        notes="Primary stress test for volume, map clustering, stale packets, and object/weather semantics.",
    ),
    "reticulum": SourceViewContract(
        family="reticulum",
        display_name="Reticulum/LXMF",
        source_specific_fields=("destination_hash", "identity_hash", "delivery_state", "lxmf_fields", "path"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("raw_lxmf", "identity_detail", "store_forward_metadata"),
        retention=RetentionContract(default_visible_limit=50, rollup_required=True),
        provenance=ProvenanceContract(label="mesh_or_store_forward", show_confidence=True, show_relay_path=True),
        map_scaling=MapScalingContract(geometry=("node", "declared_grid", "path"), auto_fit=True, clustering_required=True),
        actions=_actions_for_family("reticulum"),
        allowed_views=(
            VIEW_ATTENTION,
            VIEW_TRAFFIC_INBOX,
            VIEW_MAP_CONTEXT,
            VIEW_COMPOSE,
            VIEW_OPERATOR_DIRECTORY,
            VIEW_STATION_CONTROL_CENTER,
        ),
        default_view=VIEW_TRAFFIC_INBOX,
        notes="Store-and-forward identity context should enter through message/operator projections first.",
    ),
    "js8call": SourceViewContract(
        family="js8call",
        display_name="JS8Call",
        source_specific_fields=("offset", "snr", "directed_command", "heartbeat", "msg_auth_state"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("raw_line", "all_txt_ref", "inbox_ref"),
        retention=RetentionContract(default_visible_limit=80),
        provenance=ProvenanceContract(label="rf", show_confidence=True),
        map_scaling=MapScalingContract(geometry=("grid", "callsign", "path"), auto_fit=True),
        actions=_actions_for_family("js8call"),
        allowed_views=(
            VIEW_ATTENTION,
            VIEW_TRAFFIC_INBOX,
            VIEW_MAP_CONTEXT,
            VIEW_COMPOSE,
            VIEW_NET_CONTROL,
            VIEW_STATION_CONTROL_CENTER,
        ),
        default_view=VIEW_TRAFFIC_INBOX,
    ),
    "fiospotter": SourceViewContract(
        family="fiospotter",
        display_name="FIOSpotter",
        source_specific_fields=("form_id", "form_name", "expect_state", "msg_auth_state", "bbs_destination"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("raw_form_fields", "expect_record", "generated_payload"),
        retention=RetentionContract(default_visible_limit=60),
        provenance=ProvenanceContract(label="rf_or_imported", show_confidence=True),
        map_scaling=MapScalingContract(geometry=("grid", "state", "region"), auto_fit=True),
        actions=_actions_for_family("js8call"),
        allowed_views=(VIEW_ATTENTION, VIEW_TRAFFIC_INBOX, VIEW_MAP_CONTEXT, VIEW_COMPOSE, VIEW_STATION_CONTROL_CENTER),
        default_view=VIEW_ATTENTION,
    ),
    "commstat": SourceViewContract(
        family="commstat",
        display_name="CommStat RF",
        source_specific_fields=("statrep_type", "brevity_code", "asset_status", "scope", "report_id"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("decoded_brevity", "raw_payload", "event_list_ref"),
        retention=RetentionContract(default_visible_limit=80),
        provenance=ProvenanceContract(label="rf_or_mixed", show_confidence=True),
        map_scaling=MapScalingContract(geometry=("grid", "state", "region"), auto_fit=True),
        actions=ActionContract(
            read=True,
            reply=True,
            map=True,
            compose=True,
            pin=True,
            open_source=True,
            acknowledge=True,
        ),
        allowed_views=(VIEW_ATTENTION, VIEW_TRAFFIC_INBOX, VIEW_MAP_CONTEXT, VIEW_COMPOSE, VIEW_STATION_CONTROL_CENTER),
        default_view=VIEW_ATTENTION,
    ),
    "flmsg": SourceViewContract(
        family="flmsg",
        display_name="FLMsg",
        source_specific_fields=("form_family", "form_name", "staged_path", "priority", "message_folder"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("k2s_path", "html_path", "raw_form_fields"),
        retention=RetentionContract(default_visible_limit=50),
        provenance=ProvenanceContract(label="rf_file", show_confidence=False),
        map_scaling=MapScalingContract(geometry=("grid", "state", "form_location"), auto_fit=True),
        actions=_actions_for_family("flmsg"),
        allowed_views=(VIEW_ATTENTION, VIEW_TRAFFIC_INBOX, VIEW_MAP_CONTEXT, VIEW_COMPOSE, VIEW_STATION_CONTROL_CENTER),
        default_view=VIEW_TRAFFIC_INBOX,
    ),
    "flamp": SourceViewContract(
        family="flamp",
        display_name="FLAmp",
        source_specific_fields=("broadcast_id", "block_count", "completion_state", "staged_path"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("rx_manifest", "missing_blocks", "raw_form_fields"),
        retention=RetentionContract(default_visible_limit=50),
        provenance=ProvenanceContract(label="rf_file", show_confidence=False),
        map_scaling=MapScalingContract(geometry=("grid", "state", "form_location"), auto_fit=True),
        actions=_actions_for_family("flamp"),
        allowed_views=(VIEW_ATTENTION, VIEW_TRAFFIC_INBOX, VIEW_MAP_CONTEXT, VIEW_COMPOSE, VIEW_STATION_CONTROL_CENTER),
        default_view=VIEW_TRAFFIC_INBOX,
    ),
    "varac": SourceViewContract(
        family="varac",
        display_name="VarAC",
        source_specific_fields=("message_id", "bbs_folder", "store_forward_state", "varac_db_ref"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("raw_db_row", "bbs_file_ref", "contact_history"),
        retention=RetentionContract(default_visible_limit=60),
        provenance=ProvenanceContract(label="rf_store_forward", show_confidence=True),
        map_scaling=MapScalingContract(geometry=("callsign", "grid", "bbs_node"), auto_fit=True),
        actions=_actions_for_family("varac"),
        allowed_views=(VIEW_ATTENTION, VIEW_TRAFFIC_INBOX, VIEW_MAP_CONTEXT, VIEW_COMPOSE, VIEW_STATION_CONTROL_CENTER),
        default_view=VIEW_TRAFFIC_INBOX,
    ),
    "local": SourceViewContract(
        family="local",
        display_name="Local Reports",
        source_specific_fields=("operator_id", "ncs_session", "verified_local", "report_channel"),
        common_fields=COMMON_TRAFFIC_FIELDS,
        drilldown_fields=("local_form", "operator_record", "ncs_log_ref"),
        retention=RetentionContract(default_visible_limit=50, active_storyline_override=True),
        provenance=ProvenanceContract(label="local_or_manual", show_confidence=True),
        map_scaling=MapScalingContract(geometry=("lat_lon", "grid", "state"), auto_fit=True),
        actions=_actions_for_family("local"),
        allowed_views=(
            VIEW_ATTENTION,
            VIEW_TRAFFIC_INBOX,
            VIEW_MAP_CONTEXT,
            VIEW_COMPOSE,
            VIEW_NET_CONTROL,
            VIEW_STATION_CONTROL_CENTER,
        ),
        default_view=VIEW_ATTENTION,
    ),
}
