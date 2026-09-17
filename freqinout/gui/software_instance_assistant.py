"""Guided, cache-only Software instance setup surface.

The assistant deliberately stops at a reviewed payload.  Settings (and the
multi-radio store) remain the persistence and discovery authorities.  This
keeps opening the assistant safe and makes it possible for a host to add its
own discovery adapter without teaching a widget about databases, files, or
processes.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import re
from typing import Any, Iterable, Mapping, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from freqinout.gui.current_page_stack import CurrentPageStack
from freqinout.gui.theme import active_app_theme, button_height_for_font, button_style, label_style


SUPPORTED_INSTANCE_FAMILIES: tuple[tuple[str, str], ...] = (
    ("js8call", "JS8Call"),
    ("fast_light", "Fast Light"),
    ("varac", "VarAC"),
)

_DEFAULT_PORTS = {"js8call": 2442, "fast_light": 12345, "varac": 0}
_FAMILY_DETAILS = {
    "js8call": ("JS8Call instance", "API endpoint, profile, message storage, and launch details."),
    "fast_light": ("Fast Light instance", "FLRig/FLDigi endpoints, application paths, and message folders."),
    "varac": ("VarAC instance", "Runtime, configuration, inbox/outbox, and optional cluster ownership."),
}
_FAMILY_FIELDS = {
    "js8call": frozenset(
        {"instance_name", "ownership", "variant", "version", "rig_name", "host", "port", "udp_port", "application_path", "configuration_path", "storage_path", "launch_command", "launch_at_startup", "notes"}
    ),
    "fast_light": frozenset(
        {"instance_name", "ownership", "host", "port", "secondary_port", "application_path", "secondary_application_path", "flmsg_application_path", "flamp_application_path", "configuration_path", "secondary_configuration_path", "storage_path", "secondary_storage_path", "launch_command", "launch_at_startup", "advanced_tx_requested", "advanced_tx_acknowledged", "notes"}
    ),
    "varac": frozenset(
        {"instance_name", "ownership", "application_path", "configuration_path", "storage_path", "secondary_storage_path", "outbox_path", "working_directory", "cluster_path", "cluster_id", "cluster_name", "cluster_shared_database", "cluster_instance_number", "cluster_gateway", "cluster_ptt_lock", "launch_command", "launch_at_startup", "notes"}
    ),
}
_FAMILY_FIELD_LABELS = {
    "js8call": {
        "variant": "JS8Call variant",
        "version": "Verified application version",
        "rig_name": "JS8Call rig name",
        "host": "JS8Call API host",
        "port": "JS8Call TCP API port",
        "udp_port": "JS8Call UDP port",
        "application_path": "JS8Call application",
        "configuration_path": "JS8Call settings file",
        "storage_path": "JS8Call application-data folder",
        "launch_command": "Custom launch command (advanced)",
    },
    "fast_light": {
        "host": "Local service host",
        "port": "FLRig XML-RPC port",
        "secondary_port": "FLDigi XML-RPC port",
        "application_path": "FLRig application",
        "secondary_application_path": "FLDigi application",
        "flmsg_application_path": "FLMsg application",
        "flamp_application_path": "FLAmp application",
        "configuration_path": "FLRig profile folder",
        "secondary_configuration_path": "FLDigi profile folder",
        "storage_path": "FLDigi log folder",
        "secondary_storage_path": "FLDigi check-in folder",
        "launch_command": "FLDigi custom launch command (advanced)",
    },
    "varac": {
        "application_path": "VarAC application / launcher",
        "configuration_path": "VarAC INI file",
        "storage_path": "VarAC database",
        "secondary_storage_path": "VarAC incoming folder",
        "outbox_path": "VarAC outbox folder",
        "working_directory": "VarAC working directory",
        "cluster_path": "Cluster setup",
        "cluster_id": "VarAC cluster",
        "cluster_name": "New cluster name",
        "cluster_shared_database": "Cluster shared database",
        "cluster_instance_number": "Cluster instance number",
        "launch_command": "Instance-specific launch command",
    },
}


@dataclass(frozen=True)
class InstanceConflict:
    """A deterministic review finding; it does not perform a check itself."""

    code: str
    severity: str
    title: str
    detail: str


@dataclass(frozen=True)
class SoftwareInstanceDraft:
    """Stable UI-to-host payload for one new or imported instance."""

    family_key: str = ""
    instance_name: str = ""
    radio_id: Optional[int] = None
    owner_draft_key: str = ""
    owner_label: str = ""
    radio_role: str = "tx_rx"
    mode: str = "managed"
    ownership: str = "fio-managed"
    variant: str = ""
    version: str = ""
    writer_platform: str = ""
    writer_operation: str = "create"
    host: str = "127.0.0.1"
    port: int = 0
    udp_port: int = 0
    secondary_port: int = 0
    rig_name: str = ""
    application_path: str = ""
    secondary_application_path: str = ""
    flmsg_application_path: str = ""
    flamp_application_path: str = ""
    configuration_path: str = ""
    secondary_configuration_path: str = ""
    storage_path: str = ""
    secondary_storage_path: str = ""
    outbox_path: str = ""
    launch_command: str = ""
    launch_at_startup: bool = False
    working_directory: str = ""
    advanced_tx_requested: bool = False
    advanced_tx_acknowledged: bool = False
    cluster_path: str = "standalone"
    cluster_id: str = ""
    cluster_name: str = ""
    cluster_shared_database: str = ""
    cluster_instance_number: int = 0
    cluster_gateway: bool = False
    cluster_ptt_lock: bool = False
    notes: str = ""
    imported_id: Optional[int] = None
    imported_system_key: str = ""
    replace_existing: bool = False
    replacement_instance_id: Optional[int] = None

    def payload(self) -> dict[str, Any]:
        """Return a JSON-friendly copy with stable keys for persistence adapters."""
        # The first keys are the UI contract.  The explicit manifest aliases
        # let a Settings adapter hand this payload to the additive core model
        # without losing the operator-friendly names shown in Review.
        management_mode = {
            "fio-managed": "fio_managed",
            "operator-managed": "operator",
            "remote": "remote",
        }.get(self.ownership, "operator")
        provenance = "detected" if self.mode == "discover" else ("managed" if self.mode == "managed" else "manual")
        ports: list[dict[str, Any]] = []
        resources: list[dict[str, Any]] = []
        if self.family_key == "js8call":
            if self.port:
                ports.append({"name": "JS8Call API", "protocol": "tcp", "host": self.host, "port": self.port})
            if self.udp_port:
                ports.append({"name": "JS8Call UDP", "protocol": "udp", "host": self.host, "port": self.udp_port})
            if self.configuration_path:
                resources.append({"kind": "settings_profile", "value": self.configuration_path, "exclusive": True})
            if self.storage_path:
                resources.append({"kind": "message_storage", "value": self.storage_path, "exclusive": True})
            if self.rig_name:
                resources.append({"kind": "rig_name", "value": self.rig_name, "exclusive": True})
        elif self.family_key == "fast_light":
            if self.port:
                ports.append({"name": "FLRig XML-RPC", "protocol": "tcp", "host": self.host, "port": self.port})
            if self.secondary_port:
                ports.append({"name": "FLDigi XML-RPC", "protocol": "tcp", "host": self.host, "port": self.secondary_port})
            for kind, value in (
                ("flrig_configuration", self.configuration_path),
                ("fldigi_configuration", self.secondary_configuration_path),
                ("fldigi_logs", self.storage_path),
                ("fldigi_checkins", self.secondary_storage_path),
                ("flmsg_application", self.flmsg_application_path),
                ("flamp_application", self.flamp_application_path),
            ):
                if value:
                    resources.append(
                        {
                            "kind": kind,
                            "value": value,
                            "exclusive": kind not in {"flmsg_application", "flamp_application"},
                        }
                    )
        else:
            for kind, value in (
                ("varac_ini", self.configuration_path),
                ("varac_database", self.storage_path),
                ("varac_incoming", self.secondary_storage_path),
                ("varac_outbox", self.outbox_path),
            ):
                if value:
                    resources.append({"kind": kind, "value": value, "exclusive": True})
            if self.cluster_id and self.cluster_instance_number:
                resources.append(
                    {
                        "kind": "varac_cluster_instance",
                        "value": f"{self.cluster_id}:{self.cluster_instance_number}",
                        "exclusive": True,
                    }
                )
        payload = {
            "family_key": self.family_key,
            "instance_name": self.instance_name,
            "radio_id": self.radio_id,
            "mode": self.mode,
            "ownership": self.ownership,
            "management_mode": management_mode,
            "provenance": provenance,
            "variant": self.variant,
            "version": self.version,
            "writer_platform": self.writer_platform,
            "writer_operation": self.writer_operation,
            "owner_draft_key": self.owner_draft_key,
            "owner_label": self.owner_label,
            "radio_role": self.radio_role,
            "instance_key": (
                f"{self.family_key}:{self.imported_system_key}"
                if self.imported_system_key
                else (f"{self.family_key}:{self.imported_id}" if self.imported_id else "")
            ),
            "application_system_key": self.imported_system_key,
            "host": self.host,
            "port": int(self.port or 0),
            "udp_port": int(self.udp_port or 0),
            "secondary_port": int(self.secondary_port or 0),
            "rig_name": self.rig_name,
            "ports": ports,
            "application_path": self.application_path,
            "secondary_application_path": self.secondary_application_path,
            "flmsg_application_path": self.flmsg_application_path,
            "flamp_application_path": self.flamp_application_path,
            "executable_path": self.application_path,
            "configuration_path": self.configuration_path,
            "configuration_root": self.configuration_path,
            "secondary_configuration_path": self.secondary_configuration_path,
            "storage_path": self.storage_path,
            "data_root": self.storage_path,
            "secondary_storage_path": self.secondary_storage_path,
            "outbox_path": self.outbox_path,
            "launch_command": self.launch_command,
            "launch_at_startup": self.launch_at_startup,
            "working_directory": self.working_directory,
            "advanced_tx_requested": self.advanced_tx_requested,
            "advanced_tx_acknowledged": self.advanced_tx_acknowledged,
            "execution_scope": "receive_only" if self.radio_role == "observer" else "standard",
            "cluster_path": self.cluster_path,
            "cluster_id": self.cluster_id,
            "cluster_name": self.cluster_name,
            "cluster_shared_database": self.cluster_shared_database,
            "cluster_instance_number": int(self.cluster_instance_number or 0),
            "cluster_gateway": self.cluster_gateway,
            "cluster_ptt_lock": self.cluster_ptt_lock,
            "resource_claims": resources,
            "notes": self.notes,
            "imported_id": self.imported_id,
            "imported_system_key": self.imported_system_key,
            "replace_existing": bool(self.replace_existing),
            "replacement_instance_id": self.replacement_instance_id,
        }
        if self.family_key == "js8call":
            payload.update(
                {
                    "js8_rig_name": self.rig_name,
                    "js8_variant_family": self.variant,
                    "js8_variant_version": self.version,
                    "js8_tcp_port": int(self.port or 0),
                    "js8_udp_port": int(self.udp_port or 0),
                    "js8_application_path": self.application_path,
                    "js8_profile_path": self.configuration_path,
                    "js8_data_path": self.storage_path,
                }
            )
        elif self.family_key == "fast_light":
            payload.update(
                {
                    "flrig_port": int(self.port or 0),
                    "fldigi_port": int(self.secondary_port or 0),
                    "flrig_path": self.application_path,
                    "fldigi_path": self.secondary_application_path,
                    "flmsg_path": self.flmsg_application_path,
                    "flamp_path": self.flamp_application_path,
                    "flrig_config_path": self.configuration_path,
                    "fldigi_config_path": self.secondary_configuration_path,
                    "fldigi_log_path": self.storage_path,
                    "fldigi_checkin_dir": self.secondary_storage_path,
                }
            )
        else:
            payload.update(
                {
                    "varac_install_path": self.application_path,
                    "varac_ini_path": self.configuration_path,
                    "varac_db_path": self.storage_path,
                    "varac_incoming_path": self.secondary_storage_path,
                    "varac_outbox_dir": self.outbox_path,
                    "varac_cluster_id": self.cluster_id,
                    "varac_cluster_instance_number": int(self.cluster_instance_number or 0),
                    "varac_cluster_path": self.cluster_path,
                    "varac_cluster_name": self.cluster_name,
                    "varac_cluster_shared_database": self.cluster_shared_database,
                }
            )
        return payload


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> Optional[int]:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() not in {"", "0", "false", "no", "off"}
    return bool(value)


def normalize_instance_draft(value: Mapping[str, Any] | SoftwareInstanceDraft) -> SoftwareInstanceDraft:
    """Normalize host/discovery data without probing or changing it."""

    if isinstance(value, SoftwareInstanceDraft):
        return value
    row = dict(value or {})
    family = _text(row.get("family_key") or row.get("family")).lower()
    provenance = _text(row.get("provenance")).lower()
    mode = _text(row.get("mode") or ("discover" if provenance == "detected" else "managed")).lower()
    ownership = _text(
        row.get("ownership") or ("operator-managed" if mode == "discover" else "fio-managed")
    ).lower()
    family_storage = (
        row.get("varac_db_path") or row.get("db_path")
        if family == "varac"
        else row.get("js8_data_path") or row.get("fldigi_log_path") or row.get("data_path") or row.get("incoming_path")
    )
    family_secondary_storage = (
        row.get("varac_incoming_path") or row.get("incoming_path")
        if family == "varac"
        else row.get("fldigi_checkin_dir")
    )
    return SoftwareInstanceDraft(
        family_key=family,
        instance_name=_text(row.get("instance_name") or row.get("name")),
        radio_id=_int(row.get("radio_id")),
        owner_draft_key=_text(row.get("owner_draft_key")),
        owner_label=_text(row.get("owner_label")),
        radio_role=_text(row.get("radio_role") or row.get("device_class") or "tx_rx").lower(),
        mode=mode,
        ownership=ownership,
        variant=_text(row.get("variant") or row.get("variant_family") or row.get("js8_variant_family")),
        version=_text(row.get("version") or row.get("variant_version") or row.get("js8_variant_version")),
        writer_platform=_text(row.get("writer_platform") or row.get("js8_writer_platform")),
        writer_operation=_text(row.get("writer_operation") or row.get("js8_writer_operation") or "create").lower(),
        host=_text(row.get("host") or "127.0.0.1"),
        port=_int(row.get("port") or row.get("js8_tcp_port") or row.get("flrig_port")) or 0,
        udp_port=_int(row.get("udp_port") or row.get("js8_udp_port")) or 0,
        secondary_port=_int(row.get("secondary_port") or row.get("fldigi_port")) or 0,
        rig_name=_text(row.get("rig_name") or row.get("js8_rig_name")),
        application_path=_text(row.get("application_path") or row.get("install_path") or row.get("path") or row.get("js8_application_path") or row.get("flrig_path") or row.get("varac_install_path")),
        secondary_application_path=_text(row.get("secondary_application_path") or row.get("fldigi_path")),
        flmsg_application_path=_text(row.get("flmsg_application_path") or row.get("flmsg_path")),
        flamp_application_path=_text(row.get("flamp_application_path") or row.get("flamp_path")),
        configuration_path=_text(row.get("configuration_path") or row.get("ini_path") or row.get("profile_path") or row.get("js8_profile_path") or row.get("flrig_config_path") or row.get("varac_ini_path")),
        secondary_configuration_path=_text(row.get("secondary_configuration_path") or row.get("fldigi_config_path")),
        storage_path=_text(row.get("storage_path") or family_storage),
        secondary_storage_path=_text(row.get("secondary_storage_path") or family_secondary_storage),
        outbox_path=_text(row.get("outbox_path") or row.get("outbox_dir") or row.get("varac_outbox_dir")),
        launch_command=_text(row.get("launch_command") or row.get("launch_cmd")),
        launch_at_startup=_bool(row.get("launch_at_startup", False)),
        working_directory=_text(row.get("working_directory") or row.get("working_dir")),
        advanced_tx_requested=_bool(row.get("advanced_tx_requested", False)),
        advanced_tx_acknowledged=_bool(row.get("advanced_tx_acknowledged", False)),
        cluster_path=_text(row.get("cluster_path") or row.get("varac_cluster_path") or "standalone").lower(),
        cluster_id=_text(row.get("cluster_id") or row.get("varac_cluster_id")),
        cluster_name=_text(row.get("cluster_name") or row.get("varac_cluster_name")),
        cluster_shared_database=_text(
            row.get("cluster_shared_database") or row.get("varac_cluster_shared_database")
        ),
        cluster_instance_number=_int(row.get("cluster_instance_number") or row.get("varac_cluster_instance_number")) or 0,
        cluster_gateway=_bool(row.get("cluster_gateway", False)),
        cluster_ptt_lock=_bool(row.get("cluster_ptt_lock", False)),
        notes=_text(row.get("notes")),
        imported_id=_int(row.get("imported_id") or row.get("id")),
        imported_system_key=_text(row.get("imported_system_key") or row.get("system_key")),
        replace_existing=_bool(row.get("replace_existing", False)),
        replacement_instance_id=_int(row.get("replacement_instance_id")),
    )


def _endpoint(row: Mapping[str, Any]) -> tuple[str, int]:
    host = _text(row.get("host") or row.get("flrig_host") or row.get("fldigi_host"))
    port = _int(row.get("port") or row.get("flrig_port") or row.get("fldigi_port")) or 0
    return host, port


def instance_conflicts(
    draft: Mapping[str, Any] | SoftwareInstanceDraft,
    existing_instances: Iterable[Mapping[str, Any]] = (),
) -> tuple[InstanceConflict, ...]:
    """Return name/endpoint/path conflicts against an already-loaded inventory."""

    current = normalize_instance_draft(draft)
    conflicts: list[InstanceConflict] = []
    if not current.family_key:
        conflicts.append(InstanceConflict("family_required", "error", "Choose software", "Select JS8Call, Fast Light, or VarAC."))
    if not current.instance_name:
        conflicts.append(InstanceConflict("name_required", "error", "Name this instance", "Use a short name that distinguishes this instance from the others."))
    if current.radio_id is None and not current.owner_draft_key:
        conflicts.append(InstanceConflict("radio_required", "error", "Choose a radio", "Every software instance must be assigned to a radio before it can be added."))
    if current.mode == "remote" and not current.host:
        conflicts.append(InstanceConflict("host_required", "error", "Remote host required", "Enter the host name or address for the remote application."))
    for port_name, port in (
        ("TCP", current.port),
        ("UDP", current.udp_port),
        ("secondary", current.secondary_port),
    ):
        if port and not 1 <= port <= 65535:
            conflicts.append(InstanceConflict("port_invalid", "error", f"{port_name} port is invalid", "Use a port from 1 through 65535."))
    if current.family_key == "js8call" and current.mode == "managed" and not current.rig_name:
        severity = "error" if current.launch_at_startup else "warning"
        conflicts.append(InstanceConflict("rig_name_required", severity, "JS8 rig name is not set", "Each independently launched JS8Call instance needs a unique --rig-name."))
    if current.mode == "managed" and not current.application_path:
        conflicts.append(InstanceConflict("application_missing", "warning", "Application path is not set", "The instance can be saved as a draft, but it cannot be launched until its application is configured."))
    if current.launch_at_startup and not (current.application_path or current.launch_command):
        conflicts.append(InstanceConflict("launch_target_required", "error", "Launch target is not set", "Choose an application or an explicit launch command before enabling startup."))
    if current.family_key == "fast_light":
        observer_mode = current.radio_role == "observer"
        if observer_mode and current.application_path:
            conflicts.append(
                InstanceConflict(
                    "observer_fast_light_flrig",
                    "error",
                    "FLRig is unavailable for a receive-only radio",
                    "Use FLDigi and approved receive/file helpers only; CAT, PTT, and transmit controls remain disabled.",
                )
            )
        if observer_mode and current.advanced_tx_requested:
            conflicts.append(
                InstanceConflict(
                    "observer_fast_light_tx",
                    "error",
                    "Advanced TX is unavailable",
                    "An observer / SDR can never receive Fast Light transmit authority.",
                )
            )
        if current.advanced_tx_requested and not current.advanced_tx_acknowledged:
            conflicts.append(
                InstanceConflict(
                    "fast_light_tx_acknowledgement",
                    "error",
                    "Advanced TX acknowledgement is required",
                    "Acknowledge the operating-model, RF Guard, and final-preflight requirements or leave Fast Light receive-safe.",
                )
            )
        if current.host and current.port and current.port == current.secondary_port:
            conflicts.append(
                InstanceConflict(
                    "fast_light_endpoint_overlap",
                    "error",
                    "FLRig and FLDigi endpoints overlap",
                    "Assign different local TCP ports to FLRig and FLDigi.",
                )
            )
        missing_launch_profile = (
            not current.secondary_configuration_path
            if observer_mode
            else not current.configuration_path or not current.secondary_configuration_path
        )
        if current.launch_at_startup and missing_launch_profile:
            conflicts.append(
                InstanceConflict(
                    "fast_light_profile_required",
                    "error",
                    "Fast Light profile folders are required",
                    (
                        "Startup needs a distinct FLDigi configuration folder so the receive-only instance cannot reuse the default profile."
                        if observer_mode
                        else "Startup needs distinct FLRig and FLDigi configuration folders so a second instance cannot reuse the default profile."
                    ),
                )
            )
    if current.family_key == "varac":
        if current.radio_role == "observer":
            conflicts.append(
                InstanceConflict(
                    "observer_varac_forbidden",
                    "error",
                    "VarAC is unavailable for this radio",
                    "Observer / SDR profiles cannot use standalone or Cluster VarAC.",
                )
            )
        if current.cluster_path not in {"standalone", "create_cluster", "join_cluster"}:
            conflicts.append(InstanceConflict("cluster_path_invalid", "error", "Choose a VarAC cluster path", "Use standalone, create a cluster, or join an existing cluster."))
        if current.cluster_path == "join_cluster" and not current.cluster_id:
            conflicts.append(InstanceConflict("cluster_required", "error", "Choose an existing cluster", "Select the cluster this VarAC node should join."))
        if current.cluster_path == "create_cluster" and not (current.cluster_name or current.cluster_id):
            conflicts.append(InstanceConflict("cluster_name_required", "error", "Name the new cluster", "Enter a distinct cluster name or ID."))
        if current.cluster_path != "standalone" and not current.cluster_instance_number:
            conflicts.append(InstanceConflict("cluster_instance_required", "error", "Cluster instance number is required", "Choose a positive instance number for this VarAC node."))
        if current.cluster_path != "standalone" and not current.launch_command:
            conflicts.append(InstanceConflict("cluster_launch_required", "error", "Cluster launch command is required", "Use an instance-specific VarAC launch command so FIO cannot start the default node by mistake."))
        if current.cluster_path == "standalone" and (current.cluster_id or current.cluster_instance_number):
            conflicts.append(InstanceConflict("standalone_cluster_fields", "error", "Standalone VarAC has cluster fields", "Clear cluster identity and instance number or choose a cluster setup path."))
        if current.launch_at_startup and not current.working_directory:
            conflicts.append(InstanceConflict("varac_working_directory", "error", "Working directory is required", "VarAC launch identity includes its exact working directory."))

    wanted_name = current.instance_name.casefold()
    wanted_endpoint = (current.host.casefold(), current.port) if current.host and current.port else None
    wanted_path = current.application_path.casefold()
    for raw in existing_instances:
        row = raw if isinstance(raw, Mapping) else {}
        existing_id = _int(row.get("id"))
        if current.imported_id and existing_id == current.imported_id:
            continue
        existing_name = _text(row.get("name") or row.get("instance_name"))
        if wanted_name and existing_name.casefold() == wanted_name:
            conflicts.append(InstanceConflict("duplicate_name", "error", "Name already in use", f"{existing_name} already identifies another {current.family_key or 'software'} instance."))
        host, port = _endpoint(row)
        if wanted_endpoint and (host.casefold(), port) == wanted_endpoint:
            conflicts.append(InstanceConflict("duplicate_endpoint", "error", "Endpoint already in use", f"{host}:{port} is already assigned to {existing_name or 'another instance'}."))
        if current.family_key == "js8call" and current.udp_port:
            existing_udp = _int(row.get("udp_port") or row.get("js8_udp_port")) or 0
            if (host.casefold(), existing_udp) == (current.host.casefold(), current.udp_port):
                conflicts.append(InstanceConflict("duplicate_udp_endpoint", "error", "UDP endpoint already in use", f"{current.host}:{current.udp_port} is already assigned to {existing_name or 'another instance'}."))
        if current.family_key == "fast_light" and current.secondary_port:
            existing_fldigi = _int(row.get("fldigi_port") or row.get("secondary_port")) or 0
            existing_fldigi_host = _text(row.get("fldigi_host") or row.get("host"))
            if (existing_fldigi_host.casefold(), existing_fldigi) == (current.host.casefold(), current.secondary_port):
                conflicts.append(InstanceConflict("duplicate_secondary_endpoint", "error", "FLDigi endpoint already in use", f"{current.host}:{current.secondary_port} is already assigned to {existing_name or 'another instance'}."))
        existing_path = _text(row.get("application_path") or row.get("install_path") or row.get("path"))
        if wanted_path and existing_path and existing_path.casefold() == wanted_path:
            conflicts.append(InstanceConflict("duplicate_path", "warning", "Application path is shared", "Two instances may share an executable, but verify that their profiles and data folders are separate."))
        current_paths = {
            path.casefold()
            for path in (
                current.configuration_path,
                current.secondary_configuration_path,
                current.storage_path,
                current.secondary_storage_path,
                current.outbox_path,
            )
            if path
        }
        existing_paths = {
            _text(row.get(key)).casefold()
            for key in (
                "configuration_path", "ini_path", "profile_path", "db_path", "storage_path",
                "incoming_path", "outbox_path", "outbox_dir", "fldigi_log_path", "fldigi_checkin_dir",
            )
            if _text(row.get(key))
        }
        if current_paths & existing_paths:
            conflicts.append(InstanceConflict("duplicate_storage", "error", "Configuration or storage is shared", "Each independently launched instance needs its own native profile, database, inbox, outbox, or log path."))
        if current.family_key == "varac" and current.cluster_id and current.cluster_instance_number:
            existing_cluster = f"{_text(row.get('cluster_id'))}:{_int(row.get('cluster_instance_number')) or 0}"
            if existing_cluster == f"{current.cluster_id}:{current.cluster_instance_number}":
                conflicts.append(InstanceConflict("duplicate_cluster_instance", "error", "VarAC cluster instance is already used", "Choose a distinct cluster instance number before saving."))
    return tuple(conflicts)


class SoftwareInstanceAssistant(QWidget):
    """Seven-step instance flow: purpose, source, identity, files, and review."""

    completed = Signal(object)
    cancelled = Signal()
    discover_requested = Signal(str)
    create_radio_requested = Signal()
    validation_requested = Signal(object)
    STEP_TITLES = ("Purpose", "Find or create", "Identity", "Connections", "Files", "Launch", "Review")

    def __init__(
        self,
        family_key: str = "",
        *,
        radios: Iterable[Mapping[str, Any]] = (),
        existing_instances: Iterable[Mapping[str, Any]] = (),
        varac_clusters: Iterable[Mapping[str, Any]] = (),
        selected_radio_id: Optional[int] = None,
        unsaved_owner_key: str = "",
        unsaved_radio_label: str = "",
        radio_role: str = "tx_rx",
        initial_draft: Mapping[str, Any] | SoftwareInstanceDraft | None = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        requested_family = _text(family_key).lower()
        self._family_locked = bool(requested_family)
        self._family_key = requested_family
        self._existing_instances = tuple(dict(row) for row in existing_instances if isinstance(row, Mapping))
        self._radios = tuple(dict(row) for row in radios if isinstance(row, Mapping))
        self._varac_clusters = tuple(dict(row) for row in varac_clusters if isinstance(row, Mapping))
        self._selected_radio_id = _int(selected_radio_id)
        self._unsaved_owner_key = _text(unsaved_owner_key)
        self._unsaved_radio_label = _text(unsaved_radio_label) or "Unsaved radio draft"
        self._radio_role = _text(radio_role).lower() or "tx_rx"
        self._radio_assignments: dict[int, Mapping[str, Any]] = {}
        self._replacement_instance: Optional[Mapping[str, Any]] = None
        self._replacement_confirmed = False
        self._imported_id: Optional[int] = None
        self._imported_system_key = ""
        self._discovery_selected = False
        self._writer_platform = ""
        self._writer_operation = "create"
        self._discovery_results: tuple[Mapping[str, Any], ...] = ()
        self._step = 0
        self._field_widgets: dict[str, QWidget] = {}
        self._field_labels: dict[str, QWidget] = {}
        self._build_ui()
        if self._selected_radio_id is not None:
            selected_index = self.radio_combo.findData(self._selected_radio_id)
            if selected_index >= 0:
                self.radio_combo.setCurrentIndex(selected_index)
        self._load_family(self._family_key)
        if initial_draft is not None:
            seeded = normalize_instance_draft(
                {
                    **(
                        initial_draft.payload()
                        if isinstance(initial_draft, SoftwareInstanceDraft)
                        else dict(initial_draft)
                    ),
                    "family_key": self._family_key,
                    "owner_draft_key": self._unsaved_owner_key,
                    "owner_label": self._unsaved_radio_label,
                    "radio_role": self._radio_role,
                }
            )
            self._set_draft(seeded)
        # A workspace-launched operation is intentionally scoped to exactly
        # one family; changing family would mix the supplied inventory and
        # radio link columns.  A standalone assistant (no family argument)
        # may still choose its family.
        self.family_combo.setEnabled(not self._family_locked)
        if self._family_locked:
            self.family_combo.setToolTip(
                "Family is fixed for this operation; start another Add Instance flow to choose a different family."
            )
        self._refresh()

    def _build_ui(self) -> None:
        self.setAccessibleName("Add software instance assistant")
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(7)
        self.title_label = QLabel("Add a software instance")
        self.title_label.setStyleSheet(label_style("text", active_app_theme(), weight=700))
        root.addWidget(self.title_label)
        self.guidance_label = QLabel("Set up one distinct application instance. Nothing is saved until you review and confirm.")
        self.guidance_label.setWordWrap(True)
        root.addWidget(self.guidance_label)
        self.operation_status_label = QLabel()
        self.operation_status_label.setWordWrap(True)
        self.operation_status_label.setAccessibleName("Software instance operation status")
        self.status_label = self.operation_status_label
        root.addWidget(self.operation_status_label)
        self.replacement_banner = QLabel()
        self.replacement_banner.setWordWrap(True)
        self.replacement_banner.setObjectName("softwareInstanceReplacementBanner")
        self.replacement_banner.setAccessibleName("Software instance replacement status")
        root.addWidget(self.replacement_banner)
        self.step_label = QLabel()
        self.step_label.setAccessibleName("Software instance setup step")
        root.addWidget(self.step_label)

        self.step_buttons: list[QPushButton] = []
        step_grid = QGridLayout()
        step_grid.setContentsMargins(0, 0, 0, 0)
        step_grid.setHorizontalSpacing(6)
        step_grid.setVerticalSpacing(6)
        for index, title in enumerate(self.STEP_TITLES):
            button = QPushButton(f"{index + 1}. {title}")
            button.setObjectName(f"softwareInstanceStep_{index + 1}")
            button.setAccessibleName(f"Software instance step {index + 1}: {title}")
            button.setToolTip(f"Show {title.lower()} setup")
            button.setCheckable(True)
            button.setMinimumHeight(button_height_for_font(button))
            button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            button.clicked.connect(
                lambda _checked=False, target=index: self._select_step(target)
            )
            self.step_buttons.append(button)
            step_grid.addWidget(button, index // 4, index % 4)
        for column in range(4):
            step_grid.setColumnStretch(column, 1)
        root.addLayout(step_grid)

        self.pages = CurrentPageStack()
        self.pages.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Expanding)
        self.pages.setAccessibleName("Software instance setup pages")
        root.addWidget(self.pages, 1)
        self._build_choose_page()
        self._build_source_page()
        self._build_identity_page()
        self._build_connections_page()
        self._build_files_page()
        self._build_launch_page()
        self._build_review_page()

        actions = QHBoxLayout()
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setAccessibleName("Cancel adding software instance")
        self.cancel_button.clicked.connect(self.cancelled.emit)
        self.back_button = QPushButton("Back")
        self.back_button.clicked.connect(self._back)
        self.next_button = QPushButton("Next")
        self.next_button.setDefault(True)
        self.next_button.clicked.connect(self._next)
        actions.addWidget(self.cancel_button)
        actions.addStretch(1)
        actions.addWidget(self.back_button)
        actions.addWidget(self.next_button)
        root.addLayout(actions)

    def _build_choose_page(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        self.family_scope_label = QLabel("Choose the application family for this instance.")
        self.family_scope_label.setWordWrap(True)
        self.family_scope_label.setAccessibleName("Software family scope guidance")
        layout.addWidget(self.family_scope_label)
        self.family_combo = QComboBox()
        self.family_combo.setAccessibleName("Software family")
        for key, label in SUPPORTED_INSTANCE_FAMILIES:
            self.family_combo.addItem(label, key)
        self.family_combo.currentIndexChanged.connect(lambda _index: self._load_family(str(self.family_combo.currentData() or "")))
        layout.addWidget(self.family_combo)
        radio_group = QGroupBox("Radio context (required)")
        radio_layout = QVBoxLayout(radio_group)
        self.radio_guidance_label = QLabel()
        self.radio_guidance_label.setWordWrap(True)
        self.radio_guidance_label.setAccessibleName("Radio selection guidance")
        radio_layout.addWidget(self.radio_guidance_label)
        self.radio_combo = QComboBox()
        self.radio_combo.setAccessibleName("Radio for software instance")
        self.radio_combo.currentIndexChanged.connect(self._on_radio_changed)
        radio_layout.addWidget(self.radio_combo)
        self.create_radio_button = QPushButton("Create a radio first…")
        self.create_radio_button.setAccessibleName("Create a radio before adding a software instance")
        self.create_radio_button.setToolTip(
            "Open Radio Profiles to create a radio, then return to this guided setup"
        )
        self.create_radio_button.clicked.connect(self.create_radio_requested.emit)
        radio_layout.addWidget(self.create_radio_button)
        self.replacement_checkbox = QCheckBox("Replace the existing instance assigned to this radio")
        self.replacement_checkbox.setAccessibleName("Confirm replacement of existing software instance")
        self.replacement_checkbox.setToolTip(
            "Replacement updates the radio-to-instance mapping; the existing application record is retained."
        )
        self.replacement_checkbox.toggled.connect(self._set_replacement_confirmed)
        radio_layout.addWidget(self.replacement_checkbox)
        layout.addWidget(radio_group)
        layout.addStretch(1)
        self.pages.addWidget(page)

    def _build_source_page(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel("Choose how this instance will be used. You can import a detected configuration or enter it manually."))
        self.source_buttons: dict[str, QRadioButton] = {}
        for key, text in (
            ("managed", "Set up a new local instance (FIO-guided)"),
            ("discover", "Find or import an existing installation"),
            ("remote", "Connect to a manual or remote instance"),
        ):
            button = QRadioButton(text)
            button.setAccessibleName(text)
            button.toggled.connect(self._refresh_source)
            self.source_buttons[key] = button
            layout.addWidget(button)
        self.discovery_hint = QLabel("Discovery is explicit. The host may provide results without this page performing a scan.")
        self.discovery_hint.setWordWrap(True)
        layout.addWidget(self.discovery_hint)
        self.discover_button = QPushButton("Find existing configurations")
        self.discover_button.clicked.connect(lambda: self.discover_requested.emit(self._family_key))
        layout.addWidget(self.discover_button)
        self.discovery_list = QListWidget()
        self.discovery_list.setAccessibleName("Discovered software configurations")
        self.discovery_list.itemSelectionChanged.connect(self._import_selected_discovery)
        layout.addWidget(self.discovery_list, 1)
        self.source_buttons["managed"].setChecked(True)
        self.pages.addWidget(page)

    def _new_form_page(self, heading: str, fields: tuple[tuple[str, str, str], ...]) -> QFormLayout:
        page = QWidget()
        outer = QVBoxLayout(page)
        hint = QLabel(heading)
        hint.setWordWrap(True)
        outer.addWidget(hint)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        form_widget = QWidget()
        form = QFormLayout(form_widget)
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        form.setRowWrapPolicy(QFormLayout.WrapLongRows)
        self._active_form = form
        for key, label, placeholder in fields:
            self._add_line(key, label, placeholder)
        scroll.setWidget(form_widget)
        outer.addWidget(scroll, 1)
        self.pages.addWidget(page)
        return form

    def _build_identity_page(self) -> None:
        self._new_form_page(
            "Identity: give this instance a distinct name and confirm who manages it.",
            (
                ("instance_name", "Instance name", "A distinct name for this application instance"),
                ("variant", "JS8Call variant", "Choose the exact installed JS8Call family"),
                ("version", "Verified application version", "Exact version, for example 2.2.0"),
            ),
        )
        ownership = QComboBox()
        ownership.addItem(
            "FIO-managed identity and launch (native settings only when exactly qualified)",
            "fio-managed",
        )
        ownership.addItem("Operator-managed (observe/import only)", "operator-managed")
        ownership.addItem("Remote (observe endpoint only)", "remote")
        ownership.setAccessibleName("Software instance ownership")
        self._field_widgets["ownership"] = ownership
        # The helper's most recently created form is the identity form.
        ownership_label = QLabel("Ownership")
        self._field_labels["ownership"] = ownership_label
        self._active_form.addRow(ownership_label, ownership)

    def _build_connections_page(self) -> None:
        self._new_form_page(
            "Connections: endpoints identify the instance at runtime. Use unique local ports for independently launched instances. FIO does not rewrite third-party settings from this screen.",
            (
                ("rig_name", "JS8 rig name", "Unique --rig-name (JS8Call only)"),
                ("host", "FLRig/JS8 host", "127.0.0.1"),
                ("port", "FLRig or JS8 TCP port", "TCP port"),
                ("udp_port", "JS8 UDP port", "UDP port"),
                ("secondary_port", "FLDigi XML-RPC port", "TCP port"),
            ),
        )

    def _build_files_page(self) -> None:
        self._new_form_page(
            "Files: keep profiles/configuration, logs, and message/data storage attributable to this instance. Existing files are imported for review; external changes require an explicit supported apply.",
            (
                ("application_path", "Application / FLRig path", "Path to the application or launcher"),
                ("secondary_application_path", "FLDigi path", "Fast Light FLDigi application (optional)"),
                ("flmsg_application_path", "FLMsg path", "Fast Light FLMsg application (optional shared tool)"),
                ("flamp_application_path", "FLAmp path", "Fast Light FLAmp application (optional shared tool)"),
                ("configuration_path", "Configuration / profile / INI", "Profile, configuration, or VarAC INI"),
                ("secondary_configuration_path", "FLDigi configuration", "FLDigi profile/configuration"),
                ("storage_path", "Data / database / log folder", "Instance-owned data, database, or FLDigi logs"),
                ("secondary_storage_path", "Incoming / check-in folder", "VarAC incoming or FLDigi check-in folder"),
                ("outbox_path", "VarAC outbox", "VarAC outbox folder"),
                ("working_directory", "VarAC working directory", "Exact working directory for this VarAC node"),
                ("cluster_path", "Cluster setup", "Standalone, create, or join"),
                ("cluster_id", "VarAC cluster ID", "Optional cluster identifier"),
                ("cluster_name", "New cluster name", "Name for a new VarAC cluster"),
                ("cluster_shared_database", "Cluster shared database", "Optional cluster-owned shared database"),
                ("cluster_instance_number", "VarAC cluster instance", "Optional positive instance number"),
            ),
        )
        for key, text in (
            ("cluster_gateway", "Use this node as the new cluster gateway"),
            ("cluster_ptt_lock", "Enable cluster PTT lock"),
        ):
            checkbox = QCheckBox(text)
            checkbox.setAccessibleName(text)
            checkbox.toggled.connect(lambda _checked: self._refresh_review_if_needed())
            self._field_widgets[key] = checkbox
            label = QLabel("Cluster policy")
            self._field_labels[key] = label
            self._active_form.addRow(label, checkbox)

    def _build_launch_page(self) -> None:
        self._new_form_page(
            "Launch: review what FIO may start. An empty command leaves launching operator-managed. Startup selection is a preference, not permission to rewrite an application configuration.",
            (("launch_command", "Launch command", "Optional command FIO may launch"), ("notes", "Notes", "Why this instance exists or what it is connected to")),
        )
        launch = QCheckBox("Launch this instance at startup")
        launch.setAccessibleName("Launch software instance at startup")
        self._field_widgets["launch_at_startup"] = launch
        launch_label = QLabel("Startup policy")
        self._field_labels["launch_at_startup"] = launch_label
        self._active_form.addRow(launch_label, launch)
        advanced_tx = QCheckBox("Enable Advanced Fast Light TX")
        advanced_tx.setAccessibleName("Enable Advanced Fast Light transmit mode")
        advanced_tx.setToolTip(
            "Receive-safe is the default. Advanced TX also requires a compatible Operating Model, RF Guard, and final preflight."
        )
        advanced_ack = QCheckBox(
            "I understand Advanced TX remains subject to Operating Model, RF Guard, and final preflight"
        )
        advanced_ack.setAccessibleName("Acknowledge Advanced Fast Light transmit safeguards")
        advanced_tx.toggled.connect(advanced_ack.setEnabled)
        advanced_tx.toggled.connect(lambda _checked: self._refresh_review_if_needed())
        advanced_ack.toggled.connect(lambda _checked: self._refresh_review_if_needed())
        advanced_ack.setEnabled(False)
        self._field_widgets["advanced_tx_requested"] = advanced_tx
        self._field_widgets["advanced_tx_acknowledged"] = advanced_ack
        advanced_label = QLabel("Fast Light mode")
        acknowledgement_label = QLabel("Advanced TX acknowledgement")
        self._field_labels["advanced_tx_requested"] = advanced_label
        self._field_labels["advanced_tx_acknowledged"] = acknowledgement_label
        self._active_form.addRow(advanced_label, advanced_tx)
        self._active_form.addRow(acknowledgement_label, advanced_ack)

    def _add_line(self, key: str, label: str, placeholder: str) -> None:
        if key == "variant":
            combo = QComboBox()
            combo.setAccessibleName(label)
            combo.addItem("Choose the verified JS8Call variant", "")
            combo.addItem("JS8Call 2.2", "js8call_2_2")
            combo.addItem("JS8Call Improved 3.0.3", "js8call_improved_3_0_3")
            combo.addItem("JS8Call Subspace 4.1", "js8call_subspace_4_1")
            combo.currentIndexChanged.connect(lambda _index: self._refresh_review_if_needed())
            self._field_widgets[key] = combo
            label_widget = QLabel(label)
            self._field_labels[key] = label_widget
            self._active_form.addRow(label_widget, combo)
            return
        if key == "cluster_path":
            combo = QComboBox()
            combo.setAccessibleName(label)
            combo.addItem("Standalone VarAC node", "standalone")
            combo.addItem("Create a new cluster", "create_cluster")
            combo.addItem("Join an existing cluster", "join_cluster")
            combo.currentIndexChanged.connect(lambda _index: self._sync_family_fields())
            combo.currentIndexChanged.connect(lambda _index: self._refresh_review_if_needed())
            self._field_widgets[key] = combo
            label_widget = QLabel(label)
            self._field_labels[key] = label_widget
            self._active_form.addRow(label_widget, combo)
            return
        if key == "cluster_id":
            combo = QComboBox()
            combo.setEditable(True)
            combo.setAccessibleName(label)
            combo.setToolTip("Choose a configured cluster, or leave this blank for a standalone VarAC node.")
            combo.addItem("Choose or enter a cluster", "")
            for cluster in self._varac_clusters:
                public_id = _text(cluster.get("cluster_id"))
                name = _text(cluster.get("name")) or public_id
                if public_id:
                    combo.addItem(f"{name} · {public_id}", public_id)
            combo.currentTextChanged.connect(lambda _text: self._refresh_review_if_needed())
            self._field_widgets[key] = combo
            label_widget = QLabel(label)
            self._field_labels[key] = label_widget
            self._active_form.addRow(label_widget, combo)
            return
        edit = QLineEdit()
        edit.setAccessibleName(label)
        edit.setPlaceholderText(placeholder)
        edit.textEdited.connect(lambda _text, _key=key: self._refresh_review_if_needed())
        self._field_widgets[key] = edit
        label_widget = QLabel(label)
        label_widget.setAccessibleName(f"Label for {label}")
        self._field_labels[key] = label_widget
        self._active_form.addRow(label_widget, edit)

    def _build_review_page(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        self.review_label = QLabel()
        self.review_label.setWordWrap(True)
        self.review_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.review_label.setAccessibleName("Software instance review")
        layout.addWidget(self.review_label)
        self.conflict_label = QLabel()
        self.conflict_label.setWordWrap(True)
        self.conflict_label.setAccessibleName("Software instance conflicts")
        layout.addWidget(self.conflict_label)
        layout.addStretch(1)
        self.pages.addWidget(page)

    def _load_family(self, family_key: str) -> None:
        normalized = _text(family_key).lower()
        if normalized not in {key for key, _label in SUPPORTED_INSTANCE_FAMILIES}:
            normalized = "js8call"
        self._family_key = normalized
        if hasattr(self, "family_combo"):
            index = self.family_combo.findData(normalized)
            if index >= 0 and self.family_combo.currentIndex() != index:
                blocked = self.family_combo.blockSignals(True)
                self.family_combo.setCurrentIndex(index)
                self.family_combo.blockSignals(blocked)
        title, detail = _FAMILY_DETAILS[normalized]
        if hasattr(self, "title_label"):
            self.title_label.setText(f"Add {title}")
            self.guidance_label.setText(
                f"Set up one distinct {title.lower()} for the selected radio. "
                + detail
                + " Existing settings are evidence for review. FIO does not claim to write third-party application configuration unless a supported, explicit apply is provided."
            )
            if getattr(self, "_family_locked", False):
                self.family_scope_label.setText(
                    f"Family fixed by the selected workspace: {dict(SUPPORTED_INSTANCE_FAMILIES).get(normalized, title)}. "
                    "This operation creates exactly one instance in this family."
                )
        host = self._field_widgets.get("host")
        port = self._field_widgets.get("port")
        if isinstance(host, QLineEdit) and not host.text():
            host.setText("127.0.0.1")
        if isinstance(port, QLineEdit) and not port.text() and _DEFAULT_PORTS[normalized]:
            port.setText(str(_DEFAULT_PORTS[normalized]))
        secondary = self._field_widgets.get("secondary_port")
        if isinstance(secondary, QLineEdit) and not secondary.text() and normalized == "fast_light":
            secondary.setText(str(self._next_port(7362, "fldigi_port", "secondary_port")))
        udp = self._field_widgets.get("udp_port")
        if isinstance(udp, QLineEdit) and not udp.text() and normalized == "js8call":
            udp.setText(str(self._next_port(2242, "udp_port", "js8_udp_port")))
        if isinstance(port, QLineEdit) and normalized == "js8call" and port.text() == str(_DEFAULT_PORTS[normalized]):
            port.setText(str(self._next_port(2442, "port")))
        elif isinstance(port, QLineEdit) and normalized == "fast_light" and port.text() == str(_DEFAULT_PORTS[normalized]):
            port.setText(str(self._next_port(12345, "flrig_port", "port")))
        self._sync_family_fields()
        if normalized == "fast_light" and self._radio_role == "observer":
            self.guidance_label.setText(
                "Configure one receive-only Fast Light identity for this SDR. FLDigi and approved file helpers are available; "
                "FLRig, CAT, PTT, transmit, and automatic send remain disabled."
            )
        elif normalized == "varac" and self._radio_role == "observer":
            self.guidance_label.setText(
                "VarAC is unavailable for an observer / SDR. Return to Add Radio and choose a receive-only application."
            )
        self._rebuild_radio_choices()

    def _next_port(self, base: int, *keys: str) -> int:
        used: set[int] = set()
        for row in self._existing_instances:
            for key in keys:
                parsed = _int(row.get(key))
                if parsed is not None:
                    used.add(parsed)
        candidate = int(base)
        while candidate in used and candidate < 65535:
            candidate += 1
        return candidate

    def _apply_identity_defaults(self) -> None:
        if self._family_key != "js8call":
            return
        rig_widget = self._field_widgets.get("rig_name")
        if not isinstance(rig_widget, QLineEdit) or rig_widget.text().strip():
            return
        name_widget = self._field_widgets.get("instance_name")
        radio = self.radio_combo.currentText().strip()
        source = name_widget.text().strip() if isinstance(name_widget, QLineEdit) else ""
        source = source or radio or "JS8"
        candidate = re.sub(r"[^A-Za-z0-9_-]+", "-", source).strip("-")[:48] or "JS8"
        used = {_text(row.get("rig_name")).casefold() for row in self._existing_instances}
        base = candidate
        suffix = 2
        while candidate.casefold() in used:
            candidate = f"{base[:43]}-{suffix}"
            suffix += 1
        rig_widget.setText(candidate)

    def _family_link_column(self) -> str:
        return {
            "js8call": "js8_instance_id",
            "fast_light": "fast_light_config_id",
            "varac": "varac_node_id",
        }.get(self._family_key, "")

    def _rebuild_radio_choices(self) -> None:
        """Render every known radio with same-family ownership status."""

        if self._unsaved_owner_key:
            self.radio_combo.blockSignals(True)
            self.radio_combo.clear()
            self.radio_combo.addItem(
                f"{self._unsaved_radio_label} — inactive setup draft",
                None,
            )
            self.radio_combo.setToolTip(
                "This software instance belongs to the current unsaved radio draft. "
                "Nothing is persisted until Save Radio and Software."
            )
            self.radio_combo.setEnabled(False)
            self.radio_combo.blockSignals(False)
            self._selected_radio_id = None
            self._radio_assignments = {}
            self._replacement_instance = None
            self._refresh_radio_context()
            return

        assignments: dict[int, Mapping[str, Any]] = {}
        by_id = {
            _int(row.get("id")): row
            for row in self._existing_instances
            if _int(row.get("id")) is not None
        }
        link_column = self._family_link_column()
        for row in self._radios:
            radio_id = _int(row.get("id") or row.get("radio_id"))
            if radio_id is None:
                continue
            assigned_id = _int(row.get(link_column)) if link_column else None
            direct = _int(row.get("instance_id"))
            assigned_id = assigned_id or direct
            if assigned_id is not None:
                assignments[radio_id] = by_id.get(
                    assigned_id,
                    {"id": assigned_id, "name": f"Instance {assigned_id}"},
                )
            else:
                direct_row = next(
                    (
                        candidate for candidate in self._existing_instances
                        if _int(candidate.get("radio_id")) == radio_id
                    ),
                    None,
                )
                if direct_row is not None:
                    assignments[radio_id] = direct_row
        self._radio_assignments = assignments

        prior_id = _int(self.radio_combo.currentData())
        desired_id = self._selected_radio_id if self._selected_radio_id in {
            _int(row.get("id") or row.get("radio_id")) for row in self._radios
        } else prior_id
        self.radio_combo.blockSignals(True)
        self.radio_combo.clear()
        candidate_rows = [
            row for row in self._radios
            if _int(row.get("id") or row.get("radio_id")) is not None
        ]
        if candidate_rows:
            self.radio_combo.addItem("Choose an existing radio…", None)
        seen: set[int] = set()
        for row in self._radios:
            radio_id = _int(row.get("id") or row.get("radio_id"))
            if radio_id is None or radio_id in seen:
                continue
            seen.add(radio_id)
            name = _text(row.get("name")) or f"Radio {radio_id}"
            assigned = assignments.get(radio_id)
            instance_name = _text(assigned.get("name") or assigned.get("instance_name")) if assigned else ""
            label = f"{name} — Assigned to {instance_name}" if assigned else f"{name} — Available"
            self.radio_combo.addItem(label, radio_id)
            index = self.radio_combo.count() - 1
            tooltip = (
                "This radio already has a %s instance. Replacement mode is explicit and retains the existing record."
                % (instance_name or "software")
                if assigned
                else "This radio has no instance in the selected software family."
            )
            self.radio_combo.setItemData(index, tooltip, Qt.ToolTipRole)
        if self.radio_combo.count():
            if desired_id is None and len(candidate_rows) == 1:
                only_id = _int(candidate_rows[0].get("id") or candidate_rows[0].get("radio_id"))
                index = self.radio_combo.findData(only_id)
            else:
                index = self.radio_combo.findData(desired_id)
            self.radio_combo.setCurrentIndex(index if index >= 0 else 0)
        self.radio_combo.blockSignals(False)
        self._selected_radio_id = _int(self.radio_combo.currentData())
        self._refresh_radio_context()

    def _set_replacement_confirmed(self, checked: bool) -> None:
        self._replacement_confirmed = bool(checked)
        self._refresh_radio_context()
        self._refresh()

    def _on_radio_changed(self, _index: int) -> None:
        """Refresh both guidance and step navigation for an explicit choice."""

        self._refresh_radio_context()
        self._refresh()

    def _refresh_radio_context(self) -> None:
        if self._unsaved_owner_key:
            self._selected_radio_id = None
            self._replacement_instance = None
            self._replacement_confirmed = False
            self.create_radio_button.setVisible(False)
            self.radio_combo.setEnabled(False)
            self.replacement_checkbox.setVisible(False)
            self.replacement_checkbox.setEnabled(False)
            self.radio_guidance_label.setText(
                f"Radio draft selected: {self._unsaved_radio_label}. "
                "Apply returns the reviewed software bundle to Add Radio; it does not save it."
            )
            self.replacement_banner.setText(
                "Inactive setup draft — Save Radio and Software remains required."
            )
            return
        radio_id = _int(self.radio_combo.currentData())
        self._selected_radio_id = radio_id
        replacement = self._radio_assignments.get(radio_id) if radio_id is not None else None
        prior = self._replacement_instance
        self._replacement_instance = replacement
        if replacement is not prior:
            self._replacement_confirmed = False
            if hasattr(self, "replacement_checkbox"):
                blocked = self.replacement_checkbox.blockSignals(True)
                self.replacement_checkbox.setChecked(False)
                self.replacement_checkbox.blockSignals(blocked)
        has_radios = any(
            _int(row.get("id") or row.get("radio_id")) is not None
            for row in self._radios
        )
        self.create_radio_button.setVisible(not has_radios)
        self.radio_combo.setEnabled(has_radios)
        if not has_radios:
            self.radio_guidance_label.setText(
                "No radios exist yet. Create a radio first in Radio Profiles, then return here; "
                "a software instance cannot be created without an existing radio."
            )
            self.replacement_checkbox.setVisible(False)
            self.replacement_banner.setText("Radio required before entering instance data.")
        elif radio_id is None:
            self.radio_guidance_label.setText(
                "Choose an existing radio before entering instance data."
            )
            self.replacement_checkbox.setVisible(False)
            self.replacement_checkbox.setEnabled(False)
            self.replacement_banner.setText("Choose a radio before entering instance data.")
        elif replacement is not None:
            name = _text(replacement.get("name") or replacement.get("instance_name")) or "existing instance"
            self.radio_guidance_label.setText(
                "This radio is already assigned to this software family. "
                "Choose replacement mode before entering instance data; the existing record is retained."
            )
            self.replacement_checkbox.setText(f"Replace the existing instance assigned to {self.radio_combo.currentText().split(' — ', 1)[0]}")
            self.replacement_checkbox.setVisible(True)
            self.replacement_checkbox.setEnabled(True)
            mode = "active" if self._replacement_confirmed else "required"
            self.replacement_banner.setText(
                f"Replacement mode {mode}: {name} is currently assigned. "
                "Review will compare the current instance with the proposed one; no manual disassociate is required."
            )
        else:
            self.radio_guidance_label.setText(
                "Choose an existing radio. Available radios can accept one instance of each software family."
            )
            self.replacement_checkbox.setVisible(False)
            self.replacement_checkbox.setEnabled(False)
            self.replacement_banner.setText(
                "Radio selected: Available for this software family."
            )

    def _sync_family_fields(self) -> None:
        visible = _FAMILY_FIELDS.get(self._family_key, frozenset())
        labels = _FAMILY_FIELD_LABELS.get(self._family_key, {})
        observer_mode = self._radio_role == "observer"
        cluster_path_widget = self._field_widgets.get("cluster_path")
        cluster_path = (
            str(cluster_path_widget.currentData() or "standalone")
            if isinstance(cluster_path_widget, QComboBox)
            else "standalone"
        )
        for key, widget in self._field_widgets.items():
            shown = key in visible
            if self._family_key == "fast_light" and observer_mode and key in {
                "port",
                "application_path",
                "configuration_path",
                "advanced_tx_requested",
                "advanced_tx_acknowledged",
            }:
                shown = False
            if self._family_key == "varac":
                if key in {"cluster_id", "cluster_name", "cluster_shared_database", "cluster_instance_number", "cluster_gateway", "cluster_ptt_lock"}:
                    shown = cluster_path != "standalone"
                if key in {"cluster_name", "cluster_shared_database", "cluster_gateway", "cluster_ptt_lock"}:
                    shown = cluster_path == "create_cluster"
            widget.setVisible(shown)
            label = self._field_labels.get(key)
            if label is not None:
                if key in labels and isinstance(label, QLabel):
                    label.setText(labels[key])
                label.setVisible(shown)
        if self._family_key == "fast_light" and observer_mode:
            for key in ("advanced_tx_requested", "advanced_tx_acknowledged"):
                widget = self._field_widgets.get(key)
                if isinstance(widget, QCheckBox):
                    widget.setChecked(False)

    def _refresh_source(self) -> None:
        discover = self.source_buttons["discover"].isChecked()
        mode = next((key for key, button in self.source_buttons.items() if button.isChecked()), "managed")
        ownership = self._field_widgets.get("ownership")
        if isinstance(ownership, QComboBox):
            desired_ownership = {
                "managed": "fio-managed",
                "discover": "operator-managed",
                "remote": "remote",
            }.get(mode, "operator-managed")
            index = ownership.findData(desired_ownership)
            if index >= 0:
                ownership.setCurrentIndex(index)
        if not discover and self._discovery_selected:
            # Keep copied values as a starting point, but do not overwrite the
            # discovered application's durable record when the operator has
            # changed to a new-local or remote setup path.
            self._imported_id = None
            self._imported_system_key = ""
            self._discovery_selected = False
        self.discover_button.setVisible(discover)
        self.discovery_list.setVisible(discover and bool(self._discovery_results))
        self.discovery_hint.setText(
            "Discovery is explicit. Choose a result to import its values; review remains required."
            if discover else
            "You can change these values later in Software Administration. Nothing is saved on this step."
        )

    def _import_selected_discovery(self) -> None:
        item = self.discovery_list.currentItem()
        if item is None:
            return
        value = item.data(Qt.UserRole)
        if isinstance(value, Mapping):
            imported = normalize_instance_draft({**value, "family_key": self._family_key})
            self._discovery_selected = True
            self._set_draft(imported)

    def set_discovery_results(self, results: Iterable[Mapping[str, Any]]) -> None:
        """Render host-provided results and never scan on its own."""

        self._discovery_results = tuple(dict(row) for row in results if isinstance(row, Mapping))
        self._discovery_selected = False
        self.discovery_list.clear()
        for row in self._discovery_results:
            name = _text(row.get("name") or row.get("instance_name")) or "Unnamed configuration"
            host, port = _endpoint(row)
            detail = f"{name} · {host}:{port}" if host and port else name
            item = QListWidgetItem(detail)
            item.setData(Qt.UserRole, row)
            evidence = [
                _text(row.get("application_path") or row.get("install_path") or row.get("path")),
                _text(row.get("configuration_path") or row.get("profile_path") or row.get("ini_path")),
                _text(row.get("storage_path") or row.get("application_data_root") or row.get("db_path")),
            ]
            item.setToolTip("\n".join(part for part in evidence if part))
            self.discovery_list.addItem(item)
        self._refresh_source()

    def _set_draft(self, draft: SoftwareInstanceDraft) -> None:
        values = draft.payload()
        self._radio_role = draft.radio_role or self._radio_role
        self._imported_id = draft.imported_id
        self._imported_system_key = draft.imported_system_key
        self._writer_platform = draft.writer_platform
        self._writer_operation = draft.writer_operation or "create"
        source_button = self.source_buttons.get(draft.mode)
        if source_button is not None:
            source_button.setChecked(True)
        for key, widget in self._field_widgets.items():
            value = values.get(key, "")
            if isinstance(widget, QLineEdit):
                widget.setText(str(value if value is not None else ""))
            elif isinstance(widget, QCheckBox):
                widget.setChecked(bool(value))
            elif isinstance(widget, QComboBox):
                index = widget.findData(value)
                if index >= 0:
                    widget.setCurrentIndex(index)
                elif widget.isEditable():
                    widget.setCurrentText(str(value if value is not None else ""))
        radio_index = self.radio_combo.findData(draft.radio_id)
        if radio_index >= 0:
            self.radio_combo.setCurrentIndex(radio_index)
        # A discovered row is evidence owned by the operator until an
        # explicit supported managed apply is reviewed and confirmed.
        if draft.imported_id is not None:
            self.source_buttons["discover"].setChecked(True)
            operator_index = self._field_widgets["ownership"].findData("operator-managed")
            if operator_index >= 0 and draft.ownership == "fio-managed":
                self._field_widgets["ownership"].setCurrentIndex(operator_index)
        self._sync_family_fields()

    def draft(self) -> SoftwareInstanceDraft:
        def value(key: str) -> str:
            widget = self._field_widgets.get(key)
            if isinstance(widget, QLineEdit):
                return widget.text().strip()
            if isinstance(widget, QComboBox):
                data = widget.currentData()
                if widget.isEditable() and not str(data or "").strip():
                    # An editable combo may use a labeled blank first item.
                    # Treat that item as blank, but preserve actual operator text.
                    if widget.currentIndex() >= 0:
                        return ""
                    return widget.currentText().strip()
                return str(data or "").strip()
            return ""

        def number(key: str) -> int:
            return _int(value(key)) or 0

        def checked(key: str) -> bool:
            widget = self._field_widgets.get(key)
            return bool(widget.isChecked()) if isinstance(widget, QCheckBox) else False

        radio_id = _int(self.radio_combo.currentData())
        return SoftwareInstanceDraft(
            family_key=self._family_key,
            instance_name=value("instance_name"),
            radio_id=radio_id,
            owner_draft_key=self._unsaved_owner_key,
            owner_label=self._unsaved_radio_label if self._unsaved_owner_key else "",
            radio_role=self._radio_role,
            mode=next((key for key, button in self.source_buttons.items() if button.isChecked()), "managed"),
            ownership=str(self._field_widgets["ownership"].currentData() or "fio-managed"),
            variant=value("variant"),
            version=value("version"),
            writer_platform=self._writer_platform,
            writer_operation=self._writer_operation,
            host=value("host"),
            port=number("port"),
            udp_port=number("udp_port"),
            secondary_port=number("secondary_port"),
            rig_name=value("rig_name"),
            application_path=value("application_path"),
            secondary_application_path=value("secondary_application_path"),
            flmsg_application_path=value("flmsg_application_path"),
            flamp_application_path=value("flamp_application_path"),
            configuration_path=value("configuration_path"),
            secondary_configuration_path=value("secondary_configuration_path"),
            storage_path=value("storage_path"),
            secondary_storage_path=value("secondary_storage_path"),
            outbox_path=value("outbox_path"),
            launch_command=value("launch_command"),
            launch_at_startup=checked("launch_at_startup"),
            working_directory=value("working_directory"),
            advanced_tx_requested=checked("advanced_tx_requested"),
            advanced_tx_acknowledged=checked("advanced_tx_acknowledged"),
            cluster_path=value("cluster_path") or "standalone",
            cluster_id=value("cluster_id"),
            cluster_name=value("cluster_name"),
            cluster_shared_database=value("cluster_shared_database"),
            cluster_instance_number=number("cluster_instance_number"),
            cluster_gateway=checked("cluster_gateway"),
            cluster_ptt_lock=checked("cluster_ptt_lock"),
            notes=value("notes"),
            imported_id=self._imported_id,
            imported_system_key=self._imported_system_key,
            replace_existing=bool(self._replacement_instance and self._replacement_confirmed),
            replacement_instance_id=(
                _int(self._replacement_instance.get("id"))
                if self._replacement_instance is not None else None
            ),
        )

    def set_operation_status(self, message: str, error: bool = False) -> None:
        """Show host operation feedback without implying that a scan or save occurred."""

        self.operation_status_label.setText(_text(message))
        self.operation_status_label.setProperty("error", bool(error))
        self.operation_status_label.style().unpolish(self.operation_status_label)
        self.operation_status_label.style().polish(self.operation_status_label)

    def validation(self) -> tuple[InstanceConflict, ...]:
        draft = self.draft()
        findings = list(instance_conflicts(draft, self._existing_instances))
        if draft.mode == "discover" and not self._discovery_selected:
            findings.append(
                InstanceConflict(
                    "discovery_selection_required",
                    "error",
                    "Choose a discovered configuration",
                    "Run Find existing configurations and select one result, or choose the new local or remote setup path.",
                )
            )
        return tuple(findings)

    def _refresh_review_if_needed(self) -> None:
        if self._step == len(self.STEP_TITLES) - 1:
            self._refresh_review()

    def _refresh_review(self) -> None:
        draft = self.draft()
        family = dict(SUPPORTED_INSTANCE_FAMILIES).get(draft.family_key, draft.family_key)
        radio = self.radio_combo.currentText()
        endpoint = f"{draft.host}:{draft.port}" if draft.port else f"{draft.host} (application-defined endpoint)"
        lines = [
            family,
            "",
            f"Name: {draft.instance_name or 'Not set'}",
            f"Radio: {radio}",
            f"Source: {draft.mode.replace('-', ' ').title()}",
            f"Ownership: {draft.ownership.replace('-', ' ').title()}",
        ]
        if self._replacement_instance is not None:
            old = self._replacement_instance
            old_name = _text(old.get("name") or old.get("instance_name")) or "existing instance"
            old_id = _int(old.get("id"))
            old_endpoint = _endpoint(old)
            old_detail = f"{old_name} (id {old_id})" if old_id else old_name
            if old_endpoint[0] and old_endpoint[1]:
                old_detail += f" · {old_endpoint[0]}:{old_endpoint[1]}"
            lines.extend(
                (
                    "Replacement comparison:",
                    f"Current assignment: {old_detail}",
                    f"Proposed assignment: {draft.instance_name or 'Not set'}",
                )
            )
        if draft.family_key == "js8call":
            lines.extend(
                (
                    f"Variant/version: {draft.variant or 'Not verified'} · {draft.version or 'Not verified'}",
                    f"Rig name: {draft.rig_name or 'Not set'}",
                    f"TCP API: {endpoint}",
                    f"UDP: {draft.host}:{draft.udp_port}" if draft.udp_port else "UDP: Not configured",
                    f"Application: {draft.application_path or 'Not set'}",
                    f"Settings profile: {draft.configuration_path or 'Not set'}",
                    f"Message data: {draft.storage_path or 'Not set'}",
                )
            )
        elif draft.family_key == "fast_light":
            if draft.radio_role == "observer":
                lines.extend(
                    (
                        "Scope: Receive-only; FLRig, CAT, PTT, TX, and automatic send unavailable",
                        f"FLDigi endpoint: {draft.host}:{draft.secondary_port}" if draft.secondary_port else "FLDigi endpoint: Not configured",
                        f"FLDigi application/config: {draft.secondary_application_path or 'Not set'} · {draft.secondary_configuration_path or 'Not set'}",
                        f"FLMsg/FLAmp: {draft.flmsg_application_path or 'Operator start / not selected'} · {draft.flamp_application_path or 'Operator start / not selected'}",
                        f"FLDigi logs/check-ins: {draft.storage_path or 'Not set'} · {draft.secondary_storage_path or 'Not set'}",
                    )
                )
            else:
                lines.extend(
                    (
                        f"FLRig endpoint: {endpoint}",
                        f"FLDigi endpoint: {draft.host}:{draft.secondary_port}" if draft.secondary_port else "FLDigi endpoint: Not configured",
                        f"FLRig application/config: {draft.application_path or 'Not set'} · {draft.configuration_path or 'Not set'}",
                        f"FLDigi application/config: {draft.secondary_application_path or 'Not set'} · {draft.secondary_configuration_path or 'Not set'}",
                        f"FLMsg/FLAmp: {draft.flmsg_application_path or 'Operator start / not selected'} · {draft.flamp_application_path or 'Operator start / not selected'}",
                        f"FLDigi logs/check-ins: {draft.storage_path or 'Not set'} · {draft.secondary_storage_path or 'Not set'}",
                        "Fast Light mode: Advanced TX requested"
                        if draft.advanced_tx_requested
                        else "Fast Light mode: Receive-safe",
                    )
                )
        else:
            lines.extend(
                (
                    f"Application: {draft.application_path or 'Not set'}",
                    f"INI: {draft.configuration_path or 'Not set'}",
                    f"Database: {draft.storage_path or 'Not set'}",
                    f"Incoming/outbox: {draft.secondary_storage_path or 'Not set'} · {draft.outbox_path or 'Not set'}",
                    f"Working directory: {draft.working_directory or 'Not set'}",
                    f"Cluster path: {draft.cluster_path.replace('_', ' ').title()}",
                    f"Cluster: {draft.cluster_id or draft.cluster_name or 'Not assigned'}"
                    + (f" · instance {draft.cluster_instance_number}" if draft.cluster_instance_number else ""),
                )
            )
        lines.extend(
            (
                f"Launch command: {draft.launch_command or 'Use configured application path'}",
                f"Launch at FIO startup: {'Yes' if draft.launch_at_startup else 'No'}",
                (
                    "External configuration: eligible for reviewed native apply"
                    if draft.family_key == "js8call" and draft.variant and draft.version and draft.configuration_path
                    else "External configuration: operator action required unless an exact supported writer is qualified"
                ),
            )
        )
        self.review_label.setText("\n".join(lines))
        findings = self.validation()
        if findings:
            self.conflict_label.setText("Review findings:\n" + "\n".join(f"• {item.severity.title()}: {item.title} — {item.detail}" for item in findings))
        else:
            self.conflict_label.setText("No name, endpoint, or path conflicts found in the loaded inventory. Confirm before saving.")

    def _refresh(self) -> None:
        self.pages.setCurrentIndex(self._step)
        self.step_label.setText(f"Step {self._step + 1} of {len(self.STEP_TITLES)} · {self.STEP_TITLES[self._step]}")
        self.back_button.setEnabled(self._step > 0)
        last_step = len(self.STEP_TITLES) - 1
        final_action = "Apply to radio draft" if self._unsaved_owner_key else "Add instance"
        self.next_button.setText(final_action if self._step == last_step else "Next")
        blocked_for_radio = not self._selected_radio_id and not self._unsaved_owner_key
        blocked_for_replacement = self._replacement_instance is not None and not self._replacement_confirmed
        self.next_button.setEnabled(
            not blocked_for_radio
            and not blocked_for_replacement
            and not (self._step == last_step and any(item.severity == "error" for item in self.validation()))
        )
        theme = active_app_theme()
        for index, button in enumerate(self.step_buttons):
            current = index == self._step
            next_available = index == self._step + 1 and self.next_button.isEnabled()
            button.setChecked(current)
            button.setEnabled(index <= self._step or next_available)
            button.setStyleSheet(
                button_style(
                    "primary" if current else "secondary" if index < self._step else "muted",
                    theme,
                )
            )
        if self._step == 1:
            self._refresh_source()
        if self._step == last_step:
            self._refresh_review()

    def _select_step(self, target: int) -> None:
        """Navigate through the visible step strip without skipping gates."""

        index = int(target)
        if index < 0 or index >= len(self.STEP_TITLES):
            return
        if index == self._step:
            self._refresh()
            return
        if index > self._step + 1:
            return
        if index == self._step + 1 and not self.next_button.isEnabled():
            return
        if self._step == 2 and index > self._step:
            self._apply_identity_defaults()
        self._step = index
        self._refresh()

    def _next(self) -> None:
        if self._step < len(self.STEP_TITLES) - 1:
            if self._step == 2:
                self._apply_identity_defaults()
            self._step += 1
            self._refresh()
            return
        draft = self.draft()
        self.validation_requested.emit(draft.payload())
        if not any(item.severity == "error" for item in self.validation()):
            self.completed.emit(draft.payload())

    def _back(self) -> None:
        if self._step > 0:
            self._step -= 1
            self._refresh()


__all__ = [
    "InstanceConflict",
    "SUPPORTED_INSTANCE_FAMILIES",
    "SoftwareInstanceAssistant",
    "SoftwareInstanceDraft",
    "instance_conflicts",
    "normalize_instance_draft",
]
