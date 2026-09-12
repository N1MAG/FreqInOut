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
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)


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
        {"instance_name", "ownership", "rig_name", "host", "port", "udp_port", "application_path", "configuration_path", "storage_path", "launch_command", "launch_at_startup", "notes"}
    ),
    "fast_light": frozenset(
        {"instance_name", "ownership", "host", "port", "secondary_port", "application_path", "secondary_application_path", "configuration_path", "secondary_configuration_path", "storage_path", "secondary_storage_path", "launch_command", "launch_at_startup", "notes"}
    ),
    "varac": frozenset(
        {"instance_name", "ownership", "application_path", "configuration_path", "storage_path", "secondary_storage_path", "outbox_path", "cluster_id", "cluster_instance_number", "launch_command", "launch_at_startup", "notes"}
    ),
}
_FAMILY_FIELD_LABELS = {
    "js8call": {
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
        "cluster_id": "VarAC cluster",
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
    mode: str = "managed"
    ownership: str = "fio-managed"
    host: str = "127.0.0.1"
    port: int = 0
    udp_port: int = 0
    secondary_port: int = 0
    rig_name: str = ""
    application_path: str = ""
    secondary_application_path: str = ""
    configuration_path: str = ""
    secondary_configuration_path: str = ""
    storage_path: str = ""
    secondary_storage_path: str = ""
    outbox_path: str = ""
    launch_command: str = ""
    launch_at_startup: bool = False
    cluster_id: str = ""
    cluster_instance_number: int = 0
    notes: str = ""
    imported_id: Optional[int] = None
    imported_system_key: str = ""

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
            ):
                if value:
                    resources.append({"kind": kind, "value": value, "exclusive": True})
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
            "cluster_id": self.cluster_id,
            "cluster_instance_number": int(self.cluster_instance_number or 0),
            "resource_claims": resources,
            "notes": self.notes,
            "imported_id": self.imported_id,
            "imported_system_key": self.imported_system_key,
        }
        if self.family_key == "js8call":
            payload.update(
                {
                    "js8_rig_name": self.rig_name,
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
        mode=mode,
        ownership=ownership,
        host=_text(row.get("host") or "127.0.0.1"),
        port=_int(row.get("port") or row.get("js8_tcp_port") or row.get("flrig_port")) or 0,
        udp_port=_int(row.get("udp_port") or row.get("js8_udp_port")) or 0,
        secondary_port=_int(row.get("secondary_port") or row.get("fldigi_port")) or 0,
        rig_name=_text(row.get("rig_name") or row.get("js8_rig_name")),
        application_path=_text(row.get("application_path") or row.get("install_path") or row.get("path") or row.get("js8_application_path") or row.get("flrig_path") or row.get("varac_install_path")),
        secondary_application_path=_text(row.get("secondary_application_path") or row.get("fldigi_path")),
        configuration_path=_text(row.get("configuration_path") or row.get("ini_path") or row.get("profile_path") or row.get("js8_profile_path") or row.get("flrig_config_path") or row.get("varac_ini_path")),
        secondary_configuration_path=_text(row.get("secondary_configuration_path") or row.get("fldigi_config_path")),
        storage_path=_text(row.get("storage_path") or family_storage),
        secondary_storage_path=_text(row.get("secondary_storage_path") or family_secondary_storage),
        outbox_path=_text(row.get("outbox_path") or row.get("outbox_dir") or row.get("varac_outbox_dir")),
        launch_command=_text(row.get("launch_command") or row.get("launch_cmd")),
        launch_at_startup=_bool(row.get("launch_at_startup", False)),
        cluster_id=_text(row.get("cluster_id") or row.get("varac_cluster_id")),
        cluster_instance_number=_int(row.get("cluster_instance_number") or row.get("varac_cluster_instance_number")) or 0,
        notes=_text(row.get("notes")),
        imported_id=_int(row.get("imported_id") or row.get("id")),
        imported_system_key=_text(row.get("imported_system_key") or row.get("system_key")),
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
    if current.radio_id is None:
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
        if current.host and current.port and current.port == current.secondary_port:
            conflicts.append(
                InstanceConflict(
                    "fast_light_endpoint_overlap",
                    "error",
                    "FLRig and FLDigi endpoints overlap",
                    "Assign different local TCP ports to FLRig and FLDigi.",
                )
            )
        if current.launch_at_startup and (
            not current.configuration_path or not current.secondary_configuration_path
        ):
            conflicts.append(
                InstanceConflict(
                    "fast_light_profile_required",
                    "error",
                    "Fast Light profile folders are required",
                    "Startup needs distinct FLRig and FLDigi configuration folders so a second instance cannot reuse the default profile.",
                )
            )
    if current.family_key == "varac":
        if bool(current.cluster_id) != bool(current.cluster_instance_number):
            conflicts.append(InstanceConflict("cluster_pair_required", "error", "Cluster selection is incomplete", "Choose both a VarAC cluster and a positive instance number, or leave both blank for a standalone node."))
        if current.cluster_id and not current.launch_command:
            conflicts.append(InstanceConflict("cluster_launch_required", "error", "Cluster launch command is required", "Use an instance-specific VarAC launch command so FIO cannot start the default node by mistake."))

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
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self._family_key = _text(family_key).lower()
        self._existing_instances = tuple(dict(row) for row in existing_instances if isinstance(row, Mapping))
        self._radios = tuple(dict(row) for row in radios if isinstance(row, Mapping))
        self._varac_clusters = tuple(dict(row) for row in varac_clusters if isinstance(row, Mapping))
        self._selected_radio_id = _int(selected_radio_id)
        self._imported_id: Optional[int] = None
        self._imported_system_key = ""
        self._discovery_selected = False
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
        self._refresh()

    def _build_ui(self) -> None:
        self.setAccessibleName("Add software instance assistant")
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(7)
        self.title_label = QLabel("Add a software instance")
        self.title_label.setStyleSheet("font-size: 16pt; font-weight: 700;")
        root.addWidget(self.title_label)
        self.guidance_label = QLabel("Set up one distinct application instance. Nothing is saved until you review and confirm.")
        self.guidance_label.setWordWrap(True)
        root.addWidget(self.guidance_label)
        self.operation_status_label = QLabel()
        self.operation_status_label.setWordWrap(True)
        self.operation_status_label.setAccessibleName("Software instance operation status")
        self.status_label = self.operation_status_label
        root.addWidget(self.operation_status_label)
        self.step_label = QLabel()
        self.step_label.setAccessibleName("Software instance setup step")
        root.addWidget(self.step_label)

        self.pages = QStackedWidget()
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
        layout.addWidget(QLabel("Choose the application family for this instance."))
        self.family_combo = QComboBox()
        self.family_combo.setAccessibleName("Software family")
        for key, label in SUPPORTED_INSTANCE_FAMILIES:
            self.family_combo.addItem(label, key)
        self.family_combo.currentIndexChanged.connect(lambda _index: self._load_family(str(self.family_combo.currentData() or "")))
        layout.addWidget(self.family_combo)
        radio_group = QGroupBox("Radio context (required)")
        radio_layout = QVBoxLayout(radio_group)
        self.radio_combo = QComboBox()
        self.radio_combo.setAccessibleName("Radio for software instance")
        self.radio_combo.addItem("Not assigned yet", None)
        for row in self._radios:
            rid = _int(row.get("id"))
            if rid:
                self.radio_combo.addItem(_text(row.get("name")) or f"Radio {rid}", rid)
        radio_layout.addWidget(self.radio_combo)
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
            (("instance_name", "Instance name", "A distinct name for this application instance"),),
        )
        ownership = QComboBox()
        ownership.addItem("FIO-managed launch (external settings unchanged)", "fio-managed")
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
                ("configuration_path", "Configuration / profile / INI", "Profile, configuration, or VarAC INI"),
                ("secondary_configuration_path", "FLDigi configuration", "FLDigi profile/configuration"),
                ("storage_path", "Data / database / log folder", "Instance-owned data, database, or FLDigi logs"),
                ("secondary_storage_path", "Incoming / check-in folder", "VarAC incoming or FLDigi check-in folder"),
                ("outbox_path", "VarAC outbox", "VarAC outbox folder"),
                ("cluster_id", "VarAC cluster ID", "Optional cluster identifier"),
                ("cluster_instance_number", "VarAC cluster instance", "Optional positive instance number"),
            ),
        )

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

    def _add_line(self, key: str, label: str, placeholder: str) -> None:
        if key == "cluster_id":
            combo = QComboBox()
            combo.setEditable(False)
            combo.setAccessibleName(label)
            combo.setToolTip("Choose a configured cluster, or leave this blank for a standalone VarAC node.")
            combo.addItem("No cluster (standalone)", "")
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
                detail
                + " Existing settings are evidence for review. FIO does not claim to write third-party application configuration unless a supported, explicit apply is provided."
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

    def _sync_family_fields(self) -> None:
        visible = _FAMILY_FIELDS.get(self._family_key, frozenset())
        labels = _FAMILY_FIELD_LABELS.get(self._family_key, {})
        for key, widget in self._field_widgets.items():
            shown = key in visible
            widget.setVisible(shown)
            label = self._field_labels.get(key)
            if label is not None:
                if key in labels and isinstance(label, QLabel):
                    label.setText(labels[key])
                label.setVisible(shown)

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
        self._imported_id = draft.imported_id
        self._imported_system_key = draft.imported_system_key
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

    def draft(self) -> SoftwareInstanceDraft:
        def value(key: str) -> str:
            widget = self._field_widgets.get(key)
            if isinstance(widget, QLineEdit):
                return widget.text().strip()
            if isinstance(widget, QComboBox):
                return str(widget.currentData() or "").strip()
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
            mode=next((key for key, button in self.source_buttons.items() if button.isChecked()), "managed"),
            ownership=str(self._field_widgets["ownership"].currentData() or "fio-managed"),
            host=value("host"),
            port=number("port"),
            udp_port=number("udp_port"),
            secondary_port=number("secondary_port"),
            rig_name=value("rig_name"),
            application_path=value("application_path"),
            secondary_application_path=value("secondary_application_path"),
            configuration_path=value("configuration_path"),
            secondary_configuration_path=value("secondary_configuration_path"),
            storage_path=value("storage_path"),
            secondary_storage_path=value("secondary_storage_path"),
            outbox_path=value("outbox_path"),
            launch_command=value("launch_command"),
            launch_at_startup=checked("launch_at_startup"),
            cluster_id=value("cluster_id"),
            cluster_instance_number=number("cluster_instance_number"),
            notes=value("notes"),
            imported_id=self._imported_id,
            imported_system_key=self._imported_system_key,
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
        if draft.family_key == "js8call":
            lines.extend(
                (
                    f"Rig name: {draft.rig_name or 'Not set'}",
                    f"TCP API: {endpoint}",
                    f"UDP: {draft.host}:{draft.udp_port}" if draft.udp_port else "UDP: Not configured",
                    f"Application: {draft.application_path or 'Not set'}",
                    f"Settings profile: {draft.configuration_path or 'Not set'}",
                    f"Message data: {draft.storage_path or 'Not set'}",
                )
            )
        elif draft.family_key == "fast_light":
            lines.extend(
                (
                    f"FLRig endpoint: {endpoint}",
                    f"FLDigi endpoint: {draft.host}:{draft.secondary_port}" if draft.secondary_port else "FLDigi endpoint: Not configured",
                    f"FLRig application/config: {draft.application_path or 'Not set'} · {draft.configuration_path or 'Not set'}",
                    f"FLDigi application/config: {draft.secondary_application_path or 'Not set'} · {draft.secondary_configuration_path or 'Not set'}",
                    f"FLDigi logs/check-ins: {draft.storage_path or 'Not set'} · {draft.secondary_storage_path or 'Not set'}",
                )
            )
        else:
            lines.extend(
                (
                    f"Application: {draft.application_path or 'Not set'}",
                    f"INI: {draft.configuration_path or 'Not set'}",
                    f"Database: {draft.storage_path or 'Not set'}",
                    f"Incoming/outbox: {draft.secondary_storage_path or 'Not set'} · {draft.outbox_path or 'Not set'}",
                    f"Cluster: {draft.cluster_id or 'Not assigned'}"
                    + (f" · instance {draft.cluster_instance_number}" if draft.cluster_instance_number else ""),
                )
            )
        lines.extend(
            (
                f"Launch command: {draft.launch_command or 'Use configured application path'}",
                f"Launch at FIO startup: {'Yes' if draft.launch_at_startup else 'No'}",
                "External configuration write: None from this review",
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
        self.next_button.setText("Add instance" if self._step == last_step else "Next")
        self.next_button.setEnabled(
            not (self._step == last_step and any(item.severity == "error" for item in self.validation()))
        )
        if self._step == 1:
            self._refresh_source()
        if self._step == last_step:
            self._refresh_review()

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
