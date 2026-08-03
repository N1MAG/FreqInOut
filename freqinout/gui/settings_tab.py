from __future__ import annotations

import datetime
import json
import platform
import subprocess
import sqlite3
import os
import sys
import time
import tempfile
import zipfile
import re
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple, Mapping, Sequence

from PySide6.QtCore import Qt, QTimer, Signal, QSize
from PySide6.QtGui import QAction, QIcon, QIntValidator, QColor, QBrush, QPainter, QPainterPath, QPen, QPixmap
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QComboBox,
    QCheckBox,
    QFileDialog,
    QMessageBox,
    QGroupBox,
    QFormLayout,
    QApplication,
    QDialog,
    QDialogButtonBox,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QPlainTextEdit,
    QProgressBar,
    QHeaderView,
    QSizePolicy,
    QAbstractScrollArea,
    QAbstractItemView,
    QScrollArea,
    QCompleter,
    QFrame,
    QToolButton,
    QToolTip,
    QListWidget,
    QListWidgetItem,
    QStackedWidget,
    QStyledItemDelegate,
    QStyle,
)

from freqinout.core.logger import log, set_log_level, get_log_level, _get_log_file
from freqinout.core.perf_metrics import emit_span, span as perf_span
from freqinout.core.checkins_db import ensure_operator_checkins_schema, get_all_operators as get_shared_operators
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.config_paths import get_fldigi_checkin_dir, get_config_dir
from freqinout.core.config_autodiscovery import (
    DEFAULT_PORT_PLAN,
    PortAssignment,
    RadioInstanceProposal,
    build_autoconfig_proposal,
    discover_js8call_file_profiles,
    select_js8call_file_profile,
)
from freqinout.core.config_backup import create_config_backup
from freqinout.core.config_migration_preview import (
    build_single_rig_upgrade_apply_plan,
    build_single_rig_upgrade_preview,
)
from freqinout.core.local_ops_store import get_all_operators as get_local_operators
from freqinout.core.system_timezone import detect_system_timezone_name
from freqinout.core.js8_defaults import coerce_js8_offset_hz
from freqinout.core.launch_orchestrator import (
    DEFAULT_LAUNCH_READINESS_TIMEOUT_SEC,
    LAUNCH_APP_ORDER,
    LaunchOrchestrator,
)
from freqinout.core.dependency_status_service import get_dependency_status_service
from freqinout.core.software_path_detector import SoftwarePathDetector, PathDetectionResult
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.gpg_tools import (
    clearsign_file,
    gpg_detail_indicates_passphrase_needed,
    gpg_key_display_label,
    gpg_available,
    import_public_key_file,
    import_public_key_text,
    list_public_keys,
    list_secret_keys,
    local_sign_key,
    normalize_fingerprint,
)
from freqinout.core.guided_radio_autofill import (
    guided_app_candidate_choices,
    guided_app_candidate_identity,
    guided_detection_path,
    guided_js8_profile_choices,
    guided_js8_profile_review_text,
    guided_port_prompt_keys,
    guided_radio_autofill_suggestions,
    guided_single_install_path,
    next_default_instance_port,
)
from freqinout.core.guided_app_config_plan import build_guided_external_app_config_plan
from freqinout.core.secret_store import (
    credential_store_available,
    delete_gpg_signing_passphrase,
    has_gpg_signing_passphrase,
    store_gpg_signing_passphrase,
)
from freqinout.core.hash_tools import (
    infer_algorithm_from_hash,
    normalize_hash_algorithm,
    normalize_hash_hex,
    normalize_trusted_hash_entries,
)
from freqinout.core.js8_spotter_forms import (
    MAPPER_SETTINGS_KEY,
    PURPOSE_OPTIONS,
    discover_spotter_forms,
    effective_mapping_rows,
    factory_mapping_for_form,
    normalize_mapping_rows,
)
from freqinout.core.multi_radio_store import (
    DEFAULT_HOLD_DURATION_MINUTES,
    DEFAULT_OPERATING_NAME,
    MultiRadioStore,
    SUPPORTED_HOLD_DURATION_MINUTES,
    SUPPORTED_RUNTIME_CONTROL_BACKENDS,
    ensure_multi_rig_migration,
    multi_rig_guardrail_warnings,
)
from freqinout.core.multi_rig_guardrails import MultiRigGuardrailWarning, collect_multi_rig_guardrail_warnings
from freqinout.core.multi_rig_runtime_status import (
    STARTUP_DEFERRED,
    STARTUP_EXISTING_UNMIGRATED,
    STARTUP_FRESH_DEFAULT_READY,
    STARTUP_MIGRATED,
    STARTUP_MIGRATION_ERROR,
    MultiRigRuntimeStatus,
    build_multi_rig_runtime_status,
)
from freqinout.core.mode_utils import normalize_operating_group_mode, voice_sideband_for_band
from freqinout.core.radio_catalog import catalog_entry_control_methods, find_radio_catalog_entry, load_radio_catalog
from freqinout.core.station_readiness import (
    build_station_readiness_report,
    format_readiness_issue,
    readiness_report_detail_text,
    readiness_report_overall_text,
    readiness_state_card_level,
    readiness_state_description,
    readiness_state_label,
    readiness_summary_badge_text,
    readiness_summary_status_text,
    visible_status_programs,
)
from freqinout.core.shared_state import ActionFeedbackService
from freqinout.core.varac_bbs_config import (
    bbs_summary_text,
    format_callsign_list,
    get_varac_ini_sync_state,
    load_varac_bbs_config,
    locate_varac_ini_path,
    varac_path_to_host_path,
    varac_ini_sync_state_matches,
    varac_ini_sync_state_to_json,
    write_varac_bbs_config,
)
from freqinout.core.varac_bbs_vault import (
    DEFAULT_ACCESS_CODE_ITERATIONS,
    DEFAULT_BBS_REFRESH_PAUSE_SECONDS,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_FAILED_ATTEMPT_LIMIT,
    DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS,
    DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS,
    DEFAULT_GLOBAL_CODE_POLICY,
    DEFAULT_IDLE_TIMEOUT_SECONDS,
    DEFAULT_LOCATION_ID,
    DEFAULT_LOCATION_NAME,
    DEFAULT_RETURN_MODE,
    DEFAULT_TRIGGER_MODE,
    VaultLocation,
    VaultRuntimeState,
    apply_unlock_request,
    compute_default_managed_root,
    hash_access_code,
    import_live_bbs_to_default_location,
    initialize_managed_root,
    load_vault_locations,
    load_vault_runtime_state,
    normalize_location_alias,
    publish_location,
    publish_location_view,
    publish_root_view,
    reset_to_default_location,
    root_location_helper_filename_preview,
    vault_locations_to_data,
    vault_runtime_state_to_data,
)
from freqinout.utils.timezones import get_timezone
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer
from freqinout.gui.help_registry import resolve_help_host
from freqinout.gui.theme import (
    resolve_theme,
    normalize_ui_text_size,
    led_style,
    button_style,
    fit_combo_box_to_contents,
)
from freqinout.version import __version__


def _vault_location_requires_code_badge(
    row: Dict[str, object],
    *,
    default_location_id: str,
    global_code_policy: str,
) -> bool:
    location_id = str(row.get("id", "") or "").strip()
    open_rule = str(row.get("open_rule", "Public") or "Public").strip() or "Public"
    if location_id == default_location_id:
        return False
    if open_rule == "Allowed callsigns + access code":
        return True
    if global_code_policy == "Require for non-default locations" and open_rule != "Public":
        return True
    if global_code_policy == "Require for all restricted locations" and open_rule != "Public":
        return True
    return False


class _SettingsSectionNavDelegate(QStyledItemDelegate):
    def __init__(self, owner: "SettingsTab") -> None:
        super().__init__(owner)
        self._owner = owner

    def sizeHint(self, option, index) -> QSize:  # type: ignore[override]
        base = super().sizeHint(option, index)
        font_h = option.fontMetrics.height() if hasattr(option, "fontMetrics") else 16
        height = max(int(base.height() or 0), int(font_h) + 12, 32)
        width = max(int(base.width() or 0), int(option.fontMetrics.horizontalAdvance(str(index.data() or ""))) + 22)
        return QSize(width, height)

    def paint(self, painter: QPainter, option, index) -> None:  # type: ignore[override]
        theme = resolve_theme(self._owner.settings)
        state = str(index.data(self._owner.SECTION_HEALTH_STATE_ROLE) or "neutral").strip().lower()
        selected = bool(option.state & QStyle.State_Selected)
        hovered = bool(option.state & QStyle.State_MouseOver)
        visuals = self._owner._section_nav_visuals(state, selected=selected, hovered=hovered, theme=theme)

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)

        rect = option.rect.adjusted(3, 1, -3, -1)
        painter.setPen(QPen(visuals["border"]))
        painter.setBrush(visuals["bg"])
        painter.drawRoundedRect(rect, 6, 6)

        font = option.font
        font.setBold(bool(visuals["bold"]))
        painter.setFont(font)
        painter.setPen(QPen(visuals["fg"]))
        painter.drawText(rect.adjusted(10, 1, -8, -1), Qt.AlignVCenter | Qt.AlignLeft, str(index.data() or ""))

        painter.restore()


class _CustomToolDialog(QDialog):
    def __init__(self, parent: QWidget | None = None, *, name: str = "", command: str = "") -> None:
        super().__init__(parent)
        self.setWindowTitle("Custom Tool")
        layout = QVBoxLayout(self)
        hint = QLabel("Set a display name and the launch command FreqInOut should run for this tool.")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        form = QFormLayout()
        self.name_edit = QLineEdit(name)
        self.command_edit = QLineEdit(command)
        self.command_edit.setPlaceholderText("python /path/to/tool.py or /path/to/script.sh")
        form.addRow("Tool Name", self.name_edit)
        form.addRow("Launch Command", self.command_edit)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.resize(560, self.sizeHint().height())

    def values(self) -> tuple[str, str]:
        return self.name_edit.text().strip(), self.command_edit.text().strip()


def _coerce_json_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


TIMEZONE_CHOICES = [
    "UTC",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
]

DEVICE_CLASS_OPTIONS = [
    ("Transceiver", "tx_rx"),
    ("Observer / SDR", "observer"),
    ("Gateway", "gateway"),
]

OPERATING_SCHEDULER_MODE_OPTIONS = [
    ("Full FIO Workflow", "full"),
    ("Simple", "simple"),
]

OPERATING_ASSIGNMENT_STATE_OPTIONS = [
    ("Active", "active"),
    ("Temporary Override", "temporary_override"),
]

FLDIGI_MODE_OPTIONS = [
    "Cont-4/250",
    "MFSK32",
    "SSB",
    "FSQ",
    "CW",
    "WWV",
    "WEFAX576",
    "WEFAX288",
    "Cont-4/125",
    "Cont-4/500",
    "Cont-4/1K",
    "Cont-4/2K",
    "Cont-8/125",
    "Cont-8/250",
    "Cont-8/500",
    "Cont-8/1K",
    "Cont-8/2K",
    "Cont-16/250",
    "Cont-16/500",
    "Cont-16/1K",
    "Cont-16/2K",
    "Cont-32/1K",
    "Cont-32/2K",
    "Cont-64/500",
    "Cont-64/1K",
    "Cont-64/2K",
    "DOMEXM",
    "DOMEX4",
    "DOMEX5",
    "DOMEX8",
    "DOMX11",
    "DOMX16",
    "DOMX22",
    "DOMX44",
    "DOMX88",
    "FELDHELL",
    "SLOWHELL",
    "HELLX5",
    "HELLX9",
    "FSKH245",
    "FSKH105",
    "HELL80",
    "MFSK8",
    "MFSK16",
    "MFSK4",
    "MFSK11",
    "MFSK22",
    "MFSK31",
    "MFSK64",
    "MFSK128",
    "MFSK64L",
    "MFSK128L",
    "NAVTEX",
    "SITORB",
    "MT63-500S",
    "MT63-500L",
    "MT63-1KS",
    "MT63-1KL",
    "MT63-2KS",
    "MT63-2KL",
    "BPSK31",
    "BPSK63",
    "BPSK63F",
    "BPSK125",
    "BPSK250",
    "BPSK500",
    "BPSK1000",
    "PSK125C12",
    "PSK250C6",
    "PSK500C2",
    "PSK500C4",
    "PSK800C2",
    "PSK1000C2",
    "QPSK31",
    "QPSK63",
    "QPSK125",
    "QPSK250",
    "QPSK500",
    "8PSK125",
    "8PSK125FL",
    "8PSK125F",
    "8PSK250",
    "8PSK250FL",
    "8PSK250F",
    "8PSK500",
    "8PSK500F",
    "8PSK1000",
    "8PSK1000F",
    "8PSK1200F",
    "OFDM500F",
    "OFDM750F",
    "OFDM3500",
    "OLIVIA",
    "OLIVIA-4/125",
    "OLIVIA-4/250",
    "OLIVIA-4/500",
    "OLIVIA-4/1K",
    "OLIVIA-4/2K",
    "OLIVIA-8/125",
    "OLIVIA-8/250",
    "OLIVIA-8/500",
    "OLIVIA-8/1K",
    "OLIVIA-8/2K",
    "OLIVIA-16/500",
    "OLIVIA-16/1K",
    "OLIVIA-16/2K",
    "OLIVIA-32/1K",
    "OLIVIA-32/2K",
    "OLIVIA-64/500",
    "OLIVIA-64/1K",
    "OLIVIA-64/2K",
    "RTTY",
    "THORM",
    "THOR4",
    "THOR5",
    "THOR8",
    "THOR11",
    "THOR16",
    "THOR22",
    "THOR32",
    "THOR44",
    "THOR56",
    "THOR25x4",
    "THOR50x1",
    "THOR50x2",
    "THOR100",
    "THROB1",
    "THROB2",
    "THROB4",
    "THRBX1",
    "THRBX2",
    "THRBX4",
    "PSK125R",
    "PSK250R",
    "PSK500R",
    "PSK1000R",
    "PSK63RC4",
    "PSK63RC5",
    "PSK63RC10",
    "PSK63RC20",
    "PSK63RC32",
    "PSK125RC4",
    "PSK125RC5",
    "PSK125RC10",
    "PSK125RC12",
    "PSK125RC16",
    "PSK250RC2",
    "PSK250RC3",
    "PSK250RC5",
    "PSK250RC6",
    "PSK250RC7",
    "PSK500RC2",
    "PSK500RC3",
    "PSK500RC4",
    "PSK800RC2",
    "PSK1000RC2",
    "IFKP",
]

LOCAL_NET_RESOURCE_OPTIONS = [
    "VHF Simplex",
    "VHF Repeater",
    "UHF Simplex",
    "UHF Repeater",
    "GMRS Simplex",
    "GMRS Repeater",
    "FRS",
    "MURS",
    "Meshtastic",
    "Other",
]
# Backward-compat alias for legacy references.
LOCAL_NET_SERVICE_OPTIONS = LOCAL_NET_RESOURCE_OPTIONS


class SettingsTab(QWidget):
    """
    Global settings for FreqInOut.

    - Call sign / Name / State
    - Control mode
    - JS8Call TCP port
    - Primary JS8Call groups
    - JS8Call DIRECTED.TXT path
    - Radio software paths

    Timezone is *not* user selectable here; it is auto-detected from the
    system clock and stored under the 'timezone' key in SettingsManager.
    All entries are saved to config when:
      - The Save button is clicked, OR
      - The application exits (QApplication.aboutToQuit).

    Persistence is done via SettingsManager.set(...) when available,
    or by updating SettingsManager._data as a fallback. We *do not*
    call any .write() or .save() here to avoid AttributeError.
    """

    settings_saved = Signal()
    device_profiles_changed = Signal()
    local_net_profiles_changed = Signal()
    open_logs_requested = Signal()
    log_level_changed = Signal(str)
    SECTION_HEALTH_STATE_ROLE = int(Qt.UserRole) + 1
    SECTION_HEALTH_KEY_ROLE = int(Qt.UserRole) + 2
    SECTION_STACK_INDEX_ROLE = int(Qt.UserRole) + 3
    SECTION_SCOPE_ROLE = int(Qt.UserRole) + 4

    def __init__(self, parent=None, action_feedback_service: ActionFeedbackService | None = None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self.action_feedback_service = action_feedback_service or ActionFeedbackService()
        self._last_action_feedback_event = None
        self.software_path_detector = SoftwarePathDetector(self.settings)
        self._settings_dirty = False
        self._loading_settings = False
        self._shutdown_autosave = False
        self._op_group_condition_sync = False
        self._op_group_rows_by_group: Dict[str, List[int]] = {}
        self.loading_label: QLabel | None = None
        self._status_service = get_dependency_status_service(self.settings)
        self._software_status_probe = SoftwareStatusService(self.settings)
        try:
            self._status_service.snapshot_changed.connect(self._on_dependency_status_snapshot_changed)
        except Exception:
            pass
        self.launch_orchestrator = LaunchOrchestrator(self.settings, self)
        self.multi_radio_store = MultiRadioStore()

        self.PROGRAMS: Dict[str, Dict[str, str]] = {
            "FLRig": {"setting_key": "path_flrig", "autostart_key": "autostart_flrig"},
            "FLDigi": {"setting_key": "path_fldigi", "autostart_key": "autostart_fldigi"},
            "FLMsg": {"setting_key": "path_flmsg", "autostart_key": "autostart_flmsg"},
            "FLAmp": {"setting_key": "path_flamp", "autostart_key": "autostart_flamp"},
            # JS8Call is managed externally; no launch/autostart controls here.
        }

        self.radio_checkboxes: Dict[str, QCheckBox] = {}
        self.status_labels: Dict[str, QLabel] = {}
        self._status_text_labels: Dict[str, QLabel] = {}
        self.path_edits: Dict[str, QLineEdit] = {}
        self._autofill_status_labels: Dict[str, QLabel] = {}
        self._autofill_status_buttons: Dict[str, QPushButton] = {}
        self._autofill_preserved_buttons: Dict[str, QPushButton] = {}
        self._autofill_replace_buttons: Dict[str, QPushButton] = {}
        self._autofill_dismiss_buttons: Dict[str, QPushButton] = {}
        self._autofill_action_rows: Dict[str, QWidget] = {}
        self._autofill_review_tables: Dict[str, QTableWidget] = {}
        self._autofill_compact_status_texts: Dict[str, str] = {}
        self._autofill_full_status_texts: Dict[str, str] = {}
        self._autofill_status_expanded: Dict[str, bool] = {}
        self._autofill_preserved_suggestions: Dict[str, List[Dict[str, str]]] = {}
        self._contextual_autofill_buttons: Dict[str, QPushButton] = {}
        self._contextual_autofill_rules: Dict[str, Dict[str, object]] = {}
        self._radio_profile_software_flag_checks: Dict[str, QCheckBox] = {}
        self._refreshing_radio_profile_software_flags = False
        self._radio_profile_timer_policy_controls: Dict[str, QWidget] = {}
        self._refreshing_radio_profile_timer_policy = False
        self.js8_groups_edits: List[QLineEdit] = []
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self.device_profiles: List[Dict[str, Any]] = []
        self.operating_profiles: List[Dict[str, Any]] = []
        self.device_assignments: List[Dict[str, Any]] = []
        self.varac_clusters: List[Dict[str, Any]] = []
        self.varac_cluster_members: List[Dict[str, Any]] = []
        self.active_profile_swap: Optional[Dict[str, Any]] = None
        self.operating_groups: List[Dict[str, str]] = []
        self.local_net_profiles: List[Dict[str, str]] = []
        self._accordion_groups: List[QGroupBox] = []
        self._section_meta: Dict[QGroupBox, Dict[str, object]] = {}
        self._section_nav_items: Dict[QGroupBox, QListWidgetItem] = {}
        self._section_nav_buttons: Dict[QGroupBox, QPushButton] = {}
        self._refreshing_settings_section_combo = False
        self._global_settings_nav_collapsed = True
        self._radio_settings_nav_collapsed = False
        self._context_help_buttons: List[QPushButton] = []
        self._custom_tool_items_cache: List[Dict[str, str]] = []
        self._custom_tools_table_loading = False
        self._launch_items_cache: List[Dict[str, object]] = []
        self._launch_visible_names: List[str] = []
        self._launch_table_loading = False
        self._device_profiles_table_loading = False
        self._operating_profiles_table_loading = False
        self._device_assignments_table_loading = False
        self._varac_clusters_table_loading = False
        self._varac_cluster_members_table_loading = False
        self._varac_bbs_lookup_rows: List[Dict[str, str]] = []
        self._varac_bbs_lookup_by_callsign: Dict[str, Dict[str, str]] = {}
        self._varac_bbs_vault_locations_cache: List[Dict[str, object]] = []
        self._varac_bbs_vault_selected_location_id = ""
        self._varac_bbs_vault_runtime_state_cache: Dict[str, object] = {}
        self._varac_bbs_vault_last_summary_cache = ""
        self._varac_bbs_vault_editor_loading = False
        self._varac_bbs_vault_auto_source_dir = ""
        self._varac_bbs_vault_auto_description = ""
        self._varac_bbs_vault_auto_flamp_relay_dir = ""
        self._varac_bbs_vault_root_loading = False
        self._last_varac_bbs_dir_for_root_sync = ""
        self._gpg_keys_table_loading = False
        self._gpg_signing_keys_loading = False
        self._gpg_keys_loaded = False
        self._gpg_keys_auto_probe_attempted = False
        self._gpg_trusted_fingerprints: set[str] = set()
        self._trusted_hashes_table_loading = False
        self._trusted_hash_entries: List[Dict[str, object]] = []
        self._spotter_mapper_loading = False
        self._settings_radio_focus_id: Optional[int] = None
        self._settings_radio_selector_buttons: Dict[int, QPushButton] = {}
        self._software_radio_combo_loading = False
        self._software_radio_current_id: Optional[int] = None
        self._software_radio_drafts: Dict[int, Dict[str, Any]] = {}
        self._multi_rig_runtime_status: MultiRigRuntimeStatus | None = None
        self._multi_rig_radio_catalog_payload: Dict[str, Any] | None = None
        self._active = False
        self._last_activation_refresh_ts = 0.0
        self._activation_refresh_interval_sec = 30.0
        self._last_running_status_refresh_ts = 0.0
        self._running_status_refresh_interval_sec = 10.0
        self._last_running_status_sig: Optional[Tuple[object, ...]] = None
        self._last_varac_bbs_lookup_reload_ts = 0.0
        self._varac_bbs_lookup_reload_interval_sec = 20.0
        self._last_section_stack_index = -1
        self._last_section_target_height = 0

        self._build_ui()
        self._load_settings()

        # Auto-save on application exit (no popup)
        app = QApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(self._save_settings_quiet)

        # time updater (UTC + detected timezone)
        self.time_timer = QTimer(self)
        self.time_timer.setInterval(1000)
        self.time_timer.timeout.connect(self._update_clock_labels)
        self.time_timer.start()

        # process status timer
        self.status_timer = QTimer(self)
        self.status_timer.setInterval(10000)
        self.status_timer.timeout.connect(self._refresh_running_status)

        self._update_clock_labels()
        QTimer.singleShot(0, self._maybe_backfill_js8_geo)

    def set_tab_active(self, active: bool) -> None:
        self._active = bool(active)
        if self._active:
            if not self.status_timer.isActive():
                self.status_timer.start()
            QTimer.singleShot(0, self.on_tab_activated)
            return
        if self.status_timer.isActive():
            self.status_timer.stop()

    def on_tab_activated(self) -> None:
        with perf_span("settings.on_tab_activated", settings=self.settings, min_ms=10.0):
            now_ts = time.time()
            if (now_ts - float(self._last_activation_refresh_ts or 0.0)) < float(
                self._activation_refresh_interval_sec
            ):
                return
            self._last_activation_refresh_ts = now_ts
            self._reload_varac_bbs_operator_lookup()
            self._refresh_running_status()

    # ---------- UI ---------- #

    def _normalize_varac_bbs_callsign(self, token: object) -> str:
        text = str(token or "").strip().upper()
        if not text:
            return ""
        if "/" in text:
            text = text.split("/", 1)[0].strip()
        else:
            text = text.split()[0].strip()
        return re.sub(r"[^A-Z0-9/]", "", text)

    def _varac_bbs_operator_display(self, callsign: str, name: str, state: str) -> str:
        parts = [str(callsign or "").strip().upper()]
        clean_name = str(name or "").strip()
        clean_state = str(state or "").strip().upper()
        if clean_name:
            parts.append(clean_name)
        if clean_state:
            parts.append(clean_state)
        return " / ".join([part for part in parts if part])

    def _reload_varac_bbs_operator_lookup(self, *, force: bool = False) -> None:
        now_ts = time.time()
        if (
            not force
            and self._varac_bbs_lookup_rows
            and (now_ts - float(self._last_varac_bbs_lookup_reload_ts or 0.0))
            < float(self._varac_bbs_lookup_reload_interval_sec)
        ):
            return
        merged: Dict[str, Dict[str, str]] = {}
        for loader in (get_shared_operators, get_local_operators):
            try:
                rows = loader()
            except Exception:
                rows = []
            for row in rows:
                callsign = self._normalize_varac_bbs_callsign(row.get("callsign", ""))
                if not callsign:
                    continue
                name = str(row.get("name", "") or "").strip()
                if not name:
                    first_name = str(row.get("first_name", "") or "").strip()
                    last_name = str(row.get("last_name", "") or "").strip()
                    name = " ".join([part for part in (first_name, last_name) if part]).strip()
                state = str(row.get("state", "") or "").strip().upper()
                current = merged.get(callsign, {"callsign": callsign, "name": "", "state": ""})
                if name and (not current.get("name") or len(name) > len(str(current.get("name") or ""))):
                    current["name"] = name
                if state and not current.get("state"):
                    current["state"] = state
                merged[callsign] = current
        self._varac_bbs_lookup_by_callsign = {
            callsign: merged[callsign] for callsign in sorted(merged.keys())
        }
        self._varac_bbs_lookup_rows = list(self._varac_bbs_lookup_by_callsign.values())
        if hasattr(self, "varac_bbs_callsign_lookup_edit"):
            entries = [
                self._varac_bbs_operator_display(
                    row.get("callsign", ""),
                    row.get("name", ""),
                    row.get("state", ""),
                )
                for row in self._varac_bbs_lookup_rows
            ]
            completer = QCompleter(entries, self.varac_bbs_callsign_lookup_edit)
            completer.setCaseSensitivity(Qt.CaseInsensitive)
            completer.setFilterMode(Qt.MatchContains)
            completer.setCompletionMode(QCompleter.PopupCompletion)
            self.varac_bbs_callsign_lookup_edit.setCompleter(completer)
        self._last_varac_bbs_lookup_reload_ts = now_ts

    def _varac_bbs_selected_callsigns_text(self) -> str:
        if not hasattr(self, "varac_bbs_callsigns_list"):
            return ""
        values: List[str] = []
        for idx in range(self.varac_bbs_callsigns_list.count()):
            item = self.varac_bbs_callsigns_list.item(idx)
            if not item:
                continue
            callsign = self._normalize_varac_bbs_callsign(item.data(Qt.UserRole) or item.text())
            if callsign:
                values.append(callsign)
        return format_callsign_list(values)

    def _refresh_varac_bbs_callsign_actions(self) -> None:
        has_text = bool(
            hasattr(self, "varac_bbs_callsign_lookup_edit")
            and self._normalize_varac_bbs_callsign(self.varac_bbs_callsign_lookup_edit.text())
        )
        has_selection = bool(
            hasattr(self, "varac_bbs_callsigns_list")
            and self.varac_bbs_callsigns_list.selectedItems()
        )
        if hasattr(self, "varac_bbs_add_callsign_btn"):
            self.varac_bbs_add_callsign_btn.setEnabled(has_text)
        if hasattr(self, "varac_bbs_remove_callsign_btn"):
            self.varac_bbs_remove_callsign_btn.setEnabled(has_selection)

    def _set_varac_bbs_allowed_callsigns(self, value: object) -> None:
        if not hasattr(self, "varac_bbs_callsigns_list"):
            return
        selected_callsigns = {
            self._normalize_varac_bbs_callsign(item.data(Qt.UserRole) or item.text())
            for item in self.varac_bbs_callsigns_list.selectedItems()
        }
        self.varac_bbs_callsigns_list.clear()
        for callsign in format_callsign_list(value).split(","):
            normalized = self._normalize_varac_bbs_callsign(callsign)
            if not normalized:
                continue
            meta = self._varac_bbs_lookup_by_callsign.get(normalized, {})
            item = QListWidgetItem(
                self._varac_bbs_operator_display(
                    normalized,
                    str(meta.get("name", "") or ""),
                    str(meta.get("state", "") or ""),
                )
            )
            item.setData(Qt.UserRole, normalized)
            item.setToolTip(
                "Known operator" if normalized in self._varac_bbs_lookup_by_callsign else "Manual callsign entry"
            )
            self.varac_bbs_callsigns_list.addItem(item)
            if normalized in selected_callsigns:
                item.setSelected(True)
        self._refresh_varac_bbs_callsign_actions()

    def _add_varac_bbs_allowed_callsign(self) -> None:
        if not hasattr(self, "varac_bbs_callsign_lookup_edit"):
            return
        callsign = self._normalize_varac_bbs_callsign(self.varac_bbs_callsign_lookup_edit.text())
        if not callsign:
            self._refresh_varac_bbs_callsign_actions()
            return
        current = self._varac_bbs_selected_callsigns_text()
        values = [entry.strip() for entry in current.split(",") if entry.strip()]
        values.append(callsign)
        self._set_varac_bbs_allowed_callsigns(values)
        self.varac_bbs_callsign_lookup_edit.clear()
        self._mark_settings_dirty()

    def _remove_selected_varac_bbs_allowed_callsigns(self) -> None:
        if not hasattr(self, "varac_bbs_callsigns_list"):
            return
        selected_rows = sorted(
            [self.varac_bbs_callsigns_list.row(item) for item in self.varac_bbs_callsigns_list.selectedItems()],
            reverse=True,
        )
        if not selected_rows:
            self._refresh_varac_bbs_callsign_actions()
            return
        for row in selected_rows:
            self.varac_bbs_callsigns_list.takeItem(row)
        self._refresh_varac_bbs_callsign_actions()
        self._mark_settings_dirty()

    def _normalize_varac_bbs_vault_location(self, value: object) -> Dict[str, object]:
        row = value if isinstance(value, dict) else {}
        name = " ".join(str(row.get("name", "") or "").strip().split())
        source_dir = str(row.get("source_dir", "") or "").strip()
        enabled = bool(row.get("enabled", True))
        inherit_allowed = bool(row.get("inherit_global_allowed_callsigns", True))
        allowed_callsigns = format_callsign_list(row.get("allowed_callsigns", []))
        alias = normalize_location_alias(row.get("alias", ""), name)
        description = str(row.get("description", "") or "").strip()
        list_in_root_menu = bool(row.get("list_in_root_menu", True))
        visibility_rule = str(row.get("visibility_rule", "Public") or "Public").strip() or "Public"
        open_rule = str(row.get("open_rule", "Public") or "Public").strip() or "Public"
        access_code_hash = str(row.get("access_code_hash", "") or "").strip()
        access_code_salt = str(row.get("access_code_salt", "") or "").strip()
        try:
            access_code_iterations = int(
                row.get("access_code_iterations", DEFAULT_ACCESS_CODE_ITERATIONS) or DEFAULT_ACCESS_CODE_ITERATIONS
            )
        except Exception:
            access_code_iterations = DEFAULT_ACCESS_CODE_ITERATIONS
        access_code_plaintext = str(row.get("access_code_plaintext", "") or "").strip()
        return {
            "id": str(row.get("id", "") or "").strip(),
            "name": name,
            "source_dir": source_dir,
            "enabled": enabled,
            "inherit_global_allowed_callsigns": inherit_allowed,
            "allowed_callsigns": allowed_callsigns,
            "alias": alias,
            "description": description,
            "list_in_root_menu": list_in_root_menu,
            "visibility_rule": visibility_rule,
            "open_rule": open_rule,
            "access_code_hash": access_code_hash,
            "access_code_salt": access_code_salt,
            "access_code_iterations": access_code_iterations,
            "access_code_plaintext": access_code_plaintext,
        }

    def _selected_varac_bbs_vault_location(self) -> Optional[Dict[str, object]]:
        target_id = str(self._varac_bbs_vault_selected_location_id or "").strip()
        if not target_id:
            return None
        for row in self._varac_bbs_vault_locations_cache:
            normalized = self._normalize_varac_bbs_vault_location(row)
            if str(normalized.get("id", "") or "").strip() == target_id:
                return normalized
        return None

    def _set_varac_bbs_vault_locations(self, value: object) -> None:
        normalized = [
            self._normalize_varac_bbs_vault_location(row)
            for row in vault_locations_to_data(load_vault_locations(value))
        ]
        self._varac_bbs_vault_locations_cache = normalized
        existing_ids = {str(row.get("id", "") or "").strip() for row in normalized}
        if self._varac_bbs_vault_selected_location_id not in existing_ids:
            self._varac_bbs_vault_selected_location_id = (
                str(normalized[0].get("id", "") or "").strip() if normalized else ""
            )
        self._refresh_varac_bbs_vault_location_list()
        self._refresh_varac_bbs_vault_status_label()

    def _refresh_varac_bbs_vault_location_list(self) -> None:
        if not hasattr(self, "varac_bbs_vault_locations_list"):
            return
        self.varac_bbs_vault_locations_list.blockSignals(True)
        self.varac_bbs_vault_locations_list.clear()
        default_location_id = str(
            self.varac_bbs_vault_default_location_combo.currentData()
            if hasattr(self, "varac_bbs_vault_default_location_combo")
            else DEFAULT_LOCATION_ID
        ).strip() or DEFAULT_LOCATION_ID
        global_code_policy = (
            self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
            if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
            else DEFAULT_GLOBAL_CODE_POLICY
        )
        for row in self._varac_bbs_vault_locations_cache:
            normalized = self._normalize_varac_bbs_vault_location(row)
            name = str(normalized.get("name", "") or "").strip()
            location_id = str(normalized.get("id", "") or "").strip()
            alias = str(normalized.get("alias", "") or "").strip()
            default_marker = location_id == default_location_id
            enabled = bool(normalized.get("enabled", True))
            requires_code = _vault_location_requires_code_badge(
                normalized,
                default_location_id=default_location_id,
                global_code_policy=global_code_policy,
            )
            label = f"{name or location_id or 'Location'} [{alias}]" if alias else (name or location_id or "Location")
            badges = []
            if default_marker:
                badges.append("Default")
            if not enabled:
                badges.append("Disabled")
            if requires_code:
                badges.append("Code")
            if badges:
                label = f"{label} ({', '.join(badges)})"
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, location_id)
            item.setToolTip(str(normalized.get("source_dir", "") or "").strip())
            self.varac_bbs_vault_locations_list.addItem(item)
            if location_id == self._varac_bbs_vault_selected_location_id:
                item.setSelected(True)
        self.varac_bbs_vault_locations_list.blockSignals(False)
        self._refresh_varac_bbs_vault_default_location_combo()
        self._load_varac_bbs_vault_editor_from_selection()
        self._refresh_varac_bbs_vault_actions()

    def _refresh_varac_bbs_vault_default_location_combo(self) -> None:
        if not hasattr(self, "varac_bbs_vault_default_location_combo"):
            return
        current = str(self.varac_bbs_vault_default_location_combo.currentData() or "").strip()
        if not current:
            current = DEFAULT_LOCATION_ID
        self.varac_bbs_vault_default_location_combo.blockSignals(True)
        self.varac_bbs_vault_default_location_combo.clear()
        for row in self._varac_bbs_vault_locations_cache:
            normalized = self._normalize_varac_bbs_vault_location(row)
            location_id = str(normalized.get("id", "") or "").strip()
            if not location_id:
                continue
            self.varac_bbs_vault_default_location_combo.addItem(
                str(normalized.get("name", location_id) or location_id),
                location_id,
            )
        idx = self.varac_bbs_vault_default_location_combo.findData(current)
        if idx < 0 and self.varac_bbs_vault_default_location_combo.count() > 0:
            idx = 0
        if idx >= 0:
            self.varac_bbs_vault_default_location_combo.setCurrentIndex(idx)
        self.varac_bbs_vault_default_location_combo.blockSignals(False)

    def _load_varac_bbs_vault_editor_from_selection(self) -> None:
        selected = self._selected_varac_bbs_vault_location()
        if not selected:
            self._clear_varac_bbs_vault_editor()
            return
        self._varac_bbs_vault_editor_loading = True
        try:
            if hasattr(self, "varac_bbs_vault_location_name_edit"):
                self.varac_bbs_vault_location_name_edit.setText(str(selected.get("name", "") or "").strip())
            if hasattr(self, "varac_bbs_vault_alias_edit"):
                self.varac_bbs_vault_alias_edit.setText(str(selected.get("alias", "") or "").strip())
            if hasattr(self, "varac_bbs_vault_description_edit"):
                selected_name = str(selected.get("name", "") or "").strip()
                selected_description = str(selected.get("description", "") or "").strip()
                if selected_description.lower() in {
                    f"open {selected_name}".lower(),
                    f"to open {selected_name}".lower(),
                }:
                    selected_description = ""
                self.varac_bbs_vault_description_edit.setText(selected_description)
            if hasattr(self, "varac_bbs_vault_source_dir_edit"):
                self.varac_bbs_vault_source_dir_edit.setText(str(selected.get("source_dir", "") or "").strip())
            if hasattr(self, "varac_bbs_vault_enabled_chk"):
                self.varac_bbs_vault_enabled_chk.setChecked(bool(selected.get("enabled", True)))
            if hasattr(self, "varac_bbs_vault_list_in_root_chk"):
                self.varac_bbs_vault_list_in_root_chk.setChecked(bool(selected.get("list_in_root_menu", True)))
            if hasattr(self, "varac_bbs_vault_visibility_combo"):
                idx = self.varac_bbs_vault_visibility_combo.findText(str(selected.get("visibility_rule", "Public") or "Public").strip())
                self.varac_bbs_vault_visibility_combo.setCurrentIndex(idx if idx >= 0 else 0)
            if hasattr(self, "varac_bbs_vault_open_rule_combo"):
                idx = self.varac_bbs_vault_open_rule_combo.findText(str(selected.get("open_rule", "Public") or "Public").strip())
                self.varac_bbs_vault_open_rule_combo.setCurrentIndex(idx if idx >= 0 else 0)
            if hasattr(self, "varac_bbs_vault_inherit_callsigns_chk"):
                self.varac_bbs_vault_inherit_callsigns_chk.setChecked(
                    bool(selected.get("inherit_global_allowed_callsigns", True))
                )
            if hasattr(self, "varac_bbs_vault_allowed_callsigns_edit"):
                self.varac_bbs_vault_allowed_callsigns_edit.setText(
                    str(selected.get("allowed_callsigns", "") or "").strip()
                )
            if hasattr(self, "varac_bbs_vault_access_code_edit"):
                self.varac_bbs_vault_access_code_edit.setText(str(selected.get("access_code_plaintext", "") or "").strip())
            if hasattr(self, "varac_bbs_vault_access_code_confirm_edit"):
                self.varac_bbs_vault_access_code_confirm_edit.clear()
            self._reset_varac_bbs_vault_code_visibility()
        finally:
            self._varac_bbs_vault_editor_loading = False
        self._varac_bbs_vault_auto_description = str(selected.get("description", "") or "").strip()
        self._varac_bbs_vault_auto_source_dir = str(selected.get("source_dir", "") or "").strip()
        self._refresh_varac_bbs_vault_code_ui(selected)
        self._refresh_varac_bbs_vault_helper_preview()
        self._refresh_varac_bbs_vault_source_hint()
        self._refresh_varac_bbs_vault_actions()

    def _clear_varac_bbs_vault_editor(self) -> None:
        self._varac_bbs_vault_editor_loading = True
        try:
            if hasattr(self, "varac_bbs_vault_location_name_edit"):
                self.varac_bbs_vault_location_name_edit.clear()
            if hasattr(self, "varac_bbs_vault_alias_edit"):
                self.varac_bbs_vault_alias_edit.clear()
            if hasattr(self, "varac_bbs_vault_description_edit"):
                self.varac_bbs_vault_description_edit.clear()
            if hasattr(self, "varac_bbs_vault_source_dir_edit"):
                self.varac_bbs_vault_source_dir_edit.clear()
            if hasattr(self, "varac_bbs_vault_enabled_chk"):
                self.varac_bbs_vault_enabled_chk.setChecked(True)
            if hasattr(self, "varac_bbs_vault_list_in_root_chk"):
                self.varac_bbs_vault_list_in_root_chk.setChecked(True)
            if hasattr(self, "varac_bbs_vault_visibility_combo"):
                self.varac_bbs_vault_visibility_combo.setCurrentIndex(0)
            if hasattr(self, "varac_bbs_vault_open_rule_combo"):
                self.varac_bbs_vault_open_rule_combo.setCurrentIndex(0)
            if hasattr(self, "varac_bbs_vault_inherit_callsigns_chk"):
                self.varac_bbs_vault_inherit_callsigns_chk.setChecked(True)
            if hasattr(self, "varac_bbs_vault_allowed_callsigns_edit"):
                self.varac_bbs_vault_allowed_callsigns_edit.clear()
            if hasattr(self, "varac_bbs_vault_access_code_edit"):
                self.varac_bbs_vault_access_code_edit.clear()
            if hasattr(self, "varac_bbs_vault_access_code_confirm_edit"):
                self.varac_bbs_vault_access_code_confirm_edit.clear()
            self._reset_varac_bbs_vault_code_visibility()
        finally:
            self._varac_bbs_vault_editor_loading = False
        self._varac_bbs_vault_auto_description = ""
        self._varac_bbs_vault_auto_source_dir = ""
        self._refresh_varac_bbs_vault_code_ui(None)
        self._refresh_varac_bbs_vault_helper_preview()
        self._refresh_varac_bbs_vault_source_hint()

    def _refresh_varac_bbs_vault_code_ui(self, selected: Optional[Dict[str, object]]) -> None:
        has_code = bool(str((selected or {}).get("access_code_hash", "") or "").strip())
        has_plaintext = bool(str((selected or {}).get("access_code_plaintext", "") or "").strip())
        if hasattr(self, "varac_bbs_vault_code_status_label"):
            if has_plaintext:
                text = "Code configured. The local operator can view it here and replace it if needed."
            elif has_code:
                text = "Code configured from an older save. It cannot be shown until it is replaced."
            else:
                text = "No code configured. Enter a code twice to set one for this location."
            self.varac_bbs_vault_code_status_label.setText(text)
        if hasattr(self, "varac_bbs_vault_access_code_edit"):
            self.varac_bbs_vault_access_code_edit.setPlaceholderText(
                "Saved code is shown here when available" if has_plaintext else (
                    "Enter a new access code to replace the saved code" if has_code else "Enter a new access code"
                )
            )
            self.varac_bbs_vault_access_code_edit.setToolTip(
                "Stored locally for the operator and hashed for caller verification."
            )
        if hasattr(self, "varac_bbs_vault_access_code_confirm_edit"):
            self.varac_bbs_vault_access_code_confirm_edit.setPlaceholderText("Confirm only when changing the code")
            self.varac_bbs_vault_access_code_confirm_edit.setToolTip(
                "Required when setting or changing the code."
            )

    def _refresh_varac_bbs_vault_helper_preview(self) -> None:
        if not hasattr(self, "varac_bbs_vault_helper_preview_label"):
            return
        selected = self._selected_varac_bbs_vault_location()
        if not selected:
            text = "Select a location to preview the generated helper."
            self.varac_bbs_vault_helper_preview_label.setText(text)
            self.varac_bbs_vault_helper_preview_label.setToolTip(text)
            return
        location_id = str(selected.get("id", "") or "").strip()
        name = (
            self.varac_bbs_vault_location_name_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_location_name_edit")
            else str(selected.get("name", "") or "").strip()
        )
        alias = normalize_location_alias(
            self.varac_bbs_vault_alias_edit.text().strip() if hasattr(self, "varac_bbs_vault_alias_edit") else selected.get("alias", ""),
            name,
        )
        description = (
            self.varac_bbs_vault_description_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_description_edit")
            else str(selected.get("description", "") or "").strip()
        )
        if description.lower() in {f"open {name}".lower(), f"to open {name}".lower()}:
            description = ""
        open_rule = (
            self.varac_bbs_vault_open_rule_combo.currentText().strip()
            if hasattr(self, "varac_bbs_vault_open_rule_combo")
            else str(selected.get("open_rule", "Public") or "Public").strip()
        )
        if location_id == DEFAULT_LOCATION_ID:
            text = (
                f"All BBS views include: 00 READ FIRST - type command, wait {DEFAULT_BBS_REFRESH_PAUSE_SECONDS} sec, refresh BBS.txt\n"
                "Default is the FIO Managed Root BBS menu. It publishes helper files for visible locations "
                "and any files placed in the Default folder."
            )
        else:
            global_code_policy = (
                self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
                else DEFAULT_GLOBAL_CODE_POLICY
            )
            preview_location = VaultLocation(
                id=location_id,
                name=name or alias or "location",
                source_dir=str(selected.get("source_dir", "") or ""),
                alias=alias,
                description=description,
                open_rule=open_rule,
            )
            helper_name = root_location_helper_filename_preview(
                preview_location,
                default_location_id=DEFAULT_LOCATION_ID,
                global_code_policy=global_code_policy,
                order=20,
            )
            text = (
                f"All BBS views include: 00 READ FIRST - type command, wait {DEFAULT_BBS_REFRESH_PAUSE_SECONDS} sec, refresh BBS.txt\n"
                f"{helper_name}"
            )
        self.varac_bbs_vault_helper_preview_label.setText(text)
        self.varac_bbs_vault_helper_preview_label.setToolTip(text)

    def _default_varac_bbs_vault_location_description(self, name: object) -> str:
        return ""

    def _suggest_varac_bbs_vault_location_source_dir(self, name: object = "") -> str:
        clean_name = " ".join(str(name or "").strip().split())
        if not clean_name:
            return ""
        root_txt = (
            self.varac_bbs_vault_root_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_root_edit")
            else ""
        )
        if not root_txt and hasattr(self, "varac_bbs_dir_edit"):
            root_txt = compute_default_managed_root(self.varac_bbs_dir_edit.text().strip())
        if not root_txt:
            return ""
        return str(Path(root_txt).expanduser() / "locations" / clean_name)

    def _find_varac_bbs_live_match(self, name: object = "") -> str:
        clean_name = " ".join(str(name or "").strip().split())
        if not clean_name or not hasattr(self, "varac_bbs_dir_edit"):
            return ""
        live_dir = Path(self.varac_bbs_dir_edit.text().strip()).expanduser()
        if not live_dir.exists() or not live_dir.is_dir():
            return ""
        token = re.sub(r"[^a-z0-9]+", "", clean_name.lower())
        if not token:
            return ""
        matches: List[str] = []
        for child in sorted(live_dir.iterdir(), key=lambda item: item.name.lower()):
            name_token = re.sub(r"[^a-z0-9]+", "", child.name.lower())
            stem_token = re.sub(r"[^a-z0-9]+", "", child.stem.lower())
            if token in {name_token, stem_token}:
                matches.append(child.name)
        return ", ".join(matches[:3])

    def _refresh_varac_bbs_vault_source_hint(self) -> None:
        if not hasattr(self, "varac_bbs_vault_source_hint_label"):
            return
        current_name = (
            self.varac_bbs_vault_location_name_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_location_name_edit")
            else ""
        )
        suggested = self._suggest_varac_bbs_vault_location_source_dir(current_name)
        live_match = self._find_varac_bbs_live_match(current_name)
        current_source = (
            self.varac_bbs_vault_source_dir_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_source_dir_edit")
            else ""
        )
        parts: List[str] = []
        if suggested:
            parts.append(
                f"Suggested BBS-area source folder: {suggested}"
                if current_source
                else f"Suggested BBS-area source folder: {suggested} (will be created on save if needed)."
            )
        else:
            parts.append(
                "Typical pattern in the VarAC BBS area: Managed Root / locations / <Location Name>. Save Location can create that folder."
            )
        if live_match:
            parts.append(f"Live BBS likely match: {live_match}")
        hint = " ".join(parts)
        self.varac_bbs_vault_source_hint_label.setText(hint)
        self.varac_bbs_vault_source_hint_label.setToolTip(hint)

    def _autofill_varac_bbs_vault_location_defaults(self, *, force: bool = False) -> None:
        if self._varac_bbs_vault_editor_loading:
            return
        current_name = (
            self.varac_bbs_vault_location_name_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_location_name_edit")
            else ""
        )
        if not current_name:
            self._refresh_varac_bbs_vault_helper_preview()
            self._refresh_varac_bbs_vault_source_hint()
            return
        default_description = self._default_varac_bbs_vault_location_description(current_name)
        if hasattr(self, "varac_bbs_vault_description_edit"):
            current_description = self.varac_bbs_vault_description_edit.text().strip()
            if force or not current_description or current_description == self._varac_bbs_vault_auto_description:
                if current_description != default_description:
                    self.varac_bbs_vault_description_edit.setText(default_description)
                self._varac_bbs_vault_auto_description = default_description
        suggested_source = self._suggest_varac_bbs_vault_location_source_dir(current_name)
        if suggested_source and hasattr(self, "varac_bbs_vault_source_dir_edit"):
            current_source = self.varac_bbs_vault_source_dir_edit.text().strip()
            if force or not current_source or current_source == self._varac_bbs_vault_auto_source_dir:
                if current_source != suggested_source:
                    self.varac_bbs_vault_source_dir_edit.setText(suggested_source)
                self._varac_bbs_vault_auto_source_dir = suggested_source
        self._refresh_varac_bbs_vault_helper_preview()
        self._refresh_varac_bbs_vault_source_hint()

    def _suggest_varac_bbs_vault_flamp_relay_dir(self) -> str:
        if not hasattr(self, "msg_paths_edits"):
            return ""
        flamp_edit = self.msg_paths_edits.get("flamp")
        rx_txt = flamp_edit.text().strip() if isinstance(flamp_edit, QLineEdit) else ""
        if not rx_txt:
            return ""
        rx_path = Path(rx_txt).expanduser()
        if rx_path.name.lower() == "relay":
            return str(rx_path)
        if rx_path.name.lower() == "rx":
            return str(rx_path.with_name("relay"))
        if rx_path.name.lower() == "flamp":
            return str(rx_path / "relay")
        if rx_path.is_file() and rx_path.parent.name.lower() == "rx":
            return str(rx_path.parent.with_name("relay"))
        if rx_path.parent.name.lower() == "rx":
            return str(rx_path.parent.with_name("relay"))
        return ""

    def _maybe_autofill_varac_bbs_vault_flamp_relay_dir(self, *, force: bool = False) -> None:
        if not hasattr(self, "varac_bbs_vault_flamp_relay_dir_edit"):
            return
        suggestion = self._suggest_varac_bbs_vault_flamp_relay_dir()
        current = self.varac_bbs_vault_flamp_relay_dir_edit.text().strip()
        if suggestion and (force or not current or current == self._varac_bbs_vault_auto_flamp_relay_dir):
            if current != suggestion:
                self.varac_bbs_vault_flamp_relay_dir_edit.setText(suggestion)
            self._varac_bbs_vault_auto_flamp_relay_dir = suggestion
        self._refresh_varac_bbs_vault_flamp_hint()

    def _refresh_varac_bbs_vault_flamp_hint(self) -> None:
        if not hasattr(self, "varac_bbs_vault_flamp_relay_hint_label"):
            return
        suggestion = self._suggest_varac_bbs_vault_flamp_relay_dir()
        hint = (
            f"Suggested from FLAMP/rx: {suggestion}. Override this any time if your relay queue lives elsewhere."
            if suggestion
            else "Set FLAMP/rx first and FreqInOut will suggest a sibling relay folder automatically."
        )
        self.varac_bbs_vault_flamp_relay_hint_label.setText(hint)
        self.varac_bbs_vault_flamp_relay_hint_label.setToolTip(hint)

    def _computed_varac_bbs_vault_default_root(self, bbs_dir: object = None) -> str:
        if bbs_dir is None:
            bbs_dir = (
                self.varac_bbs_dir_edit.text().strip()
                if hasattr(self, "varac_bbs_dir_edit")
                else ""
            )
        return str(compute_default_managed_root(bbs_dir) or "").strip()

    def _set_varac_bbs_vault_root_text(self, value: str) -> None:
        if not hasattr(self, "varac_bbs_vault_root_edit"):
            return
        self._varac_bbs_vault_root_loading = True
        try:
            self.varac_bbs_vault_root_edit.setText(value)
        finally:
            self._varac_bbs_vault_root_loading = False

    def _refresh_varac_bbs_vault_root_hint(self) -> None:
        if not hasattr(self, "varac_bbs_vault_root_hint_label"):
            return
        default_root = self._computed_varac_bbs_vault_default_root()
        if default_root:
            hint = (
                f"Automatic vault location in the VarAC BBS area: {default_root}. New vault locations and files live here, "
                "alongside the live BBS folder, not inside the live published file list and not under the FreqInOut app folder."
            )
        else:
            hint = "Set the VarAC BBS directory first to see the default vault location."
        self.varac_bbs_vault_root_hint_label.setText(hint)
        self.varac_bbs_vault_root_hint_label.setToolTip(hint)

    def _sync_varac_bbs_vault_root_from_bbs_dir(self, _text: object = None, *, force: bool = False) -> None:
        if not hasattr(self, "varac_bbs_vault_root_edit"):
            return
        current_bbs_dir = (
            self.varac_bbs_dir_edit.text().strip()
            if hasattr(self, "varac_bbs_dir_edit")
            else ""
        )
        new_default = self._computed_varac_bbs_vault_default_root(current_bbs_dir)
        current_root = self.varac_bbs_vault_root_edit.text().strip()
        if new_default:
            if force or current_root != new_default:
                self._set_varac_bbs_vault_root_text(new_default)
        elif current_root:
            self._set_varac_bbs_vault_root_text("")
        self._last_varac_bbs_dir_for_root_sync = current_bbs_dir
        self._refresh_varac_bbs_vault_root_hint()

    def _on_varac_bbs_vault_root_changed(self, _text: object = None) -> None:
        if self._varac_bbs_vault_root_loading:
            return
        self._refresh_varac_bbs_vault_root_hint()

    def _set_password_line_edit_visible(self, line_edit: QLineEdit, visible: bool) -> None:
        line_edit.setEchoMode(QLineEdit.Normal if visible else QLineEdit.Password)
        action = getattr(line_edit, "_password_toggle_action", None)
        if not isinstance(action, QAction):
            return
        action.setIcon(self._password_toggle_icon(visible, line_edit))
        action.setToolTip("Hide access code" if visible else "Show access code")
        action.setText("Hide access code" if visible else "Show access code")

    def _password_toggle_icon(self, visible: bool, line_edit: Optional[QLineEdit] = None) -> QIcon:
        color = (line_edit.palette().text().color() if isinstance(line_edit, QLineEdit) else self.palette().text().color())
        size = 16
        pixmap = QPixmap(size, size)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing, True)
        pen = QPen(color)
        pen.setWidthF(1.5)
        painter.setPen(pen)
        painter.setBrush(Qt.NoBrush)
        path = QPainterPath()
        path.moveTo(2.0, size / 2.0)
        path.cubicTo(size * 0.28, 2.0, size * 0.72, 2.0, size - 2.0, size / 2.0)
        path.cubicTo(size * 0.72, size - 2.0, size * 0.28, size - 2.0, 2.0, size / 2.0)
        painter.drawPath(path)
        painter.setBrush(color)
        painter.drawEllipse(size / 2.0 - 1.6, size / 2.0 - 1.6, 3.2, 3.2)
        if visible:
            slash_pen = QPen(color)
            slash_pen.setWidthF(1.9)
            painter.setPen(slash_pen)
            painter.drawLine(3.0, size - 3.0, size - 3.0, 3.0)
        painter.end()
        return QIcon(pixmap)

    def _toggle_password_line_edit_visibility(self, line_edit: QLineEdit) -> None:
        self._set_password_line_edit_visible(line_edit, line_edit.echoMode() == QLineEdit.Password)

    def _attach_password_toggle_action(self, line_edit: QLineEdit) -> None:
        action = line_edit.addAction(
            self._password_toggle_icon(False, line_edit),
            QLineEdit.TrailingPosition,
        )
        action.triggered.connect(
            lambda _checked=False, edit=line_edit: self._toggle_password_line_edit_visibility(edit)
        )
        setattr(line_edit, "_password_toggle_action", action)
        self._set_password_line_edit_visible(line_edit, False)

    def _reset_varac_bbs_vault_code_visibility(self) -> None:
        for name in ("varac_bbs_vault_access_code_edit", "varac_bbs_vault_access_code_confirm_edit"):
            line_edit = getattr(self, name, None)
            if isinstance(line_edit, QLineEdit):
                self._set_password_line_edit_visible(line_edit, False)

    def _refresh_varac_bbs_vault_actions(self) -> None:
        has_selection = self._selected_varac_bbs_vault_location() is not None
        if hasattr(self, "varac_bbs_vault_remove_btn"):
            selected = self._selected_varac_bbs_vault_location()
            self.varac_bbs_vault_remove_btn.setEnabled(
                bool(has_selection and str((selected or {}).get("id", "") or "").strip() != DEFAULT_LOCATION_ID)
            )
        if hasattr(self, "varac_bbs_vault_save_location_btn"):
            self.varac_bbs_vault_save_location_btn.setEnabled(True)
        if hasattr(self, "varac_bbs_vault_reset_btn"):
            enabled = bool(
                hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked()
            )
            self.varac_bbs_vault_reset_btn.setEnabled(enabled and bool(self._varac_bbs_vault_locations_cache))

    def _new_varac_bbs_vault_location(self) -> None:
        self._varac_bbs_vault_selected_location_id = ""
        if hasattr(self, "varac_bbs_vault_locations_list"):
            self.varac_bbs_vault_locations_list.clearSelection()
        self._clear_varac_bbs_vault_editor()
        self._autofill_varac_bbs_vault_location_defaults(force=True)
        self._refresh_varac_bbs_vault_actions()

    def _on_varac_bbs_vault_location_selected(self) -> None:
        if not hasattr(self, "varac_bbs_vault_locations_list"):
            return
        selected = self.varac_bbs_vault_locations_list.selectedItems()
        self._varac_bbs_vault_selected_location_id = (
            str(selected[0].data(Qt.UserRole) or "").strip() if selected else ""
        )
        self._load_varac_bbs_vault_editor_from_selection()

    def _choose_varac_bbs_vault_root(self) -> None:
        root_txt = self._computed_varac_bbs_vault_default_root()
        if root_txt and hasattr(self, "varac_bbs_vault_root_edit"):
            self._set_varac_bbs_vault_root_text(root_txt)
        self._refresh_varac_bbs_vault_root_hint()
        QMessageBox.information(
            self,
            "Managed BBS Vault",
            "Managed Root is automatic in this release.\n\n"
            "FreqInOut always creates the vault in the VarAC BBS area, next to the live BBS directory.",
        )

    def _choose_varac_bbs_vault_location_source(self) -> None:
        start = (
            self.varac_bbs_vault_source_dir_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_source_dir_edit")
            else ""
        )
        fn = QFileDialog.getExistingDirectory(self, "Select location source folder", start)
        if not fn:
            return
        self.varac_bbs_vault_source_dir_edit.setText(fn)
        self._varac_bbs_vault_auto_source_dir = fn
        self._mark_settings_dirty()
        self._refresh_varac_bbs_vault_source_hint()

    def _choose_varac_bbs_vault_flamp_relay_dir(self) -> None:
        start = (
            self.varac_bbs_vault_flamp_relay_dir_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_flamp_relay_dir_edit")
            else ""
        )
        fn = QFileDialog.getExistingDirectory(self, "Select FLAMP relay folder", start)
        if not fn:
            return
        self.varac_bbs_vault_flamp_relay_dir_edit.setText(fn)
        self._varac_bbs_vault_auto_flamp_relay_dir = fn
        self._mark_settings_dirty()
        self._refresh_varac_bbs_vault_flamp_hint()

    def _varac_bbs_vault_editor_has_pending_changes(self) -> bool:
        selected = self._selected_varac_bbs_vault_location() or {}
        current_name = (
            self.varac_bbs_vault_location_name_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_location_name_edit")
            else ""
        )
        current = {
            "name": current_name,
            "alias": normalize_location_alias(
                self.varac_bbs_vault_alias_edit.text().strip() if hasattr(self, "varac_bbs_vault_alias_edit") else "",
                current_name,
            ),
            "description": (
                self.varac_bbs_vault_description_edit.text().strip()
                if hasattr(self, "varac_bbs_vault_description_edit")
                else ""
            ),
            "source_dir": (
                self.varac_bbs_vault_source_dir_edit.text().strip()
                if hasattr(self, "varac_bbs_vault_source_dir_edit")
                else ""
            ),
            "enabled": bool(
                self.varac_bbs_vault_enabled_chk.isChecked() if hasattr(self, "varac_bbs_vault_enabled_chk") else True
            ),
            "list_in_root_menu": bool(
                self.varac_bbs_vault_list_in_root_chk.isChecked()
                if hasattr(self, "varac_bbs_vault_list_in_root_chk")
                else True
            ),
            "visibility_rule": (
                self.varac_bbs_vault_visibility_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_visibility_combo")
                else "Public"
            ),
            "open_rule": (
                self.varac_bbs_vault_open_rule_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_open_rule_combo")
                else "Public"
            ),
            "inherit_global_allowed_callsigns": bool(
                self.varac_bbs_vault_inherit_callsigns_chk.isChecked()
                if hasattr(self, "varac_bbs_vault_inherit_callsigns_chk")
                else True
            ),
            "allowed_callsigns": format_callsign_list(
                self.varac_bbs_vault_allowed_callsigns_edit.text().strip()
                if hasattr(self, "varac_bbs_vault_allowed_callsigns_edit")
                else ""
            ),
        }
        selected_view = {
            "name": str(selected.get("name", "") or "").strip(),
            "alias": normalize_location_alias(selected.get("alias", ""), selected.get("name", "")),
            "description": str(selected.get("description", "") or "").strip(),
            "source_dir": str(selected.get("source_dir", "") or "").strip(),
            "enabled": bool(selected.get("enabled", True)),
            "list_in_root_menu": bool(selected.get("list_in_root_menu", True)),
            "visibility_rule": str(selected.get("visibility_rule", "Public") or "Public").strip(),
            "open_rule": str(selected.get("open_rule", "Public") or "Public").strip(),
            "inherit_global_allowed_callsigns": bool(selected.get("inherit_global_allowed_callsigns", True)),
            "allowed_callsigns": format_callsign_list(selected.get("allowed_callsigns", "")),
        }
        if current != selected_view:
            return True
        access_code = (
            self.varac_bbs_vault_access_code_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_access_code_edit")
            else ""
        )
        confirm_code = (
            self.varac_bbs_vault_access_code_confirm_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_access_code_confirm_edit")
            else ""
        )
        saved_plaintext = str(selected.get("access_code_plaintext", "") or "").strip()
        if access_code == saved_plaintext and not confirm_code:
            return False
        return bool(access_code or confirm_code)

    def _sanitize_varac_bbs_vault_alias_text(self, text: str) -> None:
        if not hasattr(self, "varac_bbs_vault_alias_edit"):
            return
        cleaned = normalize_location_alias(text, "")
        if cleaned == text:
            return
        cursor = min(self.varac_bbs_vault_alias_edit.cursorPosition(), len(cleaned))
        self.varac_bbs_vault_alias_edit.blockSignals(True)
        try:
            self.varac_bbs_vault_alias_edit.setText(cleaned)
            self.varac_bbs_vault_alias_edit.setCursorPosition(cursor)
        finally:
            self.varac_bbs_vault_alias_edit.blockSignals(False)
        self._mark_settings_dirty()
        self._refresh_varac_bbs_vault_helper_preview()

    def _confirm_readd_varac_bbs_vault_location_folder(self, source_path: Path) -> bool:
        response = QMessageBox.question(
            self,
            "Re-add Existing Location Folder",
            "A managed BBS folder already exists for this location.\n\n"
            "Re-add this folder to FIO Settings without changing files on disk?",
            QMessageBox.Yes | QMessageBox.Cancel,
            QMessageBox.Yes,
        )
        return response == QMessageBox.Yes

    def _should_offer_readd_varac_bbs_vault_location_folder(self, location_id: str, source_path: Path) -> bool:
        return not str(location_id or "").strip() and source_path.exists() and source_path.is_dir()

    def _save_varac_bbs_vault_location(self) -> bool:
        name = (
            self.varac_bbs_vault_location_name_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_location_name_edit")
            else ""
        )
        alias = normalize_location_alias(
            self.varac_bbs_vault_alias_edit.text().strip() if hasattr(self, "varac_bbs_vault_alias_edit") else "",
            name,
        )
        description = (
            self.varac_bbs_vault_description_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_description_edit")
            else ""
        )
        source_dir = (
            self.varac_bbs_vault_source_dir_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_source_dir_edit")
            else ""
        )
        if not name:
            QMessageBox.warning(self, "Managed BBS Vault", "Set a location name before saving.")
            return False
        if not source_dir:
            QMessageBox.warning(self, "Managed BBS Vault", "Set a source folder before saving.")
            return False
        source_path = Path(source_dir).expanduser()
        is_new_location = not str(self._varac_bbs_vault_selected_location_id or "").strip()
        selected = {} if is_new_location else (self._selected_varac_bbs_vault_location() or {})
        location_id = str(selected.get("id", "") or "").strip()
        if not source_path.exists():
            response = QMessageBox.question(
                self,
                "Managed BBS Vault",
                f"Create this source folder?\n\n{source_path}",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes,
            )
            if response != QMessageBox.Yes:
                return False
            source_path.mkdir(parents=True, exist_ok=True)
        elif self._should_offer_readd_varac_bbs_vault_location_folder(location_id, source_path):
            if not self._confirm_readd_varac_bbs_vault_location_folder(source_path):
                return False
        if not source_path.is_dir():
            QMessageBox.warning(self, "Managed BBS Vault", "Location source must be a directory.")
            return False
        access_code = (
            self.varac_bbs_vault_access_code_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_access_code_edit")
            else ""
        )
        confirm_code = (
            self.varac_bbs_vault_access_code_confirm_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_access_code_confirm_edit")
            else ""
        )
        for existing in self._varac_bbs_vault_locations_cache:
            existing_id = str(existing.get("id", "") or "").strip()
            existing_alias = normalize_location_alias(existing.get("alias", ""), existing.get("name", ""))
            if existing_alias and existing_alias == alias and existing_id != location_id:
                QMessageBox.warning(self, "Managed BBS Vault", f"Alias {alias} is already in use.")
                return False
        if not location_id:
            existing_ids = [str(row.get("id", "") or "").strip() for row in self._varac_bbs_vault_locations_cache]
            base = re.sub(r"[^A-Za-z0-9]+", "-", name.lower()).strip("-") or DEFAULT_LOCATION_ID
            location_id = base
            counter = 2
            while location_id in existing_ids:
                location_id = f"{base}-{counter}"
                counter += 1
        global_code_policy = (
            self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
            if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
            else DEFAULT_GLOBAL_CODE_POLICY
        )
        open_rule = (
            self.varac_bbs_vault_open_rule_combo.currentText().strip()
            if hasattr(self, "varac_bbs_vault_open_rule_combo")
            else "Public"
        )
        inherit_callsigns = bool(
            self.varac_bbs_vault_inherit_callsigns_chk.isChecked()
            if hasattr(self, "varac_bbs_vault_inherit_callsigns_chk")
            else True
        )
        location_callsigns = format_callsign_list(
            self.varac_bbs_vault_allowed_callsigns_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_allowed_callsigns_edit")
            else ""
        )
        if open_rule in {"Allowed callsigns only", "Allowed callsigns + access code"} and not inherit_callsigns and not location_callsigns:
            QMessageBox.warning(
                self,
                "Managed BBS Vault",
                "Enter Location Allowed Callsigns or turn on Inherit Global Allowed Callsigns.",
            )
            return False
        requires_code = (
            location_id != DEFAULT_LOCATION_ID
            and (
                open_rule in {"Access code required", "Allowed callsigns + access code"}
                or (global_code_policy == "Require for non-default locations" and open_rule != "Public")
                or (global_code_policy == "Require for all restricted locations" and open_rule != "Public")
            )
        )
        if requires_code and not (selected.get("access_code_hash") or access_code):
            QMessageBox.warning(
                self,
                "Managed BBS Vault",
                "This location needs an access code under the current policy before it can be saved.",
            )
            return False
        saved_plaintext = str(selected.get("access_code_plaintext", "") or "").strip()
        code_changed = bool(access_code or confirm_code) and access_code != saved_plaintext
        if code_changed:
            if access_code != confirm_code:
                QMessageBox.warning(self, "Managed BBS Vault", "Access code confirmation does not match.")
                return False
            code_payload = hash_access_code(access_code)
            access_code_plaintext = access_code
        else:
            code_payload = {
                "access_code_hash": str(selected.get("access_code_hash", "") or "").strip(),
                "access_code_salt": str(selected.get("access_code_salt", "") or "").strip(),
                "access_code_iterations": int(
                    selected.get("access_code_iterations", DEFAULT_ACCESS_CODE_ITERATIONS)
                    or DEFAULT_ACCESS_CODE_ITERATIONS
                ),
            }
            access_code_plaintext = str(selected.get("access_code_plaintext", "") or "").strip()
        row = {
            "id": location_id,
            "name": " ".join(name.split()),
            "alias": alias,
            "description": description,
            "source_dir": str(source_path),
            "enabled": bool(self.varac_bbs_vault_enabled_chk.isChecked() if hasattr(self, "varac_bbs_vault_enabled_chk") else True),
            "list_in_root_menu": bool(
                self.varac_bbs_vault_list_in_root_chk.isChecked()
                if hasattr(self, "varac_bbs_vault_list_in_root_chk")
                else True
            ),
            "visibility_rule": (
                self.varac_bbs_vault_visibility_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_visibility_combo")
                else "Public"
            ),
            "open_rule": (
                self.varac_bbs_vault_open_rule_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_open_rule_combo")
                else "Public"
            ),
            "inherit_global_allowed_callsigns": inherit_callsigns,
            "allowed_callsigns": location_callsigns,
            "access_code_hash": str(code_payload.get("access_code_hash", "") or "").strip(),
            "access_code_salt": str(code_payload.get("access_code_salt", "") or "").strip(),
            "access_code_iterations": int(
                code_payload.get("access_code_iterations", DEFAULT_ACCESS_CODE_ITERATIONS)
                or DEFAULT_ACCESS_CODE_ITERATIONS
            ),
            "access_code_plaintext": access_code_plaintext,
        }
        updated = False
        for idx, existing in enumerate(self._varac_bbs_vault_locations_cache):
            if str(existing.get("id", "") or "").strip() == location_id:
                self._varac_bbs_vault_locations_cache[idx] = self._normalize_varac_bbs_vault_location(row)
                updated = True
                break
        if not updated:
            self._varac_bbs_vault_locations_cache.append(self._normalize_varac_bbs_vault_location(row))
        self._varac_bbs_vault_selected_location_id = location_id
        if (
            hasattr(self, "varac_bbs_vault_default_location_combo")
            and self.varac_bbs_vault_default_location_combo.count() == 0
        ):
            self.varac_bbs_vault_default_location_combo.addItem(row["name"], location_id)
            self.varac_bbs_vault_default_location_combo.setCurrentIndex(0)
        self._refresh_varac_bbs_vault_location_list()
        self._mark_settings_dirty()
        self._refresh_section_titles()
        self._varac_bbs_vault_auto_description = description
        self._varac_bbs_vault_auto_source_dir = str(source_path)
        self._refresh_varac_bbs_vault_source_hint()
        return True

    def _remove_varac_bbs_vault_location(self) -> None:
        selected = self._selected_varac_bbs_vault_location()
        if not selected:
            return
        location_id = str(selected.get("id", "") or "").strip()
        if location_id == DEFAULT_LOCATION_ID:
            QMessageBox.warning(self, "Managed BBS Vault", "The Default location cannot be removed.")
            return
        self._varac_bbs_vault_locations_cache = [
            row for row in self._varac_bbs_vault_locations_cache if str(row.get("id", "") or "").strip() != location_id
        ]
        if hasattr(self, "varac_bbs_vault_default_location_combo") and (
            str(self.varac_bbs_vault_default_location_combo.currentData() or "").strip() == location_id
        ):
            idx = self.varac_bbs_vault_default_location_combo.findData(DEFAULT_LOCATION_ID)
            if idx >= 0:
                self.varac_bbs_vault_default_location_combo.setCurrentIndex(idx)
        self._varac_bbs_vault_selected_location_id = ""
        self._refresh_varac_bbs_vault_location_list()
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _refresh_varac_bbs_vault_status_label(self) -> None:
        if not hasattr(self, "varac_bbs_vault_status_label"):
            return
        if hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked():
            root_txt = self.varac_bbs_vault_root_edit.text().strip() if hasattr(self, "varac_bbs_vault_root_edit") else ""
            state = load_vault_runtime_state(self._varac_bbs_vault_runtime_state_cache)
            cached_summary = str(self._varac_bbs_vault_last_summary_cache or "").strip()
            current_name = ""
            default_id = (
                str(self.varac_bbs_vault_default_location_combo.currentData() or "").strip()
                if hasattr(self, "varac_bbs_vault_default_location_combo")
                else DEFAULT_LOCATION_ID
            )
            for row in self._varac_bbs_vault_locations_cache:
                if str(row.get("id", "") or "").strip() == state.current_location_id:
                    current_name = str(row.get("name", "") or "").strip()
                    break
            if not current_name:
                for row in self._varac_bbs_vault_locations_cache:
                    if str(row.get("id", "") or "").strip() == default_id:
                        current_name = str(row.get("name", "") or "").strip()
                        break
            summary = cached_summary or f"Managed Vault ready for {current_name or DEFAULT_LOCATION_NAME}."
            if root_txt:
                summary += f" Root: {root_txt}"
        else:
            summary = "Managed Vault is not enabled for this radio profile."
        self.varac_bbs_vault_status_label.setText(summary)
        self.varac_bbs_vault_status_label.setToolTip(summary)

    def _initialize_varac_bbs_vault(self) -> None:
        live_bbs_dir = self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        if not live_bbs_dir:
            QMessageBox.warning(self, "Managed BBS Vault", "Set the VarAC BBS directory before initializing the vault.")
            return
        live_dir = Path(live_bbs_dir).expanduser()
        if not live_dir.exists() or not live_dir.is_dir():
            QMessageBox.warning(self, "Managed BBS Vault", "The VarAC BBS directory must exist before initialization.")
            return
        default_root_txt = compute_default_managed_root(live_dir)
        root_txt = default_root_txt
        if hasattr(self, "varac_bbs_vault_root_edit"):
            self._set_varac_bbs_vault_root_text(root_txt)
        create_note = "Existing vault root will be reused." if Path(root_txt).expanduser().exists() else "This vault root will be created."
        response = QMessageBox.question(
            self,
            "Managed BBS Vault",
            "Initialize Managed Vault at this location?\n\n"
            f"Live BBS Directory:\n{live_dir}\n\n"
            f"Managed Root:\n{root_txt}\n\n"
            f"{create_note}",
            QMessageBox.Yes | QMessageBox.Cancel,
            QMessageBox.Yes,
        )
        if response != QMessageBox.Yes:
            return
        created = initialize_managed_root(root_txt)
        default_dir = created["default"]
        live_files = [child for child in live_dir.iterdir() if child.is_file()]
        imported = 0
        if live_files:
            response = QMessageBox.question(
                self,
                "Managed BBS Vault",
                "Import current live BBS files into the Default location?\n\n"
                "Yes: import and preserve the current published set.\n"
                "No: start the Default location empty and leave current live files unmanaged until the first publish.\n"
                "Cancel: stop initialization.",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
                QMessageBox.Yes,
            )
            if response == QMessageBox.Cancel:
                return
            if response == QMessageBox.Yes:
                imported = import_live_bbs_to_default_location(live_dir, default_dir)
        locations = list(self._varac_bbs_vault_locations_cache)
        default_row = None
        for row in locations:
            if str(row.get("id", "") or "").strip() == DEFAULT_LOCATION_ID:
                default_row = dict(row)
                break
        if default_row is None:
            default_row = {
                "id": DEFAULT_LOCATION_ID,
                "name": DEFAULT_LOCATION_NAME,
                "alias": "ROOT",
                "description": "Main menu",
                "source_dir": default_dir,
                "enabled": True,
                "list_in_root_menu": False,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": "",
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": DEFAULT_ACCESS_CODE_ITERATIONS,
            }
            locations.insert(0, default_row)
        else:
            default_row["source_dir"] = default_dir
            default_row["name"] = DEFAULT_LOCATION_NAME
            default_row["alias"] = "ROOT"
            default_row["description"] = "Main menu"
            default_row["list_in_root_menu"] = False
            default_row["visibility_rule"] = "Public"
            default_row["open_rule"] = "Public"
        self._varac_bbs_vault_locations_cache = [self._normalize_varac_bbs_vault_location(row) for row in locations]
        self._varac_bbs_vault_selected_location_id = DEFAULT_LOCATION_ID
        if hasattr(self, "varac_bbs_vault_default_location_combo"):
            self.varac_bbs_vault_default_location_combo.blockSignals(True)
            self.varac_bbs_vault_default_location_combo.clear()
            self.varac_bbs_vault_default_location_combo.addItem(DEFAULT_LOCATION_NAME, DEFAULT_LOCATION_ID)
            self.varac_bbs_vault_default_location_combo.blockSignals(False)
        publish_result = publish_root_view(
            sender="",
            locations=load_vault_locations(self._varac_bbs_vault_locations_cache),
            default_location_id=DEFAULT_LOCATION_ID,
            global_allowed_callsigns=parse_callsign_list(self._varac_bbs_selected_callsigns_text()),
            limit_access_enabled=bool(self.varac_bbs_limit_access_chk.isChecked() if hasattr(self, "varac_bbs_limit_access_chk") else False),
            global_code_policy=(
                self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
                else DEFAULT_GLOBAL_CODE_POLICY
            ),
            live_bbs_dir=live_dir,
            managed_root=root_txt,
            flamp_enabled=bool(self.varac_bbs_vault_flamp_enabled_chk.isChecked() if hasattr(self, "varac_bbs_vault_flamp_enabled_chk") else False),
        )
        runtime_state = VaultRuntimeState(
            current_location_id=DEFAULT_LOCATION_ID,
            current_session_callsign="",
            current_session_qso_guid="",
            current_view_mode="root",
            current_view_label=DEFAULT_LOCATION_NAME,
            current_overlay_file="",
            processed_event_keys=(),
            cooldowns={},
            failed_attempts={},
            last_publish_manifest_path=publish_result.manifest_path,
            last_publish_ts=time.time(),
            last_action=f"Managed Vault initialized. Imported {imported} file(s) into Default.",
            last_request_ts=0.0,
            last_error="",
            unmanaged_live_files=publish_result.unmanaged_live_files,
        )
        if hasattr(self, "varac_bbs_vault_enabled_chk_main"):
            self.varac_bbs_vault_enabled_chk_main.setChecked(True)
        self._varac_bbs_vault_runtime_state_cache = vault_runtime_state_to_data(runtime_state)
        self._varac_bbs_vault_last_summary_cache = runtime_state.last_action
        self._refresh_varac_bbs_vault_location_list()
        self._refresh_varac_bbs_vault_status_label()
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _reset_varac_bbs_vault_to_default(self) -> None:
        root_txt = self.varac_bbs_vault_root_edit.text().strip() if hasattr(self, "varac_bbs_vault_root_edit") else ""
        live_bbs_dir = self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        default_id = (
            str(self.varac_bbs_vault_default_location_combo.currentData() or "").strip()
            if hasattr(self, "varac_bbs_vault_default_location_combo")
            else DEFAULT_LOCATION_ID
        )
        locations = load_vault_locations(self._varac_bbs_vault_locations_cache)
        runtime_state = load_vault_runtime_state(self._varac_bbs_vault_runtime_state_cache)
        try:
            result = reset_to_default_location(
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=root_txt,
                default_location_id=default_id,
                runtime_state=runtime_state,
                reason="manual_reset",
            )
        except Exception as exc:
            QMessageBox.warning(self, "Managed BBS Vault", f"Could not reset to default:\n{exc}")
            return
        self._varac_bbs_vault_runtime_state_cache = vault_runtime_state_to_data(result.runtime_state)
        self._varac_bbs_vault_last_summary_cache = result.summary
        self._refresh_varac_bbs_vault_status_label()
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _open_context_help(self, context_key: str) -> None:
        host = resolve_help_host(self)
        if host is not None and hasattr(host, "open_context_help"):
            try:
                host.open_context_help(context_key)
            except Exception:
                pass

    def _make_context_help_button(
        self,
        context_key: str,
        *,
        text: str = "Help",
        tooltip: str | None = None,
    ) -> QPushButton:
        btn = QPushButton(text)
        btn.setToolTip(tooltip or "Open focused help for this part of FreqInOut.")
        btn.clicked.connect(lambda _checked=False, key=context_key: self._open_context_help(key))
        btn.setProperty("context_help_key", context_key)
        btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self._context_help_buttons.append(btn)
        try:
            btn.setStyleSheet(button_style("secondary", resolve_theme(self.settings)))
        except Exception:
            pass
        return btn

    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)

        header_layout = QHBoxLayout()
        title_label = QLabel("<h2>Settings</h2>")
        header_layout.addWidget(title_label)
        self.loading_label = QLabel("Wilco. Standby for Spectrum QSY...")
        self.loading_label.setVisible(False)
        self.loading_label.setStyleSheet("padding: 2px 6px; border-radius: 4px;")
        header_layout.addWidget(self.loading_label)
        header_layout.addStretch()
        self.settings_help_btn = self._make_context_help_button(
            "tab.settings",
            text="Settings Help",
            tooltip="Open the Settings help overview.",
        )
        header_layout.addWidget(self.settings_help_btn)

        self.utc_label = QLabel()
        self.local_label = QLabel()
        self.utc_label.setVisible(False)
        self.local_label.setVisible(False)
        header_layout.addWidget(self.utc_label)
        header_layout.addWidget(self.local_label)
        main_layout.addLayout(header_layout)

        # Operator Information
        callsign_layout = QVBoxLayout()
        self.callsign_edit = QLineEdit()
        self.callsign_edit.setMaxLength(16)
        self.callsign_edit.setFixedWidth(150)
        self.name_edit = QLineEdit()
        self.name_edit.setFixedWidth(200)
        self.state_edit = QLineEdit()
        self.state_edit.setFixedWidth(80)
        self.grid6_edit = QLineEdit()
        self.grid6_edit.setMaxLength(6)
        self.grid6_edit.setFixedWidth(90)
        operator_grid = QGridLayout()
        operator_grid.setContentsMargins(0, 0, 0, 0)
        operator_grid.setHorizontalSpacing(12)
        operator_grid.setVerticalSpacing(10)
        operator_grid.addWidget(QLabel("Callsign:"), 0, 0)
        operator_grid.addWidget(self.callsign_edit, 0, 1)
        operator_grid.addWidget(QLabel("Name:"), 0, 2)
        operator_grid.addWidget(self.name_edit, 0, 3)
        operator_grid.addWidget(QLabel("State:"), 1, 0)
        operator_grid.addWidget(self.state_edit, 1, 1)
        operator_grid.addWidget(QLabel("Grid 6:"), 1, 2)
        operator_grid.addWidget(self.grid6_edit, 1, 3)
        operator_grid.setColumnStretch(4, 1)
        callsign_layout.addLayout(operator_grid)
        callsign_container = QWidget()
        callsign_container.setLayout(callsign_layout)
        callsign_group = QGroupBox("Operator Information")
        callsign_group_layout = QVBoxLayout()
        callsign_group_layout.setContentsMargins(10, 10, 10, 12)
        callsign_group_layout.setSpacing(6)
        callsign_help_row = QHBoxLayout()
        callsign_help_row.addStretch()
        self.operator_info_help_btn = self._make_context_help_button(
            "settings.operator",
            tooltip="Open help for Operator Information.",
        )
        callsign_help_row.addWidget(self.operator_info_help_btn)
        callsign_group_layout.addLayout(callsign_help_row)
        callsign_group_layout.addWidget(callsign_container)
        callsign_group.setLayout(callsign_group_layout)
        callsign_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.operator_information_section_group = callsign_group

        # FreqInOut settings
        op_layout = QVBoxLayout()

        # control mode (no timezone dropdown anymore)
        top_preferences_grid = QGridLayout()
        top_preferences_grid.setContentsMargins(0, 0, 0, 0)
        top_preferences_grid.setHorizontalSpacing(12)
        top_preferences_grid.setVerticalSpacing(10)
        top_preferences_grid.addWidget(QLabel("Theme:"), 0, 0)
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["Light", "Dark"])
        self.theme_combo.currentIndexChanged.connect(self._on_theme_changed)
        top_preferences_grid.addWidget(self.theme_combo, 0, 1)
        top_preferences_grid.addWidget(QLabel("Text Size:"), 0, 2)
        self.text_size_combo = QComboBox()
        self.text_size_combo.addItems(["Normal", "Medium", "Large"])
        self.text_size_combo.currentIndexChanged.connect(self._on_text_size_changed)
        top_preferences_grid.addWidget(self.text_size_combo, 0, 3)
        top_preferences_grid.addWidget(QLabel("Frequency Control:"), 1, 0)
        self.control_combo = QComboBox()
        self.control_combo.addItems(["FLRig", "RIGCTLD", "JS8Call", "Manual"])
        top_preferences_grid.addWidget(self.control_combo, 1, 1)
        self.use_scheduler_chk = QCheckBox("Use FreqInOut Scheduler")
        self.use_scheduler_chk.setToolTip("Enable automatic schedule-driven frequency changes.")
        top_preferences_grid.addWidget(self.use_scheduler_chk, 1, 2, 1, 2)
        top_preferences_grid.setColumnStretch(5, 1)

        enforcement_choices = ["On Schedule Change", "Prompt"]
        prompt_choices = [
            "Select Interval",
            "Hourly",
            "Every 5 minutes",
            "Every 10 minutes",
            "Every 15 minutes",
            "Every 30 minutes",
        ]

        left_column_layout = QVBoxLayout()
        left_column_layout.setSpacing(10)
        left_column_layout.addLayout(top_preferences_grid)

        def build_timer_row(title_label: QLabel, heading_text: str, enforce_combo: QComboBox, prompt_combo: QComboBox) -> QWidget:
            wrapper = QWidget()
            wrapper_layout = QVBoxLayout(wrapper)
            wrapper_layout.setContentsMargins(0, 0, 0, 0)
            wrapper_layout.setSpacing(6)
            title_label.setText(heading_text)
            wrapper_layout.addWidget(title_label)
            controls_row = QHBoxLayout()
            controls_row.setContentsMargins(0, 0, 0, 0)
            controls_row.setSpacing(10)
            controls_row.addWidget(QLabel("Mode"))
            controls_row.addWidget(enforce_combo, 0)
            controls_row.addWidget(QLabel("Prompt Interval"))
            controls_row.addWidget(prompt_combo, 0)
            controls_row.addStretch()
            wrapper_layout.addLayout(controls_row)
            return wrapper

        self.freq_timer_label = QLabel("Frequency Timer")
        self.freq_enforce_combo = QComboBox()
        self.freq_enforce_combo.addItems(enforcement_choices)
        self.freq_enforce_combo.setMinimumWidth(150)
        self.freq_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self.freq_prompt_label = QLabel("Prompt Interval")
        self.freq_prompt_combo = QComboBox()
        self.freq_prompt_combo.addItems(prompt_choices)
        self.freq_prompt_combo.setMinimumWidth(170)
        self.freq_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.freq_prompt_combo)
        left_column_layout.addWidget(
            build_timer_row(self.freq_timer_label, "Frequency Timer", self.freq_enforce_combo, self.freq_prompt_combo)
        )

        self.fldigi_timer_label = QLabel("FLDigi Mode Timer")
        self.fldigi_enforce_combo = QComboBox()
        self.fldigi_enforce_combo.addItems(enforcement_choices)
        self.fldigi_enforce_combo.setMinimumWidth(150)
        self.fldigi_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self.fldigi_prompt_label = QLabel("Prompt Interval")
        self.fldigi_prompt_combo = QComboBox()
        self.fldigi_prompt_combo.addItems(prompt_choices)
        self.fldigi_prompt_combo.setMinimumWidth(170)
        self.fldigi_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.fldigi_prompt_combo)
        left_column_layout.addWidget(
            build_timer_row(
                self.fldigi_timer_label,
                "FLDigi Mode Timer",
                self.fldigi_enforce_combo,
                self.fldigi_prompt_combo,
            )
        )

        self.js8_timer_label = QLabel("JS8 Offset Timer")
        self.js8_enforce_combo = QComboBox()
        self.js8_enforce_combo.addItems(enforcement_choices)
        self.js8_enforce_combo.setMinimumWidth(150)
        self.js8_enforce_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self.js8_prompt_label = QLabel("Prompt Interval")
        self.js8_prompt_combo = QComboBox()
        self.js8_prompt_combo.addItems(prompt_choices)
        self.js8_prompt_combo.setMinimumWidth(170)
        self.js8_prompt_combo.currentIndexChanged.connect(self._on_enforcement_changed)
        self._disable_prompt_hint_item(self.js8_prompt_combo)
        left_column_layout.addWidget(
            build_timer_row(self.js8_timer_label, "JS8 Offset Timer", self.js8_enforce_combo, self.js8_prompt_combo)
        )
        left_column_layout.addStretch()

        log_warn_tip = (
            "Logging may reduce performance and increase disk usage. "
            "Enable INFO/DEBUG only while troubleshooting."
        )
        self.logging_group, logging_group_layout = self._make_compact_settings_panel(
            object_name="settingsLoggingPanel",
            accessible_name="Logging and diagnostics settings",
            tooltip=log_warn_tip,
            maximum_width=760,
        )

        self.logging_warning_label = QLabel(
            "Use INFO or DEBUG only while troubleshooting; verbose logs can slow the station and grow quickly."
        )
        self.logging_warning_label.setWordWrap(True)
        self.logging_warning_label.setMaximumWidth(720)
        self.logging_warning_label.setAccessibleName("Logging performance warning")
        self.logging_warning_label.setToolTip(log_warn_tip)
        logging_group_layout.addWidget(self.logging_warning_label, 0, 0, 1, 6)

        self.log_level_label = QLabel("Logging Level:")
        logging_group_layout.addWidget(self.log_level_label, 1, 0, Qt.AlignLeft)
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"])
        self._fit_combo_to_contents(self.log_level_combo, minimum=140, maximum=260)
        self.log_level_combo.setToolTip(log_warn_tip)
        self.log_level_combo.currentTextChanged.connect(self._on_log_level_changed)
        logging_group_layout.addWidget(self.log_level_combo, 1, 1)

        self.enable_timed_debug_btn = QPushButton("Enable DEBUG For")
        self.enable_timed_debug_btn.setToolTip(log_warn_tip)
        self.enable_timed_debug_btn.clicked.connect(self._enable_timed_debug)
        logging_group_layout.addWidget(self.enable_timed_debug_btn, 1, 2)

        self.debug_duration_combo = QComboBox()
        self.debug_duration_combo.addItem("15 min", 15)
        self.debug_duration_combo.addItem("30 min", 30)
        self.debug_duration_combo.addItem("60 min", 60)
        self.debug_duration_combo.setCurrentIndex(1)
        self._fit_combo_to_contents(self.debug_duration_combo, minimum=110, maximum=220)
        self.debug_duration_combo.setToolTip("Automatically reverts to previous logging level when timer expires.")
        logging_group_layout.addWidget(self.debug_duration_combo, 1, 3)

        self.logging_actions_grid = QGridLayout()
        self.logging_actions_grid.setContentsMargins(0, 0, 0, 0)
        self.logging_actions_grid.setHorizontalSpacing(8)
        self.logging_actions_grid.setVerticalSpacing(6)

        self.open_logs_btn = QPushButton("Open Logs")
        self.open_logs_btn.setAccessibleName("Open logs")
        self.open_logs_btn.setToolTip(log_warn_tip)
        self.open_logs_btn.clicked.connect(self._request_open_logs)
        self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0)

        self.open_log_folder_btn = QPushButton("Open Log Folder")
        self.open_log_folder_btn.setAccessibleName("Open log folder")
        self.open_log_folder_btn.setToolTip(log_warn_tip)
        self.open_log_folder_btn.clicked.connect(self._open_log_folder)
        self.logging_actions_grid.addWidget(self.open_log_folder_btn, 0, 1)

        self.export_diag_btn = QPushButton("Export Diagnostics")
        self.export_diag_btn.setAccessibleName("Export diagnostics")
        self.export_diag_btn.setToolTip(log_warn_tip)
        self.export_diag_btn.clicked.connect(self._export_diagnostics)
        self.logging_actions_grid.addWidget(self.export_diag_btn, 0, 2)
        self.logging_actions_grid.setColumnStretch(3, 1)
        logging_group_layout.addLayout(self.logging_actions_grid, 2, 0, 1, 6)
        logging_group_layout.setColumnStretch(4, 1)

        left_widget = QWidget()
        left_widget.setLayout(left_column_layout)
        left_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        op_layout.addWidget(left_widget)

        self._align_enforcement_labels()
        self._update_logging_actions_layout()

        # Operating status indicators
        self.status_layout = QHBoxLayout()
        status_container = QWidget()
        status_container.setLayout(self.status_layout)
        status_group = QGroupBox("Radio Status")
        status_group_layout = QVBoxLayout()
        status_group_layout.setContentsMargins(10, 10, 10, 12)
        status_group_layout.setSpacing(6)
        status_group_layout.addWidget(status_container)
        status_group.setLayout(status_group_layout)
        status_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.status_group = status_group
        self._rebuild_status_indicators()

        configured_radios_group = QGroupBox("Configured Radios")
        configured_radios_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        configured_radios_layout = QVBoxLayout(configured_radios_group)
        configured_radios_layout.setContentsMargins(10, 10, 10, 12)
        configured_radios_layout.setSpacing(6)
        self.device_profile_selector_title_label = QLabel("Select Radio To Edit")
        selector_title_font = self.device_profile_selector_title_label.font()
        selector_title_font.setBold(True)
        self.device_profile_selector_title_label.setFont(selector_title_font)
        configured_radios_layout.addWidget(self.device_profile_selector_title_label)
        self.device_profile_selector_widget = QWidget()
        self.device_profile_selector_layout = QHBoxLayout(self.device_profile_selector_widget)
        self.device_profile_selector_layout.setContentsMargins(0, 0, 0, 0)
        self.device_profile_selector_layout.setSpacing(8)
        self.device_profile_selector_layout.addStretch()
        self.device_profile_selector_scroll = QScrollArea()
        self.device_profile_selector_scroll.setWidgetResizable(True)
        self.device_profile_selector_scroll.setFrameShape(QFrame.NoFrame)
        self.device_profile_selector_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.device_profile_selector_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.device_profile_selector_scroll.setMinimumHeight(72)
        self.device_profile_selector_scroll.setMaximumHeight(92)
        self.device_profile_selector_scroll.setWidget(self.device_profile_selector_widget)
        configured_radios_layout.addWidget(self.device_profile_selector_scroll)
        configured_radios_layout.addWidget(self.status_group)
        main_layout.addWidget(configured_radios_group)
        self.configured_radios_group = configured_radios_group

        sections_row = QHBoxLayout()
        sections_row.setSpacing(10)
        self.sections_nav_list = QListWidget()
        self.sections_nav_list.setMinimumWidth(170)
        self.sections_nav_list.setMaximumWidth(230)
        self.sections_nav_list.setSelectionMode(QListWidget.SingleSelection)
        self.sections_nav_list.setUniformItemSizes(True)
        self.sections_nav_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.sections_nav_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.sections_nav_list.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.sections_nav_list.setMouseTracking(True)
        self.sections_nav_list.setItemDelegate(_SettingsSectionNavDelegate(self))
        self.sections_nav_list.currentRowChanged.connect(self._on_section_nav_changed)
        nav_panel = QWidget()
        nav_panel.setObjectName("settingsSectionNavPanel")
        nav_panel.setMinimumWidth(180)
        nav_panel.setMaximumWidth(240)
        nav_panel.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        nav_panel_layout = QVBoxLayout(nav_panel)
        nav_panel_layout.setContentsMargins(0, 0, 0, 0)
        nav_panel_layout.setSpacing(6)
        self.settings_compact_header = QFrame()
        self.settings_compact_header.setObjectName("settingsCompactHeaderBar")
        self.settings_compact_header.setFrameShape(QFrame.StyledPanel)
        self.settings_compact_header.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        settings_header_layout = QGridLayout(self.settings_compact_header)
        settings_header_layout.setContentsMargins(10, 8, 10, 8)
        settings_header_layout.setHorizontalSpacing(10)
        settings_header_layout.setVerticalSpacing(6)
        self.add_device_profile_btn = QPushButton("Add Radio")
        self.add_device_profile_btn.setToolTip(
            "Start guided setup for a new radio or SDR: identity, software used, connection, and readiness."
        )
        self.add_device_profile_btn.setAccessibleName("Guided Add Radio")
        self.add_device_profile_btn.clicked.connect(self._add_device_profile)
        self.settings_section_label = QLabel("Settings section")
        self.settings_section_combo = QComboBox()
        self.settings_section_combo.setObjectName("settingsSectionCombo")
        self.settings_section_combo.setAccessibleName("Settings section selector")
        self.settings_section_combo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
        self.settings_section_combo.setMinimumContentsLength(26)
        self.settings_section_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.settings_section_combo.currentIndexChanged.connect(self._on_settings_section_combo_changed)
        settings_header_layout.addWidget(self.add_device_profile_btn, 0, 0)
        settings_header_layout.addWidget(self.settings_section_label, 0, 1)
        settings_header_layout.addWidget(self.settings_section_combo, 0, 2)
        settings_header_layout.setColumnStretch(2, 1)
        main_layout.addWidget(self.settings_compact_header)
        self._global_settings_nav_collapsed = False
        self._radio_settings_nav_collapsed = False
        self.global_settings_toggle_btn = QToolButton()
        self.global_settings_toggle_btn.setCheckable(True)
        self.global_settings_toggle_btn.setChecked(True)
        self.global_settings_toggle_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self.global_settings_toggle_btn.setArrowType(Qt.DownArrow)
        self.global_settings_toggle_btn.setText("Global Settings")
        self.global_settings_toggle_btn.setMinimumHeight(28)
        self.global_settings_toggle_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.global_settings_toggle_btn.setAccessibleName("Settings navigation group: Global Settings")
        self.global_settings_toggle_btn.clicked.connect(self._on_global_settings_toggle)
        nav_panel_layout.addWidget(self.global_settings_toggle_btn)
        self.global_section_buttons_widget = QWidget()
        self.global_section_buttons_layout = QVBoxLayout(self.global_section_buttons_widget)
        self.global_section_buttons_layout.setContentsMargins(0, 0, 0, 0)
        self.global_section_buttons_layout.setSpacing(4)
        self.global_section_buttons_widget.setVisible(False)
        nav_panel_layout.addWidget(self.global_section_buttons_widget)
        self.radio_settings_toggle_btn = QToolButton()
        self.radio_settings_toggle_btn.setCheckable(True)
        self.radio_settings_toggle_btn.setChecked(True)
        self.radio_settings_toggle_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self.radio_settings_toggle_btn.setArrowType(Qt.DownArrow)
        self.radio_settings_toggle_btn.setText("Selected Radio")
        self.radio_settings_toggle_btn.setMinimumHeight(28)
        self.radio_settings_toggle_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.radio_settings_toggle_btn.setAccessibleName("Settings navigation group: Selected Radio")
        self.radio_settings_toggle_btn.clicked.connect(self._on_radio_settings_toggle)
        # Keep the legacy attribute name for older helper paths that expect it.
        self.radio_specific_nav_label = self.radio_settings_toggle_btn
        nav_panel_layout.addWidget(self.radio_settings_toggle_btn)
        self.radio_section_buttons_widget = QWidget()
        self.radio_section_buttons_layout = QVBoxLayout(self.radio_section_buttons_widget)
        self.radio_section_buttons_layout.setContentsMargins(0, 0, 0, 0)
        self.radio_section_buttons_layout.setSpacing(4)
        nav_panel_layout.addWidget(self.radio_section_buttons_widget)
        nav_panel_layout.addStretch()
        self.sections_nav_list.hide()
        self.settings_section_nav_scroll = QScrollArea()
        self.settings_section_nav_scroll.setObjectName("settingsSectionNavScroll")
        self.settings_section_nav_scroll.setWidgetResizable(True)
        self.settings_section_nav_scroll.setFrameShape(QFrame.NoFrame)
        self.settings_section_nav_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.settings_section_nav_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.settings_section_nav_scroll.setMinimumWidth(188)
        self.settings_section_nav_scroll.setMaximumWidth(250)
        self.settings_section_nav_scroll.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        self.settings_section_nav_scroll.setWidget(nav_panel)
        self.settings_section_nav_scroll.hide()

        self.sections_stack = QStackedWidget()
        self.sections_stack.setMinimumWidth(0)
        self.sections_stack.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Expanding)
        self.sections_scroll = QScrollArea()
        self.sections_scroll.setWidgetResizable(True)
        self.sections_scroll.setFrameShape(QFrame.NoFrame)
        self.sections_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.sections_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.sections_stack.currentChanged.connect(lambda _idx: self._sync_current_section_scroll_size())
        self.sections_scroll.setWidget(self.sections_stack)
        sections_row.addWidget(self.sections_scroll, 1)
        main_layout.addLayout(sections_row, 1)

        op_container = QWidget()
        op_container.setLayout(op_layout)
        op_group = self._make_collapsible_group(
            "Preferences",
            op_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.freqinout",
        )
        self._register_collapsible_group(op_group, self._summary_freqinout_settings)
        self._set_section_health_key(op_group, "freqinout")
        op_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self._add_settings_section(self.operator_information_section_group, scope="global")
        self._add_settings_section(op_group, scope="global")

        def _make_support_card(title: str, status_object_name: str) -> tuple[QFrame, QLabel, QLabel]:
            card = QFrame()
            card.setFrameShape(QFrame.StyledPanel)
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(12, 10, 12, 10)
            card_layout.setSpacing(6)
            title_label = QLabel(title)
            title_font = title_label.font()
            title_font.setBold(True)
            title_label.setFont(title_font)
            card_layout.addWidget(title_label)
            status_label = QLabel()
            status_label.setObjectName(status_object_name)
            status_label.setWordWrap(True)
            card_layout.addWidget(status_label)
            return card, title_label, status_label

        def _make_radio_profile_dashboard_section(title: str, content: QWidget, *, checked: bool = True) -> QGroupBox:
            section = QGroupBox(title)
            section.setCheckable(True)
            section.setChecked(bool(checked))
            section.setToolTip(f"Show or hide the {title} section.")
            section.setAccessibleName(f"{title} section")
            section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            content.setVisible(bool(checked))
            section_layout = QVBoxLayout(section)
            section_layout.setContentsMargins(10, 10, 10, 12)
            section_layout.setSpacing(6)
            section_layout.addWidget(content)
            section.toggled.connect(content.setVisible)
            return section

        device_group = QGroupBox("Radio Profiles")
        device_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        device_layout = QVBoxLayout()
        device_layout.setSpacing(6)
        device_group.setLayout(device_layout)

        self.device_profiles_hint_label = QLabel()
        self.device_profiles_hint_label.setWordWrap(True)
        device_layout.addWidget(self.device_profiles_hint_label)

        self.multi_rig_status_card = QFrame()
        self.multi_rig_status_card.setFrameShape(QFrame.StyledPanel)
        multi_rig_status_layout = QVBoxLayout(self.multi_rig_status_card)
        multi_rig_status_layout.setContentsMargins(12, 10, 12, 10)
        multi_rig_status_layout.setSpacing(6)
        self.multi_rig_status_title_label = QLabel("Multi-Rig Setup")
        multi_rig_title_font = self.multi_rig_status_title_label.font()
        multi_rig_title_font.setBold(True)
        self.multi_rig_status_title_label.setFont(multi_rig_title_font)
        multi_rig_status_layout.addWidget(self.multi_rig_status_title_label)
        self.multi_rig_status_summary_label = QLabel()
        self.multi_rig_status_summary_label.setWordWrap(True)
        multi_rig_status_layout.addWidget(self.multi_rig_status_summary_label)
        self.multi_rig_status_detail_label = QLabel()
        self.multi_rig_status_detail_label.setWordWrap(True)
        multi_rig_status_layout.addWidget(self.multi_rig_status_detail_label)
        self.multi_rig_autoconfig_preview_label = QLabel()
        self.multi_rig_autoconfig_preview_label.setObjectName("multiRigAutoconfigPreview")
        self.multi_rig_autoconfig_preview_label.setWordWrap(True)
        self.multi_rig_autoconfig_preview_label.setVisible(False)
        multi_rig_status_layout.addWidget(self.multi_rig_autoconfig_preview_label)
        self.multi_rig_status_actions_widget = QWidget()
        multi_rig_status_actions = QHBoxLayout(self.multi_rig_status_actions_widget)
        multi_rig_status_actions.setContentsMargins(0, 0, 0, 0)
        multi_rig_status_actions.setSpacing(8)
        self.multi_rig_preview_autoconfig_btn = QPushButton("Preview Configure Automatically")
        self.multi_rig_preview_autoconfig_btn.setToolTip(
            "Scan for installed apps and show what FIO would configure before changing anything."
        )
        self.multi_rig_preview_autoconfig_btn.clicked.connect(self._preview_multi_rig_autoconfiguration)
        self.multi_rig_setup_btn = QPushButton("Set up Multi-Rig")
        self.multi_rig_setup_btn.clicked.connect(self._start_multi_rig_setup)
        self.multi_rig_not_now_btn = QPushButton("Not Now")
        self.multi_rig_not_now_btn.clicked.connect(self._defer_multi_rig_setup)
        self.multi_rig_copy_summary_btn = QPushButton("Copy Summary")
        self.multi_rig_copy_summary_btn.clicked.connect(self._copy_multi_rig_status_summary)
        multi_rig_status_actions.addWidget(self.multi_rig_preview_autoconfig_btn)
        multi_rig_status_actions.addWidget(self.multi_rig_setup_btn)
        multi_rig_status_actions.addWidget(self.multi_rig_not_now_btn)
        multi_rig_status_actions.addWidget(self.multi_rig_copy_summary_btn)
        multi_rig_status_actions.addStretch(1)
        multi_rig_status_layout.addWidget(self.multi_rig_status_actions_widget)
        device_layout.addWidget(self.multi_rig_status_card)

        self.device_profile_detail_card = QFrame()
        self.device_profile_detail_card.setFrameShape(QFrame.StyledPanel)
        detail_layout = QVBoxLayout(self.device_profile_detail_card)
        detail_layout.setContentsMargins(12, 10, 12, 10)
        detail_layout.setSpacing(6)
        self.device_profile_detail_title_label = QLabel("Selected Radio")
        detail_title_font = self.device_profile_detail_title_label.font()
        detail_title_font.setBold(True)
        self.device_profile_detail_title_label.setFont(detail_title_font)
        detail_layout.addWidget(self.device_profile_detail_title_label)
        self.device_profile_status_chips_widget = QWidget()
        self.device_profile_status_chips_layout = QHBoxLayout(self.device_profile_status_chips_widget)
        self.device_profile_status_chips_layout.setContentsMargins(0, 0, 0, 0)
        self.device_profile_status_chips_layout.setSpacing(6)
        self.device_profile_status_chips_widget.setVisible(False)
        detail_layout.addWidget(self.device_profile_status_chips_widget)
        self.device_profile_detail_label = QLabel("Select a radio to edit that radio's settings.")
        self.device_profile_detail_label.setWordWrap(True)
        detail_layout.addWidget(self.device_profile_detail_label)

        radio_identity_content = QWidget()
        radio_identity_layout = QVBoxLayout(radio_identity_content)
        radio_identity_layout.setContentsMargins(0, 0, 0, 0)
        radio_identity_layout.setSpacing(6)
        radio_identity_layout.addWidget(self.device_profile_detail_card)
        self.radio_profile_identity_section = _make_radio_profile_dashboard_section(
            "Radio Identity",
            radio_identity_content,
            checked=True,
        )
        device_layout.addWidget(self.radio_profile_identity_section)

        radio_profile_software_content = QWidget()
        radio_profile_software_layout = QVBoxLayout(radio_profile_software_content)
        radio_profile_software_layout.setContentsMargins(0, 0, 0, 0)
        radio_profile_software_layout.setSpacing(6)
        software_chips_title = QLabel("Software Enabled For This Radio")
        software_chips_font = software_chips_title.font()
        software_chips_font.setBold(True)
        software_chips_title.setFont(software_chips_font)
        radio_profile_software_layout.addWidget(software_chips_title)
        self.radio_profile_software_flags_widget = QWidget()
        software_flags_layout = QGridLayout(self.radio_profile_software_flags_widget)
        software_flags_layout.setContentsMargins(0, 0, 0, 0)
        software_flags_layout.setHorizontalSpacing(10)
        software_flags_layout.setVerticalSpacing(4)
        for index, (key, label) in enumerate(self._radio_profile_software_flag_defs()):
            chk = QCheckBox(label)
            chk.setToolTip(f"Enable {label} for the selected radio.")
            chk.setAccessibleName(f"Enable {label} for the selected radio")
            chk.stateChanged.connect(lambda _state, k=key: self._on_radio_profile_software_flag_changed(k))
            self._radio_profile_software_flag_checks[key] = chk
            software_flags_layout.addWidget(chk, index // 4, index % 4)
        radio_profile_software_layout.addWidget(self.radio_profile_software_flags_widget)
        self.radio_profile_software_chips_widget = QWidget()
        self.radio_profile_software_chips_layout = QGridLayout(self.radio_profile_software_chips_widget)
        self.radio_profile_software_chips_layout.setContentsMargins(0, 0, 0, 0)
        self.radio_profile_software_chips_layout.setHorizontalSpacing(8)
        self.radio_profile_software_chips_layout.setVerticalSpacing(6)
        self.radio_profile_software_chips_layout.setColumnStretch(4, 1)
        radio_profile_software_layout.addWidget(self.radio_profile_software_chips_widget)
        self.radio_profile_software_stack_section = _make_radio_profile_dashboard_section(
            "Software Stack",
            radio_profile_software_content,
            checked=True,
        )
        device_layout.addWidget(self.radio_profile_software_stack_section)

        self.radio_profile_stack_guidance_widget = QWidget()
        stack_guidance_layout = QVBoxLayout(self.radio_profile_stack_guidance_widget)
        stack_guidance_layout.setContentsMargins(0, 4, 0, 4)
        stack_guidance_layout.setSpacing(6)
        self.radio_profile_stack_guidance_title_label = QLabel("Stack Guidance")
        stack_guidance_title_font = self.radio_profile_stack_guidance_title_label.font()
        stack_guidance_title_font.setBold(True)
        self.radio_profile_stack_guidance_title_label.setFont(stack_guidance_title_font)
        stack_guidance_layout.addWidget(self.radio_profile_stack_guidance_title_label)
        self.radio_profile_stack_guidance_rows = QVBoxLayout()
        self.radio_profile_stack_guidance_rows.setContentsMargins(0, 0, 0, 0)
        self.radio_profile_stack_guidance_rows.setSpacing(4)
        stack_guidance_layout.addLayout(self.radio_profile_stack_guidance_rows)
        self.radio_profile_stack_guidance_widget.setVisible(False)
        stack_guidance_content = QWidget()
        stack_guidance_content_layout = QVBoxLayout(stack_guidance_content)
        stack_guidance_content_layout.setContentsMargins(0, 0, 0, 0)
        stack_guidance_content_layout.setSpacing(6)
        stack_guidance_content_layout.addWidget(self.radio_profile_stack_guidance_widget)
        self.radio_profile_stack_guidance_section = _make_radio_profile_dashboard_section(
            "Stack Guidance",
            stack_guidance_content,
            checked=False,
        )
        self.radio_profile_stack_guidance_section.setVisible(False)
        device_layout.addWidget(self.radio_profile_stack_guidance_section)

        radio_profile_connection_content = QWidget()
        radio_profile_connection_layout = QFormLayout(radio_profile_connection_content)
        radio_profile_connection_layout.setContentsMargins(0, 0, 0, 0)
        radio_profile_connection_layout.setSpacing(6)
        radio_profile_connection_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self.radio_profile_connection_backend_label = QLabel("--")
        self.radio_profile_connection_backend_label.setWordWrap(True)
        self.radio_profile_connection_endpoint_label = QLabel("--")
        self.radio_profile_connection_endpoint_label.setWordWrap(True)
        self.radio_profile_connection_ptt_label = QLabel("--")
        self.radio_profile_connection_ptt_label.setWordWrap(True)
        self.radio_profile_connection_launch_label = QLabel("--")
        self.radio_profile_connection_launch_label.setWordWrap(True)
        radio_profile_connection_layout.addRow("Control:", self.radio_profile_connection_backend_label)
        radio_profile_connection_layout.addRow("Endpoint:", self.radio_profile_connection_endpoint_label)
        radio_profile_connection_layout.addRow("PTT group:", self.radio_profile_connection_ptt_label)
        radio_profile_connection_layout.addRow("Launch:", self.radio_profile_connection_launch_label)
        self.radio_profile_connection_section = _make_radio_profile_dashboard_section(
            "Connection Details",
            radio_profile_connection_content,
            checked=False,
        )
        device_layout.addWidget(self.radio_profile_connection_section)

        radio_profile_frequency_content = QWidget()
        radio_profile_frequency_layout = QFormLayout(radio_profile_frequency_content)
        radio_profile_frequency_layout.setContentsMargins(0, 0, 0, 0)
        radio_profile_frequency_layout.setSpacing(6)
        radio_profile_frequency_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self.radio_profile_frequency_schedule_label = QLabel("--")
        self.radio_profile_frequency_schedule_label.setWordWrap(True)
        self.radio_profile_frequency_scheduler_label = QLabel("--")
        self.radio_profile_frequency_scheduler_label.setWordWrap(True)
        self.radio_profile_frequency_js8_offset_label = QLabel("--")
        self.radio_profile_frequency_js8_offset_label.setWordWrap(True)
        self.radio_profile_frequency_timer_source_label = QLabel("--")
        self.radio_profile_frequency_timer_source_label.setWordWrap(True)
        radio_profile_frequency_layout.addRow("Schedule:", self.radio_profile_frequency_schedule_label)
        radio_profile_frequency_layout.addRow("Scheduler:", self.radio_profile_frequency_scheduler_label)
        radio_profile_frequency_layout.addRow("JS8 offset:", self.radio_profile_frequency_js8_offset_label)
        radio_profile_frequency_layout.addRow("Timer source:", self.radio_profile_frequency_timer_source_label)

        self.radio_profile_timer_scheduler_chk = QCheckBox("Scheduler automation for this radio")
        self.radio_profile_timer_scheduler_chk.setToolTip("Enable or disable scheduler automation for the selected radio.")
        self.radio_profile_timer_scheduler_chk.stateChanged.connect(self._on_radio_profile_timer_policy_changed)
        radio_profile_frequency_layout.addRow("", self.radio_profile_timer_scheduler_chk)
        self._radio_profile_timer_policy_controls["scheduler_enabled"] = self.radio_profile_timer_scheduler_chk

        def _make_radio_profile_timer_combo(items: Sequence[str], *, minimum: int) -> QComboBox:
            combo = QComboBox()
            combo.addItems(list(items))
            combo.setMinimumWidth(minimum)
            self._fit_combo_to_contents(combo, minimum=minimum)
            combo.currentIndexChanged.connect(self._on_radio_profile_timer_policy_changed)
            return combo

        self.radio_profile_default_hold_combo = _make_radio_profile_timer_combo(
            tuple(f"{minutes} minutes" for minutes in sorted(SUPPORTED_HOLD_DURATION_MINUTES)),
            minimum=140,
        )
        self.radio_profile_default_hold_combo.setAccessibleName("Default hold duration")
        radio_profile_frequency_layout.addRow("Default hold:", self.radio_profile_default_hold_combo)
        self._radio_profile_timer_policy_controls["schedule_hold_minutes_default"] = self.radio_profile_default_hold_combo

        def _add_radio_profile_timer_policy_row(label: str, mode_key: str, prompt_key: str) -> None:
            row = QWidget()
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(8)
            mode_combo = _make_radio_profile_timer_combo(("On Schedule Change", "Prompt"), minimum=150)
            prompt_combo = _make_radio_profile_timer_combo(
                ("Hourly", "Every 5 minutes", "Every 10 minutes", "Every 15 minutes", "Every 30 minutes"),
                minimum=170,
            )
            mode_combo.setAccessibleName(f"{label.rstrip(':')} mode")
            prompt_combo.setAccessibleName(f"{label.rstrip(':')} prompt interval")
            row_layout.addWidget(QLabel("Mode"))
            row_layout.addWidget(mode_combo, 0)
            row_layout.addWidget(QLabel("Prompt"))
            row_layout.addWidget(prompt_combo, 0)
            row_layout.addStretch(1)
            radio_profile_frequency_layout.addRow(label, row)
            self._radio_profile_timer_policy_controls[mode_key] = mode_combo
            self._radio_profile_timer_policy_controls[prompt_key] = prompt_combo

        _add_radio_profile_timer_policy_row("Frequency timer:", "freq_enforcement_mode", "freq_prompt_interval")
        _add_radio_profile_timer_policy_row("FLDigi mode timer:", "fldigi_enforcement_mode", "fldigi_prompt_interval")
        _add_radio_profile_timer_policy_row("JS8 offset timer:", "js8_enforcement_mode", "js8_prompt_interval")

        self.radio_profile_frequency_section = _make_radio_profile_dashboard_section(
            "Frequency / Timer Behavior",
            radio_profile_frequency_content,
            checked=False,
        )
        device_layout.addWidget(self.radio_profile_frequency_section)

        radio_profile_optional_content = QWidget()
        radio_profile_optional_layout = QFormLayout(radio_profile_optional_content)
        radio_profile_optional_layout.setContentsMargins(0, 0, 0, 0)
        radio_profile_optional_layout.setSpacing(6)
        radio_profile_optional_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self.radio_profile_optional_ptt_label = QLabel("--")
        self.radio_profile_optional_ptt_label.setWordWrap(True)
        self.radio_profile_optional_antenna_label = QLabel("--")
        self.radio_profile_optional_antenna_label.setWordWrap(True)
        self.radio_profile_optional_frontend_label = QLabel("--")
        self.radio_profile_optional_frontend_label.setWordWrap(True)
        self.radio_profile_optional_amplifier_label = QLabel("--")
        self.radio_profile_optional_amplifier_label.setWordWrap(True)
        self.radio_profile_optional_notes_label = QLabel("--")
        self.radio_profile_optional_notes_label.setWordWrap(True)
        radio_profile_optional_layout.addRow("PTT group:", self.radio_profile_optional_ptt_label)
        radio_profile_optional_layout.addRow("Antenna group:", self.radio_profile_optional_antenna_label)
        radio_profile_optional_layout.addRow("Front-end group:", self.radio_profile_optional_frontend_label)
        radio_profile_optional_layout.addRow("Amplifier group:", self.radio_profile_optional_amplifier_label)
        radio_profile_optional_layout.addRow("Notes:", self.radio_profile_optional_notes_label)
        self.radio_profile_optional_section = _make_radio_profile_dashboard_section(
            "Optional Groups and Notes",
            radio_profile_optional_content,
            checked=False,
        )
        device_layout.addWidget(self.radio_profile_optional_section)

        radio_profile_inventory_content = QWidget()
        radio_profile_inventory_layout = QFormLayout(radio_profile_inventory_content)
        radio_profile_inventory_layout.setContentsMargins(0, 0, 0, 0)
        radio_profile_inventory_layout.setSpacing(6)
        radio_profile_inventory_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self.radio_profile_inventory_id_label = QLabel("--")
        self.radio_profile_inventory_id_label.setWordWrap(True)
        self.radio_profile_inventory_system_key_label = QLabel("--")
        self.radio_profile_inventory_system_key_label.setWordWrap(True)
        self.radio_profile_inventory_instance_label = QLabel("--")
        self.radio_profile_inventory_instance_label.setWordWrap(True)
        self.radio_profile_inventory_class_label = QLabel("--")
        self.radio_profile_inventory_class_label.setWordWrap(True)
        self.radio_profile_inventory_model_label = QLabel("--")
        self.radio_profile_inventory_model_label.setWordWrap(True)
        self.radio_profile_inventory_runtime_label = QLabel("--")
        self.radio_profile_inventory_runtime_label.setWordWrap(True)
        radio_profile_inventory_layout.addRow("Profile ID:", self.radio_profile_inventory_id_label)
        radio_profile_inventory_layout.addRow("System key:", self.radio_profile_inventory_system_key_label)
        radio_profile_inventory_layout.addRow("Instance:", self.radio_profile_inventory_instance_label)
        radio_profile_inventory_layout.addRow("Class / deploy:", self.radio_profile_inventory_class_label)
        radio_profile_inventory_layout.addRow("Model:", self.radio_profile_inventory_model_label)
        radio_profile_inventory_layout.addRow("Runtime:", self.radio_profile_inventory_runtime_label)
        self.radio_profile_inventory_section = _make_radio_profile_dashboard_section(
            "Advanced Inventory",
            radio_profile_inventory_content,
            checked=False,
        )
        device_layout.addWidget(self.radio_profile_inventory_section)

        self.device_profile_readiness_card = QFrame()
        self.device_profile_readiness_card.setFrameShape(QFrame.StyledPanel)
        readiness_layout = QVBoxLayout(self.device_profile_readiness_card)
        readiness_layout.setContentsMargins(12, 10, 12, 10)
        readiness_layout.setSpacing(6)
        self.device_profile_readiness_title_label = QLabel("Focused Radio Readiness")
        readiness_title_font = self.device_profile_readiness_title_label.font()
        readiness_title_font.setBold(True)
        self.device_profile_readiness_title_label.setFont(readiness_title_font)
        readiness_layout.addWidget(self.device_profile_readiness_title_label)
        self.device_profile_readiness_status_label = QLabel(
            "Select a radio to review the readiness checklist for that radio."
        )
        self.device_profile_readiness_status_label.setWordWrap(True)
        readiness_layout.addWidget(self.device_profile_readiness_status_label)
        self.device_profile_guardrail_status_label = QLabel("")
        self.device_profile_guardrail_status_label.setObjectName("deviceProfileGuardrailStatus")
        self.device_profile_guardrail_status_label.setWordWrap(True)
        self.device_profile_guardrail_status_label.setVisible(False)
        readiness_layout.addWidget(self.device_profile_guardrail_status_label)
        self.copy_guardrail_summary_btn = QPushButton("Copy Guardrails")
        self.copy_guardrail_summary_btn.setToolTip("Copy the current multi-rig guardrail warnings for review.")
        self.copy_guardrail_summary_btn.setVisible(False)
        self.copy_guardrail_summary_btn.clicked.connect(self._copy_device_profile_guardrail_warnings)
        self.review_guardrail_conflicts_btn = QPushButton("Review Conflicts")
        self.review_guardrail_conflicts_btn.setToolTip("Review affected radios and jump to the relevant Settings section.")
        self.review_guardrail_conflicts_btn.setVisible(False)
        self.review_guardrail_conflicts_btn.clicked.connect(self._review_device_profile_guardrail_conflicts)
        self.copy_readiness_summary_btn = QPushButton("Copy Readiness Summary")
        self.copy_readiness_summary_btn.clicked.connect(self._copy_readiness_summary)
        readiness_actions = QHBoxLayout()
        readiness_actions.setContentsMargins(0, 0, 0, 0)
        readiness_actions.addStretch()
        readiness_actions.addWidget(self.review_guardrail_conflicts_btn)
        readiness_actions.addWidget(self.copy_guardrail_summary_btn)
        readiness_actions.addWidget(self.copy_readiness_summary_btn)
        readiness_layout.addLayout(readiness_actions)
        readiness_content = QWidget()
        readiness_content_layout = QVBoxLayout(readiness_content)
        readiness_content_layout.setContentsMargins(0, 0, 0, 0)
        readiness_content_layout.setSpacing(6)
        readiness_content_layout.addWidget(self.device_profile_readiness_card)
        self.radio_profile_readiness_section = _make_radio_profile_dashboard_section(
            "Readiness",
            readiness_content,
            checked=True,
        )

        device_actions = QGridLayout()
        device_actions.setHorizontalSpacing(8)
        device_actions.setVerticalSpacing(6)
        self.edit_device_profile_btn = QPushButton("Advanced Radio Edit")
        self.edit_device_profile_btn.setToolTip("Edit selected-radio identity, role, hardware, and core connection details.")
        self.edit_device_profile_btn.setAccessibleName("Advanced Radio Edit")
        self.edit_device_profile_btn.clicked.connect(self._edit_device_profile)
        self.activate_device_profile_btn = QPushButton("Use Now")
        self.activate_device_profile_btn.clicked.connect(self._activate_selected_device_profiles)
        self.deactivate_device_profile_btn = QPushButton("Stop Using Now")
        self.deactivate_device_profile_btn.clicked.connect(self._deactivate_selected_device_profiles)
        self.assign_radio_schedule_btn = QPushButton("Assign Plan...")
        self.assign_radio_schedule_btn.clicked.connect(self._assign_schedule_to_selected_radios)
        self.restore_radio_schedule_btn = QPushButton("Restore Plan")
        self.restore_radio_schedule_btn.clicked.connect(self._restore_schedule_for_selected_radios)
        self.set_active_device_profile_btn = QPushButton("Make Default")
        self.set_active_device_profile_btn.clicked.connect(self._set_active_selected_device_profile)
        self.delete_device_profile_btn = QPushButton("Delete Selected")
        self.delete_device_profile_btn.clicked.connect(self._delete_device_profiles)
        device_actions.addWidget(QLabel("Selected Radio:"), 0, 0)
        device_actions.addWidget(self.edit_device_profile_btn, 0, 1)
        device_actions.addWidget(QLabel("Use:"), 1, 0)
        device_actions.addWidget(self.activate_device_profile_btn, 1, 1)
        device_actions.addWidget(self.deactivate_device_profile_btn, 1, 2)
        device_actions.addWidget(self.set_active_device_profile_btn, 1, 3)
        device_actions.addWidget(QLabel("Schedule:"), 2, 0)
        device_actions.addWidget(self.assign_radio_schedule_btn, 2, 1)
        device_actions.addWidget(self.restore_radio_schedule_btn, 2, 2)
        device_actions.addWidget(self.delete_device_profile_btn, 2, 3)
        device_actions.setColumnStretch(4, 1)
        radio_profile_actions_content = QWidget()
        radio_profile_actions_content.setLayout(device_actions)
        self.radio_profile_actions_section = _make_radio_profile_dashboard_section(
            "Selected Radio Actions",
            radio_profile_actions_content,
            checked=True,
        )
        device_layout.addWidget(self.radio_profile_actions_section)
        device_layout.addWidget(self.radio_profile_readiness_section)

        self.device_profiles_table = QTableWidget(0, 15)
        self.device_profiles_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Active",
                "Default",
                "Radio Name",
                "Model",
                "Backend",
                "Deploy",
                "Software",
                "Endpoint",
                "Assigned Plan",
                "Readiness",
                "Launch",
                "PTT Group",
                "Class",
                "Notes",
            ]
        )
        self.device_profiles_table.verticalHeader().setVisible(False)
        self.device_profiles_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.device_profiles_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.device_profiles_table.setSelectionMode(QTableWidget.SingleSelection)
        self.device_profiles_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        self.device_profiles_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.device_profiles_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.device_profiles_table.currentCellChanged.connect(
            lambda _r, _c, _pr, _pc: (
                self._on_device_profile_table_focus_changed(),
                self._update_device_profile_readiness_detail(),
                self._sync_software_radio_to_device_focus(),
                self._sync_schedule_views_to_device_focus(),
            )
        )
        self.device_profiles_table.cellClicked.connect(
            lambda _r, _c: (
                self._on_device_profile_table_focus_changed(),
                self._update_device_profile_readiness_detail(),
                self._sync_software_radio_to_device_focus(),
                self._sync_schedule_views_to_device_focus(),
            )
        )
        device_header = self.device_profiles_table.horizontalHeader()
        device_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(8, QHeaderView.Stretch)
        device_header.setSectionResizeMode(9, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(10, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(11, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(12, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(13, QHeaderView.ResizeToContents)
        device_header.setSectionResizeMode(14, QHeaderView.Stretch)
        self.device_profiles_advanced_group = QGroupBox("Advanced Radio Inventory")
        self.device_profiles_advanced_group.setCheckable(True)
        self.device_profiles_advanced_group.setChecked(False)
        self.device_profiles_advanced_group.setToolTip(
            "Open this when you need the full radio inventory table or batch-oriented details."
        )
        advanced_layout = QVBoxLayout(self.device_profiles_advanced_group)
        advanced_layout.setContentsMargins(10, 10, 10, 12)
        advanced_layout.setSpacing(6)
        self.device_profiles_advanced_hint = QLabel(
            "The selector above is the normal one-radio-at-a-time workflow. "
            "This inventory view keeps the full details available for review."
        )
        self.device_profiles_advanced_hint.setWordWrap(True)
        self.device_profiles_advanced_hint.setVisible(False)
        advanced_layout.addWidget(self.device_profiles_advanced_hint)
        advanced_layout.addWidget(self.device_profiles_table)
        self.device_profiles_table.setVisible(False)
        self.device_profiles_advanced_group.toggled.connect(self.device_profiles_table.setVisible)
        self.device_profiles_advanced_group.toggled.connect(self.device_profiles_advanced_hint.setVisible)
        device_layout.addWidget(self.device_profiles_advanced_group)

        device_container = QWidget()
        device_container.setLayout(device_layout)
        device_group = self._make_collapsible_group("Radio Profile", device_container, checked=True, fit_content=False)
        self._register_collapsible_group(device_group, self._summary_device_profiles)
        self._set_section_health_key(device_group, "radio_profiles")
        device_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.radio_profile_section_group = device_group
        self._add_settings_section(device_group, scope="radio")

        operating_group = QGroupBox("Frequency Plans")
        operating_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        operating_layout = QVBoxLayout()
        operating_layout.setSpacing(6)
        operating_group.setLayout(operating_layout)

        self.operating_profiles_hint_label = QLabel()
        self.operating_profiles_hint_label.setWordWrap(True)
        operating_layout.addWidget(self.operating_profiles_hint_label)
        (
            self.operating_profiles_guidance_card,
            self.operating_profiles_guidance_title_label,
            self.operating_profiles_guidance_status_label,
        ) = _make_support_card("Focused Frequency Plan Guidance", "operatingProfilesGuidanceStatus")
        operating_layout.addWidget(self.operating_profiles_guidance_card)

        operating_actions = QHBoxLayout()
        self.add_operating_profile_btn = QPushButton("Add Plan")
        self.add_operating_profile_btn.clicked.connect(self._add_operating_profile)
        self.edit_operating_profile_btn = QPushButton("Edit Selected")
        self.edit_operating_profile_btn.clicked.connect(self._edit_operating_profile)
        self.delete_operating_profile_btn = QPushButton("Delete Selected")
        self.delete_operating_profile_btn.clicked.connect(self._delete_operating_profiles)
        operating_actions.addStretch()
        operating_actions.addWidget(self.add_operating_profile_btn)
        operating_actions.addWidget(self.edit_operating_profile_btn)
        operating_actions.addWidget(self.delete_operating_profile_btn)
        operating_layout.addLayout(operating_actions)

        self.operating_profiles_table = QTableWidget(0, 6)
        self.operating_profiles_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Enabled",
                "Name",
                "Scheduler",
                "Behavior",
                "Description",
            ]
        )
        self.operating_profiles_table.verticalHeader().setVisible(False)
        self.operating_profiles_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.operating_profiles_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.operating_profiles_table.setSelectionMode(QTableWidget.SingleSelection)
        self.operating_profiles_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        self.operating_profiles_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.operating_profiles_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.operating_profiles_table.setWordWrap(False)
        self.operating_profiles_table.currentCellChanged.connect(
            lambda _r, _c, _pr, _pc: self._update_operating_profile_guidance_detail()
        )
        self.operating_profiles_table.cellClicked.connect(lambda _r, _c: self._update_operating_profile_guidance_detail())
        operating_header = self.operating_profiles_table.horizontalHeader()
        operating_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        operating_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        operating_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        operating_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        operating_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        operating_header.setSectionResizeMode(5, QHeaderView.Stretch)
        operating_layout.addWidget(self.operating_profiles_table)

        operating_container = QWidget()
        operating_container.setLayout(operating_layout)
        operating_group = self._make_collapsible_group(
            "Frequency Plans",
            operating_container,
            checked=True,
            fit_content=True,
            fit_content_in_stack=True,
        )
        self._register_collapsible_group(operating_group, self._summary_operating_profiles)
        operating_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.operating_profiles_section_group = operating_group
        self._add_settings_section(operating_group, scope="radio")

        assignments_group = QGroupBox("Assigned Plans")
        assignments_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        assignments_layout = QVBoxLayout()
        assignments_layout.setSpacing(6)
        assignments_group.setLayout(assignments_layout)

        self.device_assignments_scope_label = QLabel("Editing Radio: --")
        self.device_assignments_scope_label.setWordWrap(True)
        assignments_layout.addWidget(self.device_assignments_scope_label)

        self.device_assignments_hint_label = QLabel()
        self.device_assignments_hint_label.setWordWrap(True)
        assignments_layout.addWidget(self.device_assignments_hint_label)
        (
            self.device_assignments_guidance_card,
            self.device_assignments_guidance_title_label,
            self.device_assignments_guidance_status_label,
        ) = _make_support_card("Focused Assigned Plan Guidance", "deviceAssignmentsGuidanceStatus")
        assignments_layout.addWidget(self.device_assignments_guidance_card)

        assignments_actions = QHBoxLayout()
        self.assign_device_operating_profile_btn = QPushButton("Assign / Override...")
        self.assign_device_operating_profile_btn.clicked.connect(self._assign_operating_profile_to_selected_devices)
        self.temporary_profile_swap_btn = QPushButton("Temporary Plan Swap...")
        self.temporary_profile_swap_btn.clicked.connect(self._start_temporary_profile_swap)
        self.restore_profile_swap_btn = QPushButton("Restore Swap")
        self.restore_profile_swap_btn.clicked.connect(self._restore_temporary_profile_swap)
        self.restore_device_operating_profile_btn = QPushButton("Restore Default Plan")
        self.restore_device_operating_profile_btn.clicked.connect(self._restore_default_operating_profile_for_selected_devices)
        assignments_actions.addStretch()
        assignments_actions.addWidget(self.assign_device_operating_profile_btn)
        assignments_actions.addWidget(self.temporary_profile_swap_btn)
        assignments_actions.addWidget(self.restore_profile_swap_btn)
        assignments_actions.addWidget(self.restore_device_operating_profile_btn)
        assignments_layout.addLayout(assignments_actions)

        self.device_assignments_table = QTableWidget(0, 8)
        self.device_assignments_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Active",
                "Default",
                "Radio",
                "Frequency Plan",
                "State",
                "Policy",
                "Endpoint",
            ]
        )
        self.device_assignments_table.verticalHeader().setVisible(False)
        self.device_assignments_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.device_assignments_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.device_assignments_table.setSelectionMode(QTableWidget.SingleSelection)
        self.device_assignments_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        self.device_assignments_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.device_assignments_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.device_assignments_table.currentCellChanged.connect(
            lambda _r, _c, _pr, _pc: self._update_device_assignments_guidance_detail()
        )
        self.device_assignments_table.cellClicked.connect(lambda _r, _c: self._update_device_assignments_guidance_detail())
        assignments_header = self.device_assignments_table.horizontalHeader()
        assignments_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        assignments_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        assignments_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        assignments_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        assignments_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        assignments_header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        assignments_header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        assignments_header.setSectionResizeMode(7, QHeaderView.Stretch)
        assignments_layout.addWidget(self.device_assignments_table)

        assignments_container = QWidget()
        assignments_container.setLayout(assignments_layout)
        assignments_group = self._make_collapsible_group("Assigned Plans", assignments_container, checked=True, fit_content=False)
        self._register_collapsible_group(assignments_group, self._summary_device_assignments)
        assignments_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(assignments_group, scope="radio")

        varac_clusters_group = QGroupBox("VarAC Clusters")
        varac_clusters_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        varac_clusters_layout = QVBoxLayout()
        varac_clusters_layout.setSpacing(6)
        varac_clusters_group.setLayout(varac_clusters_layout)
        self.varac_clusters_hint_label = QLabel()
        self.varac_clusters_hint_label.setWordWrap(True)
        varac_clusters_layout.addWidget(self.varac_clusters_hint_label)
        (
            self.varac_clusters_guidance_card,
            self.varac_clusters_guidance_title_label,
            self.varac_clusters_guidance_status_label,
        ) = _make_support_card("Focused VarAC Cluster Guidance", "varacClustersGuidanceStatus")
        varac_clusters_layout.addWidget(self.varac_clusters_guidance_card)
        varac_clusters_row = QHBoxLayout()
        self.add_varac_cluster_btn = QPushButton("Add Cluster")
        self.add_varac_cluster_btn.clicked.connect(self._add_varac_cluster)
        self.edit_varac_cluster_btn = QPushButton("Edit Selected")
        self.edit_varac_cluster_btn.clicked.connect(self._edit_varac_cluster)
        self.delete_varac_cluster_btn = QPushButton("Delete Selected")
        self.delete_varac_cluster_btn.clicked.connect(self._delete_varac_clusters)
        varac_clusters_row.addStretch()
        varac_clusters_row.addWidget(self.add_varac_cluster_btn)
        varac_clusters_row.addWidget(self.edit_varac_cluster_btn)
        varac_clusters_row.addWidget(self.delete_varac_cluster_btn)
        varac_clusters_layout.addLayout(varac_clusters_row)
        self.varac_clusters_table = QTableWidget(0, 8)
        self.varac_clusters_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Name",
                "Cluster ID",
                "Shared DB",
                "Members",
                "Gateway",
                "PTT Lock",
                "Refresh",
            ]
        )
        self.varac_clusters_table.verticalHeader().setVisible(False)
        self.varac_clusters_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.varac_clusters_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.varac_clusters_table.setSelectionMode(QTableWidget.SingleSelection)
        self.varac_clusters_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        self.varac_clusters_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.varac_clusters_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.varac_clusters_table.currentCellChanged.connect(
            lambda _r, _c, _pr, _pc: self._update_varac_cluster_guidance_detail()
        )
        self.varac_clusters_table.cellClicked.connect(lambda _r, _c: self._update_varac_cluster_guidance_detail())
        varac_clusters_header = self.varac_clusters_table.horizontalHeader()
        varac_clusters_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        varac_clusters_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        varac_clusters_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        varac_clusters_header.setSectionResizeMode(3, QHeaderView.Stretch)
        varac_clusters_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        varac_clusters_header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        varac_clusters_header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        varac_clusters_header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        varac_clusters_layout.addWidget(self.varac_clusters_table)
        varac_clusters_container = QWidget()
        varac_clusters_container.setLayout(varac_clusters_layout)
        varac_clusters_group = self._make_collapsible_group("VarAC Clusters", varac_clusters_container, checked=True, fit_content=False)
        self._register_collapsible_group(varac_clusters_group, self._summary_varac_clusters)
        varac_clusters_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.varac_clusters_section_group = varac_clusters_group

        varac_members_group = QGroupBox("VarAC Memberships")
        varac_members_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        varac_members_layout = QVBoxLayout()
        varac_members_layout.setSpacing(6)
        varac_members_group.setLayout(varac_members_layout)
        self.varac_members_hint_label = QLabel()
        self.varac_members_hint_label.setWordWrap(True)
        varac_members_layout.addWidget(self.varac_members_hint_label)
        (
            self.varac_memberships_guidance_card,
            self.varac_memberships_guidance_title_label,
            self.varac_memberships_guidance_status_label,
        ) = _make_support_card("Focused VarAC Membership Guidance", "varacMembershipsGuidanceStatus")
        varac_members_layout.addWidget(self.varac_memberships_guidance_card)
        varac_members_row = QHBoxLayout()
        self.add_varac_membership_btn = QPushButton("Add / Edit Selected")
        self.add_varac_membership_btn.clicked.connect(self._add_or_edit_varac_membership)
        self.remove_varac_membership_btn = QPushButton("Remove Selected")
        self.remove_varac_membership_btn.clicked.connect(self._remove_varac_memberships)
        varac_members_row.addStretch()
        varac_members_row.addWidget(self.add_varac_membership_btn)
        varac_members_row.addWidget(self.remove_varac_membership_btn)
        varac_members_layout.addLayout(varac_members_row)
        self.varac_members_table = QTableWidget(0, 8)
        self.varac_members_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Cluster",
                "Device",
                "Runtime",
                "Class",
                "Instance",
                "Enabled",
                "Gateway",
            ]
        )
        self.varac_members_table.verticalHeader().setVisible(False)
        self.varac_members_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.varac_members_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.varac_members_table.setSelectionMode(QTableWidget.SingleSelection)
        self.varac_members_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        self.varac_members_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.varac_members_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.varac_members_table.currentCellChanged.connect(
            lambda _r, _c, _pr, _pc: self._update_varac_membership_guidance_detail()
        )
        self.varac_members_table.cellClicked.connect(lambda _r, _c: self._update_varac_membership_guidance_detail())
        varac_members_header = self.varac_members_table.horizontalHeader()
        varac_members_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        varac_members_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        varac_members_header.setSectionResizeMode(2, QHeaderView.Stretch)
        varac_members_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        varac_members_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        varac_members_header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        varac_members_header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        varac_members_header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        varac_members_layout.addWidget(self.varac_members_table)
        varac_members_container = QWidget()
        varac_members_container.setLayout(varac_members_layout)
        varac_members_group = self._make_collapsible_group("VarAC Memberships", varac_members_container, checked=True, fit_content=False)
        self._register_collapsible_group(varac_members_group, self._summary_varac_memberships)
        varac_members_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.varac_memberships_section_group = varac_members_group

        logging_container = QWidget()
        logging_container_layout = QVBoxLayout()
        logging_container_layout.setContentsMargins(0, 0, 0, 0)
        logging_container_layout.setSpacing(0)
        logging_container_layout.addWidget(self.logging_group)
        logging_container_layout.addStretch()
        logging_container.setLayout(logging_container_layout)
        logging_section = self._make_collapsible_group(
            "Logging & Diagnostics",
            logging_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.logging",
        )
        self._register_collapsible_group(logging_section, self._summary_logging_settings)
        logging_section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        # HF Operating Groups panel
        ops_group = QGroupBox("HF Operating Groups")
        ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        ops_layout = QVBoxLayout()
        ops_layout.setSpacing(6)
        ops_group.setLayout(ops_layout)
        add_row = QHBoxLayout()
        self.add_group_btn = QPushButton("Add Group")
        self.add_group_btn.clicked.connect(self._add_operating_group)
        self.edit_group_btn = QPushButton("Edit Selected")
        self.edit_group_btn.clicked.connect(self._edit_operating_group)
        self.delete_group_btn = QPushButton("Delete Selected")
        self.delete_group_btn.clicked.connect(self._delete_operating_groups)
        add_row.addStretch()
        add_row.addWidget(self.add_group_btn)
        add_row.addWidget(self.edit_group_btn)
        add_row.addWidget(self.delete_group_btn)
        ops_layout.addLayout(add_row)
        cond_scope_hint = QLabel(
            "Condition Levels are group-scoped: changing one row applies to all rows for that Group."
        )
        cond_scope_hint.setWordWrap(True)
        ops_layout.addWidget(cond_scope_hint)
        self.op_groups_table = QTableWidget(0, 10)
        self.op_groups_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Group",
                "Mode",
                "Band",
                "Freq (MHz)",
                "VFO",
                "FLDigi Starting Mode",
                "FLDigi Offset",
                "Auto-Tune",
                "Use Condition Levels",
            ]
        )
        header = self.op_groups_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.Interactive)
        header.setSectionResizeMode(7, QHeaderView.Interactive)
        header.setSectionResizeMode(8, QHeaderView.Fixed)
        header.setSectionResizeMode(9, QHeaderView.Fixed)
        header.setStretchLastSection(False)
        header.setMinimumSectionSize(50)
        self.op_groups_table.setColumnWidth(6, 185)
        self.op_groups_table.setColumnWidth(7, 130)
        self.op_groups_table.setColumnWidth(8, 110)
        self.op_groups_table.setColumnWidth(9, 180)
        self.op_groups_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.op_groups_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.op_groups_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.op_groups_table.setEditTriggers(QTableWidget.NoEditTriggers)
        ops_layout.addWidget(self.op_groups_table)
        ops_container = QWidget()
        ops_container.setLayout(ops_layout)
        ops_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        ops_group = self._make_collapsible_group(
            "HF Operating Groups",
            ops_container,
            checked=True,
            fit_content=True,
            fit_content_in_stack=True,
            help_context_key="settings.hf-groups",
        )
        self._register_collapsible_group(ops_group, self._summary_operating_groups)
        self._set_section_health_key(ops_group, "operating_groups")
        ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.op_groups_section_group = ops_group
        self._add_settings_section(ops_group, scope="global")

        # Local Comms Groups panel (non-scheduler local net metadata for SOP workflows)
        local_group = QGroupBox("Local Comms Groups")
        local_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        local_layout = QVBoxLayout()
        local_layout.setSpacing(6)
        local_group.setLayout(local_layout)
        local_hint = QLabel(
            "Used by SOP local-net reminders only. A Group can contain multiple Resources and Modes. "
            "Not used by scheduler automation."
        )
        local_hint.setWordWrap(True)
        local_layout.addWidget(local_hint)
        local_row = QHBoxLayout()
        self.add_local_net_btn = QPushButton("Add Profile")
        self.add_local_net_btn.clicked.connect(self._add_local_net_profile)
        self.edit_local_net_btn = QPushButton("Edit Selected")
        self.edit_local_net_btn.clicked.connect(self._edit_local_net_profile)
        self.delete_local_net_btn = QPushButton("Delete Selected")
        self.delete_local_net_btn.clicked.connect(self._delete_local_net_profiles)
        local_row.addStretch()
        local_row.addWidget(self.add_local_net_btn)
        local_row.addWidget(self.edit_local_net_btn)
        local_row.addWidget(self.delete_local_net_btn)
        local_layout.addLayout(local_row)
        self.local_net_table = QTableWidget(0, 6)
        self.local_net_table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Group",
                "Resource",
                "Mode",
                "Target",
                "Notes",
            ]
        )
        local_header = self.local_net_table.horizontalHeader()
        local_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        local_header.setSectionResizeMode(5, QHeaderView.Stretch)
        self.local_net_table.verticalHeader().setVisible(False)
        self.local_net_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.local_net_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.local_net_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.local_net_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        local_layout.addWidget(self.local_net_table)
        local_container = QWidget()
        local_container.setLayout(local_layout)
        local_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        local_group = self._make_collapsible_group(
            "Local Comms Groups",
            local_container,
            checked=True,
            fit_content=True,
            fit_content_in_stack=True,
            help_context_key="settings.local-comms",
        )
        self._register_collapsible_group(local_group, self._summary_local_net_profiles)
        local_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.local_net_section_group = local_group
        self._add_settings_section(local_group, scope="global")

        software_scope_group = QGroupBox("Radio Software View")
        software_scope_layout = QVBoxLayout()
        software_scope_layout.setSpacing(6)
        software_scope_group.setLayout(software_scope_layout)

        self.software_scope_hint_label = QLabel(
            "These software pages stay in the familiar single-rig layout, but they now edit the selected radio profile instead of one global station shell."
        )
        self.software_scope_hint_label.setWordWrap(True)
        software_scope_layout.addWidget(self.software_scope_hint_label)

        self.software_scope_card = QFrame()
        self.software_scope_card.setFrameShape(QFrame.StyledPanel)
        software_scope_card_layout = QVBoxLayout(self.software_scope_card)
        software_scope_card_layout.setContentsMargins(12, 10, 12, 10)
        software_scope_card_layout.setSpacing(6)
        self.software_scope_title_label = QLabel("Selected Radio Software Bundle")
        software_scope_title_font = self.software_scope_title_label.font()
        software_scope_title_font.setBold(True)
        self.software_scope_title_label.setFont(software_scope_title_font)
        software_scope_card_layout.addWidget(self.software_scope_title_label)
        self.software_scope_status_label = QLabel(
            "Select a radio profile to view radio-scoped JS8Call, Fast Light, and VarAC settings."
        )
        self.software_scope_status_label.setWordWrap(True)
        software_scope_card_layout.addWidget(self.software_scope_status_label)
        software_scope_layout.addWidget(self.software_scope_card)

        software_scope_row = QHBoxLayout()
        software_scope_row.setSpacing(8)
        software_scope_row.addWidget(QLabel("Radio:"))
        self.software_radio_combo = QComboBox()
        self.software_radio_combo.setMinimumContentsLength(28)
        self.software_radio_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.software_radio_combo.currentIndexChanged.connect(self._on_software_radio_changed)
        software_scope_row.addWidget(self.software_radio_combo, 1)
        software_scope_row.addStretch()
        software_scope_layout.addLayout(software_scope_row)

        software_scope_container = QWidget()
        software_scope_container.setLayout(software_scope_layout)
        software_scope_group = self._make_collapsible_group(
            "Radio Software View",
            software_scope_container,
            checked=True,
            fit_content=True,
        )
        self._register_collapsible_group(software_scope_group, self._summary_radio_software_view)
        self._set_section_health_key(software_scope_group, "radio_software")
        software_scope_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.radio_software_scope_section_group = software_scope_group

        # JS8Call status/settings
        js8_group = QGroupBox("JS8Call Settings")
        js8_v = QVBoxLayout()
        js8_v.setSpacing(6)
        js8_v.setAlignment(Qt.AlignTop)
        js8_group.setLayout(js8_v)
        js8_label_width = 170

        self.js8_scope_label = QLabel("Editing Radio: --")
        self.js8_scope_label.setWordWrap(True)
        js8_v.addWidget(self.js8_scope_label)

        js8_host_row = QHBoxLayout()
        js8_host_row.setSpacing(8)
        js8_host_row.setContentsMargins(0, 0, 0, 0)
        js8_host_label = QLabel("TCP Host")
        js8_host_label.setFixedWidth(70)
        js8_host_row.addWidget(js8_host_label)
        self.js8_host_edit = QLineEdit()
        self.js8_host_edit.setPlaceholderText("127.0.0.1")
        self.js8_host_edit.setText("127.0.0.1")
        self.js8_host_edit.setFixedWidth(220)
        js8_host_row.addWidget(self.js8_host_edit)
        js8_host_row.addStretch()
        js8_v.addLayout(js8_host_row)

        js8_port_row = QHBoxLayout()
        js8_port_row.setSpacing(8)
        js8_port_row.setContentsMargins(0, 0, 0, 0)
        js8_port_label = QLabel("TCP Port")
        js8_port_label.setFixedWidth(70)
        js8_port_row.addWidget(js8_port_label)
        self.js8_port_edit = QLineEdit()
        self.js8_port_edit.setFixedWidth(80)
        self.js8_port_edit.setText("2442")
        js8_port_row.addWidget(self.js8_port_edit)
        js8_port_row.addSpacing(8)
        js8_offset_label = QLabel("Offset (Hz)")
        js8_offset_label.setFixedWidth(78)
        js8_port_row.addWidget(js8_offset_label)
        self.js8_offset_edit = QLineEdit()
        self.js8_offset_edit.setFixedWidth(80)
        self.js8_offset_edit.setText(str(coerce_js8_offset_hz(0)))
        js8_port_row.addWidget(self.js8_offset_edit)
        js8_port_row.addStretch()
        js8_v.addLayout(js8_port_row)

        js8_mark_row = QHBoxLayout()
        js8_mark_row.setSpacing(8)
        js8_mark_row.setContentsMargins(0, 0, 0, 0)
        js8_mark_label = QLabel("Mark JS8Call MSG Read?")
        js8_mark_label.setFixedWidth(js8_label_width)
        js8_mark_row.addWidget(js8_mark_label)
        self.js8_mark_retrieved_chk = QCheckBox()
        self.js8_mark_retrieved_chk.setToolTip(
            "When enabled, clicking 'Mark Retrieved' in Message Viewer will set JS8Call inbox entries to READ."
        )
        js8_mark_row.addWidget(self.js8_mark_retrieved_chk)
        js8_mark_row.addStretch()
        js8_v.addLayout(js8_mark_row)

        load_links_row = QHBoxLayout()
        load_links_row.setSpacing(8)
        load_links_row.setContentsMargins(0, 0, 0, 0)
        load_links_label = QLabel("Tools")
        load_links_label.setFixedWidth(js8_label_width)
        load_links_row.addWidget(load_links_label)
        self.load_js8_btn = QPushButton("Load JS8 Traffic")
        self.load_js8_btn.clicked.connect(self._load_js8_logs)
        load_links_row.addWidget(self.load_js8_btn)
        self.load_js8_progress = QProgressBar()
        self.load_js8_progress.setRange(0, 0)
        self.load_js8_progress.setTextVisible(False)
        self.load_js8_progress.setFixedWidth(120)
        self.load_js8_progress.setFixedHeight(12)
        self.load_js8_progress.setVisible(False)
        load_links_row.addWidget(self.load_js8_progress)
        self.load_js8_status_label = QLabel("Loading JS8 traffic...")
        self.load_js8_status_label.setVisible(False)
        load_links_row.addWidget(self.load_js8_status_label)
        load_links_row.addStretch()
        js8_v.addLayout(load_links_row)

        def build_js8_path_row(label: str, edit: QLineEdit, browse_cb, autofill_btn: QPushButton | None = None) -> QWidget:
            row = QHBoxLayout()
            row.setSpacing(8)
            row.setContentsMargins(0, 0, 0, 0)
            lbl = QLabel(label)
            lbl.setFixedWidth(js8_label_width)
            row.addWidget(lbl)
            row.addWidget(edit, 1)
            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(browse_cb)
            row.addWidget(browse_btn)
            if autofill_btn is not None:
                row.addWidget(autofill_btn)
            w = QWidget()
            w.setLayout(row)
            w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            return w

        self.js8call_path_edit = QLineEdit()
        self.js8call_path_edit.setPlaceholderText("Folder containing JS8Call")
        js8call_autofill_btn = self._make_contextual_autofill_button(
            "js8call_core",
            "Auto-Fill",
            "js8",
            ["path_js8call", "js8_directed_path"],
            base_edit=self.js8call_path_edit,
            tooltip="Use the JS8Call install folder to find related JS8Call paths for the selected radio.",
        )
        js8_v.addWidget(
            build_js8_path_row(
                "JS8Call Install Folder:",
                self.js8call_path_edit,
                self._choose_js8call_install_path,
                js8call_autofill_btn,
            )
        )

        self.js8_directed_edit = QLineEdit()
        js8_directed_autofill_btn = self._make_contextual_autofill_button(
            "js8_directed_log",
            "Auto-Fill",
            "js8",
            ["js8_directed_path"],
            tooltip="Find JS8Call DIRECTED.TXT from JS8Call settings and profile save folders for the selected radio.",
        )
        js8_v.addWidget(
            build_js8_path_row(
                "JS8Call DIRECTED.TXT:",
                self.js8_directed_edit,
                self._choose_js8_directed_path,
                js8_directed_autofill_btn,
            )
        )

        self.commstat_path_edit = QLineEdit()
        self.commstat_path_edit.setPlaceholderText("Select your CommStat launcher/script/shortcut")
        self.commstat_path_edit.setToolTip(
            "CommStat launchers are often stored in custom locations. Use Browse to select the launcher, "
            "script, or shortcut used for the selected radio."
        )
        js8_v.addWidget(
            build_js8_path_row("CommStat Launch Path:", self.commstat_path_edit, self._choose_commstat_launch_path)
        )

        self.js8spotter_path_edit = QLineEdit()
        self.js8spotter_path_edit.setPlaceholderText("Select your JS8Spotter launcher/script/shortcut")
        self.js8spotter_path_edit.setToolTip(
            "JS8Spotter launchers are often stored in custom locations. Use Browse to select the launcher, "
            "script, or shortcut used for the selected radio."
        )
        js8_v.addWidget(
            build_js8_path_row(
                "JS8Spotter Launch Path:",
                self.js8spotter_path_edit,
                self._choose_js8spotter_launch_path,
            )
        )

        self.js8_forms_edit = QLineEdit()
        self.js8_forms_edit.setPlaceholderText("Select your JS8Spotter forms folder")
        self.js8_forms_edit.setToolTip(
            "JS8Spotter is commonly installed in custom locations. Use Browse to select the forms folder "
            "used for the selected radio."
        )
        js8_v.addWidget(build_js8_path_row("JS8Spotter forms:", self.js8_forms_edit, self._choose_js8_forms_path))

        mapper_header = QHBoxLayout()
        mapper_header.setContentsMargins(0, 0, 0, 0)
        mapper_header.setSpacing(8)
        mapper_header.addWidget(QLabel("Spotter Form Mapper"))
        mapper_header.addStretch()
        self.spotter_mapper_refresh_btn = QPushButton("Refresh Forms")
        self.spotter_mapper_auto_btn = QPushButton("Auto-Classify")
        mapper_header.addWidget(self.spotter_mapper_refresh_btn)
        mapper_header.addWidget(self.spotter_mapper_auto_btn)
        js8_v.addLayout(mapper_header)
        self.spotter_mapper_table = QTableWidget(0, 8)
        self.spotter_mapper_table.setHorizontalHeaderLabels(
            ["Form", "Title", "Purpose", "Messages", "Map", "Alert", "Net", "Status"]
        )
        self.spotter_mapper_table.verticalHeader().setVisible(False)
        self.spotter_mapper_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.spotter_mapper_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.spotter_mapper_table.setAlternatingRowColors(True)
        self.spotter_mapper_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        self.spotter_mapper_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.spotter_mapper_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.spotter_mapper_table.setWordWrap(False)
        mapper_header_view = self.spotter_mapper_table.horizontalHeader()
        mapper_header_view.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        mapper_header_view.setSectionResizeMode(1, QHeaderView.Stretch)
        mapper_header_view.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        for col in range(3, 8):
            mapper_header_view.setSectionResizeMode(col, QHeaderView.ResizeToContents)
        self.spotter_mapper_table.itemChanged.connect(self._on_spotter_mapper_item_changed)
        js8_v.addWidget(self.spotter_mapper_table)
        mapper_hint = QLabel(
            "Map each JS8Spotter form to its operational purpose so FIO can route it to Messages, Map, Alerts, Net Control, or status workflows."
        )
        mapper_hint.setWordWrap(True)
        js8_v.addWidget(mapper_hint)

        self.js8_directed_edit.textChanged.connect(self._refresh_section_titles)
        self.js8_forms_edit.textChanged.connect(self._refresh_section_titles)
        self.js8_forms_edit.textChanged.connect(lambda _text: self._refresh_spotter_form_mapper())
        self.spotter_mapper_refresh_btn.clicked.connect(self._refresh_spotter_form_mapper)
        self.spotter_mapper_auto_btn.clicked.connect(self._auto_classify_spotter_forms)
        self.js8call_path_edit.textChanged.connect(self._refresh_section_titles)
        self.js8spotter_path_edit.textChanged.connect(self._refresh_section_titles)
        self.commstat_path_edit.textChanged.connect(self._refresh_section_titles)
        self.js8call_path_edit.textChanged.connect(self._on_launch_paths_changed)
        self.js8spotter_path_edit.textChanged.connect(self._on_launch_paths_changed)
        self.commstat_path_edit.textChanged.connect(self._on_launch_paths_changed)

        js8_autofill_status_row = QHBoxLayout()
        js8_autofill_status_row.setSpacing(8)
        js8_autofill_status_row.setContentsMargins(0, 0, 0, 0)
        js8_autofill_status_row.addSpacing(js8_label_width)
        self.js8_autofill_status_label = QLabel("No auto-fill attempt yet.")
        self.js8_autofill_status_label.setWordWrap(True)
        self.js8_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self._autofill_status_labels["js8"] = self.js8_autofill_status_label
        js8_autofill_status_row.addWidget(self.js8_autofill_status_label, 1)
        self.js8_autofill_review_toggle_btn = QPushButton("Show Full Review")
        self.js8_autofill_review_toggle_btn.setVisible(False)
        self.js8_autofill_review_toggle_btn.clicked.connect(lambda _checked=False: self._toggle_autofill_review("js8"))
        self._autofill_status_buttons["js8"] = self.js8_autofill_review_toggle_btn
        js8_autofill_status_row.addWidget(self.js8_autofill_review_toggle_btn)
        js8_autofill_actions_widget = QWidget()
        js8_autofill_actions_row = QHBoxLayout()
        js8_autofill_actions_widget.setLayout(js8_autofill_actions_row)
        js8_autofill_actions_widget.setVisible(False)
        self._autofill_action_rows["js8"] = js8_autofill_actions_widget
        js8_autofill_actions_row.setSpacing(8)
        js8_autofill_actions_row.setContentsMargins(0, 0, 0, 0)
        js8_autofill_actions_row.addSpacing(js8_label_width)
        js8_autofill_actions_row.addStretch()
        self.js8_autofill_preserved_btn = QPushButton("Copy Suggestions")
        self.js8_autofill_preserved_btn.setVisible(False)
        self.js8_autofill_preserved_btn.clicked.connect(
            lambda _checked=False: self._copy_autofill_preserved_suggestions("js8")
        )
        self._autofill_preserved_buttons["js8"] = self.js8_autofill_preserved_btn
        js8_autofill_actions_row.addWidget(self.js8_autofill_preserved_btn)
        self.js8_autofill_replace_btn = QPushButton("Replace Suggested")
        self.js8_autofill_replace_btn.setVisible(False)
        self.js8_autofill_replace_btn.clicked.connect(
            lambda _checked=False: self._replace_autofill_preserved_suggestions("js8")
        )
        self._autofill_replace_buttons["js8"] = self.js8_autofill_replace_btn
        js8_autofill_actions_row.addWidget(self.js8_autofill_replace_btn)
        self.js8_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")
        self.js8_autofill_dismiss_btn.setVisible(False)
        self.js8_autofill_dismiss_btn.clicked.connect(
            lambda _checked=False: self._dismiss_autofill_preserved_suggestions("js8")
        )
        self._autofill_dismiss_buttons["js8"] = self.js8_autofill_dismiss_btn
        js8_autofill_actions_row.addWidget(self.js8_autofill_dismiss_btn)
        js8_v.addLayout(js8_autofill_status_row)
        js8_v.addWidget(js8_autofill_actions_widget)
        js8_v.addWidget(self._make_autofill_review_table("js8"))

        js8_container = QWidget()
        js8_container.setLayout(js8_v)
        js8_group = self._make_collapsible_group(
            "JS8Call Settings",
            js8_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.js8call",
        )
        self._register_collapsible_group(js8_group, self._summary_js8_settings)
        self._set_section_health_key(js8_group, "js8call")
        js8_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.js8_section_group = js8_group
        self._add_settings_section(js8_group, scope="radio")

        msg_label_width = 170

        def build_prog_row(
            name: str,
            label: str | None = None,
            autofill: tuple[str, str, str, List[str]] | None = None,
        ) -> QWidget:
            row = QHBoxLayout()
            row.setSpacing(8)
            row.setContentsMargins(0, 0, 0, 0)
            lbl = QLabel(label or name)
            lbl.setFixedWidth(msg_label_width)
            row.addWidget(lbl)

            path_edit = QLineEdit()
            path_edit.setPlaceholderText("Path to executable")
            self.path_edits[name] = path_edit
            path_edit.textChanged.connect(self._refresh_section_titles)
            path_edit.textChanged.connect(self._on_launch_paths_changed)
            row.addWidget(path_edit, 1)

            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(lambda _, n=name: self._choose_program_path(n))
            row.addWidget(browse_btn)
            if autofill is not None:
                rule_id, text, section, keys = autofill
                row.addWidget(
                    self._make_contextual_autofill_button(
                        rule_id,
                        text,
                        section,
                        keys,
                        base_edit=path_edit if len(keys) > 1 else None,
                    )
                )
            w = QWidget()
            w.setLayout(row)
            w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            return w

        def build_msg_row(label: str, edit: QLineEdit, browse_cb) -> QWidget:
            row = QHBoxLayout()
            row.setSpacing(8)
            row.setContentsMargins(0, 0, 0, 0)
            lbl = QLabel(label)
            lbl.setFixedWidth(msg_label_width)
            row.addWidget(lbl)
            row.addWidget(edit, 1)
            browse_btn = QPushButton("Browse")
            browse_btn.setFixedWidth(70)
            browse_btn.clicked.connect(browse_cb)
            row.addWidget(browse_btn)
            w = QWidget()
            w.setLayout(row)
            w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            return w

        # Message path edits
        self.msg_paths_edits = {}
        flmsg_edit = QLineEdit()
        self.msg_paths_edits["flmsg"] = flmsg_edit
        flamp_edit = QLineEdit()
        self.msg_paths_edits["flamp"] = flamp_edit
        varac_edit = QLineEdit()
        self.msg_paths_edits["varac"] = varac_edit

        # Fast Light Settings
        fast_light_group = QGroupBox("Fast Light Settings")
        fast_light_v = QVBoxLayout()
        fast_light_v.setSpacing(6)
        fast_light_v.setAlignment(Qt.AlignTop)
        fast_light_group.setLayout(fast_light_v)
        self.fast_light_scope_label = QLabel("Editing Radio: --")
        self.fast_light_scope_label.setWordWrap(True)
        fast_light_v.addWidget(self.fast_light_scope_label)
        fast_light_v.addWidget(build_prog_row("FLRig", "FLRig", ("flrig_launch", "Find", "fast_light", ["path_flrig"])))

        flrig_port_row = QHBoxLayout()
        flrig_port_row.setContentsMargins(0, 0, 0, 0)
        flrig_port_row.setSpacing(8)
        flrig_port_label = QLabel("FLRig XMLRPC Port")
        flrig_port_label.setFixedWidth(msg_label_width)
        flrig_port_row.addWidget(flrig_port_label)
        self.flrig_port_edit = QLineEdit()
        self.flrig_port_edit.setFixedWidth(80)
        self.flrig_port_edit.setText("12345")
        flrig_port_row.addWidget(self.flrig_port_edit)
        flrig_port_row.addStretch()
        flrig_port_spacer = QWidget()
        flrig_port_spacer.setFixedWidth(70)
        flrig_port_row.addWidget(flrig_port_spacer)
        fast_light_v.addLayout(flrig_port_row)

        fast_light_v.addWidget(
            build_prog_row("FLDigi", "FLDigi", ("fldigi_core", "Auto-Fill", "fast_light", ["path_fldigi", "fldigi_log_path"]))
        )
        fldigi_host_row = QHBoxLayout()
        fldigi_host_row.setContentsMargins(0, 0, 0, 0)
        fldigi_host_row.setSpacing(8)
        fldigi_host_label = QLabel("FLDigi XMLRPC Host")
        fldigi_host_label.setFixedWidth(msg_label_width)
        fldigi_host_row.addWidget(fldigi_host_label)
        self.fldigi_host_edit = QLineEdit()
        self.fldigi_host_edit.setPlaceholderText("127.0.0.1")
        self.fldigi_host_edit.setText("127.0.0.1")
        self.fldigi_host_edit.setFixedWidth(220)
        fldigi_host_row.addWidget(self.fldigi_host_edit)
        fldigi_host_row.addStretch()
        fldigi_host_spacer = QWidget()
        fldigi_host_spacer.setFixedWidth(70)
        fldigi_host_row.addWidget(fldigi_host_spacer)
        fast_light_v.addLayout(fldigi_host_row)

        fldigi_port_row = QHBoxLayout()
        fldigi_port_row.setContentsMargins(0, 0, 0, 0)
        fldigi_port_row.setSpacing(8)
        fldigi_port_label = QLabel("FLDigi XMLRPC Port")
        fldigi_port_label.setFixedWidth(msg_label_width)
        fldigi_port_row.addWidget(fldigi_port_label)
        self.fldigi_port_edit = QLineEdit()
        self.fldigi_port_edit.setFixedWidth(80)
        self.fldigi_port_edit.setText("7362")
        fldigi_port_row.addWidget(self.fldigi_port_edit)
        fldigi_port_row.addStretch()
        fldigi_port_spacer = QWidget()
        fldigi_port_spacer.setFixedWidth(70)
        fldigi_port_row.addWidget(fldigi_port_spacer)
        fast_light_v.addLayout(fldigi_port_row)

        self.fldigi_checkin_dir_edit = QLineEdit()
        self.fldigi_checkin_dir_edit.setPlaceholderText("Directory containing check-in files")
        self.fldigi_checkin_dir_edit.textChanged.connect(self._refresh_fldigi_checkin_file_labels)
        self.fldigi_main_file_edit = QLineEdit()
        self.fldigi_main_file_edit.setReadOnly(True)
        self.fldigi_main_file_edit.hide()
        self.fldigi_late_file_edit = QLineEdit()
        self.fldigi_late_file_edit.setReadOnly(True)
        self.fldigi_late_file_edit.hide()
        fast_light_v.addWidget(
            build_msg_row("Check-in File Path", self.fldigi_checkin_dir_edit, self._choose_fldigi_checkin_dir)
        )

        self.fldigi_log_path_edit = QLineEdit()
        self.fldigi_log_path_edit.setPlaceholderText("FLDigi log folder")
        fast_light_v.addWidget(
            build_msg_row("FLDigi Log Path", self.fldigi_log_path_edit, self._choose_fldigi_log_path)
        )

        fast_light_v.addWidget(
            build_prog_row("FLMsg", "FLMsg", ("flmsg_core", "Auto-Fill", "fast_light", ["path_flmsg", "message_paths.flmsg"]))
        )
        fast_light_v.addWidget(
            build_msg_row(
                "ICS/Messages",
                flmsg_edit,
                lambda: self._choose_msg_path("flmsg", flmsg_edit),
            )
        )

        fast_light_v.addWidget(
            build_prog_row("FLAmp", "FLAmp", ("flamp_core", "Auto-Fill", "fast_light", ["path_flamp", "message_paths.flamp"]))
        )
        fast_light_v.addWidget(
            build_msg_row(
                "FLAMP/rx",
                flamp_edit,
                lambda: self._choose_msg_path("flamp", flamp_edit),
            )
        )

        fast_light_autofill_status_row = QHBoxLayout()
        fast_light_autofill_status_row.setContentsMargins(0, 0, 0, 0)
        fast_light_autofill_status_row.setSpacing(8)
        fast_light_autofill_status_row.addSpacing(msg_label_width)
        self.fast_light_autofill_status_label = QLabel("No auto-fill attempt yet.")
        self.fast_light_autofill_status_label.setWordWrap(True)
        self.fast_light_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self._autofill_status_labels["fast_light"] = self.fast_light_autofill_status_label
        fast_light_autofill_status_row.addWidget(self.fast_light_autofill_status_label, 1)
        self.fast_light_autofill_review_toggle_btn = QPushButton("Show Full Review")
        self.fast_light_autofill_review_toggle_btn.setVisible(False)
        self.fast_light_autofill_review_toggle_btn.clicked.connect(
            lambda _checked=False: self._toggle_autofill_review("fast_light")
        )
        self._autofill_status_buttons["fast_light"] = self.fast_light_autofill_review_toggle_btn
        fast_light_autofill_status_row.addWidget(self.fast_light_autofill_review_toggle_btn)
        fast_light_autofill_actions_widget = QWidget()
        fast_light_autofill_actions_row = QHBoxLayout()
        fast_light_autofill_actions_widget.setLayout(fast_light_autofill_actions_row)
        fast_light_autofill_actions_widget.setVisible(False)
        self._autofill_action_rows["fast_light"] = fast_light_autofill_actions_widget
        fast_light_autofill_actions_row.setContentsMargins(0, 0, 0, 0)
        fast_light_autofill_actions_row.setSpacing(8)
        fast_light_autofill_actions_row.addSpacing(msg_label_width)
        fast_light_autofill_actions_row.addStretch()
        self.fast_light_autofill_preserved_btn = QPushButton("Copy Suggestions")
        self.fast_light_autofill_preserved_btn.setVisible(False)
        self.fast_light_autofill_preserved_btn.clicked.connect(
            lambda _checked=False: self._copy_autofill_preserved_suggestions("fast_light")
        )
        self._autofill_preserved_buttons["fast_light"] = self.fast_light_autofill_preserved_btn
        fast_light_autofill_actions_row.addWidget(self.fast_light_autofill_preserved_btn)
        self.fast_light_autofill_replace_btn = QPushButton("Replace Suggested")
        self.fast_light_autofill_replace_btn.setVisible(False)
        self.fast_light_autofill_replace_btn.clicked.connect(
            lambda _checked=False: self._replace_autofill_preserved_suggestions("fast_light")
        )
        self._autofill_replace_buttons["fast_light"] = self.fast_light_autofill_replace_btn
        fast_light_autofill_actions_row.addWidget(self.fast_light_autofill_replace_btn)
        self.fast_light_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")
        self.fast_light_autofill_dismiss_btn.setVisible(False)
        self.fast_light_autofill_dismiss_btn.clicked.connect(
            lambda _checked=False: self._dismiss_autofill_preserved_suggestions("fast_light")
        )
        self._autofill_dismiss_buttons["fast_light"] = self.fast_light_autofill_dismiss_btn
        fast_light_autofill_actions_row.addWidget(self.fast_light_autofill_dismiss_btn)
        fast_light_v.addLayout(fast_light_autofill_status_row)
        fast_light_v.addWidget(fast_light_autofill_actions_widget)
        fast_light_v.addWidget(self._make_autofill_review_table("fast_light"))

        # Check-in log file copy helpers
        launch_row = QHBoxLayout()
        launch_row.setContentsMargins(0, 0, 0, 0)
        launch_row.setSpacing(8)
        launch_label = QLabel("Check-in Log Paths")
        launch_label.setFixedWidth(msg_label_width)
        launch_row.addWidget(launch_label)
        self.copy_main_btn = QPushButton("Copy Main")
        self.copy_main_btn.clicked.connect(lambda: self._copy_text(self.fldigi_main_file_edit))
        self.copy_late_btn = QPushButton("Copy New/Late")
        self.copy_late_btn.clicked.connect(lambda: self._copy_text(self.fldigi_late_file_edit))
        launch_row.addWidget(self.copy_main_btn)
        launch_row.addWidget(self.copy_late_btn)
        launch_row.addStretch()
        fast_light_v.addLayout(launch_row)

        fast_light_container = QWidget()
        fast_light_container.setLayout(fast_light_v)
        fast_light_group = self._make_collapsible_group(
            "Fast Light Settings",
            fast_light_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.fast-light",
        )
        self._register_collapsible_group(fast_light_group, self._summary_fast_light_settings)
        self._set_section_health_key(fast_light_group, "fast_light")
        fast_light_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.fast_light_section_group = fast_light_group
        self._add_settings_section(fast_light_group, scope="radio")

        # Message Authenticity (Key/Hash)
        gpg_group = QGroupBox("Message Auth (Key/Hash)")
        gpg_v = QVBoxLayout()
        gpg_v.setContentsMargins(0, 0, 0, 0)
        gpg_v.setSpacing(6)
        gpg_v.setAlignment(Qt.AlignTop)
        gpg_group.setLayout(gpg_v)

        def _make_message_auth_subsection(title: str, content: QWidget, *, checked: bool = True) -> QFrame:
            section = QFrame()
            section.setFrameShape(QFrame.StyledPanel)
            section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            section_layout = QVBoxLayout(section)
            section_layout.setContentsMargins(8, 6, 8, 8)
            section_layout.setSpacing(6)

            header_btn = QToolButton()
            header_btn.setCheckable(True)
            header_btn.setChecked(checked)
            header_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
            header_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
            header_btn.setText(title)
            header_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            header_btn.setMinimumHeight(26)
            header_btn.setStyleSheet(self._section_header_style("neutral", resolve_theme(self.settings)))
            content.setVisible(checked)

            def _toggle(opened: bool, *, body: QWidget = content, button: QToolButton = header_btn) -> None:
                body.setVisible(opened)
                button.setArrowType(Qt.DownArrow if opened else Qt.RightArrow)
                section.updateGeometry()
                self._sync_current_section_scroll_size()

            header_btn.toggled.connect(_toggle)
            section_layout.addWidget(header_btn)
            section_layout.addWidget(content)
            return section

        gpg_overview_tab = QWidget()
        gpg_overview_v = QVBoxLayout(gpg_overview_tab)
        gpg_overview_v.setContentsMargins(8, 8, 8, 8)
        gpg_overview_v.setSpacing(8)
        gpg_overview_v.setAlignment(Qt.AlignTop)

        self.gpg_verify_enabled_chk = QCheckBox("Verify signed .k2s/.b2s message files and signature sidecars")
        self.gpg_verify_enabled_chk.setToolTip(
            "When enabled, Message Viewer verifies detached sidecars and embedded clearsigned content "
            "for FLAmp, VarAC, and BBS .k2s/.b2s files, canonical '-sig' files, and .sig/.asc/.gpg sidecars."
        )
        gpg_overview_v.addWidget(self.gpg_verify_enabled_chk)

        self.hash_verify_enabled_chk = QCheckBox(
            "Verify .k2s/.b2s checksum sidecars (SHA-256/SHA-512 preferred)"
        )
        self.hash_verify_enabled_chk.setToolTip(
            "When enabled, Message Viewer checks checksum sidecar files for tamper/corruption detection."
        )
        gpg_overview_v.addWidget(self.hash_verify_enabled_chk)
        overview_note = QLabel(
            "Use this section to verify received message files, manage trusted hashes, review trusted GPG keys, "
            "and choose the signing identity used for FLAmp compose."
        )
        overview_note.setWordWrap(True)
        gpg_overview_v.addWidget(overview_note)
        gpg_v.addWidget(_make_message_auth_subsection("Overview", gpg_overview_tab, checked=True))

        trusted_hash_tab = QWidget()
        trusted_hash_v = QVBoxLayout(trusted_hash_tab)
        trusted_hash_v.setContentsMargins(8, 8, 8, 8)
        trusted_hash_v.setSpacing(8)
        trusted_hash_v.setAlignment(Qt.AlignTop)

        trusted_hash_row = QHBoxLayout()
        trusted_hash_row.setContentsMargins(0, 0, 0, 0)
        trusted_hash_row.setSpacing(8)
        trusted_hash_label = QLabel("Trusted Hash")
        trusted_hash_label.setFixedWidth(msg_label_width)
        trusted_hash_row.addWidget(trusted_hash_label)
        self.trusted_hash_edit = QLineEdit()
        self.trusted_hash_edit.setPlaceholderText("Paste hash (SHA-1/SHA-256/SHA-512/MD5)")
        trusted_hash_row.addWidget(self.trusted_hash_edit, 1)
        self.trusted_hash_algo_combo = QComboBox()
        self.trusted_hash_algo_combo.addItems(["Auto", "SHA-1", "SHA-256", "SHA-512", "MD5"])
        self.trusted_hash_algo_combo.setFixedWidth(110)
        trusted_hash_row.addWidget(self.trusted_hash_algo_combo)
        self.trusted_hash_label_edit = QLineEdit()
        self.trusted_hash_label_edit.setPlaceholderText("Label (optional)")
        self.trusted_hash_label_edit.setFixedWidth(180)
        trusted_hash_row.addWidget(self.trusted_hash_label_edit)
        self.trusted_hash_add_btn = QPushButton("Add")
        self.trusted_hash_add_btn.setFixedWidth(70)
        trusted_hash_row.addWidget(self.trusted_hash_add_btn)
        trusted_hash_v.addLayout(trusted_hash_row)

        trusted_hash_actions = QHBoxLayout()
        trusted_hash_actions.setContentsMargins(0, 0, 0, 0)
        trusted_hash_actions.setSpacing(8)
        trusted_hash_spacer = QLabel("")
        trusted_hash_spacer.setFixedWidth(msg_label_width)
        trusted_hash_actions.addWidget(trusted_hash_spacer)
        self.trusted_hash_import_btn = QPushButton("Import Hash File")
        self.trusted_hash_remove_btn = QPushButton("Remove Selected")
        self.trusted_hash_remove_btn.setEnabled(False)
        trusted_hash_actions.addWidget(self.trusted_hash_import_btn)
        trusted_hash_actions.addWidget(self.trusted_hash_remove_btn)
        trusted_hash_actions.addStretch()
        trusted_hash_v.addLayout(trusted_hash_actions)

        self.trusted_hash_table = QTableWidget(0, 4)
        self.trusted_hash_table.setHorizontalHeaderLabels(["Use", "Algorithm", "Hash", "Label"])
        self.trusted_hash_table.verticalHeader().setVisible(False)
        self.trusted_hash_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.trusted_hash_table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.trusted_hash_table.setAlternatingRowColors(True)
        self.trusted_hash_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        th_hdr = self.trusted_hash_table.horizontalHeader()
        th_hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        th_hdr.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        th_hdr.setSectionResizeMode(2, QHeaderView.Stretch)
        th_hdr.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.trusted_hash_table.setMinimumHeight(180)
        self.trusted_hash_table.setMaximumHeight(240)
        self.trusted_hash_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        trusted_hash_v.addWidget(self.trusted_hash_table, 1)
        gpg_v.addWidget(_make_message_auth_subsection("Trusted Hashes", trusted_hash_tab, checked=False))

        gpg_keys_tab = QWidget()
        gpg_keys_v = QVBoxLayout(gpg_keys_tab)
        gpg_keys_v.setContentsMargins(8, 8, 8, 8)
        gpg_keys_v.setSpacing(8)
        gpg_keys_v.setAlignment(Qt.AlignTop)

        gpg_path_row = QHBoxLayout()
        gpg_path_row.setContentsMargins(0, 0, 0, 0)
        gpg_path_row.setSpacing(8)
        gpg_path_label = QLabel("GPG Executable")
        gpg_path_label.setFixedWidth(msg_label_width)
        gpg_path_row.addWidget(gpg_path_label)
        self.gpg_path_edit = QLineEdit()
        self.gpg_path_edit.setPlaceholderText("Auto-detect (gpg/gpg2)")
        gpg_path_row.addWidget(self.gpg_path_edit, 1)
        self.gpg_browse_btn = QPushButton("Browse")
        self.gpg_browse_btn.setFixedWidth(70)
        self.gpg_test_btn = QPushButton("Test")
        self.gpg_test_btn.setFixedWidth(70)
        self.gpg_refresh_keys_btn = QPushButton("Refresh Keys")
        self.gpg_refresh_keys_btn.setFixedWidth(110)
        gpg_path_row.addWidget(self.gpg_browse_btn)
        gpg_path_row.addWidget(self.gpg_test_btn)
        gpg_path_row.addWidget(self.gpg_refresh_keys_btn)
        gpg_keys_v.addLayout(gpg_path_row)

        gpg_action_row = QHBoxLayout()
        gpg_action_row.setContentsMargins(0, 0, 0, 0)
        gpg_action_row.setSpacing(8)
        gpg_action_spacer = QLabel("")
        gpg_action_spacer.setFixedWidth(msg_label_width)
        gpg_action_row.addWidget(gpg_action_spacer)
        self.gpg_import_key_btn = QPushButton("Import Key File")
        self.gpg_import_text_btn = QPushButton("Import Armored Key")
        self.gpg_sign_key_btn = QPushButton("Local-Sign Selected")
        self.gpg_sign_key_btn.setEnabled(False)
        gpg_action_row.addWidget(self.gpg_import_key_btn)
        gpg_action_row.addWidget(self.gpg_import_text_btn)
        gpg_action_row.addWidget(self.gpg_sign_key_btn)
        gpg_action_row.addStretch()
        gpg_keys_v.addLayout(gpg_action_row)

        gpg_status_row = QHBoxLayout()
        gpg_status_row.setContentsMargins(0, 0, 0, 0)
        gpg_status_row.setSpacing(8)
        gpg_status_spacer = QLabel("")
        gpg_status_spacer.setFixedWidth(msg_label_width)
        gpg_status_row.addWidget(gpg_status_spacer)
        self.gpg_status_label = QLabel("GPG status: not checked")
        self.gpg_status_label.setWordWrap(True)
        gpg_status_row.addWidget(self.gpg_status_label, 1)
        gpg_keys_v.addLayout(gpg_status_row)

        gpg_filter_row = QHBoxLayout()
        gpg_filter_row.setContentsMargins(0, 0, 0, 0)
        gpg_filter_row.setSpacing(8)
        gpg_filter_label = QLabel("Filter Keys")
        gpg_filter_label.setFixedWidth(msg_label_width)
        gpg_filter_row.addWidget(gpg_filter_label)
        self.gpg_key_filter_edit = QLineEdit()
        self.gpg_key_filter_edit.setPlaceholderText("Search trusted, fingerprint, callsign, email, or user ID")
        gpg_filter_row.addWidget(self.gpg_key_filter_edit, 1)
        gpg_keys_v.addLayout(gpg_filter_row)

        self.gpg_keys_table = QTableWidget(0, 3)
        self.gpg_keys_table.setHorizontalHeaderLabels(["Trusted", "Fingerprint", "User IDs"])
        self.gpg_keys_table.verticalHeader().setVisible(False)
        self.gpg_keys_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.gpg_keys_table.setSelectionMode(QTableWidget.SingleSelection)
        self.gpg_keys_table.setAlternatingRowColors(True)
        self.gpg_keys_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        gpg_hdr = self.gpg_keys_table.horizontalHeader()
        gpg_hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        gpg_hdr.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        gpg_hdr.setSectionResizeMode(2, QHeaderView.Stretch)
        self.gpg_keys_table.setMinimumHeight(220)
        self.gpg_keys_table.setMaximumHeight(300)
        self.gpg_keys_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        gpg_keys_v.addWidget(self.gpg_keys_table, 1)
        self.gpg_key_detail_label = QLabel("Select a key to view details.")
        self.gpg_key_detail_label.setWordWrap(True)
        self.gpg_key_detail_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.gpg_key_detail_label.setMaximumHeight(72)
        gpg_keys_v.addWidget(self.gpg_key_detail_label)
        gpg_v.addWidget(_make_message_auth_subsection("GPG Keys", gpg_keys_tab, checked=True))

        signing_tab = QWidget()
        signing_v = QVBoxLayout(signing_tab)
        signing_v.setContentsMargins(8, 8, 8, 8)
        signing_v.setSpacing(8)
        signing_v.setAlignment(Qt.AlignTop)

        signing_title = QLabel("<b>Signing Identity</b>")
        signing_v.addWidget(signing_title)
        signing_row = QHBoxLayout()
        signing_row.setContentsMargins(0, 0, 0, 0)
        signing_row.setSpacing(8)
        signing_label = QLabel("Default FLAmp signing key")
        signing_label.setFixedWidth(msg_label_width)
        signing_row.addWidget(signing_label)
        self.gpg_signing_key_combo = QComboBox()
        self.gpg_signing_key_combo.addItem("Auto-select when only one private key is available", "")
        signing_row.addWidget(self.gpg_signing_key_combo, 1)
        self.gpg_refresh_signing_keys_btn = QPushButton("Refresh Signing Keys")
        signing_row.addWidget(self.gpg_refresh_signing_keys_btn)
        signing_v.addLayout(signing_row)
        passphrase_row = QHBoxLayout()
        passphrase_row.setContentsMargins(0, 0, 0, 0)
        passphrase_row.setSpacing(8)
        passphrase_label = QLabel("Signing key passphrase")
        passphrase_label.setFixedWidth(msg_label_width)
        passphrase_row.addWidget(passphrase_label)
        self.gpg_signing_passphrase_edit = QLineEdit()
        self.gpg_signing_passphrase_edit.setEchoMode(QLineEdit.Password)
        self.gpg_signing_passphrase_edit.setPlaceholderText("Stored in OS credential store, not FIO settings")
        passphrase_row.addWidget(self.gpg_signing_passphrase_edit, 1)
        self.gpg_check_save_passphrase_btn = QPushButton("Check/Save")
        self.gpg_clear_passphrase_btn = QPushButton("Clear Saved")
        passphrase_row.addWidget(self.gpg_check_save_passphrase_btn)
        passphrase_row.addWidget(self.gpg_clear_passphrase_btn)
        signing_v.addLayout(passphrase_row)
        passphrase_confirm_row = QHBoxLayout()
        passphrase_confirm_row.setContentsMargins(0, 0, 0, 0)
        passphrase_confirm_row.setSpacing(8)
        passphrase_confirm_label = QLabel("Confirm passphrase")
        passphrase_confirm_label.setFixedWidth(msg_label_width)
        passphrase_confirm_row.addWidget(passphrase_confirm_label)
        self.gpg_signing_passphrase_confirm_edit = QLineEdit()
        self.gpg_signing_passphrase_confirm_edit.setEchoMode(QLineEdit.Password)
        passphrase_confirm_row.addWidget(self.gpg_signing_passphrase_confirm_edit, 1)
        passphrase_confirm_row.addStretch()
        signing_v.addLayout(passphrase_confirm_row)
        self.gpg_signing_status_label = QLabel(
            "FIO stores the selected key fingerprint in settings. Saved passphrases use the OS credential store."
        )
        self.gpg_signing_status_label.setWordWrap(True)
        signing_v.addWidget(self.gpg_signing_status_label)
        gpg_v.addWidget(_make_message_auth_subsection("Signing", signing_tab, checked=False))

        self.gpg_verify_enabled_chk.stateChanged.connect(self._mark_settings_dirty)
        self.hash_verify_enabled_chk.stateChanged.connect(self._mark_settings_dirty)
        self.trusted_hash_edit.returnPressed.connect(self._add_trusted_hash_entry)
        self.trusted_hash_add_btn.clicked.connect(self._add_trusted_hash_entry)
        self.trusted_hash_import_btn.clicked.connect(self._import_trusted_hash_file)
        self.trusted_hash_remove_btn.clicked.connect(self._remove_selected_trusted_hash_entries)
        self.trusted_hash_table.itemChanged.connect(self._on_trusted_hash_table_item_changed)
        self.trusted_hash_table.itemSelectionChanged.connect(self._update_trusted_hash_actions)
        self.gpg_path_edit.textChanged.connect(self._mark_settings_dirty)
        self.gpg_browse_btn.clicked.connect(self._choose_gpg_executable_path)
        self.gpg_test_btn.clicked.connect(self._test_gpg_executable)
        self.gpg_refresh_keys_btn.clicked.connect(self._refresh_gpg_keys_table)
        self.gpg_import_key_btn.clicked.connect(self._import_gpg_key_file)
        self.gpg_import_text_btn.clicked.connect(self._import_gpg_key_text)
        self.gpg_sign_key_btn.clicked.connect(self._local_sign_selected_gpg_key)
        self.gpg_keys_table.itemChanged.connect(self._on_gpg_keys_table_item_changed)
        self.gpg_keys_table.itemSelectionChanged.connect(self._update_gpg_sign_button_state)
        self.gpg_keys_table.itemSelectionChanged.connect(self._refresh_gpg_key_detail)
        self.gpg_key_filter_edit.textChanged.connect(self._apply_gpg_key_filter)
        self.gpg_refresh_signing_keys_btn.clicked.connect(self._refresh_gpg_signing_keys)
        self.gpg_signing_key_combo.currentIndexChanged.connect(self._on_gpg_signing_key_changed)
        self.gpg_check_save_passphrase_btn.clicked.connect(self._check_and_save_gpg_signing_passphrase)
        self.gpg_clear_passphrase_btn.clicked.connect(self._clear_gpg_signing_passphrase)

        gpg_container = QWidget()
        gpg_container.setLayout(gpg_v)
        gpg_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        gpg_group = self._make_collapsible_group(
            "Message Auth (Key/Hash)",
            gpg_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.message-auth",
        )
        self._register_collapsible_group(gpg_group, self._summary_gpg_settings)
        gpg_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        # VarAC Settings
        varac_group = QGroupBox("VarAC Settings")
        varac_v = QVBoxLayout()
        varac_v.setSpacing(6)
        varac_v.setAlignment(Qt.AlignTop)
        varac_group.setLayout(varac_v)
        self.varac_scope_label = QLabel("Editing Radio: --")
        self.varac_scope_label.setWordWrap(True)
        varac_v.addWidget(self.varac_scope_label)

        def _make_varac_subgroup(title: str, description: str = "", *, checked: bool = True) -> QVBoxLayout:
            section = QFrame()
            section.setFrameShape(QFrame.StyledPanel)
            section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            section_layout = QVBoxLayout(section)
            section_layout.setContentsMargins(8, 6, 8, 8)
            section_layout.setSpacing(6)

            header_btn = QToolButton()
            header_btn.setCheckable(True)
            header_btn.setChecked(checked)
            header_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
            header_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
            header_btn.setText(title)
            header_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            header_btn.setMinimumHeight(26)
            header_btn.setStyleSheet(self._section_header_style("neutral", resolve_theme(self.settings)))
            section_layout.addWidget(header_btn)

            content = QWidget()
            content.setVisible(checked)
            layout = QVBoxLayout(content)
            layout.setContentsMargins(8, 8, 8, 8)
            layout.setSpacing(6)
            if description:
                hint = QLabel(description)
                hint.setWordWrap(True)
                layout.addWidget(hint)

            def _toggle(opened: bool, *, body: QWidget = content, button: QToolButton = header_btn) -> None:
                body.setVisible(opened)
                button.setArrowType(Qt.DownArrow if opened else Qt.RightArrow)
                section.updateGeometry()
                self._sync_current_section_scroll_size()

            header_btn.toggled.connect(_toggle)
            section_layout.addWidget(content)
            varac_v.addWidget(section)
            return layout

        varac_paths_v = _make_varac_subgroup(
            "VarAC Paths and Launch",
            "Configure the selected radio's VarAC application location, launch behavior, and file exchange folders.",
            checked=True,
        )
        bbs_settings_v = _make_varac_subgroup(
            "BBS Settings",
            "These settings control the selected radio's live VarAC BBS folder, access policy, archive behavior, and allowed callsigns.",
            checked=False,
        )
        vault_guard_v = _make_varac_subgroup(
            "Vault / VGuard Settings",
            "Managed BBS Vault publishes one controlled view into the live VarAC BBS while keeping source folders in a managed root. "
            "VGuard is separate and watches inbound transfers for unauthorized senders or unsafe handling paths.",
            checked=False,
        )

        varac_row = QHBoxLayout()
        varac_row.setContentsMargins(0, 0, 0, 0)
        varac_row.setSpacing(8)
        varac_install_label = QLabel("VarAC Install Folder:")
        varac_install_label.setFixedWidth(msg_label_width)
        varac_row.addWidget(varac_install_label)
        self.varac_path_edit = QLineEdit()
        self.varac_path_edit.setPlaceholderText("Folder containing VarAC")
        self.varac_path_edit.textChanged.connect(self._on_launch_paths_changed)
        varac_row.addWidget(self.varac_path_edit, 1)
        varac_browse = QPushButton("Browse")
        varac_browse.setFixedWidth(70)
        varac_browse.clicked.connect(self._choose_varac_install_path)
        varac_row.addWidget(varac_browse)
        varac_row.addWidget(
            self._make_contextual_autofill_button(
                "varac_core",
                "Auto-Fill",
                "varac",
                [
                    "varac_path",
                    "varac_ini_path",
                    "message_paths.varac",
                    "varac_outbox_dir",
                    "varac_bbs_dir",
                    "varac_bbs_archive_dir",
                ],
                base_edit=self.varac_path_edit,
                tooltip="Use the selected radio's VarAC install folder to find VarAC INI and message/BBS folders.",
            )
        )
        varac_paths_v.addLayout(varac_row)

        varac_launch_row = QHBoxLayout()
        varac_launch_row.setContentsMargins(0, 0, 0, 0)
        varac_launch_row.setSpacing(8)
        varac_launch_label = QLabel("VarAC Launch Command (Advanced):")
        varac_launch_label.setFixedWidth(msg_label_width)
        varac_launch_row.addWidget(varac_launch_label)
        self.varac_launch_cmd_edit = QLineEdit()
        if platform.system() == "Windows":
            self.varac_launch_cmd_edit.setPlaceholderText("Usually leave blank (auto-launch from Install Folder)")
        else:
            self.varac_launch_cmd_edit.setPlaceholderText(
                "Usually leave blank. Advanced override only (example: env WINEPREFIX=/home/user/.wine wine-stable C:\\VarAC\\VarAC.exe)"
            )
        self.varac_launch_cmd_edit.setToolTip(
            "Recommended: leave blank. FreqInOut auto-launches VarAC from Install Folder (including Wine wrapping on Linux). "
            "Use only if default launch fails or you need a custom Wine command/prefix."
        )
        self.varac_launch_cmd_edit.textChanged.connect(self._on_launch_paths_changed)
        varac_launch_row.addWidget(self.varac_launch_cmd_edit, 1)
        varac_paths_v.addLayout(varac_launch_row)
        varac_launch_hint_row = QHBoxLayout()
        varac_launch_hint_row.setContentsMargins(0, 0, 0, 0)
        varac_launch_hint_row.addSpacing(msg_label_width)
        varac_launch_hint = QLabel(
            "Recommended: leave blank. This is an advanced override for custom Wine launch scenarios."
        )
        varac_launch_hint.setWordWrap(True)
        varac_launch_hint_row.addWidget(varac_launch_hint, 1)
        varac_paths_v.addLayout(varac_launch_hint_row)

        varac_ini_row = QHBoxLayout()
        varac_ini_row.setContentsMargins(0, 0, 0, 0)
        varac_ini_row.setSpacing(8)
        varac_ini_label = QLabel("VarAC INI File")
        varac_ini_label.setFixedWidth(msg_label_width)
        varac_ini_row.addWidget(varac_ini_label)
        self.varac_ini_path_edit = QLineEdit()
        self.varac_ini_path_edit.setPlaceholderText("VarAC.ini path for the selected radio")
        varac_ini_row.addWidget(self.varac_ini_path_edit, 1)
        varac_ini_browse = QPushButton("Browse")
        varac_ini_browse.setFixedWidth(70)
        varac_ini_browse.clicked.connect(self._choose_varac_ini_path)
        varac_ini_row.addWidget(varac_ini_browse)
        varac_paths_v.addLayout(varac_ini_row)

        varac_paths_v.addWidget(
            build_msg_row(
                "VarAC Incoming Files",
                varac_edit,
                lambda: self._choose_msg_path("varac", varac_edit),
            )
        )

        outbox_dir_row = QHBoxLayout()
        outbox_dir_row.setContentsMargins(0, 0, 0, 0)
        outbox_dir_row.setSpacing(8)
        outbox_dir_label = QLabel("VarAC Outbox Directory")
        outbox_dir_label.setFixedWidth(msg_label_width)
        outbox_dir_row.addWidget(outbox_dir_label)
        self.varac_outbox_dir_edit = QLineEdit()
        self.varac_outbox_dir_edit.setPlaceholderText("VarAC Outbox directory")
        outbox_dir_row.addWidget(self.varac_outbox_dir_edit, 1)
        outbox_dir_browse = QPushButton("Browse")
        outbox_dir_browse.setFixedWidth(70)
        outbox_dir_browse.clicked.connect(self._choose_varac_outbox_dir)
        outbox_dir_row.addWidget(outbox_dir_browse)
        varac_paths_v.addLayout(outbox_dir_row)

        bbs_dir_row = QHBoxLayout()
        bbs_dir_row.setContentsMargins(0, 0, 0, 0)
        bbs_dir_row.setSpacing(8)
        bbs_dir_label = QLabel("BBS Directory")
        bbs_dir_label.setFixedWidth(msg_label_width)
        bbs_dir_row.addWidget(bbs_dir_label)
        self.varac_bbs_dir_edit = QLineEdit()
        self.varac_bbs_dir_edit.setPlaceholderText("VarAC BBS directory")
        bbs_dir_row.addWidget(self.varac_bbs_dir_edit, 1)
        bbs_dir_browse = QPushButton("Browse")
        bbs_dir_browse.setFixedWidth(70)
        bbs_dir_browse.clicked.connect(self._choose_varac_bbs_dir)
        bbs_dir_row.addWidget(bbs_dir_browse)
        bbs_settings_v.addLayout(bbs_dir_row)

        bbs_archive_row = QHBoxLayout()
        bbs_archive_row.setContentsMargins(0, 0, 0, 0)
        bbs_archive_row.setSpacing(8)
        bbs_archive_label = QLabel("BBS Archive")
        bbs_archive_label.setFixedWidth(msg_label_width)
        bbs_archive_row.addWidget(bbs_archive_label)
        self.varac_bbs_archive_dir_edit = QLineEdit()
        self.varac_bbs_archive_dir_edit.setPlaceholderText("Archive destination directory")
        bbs_archive_row.addWidget(self.varac_bbs_archive_dir_edit, 1)
        bbs_archive_browse = QPushButton("Browse")
        bbs_archive_browse.setFixedWidth(70)
        bbs_archive_browse.clicked.connect(self._choose_varac_bbs_archive_dir)
        bbs_archive_row.addWidget(bbs_archive_browse)
        bbs_settings_v.addLayout(bbs_archive_row)

        bbs_policy_row = QHBoxLayout()
        bbs_policy_row.setContentsMargins(0, 0, 0, 0)
        bbs_policy_row.setSpacing(8)
        self.varac_bbs_auto_archive_chk = QCheckBox("Enable Auto-Archive")
        bbs_policy_row.addWidget(self.varac_bbs_auto_archive_chk)
        bbs_policy_row.addWidget(QLabel("After"))
        self.varac_bbs_archive_days_combo = QComboBox()
        for day in (1, 3, 5, 7, 10, 14, 21, 30):
            self.varac_bbs_archive_days_combo.addItem(str(day), day)
        self.varac_bbs_archive_days_combo.setCurrentText("14")
        self.varac_bbs_archive_days_combo.setFixedWidth(80)
        bbs_policy_row.addWidget(self.varac_bbs_archive_days_combo)
        bbs_policy_row.addWidget(QLabel("days"))
        bbs_policy_row.addStretch()
        varac_hint = QLabel("Moves files older than selected days from BBS Directory to BBS Archive.")
        varac_hint.setWordWrap(True)
        varac_hint.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        varac_hint.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        policy_inner = QVBoxLayout()
        policy_inner.setSpacing(4)
        policy_inner.setContentsMargins(0, 0, 0, 0)
        policy_inner.addLayout(bbs_policy_row)
        policy_inner.addWidget(varac_hint)
        bbs_settings_v.addWidget(QLabel("Auto-Archive Policy"))
        bbs_settings_v.addLayout(policy_inner)

        bbs_access_checks_row = QHBoxLayout()
        bbs_access_checks_row.setContentsMargins(0, 0, 0, 0)
        bbs_access_checks_row.setSpacing(16)
        self.varac_bbs_enabled_chk = QCheckBox("Enable BBS")
        bbs_access_checks_row.addWidget(self.varac_bbs_enabled_chk)
        self.varac_bbs_limit_access_chk = QCheckBox("Limit Access To Callsigns")
        bbs_access_checks_row.addWidget(self.varac_bbs_limit_access_chk)
        self.varac_bbs_announce_chk = QCheckBox("Announce")
        bbs_access_checks_row.addWidget(self.varac_bbs_announce_chk)
        bbs_access_checks_row.addStretch()
        bbs_access_actions_row = QHBoxLayout()
        bbs_access_actions_row.setContentsMargins(0, 0, 0, 0)
        bbs_access_actions_row.setSpacing(8)
        self.varac_bbs_sync_btn = QPushButton("Sync From VarAC.ini")
        self.varac_bbs_sync_btn.clicked.connect(self._sync_varac_bbs_from_ini)
        bbs_access_actions_row.addWidget(self.varac_bbs_sync_btn)
        self.varac_bbs_write_btn = QPushButton("Write to VarAC.ini")
        self.varac_bbs_write_btn.clicked.connect(self._sync_varac_bbs_to_ini)
        bbs_access_actions_row.addWidget(self.varac_bbs_write_btn)
        bbs_access_actions_row.addStretch()
        bbs_access_inner = QVBoxLayout()
        bbs_access_inner.setSpacing(6)
        bbs_access_inner.setContentsMargins(0, 0, 0, 0)
        bbs_access_inner.addLayout(bbs_access_checks_row)
        bbs_access_inner.addLayout(bbs_access_actions_row)
        bbs_settings_v.addWidget(QLabel("BBS Access"))
        bbs_settings_v.addLayout(bbs_access_inner)

        bbs_settings_v.addWidget(QLabel("Allowed Callsigns"))
        bbs_callsigns_wrap = QWidget()
        bbs_callsigns_layout = QVBoxLayout(bbs_callsigns_wrap)
        bbs_callsigns_layout.setContentsMargins(0, 0, 0, 0)
        bbs_callsigns_layout.setSpacing(6)
        bbs_callsigns_lookup_row = QHBoxLayout()
        bbs_callsigns_lookup_row.setContentsMargins(0, 0, 0, 0)
        bbs_callsigns_lookup_row.setSpacing(8)
        self.varac_bbs_callsign_lookup_edit = QLineEdit()
        self.varac_bbs_callsign_lookup_edit.setPlaceholderText("Search known operators or enter a callsign")
        bbs_callsigns_lookup_row.addWidget(self.varac_bbs_callsign_lookup_edit, 1)
        self.varac_bbs_add_callsign_btn = QPushButton("Add")
        bbs_callsigns_lookup_row.addWidget(self.varac_bbs_add_callsign_btn)
        self.varac_bbs_remove_callsign_btn = QPushButton("Remove Selected")
        bbs_callsigns_lookup_row.addWidget(self.varac_bbs_remove_callsign_btn)
        bbs_callsigns_layout.addLayout(bbs_callsigns_lookup_row)
        self.varac_bbs_callsigns_list = QListWidget()
        self.varac_bbs_callsigns_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.varac_bbs_callsigns_list.setMaximumHeight(108)
        bbs_callsigns_layout.addWidget(self.varac_bbs_callsigns_list)
        bbs_callsigns_hint = QLabel(
            "Lookup uses known operators when available. Manual callsign entry is still allowed."
        )
        bbs_callsigns_hint.setWordWrap(True)
        bbs_callsigns_layout.addWidget(bbs_callsigns_hint)
        self.varac_bbs_callsign_lookup_edit.textChanged.connect(lambda _text: self._refresh_varac_bbs_callsign_actions())
        self.varac_bbs_callsign_lookup_edit.returnPressed.connect(self._add_varac_bbs_allowed_callsign)
        self.varac_bbs_add_callsign_btn.clicked.connect(self._add_varac_bbs_allowed_callsign)
        self.varac_bbs_remove_callsign_btn.clicked.connect(self._remove_selected_varac_bbs_allowed_callsigns)
        self.varac_bbs_callsigns_list.itemSelectionChanged.connect(self._refresh_varac_bbs_callsign_actions)
        self._reload_varac_bbs_operator_lookup()
        self._refresh_varac_bbs_callsign_actions()
        bbs_settings_v.addWidget(bbs_callsigns_wrap)

        bbs_sync_status_row = QHBoxLayout()
        bbs_sync_status_row.setContentsMargins(0, 0, 0, 0)
        self.varac_bbs_sync_status_label = QLabel("No VarAC.ini BBS sync yet.")
        self.varac_bbs_sync_status_label.setWordWrap(True)
        bbs_sync_status_row.addWidget(self.varac_bbs_sync_status_label, 1)
        bbs_settings_v.addLayout(bbs_sync_status_row)

        varac_sync_note_row = QHBoxLayout()
        varac_sync_note_row.setContentsMargins(0, 0, 0, 0)
        self.varac_bbs_sync_note_label = QLabel(
            "VarAC remains the source of truth; FreqInOut can sync only the [BBS] section with explicit confirmation."
        )
        self.varac_bbs_sync_note_label.setWordWrap(True)
        varac_sync_note_row.addWidget(self.varac_bbs_sync_note_label, 1)
        bbs_settings_v.addLayout(varac_sync_note_row)

        self._varac_bbs_ini_sync_state = ""

        vault_guard_v.addWidget(QLabel("Managed BBS Services"))
        vault_enabled_row = QHBoxLayout()
        vault_enabled_row.setContentsMargins(0, 0, 0, 0)
        vault_enabled_row.setSpacing(8)
        vault_enabled_inner = QVBoxLayout()
        vault_enabled_inner.setContentsMargins(0, 0, 0, 0)
        vault_enabled_inner.setSpacing(6)
        vault_enabled_top = QHBoxLayout()
        vault_enabled_top.setContentsMargins(0, 0, 0, 0)
        vault_enabled_top.setSpacing(10)
        self.varac_bbs_vault_enabled_chk_main = QCheckBox("Enable Managed BBS Vault")
        vault_enabled_top.addWidget(self.varac_bbs_vault_enabled_chk_main)
        self.varac_bbs_vault_initialize_btn = QPushButton("Initialize Managed Vault")
        self.varac_bbs_vault_initialize_btn.clicked.connect(self._initialize_varac_bbs_vault)
        vault_enabled_top.addWidget(self.varac_bbs_vault_initialize_btn)
        self.varac_bbs_vault_reset_btn = QPushButton("Reset To Default")
        self.varac_bbs_vault_reset_btn.clicked.connect(self._reset_varac_bbs_vault_to_default)
        vault_enabled_top.addWidget(self.varac_bbs_vault_reset_btn)
        vault_enabled_top.addStretch()
        vault_enabled_inner.addLayout(vault_enabled_top)
        vault_enabled_note = QLabel(
            "Managed Vault publishes a selected named location into the live VarAC BBS folder for the selected radio profile. "
            "Access codes are operational controls, not strong secrets."
        )
        vault_enabled_note.setWordWrap(True)
        vault_enabled_inner.addWidget(vault_enabled_note)
        vault_enabled_row.addLayout(vault_enabled_inner, 1)
        vault_guard_v.addLayout(vault_enabled_row)

        vault_root_row = QHBoxLayout()
        vault_root_row.setContentsMargins(0, 0, 0, 0)
        vault_root_row.setSpacing(8)
        vault_root_label = QLabel("Managed Root")
        vault_root_label.setFixedWidth(msg_label_width)
        vault_root_row.addWidget(vault_root_label)
        self.varac_bbs_vault_root_edit = QLineEdit()
        self.varac_bbs_vault_root_edit.setReadOnly(True)
        self.varac_bbs_vault_root_edit.setPlaceholderText("Defaults to the VarAC BBS area: a FIO_BBS_Vault folder next to the live BBS directory")
        vault_root_row.addWidget(self.varac_bbs_vault_root_edit, 1)
        vault_root_browse = QPushButton("Browse")
        vault_root_browse.setFixedWidth(70)
        vault_root_browse.clicked.connect(self._choose_varac_bbs_vault_root)
        vault_root_browse.setText("Info")
        vault_root_row.addWidget(vault_root_browse)
        vault_guard_v.addLayout(vault_root_row)
        self.varac_bbs_vault_root_hint_label = QLabel(
            "Set the VarAC BBS directory first to see the default vault location."
        )
        self.varac_bbs_vault_root_hint_label.setWordWrap(True)
        vault_guard_v.addWidget(self.varac_bbs_vault_root_hint_label)

        vault_guard_v.addWidget(QLabel("Vault Policy"))
        vault_policy_row = QHBoxLayout()
        vault_policy_row.setContentsMargins(0, 0, 0, 0)
        vault_policy_row.setSpacing(8)
        vault_policy_inner = QGridLayout()
        vault_policy_inner.setContentsMargins(0, 0, 0, 0)
        vault_policy_inner.setHorizontalSpacing(8)
        vault_policy_inner.setVerticalSpacing(6)
        vault_policy_inner.addWidget(QLabel("Default Location"), 0, 0)
        self.varac_bbs_vault_default_location_combo = QComboBox()
        self.varac_bbs_vault_default_location_combo.setMinimumWidth(180)
        vault_policy_inner.addWidget(self.varac_bbs_vault_default_location_combo, 0, 1)
        vault_policy_inner.addWidget(QLabel("Global Code Policy"), 0, 2)
        self.varac_bbs_vault_global_code_policy_combo = QComboBox()
        self.varac_bbs_vault_global_code_policy_combo.addItems(
            [
                "Allow public locations",
                "Require for non-default locations",
                "Require for all restricted locations",
            ]
        )
        self.varac_bbs_vault_global_code_policy_combo.setMinimumWidth(260)
        vault_policy_inner.addWidget(self.varac_bbs_vault_global_code_policy_combo, 0, 3)
        vault_policy_inner.addWidget(QLabel("Request Parsing"), 1, 0)
        self.varac_bbs_vault_trigger_mode_combo = QComboBox()
        self.varac_bbs_vault_trigger_mode_combo.addItems(["VarAC session commands", "Command prefix", "Exact code only"])
        self.varac_bbs_vault_trigger_mode_combo.setMinimumWidth(170)
        vault_policy_inner.addWidget(self.varac_bbs_vault_trigger_mode_combo, 1, 1)
        vault_policy_inner.addWidget(QLabel("Return Mode"), 1, 2)
        self.varac_bbs_vault_return_mode_combo = QComboBox()
        self.varac_bbs_vault_return_mode_combo.addItems(
            ["On disconnect", "After inactivity timeout", "Manual operator reset only"]
        )
        self.varac_bbs_vault_return_mode_combo.setMinimumWidth(220)
        vault_policy_inner.addWidget(self.varac_bbs_vault_return_mode_combo, 1, 3)
        vault_policy_inner.addWidget(QLabel("Idle Timeout"), 2, 0)
        self.varac_bbs_vault_idle_timeout_combo = QComboBox()
        for seconds, label in ((300, "5 min"), (600, "10 min"), (900, "15 min"), (1800, "30 min")):
            self.varac_bbs_vault_idle_timeout_combo.addItem(label, seconds)
        vault_policy_inner.addWidget(self.varac_bbs_vault_idle_timeout_combo, 2, 1)
        vault_policy_inner.addWidget(QLabel("Failed Attempts"), 2, 2)
        self.varac_bbs_vault_failed_attempt_limit_combo = QComboBox()
        for count in (2, 3, 4, 5):
            self.varac_bbs_vault_failed_attempt_limit_combo.addItem(str(count), count)
        vault_policy_inner.addWidget(self.varac_bbs_vault_failed_attempt_limit_combo, 2, 3)
        vault_policy_inner.addWidget(QLabel("Cooldown"), 3, 0)
        self.varac_bbs_vault_cooldown_combo = QComboBox()
        for seconds, label in ((900, "15 min"), (1800, "30 min"), (3600, "60 min")):
            self.varac_bbs_vault_cooldown_combo.addItem(label, seconds)
        vault_policy_inner.addWidget(self.varac_bbs_vault_cooldown_combo, 3, 1)
        self.varac_bbs_vault_flamp_enabled_chk = QCheckBox("Enable FLAMP relay service")
        vault_policy_inner.addWidget(self.varac_bbs_vault_flamp_enabled_chk, 3, 2)
        flamp_policy_row = QHBoxLayout()
        flamp_policy_row.setContentsMargins(0, 0, 0, 0)
        flamp_policy_row.setSpacing(8)
        self.varac_bbs_vault_flamp_relay_dir_edit = QLineEdit()
        self.varac_bbs_vault_flamp_relay_dir_edit.setPlaceholderText("FLAMP relay folder")
        flamp_policy_row.addWidget(self.varac_bbs_vault_flamp_relay_dir_edit, 1)
        self.varac_bbs_vault_flamp_relay_browse_btn = QPushButton("Browse")
        self.varac_bbs_vault_flamp_relay_browse_btn.clicked.connect(self._choose_varac_bbs_vault_flamp_relay_dir)
        flamp_policy_row.addWidget(self.varac_bbs_vault_flamp_relay_browse_btn)
        vault_policy_inner.addLayout(flamp_policy_row, 3, 3)
        vault_policy_inner.addWidget(QLabel("Limit FLAMP listing to files newer than"), 4, 0)
        self.varac_bbs_vault_flamp_listing_age_combo = QComboBox()
        for days in (3, 5, 7, 10, 14, 30):
            self.varac_bbs_vault_flamp_listing_age_combo.addItem(f"{days} days", days)
        vault_policy_inner.addWidget(self.varac_bbs_vault_flamp_listing_age_combo, 4, 1)
        self.varac_bbs_vault_flamp_relay_hint_label = QLabel(
            "Set FLAMP/rx first and FreqInOut will suggest a sibling relay folder automatically."
        )
        self.varac_bbs_vault_flamp_relay_hint_label.setWordWrap(True)
        vault_policy_inner.addWidget(self.varac_bbs_vault_flamp_relay_hint_label, 5, 0, 1, 4)
        vault_policy_row.addLayout(vault_policy_inner, 1)
        vault_guard_v.addLayout(vault_policy_row)

        vault_guard_v.addWidget(QLabel("Locations"))
        vault_locations_row = QHBoxLayout()
        vault_locations_row.setContentsMargins(0, 0, 0, 0)
        vault_locations_row.setSpacing(8)
        vault_locations_wrap = QWidget()
        vault_locations_layout = QVBoxLayout(vault_locations_wrap)
        vault_locations_layout.setContentsMargins(0, 0, 0, 0)
        vault_locations_layout.setSpacing(6)
        vault_locations_actions = QHBoxLayout()
        vault_locations_actions.setContentsMargins(0, 0, 0, 0)
        vault_locations_actions.setSpacing(8)
        self.varac_bbs_vault_new_btn = QPushButton("New Location")
        self.varac_bbs_vault_new_btn.clicked.connect(self._new_varac_bbs_vault_location)
        vault_locations_actions.addWidget(self.varac_bbs_vault_new_btn)
        self.varac_bbs_vault_save_location_btn = QPushButton("Save Location")
        self.varac_bbs_vault_save_location_btn.clicked.connect(self._save_varac_bbs_vault_location)
        vault_locations_actions.addWidget(self.varac_bbs_vault_save_location_btn)
        self.varac_bbs_vault_remove_btn = QPushButton("Remove")
        self.varac_bbs_vault_remove_btn.clicked.connect(self._remove_varac_bbs_vault_location)
        vault_locations_actions.addWidget(self.varac_bbs_vault_remove_btn)
        vault_locations_actions.addStretch()
        vault_locations_layout.addLayout(vault_locations_actions)
        self.varac_bbs_vault_locations_list = QListWidget()
        self.varac_bbs_vault_locations_list.setMaximumHeight(132)
        self.varac_bbs_vault_locations_list.itemSelectionChanged.connect(self._on_varac_bbs_vault_location_selected)
        vault_locations_layout.addWidget(self.varac_bbs_vault_locations_list)
        vault_editor_grid = QGridLayout()
        vault_editor_grid.setContentsMargins(0, 0, 0, 0)
        vault_editor_grid.setHorizontalSpacing(8)
        vault_editor_grid.setVerticalSpacing(6)
        vault_editor_grid.addWidget(QLabel("Location Name"), 0, 0)
        self.varac_bbs_vault_location_name_edit = QLineEdit()
        self.varac_bbs_vault_location_name_edit.setPlaceholderText("Example: Logistics")
        vault_editor_grid.addWidget(self.varac_bbs_vault_location_name_edit, 0, 1)
        vault_editor_grid.addWidget(QLabel("Alias"), 0, 2)
        self.varac_bbs_vault_alias_edit = QLineEdit()
        self.varac_bbs_vault_alias_edit.setPlaceholderText("Example: LOGI")
        self.varac_bbs_vault_alias_edit.setMaxLength(32)
        self.varac_bbs_vault_alias_edit.setToolTip("Alias is the VarAC command callers type. Only letters and numbers are used.")
        vault_editor_grid.addWidget(self.varac_bbs_vault_alias_edit, 0, 3)
        vault_editor_grid.addWidget(QLabel("Custom Helper Text"), 1, 0)
        self.varac_bbs_vault_description_edit = QLineEdit()
        self.varac_bbs_vault_description_edit.setMaxLength(80)
        self.varac_bbs_vault_description_edit.setPlaceholderText("Optional text appended to the helper filename")
        self.varac_bbs_vault_description_edit.setToolTip(
            "FIO generates the command text. This optional text is appended to the helper filename."
        )
        vault_editor_grid.addWidget(self.varac_bbs_vault_description_edit, 1, 1, 1, 3)
        vault_editor_grid.addWidget(QLabel("Helper Filename"), 2, 0)
        self.varac_bbs_vault_helper_preview_label = QLabel("Select a location to preview the generated helper.")
        self.varac_bbs_vault_helper_preview_label.setWordWrap(True)
        vault_editor_grid.addWidget(self.varac_bbs_vault_helper_preview_label, 2, 1, 1, 3)
        vault_editor_grid.addWidget(QLabel("Source Folder"), 3, 0)
        self.varac_bbs_vault_source_dir_edit = QLineEdit()
        self.varac_bbs_vault_source_dir_edit.setPlaceholderText("Managed Root/locations/<Location Name> is the usual pattern")
        vault_editor_grid.addWidget(self.varac_bbs_vault_source_dir_edit, 3, 1, 1, 2)
        self.varac_bbs_vault_source_dir_browse_btn = QPushButton("Browse")
        self.varac_bbs_vault_source_dir_browse_btn.clicked.connect(self._choose_varac_bbs_vault_location_source)
        vault_editor_grid.addWidget(self.varac_bbs_vault_source_dir_browse_btn, 3, 3)
        self.varac_bbs_vault_source_hint_label = QLabel(
            "Typical pattern in the VarAC BBS area: Managed Root / locations / <Location Name>. Save Location can create that folder."
        )
        self.varac_bbs_vault_source_hint_label.setWordWrap(True)
        vault_editor_grid.addWidget(self.varac_bbs_vault_source_hint_label, 4, 0, 1, 4)
        self.varac_bbs_vault_enabled_chk = QCheckBox("Enabled")
        vault_editor_grid.addWidget(self.varac_bbs_vault_enabled_chk, 5, 0)
        self.varac_bbs_vault_list_in_root_chk = QCheckBox("List In Root Menu")
        vault_editor_grid.addWidget(self.varac_bbs_vault_list_in_root_chk, 5, 1)
        vault_editor_grid.addWidget(QLabel("Visibility"), 5, 2)
        self.varac_bbs_vault_visibility_combo = QComboBox()
        self.varac_bbs_vault_visibility_combo.addItems(["Public", "Allowed callsigns only", "Hidden"])
        vault_editor_grid.addWidget(self.varac_bbs_vault_visibility_combo, 5, 3)
        vault_editor_grid.addWidget(QLabel("Open Rule"), 6, 0)
        self.varac_bbs_vault_open_rule_combo = QComboBox()
        self.varac_bbs_vault_open_rule_combo.addItems(["Public", "Allowed callsigns only", "Allowed callsigns + access code"])
        vault_editor_grid.addWidget(self.varac_bbs_vault_open_rule_combo, 6, 1, 1, 3)
        self.varac_bbs_vault_inherit_callsigns_chk = QCheckBox("Inherit Global Allowed Callsigns")
        vault_editor_grid.addWidget(self.varac_bbs_vault_inherit_callsigns_chk, 7, 0, 1, 4)
        vault_editor_grid.addWidget(QLabel("Location Allowed Callsigns"), 8, 0)
        self.varac_bbs_vault_allowed_callsigns_edit = QLineEdit()
        self.varac_bbs_vault_allowed_callsigns_edit.setPlaceholderText("Optional stricter subset, comma-separated")
        vault_editor_grid.addWidget(self.varac_bbs_vault_allowed_callsigns_edit, 8, 1, 1, 3)
        vault_editor_grid.addWidget(QLabel("Access Code"), 9, 0)
        self.varac_bbs_vault_access_code_edit = QLineEdit()
        self.varac_bbs_vault_access_code_edit.setEchoMode(QLineEdit.Password)
        self.varac_bbs_vault_access_code_edit.setPlaceholderText("Enter a new access code")
        self.varac_bbs_vault_access_code_edit.setToolTip(
            "Stored locally for the operator and hashed for caller verification."
        )
        self._attach_password_toggle_action(self.varac_bbs_vault_access_code_edit)
        vault_editor_grid.addWidget(self.varac_bbs_vault_access_code_edit, 9, 1)
        vault_editor_grid.addWidget(QLabel("Confirm Code"), 9, 2)
        self.varac_bbs_vault_access_code_confirm_edit = QLineEdit()
        self.varac_bbs_vault_access_code_confirm_edit.setEchoMode(QLineEdit.Password)
        self.varac_bbs_vault_access_code_confirm_edit.setPlaceholderText("Confirm only when changing the code")
        self.varac_bbs_vault_access_code_confirm_edit.setToolTip(
            "Required when setting or changing the code."
        )
        self._attach_password_toggle_action(self.varac_bbs_vault_access_code_confirm_edit)
        vault_editor_grid.addWidget(self.varac_bbs_vault_access_code_confirm_edit, 9, 3)
        vault_locations_layout.addLayout(vault_editor_grid)
        self.varac_bbs_vault_code_status_label = QLabel(
            "No code configured. Enter a code twice to set one for this location."
        )
        vault_locations_layout.addWidget(self.varac_bbs_vault_code_status_label)
        vault_locations_hint = QLabel(
            "Vault projects one location into the live BBS at a time. By default, source folders live in the VarAC BBS area "
            "under Managed Root / locations, not under the FreqInOut app folder. Saved access codes are hashed, and nested "
            "source subfolders are still ignored in this release."
        )
        vault_locations_hint.setWordWrap(True)
        vault_locations_layout.addWidget(vault_locations_hint)
        vault_locations_row.addWidget(vault_locations_wrap, 1)
        vault_guard_v.addLayout(vault_locations_row)

        vault_status_row = QHBoxLayout()
        vault_status_row.setContentsMargins(0, 0, 0, 0)
        self.varac_bbs_vault_status_label = QLabel("Managed Vault is not enabled for this radio profile.")
        self.varac_bbs_vault_status_label.setWordWrap(True)
        vault_status_row.addWidget(self.varac_bbs_vault_status_label, 1)
        vault_guard_v.addLayout(vault_status_row)

        self.varac_bbs_vault_enabled_chk_main.stateChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_enabled_chk_main.stateChanged.connect(self._refresh_section_titles)
        self.varac_bbs_vault_enabled_chk_main.stateChanged.connect(self._refresh_varac_bbs_vault_status_label)
        self.varac_bbs_vault_enabled_chk_main.stateChanged.connect(self._refresh_varac_bbs_vault_actions)
        self.varac_bbs_dir_edit.textChanged.connect(self._sync_varac_bbs_vault_root_from_bbs_dir)
        self.varac_bbs_vault_root_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_root_edit.textChanged.connect(self._refresh_varac_bbs_vault_status_label)
        self.varac_bbs_vault_root_edit.textChanged.connect(self._on_varac_bbs_vault_root_changed)
        self.varac_bbs_vault_root_edit.textChanged.connect(
            lambda _text: self._autofill_varac_bbs_vault_location_defaults()
        )
        self.varac_bbs_vault_default_location_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_default_location_combo.currentIndexChanged.connect(self._refresh_varac_bbs_vault_location_list)
        self.varac_bbs_vault_default_location_combo.currentIndexChanged.connect(self._refresh_varac_bbs_vault_status_label)
        self.varac_bbs_vault_global_code_policy_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_trigger_mode_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_return_mode_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_idle_timeout_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_failed_attempt_limit_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_cooldown_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_flamp_enabled_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_flamp_relay_dir_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_flamp_listing_age_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_location_name_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_location_name_edit.textChanged.connect(
            lambda _text: self._autofill_varac_bbs_vault_location_defaults()
        )
        self.varac_bbs_vault_location_name_edit.textChanged.connect(
            lambda _text: self._refresh_varac_bbs_vault_source_hint()
        )
        self.varac_bbs_vault_location_name_edit.textChanged.connect(
            lambda _text: self._refresh_varac_bbs_vault_helper_preview()
        )
        self.varac_bbs_vault_alias_edit.textChanged.connect(self._sanitize_varac_bbs_vault_alias_text)
        self.varac_bbs_vault_alias_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_alias_edit.textChanged.connect(
            lambda _text: self._refresh_varac_bbs_vault_helper_preview()
        )
        self.varac_bbs_vault_description_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_description_edit.textChanged.connect(
            lambda _text: self._refresh_varac_bbs_vault_helper_preview()
        )
        self.varac_bbs_vault_source_dir_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_source_dir_edit.textChanged.connect(
            lambda _text: self._refresh_varac_bbs_vault_source_hint()
        )
        self.varac_bbs_vault_enabled_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_list_in_root_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_visibility_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_visibility_combo.currentIndexChanged.connect(
            lambda _idx: self._refresh_varac_bbs_vault_helper_preview()
        )
        self.varac_bbs_vault_open_rule_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_open_rule_combo.currentIndexChanged.connect(
            lambda _idx: self._refresh_varac_bbs_vault_helper_preview()
        )
        self.varac_bbs_vault_inherit_callsigns_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_allowed_callsigns_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_access_code_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_access_code_confirm_edit.textChanged.connect(self._mark_settings_dirty)
        flamp_edit.textChanged.connect(lambda _text: self._maybe_autofill_varac_bbs_vault_flamp_relay_dir())

        vault_guard_v.addWidget(QLabel("VGuard File Protection"))
        guard_row = QHBoxLayout()
        guard_row.setContentsMargins(0, 0, 0, 0)
        guard_row.setSpacing(8)
        self.varac_guard_enabled_chk = QCheckBox("Enable VGuard File Protection")
        guard_row.addWidget(self.varac_guard_enabled_chk)
        self.varac_guard_mode_combo = QComboBox()
        self.varac_guard_mode_combo.addItems(
            ["Log only", "Delete unauthorized files", "Quarantine unauthorized files"]
        )
        self.varac_guard_mode_combo.setMinimumWidth(230)
        guard_row.addWidget(self.varac_guard_mode_combo)
        guard_row.addStretch()
        vault_guard_v.addLayout(guard_row)

        guard_trust_row = QHBoxLayout()
        guard_trust_row.setContentsMargins(0, 0, 0, 0)
        guard_trust_row.setSpacing(16)
        self.varac_guard_allow_bbs_chk = QCheckBox("Allow BBS allowed callsigns")
        self.varac_guard_allow_bbs_chk.setChecked(True)
        self.varac_guard_allow_bbs_chk.setToolTip("Allow files from callsigns in BBS Management -> Allowed Callsigns.")
        self.varac_guard_allow_trusted_chk = QCheckBox("Allow Operator History TRUSTED")
        self.varac_guard_allow_trusted_chk.setChecked(True)
        self.varac_guard_allow_trusted_chk.setToolTip("Allow files from callsigns marked TRUSTED in Operator History.")
        guard_trust_row.addWidget(self.varac_guard_allow_bbs_chk)
        guard_trust_row.addWidget(self.varac_guard_allow_trusted_chk)
        guard_trust_row.addStretch()
        vault_guard_v.addLayout(guard_trust_row)

        guard_dir_row = QHBoxLayout()
        guard_dir_row.setContentsMargins(0, 0, 0, 0)
        guard_dir_row.setSpacing(8)
        guard_dir_label = QLabel("Quarantine Dir")
        guard_dir_label.setFixedWidth(msg_label_width)
        guard_dir_row.addWidget(guard_dir_label)
        self.varac_guard_quarantine_dir_edit = QLineEdit()
        self.varac_guard_quarantine_dir_edit.setPlaceholderText("Optional quarantine folder for unauthorized files")
        guard_dir_row.addWidget(self.varac_guard_quarantine_dir_edit, 1)
        guard_dir_browse = QPushButton("Browse")
        guard_dir_browse.setFixedWidth(70)
        guard_dir_browse.clicked.connect(self._choose_varac_guard_quarantine_dir)
        guard_dir_row.addWidget(guard_dir_browse)
        self.varac_guard_retry_combo = QComboBox()
        self.varac_guard_retry_combo.addItems(["30", "60", "120", "300", "600"])
        self.varac_guard_retry_combo.setFixedWidth(92)
        guard_dir_row.addWidget(QLabel("Retry"))
        guard_dir_row.addWidget(self.varac_guard_retry_combo)
        guard_dir_row.addWidget(QLabel("sec"))
        guard_dir_row.addStretch()
        vault_guard_v.addLayout(guard_dir_row)

        guard_note_row = QHBoxLayout()
        guard_note_row.setContentsMargins(0, 0, 0, 0)
        self.varac_guard_note_label = QLabel(
            "VGuard watches VarAC incoming files and uses the allowed BBS callsigns list. It is separate from BBS access control."
        )
        self.varac_guard_note_label.setWordWrap(True)
        guard_note_row.addWidget(self.varac_guard_note_label, 1)
        vault_guard_v.addLayout(guard_note_row)

        guard_status_row = QHBoxLayout()
        guard_status_row.setContentsMargins(0, 0, 0, 0)
        self.varac_guard_status_label = QLabel("No VGuard scan yet.")
        self.varac_guard_status_label.setWordWrap(True)
        guard_status_row.addWidget(self.varac_guard_status_label, 1)
        vault_guard_v.addLayout(guard_status_row)

        self.varac_guard_enabled_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_guard_allow_bbs_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_guard_allow_trusted_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_guard_mode_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_guard_quarantine_dir_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_guard_retry_combo.currentIndexChanged.connect(self._mark_settings_dirty)

        varac_autofill_status_row = QHBoxLayout()
        varac_autofill_status_row.setContentsMargins(0, 0, 0, 0)
        varac_autofill_status_row.setSpacing(8)
        varac_autofill_status_row.addSpacing(msg_label_width)
        self.varac_autofill_status_label = QLabel("No auto-fill attempt yet.")
        self.varac_autofill_status_label.setWordWrap(True)
        self.varac_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self._autofill_status_labels["varac"] = self.varac_autofill_status_label
        varac_autofill_status_row.addWidget(self.varac_autofill_status_label, 1)
        self.varac_autofill_review_toggle_btn = QPushButton("Show Full Review")
        self.varac_autofill_review_toggle_btn.setVisible(False)
        self.varac_autofill_review_toggle_btn.clicked.connect(
            lambda _checked=False: self._toggle_autofill_review("varac")
        )
        self._autofill_status_buttons["varac"] = self.varac_autofill_review_toggle_btn
        varac_autofill_status_row.addWidget(self.varac_autofill_review_toggle_btn)
        varac_autofill_actions_widget = QWidget()
        varac_autofill_actions_row = QHBoxLayout()
        varac_autofill_actions_widget.setLayout(varac_autofill_actions_row)
        varac_autofill_actions_widget.setVisible(False)
        self._autofill_action_rows["varac"] = varac_autofill_actions_widget
        varac_autofill_actions_row.setContentsMargins(0, 0, 0, 0)
        varac_autofill_actions_row.setSpacing(8)
        varac_autofill_actions_row.addSpacing(msg_label_width)
        varac_autofill_actions_row.addStretch()
        self.varac_autofill_preserved_btn = QPushButton("Copy Suggestions")
        self.varac_autofill_preserved_btn.setVisible(False)
        self.varac_autofill_preserved_btn.clicked.connect(
            lambda _checked=False: self._copy_autofill_preserved_suggestions("varac")
        )
        self._autofill_preserved_buttons["varac"] = self.varac_autofill_preserved_btn
        varac_autofill_actions_row.addWidget(self.varac_autofill_preserved_btn)
        self.varac_autofill_replace_btn = QPushButton("Replace Suggested")
        self.varac_autofill_replace_btn.setVisible(False)
        self.varac_autofill_replace_btn.clicked.connect(
            lambda _checked=False: self._replace_autofill_preserved_suggestions("varac")
        )
        self._autofill_replace_buttons["varac"] = self.varac_autofill_replace_btn
        varac_autofill_actions_row.addWidget(self.varac_autofill_replace_btn)
        self.varac_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")
        self.varac_autofill_dismiss_btn.setVisible(False)
        self.varac_autofill_dismiss_btn.clicked.connect(
            lambda _checked=False: self._dismiss_autofill_preserved_suggestions("varac")
        )
        self._autofill_dismiss_buttons["varac"] = self.varac_autofill_dismiss_btn
        varac_autofill_actions_row.addWidget(self.varac_autofill_dismiss_btn)
        varac_v.addLayout(varac_autofill_status_row)
        varac_v.addWidget(varac_autofill_actions_widget)
        varac_v.addWidget(self._make_autofill_review_table("varac"))

        varac_cluster_mode_row = QHBoxLayout()
        varac_cluster_mode_row.setContentsMargins(0, 0, 0, 0)
        varac_cluster_mode_row.setSpacing(8)
        varac_cluster_mode_label = QLabel("Enable Cluster Mode")
        varac_cluster_mode_label.setFixedWidth(msg_label_width)
        varac_cluster_mode_row.addWidget(varac_cluster_mode_label)
        self.varac_cluster_mode_chk = QCheckBox("Show VarAC cluster configuration")
        self.varac_cluster_mode_chk.setToolTip(
            "Enable this only when you want multiple radio profiles to participate in coordinated VarAC cluster workflows."
        )
        varac_cluster_mode_row.addWidget(self.varac_cluster_mode_chk)
        varac_cluster_mode_row.addStretch()
        varac_v.addLayout(varac_cluster_mode_row)

        varac_cluster_mode_hint_row = QHBoxLayout()
        varac_cluster_mode_hint_row.setContentsMargins(0, 0, 0, 0)
        varac_cluster_mode_hint_row.setSpacing(8)
        varac_cluster_mode_hint_row.addSpacing(msg_label_width)
        self.varac_cluster_mode_hint_label = QLabel()
        self.varac_cluster_mode_hint_label.setWordWrap(True)
        varac_cluster_mode_hint_row.addWidget(self.varac_cluster_mode_hint_label, 1)
        varac_v.addLayout(varac_cluster_mode_hint_row)

        varac_container = QWidget()
        varac_container.setLayout(varac_v)
        varac_group = self._make_collapsible_group(
            "VarAC Settings",
            varac_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.varac",
        )
        self._register_collapsible_group(varac_group, self._summary_varac_settings)
        self._set_section_health_key(varac_group, "varac")
        varac_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.varac_section_group = varac_group
        self._add_settings_section(varac_group, scope="radio")
        self._add_settings_section(self.varac_clusters_section_group, scope="radio")
        self._add_settings_section(self.varac_memberships_section_group, scope="radio")
        self.message_auth_section_group = gpg_group

        custom_tools_group = QGroupBox("Custom Tools")
        custom_tools_v = QVBoxLayout()
        custom_tools_v.setSpacing(6)
        custom_tools_group.setLayout(custom_tools_v)

        custom_tools_hint = QLabel(
            "Add named launch commands for helper scripts and tools. "
            "Configured custom tools also appear in Launch Control."
        )
        custom_tools_hint.setWordWrap(True)
        custom_tools_v.addWidget(custom_tools_hint)
        self.custom_tools_scope_label = QLabel("Editing Radio: --")
        self.custom_tools_scope_label.setWordWrap(True)
        custom_tools_v.addWidget(self.custom_tools_scope_label)

        self.custom_tools_table = QTableWidget(0, 2)
        self.custom_tools_table.setHorizontalHeaderLabels(["Name", "Launch Command"])
        self.custom_tools_table.verticalHeader().setVisible(False)
        self.custom_tools_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.custom_tools_table.setSelectionMode(QTableWidget.SingleSelection)
        custom_tools_header = self.custom_tools_table.horizontalHeader()
        custom_tools_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        custom_tools_header.setSectionResizeMode(1, QHeaderView.Stretch)
        custom_tools_v.addWidget(self.custom_tools_table)

        custom_tools_btn_row = QHBoxLayout()
        self.custom_tool_add_btn = QPushButton("Add")
        self.custom_tool_edit_btn = QPushButton("Edit")
        self.custom_tool_remove_btn = QPushButton("Remove")
        self.custom_tool_up_btn = QPushButton("Up")
        self.custom_tool_down_btn = QPushButton("Down")
        custom_tools_btn_row.addWidget(self.custom_tool_add_btn)
        custom_tools_btn_row.addWidget(self.custom_tool_edit_btn)
        custom_tools_btn_row.addWidget(self.custom_tool_remove_btn)
        custom_tools_btn_row.addSpacing(12)
        custom_tools_btn_row.addWidget(self.custom_tool_up_btn)
        custom_tools_btn_row.addWidget(self.custom_tool_down_btn)
        custom_tools_btn_row.addStretch()
        custom_tools_v.addLayout(custom_tools_btn_row)

        self.custom_tools_summary_label = QLabel("No custom tools configured.")
        custom_tools_v.addWidget(self.custom_tools_summary_label)

        self.custom_tool_add_btn.clicked.connect(self._add_custom_tool)
        self.custom_tool_edit_btn.clicked.connect(self._edit_custom_tool)
        self.custom_tool_remove_btn.clicked.connect(self._remove_custom_tool)
        self.custom_tool_up_btn.clicked.connect(lambda: self._move_custom_tool(-1))
        self.custom_tool_down_btn.clicked.connect(lambda: self._move_custom_tool(1))
        self.custom_tools_table.itemSelectionChanged.connect(self._update_custom_tool_buttons)
        self.custom_tools_table.itemDoubleClicked.connect(lambda *_args: self._edit_custom_tool())

        custom_tools_container = QWidget()
        custom_tools_container.setLayout(custom_tools_v)
        custom_tools_group = self._make_collapsible_group(
            "Custom Tools",
            custom_tools_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.custom-tools",
        )
        self._register_collapsible_group(custom_tools_group, self._summary_custom_tools)
        custom_tools_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.custom_tools_section_group = custom_tools_group
        self._add_settings_section(custom_tools_group, scope="radio")

        # Launch Control
        launch_group = QGroupBox("Launch Control")
        launch_v = QVBoxLayout()
        launch_v.setSpacing(8)
        launch_group.setLayout(launch_v)

        self.launch_control_scope_label = QLabel("Editing Radio: --")
        self.launch_control_scope_label.setWordWrap(True)
        launch_v.addWidget(self.launch_control_scope_label)

        (
            self.launch_guidance_card,
            self.launch_guidance_title_label,
            self.launch_guidance_status_label,
        ) = _make_support_card("Projected Launch Bundle", "launchControlGuidanceStatus")
        launch_v.addWidget(self.launch_guidance_card)

        self.launch_hint_label = QLabel()
        self.launch_hint_label.setWordWrap(True)

        launch_global_row = QHBoxLayout()
        launch_global_row.setContentsMargins(0, 0, 0, 0)
        launch_global_row.setSpacing(8)
        launch_global_row.addWidget(self.launch_hint_label, 1)
        self.launch_all_with_startup_chk = QCheckBox("Launch All with FreqInOut")
        launch_global_row.addWidget(self.launch_all_with_startup_chk)
        launch_v.addLayout(launch_global_row)

        self.launch_control_table = QTableWidget(0, 3)
        self.launch_control_table.setHorizontalHeaderLabels(["Application", "Enabled", "Launch on Startup"])
        self.launch_control_table.verticalHeader().setVisible(False)
        self.launch_control_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.launch_control_table.setSelectionMode(QTableWidget.SingleSelection)
        launch_header = self.launch_control_table.horizontalHeader()
        launch_header.setSectionResizeMode(0, QHeaderView.Stretch)
        launch_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        launch_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.launch_control_table.setMinimumHeight(150)
        self.launch_control_table.setMaximumHeight(260)
        self.launch_control_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        launch_v.addWidget(self.launch_control_table)

        launch_actions_grid = QGridLayout()
        launch_actions_grid.setContentsMargins(0, 0, 0, 0)
        launch_actions_grid.setHorizontalSpacing(8)
        launch_actions_grid.setVerticalSpacing(6)
        self.launch_order_up_btn = QPushButton("Up")
        self.launch_order_down_btn = QPushButton("Down")
        self.launch_reset_order_btn = QPushButton("Reset Default Order")
        self.launch_configured_now_btn = QPushButton("Launch Configured Now")
        self.launch_stop_btn = QPushButton("Stop Launch Sequence")
        self.launch_stop_btn.setEnabled(False)
        launch_actions_grid.addWidget(QLabel("Order:"), 0, 0)
        launch_actions_grid.addWidget(self.launch_order_up_btn, 0, 1)
        launch_actions_grid.addWidget(self.launch_order_down_btn, 0, 2)
        launch_actions_grid.addWidget(self.launch_reset_order_btn, 0, 3)
        launch_actions_grid.addWidget(QLabel("Run:"), 1, 0)
        launch_actions_grid.addWidget(self.launch_configured_now_btn, 1, 1, 1, 2)
        launch_actions_grid.addWidget(self.launch_stop_btn, 1, 3)
        launch_actions_grid.setColumnStretch(4, 1)
        launch_v.addLayout(launch_actions_grid)

        self.launch_summary_label = QLabel("Launch status: Idle")
        self.launch_summary_label.setWordWrap(True)
        launch_v.addWidget(self.launch_summary_label)

        self.launch_order_up_btn.clicked.connect(lambda: self._move_launch_row(-1))
        self.launch_order_down_btn.clicked.connect(lambda: self._move_launch_row(1))
        self.launch_reset_order_btn.clicked.connect(self._reset_launch_order)
        self.launch_configured_now_btn.clicked.connect(self._launch_configured_now)
        self.launch_stop_btn.clicked.connect(self._stop_launch_sequence)
        self.launch_all_with_startup_chk.stateChanged.connect(self._refresh_section_titles)
        self.launch_control_table.itemChanged.connect(self._on_launch_table_item_changed)
        self.launch_control_table.itemSelectionChanged.connect(self._update_launch_control_buttons)
        self.launch_orchestrator.sequence_started.connect(self._on_launch_sequence_started)
        self.launch_orchestrator.sequence_progress.connect(self._on_launch_sequence_progress)
        self.launch_orchestrator.sequence_finished.connect(self._on_launch_sequence_finished)
        self.varac_cluster_mode_chk.stateChanged.connect(self._on_varac_cluster_mode_toggled)

        launch_container = QWidget()
        launch_container.setLayout(launch_v)
        launch_group = self._make_collapsible_group(
            "Launch Control",
            launch_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.launch-control",
        )
        self._register_collapsible_group(launch_group, self._summary_launch_control)
        launch_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.launch_control_section_group = launch_group
        self._add_settings_section(launch_group, scope="radio")

        # SOP Export
        sop_export_v = QVBoxLayout()
        sop_export_v.setSpacing(6)
        sop_export_hint = QLabel(
            "Optional text is inserted before and after SOP PDF export tables. "
            "Supported placeholders: {{operator_callsign}}, {{as_of_local}}, {{as_of_utc}}, {{scope}}, {{timezone}}."
        )
        sop_export_hint.setWordWrap(True)
        sop_export_v.addWidget(sop_export_hint)

        sop_preamble_label = QLabel("Preamble")
        self.sop_export_preamble_edit = QPlainTextEdit()
        self.sop_export_preamble_edit.setPlaceholderText(
            "Optional introduction for SOP PDF exports.\n\n"
            "Example:\n"
            "This SOP is effective as of {{as_of_local}} for {{operator_callsign}}."
        )
        self.sop_export_preamble_edit.setTabChangesFocus(True)
        self.sop_export_preamble_edit.setMinimumHeight(110)
        sop_export_v.addWidget(sop_preamble_label)
        sop_export_v.addWidget(self.sop_export_preamble_edit)

        sop_postamble_label = QLabel("Postamble")
        self.sop_export_postamble_edit = QPlainTextEdit()
        self.sop_export_postamble_edit.setPlaceholderText(
            "Optional closing notes, reminders, or document handling instructions."
        )
        self.sop_export_postamble_edit.setTabChangesFocus(True)
        self.sop_export_postamble_edit.setMinimumHeight(110)
        sop_export_v.addWidget(sop_postamble_label)
        sop_export_v.addWidget(self.sop_export_postamble_edit)

        self.sop_export_preamble_edit.textChanged.connect(self._on_sop_export_text_changed)
        self.sop_export_postamble_edit.textChanged.connect(self._on_sop_export_text_changed)

        sop_export_container = QWidget()
        sop_export_container.setLayout(sop_export_v)
        sop_export_group = self._make_collapsible_group(
            "SOP Export",
            sop_export_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.sop-export",
        )
        self._register_collapsible_group(sop_export_group, self._summary_sop_export)
        sop_export_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(sop_export_group, scope="global")
        self._add_settings_section(self.message_auth_section_group, scope="global")
        self._add_settings_section(logging_section, scope="global")

        # bottom save
        bottom_row = QHBoxLayout()
        self.settings_action_feedback_label = QLabel("Settings ready.")
        self.settings_action_feedback_label.setWordWrap(True)
        bottom_row.addWidget(self.settings_action_feedback_label, 1)
        self.save_btn = QPushButton("Save Settings")
        self.save_btn.clicked.connect(self._save_settings_button)
        bottom_row.addWidget(self.save_btn)
        main_layout.addLayout(bottom_row)
        self._wire_dirty_tracking()
        self._refresh_launch_control_table()
        self._set_save_button_state("success")
        self._refresh_section_titles()
        self._apply_settings_nav_scope_visibility()
        self._refresh_radio_specific_section_visibility()
        self._select_first_visible_settings_section()
        self._update_sections_nav_size()
        self._apply_accessibility_width_guards()

    def _make_collapsible_group(
        self,
        title: str,
        content: QWidget,
        *,
        checked: bool,
        fit_content: bool,
        fit_content_in_stack: bool = False,
        help_context_key: str | None = None,
    ) -> QGroupBox:
        group = QGroupBox()
        group.setMinimumHeight(0)
        content.setVisible(checked)

        header_btn = QToolButton()
        header_btn.setCheckable(True)
        header_btn.setChecked(checked)
        header_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        header_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        header_btn.setText(title)
        header_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        header_btn.setMinimumHeight(28)
        header_btn.setStyleSheet(self._section_header_style("neutral", resolve_theme(self.settings)))

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.addWidget(header_btn, 1)
        if help_context_key:
            header_row.addWidget(
                self._make_context_help_button(
                    help_context_key,
                    tooltip=f"Open help for {title}.",
                )
            )

        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 12)
        layout.setSpacing(6)
        layout.addLayout(header_row)
        layout.addWidget(content)
        group.setLayout(layout)

        header_btn.toggled.connect(lambda state, g=group, w=content: self._on_section_toggled(g, w, state))
        self._section_meta[group] = {
            **self._section_meta.get(group, {}),
            "fit_content": fit_content,
            "fit_content_in_stack": bool(fit_content_in_stack),
            "title": title,
            "header_btn": header_btn,
            "content": content,
            "help_context_key": str(help_context_key or "").strip().lower(),
        }
        self._apply_collapsed_state(group, content, checked)
        QTimer.singleShot(0, lambda g=group, w=content: self._apply_collapsed_state(g, w, header_btn.isChecked()))
        return group

    def _register_collapsible_group(self, group: QGroupBox, summary_fn) -> None:
        self._accordion_groups.append(group)
        meta = self._section_meta.get(group, {})
        meta.update({"summary_fn": summary_fn})
        self._section_meta[group] = meta

    def _add_settings_section(self, group: QGroupBox, *, scope: str = "radio") -> None:
        meta = self._section_meta.get(group, {})
        title = str(meta.get("title", group.title() if hasattr(group, "title") else "Section"))
        item = QListWidgetItem(title)
        stack_index = self.sections_stack.count()
        normalized_scope = str(scope or "radio").strip().lower()
        item.setData(self.SECTION_STACK_INDEX_ROLE, stack_index)
        item.setData(self.SECTION_SCOPE_ROLE, normalized_scope)
        self.sections_nav_list.addItem(item)
        self._section_nav_items[group] = item
        meta["scope"] = normalized_scope
        meta.setdefault("section_visible", True)
        self._section_meta[group] = meta
        self.sections_stack.addWidget(group)
        btn = QPushButton(title)
        btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        btn.setMinimumHeight(30)
        btn.setAccessibleName(f"Settings navigation: {title}")
        btn.clicked.connect(lambda _checked=False, g=group: self._select_settings_section_group(g))
        target_layout = (
            self.global_section_buttons_layout
            if normalized_scope == "global" and hasattr(self, "global_section_buttons_layout")
            else self.radio_section_buttons_layout
            if hasattr(self, "radio_section_buttons_layout")
            else None
        )
        if target_layout is not None:
            target_layout.addWidget(btn)
        self._section_nav_buttons[group] = btn
        if hasattr(self, "settings_section_combo"):
            self.settings_section_combo.addItem(self._settings_section_combo_label(group), stack_index)
        content = meta.get("content")
        header_btn = meta.get("header_btn")
        if isinstance(content, QWidget):
            expanded = bool(header_btn.isChecked()) if header_btn else True
            self._apply_collapsed_state(group, content, expanded)
        self._apply_settings_nav_scope_visibility()
        self._refresh_settings_nav_button_styles()
        self._update_sections_nav_size()

    def _select_settings_section_group(self, group: QGroupBox | None) -> None:
        if group is None or group not in self._section_meta:
            return
        try:
            stack_index = self.sections_stack.indexOf(group)
        except Exception:
            stack_index = -1
        if stack_index < 0:
            return
        self.sections_stack.setCurrentIndex(stack_index)
        nav_item = self._section_nav_items.get(group)
        if nav_item is not None and not nav_item.isHidden():
            row = self.sections_nav_list.row(nav_item)
            if row >= 0:
                was_blocked = self.sections_nav_list.blockSignals(True)
                try:
                    self.sections_nav_list.setCurrentRow(row)
                finally:
                    self.sections_nav_list.blockSignals(was_blocked)
        self._sync_current_section_scroll_size()
        self._reset_sections_scroll_to_top()
        self._refresh_settings_nav_button_styles()
        self._sync_settings_section_combo_to_group(group)

    def _settings_section_combo_label(self, group: QGroupBox) -> str:
        meta = self._section_meta.get(group, {})
        title = str(meta.get("title", group.title() if hasattr(group, "title") else "Section")).strip() or "Section"
        scope = str(meta.get("scope", "radio") or "radio").strip().lower()
        prefix = "Global" if scope == "global" else "Selected Radio"
        return f"{prefix}: {title}"

    def _sync_settings_section_combo_to_group(self, group: QGroupBox | None) -> None:
        combo = getattr(self, "settings_section_combo", None)
        if combo is None or group is None:
            return
        try:
            stack_index = self.sections_stack.indexOf(group)
        except Exception:
            stack_index = -1
        if stack_index < 0:
            return
        self._refreshing_settings_section_combo = True
        try:
            for idx in range(combo.count()):
                try:
                    if int(combo.itemData(idx) or -1) == stack_index:
                        combo.setCurrentIndex(idx)
                        break
                except Exception:
                    continue
        finally:
            self._refreshing_settings_section_combo = False

    def _refresh_settings_section_combo(self) -> None:
        combo = getattr(self, "settings_section_combo", None)
        if combo is None or not hasattr(self, "sections_stack"):
            return
        current_widget = self.sections_stack.currentWidget()
        self._refreshing_settings_section_combo = True
        try:
            combo.clear()
            for group, meta in self._section_meta.items():
                if not bool(meta.get("section_visible", True)):
                    continue
                stack_index = self.sections_stack.indexOf(group)
                if stack_index < 0:
                    continue
                combo.addItem(self._settings_section_combo_label(group), stack_index)
            if isinstance(current_widget, QGroupBox):
                self._sync_settings_section_combo_to_group(current_widget)
        finally:
            self._refreshing_settings_section_combo = False

    def _on_settings_section_combo_changed(self, index: int) -> None:
        if bool(getattr(self, "_refreshing_settings_section_combo", False)) or index < 0:
            return
        combo = getattr(self, "settings_section_combo", None)
        if combo is None:
            return
        try:
            stack_index = int(combo.itemData(index) or -1)
        except Exception:
            stack_index = -1
        if stack_index < 0 or stack_index >= self.sections_stack.count():
            return
        group = self.sections_stack.widget(stack_index)
        if isinstance(group, QGroupBox):
            self._select_settings_section_group(group)

    def _on_global_settings_toggle(self, checked: bool) -> None:
        self._global_settings_nav_collapsed = not bool(checked)
        if checked and hasattr(self, "radio_settings_toggle_btn"):
            self._radio_settings_nav_collapsed = True
            self.radio_settings_toggle_btn.setChecked(False)
            self.radio_settings_toggle_btn.setArrowType(Qt.RightArrow)
            if hasattr(self, "radio_section_buttons_widget"):
                self.radio_section_buttons_widget.setVisible(False)
        if hasattr(self, "global_settings_toggle_btn"):
            self.global_settings_toggle_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        if hasattr(self, "global_section_buttons_widget"):
            self.global_section_buttons_widget.setVisible(bool(checked))
        self._apply_settings_nav_scope_visibility()
        self._refresh_settings_nav_button_styles()

    def _on_radio_settings_toggle(self, checked: bool) -> None:
        self._radio_settings_nav_collapsed = not bool(checked)
        if checked and hasattr(self, "global_settings_toggle_btn"):
            self._global_settings_nav_collapsed = True
            self.global_settings_toggle_btn.setChecked(False)
            self.global_settings_toggle_btn.setArrowType(Qt.RightArrow)
            if hasattr(self, "global_section_buttons_widget"):
                self.global_section_buttons_widget.setVisible(False)
        if hasattr(self, "radio_settings_toggle_btn"):
            self.radio_settings_toggle_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        if hasattr(self, "radio_section_buttons_widget"):
            self.radio_section_buttons_widget.setVisible(bool(checked))
        self._apply_settings_nav_scope_visibility()
        self._refresh_settings_nav_button_styles()

    def _apply_settings_nav_scope_visibility(self) -> None:
        if not hasattr(self, "sections_nav_list"):
            return
        hide_global = bool(self._global_settings_nav_collapsed)
        hide_radio = bool(self._radio_settings_nav_collapsed)
        for group, item in self._section_nav_items.items():
            if item is None:
                continue
            scope = str(self._section_meta.get(group, {}).get("scope", item.data(self.SECTION_SCOPE_ROLE) or "radio")).strip().lower()
            section_visible = bool(self._section_meta.get(group, {}).get("section_visible", True))
            if scope == "global":
                item.setHidden(hide_global or not section_visible)
                btn = self._section_nav_buttons.get(group)
                if btn is not None:
                    btn.setVisible(not hide_global and section_visible)
            else:
                item.setHidden(hide_radio or not section_visible)
                btn = self._section_nav_buttons.get(group)
                if btn is not None:
                    btn.setVisible(not hide_radio and section_visible)
        current_widget = self.sections_stack.currentWidget() if hasattr(self, "sections_stack") else None
        if (
            hide_global
            and isinstance(current_widget, QGroupBox)
            and str(self._section_meta.get(current_widget, {}).get("scope", "")).strip().lower() == "global"
        ):
            self._select_first_visible_settings_section()
        if (
            hide_radio
            and isinstance(current_widget, QGroupBox)
            and str(self._section_meta.get(current_widget, {}).get("scope", "")).strip().lower() != "global"
        ):
            self._select_first_visible_settings_section()
        self._update_sections_nav_size()
        self._refresh_settings_section_combo()

    def _refresh_settings_nav_button_styles(self) -> None:
        theme = resolve_theme(self.settings)
        current_widget = self.sections_stack.currentWidget() if hasattr(self, "sections_stack") else None
        for group, btn in self._section_nav_buttons.items():
            if btn is None:
                continue
            role = "primary" if group is current_widget else "secondary"
            meta = self._section_meta.get(group, {})
            section_visible = bool(meta.get("section_visible", True))
            scope = str(meta.get("scope", "")).strip().lower()
            if scope == "global":
                nav_visible = section_visible and not bool(self._global_settings_nav_collapsed)
            else:
                nav_visible = section_visible and not bool(self._radio_settings_nav_collapsed)
            btn.setVisible(nav_visible)
            btn.setStyleSheet(self._settings_nav_button_style(role, theme))
        if hasattr(self, "global_settings_toggle_btn"):
            self.global_settings_toggle_btn.setStyleSheet(
                self._settings_nav_button_style(self._settings_nav_group_toggle_role("global"), theme)
            )
        if hasattr(self, "radio_settings_toggle_btn"):
            self.radio_settings_toggle_btn.setStyleSheet(
                self._settings_nav_button_style(self._settings_nav_group_toggle_role("radio"), theme)
            )

    def _settings_nav_group_toggle_role(self, scope: str) -> str:
        normalized = str(scope or "").strip().lower()
        if normalized == "global":
            return "secondary" if bool(getattr(self, "_global_settings_nav_collapsed", True)) else "eligible_info"
        return "secondary" if bool(getattr(self, "_radio_settings_nav_collapsed", False)) else "eligible_info"

    @staticmethod
    def _settings_nav_button_style(role: str, theme: Dict[str, str]) -> str:
        return (
            button_style(role, theme)
            + " QPushButton, QToolButton {"
            " text-align: left;"
            " padding-left: 10px;"
            " padding-right: 8px;"
            "}"
            " QToolButton {"
            " padding-left: 8px;"
            " padding-right: 10px;"
            "}"
        )

    def _set_settings_section_visible(self, group: QGroupBox | None, visible: bool) -> None:
        if group is None:
            return
        meta = self._section_meta.get(group, {})
        meta["section_visible"] = bool(visible)
        self._section_meta[group] = meta
        group.setVisible(bool(visible))
        nav_item = self._section_nav_items.get(group)
        if nav_item is not None:
            nav_item.setHidden(not bool(visible))
        nav_btn = self._section_nav_buttons.get(group)
        if nav_btn is not None:
            scope = str(self._section_meta.get(group, {}).get("scope", "")).strip().lower()
            if scope == "global":
                scope_visible = not bool(self._global_settings_nav_collapsed)
            else:
                scope_visible = not bool(self._radio_settings_nav_collapsed)
            nav_btn.setVisible(bool(visible) and scope_visible)
        current_widget = self.sections_stack.currentWidget() if hasattr(self, "sections_stack") else None
        if not visible and current_widget is group:
            self._select_first_visible_settings_section()
        self._refresh_settings_nav_button_styles()
        self._update_sections_nav_size()
        self._refresh_settings_section_combo()

    def _select_first_visible_settings_section(self) -> None:
        if not hasattr(self, "sections_nav_list"):
            return
        for row in range(self.sections_nav_list.count()):
            item = self.sections_nav_list.item(row)
            if item is None or item.isHidden():
                continue
            self.sections_nav_list.setCurrentRow(row)
            return

    def _varac_cluster_mode_enabled(self) -> bool:
        return bool(hasattr(self, "varac_cluster_mode_chk") and self.varac_cluster_mode_chk.isChecked())

    def _on_varac_cluster_mode_toggled(self, _state: int) -> None:
        if self._loading_settings:
            self._refresh_varac_cluster_mode_ui(refresh_tables=False)
            return
        self._refresh_varac_cluster_mode_ui(refresh_tables=True)
        self._update_device_profiles_hint()
        self._mark_settings_dirty()

    def _refresh_varac_cluster_mode_ui(self, *, refresh_tables: bool = True) -> None:
        enabled = self._varac_cluster_mode_enabled()
        hint_label = getattr(self, "varac_cluster_mode_hint_label", None)
        if hint_label is not None:
            if enabled:
                hint_label.setText(
                    "Cluster mode is enabled. VarAC Clusters and VarAC Memberships are shown below VarAC Settings so coordinated multi-radio VarAC routing can be configured."
                )
            else:
                has_saved_clusters = bool(self.varac_clusters or self.varac_cluster_members)
                preserved_note = " Existing cluster definitions are preserved." if has_saved_clusters else ""
                hint_label.setText(
                    "Cluster mode is off. Most operators should leave this off unless they are intentionally coordinating multiple radios or VarAC instances together."
                    + preserved_note
                )
        selected_profile = self._selected_settings_radio_profile()
        varac_visible = bool(isinstance(selected_profile, dict) and self._radio_software_enabled(selected_profile, "varac"))
        self._set_settings_section_visible(getattr(self, "varac_clusters_section_group", None), enabled and varac_visible)
        self._set_settings_section_visible(getattr(self, "varac_memberships_section_group", None), enabled and varac_visible)
        if refresh_tables:
            self._refresh_varac_clusters_table(refresh_memberships=True, refresh_section_titles=False)
        else:
            self._update_varac_clusters_hint()
            self._update_varac_memberships_hint()
            self._update_varac_cluster_guidance_detail()
            self._update_varac_membership_guidance_detail()
        self._refresh_section_titles()

    def _set_section_health_key(self, group: QGroupBox, health_key: str) -> None:
        meta = self._section_meta.get(group, {})
        meta["health_key"] = str(health_key or "").strip().lower()
        self._section_meta[group] = meta

    def _section_header_style(self, state: str, theme: Dict[str, str]) -> str:
        state = str(state or "neutral").strip().lower()
        if state == "warn":
            border = theme.get("warning", "#C99700")
            fg = border
            bg = theme.get("surface", "#ffffff")
            hover_bg = theme.get("surface_alt", bg)
            font_weight = "700"
        else:
            border = "transparent"
            fg = theme.get("text", "#222222")
            bg = "transparent"
            hover_bg = theme.get("surface_alt", theme.get("surface", "#f2f2f2"))
            font_weight = "600"
        return (
            "QToolButton {"
            " padding: 4px 6px;"
            f" font-weight: {font_weight};"
            f" color: {fg};"
            f" background: {bg};"
            f" border: 1px solid {border};"
            " border-radius: 6px;"
            " text-align: left;"
            "}"
            " QToolButton:hover {"
            f" background: {hover_bg};"
            f" border: 1px solid {border if state == 'warn' else theme.get('border', '#cccccc')};"
            "}"
        )

    def _section_nav_visuals(
        self,
        state: str,
        *,
        selected: bool,
        hovered: bool,
        theme: Dict[str, str],
    ) -> Dict[str, object]:
        state = str(state or "neutral").strip().lower()
        text_color = QColor(theme.get("text", "#222222"))
        transparent = QColor(0, 0, 0, 0)

        if state == "warn":
            border = QColor(theme.get("warning", "#C99700"))
            bg = QColor(border)
            bg.setAlpha(120 if selected else (84 if hovered else 58))
            return {"bg": bg, "border": border, "fg": text_color, "bold": True}

        if selected:
            border = QColor(theme.get("accent", "#2a6fd3"))
            bg = QColor(border)
            bg.setAlpha(64 if theme.get("bg") == "#E6E8EA" else 92)
            return {"bg": bg, "border": border, "fg": text_color, "bold": True}

        if hovered:
            border = QColor(theme.get("accent", "#2a6fd3"))
            bg = QColor(theme.get("surface", "#ffffff"))
            return {"bg": bg, "border": border, "fg": text_color, "bold": False}

        return {"bg": transparent, "border": transparent, "fg": text_color, "bold": False}

    def _apply_sections_nav_style(self) -> None:
        if not hasattr(self, "sections_nav_list"):
            return
        theme = resolve_theme(self.settings)
        self.sections_nav_list.setStyleSheet(
            "QListWidget {"
            f" background: {theme.get('surface_alt', theme.get('surface', '#f2f2f2'))};"
            f" border: 1px solid {theme.get('border', '#cccccc')};"
            f" color: {theme.get('text', '#222222')};"
            "}"
        )
        self.sections_nav_list.viewport().update()

    def _update_sections_nav_size(self) -> None:
        if not hasattr(self, "sections_nav_list"):
            return
        visible_items = [
            self.sections_nav_list.item(i)
            for i in range(self.sections_nav_list.count())
            if self.sections_nav_list.item(i) is not None and not self.sections_nav_list.item(i).isHidden()
        ]
        count = len(visible_items)
        if count <= 0:
            return
        row_h = self.sections_nav_list.sizeHintForRow(0)
        if row_h <= 0:
            row_h = 28
        frame = self.sections_nav_list.frameWidth()
        target = (row_h * count) + (frame * 2) + 6
        self.sections_nav_list.setFixedHeight(max(120, target))
        try:
            col_hint = int(self.sections_nav_list.sizeHintForColumn(0))
        except Exception:
            col_hint = 0
        if col_hint <= 0:
            try:
                fm = self.sections_nav_list.fontMetrics()
                col_hint = max(
                    (int(fm.horizontalAdvance(item.text())) for item in visible_items),
                    default=140,
                )
            except Exception:
                col_hint = 140
        width = max(170, min(col_hint + (frame * 2) + 26, 300))
        self.sections_nav_list.setMinimumWidth(width)
        self.sections_nav_list.setMaximumWidth(width)

    def _on_section_nav_changed(self, row: int) -> None:
        if row < 0:
            return
        item = self.sections_nav_list.item(row) if hasattr(self, "sections_nav_list") else None
        if item is None or item.isHidden():
            return
        try:
            stack_index = int(item.data(self.SECTION_STACK_INDEX_ROLE) or -1)
        except Exception:
            stack_index = -1
        if stack_index < 0 or stack_index >= self.sections_stack.count():
            return
        with perf_span("settings.section_switch", settings=self.settings, min_ms=10.0):
            self.sections_stack.setCurrentIndex(stack_index)
            self._sync_current_section_scroll_size()
            self._reset_sections_scroll_to_top()
            self._apply_sections_nav_style()
            self._refresh_settings_nav_button_styles()
            try:
                page = self.sections_stack.currentWidget()
                if isinstance(page, QGroupBox):
                    header_btn = self._section_meta.get(page, {}).get("header_btn")
                    # In stacked mode, keep headers expanded without firing toggle handlers.
                    if header_btn and not header_btn.isChecked():
                        header_btn.blockSignals(True)
                        try:
                            header_btn.setChecked(True)
                        finally:
                            header_btn.blockSignals(False)
                    title = str(self._section_meta.get(page, {}).get("title", "")).strip().lower()
                    if (
                        title == "message auth (key/hash)"
                        and not bool(self._gpg_keys_loaded)
                        and not bool(self._gpg_keys_auto_probe_attempted)
                        and not bool(self._gpg_keys_table_loading)
                        and hasattr(self, "gpg_keys_table")
                    ):
                        self._gpg_keys_auto_probe_attempted = True
                        QTimer.singleShot(0, lambda: self._refresh_gpg_keys_table(show_dialog_on_error=False))
            except Exception:
                pass

    def _reset_sections_scroll_to_top(self) -> None:
        scroll = getattr(self, "sections_scroll", None)
        if scroll is None:
            return

        def _reset() -> None:
            try:
                scroll.verticalScrollBar().setValue(0)
                scroll.horizontalScrollBar().setValue(0)
            except Exception:
                pass

        _reset()
        QTimer.singleShot(0, _reset)

    def _sync_current_section_scroll_size(self) -> None:
        if not hasattr(self, "sections_stack"):
            return
        page = self.sections_stack.currentWidget()
        if page is None:
            return
        try:
            target_h = max(0, int(page.sizeHint().height()))
            row = int(self.sections_stack.currentIndex())
            if row != int(self._last_section_stack_index) or target_h != int(self._last_section_target_height):
                page.setMinimumHeight(target_h)
                self.sections_stack.setMinimumHeight(target_h)
                self._last_section_stack_index = row
                self._last_section_target_height = target_h
        except Exception:
            pass

    def _on_section_toggled(self, group: QGroupBox, content: QWidget, checked: bool) -> None:
        stacked_mode = hasattr(self, "sections_stack") and self.sections_stack.count() > 0
        if stacked_mode:
            header_btn = self._section_meta.get(group, {}).get("header_btn")
            if not checked and header_btn:
                QTimer.singleShot(0, lambda btn=header_btn: btn.setChecked(True))
            # Collapsible behavior is disabled while using stacked section navigation.
            self._apply_collapsed_state(group, content, True)
            return
        self._apply_collapsed_state(group, content, checked)
        if checked and not stacked_mode:
            for other in self._accordion_groups:
                if other is not group:
                    other_btn = self._section_meta.get(other, {}).get("header_btn")
                    if other_btn and other_btn.isChecked():
                        other_btn.setChecked(False)
        self._refresh_section_titles()

    def _refresh_section_titles(self) -> None:
        stacked_mode = hasattr(self, "sections_stack") and self.sections_stack.count() > 0
        if stacked_mode:
            for group, meta in self._section_meta.items():
                base = str(meta.get("title", ""))
                header_btn = meta.get("header_btn")
                if header_btn and header_btn.text() != base:
                    header_btn.setText(base)
                nav_item = self._section_nav_items.get(group)
                if nav_item:
                    if nav_item.text() != base:
                        nav_item.setText(base)
                    nav_item.setToolTip(base)
            self._update_sections_nav_size()
            self._refresh_settings_section_combo()
            self._refresh_section_nav_health()
            return
        for group, meta in self._section_meta.items():
            base = str(meta.get("title", ""))
            summary_fn = meta.get("summary_fn")
            header_btn = meta.get("header_btn")
            if header_btn and header_btn.isChecked():
                if header_btn:
                    header_btn.setText(base)
                continue
            summary = ""
            try:
                if summary_fn:
                    summary = str(summary_fn()).strip()
            except Exception:
                summary = ""
            if header_btn:
                header_btn.setText(f"{base} — {summary}" if summary else base)
            nav_item = self._section_nav_items.get(group)
            if nav_item:
                nav_item.setText(base)
                nav_item.setToolTip(summary if summary else base)
        self._update_sections_nav_size()
        self._refresh_settings_section_combo()
        self._refresh_section_nav_health()

    def _build_section_health_entry(self, *, engaged: bool, issues: List[str]) -> Dict[str, str]:
        detail = "; ".join(str(issue).strip() for issue in issues if str(issue).strip())
        if detail:
            return {"state": "warn", "detail": detail}
        if engaged:
            return {"state": "ok", "detail": ""}
        return {"state": "neutral", "detail": ""}

    def _build_section_health_snapshot(self) -> Dict[str, Dict[str, str]]:
        snapshot: Dict[str, Dict[str, str]] = {}
        readiness = self._current_station_readiness_report()
        radio_profile_issues = [
            issue.message
            for issue in readiness.issues
            if issue.section_key == "radio_profiles" and issue.severity in {"required", "recommended"}
        ]

        freqinout_issues: List[str] = []
        callsign = self.callsign_edit.text().strip().upper() if hasattr(self, "callsign_edit") else ""
        grid = self.grid6_edit.text().strip().upper() if hasattr(self, "grid6_edit") else ""
        if not callsign:
            freqinout_issues.append("Callsign missing")
        if not grid:
            freqinout_issues.append("Grid missing")
        prompt_pairs = [
            ("Frequency", getattr(self, "freq_enforce_combo", None), getattr(self, "freq_prompt_combo", None)),
            ("FLDigi", getattr(self, "fldigi_enforce_combo", None), getattr(self, "fldigi_prompt_combo", None)),
            ("JS8Call", getattr(self, "js8_enforce_combo", None), getattr(self, "js8_prompt_combo", None)),
        ]
        for label, mode_combo, prompt_combo in prompt_pairs:
            try:
                mode_txt = mode_combo.currentText().strip() if mode_combo else ""
                prompt_txt = prompt_combo.currentText().strip() if prompt_combo else ""
            except Exception:
                mode_txt = ""
                prompt_txt = ""
            if mode_txt == "Prompt" and prompt_txt == "Select Interval":
                freqinout_issues.append(f"{label} prompt interval missing")
        snapshot["freqinout"] = self._build_section_health_entry(engaged=True, issues=freqinout_issues)
        snapshot["radio_profiles"] = self._build_section_health_entry(engaged=True, issues=radio_profile_issues)

        op_group_issues = [] if self.operating_groups else ["No HF operating groups configured"]
        snapshot["operating_groups"] = self._build_section_health_entry(engaged=True, issues=op_group_issues)

        js8_directed = self.js8_directed_edit.text().strip() if hasattr(self, "js8_directed_edit") else ""
        js8_forms = self.js8_forms_edit.text().strip() if hasattr(self, "js8_forms_edit") else ""
        js8_host = self.js8_host_edit.text().strip() if hasattr(self, "js8_host_edit") else ""
        js8_port = self.js8_port_edit.text().strip() if hasattr(self, "js8_port_edit") else ""
        js8call_path = self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else ""
        js8spotter_path = self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else ""
        commstat_path = self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else ""
        js8_engaged = any([js8_directed, js8_forms, js8call_path, js8spotter_path, commstat_path])
        js8_issues: List[str] = []
        if js8call_path and not js8_host:
            js8_issues.append("JS8Call TCP host missing")
        if js8call_path and not js8_port:
            js8_issues.append("JS8Call TCP port missing")
        if js8call_path and not js8_directed:
            js8_issues.append("JS8Call DIRECTED.TXT path missing")
        if js8spotter_path and not js8_forms:
            js8_issues.append("JS8Spotter forms path missing")
        snapshot["js8call"] = self._build_section_health_entry(engaged=js8_engaged, issues=js8_issues)

        default_fldigi_checkin_dir = str(get_fldigi_checkin_dir())
        flrig_path = self.path_edits.get("FLRig").text().strip() if self.path_edits.get("FLRig") else ""
        flrig_port = self.flrig_port_edit.text().strip() if hasattr(self, "flrig_port_edit") else ""
        fldigi_path = self.path_edits.get("FLDigi").text().strip() if self.path_edits.get("FLDigi") else ""
        fldigi_host = self.fldigi_host_edit.text().strip() if hasattr(self, "fldigi_host_edit") else ""
        fldigi_port = self.fldigi_port_edit.text().strip() if hasattr(self, "fldigi_port_edit") else ""
        flmsg_path = self.path_edits.get("FLMsg").text().strip() if self.path_edits.get("FLMsg") else ""
        flamp_path = self.path_edits.get("FLAmp").text().strip() if self.path_edits.get("FLAmp") else ""
        fldigi_log_path = self.fldigi_log_path_edit.text().strip() if hasattr(self, "fldigi_log_path_edit") else ""
        fldigi_checkin_dir = (
            self.fldigi_checkin_dir_edit.text().strip() if hasattr(self, "fldigi_checkin_dir_edit") else ""
        )
        fldigi_has_custom_checkin_dir = bool(fldigi_checkin_dir and fldigi_checkin_dir != default_fldigi_checkin_dir)
        flmsg_msg_path = self.msg_paths_edits.get("flmsg").text().strip() if self.msg_paths_edits.get("flmsg") else ""
        flamp_msg_path = self.msg_paths_edits.get("flamp").text().strip() if self.msg_paths_edits.get("flamp") else ""
        fast_light_engaged = any(
            [
                flrig_path,
                fldigi_path,
                flmsg_path,
                flamp_path,
                fldigi_log_path,
                fldigi_has_custom_checkin_dir,
                flmsg_msg_path,
                flamp_msg_path,
            ]
        )
        fast_light_issues: List[str] = []
        if flrig_path and not flrig_port:
            fast_light_issues.append("FLRig XML-RPC port missing")
        if fldigi_path and not fldigi_host:
            fast_light_issues.append("FLDigi XML-RPC host missing")
        if fldigi_path and not fldigi_port:
            fast_light_issues.append("FLDigi XML-RPC port missing")
        if flmsg_path and not flmsg_msg_path:
            fast_light_issues.append("FLMsg ICS/Messages path missing")
        if flamp_path and not flamp_msg_path:
            fast_light_issues.append("FLAmp FLAMP/rx path missing")
        if fldigi_has_custom_checkin_dir and not fldigi_path:
            fast_light_issues.append("FLDigi executable path missing")
        if flmsg_msg_path and not flmsg_path:
            fast_light_issues.append("FLMsg executable path missing")
        if flamp_msg_path and not flamp_path:
            fast_light_issues.append("FLAmp executable path missing")
        snapshot["fast_light"] = self._build_section_health_entry(
            engaged=fast_light_engaged,
            issues=fast_light_issues,
        )

        varac_install = self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else ""
        varac_launch = self.varac_launch_cmd_edit.text().strip() if hasattr(self, "varac_launch_cmd_edit") else ""
        varac_incoming = self.msg_paths_edits.get("varac").text().strip() if self.msg_paths_edits.get("varac") else ""
        varac_outbox = self.varac_outbox_dir_edit.text().strip() if hasattr(self, "varac_outbox_dir_edit") else ""
        varac_bbs_dir = self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        varac_bbs_archive = (
            self.varac_bbs_archive_dir_edit.text().strip() if hasattr(self, "varac_bbs_archive_dir_edit") else ""
        )
        varac_bbs_limit_access = bool(
            hasattr(self, "varac_bbs_limit_access_chk") and self.varac_bbs_limit_access_chk.isChecked()
        )
        varac_bbs_allowed_callsigns = self._varac_bbs_selected_callsigns_text()
        varac_auto_archive = bool(
            hasattr(self, "varac_bbs_auto_archive_chk") and self.varac_bbs_auto_archive_chk.isChecked()
        )
        varac_engaged = any(
            [
                varac_install,
                varac_launch,
                varac_incoming,
                varac_outbox,
                varac_bbs_dir,
                varac_bbs_archive,
                varac_auto_archive,
                varac_bbs_limit_access,
                varac_bbs_allowed_callsigns,
            ]
        )
        varac_issues: List[str] = []
        if varac_install and not varac_incoming:
            varac_issues.append("VarAC incoming files path missing")
        if (varac_incoming or varac_outbox or varac_bbs_dir or varac_bbs_archive or varac_auto_archive) and not (
            varac_install or varac_launch
        ):
            varac_issues.append("Install folder or launch override missing")
        if bool(varac_bbs_dir) != bool(varac_bbs_archive):
            varac_issues.append("BBS directory/archive setup incomplete")
        if varac_auto_archive and not (varac_bbs_dir and varac_bbs_archive):
            varac_issues.append("Auto-archive requires both BBS directories")
        if varac_bbs_limit_access and not varac_bbs_allowed_callsigns:
            varac_issues.append("BBS access limit has no allowed callsigns")
        varac_guard_enabled = bool(
            hasattr(self, "varac_guard_enabled_chk") and self.varac_guard_enabled_chk.isChecked()
        )
        varac_guard_mode = (
            self.varac_guard_mode_combo.currentText().strip() if hasattr(self, "varac_guard_mode_combo") else "Log only"
        )
        varac_guard_quarantine = (
            self.varac_guard_quarantine_dir_edit.text().strip()
            if hasattr(self, "varac_guard_quarantine_dir_edit")
            else ""
        )
        if varac_guard_enabled and not varac_incoming:
            varac_issues.append("VGuard file protection has no VarAC incoming files path")
        if varac_guard_enabled and varac_guard_mode == "Quarantine unauthorized files" and not varac_guard_quarantine:
            varac_issues.append("VGuard quarantine folder missing")
        varac_vault_enabled = bool(
            hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked()
        )
        varac_vault_root = (
            self.varac_bbs_vault_root_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_root_edit")
            else ""
        )
        if varac_vault_enabled and not varac_bbs_dir:
            varac_issues.append("Managed BBS Vault has no live BBS directory")
        if varac_vault_enabled and not varac_vault_root:
            varac_issues.append("Managed BBS Vault root missing")
        if varac_vault_enabled and not self._varac_bbs_vault_locations_cache:
            varac_issues.append("Managed BBS Vault has no locations")
        varac_engaged = any(
            [
                varac_engaged,
                varac_guard_enabled,
                varac_guard_quarantine,
                varac_vault_enabled,
                varac_vault_root,
            ]
        )
        snapshot["varac"] = self._build_section_health_entry(engaged=varac_engaged, issues=varac_issues)
        return snapshot

    def _apply_section_nav_item_health(
        self,
        item: QListWidgetItem,
        *,
        base_title: str,
        state: str,
        detail: str,
        theme: Dict[str, str],
    ) -> None:
        state = str(state or "neutral").strip().lower()
        item.setData(self.SECTION_HEALTH_STATE_ROLE, state)
        item.setToolTip(base_title if not detail else f"{base_title}\nNeeds Setup: {detail}")
        font = item.font()
        font.setBold(state == "warn")
        item.setFont(font)
        if state == "warn":
            bg = QColor(theme.get("warning", "#C99700"))
            bg.setAlpha(58 if theme.get("bg") == "#E6E8EA" else 78)
            item.setBackground(QBrush(bg))
            item.setForeground(QBrush(QColor(theme.get("text", "#222222"))))
            return
        item.setData(Qt.BackgroundRole, None)
        item.setData(Qt.ForegroundRole, None)

    def _refresh_section_nav_health(self) -> None:
        if not hasattr(self, "sections_nav_list"):
            return
        theme = resolve_theme(self.settings)
        snapshot = self._build_section_health_snapshot()
        for group, meta in self._section_meta.items():
            nav_item = self._section_nav_items.get(group)
            header_btn = meta.get("header_btn")
            if not nav_item:
                if header_btn:
                    header_btn.setStyleSheet(self._section_header_style("neutral", theme))
                continue
            base_title = str(meta.get("title", group.title() if hasattr(group, "title") else "")).strip()
            health_key = str(meta.get("health_key", "") or "").strip().lower()
            entry = snapshot.get(health_key, {"state": "neutral", "detail": ""})
            nav_item.setData(self.SECTION_HEALTH_KEY_ROLE, health_key)
            state = str(entry.get("state", "neutral"))
            self._apply_section_nav_item_health(
                nav_item,
                base_title=base_title,
                state=state,
                detail=str(entry.get("detail", "")),
                theme=theme,
            )
            if header_btn:
                header_btn.setStyleSheet(self._section_header_style(state, theme))
        self._apply_sections_nav_style()
        self._update_sections_nav_size()

    def _apply_collapsed_state(self, group: QGroupBox, content: QWidget, expanded: bool) -> None:
        content.setVisible(expanded)
        stacked_mode = hasattr(self, "sections_stack") and self.sections_stack.count() > 0
        meta = self._section_meta.get(group, {})
        fit_content = bool(meta.get("fit_content", False))
        if stacked_mode and not bool(meta.get("fit_content_in_stack", False)):
            fit_content = False
        header_btn = self._section_meta.get(group, {}).get("header_btn")
        if header_btn:
            header_btn.setArrowType(Qt.DownArrow if expanded else Qt.RightArrow)
        if expanded:
            if fit_content:
                header_height = 0
                if header_btn:
                    header_height = header_btn.sizeHint().height()
                margins = group.layout().contentsMargins() if group.layout() else None
                extra = 0
                if margins:
                    extra = margins.top() + margins.bottom()
                target_height = content.sizeHint().height() + header_height + extra
                group.setMinimumHeight(target_height)
                if stacked_mode:
                    group.setMaximumHeight(16777215)
                    group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
                else:
                    group.setMaximumHeight(target_height)
                    group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            else:
                group.setMinimumHeight(0)
                group.setMaximumHeight(16777215)
                group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        else:
            collapsed = self._collapsed_height(group)
            group.setMinimumHeight(collapsed)
            group.setMaximumHeight(collapsed)
            group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        group.updateGeometry()

    def _refresh_fit_content_section_height(self, group: QGroupBox | None) -> None:
        if group is None:
            return
        meta = self._section_meta.get(group, {})
        content = meta.get("content")
        if not isinstance(content, QWidget):
            return
        header_btn = meta.get("header_btn")
        expanded = bool(header_btn.isChecked()) if header_btn else bool(content.isVisible())
        self._apply_collapsed_state(group, content, expanded)
        current_widget = self.sections_stack.currentWidget() if hasattr(self, "sections_stack") else None
        if current_widget is group:
            self._sync_current_section_scroll_size()

    def _collapsed_height(self, group: QGroupBox) -> int:
        header_btn = self._section_meta.get(group, {}).get("header_btn")
        margins = group.layout().contentsMargins() if group.layout() else None
        extra = 0
        if margins:
            extra = margins.top() + margins.bottom()
        if header_btn:
            return max(34, header_btn.sizeHint().height() + extra)
        return max(34, group.fontMetrics().height() + 24 + extra)

    def _summary_operator_info(self) -> str:
        callsign = self.callsign_edit.text().strip().upper() if hasattr(self, "callsign_edit") else ""
        grid = self.grid6_edit.text().strip().upper() if hasattr(self, "grid6_edit") else ""
        if callsign and grid:
            return f"{callsign} / {grid}"
        if callsign:
            return callsign
        return "operator profile"

    def _summary_operating_status(self) -> str:
        return "live software indicators"

    def _summary_freqinout_settings(self) -> str:
        ctrl = self.control_combo.currentText().strip() if hasattr(self, "control_combo") else "FLRig"
        scheduler = "on" if (hasattr(self, "use_scheduler_chk") and self.use_scheduler_chk.isChecked()) else "off"
        return f"Control {ctrl}, Scheduler {scheduler}"

    def _summary_logging_settings(self) -> str:
        log_level = self.log_level_combo.currentText().strip() if hasattr(self, "log_level_combo") else "DISABLED"
        return f"Level {log_level}"

    def _summary_operating_groups(self) -> str:
        count = len(self.operating_groups)
        return f"{count} group{'s' if count != 1 else ''}"

    def _summary_local_net_profiles(self) -> str:
        rows = [r for r in self.local_net_profiles if isinstance(r, dict)]
        count = len(rows)
        groups = len({str(r.get("group") or r.get("name") or "").strip().upper() for r in rows if str(r.get("group") or r.get("name") or "").strip()})
        return f"{count} entr{'y' if count == 1 else 'ies'} in {groups} group{'s' if groups != 1 else ''}"

    def _summary_js8_settings(self) -> str:
        directed = "set" if self.js8_directed_edit.text().strip() else "missing"
        forms = "set" if self.js8_forms_edit.text().strip() else "missing"
        js8call = "set" if hasattr(self, "js8call_path_edit") and self.js8call_path_edit.text().strip() else "missing"
        spotter = "set" if hasattr(self, "js8spotter_path_edit") and self.js8spotter_path_edit.text().strip() else "missing"
        commstat = "set" if hasattr(self, "commstat_path_edit") and self.commstat_path_edit.text().strip() else "missing"
        return f"JS8Call {js8call}, Spotter {spotter}, CommStat {commstat}, DIRECTED {directed}, Forms {forms}"

    def _summary_fast_light_settings(self) -> str:
        total = len(self.PROGRAMS)
        set_count = 0
        for name in self.PROGRAMS:
            edit = self.path_edits.get(name)
            if edit and edit.text().strip():
                set_count += 1
        return f"{set_count}/{total} app paths set"

    def _summary_gpg_settings(self) -> str:
        enabled = bool(hasattr(self, "gpg_verify_enabled_chk") and self.gpg_verify_enabled_chk.isChecked())
        hash_enabled = bool(hasattr(self, "hash_verify_enabled_chk") and self.hash_verify_enabled_chk.isChecked())
        path_set = bool(hasattr(self, "gpg_path_edit") and self.gpg_path_edit.text().strip())
        trusted = len(self._gpg_trusted_fingerprints)
        local_hashes = len([r for r in self._trusted_hash_entries if bool(r.get("enabled", True))])
        return (
            f"Sig {'on' if enabled else 'off'}, Hash {'on' if hash_enabled else 'off'}, "
            f"GPG {'set' if path_set else 'auto'}, {trusted} keys, {local_hashes} hashes"
        )

    def _summary_varac_settings(self) -> str:
        install_set = bool(hasattr(self, "varac_path_edit") and self.varac_path_edit.text().strip())
        launch_cmd_set = bool(hasattr(self, "varac_launch_cmd_edit") and self.varac_launch_cmd_edit.text().strip())
        incoming_set = bool(self.msg_paths_edits.get("varac") and self.msg_paths_edits["varac"].text().strip())
        outbox_set = bool(hasattr(self, "varac_outbox_dir_edit") and self.varac_outbox_dir_edit.text().strip())
        bbs_set = bool(
            hasattr(self, "varac_bbs_dir_edit")
            and self.varac_bbs_dir_edit.text().strip()
            and hasattr(self, "varac_bbs_archive_dir_edit")
            and self.varac_bbs_archive_dir_edit.text().strip()
        )
        archive_on = bool(hasattr(self, "varac_bbs_auto_archive_chk") and self.varac_bbs_auto_archive_chk.isChecked())
        cluster_mode = "on" if self._varac_cluster_mode_enabled() else "off"
        access_summary = bbs_summary_text(
            {
                "enable_bbs": self.varac_bbs_enabled_chk.isChecked() if hasattr(self, "varac_bbs_enabled_chk") else False,
                "limit_access": (
                    self.varac_bbs_limit_access_chk.isChecked() if hasattr(self, "varac_bbs_limit_access_chk") else False
                ),
                "announce": self.varac_bbs_announce_chk.isChecked() if hasattr(self, "varac_bbs_announce_chk") else False,
                "allowed_callsigns": self._varac_bbs_selected_callsigns_text(),
            }
        )
        vault_enabled = bool(
            hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked()
        )
        vault_locations = len(self._varac_bbs_vault_locations_cache)
        return (
            f"Install {'set' if install_set else 'missing'}, "
            f"Launch {'override' if launch_cmd_set else 'auto'}, "
            f"Incoming {'set' if incoming_set else 'missing'}, "
            f"Outbox {'set' if outbox_set else 'missing'}, "
            f"BBS {'set' if bbs_set else 'missing'}, "
            f"Archive {'on' if archive_on else 'off'}, "
            f"Cluster Mode {cluster_mode}, "
            f"{access_summary}, "
            f"Vault {'on' if vault_enabled else 'off'} ({vault_locations} locations)"
        )

    def _summary_custom_tools(self) -> str:
        total = len(self._custom_tool_items_cache)
        if total <= 0:
            return "No custom tools"
        return f"{total} custom tool{'s' if total != 1 else ''} configured"

    def _summary_launch_control(self) -> str:
        total = len(self._launch_items_cache)
        enabled = sum(1 for item in self._launch_items_cache if bool(item.get("enabled", False)))
        startup = sum(1 for item in self._launch_items_cache if bool(item.get("startup", False)))
        launch_all = bool(hasattr(self, "launch_all_with_startup_chk") and self.launch_all_with_startup_chk.isChecked())
        return f"{enabled}/{total} enabled, {startup} startup, launch-all {'on' if launch_all else 'off'}"

    def _summary_sop_export(self) -> str:
        preamble_set = bool(
            hasattr(self, "sop_export_preamble_edit")
            and self.sop_export_preamble_edit.toPlainText().strip()
        )
        postamble_set = bool(
            hasattr(self, "sop_export_postamble_edit")
            and self.sop_export_postamble_edit.toPlainText().strip()
        )
        return (
            f"Preamble {'set' if preamble_set else 'blank'}, "
            f"Postamble {'set' if postamble_set else 'blank'}"
        )

    def _settings_snapshot_for_readiness(self) -> Dict[str, Any]:
        data = dict(self.settings.all())
        message_paths = dict(data.get("message_paths", {}) or {})
        data["callsign"] = self.callsign_edit.text().strip().upper() if hasattr(self, "callsign_edit") else ""
        data["operator_callsign"] = data["callsign"]
        data["grid"] = self.grid6_edit.text().strip().upper() if hasattr(self, "grid6_edit") else ""
        data["operator_grid6"] = data["grid"]
        data["control_via"] = self.control_combo.currentText().strip() if hasattr(self, "control_combo") else ""
        data["freq_enforcement_mode"] = (
            self.freq_enforce_combo.currentText().strip() if hasattr(self, "freq_enforce_combo") else ""
        )
        data["freq_prompt_interval"] = (
            self.freq_prompt_combo.currentText().strip() if hasattr(self, "freq_prompt_combo") else ""
        )
        data["fldigi_enforcement_mode"] = (
            self.fldigi_enforce_combo.currentText().strip() if hasattr(self, "fldigi_enforce_combo") else ""
        )
        data["fldigi_prompt_interval"] = (
            self.fldigi_prompt_combo.currentText().strip() if hasattr(self, "fldigi_prompt_combo") else ""
        )
        data["js8_enforcement_mode"] = (
            self.js8_enforce_combo.currentText().strip() if hasattr(self, "js8_enforce_combo") else ""
        )
        data["js8_prompt_interval"] = (
            self.js8_prompt_combo.currentText().strip() if hasattr(self, "js8_prompt_combo") else ""
        )
        data["js8_host"] = self.js8_host_edit.text().strip() if hasattr(self, "js8_host_edit") else ""
        data["js8_port"] = self.js8_port_edit.text().strip() if hasattr(self, "js8_port_edit") else ""
        data["js8_directed_path"] = self.js8_directed_edit.text().strip() if hasattr(self, "js8_directed_edit") else ""
        data["js8_forms_path"] = self.js8_forms_edit.text().strip() if hasattr(self, "js8_forms_edit") else ""
        data["path_js8call"] = self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else ""
        data["path_js8spotter"] = self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else ""
        data["path_commstat"] = self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else ""
        data["path_flrig"] = self.path_edits.get("FLRig").text().strip() if self.path_edits.get("FLRig") else ""
        data["path_fldigi"] = self.path_edits.get("FLDigi").text().strip() if self.path_edits.get("FLDigi") else ""
        data["path_flmsg"] = self.path_edits.get("FLMsg").text().strip() if self.path_edits.get("FLMsg") else ""
        data["path_flamp"] = self.path_edits.get("FLAmp").text().strip() if self.path_edits.get("FLAmp") else ""
        data["flrig_port"] = self.flrig_port_edit.text().strip() if hasattr(self, "flrig_port_edit") else ""
        data["fldigi_host"] = self.fldigi_host_edit.text().strip() if hasattr(self, "fldigi_host_edit") else ""
        data["fldigi_port"] = self.fldigi_port_edit.text().strip() if hasattr(self, "fldigi_port_edit") else ""
        data["fldigi_log_path"] = (
            self.fldigi_log_path_edit.text().strip() if hasattr(self, "fldigi_log_path_edit") else ""
        )
        data["fldigi_checkin_dir"] = (
            self.fldigi_checkin_dir_edit.text().strip() if hasattr(self, "fldigi_checkin_dir_edit") else ""
        )
        data["varac_path"] = self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else ""
        data["varac_launch_cmd"] = (
            self.varac_launch_cmd_edit.text().strip() if hasattr(self, "varac_launch_cmd_edit") else ""
        )
        data["varac_outbox_dir"] = (
            self.varac_outbox_dir_edit.text().strip() if hasattr(self, "varac_outbox_dir_edit") else ""
        )
        data["varac_bbs_dir"] = self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        data["varac_bbs_archive_dir"] = (
            self.varac_bbs_archive_dir_edit.text().strip() if hasattr(self, "varac_bbs_archive_dir_edit") else ""
        )
        data["varac_bbs_auto_archive"] = bool(
            hasattr(self, "varac_bbs_auto_archive_chk") and self.varac_bbs_auto_archive_chk.isChecked()
        )
        data["varac_bbs_auto_archive_enabled"] = data["varac_bbs_auto_archive"]
        data["varac_bbs_limit_access_enabled"] = bool(
            hasattr(self, "varac_bbs_limit_access_chk") and self.varac_bbs_limit_access_chk.isChecked()
        )
        data["varac_bbs_vault_enabled"] = bool(
            hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked()
        )
        data["varac_bbs_vault_global_code_policy"] = (
            self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
            if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
            else DEFAULT_GLOBAL_CODE_POLICY
        )
        data["varac_bbs_vault_flamp_enabled"] = bool(
            hasattr(self, "varac_bbs_vault_flamp_enabled_chk") and self.varac_bbs_vault_flamp_enabled_chk.isChecked()
        )
        data["varac_bbs_vault_flamp_relay_dir"] = (
            self.varac_bbs_vault_flamp_relay_dir_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_flamp_relay_dir_edit")
            else ""
        )
        data["varac_bbs_vault_flamp_listing_max_age_days"] = int(
            self.varac_bbs_vault_flamp_listing_age_combo.currentData()
            if hasattr(self, "varac_bbs_vault_flamp_listing_age_combo")
            and self.varac_bbs_vault_flamp_listing_age_combo.currentData() is not None
            else DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS
        )
        if hasattr(self, "msg_paths_edits"):
            for origin, edit in self.msg_paths_edits.items():
                message_paths[origin] = edit.text().strip()
        data["message_paths"] = message_paths
        return data

    def _current_station_readiness_report(self):
        report = build_station_readiness_report(
            self._settings_snapshot_for_readiness(),
            device_profiles=self.device_profiles,
            operating_groups=self.operating_groups,
        )
        self._last_station_readiness_report = report
        return report

    def _station_readiness_report_for_software_chips(self):
        cached = getattr(self, "_last_station_readiness_report", None)
        if cached is not None:
            return cached
        return self._current_station_readiness_report()

    def _clear_status_layout(self, layout: QHBoxLayout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            child_layout = item.layout()
            widget = item.widget()
            if child_layout is not None:
                self._clear_status_layout(child_layout)  # type: ignore[arg-type]
                continue
            if widget is not None:
                widget.deleteLater()

    def _current_visible_status_items(self) -> List[tuple[str, str]]:
        selected_profile = self._selected_settings_radio_profile()
        profiles = [selected_profile] if isinstance(selected_profile, dict) else self.device_profiles
        if not profiles:
            try:
                profiles = list(self.multi_radio_store.list_device_profiles())
            except Exception:
                profiles = []
        return visible_status_programs(self._settings_snapshot_for_readiness(), device_profiles=profiles)

    def _rebuild_status_indicators(self) -> None:
        if not hasattr(self, "status_layout"):
            return
        self._clear_status_layout(self.status_layout)
        self.status_labels = {}
        self._status_text_labels = {}
        theme = resolve_theme(self.settings)
        visible_items = self._current_visible_status_items()
        selected_profile = self._selected_settings_radio_profile()
        if hasattr(self, "status_group"):
            radio_name = self._profile_display_name(selected_profile) if isinstance(selected_profile, dict) else "Selected Radio"
            self.status_group.setTitle(f"Radio Status: {radio_name}")
        self.status_group.setVisible(bool(visible_items))
        for key, label in visible_items:
            led = QLabel()
            led.setFixedSize(14, 14)
            led.setStyleSheet(led_style("idle", theme))
            text_label = QLabel(label)
            self.status_labels[key] = led
            self._status_text_labels[key] = text_label
            self.status_layout.addWidget(led)
            self.status_layout.addWidget(text_label)
            self.status_layout.addSpacing(12)
        self.status_layout.addStretch()

    def focus_section_by_health_key(self, health_key: str, radio_id: int | None = None) -> bool:
        target = str(health_key or "").strip().lower()
        if not target or not hasattr(self, "sections_nav_list"):
            return False
        for row in range(self.sections_nav_list.count()):
            item = self.sections_nav_list.item(row)
            if not item:
                continue
            item_key = str(item.data(self.SECTION_HEALTH_KEY_ROLE) or "").strip().lower()
            if item_key != target:
                continue
            scope = str(item.data(self.SECTION_SCOPE_ROLE) or "").strip().lower()
            if item.isHidden() and scope == "global" and hasattr(self, "global_settings_toggle_btn"):
                self.global_settings_toggle_btn.setChecked(True)
                self._on_global_settings_toggle(True)
            if item.isHidden():
                continue
            self.sections_nav_list.setCurrentRow(row)
            self.sections_nav_list.scrollToItem(item)
            if target == "radio_profiles" and radio_id:
                QTimer.singleShot(0, lambda ident=int(radio_id): self.focus_radio_profile(ident))
            return True
        return False

    def focus_radio_profile(self, radio_id: int) -> bool:
        if int(radio_id or 0) <= 0 or not hasattr(self, "device_profiles_table"):
            return False
        table = self.device_profiles_table
        for row in range(table.rowCount()):
            item = table.item(row, 3)
            if item is None:
                continue
            try:
                item_radio_id = int(item.data(Qt.UserRole) or 0)
            except Exception:
                item_radio_id = 0
            if item_radio_id != int(radio_id):
                continue
            table.scrollToItem(item, QAbstractItemView.PositionAtCenter)
            self._set_settings_radio_focus(int(radio_id))
            table.setFocus(Qt.OtherFocusReason)
            return True
        return False

    def _disable_prompt_hint_item(self, combo: QComboBox) -> None:
        try:
            model = combo.model()
            if model is None:
                return
            idx = model.index(0, 0)
            model.setData(idx, 0, Qt.UserRole - 1)
        except Exception:
            pass

    def _align_enforcement_labels(self) -> None:
        labels = [
            self.freq_timer_label,
            self.fldigi_timer_label,
            self.js8_timer_label,
        ]
        prompt_labels = [
            self.freq_prompt_label,
            self.fldigi_prompt_label,
            self.js8_prompt_label,
        ]
        try:
            max_timer = max(lbl.sizeHint().width() for lbl in labels)
            max_prompt = max(lbl.sizeHint().width() for lbl in prompt_labels)
            for lbl in labels:
                lbl.setFixedWidth(max_timer)
            for lbl in prompt_labels:
                lbl.setFixedWidth(max_prompt)
        except Exception:
            pass

    def _make_contextual_autofill_button(
        self,
        rule_id: str,
        text: str,
        section: str,
        keys: List[str],
        *,
        base_edit: Optional[QLineEdit] = None,
        tooltip: str = "",
    ) -> QPushButton:
        btn = QPushButton(text)
        btn.setFixedWidth(96 if len(text) <= 9 else 112)
        btn.setToolTip(tooltip or "Attempt to fill related settings.")
        btn.clicked.connect(lambda _checked=False, s=section, k=tuple(keys): self._attempt_scoped_autofill(s, list(k)))
        self._contextual_autofill_buttons[rule_id] = btn
        self._contextual_autofill_rules[rule_id] = {
            "section": section,
            "keys": list(keys),
            "primary_key": str(keys[0]) if keys else "",
            "base_edit": base_edit,
            "tooltip": tooltip or "Attempt to fill related settings.",
        }
        if base_edit is not None:
            base_edit.textChanged.connect(lambda _text: self._refresh_contextual_autofill_buttons())
        QTimer.singleShot(0, lambda rid=rule_id: self._wire_contextual_autofill_rule(rid))
        QTimer.singleShot(0, self._refresh_contextual_autofill_buttons)
        return btn

    def _wire_contextual_autofill_rule(self, rule_id: str) -> None:
        rule = self._contextual_autofill_rules.get(rule_id, {})
        edits: List[QLineEdit] = []
        base_edit = rule.get("base_edit")
        if isinstance(base_edit, QLineEdit):
            edits.append(base_edit)
        for key in [str(key) for key in (rule.get("keys") or [])]:
            edit = self._autofill_target_edit(key)
            if isinstance(edit, QLineEdit):
                edits.append(edit)
        prop_name = f"fio_autofill_wired_{rule_id}"
        for edit in edits:
            if bool(edit.property(prop_name)):
                continue
            edit.setProperty(prop_name, True)
            edit.textChanged.connect(lambda _text: self._refresh_contextual_autofill_buttons())

    def _make_autofill_review_table(self, section: str) -> QTableWidget:
        table = QTableWidget(0, 6)
        table.setHorizontalHeaderLabels(["Field", "Current", "Suggested", "Confidence", "Reason", "Action"])
        table.verticalHeader().setVisible(False)
        table.setSelectionMode(QAbstractItemView.NoSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setAlternatingRowColors(True)
        table.setVisible(False)
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.Stretch)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self._autofill_review_tables[section] = table
        return table

    def _detect_autofill_results(self, section: str) -> Dict[str, PathDetectionResult]:
        normalized = str(section or "").strip().lower()
        if normalized == "fast_light":
            return self.software_path_detector.detect_fast_light()
        if normalized == "js8":
            return self._radio_scoped_js8_autofill_results(self.software_path_detector.detect_js8())
        if normalized == "varac":
            return self.software_path_detector.detect_varac()
        return {}

    def _radio_scoped_js8_autofill_results(
        self, results: Dict[str, PathDetectionResult]
    ) -> Dict[str, PathDetectionResult]:
        scoped = dict(results)
        file_profiles = discover_js8call_file_profiles()
        port_txt = self.js8_port_edit.text().strip() if hasattr(self, "js8_port_edit") else ""
        profile_name = ""
        try:
            _radio_id, profile_name = self._selected_settings_feedback_target()
        except Exception:
            profile_name = ""
        selected_profile = select_js8call_file_profile(
            file_profiles,
            tcp_port=port_txt,
            profile_name=profile_name,
        )
        if selected_profile is not None:
            scoped["js8_directed_path"] = PathDetectionResult(
                key="js8_directed_path",
                label="JS8Call DIRECTED.TXT path",
                path=selected_profile.directed_path,
                confidence=selected_profile.confidence,
                reason=selected_profile.reason,
                exists=Path(selected_profile.directed_path).is_file(),
                target_type="file",
            )
            return scoped
        if sum(1 for profile in file_profiles if profile.directed_path) > 1:
            scoped["js8_directed_path"] = PathDetectionResult(
                key="js8_directed_path",
                label="JS8Call DIRECTED.TXT path",
                path="",
                confidence="not_found",
                reason=(
                    "Multiple JS8Call profiles have DIRECTED.TXT, but none matched the selected radio's "
                    f"JS8 TCP port {port_txt or '--'}."
                ),
                exists=False,
                target_type="file",
            )
        return scoped

    def _attempt_scoped_autofill(self, section: str, keys: List[str]) -> None:
        section_label = self._autofill_section_label(section)
        self._publish_autofill_feedback(
            status="in_progress",
            summary=f"Auto-fill scanning {section_label}.",
            detail="FreqInOut is looking for blank fields it can fill for the selected radio.",
            section=section,
            operation="scan",
        )
        all_results = self._detect_autofill_results(section)
        wanted = set(keys)
        scoped = {key: result for key, result in all_results.items() if key in wanted}
        self._apply_autofill_results(section, scoped)

    def _refresh_contextual_autofill_buttons(self) -> None:
        if not hasattr(self, "_contextual_autofill_buttons"):
            return
        theme = resolve_theme(self.settings)
        for rule_id, btn in self._contextual_autofill_buttons.items():
            rule = self._contextual_autofill_rules.get(rule_id, {})
            base_edit = rule.get("base_edit")
            keys = [str(key) for key in (rule.get("keys") or [])]
            base_ready = True
            if isinstance(base_edit, QLineEdit):
                base_ready = bool(base_edit.text().strip())
            target_edits: List[QLineEdit] = []
            for key in keys:
                edit = self._autofill_target_edit(key)
                if edit is None:
                    continue
                if isinstance(base_edit, QLineEdit) and edit is base_edit:
                    continue
                target_edits.append(edit)
            if not target_edits:
                target_edits = [
                    edit
                    for key in keys
                    for edit in [self._autofill_target_edit(key)]
                    if isinstance(edit, QLineEdit)
                ]
            primary_key = str(rule.get("primary_key") or "").strip()
            primary_edit = self._autofill_target_edit(primary_key) if primary_key else None
            if isinstance(primary_edit, QLineEdit):
                missing_target = not primary_edit.text().strip()
            else:
                missing_target = any(not edit.text().strip() for edit in target_edits)
            role = "eligible_warning" if base_ready and missing_target else "secondary"
            btn.setStyleSheet(button_style(role, theme))
            base_tip = str(rule.get("tooltip") or "Attempt to fill related settings.").strip()
            if role == "eligible_warning":
                btn.setToolTip(f"{base_tip}\nSome related fields are still blank.")
            else:
                btn.setToolTip(base_tip)

    def _attempt_fast_light_autofill(self) -> None:
        self._publish_autofill_feedback(
            status="in_progress",
            summary=f"Auto-fill scanning {self._autofill_section_label('fast_light')}.",
            detail="FreqInOut is looking for blank fields it can fill for the selected radio.",
            section="fast_light",
            operation="scan",
        )
        self._apply_autofill_results("fast_light", self._detect_autofill_results("fast_light"))

    def _attempt_js8_autofill(self) -> None:
        self._publish_autofill_feedback(
            status="in_progress",
            summary=f"Auto-fill scanning {self._autofill_section_label('js8')}.",
            detail="FreqInOut is looking for blank fields it can fill for the selected radio.",
            section="js8",
            operation="scan",
        )
        self._apply_autofill_results("js8", self._detect_autofill_results("js8"))

    def _attempt_varac_autofill(self) -> None:
        self._publish_autofill_feedback(
            status="in_progress",
            summary=f"Auto-fill scanning {self._autofill_section_label('varac')}.",
            detail="FreqInOut is looking for blank fields it can fill for the selected radio.",
            section="varac",
            operation="scan",
        )
        self._apply_autofill_results("varac", self._detect_autofill_results("varac"))

    def _apply_autofill_results(self, section: str, results: Dict[str, PathDetectionResult]) -> None:
        filled: List[str] = []
        preserved: List[str] = []
        missing: List[str] = []
        detail_lines: List[str] = []
        preserved_suggestions: List[Dict[str, str]] = []
        for result in results.values():
            edit = self._autofill_target_edit(result.key)
            if edit is None:
                continue
            current = edit.text().strip()
            if not result.path or result.confidence == "not_found":
                missing.append(result.label)
                detail_lines.append(f"{result.label}: not found — {result.reason}")
                continue
            if current:
                if self._normalized_path_text(current) == self._normalized_path_text(result.path):
                    preserved.append(f"{result.label} already matches")
                    detail_lines.append(
                        f"{result.label}: kept existing value ({result.confidence}) — {result.reason}"
                    )
                else:
                    preserved.append(result.label)
                    preserved_suggestions.append(
                        {
                            "key": str(result.key or ""),
                            "label": str(result.label or ""),
                            "current": current,
                            "suggested": str(result.path or ""),
                            "confidence": str(result.confidence or ""),
                            "reason": str(result.reason or ""),
                        }
                    )
                    detail_lines.append(
                        f"{result.label}: kept existing value; suggested {result.path} ({result.confidence}) — {result.reason}"
                    )
                continue
            edit.setText(result.path)
            filled.append(f"{result.label} ({result.confidence})")
            detail_lines.append(f"{result.label}: filled {result.path} ({result.confidence}) — {result.reason}")
        summary_parts: List[str] = []
        if filled:
            summary_parts.append(f"Filled {len(filled)} field(s).")
        if preserved:
            summary_parts.append(f"Preserved {len(preserved)} existing field(s).")
        if missing:
            summary_parts.append(f"Not found: {len(missing)}.")
        if not summary_parts:
            summary_parts.append("No auto-fill changes were available.")
        summary = " ".join(summary_parts)
        detail = "\n".join(detail_lines)
        self._set_autofill_preserved_suggestions(section, preserved_suggestions)
        compact_status = self._autofill_visible_review_text(summary, detail_lines)
        full_status = self._autofill_visible_review_text(summary, detail_lines, max_lines=len(detail_lines))
        self._set_autofill_status(section, compact_status, detail, full_text=full_status)
        self._publish_autofill_feedback(
            status=self._autofill_feedback_status(
                filled_count=len(filled),
                preserved_count=len(preserved),
                missing_count=len(missing),
            ),
            summary=f"Auto-fill updated {self._autofill_section_label(section)}: {summary}",
            detail=detail or summary,
            section=section,
            operation="result",
        )
        self._refresh_section_titles()
        self._refresh_section_nav_health()
        self._refresh_contextual_autofill_buttons()
        self._publish_autofill_readiness_feedback(section)

    @staticmethod
    def _autofill_visible_review_text(summary: str, detail_lines: Sequence[str], *, max_lines: int = 4) -> str:
        summary_text = str(summary or "").strip() or "No auto-fill changes were available."
        cleaned = [str(line or "").strip() for line in detail_lines if str(line or "").strip()]
        if not cleaned:
            return summary_text
        visible_count = max(0, int(max_lines or 0))
        visible = cleaned[:visible_count] if visible_count else []
        review_lines = [summary_text, "Review suggestions:"]
        review_lines.extend(f"- {line}" for line in visible)
        remaining = len(cleaned) - len(visible)
        if remaining > 0:
            noun = "item" if remaining == 1 else "items"
            review_lines.append(f"- {remaining} more {noun}")
        return "\n".join(review_lines)

    def _set_autofill_status(self, section: str, text: str, tooltip: str, *, full_text: str = "") -> None:
        label = self._autofill_status_labels.get(section)
        if label is None:
            return
        compact_text = str(text or "").strip()
        expanded_text = str(full_text or compact_text).strip()
        self._autofill_compact_status_texts[section] = compact_text
        self._autofill_full_status_texts[section] = expanded_text
        self._autofill_status_expanded[section] = False
        label.setText(text)
        label.setToolTip(tooltip or text)
        button = self._autofill_status_buttons.get(section)
        if button is not None:
            can_expand = bool(expanded_text and expanded_text != compact_text)
            button.setVisible(can_expand)
            button.setEnabled(can_expand)
            button.setText("Show Full Review")
            button.setToolTip("Show the full Auto-Fill review in this Settings section.")

    def _toggle_autofill_review(self, section: str) -> None:
        label = self._autofill_status_labels.get(section)
        button = self._autofill_status_buttons.get(section)
        if label is None or button is None:
            return
        expanded = not bool(self._autofill_status_expanded.get(section, False))
        compact_text = self._autofill_compact_status_texts.get(section, label.text())
        full_text = self._autofill_full_status_texts.get(section, compact_text)
        self._autofill_status_expanded[section] = expanded
        label.setText(full_text if expanded else compact_text)
        button.setText("Show Less" if expanded else "Show Full Review")
        button.setToolTip(
            "Collapse the Auto-Fill review."
            if expanded
            else "Show the full Auto-Fill review in this Settings section."
        )

    @staticmethod
    def _autofill_preserved_suggestions_text(section_label: str, suggestions: Sequence[Mapping[str, str]]) -> str:
        cleaned: List[str] = []
        for suggestion in suggestions:
            label = str(suggestion.get("label", "") or "").strip() or "Field"
            current = str(suggestion.get("current", "") or "").strip()
            suggested = str(suggestion.get("suggested", "") or "").strip()
            confidence = str(suggestion.get("confidence", "") or "").strip()
            reason = str(suggestion.get("reason", "") or "").strip()
            if not suggested:
                continue
            line = f"{label}: keep {current or '(blank)'}; suggested {suggested}"
            if confidence:
                line = f"{line} ({confidence})"
            if reason:
                line = f"{line} - {reason}"
            cleaned.append(line)
        if not cleaned:
            return ""
        section = str(section_label or "Auto-Fill").strip() or "Auto-Fill"
        header = f"{section} preserved {len(cleaned)} existing field(s) with suggested replacement value(s):"
        return "\n".join([header, *(f"- {line}" for line in cleaned)])

    def _set_autofill_preserved_suggestions(
        self,
        section: str,
        suggestions: Sequence[Mapping[str, str]],
    ) -> None:
        cleaned = [dict(item) for item in suggestions if str(item.get("suggested", "") or "").strip()]
        self._autofill_preserved_suggestions[section] = cleaned
        self._refresh_autofill_review_table(section)
        has_suggestions = bool(cleaned)
        action_row = getattr(self, "_autofill_action_rows", {}).get(section)
        if action_row is not None:
            action_row.setVisible(has_suggestions)
        button = self._autofill_preserved_buttons.get(section)
        if button is not None:
            button.setVisible(has_suggestions)
            button.setEnabled(has_suggestions)
            button.setToolTip(
                "Copy Auto-Fill suggestions for preserved existing values."
                if has_suggestions
                else "No preserved Auto-Fill suggestions to copy."
            )
        replace_button = self._autofill_replace_buttons.get(section)
        if replace_button is not None:
            replace_button.setVisible(has_suggestions)
            replace_button.setEnabled(has_suggestions)
            replace_button.setToolTip(
                "Replace preserved existing values with the cached Auto-Fill suggestions."
                if has_suggestions
                else "No preserved Auto-Fill suggestions to replace."
            )
        dismiss_button = getattr(self, "_autofill_dismiss_buttons", {}).get(section)
        if dismiss_button is not None:
            dismiss_button.setVisible(has_suggestions)
            dismiss_button.setEnabled(has_suggestions)
            dismiss_button.setToolTip(
                "Dismiss cached Auto-Fill suggestions for this section."
                if has_suggestions
                else "No preserved Auto-Fill suggestions to dismiss."
            )

    @staticmethod
    def _autofill_suggestion_row_values(suggestion: Mapping[str, str]) -> Tuple[str, str, str, str, str]:
        return (
            str(suggestion.get("label", "") or "").strip() or "Field",
            str(suggestion.get("current", "") or "").strip(),
            str(suggestion.get("suggested", "") or "").strip(),
            str(suggestion.get("confidence", "") or "").strip(),
            str(suggestion.get("reason", "") or "").strip(),
        )

    def _refresh_autofill_review_table(self, section: str) -> None:
        table = getattr(self, "_autofill_review_tables", {}).get(section)
        if table is None:
            return
        suggestions = list(self._autofill_preserved_suggestions.get(section, []) or [])
        table.setRowCount(0)
        table.setVisible(bool(suggestions))
        if not suggestions:
            return
        for row_index, suggestion in enumerate(suggestions):
            label, current, suggested, confidence, reason = self._autofill_suggestion_row_values(suggestion)
            table.insertRow(row_index)
            for col, value in enumerate([label, current, suggested, confidence, reason]):
                item = QTableWidgetItem(value)
                item.setToolTip(value)
                table.setItem(row_index, col, item)
            action_widget = QWidget()
            action_layout = QHBoxLayout(action_widget)
            action_layout.setContentsMargins(0, 0, 0, 0)
            action_layout.setSpacing(6)
            replace_btn = QPushButton("Replace")
            replace_btn.setToolTip(f"Replace {label} with the suggested Auto-Fill value.")
            replace_btn.clicked.connect(
                lambda _checked=False, s=section, i=row_index: self._replace_autofill_preserved_suggestion(s, i)
            )
            dismiss_btn = QPushButton("Dismiss")
            dismiss_btn.setToolTip(f"Dismiss the Auto-Fill suggestion for {label}.")
            dismiss_btn.clicked.connect(
                lambda _checked=False, s=section, i=row_index: self._dismiss_autofill_preserved_suggestion(s, i)
            )
            action_layout.addWidget(replace_btn)
            action_layout.addWidget(dismiss_btn)
            table.setCellWidget(row_index, 5, action_widget)
        self._fit_table_height_to_rows(table, min_rows=1, max_rows=4, extra_rows=0)

    @staticmethod
    def _autofill_preserved_copy_summary(section_label: str, suggestion_count: int) -> str:
        count = max(0, int(suggestion_count or 0))
        noun = "suggestion" if count == 1 else "suggestions"
        section = str(section_label or "Auto-Fill").strip() or "Auto-Fill"
        return f"Copied {count} preserved {section} Auto-Fill {noun}."

    def _copy_autofill_preserved_suggestions(self, section: str) -> None:
        suggestions = self._autofill_preserved_suggestions.get(section, [])
        if not suggestions:
            self._set_autofill_preserved_suggestions(section, ())
            return
        section_label = self._autofill_section_label(section)
        text = self._autofill_preserved_suggestions_text(section_label, suggestions)
        if not text:
            self._set_autofill_preserved_suggestions(section, ())
            return
        QApplication.clipboard().setText(text)
        self._publish_settings_action_feedback(
            status="succeeded",
            summary=self._autofill_preserved_copy_summary(section_label, len(suggestions)),
            detail=text,
            action_type="copy_autofill_suggestions",
            source_surface=self._autofill_feedback_source_surface(section, "copy_suggestions"),
        )

    @staticmethod
    def _autofill_dismiss_summary(section_label: str, suggestion_count: int) -> str:
        count = max(0, int(suggestion_count or 0))
        noun = "suggestion" if count == 1 else "suggestions"
        section = str(section_label or "Auto-Fill").strip() or "Auto-Fill"
        return f"Dismissed {count} preserved {section} Auto-Fill {noun}."

    def _dismiss_autofill_preserved_suggestions(self, section: str) -> None:
        suggestions = list(self._autofill_preserved_suggestions.get(section, []) or [])
        if not suggestions:
            self._set_autofill_preserved_suggestions(section, ())
            return
        section_label = self._autofill_section_label(section)
        detail = self._autofill_preserved_suggestions_text(section_label, suggestions)
        self._set_autofill_preserved_suggestions(section, ())
        self._publish_settings_action_feedback(
            status="succeeded",
            summary=self._autofill_dismiss_summary(section_label, len(suggestions)),
            detail=detail,
            action_type="dismiss_autofill_suggestions",
            source_surface=self._autofill_feedback_source_surface(section, "dismiss_suggestions"),
        )

    def _dismiss_autofill_preserved_suggestion(self, section: str, index: int) -> None:
        suggestions = list(self._autofill_preserved_suggestions.get(section, []) or [])
        if index < 0 or index >= len(suggestions):
            return
        suggestion = dict(suggestions[index])
        label = self._autofill_suggestion_row_values(suggestion)[0]
        remaining = [dict(item) for pos, item in enumerate(suggestions) if pos != index]
        self._set_autofill_preserved_suggestions(section, remaining)
        section_label = self._autofill_section_label(section)
        self._publish_settings_action_feedback(
            status="succeeded",
            summary=f"Dismissed {label} Auto-Fill suggestion for {section_label}.",
            detail=self._autofill_preserved_suggestions_text(section_label, (suggestion,)),
            action_type="dismiss_autofill_suggestion",
            source_surface=self._autofill_feedback_source_surface(section, "dismiss_suggestion"),
        )

    @staticmethod
    def _autofill_replace_summary(section_label: str, replaced_count: int, skipped_count: int) -> str:
        replaced = max(0, int(replaced_count or 0))
        skipped = max(0, int(skipped_count or 0))
        section = str(section_label or "Auto-Fill").strip() or "Auto-Fill"
        noun = "suggestion" if replaced == 1 else "suggestions"
        if skipped:
            skipped_noun = "suggestion" if skipped == 1 else "suggestions"
            return f"Replaced {replaced} {section} Auto-Fill {noun}; skipped {skipped} {skipped_noun}."
        return f"Replaced {replaced} {section} Auto-Fill {noun}."

    def _replace_autofill_preserved_suggestion(self, section: str, index: int) -> None:
        suggestions = list(self._autofill_preserved_suggestions.get(section, []) or [])
        if index < 0 or index >= len(suggestions):
            return
        suggestion = dict(suggestions[index])
        key = str(suggestion.get("key", "") or "").strip()
        label = str(suggestion.get("label", "") or "").strip() or key or "Field"
        suggested = str(suggestion.get("suggested", "") or "").strip()
        edit = self._autofill_target_edit(key)
        if edit is None or not suggested:
            self._publish_settings_action_feedback(
                status="partial",
                summary=f"Could not replace {label} Auto-Fill suggestion.",
                detail=f"{label}: no editable target found",
                action_type="replace_autofill_suggestion",
                source_surface=self._autofill_feedback_source_surface(section, "replace_suggestion"),
            )
            return
        previous = edit.text().strip()
        changed = self._normalized_path_text(previous) != self._normalized_path_text(suggested)
        if changed:
            edit.setText(suggested)
            self._mark_settings_dirty()
            self._refresh_section_titles()
            self._refresh_section_nav_health()
            self._refresh_contextual_autofill_buttons()
            self._publish_autofill_readiness_feedback(section)
        remaining = [dict(item) for pos, item in enumerate(suggestions) if pos != index]
        self._set_autofill_preserved_suggestions(section, remaining)
        detail = (
            f"{label}: replaced {previous or '(blank)'} with {suggested}"
            if changed
            else f"{label}: already matched {suggested}"
        )
        section_label = self._autofill_section_label(section)
        self._publish_settings_action_feedback(
            status="succeeded",
            summary=f"Replaced {label} Auto-Fill suggestion for {section_label}."
            if changed
            else f"{label} already matched the Auto-Fill suggestion.",
            detail=detail,
            action_type="replace_autofill_suggestion",
            source_surface=self._autofill_feedback_source_surface(section, "replace_suggestion"),
        )

    def _replace_autofill_preserved_suggestions(self, section: str) -> None:
        suggestions = list(self._autofill_preserved_suggestions.get(section, []) or [])
        if not suggestions:
            self._set_autofill_preserved_suggestions(section, ())
            return
        replaced_lines: List[str] = []
        unchanged_lines: List[str] = []
        skipped_lines: List[str] = []
        remaining: List[Dict[str, str]] = []
        for suggestion in suggestions:
            key = str(suggestion.get("key", "") or "").strip()
            label = str(suggestion.get("label", "") or "").strip() or key or "Field"
            suggested = str(suggestion.get("suggested", "") or "").strip()
            edit = self._autofill_target_edit(key)
            if edit is None or not suggested:
                skipped_lines.append(f"{label}: no editable target found")
                remaining.append(dict(suggestion))
                continue
            previous = edit.text().strip()
            if self._normalized_path_text(previous) == self._normalized_path_text(suggested):
                unchanged_lines.append(f"{label}: already matched {suggested}")
                continue
            edit.setText(suggested)
            replaced_lines.append(f"{label}: replaced {previous or '(blank)'} with {suggested}")
        if replaced_lines:
            self._mark_settings_dirty()
            self._refresh_section_titles()
            self._refresh_section_nav_health()
            self._refresh_contextual_autofill_buttons()
            self._publish_autofill_readiness_feedback(section)
        self._set_autofill_preserved_suggestions(section, remaining)
        detail_lines = [*replaced_lines, *unchanged_lines, *skipped_lines]
        section_label = self._autofill_section_label(section)
        self._publish_settings_action_feedback(
            status="partial" if skipped_lines else "succeeded",
            summary=self._autofill_replace_summary(section_label, len(replaced_lines), len(skipped_lines)),
            detail="\n".join(detail_lines),
            action_type="replace_autofill_suggestions",
            source_surface=self._autofill_feedback_source_surface(section, "replace_suggestions"),
        )

    def _autofill_target_edit(self, key: str) -> Optional[QLineEdit]:
        if key.startswith("message_paths."):
            origin = key.split(".", 1)[1]
            return self.msg_paths_edits.get(origin)
        if key == "fldigi_log_path":
            return self.fldigi_log_path_edit if hasattr(self, "fldigi_log_path_edit") else None
        if key == "js8_directed_path":
            return self.js8_directed_edit if hasattr(self, "js8_directed_edit") else None
        if key == "js8_forms_path":
            return self.js8_forms_edit if hasattr(self, "js8_forms_edit") else None
        if key == "path_js8call":
            return self.js8call_path_edit if hasattr(self, "js8call_path_edit") else None
        if key == "path_js8spotter":
            return self.js8spotter_path_edit if hasattr(self, "js8spotter_path_edit") else None
        if key == "path_commstat":
            return self.commstat_path_edit if hasattr(self, "commstat_path_edit") else None
        if key == "varac_path":
            return self.varac_path_edit if hasattr(self, "varac_path_edit") else None
        if key == "varac_ini_path":
            return self.varac_ini_path_edit if hasattr(self, "varac_ini_path_edit") else None
        if key == "varac_outbox_dir":
            return self.varac_outbox_dir_edit if hasattr(self, "varac_outbox_dir_edit") else None
        if key == "varac_bbs_dir":
            return self.varac_bbs_dir_edit if hasattr(self, "varac_bbs_dir_edit") else None
        if key == "varac_bbs_archive_dir":
            return self.varac_bbs_archive_dir_edit if hasattr(self, "varac_bbs_archive_dir_edit") else None
        for prog_name, meta in self.PROGRAMS.items():
            if meta.get("setting_key") == key:
                return self.path_edits.get(prog_name)
        return None

    @staticmethod
    def _normalized_path_text(value: str) -> str:
        txt = os.path.expanduser(os.path.expandvars(str(value or "").strip()))
        if not txt:
            return ""
        return os.path.normcase(os.path.normpath(txt))

    # ---------- LOAD/SAVE ---------- #

    def _load_settings(self):
        _perf_t0 = time.perf_counter()
        self._loading_settings = True
        data = self.settings.all()

        self.callsign_edit.setText(data.get("operator_callsign", "") or "")
        self.name_edit.setText(data.get("operator_name", "") or "")
        self.state_edit.setText(data.get("operator_state", "") or "")
        self.grid6_edit.setText(data.get("operator_grid6", "") or "")

        # Timezone: prefer stored; otherwise detect from system clock
        tz = data.get("timezone")
        if not tz:
            tz = self._detect_system_timezone()
            data["timezone"] = tz
            # Just keep this in-memory; persistence happens on explicit save or exit.
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        elif tz not in TIMEZONE_CHOICES:
            # Normalise unexpected values back into one of our known IDs
            detected = self._detect_system_timezone()
            data["timezone"] = detected
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]

        ctrl = data.get("control_via", "FLRig") or "FLRig"
        allowed_ctrl = ["FLRig", "RIGCTLD", "JS8Call", "Manual"]
        if ctrl not in allowed_ctrl:
            ctrl = "FLRig"
        self.control_combo.setCurrentText(ctrl)
        log_level = (data.get("log_level", "") or "DISABLED").strip().upper()
        if log_level not in {"DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"}:
            log_level = "DISABLED"
        if hasattr(self, "log_level_combo"):
            idx = self.log_level_combo.findText(log_level)
            self.log_level_combo.setCurrentIndex(idx if idx >= 0 else self.log_level_combo.findText("DISABLED"))
        self.use_scheduler_chk.setChecked(bool(data.get("use_scheduler", True)))
        freq_mode = (data.get("freq_enforcement_mode", "On Schedule Change") or "On Schedule Change").strip()
        fldigi_mode = (data.get("fldigi_enforcement_mode", "On Schedule Change") or "On Schedule Change").strip()
        js8_mode = (data.get("js8_enforcement_mode", "On Schedule Change") or "On Schedule Change").strip()
        if freq_mode not in {"On Schedule Change", "Prompt"}:
            freq_mode = "On Schedule Change"
        if fldigi_mode not in {"On Schedule Change", "Prompt"}:
            fldigi_mode = "On Schedule Change"
        if js8_mode not in {"On Schedule Change", "Prompt"}:
            js8_mode = "On Schedule Change"
        self.freq_enforce_combo.setCurrentText(freq_mode)
        self.fldigi_enforce_combo.setCurrentText(fldigi_mode)
        self.js8_enforce_combo.setCurrentText(js8_mode)

        freq_prompt = (data.get("freq_prompt_interval", "Hourly") or "Hourly").strip()
        fldigi_prompt = (data.get("fldigi_prompt_interval", "Hourly") or "Hourly").strip()
        js8_prompt = (data.get("js8_prompt_interval", "Hourly") or "Hourly").strip()
        prompt_choices = {"Hourly", "Every 5 minutes", "Every 10 minutes", "Every 15 minutes", "Every 30 minutes"}
        if freq_prompt not in prompt_choices:
            freq_prompt = ""
        if fldigi_prompt not in prompt_choices:
            fldigi_prompt = ""
        if js8_prompt not in prompt_choices:
            js8_prompt = ""
        self.freq_prompt_combo.setCurrentText(freq_prompt or "Select Interval")
        self.fldigi_prompt_combo.setCurrentText(fldigi_prompt or "Select Interval")
        self.js8_prompt_combo.setCurrentText(js8_prompt or "Select Interval")
        if freq_mode != "Prompt":
            self.freq_prompt_combo.setCurrentText("Select Interval")
        if fldigi_mode != "Prompt":
            self.fldigi_prompt_combo.setCurrentText("Select Interval")
        if js8_mode != "Prompt":
            self.js8_prompt_combo.setCurrentText("Select Interval")
        self._update_enforcement_visibility()
        theme = (data.get("ui_theme", "light") or "light").strip().lower()
        self.theme_combo.setCurrentText("Dark" if theme == "dark" else "Light")
        ui_text_size = normalize_ui_text_size(data.get("ui_text_size", "normal"))
        self.text_size_combo.setCurrentText(
            "Normal" if ui_text_size == "normal" else ("Medium" if ui_text_size == "medium" else "Large")
        )

        js8_host_txt = str(data.get("js8_host", "") or "").strip() or "127.0.0.1"
        self.js8_host_edit.setText(js8_host_txt)
        port_txt = str(data.get("js8_port", "2442") or "2442")
        self.js8_port_edit.setText(port_txt)
        offset_val = data.get("js8_offset_hz", None)
        try:
            offset_int = int(offset_val) if offset_val not in (None, "") else 0
        except Exception:
            offset_int = 0
        if offset_int <= 0:
            offset_int = coerce_js8_offset_hz(offset_int)
            if hasattr(self.settings, "set"):
                self.settings.set("js8_offset_hz", offset_int)
            else:
                data["js8_offset_hz"] = offset_int
                if hasattr(self.settings, "_data"):
                    self.settings._data = data  # type: ignore[attr-defined]
        self.js8_offset_edit.setText(str(offset_int))
        self.js8_forms_edit.setText(data.get("js8_forms_path", "") or "")
        if MAPPER_SETTINGS_KEY not in data:
            try:
                self.settings.set(MAPPER_SETTINGS_KEY, normalize_mapping_rows([]))
            except Exception:
                pass
        self._refresh_spotter_form_mapper()
        self.js8call_path_edit.setText((data.get("path_js8call", "") or "").strip())
        self.js8spotter_path_edit.setText((data.get("path_js8spotter", "") or "").strip())
        self.commstat_path_edit.setText((data.get("path_commstat", "") or "").strip())
        self.js8_mark_retrieved_chk.setChecked(
            bool(data.get("js8_inbox_mark_retrieved_sync", False))
        )
        # Message paths
        msg_paths = data.get("message_paths", {})
        for origin, edit in self.msg_paths_edits.items():
            edit.setText(msg_paths.get(origin, ""))
        gpg_enabled = bool(data.get("gpg_verify_flamp_k2s_enabled", False))
        hash_enabled = bool(data.get("hash_verify_flamp_k2s_enabled", True))
        gpg_path = str(data.get("gpg_executable_path", "") or "").strip()
        trusted = data.get("gpg_trusted_signers", [])
        if not isinstance(trusted, list):
            trusted = []
        trusted_hashes_raw = data.get("trusted_file_hashes", [])
        if not isinstance(trusted_hashes_raw, list):
            trusted_hashes_raw = []
        self._gpg_trusted_fingerprints = {normalize_fingerprint(v) for v in trusted if normalize_fingerprint(v)}
        self._trusted_hash_entries = normalize_trusted_hash_entries(trusted_hashes_raw)
        if hasattr(self, "gpg_verify_enabled_chk"):
            self.gpg_verify_enabled_chk.setChecked(gpg_enabled)
        if hasattr(self, "hash_verify_enabled_chk"):
            self.hash_verify_enabled_chk.setChecked(hash_enabled)
        if hasattr(self, "gpg_path_edit"):
            self.gpg_path_edit.setText(gpg_path)
        self._refresh_trusted_hash_table()
        self._gpg_keys_loaded = False
        self._gpg_keys_auto_probe_attempted = False
        if hasattr(self, "gpg_keys_table"):
            self._gpg_keys_table_loading = True
            try:
                self.gpg_keys_table.setRowCount(0)
            finally:
                self._gpg_keys_table_loading = False
        if hasattr(self, "gpg_signing_key_combo"):
            self._gpg_signing_keys_loading = True
            try:
                self.gpg_signing_key_combo.clear()
                self.gpg_signing_key_combo.addItem("Auto-select when only one private key is available", "")
                saved_signing_fpr = normalize_fingerprint(
                    str(data.get("gpg_compose_signing_key_fingerprint", "") or "")
                )
                if saved_signing_fpr:
                    self.gpg_signing_key_combo.addItem(f"Saved default - {saved_signing_fpr[-16:]}", saved_signing_fpr)
                    self.gpg_signing_key_combo.setCurrentIndex(1)
            finally:
                self._gpg_signing_keys_loading = False
        if hasattr(self, "gpg_signing_status_label"):
            self.gpg_signing_status_label.setText(
                "Signing keys not loaded. Click Refresh Signing Keys to detect private keys from GPG."
            )
        self._set_gpg_status("GPG status: keys not loaded. Open this section or click Refresh Keys.")
        varac_path = (data.get("varac_path", "") or "").strip()
        if not varac_path:
            legacy_db = (data.get("varac_db_path", "") or "").strip()
            if legacy_db:
                try:
                    legacy = Path(legacy_db)
                    if legacy.is_file():
                        varac_path = str(legacy.parent)
                    elif legacy.is_dir():
                        varac_path = str(legacy)
                except Exception:
                    varac_path = legacy_db
        if hasattr(self, "varac_path_edit"):
            self.varac_path_edit.setText(varac_path)
        if hasattr(self, "varac_ini_path_edit"):
            ini_guess = (data.get("varac_ini_path", "") or "").strip()
            if not ini_guess and varac_path:
                ini_guess = locate_varac_ini_path(varac_path)
            self.varac_ini_path_edit.setText(ini_guess)
        if hasattr(self, "varac_launch_cmd_edit"):
            self.varac_launch_cmd_edit.setText((data.get("varac_launch_cmd", "") or "").strip())
        if hasattr(self, "varac_outbox_dir_edit"):
            self.varac_outbox_dir_edit.setText((data.get("varac_outbox_dir", "") or "").strip())
        if hasattr(self, "varac_bbs_dir_edit"):
            self.varac_bbs_dir_edit.setText((data.get("varac_bbs_dir", "") or "").strip())
        if hasattr(self, "varac_bbs_archive_dir_edit"):
            self.varac_bbs_archive_dir_edit.setText((data.get("varac_bbs_archive_dir", "") or "").strip())
        if hasattr(self, "varac_bbs_auto_archive_chk"):
            self.varac_bbs_auto_archive_chk.setChecked(bool(data.get("varac_bbs_auto_archive_enabled", False)))
        if hasattr(self, "varac_bbs_archive_days_combo"):
            allowed_days = {"1", "3", "5", "7", "10", "14", "21", "30"}
            day_val = str(data.get("varac_bbs_auto_archive_days", 14) or "14")
            if day_val not in allowed_days:
                day_val = "14"
            self.varac_bbs_archive_days_combo.setCurrentText(day_val)
        if hasattr(self, "varac_bbs_enabled_chk"):
            self.varac_bbs_enabled_chk.setChecked(bool(data.get("varac_bbs_enabled", False)))
        if hasattr(self, "varac_bbs_limit_access_chk"):
            self.varac_bbs_limit_access_chk.setChecked(bool(data.get("varac_bbs_limit_access_enabled", False)))
        if hasattr(self, "varac_bbs_announce_chk"):
            self.varac_bbs_announce_chk.setChecked(bool(data.get("varac_bbs_announce_enabled", False)))
        if hasattr(self, "varac_bbs_callsigns_list"):
            self._set_varac_bbs_allowed_callsigns(data.get("varac_bbs_allowed_callsigns", ""))
        if hasattr(self, "varac_guard_enabled_chk"):
            self.varac_guard_enabled_chk.setChecked(bool(data.get("varac_guard_enabled", False)))
        if hasattr(self, "varac_guard_mode_combo"):
            guard_mode = str(data.get("varac_guard_mode", "Log only") or "Log only").strip()
            if guard_mode not in {"Log only", "Delete unauthorized files", "Quarantine unauthorized files"}:
                guard_mode = "Log only"
            self.varac_guard_mode_combo.setCurrentText(guard_mode)
        if hasattr(self, "varac_guard_allow_bbs_chk"):
            self.varac_guard_allow_bbs_chk.setChecked(bool(data.get("varac_guard_allow_bbs_allowed_callsigns", True)))
        if hasattr(self, "varac_guard_allow_trusted_chk"):
            self.varac_guard_allow_trusted_chk.setChecked(bool(data.get("varac_guard_allow_operator_trusted", True)))
        if hasattr(self, "varac_guard_quarantine_dir_edit"):
            self.varac_guard_quarantine_dir_edit.setText(str(data.get("varac_guard_quarantine_dir", "") or "").strip())
        if hasattr(self, "varac_guard_retry_combo"):
            retry_txt = str(data.get("varac_guard_retry_seconds", 120) or 120).strip()
            if retry_txt not in {"30", "60", "120", "300", "600"}:
                retry_txt = "120"
            self.varac_guard_retry_combo.setCurrentText(retry_txt)
        if hasattr(self, "varac_guard_status_label"):
            guard_summary = str(data.get("varac_guard_last_summary", "") or "").strip()
            self.varac_guard_status_label.setText(guard_summary or "No VGuard scan yet.")
        try:
            if hasattr(self, "varac_ini_path_edit") and self.varac_ini_path_edit.text().strip():
                self._varac_bbs_ini_sync_state = varac_ini_sync_state_to_json(
                    get_varac_ini_sync_state(self.varac_ini_path_edit.text().strip())
                )
        except Exception:
            self._varac_bbs_ini_sync_state = ""
        if hasattr(self, "sop_export_preamble_edit"):
            self.sop_export_preamble_edit.setPlainText(str(data.get("sop_export_preamble", "") or ""))
        if hasattr(self, "sop_export_postamble_edit"):
            self.sop_export_postamble_edit.setPlainText(str(data.get("sop_export_postamble", "") or ""))
        fldigi_log_path = (data.get("fldigi_log_path", "") or "").strip()
        if hasattr(self, "fldigi_log_path_edit"):
            self.fldigi_log_path_edit.setText(fldigi_log_path)
        fldigi_dir = (data.get("fldigi_checkin_dir", "") or "").strip()
        if not fldigi_dir:
            fldigi_dir = str(get_fldigi_checkin_dir())
            if hasattr(self.settings, "set"):
                self.settings.set("fldigi_checkin_dir", fldigi_dir)
            else:
                data["fldigi_checkin_dir"] = fldigi_dir
                if hasattr(self.settings, "_data"):
                    self.settings._data = data  # type: ignore[attr-defined]
        self.fldigi_checkin_dir_edit.setText(fldigi_dir)
        self._refresh_fldigi_checkin_file_labels()
        fldigi_host_txt = self._resolved_fldigi_host_value(data)
        self.fldigi_host_edit.setText(fldigi_host_txt)
        fldigi_port_txt = str(data.get("fldigi_port", "7362") or "7362")
        self.fldigi_port_edit.setText(fldigi_port_txt)
        flrig_port_txt = str(data.get("flrig_port", "12345") or "12345")
        self.flrig_port_edit.setText(flrig_port_txt)

        groups = data.get("primary_js8_groups", [])
        if not isinstance(groups, list):
            groups = []
        for i, le in enumerate(self.js8_groups_edits):
            le.setText(groups[i] if i < len(groups) else "")

        # Load operating groups
        try:
            og = data.get("operating_groups", [])
            if isinstance(og, list):
                self.operating_groups = []
                for g in og:
                    if not isinstance(g, dict):
                        continue
                    vfo_val = (g.get("vfo") or "A").strip().upper()
                    if vfo_val not in ("A", "B"):
                        vfo_val = "A"
                    try:
                        cond_level = int(g.get("condition_level", 5) or 5)
                    except Exception:
                        cond_level = 5
                    if cond_level < 1 or cond_level > 5:
                        cond_level = 5
                    band_val = str(g.get("band", "") or "").strip().upper()
                    mode_val = normalize_operating_group_mode(g.get("mode", ""), band_val)
                    self.operating_groups.append(
                        {
                            "group": str(g.get("group", "")).upper(),
                            "mode": mode_val,
                            "band": band_val,
                            "frequency": g.get("frequency", ""),
                            "vfo": vfo_val,
                            "fldigi_mode": (g.get("fldigi_mode") or "").strip(),
                            "fldigi_offset": (g.get("fldigi_offset") or "").strip(),
                            "auto_tune": bool(g.get("auto_tune", False)),
                            "use_condition_levels": bool(g.get("use_condition_levels", False)),
                            "condition_level": cond_level,
                        }
                    )
        except Exception:
            self.operating_groups = []
        self._refresh_operating_groups_table()

        # Load local net profiles (SOP local-net reminder metadata only).
        try:
            lnp = data.get("local_net_profiles", [])
            if isinstance(lnp, list):
                self.local_net_profiles = []
                for row in lnp:
                    if not isinstance(row, dict):
                        continue
                    normalized = self._normalize_local_net_profile(row)
                    if normalized.get("group"):
                        self.local_net_profiles.append(normalized)
            else:
                self.local_net_profiles = []
        except Exception:
            self.local_net_profiles = []
        self._refresh_local_net_profiles_table()

        self.js8_directed_edit.setText(data.get("js8_directed_path", "") or "")

        for prog_name, meta in self.PROGRAMS.items():
            path_key = meta["setting_key"]

            if path_key:
                self.path_edits[prog_name].setText(data.get(path_key, "") or "")

        self._custom_tool_items_cache = self.launch_orchestrator.get_custom_tools()
        self._refresh_custom_tools_table()
        self._launch_items_cache = self.launch_orchestrator.get_launch_items()
        launch_all = bool(self.settings.get("launch_control_enabled", data.get("launch_control_enabled", True)))
        self.launch_all_with_startup_chk.setChecked(launch_all)
        self._refresh_launch_control_table()
        self._refresh_varac_cluster_mode_ui(refresh_tables=False)
        self._refresh_multi_radio_tables(refresh_section_titles=False)

        log.info("SettingsTab: settings loaded.")
        self._update_launch_control_buttons()
        self._update_device_profile_action_buttons()
        self._update_operating_profile_action_buttons()
        self._update_device_assignment_action_buttons()
        self._update_op_group_action_buttons()
        self._update_local_net_action_buttons()
        self._loading_settings = False
        self._settings_dirty = False
        self._set_save_button_state("success")
        self._refresh_radio_context_labels()
        self._refresh_section_titles()
        self._refresh_contextual_autofill_buttons()
        emit_span(
            "settings.load_settings",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=10.0,
        )

    def _save_settings_button(self):
        """Explicit save via the button (shows confirmation)."""
        self._save_settings(show_message=True)
        # Defer settings fanout one tick to keep Save interaction responsive.
        QTimer.singleShot(0, self._emit_settings_saved)
        QTimer.singleShot(0, self._maybe_backfill_js8_geo)
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _emit_settings_saved(self) -> None:
        try:
            self.settings_saved.emit()
        except Exception:
            pass

    def _save_settings_quiet(self):
        """Auto-save on application exit (no dialog)."""
        self._shutdown_autosave = True
        try:
            self._save_settings(show_message=False)
        finally:
            self._shutdown_autosave = False

    def _resolved_fldigi_host_value(self, data: Optional[Dict[str, object]] = None) -> str:
        if isinstance(data, dict):
            host_txt = str(data.get("fldigi_host", "") or "").strip()
            if host_txt:
                return host_txt
            host_txt = str(data.get("flrig_host", "") or "").strip()
            if host_txt:
                return host_txt
        try:
            host_txt = str(self.settings.get("fldigi_host", "") or "").strip()
            if host_txt:
                return host_txt
        except Exception:
            pass
        try:
            host_txt = str(self.settings.get("flrig_host", "") or "").strip()
            if host_txt:
                return host_txt
        except Exception:
            pass
        return "127.0.0.1"

    def _save_settings(self, show_message: bool = True):
        _perf_t0 = time.perf_counter()
        if self._varac_bbs_vault_editor_has_pending_changes():
            if not show_message:
                log.info("SettingsTab: skipped pending Managed BBS location editor changes during quiet shutdown save.")
                return
            if not self._save_varac_bbs_vault_location():
                return
        data = self.settings.all()
        prev_operator = {
            "callsign": str(data.get("operator_callsign", "") or "").strip().upper(),
            "name": str(data.get("operator_name", "") or "").strip(),
            "state": str(data.get("operator_state", "") or "").strip().upper(),
            "grid6": str(data.get("operator_grid6", "") or "").strip().upper(),
        }

        data["operator_callsign"] = self.callsign_edit.text().strip()
        data["operator_name"] = self.name_edit.text().strip()
        data["operator_state"] = self.state_edit.text().strip()
        data["operator_grid6"] = self.grid6_edit.text().strip().upper()
        data["operator_grid6"] = self.grid6_edit.text().strip().upper()
        operator_changed = (
            prev_operator["callsign"] != str(data["operator_callsign"]).strip().upper()
            or prev_operator["name"] != str(data["operator_name"]).strip()
            or prev_operator["state"] != str(data["operator_state"]).strip().upper()
            or prev_operator["grid6"] != str(data["operator_grid6"]).strip().upper()
        )

        # Timezone is not user-editable; keep existing value (or detect if missing)
        tz = data.get("timezone")
        if not tz:
            tz = self._detect_system_timezone()
            data["timezone"] = tz

        data["control_via"] = self.control_combo.currentText().strip()
        data["log_level"] = (
            self.log_level_combo.currentText().strip().upper() if hasattr(self, "log_level_combo") else "DISABLED"
        )
        data["use_scheduler"] = bool(self.use_scheduler_chk.isChecked())
        freq_mode = self.freq_enforce_combo.currentText().strip()
        fldigi_mode = self.fldigi_enforce_combo.currentText().strip()
        js8_mode = self.js8_enforce_combo.currentText().strip()
        freq_prompt = self.freq_prompt_combo.currentText().strip()
        fldigi_prompt = self.fldigi_prompt_combo.currentText().strip()
        js8_prompt = self.js8_prompt_combo.currentText().strip()
        missing = []
        if freq_mode == "Prompt" and freq_prompt == "Select Interval":
            missing.append("Frequency Prompt Interval")
        if fldigi_mode == "Prompt" and fldigi_prompt == "Select Interval":
            missing.append("FLDigi Prompt Interval")
        if js8_mode == "Prompt" and js8_prompt == "Select Interval":
            missing.append("JS8 Prompt Interval")
        if missing:
            if not show_message:
                if freq_mode == "Prompt" and freq_prompt == "Select Interval":
                    freq_prompt = str(data.get("freq_prompt_interval", "Hourly") or "Hourly")
                    if freq_prompt == "Select Interval":
                        freq_prompt = "Hourly"
                if fldigi_mode == "Prompt" and fldigi_prompt == "Select Interval":
                    fldigi_prompt = str(data.get("fldigi_prompt_interval", "Hourly") or "Hourly")
                    if fldigi_prompt == "Select Interval":
                        fldigi_prompt = "Hourly"
                if js8_mode == "Prompt" and js8_prompt == "Select Interval":
                    js8_prompt = str(data.get("js8_prompt_interval", "Hourly") or "Hourly")
                    if js8_prompt == "Select Interval":
                        js8_prompt = "Hourly"
                log.info("SettingsTab: defaulted missing prompt interval(s) during quiet shutdown save: %s", ", ".join(missing))
            else:
                missing_text = self._human_join(missing)
                self._block_settings_action(
                    f"Save blocked: select {missing_text}.",
                    "Choose a prompt interval for each Prompt enforcement mode before saving.",
                )
                return
        data["freq_enforcement_mode"] = freq_mode
        data["freq_prompt_interval"] = freq_prompt
        data["fldigi_enforcement_mode"] = fldigi_mode
        data["fldigi_prompt_interval"] = fldigi_prompt
        data["js8_enforcement_mode"] = js8_mode
        data["js8_prompt_interval"] = js8_prompt
        data["ui_theme"] = self.theme_combo.currentText().strip().lower()
        data["ui_text_size"] = normalize_ui_text_size(self.text_size_combo.currentText())

        host_val = self.js8_host_edit.text().strip() if hasattr(self, "js8_host_edit") else ""
        if not host_val:
            host_val = "127.0.0.1"
            if hasattr(self, "js8_host_edit"):
                self.js8_host_edit.setText(host_val)
        data["js8_host"] = host_val
        try:
            port_val = int(self.js8_port_edit.text().strip() or "2442")
        except ValueError:
            port_val = 2442
            self.js8_port_edit.setText("2442")
        data["js8_port"] = port_val
        try:
            flrig_port_val = int(self.flrig_port_edit.text().strip() or "12345")
        except ValueError:
            flrig_port_val = 12345
            self.flrig_port_edit.setText("12345")
        data["flrig_port"] = flrig_port_val
        fldigi_host_val = self.fldigi_host_edit.text().strip() if hasattr(self, "fldigi_host_edit") else ""
        if not fldigi_host_val:
            fldigi_host_val = self._resolved_fldigi_host_value(data)
            if hasattr(self, "fldigi_host_edit"):
                self.fldigi_host_edit.setText(fldigi_host_val)
        data["fldigi_host"] = fldigi_host_val
        try:
            fldigi_port_val = int(self.fldigi_port_edit.text().strip() or "7362")
        except ValueError:
            fldigi_port_val = 7362
            self.fldigi_port_edit.setText("7362")
        data["fldigi_port"] = fldigi_port_val
        try:
            offset_val = int(self.js8_offset_edit.text().strip() or "0")
        except ValueError:
            offset_val = coerce_js8_offset_hz(0)
            self.js8_offset_edit.setText(str(offset_val))
        if offset_val <= 0:
            offset_val = coerce_js8_offset_hz(offset_val)
            self.js8_offset_edit.setText(str(offset_val))
        data["js8_offset_hz"] = offset_val

        data["js8_forms_path"] = self.js8_forms_edit.text().strip()
        data[MAPPER_SETTINGS_KEY] = self._collect_spotter_form_mappings()
        data["path_js8call"] = self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else ""
        data["path_js8spotter"] = (
            self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else ""
        )
        data["path_commstat"] = self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else ""
        data["js8_inbox_mark_retrieved_sync"] = bool(self.js8_mark_retrieved_chk.isChecked())
        msg_paths = {}
        for origin, edit in self.msg_paths_edits.items():
            msg_paths[origin] = edit.text().strip()
        data["message_paths"] = msg_paths
        data["gpg_verify_flamp_k2s_enabled"] = bool(
            self.gpg_verify_enabled_chk.isChecked() if hasattr(self, "gpg_verify_enabled_chk") else False
        )
        data["hash_verify_flamp_k2s_enabled"] = bool(
            self.hash_verify_enabled_chk.isChecked() if hasattr(self, "hash_verify_enabled_chk") else True
        )
        data["gpg_executable_path"] = self.gpg_path_edit.text().strip() if hasattr(self, "gpg_path_edit") else ""
        data["gpg_trusted_signers"] = sorted(
            [fp for fp in self._gpg_trusted_fingerprints if normalize_fingerprint(fp)]
        )
        data["gpg_compose_signing_key_fingerprint"] = normalize_fingerprint(
            str(self.gpg_signing_key_combo.currentData() or "")
            if hasattr(self, "gpg_signing_key_combo")
            else ""
        )
        data["trusted_file_hashes"] = [
            {
                "enabled": bool(row.get("enabled", True)),
                "algorithm": normalize_hash_algorithm(str(row.get("algorithm", "") or "")),
                "hash": normalize_hash_hex(str(row.get("hash", "") or "")),
                "label": str(row.get("label", "") or "").strip(),
            }
            for row in normalize_trusted_hash_entries(self._trusted_hash_entries)
        ]
        varac_path = self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else ""
        data["varac_path"] = varac_path
        data["varac_db_path"] = str(Path(varac_path) / "VarAC.db") if varac_path else ""
        data["varac_ini_path"] = (
            self.varac_ini_path_edit.text().strip() if hasattr(self, "varac_ini_path_edit") else ""
        )
        data["varac_launch_cmd"] = (
            self.varac_launch_cmd_edit.text().strip() if hasattr(self, "varac_launch_cmd_edit") else ""
        )
        data["varac_outbox_dir"] = (
            self.varac_outbox_dir_edit.text().strip() if hasattr(self, "varac_outbox_dir_edit") else ""
        )
        data["varac_bbs_dir"] = (
            self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        )
        data["varac_bbs_archive_dir"] = (
            self.varac_bbs_archive_dir_edit.text().strip() if hasattr(self, "varac_bbs_archive_dir_edit") else ""
        )
        days_text = (
            self.varac_bbs_archive_days_combo.currentText().strip()
            if hasattr(self, "varac_bbs_archive_days_combo")
            else "14"
        )
        if days_text not in {"1", "3", "5", "7", "10", "14", "21", "30"}:
            days_text = "14"
        data["varac_bbs_auto_archive_enabled"] = bool(
            self.varac_bbs_auto_archive_chk.isChecked() if hasattr(self, "varac_bbs_auto_archive_chk") else False
        )
        data["varac_bbs_auto_archive_days"] = int(days_text)
        data["varac_bbs_enabled"] = bool(
            self.varac_bbs_enabled_chk.isChecked() if hasattr(self, "varac_bbs_enabled_chk") else False
        )
        data["varac_bbs_limit_access_enabled"] = bool(
            self.varac_bbs_limit_access_chk.isChecked() if hasattr(self, "varac_bbs_limit_access_chk") else False
        )
        data["varac_bbs_announce_enabled"] = bool(
            self.varac_bbs_announce_chk.isChecked() if hasattr(self, "varac_bbs_announce_chk") else False
        )
        data["varac_bbs_allowed_callsigns"] = self._varac_bbs_selected_callsigns_text()
        data["varac_guard_enabled"] = bool(
            self.varac_guard_enabled_chk.isChecked() if hasattr(self, "varac_guard_enabled_chk") else False
        )
        data["varac_guard_mode"] = (
            self.varac_guard_mode_combo.currentText().strip() if hasattr(self, "varac_guard_mode_combo") else "Log only"
        )
        data["varac_guard_allow_bbs_allowed_callsigns"] = bool(
            self.varac_guard_allow_bbs_chk.isChecked() if hasattr(self, "varac_guard_allow_bbs_chk") else True
        )
        data["varac_guard_allow_operator_trusted"] = bool(
            self.varac_guard_allow_trusted_chk.isChecked() if hasattr(self, "varac_guard_allow_trusted_chk") else True
        )
        retry_txt = self.varac_guard_retry_combo.currentText().strip() if hasattr(self, "varac_guard_retry_combo") else "120"
        if retry_txt not in {"30", "60", "120", "300", "600"}:
            retry_txt = "120"
        data["varac_guard_retry_seconds"] = int(retry_txt)
        data["varac_guard_quarantine_dir"] = (
            self.varac_guard_quarantine_dir_edit.text().strip() if hasattr(self, "varac_guard_quarantine_dir_edit") else ""
        )
        try:
            if hasattr(self, "varac_ini_path_edit") and self.varac_ini_path_edit.text().strip():
                self._varac_bbs_ini_sync_state = varac_ini_sync_state_to_json(
                    get_varac_ini_sync_state(self.varac_ini_path_edit.text().strip())
                )
        except Exception:
            pass
        data["varac_cluster_mode_enabled"] = bool(
            self.varac_cluster_mode_chk.isChecked() if hasattr(self, "varac_cluster_mode_chk") else False
        )
        data["sop_export_preamble"] = (
            self.sop_export_preamble_edit.toPlainText() if hasattr(self, "sop_export_preamble_edit") else ""
        )
        data["sop_export_postamble"] = (
            self.sop_export_postamble_edit.toPlainText() if hasattr(self, "sop_export_postamble_edit") else ""
        )
        if data["varac_bbs_auto_archive_enabled"]:
            bbs_dir_txt = data["varac_bbs_dir"]
            bbs_archive_txt = data["varac_bbs_archive_dir"]
            if not bbs_dir_txt or not bbs_archive_txt:
                QMessageBox.warning(
                    self,
                    "Settings",
                    "When Auto-Archive BBS Files is enabled, both BBS Directory and BBS Archive are required.",
                )
                return
            bbs_dir = Path(bbs_dir_txt)
            bbs_archive = Path(bbs_archive_txt)
            if not bbs_dir.exists() or not bbs_dir.is_dir() or not bbs_archive.exists() or not bbs_archive.is_dir():
                QMessageBox.warning(
                    self,
                    "Settings",
                    "BBS Directory and BBS Archive must both exist and be directories when auto-archive is enabled.",
                )
                return
            try:
                if bbs_dir.resolve() == bbs_archive.resolve():
                    QMessageBox.warning(
                        self,
                        "Settings",
                        "BBS Directory and BBS Archive must be different directories.",
                    )
                    return
            except Exception:
                pass
        if data["varac_bbs_limit_access_enabled"] and not data["varac_bbs_allowed_callsigns"]:
            QMessageBox.warning(
                self,
                "Settings",
                "Limit Access To Callsigns is enabled, but no allowed callsigns are configured.",
            )
            return
        if hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked():
            root_txt = self.varac_bbs_vault_root_edit.text().strip()
            if not data["varac_bbs_dir"]:
                QMessageBox.warning(
                    self,
                    "Managed BBS Vault",
                    "Set the VarAC BBS directory before enabling Managed BBS Vault.",
                )
                return
            if not root_txt:
                QMessageBox.warning(
                    self,
                    "Managed BBS Vault",
                    "Set the Managed Root before enabling Managed BBS Vault.",
                )
                return
            locations = load_vault_locations(self._varac_bbs_vault_locations_cache)
            if not locations:
                QMessageBox.warning(
                    self,
                    "Managed BBS Vault",
                    "Initialize the Managed BBS Vault or add at least one location before saving.",
                )
                return
            default_location_id = (
                str(self.varac_bbs_vault_default_location_combo.currentData() or "").strip() or DEFAULT_LOCATION_ID
            )
            default_location = next((loc for loc in locations if loc.id == default_location_id), None)
            if default_location is None:
                QMessageBox.warning(
                    self,
                    "Managed BBS Vault",
                    "Choose a valid Default location before saving.",
                )
                return
            missing_sources = [
                str(loc.source_dir)
                for loc in locations
                if loc.enabled and (not loc.source_dir or not Path(loc.source_dir).expanduser().is_dir())
            ]
            if missing_sources:
                QMessageBox.warning(
                    self,
                    "Managed BBS Vault",
                    "Every enabled location must point to an existing directory.\n\n"
                    + "\n".join(missing_sources[:5]),
                )
                return
            code_policy = str(data.get("varac_bbs_vault_global_code_policy", DEFAULT_GLOBAL_CODE_POLICY) or DEFAULT_GLOBAL_CODE_POLICY).strip()
            if code_policy != "Allow public locations":
                missing_codes = [
                    loc.name
                    for loc in locations
                    if loc.id != data["varac_bbs_vault_default_location_id"]
                    and loc.enabled
                    and str(loc.open_rule or "Public").strip() != "Public"
                    and not str(loc.access_code_hash or "").strip()
                ]
                if missing_codes:
                    QMessageBox.warning(
                        self,
                        "Managed BBS Vault",
                        "Current code policy requires access codes for these enabled locations:\n\n"
                        + "\n".join(missing_codes[:8]),
                    )
                    return
            if data.get("varac_bbs_vault_flamp_enabled", False):
                relay_dir = str(data.get("varac_bbs_vault_flamp_relay_dir", "") or "").strip()
                if not relay_dir or not Path(relay_dir).expanduser().is_dir():
                    QMessageBox.warning(
                        self,
                        "Managed BBS Vault",
                        "Set a valid FLAMP relay folder before enabling FLAMP relay service.",
                    )
                    return
        data["fldigi_log_path"] = (
            self.fldigi_log_path_edit.text().strip() if hasattr(self, "fldigi_log_path_edit") else ""
        )
        fldigi_dir = self.fldigi_checkin_dir_edit.text().strip()
        if not fldigi_dir:
            fldigi_dir = str(get_fldigi_checkin_dir())
            self.fldigi_checkin_dir_edit.setText(fldigi_dir)
        data["fldigi_checkin_dir"] = fldigi_dir

        groups = [le.text().strip().upper() for le in self.js8_groups_edits if le.text().strip()]
        data["primary_js8_groups"] = groups

        data["js8_directed_path"] = self.js8_directed_edit.text().strip()

        # Radio software paths from UI
        for prog_name, meta in self.PROGRAMS.items():
            path_key = meta["setting_key"]

            if path_key:
                data[path_key] = self.path_edits[prog_name].text().strip()

        # Launch Control settings
        self._sync_launch_cache_from_table()
        data["custom_tool_items"] = [dict(item) for item in self._custom_tool_items_cache]
        data["launch_control_items"] = self.launch_orchestrator.build_default_items(
            [dict(item) for item in self._launch_items_cache],
            custom_tools=self._custom_tool_items_cache,
        )
        data["launch_control_enabled"] = bool(self.launch_all_with_startup_chk.isChecked())
        data["launch_control_migrated_v1"] = True
        data["launch_readiness_timeout_sec"] = int(
            self.settings.get("launch_readiness_timeout_sec", DEFAULT_LAUNCH_READINESS_TIMEOUT_SEC)
            or DEFAULT_LAUNCH_READINESS_TIMEOUT_SEC
        )
        startup_by_name = {
            str(item.get("name", "")).strip(): bool(item.get("startup", False))
            for item in data["launch_control_items"]
            if isinstance(item, dict)
        }
        enabled_by_name = {
            str(item.get("name", "")).strip(): bool(item.get("enabled", False))
            for item in data["launch_control_items"]
            if isinstance(item, dict)
        }
        data["autostart_flrig"] = bool(startup_by_name.get("FLRig", False))
        data["autostart_fldigi"] = bool(startup_by_name.get("FLDigi", False))
        data["autostart_flmsg"] = bool(startup_by_name.get("FLMsg", False))
        data["autostart_flamp"] = bool(startup_by_name.get("FLAmp", False))
        data["autostart_js8call"] = bool(startup_by_name.get("JS8Call", False))

        launch_target_by_name: Dict[str, str] = {
            "FLRig": str(data.get("path_flrig", "") or "").strip(),
            "FLDigi": str(data.get("path_fldigi", "") or "").strip(),
            "FLMsg": str(data.get("path_flmsg", "") or "").strip(),
            "FLAmp": str(data.get("path_flamp", "") or "").strip(),
            "VarAC": str(data.get("varac_path", "") or "").strip(),
            "JS8Call": str(data.get("path_js8call", "") or "").strip(),
            "JS8Spotter": str(data.get("path_js8spotter", "") or "").strip(),
            "CommStat": str(data.get("path_commstat", "") or "").strip(),
        }
        for item in data.get("custom_tool_items", []):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            command = str(item.get("command", "")).strip()
            if name and command:
                launch_target_by_name[name] = command
        self_target_apps = [
            name
            for name, target in launch_target_by_name.items()
            if enabled_by_name.get(name, False) and self._looks_like_self_launch_target(target)
        ]
        if self_target_apps:
            QMessageBox.warning(
                self,
                "Launch Control",
                "Launch path appears to point back to FreqInOut for:\n"
                f"{', '.join(self_target_apps)}\n\n"
                "This can cause repeated already-running windows and extra Python processes. "
                "Choose the correct app executable/script path.",
            )
            return

        data["operating_groups"] = self._table_to_operating_groups()
        data["local_net_profiles"] = self._table_to_local_net_profiles()
        if not self._persist_staged_radio_software_bundles():
            return
        try:
            set_log_level(str(data.get("log_level", "DISABLED") or "DISABLED"))
        except Exception:
            pass

        # Persist with a single write when possible.
        if hasattr(self.settings, "set_many"):
            batch = {
                "operator_callsign": data["operator_callsign"],
                "operator_name": data["operator_name"],
                "operator_state": data["operator_state"],
                "operator_grid6": data["operator_grid6"],
                "timezone": data["timezone"],
                "control_via": data["control_via"],
                "log_level": data.get("log_level", "DISABLED"),
                "use_scheduler": data["use_scheduler"],
                "freq_enforcement_mode": data.get("freq_enforcement_mode", "On Schedule Change"),
                "freq_prompt_interval": data.get("freq_prompt_interval", "Hourly"),
                "fldigi_enforcement_mode": data.get("fldigi_enforcement_mode", "On Schedule Change"),
                "fldigi_prompt_interval": data.get("fldigi_prompt_interval", "Hourly"),
                "js8_enforcement_mode": data.get("js8_enforcement_mode", "On Schedule Change"),
                "js8_prompt_interval": data.get("js8_prompt_interval", "Hourly"),
                "ui_theme": data.get("ui_theme", "light"),
                "primary_js8_groups": data["primary_js8_groups"],
                MAPPER_SETTINGS_KEY: data.get(MAPPER_SETTINGS_KEY, []),
                "js8_inbox_mark_retrieved_sync": data.get("js8_inbox_mark_retrieved_sync", False),
                "gpg_verify_flamp_k2s_enabled": data.get("gpg_verify_flamp_k2s_enabled", False),
                "hash_verify_flamp_k2s_enabled": data.get("hash_verify_flamp_k2s_enabled", True),
                "gpg_executable_path": data.get("gpg_executable_path", ""),
                "gpg_trusted_signers": data.get("gpg_trusted_signers", []),
                "gpg_compose_signing_key_fingerprint": data.get("gpg_compose_signing_key_fingerprint", ""),
                "trusted_file_hashes": data.get("trusted_file_hashes", []),
                "varac_cluster_mode_enabled": data.get("varac_cluster_mode_enabled", False),
                "sop_export_preamble": data.get("sop_export_preamble", ""),
                "sop_export_postamble": data.get("sop_export_postamble", ""),
                "custom_tool_items": data.get("custom_tool_items", []),
                "launch_control_items": data.get("launch_control_items", []),
                "launch_control_enabled": data.get("launch_control_enabled", True),
                "launch_control_migrated_v1": data.get("launch_control_migrated_v1", True),
                "launch_readiness_timeout_sec": data.get(
                    "launch_readiness_timeout_sec",
                    DEFAULT_LAUNCH_READINESS_TIMEOUT_SEC,
                ),
                "operating_groups": data.get("operating_groups", []),
                "local_net_profiles": data.get("local_net_profiles", []),
            }
            for prog_name, meta in self.PROGRAMS.items():
                auto_key = meta["autostart_key"]
                if auto_key:
                    batch[auto_key] = data.get(auto_key, False)
            batch["autostart_js8call"] = data.get("autostart_js8call", False)
            self.settings.set_many(batch, save=True)  # type: ignore[attr-defined]
        elif hasattr(self.settings, "set"):
            self.settings.set("operator_callsign", data["operator_callsign"])
            self.settings.set("operator_name", data["operator_name"])
            self.settings.set("operator_state", data["operator_state"])
            self.settings.set("operator_grid6", data["operator_grid6"])
            self.settings.set("timezone", data["timezone"])
            self.settings.set("control_via", data["control_via"])
            self.settings.set("log_level", data.get("log_level", "DISABLED"))
            self.settings.set("ui_theme", data.get("ui_theme", "light"))
            self.settings.set("freq_enforcement_mode", data.get("freq_enforcement_mode", "On Schedule Change"))
            self.settings.set("freq_prompt_interval", data.get("freq_prompt_interval", "Hourly"))
            self.settings.set("fldigi_enforcement_mode", data.get("fldigi_enforcement_mode", "On Schedule Change"))
            self.settings.set("fldigi_prompt_interval", data.get("fldigi_prompt_interval", "Hourly"))
            self.settings.set("js8_enforcement_mode", data.get("js8_enforcement_mode", "On Schedule Change"))
            self.settings.set("js8_prompt_interval", data.get("js8_prompt_interval", "Hourly"))
            self.settings.set("primary_js8_groups", data["primary_js8_groups"])
            self.settings.set(MAPPER_SETTINGS_KEY, data.get(MAPPER_SETTINGS_KEY, []))
            self.settings.set(
                "js8_inbox_mark_retrieved_sync",
                data.get("js8_inbox_mark_retrieved_sync", False),
            )
            self.settings.set("gpg_verify_flamp_k2s_enabled", data.get("gpg_verify_flamp_k2s_enabled", False))
            self.settings.set("hash_verify_flamp_k2s_enabled", data.get("hash_verify_flamp_k2s_enabled", True))
            self.settings.set("gpg_executable_path", data.get("gpg_executable_path", ""))
            self.settings.set("gpg_trusted_signers", data.get("gpg_trusted_signers", []))
            self.settings.set(
                "gpg_compose_signing_key_fingerprint",
                data.get("gpg_compose_signing_key_fingerprint", ""),
            )
            self.settings.set("trusted_file_hashes", data.get("trusted_file_hashes", []))
            self.settings.set("varac_cluster_mode_enabled", data.get("varac_cluster_mode_enabled", False))
            self.settings.set("sop_export_preamble", data.get("sop_export_preamble", ""))
            self.settings.set("sop_export_postamble", data.get("sop_export_postamble", ""))
            self.settings.set("custom_tool_items", data.get("custom_tool_items", []))
            self.settings.set("launch_control_items", data.get("launch_control_items", []))
            self.settings.set("launch_control_enabled", data.get("launch_control_enabled", True))
            self.settings.set("launch_control_migrated_v1", data.get("launch_control_migrated_v1", True))
            self.settings.set(
                "launch_readiness_timeout_sec",
                data.get("launch_readiness_timeout_sec", DEFAULT_LAUNCH_READINESS_TIMEOUT_SEC),
            )
            for prog_name, meta in self.PROGRAMS.items():
                auto_key = meta["autostart_key"]
                if auto_key:
                    self.settings.set(auto_key, data.get(auto_key, False))
            self.settings.set("autostart_js8call", data.get("autostart_js8call", False))
            self.settings.set("operating_groups", data.get("operating_groups", []))
            self.settings.set("local_net_profiles", data.get("local_net_profiles", []))
        elif hasattr(self.settings, "_data"):
            # Fallback: update the internal dict only
            self.settings._data = data  # type: ignore[attr-defined]

        log.info("SettingsTab: settings saved.")
        self._refresh_runtime_projection_ui(refresh_multi_radio=True, emit_saved=False)
        self._ensure_fldigi_checkin_files()
        if show_message:
            radio_id, target = self._selected_settings_feedback_target()
            summary = f"Saved settings for {target}." if target and target != "Settings" else "Settings saved."
            self._publish_settings_action_feedback(
                status="succeeded",
                summary=summary,
                radio_profile_id=radio_id,
                target_label=target,
            )
            self._publish_save_guardrail_feedback()

        # Persist operator grid into operator_checkins for map usage
        self._persist_operator_grid_to_db(
            data.get("operator_callsign", ""),
            data.get("operator_grid6", ""),
            data.get("operator_name", ""),
            data.get("operator_state", ""),
        )
        if operator_changed:
            QTimer.singleShot(0, self._refresh_operator_history_views)
        self._settings_dirty = False
        self._set_save_button_state("success")
        emit_span(
            "settings.save_settings",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            meta={"operator_changed": operator_changed},
            min_ms=0.0,
        )

    def _on_theme_changed(self):
        theme = self.theme_combo.currentText().strip().lower() or "light"
        self._set_loading(True, "Wilco. Standby for Spectrum QSY...")
        QApplication.processEvents()
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("ui_theme", theme)
                if hasattr(self.settings, "save"):
                    self.settings.save()
        except Exception:
            pass
        try:
            self.settings_saved.emit()
        except Exception:
            pass
        self._mark_settings_dirty()
        # apply_theme will clear the toast once the app theme is applied

    def _on_text_size_changed(self):
        if self._loading_settings:
            return
        ui_text_size = normalize_ui_text_size(self.text_size_combo.currentText())
        self._set_loading(True, "Applying text size...")
        QApplication.processEvents()
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("ui_text_size", ui_text_size)
                if hasattr(self.settings, "save"):
                    self.settings.save()
        except Exception:
            pass
        try:
            self.settings_saved.emit()
        except Exception:
            pass
        self._mark_settings_dirty()
        # apply_theme will clear the toast once the app theme is applied

    def _request_open_logs(self) -> None:
        try:
            self.open_logs_requested.emit()
        except Exception:
            pass

    def _on_log_level_changed(self, level: str) -> None:
        if self._loading_settings:
            return
        level = (level or "DISABLED").strip().upper()
        if level not in {"DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"}:
            level = "DISABLED"
        prev_level = (self.settings.get("log_level", "") or "DISABLED").strip().upper()
        if not prev_level:
            try:
                prev_level = get_log_level().strip().upper()
            except Exception:
                prev_level = "DISABLED"
        if level == prev_level:
            return
        if level == "DEBUG":
            confirm = QMessageBox.question(
                self,
                "Enable DEBUG Logging",
                "DEBUG logging can impact performance and disk usage.\n\nEnable DEBUG now?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if confirm != QMessageBox.Yes:
                self.log_level_combo.blockSignals(True)
                idx = self.log_level_combo.findText(prev_level)
                self.log_level_combo.setCurrentIndex(idx if idx >= 0 else self.log_level_combo.findText("DISABLED"))
                self.log_level_combo.blockSignals(False)
                return
        try:
            set_log_level(level)
            self.settings.set_many(
                {
                    "log_level": level,
                    "timed_debug_until_utc": "",
                    "timed_debug_prev_level": "",
                }
            )
        except Exception:
            pass
        try:
            self.log_level_changed.emit(level)
        except Exception:
            pass
        self._refresh_section_titles()

    def _enable_timed_debug(self) -> None:
        minutes = int(self.debug_duration_combo.currentData() or 30)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        until = now_utc + datetime.timedelta(minutes=minutes)
        current = (self.settings.get("log_level", "") or "DISABLED").strip().upper()
        if current not in {"DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"}:
            current = "DISABLED"
        prev_level = current if current != "DEBUG" else "DISABLED"
        try:
            set_log_level("DEBUG")
            self.settings.set_many(
                {
                    "log_level": "DEBUG",
                    "timed_debug_until_utc": until.isoformat(),
                    "timed_debug_prev_level": prev_level,
                }
            )
            self.log_level_combo.blockSignals(True)
            idx = self.log_level_combo.findText("DEBUG")
            self.log_level_combo.setCurrentIndex(idx if idx >= 0 else 0)
            self.log_level_combo.blockSignals(False)
        except Exception:
            pass
        try:
            self.log_level_changed.emit("DEBUG")
        except Exception:
            pass
        self._refresh_section_titles()
        QMessageBox.information(
            self,
            "Timed DEBUG Enabled",
            f"DEBUG logging enabled for {minutes} minutes.\n"
            f"It will automatically revert at {until.astimezone():%Y-%m-%d %H:%M}.",
        )

    def _open_log_folder(self) -> None:
        log_file = _get_log_file()
        folder = str(Path(log_file).parent)
        try:
            if sys.platform.startswith("win"):
                os.startfile(folder)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", folder])
            else:
                subprocess.Popen(["xdg-open", folder])
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not open log folder:\n{e}")

    def _export_diagnostics(self) -> None:
        default_name = f"freqinout_diagnostics_{datetime.datetime.now():%Y%m%d_%H%M%S}.zip"
        out_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Diagnostics",
            default_name,
            "ZIP Files (*.zip)",
        )
        if not out_path:
            return
        if not out_path.lower().endswith(".zip"):
            out_path += ".zip"
        cfg_dir = get_config_dir() / "config"
        files = [
            Path(_get_log_file()),
            cfg_dir / "freqinout.db",
            cfg_dir / "freqinout_nets.db",
        ]
        added = 0
        try:
            with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for p in files:
                    if p.exists() and p.is_file():
                        zf.write(p, arcname=p.name)
                        added += 1
                info = (
                    f"FreqInOut version: {__version__}\n"
                    f"Exported: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n"
                    f"Platform: {platform.platform()}\n"
                    f"Log level: {(self.settings.get('log_level', 'DISABLED') or 'DISABLED')}\n"
                )
                zf.writestr("diagnostics_info.txt", info)
        except Exception as e:
            QMessageBox.critical(self, "Export Diagnostics", f"Failed to export diagnostics:\n{e}")
            return
        QMessageBox.information(
            self,
            "Export Diagnostics",
            f"Export complete.\nFiles included: {added} (+ diagnostics_info.txt).",
        )

    def _update_enforcement_visibility(self) -> None:
        freq_prompt = self.freq_enforce_combo.currentText().strip() == "Prompt"
        fldigi_prompt = self.fldigi_enforce_combo.currentText().strip() == "Prompt"
        js8_prompt = self.js8_enforce_combo.currentText().strip() == "Prompt"
        theme = resolve_theme(self.settings)
        muted_style = (
            "QComboBox {"
            f" color: {theme['text_muted']}; background-color: {theme['surface_alt']};"
            f" border: 1px solid {theme['border']};"
            "}"
        )
        warn_style = (
            "QComboBox {"
            f" color: {theme['warning']}; background-color: {theme['surface']};"
            f" border: 1px solid {theme['border']};"
            "}"
        )
        for combo, enabled in (
            (self.freq_prompt_combo, freq_prompt),
            (self.fldigi_prompt_combo, fldigi_prompt),
            (self.js8_prompt_combo, js8_prompt),
        ):
            combo.setEnabled(enabled)
            if not enabled:
                combo.setCurrentText("Select Interval")
                combo.setStyleSheet(muted_style)
                continue
            if combo.currentText().strip() == "Select Interval":
                combo.setStyleSheet(warn_style)
            else:
                combo.setStyleSheet("")

    def _update_logging_actions_layout(self) -> None:
        if not (
            hasattr(self, "logging_actions_grid")
            and hasattr(self, "open_logs_btn")
            and hasattr(self, "open_log_folder_btn")
            and hasattr(self, "export_diag_btn")
        ):
            return

        width = self.logging_group.width() if hasattr(self, "logging_group") else 0
        layout_mode = self._logging_action_layout_mode(width)

        for btn in (self.open_logs_btn, self.open_log_folder_btn, self.export_diag_btn):
            try:
                self.logging_actions_grid.removeWidget(btn)
            except Exception:
                pass

        for col in range(4):
            self.logging_actions_grid.setColumnStretch(col, 0)
        self.logging_actions_grid.setColumnStretch(3, 1)

        # Keep diagnostics actions grouped left; export wraps below on narrow panels instead of becoming a right rail.
        if layout_mode == "very_compact":
            self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0, 1, 2)
            self.logging_actions_grid.addWidget(self.open_log_folder_btn, 1, 0)
            self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 1)
            return

        self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0)
        self.logging_actions_grid.addWidget(self.open_log_folder_btn, 0, 1)
        try:
            if layout_mode == "compact":
                self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 0, 1, 2)
            else:
                self.logging_actions_grid.addWidget(self.export_diag_btn, 0, 2)
        except Exception:
            self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 0, 1, 2)

    @staticmethod
    def _logging_action_layout_mode(width: int) -> str:
        panel_width = max(0, int(width or 0))
        if panel_width < 480:
            return "very_compact"
        if panel_width < 640:
            return "compact"
        return "standard"

    def _apply_accessibility_width_guards(self) -> None:
        # Prevent clipped labels/buttons when UI text size increases (for example 125%).
        max_w = 420
        try:
            labels = self.findChildren(QLabel)
        except Exception:
            labels = []
        for lbl in labels:
            try:
                txt = str(lbl.text() or "").strip()
            except Exception:
                txt = ""
            if not txt:
                continue
            if lbl.wordWrap():
                continue
            try:
                min_w = int(lbl.minimumWidth())
                max_curr = int(lbl.maximumWidth())
            except Exception:
                continue
            if min_w <= 0 or min_w != max_curr:
                continue
            plain = re.sub(r"<[^>]+>", "", txt).strip() or txt
            try:
                needed = int(lbl.fontMetrics().horizontalAdvance(plain) + 14)
            except Exception:
                needed = min_w
            base = lbl.property("_fio_base_min_width")
            try:
                base_w = int(base)
            except Exception:
                base_w = min_w
                try:
                    lbl.setProperty("_fio_base_min_width", base_w)
                except Exception:
                    pass
            target = max(base_w, min(max_w, needed))
            if target > min_w:
                try:
                    lbl.setFixedWidth(target)
                except Exception:
                    pass
            elif target < min_w:
                try:
                    lbl.setFixedWidth(target)
                except Exception:
                    pass

        try:
            buttons = list(self.findChildren(QPushButton)) + list(self.findChildren(QToolButton))
        except Exception:
            buttons = []
        for btn in buttons:
            try:
                txt = str(btn.text() or "").strip()
            except Exception:
                txt = ""
            if not txt:
                continue
            try:
                min_w = int(btn.minimumWidth())
                max_curr = int(btn.maximumWidth())
            except Exception:
                min_w = 0
                max_curr = 0
            try:
                needed = int(btn.fontMetrics().horizontalAdvance(txt.replace("&", "")) + 30)
            except Exception:
                needed = min_w
            base = btn.property("_fio_base_min_width")
            try:
                base_w = int(base)
            except Exception:
                base_w = min_w
                try:
                    btn.setProperty("_fio_base_min_width", base_w)
                except Exception:
                    pass
            target = max(base_w, min(max_w, needed))
            try:
                if min_w > 0 and min_w == max_curr and max_curr < 16777215:
                    btn.setMaximumWidth(16777215)
                if target > 0:
                    btn.setMinimumWidth(target)
            except Exception:
                pass

        try:
            combos = self.findChildren(QComboBox)
        except Exception:
            combos = []
        for combo in combos:
            try:
                min_w = int(combo.minimumWidth())
                max_curr = int(combo.maximumWidth())
            except Exception:
                min_w = 0
                max_curr = 16777215
            try:
                item_w = max((int(combo.fontMetrics().horizontalAdvance(combo.itemText(i))) for i in range(combo.count())), default=0)
            except Exception:
                item_w = 0
            base = combo.property("_fio_base_min_width")
            try:
                base_w = int(base)
            except Exception:
                base_w = max(min_w, int(combo.sizeHint().width()))
                try:
                    combo.setProperty("_fio_base_min_width", base_w)
                except Exception:
                    pass
            target = max(base_w, min(max_w, item_w + 44))
            try:
                if max_curr < 16777215 and target > max_curr:
                    combo.setMaximumWidth(16777215)
                combo.setMinimumWidth(target)
                combo.view().setMinimumWidth(target)
            except Exception:
                pass

    def _on_enforcement_changed(self):
        self._update_enforcement_visibility()
        self._mark_settings_dirty()

    def _set_loading(self, active: bool, text: str = "Wilco. Standby for Spectrum QSY...") -> None:
        if not self.loading_label:
            return
        self.loading_label.setText(text)
        self.loading_label.setVisible(bool(active))

    def _set_js8_load_busy(self, active: bool, text: str = "Loading JS8 traffic...") -> None:
        was_active = bool(getattr(self, "_js8_load_busy_active", False))
        self._js8_load_busy_active = bool(active)
        if hasattr(self, "load_js8_btn") and self.load_js8_btn:
            self.load_js8_btn.setEnabled(not active)
            self.load_js8_btn.setText("Loading..." if active else "Load JS8 Traffic")
        if hasattr(self, "load_js8_progress") and self.load_js8_progress:
            self.load_js8_progress.setVisible(bool(active))
        if hasattr(self, "load_js8_status_label") and self.load_js8_status_label:
            self.load_js8_status_label.setText(text)
            self.load_js8_status_label.setVisible(bool(active))
        if active and not was_active:
            try:
                QApplication.setOverrideCursor(Qt.WaitCursor)
            except Exception:
                pass
        if active:
            QApplication.processEvents()
        elif was_active:
            try:
                QApplication.restoreOverrideCursor()
            except Exception:
                pass

    def _make_spotter_mapper_check_item(self, checked: bool) -> QTableWidgetItem:
        item = QTableWidgetItem("")
        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
        item.setCheckState(Qt.Checked if checked else Qt.Unchecked)
        item.setTextAlignment(Qt.AlignCenter)
        return item

    def _refresh_spotter_form_mapper(self) -> None:
        if not hasattr(self, "spotter_mapper_table"):
            return
        previous_signal_state = self.spotter_mapper_table.blockSignals(True)
        self._spotter_mapper_loading = True
        try:
            self.spotter_mapper_table.setRowCount(0)
            rows = effective_mapping_rows(self.settings, self.js8_forms_edit.text().strip())
            if not rows:
                rows = [
                    factory_mapping_for_form("F!103", "Net Checkin"),
                    factory_mapping_for_form("F!104", "@SITREP Basic Check-in"),
                    factory_mapping_for_form("F!106", "Impromptu Net Notice"),
                    factory_mapping_for_form("F!301", "Field Situation Report"),
                    factory_mapping_for_form("F!304", "Individual Situation Report"),
                ]
            for row_idx, row in enumerate(rows):
                self.spotter_mapper_table.insertRow(row_idx)
                code_item = QTableWidgetItem(str(row.get("form_code") or "").strip())
                code_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                title_item = QTableWidgetItem(str(row.get("title") or "").strip())
                title_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.spotter_mapper_table.setItem(row_idx, 0, code_item)
                self.spotter_mapper_table.setItem(row_idx, 1, title_item)

                purpose_combo = QComboBox()
                purpose_combo.addItems(list(PURPOSE_OPTIONS))
                fit_combo_box_to_contents(purpose_combo)
                purpose = str(row.get("purpose") or "Generic Message")
                if purpose_combo.findText(purpose) < 0:
                    purpose = "Generic Message"
                purpose_combo.setCurrentText(purpose)
                purpose_combo.currentIndexChanged.connect(self._on_spotter_mapper_changed)
                self.spotter_mapper_table.setCellWidget(row_idx, 2, purpose_combo)

                for col, key in enumerate(("messages", "map", "alert", "net", "status"), start=3):
                    self.spotter_mapper_table.setItem(
                        row_idx,
                        col,
                        self._make_spotter_mapper_check_item(bool(row.get(key, False))),
                    )
        finally:
            self._spotter_mapper_loading = False
            self.spotter_mapper_table.blockSignals(previous_signal_state)
        self._fit_table_height_to_rows(self.spotter_mapper_table, min_rows=3, max_rows=6, extra_rows=0)
        self._refresh_fit_content_section_height(getattr(self, "js8_section_group", None))

    def _on_spotter_mapper_changed(self, *_args) -> None:
        if self._spotter_mapper_loading:
            return
        self._mark_settings_dirty()

    def _on_spotter_mapper_item_changed(self, _item: QTableWidgetItem) -> None:
        self._on_spotter_mapper_changed()

    def _collect_spotter_form_mappings(self) -> List[Dict[str, object]]:
        if not hasattr(self, "spotter_mapper_table"):
            return []
        rows: List[Dict[str, object]] = []
        for row_idx in range(self.spotter_mapper_table.rowCount()):
            code_item = self.spotter_mapper_table.item(row_idx, 0)
            title_item = self.spotter_mapper_table.item(row_idx, 1)
            purpose_widget = self.spotter_mapper_table.cellWidget(row_idx, 2)
            purpose = purpose_widget.currentText().strip() if isinstance(purpose_widget, QComboBox) else "Generic Message"
            row = {
                "form_code": code_item.text().strip() if code_item else "",
                "title": title_item.text().strip() if title_item else "",
                "purpose": purpose,
            }
            for col, key in enumerate(("messages", "map", "alert", "net", "status"), start=3):
                item = self.spotter_mapper_table.item(row_idx, col)
                row[key] = bool(item and item.checkState() == Qt.Checked)
            rows.append(row)
        return normalize_mapping_rows(rows)

    def _auto_classify_spotter_forms(self) -> None:
        rows = [
            factory_mapping_for_form(definition.form_code, definition.title)
            for definition in discover_spotter_forms(self.js8_forms_edit.text().strip())
        ]
        if not rows:
            rows = [
                factory_mapping_for_form("F!103", "Net Checkin"),
                factory_mapping_for_form("F!104", "@SITREP Basic Check-in"),
                factory_mapping_for_form("F!106", "Impromptu Net Notice"),
                factory_mapping_for_form("F!301", "Field Situation Report"),
                factory_mapping_for_form("F!304", "Individual Situation Report"),
            ]
        try:
            self.settings.set(MAPPER_SETTINGS_KEY, normalize_mapping_rows(rows))
        except Exception:
            pass
        self._refresh_spotter_form_mapper()
        self._mark_settings_dirty()

    def _wire_dirty_tracking(self) -> None:
        edits = [
            self.callsign_edit,
            self.name_edit,
            self.state_edit,
            self.grid6_edit,
            self.js8_host_edit,
            self.js8_port_edit,
            self.js8_offset_edit,
            self.js8_directed_edit,
            self.js8_forms_edit,
            self.js8call_path_edit,
            self.js8spotter_path_edit,
            self.commstat_path_edit,
            self.fldigi_host_edit,
            self.fldigi_port_edit,
            self.fldigi_checkin_dir_edit,
            self.fldigi_log_path_edit,
            self.varac_bbs_dir_edit,
            self.varac_outbox_dir_edit,
            self.varac_bbs_archive_dir_edit,
            self.varac_path_edit,
            self.varac_launch_cmd_edit,
            self.flrig_port_edit,
        ]
        edits.extend(self.msg_paths_edits.values())
        edits.extend(self.path_edits.values())
        for edit in edits:
            edit.textChanged.connect(self._mark_settings_dirty)

        combos = [
            self.control_combo,
            self.theme_combo,
            self.text_size_combo,
            self.freq_enforce_combo,
            self.freq_prompt_combo,
            self.fldigi_enforce_combo,
            self.fldigi_prompt_combo,
            self.js8_enforce_combo,
            self.js8_prompt_combo,
            self.varac_bbs_archive_days_combo,
        ]
        for combo in combos:
            combo.currentIndexChanged.connect(self._mark_settings_dirty)

        checks = [
            self.use_scheduler_chk,
            self.js8_mark_retrieved_chk,
            self.varac_bbs_auto_archive_chk,
            self.launch_all_with_startup_chk,
        ]
        checks.extend(self.radio_checkboxes.values())
        for chk in checks:
            chk.stateChanged.connect(self._mark_settings_dirty)

    def _mark_settings_dirty(self) -> None:
        if self._loading_settings:
            return
        if not self._settings_dirty:
            self._settings_dirty = True
            self._set_save_button_state("info")
            self._set_settings_action_feedback_status("in_progress", "Unsaved settings changes.")
        self._refresh_section_nav_health()

    def _on_sop_export_text_changed(self) -> None:
        self._mark_settings_dirty()
        if self._loading_settings:
            return
        self._refresh_section_titles()

    def _set_save_button_state(self, role: str) -> None:
        theme = resolve_theme(self.settings)
        self.save_btn.setStyleSheet(button_style(role, theme))

    @staticmethod
    def _human_join(values: List[str]) -> str:
        cleaned = [str(value or "").strip() for value in values if str(value or "").strip()]
        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        if len(cleaned) == 2:
            return f"{cleaned[0]} and {cleaned[1]}"
        return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"

    @staticmethod
    def _settings_feedback_label_style(status: str, theme: Dict[str, str]) -> str:
        normalized = str(status or "").strip().lower()
        if normalized == "succeeded":
            color = theme.get("success", "#2E7D32")
        elif normalized in {"failed", "blocked"}:
            color = theme.get("danger", "#B3261E")
        elif normalized == "partial":
            color = theme.get("warning", "#C99700")
        elif normalized in {"requested", "in_progress"}:
            color = theme.get("accent", "#2a6fd3")
        else:
            color = theme.get("muted", "#666666")
        return f"color: {color}; font-weight: 600;"

    def _selected_settings_feedback_target(self) -> Tuple[Optional[str], str]:
        profile = self._selected_settings_radio_profile()
        if isinstance(profile, dict):
            radio_id = int(profile.get("id", 0) or 0)
            radio_id_txt = str(radio_id) if radio_id > 0 else None
            return radio_id_txt, self._profile_display_name(profile)
        return None, "Settings"

    def _set_settings_action_feedback_status(self, status: str, text: str, detail: str = "") -> None:
        if not hasattr(self, "settings_action_feedback_label"):
            return
        display = str(text or "").strip() or "Settings ready."
        detail_txt = str(detail or "").strip()
        self.settings_action_feedback_label.setText(display)
        self.settings_action_feedback_label.setToolTip(detail_txt or display)
        self.settings_action_feedback_label.setStyleSheet(
            self._settings_feedback_label_style(status, resolve_theme(self.settings))
        )

    def _publish_settings_action_feedback(
        self,
        *,
        status: str,
        summary: str,
        detail: str = "",
        action_type: str = "save",
        radio_profile_id: Optional[str] = None,
        target_label: str = "",
        source_surface: str = "settings",
    ) -> None:
        radio_id = radio_profile_id
        target = str(target_label or "").strip()
        if not radio_id and not target:
            radio_id, target = self._selected_settings_feedback_target()
        try:
            event = self.action_feedback_service.publish(
                scope="settings",
                action_type=action_type,
                status=status,
                summary=summary,
                radio_profile_id=radio_id,
                target_label=target,
                detail=detail,
                source_surface=source_surface,
            )
            self._last_action_feedback_event = event
            self._set_settings_action_feedback_status(event.status, event.summary, event.detail)
        except Exception:
            log.exception("SettingsTab: failed to publish settings action feedback.")
            self._set_settings_action_feedback_status(status, summary, detail)

    def _block_settings_action(self, summary: str, detail: str = "", *, action_type: str = "save") -> None:
        self._publish_settings_action_feedback(
            status="blocked",
            summary=summary,
            detail=detail,
            action_type=action_type,
        )

    def _publish_launch_control_feedback(self, *, status: str, summary: str, detail: str = "") -> None:
        self._publish_settings_action_feedback(
            status=status,
            summary=summary,
            detail=detail,
            action_type="launch_control",
        )

    @staticmethod
    def _autofill_section_label(section: str) -> str:
        normalized = str(section or "").strip().lower()
        if normalized == "fast_light":
            return "Fast Light"
        if normalized == "js8":
            return "JS8Call"
        if normalized == "varac":
            return "VarAC"
        return "Settings"

    @staticmethod
    def _autofill_feedback_status(*, filled_count: int, preserved_count: int, missing_count: int) -> str:
        if missing_count > 0:
            return "partial" if filled_count or preserved_count else "blocked"
        if filled_count or preserved_count:
            return "succeeded"
        return "blocked"

    @staticmethod
    def _autofill_feedback_source_surface(section: str, operation: str) -> str:
        normalized_section = str(section or "").strip().lower().replace("-", "_").replace(" ", "_")
        normalized_operation = str(operation or "").strip().lower().replace("-", "_").replace(" ", "_")
        if normalized_section not in {"fast_light", "js8", "varac"}:
            normalized_section = "general"
        if not normalized_operation:
            normalized_operation = "event"
        return f"settings.configure_automatically.{normalized_section}.{normalized_operation}"

    def _publish_autofill_feedback(
        self,
        *,
        status: str,
        summary: str,
        detail: str = "",
        section: str = "",
        operation: str = "event",
    ) -> None:
        self._publish_settings_action_feedback(
            status=status,
            summary=summary,
            detail=detail,
            action_type="configure_automatically",
            source_surface=self._autofill_feedback_source_surface(section, operation),
        )

    @staticmethod
    def _autofill_health_key(section: str) -> str:
        normalized = str(section or "").strip().lower()
        if normalized == "js8":
            return "js8call"
        if normalized in {"fast_light", "varac"}:
            return normalized
        return normalized

    @staticmethod
    def _autofill_readiness_summary(section_label: str, detail: str) -> str:
        first_issue = str(detail or "").split(";", 1)[0].strip()
        if first_issue:
            return f"Auto-fill needs review in {section_label}: {first_issue}."
        return f"Auto-fill needs review in {section_label}."

    def _publish_autofill_readiness_feedback(self, section: str) -> None:
        try:
            health_key = self._autofill_health_key(section)
            entry = self._build_section_health_snapshot().get(health_key, {})
            detail = str(entry.get("detail", "") or "").strip()
            if str(entry.get("state", "") or "").strip().lower() != "warn" or not detail:
                return
            section_label = self._autofill_section_label(section)
            self._publish_autofill_feedback(
                status="partial",
                summary=self._autofill_readiness_summary(section_label, detail),
                detail=detail,
                section=section,
                operation="readiness",
            )
        except Exception:
            log.exception("SettingsTab: failed to publish auto-fill readiness feedback.")

    @staticmethod
    def _save_guardrail_feedback_summary(warning_count: int) -> str:
        count = max(0, int(warning_count or 0))
        if count == 1:
            return "Settings saved, but 1 multi-rig guardrail warning needs review."
        return f"Settings saved, but {count} multi-rig guardrail warnings need review."

    def _current_multi_rig_guardrail_messages(self) -> Tuple[str, ...]:
        try:
            self._last_multi_rig_guardrail_collection_error = ""
            db_path = Path(getattr(self.multi_radio_store, "db_path", ""))
            if not db_path:
                return ()
            with sqlite3.connect(db_path) as conn:
                return tuple(multi_rig_guardrail_warnings(conn))
        except Exception as exc:
            self._last_multi_rig_guardrail_collection_error = str(exc) or exc.__class__.__name__
            log.exception("SettingsTab: failed to collect multi-rig guardrail warnings.")
            return ()

    def _current_multi_rig_guardrail_details(self) -> Tuple[MultiRigGuardrailWarning, ...]:
        try:
            self._last_multi_rig_guardrail_collection_error = ""
            db_path = Path(getattr(self.multi_radio_store, "db_path", ""))
            if not db_path:
                return ()
            with sqlite3.connect(db_path) as conn:
                return tuple(collect_multi_rig_guardrail_warnings(conn))
        except Exception as exc:
            self._last_multi_rig_guardrail_collection_error = str(exc) or exc.__class__.__name__
            log.exception("SettingsTab: failed to collect structured multi-rig guardrail warnings.")
            return ()

    @staticmethod
    def _save_guardrail_failure_summary() -> str:
        return "Settings saved, but multi-rig guardrail checking failed."

    def _publish_save_guardrail_feedback(self) -> None:
        warnings = self._current_multi_rig_guardrail_messages()
        collection_error = str(getattr(self, "_last_multi_rig_guardrail_collection_error", "") or "").strip()
        if collection_error:
            self._publish_settings_action_feedback(
                status="failed",
                summary=self._save_guardrail_failure_summary(),
                detail=collection_error,
                action_type="save_guardrails",
            )
            return
        if not warnings:
            return
        self._publish_settings_action_feedback(
            status="partial",
            summary=self._save_guardrail_feedback_summary(len(warnings)),
            detail="\n".join(warnings),
            action_type="save_guardrails",
        )

    @staticmethod
    def _guardrail_readiness_status_text(warnings: Sequence[str]) -> str:
        warning_lines = [str(item or "").strip() for item in warnings if str(item or "").strip()]
        if not warning_lines:
            return ""
        count = len(warning_lines)
        noun = "warning" if count == 1 else "warnings"
        verb = "needs" if count == 1 else "need"
        detail = "\n".join(f"- {item}" for item in warning_lines[:3])
        if count > 3:
            detail = f"{detail}\n- {count - 3} more warning(s)"
        return f"Multi-rig guardrails: {count} persisted {noun} {verb} review.\n{detail}"

    def _set_device_profile_guardrail_status(self, warnings: Sequence[str]) -> bool:
        warning_lines = tuple(str(item or "").strip() for item in warnings if str(item or "").strip())
        text = self._guardrail_readiness_status_text(warning_lines)
        self._last_device_profile_guardrail_warnings = warning_lines
        label = getattr(self, "device_profile_guardrail_status_label", None)
        if label is not None:
            label.setText(text)
            label.setToolTip("\n".join(warning_lines))
            label.setVisible(bool(text))
        button = getattr(self, "copy_guardrail_summary_btn", None)
        if button is not None:
            button.setVisible(bool(text))
            button.setEnabled(bool(text))
            button.setToolTip(
                "Copy the current multi-rig guardrail warnings for review."
                if text
                else "No multi-rig guardrail warnings to copy."
            )
        review_button = getattr(self, "review_guardrail_conflicts_btn", None)
        if review_button is not None:
            review_button.setVisible(bool(text))
            review_button.setEnabled(bool(text))
            review_button.setToolTip(
                "Review affected radios and jump to the relevant Settings section."
                if text
                else "No multi-rig guardrail conflicts to review."
            )
        return bool(text)

    @staticmethod
    def _guardrail_warning_target_attr(warning_type: str) -> str:
        normalized = str(warning_type or "").strip().lower()
        if "js8" in normalized:
            return "js8_section_group"
        if any(key in normalized for key in ("flrig", "fldigi", "flamp", "flmsg", "rigctld")):
            return "fast_light_section_group"
        if "varac" in normalized:
            return "varac_section_group"
        return "radio_profile_section_group"

    @classmethod
    def _guardrail_review_rows(
        cls,
        warnings: Sequence[MultiRigGuardrailWarning],
    ) -> Tuple[Dict[str, Any], ...]:
        rows: List[Dict[str, Any]] = []
        for warning in warnings:
            rows.append(
                {
                    "message": str(getattr(warning, "message", "") or "").strip(),
                    "warning_type": str(getattr(warning, "warning_type", "") or "").strip(),
                    "resource_type": str(getattr(warning, "resource_type", "") or "").strip(),
                    "resource_value": str(getattr(warning, "resource_value", "") or "").strip(),
                    "affected_radio_ids": tuple(int(item or 0) for item in getattr(warning, "affected_radio_ids", ()) or ()),
                    "affected_radio_names": tuple(str(item or "").strip() for item in getattr(warning, "affected_radio_names", ()) or ()),
                    "target_attr": cls._guardrail_warning_target_attr(str(getattr(warning, "warning_type", "") or "")),
                }
            )
        return tuple(rows)

    def _focus_guardrail_conflict(self, radio_id: int, target_attr: str = "") -> bool:
        focused = self.focus_radio_profile(int(radio_id or 0))
        target_group = getattr(self, str(target_attr or "").strip(), None)
        if isinstance(target_group, QGroupBox):
            try:
                target_group.setChecked(True)
            except Exception:
                pass
            self._select_settings_section_group(target_group)
        return focused or isinstance(target_group, QGroupBox)

    def _review_device_profile_guardrail_conflicts(self) -> None:
        warnings = self._current_multi_rig_guardrail_details()
        if not warnings:
            messages = self._current_multi_rig_guardrail_messages()
            self._set_device_profile_guardrail_status(messages)
            return

        rows = self._guardrail_review_rows(warnings)
        dlg = QDialog(self)
        dlg.setWindowTitle("Review Multi-Rig Conflicts")
        dlg.setAccessibleName("Review Multi-Rig Conflicts")
        dlg.resize(760, 520)
        layout = QVBoxLayout(dlg)
        intro = QLabel("Review duplicated endpoints and paths, then focus an affected radio to adjust its Settings fields.")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        scroll = QScrollArea(dlg)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        layout.addWidget(scroll, 1)
        body = QWidget()
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(0, 0, 0, 0)
        body_layout.setSpacing(8)
        scroll.setWidget(body)

        for row in rows:
            panel = QFrame()
            panel.setFrameShape(QFrame.StyledPanel)
            panel_layout = QVBoxLayout(panel)
            panel_layout.setContentsMargins(10, 8, 10, 8)
            panel_layout.setSpacing(6)
            title = QLabel(str(row.get("message", "")) or "Review this multi-rig guardrail warning.")
            title.setWordWrap(True)
            title.setAccessibleName(f"Guardrail conflict: {title.text()}")
            panel_layout.addWidget(title)
            detail = QLabel(
                f"{row.get('resource_type', 'Resource')}: {row.get('resource_value', '--')}\n"
                f"Affected radios: {', '.join(row.get('affected_radio_names', ()) or ('--',))}"
            )
            detail.setWordWrap(True)
            panel_layout.addWidget(detail)
            action_row = QHBoxLayout()
            action_row.setContentsMargins(0, 0, 0, 0)
            for radio_id, radio_name in zip(row.get("affected_radio_ids", ()), row.get("affected_radio_names", ())):
                btn = QPushButton(f"Focus {radio_name or radio_id}")
                btn.setAccessibleName(f"Focus guardrail radio {radio_name or radio_id}")
                btn.setToolTip("Focus this radio and open the Settings section most likely to contain the conflicting field.")
                btn.clicked.connect(
                    lambda _checked=False, ident=int(radio_id), target=str(row.get("target_attr", "")): self._focus_guardrail_conflict(ident, target)
                )
                action_row.addWidget(btn)
            action_row.addStretch(1)
            panel_layout.addLayout(action_row)
            body_layout.addWidget(panel)
        body_layout.addStretch(1)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dlg.close)
        layout.addWidget(buttons)
        dlg.show()
        self._guardrail_review_dialog = dlg
        radio_id, target = self._selected_settings_feedback_target()
        self._publish_settings_action_feedback(
            status="succeeded",
            summary=f"Opened {len(rows)} multi-rig guardrail conflict review item(s).",
            detail="\n".join(str(row.get("message", "")) for row in rows),
            action_type="review_guardrails",
            radio_profile_id=radio_id,
            target_label=target,
        )

    @staticmethod
    def _guardrail_copy_summary(warning_count: int) -> str:
        count = max(0, int(warning_count or 0))
        noun = "warning" if count == 1 else "warnings"
        return f"Copied {count} multi-rig guardrail {noun}."

    @staticmethod
    def _guardrail_copy_text(
        warnings: Sequence[str],
        *,
        radio_profile_id: Optional[str] = None,
        target_label: str = "",
        timestamp_utc: str = "",
    ) -> str:
        warning_lines = [str(item or "").strip() for item in warnings if str(item or "").strip()]
        if not warning_lines:
            return ""
        copied_at = str(timestamp_utc or "").strip()
        if not copied_at:
            copied_at = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace(
                "+00:00",
                "Z",
            )
        target = str(target_label or "").strip() or "Settings"
        radio_id = str(radio_profile_id or "").strip()
        radio_context = f"{target} (radio id {radio_id})" if radio_id else target
        return "\n".join(
            [
                "FIO multi-rig guardrail warnings",
                f"Copied UTC: {copied_at}",
                f"Radio context: {radio_context}",
                "Warnings:",
                *(f"- {item}" for item in warning_lines),
            ]
        )

    def _copy_device_profile_guardrail_warnings(self) -> None:
        warnings = tuple(getattr(self, "_last_device_profile_guardrail_warnings", ()) or ())
        if not warnings:
            warnings = self._current_multi_rig_guardrail_messages()
            self._set_device_profile_guardrail_status(warnings)
        if not warnings:
            return
        radio_id, target = self._selected_settings_feedback_target()
        text = self._guardrail_copy_text(warnings, radio_profile_id=radio_id, target_label=target)
        QApplication.clipboard().setText(text)
        self._publish_settings_action_feedback(
            status="succeeded",
            summary=self._guardrail_copy_summary(len(warnings)),
            detail=text,
            action_type="copy_guardrails",
            radio_profile_id=radio_id,
            target_label=target,
        )

    @staticmethod
    def _launch_sequence_feedback_status(
        *,
        launched: int,
        already_running: int,
        failed: int,
        timeout: int,
        blocked_self: int,
        cancelled: bool,
    ) -> str:
        if cancelled:
            return "partial" if launched or already_running else "blocked"
        if failed or timeout or blocked_self:
            return "partial" if launched or already_running else "failed"
        if launched or already_running:
            return "succeeded"
        return "blocked"

    @staticmethod
    def _launch_sequence_feedback_summary(
        *,
        launched: int,
        already_running: int,
        failed: int,
        timeout: int,
        blocked_self: int,
        cancelled: bool,
    ) -> str:
        if cancelled:
            prefix = "Launch cancelled"
        elif failed or timeout or blocked_self:
            prefix = "Launch completed with issues"
        elif launched or already_running:
            prefix = "Launch complete"
        else:
            prefix = "Launch complete: no applications started"
        return (
            f"{prefix}: launched {launched}, already running {already_running}, "
            f"failed {failed}, timeout {timeout}, blocked {blocked_self}."
        )

    @staticmethod
    def _launch_sequence_feedback_detail(
        *,
        launched: int,
        already_running: int,
        failed: int,
        timeout: int,
        blocked_self: int,
        cancelled: bool,
    ) -> str:
        return (
            f"Launched: {launched}\n"
            f"Already running: {already_running}\n"
            f"Failed: {failed}\n"
            f"Timeout: {timeout}\n"
            f"Blocked (self-target): {blocked_self}\n"
            f"Cancelled: {'Yes' if cancelled else 'No'}"
        )

    # ---------- Radio Profiles ---------- #

    def _summary_device_profiles(self) -> str:
        count = len(self.device_profiles)
        active_count = len(
            [
                row
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_active", 0) or 0) == 1
            ]
        )
        primary = next(
            (
                str(row.get("name", "") or "").strip()
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            "",
        )
        observer_count = len(
            [
                row
                for row in self.device_profiles
                if isinstance(row, dict) and str(row.get("device_class", "") or "").strip().lower() == "observer"
            ]
        )
        if count <= 0:
            return "No radio profiles"
        if primary:
            return (
                f"{count} radio{'s' if count != 1 else ''}, "
                f"{active_count} active, {observer_count} observer{'s' if observer_count != 1 else ''}, default {primary}"
            )
        return f"{count} radio{'s' if count != 1 else ''}"

    def _effective_assignment_map(self) -> Dict[int, Dict[str, Any]]:
        mapping: Dict[int, Dict[str, Any]] = {}
        try:
            rows = self.multi_radio_store.list_effective_assignments()
        except Exception:
            log.exception("Failed loading effective assignment map from store.")
            return mapping
        for row in rows:
            if not isinstance(row, dict):
                continue
            device_id = int(row.get("device_profile_id", 0) or 0)
            if device_id > 0:
                mapping[device_id] = dict(row)
        return mapping

    def _selected_device_profile_ids(self) -> List[int]:
        if not hasattr(self, "device_profiles_table"):
            focused = int(self._settings_radio_focus_id or 0)
            return [focused] if focused > 0 else []
        selected: List[int] = []
        for row in range(self.device_profiles_table.rowCount()):
            wrapper = self.device_profiles_table.cellWidget(row, 0)
            chk = wrapper.findChild(QCheckBox) if wrapper is not None else None
            if chk is None or not chk.isChecked():
                continue
            try:
                selected.append(int(chk.property("device_profile_id") or 0))
            except Exception:
                continue
        if not selected:
            focused = int(self._settings_radio_focus_id or 0)
            if focused > 0:
                selected.append(focused)
        return selected

    def _selected_device_profiles(self) -> List[Dict[str, Any]]:
        selected_ids = set(self._selected_device_profile_ids())
        return [
            dict(row)
            for row in self.device_profiles
            if isinstance(row, dict) and int(row.get("id", 0) or 0) in selected_ids
        ]

    def _selected_device_profiles_as_assignment_rows(self) -> List[Dict[str, Any]]:
        selected_ids = {int(row.get("id", 0) or 0) for row in self._selected_device_profiles()}
        if not selected_ids:
            return []
        assignment_map = {
            int(row.get("device_profile_id", 0) or 0): dict(row)
            for row in self.device_assignments
            if isinstance(row, dict)
        }
        rows: List[Dict[str, Any]] = []
        for profile in self._selected_device_profiles():
            device_id = int(profile.get("id", 0) or 0)
            row = dict(assignment_map.get(device_id, {}))
            row.setdefault("device_profile_id", device_id)
            row.setdefault("device_name", self._profile_display_name(profile))
            row.setdefault("runtime_active", int(profile.get("runtime_active", 0) or 0))
            row.setdefault("runtime_primary", int(profile.get("runtime_primary", 0) or 0))
            row.setdefault("device_class", str(profile.get("device_class", "") or ""))
            row.setdefault("endpoint_summary", self._device_endpoint_summary(profile))
            rows.append(row)
        return rows

    def _device_profile_by_id(self, device_profile_id: int) -> Optional[Dict[str, Any]]:
        target = int(device_profile_id or 0)
        if target <= 0:
            return None
        for row in self.device_profiles:
            if isinstance(row, dict) and int(row.get("id", 0) or 0) == target:
                return dict(row)
        return None

    def _ensure_settings_radio_focus_id(self) -> Optional[int]:
        current_id = int(self._settings_radio_focus_id or 0)
        if current_id > 0 and self._device_profile_by_id(current_id):
            return current_id
        primary_id = next(
            (
                int(row.get("id", 0) or 0)
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            0,
        )
        if primary_id > 0:
            self._settings_radio_focus_id = primary_id
            return primary_id
        first_id = next(
            (
                int(row.get("id", 0) or 0)
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("id", 0) or 0) > 0
            ),
            0,
        )
        self._settings_radio_focus_id = first_id or None
        return self._settings_radio_focus_id

    def _selected_settings_radio_profile(self) -> Optional[Dict[str, Any]]:
        focused_id = self._ensure_settings_radio_focus_id()
        if not focused_id:
            return None
        return self._device_profile_by_id(int(focused_id))

    def _on_device_profile_table_focus_changed(self) -> None:
        if self._device_profiles_table_loading:
            return
        table = getattr(self, "device_profiles_table", None)
        if table is None:
            return
        current_row = table.currentRow()
        if current_row < 0:
            return
        item = table.item(current_row, 3)
        if item is None:
            return
        try:
            radio_id = int(item.data(Qt.UserRole) or 0)
        except Exception:
            radio_id = 0
        if radio_id <= 0:
            return
        self._set_settings_radio_focus(radio_id, sync_table=False)

    def _set_settings_radio_focus(self, radio_id: int, *, sync_table: bool = True) -> None:
        radio_id = int(radio_id or 0)
        if radio_id <= 0 or not self._device_profile_by_id(radio_id):
            return
        self._settings_radio_focus_id = radio_id
        if sync_table:
            self._sync_device_profiles_table_to_settings_focus()
        self._rebuild_device_profile_selector()
        self._update_device_profile_action_buttons()
        self._update_device_profile_readiness_detail()
        self._sync_software_radio_to_device_focus()
        self._sync_schedule_views_to_device_focus()
        self._rebuild_status_indicators()
        self._refresh_radio_specific_section_visibility()
        self._refresh_radio_settings_nav_label()
        self._refresh_radio_context_labels()

    def _refresh_radio_settings_nav_label(self) -> None:
        if not hasattr(self, "radio_settings_toggle_btn"):
            return
        profile = self._selected_settings_radio_profile()
        if isinstance(profile, dict):
            text = f"{self._profile_display_name(profile)} Settings"
        else:
            text = "Selected Radio"
        self.radio_settings_toggle_btn.setText(text)
        self._refresh_settings_section_combo()

    @staticmethod
    def _fit_table_height_to_rows(table: QTableWidget, *, min_rows: int = 1, max_rows: int = 8, extra_rows: int = 1) -> None:
        row_count = max(int(table.rowCount()), int(min_rows))
        visible_rows = min(row_count + max(int(extra_rows), 0), int(max_rows))
        default_row_height = max(table.verticalHeader().defaultSectionSize(), 24)
        row_height = default_row_height
        if table.rowCount() > 0:
            try:
                row_height = max(table.rowHeight(0), default_row_height)
            except Exception:
                row_height = default_row_height
        frame = table.frameWidth() * 2
        header_height = table.horizontalHeader().height() if table.horizontalHeader() is not None else 0
        horizontal_scroll_height = table.horizontalScrollBar().sizeHint().height() if table.horizontalScrollBar() is not None else 0
        height = header_height + (visible_rows * row_height) + horizontal_scroll_height + frame + 8
        table.setMinimumHeight(height)
        table.setMaximumHeight(height)

    @staticmethod
    def _fit_combo_to_contents(combo: QComboBox, *, minimum: int = 180, maximum: int = 520) -> None:
        combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        fm = combo.fontMetrics()
        widest = max(int(minimum), 0)
        for idx in range(combo.count()):
            widest = max(widest, fm.horizontalAdvance(combo.itemText(idx)) + 56)
        width = min(max(widest, int(minimum)), int(maximum))
        combo.setMinimumWidth(width)
        try:
            combo.view().setMinimumWidth(min(max(width + 24, int(minimum)), int(maximum) + 80))
        except Exception:
            pass

    @staticmethod
    def _make_compact_settings_panel(
        *,
        object_name: str,
        accessible_name: str,
        tooltip: str = "",
        maximum_width: int = 760,
    ) -> Tuple[QWidget, QGridLayout]:
        panel = QWidget()
        panel.setObjectName(object_name)
        panel.setAccessibleName(accessible_name)
        panel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        panel.setMaximumWidth(int(maximum_width))
        if tooltip:
            panel.setToolTip(tooltip)
        layout = QGridLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setHorizontalSpacing(10)
        layout.setVerticalSpacing(8)
        layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        panel.setLayout(layout)
        return panel, layout

    def _sync_device_profiles_table_to_settings_focus(self) -> None:
        if not hasattr(self, "device_profiles_table"):
            return
        focused_id = int(self._settings_radio_focus_id or 0)
        if focused_id <= 0:
            return
        table = self.device_profiles_table
        table_was_blocked = table.blockSignals(True)
        try:
            for row in range(table.rowCount()):
                item = table.item(row, 3)
                try:
                    row_id = int(item.data(Qt.UserRole) or 0) if item is not None else 0
                except Exception:
                    row_id = 0
                wrapper = table.cellWidget(row, 0)
                chk = wrapper.findChild(QCheckBox) if wrapper is not None else None
                if chk is not None:
                    was_blocked = chk.blockSignals(True)
                    chk.setChecked(row_id == focused_id)
                    chk.blockSignals(was_blocked)
                if row_id == focused_id:
                    table.setCurrentCell(row, 3)
        finally:
            table.blockSignals(table_was_blocked)

    def _radio_selector_button_role(
        self,
        profile: Dict[str, Any],
        *,
        selected: bool,
        readiness_report: Any | None = None,
    ) -> str:
        if selected:
            return "primary"
        if not int(profile.get("enabled", 1) or 0):
            return "muted"
        if self._profile_needs_operator_name(profile):
            return "warning"
        status = self._device_readiness_summary(profile, readiness_report).strip().lower()
        if any(token in status for token in ("offline", "unreachable", "not responding", "failed")):
            return "danger"
        if any(token in status for token in ("needs", "warning", "degraded", "issue", "missing")):
            return "warning"
        if "ready" in status or "ok" in status:
            return "success_muted"
        return "info"

    def _radio_selector_button_text(self, profile: Dict[str, Any], readiness_report: Any | None = None) -> str:
        name = self._profile_display_name(profile)
        status_bits: List[str] = []
        readiness = self._device_readiness_summary(profile, readiness_report).strip()
        if readiness:
            status_bits.append(readiness)
        if int(profile.get("runtime_active", 0) or 0) == 1:
            status_bits.append("Active")
        else:
            status_bits.append("Inactive")
        if int(profile.get("runtime_primary", 0) or 0) == 1:
            status_bits.append("Default")
        if self._profile_needs_operator_name(profile):
            status_bits.append("Name Needed")
        device_class = self._device_class_label(str(profile.get("device_class", "") or ""))
        if device_class and device_class.lower() != "transceiver":
            status_bits.append(device_class)
        return f"{name}\n{' | '.join(dict.fromkeys(status_bits))}"

    def _rebuild_device_profile_selector(self) -> None:
        if not hasattr(self, "device_profile_selector_layout"):
            return
        layout = self.device_profile_selector_layout
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._settings_radio_selector_buttons = {}
        focused_id = int(self._ensure_settings_radio_focus_id() or 0)
        readiness_report = self._current_station_readiness_report()
        theme = resolve_theme(self.settings)
        if not self.device_profiles:
            empty = QLabel("No radios are configured yet. Add a radio to start the selected-radio workflow.")
            empty.setWordWrap(True)
            layout.addWidget(empty)
            layout.addStretch()
            self._refresh_radio_settings_nav_label()
            return
        for profile in self.device_profiles:
            if not isinstance(profile, dict):
                continue
            radio_id = int(profile.get("id", 0) or 0)
            if radio_id <= 0:
                continue
            selected = radio_id == focused_id
            btn = QPushButton(self._radio_selector_button_text(profile, readiness_report))
            btn.setCheckable(True)
            btn.setChecked(selected)
            btn.setMinimumWidth(170)
            btn.setMinimumHeight(52)
            btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            btn.setToolTip(
                f"Edit {self._profile_display_name(profile)}. "
                f"Software: {self._device_software_summary(profile)}. "
                f"Endpoint: {self._device_endpoint_summary(profile)}."
            )
            btn.clicked.connect(lambda _checked=False, ident=radio_id: self._set_settings_radio_focus(ident))
            btn.setStyleSheet(button_style(self._radio_selector_button_role(profile, selected=selected, readiness_report=readiness_report), theme))
            self._settings_radio_selector_buttons[radio_id] = btn
            layout.addWidget(btn)
        layout.addStretch()
        self._refresh_radio_settings_nav_label()

    @staticmethod
    def _profile_needs_operator_name(profile: Optional[Dict[str, Any]]) -> bool:
        return bool(isinstance(profile, dict) and int(profile.get("needs_operator_name", 0) or 0) == 1)

    def _selected_radio_detail_text(
        self,
        profile: Optional[Dict[str, Any]],
        readiness_report: Any | None = None,
    ) -> str:
        if not isinstance(profile, dict):
            return "Select a radio to edit that radio's settings."
        radio_id = int(profile.get("id", 0) or 0)
        assignment = self._effective_assignment_map().get(radio_id, {})
        assignment_name = str(assignment.get("operating_profile_name", "") or "").strip() or "Unassigned"
        assignment_state = str(assignment.get("assignment_state", "") or "").strip().lower()
        flags = []
        if self._profile_needs_operator_name(profile):
            flags.append("Name Needed")
        if int(profile.get("runtime_primary", 0) or 0) == 1:
            flags.append("Station Default")
        if int(profile.get("runtime_active", 0) or 0) == 1:
            flags.append("Active")
        else:
            flags.append("Inactive")
        enabled = "Enabled" if int(profile.get("enabled", 1) or 0) == 1 else "Disabled"
        flags.append(enabled)
        schedule_text = self._assignment_display_text(assignment_name, assignment_state)
        readiness_text = "Not evaluated"
        if readiness_report is not None and radio_id > 0:
            summary_for_radio = getattr(readiness_report, "summary_for_radio", None)
            if callable(summary_for_radio):
                summary = summary_for_radio(radio_id)
                if summary is not None:
                    readiness_text = readiness_summary_status_text(
                        summary,
                        subject=str(profile.get("name", "") or "This radio").strip() or "This radio",
                    )
        detail_lines = [
            f"State: {'; '.join(flags)}",
            f"Readiness: {readiness_text}",
            f"Radio model: {self._device_radio_model_summary(profile)}",
            f"Role: {self._device_class_label(str(profile.get('device_class', '') or ''))}",
            f"Control: {self._device_backend_label(str(profile.get('control_backend', '') or ''))}",
            f"Software: {self._device_software_summary(profile)}",
            f"Connection: {self._device_endpoint_summary(profile)}",
            f"Schedule: {schedule_text}",
            f"PTT group: {self._device_ptt_group_label(profile.get('ptt_group', ''))}",
        ]
        notes = str(profile.get("notes", "") or "").strip()
        if notes:
            detail_lines.append(f"Notes: {notes}")
        return "\n".join(detail_lines)

    def _selected_radio_status_chip_defs(
        self,
        profile: Optional[Dict[str, Any]],
        readiness_report: Any | None = None,
    ) -> List[Tuple[str, str]]:
        if not isinstance(profile, dict):
            return []
        chips: List[Tuple[str, str]] = []
        radio_id = int(profile.get("id", 0) or 0)
        if self._profile_needs_operator_name(profile):
            chips.append(("Name Needed", "warning"))
        if int(profile.get("runtime_primary", 0) or 0) == 1:
            chips.append(("Station Default", "success"))
        chips.append(
            (
                "Active" if int(profile.get("runtime_active", 0) or 0) == 1 else "Inactive",
                "info" if int(profile.get("runtime_active", 0) or 0) == 1 else "muted",
            )
        )
        chips.append(
            (
                "Enabled" if int(profile.get("enabled", 1) or 0) == 1 else "Disabled",
                "success" if int(profile.get("enabled", 1) or 0) == 1 else "danger",
            )
        )
        readiness_label = "Not Evaluated"
        readiness_role = "muted"
        if readiness_report is not None and radio_id > 0:
            summary_for_radio = getattr(readiness_report, "summary_for_radio", None)
            if callable(summary_for_radio):
                summary = summary_for_radio(radio_id)
                if summary is not None:
                    readiness_label = readiness_summary_badge_text(summary)
                    readiness_role = readiness_state_card_level(str(summary.overall_state or ""))
        chips.append((readiness_label, readiness_role))
        return chips

    @staticmethod
    def _status_chip_style(role: str, theme: Dict[str, str]) -> str:
        normalized = str(role or "muted").strip().lower()
        bg_key = {
            "success": "success",
            "warning": "warning",
            "danger": "danger",
            "info": "info",
        }.get(normalized)
        bg = theme.get(bg_key, theme.get("surface_alt", "#e5e7eb")) if bg_key else theme.get("surface_alt", "#e5e7eb")
        fg = "#111111" if normalized == "warning" else "#FFFFFF"
        if not bg_key:
            fg = theme.get("text_muted", theme.get("text", "#333333"))
        border = theme.get(bg_key, theme.get("border", "#999999")) if bg_key else theme.get("border", "#999999")
        return (
            f"background: {bg}; "
            f"color: {fg}; "
            f"border: 1px solid {border}; "
            "border-radius: 4px; "
            "padding: 3px 8px; "
            "font-weight: 600;"
        )

    def _make_status_chip_label(
        self,
        label: str,
        role: str,
        theme: Dict[str, str],
        accessible_prefix: str,
    ) -> QLabel:
        text = str(label or "").strip() or "--"
        chip = QLabel(text)
        chip.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        chip.setStyleSheet(self._status_chip_style(role, theme))
        chip.setAccessibleName(f"{accessible_prefix}: {text}")
        return chip

    def _refresh_device_profile_status_chips(
        self,
        profile: Optional[Dict[str, Any]],
        readiness_report: Any | None = None,
    ) -> None:
        if not hasattr(self, "device_profile_status_chips_layout"):
            return
        layout = self.device_profile_status_chips_layout
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        chip_defs = self._selected_radio_status_chip_defs(profile, readiness_report)
        if hasattr(self, "device_profile_status_chips_widget"):
            self.device_profile_status_chips_widget.setVisible(bool(chip_defs))
        if not chip_defs:
            return
        theme = resolve_theme(self.settings)
        for label, role in chip_defs:
            chip = self._make_status_chip_label(label, role, theme, "Selected radio status")
            layout.addWidget(chip)
        layout.addStretch(1)

    def _selected_radio_connection_detail_rows(
        self,
        profile: Optional[Dict[str, Any]],
    ) -> Tuple[Tuple[str, str], ...]:
        if not isinstance(profile, dict):
            return (
                ("backend", "--"),
                ("endpoint", "--"),
                ("ptt", "--"),
                ("launch", "--"),
            )
        return (
            ("backend", self._device_backend_label(str(profile.get("control_backend", "") or ""))),
            ("endpoint", self._device_endpoint_summary(profile)),
            ("ptt", self._device_ptt_group_label(profile.get("ptt_group", ""))),
            ("launch", self._radio_profile_launch_control_summary(profile)),
        )

    def _refresh_radio_profile_connection_details(self, profile: Optional[Dict[str, Any]]) -> None:
        rows = dict(self._selected_radio_connection_detail_rows(profile))
        label_map = {
            "backend": getattr(self, "radio_profile_connection_backend_label", None),
            "endpoint": getattr(self, "radio_profile_connection_endpoint_label", None),
            "ptt": getattr(self, "radio_profile_connection_ptt_label", None),
            "launch": getattr(self, "radio_profile_connection_launch_label", None),
        }
        for key, label in label_map.items():
            if isinstance(label, QLabel):
                value = str(rows.get(key, "--") or "--")
                label.setText(value)
                label.setAccessibleName(f"Selected radio {key}: {value}")

    @staticmethod
    def _set_form_detail_label(label: QLabel, value: object, accessible_prefix: str, *, hide_empty: bool = False) -> None:
        text = str(value or "").strip() or "--"
        label.setText(text)
        label.setAccessibleName(f"{accessible_prefix}: {text}")
        if not hide_empty:
            label.setVisible(True)
            return
        visible = text != "--"
        label.setVisible(visible)
        parent = label.parentWidget()
        layout = parent.layout() if parent is not None else None
        if isinstance(layout, QFormLayout):
            row_label = layout.labelForField(label)
            if row_label is not None:
                row_label.setVisible(visible)

    def _selected_radio_frequency_timer_rows(
        self,
        profile: Optional[Dict[str, Any]],
    ) -> Tuple[Tuple[str, str], ...]:
        if not isinstance(profile, dict):
            return (
                ("schedule", "--"),
                ("scheduler", "--"),
                ("js8_offset", "--"),
                ("timer_source", "--"),
            )
        radio_id = int(profile.get("id", 0) or 0)
        assignment = self._effective_assignment_map().get(radio_id, {}) if radio_id > 0 else {}
        assignment_name = str(assignment.get("operating_profile_name", "") or "").strip() or "Unassigned"
        assignment_state = str(assignment.get("assignment_state", "") or "").strip().lower()
        schedule = self._assignment_display_text(assignment_name, assignment_state)
        operating = self._operating_profile_by_id(int(assignment.get("operating_profile_id", 0) or 0))
        if isinstance(operating, dict):
            scheduler_mode = self._scheduler_mode_label(operating.get("scheduler_mode", "full"))
            scheduler = (
                f"{'Enabled' if int(operating.get('scheduler_enabled', 1) or 0) == 1 else 'Off'} / {scheduler_mode}"
            )
        elif assignment_name != "Unassigned":
            scheduler = str(assignment.get("shell_summary", "") or "Assigned plan")
        else:
            scheduler = "No assigned plan"
        try:
            offset = int(profile.get("js8_offset_hz", 0) or 0)
        except Exception:
            offset = 0
        js8_offset = f"{offset} Hz"
        timer_source = self._selected_radio_timer_policy_summary(profile)
        return (
            ("schedule", schedule),
            ("scheduler", scheduler),
            ("js8_offset", js8_offset),
            ("timer_source", timer_source),
        )

    @staticmethod
    def _selected_radio_timer_policy_summary(profile: Mapping[str, Any]) -> str:
        def _value(key: str, default: str) -> str:
            value = str(profile.get(key, default) or default).strip()
            return value or default

        scheduler = "On" if int(profile.get("scheduler_enabled", 1) or 0) == 1 else "Off"
        hold_minutes = SettingsTab._radio_profile_hold_duration_minutes(profile)
        freq_mode = _value("freq_enforcement_mode", "On Schedule Change")
        fldigi_mode = _value("fldigi_enforcement_mode", "On Schedule Change")
        js8_mode = _value("js8_enforcement_mode", "On Schedule Change")
        freq_prompt = _value("freq_prompt_interval", "Hourly")
        fldigi_prompt = _value("fldigi_prompt_interval", "Hourly")
        js8_prompt = _value("js8_prompt_interval", "Hourly")
        return (
            f"Radio policy: scheduler {scheduler}; "
            f"Hold {hold_minutes} min; "
            f"Freq {freq_mode} ({freq_prompt}); "
            f"FLDigi {fldigi_mode} ({fldigi_prompt}); "
            f"JS8 {js8_mode} ({js8_prompt})"
        )

    def _refresh_radio_profile_frequency_timer_details(self, profile: Optional[Dict[str, Any]]) -> None:
        rows = dict(self._selected_radio_frequency_timer_rows(profile))
        label_map = {
            "schedule": getattr(self, "radio_profile_frequency_schedule_label", None),
            "scheduler": getattr(self, "radio_profile_frequency_scheduler_label", None),
            "js8_offset": getattr(self, "radio_profile_frequency_js8_offset_label", None),
            "timer_source": getattr(self, "radio_profile_frequency_timer_source_label", None),
        }
        for key, label in label_map.items():
            if isinstance(label, QLabel):
                self._set_form_detail_label(label, rows.get(key, "--"), f"Selected radio {key}")
        self._refresh_radio_profile_timer_policy_controls(profile)

    @staticmethod
    def _radio_profile_timer_policy_text(
        profile: Mapping[str, Any],
        key: str,
        default: str,
        choices: Sequence[str],
    ) -> str:
        value = str(profile.get(key, default) or default).strip()
        return value if value in set(choices) else default

    @staticmethod
    def _radio_profile_hold_duration_minutes(profile: Mapping[str, Any]) -> int:
        try:
            minutes = int(profile.get("schedule_hold_minutes_default", DEFAULT_HOLD_DURATION_MINUTES) or DEFAULT_HOLD_DURATION_MINUTES)
        except Exception:
            minutes = DEFAULT_HOLD_DURATION_MINUTES
        return minutes if minutes in SUPPORTED_HOLD_DURATION_MINUTES else DEFAULT_HOLD_DURATION_MINUTES

    def _refresh_radio_profile_timer_policy_controls(self, profile: Optional[Dict[str, Any]] = None) -> None:
        controls = getattr(self, "_radio_profile_timer_policy_controls", {})
        if not controls:
            return
        if profile is None:
            profile = self._selected_settings_radio_profile()
        has_profile = isinstance(profile, dict)
        mode_choices = ("On Schedule Change", "Prompt")
        prompt_choices = ("Hourly", "Every 5 minutes", "Every 10 minutes", "Every 15 minutes", "Every 30 minutes")
        values = {
            "scheduler_enabled": bool(has_profile and int(profile.get("scheduler_enabled", 1) or 0) == 1),
            "schedule_hold_minutes_default": self._radio_profile_hold_duration_minutes(profile or {}),
            "freq_enforcement_mode": self._radio_profile_timer_policy_text(
                profile or {}, "freq_enforcement_mode", "On Schedule Change", mode_choices
            ),
            "freq_prompt_interval": self._radio_profile_timer_policy_text(profile or {}, "freq_prompt_interval", "Hourly", prompt_choices),
            "fldigi_enforcement_mode": self._radio_profile_timer_policy_text(
                profile or {}, "fldigi_enforcement_mode", "On Schedule Change", mode_choices
            ),
            "fldigi_prompt_interval": self._radio_profile_timer_policy_text(
                profile or {}, "fldigi_prompt_interval", "Hourly", prompt_choices
            ),
            "js8_enforcement_mode": self._radio_profile_timer_policy_text(
                profile or {}, "js8_enforcement_mode", "On Schedule Change", mode_choices
            ),
            "js8_prompt_interval": self._radio_profile_timer_policy_text(profile or {}, "js8_prompt_interval", "Hourly", prompt_choices),
        }
        self._refreshing_radio_profile_timer_policy = True
        try:
            scheduler_chk = controls.get("scheduler_enabled")
            if isinstance(scheduler_chk, QCheckBox):
                was_blocked = scheduler_chk.blockSignals(True)
                scheduler_chk.setChecked(bool(values["scheduler_enabled"]))
                scheduler_chk.blockSignals(was_blocked)
                scheduler_chk.setEnabled(has_profile)
                scheduler_chk.setToolTip(
                    "Enable or disable scheduler automation for the selected radio."
                    if has_profile
                    else "Select a radio before changing timer behavior."
                )
            hold_combo = controls.get("schedule_hold_minutes_default")
            if isinstance(hold_combo, QComboBox):
                was_blocked = hold_combo.blockSignals(True)
                hold_combo.setCurrentText(f"{int(values['schedule_hold_minutes_default'])} minutes")
                hold_combo.blockSignals(was_blocked)
                hold_combo.setEnabled(has_profile)
                hold_combo.setToolTip(
                    "Choose the default QSY/Suspend hold duration for the selected radio."
                    if has_profile
                    else "Select a radio before changing timer behavior."
                )
            for mode_key, prompt_key in (
                ("freq_enforcement_mode", "freq_prompt_interval"),
                ("fldigi_enforcement_mode", "fldigi_prompt_interval"),
                ("js8_enforcement_mode", "js8_prompt_interval"),
            ):
                mode_combo = controls.get(mode_key)
                prompt_combo = controls.get(prompt_key)
                if isinstance(mode_combo, QComboBox):
                    was_blocked = mode_combo.blockSignals(True)
                    mode_combo.setCurrentText(str(values[mode_key]))
                    mode_combo.blockSignals(was_blocked)
                    mode_combo.setEnabled(has_profile)
                    mode_combo.setToolTip(
                        "Choose whether this radio changes automatically on schedule changes or prompts first."
                        if has_profile
                        else "Select a radio before changing timer behavior."
                    )
                if isinstance(prompt_combo, QComboBox):
                    was_blocked = prompt_combo.blockSignals(True)
                    prompt_combo.setCurrentText(str(values[prompt_key]))
                    prompt_combo.blockSignals(was_blocked)
                    prompt_enabled = has_profile and str(values[mode_key]) == "Prompt"
                    prompt_combo.setEnabled(prompt_enabled)
                    prompt_combo.setToolTip(
                        "Choose how often FIO should prompt while this timer is in Prompt mode."
                        if prompt_enabled
                        else "Prompt interval is used only when mode is Prompt."
                    )
        finally:
            self._refreshing_radio_profile_timer_policy = False

    def _radio_profile_timer_policy_control_values(self) -> Dict[str, Any]:
        controls = getattr(self, "_radio_profile_timer_policy_controls", {})
        scheduler_chk = controls.get("scheduler_enabled")

        def combo_text(key: str, default: str) -> str:
            combo = controls.get(key)
            if isinstance(combo, QComboBox):
                return combo.currentText().strip() or default
            return default

        freq_mode = combo_text("freq_enforcement_mode", "On Schedule Change")
        fldigi_mode = combo_text("fldigi_enforcement_mode", "On Schedule Change")
        js8_mode = combo_text("js8_enforcement_mode", "On Schedule Change")
        hold_combo = controls.get("schedule_hold_minutes_default")
        hold_minutes = DEFAULT_HOLD_DURATION_MINUTES
        if isinstance(hold_combo, QComboBox):
            try:
                hold_minutes = int(hold_combo.currentText().split()[0])
            except Exception:
                hold_minutes = DEFAULT_HOLD_DURATION_MINUTES

        def prompt_text(mode: str, key: str) -> str:
            if mode != "Prompt":
                return "Hourly"
            return combo_text(key, "Hourly")

        return {
            "scheduler_enabled": bool(scheduler_chk.isChecked()) if isinstance(scheduler_chk, QCheckBox) else False,
            "schedule_hold_minutes_default": (
                hold_minutes if hold_minutes in SUPPORTED_HOLD_DURATION_MINUTES else DEFAULT_HOLD_DURATION_MINUTES
            ),
            "freq_enforcement_mode": freq_mode,
            "freq_prompt_interval": prompt_text(freq_mode, "freq_prompt_interval"),
            "fldigi_enforcement_mode": fldigi_mode,
            "fldigi_prompt_interval": prompt_text(fldigi_mode, "fldigi_prompt_interval"),
            "js8_enforcement_mode": js8_mode,
            "js8_prompt_interval": prompt_text(js8_mode, "js8_prompt_interval"),
        }

    def _on_radio_profile_timer_policy_changed(self) -> None:
        if getattr(self, "_refreshing_radio_profile_timer_policy", False):
            return
        profile = self._selected_settings_radio_profile()
        if not isinstance(profile, dict):
            self._refresh_radio_profile_timer_policy_controls(None)
            return
        payload = dict(profile)
        payload.update(self._radio_profile_timer_policy_control_values())
        self._persist_device_profile(payload, existing=profile)
        refreshed = self._selected_settings_radio_profile() or payload
        self._refresh_radio_profile_frequency_timer_details(refreshed)
        self._update_device_profile_readiness_detail()
        self._publish_radio_profile_timer_policy_feedback(refreshed)

    def _publish_radio_profile_timer_policy_feedback(self, profile: Optional[Dict[str, Any]]) -> None:
        name = self._profile_display_name(profile) if isinstance(profile, dict) else "selected radio"
        radio_id = None
        if isinstance(profile, dict):
            profile_id = int(profile.get("id", 0) or 0)
            radio_id = str(profile_id) if profile_id > 0 else None
        self._publish_settings_action_feedback(
            status="succeeded",
            summary=f"Updated timer policy for {name}.",
            detail=self._selected_radio_timer_policy_summary(profile or {}),
            action_type="timer_policy",
            radio_profile_id=radio_id,
            target_label=name,
        )

    @staticmethod
    def _radio_profile_optional_value(profile: Optional[Dict[str, Any]], key: str) -> str:
        if not isinstance(profile, dict):
            return "--"
        return str(profile.get(key, "") or "").strip() or "--"

    def _selected_radio_optional_group_rows(
        self,
        profile: Optional[Dict[str, Any]],
    ) -> Tuple[Tuple[str, str], ...]:
        return (
            ("ptt", self._device_ptt_group_label(profile.get("ptt_group", "")) if isinstance(profile, dict) else "--"),
            ("antenna", self._radio_profile_optional_value(profile, "antenna_group")),
            ("frontend", self._radio_profile_optional_value(profile, "frontend_group")),
            ("amplifier", self._radio_profile_optional_value(profile, "amplifier_group")),
            ("notes", self._radio_profile_optional_value(profile, "notes")),
        )

    def _refresh_radio_profile_optional_groups(self, profile: Optional[Dict[str, Any]]) -> None:
        rows = dict(self._selected_radio_optional_group_rows(profile))
        label_map = {
            "ptt": getattr(self, "radio_profile_optional_ptt_label", None),
            "antenna": getattr(self, "radio_profile_optional_antenna_label", None),
            "frontend": getattr(self, "radio_profile_optional_frontend_label", None),
            "amplifier": getattr(self, "radio_profile_optional_amplifier_label", None),
            "notes": getattr(self, "radio_profile_optional_notes_label", None),
        }
        for key, label in label_map.items():
            if isinstance(label, QLabel):
                self._set_form_detail_label(
                    label,
                    rows.get(key, "--"),
                    f"Selected radio optional {key}",
                    hide_empty=key in {"antenna", "frontend", "amplifier", "notes"},
                )

    def _selected_radio_inventory_rows(
        self,
        profile: Optional[Dict[str, Any]],
    ) -> Tuple[Tuple[str, str], ...]:
        if not isinstance(profile, dict):
            return (
                ("id", "--"),
                ("system_key", "--"),
                ("instance", "--"),
                ("class", "--"),
                ("model", "--"),
                ("runtime", "--"),
            )
        profile_id = int(profile.get("id", 0) or 0)
        instance_number = int(profile.get("instance_number", 0) or 0)
        device_class = self._device_class_label(str(profile.get("device_class", "") or ""))
        deployment = self._device_deployment_label(str(profile.get("deployment_mode", "") or ""))
        runtime_bits: List[str] = []
        runtime_bits.append("Enabled" if int(profile.get("enabled", 1) or 0) == 1 else "Disabled")
        if int(profile.get("runtime_primary", 0) or 0) == 1:
            runtime_bits.append("Station Default")
        if int(profile.get("runtime_active", 0) or 0) == 1:
            runtime_bits.append("Active")
        else:
            runtime_bits.append("Inactive")
        return (
            ("id", str(profile_id) if profile_id > 0 else "--"),
            ("system_key", str(profile.get("system_key", "") or "").strip() or "--"),
            ("instance", str(instance_number) if instance_number > 0 else "--"),
            ("class", f"{device_class} / {deployment}"),
            ("model", self._device_radio_model_summary(profile)),
            ("runtime", "; ".join(runtime_bits)),
        )

    def _refresh_radio_profile_inventory_details(self, profile: Optional[Dict[str, Any]]) -> None:
        rows = dict(self._selected_radio_inventory_rows(profile))
        label_map = {
            "id": getattr(self, "radio_profile_inventory_id_label", None),
            "system_key": getattr(self, "radio_profile_inventory_system_key_label", None),
            "instance": getattr(self, "radio_profile_inventory_instance_label", None),
            "class": getattr(self, "radio_profile_inventory_class_label", None),
            "model": getattr(self, "radio_profile_inventory_model_label", None),
            "runtime": getattr(self, "radio_profile_inventory_runtime_label", None),
        }
        for key, label in label_map.items():
            if isinstance(label, QLabel):
                self._set_form_detail_label(label, rows.get(key, "--"), f"Selected radio inventory {key}")

    @staticmethod
    def _radio_profile_software_flag_defs() -> Tuple[Tuple[str, str], ...]:
        return (
            ("flrig", "FLRig"),
            ("fldigi", "FLDigi"),
            ("flmsg", "FLMsg"),
            ("flamp", "FLAmp"),
            ("js8call", "JS8Call"),
            ("js8spotter", "JS8Spotter"),
            ("commstat", "CommStat"),
            ("varac", "VarAC"),
        )

    @staticmethod
    def _radio_profile_software_flag_field(key: str) -> str:
        normalized = str(key or "").strip().lower()
        return f"use_{normalized}" if normalized in {key for key, _label in SettingsTab._radio_profile_software_flag_defs()} else ""

    @staticmethod
    def _radio_profile_software_flag_label(key: str) -> str:
        normalized = str(key or "").strip().lower()
        for flag_key, label in SettingsTab._radio_profile_software_flag_defs():
            if flag_key == normalized:
                return label
        return normalized.upper() if normalized else "Software"

    @staticmethod
    def _radio_profile_backend_locked_software(profile: Optional[Dict[str, Any]]) -> Tuple[str, ...]:
        if not isinstance(profile, dict):
            return ()
        backend = str(profile.get("control_backend", "") or "").strip().lower()
        if backend == "flrig":
            return ("flrig",)
        if backend == "js8call":
            return ("js8call",)
        if backend == "rigctld":
            return ("rigctld",)
        return ()

    @staticmethod
    def _radio_profile_software_option_keys() -> Tuple[str, ...]:
        return (
            "flrig",
            "fldigi",
            "flmsg",
            "flamp",
            "rigctld",
            "js8call",
            "js8spotter",
            "commstat",
            "varac",
        )

    def _radio_profile_has_software_option(self, profile: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(profile, dict):
            return False
        return any(
            self._radio_software_enabled(profile, key)
            for key in self._radio_profile_software_option_keys()
        )

    @staticmethod
    def _radio_profile_launch_opt_in_enabled(profile: Optional[Dict[str, Any]]) -> bool:
        return bool(isinstance(profile, dict) and int(profile.get("launch_enabled", 0) or 0) == 1)

    @staticmethod
    def _radio_profile_operating_launch_allowed(profile: Optional[Dict[str, Any]]) -> bool:
        return bool(isinstance(profile, dict) and int(profile.get("use_launch_control", 0) or 0) == 1)

    @classmethod
    def _radio_profile_launch_control_enabled(cls, profile: Optional[Dict[str, Any]]) -> bool:
        # Transitional Settings visibility: show Launch Control when either the radio opt-in
        # or the projected operating-plan policy references launch control.
        return cls._radio_profile_launch_opt_in_enabled(profile) or cls._radio_profile_operating_launch_allowed(profile)

    @classmethod
    def _radio_profile_launch_control_summary(cls, profile: Optional[Dict[str, Any]]) -> str:
        if not isinstance(profile, dict):
            return "--"
        radio_enabled = cls._radio_profile_launch_opt_in_enabled(profile)
        plan_allowed = cls._radio_profile_operating_launch_allowed(profile)
        has_plan_value = "use_launch_control" in profile
        if radio_enabled and (plan_allowed or not has_plan_value):
            return "Radio opt-in; plan allows launch" if has_plan_value else "Radio opt-in"
        if radio_enabled:
            return "Radio opt-in; plan launch off"
        if plan_allowed:
            return "Plan allows launch; radio opt-out"
        return "Off"

    @classmethod
    def _radio_profile_effective_launch_control_enabled(cls, profile: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(profile, dict):
            return False
        if "use_launch_control" not in profile:
            return cls._radio_profile_launch_opt_in_enabled(profile)
        return cls._radio_profile_launch_opt_in_enabled(profile) and cls._radio_profile_operating_launch_allowed(profile)

    def _radio_profile_no_software_message(self, profile: Optional[Dict[str, Any]]) -> Tuple[str, str]:
        if not isinstance(profile, dict):
            return ("Select a radio before choosing the software used by that radio.", "muted")
        if int(profile.get("enabled", 1) or 0) != 1:
            return (
                "No radio software is enabled yet. Enable software above when this radio should participate in FIO workflows.",
                "muted",
            )
        if int(profile.get("runtime_active", 0) or 0) == 1 or int(profile.get("runtime_primary", 0) or 0) == 1:
            return (
                "No software options are enabled for this radio. Enable at least one software option above so FIO can operate it.",
                "warning",
            )
        return (
            "No radio software is enabled yet. Enable at least one software option above before using this radio.",
            "warning",
        )

    def _radio_profile_no_software_stack_guidance_item(
        self,
        profile: Optional[Dict[str, Any]],
    ) -> Tuple[str, str, str, str] | None:
        if self._radio_profile_has_software_option(profile):
            return None
        message, role = self._radio_profile_no_software_message(profile)
        if role != "warning":
            return None
        return (message, "Enable Software Options", "radio_profile_software_stack_section", "warning")

    def _refresh_radio_profile_software_flag_controls(self, profile: Optional[Dict[str, Any]] = None) -> None:
        checks = getattr(self, "_radio_profile_software_flag_checks", {})
        if not checks:
            return
        if profile is None:
            profile = self._selected_settings_radio_profile()
        has_profile = isinstance(profile, dict)
        locked = set(self._radio_profile_backend_locked_software(profile))
        self._refreshing_radio_profile_software_flags = True
        try:
            for key, chk in checks.items():
                locked_for_backend = key in locked
                checked = bool(has_profile and (self._radio_software_enabled(profile, key) or locked_for_backend))
                was_blocked = chk.blockSignals(True)
                chk.setChecked(checked)
                chk.blockSignals(was_blocked)
                chk.setEnabled(has_profile and not locked_for_backend)
                if not has_profile:
                    chk.setToolTip("Select a radio before changing the software used by that radio.")
                elif locked_for_backend:
                    chk.setToolTip("This software is required by the selected radio's control backend.")
                else:
                    chk.setToolTip(f"Enable {chk.text()} for the selected radio.")
        finally:
            self._refreshing_radio_profile_software_flags = False

    def _on_radio_profile_software_flag_changed(self, key: str) -> None:
        if getattr(self, "_refreshing_radio_profile_software_flags", False):
            return
        profile = self._selected_settings_radio_profile()
        if not isinstance(profile, dict):
            self._refresh_radio_profile_software_flag_controls(None)
            return
        field = self._radio_profile_software_flag_field(key)
        if not field:
            return
        payload = dict(profile)
        for software_key, _label in self._radio_profile_software_flag_defs():
            target_field = self._radio_profile_software_flag_field(software_key)
            chk = self._radio_profile_software_flag_checks.get(software_key)
            if target_field and chk is not None:
                payload[target_field] = bool(chk.isChecked())
        for locked_key in self._radio_profile_backend_locked_software(profile):
            locked_field = self._radio_profile_software_flag_field(locked_key)
            if locked_field:
                payload[locked_field] = True
        self._persist_device_profile(payload, existing=profile)
        self._refresh_radio_specific_section_visibility()
        self._update_device_profile_readiness_detail()
        self._publish_radio_profile_software_flag_feedback(key, bool(payload.get(field)), profile)

    def _publish_radio_profile_software_flag_feedback(
        self,
        key: str,
        enabled: bool,
        profile: Optional[Dict[str, Any]],
    ) -> None:
        label = self._radio_profile_software_flag_label(key)
        name = self._profile_display_name(profile) if isinstance(profile, dict) else "selected radio"
        action = "Enabled" if enabled else "Disabled"
        radio_id = None
        if isinstance(profile, dict):
            profile_id = int(profile.get("id", 0) or 0)
            radio_id = str(profile_id) if profile_id > 0 else None
        self._publish_settings_action_feedback(
            status="succeeded",
            summary=f"{action} {label} for {name}.",
            detail=(
                f"Software Used updated in Radio Profile for {name}. "
                "Use the radio-specific Settings panels to configure paths and endpoints."
            ),
            action_type="software_flags",
            radio_profile_id=radio_id,
            target_label=name,
        )

    @staticmethod
    def _software_family_integration_keys(family: str) -> Tuple[str, ...]:
        normalized = str(family or "").strip().lower()
        if normalized == "js8":
            return ("js8call", "js8spotter", "commstat")
        if normalized == "fast_light":
            return ("flrig", "fldigi", "flmsg", "flamp", "rigctld")
        if normalized == "varac":
            return ("varac",)
        return ()

    @staticmethod
    def _software_readiness_chip_from_issues(issues: Sequence[Any]) -> Tuple[str, str]:
        state_roles = {
            "needs_setup": ("Needs Setup", "danger"),
            "missing": ("Needs Setup", "danger"),
            "not_configured": ("Needs Setup", "danger"),
            "degraded": ("Review", "warning"),
            "not_enabled": ("Not Enabled", "muted"),
            "external_manual": ("Manual", "info"),
        }
        state_rank = {
            "needs_setup": 0,
            "missing": 0,
            "not_configured": 0,
            "degraded": 1,
            "not_enabled": 2,
            "external_manual": 3,
        }
        state_keys = {
            str(getattr(issue, "state_key", "") or "").strip().lower()
            for issue in issues
            if str(getattr(issue, "state_key", "") or "").strip()
        }
        for state_key in sorted(state_keys, key=lambda key: state_rank.get(key, 99)):
            mapped = state_roles.get(state_key)
            if mapped is not None:
                return mapped
        severities = {str(getattr(issue, "severity", "") or "").strip().lower() for issue in issues}
        if "required" in severities:
            return ("Needs Setup", "danger")
        if "recommended" in severities:
            return ("Review", "warning")
        return ("Info", "info")

    @classmethod
    def _software_family_readiness_chip(
        cls,
        family: str,
        radio_id: int,
        readiness_report: Any | None = None,
    ) -> Tuple[str, str]:
        if readiness_report is None or radio_id <= 0:
            return ("Not Evaluated", "muted")
        target_keys = set(cls._software_family_integration_keys(family))
        if not target_keys:
            return ("Available", "info")
        issues = []
        for issue in getattr(readiness_report, "issues", ()) or ():
            if int(getattr(issue, "radio_id", 0) or 0) != int(radio_id):
                continue
            integration_key = str(getattr(issue, "integration_key", "") or "").strip().lower()
            if integration_key in target_keys:
                issues.append(issue)
        if not issues:
            return ("Ready", "success")
        return cls._software_readiness_chip_from_issues(issues)

    def _refresh_radio_profile_software_chips(self, readiness_report: Any | None = None) -> None:
        if not hasattr(self, "radio_profile_software_chips_layout"):
            return
        layout = self.radio_profile_software_chips_layout
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        profile = self._selected_settings_radio_profile()
        theme = resolve_theme(self.settings)
        if readiness_report is None:
            readiness_report = self._station_readiness_report_for_software_chips()
        radio_id = int(profile.get("id", 0) or 0) if isinstance(profile, dict) else 0
        chip_defs = [
            ("JS8Call", "js8", getattr(self, "js8_section_group", None), bool(isinstance(profile, dict) and (
                self._radio_software_enabled(profile, "js8call")
                or self._radio_software_enabled(profile, "js8spotter")
                or self._radio_software_enabled(profile, "commstat")
            ))),
            ("Fast Light", "fast_light", getattr(self, "fast_light_section_group", None), bool(isinstance(profile, dict) and (
                self._radio_software_enabled(profile, "flrig")
                or self._radio_software_enabled(profile, "fldigi")
                or self._radio_software_enabled(profile, "flmsg")
                or self._radio_software_enabled(profile, "flamp")
                or self._radio_software_enabled(profile, "rigctld")
            ))),
            ("VarAC", "varac", getattr(self, "varac_section_group", None), bool(isinstance(profile, dict) and self._radio_software_enabled(profile, "varac"))),
            ("Launch Control", "launch_control", getattr(self, "launch_control_section_group", None), self._radio_profile_launch_control_enabled(profile)),
        ]
        columns = self._radio_profile_software_chip_columns(
            self.radio_profile_software_chips_widget.width()
            if hasattr(self, "radio_profile_software_chips_widget")
            else 0
        )
        added = 0
        for label, family, target_group, enabled in chip_defs:
            if not enabled:
                continue
            status_label, role = self._software_family_readiness_chip(family, radio_id, readiness_report)
            btn = QPushButton(label)
            btn.setText(f"{label}: {status_label}")
            btn.setMinimumWidth(0)
            btn.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
            btn.setStyleSheet(button_style(role, theme))
            btn.setToolTip(f"Open {label} settings for the selected radio. Status: {status_label}.")
            if isinstance(target_group, QGroupBox):
                btn.clicked.connect(lambda _checked=False, g=target_group: self._select_settings_section_group(g))
            layout.addWidget(btn, added // columns, added % columns)
            added += 1
        if added <= 0:
            empty_text, empty_role = self._radio_profile_no_software_message(profile)
            empty = QLabel(empty_text)
            empty.setWordWrap(True)
            empty.setAccessibleName(empty_text)
            if empty_role == "warning":
                empty.setStyleSheet(self._settings_feedback_label_style("partial", theme))
            else:
                empty.setStyleSheet(f"color: {theme.get('text_muted', theme.get('text', '#666'))};")
            layout.addWidget(empty, 0, 0, 1, columns)
        for col in range(5):
            layout.setColumnStretch(col, 0)
        layout.setColumnStretch(columns, 1)

    @staticmethod
    def _radio_profile_software_chip_columns(width: int) -> int:
        panel_width = max(0, int(width or 0))
        if panel_width < 380:
            return 1
        if panel_width < 620:
            return 2
        return 4

    @staticmethod
    def _stack_guidance_issue_target(issue: Any) -> Tuple[str, str]:
        integration_key = str(getattr(issue, "integration_key", "") or "").strip().lower()
        if integration_key in {"js8call", "js8spotter", "commstat"}:
            return ("Open JS8Call Settings", "js8_section_group")
        if integration_key in {"flrig", "fldigi", "flmsg", "flamp", "rigctld"}:
            return ("Open Fast Light Settings", "fast_light_section_group")
        if integration_key == "varac":
            return ("Open VarAC Settings", "varac_section_group")
        return ("Review Radio Profile", "radio_profile_section_group")

    @staticmethod
    def _stack_guidance_issue_text(issue: Any, radio_name: str = "") -> str:
        text = str(getattr(issue, "message", "") or "").strip()
        name = str(radio_name or "").strip()
        if name and text.lower().startswith(f"{name.lower()}:"):
            text = text.split(":", 1)[1].strip()
        return text or "Review this selected-radio setup item."

    @staticmethod
    def _stack_guidance_issue_role(issue: Any) -> str:
        state_key = str(getattr(issue, "state_key", "") or "").strip().lower()
        state_roles = {
            "needs_setup": "danger",
            "missing": "danger",
            "not_configured": "danger",
            "degraded": "warning",
            "not_enabled": "muted",
            "external_manual": "info",
        }
        mapped = state_roles.get(state_key)
        if mapped is not None:
            return mapped
        severity = str(getattr(issue, "severity", "") or "").strip().lower()
        if severity == "required":
            return "danger"
        if severity == "recommended":
            return "warning"
        return "info"

    @classmethod
    def _stack_guidance_issue_sort_key(cls, issue: Any) -> Tuple[int, int, str]:
        severity_rank = {"required": 0, "recommended": 1, "informational": 2}
        state_rank = {
            "needs_setup": 0,
            "missing": 0,
            "not_configured": 0,
            "degraded": 1,
            "not_enabled": 2,
            "external_manual": 3,
        }
        state_key = str(getattr(issue, "state_key", "") or "").strip().lower()
        severity = str(getattr(issue, "severity", "") or "").strip().lower()
        integration_key = str(getattr(issue, "integration_key", "") or "").strip().lower()
        return (
            state_rank.get(state_key, 99),
            severity_rank.get(severity, 3),
            integration_key,
        )

    @classmethod
    def _selected_radio_stack_guidance_items(
        cls,
        readiness_report: Any | None,
        radio_id: int,
        *,
        radio_name: str = "",
        max_items: int = 4,
    ) -> List[Tuple[str, str, str, str]]:
        if readiness_report is None or radio_id <= 0:
            return []
        issues = [
            issue
            for issue in getattr(readiness_report, "issues", ()) or ()
            if int(getattr(issue, "radio_id", 0) or 0) == int(radio_id)
            and str(getattr(issue, "integration_key", "") or "").strip()
        ]
        issues.sort(key=cls._stack_guidance_issue_sort_key)
        items: List[Tuple[str, str, str, str]] = []
        for issue in issues[: max(0, int(max_items or 0))]:
            action_label, target_attr = cls._stack_guidance_issue_target(issue)
            role = cls._stack_guidance_issue_role(issue)
            items.append((cls._stack_guidance_issue_text(issue, radio_name), action_label, target_attr, role))
        return items

    def _refresh_radio_profile_stack_guidance(
        self,
        readiness_report: Any | None,
        focused_radio_id: int | None,
        profile: Optional[Dict[str, Any]],
    ) -> None:
        if not hasattr(self, "radio_profile_stack_guidance_rows"):
            return
        rows = self.radio_profile_stack_guidance_rows
        while rows.count():
            item = rows.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        name = str(profile.get("name", "") or "Radio").strip() if isinstance(profile, dict) else ""
        items: List[Tuple[str, str, str, str]] = []
        no_software_item = self._radio_profile_no_software_stack_guidance_item(profile)
        if no_software_item is not None:
            items.append(no_software_item)
        items.extend(self._selected_radio_stack_guidance_items(
            readiness_report,
            int(focused_radio_id or 0),
            radio_name=name,
            max_items=max(0, 4 - len(items)),
        ))
        if hasattr(self, "radio_profile_stack_guidance_section"):
            self.radio_profile_stack_guidance_section.setVisible(bool(items))
            self.radio_profile_stack_guidance_section.setChecked(bool(items))
        if hasattr(self, "radio_profile_stack_guidance_widget"):
            self.radio_profile_stack_guidance_widget.setVisible(bool(items))
        if not items:
            return
        theme = resolve_theme(self.settings)
        for message, action_label, target_attr, role in items:
            row = QWidget()
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(8)
            label = QLabel(message)
            label.setWordWrap(True)
            label.setAccessibleName(f"Stack guidance: {message}")
            row_layout.addWidget(label, 1)
            btn = QPushButton(action_label)
            btn.setStyleSheet(button_style(role, theme))
            btn.setToolTip(message)
            btn.setAccessibleName(action_label)
            target_group = getattr(self, target_attr, None)
            if isinstance(target_group, QGroupBox):
                btn.clicked.connect(lambda _checked=False, g=target_group: self._select_settings_section_group(g))
            else:
                btn.setEnabled(False)
            row_layout.addWidget(btn, 0)
            rows.addWidget(row)

    def _refresh_radio_specific_section_visibility(self) -> None:
        profile = self._selected_settings_radio_profile()
        has_profile = isinstance(profile, dict)

        def enabled(key: str) -> bool:
            return bool(has_profile and self._radio_software_enabled(profile, key))

        js8_visible = enabled("js8call") or enabled("js8spotter") or enabled("commstat")
        fast_light_visible = enabled("flrig") or enabled("fldigi") or enabled("flmsg") or enabled("flamp") or enabled("rigctld")
        varac_visible = enabled("varac")
        launch_visible = self._radio_profile_launch_control_enabled(profile)

        self._set_settings_section_visible(getattr(self, "radio_software_scope_section_group", None), False)
        self._set_settings_section_visible(getattr(self, "js8_section_group", None), js8_visible)
        self._set_settings_section_visible(getattr(self, "fast_light_section_group", None), fast_light_visible)
        self._set_settings_section_visible(getattr(self, "varac_section_group", None), varac_visible)
        self._set_settings_section_visible(
            getattr(self, "varac_clusters_section_group", None),
            varac_visible and self._varac_cluster_mode_enabled(),
        )
        self._set_settings_section_visible(
            getattr(self, "varac_memberships_section_group", None),
            varac_visible and self._varac_cluster_mode_enabled(),
        )
        self._set_settings_section_visible(getattr(self, "launch_control_section_group", None), launch_visible)
        self._set_settings_section_visible(getattr(self, "custom_tools_section_group", None), launch_visible)
        self._refresh_radio_profile_software_chips()

    def _emit_device_profiles_changed(self) -> None:
        try:
            self.device_profiles_changed.emit()
        except Exception:
            pass

    @staticmethod
    def _device_backend_label(raw: str) -> str:
        backend = str(raw or "").strip().lower()
        return {
            "flrig": "FLRig",
            "js8call": "JS8Call",
            "manual": "Manual",
            "rigctld": "RIGCTLD",
        }.get(backend, backend.upper() or "Unknown")

    @staticmethod
    def _device_deployment_label(raw: str) -> str:
        mode = str(raw or "").strip().lower()
        return {"full": "Full", "minimal": "Minimal"}.get(mode, mode.title() or "Full")

    @staticmethod
    def _device_class_label(raw: str) -> str:
        device_class = str(raw or "").strip().lower()
        for label, value in DEVICE_CLASS_OPTIONS:
            if value == device_class:
                return label
        return device_class.title() or "Transceiver"

    def _device_endpoint_summary(self, profile: Dict[str, Any]) -> str:
        if str(profile.get("device_class", "") or "").strip().lower() == "observer":
            host = str(profile.get("sdr_host", "") or "").strip()
            port = str(profile.get("sdr_port", "") or "").strip()
            if host and port:
                return f"Observer SDR {host}:{port}"
            if host:
                return f"Observer SDR {host}"
            return "Observer / no endpoint"
        backend = str(profile.get("control_backend", "") or "").strip().lower()
        if backend == "rigctld":
            host = str(profile.get("rig_host", "") or "").strip() or "127.0.0.1"
            port = str(profile.get("rig_port", "") or "").strip() or "4532"
            return f"RIGCTLD {host}:{port}"
        if backend == "js8call":
            host = str(profile.get("js8_host", "") or "").strip() or "127.0.0.1"
            port = str(profile.get("js8_port", "") or "").strip() or "2442"
            return f"JS8 {host}:{port}"
        if backend == "manual":
            return "Manual / no control endpoint"
        flrig_host = str(profile.get("flrig_host", "") or "").strip() or "127.0.0.1"
        flrig_port = str(profile.get("flrig_port", "") or "").strip() or "12345"
        fldigi_host = str(profile.get("fldigi_host", "") or "").strip() or flrig_host
        fldigi_port = str(profile.get("fldigi_port", "") or "").strip() or "7362"
        return f"FLRig {flrig_host}:{flrig_port}; FLDigi {fldigi_host}:{fldigi_port}"

    def _device_software_summary(self, profile: Dict[str, Any]) -> str:
        software: List[str] = []
        backend = str(profile.get("control_backend", "") or "").strip().lower()
        device_class = str(profile.get("device_class", "") or "").strip().lower()
        if self._radio_software_enabled(profile, "flrig"):
            software.append("FLRig")
        elif backend == "rigctld":
            software.append("RigCtlD")
        elif backend == "manual":
            software.append("Manual")

        if self._radio_software_enabled(profile, "fldigi"):
            software.append("FLDigi")
        if self._radio_software_enabled(profile, "flmsg"):
            software.append("FLMsg")
        if self._radio_software_enabled(profile, "flamp"):
            software.append("FLAmp")
        if self._radio_software_enabled(profile, "js8call"):
            software.append("JS8Call")
        if self._radio_software_enabled(profile, "js8spotter"):
            software.append("JS8Spotter")
        if self._radio_software_enabled(profile, "commstat"):
            software.append("CommStat")
        if self._radio_software_enabled(profile, "varac"):
            software.append("VarAC")
        if device_class == "observer":
            software.append("SDR")
        return ", ".join(dict.fromkeys(part for part in software if part)) or "--"

    @staticmethod
    def _device_radio_model_summary(profile: Dict[str, Any]) -> str:
        manufacturer = str(profile.get("radio_manufacturer", "") or "").strip()
        model = str(profile.get("radio_model", "") or "").strip()
        return " ".join(part for part in [manufacturer, model] if part).strip() or "--"

    @staticmethod
    def _radio_software_enabled(profile: Dict[str, Any], key: str) -> bool:
        normalized = str(key or "").strip().lower()
        backend = str(profile.get("control_backend", "") or "").strip().lower()
        if normalized == "flrig":
            explicit = profile.get("use_flrig")
            if explicit not in (None, ""):
                return bool(int(explicit or 0))
            return backend == "flrig" or bool(str(profile.get("flrig_path", "") or "").strip())
        if normalized == "fldigi":
            explicit = profile.get("use_fldigi")
            if explicit not in (None, ""):
                return bool(int(explicit or 0))
            return bool(str(profile.get("fldigi_host", "") or "").strip() or str(profile.get("fldigi_port", "") or "").strip())
        if normalized == "flmsg":
            explicit = profile.get("use_flmsg")
            if explicit not in (None, ""):
                return bool(int(explicit or 0))
            return False
        if normalized == "flamp":
            explicit = profile.get("use_flamp")
            if explicit not in (None, ""):
                return bool(int(explicit or 0))
            return False
        if normalized == "rigctld":
            return backend == "rigctld" or bool(
                str(profile.get("rig_host", "") or "").strip()
                or str(profile.get("rig_port", "") or "").strip()
            )
        if normalized == "js8call":
            explicit = profile.get("use_js8call")
            if explicit not in (None, ""):
                return bool(int(explicit or 0))
            return backend == "js8call" or bool(
                str(profile.get("js8_host", "") or "").strip()
                or str(profile.get("js8_port", "") or "").strip()
                or str(profile.get("js8_install_path", "") or "").strip()
            )
        if normalized == "js8spotter":
            explicit = profile.get("use_js8spotter")
            if explicit not in (None, ""):
                return bool(int(explicit or 0))
            return bool(str(profile.get("spotter_launch_path", "") or "").strip())
        if normalized == "commstat":
            explicit = profile.get("use_commstat")
            if explicit not in (None, ""):
                return bool(int(explicit or 0))
            return bool(str(profile.get("commstat_launch_path", "") or "").strip())
        if normalized == "varac":
            explicit = profile.get("use_varac")
            if explicit not in (None, ""):
                return bool(int(explicit or 0))
            return any(
                [
                    str(profile.get("varac_install_path", "") or "").strip(),
                    str(profile.get("varac_db_path", "") or "").strip(),
                    str(profile.get("varac_ini_path", "") or "").strip(),
                    str(profile.get("launch_cmd", "") or "").strip(),
                    int(profile.get("varac_cluster_member_enabled", 0) or 0) == 1,
                ]
            )
        return False

    def _device_readiness_summary(self, profile: Dict[str, Any], readiness_report: Any | None = None) -> str:
        if not int(profile.get("enabled", 1) or 0):
            return readiness_state_label("not_enabled")
        if readiness_report is not None:
            try:
                summary = readiness_report.summary_for_radio(int(profile.get("id", 0) or 0))
            except Exception:
                summary = None
            if summary is not None:
                return readiness_summary_badge_text(summary)
        backend = str(profile.get("control_backend", "") or "").strip().lower()
        device_class = str(profile.get("device_class", "") or "").strip().lower()
        if device_class == "observer":
            host = str(profile.get("sdr_host", "") or "").strip()
            return readiness_state_label("ready") if host else readiness_state_label("needs_setup")
        if backend == "manual":
            return readiness_state_label("external_manual")
        if backend == "js8call":
            if str(profile.get("js8_host", "") or "").strip() and str(profile.get("js8_port", "") or "").strip():
                return readiness_state_label("ready")
            return readiness_state_label("needs_setup")
        if backend == "rigctld":
            if str(profile.get("rig_host", "") or "").strip() and str(profile.get("rig_port", "") or "").strip():
                return readiness_state_label("ready")
            return readiness_state_label("needs_setup")
        flrig_ok = str(profile.get("flrig_port", "") or "").strip()
        fldigi_ok = str(profile.get("fldigi_host", "") or "").strip() and str(profile.get("fldigi_port", "") or "").strip()
        if flrig_ok and fldigi_ok:
            return readiness_state_label("ready")
        if flrig_ok:
            return readiness_state_label("degraded")
        return readiness_state_label("needs_setup")

    @staticmethod
    def _device_ptt_group_label(value: object) -> str:
        txt = str(value or "").strip()
        return txt or "--"

    @staticmethod
    def _preferred_band_text(value: object) -> str:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return ""
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = [part.strip() for part in text.split(",") if part.strip()]
        elif isinstance(value, (list, tuple, set)):
            parsed = list(value)
        else:
            parsed = []
        bands = []
        for item in parsed:
            token = str(item or "").strip().upper().replace(" ", "")
            if token and token not in bands:
                bands.append(token)
        return ", ".join(bands)

    def _update_device_profiles_hint(self) -> None:
        if not hasattr(self, "device_profiles_hint_label"):
            return
        count = len(self.device_profiles)
        assignment_map = self._effective_assignment_map()
        active_profiles = [
            str(row.get("name", "") or "").strip()
            for row in self.device_profiles
            if isinstance(row, dict) and int(row.get("runtime_active", 0) or 0) == 1
        ]
        primary = next(
            (
                str(row.get("name", "") or "").strip()
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            "",
        )
        observer_count = len(
            [
                row
                for row in self.device_profiles
                if isinstance(row, dict) and str(row.get("device_class", "") or "").strip().lower() == "observer"
            ]
        )
        ptt_groups = sorted(
            {
                str(row.get("ptt_group", "") or "").strip()
                for row in self.device_profiles
                if isinstance(row, dict) and str(row.get("ptt_group", "") or "").strip()
            }
        )
        assigned_schedule_count = len(
            [
                row
                for row in self.device_profiles
                if isinstance(row, dict)
                and int(row.get("id", 0) or 0) in assignment_map
                and assignment_map[int(row.get("id", 0) or 0)].get("operating_profile_id") not in (None, "", 0)
            ]
        )
        focused = self._selected_settings_radio_profile()
        focused_name = self._profile_display_name(focused) if isinstance(focused, dict) else ""
        hint = "Select one radio, then edit that radio's schedule and software settings below."
        if focused_name:
            hint = f"Selected radio: {focused_name}. " + hint
        if primary:
            hint += f" Default radio: {primary}."
        if active_profiles:
            hint += f" Active radios: {', '.join(active_profiles)}."
        if observer_count:
            hint += f" Observer radios: {observer_count}."
        if count <= 1:
            hint += " Additional radios can be added as inactive until you are ready to use them."
        if count > 0:
            hint += f" {assigned_schedule_count}/{count} radios have a schedule assigned."
        if any(
            str(row.get("control_backend", "") or "").strip().lower() == "rigctld"
            for row in self.device_profiles
            if isinstance(row, dict)
        ):
            hint += " RIGCTLD radios use the configured TCP endpoint when selected as the default radio."
        if ptt_groups:
            hint += f" Shared PTT groups: {', '.join(ptt_groups[:5])}."
        conflict_groups = []
        for key, label in (
            ("antenna_group", "antenna"),
            ("frontend_group", "front-end"),
            ("amplifier_group", "amplifier"),
        ):
            values = sorted(
                {
                    str(row.get(key, "") or "").strip()
                    for row in self.device_profiles
                    if isinstance(row, dict) and str(row.get(key, "") or "").strip()
                }
            )
            if values:
                conflict_groups.append(f"{label}: {', '.join(values[:3])}")
        if conflict_groups:
            hint += " RF conflict groups: " + "; ".join(conflict_groups) + "."
        if self._varac_cluster_mode_enabled():
            hint += " VarAC cluster membership is managed in the dedicated VarAC sections below."
        self.device_profiles_hint_label.setText(hint)

    def _current_device_profile_focus_id(self) -> int | None:
        focused_id = self._ensure_settings_radio_focus_id()
        if focused_id:
            return int(focused_id)
        if not hasattr(self, "device_profiles_table"):
            return None
        table = self.device_profiles_table
        item = table.currentItem()
        if item is not None:
            row_item = table.item(item.row(), 3)
            if row_item is not None:
                try:
                    ident = int(row_item.data(Qt.UserRole) or 0)
                except Exception:
                    ident = 0
                if ident > 0:
                    return ident
        primary = next(
            (
                int(row.get("id", 0) or 0)
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            0,
        )
        if primary > 0:
            return primary
        first = next(
            (
                int(row.get("id", 0) or 0)
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("id", 0) or 0) > 0
            ),
            0,
        )
        return first or None

    def _summary_radio_software_view(self) -> str:
        profile = self._selected_software_radio_profile()
        if not isinstance(profile, dict):
            return "No radio selected"
        return f"{self._profile_display_name(profile)} bundle"

    def _software_radio_profiles(self) -> List[Dict[str, Any]]:
        profiles = [
            dict(row)
            for row in self.device_profiles
            if isinstance(row, dict) and str(row.get("device_class", "") or "").strip().lower() != "observer"
        ]
        if profiles:
            return profiles
        return [dict(row) for row in self.device_profiles if isinstance(row, dict)]

    def _preferred_software_radio_id(self) -> Optional[int]:
        current_id = int(self._software_radio_current_id or 0)
        if current_id > 0 and self._device_profile_by_id(current_id):
            return current_id
        focused_id = self._current_device_profile_focus_id()
        if focused_id and self._device_profile_by_id(int(focused_id)):
            focused_profile = self._device_profile_by_id(int(focused_id))
            if focused_profile and str(focused_profile.get("device_class", "") or "").strip().lower() != "observer":
                return int(focused_id)
        primary_id = next(
            (
                int(row.get("id", 0) or 0)
                for row in self._software_radio_profiles()
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            0,
        )
        if primary_id > 0:
            return primary_id
        first_id = next(
            (
                int(row.get("id", 0) or 0)
                for row in self._software_radio_profiles()
                if isinstance(row, dict) and int(row.get("id", 0) or 0) > 0
            ),
            0,
        )
        return first_id or None

    def _selected_software_radio_profile(self) -> Optional[Dict[str, Any]]:
        radio_id = int(self._software_radio_current_id or 0)
        if radio_id <= 0:
            return None
        return self._device_profile_by_id(radio_id)

    def _radio_software_scope_text(self, profile: Optional[Dict[str, Any]]) -> str:
        if not isinstance(profile, dict):
            return "Editing Radio: --"
        radio_name = self._profile_display_name(profile)
        suffix = []
        if int(profile.get("runtime_primary", 0) or 0) == 1:
            suffix.append("Station Default")
        elif int(profile.get("runtime_active", 0) or 0) == 1:
            suffix.append("Active")
        suffix_txt = f" ({', '.join(suffix)})" if suffix else ""
        return f"Editing Radio: {radio_name}{suffix_txt}. Save Settings applies these software values to this radio bundle."

    def _radio_assignment_scope_text(self, profile: Optional[Dict[str, Any]]) -> str:
        if not isinstance(profile, dict):
            return "Editing Radio: --. Select a radio to review or change that radio's schedule assignment."
        radio_id = int(profile.get("id", 0) or 0)
        assignment = self._effective_assignment_map().get(radio_id, {})
        operating_name = str(assignment.get("operating_profile_name", "") or "").strip() or "Unassigned"
        state = self._assignment_state_label(str(assignment.get("assignment_state", "") or ""))
        return (
            f"Editing Radio: {self._profile_display_name(profile)}. "
            f"Schedule actions apply to this radio's assignment row. Effective schedule: {operating_name} ({state})."
        )

    def _refresh_device_assignment_scope_label(self) -> None:
        if not hasattr(self, "device_assignments_scope_label"):
            return
        profile = self._selected_settings_radio_profile()
        self.device_assignments_scope_label.setText(self._radio_assignment_scope_text(profile))

    def _refresh_software_scope_labels(self) -> None:
        profile = self._selected_software_radio_profile()
        radio_name = self._profile_display_name(profile) if isinstance(profile, dict) else "No radio selected"
        scope_text = self._radio_software_scope_text(profile)
        if hasattr(self, "software_scope_title_label"):
            self.software_scope_title_label.setText(f"{radio_name} Software Bundle")
        if hasattr(self, "software_scope_status_label"):
            if isinstance(profile, dict):
                summary = self._device_software_summary(profile)
                endpoint = self._device_endpoint_summary(profile)
                self.software_scope_status_label.setText(
                    f"Selected radio: {radio_name}. These JS8Call, Fast Light, and VarAC pages now edit this radio's software bundle. "
                    f"Current software summary: {summary}. Endpoint summary: {endpoint}. "
                    "Launch Control and operating status still follow the Station Default compatibility projection."
                )
            else:
                self.software_scope_status_label.setText(
                    "Select a radio profile to view radio-scoped JS8Call, Fast Light, and VarAC settings."
                )
        if hasattr(self, "js8_scope_label"):
            self.js8_scope_label.setText(scope_text)
        if hasattr(self, "fast_light_scope_label"):
            self.fast_light_scope_label.setText(scope_text)
        if hasattr(self, "varac_scope_label"):
            self.varac_scope_label.setText(scope_text)
        if hasattr(self, "custom_tools_scope_label"):
            self.custom_tools_scope_label.setText(
                f"{scope_text} Custom tools are still shared until the radio-scoped custom-tools binding audit is complete."
            )
        if hasattr(self, "launch_control_scope_label"):
            self.launch_control_scope_label.setText(
                f"{scope_text} Launch Control currently follows the Station Default projection while selected-radio launch binding is reviewed."
            )

    def _refresh_radio_context_labels(self) -> None:
        self._refresh_software_scope_labels()
        self._refresh_device_assignment_scope_label()

    def _radio_software_state_from_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        message_paths = {
            "flmsg": str(profile.get("flmsg_message_path", "") or "").strip(),
            "flamp": str(profile.get("flamp_message_path", "") or "").strip(),
            "varac": str(profile.get("varac_incoming_path", "") or "").strip(),
        }
        return {
            "js8_host": str(profile.get("js8_host", "") or "").strip() or "127.0.0.1",
            "js8_port": str(profile.get("js8_port", "") or "2442"),
            "js8_offset_hz": str(profile.get("js8_offset_hz", 0) or 0),
            "js8_directed_path": str(profile.get("js8_directed_path", "") or "").strip(),
            "js8_forms_path": str(profile.get("js8_forms_path", "") or "").strip(),
            "path_js8call": str(profile.get("js8_install_path", "") or "").strip(),
            "path_js8spotter": str(profile.get("spotter_launch_path", "") or "").strip(),
            "path_commstat": str(profile.get("commstat_launch_path", "") or "").strip(),
            "flrig_port": str(profile.get("flrig_port", "") or "12345"),
            "fldigi_host": str(profile.get("fldigi_host", "") or "").strip()
            or str(profile.get("flrig_host", "") or "").strip()
            or "127.0.0.1",
            "fldigi_port": str(profile.get("fldigi_port", "") or "7362"),
            "fldigi_checkin_dir": str(profile.get("fldigi_checkin_dir", "") or "").strip(),
            "fldigi_log_path": str(profile.get("fldigi_log_path", "") or "").strip(),
            "path_flrig": str(profile.get("flrig_path", "") or "").strip(),
            "path_fldigi": str(profile.get("fldigi_path", "") or "").strip(),
            "path_flmsg": str(profile.get("flmsg_path", "") or "").strip(),
            "path_flamp": str(profile.get("flamp_path", "") or "").strip(),
            "message_paths": message_paths,
            "varac_path": str(profile.get("varac_install_path", "") or "").strip(),
            "varac_ini_path": str(profile.get("varac_ini_path", "") or "").strip(),
            "varac_launch_cmd": str(profile.get("launch_cmd", "") or "").strip(),
            "varac_outbox_dir": str(profile.get("varac_outbox_dir", "") or "").strip(),
            "varac_bbs_dir": str(profile.get("varac_bbs_dir", "") or "").strip(),
            "varac_bbs_archive_dir": str(profile.get("varac_bbs_archive_dir", "") or "").strip(),
            "varac_bbs_enabled": bool(int(profile.get("varac_bbs_enabled", 0) or 0) == 1),
            "varac_bbs_limit_access_enabled": bool(int(profile.get("varac_bbs_limit_access_enabled", 0) or 0) == 1),
            "varac_bbs_allowed_callsigns": str(profile.get("varac_bbs_allowed_callsigns", "") or "").strip(),
            "varac_bbs_announce_enabled": bool(int(profile.get("varac_bbs_announce_enabled", 0) or 0) == 1),
            "varac_bbs_auto_archive_enabled": bool(int(profile.get("varac_bbs_auto_archive_enabled", 0) or 0) == 1),
            "varac_bbs_auto_archive_days": str(profile.get("varac_bbs_auto_archive_days", 14) or 14),
            "varac_guard_allow_bbs_allowed_callsigns": bool(
                int(profile.get("varac_guard_allow_bbs_allowed_callsigns", 1) or 1) == 1
            ),
            "varac_guard_allow_operator_trusted": bool(
                int(profile.get("varac_guard_allow_operator_trusted", 1) or 1) == 1
            ),
            "varac_bbs_vault_enabled": bool(int(profile.get("varac_bbs_vault_enabled", 0) or 0) == 1),
            "varac_bbs_vault_managed_root": str(profile.get("varac_bbs_vault_managed_root", "") or "").strip(),
            "varac_bbs_vault_default_location_id": str(
                profile.get("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID) or DEFAULT_LOCATION_ID
            ).strip()
            or DEFAULT_LOCATION_ID,
            "varac_bbs_vault_global_code_policy": str(
                profile.get("varac_bbs_vault_global_code_policy", DEFAULT_GLOBAL_CODE_POLICY) or DEFAULT_GLOBAL_CODE_POLICY
            ).strip()
            or DEFAULT_GLOBAL_CODE_POLICY,
            "varac_bbs_vault_trigger_mode": str(
                profile.get("varac_bbs_vault_trigger_mode", DEFAULT_TRIGGER_MODE) or DEFAULT_TRIGGER_MODE
            ).strip()
            or DEFAULT_TRIGGER_MODE,
            "varac_bbs_vault_return_mode": str(
                profile.get("varac_bbs_vault_return_mode", DEFAULT_RETURN_MODE) or DEFAULT_RETURN_MODE
            ).strip()
            or DEFAULT_RETURN_MODE,
            "varac_bbs_vault_failed_attempt_limit": str(
                profile.get("varac_bbs_vault_failed_attempt_limit", DEFAULT_FAILED_ATTEMPT_LIMIT)
                or DEFAULT_FAILED_ATTEMPT_LIMIT
            ),
            "varac_bbs_vault_cooldown_seconds": str(
                profile.get("varac_bbs_vault_cooldown_seconds", DEFAULT_COOLDOWN_SECONDS) or DEFAULT_COOLDOWN_SECONDS
            ),
            "varac_bbs_vault_idle_timeout_seconds": str(
                profile.get("varac_bbs_vault_idle_timeout_seconds", DEFAULT_IDLE_TIMEOUT_SECONDS)
                or DEFAULT_IDLE_TIMEOUT_SECONDS
            ),
            "varac_bbs_vault_flamp_enabled": bool(int(profile.get("varac_bbs_vault_flamp_enabled", 0) or 0) == 1),
            "varac_bbs_vault_flamp_relay_dir": str(profile.get("varac_bbs_vault_flamp_relay_dir", "") or "").strip(),
            "varac_bbs_vault_flamp_listing_max_age_days": int(
                profile.get("varac_bbs_vault_flamp_listing_max_age_days", DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS)
                or DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS
            ),
            "varac_bbs_vault_locations_v1": profile.get("varac_bbs_vault_locations_v1", []) or [],
            "varac_bbs_vault_runtime_state_v1": _coerce_json_mapping(
                profile.get("varac_bbs_vault_runtime_state_v1", {})
            ),
            "varac_bbs_vault_last_summary": str(profile.get("varac_bbs_vault_last_summary", "") or "").strip(),
        }

    def _capture_radio_software_view_state(self) -> Dict[str, Any]:
        return {
            "js8_host": self.js8_host_edit.text().strip() or "127.0.0.1",
            "js8_port": self.js8_port_edit.text().strip() or "2442",
            "js8_offset_hz": self.js8_offset_edit.text().strip() or "0",
            "js8_directed_path": self.js8_directed_edit.text().strip(),
            "js8_forms_path": self.js8_forms_edit.text().strip(),
            "path_js8call": self.js8call_path_edit.text().strip(),
            "path_js8spotter": self.js8spotter_path_edit.text().strip(),
            "path_commstat": self.commstat_path_edit.text().strip(),
            "flrig_port": self.flrig_port_edit.text().strip() or "12345",
            "fldigi_host": self.fldigi_host_edit.text().strip() or "127.0.0.1",
            "fldigi_port": self.fldigi_port_edit.text().strip() or "7362",
            "fldigi_checkin_dir": self.fldigi_checkin_dir_edit.text().strip(),
            "fldigi_log_path": self.fldigi_log_path_edit.text().strip(),
            "path_flrig": self.path_edits.get("FLRig").text().strip() if self.path_edits.get("FLRig") else "",
            "path_fldigi": self.path_edits.get("FLDigi").text().strip() if self.path_edits.get("FLDigi") else "",
            "path_flmsg": self.path_edits.get("FLMsg").text().strip() if self.path_edits.get("FLMsg") else "",
            "path_flamp": self.path_edits.get("FLAmp").text().strip() if self.path_edits.get("FLAmp") else "",
            "message_paths": {
                origin: edit.text().strip()
                for origin, edit in self.msg_paths_edits.items()
            },
            "varac_path": self.varac_path_edit.text().strip(),
            "varac_ini_path": self.varac_ini_path_edit.text().strip(),
            "varac_launch_cmd": self.varac_launch_cmd_edit.text().strip(),
            "varac_outbox_dir": self.varac_outbox_dir_edit.text().strip(),
            "varac_bbs_dir": self.varac_bbs_dir_edit.text().strip(),
            "varac_bbs_archive_dir": self.varac_bbs_archive_dir_edit.text().strip(),
            "varac_bbs_enabled": bool(self.varac_bbs_enabled_chk.isChecked()),
            "varac_bbs_limit_access_enabled": bool(self.varac_bbs_limit_access_chk.isChecked()),
            "varac_bbs_allowed_callsigns": self._varac_bbs_selected_callsigns_text(),
            "varac_bbs_announce_enabled": bool(self.varac_bbs_announce_chk.isChecked()),
            "varac_bbs_auto_archive_enabled": bool(self.varac_bbs_auto_archive_chk.isChecked()),
            "varac_bbs_auto_archive_days": self.varac_bbs_archive_days_combo.currentText().strip() or "14",
            "varac_guard_allow_bbs_allowed_callsigns": bool(
                self.varac_guard_allow_bbs_chk.isChecked() if hasattr(self, "varac_guard_allow_bbs_chk") else True
            ),
            "varac_guard_allow_operator_trusted": bool(
                self.varac_guard_allow_trusted_chk.isChecked() if hasattr(self, "varac_guard_allow_trusted_chk") else True
            ),
            "varac_bbs_vault_enabled": bool(self.varac_bbs_vault_enabled_chk_main.isChecked()),
            "varac_bbs_vault_managed_root": self._computed_varac_bbs_vault_default_root(),
            "varac_bbs_vault_default_location_id": (
                str(self.varac_bbs_vault_default_location_combo.currentData() or "").strip() or DEFAULT_LOCATION_ID
            ),
            "varac_bbs_vault_global_code_policy": (
                self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
                else DEFAULT_GLOBAL_CODE_POLICY
            ),
            "varac_bbs_vault_trigger_mode": self.varac_bbs_vault_trigger_mode_combo.currentText().strip()
            or DEFAULT_TRIGGER_MODE,
            "varac_bbs_vault_return_mode": self.varac_bbs_vault_return_mode_combo.currentText().strip()
            or DEFAULT_RETURN_MODE,
            "varac_bbs_vault_failed_attempt_limit": str(
                self.varac_bbs_vault_failed_attempt_limit_combo.currentData() or DEFAULT_FAILED_ATTEMPT_LIMIT
            ),
            "varac_bbs_vault_cooldown_seconds": str(
                self.varac_bbs_vault_cooldown_combo.currentData() or DEFAULT_COOLDOWN_SECONDS
            ),
            "varac_bbs_vault_idle_timeout_seconds": str(
                self.varac_bbs_vault_idle_timeout_combo.currentData() or DEFAULT_IDLE_TIMEOUT_SECONDS
            ),
            "varac_bbs_vault_flamp_enabled": bool(
                self.varac_bbs_vault_flamp_enabled_chk.isChecked()
                if hasattr(self, "varac_bbs_vault_flamp_enabled_chk")
                else False
            ),
            "varac_bbs_vault_flamp_relay_dir": (
                self.varac_bbs_vault_flamp_relay_dir_edit.text().strip()
                if hasattr(self, "varac_bbs_vault_flamp_relay_dir_edit")
                else ""
            ),
            "varac_bbs_vault_flamp_listing_max_age_days": int(
                self.varac_bbs_vault_flamp_listing_age_combo.currentData()
                if hasattr(self, "varac_bbs_vault_flamp_listing_age_combo")
                and self.varac_bbs_vault_flamp_listing_age_combo.currentData() is not None
                else DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS
            ),
            "varac_bbs_vault_locations_v1": list(self._varac_bbs_vault_locations_cache),
            "varac_bbs_vault_runtime_state_v1": dict(self._varac_bbs_vault_runtime_state_cache),
            "varac_bbs_vault_last_summary": str(self._varac_bbs_vault_last_summary_cache or "").strip(),
        }

    def _apply_radio_software_view_state(self, state: Dict[str, Any]) -> None:
        previous_loading = self._loading_settings
        self._loading_settings = True
        try:
            self.js8_host_edit.setText(str(state.get("js8_host", "") or "").strip() or "127.0.0.1")
            self.js8_port_edit.setText(str(state.get("js8_port", "") or "2442"))
            self.js8_offset_edit.setText(str(coerce_js8_offset_hz(state.get("js8_offset_hz", ""))))
            self.js8_directed_edit.setText(str(state.get("js8_directed_path", "") or ""))
            self.js8_forms_edit.setText(str(state.get("js8_forms_path", "") or ""))
            self._refresh_spotter_form_mapper()
            self.js8call_path_edit.setText(str(state.get("path_js8call", "") or "").strip())
            self.js8spotter_path_edit.setText(str(state.get("path_js8spotter", "") or "").strip())
            self.commstat_path_edit.setText(str(state.get("path_commstat", "") or "").strip())
            self.flrig_port_edit.setText(str(state.get("flrig_port", "") or "12345"))
            self.fldigi_host_edit.setText(str(state.get("fldigi_host", "") or "").strip() or "127.0.0.1")
            self.fldigi_port_edit.setText(str(state.get("fldigi_port", "") or "7362"))
            self.fldigi_checkin_dir_edit.setText(str(state.get("fldigi_checkin_dir", "") or "").strip())
            self.fldigi_log_path_edit.setText(str(state.get("fldigi_log_path", "") or "").strip())
            if self.path_edits.get("FLRig"):
                self.path_edits["FLRig"].setText(str(state.get("path_flrig", "") or "").strip())
            if self.path_edits.get("FLDigi"):
                self.path_edits["FLDigi"].setText(str(state.get("path_fldigi", "") or "").strip())
            if self.path_edits.get("FLMsg"):
                self.path_edits["FLMsg"].setText(str(state.get("path_flmsg", "") or "").strip())
            if self.path_edits.get("FLAmp"):
                self.path_edits["FLAmp"].setText(str(state.get("path_flamp", "") or "").strip())
            message_paths = state.get("message_paths", {}) or {}
            for origin, edit in self.msg_paths_edits.items():
                edit.setText(str(message_paths.get(origin, "") or "").strip())
            self.varac_path_edit.setText(str(state.get("varac_path", "") or "").strip())
            self.varac_ini_path_edit.setText(str(state.get("varac_ini_path", "") or "").strip())
            self.varac_launch_cmd_edit.setText(str(state.get("varac_launch_cmd", "") or "").strip())
            self.varac_outbox_dir_edit.setText(str(state.get("varac_outbox_dir", "") or "").strip())
            self.varac_bbs_dir_edit.setText(str(state.get("varac_bbs_dir", "") or "").strip())
            self.varac_bbs_archive_dir_edit.setText(str(state.get("varac_bbs_archive_dir", "") or "").strip())
            self.varac_bbs_enabled_chk.setChecked(bool(state.get("varac_bbs_enabled", False)))
            self.varac_bbs_limit_access_chk.setChecked(bool(state.get("varac_bbs_limit_access_enabled", False)))
            self._set_varac_bbs_allowed_callsigns(state.get("varac_bbs_allowed_callsigns", ""))
            self.varac_bbs_announce_chk.setChecked(bool(state.get("varac_bbs_announce_enabled", False)))
            self.varac_bbs_auto_archive_chk.setChecked(bool(state.get("varac_bbs_auto_archive_enabled", False)))
            self.varac_bbs_archive_days_combo.setCurrentText(str(state.get("varac_bbs_auto_archive_days", "14") or "14"))
            if hasattr(self, "varac_guard_allow_bbs_chk"):
                self.varac_guard_allow_bbs_chk.setChecked(bool(state.get("varac_guard_allow_bbs_allowed_callsigns", True)))
            if hasattr(self, "varac_guard_allow_trusted_chk"):
                self.varac_guard_allow_trusted_chk.setChecked(bool(state.get("varac_guard_allow_operator_trusted", True)))
            self.varac_bbs_vault_enabled_chk_main.setChecked(bool(state.get("varac_bbs_vault_enabled", False)))
            root_txt = compute_default_managed_root(self.varac_bbs_dir_edit.text().strip())
            self._set_varac_bbs_vault_root_text(root_txt)
            code_policy = str(state.get("varac_bbs_vault_global_code_policy", DEFAULT_GLOBAL_CODE_POLICY) or DEFAULT_GLOBAL_CODE_POLICY).strip()
            idx = self.varac_bbs_vault_global_code_policy_combo.findText(code_policy)
            self.varac_bbs_vault_global_code_policy_combo.setCurrentIndex(idx if idx >= 0 else 1)
            trigger_mode = str(state.get("varac_bbs_vault_trigger_mode", DEFAULT_TRIGGER_MODE) or DEFAULT_TRIGGER_MODE).strip()
            if trigger_mode not in {"VarAC session commands", "Command prefix", "Exact code only"}:
                trigger_mode = DEFAULT_TRIGGER_MODE
            self.varac_bbs_vault_trigger_mode_combo.setCurrentText(trigger_mode)
            return_mode = str(state.get("varac_bbs_vault_return_mode", DEFAULT_RETURN_MODE) or DEFAULT_RETURN_MODE).strip()
            if return_mode not in {"On disconnect", "After inactivity timeout", "Manual operator reset only"}:
                return_mode = DEFAULT_RETURN_MODE
            self.varac_bbs_vault_return_mode_combo.setCurrentText(return_mode)
            timeout_seconds = int(state.get("varac_bbs_vault_idle_timeout_seconds", DEFAULT_IDLE_TIMEOUT_SECONDS) or DEFAULT_IDLE_TIMEOUT_SECONDS)
            idx = self.varac_bbs_vault_idle_timeout_combo.findData(timeout_seconds)
            if idx < 0:
                idx = self.varac_bbs_vault_idle_timeout_combo.findData(DEFAULT_IDLE_TIMEOUT_SECONDS)
            if idx >= 0:
                self.varac_bbs_vault_idle_timeout_combo.setCurrentIndex(idx)
            attempt_limit = int(state.get("varac_bbs_vault_failed_attempt_limit", DEFAULT_FAILED_ATTEMPT_LIMIT) or DEFAULT_FAILED_ATTEMPT_LIMIT)
            idx = self.varac_bbs_vault_failed_attempt_limit_combo.findData(attempt_limit)
            if idx < 0:
                idx = self.varac_bbs_vault_failed_attempt_limit_combo.findData(DEFAULT_FAILED_ATTEMPT_LIMIT)
            if idx >= 0:
                self.varac_bbs_vault_failed_attempt_limit_combo.setCurrentIndex(idx)
            cooldown_seconds = int(state.get("varac_bbs_vault_cooldown_seconds", DEFAULT_COOLDOWN_SECONDS) or DEFAULT_COOLDOWN_SECONDS)
            idx = self.varac_bbs_vault_cooldown_combo.findData(cooldown_seconds)
            if idx < 0:
                idx = self.varac_bbs_vault_cooldown_combo.findData(DEFAULT_COOLDOWN_SECONDS)
            if idx >= 0:
                self.varac_bbs_vault_cooldown_combo.setCurrentIndex(idx)
            if hasattr(self, "varac_bbs_vault_flamp_enabled_chk"):
                self.varac_bbs_vault_flamp_enabled_chk.setChecked(bool(state.get("varac_bbs_vault_flamp_enabled", False)))
            if hasattr(self, "varac_bbs_vault_flamp_relay_dir_edit"):
                self.varac_bbs_vault_flamp_relay_dir_edit.setText(str(state.get("varac_bbs_vault_flamp_relay_dir", "") or "").strip())
            if hasattr(self, "varac_bbs_vault_flamp_listing_age_combo"):
                age_days = int(
                    state.get("varac_bbs_vault_flamp_listing_max_age_days", DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS)
                    or DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS
                )
                idx = self.varac_bbs_vault_flamp_listing_age_combo.findData(age_days)
                if idx < 0:
                    idx = self.varac_bbs_vault_flamp_listing_age_combo.findData(DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS)
                if idx >= 0:
                    self.varac_bbs_vault_flamp_listing_age_combo.setCurrentIndex(idx)
            self._maybe_autofill_varac_bbs_vault_flamp_relay_dir()
            self._set_varac_bbs_vault_locations(state.get("varac_bbs_vault_locations_v1", []))
            default_id = str(state.get("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID) or DEFAULT_LOCATION_ID).strip()
            idx = self.varac_bbs_vault_default_location_combo.findData(default_id)
            if idx < 0 and self.varac_bbs_vault_default_location_combo.count() > 0:
                idx = 0
            if idx >= 0:
                self.varac_bbs_vault_default_location_combo.setCurrentIndex(idx)
            self._varac_bbs_vault_runtime_state_cache = _coerce_json_mapping(
                state.get("varac_bbs_vault_runtime_state_v1", {})
            )
            self._varac_bbs_vault_last_summary_cache = str(state.get("varac_bbs_vault_last_summary", "") or "").strip()
            self._refresh_varac_bbs_vault_flamp_hint()
        finally:
            self._loading_settings = previous_loading
        self._refresh_fldigi_checkin_file_labels()
        self._refresh_varac_bbs_vault_status_label()
        self._sync_varac_bbs_vault_root_from_bbs_dir(force=False)
        self._refresh_section_titles()

    def _stash_current_software_radio_state(self) -> None:
        radio_id = int(self._software_radio_current_id or 0)
        if radio_id <= 0:
            return
        self._software_radio_drafts[radio_id] = self._capture_radio_software_view_state()

    def _refresh_software_radio_selector(self, *, preserve_current: bool = True) -> None:
        if not hasattr(self, "software_radio_combo"):
            return
        preferred_id = self._preferred_software_radio_id() if preserve_current else None
        profiles = self._software_radio_profiles()
        self._software_radio_combo_loading = True
        try:
            self.software_radio_combo.clear()
            for profile in profiles:
                radio_id = int(profile.get("id", 0) or 0)
                if radio_id <= 0:
                    continue
                label = self._profile_display_name(profile)
                if int(profile.get("runtime_primary", 0) or 0) == 1:
                    label += " [Default]"
                elif int(profile.get("runtime_active", 0) or 0) == 1:
                    label += " [Active]"
                self.software_radio_combo.addItem(label, radio_id)
            if self.software_radio_combo.count() > 0:
                target_id = int(preferred_id or 0)
                index = 0
                if target_id > 0:
                    found = self.software_radio_combo.findData(target_id)
                    if found >= 0:
                        index = found
                self.software_radio_combo.setCurrentIndex(index)
                self._software_radio_current_id = int(self.software_radio_combo.currentData() or 0)
            else:
                self._software_radio_current_id = None
        finally:
            self._software_radio_combo_loading = False
        self._load_selected_software_radio_state()

    def _load_selected_software_radio_state(self) -> None:
        radio_id = int(self._software_radio_current_id or 0)
        profile = self._device_profile_by_id(radio_id) if radio_id > 0 else None
        if radio_id > 0 and radio_id in self._software_radio_drafts:
            state = dict(self._software_radio_drafts[radio_id])
        elif isinstance(profile, dict):
            state = self._radio_software_state_from_profile(profile)
        else:
            state = self._radio_software_state_from_profile({})
        self._apply_radio_software_view_state(state)
        self._refresh_software_scope_labels()

    def _on_software_radio_changed(self) -> None:
        if self._software_radio_combo_loading or not hasattr(self, "software_radio_combo"):
            return
        previous_id = int(self._software_radio_current_id or 0)
        new_id = int(self.software_radio_combo.currentData() or 0)
        if previous_id > 0 and previous_id != new_id:
            self._stash_current_software_radio_state()
        self._software_radio_current_id = new_id or None
        self._load_selected_software_radio_state()

    def _sync_software_radio_to_device_focus(self) -> None:
        if not hasattr(self, "software_radio_combo") or self._software_radio_combo_loading:
            return
        focused_id = self._current_device_profile_focus_id()
        profile = self._device_profile_by_id(int(focused_id or 0)) if focused_id else None
        if not profile or str(profile.get("device_class", "") or "").strip().lower() == "observer":
            return
        target_index = self.software_radio_combo.findData(int(focused_id or 0))
        if target_index < 0 or target_index == self.software_radio_combo.currentIndex():
            return
        self.software_radio_combo.setCurrentIndex(target_index)

    def _sync_schedule_views_to_device_focus(self) -> None:
        focused_id = int(self._current_device_profile_focus_id() or 0)
        if focused_id <= 0:
            return
        if hasattr(self, "device_assignments_table"):
            for row in range(self.device_assignments_table.rowCount()):
                row_item = self.device_assignments_table.item(row, 3)
                if row_item is None:
                    continue
                try:
                    device_id = int(row_item.data(Qt.UserRole) or 0)
                except Exception:
                    device_id = 0
                if device_id == focused_id:
                    self.device_assignments_table.setCurrentCell(row, 3)
                    break
        assignment_map = self._effective_assignment_map()
        assignment = assignment_map.get(focused_id, {})
        operating_profile_id = int(assignment.get("operating_profile_id", 0) or 0)
        if operating_profile_id <= 0 or not hasattr(self, "operating_profiles_table"):
            return
        for row in range(self.operating_profiles_table.rowCount()):
            row_item = self.operating_profiles_table.item(row, 2)
            if row_item is None:
                continue
            try:
                profile_id = int(row_item.data(Qt.UserRole) or 0)
            except Exception:
                profile_id = 0
            if profile_id == operating_profile_id:
                self.operating_profiles_table.setCurrentCell(row, 2)
                break

    def _runtime_primary_device_profile_id(self) -> Optional[int]:
        primary_id = next(
            (
                int(row.get("id", 0) or 0)
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            0,
        )
        return primary_id or None

    def _save_radio_software_bundle(self, profile: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(profile)
        radio_name = self._profile_display_name(profile)
        message_paths = state.get("message_paths", {}) or {}

        def _txt(key: str, default: str = "") -> str:
            return str(state.get(key, default) or default).strip()

        def _num(key: str, default: int) -> int:
            try:
                return int(str(state.get(key, default) or default).strip() or default)
            except Exception:
                return default

        existing_js8_id = int(profile.get("js8_instance_id", 0) or 0)
        js8_needed = any(
            [
                self._radio_software_enabled(profile, "js8call"),
                self._radio_software_enabled(profile, "js8spotter"),
                self._radio_software_enabled(profile, "commstat"),
                _txt("js8_host"),
                _txt("js8_port"),
                _txt("path_js8call"),
                _txt("js8_directed_path"),
                _txt("js8_forms_path"),
                _txt("path_js8spotter"),
                _txt("path_commstat"),
                existing_js8_id > 0,
            ]
        )
        if js8_needed:
            js8_saved = self.multi_radio_store.save_js8_instance(
                {
                    "id": existing_js8_id or None,
                    "name": f"{radio_name} JS8",
                    "host": _txt("js8_host", "127.0.0.1") or "127.0.0.1",
                    "port": _num("js8_port", 2442),
                    "offset_hz": _num("js8_offset_hz", coerce_js8_offset_hz(0)),
                    "profile_path": str(profile.get("js8_profile_path", "") or "").strip(),
                    "directed_path": _txt("js8_directed_path"),
                    "forms_path": _txt("js8_forms_path"),
                    "install_path": _txt("path_js8call"),
                    "spotter_launch_path": _txt("path_js8spotter"),
                    "commstat_launch_path": _txt("path_commstat"),
                }
            )
            payload["js8_instance_id"] = int(js8_saved.get("id", 0) or 0)

        existing_fast_id = int(profile.get("fast_light_config_id", 0) or 0)
        fast_needed = any(
            [
                self._radio_software_enabled(profile, "flrig"),
                self._radio_software_enabled(profile, "fldigi"),
                _txt("path_flrig"),
                _txt("path_fldigi"),
                _txt("fldigi_host"),
                _txt("fldigi_port"),
                _txt("fldigi_log_path"),
                _txt("fldigi_checkin_dir"),
                _txt("flrig_port"),
                existing_fast_id > 0,
            ]
        )
        if fast_needed:
            fast_saved = self.multi_radio_store.save_fast_light_config(
                {
                    "id": existing_fast_id or None,
                    "name": f"{radio_name} Fast Light",
                    "flrig_path": _txt("path_flrig"),
                    "flrig_host": str(profile.get("flrig_host", "") or "").strip() or "127.0.0.1",
                    "flrig_port": _num("flrig_port", 12345),
                    "fldigi_path": _txt("path_fldigi"),
                    "fldigi_host": _txt("fldigi_host", "127.0.0.1") or "127.0.0.1",
                    "fldigi_port": _num("fldigi_port", 7362),
                    "fldigi_log_path": _txt("fldigi_log_path"),
                    "fldigi_checkin_dir": _txt("fldigi_checkin_dir"),
                }
            )
            payload["fast_light_config_id"] = int(fast_saved.get("id", 0) or 0)

        existing_varac_id = int(profile.get("varac_node_id", 0) or 0)
        varac_install_path = _txt("varac_path")
        varac_db_path = (
            str(Path(varac_install_path) / "VarAC.db")
            if varac_install_path
            else str(profile.get("varac_db_path", "") or "").strip()
        )
        varac_needed = any(
            [
                self._radio_software_enabled(profile, "varac"),
                varac_install_path,
                _txt("varac_launch_cmd"),
                str(message_paths.get("varac", "") or "").strip(),
                existing_varac_id > 0,
            ]
        )
        if varac_needed:
            varac_saved = self.multi_radio_store.save_varac_node(
                {
                    "id": existing_varac_id or None,
                    "name": f"{radio_name} VarAC",
                    "install_path": varac_install_path,
                    "db_path": varac_db_path,
                    "ini_path": str(profile.get("varac_ini_path", "") or "").strip(),
                    "incoming_path": str(message_paths.get("varac", "") or "").strip(),
                    "launch_cmd": _txt("varac_launch_cmd"),
                }
            )
            payload["varac_node_id"] = int(varac_saved.get("id", 0) or 0)

        payload.update(
            {
                "js8_host": _txt("js8_host", "127.0.0.1") or "127.0.0.1",
                "js8_port": _num("js8_port", 2442),
                "js8_directed_path": _txt("js8_directed_path"),
                "js8_forms_path": _txt("js8_forms_path"),
                "flrig_port": _num("flrig_port", 12345),
                "fldigi_host": _txt("fldigi_host", "127.0.0.1") or "127.0.0.1",
                "fldigi_port": _num("fldigi_port", 7362),
                "fldigi_log_path": _txt("fldigi_log_path"),
                "fldigi_checkin_dir": _txt("fldigi_checkin_dir"),
                "flmsg_path": _txt("path_flmsg"),
                "flmsg_message_path": str(message_paths.get("flmsg", "") or "").strip(),
                "flamp_path": _txt("path_flamp"),
                "flamp_message_path": str(message_paths.get("flamp", "") or "").strip(),
                "varac_install_path": varac_install_path,
                "varac_db_path": varac_db_path,
                "varac_outbox_dir": _txt("varac_outbox_dir"),
                "varac_bbs_dir": _txt("varac_bbs_dir"),
                "varac_bbs_archive_dir": _txt("varac_bbs_archive_dir"),
                "varac_bbs_enabled": bool(state.get("varac_bbs_enabled", False)),
                "varac_bbs_limit_access_enabled": bool(state.get("varac_bbs_limit_access_enabled", False)),
                "varac_bbs_allowed_callsigns": _txt("varac_bbs_allowed_callsigns"),
                "varac_bbs_announce_enabled": bool(state.get("varac_bbs_announce_enabled", False)),
                "varac_bbs_auto_archive_enabled": bool(state.get("varac_bbs_auto_archive_enabled", False)),
                "varac_bbs_auto_archive_days": _num("varac_bbs_auto_archive_days", 14),
                "varac_guard_allow_bbs_allowed_callsigns": bool(state.get("varac_guard_allow_bbs_allowed_callsigns", True)),
                "varac_guard_allow_operator_trusted": bool(state.get("varac_guard_allow_operator_trusted", True)),
                "varac_bbs_vault_enabled": bool(state.get("varac_bbs_vault_enabled", False)),
                "varac_bbs_vault_managed_root": _txt("varac_bbs_vault_managed_root"),
                "varac_bbs_vault_default_location_id": _txt("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID)
                or DEFAULT_LOCATION_ID,
                "varac_bbs_vault_global_code_policy": _txt("varac_bbs_vault_global_code_policy", DEFAULT_GLOBAL_CODE_POLICY)
                or DEFAULT_GLOBAL_CODE_POLICY,
                "varac_bbs_vault_trigger_mode": _txt("varac_bbs_vault_trigger_mode", DEFAULT_TRIGGER_MODE)
                or DEFAULT_TRIGGER_MODE,
                "varac_bbs_vault_return_mode": _txt("varac_bbs_vault_return_mode", DEFAULT_RETURN_MODE)
                or DEFAULT_RETURN_MODE,
                "varac_bbs_vault_failed_attempt_limit": _num(
                    "varac_bbs_vault_failed_attempt_limit",
                    DEFAULT_FAILED_ATTEMPT_LIMIT,
                ),
                "varac_bbs_vault_failed_attempt_window_seconds": DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS,
                "varac_bbs_vault_cooldown_seconds": _num(
                    "varac_bbs_vault_cooldown_seconds",
                    DEFAULT_COOLDOWN_SECONDS,
                ),
                "varac_bbs_vault_idle_timeout_seconds": _num(
                    "varac_bbs_vault_idle_timeout_seconds",
                    DEFAULT_IDLE_TIMEOUT_SECONDS,
                ),
                "varac_bbs_vault_flamp_enabled": bool(state.get("varac_bbs_vault_flamp_enabled", False)),
                "varac_bbs_vault_flamp_relay_dir": _txt("varac_bbs_vault_flamp_relay_dir"),
                "varac_bbs_vault_flamp_listing_max_age_days": _num(
                    "varac_bbs_vault_flamp_listing_max_age_days",
                    DEFAULT_FLAMP_LISTING_MAX_AGE_DAYS,
                ),
                "varac_bbs_vault_locations_v1": state.get("varac_bbs_vault_locations_v1", []) or [],
                "varac_bbs_vault_runtime_state_v1": state.get("varac_bbs_vault_runtime_state_v1", {}) or {},
                "varac_bbs_vault_last_summary": _txt("varac_bbs_vault_last_summary"),
                "launch_cmd": _txt("varac_launch_cmd"),
                "use_flrig": bool(int(profile.get("use_flrig", 0) or 0))
                or bool(_txt("path_flrig"))
                or str(profile.get("control_backend", "") or "").strip().lower() == "flrig",
                "use_fldigi": bool(int(profile.get("use_fldigi", 0) or 0))
                or bool(_txt("path_fldigi"))
                or bool(_txt("fldigi_log_path"))
                or bool(_txt("fldigi_checkin_dir")),
                "use_flmsg": bool(int(profile.get("use_flmsg", 0) or 0))
                or bool(_txt("path_flmsg"))
                or bool(str(message_paths.get("flmsg", "") or "").strip()),
                "use_flamp": bool(int(profile.get("use_flamp", 0) or 0))
                or bool(_txt("path_flamp"))
                or bool(str(message_paths.get("flamp", "") or "").strip()),
                "use_js8call": bool(int(profile.get("use_js8call", 0) or 0))
                or bool(_txt("path_js8call"))
                or str(profile.get("control_backend", "") or "").strip().lower() == "js8call",
                "use_js8spotter": bool(int(profile.get("use_js8spotter", 0) or 0)) or bool(_txt("path_js8spotter")),
                "use_commstat": bool(int(profile.get("use_commstat", 0) or 0)) or bool(_txt("path_commstat")),
                "use_varac": bool(int(profile.get("use_varac", 0) or 0))
                or bool(varac_install_path)
                or bool(_txt("varac_launch_cmd"))
                or bool(str(message_paths.get("varac", "") or "").strip()),
            }
        )
        return self.multi_radio_store.save_device_profile(payload)

    def _persist_staged_radio_software_bundles(self) -> bool:
        self._stash_current_software_radio_state()
        if not self._software_radio_drafts:
            return True
        try:
            for radio_id, state in list(self._software_radio_drafts.items()):
                profile = self._device_profile_by_id(int(radio_id))
                if not isinstance(profile, dict):
                    continue
                self._save_radio_software_bundle(profile, dict(state))
            primary_id = self._runtime_primary_device_profile_id()
            if primary_id:
                self.multi_radio_store.sync_runtime_active_device_to_legacy_settings(int(primary_id))
        except ValueError as exc:
            QMessageBox.warning(self, "Radio Software View", str(exc))
            return False
        except Exception:
            log.exception("Failed to persist radio-scoped software settings.")
            QMessageBox.warning(
                self,
                "Radio Software View",
                "Unable to save the selected radio software settings.",
            )
            return False
        self._software_radio_drafts.clear()
        return True

    def set_multi_rig_runtime_status(self, status: MultiRigRuntimeStatus | None) -> None:
        self._multi_rig_runtime_status = status
        self._refresh_multi_rig_status_card()

    def _current_multi_rig_runtime_status(self) -> MultiRigRuntimeStatus:
        if self._multi_rig_runtime_status is not None:
            return self._multi_rig_runtime_status
        status = build_multi_rig_runtime_status(
            self.multi_radio_store,
            settings_values=dict(self.settings.all()),
        )
        self._multi_rig_runtime_status = status
        return status

    def _refresh_cached_multi_rig_runtime_status(self) -> MultiRigRuntimeStatus:
        status = build_multi_rig_runtime_status(
            self.multi_radio_store,
            settings_values=dict(self.settings.all()),
        )
        self._multi_rig_runtime_status = status
        self._refresh_multi_rig_status_card()
        return status

    def _radio_name_by_id(self, profile_id: int | None) -> str:
        if not profile_id:
            return ""
        for row in self.device_profiles:
            try:
                if int(row.get("id", 0) or 0) == int(profile_id):
                    return str(row.get("name", "") or "").strip()
            except Exception:
                continue
        try:
            profile = self.multi_radio_store.get_device_profile(int(profile_id))
        except Exception:
            profile = None
        return str((profile or {}).get("name", "") or "").strip()

    def _multi_rig_status_text(self, status: MultiRigRuntimeStatus) -> tuple[str, str, str, str]:
        mode = status.startup_mode
        primary_name = self._radio_name_by_id(status.primary_device_profile_id) or "Primary radio"
        active_count = len(status.active_device_profile_ids)
        if mode == STARTUP_FRESH_DEFAULT_READY:
            return (
                "Multi-Rig Setup",
                "No radios are configured yet.",
                "Use Add Radio or Configure Automatically to set up the first radio.",
                "success",
            )
        if mode == STARTUP_MIGRATED:
            detail = f"Primary radio: {primary_name}. Active radios: {active_count}."
            if active_count > 1:
                detail += " Messages and Map use all active radios by default."
            return (f"{primary_name} - Status", "Multi-Rig is ready.", detail, "success")
        if mode == STARTUP_DEFERRED:
            return (
                "Multi-Rig Setup",
                "Multi-Rig setup is paused.",
                "FIO is still using your current station setup. You can return to Multi-Rig setup from here any time.",
                "info",
            )
        if mode == STARTUP_MIGRATION_ERROR:
            warning = " ".join(status.warnings[:2]) if status.warnings else ""
            detail = "Your current settings were left unchanged. You can keep using FIO while this is reviewed."
            if warning:
                detail = f"{detail} Latest note: {warning}"
            return ("Multi-Rig Setup", "FIO could not prepare Multi-Rig setup.", detail, "warning")
        return (
            "Multi-Rig Setup",
            "FIO is using your current station setup.",
            "Multi-Rig setup is available when you are ready. Your current settings will be left unchanged until you confirm setup.",
            "info",
        )

    def _style_multi_rig_status_card(self, level: str) -> None:
        if not hasattr(self, "multi_rig_status_card"):
            return
        theme = resolve_theme(self.settings)
        level_key = (level or "info").strip().lower()
        color_map = {
            "success": theme.get("success", "#2E7D32"),
            "warning": theme.get("warning", "#C99700"),
            "danger": theme.get("danger", "#C62828"),
            "info": theme.get("info", theme.get("accent", "#1565C0")),
        }
        accent = QColor(color_map.get(level_key, color_map["info"]))
        bg = QColor(accent)
        bg.setAlpha(18 if level_key in {"info", "success"} else 28)
        border = QColor(accent)
        border.setAlpha(105 if level_key in {"info", "success"} else 150)
        text_color = theme.get("text", "#1C1F21")
        muted_color = theme.get("text_muted", text_color)
        self.multi_rig_status_card.setStyleSheet(
            "QFrame {"
            f" background-color: {bg.name(QColor.HexArgb)};"
            f" border: 1px solid {border.name(QColor.HexArgb)};"
            " border-radius: 8px;"
            "}"
            f" QLabel {{ color: {text_color}; border: none; background: transparent; }}"
            f" QLabel#multiRigStatusDetail {{ color: {muted_color}; }}"
            f" QLabel#multiRigAutoconfigPreview {{ color: {text_color}; }}"
        )
        self.multi_rig_preview_autoconfig_btn.setStyleSheet(button_style("secondary", theme))
        self.multi_rig_setup_btn.setStyleSheet(button_style("primary", theme))
        self.multi_rig_not_now_btn.setStyleSheet(button_style("muted", theme))
        self.multi_rig_copy_summary_btn.setStyleSheet(button_style("secondary", theme))

    def _refresh_multi_rig_status_card(self) -> None:
        if not hasattr(self, "multi_rig_status_card"):
            return
        status = self._current_multi_rig_runtime_status()
        title, summary, detail, level = self._multi_rig_status_text(status)
        self.multi_rig_status_title_label.setText(title)
        self.multi_rig_status_summary_label.setText(summary)
        self.multi_rig_status_detail_label.setObjectName("multiRigStatusDetail")
        self.multi_rig_status_detail_label.setText(detail)
        setup_available = status.startup_mode in {
            STARTUP_EXISTING_UNMIGRATED,
            STARTUP_DEFERRED,
            STARTUP_MIGRATION_ERROR,
        }
        self.multi_rig_preview_autoconfig_btn.setVisible(setup_available)
        self.multi_rig_setup_btn.setVisible(setup_available)
        self.multi_rig_setup_btn.setText("Continue Multi-Rig Setup" if status.startup_mode == STARTUP_DEFERRED else "Set up Multi-Rig")
        self.multi_rig_not_now_btn.setVisible(status.startup_mode == STARTUP_EXISTING_UNMIGRATED)
        self.multi_rig_copy_summary_btn.setVisible(setup_available)
        self.multi_rig_status_actions_widget.setVisible(
            self.multi_rig_preview_autoconfig_btn.isVisible()
            or self.multi_rig_setup_btn.isVisible()
            or self.multi_rig_not_now_btn.isVisible()
            or self.multi_rig_copy_summary_btn.isVisible()
        )
        self._style_multi_rig_status_card(level)

    def _settings_values_for_migration(self) -> Dict[str, Any]:
        try:
            return dict(self.settings.all())
        except Exception:
            return {}

    @staticmethod
    def _multi_rig_autoconfig_preview_text(upgrade_preview: Any, discovery_proposal: Any) -> tuple[str, str]:
        app_labels = [
            str(getattr(candidate, "display_name", "") or "").strip()
            for candidate in getattr(discovery_proposal, "candidates", ()) or ()
            if bool(getattr(candidate, "executable", False))
        ]
        app_text = ", ".join(sorted(set(app_labels))) if app_labels else "No launchable radio apps found yet"
        first_radio = next(iter(getattr(discovery_proposal, "radios", ()) or ()), None)
        port_text = ""
        if first_radio is not None:
            port_parts = []
            for assignment in getattr(first_radio, "ports", ()) or ():
                if str(getattr(assignment, "protocol", "tcp") or "tcp").lower() != "tcp":
                    continue
                service = str(getattr(assignment, "service", "") or "").strip()
                if service in {"flrig", "fldigi", "js8call"}:
                    port_parts.append(f"{service.upper()} {getattr(assignment, 'assigned_port', '')}")
            port_text = ", ".join(port_parts)
        backup_count = len(getattr(upgrade_preview, "backup_paths", ()) or ())
        referenced_count = len(getattr(upgrade_preview, "referenced_paths_not_backed_up", ()) or ())
        warnings = tuple(getattr(upgrade_preview, "warnings", ()) or ()) + tuple(
            getattr(discovery_proposal, "warnings", ()) or ()
        )
        summary = str(getattr(upgrade_preview, "summary", "") or "FIO can preview Multi-Rig setup.").strip()
        lines = [
            f"Apps found: {app_text}.",
            f"Suggested ports: {port_text or 'default local ports; no active radio apps found to validate yet'}.",
            f"Backup preview: {backup_count} config path(s).",
        ]
        if referenced_count:
            lines.append(f"Referenced data folders not copied by upgrade backup: {referenced_count}.")
        if warnings:
            lines.append("Review: " + " | ".join(str(warn) for warn in warnings[:3] if str(warn).strip()))
        return summary, "\n".join(line for line in lines if line.strip())

    @staticmethod
    def _multi_rig_autoconfig_extra_app_paths(settings_values: Mapping[str, Any]) -> tuple[Path, ...]:
        paths: List[Path] = []
        for key in (
            "path_flrig",
            "path_fldigi",
            "path_flmsg",
            "path_flamp",
            "path_js8call",
            "path_js8spotter",
            "path_commstat",
            "varac_path",
        ):
            value = str(settings_values.get(key, "") or "").strip()
            if value:
                paths.append(Path(value))
        return tuple(paths)

    def _preview_multi_rig_autoconfiguration(self) -> None:
        self._publish_settings_action_feedback(
            status="in_progress",
            summary="Scanning current station setup for Configure Automatically preview.",
            action_type="configure_automatically",
            source_surface="settings.configure_automatically.multirig.preview",
        )
        try:
            settings_values = self._settings_snapshot_for_readiness()
            upgrade_preview = build_single_rig_upgrade_preview(
                settings_values,
                config_dir=get_config_dir(),
            )
            discovery_proposal = build_autoconfig_proposal(
                radio_count=1,
                home=Path.home(),
                extra_app_paths=self._multi_rig_autoconfig_extra_app_paths(settings_values),
            )
            summary, detail = self._multi_rig_autoconfig_preview_text(upgrade_preview, discovery_proposal)
            if hasattr(self, "multi_rig_autoconfig_preview_label"):
                self.multi_rig_autoconfig_preview_label.setText(f"{summary}\n{detail}".strip())
                self.multi_rig_autoconfig_preview_label.setToolTip(detail)
                self.multi_rig_autoconfig_preview_label.setVisible(True)
            status = "partial" if getattr(discovery_proposal, "missing_apps", ()) else "succeeded"
            self._publish_settings_action_feedback(
                status=status,
                summary="Configure Automatically preview is ready.",
                detail=f"{summary}\n{detail}".strip(),
                action_type="configure_automatically",
                source_surface="settings.configure_automatically.multirig.preview",
            )
        except Exception as exc:
            log.exception("Failed building Multi-Rig Configure Automatically preview.")
            detail = str(exc) or exc.__class__.__name__
            if hasattr(self, "multi_rig_autoconfig_preview_label"):
                self.multi_rig_autoconfig_preview_label.setText(
                    "Configure Automatically preview could not be built. Your settings were not changed."
                )
                self.multi_rig_autoconfig_preview_label.setToolTip(detail)
                self.multi_rig_autoconfig_preview_label.setVisible(True)
            self._publish_settings_action_feedback(
                status="failed",
                summary="Configure Automatically preview failed.",
                detail=detail,
                action_type="configure_automatically",
                source_surface="settings.configure_automatically.multirig.preview",
            )

    def _multi_rig_radio_catalog(self) -> Dict[str, Any]:
        if self._multi_rig_radio_catalog_payload is None:
            try:
                payload = load_radio_catalog()
            except Exception as exc:
                log.debug("Failed loading radio catalog for multi-rig setup: %s", exc)
                payload = {"entries": (), "source": "unavailable"}
            self._multi_rig_radio_catalog_payload = dict(payload or {})
        return dict(self._multi_rig_radio_catalog_payload)

    def _defer_multi_rig_setup(self) -> None:
        try:
            with self.multi_radio_store.connect() as conn:
                ensure_multi_rig_migration(
                    conn,
                    self._settings_values_for_migration(),
                    defer=True,
                )
        except Exception as exc:
            log.exception("Failed deferring multi-rig setup.")
            QMessageBox.warning(self, "Multi-Rig Setup", f"Unable to pause Multi-Rig setup:\n{exc}")
            return
        self._refresh_cached_multi_rig_runtime_status()
        QMessageBox.information(
            self,
            "Multi-Rig Setup",
            "Multi-Rig setup is paused. FIO will keep using your current station setup.",
        )
        try:
            self.settings_saved.emit()
        except Exception:
            pass

    def _copy_multi_rig_status_summary(self) -> None:
        status = self._current_multi_rig_runtime_status()
        active_names = []
        for profile_id in status.active_device_profile_ids:
            name = self._radio_name_by_id(profile_id) or f"Radio {profile_id}"
            active_names.append(f"{name} ({profile_id})")
        lines = [
            "FreqInOut Multi-Rig Summary",
            f"FIO version: {__version__}",
            f"Platform: {platform.platform()}",
            f"Python: {sys.version.replace(chr(10), ' ')}",
            f"Architecture: {platform.machine()}",
            f"Startup mode: {status.startup_mode}",
            f"Migration version: {status.migration_version}",
            f"Migration current: {status.migration_current}",
            f"Migration paused: {status.migration_deferred}",
            f"Primary radio: {self._radio_name_by_id(status.primary_device_profile_id) or 'None'} ({status.primary_device_profile_id or 'none'})",
            f"Active radios: {len(status.active_device_profile_ids)}",
        ]
        if active_names:
            lines.append("Active radio list: " + ", ".join(active_names))
        if status.warnings:
            lines.append("Warnings: " + " | ".join(status.warnings))
        QApplication.clipboard().setText("\n".join(lines))
        self.multi_rig_copy_summary_btn.setText("Copied")
        QTimer.singleShot(1500, lambda: self.multi_rig_copy_summary_btn.setText("Copy Summary"))

    def _configured_text(self, key: str) -> str:
        try:
            value = self.settings.get(key, "")
        except Exception:
            value = ""
        return str(value or "").strip()

    @staticmethod
    def _next_default_instance_port(
        service: str,
        profiles: Sequence[Mapping[str, Any]],
        *,
        existing_profile_id: int = 0,
    ) -> str:
        return next_default_instance_port(service, profiles, existing_profile_id=existing_profile_id)

    @staticmethod
    def _guided_js8_profile_review_text(
        profiles: Sequence[Any],
        *,
        tcp_port: str = "",
        profile_name: str = "",
    ) -> str:
        return guided_js8_profile_review_text(profiles, tcp_port=tcp_port, profile_name=profile_name)

    @staticmethod
    def _guided_detection_path(results: Mapping[str, PathDetectionResult], key: str) -> str:
        return guided_detection_path(results, key)

    @staticmethod
    def _guided_single_install_path(
        candidates: Sequence[Any],
        app_id: str,
        fallback_results: Mapping[str, PathDetectionResult],
        result_key: str,
        label: str,
        review: List[str],
    ) -> str:
        return guided_single_install_path(candidates, app_id, fallback_results, result_key, label, review)

    @staticmethod
    def _guided_app_candidate_identity(candidate: Any) -> Tuple[str, str]:
        return guided_app_candidate_identity(candidate)

    @staticmethod
    def _guided_app_candidate_choices(candidates: Sequence[Any], app_id: str) -> Tuple[Tuple[str, str], ...]:
        return guided_app_candidate_choices(candidates, app_id)

    @staticmethod
    def _guided_js8_profile_choices(profiles: Sequence[Any]) -> Tuple[Tuple[str, Dict[str, str]], ...]:
        return guided_js8_profile_choices(profiles)

    @staticmethod
    def _guided_port_prompt_keys(
        *,
        current: Mapping[str, str],
        selected: Mapping[str, bool],
        backend: str,
        observer_mode: bool,
    ) -> Tuple[str, ...]:
        return guided_port_prompt_keys(current=current, selected=selected, backend=backend, observer_mode=observer_mode)

    @staticmethod
    def _guided_radio_autofill_suggestions(
        *,
        current: Mapping[str, str],
        selected: Mapping[str, bool],
        backend: str,
        observer_mode: bool,
        install_candidates: Sequence[Any],
        fast_results: Mapping[str, PathDetectionResult],
        js8_results: Mapping[str, PathDetectionResult],
        varac_results: Mapping[str, PathDetectionResult],
        js8_file_profiles: Sequence[Any],
        default_ports: Mapping[str, str],
        profile_name: str = "",
    ) -> Tuple[Dict[str, str], Tuple[str, ...]]:
        return guided_radio_autofill_suggestions(
            current=current,
            selected=selected,
            backend=backend,
            observer_mode=observer_mode,
            install_candidates=install_candidates,
            fast_results=fast_results,
            js8_results=js8_results,
            varac_results=varac_results,
            js8_file_profiles=js8_file_profiles,
            default_ports=default_ports,
            profile_name=profile_name,
        )

    def _detect_migration_roles(self) -> set[str]:
        roles: set[str] = set()
        message_paths = self.settings.get("message_paths", {}) or {}
        if self._configured_text("path_flrig") or self._configured_text("path_fldigi") or self._configured_text("fldigi_log_path"):
            roles.add("fast_light")
        if self._configured_text("path_js8call") or self._configured_text("js8_directed_path"):
            roles.add("js8call")
        if self._configured_text("path_js8spotter"):
            roles.add("js8spotter")
        if self._configured_text("varac_path") or self._configured_text("varac_launch_cmd") or str(message_paths.get("varac", "") or "").strip():
            roles.add("varac")
        if self._configured_text("path_flamp") or str(message_paths.get("flamp", "") or "").strip():
            roles.add("flamp")
        if self._configured_text("path_flmsg") or str(message_paths.get("flmsg", "") or "").strip():
            roles.add("flmsg")
        if self._configured_text("path_commstat"):
            roles.add("commstat")
        return roles

    def _start_multi_rig_setup(self) -> None:
        status = self._current_multi_rig_runtime_status()
        if status.startup_mode in {STARTUP_FRESH_DEFAULT_READY, STARTUP_MIGRATED}:
            self._select_settings_section_group(getattr(self, "radio_profile_section_group", None))
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Set up Multi-Rig")
        dialog.resize(620, 520)
        layout = QVBoxLayout(dialog)
        intro = QLabel(
            "FIO found your current station setup. Multi-Rig setup will make that station the first runtime radio. "
            "Your current settings stay unchanged until you confirm setup."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        layout.addLayout(form)

        catalog_payload = self._multi_rig_radio_catalog()
        catalog_entries = list(catalog_payload.get("entries", []) or [])
        model_combo = QComboBox()
        model_combo.setEditable(True)
        model_combo.setInsertPolicy(QComboBox.NoInsert)
        for entry in catalog_entries:
            model_combo.addItem(str(entry.get("display_name", "") or ""), dict(entry))
        if model_combo.count() == 0:
            model_combo.addItem("Manual entry", {})
        model_combo.setMinimumWidth(320)
        form.addRow("Radio model:", model_combo)

        manual_chk = QCheckBox("Use manual model entry")
        if not catalog_entries:
            manual_chk.setChecked(True)
            model_combo.setEnabled(False)
        form.addRow("", manual_chk)
        manufacturer_edit = QLineEdit()
        model_edit = QLineEdit()
        form.addRow("Manufacturer:", manufacturer_edit)
        form.addRow("Model:", model_edit)
        name_edit = QLineEdit()
        name_edit.setPlaceholderText("Example: IC-7300 HF Desk")
        form.addRow("Display name:", name_edit)
        plan_edit = QLineEdit("Daily HF Schedule")
        form.addRow("Plan name:", plan_edit)
        display_name_user_edited = False
        last_catalog_display_name = ""

        role_group = QGroupBox("Software FIO found in Settings")
        role_layout = QVBoxLayout(role_group)
        role_layout.setSpacing(4)
        role_checks: Dict[str, QCheckBox] = {}
        role_labels = [
            ("fast_light", "FLRig / FLDigi (Fast Light)"),
            ("js8call", "JS8Call"),
            ("js8spotter", "JS8Spotter"),
            ("varac", "VarAC"),
            ("flamp", "FLAMP"),
            ("flmsg", "FLMsg"),
            ("commstat", "CommStat"),
        ]
        detected_roles = self._detect_migration_roles()
        for role, label in role_labels:
            chk = QCheckBox(label)
            chk.setChecked(role in detected_roles)
            role_layout.addWidget(chk)
            role_checks[role] = chk
        role_note = QLabel("These choices come from configured paths and endpoints, not from live process checks.")
        role_note.setWordWrap(True)
        role_layout.addWidget(role_note)
        layout.addWidget(role_group)

        def _selected_catalog_entry() -> Dict[str, Any]:
            current_text = model_combo.currentText().strip()
            current_index = model_combo.currentIndex()
            if current_index < 0 or current_text != model_combo.itemText(current_index).strip():
                return {}
            data = model_combo.currentData()
            return dict(data) if isinstance(data, dict) else {}

        def _sync_model_fields() -> None:
            nonlocal display_name_user_edited, last_catalog_display_name
            manual = bool(manual_chk.isChecked())
            entry = _selected_catalog_entry()
            manufacturer_edit.setEnabled(manual)
            model_edit.setEnabled(manual)
            if not manual:
                manufacturer_edit.setText(str(entry.get("manufacturer", "") or ""))
                model_edit.setText(str(entry.get("model_name", "") or ""))
                display_name = str(entry.get("display_name", "") or "").strip()
                current_name = name_edit.text().strip()
                can_replace = (
                    not current_name
                    or current_name == last_catalog_display_name
                    or not display_name_user_edited
                )
                if display_name and can_replace:
                    name_edit.setText(display_name)
                    last_catalog_display_name = display_name
                    display_name_user_edited = False

        def _mark_display_name_user_edited(_text: str) -> None:
            nonlocal display_name_user_edited
            display_name_user_edited = True

        model_combo.currentIndexChanged.connect(lambda _idx: _sync_model_fields())
        manual_chk.stateChanged.connect(lambda _state: _sync_model_fields())
        name_edit.textEdited.connect(_mark_display_name_user_edited)
        _sync_model_fields()

        buttons = QDialogButtonBox()
        setup_btn = buttons.addButton("Set up Multi-Rig", QDialogButtonBox.AcceptRole)
        not_now_btn = buttons.addButton("Not Now", QDialogButtonBox.DestructiveRole)
        buttons.addButton(QDialogButtonBox.Cancel)
        layout.addWidget(buttons)

        action: Dict[str, str] = {"value": ""}

        def _accept_setup() -> None:
            if not name_edit.text().strip():
                QMessageBox.warning(dialog, "Multi-Rig Setup", "Radio display name is required.")
                return
            if manual_chk.isChecked() and (
                not manufacturer_edit.text().strip() or not model_edit.text().strip()
            ):
                QMessageBox.warning(
                    dialog,
                    "Multi-Rig Setup",
                    "Manufacturer and model are required for manual radio entry.",
                )
                return
            if not manual_chk.isChecked() and not _selected_catalog_entry():
                QMessageBox.warning(
                    dialog,
                    "Multi-Rig Setup",
                    "Choose a supported radio model from the list, or select manual model entry.",
                )
                return
            action["value"] = "setup"
            dialog.accept()

        def _accept_defer() -> None:
            action["value"] = "defer"
            dialog.accept()

        setup_btn.clicked.connect(_accept_setup)
        not_now_btn.clicked.connect(_accept_defer)
        buttons.rejected.connect(dialog.reject)
        if dialog.exec() != QDialog.Accepted:
            return
        if action["value"] == "defer":
            self._defer_multi_rig_setup()
            return

        roles = tuple(sorted(role for role, chk in role_checks.items() if chk.isChecked()))
        migration_settings = self._settings_values_for_migration()
        self._run_backup_backed_multi_rig_setup_apply(
            migration_settings=migration_settings,
            radio_name=name_edit.text().strip(),
            radio_manufacturer=manufacturer_edit.text().strip(),
            radio_model=model_edit.text().strip(),
            operating_plan_name=plan_edit.text().strip(),
            enabled_software_roles=roles,
        )

    def _set_multi_rig_setup_preview_text(self, text: str, tooltip: str = "") -> None:
        if not hasattr(self, "multi_rig_autoconfig_preview_label"):
            return
        self.multi_rig_autoconfig_preview_label.setText(text)
        self.multi_rig_autoconfig_preview_label.setToolTip(tooltip or text)
        self.multi_rig_autoconfig_preview_label.setVisible(bool(text.strip()))

    def _run_backup_backed_multi_rig_setup_apply(
        self,
        *,
        migration_settings: Mapping[str, Any],
        radio_name: str,
        radio_manufacturer: str,
        radio_model: str,
        operating_plan_name: str,
        enabled_software_roles: Sequence[str],
    ) -> bool:
        apply_plan = build_single_rig_upgrade_apply_plan(
            migration_settings,
            radio_name=radio_name,
            operating_plan_name=operating_plan_name,
            config_dir=get_config_dir(),
        )
        if not apply_plan.can_apply:
            detail = "\n".join(apply_plan.blockers)
            self._set_multi_rig_setup_preview_text(
                f"Multi-Rig setup is blocked until backup readiness is resolved.\n{detail}".strip(),
                detail,
            )
            self._publish_settings_action_feedback(
                status="blocked",
                summary="Multi-Rig setup blocked: backup readiness needs review.",
                detail=detail,
                action_type="configure_automatically",
                source_surface="settings.configure_automatically.multirig.apply",
            )
            return False
        try:
            backup_result = create_config_backup(apply_plan.backup_paths, reason=apply_plan.backup_reason)
        except Exception as exc:
            log.exception("Failed creating Multi-Rig migration backup.")
            detail = str(exc) or exc.__class__.__name__
            self._set_multi_rig_setup_preview_text(
                f"Multi-Rig setup is blocked because the backup could not be created.\n{detail}".strip(),
                detail,
            )
            self._publish_settings_action_feedback(
                status="failed",
                summary="Multi-Rig setup blocked: backup could not be created.",
                detail=detail,
                action_type="configure_automatically",
                source_surface="settings.configure_automatically.multirig.apply",
            )
            return False
        failed_backup_items = tuple(item for item in backup_result.items if item.status == "failed")
        primary_backup_ok = bool(backup_result.items and backup_result.items[0].status == "backed_up")
        if failed_backup_items or not primary_backup_ok:
            detail_parts = [f"{item.original_path}: {item.error or item.status}" for item in failed_backup_items]
            if not primary_backup_ok:
                detail_parts.insert(0, "Primary FIO configuration backup did not complete.")
            detail = "\n".join(detail_parts)
            self._set_multi_rig_setup_preview_text(
                f"Multi-Rig setup is blocked because the backup did not complete.\n{detail}".strip(),
                detail,
            )
            self._publish_settings_action_feedback(
                status="blocked",
                summary="Multi-Rig setup blocked: backup did not complete.",
                detail=detail,
                action_type="configure_automatically",
                source_surface="settings.configure_automatically.multirig.apply",
            )
            return False
        self._publish_settings_action_feedback(
            status="succeeded",
            summary="Multi-Rig setup backup created.",
            detail=f"Backup saved to {backup_result.backup_dir}",
            action_type="configure_automatically",
            source_surface="settings.configure_automatically.multirig.apply",
        )
        try:
            with self.multi_radio_store.connect() as conn:
                result = ensure_multi_rig_migration(
                    conn,
                    migration_settings,
                    radio_name=radio_name,
                    radio_manufacturer=radio_manufacturer,
                    radio_model=radio_model,
                    operating_plan_name=operating_plan_name,
                    enabled_software_roles=enabled_software_roles,
                )
        except Exception as exc:
            log.exception("Failed running multi-rig migration.")
            detail = str(exc) or exc.__class__.__name__
            self._set_multi_rig_setup_preview_text(
                f"Multi-Rig setup could not be completed after backup.\n{detail}".strip(),
                detail,
            )
            self._publish_settings_action_feedback(
                status="failed",
                summary="Multi-Rig setup failed after backup.",
                detail=detail,
                action_type="configure_automatically",
                source_surface="settings.configure_automatically.multirig.apply",
            )
            QMessageBox.warning(self, "Multi-Rig Setup", f"Unable to complete Multi-Rig setup:\n{exc}")
            return False
        if not result.applied and not result.already_current:
            self._set_multi_rig_setup_preview_text(
                "Multi-Rig setup could not be completed. Your current settings were left unchanged."
            )
            self._publish_settings_action_feedback(
                status="failed",
                summary="Multi-Rig setup failed after backup.",
                detail="FIO could not complete Multi-Rig setup. Your current settings were left unchanged.",
                action_type="configure_automatically",
                source_surface="settings.configure_automatically.multirig.apply",
            )
            QMessageBox.warning(
                self,
                "Multi-Rig Setup",
                "FIO could not complete Multi-Rig setup. Your current settings were left unchanged.",
            )
            return False
        self._refresh_multi_radio_tables(refresh_section_titles=False)
        self._refresh_cached_multi_rig_runtime_status()
        self._emit_device_profiles_changed()
        try:
            self.settings_saved.emit()
        except Exception:
            pass
        self._set_multi_rig_setup_preview_text(
            f"Multi-Rig setup is ready. Backup saved to {backup_result.backup_dir}",
            f"Backup manifest: {backup_result.manifest_path}",
        )
        self._publish_settings_action_feedback(
            status="succeeded",
            summary="Multi-Rig setup is ready.",
            detail=f"FIO created the first runtime radio after backing up settings to {backup_result.backup_dir}",
            action_type="configure_automatically",
            source_surface="settings.configure_automatically.multirig.apply",
        )
        return True
    def _update_device_profile_readiness_detail(
        self,
        readiness_report: Any | None = None,
        focused_radio_id: int | None = None,
    ) -> None:
        if not hasattr(self, "device_profile_detail_label"):
            return

        def _set_readiness_card_style(level: str) -> None:
            if not hasattr(self, "device_profile_readiness_card"):
                return
            theme = resolve_theme(self.settings)
            level_key = (level or "info").strip().lower()
            color_map = {
                "success": theme.get("success", "#2E7D32"),
                "warning": theme.get("warning", "#C99700"),
                "danger": theme.get("danger", "#C62828"),
                "info": theme.get("info", theme.get("accent", "#1565C0")),
            }
            accent = QColor(color_map.get(level_key, color_map["info"]))
            bg = QColor(accent)
            bg.setAlpha(24)
            border = QColor(accent)
            border.setAlpha(140)
            text_color = theme.get("text", "#1C1F21")
            muted_color = theme.get("text_muted", text_color)
            self.device_profile_readiness_card.setStyleSheet(
                "QFrame {"
                f" background-color: {bg.name(QColor.HexArgb)};"
                f" border: 1px solid {border.name(QColor.HexArgb)};"
                " border-radius: 8px;"
                "}"
                f" QLabel {{ color: {text_color}; border: none; background: transparent; }}"
                f" QLabel#deviceProfileReadinessStatus {{ color: {muted_color}; }}"
            )

        if hasattr(self, "device_profile_readiness_status_label"):
            self.device_profile_readiness_status_label.setObjectName("deviceProfileReadinessStatus")
        if hasattr(self, "device_profile_guardrail_status_label"):
            self.device_profile_guardrail_status_label.setObjectName("deviceProfileGuardrailStatus")
        if readiness_report is None:
            readiness_report = self._current_station_readiness_report()
        else:
            self._last_station_readiness_report = readiness_report
        if focused_radio_id is None:
            focused_radio_id = self._current_device_profile_focus_id()
        if not focused_radio_id:
            if hasattr(self, "device_profile_readiness_card"):
                self.device_profile_readiness_card.setVisible(True)
            message = "Select a radio to review the readiness checklist for that radio."
            if hasattr(self, "device_profile_detail_title_label"):
                self.device_profile_detail_title_label.setText("Selected Radio")
            if hasattr(self, "device_profile_readiness_title_label"):
                self.device_profile_readiness_title_label.setText("Focused Radio Readiness")
            if hasattr(self, "device_profile_readiness_status_label"):
                self.device_profile_readiness_status_label.setText(message)
            self._set_device_profile_guardrail_status(())
            self._refresh_device_profile_status_chips(None, readiness_report)
            self._refresh_radio_profile_connection_details(None)
            self._refresh_radio_profile_frequency_timer_details(None)
            self._refresh_radio_profile_optional_groups(None)
            self._refresh_radio_profile_inventory_details(None)
            self._refresh_radio_profile_software_flag_controls(None)
            self._refresh_radio_profile_software_chips(readiness_report)
            self._refresh_radio_profile_stack_guidance(readiness_report, None, None)
            self.device_profile_detail_label.setText("Select a radio to edit that radio's settings.")
            _set_readiness_card_style("info")
            return
        profile = self._device_profile_by_id(int(focused_radio_id))
        if not profile:
            message = "Select a radio to review the readiness checklist for that radio."
            if hasattr(self, "device_profile_detail_title_label"):
                self.device_profile_detail_title_label.setText("Selected Radio")
            if hasattr(self, "device_profile_readiness_title_label"):
                self.device_profile_readiness_title_label.setText("Focused Radio Readiness")
            if hasattr(self, "device_profile_readiness_status_label"):
                self.device_profile_readiness_status_label.setText(message)
            self._set_device_profile_guardrail_status(())
            self._refresh_device_profile_status_chips(None, readiness_report)
            self._refresh_radio_profile_connection_details(None)
            self._refresh_radio_profile_frequency_timer_details(None)
            self._refresh_radio_profile_optional_groups(None)
            self._refresh_radio_profile_inventory_details(None)
            self._refresh_radio_profile_software_flag_controls(None)
            self._refresh_radio_profile_software_chips(readiness_report)
            self._refresh_radio_profile_stack_guidance(readiness_report, None, None)
            self.device_profile_detail_label.setText("Select a radio to edit that radio's settings.")
            _set_readiness_card_style("info")
            return
        summary = readiness_report.summary_for_radio(int(focused_radio_id))
        guardrail_warnings = self._current_multi_rig_guardrail_messages()
        has_guardrail_warnings = self._set_device_profile_guardrail_status(guardrail_warnings)
        assignment = self._effective_assignment_map().get(int(focused_radio_id), {})
        name = str(profile.get("name", "") or "Radio").strip() or "Radio"
        assignment_name = str(assignment.get("operating_profile_name", "") or "").strip() or "Unassigned"
        assignment_state = str(assignment.get("assignment_state", "") or "").strip().lower()
        if hasattr(self, "device_profile_readiness_title_label"):
            self.device_profile_readiness_title_label.setText(f"{name} Readiness")
        if hasattr(self, "device_profile_detail_title_label"):
            self.device_profile_detail_title_label.setText(f"{name} Profile")
        self._refresh_device_profile_status_chips(profile, readiness_report)
        self._refresh_radio_profile_connection_details(profile)
        self._refresh_radio_profile_frequency_timer_details(profile)
        self._refresh_radio_profile_optional_groups(profile)
        self._refresh_radio_profile_inventory_details(profile)
        self._refresh_radio_profile_software_flag_controls(profile)
        self._refresh_radio_profile_software_chips(readiness_report)
        self._refresh_radio_profile_stack_guidance(readiness_report, int(focused_radio_id), profile)
        self.device_profile_detail_label.setText(self._selected_radio_detail_text(profile, readiness_report))
        if summary is None or (
            summary.required_count <= 0 and summary.recommended_count <= 0 and summary.informational_count <= 0
        ):
            if self._profile_needs_operator_name(profile):
                if hasattr(self, "device_profile_readiness_card"):
                    self.device_profile_readiness_card.setVisible(True)
                message = f"{name} is using a fallback name. Rename it so radio-specific settings are easy to recognize."
                if hasattr(self, "device_profile_readiness_status_label"):
                    self.device_profile_readiness_status_label.setText(message)
                _set_readiness_card_style("warning")
                return
            if hasattr(self, "device_profile_readiness_card"):
                self.device_profile_readiness_card.setVisible(bool(has_guardrail_warnings))
            assigned_schedule = self._assignment_display_text(assignment_name, assignment_state)
            schedule_text = (
                f" Assigned plan: {assigned_schedule}."
                if assignment_name != "Unassigned"
                else " No frequency plan is currently assigned."
            )
            message = f"{name} is ready.{schedule_text}"
            if hasattr(self, "device_profile_readiness_status_label"):
                self.device_profile_readiness_status_label.setText(message)
            _set_readiness_card_style("warning" if has_guardrail_warnings else "success")
            return
        if hasattr(self, "device_profile_readiness_card"):
            self.device_profile_readiness_card.setVisible(True)
        issue_lines = []
        for issue in readiness_report.issues:
            if int(issue.radio_id or 0) != int(focused_radio_id):
                continue
            issue_lines.append(format_readiness_issue(issue))
        detail_text = " | ".join(issue_lines[:4])
        if len(issue_lines) > 4:
            detail_text += f" | {len(issue_lines) - 4} more item(s)"
        assigned_schedule = self._assignment_display_text(assignment_name, assignment_state)
        schedule_text = (
            f"Assigned plan: {assigned_schedule}. "
            if assignment_name != "Unassigned"
            else "No frequency plan is currently assigned. "
        )
        message = f"{readiness_summary_status_text(summary, subject=name)} {schedule_text}".strip()
        detail_message = readiness_state_description(summary.overall_state)
        if detail_text:
            detail_message = f"{detail_message} Guidance: {detail_text}"
        if hasattr(self, "device_profile_readiness_status_label"):
            if self._profile_needs_operator_name(profile):
                detail_message = (
                    f"Rename this radio so radio-specific settings are easy to recognize. {detail_message}"
                )
            self.device_profile_readiness_status_label.setText(f"{message}\n{detail_message}")
        self.device_profile_detail_label.setText(self._selected_radio_detail_text(profile, readiness_report))
        _set_readiness_card_style(readiness_state_card_level(summary.overall_state))

    def _set_guidance_card_state(
        self,
        card: QWidget,
        title_label: QLabel,
        status_label: QLabel,
        *,
        title: str,
        text: str,
        level: str,
    ) -> None:
        theme = resolve_theme(self.settings)
        level_key = (level or "info").strip().lower()
        color_map = {
            "success": theme.get("success", "#2E7D32"),
            "warning": theme.get("warning", "#C99700"),
            "danger": theme.get("danger", "#C62828"),
            "info": theme.get("info", theme.get("accent", "#1565C0")),
        }
        accent = QColor(color_map.get(level_key, color_map["info"]))
        bg = QColor(accent)
        bg.setAlpha(24)
        border = QColor(accent)
        border.setAlpha(140)
        text_color = theme.get("text", "#1C1F21")
        muted_color = theme.get("text_muted", text_color)
        title_label.setText(title)
        status_label.setText(text)
        card.setStyleSheet(
            "QFrame {"
            f" background-color: {bg.name(QColor.HexArgb)};"
            f" border: 1px solid {border.name(QColor.HexArgb)};"
            " border-radius: 8px;"
            "}"
            f" QLabel {{ color: {text_color}; border: none; background: transparent; }}"
            f" QLabel#{status_label.objectName()} {{ color: {muted_color}; }}"
        )

    def _build_readiness_summary_text(self) -> str:
        report = self._current_station_readiness_report()
        lines = [readiness_report_detail_text(report, title="FreqInOut Multi-Rig Readiness Summary")]
        lines.extend(
            [
                (
                    f"Issues: {int(report.required_count)} required, "
                    f"{int(report.recommended_count)} recommended, "
                    f"{int(report.informational_count)} informational"
                ),
            ]
        )
        global_issues = [issue for issue in report.issues if issue.scope == "global"]
        if global_issues:
            lines.append("")
            lines.append("Station Guidance:")
            for issue in global_issues:
                lines.append(f"- {format_readiness_issue(issue)}")
        if report.radio_summaries:
            lines.append("")
            lines.append("Radio Guidance:")
            for summary in report.radio_summaries:
                lines.append(f"- {summary.name}: {readiness_state_label(summary.overall_state)}")
                lines.append(f"  {readiness_summary_status_text(summary, subject=summary.name)}")
                for message in summary.messages[:3]:
                    lines.append(f"  - {message}")
        return "\n".join(lines)

    def _copy_readiness_summary(self) -> None:
        text = self._build_readiness_summary_text()
        QApplication.clipboard().setText(text)
        if hasattr(self, "copy_readiness_summary_btn"):
            QToolTip.showText(
                self.copy_readiness_summary_btn.mapToGlobal(self.copy_readiness_summary_btn.rect().bottomLeft()),
                "Readiness summary copied to clipboard.",
                self.copy_readiness_summary_btn,
            )

    def _current_operating_profile_focus_id(self) -> int | None:
        if not hasattr(self, "operating_profiles_table"):
            return None
        item = self.operating_profiles_table.currentItem()
        if item is not None:
            row_item = self.operating_profiles_table.item(item.row(), 2)
            if row_item is not None:
                try:
                    ident = int(row_item.data(Qt.UserRole) or 0)
                except Exception:
                    ident = 0
                if ident > 0:
                    return ident
        first = next(
            (
                int(row.get("id", 0) or 0)
                for row in self.operating_profiles
                if isinstance(row, dict) and int(row.get("id", 0) or 0) > 0
            ),
            0,
        )
        return first or None

    def _update_operating_profile_guidance_detail(self) -> None:
        if not hasattr(self, "operating_profiles_guidance_card"):
            return
        focused_id = self._current_operating_profile_focus_id()
        if not focused_id:
            self._set_guidance_card_state(
                self.operating_profiles_guidance_card,
                self.operating_profiles_guidance_title_label,
                self.operating_profiles_guidance_status_label,
                title="Focused Frequency Plan Guidance",
                text="Add a frequency plan to define where and when a radio should operate when that plan is assigned to it.",
                level="info",
            )
            return
        profile = self._operating_profile_by_id(int(focused_id))
        if not profile:
            return
        name = str(profile.get("name", "") or "Frequency Plan").strip() or "Frequency Plan"
        assigned_rows = [
            row
            for row in self.device_assignments
            if isinstance(row, dict) and int(row.get("operating_profile_id", 0) or 0) == int(focused_id)
        ]
        assigned_devices = [str(row.get("device_name", "") or "Radio").strip() for row in assigned_rows[:3]]
        if int(profile.get("enabled", 1) or 0) != 1:
            text = (
                f"{name} is disabled. Enable it before assigning it to a radio. "
                f"Plan behavior is {self._scheduler_mode_label(profile.get('scheduler_mode', 'full'))}."
            )
            level = "warning"
        else:
            shell_summary = self._operating_profile_shell_summary(profile)
            if assigned_rows:
                text = (
                f"{name} is currently assigned to {len(assigned_rows)} radio"
                    f"{'s' if len(assigned_rows) != 1 else ''}: {', '.join(assigned_devices)}. "
                    f"Behavior: {shell_summary}."
                )
                level = "success"
            else:
                text = (
                    f"{name} is ready to assign. "
                    f"Plan behavior: {self._scheduler_mode_label(profile.get('scheduler_mode', 'full'))}. "
                    f"Behavior: {shell_summary}."
                )
                level = "info"
        self._set_guidance_card_state(
            self.operating_profiles_guidance_card,
            self.operating_profiles_guidance_title_label,
            self.operating_profiles_guidance_status_label,
            title=f"{name} Guidance",
            text=text,
            level=level,
        )

    def _current_device_assignment_focus_id(self) -> int | None:
        if not hasattr(self, "device_assignments_table"):
            return None
        item = self.device_assignments_table.currentItem()
        if item is not None:
            row_item = self.device_assignments_table.item(item.row(), 3)
            if row_item is not None:
                try:
                    ident = int(row_item.data(Qt.UserRole) or 0)
                except Exception:
                    ident = 0
                if ident > 0:
                    return ident
        first = next(
            (
                int(row.get("device_profile_id", 0) or 0)
                for row in self.device_assignments
                if isinstance(row, dict) and int(row.get("device_profile_id", 0) or 0) > 0
            ),
            0,
        )
        return first or None

    def _update_device_assignments_guidance_detail(self) -> None:
        if not hasattr(self, "device_assignments_guidance_card"):
            return
        focused_id = self._current_device_assignment_focus_id()
        active_swap = dict(self.active_profile_swap or {})
        if not focused_id:
            text = "Assigned Plans connect radios to frequency plans. Select a row to review whether that radio is using its default plan or a temporary override."
            if active_swap:
                text = (
                    "A temporary plan swap is active. Review the focused assignment rows and use Restore Swap when the temporary Station Default handoff is no longer needed."
                )
            self._set_guidance_card_state(
                self.device_assignments_guidance_card,
                self.device_assignments_guidance_title_label,
                self.device_assignments_guidance_status_label,
                title="Focused Assigned Plan Guidance",
                text=text,
                level="warning" if active_swap else "info",
            )
            return
        row = next(
            (
                dict(item)
                for item in self.device_assignments
                if isinstance(item, dict) and int(item.get("device_profile_id", 0) or 0) == int(focused_id)
            ),
            None,
        )
        if row is None:
            return
        device_name = str(row.get("device_name", "") or "Radio").strip() or "Radio"
        state = str(row.get("assignment_state", "") or "").strip().lower()
        operating_name = str(row.get("operating_profile_name", "") or "Unassigned").strip() or "Unassigned"
        if state == "temporary_override":
            text = (
                f"{device_name} is running a temporary override with {operating_name}. "
                "Use Restore Default Plan when the temporary assignment window ends."
            )
            level = "warning"
        elif row.get("operating_profile_id") in (None, "", 0):
            text = (
                f"{device_name} is currently unassigned. Assign a frequency plan if this radio should participate in Station Default schedule workflows."
            )
            level = "warning"
        else:
            assignment_text = self._assignment_display_text(operating_name, state)
            text = (
                f"{device_name} is using {assignment_text}. "
                f"Endpoint summary: {str(row.get('endpoint_summary', '') or '--')}."
            )
            level = "success" if int(row.get("runtime_primary", 0) or 0) == 1 else "info"
        self._set_guidance_card_state(
            self.device_assignments_guidance_card,
            self.device_assignments_guidance_title_label,
            self.device_assignments_guidance_status_label,
            title=f"{device_name} Assigned Plan Guidance",
            text=text,
            level=level,
        )

    def _current_varac_cluster_focus_id(self) -> int | None:
        if not hasattr(self, "varac_clusters_table"):
            return None
        item = self.varac_clusters_table.currentItem()
        if item is not None:
            row_item = self.varac_clusters_table.item(item.row(), 1)
            if row_item is not None:
                try:
                    ident = int(row_item.data(Qt.UserRole) or 0)
                except Exception:
                    ident = 0
                if ident > 0:
                    return ident
        first = next(
            (
                int(row.get("id", 0) or 0)
                for row in self.varac_clusters
                if isinstance(row, dict) and int(row.get("id", 0) or 0) > 0
            ),
            0,
        )
        return first or None

    def _update_varac_cluster_guidance_detail(self) -> None:
        if not hasattr(self, "varac_clusters_guidance_card"):
            return
        if not self._varac_cluster_mode_enabled():
            self._set_guidance_card_state(
                self.varac_clusters_guidance_card,
                self.varac_clusters_guidance_title_label,
                self.varac_clusters_guidance_status_label,
                title="Focused VarAC Cluster Guidance",
                text="Cluster mode is off. Enable Cluster Mode in VarAC Settings when you want multiple radios or VarAC instances to share coordinated cluster routing.",
                level="info",
            )
            return
        focused_id = self._current_varac_cluster_focus_id()
        if not focused_id:
            self._set_guidance_card_state(
                self.varac_clusters_guidance_card,
                self.varac_clusters_guidance_title_label,
                self.varac_clusters_guidance_status_label,
                title="Focused VarAC Cluster Guidance",
                text="Create a VarAC cluster to define a shared DB identity, refresh cadence, and optional gateway handler for radios that work together.",
                level="info",
            )
            return
        cluster = self._varac_cluster_by_id(int(focused_id))
        if not cluster:
            return
        name = str(cluster.get("name", "") or "VarAC Cluster").strip() or "VarAC Cluster"
        enabled_members = int(cluster.get("enabled_member_count", 0) or 0)
        total_members = int(cluster.get("member_count", 0) or 0)
        gateway = str(cluster.get("gateway_handler_name", "") or "").strip()
        if enabled_members > 1 and not gateway:
            text = (
                f"{name} has {enabled_members} enabled members and should designate one gateway handler before operators rely on it for coordinated VarAC routing."
            )
            level = "warning"
        elif total_members <= 0:
            text = (
                f"{name} has no memberships yet. Add radios in VarAC Memberships, then return here to choose a gateway handler if the cluster will route through one device."
            )
            level = "info"
        else:
            text = (
                f"{name} currently has {enabled_members}/{total_members} enabled memberships. "
                f"Gateway handler: {gateway or 'not selected'}. Shared DB: {str(cluster.get('shared_db_path', '') or 'device-local only')}."
            )
            level = "success" if gateway or enabled_members <= 1 else "info"
        self._set_guidance_card_state(
            self.varac_clusters_guidance_card,
            self.varac_clusters_guidance_title_label,
            self.varac_clusters_guidance_status_label,
            title=f"{name} Guidance",
            text=text,
            level=level,
        )

    def _current_varac_membership_focus(self) -> Optional[Dict[str, Any]]:
        if not hasattr(self, "varac_members_table"):
            return next((dict(row) for row in self.varac_cluster_members if isinstance(row, dict)), None)
        current_row = self.varac_members_table.currentRow()
        if current_row >= 0 and current_row < len(self.varac_cluster_members):
            row = self.varac_cluster_members[current_row]
            if isinstance(row, dict):
                return dict(row)
        return next((dict(row) for row in self.varac_cluster_members if isinstance(row, dict)), None)

    def _device_has_varac_local_config(self, device_profile_id: int) -> bool:
        device = self._device_profile_by_id(int(device_profile_id))
        if not isinstance(device, dict):
            return False
        return any(
            str(device.get(key, "") or "").strip()
            for key in ("varac_install_path", "varac_db_path", "varac_ini_path", "launch_cmd")
        )

    def _update_varac_membership_guidance_detail(self) -> None:
        if not hasattr(self, "varac_memberships_guidance_card"):
            return
        if not self._varac_cluster_mode_enabled():
            self._set_guidance_card_state(
                self.varac_memberships_guidance_card,
                self.varac_memberships_guidance_title_label,
                self.varac_memberships_guidance_status_label,
                title="Focused VarAC Membership Guidance",
                text="Cluster mode is off. Enable Cluster Mode in VarAC Settings before assigning radios to coordinated VarAC memberships.",
                level="info",
            )
            return
        membership = self._current_varac_membership_focus()
        if not membership:
            self._set_guidance_card_state(
                self.varac_memberships_guidance_card,
                self.varac_memberships_guidance_title_label,
                self.varac_memberships_guidance_status_label,
                title="Focused VarAC Membership Guidance",
                text="Assign radios to a VarAC cluster when they should share cluster identity and coordinated routing behavior.",
                level="info",
            )
            return
        cluster_name = str(membership.get("cluster_name", "") or "Cluster").strip() or "Cluster"
        device_name = str(membership.get("device_name", "") or "Radio").strip() or "Radio"
        instance_number = int(membership.get("instance_number", 0) or 0)
        enabled = int(membership.get("enabled", 1) or 0) == 1
        is_gateway = bool(membership.get("is_gateway_handler"))
        node_ready = self._device_has_varac_local_config(int(membership.get("device_profile_id", 0) or 0))
        if enabled and not node_ready:
            text = (
                f"{device_name} is enabled in {cluster_name}, but the radio does not yet have enough device-local VarAC setup to participate confidently. "
                "Review that radio's VarAC paths in Radio Profiles."
            )
            level = "warning"
        else:
            role = "gateway handler" if is_gateway else "member"
            text = (
                f"{device_name} is configured as {role} in {cluster_name} on instance {instance_number}. "
                f"Membership is {'enabled' if enabled else 'disabled'}."
            )
            level = "success" if enabled else "info"
        self._set_guidance_card_state(
            self.varac_memberships_guidance_card,
            self.varac_memberships_guidance_title_label,
            self.varac_memberships_guidance_status_label,
            title=f"{device_name} Membership Guidance",
            text=text,
            level=level,
        )

    def _update_device_profile_action_buttons(self) -> None:
        if not hasattr(self, "add_device_profile_btn"):
            return
        theme = resolve_theme(self.settings)
        selected = self._selected_device_profiles()
        count = len(selected)
        selected_assignment_rows = self._selected_device_profiles_as_assignment_rows()
        has_enabled_profile = any(
            isinstance(row, dict) and int(row.get("enabled", 1) or 0) == 1 for row in self.operating_profiles
        )
        can_edit = count == 1
        can_activate = count > 0 and any(int(row.get("runtime_active", 0) or 0) != 1 for row in selected)
        can_deactivate = count > 0 and all(int(row.get("runtime_primary", 0) or 0) != 1 for row in selected) and any(
            int(row.get("runtime_active", 0) or 0) == 1 for row in selected
        )
        can_set_active = (
            count == 1
            and int(selected[0].get("runtime_primary", 0) or 0) != 1
            and str(selected[0].get("device_class", "") or "").strip().lower() != "observer"
        )
        can_delete = count > 0 and all(int(row.get("runtime_active", 0) or 0) != 1 for row in selected)
        can_assign_schedule = count > 0 and has_enabled_profile
        can_restore_schedule = count > 0 and any(
            str(row.get("assignment_state", "") or "").strip().lower() == "temporary_override"
            or str(row.get("operating_system_key", "") or "").strip() != "default_operating"
            for row in selected_assignment_rows
        )

        self.add_device_profile_btn.setStyleSheet(button_style("primary", theme))
        if hasattr(self, "copy_readiness_summary_btn"):
            self.copy_readiness_summary_btn.setStyleSheet(button_style("secondary", theme))
        self.edit_device_profile_btn.setEnabled(can_edit)
        self.edit_device_profile_btn.setStyleSheet(button_style("info" if can_edit else "muted", theme))
        self.activate_device_profile_btn.setEnabled(can_activate)
        self.activate_device_profile_btn.setStyleSheet(button_style("info" if can_activate else "muted", theme))
        self.deactivate_device_profile_btn.setEnabled(can_deactivate)
        self.deactivate_device_profile_btn.setStyleSheet(button_style("warning" if can_deactivate else "muted", theme))
        self.assign_radio_schedule_btn.setEnabled(can_assign_schedule)
        self.assign_radio_schedule_btn.setStyleSheet(button_style("info" if can_assign_schedule else "muted", theme))
        self.restore_radio_schedule_btn.setEnabled(can_restore_schedule)
        self.restore_radio_schedule_btn.setStyleSheet(button_style("warning" if can_restore_schedule else "muted", theme))
        self.set_active_device_profile_btn.setEnabled(can_set_active)
        self.set_active_device_profile_btn.setStyleSheet(button_style("info" if can_set_active else "muted", theme))
        self.delete_device_profile_btn.setEnabled(can_delete)
        self.delete_device_profile_btn.setStyleSheet(button_style("warning" if can_delete else "muted", theme))

    def _refresh_device_profiles_table(self, *, refresh_section_titles: bool = True) -> None:
        if not hasattr(self, "device_profiles_table"):
            return
        table = self.device_profiles_table
        try:
            self.device_profiles = list(self.multi_radio_store.list_device_profiles())
        except Exception:
            log.exception("Failed loading device profiles from store.")
            self.device_profiles = []
        effective_assignments = self._effective_assignment_map()
        operating_profiles_by_id = {
            int(row.get("id", 0) or 0): dict(row)
            for row in self.operating_profiles
            if isinstance(row, dict)
        }
        readiness_report = self._current_station_readiness_report()
        self._device_profiles_table_loading = True
        try:
            table.setRowCount(0)
            for profile in self.device_profiles:
                row = table.rowCount()
                table.insertRow(row)
                profile_id = int(profile.get("id", 0) or 0)

                sel_chk = QCheckBox()
                sel_chk.setFixedWidth(22)
                sel_chk.setProperty("device_profile_id", profile_id)
                sel_chk.stateChanged.connect(self._update_device_profile_action_buttons)
                sel_wrap = QWidget()
                sel_layout = QHBoxLayout(sel_wrap)
                sel_layout.setContentsMargins(0, 0, 0, 0)
                sel_layout.setAlignment(Qt.AlignCenter)
                sel_layout.addWidget(sel_chk)
                table.setCellWidget(row, 0, sel_wrap)

                active_item = QTableWidgetItem("Yes" if int(profile.get("runtime_active", 0) or 0) == 1 else "")
                active_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 1, active_item)

                primary_item = QTableWidgetItem("Yes" if int(profile.get("runtime_primary", 0) or 0) == 1 else "")
                primary_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 2, primary_item)

                name_item = QTableWidgetItem(str(profile.get("name", "") or ""))
                name_item.setData(Qt.UserRole, profile_id)
                table.setItem(row, 3, name_item)

                table.setItem(row, 4, QTableWidgetItem(self._device_radio_model_summary(profile)))

                backend = str(profile.get("control_backend", "") or "")
                backend_item = QTableWidgetItem(self._device_backend_label(backend))
                if backend.strip().lower() == "rigctld":
                    backend_item.setToolTip("Active RIGCTLD profiles use the configured TCP endpoint.")
                table.setItem(row, 5, backend_item)
                table.setItem(row, 6, QTableWidgetItem(self._device_deployment_label(str(profile.get("deployment_mode", "") or ""))))
                table.setItem(row, 7, QTableWidgetItem(self._device_software_summary(profile)))
                table.setItem(row, 8, QTableWidgetItem(self._device_endpoint_summary(profile)))
                assignment = effective_assignments.get(profile_id, {})
                assigned_profile = operating_profiles_by_id.get(int(assignment.get("operating_profile_id", 0) or 0), {})
                assigned_name = str((assigned_profile or assignment).get("operating_profile_name", "") or (assigned_profile or {}).get("name", "") or "Unassigned").strip() or "Unassigned"
                if assigned_name != "Unassigned":
                    assignment_state = str(assignment.get("assignment_state", "") or "").strip().lower()
                    assigned_name = self._assignment_display_text(assigned_name, assignment_state)
                table.setItem(row, 9, QTableWidgetItem(assigned_name))
                readiness_item = QTableWidgetItem(self._device_readiness_summary(profile, readiness_report))
                readiness_item.setTextAlignment(Qt.AlignCenter)
                radio_issues = [
                    issue
                    for issue in readiness_report.issues
                    if int(issue.radio_id or 0) == profile_id
                ]
                if radio_issues:
                    readiness_item.setToolTip(
                        "\n".join(
                            format_readiness_issue(issue)
                            for issue in radio_issues[:6]
                        )
                    )
                table.setItem(row, 10, readiness_item)
                table.setItem(row, 11, QTableWidgetItem("Opt-in" if self._radio_profile_launch_opt_in_enabled(profile) else "Off"))
                ptt_item = QTableWidgetItem(self._device_ptt_group_label(profile.get("ptt_group", "")))
                ptt_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 12, ptt_item)
                class_item = QTableWidgetItem(self._device_class_label(str(profile.get("device_class", "") or "")))
                class_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 13, class_item)
                table.setItem(row, 14, QTableWidgetItem(str(profile.get("notes", "") or "")))
        finally:
            self._device_profiles_table_loading = False
        self._fit_table_height_to_rows(table, min_rows=1, max_rows=8, extra_rows=1)
        self._ensure_settings_radio_focus_id()
        if table.rowCount() > 0:
            self._sync_device_profiles_table_to_settings_focus()
        self._rebuild_status_indicators()
        self._rebuild_device_profile_selector()
        self._update_device_profiles_hint()
        self._refresh_multi_rig_status_card()
        self._update_device_profile_action_buttons()
        self._update_device_profile_readiness_detail(readiness_report)
        self._refresh_radio_specific_section_visibility()
        self._refresh_launch_control_guidance()
        self._refresh_section_nav_health()
        if refresh_section_titles:
            self._refresh_section_titles()

    def _refresh_multi_radio_tables(self, *, refresh_section_titles: bool = True) -> None:
        self._refresh_device_profiles_table(refresh_section_titles=refresh_section_titles)
        self._refresh_operating_profiles_table(
            refresh_assignments=True,
            refresh_section_titles=refresh_section_titles,
        )
        self._refresh_varac_clusters_table(
            refresh_memberships=True,
            refresh_section_titles=refresh_section_titles,
        )
        self._refresh_software_radio_selector(preserve_current=True)

    def _summary_varac_clusters(self) -> str:
        if not self._varac_cluster_mode_enabled():
            return "Cluster mode off"
        rows = [row for row in self.varac_clusters if isinstance(row, dict)]
        count = len(rows)
        members = sum(int(row.get("enabled_member_count", 0) or 0) for row in rows)
        gateway_count = len([row for row in rows if row.get("gateway_handler_device_id") not in (None, "", 0)])
        if count <= 0:
            return "No VarAC clusters"
        return (
            f"{count} cluster{'s' if count != 1 else ''}, "
            f"{members} member{'s' if members != 1 else ''}, "
            f"{gateway_count} gateway"
        )

    def _summary_varac_memberships(self) -> str:
        if not self._varac_cluster_mode_enabled():
            return "Cluster mode off"
        rows = [row for row in self.varac_cluster_members if isinstance(row, dict)]
        if not rows:
            return "No VarAC memberships"
        enabled_count = len([row for row in rows if int(row.get("enabled", 1) or 0) == 1])
        gateway_count = len([row for row in rows if bool(row.get("is_gateway_handler"))])
        return f"{enabled_count}/{len(rows)} enabled, {gateway_count} gateway handler{'s' if gateway_count != 1 else ''}"

    def _update_varac_clusters_hint(self) -> None:
        label = getattr(self, "varac_clusters_hint_label", None)
        if label is None:
            return
        if not self._varac_cluster_mode_enabled():
            label.setText(
                "Cluster mode is off. Enable Cluster Mode in VarAC Settings to reveal shared VarAC cluster definitions for coordinated multi-radio use."
            )
            return
        if not self.varac_clusters:
            label.setText(
                "Define shared VarAC clusters here. A cluster holds the shared DB identity, optional shared DB path, "
                "and the designated gateway handler for its enabled members."
            )
            return
        missing_gateway = [
            row
            for row in self.varac_clusters
            if isinstance(row, dict)
            and int(row.get("enabled_member_count", 0) or 0) > 1
            and row.get("gateway_handler_device_id") in (None, "", 0)
        ]
        if missing_gateway:
            names = ", ".join(str(row.get("name", "") or "Cluster").strip() for row in missing_gateway[:3])
            label.setText(
                f"Clusters with more than one enabled member should select one gateway handler. Pending handler selection: {names}."
            )
            return
        label.setText(
            "Define shared VarAC clusters here. Edit a cluster to choose its gateway handler from the enabled "
            "members already assigned to that cluster."
        )

    def _update_varac_memberships_hint(self) -> None:
        label = getattr(self, "varac_members_hint_label", None)
        if label is None:
            return
        if not self._varac_cluster_mode_enabled():
            label.setText(
                "Cluster mode is off. Enable Cluster Mode in VarAC Settings before assigning radios to VarAC clusters."
            )
            return
        if not self.varac_cluster_members:
            label.setText(
                "Assign device profiles to VarAC clusters here. Each device may hold one enabled cluster membership in this phase."
            )
            return
        missing_node_config = [
            row
            for row in self.varac_cluster_members
            if isinstance(row, dict)
            and int(row.get("enabled", 1) or 0) == 1
            and not any(
                str(device.get(key, "") or "").strip()
                for device in self.device_profiles
                if isinstance(device, dict) and int(device.get("id", 0) or 0) == int(row.get("device_profile_id", 0) or 0)
                for key in ("varac_install_path", "varac_db_path", "varac_ini_path", "launch_cmd")
            )
        ]
        if missing_node_config:
            names = ", ".join(str(row.get("device_name", "") or "Device").strip() for row in missing_node_config[:3])
            label.setText(
                f"Enabled cluster memberships should also have device-local VarAC settings. Review: {names}."
            )
            return
        label.setText(
            "Assign device profiles to VarAC clusters here. Instance numbers must be unique within each cluster, "
            "and the gateway handler must be one of that cluster's enabled members."
        )

    def _selected_varac_cluster_ids(self) -> List[int]:
        rows: List[int] = []
        if not hasattr(self, "varac_clusters_table"):
            return rows
        for row in range(self.varac_clusters_table.rowCount()):
            wrapper = self.varac_clusters_table.cellWidget(row, 0)
            chk = wrapper.findChild(QCheckBox) if wrapper is not None else None
            if chk is None or not chk.isChecked():
                continue
            try:
                rows.append(int(chk.property("varac_cluster_id")))
            except Exception:
                continue
        return rows

    def _selected_varac_clusters(self) -> List[Dict[str, Any]]:
        selected_ids = set(self._selected_varac_cluster_ids())
        return [
            dict(row)
            for row in self.varac_clusters
            if isinstance(row, dict) and int(row.get("id", 0) or 0) in selected_ids
        ]

    def _selected_varac_memberships(self) -> List[Dict[str, Any]]:
        selected_pairs: set[tuple[int, int]] = set()
        if not hasattr(self, "varac_members_table"):
            return []
        for row in range(self.varac_members_table.rowCount()):
            wrapper = self.varac_members_table.cellWidget(row, 0)
            chk = wrapper.findChild(QCheckBox) if wrapper is not None else None
            if chk is None or not chk.isChecked():
                continue
            try:
                selected_pairs.add(
                    (
                        int(chk.property("varac_cluster_id") or 0),
                        int(chk.property("device_profile_id") or 0),
                    )
                )
            except Exception:
                continue
        return [
            dict(row)
            for row in self.varac_cluster_members
            if isinstance(row, dict)
            and (
                int(row.get("cluster_db_id", 0) or 0),
                int(row.get("device_profile_id", 0) or 0),
            )
            in selected_pairs
        ]

    def _update_varac_cluster_action_buttons(self) -> None:
        if not hasattr(self, "add_varac_cluster_btn"):
            return
        theme = resolve_theme(self.settings)
        selected = self._selected_varac_clusters()
        count = len(selected)
        can_edit = count == 1
        can_delete = count > 0
        self.add_varac_cluster_btn.setStyleSheet(button_style("primary", theme))
        self.edit_varac_cluster_btn.setEnabled(can_edit)
        self.edit_varac_cluster_btn.setStyleSheet(button_style("info" if can_edit else "muted", theme))
        self.delete_varac_cluster_btn.setEnabled(can_delete)
        self.delete_varac_cluster_btn.setStyleSheet(button_style("warning" if can_delete else "muted", theme))

    def _update_varac_membership_action_buttons(self) -> None:
        if not hasattr(self, "add_varac_membership_btn"):
            return
        theme = resolve_theme(self.settings)
        selected = self._selected_varac_memberships()
        can_remove = len(selected) > 0
        self.add_varac_membership_btn.setStyleSheet(button_style("primary", theme))
        self.remove_varac_membership_btn.setEnabled(can_remove)
        self.remove_varac_membership_btn.setStyleSheet(button_style("warning" if can_remove else "muted", theme))

    def _refresh_varac_clusters_table(
        self,
        *,
        refresh_memberships: bool = True,
        refresh_section_titles: bool = True,
    ) -> None:
        if not hasattr(self, "varac_clusters_table"):
            return
        if not self._varac_cluster_mode_enabled():
            self.varac_clusters = []
            self.varac_cluster_members = []
            self.varac_clusters_table.setRowCount(0)
            if hasattr(self, "varac_members_table"):
                self.varac_members_table.setRowCount(0)
            self._update_varac_clusters_hint()
            self._update_varac_memberships_hint()
            self._update_varac_cluster_action_buttons()
            self._update_varac_cluster_guidance_detail()
            self._update_varac_membership_guidance_detail()
            if refresh_section_titles:
                self._refresh_section_titles()
            return
        table = self.varac_clusters_table
        try:
            self.varac_clusters = list(self.multi_radio_store.list_varac_clusters())
        except Exception:
            log.exception("Failed loading VarAC clusters from store.")
            self.varac_clusters = []
        self._varac_clusters_table_loading = True
        try:
            table.setRowCount(0)
            for cluster in self.varac_clusters:
                row = table.rowCount()
                table.insertRow(row)
                cluster_id = int(cluster.get("id", 0) or 0)
                sel_chk = QCheckBox()
                sel_chk.setFixedWidth(22)
                sel_chk.setProperty("varac_cluster_id", cluster_id)
                sel_chk.stateChanged.connect(self._update_varac_cluster_action_buttons)
                sel_wrap = QWidget()
                sel_layout = QHBoxLayout(sel_wrap)
                sel_layout.setContentsMargins(0, 0, 0, 0)
                sel_layout.setAlignment(Qt.AlignCenter)
                sel_layout.addWidget(sel_chk)
                table.setCellWidget(row, 0, sel_wrap)
                name_item = QTableWidgetItem(str(cluster.get("name", "") or ""))
                name_item.setData(Qt.UserRole, cluster_id)
                table.setItem(row, 1, name_item)
                table.setItem(row, 2, QTableWidgetItem(str(cluster.get("cluster_id", "") or "")))
                table.setItem(row, 3, QTableWidgetItem(str(cluster.get("shared_db_path", "") or "")))
                table.setItem(
                    row,
                    4,
                    QTableWidgetItem(f"{int(cluster.get('enabled_member_count', 0) or 0)}/{int(cluster.get('member_count', 0) or 0)}"),
                )
                table.setItem(row, 5, QTableWidgetItem(str(cluster.get("gateway_handler_name", "") or "")))
                table.setItem(row, 6, QTableWidgetItem("On" if int(cluster.get("ptt_lock_enabled", 0) or 0) == 1 else "Off"))
                table.setItem(row, 7, QTableWidgetItem(f"{int(cluster.get('counters_refresh_sec', 30) or 30)}s"))
        finally:
            self._varac_clusters_table_loading = False
        if table.rowCount() > 0 and table.currentRow() < 0:
            table.selectRow(0)
        self._update_varac_clusters_hint()
        self._update_varac_cluster_action_buttons()
        self._update_varac_cluster_guidance_detail()
        if refresh_memberships and hasattr(self, "varac_members_table"):
            self._refresh_varac_memberships_table(refresh_section_titles=False)
        if refresh_section_titles:
            self._refresh_section_titles()

    def _refresh_varac_memberships_table(self, *, refresh_section_titles: bool = True) -> None:
        if not hasattr(self, "varac_members_table"):
            return
        if not self._varac_cluster_mode_enabled():
            self.varac_cluster_members = []
            self.varac_members_table.setRowCount(0)
            self._update_varac_memberships_hint()
            self._update_varac_membership_action_buttons()
            self._update_varac_membership_guidance_detail()
            if refresh_section_titles:
                self._refresh_section_titles()
            return
        table = self.varac_members_table
        try:
            self.varac_cluster_members = list(self.multi_radio_store.list_varac_cluster_members())
        except Exception:
            log.exception("Failed loading VarAC cluster memberships from store.")
            self.varac_cluster_members = []
        self._varac_cluster_members_table_loading = True
        try:
            table.setRowCount(0)
            for membership in self.varac_cluster_members:
                row = table.rowCount()
                table.insertRow(row)
                cluster_id = int(membership.get("cluster_db_id", 0) or 0)
                device_id = int(membership.get("device_profile_id", 0) or 0)
                sel_chk = QCheckBox()
                sel_chk.setFixedWidth(22)
                sel_chk.setProperty("varac_cluster_id", cluster_id)
                sel_chk.setProperty("device_profile_id", device_id)
                sel_chk.stateChanged.connect(self._update_varac_membership_action_buttons)
                sel_wrap = QWidget()
                sel_layout = QHBoxLayout(sel_wrap)
                sel_layout.setContentsMargins(0, 0, 0, 0)
                sel_layout.setAlignment(Qt.AlignCenter)
                sel_layout.addWidget(sel_chk)
                table.setCellWidget(row, 0, sel_wrap)
                table.setItem(row, 1, QTableWidgetItem(str(membership.get("cluster_name", "") or "")))
                table.setItem(row, 2, QTableWidgetItem(str(membership.get("device_name", "") or "")))
                runtime_label = "Primary" if int(membership.get("runtime_primary", 0) or 0) == 1 else ("Active" if int(membership.get("runtime_active", 0) or 0) == 1 else "")
                runtime_item = QTableWidgetItem(runtime_label)
                runtime_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 3, runtime_item)
                table.setItem(row, 4, QTableWidgetItem(self._device_class_label(str(membership.get("device_class", "") or ""))))
                instance_item = QTableWidgetItem(str(int(membership.get("instance_number", 0) or 0)))
                instance_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 5, instance_item)
                enabled_item = QTableWidgetItem("Yes" if int(membership.get("enabled", 1) or 0) == 1 else "")
                enabled_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 6, enabled_item)
                table.setItem(row, 7, QTableWidgetItem("Handler" if bool(membership.get("is_gateway_handler")) else "Member"))
        finally:
            self._varac_cluster_members_table_loading = False
        if table.rowCount() > 0 and table.currentRow() < 0:
            table.selectRow(0)
        self._update_varac_memberships_hint()
        self._update_varac_membership_action_buttons()
        self._update_varac_clusters_hint()
        self._update_varac_membership_guidance_detail()
        if refresh_section_titles:
            self._refresh_section_titles()

    # ---------- Frequency Plans / Assigned Plans ---------- #

    def _summary_operating_profiles(self) -> str:
        count = len(self.operating_profiles)
        enabled_count = len(
            [row for row in self.operating_profiles if isinstance(row, dict) and int(row.get("enabled", 1) or 0) == 1]
        )
        if count <= 0:
            return "No frequency plans"
        return f"{count} frequency plan{'s' if count != 1 else ''}, {enabled_count} enabled"

    def _summary_device_assignments(self) -> str:
        rows = [row for row in self.device_assignments if isinstance(row, dict)]
        if not rows:
            return "No assigned plans"
        overrides = len(
            [row for row in rows if str(row.get("assignment_state", "") or "").strip().lower() == "temporary_override"]
        )
        assigned = len([row for row in rows if row.get("operating_profile_id") not in (None, "", 0)])
        summary = f"{assigned} assigned"
        if overrides:
            summary += f", {overrides} temporary override{'s' if overrides != 1 else ''}"
        if isinstance(self.active_profile_swap, dict) and self.active_profile_swap:
            summary += ", swap active"
        return summary

    def _operating_profile_by_id(self, operating_profile_id: int) -> Optional[Dict[str, Any]]:
        for row in self.operating_profiles:
            if not isinstance(row, dict):
                continue
            if int(row.get("id", 0) or 0) == int(operating_profile_id):
                return dict(row)
        return None

    @staticmethod
    def _assignment_state_label(state: str) -> str:
        normalized = str(state or "").strip().lower()
        labels = {
            "active": "Active",
            "inactive": "Inactive",
            "scheduled": "Scheduled",
            "superseded": "Superseded",
            "temporary_override": "Temporary Override",
            "unassigned": "Unassigned",
        }
        if normalized in labels:
            return labels[normalized]
        return normalized.replace("_", " ").title() if normalized else "Unassigned"

    @staticmethod
    def _assignment_display_text(name: object, state: object = "") -> str:
        assignment_name = str(name or "").strip() or "Unassigned"
        if assignment_name.lower() == "unassigned":
            return "Unassigned"
        return f"{assignment_name} ({SettingsTab._assignment_state_label(str(state or ''))})"

    @staticmethod
    def _scheduler_mode_label(raw: object) -> str:
        value = str(raw or "full").strip().lower() or "full"
        for label, option_value in OPERATING_SCHEDULER_MODE_OPTIONS:
            if option_value == value:
                return label
        return value.replace("_", " ").title()

    @staticmethod
    def _operating_profile_shell_summary(profile: Optional[Dict[str, Any]]) -> str:
        if not isinstance(profile, dict):
            return "Unassigned"
        bits: List[str] = []
        if int(profile.get("receive_only", 0) or 0) == 1:
            bits.append("Receive-only")
        if int(profile.get("scheduler_enabled", 1) or 0) != 1:
            bits.append("Scheduler Off")
        if int(profile.get("use_messages", 1) or 0) != 1:
            bits.append("Messages Off")
        if int(profile.get("use_map", 1) or 0) != 1:
            bits.append("Map Off")
        if int(profile.get("use_background_ingest", 1) or 0) != 1:
            bits.append("Ingest Off")
        if int(profile.get("use_launch_control", 0) or 0) != 1:
            bits.append("Launch Off")
        if int(profile.get("use_net_control_tabs", 1) or 0) != 1:
            bits.append("NetCtrl Off")
        if not bits:
            return "Full FIO Workflow"
        return ", ".join(bits)

    def _selected_operating_profile_ids(self) -> List[int]:
        if not hasattr(self, "operating_profiles_table"):
            return []
        selected: List[int] = []
        for row in range(self.operating_profiles_table.rowCount()):
            wrapper = self.operating_profiles_table.cellWidget(row, 0)
            chk = wrapper.findChild(QCheckBox) if wrapper is not None else None
            if chk is None or not chk.isChecked():
                continue
            try:
                selected.append(int(chk.property("operating_profile_id") or 0))
            except Exception:
                continue
        return selected

    def _selected_operating_profiles(self) -> List[Dict[str, Any]]:
        selected_ids = set(self._selected_operating_profile_ids())
        return [
            dict(row)
            for row in self.operating_profiles
            if isinstance(row, dict) and int(row.get("id", 0) or 0) in selected_ids
        ]

    def _selected_assignment_rows(self) -> List[Dict[str, Any]]:
        if not hasattr(self, "device_assignments_table"):
            return []
        selected: List[Dict[str, Any]] = []
        for row in range(self.device_assignments_table.rowCount()):
            wrapper = self.device_assignments_table.cellWidget(row, 0)
            chk = wrapper.findChild(QCheckBox) if wrapper is not None else None
            if chk is None or not chk.isChecked():
                continue
            try:
                device_profile_id = int(chk.property("device_profile_id") or 0)
            except Exception:
                continue
            match = next(
                (
                    dict(item)
                    for item in self.device_assignments
                    if isinstance(item, dict) and int(item.get("device_profile_id", 0) or 0) == device_profile_id
                ),
                None,
            )
            if match is not None:
                selected.append(match)
        return selected

    def _update_operating_profiles_hint(self) -> None:
        if not hasattr(self, "operating_profiles_hint_label"):
            return
        if not self.operating_profiles:
            self.operating_profiles_hint_label.setText("No frequency plans are available.")
            return
        primary_assignment = next(
            (
                row
                for row in self.device_assignments
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            None,
        )
        hint = "Frequency plans define where and when the scheduler should guide the default radio."
        if isinstance(primary_assignment, dict):
            operating_name = str(primary_assignment.get("operating_profile_name", "") or "").strip() or DEFAULT_OPERATING_NAME
            state_label = self._assignment_state_label(str(primary_assignment.get("assignment_state", "") or "active"))
            hint = f"Station default assigned plan: {operating_name} ({state_label}). " + hint
        self.operating_profiles_hint_label.setText(hint)

    def _update_device_assignments_hint(self) -> None:
        if not hasattr(self, "device_assignments_hint_label"):
            return
        active_swap = dict(self.active_profile_swap or {})
        if active_swap:
            source_name = str(active_swap.get("source_device_name", "") or "").strip() or "previous primary"
            target_name = str(active_swap.get("target_device_name", "") or "").strip() or "target radio"
            mode = str(active_swap.get("mode", "") or "").strip().lower()
            if mode == "carry_primary_profile":
                carried_name = (
                    str(active_swap.get("applied_operating_profile_name", "") or "").strip()
                    or "the previous primary frequency plan"
                )
                self.device_assignments_hint_label.setText(
                    f"Temporary plan swap active: {source_name} -> {target_name}. "
                    f"{carried_name} is temporarily applied on the target radio. Restore Swap returns the previous primary and target assignment."
                )
            else:
                self.device_assignments_hint_label.setText(
                    f"Temporary plan swap active: {source_name} -> {target_name}. "
                    "Restore Swap returns the previous primary radio without rewriting endpoint settings."
                )
            return
        rows = [row for row in self.device_assignments if isinstance(row, dict)]
        if not rows:
            self.device_assignments_hint_label.setText("No assigned plans are available.")
            return
        assigned = len([row for row in rows if row.get("operating_profile_id") not in (None, "", 0)])
        overrides = len(
            [row for row in rows if str(row.get("assignment_state", "") or "").strip().lower() == "temporary_override"]
        )
        primary = next((row for row in rows if int(row.get("runtime_primary", 0) or 0) == 1), None)
        hint = f"{assigned} radio{'s' if assigned != 1 else ''} currently have an effective assigned plan."
        if overrides:
            hint += f" {overrides} temporary override{'s are' if overrides != 1 else ' is'} active."
        if isinstance(primary, dict):
            operating_name = str(primary.get("operating_profile_name", "") or "").strip() or DEFAULT_OPERATING_NAME
            hint += f" The Station Default radio is currently using {operating_name}."
        self.device_assignments_hint_label.setText(hint)

    def _update_operating_profile_action_buttons(self) -> None:
        if not hasattr(self, "add_operating_profile_btn"):
            return
        theme = resolve_theme(self.settings)
        selected = self._selected_operating_profiles()
        count = len(selected)
        assigned_ids = {
            int(row.get("operating_profile_id", 0) or 0)
            for row in self.device_assignments
            if isinstance(row, dict) and row.get("operating_profile_id") not in (None, "", 0)
        }
        can_edit = count == 1
        can_delete = count > 0 and all(
            str(row.get("system_key", "") or "").strip() != "default_operating"
            and int(row.get("id", 0) or 0) not in assigned_ids
            for row in selected
        )
        self.add_operating_profile_btn.setStyleSheet(button_style("primary", theme))
        self.edit_operating_profile_btn.setEnabled(can_edit)
        self.edit_operating_profile_btn.setStyleSheet(button_style("info" if can_edit else "muted", theme))
        self.delete_operating_profile_btn.setEnabled(can_delete)
        self.delete_operating_profile_btn.setStyleSheet(button_style("warning" if can_delete else "muted", theme))

    def _update_device_assignment_action_buttons(self) -> None:
        if not hasattr(self, "assign_device_operating_profile_btn"):
            return
        theme = resolve_theme(self.settings)
        selected = self._selected_assignment_rows()
        count = len(selected)
        active_swap = dict(self.active_profile_swap or {})
        swap_device_ids = {
            int(active_swap.get("source_device_id", 0) or 0),
            int(active_swap.get("target_device_id", 0) or 0),
        }
        swap_device_ids.discard(0)
        selection_includes_swap_devices = any(
            int(row.get("device_profile_id", 0) or 0) in swap_device_ids for row in selected
        )
        has_enabled_profile = any(
            isinstance(row, dict) and int(row.get("enabled", 1) or 0) == 1 for row in self.operating_profiles
        )
        can_assign = count > 0 and has_enabled_profile and not selection_includes_swap_devices
        can_restore = count > 0 and not selection_includes_swap_devices and any(
            str(row.get("assignment_state", "") or "").strip().lower() == "temporary_override"
            or str(row.get("operating_system_key", "") or "").strip() != "default_operating"
            for row in selected
        )
        can_swap = (
            not active_swap
            and count == 1
            and int(selected[0].get("runtime_active", 0) or 0) == 1
            and int(selected[0].get("runtime_primary", 0) or 0) != 1
            and str(selected[0].get("device_class", "") or "").strip().lower() != "observer"
        )
        can_restore_swap = bool(active_swap)
        self.assign_device_operating_profile_btn.setEnabled(can_assign)
        self.assign_device_operating_profile_btn.setStyleSheet(button_style("info" if can_assign else "muted", theme))
        self.temporary_profile_swap_btn.setEnabled(can_swap)
        self.temporary_profile_swap_btn.setStyleSheet(button_style("info" if can_swap else "muted", theme))
        self.restore_profile_swap_btn.setEnabled(can_restore_swap)
        self.restore_profile_swap_btn.setStyleSheet(button_style("warning" if can_restore_swap else "muted", theme))
        self.restore_device_operating_profile_btn.setEnabled(can_restore)
        self.restore_device_operating_profile_btn.setStyleSheet(button_style("warning" if can_restore else "muted", theme))

    def _refresh_operating_profiles_table(
        self,
        *,
        refresh_assignments: bool = True,
        refresh_section_titles: bool = True,
    ) -> None:
        if not hasattr(self, "operating_profiles_table"):
            return
        table = self.operating_profiles_table
        try:
            self.operating_profiles = list(self.multi_radio_store.list_operating_profiles())
        except Exception:
            log.exception("Failed loading frequency plans from store.")
            self.operating_profiles = []
        self._operating_profiles_table_loading = True
        try:
            table.setRowCount(0)
            for profile in self.operating_profiles:
                row = table.rowCount()
                table.insertRow(row)
                profile_id = int(profile.get("id", 0) or 0)

                sel_chk = QCheckBox()
                sel_chk.setFixedWidth(22)
                sel_chk.setProperty("operating_profile_id", profile_id)
                sel_chk.stateChanged.connect(self._update_operating_profile_action_buttons)
                sel_wrap = QWidget()
                sel_layout = QHBoxLayout(sel_wrap)
                sel_layout.setContentsMargins(0, 0, 0, 0)
                sel_layout.setAlignment(Qt.AlignCenter)
                sel_layout.addWidget(sel_chk)
                table.setCellWidget(row, 0, sel_wrap)

                enabled_item = QTableWidgetItem("Yes" if int(profile.get("enabled", 1) or 0) == 1 else "")
                enabled_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 1, enabled_item)

                name_item = QTableWidgetItem(str(profile.get("name", "") or ""))
                name_item.setData(Qt.UserRole, profile_id)
                table.setItem(row, 2, name_item)
                table.setItem(row, 3, QTableWidgetItem("On" if int(profile.get("scheduler_enabled", 1) or 0) == 1 else "Off"))
                table.setItem(row, 4, QTableWidgetItem(self._operating_profile_shell_summary(profile)))
                table.setItem(row, 5, QTableWidgetItem(str(profile.get("description", "") or "")))
        finally:
            self._operating_profiles_table_loading = False
        self._fit_table_height_to_rows(table, min_rows=1, max_rows=8, extra_rows=1)
        self._refresh_fit_content_section_height(getattr(self, "operating_profiles_section_group", None))
        if table.rowCount() > 0 and table.currentRow() < 0:
            table.selectRow(0)
        self._update_operating_profiles_hint()
        self._update_operating_profile_action_buttons()
        self._update_operating_profile_guidance_detail()
        if refresh_assignments:
            self._refresh_device_assignments_table(refresh_section_titles=False)
        if refresh_section_titles:
            self._refresh_section_titles()

    def _refresh_device_assignments_table(self, *, refresh_section_titles: bool = True) -> None:
        if not hasattr(self, "device_assignments_table"):
            return
        table = self.device_assignments_table
        try:
            if not self.device_profiles:
                self.device_profiles = list(self.multi_radio_store.list_device_profiles())
            if not self.operating_profiles:
                self.operating_profiles = list(self.multi_radio_store.list_operating_profiles())
            effective_assignments = {
                int(row.get("device_profile_id", 0) or 0): dict(row)
                for row in self.multi_radio_store.list_effective_assignments()
                if isinstance(row, dict)
            }
        except Exception:
            log.exception("Failed loading radio schedule assignments from store.")
            effective_assignments = {}
        try:
            active_swap = self.multi_radio_store.get_active_profile_swap()
        except Exception:
            log.exception("Failed loading active profile swap from store.")
            active_swap = None
        self.active_profile_swap = dict(active_swap) if isinstance(active_swap, dict) else None

        profiles = {
            int(row.get("id", 0) or 0): dict(row)
            for row in self.operating_profiles
            if isinstance(row, dict)
        }
        rows: List[Dict[str, Any]] = []
        for device in self.device_profiles:
            if not isinstance(device, dict):
                continue
            device_id = int(device.get("id", 0) or 0)
            assignment = effective_assignments.get(device_id, {})
            operating_id = assignment.get("operating_profile_id")
            operating = profiles.get(int(operating_id or 0)) if operating_id not in (None, "") else None
            rows.append(
                {
                    "device_profile_id": device_id,
                    "device_name": str(device.get("name", "") or ""),
                    "runtime_active": int(device.get("runtime_active", 0) or 0),
                    "runtime_primary": int(device.get("runtime_primary", 0) or 0),
                    "device_class": str(device.get("device_class", "") or ""),
                    "endpoint_summary": self._device_endpoint_summary(device),
                    "operating_profile_id": int(operating_id or 0) if operating_id not in (None, "") else None,
                    "operating_profile_name": str((operating or {}).get("name", "") or ""),
                    "operating_system_key": str((operating or {}).get("system_key", "") or ""),
                    "assignment_state": str(assignment.get("assignment_state", "") or ""),
                    "shell_summary": self._operating_profile_shell_summary(operating),
                }
            )
        self.device_assignments = rows

        self._device_assignments_table_loading = True
        try:
            table.setRowCount(0)
            for row_data in self.device_assignments:
                row = table.rowCount()
                table.insertRow(row)
                device_profile_id = int(row_data.get("device_profile_id", 0) or 0)

                sel_chk = QCheckBox()
                sel_chk.setFixedWidth(22)
                sel_chk.setProperty("device_profile_id", device_profile_id)
                sel_chk.stateChanged.connect(self._update_device_assignment_action_buttons)
                sel_wrap = QWidget()
                sel_layout = QHBoxLayout(sel_wrap)
                sel_layout.setContentsMargins(0, 0, 0, 0)
                sel_layout.setAlignment(Qt.AlignCenter)
                sel_layout.addWidget(sel_chk)
                table.setCellWidget(row, 0, sel_wrap)

                active_item = QTableWidgetItem("Yes" if int(row_data.get("runtime_active", 0) or 0) == 1 else "")
                active_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 1, active_item)

                primary_item = QTableWidgetItem("Yes" if int(row_data.get("runtime_primary", 0) or 0) == 1 else "")
                primary_item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 2, primary_item)
                device_item = QTableWidgetItem(str(row_data.get("device_name", "") or ""))
                device_item.setData(Qt.UserRole, device_profile_id)
                table.setItem(row, 3, device_item)
                table.setItem(row, 4, QTableWidgetItem(str(row_data.get("operating_profile_name", "") or "Unassigned")))
                table.setItem(row, 5, QTableWidgetItem(self._assignment_state_label(str(row_data.get("assignment_state", "") or ""))))
                table.setItem(row, 6, QTableWidgetItem(str(row_data.get("shell_summary", "") or "")))
                table.setItem(row, 7, QTableWidgetItem(str(row_data.get("endpoint_summary", "") or "")))
        finally:
            self._device_assignments_table_loading = False
        self._fit_table_height_to_rows(table, min_rows=1, max_rows=8, extra_rows=1)
        if table.rowCount() > 0 and table.currentRow() < 0:
            table.selectRow(0)
        self._update_device_assignments_hint()
        self._refresh_radio_context_labels()
        self._update_device_assignment_action_buttons()
        self._update_device_assignments_guidance_detail()
        if refresh_section_titles:
            self._refresh_section_titles()

    def _open_operating_profile_dialog(self, existing: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        dlg = QDialog(self)
        dlg.setWindowTitle("Edit Frequency Plan" if existing else "Add Frequency Plan")
        dlg.resize(560, 0)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        layout.addLayout(form)

        name_edit = QLineEdit(str((existing or {}).get("name", "") or ""))
        form.addRow("Name:", name_edit)

        enabled_chk = QCheckBox("Plan Enabled")
        enabled_chk.setChecked(bool((existing or {}).get("enabled", 1)))
        form.addRow("", enabled_chk)

        description_edit = QPlainTextEdit(str((existing or {}).get("description", "") or ""))
        description_edit.setFixedHeight(80)
        form.addRow("Description:", description_edit)

        scheduler_enabled_chk = QCheckBox("Enable scheduler automation when primary")
        scheduler_enabled_chk.setChecked(bool((existing or {}).get("scheduler_enabled", 1)))
        form.addRow("", scheduler_enabled_chk)

        scheduler_mode_combo = QComboBox()
        for label, value in OPERATING_SCHEDULER_MODE_OPTIONS:
            scheduler_mode_combo.addItem(label, value)
        current_mode = str((existing or {}).get("scheduler_mode", "full") or "full").strip().lower()
        mode_idx = scheduler_mode_combo.findData(current_mode)
        scheduler_mode_combo.setCurrentIndex(mode_idx if mode_idx >= 0 else 0)
        self._fit_combo_to_contents(scheduler_mode_combo, minimum=220)
        form.addRow("Plan Behavior:", scheduler_mode_combo)

        preferred_bands_edit = QLineEdit(
            self._preferred_band_text(
                (existing or {}).get("preferred_band_set_json", (existing or {}).get("preferred_band_set", ""))
            )
        )
        preferred_bands_edit.setPlaceholderText("Optional preferred bands, e.g. 40M, 80M")
        form.addRow("Preferred Bands:", preferred_bands_edit)

        use_messages_chk = QCheckBox("Use Messages")
        use_messages_chk.setChecked(bool((existing or {}).get("use_messages", 1)))
        form.addRow("", use_messages_chk)

        use_map_chk = QCheckBox("Use Map")
        use_map_chk.setChecked(bool((existing or {}).get("use_map", 1)))
        form.addRow("", use_map_chk)

        use_background_ingest_chk = QCheckBox("Use background ingest")
        use_background_ingest_chk.setChecked(bool((existing or {}).get("use_background_ingest", 1)))
        form.addRow("", use_background_ingest_chk)

        use_launch_control_chk = QCheckBox("Use Launch Control")
        use_launch_control_chk.setChecked(bool((existing or {}).get("use_launch_control", 0)))
        form.addRow("", use_launch_control_chk)

        use_net_control_tabs_chk = QCheckBox("Use net control tabs")
        use_net_control_tabs_chk.setChecked(bool((existing or {}).get("use_net_control_tabs", 1)))
        form.addRow("", use_net_control_tabs_chk)

        receive_only_chk = QCheckBox("Receive-only plan (observer / SDR compatible)")
        receive_only_chk.setChecked(bool((existing or {}).get("receive_only", 0)))
        form.addRow("", receive_only_chk)

        allow_profile_swap_chk = QCheckBox("Allow assigned plan swap coordination")
        allow_profile_swap_chk.setChecked(bool((existing or {}).get("allow_profile_swap", 0)))
        form.addRow("", allow_profile_swap_chk)

        info_label = QLabel()
        info_label.setWordWrap(True)
        form.addRow("", info_label)

        def _update_hint() -> None:
            disabled: List[str] = []
            if not scheduler_enabled_chk.isChecked():
                disabled.append("scheduler automation")
            if not use_messages_chk.isChecked():
                disabled.append("Messages")
            if not use_map_chk.isChecked():
                disabled.append("Map")
            if not use_background_ingest_chk.isChecked():
                disabled.append("background ingest")
            if not use_launch_control_chk.isChecked():
                disabled.append("Launch Control")
            if not use_net_control_tabs_chk.isChecked():
                disabled.append("net control tabs")
            prefix = (
                "Receive-only plan: can be assigned to observer / SDR radios and should not be used as a transmit target. "
                if receive_only_chk.isChecked()
                else "Transmit-capable plan: assign to transmit/receive radios. "
            )
            if disabled:
                info_label.setText(
                    prefix
                    + "When this frequency plan is assigned to the Station Default radio, it suppresses: "
                    + ", ".join(disabled)
                    + "."
                )
            else:
                info_label.setText(
                    prefix
                    + "This frequency plan leaves the current Station Default compatibility shell fully enabled."
                )

        for chk in (
            scheduler_enabled_chk,
            use_messages_chk,
            use_map_chk,
            use_background_ingest_chk,
            use_launch_control_chk,
            use_net_control_tabs_chk,
            receive_only_chk,
        ):
            chk.toggled.connect(_update_hint)
        _update_hint()

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        layout.addWidget(buttons)

        out: Dict[str, Any] = {}

        def _save() -> None:
            name = name_edit.text().strip()
            if not name:
                QMessageBox.warning(self, "Validation", "Frequency plan name is required.")
                return
            out.update(
                {
                    "id": (existing or {}).get("id"),
                    "system_key": (existing or {}).get("system_key"),
                    "name": name,
                    "enabled": bool(enabled_chk.isChecked()),
                    "description": description_edit.toPlainText().strip(),
                    "scheduler_enabled": bool(scheduler_enabled_chk.isChecked()),
                    "scheduler_mode": str(scheduler_mode_combo.currentData() or "full"),
                    "preferred_band_set": preferred_bands_edit.text().strip(),
                    "use_messages": bool(use_messages_chk.isChecked()),
                    "use_map": bool(use_map_chk.isChecked()),
                    "use_background_ingest": bool(use_background_ingest_chk.isChecked()),
                    "use_launch_control": bool(use_launch_control_chk.isChecked()),
                    "use_net_control_tabs": bool(use_net_control_tabs_chk.isChecked()),
                    "receive_only": bool(receive_only_chk.isChecked()),
                    "allow_profile_swap": bool(allow_profile_swap_chk.isChecked()),
                }
            )
            dlg.accept()

        buttons.accepted.connect(_save)
        buttons.rejected.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out if out else None

    def _persist_operating_profile(self, values: Dict[str, Any]) -> None:
        try:
            self.multi_radio_store.save_operating_profile(values)
        except ValueError as exc:
            QMessageBox.warning(self, "Frequency Plans", str(exc))
            return
        except Exception:
            log.exception("Failed to save operating profile.")
            QMessageBox.warning(self, "Frequency Plans", "Unable to save the frequency plan.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _add_operating_profile(self) -> None:
        created = self._open_operating_profile_dialog(existing=None)
        if not created:
            return
        self._persist_operating_profile(created)

    def _edit_operating_profile(self) -> None:
        selected = self._selected_operating_profiles()
        if not selected:
            QMessageBox.information(self, "Edit Frequency Plan", "Select one frequency plan to edit.")
            return
        if len(selected) > 1:
            QMessageBox.warning(self, "Edit Frequency Plan", "Please select only one frequency plan to edit.")
            return
        updated = self._open_operating_profile_dialog(existing=selected[0])
        if not updated:
            return
        self._persist_operating_profile(updated)

    def _delete_operating_profiles(self) -> None:
        selected = self._selected_operating_profiles()
        if not selected:
            QMessageBox.information(self, "Delete Frequency Plans", "Select one or more frequency plans to delete.")
            return
        confirm = QMessageBox.question(
            self,
            "Delete Frequency Plans",
            f"Delete {len(selected)} selected frequency plan(s)?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            for row in selected:
                self.multi_radio_store.delete_operating_profile(int(row.get("id", 0) or 0))
        except ValueError as exc:
            QMessageBox.warning(self, "Delete Frequency Plans", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed deleting operating profiles.")
            QMessageBox.warning(self, "Delete Frequency Plans", "Unable to delete the selected frequency plans.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")
        QMessageBox.information(
            self,
            "Delete Frequency Plans",
            f"Deleted {len(selected)} frequency plan{'s' if len(selected) != 1 else ''}.",
        )

    def _open_assignment_dialog(self, selected_devices: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        enabled_profiles = [
            row for row in self.operating_profiles if isinstance(row, dict) and int(row.get("enabled", 1) or 0) == 1
        ]
        if not enabled_profiles:
            QMessageBox.information(
                self,
                "Assigned Plans",
                "Create or enable a frequency plan before assigning it to a radio.",
            )
            return None

        dlg = QDialog(self)
        dlg.setWindowTitle("Assign Frequency Plan")
        dlg.resize(540, 0)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        layout.addLayout(form)

        summary_label = QLabel(
            f"Apply one effective plan assignment to {len(selected_devices)} selected radio{'s' if len(selected_devices) != 1 else ''}."
        )
        summary_label.setWordWrap(True)
        form.addRow("", summary_label)

        selected_has_observer = any(
            str(row.get("device_class", "") or "").strip().lower() == "observer"
            for row in selected_devices
            if isinstance(row, dict)
        )

        profile_combo = QComboBox()
        for row in enabled_profiles:
            label = str(row.get("name", "") or "Frequency Plan")
            if int(row.get("receive_only", 0) or 0) == 1:
                label = f"{label} (receive-only)"
            profile_combo.addItem(label, int(row.get("id", 0) or 0))
        form.addRow("Frequency Plan:", profile_combo)

        state_combo = QComboBox()
        for label, value in OPERATING_ASSIGNMENT_STATE_OPTIONS:
            state_combo.addItem(label, value)
        form.addRow("Assignment State:", state_combo)

        reason_edit = QLineEdit()
        reason_edit.setPlaceholderText("Optional operator note")
        form.addRow("Reason:", reason_edit)

        ends_edit = QLineEdit()
        ends_edit.setPlaceholderText("Optional UTC metadata only; no automatic expiry in this checkpoint")
        form.addRow("Ends UTC:", ends_edit)

        info_label = QLabel()
        info_label.setWordWrap(True)
        form.addRow("", info_label)

        def _update_hint() -> None:
            state = str(state_combo.currentData() or "active").strip().lower()
            prefix = (
                "Observer / SDR radios can only be assigned receive-only frequency plans. "
                if selected_has_observer
                else ""
            )
            if state == "temporary_override":
                info_label.setText(
                    prefix
                    + "Temporary Override becomes effective immediately. Automatic timed expiry is not active in this checkpoint; restore the default assigned plan manually when the override ends."
                )
            else:
                info_label.setText(
                    prefix + "Active assignments become the current assigned plan for this radio immediately."
                )

        state_combo.currentIndexChanged.connect(_update_hint)
        _update_hint()

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        layout.addWidget(buttons)

        out: Dict[str, Any] = {}

        def _save() -> None:
            profile_id = int(profile_combo.currentData() or 0)
            if profile_id <= 0:
                QMessageBox.warning(self, "Validation", "Select a frequency plan.")
                return
            selected_profile = next(
                (row for row in enabled_profiles if int(row.get("id", 0) or 0) == profile_id),
                None,
            )
            if selected_has_observer and int((selected_profile or {}).get("receive_only", 0) or 0) != 1:
                QMessageBox.warning(
                    self,
                    "Assigned Plans",
                    "Observer / SDR radios can only be assigned receive-only frequency plans.",
                )
                return
            state = str(state_combo.currentData() or "active").strip().lower() or "active"
            reason_value = reason_edit.text().strip()
            if state == "temporary_override" and not reason_value:
                reason_value = "Temporary override from Settings."
            out.update(
                {
                    "operating_profile_id": profile_id,
                    "assignment_state": state,
                    "reason": reason_value,
                    "ends_utc": ends_edit.text().strip(),
                }
            )
            dlg.accept()

        buttons.accepted.connect(_save)
        buttons.rejected.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out if out else None

    def _assign_operating_profile_to_selected_devices(self) -> None:
        selected = self._selected_assignment_rows()
        if not selected:
            QMessageBox.information(self, "Assigned Plans", "Select one or more radios to assign.")
            return
        values = self._open_assignment_dialog(selected)
        if not values:
            return
        try:
            for row in selected:
                self.multi_radio_store.set_device_operating_profile(
                    int(row.get("device_profile_id", 0) or 0),
                    int(values.get("operating_profile_id", 0) or 0),
                    assignment_state=str(values.get("assignment_state", "active") or "active"),
                    reason=str(values.get("reason", "") or ""),
                    ends_utc=str(values.get("ends_utc", "") or ""),
                )
        except ValueError as exc:
            QMessageBox.warning(self, "Assigned Plans", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed updating radio schedule assignments.")
            QMessageBox.warning(self, "Assigned Plans", "Unable to update the selected assigned plans.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _restore_default_operating_profile_for_selected_devices(self) -> None:
        selected = self._selected_assignment_rows()
        if not selected:
            QMessageBox.information(self, "Assigned Plans", "Select one or more radios to restore.")
            return
        try:
            for row in selected:
                self.multi_radio_store.restore_default_operating_profile(int(row.get("device_profile_id", 0) or 0))
        except ValueError as exc:
            QMessageBox.warning(self, "Assigned Plans", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed restoring default frequency plan.")
            QMessageBox.warning(self, "Assigned Plans", "Unable to restore the default assigned plan.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _assign_schedule_to_selected_radios(self) -> None:
        selected = self._selected_device_profiles_as_assignment_rows()
        if not selected:
            QMessageBox.information(self, "Assign Plan", "Select one or more radios to assign.")
            return
        values = self._open_assignment_dialog(selected)
        if not values:
            return
        try:
            for row in selected:
                self.multi_radio_store.set_device_operating_profile(
                    int(row.get("device_profile_id", 0) or 0),
                    int(values.get("operating_profile_id", 0) or 0),
                    assignment_state=str(values.get("assignment_state", "active") or "active"),
                    reason=str(values.get("reason", "") or ""),
                    ends_utc=str(values.get("ends_utc", "") or ""),
                )
        except ValueError as exc:
            QMessageBox.warning(self, "Assign Plan", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed assigning plan from radio profiles.")
            QMessageBox.warning(self, "Assign Plan", "Unable to update the selected assigned plans.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _restore_schedule_for_selected_radios(self) -> None:
        selected = self._selected_device_profiles()
        if not selected:
            QMessageBox.information(self, "Restore Plan", "Select one or more radios to restore.")
            return
        try:
            for row in selected:
                self.multi_radio_store.restore_default_operating_profile(int(row.get("id", 0) or 0))
        except ValueError as exc:
            QMessageBox.warning(self, "Restore Plan", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed restoring schedule from radio profiles.")
            QMessageBox.warning(self, "Restore Plan", "Unable to restore the selected assigned plans.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _open_temporary_profile_swap_dialog(self, target_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        primary_row = next(
            (
                row
                for row in self.device_assignments
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            None,
        )
        if not isinstance(primary_row, dict):
            QMessageBox.warning(self, "Temporary Plan Swap", "The current Station Default radio assignment could not be resolved.")
            return None
        primary_profile = self._operating_profile_by_id(int(primary_row.get("operating_profile_id", 0) or 0))
        allow_carry = bool(primary_profile and int(primary_profile.get("allow_profile_swap", 0) or 0) == 1)

        dlg = QDialog(self)
        dlg.setWindowTitle("Temporary Plan Swap")
        dlg.resize(560, 0)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        layout.addLayout(form)

        summary_label = QLabel(
            f"Temporarily move the Station Default runtime from "
            f"{str(primary_row.get('device_name', '') or 'current primary')} to "
            f"{str(target_row.get('device_name', '') or 'selected target')}."
        )
        summary_label.setWordWrap(True)
        form.addRow("", summary_label)

        form.addRow("Current Primary:", QLabel(str(primary_row.get("device_name", "") or "")))
        form.addRow("Primary Assigned Plan:", QLabel(str(primary_row.get("operating_profile_name", "") or "Unassigned")))
        form.addRow("Target Radio:", QLabel(str(target_row.get("device_name", "") or "")))
        form.addRow("Target Assigned Plan:", QLabel(str(target_row.get("operating_profile_name", "") or "Unassigned")))

        mode_combo = QComboBox()
        mode_combo.addItem("Use target radio assigned plan (Recommended)", "use_target_profile")
        if allow_carry:
            mode_combo.addItem("Carry current Station Default assigned plan", "carry_primary_profile")
        form.addRow("Swap Mode:", mode_combo)

        reason_edit = QLineEdit()
        reason_edit.setPlaceholderText("Optional operator note")
        reason_edit.setText(
            f"Temporary plan swap {str(primary_row.get('device_name', '') or 'primary')} -> {str(target_row.get('device_name', '') or 'target')}"
        )
        form.addRow("Reason:", reason_edit)

        ends_edit = QLineEdit()
        ends_edit.setPlaceholderText("Optional UTC metadata only; no automatic expiry in this checkpoint")
        form.addRow("Ends UTC:", ends_edit)

        info_label = QLabel()
        info_label.setWordWrap(True)
        form.addRow("", info_label)

        def _update_hint() -> None:
            mode = str(mode_combo.currentData() or "use_target_profile").strip().lower()
            if mode == "carry_primary_profile":
                carried_name = str((primary_profile or {}).get("name", "") or "Current Station Default Assigned Plan")
                info_label.setText(
                    f"{carried_name} will be copied onto the target radio as a temporary override. "
                    "Restore Swap returns the target radio to its prior effective assignment."
                )
            elif not allow_carry:
                info_label.setText(
                    "The current Station Default assigned plan does not allow carried swaps, so this workflow keeps the target radio's existing effective assigned plan."
                )
            else:
                info_label.setText(
                    "This swap changes the Station Default radio temporarily but leaves the target radio's current effective assigned plan in place."
                )

        mode_combo.currentIndexChanged.connect(_update_hint)
        _update_hint()

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        layout.addWidget(buttons)

        out: Dict[str, Any] = {}

        def _save() -> None:
            out.update(
                {
                    "mode": str(mode_combo.currentData() or "use_target_profile").strip().lower() or "use_target_profile",
                    "reason": reason_edit.text().strip(),
                    "ends_utc": ends_edit.text().strip(),
                }
            )
            dlg.accept()

        buttons.accepted.connect(_save)
        buttons.rejected.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out if out else None

    def _start_temporary_profile_swap(self) -> None:
        if isinstance(self.active_profile_swap, dict) and self.active_profile_swap:
            QMessageBox.information(self, "Temporary Plan Swap", "A temporary plan swap is already active. Restore it before starting another.")
            return
        selected = self._selected_assignment_rows()
        if not selected:
            QMessageBox.information(self, "Temporary Plan Swap", "Select one active non-primary radio to use as the temporary plan swap target.")
            return
        if len(selected) != 1:
            QMessageBox.warning(self, "Temporary Plan Swap", "Please select exactly one active non-primary radio as the temporary plan swap target.")
            return
        target_row = selected[0]
        if int(target_row.get("runtime_primary", 0) or 0) == 1:
            QMessageBox.information(self, "Temporary Plan Swap", "The selected radio is already the Station Default runtime.")
            return
        if str(target_row.get("device_class", "") or "").strip().lower() == "observer":
            QMessageBox.warning(self, "Temporary Plan Swap", "Observer / SDR radios cannot be used as temporary plan swap targets.")
            return
        if int(target_row.get("runtime_active", 0) or 0) != 1:
            QMessageBox.warning(self, "Temporary Plan Swap", "The temporary plan swap target must already be active.")
            return
        values = self._open_temporary_profile_swap_dialog(target_row)
        if not values:
            return
        try:
            self.multi_radio_store.start_temporary_profile_swap(
                int(target_row.get("device_profile_id", 0) or 0),
                mode=str(values.get("mode", "use_target_profile") or "use_target_profile"),
                reason=str(values.get("reason", "") or ""),
                ends_utc=str(values.get("ends_utc", "") or ""),
            )
        except ValueError as exc:
            QMessageBox.warning(self, "Temporary Plan Swap", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed starting temporary profile swap.")
            QMessageBox.warning(self, "Temporary Plan Swap", "Unable to start the temporary plan swap.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _restore_temporary_profile_swap(self) -> None:
        active_swap = dict(self.active_profile_swap or {})
        if not active_swap:
            QMessageBox.information(self, "Restore Swap", "No temporary plan swap is currently active.")
            return
        source_name = str(active_swap.get("source_device_name", "") or "").strip() or "previous primary"
        target_name = str(active_swap.get("target_device_name", "") or "").strip() or "temporary target"
        confirm = QMessageBox.question(
            self,
            "Restore Swap",
            f"Restore the Station Default runtime from {target_name} back to {source_name}?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes,
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            self.multi_radio_store.restore_temporary_profile_swap()
        except ValueError as exc:
            QMessageBox.warning(self, "Restore Swap", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed restoring temporary profile swap.")
            QMessageBox.warning(self, "Restore Swap", "Unable to restore the temporary plan swap.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _confirm_runtime_projection_override(self, action_name: str) -> bool:
        if not self._settings_dirty:
            return True
        confirm = QMessageBox.question(
            self,
            action_name,
            "Unsaved Settings changes are present.\n\n"
            "Continuing will replace the visible compatibility backend and endpoint fields with the selected device "
            "profile projection.\n\nContinue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        return confirm == QMessageBox.Yes

    @staticmethod
    def _device_profile_dialog_title(existing: Optional[Dict[str, Any]] = None) -> str:
        return "Advanced Radio Edit" if existing else "Guided Add Radio"

    @staticmethod
    def _device_profile_dialog_intro(existing: Optional[Dict[str, Any]] = None) -> str:
        if existing:
            return (
                "Edit the selected radio's identity, role, and core connection details. "
                "Software-specific settings remain available in the selected-radio Settings sections behind this dialog."
            )
        return (
            "Set up a new radio one step at a time: choose the radio identity, pick the software used by that radio, "
            "enter the connection details that matter, and review readiness before saving."
        )

    @staticmethod
    def _device_profile_dialog_save_text(existing: Optional[Dict[str, Any]] = None) -> str:
        return "Save Changes" if existing else "Save Radio"

    def _open_device_profile_dialog(self, existing: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        dlg = QDialog(self)
        dlg_title = self._device_profile_dialog_title(existing)
        dlg.setWindowTitle(dlg_title)
        dlg.setAccessibleName(dlg_title)
        dlg.resize(760, 720)
        layout = QVBoxLayout(dlg)
        intro = QLabel(self._device_profile_dialog_intro(existing))
        intro.setAccessibleName(f"{dlg_title} guidance")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        scroll = QScrollArea(dlg)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        layout.addWidget(scroll, 1)

        body = QWidget()
        scroll.setWidget(body)
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(0, 0, 0, 0)
        body_layout.setSpacing(10)

        catalog_payload = load_radio_catalog()
        catalog_entries = list(catalog_payload.get("entries", []) or [])
        catalog_source = str(catalog_payload.get("source", "static-fallback") or "static-fallback")
        radio_model_prompt = "Select or search for a radio model"
        row_labels: Dict[QWidget, QWidget] = {}

        def _show_help(btn: QWidget, text: str) -> None:
            QToolTip.showText(btn.mapToGlobal(btn.rect().bottomLeft()), text, btn)

        def _make_help_label(text: str, help_text: str = "") -> QWidget:
            wrap = QWidget()
            row = QHBoxLayout(wrap)
            row.setContentsMargins(0, 0, 0, 0)
            row.setSpacing(4)
            label = QLabel(text)
            row.addWidget(label)
            if help_text:
                help_btn = QPushButton("i", wrap)
                help_btn.setCheckable(False)
                help_btn.setText("?")
                help_btn.setFixedSize(18, 18)
                help_btn.setToolTip(help_text)
                help_btn.setCursor(Qt.PointingHandCursor)
                help_btn.setStyleSheet(
                    "QPushButton {"
                    " border: 1px solid palette(mid);"
                    " border-radius: 9px;"
                    " padding: 0px;"
                    " font-weight: bold;"
                    " min-width: 18px;"
                    " min-height: 18px;"
                    "}"
                    "QPushButton:hover {"
                    " background: palette(base);"
                    "}"
                )
                help_btn.clicked.connect(lambda _checked=False, b=help_btn, t=help_text: _show_help(b, t))
                row.addWidget(help_btn)
            row.addStretch(1)
            return wrap

        def _add_form_row(form_layout: QFormLayout, label_text: str, field_widget: QWidget, help_text: str = "") -> None:
            label_widget = _make_help_label(label_text, help_text) if label_text else QWidget()
            if label_text:
                row_labels[field_widget] = label_widget
                form_layout.addRow(label_widget, field_widget)
            else:
                form_layout.addRow("", field_widget)

        def _add_full_width_row(form_layout: QFormLayout, field_widget: QWidget) -> None:
            field_widget.setSizePolicy(QSizePolicy.Expanding, field_widget.sizePolicy().verticalPolicy())
            form_layout.addRow(field_widget)

        def _configure_combo_width(combo: QComboBox, minimum: int = 220) -> None:
            combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
            fm = combo.fontMetrics()
            widest = minimum
            for idx in range(combo.count()):
                widest = max(widest, fm.horizontalAdvance(combo.itemText(idx)) + 56)
            combo.setMinimumWidth(min(widest, 420))
            try:
                combo.view().setMinimumWidth(min(max(widest + 24, minimum), 520))
            except Exception:
                pass

        def _make_section(title: str, help_text: str = "") -> tuple[QGroupBox, QFormLayout]:
            group = QGroupBox(title)
            group_layout = QVBoxLayout(group)
            group_layout.setContentsMargins(10, 10, 10, 10)
            group_layout.setSpacing(8)
            if help_text:
                help_label = QLabel(help_text)
                help_label.setWordWrap(True)
                group_layout.addWidget(help_label)
            form_layout = QFormLayout()
            form_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
            group_layout.addLayout(form_layout)
            body_layout.addWidget(group)
            return group, form_layout

        readiness_card = QFrame()
        readiness_card.setFrameShape(QFrame.StyledPanel)
        readiness_card_layout = QVBoxLayout(readiness_card)
        readiness_card_layout.setContentsMargins(12, 10, 12, 10)
        readiness_card_layout.setSpacing(6)
        readiness_title = QLabel("Live Radio Readiness")
        readiness_title_font = readiness_title.font()
        readiness_title_font.setBold(True)
        readiness_title.setFont(readiness_title_font)
        readiness_card_layout.addWidget(readiness_title)
        readiness_intro = QLabel(
            "FreqInOut checks this radio as you edit it so the next setup step is easier to spot before you save."
        )
        readiness_intro.setObjectName("readinessIntro")
        readiness_intro.setWordWrap(True)
        readiness_card_layout.addWidget(readiness_intro)
        dialog_readiness_status = QLabel()
        dialog_readiness_status.setWordWrap(True)
        dialog_readiness_detail = QLabel()
        dialog_readiness_detail.setWordWrap(True)
        readiness_card_layout.addWidget(dialog_readiness_status)
        readiness_card_layout.addWidget(dialog_readiness_detail)
        body_layout.addWidget(readiness_card)

        identity_group, identity_form = _make_section(
            "Radio Identity",
            "Start with the actual radio model, then give the radio a station-friendly name if you want one.",
        )
        software_group = QGroupBox("Software Stack and Guidance")
        software_group_layout = QVBoxLayout(software_group)
        software_group_layout.setContentsMargins(10, 10, 10, 10)
        software_group_layout.setSpacing(8)
        body_layout.addWidget(software_group)
        software_form = QFormLayout()
        software_form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        software_group_layout.addLayout(software_form)
        connection_group, connection_form = _make_section(
            "Connection Details",
            "Only the fields that matter for the selected role, backend, and software are shown.",
        )
        launch_group, launch_form = _make_section(
            "Launch and Support",
            "Launch settings are optional, but they help FreqInOut guide startup behavior and readiness more accurately.",
        )

        radio_model_combo = QComboBox()
        radio_model_combo.setEditable(True)
        radio_model_combo.setInsertPolicy(QComboBox.NoInsert)
        if radio_model_combo.lineEdit() is not None:
            radio_model_combo.lineEdit().setPlaceholderText(radio_model_prompt)
        refresh_catalog_btn = QPushButton("Refresh Catalog")
        refresh_catalog_btn.setToolTip("Reload the local rig catalog from Hamlib when available.")
        radio_model_row = QHBoxLayout()
        radio_model_row.setContentsMargins(0, 0, 0, 0)
        radio_model_row.addWidget(radio_model_combo, 1)
        radio_model_row.addWidget(refresh_catalog_btn)
        radio_model_wrap = QWidget()
        radio_model_wrap.setLayout(radio_model_row)
        _add_form_row(
            identity_form,
            "Radio Model:",
            radio_model_wrap,
            "Choose the actual rig model when possible so FreqInOut can offer smarter setup guidance.",
        )

        model_hint_label = QLabel()
        model_hint_label.setWordWrap(True)
        _add_full_width_row(identity_form, model_hint_label)

        name_edit = QLineEdit(str((existing or {}).get("name", "") or ""))
        name_edit.setPlaceholderText("Enter a radio name, or leave blank to use the selected model name")
        display_name_user_edited = bool(name_edit.text().strip())
        last_catalog_display_name = ""
        _add_form_row(
            identity_form,
            "Radio Name:",
            name_edit,
            "Use a name that will make sense in schedules and support, such as IC-7300 Desk or JS8 Mobile.",
        )

        device_class_combo = QComboBox()
        for label, value in DEVICE_CLASS_OPTIONS:
            device_class_combo.addItem(label, value)
        device_class = str((existing or {}).get("device_class", "tx_rx") or "tx_rx").strip().lower()
        class_idx = device_class_combo.findData(device_class)
        device_class_combo.setCurrentIndex(class_idx if class_idx >= 0 else 0)
        _configure_combo_width(device_class_combo)
        _add_form_row(
            identity_form,
            "Radio Role:",
            device_class_combo,
            "Choose whether this radio is a transmit/receive rig, an observer SDR, or a gateway-style radio.",
        )

        backend_combo = QComboBox()
        backend_options = [
            ("FLRig", "flrig"),
            ("JS8Call", "js8call"),
            ("Manual", "manual"),
            ("RIGCTLD", "rigctld"),
        ]
        for label, value in backend_options:
            backend_combo.addItem(label, value)
        backend_idx = backend_combo.findData(str((existing or {}).get("control_backend", "flrig") or "flrig").strip().lower())
        backend_combo.setCurrentIndex(backend_idx if backend_idx >= 0 else 0)
        _configure_combo_width(backend_combo)
        _add_form_row(
            identity_form,
            "Primary Rig Control:",
            backend_combo,
            "This determines how FreqInOut primarily controls or follows the rig. It does not limit the rest of the software stack assigned to this radio.",
        )

        deploy_combo = QComboBox()
        deploy_combo.addItem("Full", "full")
        deploy_combo.addItem("Minimal", "minimal")
        deploy_idx = deploy_combo.findData(str((existing or {}).get("deployment_mode", "full") or "full").strip().lower())
        deploy_combo.setCurrentIndex(deploy_idx if deploy_idx >= 0 else 0)
        _configure_combo_width(deploy_combo)
        _add_form_row(
            identity_form,
            "Deployment Mode:",
            deploy_combo,
            "Full is the normal choice. Minimal is for lighter setups where not all integrations will be used.",
        )

        software_row = QGridLayout()
        software_row.setContentsMargins(0, 0, 0, 0)
        software_row.setHorizontalSpacing(24)
        software_row.setVerticalSpacing(10)
        use_flrig_chk = QCheckBox("FLRig")
        use_fldigi_chk = QCheckBox("FLDigi")
        use_flmsg_chk = QCheckBox("FLMsg")
        use_flamp_chk = QCheckBox("FLAmp")
        use_js8call_chk = QCheckBox("JS8Call")
        use_js8spotter_chk = QCheckBox("JS8Spotter")
        use_commstat_chk = QCheckBox("CommStat")
        use_varac_chk = QCheckBox("VarAC")
        software_row.addWidget(use_flrig_chk, 0, 0)
        software_row.addWidget(use_fldigi_chk, 0, 1)
        software_row.addWidget(use_flmsg_chk, 0, 2)
        software_row.addWidget(use_flamp_chk, 0, 3)
        software_row.addWidget(use_js8call_chk, 1, 0)
        software_row.addWidget(use_js8spotter_chk, 1, 1)
        software_row.addWidget(use_commstat_chk, 1, 2)
        software_row.addWidget(use_varac_chk, 2, 0)
        software_wrap = QWidget()
        software_wrap.setLayout(software_row)
        _add_form_row(
            software_form,
            "Software Used:",
            software_wrap,
            "Choose the software bundle that belongs to this radio. A transceiver can participate in more than one operating lane, such as FLRig plus JS8Call plus VarAC.",
        )

        software_hint_label = QLabel()
        software_hint_label.setWordWrap(True)
        _add_full_width_row(software_form, software_hint_label)

        configure_auto_wrap = QWidget()
        configure_auto_row = QHBoxLayout(configure_auto_wrap)
        configure_auto_row.setContentsMargins(0, 0, 0, 0)
        configure_auto_row.setSpacing(8)
        configure_auto_btn = QPushButton("Configure Automatically")
        configure_auto_btn.setToolTip(
            "Fill blank paths, ports, and message-file locations for this radio using installed apps and existing app settings."
        )
        configure_auto_status = QLabel("Choose the software this radio uses, then let FIO fill what it can.")
        configure_auto_status.setWordWrap(True)
        configure_auto_status.setTextInteractionFlags(Qt.TextSelectableByMouse)
        configure_auto_row.addWidget(configure_auto_btn)
        configure_auto_row.addWidget(configure_auto_status, 1)
        _add_full_width_row(software_form, configure_auto_wrap)

        app_setup_plan_group = QGroupBox("Planned App Setup")
        app_setup_plan_group.setObjectName("guidedAutoAppSetupPlan")
        app_setup_plan_group.setVisible(False)
        app_setup_plan_layout = QVBoxLayout(app_setup_plan_group)
        app_setup_plan_layout.setContentsMargins(10, 8, 10, 8)
        app_setup_plan_label = QLabel()
        app_setup_plan_label.setWordWrap(True)
        app_setup_plan_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        app_setup_plan_layout.addWidget(app_setup_plan_label)
        _add_full_width_row(software_form, app_setup_plan_group)

        app_choice_group = QGroupBox("Choose Detected Apps")
        app_choice_group.setVisible(False)
        app_choice_layout = QFormLayout(app_choice_group)
        app_choice_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        app_choice_combos: Dict[str, QComboBox] = {}
        app_choice_specs = [
            ("flrig", "Which FLRig controls this radio?"),
            ("fldigi", "Which FLDigi belongs to this radio?"),
            ("flmsg", "Which FLMsg belongs to this radio?"),
            ("flamp", "Which FLAmp belongs to this radio?"),
            ("js8call", "Which JS8Call belongs to this radio?"),
            ("js8spotter", "Which JS8Spotter belongs to this radio?"),
            ("commstat", "Which CommStat belongs to this radio?"),
            ("varac", "Which VarAC belongs to this radio?"),
        ]
        for app_id, prompt_text in app_choice_specs:
            combo = QComboBox()
            combo.setObjectName(f"guidedAutoAppChoice_{app_id}")
            combo.setVisible(False)
            combo.setToolTip("FIO found more than one installed app. Choose the one this radio should use.")
            combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
            app_choice_combos[app_id] = combo
            label_widget = _make_help_label(prompt_text, "Choose the installed app path that belongs to this radio.")
            label_widget.setVisible(False)
            row_labels[combo] = label_widget
            app_choice_layout.addRow(label_widget, combo)
        js8_profile_choice_combo = QComboBox()
        js8_profile_choice_combo.setObjectName("guidedAutoJs8ProfileChoice")
        js8_profile_choice_combo.setVisible(False)
        js8_profile_choice_combo.setToolTip("FIO found more than one JS8Call profile. Choose the profile this radio uses.")
        js8_profile_choice_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        js8_profile_choice_label = _make_help_label(
            "Which JS8Call profile belongs to this radio?",
            "Choose the JS8Call profile/port pair that belongs to this radio. FIO will fill the TCP port, profile folder, and DIRECTED.TXT path.",
        )
        js8_profile_choice_label.setVisible(False)
        row_labels[js8_profile_choice_combo] = js8_profile_choice_label
        app_choice_layout.addRow(js8_profile_choice_label, js8_profile_choice_combo)
        _add_full_width_row(software_form, app_choice_group)

        role_hint_label = QLabel()
        role_hint_label.setWordWrap(True)
        setup_guidance_heading = _make_help_label(
            "Setup Guidance:",
            "This section explains what the current role and control method mean for this radio.",
        )
        _add_full_width_row(software_form, setup_guidance_heading)
        _add_full_width_row(software_form, role_hint_label)

        rig_host_edit = QLineEdit(str((existing or {}).get("rig_host", "") or ""))
        rig_port_edit = QLineEdit(str((existing or {}).get("rig_port", "") or ""))
        rig_port_edit.setValidator(QIntValidator(1, 65535, rig_port_edit))
        rig_row = QHBoxLayout()
        rig_row.addWidget(rig_host_edit, 1)
        rig_row.addWidget(QLabel("Port"))
        rig_row.addWidget(rig_port_edit)
        rig_wrap = QWidget()
        rig_wrap.setLayout(rig_row)
        _add_form_row(connection_form, "RigCtlD TCP:", rig_wrap, "Host and port for the rigctld TCP service for this radio.")

        flrig_host_edit = QLineEdit(str((existing or {}).get("flrig_host", "") or ""))
        flrig_port_edit = QLineEdit(str((existing or {}).get("flrig_port", "") or ""))
        flrig_port_edit.setValidator(QIntValidator(1, 65535, flrig_port_edit))
        flrig_row = QHBoxLayout()
        flrig_row.addWidget(flrig_host_edit, 1)
        flrig_row.addWidget(QLabel("Port"))
        flrig_row.addWidget(flrig_port_edit)
        flrig_wrap = QWidget()
        flrig_wrap.setLayout(flrig_row)
        _add_form_row(connection_form, "FLRig XML RPC:", flrig_wrap, "Host and port for FLRig XML RPC control of this radio.")

        flrig_path_edit = QLineEdit(str((existing or {}).get("flrig_path", "") or ""))
        _add_form_row(connection_form, "FLRig App:", flrig_path_edit, "Optional FLRig executable or app path associated with this radio.")

        fldigi_host_edit = QLineEdit(str((existing or {}).get("fldigi_host", "") or ""))
        fldigi_port_edit = QLineEdit(str((existing or {}).get("fldigi_port", "") or ""))
        fldigi_port_edit.setValidator(QIntValidator(1, 65535, fldigi_port_edit))
        fldigi_row = QHBoxLayout()
        fldigi_row.addWidget(fldigi_host_edit, 1)
        fldigi_row.addWidget(QLabel("Port"))
        fldigi_row.addWidget(fldigi_port_edit)
        fldigi_wrap = QWidget()
        fldigi_wrap.setLayout(fldigi_row)
        _add_form_row(connection_form, "FLDigi XML RPC:", fldigi_wrap, "Host and port for FLDigi XML RPC when this radio uses Fast Light workflows.")

        fldigi_path_edit = QLineEdit(str((existing or {}).get("fldigi_path", "") or ""))
        _add_form_row(connection_form, "FLDigi App:", fldigi_path_edit, "Optional FLDigi executable or app path associated with this radio.")

        flmsg_path_edit = QLineEdit(str((existing or {}).get("flmsg_path", "") or ""))
        _add_form_row(connection_form, "FLMsg App:", flmsg_path_edit, "Optional FLMsg executable or app path associated with this radio.")

        flamp_path_edit = QLineEdit(str((existing or {}).get("flamp_path", "") or ""))
        _add_form_row(connection_form, "FLAmp App:", flamp_path_edit, "Optional FLAmp executable or app path associated with this radio.")

        js8_host_edit = QLineEdit(str((existing or {}).get("js8_host", "") or ""))
        js8_port_edit = QLineEdit(str((existing or {}).get("js8_port", "") or ""))
        js8_port_edit.setValidator(QIntValidator(1, 65535, js8_port_edit))
        js8_row = QHBoxLayout()
        js8_row.addWidget(js8_host_edit, 1)
        js8_row.addWidget(QLabel("Port"))
        js8_row.addWidget(js8_port_edit)
        js8_wrap = QWidget()
        js8_wrap.setLayout(js8_row)
        _add_form_row(connection_form, "JS8Call TCP:", js8_wrap, "Host and port for the JS8Call TCP API for this radio.")

        js8_install_edit = QLineEdit(str((existing or {}).get("js8_install_path", "") or ""))
        _add_form_row(connection_form, "JS8Call App:", js8_install_edit, "Optional JS8Call executable or app path associated with this radio.")

        js8_profile_edit = QLineEdit(str((existing or {}).get("js8_profile_path", "") or ""))
        _add_form_row(connection_form, "JS8 Profile:", js8_profile_edit, "Optional JS8Call profile folder for this radio.")

        js8_directed_edit = QLineEdit(str((existing or {}).get("js8_directed_path", "") or ""))
        _add_form_row(connection_form, "DIRECTED.TXT:", js8_directed_edit, "Optional DIRECTED.TXT path associated with this radio's JS8 setup.")

        js8_forms_edit = QLineEdit(str((existing or {}).get("js8_forms_path", "") or ""))
        _add_form_row(connection_form, "JS8 Forms Path:", js8_forms_edit, "Optional JS8 forms / inbox path associated with this radio.")

        js8spotter_launch_edit = QLineEdit(str((existing or {}).get("spotter_launch_path", "") or ""))
        _add_form_row(connection_form, "JS8Spotter App:", js8spotter_launch_edit, "Optional JS8Spotter launch path for this radio.")

        commstat_launch_edit = QLineEdit(str((existing or {}).get("commstat_launch_path", "") or ""))
        _add_form_row(connection_form, "CommStat App:", commstat_launch_edit, "Optional CommStat launch path for this radio.")

        varac_install_edit = QLineEdit(str((existing or {}).get("varac_install_path", "") or ""))
        _add_form_row(connection_form, "VarAC Install:", varac_install_edit, "VarAC install folder used for this radio.")

        varac_db_edit = QLineEdit(str((existing or {}).get("varac_db_path", "") or ""))
        _add_form_row(connection_form, "VarAC DB:", varac_db_edit, "VarAC database path for this radio.")

        varac_ini_edit = QLineEdit(str((existing or {}).get("varac_ini_path", "") or ""))
        _add_form_row(connection_form, "VarAC INI:", varac_ini_edit, "VarAC INI/config path for this radio.")

        varac_incoming_edit = QLineEdit(str((existing or {}).get("varac_incoming_path", "") or ""))
        _add_form_row(connection_form, "VarAC Incoming:", varac_incoming_edit, "Optional VarAC incoming-files path associated with this radio.")

        varac_launch_cmd_edit = QLineEdit(str((existing or {}).get("launch_cmd", "") or ""))
        _add_form_row(connection_form, "VarAC Launch:", varac_launch_cmd_edit, "Optional VarAC launch override for this radio.")

        launch_enabled_chk = QCheckBox("Use Launch Control for this radio")
        launch_enabled_chk.setChecked(bool((existing or {}).get("launch_enabled", 0)))
        launch_form.addRow("", launch_enabled_chk)

        launch_path_edit = QLineEdit(str((existing or {}).get("launch_path", "") or ""))
        launch_path_btn = QPushButton("Browse")
        launch_row = QHBoxLayout()
        launch_row.addWidget(launch_path_edit, 1)
        launch_row.addWidget(launch_path_btn)
        launch_wrap = QWidget()
        launch_wrap.setLayout(launch_row)
        _add_form_row(
            launch_form,
            "Launch Path:",
            launch_wrap,
            "Optional executable or script path used when FreqInOut launches this radio's software.",
        )

        sdr_host_edit = QLineEdit(str((existing or {}).get("sdr_host", "") or ""))
        sdr_port_edit = QLineEdit(str((existing or {}).get("sdr_port", "") or ""))
        sdr_port_edit.setValidator(QIntValidator(1, 65535, sdr_port_edit))
        sdr_row = QHBoxLayout()
        sdr_row.addWidget(sdr_host_edit, 1)
        sdr_row.addWidget(QLabel("Port"))
        sdr_row.addWidget(sdr_port_edit)
        sdr_wrap = QWidget()
        sdr_wrap.setLayout(sdr_row)
        _add_form_row(connection_form, "Observer SDR:", sdr_wrap, "Observer SDR endpoint used when this radio is an observer.")

        port_prompt_group = QGroupBox("Enter App Ports")
        port_prompt_group.setVisible(False)
        port_prompt_layout = QFormLayout(port_prompt_group)
        port_prompt_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        port_prompt_specs = {
            "flrig_port": ("What port does this radio's FLRig use?", flrig_port_edit),
            "fldigi_port": ("What port does this radio's FLDigi use?", fldigi_port_edit),
            "js8_port": ("What port does this radio's JS8Call use?", js8_port_edit),
        }
        port_prompt_fields: Dict[str, QLineEdit] = {}
        for field_key, (prompt_text, target_edit) in port_prompt_specs.items():
            prompt_edit = QLineEdit()
            prompt_edit.setObjectName(f"guidedAutoPortPrompt_{field_key}")
            prompt_edit.setValidator(QIntValidator(1, 65535, prompt_edit))
            prompt_edit.setPlaceholderText("Port number")
            prompt_edit.setVisible(False)
            prompt_edit.setToolTip("This is the number the app uses to talk to FIO.")
            port_prompt_fields[field_key] = prompt_edit
            label_widget = _make_help_label(prompt_text, "Enter the port number configured in that app for this radio.")
            label_widget.setVisible(False)
            row_labels[prompt_edit] = label_widget
            port_prompt_layout.addRow(label_widget, prompt_edit)
            prompt_edit.textChanged.connect(
                lambda text, target=target_edit: target.setText(str(text or "").strip())
                if target.text().strip() != str(text or "").strip()
                else None
            )
            target_edit.textChanged.connect(
                lambda text, prompt=prompt_edit: prompt.setText(str(text or "").strip())
                if prompt.text().strip() != str(text or "").strip()
                else None
            )
        _add_full_width_row(software_form, port_prompt_group)

        optional_toggle = QToolButton(dlg)
        optional_toggle.setText("Optional Groups and Notes")
        optional_toggle.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        optional_toggle.setArrowType(Qt.RightArrow)
        optional_toggle.setCheckable(True)
        optional_toggle.setChecked(False)
        body_layout.addWidget(optional_toggle)

        optional_body = QWidget()
        optional_layout = QVBoxLayout(optional_body)
        optional_layout.setContentsMargins(10, 0, 10, 0)
        optional_layout.setSpacing(6)
        optional_form = QFormLayout()
        optional_form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        optional_layout.addLayout(optional_form)
        body_layout.addWidget(optional_body)

        ptt_group_edit = QLineEdit(str((existing or {}).get("ptt_group", "") or ""))
        ptt_group_edit.setPlaceholderText("Optional shared transmit/PTT domain, e.g. AMP-A")
        _add_form_row(optional_form, "PTT Group:", ptt_group_edit, "Optional shared transmit/PTT domain for conflict protection.")

        antenna_group_edit = QLineEdit(str((existing or {}).get("antenna_group", "") or ""))
        antenna_group_edit.setPlaceholderText("Optional shared antenna path, e.g. ANT-1")
        _add_form_row(optional_form, "Antenna Group:", antenna_group_edit, "Optional shared antenna path used for RF conflict warnings.")

        frontend_group_edit = QLineEdit(str((existing or {}).get("frontend_group", "") or ""))
        frontend_group_edit.setPlaceholderText("Optional shared front-end chain, e.g. FRONT-A")
        _add_form_row(optional_form, "Front-End Group:", frontend_group_edit, "Optional shared front-end chain for RF conflict warnings.")

        amplifier_group_edit = QLineEdit(str((existing or {}).get("amplifier_group", "") or ""))
        amplifier_group_edit.setPlaceholderText("Optional shared amplifier chain, e.g. AMP-MAIN")
        _add_form_row(optional_form, "Amplifier Group:", amplifier_group_edit, "Optional shared amplifier chain for RF conflict warnings.")

        notes_edit = QPlainTextEdit(str((existing or {}).get("notes", "") or ""))
        notes_edit.setMaximumHeight(96)
        _add_form_row(optional_form, "Notes:", notes_edit, "Optional operator notes about this radio profile.")

        flrig_field_widgets = [flrig_wrap, flrig_path_edit]
        rigctld_field_widgets = [rig_wrap]
        js8_field_widgets = [
            js8_wrap,
            js8_install_edit,
            js8_profile_edit,
            js8_directed_edit,
            js8_forms_edit,
            js8spotter_launch_edit,
            commstat_launch_edit,
        ]
        observer_field_widgets = [sdr_wrap]
        fldigi_field_widgets = [fldigi_wrap, fldigi_path_edit]
        flmsg_field_widgets = [flmsg_path_edit]
        flamp_field_widgets = [flamp_path_edit]
        varac_field_widgets = [varac_install_edit, varac_db_edit, varac_ini_edit, varac_incoming_edit, varac_launch_cmd_edit]
        optional_field_widgets = [ptt_group_edit, antenna_group_edit, frontend_group_edit, amplifier_group_edit, notes_edit]
        app_choice_targets: Dict[str, QLineEdit] = {
            "flrig": flrig_path_edit,
            "fldigi": fldigi_path_edit,
            "flmsg": flmsg_path_edit,
            "flamp": flamp_path_edit,
            "js8call": js8_install_edit,
            "js8spotter": js8spotter_launch_edit,
            "commstat": commstat_launch_edit,
            "varac": varac_install_edit,
        }

        def _app_choice_app_selected(app_id: str) -> bool:
            key = str(app_id or "").strip().lower()
            backend = str(backend_combo.currentData() or "").strip().lower()
            selected_by_app = {
                "flrig": use_flrig_chk.isChecked() or backend == "flrig",
                "fldigi": use_fldigi_chk.isChecked(),
                "flmsg": use_flmsg_chk.isChecked(),
                "flamp": use_flamp_chk.isChecked(),
                "js8call": use_js8call_chk.isChecked() or backend == "js8call",
                "js8spotter": use_js8spotter_chk.isChecked(),
                "commstat": use_commstat_chk.isChecked(),
                "varac": use_varac_chk.isChecked(),
            }
            return bool(selected_by_app.get(key, False))

        def _js8_app_selected() -> bool:
            return _app_choice_app_selected("js8call")

        def _apply_detected_app_choice(app_id: str) -> None:
            combo = app_choice_combos.get(app_id)
            target = app_choice_targets.get(app_id)
            if combo is None or target is None:
                return
            path_text = str(combo.currentData() or "").strip()
            if not path_text:
                return
            app_labels = {
                "flrig": "FLRig",
                "fldigi": "FLDigi",
                "flmsg": "FLMsg",
                "flamp": "FLAmp",
                "js8call": "JS8Call",
                "js8spotter": "JS8Spotter",
                "commstat": "CommStat",
                "varac": "VarAC",
            }
            label = app_labels.get(app_id, app_id)
            if target.text().strip():
                configure_auto_status.setText(f"Kept existing {label} app path. Clear the field first to use the selected app.")
            else:
                target.setText(path_text)
                configure_auto_status.setText(f"Using selected {label} app for this radio.")
            _update_dialog_readiness()

        def _apply_js8_profile_choice() -> None:
            payload = js8_profile_choice_combo.currentData()
            if not isinstance(payload, Mapping):
                return
            port = str(payload.get("port", "") or "").strip()
            profile_path = str(payload.get("profile_path", "") or "").strip()
            directed_path = str(payload.get("directed_path", "") or "").strip()
            if not any((port, profile_path, directed_path)):
                return
            filled: List[str] = []
            preserved: List[str] = []
            if port and not js8_port_edit.text().strip():
                js8_port_edit.setText(port)
                filled.append("JS8Call port")
            elif port:
                preserved.append("JS8Call port")
            if profile_path and not js8_profile_edit.text().strip():
                js8_profile_edit.setText(profile_path)
                filled.append("JS8 profile folder")
            elif profile_path:
                preserved.append("JS8 profile folder")
            if directed_path and not js8_directed_edit.text().strip():
                js8_directed_edit.setText(directed_path)
                filled.append("JS8Call DIRECTED.TXT")
            elif directed_path:
                preserved.append("JS8Call DIRECTED.TXT")
            if filled:
                configure_auto_status.setText("Using selected JS8Call profile for this radio. Filled: " + ", ".join(filled) + ".")
            elif preserved:
                configure_auto_status.setText(
                    "Kept existing JS8Call profile fields. Clear a field first to use that value from the selected profile."
                )
            _update_app_choice_visibility()
            _update_dialog_readiness()

        def _update_app_choice_visibility() -> None:
            observer_mode = str(device_class_combo.currentData() or "").strip().lower() == "observer"
            any_visible = False
            for app_id, combo in app_choice_combos.items():
                visible = (
                    not observer_mode
                    and _app_choice_app_selected(app_id)
                    and combo.count() > 2
                )
                _set_row_visible(combo, visible)
                any_visible = any_visible or visible
            js8_port_text = js8_port_edit.text().strip()
            js8_port_has_one_match = False
            if js8_port_text:
                js8_port_matches = 0
                for idx in range(1, js8_profile_choice_combo.count()):
                    payload = js8_profile_choice_combo.itemData(idx)
                    if isinstance(payload, Mapping) and str(payload.get("port", "") or "").strip() == js8_port_text:
                        js8_port_matches += 1
                js8_port_has_one_match = js8_port_matches == 1
            js8_profile_details_present = bool(js8_profile_edit.text().strip() and js8_directed_edit.text().strip())
            js8_profile_visible = (
                not observer_mode
                and _js8_app_selected()
                and js8_profile_choice_combo.count() > 2
                and not (js8_port_has_one_match and js8_profile_details_present)
            )
            _set_row_visible(js8_profile_choice_combo, js8_profile_visible)
            any_visible = any_visible or js8_profile_visible
            app_choice_group.setVisible(any_visible)

        def _update_detected_app_choices(candidates: Sequence[Any]) -> None:
            for app_id, combo in app_choice_combos.items():
                choices = self._guided_app_candidate_choices(candidates, app_id)
                combo.blockSignals(True)
                combo.clear()
                combo.addItem("Choose detected app...", "")
                for label_text, path_text in choices:
                    combo.addItem(label_text, path_text)
                combo.blockSignals(False)
                _configure_combo_width(combo, minimum=360)
            _update_app_choice_visibility()

        def _update_js8_profile_choices(profiles: Sequence[Any]) -> None:
            choices = self._guided_js8_profile_choices(profiles)
            js8_profile_choice_combo.blockSignals(True)
            js8_profile_choice_combo.clear()
            js8_profile_choice_combo.addItem("Choose JS8Call profile...", {})
            for label_text, payload in choices:
                js8_profile_choice_combo.addItem(label_text, payload)
            js8_profile_choice_combo.blockSignals(False)
            _configure_combo_width(js8_profile_choice_combo, minimum=420)
            _update_app_choice_visibility()

        def _update_port_prompt_visibility() -> None:
            current_ports = {
                "flrig_port": flrig_port_edit.text().strip(),
                "fldigi_port": fldigi_port_edit.text().strip(),
                "js8_port": js8_port_edit.text().strip(),
            }
            missing_keys = set(
                self._guided_port_prompt_keys(
                    current=current_ports,
                    selected={
                        "flrig": use_flrig_chk.isChecked(),
                        "fldigi": use_fldigi_chk.isChecked(),
                        "js8call": use_js8call_chk.isChecked(),
                    },
                    backend=str(backend_combo.currentData() or ""),
                    observer_mode=str(device_class_combo.currentData() or "").strip().lower() == "observer",
                )
            )
            any_visible = False
            for field_key, prompt_edit in port_prompt_fields.items():
                visible = field_key in missing_keys
                _set_row_visible(prompt_edit, visible)
                any_visible = any_visible or visible
            port_prompt_group.setVisible(any_visible)

        for app_id, combo in app_choice_combos.items():
            combo.currentIndexChanged.connect(lambda _idx, key=app_id: _apply_detected_app_choice(key))
        js8_profile_choice_combo.currentIndexChanged.connect(lambda _idx: _apply_js8_profile_choice())

        def _populate_radio_model_combo(entries: List[Dict[str, Any]], *, selected_text: str = "") -> None:
            current_text = selected_text or radio_model_combo.currentText().strip()
            if current_text.casefold() == radio_model_prompt.casefold():
                current_text = ""
            radio_model_combo.blockSignals(True)
            radio_model_combo.clear()
            radio_model_combo.addItem("", {})
            for entry in entries:
                radio_model_combo.addItem(str(entry.get("display_name", "") or ""), dict(entry))
            radio_model_combo.setCurrentIndex(0)
            if current_text:
                matched_idx = -1
                for idx in range(1, radio_model_combo.count()):
                    data = radio_model_combo.itemData(idx)
                    if isinstance(data, dict) and str(data.get("display_name", "") or "").strip().casefold() == current_text.casefold():
                        matched_idx = idx
                        break
                if matched_idx >= 0:
                    radio_model_combo.setCurrentIndex(matched_idx)
                else:
                    radio_model_combo.setEditText(current_text)
            radio_model_combo.blockSignals(False)
            radio_model_completer = QCompleter([str(entry.get("display_name", "") or "") for entry in entries], dlg)
            radio_model_completer.setCaseSensitivity(Qt.CaseInsensitive)
            radio_model_completer.setFilterMode(Qt.MatchContains)
            radio_model_combo.setCompleter(radio_model_completer)
            _configure_combo_width(radio_model_combo, minimum=280)

        _populate_radio_model_combo(catalog_entries)

        existing_catalog_entry = find_radio_catalog_entry(
            catalog_entries,
            catalog_id=str((existing or {}).get("radio_catalog_id", "") or ""),
            manufacturer=str((existing or {}).get("radio_manufacturer", "") or ""),
            model_name=str((existing or {}).get("radio_model", "") or ""),
            display_name=self._device_radio_model_summary(existing or {}),
        )
        if existing_catalog_entry:
            for idx in range(radio_model_combo.count()):
                data = radio_model_combo.itemData(idx)
                if isinstance(data, dict) and str(data.get("catalog_id", "") or "") == str(
                    existing_catalog_entry.get("catalog_id", "") or ""
                ):
                    radio_model_combo.setCurrentIndex(idx)
                    break
        else:
            existing_model_text = self._device_radio_model_summary(existing or {})
            if existing_model_text and existing_model_text != "--":
                radio_model_combo.setEditText(existing_model_text)

        def _current_radio_model_payload() -> Dict[str, str]:
            typed = radio_model_combo.currentText().strip()
            if typed.casefold() == radio_model_prompt.casefold():
                typed = ""
            matched = find_radio_catalog_entry(catalog_entries, display_name=typed)
            if matched:
                return {
                    "catalog_id": str(matched.get("catalog_id", "") or ""),
                    "manufacturer": str(matched.get("manufacturer", "") or ""),
                    "model_name": str(matched.get("model_name", "") or ""),
                    "display_name": str(matched.get("display_name", "") or typed),
                }
            data = radio_model_combo.currentData()
            if isinstance(data, dict) and str(data.get("catalog_id", "") or "").strip():
                return {
                    "catalog_id": str(data.get("catalog_id", "") or ""),
                    "manufacturer": str(data.get("manufacturer", "") or ""),
                    "model_name": str(data.get("model_name", "") or ""),
                    "display_name": str(data.get("display_name", "") or typed),
                }
            return {
                "catalog_id": "",
                "manufacturer": "",
                "model_name": typed,
                "display_name": typed,
            }

        use_flrig_chk.setChecked(self._radio_software_enabled(existing or {}, "flrig"))
        use_fldigi_chk.setChecked(
            self._radio_software_enabled(existing or {}, "fldigi")
        )
        use_flmsg_chk.setChecked(self._radio_software_enabled(existing or {}, "flmsg"))
        use_flamp_chk.setChecked(self._radio_software_enabled(existing or {}, "flamp"))
        use_js8call_chk.setChecked(self._radio_software_enabled(existing or {}, "js8call"))
        use_js8spotter_chk.setChecked(self._radio_software_enabled(existing or {}, "js8spotter"))
        use_commstat_chk.setChecked(self._radio_software_enabled(existing or {}, "commstat"))
        use_varac_chk.setChecked(self._radio_software_enabled(existing or {}, "varac"))
        optional_toggle.setChecked(
            any(
                [
                    ptt_group_edit.text().strip(),
                    antenna_group_edit.text().strip(),
                    frontend_group_edit.text().strip(),
                    amplifier_group_edit.text().strip(),
                    notes_edit.toPlainText().strip(),
                ]
            )
        )

        def _set_row_visible(widget: QWidget, visible: bool) -> None:
            widget.setVisible(bool(visible))
            label_widget = row_labels.get(widget)
            if label_widget is not None:
                label_widget.setVisible(bool(visible))

        def _update_radio_model_hint() -> None:
            nonlocal display_name_user_edited, last_catalog_display_name
            selected = _current_radio_model_payload()
            display_name = str(selected.get("display_name", "") or "").strip()
            matched = find_radio_catalog_entry(
                catalog_entries,
                catalog_id=str(selected.get("catalog_id", "") or ""),
                manufacturer=str(selected.get("manufacturer", "") or ""),
                model_name=str(selected.get("model_name", "") or ""),
                display_name=display_name,
            )
            control_methods = catalog_entry_control_methods(matched)
            control_labels = ", ".join(self._device_backend_label(name) for name in control_methods if name != "manual")
            if display_name:
                if catalog_source == "hamlib-rigctl":
                    model_hint_label.setText(
                        f"Using local Hamlib rig data. Selected model: {display_name}. "
                        f"Suggested control methods: {control_labels or 'Manual'}. "
                        "You can keep this as the radio name or give the station a more specific label."
                    )
                else:
                    model_hint_label.setText(
                        f"Using the bundled fallback radio list. Selected model: {display_name}. "
                        f"Suggested control methods: {control_labels or 'Manual'}. "
                        "If Hamlib is installed later, the picker can refresh to a broader local catalog."
                    )
                current_name = name_edit.text().strip()
                can_replace = (
                    not current_name
                    or current_name == last_catalog_display_name
                    or not display_name_user_edited
                )
                if can_replace:
                    name_edit.setText(display_name)
                    last_catalog_display_name = display_name
                    display_name_user_edited = False
            else:
                if catalog_source == "hamlib-rigctl":
                    model_hint_label.setText("Search the local Hamlib rig list by manufacturer or model name.")
                else:
                    model_hint_label.setText("Search the bundled fallback radio list by manufacturer or model name.")

        def _mark_radio_name_user_edited(_text: str) -> None:
            nonlocal display_name_user_edited
            display_name_user_edited = True

        name_edit.textEdited.connect(_mark_radio_name_user_edited)

        def _draft_radio_profile() -> Dict[str, Any]:
            model_choice = _current_radio_model_payload()
            draft_id = int((existing or {}).get("id", 0) or -1)
            return {
                "id": draft_id,
                "name": name_edit.text().strip() or str(model_choice.get("display_name", "") or "").strip() or "Radio",
                "radio_catalog_id": str(model_choice.get("catalog_id", "") or ""),
                "radio_manufacturer": str(model_choice.get("manufacturer", "") or ""),
                "radio_model": str(model_choice.get("model_name", "") or ""),
                "enabled": int((existing or {}).get("enabled", 1) or 1),
                "runtime_active": int((existing or {}).get("runtime_active", 0) or 0),
                "runtime_primary": int((existing or {}).get("runtime_primary", 0) or 0),
                "device_class": str(device_class_combo.currentData() or "tx_rx"),
                "control_backend": str(backend_combo.currentData() or "flrig"),
                "deployment_mode": str(deploy_combo.currentData() or "full"),
                "use_flrig": bool(use_flrig_chk.isChecked()),
                "use_fldigi": bool(use_fldigi_chk.isChecked()),
                "use_flmsg": bool(use_flmsg_chk.isChecked()),
                "use_flamp": bool(use_flamp_chk.isChecked()),
                "use_js8call": bool(use_js8call_chk.isChecked()),
                "use_js8spotter": bool(use_js8spotter_chk.isChecked()),
                "use_commstat": bool(use_commstat_chk.isChecked()),
                "use_varac": bool(use_varac_chk.isChecked()),
                "rig_host": rig_host_edit.text().strip(),
                "rig_port": rig_port_edit.text().strip(),
                "flrig_host": flrig_host_edit.text().strip(),
                "flrig_port": flrig_port_edit.text().strip(),
                "flrig_path": flrig_path_edit.text().strip(),
                "fldigi_host": fldigi_host_edit.text().strip(),
                "fldigi_port": fldigi_port_edit.text().strip(),
                "fldigi_path": fldigi_path_edit.text().strip(),
                "flmsg_path": flmsg_path_edit.text().strip(),
                "flamp_path": flamp_path_edit.text().strip(),
                "js8_host": js8_host_edit.text().strip(),
                "js8_port": js8_port_edit.text().strip(),
                "js8_install_path": js8_install_edit.text().strip(),
                "js8_profile_path": js8_profile_edit.text().strip(),
                "js8_directed_path": js8_directed_edit.text().strip(),
                "js8_forms_path": js8_forms_edit.text().strip(),
                "spotter_launch_path": js8spotter_launch_edit.text().strip(),
                "commstat_launch_path": commstat_launch_edit.text().strip(),
                "varac_install_path": varac_install_edit.text().strip(),
                "varac_db_path": varac_db_edit.text().strip(),
                "varac_ini_path": varac_ini_edit.text().strip(),
                "varac_incoming_path": varac_incoming_edit.text().strip(),
                "launch_cmd": varac_launch_cmd_edit.text().strip(),
                "launch_enabled": bool(launch_enabled_chk.isChecked()),
                "launch_path": launch_path_edit.text().strip(),
                "sdr_host": sdr_host_edit.text().strip(),
                "sdr_port": sdr_port_edit.text().strip(),
                "ptt_group": ptt_group_edit.text().strip(),
                "antenna_group": antenna_group_edit.text().strip(),
                "frontend_group": frontend_group_edit.text().strip(),
                "amplifier_group": amplifier_group_edit.text().strip(),
                "notes": notes_edit.toPlainText().strip(),
            }

        def _default_instance_port(service: str) -> str:
            try:
                profiles = list(self.device_profiles)
            except Exception:
                profiles = []
            return self._next_default_instance_port(
                service,
                profiles,
                existing_profile_id=int((existing or {}).get("id", 0) or 0),
            )

        def _fill_blank(edit: QLineEdit, value: object, label: str, filled: List[str], preserved: List[str]) -> None:
            current = edit.text().strip()
            text = str(value or "").strip()
            if not text:
                return
            if current:
                preserved.append(label)
                return
            edit.setText(text)
            filled.append(label)

        def _guided_plan_instance_name() -> str:
            model_choice = _current_radio_model_payload()
            raw_name = (
                name_edit.text().strip()
                or str(model_choice.get("display_name", "") or "").strip()
                or str((existing or {}).get("name", "") or "").strip()
                or "radio"
            )
            slug = re.sub(r"[^a-z0-9]+", "-", raw_name.strip().lower()).strip("-")
            return slug or "radio"

        def _guided_plan_port_assignment(service: str, port_text: str) -> PortAssignment:
            defaults = DEFAULT_PORT_PLAN.get(service, ())
            preferred = int(defaults[0]) if defaults else 0
            try:
                assigned = int(str(port_text or "").strip() or preferred)
            except Exception:
                assigned = preferred
            return PortAssignment(
                service=service,
                host="127.0.0.1",
                preferred_port=preferred,
                assigned_port=assigned,
                conflict=False,
                conflict_checked=False,
                note="Draft Guided Add Radio plan; live conflicts are checked before external app writes.",
            )

        def _guided_plan_enabled_apps() -> Tuple[str, ...]:
            backend = str(backend_combo.currentData() or "").strip().lower()
            apps: List[str] = []
            if use_flrig_chk.isChecked() or backend == "flrig":
                apps.append("flrig")
            if use_fldigi_chk.isChecked():
                apps.append("fldigi")
            if use_js8call_chk.isChecked() or backend == "js8call":
                apps.append("js8call")
            return tuple(apps)

        def _update_guided_app_setup_plan_review() -> None:
            if str(device_class_combo.currentData() or "").strip().lower() == "observer":
                app_setup_plan_group.setVisible(False)
                app_setup_plan_label.setText("")
                return
            enabled_apps = _guided_plan_enabled_apps()
            varac_selected = use_varac_chk.isChecked()
            if not enabled_apps and not varac_selected:
                app_setup_plan_group.setVisible(False)
                app_setup_plan_label.setText("")
                return
            proposal = RadioInstanceProposal(
                name=name_edit.text().strip() or "Radio",
                instance_name=_guided_plan_instance_name(),
                index=0,
                enabled_apps=enabled_apps,
                ports=(
                    _guided_plan_port_assignment("flrig", flrig_port_edit.text()),
                    _guided_plan_port_assignment("fldigi", fldigi_port_edit.text()),
                    _guided_plan_port_assignment("js8call", js8_port_edit.text()),
                ),
                varac_enabled=varac_selected,
            )
            app_paths = {
                "flrig": flrig_path_edit.text().strip(),
                "fldigi": fldigi_path_edit.text().strip(),
                "js8call": js8_install_edit.text().strip(),
            }
            plan = build_guided_external_app_config_plan(
                (proposal,),
                config_root=get_config_dir(),
                app_paths=app_paths,
                include_varac=varac_selected,
            )
            write_actions = [action for action in plan.actions if action.writes_external_config]
            lines: List[str] = []
            if plan.backup_required:
                lines.append("Backup required before FIO writes app profiles.")
            for action in write_actions[:4]:
                lines.append("- " + action.summary)
            if len(write_actions) > 4:
                lines.append(f"- {len(write_actions) - 4} more app setup action(s).")
            for item in plan.review_items[:2]:
                lines.append("- " + item)
            app_setup_plan_label.setText("\n".join(lines))
            app_setup_plan_group.setVisible(bool(lines))

        def _apply_dialog_autoconfigure() -> None:
            filled: List[str] = []
            preserved: List[str] = []
            observer_mode = str(device_class_combo.currentData() or "").strip().lower() == "observer"
            install_candidates: Sequence[Any] = ()
            fast_results: Dict[str, PathDetectionResult] = {}
            js8_results: Dict[str, PathDetectionResult] = {}
            varac_results: Dict[str, PathDetectionResult] = {}
            js8_file_profiles: Sequence[Any] = ()
            if not observer_mode:
                try:
                    install_candidates = build_autoconfig_proposal(
                        radio_count=1,
                        home=Path.home(),
                        busy_checker=lambda _host, _port: False,
                    ).candidates
                except Exception:
                    install_candidates = ()
                fast_results = self.software_path_detector.detect_fast_light()
                js8_results = self.software_path_detector.detect_js8()
                varac_results = self.software_path_detector.detect_varac()
                js8_file_profiles = discover_js8call_file_profiles()
            _update_detected_app_choices(install_candidates)
            _update_js8_profile_choices(js8_file_profiles)
            suggestions, review_items = self._guided_radio_autofill_suggestions(
                current={
                    "js8_port": js8_port_edit.text().strip(),
                },
                selected={
                    "flrig": use_flrig_chk.isChecked(),
                    "fldigi": use_fldigi_chk.isChecked(),
                    "flmsg": use_flmsg_chk.isChecked(),
                    "flamp": use_flamp_chk.isChecked(),
                    "js8call": use_js8call_chk.isChecked(),
                    "js8spotter": use_js8spotter_chk.isChecked(),
                    "commstat": use_commstat_chk.isChecked(),
                    "varac": use_varac_chk.isChecked(),
                },
                backend=str(backend_combo.currentData() or ""),
                observer_mode=observer_mode,
                install_candidates=install_candidates,
                fast_results=fast_results,
                js8_results=js8_results,
                varac_results=varac_results,
                js8_file_profiles=js8_file_profiles,
                default_ports={
                    "flrig": _default_instance_port("flrig"),
                    "fldigi": _default_instance_port("fldigi"),
                    "js8call": _default_instance_port("js8call"),
                },
                profile_name=name_edit.text().strip(),
            )
            field_targets: Dict[str, Tuple[QLineEdit, str]] = {
                "sdr_host": (sdr_host_edit, "Observer SDR host"),
                "flrig_host": (flrig_host_edit, "FLRig host"),
                "flrig_port": (flrig_port_edit, "FLRig port"),
                "flrig_path": (flrig_path_edit, "FLRig app"),
                "fldigi_host": (fldigi_host_edit, "FLDigi host"),
                "fldigi_port": (fldigi_port_edit, "FLDigi port"),
                "fldigi_path": (fldigi_path_edit, "FLDigi app"),
                "flmsg_path": (flmsg_path_edit, "FLMsg app"),
                "flamp_path": (flamp_path_edit, "FLAmp app"),
                "js8_host": (js8_host_edit, "JS8Call host"),
                "js8_port": (js8_port_edit, "JS8Call port"),
                "js8_install_path": (js8_install_edit, "JS8Call app"),
                "js8_directed_path": (js8_directed_edit, "JS8Call DIRECTED.TXT"),
                "js8_profile_path": (js8_profile_edit, "JS8 profile folder"),
                "spotter_launch_path": (js8spotter_launch_edit, "JS8Spotter app"),
                "commstat_launch_path": (commstat_launch_edit, "CommStat app"),
                "varac_install_path": (varac_install_edit, "VarAC install"),
                "varac_ini_path": (varac_ini_edit, "VarAC INI"),
                "varac_incoming_path": (varac_incoming_edit, "VarAC incoming"),
            }
            for field_key, value in suggestions.items():
                target = field_targets.get(field_key)
                if target is None:
                    continue
                edit, label = target
                _fill_blank(edit, value, label, filled, preserved)
            review = list(review_items)
            if filled:
                review.insert(0, "Filled: " + ", ".join(filled[:8]) + ("..." if len(filled) > 8 else ""))
            if preserved:
                review.append("Kept existing: " + ", ".join(preserved[:6]) + ("..." if len(preserved) > 6 else ""))
            if not review:
                review.append("No blank fields could be filled from the current scan.")
            status = (
                f"Configure Automatically filled {len(filled)} field(s). Review before Save."
                if filled
                else "Configure Automatically did not find new blank fields to fill."
            )
            visible_review = "\n".join(review[:4])
            if len(review) > 4:
                visible_review += f"\n{len(review) - 4} more review item(s)."
            configure_auto_status.setText(f"{status}\n{visible_review}" if visible_review else status)
            configure_auto_status.setToolTip("\n".join(review))
            _update_guided_app_setup_plan_review()
            _update_port_prompt_visibility()
            _update_dialog_visibility()

        configure_auto_btn.clicked.connect(_apply_dialog_autoconfigure)

        def _update_dialog_readiness() -> None:
            def _set_readiness_card_style(level: str) -> None:
                theme = resolve_theme(self.settings)
                level_key = (level or "info").strip().lower()
                color_map = {
                    "success": theme.get("success", "#2E7D32"),
                    "warning": theme.get("warning", "#C99700"),
                    "danger": theme.get("danger", "#C62828"),
                    "info": theme.get("info", theme.get("accent", "#1565C0")),
                }
                accent = QColor(color_map.get(level_key, color_map["info"]))
                bg = QColor(accent)
                bg.setAlpha(24)
                border = QColor(accent)
                border.setAlpha(140)
                text_color = theme.get("text", "#1C1F21")
                muted_color = theme.get("text_muted", text_color)
                readiness_card.setStyleSheet(
                    "QFrame {"
                    f" background-color: {bg.name(QColor.HexArgb)};"
                    f" border: 1px solid {border.name(QColor.HexArgb)};"
                    " border-radius: 8px;"
                    "}"
                    f" QLabel {{ color: {text_color}; border: none; background: transparent; }}"
                    f" QLabel#readinessIntro {{ color: {muted_color}; }}"
                )

            draft = _draft_radio_profile()
            try:
                other_profiles = [
                    dict(row)
                    for row in self.device_profiles
                    if isinstance(row, dict) and int(row.get("id", 0) or 0) != int(draft.get("id", 0) or 0)
                ]
            except Exception:
                other_profiles = []
            readiness = build_station_readiness_report(
                self._settings_snapshot_for_readiness(),
                device_profiles=other_profiles + [draft],
                operating_groups=self.operating_groups,
            )
            summary = readiness.summary_for_radio(int(draft.get("id", 0) or 0))
            global_items = [
                issue.message
                for issue in readiness.issues
                if issue.scope == "global" and issue.severity in {"required", "recommended"}
            ]
            if summary is None:
                readiness_card.setVisible(True)
                _set_readiness_card_style("info")
                dialog_readiness_status.setText("Radio readiness is not available yet.")
                dialog_readiness_detail.setText("")
                return
            readiness_card.setVisible(str(summary.overall_state or "").strip().lower() != "ready")
            if not readiness_card.isVisible():
                return
            _set_readiness_card_style(readiness_state_card_level(summary.overall_state))
            dialog_readiness_status.setText(readiness_summary_status_text(summary, subject="This radio"))
            detail_parts = list(summary.messages[:4])
            if global_items:
                detail_parts.append("Station prerequisites: " + "; ".join(global_items[:2]))
            if detail_parts:
                dialog_readiness_detail.setText(
                    f"{readiness_state_description(summary.overall_state)} " + " | ".join(detail_parts)
                )
            else:
                dialog_readiness_detail.setText(readiness_state_description(summary.overall_state))

        def _update_dialog_visibility() -> None:
            backend = str(backend_combo.currentData() or "flrig").strip().lower()
            device_class_value = str(device_class_combo.currentData() or "tx_rx").strip().lower()
            observer_mode = device_class_value == "observer"
            use_flrig = bool(use_flrig_chk.isChecked())
            use_fldigi = bool(use_fldigi_chk.isChecked())
            use_js8call = bool(use_js8call_chk.isChecked())
            use_js8spotter = bool(use_js8spotter_chk.isChecked())
            use_commstat = bool(use_commstat_chk.isChecked())
            use_varac = bool(use_varac_chk.isChecked())

            use_flrig_chk.setEnabled(not observer_mode)
            use_fldigi_chk.setEnabled(not observer_mode)
            use_flmsg_chk.setEnabled(not observer_mode)
            use_flamp_chk.setEnabled(not observer_mode)
            use_js8call_chk.setEnabled(not observer_mode)
            use_js8spotter_chk.setEnabled(not observer_mode)
            use_commstat_chk.setEnabled(not observer_mode)
            use_varac_chk.setEnabled(not observer_mode)
            if backend == "flrig" and not use_flrig_chk.isChecked():
                use_flrig_chk.setChecked(True)
                use_flrig = True
            if backend == "js8call" and not use_js8call_chk.isChecked():
                use_js8call_chk.setChecked(True)
                use_js8call = True
            if use_js8spotter or use_commstat:
                if not use_js8call_chk.isChecked():
                    use_js8call_chk.setChecked(True)
                    use_js8call = True

            for widget in flrig_field_widgets:
                _set_row_visible(widget, use_flrig and not observer_mode)
            for widget in rigctld_field_widgets:
                _set_row_visible(widget, (backend == "rigctld") and not observer_mode)
            for widget in js8_field_widgets:
                _set_row_visible(widget, use_js8call and not observer_mode)
            for widget in observer_field_widgets:
                _set_row_visible(widget, observer_mode)
            for widget in fldigi_field_widgets:
                _set_row_visible(widget, not observer_mode and use_fldigi)
            for widget in flmsg_field_widgets:
                _set_row_visible(widget, not observer_mode and bool(use_flmsg_chk.isChecked()))
            for widget in flamp_field_widgets:
                _set_row_visible(widget, not observer_mode and bool(use_flamp_chk.isChecked()))
            for widget in varac_field_widgets:
                _set_row_visible(widget, not observer_mode and use_varac)
            for widget in optional_field_widgets:
                _set_row_visible(widget, optional_toggle.isChecked())

            if observer_mode:
                _set_row_visible(software_wrap, False)
                _set_row_visible(software_hint_label, False)
                app_setup_plan_group.setVisible(False)
                app_setup_plan_label.setText("")
            else:
                _set_row_visible(software_wrap, True)
                _set_row_visible(software_hint_label, True)
                software_parts = []
                if use_flrig:
                    software_parts.append("FLRig")
                elif backend == "rigctld":
                    software_parts.append("RigCtlD")
                elif backend == "manual":
                    software_parts.append("Manual")
                if use_fldigi:
                    software_parts.append("FLDigi")
                if bool(use_flmsg_chk.isChecked()):
                    software_parts.append("FLMsg")
                if bool(use_flamp_chk.isChecked()):
                    software_parts.append("FLAmp")
                if use_js8call:
                    software_parts.append("JS8Call")
                if use_js8spotter:
                    software_parts.append("JS8Spotter")
                if use_commstat:
                    software_parts.append("CommStat")
                if use_varac:
                    software_parts.append("VarAC")
                software_hint_label.setText(
                    "This radio's current software bundle is: "
                    + ", ".join(software_parts)
                    + ". Hidden sections stay unchanged unless you edit their values."
                )
                if app_setup_plan_group.isVisible():
                    _update_guided_app_setup_plan_review()

            if observer_mode:
                role_hint_label.setText(
                    "Observer radios track or monitor RF activity without taking over the default control shell. "
                    "Set the SDR endpoint if this observer should be tracked in readiness and runtime summaries."
                )
            elif backend == "js8call":
                role_hint_label.setText(
                    "Primary rig control is JS8Call for this radio. The JS8 endpoint drives control, but this radio can still participate in other software options such as VarAC or Fast Light."
                )
            elif backend == "rigctld":
                role_hint_label.setText(
                    "Primary rig control is RigCtlD for this radio. Use the software stack above to show the other applications that belong to this radio's working bundle."
                )
            elif backend == "manual":
                role_hint_label.setText(
                    "Use Manual when FreqInOut should track this radio in planning and coordination, while the rest of the radio bundle remains operator-managed."
                )
            else:
                role_hint_label.setText(
                    "Primary rig control is FLRig for this radio. Turn on the other software that belongs to this radio's single-rig-style operating bundle."
                )
            optional_body.setVisible(bool(optional_toggle.isChecked()))
            optional_toggle.setArrowType(Qt.DownArrow if optional_toggle.isChecked() else Qt.RightArrow)
            _update_app_choice_visibility()
            _update_port_prompt_visibility()
            _update_dialog_readiness()

        backend_combo.currentIndexChanged.connect(_update_dialog_visibility)
        device_class_combo.currentIndexChanged.connect(_update_dialog_visibility)
        use_flrig_chk.stateChanged.connect(lambda _state: _update_dialog_visibility())
        use_fldigi_chk.stateChanged.connect(lambda _state: _update_dialog_visibility())
        use_flmsg_chk.stateChanged.connect(lambda _state: _update_dialog_visibility())
        use_flamp_chk.stateChanged.connect(lambda _state: _update_dialog_visibility())
        use_js8call_chk.stateChanged.connect(lambda _state: _update_dialog_visibility())
        use_js8spotter_chk.stateChanged.connect(lambda _state: _update_dialog_visibility())
        use_commstat_chk.stateChanged.connect(lambda _state: _update_dialog_visibility())
        use_varac_chk.stateChanged.connect(lambda _state: _update_dialog_visibility())
        radio_model_combo.currentTextChanged.connect(lambda _text: _update_radio_model_hint())
        radio_model_combo.currentTextChanged.connect(lambda _text: _update_dialog_readiness())
        optional_toggle.toggled.connect(lambda _checked: _update_dialog_visibility())
        _update_radio_model_hint()
        _update_dialog_visibility()

        def _refresh_radio_catalog() -> None:
            nonlocal catalog_entries, catalog_source
            selected_text = radio_model_combo.currentText().strip()
            payload = load_radio_catalog(force_refresh=True)
            catalog_entries = list(payload.get("entries", []) or [])
            catalog_source = str(payload.get("source", "static-fallback") or "static-fallback")
            _populate_radio_model_combo(catalog_entries, selected_text=selected_text)
            _update_radio_model_hint()
            _update_dialog_readiness()

        refresh_catalog_btn.clicked.connect(_refresh_radio_catalog)

        def _browse_launch_path() -> None:
            start = launch_path_edit.text().strip()
            fn, _ = QFileDialog.getOpenFileName(self, "Select launch path", start)
            if fn:
                launch_path_edit.setText(fn)
                _update_dialog_readiness()

        launch_path_btn.clicked.connect(_browse_launch_path)

        for widget in [
            name_edit,
            rig_host_edit,
            rig_port_edit,
            flrig_host_edit,
            flrig_port_edit,
            flrig_path_edit,
            fldigi_host_edit,
            fldigi_port_edit,
            fldigi_path_edit,
            flmsg_path_edit,
            flamp_path_edit,
            js8_host_edit,
            js8_port_edit,
            js8_install_edit,
            js8_profile_edit,
            js8_directed_edit,
            js8_forms_edit,
            js8spotter_launch_edit,
            commstat_launch_edit,
            varac_install_edit,
            varac_db_edit,
            varac_ini_edit,
            varac_incoming_edit,
            varac_launch_cmd_edit,
            launch_path_edit,
            sdr_host_edit,
            sdr_port_edit,
            ptt_group_edit,
            antenna_group_edit,
            frontend_group_edit,
            amplifier_group_edit,
        ]:
            widget.textChanged.connect(lambda _text: _update_dialog_readiness())
        for widget in [js8_port_edit, js8_profile_edit, js8_directed_edit]:
            widget.textChanged.connect(lambda _text: _update_app_choice_visibility())
        for widget in [flrig_port_edit, fldigi_port_edit, js8_port_edit, *port_prompt_fields.values()]:
            widget.textChanged.connect(lambda _text: _update_port_prompt_visibility())
        notes_edit.textChanged.connect(_update_dialog_readiness)
        launch_enabled_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        use_flrig_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        use_fldigi_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        use_flmsg_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        use_flamp_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        use_js8call_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        use_js8spotter_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        use_commstat_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        use_varac_chk.stateChanged.connect(lambda _state: _update_dialog_readiness())
        backend_combo.currentIndexChanged.connect(lambda _idx: _update_dialog_readiness())
        device_class_combo.currentIndexChanged.connect(lambda _idx: _update_dialog_readiness())
        deploy_combo.currentIndexChanged.connect(lambda _idx: _update_dialog_readiness())

        body_layout.addStretch(1)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        save_button = buttons.button(QDialogButtonBox.Save)
        if save_button is not None:
            save_button.setText(self._device_profile_dialog_save_text(existing))
            save_button.setAccessibleName(self._device_profile_dialog_save_text(existing))
        layout.addWidget(buttons)

        out: Dict[str, Any] = {}

        def _save() -> None:
            model_choice = _current_radio_model_payload()
            name = name_edit.text().strip() or str(model_choice.get("display_name", "") or "").strip()
            if name and not name_edit.text().strip():
                name_edit.setText(name)
            if not name:
                QMessageBox.warning(self, "Validation", "Radio name is required.")
                return
            out.update(
                {
                    "id": (existing or {}).get("id"),
                    "name": name,
                    "radio_catalog_id": str(model_choice.get("catalog_id", "") or ""),
                    "radio_manufacturer": str(model_choice.get("manufacturer", "") or ""),
                    "radio_model": str(model_choice.get("model_name", "") or ""),
                    "device_class": str(device_class_combo.currentData() or "tx_rx"),
                    "control_backend": str(backend_combo.currentData() or "flrig"),
                    "deployment_mode": str(deploy_combo.currentData() or "full"),
                    "use_flrig": bool(use_flrig_chk.isChecked()),
                    "use_fldigi": bool(use_fldigi_chk.isChecked()),
                    "use_flmsg": bool(use_flmsg_chk.isChecked()),
                    "use_flamp": bool(use_flamp_chk.isChecked()),
                    "use_js8call": bool(use_js8call_chk.isChecked()),
                    "use_js8spotter": bool(use_js8spotter_chk.isChecked()),
                    "use_commstat": bool(use_commstat_chk.isChecked()),
                    "use_varac": bool(use_varac_chk.isChecked()),
                    "rig_host": rig_host_edit.text().strip(),
                    "rig_port": rig_port_edit.text().strip(),
                    "flrig_host": flrig_host_edit.text().strip(),
                    "flrig_port": flrig_port_edit.text().strip(),
                    "flrig_path": flrig_path_edit.text().strip(),
                    "fldigi_host": fldigi_host_edit.text().strip(),
                    "fldigi_port": fldigi_port_edit.text().strip(),
                    "fldigi_path": fldigi_path_edit.text().strip(),
                    "flmsg_path": flmsg_path_edit.text().strip(),
                    "flamp_path": flamp_path_edit.text().strip(),
                    "js8_host": js8_host_edit.text().strip(),
                    "js8_port": js8_port_edit.text().strip(),
                    "js8_install_path": js8_install_edit.text().strip(),
                    "js8_profile_path": js8_profile_edit.text().strip(),
                    "js8_directed_path": js8_directed_edit.text().strip(),
                    "js8_forms_path": js8_forms_edit.text().strip(),
                    "spotter_launch_path": js8spotter_launch_edit.text().strip(),
                    "commstat_launch_path": commstat_launch_edit.text().strip(),
                    "varac_install_path": varac_install_edit.text().strip(),
                    "varac_db_path": varac_db_edit.text().strip(),
                    "varac_ini_path": varac_ini_edit.text().strip(),
                    "varac_incoming_path": varac_incoming_edit.text().strip(),
                    "launch_cmd": varac_launch_cmd_edit.text().strip(),
                    "launch_enabled": bool(launch_enabled_chk.isChecked()),
                    "launch_path": launch_path_edit.text().strip(),
                    "sdr_host": sdr_host_edit.text().strip(),
                    "sdr_port": sdr_port_edit.text().strip(),
                    "ptt_group": ptt_group_edit.text().strip(),
                    "antenna_group": antenna_group_edit.text().strip(),
                    "frontend_group": frontend_group_edit.text().strip(),
                    "amplifier_group": amplifier_group_edit.text().strip(),
                    "notes": notes_edit.toPlainText().strip(),
                }
            )
            dlg.accept()

        buttons.accepted.connect(_save)
        buttons.rejected.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out

    def _apply_runtime_projection_widgets(self, data: Dict[str, Any]) -> None:
        ctrl = data.get("control_via", "FLRig") or "FLRig"
        allowed_ctrl = ["FLRig", "RIGCTLD", "JS8Call", "Manual"]
        if ctrl not in allowed_ctrl:
            ctrl = "FLRig"
        self.control_combo.setCurrentText(ctrl)
        self.use_scheduler_chk.setChecked(bool(data.get("use_scheduler", True)))
        self.launch_all_with_startup_chk.setChecked(bool(data.get("launch_control_enabled", True)))
        self._launch_items_cache = self.launch_orchestrator.get_launch_items()
        self._refresh_launch_control_table()

    def _refresh_runtime_projection_ui(self, *, refresh_multi_radio: bool = False, emit_saved: bool = False) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        data = self.settings.all()
        previous_loading = self._loading_settings
        self._loading_settings = True
        try:
            self._apply_runtime_projection_widgets(data)
        finally:
            self._loading_settings = previous_loading
        if refresh_multi_radio:
            self._refresh_multi_radio_tables(refresh_section_titles=False)
        self._update_launch_control_buttons()
        self._update_launch_selected_state()
        self._update_device_profile_action_buttons()
        self._update_operating_profile_action_buttons()
        self._update_device_assignment_action_buttons()
        self._refresh_section_titles()
        self._refresh_contextual_autofill_buttons()
        self._refresh_running_status_compat(force=True)
        if emit_saved:
            try:
                self.settings_saved.emit()
            except Exception:
                pass

    def _persist_device_profile(self, values: Dict[str, Any], *, existing: Optional[Dict[str, Any]] = None) -> None:
        is_active_edit = bool(existing and int(existing.get("runtime_active", 0) or 0) == 1)
        is_primary_edit = bool(existing and int(existing.get("runtime_primary", 0) or 0) == 1)
        payload = dict(values)
        radio_name = str(payload.get("name", (existing or {}).get("name", "Radio")) or "Radio").strip() or "Radio"
        target_backend = str(payload.get("control_backend", (existing or {}).get("control_backend", "")) or "").strip().lower()
        if is_active_edit and target_backend not in SUPPORTED_RUNTIME_CONTROL_BACKENDS:
            QMessageBox.warning(
                self,
                "Radio Profiles",
                f"Cannot save {self._device_backend_label(target_backend)} as the active compatibility device until runtime support exists.",
            )
            return
        if is_primary_edit and not self._confirm_runtime_projection_override("Edit Default Radio"):
            return
        try:
            existing_js8_id = int((existing or {}).get("js8_instance_id", 0) or 0)
            js8_needed = any(
                [
                    bool(payload.get("use_js8call")),
                    bool(payload.get("use_js8spotter")),
                    bool(payload.get("use_commstat")),
                    str(payload.get("js8_host", "") or "").strip(),
                    str(payload.get("js8_port", "") or "").strip(),
                    str(payload.get("js8_install_path", "") or "").strip(),
                    str(payload.get("js8_profile_path", "") or "").strip(),
                    str(payload.get("js8_directed_path", "") or "").strip(),
                    str(payload.get("js8_forms_path", "") or "").strip(),
                    str(payload.get("spotter_launch_path", "") or "").strip(),
                    str(payload.get("commstat_launch_path", "") or "").strip(),
                    existing_js8_id > 0,
                ]
            )
            if js8_needed:
                js8_values = {
                    "id": existing_js8_id or None,
                    "name": f"{radio_name} JS8",
                    "host": str(payload.get("js8_host", "") or "").strip() or "127.0.0.1",
                    "port": int(str(payload.get("js8_port", "") or "2442") or "2442"),
                    "profile_path": str(payload.get("js8_profile_path", "") or "").strip(),
                    "directed_path": str(payload.get("js8_directed_path", "") or "").strip(),
                    "forms_path": str(payload.get("js8_forms_path", "") or "").strip(),
                    "install_path": str(payload.get("js8_install_path", "") or "").strip(),
                    "spotter_launch_path": str(payload.get("spotter_launch_path", "") or "").strip(),
                    "commstat_launch_path": str(payload.get("commstat_launch_path", "") or "").strip(),
                }
                js8_saved = self.multi_radio_store.save_js8_instance(js8_values)
                payload["js8_instance_id"] = int(js8_saved.get("id", 0) or 0)

            existing_fast_id = int((existing or {}).get("fast_light_config_id", 0) or 0)
            fast_needed = any(
                [
                    bool(payload.get("use_flrig")),
                    bool(payload.get("use_fldigi")),
                    str(payload.get("flrig_host", "") or "").strip(),
                    str(payload.get("flrig_port", "") or "").strip(),
                    str(payload.get("flrig_path", "") or "").strip(),
                    str(payload.get("fldigi_host", "") or "").strip(),
                    str(payload.get("fldigi_port", "") or "").strip(),
                    str(payload.get("fldigi_path", "") or "").strip(),
                    existing_fast_id > 0,
                ]
            )
            if fast_needed:
                fast_values = {
                    "id": existing_fast_id or None,
                    "name": f"{radio_name} Fast Light",
                    "flrig_path": str(payload.get("flrig_path", "") or "").strip(),
                    "flrig_host": str(payload.get("flrig_host", "") or "").strip() or "127.0.0.1",
                    "flrig_port": int(str(payload.get("flrig_port", "") or "12345") or "12345"),
                    "fldigi_path": str(payload.get("fldigi_path", "") or "").strip(),
                    "fldigi_host": str(payload.get("fldigi_host", "") or "").strip()
                    or str(payload.get("flrig_host", "") or "").strip()
                    or "127.0.0.1",
                    "fldigi_port": int(str(payload.get("fldigi_port", "") or "7362") or "7362"),
                }
                fast_saved = self.multi_radio_store.save_fast_light_config(fast_values)
                payload["fast_light_config_id"] = int(fast_saved.get("id", 0) or 0)

            existing_varac_id = int((existing or {}).get("varac_node_id", 0) or 0)
            varac_needed = any(
                [
                    bool(payload.get("use_varac")),
                    str(payload.get("varac_install_path", "") or "").strip(),
                    str(payload.get("varac_db_path", "") or "").strip(),
                    str(payload.get("varac_ini_path", "") or "").strip(),
                    str(payload.get("varac_incoming_path", "") or "").strip(),
                    str(payload.get("launch_cmd", "") or "").strip(),
                    existing_varac_id > 0,
                ]
            )
            if varac_needed:
                varac_values = {
                    "id": existing_varac_id or None,
                    "name": f"{radio_name} VarAC",
                    "install_path": str(payload.get("varac_install_path", "") or "").strip(),
                    "db_path": str(payload.get("varac_db_path", "") or "").strip(),
                    "ini_path": str(payload.get("varac_ini_path", "") or "").strip(),
                    "incoming_path": str(payload.get("varac_incoming_path", "") or "").strip(),
                    "launch_cmd": str(payload.get("launch_cmd", "") or "").strip(),
                }
                varac_saved = self.multi_radio_store.save_varac_node(varac_values)
                payload["varac_node_id"] = int(varac_saved.get("id", 0) or 0)

            first_radio = not bool(self.multi_radio_store.list_device_profiles())
            if first_radio:
                payload["runtime_active"] = 1
                payload["runtime_primary"] = 1
            saved = self.multi_radio_store.save_device_profile(payload)
        except ValueError as exc:
            QMessageBox.warning(self, "Radio Profiles", str(exc))
            return
        except Exception:
            log.exception("Failed to save device profile.")
            QMessageBox.warning(self, "Radio Profiles", "Unable to save the radio profile.")
            return

        if first_radio or is_primary_edit or int(saved.get("runtime_primary", 0) or 0) == 1:
            try:
                self.multi_radio_store.sync_runtime_active_device_to_legacy_settings(int(saved.get("id", 0) or 0))
            except ValueError as exc:
                QMessageBox.warning(self, "Radio Profiles", str(exc))
                self._refresh_multi_radio_tables()
                return
            except Exception:
                log.exception("Failed to refresh runtime-primary device projection.")
                QMessageBox.warning(self, "Radio Profiles", "Unable to refresh the runtime compatibility projection.")
                self._refresh_multi_radio_tables()
                return
            self._refresh_runtime_projection_ui(refresh_multi_radio=True, emit_saved=True)
        else:
            self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _add_device_profile(self) -> None:
        created = self._open_device_profile_dialog(existing=None)
        if not created:
            return
        self._persist_device_profile(created)

    def _edit_device_profile(self) -> None:
        selected = self._selected_device_profiles()
        if not selected:
            QMessageBox.information(self, "Advanced Radio Edit", "Select one radio to edit.")
            return
        if len(selected) > 1:
            QMessageBox.warning(self, "Advanced Radio Edit", "Please select only one radio to edit.")
            return
        existing = selected[0]
        updated = self._open_device_profile_dialog(existing=existing)
        if not updated:
            return
        self._persist_device_profile(updated, existing=existing)

    def _set_active_selected_device_profile(self) -> None:
        selected = self._selected_device_profiles()
        if not selected:
            QMessageBox.information(self, "Make Default", "Select one radio to use as the default radio.")
            return
        if len(selected) > 1:
            QMessageBox.warning(self, "Make Default", "Please select only one radio to use as the default radio.")
            return
        target = selected[0]
        if int(target.get("runtime_primary", 0) or 0) == 1:
            QMessageBox.information(self, "Make Default", "That radio is already the default radio.")
            return
        if not self._confirm_runtime_projection_override("Make Default"):
            return
        try:
            self.multi_radio_store.set_runtime_primary_device_profile(int(target.get("id", 0) or 0))
        except ValueError as exc:
            QMessageBox.warning(self, "Make Default", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed to set runtime-primary device profile.")
            QMessageBox.warning(self, "Make Default", "Unable to update the selected radio.")
            self._refresh_multi_radio_tables()
            return
        self._refresh_runtime_projection_ui(refresh_multi_radio=True, emit_saved=True)
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _activate_selected_device_profiles(self) -> None:
        selected = self._selected_device_profiles()
        if not selected:
            QMessageBox.information(self, "Use Now", "Select one or more radios to use now.")
            return
        activated = 0
        for target in selected:
            if int(target.get("runtime_active", 0) or 0) == 1:
                continue
            try:
                self.multi_radio_store.set_device_profile_runtime_active(int(target.get("id", 0) or 0), True)
                activated += 1
            except ValueError as exc:
                QMessageBox.warning(self, "Use Now", str(exc))
                self._refresh_multi_radio_tables()
                return
            except Exception:
                log.exception("Failed to activate device profiles.")
                QMessageBox.warning(self, "Use Now", "Unable to activate the selected radios.")
                self._refresh_multi_radio_tables()
                return
        self._refresh_multi_radio_tables()
        if activated:
            self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _deactivate_selected_device_profiles(self) -> None:
        selected = self._selected_device_profiles()
        if not selected:
            QMessageBox.information(self, "Stop Using Now", "Select one or more radios to stop using now.")
            return
        deactivated = 0
        for target in selected:
            if int(target.get("runtime_active", 0) or 0) != 1:
                continue
            try:
                self.multi_radio_store.set_device_profile_runtime_active(int(target.get("id", 0) or 0), False)
                deactivated += 1
            except ValueError as exc:
                QMessageBox.warning(self, "Stop Using Now", str(exc))
                self._refresh_multi_radio_tables()
                return
            except Exception:
                log.exception("Failed to deactivate device profiles.")
                QMessageBox.warning(
                    self,
                    "Stop Using Now",
                    "Unable to stop using the selected radios.",
                )
                self._refresh_multi_radio_tables()
                return
        self._refresh_multi_radio_tables()
        if deactivated:
            self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _delete_device_profiles(self) -> None:
        selected = self._selected_device_profiles()
        if not selected:
            QMessageBox.information(self, "Delete Radio Profiles", "Select one or more radio profiles to delete.")
            return
        if any(int(row.get("runtime_active", 0) or 0) == 1 for row in selected):
            QMessageBox.warning(
                self,
                "Delete Radio Profiles",
                "Active radios cannot be deleted. Use Stop Using Now first.",
            )
            return
        confirm = QMessageBox.question(
            self,
            "Delete Radio Profiles",
            f"Delete {len(selected)} selected radio profile(s)?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            for row in selected:
                self.multi_radio_store.delete_device_profile(int(row.get("id", 0) or 0))
        except ValueError as exc:
            QMessageBox.warning(self, "Delete Radio Profiles", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed deleting device profiles.")
            QMessageBox.warning(self, "Delete Radio Profiles", "Unable to delete the selected radio profiles.")
            self._refresh_multi_radio_tables()
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")
        QMessageBox.information(
            self,
            "Delete Radio Profiles",
            f"Deleted {len(selected)} radio profile{'s' if len(selected) != 1 else ''}.",
        )

    def _varac_cluster_by_id(self, cluster_id: int) -> Optional[Dict[str, Any]]:
        return next(
            (
                dict(row)
                for row in self.varac_clusters
                if isinstance(row, dict) and int(row.get("id", 0) or 0) == int(cluster_id)
            ),
            None,
        )

    def _open_varac_cluster_dialog(self, existing: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        dlg = QDialog(self)
        dlg.setWindowTitle("Edit VarAC Cluster" if existing else "Add VarAC Cluster")
        dlg.resize(620, 0)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        layout.addLayout(form)

        name_edit = QLineEdit(str((existing or {}).get("name", "") or ""))
        form.addRow("Name:", name_edit)

        cluster_id_edit = QLineEdit(str((existing or {}).get("cluster_id", "") or ""))
        cluster_id_edit.setPlaceholderText("Stable shared VarAC cluster ID")
        form.addRow("Cluster ID:", cluster_id_edit)

        shared_db_edit = QLineEdit(str((existing or {}).get("shared_db_path", "") or ""))
        shared_db_edit.setPlaceholderText("Optional shared VarAC DB path")
        form.addRow("Shared DB Path:", shared_db_edit)

        refresh_edit = QLineEdit(str(int((existing or {}).get("counters_refresh_sec", 30) or 30)))
        refresh_edit.setValidator(QIntValidator(5, 600, refresh_edit))
        form.addRow("Refresh Sec:", refresh_edit)

        ptt_lock_chk = QCheckBox("Enable cluster PTT lock metadata")
        ptt_lock_chk.setChecked(bool((existing or {}).get("ptt_lock_enabled", 0)))
        form.addRow("", ptt_lock_chk)

        gateway_combo = QComboBox()
        gateway_combo.addItem("No gateway handler selected", 0)
        existing_cluster_id = int((existing or {}).get("id", 0) or 0)
        enabled_members = [
            row
            for row in self.varac_cluster_members
            if isinstance(row, dict)
            and int(row.get("cluster_db_id", 0) or 0) == existing_cluster_id
            and int(row.get("enabled", 1) or 0) == 1
        ]
        for row in enabled_members:
            gateway_combo.addItem(
                f"{str(row.get('device_name', '') or 'Device').strip()} (#{int(row.get('instance_number', 0) or 0)})",
                int(row.get("device_profile_id", 0) or 0),
            )
        gateway_device_id = int((existing or {}).get("gateway_handler_device_id", 0) or 0)
        gateway_index = gateway_combo.findData(gateway_device_id)
        gateway_combo.setCurrentIndex(gateway_index if gateway_index >= 0 else 0)
        form.addRow("Gateway Handler:", gateway_combo)

        info_label = QLabel()
        info_label.setWordWrap(True)
        form.addRow("", info_label)

        def _update_hint() -> None:
            if existing_cluster_id <= 0:
                info_label.setText(
                    "Create the cluster first, then assign members in VarAC Memberships. Return here to choose the gateway handler after at least one member is enabled."
                )
                return
            if not enabled_members:
                info_label.setText(
                    "This cluster has no enabled members yet. Assign one or more device profiles in VarAC Memberships before selecting the gateway handler."
                )
                return
            if int(gateway_combo.currentData() or 0) > 0:
                info_label.setText(
                    "The selected gateway handler becomes the exclusive gateway role for this cluster in Phase F Slice 3."
                )
                return
            info_label.setText(
                "Leave Gateway Handler blank for now if the cluster is receive-only or not yet finalized. Multi-member clusters should generally designate one handler."
            )

        gateway_combo.currentIndexChanged.connect(_update_hint)
        _update_hint()

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        layout.addWidget(buttons)

        out: Dict[str, Any] = {}

        def _save() -> None:
            name = name_edit.text().strip()
            if not name:
                QMessageBox.warning(self, "Validation", "VarAC cluster name is required.")
                return
            refresh_text = refresh_edit.text().strip() or "30"
            try:
                refresh_value = int(refresh_text)
            except Exception:
                QMessageBox.warning(self, "Validation", "Refresh seconds must be a valid integer.")
                return
            out.update(
                {
                    "id": (existing or {}).get("id"),
                    "name": name,
                    "cluster_id": cluster_id_edit.text().strip(),
                    "shared_db_path": shared_db_edit.text().strip(),
                    "counters_refresh_sec": refresh_value,
                    "ptt_lock_enabled": bool(ptt_lock_chk.isChecked()),
                    "gateway_handler_device_id": int(gateway_combo.currentData() or 0) or None,
                }
            )
            dlg.accept()

        buttons.accepted.connect(_save)
        buttons.rejected.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out if out else None

    def _persist_varac_cluster(self, values: Dict[str, Any]) -> None:
        try:
            self.multi_radio_store.save_varac_cluster(values)
        except ValueError as exc:
            QMessageBox.warning(self, "VarAC Clusters", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed saving VarAC cluster.")
            QMessageBox.warning(self, "VarAC Clusters", "Unable to save the VarAC cluster.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _add_varac_cluster(self) -> None:
        created = self._open_varac_cluster_dialog(existing=None)
        if created:
            self._persist_varac_cluster(created)

    def _edit_varac_cluster(self) -> None:
        selected = self._selected_varac_clusters()
        if not selected:
            QMessageBox.information(self, "Edit VarAC Cluster", "Select one VarAC cluster to edit.")
            return
        if len(selected) > 1:
            QMessageBox.warning(self, "Edit VarAC Cluster", "Please select only one VarAC cluster to edit.")
            return
        updated = self._open_varac_cluster_dialog(existing=selected[0])
        if updated:
            self._persist_varac_cluster(updated)

    def _delete_varac_clusters(self) -> None:
        selected = self._selected_varac_clusters()
        if not selected:
            QMessageBox.information(self, "Delete VarAC Clusters", "Select one or more VarAC clusters to delete.")
            return
        confirm = QMessageBox.question(
            self,
            "Delete VarAC Clusters",
            f"Delete {len(selected)} selected VarAC cluster(s) and their memberships?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            for row in selected:
                self.multi_radio_store.delete_varac_cluster(int(row.get("id", 0) or 0))
        except Exception:
            log.exception("Failed deleting VarAC clusters.")
            QMessageBox.warning(self, "Delete VarAC Clusters", "Unable to delete the selected VarAC clusters.")
            self._refresh_multi_radio_tables()
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _open_varac_membership_dialog(self, existing: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        clusters = [row for row in self.varac_clusters if isinstance(row, dict)]
        if not clusters:
            QMessageBox.information(self, "VarAC Memberships", "Create a VarAC cluster before assigning device memberships.")
            return None
        devices = [
            row
            for row in self.device_profiles
            if isinstance(row, dict) and str(row.get("device_class", "") or "").strip().lower() != "observer"
        ]
        if not devices:
            QMessageBox.information(self, "VarAC Memberships", "Create a non-observer device profile before assigning VarAC memberships.")
            return None

        dlg = QDialog(self)
        dlg.setWindowTitle("Edit VarAC Membership" if existing else "Add VarAC Membership")
        dlg.resize(560, 0)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        layout.addLayout(form)

        cluster_combo = QComboBox()
        for row in clusters:
            cluster_combo.addItem(
                f"{str(row.get('name', '') or 'Cluster').strip()} [{str(row.get('cluster_id', '') or '').strip()}]",
                int(row.get("id", 0) or 0),
            )
        cluster_index = cluster_combo.findData(int((existing or {}).get("cluster_db_id", 0) or 0))
        cluster_combo.setCurrentIndex(cluster_index if cluster_index >= 0 else 0)
        cluster_combo.setEnabled(existing is None)
        form.addRow("Cluster:", cluster_combo)

        device_combo = QComboBox()
        for row in devices:
            label = str(row.get("name", "") or "Device").strip()
            device_combo.addItem(label, int(row.get("id", 0) or 0))
        device_index = device_combo.findData(int((existing or {}).get("device_profile_id", 0) or 0))
        device_combo.setCurrentIndex(device_index if device_index >= 0 else 0)
        device_combo.setEnabled(existing is None)
        form.addRow("Device:", device_combo)

        instance_edit = QLineEdit(str(int((existing or {}).get("instance_number", 1) or 1)))
        instance_edit.setValidator(QIntValidator(1, 9999, instance_edit))
        form.addRow("Instance Number:", instance_edit)

        enabled_chk = QCheckBox("Membership Enabled")
        enabled_chk.setChecked(True if existing is None else bool((existing or {}).get("enabled", 1)))
        form.addRow("", enabled_chk)

        info_label = QLabel()
        info_label.setWordWrap(True)
        form.addRow("", info_label)

        def _update_hint() -> None:
            selected_cluster = self._varac_cluster_by_id(int(cluster_combo.currentData() or 0)) or {}
            device_id = int(device_combo.currentData() or 0)
            device_row = next(
                (
                    row
                    for row in self.device_profiles
                    if isinstance(row, dict) and int(row.get("id", 0) or 0) == device_id
                ),
                {},
            )
            node_ready = any(
                str(device_row.get(key, "") or "").strip()
                for key in ("varac_install_path", "varac_db_path", "varac_ini_path", "launch_cmd")
            )
            base = f"Instance numbers must be unique inside {str(selected_cluster.get('name', '') or 'the selected cluster').strip()}."
            if not node_ready:
                info_label.setText(base + " This device does not yet have device-local VarAC settings configured.")
                return
            info_label.setText(base + " The cluster gateway handler is chosen from the cluster editor.")

        cluster_combo.currentIndexChanged.connect(_update_hint)
        device_combo.currentIndexChanged.connect(_update_hint)
        _update_hint()

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        layout.addWidget(buttons)

        out: Dict[str, Any] = {}

        def _save() -> None:
            try:
                instance_value = int(instance_edit.text().strip() or "0")
            except Exception:
                QMessageBox.warning(self, "Validation", "Instance number must be a valid integer.")
                return
            if instance_value <= 0:
                QMessageBox.warning(self, "Validation", "Instance number must be greater than zero.")
                return
            out.update(
                {
                    "cluster_id": int(cluster_combo.currentData() or 0),
                    "device_profile_id": int(device_combo.currentData() or 0),
                    "instance_number": instance_value,
                    "enabled": bool(enabled_chk.isChecked()),
                }
            )
            dlg.accept()

        buttons.accepted.connect(_save)
        buttons.rejected.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out if out else None

    def _add_or_edit_varac_membership(self) -> None:
        selected = self._selected_varac_memberships()
        if len(selected) > 1:
            QMessageBox.warning(self, "VarAC Memberships", "Please select at most one existing membership to edit.")
            return
        values = self._open_varac_membership_dialog(existing=selected[0] if selected else None)
        if not values:
            return
        try:
            self.multi_radio_store.set_varac_cluster_member(
                int(values.get("cluster_id", 0) or 0),
                int(values.get("device_profile_id", 0) or 0),
                instance_number=int(values.get("instance_number", 0) or 0),
                enabled=bool(values.get("enabled", True)),
            )
        except ValueError as exc:
            QMessageBox.warning(self, "VarAC Memberships", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed saving VarAC membership.")
            QMessageBox.warning(self, "VarAC Memberships", "Unable to save the VarAC membership.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    def _remove_varac_memberships(self) -> None:
        selected = self._selected_varac_memberships()
        if not selected:
            QMessageBox.information(self, "VarAC Memberships", "Select one or more VarAC memberships to remove.")
            return
        confirm = QMessageBox.question(
            self,
            "Remove VarAC Memberships",
            f"Remove {len(selected)} selected VarAC membership(s)?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            for row in selected:
                self.multi_radio_store.remove_varac_cluster_member(
                    int(row.get("cluster_db_id", 0) or 0),
                    int(row.get("device_profile_id", 0) or 0),
                )
        except ValueError as exc:
            QMessageBox.warning(self, "VarAC Memberships", str(exc))
            self._refresh_multi_radio_tables()
            return
        except Exception:
            log.exception("Failed removing VarAC memberships.")
            QMessageBox.warning(self, "VarAC Memberships", "Unable to remove the selected VarAC memberships.")
            return
        self._refresh_multi_radio_tables()
        self._emit_device_profiles_changed()
        self._set_save_button_state("info" if self._settings_dirty else "success")

    # ---------- Launch Control ---------- #

    def _on_launch_paths_changed(self, *_args) -> None:
        if self._loading_settings:
            return
        self._refresh_launch_control_table()

    @staticmethod
    def _looks_like_self_launch_target(raw: str) -> bool:
        txt = str(raw or "").strip().lower()
        if not txt:
            return False
        if "freqinout.main" in txt:
            return True
        if "freqinout.exe" in txt:
            return True
        if txt.endswith("freqinout/main.py") or txt.endswith("freqinout\\main.py"):
            return True
        return False

    def _custom_tool_command(self, name: str) -> str:
        target = str(name or "").strip().lower()
        if not target:
            return ""
        for item in self._custom_tool_items_cache:
            item_name = str(item.get("name", "")).strip().lower()
            if item_name == target:
                return str(item.get("command", "")).strip()
        return ""

    def _refresh_custom_tools_table(self) -> None:
        if not hasattr(self, "custom_tools_table"):
            return
        current_row = self.custom_tools_table.currentRow()
        self._custom_tools_table_loading = True
        self.custom_tools_table.blockSignals(True)
        self.custom_tools_table.setRowCount(len(self._custom_tool_items_cache))
        for row, item in enumerate(self._custom_tool_items_cache):
            name_item = QTableWidgetItem(str(item.get("name", "")).strip())
            name_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.custom_tools_table.setItem(row, 0, name_item)

            cmd_item = QTableWidgetItem(str(item.get("command", "")).strip())
            cmd_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.custom_tools_table.setItem(row, 1, cmd_item)
        self.custom_tools_table.blockSignals(False)
        self._custom_tools_table_loading = False
        if self.custom_tools_table.rowCount() > 0:
            self.custom_tools_table.selectRow(min(max(current_row, 0), self.custom_tools_table.rowCount() - 1))
        if hasattr(self, "custom_tools_summary_label"):
            self.custom_tools_summary_label.setText(self._summary_custom_tools())
        self._update_custom_tool_buttons()
        self._refresh_section_titles()

    def _update_custom_tool_buttons(self) -> None:
        row = self.custom_tools_table.currentRow() if hasattr(self, "custom_tools_table") else -1
        has_rows = bool(hasattr(self, "custom_tools_table") and self.custom_tools_table.rowCount() > 0)
        can_select = has_rows and row >= 0
        if hasattr(self, "custom_tool_edit_btn"):
            self.custom_tool_edit_btn.setEnabled(can_select)
        if hasattr(self, "custom_tool_remove_btn"):
            self.custom_tool_remove_btn.setEnabled(can_select)
        if hasattr(self, "custom_tool_up_btn"):
            self.custom_tool_up_btn.setEnabled(bool(can_select and row > 0))
        if hasattr(self, "custom_tool_down_btn"):
            self.custom_tool_down_btn.setEnabled(bool(can_select and row < self.custom_tools_table.rowCount() - 1))

    def _validate_custom_tool(self, name: str, command: str, *, previous_name: str = "") -> str:
        name_txt = str(name or "").strip()
        command_txt = str(command or "").strip()
        if not name_txt:
            return "Tool Name is required."
        if not command_txt:
            return "Launch Command is required."
        if name_txt.lower() in {name.lower() for name in LAUNCH_APP_ORDER} and name_txt.lower() != previous_name.lower():
            return f"{name_txt} is already a built-in Launch Control app name."
        target = name_txt.lower()
        previous = str(previous_name or "").strip().lower()
        for item in self._custom_tool_items_cache:
            existing_name = str(item.get("name", "")).strip()
            if existing_name.lower() == target and existing_name.lower() != previous:
                return f"A custom tool named {name_txt} already exists."
        return ""

    def _add_custom_tool(self) -> None:
        dialog = _CustomToolDialog(self)
        if dialog.exec() != QDialog.Accepted:
            return
        name, command = dialog.values()
        issue = self._validate_custom_tool(name, command)
        if issue:
            QMessageBox.warning(self, "Custom Tools", issue)
            return
        self._custom_tool_items_cache.append({"name": name, "command": command})
        self._launch_items_cache = self.launch_orchestrator.build_default_items(
            self._launch_items_cache,
            custom_tools=self._custom_tool_items_cache,
        )
        self._refresh_custom_tools_table()
        self._refresh_launch_control_table()
        if self.custom_tools_table.rowCount() > 0:
            self.custom_tools_table.selectRow(self.custom_tools_table.rowCount() - 1)
        self._mark_settings_dirty()

    def _edit_custom_tool(self) -> None:
        if not hasattr(self, "custom_tools_table"):
            return
        row = self.custom_tools_table.currentRow()
        if row < 0 or row >= len(self._custom_tool_items_cache):
            return
        existing = self._custom_tool_items_cache[row]
        dialog = _CustomToolDialog(
            self,
            name=str(existing.get("name", "")).strip(),
            command=str(existing.get("command", "")).strip(),
        )
        if dialog.exec() != QDialog.Accepted:
            return
        name, command = dialog.values()
        previous_name = str(existing.get("name", "")).strip()
        issue = self._validate_custom_tool(name, command, previous_name=previous_name)
        if issue:
            QMessageBox.warning(self, "Custom Tools", issue)
            return
        self._custom_tool_items_cache[row] = {"name": name, "command": command}
        self._launch_items_cache = self.launch_orchestrator.build_default_items(
            self._launch_items_cache,
            custom_tools=self._custom_tool_items_cache,
        )
        self._refresh_custom_tools_table()
        self._refresh_launch_control_table()
        if row < self.custom_tools_table.rowCount():
            self.custom_tools_table.selectRow(row)
        self._mark_settings_dirty()

    def _remove_custom_tool(self) -> None:
        if not hasattr(self, "custom_tools_table"):
            return
        row = self.custom_tools_table.currentRow()
        if row < 0 or row >= len(self._custom_tool_items_cache):
            return
        del self._custom_tool_items_cache[row]
        self._launch_items_cache = self.launch_orchestrator.build_default_items(
            self._launch_items_cache,
            custom_tools=self._custom_tool_items_cache,
        )
        self._refresh_custom_tools_table()
        self._refresh_launch_control_table()
        self._mark_settings_dirty()

    def _move_custom_tool(self, direction: int) -> None:
        if direction == 0 or not hasattr(self, "custom_tools_table"):
            return
        row = self.custom_tools_table.currentRow()
        target_row = row + direction
        if row < 0 or target_row < 0 or target_row >= len(self._custom_tool_items_cache):
            return
        self._custom_tool_items_cache[row], self._custom_tool_items_cache[target_row] = (
            self._custom_tool_items_cache[target_row],
            self._custom_tool_items_cache[row],
        )
        self._launch_items_cache = self.launch_orchestrator.build_default_items(
            self._launch_items_cache,
            custom_tools=self._custom_tool_items_cache,
        )
        self._refresh_custom_tools_table()
        self._refresh_launch_control_table()
        if target_row < self.custom_tools_table.rowCount():
            self.custom_tools_table.selectRow(target_row)
        self._mark_settings_dirty()

    def _launch_bundle_source_profile(self) -> Optional[Dict[str, Any]]:
        primary = next(
            (
                dict(row)
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_primary", 0) or 0) == 1
            ),
            None,
        )
        if primary is not None:
            return primary
        active = next(
            (
                dict(row)
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_active", 0) or 0) == 1
            ),
            None,
        )
        return active

    @staticmethod
    def _profile_display_name(profile: Dict[str, Any]) -> str:
        if not isinstance(profile, dict):
            return "Radio"
        name = str(profile.get("name", "") or "").strip()
        if name:
            return name
        ident = int(profile.get("id", 0) or 0)
        return f"Radio {ident}" if ident > 0 else "Radio"

    def _launch_item_allowed_for_profile(self, name: str, profile: Optional[Dict[str, Any]]) -> bool:
        app_name = str(name or "").strip()
        if not app_name:
            return False
        if self._custom_tool_command(app_name):
            return True
        if not isinstance(profile, dict):
            return True
        mapping = {
            "FLRig": "flrig",
            "FLDigi": "fldigi",
            "FLMsg": "flmsg",
            "FLAmp": "flamp",
            "VarAC": "varac",
            "JS8Call": "js8call",
            "JS8Spotter": "js8spotter",
            "CommStat": "commstat",
        }
        software_key = mapping.get(app_name)
        if not software_key:
            return True
        return bool(self._radio_software_enabled(profile, software_key))

    def _is_launch_item_configured(self, name: str) -> bool:
        profile = self._launch_bundle_source_profile()
        if not self._launch_item_allowed_for_profile(name, profile):
            return False
        if self._custom_tool_command(name):
            return True
        if isinstance(profile, dict):
            if name == "VarAC":
                return bool(
                    str(profile.get("varac_install_path", "") or "").strip()
                    or str(profile.get("launch_cmd", "") or "").strip()
                )
            if name == "JS8Call":
                return bool(str(profile.get("js8_install_path", "") or "").strip())
            if name == "JS8Spotter":
                return bool(str(profile.get("spotter_launch_path", "") or "").strip())
            if name == "CommStat":
                return bool(str(profile.get("commstat_launch_path", "") or "").strip())
            if name == "FLRig":
                return bool(str(profile.get("flrig_path", "") or "").strip())
            if name == "FLDigi":
                return bool(str(profile.get("fldigi_path", "") or "").strip())
            if name == "FLMsg":
                return bool(str(profile.get("flmsg_path", "") or "").strip())
            if name == "FLAmp":
                return bool(str(profile.get("flamp_path", "") or "").strip())
            return False
        settings_data = self.settings.all()
        if name == "VarAC":
            path_val = str(settings_data.get("varac_path", "") or "").strip()
            launch_cmd = str(settings_data.get("varac_launch_cmd", "") or "").strip()
            return bool(path_val or launch_cmd)
        if name == "JS8Call":
            return bool(str(settings_data.get("path_js8call", "") or "").strip())
        if name == "JS8Spotter":
            return bool(str(settings_data.get("path_js8spotter", "") or "").strip())
        if name == "CommStat":
            return bool(str(settings_data.get("path_commstat", "") or "").strip())
        meta = self.PROGRAMS.get(name)
        path_key = str((meta or {}).get("setting_key", "") or "").strip()
        return bool(str(settings_data.get(path_key, "") or "").strip()) if path_key else False

    def _sync_launch_cache_from_table(self) -> None:
        if not hasattr(self, "launch_control_table"):
            return
        for row, name in enumerate(self._launch_visible_names):
            enabled_item = self.launch_control_table.item(row, 1)
            startup_item = self.launch_control_table.item(row, 2)
            enabled = bool(enabled_item and enabled_item.checkState() == Qt.Checked)
            startup = bool(startup_item and startup_item.checkState() == Qt.Checked)
            for item in self._launch_items_cache:
                if str(item.get("name", "")).strip() == name:
                    item["enabled"] = enabled
                    item["startup"] = startup
                    break

    def _refresh_launch_control_table(self) -> None:
        _perf_t0 = time.perf_counter()
        if not hasattr(self, "launch_control_table"):
            emit_span(
                "settings.refresh_launch_control_table",
                (time.perf_counter() - _perf_t0) * 1000.0,
                settings=self.settings,
                min_ms=5.0,
            )
            return
        self._sync_launch_cache_from_table()
        existing_map: Dict[str, Dict[str, object]] = {}
        for item in self._launch_items_cache:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if name:
                existing_map[name] = item
        catalog = self.launch_orchestrator.launch_catalog_order(self._custom_tool_items_cache)
        if not existing_map:
            for name in catalog:
                existing_map[name] = {"name": name, "enabled": True, "startup": False}
        ordered: List[Dict[str, object]] = []
        seen: set[str] = set()
        for item in self._launch_items_cache:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if name not in catalog or name in seen:
                continue
            seen.add(name)
            ordered.append(
                {
                    "name": name,
                    "enabled": bool(item.get("enabled", True)),
                    "startup": bool(item.get("startup", False)),
                }
            )
        for name in catalog:
            if name in seen:
                continue
            item = existing_map.get(name, {"name": name, "enabled": True, "startup": False})
            ordered.append(
                {
                    "name": name,
                    "enabled": bool(item.get("enabled", True)),
                    "startup": bool(item.get("startup", False)),
                }
            )
        self._launch_items_cache = ordered

        visible_items = [item for item in self._launch_items_cache if self._is_launch_item_configured(str(item.get("name", "")))]
        self._launch_visible_names = [str(item.get("name", "")) for item in visible_items]

        self._launch_table_loading = True
        self.launch_control_table.blockSignals(True)
        self.launch_control_table.setRowCount(len(visible_items))
        for row, item in enumerate(visible_items):
            name = str(item.get("name", "")).strip()
            app_item = QTableWidgetItem(name)
            app_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.launch_control_table.setItem(row, 0, app_item)

            enabled_item = QTableWidgetItem()
            enabled_item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            enabled_item.setCheckState(Qt.Checked if bool(item.get("enabled", True)) else Qt.Unchecked)
            self.launch_control_table.setItem(row, 1, enabled_item)

            startup_item = QTableWidgetItem()
            startup_item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            startup_item.setCheckState(Qt.Checked if bool(item.get("startup", False)) else Qt.Unchecked)
            self.launch_control_table.setItem(row, 2, startup_item)
        self.launch_control_table.blockSignals(False)
        self._launch_table_loading = False
        if self.launch_control_table.rowCount() > 0 and self.launch_control_table.currentRow() < 0:
            self.launch_control_table.selectRow(0)
        self._refresh_launch_control_guidance()
        self._update_launch_control_buttons()
        self._refresh_section_titles()
        emit_span(
            "settings.refresh_launch_control_table",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=5.0,
        )

    def _update_launch_control_buttons(self) -> None:
        row = self.launch_control_table.currentRow() if hasattr(self, "launch_control_table") else -1
        has_rows = bool(hasattr(self, "launch_control_table") and self.launch_control_table.rowCount() > 0)
        can_move = has_rows and row >= 0
        launch_allowed = True
        if hasattr(self.launch_orchestrator, "launch_allowed"):
            try:
                launch_allowed = bool(self.launch_orchestrator.launch_allowed())
            except Exception:
                launch_allowed = True
        self.launch_order_up_btn.setEnabled(bool(can_move and row > 0))
        self.launch_order_down_btn.setEnabled(bool(can_move and row < self.launch_control_table.rowCount() - 1))
        self.launch_reset_order_btn.setEnabled(has_rows)
        self.launch_configured_now_btn.setEnabled(has_rows and launch_allowed and not self.launch_orchestrator.is_active())
        self.launch_stop_btn.setEnabled(self.launch_orchestrator.is_active())

    def _refresh_launch_control_guidance(self) -> None:
        if not hasattr(self, "launch_guidance_card"):
            return
        profile = self._launch_bundle_source_profile()
        if not isinstance(profile, dict):
            self._set_guidance_card_state(
                self.launch_guidance_card,
                self.launch_guidance_title_label,
                self.launch_guidance_status_label,
                title="Projected Launch Bundle",
                text="Launch Control follows the current Station Default compatibility shell. Select a default radio in Radio Profiles so launch behavior clearly follows one radio bundle.",
                level="warning",
            )
            if hasattr(self, "launch_hint_label"):
                self.launch_hint_label.setText(
                    "Only configured apps are shown. Custom tools remain global. Without a default radio, Launch Control falls back to whatever is currently projected into the compatibility shell."
                )
            return
        radio_name = self._profile_display_name(profile)
        backend_label = self._device_backend_label(str(profile.get("control_backend", "") or "manual"))
        bundle = self._device_software_summary(profile)
        endpoint = self._device_endpoint_summary(profile)
        launch_enabled = self._radio_profile_launch_opt_in_enabled(profile)
        level = "success" if self._radio_profile_effective_launch_control_enabled(profile) else "warning"
        text = (
            f"Station Default radio: {radio_name}. Primary rig control: {backend_label}. "
            f"Software bundle: {bundle}. Endpoint summary: {endpoint}. "
            f"Radio launch opt-in: {'enabled' if launch_enabled else 'off for this radio'}."
        )
        self._set_guidance_card_state(
            self.launch_guidance_card,
            self.launch_guidance_title_label,
            self.launch_guidance_status_label,
            title=f"{radio_name} Launch Bundle",
            text=text,
            level=level,
        )
        if hasattr(self, "launch_hint_label"):
            active_names = [
                self._profile_display_name(row)
                for row in self.device_profiles
                if isinstance(row, dict) and int(row.get("runtime_active", 0) or 0) == 1
            ]
            active_text = f" Active radios: {', '.join(active_names)}." if active_names else ""
            self.launch_hint_label.setText(
                "Only apps configured for the current Station Default radio bundle are shown here, along with any global custom tools. "
                "Launch order controls startup sequencing for that projected radio bundle."
                + active_text
            )

    def _move_launch_row(self, direction: int) -> None:
        if direction == 0:
            return
        row = self.launch_control_table.currentRow()
        if row < 0:
            return
        target_row = row + direction
        if target_row < 0 or target_row >= self.launch_control_table.rowCount():
            return
        self._sync_launch_cache_from_table()
        name_a = self._launch_visible_names[row]
        name_b = self._launch_visible_names[target_row]
        idx_a = next((i for i, item in enumerate(self._launch_items_cache) if str(item.get("name", "")) == name_a), -1)
        idx_b = next((i for i, item in enumerate(self._launch_items_cache) if str(item.get("name", "")) == name_b), -1)
        if idx_a < 0 or idx_b < 0:
            return
        self._launch_items_cache[idx_a], self._launch_items_cache[idx_b] = (
            self._launch_items_cache[idx_b],
            self._launch_items_cache[idx_a],
        )
        self._refresh_launch_control_table()
        if 0 <= target_row < self.launch_control_table.rowCount():
            self.launch_control_table.selectRow(target_row)
        self._mark_settings_dirty()

    def _reset_launch_order(self) -> None:
        self._sync_launch_cache_from_table()
        existing_map = {
            str(item.get("name", "")).strip(): item
            for item in self._launch_items_cache
            if isinstance(item, dict)
        }
        reset_items: List[Dict[str, object]] = []
        for name in self.launch_orchestrator.launch_catalog_order(self._custom_tool_items_cache):
            prev = existing_map.get(name, {})
            reset_items.append(
                {
                    "name": name,
                    "enabled": bool(prev.get("enabled", True)),
                    "startup": bool(prev.get("startup", False)),
                }
            )
        self._launch_items_cache = reset_items
        self._refresh_launch_control_table()
        self._mark_settings_dirty()

    def _on_launch_table_item_changed(self, _item: QTableWidgetItem) -> None:
        if self._loading_settings or self._launch_table_loading:
            return
        self._sync_launch_cache_from_table()
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _launch_configured_now(self) -> None:
        self._sync_launch_cache_from_table()
        if hasattr(self.settings, "set"):
            self.settings.set("custom_tool_items", [dict(item) for item in self._custom_tool_items_cache])
        if hasattr(self.launch_orchestrator, "launch_allowed"):
            try:
                if not self.launch_orchestrator.launch_allowed():
                    reason = ""
                    if hasattr(self.launch_orchestrator, "launch_block_reason"):
                        reason = str(self.launch_orchestrator.launch_block_reason() or "").strip()
                    self._publish_launch_control_feedback(
                        status="blocked",
                        summary="Launch blocked: Launch Control is disabled.",
                        detail=reason or "Launch Control is disabled by the primary frequency plan.",
                    )
                    return
            except Exception:
                pass
        started = self.launch_orchestrator.start_manual_sequence(self._launch_items_cache)
        if not started:
            self._publish_launch_control_feedback(
                status="blocked",
                summary="Launch blocked: no enabled configured applications.",
                detail="Enable at least one configured application in Launch Control before launching.",
            )
            return
        self._publish_launch_control_feedback(
            status="in_progress",
            summary="Launch sequence started.",
            detail="FreqInOut is starting the selected configured applications.",
        )
        self._update_launch_control_buttons()

    def _stop_launch_sequence(self) -> None:
        self.launch_orchestrator.stop_sequence()
        self._update_launch_control_buttons()

    def _on_launch_sequence_started(self, payload: object) -> None:
        try:
            data = payload if isinstance(payload, dict) else {}
            trigger = str(data.get("trigger", "")).strip().capitalize() or "Launch"
            self.launch_summary_label.setText(f"Launch status: {trigger} sequence running...")
        except Exception:
            self.launch_summary_label.setText("Launch status: sequence running...")
        self._update_launch_control_buttons()

    def _on_launch_sequence_progress(self, payload: object) -> None:
        try:
            data = payload if isinstance(payload, dict) else {}
            name = str(data.get("name", "")).strip()
            status = str(data.get("status", "")).strip()
            detail = str(data.get("detail", "")).strip()
            self.launch_summary_label.setText(f"Launch status: {name} {status} ({detail})")
        except Exception:
            pass
        self._update_launch_control_buttons()

    def _on_launch_sequence_finished(self, payload: object) -> None:
        data = payload if isinstance(payload, dict) else {}
        launched = int(data.get("launched", 0) or 0)
        already_running = int(data.get("already_running", 0) or 0)
        failed = int(data.get("failed", 0) or 0)
        timeout = int(data.get("timeout", 0) or 0)
        blocked_self = int(data.get("blocked_self", 0) or 0)
        cancelled = bool(data.get("cancelled", False))
        trigger = str(data.get("trigger", "")).strip().lower()
        status_txt = (
            "Launch status: done "
            f"(launched={launched}, running={already_running}, failed={failed}, timeout={timeout}, blocked={blocked_self})"
        )
        if cancelled:
            status_txt = (
                "Launch status: cancelled "
                f"(launched={launched}, running={already_running}, failed={failed}, timeout={timeout}, blocked={blocked_self})"
            )
        self.launch_summary_label.setText(status_txt)
        if trigger == "manual":
            self._publish_launch_control_feedback(
                status=self._launch_sequence_feedback_status(
                    launched=launched,
                    already_running=already_running,
                    failed=failed,
                    timeout=timeout,
                    blocked_self=blocked_self,
                    cancelled=cancelled,
                ),
                summary=self._launch_sequence_feedback_summary(
                    launched=launched,
                    already_running=already_running,
                    failed=failed,
                    timeout=timeout,
                    blocked_self=blocked_self,
                    cancelled=cancelled,
                ),
                detail=self._launch_sequence_feedback_detail(
                    launched=launched,
                    already_running=already_running,
                    failed=failed,
                    timeout=timeout,
                    blocked_self=blocked_self,
                    cancelled=cancelled,
                ),
            )
        self._update_launch_control_buttons()

    # ---------- TIME / TIMEZONE ---------- #

    def _detect_system_timezone(self) -> str:
        """
        Detect the current system timezone using OS-specific identifiers when
        available, then normalize that result into one of TIMEZONE_CHOICES.
        """
        return detect_system_timezone_name("UTC")

    def _ui_tz_abbr(self, tz_name: str, fallback: str) -> str:
        mapping = {
            "UTC": "UTC",
            "America/New_York": "ET",
            "America/Chicago": "CT",
            "America/Denver": "MT",
            "America/Los_Angeles": "PT",
        }
        return mapping.get(tz_name, fallback)

    def _update_clock_labels(self):
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a")
        self.utc_label.setText(now_utc.strftime(f"<b>UTC ({utc_day}):</b> %y%m%d %H:%M:%S Z"))

        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        now_local = now_utc.astimezone(tz)
        # Prefer our short UI label, fall back to tzname or tz_name
        fallback = now_local.tzname() or tz_name
        ui_abbr = self._ui_tz_abbr(tz_name, fallback)

        local_day = now_local.strftime("%a")
        self.local_label.setText(
            now_local.strftime(f"<b>Local ({local_day}):</b> %y%m%d %H:%M:%S {ui_abbr}")
        )

    def _persist_operator_grid_to_db(self, callsign: str, grid6: str, name: str, state: str) -> None:
        """
        Optionally upsert the operator's own grid into operator_checkins to ensure
        stations map has a primary location for link rendering.
        """
        cs = (callsign or "").strip().upper()
        grid = (grid6 or "").strip().upper()
        if not cs or len(grid) < 4:
            return
        conn = None
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path, timeout=5.0)
            conn.execute("PRAGMA busy_timeout=5000")
            ensure_operator_checkins_schema(conn)
            cur = conn.cursor()
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            cur.execute(
                """
                INSERT INTO operator_checkins
                    (callsign, name, state, grid, group1, group2, group3, group_role,
                     first_seen_utc, last_seen_utc, last_net, last_role, checkin_count, groups_json, trusted)
                VALUES (?, ?, ?, ?, '', '', '', '', ?, ?, '', '', 0, NULL, 0)
                ON CONFLICT(callsign) DO UPDATE SET
                    name=excluded.name,
                    state=excluded.state,
                    grid=excluded.grid,
                    last_seen_utc=excluded.last_seen_utc,
                    trusted=COALESCE(operator_checkins.trusted, excluded.trusted)
                """,
                (cs, name.strip(), state.strip().upper(), grid, now_iso, now_iso),
            )
            conn.commit()
        except Exception as e:
            log.debug("SettingsTab: failed to persist operator grid to DB: %s", e)
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def _refresh_operator_history_views(self) -> None:
        """
        Ask the main window to reload operator history consumers (map, history, net controls).
        """
        try:
            # Prefer top-level window; parent() may be a layout wrapper
            win = self.window()
            if win and hasattr(win, "refresh_operator_history_views"):
                win.refresh_operator_history_views()
        except Exception:
            pass

    # ---------- RADIO PROGRAMS ---------- #

    def _choose_program_path(self, program_name: str):
        fn, _ = QFileDialog.getOpenFileName(self, f"Select {program_name} Executable")
        if fn:
            self.path_edits[program_name].setText(fn)

    def _get_saved_program_path(self, program_name: str) -> Optional[Path]:
        if program_name == "JS8Call":
            return None
        meta = self.PROGRAMS.get(program_name)
        if not meta:
            return None
        path_str = self.settings.get(meta["setting_key"])
        if path_str:
            return Path(path_str)
        ui_val = self.path_edits.get(program_name)
        if ui_val:
            txt = ui_val.text().strip()
            if txt:
                return Path(txt)
        return None

    def _launch_program(self, program_name: str) -> bool:
        if program_name == "JS8Call":
            log.info("Launch request ignored for JS8Call (external management only).")
            return False
        exe_path = self._get_saved_program_path(program_name)

        creationflags = 0
        if platform.system() == "Windows":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

        if exe_path and exe_path.exists():
            try:
                subprocess.Popen([str(exe_path)], shell=False if platform.system() == "Windows" else False, creationflags=creationflags)
                log.info("Launched %s from saved path %s", program_name, exe_path)
                return True
            except Exception as e:
                log.error("Failed launching %s from saved path %s: %s", program_name, exe_path, e)

        for cand in [program_name.lower(), program_name]:
            try:
                subprocess.Popen([cand], creationflags=creationflags)
                log.info("Launched %s from system PATH as '%s'", program_name, cand)
                return True
            except Exception:
                continue

        QMessageBox.warning(
            self,
            "Launch Failed",
            f"Unable to launch {program_name}. Please set the executable path.",
        )
        return False

    def _launch_selected_programs(self):
        launched_any = False
        for name, chk in self.radio_checkboxes.items():
            if chk.isChecked():
                if self._program_is_running(name):
                    log.info("Launch Selected: %s already running; skipping.", name)
                    continue
                if self._launch_program(name):
                    launched_any = True

        if not launched_any:
            QMessageBox.information(self, "Launch", "No programs were selected.")
        else:
            QTimer.singleShot(1500, self._refresh_running_status)

    def _update_launch_selected_state(self):
        if hasattr(self, "launch_selected_btn"):
            theme = resolve_theme(self.settings)
            any_selected = any(chk.isChecked() for chk in self.radio_checkboxes.values())
            role = "info" if any_selected else "muted"
            self.launch_selected_btn.setStyleSheet(button_style(role, theme))
        self._update_launch_control_buttons()

    def _program_is_running(self, program_name: str) -> bool:
        try:
            return bool(self._software_status_probe.program_is_running(program_name))
        except Exception:
            return False

    def _find_process_exe(self, program_name: str) -> Optional[str]:
        try:
            return self._software_status_probe.find_process_exe(program_name)
        except Exception:
            return None

    def _on_dependency_status_snapshot_changed(self, _snapshot: object) -> None:
        if not self._active:
            return
        self._refresh_running_status_compat(force=True)

    def _refresh_running_status_compat(self, force: bool = False) -> None:
        try:
            self._refresh_running_status(force=force)
        except TypeError:
            self._refresh_running_status()

    def _refresh_running_status(self, force: bool = False):
        _perf_t0 = time.perf_counter()
        theme = resolve_theme(self.settings)
        visible_keys = [key for key, _label in self._current_visible_status_items()]
        if visible_keys != list(self.status_labels.keys()):
            self._rebuild_status_indicators()
        status_sig: Tuple[object, ...] = (tuple(visible_keys), self._selected_radio_status_endpoint_sig())
        now_ts = time.time()
        if (
            not force
            and self._last_running_status_sig == status_sig
            and (now_ts - float(self._last_running_status_refresh_ts or 0.0))
            < float(self._running_status_refresh_interval_sec)
        ):
            return
        snapshot = self._selected_radio_status_snapshot(force=force)
        if not snapshot:
            snapshot = self._status_service.software_status_snapshot()
        self._last_running_status_sig = status_sig
        self._last_running_status_refresh_ts = now_ts
        for program_name, lbl in self.status_labels.items():
            info = snapshot.get(program_name, {})
            state = str(info.get("state", "idle"))
            tooltip = str(info.get("tooltip", "Not running"))
            lbl.setStyleSheet(led_style(state, theme))
            lbl.setToolTip(tooltip)

        # Keep VarAC path tooltip in sync with runtime status.
        if hasattr(self, "varac_path_edit"):
            varac_info = snapshot.get("VarAC", {})
            self.varac_path_edit.setToolTip(str(varac_info.get("tooltip", "Not running")))
        emit_span(
            "settings.refresh_running_status",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=5.0,
        )

    @staticmethod
    def _int_override_from_text(text: str) -> Optional[int]:
        try:
            value = int(str(text or "").strip())
        except Exception:
            return None
        return value if value > 0 else None

    def _selected_radio_status_endpoint_sig(self) -> Tuple[str, Optional[int], Optional[int], str, Optional[int]]:
        try:
            js8_host = self.js8_host_edit.text().strip() if hasattr(self, "js8_host_edit") else ""
            js8_port = self._int_override_from_text(self.js8_port_edit.text()) if hasattr(self, "js8_port_edit") else None
            flrig_port = (
                self._int_override_from_text(self.flrig_port_edit.text()) if hasattr(self, "flrig_port_edit") else None
            )
            fldigi_host = self.fldigi_host_edit.text().strip() if hasattr(self, "fldigi_host_edit") else ""
            fldigi_port = (
                self._int_override_from_text(self.fldigi_port_edit.text()) if hasattr(self, "fldigi_port_edit") else None
            )
            return (js8_host, js8_port, flrig_port, fldigi_host, fldigi_port)
        except Exception:
            return ("", None, None, "", None)

    def _selected_radio_status_snapshot(self, force: bool = False) -> Dict[str, Dict[str, object]]:
        try:
            js8_host, js8_port, flrig_port, fldigi_host, fldigi_port = self._selected_radio_status_endpoint_sig()
            return self._software_status_probe.status_snapshot(
                force=force,
                host_override=js8_host or None,
                port_override=js8_port,
                flrig_port_override=flrig_port,
                fldigi_host_override=fldigi_host or None,
                fldigi_port_override=fldigi_port,
            )
        except Exception:
            return {}

    def apply_theme(self):
        try:
            theme = resolve_theme(self.settings)
            if self.loading_label:
                bg = theme.get("surface_alt", theme.get("surface", "#f2f2f2"))
                fg = theme.get("accent", theme.get("text", "#222"))
                border = theme.get("border", "#ccc")
                self.loading_label.setStyleSheet(
                    f"padding: 2px 6px; border-radius: 4px; background: {bg}; color: {fg}; border: 1px solid {border};"
                )
                self.loading_label.setVisible(False)
            self._refresh_running_status_compat(force=True)
            self._update_launch_selected_state()
            self._update_device_profile_action_buttons()
            self._update_op_group_action_buttons()
            self._update_local_net_action_buttons()
            self._set_save_button_state("info" if self._settings_dirty else "success")
            if hasattr(self, "open_logs_btn"):
                self.open_logs_btn.setStyleSheet(button_style("primary", theme))
            if hasattr(self, "open_log_folder_btn"):
                self.open_log_folder_btn.setStyleSheet(button_style("secondary", theme))
            if hasattr(self, "export_diag_btn"):
                self.export_diag_btn.setStyleSheet(button_style("secondary", theme))
            if hasattr(self, "enable_timed_debug_btn"):
                self.enable_timed_debug_btn.setStyleSheet(button_style("warning", theme))
            if hasattr(self, "logging_warning_label"):
                self.logging_warning_label.setStyleSheet(f"color: {theme.get('text_muted', theme.get('text', '#666'))};")
            if hasattr(self, "logging_group"):
                self.logging_group.setStyleSheet(
                    "QWidget#settingsLoggingPanel {"
                    f" background: {theme.get('surface', '#ffffff')};"
                    f" border: 1px solid {theme.get('border', '#cccccc')};"
                    " border-radius: 6px;"
                    "}"
                )
            if hasattr(self, "sections_nav_list"):
                self._apply_sections_nav_style()
                self._refresh_section_nav_health()
            for btn in getattr(self, "_context_help_buttons", []):
                try:
                    btn.setStyleSheet(button_style("secondary", theme))
                except Exception:
                    continue
            self._refresh_contextual_autofill_buttons()
            self._update_enforcement_visibility()
            self._update_logging_actions_layout()
            self._apply_accessibility_width_guards()
        except Exception:
            pass

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_logging_actions_layout()

    def _js8_api_reachable(self) -> bool:
        try:
            host_txt = self.js8_host_edit.text().strip() if hasattr(self, "js8_host_edit") else ""
            host_override = host_txt or "127.0.0.1"
        except Exception:
            host_override = "127.0.0.1"
        try:
            port_txt = self.js8_port_edit.text().strip() if hasattr(self, "js8_port_edit") else ""
            port_override = int(port_txt) if port_txt else None
        except Exception:
            port_override = None
        try:
            return bool(self._software_status_probe.js8_api_reachable(port_override=port_override, host_override=host_override))
        except Exception:
            return False

    def _program_autostart_enabled(self, program_name: str) -> bool:
        if program_name not in {"FLDigi", "FLMsg", "FLAmp", "JS8Call"}:
            return False
        meta = self.PROGRAMS.get(program_name)
        if not meta:
            return False
        key = meta.get("autostart_key")
        try:
            val = self.settings.get(key, False)
        except Exception:
            val = False
        return self._is_truthy(val)

    @staticmethod
    def _is_truthy(val) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        if isinstance(val, str):
            return val.strip().lower() in {"true", "1", "yes", "on"}
        return False

    def _auto_start_enabled_programs(self):
        for name in self.PROGRAMS.keys():
            if not self._program_autostart_enabled(name):
                continue
            if self._program_is_running(name):
                continue
            self._launch_program(name)

    # ---------- Operating Groups ---------- #

    def _add_operating_group(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("Add Operating Group")
        form = QFormLayout(dlg)

        name_edit = QLineEdit()
        form.addRow("Group Name:", name_edit)

        mode_combo = QComboBox()
        mode_combo.addItems(["Digi", "SSB"])
        mode_combo.setMinimumWidth(110)
        mode_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        form.addRow("Mode:", mode_combo)

        band_combo = QComboBox()
        band_combo.addItems([
            "20M", "40M", "80M", "2M", "6M", "10M", "12M", "15M", "17M", "30M", "60M",
        ])
        band_combo.setMinimumWidth(110)
        band_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        form.addRow("Band:", band_combo)

        freq_edit = QLineEdit()
        freq_edit.setPlaceholderText("e.g., 7.115")
        form.addRow("Frequency (MHz):", freq_edit)

        vfo_combo = QComboBox()
        vfo_combo.addItems(["A", "B"])
        vfo_combo.setMinimumWidth(110)
        vfo_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        form.addRow("VFO:", vfo_combo)

        fldigi_mode_combo = QComboBox()
        fldigi_mode_combo.setEditable(True)
        fldigi_mode_combo.addItems(FLDIGI_MODE_OPTIONS)
        fldigi_mode_combo.setInsertPolicy(QComboBox.NoInsert)
        completer = QCompleter(FLDIGI_MODE_OPTIONS, fldigi_mode_combo)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        fldigi_mode_combo.setCompleter(completer)
        form.addRow("FLDigi Starting Mode:", fldigi_mode_combo)

        _manual_voice_override = {"enabled": False}

        def _default_voice_mode() -> str:
            return voice_sideband_for_band(band_combo.currentText())

        def _sync_fldigi_voice_default(*, force: bool = False) -> None:
            if (mode_combo.currentText() or "").strip().upper() != "SSB":
                return
            current = (fldigi_mode_combo.currentText() or "").strip().upper()
            desired = _default_voice_mode()
            if not force and _manual_voice_override["enabled"]:
                return
            if force or not current or current in {"USB", "LSB"}:
                fldigi_mode_combo.blockSignals(True)
                fldigi_mode_combo.setCurrentText(desired)
                fldigi_mode_combo.blockSignals(False)
                _manual_voice_override["enabled"] = False

        def _on_mode_changed(_text: str) -> None:
            if (mode_combo.currentText() or "").strip().upper() != "SSB":
                _manual_voice_override["enabled"] = False
                return
            _sync_fldigi_voice_default()

        def _on_band_changed(*_args) -> None:
            _sync_fldigi_voice_default()

        def _on_fldigi_mode_changed(text: str) -> None:
            if (mode_combo.currentText() or "").strip().upper() != "SSB":
                _manual_voice_override["enabled"] = False
                return
            value = (text or "").strip().upper()
            desired = _default_voice_mode()
            _manual_voice_override["enabled"] = bool(value) and value != desired

        mode_combo.currentTextChanged.connect(_on_mode_changed)
        band_combo.currentTextChanged.connect(_on_band_changed)
        fldigi_mode_combo.currentTextChanged.connect(_on_fldigi_mode_changed)
        current_mode = (mode_combo.currentText() or "").strip().upper()
        current_fldigi = (fldigi_mode_combo.currentText() or "").strip().upper()
        if current_mode == "SSB" and current_fldigi:
            _manual_voice_override["enabled"] = current_fldigi != _default_voice_mode()
        else:
            _sync_fldigi_voice_default(force=True)

        fldigi_offset_edit = QLineEdit()
        fldigi_offset_edit.setValidator(QIntValidator(0, 99999, fldigi_offset_edit))
        fldigi_offset_edit.setPlaceholderText("e.g., 900")
        form.addRow("FLDigi Offset:", fldigi_offset_edit)

        auto_tune_chk = QCheckBox("Enable Auto-Tune on QSY")
        form.addRow("", auto_tune_chk)

        use_condition_levels_chk = QCheckBox("Use Condition Levels (Group)")
        form.addRow("", use_condition_levels_chk)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("OK")
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        def on_accept():
            name = name_edit.text().strip()
            band = band_combo.currentText().strip().upper()
            mode = normalize_operating_group_mode(mode_combo.currentText(), band)
            freq_txt = freq_edit.text().strip()
            if not name:
                QMessageBox.warning(self, "Validation", "Group Name is required.")
                return
            if not self._validate_band_frequency(band, mode, freq_txt):
                QMessageBox.warning(self, "Validation", f"Frequency {freq_txt} invalid for {band} {mode}.")
                return
            freq_val = float(freq_txt.replace(",", "."))
            offset_txt = fldigi_offset_edit.text().strip()
            if offset_txt:
                try:
                    int(offset_txt)
                except Exception:
                    QMessageBox.warning(self, "Validation", "FLDigi Offset must be an integer.")
                    return
            fldigi_mode = fldigi_mode_combo.currentText().strip()
            vfo = vfo_combo.currentText().strip().upper() or "A"
            self._upsert_operating_group(
                name,
                mode,
                band,
                f"{freq_val:.3f}",
                auto_tune=auto_tune_chk.isChecked(),
                vfo=vfo,
                fldigi_mode=fldigi_mode,
                fldigi_offset=offset_txt,
                use_condition_levels=use_condition_levels_chk.isChecked(),
            )
            dlg.accept()

        ok_btn.clicked.connect(on_accept)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

    def _validate_band_frequency(self, band: str, mode: str, freq_txt: str) -> bool:
        try:
            freq = float(freq_txt.replace(",", "."))
        except Exception:
            return False
        band = str(band or "").strip().upper()
        mode = normalize_operating_group_mode(mode, band)
        # Simple band/mode ranges (same as daily schedule)
        if mode in {"USB", "LSB"}:
            mode = "SSB"
        limits = {
            ("20M", "Digi"): (14.000, 14.150),
            ("20M", "SSB"): (14.150, 14.350),
            ("40M", "Digi"): (7.000, 7.125),
            ("40M", "SSB"): (7.125, 7.300),
            ("80M", "Digi"): (3.500, 3.600),
            ("80M", "SSB"): (3.600, 4.000),
            ("2M", "Digi"): (144.000, 148.000),
            ("2M", "SSB"): (144.100, 148.000),
            ("6M", "Digi"): (50.000, 54.000),
            ("6M", "SSB"): (50.100, 54.000),
            ("10M", "Digi"): (28.000, 28.300),
            ("10M", "SSB"): (28.300, 29.700),
            ("12M", "Digi"): (24.890, 24.930),
            ("12M", "SSB"): (24.930, 24.990),
            ("15M", "Digi"): (21.000, 21.200),
            ("15M", "SSB"): (21.200, 21.450),
            ("17M", "Digi"): (18.068, 18.110),
            ("17M", "SSB"): (18.110, 18.168),
            ("30M", "Digi"): (10.100, 10.150),
            ("30M", "SSB"): (10.100, 10.150),
            ("60M", "Digi"): (5.332, 5.405),
            ("60M", "SSB"): (5.332, 5.405),
        }
        key = (band, mode)
        if key not in limits:
            return False
        lo, hi = limits[key]
        return lo <= freq <= hi

    def _format_freq(self, val) -> str:
        try:
            return f"{float(val):.3f}"
        except Exception:
            return str(val) if val is not None else ""

    def _upsert_operating_group(
        self,
        name: str,
        mode: str,
        band: str,
        freq_mhz,
        auto_tune: bool = False,
        vfo: str = "A",
        fldigi_mode: str = "",
        fldigi_offset: str = "",
        use_condition_levels: bool = False,
        condition_level: int | None = None,
    ):
        # replace existing entry with same group+mode+band
        name = name.strip().upper()
        band = str(band or "").strip().upper()
        mode = normalize_operating_group_mode(mode, band)
        freq_display = self._format_freq(freq_mhz)
        cond_level: int | None
        if condition_level is None:
            cond_level = None
        else:
            try:
                cond_level = int(condition_level)
            except Exception:
                cond_level = 5
            if cond_level < 1 or cond_level > 5:
                cond_level = 5
        updated = False
        for g in self.operating_groups:
            if g.get("group") == name and g.get("mode") == mode and g.get("band") == band:
                g["frequency"] = freq_display
                g["auto_tune"] = bool(auto_tune)
                g["vfo"] = vfo
                g["fldigi_mode"] = fldigi_mode
                g["fldigi_offset"] = fldigi_offset
                g["use_condition_levels"] = bool(use_condition_levels)
                if cond_level is not None:
                    g["condition_level"] = cond_level
                elif "condition_level" not in g:
                    g["condition_level"] = 5
                updated = True
                break
        if not updated:
            self.operating_groups.append(
                {
                    "group": name,
                    "mode": mode,
                    "band": band,
                    "frequency": freq_display,
                    "vfo": vfo,
                    "fldigi_mode": fldigi_mode,
                    "fldigi_offset": fldigi_offset,
                    "auto_tune": bool(auto_tune),
                    "use_condition_levels": bool(use_condition_levels),
                    "condition_level": 5 if cond_level is None else cond_level,
                }
            )
        # Condition-level participation is group-scoped (not per band/mode row).
        for g in self.operating_groups:
            if str(g.get("group", "")).strip().upper() == name:
                g["use_condition_levels"] = bool(use_condition_levels)
        self._refresh_operating_groups_table()
        # Persist immediately so additions survive app restarts without requiring an explicit Save click.
        try:
            self._save_settings_quiet()
            self._settings_dirty = False
            self._set_save_button_state("success")
            try:
                self.settings_saved.emit()
            except Exception:
                pass
        except Exception:
            log.exception("Failed to persist Operating Group; will remain in-memory only.")

    def _refresh_operating_groups_table(self):
        _perf_t0 = time.perf_counter()
        # Sort display by Group asc, then Band asc
        self.operating_groups = sorted(
            [
                {
                    "group": str(g.get("group", "")).upper(),
                    "mode": normalize_operating_group_mode(g.get("mode", ""), g.get("band", "")),
                    "band": str(g.get("band", "")).strip().upper(),
                    "frequency": g.get("frequency", ""),
                    "vfo": (g.get("vfo") or "A").strip().upper() or "A",
                    "fldigi_mode": (g.get("fldigi_mode") or "").strip(),
                    "fldigi_offset": (g.get("fldigi_offset") or "").strip(),
                    "auto_tune": bool(g.get("auto_tune", False)),
                    "use_condition_levels": bool(g.get("use_condition_levels", False)),
                    "condition_level": g.get("condition_level", 5),
                }
                for g in self.operating_groups
            ],
            key=lambda g: (str(g.get("group", "")).lower(), str(g.get("band", "")).lower()),
        )

        table = self.op_groups_table
        table.setRowCount(0)
        self._op_group_rows_by_group = {}
        for g in self.operating_groups:
            row = table.rowCount()
            table.insertRow(row)
            group_key = str(g.get("group", "")).strip().upper()
            if group_key:
                self._op_group_rows_by_group.setdefault(group_key, []).append(row)
            sel_chk = QCheckBox()
            sel_chk.setFixedWidth(22)
            sel_chk.stateChanged.connect(self._update_op_group_action_buttons)
            sel_wrap = QWidget()
            sel_layout = QHBoxLayout(sel_wrap)
            sel_layout.setContentsMargins(0, 0, 0, 0)
            sel_layout.setAlignment(Qt.AlignCenter)
            sel_layout.addWidget(sel_chk)
            table.setCellWidget(row, 0, sel_wrap)
            table.setItem(row, 1, QTableWidgetItem(str(g.get("group", "")).upper()))
            table.setItem(row, 2, QTableWidgetItem(str(g.get("mode", ""))))
            table.setItem(row, 3, QTableWidgetItem(str(g.get("band", ""))))
            table.setItem(row, 4, QTableWidgetItem(self._format_freq(g.get("frequency", ""))))
            table.setItem(row, 5, QTableWidgetItem(str(g.get("vfo", "A")).upper()))
            table.setItem(row, 6, QTableWidgetItem(str(g.get("fldigi_mode", ""))))
            table.setItem(row, 7, QTableWidgetItem(str(g.get("fldigi_offset", ""))))
            auto_chk = QCheckBox()
            auto_chk.setChecked(bool(g.get("auto_tune", False)))
            auto_chk.setFixedSize(20, 20)
            auto_wrap = QWidget()
            auto_wrap.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            auto_layout = QHBoxLayout(auto_wrap)
            auto_layout.setContentsMargins(6, 0, 6, 0)
            auto_layout.setAlignment(Qt.AlignCenter)
            auto_layout.addWidget(auto_chk)
            table.setCellWidget(row, 8, auto_wrap)
            cond_chk = QCheckBox()
            cond_chk.setChecked(bool(g.get("use_condition_levels", False)))
            cond_chk.setFixedSize(20, 20)
            cond_wrap = QWidget()
            cond_wrap.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            cond_layout = QHBoxLayout(cond_wrap)
            cond_layout.setContentsMargins(6, 0, 6, 0)
            cond_layout.setAlignment(Qt.AlignCenter)
            cond_layout.addWidget(cond_chk)
            table.setCellWidget(row, 9, cond_wrap)
            cond_chk.stateChanged.connect(
                lambda state, r=row: self._on_operating_group_condition_toggled(r, state)
            )
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.Interactive)
        header.setSectionResizeMode(7, QHeaderView.Interactive)
        header.setSectionResizeMode(8, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(9, QHeaderView.ResizeToContents)
        table.setColumnWidth(6, max(table.columnWidth(6), 185))
        table.setColumnWidth(7, max(table.columnWidth(7), 130))
        # These group tables sit in compact Settings panels; six rows keeps the section scannable.
        self._fit_table_height_to_rows(table, min_rows=1, max_rows=6, extra_rows=1)
        self._refresh_fit_content_section_height(getattr(self, "op_groups_section_group", None))
        self._update_op_group_action_buttons()
        self._refresh_section_titles()
        emit_span(
            "settings.refresh_operating_groups_table",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=5.0,
        )

    def _update_op_group_action_buttons(self):
        theme = resolve_theme(self.settings)
        has_selection = bool(self._selected_op_rows())
        role = "info" if has_selection else "muted"
        self.edit_group_btn.setEnabled(True)
        self.delete_group_btn.setEnabled(True)
        self.edit_group_btn.setStyleSheet(button_style(role, theme))
        self.delete_group_btn.setStyleSheet(button_style(role, theme))

    def _on_operating_group_condition_toggled(self, row: int, state: int) -> None:
        if self._loading_settings or self._op_group_condition_sync:
            return
        if row < 0 or row >= self.op_groups_table.rowCount():
            return
        group_item = self.op_groups_table.item(row, 1)
        group = (group_item.text().strip().upper() if group_item else "")
        if not group:
            return
        try:
            enabled = int(state) == int(Qt.CheckState.Checked)
        except Exception:
            enabled = bool(state)
        self._op_group_condition_sync = True
        table = self.op_groups_table
        table.setUpdatesEnabled(False)
        try:
            target_rows = self._op_group_rows_by_group.get(group)
            if not target_rows:
                target_rows = []
                for r in range(self.op_groups_table.rowCount()):
                    row_group_item = self.op_groups_table.item(r, 1)
                    row_group = (row_group_item.text().strip().upper() if row_group_item else "")
                    if row_group == group:
                        target_rows.append(r)
            for r in target_rows:
                cond_widget = self.op_groups_table.cellWidget(r, 9)
                if isinstance(cond_widget, QCheckBox):
                    target_chk = cond_widget
                elif isinstance(cond_widget, QWidget):
                    target_chk = cond_widget.findChild(QCheckBox)
                else:
                    target_chk = None
                if target_chk is None:
                    continue
                if target_chk.isChecked() != enabled:
                    target_chk.blockSignals(True)
                    try:
                        target_chk.setChecked(enabled)
                    finally:
                        target_chk.blockSignals(False)
            for g in self.operating_groups:
                if str(g.get("group", "")).strip().upper() == group:
                    g["use_condition_levels"] = bool(enabled)
        finally:
            table.setUpdatesEnabled(True)
            self._op_group_condition_sync = False
        self._mark_settings_dirty()

    def _table_to_operating_groups(self) -> List[Dict[str, object]]:
        result: List[Dict[str, object]] = []
        existing_levels: Dict[Tuple[str, str, str], int] = {}
        for g in self.operating_groups:
            try:
                band_key = str(g.get("band", "")).strip().upper()
                mode_key = normalize_operating_group_mode(g.get("mode", ""), band_key)
                key = (
                    str(g.get("group", "")).strip().upper(),
                    mode_key,
                    band_key,
                )
                level = int(g.get("condition_level", 5) or 5)
            except Exception:
                continue
            if level < 1 or level > 5:
                level = 5
            existing_levels[key] = level
        group_condition_levels: Dict[str, bool] = {}
        for r in range(self.op_groups_table.rowCount()):
            group_item = self.op_groups_table.item(r, 1)
            group = (group_item.text().strip().upper() if group_item else "")
            if not group:
                continue
            cond_widget = self.op_groups_table.cellWidget(r, 9)
            use_condition_levels = False
            if isinstance(cond_widget, QCheckBox):
                use_condition_levels = cond_widget.isChecked()
            elif isinstance(cond_widget, QWidget):
                chk = cond_widget.findChild(QCheckBox)
                if chk is not None:
                    use_condition_levels = chk.isChecked()
            if group not in group_condition_levels:
                group_condition_levels[group] = use_condition_levels
            elif group_condition_levels[group] != use_condition_levels:
                # Resolve inconsistencies defensively by preferring enabled if any row is enabled.
                group_condition_levels[group] = group_condition_levels[group] or use_condition_levels
        for r in range(self.op_groups_table.rowCount()):
            group = (
                self.op_groups_table.item(r, 1).text().strip().upper() if self.op_groups_table.item(r, 1) else ""
            )
            mode = self.op_groups_table.item(r, 2).text().strip() if self.op_groups_table.item(r, 2) else ""
            band = self.op_groups_table.item(r, 3).text().strip().upper() if self.op_groups_table.item(r, 3) else ""
            mode = normalize_operating_group_mode(mode, band)
            freq_txt = self.op_groups_table.item(r, 4).text().strip() if self.op_groups_table.item(r, 4) else ""
            vfo_txt = self.op_groups_table.item(r, 5).text().strip() if self.op_groups_table.item(r, 5) else "A"
            fldigi_mode = (
                self.op_groups_table.item(r, 6).text().strip() if self.op_groups_table.item(r, 6) else ""
            )
            fldigi_offset = (
                self.op_groups_table.item(r, 7).text().strip() if self.op_groups_table.item(r, 7) else ""
            )
            auto_widget = self.op_groups_table.cellWidget(r, 8)
            auto_tune = False
            if isinstance(auto_widget, QCheckBox):
                auto_tune = auto_widget.isChecked()
            elif isinstance(auto_widget, QWidget):
                chk = auto_widget.findChild(QCheckBox)
                if chk is not None:
                    auto_tune = chk.isChecked()
            cond_widget = self.op_groups_table.cellWidget(r, 9)
            use_condition_levels = False
            if isinstance(cond_widget, QCheckBox):
                use_condition_levels = cond_widget.isChecked()
            elif isinstance(cond_widget, QWidget):
                chk = cond_widget.findChild(QCheckBox)
                if chk is not None:
                    use_condition_levels = chk.isChecked()
            use_condition_levels = bool(group_condition_levels.get(group, use_condition_levels))
            try:
                freq_val = float(freq_txt)
            except Exception:
                freq_val = None
            if group and mode and band and freq_val is not None:
                cond_level = existing_levels.get((group, mode, band), 5)
                vfo_val = (vfo_txt or "A").strip().upper()
                if vfo_val not in ("A", "B"):
                    vfo_val = "A"
                result.append(
                    {
                        "group": group,
                        "mode": mode,
                        "band": band,
                        "frequency": self._format_freq(freq_val),
                        "vfo": vfo_val,
                        "fldigi_mode": fldigi_mode,
                        "fldigi_offset": fldigi_offset,
                        "auto_tune": auto_tune,
                        "use_condition_levels": use_condition_levels,
                        "condition_level": cond_level,
                    }
                )
        return result

    def _selected_op_rows(self) -> List[int]:
        rows: List[int] = []
        for r in range(self.op_groups_table.rowCount()):
            w = self.op_groups_table.cellWidget(r, 0)
            if isinstance(w, QCheckBox) and w.isChecked():
                rows.append(r)
            elif isinstance(w, QWidget):
                chk = w.findChild(QCheckBox)
                if chk is not None and chk.isChecked():
                    rows.append(r)
        return rows

    def _edit_operating_group(self):
        rows = self._selected_op_rows()
        if not rows:
            QMessageBox.information(self, "Edit Group", "Select one Operating Group to edit.")
            return
        if len(rows) > 1:
            QMessageBox.warning(self, "Edit Group", "Please select only one Operating Group to edit.")
            return
        row = rows[0]
        group = self.op_groups_table.item(row, 1).text().strip() if self.op_groups_table.item(row, 1) else ""
        mode = self.op_groups_table.item(row, 2).text().strip() if self.op_groups_table.item(row, 2) else "Digi"
        band = self.op_groups_table.item(row, 3).text().strip() if self.op_groups_table.item(row, 3) else ""
        freq_txt = self.op_groups_table.item(row, 4).text().strip() if self.op_groups_table.item(row, 4) else ""
        vfo_txt = self.op_groups_table.item(row, 5).text().strip() if self.op_groups_table.item(row, 5) else "A"
        fldigi_mode_txt = (
            self.op_groups_table.item(row, 6).text().strip() if self.op_groups_table.item(row, 6) else ""
        )
        fldigi_offset_txt = (
            self.op_groups_table.item(row, 7).text().strip() if self.op_groups_table.item(row, 7) else ""
        )
        auto_widget = self.op_groups_table.cellWidget(row, 8)
        auto_val = False
        if isinstance(auto_widget, QCheckBox):
            auto_val = auto_widget.isChecked()
        elif isinstance(auto_widget, QWidget):
            chk = auto_widget.findChild(QCheckBox)
            if chk is not None:
                auto_val = chk.isChecked()
        cond_widget = self.op_groups_table.cellWidget(row, 9)
        use_cond_val = False
        if isinstance(cond_widget, QCheckBox):
            use_cond_val = cond_widget.isChecked()
        elif isinstance(cond_widget, QWidget):
            chk = cond_widget.findChild(QCheckBox)
            if chk is not None:
                use_cond_val = chk.isChecked()
        cond_level_val = 5
        band = (band or "").strip().upper()
        mode = normalize_operating_group_mode(mode, band)
        for g in self.operating_groups:
            if (
                str(g.get("group", "")).strip().upper() == group.strip().upper()
                and normalize_operating_group_mode(g.get("mode", ""), g.get("band", "")) == mode
                and str(g.get("band", "")).strip().upper() == band
            ):
                try:
                    cond_level_val = int(g.get("condition_level", 5) or 5)
                except Exception:
                    cond_level_val = 5
                break
        if cond_level_val < 1 or cond_level_val > 5:
            cond_level_val = 5

        dlg = QDialog(self)
        dlg.setWindowTitle("Edit Operating Group")
        form = QFormLayout(dlg)

        name_edit = QLineEdit(group)
        form.addRow("Group Name:", name_edit)

        mode_combo = QComboBox()
        mode_combo.addItems(["Digi", "SSB"])
        mode_combo.setMinimumWidth(110)
        mode_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        mode_val = normalize_operating_group_mode(mode, band)
        if mode_val in ["Digi", "SSB"]:
            mode_combo.setCurrentText(mode_val)
        form.addRow("Mode:", mode_combo)

        band_combo = QComboBox()
        band_combo.addItems(
            [
                "20M",
                "40M",
                "80M",
                "2M",
                "6M",
                "10M",
                "12M",
                "15M",
                "17M",
                "30M",
                "60M",
            ]
        )
        band_combo.setMinimumWidth(110)
        band_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        if band and band_combo.findText(band) >= 0:
            band_combo.setCurrentText(band)
        form.addRow("Band:", band_combo)

        freq_edit = QLineEdit(freq_txt)
        form.addRow("Frequency (MHz):", freq_edit)

        vfo_combo = QComboBox()
        vfo_combo.addItems(["A", "B"])
        vfo_combo.setMinimumWidth(110)
        vfo_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        vfo_val = vfo_txt.strip().upper()
        if vfo_val in ("A", "B"):
            vfo_combo.setCurrentText(vfo_val)
        form.addRow("VFO:", vfo_combo)

        fldigi_mode_combo = QComboBox()
        fldigi_mode_combo.setEditable(True)
        fldigi_mode_combo.addItems(FLDIGI_MODE_OPTIONS)
        fldigi_mode_combo.setInsertPolicy(QComboBox.NoInsert)
        completer = QCompleter(FLDIGI_MODE_OPTIONS, fldigi_mode_combo)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        fldigi_mode_combo.setCompleter(completer)
        if fldigi_mode_txt:
            fldigi_mode_combo.setCurrentText(fldigi_mode_txt)
        form.addRow("FLDigi Starting Mode:", fldigi_mode_combo)

        _manual_voice_override = {"enabled": False}

        def _default_voice_mode() -> str:
            return voice_sideband_for_band(band_combo.currentText())

        def _sync_fldigi_voice_default(*, force: bool = False) -> None:
            if (mode_combo.currentText() or "").strip().upper() != "SSB":
                return
            current = (fldigi_mode_combo.currentText() or "").strip().upper()
            desired = _default_voice_mode()
            if not force and _manual_voice_override["enabled"]:
                return
            if force or not current or current in {"USB", "LSB"}:
                fldigi_mode_combo.blockSignals(True)
                fldigi_mode_combo.setCurrentText(desired)
                fldigi_mode_combo.blockSignals(False)
                _manual_voice_override["enabled"] = False

        def _on_mode_changed(_text: str) -> None:
            if (mode_combo.currentText() or "").strip().upper() != "SSB":
                _manual_voice_override["enabled"] = False
                return
            _sync_fldigi_voice_default()

        def _on_band_changed(*_args) -> None:
            _sync_fldigi_voice_default()

        def _on_fldigi_mode_changed(text: str) -> None:
            if (mode_combo.currentText() or "").strip().upper() != "SSB":
                _manual_voice_override["enabled"] = False
                return
            value = (text or "").strip().upper()
            desired = _default_voice_mode()
            _manual_voice_override["enabled"] = bool(value) and value != desired

        mode_combo.currentTextChanged.connect(_on_mode_changed)
        band_combo.currentTextChanged.connect(_on_band_changed)
        fldigi_mode_combo.currentTextChanged.connect(_on_fldigi_mode_changed)
        current_mode = (mode_combo.currentText() or "").strip().upper()
        current_fldigi = (fldigi_mode_combo.currentText() or "").strip().upper()
        if current_mode == "SSB" and current_fldigi:
            _manual_voice_override["enabled"] = current_fldigi != _default_voice_mode()
        else:
            _sync_fldigi_voice_default(force=True)

        fldigi_offset_edit = QLineEdit(fldigi_offset_txt)
        fldigi_offset_edit.setValidator(QIntValidator(0, 99999, fldigi_offset_edit))
        fldigi_offset_edit.setPlaceholderText("e.g., 900")
        form.addRow("FLDigi Offset:", fldigi_offset_edit)

        auto_tune_chk = QCheckBox("Enable Auto-Tune on QSY")
        auto_tune_chk.setChecked(auto_val)
        form.addRow("", auto_tune_chk)

        use_condition_levels_chk = QCheckBox("Use Condition Levels (Group)")
        use_condition_levels_chk.setChecked(use_cond_val)
        form.addRow("", use_condition_levels_chk)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        def on_accept():
            new_name = name_edit.text().strip()
            new_band = band_combo.currentText().strip().upper()
            new_mode = normalize_operating_group_mode(mode_combo.currentText(), new_band)
            new_freq_txt = freq_edit.text().strip()
            if not new_name:
                QMessageBox.warning(self, "Validation", "Group Name is required.")
                return
            if not self._validate_band_frequency(new_band, new_mode, new_freq_txt):
                QMessageBox.warning(
                    self, "Validation", f"Frequency {new_freq_txt} invalid for {new_band} {new_mode}."
                )
                return
            offset_txt = fldigi_offset_edit.text().strip()
            if offset_txt:
                try:
                    int(offset_txt)
                except Exception:
                    QMessageBox.warning(self, "Validation", "FLDigi Offset must be an integer.")
                    return
            fldigi_mode = fldigi_mode_combo.currentText().strip()
            vfo = vfo_combo.currentText().strip().upper() or "A"
            # Remove old entry, then insert updated
            self.operating_groups = [
                g
                for g in self.operating_groups
                if not (g.get("group") == group and g.get("mode") == mode and g.get("band") == band)
            ]
            self._upsert_operating_group(
                new_name,
                new_mode,
                new_band,
                new_freq_txt,
                auto_tune=auto_tune_chk.isChecked(),
                vfo=vfo,
                fldigi_mode=fldigi_mode,
                fldigi_offset=offset_txt,
                use_condition_levels=use_condition_levels_chk.isChecked(),
                condition_level=cond_level_val,
            )
            dlg.accept()

        ok_btn.clicked.connect(on_accept)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

    def _delete_operating_groups(self):
        rows = self._selected_op_rows()
        if not rows:
            QMessageBox.information(self, "Delete Groups", "Select one or more HF Operating Groups to delete.")
            return
        to_remove = set()
        for r in rows:
            group = self.op_groups_table.item(r, 1).text().strip() if self.op_groups_table.item(r, 1) else ""
            mode = self.op_groups_table.item(r, 2).text().strip() if self.op_groups_table.item(r, 2) else ""
            band = self.op_groups_table.item(r, 3).text().strip() if self.op_groups_table.item(r, 3) else ""
            if group and mode and band:
                to_remove.add((group, mode, band))
        if not to_remove:
            return
        self.operating_groups = [
            g
            for g in self.operating_groups
            if (g.get("group"), g.get("mode"), g.get("band")) not in to_remove
        ]
        self._refresh_operating_groups_table()
        try:
            self._save_settings_quiet()
            self._settings_dirty = False
            self._set_save_button_state("success")
            try:
                self.settings_saved.emit()
            except Exception:
                pass
        except Exception:
            log.exception("Failed to persist Operating Group deletions; will remain in-memory only.")
        QMessageBox.information(self, "Delete Groups", f"Deleted {len(to_remove)} HF Operating Group(s).")

    # ---------- Local Net Profiles ---------- #

    def _normalize_local_net_profile(self, row: Dict) -> Dict[str, str]:
        # Legacy map: name->group, service->resource.
        group = str(row.get("group", row.get("name", "")) or "").strip()
        resource = str(row.get("resource", row.get("service", "")) or "").strip()
        mode = str(row.get("mode", "") or "").strip()
        target = str(row.get("target", "") or "").strip()
        notes = str(row.get("notes", "") or "").strip()
        if not resource:
            resource = LOCAL_NET_RESOURCE_OPTIONS[0]
        return {
            "group": group,
            "resource": resource,
            "mode": mode,
            "target": target,
            "notes": notes,
        }

    @staticmethod
    def _local_net_profile_row_key(row: Dict[str, str]) -> Tuple[str, str, str, str]:
        return (
            str(row.get("group", "")).strip().upper(),
            str(row.get("resource", "")).strip().upper(),
            str(row.get("mode", "")).strip().upper(),
            str(row.get("target", "")).strip().upper(),
        )

    def _table_to_local_net_profiles(self) -> List[Dict[str, str]]:
        cleaned: List[Dict[str, str]] = []
        seen: set[Tuple[str, str, str, str]] = set()
        for raw in self.local_net_profiles:
            if not isinstance(raw, dict):
                continue
            row = self._normalize_local_net_profile(raw)
            group = row.get("group", "")
            key = self._local_net_profile_row_key(row)
            if not group.strip() or key in seen:
                continue
            seen.add(key)
            cleaned.append(row)
        cleaned.sort(
            key=lambda r: (
                r.get("group", "").lower(),
                r.get("resource", "").lower(),
                r.get("mode", "").lower(),
                r.get("target", "").lower(),
            )
        )
        self.local_net_profiles = cleaned
        return [dict(r) for r in cleaned]

    def _selected_local_net_rows(self) -> List[int]:
        rows: List[int] = []
        for r in range(self.local_net_table.rowCount()):
            w = self.local_net_table.cellWidget(r, 0)
            if isinstance(w, QCheckBox) and w.isChecked():
                rows.append(r)
            elif isinstance(w, QWidget):
                chk = w.findChild(QCheckBox)
                if chk is not None and chk.isChecked():
                    rows.append(r)
        return rows

    def _update_local_net_action_buttons(self) -> None:
        theme = resolve_theme(self.settings)
        has_selection = bool(self._selected_local_net_rows()) if hasattr(self, "local_net_table") else False
        role = "info" if has_selection else "muted"
        self.add_local_net_btn.setStyleSheet(button_style("primary", theme))
        self.edit_local_net_btn.setEnabled(True)
        self.delete_local_net_btn.setEnabled(True)
        self.edit_local_net_btn.setStyleSheet(button_style(role, theme))
        self.delete_local_net_btn.setStyleSheet(button_style(role, theme))

    def _refresh_local_net_profiles_table(self) -> None:
        rows = self._table_to_local_net_profiles()
        table = self.local_net_table
        table.setRowCount(0)
        for prof in rows:
            row = table.rowCount()
            table.insertRow(row)
            sel_chk = QCheckBox()
            sel_chk.setFixedWidth(22)
            sel_chk.stateChanged.connect(self._update_local_net_action_buttons)
            sel_wrap = QWidget()
            sel_layout = QHBoxLayout(sel_wrap)
            sel_layout.setContentsMargins(0, 0, 0, 0)
            sel_layout.setAlignment(Qt.AlignCenter)
            sel_layout.addWidget(sel_chk)
            table.setCellWidget(row, 0, sel_wrap)
            table.setItem(row, 1, QTableWidgetItem(prof.get("group", "")))
            table.setItem(row, 2, QTableWidgetItem(prof.get("resource", "")))
            table.setItem(row, 3, QTableWidgetItem(prof.get("mode", "")))
            table.setItem(row, 4, QTableWidgetItem(prof.get("target", "")))
            table.setItem(row, 5, QTableWidgetItem(prof.get("notes", "")))
        # These group tables sit in compact Settings panels; six rows keeps the section scannable.
        self._fit_table_height_to_rows(table, min_rows=1, max_rows=6, extra_rows=1)
        self._refresh_fit_content_section_height(getattr(self, "local_net_section_group", None))
        self._update_local_net_action_buttons()
        self._refresh_section_titles()

    def _local_profile_from_row(self, row: int) -> Dict[str, str]:
        return {
            "group": self.local_net_table.item(row, 1).text().strip() if self.local_net_table.item(row, 1) else "",
            "resource": self.local_net_table.item(row, 2).text().strip() if self.local_net_table.item(row, 2) else "",
            "mode": self.local_net_table.item(row, 3).text().strip() if self.local_net_table.item(row, 3) else "",
            "target": self.local_net_table.item(row, 4).text().strip() if self.local_net_table.item(row, 4) else "",
            "notes": self.local_net_table.item(row, 5).text().strip() if self.local_net_table.item(row, 5) else "",
        }

    def _open_local_net_profile_dialog(self, existing: Optional[Dict[str, str]] = None) -> Optional[Dict[str, str]]:
        dlg = QDialog(self)
        dlg.setWindowTitle("Edit Local Net Entry" if existing else "Add Local Net Entry")
        form = QFormLayout(dlg)

        group_edit = QLineEdit((existing or {}).get("group", ""))
        resource_combo = QComboBox()
        resource_combo.setEditable(True)
        resource_combo.addItems(LOCAL_NET_RESOURCE_OPTIONS)
        if existing and (existing or {}).get("resource"):
            resource_combo.setCurrentText((existing or {}).get("resource", ""))
        mode_combo = QComboBox()
        mode_combo.setEditable(True)
        mode_combo.addItems(["Voice", "Data", "Mixed", "FM", "Digital"])
        if existing and (existing or {}).get("mode"):
            mode_combo.setCurrentText((existing or {}).get("mode", ""))
        target_edit = QLineEdit((existing or {}).get("target", ""))
        target_edit.setPlaceholderText("e.g., 146.520, Ch 16, or repeater pair/tone")
        notes_edit = QLineEdit((existing or {}).get("notes", ""))
        notes_edit.setPlaceholderText("Optional notes for SOP reminder context")

        form.addRow("Group:", group_edit)
        form.addRow("Resource:", resource_combo)
        form.addRow("Mode:", mode_combo)
        form.addRow("Target:", target_edit)
        form.addRow("Notes:", notes_edit)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        out: Dict[str, str] = {}

        def _accept() -> None:
            candidate = self._normalize_local_net_profile(
                {
                    "group": group_edit.text(),
                    "resource": resource_combo.currentText(),
                    "mode": mode_combo.currentText(),
                    "target": target_edit.text(),
                    "notes": notes_edit.text(),
                }
            )
            if not candidate.get("group"):
                QMessageBox.warning(self, "Validation", "Group is required.")
                return
            out.update(candidate)
            dlg.accept()

        ok_btn.clicked.connect(_accept)
        cancel_btn.clicked.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out if out else None

    def _upsert_local_net_profile(self, profile: Dict[str, str], old_row: Optional[Dict[str, str]] = None) -> None:
        normalized = self._normalize_local_net_profile(profile)
        new_group = normalized.get("group", "").strip().upper()
        if not new_group:
            return
        new_key = self._local_net_profile_row_key(normalized)
        old_key: Optional[Tuple[str, str, str, str]] = None
        if isinstance(old_row, dict):
            old_key = self._local_net_profile_row_key(self._normalize_local_net_profile(old_row))
        self.local_net_profiles = [
            r
            for r in self.local_net_profiles
            if self._local_net_profile_row_key(self._normalize_local_net_profile(r)) not in {new_key, old_key}
        ]
        self.local_net_profiles.append(normalized)
        self._refresh_local_net_profiles_table()
        try:
            # Persist Local Net Profiles directly so this workflow is not blocked
            # by unrelated full-settings validation requirements.
            if hasattr(self.settings, "set"):
                self.settings.set("local_net_profiles", self._table_to_local_net_profiles())
            elif hasattr(self.settings, "_data"):
                self.settings._data["local_net_profiles"] = self._table_to_local_net_profiles()  # type: ignore[attr-defined]
            self._settings_dirty = False
            self._set_save_button_state("success")
            try:
                self.local_net_profiles_changed.emit()
            except Exception:
                pass
        except Exception:
            log.exception("Failed to persist Local Net Profile; will remain in-memory only.")

    def _add_local_net_profile(self) -> None:
        created = self._open_local_net_profile_dialog(existing=None)
        if not created:
            return
        self._upsert_local_net_profile(created)

    def _edit_local_net_profile(self) -> None:
        rows = self._selected_local_net_rows()
        if not rows:
            QMessageBox.information(self, "Edit Entry", "Select one Local Net entry to edit.")
            return
        if len(rows) > 1:
            QMessageBox.warning(self, "Edit Entry", "Please select only one Local Net entry to edit.")
            return
        row = rows[0]
        existing = self._local_profile_from_row(row)
        updated = self._open_local_net_profile_dialog(existing=existing)
        if not updated:
            return
        self._upsert_local_net_profile(updated, old_row=existing)

    def _delete_local_net_profiles(self) -> None:
        rows = self._selected_local_net_rows()
        if not rows:
            QMessageBox.information(self, "Delete Entries", "Select one or more Local Net entries to delete.")
            return
        to_remove: set[Tuple[str, str, str, str]] = set()
        for r in rows:
            row_obj = self._local_profile_from_row(r)
            key = self._local_net_profile_row_key(self._normalize_local_net_profile(row_obj))
            if key[0]:
                to_remove.add(key)
        if not to_remove:
            return
        self.local_net_profiles = [
            row
            for row in self.local_net_profiles
            if self._local_net_profile_row_key(self._normalize_local_net_profile(row)) not in to_remove
        ]
        self._refresh_local_net_profiles_table()
        try:
            # Persist Local Net Profiles directly so this workflow is not blocked
            # by unrelated full-settings validation requirements.
            if hasattr(self.settings, "set"):
                self.settings.set("local_net_profiles", self._table_to_local_net_profiles())
            elif hasattr(self.settings, "_data"):
                self.settings._data["local_net_profiles"] = self._table_to_local_net_profiles()  # type: ignore[attr-defined]
            self._settings_dirty = False
            self._set_save_button_state("success")
            try:
                self.local_net_profiles_changed.emit()
            except Exception:
                pass
        except Exception:
            log.exception("Failed to persist Local Net Profile deletions; will remain in-memory only.")
        QMessageBox.information(self, "Delete Entries", f"Deleted {len(to_remove)} Local Net entr{'y' if len(to_remove) == 1 else 'ies'}.")

    # ---------- GPG authenticity ---------- #

    def _current_gpg_path(self) -> str:
        return self.gpg_path_edit.text().strip() if hasattr(self, "gpg_path_edit") else ""

    def _set_gpg_status(self, text: str, *, error: bool = False) -> None:
        if not hasattr(self, "gpg_status_label"):
            return
        self.gpg_status_label.setText(str(text or "").strip() or ("GPG status: error" if error else "GPG status: ready"))

    def _gpg_key_row_filter_text(self, row: int) -> str:
        if not hasattr(self, "gpg_keys_table") or row < 0:
            return ""
        fpr_item = self.gpg_keys_table.item(row, 1)
        uid_item = self.gpg_keys_table.item(row, 2)
        parts = [
            fpr_item.text() if fpr_item else "",
            uid_item.text() if uid_item else "",
        ]
        return " ".join(str(part or "").lower() for part in parts)

    def _gpg_key_row_is_trusted(self, row: int) -> bool:
        if not hasattr(self, "gpg_keys_table") or row < 0:
            return False
        trusted_item = self.gpg_keys_table.item(row, 0)
        return bool(trusted_item and trusted_item.checkState() == Qt.Checked)

    def _gpg_key_row_matches_filter(self, row: int, tokens: List[str]) -> bool:
        row_text = self._gpg_key_row_filter_text(row)
        trusted = self._gpg_key_row_is_trusted(row)
        for token in tokens:
            if token == "trusted":
                if not trusted:
                    return False
                continue
            if token in {"untrusted", "unchecked", "not-trusted"}:
                if trusted:
                    return False
                continue
            if token not in row_text:
                return False
        return True

    def _apply_gpg_key_filter(self, *_args) -> None:
        if not hasattr(self, "gpg_keys_table"):
            return
        query = ""
        if hasattr(self, "gpg_key_filter_edit"):
            query = self.gpg_key_filter_edit.text().strip().lower()
        tokens = [token for token in query.split() if token]
        first_visible = -1
        current_row = self.gpg_keys_table.currentRow()
        current_hidden = False
        for row in range(self.gpg_keys_table.rowCount()):
            visible = self._gpg_key_row_matches_filter(row, tokens)
            self.gpg_keys_table.setRowHidden(row, not visible)
            if visible and first_visible < 0:
                first_visible = row
            if row == current_row and not visible:
                current_hidden = True
        if current_hidden and first_visible >= 0:
            self.gpg_keys_table.selectRow(first_visible)
        elif first_visible < 0 and tokens:
            self.gpg_keys_table.clearSelection()
        self._update_gpg_sign_button_state()
        self._refresh_gpg_key_detail()

    def _refresh_gpg_key_detail(self) -> None:
        if not hasattr(self, "gpg_key_detail_label") or not hasattr(self, "gpg_keys_table"):
            return
        row = self.gpg_keys_table.currentRow()
        if row < 0 or self.gpg_keys_table.isRowHidden(row):
            if not self.gpg_keys_table.rowCount():
                text = "No GPG keys loaded."
            elif any(not self.gpg_keys_table.isRowHidden(idx) for idx in range(self.gpg_keys_table.rowCount())):
                text = "Select a key to view details."
            else:
                text = "No keys match the current filter."
            self.gpg_key_detail_label.setText(text)
            return
        trusted_item = self.gpg_keys_table.item(row, 0)
        fpr_item = self.gpg_keys_table.item(row, 1)
        uid_item = self.gpg_keys_table.item(row, 2)
        trust_text = "Trusted" if trusted_item and trusted_item.checkState() == Qt.Checked else "Not trusted"
        fpr = fpr_item.text() if fpr_item else ""
        uid_text = uid_item.text() if uid_item else "(no user id)"
        self.gpg_key_detail_label.setText(f"{trust_text} | Fingerprint: {fpr} | User IDs: {uid_text}")

    def _refresh_gpg_keys_table(self, *, show_dialog_on_error: bool = True) -> None:
        if not hasattr(self, "gpg_keys_table"):
            return
        self._gpg_keys_auto_probe_attempted = True
        with perf_span("settings.refresh_gpg_keys_table", settings=self.settings, min_ms=10.0):
            configured = self._current_gpg_path()
            ok, msg, resolved = gpg_available(configured)
            if not ok:
                self._gpg_keys_loaded = False
                self._set_gpg_status(f"GPG unavailable: {msg}", error=True)
                self._gpg_keys_table_loading = True
                try:
                    self.gpg_keys_table.setRowCount(0)
                finally:
                    self._gpg_keys_table_loading = False
                self._refresh_gpg_key_detail()
                self._update_gpg_sign_button_state()
                if show_dialog_on_error:
                    QMessageBox.warning(
                        self,
                        "GPG",
                        f"{msg}\n\nInstall GPG or set the executable path in Settings.",
                    )
                return

            if resolved:
                self._set_gpg_status(f"GPG ready: {resolved}")
            else:
                self._set_gpg_status("GPG ready.")
            keys, err = list_public_keys(configured_path=configured)
            if err:
                self._gpg_keys_loaded = False
                self._set_gpg_status(f"GPG key list failed: {err}", error=True)
                if show_dialog_on_error:
                    QMessageBox.warning(self, "GPG", err)
                return
            self._gpg_keys_loaded = True
            self._gpg_keys_table_loading = True
            try:
                self.gpg_keys_table.setRowCount(0)
                for row_idx, key in enumerate(keys):
                    self.gpg_keys_table.insertRow(row_idx)
                    fpr = normalize_fingerprint(key.fingerprint)
                    trusted = fpr in self._gpg_trusted_fingerprints
                    trusted_item = QTableWidgetItem("")
                    trusted_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
                    trusted_item.setCheckState(Qt.Checked if trusted else Qt.Unchecked)
                    trusted_item.setData(Qt.UserRole, fpr)
                    self.gpg_keys_table.setItem(row_idx, 0, trusted_item)

                    fpr_item = QTableWidgetItem(fpr)
                    fpr_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                    self.gpg_keys_table.setItem(row_idx, 1, fpr_item)

                    uid_text = "; ".join([u for u in key.user_ids if str(u).strip()]) or "(no user id)"
                    uid_item = QTableWidgetItem(uid_text)
                    uid_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                    self.gpg_keys_table.setItem(row_idx, 2, uid_item)
            finally:
                self._gpg_keys_table_loading = False
            self._apply_gpg_key_filter()
            self._update_gpg_sign_button_state()
            self._refresh_gpg_signing_keys(show_dialog_on_error=False)

    def _refresh_gpg_signing_keys(self, *, show_dialog_on_error: bool = True) -> None:
        if not hasattr(self, "gpg_signing_key_combo"):
            return
        configured = self._current_gpg_path()
        saved = normalize_fingerprint(str(self.settings.get("gpg_compose_signing_key_fingerprint", "") or ""))
        current = normalize_fingerprint(str(self.gpg_signing_key_combo.currentData() or ""))
        preferred = current or saved
        keys, err = list_secret_keys(configured_path=configured)
        self._gpg_signing_keys_loading = True
        try:
            self.gpg_signing_key_combo.clear()
            self.gpg_signing_key_combo.addItem("Auto-select when only one private key is available", "")
            selected_index = 0
            for key in keys:
                fpr = normalize_fingerprint(key.fingerprint)
                if not fpr:
                    continue
                self.gpg_signing_key_combo.addItem(gpg_key_display_label(key), fpr)
                if preferred and fpr == preferred:
                    selected_index = self.gpg_signing_key_combo.count() - 1
            self.gpg_signing_key_combo.setCurrentIndex(selected_index)
        finally:
            self._gpg_signing_keys_loading = False
        if err:
            text = f"Signing keys unavailable: {err}"
            if hasattr(self, "gpg_signing_status_label"):
                self.gpg_signing_status_label.setText(text)
            if show_dialog_on_error:
                QMessageBox.warning(self, "GPG Signing Keys", err)
            return
        count = self.gpg_signing_key_combo.count() - 1
        if hasattr(self, "gpg_signing_status_label"):
            if count == 0:
                self.gpg_signing_status_label.setText(
                    "No private signing keys found. Import or create a GPG private key to sign FLAmp files."
                )
            elif count == 1:
                self.gpg_signing_status_label.setText(
                    "One private signing key found. Compose can use it automatically when signing is enabled."
                )
            else:
                self.gpg_signing_status_label.setText(
                    "Multiple private signing keys found. Choose a default here or select one in Compose."
                )
        self._refresh_gpg_signing_passphrase_status()

    def _selected_gpg_signing_fingerprint(self) -> str:
        if not hasattr(self, "gpg_signing_key_combo"):
            return ""
        fpr = normalize_fingerprint(str(self.gpg_signing_key_combo.currentData() or ""))
        if fpr:
            return fpr
        if self.gpg_signing_key_combo.count() == 2:
            return normalize_fingerprint(str(self.gpg_signing_key_combo.itemData(1) or ""))
        return ""

    def _refresh_gpg_signing_passphrase_status(self) -> None:
        if not hasattr(self, "gpg_signing_passphrase_edit"):
            return
        fpr = self._selected_gpg_signing_fingerprint()
        store_ok, store_msg = credential_store_available()
        enabled = bool(fpr)
        self.gpg_signing_passphrase_edit.setEnabled(enabled)
        self.gpg_signing_passphrase_confirm_edit.setEnabled(enabled)
        self.gpg_check_save_passphrase_btn.setEnabled(enabled)
        self.gpg_clear_passphrase_btn.setEnabled(bool(fpr and store_ok))
        if not fpr:
            if hasattr(self, "gpg_signing_key_combo") and self.gpg_signing_key_combo.count() > 2:
                self.gpg_signing_status_label.setText(
                    "Choose a specific signing key before entering a passphrase. Auto-select only works when one private key is available."
                )
            else:
                self.gpg_signing_status_label.setText(
                    "Refresh Signing Keys and choose a signing key before entering a passphrase."
                )
            self.gpg_signing_passphrase_edit.setPlaceholderText("Select a signing key first")
            return
        self.gpg_signing_passphrase_edit.setPlaceholderText("Stored in OS credential store, not FIO settings")
        if not store_ok:
            self.gpg_signing_status_label.setText(store_msg)
            return
        saved, err = has_gpg_signing_passphrase(fpr)
        if err:
            self.gpg_signing_status_label.setText(err)
        elif saved:
            self.gpg_signing_status_label.setText(
                f"Saved passphrase is available for signing key {fpr[-16:]} in the OS credential store."
            )
        else:
            self.gpg_signing_status_label.setText(
                f"No saved passphrase for signing key {fpr[-16:]}. Use Check/Save if the key requires one."
            )

    def _check_and_save_gpg_signing_passphrase(self) -> None:
        fpr = self._selected_gpg_signing_fingerprint()
        if not fpr:
            QMessageBox.warning(self, "GPG Passphrase", "Select a default FLAmp signing key first.")
            return
        store_ok, store_msg = credential_store_available()
        if not store_ok:
            QMessageBox.warning(self, "GPG Passphrase", store_msg)
            return
        passphrase = self.gpg_signing_passphrase_edit.text() if hasattr(self, "gpg_signing_passphrase_edit") else ""
        confirm = (
            self.gpg_signing_passphrase_confirm_edit.text()
            if hasattr(self, "gpg_signing_passphrase_confirm_edit")
            else ""
        )
        if passphrase or confirm:
            if passphrase != confirm:
                self.gpg_signing_status_label.setText("Passphrase entries do not match.")
                return
        with tempfile.TemporaryDirectory(prefix="fio-gpg-check-") as tmpdir:
            src = Path(tmpdir) / "fio-passphrase-check.k2s"
            dst = Path(tmpdir) / "fio-passphrase-check-sig.k2s"
            src.write_text("FreqInOut GPG signing passphrase check\n", encoding="utf-8")
            ok, detail = clearsign_file(
                src,
                output_path=dst,
                configured_path=self._current_gpg_path(),
                signer_fingerprint=fpr,
                passphrase=passphrase if passphrase else None,
            )
        if not ok:
            if gpg_detail_indicates_passphrase_needed(detail) and not passphrase:
                self.gpg_signing_status_label.setText("This signing key requires a passphrase. Enter it and click Check/Save.")
            else:
                self.gpg_signing_status_label.setText(f"Passphrase check failed: {detail}")
            return
        if passphrase:
            saved, msg = store_gpg_signing_passphrase(fpr, passphrase)
            self.gpg_signing_passphrase_edit.clear()
            self.gpg_signing_passphrase_confirm_edit.clear()
            passphrase = ""
            confirm = ""
            if not saved:
                self.gpg_signing_status_label.setText(msg)
                return
            self.gpg_signing_status_label.setText(msg)
        else:
            self.gpg_signing_status_label.setText("Signing check passed. This key did not require a saved passphrase.")
        self._refresh_gpg_signing_passphrase_status()

    def _clear_gpg_signing_passphrase(self) -> None:
        fpr = self._selected_gpg_signing_fingerprint()
        if not fpr:
            QMessageBox.warning(self, "GPG Passphrase", "Select a default FLAmp signing key first.")
            return
        ok, msg = delete_gpg_signing_passphrase(fpr)
        if hasattr(self, "gpg_signing_passphrase_edit"):
            self.gpg_signing_passphrase_edit.clear()
        if hasattr(self, "gpg_signing_passphrase_confirm_edit"):
            self.gpg_signing_passphrase_confirm_edit.clear()
        self.gpg_signing_status_label.setText(msg)
        if not ok:
            QMessageBox.warning(self, "GPG Passphrase", msg)

    def _on_gpg_signing_key_changed(self) -> None:
        if self._gpg_signing_keys_loading:
            return
        self._mark_settings_dirty()
        if hasattr(self, "gpg_signing_passphrase_edit"):
            self.gpg_signing_passphrase_edit.clear()
        if hasattr(self, "gpg_signing_passphrase_confirm_edit"):
            self.gpg_signing_passphrase_confirm_edit.clear()
        self._refresh_gpg_signing_passphrase_status()
        self._refresh_section_titles()

    def _on_gpg_keys_table_item_changed(self, item: QTableWidgetItem) -> None:
        if self._gpg_keys_table_loading:
            return
        if item is None or item.column() != 0:
            return
        fpr = normalize_fingerprint(str(item.data(Qt.UserRole) or ""))
        if not fpr and item.row() >= 0 and hasattr(self, "gpg_keys_table"):
            cell = self.gpg_keys_table.item(item.row(), 1)
            fpr = normalize_fingerprint(cell.text() if cell else "")
        if not fpr:
            return
        if item.checkState() == Qt.Checked:
            self._gpg_trusted_fingerprints.add(fpr)
        else:
            self._gpg_trusted_fingerprints.discard(fpr)
        self._mark_settings_dirty()
        self._refresh_section_titles()
        self._apply_gpg_key_filter()
        self._refresh_gpg_key_detail()

    def _update_gpg_sign_button_state(self) -> None:
        if not hasattr(self, "gpg_sign_key_btn"):
            return
        self.gpg_sign_key_btn.setEnabled(bool(self._selected_gpg_fingerprint()))

    def _selected_gpg_fingerprint(self) -> str:
        if not hasattr(self, "gpg_keys_table"):
            return ""
        row = self.gpg_keys_table.currentRow()
        if row < 0:
            return ""
        if self.gpg_keys_table.isRowHidden(row):
            return ""
        item = self.gpg_keys_table.item(row, 1)
        return normalize_fingerprint(item.text() if item else "")

    def _choose_gpg_executable_path(self) -> None:
        start = self._current_gpg_path()
        fn, _ = QFileDialog.getOpenFileName(self, "Select GPG executable", start)
        if not fn:
            return
        self.gpg_path_edit.setText(fn)
        self._gpg_keys_loaded = False
        self._gpg_keys_auto_probe_attempted = False
        self._mark_settings_dirty()

    def _test_gpg_executable(self) -> None:
        current_path = self._current_gpg_path()
        ok, msg, resolved = gpg_available(current_path)
        if ok:
            if resolved and current_path and Path(current_path).name.lower() not in {"gpg", "gpg2", "gpg.exe", "gpg2.exe"}:
                self.gpg_path_edit.setText(resolved)
                self._gpg_keys_loaded = False
                self._gpg_keys_auto_probe_attempted = False
                self._mark_settings_dirty()
            detail = msg
            if resolved:
                detail = f"{msg}\nPath: {resolved}"
            self._set_gpg_status(f"GPG ready: {resolved or msg}")
            QMessageBox.information(self, "GPG", detail)
            return
        self._set_gpg_status(f"GPG unavailable: {msg}", error=True)
        QMessageBox.warning(self, "GPG", msg)

    def _import_gpg_key_file(self) -> None:
        fn, _ = QFileDialog.getOpenFileName(
            self,
            "Import GPG public key",
            "",
            "Key Files (*.asc *.pgp *.gpg *.key *.txt);;All Files (*)",
        )
        if not fn:
            return
        ok, msg = import_public_key_file(fn, configured_path=self._current_gpg_path())
        if not ok:
            self._set_gpg_status(f"Key import failed: {msg}", error=True)
            QMessageBox.warning(self, "GPG Import", msg)
            return
        self._set_gpg_status("Public key imported.")
        self._refresh_gpg_keys_table(show_dialog_on_error=False)
        self._mark_settings_dirty()
        QMessageBox.information(self, "GPG Import", "Public key imported successfully.")

    def _import_gpg_key_text(self) -> None:
        dlg = QDialog(self)
        dlg.setWindowTitle("Import Armored GPG Key")
        dlg.resize(720, 460)
        layout = QVBoxLayout(dlg)
        info = QLabel("Paste an armored public key block.")
        layout.addWidget(info)
        text_edit = QTextEdit()
        text_edit.setPlaceholderText("-----BEGIN PGP PUBLIC KEY BLOCK-----")
        layout.addWidget(text_edit, 1)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        ok_btn = btns.button(QDialogButtonBox.Ok)
        if ok_btn is not None:
            ok_btn.setText("Import Key")
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)
        if dlg.exec() != QDialog.Accepted:
            return
        payload = text_edit.toPlainText().strip()
        if not payload:
            QMessageBox.warning(self, "GPG Import", "No key text provided.")
            return
        ok, msg = import_public_key_text(payload, configured_path=self._current_gpg_path())
        if not ok:
            self._set_gpg_status(f"Key import failed: {msg}", error=True)
            QMessageBox.warning(self, "GPG Import", msg)
            return
        self._set_gpg_status("Public key imported.")
        self._refresh_gpg_keys_table(show_dialog_on_error=False)
        self._mark_settings_dirty()
        QMessageBox.information(self, "GPG Import", "Public key imported successfully.")

    def _local_sign_selected_gpg_key(self) -> None:
        fpr = self._selected_gpg_fingerprint()
        if not fpr:
            QMessageBox.information(self, "GPG", "Select one key to local-sign.")
            return
        resp = QMessageBox.question(
            self,
            "Local-Sign Key",
            "This will run GPG local-sign for the selected key.\nContinue?",
        )
        if resp != QMessageBox.Yes:
            return
        ok, msg = local_sign_key(fpr, configured_path=self._current_gpg_path())
        if not ok:
            self._set_gpg_status(f"Local-sign failed: {msg}", error=True)
            QMessageBox.warning(self, "GPG", msg)
            return
        self._set_gpg_status("Key local-sign complete.")
        self._refresh_gpg_keys_table(show_dialog_on_error=False)
        QMessageBox.information(self, "GPG", "Key local-sign completed.")

    def _selected_hash_algo(self) -> str:
        if not hasattr(self, "trusted_hash_algo_combo"):
            return ""
        txt = str(self.trusted_hash_algo_combo.currentText() or "").strip().lower()
        if txt == "auto":
            return ""
        return normalize_hash_algorithm(txt)

    def _normalize_single_hash_entry(self, hash_value: str, algorithm: str = "", label: str = "", enabled: bool = True) -> dict | None:
        hash_norm = normalize_hash_hex(hash_value)
        if not hash_norm:
            return None
        algo = normalize_hash_algorithm(algorithm) or infer_algorithm_from_hash(hash_norm)
        if not algo:
            return None
        return {
            "enabled": bool(enabled),
            "algorithm": algo,
            "hash": hash_norm,
            "label": str(label or "").strip(),
        }

    def _refresh_trusted_hash_table(self) -> None:
        if not hasattr(self, "trusted_hash_table"):
            return
        self._trusted_hash_entries = normalize_trusted_hash_entries(self._trusted_hash_entries)
        self._trusted_hashes_table_loading = True
        try:
            self.trusted_hash_table.setRowCount(0)
            for idx, row in enumerate(self._trusted_hash_entries):
                self.trusted_hash_table.insertRow(idx)
                use_item = QTableWidgetItem("")
                use_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
                use_item.setCheckState(Qt.Checked if bool(row.get("enabled", True)) else Qt.Unchecked)
                self.trusted_hash_table.setItem(idx, 0, use_item)

                algo = str(row.get("algorithm", "") or "").strip().upper()
                if algo == "SHA1":
                    algo = "SHA-1"
                elif algo == "SHA256":
                    algo = "SHA-256"
                elif algo == "SHA512":
                    algo = "SHA-512"
                algo_item = QTableWidgetItem(algo)
                algo_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.trusted_hash_table.setItem(idx, 1, algo_item)

                hash_item = QTableWidgetItem(str(row.get("hash", "") or ""))
                hash_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.trusted_hash_table.setItem(idx, 2, hash_item)

                label_item = QTableWidgetItem(str(row.get("label", "") or ""))
                label_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable)
                self.trusted_hash_table.setItem(idx, 3, label_item)
        finally:
            self._trusted_hashes_table_loading = False
        self._update_trusted_hash_actions()

    def _update_trusted_hash_actions(self) -> None:
        if hasattr(self, "trusted_hash_remove_btn") and hasattr(self, "trusted_hash_table"):
            self.trusted_hash_remove_btn.setEnabled(bool(self.trusted_hash_table.selectionModel().selectedRows()))

    def _on_trusted_hash_table_item_changed(self, item: QTableWidgetItem) -> None:
        if self._trusted_hashes_table_loading:
            return
        if item is None:
            return
        row_idx = int(item.row())
        if row_idx < 0 or row_idx >= len(self._trusted_hash_entries):
            return
        if item.column() == 0:
            self._trusted_hash_entries[row_idx]["enabled"] = bool(item.checkState() == Qt.Checked)
            self._mark_settings_dirty()
            self._refresh_section_titles()
            return
        if item.column() == 3:
            self._trusted_hash_entries[row_idx]["label"] = str(item.text() or "").strip()
            self._mark_settings_dirty()
            self._refresh_section_titles()

    def _add_trusted_hash_entry(self) -> None:
        raw_hash = self.trusted_hash_edit.text().strip() if hasattr(self, "trusted_hash_edit") else ""
        if not raw_hash:
            return
        algo = self._selected_hash_algo()
        label = self.trusted_hash_label_edit.text().strip() if hasattr(self, "trusted_hash_label_edit") else ""
        entry = self._normalize_single_hash_entry(raw_hash, algorithm=algo, label=label, enabled=True)
        if not entry:
            QMessageBox.warning(
                self,
                "Trusted Hash",
                "Invalid hash value. Supported lengths are MD5, SHA-1, SHA-256, and SHA-512.",
            )
            return
        key = (str(entry.get("algorithm", "")), str(entry.get("hash", "")))
        existing_keys = {
            (str(row.get("algorithm", "")), str(row.get("hash", "")))
            for row in self._trusted_hash_entries
            if isinstance(row, dict)
        }
        if key in existing_keys:
            QMessageBox.information(self, "Trusted Hash", "That hash is already stored.")
            return
        self._trusted_hash_entries.append(entry)
        self._refresh_trusted_hash_table()
        self._mark_settings_dirty()
        self._refresh_section_titles()
        self.trusted_hash_edit.clear()
        if hasattr(self, "trusted_hash_label_edit"):
            self.trusted_hash_label_edit.clear()

    @staticmethod
    def _extract_hash_candidates_from_text(text: str) -> List[dict]:
        out: List[dict] = []
        for raw_line in str(text or "").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            algo = ""
            m = re.search(r"(?i)\b(sha-?1|sha-?256|sha-?512|md5)\b", line)
            if m:
                algo = normalize_hash_algorithm(m.group(1))
            hm = re.search(r"\b([A-Fa-f0-9]{32}|[A-Fa-f0-9]{40}|[A-Fa-f0-9]{64}|[A-Fa-f0-9]{128})\b", line)
            if not hm:
                continue
            hash_norm = normalize_hash_hex(hm.group(1))
            if not hash_norm:
                continue
            if not algo:
                algo = infer_algorithm_from_hash(hash_norm)
            if not algo:
                continue
            out.append({"enabled": True, "algorithm": algo, "hash": hash_norm, "label": ""})
        return out

    def _import_trusted_hash_file(self) -> None:
        fn, _ = QFileDialog.getOpenFileName(
            self,
            "Import Trusted Hashes",
            "",
            "Text Files (*.txt *.sha1 *.sha256 *.sha512 *.md5 *.hash *.checksum);;All Files (*)",
        )
        if not fn:
            return
        try:
            text = Path(fn).read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            QMessageBox.warning(self, "Trusted Hash", f"Failed to read file:\n{e}")
            return
        imported = self._extract_hash_candidates_from_text(text)
        if not imported:
            QMessageBox.information(self, "Trusted Hash", "No supported hashes found in that file.")
            return
        existing_keys = {
            (str(row.get("algorithm", "")), str(row.get("hash", "")))
            for row in self._trusted_hash_entries
            if isinstance(row, dict)
        }
        added = 0
        for row in imported:
            key = (str(row.get("algorithm", "")), str(row.get("hash", "")))
            if key in existing_keys:
                continue
            self._trusted_hash_entries.append(row)
            existing_keys.add(key)
            added += 1
        self._refresh_trusted_hash_table()
        if added > 0:
            self._mark_settings_dirty()
            self._refresh_section_titles()
            QMessageBox.information(self, "Trusted Hash", f"Imported {added} hash entr{'y' if added == 1 else 'ies'}.")
        else:
            QMessageBox.information(self, "Trusted Hash", "All hashes from file are already stored.")

    def _remove_selected_trusted_hash_entries(self) -> None:
        if not hasattr(self, "trusted_hash_table"):
            return
        rows = sorted({idx.row() for idx in self.trusted_hash_table.selectionModel().selectedRows()}, reverse=True)
        if not rows:
            return
        for row_idx in rows:
            if 0 <= row_idx < len(self._trusted_hash_entries):
                self._trusted_hash_entries.pop(row_idx)
        self._refresh_trusted_hash_table()
        self._mark_settings_dirty()
        self._refresh_section_titles()

    # ---------- JS8 DIRECTED PATH ---------- #

    def _choose_js8_directed_path(self):
        fn, _ = QFileDialog.getOpenFileName(
            self,
            "Select JS8Call DIRECTED.TXT",
            "",
            "All Files (*);;Text Files (*.txt)",
        )
        if not fn:
            return

        path = Path(fn)
        if not path.exists():
            QMessageBox.warning(self, "Invalid Path", "Selected file does not exist.")
            return

        if path.name.lower() != "directed.txt":
            resp = QMessageBox.question(
                self,
                "Confirm",
                "The selected file is not DIRECTED.TXT. Use it anyway?",
            )
            if resp != QMessageBox.Yes:
                return

        self.js8_directed_edit.setText(str(path))
        log.info("JS8Call DIRECTED.TXT path staged for selected radio: %s", path)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    # ---------- JS8 FORMS PATH ---------- #

    def _choose_js8_forms_path(self):
        """
        Prompt for JS8Spotter forms folder (MCF###.txt files).
        """
        fn = QFileDialog.getExistingDirectory(
            self,
            "Select JS8Spotter forms folder",
            "",
        )
        if not fn:
            return
        self.js8_forms_edit.setText(fn)
        log.info("JS8Spotter forms path staged for selected radio: %s", fn)
        self._refresh_spotter_form_mapper()
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_js8call_install_path(self):
        start = self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select JS8Call install folder", start)
        if not fn:
            return
        self.js8call_path_edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_js8spotter_launch_path(self):
        start = self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else ""
        fn, _ = QFileDialog.getOpenFileName(self, "Select JS8Spotter launch path", start)
        if not fn:
            return
        self.js8spotter_path_edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_commstat_launch_path(self):
        start = self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else ""
        fn, _ = QFileDialog.getOpenFileName(self, "Select CommStat launch path", start)
        if not fn:
            return
        self.commstat_path_edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_msg_path(self, origin: str, edit: QLineEdit):
        """
        Prompt for message paths used by Message Viewer (VarAC/FLMSG/FLAMP).
        """
        fn = QFileDialog.getExistingDirectory(self, f"Select {origin.upper()} folder")
        if not fn:
            return
        edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_varac_install_path(self):
        """
        Prompt for VarAC install folder path.
        """
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC install folder")
        if not fn:
            return
        if hasattr(self, "varac_path_edit"):
            self.varac_path_edit.setText(fn)
        if hasattr(self, "varac_ini_path_edit") and not self.varac_ini_path_edit.text().strip():
            ini_guess = locate_varac_ini_path(fn)
            if ini_guess:
                self.varac_ini_path_edit.setText(ini_guess)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_varac_ini_path(self):
        start = self.varac_ini_path_edit.text().strip() if hasattr(self, "varac_ini_path_edit") else ""
        if not start and hasattr(self, "varac_path_edit"):
            start = self.varac_path_edit.text().strip()
        fn, _ = QFileDialog.getOpenFileName(self, "Select VarAC.ini file", start, "INI Files (*.ini);;All Files (*)")
        if not fn:
            return
        self.varac_ini_path_edit.setText(fn)
        try:
            self._varac_bbs_ini_sync_state = varac_ini_sync_state_to_json(get_varac_ini_sync_state(fn))
        except Exception:
            self._varac_bbs_ini_sync_state = ""
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_varac_outbox_dir(self):
        start = self.varac_outbox_dir_edit.text().strip() if hasattr(self, "varac_outbox_dir_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC Outbox directory", start)
        if not fn:
            return
        self.varac_outbox_dir_edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_varac_bbs_dir(self):
        start = self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC BBS directory", start)
        if not fn:
            return
        self.varac_bbs_dir_edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_varac_bbs_archive_dir(self):
        start = self.varac_bbs_archive_dir_edit.text().strip() if hasattr(self, "varac_bbs_archive_dir_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC BBS archive directory", start)
        if not fn:
            return
        self.varac_bbs_archive_dir_edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_varac_guard_quarantine_dir(self):
        start = (
            self.varac_guard_quarantine_dir_edit.text().strip()
            if hasattr(self, "varac_guard_quarantine_dir_edit")
            else ""
        )
        fn = QFileDialog.getExistingDirectory(self, "Select VGuard quarantine directory", start)
        if not fn:
            return
        self.varac_guard_quarantine_dir_edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _sync_varac_bbs_from_ini(self) -> None:
        ini_path = locate_varac_ini_path(
            self.varac_ini_path_edit.text().strip() if hasattr(self, "varac_ini_path_edit") else "",
            self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else "",
        )
        if not ini_path:
            QMessageBox.warning(self, "VarAC BBS", "Set VarAC INI File or VarAC Install Folder before syncing BBS settings.")
            return
        try:
            bbs_cfg = load_varac_bbs_config(ini_path)
        except Exception as exc:
            QMessageBox.warning(self, "VarAC BBS", f"Could not read VarAC BBS settings:\n{exc}")
            return
        if hasattr(self, "varac_ini_path_edit"):
            self.varac_ini_path_edit.setText(str(bbs_cfg.get("ini_path", "") or ""))
        if hasattr(self, "varac_bbs_enabled_chk"):
            self.varac_bbs_enabled_chk.setChecked(bool(bbs_cfg.get("enable_bbs", False)))
        if hasattr(self, "varac_bbs_limit_access_chk"):
            self.varac_bbs_limit_access_chk.setChecked(bool(bbs_cfg.get("limit_access", False)))
        if hasattr(self, "varac_bbs_announce_chk"):
            self.varac_bbs_announce_chk.setChecked(bool(bbs_cfg.get("announce", False)))
        if hasattr(self, "varac_bbs_callsigns_list"):
            self._set_varac_bbs_allowed_callsigns(bbs_cfg.get("allowed_callsigns", []))
        bbs_dir = varac_path_to_host_path(bbs_cfg.get("bbs_directory", ""), ini_path=ini_path)
        bbs_dir_changed = False
        if bbs_dir and hasattr(self, "varac_bbs_dir_edit"):
            current_bbs_dir = self.varac_bbs_dir_edit.text().strip()
            bbs_dir_changed = current_bbs_dir != bbs_dir
            self.varac_bbs_dir_edit.setText(bbs_dir)
        summary = f"Synced [BBS] from {ini_path}. {bbs_summary_text(bbs_cfg)}."
        if bbs_dir:
            summary += f" BBS Directory applied from INI{' (updated)' if bbs_dir_changed else ''}."
        self.varac_bbs_sync_status_label.setText(summary)
        self.varac_bbs_sync_status_label.setToolTip(summary)
        try:
            self._varac_bbs_ini_sync_state = varac_ini_sync_state_to_json(get_varac_ini_sync_state(ini_path))
        except Exception:
            self._varac_bbs_ini_sync_state = ""
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _sync_varac_bbs_to_ini(self) -> None:
        ini_path = locate_varac_ini_path(
            self.varac_ini_path_edit.text().strip() if hasattr(self, "varac_ini_path_edit") else "",
            self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else "",
        )
        if not ini_path:
            QMessageBox.warning(self, "VarAC BBS", "Set VarAC INI File or VarAC Install Folder before writing BBS settings.")
            return
        try:
            current_state = get_varac_ini_sync_state(ini_path)
        except Exception as exc:
            QMessageBox.warning(self, "VarAC BBS", f"Could not inspect VarAC.ini before saving:\n{exc}")
            return
        if self._varac_bbs_ini_sync_state and not varac_ini_sync_state_matches(self._varac_bbs_ini_sync_state, current_state):
            response = QMessageBox.warning(
                self,
                "VarAC BBS",
                "VarAC.ini changed since the last sync. Reload from disk before overwriting, or continue to overwrite only the [BBS] section?",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
                QMessageBox.Cancel,
            )
            if response == QMessageBox.Cancel:
                return
            if response == QMessageBox.Yes:
                self._sync_varac_bbs_from_ini()
                return
        try:
            updated_state = write_varac_bbs_config(
                ini_path,
                enable_bbs=bool(self.varac_bbs_enabled_chk.isChecked()) if hasattr(self, "varac_bbs_enabled_chk") else False,
                bbs_directory=self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else "",
                limit_access=bool(self.varac_bbs_limit_access_chk.isChecked()) if hasattr(self, "varac_bbs_limit_access_chk") else False,
                allowed_callsigns=self._varac_bbs_selected_callsigns_text(),
                announce=bool(self.varac_bbs_announce_chk.isChecked()) if hasattr(self, "varac_bbs_announce_chk") else False,
                expected_sync_state=current_state,
            )
        except Exception as exc:
            QMessageBox.warning(self, "VarAC BBS", f"Could not write VarAC BBS settings:\n{exc}")
            return
        self._varac_bbs_ini_sync_state = varac_ini_sync_state_to_json(updated_state)
        summary_payload = {
            "enable_bbs": self.varac_bbs_enabled_chk.isChecked() if hasattr(self, "varac_bbs_enabled_chk") else False,
            "limit_access": self.varac_bbs_limit_access_chk.isChecked() if hasattr(self, "varac_bbs_limit_access_chk") else False,
            "announce": self.varac_bbs_announce_chk.isChecked() if hasattr(self, "varac_bbs_announce_chk") else False,
            "allowed_callsigns": self._varac_bbs_selected_callsigns_text(),
        }
        summary = f"Wrote [BBS] to {ini_path}. {bbs_summary_text(summary_payload)}."
        self.varac_bbs_sync_status_label.setText(summary)
        self.varac_bbs_sync_status_label.setToolTip(summary)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_fldigi_checkin_dir(self):
        fn = QFileDialog.getExistingDirectory(self, "Select FLDigi check-in folder")
        if not fn:
            return
        self.fldigi_checkin_dir_edit.setText(fn)
        self._ensure_fldigi_checkin_files()
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _choose_fldigi_log_path(self):
        start = self.fldigi_log_path_edit.text().strip() if hasattr(self, "fldigi_log_path_edit") else ""
        start_dir = str(Path(start)) if start else ""
        fn = QFileDialog.getExistingDirectory(
            self,
            "Select FLDigi log folder",
            start_dir,
        )
        if not fn:
            return
        if hasattr(self, "fldigi_log_path_edit"):
            self.fldigi_log_path_edit.setText(fn)
        self._mark_settings_dirty()
        self._refresh_section_titles()

    def _copy_text(self, edit: QLineEdit) -> None:
        txt = edit.text().strip()
        if not txt:
            return
        cb = QApplication.clipboard()
        cb.setText(txt)
        QMessageBox.information(self, "Copied", f"Copied to clipboard:\n{txt}")
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _refresh_fldigi_checkin_file_labels(self) -> None:
        base = self.fldigi_checkin_dir_edit.text().strip()
        if not base:
            base = str(get_fldigi_checkin_dir())
        main_path = str(Path(base) / "CheckIns_TFC.txt")
        late_path = str(Path(base) / "CheckIns_LATE.txt")
        if hasattr(self, "fldigi_main_file_edit"):
            self.fldigi_main_file_edit.setText(main_path)
        if hasattr(self, "fldigi_late_file_edit"):
            self.fldigi_late_file_edit.setText(late_path)

    def _ensure_fldigi_checkin_files(self) -> None:
        base = self.fldigi_checkin_dir_edit.text().strip()
        if not base:
            base = str(get_fldigi_checkin_dir())
            self.fldigi_checkin_dir_edit.setText(base)
        folder = Path(base)
        main_path = folder / "CheckIns_TFC.txt"
        qru_path = folder / "CheckIns_QRU.txt"
        late_path = folder / "CheckIns_LATE.txt"
        all_path = folder / "CheckIns_ALL.txt"
        try:
            folder.mkdir(parents=True, exist_ok=True)
            if not main_path.exists():
                main_path.touch()
            if not qru_path.exists():
                qru_path.touch()
            if not late_path.exists():
                late_path.touch()
            if not all_path.exists():
                all_path.touch()
        except Exception as e:
            log.error("SettingsTab: failed to ensure FLDigi check-in files: %s", e)
        self._refresh_fldigi_checkin_file_labels()

    def _load_js8_logs(self):
        """
        Manually rebuild JS8 link traffic from DIRECTED.TXT and ALL.TXT.
        This is intentionally a full reload, not incremental.
        """
        self._refresh_operator_history_views()
        directed_path = self.js8_directed_edit.text().strip()
        if not directed_path:
            QMessageBox.warning(self, "Missing path", "Please set JS8Call DIRECTED.TXT path first.")
            return
        path = Path(directed_path)
        if not path.exists():
            QMessageBox.warning(self, "File not found", f"DIRECTED.TXT not found at:\n{path}")
            return
        from freqinout.core.config_paths import get_config_dir

        db_path = get_config_dir() / "config" / "freqinout_nets.db"
        self._set_js8_load_busy(True, "Rebuilding JS8 traffic from logs...")
        self._set_loading(True, "Loading JS8 traffic...")
        try:
            self._set_js8_load_busy(True, "Preparing JS8 traffic rebuild...")
            indexer = JS8LogLinkIndexer(self.settings, db_path)
            indexer._base_callsign = JS8LogLinkIndexer._base_callsign  # ensure suffix handling
            self._maybe_backfill_js8_geo()
            # Force a true full reload so swapped/replaced logs are fully re-read.
            self.settings.set_many(
                {
                    "js8_links_directed_offset": 0,
                    "js8_links_all_offset": 0,
                    "js8_links_last_load_utc": 0,
                }
            )
            self._set_js8_load_busy(True, "Clearing prior JS8 traffic rows...")
            conn = sqlite3.connect(db_path)
            try:
                indexer._ensure_table(conn)
                indexer._clear_table(conn)
            finally:
                conn.close()
            self._set_js8_load_busy(True, "Scanning JS8 logs (this may take a while)...")
            count = int(indexer.update(since_ts=0) or 0)
            latest_ts = float(indexer._ensure_latest_ts(last_default=0.0) or 0.0)
            self._set_js8_load_busy(True, "Finalizing JS8 traffic rebuild...")
            self.settings.set("js8_links_last_load_utc", latest_ts)
            self._set_js8_load_busy(False)
            self._set_loading(False)
            QMessageBox.information(
                self,
                "JS8 Traffic Loaded",
                f"JS8 logs rebuilt successfully ({count} link rows loaded).",
            )
            self._refresh_operator_history_views()
        except Exception as e:
            log.error("SettingsTab: JS8 log ingest failed: %s", e)
            self._set_js8_load_busy(False)
            self._set_loading(False)
            QMessageBox.critical(self, "Error", f"Failed to ingest JS8 logs:\n{e}")
            self._refresh_operator_history_views()
        finally:
            self._set_js8_load_busy(False)
            self._set_loading(False)

    def _maybe_backfill_js8_geo(self) -> None:
        if self._loading_settings:
            return
        if self.settings.get("js8_geo_backfill_v1_done", False):
            return
        directed_path = (self.js8_directed_edit.text().strip() or self.settings.get("js8_directed_path", "") or "")
        if not directed_path:
            return
        path = Path(directed_path)
        if not path.exists():
            return
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            indexer = JS8LogLinkIndexer(self.settings, db_path)
            indexer._base_callsign = JS8LogLinkIndexer._base_callsign  # ensure suffix handling
            scanned = indexer.backfill_geo_from_logs()
            self.settings.set("js8_geo_backfill_v1_done", True)
            log.info("SettingsTab: JS8 geo backfill complete (lines=%s).", scanned)
        except Exception as e:
            log.debug("SettingsTab: JS8 geo backfill failed: %s", e)
