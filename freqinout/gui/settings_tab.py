from __future__ import annotations

import datetime
import platform
import subprocess
import sqlite3
import os
import sys
import time
import zipfile
import re
from pathlib import Path
from typing import Dict, Optional, List, Tuple

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
    QFrame,
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
    QAbstractItemView,
    QAbstractScrollArea,
    QScrollArea,
    QCompleter,
    QToolButton,
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
from freqinout.core.local_ops_store import get_all_operators as get_local_operators
from freqinout.core.config_paths import get_fldigi_checkin_dir, get_config_dir
from freqinout.core.system_timezone import detect_system_timezone_name
from freqinout.core.launch_orchestrator import LaunchOrchestrator, LAUNCH_APP_ORDER
from freqinout.core.software_path_detector import SoftwarePathDetector, PathDetectionResult
from freqinout.core.station_readiness import (
    build_station_readiness_report,
    format_readiness_issue,
    readiness_report_detail_text,
    readiness_report_overall_text,
    readiness_state_card_level,
    readiness_state_description,
    readiness_state_label,
    visible_status_programs,
)
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.varac_bbs_config import (
    bbs_summary_text,
    format_callsign_list,
    get_varac_ini_sync_state,
    load_varac_bbs_config,
    locate_varac_ini_path,
    parse_callsign_list,
    varac_ini_sync_state_matches,
    varac_ini_sync_state_to_json,
    write_varac_bbs_config,
)
from freqinout.core.varac_bbs_vault import (
    DEFAULT_ACCESS_CODE_ITERATIONS,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_FAILED_ATTEMPT_LIMIT,
    DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS,
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
    vault_locations_to_data,
    vault_runtime_state_to_data,
)
from freqinout.core.gpg_tools import (
    gpg_available,
    import_public_key_file,
    import_public_key_text,
    list_public_keys,
    local_sign_key,
    normalize_fingerprint,
)
from freqinout.core.hash_tools import (
    infer_algorithm_from_hash,
    normalize_hash_algorithm,
    normalize_hash_hex,
    normalize_trusted_hash_entries,
)
from freqinout.core.mode_utils import normalize_operating_group_mode, voice_sideband_for_band
from freqinout.utils.timezones import get_timezone
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer
from freqinout.gui.help_registry import resolve_help_host
from freqinout.gui.theme import (
    resolve_theme,
    normalize_ui_text_size,
    led_style,
    button_style,
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


TIMEZONE_CHOICES = [
    "UTC",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
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
    local_net_profiles_changed = Signal()
    open_logs_requested = Signal()
    log_level_changed = Signal(str)
    SECTION_HEALTH_STATE_ROLE = int(Qt.UserRole) + 1
    SECTION_HEALTH_KEY_ROLE = int(Qt.UserRole) + 2

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self.software_path_detector = SoftwarePathDetector(self.settings)
        self._settings_dirty = False
        self._loading_settings = False
        self._op_group_condition_sync = False
        self._op_group_rows_by_group: Dict[str, List[int]] = {}
        self.loading_label: QLabel | None = None
        self._status_service = SoftwareStatusService(self.settings)
        self.launch_orchestrator = LaunchOrchestrator(self.settings, self)

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
        self.js8_groups_edits: List[QLineEdit] = []
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self.operating_groups: List[Dict[str, str]] = []
        self.local_net_profiles: List[Dict[str, str]] = []
        self._accordion_groups: List[QGroupBox] = []
        self._section_meta: Dict[QGroupBox, Dict[str, object]] = {}
        self._section_nav_items: Dict[QGroupBox, QListWidgetItem] = {}
        self._context_help_buttons: List[QPushButton] = []
        self._custom_tool_items_cache: List[Dict[str, str]] = []
        self._custom_tools_table_loading = False
        self._launch_items_cache: List[Dict[str, object]] = []
        self._launch_visible_names: List[str] = []
        self._launch_table_loading = False
        self._varac_bbs_lookup_rows: List[Dict[str, str]] = []
        self._varac_bbs_lookup_by_callsign: Dict[str, Dict[str, str]] = {}
        self._varac_bbs_vault_locations_cache: List[Dict[str, object]] = []
        self._varac_bbs_vault_selected_location_id = ""
        self._varac_bbs_vault_editor_loading = False
        self._varac_bbs_vault_auto_source_dir = ""
        self._varac_bbs_vault_auto_description = ""
        self._varac_bbs_vault_auto_flamp_relay_dir = ""
        self._varac_bbs_vault_root_loading = False
        self._last_varac_bbs_dir_for_root_sync = ""
        self._gpg_keys_table_loading = False
        self._gpg_keys_loaded = False
        self._gpg_keys_auto_probe_attempted = False
        self._gpg_trusted_fingerprints: set[str] = set()
        self._trusted_hashes_table_loading = False
        self._trusted_hash_entries: List[Dict[str, object]] = []
        self._active = False
        self._last_activation_refresh_ts = 0.0
        self._activation_refresh_interval_sec = 30.0

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
        self.status_timer.setInterval(5000)
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
            and (now_ts - float(getattr(self, "_last_varac_bbs_lookup_reload_ts", 0.0) or 0.0))
            < float(getattr(self, "_varac_bbs_lookup_reload_interval_sec", 20.0) or 20.0)
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
            access_code_iterations = int(row.get("access_code_iterations", DEFAULT_ACCESS_CODE_ITERATIONS) or DEFAULT_ACCESS_CODE_ITERATIONS)
        except Exception:
            access_code_iterations = DEFAULT_ACCESS_CODE_ITERATIONS
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
            self._varac_bbs_vault_selected_location_id = str(normalized[0].get("id", "") or "").strip() if normalized else ""
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
                self.varac_bbs_vault_description_edit.setText(str(selected.get("description", "") or "").strip())
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
                self.varac_bbs_vault_inherit_callsigns_chk.setChecked(bool(selected.get("inherit_global_allowed_callsigns", True)))
            if hasattr(self, "varac_bbs_vault_allowed_callsigns_edit"):
                self.varac_bbs_vault_allowed_callsigns_edit.setText(str(selected.get("allowed_callsigns", "") or "").strip())
            if hasattr(self, "varac_bbs_vault_access_code_edit"):
                self.varac_bbs_vault_access_code_edit.clear()
            if hasattr(self, "varac_bbs_vault_access_code_confirm_edit"):
                self.varac_bbs_vault_access_code_confirm_edit.clear()
            self._reset_varac_bbs_vault_code_visibility()
        finally:
            self._varac_bbs_vault_editor_loading = False
        self._varac_bbs_vault_auto_description = str(selected.get("description", "") or "").strip()
        self._varac_bbs_vault_auto_source_dir = str(selected.get("source_dir", "") or "").strip()
        self._refresh_varac_bbs_vault_code_ui(selected)
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
        self._refresh_varac_bbs_vault_source_hint()

    def _refresh_varac_bbs_vault_code_ui(self, selected: Optional[Dict[str, object]]) -> None:
        has_code = bool(str((selected or {}).get("access_code_hash", "") or "").strip())
        if hasattr(self, "varac_bbs_vault_code_status_label"):
            self.varac_bbs_vault_code_status_label.setText(
                "Code configured. Stored securely and cannot be viewed; enter a new code to replace it."
                if has_code
                else "No code configured. Enter a code twice to set one for this location."
            )
        if hasattr(self, "varac_bbs_vault_access_code_edit"):
            self.varac_bbs_vault_access_code_edit.setPlaceholderText(
                "Enter a new access code to replace the saved code" if has_code else "Enter a new access code"
            )
            self.varac_bbs_vault_access_code_edit.setToolTip(
                "Access codes are stored as secure hashes and cannot be shown again after save."
            )
        if hasattr(self, "varac_bbs_vault_access_code_confirm_edit"):
            self.varac_bbs_vault_access_code_confirm_edit.setPlaceholderText(
                "Confirm the new access code to replace the saved code"
                if has_code
                else "Confirm the new access code"
            )
            self.varac_bbs_vault_access_code_confirm_edit.setToolTip(
                "Access codes are stored as secure hashes and cannot be shown again after save."
            )

    def _default_varac_bbs_vault_location_description(self, name: object) -> str:
        clean_name = " ".join(str(name or "").strip().split())
        return f"to open {clean_name}" if clean_name else "to open this location"

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
            enabled = bool(hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked())
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
            "alias": (
                normalize_location_alias(
                    self.varac_bbs_vault_alias_edit.text().strip(),
                    current_name,
                )
                if hasattr(self, "varac_bbs_vault_alias_edit")
                else normalize_location_alias("", current_name)
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

    def _save_varac_bbs_vault_location(self) -> bool:
        name = (
            self.varac_bbs_vault_location_name_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_location_name_edit")
            else ""
        )
        alias = (
            normalize_location_alias(self.varac_bbs_vault_alias_edit.text().strip(), name)
            if hasattr(self, "varac_bbs_vault_alias_edit")
            else normalize_location_alias("", name)
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
        if not alias:
            QMessageBox.warning(self, "Managed BBS Vault", "Set a valid alias before saving.")
            return False
        source_path = Path(source_dir).expanduser()
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
        selected = self._selected_varac_bbs_vault_location() or {}
        location_id = str(selected.get("id", "") or "").strip()
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
        requires_code = (
            location_id != DEFAULT_LOCATION_ID
            and (
                open_rule == "Allowed callsigns + access code"
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
        if access_code or confirm_code:
            if access_code != confirm_code:
                QMessageBox.warning(self, "Managed BBS Vault", "Access code confirmation does not match.")
                return False
            code_payload = hash_access_code(access_code)
        else:
            code_payload = {
                "access_code_hash": str(selected.get("access_code_hash", "") or "").strip(),
                "access_code_salt": str(selected.get("access_code_salt", "") or "").strip(),
                "access_code_iterations": int(selected.get("access_code_iterations", DEFAULT_ACCESS_CODE_ITERATIONS) or DEFAULT_ACCESS_CODE_ITERATIONS),
            }
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
            "access_code_hash": str(code_payload.get("access_code_hash", "") or "").strip(),
            "access_code_salt": str(code_payload.get("access_code_salt", "") or "").strip(),
            "access_code_iterations": int(code_payload.get("access_code_iterations", DEFAULT_ACCESS_CODE_ITERATIONS) or DEFAULT_ACCESS_CODE_ITERATIONS),
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
        if hasattr(self, "varac_bbs_vault_default_location_combo") and self.varac_bbs_vault_default_location_combo.count() == 0:
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
        summary = ""
        if hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked():
            root_txt = self.varac_bbs_vault_root_edit.text().strip() if hasattr(self, "varac_bbs_vault_root_edit") else ""
            state = load_vault_runtime_state(self.settings.get("varac_bbs_vault_runtime_state_v1", {}))
            cached_summary = str(self.settings.get("varac_bbs_vault_last_summary", "") or "").strip()
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
            summary = (
                cached_summary
                or f"Managed Vault ready for {current_name or DEFAULT_LOCATION_NAME}."
            )
            if root_txt:
                summary += f" Root: {root_txt}"
        else:
            summary = "Managed Vault is not enabled for this station."
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
                "alias": normalize_location_alias("ROOT", DEFAULT_LOCATION_NAME),
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
            default_row["alias"] = normalize_location_alias(default_row.get("alias", "ROOT"), DEFAULT_LOCATION_NAME)
            default_row["description"] = str(default_row.get("description", "Main menu") or "Main menu").strip()
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
        if hasattr(self.settings, "set_many"):
            self.settings.set_many(
                {
                    "varac_bbs_vault_runtime_state_v1": vault_runtime_state_to_data(runtime_state),
                    "varac_bbs_vault_last_summary": runtime_state.last_action,
                },
                save=True,
            )
        else:
            self.settings.set("varac_bbs_vault_runtime_state_v1", vault_runtime_state_to_data(runtime_state))
            self.settings.set("varac_bbs_vault_last_summary", runtime_state.last_action)
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
        runtime_state = load_vault_runtime_state(self.settings.get("varac_bbs_vault_runtime_state_v1", {}))
        try:
            result = reset_to_default_location(
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=root_txt,
                default_location_id=default_id,
                runtime_state=runtime_state,
                global_allowed_callsigns=parse_callsign_list(self._varac_bbs_selected_callsigns_text()),
                limit_access_enabled=(
                    self.varac_bbs_limit_access_chk.isChecked()
                    if hasattr(self, "varac_bbs_limit_access_chk")
                    else False
                ),
                global_code_policy=(
                    self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
                    if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
                    else DEFAULT_GLOBAL_CODE_POLICY
                ),
                flamp_enabled=(
                    self.varac_bbs_vault_flamp_enabled_chk.isChecked()
                    if hasattr(self, "varac_bbs_vault_flamp_enabled_chk")
                    else False
                ),
                reason="manual_reset",
            )
        except Exception as exc:
            QMessageBox.warning(self, "Managed BBS Vault", f"Could not reset to default:\n{exc}")
            return
        if hasattr(self.settings, "set_many"):
            self.settings.set_many(
                {
                    "varac_bbs_vault_runtime_state_v1": vault_runtime_state_to_data(result.runtime_state),
                    "varac_bbs_vault_last_summary": result.summary,
                },
                save=True,
            )
        else:
            self.settings.set("varac_bbs_vault_runtime_state_v1", vault_runtime_state_to_data(result.runtime_state))
            self.settings.set("varac_bbs_vault_last_summary", result.summary)
        self._refresh_varac_bbs_vault_status_label()
        self._mark_settings_dirty()
        self._refresh_section_titles()
        publish_result = result.publish_result
        detail_lines = [
            result.summary,
            "",
            f"Live BBS: {live_bbs_dir or '(not configured)'}",
        ]
        if publish_result is not None:
            detail_lines.extend(
                [
                    f"Published/updated: {int(publish_result.published_count or 0)}",
                    f"Removed stale projected files: {int(publish_result.removed_count or 0)}",
                    f"Manifest: {publish_result.manifest_path or '(not written)'}",
                ]
            )
            if publish_result.unmanaged_live_files:
                preview = ", ".join(list(publish_result.unmanaged_live_files)[:5])
                extra = len(publish_result.unmanaged_live_files) - 5
                if extra > 0:
                    preview += f" +{extra} more"
                detail_lines.append(f"Unmanaged live files left in place: {preview}")
        QMessageBox.information(
            self,
            "Managed BBS Vault",
            "\n".join(detail_lines),
        )

    def _settings_snapshot_for_readiness(self) -> Dict[str, object]:
        message_paths: Dict[str, str] = {}
        for origin, edit in getattr(self, "msg_paths_edits", {}).items():
            try:
                message_paths[str(origin)] = edit.text().strip()
            except Exception:
                message_paths[str(origin)] = ""
        return {
            "callsign": self.callsign_edit.text().strip() if hasattr(self, "callsign_edit") else "",
            "operator_callsign": self.callsign_edit.text().strip() if hasattr(self, "callsign_edit") else "",
            "grid": self.grid6_edit.text().strip() if hasattr(self, "grid6_edit") else "",
            "operator_grid6": self.grid6_edit.text().strip() if hasattr(self, "grid6_edit") else "",
            "use_flrig": bool(hasattr(self, "use_flrig_chk") and self.use_flrig_chk.isChecked()),
            "use_fldigi": bool(hasattr(self, "use_fldigi_chk") and self.use_fldigi_chk.isChecked()),
            "use_flmsg": bool(hasattr(self, "use_flmsg_chk") and self.use_flmsg_chk.isChecked()),
            "use_flamp": bool(hasattr(self, "use_flamp_chk") and self.use_flamp_chk.isChecked()),
            "use_js8call": bool(hasattr(self, "use_js8call_chk") and self.use_js8call_chk.isChecked()),
            "use_js8spotter": bool(hasattr(self, "use_js8spotter_chk") and self.use_js8spotter_chk.isChecked()),
            "use_commstat": bool(hasattr(self, "use_commstat_chk") and self.use_commstat_chk.isChecked()),
            "use_varac": bool(hasattr(self, "use_varac_chk") and self.use_varac_chk.isChecked()),
            "freq_enforcement_mode": (
                self.freq_enforce_combo.currentText().strip() if hasattr(self, "freq_enforce_combo") else ""
            ),
            "frequency_enforcement_mode": (
                self.freq_enforce_combo.currentText().strip() if hasattr(self, "freq_enforce_combo") else ""
            ),
            "freq_prompt_interval": (
                self.freq_prompt_combo.currentText().strip() if hasattr(self, "freq_prompt_combo") else ""
            ),
            "frequency_prompt_interval": (
                self.freq_prompt_combo.currentText().strip() if hasattr(self, "freq_prompt_combo") else ""
            ),
            "fldigi_enforcement_mode": (
                self.fldigi_enforce_combo.currentText().strip() if hasattr(self, "fldigi_enforce_combo") else ""
            ),
            "fldigi_prompt_interval": (
                self.fldigi_prompt_combo.currentText().strip() if hasattr(self, "fldigi_prompt_combo") else ""
            ),
            "js8_enforcement_mode": (
                self.js8_enforce_combo.currentText().strip() if hasattr(self, "js8_enforce_combo") else ""
            ),
            "js8_prompt_interval": (
                self.js8_prompt_combo.currentText().strip() if hasattr(self, "js8_prompt_combo") else ""
            ),
            "path_js8call": self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else "",
            "js8_host": self.js8_host_edit.text().strip() if hasattr(self, "js8_host_edit") else "",
            "js8_port": self.js8_port_edit.text().strip() if hasattr(self, "js8_port_edit") else "",
            "js8_directed_path": self.js8_directed_edit.text().strip() if hasattr(self, "js8_directed_edit") else "",
            "js8_forms_path": self.js8_forms_edit.text().strip() if hasattr(self, "js8_forms_edit") else "",
            "path_js8spotter": (
                self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else ""
            ),
            "path_commstat": self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else "",
            "path_flrig": self.path_edits.get("FLRig").text().strip() if self.path_edits.get("FLRig") else "",
            "flrig_port": self.flrig_port_edit.text().strip() if hasattr(self, "flrig_port_edit") else "",
            "path_fldigi": self.path_edits.get("FLDigi").text().strip() if self.path_edits.get("FLDigi") else "",
            "fldigi_host": self.fldigi_host_edit.text().strip() if hasattr(self, "fldigi_host_edit") else "",
            "fldigi_port": self.fldigi_port_edit.text().strip() if hasattr(self, "fldigi_port_edit") else "",
            "fldigi_log_path": self.fldigi_log_path_edit.text().strip() if hasattr(self, "fldigi_log_path_edit") else "",
            "fldigi_checkin_dir": (
                self.fldigi_checkin_dir_edit.text().strip() if hasattr(self, "fldigi_checkin_dir_edit") else ""
            ),
            "default_fldigi_checkin_dir": str(get_fldigi_checkin_dir()),
            "path_flmsg": self.path_edits.get("FLMsg").text().strip() if self.path_edits.get("FLMsg") else "",
            "path_flamp": self.path_edits.get("FLAmp").text().strip() if self.path_edits.get("FLAmp") else "",
            "varac_path": self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else "",
            "varac_launch_cmd": (
                self.varac_launch_cmd_edit.text().strip() if hasattr(self, "varac_launch_cmd_edit") else ""
            ),
            "varac_outbox_dir": (
                self.varac_outbox_dir_edit.text().strip() if hasattr(self, "varac_outbox_dir_edit") else ""
            ),
            "varac_bbs_dir": self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else "",
            "varac_bbs_archive_dir": (
                self.varac_bbs_archive_dir_edit.text().strip() if hasattr(self, "varac_bbs_archive_dir_edit") else ""
            ),
            "varac_bbs_auto_archive": bool(
                hasattr(self, "varac_bbs_auto_archive_chk") and self.varac_bbs_auto_archive_chk.isChecked()
            ),
            "varac_bbs_limit_access": bool(
                hasattr(self, "varac_bbs_limit_access_chk") and self.varac_bbs_limit_access_chk.isChecked()
            ),
            "varac_bbs_allowed_callsigns": self._varac_bbs_selected_callsigns_text(),
            "varac_guard_enabled": bool(
                hasattr(self, "varac_guard_enabled_chk") and self.varac_guard_enabled_chk.isChecked()
            ),
            "varac_guard_mode": (
                self.varac_guard_mode_combo.currentText().strip()
                if hasattr(self, "varac_guard_mode_combo")
                else "Log only"
            ),
            "varac_guard_quarantine_dir": (
                self.varac_guard_quarantine_dir_edit.text().strip()
                if hasattr(self, "varac_guard_quarantine_dir_edit")
                else ""
            ),
            "varac_bbs_vault_enabled": bool(
                hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked()
            ),
            "varac_bbs_vault_managed_root": self._computed_varac_bbs_vault_default_root(),
            "varac_bbs_vault_default_location_id": (
                str(self.varac_bbs_vault_default_location_combo.currentData() or "").strip()
                if hasattr(self, "varac_bbs_vault_default_location_combo")
                else DEFAULT_LOCATION_ID
            ),
            "varac_bbs_vault_global_code_policy": (
                self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
                else DEFAULT_GLOBAL_CODE_POLICY
            ),
            "varac_bbs_vault_return_mode": (
                self.varac_bbs_vault_return_mode_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_return_mode_combo")
                else DEFAULT_RETURN_MODE
            ),
            "varac_bbs_vault_trigger_mode": (
                self.varac_bbs_vault_trigger_mode_combo.currentText().strip()
                if hasattr(self, "varac_bbs_vault_trigger_mode_combo")
                else DEFAULT_TRIGGER_MODE
            ),
            "varac_bbs_vault_flamp_enabled": bool(
                hasattr(self, "varac_bbs_vault_flamp_enabled_chk") and self.varac_bbs_vault_flamp_enabled_chk.isChecked()
            ),
            "varac_bbs_vault_flamp_relay_dir": (
                self.varac_bbs_vault_flamp_relay_dir_edit.text().strip()
                if hasattr(self, "varac_bbs_vault_flamp_relay_dir_edit")
                else ""
            ),
            "varac_bbs_vault_locations_v1": list(self._varac_bbs_vault_locations_cache),
            "varac_bbs_vault_last_summary": str(self.settings.get("varac_bbs_vault_last_summary", "") or "").strip(),
            "message_paths": message_paths,
        }

    def _current_station_readiness_report(self):
        return build_station_readiness_report(
            self._settings_snapshot_for_readiness(),
            operating_groups=self.operating_groups,
        )

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

    def _rebuild_status_indicators(self) -> None:
        if not hasattr(self, "status_layout"):
            return
        self._clear_status_layout(self.status_layout)
        self.status_labels = {}
        self._status_text_labels = {}
        theme = resolve_theme(self.settings)
        visible_items = visible_status_programs(self._settings_snapshot_for_readiness())
        if hasattr(self, "status_group"):
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

    def _readiness_card_styles(self, level: str) -> tuple[str, str]:
        theme = resolve_theme(self.settings)
        border = theme.get("border", "#cccccc")
        bg = theme.get("surface_alt", theme.get("surface", "#f7f7f7"))
        if level == "danger":
            border = theme.get("danger", "#b3261e")
        elif level == "warning":
            border = theme.get("warning", "#c99700")
        elif level == "success":
            border = theme.get("success", theme.get("accent", "#2a6fd3"))
        elif level == "info":
            border = theme.get("accent", "#2a6fd3")
        return border, bg

    def _build_readiness_summary_text(self) -> str:
        report = self._current_station_readiness_report()
        return readiness_report_detail_text(report)

    def _copy_readiness_summary(self) -> None:
        QApplication.clipboard().setText(self._build_readiness_summary_text())
        if hasattr(self, "copy_readiness_summary_btn"):
            self.copy_readiness_summary_btn.setText("Copied")
            QTimer.singleShot(1500, lambda: self.copy_readiness_summary_btn.setText("Copy Readiness Summary"))

    def _update_readiness_summary_card(self) -> None:
        if not hasattr(self, "readiness_summary_card"):
            return
        report = self._current_station_readiness_report()
        self.readiness_summary_card.setVisible(str(report.overall_state or "").strip().lower() != "ready")
        if not self.readiness_summary_card.isVisible():
            return
        first_issue = report.first_actionable_issue()
        detail = f" Next item: {format_readiness_issue(first_issue)}." if first_issue else ""
        self.readiness_summary_status_label.setText(f"{readiness_report_overall_text(report)}{detail}")
        self.readiness_summary_status_label.setToolTip(self._build_readiness_summary_text())
        level = readiness_state_card_level(report.overall_state)
        border, bg = self._readiness_card_styles(level)
        fg = resolve_theme(self.settings).get("text", "#222222")
        self.readiness_summary_card.setStyleSheet(
            "QFrame {"
            f" background: {bg};"
            f" border: 1px solid {border};"
            " border-radius: 6px;"
            "}"
            " QLabel {"
            f" color: {fg};"
            " border: none;"
            " background: transparent;"
            "}"
        )

    # ---------- UI ---------- #

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
        main_layout.addWidget(callsign_group)

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
        self.control_combo.addItems(["FLRig", "JS8Call", "Manual"])
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

        software_used_container = QWidget()
        software_used_layout = QVBoxLayout(software_used_container)
        software_used_layout.setContentsMargins(0, 0, 0, 0)
        software_used_layout.setSpacing(8)
        software_used_header = QHBoxLayout()
        software_used_header.setContentsMargins(0, 0, 0, 0)
        software_used_header.addWidget(QLabel("Software Used"))
        software_used_header.addStretch()
        self.software_used_help_btn = self._make_context_help_button(
            "settings.software_used",
            tooltip="Open help for selecting which software this station actually uses.",
        )
        software_used_header.addWidget(self.software_used_help_btn)
        software_used_layout.addLayout(software_used_header)
        software_used_grid = QGridLayout()
        software_used_grid.setContentsMargins(0, 0, 0, 0)
        software_used_grid.setHorizontalSpacing(24)
        software_used_grid.setVerticalSpacing(10)
        self.use_flrig_chk = QCheckBox("FLRig")
        self.use_fldigi_chk = QCheckBox("FLDigi")
        self.use_flmsg_chk = QCheckBox("FLMsg")
        self.use_flamp_chk = QCheckBox("FLAmp")
        self.use_js8call_chk = QCheckBox("JS8Call")
        self.use_js8spotter_chk = QCheckBox("JS8Spotter")
        self.use_commstat_chk = QCheckBox("CommStat")
        self.use_varac_chk = QCheckBox("VarAC")
        software_used_grid.addWidget(self.use_flrig_chk, 0, 0)
        software_used_grid.addWidget(self.use_fldigi_chk, 0, 1)
        software_used_grid.addWidget(self.use_flmsg_chk, 0, 2)
        software_used_grid.addWidget(self.use_flamp_chk, 0, 3)
        software_used_grid.addWidget(self.use_js8call_chk, 1, 0)
        software_used_grid.addWidget(self.use_js8spotter_chk, 1, 1)
        software_used_grid.addWidget(self.use_commstat_chk, 1, 2)
        software_used_grid.addWidget(self.use_varac_chk, 2, 0)
        software_used_grid.setColumnStretch(4, 1)
        software_used_layout.addLayout(software_used_grid)
        left_column_layout.addWidget(software_used_container)

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
        self.logging_group = QWidget()
        self.logging_group.setToolTip(log_warn_tip)
        logging_group_layout = QVBoxLayout()
        logging_group_layout.setContentsMargins(8, 8, 8, 8)
        logging_group_layout.setSpacing(6)

        self.logging_warning_label = QLabel(
            "Verbose logging can increase disk I/O and reduce performance."
        )
        self.logging_warning_label.setWordWrap(True)
        self.logging_warning_label.setToolTip(log_warn_tip)
        logging_group_layout.addWidget(self.logging_warning_label)

        level_row = QHBoxLayout()
        level_row.addWidget(QLabel("Logging Level:"))
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["DISABLED", "ERROR", "WARNING", "INFO", "DEBUG"])
        self.log_level_combo.setToolTip(log_warn_tip)
        self.log_level_combo.currentTextChanged.connect(self._on_log_level_changed)
        level_row.addWidget(self.log_level_combo)
        level_row.addStretch()
        logging_group_layout.addLayout(level_row)

        timed_row = QHBoxLayout()
        self.enable_timed_debug_btn = QPushButton("Enable DEBUG For")
        self.enable_timed_debug_btn.setToolTip(log_warn_tip)
        self.enable_timed_debug_btn.clicked.connect(self._enable_timed_debug)
        timed_row.addWidget(self.enable_timed_debug_btn)

        self.debug_duration_combo = QComboBox()
        self.debug_duration_combo.addItem("15 min", 15)
        self.debug_duration_combo.addItem("30 min", 30)
        self.debug_duration_combo.addItem("60 min", 60)
        self.debug_duration_combo.setCurrentIndex(1)
        self.debug_duration_combo.setToolTip("Automatically reverts to previous logging level when timer expires.")
        timed_row.addWidget(self.debug_duration_combo)
        timed_row.addStretch()
        logging_group_layout.addLayout(timed_row)

        self.logging_actions_grid = QGridLayout()
        self.logging_actions_grid.setHorizontalSpacing(8)
        self.logging_actions_grid.setVerticalSpacing(6)

        self.open_logs_btn = QPushButton("Open Logs")
        self.open_logs_btn.setToolTip(log_warn_tip)
        self.open_logs_btn.clicked.connect(self._request_open_logs)
        self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0)

        self.open_log_folder_btn = QPushButton("Open Log Folder")
        self.open_log_folder_btn.setToolTip(log_warn_tip)
        self.open_log_folder_btn.clicked.connect(self._open_log_folder)
        self.logging_actions_grid.addWidget(self.open_log_folder_btn, 0, 1)

        self.export_diag_btn = QPushButton("Export Diagnostics")
        self.export_diag_btn.setToolTip(log_warn_tip)
        self.export_diag_btn.clicked.connect(self._export_diagnostics)
        self.logging_actions_grid.addWidget(self.export_diag_btn, 0, 2)
        self.logging_actions_grid.setColumnStretch(3, 1)
        logging_group_layout.addLayout(self.logging_actions_grid)

        self.logging_group.setLayout(logging_group_layout)

        left_widget = QWidget()
        left_widget.setLayout(left_column_layout)
        left_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        op_layout.addWidget(left_widget)

        self._align_enforcement_labels()
        self._update_logging_actions_layout()

        # Operating status indicators
        status_container = QWidget()
        self.status_layout = QHBoxLayout()
        status_container.setLayout(self.status_layout)
        self.status_group = QGroupBox("Operating Status")
        status_group_layout = QVBoxLayout()
        status_group_layout.setContentsMargins(10, 10, 10, 12)
        status_group_layout.setSpacing(6)
        status_group_layout.addWidget(status_container)
        self.status_group.setLayout(status_group_layout)
        self.status_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._rebuild_status_indicators()
        main_layout.addWidget(self.status_group)

        self.readiness_summary_card = QFrame()
        self.readiness_summary_card.setFrameShape(QFrame.StyledPanel)
        readiness_layout = QVBoxLayout(self.readiness_summary_card)
        readiness_layout.setContentsMargins(12, 10, 12, 10)
        readiness_layout.setSpacing(6)
        readiness_title = QLabel("Setup Readiness")
        readiness_title_font = readiness_title.font()
        readiness_title_font.setBold(True)
        readiness_title.setFont(readiness_title_font)
        readiness_layout.addWidget(readiness_title)
        self.readiness_summary_status_label = QLabel("Reviewing current setup...")
        self.readiness_summary_status_label.setWordWrap(True)
        readiness_layout.addWidget(self.readiness_summary_status_label)
        readiness_actions = QHBoxLayout()
        readiness_actions.setContentsMargins(0, 0, 0, 0)
        readiness_actions.addStretch()
        self.copy_readiness_summary_btn = QPushButton("Copy Readiness Summary")
        self.copy_readiness_summary_btn.clicked.connect(self._copy_readiness_summary)
        readiness_actions.addWidget(self.copy_readiness_summary_btn)
        readiness_layout.addLayout(readiness_actions)
        main_layout.addWidget(self.readiness_summary_card)

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
        sections_row.addWidget(self.sections_nav_list, 0, Qt.AlignTop)

        self.sections_stack = QStackedWidget()
        self.sections_scroll = QScrollArea()
        self.sections_scroll.setWidgetResizable(True)
        self.sections_scroll.setFrameShape(QFrame.NoFrame)
        self.sections_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.sections_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.sections_scroll.setWidget(self.sections_stack)
        sections_row.addWidget(self.sections_scroll, 1)
        main_layout.addLayout(sections_row, 1)

        op_container = QWidget()
        op_container.setLayout(op_layout)
        op_group = self._make_collapsible_group(
            "FreqInOut Settings",
            op_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.freqinout",
        )
        self._register_collapsible_group(op_group, self._summary_freqinout_settings)
        self._set_section_health_key(op_group, "freqinout")
        op_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(op_group)

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
        logging_section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # HF Operating Groups panel
        ops_group = QGroupBox("HF Operating Groups")
        ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
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
        self.op_groups_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.op_groups_table.setEditTriggers(QTableWidget.NoEditTriggers)
        ops_layout.addWidget(self.op_groups_table)
        ops_container = QWidget()
        ops_container.setLayout(ops_layout)
        ops_group = self._make_collapsible_group(
            "HF Operating Groups",
            ops_container,
            checked=True,
            fit_content=False,
            help_context_key="settings.hf-groups",
        )
        self._register_collapsible_group(ops_group, self._summary_operating_groups)
        self._set_section_health_key(ops_group, "operating_groups")
        ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(ops_group)

        # Local Comms Groups panel (non-scheduler local net metadata for SOP workflows)
        local_group = QGroupBox("Local Comms Groups")
        local_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
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
        self.local_net_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        local_layout.addWidget(self.local_net_table)
        local_container = QWidget()
        local_container.setLayout(local_layout)
        local_group = self._make_collapsible_group(
            "Local Comms Groups",
            local_container,
            checked=True,
            fit_content=False,
            help_context_key="settings.local-comms",
        )
        self._register_collapsible_group(local_group, self._summary_local_net_profiles)
        local_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(local_group)

        # JS8Call status/settings
        js8_group = QGroupBox("JS8Call Settings")
        js8_v = QVBoxLayout()
        js8_v.setSpacing(6)
        js8_v.setAlignment(Qt.AlignTop)
        js8_group.setLayout(js8_v)
        js8_label_width = 170

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
        self.js8_offset_edit.setText("0")
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

        def build_js8_path_row(label: str, edit: QLineEdit, browse_cb) -> QWidget:
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
            w = QWidget()
            w.setLayout(row)
            w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            return w

        self.js8call_path_edit = QLineEdit()
        self.js8call_path_edit.setPlaceholderText("Folder containing JS8Call")
        js8_v.addWidget(
            build_js8_path_row("JS8Call Install Folder:", self.js8call_path_edit, self._choose_js8call_install_path)
        )

        self.js8_directed_edit = QLineEdit()
        js8_v.addWidget(
            build_js8_path_row("JS8Call DIRECTED.TXT:", self.js8_directed_edit, self._choose_js8_directed_path)
        )

        self.js8_forms_edit = QLineEdit()
        js8_v.addWidget(
            build_js8_path_row("JS8Spotter forms:", self.js8_forms_edit, self._choose_js8_forms_path)
        )

        self.js8spotter_path_edit = QLineEdit()
        self.js8spotter_path_edit.setPlaceholderText("Executable/script/.desktop path")
        js8_v.addWidget(
            build_js8_path_row(
                "JS8Spotter Script/Launcher:", self.js8spotter_path_edit, self._choose_js8spotter_launch_path
            )
        )

        self.commstat_path_edit = QLineEdit()
        self.commstat_path_edit.setPlaceholderText("Executable/script/.desktop path")
        js8_v.addWidget(
            build_js8_path_row(
                "CommStat Script/Launcher:", self.commstat_path_edit, self._choose_commstat_launch_path
            )
        )

        self.js8_directed_edit.textChanged.connect(self._refresh_section_titles)
        self.js8_forms_edit.textChanged.connect(self._refresh_section_titles)
        self.js8call_path_edit.textChanged.connect(self._refresh_section_titles)
        self.js8spotter_path_edit.textChanged.connect(self._refresh_section_titles)
        self.commstat_path_edit.textChanged.connect(self._refresh_section_titles)
        self.js8call_path_edit.textChanged.connect(self._on_launch_paths_changed)
        self.js8spotter_path_edit.textChanged.connect(self._on_launch_paths_changed)
        self.commstat_path_edit.textChanged.connect(self._on_launch_paths_changed)

        js8_autofill_row = QHBoxLayout()
        js8_autofill_row.setSpacing(8)
        js8_autofill_row.setContentsMargins(0, 0, 0, 0)
        js8_autofill_label = QLabel("Auto-Fill")
        js8_autofill_label.setFixedWidth(js8_label_width)
        js8_autofill_row.addWidget(js8_autofill_label)
        self.js8_autofill_btn = QPushButton("Attempt Auto-Fill")
        self.js8_autofill_btn.clicked.connect(self._attempt_js8_autofill)
        js8_autofill_row.addWidget(self.js8_autofill_btn)
        js8_autofill_row.addStretch()
        js8_v.addLayout(js8_autofill_row)

        js8_autofill_status_row = QHBoxLayout()
        js8_autofill_status_row.setSpacing(8)
        js8_autofill_status_row.setContentsMargins(0, 0, 0, 0)
        js8_autofill_status_row.addSpacing(js8_label_width)
        self.js8_autofill_status_label = QLabel("No auto-fill attempt yet.")
        self.js8_autofill_status_label.setWordWrap(True)
        self._autofill_status_labels["js8"] = self.js8_autofill_status_label
        js8_autofill_status_row.addWidget(self.js8_autofill_status_label, 1)
        js8_v.addLayout(js8_autofill_status_row)

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
        self._add_settings_section(js8_group)

        msg_label_width = 170

        def build_prog_row(name: str, label: str | None = None) -> QWidget:
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
        fast_light_v.addWidget(build_prog_row("FLRig", "FLRig"))

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

        fast_light_v.addWidget(build_prog_row("FLDigi", "FLDigi"))
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

        fast_light_v.addWidget(build_prog_row("FLMsg", "FLMsg"))
        fast_light_v.addWidget(
            build_msg_row(
                "ICS/Messages",
                flmsg_edit,
                lambda: self._choose_msg_path("flmsg", flmsg_edit),
            )
        )

        fast_light_v.addWidget(build_prog_row("FLAmp", "FLAmp"))
        fast_light_v.addWidget(
            build_msg_row(
                "FLAMP/rx",
                flamp_edit,
                lambda: self._choose_msg_path("flamp", flamp_edit),
            )
        )

        fast_light_autofill_row = QHBoxLayout()
        fast_light_autofill_row.setContentsMargins(0, 0, 0, 0)
        fast_light_autofill_row.setSpacing(8)
        fast_light_autofill_label = QLabel("Auto-Fill")
        fast_light_autofill_label.setFixedWidth(msg_label_width)
        fast_light_autofill_row.addWidget(fast_light_autofill_label)
        self.fast_light_autofill_btn = QPushButton("Attempt Auto-Fill")
        self.fast_light_autofill_btn.clicked.connect(self._attempt_fast_light_autofill)
        fast_light_autofill_row.addWidget(self.fast_light_autofill_btn)
        fast_light_autofill_row.addStretch()
        fast_light_v.addLayout(fast_light_autofill_row)

        fast_light_autofill_status_row = QHBoxLayout()
        fast_light_autofill_status_row.setContentsMargins(0, 0, 0, 0)
        fast_light_autofill_status_row.setSpacing(8)
        fast_light_autofill_status_row.addSpacing(msg_label_width)
        self.fast_light_autofill_status_label = QLabel("No auto-fill attempt yet.")
        self.fast_light_autofill_status_label.setWordWrap(True)
        self._autofill_status_labels["fast_light"] = self.fast_light_autofill_status_label
        fast_light_autofill_status_row.addWidget(self.fast_light_autofill_status_label, 1)
        fast_light_v.addLayout(fast_light_autofill_status_row)

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
        self._add_settings_section(fast_light_group)

        # Message Authenticity (Key/Hash)
        gpg_group = QGroupBox("Message Auth (Key/Hash)")
        gpg_v = QVBoxLayout()
        gpg_v.setContentsMargins(0, 0, 0, 0)
        gpg_v.setSpacing(4)
        gpg_v.setAlignment(Qt.AlignTop)
        gpg_group.setLayout(gpg_v)

        self.gpg_verify_enabled_chk = QCheckBox("Verify signed .k2s/.b2s message files and signature sidecars")
        self.gpg_verify_enabled_chk.setToolTip(
            "When enabled, Message Viewer verifies detached sidecars and embedded clearsigned content "
            "for FLAmp, VarAC, and BBS .k2s/.b2s files, canonical '-sig' files, and .sig/.asc/.gpg sidecars."
        )
        gpg_v.addWidget(self.gpg_verify_enabled_chk)

        self.hash_verify_enabled_chk = QCheckBox(
            "Verify .k2s/.b2s checksum sidecars (SHA-256/SHA-512 preferred)"
        )
        self.hash_verify_enabled_chk.setToolTip(
            "When enabled, Message Viewer checks checksum sidecar files for tamper/corruption detection."
        )
        gpg_v.addWidget(self.hash_verify_enabled_chk)

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
        gpg_v.addLayout(trusted_hash_row)

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
        gpg_v.addLayout(trusted_hash_actions)

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
        self.trusted_hash_table.setMinimumHeight(58)
        self.trusted_hash_table.setMaximumHeight(72)
        self.trusted_hash_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        gpg_v.addWidget(self.trusted_hash_table)

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
        gpg_v.addLayout(gpg_path_row)

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
        gpg_v.addLayout(gpg_action_row)

        gpg_status_row = QHBoxLayout()
        gpg_status_row.setContentsMargins(0, 0, 0, 0)
        gpg_status_row.setSpacing(8)
        gpg_status_spacer = QLabel("")
        gpg_status_spacer.setFixedWidth(msg_label_width)
        gpg_status_row.addWidget(gpg_status_spacer)
        self.gpg_status_label = QLabel("GPG status: not checked")
        self.gpg_status_label.setWordWrap(True)
        self.gpg_status_label.setMaximumHeight(40)
        gpg_status_row.addWidget(self.gpg_status_label, 1)
        gpg_v.addLayout(gpg_status_row)

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
        self.gpg_keys_table.setMinimumHeight(64)
        self.gpg_keys_table.setMaximumHeight(84)
        self.gpg_keys_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        gpg_v.addWidget(self.gpg_keys_table)

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

        gpg_container = QWidget()
        gpg_container.setLayout(gpg_v)
        gpg_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        gpg_group = self._make_collapsible_group(
            "Message Auth (Key/Hash)",
            gpg_container,
            checked=True,
            fit_content=True,
            help_context_key="settings.message-auth",
        )
        self._register_collapsible_group(gpg_group, self._summary_gpg_settings)
        self._section_meta[gpg_group]["fit_content_in_stack"] = True
        self._apply_collapsed_state(gpg_group, gpg_container, True)

        # VarAC Settings
        varac_group = QGroupBox("VarAC Settings")
        varac_v = QVBoxLayout()
        varac_v.setSpacing(6)
        varac_v.setAlignment(Qt.AlignTop)
        varac_group.setLayout(varac_v)

        def _make_varac_subgroup(title: str, description: str = "") -> QVBoxLayout:
            group = QGroupBox(title)
            layout = QVBoxLayout()
            layout.setContentsMargins(8, 10, 8, 10)
            layout.setSpacing(6)
            if description:
                hint = QLabel(description)
                hint.setWordWrap(True)
                layout.addWidget(hint)
            group.setLayout(layout)
            varac_v.addWidget(group)
            return layout

        varac_paths_v = _make_varac_subgroup(
            "VarAC Paths and Launch",
            "Configure the VarAC application location, launch behavior, and the folders FreqInOut uses for file exchange.",
        )
        bbs_settings_v = _make_varac_subgroup(
            "BBS Settings",
            "These settings control the live VarAC BBS folder, access policy, archive behavior, and allowed callsigns.",
        )
        vault_guard_v = _make_varac_subgroup(
            "Vault / VGuard Settings",
            "Managed BBS Vault publishes one controlled view into the live VarAC BBS while keeping source folders in a managed root. "
            "VGuard is separate and watches inbound transfers for unauthorized senders or unsafe handling paths.",
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
        self.varac_ini_path_edit.setPlaceholderText("VarAC.ini path")
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

        varac_paths_v.addWidget(QLabel("Auto-Fill"))
        varac_autofill_row = QHBoxLayout()
        varac_autofill_row.setContentsMargins(0, 0, 0, 0)
        varac_autofill_row.setSpacing(8)
        self.varac_autofill_btn = QPushButton("Attempt Auto-Fill")
        self.varac_autofill_btn.clicked.connect(self._attempt_varac_autofill)
        varac_autofill_row.addWidget(self.varac_autofill_btn)
        varac_autofill_row.addStretch()
        varac_paths_v.addLayout(varac_autofill_row)

        varac_autofill_status_row = QHBoxLayout()
        varac_autofill_status_row.setContentsMargins(0, 0, 0, 0)
        varac_autofill_status_row.setSpacing(8)
        self.varac_autofill_status_label = QLabel("No auto-fill attempt yet.")
        self.varac_autofill_status_label.setWordWrap(True)
        self._autofill_status_labels["varac"] = self.varac_autofill_status_label
        varac_autofill_status_row.addWidget(self.varac_autofill_status_label, 1)
        varac_paths_v.addLayout(varac_autofill_status_row)

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
            "Managed BBS Services keeps the live VarAC BBS folder stable while FreqInOut publishes "
            "a root menu, virtual folders, and optional FLAMP relay responses for the active requester. "
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
        vault_policy_inner.addWidget(QLabel("Return Mode"), 1, 0)
        self.varac_bbs_vault_return_mode_combo = QComboBox()
        self.varac_bbs_vault_return_mode_combo.addItems(
            ["On disconnect", "After inactivity timeout", "Manual operator reset only"]
        )
        self.varac_bbs_vault_return_mode_combo.setMinimumWidth(220)
        vault_policy_inner.addWidget(self.varac_bbs_vault_return_mode_combo, 1, 1)
        vault_policy_inner.addWidget(QLabel("Idle Timeout"), 1, 2)
        self.varac_bbs_vault_idle_timeout_combo = QComboBox()
        for seconds, label in ((300, "5 min"), (600, "10 min"), (900, "15 min"), (1800, "30 min")):
            self.varac_bbs_vault_idle_timeout_combo.addItem(label, seconds)
        vault_policy_inner.addWidget(self.varac_bbs_vault_idle_timeout_combo, 1, 3)
        vault_policy_inner.addWidget(QLabel("Request Parsing"), 2, 0)
        self.varac_bbs_vault_trigger_mode_combo = QComboBox()
        self.varac_bbs_vault_trigger_mode_combo.addItems(["VarAC session commands", "Command prefix", "Exact code only"])
        self.varac_bbs_vault_trigger_mode_combo.setMinimumWidth(220)
        vault_policy_inner.addWidget(self.varac_bbs_vault_trigger_mode_combo, 2, 1)
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
        vault_policy_inner.addWidget(QLabel("FLAMP Relay"), 3, 2)
        flamp_policy_row = QHBoxLayout()
        flamp_policy_row.setContentsMargins(0, 0, 0, 0)
        flamp_policy_row.setSpacing(8)
        self.varac_bbs_vault_flamp_enabled_chk = QCheckBox("Enable FLAMP relay service")
        flamp_policy_row.addWidget(self.varac_bbs_vault_flamp_enabled_chk)
        self.varac_bbs_vault_flamp_relay_dir_edit = QLineEdit()
        self.varac_bbs_vault_flamp_relay_dir_edit.setPlaceholderText("FLAMP relay folder")
        flamp_policy_row.addWidget(self.varac_bbs_vault_flamp_relay_dir_edit, 1)
        self.varac_bbs_vault_flamp_relay_browse_btn = QPushButton("Browse")
        self.varac_bbs_vault_flamp_relay_browse_btn.clicked.connect(self._choose_varac_bbs_vault_flamp_relay_dir)
        flamp_policy_row.addWidget(self.varac_bbs_vault_flamp_relay_browse_btn)
        vault_policy_inner.addLayout(flamp_policy_row, 3, 3)
        self.varac_bbs_vault_flamp_relay_hint_label = QLabel(
            "Set FLAMP/rx first and FreqInOut will suggest a sibling relay folder automatically."
        )
        self.varac_bbs_vault_flamp_relay_hint_label.setWordWrap(True)
        vault_policy_inner.addWidget(self.varac_bbs_vault_flamp_relay_hint_label, 4, 0, 1, 4)
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
        self.varac_bbs_vault_location_name_edit.setPlaceholderText("Example: Intel")
        vault_editor_grid.addWidget(self.varac_bbs_vault_location_name_edit, 0, 1)
        vault_editor_grid.addWidget(QLabel("Alias"), 0, 2)
        self.varac_bbs_vault_alias_edit = QLineEdit()
        self.varac_bbs_vault_alias_edit.setPlaceholderText("Example: INTEL")
        self.varac_bbs_vault_alias_edit.setMaxLength(32)
        self.varac_bbs_vault_alias_edit.setToolTip("Alias is the VarAC command callers type. Spaces are removed.")
        vault_editor_grid.addWidget(self.varac_bbs_vault_alias_edit, 0, 3)
        vault_editor_grid.addWidget(QLabel("Description"), 1, 0)
        self.varac_bbs_vault_description_edit = QLineEdit()
        self.varac_bbs_vault_description_edit.setPlaceholderText("Example: to open Logistics")
        vault_editor_grid.addWidget(self.varac_bbs_vault_description_edit, 1, 1, 1, 3)
        vault_editor_grid.addWidget(QLabel("Source Folder"), 2, 0)
        self.varac_bbs_vault_source_dir_edit = QLineEdit()
        self.varac_bbs_vault_source_dir_edit.setPlaceholderText("Managed Root/locations/<Location Name> is the usual pattern")
        vault_editor_grid.addWidget(self.varac_bbs_vault_source_dir_edit, 2, 1, 1, 2)
        self.varac_bbs_vault_source_dir_browse_btn = QPushButton("Browse")
        self.varac_bbs_vault_source_dir_browse_btn.clicked.connect(self._choose_varac_bbs_vault_location_source)
        vault_editor_grid.addWidget(self.varac_bbs_vault_source_dir_browse_btn, 2, 3)
        self.varac_bbs_vault_source_hint_label = QLabel(
            "Typical pattern in the VarAC BBS area: Managed Root / locations / <Location Name>. Save Location can create that folder."
        )
        self.varac_bbs_vault_source_hint_label.setWordWrap(True)
        vault_editor_grid.addWidget(self.varac_bbs_vault_source_hint_label, 3, 0, 1, 4)
        self.varac_bbs_vault_enabled_chk = QCheckBox("Enabled")
        vault_editor_grid.addWidget(self.varac_bbs_vault_enabled_chk, 4, 0)
        self.varac_bbs_vault_list_in_root_chk = QCheckBox("List In Root Menu")
        vault_editor_grid.addWidget(self.varac_bbs_vault_list_in_root_chk, 4, 1)
        vault_editor_grid.addWidget(QLabel("Visibility"), 4, 2)
        self.varac_bbs_vault_visibility_combo = QComboBox()
        self.varac_bbs_vault_visibility_combo.addItems(["Public", "Allowed callsigns only", "Hidden"])
        vault_editor_grid.addWidget(self.varac_bbs_vault_visibility_combo, 4, 3)
        vault_editor_grid.addWidget(QLabel("Open Rule"), 5, 0)
        self.varac_bbs_vault_open_rule_combo = QComboBox()
        self.varac_bbs_vault_open_rule_combo.addItems(["Public", "Allowed callsigns only", "Allowed callsigns + access code"])
        vault_editor_grid.addWidget(self.varac_bbs_vault_open_rule_combo, 5, 1, 1, 3)
        self.varac_bbs_vault_inherit_callsigns_chk = QCheckBox("Inherit Global Allowed Callsigns")
        vault_editor_grid.addWidget(self.varac_bbs_vault_inherit_callsigns_chk, 6, 0, 1, 4)
        vault_editor_grid.addWidget(QLabel("Location Allowed Callsigns"), 7, 0)
        self.varac_bbs_vault_allowed_callsigns_edit = QLineEdit()
        self.varac_bbs_vault_allowed_callsigns_edit.setPlaceholderText("Optional stricter subset, comma-separated")
        vault_editor_grid.addWidget(self.varac_bbs_vault_allowed_callsigns_edit, 7, 1, 1, 3)
        vault_editor_grid.addWidget(QLabel("Access Code"), 8, 0)
        self.varac_bbs_vault_access_code_edit = QLineEdit()
        self.varac_bbs_vault_access_code_edit.setEchoMode(QLineEdit.Password)
        self.varac_bbs_vault_access_code_edit.setPlaceholderText("Enter a new access code")
        self.varac_bbs_vault_access_code_edit.setToolTip(
            "Access codes are stored as secure hashes and cannot be shown again after save."
        )
        self._attach_password_toggle_action(self.varac_bbs_vault_access_code_edit)
        vault_editor_grid.addWidget(self.varac_bbs_vault_access_code_edit, 8, 1)
        vault_editor_grid.addWidget(QLabel("Confirm Code"), 8, 2)
        self.varac_bbs_vault_access_code_confirm_edit = QLineEdit()
        self.varac_bbs_vault_access_code_confirm_edit.setEchoMode(QLineEdit.Password)
        self.varac_bbs_vault_access_code_confirm_edit.setPlaceholderText("Confirm the new access code")
        self.varac_bbs_vault_access_code_confirm_edit.setToolTip(
            "Access codes are stored as secure hashes and cannot be shown again after save."
        )
        self._attach_password_toggle_action(self.varac_bbs_vault_access_code_confirm_edit)
        vault_editor_grid.addWidget(self.varac_bbs_vault_access_code_confirm_edit, 8, 3)
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
        self.varac_bbs_vault_status_label = QLabel("Managed Vault is not enabled for this station.")
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
        self.varac_bbs_vault_location_name_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_location_name_edit.textChanged.connect(
            lambda _text: self._autofill_varac_bbs_vault_location_defaults()
        )
        self.varac_bbs_vault_location_name_edit.textChanged.connect(
            lambda _text: self._refresh_varac_bbs_vault_source_hint()
        )
        self.varac_bbs_vault_alias_edit.textChanged.connect(self._sanitize_varac_bbs_vault_alias_text)
        self.varac_bbs_vault_alias_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_description_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_source_dir_edit.textChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_source_dir_edit.textChanged.connect(
            lambda _text: self._refresh_varac_bbs_vault_source_hint()
        )
        self.varac_bbs_vault_enabled_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_list_in_root_chk.stateChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_visibility_combo.currentIndexChanged.connect(self._mark_settings_dirty)
        self.varac_bbs_vault_open_rule_combo.currentIndexChanged.connect(self._mark_settings_dirty)
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
        self._add_settings_section(varac_group)
        self._add_settings_section(gpg_group)

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
        )
        self._register_collapsible_group(custom_tools_group, self._summary_custom_tools)
        custom_tools_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(custom_tools_group)

        # Launch Control
        launch_group = QGroupBox("Launch Control")
        launch_v = QVBoxLayout()
        launch_v.setSpacing(6)
        launch_group.setLayout(launch_v)

        launch_hint = QLabel("Only configured apps are shown. Launch order controls startup sequence.")
        launch_hint.setWordWrap(True)
        launch_v.addWidget(launch_hint)

        launch_global_row = QHBoxLayout()
        self.launch_all_with_startup_chk = QCheckBox("Launch All with FreqInOut")
        launch_global_row.addWidget(self.launch_all_with_startup_chk)
        launch_global_row.addStretch()
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
        launch_v.addWidget(self.launch_control_table)

        launch_btn_row = QHBoxLayout()
        self.launch_order_up_btn = QPushButton("Up")
        self.launch_order_down_btn = QPushButton("Down")
        self.launch_reset_order_btn = QPushButton("Reset Default Order")
        self.launch_configured_now_btn = QPushButton("Launch Configured Now")
        self.launch_stop_btn = QPushButton("Stop Launch Sequence")
        self.launch_stop_btn.setEnabled(False)
        launch_btn_row.addWidget(self.launch_order_up_btn)
        launch_btn_row.addWidget(self.launch_order_down_btn)
        launch_btn_row.addWidget(self.launch_reset_order_btn)
        launch_btn_row.addStretch()
        launch_btn_row.addWidget(self.launch_configured_now_btn)
        launch_btn_row.addWidget(self.launch_stop_btn)
        launch_v.addLayout(launch_btn_row)

        self.launch_summary_label = QLabel("Launch status: Idle")
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
        self._add_settings_section(launch_group)

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
        sop_export_group = self._make_collapsible_group("SOP Export", sop_export_container, checked=True, fit_content=True)
        self._register_collapsible_group(sop_export_group, self._summary_sop_export)
        sop_export_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._add_settings_section(sop_export_group)
        self._add_settings_section(logging_section)

        # bottom save
        bottom_row = QHBoxLayout()
        bottom_row.addStretch()
        self.save_btn = QPushButton("Save Settings")
        self.save_btn.clicked.connect(self._save_settings_button)
        bottom_row.addWidget(self.save_btn)
        main_layout.addLayout(bottom_row)
        self._wire_dirty_tracking()
        self._refresh_launch_control_table()
        self._set_save_button_state("success")
        self._refresh_section_titles()
        if self.sections_nav_list.count() > 0:
            self.sections_nav_list.setCurrentRow(0)
        self._update_sections_nav_size()
        self._apply_accessibility_width_guards()

    def _make_collapsible_group(
        self,
        title: str,
        content: QWidget,
        *,
        checked: bool,
        fit_content: bool,
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
        header_row.addWidget(header_btn)
        if help_context_key:
            header_row.addWidget(
                self._make_context_help_button(
                    help_context_key,
                    tooltip=f"Open help for {title}.",
                )
            )
        header_row.addStretch()

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

    def _add_settings_section(self, group: QGroupBox) -> None:
        meta = self._section_meta.get(group, {})
        title = str(meta.get("title", group.title() if hasattr(group, "title") else "Section"))
        item = QListWidgetItem(title)
        self.sections_nav_list.addItem(item)
        self._section_nav_items[group] = item
        self.sections_stack.addWidget(group)
        content = meta.get("content")
        header_btn = meta.get("header_btn")
        if isinstance(content, QWidget):
            expanded = bool(header_btn.isChecked()) if header_btn else True
            self._apply_collapsed_state(group, content, expanded)
        self._update_sections_nav_size()

    def _set_section_health_key(self, group: QGroupBox, health_key: str) -> None:
        meta = self._section_meta.get(group, {})
        meta["health_key"] = str(health_key or "").strip().lower()
        self._section_meta[group] = meta

    def focus_section_by_health_key(self, health_key: str, radio_id: int | None = None) -> bool:
        del radio_id
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
            self.sections_nav_list.setCurrentRow(row)
            self.sections_nav_list.scrollToItem(item)
            return True
        return False

    def _section_header_style(self, state: str, theme: Dict[str, str]) -> str:
        state = str(state or "neutral").strip().lower()
        if state in {"warn", "needs_setup"}:
            border = theme.get("warning", "#C99700")
            fg = border
            bg = theme.get("surface", "#ffffff")
            hover_bg = theme.get("surface_alt", bg)
            font_weight = "700"
        elif state == "degraded":
            border = theme.get("warning", "#C99700")
            fg = theme.get("text", "#222222")
            bg = theme.get("surface", "#ffffff")
            hover_bg = theme.get("surface_alt", bg)
            font_weight = "700"
        elif state in {"not_enabled", "external_manual"}:
            border = theme.get("border", "#cccccc")
            fg = theme.get("text_muted", theme.get("text", "#666666"))
            bg = theme.get("surface", "#ffffff")
            hover_bg = theme.get("surface_alt", bg)
            font_weight = "600"
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
            f" border: 1px solid {border if state in {'warn', 'needs_setup', 'degraded'} else theme.get('border', '#cccccc')};"
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

        if state in {"warn", "needs_setup"}:
            border = QColor(theme.get("warning", "#C99700"))
            bg = QColor(border)
            bg.setAlpha(120 if selected else (84 if hovered else 58))
            return {"bg": bg, "border": border, "fg": text_color, "bold": True}

        if state == "degraded":
            border = QColor(theme.get("warning", "#C99700"))
            bg = QColor(border)
            bg.setAlpha(88 if selected else (64 if hovered else 44))
            return {"bg": bg, "border": border, "fg": text_color, "bold": True}

        if state in {"not_enabled", "external_manual"}:
            border = QColor(theme.get("border", "#cccccc"))
            bg = QColor(theme.get("surface", "#ffffff"))
            bg.setAlpha(68 if selected else (44 if hovered else 22))
            muted = QColor(theme.get("text_muted", theme.get("text", "#666666")))
            return {"bg": bg, "border": border, "fg": muted, "bold": False}

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
        count = self.sections_nav_list.count()
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
                    (int(fm.horizontalAdvance(self.sections_nav_list.item(i).text())) for i in range(count)),
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
        if row >= self.sections_stack.count():
            return
        with perf_span("settings.section_switch", settings=self.settings, min_ms=10.0):
            self.sections_stack.setCurrentIndex(row)
            self._sync_current_section_scroll_size()
            self._apply_sections_nav_style()
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

    def _sync_current_section_scroll_size(self) -> None:
        if not hasattr(self, "sections_stack"):
            return
        page = self.sections_stack.currentWidget()
        if page is None:
            return
        try:
            target_h = max(0, int(page.sizeHint().height()))
            page.setMinimumHeight(target_h)
            self.sections_stack.setMinimumHeight(target_h)
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
        self._refresh_section_nav_health()

    def _build_section_health_entry(self, *, engaged: bool, issues: List[str]) -> Dict[str, str]:
        detail = "; ".join(str(issue).strip() for issue in issues if str(issue).strip())
        if detail:
            return {"state": "needs_setup", "detail": detail}
        if engaged:
            return {"state": "ready", "detail": ""}
        return {"state": "not_enabled", "detail": ""}

    def _section_readiness_entry(self, report, *, section_key: str, engaged: bool) -> Dict[str, str]:
        section_issues = [issue for issue in report.issues if issue.section_key == section_key]
        detail = "; ".join(format_readiness_issue(issue, include_resolution=False) for issue in section_issues)
        if any(issue.severity == "required" for issue in section_issues):
            return {"state": "needs_setup", "detail": detail}
        if any(issue.severity == "recommended" for issue in section_issues):
            return {"state": "degraded", "detail": detail}
        if any(issue.severity == "informational" for issue in section_issues):
            state_key = next((issue.state_key for issue in section_issues if issue.state_key), "not_enabled")
            return {"state": state_key, "detail": detail}
        if engaged:
            return {"state": "ready", "detail": ""}
        return {"state": "not_enabled", "detail": ""}

    def _build_section_health_snapshot(self) -> Dict[str, Dict[str, str]]:
        snapshot: Dict[str, Dict[str, str]] = {}
        report = self._current_station_readiness_report()
        snapshot["freqinout"] = self._section_readiness_entry(report, section_key="freqinout", engaged=True)
        snapshot["operating_groups"] = self._section_readiness_entry(
            report,
            section_key="operating_groups",
            engaged=True,
        )

        js8_engaged = any(
            [
                hasattr(self, "use_js8call_chk") and self.use_js8call_chk.isChecked(),
                hasattr(self, "use_js8spotter_chk") and self.use_js8spotter_chk.isChecked(),
                hasattr(self, "use_commstat_chk") and self.use_commstat_chk.isChecked(),
            ]
        )
        snapshot["js8call"] = self._section_readiness_entry(report, section_key="js8call", engaged=js8_engaged)

        default_fldigi_checkin_dir = str(get_fldigi_checkin_dir())
        flrig_path = self.path_edits.get("FLRig").text().strip() if self.path_edits.get("FLRig") else ""
        fldigi_path = self.path_edits.get("FLDigi").text().strip() if self.path_edits.get("FLDigi") else ""
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
                hasattr(self, "use_flrig_chk") and self.use_flrig_chk.isChecked(),
                hasattr(self, "use_fldigi_chk") and self.use_fldigi_chk.isChecked(),
                hasattr(self, "use_flmsg_chk") and self.use_flmsg_chk.isChecked(),
                hasattr(self, "use_flamp_chk") and self.use_flamp_chk.isChecked(),
            ]
        )
        snapshot["fast_light"] = self._section_readiness_entry(
            report,
            section_key="fast_light",
            engaged=fast_light_engaged,
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
        varac_engaged = bool(hasattr(self, "use_varac_chk") and self.use_varac_chk.isChecked())
        varac_guard_enabled = bool(
            hasattr(self, "varac_guard_enabled_chk") and self.varac_guard_enabled_chk.isChecked()
        )
        varac_guard_quarantine = (
            self.varac_guard_quarantine_dir_edit.text().strip()
            if hasattr(self, "varac_guard_quarantine_dir_edit")
            else ""
        )
        varac_vault_enabled = bool(
            hasattr(self, "varac_bbs_vault_enabled_chk_main") and self.varac_bbs_vault_enabled_chk_main.isChecked()
        )
        varac_vault_root = (
            self.varac_bbs_vault_root_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_root_edit")
            else ""
        )
        varac_engaged = any([varac_engaged, varac_guard_enabled, varac_guard_quarantine, varac_vault_enabled, varac_vault_root])
        snapshot["varac"] = self._section_readiness_entry(report, section_key="varac", engaged=varac_engaged)
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
        state_label = readiness_state_label(state)
        item.setToolTip(base_title if not detail else f"{base_title}\n{state_label}: {detail}")
        font = item.font()
        font.setBold(state in {"warn", "needs_setup", "degraded"})
        item.setFont(font)
        if state in {"warn", "needs_setup", "degraded", "not_enabled", "external_manual"}:
            visuals = self._section_nav_visuals(state, selected=False, hovered=False, theme=theme)
            item.setBackground(QBrush(visuals["bg"]))
            item.setForeground(QBrush(visuals["fg"]))
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
        self._update_readiness_summary_card()

    def _apply_collapsed_state(self, group: QGroupBox, content: QWidget, expanded: bool) -> None:
        content.setVisible(expanded)
        stacked_mode = hasattr(self, "sections_stack") and self.sections_stack.count() > 0
        fit_content = bool(self._section_meta.get(group, {}).get("fit_content", False))
        if stacked_mode and not bool(self._section_meta.get(group, {}).get("fit_content_in_stack", False)):
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

    def _attempt_fast_light_autofill(self) -> None:
        self._apply_autofill_results("fast_light", self.software_path_detector.detect_fast_light())

    def _attempt_js8_autofill(self) -> None:
        self._apply_autofill_results("js8", self.software_path_detector.detect_js8())

    def _attempt_varac_autofill(self) -> None:
        self._apply_autofill_results("varac", self.software_path_detector.detect_varac())

    def _apply_autofill_results(self, section: str, results: Dict[str, PathDetectionResult]) -> None:
        filled: List[str] = []
        preserved: List[str] = []
        missing: List[str] = []
        detail_lines: List[str] = []
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
        self._set_autofill_status(section, " ".join(summary_parts), "\n".join(detail_lines))
        self._refresh_section_titles()
        self._refresh_section_nav_health()

    def _set_autofill_status(self, section: str, text: str, tooltip: str) -> None:
        label = self._autofill_status_labels.get(section)
        if label is None:
            return
        label.setText(text)
        label.setToolTip(tooltip or text)

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

    @staticmethod
    def _coerce_boolish_setting(value: object) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        txt = str(value or "").strip().lower()
        if not txt:
            return None
        if txt in {"1", "true", "yes", "on"}:
            return True
        if txt in {"0", "false", "no", "off"}:
            return False
        return None

    def _single_software_used_value(self, data: Dict[str, object], key: str) -> bool:
        normalized = str(key or "").strip().lower()
        explicit = self._coerce_boolish_setting(data.get(f"use_{normalized}"))
        if explicit is not None:
            return explicit
        message_paths = data.get("message_paths", {})
        if not isinstance(message_paths, dict):
            message_paths = {}
        if normalized == "flrig":
            return any(
                [
                    str(data.get("path_flrig", "") or "").strip(),
                    str(data.get("flrig_host", "") or "").strip(),
                    str(data.get("flrig_port", "") or "").strip(),
                    str(data.get("control_via", "") or "").strip().upper() == "FLRIG",
                ]
            )
        if normalized == "fldigi":
            return any(
                [
                    str(data.get("path_fldigi", "") or "").strip(),
                    str(data.get("fldigi_host", "") or "").strip(),
                    str(data.get("fldigi_port", "") or "").strip(),
                    str(data.get("fldigi_log_path", "") or "").strip(),
                    str(data.get("fldigi_checkin_dir", "") or "").strip(),
                ]
            )
        if normalized == "flmsg":
            return any([str(data.get("path_flmsg", "") or "").strip(), str(message_paths.get("flmsg", "") or "").strip()])
        if normalized == "flamp":
            return any([str(data.get("path_flamp", "") or "").strip(), str(message_paths.get("flamp", "") or "").strip()])
        if normalized == "js8call":
            return any(
                [
                    str(data.get("path_js8call", "") or "").strip(),
                    str(data.get("js8_host", "") or "").strip(),
                    str(data.get("js8_port", "") or "").strip(),
                    str(data.get("js8_directed_path", "") or "").strip(),
                    str(data.get("js8_forms_path", "") or "").strip(),
                    str(data.get("control_via", "") or "").strip().upper() == "JS8CALL",
                ]
            )
        if normalized == "js8spotter":
            return any([str(data.get("path_js8spotter", "") or "").strip(), str(data.get("js8_forms_path", "") or "").strip()])
        if normalized == "commstat":
            return bool(str(data.get("path_commstat", "") or "").strip())
        if normalized == "varac":
            return any(
                [
                    str(data.get("varac_path", "") or "").strip(),
                    str(data.get("varac_launch_cmd", "") or "").strip(),
                    str(message_paths.get("varac", "") or "").strip(),
                    str(data.get("varac_outbox_dir", "") or "").strip(),
                    str(data.get("varac_bbs_dir", "") or "").strip(),
                    str(data.get("varac_bbs_archive_dir", "") or "").strip(),
                ]
            )
        return False

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
        allowed_ctrl = ["FLRig", "JS8Call", "Manual"]
        if ctrl not in allowed_ctrl:
            ctrl = "FLRig"
        self.control_combo.setCurrentText(ctrl)
        if hasattr(self, "use_flrig_chk"):
            self.use_flrig_chk.setChecked(self._single_software_used_value(data, "flrig"))
        if hasattr(self, "use_fldigi_chk"):
            self.use_fldigi_chk.setChecked(self._single_software_used_value(data, "fldigi"))
        if hasattr(self, "use_flmsg_chk"):
            self.use_flmsg_chk.setChecked(self._single_software_used_value(data, "flmsg"))
        if hasattr(self, "use_flamp_chk"):
            self.use_flamp_chk.setChecked(self._single_software_used_value(data, "flamp"))
        if hasattr(self, "use_js8call_chk"):
            self.use_js8call_chk.setChecked(self._single_software_used_value(data, "js8call"))
        if hasattr(self, "use_js8spotter_chk"):
            self.use_js8spotter_chk.setChecked(self._single_software_used_value(data, "js8spotter"))
        if hasattr(self, "use_commstat_chk"):
            self.use_commstat_chk.setChecked(self._single_software_used_value(data, "commstat"))
        if hasattr(self, "use_varac_chk"):
            self.use_varac_chk.setChecked(self._single_software_used_value(data, "varac"))
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
            offset_int = 1900 + (datetime.datetime.now(datetime.timezone.utc).hour % 7) * 50
            if hasattr(self.settings, "set"):
                self.settings.set("js8_offset_hz", offset_int)
            else:
                data["js8_offset_hz"] = offset_int
                if hasattr(self.settings, "_data"):
                    self.settings._data = data  # type: ignore[attr-defined]
        self.js8_offset_edit.setText(str(offset_int))
        self.js8_forms_edit.setText(data.get("js8_forms_path", "") or "")
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
        if hasattr(self, "varac_bbs_vault_enabled_chk_main"):
            self.varac_bbs_vault_enabled_chk_main.setChecked(bool(data.get("varac_bbs_vault_enabled", False)))
        if hasattr(self, "varac_bbs_vault_root_edit"):
            root_txt = compute_default_managed_root(self.varac_bbs_dir_edit.text().strip())
            self._set_varac_bbs_vault_root_text(root_txt)
        if hasattr(self, "varac_bbs_vault_trigger_mode_combo"):
            trigger_mode = str(data.get("varac_bbs_vault_trigger_mode", DEFAULT_TRIGGER_MODE) or DEFAULT_TRIGGER_MODE).strip()
            if trigger_mode not in {"VarAC session commands", "Command prefix", "Exact code only"}:
                trigger_mode = "VarAC session commands"
            self.varac_bbs_vault_trigger_mode_combo.setCurrentText(trigger_mode)
        if hasattr(self, "varac_bbs_vault_global_code_policy_combo"):
            code_policy = str(data.get("varac_bbs_vault_global_code_policy", DEFAULT_GLOBAL_CODE_POLICY) or DEFAULT_GLOBAL_CODE_POLICY).strip()
            idx = self.varac_bbs_vault_global_code_policy_combo.findText(code_policy)
            if idx < 0:
                idx = self.varac_bbs_vault_global_code_policy_combo.findText(DEFAULT_GLOBAL_CODE_POLICY)
            self.varac_bbs_vault_global_code_policy_combo.setCurrentIndex(max(0, idx))
        if hasattr(self, "varac_bbs_vault_return_mode_combo"):
            return_mode = str(data.get("varac_bbs_vault_return_mode", DEFAULT_RETURN_MODE) or DEFAULT_RETURN_MODE).strip()
            if return_mode not in {"On disconnect", "After inactivity timeout", "Manual operator reset only"}:
                return_mode = DEFAULT_RETURN_MODE
            self.varac_bbs_vault_return_mode_combo.setCurrentText(return_mode)
        if hasattr(self, "varac_bbs_vault_idle_timeout_combo"):
            timeout_seconds = int(data.get("varac_bbs_vault_idle_timeout_seconds", DEFAULT_IDLE_TIMEOUT_SECONDS) or DEFAULT_IDLE_TIMEOUT_SECONDS)
            idx = self.varac_bbs_vault_idle_timeout_combo.findData(timeout_seconds)
            if idx < 0:
                idx = self.varac_bbs_vault_idle_timeout_combo.findData(DEFAULT_IDLE_TIMEOUT_SECONDS)
            if idx >= 0:
                self.varac_bbs_vault_idle_timeout_combo.setCurrentIndex(idx)
        if hasattr(self, "varac_bbs_vault_failed_attempt_limit_combo"):
            attempt_limit = int(data.get("varac_bbs_vault_failed_attempt_limit", DEFAULT_FAILED_ATTEMPT_LIMIT) or DEFAULT_FAILED_ATTEMPT_LIMIT)
            idx = self.varac_bbs_vault_failed_attempt_limit_combo.findData(attempt_limit)
            if idx < 0:
                idx = self.varac_bbs_vault_failed_attempt_limit_combo.findData(DEFAULT_FAILED_ATTEMPT_LIMIT)
            if idx >= 0:
                self.varac_bbs_vault_failed_attempt_limit_combo.setCurrentIndex(idx)
        if hasattr(self, "varac_bbs_vault_cooldown_combo"):
            cooldown_seconds = int(data.get("varac_bbs_vault_cooldown_seconds", DEFAULT_COOLDOWN_SECONDS) or DEFAULT_COOLDOWN_SECONDS)
            idx = self.varac_bbs_vault_cooldown_combo.findData(cooldown_seconds)
            if idx < 0:
                idx = self.varac_bbs_vault_cooldown_combo.findData(DEFAULT_COOLDOWN_SECONDS)
            if idx >= 0:
                self.varac_bbs_vault_cooldown_combo.setCurrentIndex(idx)
        if hasattr(self, "varac_bbs_vault_flamp_enabled_chk"):
            self.varac_bbs_vault_flamp_enabled_chk.setChecked(bool(data.get("varac_bbs_vault_flamp_enabled", False)))
        if hasattr(self, "varac_bbs_vault_flamp_relay_dir_edit"):
            self.varac_bbs_vault_flamp_relay_dir_edit.setText(str(data.get("varac_bbs_vault_flamp_relay_dir", "") or "").strip())
        self._maybe_autofill_varac_bbs_vault_flamp_relay_dir()
        self._set_varac_bbs_vault_locations(data.get("varac_bbs_vault_locations_v1", []))
        if hasattr(self, "varac_bbs_vault_default_location_combo"):
            default_id = str(data.get("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID) or DEFAULT_LOCATION_ID).strip()
            idx = self.varac_bbs_vault_default_location_combo.findData(default_id)
            if idx < 0 and self.varac_bbs_vault_default_location_combo.count() > 0:
                idx = 0
            if idx >= 0:
                self.varac_bbs_vault_default_location_combo.setCurrentIndex(idx)
        self._refresh_varac_bbs_vault_status_label()
        self._sync_varac_bbs_vault_root_from_bbs_dir(force=False)
        self._refresh_varac_bbs_vault_flamp_hint()
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

        log.info("SettingsTab: settings loaded.")
        self._update_launch_control_buttons()
        self._update_op_group_action_buttons()
        self._update_local_net_action_buttons()
        self._loading_settings = False
        self._settings_dirty = False
        self._set_save_button_state("success")
        self._refresh_section_titles()
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
        self._save_settings(show_message=False)

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
        data["use_flrig"] = bool(self.use_flrig_chk.isChecked()) if hasattr(self, "use_flrig_chk") else False
        data["use_fldigi"] = bool(self.use_fldigi_chk.isChecked()) if hasattr(self, "use_fldigi_chk") else False
        data["use_flmsg"] = bool(self.use_flmsg_chk.isChecked()) if hasattr(self, "use_flmsg_chk") else False
        data["use_flamp"] = bool(self.use_flamp_chk.isChecked()) if hasattr(self, "use_flamp_chk") else False
        data["use_js8call"] = bool(self.use_js8call_chk.isChecked()) if hasattr(self, "use_js8call_chk") else False
        data["use_js8spotter"] = bool(self.use_js8spotter_chk.isChecked()) if hasattr(self, "use_js8spotter_chk") else False
        data["use_commstat"] = bool(self.use_commstat_chk.isChecked()) if hasattr(self, "use_commstat_chk") else False
        data["use_varac"] = bool(self.use_varac_chk.isChecked()) if hasattr(self, "use_varac_chk") else False
        freq_mode = self.freq_enforce_combo.currentText().strip()
        fldigi_mode = self.fldigi_enforce_combo.currentText().strip()
        js8_mode = self.js8_enforce_combo.currentText().strip()
        freq_prompt = self.freq_prompt_combo.currentText().strip()
        fldigi_prompt = self.fldigi_prompt_combo.currentText().strip()
        js8_prompt = self.js8_prompt_combo.currentText().strip()
        missing = []
        if freq_mode == "Prompt" and freq_prompt == "Select Interval":
            missing.append("Frequency Prompt Interval")
        if any([data["use_flrig"], data["use_fldigi"], data["use_flmsg"], data["use_flamp"]]) and fldigi_mode == "Prompt" and fldigi_prompt == "Select Interval":
            missing.append("FLDigi Prompt Interval")
        if any([data["use_js8call"], data["use_js8spotter"], data["use_commstat"]]) and js8_mode == "Prompt" and js8_prompt == "Select Interval":
            missing.append("JS8 Prompt Interval")
        if missing:
            QMessageBox.warning(self, "Settings", f"Please select: {', '.join(missing)}.")
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
            offset_val = 0
            self.js8_offset_edit.setText("0")
        data["js8_offset_hz"] = offset_val

        data["js8_forms_path"] = self.js8_forms_edit.text().strip()
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
        retry_txt = self.varac_guard_retry_combo.currentText().strip() if hasattr(self, "varac_guard_retry_combo") else "120"
        if retry_txt not in {"30", "60", "120", "300", "600"}:
            retry_txt = "120"
        data["varac_guard_retry_seconds"] = int(retry_txt)
        data["varac_guard_quarantine_dir"] = (
            self.varac_guard_quarantine_dir_edit.text().strip() if hasattr(self, "varac_guard_quarantine_dir_edit") else ""
        )
        data["varac_bbs_vault_enabled"] = bool(
            self.varac_bbs_vault_enabled_chk_main.isChecked()
            if hasattr(self, "varac_bbs_vault_enabled_chk_main")
            else False
        )
        data["varac_bbs_vault_managed_root"] = self._computed_varac_bbs_vault_default_root()
        data["varac_bbs_vault_default_location_id"] = (
            str(self.varac_bbs_vault_default_location_combo.currentData() or "").strip()
            if hasattr(self, "varac_bbs_vault_default_location_combo")
            else DEFAULT_LOCATION_ID
        ) or DEFAULT_LOCATION_ID
        data["varac_bbs_vault_trigger_mode"] = (
            self.varac_bbs_vault_trigger_mode_combo.currentText().strip()
            if hasattr(self, "varac_bbs_vault_trigger_mode_combo")
            else DEFAULT_TRIGGER_MODE
        )
        data["varac_bbs_vault_return_mode"] = (
            self.varac_bbs_vault_return_mode_combo.currentText().strip()
            if hasattr(self, "varac_bbs_vault_return_mode_combo")
            else DEFAULT_RETURN_MODE
        )
        data["varac_bbs_vault_failed_attempt_limit"] = int(
            self.varac_bbs_vault_failed_attempt_limit_combo.currentData()
            if hasattr(self, "varac_bbs_vault_failed_attempt_limit_combo")
            and self.varac_bbs_vault_failed_attempt_limit_combo.currentData() is not None
            else DEFAULT_FAILED_ATTEMPT_LIMIT
        )
        data["varac_bbs_vault_failed_attempt_window_seconds"] = DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS
        data["varac_bbs_vault_cooldown_seconds"] = int(
            self.varac_bbs_vault_cooldown_combo.currentData()
            if hasattr(self, "varac_bbs_vault_cooldown_combo")
            and self.varac_bbs_vault_cooldown_combo.currentData() is not None
            else DEFAULT_COOLDOWN_SECONDS
        )
        data["varac_bbs_vault_global_code_policy"] = (
            self.varac_bbs_vault_global_code_policy_combo.currentText().strip()
            if hasattr(self, "varac_bbs_vault_global_code_policy_combo")
            else DEFAULT_GLOBAL_CODE_POLICY
        )
        data["varac_bbs_vault_idle_timeout_seconds"] = int(
            self.varac_bbs_vault_idle_timeout_combo.currentData()
            if hasattr(self, "varac_bbs_vault_idle_timeout_combo")
            and self.varac_bbs_vault_idle_timeout_combo.currentData() is not None
            else DEFAULT_IDLE_TIMEOUT_SECONDS
        )
        data["varac_bbs_vault_flamp_enabled"] = bool(
            self.varac_bbs_vault_flamp_enabled_chk.isChecked() if hasattr(self, "varac_bbs_vault_flamp_enabled_chk") else False
        )
        data["varac_bbs_vault_flamp_relay_dir"] = (
            self.varac_bbs_vault_flamp_relay_dir_edit.text().strip()
            if hasattr(self, "varac_bbs_vault_flamp_relay_dir_edit")
            else ""
        )
        data["varac_bbs_vault_locations_v1"] = list(self._varac_bbs_vault_locations_cache)
        data["varac_bbs_vault_runtime_state_v1"] = self.settings.get("varac_bbs_vault_runtime_state_v1", {})
        data["varac_bbs_vault_last_summary"] = str(self.settings.get("varac_bbs_vault_last_summary", "") or "").strip()
        try:
            if hasattr(self, "varac_ini_path_edit") and self.varac_ini_path_edit.text().strip():
                self._varac_bbs_ini_sync_state = varac_ini_sync_state_to_json(
                    get_varac_ini_sync_state(self.varac_ini_path_edit.text().strip())
                )
        except Exception:
            pass
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
        if data["varac_bbs_vault_enabled"]:
            if not data["varac_bbs_dir"]:
                QMessageBox.warning(
                    self,
                    "Managed BBS Vault",
                    "Set the VarAC BBS directory before enabling Managed BBS Vault.",
                )
                return
            if not data["varac_bbs_vault_managed_root"]:
                QMessageBox.warning(
                    self,
                    "Managed BBS Vault",
                    "Set the Managed Root before enabling Managed BBS Vault.",
                )
                return
            locations = load_vault_locations(data.get("varac_bbs_vault_locations_v1", []))
            if not locations:
                QMessageBox.warning(
                    self,
                    "Managed BBS Vault",
                    "Initialize the Managed BBS Vault or add at least one location before saving.",
                )
                return
            default_location = next(
                (loc for loc in locations if loc.id == data["varac_bbs_vault_default_location_id"]),
                None,
            )
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
        data["launch_readiness_timeout_sec"] = int(self.settings.get("launch_readiness_timeout_sec", 30) or 30)
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
                "use_flrig": data.get("use_flrig", False),
                "use_fldigi": data.get("use_fldigi", False),
                "use_flmsg": data.get("use_flmsg", False),
                "use_flamp": data.get("use_flamp", False),
                "use_js8call": data.get("use_js8call", False),
                "use_js8spotter": data.get("use_js8spotter", False),
                "use_commstat": data.get("use_commstat", False),
                "use_varac": data.get("use_varac", False),
                "freq_enforcement_mode": data.get("freq_enforcement_mode", "On Schedule Change"),
                "freq_prompt_interval": data.get("freq_prompt_interval", "Hourly"),
                "fldigi_enforcement_mode": data.get("fldigi_enforcement_mode", "On Schedule Change"),
                "fldigi_prompt_interval": data.get("fldigi_prompt_interval", "Hourly"),
                "js8_enforcement_mode": data.get("js8_enforcement_mode", "On Schedule Change"),
                "js8_prompt_interval": data.get("js8_prompt_interval", "Hourly"),
                "ui_theme": data.get("ui_theme", "light"),
                "flrig_port": data.get("flrig_port", 12345),
                "fldigi_host": data.get("fldigi_host", self._resolved_fldigi_host_value(data)),
                "fldigi_port": data.get("fldigi_port", 7362),
                "js8_host": data.get("js8_host", "127.0.0.1"),
                "js8_port": data["js8_port"],
                "js8_offset_hz": data.get("js8_offset_hz", 0),
                "primary_js8_groups": data["primary_js8_groups"],
                "js8_directed_path": data["js8_directed_path"],
                "js8_forms_path": data.get("js8_forms_path", ""),
                "path_js8call": data.get("path_js8call", ""),
                "path_js8spotter": data.get("path_js8spotter", ""),
                "path_commstat": data.get("path_commstat", ""),
                "js8_inbox_mark_retrieved_sync": data.get("js8_inbox_mark_retrieved_sync", False),
                "message_paths": data.get("message_paths", {}),
                "gpg_verify_flamp_k2s_enabled": data.get("gpg_verify_flamp_k2s_enabled", False),
                "hash_verify_flamp_k2s_enabled": data.get("hash_verify_flamp_k2s_enabled", True),
                "gpg_executable_path": data.get("gpg_executable_path", ""),
                "gpg_trusted_signers": data.get("gpg_trusted_signers", []),
                "trusted_file_hashes": data.get("trusted_file_hashes", []),
                "varac_path": data.get("varac_path", ""),
                "varac_db_path": data.get("varac_db_path", ""),
                "varac_launch_cmd": data.get("varac_launch_cmd", ""),
                "varac_outbox_dir": data.get("varac_outbox_dir", ""),
                "varac_bbs_dir": data.get("varac_bbs_dir", ""),
                "varac_bbs_archive_dir": data.get("varac_bbs_archive_dir", ""),
                "varac_bbs_auto_archive_enabled": data.get("varac_bbs_auto_archive_enabled", False),
                "varac_bbs_auto_archive_days": data.get("varac_bbs_auto_archive_days", 14),
                "varac_bbs_enabled": data.get("varac_bbs_enabled", False),
                "varac_bbs_limit_access_enabled": data.get("varac_bbs_limit_access_enabled", False),
                "varac_bbs_announce_enabled": data.get("varac_bbs_announce_enabled", False),
                "varac_bbs_allowed_callsigns": data.get("varac_bbs_allowed_callsigns", ""),
                "varac_guard_enabled": data.get("varac_guard_enabled", False),
                "varac_guard_mode": data.get("varac_guard_mode", "Log only"),
                "varac_guard_retry_seconds": data.get("varac_guard_retry_seconds", 120),
                "varac_guard_quarantine_dir": data.get("varac_guard_quarantine_dir", ""),
                "varac_bbs_vault_enabled": data.get("varac_bbs_vault_enabled", False),
                "varac_bbs_vault_managed_root": data.get("varac_bbs_vault_managed_root", ""),
                "varac_bbs_vault_default_location_id": data.get("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID),
                "varac_bbs_vault_global_code_policy": data.get("varac_bbs_vault_global_code_policy", DEFAULT_GLOBAL_CODE_POLICY),
                "varac_bbs_vault_trigger_mode": data.get("varac_bbs_vault_trigger_mode", DEFAULT_TRIGGER_MODE),
                "varac_bbs_vault_return_mode": data.get("varac_bbs_vault_return_mode", DEFAULT_RETURN_MODE),
                "varac_bbs_vault_failed_attempt_limit": data.get("varac_bbs_vault_failed_attempt_limit", DEFAULT_FAILED_ATTEMPT_LIMIT),
                "varac_bbs_vault_failed_attempt_window_seconds": data.get("varac_bbs_vault_failed_attempt_window_seconds", DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS),
                "varac_bbs_vault_cooldown_seconds": data.get("varac_bbs_vault_cooldown_seconds", DEFAULT_COOLDOWN_SECONDS),
                "varac_bbs_vault_idle_timeout_seconds": data.get("varac_bbs_vault_idle_timeout_seconds", DEFAULT_IDLE_TIMEOUT_SECONDS),
                "varac_bbs_vault_flamp_enabled": data.get("varac_bbs_vault_flamp_enabled", False),
                "varac_bbs_vault_flamp_relay_dir": data.get("varac_bbs_vault_flamp_relay_dir", ""),
                "varac_bbs_vault_locations_v1": data.get("varac_bbs_vault_locations_v1", []),
                "varac_bbs_vault_runtime_state_v1": data.get("varac_bbs_vault_runtime_state_v1", {}),
                "varac_bbs_vault_last_summary": data.get("varac_bbs_vault_last_summary", ""),
                "sop_export_preamble": data.get("sop_export_preamble", ""),
                "sop_export_postamble": data.get("sop_export_postamble", ""),
                "fldigi_log_path": data.get("fldigi_log_path", ""),
                "fldigi_checkin_dir": data.get("fldigi_checkin_dir", ""),
                "custom_tool_items": data.get("custom_tool_items", []),
                "launch_control_items": data.get("launch_control_items", []),
                "launch_control_enabled": data.get("launch_control_enabled", True),
                "launch_control_migrated_v1": data.get("launch_control_migrated_v1", True),
                "launch_readiness_timeout_sec": data.get("launch_readiness_timeout_sec", 30),
                "operating_groups": data.get("operating_groups", []),
                "local_net_profiles": data.get("local_net_profiles", []),
            }
            for prog_name, meta in self.PROGRAMS.items():
                path_key = meta["setting_key"]
                auto_key = meta["autostart_key"]
                if path_key:
                    batch[path_key] = data.get(path_key, "")
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
            self.settings.set("use_flrig", data.get("use_flrig", False))
            self.settings.set("use_fldigi", data.get("use_fldigi", False))
            self.settings.set("use_flmsg", data.get("use_flmsg", False))
            self.settings.set("use_flamp", data.get("use_flamp", False))
            self.settings.set("use_js8call", data.get("use_js8call", False))
            self.settings.set("use_js8spotter", data.get("use_js8spotter", False))
            self.settings.set("use_commstat", data.get("use_commstat", False))
            self.settings.set("use_varac", data.get("use_varac", False))
            self.settings.set("freq_enforcement_mode", data.get("freq_enforcement_mode", "On Schedule Change"))
            self.settings.set("freq_prompt_interval", data.get("freq_prompt_interval", "Hourly"))
            self.settings.set("fldigi_enforcement_mode", data.get("fldigi_enforcement_mode", "On Schedule Change"))
            self.settings.set("fldigi_prompt_interval", data.get("fldigi_prompt_interval", "Hourly"))
            self.settings.set("js8_enforcement_mode", data.get("js8_enforcement_mode", "On Schedule Change"))
            self.settings.set("js8_prompt_interval", data.get("js8_prompt_interval", "Hourly"))
            self.settings.set("flrig_port", data.get("flrig_port", 12345))
            self.settings.set("fldigi_host", data.get("fldigi_host", self._resolved_fldigi_host_value(data)))
            self.settings.set("fldigi_port", data.get("fldigi_port", 7362))
            self.settings.set("js8_host", data.get("js8_host", "127.0.0.1"))
            self.settings.set("js8_port", data["js8_port"])
            self.settings.set("js8_offset_hz", data.get("js8_offset_hz", 0))
            self.settings.set("primary_js8_groups", data["primary_js8_groups"])
            self.settings.set("js8_directed_path", data["js8_directed_path"])
            self.settings.set("js8_forms_path", data.get("js8_forms_path", ""))
            self.settings.set("path_js8call", data.get("path_js8call", ""))
            self.settings.set("path_js8spotter", data.get("path_js8spotter", ""))
            self.settings.set("path_commstat", data.get("path_commstat", ""))
            self.settings.set(
                "js8_inbox_mark_retrieved_sync",
                data.get("js8_inbox_mark_retrieved_sync", False),
            )
            self.settings.set("message_paths", data.get("message_paths", {}))
            self.settings.set("gpg_verify_flamp_k2s_enabled", data.get("gpg_verify_flamp_k2s_enabled", False))
            self.settings.set("hash_verify_flamp_k2s_enabled", data.get("hash_verify_flamp_k2s_enabled", True))
            self.settings.set("gpg_executable_path", data.get("gpg_executable_path", ""))
            self.settings.set("gpg_trusted_signers", data.get("gpg_trusted_signers", []))
            self.settings.set("trusted_file_hashes", data.get("trusted_file_hashes", []))
            self.settings.set("varac_path", data.get("varac_path", ""))
            self.settings.set("varac_db_path", data.get("varac_db_path", ""))
            self.settings.set("varac_ini_path", data.get("varac_ini_path", ""))
            self.settings.set("varac_launch_cmd", data.get("varac_launch_cmd", ""))
            self.settings.set("varac_outbox_dir", data.get("varac_outbox_dir", ""))
            self.settings.set("varac_bbs_dir", data.get("varac_bbs_dir", ""))
            self.settings.set("varac_bbs_archive_dir", data.get("varac_bbs_archive_dir", ""))
            self.settings.set("varac_bbs_auto_archive_enabled", data.get("varac_bbs_auto_archive_enabled", False))
            self.settings.set("varac_bbs_auto_archive_days", data.get("varac_bbs_auto_archive_days", 14))
            self.settings.set("varac_bbs_enabled", data.get("varac_bbs_enabled", False))
            self.settings.set(
                "varac_bbs_limit_access_enabled",
                data.get("varac_bbs_limit_access_enabled", False),
            )
            self.settings.set("varac_bbs_announce_enabled", data.get("varac_bbs_announce_enabled", False))
            self.settings.set("varac_bbs_allowed_callsigns", data.get("varac_bbs_allowed_callsigns", ""))
            self.settings.set("varac_guard_enabled", data.get("varac_guard_enabled", False))
            self.settings.set("varac_guard_mode", data.get("varac_guard_mode", "Log only"))
            self.settings.set("varac_guard_retry_seconds", data.get("varac_guard_retry_seconds", 120))
            self.settings.set("varac_guard_quarantine_dir", data.get("varac_guard_quarantine_dir", ""))
            self.settings.set("varac_bbs_vault_enabled", data.get("varac_bbs_vault_enabled", False))
            self.settings.set("varac_bbs_vault_managed_root", data.get("varac_bbs_vault_managed_root", ""))
            self.settings.set("varac_bbs_vault_default_location_id", data.get("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID))
            self.settings.set("varac_bbs_vault_global_code_policy", data.get("varac_bbs_vault_global_code_policy", DEFAULT_GLOBAL_CODE_POLICY))
            self.settings.set("varac_bbs_vault_trigger_mode", data.get("varac_bbs_vault_trigger_mode", DEFAULT_TRIGGER_MODE))
            self.settings.set("varac_bbs_vault_return_mode", data.get("varac_bbs_vault_return_mode", DEFAULT_RETURN_MODE))
            self.settings.set("varac_bbs_vault_failed_attempt_limit", data.get("varac_bbs_vault_failed_attempt_limit", DEFAULT_FAILED_ATTEMPT_LIMIT))
            self.settings.set("varac_bbs_vault_failed_attempt_window_seconds", data.get("varac_bbs_vault_failed_attempt_window_seconds", DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS))
            self.settings.set("varac_bbs_vault_cooldown_seconds", data.get("varac_bbs_vault_cooldown_seconds", DEFAULT_COOLDOWN_SECONDS))
            self.settings.set("varac_bbs_vault_idle_timeout_seconds", data.get("varac_bbs_vault_idle_timeout_seconds", DEFAULT_IDLE_TIMEOUT_SECONDS))
            self.settings.set("varac_bbs_vault_flamp_enabled", data.get("varac_bbs_vault_flamp_enabled", False))
            self.settings.set("varac_bbs_vault_flamp_relay_dir", data.get("varac_bbs_vault_flamp_relay_dir", ""))
            self.settings.set("varac_bbs_vault_locations_v1", data.get("varac_bbs_vault_locations_v1", []))
            self.settings.set("varac_bbs_vault_runtime_state_v1", data.get("varac_bbs_vault_runtime_state_v1", {}))
            self.settings.set("varac_bbs_vault_last_summary", data.get("varac_bbs_vault_last_summary", ""))
            self.settings.set("sop_export_preamble", data.get("sop_export_preamble", ""))
            self.settings.set("sop_export_postamble", data.get("sop_export_postamble", ""))
            self.settings.set("fldigi_log_path", data.get("fldigi_log_path", ""))
            self.settings.set("fldigi_checkin_dir", data.get("fldigi_checkin_dir", ""))
            self.settings.set("custom_tool_items", data.get("custom_tool_items", []))
            self.settings.set("launch_control_items", data.get("launch_control_items", []))
            self.settings.set("launch_control_enabled", data.get("launch_control_enabled", True))
            self.settings.set("launch_control_migrated_v1", data.get("launch_control_migrated_v1", True))
            self.settings.set("launch_readiness_timeout_sec", data.get("launch_readiness_timeout_sec", 30))
            for prog_name, meta in self.PROGRAMS.items():
                path_key = meta["setting_key"]
                auto_key = meta["autostart_key"]
                if path_key:
                    self.settings.set(path_key, data.get(path_key, ""))
                if auto_key:
                    self.settings.set(auto_key, data.get(auto_key, False))
            self.settings.set("autostart_js8call", data.get("autostart_js8call", False))
            self.settings.set("operating_groups", data.get("operating_groups", []))
            self.settings.set("local_net_profiles", data.get("local_net_profiles", []))
        elif hasattr(self.settings, "_data"):
            # Fallback: update the internal dict only
            self.settings._data = data  # type: ignore[attr-defined]

        log.info("SettingsTab: settings saved.")
        self._ensure_fldigi_checkin_files()
        if show_message:
            QMessageBox.information(self, "Settings", "Settings saved.")

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
        compact = width < 640
        very_compact = width < 480

        for btn in (self.open_logs_btn, self.open_log_folder_btn, self.export_diag_btn):
            try:
                self.logging_actions_grid.removeWidget(btn)
            except Exception:
                pass

        for col in range(4):
            self.logging_actions_grid.setColumnStretch(col, 0)
        self.logging_actions_grid.setColumnStretch(3, 1)

        if very_compact:
            self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0, 1, 2)
            self.logging_actions_grid.addWidget(self.open_log_folder_btn, 1, 0)
            self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 1)
            return

        self.logging_actions_grid.addWidget(self.open_logs_btn, 0, 0)
        self.logging_actions_grid.addWidget(self.open_log_folder_btn, 0, 1)
        try:
            if compact:
                self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 0, 1, 2)
            else:
                self.logging_actions_grid.addWidget(self.export_diag_btn, 0, 2)
        except Exception:
            self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 0, 1, 2)

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
            self.varac_guard_quarantine_dir_edit,
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
            self.varac_guard_mode_combo,
            self.varac_guard_retry_combo,
        ]
        for combo in combos:
            combo.currentIndexChanged.connect(self._mark_settings_dirty)

        checks = [
            self.use_scheduler_chk,
            self.use_flrig_chk,
            self.use_fldigi_chk,
            self.use_flmsg_chk,
            self.use_flamp_chk,
            self.use_js8call_chk,
            self.use_js8spotter_chk,
            self.use_commstat_chk,
            self.use_varac_chk,
            self.js8_mark_retrieved_chk,
            self.varac_bbs_auto_archive_chk,
            self.varac_guard_enabled_chk,
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
        self._refresh_section_nav_health()

    def _on_sop_export_text_changed(self) -> None:
        self._mark_settings_dirty()
        if self._loading_settings:
            return
        self._refresh_section_titles()

    def _set_save_button_state(self, role: str) -> None:
        theme = resolve_theme(self.settings)
        self.save_btn.setStyleSheet(button_style(role, theme))

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

    def _is_launch_item_configured(self, name: str) -> bool:
        if self._custom_tool_command(name):
            return True
        if name == "VarAC":
            path_val = self.varac_path_edit.text().strip() if hasattr(self, "varac_path_edit") else ""
            launch_cmd = self.varac_launch_cmd_edit.text().strip() if hasattr(self, "varac_launch_cmd_edit") else ""
            return bool(path_val or launch_cmd)
        if name == "JS8Call":
            return bool(self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else "")
        if name == "JS8Spotter":
            return bool(self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else "")
        if name == "CommStat":
            return bool(self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else "")
        edit = self.path_edits.get(name)
        return bool(edit and edit.text().strip())

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
        self.launch_order_up_btn.setEnabled(bool(can_move and row > 0))
        self.launch_order_down_btn.setEnabled(bool(can_move and row < self.launch_control_table.rowCount() - 1))
        self.launch_reset_order_btn.setEnabled(has_rows)
        self.launch_configured_now_btn.setEnabled(has_rows and not self.launch_orchestrator.is_active())
        self.launch_stop_btn.setEnabled(self.launch_orchestrator.is_active())

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
        started = self.launch_orchestrator.start_manual_sequence(self._launch_items_cache)
        if not started:
            QMessageBox.information(self, "Launch Control", "No enabled configured applications to launch.")
            return
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
            QMessageBox.information(
                self,
                "Launch Summary",
                (
                    f"Launched: {launched}\n"
                    f"Already running: {already_running}\n"
                    f"Failed: {failed}\n"
                    f"Timeout: {timeout}\n"
                    f"Blocked (self-target): {blocked_self}\n"
                    f"Cancelled: {'Yes' if cancelled else 'No'}"
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
            return bool(self._status_service.program_is_running(program_name))
        except Exception:
            return False

    def _find_process_exe(self, program_name: str) -> Optional[str]:
        try:
            return self._status_service.find_process_exe(program_name)
        except Exception:
            return None

    def _refresh_running_status(self):
        _perf_t0 = time.perf_counter()
        theme = resolve_theme(self.settings)
        self._rebuild_status_indicators()
        port_override: Optional[int] = None
        flrig_port_override: Optional[int] = None
        fldigi_host_override: Optional[str] = None
        fldigi_port_override: Optional[int] = None
        try:
            host_txt = self.js8_host_edit.text().strip() if hasattr(self, "js8_host_edit") else ""
            host_override = host_txt or "127.0.0.1"
        except Exception:
            host_override = "127.0.0.1"
        try:
            txt = self.js8_port_edit.text().strip() if hasattr(self, "js8_port_edit") else ""
            port_override = int(txt) if txt else None
        except Exception:
            port_override = None
        try:
            txt = self.flrig_port_edit.text().strip() if hasattr(self, "flrig_port_edit") else ""
            flrig_port_override = int(txt) if txt else None
        except Exception:
            flrig_port_override = None
        try:
            host_txt = self.fldigi_host_edit.text().strip() if hasattr(self, "fldigi_host_edit") else ""
            fldigi_host_override = host_txt or self._resolved_fldigi_host_value()
        except Exception:
            fldigi_host_override = self._resolved_fldigi_host_value()
        try:
            txt = self.fldigi_port_edit.text().strip() if hasattr(self, "fldigi_port_edit") else ""
            fldigi_port_override = int(txt) if txt else None
        except Exception:
            fldigi_port_override = None
        snapshot = self._status_service.status_snapshot(
            port_override=port_override,
            host_override=host_override,
            flrig_port_override=flrig_port_override,
            fldigi_host_override=fldigi_host_override,
            fldigi_port_override=fldigi_port_override,
        )
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
        self._update_readiness_summary_card()
        emit_span(
            "settings.refresh_running_status",
            (time.perf_counter() - _perf_t0) * 1000.0,
            settings=self.settings,
            min_ms=5.0,
        )

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
            self._rebuild_status_indicators()
            self._refresh_running_status()
            self._update_launch_selected_state()
            self._update_op_group_action_buttons()
            self._update_local_net_action_buttons()
            self._set_save_button_state("info" if self._settings_dirty else "success")
            if hasattr(self, "copy_readiness_summary_btn"):
                self.copy_readiness_summary_btn.setStyleSheet(button_style("secondary", theme))
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
            if hasattr(self, "sections_nav_list"):
                self._apply_sections_nav_style()
                self._refresh_section_nav_health()
            for btn in getattr(self, "_context_help_buttons", []):
                try:
                    btn.setStyleSheet(button_style("secondary", theme))
                except Exception:
                    continue
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
            return bool(self._status_service.js8_api_reachable(port_override=port_override, host_override=host_override))
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
            self._update_gpg_sign_button_state()

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
        ok, msg, resolved = gpg_available(self._current_gpg_path())
        if ok:
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

        # Persist path without calling write/save
        if hasattr(self.settings, "set"):
            self.settings.set("js8_directed_path", str(path))
        else:
            data = self.settings.all()
            data["js8_directed_path"] = str(path)
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]

        log.info("JS8Call DIRECTED.TXT path saved: %s", path)
        self._settings_dirty = False
        self._set_save_button_state("success")

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
        if hasattr(self.settings, "set"):
            self.settings.set("js8_forms_path", fn)
        else:
            data = self.settings.all()
            data["js8_forms_path"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        log.info("JS8Spotter forms path saved: %s", fn)
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_js8call_install_path(self):
        start = self.js8call_path_edit.text().strip() if hasattr(self, "js8call_path_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select JS8Call install folder", start)
        if not fn:
            return
        self.js8call_path_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("path_js8call", fn)
        else:
            data = self.settings.all()
            data["path_js8call"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_js8spotter_launch_path(self):
        start = self.js8spotter_path_edit.text().strip() if hasattr(self, "js8spotter_path_edit") else ""
        fn, _ = QFileDialog.getOpenFileName(self, "Select JS8Spotter launch path", start)
        if not fn:
            return
        self.js8spotter_path_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("path_js8spotter", fn)
        else:
            data = self.settings.all()
            data["path_js8spotter"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_commstat_launch_path(self):
        start = self.commstat_path_edit.text().strip() if hasattr(self, "commstat_path_edit") else ""
        fn, _ = QFileDialog.getOpenFileName(self, "Select CommStat launch path", start)
        if not fn:
            return
        self.commstat_path_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("path_commstat", fn)
        else:
            data = self.settings.all()
            data["path_commstat"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_msg_path(self, origin: str, edit: QLineEdit):
        """
        Prompt for message paths used by Message Viewer (VarAC/FLMSG/FLAMP).
        """
        fn = QFileDialog.getExistingDirectory(self, f"Select {origin.upper()} folder")
        if not fn:
            return
        edit.setText(fn)
        data = self.settings.all() if hasattr(self.settings, "all") else {}
        if isinstance(data, dict):
            mp = data.get("message_paths", {}) or {}
            mp[origin] = fn
            data["message_paths"] = mp
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        if hasattr(self.settings, "set"):
            mp = self.settings.get("message_paths", {}) or {}
            mp[origin] = fn
            self.settings.set("message_paths", mp)
        self._settings_dirty = False
        self._set_save_button_state("success")

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
        data = self.settings.all() if hasattr(self.settings, "all") else {}
        if isinstance(data, dict):
            data["varac_path"] = fn
            data["varac_db_path"] = str(Path(fn) / "VarAC.db")
            if hasattr(self, "varac_ini_path_edit"):
                data["varac_ini_path"] = self.varac_ini_path_edit.text().strip()
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        if hasattr(self.settings, "set"):
            self.settings.set("varac_path", fn)
            self.settings.set("varac_db_path", str(Path(fn) / "VarAC.db"))
            if hasattr(self, "varac_ini_path_edit"):
                self.settings.set("varac_ini_path", self.varac_ini_path_edit.text().strip())
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_varac_ini_path(self):
        start = self.varac_ini_path_edit.text().strip() if hasattr(self, "varac_ini_path_edit") else ""
        if not start and hasattr(self, "varac_path_edit"):
            start = self.varac_path_edit.text().strip()
        fn, _ = QFileDialog.getOpenFileName(self, "Select VarAC.ini file", start, "INI Files (*.ini);;All Files (*)")
        if not fn:
            return
        self.varac_ini_path_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("varac_ini_path", fn)
        else:
            data = self.settings.all()
            data["varac_ini_path"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        try:
            self._varac_bbs_ini_sync_state = varac_ini_sync_state_to_json(get_varac_ini_sync_state(fn))
        except Exception:
            self._varac_bbs_ini_sync_state = ""
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_varac_outbox_dir(self):
        start = self.varac_outbox_dir_edit.text().strip() if hasattr(self, "varac_outbox_dir_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC Outbox directory", start)
        if not fn:
            return
        self.varac_outbox_dir_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("varac_outbox_dir", fn)
        else:
            data = self.settings.all()
            data["varac_outbox_dir"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_varac_bbs_dir(self):
        start = self.varac_bbs_dir_edit.text().strip() if hasattr(self, "varac_bbs_dir_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC BBS directory", start)
        if not fn:
            return
        self.varac_bbs_dir_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("varac_bbs_dir", fn)
        else:
            data = self.settings.all()
            data["varac_bbs_dir"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

    def _choose_varac_bbs_archive_dir(self):
        start = self.varac_bbs_archive_dir_edit.text().strip() if hasattr(self, "varac_bbs_archive_dir_edit") else ""
        fn = QFileDialog.getExistingDirectory(self, "Select VarAC BBS archive directory", start)
        if not fn:
            return
        self.varac_bbs_archive_dir_edit.setText(fn)
        if hasattr(self.settings, "set"):
            self.settings.set("varac_bbs_archive_dir", fn)
        else:
            data = self.settings.all()
            data["varac_bbs_archive_dir"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

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
        if hasattr(self.settings, "set"):
            self.settings.set("varac_guard_quarantine_dir", fn)
        else:
            data = self.settings.all()
            data["varac_guard_quarantine_dir"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

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
        bbs_dir = str(bbs_cfg.get("bbs_directory", "") or "").strip()
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
        if hasattr(self.settings, "set"):
            self.settings.set("fldigi_checkin_dir", fn)
        else:
            data = self.settings.all()
            data["fldigi_checkin_dir"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._ensure_fldigi_checkin_files()

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
        if hasattr(self.settings, "set"):
            self.settings.set("fldigi_log_path", fn)
        else:
            data = self.settings.all()
            data["fldigi_log_path"] = fn
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
        self._settings_dirty = False
        self._set_save_button_state("success")

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
        main_path = str(Path(base) / "main_checkins.txt")
        late_path = str(Path(base) / "new-late_checkins.txt")
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
        main_path = folder / "main_checkins.txt"
        qru_path = folder / "qru_checkins.txt"
        late_path = folder / "new-late_checkins.txt"
        all_path = folder / "all_checkins.txt"
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
