from __future__ import annotations

import datetime
import sqlite3
import platform
import subprocess
import json
from pathlib import Path
from typing import Any, List, Dict, Optional, Set, Tuple

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QToolButton,
    QLineEdit,
    QTableWidget,
    QTableWidgetItem,
    QComboBox,
    QHeaderView,
    QMessageBox,
    QCheckBox,
    QApplication,
    QFileDialog,
    QGroupBox,
    QMenu,
    QInputDialog,
)
from PySide6.QtGui import QAction, QColor

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.logger import log
from freqinout.core.sop_manager import SOPManager
from freqinout.utils.timezones import get_timezone
from freqinout.gui.theme import resolve_theme, button_style
from freqinout.gui.qsy_helper import (
    load_operating_groups as qsy_load_operating_groups,
    snapshot_operating_groups as qsy_snapshot_operating_groups,
    build_qsy_options,
    refresh_qsy_combo,
    selected_qsy_meta,
    current_scheduler_freq,
    perform_qsy,
    get_suspend_until,
    set_suspend_until,
    suspend_active,
    scheduler_enabled,
)


DAY_OPTIONS = [
    "ALL",
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]
DAY_CANON = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

BAND_OPTIONS = [
    "20M",
    "40M",
    "80M",
    "--",
    "2M",
    "6M",
    "10M",
    "12M",
    "15M",
    "17M",
    "30M",
    "60M",
]

MODE_OPTIONS = ["Digi", "SSB"]


# Radio program metadata (must match SettingsTab keys)
PROGRAMS = {
    "FLRig": {
        "path_key": "path_flrig",
        "autostart_key": "autostart_flrig",
        "default_cmd": "flrig",
    },
    "FLDigi": {
        "path_key": "path_fldigi",
        "autostart_key": "autostart_fldigi",
        "default_cmd": "fldigi",
    },
    "FLMsg": {
        "path_key": "path_flmsg",
        "autostart_key": "autostart_flmsg",
        "default_cmd": "flmsg",
    },
    "FLAmp": {
        "path_key": "path_flamp",
        "autostart_key": "autostart_flamp",
        "default_cmd": "flamp",
    },
    "JS8Call": {
        "path_key": "path_js8call",
        "autostart_key": "autostart_js8call",
        "default_cmd": "js8call",
    },
}


class DailyScheduleTab(QWidget):
    schedule_saved = Signal()
    """
    HF Frequency Schedule tab.

    This tab is intentionally very similar to the Net Schedule tab, with
    the following differences:

      - The 'Net Name' column is renamed 'Group Name'.
      - The 'Day' column allows 'ALL' in addition to each day of week.
        ('ALL' means the entry is used every day.)
      - No limit to the number of rows.

    Data is stored in settings/DB; offsets/comments are no longer used.
    """

    # Column indices
    COL_SELECT = 0
    COL_DAY = 1
    COL_SOURCE = 2
    COL_GROUP = 3
    COL_MODE = 4
    COL_BAND = 5
    COL_FREQ = 6
    COL_START = 7
    COL_END = 8
    COL_AUTOTUNE = 9

    # Resource table column indices
    RES_COL_SELECT = 0
    RES_COL_SET = 1
    RES_COL_DAY = 2
    RES_COL_GROUP = 3
    RES_COL_MODE = 4
    RES_COL_BAND = 5
    RES_COL_FREQ = 6
    RES_COL_START = 7
    RES_COL_END = 8
    RES_COL_SOURCE = 9
    RES_COL_UPDATED = 10
    RES_COL_CONFLICT = 11

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._status_service = SoftwareStatusService(self.settings)
        try:
            self.settings.reload()
        except Exception:
            pass
        self.operating_groups: List[Dict] = self._load_operating_groups()
        self._operating_groups_sig = self._snapshot_operating_groups(self.operating_groups)
        default_mode = (self.settings.get("display_time_mode", "LOCAL") or "LOCAL").upper()
        self._show_local: bool = default_mode != "UTC"
        self._raw_schedule: List[Dict] = []
        self._dirty: bool = False
        self._suspend_dirty_tracking: bool = False
        self._saved_rows_signature: str = ""

        self._clock_timer: Optional[QTimer] = None
        self._sop_panel_timer: Optional[QTimer] = None
        self._suppress_autostart: bool = True  # avoid auto-start during initial load
        self._qsy_options: Dict[str, Dict] = {}
        self._sop_manager = SOPManager()
        self._last_sop_panel_refresh_ts: float = 0.0
        self._schedule_resource_rows: List[Dict[str, Any]] = []
        self._resource_view_rows: List[Dict[str, Any]] = []
        self._schedule_resource_token: Tuple[Any, ...] | None = None
        self._sop_profile_lookup: Dict[int, Dict[str, Any]] = {}
        self._sop_group_profile_choice: Dict[str, int] = {}
        self._hidden_sop_overlay_keys: Set[str] = set()

        self._build_ui()
        self._refresh_qsy_options()
        self._load_schedule()
        self._setup_clock_timer()
        self._setup_sop_panel_timer()
        self._refresh_sop_profiles_panel(force=True)
        self._refresh_schedule_resources(force=True)
        self._suppress_autostart = False

    def _format_freq(self, val) -> str:
        try:
            return f"{float(val):.3f}"
        except Exception:
            return str(val) if val is not None else ""

    # ---------------- UI ---------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>HF Frequency Schedule</h3>"))
        header.addStretch()

        # UTC / Local labels like net_schedule_tab
        self.utc_label = QLabel()
        self.local_label = QLabel()
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        self.time_toggle_btn = QPushButton("Showing: Local" if self._show_local else "Showing: UTC")
        theme = resolve_theme(self.settings)
        self.time_toggle_btn.setStyleSheet(button_style("primary", theme))
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        header.addWidget(self.time_toggle_btn)
        self.effective_source_label = QLabel("Runtime Source: --")
        self.effective_source_label.setToolTip("Shows which runtime schedule source is currently driving decisions.")
        layout.addLayout(header)

        # QSY controls row (right aligned under time bar)
        qsy_row = QHBoxLayout()
        qsy_row.addStretch()
        self.qsy_combo = QComboBox()
        self.qsy_combo.currentIndexChanged.connect(self._update_qsy_button_enabled)
        qsy_row.addWidget(self.qsy_combo)
        self.suspend_btn = QPushButton("QSY")
        self.suspend_btn.clicked.connect(self._on_suspend_clicked)
        qsy_row.addWidget(self.suspend_btn)
        layout.addLayout(qsy_row)

        # SOP status panel (merged runtime + issues)
        self.sop_runtime_box = QGroupBox("SOP Schedule Status (HF)")
        sop_layout = QVBoxLayout(self.sop_runtime_box)
        sop_layout.setContentsMargins(6, 6, 6, 6)
        sop_layout.setSpacing(4)
        self.sop_runtime_summary_label = QLabel("Now: -- | Next: --")
        self.sop_runtime_summary_label.setWordWrap(True)
        sop_layout.addWidget(self.sop_runtime_summary_label)
        self.sop_profile_summary_label = QLabel("HF SOP Sets: --")
        self.sop_profile_summary_label.setWordWrap(True)
        sop_layout.addWidget(self.sop_profile_summary_label)
        self.sop_status_table = QTableWidget()
        self.sop_status_table.setColumnCount(6)
        self.sop_status_table.setHorizontalHeaderLabels(
            [
                "Group Name",
                "SOP Name",
                "Status",
                "Issue Summary",
                "Open SOP",
                "Activate/Deactivate",
            ]
        )
        self.sop_status_table.verticalHeader().setVisible(False)
        self.sop_status_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.sop_status_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.sop_status_table.setFocusPolicy(Qt.NoFocus)
        status_hv = self.sop_status_table.horizontalHeader()
        status_hv.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        status_hv.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        status_hv.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        status_hv.setSectionResizeMode(3, QHeaderView.Stretch)
        status_hv.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        status_hv.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        sop_layout.addWidget(self.sop_status_table)
        self.sop_runtime_box.setMaximumHeight(260)
        layout.addWidget(self.sop_runtime_box)

        # Active schedule section
        active_header = QHBoxLayout()
        active_header.addWidget(QLabel("<h3>Active Schedule</h3>"))
        active_header.addWidget(self.effective_source_label)
        active_header.addStretch()
        layout.addLayout(active_header)

        # Active schedule table
        self.table = QTableWidget()
        self._set_headers()

        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_SELECT, QHeaderView.ResizeToContents)
        hv.setMinimumSectionSize(50)
        hv.setDefaultSectionSize(100)
        for col in (
            self.COL_DAY,
            self.COL_SOURCE,
            self.COL_GROUP,
            self.COL_MODE,
            self.COL_BAND,
            self.COL_FREQ,
            self.COL_START,
            self.COL_END,
            self.COL_AUTOTUNE,
        ):
            hv.setSectionResizeMode(col, QHeaderView.Stretch)

        layout.addWidget(self.table)

        # Buttons row
        btn_row = QHBoxLayout()
        self.add_row_btn = QPushButton("Add Row")
        self.del_row_btn = QPushButton("Delete Selected")
        self.move_to_resources_btn = QPushButton("Move Selected to Resources")
        self.save_btn = QPushButton("Save HF Schedule")
        self.export_btn = QPushButton("Export HF Schedule")
        btn_row.addWidget(self.add_row_btn)
        btn_row.addWidget(self.del_row_btn)
        btn_row.addWidget(self.move_to_resources_btn)
        btn_row.addWidget(self.export_btn)
        btn_row.addStretch()
        btn_row.addWidget(self.save_btn)
        layout.addLayout(btn_row)

        resources_header = QHBoxLayout()
        resources_header.addWidget(QLabel("<h3>Schedule Resources</h3>"))
        resources_header.addStretch()
        layout.addLayout(resources_header)

        filters_row = QHBoxLayout()
        filters_row.addWidget(QLabel("Set:"))
        self.resources_set_combo = QComboBox()
        self.resources_set_combo.addItem("All", "All")
        filters_row.addWidget(self.resources_set_combo)
        filters_row.addWidget(QLabel("Filter:"))
        self.resources_group_filter = QLineEdit()
        self.resources_group_filter.setPlaceholderText("Search set/group/band/frequency...")
        filters_row.addWidget(self.resources_group_filter, 1)
        filters_row.addWidget(QLabel("Apply To:"))
        self.resources_apply_target_combo = QComboBox()
        self.resources_apply_target_combo.addItem("HF Active Schedule", "hf")
        self.resources_apply_target_combo.addItem("SOP Layer", "sop")
        filters_row.addWidget(self.resources_apply_target_combo)
        self.add_to_schedule_btn = QToolButton()
        self.add_to_schedule_btn.setPopupMode(QToolButton.MenuButtonPopup)
        add_menu = QMenu(self.add_to_schedule_btn)
        self.add_selected_resource_action = QAction("Apply Selected Rows", self)
        self.add_filtered_resource_action = QAction("Apply Filtered Rows", self)
        add_menu.addAction(self.add_selected_resource_action)
        add_menu.addAction(self.add_filtered_resource_action)
        self.add_to_schedule_btn.setMenu(add_menu)
        self.add_to_schedule_default_action = QAction("Apply to Selected Target", self)
        self.add_to_schedule_btn.setDefaultAction(self.add_to_schedule_default_action)
        filters_row.addWidget(self.add_to_schedule_btn)
        self.resources_resolve_btn = QPushButton("Resolve Conflicts")
        filters_row.addWidget(self.resources_resolve_btn)
        self.resources_refresh_btn = QPushButton("Refresh")
        filters_row.addWidget(self.resources_refresh_btn)
        layout.addLayout(filters_row)

        self.resources_table = QTableWidget()
        self.resources_table.setColumnCount(self.RES_COL_CONFLICT + 1)
        self.resources_table.setHorizontalHeaderLabels(
            [
                "Select",
                "Set",
                "Day",
                "Group Name",
                "Mode",
                "Band",
                "Freq (MHz)",
                "Start",
                "End",
                "Source",
                "Updated (UTC)",
                "Conflict",
            ]
        )
        self.resources_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.resources_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.resources_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.resources_table.verticalHeader().setVisible(False)
        self.resources_table.setSortingEnabled(True)
        resources_hv = self.resources_table.horizontalHeader()
        resources_hv.setSectionResizeMode(self.RES_COL_SELECT, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_SET, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_DAY, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_GROUP, QHeaderView.Stretch)
        resources_hv.setSectionResizeMode(self.RES_COL_MODE, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_BAND, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_FREQ, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_START, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_END, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_SOURCE, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_UPDATED, QHeaderView.ResizeToContents)
        resources_hv.setSectionResizeMode(self.RES_COL_CONFLICT, QHeaderView.Stretch)
        layout.addWidget(self.resources_table)

        # Signals
        self.add_row_btn.clicked.connect(self._add_row)
        self.del_row_btn.clicked.connect(self._delete_selected_rows)
        self.move_to_resources_btn.clicked.connect(self._move_selected_schedule_rows_to_resources)
        self.save_btn.clicked.connect(self._save_schedule)
        self.export_btn.clicked.connect(self._export_schedule)
        self.table.itemSelectionChanged.connect(self._update_delete_button_state)
        self.table.itemChanged.connect(self._on_table_item_changed)
        self.resources_set_combo.currentIndexChanged.connect(self._populate_schedule_resources_table)
        self.resources_group_filter.textChanged.connect(self._populate_schedule_resources_table)
        self.resources_apply_target_combo.currentIndexChanged.connect(self._update_resource_action_state)
        self.resources_table.itemSelectionChanged.connect(self._update_resource_action_state)
        self.add_to_schedule_default_action.triggered.connect(self._add_resources_default)
        self.add_selected_resource_action.triggered.connect(self._add_selected_resources_to_schedule)
        self.add_filtered_resource_action.triggered.connect(self._add_filtered_resources_to_schedule)
        self.resources_resolve_btn.clicked.connect(self._resolve_resource_conflicts)
        self.resources_refresh_btn.clicked.connect(lambda: self._refresh_schedule_resources(force=True))

        # Initialize clock labels once
        self._update_clock_labels()
        self._update_effective_source_label()
        self._update_suspend_state()
        self._apply_theme()
        self._update_delete_button_state()
        self._update_resource_action_state()

    def _load_operating_groups(self) -> List[Dict]:
        return qsy_load_operating_groups(self.settings)

    def _snapshot_operating_groups(self, og_list: List[Dict]) -> str:
        return qsy_snapshot_operating_groups(og_list)

    # ---------------- CLOCK / TIMEZONE (shared logic) ---------------- #

    def _setup_clock_timer(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(lambda: (self._update_clock_labels(), self._update_suspend_state()))
        self._clock_timer.start(1000)

    def _setup_sop_panel_timer(self) -> None:
        self._sop_panel_timer = QTimer(self)
        self._sop_panel_timer.timeout.connect(lambda: self._refresh_sop_profiles_panel(force=False))
        self._sop_panel_timer.timeout.connect(lambda: self._refresh_schedule_resources(force=False))
        self._sop_panel_timer.start(30_000)

    def _refresh_group_band_cells(self):
        """
        Update group/band combos and mode/frequency cells in-place based on refreshed operating_groups.
        """
        for r in range(self.table.rowCount()):
            if self._is_sop_overlay_row(r):
                continue
            group_combo = self.table.cellWidget(r, self.COL_GROUP)
            band_combo = self.table.cellWidget(r, self.COL_BAND)
            # repopulate group options
            if isinstance(group_combo, QComboBox):
                current_group = group_combo.currentText()
                group_combo.blockSignals(True)
                group_combo.clear()
                group_names = sorted({g.get("group", "") for g in self.operating_groups if g.get("group")})
                group_combo.addItems(group_names)
                if current_group in group_names:
                    group_combo.setCurrentText(current_group)
                group_combo.blockSignals(False)
            # repopulate band options based on selected group
            if isinstance(band_combo, QComboBox):
                current_band = band_combo.currentText()
                self._populate_band_combo(band_combo, self._get_combo_value(r, self.COL_GROUP, ""))
                if current_band and band_combo.findText(current_band) >= 0:
                    band_combo.setCurrentText(current_band)
            # refresh mode/freq cells
            self._update_mode_freq(r)
        self._update_clock_labels()

    def _current_timezone(self) -> tuple[str, datetime.tzinfo]:
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        return tz_name, tz
    
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
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")
        self._update_time_toggle_style()
        self._update_effective_source_label()

    def _update_time_toggle_style(self, theme: Optional[Dict[str, str]] = None) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        role = "info" if not self._show_local else "muted"
        self.time_toggle_btn.setStyleSheet(button_style(role, theme))

    def _update_effective_source_label(self, theme: Optional[Dict[str, str]] = None) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        muted = str(theme.get("text_muted", "#888"))
        source_text = "Runtime Source: --"
        style = f"color: {muted};"
        tip = ""
        status: Optional[Dict[str, Any]] = None
        try:
            sched = getattr(self.window(), "scheduler", None)
            if sched and hasattr(sched, "get_status_summary"):
                status = sched.get_status_summary()
                source = str(status.get("source") or "").strip().upper()
                net_kind = str(status.get("net_kind") or "").strip()
                source_reason_detail = str(status.get("source_reason_detail") or "").strip()
                sop_contention = bool(status.get("sop_contention"))
                sop_profiles = [str(x).strip() for x in (status.get("sop_contention_profiles") or []) if str(x).strip()]
                sop_selected = str(status.get("sop_selected_profile") or "").strip()
                next_source = str(status.get("next_source") or "").strip().upper()
                next_net_kind = str(status.get("next_net_kind") or "").strip()
                next_source_change = bool(status.get("next_source_change"))
                source_key = source
                if source_key not in {"NET", "SOP", "HF"}:
                    if "NET" in source_key:
                        source_key = "NET"
                    elif "SOP" in source_key:
                        source_key = "SOP"
                    elif "HF" in source_key:
                        source_key = "HF"
                if source_key == "SOP":
                    source_text = "Runtime Source: SOP Layer"
                    style = f"font-weight: 600; color: {theme.get('warning', '#C99700')};"
                    tip = "SOP Layer currently overrides the baseline HF schedule."
                    if net_kind:
                        tip = f"{tip}\nSOP runtime kind: {net_kind}"
                    if sop_contention:
                        others = [p for p in sop_profiles if p and p != sop_selected]
                        if others:
                            source_text += " (Contention)"
                            tip = f"SOP contention: selected {sop_selected or 'winner'} over {', '.join(others[:4])}."
                        else:
                            source_text += " (Contention)"
                            tip = "SOP contention detected across active profiles."
                    if source_reason_detail:
                        tip = f"{tip}\nSelection: {source_reason_detail}"
                elif source_key == "NET":
                    source_text = "Runtime Source: Net Schedule"
                    style = f"font-weight: 600; color: {theme.get('info', '#1E88E5')};"
                    tip = "Active net schedule has highest precedence."
                    if net_kind:
                        tip = f"{tip}\nActive net kind: {net_kind}"
                    if source_reason_detail:
                        tip = f"{tip}\nSelection: {source_reason_detail}"
                elif source_key == "HF":
                    source_text = "Runtime Source: HF Schedule"
                    style = f"color: {theme.get('text', '#111')};"
                    tip = "Baseline HF schedule is active."
                    if net_kind:
                        tip = f"{tip}\nRuntime kind: {net_kind}"
                    if source_reason_detail:
                        tip = f"{tip}\nSelection: {source_reason_detail}"
                if next_source_change and next_source and next_source != source_key:
                    next_label = next_net_kind or next_source
                    tip = (f"{tip}\n" if tip else "") + f"Next source transition: {next_label}."
        except Exception:
            pass
        self.effective_source_label.setText(source_text)
        self.effective_source_label.setStyleSheet(style)
        self.effective_source_label.setToolTip(tip)
        self._update_runtime_source_summary(status=status, theme=theme)

    @staticmethod
    def _source_label(source: str, net_kind: str = "") -> str:
        src = str(source or "").strip().upper()
        kind = str(net_kind or "").strip()
        if kind:
            return kind
        if src == "NET":
            return "Net Schedule"
        if src == "SOP":
            return "SOP Layer"
        if src == "HF":
            return "HF Schedule"
        return "None"

    def _format_transition_time(self, when_utc: Any) -> str:
        if not isinstance(when_utc, datetime.datetime):
            return ""
        try:
            if self._show_local:
                _tz_name, tz = self._current_timezone()
                return when_utc.astimezone(tz).strftime("%a %H:%M")
            return when_utc.astimezone(datetime.timezone.utc).strftime("%a %H:%MZ")
        except Exception:
            return ""

    def _update_runtime_source_summary(
        self,
        *,
        status: Optional[Dict[str, Any]] = None,
        theme: Optional[Dict[str, str]] = None,
    ) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        if status is None:
            try:
                sched = getattr(self.window(), "scheduler", None)
                if sched and hasattr(sched, "get_status_summary"):
                    status = sched.get_status_summary()
            except Exception:
                status = None
        if not status:
            self.sop_runtime_summary_label.setText("Now: -- | Next: --")
            self.sop_runtime_summary_label.setStyleSheet(f"color: {theme.get('text_muted', '#888')};")
            return
        now_source = self._source_label(str(status.get("source") or ""), str(status.get("net_kind") or ""))
        next_source = self._source_label(str(status.get("next_source") or ""), str(status.get("next_net_kind") or ""))
        next_change = bool(status.get("next_source_change"))
        transition_time = self._format_transition_time(status.get("next_transition_utc"))
        if next_change and next_source and next_source != "None":
            if transition_time:
                next_text = f"Next: {next_source} at {transition_time}"
            else:
                next_text = f"Next: {next_source}"
        else:
            next_text = "Next: no source change scheduled"
        self.sop_runtime_summary_label.setText(f"Now: {now_source} | {next_text}")
        self.sop_runtime_summary_label.setStyleSheet(f"color: {theme.get('text', '#111')};")

    @staticmethod
    def _sop_category_label(raw_category: str) -> str:
        text = str(raw_category or "").strip().upper()
        if text in {"HF + LOCAL NET", "LOCAL NET + HF", "SOP-MIXED", "MIXED"}:
            return "SOP-Mixed"
        if text in {"LOCAL NET", "SOP-LOCAL NET"}:
            return "SOP-Local Net"
        return "SOP-HF"

    def _load_sop_profile_catalog(self) -> List[Dict[str, Any]]:
        profiles: List[Dict[str, Any]] = []
        try:
            profiles = self._sop_manager.list_profiles_with_category()
        except Exception as e:
            log.debug("HF Schedule: failed loading SOP profiles: %s", e)
            profiles = []
        for profile in profiles:
            profile["sop_category"] = self._sop_category_label(str(profile.get("sop_category") or ""))
        self._sop_profile_lookup = {int(p.get("id") or 0): p for p in profiles if int(p.get("id") or 0) > 0}
        return profiles

    @staticmethod
    def _safe_mtime(path: Path) -> int:
        try:
            return int(path.stat().st_mtime_ns)
        except Exception:
            return 0

    def _scheduler_status_summary(self) -> Dict[str, Any]:
        try:
            sched = getattr(self.window(), "scheduler", None)
            if sched and hasattr(sched, "get_status_summary"):
                return sched.get_status_summary() or {}
        except Exception:
            pass
        return {}

    def _schedule_state_token(self) -> Tuple[Any, ...]:
        profiles = self._load_sop_profile_catalog()
        profile_sig = tuple(
            sorted(
                (
                    int(p.get("id") or 0),
                    bool(p.get("active")),
                    str(p.get("updated_utc") or ""),
                )
                for p in profiles
            )
        )
        status = self._scheduler_status_summary()
        status_sig = (
            str(status.get("source") or ""),
            str(status.get("sop_selected_profile") or ""),
            bool(status.get("sop_contention")),
            tuple(str(x) for x in (status.get("sop_contention_profiles") or [])),
            str(status.get("next_source") or ""),
        )
        return (
            self._safe_mtime(self._db_path()),
            self._safe_mtime(self._nets_db_path()),
            profile_sig,
            status_sig,
        )

    def _nets_db_path(self) -> Path:
        from freqinout.core.config_paths import get_config_dir

        return get_config_dir() / "config" / "freqinout_nets.db"

    @staticmethod
    def _normalize_freq_text(value: str) -> str:
        txt = str(value or "").strip()
        if not txt:
            return ""
        try:
            return f"{float(txt):.3f}"
        except Exception:
            return txt

    def _display_resource_day_time(self, day_utc: str, start_utc: str, end_utc: str) -> Tuple[str, str, str]:
        day_txt = str(day_utc or "ALL").strip() or "ALL"
        start_txt = str(start_utc or "").strip()
        end_txt = str(end_utc or "").strip()
        if not self._show_local:
            return day_txt, start_txt, end_txt
        if start_txt:
            day_txt, start_txt = self._convert_day_time(day_txt, start_txt, to_local=True)
        if end_txt:
            _, end_txt = self._convert_day_time(day_utc, end_txt, to_local=True)
        return day_txt, start_txt, end_txt

    def _is_hf_sop_profile(self, profile: Dict[str, Any]) -> bool:
        category = self._sop_category_label(str(profile.get("sop_category") or ""))
        if category == "SOP-Local Net":
            return False
        group_name = str(profile.get("operating_group") or "").strip()
        return bool(group_name)

    def _format_conflict_span(self, start_utc: Any, end_utc: Any) -> str:
        if not isinstance(start_utc, datetime.datetime):
            return "--"
        if self._show_local:
            _tz_name, tz = self._current_timezone()
            start_txt = start_utc.astimezone(tz).strftime("%a %H:%M")
            if isinstance(end_utc, datetime.datetime):
                end_txt = end_utc.astimezone(tz).strftime("%H:%M")
                return f"{start_txt}-{end_txt}"
            return start_txt
        start_txt = start_utc.astimezone(datetime.timezone.utc).strftime("%a %H:%MZ")
        if isinstance(end_utc, datetime.datetime):
            end_txt = end_utc.astimezone(datetime.timezone.utc).strftime("%H:%MZ")
            return f"{start_txt}-{end_txt}"
        return start_txt

    def _build_hf_sop_status_rows(self) -> List[Dict[str, Any]]:
        profiles = [p for p in self._load_sop_profile_catalog() if self._is_hf_sop_profile(p)]
        conflict_map: Dict[int, List[Tuple[datetime.datetime, datetime.datetime]]] = {}
        if profiles:
            hf_profile_ids = {int(p.get("id") or 0) for p in profiles if int(p.get("id") or 0) > 0}
            try:
                upcoming_rows = self._sop_manager.build_upcoming_actions(horizon_hours=24, only_active=True)
            except Exception as e:
                log.debug("HF Schedule: failed loading SOP conflicts: %s", e)
                upcoming_rows = []
            for row in upcoming_rows:
                profile_id = int(row.get("profile_id") or 0)
                if profile_id <= 0 or profile_id not in hf_profile_ids:
                    continue
                if bool(row.get("aligned", True)):
                    continue
                if str(row.get("contact_rule") or "").strip().lower() in {"local_profile", "local_group"}:
                    continue
                due_utc = row.get("next_due_utc")
                if not isinstance(due_utc, datetime.datetime):
                    continue
                interval_minutes = max(1, int(row.get("interval_minutes") or 1))
                end_utc = due_utc + datetime.timedelta(minutes=interval_minutes)
                conflict_map.setdefault(profile_id, []).append((due_utc, end_utc))

        rows: List[Dict[str, Any]] = []
        for profile in profiles:
            profile_id = int(profile.get("id") or 0)
            active = bool(profile.get("active"))
            conflicts = sorted(conflict_map.get(profile_id, []), key=lambda x: x[0])
            if conflicts:
                status = "Conflict"
                first_span = self._format_conflict_span(conflicts[0][0], conflicts[0][1])
                extra = len(conflicts) - 1
                issue_summary = (
                    f"Schedule Conflict: {first_span} (+{extra} more)"
                    if extra > 0
                    else f"Schedule Conflict: {first_span}"
                )
            elif active:
                status = "Active"
                issue_summary = "No overlaps in next 24h."
            else:
                status = "Inactive"
                issue_summary = "Inactive."
            rows.append(
                {
                    "profile_id": profile_id,
                    "group_name": str(profile.get("operating_group") or "").strip(),
                    "profile_name": str(profile.get("name") or "").strip() or f"SOP {profile_id}",
                    "status": status,
                    "issue_summary": issue_summary,
                    "active": active,
                }
            )
        status_order = {"Conflict": 0, "Active": 1, "Inactive": 2}
        rows.sort(
            key=lambda r: (
                status_order.get(str(r.get("status") or ""), 9),
                str(r.get("group_name") or "").upper(),
                str(r.get("profile_name") or "").upper(),
            )
        )
        return rows

    def _refresh_sop_profiles_panel(self, *, force: bool = False, theme: Optional[Dict[str, str]] = None) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
        if not force and (now_ts - float(self._last_sop_panel_refresh_ts or 0.0)) < 10.0:
            return
        self._last_sop_panel_refresh_ts = now_ts
        rows = self._build_hf_sop_status_rows()
        conflict_count = sum(1 for r in rows if str(r.get("status") or "") == "Conflict")
        active_count = sum(1 for r in rows if bool(r.get("active")))
        if rows:
            self.sop_profile_summary_label.setText(
                f"HF SOP Sets: {len(rows)} | Active: {active_count} | Conflict: {conflict_count}"
            )
        else:
            self.sop_profile_summary_label.setText("HF SOP Sets: none configured")
        self.sop_profile_summary_label.setStyleSheet(f"color: {theme.get('text', '#111')}; font-weight: 600;")
        self.sop_status_table.setRowCount(0)
        for row in rows:
            r = self.sop_status_table.rowCount()
            self.sop_status_table.insertRow(r)
            self.sop_status_table.setItem(r, 0, QTableWidgetItem(str(row.get("group_name") or "")))
            self.sop_status_table.setItem(r, 1, QTableWidgetItem(str(row.get("profile_name") or "")))
            self.sop_status_table.setItem(r, 2, QTableWidgetItem(str(row.get("status") or "")))
            self.sop_status_table.setItem(r, 3, QTableWidgetItem(str(row.get("issue_summary") or "")))

            profile_id = int(row.get("profile_id") or 0)
            open_btn = QPushButton("Open SOP")
            open_btn.setStyleSheet(button_style("eligible_info", theme))
            open_btn.clicked.connect(lambda _=False, pid=profile_id: self._open_sop_profile(pid))
            self.sop_status_table.setCellWidget(r, 4, open_btn)

            active = bool(row.get("active"))
            toggle_btn = QPushButton("Deactivate" if active else "Activate")
            toggle_btn.setStyleSheet(button_style("eligible_warning" if active else "eligible_success", theme))
            toggle_btn.clicked.connect(
                lambda _=False, pid=profile_id, target=(not active): self._on_toggle_sop_profile_active(pid, target)
            )
            self.sop_status_table.setCellWidget(r, 5, toggle_btn)
        self.sop_status_table.resizeRowsToContents()

    def _on_toggle_sop_profile_active(self, profile_id: int, active: bool) -> None:
        try:
            profile_id = int(profile_id or 0)
            if profile_id <= 0:
                return
            profile = self._sop_profile_lookup.get(profile_id)
            if not profile:
                profile = self._sop_manager.get_profile(profile_id) or {}
            profile_name = str(profile.get("name") or "").strip()
            if not active:
                status = self._scheduler_status_summary()
                source = str(status.get("source") or "").strip().upper()
                selected = str(status.get("sop_selected_profile") or "").strip()
                if source == "SOP" and profile_name and profile_name == selected:
                    confirm = QMessageBox.question(
                        self,
                        "Deactivate Active SOP",
                        (
                            f"'{profile_name}' is currently selected as the active SOP layer.\n"
                            "Deactivate it now?"
                        ),
                    )
                    if confirm != QMessageBox.Yes:
                        return
            if not self._sop_manager.set_profile_active(profile_id, active):
                QMessageBox.warning(self, "SOP", "Could not update SOP active state.")
                return
            self._schedule_resource_token = None
            win = self.window()
            dispatched = False
            try:
                if hasattr(win, "sop_tab") and hasattr(win.sop_tab, "on_sop_profiles_updated"):
                    win.sop_tab.on_sop_profiles_updated()
            except Exception:
                pass
            try:
                if hasattr(win, "_on_sop_data_changed"):
                    win._on_sop_data_changed()
                    dispatched = True
                elif hasattr(win, "scheduler"):
                    win.scheduler.force_refresh()
            except Exception:
                pass
            if not dispatched:
                self._refresh_sop_overlay_rows_in_table()
                self._refresh_sop_profiles_panel(force=True)
                self._update_effective_source_label()
                self._refresh_schedule_resources(force=True)
        except Exception as e:
            QMessageBox.warning(self, "SOP", f"Could not update SOP active state:\n{e}")

    def _load_schedule_resource_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        profiles = [p for p in self._load_sop_profile_catalog() if self._is_hf_sop_profile(p)]
        manual_rows = self._load_manual_schedule_resource_rows()
        sop_rows = self._load_sop_schedule_resource_rows(profiles=profiles)
        sop_gap_rows = self._load_sop_gap_resource_rows(profiles=profiles)
        rows.extend(manual_rows)
        rows.extend(sop_rows)
        rows.extend(sop_gap_rows)
        dedup: Dict[Tuple[str, str, str, str, str, str, str, str], Dict[str, Any]] = {}
        for row in rows:
            source = str(row.get("source") or "manual").strip().lower()
            profile = str(row.get("sop_profile_name") or "").strip().upper()
            key = (
                source,
                profile,
                self._normalize_day(str(row.get("day_utc") or "ALL")),
                str(row.get("group_name") or "").strip().upper(),
                str(row.get("mode") or "").strip().upper(),
                str(row.get("band") or "").strip().upper(),
                self._normalize_freq_text(str(row.get("frequency") or "")),
                f"{self._normalize_hhmm(str(row.get('start_utc') or ''))}-{self._normalize_hhmm(str(row.get('end_utc') or ''))}",
            )
            existing = dedup.get(key)
            if not existing:
                dedup[key] = dict(row)
                continue
            # Prefer persisted/manual rows over virtual rows for duplicates.
            if str(existing.get("source") or "").strip().lower() != "manual" and source == "manual":
                dedup[key] = dict(row)
        out = list(dedup.values())
        out.sort(
            key=lambda r: (
                str(r.get("resource_set") or "").upper(),
                str(r.get("sop_profile_name") or "").upper(),
                str(r.get("group_name") or "").upper(),
                self._normalize_day(str(r.get("day_utc") or "ALL")),
                self._normalize_hhmm(str(r.get("start_utc") or "")),
                self._normalize_hhmm(str(r.get("end_utc") or "")),
            )
        )
        return out

    def _load_manual_schedule_resource_rows(self) -> List[Dict[str, Any]]:
        db_path = self._db_path()
        if not db_path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        conn = sqlite3.connect(db_path)
        try:
            self._ensure_schedule_resources_table(conn)
            cur = conn.execute(
                """
                SELECT
                    id,
                    COALESCE(resource_set, 'Custom'),
                    COALESCE(day_utc, 'ALL'),
                    COALESCE(group_name, ''),
                    COALESCE(mode, ''),
                    COALESCE(band, ''),
                    COALESCE(frequency, ''),
                    COALESCE(start_utc, ''),
                    COALESCE(end_utc, ''),
                    COALESCE(source_type, 'manual'),
                    COALESCE(updated_utc, '')
                FROM hf_schedule_resources
                ORDER BY resource_set COLLATE NOCASE, group_name COLLATE NOCASE, day_utc, start_utc, id
                """
            )
            for row in cur.fetchall():
                rid = int(row[0] or 0)
                rows.append(
                    {
                        "id": rid,
                        "source_key": f"db:{rid}",
                        "resource_set": str(row[1] or "Custom").strip() or "Custom",
                        "day_utc": str(row[2] or "ALL").strip() or "ALL",
                        "group_name": str(row[3] or "").strip(),
                        "mode": str(row[4] or "").strip().upper(),
                        "band": str(row[5] or "").strip().upper(),
                        "frequency": self._normalize_freq_text(str(row[6] or "").strip()),
                        "start_utc": str(row[7] or "").strip(),
                        "end_utc": str(row[8] or "").strip(),
                        "source": str(row[9] or "manual").strip().lower(),
                        "updated_utc": str(row[10] or "").strip(),
                        "recurrence": "Daily" if str(row[2] or "ALL").strip().upper() == "ALL" else "Weekly",
                        "biweekly_offset_weeks": 0,
                        "month_weeks": "",
                        "vfo": "A",
                    }
                )
        except Exception as e:
            log.debug("HF Schedule: failed loading persisted schedule resources: %s", e)
            rows = []
        finally:
            conn.close()
        return rows

    def _load_sop_schedule_resource_rows(self, *, profiles: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        profiles = [dict(p) for p in (profiles or [p for p in self._load_sop_profile_catalog() if self._is_hf_sop_profile(p)])]
        for profile in profiles:
            pid = int(profile.get("id") or 0)
            if pid <= 0:
                continue
            profile_name = str(profile.get("name") or "").strip() or f"SOP {pid}"
            group_name = str(profile.get("operating_group") or "").strip()
            full = self._sop_manager.get_profile(pid)
            if not full:
                continue
            for idx, layer in enumerate(full.get("schedule_layer") or []):
                if not isinstance(layer, dict):
                    continue
                if not bool(layer.get("enabled", True)):
                    continue
                layer_id = int(layer.get("id") or 0)
                recurrence_raw = str(layer.get("recurrence") or "Weekly").strip().upper()
                if recurrence_raw == "MONTHLY":
                    recurrence_raw = "PERIODIC"
                if recurrence_raw == "BI-WEEKLY":
                    recurrence = "Bi-Weekly"
                elif recurrence_raw in {"DAILY", "PERIODIC", "WEEKLY"}:
                    recurrence = recurrence_raw.title()
                else:
                    recurrence = "Weekly"
                day_utc = self._normalize_day(str(layer.get("day_utc") or "ALL"))
                if recurrence == "Daily":
                    day_utc = "ALL"
                band = str(layer.get("band") or "").strip().upper()
                mode = str(layer.get("mode") or "").strip().upper()
                vfo = str(layer.get("vfo") or "A").strip().upper() or "A"
                freq = self._normalize_freq_text(str(layer.get("frequency") or "").strip())
                start = self._normalize_hhmm(str(layer.get("start_utc") or ""))
                end = self._normalize_hhmm(str(layer.get("end_utc") or ""))
                if not (band and mode and freq and start and end):
                    continue
                key_layer = layer_id if layer_id > 0 else (idx + 1)
                rows.append(
                    {
                        "id": 0,
                        "source_key": f"sop:{pid}:{key_layer}",
                        "resource_set": f"SOP: {profile_name}",
                        "day_utc": day_utc,
                        "group_name": group_name,
                        "mode": mode,
                        "band": band,
                        "frequency": freq,
                        "start_utc": start,
                        "end_utc": end,
                        "source": "sop_layer",
                        "updated_utc": str(layer.get("updated_utc") or profile.get("updated_utc") or "").strip(),
                        "recurrence": recurrence,
                        "biweekly_offset_weeks": int(layer.get("biweekly_offset_weeks") or 0),
                        "month_weeks": str(layer.get("month_weeks") or "").strip(),
                        "vfo": vfo,
                        "sop_profile_id": pid,
                        "sop_profile_name": profile_name,
                    }
                )
        return rows

    def _sop_group_defaults(self, group_name: str) -> Tuple[str, str]:
        group = str(group_name or "").strip().upper()
        if not group:
            return "DIGI", "A"
        for g in self.operating_groups:
            if str(g.get("group") or "").strip().upper() != group:
                continue
            mode = str(g.get("mode") or "").strip().upper() or "DIGI"
            vfo = str(g.get("vfo") or "").strip().upper() or "A"
            return mode, vfo
        return "DIGI", "A"

    def _load_sop_gap_resource_rows(self, *, profiles: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        profile_map: Dict[int, Dict[str, Any]] = {}
        for p in (profiles or [p for p in self._load_sop_profile_catalog() if self._is_hf_sop_profile(p)]):
            pid = int(p.get("id") or 0)
            if pid > 0:
                profile_map[pid] = dict(p)
        if not profile_map:
            return rows
        try:
            upcoming = self._sop_manager.build_upcoming_actions(horizon_hours=24, only_active=True)
        except Exception as e:
            log.debug("HF Schedule: failed loading SOP gap resource rows: %s", e)
            return rows
        seen: Set[str] = set()
        for row in upcoming:
            if bool(row.get("aligned", True)):
                continue
            if str(row.get("contact_rule") or "").strip().lower() in {"local_profile", "local_group"}:
                continue
            profile_id = int(row.get("profile_id") or 0)
            profile = profile_map.get(profile_id)
            if not profile:
                continue
            due_utc = row.get("next_due_utc")
            if not isinstance(due_utc, datetime.datetime):
                continue
            band = str(row.get("band") or "").strip().upper()
            freq = self._normalize_freq_text(str(row.get("frequency") or "").strip())
            group_name = str(row.get("operating_group") or "").strip().upper()
            if not (band and freq and group_name):
                continue
            mode, vfo = self._sop_group_defaults(group_name)
            interval_minutes = max(1, int(row.get("interval_minutes") or 0 or 60))
            due_utc = due_utc.astimezone(datetime.timezone.utc)
            end_utc_dt = due_utc + datetime.timedelta(minutes=interval_minutes)
            start_utc = due_utc.strftime("%H:%M")
            end_utc = end_utc_dt.strftime("%H:%M")
            profile_name = str(profile.get("name") or row.get("profile_name") or f"SOP {profile_id}").strip() or f"SOP {profile_id}"
            action_id = int(row.get("action_id") or 0)
            source_key = f"sop_gap:{profile_id}:{action_id}:{band}:{freq}:{start_utc}:{end_utc}"
            if source_key in seen:
                continue
            seen.add(source_key)
            rows.append(
                {
                    "id": 0,
                    "source_key": source_key,
                    "resource_set": f"SOP Gap: {profile_name}",
                    "day_utc": "ALL",
                    "group_name": group_name,
                    "mode": mode,
                    "band": band,
                    "frequency": freq,
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "source": "sop_gap",
                    "updated_utc": self._utc_now_iso(),
                    "recurrence": "Daily",
                    "biweekly_offset_weeks": 0,
                    "month_weeks": "",
                    "vfo": vfo,
                    "sop_profile_id": profile_id,
                    "sop_profile_name": profile_name,
                }
            )
        return rows

    def _ensure_schedule_resources_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hf_schedule_resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_set TEXT NOT NULL DEFAULT 'Custom',
                day_utc TEXT NOT NULL DEFAULT 'ALL',
                group_name TEXT,
                mode TEXT,
                band TEXT,
                frequency TEXT,
                start_utc TEXT,
                end_utc TEXT,
                source_type TEXT DEFAULT 'manual',
                source_ref TEXT DEFAULT '',
                updated_utc TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_hf_resources_set_day_time
            ON hf_schedule_resources(resource_set, day_utc, start_utc)
            """
        )

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

    def _normalize_day(self, day: str) -> str:
        val = str(day or "ALL").strip()
        if not val:
            return "ALL"
        up = val.upper()
        if up == "ALL":
            return "ALL"
        for opt in DAY_OPTIONS:
            if opt.upper() == up:
                return opt
        return "ALL"

    def _normalize_hhmm(self, value: str) -> str:
        txt = str(value or "").strip()
        if not txt:
            return ""
        try:
            hh, mm = txt.split(":")
            h = int(hh)
            m = int(mm)
            if 0 <= h <= 23 and 0 <= m <= 59:
                return f"{h:02d}:{m:02d}"
        except Exception:
            pass
        return txt

    def _schedule_resource_key(self, row: Dict[str, Any]) -> Tuple[str, str, str, str, str, str, str]:
        return (
            self._normalize_day(str(row.get("day_utc") or "")),
            str(row.get("group_name") or "").strip().upper(),
            str(row.get("mode") or "").strip().upper(),
            str(row.get("band") or "").strip().upper(),
            self._normalize_freq_text(str(row.get("frequency") or "").strip()),
            self._normalize_hhmm(str(row.get("start_utc") or "")),
            self._normalize_hhmm(str(row.get("end_utc") or "")),
        )

    def _refresh_schedule_resources(self, *, force: bool = False) -> None:
        token = (
            self._safe_mtime(self._db_path()),
            self._safe_mtime(self._nets_db_path()),
        )
        if force or self._schedule_resource_token != token:
            self._schedule_resource_rows = self._load_schedule_resource_rows()
            self._schedule_resource_token = token
            self._refresh_resource_set_combo()
        self._populate_schedule_resources_table()

    def _refresh_resource_set_combo(self) -> None:
        current = str(self.resources_set_combo.currentData() or "All")
        sets = sorted(
            {
                str(row.get("resource_set") or "Custom").strip() or "Custom"
                for row in self._schedule_resource_rows
            },
            key=lambda x: x.upper(),
        )
        self.resources_set_combo.blockSignals(True)
        self.resources_set_combo.clear()
        self.resources_set_combo.addItem("All", "All")
        for resource_set in sets:
            self.resources_set_combo.addItem(resource_set, resource_set)
        idx = self.resources_set_combo.findData(current)
        if idx >= 0:
            self.resources_set_combo.setCurrentIndex(idx)
        self.resources_set_combo.blockSignals(False)

    def _resource_source_key(self, row: Dict[str, Any]) -> str:
        source_key = str(row.get("source_key") or "").strip()
        if source_key:
            return source_key
        rid = int(row.get("id") or 0)
        return f"db:{rid}" if rid > 0 else ""

    def _selected_view_resource_rows(self) -> List[Dict[str, Any]]:
        selected = self._selected_resource_rows()
        if not selected:
            return []
        selected_keys = {self._resource_source_key(r) for r in selected if self._resource_source_key(r)}
        if not selected_keys:
            return []
        return [dict(r) for r in self._resource_view_rows if self._resource_source_key(r) in selected_keys]

    def _active_schedule_keys(self) -> Set[Tuple[str, str, str, str, str, str, str]]:
        keys: Set[Tuple[str, str, str, str, str, str, str]] = set()
        for r in range(self.table.rowCount()):
            row = self._active_row_to_utc(r)
            if row:
                keys.add(self._active_dup_key(row))
        return keys

    def _sop_profiles_by_group(self) -> Dict[str, List[Dict[str, Any]]]:
        by_group: Dict[str, List[Dict[str, Any]]] = {}
        profiles = [p for p in self._load_sop_profile_catalog() if self._is_hf_sop_profile(p)]
        for profile in profiles:
            group = str(profile.get("operating_group") or "").strip().upper()
            if not group:
                continue
            by_group.setdefault(group, []).append(profile)
        return by_group

    def _resource_conflict_summary(
        self,
        row: Dict[str, Any],
        *,
        target: str,
        active_keys: Optional[Set[Tuple[str, str, str, str, str, str, str]]] = None,
        sop_groups: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Tuple[str, bool]:
        if target == "sop":
            group = str(row.get("group_name") or "").strip().upper()
            if not group:
                return "Missing group", True
            groups = sop_groups or {}
            candidates = groups.get(group, [])
            if not candidates:
                return "No SOP profile for group", True
            if len(candidates) > 1:
                remembered = int(self._sop_group_profile_choice.get(group, 0) or 0)
                if remembered <= 0 or not any(int(c.get("id") or 0) == remembered for c in candidates):
                    return "Select SOP profile", True
            return "Ready", False

        key = self._active_dup_key(row)
        if active_keys is not None and key in active_keys:
            return "Duplicate in HF Active", True
        return "Ready", False

    def _populate_schedule_resources_table(self) -> None:
        set_filter = str(self.resources_set_combo.currentData() or "All").strip()
        text_filter = str(self.resources_group_filter.text() or "").strip().upper()
        rows: List[Dict[str, Any]] = []
        for row in self._schedule_resource_rows:
            resource_set = str(row.get("resource_set") or "Custom").strip() or "Custom"
            if set_filter != "All" and resource_set != set_filter:
                continue
            if text_filter:
                hay = " ".join(
                    [
                        resource_set,
                        str(row.get("sop_profile_name") or ""),
                        str(row.get("group_name") or ""),
                        str(row.get("mode") or ""),
                        str(row.get("band") or ""),
                        str(row.get("frequency") or ""),
                        str(row.get("start_utc") or ""),
                        str(row.get("end_utc") or ""),
                        str(row.get("source") or ""),
                    ]
                ).upper()
                if text_filter not in hay:
                    continue
            rows.append(row)
        target = self._current_resources_apply_target()
        active_keys = self._active_schedule_keys() if target == "hf" else None
        sop_groups = self._sop_profiles_by_group() if target == "sop" else None
        view_rows: List[Dict[str, Any]] = []
        for row in rows:
            view = dict(row)
            conflict_text, has_conflict = self._resource_conflict_summary(
                view,
                target=target,
                active_keys=active_keys,
                sop_groups=sop_groups,
            )
            view["_conflict_text"] = conflict_text
            view["_has_conflict"] = bool(has_conflict)
            view_rows.append(view)
        self._resource_view_rows = view_rows

        self.resources_table.setSortingEnabled(False)
        self.resources_table.setRowCount(0)
        for row in view_rows:
            r = self.resources_table.rowCount()
            self.resources_table.insertRow(r)
            source_key = self._resource_source_key(row)
            select_wrap = QWidget()
            select_layout = QHBoxLayout(select_wrap)
            select_layout.setContentsMargins(0, 0, 0, 0)
            select_layout.setAlignment(Qt.AlignCenter)
            select_chk = QCheckBox()
            select_chk.setProperty("source_key", source_key)
            select_chk.stateChanged.connect(lambda _=None: self._update_resource_action_state())
            select_layout.addWidget(select_chk)
            self.resources_table.setCellWidget(r, self.RES_COL_SELECT, select_wrap)
            day_txt, start_txt, end_txt = self._display_resource_day_time(
                str(row.get("day_utc") or "ALL"),
                str(row.get("start_utc") or ""),
                str(row.get("end_utc") or ""),
            )
            values = [
                str(row.get("resource_set") or "Custom"),
                day_txt,
                str(row.get("group_name") or ""),
                str(row.get("mode") or ""),
                str(row.get("band") or ""),
                str(row.get("frequency") or ""),
                start_txt,
                end_txt,
                (
                    "SOP Layer"
                    if str(row.get("source") or "").strip().lower() == "sop_layer"
                    else (
                        "SOP Gap"
                        if str(row.get("source") or "").strip().lower() == "sop_gap"
                        else str(row.get("source") or "manual")
                    )
                ),
                str(row.get("updated_utc") or ""),
                str(row.get("_conflict_text") or ""),
            ]
            for offset, val in enumerate(values):
                c = self.RES_COL_SET + offset
                item = QTableWidgetItem(val)
                if c == self.RES_COL_SET:
                    item.setData(Qt.UserRole, source_key)
                self.resources_table.setItem(r, c, item)
        self.resources_table.setSortingEnabled(True)
        self._update_resource_action_state()

    def _update_resource_action_state(self) -> None:
        theme = resolve_theme(self.settings)
        selected_rows = self._selected_resource_rows()
        has_selected = bool(selected_rows)
        has_rows = bool(self._resource_view_rows)
        if has_selected and hasattr(self, "resources_apply_target_combo"):
            sources = {str(r.get("source") or "").strip().lower() for r in selected_rows}
            desired = "sop" if sources and sources.issubset({"sop_layer", "sop_gap"}) else "hf"
            current = str(self.resources_apply_target_combo.currentData() or "hf").strip().lower()
            if desired != current:
                self.resources_apply_target_combo.blockSignals(True)
                idx = self.resources_apply_target_combo.findData(desired)
                if idx >= 0:
                    self.resources_apply_target_combo.setCurrentIndex(idx)
                self.resources_apply_target_combo.blockSignals(False)
        target = self._current_resources_apply_target()
        target_label = "SOP Layer" if target == "sop" else "HF Active Schedule"
        selected_view_rows = self._selected_view_resource_rows()
        conflict_scope = selected_view_rows if selected_view_rows else self._resource_view_rows
        has_conflicts = any(bool(r.get("_has_conflict")) for r in conflict_scope)
        self.add_selected_resource_action.setEnabled(has_selected)
        self.add_filtered_resource_action.setEnabled(has_rows)
        self.add_to_schedule_default_action.setEnabled(has_selected or has_rows)
        self.add_to_schedule_default_action.setText(f"Apply to {target_label}")
        self.add_selected_resource_action.setText(f"Apply Selected to {target_label}")
        self.add_filtered_resource_action.setText(f"Apply Filtered to {target_label}")
        self.add_to_schedule_btn.setToolTip(f"Applies selected or filtered rows to {target_label}.")
        self.add_to_schedule_btn.setText(f"Apply to {target_label}")
        self.add_to_schedule_btn.setStyleSheet(
            button_style(
                (
                    "eligible_warning"
                    if has_conflicts
                    else ("eligible_warning" if target == "sop" else "eligible_info")
                )
                if (has_selected or has_rows)
                else "muted",
                theme,
            )
        )
        self.resources_resolve_btn.setEnabled(has_conflicts)
        self.resources_resolve_btn.setStyleSheet(button_style("eligible_warning" if has_conflicts else "muted", theme))
        self.resources_resolve_btn.setToolTip(
            "Resolve SOP profile/group conflicts for selected rows." if has_conflicts else "No conflicts detected."
        )
        self.resources_refresh_btn.setStyleSheet(button_style("muted", theme))

    def _selected_resource_rows(self) -> List[Dict[str, Any]]:
        checked_keys: set[str] = set()
        for r in range(self.resources_table.rowCount()):
            wrap = self.resources_table.cellWidget(r, self.RES_COL_SELECT)
            chk: Optional[QCheckBox] = None
            if isinstance(wrap, QCheckBox):
                chk = wrap
            elif isinstance(wrap, QWidget):
                chk = wrap.findChild(QCheckBox)
            if chk is None or not chk.isChecked():
                continue
            key = str(chk.property("source_key") or "").strip()
            if key:
                checked_keys.add(key)

        source_keys: set[str]
        if checked_keys:
            source_keys = checked_keys
        else:
            source_keys = set()
            for idx in self.resources_table.selectionModel().selectedRows() if self.resources_table.selectionModel() else []:
                item = self.resources_table.item(idx.row(), self.RES_COL_SET)
                if item is None:
                    continue
                raw_key = str(item.data(Qt.UserRole) or "").strip()
                if raw_key:
                    source_keys.add(raw_key)
        if not source_keys:
            return []
        out: List[Dict[str, Any]] = []
        for row in self._schedule_resource_rows:
            source_key = self._resource_source_key(row)
            if source_key and source_key in source_keys:
                out.append(dict(row))
        return out

    def _active_row_is_empty(self, row_index: int) -> bool:
        day = self._get_combo_value(row_index, self.COL_DAY, "")
        group = self._get_combo_value(row_index, self.COL_GROUP, "")
        mode = self._get_combo_value(row_index, self.COL_MODE, "")
        band = self._get_combo_value(row_index, self.COL_BAND, "")
        freq = self._get_text_value(row_index, self.COL_FREQ)
        start = self._get_text_value(row_index, self.COL_START)
        end = self._get_text_value(row_index, self.COL_END)
        return not (day or group or mode or band or freq or start or end)

    def _active_row_to_utc(self, row_index: int, *, include_sop_overlay: bool = False) -> Optional[Dict[str, Any]]:
        is_sop_overlay = self._is_sop_overlay_row(row_index)
        if is_sop_overlay and not include_sop_overlay:
            return None
        day = self._normalize_day(self._get_combo_value(row_index, self.COL_DAY, "ALL"))
        group = self._get_combo_value(row_index, self.COL_GROUP, "")
        if is_sop_overlay:
            sel_wrap = self.table.cellWidget(row_index, self.COL_SELECT)
            if isinstance(sel_wrap, QWidget):
                raw_group = str(sel_wrap.property("sop_overlay_group_name") or "").strip()
                if raw_group:
                    group = raw_group
            if str(group).upper().startswith("SOP:"):
                group = str(group)[4:]
        mode = self._get_combo_value(row_index, self.COL_MODE, "Digi")
        band = self._get_combo_value(row_index, self.COL_BAND, "")
        freq = self._normalize_freq_text(self._get_text_value(row_index, self.COL_FREQ))
        start = self._normalize_hhmm(self._get_text_value(row_index, self.COL_START))
        end = self._normalize_hhmm(self._get_text_value(row_index, self.COL_END))
        auto_tune = self._get_checkbox_value(row_index, self.COL_AUTOTUNE) if not is_sop_overlay else False
        if not (group and mode and band and freq and start and end):
            return None
        if self._show_local:
            source_day = day
            day, start = self._convert_day_time(source_day, start, to_local=False)
            _, end = self._convert_day_time(source_day, end, to_local=False)
        return {
            "day_utc": self._normalize_day(day),
            "group_name": str(group).strip(),
            "mode": str(mode).strip().upper(),
            "band": str(band).strip().upper(),
            "frequency": self._normalize_freq_text(freq),
            "start_utc": self._normalize_hhmm(start),
            "end_utc": self._normalize_hhmm(end),
            "auto_tune": bool(auto_tune),
        }

    def _active_dup_key(self, row: Dict[str, Any]) -> Tuple[str, str, str, str, str, str, str]:
        return (
            self._normalize_day(str(row.get("day_utc") or "ALL")),
            str(row.get("group_name") or "").strip().upper(),
            str(row.get("mode") or "").strip().upper(),
            str(row.get("band") or "").strip().upper(),
            self._normalize_freq_text(str(row.get("frequency") or "")),
            self._normalize_hhmm(str(row.get("start_utc") or "")),
            self._normalize_hhmm(str(row.get("end_utc") or "")),
        )

    def _resource_rows_to_schedule_rows(self, resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for r in resources:
            day_utc = self._normalize_day(str(r.get("day_utc") or "ALL"))
            recurrence = str(r.get("recurrence") or "").strip()
            if not recurrence:
                recurrence = "Daily" if day_utc == "ALL" else "Weekly"
            rows.append(
                {
                    "day_utc": day_utc,
                    "group_name": str(r.get("group_name") or "").strip(),
                    "mode": str(r.get("mode") or "").strip().upper(),
                    "band": str(r.get("band") or "").strip().upper(),
                    "frequency": self._normalize_freq_text(str(r.get("frequency") or "")),
                    "start_utc": self._normalize_hhmm(str(r.get("start_utc") or "")),
                    "end_utc": self._normalize_hhmm(str(r.get("end_utc") or "")),
                    "recurrence": recurrence,
                    "biweekly_offset_weeks": int(r.get("biweekly_offset_weeks") or 0),
                    "month_weeks": str(r.get("month_weeks") or "").strip(),
                    "vfo": str(r.get("vfo") or "A").strip().upper() or "A",
                    "auto_tune": False,
                    "_resource_id": int(r.get("id") or 0),
                    "_resource_set": str(r.get("resource_set") or "").strip(),
                    "_source_key": str(r.get("source_key") or "").strip(),
                }
            )
        return rows

    def _add_resources_to_schedule(self, resources: List[Dict[str, Any]], *, origin: str) -> None:
        if not resources:
            QMessageBox.information(self, "Schedule Resources", "No resources selected.")
            return
        active_rows: List[Dict[str, Any]] = []
        for r in range(self.table.rowCount()):
            row = self._active_row_to_utc(r)
            if row:
                active_rows.append(row)
        active_keys = {self._active_dup_key(r) for r in active_rows}
        candidates = self._resource_rows_to_schedule_rows(resources)
        duplicate_lines: List[str] = []
        for row in candidates:
            key = self._active_dup_key(row)
            if key in active_keys:
                duplicate_lines.append(
                    f"{key[0]} {key[5]}-{key[6]} {key[1]} {key[3]} {key[4]}"
                )
        if duplicate_lines:
            preview = "\n".join(duplicate_lines[:20])
            if len(duplicate_lines) > 20:
                preview += f"\n... and {len(duplicate_lines) - 20} more."
            QMessageBox.warning(
                self,
                "Duplicate HF Schedule Entries",
                "Add blocked due to duplicate day/time/group/band/frequency entries.\n\n" + preview,
            )
            return
        confirm = QMessageBox.question(
            self,
            "Add to Active Schedule",
            f"Add {len(candidates)} row(s) from {origin}?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes,
        )
        if confirm != QMessageBox.Yes:
            return
        prev_suppress = self._suppress_autostart
        self._suppress_autostart = True
        try:
            for row in candidates:
                self._append_entry_row(self._entry_for_display(row))
        finally:
            self._suppress_autostart = prev_suppress
        self._mark_dirty()
        self._update_delete_button_state()
        QMessageBox.information(self, "Schedule Resources", f"Added {len(candidates)} row(s) to Active Schedule.")

    def _current_resources_apply_target(self) -> str:
        if not hasattr(self, "resources_apply_target_combo"):
            return "hf"
        target = str(self.resources_apply_target_combo.currentData() or "hf").strip().lower()
        return target if target in {"hf", "sop"} else "hf"

    def _resolve_resource_conflicts(self) -> None:
        target = self._current_resources_apply_target()
        selected = self._selected_resource_rows()
        scope = selected if selected else [dict(r) for r in self._resource_view_rows]
        if not scope:
            QMessageBox.information(self, "Resolve Conflicts", "No resource rows available.")
            return

        if target == "hf":
            active_keys = self._active_schedule_keys()
            duplicates: List[str] = []
            for row in scope:
                key = self._active_dup_key(row)
                if key in active_keys:
                    duplicates.append(f"{key[0]} {key[5]}-{key[6]} {key[1]} {key[3]} {key[4]}")
            if not duplicates:
                QMessageBox.information(self, "Resolve Conflicts", "No HF schedule conflicts detected.")
                return
            preview = "\n".join(duplicates[:15])
            if len(duplicates) > 15:
                preview += f"\n... and {len(duplicates) - 15} more."
            QMessageBox.information(
                self,
                "HF Conflicts",
                (
                    "These rows already exist in Active Schedule.\n"
                    "Resolve by editing/deleting duplicates or switch Apply To to SOP Layer.\n\n"
                    f"{preview}"
                ),
            )
            self._populate_schedule_resources_table()
            return

        groups = self._sop_profiles_by_group()
        unresolved: List[str] = []
        resolved_count = 0
        unique_groups = sorted({str(r.get("group_name") or "").strip().upper() for r in scope if str(r.get("group_name") or "").strip()})
        for group in unique_groups:
            candidates = groups.get(group, [])
            if not candidates:
                unresolved.append(f"{group}: no HF SOP profile found.")
                continue
            if len(candidates) == 1:
                pid = int(candidates[0].get("id") or 0)
                if pid > 0:
                    self._sop_group_profile_choice[group] = pid
                    resolved_count += 1
                continue
            pid = self._resolve_sop_profile_for_group(group, candidates)
            if pid > 0:
                resolved_count += 1
            else:
                unresolved.append(f"{group}: selection cancelled.")

        self._populate_schedule_resources_table()
        if unresolved:
            QMessageBox.information(
                self,
                "Resolve Conflicts",
                (
                    f"Resolved {resolved_count} group mapping(s).\n\n"
                    "Remaining:\n" + "\n".join(unresolved[:12])
                ),
            )
            return
        QMessageBox.information(
            self,
            "Resolve Conflicts",
            f"Resolved {resolved_count} SOP group mapping(s).",
        )

    def _resolve_sop_profile_for_group(self, group_name: str, candidates: List[Dict[str, Any]]) -> int:
        group = str(group_name or "").strip().upper()
        valid: List[Dict[str, Any]] = [dict(c) for c in candidates if int(c.get("id") or 0) > 0]
        if not valid:
            return 0
        if len(valid) == 1:
            return int(valid[0].get("id") or 0)
        remembered = int(self._sop_group_profile_choice.get(group, 0) or 0)
        if remembered > 0 and any(int(v.get("id") or 0) == remembered for v in valid):
            return remembered

        labels: List[str] = []
        id_by_label: Dict[str, int] = {}
        for profile in valid:
            pid = int(profile.get("id") or 0)
            name = str(profile.get("name") or f"SOP {pid}").strip() or f"SOP {pid}"
            state = "active" if bool(profile.get("active")) else "inactive"
            label = f"{name} (#{pid}, {state})"
            labels.append(label)
            id_by_label[label] = pid
        choice, ok = QInputDialog.getItem(
            self,
            "Select SOP Profile",
            (
                f"Multiple SOP profiles are associated with HF group '{group}'.\n"
                "Select the SOP profile to receive these rows."
            ),
            labels,
            0,
            False,
        )
        if not ok:
            return 0
        selected = int(id_by_label.get(str(choice), 0) or 0)
        if selected > 0:
            self._sop_group_profile_choice[group] = selected
        return selected

    def _resource_row_to_sop_layer_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        day_utc = self._normalize_day(str(row.get("day_utc") or "ALL"))
        recurrence = str(row.get("recurrence") or "").strip()
        if not recurrence:
            recurrence = "Daily" if day_utc == "ALL" else "Weekly"
        band = str(row.get("band") or "").strip().upper()
        mode = str(row.get("mode") or "").strip().upper()
        vfo = str(row.get("vfo") or "A").strip().upper() or "A"
        freq = self._normalize_freq_text(str(row.get("frequency") or "").strip())
        start = self._normalize_hhmm(str(row.get("start_utc") or "").strip())
        end = self._normalize_hhmm(str(row.get("end_utc") or "").strip())
        if not (band and mode and freq and start and end):
            return None
        return {
            "day_utc": day_utc,
            "recurrence": recurrence,
            "biweekly_offset_weeks": int(row.get("biweekly_offset_weeks") or 0),
            "month_weeks": str(row.get("month_weeks") or "").strip(),
            "band": band,
            "mode": mode,
            "vfo": vfo,
            "frequency": freq,
            "start_utc": start,
            "end_utc": end,
            "enabled": True,
        }

    def _dispatch_sop_schedule_change(self) -> None:
        win = self.window()
        try:
            if hasattr(win, "sop_tab") and hasattr(win.sop_tab, "on_sop_profiles_updated"):
                win.sop_tab.on_sop_profiles_updated()
        except Exception:
            pass
        try:
            if hasattr(win, "_on_sop_data_changed"):
                win._on_sop_data_changed()
            elif hasattr(win, "scheduler"):
                win.scheduler.force_refresh()
        except Exception:
            pass
        self._schedule_resource_token = None
        self._refresh_sop_overlay_rows_in_table()
        self._refresh_sop_profiles_panel(force=True)
        self._update_effective_source_label()
        self._refresh_schedule_resources(force=True)

    def _add_resources_to_sop_layer(self, resources: List[Dict[str, Any]], *, origin: str) -> None:
        if not resources:
            QMessageBox.information(self, "Schedule Resources", "No resources selected.")
            return

        profiles = [p for p in self._load_sop_profile_catalog() if self._is_hf_sop_profile(p)]
        by_group: Dict[str, List[Dict[str, Any]]] = {}
        for profile in profiles:
            grp = str(profile.get("operating_group") or "").strip().upper()
            if not grp:
                continue
            by_group.setdefault(grp, []).append(profile)

        profile_rows: Dict[int, List[Dict[str, Any]]] = {}
        unresolved: List[str] = []
        for row in resources:
            group = str(row.get("group_name") or "").strip().upper()
            if not group:
                unresolved.append("Missing group name in one or more rows.")
                continue
            candidates = by_group.get(group, [])
            if not candidates:
                unresolved.append(f"{group}: no HF SOP profile found.")
                continue
            profile_id = self._resolve_sop_profile_for_group(group, candidates)
            if profile_id <= 0:
                unresolved.append(f"{group}: selection cancelled.")
                continue
            layer_row = self._resource_row_to_sop_layer_row(row)
            if not layer_row:
                unresolved.append(f"{group}: incomplete row (band/mode/frequency/time required).")
                continue
            profile_rows.setdefault(profile_id, []).append(layer_row)

        if not profile_rows:
            msg = "No rows could be applied to SOP Layer."
            if unresolved:
                msg += "\n\n" + "\n".join(unresolved[:12])
            QMessageBox.warning(self, "SOP Layer", msg)
            return

        apply_count = sum(len(v) for v in profile_rows.values())
        confirm = QMessageBox.question(
            self,
            "Apply to SOP Layer",
            f"Apply {apply_count} row(s) from {origin} to SOP Layer across {len(profile_rows)} profile(s)?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes,
        )
        if confirm != QMessageBox.Yes:
            return

        changed = 0
        for profile_id, rows in profile_rows.items():
            try:
                changed += int(self._sop_manager.upsert_schedule_layer_rows(profile_id, rows) or 0)
            except Exception as e:
                log.debug("HF Schedule: failed applying rows to SOP layer profile %s: %s", profile_id, e)
                unresolved.append(f"Profile {profile_id}: update failed.")

        if changed > 0:
            try:
                self.resources_set_combo.blockSignals(True)
                idx = self.resources_set_combo.findData("All")
                if idx >= 0:
                    self.resources_set_combo.setCurrentIndex(idx)
            finally:
                self.resources_set_combo.blockSignals(False)
            self._dispatch_sop_schedule_change()

        detail = f"Applied {changed} row(s) to SOP Layer."
        if unresolved:
            detail += "\n\nIssues:\n" + "\n".join(unresolved[:12])
        QMessageBox.information(self, "SOP Layer", detail)

    def _apply_resources(self, resources: List[Dict[str, Any]], *, origin: str) -> None:
        target = self._current_resources_apply_target()
        if target == "sop":
            self._add_resources_to_sop_layer(resources, origin=origin)
            return
        self._add_resources_to_schedule(resources, origin=origin)

    def _add_resources_default(self) -> None:
        selected = self._selected_resource_rows()
        if selected:
            self._apply_resources(selected, origin="selected resources")
            return
        self._apply_resources([dict(r) for r in self._resource_view_rows], origin="filtered resources")

    def _add_selected_resources_to_schedule(self) -> None:
        self._apply_resources(self._selected_resource_rows(), origin="selected resources")

    def _add_filtered_resources_to_schedule(self) -> None:
        self._apply_resources([dict(r) for r in self._resource_view_rows], origin="filtered resources")

    def _checked_schedule_row_indexes(self) -> List[int]:
        selected: List[int] = []
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, self.COL_SELECT)
            chk: Optional[QCheckBox] = None
            if isinstance(w, QCheckBox):
                chk = w
            elif isinstance(w, QWidget):
                chk = w.findChild(QCheckBox)
            if chk is not None and chk.isChecked():
                selected.append(r)
        return selected

    def _upsert_schedule_resource_row(
        self,
        conn: sqlite3.Connection,
        row: Dict[str, Any],
        *,
        resource_set: str,
        source_type: str,
        source_ref: str,
        resource_id: Optional[int] = None,
    ) -> int:
        now_utc = self._utc_now_iso()
        day = self._normalize_day(str(row.get("day_utc") or "ALL"))
        group_name = str(row.get("group_name") or "").strip()
        mode = str(row.get("mode") or "").strip().upper()
        band = str(row.get("band") or "").strip().upper()
        freq = self._normalize_freq_text(str(row.get("frequency") or ""))
        start = self._normalize_hhmm(str(row.get("start_utc") or ""))
        end = self._normalize_hhmm(str(row.get("end_utc") or ""))
        set_name = str(resource_set or "Custom").strip() or "Custom"
        rid = int(resource_id or 0)
        if rid > 0:
            conn.execute(
                """
                UPDATE hf_schedule_resources
                SET resource_set = ?, day_utc = ?, group_name = ?, mode = ?, band = ?, frequency = ?,
                    start_utc = ?, end_utc = ?, source_type = ?, source_ref = ?, updated_utc = ?
                WHERE id = ?
                """,
                (set_name, day, group_name, mode, band, freq, start, end, source_type, source_ref, now_utc, rid),
            )
            return rid
        cur = conn.execute(
            """
            SELECT id
            FROM hf_schedule_resources
            WHERE resource_set = ? AND day_utc = ? AND group_name = ? AND mode = ? AND band = ?
              AND frequency = ? AND start_utc = ? AND end_utc = ?
            LIMIT 1
            """,
            (set_name, day, group_name, mode, band, freq, start, end),
        )
        existing = cur.fetchone()
        if existing:
            rid = int(existing[0] or 0)
            conn.execute(
                """
                UPDATE hf_schedule_resources
                SET source_type = ?, source_ref = ?, updated_utc = ?
                WHERE id = ?
                """,
                (source_type, source_ref, now_utc, rid),
            )
            return rid
        cur = conn.execute(
            """
            INSERT INTO hf_schedule_resources
                (resource_set, day_utc, group_name, mode, band, frequency, start_utc, end_utc, source_type, source_ref, updated_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (set_name, day, group_name, mode, band, freq, start, end, source_type, source_ref, now_utc),
        )
        return int(cur.lastrowid or 0)

    def _move_selected_schedule_rows_to_resources(self) -> None:
        selected = self._checked_schedule_row_indexes()
        if not selected:
            QMessageBox.information(self, "Move to Resources", "No Active Schedule rows selected.")
            return
        target_set = str(self.resources_set_combo.currentData() or "All").strip()
        if target_set == "All":
            target_set = "Custom"
        db_path = self._db_path()
        conn = sqlite3.connect(db_path)
        moved = 0
        moved_hf = 0
        moved_sop = 0
        try:
            self._ensure_schedule_resources_table(conn)
            for r in selected:
                is_sop_overlay = self._is_sop_overlay_row(r)
                row = self._active_row_to_utc(r, include_sop_overlay=True)
                if not row:
                    continue
                sel_w = self.table.cellWidget(r, self.COL_SELECT)
                resource_id = None
                if isinstance(sel_w, QWidget) and not is_sop_overlay:
                    rid = sel_w.property("resource_id")
                    if rid not in (None, ""):
                        try:
                            resource_id = int(rid)
                        except Exception:
                            resource_id = None
                source_type = "moved"
                source_ref = "active_schedule"
                if is_sop_overlay:
                    source_type = "sop_overlay"
                    source_ref = self._sop_overlay_source_key(r) or "sop_overlay"
                self._upsert_schedule_resource_row(
                    conn,
                    row,
                    resource_set=target_set,
                    source_type=source_type,
                    source_ref=source_ref,
                    resource_id=resource_id,
                )
                moved += 1
                if is_sop_overlay:
                    moved_sop += 1
                else:
                    moved_hf += 1
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            QMessageBox.critical(self, "Move Failed", f"Could not move rows to resources:\n{e}")
            return
        finally:
            conn.close()
        for r in sorted(selected, reverse=True):
            if self._is_sop_overlay_row(r):
                source_key = self._sop_overlay_source_key(r)
                if source_key:
                    self._hidden_sop_overlay_keys.add(source_key)
            self.table.removeRow(r)
        if self.table.rowCount() == 0:
            self._add_row()
        if moved_hf > 0:
            self._mark_dirty()
        self._refresh_schedule_resources(force=True)
        self._highlight_time_conflicts()
        QMessageBox.information(
            self,
            "Move to Resources",
            f"Moved {moved} row(s) to Schedule Resources. HF: {moved_hf}, SOP: {moved_sop}.",
        )

    def _refresh_schedule_issues(self, *, force: bool = False) -> None:
        # Retained method name for call-site compatibility.
        self._refresh_sop_profiles_panel(force=force)

    def _populate_schedule_issues_table(self) -> None:
        # Retained method name for call-site compatibility.
        self._refresh_sop_profiles_panel(force=True)

    def _dismiss_issue(self, key: str) -> None:
        # Retained for compatibility; schedule conflicts are now represented directly in SOP status rows.
        return

    def _open_sop_profile(self, profile_id: int) -> None:
        pid = int(profile_id or 0)
        if pid <= 0:
            return
        self._navigate_to_tab("SOP")
        win = self.window()
        try:
            sop_tab = getattr(win, "sop_tab", None)
            if sop_tab is None:
                return
            if hasattr(sop_tab, "select_profile") and sop_tab.select_profile(pid):
                return
            if hasattr(sop_tab, "profile_combo"):
                for idx in range(sop_tab.profile_combo.count()):
                    if int(sop_tab.profile_combo.itemData(idx) or 0) != pid:
                        continue
                    sop_tab.profile_combo.setCurrentIndex(idx)
                    return
        except Exception as e:
            log.debug("HF Schedule: failed to open SOP profile %s: %s", pid, e)

    def _focus_daily_row(self, group_name: str, band: str, frequency: str) -> None:
        group_norm = str(group_name or "").strip().upper()
        band_norm = str(band or "").strip().upper()
        freq_norm = self._normalize_freq_text(str(frequency or "").strip())
        for r in range(self.table.rowCount()):
            if self._is_sop_overlay_row(r):
                continue
            group_val = self._get_combo_value(r, self.COL_GROUP, default="").strip().upper()
            band_val = self._get_combo_value(r, self.COL_BAND, default="").strip().upper()
            freq_val = self._normalize_freq_text(self._get_text_value(r, self.COL_FREQ))
            if group_norm and group_val != group_norm:
                continue
            if band_norm and band_val != band_norm:
                continue
            if freq_norm and freq_val and freq_val != freq_norm:
                continue
            self.table.selectRow(r)
            self.table.scrollToItem(self.table.item(r, self.COL_FREQ))
            self.table.setFocus(Qt.TabFocusReason)
            return
        if self.table.rowCount() > 0:
            self.table.selectRow(0)
            self.table.setFocus(Qt.TabFocusReason)

    def _navigate_to_tab(self, tab_label: str) -> None:
        label_target = str(tab_label or "").strip().upper()
        if not label_target:
            return
        win = self.window()
        try:
            if hasattr(win, "_screens") and hasattr(win, "_set_screen"):
                for idx, (lbl, _w) in enumerate(win._screens):
                    if str(lbl or "").strip().upper() == label_target:
                        win._set_screen(idx)
                        return
        except Exception:
            pass

    def _handle_issue_action(self, issue: Dict[str, Any]) -> None:
        action_type = str(issue.get("action_type") or "").strip().lower()
        if action_type == "open_sop":
            self._navigate_to_tab("SOP")
            return
        if action_type == "deactivate_profile":
            profile_id = int(issue.get("profile_id") or 0)
            if profile_id > 0:
                self._on_toggle_sop_profile_active(profile_id, False)
            return
        if action_type == "adjust_daily":
            self._focus_daily_row(
                str(issue.get("group_name") or ""),
                str(issue.get("band") or ""),
                str(issue.get("frequency") or ""),
            )
            self.save_btn.setStyleSheet(button_style("eligible_warning", resolve_theme(self.settings)))
            return

    def _set_headers(self):
        mode_label = "Local" if self._show_local else "UTC"
        headers = [
            "Selected",
            f"Day ({mode_label})",
            "Source",
            "Group Name",
            "Mode",
            "Band",
            "Freq (MHz)",
            f"Start ({mode_label})",
            f"End ({mode_label})",
            "Auto-Tune",
        ]
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)

    def _day_offset(self, day_name: str) -> int:
        """
        Return 0-6 offset for canonical day names (Sunday=0). Defaults to 0 on unknown.
        """
        try:
            return DAY_CANON.index(day_name)
        except Exception:
            return 0

    def _anchor_utc_sunday(self) -> datetime.datetime:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        delta = (now_utc.weekday() + 1) % 7  # Sunday=0, Monday=1, ...
        sunday = now_utc - datetime.timedelta(days=delta)
        return sunday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)

    def _anchor_local_sunday(self) -> datetime.datetime:
        """
        Sunday 00:00 in the configured local timezone.
        """
        _, tz = self._current_timezone()
        now_local = datetime.datetime.now(tz)
        delta = (now_local.weekday() + 1) % 7
        sunday = now_local - datetime.timedelta(days=delta)
        return sunday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=tz)

    def _convert_day_time(self, day: str, hhmm: str, to_local: bool) -> Tuple[str, str]:
        """
        Convert (day, HH:MM) between UTC and local time using current timezone.
        Returns (day_name, hh:mm) in target zone. 'ALL' keeps day as ALL but still converts time.
        """
        day = (day or "ALL").strip()
        if not hhmm:
            return day, hhmm
        try:
            hour, minute = hhmm.split(":")
            hour = int(hour)
            minute = int(minute)
        except Exception:
            return day, hhmm
        # Map day to canonical offset (Sunday=0)
        day_upper = day.upper()
        day_idx = 0 if day_upper == "ALL" else self._day_offset(day)
        if to_local:
            anchor = self._anchor_utc_sunday()
            _, tz = self._current_timezone()
            dt_utc = anchor + datetime.timedelta(days=day_idx, hours=hour, minutes=minute)
            dt_loc = dt_utc.astimezone(tz)
            return ("ALL" if day_upper == "ALL" else dt_loc.strftime("%A")), dt_loc.strftime("%H:%M")
        else:
            anchor_loc = self._anchor_local_sunday()
            dt_loc = anchor_loc + datetime.timedelta(days=day_idx, hours=hour, minutes=minute)
            dt_utc = dt_loc.astimezone(datetime.timezone.utc)
            return ("ALL" if day_upper == "ALL" else dt_utc.strftime("%A")), dt_utc.strftime("%H:%M")

    # ---------------- Data load/save ---------------- #

    def _db_path(self) -> Path:
        """
        Location of the primary settings DB (freqinout.db).
        """
        from freqinout.core.config_paths import get_config_dir

        cfg_path = getattr(self.settings, "_config_path", None)
        if cfg_path:
            try:
                return Path(cfg_path)
            except Exception:
                pass
        return get_config_dir() / "config" / "freqinout.db"

    def _load_schedule_from_db(self) -> List[Dict]:
        """
        Load HF schedule rows from SQLite table daily_schedule_tab, if present.
        """
        db_path = self._db_path()
        if not db_path.exists():
            return []

        conn = sqlite3.connect(db_path)
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='daily_schedule_tab'"
            )
            if not cur.fetchone():
                return []

            # Try new schema; if fails, fall back to legacy (we'll map)
            try:
                cur = conn.execute(
                    """
                    SELECT
                        day_utc,
                        band,
                        mode,
                        vfo,
                        frequency,
                        start_utc,
                        end_utc,
                        group_name,
                        auto_tune
                    FROM daily_schedule_tab
                    """
                )
                rows: List[Dict] = []
                for (
                    day_utc,
                    band,
                    mode,
                    vfo,
                    freq,
                    start_utc,
                    end_utc,
                    group_name,
                    auto_tune,
                ) in cur.fetchall():
                    rows.append(
                        {
                            "day_utc": (day_utc or "ALL").strip(),
                            "band": (band or "").strip(),
                            "mode": (mode or "Digi").strip(),
                            "vfo": (vfo or "A").strip().upper(),
                            "frequency": str(freq or ""),
                            "start_utc": start_utc or "",
                            "end_utc": end_utc or "",
                            "group_name": (group_name or "").strip(),
                            "auto_tune": bool(auto_tune),
                            "fldigi_offset": "",
                            "js8_offset": "",
                            "primary_js8call_group": "",
                            "comment": "",
                        }
                    )
                return rows
            except Exception:
                pass

            # Legacy schema fallback
            cur = conn.execute(
                """
                SELECT
                    day_utc,
                    band,
                    mode,
                    vfo,
                    frequency,
                    fldigi_offset,
                    js8_offset,
                    start_utc,
                    end_utc,
                    primary_js8call_group,
                    group_name,
                    comment,
                    auto_tune
                FROM daily_schedule_tab
                """
            )
            rows: List[Dict] = []
            for (
                day_utc,
                band,
                mode,
                vfo,
                freq,
                fldigi_offset,
                js8_offset,
                start_utc,
                end_utc,
                primary_group,
                group_name,
                comment,
                auto_tune,
            ) in cur.fetchall():
                rows.append(
                    {
                        "day_utc": (day_utc or "ALL").strip(),
                        "band": (band or "").strip(),
                        "mode": (mode or "Digi").strip(),
                        "vfo": (vfo or "A").strip().upper(),
                        "frequency": str(freq or ""),
                        "fldigi_offset": "",
                        "js8_offset": "",
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "primary_js8call_group": "",
                        "group_name": group_name or "",
                        "comment": "",
                        "auto_tune": bool(auto_tune),
                    }
                )
            return rows
        except Exception as e:
            log.error("HF Frequency Schedule: failed to load from DB %s: %s", db_path, e)
            return []
        finally:
            conn.close()

    def _save_schedule_to_db(self, rows: List[Dict]) -> None:
        """
        Persist HF schedule rows to SQLite table daily_schedule_tab.
        """
        db_path = self._db_path()
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("DROP TABLE IF EXISTS daily_schedule_tab")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_schedule_tab (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    day_utc TEXT NOT NULL,
                    band TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    vfo TEXT,
                    frequency TEXT NOT NULL,
                    start_utc TEXT NOT NULL,
                    end_utc TEXT NOT NULL,
                    group_name TEXT,
                    auto_tune INTEGER DEFAULT 0
                )
                """
            )
            conn.execute("DELETE FROM daily_schedule_tab")
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO daily_schedule_tab
                        (day_utc, band, mode, vfo, frequency,
                         start_utc, end_utc, group_name, auto_tune)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row.get("day_utc"),
                        row.get("band"),
                        row.get("mode"),
                        row.get("vfo"),
                        row.get("frequency"),
                        row.get("start_utc"),
                        row.get("end_utc"),
                        row.get("group_name"),
                        1 if row.get("auto_tune") else 0,
                    ),
                )
            conn.commit()
            log.info("HF schedule mirrored to DB at %s (%d entries).", db_path, len(rows))
        finally:
            conn.close()

    def _load_schedule(self):
        hf_sched = self._load_schedule_from_db()
        loaded_from_db = bool(hf_sched)

        if not hf_sched:
            data = self.settings.all()
            hf_sched = data.get("hf_schedule")

            # Backwards compatibility: if hf_schedule not present, try daily_schedule
            if hf_sched is None:
                hf_sched = data.get("daily_schedule", [])

            if not isinstance(hf_sched, list):
                hf_sched = []

        self._suspend_dirty_tracking = True
        try:
            self.table.setRowCount(0)
            self._raw_schedule = hf_sched

            for entry in hf_sched:
                self._append_entry_row(self._entry_for_display(entry))

            if self.table.rowCount() == 0:
                # Add a single empty row to start with
                self._add_row()
            self._append_sop_overlay_rows()
        finally:
            self._suspend_dirty_tracking = False

        src = "DB" if loaded_from_db else "settings"
        log.info("HF Frequency Schedule loaded from %s: %d rows", src, self.table.rowCount())
        self._set_headers()
        self._update_clock_labels()
        self._saved_rows_signature = self._rows_signature(self._raw_schedule)
        self._set_dirty(False)
        self._refresh_schedule_resources(force=True)
        self._refresh_schedule_issues(force=True)
        self._saved_rows_signature = self._rows_signature(self._collect_rows_for_signature())
        self._set_dirty(False)
        self._highlight_time_conflicts()

    def _load_active_sop_overlay_rows(self) -> List[Dict[str, Any]]:
        """
        Return active HF SOP layer rows rendered as read-only overlays in Active Schedule.
        """
        profiles = [p for p in self._load_sop_profile_catalog() if self._is_hf_sop_profile(p) and bool(p.get("active"))]
        if not profiles:
            return []
        rows: List[Dict[str, Any]] = []
        try:
            sop_rows = self._load_sop_schedule_resource_rows(profiles=profiles)
            active_keys = {str(r.get("source_key") or "").strip() for r in sop_rows if str(r.get("source_key") or "").strip()}
            if active_keys:
                self._hidden_sop_overlay_keys.intersection_update(active_keys)
            for row in sop_rows:
                source_key = str(row.get("source_key") or "").strip()
                if source_key and source_key in self._hidden_sop_overlay_keys:
                    continue
                layer_id = 0
                if source_key.startswith("sop:"):
                    parts = source_key.split(":")
                    if len(parts) >= 3 and str(parts[2]).isdigit():
                        layer_id = int(parts[2])
                entry = {
                    "day_utc": self._normalize_day(str(row.get("day_utc") or "ALL")),
                    "group_name": str(row.get("group_name") or "").strip(),
                    "mode": str(row.get("mode") or "").strip().upper(),
                    "band": str(row.get("band") or "").strip().upper(),
                    "frequency": self._normalize_freq_text(str(row.get("frequency") or "")),
                    "start_utc": self._normalize_hhmm(str(row.get("start_utc") or "")),
                    "end_utc": self._normalize_hhmm(str(row.get("end_utc") or "")),
                    "auto_tune": False,
                    "_sop_overlay": True,
                    "_sop_profile_name": str(row.get("sop_profile_name") or "").strip(),
                    "_source_key": source_key,
                    "_sop_profile_id": int(row.get("sop_profile_id") or 0),
                    "_sop_layer_id": layer_id,
                    "_sop_recurrence": str(row.get("recurrence") or "Weekly"),
                    "_sop_biweekly_offset_weeks": int(row.get("biweekly_offset_weeks") or 0),
                    "_sop_month_weeks": str(row.get("month_weeks") or "").strip(),
                    "_sop_vfo": str(row.get("vfo") or "A").strip().upper() or "A",
                }
                if not (
                    entry["group_name"]
                    and entry["mode"]
                    and entry["band"]
                    and entry["frequency"]
                    and entry["start_utc"]
                    and entry["end_utc"]
                ):
                    continue
                rows.append(entry)
        except Exception as e:
            log.debug("HF Schedule: failed loading active SOP overlay rows: %s", e)
            rows = []
        rows.sort(
            key=lambda r: (
                str(r.get("_sop_profile_name") or "").upper(),
                str(r.get("group_name") or "").upper(),
                self._normalize_day(str(r.get("day_utc") or "ALL")),
                self._normalize_hhmm(str(r.get("start_utc") or "")),
                self._normalize_hhmm(str(r.get("end_utc") or "")),
            )
        )
        return rows

    def _append_sop_overlay_rows(self) -> None:
        overlays = self._load_active_sop_overlay_rows()
        for entry in overlays:
            self._append_entry_row(self._entry_for_display(entry))

    def _refresh_sop_overlay_rows_in_table(self) -> None:
        prev_suspend = self._suspend_dirty_tracking
        self._suspend_dirty_tracking = True
        try:
            for r in range(self.table.rowCount() - 1, -1, -1):
                if self._is_sop_overlay_row(r):
                    self.table.removeRow(r)
            self._append_sop_overlay_rows()
        finally:
            self._suspend_dirty_tracking = prev_suspend
        self._update_delete_button_state()
        self._highlight_time_conflicts()

    def _is_sop_overlay_row(self, row_index: int) -> bool:
        wrap = self.table.cellWidget(row_index, self.COL_SELECT)
        if not isinstance(wrap, QWidget):
            return False
        return bool(wrap.property("sop_overlay"))

    def _sop_overlay_source_key(self, row_index: int) -> str:
        wrap = self.table.cellWidget(row_index, self.COL_SELECT)
        if not isinstance(wrap, QWidget):
            return ""
        return str(wrap.property("sop_overlay_source_key") or "").strip()

    def _schedule_day_names(self, day_utc: str) -> List[str]:
        day = self._normalize_day(day_utc)
        if day == "ALL":
            return list(DAY_CANON)
        return [day]

    @staticmethod
    def _time_to_minutes(hhmm: str) -> Optional[int]:
        try:
            hh, mm = str(hhmm or "").split(":")
            h = int(hh)
            m = int(mm)
            if 0 <= h <= 23 and 0 <= m <= 59:
                return h * 60 + m
        except Exception:
            return None
        return None

    @staticmethod
    def _overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
        return max(a_start, b_start) < min(a_end, b_end)

    def _highlight_time_conflicts(self) -> None:
        prev_block = self.table.blockSignals(True)
        try:
            day_intervals: Dict[str, List[Tuple[int, int, int]]] = {d: [] for d in DAY_CANON}
            conflict_rows: Set[int] = set()

            for r in range(self.table.rowCount()):
                row = self._active_row_to_utc(r, include_sop_overlay=True)
                if not row:
                    continue
                start_m = self._time_to_minutes(str(row.get("start_utc") or ""))
                end_m = self._time_to_minutes(str(row.get("end_utc") or ""))
                if start_m is None or end_m is None:
                    continue
                day_names = self._schedule_day_names(str(row.get("day_utc") or "ALL"))
                for day_name in day_names:
                    day_idx = DAY_CANON.index(day_name)
                    next_day = DAY_CANON[(day_idx + 1) % len(DAY_CANON)]
                    if start_m < end_m:
                        day_intervals[day_name].append((r, start_m, end_m))
                    elif start_m > end_m:
                        # Overnight span split across day boundary.
                        day_intervals[day_name].append((r, start_m, 24 * 60))
                        day_intervals[next_day].append((r, 0, end_m))
                    else:
                        # Same start/end means full-day occupancy.
                        day_intervals[day_name].append((r, 0, 24 * 60))

            for day_name, spans in day_intervals.items():
                spans_sorted = sorted(spans, key=lambda x: (x[1], x[2], x[0]))
                for i in range(len(spans_sorted)):
                    r1, s1, e1 = spans_sorted[i]
                    for j in range(i + 1, len(spans_sorted)):
                        r2, s2, e2 = spans_sorted[j]
                        if s2 >= e1:
                            break
                        if self._overlap(s1, e1, s2, e2):
                            conflict_rows.add(r1)
                            conflict_rows.add(r2)

            theme = resolve_theme(self.settings)
            warn = QColor(str(theme.get("warning", "#C99700")))
            warn.setAlpha(64)
            for r in range(self.table.rowCount()):
                for col in (self.COL_START, self.COL_END):
                    item = self.table.item(r, col)
                    if item is None:
                        continue
                    base_tip = str(item.data(Qt.UserRole) or "")
                    if r in conflict_rows:
                        item.setBackground(warn)
                        item.setToolTip((base_tip + "\n" if base_tip else "") + "Time conflict with another active row.")
                    else:
                        item.setData(Qt.BackgroundRole, None)
                        item.setToolTip(base_tip)
        finally:
            self.table.blockSignals(prev_block)

    def _save_schedule(self):
        # Ensure in-progress cell edits are committed
        fw = QApplication.focusWidget()
        if fw is not None and self.table.isAncestorOf(fw):
            fw.clearFocus()
            QApplication.processEvents()

        hf_rows: List[Dict[str, Any]] = []
        sop_updates: Dict[int, List[Dict[str, Any]]] = {}
        errors: List[str] = []

        for r in range(self.table.rowCount()):
            is_sop = self._is_sop_overlay_row(r)
            day = self._get_combo_value(r, self.COL_DAY, default="ALL")
            group_name = self._get_combo_value(r, self.COL_GROUP, default="")
            mode = self._get_combo_value(r, self.COL_MODE, default="Digi")
            band = self._get_combo_value(r, self.COL_BAND, default="")
            freq_text = self._get_text_value(r, self.COL_FREQ)
            start_val = self._get_text_value(r, self.COL_START)
            end_val = self._get_text_value(r, self.COL_END)
            auto_tune = self._get_checkbox_value(r, self.COL_AUTOTUNE)

            if not group_name or not band or not freq_text or not start_val or not end_val:
                continue

            # Enforce frequency validity for band/mode
            if not self._validate_frequency(band, mode, freq_text):
                return  # validation already warned the user
            freq_text = self._format_freq(freq_text)

            # Validate times
            if not self._validate_time(start_val) or not self._validate_time(end_val):
                errors.append(f"Row {r+1}: Start/End must be HH:MM (24h)")
                continue

            if self._show_local:
                day_utc, start_utc = self._convert_day_time(day, start_val, to_local=False)
                _, end_utc = self._convert_day_time(day, end_val, to_local=False)
            else:
                day_utc = day
                start_utc = start_val
                end_utc = end_val

            if is_sop:
                sel_wrap = self.table.cellWidget(r, self.COL_SELECT)
                profile_id = 0
                layer_id = 0
                recurrence = "Weekly"
                biweekly = 0
                month_weeks = ""
                vfo = "A"
                if isinstance(sel_wrap, QWidget):
                    try:
                        profile_id = int(sel_wrap.property("sop_overlay_profile_id") or 0)
                    except Exception:
                        profile_id = 0
                    try:
                        layer_id = int(sel_wrap.property("sop_overlay_layer_id") or 0)
                    except Exception:
                        layer_id = 0
                    recurrence = str(sel_wrap.property("sop_overlay_recurrence") or "Weekly").strip() or "Weekly"
                    try:
                        biweekly = int(sel_wrap.property("sop_overlay_biweekly") or 0)
                    except Exception:
                        biweekly = 0
                    month_weeks = str(sel_wrap.property("sop_overlay_month_weeks") or "").strip()
                    vfo = str(sel_wrap.property("sop_overlay_vfo") or "A").strip().upper() or "A"
                if profile_id <= 0:
                    errors.append(f"Row {r+1}: SOP row missing profile mapping; skipped.")
                    continue
                sop_updates.setdefault(profile_id, []).append(
                    {
                        "id": layer_id,
                        "day_utc": day_utc,
                        "recurrence": recurrence,
                        "biweekly_offset_weeks": biweekly,
                        "month_weeks": month_weeks,
                        "band": str(band).strip().upper(),
                        "mode": str(mode).strip().upper(),
                        "vfo": vfo,
                        "frequency": freq_text,
                        "start_utc": start_utc,
                        "end_utc": end_utc,
                        "enabled": True,
                    }
                )
                continue

            hf_rows.append(
                {
                    "day_utc": day_utc,
                    "band": band,
                    "mode": mode,
                    "vfo": "A",
                    "frequency": freq_text,
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "group_name": group_name,
                    "fldigi_offset": "",
                    "js8_offset": "",
                    "primary_js8call_group": "",
                    "comment": "",
                    "auto_tune": bool(auto_tune),
                }
            )

        if errors:
            QMessageBox.warning(
                self,
                "Partial Save",
                "Some rows were skipped:\n" + "\n".join(errors),
            )

        # Persist via SettingsManager
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("hf_schedule", hf_rows)
                self.settings.set("daily_schedule", hf_rows)  # keep legacy key in sync
                if hasattr(self.settings, "save"):
                    self.settings.save()
            else:
                data = self.settings.all()
                data["hf_schedule"] = hf_rows
                data["daily_schedule"] = hf_rows
                if hasattr(self.settings, "_data"):
                    self.settings._data = data  # type: ignore[attr-defined]
                if hasattr(self.settings, "save"):
                    self.settings.save()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Save Failed",
                f"Could not save HF schedule:\n{e}",
            )
            log.error("HF Frequency Schedule save failed: %s", e)
            return

        # Mirror to SQLite for scheduler_engine
        try:
            self._save_schedule_to_db(hf_rows)
        except Exception as e:
            log.error("HF Frequency Schedule DB save failed: %s", e)
            QMessageBox.warning(
                self,
                "DB Save Error",
                f"HF schedule saved to settings, but DB save failed:\n{e}",
            )
            return

        sop_changed = 0
        sop_failures: List[str] = []
        for profile_id, layer_rows in sop_updates.items():
            try:
                sop_changed += int(self._sop_manager.upsert_schedule_layer_rows(profile_id, layer_rows) or 0)
            except Exception as e:
                sop_failures.append(f"Profile {profile_id}: {e}")

        if sop_updates:
            self._dispatch_sop_schedule_change()

        if sop_failures:
            QMessageBox.warning(
                self,
                "SOP Save Warning",
                "Some SOP rows could not be updated:\n" + "\n".join(sop_failures[:12]),
            )

        QMessageBox.information(
            self,
            "Saved",
            f"HF Schedule saved. HF rows: {len(hf_rows)} | SOP rows updated: {sop_changed}.",
        )
        log.info("HF Frequency Schedule saved: %d HF rows, %d SOP rows updated", len(hf_rows), sop_changed)
        self._raw_schedule = hf_rows
        self._saved_rows_signature = self._rows_signature(self._collect_rows_for_signature())
        self._set_dirty(False)
        self._refresh_freq_planner()
        self._refresh_schedule_resources(force=True)
        self._refresh_schedule_issues(force=True)

    def _export_schedule(self):
        """
        Export HF schedule (no nets) to JSON with callsign in filename.
        """
        data = self.settings.all()
        callsign = (data.get("operator_callsign") or "").strip().upper() or "UNKNOWN"
        default_name = f"{callsign}-hf-schedule-{datetime.datetime.utcnow().strftime('%Y%m%d')}.json"
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export HF Schedule",
            default_name,
            "JSON Files (*.json);;All Files (*)",
        )
        if not path:
            return
        rows = self._raw_schedule if hasattr(self, "_raw_schedule") and self._raw_schedule else data.get("hf_schedule", [])
        if not rows:
            QMessageBox.warning(self, "Export", "No HF schedule rows to export.")
            return
        try:
            payload = {
                "callsign": callsign,
                "created_utc": datetime.datetime.utcnow().isoformat(),
                "rows": [],
            }
            for r in rows:
                payload["rows"].append(
                    {
                        "day_utc": r.get("day_utc", "ALL"),
                        "start_utc": r.get("start_utc", ""),
                        "end_utc": r.get("end_utc", ""),
                        "band": r.get("band", ""),
                        "mode": r.get("mode", ""),
                        "frequency": r.get("frequency", ""),
                    }
                )
            Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
            QMessageBox.information(self, "Exported", f"HF schedule exported to:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Failed", f"Could not export:\n{e}")
            log.error("HF schedule export failed: %s", e)

    # ---------------- Row helpers ---------------- #

    def _entry_for_display(self, entry: Dict) -> Dict:
        d = dict(entry)
        if self._show_local:
            day_loc, start_loc = self._convert_day_time(d.get("day_utc", ""), d.get("start_utc", ""), to_local=True)
            _, end_loc = self._convert_day_time(d.get("day_utc", ""), d.get("end_utc", ""), to_local=True)
            d["day_utc"] = day_loc  # reuse column but reflects view
            d["start_utc"] = start_loc
            d["end_utc"] = end_loc
        return d

    def _rebuild_from_raw(self):
        self._suspend_dirty_tracking = True
        try:
            self.table.setRowCount(0)
            for entry in self._raw_schedule:
                self._append_entry_row(self._entry_for_display(entry))
            if self.table.rowCount() == 0:
                self._add_row()
            self._append_sop_overlay_rows()
        finally:
            self._suspend_dirty_tracking = False
        self._set_headers()
        self._update_clock_labels()
        self._mark_dirty()
        self._highlight_time_conflicts()

    def _toggle_time_view(self):
        self._show_local = not self._show_local
        self._rebuild_from_raw()
        self._update_suspend_state()
        self._populate_schedule_resources_table()
        self._populate_schedule_issues_table()

    # --------- Suspend (shared across tabs) --------- #

    def _get_suspend_until(self) -> Optional[datetime.datetime]:
        try:
            if hasattr(self.settings, "reload"):
                self.settings.reload()
        except Exception:
            pass
        return get_suspend_until(self.settings)

    def _set_suspend_until(self, dt: Optional[datetime.datetime]) -> None:
        set_suspend_until(self.settings, dt)

    def _suspend_active(self) -> bool:
        return suspend_active(self.settings)

    def _scheduler_enabled(self) -> bool:
        return scheduler_enabled(self.settings)

    def _set_suspend_button(self, active: bool, remaining_sec: Optional[float] = None):
        theme = resolve_theme(self.settings)
        if active:
            mins = 0
            if remaining_sec is not None:
                mins = max(0, int((remaining_sec + 59) // 60))
            label = f"Sched. Paused: {mins} min" if mins else "Sched. Paused"
            self.suspend_btn.setText(label)
            self.suspend_btn.setStyleSheet(button_style("info", theme))
        else:
            self.suspend_btn.setText("QSY")
            self.suspend_btn.setStyleSheet(button_style("warning", theme))
        self._update_qsy_button_enabled()

    def _refresh_qsy_options(self):
        """
        Build a unique frequency list from Operating Groups (auto-tune wins on duplicates).
        """
        ops = self._load_operating_groups()
        self._qsy_options = build_qsy_options(ops)
        refresh_qsy_combo(self.qsy_combo, self._qsy_options)
        self._update_qsy_button_enabled()

    def _selected_qsy_meta(self) -> Optional[Dict]:
        return selected_qsy_meta(self.qsy_combo)

    def _current_scheduler_freq(self) -> Optional[float]:
        return current_scheduler_freq(self.window())

    def _update_qsy_button_enabled(self):
        if self._suspend_active():
            self.suspend_btn.setEnabled(True)
            return
        enabled = self._scheduler_enabled()
        meta = self._selected_qsy_meta()
        if not enabled or not meta:
            self.suspend_btn.setEnabled(False)
            return
        cur = self._current_scheduler_freq()
        if cur is not None and abs(cur - meta.get("freq", -1)) < 0.001:
            self.suspend_btn.setEnabled(False)
        else:
            self.suspend_btn.setEnabled(True)

    def _perform_qsy(self, meta: Dict) -> bool:
        win = self.window()
        return perform_qsy(win, meta)

    def _update_suspend_state(self):
        enabled = self._scheduler_enabled()
        self.suspend_btn.setEnabled(enabled)
        if not enabled:
            self._set_suspend_button(False)
            return

        dt = self._get_suspend_until()
        if dt and datetime.datetime.now(datetime.timezone.utc) < dt:
            remaining = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            self._set_suspend_button(True, remaining_sec=remaining)
        else:
            if dt:
                self._set_suspend_until(None)
            self._set_suspend_button(False)
        self._update_qsy_button_enabled()

    def _refresh_freq_planner(self) -> None:
        """
        Ask the main window to refresh the Frequency Planner after schedule changes.
        """
        try:
            self.schedule_saved.emit()
        except Exception:
            pass

    def on_settings_saved(self):
        """
        Refresh operating groups/QSY options when settings are saved.
        """
        try:
            self.settings.reload()
        except Exception:
            pass
        latest = self._load_operating_groups()
        self.operating_groups = latest
        self._operating_groups_sig = self._snapshot_operating_groups(latest)
        prev_suppress = self._suppress_autostart
        self._suppress_autostart = True
        prev_dirty = self._suspend_dirty_tracking
        self._suspend_dirty_tracking = True
        try:
            self._refresh_group_band_cells()
        finally:
            self._suppress_autostart = prev_suppress
            self._suspend_dirty_tracking = prev_dirty
        self._refresh_sop_overlay_rows_in_table()
        self._refresh_qsy_options()
        self._apply_theme()
        self._refresh_sop_profiles_panel(force=True)
        self._refresh_schedule_resources(force=True)

    def on_sop_data_changed(self) -> None:
        self._refresh_sop_overlay_rows_in_table()
        self._refresh_sop_profiles_panel(force=True)
        self._update_effective_source_label()
        self._refresh_schedule_resources(force=True)

    def on_tab_activated(self) -> None:
        self._refresh_sop_overlay_rows_in_table()
        self._refresh_sop_profiles_panel(force=True)
        self._update_effective_source_label()
        self._update_suspend_state()
        self._refresh_schedule_resources(force=True)

    def _on_suspend_clicked(self):
        if self._suspend_active():
            self._set_suspend_until(None)
            self._set_suspend_button(False)
            QMessageBox.information(self, "Scheduling", "Scheduling resumed.")
        else:
            meta = self._selected_qsy_meta()
            if not meta:
                QMessageBox.warning(self, "QSY", "Select a frequency to QSY to.")
                return
            if not self._perform_qsy(meta):
                return
            new_until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=30)
            self._set_suspend_until(new_until)
            remaining = (new_until - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            self._set_suspend_button(True, remaining_sec=remaining)
            QMessageBox.information(self, "QSY Applied", "Frequency changed and scheduling paused for 30 minutes.")

    def _apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self._update_time_toggle_style(theme)
        self._update_effective_source_label(theme)
        self.sop_runtime_box.setStyleSheet(
            "QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 4px; }"
        )
        self.add_row_btn.setStyleSheet(button_style("primary", theme))
        self._refresh_save_button_state(theme)
        self.export_btn.setStyleSheet(button_style("info", theme))
        self.resources_refresh_btn.setStyleSheet(button_style("muted", theme))
        self._update_suspend_state()
        self._update_delete_button_state()
        self._refresh_sop_profiles_panel(force=True, theme=theme)
        self._populate_schedule_resources_table()

    def apply_theme(self) -> None:
        self._apply_theme()

    def _rows_signature(self, rows: List[Dict]) -> str:
        normalized: List[Dict[str, object]] = []
        for row in rows:
            normalized.append(
                {
                    "source": str(row.get("source", "HF")),
                    "source_key": str(row.get("source_key", "")),
                    "day_utc": str(row.get("day_utc", "")),
                    "group_name": str(row.get("group_name", "")),
                    "mode": str(row.get("mode", "")),
                    "band": str(row.get("band", "")),
                    "frequency": str(row.get("frequency", "")),
                    "start_utc": str(row.get("start_utc", "")),
                    "end_utc": str(row.get("end_utc", "")),
                    "auto_tune": bool(row.get("auto_tune", False)),
                }
            )
        return json.dumps(normalized, sort_keys=True)

    def _collect_rows_for_signature(self) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for r in range(self.table.rowCount()):
            is_sop = self._is_sop_overlay_row(r)
            row = self._active_row_to_utc(r, include_sop_overlay=True)
            if row is None:
                continue
            rows.append(
                {
                    "source": "SOP" if is_sop else "HF",
                    "source_key": self._sop_overlay_source_key(r) if is_sop else "",
                    "day_utc": str(row.get("day_utc") or ""),
                    "group_name": str(row.get("group_name") or ""),
                    "mode": str(row.get("mode") or ""),
                    "band": str(row.get("band") or ""),
                    "frequency": str(row.get("frequency") or ""),
                    "start_utc": str(row.get("start_utc") or ""),
                    "end_utc": str(row.get("end_utc") or ""),
                    "auto_tune": bool(row.get("auto_tune", False)),
                }
            )
        return rows

    def _refresh_save_button_state(self, theme: Optional[Dict[str, str]] = None) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        role = "eligible_success" if self._dirty else "muted"
        self.save_btn.setStyleSheet(button_style(role, theme))

    def _set_dirty(self, dirty: bool) -> None:
        self._dirty = bool(dirty)
        self._refresh_save_button_state()

    def _mark_dirty(self, *_args) -> None:
        if self._suspend_dirty_tracking:
            return
        try:
            current_sig = self._rows_signature(self._collect_rows_for_signature())
            self._set_dirty(current_sig != self._saved_rows_signature)
        except Exception:
            self._set_dirty(True)

    def _on_table_item_changed(self, _item: QTableWidgetItem) -> None:
        self._mark_dirty()
        self._highlight_time_conflicts()

    def _has_delete_selection(self) -> bool:
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, self.COL_SELECT)
            if isinstance(w, QCheckBox) and w.isChecked():
                return True
            if isinstance(w, QWidget):
                chk = w.findChild(QCheckBox)
                if chk is not None and chk.isChecked():
                    return True
        return bool(self.table.selectedIndexes())

    def _update_delete_button_state(self) -> None:
        theme = resolve_theme(self.settings)
        has_selection = self._has_delete_selection()
        self.del_row_btn.setEnabled(has_selection)
        role = "eligible_danger" if has_selection else "muted"
        self.del_row_btn.setStyleSheet(button_style(role, theme))
        self.move_to_resources_btn.setEnabled(has_selection)
        self.move_to_resources_btn.setStyleSheet(
            button_style("eligible_warning" if has_selection else "muted", theme)
        )

    def _append_entry_row(self, entry: Dict):
        row = self.table.rowCount()
        self.table.insertRow(row)
        is_sop_overlay = bool(entry.get("_sop_overlay"))
        overlay_profile = str(entry.get("_sop_profile_name") or "").strip()
        overlay_tip = (
            "SOP layer entry. Edit in Active Schedule and Save to update SOP schedule."
            if is_sop_overlay
            else ""
        )

        # Select checkbox
        sel_chk = QCheckBox()
        sel_chk.stateChanged.connect(self._update_delete_button_state)
        sel_chk.setEnabled(True)
        sel_wrap = QWidget()
        sel_layout = QHBoxLayout(sel_wrap)
        sel_layout.setContentsMargins(0, 0, 0, 0)
        sel_layout.setAlignment(Qt.AlignCenter)
        sel_layout.addWidget(sel_chk)
        try:
            sel_wrap.setProperty("resource_id", int(entry.get("_resource_id") or 0))
        except Exception:
            sel_wrap.setProperty("resource_id", 0)
        sel_wrap.setProperty("resource_set", str(entry.get("_resource_set") or ""))
        sel_wrap.setProperty("sop_overlay", is_sop_overlay)
        sel_wrap.setProperty("sop_profile_name", overlay_profile)
        sel_wrap.setProperty("sop_overlay_source_key", str(entry.get("_source_key") or "").strip())
        sel_wrap.setProperty("sop_overlay_group_name", str(entry.get("group_name") or "").strip())
        sel_wrap.setProperty("sop_overlay_profile_id", int(entry.get("_sop_profile_id") or 0))
        sel_wrap.setProperty("sop_overlay_layer_id", int(entry.get("_sop_layer_id") or 0))
        sel_wrap.setProperty("sop_overlay_recurrence", str(entry.get("_sop_recurrence") or "Weekly"))
        sel_wrap.setProperty("sop_overlay_biweekly", int(entry.get("_sop_biweekly_offset_weeks") or 0))
        sel_wrap.setProperty("sop_overlay_month_weeks", str(entry.get("_sop_month_weeks") or ""))
        sel_wrap.setProperty("sop_overlay_vfo", str(entry.get("_sop_vfo") or "A"))
        self.table.setCellWidget(row, self.COL_SELECT, sel_wrap)

        # Source
        source_item = QTableWidgetItem("SOP" if is_sop_overlay else "HF")
        source_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        if overlay_tip:
            source_item.setToolTip(overlay_tip)
        self.table.setItem(row, self.COL_SOURCE, source_item)

        # Day
        day_combo = QComboBox()
        day_combo.addItems(DAY_OPTIONS)
        day_val = (entry.get("day_utc") or "ALL").strip()
        if day_val not in DAY_OPTIONS:
            day_val = "ALL"
        day_combo.setCurrentText(day_val)
        day_combo.setEnabled(True)
        day_combo.currentTextChanged.connect(self._mark_dirty)
        if overlay_tip:
            day_combo.setToolTip(overlay_tip)
        self.table.setCellWidget(row, self.COL_DAY, day_combo)

        # Group (from operating groups)
        group_combo = QComboBox()
        group_val = (entry.get("group_name") or "").strip()
        if is_sop_overlay:
            group_display = f"SOP:{group_val}" if group_val else "SOP"
            group_combo.addItem(group_display)
            group_combo.setCurrentText(group_display)
            group_combo.setEnabled(False)
            if overlay_tip:
                group_combo.setToolTip(overlay_tip)
        else:
            group_names = sorted({g.get("group", "") for g in self.operating_groups if g.get("group")})
            group_combo.addItems(group_names)
            if group_val and group_val in group_names:
                group_combo.setCurrentText(group_val)
            group_combo.currentTextChanged.connect(self._mark_dirty)
        self.table.setCellWidget(row, self.COL_GROUP, group_combo)

        # Band
        band_combo = QComboBox()
        band_val = (entry.get("band") or "").strip()
        if is_sop_overlay:
            band_combo.addItem(band_val)
            band_combo.setCurrentText(band_val)
            band_combo.setEnabled(False)
            if overlay_tip:
                band_combo.setToolTip(overlay_tip)
        else:
            self._populate_band_combo(band_combo, group_combo.currentText())
            if band_val and band_combo.findText(band_val) >= 0:
                band_combo.setCurrentText(band_val)
            band_combo.currentTextChanged.connect(self._mark_dirty)
        self.table.setCellWidget(row, self.COL_BAND, band_combo)

        # Mode + Frequency
        if is_sop_overlay:
            mode_combo = QComboBox()
            mode_combo.addItem(str(entry.get("mode") or "").strip().upper())
            mode_combo.setEnabled(False)
            if overlay_tip:
                mode_combo.setToolTip(overlay_tip)
            self.table.setCellWidget(row, self.COL_MODE, mode_combo)

        freq_item = QTableWidgetItem()
        freq_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        freq_item.setText(str(entry.get("frequency") or ""))
        if overlay_tip:
            freq_item.setToolTip(overlay_tip)
        self.table.setItem(row, self.COL_FREQ, freq_item)

        # Start / End
        st_item = QTableWidgetItem(entry.get("start_utc", ""))
        self._make_editable(st_item)
        if overlay_tip:
            st_item.setToolTip(overlay_tip)
        st_item.setData(Qt.UserRole, st_item.toolTip())
        self.table.setItem(row, self.COL_START, st_item)

        en_item = QTableWidgetItem(entry.get("end_utc", ""))
        self._make_editable(en_item)
        if overlay_tip:
            en_item.setToolTip(overlay_tip)
        en_item.setData(Qt.UserRole, en_item.toolTip())
        self.table.setItem(row, self.COL_END, en_item)

        # Auto-Tune
        chk = QCheckBox()
        chk.setChecked(bool(entry.get("auto_tune", False)))
        chk.setTristate(False)
        chk.setEnabled(not is_sop_overlay)
        if not is_sop_overlay:
            chk.stateChanged.connect(self._mark_dirty)
        auto_wrap = QWidget()
        auto_layout = QHBoxLayout(auto_wrap)
        auto_layout.setContentsMargins(0, 0, 0, 0)
        auto_layout.setAlignment(Qt.AlignCenter)
        auto_layout.addWidget(chk)
        self.table.setCellWidget(row, self.COL_AUTOTUNE, auto_wrap)

        if is_sop_overlay:
            self._update_delete_button_state()
            return

        # wiring for group/band changes
        def on_group_changed(text: str, self=self, row=row, band_combo=band_combo):
            self._populate_band_combo(band_combo, text)
            # auto-select first band
            if band_combo.count() > 0:
                band_combo.setCurrentIndex(0)
            self._update_mode_freq(row)

        def on_band_changed(text: str, self=self, row=row):
            self._update_mode_freq(row)

        group_combo.currentTextChanged.connect(on_group_changed)
        band_combo.currentTextChanged.connect(on_band_changed)
        # Ensure initial mode/freq selection is synced to operating group data
        self._update_mode_freq(row)
        self._update_delete_button_state()

    def _add_row(self):
        self._append_entry_row({})
        self.table.scrollToBottom()
        self._mark_dirty()
        self._highlight_time_conflicts()

    def _delete_selected_rows(self):
        selected = set()
        removed_hf = 0
        # Prefer checkbox selection
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, self.COL_SELECT)
            if isinstance(w, QCheckBox) and w.isChecked():
                selected.add(r)
            if isinstance(w, QWidget):
                chk = w.findChild(QCheckBox)
                if chk is not None and chk.isChecked():
                    selected.add(r)
        # Fallback to selected cells if no checkboxes are ticked
        if not selected:
            for idx in self.table.selectedIndexes():
                selected.add(idx.row())
        for r in sorted(selected, reverse=True):
            if self._is_sop_overlay_row(r):
                source_key = self._sop_overlay_source_key(r)
                if source_key:
                    self._hidden_sop_overlay_keys.add(source_key)
            else:
                removed_hf += 1
            self.table.removeRow(r)
        self._update_delete_button_state()
        if removed_hf > 0:
            self._mark_dirty()
        self._highlight_time_conflicts()

    # ---------------- Cell access helpers ---------------- #

    def _get_combo_value(self, row: int, col: int, default: str = "") -> str:
        w = self.table.cellWidget(row, col)
        if isinstance(w, QComboBox):
            return w.currentText().strip()
        item = self.table.item(row, col)
        if item is not None:
            return item.text().strip()
        return default

    def _get_checkbox_value(self, row: int, col: int) -> bool:
        w = self.table.cellWidget(row, col)
        if isinstance(w, QCheckBox):
            return w.isChecked()
        if isinstance(w, QWidget):
            chk = w.findChild(QCheckBox)
            if chk is not None:
                return chk.isChecked()
        return False

    def _get_text_value(self, row: int, col: int) -> str:
        w = self.table.cellWidget(row, col)
        if isinstance(w, QComboBox):
            return w.currentText().strip()
        item = self.table.item(row, col)
        if item is None:
            return ""
        return item.text().strip()

    def _make_editable(self, item: QTableWidgetItem):
        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable)

    def _populate_band_combo(self, band_combo: QComboBox, group_name: str):
        band_combo.blockSignals(True)
        band_combo.clear()
        bands = sorted(
            {g.get("band") for g in self.operating_groups if g.get("group") == group_name and g.get("band")}
        )
        for b in bands:
            band_combo.addItem(b)
        band_combo.blockSignals(False)

    def _matching_operating_groups(self, group: str, band: str) -> List[Dict]:
        return [
            g
            for g in self.operating_groups
            if g.get("group") == group and g.get("band") == band
        ]

    def _set_mode_widget(
        self, row: int, group: str, band: str, preferred_mode: str = "", entries: Optional[List[Dict]] = None
    ) -> QComboBox:
        if entries is None:
            entries = self._matching_operating_groups(group, band)
        modes = sorted({(e.get("mode") or "").strip() for e in entries if e.get("mode")})
        combo = QComboBox()
        if modes:
            combo.addItems(modes)
        else:
            combo.addItem("")
        if preferred_mode and preferred_mode in modes:
            combo.setCurrentText(preferred_mode)
        elif modes:
            combo.setCurrentIndex(0)
        combo.setEnabled(len(modes) > 1)
        combo.currentTextChanged.connect(lambda _m, r=row: self._update_mode_freq(r))
        combo.currentTextChanged.connect(self._mark_dirty)
        self.table.setCellWidget(row, self.COL_MODE, combo)
        return combo

    def _update_mode_freq(self, row: int):
        group = self._get_combo_value(row, self.COL_GROUP, "")
        band = self._get_combo_value(row, self.COL_BAND, "")
        entries = self._matching_operating_groups(group, band)
        preferred_mode = self._get_combo_value(row, self.COL_MODE, "")
        mode_combo = self._set_mode_widget(row, group, band, preferred_mode, entries)
        mode_val = mode_combo.currentText().strip() if isinstance(mode_combo, QComboBox) else preferred_mode
        entry = None
        for g in entries:
            if (g.get("mode") or "").strip() == mode_val:
                entry = g
                break
        if entry is None and entries:
            entry = entries[0]
            if isinstance(mode_combo, QComboBox):
                mode_combo.blockSignals(True)
                mode_combo.setCurrentText(entry.get("mode", ""))
                mode_combo.blockSignals(False)
            mode_val = entry.get("mode", "")
        freq_val = self._format_freq(entry.get("frequency", "")) if entry else ""
        freq_item = self.table.item(row, self.COL_FREQ)
        if freq_item is None:
            freq_item = QTableWidgetItem()
            freq_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            self.table.setItem(row, self.COL_FREQ, freq_item)
        freq_item.setText(freq_val)
        # trigger autostart if mode changes
        if mode_val:
            self._auto_start_for_mode(mode_val)

    # ---------------- Auto-start radio software ---------------- #

    def _auto_start_for_mode(self, mode: str):
        """
        Start radio programs according to mode, if their Auto-Start flags
        are enabled in Settings and the programs are not already running.

        - JS8: JS8Call
        - Digi: FLDigi, FLMsg, FLAmp
        - Tri: all radio programs
        - SSB: no auto-start
        """
        if getattr(self, "_suppress_autostart", False):
            return
        if self._is_truthy(self.settings.get("launch_control_enabled", True)):
            return

        mode = (mode or "").strip().upper()
        if not mode:
            return

        if mode == "DIGI":
            programs = ["FLDigi", "FLMsg", "FLAmp"]
        else:
            # SSB (or anything else): no auto-start
            return

        allowed_autostart = {"FLDigi", "FLMsg", "FLAmp"}
        programs = [p for p in programs if p in allowed_autostart]
        if not programs:
            return

        for prog in programs:
            self._launch_program_if_autostart_enabled(prog)

    def _launch_program_if_autostart_enabled(self, prog_name: str):
        if prog_name not in {"FLDigi", "FLMsg", "FLAmp"}:
            return
        meta = PROGRAMS.get(prog_name)
        if not meta:
            return

        autostart_key = meta["autostart_key"]
        autostart = self._is_truthy(self.settings.get(autostart_key, False))
        if not autostart:
            return

        if self._program_is_running(prog_name, meta):
            return

        path_str = self.settings.get(meta["path_key"], "") or ""
        if path_str:
            exe_path = Path(path_str)
            cmd = [str(exe_path)]
        else:
            cmd = [meta["default_cmd"]]

        try:
            if platform.system() == "Windows":
                subprocess.Popen(
                    cmd,
                    shell=False,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
                )
            else:
                subprocess.Popen(cmd)
            log.info("DailyScheduleTab: auto-started %s via %r", prog_name, cmd)
        except Exception as e:
            log.error("DailyScheduleTab: failed to auto-start %s via %r: %s", prog_name, cmd, e)

    def _program_is_running(self, prog_name: str, meta: Dict) -> bool:
        """
        Check if a program is already running using the shared status service.
        """
        _ = meta  # Kept for signature compatibility with existing call sites.
        try:
            return bool(self._status_service.program_is_running(prog_name))
        except Exception:
            return False

    @staticmethod
    def _is_truthy(val) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        if isinstance(val, str):
            return val.strip().lower() in {"true", "1", "yes", "on"}
        return False

    # ---------------- Validation ---------------- #

    def _validate_time(self, text: str) -> bool:
        text = (text or "").strip()
        if not text:
            return False
        try:
            h, m = text.split(":")
            h = int(h)
            m = int(m)
            return 0 <= h <= 23 and 0 <= m <= 59
        except Exception:
            return False

    def _validate_frequency(self, band: str, mode: str, freq_text: str) -> bool:
        """
        Validate frequency based on band/mode constraints.

        Modes are now JS8, Digi, Tri, SSB.

        For band/mode limits, we treat:
          - JS8 as Digi
          - Tri as Digi
        """
        band = (band or "").strip().upper()
        mode_raw = (mode or "").strip().title()
        # Map JS8 and Tri to Digi for band-plan limits
        if mode_raw in ("Js8", "Tri"):
            eff_mode = "Digi"
        else:
            eff_mode = mode_raw

        freq_text = (freq_text or "").strip()
        if not freq_text:
            QMessageBox.warning(self, "Missing Frequency", "Frequency is required for all HF schedule rows.")
            return False

        # Parse frequency; handle "5.358.500" style if user types with extra dot
        try:
            normalized = freq_text.replace(",", ".").replace(" ", "")
            parts = normalized.split(".")
            if len(parts) > 2:
                normalized = parts[0] + "." + "".join(parts[1:])
            freq = float(normalized)
        except Exception:
            QMessageBox.warning(
                self,
                "Invalid Frequency",
                f"Frequency '{freq_text}' is not a valid number.",
            )
            return False

        # Special 60M handling
        if band == "60M":
            allowed = [5.332, 5.348, 5.3585, 5.373, 5.405]
            for a in allowed:
                if abs(freq - a) < 0.0005:
                    return True
            QMessageBox.warning(
                self,
                "Invalid 60M Frequency",
                "On 60M the only allowed channels are:\n"
                " 5.332, 5.348, 5.358.500, 5.373, 5.405 MHz",
            )
            return False

        # Range table
        ranges = {
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
            ("30M", "Any"): (10.100, 10.150),
        }

        key = (band, eff_mode)
        any_key = (band, "Any")

        if key in ranges:
            lo, hi = ranges[key]
        elif any_key in ranges:
            lo, hi = ranges[any_key]
        else:
            # If band not in our table, accept anything
            return True

        if not (lo <= freq <= hi):
            QMessageBox.warning(
                self,
                "Frequency out of range",
                f"{band} {mode_raw}: {freq:.3f} MHz is outside allowed range "
                f"{lo:.3f} - {hi:.3f} MHz.",
            )
            return False

        return True
