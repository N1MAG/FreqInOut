
from __future__ import annotations

import datetime
import html
import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from PySide6.QtCore import QTimer, Qt, Signal
from PySide6.QtGui import QColor, QFontMetrics, QPageLayout, QPageSize, QTextDocument
from PySide6.QtPrintSupport import QPrinter
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QCompleter,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sop_manager import SOPManager
from freqinout.gui.theme import resolve_theme, button_style
from freqinout.utils.timezones import get_timezone


def _contrast_text_hex(bg_hex: str) -> str:
    h = (bg_hex or "").strip().lstrip("#")
    if len(h) != 6:
        return "#111111"
    try:
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
    except Exception:
        return "#111111"
    yiq = ((r * 299) + (g * 587) + (b * 114)) / 1000
    return "#111111" if yiq >= 140 else "#FFFFFF"


class _LegacySOPTab(QWidget):
    """
    SOP reminders tab.
    Reminder-only workflow with manual completion and UTC-driven cadence.
    Terminology:
      - Group Name = HF Operating Group mapping
      - SOP Group = optional SOP profile grouping label
      - SOP Category = HF or Local Net action context
    """

    CONTACT_RULE_OPTIONS = [
        ("none", "None"),
        ("hub_or_hub_alt", "HUB OR HUB-ALT"),
        ("ncs_or_ancs", "NCS OR ANCS"),
        ("peer", "PEER"),
        ("callsign", "CallSign"),
    ]
    ANY_ROLE_TOKEN = "__any_role__"
    INTERVAL_PRESETS = ["00:30", "01:00", "03:00", "06:00", "12:00"]
    SOFTWARES = ["JS8Call", "VarAC", "FLDigi"]
    LOCAL_NET_SOFTWARE = "Local Net"
    LOCAL_NET_ACTION_NCS_KEY = "local_ncs"
    LOCAL_NET_ACTION_CHECKIN_KEY = "local_checkin"
    LOCAL_NET_ACTION_MESSAGE_KEY = "local_message"
    # Legacy key retained for backward compatibility with existing profiles.
    LOCAL_NET_ACTION_KEY = "local_open_net"
    LOCAL_NET_ACTION_KEYS = (
        LOCAL_NET_ACTION_NCS_KEY,
        LOCAL_NET_ACTION_CHECKIN_KEY,
        LOCAL_NET_ACTION_MESSAGE_KEY,
        LOCAL_NET_ACTION_KEY,
    )
    BAND_CHOICES = ["160M", "80M", "60M", "40M", "30M", "20M", "17M", "15M", "12M", "10M", "6M"]
    sop_data_changed = Signal()

    COL_BAND = 0
    COL_FREQ = 1
    COL_SOFTWARE = 2
    COL_ACTION = 3
    COL_INTERVAL = 4
    COL_CONTACT = 5
    COL_CONTACT_TARGET = 6
    COL_DESC = 7
    COL_REMOVE = 8

    LAYER_COL_DAY = 0
    LAYER_COL_RECURRENCE = 1
    LAYER_COL_MONTH_WEEKS = 2
    LAYER_COL_START = 3
    LAYER_COL_END = 4
    LAYER_COL_BAND = 5
    LAYER_COL_FREQ = 6
    LAYER_COL_MODE = 7
    LAYER_COL_REMOVE = 8

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self.manager = SOPManager()
        self._profiles: List[Dict[str, Any]] = []
        self._selected_profile_id: int | None = None
        self._upcoming_rows: List[Dict[str, Any]] = []
        self._loading_ui = False
        self._operating_groups: List[Dict[str, Any]] = []
        self._local_net_profiles: List[Dict[str, str]] = []
        self._hidden_actions: List[Dict[str, Any]] = []
        default_mode = (self.settings.get("display_time_mode", "LOCAL") or "LOCAL").upper()
        self._show_local = default_mode != "UTC"
        self._dirty = False
        self._layer_time_header_tag = ""
        self._layer_sync_out_of_sync = False
        self._layer_sync_has_basis = False
        self._layer_sync_cache_key: Tuple[Any, ...] | None = None
        self._layer_sync_cache_value: Tuple[List[Dict[str, Any]], List[str], int] | None = None

        self._build_ui()
        self._set_save_dirty(False)
        self._refresh_reference_data()
        self._reload_profiles(select_id=None)
        self.refresh_upcoming()

        self._timer = QTimer(self)
        self._timer.setInterval(30_000)
        self._timer.timeout.connect(self.refresh_upcoming)
        self._timer.start()

        self._clock_timer = QTimer(self)
        self._clock_timer.setInterval(1000)
        self._clock_timer.timeout.connect(self._update_clock_labels)
        self._clock_timer.start()
        self._update_clock_labels()

        self._layer_sync_timer = QTimer(self)
        self._layer_sync_timer.setSingleShot(True)
        self._layer_sync_timer.setInterval(220)
        self._layer_sync_timer.timeout.connect(self._refresh_layer_sync_hint)
        self._schedule_layer_sync_refresh()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)

        title_row = QHBoxLayout()
        title_row.addWidget(QLabel("<h3>SOP Builder</h3>"))
        title_row.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        title_row.addWidget(self.utc_label)
        title_row.addWidget(self.local_label)
        self.time_toggle_btn = QPushButton("Showing: Local")
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        title_row.addWidget(self.time_toggle_btn)
        root.addLayout(title_row)

        header = QHBoxLayout()
        self.profile_combo = QComboBox()
        self.profile_combo.setPlaceholderText("Select existing or add new...")
        self.profile_combo.currentIndexChanged.connect(self._on_profile_selected)
        header.addWidget(QLabel("SOP:"))
        header.addWidget(self.profile_combo, stretch=1)

        self.new_btn = QPushButton("New")
        self.save_btn = QPushButton("Save")
        self.delete_btn = QPushButton("Delete")
        self.export_pdf_btn = QPushButton("Export PDF")
        self.export_import_btn = QToolButton()
        self.export_import_btn.setText("Export/Import")
        self.export_import_btn.setPopupMode(QToolButton.InstantPopup)
        self.export_import_menu = QMenu(self.export_import_btn)
        self.export_json_action = self.export_import_menu.addAction("Export JSON")
        self.import_json_action = self.export_import_menu.addAction("Import JSON")
        self.export_import_btn.setMenu(self.export_import_menu)
        self.new_btn.clicked.connect(self._new_profile)
        self.save_btn.clicked.connect(self._save_profile)
        self.delete_btn.clicked.connect(self._delete_profile)
        self.export_pdf_btn.clicked.connect(self._export_pdf)
        self.export_json_action.triggered.connect(self._export_profile)
        self.import_json_action.triggered.connect(self._import_profile)
        for btn in (self.new_btn, self.save_btn, self.delete_btn, self.export_pdf_btn, self.export_import_btn):
            header.addWidget(btn)
        root.addLayout(header)

        cfg_box = QGroupBox("SOP Configuration")
        cfg_layout = QVBoxLayout(cfg_box)

        row1 = QHBoxLayout()
        self.name_edit = QLineEdit()
        self.group_combo = QComboBox()
        self.secondary_combo = QComboBox()
        self.group_combo.setToolTip("Group Name (HF Operating Group) drives HF band/frequency mapping.")
        self.secondary_combo.setToolTip("SOP Group is optional and can be used for SOP profile organization.")
        self.start_edit = QLineEdit()
        self.start_edit.setPlaceholderText("HH:MM")
        self.active_cb = QCheckBox("Active")
        row1.addWidget(QLabel("Name:"))
        row1.addWidget(self.name_edit, stretch=2)
        row1.addWidget(QLabel("Group Name (HF):"))
        row1.addWidget(self.group_combo, stretch=1)
        row1.addWidget(QLabel("SOP Group:"))
        row1.addWidget(self.secondary_combo, stretch=1)
        cfg_layout.addLayout(row1)
        self.terms_hint_label = QLabel(
            "SOP Category = HF or Local Net | SOP Group = optional profile grouping | "
            "Group Name = HF Operating Group frequency mapping"
        )
        self.terms_hint_label.setWordWrap(True)
        cfg_layout.addWidget(self.terms_hint_label)

        row2 = QHBoxLayout()
        self.start_label = QLabel("SOP Daily Start (UTC):")
        row2.addWidget(self.start_label)
        self.start_edit.setMinimumWidth(100)
        self.start_edit.setMaximumWidth(150)
        row2.addWidget(self.start_edit)
        row2.addWidget(QLabel("Priority:"))
        self.priority_spin = QSpinBox()
        self.priority_spin.setRange(1, 999)
        self.priority_spin.setValue(100)
        self.priority_spin.setToolTip("Lower number wins when multiple active SOP profiles overlap.")
        row2.addWidget(self.priority_spin)
        row2.addWidget(self.active_cb)
        row2.addStretch()
        cfg_layout.addLayout(row2)

        self.contact_label = QLabel("Primary Contacts: --")
        self.contact_label.setWordWrap(True)
        cfg_layout.addWidget(self.contact_label)

        rows_head = QHBoxLayout()
        rows_head.addWidget(QLabel("Action Rows (each row = one SOP reminder action)"))
        rows_head.addStretch()
        self.hidden_rows_label = QLabel("")
        rows_head.addWidget(self.hidden_rows_label)
        self.add_row_btn = QPushButton("Add Action Row")
        self.add_row_btn.clicked.connect(lambda: self._add_action_row(existing=None))
        rows_head.addWidget(self.add_row_btn)
        cfg_layout.addLayout(rows_head)

        self.actions_table = QTableWidget(0, 9)
        self.actions_table.setHorizontalHeaderLabels(
            [
                "Band",
                "Frequency",
                "Resource",
                "Action",
                "Interval (HH:MM)",
                "Contact Rule",
                "Contact Target",
                "Description",
                "Remove",
            ]
        )
        self.actions_table.verticalHeader().setVisible(False)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_BAND, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_FREQ, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_SOFTWARE, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_ACTION, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_INTERVAL, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_CONTACT, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_CONTACT_TARGET, QHeaderView.Fixed)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_DESC, QHeaderView.Stretch)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_REMOVE, QHeaderView.ResizeToContents)
        self.actions_table.setColumnWidth(self.COL_CONTACT_TARGET, 170)
        cfg_layout.addWidget(self.actions_table)
        root.addWidget(cfg_box)

        layer_box = QGroupBox("SOP Schedule Layer (Overrides HF While Active)")
        layer_layout = QVBoxLayout(layer_box)
        layer_hint = QLabel(
            "Optional schedule profile for this SOP. While SOP is Active, these rows supersede HF schedule. "
            "Net schedule remains highest priority."
        )
        layer_hint.setWordWrap(True)
        layer_layout.addWidget(layer_hint)

        layer_head = QHBoxLayout()
        layer_head.addWidget(QLabel("Layer Rows"))
        layer_head.addStretch()
        self.populate_layer_btn = QPushButton("Populate Layer from Actions")
        self.populate_layer_btn.clicked.connect(self._on_populate_layer_from_actions)
        layer_head.addWidget(self.populate_layer_btn)
        self.rebuild_layer_btn = QPushButton("Rebuild Layer Preview")
        self.rebuild_layer_btn.clicked.connect(self._on_rebuild_layer_preview)
        layer_head.addWidget(self.rebuild_layer_btn)
        self.add_layer_row_btn = QPushButton("Add Layer Row")
        self.add_layer_row_btn.clicked.connect(lambda: self._add_layer_row(existing=None))
        layer_head.addWidget(self.add_layer_row_btn)
        layer_layout.addLayout(layer_head)
        self.layer_sync_label = QLabel("")
        self.layer_sync_label.setWordWrap(True)
        self.layer_sync_label.setVisible(False)
        layer_layout.addWidget(self.layer_sync_label)

        self.layer_table = QTableWidget(0, 9)
        self.layer_table.setHorizontalHeaderLabels(
            ["Day", "Recurrence", "Weeks", "Start (UTC)", "End (UTC)", "Band", "Frequency", "Mode", "Remove"]
        )
        self.layer_table.verticalHeader().setVisible(False)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_DAY, QHeaderView.ResizeToContents)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_RECURRENCE, QHeaderView.ResizeToContents)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_MONTH_WEEKS, QHeaderView.ResizeToContents)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_START, QHeaderView.Stretch)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_END, QHeaderView.Stretch)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_BAND, QHeaderView.Stretch)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_FREQ, QHeaderView.Stretch)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_MODE, QHeaderView.Stretch)
        self.layer_table.horizontalHeader().setSectionResizeMode(self.LAYER_COL_REMOVE, QHeaderView.ResizeToContents)
        self.layer_table.horizontalHeader().setStretchLastSection(False)
        layer_layout.addWidget(self.layer_table)
        self.layer_validation_label = QLabel("")
        self.layer_validation_label.setWordWrap(True)
        self.layer_validation_label.setVisible(False)
        layer_layout.addWidget(self.layer_validation_label)
        self.layer_runtime_label = QLabel("")
        self.layer_runtime_label.setWordWrap(True)
        self.layer_runtime_label.setVisible(False)
        layer_layout.addWidget(self.layer_runtime_label)
        root.addWidget(layer_box)

        upcoming_box = QGroupBox("Upcoming SOP Actions")
        upcoming_layout = QVBoxLayout(upcoming_box)
        top = QHBoxLayout()
        self.horizon_spin = QSpinBox()
        self.horizon_spin.setRange(1, 48)
        self.horizon_spin.setValue(12)
        self.horizon_spin.valueChanged.connect(self.refresh_upcoming)
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh_upcoming)
        top.addWidget(QLabel("Show next N hours:"))
        top.addWidget(self.horizon_spin)
        top.addStretch()
        top.addWidget(self.refresh_btn)
        upcoming_layout.addLayout(top)

        self.alignment_label = QLabel("")
        self.alignment_label.setWordWrap(True)
        self.alignment_label.setVisible(False)
        upcoming_layout.addWidget(self.alignment_label)

        self.upcoming_table = QTableWidget(0, 8)
        self.upcoming_table.setHorizontalHeaderLabels(
            ["Profile", "Band/Freq", "Resource", "Action", "Description", "Next", "Contact", "Status"]
        )
        self.upcoming_table.verticalHeader().setVisible(False)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeToContents)
        upcoming_layout.addWidget(self.upcoming_table)
        root.addWidget(upcoming_box)

        self.group_combo.currentIndexChanged.connect(self._on_group_changed)
        self.secondary_combo.currentIndexChanged.connect(self._on_secondary_group_changed)
        self._wire_dirty_tracking()
        self._apply_accessibility_width_guards()

    def _apply_accessibility_width_guards(self) -> None:
        # Keep action labels readable when UI text size is increased.
        try:
            time_w = int(self.start_edit.fontMetrics().horizontalAdvance("23:59") + 26)
            base = self.start_edit.property("_fio_base_min_width")
            try:
                base_w = int(base)
            except Exception:
                base_w = 100
                self.start_edit.setProperty("_fio_base_min_width", base_w)
            self.start_edit.setFixedWidth(max(base_w, min(150, time_w)))
        except Exception:
            pass
        buttons = [
            self.time_toggle_btn,
            self.new_btn,
            self.save_btn,
            self.delete_btn,
            self.export_pdf_btn,
            self.export_import_btn,
            self.add_row_btn,
            self.populate_layer_btn,
            self.rebuild_layer_btn,
            self.add_layer_row_btn,
            self.refresh_btn,
        ]
        for btn in buttons:
            try:
                txt = str(btn.text() or "").strip()
            except Exception:
                txt = ""
            if not txt:
                continue
            try:
                needed = int(btn.fontMetrics().horizontalAdvance(txt.replace("&", "")) + 30)
            except Exception:
                continue
            try:
                current_min = int(btn.minimumWidth() or 0)
            except Exception:
                current_min = 0
            base = btn.property("_fio_base_min_width")
            try:
                base_w = int(base)
            except Exception:
                base_w = current_min
                btn.setProperty("_fio_base_min_width", base_w)
            target = max(base_w, min(360, needed))
            try:
                btn.setMinimumWidth(target)
            except Exception:
                pass

    def _refresh_reference_data(self) -> None:
        data = self.settings.all()
        og = data.get("operating_groups", [])
        self._operating_groups = [g for g in og if isinstance(g, dict)]
        self._load_local_net_profiles_from_data(data)

        self.group_combo.blockSignals(True)
        self.group_combo.clear()
        group_names = sorted({(g.get("group") or "").strip().upper() for g in self._operating_groups if g.get("group")})
        self.group_combo.addItem("")
        for name in group_names:
            self.group_combo.addItem(name)
        self.group_combo.blockSignals(False)

        self.secondary_combo.clear()
        self.secondary_combo.addItem("")
        for g in self.manager.load_secondary_groups():
            self.secondary_combo.addItem(g)

    def _load_local_net_profiles_from_data(self, data: Dict[str, Any]) -> None:
        local_profiles = data.get("local_net_profiles", [])
        self._local_net_profiles = []
        if isinstance(local_profiles, list):
            for raw in local_profiles:
                if not isinstance(raw, dict):
                    continue
                group = str(raw.get("group", raw.get("name", "")) or "").strip()
                if not group:
                    continue
                self._local_net_profiles.append(
                    {
                        "group": group,
                        "resource": str(raw.get("resource", raw.get("service", "")) or "").strip(),
                        "mode": str(raw.get("mode", "") or "").strip(),
                        "target": str(raw.get("target", "") or "").strip(),
                        "notes": str(raw.get("notes", "") or "").strip(),
                    }
                )
        self._local_net_profiles = sorted(
            self._local_net_profiles,
            key=lambda x: (
                str(x.get("group", "")).upper(),
                str(x.get("resource", "")).upper(),
                str(x.get("mode", "")).upper(),
                str(x.get("target", "")).upper(),
            ),
        )

    def _local_profile_names(self) -> List[str]:
        names = {
            str(p.get("group", "")).strip()
            for p in self._local_net_profiles
            if str(p.get("group", "")).strip()
        }
        return sorted(names, key=lambda x: x.upper())

    def _local_profile_lookup(self) -> Dict[str, List[Dict[str, str]]]:
        out: Dict[str, List[Dict[str, str]]] = {}
        for row in self._local_net_profiles:
            name = str(row.get("group", "")).strip()
            if not name:
                continue
            out.setdefault(name.upper(), []).append(dict(row))
        for rows in out.values():
            rows.sort(
                key=lambda r: (
                    str(r.get("resource", "")).upper(),
                    str(r.get("mode", "")).upper(),
                    str(r.get("target", "")).upper(),
                )
            )
        return out

    def _wire_dirty_tracking(self) -> None:
        self.name_edit.textChanged.connect(self._mark_dirty)
        self.group_combo.currentIndexChanged.connect(self._mark_dirty)
        self.secondary_combo.currentIndexChanged.connect(self._mark_dirty)
        self.start_edit.textChanged.connect(self._mark_dirty)
        self.priority_spin.valueChanged.connect(self._mark_dirty)
        self.active_cb.toggled.connect(self._mark_dirty)

    def _set_save_dirty(self, dirty: bool) -> None:
        self._dirty = bool(dirty)
        self._update_profile_action_styles()

    def _mark_dirty(self, *_args) -> None:
        if self._loading_ui:
            return
        self._set_save_dirty(True)
        self._schedule_layer_sync_refresh()

    def _schedule_layer_sync_refresh(self) -> None:
        if getattr(self, "_loading_ui", False):
            return
        timer = getattr(self, "_layer_sync_timer", None)
        if timer is None:
            return
        timer.start()

    def _update_profile_action_styles(self, theme: Dict[str, str] | None = None) -> None:
        try:
            if theme is None:
                theme = resolve_theme(self.settings)
            has_profile = self._selected_profile_id is not None
            has_active_profile = any(bool(p.get("active")) for p in (self._profiles or []))
            self.new_btn.setStyleSheet(button_style("eligible_info" if has_profile else "muted", theme))
            self.save_btn.setStyleSheet(button_style("eligible_success" if self._dirty else "muted", theme))
            self.delete_btn.setEnabled(has_profile)
            self.delete_btn.setStyleSheet(button_style("eligible_danger" if has_profile else "muted", theme))
            export_pdf_eligible = has_profile or has_active_profile
            self.export_pdf_btn.setEnabled(export_pdf_eligible)
            self.export_pdf_btn.setStyleSheet(button_style("eligible_info" if export_pdf_eligible else "muted", theme))
            self.export_import_btn.setEnabled(True)
            self.export_import_btn.setStyleSheet(button_style("muted", theme))
            self.refresh_btn.setStyleSheet(button_style("muted", theme))
            add_row_eligible = has_profile and self.add_row_btn.isEnabled()
            self.add_row_btn.setStyleSheet(
                button_style("eligible_primary" if add_row_eligible else "muted", theme)
            )
            add_layer_eligible = has_profile and self.add_layer_row_btn.isEnabled()
            self.add_layer_row_btn.setStyleSheet(
                button_style("eligible_primary" if add_layer_eligible else "muted", theme)
            )
            has_non_local = len(self._collect_action_rows_for_layer_seed()) > 0
            populate_eligible = bool(self.group_combo.currentText().strip().upper()) and has_non_local
            rebuild_role = "muted"
            if populate_eligible:
                if self._layer_sync_has_basis and self._layer_sync_out_of_sync:
                    rebuild_role = "eligible_warning"
                else:
                    rebuild_role = "eligible_info"
            self.populate_layer_btn.setEnabled(populate_eligible)
            self.rebuild_layer_btn.setEnabled(populate_eligible)
            self.populate_layer_btn.setStyleSheet(
                button_style("eligible_info" if populate_eligible else "muted", theme)
            )
            self.rebuild_layer_btn.setStyleSheet(
                button_style(rebuild_role, theme)
            )
        except Exception:
            pass

    def _update_time_toggle_style(self, theme: Dict[str, str] | None = None) -> None:
        try:
            if theme is None:
                theme = resolve_theme(self.settings)
            # Local is default display mode; only emphasize UTC override.
            role = "info" if not self._show_local else "muted"
            self.time_toggle_btn.setStyleSheet(button_style(role, theme))
        except Exception:
            pass

    def _configured_softwares(self) -> List[str]:
        data = self.settings.all()
        configured: List[str] = []
        if (data.get("js8_directed_path") or "").strip() or (data.get("js8_forms_path") or "").strip():
            configured.append("JS8Call")
        msg_paths = data.get("message_paths", {}) or {}
        if (data.get("varac_path") or "").strip() or str(msg_paths.get("varac") or "").strip():
            configured.append("VarAC")
        if (
            (data.get("path_fldigi") or "").strip()
            or (data.get("fldigi_log_path") or "").strip()
            or (data.get("fldigi_checkin_dir") or "").strip()
        ):
            configured.append("FLDigi")
        if self.LOCAL_NET_SOFTWARE not in configured:
            configured.append(self.LOCAL_NET_SOFTWARE)
        return configured

    def _frequency_options_for_group(self, group: str) -> List[str]:
        return self._frequency_options_for_group_band(group, "")

    def _frequency_options_for_group_band(self, group: str, band: str) -> List[str]:
        grp = (group or "").strip().upper()
        band_uc = (band or "").strip().upper()
        values = []
        for row in self._operating_groups:
            if (row.get("group") or "").strip().upper() != grp:
                continue
            row_band = (row.get("band") or "").strip().upper()
            if band_uc and row_band != band_uc:
                continue
            try:
                values.append(f"{float(row.get('frequency', 0)):.3f}")
            except Exception:
                pass
        return sorted(set(values), key=lambda x: float(x)) if values else []

    def _band_options_for_group(self, group: str) -> List[str]:
        grp = (group or "").strip().upper()
        values = set()
        for row in self._operating_groups:
            if (row.get("group") or "").strip().upper() != grp:
                continue
            band = (row.get("band") or "").strip().upper()
            if band:
                values.add(band)
        return sorted(values, key=lambda x: (len(x), x))

    def _load_spotter_forms(self) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        forms_dir = Path(self.settings.get("js8_forms_path", "") or "")
        if not forms_dir.exists():
            return out
        for fn in sorted(forms_dir.glob("MCF*.txt")):
            try:
                num = fn.stem.replace("MCF", "").strip()
                if not num.isdigit():
                    continue
                code = f"F!{num}"
                out.append((f"js8_spotter_{code}", code))
            except Exception:
                continue
        return out

    def _action_catalog(self) -> Dict[str, List[Tuple[str, str]]]:
        catalog: Dict[str, List[Tuple[str, str]]] = {
            "JS8Call": [("js8_send_status", "Status"), ("js8_commstat", "CommStat")],
            "VarAC": [
                ("varac_send_broadcast", "Broadcast"),
                ("varac_direct_contact", "Direct Contact"),
                ("varac_send_sitrep", "SitRep"),
                ("varac_send_statrep", "StatRep"),
                ("varac_send_report", "General"),
            ],
            "FLDigi": [
                ("fldigi_send_sitrep", "SitRep"),
                ("fldigi_send_statrep", "StatRep"),
                ("fldigi_send_report", "General"),
            ],
            self.LOCAL_NET_SOFTWARE: [
                (self.LOCAL_NET_ACTION_NCS_KEY, "NCS"),
                (self.LOCAL_NET_ACTION_CHECKIN_KEY, "Check-in"),
                (self.LOCAL_NET_ACTION_MESSAGE_KEY, "Message"),
            ],
        }
        for key, label in self._load_spotter_forms():
            catalog.setdefault("JS8Call", []).append((key, label))
        return catalog

    def _reload_profiles(self, select_id: int | None) -> None:
        self._profiles = self.manager.list_profiles()
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        self.profile_combo.addItem("New SOP", None)
        for p in self._profiles:
            self.profile_combo.addItem(p.get("name", ""), int(p.get("id")))
        self.profile_combo.blockSignals(False)

        if select_id:
            for i in range(self.profile_combo.count()):
                if self.profile_combo.itemData(i) == select_id:
                    self.profile_combo.setCurrentIndex(i)
                    self._on_profile_selected(i)
                    return
        self.profile_combo.setCurrentIndex(-1)
        self._new_profile()

    def _on_profile_selected(self, idx: int) -> None:
        if self._loading_ui:
            return
        if idx < 0:
            return
        profile_id = self.profile_combo.itemData(idx)
        if not profile_id:
            self._new_profile()
            return
        profile = self.manager.get_profile(int(profile_id))
        if not profile:
            self._new_profile()
            return
        self._selected_profile_id = int(profile["id"])
        self._loading_ui = True
        try:
            self.name_edit.setText(profile.get("name", ""))
            self.group_combo.setCurrentText(profile.get("operating_group", ""))
            self.secondary_combo.setCurrentText(profile.get("secondary_group", ""))
            self.start_edit.setText(self._display_start_hhmm_from_utc(profile.get("sop_start_utc", "00:00")))
            self.priority_spin.setValue(int(profile.get("priority") or 100))
            self.active_cb.setChecked(bool(profile.get("active")))
            self._populate_actions(profile.get("actions", []))
            self._populate_schedule_layer(profile.get("schedule_layer", []))
            self._refresh_contact_label()
        finally:
            self._loading_ui = False
        self._invalidate_layer_sync_cache()
        self._set_save_dirty(False)
        self._update_profile_action_styles()
        self._schedule_layer_sync_refresh()
        self.refresh_upcoming()

    def _new_profile(self) -> None:
        self._selected_profile_id = None
        self._loading_ui = True
        try:
            self.name_edit.setText("")
            if self.group_combo.count() > 0:
                self.group_combo.setCurrentIndex(0)
            if self.secondary_combo.count() > 0:
                self.secondary_combo.setCurrentIndex(0)
            self.start_edit.setText(self._display_start_hhmm_from_utc("00:00"))
            self.priority_spin.setValue(100)
            self.active_cb.setChecked(False)
            self._populate_actions([])
            self._populate_schedule_layer([])
            self._refresh_contact_label()
        finally:
            self._loading_ui = False
        self._invalidate_layer_sync_cache()
        self._set_save_dirty(False)
        self._update_profile_action_styles()
        self._schedule_layer_sync_refresh()
        self.refresh_upcoming()

    def _on_group_changed(self) -> None:
        if self._loading_ui:
            return
        self._invalidate_layer_sync_cache()
        self._refresh_all_row_group_options()
        self._refresh_contact_label()
        self._schedule_layer_sync_refresh()

    def _on_secondary_group_changed(self) -> None:
        if self._loading_ui:
            return
        self._refresh_all_contact_target_options()
        self._refresh_contact_label()

    def _refresh_all_row_group_options(self) -> None:
        group = self.group_combo.currentText().strip().upper()
        band_opts = self._band_options_for_group(group)
        for r in range(self.actions_table.rowCount()):
            if self._is_local_net_action_row(r):
                continue
            band_combo = self.actions_table.cellWidget(r, self.COL_BAND)
            freq_combo = self.actions_table.cellWidget(r, self.COL_FREQ)
            if isinstance(band_combo, QComboBox):
                current = band_combo.currentText().strip()
                band_combo.blockSignals(True)
                band_combo.clear()
                band_combo.addItem("")
                for v in band_opts:
                    band_combo.addItem(v)
                band_combo.setCurrentText(current)
                band_combo.blockSignals(False)
            if isinstance(freq_combo, QComboBox):
                selected_band = band_combo.currentText().strip() if isinstance(band_combo, QComboBox) else ""
                freq_opts = self._frequency_options_for_group_band(group, selected_band)
                current = freq_combo.currentText().strip()
                freq_combo.blockSignals(True)
                freq_combo.clear()
                freq_combo.addItem("")
                for v in freq_opts:
                    freq_combo.addItem(v)
                freq_combo.setCurrentText(current)
                freq_combo.blockSignals(False)
        self._refresh_all_layer_group_options()
        self._refresh_all_contact_target_options()

    def _available_callsign_targets(self) -> List[str]:
        group = self.group_combo.currentText().strip().upper()
        subgroup = self.secondary_combo.currentText().strip().upper()
        return self.manager.resolve_group_callsigns(group, subgroup)

    def _contact_rule_options_for_current_filter(self) -> List[Tuple[str, str]]:
        group = self.group_combo.currentText().strip().upper()
        subgroup = self.secondary_combo.currentText().strip().upper()
        contacts = self.manager.resolve_primary_contacts(group, subgroup)
        out: List[Tuple[str, str]] = [("none", "None")]
        if contacts.get("hub"):
            out.append(("hub_or_hub_alt", "HUB OR HUB-ALT"))
        if contacts.get("ncs"):
            out.append(("ncs_or_ancs", "NCS OR ANCS"))
        if contacts.get("peer"):
            out.append(("peer", "PEER"))
        out.append(("callsign", "CallSign"))
        return out

    def _is_local_net_action_row(self, row: int) -> bool:
        if row < 0 or row >= self.actions_table.rowCount():
            return False
        sw_combo = self.actions_table.cellWidget(row, self.COL_SOFTWARE)
        action_combo = self.actions_table.cellWidget(row, self.COL_ACTION)
        if not isinstance(sw_combo, QComboBox) or not isinstance(action_combo, QComboBox):
            return False
        software = sw_combo.currentText().strip()
        action_key = str(action_combo.currentData() or "").strip()
        return software == self.LOCAL_NET_SOFTWARE and action_key in self.LOCAL_NET_ACTION_KEYS

    def _contact_rule_options_for_row(self, row: int) -> List[Tuple[str, str]]:
        if self._is_local_net_action_row(row):
            return [("local_group", "Local Group")]
        return self._contact_rule_options_for_current_filter()

    def _local_profile_display(self, profile_name: str) -> str:
        key = (profile_name or "").strip().upper()
        if not key:
            return "--"
        group_rows = self._local_profile_lookup().get(key) or []
        if not group_rows:
            return profile_name
        group_name = str(group_rows[0].get("group", "")).strip() or profile_name
        details: List[str] = []
        for row in group_rows[:3]:
            resource = str(row.get("resource", "")).strip()
            mode = str(row.get("mode", "")).strip()
            target = str(row.get("target", "")).strip()
            chunk = " ".join([p for p in [resource, mode] if p]).strip()
            if target:
                chunk = f"{chunk} {target}".strip() if chunk else target
            if chunk:
                details.append(chunk)
        suffix = ""
        extra = max(0, len(group_rows) - len(details))
        if extra:
            suffix = f" (+{extra} more)"
        if details:
            return f"{group_name} | {'; '.join(details)}{suffix}"
        return group_name

    def _refresh_all_contact_target_options(self) -> None:
        for r in range(self.actions_table.rowCount()):
            self._refresh_contact_rule_options_for_row(r)
            self._on_contact_rule_changed(r)

    def _refresh_contact_rule_options_for_row(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        rule_combo = self.actions_table.cellWidget(row, self.COL_CONTACT)
        if not isinstance(rule_combo, QComboBox):
            return
        current = str(rule_combo.currentData() or "none").strip()
        if self._is_local_net_action_row(row) and current == "local_profile":
            current = "local_group"
        opts = self._contact_rule_options_for_row(row)
        rule_combo.blockSignals(True)
        rule_combo.clear()
        for code, txt in opts:
            rule_combo.addItem(txt, code)
        idx = rule_combo.findData(current)
        if idx >= 0:
            rule_combo.setCurrentIndex(idx)
        else:
            idx_none = rule_combo.findData("none")
            rule_combo.setCurrentIndex(idx_none if idx_none >= 0 else 0)
        self._fit_combo_popup(rule_combo)
        rule_combo.blockSignals(False)

    def _update_hidden_actions_label(self) -> None:
        configured = self._configured_softwares()
        if not configured:
            self.add_row_btn.setEnabled(False)
            self.hidden_rows_label.setText("No software configured in Settings. Configure JS8/VarAC/FLDigi first.")
            self._update_profile_action_styles()
            return
        self.add_row_btn.setEnabled(True)
        if self._hidden_actions:
            self.hidden_rows_label.setText(
                f"{len(self._hidden_actions)} row(s) hidden (software not configured). Stored and preserved."
            )
        else:
            self.hidden_rows_label.setText("")
        self._update_profile_action_styles()

    def _autosize_actions_table(self) -> None:
        try:
            for col in (
                self.COL_BAND,
                self.COL_FREQ,
                self.COL_SOFTWARE,
                self.COL_ACTION,
                self.COL_INTERVAL,
                self.COL_CONTACT,
                self.COL_REMOVE,
            ):
                self.actions_table.resizeColumnToContents(col)
            if self.actions_table.columnWidth(self.COL_CONTACT_TARGET) < 170:
                self.actions_table.setColumnWidth(self.COL_CONTACT_TARGET, 170)
            if self.actions_table.columnWidth(self.COL_DESC) < 260:
                self.actions_table.setColumnWidth(self.COL_DESC, 260)
        except Exception:
            pass

    def _mode_options_for_group_band(self, group: str, band: str) -> List[str]:
        grp = (group or "").strip().upper()
        band_uc = (band or "").strip().upper()
        values: set[str] = set()
        for row in self._operating_groups:
            if (row.get("group") or "").strip().upper() != grp:
                continue
            row_band = (row.get("band") or "").strip().upper()
            if band_uc and row_band and row_band != band_uc:
                continue
            mode = (row.get("mode") or "").strip().upper()
            if mode:
                values.add(mode)
        return sorted(values)

    def _refresh_all_layer_group_options(self) -> None:
        for r in range(self.layer_table.rowCount()):
            self._refresh_layer_freq_for_row(r)
            self._refresh_layer_mode_for_row(r)
        self._refresh_layer_validation_hints()

    def _refresh_layer_freq_for_row(self, row: int) -> None:
        if row < 0 or row >= self.layer_table.rowCount():
            return
        group = self.group_combo.currentText().strip().upper()
        band_combo = self.layer_table.cellWidget(row, self.LAYER_COL_BAND)
        freq_combo = self.layer_table.cellWidget(row, self.LAYER_COL_FREQ)
        if not isinstance(band_combo, QComboBox) or not isinstance(freq_combo, QComboBox):
            return
        band = band_combo.currentText().strip().upper()
        values = self._frequency_options_for_group_band(group, band)
        current = freq_combo.currentText().strip()
        freq_combo.blockSignals(True)
        freq_combo.clear()
        freq_combo.addItem("")
        for val in values:
            freq_combo.addItem(val)
        if not current and len(values) == 1:
            freq_combo.setCurrentText(values[0])
        else:
            freq_combo.setCurrentText(current)
        self._fit_combo_popup(freq_combo)
        freq_combo.blockSignals(False)
        self._refresh_layer_validation_hints()

    def _refresh_layer_mode_for_row(self, row: int) -> None:
        if row < 0 or row >= self.layer_table.rowCount():
            return
        group = self.group_combo.currentText().strip().upper()
        band_combo = self.layer_table.cellWidget(row, self.LAYER_COL_BAND)
        mode_combo = self.layer_table.cellWidget(row, self.LAYER_COL_MODE)
        if not isinstance(band_combo, QComboBox) or not isinstance(mode_combo, QComboBox):
            return
        band = band_combo.currentText().strip().upper()
        options = self._mode_options_for_group_band(group, band)
        current = mode_combo.currentText().strip().upper()
        mode_combo.blockSignals(True)
        mode_combo.clear()
        mode_combo.addItem("")
        for val in options:
            mode_combo.addItem(val)
        if current and mode_combo.findText(current) < 0:
            mode_combo.addItem(current)
        mode_combo.setCurrentText(current)
        self._fit_combo_popup(mode_combo)
        mode_combo.blockSignals(False)
        self._refresh_layer_validation_hints()

    def _autosize_layer_table(self) -> None:
        try:
            for col in (
                self.LAYER_COL_DAY,
                self.LAYER_COL_RECURRENCE,
                self.LAYER_COL_MONTH_WEEKS,
                self.LAYER_COL_REMOVE,
            ):
                self.layer_table.resizeColumnToContents(col)
            if self.layer_table.columnWidth(self.LAYER_COL_MONTH_WEEKS) < 88:
                self.layer_table.setColumnWidth(self.LAYER_COL_MONTH_WEEKS, 88)
        except Exception:
            pass

    @staticmethod
    def _normalize_weeks_text(value: str) -> str:
        weeks: List[int] = []
        for token in str(value or "").split(","):
            token = token.strip()
            if not token:
                continue
            try:
                week = int(token)
            except Exception:
                continue
            if 1 <= week <= 5:
                weeks.append(week)
        return ",".join(str(w) for w in sorted(set(weeks)))

    @staticmethod
    def _is_valid_hhmm(value: str) -> bool:
        txt = (value or "").strip()
        if len(txt) != 5 or ":" not in txt:
            return False
        try:
            h, m = txt.split(":", 1)
            hh = int(h)
            mm = int(m)
            return 0 <= hh <= 23 and 0 <= mm <= 59
        except Exception:
            return False

    def _populate_schedule_layer(self, existing: List[Dict[str, Any]]) -> None:
        self.layer_table.setRowCount(0)
        ordered = sorted((existing or []), key=lambda x: int(x.get("sort_order") or 0))
        for row in ordered:
            if not isinstance(row, dict):
                continue
            self._add_layer_row(existing=row)
        self._autosize_layer_table()
        self._refresh_layer_validation_hints()

    def _add_layer_row(self, existing: Dict[str, Any] | None) -> None:
        row_data = existing or {}
        row = self.layer_table.rowCount()
        self.layer_table.insertRow(row)

        day_combo = QComboBox()
        for day in ("ALL", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"):
            day_combo.addItem(day, day)
        day_val = (row_data.get("day_utc") or "ALL").strip()
        day_combo.setCurrentText(day_val if day_val else "ALL")
        day_combo.setProperty("layer_id", int(row_data.get("id") or 0))
        day_combo.setProperty("sort_order", int(row_data.get("sort_order") or row))
        self.layer_table.setCellWidget(row, self.LAYER_COL_DAY, day_combo)

        rec_combo = QComboBox()
        for rec in ("Weekly", "Daily", "Periodic", "Bi-Weekly"):
            rec_combo.addItem(rec, rec)
        rec_val = (row_data.get("recurrence") or "Daily").strip()
        rec_combo.setCurrentText(rec_val if rec_val else "Daily")
        self.layer_table.setCellWidget(row, self.LAYER_COL_RECURRENCE, rec_combo)

        weeks_edit = QLineEdit(self._normalize_weeks_text(str(row_data.get("month_weeks") or "")))
        weeks_edit.setPlaceholderText("1,3,5")
        weeks_edit.setMaximumWidth(120)
        self.layer_table.setCellWidget(row, self.LAYER_COL_MONTH_WEEKS, weeks_edit)

        start_edit = QLineEdit(self._display_layer_hhmm_from_utc((row_data.get("start_utc") or "").strip()))
        start_edit.setPlaceholderText("HH:MM")
        start_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.layer_table.setCellWidget(row, self.LAYER_COL_START, start_edit)

        end_edit = QLineEdit(self._display_layer_hhmm_from_utc((row_data.get("end_utc") or "").strip()))
        end_edit.setPlaceholderText("HH:MM")
        end_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.layer_table.setCellWidget(row, self.LAYER_COL_END, end_edit)

        band_combo = self._make_band_widget((row_data.get("band") or "").strip().upper())
        self.layer_table.setCellWidget(row, self.LAYER_COL_BAND, band_combo)

        freq_combo = self._make_freq_widget((row_data.get("frequency") or "").strip())
        self.layer_table.setCellWidget(row, self.LAYER_COL_FREQ, freq_combo)

        mode_combo = QComboBox()
        mode_combo.setEditable(True)
        self.layer_table.setCellWidget(row, self.LAYER_COL_MODE, mode_combo)

        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(lambda _=False, b=remove_btn: self._remove_layer_row_for_button(b))
        self.layer_table.setCellWidget(row, self.LAYER_COL_REMOVE, remove_btn)

        band_combo.currentIndexChanged.connect(lambda _=0, r=row: self._refresh_layer_freq_for_row(r))
        band_combo.currentTextChanged.connect(lambda _=None, r=row: self._refresh_layer_freq_for_row(r))
        band_combo.currentIndexChanged.connect(lambda _=0, r=row: self._refresh_layer_mode_for_row(r))
        band_combo.currentTextChanged.connect(lambda _=None, r=row: self._refresh_layer_mode_for_row(r))
        rec_combo.currentIndexChanged.connect(lambda _=0, r=row: self._on_layer_recurrence_changed(r))

        self._refresh_layer_freq_for_row(row)
        self._refresh_layer_mode_for_row(row)
        if (row_data.get("mode") or "").strip():
            mode_combo.setCurrentText((row_data.get("mode") or "").strip().upper())
        self._on_layer_recurrence_changed(row)

        day_combo.currentIndexChanged.connect(self._mark_dirty)
        day_combo.currentIndexChanged.connect(self._refresh_layer_validation_hints)
        rec_combo.currentIndexChanged.connect(self._mark_dirty)
        rec_combo.currentIndexChanged.connect(self._refresh_layer_validation_hints)
        weeks_edit.textChanged.connect(self._mark_dirty)
        weeks_edit.textChanged.connect(self._refresh_layer_validation_hints)
        start_edit.textChanged.connect(self._mark_dirty)
        start_edit.textChanged.connect(self._refresh_layer_validation_hints)
        end_edit.textChanged.connect(self._mark_dirty)
        end_edit.textChanged.connect(self._refresh_layer_validation_hints)
        band_combo.currentIndexChanged.connect(self._mark_dirty)
        band_combo.currentIndexChanged.connect(self._refresh_layer_validation_hints)
        band_combo.currentTextChanged.connect(self._mark_dirty)
        band_combo.currentTextChanged.connect(self._refresh_layer_validation_hints)
        freq_combo.currentIndexChanged.connect(self._mark_dirty)
        freq_combo.currentIndexChanged.connect(self._refresh_layer_validation_hints)
        freq_combo.currentTextChanged.connect(self._mark_dirty)
        freq_combo.currentTextChanged.connect(self._refresh_layer_validation_hints)
        mode_combo.currentIndexChanged.connect(self._mark_dirty)
        mode_combo.currentIndexChanged.connect(self._refresh_layer_validation_hints)
        mode_combo.currentTextChanged.connect(self._mark_dirty)
        mode_combo.currentTextChanged.connect(self._refresh_layer_validation_hints)
        self._mark_dirty()
        self._autosize_layer_table()
        self._refresh_layer_validation_hints()

    def _on_layer_recurrence_changed(self, row: int) -> None:
        if row < 0 or row >= self.layer_table.rowCount():
            return
        rec_combo = self.layer_table.cellWidget(row, self.LAYER_COL_RECURRENCE)
        weeks_edit = self.layer_table.cellWidget(row, self.LAYER_COL_MONTH_WEEKS)
        if not isinstance(rec_combo, QComboBox) or not isinstance(weeks_edit, QLineEdit):
            return
        recurrence = (rec_combo.currentText() or "Weekly").strip().title()
        periodic = recurrence == "Periodic"
        weeks_edit.setEnabled(periodic)
        if not periodic:
            weeks_edit.setText("")
        self._refresh_layer_validation_hints()

    def _remove_layer_row_for_button(self, btn: QPushButton) -> None:
        for r in range(self.layer_table.rowCount()):
            if self.layer_table.cellWidget(r, self.LAYER_COL_REMOVE) is btn:
                self.layer_table.removeRow(r)
                self._mark_dirty()
                self._autosize_layer_table()
                self._refresh_layer_validation_hints()
                break

    @staticmethod
    def _hhmm_to_minutes(value: str) -> int | None:
        txt = (value or "").strip()
        if len(txt) != 5 or ":" not in txt:
            return None
        try:
            hh, mm = txt.split(":", 1)
            h = int(hh)
            m = int(mm)
            if h < 0 or h > 23 or m < 0 or m > 59:
                return None
            return (h * 60) + m
        except Exception:
            return None

    @staticmethod
    def _normalize_freq_text(value: str) -> str:
        txt = (value or "").strip()
        if not txt:
            return ""
        try:
            return f"{float(txt):.3f}"
        except Exception:
            return ""

    @staticmethod
    def _interval_segments(start_min: int, end_min: int) -> List[Tuple[int, int]]:
        if start_min == end_min:
            return [(0, 1440)]
        if end_min > start_min:
            return [(start_min, end_min)]
        return [(start_min, 1440), (0, end_min)]

    def _layer_time_windows_overlap(self, start_a: str, end_a: str, start_b: str, end_b: str) -> bool:
        a0 = self._hhmm_to_minutes(start_a)
        a1 = self._hhmm_to_minutes(end_a)
        b0 = self._hhmm_to_minutes(start_b)
        b1 = self._hhmm_to_minutes(end_b)
        if a0 is None or a1 is None or b0 is None or b1 is None:
            return False
        for sa, ea in self._interval_segments(a0, a1):
            for sb, eb in self._interval_segments(b0, b1):
                if sa < eb and sb < ea:
                    return True
        return False

    @staticmethod
    def _row_may_share_day_context(row_a: Dict[str, Any], row_b: Dict[str, Any]) -> bool:
        rec_a = str(row_a.get("recurrence") or "Weekly").strip().title()
        rec_b = str(row_b.get("recurrence") or "Weekly").strip().title()
        day_a = str(row_a.get("day") or "ALL").strip() or "ALL"
        day_b = str(row_b.get("day") or "ALL").strip() or "ALL"
        if rec_a == "Daily" or rec_b == "Daily":
            return True
        if day_a != "ALL" and day_b != "ALL" and day_a != day_b:
            return False
        if rec_a == "Periodic" and rec_b == "Periodic":
            weeks_a = {int(x) for x in str(row_a.get("weeks") or "").split(",") if str(x).strip().isdigit()}
            weeks_b = {int(x) for x in str(row_b.get("weeks") or "").split(",") if str(x).strip().isdigit()}
            if weeks_a and weeks_b and not (weeks_a & weeks_b):
                return False
        return True

    @staticmethod
    def _set_layer_widget_warning(widget: QWidget | None, issues: List[str]) -> None:
        if not isinstance(widget, QWidget):
            return
        if issues:
            widget.setStyleSheet("border: 1px solid #C99700; border-radius: 3px;")
            widget.setToolTip("\n".join(issues))
        else:
            widget.setStyleSheet("")
            widget.setToolTip("")

    def _refresh_layer_validation_hints(self, *_args) -> None:
        row_meta: List[Dict[str, Any]] = []
        for r in range(self.layer_table.rowCount()):
            day_combo = self.layer_table.cellWidget(r, self.LAYER_COL_DAY)
            rec_combo = self.layer_table.cellWidget(r, self.LAYER_COL_RECURRENCE)
            weeks_edit = self.layer_table.cellWidget(r, self.LAYER_COL_MONTH_WEEKS)
            start_edit = self.layer_table.cellWidget(r, self.LAYER_COL_START)
            end_edit = self.layer_table.cellWidget(r, self.LAYER_COL_END)
            band_combo = self.layer_table.cellWidget(r, self.LAYER_COL_BAND)
            freq_combo = self.layer_table.cellWidget(r, self.LAYER_COL_FREQ)
            mode_combo = self.layer_table.cellWidget(r, self.LAYER_COL_MODE)
            if not isinstance(day_combo, QComboBox) or not isinstance(rec_combo, QComboBox):
                continue
            if not isinstance(weeks_edit, QLineEdit) or not isinstance(start_edit, QLineEdit):
                continue
            if not isinstance(end_edit, QLineEdit):
                continue
            if not isinstance(freq_combo, QComboBox):
                continue
            day_val = str(day_combo.currentData() or day_combo.currentText() or "ALL").strip() or "ALL"
            rec_val = (rec_combo.currentText() or "Daily").strip().title()
            weeks_val = self._normalize_weeks_text(weeks_edit.text())
            start_val = start_edit.text().strip()
            end_val = end_edit.text().strip()
            band_val = band_combo.currentText().strip().upper() if isinstance(band_combo, QComboBox) else ""
            freq_val = freq_combo.currentText().strip()
            mode_val = mode_combo.currentText().strip().upper() if isinstance(mode_combo, QComboBox) else ""
            row_blank = not any([weeks_val, start_val, end_val, band_val, freq_val, mode_val])
            issues: Set[str] = set()
            if not row_blank:
                if not self._is_valid_hhmm(start_val):
                    issues.add("Start time must be HH:MM.")
                if not self._is_valid_hhmm(end_val):
                    issues.add("End time must be HH:MM.")
                if not freq_val:
                    issues.add("Frequency is required.")
                elif self._normalize_freq_text(freq_val) == "":
                    issues.add("Frequency is invalid.")
                if rec_val == "Periodic" and not weeks_val:
                    issues.add("Weeks are required for Periodic recurrence.")
            row_meta.append(
                {
                    "row": r,
                    "day": "ALL" if rec_val == "Daily" else day_val,
                    "recurrence": rec_val,
                    "weeks": weeks_val,
                    "start": start_val,
                    "end": end_val,
                    "band": band_val,
                    "freq": self._normalize_freq_text(freq_val),
                    "blank": row_blank,
                    "issues": issues,
                    "widgets": {
                        "day": day_combo,
                        "recurrence": rec_combo,
                        "weeks": weeks_edit,
                        "start": start_edit,
                        "end": end_edit,
                        "band": band_combo,
                        "freq": freq_combo,
                        "mode": mode_combo,
                    },
                }
            )

        overlap_pairs: Set[Tuple[int, int]] = set()
        for i in range(len(row_meta)):
            a = row_meta[i]
            if a["blank"] or a["issues"] or not a["freq"]:
                continue
            for j in range(i + 1, len(row_meta)):
                b = row_meta[j]
                if b["blank"] or b["issues"] or not b["freq"]:
                    continue
                if a["band"] != b["band"] or a["freq"] != b["freq"]:
                    continue
                if not self._row_may_share_day_context(a, b):
                    continue
                if self._layer_time_windows_overlap(a["start"], a["end"], b["start"], b["end"]):
                    overlap_pairs.add((int(a["row"]), int(b["row"])))

        for ra, rb in overlap_pairs:
            for meta in row_meta:
                if int(meta["row"]) == ra:
                    meta["issues"].add(f"Potential overlap with row {rb + 1}.")
                if int(meta["row"]) == rb:
                    meta["issues"].add(f"Potential overlap with row {ra + 1}.")

        warnings: List[str] = []
        for meta in row_meta:
            row_issues = sorted(list(meta["issues"]))
            row_widgets = meta["widgets"]
            self._set_layer_widget_warning(row_widgets.get("start"), row_issues)
            self._set_layer_widget_warning(row_widgets.get("end"), row_issues)
            self._set_layer_widget_warning(row_widgets.get("freq"), row_issues)
            self._set_layer_widget_warning(row_widgets.get("weeks"), row_issues)
            if row_issues:
                warnings.append(f"Row {int(meta['row']) + 1}: {'; '.join(row_issues)}")

        if warnings:
            preview = warnings[:5]
            if len(warnings) > 5:
                preview.append(f"...and {len(warnings) - 5} more row warning(s).")
            self.layer_validation_label.setText("Layer warnings:\n" + "\n".join(preview))
            self.layer_validation_label.setVisible(True)
        else:
            self.layer_validation_label.setText("")
            self.layer_validation_label.setVisible(False)

    def _layer_row_key(self, row: Dict[str, Any]) -> Tuple[str, str, int, str, str, str, str, str, str]:
        return (
            str(row.get("day_utc") or "ALL"),
            str(row.get("recurrence") or "Weekly"),
            int(row.get("biweekly_offset_weeks") or 0),
            self._normalize_weeks_text(str(row.get("month_weeks") or "")),
            str(row.get("band") or "").strip().upper(),
            str(row.get("mode") or "").strip().upper(),
            str(row.get("frequency") or "").strip(),
            str(row.get("start_utc") or "").strip(),
            str(row.get("end_utc") or "").strip(),
        )

    def _collect_action_rows_for_layer_seed(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for r in range(self.actions_table.rowCount()):
            sw_combo = self.actions_table.cellWidget(r, self.COL_SOFTWARE)
            action_combo = self.actions_table.cellWidget(r, self.COL_ACTION)
            band_combo = self.actions_table.cellWidget(r, self.COL_BAND)
            freq_combo = self.actions_table.cellWidget(r, self.COL_FREQ)
            if not isinstance(sw_combo, QComboBox):
                continue
            software = sw_combo.currentText().strip()
            if software == self.LOCAL_NET_SOFTWARE:
                continue
            action_label = action_combo.currentText().strip() if isinstance(action_combo, QComboBox) else ""
            band = band_combo.currentText().strip().upper() if isinstance(band_combo, QComboBox) else ""
            frequency = freq_combo.currentText().strip() if isinstance(freq_combo, QComboBox) else ""
            rows.append(
                {
                    "software": software,
                    "action_label": action_label,
                    "band": band,
                    "frequency": frequency,
                }
            )
        return rows

    def _count_local_net_actions(self) -> int:
        count = 0
        for r in range(self.actions_table.rowCount()):
            if self._is_local_net_action_row(r):
                count += 1
        return count

    def _current_layer_rows(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in range(self.layer_table.rowCount()):
            day_combo = self.layer_table.cellWidget(r, self.LAYER_COL_DAY)
            rec_combo = self.layer_table.cellWidget(r, self.LAYER_COL_RECURRENCE)
            weeks_edit = self.layer_table.cellWidget(r, self.LAYER_COL_MONTH_WEEKS)
            start_edit = self.layer_table.cellWidget(r, self.LAYER_COL_START)
            end_edit = self.layer_table.cellWidget(r, self.LAYER_COL_END)
            band_combo = self.layer_table.cellWidget(r, self.LAYER_COL_BAND)
            freq_combo = self.layer_table.cellWidget(r, self.LAYER_COL_FREQ)
            mode_combo = self.layer_table.cellWidget(r, self.LAYER_COL_MODE)
            if not isinstance(day_combo, QComboBox) or not isinstance(rec_combo, QComboBox):
                continue
            if not isinstance(weeks_edit, QLineEdit) or not isinstance(start_edit, QLineEdit):
                continue
            if not isinstance(end_edit, QLineEdit) or not isinstance(freq_combo, QComboBox):
                continue
            out.append(
                {
                    "id": int(day_combo.property("layer_id") or 0),
                    "day_utc": str(day_combo.currentData() or day_combo.currentText() or "ALL").strip(),
                    "recurrence": (rec_combo.currentText() or "Daily").strip().title(),
                    "biweekly_offset_weeks": 0,
                    "month_weeks": self._normalize_weeks_text(weeks_edit.text()),
                    "band": band_combo.currentText().strip().upper() if isinstance(band_combo, QComboBox) else "",
                    "mode": mode_combo.currentText().strip().upper() if isinstance(mode_combo, QComboBox) else "",
                    "frequency": freq_combo.currentText().strip(),
                    "start_utc": self._utc_layer_hhmm_from_display(start_edit.text().strip(), show_local=self._show_local),
                    "end_utc": self._utc_layer_hhmm_from_display(end_edit.text().strip(), show_local=self._show_local),
                    "enabled": True,
                    "sort_order": int(day_combo.property("sort_order") if day_combo.property("sort_order") is not None else r),
                }
            )
        return out

    def _apply_layer_rows(self, rows: List[Dict[str, Any]], *, replace_existing: bool) -> int:
        if replace_existing:
            self.layer_table.setRowCount(0)
            for row in rows:
                self._add_layer_row(existing=row)
            self._mark_dirty()
            self._autosize_layer_table()
            self._refresh_layer_validation_hints()
            return len(rows)

        existing_rows = self._current_layer_rows()
        existing_keys = {self._layer_row_key(row) for row in existing_rows}
        added = 0
        for row in rows:
            key = self._layer_row_key(row)
            if key in existing_keys:
                continue
            self._add_layer_row(existing=row)
            existing_keys.add(key)
            added += 1
        if added > 0:
            self._mark_dirty()
            self._autosize_layer_table()
            self._refresh_layer_validation_hints()
        return added

    def _invalidate_layer_sync_cache(self) -> None:
        self._layer_sync_cache_key = None
        self._layer_sync_cache_value = None

    def _build_layer_candidates_from_actions(
        self,
        *,
        use_cache: bool = False,
    ) -> Tuple[List[Dict[str, Any]], List[str], int]:
        group = self.group_combo.currentText().strip().upper()
        if not group:
            raise ValueError("Select a Group Name (HF Operating Group) before populating layer rows.")
        action_rows = self._collect_action_rows_for_layer_seed()
        if not action_rows:
            raise ValueError(
                "No eligible non-local action rows found. "
                "Local Net SOP actions are supported, but Layer Sync applies to HF/Net schedule actions."
            )
        action_sig: List[Tuple[str, str, str, str]] = []
        for row in action_rows:
            band = str(row.get("band") or "").strip().upper()
            freq = self._normalize_freq_text(str(row.get("frequency") or "").strip())
            software = str(row.get("software") or "").strip().upper()
            action_label = str(row.get("action_label") or "").strip().upper()
            action_sig.append((software, action_label, band, freq))
        cache_key: Tuple[Any, ...] = ("layer_candidates", group, tuple(sorted(action_sig)))
        if use_cache and self._layer_sync_cache_key == cache_key and self._layer_sync_cache_value is not None:
            c_rows, c_unmatched, c_matched = self._layer_sync_cache_value
            return [dict(x) for x in c_rows], list(c_unmatched), int(c_matched)
        result = self.manager.build_schedule_layer_candidates(
            operating_group=group,
            action_rows=action_rows,
        )
        candidates = result.get("rows", []) or []
        unmatched = result.get("unmatched", []) or []
        matched_actions = int(result.get("matched_actions") or 0)
        self._layer_sync_cache_key = cache_key
        self._layer_sync_cache_value = ([dict(x) for x in candidates], list(unmatched), matched_actions)
        return candidates, unmatched, matched_actions

    def _refresh_layer_sync_hint(self) -> None:
        theme = resolve_theme(self.settings)
        default_style = f"color: {theme.get('text_muted', '#888')};"
        text = ""
        style = default_style
        show = False
        out_of_sync = False
        has_basis = False
        try:
            candidates, unmatched, matched_actions = self._build_layer_candidates_from_actions(use_cache=True)
            existing_rows = self._current_layer_rows()
            existing_keys = {self._layer_row_key(r) for r in existing_rows}
            candidate_keys = {self._layer_row_key(r) for r in candidates}
            add_count = len([key for key in candidate_keys if key not in existing_keys])
            remove_count = len([key for key in existing_keys if key not in candidate_keys])
            has_basis = True
            show = True
            if not candidates:
                text = "Layer Sync: No matching HF/Net schedule windows for current action targets."
                style = f"color: {theme.get('warning', '#C99700')};"
            elif add_count == 0 and remove_count == 0:
                text = "Layer Sync: In Sync"
                style = f"color: {theme.get('success', '#2E7D32')}; font-weight: 600;"
            else:
                out_of_sync = True
                unmatched_count = len(unmatched)
                text = f"Layer Sync: Out of Sync (+{add_count} / -{remove_count})"
                if unmatched_count > 0:
                    text += f" | {unmatched_count} unmatched action target(s)"
                if matched_actions >= 0:
                    text += f" | matched targets: {matched_actions}"
                style = f"color: {theme.get('warning', '#C99700')}; font-weight: 600;"
        except ValueError:
            group = self.group_combo.currentText().strip().upper()
            actions = self._collect_action_rows_for_layer_seed()
            local_count = self._count_local_net_actions()
            if not group and actions:
                show = True
                text = "Layer Sync: Select a Group Name (HF Operating Group) to evaluate layer alignment."
            elif group and not actions:
                show = True
                if local_count > 0:
                    text = (
                        "Layer Sync: Local Net actions are active reminders. "
                        "Layer Sync is only used for HF/Net schedule actions."
                    )
                else:
                    text = "Layer Sync: Add non-local action rows to evaluate layer alignment."
            else:
                show = False
                text = ""
            style = default_style
            has_basis = False
            out_of_sync = False
        except Exception as e:
            log.debug("SOP: layer sync hint refresh failed: %s", e)
            show = False
            text = ""
            has_basis = False
            out_of_sync = False

        self._layer_sync_has_basis = has_basis
        self._layer_sync_out_of_sync = out_of_sync
        self.layer_sync_label.setVisible(show)
        self.layer_sync_label.setText(text)
        self.layer_sync_label.setStyleSheet(style)
        self._update_profile_action_styles(theme)

    def _layer_row_summary(self, row: Dict[str, Any]) -> str:
        day = str(row.get("day_utc") or "ALL").strip() or "ALL"
        rec = str(row.get("recurrence") or "Daily").strip().title()
        weeks = self._normalize_weeks_text(str(row.get("month_weeks") or ""))
        start = str(row.get("start_utc") or "").strip()
        end = str(row.get("end_utc") or "").strip()
        band = str(row.get("band") or "").strip().upper()
        freq = str(row.get("frequency") or "").strip()
        mode = str(row.get("mode") or "").strip().upper()
        rec_txt = rec if rec != "Periodic" else f"{rec}({weeks or '-'})"
        return f"{day} {rec_txt} {start}-{end} {band} {freq} {mode}".strip()

    def _preview_layer_candidates(
        self,
        *,
        candidates: List[Dict[str, Any]],
        unmatched: List[str],
        matched_actions: int,
        title: str,
    ) -> None:
        if not candidates:
            detail = "\n".join(unmatched[:8]) if unmatched else "No matching schedule windows were found."
            QMessageBox.information(self, "SOP Layer", f"No layer candidates found.\n\n{detail}")
            return
        existing_rows = self._current_layer_rows()
        existing_keys = {self._layer_row_key(r) for r in existing_rows}
        candidate_keys = {self._layer_row_key(r) for r in candidates}
        add_rows = [r for r in candidates if self._layer_row_key(r) not in existing_keys]
        remove_rows = [r for r in existing_rows if self._layer_row_key(r) not in candidate_keys]
        unchanged = len(existing_keys & candidate_keys)
        summary_lines = [
            f"Generated {len(candidates)} candidate row(s) from {matched_actions} matched action target(s).",
            "",
            "Diff vs current layer:",
            f"+ Add: {len(add_rows)}",
            f"- Remove (rebuild): {len(remove_rows)}",
            f"= Unchanged: {unchanged}",
        ]
        if add_rows:
            summary_lines.append("")
            summary_lines.append("Adds:")
            summary_lines.extend(self._layer_row_summary(row) for row in add_rows[:4])
            if len(add_rows) > 4:
                summary_lines.append(f"...and {len(add_rows) - 4} more add row(s).")
        if remove_rows:
            summary_lines.append("")
            summary_lines.append("Removals on rebuild:")
            summary_lines.extend(self._layer_row_summary(row) for row in remove_rows[:4])
            if len(remove_rows) > 4:
                summary_lines.append(f"...and {len(remove_rows) - 4} more remove row(s).")
        if unmatched:
            summary_lines.append("")
            summary_lines.append("Unmatched actions:")
            summary_lines.extend(unmatched[:4])
            if len(unmatched) > 4:
                summary_lines.append(f"...and {len(unmatched) - 4} more unmatched action(s).")

        msg = QMessageBox(self)
        msg.setWindowTitle(title)
        msg.setText("\n".join(summary_lines))
        append_btn = msg.addButton("Append Missing", QMessageBox.AcceptRole)
        rebuild_btn = msg.addButton("Apply Rebuild", QMessageBox.DestructiveRole)
        msg.addButton(QMessageBox.Cancel)
        msg.exec()
        clicked = msg.clickedButton()
        if clicked == append_btn:
            added = self._apply_layer_rows(candidates, replace_existing=False)
            QMessageBox.information(
                self,
                "SOP Layer",
                f"Appended {added} new row(s). {len(candidates) - added} duplicate row(s) were skipped.",
            )
        elif clicked == rebuild_btn:
            replaced = self._apply_layer_rows(candidates, replace_existing=True)
            QMessageBox.information(
                self,
                "SOP Layer",
                f"Rebuilt layer with {replaced} row(s). Removed {len(remove_rows)} row(s).",
            )

    def _on_populate_layer_from_actions(self) -> None:
        try:
            candidates, unmatched, matched_actions = self._build_layer_candidates_from_actions()
        except ValueError as e:
            QMessageBox.information(self, "SOP Layer", str(e))
            return
        self._preview_layer_candidates(
            candidates=candidates,
            unmatched=unmatched,
            matched_actions=matched_actions,
            title="Populate SOP Layer",
        )

    def _on_rebuild_layer_preview(self) -> None:
        try:
            candidates, unmatched, matched_actions = self._build_layer_candidates_from_actions()
        except ValueError as e:
            QMessageBox.information(self, "SOP Layer", str(e))
            return
        self._preview_layer_candidates(
            candidates=candidates,
            unmatched=unmatched,
            matched_actions=matched_actions,
            title="Rebuild SOP Layer Preview",
        )

    def _populate_actions(self, existing: List[Dict[str, Any]]) -> None:
        self.actions_table.setRowCount(0)
        self._hidden_actions = []

        configured = set(self._configured_softwares())
        ordered = sorted(existing, key=lambda x: int(x.get("sort_order") or 0))
        for row in ordered:
            sw = (row.get("software") or "").strip()
            if sw not in configured:
                self._hidden_actions.append(dict(row))
                continue
            self._add_action_row(existing=row)

        if self.actions_table.rowCount() == 0 and configured:
            self._add_action_row(existing=None)

        self._update_hidden_actions_label()
        self._autosize_actions_table()

    def _make_band_widget(self, value: str) -> QComboBox:
        combo = QComboBox()
        combo.setEditable(True)
        combo.setMinimumContentsLength(4)
        combo.setMinimumWidth(84)
        combo.addItem("")
        for b in self._band_options_for_group(self.group_combo.currentText()):
            combo.addItem(b)
        combo.setCurrentText((value or "").strip().upper())
        self._fit_combo_popup(combo)
        return combo

    def _make_freq_widget(self, value: str) -> QComboBox:
        combo = QComboBox()
        combo.setEditable(True)
        combo.addItem("")
        for f in self._frequency_options_for_group_band(self.group_combo.currentText(), ""):
            combo.addItem(f)
        combo.setCurrentText((value or "").strip())
        self._fit_combo_popup(combo)
        return combo

    def _make_software_widget(self, value: str) -> QComboBox:
        combo = QComboBox()
        combo.setProperty("_fio_popup_max_width", 360)
        for sw in self._configured_softwares():
            combo.addItem(sw)
        if value and combo.findText(value) < 0:
            combo.addItem(value)
        if value:
            combo.setCurrentText(value)
        self._fit_combo_popup(combo)
        return combo

    def _apply_typeahead(self, combo: QComboBox) -> None:
        try:
            completer = QCompleter(combo.model(), combo)
            completer.setCaseSensitivity(Qt.CaseInsensitive)
            completer.setFilterMode(Qt.MatchContains)
            completer.setCompletionMode(QCompleter.PopupCompletion)
            combo.setCompleter(completer)
        except Exception:
            pass

    def _tz_short_name(self) -> str:
        tz_name = str(self.settings.get("timezone", "UTC") or "UTC")
        upper = tz_name.upper()
        if "UTC" in upper:
            return "UTC"
        if "NEW_YORK" in upper or "EASTERN" in upper:
            return "ET"
        if "CHICAGO" in upper or "CENTRAL" in upper:
            return "CT"
        if "DENVER" in upper or "MOUNTAIN" in upper:
            return "MT"
        if "LOS_ANGELES" in upper or "PACIFIC" in upper:
            return "PT"
        try:
            tz = get_timezone(tz_name)
            now = datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
            return now.tzname() or "Local"
        except Exception:
            return "Local"

    @staticmethod
    def _format_interval_hhmm(minutes: int) -> str:
        m = max(1, int(minutes))
        return f"{m // 60:02d}:{m % 60:02d}"

    @staticmethod
    def _format_interval_spec(interval_minutes: int, phase_minutes: int = 0) -> str:
        base = SOPTab._format_interval_hhmm(interval_minutes)
        phase = max(0, int(phase_minutes or 0))
        if phase <= 0:
            return base
        return f"{base}@{phase}m"

    def _parse_interval_minutes(self, text: str) -> int:
        minutes, _phase = self._parse_interval_spec(text)
        return minutes

    def _parse_simple_minutes(self, text: str) -> int:
        raw = (text or "").strip().lower()
        if not raw:
            raise ValueError("Interval is required.")
        if raw.endswith("m"):
            return max(1, int(float(raw[:-1].strip())))
        if raw.endswith("h"):
            return max(1, int(round(float(raw[:-1].strip()) * 60)))
        if ":" in raw:
            hh, mm = raw.split(":", 1)
            total = (int(hh.strip() or "0") * 60) + int(mm.strip() or "0")
            if total <= 0:
                raise ValueError("Interval must be greater than 00:00.")
            return total
        if raw.isdigit() and len(raw) == 4:
            total = (int(raw[:2]) * 60) + int(raw[2:])
            if total <= 0:
                raise ValueError("Interval must be greater than 00:00.")
            return total
        if raw.isdigit():
            return max(1, int(raw))
        if "." in raw:
            return max(1, int(round(float(raw) * 60)))
        raise ValueError(f"Invalid interval: {text}")

    def _parse_phase_minutes(self, text: str) -> int:
        raw = (text or "").strip().lower()
        if not raw:
            return 0
        if raw.endswith("m"):
            raw = raw[:-1].strip()
        if ":" in raw:
            hh, mm = raw.split(":", 1)
            total = (int(hh.strip() or "0") * 60) + int(mm.strip() or "0")
            if total < 0:
                raise ValueError(f"Invalid interval phase: {text}")
            return total
        total = int(raw)
        if total < 0:
            raise ValueError(f"Invalid interval phase: {text}")
        return total

    def _parse_interval_spec(self, text: str) -> Tuple[int, int]:
        raw = (text or "").strip()
        if not raw:
            raise ValueError("Interval is required.")
        base = raw
        phase = 0
        if "@" in raw:
            base_part, phase_part = raw.split("@", 1)
            base = base_part.strip()
            phase = self._parse_phase_minutes(phase_part)
        interval_minutes = self._parse_simple_minutes(base)
        if phase:
            phase = phase % max(1, interval_minutes)
        return interval_minutes, phase

    def _refresh_action_combo_for_row(
        self,
        row: int,
        preferred_key: str | None = None,
        keep_current: bool = True,
    ) -> None:
        sw_combo = self.actions_table.cellWidget(row, self.COL_SOFTWARE)
        action_combo = self.actions_table.cellWidget(row, self.COL_ACTION)
        if not isinstance(sw_combo, QComboBox) or not isinstance(action_combo, QComboBox):
            return
        software = sw_combo.currentText().strip()
        preferred = preferred_key
        if preferred is None and keep_current:
            preferred = action_combo.currentData() if action_combo.count() else ""
        preferred = str(preferred or "").strip()
        catalog = self._action_catalog().get(software, [])
        action_combo.blockSignals(True)
        action_combo.clear()
        has_spotter = any(key.startswith("js8_spotter_") for key, _label in catalog)
        inserted_spotter_header = False
        for key, label in catalog:
            if key.startswith("js8_spotter_") and not inserted_spotter_header:
                action_combo.addItem("Spotter", "__spotter_header__")
                inserted_spotter_header = True
            action_combo.addItem(label, key)
        if has_spotter:
            model = action_combo.model()
            for idx in range(action_combo.count()):
                if action_combo.itemData(idx) == "__spotter_header__":
                    item = model.item(idx)
                    if item is not None:
                        item.setEnabled(False)
                    break
        if preferred and action_combo.findData(preferred) < 0 and keep_current:
            action_combo.addItem(preferred, preferred)
        idx = action_combo.findData(preferred)
        if idx >= 0:
            action_combo.setCurrentIndex(idx)
        elif action_combo.count() > 0:
            action_combo.setCurrentIndex(0)
        self._fit_combo_popup(action_combo)
        action_combo.blockSignals(False)

    def _add_action_row(self, existing: Dict[str, Any] | None) -> None:
        row = self.actions_table.rowCount()
        self.actions_table.insertRow(row)

        band_combo = self._make_band_widget((existing or {}).get("band", ""))
        self.actions_table.setCellWidget(row, self.COL_BAND, band_combo)

        freq_combo = self._make_freq_widget((existing or {}).get("frequency", ""))
        self.actions_table.setCellWidget(row, self.COL_FREQ, freq_combo)
        band_combo.currentIndexChanged.connect(lambda _=0, r=row: self._refresh_freq_combo_for_row(r))
        band_combo.currentTextChanged.connect(lambda _=None, r=row: self._refresh_freq_combo_for_row(r))
        self._refresh_freq_combo_for_row(row)

        sw_combo = self._make_software_widget((existing or {}).get("software", ""))
        sw_combo.setProperty("action_id", int((existing or {}).get("id") or 0))
        sw_combo.setProperty("sort_order", int((existing or {}).get("sort_order") or row))
        sw_combo.currentIndexChanged.connect(lambda _=0, r=row: self._on_software_changed(r))
        self.actions_table.setCellWidget(row, self.COL_SOFTWARE, sw_combo)

        action_combo = QComboBox()
        self.actions_table.setCellWidget(row, self.COL_ACTION, action_combo)
        self._refresh_action_combo_for_row(row, (existing or {}).get("action_key", ""))

        interval_combo = QComboBox()
        interval_combo.setEditable(True)
        for preset in self.INTERVAL_PRESETS:
            interval_combo.addItem(preset, preset)
        interval_minutes = int((existing or {}).get("interval_minutes") or 0)
        if interval_minutes <= 0:
            interval_minutes = int((existing or {}).get("interval_hours") or 3) * 60
        interval_phase = int((existing or {}).get("interval_phase_minutes") or 0)
        interval_txt = self._format_interval_spec(interval_minutes, interval_phase)
        if interval_combo.findText(interval_txt) < 0:
            interval_combo.addItem(interval_txt, interval_txt)
        interval_combo.setCurrentText(interval_txt)
        interval_combo.setToolTip("Examples: 00:45, 90m, 1.5h, 0130, 03:00@30m")
        if interval_combo.lineEdit() is not None:
            interval_combo.lineEdit().setPlaceholderText("type or select...")
        self._fit_combo_popup(interval_combo)
        self.actions_table.setCellWidget(row, self.COL_INTERVAL, interval_combo)

        rule_combo = QComboBox()
        for code, txt in self._contact_rule_options_for_row(row):
            rule_combo.addItem(txt, code)
        rule = ((existing or {}).get("contact_rule") or "none").strip()
        idx_rule = rule_combo.findData(rule)
        rule_combo.setCurrentIndex(idx_rule if idx_rule >= 0 else 0)
        self._fit_combo_popup(rule_combo)
        self.actions_table.setCellWidget(row, self.COL_CONTACT, rule_combo)

        target_combo = QComboBox()
        target_combo.setEditable(True)
        target_combo.setInsertPolicy(QComboBox.NoInsert)
        target_combo.setMaxVisibleItems(12)
        self._apply_typeahead(target_combo)
        target_combo.setProperty("saved_target", ((existing or {}).get("contact_target") or "").strip().upper())
        self.actions_table.setCellWidget(row, self.COL_CONTACT_TARGET, target_combo)
        self._sync_row_mode_for_action(row)
        self._refresh_contact_target_options_for_row(row)
        rule_combo.currentIndexChanged.connect(lambda _=0, r=row: self._on_contact_rule_changed(r))
        self._on_contact_rule_changed(row)

        desc_edit = QLineEdit((existing or {}).get("description", ""))
        desc_edit.setPlaceholderText("Optional description for reminder meaning")
        self.actions_table.setCellWidget(row, self.COL_DESC, desc_edit)

        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(lambda _=False, b=remove_btn: self._remove_row_for_button(b))
        self.actions_table.setCellWidget(row, self.COL_REMOVE, remove_btn)
        band_combo.currentIndexChanged.connect(self._mark_dirty)
        band_combo.currentTextChanged.connect(self._mark_dirty)
        freq_combo.currentIndexChanged.connect(self._mark_dirty)
        freq_combo.currentTextChanged.connect(self._mark_dirty)
        sw_combo.currentIndexChanged.connect(self._mark_dirty)
        action_combo.currentIndexChanged.connect(lambda _=0, r=row: self._on_action_selection_changed(r))
        action_combo.currentIndexChanged.connect(self._mark_dirty)
        interval_combo.currentIndexChanged.connect(self._mark_dirty)
        if interval_combo.lineEdit() is not None:
            interval_combo.lineEdit().textChanged.connect(self._mark_dirty)
        rule_combo.currentIndexChanged.connect(self._mark_dirty)
        target_combo.currentIndexChanged.connect(self._mark_dirty)
        target_combo.currentTextChanged.connect(self._mark_dirty)
        desc_edit.textChanged.connect(self._mark_dirty)
        self._mark_dirty()
        self._autosize_actions_table()

    def _on_software_changed(self, row: int) -> None:
        self._refresh_action_combo_for_row(row, preferred_key=None, keep_current=False)
        self._on_action_selection_changed(row)

    def _on_action_selection_changed(self, row: int) -> None:
        self._sync_row_mode_for_action(row)
        self._refresh_contact_rule_options_for_row(row)
        self._on_contact_rule_changed(row)
        self._refresh_freq_combo_for_row(row)

    def _sync_row_mode_for_action(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        is_local = self._is_local_net_action_row(row)
        band_combo = self.actions_table.cellWidget(row, self.COL_BAND)
        freq_combo = self.actions_table.cellWidget(row, self.COL_FREQ)
        if isinstance(band_combo, QComboBox):
            if is_local:
                band_combo.blockSignals(True)
                band_combo.setCurrentText("")
                band_combo.blockSignals(False)
            band_combo.setEnabled(not is_local)
        if isinstance(freq_combo, QComboBox):
            if is_local:
                freq_combo.blockSignals(True)
                freq_combo.setCurrentText("")
                freq_combo.blockSignals(False)
            freq_combo.setEnabled(not is_local)

    def _remove_row_for_button(self, btn: QPushButton) -> None:
        for r in range(self.actions_table.rowCount()):
            if self.actions_table.cellWidget(r, self.COL_REMOVE) is btn:
                self.actions_table.removeRow(r)
                self._mark_dirty()
                self._autosize_actions_table()
                break

    def _refresh_freq_combo_for_row(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        if self._is_local_net_action_row(row):
            return
        group = self.group_combo.currentText().strip().upper()
        band_combo = self.actions_table.cellWidget(row, self.COL_BAND)
        freq_combo = self.actions_table.cellWidget(row, self.COL_FREQ)
        if not isinstance(band_combo, QComboBox) or not isinstance(freq_combo, QComboBox):
            return
        selected_band = band_combo.currentText().strip().upper()
        options = self._frequency_options_for_group_band(group, selected_band)
        current = freq_combo.currentText().strip()
        freq_combo.blockSignals(True)
        freq_combo.clear()
        freq_combo.addItem("")
        for val in options:
            freq_combo.addItem(val)
        if not current and len(options) == 1:
            freq_combo.setCurrentText(options[0])
        else:
            freq_combo.setCurrentText(current)
        self._fit_combo_popup(freq_combo)
        freq_combo.blockSignals(False)

    def _role_targets_for_rule(self, rule: str) -> List[str]:
        group = self.group_combo.currentText().strip().upper()
        subgroup = self.secondary_combo.currentText().strip().upper()
        contacts = self.manager.resolve_primary_contacts(group, subgroup)
        if rule == "hub_or_hub_alt":
            return contacts.get("hub", []) or []
        if rule == "ncs_or_ancs":
            return contacts.get("ncs", []) or []
        if rule == "peer":
            return contacts.get("peer", []) or []
        return []

    def _refresh_contact_target_options_for_row(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        rule_combo = self.actions_table.cellWidget(row, self.COL_CONTACT)
        target_combo = self.actions_table.cellWidget(row, self.COL_CONTACT_TARGET)
        if not isinstance(target_combo, QComboBox) or not isinstance(rule_combo, QComboBox):
            return
        current = (
            target_combo.currentText().strip().upper()
            or str(target_combo.property("saved_target") or "").strip().upper()
        )
        rule = str(rule_combo.currentData() or "none").strip()
        target_combo.blockSignals(True)
        target_combo.clear()
        target_combo.addItem("")
        if rule in {"local_group", "local_profile"}:
            for name in self._local_profile_names():
                target_combo.addItem(name, name.upper())
            if current and target_combo.findData(current) < 0:
                target_combo.addItem(current, current)
            idx = target_combo.findData(current)
            target_combo.setCurrentIndex(idx if idx >= 0 else 0)
        else:
            for cs in self._available_callsign_targets():
                target_combo.addItem(cs, cs)
            if current and target_combo.findText(current) < 0:
                target_combo.addItem(current, current)
            target_combo.setCurrentText(current)
        self._fit_combo_popup(target_combo)
        target_combo.blockSignals(False)

    def _on_contact_rule_changed(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        rule_combo = self.actions_table.cellWidget(row, self.COL_CONTACT)
        target_combo = self.actions_table.cellWidget(row, self.COL_CONTACT_TARGET)
        if not isinstance(rule_combo, QComboBox) or not isinstance(target_combo, QComboBox):
            return
        rule = str(rule_combo.currentData() or "none").strip()
        saved_target = str(target_combo.property("saved_target") or "").strip().upper()
        target_combo.blockSignals(True)
        target_combo.clear()
        target_combo.setEditable(True)
        target_combo.setEnabled(True)

        if rule in {"hub_or_hub_alt", "ncs_or_ancs"}:
            target_combo.addItem("Any (Role Match)", self.ANY_ROLE_TOKEN)
            for cs in self._role_targets_for_rule(rule):
                target_combo.addItem(cs, cs)
            chosen = saved_target if saved_target else self.ANY_ROLE_TOKEN
            idx = target_combo.findData(chosen)
            target_combo.setCurrentIndex(idx if idx >= 0 else 0)
            target_combo.setEnabled(True)
            target_combo.setEditable(False)
        elif rule == "peer":
            target_combo.addItem("", "")
            for cs in self._role_targets_for_rule("peer"):
                target_combo.addItem(cs, cs)
            if target_combo.lineEdit() is not None:
                target_combo.lineEdit().setPlaceholderText("type or select...")
            if saved_target:
                idx = target_combo.findData(saved_target)
                target_combo.setCurrentIndex(idx if idx >= 0 else 0)
            else:
                target_combo.setCurrentIndex(0)
            target_combo.setEnabled(True)
            target_combo.setEditable(True)
        elif rule == "callsign":
            target_combo.addItem("", "")
            for cs in self._available_callsign_targets():
                target_combo.addItem(cs, cs)
            if target_combo.lineEdit() is not None:
                target_combo.lineEdit().setPlaceholderText("type or select...")
            if saved_target:
                idx = target_combo.findData(saved_target)
                target_combo.setCurrentIndex(idx if idx >= 0 else 0)
            else:
                target_combo.setCurrentIndex(0)
            target_combo.setEnabled(True)
            target_combo.setEditable(True)
        elif rule in {"local_group", "local_profile"}:
            target_combo.addItem("", "")
            for name in self._local_profile_names():
                target_combo.addItem(name, name.upper())
            idx = target_combo.findData(saved_target)
            target_combo.setCurrentIndex(idx if idx >= 0 else 0)
            target_combo.setEnabled(target_combo.count() > 1)
            target_combo.setEditable(False)
        else:
            target_combo.addItem("", "")
            target_combo.setCurrentIndex(0)
            target_combo.setEnabled(False)
            target_combo.setEditable(False)
        self._fit_combo_popup(target_combo)
        target_combo.blockSignals(False)
        self._autosize_actions_table()

    def _fit_combo_popup(self, combo: QComboBox) -> None:
        try:
            fm = QFontMetrics(combo.font())
            text_w = 0
            for i in range(combo.count()):
                text_w = max(text_w, fm.horizontalAdvance(combo.itemText(i)))
            popup_w = max(combo.width(), text_w + 44)
            max_w_raw = combo.property("_fio_popup_max_width")
            try:
                max_w = int(max_w_raw)
            except Exception:
                max_w = 0
            if max_w > 0:
                popup_w = min(popup_w, max_w)
            view = combo.view()
            if view is not None:
                view.setMinimumWidth(popup_w)
        except Exception:
            pass

    def _collect_profile_payload(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
        name = self.name_edit.text().strip()
        if not name:
            raise ValueError("SOP name is required.")
        group = self.group_combo.currentText().strip().upper()
        start = self.start_edit.text().strip()
        if len(start) != 5 or ":" not in start:
            raise ValueError("SOP Start Time must be HH:MM.")
        start_utc = self._utc_start_hhmm_from_display(start)

        payload = {
            "id": self._selected_profile_id,
            "name": name,
            "operating_group": group,
            "secondary_group": self.secondary_combo.currentText().strip().upper(),
            "frequency": "",
            "sop_start_utc": start_utc,
            "priority": int(self.priority_spin.value()),
            "active": self.active_cb.isChecked(),
            "window_hours": int(self.horizon_spin.value()),
        }

        actions: List[Dict[str, Any]] = []
        schedule_layer: List[Dict[str, Any]] = []
        requires_operating_group = False
        for r in range(self.actions_table.rowCount()):
            band_combo = self.actions_table.cellWidget(r, self.COL_BAND)
            freq_combo = self.actions_table.cellWidget(r, self.COL_FREQ)
            sw_combo = self.actions_table.cellWidget(r, self.COL_SOFTWARE)
            action_combo = self.actions_table.cellWidget(r, self.COL_ACTION)
            interval_combo = self.actions_table.cellWidget(r, self.COL_INTERVAL)
            rule_combo = self.actions_table.cellWidget(r, self.COL_CONTACT)
            target_combo = self.actions_table.cellWidget(r, self.COL_CONTACT_TARGET)
            desc_edit = self.actions_table.cellWidget(r, self.COL_DESC)

            if not isinstance(sw_combo, QComboBox) or not isinstance(action_combo, QComboBox):
                continue

            software = sw_combo.currentText().strip()
            action_key = str(action_combo.currentData() or "").strip()
            action_label = action_combo.currentText().strip()
            if not software or not action_key or action_key == "__spotter_header__":
                continue
            is_local_action = software == self.LOCAL_NET_SOFTWARE and action_key in self.LOCAL_NET_ACTION_KEYS
            if not is_local_action:
                requires_operating_group = True
            contact_rule = rule_combo.currentData() if isinstance(rule_combo, QComboBox) else "none"
            if is_local_action:
                contact_rule = "local_group"
            contact_target = ""
            if isinstance(target_combo, QComboBox):
                if str(contact_rule) in {"hub_or_hub_alt", "ncs_or_ancs"}:
                    contact_target = str(target_combo.currentData() or "").strip().upper()
                elif str(contact_rule) in {"callsign", "peer", "local_group", "local_profile"}:
                    contact_target = str(target_combo.currentData() or target_combo.currentText() or "").strip().upper()
            if str(contact_rule) != "none" and not contact_target:
                raise ValueError(f"Contact Target is required on row {r + 1}.")
            interval_minutes, interval_phase_minutes = (
                self._parse_interval_spec(interval_combo.currentText())
                if isinstance(interval_combo, QComboBox)
                else (180, 0)
            )
            description = desc_edit.text().strip() if isinstance(desc_edit, QLineEdit) else ""
            if is_local_action and not description and contact_target:
                description = f"Open local net group {contact_target}"

            actions.append(
                {
                    "id": int(sw_combo.property("action_id") or 0),
                    "band": (
                        band_combo.currentText().strip().upper()
                        if isinstance(band_combo, QComboBox) and not is_local_action
                        else ""
                    ),
                    "frequency": (
                        freq_combo.currentText().strip()
                        if isinstance(freq_combo, QComboBox) and not is_local_action
                        else ""
                    ),
                    "software": software,
                    "action_key": action_key,
                    "action_label": action_label,
                    "enabled": True,
                    "interval_minutes": interval_minutes,
                    "interval_phase_minutes": interval_phase_minutes,
                    "interval_hours": max(1, int((interval_minutes + 59) // 60)),
                    "description": description,
                    "contact_rule": contact_rule,
                    "contact_target": contact_target,
                    "sort_order": int(sw_combo.property("sort_order") or r),
                }
            )

        for i, hidden in enumerate(self._hidden_actions):
            preserved = dict(hidden)
            preserved["sort_order"] = int(
                preserved.get("sort_order") if preserved.get("sort_order") is not None else len(actions) + i
            )
            actions.append(preserved)

        for r in range(self.layer_table.rowCount()):
            day_combo = self.layer_table.cellWidget(r, self.LAYER_COL_DAY)
            rec_combo = self.layer_table.cellWidget(r, self.LAYER_COL_RECURRENCE)
            weeks_edit = self.layer_table.cellWidget(r, self.LAYER_COL_MONTH_WEEKS)
            start_edit = self.layer_table.cellWidget(r, self.LAYER_COL_START)
            end_edit = self.layer_table.cellWidget(r, self.LAYER_COL_END)
            band_combo = self.layer_table.cellWidget(r, self.LAYER_COL_BAND)
            freq_combo = self.layer_table.cellWidget(r, self.LAYER_COL_FREQ)
            mode_combo = self.layer_table.cellWidget(r, self.LAYER_COL_MODE)
            if not isinstance(day_combo, QComboBox) or not isinstance(rec_combo, QComboBox):
                continue
            if not isinstance(weeks_edit, QLineEdit) or not isinstance(start_edit, QLineEdit):
                continue
            if not isinstance(end_edit, QLineEdit):
                continue
            if not isinstance(freq_combo, QComboBox):
                continue
            day = str(day_combo.currentData() or day_combo.currentText() or "ALL").strip()
            recurrence = (rec_combo.currentText() or "Daily").strip().title()
            weeks = self._normalize_weeks_text(weeks_edit.text())
            start_display = start_edit.text().strip()
            end_display = end_edit.text().strip()
            band_val = band_combo.currentText().strip().upper() if isinstance(band_combo, QComboBox) else ""
            mode_val = mode_combo.currentText().strip().upper() if isinstance(mode_combo, QComboBox) else ""
            freq_txt = freq_combo.currentText().strip()
            row_blank = not any([weeks, start_display, end_display, band_val, mode_val, freq_txt])
            if row_blank:
                continue
            if not self._is_valid_hhmm(start_display):
                raise ValueError(f"Layer row {r + 1}: Start time must be HH:MM.")
            if not self._is_valid_hhmm(end_display):
                raise ValueError(f"Layer row {r + 1}: End time must be HH:MM.")
            start_utc = self._utc_layer_hhmm_from_display(start_display, show_local=self._show_local)
            end_utc = self._utc_layer_hhmm_from_display(end_display, show_local=self._show_local)
            if not freq_txt:
                raise ValueError(f"Layer row {r + 1}: Frequency is required.")
            try:
                freq_norm = f"{float(freq_txt):.3f}"
            except Exception:
                raise ValueError(f"Layer row {r + 1}: Frequency is invalid.")
            if recurrence not in {"Weekly", "Daily", "Periodic", "Bi-Weekly"}:
                recurrence = "Daily"
            if recurrence == "Periodic" and not weeks:
                raise ValueError(f"Layer row {r + 1}: Weeks are required for Periodic recurrence.")
            if recurrence != "Periodic":
                weeks = ""
            if recurrence == "Daily":
                day = "ALL"
            schedule_layer.append(
                {
                    "id": int(day_combo.property("layer_id") or 0),
                    "day_utc": day or "ALL",
                    "recurrence": recurrence,
                    "biweekly_offset_weeks": 0,
                    "month_weeks": weeks,
                    "band": band_val,
                    "mode": mode_val,
                    "vfo": "",
                    "frequency": freq_norm,
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "enabled": True,
                    "sort_order": int(day_combo.property("sort_order") if day_combo.property("sort_order") is not None else r),
                }
            )

        if not actions:
            raise ValueError("Add at least one action row.")
        if schedule_layer:
            requires_operating_group = True
        if requires_operating_group and not group:
            raise ValueError("Group Name (HF Operating Group) is required for non-local SOP actions.")

        return payload, actions, schedule_layer

    def _save_profile(self) -> None:
        try:
            payload, actions, schedule_layer = self._collect_profile_payload()
            profile_id = self.manager.save_profile(payload, actions, schedule_layer)
            self._reload_profiles(select_id=profile_id)
            self._set_save_dirty(False)
            self.refresh_upcoming()
            self._emit_sop_data_changed()
            QMessageBox.information(self, "SOP", "SOP saved.")
        except Exception as e:
            QMessageBox.warning(self, "SOP", str(e))

    def _delete_profile(self) -> None:
        if not self._selected_profile_id:
            return
        resp = QMessageBox.question(self, "Delete SOP", "Delete this SOP profile?")
        if resp != QMessageBox.Yes:
            return
        self.manager.delete_profile(int(self._selected_profile_id))
        self._reload_profiles(select_id=None)
        self.refresh_upcoming()
        self._emit_sop_data_changed()

    def _operator_callsign(self) -> str:
        return str(self.settings.get("operator_callsign", "") or "").strip().upper()

    def _export_pdf_default_name(self, *, scope: str, now_utc: datetime.datetime) -> str:
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        stamp = now_utc.astimezone(tz).strftime("%Y%m%d-%H%M")
        callsign = self._operator_callsign() or "operator"
        scope_tag = "selected" if scope == "selected" else "active"
        return f"{callsign}-sop-{scope_tag}-{stamp}.pdf"

    def _prompt_pdf_export_options(self) -> Dict[str, Any] | None:
        dlg = QDialog(self)
        dlg.setWindowTitle("Export SOP to PDF")
        dlg.setModal(True)
        layout = QVBoxLayout(dlg)

        general_box = QGroupBox("SOP PDF Export Options")
        general_form = QFormLayout(general_box)

        selected_name = ""
        if self._selected_profile_id:
            sid = int(self._selected_profile_id)
            for p in self._profiles:
                if int(p.get("id") or 0) == sid:
                    selected_name = str(p.get("name") or "")
                    break

        scope_combo = QComboBox()
        selected_label = f"Selected SOP ({selected_name})" if selected_name else "Selected SOP"
        scope_combo.addItem(selected_label, "selected")
        scope_combo.addItem("All Active SOPs (Unified)", "active")
        if not self._selected_profile_id:
            scope_combo.setCurrentIndex(1)
        general_form.addRow("Scope:", scope_combo)

        time_combo = QComboBox()
        time_combo.addItem("Local", "Local")
        time_combo.addItem("UTC", "UTC")
        time_combo.setCurrentIndex(0 if self._show_local else 1)
        general_form.addRow("SOP Time Display:", time_combo)
        layout.addWidget(general_box)

        roster_box = QGroupBox("Optional Operator Appendix")
        roster_layout = QVBoxLayout(roster_box)
        include_roster_cb = QCheckBox("Include Operator Rosters")
        include_roster_cb.setChecked(False)
        roster_layout.addWidget(include_roster_cb)

        roster_options_widget = QWidget()
        roster_options_layout = QVBoxLayout(roster_options_widget)
        roster_options_layout.setContentsMargins(0, 0, 0, 0)
        roster_options_layout.setSpacing(6)

        source_row = QHBoxLayout()
        include_hf_cb = QCheckBox("HF Operators")
        include_local_cb = QCheckBox("Local Operators")
        include_hf_cb.setChecked(False)
        include_local_cb.setChecked(False)
        source_row.addWidget(include_hf_cb)
        source_row.addWidget(include_local_cb)
        source_row.addStretch()
        roster_options_layout.addLayout(source_row)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Roster Filter:"))
        filter_combo = QComboBox()
        filter_combo.addItem("All", "all")
        filter_combo.addItem("State", "state")
        filter_combo.addItem("FEMA Region", "region")
        filter_row.addWidget(filter_combo)

        state_edit = QLineEdit()
        state_edit.setPlaceholderText("State (e.g., TX)")
        state_edit.setMaxLength(2)
        state_edit.setMinimumWidth(130)
        filter_row.addWidget(state_edit)

        region_combo = QComboBox()
        for region in self.manager.list_export_regions():
            region_combo.addItem(region, region)
        region_combo.setMinimumWidth(130)
        filter_row.addWidget(region_combo)
        filter_row.addStretch()
        roster_options_layout.addLayout(filter_row)

        hf_groups = self.manager.list_hf_groups_for_export()
        hf_filter_box = QGroupBox("HF Operator Filters")
        hf_filter_layout = QVBoxLayout(hf_filter_box)
        hf_filter_layout.addWidget(QLabel("Groups (multi-select):"))
        hf_group_checks: List[QCheckBox] = []
        if hf_groups:
            hf_groups_grid = QGridLayout()
            for idx, grp in enumerate(hf_groups):
                cb = QCheckBox(grp)
                cb.setChecked(False)
                hf_group_checks.append(cb)
                hf_groups_grid.addWidget(cb, idx // 3, idx % 3)
            hf_filter_layout.addLayout(hf_groups_grid)
        else:
            hf_filter_layout.addWidget(QLabel("No HF groups detected."))

        trusted_row = QHBoxLayout()
        trusted_row.addWidget(QLabel("Trusted (multi-select):"))
        hf_trusted_yes_cb = QCheckBox("Trusted")
        hf_trusted_no_cb = QCheckBox("Untrusted")
        hf_trusted_yes_cb.setChecked(True)
        hf_trusted_no_cb.setChecked(False)
        trusted_row.addWidget(hf_trusted_yes_cb)
        trusted_row.addWidget(hf_trusted_no_cb)
        trusted_row.addStretch()
        hf_filter_layout.addLayout(trusted_row)
        roster_options_layout.addWidget(hf_filter_box)

        local_categories = self.manager.list_local_categories_for_export()
        local_filter_box = QGroupBox("Local Operator Filters")
        local_filter_layout = QVBoxLayout(local_filter_box)
        local_filter_layout.addWidget(QLabel("Category (multi-select):"))
        local_category_checks: List[QCheckBox] = []
        if local_categories:
            local_grid = QGridLayout()
            for idx, category in enumerate(local_categories):
                cb = QCheckBox(category)
                cb.setChecked(True)
                local_category_checks.append(cb)
                local_grid.addWidget(cb, idx // 3, idx % 3)
            local_filter_layout.addLayout(local_grid)
        else:
            local_filter_layout.addWidget(QLabel("No local categories detected."))
        roster_options_layout.addWidget(local_filter_box)

        roster_layout.addWidget(roster_options_widget)

        layout.addWidget(roster_box)

        def _sync_controls() -> None:
            roster_on = include_roster_cb.isChecked()
            roster_options_widget.setVisible(roster_on)
            hf_on = roster_on and include_hf_cb.isChecked()
            local_on = roster_on and include_local_cb.isChecked()
            include_hf_cb.setEnabled(roster_on)
            include_local_cb.setEnabled(roster_on)
            filter_combo.setEnabled(roster_on)
            mode = str(filter_combo.currentData() or "all")
            state_mode = roster_on and mode == "state"
            region_mode = roster_on and mode == "region"
            state_edit.setEnabled(state_mode)
            region_combo.setEnabled(region_mode)
            hf_filter_box.setEnabled(hf_on)
            local_filter_box.setEnabled(local_on)
            for cb in hf_group_checks:
                cb.setEnabled(hf_on)
            hf_trusted_yes_cb.setEnabled(hf_on)
            hf_trusted_no_cb.setEnabled(hf_on)
            for cb in local_category_checks:
                cb.setEnabled(local_on)

        include_roster_cb.toggled.connect(_sync_controls)
        include_hf_cb.toggled.connect(_sync_controls)
        include_local_cb.toggled.connect(_sync_controls)
        filter_combo.currentIndexChanged.connect(_sync_controls)
        _sync_controls()

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, parent=dlg)

        def _accept() -> None:
            scope = str(scope_combo.currentData() or "selected")
            if scope == "selected" and not self._selected_profile_id:
                QMessageBox.warning(dlg, "Export SOP to PDF", "Select an SOP first, or choose All Active SOPs.")
                return
            if scope == "active" and not any(bool(p.get("active")) for p in self._profiles):
                QMessageBox.warning(dlg, "Export SOP to PDF", "No active SOP profiles found.")
                return
            if include_roster_cb.isChecked() and not (include_hf_cb.isChecked() or include_local_cb.isChecked()):
                QMessageBox.warning(dlg, "Export SOP to PDF", "Select HF Operators, Local Operators, or both.")
                return
            mode = str(filter_combo.currentData() or "all")
            if include_roster_cb.isChecked() and mode == "state":
                st = state_edit.text().strip().upper()
                if len(st) != 2:
                    QMessageBox.warning(dlg, "Export SOP to PDF", "State filter must be a 2-letter abbreviation.")
                    return
            if include_roster_cb.isChecked() and mode == "region" and not str(region_combo.currentData() or "").strip():
                QMessageBox.warning(dlg, "Export SOP to PDF", "Select a FEMA region for filtering.")
                return
            if include_roster_cb.isChecked() and include_hf_cb.isChecked():
                if hf_group_checks and not any(cb.isChecked() for cb in hf_group_checks):
                    QMessageBox.warning(dlg, "Export SOP to PDF", "Select at least one HF group filter.")
                    return
                if not (hf_trusted_yes_cb.isChecked() or hf_trusted_no_cb.isChecked()):
                    QMessageBox.warning(dlg, "Export SOP to PDF", "Select at least one HF trusted filter.")
                    return
            if include_roster_cb.isChecked() and include_local_cb.isChecked():
                if local_category_checks and not any(cb.isChecked() for cb in local_category_checks):
                    QMessageBox.warning(dlg, "Export SOP to PDF", "Select at least one local category filter.")
                    return
            dlg.accept()

        buttons.accepted.connect(_accept)
        buttons.rejected.connect(dlg.reject)
        layout.addWidget(buttons)

        if dlg.exec() != QDialog.Accepted:
            return None

        mode = str(filter_combo.currentData() or "all")
        return {
            "scope": str(scope_combo.currentData() or "selected"),
            "time_mode": str(time_combo.currentData() or "Local"),
            "include_roster": bool(include_roster_cb.isChecked()),
            "include_hf": bool(include_hf_cb.isChecked()),
            "include_local": bool(include_local_cb.isChecked()),
            "filter_mode": mode,
            "state_filter": state_edit.text().strip().upper() if mode == "state" else "",
            "region_filter": str(region_combo.currentData() or "").strip().upper() if mode == "region" else "",
            "hf_groups": [cb.text().strip() for cb in hf_group_checks if cb.isChecked()],
            "hf_trusted": [
                label
                for checked, label in (
                    (hf_trusted_yes_cb.isChecked(), "TRUSTED"),
                    (hf_trusted_no_cb.isChecked(), "UNTRUSTED"),
                )
                if checked
            ],
            "local_categories": [cb.text().strip() for cb in local_category_checks if cb.isChecked()],
        }

    def _collect_pdf_profiles(
        self,
        *,
        scope: str,
        now_utc: datetime.datetime,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if scope == "selected":
            profile_id = int(self._selected_profile_id or 0)
            if profile_id <= 0:
                return rows
            profile = self.manager.get_profile(profile_id)
            if not profile:
                return rows
            rows.append({"profile": profile})
            return rows
        for p in self._profiles:
            if not bool(p.get("active")):
                continue
            profile_id = int(p.get("id") or 0)
            if profile_id <= 0:
                continue
            profile = self.manager.get_profile(profile_id)
            if not profile:
                continue
            rows.append({"profile": profile})
        rows.sort(key=lambda x: str((x.get("profile") or {}).get("name") or "").upper())
        return rows

    def _daily_export_window_utc(
        self,
        *,
        time_mode: str,
        now_utc: datetime.datetime,
    ) -> Tuple[datetime.datetime, datetime.datetime, str]:
        mode = str(time_mode or "Local").strip().upper()
        now_norm = now_utc if isinstance(now_utc, datetime.datetime) else datetime.datetime.now(datetime.timezone.utc)
        if now_norm.tzinfo is None:
            now_norm = now_norm.replace(tzinfo=datetime.timezone.utc)
        else:
            now_norm = now_norm.astimezone(datetime.timezone.utc)
        if mode == "UTC":
            day_start = now_norm.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start.replace(hour=23, minute=59, second=59, microsecond=0)
            return day_start, day_end, f"{day_start.strftime('%Y-%m-%d')} UTC"
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        local_now = now_norm.astimezone(tz)
        local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        local_end = local_start.replace(hour=23, minute=59, second=59, microsecond=0)
        return (
            local_start.astimezone(datetime.timezone.utc),
            local_end.astimezone(datetime.timezone.utc),
            f"{local_start.strftime('%Y-%m-%d')} Local ({tz_name})",
        )

    def _format_due_for_pdf(self, due_utc: datetime.datetime, *, time_mode: str) -> str:
        if not isinstance(due_utc, datetime.datetime):
            return ""
        if str(time_mode).strip() == "UTC":
            return due_utc.strftime("%Y-%m-%d %H:%M")
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        return due_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")

    def _format_due_clock_for_pdf(self, due_utc: datetime.datetime, *, time_mode: str) -> str:
        if not isinstance(due_utc, datetime.datetime):
            return ""
        if str(time_mode).strip().upper() == "UTC":
            return due_utc.astimezone(datetime.timezone.utc).strftime("%H:%M")
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        return due_utc.astimezone(tz).strftime("%H:%M")

    def _build_daily_action_plan_html(self, rows: List[Dict[str, Any]], *, time_mode: str) -> str:
        if not rows:
            return "<p class='empty'>No actions found for the selected day.</p>"
        out = [
            "<table>",
            "<thead><tr>"
            "<th style='width: 10%;'>Time</th>"
            "<th style='width: 14%;'>Resource</th>"
            "<th style='width: 12%;'>Action</th>"
            "<th style='width: 12%;'>Band/Freq</th>"
            "<th style='width: 18%;'>Contact</th>"
            "<th style='width: 34%;'>Description</th>"
            "</tr></thead><tbody>",
        ]
        for row in rows:
            out.append(
                "<tr>"
                f"<td>{html.escape(self._format_due_clock_for_pdf(row.get('due_utc'), time_mode=time_mode))}</td>"
                f"<td>{html.escape(str(row.get('resource') or ''))}</td>"
                f"<td>{html.escape(str(row.get('action_label') or ''))}</td>"
                f"<td>{html.escape(str(row.get('band_freq') or '--'))}</td>"
                f"<td>{html.escape(str(row.get('contact_display') or '--'))}</td>"
                f"<td>{html.escape(str(row.get('description') or ''))}</td>"
                "</tr>"
            )
        out.append("</tbody></table>")
        return "".join(out)

    def _build_periodic_action_plan_html(self, rows: List[Dict[str, Any]]) -> str:
        if not rows:
            return "<p class='empty'>No periodic action rows found.</p>"
        out = [
            "<table>",
            "<thead><tr>"
            "<th style='width: 12%;'>Week(s) of Month</th>"
            "<th style='width: 11%;'>Day of Week</th>"
            "<th style='width: 12%;'>Resource</th>"
            "<th style='width: 12%;'>Action</th>"
            "<th style='width: 12%;'>Band/Freq</th>"
            "<th style='width: 17%;'>Contact</th>"
            "<th style='width: 24%;'>Description</th>"
            "</tr></thead><tbody>",
        ]
        for row in rows:
            out.append(
                "<tr>"
                f"<td>{html.escape(str(row.get('weeks_of_month') or ''))}</td>"
                f"<td>{html.escape(str(row.get('day_of_week') or ''))}</td>"
                f"<td>{html.escape(str(row.get('resource') or ''))}</td>"
                f"<td>{html.escape(str(row.get('action_label') or ''))}</td>"
                f"<td>{html.escape(str(row.get('band_freq') or '--'))}</td>"
                f"<td>{html.escape(str(row.get('contact_display') or '--'))}</td>"
                f"<td>{html.escape(str(row.get('description') or ''))}</td>"
                "</tr>"
            )
        out.append("</tbody></table>")
        return "".join(out)

    def _build_hf_operators_html(self, rows: List[Dict[str, str]]) -> str:
        if not rows:
            return "<p class='empty'>No HF operators match the selected filter.</p>"
        out = [
            "<table>",
            "<thead><tr>"
            "<th style='width: 12%;'>Callsign</th>"
            "<th style='width: 20%;'>Name</th>"
            "<th style='width: 8%;'>State</th>"
            "<th style='width: 10%;'>SitRep</th>"
            "<th style='width: 50%;'>Notes</th>"
            "</tr></thead><tbody>",
        ]
        for row in rows:
            out.append(
                "<tr>"
                f"<td>{html.escape(str(row.get('callsign') or ''))}</td>"
                f"<td>{html.escape(str(row.get('name') or ''))}</td>"
                f"<td>{html.escape(str(row.get('state') or ''))}</td>"
                f"<td>{html.escape(str(row.get('sitrep') or 'UNKNOWN'))}</td>"
                f"<td>{html.escape(str(row.get('notes') or ''))}</td>"
                "</tr>"
            )
        out.append("</tbody></table>")
        return "".join(out)

    def _build_local_operators_html(self, rows: List[Dict[str, str]]) -> str:
        if not rows:
            return "<p class='empty'>No local operators match the selected filter.</p>"
        out = [
            "<table>",
            "<thead><tr>"
            "<th style='width: 11%;'>Callsign</th>"
            "<th style='width: 10%;'>First</th>"
            "<th style='width: 10%;'>Last</th>"
            "<th style='width: 11%;'>City</th>"
            "<th style='width: 7%;'>State</th>"
            "<th style='width: 11%;'>Category</th>"
            "<th style='width: 8%;'>SitRep</th>"
            "<th style='width: 32%;'>Notes</th>"
            "</tr></thead><tbody>",
        ]
        for row in rows:
            out.append(
                "<tr>"
                f"<td>{html.escape(str(row.get('callsign') or ''))}</td>"
                f"<td>{html.escape(str(row.get('first_name') or ''))}</td>"
                f"<td>{html.escape(str(row.get('last_name') or ''))}</td>"
                f"<td>{html.escape(str(row.get('city') or ''))}</td>"
                f"<td>{html.escape(str(row.get('state') or ''))}</td>"
                f"<td>{html.escape(str(row.get('category') or ''))}</td>"
                f"<td>{html.escape(str(row.get('sitrep') or 'UNKNOWN'))}</td>"
                f"<td>{html.escape(str(row.get('notes') or ''))}</td>"
                "</tr>"
            )
        out.append("</tbody></table>")
        return "".join(out)

    def _build_pdf_html(
        self,
        *,
        profiles: List[Dict[str, Any]],
        options: Dict[str, Any],
        now_utc: datetime.datetime,
    ) -> str:
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        as_of_local = now_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")
        scope_label = "Selected SOP" if options.get("scope") == "selected" else "All Active SOPs"
        time_label = "UTC" if str(options.get("time_mode") or "Local") == "UTC" else f"Local ({tz_name})"
        parts = [
            "<html><head><meta charset='utf-8'>",
            "<style>"
            "@page { size: Letter; margin: 0.55in; }"
            "body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 10.5pt; color: #111; }"
            "h1 { font-size: 18pt; margin: 0 0 6pt 0; }"
            "h2 { font-size: 13pt; margin: 16pt 0 6pt 0; }"
            ".meta { font-size: 9.5pt; color: #333; margin: 0 0 3pt 0; }"
            ".section { margin-top: 8pt; }"
            ".page-break { page-break-before: always; }"
            "table { width: 100%; border-collapse: collapse; table-layout: fixed; margin: 6pt 0 14pt 0; }"
            "th, td { border: 1px solid #6f7682; padding: 5px 6px; vertical-align: top; word-wrap: break-word; }"
            "th { background: #edf1f5; font-weight: 700; }"
            ".empty { font-style: italic; color: #444; margin: 6pt 0 10pt 0; }"
            "</style></head><body>",
            "<h1>SOP Export</h1>",
            f"<div class='meta'><b>As Of:</b> {html.escape(as_of_local)} Local</div>",
            f"<div class='meta'><b>Scope:</b> {html.escape(scope_label)}</div>",
            f"<div class='meta'><b>SOP Time Basis:</b> {html.escape(time_label)}</div>",
        ]
        callsign = self._operator_callsign()
        if callsign:
            parts.append(f"<div class='meta'><b>Operator:</b> {html.escape(callsign)}</div>")

        time_mode = str(options.get("time_mode") or "Local")
        day_start_utc, day_end_utc, day_label = self._daily_export_window_utc(time_mode=time_mode, now_utc=now_utc)
        blended_daily_rows: List[Dict[str, Any]] = []
        periodic_rows: List[Dict[str, Any]] = []
        for section in profiles:
            profile = section.get("profile") or {}
            profile_id = int(profile.get("id") or 0)
            if profile_id <= 0:
                continue
            blended_daily_rows.extend(
                self.manager.build_profile_daily_plan_rows(
                    profile_id,
                    day_start_utc=day_start_utc,
                    day_end_utc=day_end_utc,
                )
            )
            periodic_rows.extend(self.manager.build_profile_periodic_action_rows(profile_id))

        daily_seen: Set[Tuple[str, str, str, str, str, str]] = set()
        daily_unique: List[Dict[str, Any]] = []
        for row in blended_daily_rows:
            due = row.get("due_utc")
            due_key = (
                due.astimezone(datetime.timezone.utc).replace(second=0, microsecond=0).isoformat()
                if isinstance(due, datetime.datetime)
                else ""
            )
            key = (
                due_key,
                str(row.get("resource") or ""),
                str(row.get("action_label") or ""),
                str(row.get("band_freq") or ""),
                str(row.get("contact_display") or ""),
                str(row.get("description") or ""),
            )
            if key in daily_seen:
                continue
            daily_seen.add(key)
            daily_unique.append(row)
        daily_unique.sort(
            key=lambda x: (
                x.get("due_utc")
                if isinstance(x.get("due_utc"), datetime.datetime)
                else datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
                str(x.get("resource") or ""),
                str(x.get("action_label") or ""),
            )
        )

        periodic_seen: Set[Tuple[str, str, str, str, str, str, str]] = set()
        periodic_unique: List[Dict[str, Any]] = []
        for row in periodic_rows:
            key = (
                str(row.get("weeks_of_month") or ""),
                str(row.get("day_of_week") or ""),
                str(row.get("resource") or ""),
                str(row.get("action_label") or ""),
                str(row.get("band_freq") or ""),
                str(row.get("contact_display") or ""),
                str(row.get("description") or ""),
            )
            if key in periodic_seen:
                continue
            periodic_seen.add(key)
            periodic_unique.append(row)
        periodic_unique.sort(
            key=lambda x: (
                str(x.get("weeks_of_month") or ""),
                str(x.get("day_of_week") or ""),
                str(x.get("resource") or ""),
                str(x.get("action_label") or ""),
            )
        )

        parts.append("<div class='section'>")
        parts.append("<h2>Daily Action Plan</h2>")
        parts.append(f"<div class='meta'><b>Day:</b> {html.escape(day_label)}</div>")
        parts.append(
            "<div class='meta'><b>Columns:</b> Time, Resource, Action, Band/Freq, Contact, Description</div>"
        )
        parts.append(self._build_daily_action_plan_html(daily_unique, time_mode=time_mode))
        parts.append("</div>")

        if periodic_unique:
            parts.append("<div class='page-break'>")
            parts.append("<h2>Periodic Actions</h2>")
            parts.append("<div class='meta'><b>Columns:</b> Week(s) of Month, Day of Week, Resource, Action, Band/Freq, Contact, Description</div>")
            parts.append(self._build_periodic_action_plan_html(periodic_unique))
            parts.append("</div>")

        if options.get("include_roster"):
            filter_mode = str(options.get("filter_mode") or "all")
            state_filter = str(options.get("state_filter") or "").strip().upper() if filter_mode == "state" else ""
            region_filter = str(options.get("region_filter") or "").strip().upper() if filter_mode == "region" else ""
            filter_desc = "All"
            if state_filter:
                filter_desc = f"State: {state_filter}"
            elif region_filter:
                filter_desc = f"FEMA Region: {region_filter}"

            if options.get("include_hf"):
                selected_groups = [str(v).strip() for v in list(options.get("hf_groups") or []) if str(v).strip()]
                selected_trusted = [str(v).strip() for v in list(options.get("hf_trusted") or []) if str(v).strip()]
                hf_rows = self.manager.list_hf_operators_for_export(
                    state_filter=state_filter,
                    region_filter=region_filter,
                    group_filters=selected_groups,
                    trusted_filters=selected_trusted,
                )
                parts.append("<div class='page-break'>")
                parts.append("<h2>HF Operators</h2>")
                parts.append(f"<div class='meta'><b>Filter:</b> {html.escape(filter_desc)}</div>")
                if selected_groups:
                    parts.append(f"<div class='meta'><b>Groups:</b> {html.escape(', '.join(selected_groups))}</div>")
                if selected_trusted:
                    parts.append(f"<div class='meta'><b>Trusted:</b> {html.escape(', '.join(selected_trusted))}</div>")
                parts.append(self._build_hf_operators_html(hf_rows))
                parts.append("</div>")

            if options.get("include_local"):
                selected_categories = [
                    str(v).strip()
                    for v in list(options.get("local_categories") or [])
                    if str(v).strip()
                ]
                local_rows = self.manager.list_local_operators_for_export(
                    state_filter=state_filter,
                    region_filter=region_filter,
                    category_filters=selected_categories,
                )
                parts.append("<div class='page-break'>")
                parts.append("<h2>Local Operators</h2>")
                parts.append(f"<div class='meta'><b>Filter:</b> {html.escape(filter_desc)}</div>")
                if selected_categories:
                    parts.append(
                        f"<div class='meta'><b>Category:</b> {html.escape(', '.join(selected_categories))}</div>"
                    )
                parts.append(self._build_local_operators_html(local_rows))
                parts.append("</div>")

        parts.append("</body></html>")
        return "".join(parts)

    def _export_pdf(self) -> None:
        try:
            options = self._prompt_pdf_export_options()
            if not options:
                return
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            profiles = self._collect_pdf_profiles(scope=str(options.get("scope") or "selected"), now_utc=now_utc)
            if not profiles:
                QMessageBox.information(self, "Export SOP to PDF", "No SOP data available for the selected scope.")
                return
            default_name = self._export_pdf_default_name(scope=str(options.get("scope") or "selected"), now_utc=now_utc)
            out, _ = QFileDialog.getSaveFileName(self, "Export SOP to PDF", default_name, "PDF Files (*.pdf)")
            if not out:
                return

            doc = QTextDocument(self)
            doc.setDocumentMargin(18.0)
            doc.setHtml(self._build_pdf_html(profiles=profiles, options=options, now_utc=now_utc))

            printer = QPrinter(QPrinter.HighResolution)
            printer.setOutputFormat(QPrinter.PdfFormat)
            printer.setOutputFileName(out)
            page_layout = printer.pageLayout()
            page_layout.setPageSize(QPageSize(QPageSize.Letter))
            page_layout.setUnits(QPageLayout.Inch)
            printer.setPageLayout(page_layout)
            doc.print_(printer)
            QMessageBox.information(self, "Export SOP to PDF", f"SOP PDF exported to:\n{out}")
        except Exception as e:
            QMessageBox.warning(self, "Export SOP to PDF", str(e))

    def _export_profile(self) -> None:
        if not self._selected_profile_id:
            QMessageBox.information(self, "Export SOP", "Save the SOP first before exporting.")
            return
        try:
            payload = self.manager.export_profile_json(int(self._selected_profile_id))
            out, _ = QFileDialog.getSaveFileName(
                self,
                "Export SOP",
                f"{payload['profile'].get('name', 'sop')}.json",
                "JSON Files (*.json)",
            )
            if not out:
                return
            Path(out).write_text(self.manager.dumps_json(payload), encoding="utf-8")
            QMessageBox.information(self, "Export SOP", f"Exported SOP to:\n{out}")
        except Exception as e:
            QMessageBox.warning(self, "Export SOP", str(e))

    def _import_profile(self) -> None:
        try:
            src, _ = QFileDialog.getOpenFileName(self, "Import SOP", "", "JSON Files (*.json)")
            if not src:
                return
            payload = json.loads(Path(src).read_text(encoding="utf-8"))
            profile_id = self.manager.import_profile_json(payload)
            self._reload_profiles(select_id=profile_id)
            self.refresh_upcoming()
            self._emit_sop_data_changed()
            QMessageBox.information(self, "Import SOP", "SOP imported.")
        except Exception as e:
            QMessageBox.warning(self, "Import SOP", str(e))

    def _refresh_contact_label(self) -> None:
        group = self.group_combo.currentText().strip().upper()
        subgroup = self.secondary_combo.currentText().strip().upper()
        contacts = self.manager.resolve_primary_contacts(group, subgroup)
        hub = contacts.get("hub", [])
        ncs = contacts.get("ncs", [])
        parts = []
        if hub:
            parts.append(f"HUB/HUB-ALT: {', '.join(hub[:6])}")
        if ncs:
            parts.append(f"NCS/ANCS: {', '.join(ncs[:6])}")
        if parts:
            self.contact_label.setText(f"Primary Contacts ({group or 'N/A'}): " + " | ".join(parts))
        else:
            self.contact_label.setText(f"Primary Contacts ({group or 'N/A'}): None")

    def _format_due(self, due_utc, tz_mode: str) -> str:
        if tz_mode == "Local":
            tz_name = self.settings.get("timezone", "UTC")
            tz = get_timezone(tz_name)
            return due_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")
        return due_utc.strftime("%Y-%m-%d %H:%M")

    def _update_clock_labels(self) -> None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a").upper()
        self.utc_label.setText(now_utc.strftime(f"<b>UTC ({utc_day}):</b> %y%m%d %H:%M:%S Z"))
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        try:
            local_dt = now_utc.astimezone(get_timezone(tz_name))
            local_day = local_dt.strftime("%a").upper()
            self.local_label.setText(local_dt.strftime(f"<b>{tz_name} ({local_day}):</b> %y%m%d %H:%M:%S"))
        except Exception:
            self.local_label.setText("<b>Local:</b> --")
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")
        self._update_time_toggle_style()
        tz_short = self._tz_short_name() if self._show_local else "UTC"
        self.start_label.setText(f"SOP Daily Start ({tz_short}):")
        self._update_layer_time_headers(tz_short)
        self._refresh_runtime_layer_hint()

    def _update_layer_time_headers(self, tz_short: str) -> None:
        if not hasattr(self, "layer_table"):
            return
        key = str(tz_short or "UTC").strip().upper()
        if key == self._layer_time_header_tag:
            return
        self._layer_time_header_tag = key
        self.layer_table.setHorizontalHeaderItem(self.LAYER_COL_START, QTableWidgetItem(f"Start ({tz_short})"))
        self.layer_table.setHorizontalHeaderItem(self.LAYER_COL_END, QTableWidgetItem(f"End ({tz_short})"))

    def _toggle_time_view(self) -> None:
        prev_show_local = self._show_local
        prior_text = self.start_edit.text().strip()
        prior_utc = self._utc_start_hhmm_from_display(prior_text, show_local=prev_show_local)
        layer_prior: List[Tuple[QLineEdit, QLineEdit, str, str]] = []
        for r in range(self.layer_table.rowCount()):
            start_edit = self.layer_table.cellWidget(r, self.LAYER_COL_START)
            end_edit = self.layer_table.cellWidget(r, self.LAYER_COL_END)
            if not isinstance(start_edit, QLineEdit) or not isinstance(end_edit, QLineEdit):
                continue
            layer_prior.append(
                (
                    start_edit,
                    end_edit,
                    self._utc_layer_hhmm_from_display(start_edit.text().strip(), show_local=prev_show_local),
                    self._utc_layer_hhmm_from_display(end_edit.text().strip(), show_local=prev_show_local),
                )
            )
        self._show_local = not self._show_local
        self.start_edit.setText(self._display_start_hhmm_from_utc(prior_utc))
        for start_edit, end_edit, start_utc, end_utc in layer_prior:
            start_edit.setText(self._display_layer_hhmm_from_utc(start_utc))
            end_edit.setText(self._display_layer_hhmm_from_utc(end_utc))
        self._update_clock_labels()
        self._refresh_layer_validation_hints()
        self.refresh_upcoming()

    def _display_start_hhmm_from_utc(self, utc_hhmm: str, show_local: bool | None = None) -> str:
        text = (utc_hhmm or "00:00").strip()
        if len(text) != 5 or ":" not in text:
            return "00:00"
        use_local = self._show_local if show_local is None else bool(show_local)
        if not use_local:
            return text
        try:
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            tz = get_timezone(tz_name)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            h, m = [int(x) for x in text.split(":")]
            dt_utc = now_utc.replace(hour=h, minute=m, second=0, microsecond=0)
            return dt_utc.astimezone(tz).strftime("%H:%M")
        except Exception:
            return text

    def _display_layer_hhmm_from_utc(self, utc_hhmm: str, show_local: bool | None = None) -> str:
        return self._display_start_hhmm_from_utc(utc_hhmm, show_local=show_local)

    def _utc_start_hhmm_from_display(self, display_hhmm: str, show_local: bool | None = None) -> str:
        text = (display_hhmm or "00:00").strip()
        if len(text) != 5 or ":" not in text:
            return "00:00"
        use_local = self._show_local if show_local is None else bool(show_local)
        if not use_local:
            return text
        try:
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            tz = get_timezone(tz_name)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_local = now_utc.astimezone(tz)
            h, m = [int(x) for x in text.split(":")]
            dt_local = now_local.replace(hour=h, minute=m, second=0, microsecond=0)
            return dt_local.astimezone(datetime.timezone.utc).strftime("%H:%M")
        except Exception:
            return text

    def _utc_layer_hhmm_from_display(self, display_hhmm: str, show_local: bool | None = None) -> str:
        return self._utc_start_hhmm_from_display(display_hhmm, show_local=show_local)

    def refresh_upcoming(self) -> None:
        try:
            horizon = int(self.horizon_spin.value())
            rows = self.manager.build_upcoming_actions(horizon_hours=horizon, only_active=True)
            configured = set(self._configured_softwares())
            if configured:
                rows = [r for r in rows if (r.get("software") or "").strip() in configured]
            else:
                rows = []
            self._upcoming_rows = rows
            self._populate_upcoming_table()
        except Exception as e:
            log.debug("SOP: refresh_upcoming failed: %s", e)
        self._refresh_runtime_layer_hint()

    def _refresh_runtime_layer_hint(self) -> None:
        theme = resolve_theme(self.settings)
        text = ""
        style = f"color: {theme.get('text_muted', '#888')};"
        visible = False
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

                if source in {"SOP", "NET", "HF"}:
                    visible = True
                    text = f"Runtime Source: {net_kind or source.title()}"
                    if source == "SOP":
                        style = f"color: {theme.get('warning', '#C99700')}; font-weight: 600;"
                    elif source == "NET":
                        style = f"color: {theme.get('info', '#1E88E5')}; font-weight: 600;"
                    else:
                        style = f"color: {theme.get('text', '#111')};"
                    extras: List[str] = []
                    if source_reason_detail:
                        extras.append(source_reason_detail)
                    if source == "SOP" and sop_contention:
                        contenders = [p for p in sop_profiles if p and p != sop_selected]
                        if contenders:
                            extras.append(f"Contention: {sop_selected or 'Winner'} over {', '.join(contenders[:3])}")
                        else:
                            extras.append("Contention detected")
                    if next_source_change and next_source and next_source != source:
                        next_label = next_net_kind or next_source
                        extras.append(f"Next Source: {next_label}")
                    if extras:
                        text = f"{text} | {' | '.join(extras)}"
        except Exception:
            visible = False
        self.layer_runtime_label.setVisible(visible)
        self.layer_runtime_label.setText(text)
        self.layer_runtime_label.setStyleSheet(style)

    def _populate_upcoming_table(self) -> None:
        tz_mode = "Local" if self._show_local else "UTC"
        theme = resolve_theme(self.settings)
        self.upcoming_table.setRowCount(0)
        misaligned = 0
        for row in self._upcoming_rows:
            r = self.upcoming_table.rowCount()
            self.upcoming_table.insertRow(r)
            self.upcoming_table.setItem(r, 0, QTableWidgetItem(row.get("profile_name", "")))

            band = (row.get("band") or "").strip()
            freq = (row.get("frequency") or "").strip()
            band_freq = f"{band} {freq}".strip() if (band or freq) else "--"
            band_item = QTableWidgetItem(band_freq)
            aligned = bool(row.get("aligned", True))
            if not aligned:
                warn_bg = theme.get("warning", "#C99700")
                band_item.setBackground(QColor(warn_bg))
                band_item.setForeground(QColor(_contrast_text_hex(warn_bg)))
                band_item.setToolTip("Scheduling Mismatch")
                misaligned += 1
            self.upcoming_table.setItem(r, 1, band_item)

            self.upcoming_table.setItem(r, 2, QTableWidgetItem(row.get("software", "")))
            self.upcoming_table.setItem(r, 3, QTableWidgetItem(row.get("action_label", "")))
            self.upcoming_table.setItem(r, 4, QTableWidgetItem(row.get("description", "")))
            self.upcoming_table.setItem(r, 5, QTableWidgetItem(self._format_due(row.get("next_due_utc"), tz_mode)))
            targets = row.get("contact_targets", []) or []
            contact_rule = str(row.get("contact_rule") or "").strip()
            if contact_rule in {"local_group", "local_profile"} and targets:
                contact_txt = self._local_profile_display(str(targets[0] or "").strip().upper())
            elif targets:
                contact_txt = " OR ".join(targets[:4])
            else:
                contact_txt = "--"
            self.upcoming_table.setItem(r, 6, QTableWidgetItem(contact_txt))

            btn = QPushButton("Complete")
            pid = int(row.get("profile_id"))
            aid = int(row.get("action_id"))
            status = (row.get("status") or "").strip()
            if status == "Due Now":
                btn.setText("Due Now")
                btn.setStyleSheet(button_style("info", theme))
            elif status == "Overdue":
                btn.setText(status)
                btn.setStyleSheet(button_style("warning", theme))
            elif status == "Completed":
                btn.setText("Completed")
                btn.setStyleSheet(button_style("success", theme))
            else:
                btn.setText(status or "Upcoming")
            btn.clicked.connect(lambda _=False, p=pid, a=aid: self._complete_action(p, a))
            self.upcoming_table.setCellWidget(r, 7, btn)

        if misaligned > 0:
            self.alignment_label.setText(
                f"Warning: {misaligned} upcoming SOP check-in reminder(s) do not align with Daily/Net/SOP Layer schedule windows."
            )
            self.alignment_label.setVisible(True)
        else:
            self.alignment_label.setVisible(False)

    def _complete_action(self, profile_id: int, action_id: int) -> None:
        try:
            self.manager.mark_action_complete(profile_id, action_id)
            self.refresh_upcoming()
            self._emit_sop_data_changed()
        except Exception as e:
            QMessageBox.warning(self, "SOP", f"Could not complete action: {e}")

    def _emit_sop_data_changed(self) -> None:
        try:
            self.sop_data_changed.emit()
        except Exception:
            pass

    def on_settings_saved(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        self._invalidate_layer_sync_cache()
        current_group = self.group_combo.currentText()
        current_start_utc = self._utc_start_hhmm_from_display(self.start_edit.text().strip())
        self._refresh_reference_data()
        self.group_combo.setCurrentText(current_group)
        self.start_edit.setText(self._display_start_hhmm_from_utc(current_start_utc))
        self._update_clock_labels()
        self._refresh_contact_label()
        self._on_profile_selected(self.profile_combo.currentIndex())
        self.refresh_upcoming()
        self._schedule_layer_sync_refresh()

    def on_local_net_profiles_updated(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        try:
            self._load_local_net_profiles_from_data(self.settings.all())
            self._refresh_all_contact_target_options()
            self._schedule_layer_sync_refresh()
        except Exception as e:
            log.debug("SOP: local net profiles refresh failed: %s", e)

    def on_tab_activated(self) -> None:
        self._invalidate_layer_sync_cache()
        self._schedule_layer_sync_refresh()
        self.refresh_upcoming()

    def on_sop_profiles_updated(self) -> None:
        current_id = self._selected_profile_id
        self._reload_profiles(select_id=current_id)
        self.refresh_upcoming()
        self._schedule_layer_sync_refresh()

    def select_profile(self, profile_id: int) -> bool:
        """
        Programmatically select a profile in the SOP tab.
        Returns True when selection succeeds.
        """
        try:
            target_id = int(profile_id or 0)
        except Exception:
            return False
        if target_id <= 0:
            return False
        try:
            self._reload_profiles(select_id=target_id)
            for idx in range(self.profile_combo.count()):
                if int(self.profile_combo.itemData(idx) or 0) != target_id:
                    continue
                if self.profile_combo.currentIndex() != idx:
                    self.profile_combo.setCurrentIndex(idx)
                else:
                    self._on_profile_selected(idx)
                return True
        except Exception:
            return False
        return False

    def apply_theme(self) -> None:
        try:
            theme = resolve_theme(self.settings)
            self.alignment_label.setStyleSheet(f"color: {theme.get('warning', '#B71C1C')}; font-weight: 600;")
            self.layer_validation_label.setStyleSheet(f"color: {theme.get('warning', '#B71C1C')}; font-weight: 600;")
            self.terms_hint_label.setStyleSheet(f"color: {theme.get('text_muted', '#888')};")
            self._update_time_toggle_style(theme)
            self._update_profile_action_styles(theme)
            self._apply_accessibility_width_guards()
            self._refresh_layer_sync_hint()
        except Exception:
            pass


class SOPTab(_LegacySOPTab):
    """
    SOP Builder v2.
    Category-first workflow with one HF SOP and one Local Comms SOP profile,
    per-action UTC scheduling fields, and conflict-gated HF activation.
    """

    CAT_HF = SOPManager.CATEGORY_HF
    CAT_LOCAL = SOPManager.CATEGORY_LOCAL

    COL_GROUP = 0
    COL_COND = 1
    COL_RESOURCE = 2
    COL_MODE = 3
    COL_ACTION = 4
    COL_BANDFREQ = 5
    COL_START = 6
    COL_END = 7
    COL_DURATION = 8
    COL_INTERVAL = 9
    COL_CONTACT = 10
    COL_CONTACT_TARGET = 11
    COL_DESC = 12
    COL_CONFLICT = 13
    COL_REMOVE = 14

    DURATION_OPTIONS: List[Tuple[str, int]] = [("00:30", 30), ("01:00", 60)]
    HF_RESOURCE_OPTIONS = ["FLDigi", "JS8Call", "VarAC", "SSB"]
    LOCAL_RESOURCE_FALLBACK = ["VHF", "UHF", "GMRS", "MURS", "FRS", "Meshtastic"]
    LOCAL_MODE_FALLBACK = ["Voice", "FM", "Digital", "Data", "Mixed", "Simplex", "Repeater"]
    LOCAL_CONTACT_OPTIONS: List[Tuple[str, str]] = [
        ("ncs_or_ancs", "NCS"),
        ("callsign", "Callsign"),
    ]

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)

        title_row = QHBoxLayout()
        title_row.addWidget(QLabel("<h3>SOP Builder</h3>"))
        title_row.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        title_row.addWidget(self.utc_label)
        title_row.addWidget(self.local_label)
        self.time_toggle_btn = QPushButton("Showing: Local")
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        title_row.addWidget(self.time_toggle_btn)
        root.addLayout(title_row)

        header = QHBoxLayout()
        self.profile_combo = QComboBox()
        self.profile_combo.currentIndexChanged.connect(self._on_profile_selected)
        header.addWidget(QLabel("Manage SOP:"))
        header.addWidget(self.profile_combo, stretch=1)

        self.new_btn = QPushButton("New SOP")
        self.save_btn = QPushButton("Save")
        self.delete_btn = QPushButton("Delete")
        self.export_pdf_btn = QPushButton("Export PDF")
        self.export_import_btn = QToolButton()
        self.export_import_btn.setText("Export/Import")
        self.export_import_btn.setPopupMode(QToolButton.InstantPopup)
        self.export_import_menu = QMenu(self.export_import_btn)
        self.export_json_action = self.export_import_menu.addAction("Export JSON")
        self.import_json_action = self.export_import_menu.addAction("Import JSON")
        self.export_import_btn.setMenu(self.export_import_menu)

        self.new_btn.clicked.connect(self._new_profile)
        self.save_btn.clicked.connect(self._save_profile)
        self.delete_btn.clicked.connect(self._delete_profile)
        self.export_pdf_btn.clicked.connect(self._export_pdf)
        self.export_json_action.triggered.connect(self._export_profile)
        self.import_json_action.triggered.connect(self._import_profile)
        for btn in (self.new_btn, self.save_btn, self.delete_btn, self.export_pdf_btn, self.export_import_btn):
            header.addWidget(btn)
        root.addLayout(header)

        cfg_box = QGroupBox("SOP")
        cfg_layout = QVBoxLayout(cfg_box)

        top_row = QHBoxLayout()
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("SOP name")
        self.category_combo = QComboBox()
        self.category_combo.addItem("HF SOP", self.CAT_HF)
        self.category_combo.addItem("Local Comms SOP", self.CAT_LOCAL)
        self.category_combo.currentIndexChanged.connect(self._on_category_changed)
        self.active_cb = QCheckBox("Active")
        self.active_cb.toggled.connect(self._mark_dirty)
        top_row.addWidget(QLabel("Name:"))
        top_row.addWidget(self.name_edit, stretch=2)
        top_row.addWidget(QLabel("Category:"))
        top_row.addWidget(self.category_combo, stretch=1)
        top_row.addWidget(self.active_cb)
        cfg_layout.addLayout(top_row)

        self.terms_hint_label = QLabel(
            "Action rows are stored in UTC and displayed in Local/UTC based on the Showing toggle."
        )
        self.terms_hint_label.setWordWrap(True)
        cfg_layout.addWidget(self.terms_hint_label)

        rows_head = QHBoxLayout()
        rows_head.addWidget(QLabel("Action Rows"))
        rows_head.addStretch()
        self.hidden_rows_label = QLabel("")
        rows_head.addWidget(self.hidden_rows_label)
        self.add_row_btn = QPushButton("Add Action Row")
        self.add_row_btn.clicked.connect(lambda: self._add_action_row(existing=None))
        rows_head.addWidget(self.add_row_btn)
        cfg_layout.addLayout(rows_head)

        self.actions_table = QTableWidget(0, 15)
        self.actions_table.setHorizontalHeaderLabels(
            [
                "Group",
                "Condition Levels",
                "Resource",
                "Mode",
                "Action",
                "Band - Freq",
                "Start (UTC)",
                "End (UTC)",
                "Duration",
                "Interval",
                "Contact Type",
                "Contact Target",
                "Description",
                "Conflict",
                "Remove",
            ]
        )
        self.actions_table.verticalHeader().setVisible(False)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_GROUP, QHeaderView.Interactive)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_COND, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_RESOURCE, QHeaderView.Interactive)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_MODE, QHeaderView.Interactive)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_ACTION, QHeaderView.Interactive)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_BANDFREQ, QHeaderView.Interactive)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_START, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_END, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_DURATION, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_INTERVAL, QHeaderView.Interactive)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_CONTACT, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_CONTACT_TARGET, QHeaderView.Interactive)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_DESC, QHeaderView.Stretch)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_CONFLICT, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_REMOVE, QHeaderView.ResizeToContents)
        self.actions_table.setColumnWidth(self.COL_GROUP, 150)
        self.actions_table.setColumnWidth(self.COL_RESOURCE, 130)
        self.actions_table.setColumnWidth(self.COL_MODE, 120)
        self.actions_table.setColumnWidth(self.COL_ACTION, 140)
        self.actions_table.setColumnWidth(self.COL_BANDFREQ, 180)
        self.actions_table.setColumnWidth(self.COL_INTERVAL, 110)
        self.actions_table.setColumnWidth(self.COL_CONTACT_TARGET, 170)
        cfg_layout.addWidget(self.actions_table)
        root.addWidget(cfg_box, stretch=1)

        # Compatibility placeholders for inherited methods not used in v2 UI.
        self.group_combo = QComboBox()
        self.secondary_combo = QComboBox()
        self.start_edit = QLineEdit("00:00")
        self.priority_spin = QSpinBox()
        self.horizon_spin = QSpinBox()
        self.contact_label = QLabel("")
        self.start_label = QLabel("")
        self.layer_sync_label = QLabel("")
        self.layer_validation_label = QLabel("")
        self.layer_runtime_label = QLabel("")
        self.alignment_label = QLabel("")
        self.populate_layer_btn = QPushButton()
        self.rebuild_layer_btn = QPushButton()
        self.add_layer_row_btn = QPushButton()
        self.refresh_btn = QPushButton()
        self.layer_table = QTableWidget(0, 0)
        self.upcoming_table = QTableWidget(0, 0)

        self._wire_dirty_tracking()
        self._realtime_conflict_timer = QTimer(self)
        self._realtime_conflict_timer.setSingleShot(True)
        self._realtime_conflict_timer.setInterval(550)
        self._realtime_conflict_timer.timeout.connect(self._run_realtime_hf_conflict_check)
        self._last_realtime_conflict_signature: Tuple[Any, ...] | None = None
        self._suppress_realtime_conflict_checks = False
        self._apply_accessibility_width_guards()
        self._update_clock_labels()
        self._update_action_time_headers()
        self._update_time_toggle_style()
        self._apply_category_table_view()

    def _wire_dirty_tracking(self) -> None:
        self.name_edit.textChanged.connect(self._mark_dirty)
        self.category_combo.currentIndexChanged.connect(self._mark_dirty)
        self.active_cb.toggled.connect(self._mark_dirty)

    def _schedule_realtime_hf_conflict_check(self, *_args) -> None:
        if getattr(self, "_loading_ui", False):
            return
        if getattr(self, "_suppress_realtime_conflict_checks", False):
            return
        if self._current_category() != self.CAT_HF:
            return
        timer = getattr(self, "_realtime_conflict_timer", None)
        if timer is None:
            return
        timer.start()

    def _collect_hf_action_for_realtime(self, row_index: int) -> Dict[str, Any] | None:
        if row_index < 0 or row_index >= self.actions_table.rowCount():
            return None
        if self._current_category() != self.CAT_HF:
            return None
        group_combo = self.actions_table.cellWidget(row_index, self.COL_GROUP)
        cond_edit = self.actions_table.cellWidget(row_index, self.COL_COND)
        resource_combo = self.actions_table.cellWidget(row_index, self.COL_RESOURCE)
        action_combo = self.actions_table.cellWidget(row_index, self.COL_ACTION)
        bandfreq_combo = self.actions_table.cellWidget(row_index, self.COL_BANDFREQ)
        start_edit = self.actions_table.cellWidget(row_index, self.COL_START)
        duration_combo = self.actions_table.cellWidget(row_index, self.COL_DURATION)
        interval_combo = self.actions_table.cellWidget(row_index, self.COL_INTERVAL)
        contact_combo = self.actions_table.cellWidget(row_index, self.COL_CONTACT)
        target_combo = self.actions_table.cellWidget(row_index, self.COL_CONTACT_TARGET)
        desc_edit = self.actions_table.cellWidget(row_index, self.COL_DESC)
        if not isinstance(group_combo, QComboBox) or not isinstance(resource_combo, QComboBox):
            return None
        if not isinstance(action_combo, QComboBox) or not isinstance(bandfreq_combo, QComboBox):
            return None
        if not isinstance(start_edit, QLineEdit) or not isinstance(duration_combo, QComboBox):
            return None
        if not isinstance(interval_combo, QComboBox) or not isinstance(contact_combo, QComboBox):
            return None
        if not isinstance(target_combo, QComboBox) or not isinstance(desc_edit, QLineEdit):
            return None

        group_name = group_combo.currentText().strip().upper()
        resource = resource_combo.currentText().strip()
        action_key = str(action_combo.currentData() or "").strip()
        action_label = action_combo.currentText().strip()
        band, freq = self._split_band_freq(bandfreq_combo.currentText().strip())
        start_display = start_edit.text().strip()
        if not (group_name and resource and action_key and band and freq and self._is_valid_hhmm(start_display)):
            return None
        start_utc = self._utc_start_hhmm_from_display(start_display, show_local=self._show_local)
        duration_minutes = int(duration_combo.currentData() or 60)
        if duration_minutes not in {30, 60}:
            duration_minutes = 60
        end_utc = self._add_minutes_hhmm(start_utc, duration_minutes)
        interval_minutes, phase_minutes = self._parse_interval_spec(interval_combo.currentText())
        if interval_minutes <= 0:
            return None
        condition_levels = "ALL"
        if isinstance(cond_edit, QLineEdit):
            condition_levels = self.manager._normalize_condition_levels(cond_edit.text().strip())
        contact_rule = str(contact_combo.currentData() or "none").strip()
        contact_target = str(target_combo.currentText() or "").strip().upper()
        if contact_target == "ANY (ROLE MATCH)":
            contact_target = self.ANY_ROLE_TOKEN
        return {
            "id": int(group_combo.property("action_id") or 0),
            "group_name": group_name,
            "condition_levels": condition_levels,
            "band": band,
            "frequency": freq,
            "software": resource,
            "action_key": action_key,
            "action_label": action_label or action_key,
            "enabled": True,
            "daily_start_utc": start_utc,
            "daily_end_utc": end_utc,
            "duration_minutes": duration_minutes,
            "interval_minutes": interval_minutes,
            "interval_phase_minutes": phase_minutes,
            "interval_hours": max(1, int((interval_minutes + 59) // 60)),
            "conflict_policy": self.manager._normalize_conflict_policy(group_combo.property("conflict_policy")),
            "schedule_applied": bool(group_combo.property("schedule_applied")),
            "description": desc_edit.text().strip(),
            "contact_rule": contact_rule,
            "contact_target": contact_target,
            "sort_order": row_index,
        }

    def _collect_hf_actions_for_realtime(self) -> List[Tuple[int, Dict[str, Any]]]:
        out: List[Tuple[int, Dict[str, Any]]] = []
        for r in range(self.actions_table.rowCount()):
            action = self._collect_hf_action_for_realtime(r)
            if not action:
                continue
            out.append((r, action))
        return out

    def _set_inline_conflict_badge(self, row_index: int, status: str, tooltip: str = "") -> None:
        badge = self.actions_table.cellWidget(row_index, self.COL_CONFLICT)
        if not isinstance(badge, QLabel):
            return
        theme = resolve_theme(self.settings)
        status_key = str(status or "").strip().lower()
        if status_key == "conflict":
            bg = theme.get("warning", "#c28c18")
            fg = theme.get("button_text", "#ffffff")
            label = "Conflict"
        elif status_key == "ok":
            bg = theme.get("success", "#17863a")
            fg = theme.get("button_text", "#ffffff")
            label = "OK"
        elif status_key == "local":
            bg = theme.get("panel_alt", "#334155")
            fg = theme.get("text_muted", "#b9c4d2")
            label = "Local"
        else:
            bg = theme.get("panel_alt", "#334155")
            fg = theme.get("text_muted", "#b9c4d2")
            label = "Pending"
        badge.setText(label)
        badge.setToolTip(str(tooltip or "").strip())
        badge.setAlignment(Qt.AlignCenter)
        badge.setStyleSheet(
            (
                "QLabel {"
                f"background: {bg};"
                f"color: {fg};"
                "padding: 2px 8px;"
                "border-radius: 10px;"
                "font-weight: 600;"
                "}"
            )
        )

    def _refresh_inline_conflict_badges(
        self,
    ) -> List[Tuple[int, Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]]:
        if self._current_category() != self.CAT_HF:
            for r in range(self.actions_table.rowCount()):
                self._set_inline_conflict_badge(r, "local", "Local Comms actions do not use HF conflict checks.")
            return []

        action_rows = self._collect_hf_actions_for_realtime()
        action_map = {r: a for r, a in action_rows}
        out: List[Tuple[int, Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]] = []

        for row_index in range(self.actions_table.rowCount()):
            action = action_map.get(row_index)
            if not action:
                self._set_inline_conflict_badge(
                    row_index,
                    "pending",
                    "Complete Group, Resource, Action, Band-Freq, Start, Duration, and Interval to validate conflicts.",
                )
                continue
            peers = [dict(other_action) for other_row, other_action in action_rows if other_row != row_index]
            diag = self.manager.detect_action_conflicts(
                action=action,
                operating_group=str(action.get("group_name") or "").strip().upper(),
                horizon_days=7,
                check_all_groups=True,
                peer_actions=peers,
            )
            if bool(diag.get("has_conflict")):
                tooltip_lines = [
                    f"Daily: {diag.get('daily_summary') or 'None'}",
                    f"Nets: {diag.get('net_summary') or 'None'}",
                    f"SOP: {diag.get('sop_summary') or 'None'}",
                ]
                self._set_inline_conflict_badge(row_index, "conflict", "\n".join(tooltip_lines))
            else:
                self._set_inline_conflict_badge(row_index, "ok", "No Daily/Net/SOP conflicts detected.")
            out.append((row_index, action, diag, peers))
        return out

    def _run_realtime_hf_conflict_check(self) -> None:
        if getattr(self, "_loading_ui", False):
            return
        if getattr(self, "_suppress_realtime_conflict_checks", False):
            return
        analyses = self._refresh_inline_conflict_badges()
        if not analyses:
            self._last_realtime_conflict_signature = None
            return
        for row_index, action, diag, peers in analyses:
            if not bool(diag.get("has_conflict")):
                continue
            signature = (
                int(row_index),
                str(action.get("action_key") or ""),
                str(action.get("group_name") or ""),
                str(action.get("daily_start_utc") or ""),
                str(action.get("daily_end_utc") or ""),
                str(diag.get("daily_summary") or ""),
                str(diag.get("net_summary") or ""),
                str(diag.get("sop_summary") or ""),
            )
            if signature == self._last_realtime_conflict_signature:
                return
            # Mark this signature as handled before prompting so one decision
            # does not immediately retrigger the same dialog on unchanged data.
            self._last_realtime_conflict_signature = signature
            self._prompt_realtime_hf_conflict_resolution(row_index, action, diag, peers)
            return
        self._last_realtime_conflict_signature = None

    def _prompt_realtime_hf_conflict_resolution(
        self,
        row_index: int,
        action: Dict[str, Any],
        diag: Dict[str, Any],
        peer_actions: List[Dict[str, Any]],
    ) -> None:
        details = [
            f"Action Row {row_index + 1}: {action.get('action_label', 'Action')}",
            f"Group: {str(action.get('group_name') or '').strip().upper()}",
            "",
            f"Daily Schedule Conflicts: {diag.get('daily_summary') or 'None'}",
            f"Net Schedule Conflicts: {diag.get('net_summary') or 'None'}",
            f"SOP Action Conflicts: {diag.get('sop_summary') or 'None'}",
            "",
            "Choose conflict handling policy:",
        ]
        msg = QMessageBox(self)
        msg.setWindowTitle("SOP Conflict Resolution")
        msg.setText("\n".join(details))
        sop_btn = msg.addButton("1) SOP Priority For ALL", QMessageBox.AcceptRole)
        net_btn = msg.addButton("2) Net Priority on Conflicts", QMessageBox.ActionRole)
        daily_btn = msg.addButton("3) Daily Schedule Priority on Conflicts", QMessageBox.ActionRole)
        cancel_btn = msg.addButton(QMessageBox.Cancel)
        msg.exec()
        clicked = msg.clickedButton()
        if clicked == cancel_btn:
            return
        if clicked == net_btn:
            policy = self.manager.CONFLICT_POLICY_NET
        elif clicked == daily_btn:
            policy = self.manager.CONFLICT_POLICY_DAILY
        else:
            policy = self.manager.CONFLICT_POLICY_SOP
        action["conflict_policy"] = policy

        must_adjust_start = bool(diag.get("first_occurrence_conflict")) and (
            policy in {self.manager.CONFLICT_POLICY_NET, self.manager.CONFLICT_POLICY_DAILY}
            or bool(diag.get("sop_conflicts"))
        )
        if must_adjust_start:
            suggested_utc = self.manager.suggest_non_conflicting_start(
                action=action,
                operating_group=str(action.get("group_name") or "").strip().upper(),
                check_all_groups=True,
                peer_actions=peer_actions,
            )
            suggested_display = self._display_start_hhmm_from_utc(suggested_utc, show_local=self._show_local)
            reason_txt = (
                "First occurrence overlaps another SOP action."
                if bool(diag.get("sop_conflicts"))
                else "First occurrence conflicts with selected priority policy."
            )
            prompt = (
                f"{reason_txt}\n"
                f"Suggested start: {suggested_display}\n\n"
                "Enter a new Daily Start time (HH:MM):"
            )
            while True:
                text, ok = QInputDialog.getText(
                    self,
                    "Adjust Daily Start",
                    prompt,
                    text=suggested_display,
                )
                if not ok:
                    return
                candidate = str(text or "").strip()
                if not self._is_valid_hhmm(candidate):
                    QMessageBox.warning(self, "SOP Conflict Resolution", "Start time must be HH:MM.")
                    continue
                new_start_utc = self._utc_start_hhmm_from_display(candidate, show_local=self._show_local)
                action["daily_start_utc"] = new_start_utc
                action["daily_end_utc"] = self._add_minutes_hhmm(new_start_utc, int(action.get("duration_minutes") or 60))
                validate = self.manager.detect_action_conflicts(
                    action=action,
                    operating_group=str(action.get("group_name") or "").strip().upper(),
                    horizon_days=7,
                    check_all_groups=True,
                    peer_actions=peer_actions,
                )
                if bool(validate.get("sop_conflicts")):
                    QMessageBox.warning(
                        self,
                        "SOP Conflict Resolution",
                        "This start time still overlaps another SOP action on a different frequency. Choose another time.",
                    )
                    continue
                if policy in {self.manager.CONFLICT_POLICY_NET, self.manager.CONFLICT_POLICY_DAILY} and bool(
                    validate.get("first_occurrence_conflict")
                ):
                    QMessageBox.warning(
                        self,
                        "SOP Conflict Resolution",
                        "This start time still conflicts for the first occurrence. Choose another time.",
                    )
                    continue
                break

        self._apply_realtime_action_resolution_to_row(row_index, action)

    def _apply_realtime_action_resolution_to_row(self, row_index: int, action: Dict[str, Any]) -> None:
        if row_index < 0 or row_index >= self.actions_table.rowCount():
            return
        group_combo = self.actions_table.cellWidget(row_index, self.COL_GROUP)
        start_edit = self.actions_table.cellWidget(row_index, self.COL_START)
        end_edit = self.actions_table.cellWidget(row_index, self.COL_END)
        if isinstance(group_combo, QComboBox):
            group_combo.setProperty(
                "conflict_policy",
                self.manager._normalize_conflict_policy(action.get("conflict_policy")),
            )
        display_start = self._display_start_hhmm_from_utc(str(action.get("daily_start_utc") or "00:00"), show_local=self._show_local)
        display_end = self._display_start_hhmm_from_utc(str(action.get("daily_end_utc") or "23:59"), show_local=self._show_local)
        self._suppress_realtime_conflict_checks = True
        try:
            if isinstance(start_edit, QLineEdit):
                start_edit.setText(display_start)
            if isinstance(end_edit, QLineEdit):
                end_edit.setText(display_end)
        finally:
            self._suppress_realtime_conflict_checks = False
        self._mark_dirty()
        self._schedule_realtime_hf_conflict_check()

    def _apply_accessibility_width_guards(self) -> None:
        buttons = [
            self.time_toggle_btn,
            self.new_btn,
            self.save_btn,
            self.delete_btn,
            self.export_pdf_btn,
            self.export_import_btn,
            self.add_row_btn,
        ]
        for btn in buttons:
            txt = str(btn.text() or "").replace("&", "").strip()
            if not txt:
                continue
            try:
                needed = int(btn.fontMetrics().horizontalAdvance(txt) + 30)
            except Exception:
                continue
            try:
                btn.setMinimumWidth(max(100, min(360, needed)))
            except Exception:
                pass

    def _schedule_layer_sync_refresh(self) -> None:
        return

    def _refresh_layer_sync_hint(self) -> None:
        return

    def _update_clock_labels(self) -> None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        now_local = now_utc.astimezone(tz)
        self.utc_label.setText(f"UTC: {now_utc.strftime('%Y-%m-%d %H:%M')}")
        self.local_label.setText(f"Local: {now_local.strftime('%Y-%m-%d %H:%M')}")
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")
        self._update_action_time_headers()
        self._update_time_toggle_style()

    def _update_action_time_headers(self) -> None:
        tz_short = "UTC" if not self._show_local else self._tz_short_name()
        start_item = self.actions_table.horizontalHeaderItem(self.COL_START)
        if start_item is None:
            start_item = QTableWidgetItem()
            self.actions_table.setHorizontalHeaderItem(self.COL_START, start_item)
        start_item.setText(f"Start ({tz_short})")
        end_item = self.actions_table.horizontalHeaderItem(self.COL_END)
        if end_item is None:
            end_item = QTableWidgetItem()
            self.actions_table.setHorizontalHeaderItem(self.COL_END, end_item)
        end_item.setText(f"End ({tz_short})")

    def _toggle_time_view(self) -> None:
        old_show_local = bool(self._show_local)
        self._show_local = not self._show_local
        self._update_clock_labels()
        for r in range(self.actions_table.rowCount()):
            start_edit = self.actions_table.cellWidget(r, self.COL_START)
            end_edit = self.actions_table.cellWidget(r, self.COL_END)
            if isinstance(start_edit, QLineEdit):
                current = start_edit.text().strip()
                if self._is_valid_hhmm(current):
                    utc_hhmm = self._utc_start_hhmm_from_display(current, show_local=old_show_local)
                    start_edit.setText(self._display_start_hhmm_from_utc(utc_hhmm, show_local=self._show_local))
            if isinstance(end_edit, QLineEdit):
                current = end_edit.text().strip()
                if self._is_valid_hhmm(current):
                    utc_hhmm = self._utc_start_hhmm_from_display(current, show_local=old_show_local)
                    end_edit.setText(self._display_start_hhmm_from_utc(utc_hhmm, show_local=self._show_local))
        self._mark_dirty()

    def _refresh_reference_data(self) -> None:
        data = self.settings.all()
        og = data.get("operating_groups", [])
        self._operating_groups = [g for g in og if isinstance(g, dict)]
        self._load_local_net_profiles_from_data(data)
        self._refresh_all_rows_dynamic_options()

    def _hf_group_names(self) -> List[str]:
        return sorted(
            {
                str(row.get("group", "")).strip().upper()
                for row in (self._operating_groups or [])
                if str(row.get("group", "")).strip()
            }
        )

    def _local_group_names(self) -> List[str]:
        return sorted(
            {
                str(row.get("group", "")).strip().upper()
                for row in (self._local_net_profiles or [])
                if str(row.get("group", "")).strip()
            }
        )

    def _local_resources_for_group(self, group: str) -> List[str]:
        grp = str(group or "").strip().upper()
        out: Set[str] = set()
        for row in self._local_net_profiles or []:
            if str(row.get("group", "")).strip().upper() != grp:
                continue
            resource = str(row.get("resource", "")).strip()
            if resource:
                out.add(resource)
        if not out:
            out.update(self.LOCAL_RESOURCE_FALLBACK)
        return sorted(out, key=lambda x: x.upper())

    def _local_modes_for_group_resource(self, group: str, resource: str) -> List[str]:
        grp = str(group or "").strip().upper()
        res = str(resource or "").strip().upper()
        out: Set[str] = set()
        for row in self._local_net_profiles or []:
            if str(row.get("group", "")).strip().upper() != grp:
                continue
            if res and str(row.get("resource", "")).strip().upper() != res:
                continue
            mode = str(row.get("mode", "")).strip()
            if mode:
                out.add(mode)
        if not out:
            out.update(self.LOCAL_MODE_FALLBACK)
        return sorted(out, key=lambda x: x.upper())

    def _hf_band_freq_options_for_group(self, group: str) -> List[str]:
        grp = str(group or "").strip().upper()
        out: List[Tuple[str, str]] = []
        for row in self._operating_groups or []:
            if str(row.get("group", "")).strip().upper() != grp:
                continue
            band = str(row.get("band", "")).strip().upper()
            freq = str(row.get("frequency", "")).strip()
            if not band or not freq:
                continue
            try:
                freq_fmt = f"{float(freq):.3f}"
            except Exception:
                freq_fmt = freq
            out.append((band, freq_fmt))
        out = sorted(set(out), key=lambda x: (x[0], float(x[1]) if x[1].replace(".", "", 1).isdigit() else 0.0))
        return [f"{band} - {freq}" for band, freq in out]

    def _resource_options_for_category(self, category: str, group: str) -> List[str]:
        if category == self.CAT_LOCAL:
            return self._local_resources_for_group(group)
        return list(self.HF_RESOURCE_OPTIONS)

    def _action_catalog(self) -> Dict[str, List[Tuple[str, str]]]:
        catalog: Dict[str, List[Tuple[str, str]]] = {
            "JS8Call": [("js8_send_status", "Status"), ("js8_commstat", "CommStat")],
            "VarAC": [
                ("varac_send_broadcast", "Broadcast"),
                ("varac_direct_contact", "Direct Contact"),
                ("varac_send_sitrep", "SitRep"),
                ("varac_send_statrep", "StatRep"),
                ("varac_send_report", "General"),
            ],
            "FLDigi": [
                ("fldigi_send_sitrep", "SitRep"),
                ("fldigi_send_statrep", "StatRep"),
                ("fldigi_send_report", "General"),
            ],
            "SSB": [
                ("ssb_monitor", "Monitor"),
                ("ssb_checkin", "Check-in"),
                ("ssb_message", "Message"),
            ],
            "Local Net": [
                ("local_ncs", "NCS"),
                ("local_checkin", "Check-in"),
                ("local_message", "Message"),
                ("local_monitor", "Monitor"),
            ],
        }
        for key, label in self._load_spotter_forms():
            catalog.setdefault("JS8Call", []).append((key, label))
        return catalog

    def _current_category(self) -> str:
        return str(self.category_combo.currentData() or self.CAT_HF)

    def _apply_category_table_view(self) -> None:
        cat = self._current_category()
        is_hf = cat == self.CAT_HF
        self.actions_table.setColumnHidden(self.COL_COND, not is_hf)
        self.actions_table.setColumnHidden(self.COL_BANDFREQ, not is_hf)
        self.actions_table.setColumnHidden(self.COL_MODE, is_hf)
        self.actions_table.setColumnHidden(self.COL_END, True)
        self.actions_table.setColumnHidden(self.COL_CONFLICT, not is_hf)
        self._autosize_actions_table()
        self._refresh_inline_conflict_badges()

    def _profile_id_for_category(self, category: str) -> int:
        cat = self.manager._normalize_category(category)
        for p in self._profiles:
            if self.manager._normalize_category(p.get("category")) == cat:
                return int(p.get("id") or 0)
        return 0

    def _reload_profiles(self, select_id: int | None) -> None:
        try:
            self.manager.enforce_single_profile_per_category()
        except Exception:
            pass
        hf = self.manager.ensure_category_profile(self.CAT_HF)
        local = self.manager.ensure_category_profile(self.CAT_LOCAL)
        profiles = {int(p.get("id") or 0): p for p in self.manager.list_profiles()}
        self._profiles = [
            profiles.get(int(hf.get("id") or 0), hf),
            profiles.get(int(local.get("id") or 0), local),
        ]

        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        self.profile_combo.addItem("HF SOP", int(hf.get("id") or 0))
        self.profile_combo.addItem("Local Comms SOP", int(local.get("id") or 0))
        self.profile_combo.blockSignals(False)

        target_id = int(select_id or 0)
        if target_id <= 0:
            target_id = int(self._selected_profile_id or 0)
        if target_id <= 0:
            target_id = int(hf.get("id") or 0)
        for idx in range(self.profile_combo.count()):
            if int(self.profile_combo.itemData(idx) or 0) != target_id:
                continue
            self.profile_combo.setCurrentIndex(idx)
            self._on_profile_selected(idx)
            return
        if self.profile_combo.count() > 0:
            self.profile_combo.setCurrentIndex(0)
            self._on_profile_selected(0)

    def _on_category_changed(self) -> None:
        if getattr(self, "_loading_ui", False):
            return
        self._last_realtime_conflict_signature = None
        cat = self._current_category()
        pid = self._profile_id_for_category(cat)
        if pid > 0:
            for idx in range(self.profile_combo.count()):
                if int(self.profile_combo.itemData(idx) or 0) != pid:
                    continue
                if self.profile_combo.currentIndex() != idx:
                    self.profile_combo.setCurrentIndex(idx)
                break
        self._apply_category_table_view()
        self._refresh_all_rows_dynamic_options()
        self._schedule_realtime_hf_conflict_check()

    @staticmethod
    def _is_local_action_dict(action: Dict[str, Any]) -> bool:
        software = str(action.get("software", "")).strip().upper()
        action_key = str(action.get("action_key", "")).strip().lower()
        contact_rule = str(action.get("contact_rule", "")).strip().lower()
        return software == "LOCAL NET" or action_key.startswith("local_") or contact_rule in {"local_group", "local_profile"}

    def _on_profile_selected(self, idx: int) -> None:
        if idx < 0:
            return
        profile_id = int(self.profile_combo.itemData(idx) or 0)
        if profile_id <= 0:
            return
        profile = self.manager.get_profile(profile_id)
        if not profile:
            return
        self._selected_profile_id = profile_id
        category = self.manager._normalize_category(profile.get("category"))
        actions = list(profile.get("actions") or [])
        if category == self.CAT_HF:
            actions = [a for a in actions if not self._is_local_action_dict(a)]
        else:
            actions = [a for a in actions if self._is_local_action_dict(a)]
        self._loading_ui = True
        try:
            self.name_edit.setText(str(profile.get("name") or ("HF SOP" if category == self.CAT_HF else "Local Comms SOP")))
            self.active_cb.setChecked(bool(profile.get("active")))
            self.category_combo.setCurrentIndex(0 if category == self.CAT_HF else 1)
            self._populate_actions(actions)
        finally:
            self._loading_ui = False
        self._apply_category_table_view()
        self._set_save_dirty(False)
        self._update_profile_action_styles()

    def _new_profile(self) -> None:
        choices = ["HF SOP", "Local Comms SOP"]
        choice, ok = QInputDialog.getItem(self, "New SOP", "Select SOP category:", choices, 0, False)
        if not ok:
            return
        category = self.CAT_HF if choice == "HF SOP" else self.CAT_LOCAL
        profile = self.manager.ensure_category_profile(category)
        pid = int(profile.get("id") or 0)
        if pid <= 0:
            return
        for idx in range(self.profile_combo.count()):
            if int(self.profile_combo.itemData(idx) or 0) != pid:
                continue
            self.profile_combo.setCurrentIndex(idx)
            break
        if self.actions_table.rowCount() > 0:
            resp = QMessageBox.question(
                self,
                "New SOP Draft",
                "Clear current action rows and start a new draft for this category?",
            )
            if resp == QMessageBox.Yes:
                self._populate_actions([])
                self.name_edit.setText("HF SOP" if category == self.CAT_HF else "Local Comms SOP")
                self.active_cb.setChecked(False)
                self._set_save_dirty(True)

    def _update_profile_action_styles(self, theme: Dict[str, str] | None = None) -> None:
        try:
            if theme is None:
                theme = resolve_theme(self.settings)
            has_profile = int(self._selected_profile_id or 0) > 0
            self.new_btn.setStyleSheet(button_style("muted", theme))
            self.save_btn.setStyleSheet(button_style("eligible_success" if self._dirty else "muted", theme))
            self.save_btn.setEnabled(True)
            self.delete_btn.setEnabled(has_profile)
            self.delete_btn.setStyleSheet(button_style("eligible_danger" if has_profile else "muted", theme))
            self.export_pdf_btn.setEnabled(True)
            self.export_pdf_btn.setStyleSheet(button_style("muted", theme))
            self.export_import_btn.setEnabled(True)
            self.export_import_btn.setStyleSheet(button_style("muted", theme))
            self.add_row_btn.setEnabled(has_profile)
            self.add_row_btn.setStyleSheet(button_style("eligible_primary" if has_profile else "muted", theme))
        except Exception:
            pass

    def _refresh_all_rows_dynamic_options(self) -> None:
        for r in range(self.actions_table.rowCount()):
            self._refresh_row_dynamic_options(r, preserve_current=True)

    def _combo_set_items(self, combo: QComboBox, values: List[str], current_text: str = "") -> None:
        combo.blockSignals(True)
        combo.clear()
        combo.addItem("")
        for val in values:
            if val:
                combo.addItem(val)
        if current_text and combo.findText(current_text) < 0:
            combo.addItem(current_text)
        combo.setCurrentText(current_text)
        self._fit_combo_popup(combo)
        combo.blockSignals(False)

    def _lookup_callsigns_for_contact_rule(self, group: str, rule: str) -> List[str]:
        grp = str(group or "").strip().upper()
        if not grp:
            return []
        normalized_rule = str(rule or "none").strip().lower()
        contacts = self.manager.resolve_primary_contacts(grp, "")
        if normalized_rule == "hub_or_hub_alt":
            return list(contacts.get("hub", []) or [])
        if normalized_rule == "ncs_or_ancs":
            return list(contacts.get("ncs", []) or [])
        if normalized_rule == "peer":
            return list(contacts.get("peer", []) or [])
        return self.manager.resolve_group_callsigns(grp, "")

    def _refresh_row_dynamic_options(self, row: int, *, preserve_current: bool) -> None:
        cat = self._current_category()
        group_combo = self.actions_table.cellWidget(row, self.COL_GROUP)
        cond_edit = self.actions_table.cellWidget(row, self.COL_COND)
        resource_combo = self.actions_table.cellWidget(row, self.COL_RESOURCE)
        mode_combo = self.actions_table.cellWidget(row, self.COL_MODE)
        action_combo = self.actions_table.cellWidget(row, self.COL_ACTION)
        bandfreq_combo = self.actions_table.cellWidget(row, self.COL_BANDFREQ)
        contact_combo = self.actions_table.cellWidget(row, self.COL_CONTACT)
        target_combo = self.actions_table.cellWidget(row, self.COL_CONTACT_TARGET)
        if not isinstance(group_combo, QComboBox) or not isinstance(resource_combo, QComboBox):
            return
        if not isinstance(mode_combo, QComboBox) or not isinstance(action_combo, QComboBox):
            return
        if not isinstance(bandfreq_combo, QComboBox) or not isinstance(contact_combo, QComboBox):
            return
        if not isinstance(target_combo, QComboBox):
            return

        group_current = group_combo.currentText().strip().upper()
        resource_current = resource_combo.currentText().strip()
        mode_current = mode_combo.currentText().strip()
        action_key_current = str(action_combo.currentData() or "").strip()
        bandfreq_current = bandfreq_combo.currentText().strip()
        contact_current = str(contact_combo.currentData() or "none").strip()

        group_values = self._hf_group_names() if cat == self.CAT_HF else self._local_group_names()
        self._combo_set_items(group_combo, group_values, group_current if preserve_current else "")
        group_current = group_combo.currentText().strip().upper()

        resource_values = self._resource_options_for_category(cat, group_current)
        if cat == self.CAT_LOCAL and resource_current and resource_current not in resource_values:
            resource_values = resource_values + [resource_current]
        self._combo_set_items(resource_combo, resource_values, resource_current if preserve_current else "")
        resource_current = resource_combo.currentText().strip()

        mode_values: List[str] = []
        if cat == self.CAT_LOCAL:
            mode_values = self._local_modes_for_group_resource(group_current, resource_current)
        else:
            mode_values = self._mode_options_for_group_band(group_current, "")
            if not mode_values:
                mode_values = ["DIGI", "USB", "LSB"]
        self._combo_set_items(mode_combo, mode_values, mode_current if preserve_current else "")

        action_pairs = []
        if cat == self.CAT_LOCAL:
            action_pairs = self._action_catalog().get("Local Net", [])
        else:
            action_pairs = self._action_catalog().get(resource_current, [])
        action_combo.blockSignals(True)
        action_combo.clear()
        for key, label in action_pairs:
            action_combo.addItem(label, key)
        if action_key_current and action_combo.findData(action_key_current) < 0:
            action_combo.addItem(action_key_current, action_key_current)
        idx = action_combo.findData(action_key_current)
        action_combo.setCurrentIndex(idx if idx >= 0 else (0 if action_combo.count() else -1))
        self._fit_combo_popup(action_combo)
        action_combo.blockSignals(False)
        action_key_current = str(action_combo.currentData() or "").strip()

        if cat == self.CAT_HF:
            bandfreq_values = self._hf_band_freq_options_for_group(group_current)
            self._combo_set_items(bandfreq_combo, bandfreq_values, bandfreq_current if preserve_current else "")
            bandfreq_combo.setEnabled(True)
        else:
            self._combo_set_items(bandfreq_combo, [], "")
            bandfreq_combo.setEnabled(False)

        contact_opts = self.CONTACT_RULE_OPTIONS
        if cat == self.CAT_LOCAL:
            if action_key_current == "local_monitor":
                contact_opts = [("none", "None")]
            else:
                contact_opts = list(self.LOCAL_CONTACT_OPTIONS)
        contact_combo.blockSignals(True)
        contact_combo.clear()
        for code, label in contact_opts:
            contact_combo.addItem(label, code)
        idx_contact = contact_combo.findData(contact_current)
        if idx_contact < 0:
            idx_contact = 0
        contact_combo.setCurrentIndex(idx_contact)
        self._fit_combo_popup(contact_combo)
        contact_combo.blockSignals(False)

        if cat == self.CAT_HF and isinstance(cond_edit, QLineEdit):
            if not cond_edit.text().strip():
                cond_edit.setText("ALL")
        if cat == self.CAT_LOCAL and isinstance(cond_edit, QLineEdit):
            cond_edit.setText("ALL")
            cond_edit.setEnabled(False)
        elif isinstance(cond_edit, QLineEdit):
            cond_edit.setEnabled(True)

        selected_contact = str(contact_combo.currentData() or "none").strip()
        target_enabled = selected_contact != "none"
        if cat == self.CAT_LOCAL and action_key_current == "local_monitor":
            target_enabled = False
        existing_target = str(target_combo.currentText() or "").strip().upper()
        option_values = self._lookup_callsigns_for_contact_rule(group_current, selected_contact) if target_enabled else []
        if selected_contact in {"hub_or_hub_alt", "ncs_or_ancs"} and target_enabled:
            option_values = ["Any (Role Match)"] + option_values
        self._combo_set_items(target_combo, option_values, existing_target if preserve_current else "")
        target_combo.setEditable(True)
        target_combo.setEnabled(target_enabled)
        self._apply_typeahead(target_combo)

    def _populate_actions(self, existing: List[Dict[str, Any]]) -> None:
        self.actions_table.setRowCount(0)
        rows = [r for r in (existing or []) if isinstance(r, dict)]
        rows.sort(key=lambda x: int(x.get("sort_order") or 0))
        for row in rows:
            self._add_action_row(existing=row, mark_dirty=False)
        if self.actions_table.rowCount() == 0:
            self._add_action_row(existing=None, mark_dirty=False)
        self._apply_category_table_view()
        self._autosize_actions_table()

    def _add_action_row(self, existing: Dict[str, Any] | None, *, mark_dirty: bool = True) -> None:
        row = self.actions_table.rowCount()
        self.actions_table.insertRow(row)
        cat = self._current_category()

        group_combo = QComboBox()
        group_combo.setEditable(True)
        group_combo.setProperty("action_id", int((existing or {}).get("id") or 0))
        group_combo.setProperty("sort_order", int((existing or {}).get("sort_order") or row))
        group_combo.setProperty(
            "conflict_policy",
            self.manager._normalize_conflict_policy((existing or {}).get("conflict_policy")),
        )
        group_combo.setProperty(
            "schedule_applied",
            bool((existing or {}).get("schedule_applied", True)),
        )
        self.actions_table.setCellWidget(row, self.COL_GROUP, group_combo)

        cond_edit = QLineEdit(self.manager._normalize_condition_levels((existing or {}).get("condition_levels") or "ALL"))
        cond_edit.setPlaceholderText("ALL or 1,3,5")
        self.actions_table.setCellWidget(row, self.COL_COND, cond_edit)

        resource_combo = QComboBox()
        resource_combo.setEditable(True)
        self.actions_table.setCellWidget(row, self.COL_RESOURCE, resource_combo)

        mode_combo = QComboBox()
        mode_combo.setEditable(True)
        self.actions_table.setCellWidget(row, self.COL_MODE, mode_combo)

        action_combo = QComboBox()
        self.actions_table.setCellWidget(row, self.COL_ACTION, action_combo)

        bandfreq_combo = QComboBox()
        bandfreq_combo.setEditable(True)
        self.actions_table.setCellWidget(row, self.COL_BANDFREQ, bandfreq_combo)

        start_utc = str((existing or {}).get("daily_start_utc") or "00:00")
        end_utc = str((existing or {}).get("daily_end_utc") or "23:59")
        start_edit = QLineEdit(self._display_start_hhmm_from_utc(start_utc, show_local=self._show_local))
        end_edit = QLineEdit(self._display_start_hhmm_from_utc(end_utc, show_local=self._show_local))
        start_edit.setPlaceholderText("HH:MM")
        end_edit.setPlaceholderText("HH:MM")
        self.actions_table.setCellWidget(row, self.COL_START, start_edit)
        self.actions_table.setCellWidget(row, self.COL_END, end_edit)

        duration_combo = QComboBox()
        for label, minutes in self.DURATION_OPTIONS:
            duration_combo.addItem(label, minutes)
        duration_val = int((existing or {}).get("duration_minutes") or 60)
        idx_dur = duration_combo.findData(duration_val)
        duration_combo.setCurrentIndex(idx_dur if idx_dur >= 0 else 1)
        self.actions_table.setCellWidget(row, self.COL_DURATION, duration_combo)

        interval_combo = QComboBox()
        interval_combo.setEditable(True)
        for preset in self.INTERVAL_PRESETS:
            interval_combo.addItem(preset, preset)
        interval_minutes = int((existing or {}).get("interval_minutes") or 0)
        if interval_minutes <= 0:
            interval_minutes = int((existing or {}).get("interval_hours") or 3) * 60
        interval_phase = int((existing or {}).get("interval_phase_minutes") or 0)
        interval_text = self._format_interval_spec(interval_minutes, interval_phase)
        if interval_combo.findText(interval_text) < 0:
            interval_combo.addItem(interval_text, interval_text)
        interval_combo.setCurrentText(interval_text)
        interval_combo.setToolTip("Examples: 00:30, 01:00, 03:00@30m")
        self.actions_table.setCellWidget(row, self.COL_INTERVAL, interval_combo)

        contact_combo = QComboBox()
        self.actions_table.setCellWidget(row, self.COL_CONTACT, contact_combo)

        target_combo = QComboBox()
        target_combo.setEditable(True)
        target_combo.setInsertPolicy(QComboBox.NoInsert)
        target_combo.setCurrentText(str((existing or {}).get("contact_target") or "").strip().upper())
        target_combo.setProperty("saved_target", str((existing or {}).get("contact_target") or "").strip().upper())
        if target_combo.lineEdit() is not None:
            target_combo.lineEdit().setPlaceholderText("Optional (type or select)")
        self.actions_table.setCellWidget(row, self.COL_CONTACT_TARGET, target_combo)

        desc_edit = QLineEdit(str((existing or {}).get("description") or "").strip())
        desc_edit.setPlaceholderText("Optional description")
        self.actions_table.setCellWidget(row, self.COL_DESC, desc_edit)

        conflict_badge = QLabel("Pending")
        conflict_badge.setAlignment(Qt.AlignCenter)
        self.actions_table.setCellWidget(row, self.COL_CONFLICT, conflict_badge)

        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(lambda _=False, b=remove_btn: self._remove_row_for_button(b))
        self.actions_table.setCellWidget(row, self.COL_REMOVE, remove_btn)

        existing_group = str((existing or {}).get("group_name") or "").strip().upper()
        if not existing_group:
            existing_group = str((existing or {}).get("operating_group") or "").strip().upper()
        existing_resource = str((existing or {}).get("software") or "").strip()
        existing_mode = str((existing or {}).get("mode") or "").strip()
        existing_action_key = str((existing or {}).get("action_key") or "").strip()
        existing_contact = str((existing or {}).get("contact_rule") or "none").strip()

        self._refresh_row_dynamic_options(row, preserve_current=False)
        group_combo.setCurrentText(existing_group)
        self._refresh_row_dynamic_options(row, preserve_current=True)
        if existing_resource:
            resource_combo.setCurrentText(existing_resource)
            self._refresh_row_dynamic_options(row, preserve_current=True)
        if existing_mode:
            mode_combo.setCurrentText(existing_mode)
        if cat == self.CAT_HF:
            band = str((existing or {}).get("band") or "").strip().upper()
            freq = str((existing or {}).get("frequency") or "").strip()
            if band and freq:
                bandfreq_combo.setCurrentText(f"{band} - {freq}")
        if existing_action_key:
            idx_action = action_combo.findData(existing_action_key)
            if idx_action >= 0:
                action_combo.setCurrentIndex(idx_action)
        idx_contact = contact_combo.findData(existing_contact)
        if idx_contact >= 0:
            contact_combo.setCurrentIndex(idx_contact)

        group_combo.currentIndexChanged.connect(lambda _=0, r=row: self._refresh_row_dynamic_options(r, preserve_current=True))
        group_combo.currentTextChanged.connect(lambda _=None, r=row: self._refresh_row_dynamic_options(r, preserve_current=True))
        resource_combo.currentIndexChanged.connect(lambda _=0, r=row: self._refresh_row_dynamic_options(r, preserve_current=True))
        resource_combo.currentTextChanged.connect(lambda _=None, r=row: self._refresh_row_dynamic_options(r, preserve_current=True))
        action_combo.currentIndexChanged.connect(lambda _=0, r=row: self._refresh_row_dynamic_options(r, preserve_current=True))
        contact_combo.currentIndexChanged.connect(lambda _=0, r=row: self._refresh_row_dynamic_options(r, preserve_current=True))

        for widget in (
            group_combo,
            cond_edit,
            resource_combo,
            mode_combo,
            action_combo,
            bandfreq_combo,
            start_edit,
            end_edit,
            duration_combo,
            interval_combo,
            contact_combo,
            target_combo,
            desc_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._mark_dirty)
                widget.currentTextChanged.connect(self._mark_dirty)
                widget.currentIndexChanged.connect(self._schedule_realtime_hf_conflict_check)
                widget.currentTextChanged.connect(self._schedule_realtime_hf_conflict_check)
            elif isinstance(widget, QLineEdit):
                widget.textChanged.connect(self._mark_dirty)
                widget.textChanged.connect(self._schedule_realtime_hf_conflict_check)
        self._autosize_actions_table()
        self._set_inline_conflict_badge(row, "pending")
        if mark_dirty:
            self._mark_dirty()

    def _remove_row_for_button(self, btn: QPushButton) -> None:
        for r in range(self.actions_table.rowCount()):
            if self.actions_table.cellWidget(r, self.COL_REMOVE) is btn:
                self.actions_table.removeRow(r)
                if self.actions_table.rowCount() == 0:
                    self._add_action_row(existing=None, mark_dirty=False)
                self._autosize_actions_table()
                self._mark_dirty()
                self._last_realtime_conflict_signature = None
                self._schedule_realtime_hf_conflict_check()
                return

    def _autosize_actions_table(self) -> None:
        try:
            for col in (
                self.COL_GROUP,
                self.COL_COND,
                self.COL_RESOURCE,
                self.COL_MODE,
                self.COL_ACTION,
                self.COL_BANDFREQ,
                self.COL_START,
                self.COL_END,
                self.COL_DURATION,
                self.COL_INTERVAL,
                self.COL_CONTACT,
                self.COL_CONTACT_TARGET,
                self.COL_CONFLICT,
                self.COL_REMOVE,
            ):
                self.actions_table.resizeColumnToContents(col)
            min_widths = {
                self.COL_GROUP: 150,
                self.COL_RESOURCE: 130,
                self.COL_ACTION: 140,
                self.COL_BANDFREQ: 180,
                self.COL_INTERVAL: 110,
                self.COL_CONTACT_TARGET: 170,
                self.COL_CONFLICT: 96,
            }
            for col, min_w in min_widths.items():
                if self.actions_table.columnWidth(col) < min_w:
                    self.actions_table.setColumnWidth(col, min_w)
            if self.actions_table.columnWidth(self.COL_DESC) < 240:
                self.actions_table.setColumnWidth(self.COL_DESC, 240)
        except Exception:
            pass

    @staticmethod
    def _split_band_freq(value: str) -> Tuple[str, str]:
        text = str(value or "").strip()
        if not text:
            return "", ""
        if "-" in text:
            band, freq = text.split("-", 1)
            return band.strip().upper(), freq.strip()
        parts = text.split()
        if len(parts) >= 2:
            return parts[0].strip().upper(), parts[-1].strip()
        return "", text

    @staticmethod
    def _add_minutes_hhmm(hhmm: str, minutes: int) -> str:
        try:
            parts = str(hhmm or "00:00").strip().split(":")
            h = max(0, min(23, int(parts[0])))
            m = max(0, min(59, int(parts[1])))
        except Exception:
            h, m = 0, 0
        base = (h * 60) + m
        total = max(0, min((24 * 60) - 1, base + int(minutes)))
        return f"{total // 60:02d}:{total % 60:02d}"

    def _collect_profile_payload(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]], None]:
        name = self.name_edit.text().strip()
        if not name:
            raise ValueError("SOP name is required.")
        category = self._current_category()
        active = bool(self.active_cb.isChecked())
        profile_id = int(self._selected_profile_id or 0)

        actions: List[Dict[str, Any]] = []
        for r in range(self.actions_table.rowCount()):
            group_combo = self.actions_table.cellWidget(r, self.COL_GROUP)
            cond_edit = self.actions_table.cellWidget(r, self.COL_COND)
            resource_combo = self.actions_table.cellWidget(r, self.COL_RESOURCE)
            mode_combo = self.actions_table.cellWidget(r, self.COL_MODE)
            action_combo = self.actions_table.cellWidget(r, self.COL_ACTION)
            bandfreq_combo = self.actions_table.cellWidget(r, self.COL_BANDFREQ)
            start_edit = self.actions_table.cellWidget(r, self.COL_START)
            end_edit = self.actions_table.cellWidget(r, self.COL_END)
            duration_combo = self.actions_table.cellWidget(r, self.COL_DURATION)
            interval_combo = self.actions_table.cellWidget(r, self.COL_INTERVAL)
            contact_combo = self.actions_table.cellWidget(r, self.COL_CONTACT)
            target_combo = self.actions_table.cellWidget(r, self.COL_CONTACT_TARGET)
            desc_edit = self.actions_table.cellWidget(r, self.COL_DESC)
            if not isinstance(group_combo, QComboBox) or not isinstance(resource_combo, QComboBox):
                continue
            if not isinstance(mode_combo, QComboBox) or not isinstance(action_combo, QComboBox):
                continue
            if not isinstance(bandfreq_combo, QComboBox) or not isinstance(start_edit, QLineEdit):
                continue
            if not isinstance(end_edit, QLineEdit) or not isinstance(duration_combo, QComboBox):
                continue
            if not isinstance(interval_combo, QComboBox) or not isinstance(contact_combo, QComboBox):
                continue
            if not isinstance(target_combo, QComboBox) or not isinstance(desc_edit, QLineEdit):
                continue

            group_name = group_combo.currentText().strip().upper()
            condition_levels = self.manager._normalize_condition_levels(cond_edit.text().strip()) if isinstance(cond_edit, QLineEdit) else "ALL"
            resource = resource_combo.currentText().strip()
            mode = mode_combo.currentText().strip().upper()
            action_key = str(action_combo.currentData() or "").strip()
            action_label = action_combo.currentText().strip()
            band = ""
            freq = ""
            if category == self.CAT_HF:
                band, freq = self._split_band_freq(bandfreq_combo.currentText().strip())
            start_display = start_edit.text().strip()
            end_display = end_edit.text().strip() if isinstance(end_edit, QLineEdit) else ""
            description = desc_edit.text().strip()
            contact_rule = str(contact_combo.currentData() or "none").strip()
            contact_target = str(target_combo.currentText() or "").strip().upper()
            if contact_target == "ANY (ROLE MATCH)":
                contact_target = self.ANY_ROLE_TOKEN

            row_blank = not any([group_name, resource, action_key, action_label, description, contact_target, band, freq])
            if row_blank:
                continue

            if category == self.CAT_HF and not group_name:
                raise ValueError(f"Row {r + 1}: Group is required for HF SOP.")
            if category == self.CAT_LOCAL and not group_name:
                raise ValueError(f"Row {r + 1}: Group is required for Local Comms SOP.")
            if not resource:
                raise ValueError(f"Row {r + 1}: Resource is required.")
            if not action_key:
                raise ValueError(f"Row {r + 1}: Action is required.")
            if category == self.CAT_HF and (not band or not freq):
                raise ValueError(f"Row {r + 1}: Band - Freq is required for HF SOP.")
            if not self._is_valid_hhmm(start_display):
                raise ValueError(f"Row {r + 1}: Daily Start must be HH:MM.")
            start_utc = self._utc_start_hhmm_from_display(start_display, show_local=self._show_local)
            duration_minutes = int(duration_combo.currentData() or 60)
            if duration_minutes not in {30, 60}:
                duration_minutes = 60
            end_utc = self._add_minutes_hhmm(start_utc, duration_minutes)
            if isinstance(end_edit, QLineEdit):
                end_edit.setText(self._display_start_hhmm_from_utc(end_utc, show_local=self._show_local))
            interval_minutes, phase_minutes = self._parse_interval_spec(interval_combo.currentText())
            if category == self.CAT_LOCAL and action_key == "local_monitor":
                contact_rule = "none"
                contact_target = ""

            action_id = int(group_combo.property("action_id") or 0)
            conflict_policy = self.manager._normalize_conflict_policy(group_combo.property("conflict_policy"))
            schedule_applied = bool(group_combo.property("schedule_applied"))
            actions.append(
                {
                    "id": action_id,
                    "group_name": group_name,
                    "condition_levels": condition_levels if category == self.CAT_HF else "ALL",
                    "band": band if category == self.CAT_HF else "",
                    "frequency": freq if category == self.CAT_HF else "",
                    "software": "Local Net" if category == self.CAT_LOCAL else resource,
                    "mode": mode if category == self.CAT_LOCAL else "",
                    "action_key": action_key,
                    "action_label": action_label or action_key,
                    "enabled": True,
                    "daily_start_utc": start_utc,
                    "daily_end_utc": end_utc,
                    "duration_minutes": duration_minutes,
                    "interval_minutes": interval_minutes,
                    "interval_phase_minutes": phase_minutes,
                    "interval_hours": max(1, int((interval_minutes + 59) // 60)),
                    "conflict_policy": conflict_policy,
                    "daily_conflict_summary": "",
                    "net_conflict_summary": "",
                    "schedule_applied": schedule_applied,
                    "description": description,
                    "contact_rule": contact_rule,
                    "contact_target": contact_target,
                    "sort_order": r,
                }
            )

        if not actions:
            raise ValueError("Add at least one action row.")

        profile_group = ""
        if category == self.CAT_HF:
            for action in actions:
                grp = str(action.get("group_name") or "").strip().upper()
                if grp:
                    profile_group = grp
                    break
            if not profile_group:
                raise ValueError("At least one HF action row must include a Group.")

        payload = {
            "id": profile_id,
            "name": name,
            "category": category,
            "operating_group": profile_group,
            "secondary_group": "",
            "frequency": "",
            "sop_start_utc": "00:00",
            "priority": 100,
            "active": active,
            "window_hours": 24,
        }
        return payload, actions, None

    def _resolve_hf_activation_conflicts(self, actions: List[Dict[str, Any]]) -> bool:
        hf_peer_actions = [
            a
            for a in actions
            if isinstance(a, dict) and not self.manager._is_local_action(a) and bool(a.get("enabled", True))
        ]
        for idx, action in enumerate(actions):
            group = str(action.get("group_name") or "").strip().upper()
            if not group:
                continue
            diag = self.manager.detect_action_conflicts(
                action=action,
                operating_group=group,
                horizon_days=7,
                check_all_groups=True,
                peer_actions=hf_peer_actions,
            )
            action["daily_conflict_summary"] = str(diag.get("daily_summary") or "")
            action["net_conflict_summary"] = str(diag.get("net_summary") or "")
            if not bool(diag.get("has_conflict")):
                action["conflict_policy"] = self.manager.CONFLICT_POLICY_SOP
                continue

            details = [
                f"Action Row {idx + 1}: {action.get('action_label', 'Action')}",
                f"Group: {group}",
                "",
                f"Daily Schedule Conflicts: {diag.get('daily_summary') or 'None'}",
                f"Net Schedule Conflicts: {diag.get('net_summary') or 'None'}",
                f"SOP Action Conflicts: {diag.get('sop_summary') or 'None'}",
                "",
                "Choose conflict handling policy:",
            ]
            msg = QMessageBox(self)
            msg.setWindowTitle("SOP Conflict Resolution")
            msg.setText("\n".join(details))
            sop_btn = msg.addButton("1) SOP Priority For ALL", QMessageBox.AcceptRole)
            net_btn = msg.addButton("2) Net Priority on Conflicts", QMessageBox.ActionRole)
            daily_btn = msg.addButton("3) Daily Schedule Priority on Conflicts", QMessageBox.ActionRole)
            cancel_btn = msg.addButton(QMessageBox.Cancel)
            msg.exec()
            clicked = msg.clickedButton()
            if clicked == cancel_btn:
                return False
            if clicked == net_btn:
                policy = self.manager.CONFLICT_POLICY_NET
            elif clicked == daily_btn:
                policy = self.manager.CONFLICT_POLICY_DAILY
            else:
                policy = self.manager.CONFLICT_POLICY_SOP
            action["conflict_policy"] = policy

            # Net/Daily policy must avoid first-occurrence conflicts; SOP-vs-SOP overlap must always be resolved.
            must_adjust_start = bool(diag.get("first_occurrence_conflict")) and (
                policy in {self.manager.CONFLICT_POLICY_NET, self.manager.CONFLICT_POLICY_DAILY}
                or bool(diag.get("sop_conflicts"))
            )
            if must_adjust_start:
                suggested_utc = self.manager.suggest_non_conflicting_start(
                    action=action,
                    operating_group=group,
                    check_all_groups=True,
                    peer_actions=hf_peer_actions,
                )
                suggested_display = self._display_start_hhmm_from_utc(suggested_utc, show_local=self._show_local)
                reason_txt = (
                    "First occurrence overlaps another SOP action."
                    if bool(diag.get("sop_conflicts"))
                    else "First occurrence conflicts with selected priority policy."
                )
                prompt = (
                    f"{reason_txt}\n"
                    f"Suggested start: {suggested_display}\n\n"
                    "Enter a new Daily Start time (HH:MM):"
                )
                while True:
                    text, ok = QInputDialog.getText(
                        self,
                        "Adjust Daily Start",
                        prompt,
                        text=suggested_display,
                    )
                    if not ok:
                        return False
                    candidate = str(text or "").strip()
                    if not self._is_valid_hhmm(candidate):
                        QMessageBox.warning(self, "SOP Conflict Resolution", "Start time must be HH:MM.")
                        continue
                    new_start_utc = self._utc_start_hhmm_from_display(candidate, show_local=self._show_local)
                    action["daily_start_utc"] = new_start_utc
                    action["daily_end_utc"] = self._add_minutes_hhmm(new_start_utc, int(action.get("duration_minutes") or 60))
                    validate = self.manager.detect_action_conflicts(
                        action=action,
                        operating_group=group,
                        horizon_days=7,
                        check_all_groups=True,
                        peer_actions=hf_peer_actions,
                    )
                    action["daily_conflict_summary"] = str(validate.get("daily_summary") or "")
                    action["net_conflict_summary"] = str(validate.get("net_summary") or "")
                    if bool(validate.get("sop_conflicts")):
                        QMessageBox.warning(
                            self,
                            "SOP Conflict Resolution",
                            "This start time still overlaps another SOP action on a different frequency. Choose another time.",
                        )
                        continue
                    if policy in {self.manager.CONFLICT_POLICY_NET, self.manager.CONFLICT_POLICY_DAILY} and bool(
                        validate.get("first_occurrence_conflict")
                    ):
                        QMessageBox.warning(
                            self,
                            "SOP Conflict Resolution",
                            "This start time still conflicts for the first occurrence. Choose another time.",
                        )
                        continue
                    break
        return True

    def _save_profile(self) -> None:
        timer = getattr(self, "_realtime_conflict_timer", None)
        if timer is not None:
            timer.stop()
        prev_suppress = getattr(self, "_suppress_realtime_conflict_checks", False)
        self._suppress_realtime_conflict_checks = True
        try:
            payload, actions, _schedule_layer = self._collect_profile_payload()
            if payload.get("category") == self.CAT_HF:
                if not self._resolve_hf_activation_conflicts(actions):
                    return
            profile_id = self.manager.save_profile(payload, actions, schedule_layer=None)
            self._reload_profiles(select_id=profile_id)
            self._set_save_dirty(False)
            self._emit_sop_data_changed()
            QMessageBox.information(self, "SOP", "SOP saved.")
        except Exception as e:
            QMessageBox.warning(self, "SOP", str(e))
        finally:
            self._suppress_realtime_conflict_checks = prev_suppress

    def _delete_profile(self) -> None:
        if int(self._selected_profile_id or 0) <= 0:
            return
        resp = QMessageBox.question(
            self,
            "Clear SOP",
            "Clear all action rows for this SOP category?",
        )
        if resp != QMessageBox.Yes:
            return
        profile = self.manager.get_profile(int(self._selected_profile_id or 0))
        if not profile:
            return
        payload = dict(profile)
        payload["active"] = False
        payload["id"] = int(profile.get("id") or 0)
        actions: List[Dict[str, Any]] = []
        self.manager.save_profile(payload, actions, schedule_layer=None)
        self._reload_profiles(select_id=int(profile.get("id") or 0))
        self._set_save_dirty(False)
        self._emit_sop_data_changed()

    def refresh_upcoming(self) -> None:
        self._upcoming_rows = []

    def on_settings_saved(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        selected_id = int(self._selected_profile_id or 0)
        self._refresh_reference_data()
        self._reload_profiles(select_id=selected_id)
        self._update_clock_labels()

    def on_local_net_profiles_updated(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        self._refresh_reference_data()
        self._refresh_all_rows_dynamic_options()

    def on_tab_activated(self) -> None:
        self._update_clock_labels()

    def on_sop_profiles_updated(self) -> None:
        self._reload_profiles(select_id=int(self._selected_profile_id or 0))

    def select_profile(self, profile_id: int) -> bool:
        try:
            target = int(profile_id or 0)
        except Exception:
            return False
        if target <= 0:
            return False
        self._reload_profiles(select_id=target)
        return int(self._selected_profile_id or 0) == target

    def _import_profile(self) -> None:
        timer = getattr(self, "_realtime_conflict_timer", None)
        if timer is not None:
            timer.stop()
        prev_suppress = getattr(self, "_suppress_realtime_conflict_checks", False)
        self._suppress_realtime_conflict_checks = True
        try:
            src, _ = QFileDialog.getOpenFileName(self, "Import SOP", "", "JSON Files (*.json)")
            if not src:
                return
            payload = json.loads(Path(src).read_text(encoding="utf-8"))
            profile = payload.get("profile")
            if not isinstance(profile, dict):
                raise ValueError("Invalid SOP import payload.")
            category = self.manager._normalize_category(profile.get("category"))
            target_profile = self.manager.ensure_category_profile(category)
            target_id = int(target_profile.get("id") or 0)
            if target_id <= 0:
                raise ValueError("Could not resolve target SOP category profile.")

            imported = dict(profile)
            imported["id"] = target_id
            imported["category"] = category
            raw_actions = imported.pop("actions", [])
            if not isinstance(raw_actions, list):
                raw_actions = []
            imported_actions: List[Dict[str, Any]] = []
            for row in raw_actions:
                if not isinstance(row, dict):
                    continue
                clean = dict(row)
                clean.pop("id", None)
                imported_actions.append(clean)

            if category == self.CAT_HF:
                if not self._resolve_hf_activation_conflicts(imported_actions):
                    return
            self.manager.save_profile(imported, imported_actions, schedule_layer=None)
            self._reload_profiles(select_id=target_id)
            self._set_save_dirty(False)
            self._emit_sop_data_changed()
            QMessageBox.information(self, "Import SOP", "SOP imported.")
        except Exception as e:
            QMessageBox.warning(self, "Import SOP", str(e))
        finally:
            self._suppress_realtime_conflict_checks = prev_suppress

    def apply_theme(self) -> None:
        try:
            theme = resolve_theme(self.settings)
            self.terms_hint_label.setStyleSheet(f"color: {theme.get('text_muted', '#888')};")
            self._update_time_toggle_style(theme)
            self._update_profile_action_styles(theme)
            self._apply_accessibility_width_guards()
            self._refresh_inline_conflict_badges()
        except Exception:
            pass
