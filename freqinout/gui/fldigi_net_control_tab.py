from __future__ import annotations

import datetime
import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional
import re

from PySide6.QtCore import Qt, QTimer, QEvent, Signal
from PySide6.QtWidgets import (
    QFrame,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QCheckBox,
    QLineEdit,
    QTextEdit,
    QPushButton,
    QFileDialog,
    QMessageBox,
    QComboBox,
    QApplication,
    QCompleter,
    QDialog,
    QHeaderView,
    QTableWidget,
    QTableWidgetItem,
    QSizePolicy,
    QTabWidget,
    QToolButton,
    QScrollArea,
)
from PySide6.QtGui import QFontMetrics, QColor

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.core.perf_metrics import span as perf_span
from freqinout.core.checkins_db import upsert_checkins
from freqinout.core.config_paths import get_fldigi_checkin_dir
from freqinout.core.fldigi_log_checkin_parser import resolve_fldigi_log_path, scan_fldigi_log_file
from freqinout.core.fldigi_macro_parser import scan_macro_profile, count_detected_file_references
from freqinout.core.fldigi_role_workspace import (
    default_role_workspace_prefs,
    get_role_workspace_preset,
    load_role_workspace_prefs,
    normalize_role,
    role_compare_defaults,
)
from freqinout.gui.fldigi_macro_mapping_dialog import FldigiMacroMappingDialog
from freqinout.gui.fldigi_workspace_cards import WorkspaceBucketCard
from freqinout.utils.timezones import get_timezone
from freqinout.gui.qsy_helper import (
    load_operating_groups as qsy_load_operating_groups,
    snapshot_operating_groups as qsy_snapshot_operating_groups,
    build_qsy_options,
    refresh_qsy_combo,
    refresh_hold_duration_combo,
    selected_qsy_meta,
    selected_hold_duration,
    set_hold_duration_default,
    notify_hold_duration_default_changed,
    current_scheduler_freq,
    perform_qsy,
    perform_qsy_with_hold,
    get_suspend_until,
    set_suspend_until,
    suspend_snapshot,
    suspend_active,
    scheduler_enabled,
    resume_schedule_hold,
    active_hold_button_role,
    active_hold_button_text,
    active_hold_status_text,
)
from freqinout.gui.theme import resolve_theme, button_style


class FldigiNetControlTab(QWidget):
    """
    FLDigi Net Control tab.

    - Uses Settings (callsign, name, state)
    - Uses net_schedule entries to help auto-complete Net Name
    - Uses operator_checkins SQLite DB to auto-suggest known operators
    - Manages two files:
        * Net Check-in Macro File (main log)
        * Late Check-in Macro File (feed for late/new check-ins)
    """
    net_status_changed = Signal(str, bool)
    FLDIGI_MACRO_PROFILES_KEY = "fldigi_macro_profiles_v1"
    FLDIGI_SELECTED_MACRO_PROFILE_KEY = "fldigi_selected_macro_profile"
    FLDIGI_LOG_ASSISTED_ENABLED_KEY = "fldigi_log_assisted_enabled_v1"
    FLDIGI_LOG_ASSISTED_SHOW_REVIEW_KEY = "fldigi_log_assisted_show_review_v1"
    FLDIGI_LOG_ASSISTED_INCLUDE_TX_KEY = "fldigi_log_assisted_include_tx_v1"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()

        self._net_in_progress = False
        self._net_start_utc: Optional[str] = None
        self._active = False

        self._clock_timer: Optional[QTimer] = None

        # Next frequency change tracking
        self._next_change_utc: Optional[datetime.datetime] = None
        self._auto_end_done: bool = False
        self._qsy_options: Dict[str, Dict] = {}
        self._opgroups_sig: str = ""

        self._start_btn_default_style: str = ""
        self._save_btn_default_style: str = ""
        self._normalizing_main = False
        self._normalizing_late = False
        self._known_operator_rows: List[Dict[str, str]] = []
        self._known_operator_by_callsign: Dict[str, Dict[str, str]] = {}
        self._known_op_autofilled_prefix: str = ""
        self._known_op_autofill_consumed: bool = False
        self._known_op_tab_stage: int = 0
        self._known_op_pending_focus: Optional[str] = None
        self._macro_profile_loading = False
        self._workspace_role_loading = False
        self._role_workspace_prefs = default_role_workspace_prefs()
        self._workspace_bucket_cards: Dict[str, WorkspaceBucketCard] = {}
        self._workspace_bucket_defaults: Dict[str, Dict[str, str]] = {}
        self._workspace_visible_bucket_ids: set[str] = set()
        self._custom_bucket_cards: Dict[str, WorkspaceBucketCard] = {}
        self._custom_bucket_sources: Dict[str, str] = {}
        self._roster_syncing = False
        self._roster_loading = False
        self._log_assisted_loading = False
        self._macro_profile_combo_loading = False
        self._setup_details_expanded = False
        self._compare_workspace_expanded = False
        self._log_assisted_session_path: str = ""
        self._log_assisted_session_offset: int = 0
        self._log_assisted_session_start_utc: Optional[datetime.datetime] = None
        self._log_assisted_session_tx_context: str = ""
        self._log_assisted_seen_normalized: set[str] = set()
        self._log_assisted_candidates_by_callsign: Dict[str, Dict[str, object]] = {}
        self._activation_secondary_refresh_pending: bool = False
        self._activation_secondary_refresh_inflight: bool = False

        self._build_ui()
        self._apply_theme()
        self._load_settings()
        self._load_known_operators()
        self._setup_timers()
        self._refresh_qsy_options()
        self._set_net_button_styles(active=False)

    # ---------------- UI BUILD ---------------- #

    def _build_session_bar(self, layout: QVBoxLayout) -> None:
        session_frame = QFrame()
        session_frame.setFrameShape(QFrame.StyledPanel)
        session_frame.setFrameShadow(QFrame.Raised)
        session_layout = QVBoxLayout(session_frame)
        session_layout.setContentsMargins(10, 8, 10, 8)
        session_layout.setSpacing(6)

        title_row = QHBoxLayout()
        title_row.addWidget(QLabel("<h3>FLDigi / SSB Net Control</h3>"))
        title_row.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        title_row.addWidget(self.utc_label)
        title_row.addWidget(self.local_label)
        self.total_checkins_label = QLabel("Total Check-ins: 0")
        self.total_checkins_label.setStyleSheet("QLabel { border: 1px solid #888888; padding: 2px 6px; border-radius: 3px; }")
        title_row.addWidget(self.total_checkins_label)
        session_layout.addLayout(title_row)

        context_row = QHBoxLayout()
        context_row.addWidget(QLabel("Role:"))
        self.role_combo = QComboBox()
        self.role_combo.addItems(["NCS", "ANCS", "Joiner"])
        self.role_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.role_combo.setMinimumWidth(max(QFontMetrics(self.role_combo.font()).horizontalAdvance("Joiner") + 48, 120))
        context_row.addWidget(self.role_combo)

        context_row.addSpacing(16)
        context_row.addWidget(QLabel("Net Name:"))
        self.net_name_combo = QComboBox()
        self.net_name_combo.setEditable(True)
        self.net_name_combo.setInsertPolicy(QComboBox.NoInsert)
        self.net_name_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        context_row.addWidget(self.net_name_combo, stretch=1)

        context_row.addSpacing(16)
        self.next_change_label = QLabel("Next Scheduled Net: (unknown)")
        self.next_change_label.setAlignment(Qt.AlignCenter)
        self.next_change_label.setStyleSheet(
            "QLabel { border: 1px solid #888888; padding: 2px 6px; border-radius: 3px; }"
        )
        context_row.addWidget(self.next_change_label)
        session_layout.addLayout(context_row)

        qsy_row = QHBoxLayout()
        qsy_row.addStretch()
        self.qsy_combo = QComboBox()
        self.qsy_combo.currentIndexChanged.connect(self._update_qsy_button_enabled)
        qsy_row.addWidget(self.qsy_combo)
        self.hold_duration_combo = QComboBox()
        self.hold_duration_combo.setToolTip("Temporary schedule hold duration after QSY.")
        self.hold_duration_combo.currentIndexChanged.connect(self._on_hold_duration_changed)
        qsy_row.addWidget(self.hold_duration_combo)
        self.suspend_btn = QPushButton("QSY + Hold")
        self.suspend_btn.clicked.connect(self._on_suspend_clicked)
        qsy_row.addWidget(self.suspend_btn)
        self.ad_hoc_btn = QPushButton("Ad Hoc Net")
        self.ad_hoc_btn.clicked.connect(self._start_ad_hoc_net)
        qsy_row.addWidget(self.ad_hoc_btn)
        session_layout.addLayout(qsy_row)

        layout.addWidget(session_frame)

    def _build_setup_strip(self, layout: QVBoxLayout) -> None:
        setup_frame = QFrame()
        setup_frame.setFrameShape(QFrame.StyledPanel)
        setup_frame.setFrameShadow(QFrame.Raised)
        setup_layout = QVBoxLayout(setup_frame)
        setup_layout.setContentsMargins(10, 8, 10, 8)
        setup_layout.setSpacing(6)

        summary_row = QHBoxLayout()
        summary_row.addWidget(QLabel("Macro Set:"))
        self.macro_profile_combo = QComboBox()
        self.macro_profile_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.macro_profile_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.macro_profile_combo.setMinimumWidth(220)
        summary_row.addWidget(self.macro_profile_combo, stretch=1)
        self.macro_profile_refresh_btn = QPushButton("Refresh")
        self.macro_profile_map_btn = QPushButton("Mappings...")
        self.macro_profile_details_btn = QToolButton()
        self.macro_profile_details_btn.setText("Details")
        self.macro_profile_details_btn.setCheckable(True)
        self.macro_profile_details_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        summary_row.addWidget(self.macro_profile_refresh_btn)
        summary_row.addWidget(self.macro_profile_map_btn)
        summary_row.addWidget(self.macro_profile_details_btn)
        setup_layout.addLayout(summary_row)

        chip_row = QHBoxLayout()
        self.macro_mode_chip = self._make_status_chip("Macro: None")
        self.log_intake_chip = self._make_status_chip("Log Intake: Off")
        self.review_mode_chip = self._make_status_chip("Review: Off")
        self.tx_context_chip = self._make_status_chip("TX Context: Off")
        for chip in (
            self.macro_mode_chip,
            self.log_intake_chip,
            self.review_mode_chip,
            self.tx_context_chip,
        ):
            chip_row.addWidget(chip)
        chip_row.addStretch()
        setup_layout.addLayout(chip_row)

        self.setup_details_frame = QFrame()
        self.setup_details_frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        details_layout = QVBoxLayout(self.setup_details_frame)
        details_layout.setContentsMargins(0, 4, 0, 0)
        details_layout.setSpacing(4)

        path_row = QHBoxLayout()
        path_row.addWidget(QLabel("Macro File:"))
        self.macro_profile_edit = QLineEdit()
        self.macro_profile_edit.setPlaceholderText("Select an FLDigi macro profile (.mdf)")
        self.macro_profile_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        path_row.addWidget(self.macro_profile_edit, stretch=1)
        self.macro_profile_browse_btn = QPushButton("Browse...")
        self.macro_profile_clear_btn = QPushButton("Clear")
        path_row.addWidget(self.macro_profile_browse_btn)
        path_row.addWidget(self.macro_profile_clear_btn)
        details_layout.addLayout(path_row)

        self.macro_profile_status = QLabel("Legacy mode: no macro profile selected.")
        self.macro_profile_status.setWordWrap(True)
        details_layout.addWidget(self.macro_profile_status)

        self.macro_mapping_locations_label = QLabel()
        self.macro_mapping_locations_label.setWordWrap(True)
        self.macro_mapping_locations_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        details_layout.addWidget(self.macro_mapping_locations_label)

        log_controls_layout = QVBoxLayout()
        log_controls_layout.setSpacing(2)
        self.log_assisted_enable_chk = QCheckBox("Auto-add from FLDigi log")
        self.log_assisted_enable_chk.setToolTip("Automatically pull likely check-ins from the FLDigi log into the local roster.")
        log_controls_layout.addWidget(self.log_assisted_enable_chk)
        self.log_assisted_enable_help = QLabel("Pull likely check-ins from the FLDigi log into the local roster during the net.")
        self.log_assisted_enable_help.setWordWrap(True)
        self.log_assisted_enable_help.setStyleSheet("QLabel { color: #666666; margin-left: 24px; }")
        log_controls_layout.addWidget(self.log_assisted_enable_help)
        self.log_assisted_review_chk = QCheckBox("Hold uncertain decodes for review")
        self.log_assisted_review_chk.setToolTip("Keep incomplete or lower-confidence decodes out of the roster until reviewed.")
        log_controls_layout.addWidget(self.log_assisted_review_chk)
        self.log_assisted_review_help = QLabel("Questionable decodes stay in the Review tab so they can be checked before being added.")
        self.log_assisted_review_help.setWordWrap(True)
        self.log_assisted_review_help.setStyleSheet("QLabel { color: #666666; margin-left: 24px; }")
        log_controls_layout.addWidget(self.log_assisted_review_help)
        self.log_assisted_tx_chk = QCheckBox("Use sent TX as context")
        self.log_assisted_tx_chk.setToolTip("Use transmitted lines only to help interpret nearby received text. TX lines never become check-ins.")
        log_controls_layout.addWidget(self.log_assisted_tx_chk)
        self.log_assisted_tx_help = QLabel("Sent TX text is only used as nearby context. It never creates a check-in on its own.")
        self.log_assisted_tx_help.setWordWrap(True)
        self.log_assisted_tx_help.setStyleSheet("QLabel { color: #666666; margin-left: 24px; }")
        log_controls_layout.addWidget(self.log_assisted_tx_help)
        details_layout.addLayout(log_controls_layout)

        self.log_assisted_status = QLabel("Log-assisted intake disabled.")
        self.log_assisted_status.setWordWrap(True)
        details_layout.addWidget(self.log_assisted_status)
        setup_layout.addWidget(self.setup_details_frame)

        layout.addWidget(setup_frame)
        self._set_setup_details_expanded(False)

    def _make_status_chip(self, text: str) -> QLabel:
        chip = QLabel(text)
        chip.setStyleSheet(self._status_chip_style(resolve_theme(self.settings), "neutral"))
        return chip

    @staticmethod
    def _theme_chip_fill(color_hex: str, alpha: float) -> str:
        color = QColor(color_hex)
        color.setAlphaF(max(0.0, min(alpha, 1.0)))
        return color.name(QColor.HexArgb)

    def _status_chip_style(self, theme: Dict[str, str], state: str) -> str:
        state_key = str(state or "neutral").strip().lower()
        palette = {
            "neutral": (
                theme.get("surface_alt", theme.get("surface", "#f5f5f5")),
                theme.get("border", "#888888"),
                theme.get("text_muted", theme.get("text", "#444444")),
            ),
            "active": (
                self._theme_chip_fill(theme.get("accent", "#1565C0"), 0.18),
                theme.get("accent", "#1565C0"),
                theme.get("accent", "#1565C0"),
            ),
            "ready": (
                self._theme_chip_fill(theme.get("success", "#2E7D32"), 0.16),
                theme.get("success", "#2E7D32"),
                theme.get("success", "#2E7D32"),
            ),
            "warning": (
                self._theme_chip_fill(theme.get("warning", "#EF6C00"), 0.16),
                theme.get("warning", "#EF6C00"),
                theme.get("warning", "#EF6C00"),
            ),
        }
        background, border, text_color = palette.get(state_key, palette["neutral"])
        return (
            "QLabel {"
            f" border: 1px solid {border};"
            " padding: 2px 8px;"
            " border-radius: 9px;"
            f" background-color: {background};"
            f" color: {text_color};"
            " font-weight: 600;"
            " }"
        )

    def _set_status_chip(self, chip: QLabel, text: str, state: str) -> None:
        chip.setText(text)
        chip.setStyleSheet(self._status_chip_style(resolve_theme(self.settings), state))

    def _set_setup_details_expanded(self, expanded: bool) -> None:
        self._setup_details_expanded = bool(expanded)
        self.setup_details_frame.setVisible(self._setup_details_expanded)
        self.macro_profile_details_btn.setChecked(self._setup_details_expanded)
        self.macro_profile_details_btn.setArrowType(Qt.DownArrow if self._setup_details_expanded else Qt.RightArrow)
        self.macro_profile_details_btn.setStyleSheet(
            button_style("info", resolve_theme(self.settings)) if self._setup_details_expanded else ""
        )

    def _build_operator_action_band(self, layout: QVBoxLayout) -> None:
        known_row = QHBoxLayout()
        known_row.addWidget(QLabel("Operator Lookup/Add:"))
        self.known_op_edit = QLineEdit()
        self.known_op_edit.setPlaceholderText("Enter Callsign Name State...")
        self.known_op_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        known_row.addWidget(self.known_op_edit, stretch=1)
        known_row.addStretch()
        layout.addLayout(known_row)

        known_btn_row = QHBoxLayout()
        self.add_known_tfc_btn = QPushButton("Add to TFC")
        self.add_known_qru_btn = QPushButton("Add to QRU")
        self.add_known_late_btn = QPushButton("Add to LATE")
        self.add_known_seen_locally_btn = QPushButton("Add to Seen Locally")
        for btn in (
            self.add_known_tfc_btn,
            self.add_known_qru_btn,
            self.add_known_late_btn,
            self.add_known_seen_locally_btn,
        ):
            btn.setFocusPolicy(Qt.StrongFocus)
            known_btn_row.addWidget(btn)
        self._known_add_buttons = {
            "tfc": self.add_known_tfc_btn,
            "qru": self.add_known_qru_btn,
            "late": self.add_known_late_btn,
            "seen_locally": self.add_known_seen_locally_btn,
        }
        self._known_add_button_targets = {button: target for target, button in self._known_add_buttons.items()}
        known_btn_row.addStretch()
        layout.addLayout(known_btn_row)

    def _build_ui(self):
        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(0, 0, 0, 0)
        outer_layout.setSpacing(0)

        self._ncs_scroll_area = QScrollArea()
        self._ncs_scroll_area.setWidgetResizable(True)
        self._ncs_scroll_area.setFrameShape(QFrame.NoFrame)
        outer_layout.addWidget(self._ncs_scroll_area)

        self._ncs_scroll_content = QWidget()
        layout = QVBoxLayout(self._ncs_scroll_content)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(10)

        self._build_session_bar(layout)
        self._build_setup_strip(layout)
        self._build_operator_action_band(layout)

        layout.addSpacing(24)

        # Bucket workspace panels
        self._left_bucket_col = QVBoxLayout()
        self._left_bucket_col.setSpacing(10)
        layout.addLayout(self._left_bucket_col)

        self.roster_frame = QFrame()
        self.roster_frame.setFrameShape(QFrame.StyledPanel)
        self.roster_frame.setFrameShadow(QFrame.Raised)
        self.roster_frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        roster_layout = QVBoxLayout(self.roster_frame)
        roster_layout.setContentsMargins(10, 8, 10, 8)
        roster_layout.setSpacing(6)

        roster_header = QHBoxLayout()
        roster_header.addWidget(QLabel("<h3>Local Roster</h3>"))
        roster_header.addStretch()
        self.roster_total_label = QLabel("Total Check-ins: 0")
        self.roster_total_label.setStyleSheet("QLabel { border: 1px solid #888888; padding: 2px 6px; border-radius: 3px; }")
        roster_header.addWidget(self.roster_total_label)
        self.roster_tfc_label = QLabel("TFC: 0")
        self.roster_qru_label = QLabel("QRU: 0")
        self.roster_late_label = QLabel("LATE: 0")
        for label in (self.roster_tfc_label, self.roster_qru_label, self.roster_late_label):
            label.setStyleSheet("QLabel { border: 1px solid #888888; padding: 2px 6px; border-radius: 3px; }")
            roster_header.addWidget(label)
        roster_layout.addLayout(roster_header)

        roster_actions = QHBoxLayout()
        roster_actions.addWidget(QLabel("Roster Actions:"))
        self.copy_tfc_btn = QPushButton("Copy TFC")
        self.copy_qru_btn = QPushButton("Copy QRU")
        self.copy_late_btn = QPushButton("Copy LATE")
        self.copy_seen_locally_btn = QPushButton("Copy Seen Locally")
        self.copy_roster_summary_btn = QPushButton("Copy All Check-ins")
        for btn in (
            self.copy_tfc_btn,
            self.copy_qru_btn,
            self.copy_late_btn,
            self.copy_seen_locally_btn,
            self.copy_roster_summary_btn,
        ):
            roster_actions.addWidget(btn)
        roster_actions.addStretch()
        roster_layout.addLayout(roster_actions)

        self.roster_table = QTableWidget(0, 6)
        self.roster_table.setHorizontalHeaderLabels(["Callsign", "Name", "State", "Traffic", "Category", "Source"])
        self.roster_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.roster_table.setSelectionMode(QTableWidget.SingleSelection)
        self.roster_table.setAlternatingRowColors(True)
        self.roster_table.setWordWrap(False)
        self.roster_table.setEditTriggers(
            QTableWidget.DoubleClicked | QTableWidget.EditKeyPressed | QTableWidget.AnyKeyPressed
        )
        roster_header_view = self.roster_table.horizontalHeader()
        roster_header_view.setStretchLastSection(False)
        roster_header_view.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        roster_header_view.setSectionResizeMode(1, QHeaderView.Stretch)
        roster_header_view.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        roster_header_view.setSectionResizeMode(3, QHeaderView.Stretch)
        roster_header_view.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        roster_header_view.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.roster_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.roster_table.setMinimumHeight(220)
        roster_layout.addWidget(self.roster_table)

        self._left_bucket_col.addWidget(self.roster_frame)

        # These cards remain as hidden compatibility buffers for derived file
        # output and legacy save/end-net code. The roster table is the sole
        # operator-facing local working surface.
        self.tfc_card = WorkspaceBucketCard(self.settings, title="TFC")
        self.qru_card = WorkspaceBucketCard(self.settings, title="QRU")
        self.late_card = WorkspaceBucketCard(self.settings, title="LATE")
        self.reference_card = WorkspaceBucketCard(
            self.settings,
            title="NCS List",
            allow_paste=True,
            copy_label="Compare",
            on_copy=self._run_inline_compare,
            secondary_label="Add Missing",
            on_secondary=self._import_reference_missing,
            on_paste=self._paste_into_reference_card,
        )
        self.compare_results_card = WorkspaceBucketCard(
            self.settings,
            title="Compare Results",
            read_only=True,
            copy_label="Copy Missing",
            on_copy=self._copy_compare_results,
            secondary_label="Merge Missing",
            on_secondary=self._merge_compare_missing,
        )
        self.review_card = WorkspaceBucketCard(
            self.settings,
            title="Log Review",
            allow_paste=True,
            on_paste=self._paste_into_review_card,
            secondary_label="Merge Reviewed",
            on_secondary=self._merge_review_candidates,
        )

        self._workspace_bucket_cards = {
            "tfc": self.tfc_card,
            "qru": self.qru_card,
            "late": self.late_card,
            "reference": self.reference_card,
            "compare_results": self.compare_results_card,
            "review": self.review_card,
        }

        self._left_bucket_col.addStretch()

        self.tfc_card.setVisible(False)
        self.qru_card.setVisible(False)
        self.late_card.setVisible(False)

        self.compare_workspace_frame = QFrame()
        self.compare_workspace_frame.setFrameShape(QFrame.StyledPanel)
        self.compare_workspace_frame.setFrameShadow(QFrame.Raised)
        self.compare_workspace_frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        compare_workspace_layout = QVBoxLayout(self.compare_workspace_frame)
        compare_workspace_layout.setContentsMargins(10, 8, 10, 8)
        compare_workspace_layout.setSpacing(6)

        compare_header = QHBoxLayout()
        compare_header.addWidget(QLabel("<b>Compare / Reference</b>"))
        compare_header.addStretch()
        self.compare_workspace_toggle_btn = QToolButton()
        self.compare_workspace_toggle_btn.setText("Show")
        self.compare_workspace_toggle_btn.setCheckable(True)
        self.compare_workspace_toggle_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        compare_header.addWidget(self.compare_workspace_toggle_btn)
        compare_workspace_layout.addLayout(compare_header)

        self.compare_workspace_body = QWidget()
        compare_workspace_body_layout = QVBoxLayout(self.compare_workspace_body)
        compare_workspace_body_layout.setContentsMargins(0, 0, 0, 0)
        compare_workspace_body_layout.setSpacing(6)
        self.compare_workspace_tabs = QTabWidget()
        self.compare_workspace_tabs.setDocumentMode(True)
        self.compare_workspace_tabs.addTab(self.reference_card, "Reference")
        self.compare_workspace_tabs.addTab(self.compare_results_card, "Compare Results")
        self.compare_workspace_tabs.addTab(self.review_card, "Review")
        compare_workspace_body_layout.addWidget(self.compare_workspace_tabs)
        compare_workspace_layout.addWidget(self.compare_workspace_body)
        layout.addWidget(self.compare_workspace_frame)
        self._set_compare_workspace_expanded(False)

        # Legacy aliases kept for existing save/start/merge behavior.
        self.main_text = self.tfc_card.text_edit
        self.late_text = self.late_card.text_edit
        self.qru_text = self.qru_card.text_edit
        self.reference_text = self.reference_card.text_edit
        self.compare_results_text = self.compare_results_card.text_edit

        # Buttons bottom row (all on one row)
        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("Start Net")
        self.save_btn = QPushButton("Save Check-ins")
        self.merge_btn = QPushButton("Merge Check-ins")
        self.end_btn = QPushButton("End Net")

        # Button colors
        theme = resolve_theme(self.settings)
        self.start_btn.setStyleSheet(button_style("success", theme))
        self.end_btn.setStyleSheet(button_style("danger", theme))
        self.merge_btn.setStyleSheet(button_style("info", theme))
        self._start_btn_default_style = self.start_btn.styleSheet()
        self._save_btn_default_style = button_style("success", theme)
        self.save_btn.setStyleSheet(self._save_btn_default_style)

        btn_row.addWidget(self.start_btn)
        btn_row.addWidget(self.save_btn)
        btn_row.addWidget(self.end_btn)
        btn_row.addStretch()

        layout.addLayout(btn_row)

        # Signals
        self.main_text.textChanged.connect(self._on_main_text_changed)
        self.late_text.textChanged.connect(self._on_late_text_changed)
        self.roster_table.itemChanged.connect(self._on_roster_item_changed)
        self.roster_table.model().dataChanged.connect(self._on_roster_model_data_changed)
        self.roster_table.model().rowsInserted.connect(self._on_roster_model_rows_changed)
        self.roster_table.model().rowsRemoved.connect(self._on_roster_model_rows_changed)
        self.known_op_edit.textChanged.connect(self._on_known_op_text_changed)
        self.known_op_edit.returnPressed.connect(self._on_known_op_return)
        self.known_op_edit.installEventFilter(self)
        self.copy_tfc_btn.clicked.connect(lambda: self._copy_roster_category("TFC"))
        self.copy_qru_btn.clicked.connect(lambda: self._copy_roster_category("QRU"))
        self.copy_late_btn.clicked.connect(lambda: self._copy_roster_category("LATE"))
        self.copy_seen_locally_btn.clicked.connect(self._copy_roster_seen_locally)
        self.copy_roster_summary_btn.clicked.connect(self._copy_roster_summary)
        self.setTabOrder(self.known_op_edit, self.add_known_tfc_btn)

        self.start_btn.clicked.connect(self._start_net)
        self.save_btn.clicked.connect(self._save_checkins)
        self.end_btn.clicked.connect(self._end_net)

        for button in self._known_add_buttons.values():
            button.setDefault(False)
            button.installEventFilter(self)
        self.add_known_tfc_btn.clicked.connect(lambda: self._insert_known_into_bucket("tfc"))
        self.add_known_qru_btn.clicked.connect(lambda: self._insert_known_into_bucket("qru"))
        self.add_known_late_btn.clicked.connect(lambda: self._insert_known_into_bucket("late"))
        self.add_known_seen_locally_btn.clicked.connect(lambda: self._insert_known_into_bucket("seen_locally"))
        self.macro_profile_combo.currentIndexChanged.connect(self._on_macro_profile_combo_changed)
        self.macro_profile_refresh_btn.clicked.connect(self._refresh_macro_profile_choices)
        self.macro_profile_edit.editingFinished.connect(self._on_macro_profile_editing_finished)
        self.macro_profile_browse_btn.clicked.connect(self._choose_macro_profile)
        self.macro_profile_map_btn.clicked.connect(self._open_macro_mapping_dialog)
        self.macro_profile_clear_btn.clicked.connect(self._clear_macro_profile)
        self.macro_profile_details_btn.clicked.connect(self._toggle_setup_details)
        self.compare_workspace_toggle_btn.clicked.connect(self._toggle_compare_workspace)
        self.role_combo.currentTextChanged.connect(self._on_role_changed)
        self.log_assisted_enable_chk.toggled.connect(self._on_log_assisted_controls_changed)
        self.log_assisted_review_chk.toggled.connect(self._on_log_assisted_controls_changed)
        self.log_assisted_tx_chk.toggled.connect(self._on_log_assisted_controls_changed)
        self.main_text.textChanged.connect(self._on_workspace_text_changed)
        self.late_text.textChanged.connect(self._on_workspace_text_changed)
        self.qru_text.textChanged.connect(self._on_workspace_text_changed)
        self.reference_text.textChanged.connect(self._on_workspace_text_changed)
        self.compare_results_text.textChanged.connect(self._on_workspace_text_changed)
        self.review_card.text_edit.textChanged.connect(self._on_workspace_text_changed)

        self._apply_role_workspace(self.role_combo.currentText())
        self._ncs_scroll_area.setWidget(self._ncs_scroll_content)

    def _toggle_setup_details(self, *_args) -> None:
        self._set_setup_details_expanded(not self._setup_details_expanded)

    def _set_compare_workspace_expanded(self, expanded: bool) -> None:
        self._compare_workspace_expanded = bool(expanded)
        self.compare_workspace_body.setVisible(self._compare_workspace_expanded)
        self.compare_workspace_toggle_btn.setChecked(self._compare_workspace_expanded)
        self.compare_workspace_toggle_btn.setText("Hide" if self._compare_workspace_expanded else "Show")
        self.compare_workspace_toggle_btn.setArrowType(Qt.DownArrow if self._compare_workspace_expanded else Qt.RightArrow)

    def _toggle_compare_workspace(self, *_args) -> None:
        self._set_compare_workspace_expanded(not self._compare_workspace_expanded)

    def _set_compare_tab_visible(self, widget: QWidget, visible: bool) -> None:
        index = self.compare_workspace_tabs.indexOf(widget)
        if index < 0:
            return
        self.compare_workspace_tabs.setTabVisible(index, visible)
        if not visible and self.compare_workspace_tabs.currentIndex() == index:
            for candidate in (self.reference_card, self.compare_results_card, self.review_card):
                candidate_index = self.compare_workspace_tabs.indexOf(candidate)
                if candidate_index >= 0 and self.compare_workspace_tabs.isTabVisible(candidate_index):
                    self.compare_workspace_tabs.setCurrentIndex(candidate_index)
                    break

    def _sync_compare_workspace_tabs(self) -> None:
        self.compare_workspace_tabs.setTabText(self.compare_workspace_tabs.indexOf(self.reference_card), self.reference_card.title())
        self.compare_workspace_tabs.setTabText(self.compare_workspace_tabs.indexOf(self.compare_results_card), self.compare_results_card.title())
        self.compare_workspace_tabs.setTabText(self.compare_workspace_tabs.indexOf(self.review_card), self.review_card.title())
        self._set_compare_tab_visible(self.reference_card, not self.reference_card.isHidden())
        self._set_compare_tab_visible(self.compare_results_card, not self.compare_results_card.isHidden())
        self._set_compare_tab_visible(self.review_card, not self.review_card.isHidden())

    def _open_compare_dialog(self) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle("Compare Workspace Buckets")
        dialog.setMinimumWidth(760)
        layout = QVBoxLayout(dialog)

        prompt = QLabel("Compare any two buckets/lists. Role defaults are preselected but can be changed.")
        layout.addWidget(prompt)

        source_row = QHBoxLayout()
        source_row.addWidget(QLabel("Source bucket:"))
        source_combo = QComboBox()
        target_row = QHBoxLayout()
        target_row.addWidget(QLabel("Compare against:"))
        target_combo = QComboBox()
        bucket_options = self._workspace_bucket_options()
        for bucket_id, label in bucket_options:
            source_combo.addItem(label, bucket_id)
            target_combo.addItem(label, bucket_id)
        defaults = self._workspace_compare_defaults()
        source_idx = source_combo.findData(defaults.get("source_bucket_id", ""))
        target_idx = target_combo.findData(defaults.get("target_bucket_id", ""))
        if source_idx >= 0:
            source_combo.setCurrentIndex(source_idx)
        if target_idx >= 0:
            target_combo.setCurrentIndex(target_idx)
        source_row.addWidget(source_combo, stretch=1)
        target_row.addWidget(target_combo, stretch=1)
        layout.addLayout(source_row)
        layout.addLayout(target_row)

        result_label = QLabel("Local entries missing from NCS list")
        layout.addWidget(result_label)

        result_box = QTextEdit()
        result_box.setReadOnly(True)
        layout.addWidget(result_box)

        btn_row = QHBoxLayout()
        compare_btn = QPushButton("Compare")
        copy_btn = QPushButton("Copy Missing")
        close_btn = QPushButton("Close")
        btn_row.addStretch()
        btn_row.addWidget(compare_btn)
        btn_row.addWidget(copy_btn)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

        last_missing_text = ""
        theme = resolve_theme(self.settings)
        copy_btn.setStyleSheet(button_style("info", theme))

        def extract_callsigns(text: str) -> List[str]:
            seen = set()
            calls = []
            for line in (text or "").splitlines():
                cs, _, _ = self._parse_checkin_line(line)
                cs = (cs or "").strip().upper()
                if not cs or cs in seen:
                    continue
                seen.add(cs)
                calls.append(cs)
            return calls

        def extract_local_entries(text: str) -> List[Dict[str, str]]:
            entries: List[Dict[str, str]] = []
            seen = set()
            for line in (text or "").splitlines():
                cs, name, state = self._parse_checkin_line(line)
                cs = (cs or "").strip().upper()
                if not cs or cs in seen:
                    continue
                seen.add(cs)
                entry = {
                    "callsign": cs,
                    "name": (name or "").strip(),
                    "state": (state or "").strip().upper(),
                }
                entries.append(entry)
            return entries

        def run_compare() -> None:
            source_bucket = str(source_combo.currentData() or "")
            target_bucket = str(target_combo.currentData() or "")
            source_text = self._workspace_bucket_text(source_bucket)
            target_text = self._workspace_bucket_text(target_bucket)
            local_entries = extract_local_entries(source_text)
            pasted_calls = set(extract_callsigns(target_text))
            missing_entries = [
                self._format_entry(e["callsign"], e["name"], e["state"])
                for e in local_entries
                if e["callsign"] not in pasted_calls
            ]
            nonlocal last_missing_text
            last_missing_text = "\n".join(missing_entries) if missing_entries else ""

            lines = [
                f"Source bucket ({source_bucket}): {len(local_entries)}",
                f"Compare bucket ({target_bucket}): {len(pasted_calls)}",
                "",
                "Local entries missing from NCS list:",
                last_missing_text if missing_entries else "(none)",
            ]
            result_box.setPlainText("\n".join(lines))
            if not missing_entries:
                msg = QMessageBox(dialog)
                msg.setWindowTitle("Compare NCS Lists")
                msg.setText("Full Match")
                msg.addButton("Close", QMessageBox.AcceptRole)
                msg.exec()
                dialog.accept()

        compare_btn.clicked.connect(run_compare)
        def copy_and_close() -> None:
            if not last_missing_text:
                QMessageBox.information(dialog, "Compare Workspace Buckets", "No missing entries to copy.")
                return
            QApplication.clipboard().setText(last_missing_text)
            dialog.accept()

        copy_btn.clicked.connect(copy_and_close)
        close_btn.clicked.connect(dialog.accept)

        dialog.exec()

    def _workspace_bucket_options(self) -> List[tuple[str, str]]:
        role = normalize_role(self.role_combo.currentText())
        preset = get_role_workspace_preset(role)
        options: List[tuple[str, str]] = []
        visible_ids = set(self._workspace_visible_bucket_ids)
        visible_ids.add("roster")
        visible_ids.update({preset.compare_source_bucket_id, preset.compare_target_bucket_id})
        options.append(("roster", "Local Roster"))
        for bucket in preset.all_buckets():
            if bucket.bucket_id in visible_ids or bucket.visible_by_default:
                card = self._workspace_bucket_cards.get(bucket.bucket_id)
                options.append((bucket.bucket_id, card.title() if card is not None else bucket.title))
        review_card = self._workspace_bucket_cards.get("review")
        if review_card is not None and review_card.isVisible():
            options.append(("review", review_card.title()))
        for bucket_id, card in self._custom_bucket_cards.items():
            if bucket_id in self._workspace_visible_bucket_ids and card.isVisible():
                options.append((bucket_id, card.title()))
        if not options:
            options = [("tfc", "TFC"), ("late", "LATE")]
        return options

    def _workspace_compare_defaults(self) -> Dict[str, str]:
        defaults = role_compare_defaults(self.role_combo.currentText(), self._role_workspace_prefs)
        defaults["source_bucket_id"] = "roster"
        return defaults

    def _workspace_bucket_text(self, bucket_id: str) -> str:
        if bucket_id in {"roster", "local_roster"}:
            return self._roster_table_text()
        if bucket_id in {"reference", "ncs_reference", "ancs_reference"}:
            return self.reference_text.toPlainText()
        if bucket_id == "compare_results":
            return self.compare_results_text.toPlainText()
        if bucket_id == "review":
            return self.review_card.text()
        if bucket_id == "seen_locally":
            return self._roster_table_text()
        bucket = self._workspace_bucket_cards.get(bucket_id)
        return bucket.text() if bucket else ""

    def _roster_table_rows(self) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        if not hasattr(self, "roster_table"):
            return rows
        for row in range(self.roster_table.rowCount()):
            callsign_item = self.roster_table.item(row, 0)
            name_item = self.roster_table.item(row, 1)
            state_item = self.roster_table.item(row, 2)
            traffic_item = self.roster_table.item(row, 3)
            source_item = self.roster_table.item(row, 5)
            category_widget = self.roster_table.cellWidget(row, 4)
            category = ""
            if category_widget is not None and hasattr(category_widget, "currentText"):
                category = str(category_widget.currentText() or "").strip().upper()
            elif self.roster_table.item(row, 4) is not None:
                category = str(self.roster_table.item(row, 4).text() or "").strip().upper()
            rows.append({
                "callsign": (callsign_item.text() if callsign_item else "").strip().upper(),
                "name": (name_item.text() if name_item else "").strip(),
                "state": (state_item.text() if state_item else "").strip().upper(),
                "traffic": (traffic_item.text() if traffic_item else "").strip(),
                "category": category or "TFC",
                "source": (source_item.text() if source_item else "").strip(),
            })
        return rows

    def _roster_table_text(self, category: Optional[str] = None) -> str:
        wanted = (category or "").strip().upper()
        lines: List[str] = []
        for row in self._roster_table_rows():
            if wanted and row["category"].upper() != wanted:
                continue
            formatted = self._format_entry(row["callsign"], row["name"], row["state"])
            if row["traffic"]:
                formatted = f"{formatted} {row['traffic']}"
            if formatted.strip():
                lines.append(formatted)
        return "\n".join(lines)

    def _roster_unique_callsigns(self) -> List[str]:
        seen = set()
        callsigns: List[str] = []
        for row in self._roster_table_rows():
            cs = row["callsign"]
            if not cs or cs in seen:
                continue
            seen.add(cs)
            callsigns.append(cs)
        return callsigns

    def _roster_sync_legacy_buffers(self, *, write_files: bool = True) -> None:
        if self._roster_syncing:
            return
        self._roster_syncing = True
        try:
            main_text = self._roster_table_text("TFC")
            qru_text = self._roster_table_text("QRU")
            late_text = self._roster_table_text("LATE")
            self.main_text.setPlainText(main_text)
            self.qru_text.setPlainText(qru_text)
            self.late_text.setPlainText(late_text)
            self._update_bucket_card_states()
            if write_files and self._net_in_progress:
                main_path, qru_path, late_path = self._checkin_file_paths()
                self._write_file(main_path, main_text)
                self._write_file(qru_path, qru_text)
                self._write_file(late_path, late_text)
                self._write_file(self._all_checkins_file_path(), self._roster_table_text())
        finally:
            self._roster_syncing = False

    def _reference_source_label(self) -> str:
        role = normalize_role(self.role_combo.currentText())
        if role == "NCS":
            return "ANCS"
        return "NCS"

    def _extract_unique_entries(self, text: str) -> List[Dict[str, str]]:
        entries: List[Dict[str, str]] = []
        seen = set()
        for line in (text or "").splitlines():
            cs, name, state, extra = self._checkin_line_details(line)
            cs = (cs or "").strip().upper()
            if not cs or cs in seen:
                continue
            seen.add(cs)
            traffic, category = self._traffic_category_from_text(extra)
            entries.append({
                "callsign": cs,
                "name": (name or "").strip(),
                "state": (state or "").strip().upper(),
                "traffic": traffic,
                "category": category,
            })
        return entries

    def _reference_entries_missing_from_roster(self) -> List[Dict[str, str]]:
        roster_calls = {row["callsign"] for row in self._roster_table_rows() if row["callsign"]}
        missing: List[Dict[str, str]] = []
        for entry in self._extract_unique_entries(self.reference_text.toPlainText()):
            if entry["callsign"] not in roster_calls:
                missing.append(entry)
        return missing

    def _roster_clear(self) -> None:
        self.roster_table.setRowCount(0)

    def _roster_find_row(self, callsign: str) -> int:
        target = (callsign or "").strip().upper()
        if not target:
            return -1
        for row in range(self.roster_table.rowCount()):
            item = self.roster_table.item(row, 0)
            if item is not None and item.text().strip().upper() == target:
                return row
        return -1

    def _roster_remove_callsign(self, callsign: str) -> None:
        row = self._roster_find_row(callsign)
        if row >= 0:
            self.roster_table.removeRow(row)

    def _roster_set_category(self, row: int, category: str) -> None:
        widget = self.roster_table.cellWidget(row, 4)
        target = (category or "TFC").strip().upper()
        if widget is not None and hasattr(widget, "setCurrentText"):
            widget.blockSignals(True)
            try:
                widget.setCurrentText(target)
            finally:
                widget.blockSignals(False)
        else:
            item = self.roster_table.item(row, 4)
            if item is None:
                item = QTableWidgetItem(target)
                self.roster_table.setItem(row, 4, item)
            item.setText(target)

    def _roster_set_row(
        self,
        row: int,
        callsign: str,
        name: str,
        state: str,
        traffic: str,
        category: str,
        source: str,
    ) -> None:
        self._roster_syncing = True
        try:
            values = [
                callsign.strip().upper(),
                name.strip(),
                state.strip().upper(),
                traffic.strip(),
                category.strip().upper() or "TFC",
                source.strip(),
            ]
            for col in range(4):
                item = self.roster_table.item(row, col)
                if item is None:
                    item = QTableWidgetItem("")
                    if col != 4:
                        item.setFlags(item.flags() | Qt.ItemIsEditable)
                    self.roster_table.setItem(row, col, item)
                item.setText(values[col])
            self._roster_set_category(row, values[4])
            source_item = self.roster_table.item(row, 5)
            if source_item is None:
                source_item = QTableWidgetItem("")
                source_item.setFlags(source_item.flags() | Qt.ItemIsEditable)
                self.roster_table.setItem(row, 5, source_item)
            source_item.setText(values[5])
        finally:
            self._roster_syncing = False

    def _roster_append_row(
        self,
        callsign: str,
        name: str,
        state: str,
        traffic: str = "",
        category: str = "TFC",
        source: str = "Local",
        *,
        overwrite_source: bool = False,
    ) -> int:
        cs = (callsign or "").strip().upper()
        if not cs:
            return -1
        existing = self._roster_find_row(cs)
        if existing >= 0:
            current_source_item = self.roster_table.item(existing, 5)
            current_source = (current_source_item.text() if current_source_item else "").strip()
            next_source = source.strip() if overwrite_source or not current_source else current_source
            self._roster_set_row(existing, cs, name, state, traffic, category, next_source)
            self._roster_sync_legacy_buffers()
            return existing
        row = self.roster_table.rowCount()
        self.roster_table.insertRow(row)
        for col in (0, 1, 2, 3, 5):
            item = QTableWidgetItem("")
            item.setFlags(item.flags() | Qt.ItemIsEditable)
            self.roster_table.setItem(row, col, item)
        self._roster_configure_category_editor(row)
        self._roster_set_row(row, cs, name, state, traffic, category, source)
        self._roster_sync_legacy_buffers()
        return row

    def _roster_row_widget(self, row: int) -> Optional[QComboBox]:
        widget = self.roster_table.cellWidget(row, 4)
        return widget if isinstance(widget, QComboBox) else None

    def _roster_configure_category_editor(self, row: int) -> None:
        combo = QComboBox(self.roster_table)
        categories = ["TFC", "QRU", "LATE"]
        combo.addItems(categories)
        combo.setCurrentText("TFC")
        combo.setMinimumContentsLength(5)
        combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)

        def _on_category_changed(value: str, row_index: int = row) -> None:
            if self._roster_syncing:
                return
            self._roster_sync_legacy_buffers()

        combo.currentTextChanged.connect(_on_category_changed)
        self.roster_table.setCellWidget(row, 4, combo)

    def _roster_rebuild_category_editors(self) -> None:
        for row in range(self.roster_table.rowCount()):
            if self.roster_table.cellWidget(row, 4) is None:
                self._roster_configure_category_editor(row)
            category = self.roster_table.item(row, 4).text().strip().upper() if self.roster_table.item(row, 4) else "TFC"
            self._roster_set_category(row, category or "TFC")

    def _roster_reset_from_text(self, text: str, category: str = "TFC") -> None:
        self._roster_syncing = True
        try:
            self.roster_table.setRowCount(0)
            for line in (text or "").splitlines():
                line = line.strip()
                if not line:
                    continue
                cs, name, state, extra = self._split_checkin_with_extra(line)
                if not cs and not name and not state:
                    continue
                self.roster_table.insertRow(self.roster_table.rowCount())
                row = self.roster_table.rowCount() - 1
                for col in (0, 1, 2, 3, 5):
                    item = QTableWidgetItem("")
                    item.setFlags(item.flags() | Qt.ItemIsEditable)
                    self.roster_table.setItem(row, col, item)
                self._roster_configure_category_editor(row)
                self._roster_set_row(row, cs, name, state, extra, category, "Local")
        finally:
            self._roster_syncing = False
        self._roster_sync_legacy_buffers(write_files=False)

    def _copy_roster_category(self, category: str) -> None:
        text = self._roster_table_text(category)
        if text:
            QApplication.clipboard().setText(text)

    def _copy_roster_seen_locally(self) -> None:
        text = self._roster_table_text()
        if text:
            QApplication.clipboard().setText(text)

    def _copy_roster_summary(self) -> None:
        text = self._roster_table_text()
        if text:
            QApplication.clipboard().setText(text)
            self._write_file(self._all_checkins_file_path(), text)
            QMessageBox.information(self, "Copied", "Check-ins copied to clipboard.")

    def _workspace_custom_bucket_id(self, mapping: Dict[str, object], index: int) -> str:
        scope = str(mapping.get("scope") or "").strip().upper() or "SHARED"
        function = str(mapping.get("function") or "").strip().upper() or "CUSTOM"
        macro_id = str(mapping.get("macro_id") or "").strip()
        source_file = str(mapping.get("source_file") or "").strip()
        custom_name = str(mapping.get("custom_name") or "").strip()
        identity = macro_id or source_file or custom_name or f"row_{index + 1}"
        return f"custom::{scope}::{function}::{identity}"

    def _workspace_custom_bucket_title(self, mapping: Dict[str, object], index: int) -> str:
        custom_name = str(mapping.get("custom_name") or "").strip()
        if custom_name:
            return custom_name
        macro_label = str(mapping.get("macro_label") or "").strip()
        if macro_label:
            return macro_label
        macro_id = str(mapping.get("macro_id") or "").strip()
        if macro_id:
            return macro_id
        return f"CUSTOM_{index + 1}"

    def _log_assisted_enabled(self) -> bool:
        return bool(self.log_assisted_enable_chk.isChecked())

    def _log_assisted_show_review(self) -> bool:
        return bool(self.log_assisted_review_chk.isChecked())

    def _log_assisted_include_tx(self) -> bool:
        return bool(self.log_assisted_tx_chk.isChecked())

    def _on_log_assisted_controls_changed(self, *_args) -> None:
        if self._log_assisted_loading:
            return
        self.settings.set(self.FLDIGI_LOG_ASSISTED_ENABLED_KEY, self._log_assisted_enabled())
        self.settings.set(self.FLDIGI_LOG_ASSISTED_SHOW_REVIEW_KEY, self._log_assisted_show_review())
        self.settings.set(self.FLDIGI_LOG_ASSISTED_INCLUDE_TX_KEY, self._log_assisted_include_tx())
        if self._net_in_progress and self._log_assisted_enabled():
            self._capture_log_assisted_session()
        elif not self._log_assisted_enabled():
            self._reset_log_assisted_session()
        self._update_log_assisted_status()
        self._apply_role_workspace(self.role_combo.currentText())

    def _load_log_assisted_state(self) -> None:
        self._log_assisted_loading = True
        try:
            self.log_assisted_enable_chk.setChecked(bool(self.settings.get(self.FLDIGI_LOG_ASSISTED_ENABLED_KEY, False)))
            self.log_assisted_review_chk.setChecked(bool(self.settings.get(self.FLDIGI_LOG_ASSISTED_SHOW_REVIEW_KEY, True)))
            self.log_assisted_tx_chk.setChecked(bool(self.settings.get(self.FLDIGI_LOG_ASSISTED_INCLUDE_TX_KEY, True)))
        finally:
            self._log_assisted_loading = False
        self._update_log_assisted_status()
        self._apply_role_workspace(self.role_combo.currentText())

    def _reset_log_assisted_session(self) -> None:
        self._clear_log_assisted_candidates_from_buckets()
        self._log_assisted_session_path = ""
        self._log_assisted_session_offset = 0
        self._log_assisted_session_start_utc = None
        self._log_assisted_session_tx_context = ""
        self._log_assisted_seen_normalized.clear()
        self._log_assisted_candidates_by_callsign.clear()
        self.review_card.set_text("")

    def _clear_log_assisted_candidates_from_buckets(self) -> None:
        if not self._log_assisted_candidates_by_callsign:
            return
        for callsign, candidate in list(self._log_assisted_candidates_by_callsign.items()):
            bucket_id = self._log_assisted_target_bucket(candidate)
            if bucket_id in {"tfc", "qru"}:
                self._roster_remove_callsign(callsign)
            elif bucket_id == "review":
                self._remove_bucket_line_by_callsign(bucket_id, callsign)

    def _capture_log_assisted_session(self) -> None:
        if not self._log_assisted_enabled():
            self._reset_log_assisted_session()
            return
        self._clear_log_assisted_candidates_from_buckets()
        path = resolve_fldigi_log_path(self.settings.get("fldigi_log_path", ""))
        self._log_assisted_session_path = str(path) if path is not None else ""
        self._log_assisted_session_offset = int(path.stat().st_size) if path is not None and path.exists() else 0
        self._log_assisted_session_start_utc = datetime.datetime.now(datetime.timezone.utc)
        self._log_assisted_session_tx_context = ""
        self._log_assisted_seen_normalized.clear()
        self._log_assisted_candidates_by_callsign.clear()
        self.review_card.set_text("")
        self._update_log_assisted_status()

    def _update_log_assisted_status(self, *, scanned: int = 0, imported: int = 0, reviewed: int = 0) -> None:
        if not self._log_assisted_enabled():
            self.log_assisted_status.setText("Log-assisted intake disabled.")
            self._refresh_setup_summary()
            return
        if not self._log_assisted_session_path:
            self.log_assisted_status.setText("Log-assisted intake is enabled, but no FLDigi log file is configured or resolved yet.")
            self._refresh_setup_summary()
            return
        parts = [f"Session log: {Path(self._log_assisted_session_path).name or self._log_assisted_session_path}"]
        if self._log_assisted_session_start_utc is not None:
            parts.append(self._log_assisted_session_start_utc.strftime("started %Y-%m-%d %H:%M:%S UTC"))
        if scanned or imported or reviewed:
            parts.append(f"scanned {scanned} lines, imported {imported}, review {reviewed}")
        self.log_assisted_status.setText("; ".join(parts))
        self._refresh_setup_summary()

    def _log_assisted_target_bucket(self, candidate) -> str:
        role = normalize_role(self.role_combo.currentText())
        if candidate.bucket == "REVIEW":
            return "review"
        if role == "JOINER":
            return "tfc"
        if candidate.bucket == "QRU":
            return "qru"
        return "tfc"

    def _log_assisted_candidate_line(self, candidate) -> str:
        parts = [candidate.callsign]
        if candidate.name:
            parts.append(candidate.name)
        if candidate.state:
            parts.append(candidate.state)
        if candidate.traffic:
            parts.append(candidate.traffic)
        line = " / ".join(parts)
        tx_context = str(getattr(candidate, "tx_context", "") or "").strip()
        if candidate.bucket == "REVIEW" and tx_context:
            line = f"{line} [ctx: {tx_context}]"
        return line

    def _log_assisted_candidate_rank(self, candidate) -> tuple[int, int, int]:
        confidence_rank = {"low": 1, "medium": 2, "high": 3}.get(str(candidate.confidence or "").lower(), 0)
        return candidate.completeness_score(), confidence_rank, int(candidate.timestamp_utc.timestamp()) if candidate.timestamp_utc else 0

    def _remove_bucket_line_by_callsign(self, bucket_id: str, callsign: str) -> None:
        card = self._workspace_bucket_cards.get(bucket_id)
        if card is None:
            return
        lines = []
        removed = False
        for raw_line in card.text().splitlines():
            cs, _, _ = self._parse_checkin_line(raw_line)
            if (cs or "").strip().upper() == callsign:
                removed = True
                continue
            lines.append(raw_line)
        if removed:
            card.set_text("\n".join(lines).strip())

    def _upsert_bucket_line(self, bucket_id: str, line: str, callsign: str) -> None:
        card = self._workspace_bucket_cards.get(bucket_id)
        if card is None or not line:
            return
        lines = []
        replaced = False
        for raw_line in card.text().splitlines():
            cs, _, _ = self._parse_checkin_line(raw_line)
            if (cs or "").strip().upper() == callsign:
                if not replaced:
                    lines.append(line)
                    replaced = True
                continue
            lines.append(raw_line)
        if not replaced:
            lines.append(line)
        card.set_text("\n".join([ln for ln in lines if ln]).strip())

    def _log_assisted_bucket_text(self, bucket_id: str) -> str:
        return self._workspace_bucket_text(bucket_id)

    def _render_log_assisted_candidate(self, candidate) -> str:
        return self._log_assisted_candidate_line(candidate)

    def _apply_log_assisted_candidate(self, candidate) -> None:
        callsign = (candidate.callsign or "").strip().upper()
        if not callsign:
            return
        current = self._log_assisted_candidates_by_callsign.get(callsign)
        current_rank = self._log_assisted_candidate_rank(current) if current is not None else (-1, -1, -1)
        new_rank = self._log_assisted_candidate_rank(candidate)
        if current is not None and new_rank <= current_rank:
            return

        old_bucket = self._log_assisted_target_bucket(current) if current is not None else ""
        new_bucket = self._log_assisted_target_bucket(candidate)
        if new_bucket == "review":
            if old_bucket in {"tfc", "qru"}:
                self._roster_remove_callsign(callsign)
            line = self._render_log_assisted_candidate(candidate)
            lines = [ln for ln in self.review_card.text().splitlines() if (self._parse_checkin_line(ln)[0] or "").strip().upper() != callsign]
            lines.append(line)
            self.review_card.set_text("\n".join(lines).strip())
            if self._log_assisted_show_review():
                self._set_compare_workspace_expanded(True)
                self.compare_workspace_tabs.setCurrentWidget(self.review_card)
        else:
            if old_bucket == "review":
                lines = [ln for ln in self.review_card.text().splitlines() if (self._parse_checkin_line(ln)[0] or "").strip().upper() != callsign]
                self.review_card.set_text("\n".join(lines).strip())
            category = "QRU" if new_bucket == "qru" else "TFC"
            self._roster_append_row(
                callsign,
                candidate.name or "",
                candidate.state or "",
                candidate.traffic or "",
                category,
                "Log",
            )
        self._log_assisted_candidates_by_callsign[callsign] = candidate

        if new_bucket == "review":
            self.review_card.setVisible(self._log_assisted_show_review())
        self._update_log_assisted_status(imported=sum(1 for c in self._log_assisted_candidates_by_callsign.values() if self._log_assisted_target_bucket(c) != "review"), reviewed=sum(1 for c in self._log_assisted_candidates_by_callsign.values() if self._log_assisted_target_bucket(c) == "review"))

    def _paste_into_review_card(self) -> None:
        from PySide6.QtWidgets import QApplication

        self.review_card.set_text(QApplication.clipboard().text())

    @staticmethod
    def _strip_inline_review_context(line: str) -> str:
        return re.sub(r"\s*\[ctx:\s*.*\]\s*$", "", str(line or "").strip(), flags=re.IGNORECASE)

    def _poll_log_assisted_intake(self) -> None:
        if not self._net_in_progress or not self._log_assisted_enabled():
            return
        path = resolve_fldigi_log_path(self.settings.get("fldigi_log_path", ""))
        if path is None:
            self._update_log_assisted_status()
            return
        path_text = str(path)
        if path_text != self._log_assisted_session_path:
            self._clear_log_assisted_candidates_from_buckets()
            self._log_assisted_session_path = path_text
            self._log_assisted_session_offset = int(path.stat().st_size) if path.exists() else 0
            self._log_assisted_session_tx_context = ""
            self._log_assisted_seen_normalized.clear()
            self._log_assisted_candidates_by_callsign.clear()
            self.review_card.set_text("")
            self._update_log_assisted_status()
            return
        candidates, new_offset, last_tx_context = scan_fldigi_log_file(
            path,
            start_offset=self._log_assisted_session_offset,
            session_start_utc=self._log_assisted_session_start_utc,
            last_tx_context=self._log_assisted_session_tx_context,
            include_tx_context=self._log_assisted_include_tx(),
            seen_normalized=self._log_assisted_seen_normalized,
        )
        self._log_assisted_session_offset = new_offset
        self._log_assisted_session_tx_context = last_tx_context
        if not candidates:
            self._update_log_assisted_status()
            return
        for candidate in candidates:
            self._apply_log_assisted_candidate(candidate)
        self._update_log_assisted_status(scanned=len(candidates), imported=sum(1 for c in candidates if self._log_assisted_target_bucket(c) != "review"), reviewed=sum(1 for c in candidates if self._log_assisted_target_bucket(c) == "review"))

    def _insert_left_bucket_widget(self, widget: WorkspaceBucketCard) -> None:
        if self._left_bucket_col.indexOf(widget) >= 0:
            return
        insert_at = max(self._left_bucket_col.count() - 1, 0)
        self._left_bucket_col.insertWidget(insert_at, widget)

    def _enabled_custom_bucket_mappings(self, role: Optional[str] = None) -> List[Dict[str, object]]:
        selected = self._selected_macro_profile_path()
        record = self._macro_profile_record(selected)
        mappings = record.get("mappings")
        if not isinstance(mappings, list):
            return []
        active_role = normalize_role(role or self.role_combo.currentText())
        if active_role == "JOINER":
            return []
        custom_mappings: List[Dict[str, object]] = []
        for mapping in mappings:
            if not self._macro_profile_mapping_is_complete(mapping):
                continue
            if str(mapping.get("function") or "").strip().upper() != "CUSTOM":
                continue
            mapping_role = str(mapping.get("scope") or "").strip().upper()
            if mapping_role not in {active_role, "SHARED"}:
                continue
            custom_mappings.append(mapping)
        return custom_mappings

    def _refresh_custom_bucket_cards(self, role: Optional[str] = None) -> None:
        active_role = normalize_role(role or self.role_combo.currentText())
        self._workspace_visible_bucket_ids.difference_update(self._custom_bucket_cards.keys())
        for bucket_id, card in self._custom_bucket_cards.items():
            card.setVisible(False)

        custom_mappings = self._enabled_custom_bucket_mappings(active_role)
        for index, mapping in enumerate(custom_mappings):
            bucket_id = self._workspace_custom_bucket_id(mapping, index)
            title = self._workspace_custom_bucket_title(mapping, index)
            source_file = str(mapping.get("source_file") or "").strip()
            card = self._custom_bucket_cards.get(bucket_id)
            if card is None:
                card = WorkspaceBucketCard(self.settings, title=title, allow_paste=True)
                self._custom_bucket_cards[bucket_id] = card
                self._workspace_bucket_cards[bucket_id] = card
                self._insert_left_bucket_widget(card)
            card.set_title(title)
            card.set_read_only(False)
            if source_file:
                loaded_text = self._read_file(source_file)
                card.set_text(loaded_text)
            else:
                card.set_text("")
            self._custom_bucket_sources[bucket_id] = source_file
            card.setVisible(True)
            self._workspace_visible_bucket_ids.add(bucket_id)

        for bucket_id in list(self._custom_bucket_cards.keys()):
            if bucket_id not in self._workspace_visible_bucket_ids:
                self._custom_bucket_cards[bucket_id].setVisible(False)

    def _on_role_changed(self, role: str) -> None:
        if self._workspace_role_loading:
            return
        self._apply_role_workspace(role)

    def _on_workspace_text_changed(self) -> None:
        self._update_bucket_card_states()

    def _on_roster_item_changed(self, item: QTableWidgetItem) -> None:
        if self._roster_syncing or self._roster_loading or item is None:
            return
        row = item.row()
        if row < 0:
            return
        if item.column() == 0:
            item.setText(item.text().strip().upper())
        elif item.column() == 2:
            item.setText(item.text().strip().upper())
        elif item.column() == 5:
            item.setText(item.text().strip())
        self._roster_sync_legacy_buffers()

    def _on_roster_model_data_changed(self, *_args) -> None:
        if self._roster_syncing or self._roster_loading:
            return
        self._roster_sync_legacy_buffers()

    def _on_roster_model_rows_changed(self, *_args) -> None:
        if self._roster_syncing or self._roster_loading:
            return
        self._roster_sync_legacy_buffers()

    def _known_add_button_role_map(self, role: Optional[str] = None) -> List[tuple[str, str, str]]:
        active_role = normalize_role(role or self.role_combo.currentText())
        if active_role == "JOINER":
            return [("seen_locally", "Add to Seen Locally", "success")]
        return [
            ("tfc", "Add to TFC", "success"),
            ("qru", "Add to QRU", "info"),
            ("late", "Add to LATE", "warning"),
        ]

    def _known_target_widget(self, target: str):
        return None

    def _refresh_known_action_band(self, role: Optional[str] = None) -> None:
        active_role = normalize_role(role or self.role_combo.currentText())
        visible_targets = {target for target, _, _ in self._known_add_button_role_map(active_role)}
        for target, button in self._known_add_buttons.items():
            button.setVisible(target in visible_targets)

        ordered_buttons = [self._known_add_buttons[target] for target, _, _ in self._known_add_button_role_map(active_role)]
        if ordered_buttons:
            self.setTabOrder(self.known_op_edit, ordered_buttons[0])
            for prev, nxt in zip(ordered_buttons, ordered_buttons[1:]):
                self.setTabOrder(prev, nxt)
        self._update_add_buttons_state()

    def _insert_known_into_bucket(self, target: str) -> None:
        line = self.known_op_edit.text().strip()
        if not line:
            return
        cs, name, state, extra = self._split_checkin_with_extra(line)
        if not cs and not name and not state:
            return
        lookup = self._known_operator_by_callsign.get(cs or "")
        if lookup:
            cs = lookup.get("callsign", cs)
            name = lookup.get("name", name)
            state = lookup.get("state", state)
        category = {"tfc": "TFC", "qru": "QRU", "late": "LATE", "seen_locally": "TFC"}.get(target, "TFC")
        self._roster_append_row(cs, name, state, extra, category)
        self.known_op_edit.clear()
        self.known_op_edit.setFocus()

    def _workspace_line_count(self, text: str) -> int:
        return sum(1 for line in (text or "").splitlines() if line.strip())

    def _workspace_unique_entries(self, text: str) -> List[str]:
        entries: List[str] = []
        seen = set()
        for line in (text or "").splitlines():
            cs, name, state = self._parse_checkin_line(line)
            normalized = self._format_entry(cs, name, state)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            entries.append(normalized)
        return entries

    def _reference_compare_label(self) -> str:
        return self.reference_card.title()

    def _review_entries_missing_from_roster(self) -> List[Dict[str, str]]:
        roster_calls = {row["callsign"] for row in self._roster_table_rows() if row["callsign"]}
        missing: List[Dict[str, str]] = []
        for entry in self._extract_unique_entries(self.review_card.text()):
            if entry["callsign"] not in roster_calls:
                missing.append(entry)
        return missing

    def _checkin_line_details(self, line: str) -> tuple[str, str, str, str]:
        raw_line = self._strip_inline_review_context(line)
        if not raw_line:
            return "", "", "", ""
        if "/" in raw_line and not raw_line.startswith("#"):
            parts = [p.strip() for p in raw_line.split("/") if p.strip()]
            if len(parts) >= 3:
                cs = parts[0].split()[0].upper()
                name = parts[1]
                state_tokens = parts[2].split()
                state = state_tokens[0].upper() if state_tokens else ""
                extra_parts = []
                trailing = " ".join(state_tokens[1:]).strip()
                if trailing:
                    extra_parts.append(trailing)
                if len(parts) > 3:
                    extra_parts.extend(parts[3:])
                extra = " ".join(part for part in extra_parts if part).strip()
                return cs, name, state, extra
        return self._split_checkin_with_extra(raw_line)

    def _traffic_category_from_text(self, text: str) -> tuple[str, str]:
        raw = str(text or "").strip()
        upper = raw.upper()
        if not raw:
            return "", "TFC"
        if re.search(r"\b(QRU|NO\s+TFC|NO\s+TRAFFIC)\b", upper):
            return "No TFC", "QRU"
        traffic_match = re.search(r"\b(\d+\s*)?(RR|PP|QST)\b", upper)
        if traffic_match:
            count = re.sub(r"\s+", "", traffic_match.group(1) or "")
            suffix = traffic_match.group(2)
            return (f"{count}{suffix}" if count else suffix), "TFC"
        return raw, "TFC"

    def _normalize_merge_entry_fields(self, entry: Dict[str, str]) -> tuple[str, str, str]:
        state = str(entry.get("state", "") or "").strip().upper()
        traffic = str(entry.get("traffic", "") or "").strip()
        category = str(entry.get("category", "") or "TFC").strip().upper() or "TFC"

        state_tokens = state.split()
        merged_extra_parts = []
        if len(state_tokens) > 1:
            state = state_tokens[0]
            merged_extra_parts.extend(state_tokens[1:])
        if traffic:
            merged_extra_parts.append(traffic)
        merged_extra = " ".join(part for part in merged_extra_parts if part).strip()
        if merged_extra:
            parsed_traffic, parsed_category = self._traffic_category_from_text(merged_extra)
            traffic = parsed_traffic
            category = parsed_category or category
        return state, traffic, category

    def _workspace_compare_payload(self, source_bucket: str, target_bucket: str) -> tuple[str, str]:
        source_text = self._workspace_bucket_text(source_bucket)
        target_text = self._workspace_bucket_text(target_bucket)
        local_entries = self._extract_unique_entries(source_text)
        compare_entries = self._extract_unique_entries(target_text)
        local_calls = {entry["callsign"] for entry in local_entries}
        mergeable_entries = [
            self._format_entry(entry["callsign"], entry["name"], entry["state"])
            for entry in compare_entries
            if entry["callsign"] not in local_calls
        ]
        compare_label = self._reference_compare_label() if target_bucket in {"reference", "ncs_reference", "ancs_reference"} else target_bucket
        lines = [
            f"Local Roster: {len(local_entries)}",
            f"{compare_label}: {len(compare_entries)}",
            "",
            f"Entries in {compare_label} not in Local Roster:",
            "\n".join(mergeable_entries) if mergeable_entries else "(none)",
        ]
        return "\n".join(lines), "\n".join(mergeable_entries)

    def _run_inline_compare(self) -> None:
        defaults = self._workspace_compare_defaults()
        source_bucket = defaults.get("source_bucket_id", "tfc")
        target_bucket = defaults.get("target_bucket_id", "reference")
        result_text, missing_text = self._workspace_compare_payload(source_bucket, target_bucket)
        self.compare_results_card.set_text(result_text)
        self.compare_results_card.set_count(len(self._reference_entries_missing_from_roster()))
        self._compare_missing_text = missing_text
        self._compare_reference_missing_entries = self._reference_entries_missing_from_roster()
        self.compare_workspace_tabs.setCurrentWidget(self.compare_results_card)

    def _copy_compare_results(self) -> None:
        if not getattr(self, "_compare_missing_text", ""):
            return
        from PySide6.QtWidgets import QApplication

        QApplication.clipboard().setText(self._compare_missing_text)

    def _import_reference_missing(self) -> None:
        missing_entries = self._reference_entries_missing_from_roster()
        if not missing_entries:
            QMessageBox.information(self, "Reference Import", "No reference entries are missing from the local roster.")
            return
        source_label = self._reference_source_label()
        added = 0
        for entry in missing_entries:
            state, traffic, category = self._normalize_merge_entry_fields(entry)
            self._roster_append_row(
                entry["callsign"],
                entry["name"],
                state,
                traffic,
                category,
                source_label,
            )
            added += 1
        self._compare_reference_missing_entries = self._reference_entries_missing_from_roster()
        self._run_inline_compare()
        QMessageBox.information(self, "Reference Import", f"Added {added} reference entr{'y' if added == 1 else 'ies'} to the local roster.")

    def _merge_compare_missing(self) -> None:
        missing_entries = self._reference_entries_missing_from_roster()
        if not missing_entries:
            QMessageBox.information(self, "Merge Missing", f"No entries from {self._reference_compare_label()} are missing from the local roster.")
            return
        source_label = self._reference_source_label()
        added = 0
        for entry in missing_entries:
            self._roster_append_row(
                entry["callsign"],
                entry["name"],
                entry["state"],
                entry.get("traffic", "") or "",
                entry.get("category", "TFC") or "TFC",
                source_label,
            )
            added += 1
        self._run_inline_compare()
        QMessageBox.information(self, "Merge Missing", f"Added {added} entr{'y' if added == 1 else 'ies'} from {source_label} to the local roster.")

    def _merge_review_candidates(self) -> None:
        missing_entries = self._review_entries_missing_from_roster()
        if not missing_entries:
            QMessageBox.information(self, "Merge Reviewed", "No reviewed entries are missing from the local roster.")
            return
        added_calls = {entry["callsign"] for entry in missing_entries}
        added = 0
        for entry in missing_entries:
            state, traffic, category = self._normalize_merge_entry_fields(entry)
            self._roster_append_row(
                entry["callsign"],
                entry["name"],
                state,
                traffic,
                category,
                "Review",
            )
            added += 1
        remaining_lines = []
        for raw_line in self.review_card.text().splitlines():
            callsign, _name, _state = self._parse_checkin_line(raw_line)
            if (callsign or "").strip().upper() in added_calls:
                continue
            remaining_lines.append(raw_line)
        self.review_card.set_text("\n".join(remaining_lines).strip())
        self._run_inline_compare()
        QMessageBox.information(self, "Merge Reviewed", f"Added {added} reviewed entr{'y' if added == 1 else 'ies'} to the local roster.")

    def _apply_role_workspace(self, role: str) -> None:
        normalized = normalize_role(role)
        preset = get_role_workspace_preset(normalized)
        self._workspace_role_loading = True
        try:
            self.roster_frame.setVisible(True)
            self.roster_table.setColumnHidden(4, normalized == "JOINER")
            self.roster_table.setColumnHidden(5, False)
            if normalized == "JOINER":
                self.copy_tfc_btn.setVisible(False)
                self.copy_qru_btn.setVisible(False)
                self.copy_late_btn.setVisible(False)
                self.copy_seen_locally_btn.setVisible(True)
            else:
                self.copy_tfc_btn.setVisible(True)
                self.copy_qru_btn.setVisible(True)
                self.copy_late_btn.setVisible(True)
                self.copy_seen_locally_btn.setVisible(False)
            self.tfc_card.set_title("Seen Locally" if normalized == "JOINER" else f"{normalized} / TFC")
            self.qru_card.set_title(f"{normalized} / QRU")
            self.late_card.set_title(f"{normalized} / LATE")
            self.reference_card.set_title("ANCS List" if normalized == "NCS" else "NCS List")

            visible_ids = set(preset.visible_bucket_ids())
            # Local category panels are no longer operator-facing UI. Keep them
            # hidden even though their text buffers are still used internally.
            self.tfc_card.setVisible(False)
            self.qru_card.setVisible(False)
            self.late_card.setVisible(False)
            self.reference_card.setVisible("ncs_reference" in visible_ids or "ancs_reference" in visible_ids)
            self.compare_results_card.setVisible(True)

            self.reference_card.set_read_only(False)
            self.reference_card.set_placeholder("Paste or edit the ANCS list here.")
            if normalized != "NCS":
                self.reference_card.set_placeholder("Paste or edit the NCS reference list here.")
            self.compare_results_card.set_read_only(True)
            self.compare_results_card.set_placeholder("Run Compare to list entries that can be merged into the local roster.")
            self.compare_results_card.set_count(len(self._reference_entries_missing_from_roster()))
            review_visible = self._log_assisted_enabled() and self._log_assisted_show_review()
            self.review_card.setVisible(review_visible)
            self.review_card.set_read_only(False)
            self.review_card.set_placeholder("Review lower-confidence log-assisted candidates here.")
            self._workspace_bucket_defaults = {
                "source_bucket_id": "roster",
                "target_bucket_id": preset.compare_target_bucket_id,
            }
            self._workspace_visible_bucket_ids = set(visible_ids)
            self._workspace_visible_bucket_ids.add("roster")
            self._workspace_visible_bucket_ids.add("compare_results")
            if review_visible:
                self._workspace_visible_bucket_ids.add("review")
            self._refresh_custom_bucket_cards(normalized)
            self._refresh_known_action_band(normalized)
            self._sync_compare_workspace_tabs()
            self._update_bucket_card_states()
        finally:
            self._workspace_role_loading = False

    def _paste_into_reference_card(self) -> None:
        from PySide6.QtWidgets import QApplication

        self.reference_text.setPlainText(QApplication.clipboard().text())

    def _update_bucket_card_states(self) -> None:
        theme = resolve_theme(self.settings)
        roster_rows = self._roster_table_rows()
        self.roster_total_label.setText(f"Total Check-ins: {len(self._roster_unique_callsigns())}")
        self.roster_tfc_label.setText(f"TFC: {sum(1 for row in roster_rows if row['category'] == 'TFC')}")
        self.roster_qru_label.setText(f"QRU: {sum(1 for row in roster_rows if row['category'] == 'QRU')}")
        self.roster_late_label.setText(f"LATE: {sum(1 for row in roster_rows if row['category'] == 'LATE')}")
        self.tfc_card.set_count(self._workspace_line_count(self.tfc_card.text()))
        self.qru_card.set_count(self._workspace_line_count(self.qru_card.text()))
        self.late_card.set_count(self._workspace_line_count(self.late_card.text()))
        self.reference_card.set_count(self._workspace_line_count(self.reference_card.text()))
        self.compare_results_card.set_count(len(self._reference_entries_missing_from_roster()))
        self.review_card.set_count(self._workspace_line_count(self.review_card.text()))
        for bucket_id, card in self._custom_bucket_cards.items():
            card.set_count(self._workspace_line_count(card.text()))
        total = len(self._roster_unique_callsigns())
        self.total_checkins_label.setText(f"Total Check-ins: {total}")
        bucket_states = [
            (card, bool(card.text().strip()), "eligible_success" if bucket_id in {"tfc", "late"} else "eligible_info")
            for bucket_id, card in self._workspace_bucket_cards.items()
            if card.isVisible()
        ]
        for card, has_text, role in bucket_states:
            if hasattr(card, "copy_btn") and card.copy_btn is not None:
                card.copy_btn.setStyleSheet(button_style(role if has_text else "muted", theme))

    def _set_net_button_styles(self, active: bool):
        """
        Update button highlight when a net is running.
        """
        theme = resolve_theme(self.settings)
        if active:
            self.start_btn.setStyleSheet(button_style("muted", theme))
            self.save_btn.setStyleSheet(button_style("eligible_success", theme))
            self.merge_btn.setStyleSheet(button_style("eligible_info", theme))
            self.end_btn.setStyleSheet(button_style("eligible_danger", theme))
            self.ad_hoc_btn.setStyleSheet(button_style("muted", theme))
            self.start_btn.setEnabled(False)
            self.ad_hoc_btn.setEnabled(False)
        else:
            self.start_btn.setStyleSheet(button_style("eligible_success", theme))
            self.save_btn.setStyleSheet(button_style("muted", theme))
            self.merge_btn.setStyleSheet(button_style("muted", theme))
            self.end_btn.setStyleSheet(button_style("muted", theme))
            self.ad_hoc_btn.setStyleSheet(button_style("eligible_info", theme))
            self.start_btn.setEnabled(True)
            self.ad_hoc_btn.setEnabled(True)

    def _refresh_operator_history_views(self) -> None:
        try:
            win = self.window()
            if win and hasattr(win, "refresh_operator_history_views"):
                win.refresh_operator_history_views()
        except Exception:
            pass

    def on_settings_saved(self):
        """
        Refresh QSY options when settings are saved.
        """
        self._maybe_reload_operating_groups()
        self._apply_theme()
        self._refresh_macro_profile_choices()
        self._update_log_assisted_status()

    def show_loading_toast(self) -> None:
        # NCS tabs do not use a loading banner/toast.
        return

    def on_tab_activated(self) -> None:
        with perf_span("digi_ncs.on_tab_activated", settings=self.settings, min_ms=5.0):
            self._update_clock_labels()
            self._update_suspend_state()
            self._update_next_change_display()
            self._schedule_activation_secondary_refresh()

    def _schedule_activation_secondary_refresh(self) -> None:
        if self._activation_secondary_refresh_pending:
            return
        self._activation_secondary_refresh_pending = True
        QTimer.singleShot(120, self._run_activation_secondary_refresh)

    def _run_activation_secondary_refresh(self) -> None:
        if self._activation_secondary_refresh_inflight:
            self._activation_secondary_refresh_pending = False
            self._schedule_activation_secondary_refresh()
            return
        self._activation_secondary_refresh_pending = False
        self._activation_secondary_refresh_inflight = True
        try:
            self._maybe_reload_operating_groups()
            self._refresh_macro_profile_choices()
            self._poll_log_assisted_intake()
        finally:
            self._activation_secondary_refresh_inflight = False

    def set_tab_active(self, active: bool) -> None:
        self._active = bool(active)
        if self._active:
            if self._clock_timer and not self._clock_timer.isActive():
                self._clock_timer.start(1000)
            QTimer.singleShot(0, self.on_tab_activated)
            return
        if self._clock_timer and self._clock_timer.isActive() and not self._net_in_progress:
            self._clock_timer.stop()

    # ---------------- TIMERS & CLOCKS ---------------- #

    def _setup_timers(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._on_timer_tick)
        self._update_clock_labels()
        self._update_suspend_state()
        self._update_next_change_display()
        self._poll_log_assisted_intake()

    def _maybe_reload_operating_groups(self):
        try:
            if hasattr(self.settings, "reload"):
                self.settings.reload()
        except Exception:
            pass
        og = self._load_operating_groups()
        sig = self._snapshot_operating_groups(og)
        if sig == self._opgroups_sig:
            return
        self._opgroups_sig = sig
        self._refresh_qsy_options(og)

    def _load_operating_groups(self) -> List[Dict]:
        return qsy_load_operating_groups(self.settings)

    def _snapshot_operating_groups(self, og_list: List[Dict]) -> str:
        return qsy_snapshot_operating_groups(og_list)

    def _current_freq_mhz(self) -> Optional[float]:
        try:
            cur = self._current_scheduler_freq()
            if cur:
                return float(cur)
        except Exception:
            pass
        meta = self._selected_qsy_meta()
        try:
            if meta and meta.get("freq") is not None:
                return float(meta.get("freq"))
        except Exception:
            pass
        return None

    def _operating_group_for_freq(self, freq_mhz: Optional[float]) -> str:
        if not freq_mhz:
            return ""
        ops = self._load_operating_groups()
        for g in ops:
            try:
                fval = float(g.get("frequency", 0))
            except Exception:
                continue
            if abs(fval - float(freq_mhz)) <= 0.0005:
                name = (g.get("group") or g.get("group_name") or "").strip()
                if name:
                    return name.upper()
        return ""

    def _lookup_operator_groups(self, callsign: str) -> List[str]:
        cs = (callsign or "").strip().upper()
        if not cs:
            return []
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return []
        if not db_path.exists():
            return []
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT group1, group2, group3, groups_json FROM operator_checkins WHERE callsign=?", (cs,))
            row = cur.fetchone()
            conn.close()
        except Exception:
            return []
        if not row:
            return []
        g1, g2, g3, gj = row
        groups: List[str] = []
        for g in (g1, g2, g3):
            val = (g or "").strip().upper()
            if val and val not in groups:
                groups.append(val)
        if gj:
            try:
                parsed = json.loads(gj)
                if isinstance(parsed, list):
                    for g in parsed:
                        val = (str(g) or "").strip().upper()
                        if val and val not in groups:
                            groups.append(val)
            except Exception:
                pass
        return groups

    def _refresh_qsy_options(self, og_list: Optional[List[Dict]] = None):
        """
        Build a unique frequency list from Operating Groups (auto-tune wins on duplicates).
        """
        ops = og_list if og_list is not None else self._load_operating_groups()
        self._opgroups_sig = self._snapshot_operating_groups(ops)
        self._qsy_options = build_qsy_options(ops)
        refresh_qsy_combo(self.qsy_combo, self._qsy_options)
        refresh_hold_duration_combo(self.hold_duration_combo, self.settings)
        self._update_qsy_button_enabled()

    def _selected_qsy_meta(self) -> Optional[Dict]:
        return selected_qsy_meta(self.qsy_combo)

    def _selected_hold_minutes(self) -> int:
        return selected_hold_duration(self.hold_duration_combo, self.settings)

    def _on_hold_duration_changed(self) -> None:
        mins = self._selected_hold_minutes()
        set_hold_duration_default(self.settings, mins)
        notify_hold_duration_default_changed(self.window())
        self._update_suspend_state()

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

    def _on_timer_tick(self):
        self._update_clock_labels()
        self._update_next_change_display()
        self._poll_log_assisted_intake()

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
        """
        UTC from system, local derived from SettingsManager timezone (via get_timezone),
        with a short UI label like ET/CT/MT/PT.
        """
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a")
        self.utc_label.setText(now_utc.strftime(f"<b>UTC ({utc_day}):</b> %y%m%d %H:%M:%S Z"))

        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        now_local = now_utc.astimezone(tz)
        fallback = now_local.tzname() or tz_name
        abbr = self._ui_tz_abbr(tz_name, fallback)

        local_day = now_local.strftime("%a")
        self.local_label.setText(
            now_local.strftime(f"<b>Local ({local_day}):</b> %y%m%d %H:%M:%S {abbr}")
        )

    # --------- Next frequency change display / countdown --------- #

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

    def _update_suspend_state(self, snapshot: Optional[Dict[str, object]] = None):
        try:
            if not self.hold_duration_combo.view().isVisible() and not self.hold_duration_combo.hasFocus():
                refresh_hold_duration_combo(self.hold_duration_combo, self.settings)
        except Exception:
            pass
        enabled = self._scheduler_enabled()
        self.suspend_btn.setEnabled(enabled)
        if not enabled:
            self._set_suspend_button(active=False)
            self._update_qsy_button_enabled()
            return

        if not isinstance(snapshot, dict):
            snapshot = suspend_snapshot(self.settings)
        if not snapshot.get("active"):
            if snapshot.get("until"):
                resume_schedule_hold(self.window(), self.settings)
            self._set_suspend_button(active=False)
            self._update_qsy_button_enabled()
            return

        self._set_suspend_button(active=True, remaining_sec=snapshot.get("remaining_sec"))
        self._update_qsy_button_enabled()

    def on_hold_state_changed(self, snapshot: Optional[Dict[str, object]] = None) -> None:
        self._update_suspend_state(snapshot=snapshot)

    def _set_suspend_button(self, active: bool, remaining_sec: Optional[float] = None):
        theme = resolve_theme(self.settings)
        if active:
            self.suspend_btn.setText(active_hold_button_text(remaining_sec))
            self.suspend_btn.setToolTip(active_hold_status_text(remaining_sec))
            self.suspend_btn.setStyleSheet(button_style(active_hold_button_role(remaining_sec), theme))
        else:
            mins = self._selected_hold_minutes()
            self.suspend_btn.setText("QSY + Hold")
            self.suspend_btn.setToolTip(f"QSY now and pause schedule control for {mins} minutes.")
            self.suspend_btn.setStyleSheet(button_style("warning", theme))
        self._update_qsy_button_enabled()

    def _apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.start_btn.setStyleSheet(button_style("success", theme))
        self.end_btn.setStyleSheet(button_style("danger", theme))
        self.merge_btn.setStyleSheet(button_style("info", theme))
        self._start_btn_default_style = self.start_btn.styleSheet()
        self._save_btn_default_style = button_style("success", theme)
        self.save_btn.setStyleSheet(self._save_btn_default_style)
        self.suspend_btn.setStyleSheet(button_style("warning", theme))
        self._set_net_button_styles(self._net_in_progress)
        self._update_copy_buttons_state()
        self._apply_known_op_styles(theme)
        self._update_add_buttons_state()
        self._set_setup_details_expanded(self._setup_details_expanded)

    def apply_theme(self) -> None:
        self._apply_theme()

    def _on_suspend_clicked(self):
        if self._suspend_active():
            # Resume immediately
            resume_schedule_hold(self.window(), self.settings)
            self._set_suspend_button(active=False)
            QMessageBox.information(self, "Scheduling", "Scheduling resumed.")
        else:
            meta = self._selected_qsy_meta()
            if not meta:
                QMessageBox.warning(self, "QSY", "Select a frequency to QSY to.")
                return
            mins = perform_qsy_with_hold(self.window(), self.settings, meta, self._selected_hold_minutes())
            if mins <= 0:
                return
            snapshot = suspend_snapshot(self.settings)
            self._set_suspend_button(active=True, remaining_sec=snapshot.get("remaining_sec"))
            QMessageBox.information(
                self,
                "QSY Applied",
                f"Frequency changed and scheduling paused for {mins} minutes.",
            )

    def _compute_next_change_utc(self) -> Optional[datetime.datetime]:
        """
        Ask scheduler_engine for the next scheduled frequency change time (UTC).
        """
        try:
            from freqinout.core.scheduler_engine import compute_next_change_time as cnext
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            hf_active, net_active = self._active_schedule_entries(now_utc)
            dt = cnext(now_utc, hf_active, net_active)
        except Exception as e:
            log.error("FldigiNetControl: compute_next_change_time failed: %s", e)
            return None

        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt

    def _active_schedule_entries(
        self, now_utc: Optional[datetime.datetime] = None
    ) -> tuple[Optional[Dict], Optional[Dict]]:
        """
        Return (hf_active, net_active) entries for the current UTC time.
        Prefers the newer hf_schedule key but falls back to daily_schedule.
        """
        if now_utc is None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)

        data = self.settings.all()
        hf_sched = data.get("hf_schedule") or data.get("daily_schedule") or []
        net_sched = data.get("net_schedule") or []
        if not isinstance(hf_sched, list):
            hf_sched = []
        if not isinstance(net_sched, list):
            net_sched = []

        weekday_name = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][now_utc.weekday()]
        prev_day = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][(now_utc.weekday() - 1) % 7]
        now_min = now_utc.hour * 60 + now_utc.minute

        def parse_minutes(text: str) -> Optional[int]:
            txt = (text or "").strip()
            if not txt:
                return None
            try:
                h, m = [int(x) for x in txt.split(":")]
                if 0 <= h <= 23 and 0 <= m <= 59:
                    return h * 60 + m
            except Exception:
                return None
            return None

        hf_active = None
        hf_best_start = -1
        for row in hf_sched:
            try:
                day = (row.get("day_utc") or "ALL").strip()
                smin = parse_minutes(row.get("start_utc", ""))
                emin = parse_minutes(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue
                overnight = smin > emin
                active = False
                if day.upper() == "ALL" or day == weekday_name:
                    active = smin <= now_min < emin if not overnight else (now_min >= smin or now_min < emin)
                elif overnight and day == prev_day:
                    active = now_min < emin
                if active and smin > hf_best_start:
                    hf_best_start = smin
                    hf_active = row
            except Exception:
                continue

        net_active = None
        net_best_start = -1
        for row in net_sched:
            try:
                day = (row.get("day_utc") or "").strip()
                smin = parse_minutes(row.get("start_utc", ""))
                emin = parse_minutes(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue
                early = int(row.get("early_checkin", 0) or 0)
                window_start = max(0, smin - early)
                overnight = smin > emin
                active = False
                if day == weekday_name:
                    active = window_start <= now_min < emin if not overnight else (now_min >= window_start or now_min < emin)
                elif overnight and day == prev_day:
                    active = now_min < emin
                if active and smin > net_best_start:
                    net_best_start = smin
                    net_active = row
            except Exception:
                continue

        return hf_active, net_active

    def _current_schedule_entry(
        self, now_utc: Optional[datetime.datetime] = None
    ) -> tuple[str, Optional[Dict]]:
        """
        Return (source, entry) where source is NET/HF/NONE.
        """
        hf_active, net_active = self._active_schedule_entries(now_utc)
        if net_active:
            return "NET", net_active
        if hf_active:
            return "HF", hf_active
        return "NONE", None

    def _format_current_band(self, now_utc: Optional[datetime.datetime] = None) -> str:
        source, entry = self._current_schedule_entry(now_utc)
        if not entry:
            return "Current Band: (none active)"
        band = (entry.get("band") or "").strip()
        freq = (entry.get("frequency") or "").strip()
        details = " ".join([p for p in (band, freq) if p]).strip()
        return f"Current Band: {details or '(unknown)'}"

    def _next_net_occurrence(self, row: Dict, now: datetime.datetime) -> Optional[Dict]:
        """
        Compute the next occurrence window for a net row, returning
        start/end/window_start along with active flag.
        """
        def day_to_idx(day_name: str) -> Optional[int]:
            names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            try:
                return names.index(day_name)
            except ValueError:
                return None

        def parse_time(txt: str) -> Optional[datetime.time]:
            try:
                h, m = [int(x) for x in (txt or "").split(":")]
                if 0 <= h <= 23 and 0 <= m <= 59:
                    return datetime.time(hour=h, minute=m, tzinfo=datetime.timezone.utc)
            except Exception:
                return None
            return None

        def parse_month_weeks(txt: str) -> List[int]:
            weeks: List[int] = []
            for token in (txt or "").split(","):
                token = token.strip()
                if not token:
                    continue
                try:
                    val = int(token)
                except Exception:
                    continue
                if 1 <= val <= 5:
                    weeks.append(val)
            return sorted(set(weeks))

        def nth_weekday_date(year: int, month: int, weekday_idx: int, nth: int) -> Optional[datetime.date]:
            try:
                first = datetime.date(year, month, 1)
            except Exception:
                return None
            offset = (weekday_idx - first.weekday()) % 7
            day = 1 + offset + (nth - 1) * 7
            try:
                return datetime.date(year, month, day)
            except Exception:
                return None

        day_name = (row.get("day_utc") or "").strip()
        day_idx = day_to_idx(day_name) if day_name else None
        if day_idx is None:
            return None
        start_t = parse_time(row.get("start_utc", ""))
        end_t = parse_time(row.get("end_utc", ""))
        if start_t is None or end_t is None:
            return None

        recurrence = (row.get("recurrence") or "Weekly").strip()
        if recurrence == "Monthly":
            recurrence = "Periodic"
        interval_weeks = 2 if recurrence == "Bi-Weekly" else 1
        offset_weeks = int(row.get("biweekly_offset_weeks", 0) or 0)
        early = int(row.get("early_checkin", 0) or 0)

        if recurrence == "Daily":
            start_dt = datetime.datetime.combine(now.date(), start_t)
            end_dt = datetime.datetime.combine(now.date(), end_t)
            if end_dt <= start_dt:
                end_dt += datetime.timedelta(days=1)
            if end_dt < now:
                start_dt += datetime.timedelta(days=1)
                end_dt += datetime.timedelta(days=1)
            window_start = start_dt - datetime.timedelta(minutes=early)
            active = window_start <= now < end_dt
            return {
                "start_dt": start_dt,
                "end_dt": end_dt,
                "window_start": window_start,
                "active": active,
                "row": row,
            }

        if recurrence == "Periodic":
            weeks = parse_month_weeks(row.get("month_weeks", "")) or [1]
            for month_offset in range(0, 13):
                year = (now.year + (now.month - 1 + month_offset) // 12)
                month = ((now.month - 1 + month_offset) % 12) + 1
                for nth in weeks:
                    occ_date = nth_weekday_date(year, month, day_idx, nth)
                    if not occ_date:
                        continue
                    start_dt = datetime.datetime.combine(occ_date, start_t)
                    end_dt = datetime.datetime.combine(occ_date, end_t)
                    if end_dt <= start_dt:
                        end_dt += datetime.timedelta(days=1)
                    window_start = start_dt - datetime.timedelta(minutes=early)
                    if end_dt < now:
                        continue
                    active = window_start <= now < end_dt
                    return {
                        "start_dt": start_dt,
                        "end_dt": end_dt,
                        "window_start": window_start,
                        "active": active,
                        "row": row,
                    }
            return None

        today_idx = now.weekday()
        days_ahead = (day_idx - today_idx) % 7
        start_date = now.date() + datetime.timedelta(days=days_ahead)
        start_dt = datetime.datetime.combine(start_date, start_t)
        end_dt = datetime.datetime.combine(start_date, end_t)
        if end_dt <= start_dt:
            end_dt += datetime.timedelta(days=1)

        if interval_weeks == 2:
            start_dt += datetime.timedelta(weeks=offset_weeks)
            end_dt += datetime.timedelta(weeks=offset_weeks)

        interval = datetime.timedelta(weeks=interval_weeks)
        for _ in range(3):  # safety loop to advance if end already passed
            window_start = start_dt - datetime.timedelta(minutes=early)
            if end_dt >= now:
                active = window_start <= now < end_dt
                return {
                    "start_dt": start_dt,
                    "end_dt": end_dt,
                    "window_start": window_start,
                    "active": active,
                    "row": row,
                }
            start_dt += interval
            end_dt += interval

        return None

    def _format_next_net_summary(self) -> str:
        occ = self._next_net_from_schedule()
        if not occ:
            return "(none scheduled)"
        row = occ["row"]
        start_dt = occ.get("start_dt")
        net_name = (row.get("net_name") or "").strip()
        band = (row.get("band") or "").strip()
        freq = (row.get("frequency") or "").strip()
        parts = [net_name, band, freq]
        if isinstance(start_dt, datetime.datetime):
            parts.append(start_dt.strftime("%a %H:%M UTC"))
        summary = " - ".join([p for p in parts if p])
        return summary or "(none scheduled)"

    def _update_next_change_display(self):
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        base_style = "QLabel { border: 1px solid #888888; padding: 4px; border-radius: 3px; }"

        # If schedule is suspended, show resume info and skip change handling
        if self._suspend_active():
            su = self._get_suspend_until()
            resume_str = su.strftime("%H:%M UTC") if su else ""
            suspended_text = f"Next Scheduled Net: (suspended until {resume_str})"
            suspended_text = f"{suspended_text} : {self._format_current_band(now_utc)}"
            self.next_change_label.setText(suspended_text)
            self.next_change_label.setStyleSheet(
                "QLabel { border: 1px solid #888888; padding: 4px; border-radius: 3px; background-color: #E3F2FD; }"
            )
            return

        # Refresh next_change_utc if we don't have one or it's in the past
        if self._next_change_utc is None or self._next_change_utc <= now_utc:
            self._next_change_utc = self._compute_next_change_utc()
            self._auto_end_done = False  # reset flag when we get a new target

        delta_sec = None
        if self._next_change_utc:
            delta_sec = (self._next_change_utc - now_utc).total_seconds()

        next_net_text = self._format_next_net_summary()
        current_band_text = self._format_current_band(now_utc)
        display_text = f"Next Scheduled Net: {next_net_text} : {current_band_text}"
        self.next_change_label.setText(display_text)
        self.next_change_label.setStyleSheet(base_style)

        # Auto end net exactly at change time if not paused
        if (
            delta_sec is not None
            and delta_sec <= 0
            and self._net_in_progress
            and not self._suspend_active()
            and not self._auto_end_done
        ):
            self._auto_end_done = True
            log.info("FldigiNetControl: auto-ending net for scheduled frequency change.")
            self._end_net()
            # Next scheduler tick will pick up any new next_change_utc

    def _effective_fldigi_log_root(self) -> Optional[Path]:
        raw_value = str(self.settings.get("fldigi_log_path", "") or "").strip()
        if not raw_value:
            return None
        path = Path(raw_value).expanduser()
        if path.exists():
            return path if path.is_dir() else path.parent
        if path.suffix.lower() == ".log" or "log" in path.name.lower():
            return path.parent
        return path

    def _macro_profiles_dir(self) -> Optional[Path]:
        root = self._effective_fldigi_log_root()
        if root is None:
            return None
        return root / "macros"

    def _discover_macro_profile_paths(self) -> list[str]:
        macros_dir = self._macro_profiles_dir()
        if macros_dir is None or not macros_dir.exists() or not macros_dir.is_dir():
            return []
        paths = [self._normalize_macro_profile_path(str(path)) for path in macros_dir.glob("*.mdf") if path.is_file()]
        return sorted({path for path in paths if path}, key=lambda item: Path(item).name.lower())

    def _refresh_macro_profile_choices(self, *_args, select_path: Optional[str] = None) -> None:
        selected = self._normalize_macro_profile_path(select_path or self._selected_macro_profile_path())
        discovered = self._discover_macro_profile_paths()
        current_items = ["Select macro set"]
        current_data = [""]
        for path in discovered:
            current_items.append(Path(path).stem or Path(path).name)
            current_data.append(path)
        if selected and selected not in discovered:
            current_items.append(f"{Path(selected).stem or Path(selected).name} (custom)")
            current_data.append(selected)
        self._macro_profile_combo_loading = True
        try:
            self.macro_profile_combo.clear()
            for label, data in zip(current_items, current_data):
                self.macro_profile_combo.addItem(label, data)
            index = self.macro_profile_combo.findData(selected)
            if index < 0:
                index = 0
            self.macro_profile_combo.setCurrentIndex(index)
        finally:
            self._macro_profile_combo_loading = False
        self.macro_profile_combo.setToolTip(selected or "Select a discovered macro set from the FLDigi macros directory.")
        self._refresh_setup_summary()

    def _on_macro_profile_combo_changed(self, index: int) -> None:
        if self._macro_profile_combo_loading or index < 0:
            return
        path = str(self.macro_profile_combo.itemData(index) or "").strip()
        if not path:
            if self._selected_macro_profile_path():
                self._clear_macro_profile()
            return
        if path == self._normalize_macro_profile_path(self._selected_macro_profile_path()):
            return
        self._set_macro_profile_text(path)
        self._save_macro_profile_selection(path, refresh_metadata=True)

    def _refresh_setup_summary(self) -> None:
        mode = self._macro_profile_mode()
        record = self._macro_profile_record(self._selected_macro_profile_path())
        mapping_count = len(record.get("mappings", [])) if isinstance(record.get("mappings", []), list) else 0
        if not self._selected_macro_profile_path():
            self._set_status_chip(self.macro_mode_chip, "Macro: None", "neutral")
        elif mode == "mapped":
            self._set_status_chip(self.macro_mode_chip, "Macro: Mapped", "ready")
        elif mapping_count:
            self._set_status_chip(self.macro_mode_chip, "Macro: Draft", "warning")
        else:
            self._set_status_chip(self.macro_mode_chip, "Macro: Selected", "active")

        log_path = resolve_fldigi_log_path(self.settings.get("fldigi_log_path", ""))
        if not self._log_assisted_enabled():
            self._set_status_chip(self.log_intake_chip, "Log Intake: Off", "neutral")
        elif log_path is None:
            self._set_status_chip(self.log_intake_chip, "Log Intake: Waiting", "warning")
        else:
            self._set_status_chip(self.log_intake_chip, "Log Intake: On", "ready")

        self._set_status_chip(
            self.review_mode_chip,
            "Review: Hold" if self._log_assisted_show_review() else "Review: Off",
            "active" if self._log_assisted_show_review() else "neutral",
        )
        self._set_status_chip(
            self.tx_context_chip,
            "TX Context: On" if self._log_assisted_include_tx() else "TX Context: Off",
            "active" if self._log_assisted_include_tx() else "neutral",
        )
        self._refresh_macro_mapping_locations()

    def _macro_mapping_locations_text(self) -> str:
        checkin_dir = self._resolve_checkin_dir()
        default_paths = [
            ("TFC", checkin_dir / "main_checkins.txt"),
            ("QRU", checkin_dir / "qru_checkins.txt"),
            ("LATE", checkin_dir / "new-late_checkins.txt"),
            ("ALL", checkin_dir / "all_checkins.txt"),
        ]
        lines = ["Macro check-in files:"]
        lines.extend(f"{label}: {path}" for label, path in default_paths)
        lines.append(f"Archive: {checkin_dir / 'archive'}")

        selected = self._normalize_macro_profile_path(self._selected_macro_profile_path())
        record = self._macro_profile_record(selected)
        mappings = record.get("mappings")
        active_mappings = [
            mapping for mapping in mappings if self._macro_profile_mapping_is_complete(mapping)
        ] if isinstance(mappings, list) else []
        if not active_mappings:
            lines.append("Mapped macro files: none active.")
            return "\n".join(lines)

        lines.append("Mapped macro files:")
        for mapping in active_mappings:
            function = str(mapping.get("function") or "").strip().upper() or "CUSTOM"
            if function == "CUSTOM":
                function = str(mapping.get("custom_name") or "").strip() or "CUSTOM"
            scope = str(mapping.get("scope") or "").strip().upper()
            source_file = str(mapping.get("source_file") or "").strip()
            label = f"{scope} {function}".strip()
            lines.append(f"{label}: {source_file or '(macro-only mapping)'}")
        return "\n".join(lines)

    def _refresh_macro_mapping_locations(self) -> None:
        if hasattr(self, "macro_mapping_locations_label"):
            self.macro_mapping_locations_label.setText(self._macro_mapping_locations_text())

    # ---------------- SETTINGS LOAD ---------------- #

    def _load_settings(self):
        self._role_workspace_prefs = load_role_workspace_prefs(self.settings)
        self._load_macro_profile_state()
        self._load_log_assisted_state()
        self._resolve_checkin_dir()
        self._refresh_macro_profile_choices()

        self._populate_net_name_from_schedule()
        self._update_net_name_min_width()

    def _macro_profile_store(self) -> Dict[str, Dict[str, object]]:
        data = self.settings.all()
        store = data.get(self.FLDIGI_MACRO_PROFILES_KEY, {})
        return store if isinstance(store, dict) else {}

    def _selected_macro_profile_path(self) -> str:
        return str(self.settings.get(self.FLDIGI_SELECTED_MACRO_PROFILE_KEY, "") or "").strip()

    def _normalize_macro_profile_path(self, path: str) -> str:
        text = str(path or "").strip()
        if not text:
            return ""
        try:
            return str(Path(text).expanduser().resolve())
        except Exception:
            return str(Path(text).expanduser().absolute())

    def _canonical_macro_profile_store(self) -> Dict[str, Dict[str, object]]:
        store = self._macro_profile_store()
        normalized: Dict[str, Dict[str, object]] = {}
        changed = False
        for raw_path, record in store.items():
            canon_path = self._normalize_macro_profile_path(raw_path)
            if not canon_path:
                continue
            if canon_path != raw_path:
                changed = True
            normalized[canon_path] = dict(record) if isinstance(record, dict) else {}
        if changed:
            self.settings.set(self.FLDIGI_MACRO_PROFILES_KEY, normalized)
        return normalized

    def _macro_profile_record(self, path: str) -> Dict[str, object]:
        canonical = self._normalize_macro_profile_path(path)
        if not canonical:
            return {}
        store = self._canonical_macro_profile_store()
        record = store.get(canonical, {})
        return dict(record) if isinstance(record, dict) else {}

    def _macro_profile_metadata(self, path: str) -> Dict[str, object]:
        canonical = self._normalize_macro_profile_path(path)
        if not canonical:
            return {}
        return scan_macro_profile(canonical)

    def _macro_profile_mapping_is_complete(self, mapping: Dict[str, object]) -> bool:
        if not isinstance(mapping, dict):
            return False
        if not mapping.get("enabled"):
            return False
        if bool(mapping.get("read_only")):
            return False
        scope = str(mapping.get("scope", "") or "").strip()
        function = str(mapping.get("function", "") or "").strip()
        source_file = str(mapping.get("source_file", "") or "").strip()
        macro_id = str(mapping.get("macro_id", "") or "").strip()
        if not scope or not function:
            return False
        if not source_file and not macro_id:
            return False
        return True

    def _macro_profile_has_enabled_mappings(self, record: Dict[str, object]) -> bool:
        mappings = record.get("mappings")
        if not isinstance(mappings, list):
            return False
        for mapping in mappings:
            if self._macro_profile_mapping_is_complete(mapping):
                return True
        return False

    def _macro_profile_mode(self) -> str:
        selected = self._normalize_macro_profile_path(self._selected_macro_profile_path())
        if not selected:
            return "legacy"
        if not Path(selected).exists():
            return "legacy"
        record = self._macro_profile_record(selected)
        return "mapped" if self._macro_profile_has_enabled_mappings(record) else "legacy"

    def _format_macro_profile_timestamp(self, value: object) -> str:
        try:
            dt = datetime.datetime.fromtimestamp(float(value))
        except Exception:
            return ""
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _macro_profile_status_text(self) -> str:
        selected = self._normalize_macro_profile_path(self._selected_macro_profile_path())
        if not selected:
            return "No macro set selected."
        p = Path(selected)
        record = self._macro_profile_record(selected)
        display_name = str(record.get("profile_name") or p.stem or p.name or selected)
        detected_macros = record.get("detected_macros", [])
        meta_bits = []
        if isinstance(detected_macros, list) and detected_macros:
            refs = count_detected_file_references(record)
            meta_bits.append(f"{len(detected_macros)} macros")
            meta_bits.append(f"{refs} file refs")
        meta_suffix = f" ({', '.join(meta_bits)})" if meta_bits else ""
        if not p.exists():
            return f"Selected macro file not found.\nProfile: {display_name}{meta_suffix}"
        enabled = self._macro_profile_has_enabled_mappings(record)
        total_mappings = len(record.get("mappings", [])) if isinstance(record.get("mappings", []), list) else 0
        if enabled:
            mapping_count = len(
                [m for m in record.get("mappings", []) if self._macro_profile_mapping_is_complete(m)]
            )
            return f"Mapped mode ready.\nProfile: {display_name}{meta_suffix}\nActive mappings: {mapping_count}"
        if total_mappings:
            return (
                "Macro set selected. Mapped mode activates when at least one mapping is enabled and complete.\n"
                f"Profile: {display_name}{meta_suffix}\nSaved mappings: {total_mappings}"
            )
        return f"Macro set selected.\nProfile: {display_name}{meta_suffix}\nNo mappings saved yet."

    def _set_macro_profile_text(self, path: str) -> None:
        self._macro_profile_loading = True
        try:
            self.macro_profile_edit.setText(path)
        finally:
            self._macro_profile_loading = False
        self._refresh_macro_profile_choices(select_path=path)

    def _save_macro_profile_selection(self, path: str, *, refresh_metadata: bool = True) -> None:
        canonical = self._normalize_macro_profile_path(path)
        if not canonical:
            self.settings.set(self.FLDIGI_SELECTED_MACRO_PROFILE_KEY, "")
            self.macro_profile_status.setText(self._macro_profile_status_text())
            self._refresh_macro_profile_choices(select_path="")
            self._refresh_custom_bucket_cards()
            self._refresh_setup_summary()
            return
        store = self._canonical_macro_profile_store()
        record = dict(store.get(canonical, {}))
        metadata = self._macro_profile_metadata(canonical) if refresh_metadata else {"profile_path": canonical}
        record.update(metadata)
        store[canonical] = record
        self.settings.set(self.FLDIGI_MACRO_PROFILES_KEY, store)
        self.settings.set(self.FLDIGI_SELECTED_MACRO_PROFILE_KEY, canonical)
        self.macro_profile_status.setText(self._macro_profile_status_text())
        self._refresh_macro_profile_choices(select_path=canonical)
        self._refresh_custom_bucket_cards()
        self._refresh_setup_summary()

    def _load_macro_profile_state(self) -> None:
        selected = self._normalize_macro_profile_path(self._selected_macro_profile_path())
        if selected:
            self._set_macro_profile_text(selected)
            if Path(selected).exists():
                self._save_macro_profile_selection(selected, refresh_metadata=True)
            else:
                self.macro_profile_status.setText(self._macro_profile_status_text())
                self._refresh_setup_summary()
            return
        self._set_macro_profile_text("")
        self.macro_profile_status.setText(self._macro_profile_status_text())
        self._refresh_setup_summary()

    def _choose_macro_profile(self) -> None:
        fn, _ = QFileDialog.getOpenFileName(
            self,
            "Select FLDigi macro profile",
            str(Path(self._selected_macro_profile_path()).parent) if self._selected_macro_profile_path() else "",
            "FLDigi macro files (*.mdf);;All Files (*)",
        )
        if not fn:
            return
        self._set_macro_profile_text(fn)
        self._save_macro_profile_selection(fn, refresh_metadata=True)

    def _clear_macro_profile(self) -> None:
        self._set_macro_profile_text("")
        self._save_macro_profile_selection("", refresh_metadata=False)

    def _on_macro_profile_editing_finished(self) -> None:
        if self._macro_profile_loading:
            return
        path = self.macro_profile_edit.text().strip()
        if not path:
            self._clear_macro_profile()
            return
        canonical = self._normalize_macro_profile_path(path)
        if not canonical:
            self._clear_macro_profile()
            return
        if not Path(canonical).exists():
            QMessageBox.warning(
                self,
                "Macro Profile Not Found",
                "The selected macro profile file does not exist. The previous selection has been kept.",
            )
            self._set_macro_profile_text(self._selected_macro_profile_path())
            return
        self._set_macro_profile_text(canonical)
        self._save_macro_profile_selection(canonical, refresh_metadata=True)

    def _open_macro_mapping_dialog(self) -> None:
        profile_path = self._selected_macro_profile_path()
        if not profile_path:
            QMessageBox.information(self, "Macro Discovery & Mapping", "Select a macro profile first.")
            return
        dialog = FldigiMacroMappingDialog(self.settings, profile_path, self)
        if dialog.exec() == QDialog.Accepted:
            self._load_macro_profile_state()
            self.macro_profile_status.setText(self._macro_profile_status_text())
            self._refresh_setup_summary()

    # ---------------- KNOWN OPERATORS FROM DB ---------------- #

    def _load_known_operators(self):
        """
        Load known operators from the SQLite DB (operator_checkins table) and
        hook them into a QCompleter for the 'known_op_edit' field.

        Format for suggestions: "CALLSIGN NAME STATE"
        """
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.error("Unable to resolve DB path for known operators: %s", e)
            return

        if not db_path.exists():
            return

        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT callsign, IFNULL(name,''), IFNULL(state,'') "
                "FROM operator_checkins ORDER BY callsign ASC"
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error("Failed to load known operators from DB: %s", e)
            return

        suggestions: List[str] = []
        self._known_operator_rows = []
        self._known_operator_by_callsign = {}
        for callsign, name, state in rows:
            cs = (callsign or "").strip().upper()
            nm = (name or "").strip()
            st = (state or "").strip().upper()
            if not cs:
                continue
            parts = [cs]
            if nm:
                parts.append(nm)
            if st:
                parts.append(st)
            display = " ".join(parts)
            suggestions.append(display)
            row = {"callsign": cs, "name": nm, "state": st, "display": display}
            self._known_operator_rows.append(row)
            self._known_operator_by_callsign[cs] = row

        if suggestions:
            completer = QCompleter(sorted(suggestions), self)
            completer.setCaseSensitivity(Qt.CaseInsensitive)
            completer.setFilterMode(Qt.MatchContains)
            self.known_op_edit.setCompleter(completer)

    def _matching_known_operators(self, query: str) -> List[Dict[str, str]]:
        needle = (query or "").strip().lower()
        if not needle:
            return []
        matches = []
        for row in self._known_operator_rows:
            if needle in row.get("display", "").lower():
                matches.append(row)
        return matches

    def _extract_extra_after_tokens(self, text: str, *, min_tokens: int) -> str:
        matches = list(re.finditer(r"[A-Za-z0-9]+", text))
        if len(matches) < min_tokens:
            return ""
        end = matches[min_tokens - 1].end()
        return text[end:].strip()

    def _split_checkin_with_extra(self, line: str) -> tuple[str, str, str, str]:
        raw = self._strip_inline_review_context(line)
        if not raw:
            return "", "", "", ""
        if "/" in raw and not raw.startswith("#"):
            parts = [p.strip() for p in raw.split("/") if p.strip()]
            if len(parts) >= 3:
                cs = parts[0].split()[0].upper()
                name = parts[1]
                state_tokens = parts[2].split()
                state = state_tokens[0].upper() if state_tokens else ""
                trailing = " ".join(state_tokens[1:]).strip()
                extra_parts = []
                if trailing:
                    extra_parts.append(trailing)
                if len(parts) > 3:
                    extra_parts.extend(parts[3:])
                extra = " ".join(part for part in extra_parts if part).strip()
                return cs, name, state, extra
        matches = list(re.finditer(r"[A-Za-z0-9]+", raw))
        if not matches:
            return "", "", "", ""
        cs = matches[0].group(0).upper()
        name = matches[1].group(0) if len(matches) > 1 else ""
        state = matches[2].group(0).upper() if len(matches) > 2 else ""
        extra = self._extract_extra_after_tokens(raw, min_tokens=3)
        return cs, name, state, extra

    def _apply_known_autofill(self) -> bool:
        text = self.known_op_edit.text().strip()
        if not text:
            return False
        matches = self._matching_known_operators(text)
        if not matches:
            comp = self.known_op_edit.completer()
            if comp:
                comp.setCompletionPrefix(text)
                model = comp.completionModel()
                if model and model.rowCount() == 1:
                    completion = model.index(0, 0).data()
                    if completion:
                        matches = self._matching_known_operators(str(completion))
        row = matches[0] if len(matches) == 1 else None
        if row is None:
            tokens = list(re.finditer(r"[A-Za-z0-9]+", text))
            if tokens:
                cs = tokens[0].group(0).upper()
                row = self._known_operator_by_callsign.get(cs)
        if row is None:
            return False
        tokens = list(re.finditer(r"[A-Za-z0-9]+", text))
        if len(tokens) >= 3:
            extra = self._extract_extra_after_tokens(text, min_tokens=3)
        else:
            extra = self._extract_extra_after_tokens(text, min_tokens=1)
        formatted = self._format_entry(row["callsign"], row["name"], row["state"])
        new_text = f"{formatted} {extra}".strip() if extra else formatted
        self.known_op_edit.setText(new_text)
        self._known_op_autofilled_prefix = formatted
        self._known_op_autofill_consumed = True
        self._known_op_tab_stage = 1
        self.known_op_edit.setFocus()
        self.known_op_edit.setCursorPosition(len(new_text))
        return True

    def _focus_add_button(self, target: str) -> None:
        button = self._known_add_buttons.get(target)
        if button is not None:
            button.setFocus()

    def eventFilter(self, obj, event):
        if obj in self._known_add_button_targets and event.type() == QEvent.KeyPress:
            if event.key() in (Qt.Key_Return, Qt.Key_Enter, Qt.Key_Space):
                self._insert_known_into_bucket(self._known_add_button_targets[obj])
                event.accept()
                return True
        if obj is self.known_op_edit and event.type() == QEvent.FocusIn:
            if self._known_op_autofilled_prefix and self.known_op_edit.text().startswith(
                self._known_op_autofilled_prefix
            ):
                self._known_op_tab_stage = 1
            else:
                self._known_op_tab_stage = 0
            return False
        if obj is self.known_op_edit and event.type() == QEvent.KeyPress:
            if event.key() in (Qt.Key_Tab, Qt.Key_Return, Qt.Key_Enter):
                if self._known_op_pending_focus:
                    target = self._known_op_pending_focus
                    self._known_op_pending_focus = None
                    QTimer.singleShot(0, lambda t=target: self._focus_add_button(t))
                    event.accept()
                    return True
        if obj is self.known_op_edit and event.type() == QEvent.ShortcutOverride:
            if event.key() in (Qt.Key_Tab, Qt.Key_Return, Qt.Key_Enter):
                has_prefix = self._known_op_autofilled_prefix and self.known_op_edit.text().startswith(
                    self._known_op_autofilled_prefix
                )
                if has_prefix and self._known_op_tab_stage == 1:
                    self._known_op_pending_focus = "tfc"
                    self._known_op_tab_stage = 2
                    event.accept()
                    return True
                if has_prefix and self._known_op_tab_stage >= 2:
                    self._known_op_pending_focus = "late"
                    event.accept()
                    return True
                if self._apply_known_autofill():
                    QTimer.singleShot(0, self.known_op_edit.setFocus)
                    event.accept()
                    return True
        if obj in self._known_add_button_targets and event.type() == QEvent.KeyPress:
            if event.key() in (Qt.Key_Return, Qt.Key_Enter, Qt.Key_Space):
                self._insert_known_into_bucket(self._known_add_button_targets[obj])
                event.accept()
                return True
        return super().eventFilter(obj, event)

    def _on_known_op_return(self) -> None:
        if self._apply_known_autofill():
            return
        text = self.known_op_edit.text().strip()
        cs, name, state = self._parse_checkin_line(text)
        if cs and name and state:
            formatted = self._format_entry(cs, name, state)
            self.known_op_edit.setText(formatted)

    def _on_known_op_text_changed(self) -> None:
        if (
            self._known_op_autofilled_prefix
            and not self.known_op_edit.text().startswith(self._known_op_autofilled_prefix)
        ):
            self._known_op_autofilled_prefix = ""
            self._known_op_autofill_consumed = False
            self._known_op_tab_stage = 0
        self._update_add_buttons_state()

    def _update_copy_buttons_state(self) -> None:
        self._update_bucket_card_states()

    def _update_add_buttons_state(self) -> None:
        theme = resolve_theme(self.settings)
        text = self.known_op_edit.text().strip()
        cs, name, state = self._parse_checkin_line(text)
        ready = bool(cs and name and state)
        for target, button in self._known_add_buttons.items():
            if not button.isVisible():
                continue
            role = {"tfc": "success", "qru": "info", "late": "warning", "seen_locally": "success"}.get(target, "info")
            button.setStyleSheet(self._known_action_button_style(role, theme, ready))

    def _apply_known_op_styles(self, theme: Dict[str, str]) -> None:
        focus = theme.get("focus", "#7FB5FF")
        border = theme.get("border", "#D3D7DD")
        surface = theme.get("surface", "#F0F2F4")
        text = theme.get("text", "#1C1F21")
        self.known_op_edit.setStyleSheet(
            "QLineEdit {"
            f" background-color: {surface}; color: {text}; border: 1px solid {border};"
            " border-radius: 4px; padding: 4px 6px;"
            " }"
            " QLineEdit:focus {"
            f" border: 2px solid {focus}; padding: 3px 5px;"
            " }"
        )

    def _known_action_button_style(self, role: str, theme: Dict[str, str], ready: bool) -> str:
        focus = theme.get("focus", "#7FB5FF")
        role_color = theme.get(role, theme.get("accent", "#2E6F9E"))
        style = button_style(f"eligible_{role}" if ready else "muted", theme)
        if ready:
            style += (
                " QPushButton:focus {"
                f" background-color: {role_color}; color: #FFFFFF; border: 2px solid {role_color};"
                " padding: 3px 9px;"
                " }"
            )
        else:
            style += (
                " QPushButton:focus {"
                f" border: 2px solid {focus}; padding: 3px 9px;"
                " }"
            )
        return style

    # ---------------- Net name auto-fill ---------------- #

    def _next_net_from_schedule(self) -> Optional[Dict]:
        data = self.settings.all()
        net_sched = data.get("net_schedule", [])
        if not isinstance(net_sched, list):
            return None

        now = datetime.datetime.now(datetime.timezone.utc)
        best_active = None
        best_future = None

        for row in net_sched:
            if not isinstance(row, dict):
                continue
            occ = self._next_net_occurrence(row, now)
            if not occ:
                continue
            if occ["active"]:
                if best_active is None or occ["start_dt"] < best_active["start_dt"]:
                    best_active = occ
            else:
                if occ["window_start"] >= now and (
                    best_future is None or occ["start_dt"] < best_future["start_dt"]
                ):
                    best_future = occ

        if best_active:
            return best_active
        return best_future

    def _populate_net_name_from_schedule(self):
        occ = self._next_net_from_schedule()
        if not occ:
            return
        row = occ["row"]
        net_name = (row.get("net_name") or "").strip()
        mode = (row.get("mode") or "").strip()
        band = (row.get("band") or "").strip()
        freq = (row.get("frequency") or "").strip()
        start = (row.get("start_utc") or "").strip()
        group = net_name  # No group field in net schedule; reuse net name for display
        day = (row.get("day_utc") or "").strip()
        parts = [group, net_name, mode, band, freq, day, f"{start} UTC"]
        formatted = " - ".join([p for p in parts if p])
        if formatted:
            idx = self.net_name_combo.findText(formatted)
            if idx < 0:
                self.net_name_combo.addItem(formatted)
                idx = self.net_name_combo.count() - 1
            self.net_name_combo.setCurrentIndex(idx)
            self._update_net_name_min_width()

    def _update_net_name_min_width(self):
        """
        Set the minimum width based on the widest current entry plus space for ~10 extra characters.
        """
        metrics = QFontMetrics(self.net_name_combo.font())
        pad = metrics.horizontalAdvance("0" * 10)
        max_w = 0
        for i in range(self.net_name_combo.count()):
            txt = self.net_name_combo.itemText(i)
            if not txt:
                continue
            w = metrics.horizontalAdvance(txt) + pad
            if w > max_w:
                max_w = w
        if max_w > 0:
            # Add a small safety margin
            self.net_name_combo.setMinimumWidth(min(max_w + 8, 300))
        else:
            # Fallback if no items exist yet
            self.net_name_combo.setMinimumWidth(320)

    # ---------------- Browse / HINT ---------------- #

    # ---------------- FILE HELPERS ---------------- #

    def _read_file(self, path: str) -> str:
        if not path:
            return ""
        try:
            p = Path(path)
            if not p.exists():
                return ""
            return p.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            log.error("Failed to read file %s: %s", path, e)
            return ""

    def _append_file(self, path: str, text: str):
        if not path:
            return
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("a", encoding="utf-8") as f:
                f.write(text)
        except Exception as e:
            log.error("Failed to append to file %s: %s", path, e)

    def _write_file(self, path: str, text: str):
        if not path:
            return
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(text, encoding="utf-8")
        except Exception as e:
            log.error("Failed to write file %s: %s", path, e)

    def _resolve_checkin_dir(self) -> Path:
        stored = (self.settings.get("fldigi_checkin_dir", "") or "").strip()
        if not stored:
            stored = str(get_fldigi_checkin_dir())
            try:
                self.settings.set("fldigi_checkin_dir", stored)
            except Exception:
                pass
        return Path(stored)

    def _checkin_file_paths(self) -> tuple[str, str, str]:
        base = self._resolve_checkin_dir()
        return (
            str(base / "main_checkins.txt"),
            str(base / "qru_checkins.txt"),
            str(base / "new-late_checkins.txt"),
        )

    def _all_checkins_file_path(self) -> str:
        return str(self._resolve_checkin_dir() / "all_checkins.txt")

    def _checkin_archive_dir(self) -> Path:
        return self._resolve_checkin_dir() / "archive"

    def _ensure_checkin_files(self) -> tuple[str, str, str]:
        base = self._resolve_checkin_dir()
        main_path = base / "main_checkins.txt"
        qru_path = base / "qru_checkins.txt"
        late_path = base / "new-late_checkins.txt"
        all_path = base / "all_checkins.txt"
        base.mkdir(parents=True, exist_ok=True)
        if not main_path.exists():
            main_path.touch()
        if not qru_path.exists():
            qru_path.touch()
        if not late_path.exists():
            late_path.touch()
        if not all_path.exists():
            all_path.touch()
        return str(main_path), str(qru_path), str(late_path)

    def _archive_checkin_files(self) -> List[Path]:
        archive_dir = self._checkin_archive_dir()
        archive_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%SZ")
        net_name = self.net_name_combo.currentText().strip() if hasattr(self, "net_name_combo") else ""
        net_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", net_name).strip("._-")
        prefix = f"{stamp}_{net_slug}" if net_slug else stamp
        archived: List[Path] = []
        paths = [Path(path) for path in self._checkin_file_paths()]
        paths.append(Path(self._all_checkins_file_path()))
        for source in paths:
            if not source.exists() or not source.is_file():
                continue
            target = archive_dir / f"{prefix}_{source.name}"
            counter = 2
            while target.exists():
                target = archive_dir / f"{prefix}_{source.stem}-{counter}{source.suffix}"
                counter += 1
            target.write_text(source.read_text(encoding="utf-8", errors="ignore"), encoding="utf-8")
            archived.append(target)
        if archived:
            log.info("Archived %d FLDigi check-in files to %s", len(archived), archive_dir)
        return archived

    # ---------------- BUTTON LOGIC ---------------- #

    def _validate_before_start(self) -> bool:
        net_name = self.net_name_combo.currentText().strip()

        if not net_name:
            QMessageBox.warning(self, "Missing Net Name", "Enter Net Name before starting the net.")
            return False

        try:
            self._ensure_checkin_files()
        except Exception as e:
            QMessageBox.critical(
                self,
                "File Error",
                f"Unable to create or access log files:\n{e}",
            )
            return False

        return True

    def _start_net(self):
        if self._net_in_progress:
            QMessageBox.information(self, "Net In Progress", "A net is already active. End it before starting a new one.")
            return
        if not self._validate_before_start():
            return
        main_path, qru_path, late_path = self._ensure_checkin_files()

        # Load existing file contents
        # Clear files to avoid loading stale/pre-populated data
        self._write_file(main_path, "")
        self._write_file(qru_path, "")
        self._write_file(late_path, "")
        self._write_file(self._all_checkins_file_path(), "")
        self._roster_clear()
        self.main_text.setPlainText("")
        self.qru_text.setPlainText("")
        self.late_text.setPlainText("")
        self.reference_card.set_text("")
        self.compare_results_card.set_text("")
        self.review_card.set_text("")
        self._compare_missing_text = ""
        self._compare_reference_missing_entries = []
        self._sync_compare_workspace_tabs()
        self.compare_workspace_tabs.setCurrentWidget(self.reference_card)
        self._update_bucket_card_states()

        # Append operator line CALLSIGN / NAME / STATE [/ ROLE]
        cs = (self.settings.get("operator_callsign", "") or "").strip().upper()
        name = (self.settings.get("operator_name", "") or "").strip()
        state = (self.settings.get("operator_state", "") or "").strip().upper()
        if cs or name or state:
            self._roster_append_row(cs, name, state, "", "TFC")

        self._net_in_progress = True
        self._net_start_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
        self._roster_sync_legacy_buffers(write_files=True)
        self._capture_log_assisted_session()
        self.net_status_changed.emit("FLDIGI", True)
        self._set_net_button_styles(active=True)
        log.info("FLDigi net started: %s (%s)", self.net_name_combo.currentText().strip(), self.role_combo.currentText())
        self._refresh_operator_history_views()

    def _start_ad_hoc_net(self):
        """
        Generate and start an ad hoc net with a UTC timestamped name.
        """
        if self._net_in_progress:
            QMessageBox.information(self, "Net In Progress", "End the current net before starting an ad hoc net.")
            return
        current_name = self.net_name_combo.currentText().strip()
        if current_name:
            resp = QMessageBox.question(
                self,
                "Replace Net Name",
                "Replace the current net name with an ad hoc name?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if resp != QMessageBox.Yes:
                return
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d %H:%M")
        ad_hoc_name = f"FLDIGI - Ad Hoc - {ts} UTC"
        self.net_name_combo.setEditText(ad_hoc_name)
        self._start_net()

    def _save_checkins(self):
        main_path, qru_path, late_path = self._ensure_checkin_files()
        self._roster_sync_legacy_buffers(write_files=False)
        main_text = self._roster_table_text("TFC")
        qru_text = self._roster_table_text("QRU")
        late_text = self._roster_table_text("LATE")
        self._write_file(main_path, main_text)
        self._write_file(qru_path, qru_text)
        self._write_file(late_path, late_text)
        self._write_file(self._all_checkins_file_path(), self._roster_table_text())

        QMessageBox.information(self, "Saved", "Check-in logs saved.")
        self._refresh_operator_history_views()

    def _merge_late_into_main(self):
        main_path, _qru_path, late_path = self._ensure_checkin_files()
        late_text = self._roster_table_text("LATE")
        if not late_text.strip():
            late_text = self._read_file(late_path)
        if not late_text.strip():
            self._write_file(late_path, "")
            return

        for raw in late_text.splitlines():
            cs, name, state, extra = self._split_checkin_with_extra(raw)
            if not cs and not name and not state:
                continue
            self._roster_append_row(cs, name, state, extra, "TFC")
        for row in range(self.roster_table.rowCount()):
            item = self.roster_table.item(row, 4)
            widget = self.roster_table.cellWidget(row, 4)
            current = ""
            if widget is not None and hasattr(widget, "currentText"):
                current = str(widget.currentText() or "").strip().upper()
            elif item is not None:
                current = item.text().strip().upper()
            if current == "LATE":
                self._roster_set_category(row, "TFC")
        self._roster_sync_legacy_buffers(write_files=True)
        self._write_file(late_path, "")

        # No popup needed; UI visibly clears the late list.

    def _copy_summary(self):
        """
        Copy the consolidated check-in log to the clipboard.
        """
        text = self._roster_table_text()
        if text:
            QApplication.clipboard().setText(text)
            self._write_file(self._all_checkins_file_path(), text)
        QMessageBox.information(self, "Copied", "Check-ins copied to clipboard.")

    def _copy_text_to_clipboard(self, text: str):
        """
        Copy raw text (already normalized per line) to clipboard.
        """
        QApplication.clipboard().setText(text)

    def _format_freq(self, val) -> str:
        try:
            return f"{float(val):.3f}"
        except Exception:
            return str(val) if val is not None else ""

    # ---------------- NORMALIZATION ---------------- #

    def _on_main_text_changed(self):
        # Avoid auto-normalizing while the operator is editing in real time.
        self._update_copy_buttons_state()

    def _on_late_text_changed(self):
        # Avoid auto-normalizing while the operator is editing in real time.
        self._update_copy_buttons_state()

    def _normalize_text_edit(self, edit: QTextEdit, flag_attr: str) -> None:
        """
        Keep entries normalized to 'CALL / Name / ST' as users type or paste.
        """
        if getattr(self, flag_attr, False):
            return
        setattr(self, flag_attr, True)
        try:
            original = edit.toPlainText()
            if original is None:
                return
            lines = original.splitlines()
            normalized_lines = []
            for line in lines:
                raw_line = line
                if "/" in raw_line and raw_line.count("/") < 2:
                    normalized_lines.append(raw_line)
                    continue
                cs, name, state = self._parse_checkin_line(line)
                if cs or name or state:
                    normalized_lines.append(self._format_entry(cs, name, state))
                else:
                    normalized_lines.append(raw_line)
            normalized = "\n".join(normalized_lines)
            if original.endswith("\n"):
                normalized += "\n"
            if normalized != original:
                cursor = edit.textCursor()
                pos = cursor.position()
                edit.blockSignals(True)
                try:
                    edit.setPlainText(normalized)
                    new_cursor = edit.textCursor()
                    new_cursor.setPosition(min(pos, len(normalized)))
                    edit.setTextCursor(new_cursor)
                finally:
                    edit.blockSignals(False)
        finally:
            setattr(self, flag_attr, False)

    def _end_net(self):
        resp = QMessageBox.question(
            self,
            "End Net",
            "End net now?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return
        if not self._net_in_progress:
            log.info("End Net clicked but no net_in_progress flag set; proceeding with DB load from file.")
        self._save_checkins()

        main_path, qru_path, _ = self._checkin_file_paths()

        main_text = self._roster_table_text("TFC") or self._read_file(main_path)
        qru_text = self._roster_table_text("QRU") or self._read_file(qru_path)
        late_path = self._checkin_file_paths()[2]
        late_text = self._roster_table_text("LATE") or self._read_file(late_path)
        combined_text = "\n".join(text for text in (main_text, qru_text, late_text) if text.strip())
        if not combined_text.strip():
            resp = QMessageBox.question(
                self,
                "End Net?",
                "Main, QRU, and LATE check-in logs are empty. End the net without importing any check-ins?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if resp != QMessageBox.Yes:
                return
            self._archive_checkin_files()
            # End net even though no check-ins exist
            self._net_in_progress = False
            self._reset_log_assisted_session()
            self.net_status_changed.emit("FLDIGI", False)
            self._set_net_button_styles(active=False)
            log.info("FLDigi net ended (no check-ins file content).")
            return

        now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
        net_name = self.net_name_combo.currentText().strip()
        role = self.role_combo.currentText().strip().upper()
        group_name = self._operating_group_for_freq(self._current_freq_mhz())

        entries: List[Dict] = []
        seen_callsigns = set()
        for line in combined_text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cs, name, state, _extra = self._split_checkin_with_extra(line)
            cs, name, state = self._normalize_checkin_fields(cs, name, state)
            if not cs:
                continue
            if cs in seen_callsigns:
                continue
            seen_callsigns.add(cs)
            existing_name, existing_state, exists = self._lookup_operator_name_state(cs)
            name_out = existing_name
            if name and (not existing_name or name.strip().lower() != existing_name.strip().lower()):
                name_out = name
            state_out = existing_state
            if not existing_state and state:
                state_out = state
            if not exists:
                name_out = name
                state_out = state
            group1 = group2 = group3 = None
            groups_json = None
            if group_name:
                existing_groups = self._lookup_operator_groups(cs)
                if group_name not in existing_groups:
                    existing_groups.append(group_name)
                group1 = existing_groups[0] if len(existing_groups) > 0 else ""
                group2 = existing_groups[1] if len(existing_groups) > 1 else ""
                group3 = existing_groups[2] if len(existing_groups) > 2 else ""
                groups_json = existing_groups if existing_groups else None
            entries.append(
                {
                    "callsign": cs,
                    "name": name_out,
                    "state": state_out,
                    "first_seen_utc": now_utc,
                    "last_seen_utc": now_utc,
                    "net_name": net_name,
                    "role": role,
                    "trusted": None,
                    "group1": group1 or "",
                    "group2": group2 or "",
                    "group3": group3 or "",
                    "groups_json": json.dumps(groups_json) if groups_json else None,
                }
            )

        if entries:
            upsert_checkins(entries)
            self._bump_operator_history(entries)
            self._roster_sync_legacy_buffers(write_files=True)
            QMessageBox.information(
                self,
                "Net Ended",
                f"Net ended. {len(entries)} check-ins imported into the operator database.",
            )
        else:
            QMessageBox.information(
                self,
                "Net Ended",
                "Net ended. No valid check-ins found to import.",
            )

        self._archive_checkin_files()
        self._net_in_progress = False
        self._reset_log_assisted_session()
        self.net_status_changed.emit("FLDIGI", False)
        self._set_net_button_styles(active=False)
        log.info("FLDigi net ended: %s (%s)", net_name, role)

    # ---------------- INSERT KNOWN OPERATOR ---------------- #

    def _insert_known_into_main(self):
        line = self.known_op_edit.text().strip()
        if not line:
            return
        cs, name, state, extra = self._split_checkin_with_extra(line)
        if not cs and not name and not state:
            return
        lookup = self._known_operator_by_callsign.get(cs or "")
        if lookup:
            cs = lookup.get("callsign", cs)
            name = lookup.get("name", name)
            state = lookup.get("state", state)
        self._roster_append_row(cs, name, state, extra, "TFC")
        self.known_op_edit.clear()
        self.known_op_edit.setFocus()

    def _insert_known_into_late(self):
        line = self.known_op_edit.text().strip()
        if not line:
            return
        cs, name, state, extra = self._split_checkin_with_extra(line)
        if not cs and not name and not state:
            return
        lookup = self._known_operator_by_callsign.get(cs or "")
        if lookup:
            cs = lookup.get("callsign", cs)
            name = lookup.get("name", name)
            state = lookup.get("state", state)
        self._roster_append_row(cs, name, state, extra, "LATE")
        self.known_op_edit.clear()
        self.known_op_edit.setFocus()

    # ---------------- PARSING ---------------- #

    def _parse_checkin_line(self, line: str):
        """
        Accept both:
          - CALLSIGN/NAME/STATE/
          - CALLSIGN NAME STATE [traffic...]

        Returns (callsign, name, state)
        """
        line = self._strip_inline_review_context(line)
        if not line:
            return "", "", ""

        if "/" in line and not line.startswith("#"):
            parts = [p.strip() for p in line.split("/") if p.strip()]
            if len(parts) >= 3:
                state_tokens = parts[2].split()
                state = state_tokens[0].upper() if state_tokens else ""
                return parts[0].split()[0].upper(), parts[1], state
            elif len(parts) == 2:
                return parts[0].split()[0].upper(), parts[1], ""
            else:
                pass

        tokens = line.split()
        if len(tokens) >= 3:
            return tokens[0].upper(), tokens[1], tokens[2]
        elif len(tokens) == 2:
            return tokens[0].upper(), tokens[1], ""
        elif tokens:
            return tokens[0].upper(), "", ""
        return "", "", ""

    def _format_entry(self, cs: str, name: str, state: str) -> str:
        """
        Normalize check-in display to 'CALL / Name / ST' with single separators.
        """
        parts = [p for p in (cs.strip().upper(), name.strip(), state.strip().upper()) if p]
        return " / ".join(parts)

    @staticmethod
    def _normalize_checkin_fields(cs: str, name: str, state: str) -> tuple[str, str, str]:
        cs_norm = (cs or "").strip().upper()
        state_norm = (state or "").strip().upper()
        name_norm = (name or "").strip()
        if name_norm:
            name_norm = name_norm[0].upper() + name_norm[1:]
        return cs_norm, name_norm, state_norm

    def _lookup_operator_name_state(self, callsign: str) -> tuple[str, str, bool]:
        cs = (callsign or "").strip().upper()
        if not cs:
            return "", "", False
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return "", "", False
        if not db_path.exists():
            return "", "", False
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT name, state FROM operator_checkins WHERE callsign=?", (cs,))
            row = cur.fetchone()
            conn.close()
        except Exception:
            return "", "", False
        if not row:
            return "", "", False
        return (row[0] or "").strip(), (row[1] or "").strip().upper(), True

    def _bump_operator_history(self, entries: List[Dict]):
        """
        Update operator_checkins table with new/updated operator info and increment checkin_count by 1.
        Schema matches OperatorHistoryTab.
        """
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return
        if not db_path.exists():
            return
        try:
            # Ensure schema matches OperatorHistoryTab expectations
            from freqinout.gui.operator_history_tab import OperatorHistoryTab
            dummy = OperatorHistoryTab()
            conn = sqlite3.connect(db_path)
            try:
                dummy._ensure_schema(conn)  # type: ignore[attr-defined]
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fldigi_checkins (
                        callsign TEXT PRIMARY KEY,
                        last_seen_ts REAL
                    )
                    """
                )
                today_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
                for e in entries:
                    cs = (e.get("callsign") or "").strip().upper()
                    if not cs:
                        continue
                    first_seen = (e.get("first_seen_utc") or "").strip() or today_str
                    last_seen = (e.get("last_seen_utc") or "").strip() or today_str
                    trusted_in = e.get("trusted")
                    trusted_val = 0 if trusted_in is False or trusted_in == 0 else None
                    cur.execute(
                        """
                        INSERT INTO operator_checkins (
                            callsign, name, state, first_seen_utc, last_seen_utc, checkin_count, trusted
                        )
                        VALUES (?, ?, ?, ?, ?, 1, COALESCE(?, 0))
                        ON CONFLICT(callsign) DO UPDATE SET
                            name=excluded.name,
                            state=excluded.state,
                            first_seen_utc=COALESCE(operator_checkins.first_seen_utc, excluded.first_seen_utc),
                            last_seen_utc=COALESCE(excluded.last_seen_utc, operator_checkins.last_seen_utc),
                            checkin_count=operator_checkins.checkin_count + 1,
                            trusted=COALESCE(operator_checkins.trusted, excluded.trusted)
                        """,
                        (
                            cs,
                            (e.get("name") or "").strip(),
                            (e.get("state") or "").strip().upper(),
                            first_seen,
                            last_seen,
                            trusted_val,
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO fldigi_checkins (callsign, last_seen_ts)
                        VALUES (?, ?)
                        ON CONFLICT(callsign) DO UPDATE SET last_seen_ts=excluded.last_seen_ts
                        """,
                        (cs, float(datetime.datetime.now(datetime.timezone.utc).timestamp())),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as ex:
            log.error("Failed to bump operator history: %s", ex)
