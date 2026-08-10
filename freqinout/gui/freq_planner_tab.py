from __future__ import annotations

import datetime
import json
import time
import sqlite3
import re
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QComboBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QColorDialog,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QInputDialog,
    QMessageBox,
    QLineEdit,
)

from pathlib import Path

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.core.config_paths import get_config_dir
from freqinout.core.perf_metrics import emit_span
from freqinout.core.plan_context_service import PlanContextService
from freqinout.core.schedule_source_sets import (
    LIVE_SOURCE_SET_ID,
    HF_DAILY_SOURCE_CATEGORY,
    HF_DAILY_SOURCE_SETS_KEY,
    HF_NET_SOURCE_CATEGORY,
    HF_NET_SOURCE_SETS_KEY,
    SELECTED_HF_DAILY_SOURCE_SET_KEY,
    SELECTED_HF_NET_SOURCE_SET_KEY,
    selected_source_schedule_dependency_refs,
    selected_source_set_id,
    source_set_row_by_id_for_category,
    source_sets_for_category,
)
from freqinout.core.schedule_projection import (
    BlendedScheduleProjection,
    ProjectionCell,
    ScheduleSegment,
    build_blended_schedule_projection,
)
from freqinout.core.operational_projection import (
    OperationalCell,
    OperationalDayProjection,
    build_operational_day_projection,
    build_operational_day_projection_from_refs,
)
from freqinout.utils.timezones import get_timezone
from freqinout.gui.plan_context_label import PLAN_CONTEXT_FALLBACK_TEXT, PlanContextLabel
from freqinout.gui.theme import resolve_theme, button_style, band_cell_colors, qcolor, BAND_COLORS_LIGHT, BAND_COLORS_DARK

DAY_NAMES = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]
DAY_NAMES_UPPER = [d.upper() for d in DAY_NAMES]
FREQPLANNER_CONTEXT_FALLBACK_TEXT = PLAN_CONTEXT_FALLBACK_TEXT


class FreqPlannerTab(QWidget):
    """
    Frequency planner view.

    - Rows: hours 00..23 (UTC hour buckets)
    - Columns:
        0: UTC Hour
        1: Local Time (HH:00 AM/PM TZ)
        2-8: Sunday .. Saturday

    Cell contents:
      - If only HF schedule applies at that hour: show the band (or multiple bands as "40M / 80M").
      - If one or more nets apply: show "band|net name" or "band1 / band2|net1 / net2".
      - Uses hf_schedule (or legacy daily_schedule) and net_schedule from config.json.

    Highlighting:
      - Current UTC weekday column cells are highlighted *only if* they have a net in that hour.

    Local time:
      - Uses the timezone stored in Settings ("timezone") via get_timezone(), so it is
        consistent and cross-platform.
    """

    COL_UTC = 0
    COL_LOCAL = 1
    COL_DAY_OFFSET = 2  # Sunday at column 2

    def __init__(self, parent=None, *, plan_context_service: Optional[PlanContextService] = None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self.plan_context_service = plan_context_service or PlanContextService()
        default_mode = (self.settings.get("display_time_mode", "LOCAL") or "LOCAL").upper()
        self._show_local = default_mode != "UTC"
        self._show_band = True
        self._band_colors: Dict[str, str] = {}
        self._visible_bands: List[str] = []
        self._selected_projection_cell: Optional[ProjectionCell] = None
        self._selected_operational_cell: Optional[OperationalCell] = None
        self._clock_timer: QTimer | None = None
        self._last_snapshot: str = ""
        self._last_rebuild_check_ts: float = 0.0
        self._pending_rebuild: bool = False
        self._build_ui()
        self._apply_theme()
        self.rebuild_table()

    # ------------- UI ------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>FreqPlanner</h3>"))
        header.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        self.utc_label.setVisible(False)
        self.local_label.setVisible(False)
        self.time_toggle_btn = QPushButton("Times: Local" if self._show_local else "Times: UTC")
        theme = resolve_theme(self.settings)
        self.time_toggle_btn.setStyleSheet(button_style("primary", theme))
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        layout.addLayout(header)

        self.plan_context_label = PlanContextLabel(
            "freqplanner",
            service=self.plan_context_service,
            fallback_text=FREQPLANNER_CONTEXT_FALLBACK_TEXT,
        )
        self.plan_context_label.setToolTip(
            "Use FreqPlanner to verify where and when the saved plan expects activity. Edit the source rows in HF Daily, HF Nets, SOP Builder, or Settings."
        )
        self.plan_context_label.setVisible(False)
        layout.addWidget(self.plan_context_label)
        self.plan_context_label.refresh_context(refresh=True)

        plan_workspace = QHBoxLayout()
        plan_workspace.setSpacing(8)
        plan_workspace.addWidget(QLabel("Frequency Plan:"))
        self.frequency_plan_combo = QComboBox()
        self.frequency_plan_combo.setObjectName("freqPlannerFrequencyPlanCombo")
        self.frequency_plan_combo.currentIndexChanged.connect(self._on_frequency_plan_selected)
        plan_workspace.addWidget(self.frequency_plan_combo, 1)
        plan_workspace.addWidget(self.time_toggle_btn)
        self.save_plan_btn = QPushButton("Save Plan")
        self.save_sop_plan_btn = QPushButton("Save SOP Plan")
        self.assign_plan_btn = QPushButton("Assign Plan")
        self.make_active_plan_btn = QPushButton("Make Active")
        self.use_ad_hoc_plan_btn = QPushButton("Use Ad Hoc")
        self.save_plan_btn.clicked.connect(self._on_save_plan_clicked)
        self.save_plan_btn.setToolTip("Review the blended HF Daily + HF Nets + SOP projection and save it as a named Frequency Plan.")
        plan_workspace.addWidget(self.save_plan_btn)
        self.save_sop_plan_btn.clicked.connect(self._on_save_sop_plan_clicked)
        self.save_sop_plan_btn.setToolTip("Review side-by-side SOP, HF Daily, HF Net, and Net Resource lanes and save them as an SOP Schedule Plan.")
        plan_workspace.addWidget(self.save_sop_plan_btn)
        self.assign_plan_btn.clicked.connect(self._on_assign_plan_clicked)
        self.assign_plan_btn.setEnabled(False)
        self.assign_plan_btn.setToolTip("Select or save a Frequency Plan, then assign it to radios in Settings > Schedule Assignment.")
        plan_workspace.addWidget(self.assign_plan_btn)
        for btn in (self.make_active_plan_btn, self.use_ad_hoc_plan_btn):
            btn.setEnabled(False)
            btn.setToolTip("Frequency Plan assignment and activation flows remain in Settings for this slice.")
            plan_workspace.addWidget(btn)
        layout.addLayout(plan_workspace)

        source_workspace = QHBoxLayout()
        source_workspace.setSpacing(8)
        source_workspace.addWidget(QLabel("HF Daily:"))
        self.hf_daily_source_combo = QComboBox()
        self.hf_daily_source_combo.setObjectName("freqPlannerHfDailySourceCombo")
        self.hf_daily_source_combo.setToolTip("Select Live Current or a named HF Daily schedule saved from the HF Daily tab.")
        self.hf_daily_source_combo.currentIndexChanged.connect(self._on_source_set_selected)
        source_workspace.addWidget(self.hf_daily_source_combo, 1)
        source_workspace.addWidget(QLabel("HF Nets:"))
        self.hf_net_source_combo = QComboBox()
        self.hf_net_source_combo.setObjectName("freqPlannerHfNetSourceCombo")
        self.hf_net_source_combo.setToolTip("Select Live Current or a named HF Net schedule saved from the HF Nets tab.")
        self.hf_net_source_combo.currentIndexChanged.connect(self._on_source_set_selected)
        source_workspace.addWidget(self.hf_net_source_combo, 1)
        layout.addLayout(source_workspace)

        view_workspace = QHBoxLayout()
        view_workspace.setSpacing(8)
        view_workspace.addWidget(QLabel("View:"))
        self.planner_view_combo = QComboBox()
        self.planner_view_combo.setObjectName("freqPlannerViewCombo")
        self.planner_view_combo.addItem("Blended Week", "blended")
        self.planner_view_combo.addItem("SOP Lanes", "operational")
        self.planner_view_combo.currentIndexChanged.connect(self._on_planner_view_changed)
        view_workspace.addWidget(self.planner_view_combo)
        view_workspace.addWidget(QLabel("Day:"))
        self.operational_day_combo = QComboBox()
        self.operational_day_combo.setObjectName("freqPlannerOperationalDayCombo")
        for day in DAY_NAMES:
            self.operational_day_combo.addItem(day, day)
        self.operational_day_combo.currentIndexChanged.connect(self._on_planner_view_changed)
        self.operational_day_combo.setEnabled(False)
        view_workspace.addWidget(self.operational_day_combo)
        view_workspace.addStretch()
        layout.addLayout(view_workspace)

        self.frequency_plan_summary_label = QLabel("")
        self.frequency_plan_summary_label.setObjectName("freqPlannerFrequencyPlanSummary")
        self.frequency_plan_summary_label.setWordWrap(True)
        layout.addWidget(self.frequency_plan_summary_label)
        self.frequency_plan_action_hint_label = QLabel("Save Plan captures the reviewed HF Daily + HF Nets + SOP projection as a named Frequency Plan.")
        self.frequency_plan_action_hint_label.setObjectName("freqPlannerFrequencyPlanActionHint")
        self.frequency_plan_action_hint_label.setWordWrap(True)
        layout.addWidget(self.frequency_plan_action_hint_label)
        self.cell_inspector_label = QLabel("Select a schedule cell to review the blended HF Daily, HF Nets, and SOP sources.")
        self.cell_inspector_label.setObjectName("freqPlannerCellInspector")
        self.cell_inspector_label.setWordWrap(True)
        layout.addWidget(self.cell_inspector_label)
        inspector_actions = QHBoxLayout()
        self.edit_hf_daily_btn = QPushButton("Edit HF Daily")
        self.edit_hf_net_btn = QPushButton("Edit HF Net")
        self.open_sop_builder_btn = QPushButton("Open SOP Builder")
        self.edit_sop_plan_entry_btn = QPushButton("Edit Plan Entry")
        self.edit_hf_daily_btn.clicked.connect(self._on_edit_hf_daily_clicked)
        self.edit_hf_net_btn.clicked.connect(self._on_edit_hf_net_clicked)
        self.open_sop_builder_btn.clicked.connect(self._on_open_sop_builder_clicked)
        self.edit_sop_plan_entry_btn.clicked.connect(self._on_edit_sop_plan_entry_clicked)
        for btn in (self.edit_hf_daily_btn, self.edit_hf_net_btn, self.open_sop_builder_btn, self.edit_sop_plan_entry_btn):
            btn.setEnabled(False)
            inspector_actions.addWidget(btn)
        inspector_actions.addStretch()
        layout.addLayout(inspector_actions)
        self._refresh_plan_workspace_header()

        self.band_legend = QWidget()
        self.band_legend_layout = QHBoxLayout(self.band_legend)
        self.band_legend_layout.setContentsMargins(0, 0, 0, 0)
        self.band_legend_layout.setSpacing(6)
        self.band_toggle_btn = QPushButton("Showing Band")
        self.band_toggle_btn.setStyleSheet(button_style("info", theme))
        self.band_toggle_btn.clicked.connect(self._toggle_band_view)
        self.band_legend_layout.addWidget(self.band_toggle_btn)
        layout.addWidget(self.band_legend)

        self.table = QTableWidget()
        self.table.setRowCount(24)
        self.table.setColumnCount(9)  # UTC, Local, Sun..Sat
        self.table.cellClicked.connect(self._on_schedule_cell_clicked)

        # Set headers with local TZ name in Local column
        tz_name, tz_abbr = self._current_timezone_label()
        self.table.setHorizontalHeaderLabels(
            [
                "UTC Hour",
                f"Local Time ({tz_abbr})",
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]
        )

        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_UTC, QHeaderView.Stretch)
        hv.setSectionResizeMode(self.COL_LOCAL, QHeaderView.Stretch)
        for col in range(self.COL_DAY_OFFSET, 9):
            hv.setSectionResizeMode(col, QHeaderView.Stretch)
        hv.setHighlightSections(False)

        layout.addWidget(self.table)

        self._setup_clock_timer()
        self._load_band_colors()
        self._refresh_source_set_controls()
        self._render_band_legend()

    def _on_planner_view_changed(self) -> None:
        if hasattr(self, "operational_day_combo"):
            self.operational_day_combo.setEnabled(self._planner_view_mode() == "operational")
        self.rebuild_table()

    # ------------- helpers ------------- #

    def _current_timezone(self) -> tuple[str, datetime.tzinfo]:
        """
        Returns (tz_name, tzinfo) using the Settings timezone and the
        shared get_timezone() helper so it works on all platforms.
        """
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        return tz_name, tz

    def _current_timezone_label(self) -> tuple[str, str]:
        """
        Returns (tz_name, tz_abbr) for labeling the Local column header.
        Uses tzname() when available, otherwise a short fallback (ET/CT/MT/PT/UTC).
        """
        tz_name, tz = self._current_timezone()
        now = datetime.datetime.now(tz)
        abbr = now.tzname() or self._ui_tz_abbr(tz_name, tz_name)
        if abbr and len(abbr) > 5:
            abbr = self._ui_tz_abbr(tz_name, abbr)
        return tz_name, abbr

    def _planner_view_mode(self) -> str:
        if not hasattr(self, "planner_view_combo"):
            return "blended"
        mode = str(self.planner_view_combo.currentData() or "").strip().lower()
        return mode if mode in {"blended", "operational"} else "blended"

    def _selected_operational_day(self) -> str:
        if not hasattr(self, "operational_day_combo"):
            return DAY_NAMES[0]
        day = str(self.operational_day_combo.currentData() or self.operational_day_combo.currentText() or "").strip()
        return day if day in DAY_NAMES else DAY_NAMES[0]

    def _prompt_for_name(self, title: str, label: str, default_name: str) -> Tuple[str, bool]:
        dialog = QDialog(self)
        dialog.setWindowTitle(title)
        layout = QVBoxLayout(dialog)
        prompt = QLabel(label)
        prompt.setWordWrap(True)
        layout.addWidget(prompt)
        name_edit = QLineEdit(str(default_name or "").strip())
        name_edit.setObjectName("freqPlannerNameEdit")
        name_edit.selectAll()
        layout.addWidget(name_edit)
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        name_edit.setFocus(Qt.OtherFocusReason)
        if dialog.exec() != QDialog.Accepted:
            return "", False
        return name_edit.text().strip(), True

    def _source_sets(self, key: str) -> List[Dict[str, Any]]:
        category = HF_DAILY_SOURCE_CATEGORY if key == HF_DAILY_SOURCE_SETS_KEY else HF_NET_SOURCE_CATEGORY
        return source_sets_for_category(self.settings, key, category)

    def _selected_source_set_id(self, settings_key: str) -> str:
        return selected_source_set_id(self.settings, settings_key)

    def _refresh_source_combo(self, combo: QComboBox, sets_key: str, selected_key: str) -> None:
        selected = self._selected_source_set_id(selected_key)
        combo.blockSignals(True)
        combo.clear()
        combo.addItem("Live Current", LIVE_SOURCE_SET_ID)
        for row in self._source_sets(sets_key):
            set_id = str(row.get("id") or "").strip()
            if not set_id:
                continue
            combo.addItem(str(row.get("name") or set_id), set_id)
        idx = combo.findData(selected)
        combo.setCurrentIndex(idx if idx >= 0 else 0)
        combo.blockSignals(False)

    def _refresh_source_set_controls(self) -> None:
        if not hasattr(self, "hf_daily_source_combo"):
            return
        self._refresh_source_combo(self.hf_daily_source_combo, HF_DAILY_SOURCE_SETS_KEY, SELECTED_HF_DAILY_SOURCE_SET_KEY)
        self._refresh_source_combo(self.hf_net_source_combo, HF_NET_SOURCE_SETS_KEY, SELECTED_HF_NET_SOURCE_SET_KEY)

    def _on_source_set_selected(self, *_args: Any) -> None:
        if hasattr(self, "hf_daily_source_combo"):
            self.settings.set(SELECTED_HF_DAILY_SOURCE_SET_KEY, str(self.hf_daily_source_combo.currentData() or LIVE_SOURCE_SET_ID))
        if hasattr(self, "hf_net_source_combo"):
            self.settings.set(SELECTED_HF_NET_SOURCE_SET_KEY, str(self.hf_net_source_combo.currentData() or LIVE_SOURCE_SET_ID))
        self.rebuild_table()

    def _source_set_row_by_id(self, sets_key: str, set_id: str) -> Optional[Dict[str, Any]]:
        target = str(set_id or "").strip()
        if not target or target == LIVE_SOURCE_SET_ID:
            return None
        category = HF_DAILY_SOURCE_CATEGORY if sets_key == HF_DAILY_SOURCE_SETS_KEY else HF_NET_SOURCE_CATEGORY
        return source_set_row_by_id_for_category(self.settings, sets_key, category, target)

    def _source_selection_summary(self) -> str:
        hf_id = self._selected_source_set_id(SELECTED_HF_DAILY_SOURCE_SET_KEY)
        net_id = self._selected_source_set_id(SELECTED_HF_NET_SOURCE_SET_KEY)
        hf_row = self._source_set_row_by_id(HF_DAILY_SOURCE_SETS_KEY, hf_id)
        net_row = self._source_set_row_by_id(HF_NET_SOURCE_SETS_KEY, net_id)
        hf_label = str((hf_row or {}).get("name") or "Live Current")
        net_label = str((net_row or {}).get("name") or "Live Current")
        return f"HF Daily: {hf_label}; HF Nets: {net_label}"

    @staticmethod
    def _normalize_condition_levels(value: Any) -> str:
        raw = str(value or "").strip().upper()
        if not raw or raw == "ALL":
            return "ALL"
        vals: List[int] = []
        for token in raw.replace(";", ",").replace("|", ",").split(","):
            token = token.strip()
            if not token:
                continue
            if token == "ALL":
                return "ALL"
            try:
                lvl = int(token)
            except Exception:
                continue
            if 1 <= lvl <= 5:
                vals.append(lvl)
        if not vals:
            return "ALL"
        return ",".join(str(v) for v in sorted(set(vals)))

    def _condition_level_map(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        rows = self.settings.get("operating_groups", []) or []
        if not isinstance(rows, list):
            return out
        for row in rows:
            if not isinstance(row, dict):
                continue
            group = str(row.get("group", "") or "").strip().upper()
            if not group:
                continue
            if not bool(row.get("use_condition_levels", False)):
                continue
            try:
                level = int(row.get("condition_level", 0) or 0)
            except Exception:
                level = 0
            if not (1 <= level <= 5):
                continue
            prev = out.get(group)
            if prev is None or level < prev:
                out[group] = level
        return out

    def _configured_operating_group_names(self) -> Set[str]:
        groups: Set[str] = set()
        rows = self.settings.get("operating_groups", []) or []
        if not isinstance(rows, list):
            return groups
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in ("group", "name", "group_name"):
                value = str(row.get(key) or "").strip().upper()
                if value:
                    groups.add(value)
                    break
        return groups

    def _row_group_name(self, row: Mapping[str, Any]) -> str:
        return str(row.get("group_name") or row.get("group") or row.get("primary_js8call_group") or "").strip().upper()

    def _filter_rows_to_configured_groups(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        configured = self._configured_operating_group_names()
        if not configured:
            return []
        return [dict(row) for row in rows if self._row_group_name(row) in configured]

    def _group_is_configured(self, group_name: Any) -> bool:
        group = str(group_name or "").strip().upper()
        return bool(group and group in self._configured_operating_group_names())

    def _configured_group_help_text(self) -> str:
        groups = sorted(self._configured_operating_group_names())
        if not groups:
            return "No Operating Groups are configured in Settings."
        return "Configured Operating Groups: " + ", ".join(groups)

    @staticmethod
    def _json_ref_count(value: Any) -> int:
        if isinstance(value, (list, tuple, set)):
            return len([item for item in value if str(item or "").strip()])
        if value in (None, ""):
            return 0
        try:
            parsed = json.loads(str(value))
        except Exception:
            parsed = [part.strip() for part in str(value).split(",") if part.strip()]
        if not isinstance(parsed, list):
            return 0
        return len([item for item in parsed if str(item or "").strip()])

    def _frequency_plan_summary_text(self, profile: Optional[Dict[str, Any]]) -> str:
        if not isinstance(profile, dict):
            return "No Frequency Plan selected."
        status = str(profile.get("status", "saved") or "saved").strip().title()
        category = str(profile.get("category", "normal") or "normal").replace("_", " ").title()
        source_count = self._json_ref_count(profile.get("source_refs_json"))
        schedule_count = self._json_ref_count(profile.get("schedule_refs_json"))
        frequency_count = self._json_ref_count(profile.get("frequency_refs_json"))
        group_count = self._json_ref_count(profile.get("group_refs_json"))
        refs: List[str] = []
        if source_count:
            refs.append(f"{source_count} source{'s' if source_count != 1 else ''}")
        if schedule_count:
            refs.append(f"{schedule_count} schedule ref{'s' if schedule_count != 1 else ''}")
        if frequency_count:
            refs.append(f"{frequency_count} frequency ref{'s' if frequency_count != 1 else ''}")
        if group_count:
            refs.append(f"{group_count} group ref{'s' if group_count != 1 else ''}")
        ref_text = ", ".join(refs) if refs else "No source refs yet"
        return f"{status} {category} plan. {ref_text}."

    def _refresh_plan_workspace_header(self) -> None:
        if not hasattr(self, "frequency_plan_combo"):
            return
        selected_id = self.frequency_plan_combo.currentData()
        try:
            plans = list(self.plan_context_service.store.list_frequency_plans())
        except Exception:
            plans = []
        plans = [
            plan
            for plan in plans
            if str(plan.get("category") or "").strip().lower()
            not in {HF_DAILY_SOURCE_CATEGORY, HF_NET_SOURCE_CATEGORY}
        ]
        context = self.plan_context_service.context_for_tab("freqplanner", refresh=True)
        context_plan_id = 0
        if context is not None and context.frequency_plan_id:
            try:
                context_plan_id = int(str(context.frequency_plan_id).split("_")[-1])
            except Exception:
                context_plan_id = 0
        preferred_id = int(selected_id or 0) or context_plan_id
        self.frequency_plan_combo.blockSignals(True)
        self.frequency_plan_combo.clear()
        for plan in plans:
            plan_id = int(plan.get("id", 0) or 0)
            label = str(plan.get("name", "") or f"Frequency Plan #{plan_id}")
            status = str(plan.get("status", "saved") or "saved").strip().lower()
            if status == "draft":
                label = f"{label} (draft)"
            self.frequency_plan_combo.addItem(label, plan_id)
        if preferred_id:
            idx = self.frequency_plan_combo.findData(preferred_id)
            if idx >= 0:
                self.frequency_plan_combo.setCurrentIndex(idx)
        self.frequency_plan_combo.blockSignals(False)
        self._update_frequency_plan_summary()
        if hasattr(self, "assign_plan_btn"):
            self.assign_plan_btn.setEnabled(self._selected_frequency_plan_row() is not None)

    def _selected_frequency_plan_row(self) -> Optional[Dict[str, Any]]:
        if not hasattr(self, "frequency_plan_combo"):
            return None
        selected_id = int(self.frequency_plan_combo.currentData() or 0)
        if selected_id <= 0:
            return None
        try:
            return self.plan_context_service.store.get_frequency_plan(selected_id)
        except Exception:
            return None

    def _update_frequency_plan_summary(self) -> None:
        if not hasattr(self, "frequency_plan_summary_label"):
            return
        self.frequency_plan_summary_label.setText(self._frequency_plan_summary_text(self._selected_frequency_plan_row()))

    def _on_frequency_plan_selected(self, *_args: Any) -> None:
        self._update_frequency_plan_summary()
        if hasattr(self, "assign_plan_btn"):
            self.assign_plan_btn.setEnabled(self._selected_frequency_plan_row() is not None)
        if self._planner_view_mode() == "operational":
            self.rebuild_table()

    def _on_assign_plan_clicked(self) -> None:
        plan = self._selected_frequency_plan_row()
        if not plan:
            self.frequency_plan_action_hint_label.setText("Select or save a Frequency Plan before assigning it to radios.")
            return
        self.frequency_plan_action_hint_label.setText(
            f"Assign '{str(plan.get('name') or 'Frequency Plan')}' from Settings > Radio Profiles > Schedule Assignment. "
            "RF Safety Guard checks run before the assignment is saved."
        )

    def _build_blended_projection(self) -> BlendedScheduleProjection:
        hf_sched, net_sched, sop_sched, policy_rows = self._load_schedules()
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        week_sunday = self._week_start_sunday_utc(now_utc)
        return build_blended_schedule_projection(
            hf_sched,
            net_sched,
            sop_sched,
            policy_rows,
            week_start_utc=week_sunday,
        )

    def _build_operational_projection(self) -> OperationalDayProjection:
        hf_sched, net_sched, sop_sched, policy_rows = self._load_schedules()
        hf_sched = self._filter_rows_to_configured_groups(hf_sched)
        net_sched = self._filter_rows_to_configured_groups(net_sched)
        sop_sched = self._filter_rows_to_configured_groups(sop_sched)
        net_resources = self._filter_rows_to_configured_groups(self._load_net_resources_from_db() or [])
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        week_sunday = self._week_start_sunday_utc(now_utc)
        return build_operational_day_projection(
            hf_sched,
            net_sched,
            sop_sched,
            net_resources,
            policy_rows,
            week_start_utc=week_sunday,
        )

    def _selected_sop_schedule_plan_row(self) -> Optional[Dict[str, Any]]:
        plan = self._selected_frequency_plan_row()
        if not isinstance(plan, dict):
            return None
        category = str(plan.get("category") or "").strip().lower()
        return plan if category == "sop_schedule" else None

    def _schedule_refs_from_plan_row(self, plan: Mapping[str, Any]) -> List[Dict[str, Any]]:
        raw = plan.get("schedule_refs_json", plan.get("schedule_refs", "[]"))
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = []
        else:
            parsed = raw
        refs: List[Dict[str, Any]] = []
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    refs.append(dict(item))
        return refs

    def _build_selected_sop_plan_projection(self, week_sunday: datetime.date) -> Optional[OperationalDayProjection]:
        plan = self._selected_sop_schedule_plan_row()
        if not plan:
            return None
        refs = self._filter_rows_to_configured_groups(self._schedule_refs_from_plan_row(plan))
        return build_operational_day_projection_from_refs(refs, week_start_utc=week_sunday)

    @staticmethod
    def _plan_context_radio_id(context: Any) -> int:
        raw = getattr(context, "radio_profile_id", "") if context is not None else ""
        try:
            return int(str(raw).split("_")[-1])
        except Exception:
            return 0

    def _default_save_plan_name(self, projection: BlendedScheduleProjection) -> str:
        source_bits = []
        counts = projection.source_counts
        if counts.get("HF", 0):
            source_bits.append("HF Daily")
        if counts.get("NET", 0):
            source_bits.append("HF Nets")
        if counts.get("SOP", 0):
            source_bits.append("SOP")
        source_text = " + ".join(source_bits) if source_bits else "Schedule"
        return f"{source_text} Plan {datetime.datetime.now().strftime('%Y-%m-%d')}"

    def _projection_review_text(self, projection: BlendedScheduleProjection) -> str:
        counts = projection.source_counts
        effective_count = len(projection.effective_segments)
        lines = [
            "Review blended schedule before saving:",
            self._source_selection_summary(),
            f"HF Daily rows considered: {counts.get('HF', 0)}",
            f"HF Net rows considered: {counts.get('NET', 0)}",
            f"SOP rows considered: {counts.get('SOP', 0)}",
            f"Effective windows to save: {effective_count}",
        ]
        preview = projection.effective_segments[:8]
        if preview:
            lines.append("")
            lines.append("First saved windows:")
            for segment in preview:
                label = segment.net_name or segment.group_name or segment.profile_name or segment.band or segment.source
                band_freq = f"{segment.band} {segment.frequency}".strip()
                lines.append(
                    f"- {segment.day_utc} {segment.start_utc}-{segment.end_utc} "
                    f"{segment.source} {label} {band_freq}".strip()
                )
            if len(projection.effective_segments) > len(preview):
                lines.append(f"- +{len(projection.effective_segments) - len(preview)} more")
        return "\n".join(lines)

    def _operational_projection_review_text(self, projection: OperationalDayProjection) -> str:
        counts = projection.source_counts
        refs = projection.schedule_refs()
        lines = [
            "Review SOP Schedule Plan before saving:",
            self._source_selection_summary(),
            f"Operational lanes: {len(projection.lanes)}",
            f"HF Daily entries: {counts.get('HF', 0)}",
            f"HF Net entries: {counts.get('NET', 0)}",
            f"Net Resource entries: {counts.get('NET_RESOURCE', 0)}",
            f"SOP entries: {counts.get('SOP', 0)}",
            f"Saved operational entries: {len(refs)}",
        ]
        lanes = projection.lanes[:8]
        if lanes:
            lines.append("")
            lines.append("Lanes:")
            for lane in lanes:
                lines.append(f"- {lane.lane_label}: {len(lane.entries)} entr{'y' if len(lane.entries) == 1 else 'ies'}")
            if len(projection.lanes) > len(lanes):
                lines.append(f"- +{len(projection.lanes) - len(lanes)} more lane(s)")
        preview = refs[:8]
        if preview:
            lines.append("")
            lines.append("First saved entries:")
            for row in preview:
                label = str(row.get("action_label") or row.get("net_name") or row.get("profile_name") or row.get("group_name") or row.get("source") or "").strip()
                band_freq = f"{str(row.get('band') or '').strip()} {str(row.get('frequency') or '').strip()}".strip()
                lines.append(
                    f"- {row.get('lane_label')} | {row.get('day_utc')} {row.get('start_utc')}-{row.get('end_utc')} "
                    f"{row.get('source')} {label} {band_freq}".strip()
                )
            if len(refs) > len(preview):
                lines.append(f"- +{len(refs) - len(preview)} more")
        return "\n".join(lines)

    def _rf_guard_preflight_for_plan(self, plan_payload: Dict[str, Any]) -> Dict[str, Any]:
        radio_lane_ids = self._radio_lane_ids_for_plan(plan_payload)
        if radio_lane_ids:
            validations: List[Dict[str, Any]] = []
            for radio_id in radio_lane_ids:
                subset_payload = self._plan_payload_for_radio_lane(plan_payload, radio_id)
                validation = self.plan_context_service.store.validate_frequency_plan_for_device(radio_id, subset_payload)
                validations.append(validation)
            sibling_validation = self._sibling_radio_lane_guard_validation(plan_payload, radio_lane_ids)
            if sibling_validation:
                validations.append(sibling_validation)
            return self._merge_rf_guard_validations(validations)
        context = self.plan_context_service.context_for_tab("freqplanner", refresh=True)
        radio_id = self._plan_context_radio_id(context)
        if radio_id <= 0:
            return {
                "state": "off",
                "rf_guard_validation": "not_enforced",
                "messages": ["RF Guard preflight skipped because no radio context is selected."],
            }
        return self.plan_context_service.store.validate_frequency_plan_for_device(radio_id, plan_payload)

    def _radio_lane_ids_for_plan(self, plan_payload: Dict[str, Any]) -> List[int]:
        ids: List[int] = []
        for ref in self._schedule_ref_mappings(plan_payload):
            lane_key = str(ref.get("lane_key") or "").strip()
            radio_id = 0
            if lane_key.startswith("radio:"):
                try:
                    radio_id = int(lane_key.split(":", 1)[1] or 0)
                except Exception:
                    radio_id = 0
            if radio_id <= 0:
                try:
                    radio_id = int(
                        ref.get("radio_id")
                        or ref.get("device_profile_id")
                        or ref.get("target_device_profile_id")
                        or 0
                    )
                except Exception:
                    radio_id = 0
            if radio_id > 0:
                ids.append(radio_id)
        return sorted(set(ids))

    def _schedule_ref_mappings(self, plan_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw = plan_payload.get("schedule_refs", plan_payload.get("schedule_refs_json", []))
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = []
        refs: List[Dict[str, Any]] = []
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    refs.append(dict(item))
        return refs

    def _plan_payload_for_radio_lane(self, plan_payload: Dict[str, Any], radio_id: int) -> Dict[str, Any]:
        lane_key = f"radio:{int(radio_id)}"
        refs = [
            ref
            for ref in self._schedule_ref_mappings(plan_payload)
            if str(ref.get("lane_key") or "").strip() == lane_key
            or self._coerce_positive_int(
                ref.get("radio_id") or ref.get("device_profile_id") or ref.get("target_device_profile_id")
            ) == int(radio_id)
        ]
        subset = dict(plan_payload)
        subset["name"] = f"{str(plan_payload.get('name') or 'SOP Schedule Plan').strip()} / Radio {radio_id}"
        subset["schedule_refs"] = refs
        subset["schedule_refs_json"] = json.dumps(refs)
        subset["frequency_refs"] = self._frequency_refs_for_schedule_refs(refs)
        subset["frequency_refs_json"] = json.dumps(subset["frequency_refs"])
        subset["group_refs"] = list(
            dict.fromkeys(str(ref.get("group_name") or "").strip().upper() for ref in refs if str(ref.get("group_name") or "").strip())
        )
        subset["group_refs_json"] = json.dumps(subset["group_refs"])
        return subset

    def _frequency_refs_for_schedule_refs(self, refs: List[Dict[str, Any]]) -> List[str]:
        out: List[str] = []
        for ref in refs:
            band = str(ref.get("band") or "").strip().upper()
            freq = str(ref.get("frequency") or ref.get("freq") or "").strip()
            if band and freq:
                out.append(f"{band}:{freq}")
            elif band:
                out.append(band)
            elif freq:
                out.append(freq)
        return list(dict.fromkeys(out))

    def _coerce_positive_int(self, value: Any) -> int:
        try:
            number = int(value or 0)
        except Exception:
            number = 0
        return number if number > 0 else 0

    def _sibling_radio_lane_guard_validation(
        self,
        plan_payload: Dict[str, Any],
        radio_lane_ids: List[int],
    ) -> Optional[Dict[str, Any]]:
        if len(radio_lane_ids) < 2:
            return None
        store = self.plan_context_service.store
        devices = {
            int(radio_id): store.get_device_profile(int(radio_id))
            for radio_id in radio_lane_ids
            if int(radio_id) > 0
        }
        lane_payloads = {
            int(radio_id): self._plan_payload_for_radio_lane(plan_payload, int(radio_id))
            for radio_id in radio_lane_ids
            if int(radio_id) > 0
        }
        warnings: List[str] = []
        blocked: List[str] = []
        ids = sorted(lane_payloads)
        for left_index, left_id in enumerate(ids):
            left_device = devices.get(left_id) or {}
            left_payload = lane_payloads[left_id]
            for right_id in ids[left_index + 1 :]:
                right_device = devices.get(right_id) or {}
                right_payload = lane_payloads[right_id]
                for band in self._sibling_overlap_bands(left_payload, right_payload):
                    group = self._normalized_group(left_device.get("band_overlap_guard_group"))
                    if not group or group != self._normalized_group(right_device.get("band_overlap_guard_group")):
                        continue
                    mode = self._stricter_guard_mode(
                        left_device.get("band_overlap_guard_mode"),
                        right_device.get("band_overlap_guard_mode"),
                    )
                    message = (
                        f"{left_device.get('name') or f'Radio {left_id}'} and "
                        f"{right_device.get('name') or f'Radio {right_id}'} both have SOP Schedule Plan lanes "
                        f"on {band} in Prevent Band Overlap group {group}."
                    )
                    (blocked if mode == "block" else warnings).append(message)
                close_frequency_message = self._sibling_close_frequency_message(
                    left_device,
                    right_device,
                    left_payload,
                    right_payload,
                    left_id,
                    right_id,
                )
                if close_frequency_message:
                    mode, message = close_frequency_message
                    (blocked if mode == "block" else warnings).append(message)
        if not warnings and not blocked:
            return None
        return {
            "state": "blocked" if blocked else "warning",
            "rf_guard_validation": "enforced",
            "messages": warnings + blocked,
            "warnings": warnings,
            "blocked": blocked,
        }

    def _sibling_close_frequency_message(
        self,
        left_device: Dict[str, Any],
        right_device: Dict[str, Any],
        left_payload: Dict[str, Any],
        right_payload: Dict[str, Any],
        left_id: int,
        right_id: int,
    ) -> Optional[Tuple[str, str]]:
        group = self._normalized_group(left_device.get("advanced_frequency_guard_group"))
        if not group or group != self._normalized_group(right_device.get("advanced_frequency_guard_group")):
            return None
        left_window = self._coerce_nonnegative_int(left_device.get("advanced_frequency_guard_window_hz"))
        right_window = self._coerce_nonnegative_int(right_device.get("advanced_frequency_guard_window_hz"))
        threshold = max(left_window, right_window)
        if threshold <= 0 or not self._schedule_refs_overlap(left_payload, right_payload):
            return None
        left_freqs = self._frequency_hz_values_for_plan(left_payload)
        right_freqs = self._frequency_hz_values_for_plan(right_payload)
        for left_freq in left_freqs:
            for right_freq in right_freqs:
                if abs(left_freq - right_freq) <= threshold:
                    mode = self._stricter_guard_mode(
                        left_device.get("advanced_frequency_guard_mode"),
                        right_device.get("advanced_frequency_guard_mode"),
                    )
                    return (
                        mode,
                        f"{left_device.get('name') or f'Radio {left_id}'} and "
                        f"{right_device.get('name') or f'Radio {right_id}'} have SOP Schedule Plan lanes "
                        f"within {threshold} Hz in Advanced Guard group {group} "
                        f"({left_freq} Hz vs {right_freq} Hz).",
                    )
        return None

    def _sibling_overlap_bands(self, left_payload: Dict[str, Any], right_payload: Dict[str, Any]) -> List[str]:
        bands: List[str] = []
        for left_ref in self._schedule_ref_mappings(left_payload):
            for right_ref in self._schedule_ref_mappings(right_payload):
                left_band = str(left_ref.get("band") or "").strip().upper()
                right_band = str(right_ref.get("band") or "").strip().upper()
                if left_band and left_band == right_band and self._schedule_refs_overlap({"schedule_refs": [left_ref]}, {"schedule_refs": [right_ref]}):
                    bands.append(left_band)
        return sorted(set(bands))

    def _schedule_refs_overlap(self, left_payload: Dict[str, Any], right_payload: Dict[str, Any]) -> bool:
        for left_ref in self._schedule_ref_mappings(left_payload):
            for right_ref in self._schedule_ref_mappings(right_payload):
                if self._single_schedule_ref_overlap(left_ref, right_ref):
                    return True
        return False

    def _single_schedule_ref_overlap(self, left_ref: Dict[str, Any], right_ref: Dict[str, Any]) -> bool:
        left_start = self._parse_guard_hhmm(left_ref.get("start_utc") or left_ref.get("start"))
        left_end = self._parse_guard_hhmm(left_ref.get("end_utc") or left_ref.get("end"))
        right_start = self._parse_guard_hhmm(right_ref.get("start_utc") or right_ref.get("start"))
        right_end = self._parse_guard_hhmm(right_ref.get("end_utc") or right_ref.get("end"))
        if None in (left_start, left_end, right_start, right_end):
            return True
        left_segments = self._weekly_ref_segments(
            self._schedule_day_token(left_ref.get("day_utc") or left_ref.get("day")),
            int(left_start),
            int(left_end),
        )
        right_segments = self._weekly_ref_segments(
            self._schedule_day_token(right_ref.get("day_utc") or right_ref.get("day")),
            int(right_start),
            int(right_end),
        )
        for left_segment in left_segments:
            for right_segment in right_segments:
                if left_segment[0] == right_segment[0] and left_segment[1] < right_segment[2] and right_segment[1] < left_segment[2]:
                    return True
        return False

    def _weekly_ref_segments(self, day_token: str, start_minute: int, end_minute: int) -> List[Tuple[int, int, int]]:
        day_indices = range(7) if day_token == "ALL" else [self._day_index(day_token)]
        segments: List[Tuple[int, int, int]] = []
        for day_index in day_indices:
            if day_index < 0:
                continue
            if end_minute <= start_minute:
                segments.append((day_index, start_minute, 24 * 60))
                segments.append(((day_index + 1) % 7, 0, end_minute))
            else:
                segments.append((day_index, start_minute, end_minute))
        return segments

    def _day_index(self, day_token: str) -> int:
        normalized = str(day_token or "").strip().upper()
        for idx, day in enumerate(DAY_NAMES):
            if normalized == day.upper():
                return idx
        return -1

    def _frequency_hz_values_for_plan(self, plan_payload: Dict[str, Any]) -> List[int]:
        values: List[int] = []
        for ref in self._schedule_ref_mappings(plan_payload):
            for key in ("frequency_hz", "freq_hz", "frequency", "freq"):
                parsed = self._parse_frequency_hz(ref.get(key))
                if parsed:
                    values.append(parsed)
        return list(dict.fromkeys(values))

    def _parse_frequency_hz(self, value: Any) -> Optional[int]:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            if number <= 0:
                return None
            if number < 1000:
                return int(round(number * 1_000_000))
            if number < 1_000_000:
                return int(round(number * 1000))
            return int(round(number))
        text = str(value or "").strip().upper().replace(" ", "")
        if not text:
            return None
        match = re.search(r"(\d{1,3}\.\d+|\d+(?:\.\d+)?)(MHZ|KHZ|HZ)?", text)
        if not match:
            return None
        try:
            number = float(match.group(1))
        except Exception:
            return None
        suffix = match.group(2) or ""
        if suffix == "HZ":
            return int(round(number))
        if suffix == "KHZ":
            return int(round(number * 1000))
        if suffix == "MHZ" or number < 1000:
            return int(round(number * 1_000_000))
        return None

    def _parse_guard_hhmm(self, value: Any) -> Optional[int]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            hour_text, minute_text = text.split(":", 1)
            hour = int(hour_text)
            minute = int(minute_text)
        except Exception:
            return None
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour * 60 + minute
        return None

    def _schedule_day_token(self, value: Any) -> str:
        text = re.sub(r"[^A-Z]", "", str(value or "ALL").strip().upper())
        if not text or text in {"ALL", "DAILY", "EVERYDAY"}:
            return "ALL"
        for day in DAY_NAMES:
            if text.startswith(day[:3].upper()):
                return day.upper()
        return text

    def _normalized_group(self, value: Any) -> str:
        text = str(value or "").strip().upper()
        return re.sub(r"\s+", " ", text)

    def _guard_mode(self, value: Any) -> str:
        text = str(value or "warn").strip().lower().replace("_", "-").replace(" ", "-")
        aliases = {"warn-only": "warn", "warning": "warn", "blocked": "block", "confirmation": "confirm"}
        text = aliases.get(text, text)
        return text if text in {"warn", "confirm", "block"} else "warn"

    def _stricter_guard_mode(self, left: Any, right: Any) -> str:
        order = {"warn": 0, "confirm": 1, "block": 2}
        left_mode = self._guard_mode(left)
        right_mode = self._guard_mode(right)
        return left_mode if order[left_mode] >= order[right_mode] else right_mode

    def _coerce_nonnegative_int(self, value: Any) -> int:
        try:
            parsed = int(float(str(value).strip()))
        except Exception:
            parsed = 0
        return max(0, parsed)

    def _merge_rf_guard_validations(self, validations: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not validations:
            return {"state": "ok", "rf_guard_validation": "enforced", "messages": ["RF guard validation completed."]}
        messages: List[str] = []
        warnings: List[str] = []
        blocked: List[str] = []
        for validation in validations:
            radio_id = int(validation.get("device_profile_id") or 0)
            prefix = f"Radio {radio_id}: " if radio_id > 0 else ""
            for item in validation.get("warnings", []) or []:
                warnings.append(prefix + str(item))
            for item in validation.get("blocked", []) or []:
                blocked.append(prefix + str(item))
            for item in validation.get("messages", []) or []:
                text = str(item or "").strip()
                if text and text not in warnings and text not in blocked:
                    messages.append(prefix + text)
        state = "blocked" if blocked else "warning" if warnings else "ok"
        return {
            "state": state,
            "rf_guard_validation": "enforced",
            "messages": messages + warnings + blocked,
            "warnings": warnings,
            "blocked": blocked,
            "radio_lane_ids": [int(v.get("device_profile_id") or 0) for v in validations if int(v.get("device_profile_id") or 0) > 0],
        }

    def _save_plan_payload_with_guard(
        self,
        plan_payload: Dict[str, Any],
        *,
        schedule_count: int,
        success_kind: str = "Frequency Plan",
    ) -> Optional[Dict[str, Any]]:
        try:
            validation = self._rf_guard_preflight_for_plan(plan_payload)
        except ValueError as exc:
            self.frequency_plan_action_hint_label.setText(f"RF Guard blocked this plan before save: {exc}")
            return None
        except Exception as exc:
            log.exception("FreqPlanner: RF Guard preflight failed.")
            validation = {
                "state": "warning",
                "messages": [f"RF Guard preflight could not complete: {exc}"],
            }
        state = str(validation.get("state") or "").strip().lower()
        messages = [str(item) for item in validation.get("messages", []) if str(item or "").strip()]
        if state == "blocked":
            self.frequency_plan_action_hint_label.setText(
                "RF Guard blocked this Frequency Plan before save. " + (messages[0] if messages else "")
            )
            return None
        if state == "warning":
            response = QMessageBox.question(
                self,
                "RF Guard Warning",
                "RF Guard found warnings for the selected radio context.\n\n"
                + "\n".join(messages[:5])
                + "\n\nSave the Frequency Plan anyway?",
                QMessageBox.Save | QMessageBox.Cancel,
                QMessageBox.Cancel,
            )
            if response != QMessageBox.Save:
                return None
        if state in {"off", "not_enforced"} or str(validation.get("rf_guard_validation") or "").strip().lower() == "not_enforced":
            response = QMessageBox.question(
                self,
                "RF Guard Preflight Skipped",
                "No radio context is selected, so FreqPlanner could not run assignment-specific RF Safety Guard checks.\n\n"
                "Save this Frequency Plan anyway? RF Safety Guard will run when the plan is assigned to a radio.",
                QMessageBox.Save | QMessageBox.Cancel,
                QMessageBox.Cancel,
            )
            if response != QMessageBox.Save:
                return None
        try:
            saved = self.plan_context_service.store.save_frequency_plan(plan_payload)
        except Exception as exc:
            log.exception("FreqPlanner: failed saving Frequency Plan.")
            self.frequency_plan_action_hint_label.setText(f"Unable to save Frequency Plan: {exc}")
            return None
        self.plan_context_service.invalidate()
        self._refresh_plan_workspace_header()
        saved_id = int(saved.get("id", 0) or 0)
        if saved_id:
            idx = self.frequency_plan_combo.findData(saved_id)
            if idx >= 0:
                self.frequency_plan_combo.setCurrentIndex(idx)
        if state in {"", "ok"}:
            guard_text = "RF Guard preflight passed."
        elif state in {"off", "not_enforced"} or str(validation.get("rf_guard_validation") or "").strip().lower() == "not_enforced":
            guard_text = "RF Guard preflight skipped; assignment checks still required."
        else:
            guard_text = "RF Guard warning accepted."
        self.frequency_plan_action_hint_label.setText(
            f"Saved {success_kind} '{str(saved.get('name') or plan_payload.get('name') or 'Plan')}' "
            f"with {schedule_count} effective window(s). {guard_text}"
        )
        return saved

    def _on_save_plan_clicked(self) -> None:
        try:
            projection = self._build_blended_projection()
        except Exception as exc:
            log.exception("FreqPlanner: failed building blended projection.")
            self.frequency_plan_action_hint_label.setText(f"Unable to build the blended schedule projection: {exc}")
            return
        if not projection.effective_segments:
            self.frequency_plan_action_hint_label.setText(
                "No effective HF Daily, HF Nets, or SOP windows are available to save."
            )
            return
        default_name = self._default_save_plan_name(projection)
        name, ok = self._prompt_for_name("Save Frequency Plan", "Name this Frequency Plan:", default_name)
        if not ok:
            return
        name = str(name or "").strip()
        if not name:
            self.frequency_plan_action_hint_label.setText("Enter a clear Frequency Plan name before saving.")
            return
        schedule_refs = projection.schedule_refs()
        source_refs = projection.source_refs() + selected_source_schedule_dependency_refs(self.settings)
        plan_payload: Dict[str, Any] = {
            "name": name,
            "status": "saved",
            "category": "normal",
            "description": "Saved from FreqPlanner blended HF Daily + HF Nets + SOP projection.",
            "source_refs": source_refs,
            "schedule_refs": schedule_refs,
            "frequency_refs": projection.frequency_refs(),
            "group_refs": projection.group_refs(),
            "notes": (
                f"FreqPlanner blended projection saved {datetime.datetime.now(datetime.timezone.utc).isoformat()}; "
                f"{self._source_selection_summary()}"
            ),
        }
        self._save_plan_payload_with_guard(
            plan_payload,
            schedule_count=len(schedule_refs),
            success_kind="Frequency Plan",
        )

    def _on_save_sop_plan_clicked(self) -> None:
        try:
            projection = self._build_operational_projection()
        except Exception as exc:
            log.exception("FreqPlanner: failed building SOP Schedule Plan projection.")
            self.frequency_plan_action_hint_label.setText(f"Unable to build the SOP Schedule Plan projection: {exc}")
            return
        refs = projection.schedule_refs()
        if not refs:
            self.frequency_plan_action_hint_label.setText(
                "No HF Daily, HF Nets, Net Resources, or SOP entries are available to save as an SOP Schedule Plan."
            )
            return
        default_name = f"SOP Schedule Plan {datetime.datetime.now().strftime('%Y-%m-%d')}"
        name, ok = self._prompt_for_name("Save SOP Schedule Plan", "Name this SOP Schedule Plan:", default_name)
        if not ok:
            return
        name = str(name or "").strip()
        if not name:
            self.frequency_plan_action_hint_label.setText("Enter a clear SOP Schedule Plan name before saving.")
            return
        plan_payload = projection.to_frequency_plan_payload(
            name,
            description=f"Saved from FreqPlanner operational SOP Schedule Plan projection. {self._source_selection_summary()}",
        )
        self._save_plan_payload_with_guard(
            plan_payload,
            schedule_count=len(refs),
            success_kind="SOP Schedule Plan",
        )

    @classmethod
    def _condition_level_match(cls, condition_levels: str, group_level: Optional[int]) -> bool:
        normalized = cls._normalize_condition_levels(condition_levels)
        if normalized == "ALL":
            return True
        if group_level is None:
            return True
        allowed: Set[int] = set()
        for token in normalized.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                lvl = int(token)
            except Exception:
                continue
            if 1 <= lvl <= 5:
                allowed.add(lvl)
        if not allowed:
            return True
        return int(group_level) in allowed

    def _load_live_schedules(self) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict[str, Any]]]:
        data = self.settings.all()

        # Try DB-backed schedules first
        hf_db = self._load_hf_from_db()
        net_db = self._load_net_from_db()
        sop_db = self._load_sop_layer_from_db()
        policy_db = self._load_net_sop_policies_from_db()

        hf = hf_db if hf_db is not None else data.get("hf_schedule") or data.get("daily_schedule") or []
        net = net_db if net_db is not None else data.get("net_schedule") or []
        sop = sop_db if sop_db is not None else []
        policies = policy_db if policy_db is not None else []
        if not isinstance(hf, list):
            hf = []
        if not isinstance(net, list):
            net = []
        if not isinstance(sop, list):
            sop = []
        if not isinstance(policies, list):
            policies = []
        return hf, net, sop, policies

    def _load_schedules(self) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict[str, Any]]]:
        hf, net, sop, policies = self._load_live_schedules()
        selected_hf = self._selected_source_set_id(SELECTED_HF_DAILY_SOURCE_SET_KEY)
        selected_net = self._selected_source_set_id(SELECTED_HF_NET_SOURCE_SET_KEY)
        hf_set = self._source_set_row_by_id(HF_DAILY_SOURCE_SETS_KEY, selected_hf)
        net_set = self._source_set_row_by_id(HF_NET_SOURCE_SETS_KEY, selected_net)
        if hf_set is not None:
            hf = [dict(row) for row in hf_set.get("rows", []) if isinstance(row, dict)]
        if net_set is not None:
            net = [dict(row) for row in net_set.get("rows", []) if isinstance(row, dict)]
        return hf, net, sop, policies

    @staticmethod
    def _policy_overlap(
        a_start: datetime.datetime,
        a_end: datetime.datetime,
        b_start: datetime.datetime,
        b_end: datetime.datetime,
    ) -> bool:
        return a_start < b_end and b_start < a_end

    @staticmethod
    def _parse_iso_utc(value: str) -> Optional[datetime.datetime]:
        txt = str(value or "").strip()
        if not txt:
            return None
        try:
            parsed = datetime.datetime.fromisoformat(txt.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=datetime.timezone.utc)
            return parsed.astimezone(datetime.timezone.utc)
        except Exception:
            return None

    def _load_net_sop_policies_from_db(self) -> Optional[List[Dict[str, Any]]]:
        conn: sqlite3.Connection | None = None
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            exists = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='sop_net_conflict_policy'"
            ).fetchone()
            if not exists:
                return []
            cur.execute(
                """
                SELECT
                    policy,
                    window_start_utc,
                    window_end_utc,
                    net_row_signature,
                    sop_row_signature
                FROM sop_net_conflict_policy
                WHERE COALESCE(active, 1) = 1
                ORDER BY COALESCE(updated_utc, '') DESC, id DESC
                """
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            for policy, start_utc, end_utc, net_sig, sop_sig in rows:
                pol = str(policy or "").strip().upper()
                if pol not in {"SOP_PRIORITY", "NET_PRIORITY"}:
                    continue
                start_dt = self._parse_iso_utc(str(start_utc or ""))
                end_dt = self._parse_iso_utc(str(end_utc or ""))
                if not isinstance(start_dt, datetime.datetime) or not isinstance(end_dt, datetime.datetime):
                    continue
                if end_dt <= start_dt:
                    continue
                out.append(
                    {
                        "policy": pol,
                        "start_utc": start_dt,
                        "end_utc": end_dt,
                        "net_row_signature": str(net_sig or "").strip(),
                        "sop_row_signature": str(sop_sig or "").strip(),
                    }
                )
            return out
        except Exception:
            return None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _effective_net_sop_policy_for_window(
        self,
        *,
        cell_start_utc: datetime.datetime,
        cell_end_utc: datetime.datetime,
        net_slices: List[Tuple[int, int, str, str]],
        sop_slices: List[Tuple[int, int, str, str]],
        policy_rows: List[Dict[str, Any]],
    ) -> str:
        saw_sop = False
        for n_start, n_end, _n_label, n_sig in net_slices:
            if n_end <= n_start:
                continue
            for s_start, s_end, _s_label, s_sig in sop_slices:
                if s_end <= s_start:
                    continue
                overlap_start_min = max(n_start, s_start)
                overlap_end_min = min(n_end, s_end)
                if overlap_end_min <= overlap_start_min:
                    continue
                overlap_start_utc = cell_start_utc + datetime.timedelta(minutes=overlap_start_min)
                overlap_end_utc = cell_start_utc + datetime.timedelta(minutes=overlap_end_min)
                for row in policy_rows:
                    if str(row.get("net_row_signature") or "") != str(n_sig or ""):
                        continue
                    if str(row.get("sop_row_signature") or "") != str(s_sig or ""):
                        continue
                    policy = str(row.get("policy") or "").strip().upper()
                    start_dt = row.get("start_utc")
                    end_dt = row.get("end_utc")
                    if not isinstance(start_dt, datetime.datetime) or not isinstance(end_dt, datetime.datetime):
                        continue
                    if not self._policy_overlap(overlap_start_utc, overlap_end_utc, start_dt, end_dt):
                        continue
                    if policy == "NET_PRIORITY":
                        return "NET_PRIORITY"
                    if policy == "SOP_PRIORITY":
                        saw_sop = True
        return "SOP_PRIORITY" if saw_sop else ""

    @staticmethod
    def _normalize_day_for_signature(day: str) -> str:
        raw = str(day or "").strip()
        if not raw:
            return "ALL"
        up = raw.upper()
        if up in {"ALL", "DAILY"}:
            return "ALL"
        for opt in DAY_NAMES:
            if up.startswith(opt[:3].upper()):
                return opt
        return raw

    @staticmethod
    def _normalize_recurrence_for_signature(recurrence: str) -> str:
        raw = str(recurrence or "Weekly").strip().upper()
        if raw == "MONTHLY":
            raw = "PERIODIC"
        if raw in {"DAILY", "PERIODIC", "BI-WEEKLY", "WEEKLY"}:
            return "Bi-Weekly" if raw == "BI-WEEKLY" else raw.title()
        return "Weekly"

    @staticmethod
    def _normalize_frequency_for_signature(value: Any) -> str:
        txt = str(value or "").strip()
        if not txt:
            return ""
        try:
            return f"{float(txt):.3f}"
        except Exception:
            return txt

    def _normalize_month_weeks_for_signature(self, value: Any) -> str:
        weeks = self._parse_month_weeks(str(value or ""))
        return ",".join(str(v) for v in weeks)

    def _net_row_signature(self, row: Dict[str, Any]) -> str:
        day = self._normalize_day_for_signature(str(row.get("day_utc") or "ALL"))
        recurrence = self._normalize_recurrence_for_signature(str(row.get("recurrence") or "Weekly"))
        biweekly = int(row.get("biweekly_offset_weeks") or 0)
        weeks = self._normalize_month_weeks_for_signature(row.get("month_weeks"))
        group = str(row.get("group_name") or "").strip().upper()
        band = str(row.get("band") or "").strip().upper()
        freq = self._normalize_frequency_for_signature(row.get("frequency"))
        start = str(row.get("start_utc") or "").strip()
        end = str(row.get("end_utc") or "").strip()
        net_name = str(row.get("net_name") or row.get("name") or "").strip().upper()
        return (
            f"NET|{group}|{band}|{freq}|{day}|{recurrence}|{biweekly}|"
            f"{weeks}|{start}|{end}|{net_name}"
        )

    def _sop_row_signature(self, row: Dict[str, Any]) -> str:
        day = self._normalize_day_for_signature(str(row.get("day_utc") or "ALL"))
        recurrence = self._normalize_recurrence_for_signature(str(row.get("recurrence") or "Weekly"))
        biweekly = int(row.get("biweekly_offset_weeks") or 0)
        weeks = self._normalize_month_weeks_for_signature(row.get("month_weeks"))
        group = str(row.get("group_name") or "").strip().upper()
        band = str(row.get("band") or "").strip().upper()
        freq = self._normalize_frequency_for_signature(row.get("frequency"))
        start = str(row.get("start_utc") or "").strip()
        end = str(row.get("end_utc") or "").strip()
        profile_id = int(row.get("sop_profile_id") or 0)
        layer_id = int(row.get("sop_layer_id") or row.get("id") or 0)
        return (
            f"SOP|{profile_id}|{layer_id}|{group}|{band}|{freq}|{day}|"
            f"{recurrence}|{biweekly}|{weeks}|{start}|{end}"
        )

    def _load_hf_from_db(self) -> Optional[List[Dict]]:
        """
        Load HF/daily schedule from config/freqinout.db if available.
        """
        try:
            db_path = get_config_dir() / "config" / "freqinout.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, day_utc, band, mode, vfo, frequency, start_utc, end_utc, group_name, auto_tune
                FROM daily_schedule_tab
                """
            )
            rows = cur.fetchall()
            conn.close()
            out = []
            for row_id, day_utc, band, mode, vfo, freq, start_utc, end_utc, group_name, auto_tune in rows:
                out.append(
                    {
                        "id": int(row_id or 0),
                        "source_row_id": int(row_id or 0),
                        "source_table": "daily_schedule_tab",
                        "day_utc": day_utc or "ALL",
                        "band": band or "",
                        "mode": mode or "",
                        "vfo": (vfo or "A").strip().upper() or "A",
                        "frequency": str(freq or ""),
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "group_name": group_name or "",
                        "auto_tune": bool(auto_tune),
                    }
                )
            return out
        except Exception:
            return None

    def _load_net_from_db(self) -> Optional[List[Dict]]:
        """
        Load net schedule from config/freqinout_nets.db if available.
        """
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            rows = []
            try:
                cur.execute(
                    """
                    SELECT id, day_utc, recurrence, biweekly_offset_weeks, month_weeks, band, mode, vfo, frequency,
                           start_utc, end_utc, early_checkin, primary_js8call_group, comment, net_name, group_name,
                           resource_id, target_scope, target_device_profile_id, target_operating_profile_id
                    FROM net_schedule_tab
                    """
                )
                rows = [("net_schedule_tab", *row) for row in cur.fetchall()]
            except Exception:
                rows = []
            # Fallback for older table shape created before row ids/resource targeting were loaded by FreqPlanner.
            if not rows:
                try:
                    cur.execute(
                        """
                    SELECT day_utc, recurrence, biweekly_offset_weeks, month_weeks, band, mode, vfo, frequency,
                           start_utc, end_utc, early_checkin, primary_js8call_group, comment, net_name, group_name
                    FROM net_schedule_tab
                    """
                    )
                    rows = [
                        (
                            "net_schedule_tab",
                            None,
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            month_weeks,
                            band,
                            mode,
                            vfo,
                            freq,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name,
                            group_name,
                            None,
                            "station",
                            None,
                            None,
                        )
                        for (
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            month_weeks,
                            band,
                            mode,
                            vfo,
                            freq,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name,
                            group_name,
                        ) in cur.fetchall()
                    ]
                except Exception:
                    rows = []
            # Fallback to legacy table if the richer table is empty/missing
            if not rows:
                try:
                    cur.execute(
                        """
                        SELECT id, day_utc, recurrence, biweekly_offset_weeks, band, mode, frequency,
                               start_utc, end_utc, early_checkin, primary_js8call_group, comment, net_name
                        FROM net_schedule
                        """
                    )
                    legacy = cur.fetchall()
                    # Pad legacy rows to align with expected tuple positions (insert vfo=None, group_name='')
                    rows = [
                        (
                            "net_schedule",
                            row_id,
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            "",
                            band,
                            mode,
                            None,
                            freq,
                            start_utc,
                            end_utc,
                             early_checkin,
                             primary_js8call_group,
                             comment,
                             net_name,
                             "",
                             None,
                             "station",
                             None,
                             None,
                         )
                         for (
                             row_id,
                             day_utc,
                             recurrence,
                            biweekly_offset_weeks,
                            band,
                            mode,
                            freq,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name,
                        ) in legacy
                    ]
                except Exception:
                    rows = []
            conn.close()
            out = []
            for (
                source_table,
                row_id,
                day_utc,
                recurrence,
                biweekly_offset_weeks,
                month_weeks,
                band,
                mode,
                vfo,
                freq,
                start_utc,
                end_utc,
                early_checkin,
                primary_js8call_group,
                comment,
                net_name,
                group_name,
                resource_id,
                target_scope,
                target_device_profile_id,
                target_operating_profile_id,
            ) in rows:
                out.append(
                    {
                        "id": int(row_id or 0),
                        "source_row_id": int(row_id or 0),
                        "source_table": str(source_table or "net_schedule_tab"),
                        "resource_id": int(resource_id) if resource_id not in (None, "") else None,
                        "day_utc": day_utc or "ALL",
                        "recurrence": recurrence or "Weekly",
                        "biweekly_offset_weeks": biweekly_offset_weeks or 0,
                        "month_weeks": month_weeks or "",
                        "band": band or "",
                        "mode": mode or "",
                        "vfo": (vfo or "A").strip().upper() or "A",
                        "frequency": str(freq or ""),
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "early_checkin": int(early_checkin or 0),
                        "primary_js8call_group": primary_js8call_group or "",
                        "comment": comment or "",
                        "net_name": net_name or "",
                        "group_name": group_name or primary_js8call_group or "",
                        "target_scope": target_scope or "station",
                        "target_device_profile_id": target_device_profile_id,
                        "target_operating_profile_id": target_operating_profile_id,
                    }
                )
            return out
        except Exception:
            return None

    def _load_net_resources_from_db(self) -> Optional[List[Dict[str, Any]]]:
        """
        Load known net resources from config/freqinout_nets.db.
        These may be included in SOP Schedule Plans even when they are not folded into HF Nets.
        """
        conn: sqlite3.Connection | None = None
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            exists = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='net_resources'"
            ).fetchone()
            if not exists:
                return []
            cur.execute(
                """
                SELECT
                    id, resource_set, source_type, source_ref, readonly, day_utc, recurrence,
                    biweekly_offset_weeks, month_weeks, group_name, band, mode, frequency,
                    start_utc, end_utc, early_checkin, primary_js8call_group, coverage, comment, net_name,
                    fldigi_mode, fldigi_offset, updated_utc
                  FROM net_resources
                """
            )
            out: List[Dict[str, Any]] = []
            for row in cur.fetchall():
                out.append(
                    {
                        "id": int(row[0] or 0),
                        "resource_id": int(row[0] or 0),
                        "source_table": "net_resources",
                        "source_key": f"NET_RESOURCE:{int(row[0] or 0)}" if int(row[0] or 0) > 0 else "",
                        "resource_set": row[1] or "Custom",
                        "source_type": row[2] or "manual",
                        "source_ref": row[3] or "",
                        "readonly": int(row[4] or 0),
                        "day_utc": row[5] or "ALL",
                        "recurrence": row[6] or "Weekly",
                        "biweekly_offset_weeks": int(row[7] or 0),
                        "month_weeks": row[8] or "",
                        "group_name": row[9] or row[16] or "",
                        "band": row[10] or "",
                        "mode": row[11] or "",
                        "frequency": str(row[12] or ""),
                        "start_utc": row[13] or "",
                        "end_utc": row[14] or "",
                        "early_checkin": str(row[15] if row[15] is not None else 0),
                        "primary_js8call_group": row[16] or "",
                        "coverage": row[17] or "",
                        "comment": row[18] or "",
                        "net_name": row[19] or "",
                        "fldigi_mode": row[20] or "",
                        "fldigi_offset": row[21] or "",
                        "updated_utc": row[22] or "",
                    }
                )
            return out
        except Exception:
            return None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _load_sop_layer_from_db(self) -> Optional[List[Dict[str, Any]]]:
        """
        Load HF SOP schedule-layer rows from config/freqinout_nets.db.
        Only active SOP profiles are considered for planner overlay rows.
        """
        conn: sqlite3.Connection | None = None
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return None
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            profile_cols: set[str] = set()
            try:
                cur.execute("PRAGMA table_info(sop_profiles)")
                profile_cols = {str(r[1] or "").strip().lower() for r in cur.fetchall() if len(r) > 1}
            except Exception:
                profile_cols = set()
            has_secondary_group = "secondary_group" in profile_cols
            layer_cols: set[str] = set()
            try:
                cur.execute("PRAGMA table_info(sop_schedule_layer)")
                layer_cols = {str(r[1] or "").strip().lower() for r in cur.fetchall() if len(r) > 1}
            except Exception:
                layer_cols = set()
            condition_expr = "COALESCE(l.condition_levels, 'ALL')" if "condition_levels" in layer_cols else "'ALL'"
            profile_group_expr = (
                "COALESCE(NULLIF(TRIM(p.operating_group), ''), NULLIF(TRIM(p.secondary_group), ''),"
                " NULLIF(TRIM(p.name), ''), '')"
                if has_secondary_group
                else "COALESCE(NULLIF(TRIM(p.operating_group), ''), NULLIF(TRIM(p.name), ''), '')"
            )
            layer_group_expr = (
                f"COALESCE(NULLIF(TRIM(l.group_name), ''), {profile_group_expr})"
                if "group_name" in layer_cols
                else profile_group_expr
            )
            base_sql = """
                SELECT
                    COALESCE(l.id, 0),
                    COALESCE(l.profile_id, 0),
                    COALESCE(l.day_utc, 'ALL'),
                    COALESCE(l.recurrence, 'Weekly'),
                    COALESCE(l.biweekly_offset_weeks, 0),
                    COALESCE(l.month_weeks, ''),
                    {condition_expr},
                    COALESCE(l.band, ''),
                    COALESCE(l.mode, ''),
                    COALESCE(l.vfo, 'A'),
                    COALESCE(l.frequency, ''),
                    COALESCE(l.start_utc, ''),
                    COALESCE(l.end_utc, ''),
                    {layer_group_expr},
                    COALESCE(p.name, '')
                FROM sop_schedule_layer l
                JOIN sop_profiles p ON p.id = l.profile_id
                WHERE COALESCE(l.enabled, 1) = 1
                  AND (
                        TRIM(COALESCE(l.band, '')) <> ''
                        OR TRIM(COALESCE(l.frequency, '')) <> ''
                  )
                  {active_clause}
                ORDER BY p.operating_group COLLATE NOCASE, l.day_utc, l.start_utc
            """
            cur.execute(
                base_sql.format(
                    layer_group_expr=layer_group_expr,
                    condition_expr=condition_expr,
                    active_clause="AND COALESCE(p.active, 0) = 1",
                )
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            cond_map = self._condition_level_map()
            for (
                layer_id,
                profile_id,
                day_utc,
                recurrence,
                biweekly_offset_weeks,
                month_weeks,
                condition_levels,
                band,
                mode,
                vfo,
                freq,
                start_utc,
                end_utc,
                operating_group,
                profile_name,
            ) in rows:
                group_name = str(operating_group or "").strip().upper()
                group_level = cond_map.get(group_name)
                if not self._condition_level_match(str(condition_levels or "ALL"), group_level):
                    continue
                out.append(
                    {
                        "id": int(layer_id or 0),
                        "source_row_id": int(layer_id or 0),
                        "source_table": "sop_schedule_layer",
                        "sop_layer_id": int(layer_id or 0),
                        "sop_profile_id": int(profile_id or 0),
                        "day_utc": day_utc or "ALL",
                        "recurrence": recurrence or "Weekly",
                        "biweekly_offset_weeks": biweekly_offset_weeks or 0,
                        "month_weeks": month_weeks or "",
                        "condition_levels": self._normalize_condition_levels(condition_levels),
                        "band": band or "",
                        "mode": mode or "",
                        "vfo": (vfo or "A").strip().upper() or "A",
                        "frequency": str(freq or ""),
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "group_name": group_name,
                        "profile_name": str(profile_name or "").strip(),
                    }
                )
            return out
        except Exception:
            return None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _parse_hhmm(self, s: str) -> int | None:
        s = (s or "").strip()
        if not s:
            return None
        try:
            h, m = s.split(":")
            h = int(h)
            m = int(m)
            if 0 <= h <= 23 and 0 <= m <= 59:
                return h * 60 + m
        except Exception:
            return None
        return None

    def _week_start_sunday_utc(self, now_utc: datetime.datetime) -> datetime.date:
        delta = (now_utc.weekday() + 1) % 7  # Sunday=0
        return (now_utc - datetime.timedelta(days=delta)).date()

    def _month_week_index(self, date_val: datetime.date) -> int:
        return 1 + ((date_val.day - 1) // 7)

    def _parse_month_weeks(self, txt: str) -> List[int]:
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

    def _net_row_applies_this_week(
        self, row: Dict, targets: List[str], week_sunday: datetime.date
    ) -> bool:
        recurrence = (row.get("recurrence") or "Weekly").strip()
        if recurrence not in ("Periodic", "Monthly"):
            return True
        weeks = self._parse_month_weeks(row.get("month_weeks", ""))
        if not weeks:
            weeks = [1]
        for idx, day_name in enumerate(DAY_NAMES):
            if day_name not in targets:
                continue
            date_val = week_sunday + datetime.timedelta(days=idx)
            if self._month_week_index(date_val) in weeks:
                return True
        return False

    def _hour_overlaps(self, start_min: int, end_min: int, hour: int) -> bool:
        """
        Returns True if the [start_min, end_min] interval overlaps any minute in this hour bucket.
        """
        hour_start = hour * 60
        hour_end = hour * 60 + 59
        return not (end_min < hour_start or start_min > hour_end)

    def _next_day(self, day_name_upper: str) -> str:
        try:
            idx = DAY_NAMES_UPPER.index(day_name_upper)
            return DAY_NAMES[(idx + 1) % 7]
        except Exception:
            return DAY_NAMES[0]

    def _expand_hours_for_day(self, day_val: str, start_min: int, end_min: int, *, early: int = 0) -> List[tuple[str, int]]:
        """
        Expand a schedule row into (day_name, hour) tuples, handling ALL and overnight spans.
        Times are in minutes from 00:00 UTC. early applies only to net rows (already adjusted).
        """
        targets: List[str] = []
        day_txt = (day_val or "ALL").strip().upper()
        if day_txt == "ALL" or day_txt not in DAY_NAMES_UPPER:
            targets = DAY_NAMES[:]  # all days in Title case
        else:
            # Title-case version from canonical list
            targets = [DAY_NAMES[DAY_NAMES_UPPER.index(day_txt)]]

        hours: List[tuple[str, int]] = []
        smin = start_min
        emin = end_min
        overnight = smin > emin

        for day_name in targets:
            day_upper = day_name.upper()
            if not overnight:
                for h in range(24):
                    if self._hour_overlaps(smin, emin, h):
                        hours.append((day_name, h))
            else:
                # Segment 1: from start to 23:59 on current day
                for h in range(24):
                    if self._hour_overlaps(smin, 23 * 60 + 59, h):
                        hours.append((day_name, h))
                # Segment 2: from 00:00 to end on next day
                next_day = self._next_day(day_upper)
                for h in range(24):
                    if self._hour_overlaps(0, emin, h):
                        hours.append((next_day, h))

        return hours

    def _net_window_for_day(
        self, row: Dict, day_name: str, now_utc: datetime.datetime
    ) -> Optional[tuple[datetime.datetime, datetime.datetime]]:
        """
        Given a net row and target day name, compute start/end UTC datetimes for that day.
        Returns None if times are invalid; caller filters by time window.
        """
        start_m = self._parse_hhmm(row.get("start_utc", ""))
        end_m = self._parse_hhmm(row.get("end_utc", ""))
        if start_m is None or end_m is None:
            return None
        overnight = start_m > end_m
        if not overnight:
            if end_m % 60 == 0:
                end_m = min(end_m + 60, 24 * 60)

        # Map day_name to offset from current UTC day (DAY_NAMES starts with Sunday=0)
        try:
            day_idx = DAY_NAMES.index(day_name)
        except ValueError:
            return None
        now_idx = now_utc.weekday()  # Monday=0
        now_day_sun0 = (now_idx + 1) % 7  # convert to Sunday=0..Saturday=6
        offset = (day_idx - now_day_sun0) % 7

        base_date = now_utc.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=offset)
        start_dt = base_date + datetime.timedelta(minutes=start_m)
        end_dt = base_date + datetime.timedelta(minutes=end_m)
        if overnight:
            end_dt += datetime.timedelta(days=1)
        return start_dt, end_dt

    def _start_of_week_local(self, tz: datetime.tzinfo) -> datetime.datetime:
        """
        Returns a datetime for Sunday 00:00 of the current local week.
        """
        now_local = datetime.datetime.now(tz)
        # weekday: Monday=0, Sunday=6; want Sunday as start -> offset (weekday+1) % 7
        days_to_sunday = (now_local.weekday() + 1) % 7
        start_date = (now_local - datetime.timedelta(days=days_to_sunday)).date()
        return datetime.datetime.combine(start_date, datetime.time(0, 0)).replace(tzinfo=tz)

    def _projection_cells_by_utc_key(self, projection: BlendedScheduleProjection) -> Dict[Tuple[str, int], ProjectionCell]:
        return {(cell.day_utc, int(cell.hour_utc)): cell for cell in projection.cells}

    def _projection_cell_text(self, cell: Optional[ProjectionCell]) -> str:
        if cell is None or not cell.effective_source:
            return ""
        if cell.effective_source == "HF" and not self._show_band:
            return cell.frequency or cell.band or cell.display_label
        return cell.display_label or cell.band or cell.frequency

    def _projection_cell_tooltip(self, cell: Optional[ProjectionCell]) -> str:
        if cell is None:
            return ""
        lines = [
            f"{cell.day_utc} {cell.start_utc.strftime('%H:%M')}-{cell.end_utc.strftime('%H:%M')} UTC",
            f"Effective source: {cell.effective_source or 'None'}",
        ]
        if cell.band or cell.frequency:
            lines.append(f"Band/Frequency: {cell.band or '--'} {cell.frequency or ''}".strip())
        lines.append(
            f"Sources: HF {len(cell.hf_segments)}, Net {len(cell.net_segments)}, SOP {len(cell.sop_segments)}"
        )
        return "\n".join(lines)

    def _operational_cell_text(self, cell: Optional[OperationalCell]) -> str:
        if cell is None or not cell.entries:
            return ""
        label = cell.display_label
        return f"! {label}" if cell.has_contention and label else label

    def _operational_cell_tooltip(self, cell: Optional[OperationalCell]) -> str:
        if cell is None:
            return ""
        lines = [
            f"{cell.day_utc} {cell.start_utc.strftime('%H:%M')}-{cell.end_utc.strftime('%H:%M')} UTC",
            f"Entries: {len(cell.entries)}",
        ]
        for entry in cell.entries[:5]:
            band_freq = f"{entry.band} {entry.frequency}".strip()
            label = entry.action_label or entry.net_name or entry.group_name or entry.profile_name or entry.source
            lines.append(f"- {entry.source} {entry.start_utc}-{entry.end_utc} {label} {band_freq}".strip())
        if len(cell.entries) > 5:
            lines.append(f"- +{len(cell.entries) - 5} more")
        return "\n".join(lines)

    def _set_operational_inspector(self, cell: Optional[OperationalCell]) -> None:
        self._selected_projection_cell = None
        self._selected_operational_cell = cell
        self._update_inspector_action_buttons(None)
        if hasattr(self, "edit_sop_plan_entry_btn"):
            can_edit = bool(cell is not None and cell.entries and self._selected_sop_schedule_plan_row())
            self.edit_sop_plan_entry_btn.setEnabled(can_edit)
            self.edit_sop_plan_entry_btn.setToolTip(
                "Edit the selected saved SOP Schedule Plan entry only."
                if can_edit
                else "Select a saved SOP Schedule Plan entry in SOP Lanes view to edit it locally."
            )
        if not hasattr(self, "cell_inspector_label"):
            return
        if cell is None:
            self.cell_inspector_label.setText("Select an SOP lane cell to review where to be, when to be there, and what to do.")
            return
        lines = [
            f"{cell.day_utc} {cell.start_utc.strftime('%H:%M')}-{cell.end_utc.strftime('%H:%M')} UTC",
            f"Lane: {cell.lane_key}",
            "Contention: yes" if cell.has_contention else "Contention: no",
        ]
        if not cell.entries:
            lines.append("No scheduled operational entry.")
        for entry in cell.entries[:6]:
            band_freq = f"{entry.band} {entry.frequency}".strip()
            label = entry.action_label or entry.net_name or entry.group_name or entry.profile_name or entry.source
            lines.append(
                f"{entry.source}: {entry.start_utc}-{entry.end_utc} {label} {band_freq}".strip()
            )
        if len(cell.entries) > 6:
            lines.append(f"+{len(cell.entries) - 6} more")
        self.cell_inspector_label.setText("\n".join(lines))

    def _choose_operational_entry_for_edit(self) -> Optional[Any]:
        cell = self._selected_operational_cell
        if cell is None or not cell.entries:
            return None
        if len(cell.entries) == 1:
            return cell.entries[0]
        labels = []
        for idx, entry in enumerate(cell.entries):
            labels.append(
                f"{idx + 1}. {entry.source} {entry.start_utc}-{entry.end_utc} "
                f"{entry.action_label or entry.net_name or entry.group_name or entry.profile_name or entry.source}"
            )
        selected, ok = QInputDialog.getItem(
            self,
            "Choose SOP Plan Entry",
            "Multiple entries are active in this cell. Choose the plan-local entry to edit:",
            labels,
            0,
            False,
        )
        if not ok:
            return None
        try:
            return cell.entries[labels.index(str(selected))]
        except ValueError:
            return None

    def _edit_plan_entry_dialog(self, entry: Any) -> Optional[Dict[str, Any]]:
        dialog = QDialog(self)
        dialog.setWindowTitle("Edit SOP Schedule Plan Entry")
        layout = QVBoxLayout(dialog)
        note = QLabel("These changes update the selected SOP Schedule Plan. Resource-backed entries can optionally update the master Net Resource after the plan save passes RF Guard.")
        note.setWordWrap(True)
        layout.addWidget(note)
        form = QFormLayout()
        day_combo = QComboBox()
        for day in DAY_NAMES:
            day_combo.addItem(day, day)
        day_idx = day_combo.findData(str(entry.day_utc or ""))
        if day_idx >= 0:
            day_combo.setCurrentIndex(day_idx)
        start_edit = QLineEdit(str(entry.start_utc or ""))
        end_edit = QLineEdit(str(entry.end_utc or ""))
        band_edit = QLineEdit(str(entry.band or ""))
        frequency_edit = QLineEdit(str(entry.frequency or ""))
        mode_edit = QLineEdit(str(entry.mode or ""))
        group_edit = QLineEdit(str(entry.group_name or ""))
        net_edit = QLineEdit(str(entry.net_name or ""))
        action_edit = QLineEdit(str(entry.action_label or ""))
        lane_label_edit = QLineEdit(str(entry.lane_label or ""))
        for label, widget in (
            ("Day", day_combo),
            ("Start UTC", start_edit),
            ("End UTC", end_edit),
            ("Band", band_edit),
            ("Frequency", frequency_edit),
            ("Mode", mode_edit),
            ("Group", group_edit),
            ("Net name", net_edit),
            ("Action", action_edit),
            ("Lane label", lane_label_edit),
        ):
            form.addRow(label, widget)
        layout.addLayout(form)
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        if dialog.exec() != QDialog.Accepted:
            return None
        updates = {
            "day_utc": str(day_combo.currentData() or day_combo.currentText() or "").strip(),
            "start_utc": start_edit.text().strip(),
            "end_utc": end_edit.text().strip(),
            "band": band_edit.text().strip().upper(),
            "frequency": frequency_edit.text().strip(),
            "mode": mode_edit.text().strip(),
            "group_name": group_edit.text().strip().upper(),
            "net_name": net_edit.text().strip(),
            "action_label": action_edit.text().strip(),
            "lane_label": lane_label_edit.text().strip(),
        }
        if self._parse_guard_hhmm(updates["start_utc"]) is None or self._parse_guard_hhmm(updates["end_utc"]) is None:
            self.frequency_plan_action_hint_label.setText("Enter valid UTC times as HH:MM before saving the plan-local edit.")
            return None
        if not updates["band"] and not updates["frequency"]:
            self.frequency_plan_action_hint_label.setText("Enter at least a band or frequency before saving the plan-local edit.")
            return None
        if not self._group_is_configured(updates["group_name"]):
            self.frequency_plan_action_hint_label.setText(
                f"Choose a configured Operating Group before saving the plan-local edit. {self._configured_group_help_text()}"
            )
            return None
        return updates

    def _updated_sop_plan_payload_for_entry(self, plan: Dict[str, Any], entry: Any, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._group_is_configured(updates.get("group_name")):
            self.frequency_plan_action_hint_label.setText(
                f"Choose a configured Operating Group before saving the plan-local edit. {self._configured_group_help_text()}"
            )
            return None
        refs = self._schedule_refs_from_plan_row(plan)
        try:
            index = int((entry.raw or {}).get("plan_ref_index"))
        except Exception:
            index = -1
        if index < 0 or index >= len(refs):
            return None
        updated_ref = dict(refs[index])
        updated_ref.update(updates)
        updated_ref.setdefault("source", entry.source)
        if entry.radio_id and not updated_ref.get("radio_id"):
            updated_ref["radio_id"] = entry.radio_id
        self._normalize_plan_local_lane_identity(updated_ref)
        refs[index] = updated_ref
        projection = build_operational_day_projection_from_refs(refs)
        payload = dict(plan)
        payload.update(
            {
                "id": int(plan.get("id") or 0),
                "name": str(plan.get("name") or "SOP Schedule Plan"),
                "category": "sop_schedule",
                "source_refs": projection.source_refs(),
                "schedule_refs": refs,
                "frequency_refs": projection.frequency_refs(),
                "group_refs": projection.group_refs(),
                "notes": str(plan.get("notes") or ""),
            }
        )
        return payload

    def _resource_id_for_operational_entry(self, entry: Any) -> int:
        raw = getattr(entry, "raw", {}) or {}
        return self._coerce_positive_int(raw.get("resource_id") or raw.get("_resource_id"))

    def _resource_update_payload_from_plan_ref(self, ref: Mapping[str, Any], updates: Mapping[str, Any]) -> Dict[str, Any]:
        row = dict(ref)
        row.update(dict(updates))
        group = str(row.get("group_name") or "").strip().upper()
        return {
            "day_utc": str(row.get("day_utc") or "ALL").strip() or "ALL",
            "recurrence": str(row.get("recurrence") or "Weekly").strip() or "Weekly",
            "biweekly_offset_weeks": self._coerce_positive_int(row.get("biweekly_offset_weeks")),
            "month_weeks": str(row.get("month_weeks") or "").strip(),
            "group_name": group,
            "band": str(row.get("band") or "").strip().upper(),
            "mode": str(row.get("mode") or "").strip(),
            "frequency": str(row.get("frequency") or row.get("freq") or "").strip(),
            "start_utc": str(row.get("start_utc") or "").strip(),
            "end_utc": str(row.get("end_utc") or "").strip(),
            "early_checkin": self._coerce_positive_int(row.get("early_checkin")),
            "primary_js8call_group": str(row.get("primary_js8call_group") or group).strip().upper(),
            "comment": str(row.get("comment") or "").strip(),
            "net_name": str(row.get("net_name") or "").strip(),
            "fldigi_mode": str(row.get("fldigi_mode") or "").strip(),
            "fldigi_offset": str(row.get("fldigi_offset") or "").strip(),
        }

    def _update_master_net_resource_from_plan_ref(
        self,
        resource_id: int,
        ref: Mapping[str, Any],
        updates: Mapping[str, Any],
    ) -> bool:
        rid = int(resource_id or 0)
        if rid <= 0:
            return False
        db_path = get_config_dir() / "config" / "freqinout_nets.db"
        if not db_path.exists():
            raise FileNotFoundError(f"Net Resources database not found: {db_path}")
        payload = self._resource_update_payload_from_plan_ref(ref, updates)
        with sqlite3.connect(db_path) as conn:
            exists = conn.execute(
                "SELECT id FROM net_resources WHERE id=?",
                (rid,),
            ).fetchone()
            if not exists:
                raise KeyError(f"Unknown Net Resource id: {rid}")
            conn.execute(
                """
                UPDATE net_resources
                   SET source_type='manual',
                       source_ref='updated_from_sop_schedule_plan',
                       day_utc=?,
                       recurrence=?,
                       biweekly_offset_weeks=?,
                       month_weeks=?,
                       group_name=?,
                       band=?,
                       mode=?,
                       frequency=?,
                       start_utc=?,
                       end_utc=?,
                       early_checkin=?,
                       primary_js8call_group=?,
                       comment=?,
                       net_name=?,
                       fldigi_mode=?,
                       fldigi_offset=?,
                       updated_utc=?
                 WHERE id=?
                """,
                (
                    payload["day_utc"],
                    payload["recurrence"],
                    int(payload["biweekly_offset_weeks"]),
                    payload["month_weeks"],
                    payload["group_name"],
                    payload["band"],
                    payload["mode"],
                    payload["frequency"],
                    payload["start_utc"],
                    payload["end_utc"],
                    int(payload["early_checkin"]),
                    payload["primary_js8call_group"],
                    payload["comment"],
                    payload["net_name"],
                    payload["fldigi_mode"],
                    payload["fldigi_offset"],
                    datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    rid,
                ),
            )
            conn.commit()
        return True

    def _preflight_master_net_resource_update(self, resource_id: int) -> None:
        rid = int(resource_id or 0)
        if rid <= 0:
            raise ValueError("No Net Resource id is linked to this entry.")
        db_path = get_config_dir() / "config" / "freqinout_nets.db"
        if not db_path.exists():
            raise FileNotFoundError(f"Net Resources database not found: {db_path}")
        with sqlite3.connect(db_path) as conn:
            exists = conn.execute("SELECT id FROM net_resources WHERE id=?", (rid,)).fetchone()
        if not exists:
            raise KeyError(f"Unknown Net Resource id: {rid}")

    def _prompt_master_resource_update(self, resource_id: int) -> Optional[bool]:
        if int(resource_id or 0) <= 0:
            return False
        response = QMessageBox.question(
            self,
            "Update Master Net Resource?",
            f"This entry is linked to Net Resource #{int(resource_id)}.\n\n"
            "Save the SOP Schedule Plan edit only, or also update the master Net Resource so future schedules use the change?\n\n"
            "Choose Yes to update both. Choose No to update this plan only.",
            QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
            QMessageBox.No,
        )
        if response == QMessageBox.Cancel:
            return None
        return response == QMessageBox.Yes

    def _normalize_plan_local_lane_identity(self, ref: Dict[str, Any]) -> None:
        radio_id = self._coerce_positive_int(
            ref.get("radio_id") or ref.get("device_profile_id") or ref.get("target_device_profile_id")
        )
        if radio_id > 0:
            ref["lane_key"] = f"radio:{radio_id}"
            ref.setdefault("lane_label", f"Radio {radio_id}")
            return
        source = str(ref.get("source") or "").strip().upper()
        if source == "SOP":
            sop_id = self._coerce_positive_int(ref.get("sop_profile_id") or ref.get("profile_id"))
            if sop_id > 0:
                ref["lane_key"] = f"sop:{sop_id}"
                ref.setdefault("lane_label", str(ref.get("profile_name") or f"SOP {sop_id}"))
                return
        group = str(ref.get("group_name") or "").strip().upper()
        if group:
            ref["lane_key"] = f"group:{group}"
            ref["lane_label"] = group
            return
        ref["lane_key"] = "station"
        if not str(ref.get("lane_label") or "").strip():
            ref["lane_label"] = "Station"

    def _on_edit_sop_plan_entry_clicked(self) -> None:
        plan = self._selected_sop_schedule_plan_row()
        if not plan:
            self.frequency_plan_action_hint_label.setText("Select a saved SOP Schedule Plan before editing plan-local entries.")
            return
        entry = self._choose_operational_entry_for_edit()
        if entry is None:
            self.frequency_plan_action_hint_label.setText("Select an SOP Lanes cell with a plan entry to edit.")
            return
        updates = self._edit_plan_entry_dialog(entry)
        if updates is None:
            return
        payload = self._updated_sop_plan_payload_for_entry(plan, entry, updates)
        if payload is None:
            self.frequency_plan_action_hint_label.setText("Unable to locate the selected entry in the saved SOP Schedule Plan.")
            return
        resource_id = self._resource_id_for_operational_entry(entry)
        update_master_resource = self._prompt_master_resource_update(resource_id) if resource_id > 0 else False
        if update_master_resource is None:
            return
        if update_master_resource:
            try:
                self._preflight_master_net_resource_update(resource_id)
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    "Master Net Resource Unavailable",
                    f"Net Resource #{resource_id} cannot be updated, so this combined save was not started.\n\n{exc}",
                )
                self.frequency_plan_action_hint_label.setText(
                    f"Could not update Net Resource #{resource_id}; no SOP Schedule Plan edit was saved."
                )
                return
        if update_master_resource:
            confirm_text = (
                f"Save this change to the selected SOP Schedule Plan and update Net Resource #{resource_id}?\n\n"
                "This will change the saved plan after RF Guard preflight and then update the master Net Resource record."
            )
        else:
            confirm_text = (
                "Save this change to the selected SOP Schedule Plan only?\n\n"
                "HF Daily, HF Nets, SOP Builder, and master Net Resources will not be changed."
            )
        response = QMessageBox.question(
            self,
            "Save Plan-Local Edit",
            confirm_text,
            QMessageBox.Save | QMessageBox.Cancel,
            QMessageBox.Save,
        )
        if response != QMessageBox.Save:
            return
        saved = self._save_plan_payload_with_guard(
            payload,
            schedule_count=len(payload.get("schedule_refs") or []),
            success_kind="SOP Schedule Plan",
        )
        if saved:
            followup_message = ""
            if update_master_resource:
                try:
                    index = int((entry.raw or {}).get("plan_ref_index"))
                    updated_ref = (payload.get("schedule_refs") or [])[index]
                    self._update_master_net_resource_from_plan_ref(resource_id, updated_ref, updates)
                    followup_message = (
                        f"Saved SOP Schedule Plan '{str(saved.get('name') or plan.get('name') or 'Plan')}' "
                        f"and updated Net Resource #{resource_id}."
                    )
                except Exception as exc:
                    log.exception("FreqPlanner: failed updating master Net Resource from SOP Schedule Plan edit.")
                    QMessageBox.warning(
                        self,
                        "Master Net Resource Update Failed",
                        f"The SOP Schedule Plan was saved, but Net Resource #{resource_id} could not be updated.\n\n{exc}",
                    )
                    followup_message = (
                        f"Saved the SOP Schedule Plan, but could not update Net Resource #{resource_id}: {exc}"
                    )
            self.rebuild_table()
            if followup_message:
                self.frequency_plan_action_hint_label.setText(followup_message)

    def _format_segment_for_inspector(self, segment: ScheduleSegment) -> str:
        label = segment.net_name or segment.group_name or segment.profile_name or segment.band or segment.source
        band_freq = f"{segment.band} {segment.frequency}".strip()
        return f"{segment.source} {segment.start_utc}-{segment.end_utc} {label} {band_freq}".strip()

    @staticmethod
    def _segments_for_source(cell: Optional[ProjectionCell], source: str) -> Tuple[ScheduleSegment, ...]:
        if not isinstance(cell, ProjectionCell):
            return ()
        key = str(source or "").strip().upper()
        if key == "HF":
            return cell.hf_segments
        if key == "NET":
            return cell.net_segments
        if key == "SOP":
            return cell.sop_segments
        return ()

    @staticmethod
    def _source_button_state(
        segments: Tuple[ScheduleSegment, ...],
        open_text: str,
        none_text: str,
        multiple_text: str,
    ) -> Tuple[bool, str]:
        if len(segments) == 1:
            return True, open_text
        if len(segments) > 1:
            return True, multiple_text
        return False, none_text

    def _update_inspector_action_buttons(self, cell: Optional[ProjectionCell]) -> None:
        hf_segments = self._segments_for_source(cell, "HF")
        net_segments = self._segments_for_source(cell, "NET")
        sop_segments = self._segments_for_source(cell, "SOP")
        if hasattr(self, "edit_hf_daily_btn"):
            enabled, tooltip = self._source_button_state(
                hf_segments,
                "Open HF Daily and focus the underlying source row.",
                "No HF Daily source row for this cell.",
                "Multiple HF Daily source rows match this cell; open HF Daily and choose the row to edit.",
            )
            self.edit_hf_daily_btn.setEnabled(enabled)
            self.edit_hf_daily_btn.setToolTip(tooltip)
            self.edit_hf_daily_btn.setStyleSheet(button_style("primary" if enabled else "muted", resolve_theme(self.settings)))
        if hasattr(self, "edit_hf_net_btn"):
            enabled, tooltip = self._source_button_state(
                net_segments,
                "Open HF Nets and focus the underlying source row.",
                "No HF Net source row for this cell.",
                "Multiple HF Net source rows match this cell; open HF Nets and choose the row to edit.",
            )
            self.edit_hf_net_btn.setEnabled(enabled)
            self.edit_hf_net_btn.setToolTip(tooltip)
            self.edit_hf_net_btn.setStyleSheet(button_style("primary" if enabled else "muted", resolve_theme(self.settings)))
        if hasattr(self, "open_sop_builder_btn"):
            enabled, tooltip = self._source_button_state(
                sop_segments,
                "Open SOP Builder and select the underlying SOP profile.",
                "No SOP source row for this cell.",
                "Multiple SOP source rows match this cell; open SOP Builder and choose the source to edit.",
            )
            self.open_sop_builder_btn.setEnabled(enabled)
            self.open_sop_builder_btn.setToolTip(tooltip)
            self.open_sop_builder_btn.setStyleSheet(button_style("primary" if enabled else "muted", resolve_theme(self.settings)))

    def _set_projection_inspector(self, cell: Optional[ProjectionCell]) -> None:
        self._selected_projection_cell = cell
        self._selected_operational_cell = None
        self._update_inspector_action_buttons(cell)
        if not hasattr(self, "cell_inspector_label"):
            return
        if cell is None:
            self.cell_inspector_label.setText("Select a schedule cell to review the blended HF Daily, HF Nets, and SOP sources.")
            return
        lines = [
            f"{cell.day_utc} {cell.start_utc.strftime('%H:%M')}-{cell.end_utc.strftime('%H:%M')} UTC",
            f"Effective: {cell.effective_source or 'None'} {cell.display_label}".strip(),
        ]
        for title, segments in (
            ("HF Daily", cell.hf_segments),
            ("HF Nets", cell.net_segments),
            ("SOP", cell.sop_segments),
        ):
            if not segments:
                lines.append(f"{title}: none")
                continue
            detail = "; ".join(self._format_segment_for_inspector(segment) for segment in segments[:3])
            if len(segments) > 3:
                detail += f"; +{len(segments) - 3} more"
            lines.append(f"{title}: {detail}")
        self.cell_inspector_label.setText("\n".join(lines))

    def _on_schedule_cell_clicked(self, row: int, col: int) -> None:
        if col < self.COL_DAY_OFFSET:
            if self._planner_view_mode() == "operational":
                self._set_operational_inspector(None)
            else:
                self._set_projection_inspector(None)
            return
        item = self.table.item(row, col)
        cell = item.data(Qt.UserRole) if item is not None else None
        if isinstance(cell, OperationalCell):
            self._set_operational_inspector(cell)
            return
        self._set_projection_inspector(cell if isinstance(cell, ProjectionCell) else None)

    def _selected_source_segment(self, source: str) -> Optional[ScheduleSegment]:
        cell = self._selected_projection_cell
        segments = self._segments_for_source(cell, source)
        return segments[0] if len(segments) == 1 else None

    def _segment_choice_label(self, segment: ScheduleSegment, index: int) -> str:
        identity = str(segment.raw.get("source_key") or "").strip()
        source_row_id = segment.raw.get("source_row_id")
        resource_id = segment.raw.get("resource_id")
        if source_row_id not in (None, "", 0):
            identity = f"row {source_row_id}"
        elif resource_id not in (None, "", 0):
            identity = f"resource {resource_id}"
        elif not identity:
            identity = f"candidate {index + 1}"
        return f"{index + 1}. {self._format_segment_for_inspector(segment)} [{identity}]"

    def _choose_source_segment(self, source: str, title: str) -> Optional[ScheduleSegment]:
        cell = self._selected_projection_cell
        segments = self._segments_for_source(cell, source)
        if len(segments) == 1:
            return segments[0]
        if len(segments) <= 0:
            return None
        labels = [self._segment_choice_label(segment, idx) for idx, segment in enumerate(segments)]
        selected, ok = QInputDialog.getItem(
            self,
            f"Choose {title} Source",
            "Multiple source rows match this cell. Choose the row to review:",
            labels,
            0,
            False,
        )
        if not ok:
            return None
        try:
            return segments[labels.index(str(selected))]
        except ValueError:
            return None

    def _navigate_to_tab(self, tab_label: str) -> None:
        target = str(tab_label or "").strip().upper()
        if not target:
            return
        win = self.window()
        try:
            if hasattr(win, "_screens") and hasattr(win, "_set_screen"):
                for idx, (label, _widget) in enumerate(win._screens):
                    if str(label or "").strip().upper() == target:
                        win._set_screen(idx)
                        return
        except Exception as exc:
            log.debug("FreqPlanner: failed navigating to %s: %s", tab_label, exc)

    def _load_selected_source_schedule_in_tab(
        self,
        tab: Any,
        *,
        selected_key: str,
        sets_key: str,
        category: str,
        live_loader_name: str,
    ) -> Optional[bool]:
        if tab is None:
            return False
        selected_id = self._selected_source_set_id(selected_key)
        if hasattr(tab, "_refresh_freqplanner_source_combo"):
            try:
                tab._refresh_freqplanner_source_combo()
            except Exception:
                pass
        combo = getattr(tab, "schedule_source_combo", None)
        if combo is not None:
            try:
                idx = combo.findData(selected_id)
                if idx >= 0:
                    combo.setCurrentIndex(idx)
            except Exception:
                pass
        if hasattr(tab, "_confirm_discard_unsaved_source_load"):
            try:
                if not tab._confirm_discard_unsaved_source_load():
                    return None
            except Exception:
                pass
        if selected_id == LIVE_SOURCE_SET_ID:
            loader = getattr(tab, live_loader_name, None)
            if callable(loader):
                try:
                    loader()
                    return True
                except Exception as exc:
                    log.debug("FreqPlanner: failed loading live source schedule: %s", exc)
                    return False
            return True
        row = source_set_row_by_id_for_category(self.settings, sets_key, category, selected_id)
        if row is None:
            return False
        loader = getattr(tab, "_load_source_rows_into_table", None)
        if not callable(loader):
            return False
        try:
            loader([dict(item) for item in row.get("rows", []) if isinstance(item, dict)])
            return True
        except Exception as exc:
            log.debug("FreqPlanner: failed loading selected source schedule %s: %s", selected_id, exc)
            return False

    @staticmethod
    def _normalize_freq_text(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            return f"{float(text):.3f}"
        except Exception:
            return text

    @staticmethod
    def _widget_text(widget: Any) -> str:
        if widget is None:
            return ""
        if hasattr(widget, "currentText"):
            try:
                return str(widget.currentText() or "").strip()
            except Exception:
                return ""
        if isinstance(widget, QLineEdit) or hasattr(widget, "text"):
            try:
                return str(widget.text() or "").strip()
            except Exception:
                return ""
        return ""

    def _focus_tab_row_for_segment(self, tab: Any, segment: ScheduleSegment, *, source: str) -> bool:
        table = getattr(tab, "table", None)
        if table is None:
            return False
        col_day = getattr(tab, "COL_DAY", None)
        col_group = getattr(tab, "COL_GROUP", None)
        col_band = getattr(tab, "COL_BAND", None)
        col_freq = getattr(tab, "COL_FREQ", None)
        col_start = getattr(tab, "COL_START", None)
        col_end = getattr(tab, "COL_END", None)
        col_net = getattr(tab, "COL_NETNAME", None)
        target_day = str(segment.day_utc or "").strip().upper()
        target_group = str(segment.group_name or "").strip().upper()
        target_band = str(segment.band or "").strip().upper()
        target_freq = self._normalize_freq_text(segment.frequency)
        target_start = str(segment.start_utc or "").strip()
        target_end = str(segment.end_utc or "").strip()
        target_net = str(segment.net_name or "").strip().upper()
        for row in range(table.rowCount()):
            def combo_value(col: Optional[int]) -> str:
                if col is None:
                    return ""
                if hasattr(tab, "_get_combo_value"):
                    try:
                        return str(tab._get_combo_value(row, col, "") or "").strip()
                    except Exception:
                        pass
                return self._widget_text(table.cellWidget(row, col))

            def item_value(col: Optional[int]) -> str:
                if col is None:
                    return ""
                item = table.item(row, col)
                return str(item.text() if item is not None else "").strip()

            day_val = combo_value(col_day).upper()
            group_val = combo_value(col_group).upper()
            band_val = combo_value(col_band).upper()
            freq_val = self._normalize_freq_text(item_value(col_freq))
            start_val = item_value(col_start)
            end_val = item_value(col_end)
            net_val = self._widget_text(table.cellWidget(row, col_net)).upper() if col_net is not None else ""
            if target_day and day_val and day_val != target_day:
                continue
            if target_group and group_val and group_val != target_group:
                continue
            if target_band and band_val and band_val != target_band:
                continue
            if target_freq and freq_val and freq_val != target_freq:
                continue
            if target_start and start_val and start_val != target_start:
                continue
            if target_end and end_val and end_val != target_end:
                continue
            if source.upper() == "NET" and target_net and net_val and net_val != target_net:
                continue
            table.selectRow(row)
            focus_col = col_net if source.upper() == "NET" and col_net is not None else col_freq
            if focus_col is not None and table.item(row, focus_col) is not None:
                table.scrollToItem(table.item(row, focus_col))
            table.setFocus(Qt.TabFocusReason)
            return True
        return False

    def _on_edit_hf_daily_clicked(self) -> None:
        segment = self._choose_source_segment("HF", "HF Daily")
        if segment is None:
            self.frequency_plan_action_hint_label.setText("Select a cell with an HF Daily source row first.")
            return
        self._navigate_to_tab("HF Schedule")
        tab = getattr(self.window(), "hf_schedule_tab", None)
        loaded = self._load_selected_source_schedule_in_tab(
            tab,
            selected_key=SELECTED_HF_DAILY_SOURCE_SET_KEY,
            sets_key=HF_DAILY_SOURCE_SETS_KEY,
            category=HF_DAILY_SOURCE_CATEGORY,
            live_loader_name="_load_schedule",
        )
        if loaded is None:
            self.frequency_plan_action_hint_label.setText(
                "HF Daily load was cancelled; the selected source row was not opened."
            )
            return
        focused = False
        if tab is not None and hasattr(tab, "focus_source_segment"):
            try:
                focused = bool(tab.focus_source_segment(segment))
            except Exception:
                focused = False
        if not focused and tab is not None and hasattr(tab, "_focus_daily_row"):
            try:
                tab._focus_daily_row(segment.group_name, segment.band, segment.frequency)
                focused = True
            except Exception:
                focused = False
        if not focused and tab is not None:
            focused = self._focus_tab_row_for_segment(tab, segment, source="HF")
        self.frequency_plan_action_hint_label.setText(
            (
                "Opened the selected HF Daily schedule and source row for review."
                if focused and loaded
                else (
                    "Opened HF Daily source row for review."
                    if focused
                    else "Opened HF Daily. Review the matching source row before editing."
                )
            )
        )

    def _on_edit_hf_net_clicked(self) -> None:
        segment = self._choose_source_segment("NET", "HF Net")
        if segment is None:
            self.frequency_plan_action_hint_label.setText("Select a cell with an HF Net source row first.")
            return
        self._navigate_to_tab("Net Schedule")
        tab = getattr(self.window(), "net_tab", None)
        loaded = self._load_selected_source_schedule_in_tab(
            tab,
            selected_key=SELECTED_HF_NET_SOURCE_SET_KEY,
            sets_key=HF_NET_SOURCE_SETS_KEY,
            category=HF_NET_SOURCE_CATEGORY,
            live_loader_name="_load",
        )
        if loaded is None:
            self.frequency_plan_action_hint_label.setText(
                "HF Net load was cancelled; the selected source row was not opened."
            )
            return
        focused = False
        if tab is not None and hasattr(tab, "focus_source_segment"):
            try:
                focused = bool(tab.focus_source_segment(segment))
            except Exception:
                focused = False
        if not focused:
            focused = self._focus_tab_row_for_segment(tab, segment, source="NET") if tab is not None else False
        self.frequency_plan_action_hint_label.setText(
            (
                "Opened the selected HF Net schedule and source row for review."
                if focused and loaded
                else (
                    "Opened HF Net source row for review."
                    if focused
                    else "Opened HF Nets. Review the matching source row before editing."
                )
            )
        )

    def _on_open_sop_builder_clicked(self) -> None:
        segment = self._choose_source_segment("SOP", "SOP")
        if segment is None:
            self.frequency_plan_action_hint_label.setText("Select a cell with an SOP source row first.")
            return
        self._navigate_to_tab("SOP")
        profile_id = int(segment.raw.get("sop_profile_id") or 0)
        tab = getattr(self.window(), "sop_tab", None)
        selected = False
        focused = False
        if tab is not None and hasattr(tab, "focus_source_segment"):
            try:
                focused = bool(tab.focus_source_segment(segment))
                selected = focused
            except Exception as exc:
                log.debug("FreqPlanner: failed focusing SOP layer source: %s", exc)
                focused = False
        if not focused and tab is not None and profile_id > 0:
            try:
                if hasattr(tab, "select_profile"):
                    selected = bool(tab.select_profile(profile_id))
                elif hasattr(tab, "profile_combo"):
                    for idx in range(tab.profile_combo.count()):
                        if int(tab.profile_combo.itemData(idx) or 0) == profile_id:
                            tab.profile_combo.setCurrentIndex(idx)
                            selected = True
                            break
            except Exception as exc:
                log.debug("FreqPlanner: failed selecting SOP profile %s: %s", profile_id, exc)
        self.frequency_plan_action_hint_label.setText(
            "Opened SOP Builder source row for review."
            if focused
            else (
                "Opened SOP Builder source profile for review."
                if selected
                else "Opened SOP Builder. Review the matching SOP source before editing."
            )
        )

    def mark_schedule_dirty(self) -> None:
        self._pending_rebuild = True

    def on_tab_activated(self) -> None:
        if not self._pending_rebuild:
            return
        self._pending_rebuild = False
        self.rebuild_table()

    # ------------- core rebuild ------------- #

    def _rebuild_operational_lane_table(
        self,
        hf_sched: List[Dict],
        net_sched: List[Dict],
        sop_sched: List[Dict],
        policy_rows: List[Dict[str, Any]],
        week_sunday: datetime.date,
        theme: Dict[str, Any],
    ) -> None:
        selected_plan = self._selected_sop_schedule_plan_row()
        projection = self._build_selected_sop_plan_projection(week_sunday) if selected_plan else None
        source_text = f"saved plan '{str(selected_plan.get('name') or 'SOP Schedule Plan')}'" if selected_plan else "live projection"
        if projection is None:
            hf_sched = self._filter_rows_to_configured_groups(hf_sched)
            net_sched = self._filter_rows_to_configured_groups(net_sched)
            sop_sched = self._filter_rows_to_configured_groups(sop_sched)
            net_resources = self._filter_rows_to_configured_groups(self._load_net_resources_from_db() or [])
            projection = build_operational_day_projection(
                hf_sched,
                net_sched,
                sop_sched,
                net_resources,
                policy_rows,
                week_start_utc=week_sunday,
            )
        selected_day = self._selected_operational_day()
        lanes = list(projection.lanes)
        column_count = max(self.COL_DAY_OFFSET + len(lanes), self.COL_DAY_OFFSET + 1)
        self.table.setColumnCount(column_count)
        tz_name, tz_abbr = self._current_timezone_label()
        headers = ["UTC Hour", f"Local Time ({tz_abbr})"]
        headers.extend(lane.lane_label for lane in lanes)
        if not lanes:
            headers.append("SOP Lanes")
        self.table.setHorizontalHeaderLabels(headers)
        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_UTC, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_LOCAL, QHeaderView.ResizeToContents)
        for col in range(self.COL_DAY_OFFSET, column_count):
            hv.setSectionResizeMode(col, QHeaderView.Stretch)
        tz_name_cfg, tz = self._current_timezone()
        try:
            day_index = DAY_NAMES.index(selected_day)
        except ValueError:
            day_index = 0
        date_value = week_sunday + datetime.timedelta(days=day_index)
        visible_bands: set[str] = set()
        for hour in range(24):
            utc_dt = datetime.datetime.combine(date_value, datetime.time(hour=hour), tzinfo=datetime.timezone.utc)
            local_dt = utc_dt.astimezone(tz)
            utc_item = QTableWidgetItem(f"{hour:02d}:00")
            utc_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            self.table.setItem(hour, self.COL_UTC, utc_item)
            local_item = QTableWidgetItem(f"{local_dt.hour:02d}:00")
            local_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            self.table.setItem(hour, self.COL_LOCAL, local_item)
            if not lanes:
                item = QTableWidgetItem("")
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.table.setItem(hour, self.COL_DAY_OFFSET, item)
                continue
            for lane_index, lane in enumerate(lanes):
                col = self.COL_DAY_OFFSET + lane_index
                cell = projection.cell_for(lane.lane_key, selected_day, hour)
                item = QTableWidgetItem(self._operational_cell_text(cell))
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if cell is not None:
                    item.setData(Qt.UserRole, cell)
                    tooltip = self._operational_cell_tooltip(cell)
                    if tooltip:
                        item.setToolTip(tooltip)
                    if cell.has_contention:
                        item.setBackground(qcolor(theme["surface_alt"]))
                    elif cell.entries:
                        primary_band = str(cell.entries[0].band or "").split("/")[0].strip()
                        if primary_band:
                            visible_bands.add(primary_band.lower())
                            colors = self._band_cell_colors(primary_band, theme)
                            if colors:
                                item.setBackground(qcolor(colors["bg"]))
                                item.setForeground(qcolor(colors["fg"]))
                self.table.setItem(hour, col, item)
        self._visible_bands = sorted(visible_bands)
        self._render_band_legend()
        self._set_operational_inspector(None)
        counts = projection.source_counts
        self.frequency_plan_action_hint_label.setText(
            f"SOP Lanes view from {source_text}: {selected_day}, {len(projection.lanes)} lane(s), "
            f"{sum(counts.values())} operational entr{'y' if sum(counts.values()) == 1 else 'ies'}."
        )

    def rebuild_table(self):
        """
        Recompute the table based on current hf_schedule and net_schedule in config.
        """
        perf_start = time.perf_counter()
        try:
            self.settings.reload()
        except Exception:
            pass
        self.plan_context_label.refresh_context(refresh=True)
        self._refresh_plan_workspace_header()
        self._refresh_source_set_controls()
        self.table.clearContents()
        tz_name, tz_abbr = self._current_timezone_label()
        if not self._show_local:
            headers = [
                "UTC Hour",
                f"Local Time ({tz_abbr})",
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]
        else:
            headers = [
                f"Local Hour ({tz_abbr})",
                "UTC Time",
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]
        self.table.setHorizontalHeaderLabels(headers)

        hf_sched, net_sched, sop_sched, policy_rows = self._load_schedules()
        theme = resolve_theme(self.settings)
        self._last_snapshot = self._snapshot(hf_sched, net_sched, sop_sched, policy_rows)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        week_sunday = self._week_start_sunday_utc(now_utc)
        if self._planner_view_mode() == "operational":
            self._rebuild_operational_lane_table(hf_sched, net_sched, sop_sched, policy_rows, week_sunday, theme)
            self._update_clock_labels()
            emit_span(
                "freqplanner.rebuild_table",
                (time.perf_counter() - perf_start) * 1000.0,
                settings=self.settings,
                meta={"show_local": bool(self._show_local), "show_band": bool(self._show_band), "projection": "operational"},
                min_ms=5.0,
            )
            log.info("FreqPlanner table rebuilt from operational day projection.")
            return
        self.table.setColumnCount(9)
        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_UTC, QHeaderView.Stretch)
        hv.setSectionResizeMode(self.COL_LOCAL, QHeaderView.Stretch)
        for col in range(self.COL_DAY_OFFSET, 9):
            hv.setSectionResizeMode(col, QHeaderView.Stretch)
        projection = build_blended_schedule_projection(
            hf_sched,
            net_sched,
            sop_sched,
            policy_rows,
            week_start_utc=week_sunday,
        )
        projection_cells = self._projection_cells_by_utc_key(projection)

        tz_name_cfg, tz = self._current_timezone()
        now_local = datetime.datetime.now(tz)
        now_plus_24 = now_utc + datetime.timedelta(hours=24)
        today_utc = now_utc.replace(minute=0, second=0, microsecond=0)
        week_start_local = self._start_of_week_local(tz)
        visible_bands: set[str] = {
            band.strip().lower()
            for segment in projection.effective_segments
            for band in str(segment.band or "").split("/")
            if band.strip()
        }

        for hour in range(24):
            if not self._show_local:
                utc_item = QTableWidgetItem(f"{hour:02d}:00")
                utc_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if hour == now_utc.hour:
                    utc_item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, self.COL_UTC, utc_item)

                utc_dt = datetime.datetime(
                    year=today_utc.year,
                    month=today_utc.month,
                    day=today_utc.day,
                    hour=hour,
                    minute=0,
                    second=0,
                    tzinfo=datetime.timezone.utc,
                )
                local_dt = utc_dt.astimezone(tz)
                local_item = QTableWidgetItem(f"{local_dt.hour:02d}:00")
                local_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if local_dt.hour == now_local.hour:
                    local_item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, self.COL_LOCAL, local_item)
            else:
                local_dt = week_start_local + datetime.timedelta(hours=hour)
                utc_dt = local_dt.astimezone(datetime.timezone.utc)

                local_item = QTableWidgetItem(f"{local_dt.hour:02d}:00")
                local_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if local_dt.hour == now_local.hour:
                    local_item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, self.COL_UTC, local_item)

                utc_item = QTableWidgetItem(f"{utc_dt.hour:02d}:00")
                utc_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if utc_dt.hour == now_utc.hour:
                    utc_item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, self.COL_LOCAL, utc_item)

            for col in range(self.COL_DAY_OFFSET, 9):
                if not self._show_local:
                    lookup_day = DAY_NAMES[col - self.COL_DAY_OFFSET]
                    lookup_hour = hour
                else:
                    cell_local_dt = week_start_local + datetime.timedelta(days=(col - self.COL_DAY_OFFSET), hours=hour)
                    cell_dt_utc = cell_local_dt.astimezone(datetime.timezone.utc)
                    lookup_day = cell_dt_utc.strftime("%A")
                    lookup_hour = cell_dt_utc.hour

                cell = projection_cells.get((lookup_day, lookup_hour))
                cell_text = self._projection_cell_text(cell)
                item = QTableWidgetItem(cell_text)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                if cell is not None:
                    item.setData(Qt.UserRole, cell)
                    tooltip = self._projection_cell_tooltip(cell)
                    if tooltip:
                        item.setToolTip(tooltip)
                if cell and cell.effective_source == "HF" and cell.band:
                    primary_band = cell.band.split("/")[0].strip()
                    colors = self._band_cell_colors(primary_band, theme)
                    if colors:
                        item.setBackground(qcolor(colors["bg"]))
                        item.setForeground(qcolor(colors["fg"]))
                if cell and cell.net_segments and (
                    cell.start_utc <= now_utc <= cell.end_utc or now_utc <= cell.start_utc <= now_plus_24
                ):
                    item.setBackground(qcolor(theme["surface_alt"]))
                self.table.setItem(hour, col, item)

        self._update_clock_labels()
        self._visible_bands = sorted(visible_bands)
        self._render_band_legend()
        emit_span(
            "freqplanner.rebuild_table",
            (time.perf_counter() - perf_start) * 1000.0,
            settings=self.settings,
            meta={"show_local": bool(self._show_local), "show_band": bool(self._show_band), "projection": "shared"},
            min_ms=5.0,
        )
        log.info("FreqPlanner table rebuilt from shared schedule projection.")
        return

    def _snapshot(
        self,
        hf_sched: List[Dict],
        net_sched: List[Dict],
        sop_sched: List[Dict],
        policy_rows: List[Dict[str, Any]],
    ) -> str:
        """
        Deterministic snapshot of schedules and time view to avoid unnecessary rebuilds.
        """
        parts = ["LOCAL" if self._show_local else "UTC", "BAND" if self._show_band else "FREQ"]
        for s in sorted(hf_sched, key=lambda x: (x.get("day_utc", ""), x.get("start_utc", ""), x.get("group_name", ""))):
            parts.append(
                f"H|{s.get('day_utc','')}|{s.get('group_name','')}|{s.get('start_utc','')}|{s.get('end_utc','')}|{s.get('band','')}"
            )
        for n in sorted(net_sched, key=lambda x: (x.get("day_utc", ""), x.get("start_utc", ""), x.get("net_name", ""))):
            parts.append(
                f"N|{n.get('day_utc','')}|{n.get('net_name','')}|{n.get('start_utc','')}|{n.get('end_utc','')}|{n.get('recurrence','')}|{n.get('month_weeks','')}"
            )
        for s in sorted(sop_sched, key=lambda x: (x.get("group_name", ""), x.get("day_utc", ""), x.get("start_utc", ""))):
            parts.append(
                f"S|{s.get('group_name','')}|{s.get('day_utc','')}|{s.get('start_utc','')}|{s.get('end_utc','')}|{s.get('recurrence','')}|{s.get('month_weeks','')}"
            )
        for p in sorted(
            policy_rows,
            key=lambda x: (
                str(x.get("policy") or ""),
                str(x.get("start_utc") or ""),
                str(x.get("end_utc") or ""),
                str(x.get("net_row_signature") or ""),
                str(x.get("sop_row_signature") or ""),
            ),
        ):
            parts.append(
                f"P|{p.get('policy','')}|{p.get('start_utc','')}|{p.get('end_utc','')}|"
                f"{p.get('net_row_signature','')}|{p.get('sop_row_signature','')}"
            )
        return ";".join(parts)

    def _maybe_rebuild_if_changed(self):
        hf_sched, net_sched, sop_sched, policy_rows = self._load_schedules()
        snap = self._snapshot(hf_sched, net_sched, sop_sched, policy_rows)
        if snap != self._last_snapshot:
            self.rebuild_table()

    def _ui_tz_abbr(self, tz_name: str, fallback: str) -> str:
        mapping = {
            "UTC": "UTC",
            "America/New_York": "ET",
            "America/Chicago": "CT",
            "America/Denver": "MT",
            "America/Los_Angeles": "PT",
            "Mountain Standard Time": "MST",
            "Central Standard Time": "CST",
            "Eastern Standard Time": "EST",
            "Pacific Standard Time": "PST",
        }
        return mapping.get(tz_name, fallback)

    def _update_clock_labels(self):
        """
        UTC from system clock; local time derived via Settings timezone + get_timezone(),
        with a UI label like ET / CT / MT / PT / UTC.
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
        self.time_toggle_btn.setText("Times: Local" if self._show_local else "Times: UTC")
        self._update_toggle_button_styles()

    def _setup_clock_timer(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._on_clock_tick)
        self._clock_timer.start(1000)

    def _on_clock_tick(self):
        self._update_clock_labels()
        now_ts = time.time()
        if now_ts - self._last_rebuild_check_ts >= 2.0:
            self._last_rebuild_check_ts = now_ts
            self._maybe_rebuild_if_changed()

    def set_tab_active(self, active: bool) -> None:
        if active:
            if self._clock_timer is None:
                self._setup_clock_timer()
            elif not self._clock_timer.isActive():
                self._clock_timer.start(1000)
            self._update_clock_labels()
            self._maybe_rebuild_if_changed()
            return
        if self._clock_timer and self._clock_timer.isActive():
            self._clock_timer.stop()

    def _toggle_time_view(self):
        self._show_local = not self._show_local
        self._update_toggle_button_styles()
        self.rebuild_table()
        self._update_toggle_button_styles()

    def _toggle_band_view(self):
        self._show_band = not self._show_band
        self.band_toggle_btn.setText("Showing Band" if self._show_band else "Showing Frequency")
        self._update_toggle_button_styles()
        self.rebuild_table()
        self._update_toggle_button_styles()

    def _load_band_colors(self) -> None:
        raw = self.settings.get("band_colors", {}) or {}
        self._band_colors = {}
        for k, v in raw.items():
            if not k or not v:
                continue
            self._band_colors[str(k).lower().strip()] = str(v).strip()

    def _default_band_colors(self) -> Dict[str, str]:
        theme = resolve_theme(self.settings)
        is_dark = theme.get("bg") == "#0F1216"
        palette = BAND_COLORS_DARK if is_dark else BAND_COLORS_LIGHT
        return {k.lower(): v for k, v in palette.items()}

    def _band_cell_colors(self, band: str, theme: Dict[str, str]) -> Dict[str, str] | None:
        band_key = (band or "").strip().lower()
        if not band_key:
            return None
        base = self._band_colors.get(band_key)
        if not base:
            return band_cell_colors(band_key, theme)
        alpha = 0.18 if theme.get("bg") == "#0F1216" else 0.28
        bg = self._blend_hex(base, theme.get("surface", "#F0F2F4"), alpha)
        fg = self._pick_text_color(bg, theme.get("text", "#1C1F21"), "#111111")
        return {"bg": bg, "fg": fg, "border": base}

    def _render_band_legend(self) -> None:
        while self.band_legend_layout.count():
            item = self.band_legend_layout.takeAt(0)
            if item.widget():
                if item.widget() is self.band_toggle_btn:
                    item.widget().setParent(None)
                else:
                    item.widget().deleteLater()

        theme = resolve_theme(self.settings)
        if not hasattr(self, "band_toggle_btn") or self.band_toggle_btn is None:
            self.band_toggle_btn = QPushButton("Showing Band")
            self.band_toggle_btn.clicked.connect(self._toggle_band_view)
        self._update_toggle_button_styles(theme=theme)
        self.band_toggle_btn.setText("Showing Band" if self._show_band else "Showing Frequency")
        self.band_legend_layout.addWidget(self.band_toggle_btn)

        if not self._visible_bands:
            empty = QLabel("Band colors: none")
            empty.setStyleSheet(f"color: {theme['text_muted']};")
            self.band_legend_layout.addWidget(empty)
            self.band_legend_layout.addStretch()
            return

        label = QLabel("Band colors:")
        label.setStyleSheet(f"color: {theme['text_muted']};")
        self.band_legend_layout.addWidget(label)

        for band in self._visible_bands:
            btn = QPushButton(band.upper())
            btn.setCursor(Qt.PointingHandCursor)
            btn.setProperty("band_key", band)
            btn.clicked.connect(self._on_band_color_clicked)
            btn.setStyleSheet(self._band_chip_style(band, theme))
            self.band_legend_layout.addWidget(btn)

        self.band_legend_layout.addStretch()

    def _band_chip_style(self, band: str, theme: Dict[str, str]) -> str:
        band_key = (band or "").strip().lower()
        base = self._band_colors.get(band_key) or self._default_band_colors().get(band_key)
        if not base:
            base = theme.get("surface_alt", "#DDE1E6")
        fg = self._pick_text_color(base, theme.get("text", "#1C1F21"), "#111111")
        return (
            "QPushButton {"
            f" background-color: {base}; color: {fg}; border: 1px solid {theme['border']};"
            " border-radius: 10px; padding: 2px 10px; font-weight: 600;"
            " }"
            " QPushButton:hover { opacity: 0.9; }"
        )

    def _on_band_color_clicked(self) -> None:
        btn = self.sender()
        if not isinstance(btn, QPushButton):
            return
        band_key = (btn.property("band_key") or "").strip().lower()
        if not band_key:
            return
        current = self._band_colors.get(band_key) or self._default_band_colors().get(band_key, "#CCCCCC")
        selected = self._pick_band_color(band_key, current)
        if not selected:
            return
        self._band_colors[band_key] = selected
        self.settings.set("band_colors", dict(self._band_colors))
        self.rebuild_table()

    def _reset_band_colors(self) -> None:
        self._band_colors = {}
        self.settings.set("band_colors", {})
        self.rebuild_table()

    def _pick_band_color(self, band_key: str, current: str) -> str | None:
        dialog = QColorDialog(qcolor(current), self)
        dialog.setOption(QColorDialog.DontUseNativeDialog, True)
        dialog.setWindowTitle(f"Select {band_key.upper()} Color")
        reset_btn = QPushButton("Reset Default")
        reset_btn.setAutoDefault(False)
        reset_btn.setDefault(False)
        layout = dialog.layout()
        if layout is not None:
            if hasattr(layout, "rowCount"):
                row = layout.rowCount()
                layout.addWidget(reset_btn, row, 0, 1, layout.columnCount())
            else:
                layout.addWidget(reset_btn)

        def reset_and_accept():
            default = self._default_band_colors().get(band_key, current)
            dialog.setCurrentColor(qcolor(default))
            dialog.done(QDialog.Accepted)

        reset_btn.clicked.connect(reset_and_accept)
        if dialog.exec() != QDialog.Accepted:
            return None
        color = dialog.currentColor()
        if not color.isValid():
            return None
        return color.name().upper()

    def _hex_to_rgb(self, value: str) -> tuple[int, int, int]:
        value = (value or "").lstrip("#")
        if len(value) != 6:
            return 0, 0, 0
        return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)

    def _rgb_to_hex(self, r: int, g: int, b: int) -> str:
        return f"#{r:02X}{g:02X}{b:02X}"

    def _blend_hex(self, fg: str, bg: str, alpha: float) -> str:
        fr, fg_c, fb = self._hex_to_rgb(fg)
        br, bg_c, bb = self._hex_to_rgb(bg)
        r = int(fr * alpha + br * (1 - alpha))
        g = int(fg_c * alpha + bg_c * (1 - alpha))
        b = int(fb * alpha + bb * (1 - alpha))
        return self._rgb_to_hex(r, g, b)

    def _luminance(self, value: str) -> float:
        r, g, b = self._hex_to_rgb(value)
        return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255.0

    def _pick_text_color(self, bg_hex: str, light: str, dark: str) -> str:
        return dark if self._luminance(bg_hex) > 0.6 else light

    def _apply_theme(self):
        theme = resolve_theme(self.settings)
        self._update_toggle_button_styles(theme=theme)
        self._render_band_legend()

    def _update_toggle_button_styles(self, theme: Dict[str, str] | None = None) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        # Local + Band are defaults; highlight when user selects an alternate view.
        # Use explicit info styling so the active alternate state is clearly visible.
        time_role = "info" if not self._show_local else "muted"
        band_role = "info" if not self._show_band else "muted"
        self.time_toggle_btn.setStyleSheet(button_style(time_role, theme))
        self.band_toggle_btn.setStyleSheet(button_style(band_role, theme))

    def on_settings_saved(self):
        try:
            self.settings.reload()
        except Exception:
            pass
        self.plan_context_label.invalidate_context()
        self._apply_theme()
        self.rebuild_table()

    def on_condition_levels_changed(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        if self.isVisible():
            self.rebuild_table()

    def apply_theme(self):
        self._apply_theme()

    # ------------- Qt events ------------- #

    def showEvent(self, event):
        """
        Rebuild the planner whenever the tab becomes visible, so changes from
        HF Schedule or Net Schedule are reflected immediately.
        """
        super().showEvent(event)
        try:
            self.rebuild_table()
        except Exception as e:
            log.error("Failed to rebuild FreqPlanner on showEvent: %s", e)
