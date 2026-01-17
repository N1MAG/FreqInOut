from __future__ import annotations

import datetime
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional
import re

from PySide6.QtCore import Qt, QTimer, QEvent
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QTextEdit,
    QPushButton,
    QMessageBox,
    QComboBox,
    QApplication,
    QCompleter,
)
from PySide6.QtGui import QFontMetrics

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.core.checkins_db import upsert_checkins
from freqinout.core.config_paths import get_fldigi_checkin_dir
from freqinout.utils.timezones import get_timezone
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
from freqinout.gui.operator_history_tab import OperatorHistoryTab  # for schema helper
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

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()

        self._net_in_progress = False
        self._net_start_utc: Optional[str] = None

        self._clock_timer: Optional[QTimer] = None

        # Next frequency change tracking
        self._next_change_utc: Optional[datetime.datetime] = None
        self._auto_end_done: bool = False
        self._suspend_warned: bool = False
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

        self._build_ui()
        self._apply_theme()
        self._load_settings()
        self._load_known_operators()
        self._setup_timers()
        self._refresh_qsy_options()
        self._set_net_button_styles(active=False)

    # ---------------- UI BUILD ---------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Header with clocks
        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>FLDigi Net Control</h3>"))
        header.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        layout.addLayout(header)

        # Next scheduled frequency change display (bordered)
        self.next_change_label = QLabel("Next Scheduled Net: (unknown)")
        self.next_change_label.setAlignment(Qt.AlignCenter)
        self.next_change_label.setStyleSheet(
            "QLabel { border: 1px solid #888888; padding: 4px; border-radius: 3px; }"
        )
        layout.addWidget(self.next_change_label)

        # Role + Net Name row
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("Role:"))
        self.role_combo = QComboBox()
        self.role_combo.addItems(["NCS", "ANCS", "JOINER"])
        top_row.addWidget(self.role_combo)

        top_row.addSpacing(20)
        top_row.addWidget(QLabel("Net Name:"))
        self.net_name_combo = QComboBox()
        self.net_name_combo.setEditable(True)
        self.net_name_combo.setInsertPolicy(QComboBox.NoInsert)
        self.net_name_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        top_row.addWidget(self.net_name_combo, stretch=1)

        top_row.addStretch()
        layout.addLayout(top_row)

        # Suspend / QSY button row (under net name label)
        suspend_row = QHBoxLayout()
        suspend_row.addStretch()
        self.qsy_combo = QComboBox()
        self.qsy_combo.currentIndexChanged.connect(self._update_qsy_button_enabled)
        suspend_row.addWidget(self.qsy_combo)
        self.suspend_btn = QPushButton("QSY")
        self.suspend_btn.clicked.connect(self._on_suspend_clicked)
        suspend_row.addWidget(self.suspend_btn)
        self.ad_hoc_btn = QPushButton("Ad Hoc Net")
        self.ad_hoc_btn.clicked.connect(self._start_ad_hoc_net)
        suspend_row.addWidget(self.ad_hoc_btn)
        layout.addLayout(suspend_row)

        # Known operators row (DB-backed auto-suggest)
        known_row = QHBoxLayout()
        known_row.addWidget(QLabel("Operator Lookup/Add:"))
        self.known_op_edit = QLineEdit()
        self.known_op_edit.setPlaceholderText("Enter Callsign Name State...")
        self.known_op_edit.setMaximumWidth(260)
        known_row.addWidget(self.known_op_edit, stretch=1)
        known_row.addStretch()
        layout.addLayout(known_row)

        known_btn_row = QHBoxLayout()
        self.add_known_main_btn = QPushButton("Add to Main")
        self.add_known_late_btn = QPushButton("Add to New/Late")
        self.add_known_main_btn.setFocusPolicy(Qt.StrongFocus)
        self.add_known_late_btn.setFocusPolicy(Qt.StrongFocus)
        known_btn_row.addWidget(self.add_known_main_btn)
        known_btn_row.addWidget(self.add_known_late_btn)
        known_btn_row.addStretch()
        layout.addLayout(known_btn_row)

        layout.addSpacing(24)

        # Two panels for check-ins
        panels_row = QHBoxLayout()

        # Left: Main log
        left_col = QVBoxLayout()
        main_header = QHBoxLayout()
        self.copy_main_btn = QPushButton("Copy")
        self.copy_main_btn.clicked.connect(lambda: self._copy_text_to_clipboard(self.main_text.toPlainText()))
        main_header.addWidget(self.copy_main_btn)
        main_header.addWidget(QLabel("<b>Main Check-in Log</b>"))
        main_header.addStretch()
        left_col.addLayout(main_header)
        self.main_text = QTextEdit()
        self.main_text.setReadOnly(False)
        left_col.addWidget(self.main_text)

        # Right: New/Late
        right_col = QVBoxLayout()
        late_header = QHBoxLayout()
        self.copy_late_btn = QPushButton("Copy")
        self.copy_late_btn.clicked.connect(lambda: self._copy_text_to_clipboard(self.late_text.toPlainText()))
        late_header.addWidget(self.copy_late_btn)
        late_header.addWidget(QLabel("<b>New / Late Check-ins</b>"))
        late_header.addStretch()
        right_col.addLayout(late_header)
        self.late_text = QTextEdit()
        self.late_text.setReadOnly(False)
        right_col.addWidget(self.late_text)

        panels_row.addLayout(left_col, stretch=1)
        panels_row.addLayout(right_col, stretch=1)

        # Ensure both text panels expand equally
        left_col.setStretch(1, 1)   # index 1 = main_text
        right_col.setStretch(1, 1)  # index 1 = late_text
        panels_row.setStretch(0, 1)
        panels_row.setStretch(1, 1)

        layout.addLayout(panels_row)

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
        btn_row.addWidget(self.merge_btn)
        btn_row.addWidget(self.end_btn)
        btn_row.addStretch()

        layout.addLayout(btn_row)

        # Signals
        self.main_text.textChanged.connect(self._on_main_text_changed)
        self.late_text.textChanged.connect(self._on_late_text_changed)
        self.known_op_edit.textChanged.connect(self._on_known_op_text_changed)
        self.known_op_edit.returnPressed.connect(self._on_known_op_return)
        self.known_op_edit.installEventFilter(self)
        self.setTabOrder(self.known_op_edit, self.add_known_main_btn)
        self.setTabOrder(self.add_known_main_btn, self.add_known_late_btn)

        self.start_btn.clicked.connect(self._start_net)
        self.save_btn.clicked.connect(self._save_checkins)
        self.end_btn.clicked.connect(self._end_net)
        self.merge_btn.clicked.connect(self._merge_late_into_main)

        self.add_known_main_btn.clicked.connect(self._insert_known_into_main)
        self.add_known_late_btn.clicked.connect(self._insert_known_into_late)
        self.add_known_main_btn.setDefault(False)
        self.add_known_late_btn.setDefault(False)
        self.add_known_main_btn.installEventFilter(self)
        self.add_known_late_btn.installEventFilter(self)

    def _set_net_button_styles(self, active: bool):
        """
        Update button highlight when a net is running.
        """
        theme = resolve_theme(self.settings)
        if active:
            self.start_btn.setStyleSheet(button_style("muted", theme))
            self.save_btn.setStyleSheet(button_style("success", theme))
            self.merge_btn.setStyleSheet(button_style("info", theme))
            self.end_btn.setStyleSheet(button_style("danger", theme))
            self.ad_hoc_btn.setStyleSheet(button_style("info", theme))
            self.ad_hoc_btn.setEnabled(False)
        else:
            self.start_btn.setStyleSheet(self._start_btn_default_style)
            self.save_btn.setStyleSheet(button_style("muted", theme))
            self.merge_btn.setStyleSheet(button_style("muted", theme))
            self.end_btn.setStyleSheet(button_style("muted", theme))
            self.ad_hoc_btn.setStyleSheet(button_style("info", theme))
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

    # ---------------- TIMERS & CLOCKS ---------------- #

    def _setup_timers(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._on_timer_tick)
        self._clock_timer.start(1000)
        self._update_clock_labels()
        self._update_suspend_state()
        self._update_next_change_display()

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

    def _refresh_qsy_options(self, og_list: Optional[List[Dict]] = None):
        """
        Build a unique frequency list from Operating Groups (auto-tune wins on duplicates).
        """
        ops = og_list if og_list is not None else self._load_operating_groups()
        self._opgroups_sig = self._snapshot_operating_groups(ops)
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

    def _on_timer_tick(self):
        self._update_clock_labels()
        self._update_suspend_state()
        self._update_next_change_display()

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

    def _update_suspend_state(self):
        enabled = self._scheduler_enabled()
        self.suspend_btn.setEnabled(enabled)
        if not enabled:
            self._suspend_warned = False
            self._set_suspend_button(active=False)
            self._update_qsy_button_enabled()
            return

        suspend_until = self._get_suspend_until()
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if suspend_until is None:
            self._suspend_warned = False
            self._set_suspend_button(active=False)
            return

        if now_utc >= suspend_until:
            # Suspension expired
            self._set_suspend_until(None)
            self._suspend_warned = False
            self._set_suspend_button(active=False)
            return

        # 5-minute warning prompt
        remaining = (suspend_until - now_utc).total_seconds()
        if remaining <= 300 and not self._suspend_warned:
            self._suspend_warned = True
            resp = QMessageBox.question(
                self,
                "Schedule Resume Soon",
                "Scheduling will resume in 5 minutes. Extend 30 minutes?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if resp == QMessageBox.Yes:
                # Extend by 30 minutes from the original expiry
                new_until = suspend_until + datetime.timedelta(minutes=30)
                self._set_suspend_until(new_until)
                self._suspend_warned = False  # allow another warning near new expiry
        self._set_suspend_button(active=True, remaining_sec=remaining)
        self._update_qsy_button_enabled()

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

    def _apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.start_btn.setStyleSheet(button_style("success", theme))
        self.end_btn.setStyleSheet(button_style("danger", theme))
        self.merge_btn.setStyleSheet(button_style("info", theme))
        self._start_btn_default_style = self.start_btn.styleSheet()
        self._save_btn_default_style = button_style("success", theme)
        self.save_btn.setStyleSheet(self._save_btn_default_style)
        self.suspend_btn.setStyleSheet(button_style("warning", theme))
        self.ad_hoc_btn.setStyleSheet(button_style("info", theme))
        self._set_net_button_styles(self._net_in_progress)
        self._update_copy_buttons_state()
        self._update_add_buttons_state()

    def apply_theme(self) -> None:
        self._apply_theme()

    def _on_suspend_clicked(self):
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if self._suspend_active():
            # Resume immediately
            self._set_suspend_until(None)
            self._suspend_warned = False
            self._set_suspend_button(active=False)
            QMessageBox.information(self, "Scheduling", "Scheduling resumed.")
        else:
            meta = self._selected_qsy_meta()
            if not meta:
                QMessageBox.warning(self, "QSY", "Select a frequency to QSY to.")
                return
            if not self._perform_qsy(meta):
                return
            self._set_suspend_until(now_utc + datetime.timedelta(minutes=30))
            self._suspend_warned = False
            su = self._get_suspend_until()
            remaining = (su - datetime.datetime.now(datetime.timezone.utc)).total_seconds() if su else None
            self._set_suspend_button(active=True, remaining_sec=remaining)
            QMessageBox.information(
                self,
                "QSY Applied",
                "Frequency changed and scheduling paused for 30 minutes.",
            )

    def _compute_next_change_utc(self) -> Optional[datetime.datetime]:
        """
        Ask scheduler_engine for the next scheduled frequency change time (UTC).
        """
        # Prefer the newer scheduler_engine compute_next_change_time(now_utc,...)
        try:
            from freqinout.core.scheduler_engine import compute_next_change_time as cnext
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            hf_active, net_active = self._active_schedule_entries(now_utc)
            dt = cnext(now_utc, hf_active, net_active)
        except Exception:
            # Fallback to legacy helper if available
            try:
                from freqinout.core import scheduler_engine_orig as se_orig
                dt = se_orig.compute_next_change_time()
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

        day_name = (row.get("day_utc") or "").strip()
        day_idx = day_to_idx(day_name) if day_name else None
        if day_idx is None:
            return None
        start_t = parse_time(row.get("start_utc", ""))
        end_t = parse_time(row.get("end_utc", ""))
        if start_t is None or end_t is None:
            return None

        recurrence = (row.get("recurrence") or "Weekly").strip()
        interval_weeks = 2 if recurrence == "Bi-Weekly" else 1
        offset_weeks = int(row.get("biweekly_offset_weeks", 0) or 0)
        early = int(row.get("early_checkin", 0) or 0)

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

    # ---------------- SETTINGS LOAD ---------------- #

    def _load_settings(self):
        data = self.settings.all()

        # Net schedule net names for autocomplete
        net_sched = data.get("net_schedule", [])
        net_names = sorted(
            {row.get("net_name", "") for row in net_sched if isinstance(row, dict) and row.get("net_name")}
        )

        self._resolve_checkin_dir()

        self._populate_net_name_from_schedule()
        self._update_net_name_min_width()

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
        matches = list(re.finditer(r"[A-Za-z0-9]+", line))
        if not matches:
            return "", "", "", ""
        cs = matches[0].group(0).upper()
        name = matches[1].group(0) if len(matches) > 1 else ""
        state = matches[2].group(0).upper() if len(matches) > 2 else ""
        extra = self._extract_extra_after_tokens(line, min_tokens=3)
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
        if target == "main":
            self.add_known_main_btn.setFocus()
        elif target == "late":
            self.add_known_late_btn.setFocus()

    def eventFilter(self, obj, event):
        if obj in (self.add_known_main_btn, self.add_known_late_btn) and event.type() == QEvent.KeyPress:
            if event.key() in (Qt.Key_Return, Qt.Key_Enter, Qt.Key_Space):
                if obj is self.add_known_main_btn:
                    self._insert_known_into_main()
                else:
                    self._insert_known_into_late()
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
                    self._known_op_pending_focus = "main"
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
        theme = resolve_theme(self.settings)
        main_has = bool(self.main_text.toPlainText().strip())
        late_has = bool(self.late_text.toPlainText().strip())
        self.copy_main_btn.setStyleSheet(
            button_style("success", theme) if main_has else button_style("muted", theme)
        )
        self.copy_late_btn.setStyleSheet(
            button_style("info", theme) if late_has else button_style("muted", theme)
        )

    def _update_add_buttons_state(self) -> None:
        theme = resolve_theme(self.settings)
        text = self.known_op_edit.text().strip()
        cs, name, state = self._parse_checkin_line(text)
        ready = bool(cs and name and state)
        self.add_known_main_btn.setStyleSheet(
            button_style("success", theme) if ready else button_style("muted", theme)
        )
        self.add_known_late_btn.setStyleSheet(
            button_style("info", theme) if ready else button_style("muted", theme)
        )

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

    def _checkin_file_paths(self) -> tuple[str, str]:
        base = self._resolve_checkin_dir()
        return str(base / "main_checkins.txt"), str(base / "new-late_checkins.txt")

    def _ensure_checkin_files(self) -> tuple[str, str]:
        base = self._resolve_checkin_dir()
        main_path = base / "main_checkins.txt"
        late_path = base / "new-late_checkins.txt"
        base.mkdir(parents=True, exist_ok=True)
        if not main_path.exists():
            main_path.touch()
        if not late_path.exists():
            late_path.touch()
        return str(main_path), str(late_path)

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
        if not self._validate_before_start():
            return
        main_path, late_path = self._ensure_checkin_files()

        # Load existing file contents
        # Clear files to avoid loading stale/pre-populated data
        self._write_file(main_path, "")
        self._write_file(late_path, "")
        self.main_text.setPlainText("")
        self.late_text.setPlainText("")

        # Append operator line CALLSIGN / NAME / STATE [/ ROLE]
        cs = (self.settings.get("operator_callsign", "") or "").strip().upper()
        name = (self.settings.get("operator_name", "") or "").strip()
        state = (self.settings.get("operator_state", "") or "").strip().upper()
        if cs or name or state:
            op_line = self._format_entry(cs, name, state)
            role = self.role_combo.currentText().strip().upper()
            if role in ("NCS", "ANCS") and op_line:
                op_line = f"{op_line} / {role}"
            if self.main_text.toPlainText().strip():
                self.main_text.append(op_line)
            else:
                self.main_text.setPlainText(op_line)
            self._append_file(main_path, op_line + "\n")

        self._net_in_progress = True
        self._net_start_utc = datetime.datetime.utcnow().isoformat(timespec="seconds")
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
        ts = datetime.datetime.utcnow().strftime("%Y%m%d %H:%M")
        ad_hoc_name = f"FLDIGI - Ad Hoc - {ts} UTC"
        self.net_name_combo.setEditText(ad_hoc_name)
        self._start_net()

    def _save_checkins(self):
        main_path, late_path = self._ensure_checkin_files()

        main_text = self.main_text.toPlainText()
        late_text = self.late_text.toPlainText()

        self._write_file(main_path, main_text)
        self._write_file(late_path, late_text)

        QMessageBox.information(self, "Saved", "Check-in logs saved.")
        self._refresh_operator_history_views()

    def _merge_late_into_main(self):
        main_path, late_path = self._ensure_checkin_files()

        # Prefer current UI content (may have manual edits) and fall back to file
        late_from_file = self.late_text.toPlainText()
        if not late_from_file.strip():
            late_from_file = self._read_file(late_path)
        if not late_from_file.strip():
            self.late_text.clear()
            self._write_file(late_path, "")
            return

        # Append to main file & panel
        main_text = self.main_text.toPlainText()
        appended_lines = []
        for raw in late_from_file.splitlines():
            cs, name, state = self._parse_checkin_line(raw)
            if not cs and not name and not state:
                continue
            appended_lines.append(self._format_entry(cs, name, state))
        appended_block = "\n".join(appended_lines).strip()
        if appended_block:
            if main_text.strip():
                new_main = main_text.rstrip("\n") + "\n" + appended_block + "\n"
            else:
                new_main = appended_block + "\n"
        else:
            new_main = main_text

        self.main_text.setPlainText(new_main)
        self._write_file(main_path, new_main)

        # Clear late file and panel
        self.late_text.clear()
        self._write_file(late_path, "")

        # No popup needed; UI visibly clears the late list.

    def _copy_summary(self):
        """
        Copy a summary of callsigns from the Main Check-in Log to the clipboard.
        """
        text = self.main_text.toPlainText()
        callsigns = []
        for line in text.splitlines():
            cs, _, _ = self._parse_checkin_line(line)
            if cs:
                callsigns.append(cs)
        seen = set()
        unique = []
        for cs in callsigns:
            if cs not in seen:
                seen.add(cs)
                unique.append(cs)

        summary = " ".join(unique)
        QApplication.clipboard().setText(summary)
        QMessageBox.information(self, "Copied", "Summary of check-in callsigns copied to clipboard.")

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

        main_path, _ = self._checkin_file_paths()

        main_text = self._read_file(main_path)
        if not main_text.strip():
            resp = QMessageBox.question(
                self,
                "End Net?",
                "Main Check-in Log file is empty. End the net without importing any check-ins?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if resp != QMessageBox.Yes:
                return
            # End net even though no check-ins exist
            self._net_in_progress = False
            self._set_net_button_styles(active=False)
            log.info("FLDigi net ended (no check-ins file content).")
            return

        now_utc = datetime.datetime.utcnow().isoformat(timespec="seconds")
        net_name = self.net_name_combo.currentText().strip()
        role = self.role_combo.currentText().strip().upper()

        entries: List[Dict] = []
        for line in main_text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cs, name, state, _extra = self._split_checkin_with_extra(line)
            cs, name, state = self._normalize_checkin_fields(cs, name, state)
            if not cs:
                continue
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
                }
            )

        if entries:
            upsert_checkins(entries)
            self._bump_operator_history(entries)
            # Rewrite main text with normalized formatting
            normalized_lines = [self._format_entry(e["callsign"], e["name"], e["state"]) for e in entries]
            self.main_text.setPlainText("\n".join(normalized_lines) + "\n")
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

        self._net_in_progress = False
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
        formatted = self._format_entry(cs, name, state)
        if extra:
            formatted = f"{formatted} {extra}"
        if not formatted:
            return
        if self.main_text.toPlainText().strip():
            self.main_text.append(formatted)
        else:
            self.main_text.setPlainText(formatted)
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
        formatted = self._format_entry(cs, name, state)
        if extra:
            formatted = f"{formatted} {extra}"
        if not formatted:
            return
        if self.late_text.toPlainText().strip():
            self.late_text.append(formatted)
        else:
            self.late_text.setPlainText(formatted)
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
        line = line.strip()
        if not line:
            return "", "", ""

        if "/" in line and not line.startswith("#"):
            parts = [p.strip() for p in line.split("/") if p.strip()]
            if len(parts) >= 3:
                return parts[0].split()[0].upper(), parts[1], parts[2]
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
                today_str = datetime.datetime.utcnow().strftime("%Y%m%d")
                for e in entries:
                    cs = (e.get("callsign") or "").strip().upper()
                    if not cs:
                        continue
                    date_added = (e.get("date_added") or "").strip() or today_str
                    trusted_in = e.get("trusted")
                    trusted_val = 0 if trusted_in is False or trusted_in == 0 else None
                    cur.execute(
                        """
                        INSERT INTO operator_checkins (callsign, name, state, date_added, checkin_count, trusted)
                        VALUES (?, ?, ?, ?, 1, COALESCE(?, 0))
                        ON CONFLICT(callsign) DO UPDATE SET
                            name=excluded.name,
                            state=excluded.state,
                            date_added=COALESCE(operator_checkins.date_added, excluded.date_added),
                            checkin_count=operator_checkins.checkin_count + 1,
                            trusted=COALESCE(operator_checkins.trusted, excluded.trusted)
                        """,
                        (
                            cs,
                            (e.get("name") or "").strip(),
                            (e.get("state") or "").strip().upper(),
                            date_added,
                            trusted_val,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as ex:
            log.error("Failed to bump operator history: %s", ex)

