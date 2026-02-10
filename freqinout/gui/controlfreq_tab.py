from __future__ import annotations

import datetime as dt
import json
import math
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFontMetrics
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QComboBox,
    QPushButton,
    QGroupBox,
    QTableWidget,
    QTableWidgetItem,
    QSizePolicy,
    QMessageBox,
    QHeaderView,
    QFrame,
)

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sop_manager import SOPManager
from freqinout.utils.timezones import get_timezone
from freqinout.gui.qsy_helper import (
    load_operating_groups,
    selected_qsy_meta,
    perform_qsy,
    current_scheduler_freq,
)
from freqinout.gui.stations_map_tab import (
    FEMA_REGIONS,
    PROP_BANDS,
    PROP_DEFAULT_PROFILES,
    STATE_CENTERS,
    STATE_TO_FEMA_REGION,
    maidenhead_to_latlon,
)
from freqinout.gui.theme import resolve_theme, button_style


class ControlFreqTab(QWidget):
    """
    ControlFreq: summary/console view for activity and operational status.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._timer: Optional[QTimer] = None
        self._active = False
        self._last_refresh_ts = 0.0
        self._freq_timer: Optional[QTimer] = None
        self._show_local = True
        self._intersection_cache_ts = 0.0
        self._intersection_cache_key: Tuple[str, str] = ("", "")
        self._intersection_cache_rows: List[List[str]] = []
        self._prop_db_cache: Dict[Tuple[str, int, int, int], float] = {}
        self._prop_db_loaded = False
        self._prop_db_available = False
        self._build_ui()
        self._apply_theme()
        self._refresh_all()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel("<h3>ControlFreq</h3>")
        header.addWidget(title)

        header.addStretch(1)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter by keyword...")
        self.search_edit.textChanged.connect(self._refresh_all)
        self.search_edit.setMinimumWidth(340)
        self.search_edit.setMaximumWidth(420)
        header.addWidget(self.search_edit)

        self.group_combo = QComboBox()
        self.group_combo.setMinimumWidth(180)
        self.group_combo.currentIndexChanged.connect(self._refresh_all)
        header.addWidget(self.group_combo)

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self._refresh_all)
        header.addWidget(self.refresh_btn)

        self.time_toggle_btn = QPushButton("Showing: Local")
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        header.addWidget(self.time_toggle_btn)

        root.addLayout(header)

        updated_row = QHBoxLayout()
        updated_row.addStretch(1)
        self.updated_label = QLabel("Last updated: --")
        self.updated_label.setStyleSheet("color: #888;")
        updated_row.addWidget(self.updated_label)
        root.addLayout(updated_row)

        # Activity + Frequency Control
        activity_row = QHBoxLayout()

        self.activity_box = QGroupBox("Activity")
        act_layout = QVBoxLayout(self.activity_box)
        act_header = QHBoxLayout()
        act_header.addWidget(QLabel("Window"))
        self.activity_window_combo = QComboBox()
        self.activity_window_combo.addItem("30m", 30)
        self.activity_window_combo.addItem("1h", 60)
        self.activity_window_combo.addItem("2h", 120)
        self.activity_window_combo.addItem("6h", 360)
        self.activity_window_combo.addItem("12h", 720)
        self.activity_window_combo.addItem("24h", 1440)
        self.activity_window_combo.setCurrentIndex(2)
        self.activity_window_combo.currentIndexChanged.connect(self._refresh_activity)
        act_header.addWidget(self.activity_window_combo)
        act_header.addStretch(1)
        act_layout.addLayout(act_header)
        self.activity_table = QTableWidget(0, 4)
        self.activity_table.setHorizontalHeaderLabels(
            ["Group", "Band/Freq", "Callsigns Seen", "Traffic"]
        )
        self.activity_table.horizontalHeader().setStretchLastSection(True)
        self.activity_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        act_layout.addWidget(self.activity_table)
        activity_row.addWidget(self.activity_box, 1)

        self.freq_ctrl_box = QGroupBox("Frequency Control")
        freq_layout = QVBoxLayout(self.freq_ctrl_box)
        self.freq_ctrl_label = QLabel("Scheduled: --")
        self.freq_ctrl_label.setWordWrap(True)
        self.freq_ctrl_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.freq_ctrl_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        freq_layout.addWidget(self.freq_ctrl_label)
        self.freq_active_label = QLabel("Active: --")
        self.freq_active_label.setWordWrap(True)
        self.freq_active_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.freq_active_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        freq_layout.addWidget(self.freq_active_label)
        self.freq_combo = QComboBox()
        self.freq_combo.setMinimumWidth(220)
        self.freq_combo.currentIndexChanged.connect(self._on_freq_selection_changed)
        freq_layout.addWidget(self.freq_combo)
        btn_row = QHBoxLayout()
        self.freq_set_btn = QPushButton("Set Frequency")
        self.freq_set_btn.clicked.connect(self._on_freq_set_clicked)
        btn_row.addWidget(self.freq_set_btn)
        self.freq_resume_btn = QPushButton("Resume Schedule")
        self.freq_resume_btn.clicked.connect(self._on_resume_schedule_clicked)
        btn_row.addWidget(self.freq_resume_btn)
        freq_layout.addLayout(btn_row)

        freq_layout.addSpacing(6)
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Sunken)
        freq_layout.addWidget(sep)
        freq_layout.addSpacing(4)

        inter_header_row = QHBoxLayout()
        self.intersection_label = QLabel("Schedule Intersections (Now +2h)")
        self.intersection_label.setStyleSheet("font-weight: bold;")
        inter_header_row.addWidget(self.intersection_label)
        self.intersection_info = QLabel("?")
        self.intersection_info.setToolTip(
            "Exact-frequency overlaps between your schedule and peer schedules\n"
            "for now and the next two hours."
        )
        self.intersection_info.setStyleSheet(
            "font-weight: bold; border: 1px solid #888; border-radius: 8px; "
            "padding: 0 4px;"
        )
        inter_header_row.addWidget(self.intersection_info)
        inter_header_row.addStretch(1)
        freq_layout.addLayout(inter_header_row)
        self.intersection_table = QTableWidget(0, 3)
        self.intersection_table.setHorizontalHeaderLabels(
            ["When", "Overlaps", "Group/Band/Freq"]
        )
        inter_header = self.intersection_table.horizontalHeader()
        inter_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        inter_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        inter_header.setSectionResizeMode(2, QHeaderView.Stretch)
        self.intersection_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.intersection_table.setFixedHeight(110)
        freq_layout.addWidget(self.intersection_table)

        activity_row.addWidget(self.freq_ctrl_box, 1)

        root.addLayout(activity_row)

        # Today + 7 Days
        row = QHBoxLayout()

        self.today_box = QGroupBox("Today")
        today_layout = QVBoxLayout(self.today_box)
        self.today_table = QTableWidget(0, 4)
        self.today_table.setHorizontalHeaderLabels(["When", "Group/Net", "Band/Freq", "Type"])
        self.today_table.horizontalHeader().setStretchLastSection(True)
        today_layout.addWidget(self.today_table)
        row.addWidget(self.today_box, 1)

        self.week_box = QGroupBox("7 Days")
        week_layout = QVBoxLayout(self.week_box)
        self.week_table = QTableWidget(0, 4)
        self.week_table.setHorizontalHeaderLabels(["Day", "Type", "Group/Net", "Band/Freq"])
        self.week_table.horizontalHeader().setStretchLastSection(True)
        week_layout.addWidget(self.week_table)
        row.addWidget(self.week_box, 1)

        root.addLayout(row)

        # Inbox + Propagation
        row2 = QHBoxLayout()
        self.inbox_box = QGroupBox("Inbox Summary")
        inbox_layout = QVBoxLayout(self.inbox_box)
        self.inbox_table = QTableWidget(0, 3)
        self.inbox_table.setHorizontalHeaderLabels(["Type", "Unread", "Top Senders"])
        inbox_header = self.inbox_table.horizontalHeader()
        inbox_header.setStretchLastSection(True)
        inbox_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        inbox_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        inbox_header.setSectionResizeMode(2, QHeaderView.Stretch)
        inbox_layout.addWidget(self.inbox_table)
        row2.addWidget(self.inbox_box, 1)

        self.prop_box = QGroupBox("Propagation Forecast")
        prop_layout = QVBoxLayout(self.prop_box)
        self.prop_table = QTableWidget(0, 4)
        self.prop_table.setHorizontalHeaderLabels(
            ["Zone", "Morning", "Day", "Night"]
        )
        self.prop_table.horizontalHeader().setStretchLastSection(True)
        self.prop_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.prop_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        prop_layout.addWidget(self.prop_table)
        self.prop_hint = QLabel(
            "Modeled snapshot based on today's schedule bands. "
            "Morning = dawn–10:00, Day = 10:00–sunset, Night = sunset–dawn (local)."
        )
        self.prop_hint.setWordWrap(True)
        self.prop_hint.setStyleSheet("color: #666;")
        prop_layout.addWidget(self.prop_hint)
        row2.addWidget(self.prop_box, 1)

        root.addLayout(row2)

    def _apply_theme(self) -> None:
        try:
            theme = resolve_theme(self.settings)
            self.refresh_btn.setStyleSheet(button_style("primary", theme))
            self.freq_set_btn.setStyleSheet(button_style("secondary", theme))
            self.freq_resume_btn.setStyleSheet(button_style("secondary", theme))
            self.time_toggle_btn.setStyleSheet(button_style("primary", theme))
        except Exception:
            pass
        self._update_time_toggle_text()

    def apply_theme(self) -> None:
        self._apply_theme()

    def set_tab_active(self, active: bool) -> None:
        self._active = bool(active)
        if self._active:
            if self._timer is None:
                self._timer = QTimer(self)
                self._timer.timeout.connect(self._refresh_all)
            self._timer.start(60_000)
            if self._freq_timer is None:
                self._freq_timer = QTimer(self)
                self._freq_timer.timeout.connect(self._refresh_frequency_control)
            self._freq_timer.start(2000)
            self._refresh_frequency_control()
            return
        if self._timer:
            self._timer.stop()
        if self._freq_timer:
            self._freq_timer.stop()

    def on_tab_activated(self) -> None:
        self._refresh_all()
        self._refresh_frequency_control()

    def on_settings_saved(self) -> None:
        self._apply_theme()

    def _update_time_toggle_text(self) -> None:
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")

    def _toggle_time_view(self) -> None:
        self._show_local = not self._show_local
        self._update_time_toggle_text()
        self._refresh_today()
        self._refresh_week()

    def _refresh_all(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        self._refresh_frequency_control()
        self._load_group_combo()
        self._refresh_activity()
        self._refresh_today()
        self._refresh_week()
        self._refresh_inbox()
        self._refresh_propagation_snapshot()
        self._last_refresh_ts = time.time()
        ts = dt.datetime.fromtimestamp(self._last_refresh_ts).strftime("%Y-%m-%d %H:%M:%S")
        self.updated_label.setText(f"Last updated: {ts}")

    def _db_path(self) -> Path:
        return get_config_dir() / "config" / "freqinout_nets.db"

    def _settings_db_path(self) -> Path:
        return get_config_dir() / "config" / "freqinout.db"

    def _load_group_combo(self) -> None:
        groups = self._get_operating_groups()
        current = self.group_combo.currentData()
        self.group_combo.blockSignals(True)
        self.group_combo.clear()
        self.group_combo.addItem("All Groups", "")
        for g in groups:
            self.group_combo.addItem(g, g)
        # restore
        idx = self.group_combo.findData(current)
        if idx >= 0:
            self.group_combo.setCurrentIndex(idx)
        self.group_combo.blockSignals(False)

    def _get_operating_groups(self) -> List[str]:
        ops = self.settings.get("operating_groups", []) or []
        out: List[str] = []
        for row in ops:
            grp = str(row.get("group", "") or "").strip().upper()
            if grp and grp not in out:
                out.append(grp)
        return sorted(out)

    def _group_freq_map(self) -> Dict[str, List[float]]:
        ops = self.settings.get("operating_groups", []) or []
        out: Dict[str, List[float]] = {}
        for row in ops:
            grp = str(row.get("group", "") or "").strip().upper()
            if not grp:
                continue
            try:
                freq = float(row.get("frequency"))
            except Exception:
                continue
            out.setdefault(grp, []).append(freq)
        return out

    def _group_band_map(self) -> Dict[str, Set[str]]:
        ops = self.settings.get("operating_groups", []) or []
        out: Dict[str, Set[str]] = {}
        for row in ops:
            grp = str(row.get("group", "") or "").strip().upper()
            band = str(row.get("band", "") or "").strip().upper()
            if not grp or not band:
                continue
            out.setdefault(grp, set()).add(band)
        return out

    def _load_operator_group_map(self) -> Dict[str, Set[str]]:
        db_path = self._db_path()
        mapping: Dict[str, Set[str]] = {}
        if not db_path.exists():
            return mapping
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT callsign, group1, group2, group3, groups_json FROM operator_checkins"
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load operator groups: %s", e)
            return mapping
        for r in rows:
            cs = (r["callsign"] or "").strip().upper()
            if not cs:
                continue
            groups: Set[str] = set()
            for key in ("group1", "group2", "group3"):
                g = (r[key] or "").strip().upper()
                if g:
                    groups.add(g)
            try:
                if r["groups_json"]:
                    gj = json.loads(r["groups_json"])
                    for g in gj or []:
                        g = str(g).strip().upper()
                        if g:
                            groups.add(g)
            except Exception:
                pass
            if groups:
                mapping[cs] = groups
        return mapping

    def _refresh_activity(self) -> None:
        window_minutes = int(self.activity_window_combo.currentData() or 120)
        search = (self.search_edit.text() or "").strip().upper()
        group_filter = self.group_combo.currentData() or ""
        group_freqs = self._group_freq_map()
        group_bands = self._group_band_map()
        sched_freqs, sched_bands = self._scheduled_group_freqs(window_minutes)
        if sched_freqs:
            filtered_freqs: Dict[str, List[float]] = {}
            for grp, freqs in group_freqs.items():
                allowed = sched_freqs.get(grp, set())
                if not allowed:
                    continue
                filtered = [f for f in freqs if any(abs(f - a) < 0.0005 for a in allowed)]
                if filtered:
                    filtered_freqs[grp] = filtered
            group_freqs = filtered_freqs
            if sched_bands:
                filtered_bands: Dict[str, Set[str]] = {}
                for grp, bands in group_bands.items():
                    allowed_b = sched_bands.get(grp, set())
                    if not allowed_b:
                        continue
                    filtered = {b for b in bands if b in allowed_b}
                    if filtered:
                        filtered_bands[grp] = filtered
                group_bands = filtered_bands
        operator_groups = self._load_operator_group_map()
        db_path = self._db_path()
        if not db_path.exists():
            self._set_table_rows(self.activity_table, [])
            return
        now_ts = time.time()
        since_ts = now_ts - (window_minutes * 60)

        # callsigns seen by group based on js8_links + checkins
        group_seen: Dict[str, Set[str]] = {g: set() for g in group_freqs}
        group_traffic: Dict[str, int] = {g: 0 for g in group_freqs}
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT origin, destination, band, freq_hz FROM js8_links WHERE ts >= ?",
                (since_ts,),
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load js8_links: %s", e)
            rows = []
        for origin, dest, band, freq_hz in rows:
            band = (band or "").strip().upper()
            for grp, freqs in group_freqs.items():
                if group_filter and grp != group_filter:
                    continue
                if band and grp in group_bands and band not in group_bands.get(grp, set()):
                    continue
                if freq_hz is None:
                    continue
                try:
                    mhz = float(freq_hz) / 1_000_000.0
                except Exception:
                    continue
                if any(abs(mhz - f) < 0.0005 for f in freqs):
                    if origin:
                        group_seen[grp].add(str(origin).strip().upper())
                    if dest:
                        group_seen[grp].add(str(dest).strip().upper())
                    group_traffic[grp] = group_traffic.get(grp, 0) + 1

        # traffic by group (messages + observed links + checkins)

        def _add_group_traffic(cs: str):
            cs = (cs or "").strip().upper()
            if not cs:
                return
            if search and search not in cs:
                return
            groups = operator_groups.get(cs, set())
            for g in groups:
                if g in group_traffic and (not group_filter or g == group_filter):
                    group_traffic[g] += 1
                    group_seen[g].add(cs)

        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT from_call, utc_ts FROM js8_messages WHERE utc_ts >= ?",
                (since_ts,),
            )
            for cs, _ in cur.fetchall():
                _add_group_traffic(cs)
            cur.execute(
                "SELECT from_call, utc_ts FROM spotter_traffic WHERE utc_ts >= ?",
                (since_ts,),
            )
            for cs, _ in cur.fetchall():
                _add_group_traffic(cs)
            cur.execute(
                "SELECT from_call, ts FROM varac_messages WHERE ts >= ?",
                (since_ts,),
            )
            for cs, _ in cur.fetchall():
                _add_group_traffic(cs)
            cur.execute(
                "SELECT callsign, last_seen_ts FROM fldigi_checkins WHERE last_seen_ts >= ?",
                (since_ts,),
            )
            for cs, _ in cur.fetchall():
                _add_group_traffic(cs)
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load recent messages: %s", e)

        rows_out: List[List[str]] = []
        for grp in sorted(group_freqs.keys()):
            if group_filter and grp != group_filter:
                continue
            bands = sorted(group_bands.get(grp, set()))
            freqs = group_freqs.get(grp, [])
            freq_txt = ", ".join(f"{f:.3f}" for f in freqs[:2])
            if len(freqs) > 2:
                freq_txt += "…"
            band_txt = "/".join(bands[:2]) + ("…" if len(bands) > 2 else "")
            band_freq = f"{band_txt} {freq_txt}".strip()
            calls_seen = len(group_seen.get(grp, set()))
            msg_ct = int(group_traffic.get(grp, 0))
            if calls_seen == 0 and msg_ct == 0:
                continue
            if search and search not in grp and not any(search in cs for cs in group_seen.get(grp, set())):
                continue
            rows_out.append([grp, band_freq or "-", str(calls_seen), str(msg_ct)])
        self._set_table_rows(self.activity_table, rows_out)

    def _refresh_frequency_control(self) -> None:
        # Avoid clobbering selection while user is interacting
        try:
            if self.freq_combo.view().isVisible():
                return
        except Exception:
            pass
        og_list = load_operating_groups(self.settings)
        current = selected_qsy_meta(self.freq_combo)
        current_freq = None
        try:
            if current:
                current_freq = float(current.get("freq"))
        except Exception:
            current_freq = None
        self.freq_combo.blockSignals(True)
        self.freq_combo.clear()
        self.freq_combo.addItem("Select frequency", None)
        restore_idx = -1
        for g in sorted(
            og_list,
            key=lambda x: (
                str(x.get("group", "")).upper(),
                str(x.get("band", "")).upper(),
                float(x.get("frequency", 0) or 0),
            ),
        ):
            try:
                freq_val = float(g.get("frequency", 0))
            except Exception:
                continue
            label = f"{g.get('group','').strip()} - {g.get('band','').strip()} - {freq_val:.3f} MHz"
            meta = {
                "freq": freq_val,
                "mode": g.get("mode", ""),
                "band": g.get("band", ""),
                "auto_tune": bool(g.get("auto_tune", False)),
                "vfo": (g.get("vfo") or "").strip().upper(),
            }
            self.freq_combo.addItem(label.strip(" -"), meta)
            if current_freq is not None and abs(freq_val - current_freq) < 0.0005:
                restore_idx = self.freq_combo.count() - 1
        if restore_idx >= 0:
            self.freq_combo.setCurrentIndex(restore_idx)
        self.freq_combo.blockSignals(False)
        sched_freq = current_scheduler_freq(self.window())
        sched_group = self._get_scheduled_group_name()
        if sched_freq is not None:
            grp = sched_group or "--"
            self.freq_ctrl_label.setText(f"Scheduled: {grp} {sched_freq:.3f} MHz")
        else:
            self.freq_ctrl_label.setText("Scheduled: --")
        active_freq = self._get_active_frequency_mhz()
        if active_freq is not None:
            grp = sched_group or "--"
            self.freq_active_label.setText(f"Active: {grp} {active_freq:.3f} MHz")
        else:
            self.freq_active_label.setText("Active: --")
        self._update_resume_button_style(sched_freq, active_freq)
        self._update_active_label_style(sched_freq, active_freq)
        self._refresh_intersections()

    def _refresh_intersections(self) -> None:
        now_ts = time.time()
        group_filter = (self.group_combo.currentData() or "").strip().upper()
        search = (self.search_edit.text() or "").strip().upper()
        cache_key = (group_filter, search)
        if (
            cache_key == self._intersection_cache_key
            and now_ts - self._intersection_cache_ts < 30
        ):
            self._set_table_rows(self.intersection_table, self._intersection_cache_rows)
            self._style_intersection_rows()
            return

        rows = self._compute_intersection_summary_rows(group_filter, search)
        self._intersection_cache_ts = now_ts
        self._intersection_cache_key = cache_key
        self._intersection_cache_rows = rows
        self._set_table_rows(self.intersection_table, rows)
        self._style_intersection_rows()

    def _compute_intersection_summary_rows(
        self, group_filter: str, search: str
    ) -> List[List[str]]:
        rows: List[List[str]] = []
        now_utc = dt.datetime.now(dt.timezone.utc)
        now_min = now_utc.hour * 60 + now_utc.minute
        horizon_min = min(now_min + 120, 1440)
        today_name = now_utc.strftime("%A")
        tz = self._get_display_tz()

        my_entries = self._load_my_schedule_entries(today_name)
        if not my_entries:
            return rows
        operator_groups = self._load_operator_group_map()

        db_path = self._db_path()
        if not db_path.exists():
            return rows
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                """
                SELECT owner_callsign, day_utc, start_utc, end_utc, band, frequency
                FROM peer_hf_schedule
                """
            )
            peer_rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load peer schedule: %s", e)
            return rows

        now_calls: Set[str] = set()
        next_calls: Set[str] = set()
        now_labels: Set[str] = set()
        next_labels: Set[str] = set()
        for r in peer_rows:
            day = (r["day_utc"] or "ALL").strip()
            if not self._day_matches_today(day, today_name):
                continue
            cs = (r["owner_callsign"] or "").strip().upper()
            if not cs:
                continue
            groups = operator_groups.get(cs, set())
            if group_filter:
                if group_filter not in groups:
                    continue
            if search and search not in cs and not any(search in g for g in groups):
                continue
            peer_start = self._parse_time_minutes(r["start_utc"])
            peer_end = self._parse_time_minutes(r["end_utc"])
            if peer_start is None or peer_end is None or peer_end <= peer_start:
                continue
            if peer_end <= now_min or peer_start >= horizon_min:
                continue
            try:
                peer_freq = float(str(r["frequency"]).strip())
            except Exception:
                continue

            for entry in my_entries:
                if abs(entry["freq"] - peer_freq) > 0.0001:
                    continue
                start = max(peer_start, entry["start_min"], now_min)
                end = min(peer_end, entry["end_min"], horizon_min)
                if end > start:
                    if start <= now_min < end:
                        now_calls.add(cs)
                        now_labels.add(self._format_group_band_freq_label(entry))
                    else:
                        next_calls.add(cs)
                        next_labels.add(self._format_group_band_freq_label(entry))

        rows.append(["Now", str(len(now_calls)), self._summarize_labels(now_labels)])
        rows.append(["Next 2 hours", str(len(next_calls)), self._summarize_labels(next_labels)])
        return rows

    def _format_group_band_freq_label(self, entry: Dict[str, object]) -> str:
        grp = (entry.get("group") or "--").strip().upper()
        band = (entry.get("band") or "--").strip().upper()
        freq = entry.get("freq")
        try:
            freq_txt = f"{float(freq):.3f} MHz"
        except Exception:
            freq_txt = "--"
        return f"{grp} {band} {freq_txt}"

    def _summarize_labels(self, labels: Set[str]) -> str:
        if not labels:
            return "--"
        ordered = sorted(labels)
        if len(ordered) <= 2:
            return ", ".join(ordered)
        return f"{ordered[0]}, {ordered[1]} +{len(ordered) - 2} more"

    def _style_intersection_rows(self) -> None:
        # Emphasize "Now" and de-emphasize "Next hour"
        if self.intersection_table.rowCount() < 2:
            return
        try:
            now_item = self.intersection_table.item(0, 0)
            if now_item:
                now_item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                font = now_item.font()
                font.setBold(True)
                now_item.setFont(font)
            for col in range(self.intersection_table.columnCount()):
                item = self.intersection_table.item(1, col)
                if item:
                    item.setForeground(Qt.darkGray)
        except Exception:
            pass

    def _format_current_schedule_label(self) -> str:
        sched_freq = current_scheduler_freq(self.window())
        if sched_freq is None:
            return "--"
        sched_group = self._get_scheduled_group_name()
        band = self._band_for_group_freq(sched_group, sched_freq)
        grp = sched_group or "--"
        band_txt = band or "--"
        return f"{grp} {band_txt} {sched_freq:.3f} MHz"

    def _band_for_group_freq(self, group: str, freq: float) -> str:
        if not group:
            return ""
        ops = self.settings.get("operating_groups", []) or []
        for row in ops:
            grp = (row.get("group") or "").strip().upper()
            if grp != group:
                continue
            try:
                f = float(row.get("frequency", 0))
            except Exception:
                continue
            if abs(f - freq) < 0.0005:
                return (row.get("band") or "").strip().upper()
        return ""

    def _load_my_schedule_entries(self, today_name: str) -> List[Dict[str, object]]:
        entries: List[Dict[str, object]] = []

        def add_entry(day: str, start: str, end: str, band: str, freq_val, group: str) -> None:
            if not self._day_matches_today(day, today_name):
                return
            start_min = self._parse_time_minutes(start)
            end_min = self._parse_time_minutes(end)
            if start_min is None or end_min is None or end_min <= start_min:
                return
            try:
                freq_num = float(str(freq_val).strip())
            except Exception:
                return
            entries.append(
                {
                    "start_min": start_min,
                    "end_min": end_min,
                    "freq": freq_num,
                    "band": (band or "").strip().upper(),
                    "group": (group or "").strip().upper(),
                }
            )

        db_path = self._settings_db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT day_utc, start_utc, end_utc, band, frequency, group_name FROM daily_schedule_tab")
                rows = cur.fetchall()
                conn.close()
                for r in rows:
                    add_entry(
                        r["day_utc"],
                        r["start_utc"],
                        r["end_utc"],
                        r["band"],
                        r["frequency"],
                        r["group_name"],
                    )
            except Exception as e:
                log.debug("ControlFreq: failed to load daily schedule for overlaps: %s", e)

        db_path = self._db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(
                    "SELECT day_utc, start_utc, end_utc, band, frequency, group_name, net_name FROM net_schedule_tab"
                )
                rows = cur.fetchall()
                conn.close()
                for r in rows:
                    group = r["group_name"] or r["net_name"]
                    add_entry(
                        r["day_utc"],
                        r["start_utc"],
                        r["end_utc"],
                        r["band"],
                        r["frequency"],
                        group,
                    )
            except Exception as e:
                log.debug("ControlFreq: failed to load net schedule for overlaps: %s", e)

        return entries

    @staticmethod
    def _day_matches_today(day: str, today_name: str) -> bool:
        day = (day or "ALL").strip().upper()
        if day in ("ALL", "DAILY"):
            return True
        if not day:
            return False
        today = today_name.upper()
        return today.startswith(day[:3]) or day.startswith(today[:3])

    @staticmethod
    def _parse_time_minutes(value: str) -> Optional[int]:
        txt = (value or "").strip()
        if not txt:
            return None
        parts = txt.split(":")
        try:
            if len(parts) == 1 and txt.isdigit() and len(txt) in (3, 4):
                hour = int(txt[:-2])
                minute = int(txt[-2:])
            elif len(parts) == 2:
                hour = int(parts[0])
                minute = int(parts[1])
            else:
                return None
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                return None
            return hour * 60 + minute
        except Exception:
            return None

    def _format_overlap_time(self, start_min: int, end_min: int, tz: dt.tzinfo) -> str:
        base = dt.datetime.now(dt.timezone.utc).date()
        start_dt = dt.datetime(
            base.year,
            base.month,
            base.day,
            start_min // 60,
            start_min % 60,
            tzinfo=dt.timezone.utc,
        )
        end_dt = dt.datetime(
            base.year,
            base.month,
            base.day,
            end_min // 60,
            end_min % 60,
            tzinfo=dt.timezone.utc,
        )
        if self._show_local:
            start_dt = start_dt.astimezone(tz)
            end_dt = end_dt.astimezone(tz)
        return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"

    def _get_scheduled_group_name(self) -> str:
        try:
            sched = getattr(self.window(), "scheduler", None)
            entry = getattr(sched, "current_schedule_entry", {}) if sched else {}
            return (entry.get("group_name") or entry.get("group") or "").strip().upper()
        except Exception:
            return ""

    def _get_active_frequency_mhz(self) -> Optional[float]:
        try:
            sched = getattr(self.window(), "scheduler", None)
            if not sched or not hasattr(sched, "get_status_summary"):
                return None
            status = sched.get_status_summary()
            freq_label = status.get("freq_label") or ""
            return self._parse_freq_label(freq_label)
        except Exception:
            return None

    @staticmethod
    def _parse_freq_label(label: str) -> Optional[float]:
        try:
            parts = str(label).replace("MHz", "").strip().split()
            for token in parts:
                try:
                    return float(token)
                except Exception:
                    continue
        except Exception:
            return None
        return None

    def _update_resume_button_style(
        self, scheduled: Optional[float], active: Optional[float]
    ) -> None:
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        mismatch = False
        if scheduled is not None and active is not None:
            mismatch = abs(scheduled - active) > 0.0005
        if theme:
            style = "warning" if mismatch else "secondary"
            self.freq_resume_btn.setStyleSheet(button_style(style, theme))

    def _update_active_label_style(
        self, scheduled: Optional[float], active: Optional[float]
    ) -> None:
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        mismatch = False
        if scheduled is not None and active is not None:
            mismatch = abs(scheduled - active) > 0.0005
        if theme:
            color = theme["info"] if mismatch else theme["text"]
            self.freq_active_label.setStyleSheet(
                f"font-size: 14px; font-weight: bold; color: {color};"
            )

    def _on_freq_set_clicked(self) -> None:
        control_via = (self.settings.get("control_via", "") or "").strip()
        if control_via not in {"FLRig", "JS8Call"}:
            QMessageBox.information(
                self,
                "Frequency Control",
                "Frequency control is available when Control Via is FLRig or JS8Call.",
            )
            return
        meta = selected_qsy_meta(self.freq_combo)
        if not meta:
            QMessageBox.warning(self, "Frequency Control", "Select a frequency first.")
            return
        ok = perform_qsy(self.window(), meta)
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        if theme:
            self.freq_set_btn.setStyleSheet(
                button_style("success" if ok else "warning", theme)
            )
        self._refresh_frequency_control()
        if ok:
            QTimer.singleShot(800, self._refresh_frequency_control)

    def _on_freq_selection_changed(self) -> None:
        meta = selected_qsy_meta(self.freq_combo)
        try:
            theme = resolve_theme(self.settings)
        except Exception:
            theme = None
        if theme:
            if meta:
                self.freq_set_btn.setStyleSheet(button_style("info", theme))
            else:
                self.freq_set_btn.setStyleSheet(button_style("secondary", theme))

    def _on_resume_schedule_clicked(self) -> None:
        try:
            sched = getattr(self.window(), "scheduler", None)
            if sched and hasattr(sched, "resume_schedule"):
                sched.resume_schedule()
                return
            if sched:
                sched.apply_current_entry(force=True, ignore_wait_prompt=True, ignore_suspend=True)
        except Exception:
            pass

    def _refresh_today(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        rows = self._collect_schedule_rows(now, end)
        self._set_table_rows(self.today_table, rows)

    def _refresh_week(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        end = now + dt.timedelta(days=7)
        rows = self._collect_schedule_rows(now, end, include_day=True, include_hf=False)
        self._set_table_rows(self.week_table, rows)

    def _collect_schedule_rows(
        self,
        start: dt.datetime,
        end: dt.datetime,
        include_day: bool = False,
        include_hf: bool = True,
    ) -> List[List[str]]:
        rows_out: List[List[str]] = []
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        tz = self._get_display_tz()

        # SOP upcoming actions
        try:
            mgr = SOPManager()
            horizon_hours = max(1, int((end - start).total_seconds() // 3600))
            actions = mgr.build_upcoming_actions(
                horizon_hours=horizon_hours, only_active=True, now_utc=start
            )
            for a in actions:
                due = a.get("next_due_utc") or a.get("due_utc")
                if not isinstance(due, dt.datetime):
                    continue
                if not (start <= due <= end):
                    continue
                grp = (a.get("operating_group") or "").strip().upper()
                if group_filter and grp != group_filter:
                    continue
                label = a.get("action_label") or a.get("action") or "SOP"
                band = (a.get("band") or "").strip().upper()
                freq = (a.get("frequency") or "").strip()
                when = self._format_display_time(due, include_day, tz)
                if search and search not in grp and search not in str(label).upper() and search not in "SOP":
                    continue
                rows_out.append([when, f"{grp} {label}".strip(), f"{band} {freq}".strip(), "SOP"])
        except Exception as e:
            log.debug("ControlFreq: SOP load failed: %s", e)

        # HF + Net schedule (simple view)
        if include_hf:
            rows_out.extend(self._load_hf_schedule(start, end, include_day, tz))
        rows_out.extend(self._load_net_schedule(start, end, include_day, tz))
        return rows_out[:200]

    def _load_hf_schedule(
        self, start: dt.datetime, end: dt.datetime, include_day: bool, tz: dt.tzinfo
    ) -> List[List[str]]:
        rows_out: List[List[str]] = []
        db_path = self._settings_db_path()
        if not db_path.exists():
            return rows_out
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM daily_schedule_tab")
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load HF schedule: %s", e)
            return rows_out
        today_name = start.strftime("%A")
        for r in rows:
            row = dict(r)
            day = (row.get("day") or row.get("day_utc") or row.get("day_name") or "").strip()
            if day and day not in {today_name, "ALL"} and not include_day:
                continue
            grp = (row.get("group_name") or row.get("group") or "").strip().upper()
            if group_filter and grp != group_filter:
                continue
            start_hm = (row.get("start") or row.get("start_utc") or "").strip()
            band = (row.get("band") or "").strip().upper()
            freq = (row.get("frequency") or row.get("freq") or "").strip()
            when = self._format_hhmm_display(start, start_hm, include_day, tz)
            if search and search not in grp and search not in (band + freq).upper() and search not in "HF":
                continue
            rows_out.append([when, grp, f"{band} {freq}".strip(), "HF"])
        return rows_out

    def _scheduled_group_freqs(
        self, window_minutes: int
    ) -> Tuple[Dict[str, Set[float]], Dict[str, Set[str]]]:
        sched_freqs: Dict[str, Set[float]] = {}
        sched_bands: Dict[str, Set[str]] = {}
        db_path = self._settings_db_path()
        if not db_path.exists():
            return sched_freqs, sched_bands
        now_utc = dt.datetime.now(dt.timezone.utc)
        today_utc_name = now_utc.strftime("%A").upper()

        def parse_hhmm(value: str) -> Optional[Tuple[int, int]]:
            txt = (value or "").strip()
            if not txt:
                return None
            parts = txt.split(":")
            if len(parts) == 1 and txt.isdigit() and len(txt) in (3, 4):
                h = int(txt[:-2])
                m = int(txt[-2:])
            elif len(parts) == 2:
                h = int(parts[0])
                m = int(parts[1])
            else:
                return None
            if h < 0 or h > 23 or m < 0 or m > 59:
                return None
            return h, m

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM daily_schedule_tab")
            rows = cur.fetchall()
            conn.close()
        except Exception:
            rows = []
        for r in rows:
            row = dict(r)
            day = (row.get("day_utc") or row.get("day") or "ALL").strip().upper()
            if day not in {"ALL", today_utc_name}:
                continue
            start_hm = row.get("start_utc") or row.get("start") or ""
            hm = parse_hhmm(str(start_hm))
            if not hm:
                continue
            start_utc = now_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
            delta_min = abs((start_utc - now_utc).total_seconds()) / 60.0
            if delta_min > float(window_minutes):
                continue
            grp = (row.get("group_name") or row.get("group") or "").strip().upper()
            if not grp:
                continue
            band = (row.get("band") or "").strip().upper()
            freq_val = row.get("frequency") or row.get("freq")
            if band:
                sched_bands.setdefault(grp, set()).add(band)
            try:
                freq = float(freq_val)
                sched_freqs.setdefault(grp, set()).add(freq)
            except Exception:
                continue
        return sched_freqs, sched_bands

    def _load_net_schedule(
        self, start: dt.datetime, end: dt.datetime, include_day: bool, tz: dt.tzinfo
    ) -> List[List[str]]:
        rows_out: List[List[str]] = []
        db_path = self._db_path()
        if not db_path.exists():
            return rows_out
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM net_schedule_tab")
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: failed to load Net schedule: %s", e)
            return rows_out
        today_name = start.strftime("%A")
        for r in rows:
            row = dict(r)
            day = (row.get("day") or row.get("day_utc") or "").strip()
            if day and day not in {today_name, "ALL"} and not include_day:
                continue
            grp = (row.get("group_name") or row.get("group") or "").strip().upper()
            if group_filter and grp != group_filter:
                continue
            start_hm = (row.get("start") or row.get("start_utc") or "").strip()
            band = (row.get("band") or "").strip().upper()
            freq = (row.get("frequency") or row.get("freq") or "").strip()
            net_name = (row.get("net_name") or "").strip()
            when = self._format_hhmm_display(start, start_hm, include_day, tz)
            if search and search not in grp and search not in net_name.upper() and search not in "NET":
                continue
            rows_out.append([when, net_name or grp, f"{band} {freq}".strip(), "NET"])
        return rows_out

    def _get_display_tz(self) -> dt.tzinfo:
        if not self._show_local:
            return dt.timezone.utc
        try:
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            return get_timezone(tz_name)
        except Exception:
            return dt.timezone.utc

    def _format_display_time(
        self, utc_dt: dt.datetime, include_day: bool, tz: dt.tzinfo
    ) -> str:
        if not isinstance(utc_dt, dt.datetime):
            return "--:--"
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=dt.timezone.utc)
        local_dt = utc_dt.astimezone(tz) if self._show_local else utc_dt.astimezone(dt.timezone.utc)
        return local_dt.strftime("%a %H:%M") if include_day else local_dt.strftime("%H:%M")

    def _parse_hhmm(self, value: str) -> Optional[Tuple[int, int]]:
        txt = (value or "").strip()
        if not txt:
            return None
        parts = txt.split(":")
        if len(parts) == 1 and txt.isdigit() and len(txt) in (3, 4):
            h = int(txt[:-2])
            m = int(txt[-2:])
        elif len(parts) == 2:
            h = int(parts[0])
            m = int(parts[1])
        else:
            return None
        if h < 0 or h > 23 or m < 0 or m > 59:
            return None
        return h, m

    def _format_hhmm_display(
        self, base_utc: dt.datetime, hhmm: str, include_day: bool, tz: dt.tzinfo
    ) -> str:
        hm = self._parse_hhmm(hhmm)
        if not hm:
            return "--:--"
        dt_utc = base_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=dt.timezone.utc)
        return self._format_display_time(dt_utc, include_day, tz)

    def _refresh_inbox(self) -> None:
        db_path = self._db_path()
        if not db_path.exists():
            self._set_table_rows(self.inbox_table, [])
            return
        search = (self.search_edit.text() or "").strip().upper()
        counts = {"JS8": 0, "Spotter": 0, "VarAC": 0}
        top_senders: Dict[str, Dict[str, int]] = {"JS8": {}, "Spotter": {}, "VarAC": {}}
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT from_call, state FROM js8_messages")
            for cs, state in cur.fetchall():
                if (state or "").upper() == "READ":
                    continue
                cs = (cs or "").strip().upper()
                if search and search not in cs and search not in "JS8":
                    continue
                counts["JS8"] += 1
                top_senders["JS8"][cs] = top_senders["JS8"].get(cs, 0) + 1
            cur.execute("SELECT from_call, state FROM spotter_traffic")
            for cs, state in cur.fetchall():
                if (state or "").upper() == "READ":
                    continue
                cs = (cs or "").strip().upper()
                if search and search not in cs and search not in "SPOTTER":
                    continue
                counts["Spotter"] += 1
                top_senders["Spotter"][cs] = top_senders["Spotter"].get(cs, 0) + 1
            cur.execute("SELECT from_call, read_status FROM varac_messages")
            for cs, read_status in cur.fetchall():
                if int(read_status or 0) != 0:
                    continue
                cs = (cs or "").strip().upper()
                if search and search not in cs and search not in "VARAC":
                    continue
                counts["VarAC"] += 1
                top_senders["VarAC"][cs] = top_senders["VarAC"].get(cs, 0) + 1
            conn.close()
        except Exception as e:
            log.debug("ControlFreq: inbox summary load failed: %s", e)

        rows_out: List[List[str]] = []
        for key in ("JS8", "Spotter", "VarAC"):
            if search and search not in key.upper() and counts[key] == 0:
                continue
            senders = sorted(top_senders[key].items(), key=lambda kv: kv[1], reverse=True)[:3]
            sender_txt = ", ".join([f"{c}({n})" for c, n in senders]) or "-"
            rows_out.append([key, str(counts[key]), sender_txt])
        self._set_table_rows(self.inbox_table, rows_out)
        self._apply_elide_tooltips(self.inbox_table, 2)

    def _refresh_propagation_snapshot(self) -> None:
        try:
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            tz = get_timezone(tz_name)
        except Exception:
            tz = dt.timezone.utc
        now_utc = dt.datetime.now(dt.timezone.utc)
        now_local = now_utc.astimezone(tz)

        user_grid = (
            self.settings.get("operator_grid6", "")
            or self.settings.get("operator_grid", "")
            or ""
        ).strip().upper()
        user_ll = maidenhead_to_latlon(user_grid) if user_grid else None
        if not user_ll:
            self._set_table_rows(self.prop_table, [])
            self.prop_hint.setText(
                "Set your Grid 6 in Settings to enable propagation snapshots."
            )
            return

        dawn_local, sunset_local = self._sunrise_sunset_local(
            now_local.date(), user_ll[0], user_ll[1], tz
        )
        if dawn_local is None or sunset_local is None:
            # Fallback for polar regions
            dawn_local = now_local.replace(hour=6, minute=0, second=0, microsecond=0)
            sunset_local = now_local.replace(hour=18, minute=0, second=0, microsecond=0)
        day_start_local = now_local.replace(hour=10, minute=0, second=0, microsecond=0)
        if dawn_local.date() != now_local.date():
            dawn_local = dawn_local.replace(
                year=now_local.year, month=now_local.month, day=now_local.day
            )
        if sunset_local.date() != now_local.date():
            sunset_local = sunset_local.replace(
                year=now_local.year, month=now_local.month, day=now_local.day
            )
        night_start = sunset_local
        night_end = dawn_local + dt.timedelta(days=1) if dawn_local <= sunset_local else dawn_local

        schedule_entries = self._load_today_schedule_local(now_local, tz)
        band_rows: Dict[str, List[Tuple[dt.datetime, str]]] = {}
        for when_local, band, label in schedule_entries:
            if not band:
                continue
            band_rows.setdefault(band, []).append((when_local, label))

        # Build window bands from schedule
        window_bands = {"morning": set(), "day": set(), "night": set()}
        for band, entries in band_rows.items():
            entries.sort(key=lambda e: e[0])
            for when_local, _label in entries:
                if dawn_local <= when_local < day_start_local:
                    window_bands["morning"].add(band)
                if day_start_local <= when_local < sunset_local:
                    window_bands["day"].add(band)
                if when_local >= night_start or when_local < dawn_local:
                    window_bands["night"].add(band)

        all_bands = sorted(band_rows.keys(), key=lambda b: (b not in PROP_BANDS, b))
        if not all_bands:
            self._set_table_rows(self.prop_table, [])
            self.prop_hint.setText(
                "No scheduled bands found for today. Add a schedule to see modeled bands."
            )
            return

        # Determine region for snapshot (from operator state)
        operator_state = (self.settings.get("operator_state", "") or "").strip().upper()
        region_id = STATE_TO_FEMA_REGION.get(operator_state, "")
        region_label = f"Region {region_id}" if region_id else "Region --"

        # Compute modeled top-2 for each window
        morning_mid = dawn_local + (day_start_local - dawn_local) / 2
        day_mid = day_start_local + (sunset_local - day_start_local) / 2
        night_mid = night_start + (night_end - night_start) / 2
        window_mid = {"morning": morning_mid, "day": day_mid, "night": night_mid}

        nat_scores = {}
        reg_scores = {}
        for window, mid_local in window_mid.items():
            bands = sorted(window_bands.get(window) or all_bands)
            nat_scores[window] = self._top_bands_modeled(
                bands, mid_local, user_ll, points=list(STATE_CENTERS.values())
            )
            if region_id and region_id in FEMA_REGIONS:
                region_points = [STATE_CENTERS[s] for s in FEMA_REGIONS[region_id] if s in STATE_CENTERS]
                reg_scores[window] = self._top_bands_modeled(
                    bands, mid_local, user_ll, points=region_points
                )
            else:
                reg_scores[window] = []

        schedule_rows: List[List[str]] = []
        schedule_rows.append(
            [
                "National",
                self._format_band_list(nat_scores["morning"]) or "--",
                self._format_band_list(nat_scores["day"]) or "--",
                self._format_band_list(nat_scores["night"]) or "--",
            ]
        )
        schedule_rows.append(
            [
                "Regional",
                self._format_band_list(reg_scores["morning"]) or "--",
                self._format_band_list(reg_scores["day"]) or "--",
                self._format_band_list(reg_scores["night"]) or "--",
            ]
        )

        modeled_nat: Dict[str, List[Tuple[str, float]]] = {}
        modeled_reg: Dict[str, List[Tuple[str, float]]] = {}
        for window, mid_local in window_mid.items():
            modeled_nat[window] = self._top_bands_modeled(
                PROP_BANDS, mid_local, user_ll, points=list(STATE_CENTERS.values())
            )
            if region_id and region_id in FEMA_REGIONS:
                region_points = [STATE_CENTERS[s] for s in FEMA_REGIONS[region_id] if s in STATE_CENTERS]
                modeled_reg[window] = self._top_bands_modeled(
                    PROP_BANDS, mid_local, user_ll, points=region_points
                )
            else:
                modeled_reg[window] = []

        modeled_rows: List[List[str]] = []
        modeled_rows.append(
            [
                "Best",
                self._format_best_band(modeled_nat["morning"], modeled_reg["morning"]) or "--",
                self._format_best_band(modeled_nat["day"], modeled_reg["day"]) or "--",
                self._format_best_band(modeled_nat["night"], modeled_reg["night"]) or "--",
            ]
        )
        self._set_sectioned_prop_rows("Schedule-based Forecast", schedule_rows, "Modeled Forecast", modeled_rows)
        self.prop_hint.setText(
            f"Modeled snapshot for {now_local.strftime('%Y-%m-%d')} "
            f"({tz.tzname(now_local)}). Origin Grid: {user_grid}."
        )

    def _load_today_schedule_local(
        self, now_local: dt.datetime, tz: dt.tzinfo
    ) -> List[Tuple[dt.datetime, str, str]]:
        entries: List[Tuple[dt.datetime, str, str]] = []
        today_local = now_local.date()
        now_utc = now_local.astimezone(dt.timezone.utc)
        today_utc_name = now_utc.strftime("%A")
        group_filter = self.group_combo.currentData() or ""
        search = (self.search_edit.text() or "").strip().upper()

        def parse_hhmm(value: str) -> Optional[Tuple[int, int]]:
            txt = (value or "").strip()
            if not txt:
                return None
            parts = txt.split(":")
            if len(parts) == 1 and txt.isdigit() and len(txt) in (3, 4):
                h = int(txt[:-2])
                m = int(txt[-2:])
            elif len(parts) == 2:
                h = int(parts[0])
                m = int(parts[1])
            else:
                return None
            if h < 0 or h > 23 or m < 0 or m > 59:
                return None
            return h, m

        # HF schedule from settings DB
        db_path = self._settings_db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT * FROM daily_schedule_tab")
                rows = cur.fetchall()
                conn.close()
            except Exception:
                rows = []
            for r in rows:
                row = dict(r)
                day = (row.get("day_utc") or row.get("day") or "ALL").strip().upper()
                if day not in {"ALL", today_utc_name.upper()}:
                    continue
                start_hm = row.get("start_utc") or row.get("start") or ""
                hm = parse_hhmm(str(start_hm))
                if not hm:
                    continue
                band = (row.get("band") or "").strip().upper()
                label = (row.get("group_name") or row.get("group") or "").strip().upper()
                if group_filter and label != group_filter:
                    continue
                if search and search not in label and search not in band:
                    continue
                start_utc = now_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
                start_local = start_utc.astimezone(tz)
                if start_local.date() == today_local:
                    entries.append((start_local, band, label))

        # Net schedule from nets DB
        db_path = self._db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT * FROM net_schedule_tab")
                rows = cur.fetchall()
                conn.close()
            except Exception:
                rows = []
            for r in rows:
                row = dict(r)
                day = (row.get("day_utc") or row.get("day") or "ALL").strip().upper()
                if day not in {"ALL", today_utc_name.upper()}:
                    continue
                start_hm = row.get("start_utc") or row.get("start") or ""
                hm = parse_hhmm(str(start_hm))
                if not hm:
                    continue
                band = (row.get("band") or "").strip().upper()
                label = (row.get("net_name") or row.get("group_name") or "").strip().upper()
                if group_filter and label != group_filter:
                    continue
                if search and search not in label and search not in band:
                    continue
                start_utc = now_utc.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
                start_local = start_utc.astimezone(tz)
                if start_local.date() == today_local:
                    entries.append((start_local, band, label))
        return entries

    def _top_bands_modeled(
        self,
        bands: List[str],
        mid_local: dt.datetime,
        user_ll: Tuple[float, float],
        points: List[Tuple[float, float]],
    ) -> List[Tuple[str, float]]:
        if not points:
            return []
        mid_utc = mid_local.astimezone(dt.timezone.utc)
        scores: List[Tuple[str, float]] = []
        for band in bands:
            vals: List[float] = []
            for lat, lon in points:
                dist = self._haversine_km(user_ll[0], user_ll[1], lat, lon)
                vals.append(self._modeled_band_score(band, user_ll, lat, lon, mid_utc, dist))
            if not vals:
                continue
            scores.append((band, sum(vals) / max(1, len(vals))))
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:2]

    def _format_band_list(self, bands: List[Tuple[str, float]]) -> str:
        if not bands:
            return ""
        out = []
        for band, score in bands:
            qual = self._score_to_qual(score)
            out.append(f"{band} ({qual})")
        return "/".join(out)

    @staticmethod
    def _score_to_qual(score: float) -> str:
        if score >= 70:
            return "HIGH"
        if score >= 40:
            return "MED"
        return "LOW"

    def _format_best_band(
        self, nat: List[Tuple[str, float]], reg: List[Tuple[str, float]]
    ) -> str:
        if reg:
            band, score = reg[0]
            return f"{band} ({self._score_to_qual(score)})"
        if nat:
            band, score = nat[0]
            return f"{band} ({self._score_to_qual(score)})"
        return ""

    def _set_sectioned_prop_rows(
        self,
        label_a: str,
        rows_a: List[List[str]],
        label_b: str,
        rows_b: List[List[str]],
    ) -> None:
        self.prop_table.setRowCount(0)
        self._append_section_row(label_a)
        self._append_rows(rows_a)
        self._append_section_row(label_b)
        self._append_rows(rows_b)
        self.prop_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

    def _append_section_row(self, text: str) -> None:
        row = self.prop_table.rowCount()
        self.prop_table.insertRow(row)
        item = QTableWidgetItem(text)
        item.setFlags(item.flags() ^ Qt.ItemIsEditable)
        item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        item.setBackground(Qt.lightGray)
        item.setForeground(Qt.black)
        self.prop_table.setItem(row, 0, item)
        self.prop_table.setSpan(row, 0, 1, self.prop_table.columnCount())

    def _append_rows(self, rows: List[List[str]]) -> None:
        for row in rows:
            r = self.prop_table.rowCount()
            self.prop_table.insertRow(r)
            for c, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                self.prop_table.setItem(r, c, item)

    def _sunrise_sunset_local(
        self, date_val: dt.date, lat: float, lon: float, tz: dt.tzinfo
    ) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
        def _calc(is_sunrise: bool) -> Optional[dt.datetime]:
            n = date_val.timetuple().tm_yday
            lng_hour = lon / 15.0
            t = n + ((6 - lng_hour) / 24.0) if is_sunrise else n + ((18 - lng_hour) / 24.0)
            m = (0.9856 * t) - 3.289
            l = m + (1.916 * math.sin(math.radians(m))) + (0.020 * math.sin(math.radians(2 * m))) + 282.634
            l = (l + 360.0) % 360.0
            ra = math.degrees(math.atan(0.91764 * math.tan(math.radians(l))))
            ra = (ra + 360.0) % 360.0
            l_quadrant = (math.floor(l / 90.0)) * 90.0
            ra_quadrant = (math.floor(ra / 90.0)) * 90.0
            ra = ra + (l_quadrant - ra_quadrant)
            ra /= 15.0
            sin_dec = 0.39782 * math.sin(math.radians(l))
            cos_dec = math.cos(math.asin(sin_dec))
            cos_h = (math.cos(math.radians(90.833)) - (sin_dec * math.sin(math.radians(lat)))) / (
                cos_dec * math.cos(math.radians(lat))
            )
            if cos_h > 1 or cos_h < -1:
                return None
            if is_sunrise:
                h = 360.0 - math.degrees(math.acos(cos_h))
            else:
                h = math.degrees(math.acos(cos_h))
            h /= 15.0
            t_local = h + ra - (0.06571 * t) - 6.622
            ut = (t_local - lng_hour) % 24.0
            dt_utc = dt.datetime(
                date_val.year, date_val.month, date_val.day, tzinfo=dt.timezone.utc
            ) + dt.timedelta(hours=ut)
            return dt_utc.astimezone(tz)

        sunrise = _calc(True)
        sunset = _calc(False)
        return sunrise, sunset

    def _modeled_band_score(
        self,
        band: str,
        user_ll: Tuple[float, float],
        dest_lat: float,
        dest_lon: float,
        now_utc: dt.datetime,
        distance_km: float,
    ) -> float:
        mid_lat = (user_ll[0] + dest_lat) / 2.0
        mid_lon = (user_ll[1] + dest_lon) / 2.0
        hour_local = self._local_hour_from_lon(now_utc, mid_lon)
        base = self._band_score_db(band, mid_lat, mid_lon, now_utc.month)
        if base is None:
            base = self._band_score(band, distance_km, now_utc.hour)
        diurnal = self._diurnal_weight(band, hour_local)
        path_weight = self._path_band_weight(band, distance_km, hour_local)
        score = float(base) * float(diurnal) * float(path_weight)
        return max(0.0, min(100.0, score))

    def _load_prop_db_cache(self) -> None:
        if self._prop_db_loaded:
            return
        self._prop_db_loaded = True
        try:
            db_path = get_config_dir() / "config" / "propagation" / "prop_climatology.db"
        except Exception:
            self._prop_db_available = False
            return
        if not db_path.exists():
            self._prop_db_available = False
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT month, band, lat_idx, lon_idx, muf_score FROM muf_grid")
            for month, band, lat_idx, lon_idx, score in cur.fetchall():
                key = (str(band).upper(), int(month), int(lat_idx), int(lon_idx))
                self._prop_db_cache[key] = float(score)
            conn.close()
            self._prop_db_available = bool(self._prop_db_cache)
        except Exception:
            self._prop_db_available = False

    def _lookup_db_score(self, band: str, lat: float, lon: float, month: int) -> Optional[float]:
        self._load_prop_db_cache()
        if not self._prop_db_available:
            return None
        try:
            lat_idx = int(round((lat + 90.0) * 2))
            lon_idx = int(round((lon + 180.0) * 2))
        except Exception:
            return None
        key = (band.upper(), int(month), int(lat_idx), int(lon_idx))
        return self._prop_db_cache.get(key)

    def _band_score_db(self, band: str, lat: float, lon: float, month: int) -> Optional[float]:
        score = self._lookup_db_score(band, lat, lon, month)
        if score is None:
            return None
        if score <= 1.0:
            return max(0.0, min(100.0, score * 100.0))
        return max(0.0, min(100.0, score))

    def _band_score(self, band: str, distance_km: float, hour_utc: int) -> float:
        profiles = dict(PROP_DEFAULT_PROFILES)
        prof = profiles.get(band, {})
        ideal = float(prof.get("ideal_km", 2000))
        spread = float(prof.get("spread_km", 2000))
        day_factor = float(prof.get("day", 0.8))
        night_factor = float(prof.get("night", 0.8))
        is_day = 6 <= hour_utc < 18
        factor = day_factor if is_day else night_factor
        if spread <= 0:
            spread = 1.0
        dist_pen = max(0.0, 1.0 - abs(distance_km - ideal) / spread)
        score = 100.0 * factor * dist_pen
        return max(0.0, min(100.0, score))

    def _diurnal_weight(self, band: str, hour_local: int) -> float:
        prof = dict(PROP_DEFAULT_PROFILES).get(band, {})
        day_factor = float(prof.get("day", 0.8))
        night_factor = float(prof.get("night", 0.8))
        is_day = 6 <= hour_local < 18
        return day_factor if is_day else night_factor

    def _local_hour_from_lon(self, utc_dt: dt.datetime, lon: float) -> int:
        try:
            offset = lon / 15.0
        except Exception:
            offset = 0.0
        hour = (utc_dt.hour + offset) % 24
        return int(hour)

    def _path_band_weight(self, band: str, distance_km: float, hour_local: int) -> float:
        band = (band or "").upper()
        is_day = 6 <= hour_local < 18
        if distance_km < 300:
            weights = {"80M": 1.0, "40M": 1.2, "30M": 0.8, "20M": 0.4, "15M": 0.2, "10M": 0.1} if is_day else {
                "80M": 1.3, "40M": 1.1, "30M": 0.6, "20M": 0.3, "15M": 0.15, "10M": 0.1
            }
        elif distance_km < 900:
            weights = {"80M": 0.6, "40M": 1.0, "30M": 1.0, "20M": 0.8, "15M": 0.5, "10M": 0.3} if is_day else {
                "80M": 0.9, "40M": 1.1, "30M": 0.9, "20M": 0.5, "15M": 0.2, "10M": 0.1
            }
        else:
            weights = {"80M": 0.2, "40M": 0.6, "30M": 0.9, "20M": 1.2, "15M": 1.0, "10M": 0.7} if is_day else {
                "80M": 0.4, "40M": 1.2, "30M": 1.0, "20M": 0.7, "15M": 0.3, "10M": 0.2
            }
        return float(weights.get(band, 0.5))

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r = 6371.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return r * c

    @staticmethod
    def _set_table_rows(table: QTableWidget, rows: List[List[str]]) -> None:
        table.setRowCount(0)
        for r, row in enumerate(rows):
            table.insertRow(r)
            for c, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                table.setItem(r, c, item)

    @staticmethod
    def _apply_elide_tooltips(table: QTableWidget, col: int) -> None:
        if col < 0:
            return
        width = table.columnWidth(col) - 10
        if width <= 0:
            return
        for r in range(table.rowCount()):
            item = table.item(r, col)
            if item is None:
                continue
            text = item.text()
            fm = QFontMetrics(item.font())
            elided = fm.elidedText(text, Qt.ElideRight, width)
            if elided != text:
                item.setText(elided)
                item.setToolTip(text)
            else:
                item.setToolTip("")
