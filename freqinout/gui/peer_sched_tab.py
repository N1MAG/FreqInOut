from __future__ import annotations

import datetime
import json
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QComboBox,
    QTableWidget,
    QTableWidgetItem,
    QFileDialog,
    QMessageBox,
    QLineEdit,
)

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.utils.timezones import get_timezone
from freqinout.gui.theme import resolve_theme, button_style


# Simple FEMA region mapping for filtering
FEMA_REGIONS = {
    "R01": ["CT", "ME", "MA", "NH", "RI", "VT"],
    "R02": ["NJ", "NY", "PR", "VI"],
    "R03": ["DC", "DE", "MD", "PA", "VA", "WV"],
    "R04": ["AL", "FL", "GA", "KY", "MS", "NC", "SC", "TN"],
    "R05": ["IL", "IN", "MI", "MN", "OH", "WI"],
    "R06": ["AR", "LA", "NM", "OK", "TX"],
    "R07": ["IA", "KS", "MO", "NE"],
    "R08": ["CO", "MT", "ND", "SD", "UT", "WY"],
    "R09": ["AZ", "CA", "HI", "NV", "GU", "AS", "MP"],
    "R10": ["AK", "ID", "OR", "WA"],
}
STATE_TO_REGION = {st: region for region, states in FEMA_REGIONS.items() for st in states}


class PeerSchedTab(QWidget):
    """
    View and manage imported peer HF schedules (non-net).
    """

    COLS: Sequence[str] = (
        "CALLSIGN",
        "NAME",
        "STATE",
        "GROUPS",
        "DAY (UTC)",
        "START UTC",
        "END UTC",
        "BAND",
        "MODE",
        "FREQ",
        "OVERLAP",
    )
    DAY_CANON = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._rows: List[Dict] = []
        self._operator_meta: Dict[str, Dict[str, str]] = {}
        self._my_schedule: List[Dict] = []
        self._my_schedule_by_mode: Dict[str, List[Dict]] = {}
        default_mode = (self.settings.get("display_time_mode", "LOCAL") or "LOCAL").upper()
        self._show_local_times = default_mode != "UTC"
        self._build_ui()
        self._load_operator_meta()
        self._load_data()

    # ---------- UI ----------

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Peer HF Schedules</h3>"))
        header.addStretch()
        self.import_btn = QPushButton("Import Schedule")
        self.refresh_btn = QPushButton("Refresh")
        self.delete_callsign_combo = QComboBox()
        self.delete_callsign_combo.addItem("Select callsign", None)
        self.delete_btn = QPushButton("Delete Schedule")
        self.tz_toggle_btn = QPushButton("Showing: UTC")
        header.addWidget(self.import_btn)
        header.addWidget(self.refresh_btn)
        header.addWidget(QLabel("Delete:"))
        header.addWidget(self.delete_callsign_combo)
        header.addWidget(self.delete_btn)
        header.addWidget(self.tz_toggle_btn)
        layout.addLayout(header)

        # Filters
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Callsign:"))
        self.callsign_filter = QComboBox()
        self.callsign_filter.addItem("All")
        self.callsign_filter.setMinimumWidth(150)
        filter_row.addWidget(self.callsign_filter)

        filter_row.addWidget(QLabel("Region:"))
        self.region_filter = QComboBox()
        self.region_filter.addItem("All")
        for r in sorted(FEMA_REGIONS.keys()):
            self.region_filter.addItem(r)
        self.region_filter.setMinimumWidth(120)
        filter_row.addWidget(self.region_filter)

        filter_row.addWidget(QLabel("Group:"))
        self.group_filter = QComboBox()
        self.group_filter.addItem("All")
        self.group_filter.setMinimumWidth(150)
        filter_row.addWidget(self.group_filter)

        filter_row.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter by callsign/name/groups/band/mode/freq")
        filter_row.addWidget(self.search_edit, stretch=1)
        self.clear_filters_btn = QPushButton("Clear Filters")
        filter_row.addWidget(self.clear_filters_btn)
        filter_row.addStretch()
        layout.addLayout(filter_row)

        # Table
        self.table = QTableWidget(0, len(self.COLS))
        self._overlap_col = self.COLS.index("OVERLAP")
        self._set_time_headers()
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        layout.addWidget(self.table)

        # Signals
        self.import_btn.clicked.connect(self._import_schedule)
        self.refresh_btn.clicked.connect(self._load_data)
        self.callsign_filter.currentIndexChanged.connect(self._apply_filters)
        self.region_filter.currentIndexChanged.connect(self._apply_filters)
        self.group_filter.currentIndexChanged.connect(self._apply_filters)
        self.search_edit.textChanged.connect(self._apply_filters)
        self.clear_filters_btn.clicked.connect(self._clear_filters)
        self.delete_callsign_combo.currentIndexChanged.connect(self._update_delete_button_state)
        self.delete_btn.clicked.connect(self._delete_selected)
        self.tz_toggle_btn.clicked.connect(self._toggle_timezone_view)
        self.table.cellClicked.connect(self._on_table_cell_clicked)
        self.tz_toggle_btn.setText("Showing: Local" if self._show_local_times else "Showing: UTC")
        self._apply_theme()
        self._update_delete_button_state()

    # ---------- data ----------

    def _db_path(self) -> Path:
        from freqinout.core.config_paths import get_config_dir

        return get_config_dir() / "config" / "freqinout_nets.db"

    @staticmethod
    def _normalize_callsign(value: object) -> str:
        raw = str(value or "").strip().upper()
        if not raw:
            return ""
        return re.sub(r"/(P|M|MM|QRP|SOTA|ROVER|[A-Z0-9]{1,4})$", "", raw)

    def _load_operator_meta(self) -> None:
        """
        Load operator info (name/state/groups) for display and region mapping.
        """
        self._operator_meta = {}
        try:
            db_path = self._db_path()
            if not db_path.exists():
                return
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT callsign, name, state, group1, group2, group3, groups_json FROM operator_checkins"
            )
            for cs, name, state, g1, g2, g3, gj in cur.fetchall():
                cs_norm = self._normalize_callsign(cs)
                if not cs_norm:
                    continue
                groups: List[str] = []
                for g in (g1, g2, g3):
                    if g:
                        groups.append(str(g).strip().upper())
                try:
                    if gj:
                        for g in json.loads(gj):
                            gtxt = str(g).strip().upper()
                            if gtxt:
                                groups.append(gtxt)
                except Exception:
                    pass
                deduped: List[str] = []
                seen = set()
                for g in groups:
                    if g and g not in seen:
                        seen.add(g)
                        deduped.append(g)
                self._operator_meta[cs_norm] = {
                    "name": (name or "").strip(),
                    "state": (state or "").strip().upper(),
                    "groups": ", ".join(deduped),
                }
            conn.close()
        except Exception as e:
            log.debug("PeerSched: failed to load operator meta: %s", e)

    def _load_data(self) -> None:
        """
        Load peer schedules from DB and populate filters/table.
        """
        self._load_operator_meta()
        self._rows = []
        try:
            db_path = self._db_path()
            if not db_path.exists():
                return
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            has_effective_view = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='view' AND name='peer_hf_schedule_effective'"
            ).fetchone()
            if has_effective_view:
                cur.execute(
                    """
                    SELECT
                        owner_callsign,
                        day_utc,
                        start_utc,
                        end_utc,
                        band,
                        mode,
                        frequency,
                        source_type,
                        confidence
                    FROM peer_hf_schedule_effective
                    """
                )
                rows = cur.fetchall()
            else:
                cur.execute(
                    """
                    SELECT owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency, 'IMPORTED', NULL
                    FROM peer_hf_schedule
                """
                )
                rows = cur.fetchall()
            # Collapse suffix variants to base callsign and dedupe by schedule identity.
            deduped_rows: Dict[tuple, Dict] = {}
            for cs, day, start, end, band, mode, freq, source_type, confidence in rows:
                cs_norm = self._normalize_callsign(cs)
                if not cs_norm:
                    continue
                day_val = (day or "ALL")
                start_val = (start or "")
                end_val = (end or "")
                band_val = (band or "")
                mode_val = (mode or "")
                freq_txt = str(freq or "")
                src = (source_type or "IMPORTED")
                key = (
                    cs_norm,
                    str(day_val).strip().upper(),
                    str(start_val).strip(),
                    str(end_val).strip(),
                    str(band_val).strip().upper(),
                    str(mode_val).strip().upper(),
                    str(freq_txt).strip(),
                )
                row_obj = {
                    "callsign": cs_norm,
                    "day_utc": day_val,
                    "start_utc": start_val,
                    "end_utc": end_val,
                    "band": band_val,
                    "mode": mode_val,
                    "frequency": freq_txt,
                    "freq_num": self._parse_freq(freq),
                    "source_type": src,
                    "confidence": confidence,
                }
                src_priority = 1 if str(src).strip().upper() == "IMPORTED" else 0
                prev = deduped_rows.get(key)
                if prev is None:
                    row_obj["_src_priority"] = src_priority
                    deduped_rows[key] = row_obj
                    continue
                prev_priority = int(prev.get("_src_priority", 0) or 0)
                if src_priority > prev_priority:
                    row_obj["_src_priority"] = src_priority
                    deduped_rows[key] = row_obj
                    continue
                if src_priority == prev_priority:
                    try:
                        conf_prev = float(prev.get("confidence") or 0.0)
                    except Exception:
                        conf_prev = 0.0
                    try:
                        conf_new = float(confidence or 0.0)
                    except Exception:
                        conf_new = 0.0
                    if conf_new > conf_prev:
                        row_obj["_src_priority"] = src_priority
                        deduped_rows[key] = row_obj
            self._rows = sorted(
                [
                    {k: v for k, v in row.items() if k != "_src_priority"}
                    for row in deduped_rows.values()
                ],
                key=lambda r: (
                    str(r.get("callsign", "")),
                    str(r.get("day_utc", "")),
                    str(r.get("start_utc", "")),
                    str(r.get("frequency", "")),
                ),
            )
            conn.close()
        except Exception as e:
            log.error("PeerSched: failed to load peer schedules: %s", e)
        self._load_my_schedule()
        self._populate_filters()
        self._populate_delete_callsigns()
        self._apply_filters()

    def _populate_filters(self) -> None:
        calls = sorted({row["callsign"] for row in self._rows if row.get("callsign")})
        groups = sorted(
            {
                g.strip()
                for meta in self._operator_meta.values()
                for g in meta.get("groups", "").split(",")
                if g.strip()
            }
        )

        # Callsign filter
        current_call = self.callsign_filter.currentText()
        self.callsign_filter.blockSignals(True)
        self.callsign_filter.clear()
        self.callsign_filter.addItem("All")
        for c in calls:
            self.callsign_filter.addItem(c)
        idx = self.callsign_filter.findText(current_call)
        if idx >= 0:
            self.callsign_filter.setCurrentIndex(idx)
        self.callsign_filter.blockSignals(False)

        # Group filter
        current_group = self.group_filter.currentText()
        self.group_filter.blockSignals(True)
        self.group_filter.clear()
        self.group_filter.addItem("All")
        for g in groups:
            self.group_filter.addItem(g)
        idx = self.group_filter.findText(current_group)
        if idx >= 0:
            self.group_filter.setCurrentIndex(idx)
        self.group_filter.blockSignals(False)

    def _populate_delete_callsigns(self) -> None:
        calls = sorted(
            {
                row["callsign"]
                for row in self._rows
                if row.get("callsign")
                and str(row.get("source_type") or "").strip().upper() != "INFERRED"
            }
        )
        current = self.delete_callsign_combo.currentData()
        self.delete_callsign_combo.blockSignals(True)
        self.delete_callsign_combo.clear()
        self.delete_callsign_combo.addItem("Select callsign", None)
        self.delete_callsign_combo.addItem("Clear All Schedules", "__CLEAR_ALL__")
        for c in calls:
            self.delete_callsign_combo.addItem(c, c)
        idx = self.delete_callsign_combo.findData(current)
        if idx >= 0:
            self.delete_callsign_combo.setCurrentIndex(idx)
        self.delete_callsign_combo.blockSignals(False)
        self._update_delete_button_state()

    def _apply_filters(self) -> None:
        cs_filter = self.callsign_filter.currentText()
        region_filter = self.region_filter.currentText()
        group_filter = self.group_filter.currentText()
        search = self.search_edit.text().strip().lower()

        filtered: List[Dict] = []
        for row in self._rows:
            cs = row.get("callsign", "")
            if cs_filter != "All" and cs != cs_filter:
                continue
            meta = self._operator_meta.get(cs, {})
            state = meta.get("state", "")
            region = STATE_TO_REGION.get(state, "")
            if region_filter != "All" and region != region_filter:
                continue
            groups = [g.strip() for g in meta.get("groups", "").split(",") if g.strip()]
            if group_filter != "All" and group_filter not in groups:
                continue
            if search:
                blob = " ".join(
                    [
                        cs,
                        meta.get("name", ""),
                        meta.get("groups", ""),
                        row.get("band", ""),
                        row.get("mode", ""),
                        row.get("frequency", ""),
                    ]
                ).lower()
                if search not in blob:
                    continue
            filtered.append(row)

        was_sorting = self.table.isSortingEnabled()
        sort_col = self.table.horizontalHeader().sortIndicatorSection()
        sort_order = self.table.horizontalHeader().sortIndicatorOrder()
        if was_sorting:
            self.table.setSortingEnabled(False)
        self.table.setRowCount(len(filtered))
        for r, row in enumerate(filtered):
            cs = row.get("callsign", "")
            meta = self._operator_meta.get(cs, {})
            overlap_ranges = self._compute_overlaps(row)
            overlap_display = self._format_overlap_summary(overlap_ranges)
            mode_val = row.get("mode", "")
            if str(row.get("source_type") or "").strip().upper() == "INFERRED":
                mode_val = f"{mode_val} [I]"
            vals = [
                cs,
                meta.get("name", ""),
                meta.get("state", ""),
                meta.get("groups", ""),
                row.get("day_utc", ""),
                row.get("start_utc", ""),
                row.get("end_utc", ""),
                row.get("band", ""),
                mode_val,
                row.get("frequency", ""),
                overlap_display,
            ]
            if self._show_local_times:
                day_loc, start_loc = self._convert_day_time(row.get("day_utc", ""), row.get("start_utc", ""))
                _, end_loc = self._convert_day_time(row.get("day_utc", ""), row.get("end_utc", ""))
                vals[4] = day_loc
                vals[5] = start_loc
                vals[6] = end_loc
            for c, val in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                if c == self._overlap_col and overlap_ranges:
                    item.setToolTip("\n".join(self._format_overlap_ranges(overlap_ranges)))
                    item.setData(Qt.UserRole, overlap_ranges)
                self.table.setItem(r, c, item)
        if was_sorting:
            self.table.setSortingEnabled(True)
            if 0 <= sort_col < self.table.columnCount():
                self.table.sortItems(sort_col, sort_order)

    # ---------- helpers ----------

    def _selected_delete_action(self) -> Optional[str]:
        selected = self.delete_callsign_combo.currentData()
        if not selected:
            return None
        if selected == "__CLEAR_ALL__":
            return selected
        return self._normalize_callsign(selected)

    def _delete_callsign_variants(self, cur: sqlite3.Cursor, table: str, callsign: str) -> int:
        base = self._normalize_callsign(callsign)
        if not base:
            return 0
        deleted = 0
        try:
            cur.execute(
                f"""
                SELECT DISTINCT owner_callsign
                FROM {table}
                WHERE owner_callsign IS NOT NULL AND TRIM(owner_callsign) <> ''
                """
            )
            raw_values = [str(v or "").strip() for (v,) in cur.fetchall()]
        except Exception:
            raw_values = []
        for raw in raw_values:
            if self._normalize_callsign(raw) != base:
                continue
            cur.execute(f"DELETE FROM {table} WHERE owner_callsign=?", (raw,))
            deleted += int(cur.rowcount or 0)
        return deleted

    def _settings_db_path(self) -> Path:
        cfg_path = getattr(self.settings, "_config_path", None)
        if cfg_path:
            try:
                return Path(cfg_path)
            except Exception:
                pass
        from freqinout.core.config_paths import get_config_dir

        return get_config_dir() / "config" / "freqinout.db"

    def _current_timezone(self) -> datetime.tzinfo:
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        return get_timezone(tz_name)

    def _day_offset(self, day_name: str) -> int:
        if not day_name:
            return 0
        day_name = day_name.strip().lower()
        for idx, name in enumerate(self.DAY_CANON):
            if name.lower().startswith(day_name[:3]):
                return idx
        return 0

    def _anchor_utc_sunday(self) -> datetime.datetime:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        delta = (now_utc.weekday() + 1) % 7  # Sunday=0, Monday=1, ...
        sunday = now_utc - datetime.timedelta(days=delta)
        return sunday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)

    def _convert_day_time(self, day: str, hhmm: str) -> tuple[str, str]:
        day = (day or "ALL").strip()
        if not hhmm:
            return day, hhmm
        try:
            hour, minute = hhmm.split(":")
            hour = int(hour)
            minute = int(minute)
        except Exception:
            return day, hhmm
        day_upper = day.upper()
        day_idx = 0 if day_upper == "ALL" else self._day_offset(day)
        anchor = self._anchor_utc_sunday()
        dt_utc = anchor + datetime.timedelta(days=day_idx, hours=hour, minutes=minute)
        dt_loc = dt_utc.astimezone(self._current_timezone())
        day_label = "ALL" if day_upper == "ALL" else dt_loc.strftime("%A")
        return day_label, dt_loc.strftime("%H:%M")

    def _parse_time_minutes(self, hhmm: str) -> Optional[int]:
        if not hhmm:
            return None
        try:
            hour, minute = hhmm.split(":")
            return int(hour) * 60 + int(minute)
        except Exception:
            return None

    def _parse_freq(self, freq_val) -> Optional[float]:
        try:
            return float(str(freq_val).strip())
        except Exception:
            return None

    def _day_to_index(self, day_name: str) -> Optional[int]:
        if not day_name:
            return None
        txt = day_name.strip().lower()
        for idx, name in enumerate(self.DAY_CANON):
            low = name.lower()
            if txt.startswith(low[:3]) or low.startswith(txt[:3]):
                return idx
        return None

    def _normalize_day(self, day: str) -> str:
        val = (day or "ALL").strip()
        up = val.upper()
        if up in {"", "ALL", "DAILY"}:
            return "ALL"
        idx = self._day_to_index(val)
        if idx is None:
            return "ALL"
        return self.DAY_CANON[idx]

    def _expand_week_segments(self, day: str, start_min: int, end_min: int) -> List[tuple[int, int, int]]:
        """
        Expand a schedule row into weekly minute windows:
          (weekday_index_sun0, start_minute, end_minute)
        Supports overnight ranges by splitting across day boundary.
        """
        if start_min < 0 or start_min > 1439 or end_min < 0 or end_min > 1439:
            return []
        days: List[int]
        day_norm = self._normalize_day(day)
        if day_norm == "ALL":
            days = list(range(7))
        else:
            idx = self._day_to_index(day_norm)
            if idx is None:
                return []
            days = [idx]
        segments: List[tuple[int, int, int]] = []
        for d in days:
            if end_min > start_min:
                segments.append((d, start_min, end_min))
                continue
            if end_min == start_min:
                continue
            # Overnight: split into [start,24:00) on day d and [00:00,end) next day.
            segments.append((d, start_min, 24 * 60))
            segments.append(((d + 1) % 7, 0, end_min))
        return segments

    def _load_my_schedule(self) -> None:
        self._my_schedule = []
        self._my_schedule_by_mode = {}

        def add_entry(day: str, start: str, end: str, mode: str, freq) -> None:
            start_min = self._parse_time_minutes(start)
            end_min = self._parse_time_minutes(end)
            freq_num = self._parse_freq(freq)
            if start_min is None or end_min is None or freq_num is None:
                return
            mode_key = (mode or "").strip().upper()
            if not mode_key:
                return
            segments = self._expand_week_segments(day, start_min, end_min)
            if not segments:
                return
            entry = {
                "day_utc": self._normalize_day(day),
                "start_min": start_min,
                "end_min": end_min,
                "segments": segments,
                "mode": mode_key,
                "freq": freq_num,
            }
            self._my_schedule.append(entry)
            self._my_schedule_by_mode.setdefault(mode_key, []).append(entry)

        db_path = self._settings_db_path()
        if db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                has_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='daily_schedule_tab'"
                ).fetchone()
                if has_table:
                    for day, start, end, mode, freq in conn.execute(
                        "SELECT day_utc, start_utc, end_utc, mode, frequency FROM daily_schedule_tab"
                    ).fetchall():
                        add_entry(day, start, end, mode, freq)
                conn.close()
            except Exception as e:
                log.debug("PeerSched: failed to load daily schedule: %s", e)

        nets_path = self._db_path()
        if nets_path.exists():
            try:
                conn = sqlite3.connect(nets_path)
                has_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='net_schedule_tab'"
                ).fetchone()
                if has_table:
                    for day, start, end, mode, freq in conn.execute(
                        "SELECT day_utc, start_utc, end_utc, mode, frequency FROM net_schedule_tab"
                    ).fetchall():
                        add_entry(day, start, end, mode, freq)
                conn.close()
            except Exception as e:
                log.debug("PeerSched: failed to load net schedule: %s", e)

    def _compute_overlaps(self, row: Dict) -> List[tuple[int, int, int]]:
        mode = (row.get("mode") or "").strip().upper()
        freq = row.get("freq_num")
        if not mode or freq is None:
            return []
        peer_start = self._parse_time_minutes(row.get("start_utc", ""))
        peer_end = self._parse_time_minutes(row.get("end_utc", ""))
        if peer_start is None or peer_end is None:
            return []
        peer_segments = self._expand_week_segments(row.get("day_utc", "ALL"), peer_start, peer_end)
        if not peer_segments:
            return []

        matches = self._my_schedule_by_mode.get(mode, [])
        overlaps: List[tuple[int, int, int]] = []
        by_day: Dict[int, List[tuple[int, int]]] = {}
        for day_idx, seg_start, seg_end in peer_segments:
            by_day.setdefault(day_idx, []).append((seg_start, seg_end))
        for entry in matches:
            if abs(entry["freq"] - freq) > 0.0001:
                continue
            entry_segments = entry.get("segments") or []
            for day_idx, my_start, my_end in entry_segments:
                peers = by_day.get(int(day_idx), [])
                if not peers:
                    continue
                for peer_seg_start, peer_seg_end in peers:
                    start = max(int(peer_seg_start), int(my_start))
                    end = min(int(peer_seg_end), int(my_end))
                    if end > start:
                        overlaps.append((int(day_idx), start, end))
        if not overlaps:
            return overlaps
        # Merge touching/overlapping ranges per day for cleaner display.
        overlaps.sort(key=lambda x: (x[0], x[1], x[2]))
        merged: List[tuple[int, int, int]] = []
        for day_idx, start, end in overlaps:
            if not merged or merged[-1][0] != day_idx or start > merged[-1][2]:
                merged.append((day_idx, start, end))
                continue
            prev_day, prev_start, prev_end = merged[-1]
            merged[-1] = (prev_day, prev_start, max(prev_end, end))
        return merged

    def _format_overlap_ranges(self, ranges: List[tuple[int, int, int]]) -> List[str]:
        if not ranges:
            return []
        base_utc = self._anchor_utc_sunday()
        tz = self._current_timezone()
        use_local = self._show_local_times
        formatted = []
        for day_idx, start_min, end_min in ranges:
            start_dt = base_utc + datetime.timedelta(days=int(day_idx), minutes=int(start_min))
            end_dt = base_utc + datetime.timedelta(days=int(day_idx), minutes=int(end_min))
            if use_local:
                start_dt = start_dt.astimezone(tz)
                end_dt = end_dt.astimezone(tz)
            day_label = start_dt.strftime("%a")
            formatted.append(f"{day_label} {start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}")
        return formatted

    def _overlap_range_datetimes(
        self,
        ranges: List[tuple[int, int, int]],
    ) -> List[tuple[datetime.datetime, datetime.datetime]]:
        if not ranges:
            return []
        base_utc = self._anchor_utc_sunday()
        use_local = self._show_local_times
        tz = self._current_timezone() if use_local else datetime.timezone.utc
        out: List[tuple[datetime.datetime, datetime.datetime]] = []
        for day_idx, start_min, end_min in ranges:
            start_utc = base_utc + datetime.timedelta(days=int(day_idx), minutes=int(start_min))
            end_utc = base_utc + datetime.timedelta(days=int(day_idx), minutes=int(end_min))
            if use_local:
                start_dt = start_utc.astimezone(tz)
                end_dt = end_utc.astimezone(tz)
            else:
                start_dt = start_utc
                end_dt = end_utc
            out.append((start_dt, end_dt))
        out.sort(key=lambda it: it[0])
        return out

    def _format_overlap_summary(self, ranges: List[tuple[int, int, int]]) -> str:
        if not ranges:
            return ""
        slots = self._overlap_range_datetimes(ranges)
        if not slots:
            return ""
        use_local = self._show_local_times
        tz = self._current_timezone() if use_local else datetime.timezone.utc
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_dt = now_utc.astimezone(tz) if use_local else now_utc

        # 1) Action now: currently overlapping.
        active = [slot for slot in slots if slot[0] <= now_dt < slot[1]]
        if active:
            start_dt, end_dt = min(active, key=lambda it: it[1])
            return f"NOW {start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"

        # 2) Action later today.
        later_today = [slot for slot in slots if slot[0].date() == now_dt.date() and slot[0] > now_dt]
        if later_today:
            start_dt, end_dt = min(later_today, key=lambda it: it[0])
            return f"Today {start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"

        # 3) Next weekly overlap (roll past slots by +7 days).
        rolled: List[tuple[datetime.datetime, datetime.datetime]] = []
        for start_dt, end_dt in slots:
            while end_dt <= now_dt:
                start_dt += datetime.timedelta(days=7)
                end_dt += datetime.timedelta(days=7)
            rolled.append((start_dt, end_dt))
        if not rolled:
            return ""
        next_start, next_end = min(rolled, key=lambda it: it[0])
        if next_start.date() == now_dt.date():
            return f"Today {next_start.strftime('%H:%M')}-{next_end.strftime('%H:%M')}"
        return f"{next_start.strftime('%a')} {next_start.strftime('%H:%M')}-{next_end.strftime('%H:%M')}"

    def _set_time_headers(self) -> None:
        cols = list(self.COLS)
        if self._show_local_times:
            cols[4] = "DAY (Local)"
            cols[5] = "START (Local)"
            cols[6] = "END (Local)"
            cols[self._overlap_col] = "OVERLAP (Local)"
        else:
            cols[4] = "DAY (UTC)"
            cols[5] = "START UTC"
            cols[6] = "END UTC"
            cols[self._overlap_col] = "OVERLAP (UTC)"
        self.table.setHorizontalHeaderLabels(cols)

    def _toggle_timezone_view(self) -> None:
        self._show_local_times = not self._show_local_times
        self.tz_toggle_btn.setText("Showing: Local" if self._show_local_times else "Showing: UTC")
        self._set_time_headers()
        self._update_timezone_button_style()
        self._apply_filters()

    def _apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.delete_btn.setStyleSheet(button_style("muted", theme))
        self.clear_filters_btn.setStyleSheet(button_style("muted", theme))
        self._update_timezone_button_style()

    def apply_theme(self) -> None:
        self._apply_theme()

    def _clear_filters(self) -> None:
        self.callsign_filter.setCurrentIndex(0)
        self.region_filter.setCurrentIndex(0)
        self.group_filter.setCurrentIndex(0)
        self.search_edit.clear()

    def _update_delete_button_state(self) -> None:
        theme = resolve_theme(self.settings)
        selection = self._selected_delete_action()
        has_rows = any(
            str(r.get("source_type") or "").strip().upper() != "INFERRED"
            for r in self._rows
        )
        if selection == "__CLEAR_ALL__":
            enabled = has_rows
            label = "Clear All"
        elif selection:
            enabled = True
            label = "Delete Schedule"
        else:
            enabled = False
            label = "Delete Schedule"
        self.delete_btn.setEnabled(enabled)
        self.delete_btn.setText(label)
        role = "danger" if enabled else "muted"
        self.delete_btn.setStyleSheet(button_style(role, theme))

    def _update_timezone_button_style(self) -> None:
        theme = resolve_theme(self.settings)
        self.tz_toggle_btn.setStyleSheet(button_style("primary", theme))

    def _on_table_cell_clicked(self, row: int, col: int) -> None:
        if col != self._overlap_col:
            return
        item = self.table.item(row, col)
        if not item:
            return
        ranges = item.data(Qt.UserRole)
        if not ranges or len(ranges) <= 1:
            return
        lines = self._format_overlap_ranges(ranges)
        if not lines:
            return
        msg = "\n".join(lines)
        QMessageBox.information(self, "Overlap Details", msg)

    # ---------- import / delete ----------

    def _import_schedule(self) -> None:
        """
        Import a peer HF schedule JSON and store it in peer_hf_schedule.
        """
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Import Peer HF Schedule",
            "",
            "JSON Files (*.json);;All Files (*)",
        )
        if not path:
            return
        try:
            raw = Path(path).read_text(encoding="utf-8")
            data = json.loads(raw)
        except Exception as e:
            QMessageBox.critical(self, "Import Failed", f"Could not read JSON:\n{e}")
            return

        owner = self._normalize_callsign(data.get("callsign"))
        rows = data.get("rows", [])
        if not owner or not isinstance(rows, list):
            QMessageBox.warning(self, "Invalid File", "Expected keys: 'callsign' and 'rows'.")
            return

        valid_rows: List[Dict] = []
        for row in rows:
            try:
                day = (row.get("day_utc", "ALL") or "ALL").strip()
                start = (row.get("start_utc") or "").strip()
                end = (row.get("end_utc") or "").strip()
                band = (row.get("band") or "").strip()
                mode = (row.get("mode") or "").strip()
                freq = str(row.get("frequency") or "").strip()
                if not start or not end or not band:
                    continue
                valid_rows.append(
                    {
                        "day_utc": day,
                        "start_utc": start,
                        "end_utc": end,
                        "band": band,
                        "mode": mode,
                        "frequency": freq,
                    }
                )
            except Exception:
                continue

        if not valid_rows:
            QMessageBox.warning(self, "Import", "No valid rows found to import.")
            return

        try:
            db_path = self._db_path()
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            self._delete_callsign_variants(cur, "peer_hf_schedule", owner)
            # Imported schedule is authoritative for this callsign; clear stale inferred rows.
            self._delete_callsign_variants(cur, "peer_hf_schedule_inferred", owner)
            now_str = datetime.datetime.utcnow().isoformat()
            for row in valid_rows:
                cur.execute(
                    """
                    INSERT INTO peer_hf_schedule
                        (owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency, meta_json, imported_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        owner,
                        row["day_utc"],
                        row["start_utc"],
                        row["end_utc"],
                        row["band"],
                        row["mode"],
                        row["frequency"],
                        json.dumps({"created_utc": data.get("created_utc"), "timezone": data.get("timezone")}),
                        now_str,
                    ),
                )
            conn.commit()
            conn.close()
            QMessageBox.information(self, "Import", f"Imported {len(valid_rows)} rows for {owner}.")
            self._load_data()
        except Exception as e:
            QMessageBox.critical(self, "Import Failed", f"DB write failed:\n{e}")
            log.error("PeerSched: import failed: %s", e)

    def _delete_selected(self) -> None:
        action = self._selected_delete_action()
        if not action:
            QMessageBox.information(self, "Delete", "Select a callsign to delete.")
            return
        if action == "__CLEAR_ALL__":
            self._clear_all()
            return
        cs = action
        confirm = QMessageBox.question(
            self,
            "Delete Schedule",
            f"Delete all imported schedule rows for {cs}?",
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            conn = sqlite3.connect(self._db_path())
            cur = conn.cursor()
            deleted = self._delete_callsign_variants(cur, "peer_hf_schedule", cs)
            conn.commit()
            conn.close()
            self._load_data()
            QMessageBox.information(self, "Delete", f"Deleted {deleted} imported row(s) for {cs}.")
        except Exception as e:
            QMessageBox.critical(self, "Delete Failed", f"DB delete failed:\n{e}")
            log.error("PeerSched: delete failed for %s: %s", cs, e)

    def _clear_all(self) -> None:
        has_imported = any(
            str(r.get("source_type") or "").strip().upper() != "INFERRED"
            for r in self._rows
        )
        if not has_imported:
            return
        confirm = QMessageBox.question(
            self,
            "Clear All",
            "Delete all imported peer schedules?",
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            conn = sqlite3.connect(self._db_path())
            cur = conn.cursor()
            cur.execute("DELETE FROM peer_hf_schedule")
            conn.commit()
            conn.close()
            self._load_data()
            QMessageBox.information(self, "Clear All", "All peer schedules removed.")
        except Exception as e:
            QMessageBox.critical(self, "Clear Failed", f"DB delete failed:\n{e}")
            log.error("PeerSched: clear all failed: %s", e)
