from __future__ import annotations

import datetime
import json
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
        self._show_local_times = False
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
        self.tz_toggle_btn = QPushButton("Show Local")
        self.tz_toggle_btn.setCheckable(True)
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
        filter_row.addStretch()
        layout.addLayout(filter_row)

        # Table
        self.table = QTableWidget(0, len(self.COLS))
        self._overlap_col = self.COLS.index("OVERLAP")
        self._set_time_headers()
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
        self.delete_callsign_combo.currentIndexChanged.connect(self._update_delete_button_state)
        self.delete_btn.clicked.connect(self._delete_selected)
        self.tz_toggle_btn.toggled.connect(self._toggle_timezone_view)
        self.table.cellClicked.connect(self._on_table_cell_clicked)
        self._apply_theme()
        self._update_delete_button_state()

    # ---------- data ----------

    def _db_path(self) -> Path:
        from freqinout.core.config_paths import get_config_dir

        return get_config_dir() / "config" / "freqinout_nets.db"

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
                self._operator_meta[cs.upper()] = {
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
            cur.execute(
                """
                SELECT owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency
                FROM peer_hf_schedule
            """
            )
            for cs, day, start, end, band, mode, freq in cur.fetchall():
                self._rows.append(
                    {
                        "callsign": (cs or "").upper(),
                        "day_utc": day or "ALL",
                        "start_utc": start or "",
                        "end_utc": end or "",
                        "band": band or "",
                        "mode": mode or "",
                        "frequency": str(freq or ""),
                        "freq_num": self._parse_freq(freq),
                    }
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
        calls = sorted({row["callsign"] for row in self._rows if row.get("callsign")})
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

        self.table.setRowCount(len(filtered))
        for r, row in enumerate(filtered):
            cs = row.get("callsign", "")
            meta = self._operator_meta.get(cs, {})
            overlap_ranges = self._compute_overlaps(row)
            overlap_display = self._format_overlap_summary(overlap_ranges)
            vals = [
                cs,
                meta.get("name", ""),
                meta.get("state", ""),
                meta.get("groups", ""),
                row.get("day_utc", ""),
                row.get("start_utc", ""),
                row.get("end_utc", ""),
                row.get("band", ""),
                row.get("mode", ""),
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

    # ---------- helpers ----------

    def _selected_delete_action(self) -> Optional[str]:
        selected = self.delete_callsign_combo.currentData()
        if not selected:
            return None
        if selected == "__CLEAR_ALL__":
            return selected
        return str(selected).strip().upper()

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

    def _day_matches_today(self, day: str, today_name: str) -> bool:
        day = (day or "ALL").strip().upper()
        if day in ("ALL", "DAILY"):
            return True
        if not day:
            return False
        today = today_name.upper()
        return today.startswith(day[:3]) or day.startswith(today[:3])

    def _load_my_schedule(self) -> None:
        self._my_schedule = []
        self._my_schedule_by_mode = {}
        today_name = datetime.datetime.now(datetime.timezone.utc).strftime("%A")

        def add_entry(day: str, start: str, end: str, mode: str, freq) -> None:
            start_min = self._parse_time_minutes(start)
            end_min = self._parse_time_minutes(end)
            freq_num = self._parse_freq(freq)
            if start_min is None or end_min is None or freq_num is None:
                return
            if end_min <= start_min:
                return
            if not self._day_matches_today(day, today_name):
                return
            mode_key = (mode or "").strip().upper()
            if not mode_key:
                return
            entry = {
                "day_utc": (day or "ALL").strip(),
                "start_min": start_min,
                "end_min": end_min,
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

    def _compute_overlaps(self, row: Dict) -> List[tuple[int, int]]:
        mode = (row.get("mode") or "").strip().upper()
        freq = row.get("freq_num")
        if not mode or freq is None:
            return []
        today_name = datetime.datetime.now(datetime.timezone.utc).strftime("%A")
        if not self._day_matches_today(row.get("day_utc", "ALL"), today_name):
            return []
        peer_start = self._parse_time_minutes(row.get("start_utc", ""))
        peer_end = self._parse_time_minutes(row.get("end_utc", ""))
        if peer_start is None or peer_end is None or peer_end <= peer_start:
            return []
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_min = now_utc.hour * 60 + now_utc.minute
        if peer_end <= now_min:
            return []

        matches = self._my_schedule_by_mode.get(mode, [])
        overlaps: List[tuple[int, int]] = []
        for entry in matches:
            if abs(entry["freq"] - freq) > 0.0001:
                continue
            start = max(peer_start, entry["start_min"], now_min)
            end = min(peer_end, entry["end_min"])
            if end > start:
                overlaps.append((start, end))
        overlaps.sort()
        return overlaps

    def _format_overlap_ranges(self, ranges: List[tuple[int, int]]) -> List[str]:
        if not ranges:
            return []
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        base_date = now_utc.date()
        tz = self._current_timezone()
        use_local = self._show_local_times
        formatted = []
        for start_min, end_min in ranges:
            start_dt = datetime.datetime(
                base_date.year,
                base_date.month,
                base_date.day,
                start_min // 60,
                start_min % 60,
                tzinfo=datetime.timezone.utc,
            )
            end_dt = datetime.datetime(
                base_date.year,
                base_date.month,
                base_date.day,
                end_min // 60,
                end_min % 60,
                tzinfo=datetime.timezone.utc,
            )
            if use_local:
                start_dt = start_dt.astimezone(tz)
                end_dt = end_dt.astimezone(tz)
            formatted.append(f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}")
        return formatted

    def _format_overlap_summary(self, ranges: List[tuple[int, int]]) -> str:
        if not ranges:
            return ""
        if len(ranges) == 1:
            return self._format_overlap_ranges(ranges)[0]
        return f"{len(ranges)} overlaps"

    def _set_time_headers(self) -> None:
        cols = list(self.COLS)
        if self._show_local_times:
            tz_abbrev = datetime.datetime.now(self._current_timezone()).tzname() or "LOCAL"
            cols[4] = f"DAY ({tz_abbrev})"
            cols[5] = f"START ({tz_abbrev})"
            cols[6] = f"END ({tz_abbrev})"
            cols[self._overlap_col] = f"OVERLAP ({tz_abbrev})"
        else:
            cols[4] = "DAY (UTC)"
            cols[5] = "START UTC"
            cols[6] = "END UTC"
            cols[self._overlap_col] = "OVERLAP (UTC)"
        self.table.setHorizontalHeaderLabels(cols)

    def _toggle_timezone_view(self, checked: bool) -> None:
        self._show_local_times = bool(checked)
        self.tz_toggle_btn.setText("Show UTC" if checked else "Show Local")
        self._set_time_headers()
        self._update_timezone_button_style()
        self._apply_filters()

    def _apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.delete_btn.setStyleSheet(button_style("muted", theme))
        self._update_timezone_button_style()

    def _update_delete_button_state(self) -> None:
        theme = resolve_theme(self.settings)
        selection = self._selected_delete_action()
        has_rows = bool(self._rows)
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
        role = "info" if self._show_local_times else "muted"
        self.tz_toggle_btn.setStyleSheet(button_style(role, theme))

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

        owner = (data.get("callsign") or "").strip().upper()
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
            cur.execute("DELETE FROM peer_hf_schedule WHERE owner_callsign=?", (owner,))
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
            cur.execute("DELETE FROM peer_hf_schedule WHERE owner_callsign=?", (cs,))
            conn.commit()
            conn.close()
            self._load_data()
            QMessageBox.information(self, "Delete", f"Deleted schedule for {cs}.")
        except Exception as e:
            QMessageBox.critical(self, "Delete Failed", f"DB delete failed:\n{e}")
            log.error("PeerSched: delete failed for %s: %s", cs, e)

    def _clear_all(self) -> None:
        if not self._rows:
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
