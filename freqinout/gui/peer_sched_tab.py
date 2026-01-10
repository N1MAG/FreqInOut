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
    )

    def __init__(self, parent=None):
        super().__init__(parent)
        self._rows: List[Dict] = []
        self._operator_meta: Dict[str, Dict[str, str]] = {}
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
        self.delete_btn = QPushButton("Delete Selected")
        self.clear_btn = QPushButton("Clear All")
        header.addWidget(self.import_btn)
        header.addWidget(self.refresh_btn)
        header.addWidget(self.delete_btn)
        header.addWidget(self.clear_btn)
        layout.addLayout(header)

        # Filters
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Callsign:"))
        self.callsign_filter = QComboBox()
        self.callsign_filter.addItem("All")
        filter_row.addWidget(self.callsign_filter)

        filter_row.addWidget(QLabel("Region:"))
        self.region_filter = QComboBox()
        self.region_filter.addItem("All")
        for r in sorted(FEMA_REGIONS.keys()):
            self.region_filter.addItem(r)
        filter_row.addWidget(self.region_filter)

        filter_row.addWidget(QLabel("Group:"))
        self.group_filter = QComboBox()
        self.group_filter.addItem("All")
        filter_row.addWidget(self.group_filter)

        filter_row.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter by callsign/name/groups/band/mode/freq")
        filter_row.addWidget(self.search_edit, stretch=1)
        filter_row.addStretch()
        layout.addLayout(filter_row)

        # Table
        self.table = QTableWidget(0, len(self.COLS))
        self.table.setHorizontalHeaderLabels(self.COLS)
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
        self.delete_btn.clicked.connect(self._delete_selected)
        self.clear_btn.clicked.connect(self._clear_all)

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
                    }
                )
            conn.close()
        except Exception as e:
            log.error("PeerSched: failed to load peer schedules: %s", e)
        self._populate_filters()
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
            ]
            for c, val in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                self.table.setItem(r, c, item)

    # ---------- helpers ----------

    def _selected_callsign(self) -> Optional[str]:
        row = self.table.currentRow()
        if row < 0:
            return None
        item = self.table.item(row, 0)
        if not item:
            return None
        return (item.text() or "").strip().upper()

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
        cs = self._selected_callsign()
        if not cs:
            QMessageBox.information(self, "Delete", "Select a schedule row to choose an operator to delete.")
            return
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
