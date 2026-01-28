from __future__ import annotations

import csv
import datetime
import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtGui import QColor, QStandardItemModel, QStandardItem
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QMenu,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QMessageBox,
    QTextEdit,
    QFileDialog,
    QDialog,
    QFormLayout,
    QCheckBox,
    QComboBox,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.core.varac_ingest import ingest_varac
from freqinout.gui.theme import resolve_theme


def _normalize_date_only(val: Optional[str]) -> Optional[str]:
    """
    Normalize a date/datetime string to YYYYMMDD. Returns None on empty input.
    """
    if not val:
        return None
    txt = str(val).strip()
    if not txt:
        return None
    if len(txt) == 8 and txt.isdigit():
        return txt
    try:
        dt = datetime.datetime.fromisoformat(txt.replace("Z", ""))
        return dt.strftime("%Y%m%d")
    except Exception:
        pass
    digits = "".join(ch for ch in txt if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


class OperatorHistoryTab(QWidget):
    """
    Operator history viewer.

    Reads operator_checkins from freqinout_nets.db and shows:

      - Callsign
      - Name
      - State
      - Grid
      - Group1 / Group2 / Group3
      - Group Role
      - Date Added (YYYYMMDD)
      - Check-ins

    Features:
      - Refresh button
      - CSV import (callsign mandatory)
      - Add / Edit / Delete selected (selection via checkbox column)
      - Search box (filters by all visible columns, case-insensitive)
    """

    COL_SELECT = 0
    COL_CALLSIGN = 1
    COL_NAME = 2
    COL_STATE = 3
    COL_GRID = 4
    COL_GROUPS = 5
    COL_G1 = 6
    COL_G2 = 7
    COL_G3 = 8
    COL_ROLE = 9
    COL_FIRST_SEEN = 10
    COL_LAST_SEEN = 11
    COL_TRUSTED = 12
    COL_COUNT = 13

    operator_history_updated = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._rows: List[Dict] = []
        self.loading_label: QLabel | None = None
        self._nav_refresh_inflight = False
        self._update_timer = QTimer(self)
        self._update_timer.setSingleShot(True)
        self._update_timer.setInterval(300)
        self._update_timer.timeout.connect(self.operator_history_updated.emit)

        self._build_ui()
        QTimer.singleShot(0, self._load_data)

    # ------------- UI ------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Operator History</h3>"))
        self.loading_label = QLabel("Brewing it fresh...")
        self.loading_label.setStyleSheet("color: #888;")
        self.loading_label.setVisible(False)
        header.addWidget(self.loading_label)
        header.addStretch()
        layout.addLayout(header)

        # Search + actions row
        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Any column...")
        search_row.addWidget(self.search_edit, stretch=1)

        search_row.addWidget(QLabel("Filter by:"))
        self.group_filter = QComboBox()
        self.group_filter.addItem("All")
        self.group_filter.addItem("Untrusted")
        self.group_filter.setSizeAdjustPolicy(QComboBox.AdjustToContentsOnFirstShow)
        self.group_filter.setMinimumContentsLength(12)
        self.group_filter.view().setMinimumWidth(240)
        search_row.addWidget(self.group_filter)
        self.manage_btn = QPushButton("Manage Operators")
        search_row.addWidget(self.manage_btn)
        self.select_all_btn = QPushButton("Select All")
        search_row.addWidget(self.select_all_btn)
        self.export_group_combo = QComboBox()
        self.export_group_combo.setEditable(False)
        self.export_group_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.export_group_combo.setMinimumWidth(160)
        self.export_group_combo.setEditable(True)
        self.export_group_combo.lineEdit().setReadOnly(True)
        self.export_group_combo.lineEdit().setPlaceholderText("Export by Group")
        search_row.addWidget(self.export_group_combo)
        self.import_btn = QPushButton("Import/Export...")
        search_row.addWidget(self.import_btn)

        layout.addLayout(search_row)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(14)
        self.table.setHorizontalHeaderLabels(
            [
                "",
                "Callsign",
                "Name",
                "State",
                "Grid",
                "Groups",
                "Group 1",
                "Group 2",
                "Group 3",
                "Group Role",
                "First Seen",
                "Last Seen",
                "Trusted",
                "Check-ins",
            ]
        )
        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_SELECT, QHeaderView.ResizeToContents)
        hv.setMinimumSectionSize(50)
        hv.setDefaultSectionSize(100)
        for col in (
            self.COL_CALLSIGN,
            self.COL_NAME,
            self.COL_STATE,
            self.COL_GRID,
            self.COL_GROUPS,
            self.COL_G1,
            self.COL_G2,
            self.COL_G3,
            self.COL_ROLE,
            self.COL_FIRST_SEEN,
            self.COL_LAST_SEEN,
            self.COL_TRUSTED,
            self.COL_COUNT,
        ):
            hv.setSectionResizeMode(col, QHeaderView.Stretch)
        for col in (self.COL_G1, self.COL_G2, self.COL_G3):
            self.table.setColumnHidden(col, True)

        layout.addWidget(self.table)

        # Signals
        self.search_edit.textChanged.connect(self._apply_filter)
        self.import_btn.clicked.connect(self._show_import_export_menu)
        self.manage_btn.clicked.connect(self._show_manage_menu)
        self.select_all_btn.clicked.connect(self._select_all_rows)
        self.group_filter.currentTextChanged.connect(self._apply_filter)
        self.table.cellDoubleClicked.connect(self._on_cell_double_clicked)
        self._wire_export_group_combo()

    # ------------- DB LOAD ------------- #

    def _db_path(self) -> Path | None:
        """
        Use the same shared DB path as checkin_db:
            <config_dir>/config/freqinout_nets.db
        """
        try:
            from freqinout.core.config_paths import get_config_dir

            return get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.error("OperatorHistoryTab: failed to resolve DB path: %s", e)
            return None

    def _ensure_schema(self, conn: sqlite3.Connection):
        """
        Ensure operator_checkins has the unified columns.
        """
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS operator_checkins (
                callsign TEXT PRIMARY KEY,
                name TEXT,
                state TEXT,
                grid TEXT,
                group1 TEXT,
                group2 TEXT,
                group3 TEXT,
                group_role TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                last_net TEXT,
                last_role TEXT,
                checkin_count INTEGER DEFAULT 0,
                groups_json TEXT,
                trusted INTEGER DEFAULT 1
            )
            """
        )

        cur.execute("PRAGMA table_info(operator_checkins)")
        cols = [row[1] for row in cur.fetchall()]
        desired = {
            "callsign",
            "name",
            "state",
            "grid",
            "group1",
            "group2",
            "group3",
            "group_role",
            "first_seen_utc",
            "last_seen_utc",
            "last_net",
            "last_role",
            "checkin_count",
            "groups_json",
            "trusted",
        }
        if not desired.issubset(set(cols)):
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS operator_checkins_new (
                    callsign TEXT PRIMARY KEY,
                    name TEXT,
                    state TEXT,
                    grid TEXT,
                    group1 TEXT,
                    group2 TEXT,
                    group3 TEXT,
                    group_role TEXT,
                    first_seen_utc TEXT,
                    last_seen_utc TEXT,
                    last_net TEXT,
                    last_role TEXT,
                    checkin_count INTEGER DEFAULT 0,
                    groups_json TEXT,
                    trusted INTEGER DEFAULT 1
                )
                """
            )
            try:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO operator_checkins_new
                        (callsign, name, state, grid, group1, group2, group3, group_role,
                         first_seen_utc, last_seen_utc, last_net, last_role,
                         checkin_count, groups_json, trusted)
                    SELECT
                        callsign,
                        IFNULL(name,''),
                        IFNULL(state,''),
                        IFNULL(grid,''),
                        IFNULL(group1,''),
                        IFNULL(group2,''),
                        IFNULL(group3,''),
                        IFNULL(group_role,''),
                        COALESCE(first_seen_utc, last_seen_utc, date_added, ''),
                        COALESCE(last_seen_utc,''),
                        COALESCE(last_net,''),
                        COALESCE(last_role,''),
                        COALESCE(checkin_count,0),
                        groups_json,
                        COALESCE(trusted,1)
                    FROM operator_checkins
                    """
                )
            except Exception:
                pass
            cur.execute("DROP TABLE operator_checkins")
            cur.execute("ALTER TABLE operator_checkins_new RENAME TO operator_checkins")
            cur.execute("PRAGMA table_info(operator_checkins)")
            cols = [row[1] for row in cur.fetchall()]

        for missing_col, ddl in (
            ("trusted", "INTEGER DEFAULT 1"),
            ("groups_json", "TEXT"),
            ("first_seen_utc", "TEXT"),
            ("last_seen_utc", "TEXT"),
            ("last_net", "TEXT"),
            ("last_role", "TEXT"),
        ):
            if missing_col not in cols:
                cur.execute(f"ALTER TABLE operator_checkins ADD COLUMN {missing_col} {ddl}")

        # Backfill trusted to 1 and hydrate groups_json; also seed first_seen if missing.
        cur.execute("UPDATE operator_checkins SET trusted=1 WHERE trusted IS NULL")
        cur.execute(
            "SELECT callsign, group1, group2, group3, groups_json, first_seen_utc, last_seen_utc FROM operator_checkins"
        )
        rows = cur.fetchall()
        for cs, g1, g2, g3, gj, first_seen, last_seen in rows:
            if not gj:
                groups = self._normalize_groups_list([g1, g2, g3])
                cur.execute(
                    "UPDATE operator_checkins SET groups_json=? WHERE callsign=?",
                    (json.dumps(groups) if groups else None, cs),
                )
            if (not first_seen) and last_seen:
                cur.execute(
                    "UPDATE operator_checkins SET first_seen_utc=? WHERE callsign=? AND (first_seen_utc IS NULL OR first_seen_utc='')",
                    (last_seen, cs),
                )
        conn.commit()

    def _normalize_groups_list(self, groups: List[str]) -> List[str]:
        seen = set()
        norm: List[str] = []
        for g in groups:
            val = (g or "").strip()
            if not val:
                continue
            key = val.upper()
            if key in seen:
                continue
            seen.add(key)
            norm.append(val)
        return norm

    def _normalize_groups_for_save(
        self, row: Dict, existing_groups: Optional[List[str]] = None
    ) -> Tuple[List[str], str, str, str]:
        """
        Build a normalized group list and the first three columns.
        """
        raw = []
        raw.extend(row.get("groups") or [])
        for key in ("group1", "group2", "group3"):
            raw.append(row.get(key, ""))
        if existing_groups:
            raw.extend(existing_groups)
        groups = self._normalize_groups_list(raw)
        g1 = groups[0] if len(groups) > 0 else ""
        g2 = groups[1] if len(groups) > 1 else ""
        g3 = groups[2] if len(groups) > 2 else ""
        return groups, g1, g2, g3

    def _load_data(self, *, show_toast: bool = False):
        """
        Load operator_checkins table into self._rows.
        """
        if show_toast:
            self._set_loading(True)
        else:
            self._set_loading(False)
        try:
            ingest_varac(self.settings)
        except Exception:
            pass
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            self._rows = []
            self._render_rows()
            if show_toast:
                self._set_loading(False)
            return

        # One-time backfill: hydrate first_seen_utc from DIRECTED/ALL logs if earlier than stored
        self._backfill_first_seen_from_logs()

        rows: List[Dict] = []
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            self._ensure_schema(conn)
            cur.execute(
                """
                SELECT
                    IFNULL(callsign,''),
                    IFNULL(name,''),
                    IFNULL(state,''),
                    IFNULL(grid,''),
                    IFNULL(group1,''),
                    IFNULL(group2,''),
                    IFNULL(group3,''),
                    IFNULL(group_role,''),
                    IFNULL(first_seen_utc,''),
                    IFNULL(last_seen_utc,''),
                    IFNULL(checkin_count,0),
                    groups_json,
                    COALESCE(trusted,1)
                FROM operator_checkins
                ORDER BY callsign COLLATE NOCASE
                """
            )
            for (
                cs,
                name,
                state,
                grid,
                g1,
                g2,
                g3,
                role,
                first_seen,
                last_seen,
                count,
                gj,
                trusted,
            ) in cur.fetchall():
                groups = []
                try:
                    if gj:
                        maybe = json.loads(gj)
                        if isinstance(maybe, list):
                            groups = self._normalize_groups_list([str(x) for x in maybe])
                except Exception:
                    groups = []
                if not groups:
                    groups = self._normalize_groups_list([g1, g2, g3])
                rows.append(
                    {
                        "callsign": (cs or "").strip().upper(),
                        "name": (name or "").strip(),
                        "state": (state or "").strip().upper(),
                        "grid": (grid or "").strip().upper(),
                        "group1": (g1 or "").strip(),
                        "group2": (g2 or "").strip(),
                        "group3": (g3 or "").strip(),
                        "groups": groups,
                        "group_role": (role or "").strip(),
                        "first_seen_utc": _normalize_date_only(first_seen) or (first_seen or "").strip(),
                        "last_seen_utc": _normalize_date_only(last_seen) or (last_seen or "").strip(),
                        "checkin_count": int(count or 0),
                        "trusted": 1 if int(trusted or 0) else 0,
                    }
                )
            conn.close()
        except Exception as e:
            log.error("OperatorHistoryTab: failed to load from DB: %s", e)
            QMessageBox.warning(self, "DB Error", f"Failed to load operator history:\n{e}")
            rows = []

        # Ensure the operator's own callsign appears at the top if present
        my_call = (self.settings.get("operator_callsign", "") or "").strip().upper()
        if my_call:
            for idx, row in enumerate(rows):
                if row.get("callsign") == my_call:
                    rows.insert(0, rows.pop(idx))
                    break

        self._rows = rows
        self._apply_filter()
        if show_toast:
            self._set_loading(False)

    def _set_loading(self, active: bool, text: str = "Brewing it fresh...") -> None:
        if not self.loading_label:
            return
        self.loading_label.setText(text)
        self.loading_label.setVisible(bool(active))

    def _backfill_first_seen_from_logs(self):
        """
        Parse DIRECTED.TXT / ALL.TXT (if configured) and backfill earlier
        first_seen_utc values for any callsign found in operator_checkins.
        """
        directed_path = (self.settings.get("js8_directed_path", "") or "").strip()
        if not directed_path:
            return
        directed = Path(directed_path)
        all_txt = directed.parent / "ALL.TXT" if directed_path else None
        if not directed.exists():
            return
        try:
            directed_offset = int(self.settings.get("operator_backfill_directed_offset", 0) or 0)
        except Exception:
            directed_offset = 0
        try:
            all_offset = int(self.settings.get("operator_backfill_all_offset", 0) or 0)
        except Exception:
            all_offset = 0

        def parse_ts(line: str) -> Optional[str]:
            ts_str = line[:19]
            try:
                dt = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
                return dt.isoformat()
            except Exception:
                return None

        earliest: Dict[str, str] = {}
        try:
            size_now = directed.stat().st_size
            if directed_offset < 0 or directed_offset > size_now:
                directed_offset = 0
            with directed.open("r", encoding="utf-8", errors="ignore") as fh:
                if directed_offset > 0:
                    fh.seek(directed_offset)
                for line in fh:
                    if "\t" not in line:
                        continue
                    ts = parse_ts(line)
                    if not ts:
                        continue
                    parts = line.split("\t")
                    msg = parts[4] if len(parts) > 4 else ""
                    # crude extract: before the colon is origin, after is dest
                    if ":" in msg:
                        origin = msg.split(":", 1)[0].strip().upper()
                        if origin:
                            earliest[origin] = min(earliest.get(origin, ts), ts)
                        rest = msg.split(":", 1)[1]
                        tokens = rest.split()
                        if tokens:
                            dest = tokens[0].strip().upper()
                            if dest:
                                earliest[dest] = min(earliest.get(dest, ts), ts)
                try:
                    self.settings.set("operator_backfill_directed_offset", int(fh.tell()))
                except Exception:
                    pass
        except Exception:
            pass

        if all_txt and all_txt.exists():
            try:
                size_now = all_txt.stat().st_size
                if all_offset < 0 or all_offset > size_now:
                    all_offset = 0
                with all_txt.open("r", encoding="utf-8", errors="ignore") as fh:
                    if all_offset > 0:
                        fh.seek(all_offset)
                    for line in fh:
                        if "Transmitting" not in line:
                            continue
                        ts = parse_ts(line)
                        if not ts:
                            continue
                        try:
                            msg_part = line.split("JS8:", 1)[1]
                        except Exception:
                            continue
                        msg = msg_part.strip()
                        if ":" in msg:
                            origin = msg.split(":", 1)[0].strip().upper()
                            if origin:
                                earliest[origin] = min(earliest.get(origin, ts), ts)
                            rest = msg.split(":", 1)[1]
                            tokens = rest.split()
                            if tokens:
                                dest = tokens[0].strip().upper()
                                if dest:
                                    earliest[dest] = min(earliest.get(dest, ts), ts)
                    try:
                        self.settings.set("operator_backfill_all_offset", int(fh.tell()))
                    except Exception:
                        pass
            except Exception:
                pass

        if not earliest:
            return

        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            self._ensure_schema(conn)
            cur = conn.cursor()
            for cs, ts in earliest.items():
                cur.execute(
                    """
                    UPDATE operator_checkins
                       SET first_seen_utc = CASE
                            WHEN first_seen_utc IS NULL OR first_seen_utc='' THEN ?
                            WHEN first_seen_utc > ? THEN ?
                            ELSE first_seen_utc
                          END
                     WHERE callsign=?
                    """,
                    (ts, ts, ts, cs),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            log.error("OperatorHistoryTab: first_seen backfill failed: %s", e)

    # ------------- FILTER + RENDER ------------- #

    def _apply_filter(self):
        self._render_rows(self._filtered_rows())

    def _wire_export_group_combo(self) -> None:
        model = QStandardItemModel(self.export_group_combo)
        self.export_group_combo.setModel(model)
        model.itemChanged.connect(self._on_export_group_item_changed)

    def _refresh_export_group_options(self, groups: List[str]) -> None:
        model: QStandardItemModel = self.export_group_combo.model()  # type: ignore[assignment]
        prior = set(self._get_selected_export_groups())
        model.blockSignals(True)
        model.clear()
        hint = QStandardItem("Select up to 3")
        hint.setFlags(Qt.ItemIsEnabled)
        model.appendRow(hint)
        for g in groups:
            item = QStandardItem(g)
            item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
            item.setData(Qt.Checked if g in prior else Qt.Unchecked, Qt.CheckStateRole)
            model.appendRow(item)
        model.blockSignals(False)
        if self.export_group_combo.currentIndex() >= 0:
            self.export_group_combo.setCurrentIndex(-1)
        self._update_export_group_placeholder()

    def _on_export_group_item_changed(self, item: QStandardItem) -> None:
        if not (item.flags() & Qt.ItemIsUserCheckable):
            return
        if item.checkState() != Qt.Checked:
            self._update_export_group_placeholder()
            return
        selected = self._get_selected_export_groups()
        if len(selected) > 3:
            item.setCheckState(Qt.Unchecked)
            QMessageBox.information(
                self,
                "Export Groups",
                "You can select up to 3 groups for export.",
            )
        self._update_export_group_placeholder()

    def _get_selected_export_groups(self) -> List[str]:
        model: QStandardItemModel = self.export_group_combo.model()  # type: ignore[assignment]
        selected: List[str] = []
        for i in range(1, model.rowCount()):
            item = model.item(i)
            if item and item.checkState() == Qt.Checked:
                selected.append(item.text().strip())
        return [g for g in selected if g]

    def _update_export_group_placeholder(self) -> None:
        selected = self._get_selected_export_groups()
        if selected:
            self.export_group_combo.lineEdit().setPlaceholderText(f"{len(selected)} Selected")
        else:
            self.export_group_combo.lineEdit().setPlaceholderText("Export by Group")

    def _filtered_rows(self) -> List[Dict]:
        term = self.search_edit.text().strip().lower()
        filter_term = self.group_filter.currentText().strip().lower()
        if not term:
            filtered = list(self._rows)
        else:
            filtered = []
            for r in self._rows:
                if (
                    term in r["callsign"].lower()
                    or term in r["name"].lower()
                    or term in r["state"].lower()
                    or term in r.get("grid", "").lower()
                    or term in r.get("group1", "").lower()
                    or term in r.get("group2", "").lower()
                    or term in r.get("group3", "").lower()
                    or term in " ".join(r.get("groups", [])).lower()
                    or term in r.get("group_role", "").lower()
                    or term in r.get("first_seen_utc", "").lower()
                    or term in r.get("last_seen_utc", "").lower()
                    or term in ("trusted" if r.get("trusted") else "untrusted")
                ):
                    filtered.append(r)
        if filter_term and filter_term != "all":
            if filter_term == "untrusted":
                filtered = [r for r in filtered if not r.get("trusted")]
            else:
                filtered = [
                    r
                    for r in filtered
                    if (
                        (filter_term == "blank" and not any([(r.get("group1") or "").strip(), (r.get("group2") or "").strip(), (r.get("group3") or "").strip()]))
                        or (
                            filter_term != "blank"
                            and filter_term
                            in {
                                (r.get("group1", "") or "").lower(),
                                (r.get("group2", "") or "").lower(),
                                (r.get("group3", "") or "").lower(),
                                *[g.lower() for g in r.get("groups", [])],
                            }
                        )
                    )
                ]
        return filtered

    def _render_rows(self, rows: List[Dict] | None = None):
        if rows is None:
            rows = self._rows
        self.table.setRowCount(0)
        # rebuild group filter options
        groups = set()
        for r in rows:
            row_idx = self.table.rowCount()
            self.table.insertRow(row_idx)

            def set_item(col: int, text: str):
                item = QTableWidgetItem(text)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.table.setItem(row_idx, col, item)

            sel_chk = QCheckBox()
            self.table.setCellWidget(row_idx, self.COL_SELECT, sel_chk)
            set_item(self.COL_CALLSIGN, r["callsign"])
            set_item(self.COL_NAME, r["name"])
            set_item(self.COL_STATE, r["state"])
            set_item(self.COL_GRID, r.get("grid", ""))
            groups_all = [g for g in (r.get("groups") or []) if str(g).strip()]
            groups_first = [g for g in groups_all if g][:3]
            if not groups_first:
                groups_first = [g for g in [(r.get("group1") or "").strip(), (r.get("group2") or "").strip(), (r.get("group3") or "").strip()] if g]
            groups_display = ", ".join(groups_first)
            set_item(self.COL_GROUPS, groups_display)
            set_item(self.COL_G1, r.get("group1", ""))
            set_item(self.COL_G2, r.get("group2", ""))
            set_item(self.COL_G3, r.get("group3", ""))
            set_item(self.COL_ROLE, r.get("group_role", ""))
            first_fmt = _normalize_date_only(r.get("first_seen_utc", "") or "") or ""
            last_fmt = _normalize_date_only(r.get("last_seen_utc", "") or "") or ""
            set_item(self.COL_FIRST_SEEN, first_fmt)
            set_item(self.COL_LAST_SEEN, last_fmt)
            set_item(self.COL_TRUSTED, "Yes" if r.get("trusted") else "No")
            set_item(self.COL_COUNT, str(r["checkin_count"]))
            # Highlight untrusted rows
            if not r.get("trusted"):
                for c in range(self.table.columnCount()):
                    item = self.table.item(row_idx, c)
                    if item:
                        item.setForeground(QColor("#D55E00"))
            gvals = [
                (r.get("group1", "") or "").strip(),
                (r.get("group2", "") or "").strip(),
                (r.get("group3", "") or "").strip(),
            ]
            gvals.extend((r.get("groups") or []))
            if not any(gvals):
                groups.add("Blank")
            else:
                for g in gvals:
                    if g:
                        groups.add(g)
        current = self.group_filter.currentText()
        self.group_filter.blockSignals(True)
        self.group_filter.clear()
        self.group_filter.addItem("All")
        for g in sorted(groups, key=lambda x: x.lower()):
            if g not in ("All", "Untrusted", "Blank"):
                self.group_filter.addItem(g)
        export_groups = sorted([g for g in groups if g not in ("All", "Untrusted", "Blank")], key=lambda x: x.lower())
        self._refresh_export_group_options(export_groups)
        self.group_filter.addItem("Untrusted")
        self.group_filter.addItem("Blank")
        # restore selection if still present
        if current in [self.group_filter.itemText(i) for i in range(self.group_filter.count())]:
            self.group_filter.setCurrentText(current)
        self.group_filter.blockSignals(False)

    # ------------- Qt events ------------- #

    def showEvent(self, event):
        """
        Refresh on show so the operator history is up to date.
        """
        super().showEvent(event)
        if self._nav_refresh_inflight:
            return
        self._set_loading(False)
        self._load_data(show_toast=False)

    def on_tab_activated(self) -> None:
        self._nav_refresh_inflight = True
        try:
            self._load_data(show_toast=True)
        finally:
            self._nav_refresh_inflight = False

    def show_loading_toast(self) -> None:
        self._set_loading(True)

    # ------------- Helpers for selection / DB ops ------------- #

    def _selected_callsigns(self) -> List[str]:
        calls = []
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, self.COL_SELECT)
            if isinstance(w, QCheckBox) and w.isChecked():
                item = self.table.item(r, self.COL_CALLSIGN)
                if item:
                    calls.append(item.text().strip().upper())
        return calls

    def _select_all_rows(self):
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, self.COL_SELECT)
            if isinstance(w, QCheckBox):
                w.setChecked(True)

    def _on_cell_double_clicked(self, row: int, col: int) -> None:
        if col != self.COL_GROUPS:
            return
        item = self.table.item(row, self.COL_CALLSIGN)
        if not item:
            return
        callsign = (item.text() or "").strip().upper()
        if not callsign:
            return
        record = next((r for r in self._rows if (r.get("callsign") or "").strip().upper() == callsign), None)
        if not record:
            return
        groups = [g for g in (record.get("groups") or []) if str(g).strip()]
        if not groups:
            groups = [
                g
                for g in [
                    (record.get("group1") or "").strip(),
                    (record.get("group2") or "").strip(),
                    (record.get("group3") or "").strip(),
                ]
                if g
            ]
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Groups for {callsign}")
        layout = QVBoxLayout(dlg)
        label = QLabel("All Groups:")
        layout.addWidget(label)
        text = QTextEdit()
        text.setReadOnly(True)
        text.setPlainText("\n".join(groups) if groups else "(none)")
        text.setMinimumHeight(220)
        layout.addWidget(text)
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dlg.accept)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)
        dlg.exec()

    def _upsert_record(self, row: Dict, *, merge_groups: bool = True):
        db_path = self._db_path()
        if not db_path:
            QMessageBox.warning(self, "DB Error", "Database path not found.")
            return False
        conn = sqlite3.connect(db_path)
        try:
            self._ensure_schema(conn)
            cur = conn.cursor()
            cs = (row.get("callsign", "") or "").upper()
            existing = cur.execute(
                """
                SELECT name, state, grid, group1, group2, group3, group_role,
                       first_seen_utc, last_seen_utc, last_net, last_role,
                       checkin_count, groups_json, trusted
                FROM operator_checkins WHERE callsign=?
                """,
                (cs,),
            ).fetchone()
            existing_groups: List[str] = []
            existing_trusted = 1
            existing_count = 0
            existing_role = ""
            existing_first = ""
            existing_last = ""
            existing_last_net = ""
            existing_last_role = ""
            existing_name = ""
            existing_state = ""
            existing_grid = ""
            if existing:
                (
                    existing_name,
                    existing_state,
                    existing_grid,
                    eg1,
                    eg2,
                    eg3,
                    existing_role,
                    existing_first,
                    existing_last,
                    existing_last_net,
                    existing_last_role,
                    existing_count,
                    existing_gjson,
                    existing_trusted,
                ) = existing
                try:
                    if existing_gjson:
                        parsed = json.loads(existing_gjson)
                        if isinstance(parsed, list):
                            existing_groups = [str(x) for x in parsed]
                except Exception:
                    existing_groups = []
                if not existing_groups:
                    existing_groups = [eg1 or "", eg2 or "", eg3 or ""]

            csv_name = (row.get("name") or "").strip()
            csv_state = (row.get("state") or "").strip().upper()
            csv_grid = (row.get("grid") or "").strip().upper()
            base_grid = (existing_grid or "").strip().upper()
            if csv_grid and len(csv_grid) >= len(base_grid):
                final_grid = csv_grid
            else:
                final_grid = base_grid

            groups_source = existing_groups if merge_groups else []
            groups, g1, g2, g3 = self._normalize_groups_for_save(row, groups_source)
            trusted = row.get("trusted")
            if trusted is None:
                trusted = 1 if existing_trusted else 0
            if existing:
                first_seen = _normalize_date_only(existing_first)
                last_seen = _normalize_date_only(existing_last)
            else:
                first_seen = _normalize_date_only(row.get("first_seen_utc") or existing_first)
                last_seen = _normalize_date_only(row.get("last_seen_utc") or existing_last)

            cur.execute(
                """
                INSERT OR REPLACE INTO operator_checkins
                    (callsign, name, state, grid, group1, group2, group3, group_role,
                     first_seen_utc, last_seen_utc, last_net, last_role,
                     checkin_count, groups_json, trusted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT checkin_count FROM operator_checkins WHERE callsign=?),0), ?, ?)
                """,
                (
                    cs,
                    csv_name or (existing_name or ""),
                    csv_state or (existing_state or ""),
                    final_grid,
                    g1,
                    g2,
                    g3,
                    row.get("group_role", existing_role or ""),
                    first_seen or last_seen or datetime.datetime.utcnow().strftime("%Y%m%d"),
                    last_seen or datetime.datetime.utcnow().strftime("%Y%m%d"),
                    row.get("last_net", existing_last_net or ""),
                    row.get("last_role", existing_last_role or ""),
                    cs,
                    json.dumps(groups) if groups else None,
                    1 if trusted else 0,
                ),
            )
            conn.commit()
            return True
        except Exception as e:
            log.error("OperatorHistoryTab: upsert failed: %s", e)
            QMessageBox.warning(self, "DB Error", f"Failed to save record:\n{e}")
            return False
        finally:
            conn.close()

    def _show_import_export_menu(self):
        menu = QMenu(self)
        menu.addAction("Import CSV...", self._import_csv)
        menu.addAction("Export CSV...", self._export_csv)
        menu.exec(self.import_btn.mapToGlobal(self.import_btn.rect().bottomLeft()))

    def _show_manage_menu(self):
        menu = QMenu(self)
        menu.addAction("Add Operator...", self._add_operator_dialog)
        menu.addAction("Edit Selected...", self._edit_selected_dialog)
        menu.addAction("Delete Selected...", self._delete_selected)
        menu.exec(self.manage_btn.mapToGlobal(self.manage_btn.rect().bottomLeft()))

    def _export_csv(self):
        selected_groups = self._get_selected_export_groups()
        if not selected_groups:
            QMessageBox.information(
                self,
                "Export CSV",
                "Select 1–3 groups in the Export groups dropdown before exporting.",
            )
            return
        if len(selected_groups) > 3:
            QMessageBox.information(
                self,
                "Export CSV",
                "Select no more than 3 groups for export.",
            )
            return
        selected = set(self._selected_callsigns())
        rows: List[Dict]
        if selected:
            rows = [r for r in self._rows if r.get("callsign") in selected]
        else:
            current_filter = self.group_filter.currentText().strip()
            if not current_filter or current_filter == "All":
                rows = list(self._rows)
            elif current_filter and current_filter != "All":
                rows = self._filtered_rows()
            else:
                rows = list(self._rows)

        if selected_groups:
            def _row_groups(row: Dict) -> set[str]:
                groups = {
                    (row.get("group1") or "").strip(),
                    (row.get("group2") or "").strip(),
                    (row.get("group3") or "").strip(),
                }
                groups.update({str(x).strip() for x in (row.get("groups") or [])})
                return {g for g in groups if g}
            rows = [r for r in rows if _row_groups(r) & set(selected_groups)]
        rows = [
            r
            for r in rows
            if r.get("trusted") and any(
                [
                    (r.get("group1") or "").strip(),
                    (r.get("group2") or "").strip(),
                    (r.get("group3") or "").strip(),
                    *(r.get("groups") or []),
                ]
            )
        ]

        if not rows:
            QMessageBox.information(self, "Export CSV", "No rows to export.")
            return
        callsign = (self.settings.get("operator_callsign", "") or "").strip().upper() or "CALLSIGN"
        date_str = datetime.datetime.utcnow().strftime("%Y%m%d")
        name_parts = [callsign] + [g.replace(" ", "") for g in selected_groups] + [date_str]
        default_name = "_".join([p for p in name_parts if p]) + ".csv"
        fn, _ = QFileDialog.getSaveFileName(self, "Export Operators CSV", default_name, "CSV Files (*.csv)")
        if not fn:
            return
        try:
            with open(fn, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "callsign",
                        "name",
                        "state",
                        "grid",
                        "groups",
                        "group1",
                        "group2",
                        "group3",
                        "group_role",
                        "first_seen_utc",
                        "last_seen_utc",
                        "trusted",
                    ],
                )
                writer.writeheader()
                for r in rows:
                    base_groups = [
                        (r.get("group1") or "").strip(),
                        (r.get("group2") or "").strip(),
                        (r.get("group3") or "").strip(),
                    ]
                    used = {g for g in base_groups if g}
                    export_g1, export_g2, export_g3 = base_groups
                    for g in selected_groups:
                        if g in used:
                            continue
                        if not export_g1:
                            export_g1 = g
                            used.add(g)
                            continue
                        if not export_g2:
                            export_g2 = g
                            used.add(g)
                            continue
                        if not export_g3:
                            export_g3 = g
                            used.add(g)
                            continue
                    writer.writerow(
                        {
                            "callsign": r.get("callsign", ""),
                            "name": r.get("name", ""),
                            "state": r.get("state", ""),
                            "grid": r.get("grid", ""),
                            "groups": ",".join(selected_groups),
                            "group1": export_g1,
                            "group2": export_g2,
                            "group3": export_g3,
                            "group_role": r.get("group_role", ""),
                            "first_seen_utc": r.get("first_seen_utc", ""),
                            "last_seen_utc": r.get("last_seen_utc", ""),
                            "trusted": 1 if r.get("trusted") else 0,
                        }
                    )
        except Exception as e:
            QMessageBox.warning(self, "Export CSV", f"Failed to export:\n{e}")
            return
        QMessageBox.information(self, "Export CSV", f"Exported {len(rows)} record(s).")

    # ------------- CSV import ------------- #

    def _import_csv(self):
        fn, _ = QFileDialog.getOpenFileName(self, "Import Operators CSV", "", "CSV Files (*.csv);;All Files (*)")
        if not fn:
            return
        imported = 0
        skipped = 0
        try:
            with open(fn, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = [h.lower() for h in reader.fieldnames or []]
                required = {"callsign"}
                missing = required - set(headers)
                if missing:
                    QMessageBox.warning(
                        self,
                        "CSV Import",
                        f"Missing required column(s): {', '.join(sorted(missing))}",
                    )
                    return
                for row in reader:
                    lower_row = {k.lower(): (v or "") for k, v in row.items()}
                    cs = lower_row.get("callsign", "").strip().upper()
                    if not cs:
                        skipped += 1
                        continue
                    groups_raw = (lower_row.get("groups") or "").strip()
                    groups_list = [g.strip().upper() for g in groups_raw.split(",") if g.strip()]
                    date_val = (lower_row.get("date added") or lower_row.get("date_added") or "").strip()
                    if not date_val:
                        date_val = datetime.datetime.utcnow().strftime("%Y%m%d")
                    data = {
                        "callsign": cs,
                        "name": lower_row.get("name", "").strip(),
                        "state": lower_row.get("state", "").strip().upper(),
                        "grid": lower_row.get("grid", "").strip().upper(),
                        "group1": lower_row.get("group1", "").strip().upper(),
                        "group2": lower_row.get("group2", "").strip().upper(),
                        "group3": lower_row.get("group3", "").strip().upper(),
                        "groups": groups_list,
                        "group_role": (lower_row.get("group role") or lower_row.get("group_role") or "").strip().upper(),
                        "first_seen_utc": date_val,
                        "last_seen_utc": date_val,
                        "trusted": 1,
                    }
                    if self._upsert_record(data):
                        imported += 1
                    else:
                        skipped += 1
        except Exception as e:
            QMessageBox.warning(self, "CSV Import", f"Failed to import:\n{e}")
            log.error("OperatorHistoryTab: CSV import failed: %s", e)
            return

        self._load_data(show_toast=True)
        self._schedule_history_update()
        QMessageBox.information(self, "CSV Import", f"Imported {imported} record(s). Skipped {skipped}.")

    # ------------- Add / Edit / Delete dialogs ------------- #

    def _collect_dialog_data(self, defaults: Optional[Dict] = None) -> Optional[Dict]:
        defaults = defaults or {}
        dlg = QDialog(self)
        dlg.setWindowTitle("Operator")
        form = QFormLayout(dlg)

        cs_edit = QLineEdit(defaults.get("callsign", ""))
        name_edit = QLineEdit(defaults.get("name", ""))
        state_edit = QLineEdit(defaults.get("state", ""))
        grid_edit = QLineEdit(defaults.get("grid", ""))
        g1_edit = QLineEdit(defaults.get("group1", ""))
        g2_edit = QLineEdit(defaults.get("group2", ""))
        g3_edit = QLineEdit(defaults.get("group3", ""))
        role_edit = QLineEdit(defaults.get("group_role", ""))
        first_edit = QLineEdit(defaults.get("first_seen_utc", ""))
        last_edit = QLineEdit(defaults.get("last_seen_utc", ""))
        date_edit = QLineEdit(defaults.get("date_added", ""))
        date_edit.setVisible(False)
        date_label = QLabel("Date Added (legacy):")
        date_label.setVisible(False)
        trusted_chk = QCheckBox("Trusted")
        trusted_chk.setChecked(bool(defaults.get("trusted", 1)))

        form.addRow("Callsign*:", cs_edit)
        form.addRow("Name:", name_edit)
        form.addRow("State:", state_edit)
        form.addRow("Grid:", grid_edit)
        form.addRow("Group 1:", g1_edit)
        form.addRow("Group 2:", g2_edit)
        form.addRow("Group 3:", g3_edit)
        form.addRow("Group Role:", role_edit)
        form.addRow("First Seen (UTC):", first_edit)
        form.addRow("Last Seen (UTC):", last_edit)
        form.addRow(date_label, date_edit)
        form.addRow("Trusted:", trusted_chk)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        def accept():
            cs = cs_edit.text().strip().upper()
            if not cs:
                QMessageBox.warning(dlg, "Validation", "Callsign is required.")
                return
            dlg.accept()

        ok_btn.clicked.connect(accept)
        cancel_btn.clicked.connect(dlg.reject)

        if dlg.exec() != QDialog.Accepted:
            return None

        return {
            "callsign": cs_edit.text().strip().upper(),
            "name": name_edit.text().strip(),
            "state": state_edit.text().strip().upper(),
            "grid": grid_edit.text().strip().upper(),
            "group1": g1_edit.text().strip(),
            "group2": g2_edit.text().strip(),
            "group3": g3_edit.text().strip(),
            "group_role": role_edit.text().strip(),
            "first_seen_utc": first_edit.text().strip(),
            "last_seen_utc": last_edit.text().strip(),
            "date_added": date_edit.text().strip(),
            "trusted": 1 if trusted_chk.isChecked() else 0,
        }

    def _add_operator_dialog(self):
        data = self._collect_dialog_data()
        if not data:
            return
        if self._upsert_record(data, merge_groups=False):
            self._load_data(show_toast=True)
            self._schedule_history_update()

    def _edit_selected_dialog(self):
        calls = self._selected_callsigns()
        if not calls:
            QMessageBox.information(self, "Edit", "Select a record using the checkbox.")
            return
        if len(calls) > 1:
            resp = QMessageBox.question(
                self,
                "Bulk Edit Trusted",
                "Update Trusted flag for selected operators?\nYes = set Trusted, No = set Untrusted, Cancel = abort.",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
                QMessageBox.Cancel,
            )
            if resp == QMessageBox.Cancel:
                return
            trusted_val = 1 if resp == QMessageBox.Yes else 0
            changed = 0
            for cs in calls:
                if self._upsert_record({"callsign": cs, "trusted": trusted_val}):
                    changed += 1
            if changed:
                self._load_data(show_toast=True)
                self._schedule_history_update()
            return
        # find existing row data
        existing = next((r for r in self._rows if r["callsign"] == calls[0]), None)
        data = self._collect_dialog_data(existing or {"callsign": calls[0]})
        if not data:
            return
        if self._upsert_record(data, merge_groups=False):
            self._load_data(show_toast=True)
            self._schedule_history_update()

    def _delete_selected(self):
        calls = self._selected_callsigns()
        if not calls:
            QMessageBox.information(self, "Delete", "Select records to delete using the checkbox.")
            return
        confirm = QMessageBox.question(
            self,
            "Delete Operators",
            f"Delete {len(calls)} record(s)?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        db_path = self._db_path()
        if not db_path:
            return
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            cur.executemany("DELETE FROM operator_checkins WHERE callsign = ?", [(c,) for c in calls])
            conn.commit()
        except Exception as e:
            log.error("OperatorHistoryTab: delete failed: %s", e)
            QMessageBox.warning(self, "DB Error", f"Delete failed:\n{e}")
        finally:
            conn.close()
        self._load_data(show_toast=True)
        self._schedule_history_update()

    def _schedule_history_update(self) -> None:
        if not self._update_timer.isActive():
            self._update_timer.start()
