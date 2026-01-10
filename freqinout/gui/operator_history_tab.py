from __future__ import annotations

import csv
import datetime
import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QMessageBox,
    QFileDialog,
    QDialog,
    QFormLayout,
    QCheckBox,
    QComboBox,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log


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
    COL_G1 = 5
    COL_G2 = 6
    COL_G3 = 7
    COL_ROLE = 8
    COL_FIRST_SEEN = 9
    COL_LAST_SEEN = 10
    COL_TRUSTED = 11
    COL_COUNT = 12

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._rows: List[Dict] = []

        self._build_ui()
        self._load_data()

    # ------------- UI ------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Operator History</h3>"))
        header.addStretch()
        layout.addLayout(header)

        # Search + actions row
        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter by any column...")
        search_row.addWidget(self.search_edit, stretch=1)

        self.refresh_btn = QPushButton("Refresh")
        search_row.addWidget(self.refresh_btn)
        self.import_btn = QPushButton("Import CSV")
        search_row.addWidget(self.import_btn)
        self.add_btn = QPushButton("Add Operator")
        search_row.addWidget(self.add_btn)
        self.edit_btn = QPushButton("Edit Selected")
        search_row.addWidget(self.edit_btn)
        self.delete_btn = QPushButton("Delete Selected")
        search_row.addWidget(self.delete_btn)
        self.select_all_btn = QPushButton("Select All")
        search_row.addWidget(self.select_all_btn)
        search_row.addWidget(QLabel("Filter by:"))
        self.group_filter = QComboBox()
        self.group_filter.addItem("All")
        self.group_filter.addItem("Untrusted")
        search_row.addWidget(self.group_filter)

        layout.addLayout(search_row)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(13)
        self.table.setHorizontalHeaderLabels(
            [
                "",
                "Callsign",
                "Name",
                "State",
                "Grid",
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

        layout.addWidget(self.table)

        # Signals
        self.refresh_btn.clicked.connect(self._load_data)
        self.search_edit.textChanged.connect(self._apply_filter)
        self.import_btn.clicked.connect(self._import_csv)
        self.add_btn.clicked.connect(self._add_operator_dialog)
        self.edit_btn.clicked.connect(self._edit_selected_dialog)
        self.delete_btn.clicked.connect(self._delete_selected)
        self.select_all_btn.clicked.connect(self._select_all_rows)
        self.group_filter.currentTextChanged.connect(self._apply_filter)

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

    def _load_data(self):
        """
        Load operator_checkins table into self._rows.
        """
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            self._rows = []
            self._render_rows()
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

        def parse_ts(line: str) -> Optional[str]:
            ts_str = line[:19]
            try:
                dt = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
                return dt.isoformat()
            except Exception:
                return None

        earliest: Dict[str, str] = {}
        try:
            text = directed.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            text = ""
        for line in text.splitlines():
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

        if all_txt and all_txt.exists():
            try:
                text = all_txt.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                text = ""
            for line in text.splitlines():
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
        term = self.search_edit.text().strip().lower()
        filter_term = self.group_filter.currentText().strip().lower()
        if not term:
            filtered = self._rows
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
        self._render_rows(filtered)

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
                        item.setBackground(QColor("#ffe5e5"))
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
        self._load_data()

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

    def _upsert_record(self, row: Dict):
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

            groups, g1, g2, g3 = self._normalize_groups_for_save(row, existing_groups)
            trusted = row.get("trusted")
            if trusted is None:
                trusted = 1 if existing_trusted else 0
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
                    row.get("name", existing_name or ""),
                    row.get("state", existing_state or ""),
                    row.get("grid", "") or "",
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

        self._load_data()
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
        if self._upsert_record(data):
            self._load_data()

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
                self._load_data()
            return
        # find existing row data
        existing = next((r for r in self._rows if r["callsign"] == calls[0]), None)
        data = self._collect_dialog_data(existing or {"callsign": calls[0]})
        if not data:
            return
        if self._upsert_record(data):
            self._load_data()

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
        self._load_data()
