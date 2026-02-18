from __future__ import annotations

import csv
import datetime
import json
import sqlite3
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from PySide6.QtCore import Qt, Signal, QTimer, QRect, QPoint
from PySide6.QtGui import QColor, QPainter, QCursor
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QLineEdit,
    QPushButton,
    QMenu,
    QTableWidget,
    QTableWidgetItem,
    QMessageBox,
    QTextEdit,
    QFileDialog,
    QDialog,
    QFormLayout,
    QCheckBox,
    QComboBox,
    QWidgetAction,
    QHeaderView,
    QStyle,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.core.perf_metrics import span as perf_span
from freqinout.core.varac_ingest import ingest_varac
from freqinout.gui.theme import resolve_theme, button_style

ALLOWED_GROUP_ROLES = {"", "HUB", "HUB-ALT", "NCS", "ANCS", "PEER"}
GROUP_ROLE_OPTIONS = ["", "HUB", "HUB-ALT", "NCS", "ANCS", "PEER"]


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


class OperatorHeaderWithCheckbox(QHeaderView):
    checkboxToggled = Signal(int)

    def __init__(self, orientation: Qt.Orientation, parent=None):
        super().__init__(orientation, parent)
        self._checkbox_state = Qt.Unchecked
        self._checkbox_enabled = False
        self._cb_bg = QColor("#ffffff")
        self._cb_border = QColor("#777777")
        self._cb_accent = QColor("#2d8cf0")
        self._cb_mark = QColor("#ffffff")
        self.setSectionsClickable(True)

    def set_checkbox_state(self, state: Qt.CheckState, enabled: Optional[bool] = None) -> None:
        if enabled is not None:
            self._checkbox_enabled = bool(enabled)
        self._checkbox_state = state
        self.updateSection(0)

    def set_checkbox_colors(
        self, *, bg: QColor, border: QColor, accent: QColor, mark: QColor
    ) -> None:
        self._cb_bg = bg
        self._cb_border = border
        self._cb_accent = accent
        self._cb_mark = mark
        self.updateSection(0)

    def _checkbox_rect(self, rect: QRect) -> QRect:
        style = self.style()
        width = style.pixelMetric(QStyle.PM_IndicatorWidth)
        height = style.pixelMetric(QStyle.PM_IndicatorHeight)
        x = rect.x() + 4
        y = rect.y() + (rect.height() - height) // 2
        return QRect(x, y, width, height)

    def paintSection(self, painter: QPainter, rect: QRect, logicalIndex: int) -> None:
        super().paintSection(painter, rect, logicalIndex)
        if logicalIndex != 0:
            return
        box = self._checkbox_rect(rect)
        border = self._cb_accent if self._checkbox_enabled else self._cb_border
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setPen(border)
        painter.setBrush(self._cb_bg)
        painter.drawRoundedRect(box.adjusted(0, 0, -1, -1), 2, 2)
        if self._checkbox_state in (Qt.Checked, Qt.PartiallyChecked):
            inner = box.adjusted(3, 3, -3, -3)
            painter.setBrush(self._cb_accent)
            painter.setPen(Qt.NoPen)
            painter.drawRoundedRect(inner, 1, 1)
            painter.setPen(self._cb_mark)
            if self._checkbox_state == Qt.PartiallyChecked:
                y = inner.center().y()
                painter.drawLine(inner.left() + 2, y, inner.right() - 2, y)
            else:
                x1 = inner.left() + 2
                y1 = inner.center().y()
                x2 = inner.center().x()
                y2 = inner.bottom() - 2
                x3 = inner.right() - 2
                y3 = inner.top() + 2
                painter.drawLine(x1, y1, x2, y2)
                painter.drawLine(x2, y2, x3, y3)
        painter.restore()

    def mousePressEvent(self, event) -> None:
        if self._checkbox_enabled:
            idx = self.logicalIndexAt(event.pos())
            if idx == 0:
                rect = QRect(
                    self.sectionViewportPosition(0),
                    0,
                    self.sectionSize(0),
                    self.height(),
                )
                if self._checkbox_rect(rect).contains(event.pos()):
                    if self._checkbox_state == Qt.Checked:
                        self._checkbox_state = Qt.Unchecked
                    else:
                        self._checkbox_state = Qt.Checked
                    self.updateSection(0)
                    self.checkboxToggled.emit(int(self._checkbox_state.value))
                    return
        super().mousePressEvent(event)


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
    COL_SITREP = 2
    COL_NAME = 3
    COL_STATE = 4
    COL_GRID = 5
    COL_GROUPS = 6
    COL_G1 = 7
    COL_G2 = 8
    COL_G3 = 9
    COL_ROLE = 10
    COL_FIRST_SEEN = 11
    COL_LAST_SEEN = 12
    COL_TRUSTED = 13
    COL_COUNT = 14

    operator_history_updated = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._rows: List[Dict] = []
        self._export_group_checks: Dict[str, QCheckBox] = {}
        self.loading_label: QLabel | None = None
        self._loading_progress: QProgressBar | None = None
        self._nav_refresh_inflight = False
        self._bulk_select_inflight = False
        self._last_load_ts: float = 0.0
        try:
            self._load_min_interval_sec = float(
                self.settings.get("operator_refresh_min_interval_sec", 60.0) or 60.0
            )
        except Exception:
            self._load_min_interval_sec = 60.0
        if self._load_min_interval_sec < 10.0:
            self._load_min_interval_sec = 10.0
        self._rows_fingerprint: Optional[Tuple] = None
        self._last_varac_ingest_ts: float = 0.0
        self._varac_ingest_interval_sec: float = 60.0
        self._last_backfill_ts: float = 0.0
        self._backfill_interval_sec: float = 600.0
        self._update_timer = QTimer(self)
        self._update_timer.setSingleShot(True)
        self._update_timer.setInterval(300)
        self._update_timer.timeout.connect(self.operator_history_updated.emit)

        self._build_ui()

    # ------------- UI ------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Operator History</h3>"))
        header.addSpacing(8)
        self.loading_label = QLabel("Brewing it fresh...")
        self.loading_label.setStyleSheet("color: #888;")
        self.loading_label.setVisible(False)
        header.addWidget(self.loading_label)
        self._loading_progress = QProgressBar()
        self._loading_progress.setRange(0, 0)
        self._loading_progress.setFixedWidth(140)
        self._loading_progress.setVisible(False)
        header.addWidget(self._loading_progress)
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
        self.clear_filters_btn = QPushButton("Clear Filters")
        search_row.addWidget(self.clear_filters_btn)
        self.manage_btn = QPushButton("Manage Operators")
        search_row.addWidget(self.manage_btn)
        self.export_group_btn = QPushButton("Export by Group")
        self.export_group_btn.setMinimumWidth(180)
        search_row.addWidget(self.export_group_btn)
        self.import_btn = QPushButton("Import/Export...")
        search_row.addWidget(self.import_btn)

        layout.addLayout(search_row)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(15)
        self.table.setHorizontalHeaderLabels(
            [
                "",
                "Callsign",
                "SitRep",
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
        self.table.setSortingEnabled(True)
        hv = OperatorHeaderWithCheckbox(Qt.Horizontal, self.table)
        self.table.setHorizontalHeader(hv)
        hv.setSectionResizeMode(self.COL_SELECT, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_SITREP, QHeaderView.ResizeToContents)
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
        hv.resizeSection(self.COL_SITREP, 68)
        for col in (self.COL_G1, self.COL_G2, self.COL_G3):
            self.table.setColumnHidden(col, True)
        hv.checkboxToggled.connect(self._on_header_checkbox_toggled)

        layout.addWidget(self.table)

        # Signals
        self.search_edit.textChanged.connect(self._apply_filter)
        self.clear_filters_btn.clicked.connect(self._clear_filters)
        self.import_btn.clicked.connect(self._show_import_export_menu)
        self.manage_btn.clicked.connect(self._show_manage_menu)
        self.group_filter.currentTextChanged.connect(self._apply_filter)
        self.table.itemChanged.connect(self._on_table_item_changed)
        self.table.cellClicked.connect(self._on_cell_clicked)
        self.table.cellDoubleClicked.connect(self._on_cell_double_clicked)
        self._wire_export_group_menu()
        self._update_clear_filters_button_style()
        self._update_action_button_styles()

    def _clear_filters(self) -> None:
        self.search_edit.clear()
        self.group_filter.setCurrentText("All")
        self._apply_filter()

    def _normalize_group_role(self, value: Optional[str]) -> str:
        role = (value or "").strip().upper()
        return role if role in ALLOWED_GROUP_ROLES else ""

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
                trusted INTEGER DEFAULT 0
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
                    trusted INTEGER DEFAULT 0
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
                        COALESCE(trusted,0)
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
            ("trusted", "INTEGER DEFAULT 0"),
            ("groups_json", "TEXT"),
            ("first_seen_utc", "TEXT"),
            ("last_seen_utc", "TEXT"),
            ("last_net", "TEXT"),
            ("last_role", "TEXT"),
        ):
            if missing_col not in cols:
                cur.execute(f"ALTER TABLE operator_checkins ADD COLUMN {missing_col} {ddl}")

        # Backfill trusted to 0 and hydrate groups_json; also seed first_seen if missing.
        cur.execute("UPDATE operator_checkins SET trusted=0 WHERE trusted IS NULL")
        # Standardize group roles and clear unknown values.
        cur.execute(
            """
            UPDATE operator_checkins
               SET group_role = CASE
                    WHEN TRIM(UPPER(COALESCE(group_role, ''))) IN ('HUB','HUB-ALT','NCS','ANCS','PEER')
                        THEN TRIM(UPPER(COALESCE(group_role, '')))
                    ELSE ''
                  END
            """
        )
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

    def _ensure_sitrep_status_schema(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS spotter_station_status (
                from_call TEXT PRIMARY KEY,
                form_id TEXT NOT NULL,
                status_key TEXT NOT NULL,
                status_label TEXT NOT NULL,
                response_code TEXT,
                updated_utc_ts REAL NOT NULL DEFAULT 0,
                updated_utc_str TEXT,
                raw_text TEXT,
                updated_ingested_ts REAL,
                status_source TEXT,
                status_source_detail TEXT
            )
            """
        )
        for col_name, col_ddl in (
            ("status_source", "TEXT"),
            ("status_source_detail", "TEXT"),
        ):
            try:
                cur.execute(f"ALTER TABLE spotter_station_status ADD COLUMN {col_name} {col_ddl}")
            except Exception:
                pass
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_spotter_status_key_ts ON spotter_station_status(status_key, updated_utc_ts DESC)"
        )
        conn.commit()

    @staticmethod
    def _settings_bool(settings: SettingsManager, key: str, default: bool) -> bool:
        try:
            value = settings.get(key, default)
        except Exception:
            value = default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        txt = str(value or "").strip().lower()
        if txt in {"1", "true", "yes", "on", "enabled"}:
            return True
        if txt in {"0", "false", "no", "off", "disabled"}:
            return False
        return bool(default)

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            return float(value or 0.0)
        except Exception:
            return float(default)

    @staticmethod
    def _decode_source_summary(value: object) -> Dict[str, str]:
        txt = str(value or "").strip()
        if not txt:
            return {}
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                out: Dict[str, str] = {}
                for raw_src, raw_status in obj.items():
                    src = str(raw_src or "").strip().upper()
                    status = str(raw_status or "").strip().lower()
                    if not src:
                        continue
                    if status not in {"red", "yellow", "green", "unknown", "not_reported"}:
                        status = "unknown"
                    out[src] = status
                return out
        except Exception:
            pass
        return {}

    @staticmethod
    def _source_short_label(source: str) -> str:
        src = (source or "").strip().upper()
        if not src:
            return "UNK"
        if src == "JS8SPOTTER":
            return "SPT"
        if src == "COMMSTAT3":
            return "CS3"
        if src == "COMMSTAT23":
            return "CS2"
        if src == "MANUAL":
            return "MAN"
        if len(src) <= 4:
            return src
        return src[:4]

    @classmethod
    def _encode_source_chips(cls, source_summary: Dict[str, str]) -> str:
        if not source_summary:
            return ""
        parts = []
        for source in sorted(source_summary.keys()):
            key = str(source_summary.get(source) or "unknown").strip().lower()
            chip = cls._sitrep_status_chip(key)
            parts.append(f"{cls._source_short_label(source)}:{chip}")
        return " ".join(parts)

    @staticmethod
    def _sitrep_conflict(source_summary: Dict[str, str]) -> bool:
        vals = {
            str(v or "").strip().lower()
            for v in source_summary.values()
            if str(v or "").strip().lower() in {"red", "yellow", "green", "unknown"}
        }
        return len(vals) > 1

    @staticmethod
    def _utc_from_epoch(ts: float) -> str:
        if ts <= 0:
            return ""
        return datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _age_text_from_epoch(ts: float) -> str:
        if ts <= 0:
            return ""
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        age = max(0, int(now - ts))
        if age < 60:
            return f"{age}s"
        mins, sec = divmod(age, 60)
        if mins < 60:
            return f"{mins}m {sec}s"
        hrs, mins = divmod(mins, 60)
        if hrs < 24:
            return f"{hrs}h {mins}m"
        days, hrs = divmod(hrs, 24)
        return f"{days}d {hrs}h"

    def _sitrep_display_text(self, row: Dict) -> str:
        key = (row.get("sitrep_status_key") or "unknown").strip().lower()
        base = self._sitrep_status_chip(key)
        if bool(row.get("sitrep_conflict")):
            return f"{base}!"
        try:
            source_count = int(row.get("sitrep_source_count") or 0)
        except Exception:
            source_count = 0
        if source_count > 1:
            return f"{base}+"
        return base

    @staticmethod
    def _sitrep_status_label(status_key: str) -> str:
        key = (status_key or "").strip().lower()
        if key == "red":
            return "Not Functioning"
        if key == "yellow":
            return "Partially Functioning"
        if key == "green":
            return "Functioning"
        return "Unknown"

    @staticmethod
    def _sitrep_status_chip(status_key: str) -> str:
        key = (status_key or "").strip().lower()
        if key == "red":
            return "R"
        if key == "yellow":
            return "Y"
        if key == "green":
            return "G"
        return "?"

    def _load_sitrep_status_map(self, cur: sqlite3.Cursor) -> Dict[str, Dict[str, str]]:
        out: Dict[str, Dict[str, str]] = {}
        # Legacy status table (includes manual overrides authored from Operators tab).
        try:
            cur.execute(
                """
                SELECT from_call, status_key, status_label, updated_utc_ts, updated_utc_str, status_source
                FROM spotter_station_status
                """
            )
        except Exception:
            return out
        for from_call, status_key, status_label, updated_utc_ts, updated_utc_str, status_source in cur.fetchall():
            cs = (from_call or "").strip().upper()
            if not cs:
                continue
            key = (status_key or "").strip().lower()
            if key not in {"red", "yellow", "green", "unknown"}:
                key = "unknown"
            label = (status_label or "").strip() or self._sitrep_status_label(key)
            source = (status_source or "").strip().upper() or "UNKNOWN"
            ts_txt = (updated_utc_str or "").strip()
            ts_val = self._safe_float(updated_utc_ts, 0.0)
            if not ts_txt:
                if ts_val > 0:
                    ts_txt = self._utc_from_epoch(ts_val)
            summary = {source: key} if source else {}
            out[cs] = {
                "key": key,
                "label": label,
                "source": source,
                "updated": ts_txt,
                "updated_ts": ts_val,
                "source_summary": summary,
                "source_chips": self._encode_source_chips(summary),
                "conflict": False,
                "source_count": 1 if source else 0,
                "age": self._age_text_from_epoch(ts_val),
            }

        # Unified fused projection (effective status + per-source summary), behind a feature flag.
        if not self._settings_bool(self.settings, "sitrep_unified_operators_enabled", True):
            return out

        try:
            cur.execute(
                """
                SELECT
                    callsign,
                    effective_status,
                    latest_event_ts,
                    latest_event_ts_utc,
                    source_summary_json
                FROM sitrep_latest_by_callsign
                """
            )
            fused_rows = cur.fetchall()
        except Exception:
            fused_rows = []

        for callsign, effective_status, latest_event_ts, latest_event_ts_utc, source_summary_json in fused_rows:
            cs = (callsign or "").strip().upper()
            if not cs:
                continue
            key = str(effective_status or "").strip().lower()
            if key not in {"red", "yellow", "green", "unknown", "not_reported"}:
                key = "unknown"
            if key == "not_reported":
                key = "unknown"
            ts_val = self._safe_float(latest_event_ts, 0.0)
            ts_txt = (latest_event_ts_utc or "").strip() or self._utc_from_epoch(ts_val)
            summary = self._decode_source_summary(source_summary_json)
            if not summary:
                summary = {"FUSED": key}
            source_count = len(summary)
            if source_count <= 1:
                source = next(iter(summary.keys()), "FUSED")
            else:
                source = f"MULTI({source_count})"
            candidate = {
                "key": key,
                "label": self._sitrep_status_label(key),
                "source": source,
                "updated": ts_txt,
                "updated_ts": ts_val,
                "source_summary": summary,
                "source_chips": self._encode_source_chips(summary),
                "conflict": self._sitrep_conflict(summary),
                "source_count": source_count,
                "age": self._age_text_from_epoch(ts_val),
            }
            existing = out.get(cs)
            if not existing:
                out[cs] = candidate
                continue
            existing_src = str(existing.get("source") or "").strip().upper()
            existing_ts = self._safe_float(existing.get("updated_ts"), 0.0)
            # Keep manual status until a newer fused report is available.
            if existing_src == "MANUAL" and existing_ts >= ts_val:
                continue
            if ts_val >= existing_ts:
                out[cs] = candidate
        return out

    def _sitrep_tooltip(self, row: Dict) -> str:
        key = (row.get("sitrep_status_key") or "unknown").strip().lower()
        label = (row.get("sitrep_status_label") or self._sitrep_status_label(key)).strip()
        src = (row.get("sitrep_status_source") or "UNKNOWN").strip().upper()
        updated = (row.get("sitrep_status_updated") or "").strip()
        chips = (row.get("sitrep_source_chips") or "").strip()
        age = (row.get("sitrep_status_age") or "").strip()
        conflict = bool(row.get("sitrep_conflict"))
        lines = [f"SitRep: {label}", f"Source: {src}"]
        if updated:
            lines.append(f"Updated: {updated} UTC")
        if age:
            lines.append(f"Age: {age}")
        if chips:
            lines.append(f"Sources: {chips}")
        if conflict:
            lines.append("Conflict: sources disagree")
        return "\n".join(lines)

    def _apply_sitrep_button_style(self, btn: QPushButton, status_key: str) -> None:
        key = (status_key or "").strip().lower()
        if key == "red":
            bg = "#D32F2F"
            fg = "#FFFFFF"
            border = "#8E0000"
        elif key == "yellow":
            bg = "#FBC02D"
            fg = "#111111"
            border = "#8D6E00"
        elif key == "green":
            bg = "#43A047"
            fg = "#FFFFFF"
            border = "#1B5E20"
        else:
            bg = "#4FC3F7"
            fg = "#111111"
            border = "#1976D2"
        btn.setStyleSheet(
            f"QPushButton {{ background: {bg}; color: {fg}; border: 1px solid {border}; border-radius: 9px; padding: 1px 8px; font-weight: 700; }}"
            f"QPushButton:hover {{ border: 1px solid {fg}; }}"
        )

    def _apply_sitrep_item_style(self, item: QTableWidgetItem, status_key: str) -> None:
        key = (status_key or "").strip().lower()
        if key == "red":
            bg = QColor("#D32F2F")
            fg = QColor("#FFFFFF")
        elif key == "yellow":
            bg = QColor("#FBC02D")
            fg = QColor("#111111")
        elif key == "green":
            bg = QColor("#43A047")
            fg = QColor("#FFFFFF")
        else:
            bg = QColor("#4FC3F7")
            fg = QColor("#111111")
        item.setBackground(bg)
        item.setForeground(fg)

    def _show_sitrep_status_menu(self, callsign: str, anchor_or_pos=None) -> None:
        cs = (callsign or "").strip().upper()
        if not cs:
            return
        if isinstance(anchor_or_pos, QPoint):
            global_pos = anchor_or_pos
        elif isinstance(anchor_or_pos, QPushButton):
            global_pos = anchor_or_pos.mapToGlobal(anchor_or_pos.rect().bottomLeft())
        else:
            global_pos = QCursor.pos()
        menu = QMenu(self)
        options = [
            ("Green", "green"),
            ("Yellow", "yellow"),
            ("Red", "red"),
            ("Unknown", "unknown"),
        ]
        actions = {}
        for label, key in options:
            act = menu.addAction(label)
            actions[act] = key
        chosen = menu.exec(global_pos)
        if not chosen:
            return
        key = actions.get(chosen, "")
        if not key:
            return
        self._set_manual_sitrep_status(cs, key)

    def _set_manual_sitrep_status(self, callsign: str, status_key: str) -> None:
        cs = (callsign or "").strip().upper()
        key = (status_key or "").strip().lower()
        if not cs or key not in {"red", "yellow", "green", "unknown"}:
            return
        db_path = self._db_path()
        if not db_path:
            return
        now = datetime.datetime.now(datetime.timezone.utc)
        now_ts = float(now.timestamp())
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        label = self._sitrep_status_label(key)
        try:
            conn = sqlite3.connect(db_path)
            self._ensure_sitrep_status_schema(conn)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO spotter_station_status
                    (from_call, form_id, status_key, status_label, response_code, updated_utc_ts, updated_utc_str,
                     raw_text, updated_ingested_ts, status_source, status_source_detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(from_call) DO UPDATE SET
                    form_id=excluded.form_id,
                    status_key=excluded.status_key,
                    status_label=excluded.status_label,
                    response_code=excluded.response_code,
                    updated_utc_ts=excluded.updated_utc_ts,
                    updated_utc_str=excluded.updated_utc_str,
                    raw_text=excluded.raw_text,
                    updated_ingested_ts=excluded.updated_ingested_ts,
                    status_source=excluded.status_source,
                    status_source_detail=excluded.status_source_detail
                WHERE (
                    excluded.updated_utc_ts > COALESCE(spotter_station_status.updated_utc_ts, 0)
                    OR (
                        excluded.updated_utc_ts = COALESCE(spotter_station_status.updated_utc_ts, 0)
                        AND excluded.updated_ingested_ts >= COALESCE(spotter_station_status.updated_ingested_ts, 0)
                    )
                )
                """,
                (
                    cs,
                    "MANUAL",
                    key,
                    label,
                    "",
                    now_ts,
                    now_str,
                    f"MANUAL:{key}",
                    now_ts,
                    "MANUAL",
                    "Operators Tab",
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.error("OperatorHistoryTab: failed to update SitRep status for %s: %s", cs, e)
            QMessageBox.warning(self, "SitRep Update", f"Failed to update SitRep status for {cs}.\n{e}")
            return

        updated_row: Optional[Dict] = None
        for row in self._rows:
            if (row.get("callsign") or "").strip().upper() != cs:
                continue
            row["sitrep_status_key"] = key
            row["sitrep_status_label"] = label
            row["sitrep_status_source"] = "MANUAL"
            row["sitrep_status_updated"] = now_str
            row["sitrep_status_age"] = "0s"
            row["sitrep_conflict"] = False
            row["sitrep_source_count"] = 1
            row["sitrep_source_chips"] = "MAN:{}".format(self._sitrep_status_chip(key))
            updated_row = row
            break
        if self._rows:
            self._rows_fingerprint = self._build_rows_fingerprint(self._rows)
        search_term = self.search_edit.text().strip().lower()
        if search_term:
            # Active search can match sitrep fields, so preserve full filter behavior.
            self._apply_filter()
        else:
            self._update_visible_sitrep_cell(cs, key, updated_row)
        self._schedule_history_update()

    def _update_visible_sitrep_cell(self, callsign: str, status_key: str, row_data: Optional[Dict]) -> None:
        cs = (callsign or "").strip().upper()
        if not cs:
            return
        for r in range(self.table.rowCount()):
            call_item = self.table.item(r, self.COL_CALLSIGN)
            if not call_item:
                continue
            if (call_item.text() or "").strip().upper() != cs:
                continue
            sitrep_item = self.table.item(r, self.COL_SITREP)
            if sitrep_item is None:
                continue
            sitrep_item.setText(self._sitrep_display_text(row_data or {"sitrep_status_key": status_key}))
            self._apply_sitrep_item_style(sitrep_item, status_key)
            if row_data is not None:
                sitrep_item.setToolTip(self._sitrep_tooltip(row_data))
            break

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
        with perf_span(
            "operators.load_data",
            settings=self.settings,
            meta={"show_toast": bool(show_toast)},
            min_ms=5.0,
        ):
            if show_toast:
                self._set_loading(True)
            else:
                self._set_loading(False)
            now = time.time()
            if now - float(self._last_varac_ingest_ts) >= self._varac_ingest_interval_sec:
                try:
                    ingest_varac(self.settings)
                    self._last_varac_ingest_ts = now
                except Exception:
                    pass
            db_path = self._db_path()
            if not db_path or not db_path.exists():
                self._rows = []
                empty_fp: Tuple = ()
                if self._rows_fingerprint != empty_fp or self.table.rowCount() > 0:
                    self._rows_fingerprint = empty_fp
                    self._render_rows()
                if show_toast:
                    self._set_loading(False)
                return

            # One-time backfill: hydrate first_seen_utc from DIRECTED/ALL logs if earlier than stored
            if now - float(self._last_backfill_ts) >= self._backfill_interval_sec:
                self._backfill_first_seen_from_logs()
                self._last_backfill_ts = now

            rows: List[Dict] = []
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                self._ensure_schema(conn)
                self._ensure_sitrep_status_schema(conn)
                sitrep_map = self._load_sitrep_status_map(cur)
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
                        COALESCE(trusted,0)
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
                            "group_role": self._normalize_group_role(role),
                            "first_seen_utc": _normalize_date_only(first_seen) or (first_seen or "").strip(),
                            "last_seen_utc": _normalize_date_only(last_seen) or (last_seen or "").strip(),
                            "checkin_count": int(count or 0),
                            "trusted": 1 if int(trusted or 0) else 0,
                            "sitrep_status_key": sitrep_map.get((cs or "").strip().upper(), {}).get("key", "unknown"),
                            "sitrep_status_label": sitrep_map.get((cs or "").strip().upper(), {}).get("label", "Unknown"),
                            "sitrep_status_source": sitrep_map.get((cs or "").strip().upper(), {}).get("source", "UNKNOWN"),
                            "sitrep_status_updated": sitrep_map.get((cs or "").strip().upper(), {}).get("updated", ""),
                            "sitrep_status_age": sitrep_map.get((cs or "").strip().upper(), {}).get("age", ""),
                            "sitrep_conflict": bool(sitrep_map.get((cs or "").strip().upper(), {}).get("conflict", False)),
                            "sitrep_source_count": int(sitrep_map.get((cs or "").strip().upper(), {}).get("source_count", 0) or 0),
                            "sitrep_source_chips": sitrep_map.get((cs or "").strip().upper(), {}).get("source_chips", ""),
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

            next_fp = self._build_rows_fingerprint(rows)
            if self._rows_fingerprint is not None and next_fp == self._rows_fingerprint:
                # Data unchanged: skip expensive table/widget rebuild.
                self._rows = rows
                self._last_load_ts = time.time()
                if show_toast:
                    self._set_loading(False)
                return

            self._rows_fingerprint = next_fp
            self._rows = rows
            self._apply_filter()
            self._last_load_ts = time.time()
            if show_toast:
                self._set_loading(False)

    @staticmethod
    def _build_rows_fingerprint(rows: List[Dict]) -> Tuple:
        def _norm(value: object) -> str:
            return str(value or "").strip().upper()

        out: List[Tuple] = []
        for row in rows:
            groups = tuple(_norm(g) for g in (row.get("groups") or []) if _norm(g))
            out.append(
                (
                    _norm(row.get("callsign")),
                    _norm(row.get("name")),
                    _norm(row.get("state")),
                    _norm(row.get("grid")),
                    _norm(row.get("group1")),
                    _norm(row.get("group2")),
                    _norm(row.get("group3")),
                    _norm(row.get("group_role")),
                    _norm(row.get("first_seen_utc")),
                    _norm(row.get("last_seen_utc")),
                    int(row.get("checkin_count") or 0),
                    int(row.get("trusted") or 0),
                    _norm(row.get("sitrep_status_key")),
                    _norm(row.get("sitrep_status_label")),
                    _norm(row.get("sitrep_status_source")),
                    _norm(row.get("sitrep_status_updated")),
                    _norm(row.get("sitrep_status_age")),
                    1 if bool(row.get("sitrep_conflict")) else 0,
                    int(row.get("sitrep_source_count") or 0),
                    _norm(row.get("sitrep_source_chips")),
                    groups,
                )
            )
        return tuple(out)

    def _set_loading(self, active: bool, text: str = "Brewing it fresh...") -> None:
        if not self.loading_label:
            return
        self.loading_label.setText(text)
        self.loading_label.setVisible(bool(active))
        if self._loading_progress is not None:
            self._loading_progress.setVisible(bool(active))

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
                last_pos = fh.tell()
                while True:
                    line = fh.readline()
                    if not line:
                        break
                    last_pos = fh.tell()
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
                    self.settings.set("operator_backfill_directed_offset", int(last_pos))
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
                    last_pos = fh.tell()
                    while True:
                        line = fh.readline()
                        if not line:
                            break
                        last_pos = fh.tell()
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
                        self.settings.set("operator_backfill_all_offset", int(last_pos))
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
        self._update_bulk_select_controls()
        self._update_clear_filters_button_style()
        self._update_action_button_styles()

    def _is_filter_active(self) -> bool:
        term = self.search_edit.text().strip()
        filt = self.group_filter.currentText().strip().lower()
        return bool(term) or (filt and filt != "all")

    def _update_clear_filters_button_style(self) -> None:
        theme = resolve_theme(self.settings)
        role = "eligible_warning" if self._is_filter_active() else "muted"
        self.clear_filters_btn.setStyleSheet(button_style(role, theme))

    def _selected_row_count(self) -> int:
        selected = 0
        for r in range(self.table.rowCount()):
            item = self.table.item(r, self.COL_SELECT)
            if item and item.checkState() == Qt.Checked:
                selected += 1
        return selected

    def _update_action_button_styles(self, theme: Optional[Dict[str, str]] = None) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        selected_count = self._selected_row_count()
        has_export_groups = bool(self._get_selected_export_groups())
        self.manage_btn.setStyleSheet(button_style("eligible_info" if selected_count > 0 else "muted", theme))
        self.import_btn.setStyleSheet(button_style("muted", theme))
        self.export_group_btn.setStyleSheet(button_style("eligible_info" if has_export_groups else "muted", theme))

    def _update_bulk_select_controls(self) -> None:
        self._sync_header_checkbox()

    def _sync_header_checkbox(self) -> None:
        header = self.table.horizontalHeader()
        if not isinstance(header, OperatorHeaderWithCheckbox):
            return
        total = self.table.rowCount()
        enabled = self._is_filter_active() and total > 0
        if total <= 0:
            header.set_checkbox_state(Qt.Unchecked, enabled=False)
            return
        selected = 0
        for r in range(total):
            item = self.table.item(r, self.COL_SELECT)
            if item and item.checkState() == Qt.Checked:
                selected += 1
        if selected == 0:
            header.set_checkbox_state(Qt.Unchecked, enabled=enabled)
        elif selected == total:
            header.set_checkbox_state(Qt.Checked, enabled=enabled)
        else:
            header.set_checkbox_state(Qt.PartiallyChecked, enabled=enabled)

    def _on_header_checkbox_toggled(self, state: int) -> None:
        if not self._is_filter_active():
            self._sync_header_checkbox()
            return
        state_val = int(getattr(state, "value", state))
        if state_val == Qt.PartiallyChecked.value:
            return
        self._select_filtered_rows(state_val == Qt.Checked.value)

    def _select_filtered_rows(self, selected: bool) -> None:
        if self._bulk_select_inflight:
            return
        self._bulk_select_inflight = True
        try:
            for r in range(self.table.rowCount()):
                item = self.table.item(r, self.COL_SELECT)
                if item is not None:
                    item.setCheckState(Qt.Checked if selected else Qt.Unchecked)
        finally:
            self._bulk_select_inflight = False
        self._sync_header_checkbox()

    def _wire_export_group_menu(self) -> None:
        menu = QMenu(self.export_group_btn)
        self.export_group_btn.setMenu(menu)
        self._export_group_checks = {}

    def _refresh_export_group_options(self, groups: List[str]) -> None:
        prior = set(self._get_selected_export_groups())
        menu = self.export_group_btn.menu()
        if menu is None:
            menu = QMenu(self.export_group_btn)
            self.export_group_btn.setMenu(menu)
        menu.clear()
        self._export_group_checks = {}

        hint_action = menu.addAction("Select groups...")
        hint_action.setEnabled(False)

        if not groups:
            empty_action = menu.addAction("(no groups available)")
            empty_action.setEnabled(False)
            self._update_export_group_placeholder()
            return

        for g in groups:
            cb = QCheckBox(g)
            cb.setChecked(g in prior)
            cb.stateChanged.connect(self._on_export_group_checkbox_changed)
            action = QWidgetAction(menu)
            action.setDefaultWidget(cb)
            menu.addAction(action)
            self._export_group_checks[g] = cb
        self._update_export_group_placeholder()

    def _on_export_group_checkbox_changed(self, _state: int) -> None:
        self._update_export_group_placeholder()

    def _get_selected_export_groups(self) -> List[str]:
        selected = [g for g, cb in self._export_group_checks.items() if cb.isChecked()]
        return [g for g in selected if g.strip()]

    def _update_export_group_placeholder(self) -> None:
        selected = self._get_selected_export_groups()
        label = f"{len(selected)} Selected" if selected else "Export by Group"
        self.export_group_btn.setText(label)
        self._update_action_button_styles()

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
                    or term in r.get("sitrep_status_key", "").lower()
                    or term in r.get("sitrep_status_label", "").lower()
                    or term in r.get("sitrep_status_source", "").lower()
                    or term in r.get("sitrep_status_updated", "").lower()
                    or term in r.get("sitrep_status_age", "").lower()
                    or term in r.get("sitrep_source_chips", "").lower()
                    or (term == "conflict" and bool(r.get("sitrep_conflict")))
                    or (term == "sitrep")
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
        with perf_span(
            "operators.render_rows",
            settings=self.settings,
            meta={"rows": len(rows) if rows is not None else len(self._rows)},
            min_ms=5.0,
        ):
            if rows is None:
                rows = self._rows
            was_sorting = self.table.isSortingEnabled()
            sort_col = self.table.horizontalHeader().sortIndicatorSection()
            sort_order = self.table.horizontalHeader().sortIndicatorOrder()
            self.table.setUpdatesEnabled(False)
            self.table.blockSignals(True)
            try:
                if was_sorting:
                    self.table.setSortingEnabled(False)
                self.table.setRowCount(len(rows))
                # rebuild group filter options
                groups = set()
                for row_idx, r in enumerate(rows):

                    def set_item(col: int, text: str):
                        item = QTableWidgetItem(text)
                        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                        self.table.setItem(row_idx, col, item)

                    select_item = QTableWidgetItem("")
                    select_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
                    select_item.setCheckState(Qt.Unchecked)
                    self.table.setItem(row_idx, self.COL_SELECT, select_item)
                    set_item(self.COL_CALLSIGN, r["callsign"])
                    sitrep_key = (r.get("sitrep_status_key") or "unknown").strip().lower()
                    sitrep_item = QTableWidgetItem(self._sitrep_display_text(r))
                    sitrep_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                    sitrep_item.setTextAlignment(Qt.AlignCenter)
                    sitrep_item.setToolTip(self._sitrep_tooltip(r))
                    self._apply_sitrep_item_style(sitrep_item, sitrep_key)
                    self.table.setItem(row_idx, self.COL_SITREP, sitrep_item)
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
                available = {self.group_filter.itemText(i) for i in range(self.group_filter.count())}
                if current in available:
                    self.group_filter.setCurrentText(current)
                self.group_filter.blockSignals(False)
                if was_sorting:
                    self.table.setSortingEnabled(True)
                    if 0 <= sort_col < self.table.columnCount():
                        self.table.sortItems(sort_col, sort_order)
            finally:
                self.table.blockSignals(False)
                self.table.setUpdatesEnabled(True)
            self._sync_header_checkbox()

    # ------------- Qt events ------------- #

    def showEvent(self, event):
        """
        Refresh on show so the operator history is up to date.
        """
        super().showEvent(event)
        # Avoid synchronous DB/table rebuild on widget show, because this event
        # fires during tab switch and blocks navigation latency.
        self.on_tab_activated()

    def apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self._update_clear_filters_button_style()
        self._update_action_button_styles(theme)
        if self.loading_label:
            bg = theme.get("surface_alt", theme.get("surface", "#f2f2f2"))
            fg = theme.get("accent", theme.get("text", "#222"))
            border = theme.get("border", "#ccc")
            self.loading_label.setStyleSheet(
                f"padding: 2px 6px; border-radius: 4px; background: {bg}; color: {fg}; border: 1px solid {border};"
            )
        grid = theme["border"]
        table_style = f"QTableWidget {{ gridline-color: {grid}; }}"
        self.table.setStyleSheet(table_style)
        header = self.table.horizontalHeader()
        if isinstance(header, OperatorHeaderWithCheckbox):
            accent = QColor(theme["accent"])
            luminance = (
                0.299 * accent.redF()
                + 0.587 * accent.greenF()
                + 0.114 * accent.blueF()
            )
            mark = QColor("#111111") if luminance >= 0.62 else QColor("#ffffff")
            header.set_checkbox_colors(
                bg=QColor(theme.get("surface_alt", theme["surface"])),
                border=QColor(theme.get("accent", theme["border"])),
                accent=accent,
                mark=mark,
            )

    def on_tab_activated(self) -> None:
        with perf_span("operators.on_tab_activated", settings=self.settings, min_ms=5.0):
            if time.time() - float(self._last_load_ts) < self._load_min_interval_sec:
                self._set_loading(False)
                return
            if self._nav_refresh_inflight:
                return
            self._nav_refresh_inflight = True
            self._set_loading(True)
            QTimer.singleShot(0, self._run_activation_refresh)

    def _run_activation_refresh(self) -> None:
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
            item_sel = self.table.item(r, self.COL_SELECT)
            if item_sel and item_sel.checkState() == Qt.Checked:
                item = self.table.item(r, self.COL_CALLSIGN)
                if item:
                    calls.append(item.text().strip().upper())
        return calls

    def _select_all_rows(self):
        if not self._is_filter_active():
            QMessageBox.information(self, "Select All", "Apply a filter before bulk selecting.")
            return
        self._select_filtered_rows(True)

    def _on_row_checkbox_changed(self, _=None) -> None:
        if self._bulk_select_inflight:
            return
        self._sync_header_checkbox()

    def _on_table_item_changed(self, item: QTableWidgetItem) -> None:
        if item.column() != self.COL_SELECT:
            return
        if self._bulk_select_inflight:
            return
        self._sync_header_checkbox()
        self._update_action_button_styles()

    def _on_cell_clicked(self, row: int, col: int) -> None:
        if col != self.COL_SITREP:
            return
        item = self.table.item(row, self.COL_CALLSIGN)
        if not item:
            return
        callsign = (item.text() or "").strip().upper()
        if not callsign:
            return
        anchor_item = self.table.item(row, self.COL_SITREP)
        rect = self.table.visualItemRect(anchor_item) if anchor_item is not None else QRect()
        pos = self.table.viewport().mapToGlobal(rect.bottomLeft())
        self._show_sitrep_status_menu(callsign, pos)

    def _on_cell_double_clicked(self, row: int, col: int) -> None:
        if col != self.COL_GROUPS:
            return
        item = self.table.item(row, self.COL_CALLSIGN)
        if not item:
            return
        callsign = (item.text() or "").strip().upper()
        if not callsign:
            return
        self._edit_groups_dialog(callsign)

    def _edit_groups_dialog(self, callsign: str) -> None:
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
        dlg.setWindowTitle(f"Edit Groups for {callsign}")
        layout = QVBoxLayout(dlg)
        label = QLabel("One group per line:")
        layout.addWidget(label)
        text = QTextEdit()
        text.setPlainText("\n".join(groups))
        text.setMinimumHeight(220)
        layout.addWidget(text)
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        save_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        btn_row.addWidget(save_btn)
        btn_row.addWidget(cancel_btn)
        layout.addLayout(btn_row)

        def _save():
            raw = [ln.strip() for ln in text.toPlainText().splitlines() if ln.strip()]
            groups_norm = self._normalize_groups_list(raw)
            data = {"callsign": callsign, "groups": groups_norm}
            if self._upsert_record(data, merge_groups=False):
                self._load_data(show_toast=True)
                self._schedule_history_update()
            dlg.accept()

        save_btn.clicked.connect(_save)
        cancel_btn.clicked.connect(dlg.reject)
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
            existing_trusted = 0
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
            role_val = self._normalize_group_role(row.get("group_role", existing_role or ""))
            trusted = row.get("trusted")
            if trusted is None:
                trusted = 1 if int(existing_trusted or 0) == 1 else 0
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
                    role_val,
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
                "Select one or more groups in the Export groups dropdown before exporting.",
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
                    all_groups = {
                        (r.get("group1") or "").strip(),
                        (r.get("group2") or "").strip(),
                        (r.get("group3") or "").strip(),
                    }
                    all_groups.update({str(x).strip() for x in (r.get("groups") or [])})
                    row_groups = [g for g in selected_groups if g and g in all_groups]
                    export_g1 = row_groups[0] if len(row_groups) > 0 else ""
                    export_g2 = row_groups[1] if len(row_groups) > 1 else ""
                    export_g3 = row_groups[2] if len(row_groups) > 2 else ""
                    writer.writerow(
                        {
                            "callsign": r.get("callsign", ""),
                            "name": r.get("name", ""),
                            "state": r.get("state", ""),
                            "grid": r.get("grid", ""),
                            "groups": ",".join(row_groups),
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
        # Clear export group selections after successful export
        for cb in self._export_group_checks.values():
            cb.blockSignals(True)
            cb.setChecked(False)
            cb.blockSignals(False)
        self._update_export_group_placeholder()

    # ------------- CSV import ------------- #

    def _import_csv(self):
        fn, _ = QFileDialog.getOpenFileName(self, "Import Operators CSV", "", "CSV Files (*.csv);;All Files (*)")
        if not fn:
            return
        imported = 0
        skipped = 0
        role_cleared = 0
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
                    raw_role = (lower_row.get("group role") or lower_row.get("group_role") or "").strip()
                    normalized_role = self._normalize_group_role(raw_role)
                    data = {
                        "callsign": cs,
                        "name": lower_row.get("name", "").strip(),
                        "state": lower_row.get("state", "").strip().upper(),
                        "grid": lower_row.get("grid", "").strip().upper(),
                        "group1": lower_row.get("group1", "").strip().upper(),
                        "group2": lower_row.get("group2", "").strip().upper(),
                        "group3": lower_row.get("group3", "").strip().upper(),
                        "groups": groups_list,
                        "group_role": normalized_role,
                        "first_seen_utc": date_val,
                        "last_seen_utc": date_val,
                        "trusted": 1,
                    }
                    if self._upsert_record(data):
                        imported += 1
                        if raw_role and normalized_role == "":
                            role_cleared += 1
                    else:
                        skipped += 1
        except Exception as e:
            QMessageBox.warning(self, "CSV Import", f"Failed to import:\n{e}")
            log.error("OperatorHistoryTab: CSV import failed: %s", e)
            return

        self._load_data(show_toast=True)
        self._schedule_history_update()
        msg = f"Imported {imported} record(s). Skipped {skipped}."
        if role_cleared:
            msg += f" Cleared {role_cleared} non-standard group role value(s)."
        QMessageBox.information(self, "CSV Import", msg)

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
        role_combo = QComboBox()
        for role in GROUP_ROLE_OPTIONS:
            role_combo.addItem(role)
        role_combo.setCurrentText(self._normalize_group_role(defaults.get("group_role", "")))
        first_edit = QLineEdit(defaults.get("first_seen_utc", ""))
        last_edit = QLineEdit(defaults.get("last_seen_utc", ""))
        date_edit = QLineEdit(defaults.get("date_added", ""))
        date_edit.setVisible(False)
        date_label = QLabel("Date Added (legacy):")
        date_label.setVisible(False)
        trusted_chk = QCheckBox("Trusted")
        trusted_chk.setChecked(bool(defaults.get("trusted", 0)))

        form.addRow("Callsign*:", cs_edit)
        form.addRow("Name:", name_edit)
        form.addRow("State:", state_edit)
        form.addRow("Grid:", grid_edit)
        form.addRow("Group 1:", g1_edit)
        form.addRow("Group 2:", g2_edit)
        form.addRow("Group 3:", g3_edit)
        form.addRow("Group Role:", role_combo)
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
            "group_role": self._normalize_group_role(role_combo.currentText()),
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
            dlg = QDialog(self)
            dlg.setWindowTitle("Bulk Edit Operators")
            form = QFormLayout(dlg)

            trusted_combo = QComboBox()
            trusted_combo.addItem("No Change", "no_change")
            trusted_combo.addItem("Trusted", 1)
            trusted_combo.addItem("Untrusted", 0)

            role_combo = QComboBox()
            role_combo.addItem("No Change", "__no_change__")
            role_combo.addItem("(Clear)", "")
            for role in GROUP_ROLE_OPTIONS:
                if role:
                    role_combo.addItem(role, role)

            form.addRow("Trusted:", trusted_combo)
            form.addRow("Group Role:", role_combo)

            btn_row = QHBoxLayout()
            save_btn = QPushButton("Apply")
            cancel_btn = QPushButton("Cancel")
            btn_row.addStretch()
            btn_row.addWidget(save_btn)
            btn_row.addWidget(cancel_btn)
            form.addRow(btn_row)

            save_btn.clicked.connect(dlg.accept)
            cancel_btn.clicked.connect(dlg.reject)

            if dlg.exec() != QDialog.Accepted:
                return

            trusted_choice = trusted_combo.currentData()
            role_choice = role_combo.currentData()
            trusted_update = trusted_choice != "no_change"
            role_update = role_choice != "__no_change__"
            if not trusted_update and not role_update:
                QMessageBox.information(self, "Bulk Edit Operators", "No changes selected.")
                return

            trusted_txt = "No Change"
            if trusted_update:
                trusted_txt = "Trusted" if int(trusted_choice) == 1 else "Untrusted"
            role_txt = "No Change"
            if role_update:
                role_txt = "(Clear)" if str(role_choice) == "" else str(role_choice)
            confirm = QMessageBox.question(
                self,
                "Confirm Bulk Update",
                (
                    f"Apply bulk update to {len(calls)} selected operator(s)?\n\n"
                    f"Trusted: {trusted_txt}\n"
                    f"Group Role: {role_txt}"
                ),
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if confirm != QMessageBox.Yes:
                return

            changed = 0
            failed = 0
            for cs in calls:
                payload: Dict[str, object] = {"callsign": cs}
                if trusted_update:
                    payload["trusted"] = int(trusted_choice)
                if role_update:
                    payload["group_role"] = self._normalize_group_role(str(role_choice))
                if self._upsert_record(payload):
                    changed += 1
                else:
                    failed += 1
            if changed:
                self._load_data(show_toast=True)
                self._schedule_history_update()
            QMessageBox.information(
                self,
                "Bulk Edit Operators",
                f"Updated {changed} record(s). Failed {failed}.",
            )
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
        deleted = 0
        skipped = 0
        failed = 0
        try:
            cur = conn.cursor()
            for cs in calls:
                try:
                    cur.execute("DELETE FROM operator_checkins WHERE callsign = ?", (cs,))
                    if cur.rowcount:
                        deleted += 1
                    else:
                        skipped += 1
                except Exception:
                    failed += 1
            conn.commit()
        except Exception as e:
            log.error("OperatorHistoryTab: delete failed: %s", e)
            QMessageBox.warning(self, "DB Error", f"Delete failed:\n{e}")
            conn.close()
            return
        finally:
            conn.close()
        self._load_data(show_toast=True)
        self._schedule_history_update()
        QMessageBox.information(
            self,
            "Delete Operators",
            f"Deleted {deleted} record(s). Skipped {skipped}. Failed {failed}.",
        )

    def _schedule_history_update(self) -> None:
        if not self._update_timer.isActive():
            self._update_timer.start()
