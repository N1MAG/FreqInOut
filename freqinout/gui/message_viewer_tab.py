from __future__ import annotations

import html
import json
import os
import re
import sqlite3
import ctypes
import ctypes.wintypes
import platform
import shutil
import subprocess
import xml.dom.minidom
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from PySide6.QtCore import Qt, QTimer, QAbstractTableModel, QModelIndex, QEvent
from PySide6.QtGui import QPainter, QColor, QPalette
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QFileDialog,
    QGroupBox,
    QComboBox,
    QLineEdit,
    QTableWidget,
    QTableView,
    QTableWidgetItem,
    QHeaderView,
    QAbstractItemView,
    QMessageBox,
    QSizePolicy,
    QAbstractScrollArea,
    QSplitter,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QStyle,
)

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.gui.theme import resolve_theme, button_style


SUPPORTED_EXT = {".b2s", ".k2s", ".txt", ".ff", ".xml", ".json", ".html", ".htm"}
ORIGIN_EXTS = {
    "flmsg": {".b2s", ".k2s"},
    "flamp": {".b2s", ".k2s"},
    "varac": {".txt", ".html", ".htm", ".b2s", ".k2s"},
}

DEFAULT_WATCH_DIRS = [
    {"path": r"C:\VarAC", "origin": "varac"},
    {"path": r"C:\Users\HP\NBEMS.files\ICS\messages", "origin": "flmsg"},
    {"path": r"C:\Users\HP\NBEMS.files\FLAMP", "origin": "flamp"},
]

SCAN_CHOICES = [1, 15, 30, 60]  # minutes
JS8_POLL_SECONDS = 90  # 90 seconds
PENDING_POLL_SECONDS = 30
JS8_MAX_AGE_SECONDS = 30 * 24 * 60 * 60  # 30 days


@dataclass
class FileRecord:
    path: Path
    origin: str
    size: int = 0
    mtime: float = 0.0

    def display_name(self) -> str:
        return self.path.name

    def info_line(self) -> str:
        return f"{self.display_name()} - {self.size} bytes"


@dataclass
class JS8Message:
    msg_id: int
    from_call: str
    to_call: str
    msg_type: str  # "MSG" or "F!###"
    utc_str: str
    utc_ts: float
    raw_text: str
    decoded_text: str
    state: str  # UNREAD / READ
    read_ts: float = 0.0

    def display_line(self) -> str:
        return f"{self.utc_str[:10]}  {self.msg_type}  {self.from_call} -> {self.to_call}"


@dataclass
class UnifiedMessage:
    msg_type: str
    status: str
    from_call: str
    to_call: str
    rcv_ts: float
    rcv_display: str
    title: str
    origin: str
    payload: object


class MessageTableModel(QAbstractTableModel):
    HEADERS = ["MSG Type", "Status", "From", "To", "RCV_DT", "Message Title", ""]

    def __init__(self, rows: List[UnifiedMessage]):
        super().__init__()
        self._rows = rows

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.HEADERS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        row = self._rows[index.row()]
        col = index.column()
        if role == Qt.DisplayRole:
            if col == 0:
                return row.msg_type
            if col == 1:
                return row.status
            if col == 2:
                return row.from_call
            if col == 3:
                return row.to_call
            if col == 4:
                return row.rcv_display
            if col == 5:
                return row.title
            if col == 6:
                return "View" if isinstance(row.payload, JS8Message) else "View | Delete"
        if role == Qt.UserRole:
            return row
        if role == Qt.ForegroundRole and col == 1 and row.status == "NEW":
            return QColor(Qt.red)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.HEADERS):
            return self.HEADERS[section]
        return None

    def set_rows(self, rows: List[UnifiedMessage]) -> None:
        self.beginResetModel()
        self._rows = rows
        self.endResetModel()


class MessageActionDelegate(QStyledItemDelegate):
    def __init__(self, parent, danger_color: QColor | None = None):
        super().__init__(parent)
        self._danger = danger_color or QColor(Qt.red)

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: QModelIndex) -> None:
        if index.column() != 6:
            super().paint(painter, option, index)
            return
        row = index.data(Qt.UserRole)
        if row is None:
            return
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, False)
        rect = option.rect
        link_color = option.palette.color(QPalette.Link)
        painter.setPen(link_color)
        view_text = "View"
        fm = option.fontMetrics
        view_rect = rect.adjusted(6, 0, -6, 0)
        painter.drawText(view_rect, Qt.AlignVCenter | Qt.AlignLeft, view_text)
        if isinstance(row.payload, FileRecord):
            del_text = "Delete"
            del_width = fm.horizontalAdvance(del_text)
            del_rect = rect.adjusted(rect.width() - del_width - 6, 0, -6, 0)
            painter.setPen(self._danger)
            painter.drawText(del_rect, Qt.AlignVCenter | Qt.AlignLeft, del_text)
        painter.restore()

    def editorEvent(self, event, model, option, index):
        if index.column() != 6:
            return False
        if event.type() != QEvent.MouseButtonRelease:
            return False
        row = index.data(Qt.UserRole)
        if row is None:
            return False
        rect = option.rect
        pos = event.position().toPoint()
        if isinstance(row.payload, FileRecord):
            if pos.x() > rect.center().x():
                self.parent()._delete_file_record(row.payload)
            else:
                self.parent()._on_view_message(row)
        else:
            self.parent()._on_view_message(row)
        return True

class MessageViewerTab(QWidget):
    """
    Message Viewer for VarAC / FLMSG / FLAMP inbox-like folders.

    - Watches configured folders by origin
    - Shows a unified messages table (JS8 + file-based) with a viewer
    - Scan interval selectable (1 / 15 / 30 / 60 minutes)
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        cfg = self.settings.get("message_viewer", {}) or {}
        msg_paths = self.settings.get("message_paths", {}) or {}
        self.watch_dirs: List[Dict] = []
        for origin in ["js8", "varac", "flmsg", "flamp"]:
            p = msg_paths.get(origin, "")
            if p:
                self.watch_dirs.append({"path": p, "origin": origin})
        if not self.watch_dirs:
            self.watch_dirs = DEFAULT_WATCH_DIRS
        self.scan_minutes: int = cfg.get("scan_minutes") or 15
        if self.scan_minutes not in SCAN_CHOICES:
            self.scan_minutes = 15

        self.js8_messages: List[JS8Message] = []
        self.current_js8: JS8Message | None = None
        self._js8_timer: QTimer | None = None
        self._pending_timer: QTimer | None = None
        self._pending_rows: List[Dict[str, str | float]] = []
        self._form_cache: Dict[str, List[Dict]] = {}
        self.forms_path = (self.settings.get("js8_forms_path", "") or "").strip()
        self._read_state_map: Dict[tuple, tuple[str, float]] = {}
        self._message_rows: List[UnifiedMessage] = []
        self._filters_initialized = False
        self._has_active_view = False
        self._default_sort_column = 4
        self._default_sort_order = Qt.DescendingOrder
        self._sort_column = self._default_sort_column
        self._sort_order = self._default_sort_order
        self._freeze_messages_table = False
        self._deferred_refresh = False
        self._messages_model = MessageTableModel([])
        self._actions_delegate = None
        self._header_cells: List[QWidget] = []
        self._is_shutting_down = False
        self._refresh_files_inflight = False

        # merge DB paths if present
        self._load_watch_dirs_from_db()
        self._clear_backlog_on_upgrade()
        self._ensure_read_state_table()
        self._read_state_map = self._load_read_state_map()

        self.files: Dict[str, List[FileRecord]] = {"varac": [], "flmsg": [], "flamp": []}
        self.current_record: FileRecord | None = None

        self._timer: QTimer | None = None
        self.paths_labels: Dict[str, QLabel] = {}

        self._build_ui()
        self._load_paths_lists()
        self._refresh_files()
        self._setup_timer()
        self._refresh_js8_messages()
        self._setup_js8_timer()
        self._refresh_pending_backlog()
        self._setup_pending_timer()

    # ---------- DB helpers ----------

    def _db_path(self) -> Path | None:
        try:
            root = Path(__file__).resolve().parents[2]
            from freqinout.core.config_paths import get_config_dir

            return get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.error("MessageViewer: failed to resolve DB path: %s", e)
            return None

    def _load_watch_dirs_from_db(self):
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS message_viewer_paths (origin TEXT, path TEXT UNIQUE)"
            )
            cur.execute("SELECT origin, path FROM message_viewer_paths")
            rows = cur.fetchall()
            conn.close()
            existing = {(w.get("origin"), w.get("path")) for w in self.watch_dirs}
            for origin, path in rows:
                if (origin, path) not in existing:
                    self.watch_dirs.append({"origin": origin, "path": path})
        except Exception as e:
            log.error("MessageViewer: failed to load watch dirs from DB: %s", e)

    def _save_paths_to_db(self):
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS message_viewer_paths (origin TEXT, path TEXT UNIQUE)"
            )
            cur.execute("DELETE FROM message_viewer_paths")
            cur.executemany(
                "INSERT OR IGNORE INTO message_viewer_paths (origin, path) VALUES (?, ?)",
                [(w.get("origin"), w.get("path")) for w in self.watch_dirs if w.get("path")],
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.error("MessageViewer: failed to save watch dirs to DB: %s", e)

    def _backlog_db_path(self) -> Path | None:
        return self._db_path()

    def _ensure_backlog_table(self) -> None:
        db_path = self._backlog_db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS autoquery_backlog (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    callsign TEXT NOT NULL,
                    msg_id TEXT,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    attempts INTEGER DEFAULT 0,
                    last_attempt_ts REAL,
                    created_ts REAL
                )
                """
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to ensure backlog table: %s", e)

    def _ensure_read_state_table(self) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_read_state (
                    origin TEXT NOT NULL,
                    path TEXT NOT NULL,
                    mtime REAL NOT NULL,
                    size INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    read_ts REAL,
                    PRIMARY KEY (origin, path, mtime, size)
                )
                """
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to ensure read state table: %s", e)

    @staticmethod
    def _read_state_key(origin: str, rec: FileRecord) -> tuple:
        return (origin, str(rec.path), float(rec.mtime), int(rec.size))

    def _load_read_state_map(self) -> Dict[tuple, tuple[str, float]]:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return {}
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT origin, path, mtime, size, status, read_ts
                FROM message_read_state
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load read state: %s", e)
            return {}
        out: Dict[tuple, tuple[str, float]] = {}
        for origin, path, mtime, size, status, read_ts in rows:
            key = (origin, path, float(mtime or 0.0), int(size or 0))
            out[key] = (str(status or "").upper(), float(read_ts or 0.0))
        return out

    def _get_read_state(self, rec: FileRecord) -> str:
        key = self._read_state_key(rec.origin, rec)
        state = self._read_state_map.get(key)
        if state and state[0]:
            return state[0]
        return "NEW"

    def _set_read_state(self, rec: FileRecord, status: str) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        status = (status or "READ").upper()
        key = self._read_state_key(rec.origin, rec)
        read_ts = time.time() if status == "READ" else 0.0
        self._read_state_map[key] = (status, read_ts)
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT OR REPLACE INTO message_read_state
                    (origin, path, mtime, size, status, read_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (rec.origin, str(rec.path), float(rec.mtime), int(rec.size), status, float(read_ts)),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to persist read state: %s", e)
        self._refresh_table_after_read(
            lambda row: isinstance(row.payload, FileRecord)
            and self._read_state_key(row.payload.origin, row.payload) == key
        )

    def _clear_backlog_on_upgrade(self) -> None:
        if self.settings.get("autoquery_backlog_cleared_v1", False):
            return
        self._ensure_backlog_table()
        db_path = self._backlog_db_path()
        if not db_path or not db_path.exists():
            self.settings.set("autoquery_backlog_cleared_v1", True)
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM autoquery_backlog")
            conn.commit()
            conn.close()
            log.info("MessageViewer: cleared autoquery_backlog on upgrade")
        except Exception as e:
            log.debug("MessageViewer: failed to clear backlog on upgrade: %s", e)
        self.settings.set("autoquery_backlog_cleared_v1", True)

    # ---------- UI ----------

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Message Viewer</h3>"))
        header.addStretch()

        header.addWidget(QLabel("Scan every:"))
        self.scan_combo = QComboBox()
        for m in SCAN_CHOICES:
            self.scan_combo.addItem(f"{m} min", m)
        self.scan_combo.setCurrentText(f"{self.scan_minutes} min")
        self.scan_combo.currentIndexChanged.connect(self._on_scan_changed)
        header.addWidget(self.scan_combo)

        self.refresh_btn = QPushButton("Refresh Now")
        self.refresh_btn.clicked.connect(self._on_refresh_now)
        header.addWidget(self.refresh_btn)

        self.export_btn = QPushButton("Export to PDF")
        self.export_btn.clicked.connect(self._export_pdf)
        header.addWidget(self.export_btn)

        layout.addLayout(header)

        # Main layout
        body = QVBoxLayout()
        layout.addLayout(body)

        pending_box = QGroupBox("Pending JS8 MSGs")
        pending_layout = QVBoxLayout()
        pending_header = QHBoxLayout()
        self.pending_count = QLabel("0 pending")
        pending_header.addWidget(self.pending_count)
        pending_header.addStretch()
        pending_layout.addLayout(pending_header)

        self.pending_table = QTableWidget(0, 5)
        self.pending_table.setHorizontalHeaderLabels(
            ["Callsign", "Msg ID", "Last Seen", "Status", "Actions"]
        )
        self.pending_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.pending_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.pending_table.setAlternatingRowColors(True)
        self.pending_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        header = self.pending_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.Stretch)
        pending_layout.addWidget(self.pending_table)
        pending_box.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Maximum)
        pending_box.setLayout(pending_layout)
        body.addWidget(pending_box)

        messages_box = QGroupBox("Messages")
        messages_layout = QVBoxLayout()
        self.messages_header = QWidget()
        self.messages_header_layout = QHBoxLayout(self.messages_header)
        self.messages_header_layout.setContentsMargins(0, 0, 0, 4)
        self.messages_header_layout.setSpacing(0)
        messages_layout.addWidget(self.messages_header)

        self.messages_table = QTableView()
        self.messages_table.setModel(self._messages_model)
        self.messages_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.messages_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.messages_table.setAlternatingRowColors(True)
        self.messages_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        msg_header = self.messages_table.horizontalHeader()
        msg_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        msg_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        msg_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        msg_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        msg_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        msg_header.setSectionResizeMode(5, QHeaderView.Stretch)
        msg_header.setSectionResizeMode(6, QHeaderView.Fixed)
        self.messages_table.setColumnWidth(6, 140)
        msg_header.setVisible(True)
        msg_header.sectionClicked.connect(self._on_sort_clicked)
        self._actions_delegate = MessageActionDelegate(self, QColor(resolve_theme(self.settings)["danger"]))
        self.messages_table.setItemDelegateForColumn(6, self._actions_delegate)
        messages_layout.addWidget(self.messages_table)
        messages_box.setLayout(messages_layout)
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(messages_box)
        self.messages_splitter = splitter

        viewer_container = QWidget()
        viewer_layout = QVBoxLayout(viewer_container)
        self.info_label = QLabel("No file selected")
        self.info_label.setStyleSheet("font-weight: bold;")
        viewer_layout.addWidget(self.info_label)
        self.viewer = QTextEdit()
        self.viewer.setReadOnly(True)
        self.viewer.setAcceptRichText(False)
        viewer_layout.addWidget(self.viewer)
        splitter.addWidget(viewer_container)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 3)
        body.addWidget(splitter, 3)

        self.type_filter = QComboBox()
        self.status_filter = QComboBox()
        self.from_filter = QComboBox()
        self.to_filter = QComboBox()
        self.rcv_search = QLineEdit()
        self.clear_filters_btn = QPushButton("Clear Filters")
        self.clear_filters_btn.setMinimumWidth(130)
        self.clear_filters_btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.clear_filters_btn.setFont(self.pending_count.font())
        self.clear_filters_btn.clicked.connect(self._clear_filters)
        self.clear_filters_btn.setStyleSheet(button_style("muted", resolve_theme(self.settings)))
        self.type_filter.currentIndexChanged.connect(self._on_filter_changed)
        self.status_filter.currentIndexChanged.connect(self._on_filter_changed)
        self.from_filter.currentIndexChanged.connect(self._on_filter_changed)
        self.to_filter.currentIndexChanged.connect(self._on_filter_changed)
        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.timeout.connect(self._on_filter_changed)
        self.rcv_search.setPlaceholderText("Search...")
        self.rcv_search.textChanged.connect(lambda _: self._filter_timer.start(200))
        self._build_messages_header()
        QTimer.singleShot(0, self._set_initial_splitter_sizes)

    # ---------- Timer ----------

    def _setup_timer(self):
        if self._timer:
            self._timer.stop()
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._refresh_files)
        self._timer.start(self.scan_minutes * 60 * 1000)

    def _setup_js8_timer(self):
        if self._js8_timer:
            self._js8_timer.stop()
        self._js8_timer = QTimer(self)
        self._js8_timer.timeout.connect(self._refresh_js8_messages)
        self._js8_timer.start(JS8_POLL_SECONDS * 1000)

    def _setup_pending_timer(self):
        if self._pending_timer:
            self._pending_timer.stop()
        self._pending_timer = QTimer(self)
        self._pending_timer.timeout.connect(self._refresh_pending_backlog)
        self._pending_timer.start(PENDING_POLL_SECONDS * 1000)

    def _on_refresh_now(self) -> None:
        self._unfreeze_table()
        self._refresh_files(force=True)
        self._refresh_js8_messages(force=True)

    def _on_scan_changed(self):
        val = self.scan_combo.currentData()
        if not val:
            return
        self.scan_minutes = int(val)
        self._setup_timer()
        self._save_settings()

    # ---------- Paths ----------

    def _load_paths_lists(self):
        by_origin: Dict[str, List[str]] = {"varac": [], "flmsg": [], "flamp": []}
        for entry in self.watch_dirs:
            origin = entry.get("origin", "unknown")
            path = entry.get("path", "")
            if origin in by_origin and path:
                by_origin[origin].append(path)
        for origin, lbl in self.paths_labels.items():
            paths_raw = "; ".join(by_origin.get(origin, [])) if by_origin.get(origin) else "(none)"
            paths_txt = paths_raw if len(paths_raw) <= 50 else paths_raw[:50] + "..."
            lbl.setText(f"Paths: {paths_txt}")

    def _add_path(self, origin: str):
        fn = QFileDialog.getExistingDirectory(self, f"Add {origin.upper()} watch folder")
        if not fn:
            return
        self.watch_dirs.append({"path": fn, "origin": origin})
        self._save_settings()
        self._refresh_files()

    def _remove_path(self, origin: str):
        # remove last added path for this origin (or prompt later)
        paths = [w for w in self.watch_dirs if w.get("origin") == origin]
        if not paths:
            return
        last = paths[-1]
        self.watch_dirs = [w for w in self.watch_dirs if not (w.get("origin") == origin and w.get("path") == last.get("path"))]
        self._save_settings()
        self._refresh_files()

    # ---------- Scanning ----------

    def _refresh_files(self, force: bool = False):
        if self._is_shutting_down or self._refresh_files_inflight:
            return
        self._refresh_files_inflight = True
        start_ts = time.time()
        try:
            self._load_paths_lists()
            records: Dict[str, List[FileRecord]] = {"varac": [], "flmsg": [], "flamp": []}
            for entry in self.watch_dirs:
                origin = entry.get("origin", "unknown")
                if origin not in records:
                    continue
                allowed_exts = ORIGIN_EXTS.get(origin)
                p = entry.get("path", "")
                if not p:
                    continue
                base = Path(p)
                if not base.exists():
                    continue
                for f in base.glob("**/*"):
                    if not f.is_file():
                        continue
                    if f.suffix.lower() not in SUPPORTED_EXT:
                        continue
                    if allowed_exts and f.suffix.lower() not in allowed_exts:
                        continue
                    try:
                        st = f.stat()
                    except OSError:
                        continue
                    rec = FileRecord(path=f, origin=origin, size=st.st_size, mtime=st.st_mtime)
                    records[origin].append(rec)

            # Sort by mtime desc
            for origin in records:
                records[origin].sort(key=lambda r: r.mtime, reverse=True)

            self.files = records
            self._read_state_map = self._load_read_state_map()
            self._populate_messages_table(force=force)
        finally:
            self._refresh_files_inflight = False
            elapsed = time.time() - start_ts
            if elapsed > 0.5:
                log.debug("MessageViewer: refresh_files took %.2fs", elapsed)

    def _refresh_js8_messages(self, force: bool = False):
        if self._is_shutting_down:
            return
        # First ingest any new messages into local cache, then load from local cache for display
        try:
            self._ingest_js8_messages()
        except Exception as e:
            log.debug("MessageViewer: JS8 ingest failed: %s", e)
        try:
            self._load_js8_from_local(force=force)
        except Exception as e:
            log.debug("MessageViewer: JS8 local load failed: %s", e)

    # ---------- Pending JS8 MSG backlog ---------- #

    def _refresh_pending_backlog(self) -> None:
        if self._is_shutting_down:
            return
        self._ensure_backlog_table()
        self._update_pending_table()

    def _load_pending_rows(self) -> List[Dict[str, str | float]]:
        db_path = self._backlog_db_path()
        if not db_path or not db_path.exists():
            self._pending_rows = []
            return []
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT callsign, msg_id, status, last_attempt_ts, created_ts
                FROM autoquery_backlog
                WHERE kind='MSG'
                ORDER BY created_ts DESC
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load pending backlog: %s", e)
            self._pending_rows = []
            return []
        out: List[Dict[str, str | float]] = []
        for row in rows:
            out.append(
                {
                    "callsign": (row[0] or "").strip().upper(),
                    "msg_id": str(row[1] or "").strip(),
                    "status": (row[2] or "PENDING").strip().upper(),
                    "last_seen_ts": float(row[3] or row[4] or 0.0),
                }
            )
        self._pending_rows = out
        return out

    def _update_pending_table(self) -> None:
        rows = self._load_pending_rows()
        pending_count = sum(1 for row in rows if str(row.get("status", "")).upper() != "RETRIEVED")
        self.pending_count.setText(f"{pending_count} pending")
        self.pending_table.setRowCount(0)
        for idx, row in enumerate(rows):
            self.pending_table.insertRow(idx)
            callsign = str(row.get("callsign", ""))
            msg_id = str(row.get("msg_id", ""))
            status = str(row.get("status", "PENDING")).upper()
            last_seen_ts = float(row.get("last_seen_ts", 0.0))

            self.pending_table.setItem(idx, 0, QTableWidgetItem(callsign))
            self.pending_table.setItem(idx, 1, QTableWidgetItem(msg_id))
            self.pending_table.setItem(idx, 2, QTableWidgetItem(self._fmt_ts(last_seen_ts)))
            self.pending_table.setItem(idx, 3, QTableWidgetItem(status))

            action_widget = QWidget()
            action_layout = QHBoxLayout(action_widget)
            action_layout.setContentsMargins(0, 0, 0, 0)
            action_layout.setSpacing(6)

            get_btn = QPushButton()
            retrieved_btn = QPushButton()
            theme = resolve_theme(self.settings)

            if status == "RETRIEVED":
                get_btn.setText("Get")
                get_btn.setEnabled(False)
                get_btn.setStyleSheet(button_style("muted", theme))
                retrieved_btn.setText("Retrieved")
                retrieved_btn.setEnabled(False)
                retrieved_btn.setStyleSheet(button_style("muted", theme))
            else:
                get_btn.setText("Get")
                get_btn.setEnabled(True)
                get_btn.setStyleSheet(button_style("success", theme))
                retrieved_btn.setText("Mark Retrieved")
                retrieved_btn.setEnabled(True)
                retrieved_btn.setStyleSheet(button_style("warning", theme))

            get_btn.clicked.connect(lambda _, c=callsign, m=msg_id: self._on_pending_get(c, m))
            retrieved_btn.clicked.connect(lambda _, c=callsign, m=msg_id: self._on_pending_mark_retrieved(c, m))
            action_layout.addWidget(get_btn)
            action_layout.addWidget(retrieved_btn)
            action_layout.addStretch()
            self.pending_table.setCellWidget(idx, 4, action_widget)
        self._adjust_pending_table_height(len(rows))

    def apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        grid = theme["border"]
        table_style = f"QTableView {{ gridline-color: {grid}; }}"
        self.messages_table.setStyleSheet(table_style)
        self.pending_table.setStyleSheet(f"QTableWidget {{ gridline-color: {grid}; }}")
        if self._actions_delegate:
            self._actions_delegate._danger = QColor(theme["danger"])
        self._update_clear_filters_style()
        self._update_pending_table()

    def shutdown(self) -> None:
        self._is_shutting_down = True
        try:
            if self._timer:
                self._timer.stop()
        except Exception:
            pass
        try:
            if self._js8_timer:
                self._js8_timer.stop()
        except Exception:
            pass
        try:
            if self._pending_timer:
                self._pending_timer.stop()
        except Exception:
            pass
        try:
            if self._filter_timer:
                self._filter_timer.stop()
        except Exception:
            pass

    def on_settings_saved(self) -> None:
        try:
            if hasattr(self.settings, "reload"):
                self.settings.reload()
        except Exception:
            pass
        self._update_pending_table()

    def _adjust_pending_table_height(self, rows: int) -> None:
        header_h = self.pending_table.horizontalHeader().height()
        frame = self.pending_table.frameWidth() * 2
        if rows <= 0:
            self.pending_table.setVisible(False)
            self.pending_table.setMinimumHeight(0)
            self.pending_table.setMaximumHeight(header_h + frame)
            return
        self.pending_table.setVisible(True)
        self.pending_table.resizeRowsToContents()
        total_rows = sum(self.pending_table.rowHeight(i) for i in range(rows))
        total = header_h + total_rows + frame
        self.pending_table.setMinimumHeight(total)
        self.pending_table.setMaximumHeight(total)

    def _pending_set_status(self, callsign: str, msg_id: str, status: str) -> None:
        db_path = self._backlog_db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE autoquery_backlog
                SET status=?, last_attempt_ts=?
                WHERE callsign=? AND COALESCE(msg_id,'')=COALESCE(?, '') AND kind='MSG'
                """,
                (status.upper(), time.time(), callsign, msg_id or ""),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to set pending status: %s", e)

    def _pending_delete(self, callsign: str, msg_id: str) -> None:
        db_path = self._backlog_db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                DELETE FROM autoquery_backlog
                WHERE callsign=? AND COALESCE(msg_id,'')=COALESCE(?, '') AND kind='MSG'
                """,
                (callsign, msg_id or ""),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to delete pending row: %s", e)

    def _on_pending_get(self, callsign: str, msg_id: str) -> None:
        if not callsign or not msg_id:
            return
        mycall = self._my_callsign()
        if not mycall:
            QMessageBox.warning(self, "Missing Callsign", "Configure your callsign in the Settings tab.")
            return
        text = f"{mycall}: {callsign} QUERY MSG {msg_id}".strip()
        resp = QMessageBox.question(
            self,
            "Send MSG",
            f"Send this JS8Call message?\n\n{text}",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return
        if self._send_js8_message(text):
            self._pending_set_status(callsign, msg_id, "WAITING")
        self._update_pending_table()

    def _on_pending_mark_retrieved(self, callsign: str, msg_id: str) -> None:
        if not callsign or not msg_id:
            return
        if self.settings.get("js8_inbox_mark_retrieved_sync", False):
            ok = self._mark_js8call_inbox_read(callsign, msg_id)
            if not ok:
                log.debug(
                    "MessageViewer: JS8Call inbox mark READ failed (callsign=%s msg_id=%s)",
                    callsign,
                    msg_id,
                )
        self._pending_delete(callsign, msg_id)
        self._update_pending_table()

    def _send_js8_message(self, text: str) -> bool:
        import socket
        host = (self.settings.get("js8_host", "") or "").strip() or "127.0.0.1"
        try:
            port = int(self.settings.get("js8_port", 2442) or 2442)
        except Exception:
            port = 2442
        payload = json.dumps({"params": {}, "type": "TX.SEND_MESSAGE", "value": text}) + "\r\n"
        try:
            with socket.create_connection((host, port), timeout=2) as s:
                s.sendall(payload.encode("utf-8"))
            log.info("MessageViewer: sent JS8 TX.SEND_MESSAGE to %s:%s text=%s", host, port, text)
            return True
        except Exception as e:
            log.error("MessageViewer: failed to send JS8 message to %s:%s text=%s err=%s", host, port, text, e)
            return False

    def _my_callsign(self) -> str:
        return (
            (self.settings.get("operator_callsign", "") or self.settings.get("callsign", "") or "")
            .strip()
            .upper()
        )

    @staticmethod
    def _fmt_ts(ts: float) -> str:
        if not ts:
            return ""
        try:
            from datetime import datetime

            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""

    def _populate_messages_table(self, force: bool = False):
        rows = self._build_message_rows()
        self._message_rows = rows
        if self._freeze_messages_table and not force:
            self._deferred_refresh = True
            log.debug("MessageViewer: table refresh deferred (freeze active)")
            return
        self._deferred_refresh = False
        self._refresh_message_filters(rows)
        self._apply_message_filters()
        log.debug("MessageViewer: built %d unified messages", len(rows))

    def _refresh_message_filters(self, rows: List[UnifiedMessage]) -> None:
        type_vals = sorted({r.msg_type for r in rows if r.msg_type})
        status_vals = sorted({r.status for r in rows if r.status})
        from_vals = sorted({r.from_call for r in rows if r.from_call})
        to_vals = sorted({r.to_call for r in rows if r.to_call})

        current_type = self.type_filter.currentText() if hasattr(self, "type_filter") else "ALL"
        current_status = self.status_filter.currentText() if hasattr(self, "status_filter") else "ALL"
        current_from = self.from_filter.currentText() if hasattr(self, "from_filter") else "ALL"
        current_to = self.to_filter.currentText() if hasattr(self, "to_filter") else "ALL"
        if not current_type:
            current_type = "ALL"
        if not current_status:
            current_status = "ALL"
        if not current_from:
            current_from = "ALL"
        if not current_to:
            current_to = "ALL"

        self.type_filter.blockSignals(True)
        self.type_filter.clear()
        self.type_filter.addItem("MSG Type...")
        self.type_filter.addItems(type_vals)
        if not self._filters_initialized:
            self.type_filter.setCurrentText("MSG Type...")
        elif current_type in ["MSG Type..."] + type_vals:
            self.type_filter.setCurrentText(current_type)
        self.type_filter.blockSignals(False)

        self.status_filter.blockSignals(True)
        self.status_filter.clear()
        self.status_filter.addItem("Status...")
        self.status_filter.addItems(status_vals)
        if not self._filters_initialized:
            self.status_filter.setCurrentText("Status...")
        elif current_status in ["Status..."] + status_vals:
            self.status_filter.setCurrentText(current_status)
        self.status_filter.blockSignals(False)

        self.from_filter.blockSignals(True)
        self.from_filter.clear()
        self.from_filter.addItem("From...")
        self.from_filter.addItems(from_vals)
        if not self._filters_initialized:
            self.from_filter.setCurrentText("From...")
        elif current_from in ["From..."] + from_vals:
            self.from_filter.setCurrentText(current_from)
        self.from_filter.blockSignals(False)

        self.to_filter.blockSignals(True)
        self.to_filter.clear()
        self.to_filter.addItem("To...")
        self.to_filter.addItems(to_vals)
        if not self._filters_initialized:
            self.to_filter.setCurrentText("To...")
        elif current_to in ["To..."] + to_vals:
            self.to_filter.setCurrentText(current_to)
        self.to_filter.blockSignals(False)
        self._filters_initialized = True

    def _apply_message_filters(self) -> None:
        rows = self._message_rows
        type_sel = self.type_filter.currentText() if hasattr(self, "type_filter") else "MSG Type..."
        status_sel = self.status_filter.currentText() if hasattr(self, "status_filter") else "Status..."
        from_sel = self.from_filter.currentText() if hasattr(self, "from_filter") else "From..."
        to_sel = self.to_filter.currentText() if hasattr(self, "to_filter") else "To..."
        rcv_query = (self.rcv_search.text() if hasattr(self, "rcv_search") else "").strip().lower()

        filtered = []
        for row in rows:
            if type_sel != "MSG Type..." and row.msg_type != type_sel:
                continue
            if status_sel != "Status..." and row.status != status_sel:
                continue
            if from_sel != "From..." and row.from_call != from_sel:
                continue
            if to_sel != "To..." and row.to_call != to_sel:
                continue
            if rcv_query:
                hay = " ".join(
                    [
                        row.msg_type or "",
                        row.status or "",
                        row.from_call or "",
                        row.to_call or "",
                        row.rcv_display or "",
                        row.title or "",
                    ]
                ).lower()
                if rcv_query not in hay:
                    continue
            filtered.append(row)
        filtered = self._sort_rows(filtered)
        self._render_messages_table(filtered)
        self._update_clear_filters_style()
        log.debug(
            "MessageViewer: filters type=%s status=%s from=%s to=%s rcv=%s => %d rows",
            type_sel,
            status_sel,
            from_sel,
            to_sel,
            rcv_query or "ALL",
            len(filtered),
        )

    def _is_filter_or_sort_active(self) -> bool:
        type_sel = self.type_filter.currentText() if hasattr(self, "type_filter") else "MSG Type..."
        status_sel = self.status_filter.currentText() if hasattr(self, "status_filter") else "Status..."
        from_sel = self.from_filter.currentText() if hasattr(self, "from_filter") else "From..."
        to_sel = self.to_filter.currentText() if hasattr(self, "to_filter") else "To..."
        if type_sel not in ("", "MSG Type..."):
            return True
        if status_sel not in ("", "Status..."):
            return True
        if from_sel not in ("", "From..."):
            return True
        if to_sel not in ("", "To..."):
            return True
        if (self.rcv_search.text() if hasattr(self, "rcv_search") else "").strip():
            return True
        if (
            self._sort_column != self._default_sort_column
            or self._sort_order != self._default_sort_order
        ):
            return True
        return False

    def _apply_message_filters_preserve_scroll(self) -> None:
        if not hasattr(self, "messages_table"):
            self._apply_message_filters()
            return
        bar = self.messages_table.verticalScrollBar()
        value = bar.value()
        self._apply_message_filters()
        bar.setValue(min(value, bar.maximum()))

    def _update_rendered_status(self, match_fn) -> None:
        if not hasattr(self, "_messages_model"):
            return
        for i, row in enumerate(self._messages_model._rows):
            if match_fn(row):
                row.status = "READ"
                idx = self._messages_model.index(i, 1)
                self._messages_model.dataChanged.emit(
                    idx, idx, [Qt.DisplayRole, Qt.ForegroundRole]
                )

    def _refresh_table_after_read(self, match_fn) -> None:
        updated = False
        for row in self._message_rows:
            if match_fn(row):
                row.status = "READ"
                updated = True
        if not updated:
            return
        self._refresh_message_filters(self._message_rows)
        if self._is_filter_or_sort_active():
            self._apply_message_filters_preserve_scroll()
        else:
            self._update_rendered_status(match_fn)

    def _clear_filters(self) -> None:
        self._unfreeze_table()
        if (
            self.type_filter.currentText() in ("", "MSG Type...")
            and self.status_filter.currentText() in ("", "Status...")
            and self.from_filter.currentText() in ("", "From...")
            and self.to_filter.currentText() in ("", "To...")
            and not self.rcv_search.text().strip()
        ):
            return
        self.type_filter.blockSignals(True)
        self.status_filter.blockSignals(True)
        self.from_filter.blockSignals(True)
        self.to_filter.blockSignals(True)
        self.rcv_search.blockSignals(True)
        self.type_filter.setCurrentText("MSG Type...")
        self.status_filter.setCurrentText("Status...")
        self.from_filter.setCurrentText("From...")
        self.to_filter.setCurrentText("To...")
        self.rcv_search.clear()
        self.type_filter.blockSignals(False)
        self.status_filter.blockSignals(False)
        self.from_filter.blockSignals(False)
        self.to_filter.blockSignals(False)
        self.rcv_search.blockSignals(False)
        self._apply_message_filters()

    def _on_filter_changed(self) -> None:
        self._unfreeze_table()
        self._apply_message_filters()

    def _render_messages_table(self, rows: List[UnifiedMessage]) -> None:
        self.messages_table.setUpdatesEnabled(False)
        self._messages_model.set_rows(rows)
        if not self._has_active_view:
            self.info_label.setText("No file selected")
            self.viewer.clear()
            self.current_record = None
            self.current_js8 = None
        self.messages_table.setUpdatesEnabled(True)

    def _build_messages_header(self) -> None:
        while self.messages_header_layout.count():
            item = self.messages_header_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self._header_cells = []
        type_hdr = self._make_filter_cell(self.type_filter)
        status_hdr = self._make_filter_cell(self.status_filter)
        from_hdr = self._make_filter_cell(self.from_filter)
        to_hdr = self._make_filter_cell(self.to_filter)
        rcv_hdr = self._make_search_filter_cell(self.rcv_search)
        title_hdr = self._make_header_spacer()
        self.messages_header_layout.addWidget(type_hdr)
        self.messages_header_layout.addWidget(status_hdr)
        self.messages_header_layout.addWidget(from_hdr)
        self.messages_header_layout.addWidget(to_hdr)
        self.messages_header_layout.addWidget(rcv_hdr)
        self.messages_header_layout.addWidget(title_hdr, 1)
        self._header_cells.extend([type_hdr, status_hdr, from_hdr, to_hdr, rcv_hdr, title_hdr])
        clear_wrap = QWidget()
        clear_layout = QHBoxLayout(clear_wrap)
        clear_layout.setContentsMargins(2, 2, 2, 2)
        clear_layout.addStretch()
        clear_layout.addWidget(self.clear_filters_btn)
        clear_wrap.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        clear_layout.setAlignment(self.clear_filters_btn, Qt.AlignRight | Qt.AlignVCenter)
        self.messages_header_layout.addWidget(clear_wrap)
        self._header_cells.append(clear_wrap)
        self._update_clear_filters_style()
        self._sync_header_widths()
        self.messages_table.horizontalHeader().sectionResized.connect(self._sync_header_widths)
        self.messages_header.setMinimumHeight(self.messages_header.sizeHint().height())

    def _set_initial_splitter_sizes(self) -> None:
        if not hasattr(self, "messages_splitter"):
            return
        row_height = self.messages_table.verticalHeader().defaultSectionSize()
        header_height = self.messages_header.sizeHint().height()
        target = (row_height * 5) + header_height + 12
        total = max(target * 3, 400)
        self.messages_table.setMinimumHeight((row_height * 5) + 8)
        self.messages_splitter.setSizes([target, total - target])
        self._sync_header_widths()

    def _unfreeze_table(self) -> None:
        if not self._freeze_messages_table:
            return
        self._freeze_messages_table = False
        if self._deferred_refresh:
            self._populate_messages_table(force=True)

    def _make_filter_cell(self, combo: QComboBox) -> QWidget:
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(4, 0, 4, 0)
        layout.setSpacing(2)
        combo.setMinimumWidth(110)
        combo.setMinimumContentsLength(6)
        combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        layout.addWidget(combo)
        return container

    def _make_search_filter_cell(self, edit: QLineEdit) -> QWidget:
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(4, 0, 4, 0)
        layout.setSpacing(2)
        edit.setMinimumWidth(200)
        edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        layout.addWidget(edit)
        return container

    @staticmethod
    def _make_header_spacer() -> QWidget:
        spacer = QWidget()
        return spacer

    def _sync_header_widths(self) -> None:
        header = self.messages_table.horizontalHeader()
        for idx, widget in enumerate(self._header_cells):
            if widget is None:
                continue
            width = header.sectionSize(idx)
            min_width = 140 if idx == 6 else 60
            widget.setFixedWidth(max(min_width, width))

    def _filters_active(self) -> bool:
        return (
            self.type_filter.currentText() not in ("", "MSG Type...")
            or self.status_filter.currentText() not in ("", "Status...")
            or self.from_filter.currentText() not in ("", "From...")
            or self.to_filter.currentText() not in ("", "To...")
            or bool(self.rcv_search.text().strip())
        )

    def _update_clear_filters_style(self) -> None:
        theme = resolve_theme(self.settings)
        role = "warning" if self._filters_active() else "muted"
        self.clear_filters_btn.setStyleSheet(button_style(role, theme))

    def _on_sort_clicked(self, section: int) -> None:
        if section >= 6:
            return
        if section == self._sort_column:
            self._sort_order = (
                Qt.AscendingOrder if self._sort_order == Qt.DescendingOrder else Qt.DescendingOrder
            )
        else:
            self._sort_column = section
            self._sort_order = Qt.AscendingOrder
        self._apply_message_filters()

    def _sort_rows(self, rows: List[UnifiedMessage]) -> List[UnifiedMessage]:
        reverse = self._sort_order == Qt.DescendingOrder
        col = self._sort_column

        def key(row: UnifiedMessage):
            if col == 0:
                return row.msg_type or ""
            if col == 1:
                return row.status or ""
            if col == 2:
                return row.from_call or ""
            if col == 3:
                return row.to_call or ""
            if col == 4:
                return row.rcv_ts or 0.0
            if col == 5:
                return row.title or ""
            return row.rcv_ts or 0.0

        return sorted(rows, key=key, reverse=reverse)


    # ---------- Selection / Viewing ----------

    def _build_message_rows(self) -> List[UnifiedMessage]:
        rows: List[UnifiedMessage] = []
        for msg in self.js8_messages:
            msg_type = "Spotter" if msg.msg_type.startswith("F!") else "JS8 MSG"
            status = "READ" if msg.state.upper() == "READ" else "NEW"
            rcv_ts = msg.utc_ts or 0.0
            rcv_display = msg.utc_str or self._fmt_ts(rcv_ts)
            title = ""
            if msg_type == "Spotter":
                title = "Spotter"
            else:
                title = (msg.decoded_text or msg.raw_text or "").strip()
            if len(title) > 60:
                title = title[:57].rstrip() + "..."
            rows.append(
                UnifiedMessage(
                    msg_type=msg_type,
                    status=status,
                    from_call=(msg.from_call or "").strip().upper(),
                    to_call=(msg.to_call or "").strip().upper(),
                    rcv_ts=rcv_ts,
                    rcv_display=rcv_display,
                    title=title,
                    origin="js8",
                    payload=msg,
                )
            )

        for origin, recs in self.files.items():
            for rec in recs:
                status = self._get_read_state(rec)
                from_call = self._extract_sender_from_file(rec)
                title = rec.path.name
                rcv_ts = rec.mtime or 0.0
                rcv_display = self._fmt_ts(rcv_ts)
                rows.append(
                    UnifiedMessage(
                        msg_type=origin.upper() if origin != "varac" else "VarAC",
                        status=status,
                        from_call=from_call,
                        to_call="",
                        rcv_ts=rcv_ts,
                        rcv_display=rcv_display,
                        title=title,
                        origin=origin,
                        payload=rec,
                    )
                )

        rows.sort(key=lambda r: r.rcv_ts, reverse=True)
        return rows

    def _on_view_message(self, row: UnifiedMessage) -> None:
        log.debug(
            "MessageViewer: view requested type=%s origin=%s title=%s",
            row.msg_type,
            row.origin,
            row.title,
        )
        self._has_active_view = True
        self._freeze_messages_table = True
        if isinstance(row.payload, JS8Message):
            self.current_record = None
            self.current_js8 = row.payload
            self._load_js8_content(row.payload)
            self._mark_js8_read(row.payload)
        elif isinstance(row.payload, FileRecord):
            self.current_js8 = None
            self.current_record = row.payload
            self._load_content(row.payload)
            self._set_read_state(row.payload, "READ")

    def _read_file_head(self, path: Path, limit: int = 4096) -> str:
        try:
            with path.open("rb") as fh:
                raw = fh.read(limit)
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return ""

    def _extract_sender_from_file(self, rec: FileRecord) -> str:
        text = self._read_file_head(rec.path)
        if not text:
            log.debug("MessageViewer: sender parse empty for %s", rec.path)
            return ""
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            log.debug("MessageViewer: sender parse no lines for %s", rec.path)
            return ""
        for marker in (":hdr_fm:", ":hdr_ed:"):
            for idx, line in enumerate(lines):
                if line.lower().startswith(marker):
                    for nxt in lines[idx + 1 :]:
                        parts = nxt.split()
                        if parts:
                            sender = parts[0].strip().upper()
                            log.debug(
                                "MessageViewer: sender parsed via %s for %s => %s",
                                marker,
                                rec.path.name,
                                sender,
                            )
                            return sender
                    break
        tokens = re.split(r"[-_\\s]+", rec.path.stem)
        for tok in tokens:
            up = tok.strip().upper()
            if re.fullmatch(r"[A-Z0-9]{3,6}", up):
                log.debug("MessageViewer: sender fallback from filename %s => %s", rec.path.name, up)
                return up
        log.debug("MessageViewer: sender not found for %s", rec.path.name)
        return ""

    @staticmethod
    def _title_from_filename(path: Path) -> str:
        stem = path.stem
        tokens = [t for t in re.split(r"[-_]", stem) if t]
        if not tokens:
            return stem
        date_idx: Optional[int] = None
        for i, tok in enumerate(tokens):
            t = tok.lower()
            if re.fullmatch(r"\d{6,8}", t) or re.fullmatch(r"\d{4,6}z", t) or re.fullmatch(r"\d{5,6}z", t):
                date_idx = i
                break
        title_tokens = tokens[date_idx + 1 :] if date_idx is not None else tokens[-1:]
        title = " ".join(title_tokens).strip()
        return title or stem

    def _resolve_custom_forms_path(self) -> Optional[Path]:
        override = (self.settings.get("nbems_custom_forms_path", "") or "").strip()
        if override:
            p = Path(override)
            if p.exists():
                log.debug("MessageViewer: using custom forms override %s", p)
                return p
        msg_paths = self.settings.get("message_paths", {}) or {}
        for origin in ("flmsg", "flamp"):
            base = (msg_paths.get(origin) or "").strip()
            if not base:
                continue
            p = Path(base)
            for parent in [p] + list(p.parents):
                name = parent.name.lower()
                if name in {"nbems.files", ".nbems"}:
                    cand = parent / "CUSTOM"
                    if cand.exists():
                        log.debug("MessageViewer: using custom forms path %s", cand)
                        return cand
        fallback = Path(r"C:\Users\billd\NBEMS.files\CUSTOM")
        if fallback.exists():
            log.debug("MessageViewer: using custom forms fallback %s", fallback)
        return fallback if fallback.exists() else None

    @staticmethod
    def _extract_custom_form_name(text: str) -> str:
        if not text:
            return ""
        m = re.search(r"CUSTOM_FORM,([A-Za-z0-9_.-]+)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return ""

    @staticmethod
    def _parse_custom_form_fields(text: str) -> Dict[str, str]:
        fields: Dict[str, str] = {}
        if not text:
            return fields
        for line in text.splitlines():
            line = line.strip()
            if not line or "," not in line:
                continue
            key, val = line.split(",", 1)
            key = key.strip().upper()
            if re.fullmatch(r"L\d{1,2}", key):
                fields[key] = val.strip()
        return fields

    @staticmethod
    def _apply_form_fields(template: str, fields: Dict[str, str]) -> str:
        if not template or not fields:
            return template
        out = template
        for key, raw_val in fields.items():
            val = html.escape(raw_val or "")
            input_re = re.compile(
                rf'(<input[^>]*\bname="{key}"[^>]*)(>)',
                re.IGNORECASE,
            )
            def repl_input(match):
                tag = match.group(1)
                if re.search(r"\bvalue=", tag, re.IGNORECASE):
                    tag = re.sub(r'\bvalue="[^"]*"', f'value="{val}"', tag, flags=re.IGNORECASE)
                    return tag + match.group(2)
                return tag + f' value="{val}"' + match.group(2)
            out = input_re.sub(repl_input, out)

            textarea_re = re.compile(
                rf'(<textarea[^>]*\bname="{key}"[^>]*>)(.*?)(</textarea>)',
                re.IGNORECASE | re.DOTALL,
            )
            out = textarea_re.sub(rf'\1{val}\3', out)

            select_re = re.compile(
                rf'(<select[^>]*\bname="{key}"[^>]*>)(.*?)(</select>)',
                re.IGNORECASE | re.DOTALL,
            )
            def repl_select(match):
                block = match.group(2)
                block = re.sub(r'\sselected="selected"', "", block, flags=re.IGNORECASE)
                opt_re = re.compile(
                    r'(<option[^>]*value="([^"]*)"[^>]*>)(.*?)</option>',
                    re.IGNORECASE | re.DOTALL,
                )
                def repl_opt(opt_match):
                    opt_val = opt_match.group(2)
                    label = re.sub(r"\s+", " ", opt_match.group(3)).strip()
                    if opt_val == raw_val or label == raw_val:
                        tag = opt_match.group(1)
                        if "selected" not in tag.lower():
                            tag = (
                                tag[:-1] + ' selected="selected">'
                                if tag.endswith(">")
                                else tag + ' selected="selected">'
                            )
                        return tag + opt_match.group(3) + "</option>"
                    return opt_match.group(0)
                block = opt_re.sub(repl_opt, block)
                return match.group(1) + block + match.group(3)
            out = select_re.sub(repl_select, out)
        return out

    @staticmethod
    def _strip_html(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _extract_title_from_template(template: str) -> str:
        if not template:
            return ""
        m = re.search(r"<title>(.*?)</title>", template, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        return MessageViewerTab._strip_html(m.group(1))

    @staticmethod
    def _extract_template_labels(template: str) -> List[Tuple[str, str]]:
        if not template:
            return []
        field_re = re.compile(
            r"<(input|select|textarea)[^>]*\bname=\"(L\d{1,2})\"[^>]*>",
            re.IGNORECASE,
        )
        label_re = re.compile(r"<label[^>]*>(.*?)</label>", re.IGNORECASE | re.DOTALL)
        container_re = re.compile(r"<(td|div)[^>]*>", re.IGNORECASE)
        labels: List[Tuple[str, str]] = []
        seen = set()
        for match in field_re.finditer(template):
            name = match.group(2).upper()
            if name in seen:
                continue
            start = max(0, match.start() - 1200)
            window = template[start:match.start()]
            label_text = ""
            for label_match in label_re.finditer(window):
                label_text = label_match.group(1)
            if not label_text:
                container_pos = window.lower().rfind("<td")
                if container_pos == -1:
                    container_pos = window.lower().rfind("<div")
                if container_pos != -1:
                    container = window[container_pos:]
                    label_text = MessageViewerTab._strip_html(container)
                    if label_text:
                        parts = [p.strip() for p in label_text.splitlines() if p.strip()]
                        if parts:
                            label_text = parts[-1]
            label_text = MessageViewerTab._strip_html(label_text) or name
            labels.append((name, label_text))
            seen.add(name)
        return labels

    @staticmethod
    def _render_custom_form_fields(fields: Dict[str, str], labels: List[Tuple[str, str]], title: str = "") -> str:
        rows = []
        if labels:
            for key, label in labels:
                value = MessageViewerTab._normalize_field_value(fields.get(key, ""))
                rows.append((label, value))
        else:
            for key in sorted(fields.keys()):
                rows.append((key, MessageViewerTab._normalize_field_value(fields.get(key, ""))))
        html_out = [
            "<style>",
            ".field-table { width: 100%; border-collapse: collapse; }",
            ".field-row { border-bottom: 1px solid; }",
            ".field-cell { padding: 6px; vertical-align: top; }",
            ".label { font-weight: bold; }",
            ".value { white-space: pre-wrap; }",
            "</style>",
        ]
        if title:
            html_out.append(f"<div class='label' style='font-size: 16px; margin-bottom: 8px;'>{html.escape(title)}</div>")
        html_out.append("<table class='field-table'>")
        for label, value in rows:
            html_out.append("<tr class='field-row'>")
            html_out.append(f"<td class='field-cell label'>{html.escape(label)}</td>")
            html_out.append(f"<td class='field-cell value'>{html.escape(value)}</td>")
            html_out.append("</tr>")
        html_out.append("</table>")
        return "".join(html_out)

    @staticmethod
    def _normalize_field_value(value: str) -> str:
        if not value:
            return ""
        out = value.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", "\t")
        out = out.replace('\\"', '"').replace("\\'", "'")
        return out

    @staticmethod
    def _merge_template_with_raw(template: str, raw_text: str) -> str:
        safe_raw = '<pre>' + raw_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;') + '</pre>'
        for token in ('{{DATA}}', '{{RAW}}', '%%DATA%%', '%%RAW%%'):
            if token in template:
                return template.replace(token, safe_raw)
        return template + '\n' + safe_raw

    @staticmethod
    def _parse_form_fields(text: str, field_titles: Dict[str, str], value_mappings: Dict[str, Dict[str, str]] | None = None) -> Dict[str, str]:
        parsed = {title: "" for title in field_titles.values()}
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            for key, title in field_titles.items():
                if line.startswith(key):
                    value = line[len(key):].strip().strip(",")
                    if value_mappings and title in value_mappings:
                        mapped = value_mappings[title].get(value, value)
                        value = mapped
                    parsed[title] = value
                    break
        return parsed

    @staticmethod
    def _format_fields_table(parsed: Dict[str, str], last_field: str, status_fields: Optional[Dict[str, str]] = None) -> str:
        html_out = [
            "<style>",
            ".field-table { width: 100%; border-collapse: collapse; }",
            ".field-cell { width: 50%; padding: 4px; vertical-align: top; }",
            ".label { font-weight: bold; }",
            ".long-text { white-space: pre-wrap; }",
            "</style>",
            "<table class='field-table'>",
        ]
        items = list(parsed.items())
        if not items:
            return ""
        last_index = len(items) - 1
        for i in range(0, last_index, 2):
            html_out.append("<tr>")
            html_out.append(MessageViewerTab._render_field_cell(items[i], status_fields))
            if i + 1 < last_index:
                html_out.append(MessageViewerTab._render_field_cell(items[i + 1], status_fields))
            else:
                html_out.append("<td></td>")
            html_out.append("</tr>")
        html_out.append("<tr><td colspan='2' style='height:10px;'></td></tr>")
        title, value = items[last_index]
        html_out.append("<tr><td colspan='2'>")
        html_out.append(f"<div class='label'>{html.escape(title)}:</div>")
        html_out.append(f"<div class='long-text'>{html.escape(value)}</div>")
        html_out.append("</td></tr></table>")
        return "".join(html_out)

    @staticmethod
    def _render_field_cell(item: Tuple[str, str], status_fields: Optional[Dict[str, str]] = None) -> str:
        title, value = item
        display_value = value if value.strip() else "Unknown"
        label = html.escape(title)
        display = html.escape(display_value)
        return f"<td class='field-cell'><span class='label'>{label}:</span> {display}</td>"

    @staticmethod
    def _parse_blank_form_content(text: str) -> str:
        field_titles = {
            "L01": "To",
            "L02": "From",
            "L03": "Prec",
            "L04": "DTG",
            "L05": "Subject",
            "L06": "Message",
        }
        prec_mapping = {"R": "Routine", "P": "Priority", "I": "Immediate", "F": "Flash"}
        parsed = MessageViewerTab._parse_form_fields(text, field_titles, {"Prec": prec_mapping})
        if not any(v.strip() for v in parsed.values()):
            msg = MessageViewerTab._match_field(text, r":mg:\s*(.*)$")
            if msg:
                msg = msg.replace("\\n\\n", "\n\n").replace("\\n", "\n")
            from_call = MessageViewerTab._extract_hdr_call(text, ":hdr_fm:")
            fallback = {"From": from_call, "Message": msg or ""}
            return MessageViewerTab._format_fields_table(fallback, "Message")
        if "Message" in parsed:
            parsed["Message"] = parsed["Message"].replace("\\n\\n", "\n\n").replace("\\n", "\n")
        return MessageViewerTab._format_fields_table(parsed, "Message")

    @staticmethod
    def _parse_sitrep_content(text: str) -> str:
        field_titles = {
            "L01": "To",
            "L02": "From",
            "L03": "Prec",
            "L04": "State",
            "L05": "Grid",
            "L06": "Scope",
            "L07": "DTG",
            "L08": "Expires",
            "L09": "Status",
            "L10": "Narrative",
        }
        mappings = {
            "Prec": {"R": "Routine", "P": "Priority", "I": "Immediate", "F": "Flash"},
            "Scope": {"L": "Local", "R": "Regional", "N": "National", "U": "Unknown"},
            "Status": {"N": "New", "O": "On Going", "R": "Resolved", "U": "Unknown"},
        }
        parsed = MessageViewerTab._parse_form_fields(text, field_titles, mappings)
        if "Narrative" in parsed:
            parsed["Narrative"] = parsed["Narrative"].replace("\\n\\n", "\n\n").replace("\\n", "\n")
        return MessageViewerTab._format_fields_table(parsed, "Narrative")

    @staticmethod
    def _parse_statrep_content(text: str) -> str:
        field_titles = {
            "L01a": "To",
            "L01b": "From",
            "L02": "Scope",
            "L03": "DTG",
            "L04": "State",
            "L05": "Grid",
            "L06": "Map Pin",
            "L07": "Power",
            "L08": "Pub Water",
            "L09": "Medical",
            "L10": "Ovr Air Comms",
            "L11": "Travl Cndtns",
            "L12": "Internet",
            "L13": "Fuel",
            "L14": "Food",
            "L15": "Criminal Act",
            "L16": "Civil",
            "L17": "Political",
            "L18": "Remarks or Narrative",
        }
        status_mapping = {"G": "Green", "Y": "Yellow", "R": "Red", "U": "Unknown"}
        mappings = {
            "Scope": {"C": "My Community", "N": "My County", "R": "My Region", "O": "Other Location"},
            "Map Pin": status_mapping,
            "Power": status_mapping,
            "Pub Water": status_mapping,
            "Medical": status_mapping,
            "Ovr Air Comms": status_mapping,
            "Travl Cndtns": status_mapping,
            "Internet": status_mapping,
            "Fuel": status_mapping,
            "Food": status_mapping,
            "Criminal Act": status_mapping,
            "Civil": status_mapping,
            "Political": status_mapping,
        }
        parsed = MessageViewerTab._parse_form_fields(text, field_titles, mappings)
        if not parsed.get("Scope", "").strip():
            parsed["Scope"] = "My Location"
        return MessageViewerTab._format_fields_table(parsed, "Remarks or Narrative")

    @staticmethod
    def _parse_b2s_form_content(text: str) -> str:
        parsed = {
            "From": MessageViewerTab._match_field(text, r":hdr_fm:\s*(.*?)\s*(?=:)"),
            "DTG": MessageViewerTab._match_field(text, r":hdr_ed:\s*(.*?)\s*(?=:)"),
            "Prec": MessageViewerTab._match_field(text, r":prec:\s*(.*?)\s*(?=:)"),
            "Subject": MessageViewerTab._match_field(text, r":sub:\s*(.*?)\s*(?=:)"),
            "Message": MessageViewerTab._match_field(text, r":mg:\s*(.*)$"),
        }
        prec_mapping = {"R": "Routine", "P": "Priority", "I": "Immediate", "F": "Flash"}
        parsed["Prec"] = prec_mapping.get(parsed["Prec"].upper(), parsed["Prec"])
        return MessageViewerTab._format_fields_table(parsed, "Message")

    @staticmethod
    def _match_field(text: str, pattern: str) -> str:
        m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        return m.group(1).strip().replace("\r\n", "\n")

    @staticmethod
    def _extract_hdr_call(text: str, marker: str) -> str:
        if not text:
            return ""
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        for idx, line in enumerate(lines):
            if line.lower().startswith(marker):
                for nxt in lines[idx + 1 :]:
                    parts = nxt.split()
                    if parts:
                        return parts[0].strip().upper()
                break
        return ""

    def _delete_file_record(self, rec: FileRecord) -> None:
        if not rec or not rec.path.exists():
            return
        title = "Delete File"
        details = (
            f"Move this file to the Recycle Bin?\n\n"
            f"{rec.path}\n"
            f"Size: {rec.size} bytes\n"
            f"Modified: {self._fmt_mtime(rec.mtime)}"
        )
        resp = QMessageBox.question(self, title, details, QMessageBox.Yes | QMessageBox.No)
        if resp != QMessageBox.Yes:
            return
        ok = self._send_to_recycle_bin(rec.path)
        if not ok:
            QMessageBox.warning(self, title, "Failed to move file to the Recycle Bin.")
            return
        log.info("MessageViewer: deleted file %s", rec.path)
        self._remove_file_record(rec)
        self._unfreeze_table()
        self._populate_messages_table(force=True)

    def _remove_file_record(self, rec: FileRecord) -> None:
        origin = rec.origin
        if origin in self.files:
            self.files[origin] = [r for r in self.files[origin] if r.path != rec.path]
        key = self._read_state_key(origin, rec)
        self._read_state_map.pop(key, None)
        db_path = self._db_path()
        if db_path and db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute(
                    """
                    DELETE FROM message_read_state
                    WHERE origin=? AND path=? AND mtime=? AND size=?
                    """,
                    (origin, str(rec.path), float(rec.mtime), int(rec.size)),
                )
                conn.commit()
                conn.close()
            except Exception:
                pass
        if self.current_record and self.current_record.path == rec.path:
            self.current_record = None
            self._has_active_view = False
            self.info_label.setText("No file selected")
            self.viewer.clear()

    @staticmethod
    def _send_to_recycle_bin(path: Path) -> bool:
        if platform.system() == "Windows":
            try:
                FO_DELETE = 3
                FOF_ALLOWUNDO = 0x40
                FOF_NOCONFIRMATION = 0x10
                class SHFILEOPSTRUCTW(ctypes.Structure):
                    _fields_ = [
                        ("hwnd", ctypes.wintypes.HWND),
                        ("wFunc", ctypes.wintypes.UINT),
                        ("pFrom", ctypes.wintypes.LPCWSTR),
                        ("pTo", ctypes.wintypes.LPCWSTR),
                        ("fFlags", ctypes.c_uint16),
                        ("fAnyOperationsAborted", ctypes.wintypes.BOOL),
                        ("hNameMappings", ctypes.wintypes.LPVOID),
                        ("lpszProgressTitle", ctypes.wintypes.LPCWSTR),
                    ]
                path_str = str(path) + "\0\0"
                op = SHFILEOPSTRUCTW()
                op.wFunc = FO_DELETE
                op.pFrom = path_str
                op.fFlags = FOF_ALLOWUNDO | FOF_NOCONFIRMATION
                res = ctypes.windll.shell32.SHFileOperationW(ctypes.byref(op))
                if res != 0 or op.fAnyOperationsAborted:
                    log.debug(
                        "MessageViewer: recycle bin delete failed res=%s aborted=%s path=%s",
                        res,
                        bool(op.fAnyOperationsAborted),
                        path,
                    )
                return res == 0 and not op.fAnyOperationsAborted
            except Exception as e:
                log.debug("MessageViewer: recycle bin delete exception path=%s err=%s", path, e)
                return False

        # Linux fallbacks: gio, trash-put (trash-cli), then kioclient
        path_str = str(path)
        for cmd in (["gio", "trash", path_str], ["trash-put", path_str], ["kioclient5", "move", path_str, "trash:/"], ["kioclient", "move", path_str, "trash:/"]):
            exe = cmd[0]
            if not shutil.which(exe):
                continue
            try:
                res = subprocess.run(cmd, capture_output=True, text=True, check=False)
                if res.returncode == 0:
                    return True
                log.debug(
                    "MessageViewer: recycle bin delete failed cmd=%s code=%s stderr=%s",
                    " ".join(cmd),
                    res.returncode,
                    res.stderr.strip(),
                )
            except Exception as e:
                log.debug(
                    "MessageViewer: recycle bin delete exception cmd=%s err=%s",
                    " ".join(cmd),
                    e,
                )
        return False

    @staticmethod
    def _parse_unknown_content(text: str) -> str:
        lines = text.splitlines()
        parsed_fields: List[Tuple[Optional[str], str]] = []
        skip_patterns = [
            r"^\d+\.\d+\.\d+$",
            r"^---",
            r"^QTC",
            r"^[A-Z\s\d\.\$]+$",
        ]
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if any(re.match(pattern, line, re.IGNORECASE) for pattern in skip_patterns):
                continue
            match = re.match(r"^:?([a-zA-Z0-9]+):\d*\s*(.*)$", line)
            if match:
                key, value = match.groups()
                key = key.strip().lower()
                value = value.strip()
                if key in {"hdr_fm", "hdr_ed"}:
                    continue
                parsed_fields.append((key.upper(), value))
            else:
                if len(line) > 20:
                    parsed_fields.append((None, line))
        html_out = [
            "<style>",
            ".field-table { width: 100%; }",
            ".field-row { border-bottom: 1px solid; }",
            ".field-cell { padding: 4px; vertical-align: top; }",
            ".label { font-weight: bold; min-width: 80px; display: inline-block; }",
            ".long-text { white-space: pre-wrap; }",
            "</style>",
            "<table class='field-table'>",
        ]
        for i in range(len(parsed_fields)):
            key, value = parsed_fields[i]
            is_last_field = i == len(parsed_fields) - 1
            is_long_text = key is None or "\n" in value or len(value) > 100 or is_last_field
            safe_val = html.escape(value)
            if is_long_text:
                html_out.append("<tr class='field-row'>")
                html_out.append("<td colspan='2' class='field-cell long-text'>")
                if key:
                    html_out.append(f"<span class='label'>{html.escape(key)}:</span><br>")
                html_out.append(safe_val)
                html_out.append("</td></tr>")
            else:
                html_out.append("<tr class='field-row'>")
                html_out.append(
                    f"<td class='field-cell'><span class='label'>{html.escape(key)}:</span> {safe_val}</td>"
                )
                html_out.append("</tr>")
        html_out.append("</table>")
        return "".join(html_out)

    def _load_content(self, rec: FileRecord):
        log.debug("MessageViewer: loading file %s", rec.path)
        try:
            data = rec.path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            self.viewer.setPlainText(f"Failed to read file:\n{e}")
            return

        content = data
        is_html = False
        ext = rec.path.suffix.lower()
        if ext in {".html", ".htm"}:
            is_html = True
        elif ext in {".b2s", ".k2s"}:
            lower = data.lower()
            form_name = self._extract_custom_form_name(data)
            forms_dir = self._resolve_custom_forms_path()
            if form_name and forms_dir:
                template_path = forms_dir / form_name
                if template_path.exists():
                    log.debug(
                        "MessageViewer: rendering custom form %s for %s",
                        template_path.name,
                        rec.path.name,
                    )
                    try:
                        template = template_path.read_text(encoding="utf-8", errors="replace")
                        fields = self._parse_custom_form_fields(data)
                        log.debug(
                            "MessageViewer: custom form fields %s for %s",
                            ", ".join(sorted(fields.keys())),
                            rec.path.name,
                        )
                        title = self._extract_title_from_template(template)
                        labels = self._extract_template_labels(template)
                        content = self._render_custom_form_fields(fields, labels, title)
                        is_html = True
                    except Exception:
                        is_html = False
                else:
                    log.debug(
                        "MessageViewer: custom form template missing %s for %s",
                        template_path,
                        rec.path.name,
                    )
            if not is_html:
                if "<blankform>" in lower or "blank_form_v5." in lower:
                    log.debug("MessageViewer: parsed blank form for %s", rec.path.name)
                    content = self._parse_blank_form_content(data)
                    is_html = True
                elif "sitrep_v5." in lower:
                    log.debug("MessageViewer: parsed sitrep form for %s", rec.path.name)
                    content = self._parse_sitrep_content(data)
                    is_html = True
                elif "statrep_v5.1" in lower:
                    log.debug("MessageViewer: parsed statrep form for %s", rec.path.name)
                    content = self._parse_statrep_content(data)
                    is_html = True
                elif ext == ".b2s":
                    log.debug("MessageViewer: parsed b2s form for %s", rec.path.name)
                    content = self._parse_b2s_form_content(data)
                    is_html = True
                else:
                    log.debug("MessageViewer: parsed unknown form for %s", rec.path.name)
                    content = self._parse_unknown_content(data)
                    is_html = True

        if not is_html:
            try:
                if ext in {".json"}:
                    parsed = json.loads(data)
                    content = json.dumps(parsed, indent=2)
                elif ext in {".xml"}:
                    dom = xml.dom.minidom.parseString(data.encode("utf-8"))
                    content = dom.toprettyxml()
            except Exception:
                content = data  # fallback to raw

        info = f"{rec.path.name} - {rec.origin.upper()} - {rec.size} bytes - {self._fmt_mtime(rec.mtime)}"
        self.info_label.setText(info)
        if is_html:
            self.viewer.setAcceptRichText(True)
            self.viewer.setHtml(content)
        else:
            self.viewer.setAcceptRichText(False)
            self.viewer.setPlainText(content)

    def _load_js8_content(self, msg: JS8Message):
        header = [
            f"FROM: {msg.from_call}",
            f"TO:   {msg.to_call}",
            f"TYPE: {msg.msg_type}",
            f"UTC:  {msg.utc_str}",
            "",
        ]
        body = msg.decoded_text or msg.raw_text
        self.info_label.setText(f"{msg.msg_type} {msg.from_call} -> {msg.to_call}")
        self.viewer.setAcceptRichText(False)
        self.viewer.setPlainText("\n".join(header + [body]))

    def _fmt_mtime(self, mtime: float) -> str:
        try:
            from datetime import datetime

            return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return ""

    def _inbox_path(self) -> Path | None:
        directed = (self.settings.get("js8_directed_path", "") or "").strip()
        if not directed:
            return None
        p = Path(directed)
        candidates = [
            p.parent / "inbox_v1",
            p.parent / "inbox_v1.sqlite",
            p.parent / "inbox_v1.db",
            p.parent / "inbox.db3",
        ]
        for c in candidates:
            if c.exists():
                return c
        # Last resort: first file starting with inbox
        for c in p.parent.glob("inbox*"):
            if c.is_file():
                return c
        return candidates[0]

    def _local_js8_db(self) -> Path | None:
        try:
            root = Path(__file__).resolve().parents[2]
            from freqinout.core.config_paths import get_config_dir

            return get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.debug("MessageViewer: failed to resolve local JS8 DB path: %s", e)
            return None

    def _table_has_column(self, conn: sqlite3.Connection, table: str, column: str) -> bool:
        try:
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table})")
            rows = cur.fetchall()
            return any((r[1] or "").lower() == column.lower() for r in rows)
        except Exception:
            return False

    def _mark_js8call_inbox_read(self, callsign: str, msg_id: str) -> bool:
        inbox_path = self._inbox_path()
        if not inbox_path or not inbox_path.exists():
            return False
        callsign = (callsign or "").strip().upper()
        msg_id = (msg_id or "").strip()
        if not callsign or not msg_id:
            return False
        like_id = f'%\"_ID\":\"{msg_id}\"%'
        like_from = f'%\"FROM\":\"{callsign}\"%'
        candidates = [
            ("inbox_v1", "blob"),
            ("inbox_v1", "json"),
            ("inbox_v1", "message"),
            ("inbox", "blob"),
            ("inbox", "json"),
            ("inbox", "message"),
        ]
        try:
            conn = sqlite3.connect(inbox_path, timeout=1.0)
            cur = conn.cursor()
            cur.execute("PRAGMA busy_timeout = 1000")
            for table, col in candidates:
                try:
                    cur.execute(f"SELECT id, {col} FROM {table} WHERE {col} LIKE ?", (like_from,))
                    rows = cur.fetchall()
                except Exception:
                    continue
                if not rows:
                    continue
                has_type_col = self._table_has_column(conn, table, "type")
                matched_row = self._select_inbox_row(rows, callsign, msg_id)
                if matched_row is None:
                    continue
                row_id, parsed = matched_row
                current_type = str(parsed.get("type", "") or "").strip().upper()
                if current_type == "DELIVERED":
                    log.debug(
                        "MessageViewer: JS8Call inbox row %s skipped (DELIVERED) callsign=%s msg_id=%s",
                        row_id,
                        callsign,
                        msg_id,
                    )
                    conn.close()
                    return False
                if current_type != "READ":
                    parsed["type"] = "READ"
                    new_blob = json.dumps(parsed, separators=(",", ":"))
                    if has_type_col:
                        cur.execute(
                            f"UPDATE {table} SET {col}=?, type=? WHERE id=?",
                            (new_blob, "READ", row_id),
                        )
                    else:
                        cur.execute(
                            f"UPDATE {table} SET {col}=? WHERE id=?",
                            (new_blob, row_id),
                        )
                    conn.commit()
                    conn.close()
                    log.debug(
                        "MessageViewer: JS8Call inbox row %s marked READ callsign=%s msg_id=%s",
                        row_id,
                        callsign,
                        msg_id,
                    )
                    return True
                conn.close()
                log.debug(
                    "MessageViewer: JS8Call inbox row %s already READ callsign=%s msg_id=%s",
                    row_id,
                    callsign,
                    msg_id,
                )
                return True
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: JS8Call inbox update failed: %s", e)
        return False

    def _mark_js8call_inbox_read_by_id(self, row_id: int) -> bool:
        inbox_path = self._inbox_path()
        if not inbox_path or not inbox_path.exists():
            return False
        if row_id is None:
            return False
        candidates = [
            ("inbox_v1", "blob"),
            ("inbox_v1", "json"),
            ("inbox_v1", "message"),
            ("inbox", "blob"),
            ("inbox", "json"),
            ("inbox", "message"),
        ]
        try:
            conn = sqlite3.connect(inbox_path, timeout=1.0)
            cur = conn.cursor()
            cur.execute("PRAGMA busy_timeout = 1000")
            for table, col in candidates:
                rows = []
                try:
                    cur.execute(f"SELECT id, {col} FROM {table} WHERE id=?", (int(row_id),))
                    rows = cur.fetchall()
                except Exception:
                    try:
                        cur.execute(f"SELECT rowid as id, {col} FROM {table} WHERE rowid=?", (int(row_id),))
                        rows = cur.fetchall()
                    except Exception:
                        rows = []
                if not rows:
                    continue
                has_type_col = self._table_has_column(conn, table, "type")
                row = rows[0]
                blob = row[1] or ""
                try:
                    parsed = json.loads(blob)
                except Exception:
                    continue
                if not isinstance(parsed, dict):
                    continue
                current_type = str(parsed.get("type", "") or "").strip().upper()
                if current_type == "DELIVERED":
                    conn.close()
                    return False
                if current_type != "READ":
                    parsed["type"] = "READ"
                    new_blob = json.dumps(parsed, separators=(",", ":"))
                    if has_type_col:
                        cur.execute(
                            f"UPDATE {table} SET {col}=?, type=? WHERE id=?",
                            (new_blob, "READ", int(row_id)),
                        )
                    else:
                        cur.execute(
                            f"UPDATE {table} SET {col}=? WHERE id=?",
                            (new_blob, int(row_id)),
                        )
                    conn.commit()
                conn.close()
                return True
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: JS8Call inbox update by id failed: %s", e)
        return False

    def _select_inbox_row(
        self, rows: List[tuple], callsign: str, msg_id: str
    ) -> tuple[int, Dict] | None:
        """
        Match inbox rows by FROM + UTC time window + TEXT tie-breaker.
        msg_id is assumed to be the directed.txt message id.
        """
        from datetime import datetime

        call = (callsign or "").strip().upper()
        target_ts, target_text = self._directed_msg_info(call, msg_id)
        if target_ts is None:
            log.debug(
                "MessageViewer: no directed timestamp for callsign=%s msg_id=%s",
                call,
                msg_id,
            )
        window = 180.0  # seconds
        candidates: List[tuple[int, Dict, float, str]] = []
        for row in rows:
            row_id = row[0]
            blob = row[1] or ""
            try:
                parsed = json.loads(blob)
            except Exception:
                continue
            if not isinstance(parsed, dict):
                continue
            params = parsed.get("params") if isinstance(parsed.get("params"), dict) else parsed
            if not isinstance(params, dict):
                continue
            from_call = (params.get("FROM") or "").strip().upper()
            if from_call != call:
                continue
            blob_id = (params.get("_ID") or "").strip()
            if blob_id and blob_id == msg_id:
                return int(row_id), parsed
            utc_str = (params.get("UTC") or "").strip()
            try:
                utc_ts = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception:
                utc_ts = 0.0
            if target_ts is not None:
                if not utc_ts or abs(utc_ts - target_ts) > window:
                    continue
            text = (params.get("TEXT") or "").strip()
            candidates.append((int(row_id), parsed, utc_ts, text))

        if not candidates:
            log.debug(
                "MessageViewer: no inbox candidates for callsign=%s msg_id=%s",
                call,
                msg_id,
            )
            return None
        if len(candidates) == 1:
            return candidates[0][0], candidates[0][1]

        # If multiple matches, prefer closest timestamp
        if target_ts is not None:
            candidates.sort(key=lambda c: abs(c[2] - target_ts))
            best = candidates[0]
            # If top candidate is unique by timestamp, use it
            if len(candidates) == 1 or abs(candidates[1][2] - target_ts) > 1:
                return best[0], best[1]

        # As tie-breaker, attempt exact TEXT match to directed.txt line
        if target_text:
            exact = [c for c in candidates if c[3] == target_text]
            if len(exact) == 1:
                return exact[0][0], exact[0][1]
            if len(exact) > 1:
                return exact[0][0], exact[0][1]
            lower = target_text.lower()
            fuzzy = [c for c in candidates if c[3].lower() == lower]
            if len(fuzzy) >= 1:
                return fuzzy[0][0], fuzzy[0][1]
        else:
            log.debug(
                "MessageViewer: no directed text for callsign=%s msg_id=%s",
                call,
                msg_id,
            )

        # Fall back to most recent by UTC
        candidates.sort(key=lambda c: c[2], reverse=True)
        return candidates[0][0], candidates[0][1]

    def _directed_msg_info(self, callsign: str, msg_id: str) -> tuple[float | None, str | None]:
        directed = (self.settings.get("js8_directed_path", "") or "").strip()
        if not directed:
            return None, None
        path = Path(directed)
        if not path.exists():
            return None, None
        call = (callsign or "").strip().upper()
        msg_id = (msg_id or "").strip()
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            return None, None
        import re
        from datetime import datetime

        ts_re = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})")
        mycall = self._my_callsign()
        yes_re = re.compile(rf"{call}:\s+{mycall}\s+YES\s+MSG\s+ID\s+{re.escape(msg_id)}\b")
        msg_re = re.compile(rf"{call}:\s+{mycall}\s+MSG\s+", re.IGNORECASE)

        yes_idx = None
        yes_ts = None
        for idx, line in enumerate(lines):
            if not yes_re.search(line):
                continue
            m = ts_re.match(line.strip())
            if not m:
                continue
            ts_txt = f"{m.group(1)} {m.group(2)}"
            try:
                yes_ts = datetime.strptime(ts_txt, "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception:
                yes_ts = None
            yes_idx = idx
            break

        search_start = yes_idx + 1 if yes_idx is not None else 0
        for line in lines[search_start:]:
            if not msg_re.search(line):
                continue
            m = ts_re.match(line.strip())
            if not m:
                continue
            ts_txt = f"{m.group(1)} {m.group(2)}"
            try:
                msg_ts = datetime.strptime(ts_txt, "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception:
                msg_ts = None
            # Extract text after "MSG "
            text = ""
            try:
                text = re.split(rf"{call}:\s+{mycall}\s+MSG\s+", line, maxsplit=1, flags=re.IGNORECASE)[1].strip()
            except Exception:
                text = line.strip()
            return msg_ts, text

        return yes_ts, None

    # ---------- JS8 Helpers ----------

    def _mark_js8_read(self, msg: JS8Message):
        if msg.state.upper() == "READ":
            return
        ts = time.time()
        # Persist read state in local app DB
        try:
            self._save_js8_state(msg.msg_id, "READ", msg.utc_ts, read_ts=ts)
            self._update_local_read(msg.msg_id, ts)
        except Exception as e:
            log.debug("MessageViewer: failed to persist JS8 READ state: %s", e)
        if self.settings.get("js8_inbox_mark_retrieved_sync", False):
            ok = self._mark_js8call_inbox_read_by_id(msg.msg_id)
            if not ok:
                log.debug(
                    "MessageViewer: JS8Call inbox mark READ failed (msg_id=%s)",
                    msg.msg_id,
                )
        msg.state = "READ"
        msg.read_ts = ts
        self._refresh_table_after_read(
            lambda row: isinstance(row.payload, JS8Message) and row.payload.msg_id == msg.msg_id
        )

    def _decode_form(self, form_id: str, responses: str, comment: str, raw: str = "") -> str:
        form_id = form_id.strip()
        if not form_id:
            return raw or responses
        form = self._load_form_definition(form_id)
        if not form:
            return raw or responses
        out_lines: List[str] = []
        for idx, q in enumerate(form):
            question = q.get("q", "").strip()
            answers = q.get("ans", {})
            out_lines.append(question)
            if idx < len(responses):
                code = responses[idx]
                ans = answers.get(code, f"(unknown: {code})")
                out_lines.append(ans)
            else:
                out_lines.append("(no response)")
            out_lines.append("")  # spacer
        if comment:
            out_lines.append("Comment:")
            out_lines.append(comment.strip())
        return "\n".join(out_lines).strip() or (raw or responses)

    @staticmethod
    def _parse_form_parts(text: str) -> tuple[str, str, str]:
        """
        Split an F!### message into (form_id, response_string, comment)
        """
        parts = (text or "").split()
        if not parts or not parts[0].startswith("F!"):
            return "", "", ""
        form_part = parts[0][2:] if len(parts[0]) > 2 else ""
        resp = parts[1] if len(parts) > 1 else ""
        comment = " ".join(parts[2:]) if len(parts) > 2 else ""
        return form_part, resp, comment

    def _load_form_definition(self, form_id: str) -> List[Dict]:
        if form_id in self._form_cache:
            return self._form_cache[form_id]
        forms_dir = (self.settings.get("js8_forms_path", self.forms_path) or "").strip()
        if not forms_dir:
            return []
        path = Path(forms_dir) / f"MCF{form_id}.txt"
        if not path.exists():
            return []
        questions: List[Dict] = []
        current_q = None
        try:
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("."):
                    continue
                if line.startswith("?"):
                    if current_q:
                        questions.append(current_q)
                    current_q = {"q": line[1:].strip(), "ans": {}}
                elif line.startswith("@") and current_q:
                    try:
                        key, text = line[1], line[2:].strip()
                        current_q["ans"][key] = text
                    except Exception:
                        continue
            if current_q:
                questions.append(current_q)
        except Exception as e:
            log.debug("MessageViewer: failed to parse form %s: %s", form_id, e)
            questions = []
        self._form_cache[form_id] = questions
        return questions

    # ---------- JS8 state persistence (local DB) ---------- #

    def _load_js8_state_map(self) -> Dict[int, Tuple[str, float]]:
        db_path = self._local_js8_db()
        if not db_path or not db_path.exists():
            return {}
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL, read_ts REAL, last_ingested_id INTEGER)"
            )
            cur.execute("SELECT id, state, read_ts FROM js8_inbox_state")
            rows = cur.fetchall()
            conn.close()
            return {int(r[0]): ((r[1] or "").upper(), float(r[2] or 0.0)) for r in rows if r and r[0] is not None}
        except Exception as e:
            log.debug("MessageViewer: failed to load js8 state map: %s", e)
            return {}

    def _save_js8_state(self, msg_id: int, state: str, last_seen_ts: float = 0.0, read_ts: float = 0.0) -> None:
        db_path = self._local_js8_db()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL, read_ts REAL, last_ingested_id INTEGER)"
            )
            cur.execute(
                "INSERT INTO js8_inbox_state (id, state, last_seen, read_ts) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET state=excluded.state, last_seen=excluded.last_seen, read_ts=excluded.read_ts",
                (int(msg_id), state.upper(), float(last_seen_ts or 0.0), float(read_ts or 0.0)),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to save js8 state: %s", e)

    # ---------- JS8 message cache (local) ---------- #

    def _ensure_local_js8_tables(self) -> None:
        db_path = self._local_js8_db()
        if not db_path:
            return
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS js8_messages (
                id INTEGER PRIMARY KEY,
                from_call TEXT,
                to_call TEXT,
                msg_type TEXT,
                utc_str TEXT,
                utc_ts REAL,
                raw_text TEXT,
                decoded_text TEXT,
                state TEXT,
                read_ts REAL
            )
            """
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL, read_ts REAL, last_ingested_id INTEGER)"
        )
        # Add columns if missing
        try:
            cur.execute("ALTER TABLE js8_messages ADD COLUMN read_ts REAL")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE js8_inbox_state ADD COLUMN read_ts REAL")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE js8_inbox_state ADD COLUMN last_ingested_id INTEGER")
        except Exception:
            pass
        conn.commit()
        conn.close()

    def _local_max_js8_id(self) -> int:
        db_path = self._local_js8_db()
        if not db_path or not db_path.exists():
            return 0
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT MAX(id) FROM js8_messages")
            row = cur.fetchone()
            conn.close()
            return int(row[0]) if row and row[0] is not None else 0
        except Exception:
            return 0

    def _insert_js8_local(self, msg: JS8Message) -> None:
        db_path = self._local_js8_db()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO js8_messages (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text, state, read_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO NOTHING
                """,
                (
                    msg.msg_id,
                    msg.from_call,
                    msg.to_call,
                    msg.msg_type,
                    msg.utc_str,
                    msg.utc_ts,
                    msg.raw_text,
                    msg.decoded_text,
                    msg.state,
                    msg.read_ts,
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to insert local js8 message: %s", e)

    def _update_local_decoded(self, msg_id: int, decoded: str) -> None:
        db_path = self._local_js8_db()
        if not db_path or not Path(db_path).exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("UPDATE js8_messages SET decoded_text=? WHERE id=?", (decoded, int(msg_id)))
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to update local decoded text: %s", e)

    def _update_local_read(self, msg_id: int, read_ts: float) -> None:
        db_path = self._local_js8_db()
        if not db_path or not Path(db_path).exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("UPDATE js8_messages SET state='READ', read_ts=? WHERE id=?", (float(read_ts), int(msg_id)))
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to update local read state: %s", e)

    def _load_js8_from_local(self, force: bool = False) -> None:
        self._ensure_local_js8_tables()
        db_path = self._local_js8_db()
        msgs: List[JS8Message] = []
        if not db_path or not Path(db_path).exists():
            self.js8_messages = msgs
            self._populate_messages_table(force=force)
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text, state, read_ts
                FROM js8_messages
                WHERE utc_ts IS NULL OR utc_ts >= ?
                """,
                (time.time() - JS8_MAX_AGE_SECONDS,),
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load local js8 messages: %s", e)
            rows = []
        for r in rows:
            msg = JS8Message(
                msg_id=int(r[0]),
                from_call=(r[1] or ""),
                to_call=(r[2] or ""),
                msg_type=(r[3] or ""),
                utc_str=(r[4] or ""),
                utc_ts=float(r[5] or 0.0),
                raw_text=(r[6] or ""),
                decoded_text=(r[7] or ""),
                state=(r[8] or "UNREAD").upper(),
                read_ts=float(r[9] or 0.0),
            )
            # If older than retention and read, skip
            now_ts = time.time()
            if msg.state == "READ" and msg.read_ts and (now_ts - msg.read_ts) > (24 * 60 * 60):
                continue
            # Re-decode forms if previously stored without decoded text (e.g., forms path was missing)
            if msg.msg_type.startswith("F!") and (not msg.decoded_text or msg.decoded_text == msg.raw_text):
                form_id, resp, comment = self._parse_form_parts(msg.raw_text)
                if form_id:
                    new_decoded = self._decode_form(form_id, resp, comment, raw=msg.raw_text)
                    if new_decoded:
                        msg.decoded_text = new_decoded
                        self._update_local_decoded(msg.msg_id, new_decoded)
            msgs.append(msg)
        msgs.sort(key=lambda m: (m.state != "UNREAD", m.utc_ts))
        self.js8_messages = msgs
        self._populate_messages_table(force=force)

    def _ingest_js8_messages(self) -> None:
        inbox_path = self._inbox_path()
        if not inbox_path or not inbox_path.exists():
            return
        self._ensure_local_js8_tables()
        max_local_id = self._local_max_js8_id()
        try:
            conn = sqlite3.connect(inbox_path)
            cur = conn.cursor()
            queries = [
                ("inbox_v1", "id, json, type, value"),
                ("inbox_v1", "rowid as id, json, type, value"),
                ("inbox_v1", "id, message, type, value"),
                ("inbox_v1", "id, blob"),
                ("inbox", "id, json, type, value"),
                ("inbox", "rowid as id, json, type, value"),
                ("inbox", "id, message, type, value"),
            ]
            rows = []
            for table, cols in queries:
                try:
                    cur.execute(f"SELECT {cols} FROM {table} WHERE id > ?", (max_local_id,))
                    rows = cur.fetchall()
                    break
                except Exception:
                    rows = []
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: JS8 ingest read failed: %s", e)
            rows = []

        state_map = self._load_js8_state_map()
        now_ts = time.time()
        for row in rows:
            rid = row[0] if len(row) > 0 else 0
            if rid <= max_local_id:
                continue
            blob = row[1] if len(row) > 1 else ""
            state = row[2] if len(row) > 2 else ""
            js = blob
            try:
                parsed = json.loads(js or "{}")
                if "params" not in parsed and len(row) >= 4:
                    parsed = {"params": parsed, "type": row[2] if len(row) > 2 else "", "value": row[3] if len(row) > 3 else ""}
                params = parsed.get("params", {})
                if not state:
                    state = parsed.get("type", "") or parsed.get("TYPE", "")
            except Exception:
                params = {}
            text = (params.get("TEXT") or "").strip()
            from_call = (params.get("FROM") or "").strip().upper()
            to_call = (params.get("TO") or "").strip()
            utc_str = (params.get("UTC") or "").strip()
            try:
                from datetime import datetime

                utc_ts = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception:
                utc_ts = 0.0
            if utc_ts and (now_ts - utc_ts) > JS8_MAX_AGE_SECONDS:
                continue
            msg_type = "MSG"
            decoded = text
            if text.startswith("F!"):
                parts = text.split()
                form_part = parts[0][2:] if parts else ""
                resp = parts[1] if len(parts) > 1 else ""
                comment = " ".join(parts[2:]) if len(parts) > 2 else ""
                msg_type = f"F!{form_part}" if form_part else "MSG"
                decoded = self._decode_form(form_part, resp, comment, raw=text)
            # Apply stored state if present
            saved_state = state_map.get(rid)
            if saved_state:
                eff_state = saved_state[0]
                read_ts = saved_state[1]
            else:
                eff_state = (state or "").upper() or "UNREAD"
                read_ts = 0.0
            msg = JS8Message(
                msg_id=rid,
                from_call=from_call,
                to_call=to_call,
                msg_type=msg_type,
                utc_str=utc_str,
                utc_ts=utc_ts,
                raw_text=text,
                decoded_text=decoded,
                state=eff_state,
                read_ts=read_ts,
            )
            self._insert_js8_local(msg)
            try:
                self._enqueue_next_msg_id(from_call, text)
            except Exception:
                pass

    def _enqueue_next_msg_id(self, from_call: str, text: str) -> None:
        """
        If message text contains "NEXT MSG ID ###", add it to autoquery_backlog.
        """
        import re

        call = (from_call or "").strip().upper()
        if not call or not text:
            return
        m = re.search(r"NEXT\s+MSG\s+ID\s+(\d+)", text.upper())
        if not m:
            return
        next_id = m.group(1)
        if not next_id:
            return
        self._ensure_backlog_table()
        db_path = self._backlog_db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1 FROM autoquery_backlog
                WHERE callsign=? AND COALESCE(msg_id,'')=COALESCE(?, '') AND kind='MSG'
                LIMIT 1
                """,
                (call, next_id),
            )
            if cur.fetchone():
                conn.close()
                return
            now_ts = time.time()
            cur.execute(
                """
                INSERT INTO autoquery_backlog (callsign, msg_id, kind, status, attempts, last_attempt_ts, created_ts)
                VALUES (?, ?, 'MSG', 'PENDING', 0, ?, ?)
                """,
                (call, next_id, now_ts, now_ts),
            )
            conn.commit()
            conn.close()
            log.debug("MessageViewer: queued NEXT MSG ID %s for %s", next_id, call)
        except Exception as e:
            log.debug("MessageViewer: failed to enqueue NEXT MSG ID: %s", e)

    # ---------- Actions ----------

    def _export_pdf(self):
        if not self.current_record:
            return
        text = self.viewer.toPlainText()
        if not text.strip():
            return
        fn, _ = QFileDialog.getSaveFileName(self, "Export to PDF", self.current_record.path.stem + ".pdf", "PDF Files (*.pdf)")
        if not fn:
            return
        try:
            import textwrap

            c = canvas.Canvas(fn, pagesize=letter)
            c.setFont("Helvetica", 12)
            width, height = letter
            margin = 50
            usable_width = width - 2 * margin
            line_height = 14
            # Roughly estimate characters per line at 12pt Helvetica (~6.5 px avg)
            max_chars = max(40, int(usable_width / 6.5))
            y = height - margin
            for raw_line in text.splitlines():
                wrapped = textwrap.wrap(raw_line, max_chars) or [""]
                for line in wrapped:
                    c.drawString(margin, y, line)
                    y -= line_height
                    if y < margin:
                        c.showPage()
                        c.setFont("Helvetica", 12)
                        y = height - margin
            c.save()
            log.info("MessageViewer: exported PDF to %s", fn)
        except Exception as e:
            log.error("MessageViewer: PDF export failed: %s", e)

    # ---------- Settings ----------

    def _save_settings(self):
        try:
            data = self.settings.get("message_viewer", {}) or {}
            # Persist only legacy scan interval; paths now come from Settings tab
            data["scan_minutes"] = self.scan_minutes
            if hasattr(self.settings, "set"):
                self.settings.set("message_viewer", data)
                if hasattr(self.settings, "save"):
                    self.settings.save()
        except Exception as e:
            log.error("MessageViewer: failed to save settings: %s", e)
