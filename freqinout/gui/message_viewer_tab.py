from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import xml.dom.minidom
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from PySide6.QtCore import Qt, QTimer, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QListWidget,
    QListWidgetItem,
    QTextEdit,
    QFileDialog,
    QGroupBox,
    QFormLayout,
    QComboBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QAbstractItemView,
    QMessageBox,
    QSizePolicy,
    QAbstractScrollArea,
)

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.gui.theme import resolve_theme, button_style


SUPPORTED_EXT = {".b2s", ".k2s", ".txt", ".ff", ".xml", ".json", ".html", ".htm"}

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
        return f"{self.display_name()} — {self.size} bytes"


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


class MessageViewerTab(QWidget):
    """
    Message Viewer for VarAC / FLMSG / FLAMP inbox-like folders.

    - Watches configured folders by origin
    - Lists files per origin; shows content preview
    - Open externally and export to PDF
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

        # merge DB paths if present
        self._load_watch_dirs_from_db()
        self._clear_backlog_on_upgrade()

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
        self.refresh_btn.clicked.connect(self._refresh_files)
        header.addWidget(self.refresh_btn)

        self.open_btn = QPushButton("Open Externally")
        self.open_btn.clicked.connect(self._open_external)
        header.addWidget(self.open_btn)

        self.export_btn = QPushButton("Export to PDF")
        self.export_btn.clicked.connect(self._export_pdf)
        header.addWidget(self.export_btn)

        layout.addLayout(header)

        # Split left/right
        body = QHBoxLayout()
        layout.addLayout(body)

        left_widget = QWidget()
        left_widget.setMaximumWidth(450)
        left = QVBoxLayout(left_widget)
        body.addWidget(left_widget, 1)

        self.list_js8 = self._make_list_section(left, "JS8 Messages", "js8", allow_paths=False)
        self.list_flmsg = self._make_list_section(left, "FLMSG Files", "flmsg", allow_paths=False)
        self.list_flamp = self._make_list_section(left, "FLAMP Files", "flamp", allow_paths=False)
        self.list_varac = self._make_list_section(left, "VarAC Files", "varac", allow_paths=False)

        right = QVBoxLayout()
        body.addLayout(right, 3)

        pending_box = QGroupBox("Pending JS8 MSGs")
        pending_layout = QVBoxLayout()
        pending_header = QHBoxLayout()
        self.pending_count = QLabel("0 pending")
        pending_header.addWidget(self.pending_count)
        pending_header.addStretch()
        self.pending_clear_btn = QPushButton("Clear All")
        self.pending_clear_btn.clicked.connect(self._clear_pending_backlog)
        pending_header.addWidget(self.pending_clear_btn)
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
        right.addWidget(pending_box)

        self.info_label = QLabel("No file selected")
        self.info_label.setStyleSheet("font-weight: bold;")
        right.addWidget(self.info_label)

        self.viewer = QTextEdit()
        self.viewer.setReadOnly(True)
        self.viewer.setAcceptRichText(False)
        right.addWidget(self.viewer, 1)

    def _make_list_section(self, parent_layout: QVBoxLayout, title: str, origin: str, allow_paths: bool = True, allow_remove: bool = True) -> QListWidget:
        box = QGroupBox(title)
        v = QVBoxLayout()
        lst = QListWidget()
        lst.itemSelectionChanged.connect(self._on_selection_changed)
        lst.itemClicked.connect(lambda it, l=lst: self._on_selection_changed(item=it, sender_override=l))
        lst.setSelectionMode(QListWidget.SingleSelection)
        v.addWidget(lst)
        if allow_paths:
            # Paths controls under the list
            row = QHBoxLayout()
            self.paths_labels[origin] = QLabel("")
            self.paths_labels[origin].setWordWrap(False)
            row.addWidget(self.paths_labels[origin], 1)
            add_btn = QPushButton("Browse")
            add_btn.clicked.connect(lambda _, o=origin: self._add_path(o))
            row.addWidget(add_btn)
            if allow_remove:
                rem_btn = QPushButton("Remove Selected Path")
                rem_btn.clicked.connect(lambda _, o=origin: self._remove_path(o))
                row.addWidget(rem_btn)
            v.addLayout(row)
        box.setLayout(v)
        parent_layout.addWidget(box)
        return lst

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

    def _refresh_files(self):
        self._load_paths_lists()
        records: Dict[str, List[FileRecord]] = {"varac": [], "flmsg": [], "flamp": []}
        for entry in self.watch_dirs:
            origin = entry.get("origin", "unknown")
            if origin not in records:
                continue
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
        self._populate_lists()

    def _refresh_js8_messages(self):
        # First ingest any new messages into local cache, then load from local cache for display
        try:
            self._ingest_js8_messages()
        except Exception as e:
            log.debug("MessageViewer: JS8 ingest failed: %s", e)
        try:
            self._load_js8_from_local()
        except Exception as e:
            log.debug("MessageViewer: JS8 local load failed: %s", e)

    # ---------- Pending JS8 MSG backlog ---------- #

    def _refresh_pending_backlog(self) -> None:
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
        self._update_pending_table()

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

    def _clear_pending_backlog(self) -> None:
        confirm = QMessageBox.question(
            self,
            "Clear Pending",
            "Remove all pending JS8 MSG rows?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        db_path = self._backlog_db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM autoquery_backlog WHERE kind='MSG'")
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to clear pending backlog: %s", e)
        self._update_pending_table()

    def _on_pending_get(self, callsign: str, msg_id: str) -> None:
        if not callsign or not msg_id:
            return
        mycall = self._my_callsign()
        if not mycall:
            QMessageBox.warning(self, "Missing Callsign", "Configure your callsign in the Settings tab.")
            return
        text = f"{mycall}: {callsign} QUERY MSG {msg_id}".strip()
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
        payload = json.dumps({"params": {}, "type": "TX.SEND_MESSAGE", "value": text}) + "\n"
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

    def _populate_lists(self):
        mapping = {
            "varac": self.list_varac,
            "flmsg": self.list_flmsg,
            "flamp": self.list_flamp,
        }
        for origin, lst in mapping.items():
            lst.blockSignals(True)
            lst.clear()
            for rec in self.files.get(origin, []):
                item = QListWidgetItem(rec.display_name())
                item.setData(Qt.UserRole, rec)
                lst.addItem(item)
            lst.blockSignals(False)

        # JS8 messages
        if hasattr(self, "list_js8"):
            self.list_js8.blockSignals(True)
            self.list_js8.clear()
            for msg in self.js8_messages:
                item = QListWidgetItem(msg.display_line())
                item.setData(Qt.UserRole, msg)
                # visually indicate unread
                if msg.state.upper() == "UNREAD":
                    item.setForeground(Qt.red)
                self.list_js8.addItem(item)
            self.list_js8.blockSignals(False)

        self.info_label.setText("No file selected")
        self.viewer.clear()
        self.current_record = None
        self.current_js8 = None

    # ---------- Selection / Viewing ----------

    def _on_selection_changed(self, item: QListWidgetItem | None = None, sender_override: QListWidget | None = None):
        sender = sender_override or self.sender()
        if not isinstance(sender, QListWidget):
            return
        # Clear selection in other lists so only one message is highlighted
        for lst in [self.list_js8, self.list_flmsg, self.list_flamp, self.list_varac]:
            if lst is not sender:
                lst.blockSignals(True)
                lst.clearSelection()
                lst.blockSignals(False)
        if item is None:
            item = sender.currentItem()
        if not item:
            return
        rec = item.data(Qt.UserRole)
        if isinstance(rec, FileRecord):
            self.current_js8 = None
            self.current_record = rec
            self._load_content(rec)
        elif isinstance(rec, JS8Message):
            self.current_record = None
            self.current_js8 = rec
            self._load_js8_content(rec)
            self._mark_js8_read(rec)

    def _load_content(self, rec: FileRecord):
        try:
            data = rec.path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            self.viewer.setPlainText(f"Failed to read file:\n{e}")
            return

        # Pretty format for JSON/XML
        content = data
        try:
            if rec.path.suffix.lower() in {".json"}:
                parsed = json.loads(data)
                content = json.dumps(parsed, indent=2)
            elif rec.path.suffix.lower() in {".xml"}:
                dom = xml.dom.minidom.parseString(data.encode("utf-8"))
                content = dom.toprettyxml()
        except Exception:
            content = data  # fallback to raw

        info = f"{rec.path.name} — {rec.origin.upper()} — {rec.size} bytes — {self._fmt_mtime(rec.mtime)}"
        self.info_label.setText(info)
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
        # Persist read state in local app DB (do not modify JS8Call inbox)
        try:
            self._save_js8_state(msg.msg_id, "READ", msg.utc_ts, read_ts=ts)
            self._update_local_read(msg.msg_id, ts)
        except Exception as e:
            log.debug("MessageViewer: failed to persist JS8 READ state: %s", e)
        msg.state = "READ"
        msg.read_ts = ts
        self._populate_lists()

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

    def _load_js8_from_local(self) -> None:
        self._ensure_local_js8_tables()
        db_path = self._local_js8_db()
        msgs: List[JS8Message] = []
        if not db_path or not Path(db_path).exists():
            self.js8_messages = msgs
            self._populate_lists()
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
        self._populate_lists()

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

    def _open_external(self):
        if not self.current_record:
            return
        url = QUrl.fromLocalFile(str(self.current_record.path))
        QDesktopServices.openUrl(url)

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
