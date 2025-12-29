from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import xml.dom.minidom
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
)

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log


SUPPORTED_EXT = {".b2s", ".k2s", ".txt", ".ff", ".xml", ".json", ".html", ".htm"}

DEFAULT_WATCH_DIRS = [
    {"path": r"C:\VarAC", "origin": "varac"},
    {"path": r"C:\Users\HP\NBEMS.files\ICS\messages", "origin": "flmsg"},
    {"path": r"C:\Users\HP\NBEMS.files\FLAMP", "origin": "flamp"},
]

SCAN_CHOICES = [1, 15, 30, 60]  # minutes
JS8_POLL_SECONDS = 180  # 3 minutes


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
        self.watch_dirs: List[Dict] = cfg.get("watch_dirs") or DEFAULT_WATCH_DIRS
        self.scan_minutes: int = cfg.get("scan_minutes") or 15
        if self.scan_minutes not in SCAN_CHOICES:
            self.scan_minutes = 15

        self.js8_messages: List[JS8Message] = []
        self.current_js8: JS8Message | None = None
        self._js8_timer: QTimer | None = None
        self._form_cache: Dict[str, List[Dict]] = {}
        self.forms_path = (self.settings.get("js8_forms_path", "") or "").strip()

        # merge DB paths if present
        self._load_watch_dirs_from_db()

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

    # ---------- DB helpers ----------

    def _db_path(self) -> Path | None:
        try:
            root = Path(__file__).resolve().parents[2]
            return root / "config" / "freqinout_nets.db"
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

        self.save_btn = QPushButton("Save Settings")
        self.save_btn.clicked.connect(self._save_settings)
        header.addWidget(self.save_btn)

        layout.addLayout(header)

        # Split left/right
        body = QHBoxLayout()
        layout.addLayout(body)

        left_widget = QWidget()
        left_widget.setMaximumWidth(280)
        left = QVBoxLayout(left_widget)
        body.addWidget(left_widget, 1)

        self.list_varac = self._make_list_section(left, "VarAC Files", "varac")
        self.list_flmsg = self._make_list_section(left, "FLMSG Files", "flmsg")
        self.list_flamp = self._make_list_section(left, "FLAMP Files", "flamp")
        self.list_js8 = self._make_list_section(left, "JS8 Messages", "js8", allow_paths=False)

        right = QVBoxLayout()
        body.addLayout(right, 3)

        self.info_label = QLabel("No file selected")
        self.info_label.setStyleSheet("font-weight: bold;")
        right.addWidget(self.info_label)

        self.viewer = QTextEdit()
        self.viewer.setReadOnly(True)
        self.viewer.setAcceptRichText(False)
        right.addWidget(self.viewer, 1)

    def _make_list_section(self, parent_layout: QVBoxLayout, title: str, origin: str, allow_paths: bool = True) -> QListWidget:
        box = QGroupBox(title)
        v = QVBoxLayout()
        lst = QListWidget()
        lst.itemSelectionChanged.connect(self._on_selection_changed)
        v.addWidget(lst)
        if allow_paths:
            # Paths controls under the list
            row = QHBoxLayout()
            self.paths_labels[origin] = QLabel("")
            self.paths_labels[origin].setWordWrap(False)
            row.addWidget(self.paths_labels[origin], 1)
            add_btn = QPushButton("Browse")
            add_btn.clicked.connect(lambda _, o=origin: self._add_path(o))
            rem_btn = QPushButton("Remove Selected Path")
            rem_btn.clicked.connect(lambda _, o=origin: self._remove_path(o))
            row.addWidget(add_btn)
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
        msgs: List[JS8Message] = []
        inbox_path = self._inbox_path()
        if not inbox_path or not inbox_path.exists():
            self.js8_messages = msgs
            self._populate_lists()
            return
        try:
            conn = sqlite3.connect(inbox_path)
            cur = conn.cursor()
            # Try common inbox schemas
            queries = [
                "SELECT id, json, type, value FROM inbox",
                "SELECT rowid, json, type, value FROM inbox",
                "SELECT id, message, type, value FROM inbox",
            ]
            rows = []
            for q in queries:
                try:
                    cur.execute(q)
                    rows = cur.fetchall()
                    break
                except Exception:
                    rows = []
            conn.close()
            if not rows:
                self.js8_messages = msgs
                self._populate_lists()
                return
        except Exception as e:
            log.error("MessageViewer: failed to read JS8 inbox: %s", e)
            rows = []

        for rid, js, state, _val in rows:
            try:
                parsed = json.loads(js or "{}")
                params = parsed.get("params", {})
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
        msg_type = "MSG"
        decoded = text
        if text.startswith("F!"):
            parts = text.split()
            form_part = parts[0][2:] if parts else ""
            resp = parts[1] if len(parts) > 1 else ""
            comment = " ".join(parts[2:]) if len(parts) > 2 else ""
            msg_type = f"F!{form_part}" if form_part else "MSG"
            decoded = self._decode_form(form_part, resp, comment, raw=text)
            msgs.append(
                JS8Message(
                    msg_id=rid,
                    from_call=from_call,
                    to_call=to_call,
                    msg_type=msg_type,
                    utc_str=utc_str,
                    utc_ts=utc_ts,
                    raw_text=text,
                    decoded_text=decoded,
                    state=(state or "").upper(),
                )
            )

        msgs.sort(key=lambda m: (m.state != "UNREAD", m.utc_ts))
        self.js8_messages = msgs
        self._populate_lists()

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

    def _on_selection_changed(self):
        sender = self.sender()
        if not isinstance(sender, QListWidget):
            return
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
        ]
        for c in candidates:
            if c.exists():
                return c
        # Last resort: first file starting with inbox
        for c in p.parent.glob("inbox*"):
            if c.is_file():
                return c
        return candidates[0]

    # ---------- JS8 Helpers ----------

    def _mark_js8_read(self, msg: JS8Message):
        if msg.state.upper() == "READ":
            return
        inbox_path = self._inbox_path()
        if not inbox_path or not inbox_path.exists():
            return
        try:
            conn = sqlite3.connect(inbox_path)
            cur = conn.cursor()
            cur.execute("UPDATE inbox SET type='READ' WHERE id=?", (msg.msg_id,))
            conn.commit()
            conn.close()
            msg.state = "READ"
            self._populate_lists()
        except Exception as e:
            log.debug("MessageViewer: failed to mark JS8 message read: %s", e)

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
            c = canvas.Canvas(fn, pagesize=letter)
            width, height = letter
            margin = 50
            y = height - margin
            for line in text.splitlines():
                c.drawString(margin, y, line[:1500])
                y -= 14
                if y < margin:
                    c.showPage()
                    y = height - margin
            c.save()
            log.info("MessageViewer: exported PDF to %s", fn)
        except Exception as e:
            log.error("MessageViewer: PDF export failed: %s", e)

    # ---------- Settings ----------

    def _save_settings(self):
        try:
            data = self.settings.get("message_viewer", {}) or {}
            data["watch_dirs"] = self.watch_dirs
            data["scan_minutes"] = self.scan_minutes
            if hasattr(self.settings, "set"):
                self.settings.set("message_viewer", data)
                if hasattr(self.settings, "save"):
                    self.settings.save()
            self._save_paths_to_db()
        except Exception as e:
            log.error("MessageViewer: failed to save settings: %s", e)
