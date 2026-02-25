from __future__ import annotations

import html
import hashlib
import json
import os
import re
import sqlite3
import ctypes
import ctypes.wintypes
import datetime
import platform
import shutil
import subprocess
import xml.dom.minidom
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from PySide6.QtCore import Qt, QTimer, QAbstractTableModel, QModelIndex, QEvent, QRect, Signal, QObject, QThread
from PySide6.QtGui import QPainter, QColor, QPalette, QFont
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QProgressBar,
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
    QApplication,
    QCompleter,
    QSizePolicy,
    QAbstractScrollArea,
    QSplitter,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QStyle,
    QStyleOptionButton,
)

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.core.perf_metrics import emit_span, span as perf_span
from freqinout.utils.timezones import get_timezone
from freqinout.core.varac_ingest import ingest_varac
from freqinout.core.gpg_tools import (
    DEFAULT_INLINE_SIGNED_SUFFIXES,
    find_detached_signature,
    normalize_fingerprint,
    normalize_fingerprints,
    normalize_signature_name_suffixes,
    verify_file_with_discovery,
)
from freqinout.core.hash_tools import (
    existing_checksum_sidecars,
    normalize_trusted_hash_entries,
    verify_file_hash_against_registry,
    verify_file_hash_with_discovery,
)
from freqinout.gui.theme import resolve_theme, button_style
from freqinout.gui.qsy_helper import suspend_active, scheduler_enabled


IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp", ".raw"}
IMAGE_PREVIEW_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp"}
SUPPORTED_EXT = {
    ".b2s",
    ".k2s",
    ".txt",
    ".rtf",
    ".ff",
    ".xml",
    ".json",
    ".html",
    ".htm",
    *IMAGE_EXTS,
}
ORIGIN_EXTS = {
    "flmsg": {".b2s", ".k2s", ".txt", ".rtf", *IMAGE_EXTS},
    "flamp": {".b2s", ".k2s", ".txt", ".rtf"},
    "varac": {".txt", ".html", ".htm", ".b2s", ".k2s", *IMAGE_EXTS},
    "bbs": set(SUPPORTED_EXT),
}
FLAMP_AUTH_EXTS = {".b2s", ".k2s"}

DEFAULT_WATCH_DIRS = [
    {"path": r"C:\VarAC", "origin": "varac"},
    {"path": r"C:\Users\HP\NBEMS.files\ICS\messages", "origin": "flmsg"},
    {"path": r"C:\Users\HP\NBEMS.files\FLAMP", "origin": "flamp"},
]

SCAN_CHOICES = [1, 15, 30, 60]  # minutes
JS8_POLL_SECONDS = 90  # 90 seconds
PENDING_POLL_SECONDS = 30
JS8_MAX_AGE_SECONDS = 30 * 24 * 60 * 60  # 30 days
BBS_AUTO_ARCHIVE_INTERVAL_SECONDS = 24 * 60 * 60  # once daily max
BBS_AUTO_ARCHIVE_LAST_CHECK_KEY = "varac_bbs_auto_archive_last_check_ts"


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
    flag_state: int = 0

    def display_line(self) -> str:
        return f"{self.utc_str[:10]}  {self.msg_type}  {self.from_call} -> {self.to_call}"


@dataclass
class SpotterMessage:
    spotter_id: int
    from_call: str
    to_call: str
    msg_type: str  # "F!###"
    utc_str: str
    utc_ts: float
    raw_text: str
    decoded_text: str
    state: str  # UNREAD / READ
    read_ts: float = 0.0
    relay_via: str = ""
    flag_state: int = 0

    def display_line(self) -> str:
        return f"{self.utc_str[:10]}  {self.msg_type}  {self.from_call} -> {self.to_call}"


class _FileScanWorker(QObject):
    finished = Signal(object, bool)

    def __init__(
        self,
        watch_dirs: List[Dict],
        force: bool,
        base_records: Optional[Dict[str, List[FileRecord]]] = None,
        base_dir_mtimes: Optional[Dict[str, float]] = None,
    ):
        super().__init__()
        self._watch_dirs = list(watch_dirs)
        self._force = force
        self._base_records = base_records or {}
        self._base_dir_mtimes: Dict[str, float] = {}
        for k, v in (base_dir_mtimes or {}).items():
            try:
                self._base_dir_mtimes[self._norm_path(k)] = float(v)
            except Exception:
                continue
        self._roots_by_origin: Dict[str, List[str]] = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
        for entry in self._watch_dirs:
            origin = str(entry.get("origin", "") or "").strip().lower()
            path = str(entry.get("path", "") or "").strip()
            if origin in self._roots_by_origin and path:
                norm = self._norm_path(path)
                if norm not in self._roots_by_origin[origin]:
                    self._roots_by_origin[origin].append(norm)

    @staticmethod
    def _norm_path(path: str | Path) -> str:
        return os.path.normcase(os.path.normpath(str(path)))

    @staticmethod
    def _is_under(path_norm: str, root_norm: str) -> bool:
        if path_norm == root_norm:
            return True
        return path_norm.startswith(root_norm + os.sep)

    def _is_under_any(self, path_norm: str, roots: set[str] | List[str]) -> bool:
        for root_norm in roots:
            if self._is_under(path_norm, root_norm):
                return True
        return False

    def _empty_result(self) -> Dict[str, Dict[str, FileRecord]]:
        return {"varac": {}, "flmsg": {}, "flamp": {}, "bbs": {}}

    def _full_scan_recursive(
        self,
        base: Path,
        origin: str,
        allowed_exts: Optional[set[str]],
        out_map: Dict[str, Dict[str, FileRecord]],
        dir_mtimes: Dict[str, float],
    ) -> None:
        base_norm = self._norm_path(base)
        try:
            dir_mtimes[base_norm] = float(base.stat().st_mtime)
        except OSError:
            return
        try:
            with os.scandir(base) as it:
                for dent in it:
                    try:
                        if dent.is_dir(follow_symlinks=False):
                            self._full_scan_recursive(Path(dent.path), origin, allowed_exts, out_map, dir_mtimes)
                            continue
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        suffix = Path(dent.name).suffix.lower()
                        if suffix not in SUPPORTED_EXT:
                            continue
                        if allowed_exts and suffix not in allowed_exts:
                            continue
                        st = dent.stat()
                        rec = FileRecord(path=Path(dent.path), origin=origin, size=st.st_size, mtime=st.st_mtime)
                        out_map[origin][self._norm_path(rec.path)] = rec
                    except OSError:
                        continue
        except OSError:
            return

    def _full_scan_bbs(
        self,
        base: Path,
        out_map: Dict[str, Dict[str, FileRecord]],
        dir_mtimes: Dict[str, float],
    ) -> None:
        base_norm = self._norm_path(base)
        try:
            dir_mtimes[base_norm] = float(base.stat().st_mtime)
        except OSError:
            return
        try:
            with os.scandir(base) as it:
                for dent in it:
                    try:
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        suffix = Path(dent.name).suffix.lower()
                        if suffix not in SUPPORTED_EXT:
                            continue
                        st = dent.stat()
                        rec = FileRecord(path=Path(dent.path), origin="bbs", size=st.st_size, mtime=st.st_mtime)
                        out_map["bbs"][self._norm_path(rec.path)] = rec
                    except OSError:
                        continue
        except OSError:
            return

    def _scan_changed_recursive(
        self,
        base: Path,
        origin: str,
        allowed_exts: Optional[set[str]],
        out_map: Dict[str, Dict[str, FileRecord]],
        dir_mtimes: Dict[str, float],
        seen_files: Dict[str, set[str]],
        changed_dirs: Dict[str, set[str]],
        reused_dirs: Dict[str, set[str]],
    ) -> None:
        base_norm = self._norm_path(base)
        try:
            base_st = base.stat()
            cur_dir_mtime = float(base_st.st_mtime)
        except OSError:
            return
        dir_mtimes[base_norm] = cur_dir_mtime
        prev_mtime = self._base_dir_mtimes.get(base_norm)
        if prev_mtime is not None and abs(prev_mtime - cur_dir_mtime) < 1e-6:
            reused_dirs[origin].add(base_norm)
            return
        changed_dirs[origin].add(base_norm)
        try:
            with os.scandir(base) as it:
                for dent in it:
                    try:
                        if dent.is_dir(follow_symlinks=False):
                            self._scan_changed_recursive(
                                Path(dent.path),
                                origin,
                                allowed_exts,
                                out_map,
                                dir_mtimes,
                                seen_files,
                                changed_dirs,
                                reused_dirs,
                            )
                            continue
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        suffix = Path(dent.name).suffix.lower()
                        if suffix not in SUPPORTED_EXT:
                            continue
                        if allowed_exts and suffix not in allowed_exts:
                            continue
                        st = dent.stat()
                        rec = FileRecord(path=Path(dent.path), origin=origin, size=st.st_size, mtime=st.st_mtime)
                        key = self._norm_path(rec.path)
                        out_map[origin][key] = rec
                        seen_files[origin].add(key)
                    except OSError:
                        continue
        except OSError:
            return

    def _scan_changed_bbs(
        self,
        base: Path,
        out_map: Dict[str, Dict[str, FileRecord]],
        dir_mtimes: Dict[str, float],
        seen_files: Dict[str, set[str]],
        changed_dirs: Dict[str, set[str]],
        reused_dirs: Dict[str, set[str]],
    ) -> None:
        base_norm = self._norm_path(base)
        try:
            base_st = base.stat()
            cur_dir_mtime = float(base_st.st_mtime)
        except OSError:
            return
        dir_mtimes[base_norm] = cur_dir_mtime
        prev_mtime = self._base_dir_mtimes.get(base_norm)
        if prev_mtime is not None and abs(prev_mtime - cur_dir_mtime) < 1e-6:
            reused_dirs["bbs"].add(base_norm)
            return
        changed_dirs["bbs"].add(base_norm)
        try:
            with os.scandir(base) as it:
                for dent in it:
                    try:
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        suffix = Path(dent.name).suffix.lower()
                        if suffix not in SUPPORTED_EXT:
                            continue
                        st = dent.stat()
                        rec = FileRecord(path=Path(dent.path), origin="bbs", size=st.st_size, mtime=st.st_mtime)
                        key = self._norm_path(rec.path)
                        out_map["bbs"][key] = rec
                        seen_files["bbs"].add(key)
                    except OSError:
                        continue
        except OSError:
            return

    def _finalize_maps(self, records_map: Dict[str, Dict[str, FileRecord]]) -> Dict[str, List[FileRecord]]:
        out: Dict[str, List[FileRecord]] = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
        for origin in out:
            out[origin] = sorted(records_map.get(origin, {}).values(), key=lambda r: r.mtime, reverse=True)
        return out

    def _run_full(self) -> tuple[Dict[str, List[FileRecord]], Dict[str, float]]:
        records_map = self._empty_result()
        dir_mtimes: Dict[str, float] = {}
        for entry in self._watch_dirs:
            origin = str(entry.get("origin", "") or "").strip().lower()
            if origin not in records_map:
                continue
            p = str(entry.get("path", "") or "").strip()
            if not p:
                continue
            base = Path(p)
            if not base.exists():
                continue
            if origin == "bbs":
                self._full_scan_bbs(base, records_map, dir_mtimes)
            else:
                self._full_scan_recursive(base, origin, ORIGIN_EXTS.get(origin), records_map, dir_mtimes)
        return self._finalize_maps(records_map), dir_mtimes

    def _run_incremental(self) -> tuple[Dict[str, List[FileRecord]], Dict[str, float]]:
        records_map = self._empty_result()
        for origin, rows in (self._base_records or {}).items():
            origin_norm = str(origin or "").strip().lower()
            if origin_norm not in records_map:
                continue
            for rec in rows or []:
                key = self._norm_path(rec.path)
                records_map[origin_norm][key] = FileRecord(
                    path=Path(rec.path),
                    origin=origin_norm,
                    size=int(rec.size or 0),
                    mtime=float(rec.mtime or 0.0),
                )

        dir_mtimes: Dict[str, float] = {}
        seen_files: Dict[str, set[str]] = {"varac": set(), "flmsg": set(), "flamp": set(), "bbs": set()}
        changed_dirs: Dict[str, set[str]] = {"varac": set(), "flmsg": set(), "flamp": set(), "bbs": set()}
        reused_dirs: Dict[str, set[str]] = {"varac": set(), "flmsg": set(), "flamp": set(), "bbs": set()}
        missing_roots: Dict[str, set[str]] = {"varac": set(), "flmsg": set(), "flamp": set(), "bbs": set()}

        for entry in self._watch_dirs:
            origin = str(entry.get("origin", "") or "").strip().lower()
            if origin not in records_map:
                continue
            p = str(entry.get("path", "") or "").strip()
            if not p:
                continue
            base = Path(p)
            base_norm = self._norm_path(base)
            if not base.exists():
                missing_roots[origin].add(base_norm)
                continue
            if origin == "bbs":
                self._scan_changed_bbs(base, records_map, dir_mtimes, seen_files, changed_dirs, reused_dirs)
            else:
                self._scan_changed_recursive(
                    base,
                    origin,
                    ORIGIN_EXTS.get(origin),
                    records_map,
                    dir_mtimes,
                    seen_files,
                    changed_dirs,
                    reused_dirs,
                )

        for origin, path_map in records_map.items():
            roots = set(self._roots_by_origin.get(origin, []))
            changed = changed_dirs.get(origin, set())
            reused = reused_dirs.get(origin, set())
            seen = seen_files.get(origin, set())
            missing = missing_roots.get(origin, set())
            if not roots:
                path_map.clear()
                continue
            for key in list(path_map.keys()):
                if not self._is_under_any(key, roots):
                    path_map.pop(key, None)
                    continue
                if missing and self._is_under_any(key, missing):
                    path_map.pop(key, None)
                    continue
                if changed and self._is_under_any(key, changed):
                    if key in seen:
                        continue
                    if reused and self._is_under_any(key, reused):
                        continue
                    path_map.pop(key, None)

        return self._finalize_maps(records_map), dir_mtimes

    def run(self) -> None:
        have_base = any(bool(v) for v in (self._base_records or {}).values())
        try:
            if self._force or not have_base:
                records, dir_mtimes = self._run_full()
                self.finished.emit({"records": records, "dir_mtimes": dir_mtimes, "mode": "full"}, self._force)
                return
            records, dir_mtimes = self._run_incremental()
            self.finished.emit({"records": records, "dir_mtimes": dir_mtimes, "mode": "incremental"}, self._force)
        except Exception:
            records, dir_mtimes = self._run_full()
            self.finished.emit({"records": records, "dir_mtimes": dir_mtimes, "mode": "fallback"}, self._force)


class _BbsAutoArchiveWorker(QObject):
    finished = Signal(object)

    def __init__(
        self,
        *,
        bbs_dir: str,
        archive_dir: str,
        days: int,
        allowed_exts: List[str],
        reason: str,
    ):
        super().__init__()
        self._bbs_dir = Path(str(bbs_dir or ""))
        self._archive_dir = Path(str(archive_dir or ""))
        try:
            self._days = max(1, int(days or 1))
        except Exception:
            self._days = 1
        self._allowed_exts = {
            str(ext or "").strip().lower()
            for ext in (allowed_exts or [])
            if str(ext or "").strip()
        }
        self._reason = str(reason or "").strip() or "timer"

    def _archive_destination(self, src: Path) -> Path:
        dst = self._archive_dir / src.name
        if not dst.exists():
            return dst
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        dst = self._archive_dir / f"{src.stem}_{stamp}{src.suffix}"
        attempt = 2
        while dst.exists():
            dst = self._archive_dir / f"{src.stem}_{stamp}_{attempt}{src.suffix}"
            attempt += 1
        return dst

    def run(self) -> None:
        started_ts = time.time()
        completed_ts = started_ts
        scanned_count = 0
        eligible_count = 0
        moved_count = 0
        error_count = 0
        moved_items: List[tuple[str, str]] = []
        errors: List[str] = []
        cutoff_ts = started_ts - (float(self._days) * 86400.0)
        try:
            with os.scandir(self._bbs_dir) as it:
                for dent in it:
                    try:
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        scanned_count += 1
                        suffix = Path(dent.name).suffix.lower()
                        if self._allowed_exts and suffix not in self._allowed_exts:
                            continue
                        st = dent.stat()
                        if float(st.st_mtime) > cutoff_ts:
                            continue
                        src = Path(dent.path)
                        dst = self._archive_destination(src)
                        eligible_count += 1
                        shutil.move(str(src), str(dst))
                        moved_count += 1
                        moved_items.append((str(src), str(dst)))
                    except Exception as e:
                        error_count += 1
                        errors.append(str(e))
                        continue
        except Exception as e:
            completed_ts = time.time()
            self.finished.emit(
                {
                    "reason": self._reason,
                    "started_ts": started_ts,
                    "completed_ts": completed_ts,
                    "days": self._days,
                    "bbs_dir": str(self._bbs_dir),
                    "archive_dir": str(self._archive_dir),
                    "scanned_count": scanned_count,
                    "eligible_count": eligible_count,
                    "moved_count": moved_count,
                    "error_count": error_count + 1,
                    "moved_items": moved_items,
                    "errors": errors + [str(e)],
                    "fatal_error": str(e),
                }
            )
            return
        completed_ts = time.time()
        self.finished.emit(
            {
                "reason": self._reason,
                "started_ts": started_ts,
                "completed_ts": completed_ts,
                "days": self._days,
                "bbs_dir": str(self._bbs_dir),
                "archive_dir": str(self._archive_dir),
                "scanned_count": scanned_count,
                "eligible_count": eligible_count,
                "moved_count": moved_count,
                "error_count": error_count,
                "moved_items": moved_items,
                "errors": errors,
            }
        )


class _RowsBuildWorker(QObject):
    finished = Signal(object)

    def __init__(
        self,
        *,
        js8_messages: List[JS8Message],
        spotter_messages: List[SpotterMessage],
        varac_messages: List["VarACMessage"],
        sitrep_messages: List["SitrepMessage"],
        files: Dict[str, List[FileRecord]],
        read_state_map: Dict[tuple, tuple[str, float, int]],
        signature_state_map: Dict[tuple, Dict[str, object]],
        sender_cache_seed: Dict[tuple, str],
        form_titles: Dict[str, str],
        show_local_time: bool,
        tz_name: str,
        sitrep_dedupe_enabled: bool,
        sitrep_show_raw_duplicates: bool,
        force: bool,
        generation: int,
    ):
        super().__init__()
        self._js8_messages = list(js8_messages)
        self._spotter_messages = list(spotter_messages)
        self._varac_messages = list(varac_messages)
        self._sitrep_messages = list(sitrep_messages)
        self._files = {
            "varac": list(files.get("varac", [])),
            "flmsg": list(files.get("flmsg", [])),
            "flamp": list(files.get("flamp", [])),
            "bbs": list(files.get("bbs", [])),
        }
        self._read_state_map = dict(read_state_map)
        self._signature_state_map = dict(signature_state_map or {})
        self._sender_cache_seed = dict(sender_cache_seed)
        self._sender_cache_updates: Dict[tuple, str] = {}
        self._form_titles = {str(k): str(v or "") for k, v in (form_titles or {}).items()}
        self._show_local_time = bool(show_local_time)
        self._tz_name = str(tz_name or "UTC")
        self._sitrep_dedupe_enabled = bool(sitrep_dedupe_enabled)
        self._sitrep_show_raw_duplicates = bool(sitrep_show_raw_duplicates)
        self._force = bool(force)
        self._generation = int(generation)

    @staticmethod
    def _compose_search_text(
        msg_type: str,
        status: str,
        from_call: str,
        to_call: str,
        rcv_display: str,
        title: str,
    ) -> str:
        return " ".join(
            [
                str(msg_type or ""),
                str(status or ""),
                str(from_call or ""),
                str(to_call or ""),
                str(rcv_display or ""),
                str(title or ""),
            ]
        ).lower()

    def _format_rcv_display(self, rcv_ts: float, utc_str: Optional[str]) -> str:
        if self._show_local_time:
            try:
                if rcv_ts:
                    dt = datetime.datetime.fromtimestamp(float(rcv_ts), tz=datetime.timezone.utc)
                elif utc_str:
                    dt = datetime.datetime.strptime(str(utc_str), "%Y-%m-%d %H:%M:%S").replace(
                        tzinfo=datetime.timezone.utc
                    )
                else:
                    return ""
                tz = get_timezone(self._tz_name)
                return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(utc_str or "")
        if utc_str:
            return str(utc_str)
        if rcv_ts:
            try:
                return datetime.datetime.fromtimestamp(float(rcv_ts), tz=datetime.timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except Exception:
                return ""
        return ""

    @staticmethod
    def _canonicalize_value(value: object) -> str:
        txt = str(value or "").strip().lower()
        if not txt:
            return "not_reported"
        if txt in {"red", "yellow", "green", "unknown", "not_reported"}:
            return txt
        if txt in {"3", "r"}:
            return "red"
        if txt in {"2", "y"}:
            return "yellow"
        if txt in {"1", "g"}:
            return "green"
        if txt in {"4", "u", "5"}:
            return "unknown"
        return "unknown"

    @staticmethod
    def _canonical_scope(value: object) -> str:
        txt = str(value or "").strip()
        if not txt:
            return ""
        low = txt.lower()
        if low in {"1", "my location"}:
            return "My Location"
        if low in {"2", "my community"}:
            return "My Community"
        if low in {"3", "my county"}:
            return "My County"
        if low in {"4", "my region"}:
            return "My Region"
        if low in {"5", "other", "other location"}:
            return "Other Location"
        return txt

    @staticmethod
    def _aggregate_status(values: List[str]) -> str:
        order = {"red": 5, "yellow": 4, "green": 3, "unknown": 2, "not_reported": 1}
        best = "not_reported"
        score = order[best]
        for val in values:
            key = _RowsBuildWorker._canonicalize_value(val)
            cur = order.get(key, 2)
            if cur > score:
                best = key
                score = cur
        return best

    @staticmethod
    def _spotter_subtype(msg_type: str) -> str:
        form = str(msg_type or "").strip().upper()
        if form == "F!104":
            return "SPOTTER_104"
        if form == "F!301":
            return "SPOTTER_301"
        if form == "F!304":
            return "SPOTTER_304"
        return ""

    @staticmethod
    def _parse_form_responses(raw_text: str) -> str:
        text = str(raw_text or "").strip()
        if not text:
            return ""
        parts = text.split()
        if len(parts) < 2:
            return ""
        if not str(parts[0]).upper().startswith("F!"):
            return ""
        return str(parts[1] or "").strip()

    @staticmethod
    def _spotter_304_fields(digits: str) -> Dict[str, str]:
        out = {
            "overall_status": "not_reported",
            "power": "not_reported",
            "water": "not_reported",
            "medical": "not_reported",
            "communications": "not_reported",
            "internet": "not_reported",
            "travel": "not_reported",
            "food": "not_reported",
            "fuel": "not_reported",
            "crime": "not_reported",
            "civil_unrest": "not_reported",
            "political": "not_reported",
        }
        d = [ch for ch in str(digits or "").strip() if ch.isdigit()]
        if len(d) < 6:
            return out
        out["communications"] = _RowsBuildWorker._canonicalize_value(d[1] if len(d) > 1 else "")
        out["internet"] = _RowsBuildWorker._canonicalize_value(d[3] if len(d) > 3 else "")
        out["water"] = _RowsBuildWorker._canonicalize_value(d[4] if len(d) > 4 else "")
        out["power"] = _RowsBuildWorker._canonicalize_value(d[5] if len(d) > 5 else "")
        out["overall_status"] = _RowsBuildWorker._aggregate_status(
            [out["communications"], out["internet"], out["water"], out["power"]]
        )
        return out

    @staticmethod
    def _status_signature(fields: Dict[str, str]) -> str:
        dims = (
            "overall_status",
            "power",
            "water",
            "medical",
            "communications",
            "internet",
            "travel",
            "food",
            "fuel",
            "crime",
            "civil_unrest",
            "political",
        )
        return "|".join(_RowsBuildWorker._canonicalize_value(fields.get(k, "not_reported")) for k in dims)

    @staticmethod
    def _semantic_report_key(
        *,
        subtype: str,
        from_call: str,
        target: str,
        grid: str,
        scope: str,
        event_ts: float,
        fields: Dict[str, str],
        bucket_seconds: int = 60,
    ) -> str:
        call = str(from_call or "").strip().upper()
        if not call:
            return ""
        bucket = int(bucket_seconds or 60)
        if bucket <= 0:
            bucket = 60
        ts_bucket = int(float(event_ts or 0.0) // float(bucket)) if float(event_ts or 0.0) > 0 else 0
        base = "|".join(
            [
                call,
                str(target or "").strip().upper(),
                str(grid or "").strip().upper(),
                str(scope or "").strip(),
                str(subtype or "").strip().upper(),
                str(ts_bucket),
                _RowsBuildWorker._status_signature(fields),
            ]
        )
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    @staticmethod
    def _spotter_message_report_key(msg: SpotterMessage) -> str:
        subtype = _RowsBuildWorker._spotter_subtype(msg.msg_type)
        if not subtype:
            return ""
        responses = _RowsBuildWorker._parse_form_responses(msg.raw_text)
        fields = {
            "overall_status": "not_reported",
            "power": "not_reported",
            "water": "not_reported",
            "medical": "not_reported",
            "communications": "not_reported",
            "internet": "not_reported",
            "travel": "not_reported",
            "food": "not_reported",
            "fuel": "not_reported",
            "crime": "not_reported",
            "civil_unrest": "not_reported",
            "political": "not_reported",
        }
        scope = ""
        if subtype == "SPOTTER_104":
            first = str(responses or "")[:1]
            fields["overall_status"] = _RowsBuildWorker._canonicalize_value(first)
        elif subtype == "SPOTTER_301":
            digits = [ch for ch in str(responses or "").strip() if ch.isdigit()]
            if digits:
                scope = _RowsBuildWorker._canonical_scope(digits[0])
                mapped = _RowsBuildWorker._spotter_304_fields("".join(digits[1:9]))
                fields.update(mapped)
        elif subtype == "SPOTTER_304":
            mapped = _RowsBuildWorker._spotter_304_fields(responses)
            fields.update(mapped)
        return _RowsBuildWorker._semantic_report_key(
            subtype=subtype,
            from_call=(msg.from_call or "").strip().upper(),
            target=(msg.to_call or "").strip().upper(),
            grid="",
            scope=scope,
            event_ts=float(msg.utc_ts or 0.0),
            fields=fields,
        )

    @staticmethod
    def _sitrep_message_report_key(msg: "SitrepMessage") -> str:
        key = str(msg.report_key or "").strip().lower()
        if key:
            return key
        return _RowsBuildWorker._sitrep_message_semantic_key(msg, bucket_seconds=60)

    @staticmethod
    def _sitrep_message_semantic_key(msg: "SitrepMessage", *, bucket_seconds: int = 60) -> str:
        fields = {
            "overall_status": _RowsBuildWorker._canonicalize_value(msg.overall_status),
            "power": _RowsBuildWorker._canonicalize_value(msg.power),
            "water": _RowsBuildWorker._canonicalize_value(msg.water),
            "medical": _RowsBuildWorker._canonicalize_value(msg.medical),
            "communications": _RowsBuildWorker._canonicalize_value(msg.communications),
            "internet": _RowsBuildWorker._canonicalize_value(msg.internet),
            "travel": _RowsBuildWorker._canonicalize_value(msg.travel),
            "food": _RowsBuildWorker._canonicalize_value(msg.food),
            "fuel": _RowsBuildWorker._canonicalize_value(msg.fuel),
            "crime": _RowsBuildWorker._canonicalize_value(msg.crime),
            "civil_unrest": _RowsBuildWorker._canonicalize_value(msg.civil_unrest),
            "political": _RowsBuildWorker._canonicalize_value(msg.political),
        }
        return _RowsBuildWorker._semantic_report_key(
            subtype=str(msg.subtype or "").strip().upper(),
            from_call=(msg.from_call or "").strip().upper(),
            target=(msg.target or "").strip().upper(),
            grid=(msg.grid or "").strip().upper(),
            scope=_RowsBuildWorker._canonical_scope(msg.scope),
            event_ts=float(msg.event_ts or 0.0),
            fields=fields,
            bucket_seconds=int(bucket_seconds or 60),
        )

    @staticmethod
    def _read_file_head(path: Path, limit: int = 4096) -> str:
        try:
            with path.open("rb") as fh:
                raw = fh.read(limit)
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return ""

    def _extract_sender_from_file(self, rec: FileRecord) -> str:
        cache_key = (str(rec.path), float(rec.mtime or 0.0), int(rec.size or 0))
        if cache_key in self._sender_cache_updates:
            return self._sender_cache_updates.get(cache_key, "")
        if cache_key in self._sender_cache_seed:
            return self._sender_cache_seed.get(cache_key, "")
        text = self._read_file_head(rec.path)
        sender = ""
        if text:
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            for marker in (":hdr_fm:", ":hdr_ed:"):
                for idx, line in enumerate(lines):
                    if line.lower().startswith(marker):
                        for nxt in lines[idx + 1 :]:
                            match = re.search(r"\b[A-Z]{1,2}\d[A-Z0-9]{1,4}\b", nxt.upper())
                            if match:
                                sender = match.group(0)
                                break
                        break
                if sender:
                    break
            if not sender:
                tokens = re.split(r"[-_\\s]+", rec.path.stem)
                for tok in tokens:
                    up = tok.strip().upper()
                    if re.fullmatch(r"[A-Z]{1,2}\d[A-Z0-9]{1,4}", up):
                        sender = up
                        break
        self._sender_cache_updates[cache_key] = sender
        return sender

    def _file_status(self, rec: FileRecord) -> str:
        key = (rec.origin, str(rec.path), float(rec.mtime), int(rec.size))
        state = self._read_state_map.get(key)
        if state and state[0]:
            return str(state[0]).upper()
        return "NEW"

    @staticmethod
    def _signature_key(rec: FileRecord) -> tuple:
        return (str(rec.origin or "").strip().lower(), str(rec.path), float(rec.mtime or 0.0), int(rec.size or 0))

    @staticmethod
    def _is_flamp_auth_file(rec: FileRecord) -> bool:
        return (
            str(rec.origin or "").strip().lower() == "flamp"
            and str(rec.path.suffix or "").strip().lower() in FLAMP_AUTH_EXTS
        )

    def _signature_row_state(self, rec: FileRecord) -> tuple[str, str, bool]:
        if not self._is_flamp_auth_file(rec):
            return "", "", False
        key = self._signature_key(rec)
        state = self._signature_state_map.get(key, {}) if isinstance(self._signature_state_map, dict) else {}
        if not isinstance(state, dict):
            return "", "", False
        status = str(state.get("status", "") or "").strip()
        detail = str(state.get("detail", "") or "").strip()
        trusted = bool(state.get("trusted", False))
        return status, detail, trusted

    def run(self) -> None:
        start = time.perf_counter()
        rows: List[UnifiedMessage] = []
        dedupe_raw_spotter = bool(self._sitrep_dedupe_enabled and not self._sitrep_show_raw_duplicates)
        sitrep_report_keys: set[str] = set()
        sitrep_render_keys: set[str] = set()
        if dedupe_raw_spotter:
            for sitrep in self._sitrep_messages:
                key = self._sitrep_message_semantic_key(sitrep, bucket_seconds=60)
                if key:
                    sitrep_report_keys.add(key)

        for msg in self._js8_messages:
            msg_type = msg.msg_type if msg.msg_type.startswith("F!") else "JS8 MSG"
            status = "READ" if msg.state.upper() == "READ" else "NEW"
            rcv_ts = float(msg.utc_ts or 0.0)
            rcv_display = self._format_rcv_display(rcv_ts, msg.utc_str)
            title = ""
            if msg.msg_type.startswith("F!"):
                form_id = msg.msg_type[2:].strip()
                title = self._form_titles.get(form_id, "")
            if not title:
                title = (msg.decoded_text or msg.raw_text or "").strip()
            if len(title) > 60:
                title = title[:57].rstrip() + "..."
            from_call = (msg.from_call or "").strip().upper()
            to_call = (msg.to_call or "").strip().upper()
            rows.append(
                UnifiedMessage(
                    msg_type=msg_type,
                    status=status,
                    from_call=from_call,
                    to_call=to_call,
                    rcv_ts=rcv_ts,
                    rcv_display=rcv_display,
                    title=title,
                    origin="js8",
                    payload=msg,
                    search_text=self._compose_search_text(msg_type, status, from_call, to_call, rcv_display, title),
                )
            )

        for msg in self._spotter_messages:
            if dedupe_raw_spotter:
                spotter_key = self._spotter_message_report_key(msg)
                if spotter_key and spotter_key in sitrep_report_keys:
                    continue
            msg_type = msg.msg_type or "F!"
            status = "READ" if msg.state.upper() == "READ" else "NEW"
            rcv_ts = float(msg.utc_ts or 0.0)
            rcv_display = self._format_rcv_display(rcv_ts, msg.utc_str)
            title = ""
            if msg_type.startswith("F!"):
                form_id = msg_type[2:].strip()
                title = self._form_titles.get(form_id, "")
            if not title:
                title = (msg.decoded_text or msg.raw_text or "").strip()
            if len(title) > 60:
                title = title[:57].rstrip() + "..."
            from_call = (msg.from_call or "").strip().upper()
            to_call = (msg.to_call or "").strip().upper()
            rows.append(
                UnifiedMessage(
                    msg_type=msg_type,
                    status=status,
                    from_call=from_call,
                    to_call=to_call,
                    rcv_ts=rcv_ts,
                    rcv_display=rcv_display,
                    title=title,
                    origin="spotter",
                    payload=msg,
                    search_text=self._compose_search_text(msg_type, status, from_call, to_call, rcv_display, title),
                )
            )

        for msg in self._varac_messages:
            msg_type = "VarAC"
            status = "NEW" if (msg.read_status == 0 and msg.msg_type.upper() != "QSO") else "READ"
            rcv_ts = float(msg.ts or 0.0)
            rcv_display = self._format_rcv_display(rcv_ts, None)
            if (msg.msg_type or "").upper() == "VMAIL":
                title_base = (msg.subject or "").strip()
            else:
                title_base = (msg.subject or msg.body or "").strip()
            title = f"{msg.msg_type}: {title_base}" if title_base else (msg.msg_type or "VarAC")
            if len(title) > 60:
                title = title[:57].rstrip() + "..."
            from_call = (msg.from_call or "").strip().upper()
            to_call = (msg.to_call or "").strip().upper()
            rows.append(
                UnifiedMessage(
                    msg_type=msg_type,
                    status=status,
                    from_call=from_call,
                    to_call=to_call,
                    rcv_ts=rcv_ts,
                    rcv_display=rcv_display,
                    title=title,
                    origin="varac",
                    payload=msg,
                    search_text=self._compose_search_text(msg_type, status, from_call, to_call, rcv_display, title),
                )
            )

        for msg in self._sitrep_messages:
            if dedupe_raw_spotter:
                ui_key = self._sitrep_message_semantic_key(msg, bucket_seconds=1)
                if ui_key and ui_key in sitrep_render_keys:
                    continue
                if ui_key:
                    sitrep_render_keys.add(ui_key)
            rcv_ts = float(msg.event_ts or 0.0)
            rcv_display = self._format_rcv_display(rcv_ts, msg.event_ts_utc)
            from_call = (msg.from_call or "").strip().upper()
            to_call = (msg.target or "").strip().upper()
            overall = (msg.overall_status or "").strip().lower()
            scope = (msg.scope or "").strip()
            title_parts = [msg.subtype]
            if scope:
                title_parts.append(scope)
            if overall:
                title_parts.append(overall.upper())
            title = " | ".join([p for p in title_parts if p]) or "SitRep"
            if len(title) > 60:
                title = title[:57].rstrip() + "..."
            rows.append(
                UnifiedMessage(
                    msg_type="SitRep",
                    status="INFO",
                    from_call=from_call,
                    to_call=to_call,
                    rcv_ts=rcv_ts,
                    rcv_display=rcv_display,
                    title=title,
                    origin="sitrep",
                    payload=msg,
                    search_text=self._compose_search_text(
                        "SitRep",
                        "INFO",
                        from_call,
                        to_call,
                        rcv_display,
                        f"{title} {msg.subtype}",
                    ),
                )
            )

        for origin, recs in self._files.items():
            for rec in recs:
                status = self._file_status(rec)
                is_image = rec.path.suffix.lower() in IMAGE_EXTS
                from_call = "" if is_image else self._extract_sender_from_file(rec)
                title = "Image Received" if is_image else rec.path.name
                rcv_ts = float(rec.mtime or 0.0)
                rcv_display = self._format_rcv_display(rcv_ts, None)
                msg_type = origin.upper() if origin != "varac" else "VarAC"
                if origin == "flmsg":
                    msg_type = "FLMSG"
                elif origin == "bbs":
                    msg_type = "BBS"
                auth_state, auth_detail, auth_trusted = self._signature_row_state(rec)
                rows.append(
                    UnifiedMessage(
                        msg_type=msg_type,
                        status=status,
                        from_call=from_call,
                        to_call="",
                        rcv_ts=rcv_ts,
                        rcv_display=rcv_display,
                        title=title,
                        origin=origin,
                        payload=rec,
                        search_text=self._compose_search_text(msg_type, status, from_call, "", rcv_display, title),
                        auth_state=auth_state,
                        auth_detail=auth_detail,
                        auth_trusted=auth_trusted,
                    )
                )

        rows.sort(key=lambda r: r.rcv_ts, reverse=True)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        self.finished.emit(
            {
                "rows": rows,
                "elapsed_ms": elapsed_ms,
                "sender_cache_updates": dict(self._sender_cache_updates),
                "generation": self._generation,
                "force": self._force,
            }
        )


@dataclass
class FileSignatureState:
    status: str = "unsigned"
    detail: str = ""
    signer_fingerprint: str = ""
    signer_uid: str = ""
    trusted: bool = False
    signature_path: str = ""
    signature_mtime: float = 0.0
    signature_size: int = 0
    hash_status: str = "unsigned"
    hash_detail: str = ""
    hash_algorithm: str = ""
    hash_expected: str = ""
    hash_actual: str = ""
    hash_path: str = ""
    hash_mtime: float = 0.0
    hash_size: int = 0
    local_hash_status: str = "unsigned"
    local_hash_detail: str = ""
    local_hash_algorithm: str = ""
    local_hash_expected: str = ""
    local_hash_actual: str = ""
    local_hash_label: str = ""
    local_hash_set_sig: str = ""
    verified_ts: float = 0.0


class _SignatureVerifyWorker(QObject):
    finished = Signal(object)

    def __init__(
        self,
        records: List[FileRecord],
        *,
        verify_signature: bool,
        verify_hash: bool,
        inline_sig_name_suffixes: List[str],
        trusted_hash_entries: List[dict],
        trusted_hash_sig: str,
        gpg_path: str,
        trusted_signers: set[str],
        generation: int,
    ):
        super().__init__()
        self._records = list(records)
        self._verify_signature = bool(verify_signature)
        self._verify_hash = bool(verify_hash)
        self._inline_sig_name_suffixes = [str(v or "").strip().lower() for v in (inline_sig_name_suffixes or []) if str(v or "").strip()]
        self._trusted_hash_entries = list(trusted_hash_entries or [])
        self._trusted_hash_sig = str(trusted_hash_sig or "")
        self._gpg_path = str(gpg_path or "").strip()
        self._trusted_signers = set(trusted_signers)
        self._generation = int(generation)

    @staticmethod
    def _cache_key(rec: FileRecord) -> tuple:
        return (str(rec.origin or "").strip().lower(), str(rec.path), float(rec.mtime or 0.0), int(rec.size or 0))

    def run(self) -> None:
        start = time.perf_counter()
        out: Dict[tuple, dict] = {}
        for rec in self._records:
            key = self._cache_key(rec)
            if self._verify_signature:
                sig_result = verify_file_with_discovery(
                    rec.path,
                    configured_path=self._gpg_path,
                    trusted_fingerprints=self._trusted_signers,
                    allow_inline_clearsigned=True,
                    inline_name_suffixes=self._inline_sig_name_suffixes,
                )
            else:
                sig_result = None
            if self._verify_hash:
                hash_result = verify_file_hash_with_discovery(rec.path)
                local_hash_result = verify_file_hash_against_registry(rec.path, self._trusted_hash_entries)
            else:
                hash_result = None
                local_hash_result = None

            sig_path = Path(sig_result.signature_path) if sig_result and sig_result.signature_path else find_detached_signature(rec.path)
            sig_mtime = 0.0
            sig_size = 0
            sig_path_str = ""
            if sig_path is not None:
                sig_path_str = str(sig_path)
                try:
                    st = sig_path.stat()
                    sig_mtime = float(st.st_mtime)
                    sig_size = int(st.st_size)
                except Exception:
                    sig_mtime = 0.0
                    sig_size = 0
            hash_path_obj = Path(hash_result.checksum_path) if hash_result and hash_result.checksum_path else None
            hash_mtime = 0.0
            hash_size = 0
            hash_path_str = ""
            if hash_path_obj is not None:
                hash_path_str = str(hash_path_obj)
                try:
                    st = hash_path_obj.stat()
                    hash_mtime = float(st.st_mtime)
                    hash_size = int(st.st_size)
                except Exception:
                    hash_mtime = 0.0
                    hash_size = 0
            out[key] = {
                "status": str(sig_result.status or "unsigned") if sig_result else "unsigned",
                "detail": str(sig_result.detail or "") if sig_result else "Signature verification disabled.",
                "signer_fingerprint": str(sig_result.signer_fingerprint or "") if sig_result else "",
                "signer_uid": str(sig_result.signer_uid or "") if sig_result else "",
                "trusted": bool(sig_result.trusted) if sig_result else False,
                "signature_path": sig_path_str,
                "signature_mtime": float(sig_mtime),
                "signature_size": int(sig_size),
                "hash_status": str(hash_result.status or "unsigned") if hash_result else "unsigned",
                "hash_detail": str(hash_result.detail or "") if hash_result else "Checksum verification disabled.",
                "hash_algorithm": str(hash_result.algorithm or "") if hash_result else "",
                "hash_expected": str(hash_result.expected_hash or "") if hash_result else "",
                "hash_actual": str(hash_result.actual_hash or "") if hash_result else "",
                "hash_path": hash_path_str,
                "hash_mtime": float(hash_mtime),
                "hash_size": int(hash_size),
                "local_hash_status": str(local_hash_result.status or "unsigned") if local_hash_result else "unsigned",
                "local_hash_detail": str(local_hash_result.detail or "") if local_hash_result else "Local hash verification disabled.",
                "local_hash_algorithm": str(local_hash_result.algorithm or "") if local_hash_result else "",
                "local_hash_expected": str(local_hash_result.expected_hash or "") if local_hash_result else "",
                "local_hash_actual": str(local_hash_result.actual_hash or "") if local_hash_result else "",
                "local_hash_label": str(local_hash_result.entry_label or "") if local_hash_result else "",
                "local_hash_set_sig": str(self._trusted_hash_sig or ""),
                "verified_ts": float(time.time()),
            }
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        self.finished.emit(
            {
                "generation": self._generation,
                "elapsed_ms": elapsed_ms,
                "results": out,
            }
        )


@dataclass
class VarACMessage:
    msg_id: int
    guid: str
    source: str
    msg_type: str
    from_call: str
    to_call: str
    subject: str
    body: str
    ts: float
    band: str
    freq_hz: float | None
    snr: float | None
    read_status: int
    folder: str
    vmail_guid: str
    flag_state: int = 0


@dataclass
class SitrepMessage:
    event_id: int
    report_key: str
    event_ts: float
    event_ts_utc: str
    from_call: str
    target: str
    grid: str
    scope: str
    subtype: str
    overall_status: str
    power: str
    water: str
    medical: str
    communications: str
    internet: str
    travel: str
    food: str
    fuel: str
    crime: str
    civil_unrest: str
    political: str
    source_first: str
    source_last: str
    source_count: int
    sources_json: str
    source_refs_json: str
    raw_payload_json: str
    updated_ts: float


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
    search_text: str = ""
    auth_state: str = ""
    auth_detail: str = ""
    auth_trusted: bool = False


class MessageTableModel(QAbstractTableModel):
    def __init__(self, rows: List[UnifiedMessage]):
        super().__init__()
        self._rows = rows
        self._selected_keys: set[tuple] = set()
        self._row_index_by_key: Dict[tuple, int] = {}
        self._select_column_index = 0
        self._headers = ["", "MSG Type", "Status", "From", "To", "RCV_DT (UTC)", "Message Title", ""]

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._headers)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        row = self._rows[index.row()]
        col = index.column()
        if role == Qt.CheckStateRole and col == 0:
            key = self._row_key(row)
            if key is None:
                return None
            return Qt.Checked if key in self._selected_keys else Qt.Unchecked
        if role == Qt.DisplayRole:
            if col == 1:
                return row.msg_type
            if col == 2:
                return row.status
            if col == 3:
                return row.from_call
            if col == 4:
                return row.to_call
            if col == 5:
                return row.rcv_display
            if col == 6:
                return row.title
            if col == 7:
                if isinstance(row.payload, FileRecord) and (row.payload.origin or "").lower() == "bbs":
                    return "View | Archive | Delete"
                if isinstance(row.payload, (JS8Message, FileRecord, VarACMessage, SpotterMessage)):
                    return "View | Delete"
                return "View"
        if role == Qt.UserRole:
            return row
        if role == Qt.DecorationRole and col == 1:
            auth = str(getattr(row, "auth_state", "") or "").strip().lower()
            app = QApplication.instance()
            style = app.style() if app is not None else None
            if style is None:
                return None
            if auth == "valid":
                return style.standardIcon(QStyle.SP_DialogApplyButton)
            if auth in {"invalid", "error"}:
                return style.standardIcon(QStyle.SP_MessageBoxWarning)
            return None
        if role == Qt.ToolTipRole and col in (1, 6):
            detail = str(getattr(row, "auth_detail", "") or "").strip()
            if detail:
                return detail
        if role == Qt.ForegroundRole and col == 2 and row.status == "NEW":
            return QColor(Qt.red)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self._headers):
            return self._headers[section]
        return None

    def set_time_header(self, label: str) -> None:
        self._headers[5] = label
        self.headerDataChanged.emit(Qt.Horizontal, 5, 5)

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags
        row = self._rows[index.row()]
        if index.column() == 0 and self._row_key(row) is not None:
            return Qt.ItemIsEnabled | Qt.ItemIsUserCheckable | Qt.ItemIsEditable
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def setData(self, index: QModelIndex, value, role: int = Qt.EditRole) -> bool:
        if not index.isValid():
            return False
        if index.column() != 0 or role != Qt.CheckStateRole:
            return False
        row = self._rows[index.row()]
        key = self._row_key(row)
        if key is None:
            return False
        if value == Qt.Checked:
            self._selected_keys.add(key)
        else:
            self._selected_keys.discard(key)
        self.dataChanged.emit(index, index, [Qt.CheckStateRole])
        return True

    def set_rows(self, rows: List[UnifiedMessage]) -> None:
        if len(rows) == len(self._rows):
            unchanged = True
            for old, new in zip(self._rows, rows):
                if old is not new:
                    unchanged = False
                    break
            if unchanged:
                return
        self.beginResetModel()
        self._rows = rows
        row_index_by_key: Dict[tuple, int] = {}
        for i, row in enumerate(rows):
            key = self._row_key(row)
            if key is not None and key not in row_index_by_key:
                row_index_by_key[key] = i
        self._row_index_by_key = row_index_by_key
        keep = {self._row_key(r) for r in rows if self._row_key(r) is not None}
        self._selected_keys = {k for k in self._selected_keys if k in keep}
        self.endResetModel()

    def index_for_row(self, row: UnifiedMessage) -> int:
        key = self._row_key(row)
        if key is not None:
            idx = self._row_index_by_key.get(key)
            if idx is not None:
                return int(idx)
        for i, cur in enumerate(self._rows):
            if cur is row:
                return i
        return -1

    def mark_row_read(self, row: UnifiedMessage) -> bool:
        idx_row = self.index_for_row(row)
        if idx_row < 0:
            return False
        row.status = "READ"
        idx = self.index(idx_row, 2)
        self.dataChanged.emit(idx, idx, [Qt.DisplayRole, Qt.ForegroundRole])
        return True

    def rows(self) -> List[UnifiedMessage]:
        return list(self._rows)

    def selected_rows(self) -> List[UnifiedMessage]:
        out: List[UnifiedMessage] = []
        for row in self._rows:
            key = self._row_key(row)
            if key is not None and key in self._selected_keys:
                out.append(row)
        return out

    def clear_selection(self) -> None:
        if not self._selected_keys:
            return
        self._selected_keys.clear()
        if self.rowCount() > 0:
            self.dataChanged.emit(
                self.index(0, 0),
                self.index(self.rowCount() - 1, 0),
                [Qt.CheckStateRole],
            )

    def set_selected_for_rows(self, rows: List[UnifiedMessage], selected: bool) -> None:
        if not rows:
            return
        updated = False
        for row in rows:
            key = self._row_key(row)
            if key is None:
                continue
            if selected:
                if key not in self._selected_keys:
                    self._selected_keys.add(key)
                    updated = True
            else:
                if key in self._selected_keys:
                    self._selected_keys.discard(key)
                    updated = True
        if updated and self.rowCount() > 0:
            self.dataChanged.emit(
                self.index(0, self._select_column_index),
                self.index(self.rowCount() - 1, self._select_column_index),
                [Qt.CheckStateRole],
            )

    @staticmethod
    def _row_key(row: UnifiedMessage) -> tuple | None:
        payload = row.payload
        if isinstance(payload, JS8Message):
            msg_id = int(getattr(payload, "msg_id", 0) or 0)
            return ("js8", msg_id) if msg_id > 0 else None
        if isinstance(payload, SpotterMessage):
            msg_id = int(getattr(payload, "spotter_id", 0) or 0)
            return ("spotter", msg_id) if msg_id > 0 else None
        if isinstance(payload, VarACMessage):
            msg_id = int(getattr(payload, "msg_id", 0) or 0)
            source = str(getattr(payload, "source", "") or "")
            return ("varac", source, msg_id) if msg_id > 0 and source else None
        if isinstance(payload, FileRecord):
            return ("file", payload.origin, str(payload.path), float(payload.mtime), int(payload.size))
        return None


class MessageActionDelegate(QStyledItemDelegate):
    def __init__(self, parent, danger_color: QColor | None = None):
        super().__init__(parent)
        self._danger = danger_color or QColor(Qt.red)
        self._flag_color_red = QColor("#d32f2f")
        self._flag_color_green = QColor("#2e7d32")

    @staticmethod
    def _is_bbs_file_row(row: UnifiedMessage | None) -> bool:
        return isinstance(getattr(row, "payload", None), FileRecord) and (
            (row.payload.origin or "").strip().lower() == "bbs"
        )

    @staticmethod
    def _action_rects(rect: QRect, fm, bbs_row: bool, bbs_copy_row: bool) -> tuple[QRect, QRect, QRect, QRect]:
        view_text = "View"
        view_width = fm.horizontalAdvance(view_text)
        view_left = rect.left() + 6
        view_rect = QRect(view_left, rect.y(), view_width, rect.height())

        del_text = "Delete"
        del_width = fm.horizontalAdvance(del_text)
        del_right = rect.right() - 6
        del_left = del_right - del_width + 1
        del_rect = QRect(del_left, rect.y(), del_width, rect.height())

        if bbs_row:
            arch_text = "Archive"
            arch_width = fm.horizontalAdvance(arch_text)
            arch_right = del_left - 12
            arch_left = arch_right - arch_width + 1
            aux_rect = QRect(arch_left, rect.y(), arch_width, rect.height())
            return view_rect, aux_rect, QRect(), del_rect

        bbs_rect = QRect()
        gap_right = del_left - 10
        if bbs_copy_row:
            bbs_text = "+BBS"
            bbs_width = fm.horizontalAdvance(bbs_text)
            bbs_right = del_left - 10
            bbs_left = bbs_right - bbs_width + 1
            bbs_rect = QRect(bbs_left, rect.y(), bbs_width, rect.height())
            gap_right = bbs_left - 10

        flag_text = "\u2691"
        flag_width = fm.horizontalAdvance(flag_text)
        gap_left = view_left + view_width + 10
        flag_center = (gap_left + gap_right) // 2
        flag_left = max(gap_left, flag_center - (flag_width // 2))
        aux_rect = QRect(flag_left, rect.y(), flag_width, rect.height())
        return view_rect, aux_rect, bbs_rect, del_rect

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: QModelIndex) -> None:
        if index.column() != 7:
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
        fm = option.fontMetrics
        parent_widget = self.parent()
        bbs_row = self._is_bbs_file_row(row)
        bbs_copy_row = bool(
            hasattr(parent_widget, "_can_copy_row_to_varac_bbs")
            and parent_widget._can_copy_row_to_varac_bbs(row)
        )
        bbs_copy_enabled = bool(
            bbs_copy_row
            and (
                not hasattr(parent_widget, "_is_row_bbs_copy_action_enabled")
                or parent_widget._is_row_bbs_copy_action_enabled(row)
            )
        )
        view_rect, aux_rect, bbs_rect, del_rect = self._action_rects(rect, fm, bbs_row, bbs_copy_row)
        painter.drawText(view_rect, Qt.AlignVCenter | Qt.AlignLeft, "View")
        if bbs_row:
            painter.setPen(link_color)
            painter.drawText(aux_rect, Qt.AlignVCenter | Qt.AlignLeft, "Archive")
            painter.setPen(self._danger)
            painter.drawText(del_rect, Qt.AlignVCenter | Qt.AlignLeft, "Delete")
            painter.restore()
            return

        if isinstance(row.payload, (JS8Message, FileRecord, VarACMessage, SpotterMessage)):
            flag_state = getattr(row.payload, "flag_state", 0)
            if flag_state == 1:
                painter.setPen(self._flag_color_red)
            elif flag_state == 2:
                painter.setPen(self._flag_color_green)
            else:
                painter.setPen(option.palette.color(QPalette.Disabled, QPalette.Text))
            font = QFont(option.font)
            font.setBold(True)
            painter.setFont(font)
            painter.drawText(aux_rect, Qt.AlignVCenter | Qt.AlignLeft, "\u2691")

            if bbs_copy_row:
                if bbs_copy_enabled:
                    painter.setPen(link_color)
                else:
                    painter.setPen(option.palette.color(QPalette.Disabled, QPalette.Text))
                painter.setFont(option.font)
                painter.drawText(bbs_rect, Qt.AlignVCenter | Qt.AlignLeft, "+BBS")

            painter.setPen(self._danger)
            painter.setFont(option.font)
            painter.drawText(del_rect, Qt.AlignVCenter | Qt.AlignLeft, "Delete")
        painter.restore()

    def editorEvent(self, event, model, option, index):
        if index.column() != 7:
            return False
        if event.type() != QEvent.MouseButtonRelease:
            return False
        if hasattr(event, "button") and event.button() != Qt.LeftButton:
            return False
        row = index.data(Qt.UserRole)
        if row is None:
            return False
        rect = option.rect
        pos = event.position().toPoint()
        fm = option.fontMetrics
        parent_widget = self.parent()
        bbs_row = self._is_bbs_file_row(row)
        bbs_copy_row = bool(
            hasattr(parent_widget, "_can_copy_row_to_varac_bbs")
            and parent_widget._can_copy_row_to_varac_bbs(row)
        )
        bbs_copy_enabled = bool(
            bbs_copy_row
            and (
                not hasattr(parent_widget, "_is_row_bbs_copy_action_enabled")
                or parent_widget._is_row_bbs_copy_action_enabled(row)
            )
        )
        _view_rect, aux_rect, bbs_rect, del_rect = self._action_rects(rect, fm, bbs_row, bbs_copy_row)
        if isinstance(row.payload, FileRecord):
            if bbs_row and aux_rect.contains(pos):
                self.parent()._archive_file_record(row.payload)
            elif bbs_copy_row and bbs_rect.contains(pos):
                if bbs_copy_enabled:
                    self.parent()._copy_row_to_varac_bbs(row)
                return True
            elif not bbs_row and aux_rect.contains(pos):
                self.parent()._cycle_flag_state(row.payload)
            elif del_rect.contains(pos):
                self.parent()._delete_file_record(row.payload)
            else:
                self.parent()._on_view_message(row)
        elif isinstance(row.payload, JS8Message):
            if aux_rect.contains(pos):
                self.parent()._cycle_flag_state(row.payload)
            elif del_rect.contains(pos):
                self.parent()._delete_js8_message(row.payload)
            else:
                self.parent()._on_view_message(row)
        elif isinstance(row.payload, SpotterMessage):
            if aux_rect.contains(pos):
                self.parent()._cycle_flag_state(row.payload)
            elif del_rect.contains(pos):
                self.parent()._delete_spotter_message(row.payload)
            else:
                self.parent()._on_view_message(row)
        elif isinstance(row.payload, VarACMessage):
            if aux_rect.contains(pos):
                self.parent()._cycle_flag_state(row.payload)
            elif del_rect.contains(pos):
                self.parent()._delete_varac_message(row.payload)
            else:
                self.parent()._on_view_message(row)
        else:
            self.parent()._on_view_message(row)
        return True


class MessageCheckboxDelegate(QStyledItemDelegate):
    def editorEvent(self, event, model, option, index):
        if index.column() != 0:
            return False
        if event.type() != QEvent.MouseButtonRelease:
            return False
        if hasattr(event, "button") and event.button() != Qt.LeftButton:
            return False
        state = model.data(index, Qt.CheckStateRole)
        if state is None:
            return False
        new_state = Qt.Unchecked if state == Qt.Checked else Qt.Checked
        model.setData(index, new_state, Qt.CheckStateRole)
        return True


class MessageHeaderWithCheckbox(QHeaderView):
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
        default_mode = (self.settings.get("display_time_mode", "LOCAL") or "LOCAL").upper()
        self._time_mode_override: str | None = None
        self._show_local_time = default_mode != "UTC"
        cfg = self.settings.get("message_viewer", {}) or {}
        msg_paths = self.settings.get("message_paths", {}) or {}
        self.watch_dirs: List[Dict] = []
        for origin in ["js8", "varac", "flmsg", "flamp"]:
            p = msg_paths.get(origin, "")
            if p:
                self.watch_dirs.append({"path": p, "origin": origin})
        if not self.watch_dirs:
            self.watch_dirs = DEFAULT_WATCH_DIRS
        fldigi_log_path = (self.settings.get("fldigi_log_path", "") or "").strip()
        if fldigi_log_path:
            images_dir = Path(fldigi_log_path) / "images"
            if images_dir.exists():
                existing = {(w.get("origin"), w.get("path")) for w in self.watch_dirs}
                entry = ("flmsg", str(images_dir))
                if entry not in existing:
                    self.watch_dirs.append({"path": str(images_dir), "origin": "flmsg"})
        self.scan_minutes: int = cfg.get("scan_minutes") or 15
        if self.scan_minutes not in SCAN_CHOICES:
            self.scan_minutes = 15

        self.js8_messages: List[JS8Message] = []
        self.spotter_messages: List[SpotterMessage] = []
        self.varac_messages: List[VarACMessage] = []
        self.sitrep_messages: List[SitrepMessage] = []
        self.current_js8: JS8Message | None = None
        self._js8_timer: QTimer | None = None
        self._pending_timer: QTimer | None = None
        self._pending_rows: List[Dict[str, str | float]] = []
        self._form_cache: Dict[str, List[Dict]] = {}
        self._form_title_cache: Dict[str, str] = {}
        self.forms_path = (self.settings.get("js8_forms_path", "") or "").strip()
        self._read_state_map: Dict[tuple, tuple[str, float, int]] = {}
        self._message_rows: List[UnifiedMessage] = []
        self._filters_initialized = False
        self._has_active_view = False
        self._default_sort_column = 5
        self._default_sort_order = Qt.DescendingOrder
        self._sort_column = self._default_sort_column
        self._sort_order = self._default_sort_order
        self._freeze_messages_table = False
        self._deferred_refresh = False
        self._messages_model = MessageTableModel([])
        self._actions_delegate = None
        self._header_cells: List[QWidget] = []
        self._messages_header_sync_connected: bool = False
        self._is_shutting_down = False
        self._refresh_files_inflight = False
        self.loading_label: QLabel | None = None
        self._file_scan_thread: QThread | None = None
        self._file_scan_worker: _FileScanWorker | None = None
        self._file_scan_start_ts: float = 0.0
        self._rows_build_thread: QThread | None = None
        self._rows_build_worker: _RowsBuildWorker | None = None
        self._rows_build_generation: int = 0
        self._rows_build_pending: bool = False
        self._rows_build_pending_force: bool = False
        self._open_external_path: Path | None = None
        self._loading_timer: QTimer | None = None
        self._loading_text: str = "Getting messages..."
        self._loading_progress: QProgressBar | None = None
        self._persist_timer: QTimer | None = None
        self._pending_persist_ops: List[Tuple[str, Tuple]] = []
        self._activation_refresh_pending: bool = False
        self._activation_refresh_interval_sec: float = 60.0
        self._last_activation_refresh_ts: float = 0.0
        self._bbs_auto_archive_timer: QTimer | None = None
        self._bbs_auto_archive_thread: QThread | None = None
        self._bbs_auto_archive_worker: _BbsAutoArchiveWorker | None = None
        self._bbs_auto_archive_inflight: bool = False
        self._bbs_auto_archive_check_pending: bool = False
        self._bbs_auto_archive_first_activation_pending: bool = True
        self._bbs_auto_archive_interval_sec: float = float(BBS_AUTO_ARCHIVE_INTERVAL_SECONDS)
        self._varac_ingest_interval_sec: float = 20.0
        self._last_varac_ingest_ts: float = 0.0
        self._js8_ingest_interval_sec: float = 20.0
        self._last_js8_ingest_ts: float = 0.0
        self._file_refresh_interval_sec: float = 60.0
        self._last_file_refresh_ts: float = 0.0
        self._sender_cache: Dict[tuple, str] = {}
        self._file_view_cache: Dict[tuple, tuple[bool, str, str, Optional[Path], str]] = {}
        self._cache_max_sender_entries: int = 2500
        self._cache_max_view_entries: int = 500
        self._cache_max_form_entries: int = 256
        self._cache_max_form_title_entries: int = 256
        self._scan_cache_loaded: bool = False
        self._scan_cache_saved_ts: float = 0.0
        self._scan_dir_mtime_cache: Dict[str, float] = {}
        self._files_snapshot_fp: Optional[Tuple[Tuple[str, int, int], ...]] = None
        self._signature_state_map: Dict[tuple, FileSignatureState] = {}
        self._signature_verify_thread: QThread | None = None
        self._signature_verify_worker: _SignatureVerifyWorker | None = None
        self._signature_verify_generation: int = 0
        self._signature_verify_pending: bool = False
        self._signature_verify_pending_records: List[FileRecord] = []
        self._signature_verify_deferred_until_active: bool = False
        self._bbs_copied_session_keys: set[tuple[str, float, int]] = set()

        # merge DB paths if present
        self._load_watch_dirs_from_db()
        self._clear_backlog_on_upgrade()
        self._ensure_read_state_table()
        self._ensure_spotter_table()
        self._ensure_fldigi_sender_table()
        self._ensure_file_scan_cache_table()
        self._ensure_signature_cache_table()
        self._read_state_map = self._load_read_state_map()
        self._signature_state_map = self._load_signature_state_map()

        self.files: Dict[str, List[FileRecord]] = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
        self.current_record: FileRecord | None = None
        self._scan_cache_loaded = self._load_file_scan_cache()
        if self._scan_cache_loaded:
            try:
                self._update_fldigi_senders(self.files)
            except Exception:
                pass
            self._files_snapshot_fp = self._files_records_fingerprint(self.files)
            self._last_file_refresh_ts = time.time()

        self._timer: QTimer | None = None
        self.paths_labels: Dict[str, QLabel] = {}

        self._build_ui()
        QTimer.singleShot(0, self._initial_refresh)
        self._setup_timer()
        self._setup_js8_timer()
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
                    flag_state INTEGER DEFAULT 0,
                    PRIMARY KEY (origin, path, mtime, size)
                )
                """
            )
            try:
                cur.execute("ALTER TABLE message_read_state ADD COLUMN flag_state INTEGER DEFAULT 0")
            except Exception:
                pass
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to ensure read state table: %s", e)

    def _ensure_spotter_table(self) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS spotter_traffic (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    utc_ts REAL,
                    utc_str TEXT,
                    from_call TEXT,
                    to_call TEXT,
                    form_id TEXT,
                    spotter_token TEXT,
                    raw_text TEXT,
                    decoded_text TEXT,
                    state TEXT,
                    read_ts REAL,
                    flag_state INTEGER DEFAULT 0,
                    relay_via TEXT,
                    ingested_ts REAL
                )
                """
            )
            try:
                cur.execute("ALTER TABLE spotter_traffic ADD COLUMN flag_state INTEGER DEFAULT 0")
            except Exception:
                pass
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to ensure spotter table: %s", e)

    def _ensure_fldigi_sender_table(self) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS fldigi_file_senders (
                    callsign TEXT PRIMARY KEY,
                    last_seen_ts REAL,
                    origin TEXT
                )
                """
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to ensure fldigi sender table: %s", e)

    def _ensure_file_scan_cache_table(self) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_scan_cache (
                    origin TEXT NOT NULL,
                    path TEXT NOT NULL,
                    mtime REAL NOT NULL,
                    size INTEGER NOT NULL,
                    PRIMARY KEY (origin, path)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_scan_cache_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_scan_cache_dirs (
                    dir_path TEXT PRIMARY KEY,
                    mtime REAL NOT NULL
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_message_scan_cache_origin_mtime ON message_scan_cache(origin, mtime DESC)"
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to ensure file scan cache table: %s", e)

    def _ensure_signature_cache_table(self) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_signature_cache (
                    origin TEXT NOT NULL,
                    path TEXT NOT NULL,
                    mtime REAL NOT NULL,
                    size INTEGER NOT NULL,
                    sig_path TEXT,
                    sig_mtime REAL,
                    sig_size INTEGER,
                    status TEXT NOT NULL,
                    detail TEXT,
                    signer_fingerprint TEXT,
                    signer_uid TEXT,
                    trusted INTEGER DEFAULT 0,
                    hash_status TEXT DEFAULT 'unsigned',
                    hash_detail TEXT,
                    hash_algorithm TEXT,
                    hash_expected TEXT,
                    hash_actual TEXT,
                    hash_path TEXT,
                    hash_mtime REAL,
                    hash_size INTEGER,
                    local_hash_status TEXT DEFAULT 'unsigned',
                    local_hash_detail TEXT,
                    local_hash_algorithm TEXT,
                    local_hash_expected TEXT,
                    local_hash_actual TEXT,
                    local_hash_label TEXT,
                    local_hash_set_sig TEXT,
                    verified_ts REAL,
                    PRIMARY KEY (origin, path, mtime, size)
                )
                """
            )
            for col_def in (
                "hash_status TEXT DEFAULT 'unsigned'",
                "hash_detail TEXT",
                "hash_algorithm TEXT",
                "hash_expected TEXT",
                "hash_actual TEXT",
                "hash_path TEXT",
                "hash_mtime REAL",
                "hash_size INTEGER",
                "local_hash_status TEXT DEFAULT 'unsigned'",
                "local_hash_detail TEXT",
                "local_hash_algorithm TEXT",
                "local_hash_expected TEXT",
                "local_hash_actual TEXT",
                "local_hash_label TEXT",
                "local_hash_set_sig TEXT",
            ):
                try:
                    cur.execute(f"ALTER TABLE message_signature_cache ADD COLUMN {col_def}")
                except Exception:
                    pass
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_message_signature_cache_status ON message_signature_cache(status)"
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to ensure signature cache table: %s", e)

    @staticmethod
    def _signature_cache_key(rec: FileRecord) -> tuple:
        return (
            str(rec.origin or "").strip().lower(),
            str(rec.path),
            float(rec.mtime or 0.0),
            int(rec.size or 0),
        )

    def _load_signature_state_map(self) -> Dict[tuple, FileSignatureState]:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return {}
        self._ensure_signature_cache_table()
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT origin, path, mtime, size, sig_path, sig_mtime, sig_size,
                       status, detail, signer_fingerprint, signer_uid, trusted,
                       hash_status, hash_detail, hash_algorithm, hash_expected, hash_actual,
                       hash_path, hash_mtime, hash_size,
                       local_hash_status, local_hash_detail, local_hash_algorithm,
                       local_hash_expected, local_hash_actual, local_hash_label, local_hash_set_sig,
                       verified_ts
                FROM message_signature_cache
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load signature cache: %s", e)
            return {}

        out: Dict[tuple, FileSignatureState] = {}
        for row in rows:
            try:
                (
                    origin,
                    path,
                    mtime,
                    size,
                    sig_path,
                    sig_mtime,
                    sig_size,
                    status,
                    detail,
                    signer_fpr,
                    signer_uid,
                    trusted,
                    hash_status,
                    hash_detail,
                    hash_algorithm,
                    hash_expected,
                    hash_actual,
                    hash_path,
                    hash_mtime,
                    hash_size,
                    local_hash_status,
                    local_hash_detail,
                    local_hash_algorithm,
                    local_hash_expected,
                    local_hash_actual,
                    local_hash_label,
                    local_hash_set_sig,
                    verified_ts,
                ) = row
                key = (str(origin or "").strip().lower(), str(path or ""), float(mtime or 0.0), int(size or 0))
                out[key] = FileSignatureState(
                    status=str(status or "unsigned"),
                    detail=str(detail or ""),
                    signer_fingerprint=str(signer_fpr or ""),
                    signer_uid=str(signer_uid or ""),
                    trusted=bool(int(trusted or 0)),
                    signature_path=str(sig_path or ""),
                    signature_mtime=float(sig_mtime or 0.0),
                    signature_size=int(sig_size or 0),
                    hash_status=str(hash_status or "unsigned"),
                    hash_detail=str(hash_detail or ""),
                    hash_algorithm=str(hash_algorithm or ""),
                    hash_expected=str(hash_expected or ""),
                    hash_actual=str(hash_actual or ""),
                    hash_path=str(hash_path or ""),
                    hash_mtime=float(hash_mtime or 0.0),
                    hash_size=int(hash_size or 0),
                    local_hash_status=str(local_hash_status or "unsigned"),
                    local_hash_detail=str(local_hash_detail or ""),
                    local_hash_algorithm=str(local_hash_algorithm or ""),
                    local_hash_expected=str(local_hash_expected or ""),
                    local_hash_actual=str(local_hash_actual or ""),
                    local_hash_label=str(local_hash_label or ""),
                    local_hash_set_sig=str(local_hash_set_sig or ""),
                    verified_ts=float(verified_ts or 0.0),
                )
            except Exception:
                continue
        return out

    def _save_signature_state_batch(self, values: Dict[tuple, FileSignatureState]) -> None:
        if not values:
            return
        db_path = self._db_path()
        if not db_path:
            return
        self._ensure_signature_cache_table()
        payload: List[tuple] = []
        for key, state in values.items():
            try:
                origin, path, mtime, size = key
                payload.append(
                    (
                        str(origin),
                        str(path),
                        float(mtime or 0.0),
                        int(size or 0),
                        str(state.signature_path or ""),
                        float(state.signature_mtime or 0.0),
                        int(state.signature_size or 0),
                        str(state.status or "error"),
                        str(state.detail or ""),
                        str(state.signer_fingerprint or ""),
                        str(state.signer_uid or ""),
                        1 if state.trusted else 0,
                        str(state.hash_status or "unsigned"),
                        str(state.hash_detail or ""),
                        str(state.hash_algorithm or ""),
                        str(state.hash_expected or ""),
                        str(state.hash_actual or ""),
                        str(state.hash_path or ""),
                        float(state.hash_mtime or 0.0),
                        int(state.hash_size or 0),
                        str(state.local_hash_status or "unsigned"),
                        str(state.local_hash_detail or ""),
                        str(state.local_hash_algorithm or ""),
                        str(state.local_hash_expected or ""),
                        str(state.local_hash_actual or ""),
                        str(state.local_hash_label or ""),
                        str(state.local_hash_set_sig or ""),
                        float(state.verified_ts or 0.0),
                    )
                )
            except Exception:
                continue
        if not payload:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT OR REPLACE INTO message_signature_cache
                    (origin, path, mtime, size, sig_path, sig_mtime, sig_size,
                     status, detail, signer_fingerprint, signer_uid, trusted,
                     hash_status, hash_detail, hash_algorithm, hash_expected, hash_actual,
                     hash_path, hash_mtime, hash_size,
                     local_hash_status, local_hash_detail, local_hash_algorithm,
                     local_hash_expected, local_hash_actual, local_hash_label, local_hash_set_sig,
                     verified_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to save signature cache batch: %s", e)

    def _watch_dirs_signature(self, watch_dirs: List[Dict]) -> str:
        parts: List[tuple[str, str]] = []
        for entry in watch_dirs:
            origin = str(entry.get("origin", "") or "").strip().lower()
            path = str(entry.get("path", "") or "").strip()
            if origin and path:
                parts.append((origin, path))
        parts.sort()
        return json.dumps(parts, ensure_ascii=True, separators=(",", ":"))

    def _load_file_scan_cache(self) -> bool:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return False
        self._ensure_file_scan_cache_table()
        sig = self._watch_dirs_signature(self._effective_watch_dirs())
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT value FROM message_scan_cache_meta WHERE key='watch_dirs_sig'")
            row = cur.fetchone()
            stored_sig = str(row[0] or "") if row else ""
            if not stored_sig or stored_sig != sig:
                conn.close()
                return False
            cur.execute("SELECT value FROM message_scan_cache_meta WHERE key='saved_ts'")
            row = cur.fetchone()
            try:
                self._scan_cache_saved_ts = float(row[0]) if row and row[0] is not None else 0.0
            except Exception:
                self._scan_cache_saved_ts = 0.0
            cur.execute("SELECT origin, path, mtime, size FROM message_scan_cache")
            rows = cur.fetchall()
            cur.execute("SELECT dir_path, mtime FROM message_scan_cache_dirs")
            dir_rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load file scan cache: %s", e)
            return False

        loaded_dir_mtimes: Dict[str, float] = {}
        for dir_path, mtime in dir_rows:
            try:
                norm = os.path.normcase(os.path.normpath(str(dir_path or "")))
                if not norm:
                    continue
                loaded_dir_mtimes[norm] = float(mtime or 0.0)
            except Exception:
                continue

        out: Dict[str, List[FileRecord]] = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
        for origin, path, mtime, size in rows:
            origin_norm = str(origin or "").strip().lower()
            if origin_norm not in out:
                continue
            rec_path = Path(str(path or ""))
            suffix = rec_path.suffix.lower()
            if suffix not in SUPPORTED_EXT:
                continue
            allowed = ORIGIN_EXTS.get(origin_norm)
            if allowed and suffix not in allowed:
                continue
            out[origin_norm].append(
                FileRecord(
                    path=rec_path,
                    origin=origin_norm,
                    size=int(size or 0),
                    mtime=float(mtime or 0.0),
                )
            )
        total = 0
        for origin in out:
            out[origin].sort(key=lambda r: r.mtime, reverse=True)
            total += len(out[origin])
        if total <= 0:
            self._scan_dir_mtime_cache = {}
            return False
        self.files = out
        self._files_snapshot_fp = self._files_records_fingerprint(out)
        self._scan_dir_mtime_cache = loaded_dir_mtimes
        if self._scan_cache_saved_ts > 0:
            self._last_file_refresh_ts = float(self._scan_cache_saved_ts)
        log.debug("MessageViewer: loaded file scan cache (%s records)", total)
        return True

    def _save_file_scan_cache(
        self,
        records: Dict[str, List[FileRecord]],
        *,
        dir_mtimes: Optional[Dict[str, float]] = None,
    ) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        self._ensure_file_scan_cache_table()
        sig = self._watch_dirs_signature(self._effective_watch_dirs())
        saved_ts = time.time()
        payload: List[tuple[str, str, float, int]] = []
        dir_payload: List[tuple[str, float]] = []
        for origin, recs in records.items():
            origin_norm = str(origin or "").strip().lower()
            if origin_norm not in ORIGIN_EXTS:
                continue
            for rec in recs:
                payload.append(
                    (
                        origin_norm,
                        str(rec.path),
                        float(rec.mtime or 0.0),
                        int(rec.size or 0),
                    )
                )
        for dir_path, mtime in (dir_mtimes or {}).items():
            try:
                norm = os.path.normcase(os.path.normpath(str(dir_path or "")))
                if not norm:
                    continue
                dir_payload.append((norm, float(mtime or 0.0)))
            except Exception:
                continue
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM message_scan_cache")
            cur.execute("DELETE FROM message_scan_cache_dirs")
            if payload:
                cur.executemany(
                    "INSERT OR REPLACE INTO message_scan_cache(origin, path, mtime, size) VALUES (?, ?, ?, ?)",
                    payload,
                )
            if dir_payload:
                cur.executemany(
                    "INSERT OR REPLACE INTO message_scan_cache_dirs(dir_path, mtime) VALUES (?, ?)",
                    dir_payload,
                )
            cur.execute(
                "INSERT OR REPLACE INTO message_scan_cache_meta(key, value) VALUES (?, ?)",
                ("watch_dirs_sig", sig),
            )
            cur.execute(
                "INSERT OR REPLACE INTO message_scan_cache_meta(key, value) VALUES (?, ?)",
                ("saved_ts", str(saved_ts)),
            )
            conn.commit()
            conn.close()
            self._scan_cache_saved_ts = saved_ts
            self._scan_dir_mtime_cache = {
                os.path.normcase(os.path.normpath(str(k))): float(v or 0.0)
                for k, v in (dir_mtimes or {}).items()
                if str(k or "").strip()
            }
        except Exception as e:
            log.debug("MessageViewer: failed to save file scan cache: %s", e)

    def _save_file_scan_cache_meta_only(
        self,
        *,
        dir_mtimes: Optional[Dict[str, float]] = None,
    ) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        self._ensure_file_scan_cache_table()
        sig = self._watch_dirs_signature(self._effective_watch_dirs())
        saved_ts = time.time()
        dir_payload: List[tuple[str, float]] = []
        for dir_path, mtime in (dir_mtimes or {}).items():
            try:
                norm = os.path.normcase(os.path.normpath(str(dir_path or "")))
                if not norm:
                    continue
                dir_payload.append((norm, float(mtime or 0.0)))
            except Exception:
                continue
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM message_scan_cache_dirs")
            if dir_payload:
                cur.executemany(
                    "INSERT OR REPLACE INTO message_scan_cache_dirs(dir_path, mtime) VALUES (?, ?)",
                    dir_payload,
                )
            cur.execute(
                "INSERT OR REPLACE INTO message_scan_cache_meta(key, value) VALUES (?, ?)",
                ("watch_dirs_sig", sig),
            )
            cur.execute(
                "INSERT OR REPLACE INTO message_scan_cache_meta(key, value) VALUES (?, ?)",
                ("saved_ts", str(saved_ts)),
            )
            conn.commit()
            conn.close()
            self._scan_cache_saved_ts = saved_ts
            self._scan_dir_mtime_cache = {
                os.path.normcase(os.path.normpath(str(k))): float(v or 0.0)
                for k, v in (dir_mtimes or {}).items()
                if str(k or "").strip()
            }
        except Exception as e:
            log.debug("MessageViewer: failed to save file scan cache meta-only: %s", e)

    @staticmethod
    def _files_records_fingerprint(
        records: Dict[str, List[FileRecord]],
    ) -> Tuple[Tuple[str, int, int], ...]:
        out: List[Tuple[str, int, int]] = []
        for origin in ("varac", "flmsg", "flamp", "bbs"):
            recs = records.get(origin, [])
            digest = 1469598103934665603
            for rec in recs:
                item = (
                    str(rec.path),
                    int(rec.size or 0),
                    int(float(rec.mtime or 0.0) * 1000.0),
                )
                digest ^= hash(item)
                digest = (digest * 1099511628211) & 0xFFFFFFFFFFFFFFFF
            out.append((origin, int(len(recs)), int(digest)))
        return tuple(out)

    def _can_skip_file_scan_quick(self, watch_dirs: List[Dict], force: bool) -> bool:
        if force:
            return False
        if not self._scan_cache_loaded:
            return False
        if not self._scan_dir_mtime_cache:
            return False
        roots: set[str] = set()
        for entry in watch_dirs:
            path = str(entry.get("path", "") or "").strip()
            if not path:
                continue
            roots.add(os.path.normcase(os.path.normpath(path)))
        if not roots:
            return False
        cached = self._scan_dir_mtime_cache
        for root in roots:
            if root not in cached:
                return False
        for dir_path, prev_mtime in cached.items():
            norm = os.path.normcase(os.path.normpath(str(dir_path or "")))
            if not norm:
                return False
            if not any(norm == root or norm.startswith(root + os.sep) for root in roots):
                return False
            try:
                cur_mtime = float(Path(norm).stat().st_mtime)
            except OSError:
                return False
            if abs(cur_mtime - float(prev_mtime or 0.0)) > 1e-6:
                return False
        return True

    @staticmethod
    def _read_state_key(origin: str, rec: FileRecord) -> tuple:
        return (origin, str(rec.path), float(rec.mtime), int(rec.size))

    def _load_read_state_map(self) -> Dict[tuple, tuple[str, float, int]]:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return {}
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT origin, path, mtime, size, status, read_ts, flag_state
                FROM message_read_state
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load read state: %s", e)
            return {}
        out: Dict[tuple, tuple[str, float, int]] = {}
        for origin, path, mtime, size, status, read_ts, flag_state in rows:
            key = (origin, path, float(mtime or 0.0), int(size or 0))
            out[key] = (
                str(status or "").upper(),
                float(read_ts or 0.0),
                int(flag_state or 0),
            )
        return out

    def _get_read_state(self, rec: FileRecord) -> str:
        key = self._read_state_key(rec.origin, rec)
        state = self._read_state_map.get(key)
        if state and state[0]:
            return state[0]
        return "NEW"

    def _get_flag_state(self, rec: FileRecord) -> int:
        key = self._read_state_key(rec.origin, rec)
        state = self._read_state_map.get(key)
        if state and len(state) > 2:
            return int(state[2] or 0)
        return 0

    def _set_read_state(self, rec: FileRecord, status: str, row_ref: Optional[UnifiedMessage] = None) -> None:
        status = (status or "READ").upper()
        key = self._read_state_key(rec.origin, rec)
        read_ts = time.time() if status == "READ" else 0.0
        flag_state = self._get_flag_state(rec)
        self._read_state_map[key] = (status, read_ts, flag_state)
        self._queue_persist_op(
            "file_read_state",
            (
                rec.origin,
                str(rec.path),
                float(rec.mtime),
                int(rec.size),
                status,
                float(read_ts),
                int(flag_state),
            ),
        )
        self._refresh_table_after_read(
            lambda row: isinstance(row.payload, FileRecord)
            and self._read_state_key(row.payload.origin, row.payload) == key,
            row_ref=row_ref,
        )

    @staticmethod
    def _is_flamp_auth_file(rec: FileRecord) -> bool:
        return (
            str(rec.origin or "").strip().lower() == "flamp"
            and str(rec.path.suffix or "").strip().lower() in FLAMP_AUTH_EXTS
        )

    def _is_signature_verification_enabled(self) -> bool:
        return bool(self.settings.get("gpg_verify_flamp_k2s_enabled", False))

    def _is_hash_verification_enabled(self) -> bool:
        return bool(self.settings.get("hash_verify_flamp_k2s_enabled", True))

    def _is_any_auth_verification_enabled(self) -> bool:
        return bool(self._is_signature_verification_enabled() or self._is_hash_verification_enabled())

    def _trusted_hash_entries(self) -> List[dict]:
        raw = self.settings.get("trusted_file_hashes", []) or []
        if not isinstance(raw, list):
            raw = []
        return normalize_trusted_hash_entries(raw)

    def _trusted_hash_set_signature(self) -> str:
        entries = self._trusted_hash_entries()
        compact = [
            (
                str(row.get("algorithm", "") or ""),
                str(row.get("hash", "") or ""),
                bool(row.get("enabled", True)),
            )
            for row in entries
        ]
        try:
            return json.dumps(compact, separators=(",", ":"), ensure_ascii=True)
        except Exception:
            return ""

    def _trusted_signer_fingerprints(self) -> set[str]:
        raw = self.settings.get("gpg_trusted_signers", []) or []
        if not isinstance(raw, list):
            raw = []
        return normalize_fingerprints(str(v) for v in raw)

    def _inline_signature_name_suffixes(self) -> List[str]:
        raw = self.settings.get("gpg_inline_signed_filename_suffixes", list(DEFAULT_INLINE_SIGNED_SUFFIXES))
        values: List[str]
        if isinstance(raw, str):
            values = [v.strip() for v in raw.split(",")]
        elif isinstance(raw, list):
            values = [str(v) for v in raw]
        else:
            values = list(DEFAULT_INLINE_SIGNED_SUFFIXES)
        normalized = normalize_signature_name_suffixes(values)
        if not normalized:
            normalized = normalize_signature_name_suffixes(DEFAULT_INLINE_SIGNED_SUFFIXES)
        return normalized

    def _is_inline_signature_name_candidate(self, path_obj: Path, suffixes: Optional[List[str]] = None) -> bool:
        name = str(getattr(path_obj, "name", "") or "").strip().lower()
        if not name:
            return False
        use_suffixes = suffixes if suffixes is not None else self._inline_signature_name_suffixes()
        for suffix in use_suffixes:
            if suffix and name.endswith(suffix):
                return True
        return False

    def _signature_state_for_record(self, rec: FileRecord) -> FileSignatureState:
        key = self._signature_cache_key(rec)
        return self._signature_state_map.get(key, FileSignatureState(status="unsigned", detail="No signature state."))

    def _signature_cache_fresh(
        self,
        rec: FileRecord,
        state: FileSignatureState,
        inline_name_suffixes: Optional[List[str]] = None,
    ) -> bool:
        if not self._is_signature_verification_enabled():
            return True
        if not isinstance(state, FileSignatureState):
            return False
        sig = find_detached_signature(rec.path)
        if sig is None:
            if str(state.signature_path or "").strip():
                return False
            # No detached signature exists. For inline-candidate names we still force one
            # re-check if this state came from old detached-only logic.
            if self._is_inline_signature_name_candidate(rec.path, inline_name_suffixes):
                detail_txt = str(state.detail or "").strip().lower()
                if detail_txt == "no detached signature found.":
                    return False
            return True
        if str(state.signature_path or "").strip() != str(sig):
            return False
        try:
            st = sig.stat()
            sig_mtime = float(st.st_mtime)
            sig_size = int(st.st_size)
        except Exception:
            sig_mtime = 0.0
            sig_size = 0
        if abs(float(state.signature_mtime or 0.0) - sig_mtime) > 1e-6:
            return False
        if int(state.signature_size or 0) != sig_size:
            return False
        return True

    def _hash_cache_fresh(self, rec: FileRecord, state: FileSignatureState, hash_set_sig: str) -> bool:
        if not self._is_hash_verification_enabled():
            return True
        if not isinstance(state, FileSignatureState):
            return False
        if str(state.local_hash_set_sig or "") != str(hash_set_sig or ""):
            return False
        sidecars = existing_checksum_sidecars(rec.path)
        if not sidecars:
            return (
                str(state.hash_status or "").strip().lower() == "unsigned"
                and not str(state.hash_path or "").strip()
            )
        # Fresh if the cached sidecar still exists with same metadata.
        cached_path = str(state.hash_path or "").strip()
        if not cached_path:
            return False
        target = None
        for cand in sidecars:
            if str(cand) == cached_path:
                target = cand
                break
        if target is None:
            return False
        try:
            st = target.stat()
            cur_mtime = float(st.st_mtime)
            cur_size = int(st.st_size)
        except Exception:
            cur_mtime = 0.0
            cur_size = 0
        if abs(float(state.hash_mtime or 0.0) - cur_mtime) > 1e-6:
            return False
        if int(state.hash_size or 0) != cur_size:
            return False
        return True

    def _signature_candidates_to_verify(self, *, force: bool = False) -> List[FileRecord]:
        if not self._is_any_auth_verification_enabled():
            return []
        hash_set_sig = self._trusted_hash_set_signature()
        inline_sig_name_suffixes = self._inline_signature_name_suffixes()
        out: List[FileRecord] = []
        for rec in self.files.get("flamp", []):
            if not self._is_flamp_auth_file(rec):
                continue
            key = self._signature_cache_key(rec)
            state = self._signature_state_map.get(key)
            if (
                force
                or state is None
                or not self._signature_cache_fresh(rec, state, inline_sig_name_suffixes)
                or not self._hash_cache_fresh(rec, state, hash_set_sig)
            ):
                out.append(rec)
        return out

    def _start_signature_verification(self, *, force: bool = False) -> None:
        if self._is_shutting_down:
            return
        try:
            self.settings.reload()
        except Exception:
            pass
        if not self._is_any_auth_verification_enabled():
            return
        records = self._signature_candidates_to_verify(force=force)
        if not records:
            return
        if self._signature_verify_thread:
            try:
                if self._signature_verify_thread.isRunning():
                    self._signature_verify_pending = True
                    self._signature_verify_pending_records = records
                    return
            except RuntimeError:
                self._signature_verify_thread = None
                self._signature_verify_worker = None
        self._signature_verify_generation += 1
        generation = int(self._signature_verify_generation)
        self._signature_verify_pending = False
        self._signature_verify_pending_records = []
        gpg_path = str(self.settings.get("gpg_executable_path", "") or "").strip()
        trusted_signers = self._trusted_signer_fingerprints()
        verify_signature = self._is_signature_verification_enabled()
        verify_hash = self._is_hash_verification_enabled()
        inline_sig_name_suffixes = self._inline_signature_name_suffixes()
        trusted_hash_entries = self._trusted_hash_entries()
        trusted_hash_sig = self._trusted_hash_set_signature()
        self._signature_verify_thread = QThread(self)
        self._signature_verify_worker = _SignatureVerifyWorker(
            records,
            verify_signature=verify_signature,
            verify_hash=verify_hash,
            inline_sig_name_suffixes=inline_sig_name_suffixes,
            trusted_hash_entries=trusted_hash_entries,
            trusted_hash_sig=trusted_hash_sig,
            gpg_path=gpg_path,
            trusted_signers=trusted_signers,
            generation=generation,
        )
        self._signature_verify_worker.moveToThread(self._signature_verify_thread)
        self._signature_verify_thread.started.connect(self._signature_verify_worker.run)
        self._signature_verify_worker.finished.connect(self._on_signature_verify_finished)
        self._signature_verify_worker.finished.connect(self._signature_verify_thread.quit)
        self._signature_verify_worker.finished.connect(self._signature_verify_worker.deleteLater)
        self._signature_verify_thread.finished.connect(self._on_signature_verify_thread_finished)
        self._signature_verify_thread.finished.connect(self._signature_verify_thread.deleteLater)
        self._signature_verify_thread.start()

    def _on_signature_verify_thread_finished(self) -> None:
        self._signature_verify_thread = None
        self._signature_verify_worker = None
        if self._signature_verify_pending and not self._is_shutting_down:
            self._signature_verify_pending = False
            self._start_signature_verification(force=False)

    def _on_signature_verify_finished(self, payload: object) -> None:
        if self._is_shutting_down:
            return
        data = payload if isinstance(payload, dict) else {}
        try:
            generation = int(data.get("generation", 0) or 0)
        except Exception:
            generation = 0
        if generation and generation != self._signature_verify_generation:
            return
        raw_results = data.get("results", {})
        if not isinstance(raw_results, dict) or not raw_results:
            return
        updates: Dict[tuple, FileSignatureState] = {}
        for key, raw in raw_results.items():
            if not isinstance(key, tuple) or len(key) != 4:
                continue
            row = raw if isinstance(raw, dict) else {}
            state = FileSignatureState(
                status=str(row.get("status", "error") or "error"),
                detail=str(row.get("detail", "") or ""),
                signer_fingerprint=normalize_fingerprint(str(row.get("signer_fingerprint", "") or "")),
                signer_uid=str(row.get("signer_uid", "") or ""),
                trusted=bool(row.get("trusted", False)),
                signature_path=str(row.get("signature_path", "") or ""),
                signature_mtime=float(row.get("signature_mtime", 0.0) or 0.0),
                signature_size=int(row.get("signature_size", 0) or 0),
                hash_status=str(row.get("hash_status", "unsigned") or "unsigned"),
                hash_detail=str(row.get("hash_detail", "") or ""),
                hash_algorithm=str(row.get("hash_algorithm", "") or ""),
                hash_expected=str(row.get("hash_expected", "") or ""),
                hash_actual=str(row.get("hash_actual", "") or ""),
                hash_path=str(row.get("hash_path", "") or ""),
                hash_mtime=float(row.get("hash_mtime", 0.0) or 0.0),
                hash_size=int(row.get("hash_size", 0) or 0),
                local_hash_status=str(row.get("local_hash_status", "unsigned") or "unsigned"),
                local_hash_detail=str(row.get("local_hash_detail", "") or ""),
                local_hash_algorithm=str(row.get("local_hash_algorithm", "") or ""),
                local_hash_expected=str(row.get("local_hash_expected", "") or ""),
                local_hash_actual=str(row.get("local_hash_actual", "") or ""),
                local_hash_label=str(row.get("local_hash_label", "") or ""),
                local_hash_set_sig=str(row.get("local_hash_set_sig", "") or ""),
                verified_ts=float(row.get("verified_ts", time.time()) or time.time()),
            )
            updates[key] = state
        if not updates:
            return
        self._signature_state_map.update(updates)
        self._save_signature_state_batch(updates)
        self._apply_signature_updates_to_rows(updates)
        try:
            verify_ms = float(data.get("elapsed_ms", 0.0) or 0.0)
        except Exception:
            verify_ms = 0.0
        if verify_ms > 0:
            emit_span(
                "messages.signature_verify",
                verify_ms,
                settings=self.settings,
                meta={"records": len(updates)},
                min_ms=5.0,
            )
        self._refresh_current_record_signature_info()

    def _apply_signature_updates_to_rows(self, updates: Dict[tuple, FileSignatureState]) -> None:
        if not updates:
            return
        for row in self._message_rows:
            payload = getattr(row, "payload", None)
            if not isinstance(payload, FileRecord) or not self._is_flamp_auth_file(payload):
                continue
            key = self._signature_cache_key(payload)
            state = updates.get(key)
            if not state:
                continue
            ui_state, ui_detail, ui_trusted = self._derive_auth_ui(state)
            row.auth_state = ui_state
            row.auth_detail = ui_detail
            row.auth_trusted = ui_trusted
        if not hasattr(self, "_messages_model"):
            return
        rows = self._messages_model.rows()
        if not rows:
            return
        changed_indices: List[int] = []
        for idx, row in enumerate(rows):
            payload = getattr(row, "payload", None)
            if not isinstance(payload, FileRecord) or not self._is_flamp_auth_file(payload):
                continue
            key = self._signature_cache_key(payload)
            state = updates.get(key)
            if not state:
                continue
            ui_state, ui_detail, ui_trusted = self._derive_auth_ui(state)
            row.auth_state = ui_state
            row.auth_detail = ui_detail
            row.auth_trusted = ui_trusted
            changed_indices.append(idx)
        for idx in changed_indices:
            i1 = self._messages_model.index(idx, 1)
            i6 = self._messages_model.index(idx, 6)
            self._messages_model.dataChanged.emit(
                i1,
                i6,
                [Qt.DecorationRole, Qt.ToolTipRole, Qt.DisplayRole],
            )

    @staticmethod
    def _status_weight(status: str) -> int:
        v = str(status or "").strip().lower()
        if v in {"invalid", "error"}:
            return 3
        if v == "valid":
            return 2
        if v == "unsigned":
            return 1
        return 0

    def _derive_auth_ui(self, state: FileSignatureState) -> tuple[str, str, bool]:
        sig_enabled = self._is_signature_verification_enabled()
        hash_enabled = self._is_hash_verification_enabled()
        sig_status = str(state.status or "unsigned").strip().lower() if sig_enabled else "unsigned"
        hash_status = str(state.hash_status or "unsigned").strip().lower() if hash_enabled else "unsigned"
        local_status = str(state.local_hash_status or "unsigned").strip().lower() if hash_enabled else "unsigned"
        overall = ""
        # User requirement: any successful key/hash validation is enough to trust the file.
        if sig_status == "valid" or hash_status == "valid" or local_status == "valid":
            overall = "valid"
        elif max(self._status_weight(sig_status), self._status_weight(hash_status), self._status_weight(local_status)) == 3:
            overall = "invalid"
        else:
            overall = ""

        success_parts: List[str] = []
        if sig_enabled:
            if sig_status == "valid":
                trust_text = "trusted signer" if state.trusted else "signer not in trusted list"
                success_parts.append(f"Signature: Valid ({trust_text})")

        if hash_enabled:
            hs = hash_status
            algo = str(state.hash_algorithm or "").strip().upper()
            if hs == "valid":
                success_parts.append(f"Checksum: Valid ({algo or 'HASH'})")

            ls = local_status
            algo_local = str(state.local_hash_algorithm or "").strip().upper()
            if ls == "valid":
                label = str(state.local_hash_label or "").strip()
                if label:
                    success_parts.append(f"Local Hash: Matched ({algo_local or 'HASH'}, {label})")
                else:
                    success_parts.append(f"Local Hash: Matched ({algo_local or 'HASH'})")

        if success_parts:
            detail = success_parts[0]
        else:
            detail = "Signature: No Signatures or Hash Matches"
        return overall, detail, bool(state.trusted)

    def _format_signature_detail(self, state: FileSignatureState) -> str:
        _overall, detail, _trusted = self._derive_auth_ui(state)
        return detail

    def _signature_detail_for_record(self, rec: Optional[FileRecord]) -> str:
        if rec is None or not self._is_flamp_auth_file(rec):
            return ""
        state = self._signature_state_for_record(rec)
        detail = self._format_signature_detail(state)
        return detail

    def _compose_info_with_signature(self, rec: Optional[FileRecord], info: str) -> str:
        base = str(info or "").strip()
        detail = self._signature_detail_for_record(rec)
        if detail:
            return f"{base}\n{detail}"
        return base

    def _refresh_current_record_signature_info(self) -> None:
        rec = self.current_record
        if rec is None or not self._is_flamp_auth_file(rec):
            return
        info_txt = self.info_label.text() if hasattr(self, "info_label") else ""
        if not info_txt:
            return
        base = info_txt
        for marker in ("\nSignature:", "\nChecksum:", "\nLocal Hash:"):
            base = base.split(marker, 1)[0]
        sig_detail = self._signature_detail_for_record(rec)
        if sig_detail:
            self.info_label.setText(f"{base}\n{sig_detail}")
        else:
            self.info_label.setText(base)

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
        header.addSpacing(8)
        self.loading_label = QLabel("Getting messages...")
        self.loading_label.setStyleSheet("color: #888;")
        self.loading_label.setVisible(False)
        header.addWidget(self.loading_label)
        self._loading_progress = QProgressBar()
        self._loading_progress.setRange(0, 0)
        self._loading_progress.setFixedWidth(140)
        self._loading_progress.setVisible(False)
        header.addWidget(self._loading_progress)
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

        self.delete_selected_btn = QPushButton("Delete Selected")
        self.delete_selected_btn.clicked.connect(self._delete_selected_messages)
        self.delete_selected_btn.setEnabled(False)
        self.delete_selected_btn.setStyleSheet(button_style("muted", resolve_theme(self.settings)))
        header.addWidget(self.delete_selected_btn)

        self.mark_all_read_btn = QPushButton("Mark All as Read")
        self.mark_all_read_btn.setMinimumWidth(160)
        self.mark_all_read_btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.mark_all_read_btn.clicked.connect(self._mark_all_filtered_read)
        self.mark_all_read_btn.setStyleSheet(button_style("muted", resolve_theme(self.settings)))
        header.addWidget(self.mark_all_read_btn)

        self.time_toggle_btn = QPushButton("Showing: Local" if self._show_local_time else "Showing: UTC")
        self.time_toggle_btn.setStyleSheet(button_style("primary", resolve_theme(self.settings)))
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        header.addWidget(self.time_toggle_btn)

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
            ["Callsign", "Msg ID", "Last Seen (UTC)", "Status", "Actions"]
        )
        self.pending_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.pending_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.pending_table.setAlternatingRowColors(True)
        self.pending_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
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
        self.messages_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        msg_header = MessageHeaderWithCheckbox(Qt.Horizontal, self.messages_table)
        self.messages_table.setHorizontalHeader(msg_header)
        msg_header.setSectionResizeMode(0, QHeaderView.Fixed)
        msg_header.setSectionResizeMode(1, QHeaderView.Interactive)
        msg_header.setSectionResizeMode(2, QHeaderView.Interactive)
        msg_header.setSectionResizeMode(3, QHeaderView.Interactive)
        msg_header.setSectionResizeMode(4, QHeaderView.Interactive)
        msg_header.setSectionResizeMode(5, QHeaderView.Interactive)
        msg_header.setSectionResizeMode(6, QHeaderView.Stretch)
        msg_header.setSectionResizeMode(7, QHeaderView.Fixed)
        self.messages_table.setColumnWidth(0, 32)
        self.messages_table.setColumnWidth(1, 100)
        self.messages_table.setColumnWidth(2, 96)
        self.messages_table.setColumnWidth(3, 122)
        self.messages_table.setColumnWidth(4, 122)
        self.messages_table.setColumnWidth(5, 162)
        self.messages_table.setColumnWidth(7, 250)
        msg_header.setVisible(True)
        msg_header.sectionClicked.connect(self._on_sort_clicked)
        msg_header.checkboxToggled.connect(self._on_header_checkbox_toggled)

        self._update_time_ui()
        self._actions_delegate = MessageActionDelegate(self, QColor(resolve_theme(self.settings)["danger"]))
        self.messages_table.setItemDelegateForColumn(7, self._actions_delegate)
        self.messages_table.setItemDelegateForColumn(0, MessageCheckboxDelegate(self.messages_table))
        messages_layout.addWidget(self.messages_table)
        messages_box.setLayout(messages_layout)
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(messages_box)
        self.messages_splitter = splitter

        viewer_container = QWidget()
        viewer_layout = QVBoxLayout(viewer_container)
        self.info_label = QLabel("No file selected")
        self.info_label.setStyleSheet("font-weight: bold;")
        info_row = QHBoxLayout()
        info_row.addWidget(self.info_label)
        info_row.addStretch()
        self.open_external_btn = QPushButton("Open Image")
        self.open_external_btn.clicked.connect(self._open_external_file)
        self.open_external_btn.setVisible(False)
        info_row.addWidget(self.open_external_btn)
        viewer_layout.addLayout(info_row)
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
        self._make_combo_searchable(self.from_filter, "From")
        self._make_combo_searchable(self.to_filter, "To")
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
        self._apply_accessibility_width_guards()
        QTimer.singleShot(0, self._set_initial_splitter_sizes)
        self._messages_model.dataChanged.connect(self._update_bulk_delete_buttons)

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

    def _setup_bbs_auto_archive_timer(self):
        if self._bbs_auto_archive_timer:
            self._bbs_auto_archive_timer.stop()
        self._bbs_auto_archive_timer = QTimer(self)
        self._bbs_auto_archive_timer.timeout.connect(self._on_bbs_auto_archive_timer)
        self._bbs_auto_archive_timer.start(int(self._bbs_auto_archive_interval_sec * 1000))

    def _on_bbs_auto_archive_timer(self) -> None:
        self._queue_bbs_auto_archive_check("timer", delay_ms=0)

    def _on_refresh_now(self) -> None:
        self._unfreeze_table()
        self._set_loading(True)
        try:
            self._refresh_files(force=True)
            self._refresh_js8_messages(force=True, rebuild=False)
            self._refresh_varac_messages(force=True, rebuild=False)
            self._populate_messages_table(force=True)
            self._last_activation_refresh_ts = time.time()
        finally:
            self._set_loading(False)

    def _on_scan_changed(self):
        val = self.scan_combo.currentData()
        if not val:
            return
        self.scan_minutes = int(val)
        self._setup_timer()
        self._save_settings()

    def _initial_refresh(self) -> None:
        self._set_loading(False)
        self._load_paths_lists()
        if not self._scan_cache_loaded:
            self._refresh_files()
        else:
            # Cache-first startup: keep first paint fast, then reconcile in background.
            self._last_file_refresh_ts = time.time()
            QTimer.singleShot(1500, lambda: self._refresh_files(force=False))
        self._refresh_js8_messages(rebuild=False)
        self._refresh_varac_messages(force=True, rebuild=False)
        self._populate_messages_table(force=True)
        if self._has_active_view:
            QTimer.singleShot(0, lambda: self._start_signature_verification(force=False))
        else:
            # Hidden startup lazy-prewarm should not launch GPG verification; defer
            # until the Messages tab is first shown.
            self._signature_verify_deferred_until_active = True
        self._refresh_pending_backlog()
        self._last_activation_refresh_ts = time.time()

    def _set_loading(self, active: bool, text: str = "Getting messages...") -> None:
        if not self.loading_label:
            return
        if self._loading_timer and self._loading_timer.isActive():
            self._loading_timer.stop()
        self.loading_label.setText(text)
        self.loading_label.setVisible(bool(active))
        if self._loading_progress is not None:
            self._loading_progress.setVisible(bool(active))

    def _schedule_loading(self, text: str = "Getting messages...", delay_ms: int = 350) -> None:
        if not self.loading_label:
            return
        if self._loading_timer is None:
            self._loading_timer = QTimer(self)
            self._loading_timer.setSingleShot(True)
            self._loading_timer.timeout.connect(self._show_loading_delayed)
        self._loading_text = text
        self._loading_timer.stop()
        self._loading_timer.start(max(0, int(delay_ms)))

    def _show_loading_delayed(self) -> None:
        self._set_loading(True, self._loading_text)

    def show_loading_toast(self) -> None:
        self._set_loading(True)

    def on_tab_activated(self) -> None:
        with perf_span(
            "messages.on_tab_activated",
            settings=self.settings,
            meta={"rows": len(self._message_rows)},
            min_ms=10.0,
        ):
            self._unfreeze_table()
            if self._message_rows and self._deferred_refresh:
                self._refresh_message_filters(self._message_rows)
                self._apply_message_filters_preserve_scroll()
            self._update_pending_table()
            if self._bbs_auto_archive_first_activation_pending:
                self._bbs_auto_archive_first_activation_pending = False
                self._queue_bbs_auto_archive_check("first_activation", delay_ms=1200)
            else:
                self._queue_bbs_auto_archive_check("tab_activated", delay_ms=250)
            now = time.time()
            stale = (not self._message_rows) or (
                now - float(self._last_activation_refresh_ts) >= self._activation_refresh_interval_sec
            )
            if not stale:
                self._set_loading(False)
                return
            if self._activation_refresh_pending:
                return
            self._activation_refresh_pending = True
            self._schedule_loading("Getting messages...")
            QTimer.singleShot(0, lambda: self._run_activation_refresh(force=not self._message_rows))

    def _run_activation_refresh(self, force: bool = False) -> None:
        with perf_span(
            "messages.activation_refresh",
            settings=self.settings,
            meta={"force": bool(force)},
            min_ms=5.0,
        ):
            try:
                self._load_paths_lists()
                now = time.time()
                should_refresh_files = bool(force) or (
                    now - float(self._last_file_refresh_ts) >= self._file_refresh_interval_sec
                )
                if should_refresh_files:
                    self._refresh_files(force=force)
                self._refresh_js8_messages(force=force, rebuild=False)
                self._refresh_varac_messages(force=force, rebuild=False)
                self._populate_messages_table(force=force)
                self._refresh_pending_backlog()
                self._last_activation_refresh_ts = time.time()
            finally:
                self._activation_refresh_pending = False
                self._set_loading(False)

    def set_tab_active(self, active: bool) -> None:
        self._has_active_view = bool(active)
        if active:
            self._setup_timer()
            self._setup_js8_timer()
            self._setup_pending_timer()
            self._setup_bbs_auto_archive_timer()
            if self._signature_verify_deferred_until_active:
                self._signature_verify_deferred_until_active = False
                QTimer.singleShot(0, lambda: self._start_signature_verification(force=False))
            return
        for timer in (self._timer, self._js8_timer, self._pending_timer, self._bbs_auto_archive_timer):
            if timer:
                timer.stop()

    # ---------- BBS Auto-Archive ----------

    def _bbs_auto_archive_settings_snapshot(self) -> Optional[Dict[str, object]]:
        if not self._is_truthy(self.settings.get("varac_bbs_auto_archive_enabled", False), False):
            return None
        bbs_dir_txt = str(self.settings.get("varac_bbs_dir", "") or "").strip()
        archive_dir_txt = str(self.settings.get("varac_bbs_archive_dir", "") or "").strip()
        if not bbs_dir_txt or not archive_dir_txt:
            log.debug("MessageViewer: BBS auto-archive skipped (paths not configured)")
            return None
        bbs_dir = Path(bbs_dir_txt)
        archive_dir = Path(archive_dir_txt)
        if not bbs_dir.exists() or not bbs_dir.is_dir() or not archive_dir.exists() or not archive_dir.is_dir():
            log.debug("MessageViewer: BBS auto-archive skipped (invalid directories)")
            return None
        try:
            if bbs_dir.resolve() == archive_dir.resolve():
                log.debug("MessageViewer: BBS auto-archive skipped (BBS and archive dirs are the same)")
                return None
        except Exception:
            pass
        days_raw = self.settings.get("varac_bbs_auto_archive_days", 14)
        try:
            days_val = max(1, int(days_raw or 14))
        except Exception:
            days_val = 14
        allowed = sorted(set(ORIGIN_EXTS.get("bbs", set(SUPPORTED_EXT))))
        return {
            "bbs_dir": str(bbs_dir),
            "archive_dir": str(archive_dir),
            "days": int(days_val),
            "allowed_exts": allowed,
        }

    def _bbs_auto_archive_last_check_ts(self) -> float:
        raw = self.settings.get(BBS_AUTO_ARCHIVE_LAST_CHECK_KEY, 0.0)
        try:
            return float(raw or 0.0)
        except Exception:
            return 0.0

    def _bbs_auto_archive_due_now(self, now_ts: Optional[float] = None) -> bool:
        now_val = float(now_ts if now_ts is not None else time.time())
        last_ts = self._bbs_auto_archive_last_check_ts()
        if last_ts <= 0:
            return True
        return (now_val - last_ts) >= float(self._bbs_auto_archive_interval_sec)

    def _set_bbs_auto_archive_last_check_ts(self, ts: float) -> None:
        try:
            self.settings.set(BBS_AUTO_ARCHIVE_LAST_CHECK_KEY, float(ts))
        except Exception as e:
            log.debug("MessageViewer: failed to persist BBS auto-archive check ts: %s", e)

    def _queue_bbs_auto_archive_check(self, reason: str, delay_ms: int = 0) -> None:
        if self._is_shutting_down:
            return
        if self._bbs_auto_archive_check_pending or self._bbs_auto_archive_inflight:
            return
        self._bbs_auto_archive_check_pending = True

        def _run() -> None:
            self._bbs_auto_archive_check_pending = False
            self._run_bbs_auto_archive_if_due(reason=reason)

        QTimer.singleShot(max(0, int(delay_ms)), _run)

    def _run_bbs_auto_archive_if_due(self, *, reason: str) -> None:
        if self._is_shutting_down or self._bbs_auto_archive_inflight:
            return
        if not self._has_active_view:
            return
        if self._bbs_auto_archive_thread:
            try:
                if self._bbs_auto_archive_thread.isRunning():
                    return
            except RuntimeError:
                self._bbs_auto_archive_thread = None
                self._bbs_auto_archive_worker = None
        snapshot = self._bbs_auto_archive_settings_snapshot()
        if not snapshot:
            return
        if not self._bbs_auto_archive_due_now():
            return
        if self._refresh_files_inflight:
            self._queue_bbs_auto_archive_check(reason, delay_ms=3000)
            return
        self._bbs_auto_archive_inflight = True
        self._bbs_auto_archive_thread = QThread(self)
        self._bbs_auto_archive_worker = _BbsAutoArchiveWorker(
            bbs_dir=str(snapshot.get("bbs_dir", "") or ""),
            archive_dir=str(snapshot.get("archive_dir", "") or ""),
            days=int(snapshot.get("days", 14) or 14),
            allowed_exts=[str(v) for v in (snapshot.get("allowed_exts", []) or [])],
            reason=str(reason or "timer"),
        )
        self._bbs_auto_archive_worker.moveToThread(self._bbs_auto_archive_thread)
        self._bbs_auto_archive_thread.started.connect(self._bbs_auto_archive_worker.run)
        self._bbs_auto_archive_worker.finished.connect(self._on_bbs_auto_archive_finished)
        self._bbs_auto_archive_worker.finished.connect(self._bbs_auto_archive_thread.quit)
        self._bbs_auto_archive_worker.finished.connect(self._bbs_auto_archive_worker.deleteLater)
        self._bbs_auto_archive_thread.finished.connect(self._on_bbs_auto_archive_thread_finished)
        self._bbs_auto_archive_thread.finished.connect(self._bbs_auto_archive_thread.deleteLater)
        self._bbs_auto_archive_thread.start()

    def _on_bbs_auto_archive_thread_finished(self) -> None:
        self._bbs_auto_archive_thread = None
        self._bbs_auto_archive_worker = None

    def _on_bbs_auto_archive_finished(self, payload: object) -> None:
        self._bbs_auto_archive_inflight = False
        if self._is_shutting_down:
            return
        data = payload if isinstance(payload, dict) else {}
        try:
            completed_ts = float(data.get("completed_ts", time.time()) or time.time())
        except Exception:
            completed_ts = time.time()
        self._set_bbs_auto_archive_last_check_ts(completed_ts)
        reason = str(data.get("reason", "") or "timer")
        moved_count = int(data.get("moved_count", 0) or 0)
        error_count = int(data.get("error_count", 0) or 0)
        scanned_count = int(data.get("scanned_count", 0) or 0)
        eligible_count = int(data.get("eligible_count", 0) or 0)
        fatal_error = str(data.get("fatal_error", "") or "").strip()
        if fatal_error:
            log.warning("MessageViewer: BBS auto-archive failed (%s): %s", reason, fatal_error)
            return
        if moved_count or error_count:
            log.info(
                "MessageViewer: BBS auto-archive check (%s) scanned=%s eligible=%s moved=%s errors=%s",
                reason,
                scanned_count,
                eligible_count,
                moved_count,
                error_count,
            )
        else:
            log.debug(
                "MessageViewer: BBS auto-archive check (%s) scanned=%s eligible=%s moved=0",
                reason,
                scanned_count,
                eligible_count,
            )
        if moved_count > 0:
            if self._refresh_files_inflight:
                QTimer.singleShot(1200, lambda: self._refresh_files(force=False))
            else:
                self._refresh_files(force=False)


    # ---------- Paths ----------

    def _load_paths_lists(self):
        by_origin: Dict[str, List[str]] = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
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
        self._scan_cache_loaded = False
        self._scan_cache_saved_ts = 0.0
        self._scan_dir_mtime_cache = {}
        self._files_snapshot_fp = None
        self._save_settings()
        self._refresh_files()

    def _remove_path(self, origin: str):
        # remove last added path for this origin (or prompt later)
        paths = [w for w in self.watch_dirs if w.get("origin") == origin]
        if not paths:
            return
        last = paths[-1]
        self.watch_dirs = [w for w in self.watch_dirs if not (w.get("origin") == origin and w.get("path") == last.get("path"))]
        self._scan_cache_loaded = False
        self._scan_cache_saved_ts = 0.0
        self._scan_dir_mtime_cache = {}
        self._files_snapshot_fp = None
        self._save_settings()
        self._refresh_files()

    # ---------- Scanning ----------

    def _effective_watch_dirs(self) -> List[Dict]:
        out: List[Dict] = []
        seen: set[tuple[str, str]] = set()
        for entry in self.watch_dirs:
            origin = str(entry.get("origin", "") or "").strip().lower()
            path = str(entry.get("path", "") or "").strip()
            if not origin or not path:
                continue
            key = (origin, path)
            if key in seen:
                continue
            out.append({"origin": origin, "path": path})
            seen.add(key)
        bbs_dir = (self.settings.get("varac_bbs_dir", "") or "").strip()
        if bbs_dir:
            key = ("bbs", bbs_dir)
            if key not in seen:
                out.append({"origin": "bbs", "path": bbs_dir})
        return out

    def _refresh_files(self, force: bool = False):
        if self._is_shutting_down or self._refresh_files_inflight or self._bbs_auto_archive_inflight:
            return
        if self._file_scan_thread:
            try:
                if self._file_scan_thread.isRunning():
                    return
            except RuntimeError:
                self._file_scan_thread = None
                self._file_scan_worker = None
        self._refresh_files_inflight = True
        self._file_scan_start_ts = time.time()
        self._load_paths_lists()
        watch_dirs = self._effective_watch_dirs()
        if self._can_skip_file_scan_quick(watch_dirs, force):
            try:
                self._save_file_scan_cache_meta_only(dir_mtimes=self._scan_dir_mtime_cache)
            except Exception:
                pass
            finally:
                self._refresh_files_inflight = False
                self._last_file_refresh_ts = time.time()
                elapsed = time.time() - self._file_scan_start_ts
                total_records = sum(len(v) for v in self.files.values())
                emit_span(
                    "messages.file_scan_total",
                    elapsed * 1000.0,
                    settings=self.settings,
                    meta={
                        "force": bool(force),
                        "records": int(total_records),
                        "mode": "quick_skip",
                        "unchanged": True,
                    },
                    min_ms=5.0,
                )
            return
        base_records = None if force else self.files
        base_dir_mtimes = None if force else self._scan_dir_mtime_cache
        self._file_scan_thread = QThread(self)
        self._file_scan_worker = _FileScanWorker(
            watch_dirs,
            force,
            base_records=base_records,
            base_dir_mtimes=base_dir_mtimes,
        )
        self._file_scan_worker.moveToThread(self._file_scan_thread)
        self._file_scan_thread.started.connect(self._file_scan_worker.run)
        self._file_scan_worker.finished.connect(self._on_file_scan_finished)
        self._file_scan_worker.finished.connect(self._file_scan_thread.quit)
        self._file_scan_worker.finished.connect(self._file_scan_worker.deleteLater)
        self._file_scan_thread.finished.connect(self._on_file_scan_thread_finished)
        self._file_scan_thread.finished.connect(self._file_scan_thread.deleteLater)
        self._file_scan_thread.start()

    def _on_file_scan_thread_finished(self) -> None:
        self._file_scan_thread = None
        self._file_scan_worker = None

    def _on_file_scan_finished(self, payload: object, force: bool) -> None:
        if self._is_shutting_down:
            self._refresh_files_inflight = False
            return
        records: Dict[str, List[FileRecord]]
        dir_mtimes: Dict[str, float] = {}
        mode = "legacy"
        if isinstance(payload, dict) and "records" in payload:
            maybe_records = payload.get("records")
            if isinstance(maybe_records, dict):
                records = maybe_records
            else:
                records = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
            maybe_dirs = payload.get("dir_mtimes")
            if isinstance(maybe_dirs, dict):
                for k, v in maybe_dirs.items():
                    try:
                        dir_mtimes[os.path.normcase(os.path.normpath(str(k)))] = float(v or 0.0)
                    except Exception:
                        continue
            mode = str(payload.get("mode", "unknown") or "unknown")
        elif isinstance(payload, dict):
            records = payload  # type: ignore[assignment]
        else:
            records = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
        total_records = sum(len(v) for v in records.values())
        records_fp = self._files_records_fingerprint(records)
        unchanged_records = (not force) and (self._files_snapshot_fp == records_fp)
        try:
            with perf_span(
                "messages.file_scan_finished_handler",
                settings=self.settings,
                meta={
                    "force": bool(force),
                    "records": total_records,
                    "mode": mode,
                    "unchanged": bool(unchanged_records),
                },
                min_ms=5.0,
            ):
                if unchanged_records:
                    self.files = records
                    self._save_file_scan_cache_meta_only(dir_mtimes=dir_mtimes)
                    self._scan_cache_loaded = True
                else:
                    self.files = records
                    self._update_fldigi_senders(records)
                    self._read_state_map = self._load_read_state_map()
                    self._save_file_scan_cache(records, dir_mtimes=dir_mtimes)
                    self._scan_cache_loaded = True
                    self._refresh_varac_messages(force=force, rebuild=False)
                    self._populate_messages_table(force=force)
                self._start_signature_verification(force=force)
                self._files_snapshot_fp = records_fp
        finally:
            self._refresh_files_inflight = False
            self._last_file_refresh_ts = time.time()
            elapsed = time.time() - self._file_scan_start_ts
            emit_span(
                "messages.file_scan_total",
                elapsed * 1000.0,
                settings=self.settings,
                meta={
                    "force": bool(force),
                    "records": total_records,
                    "mode": mode,
                    "unchanged": bool(unchanged_records),
                },
                min_ms=5.0,
            )
            if elapsed > 0.5:
                log.debug("MessageViewer: refresh_files took %.2fs", elapsed)

    def _refresh_js8_messages(self, force: bool = False, rebuild: bool = True):
        if self._is_shutting_down:
            return
        with perf_span(
            "messages.refresh_js8_messages",
            settings=self.settings,
            meta={"force": bool(force), "rebuild": bool(rebuild)},
            min_ms=5.0,
        ):
            now = time.time()
            should_ingest = bool(force) or (
                now - float(self._last_js8_ingest_ts) >= self._js8_ingest_interval_sec
            )
            # First ingest any new messages into local cache, then load from local cache for display
            if should_ingest:
                try:
                    self._ingest_js8_messages()
                except Exception as e:
                    log.debug("MessageViewer: JS8 ingest failed: %s", e)
                try:
                    self._ingest_spotter_from_directed()
                except Exception as e:
                    log.debug("MessageViewer: spotter ingest failed: %s", e)
                self._last_js8_ingest_ts = now
            try:
                self._load_js8_from_local(force=force, rebuild=False)
            except Exception as e:
                log.debug("MessageViewer: JS8 local load failed: %s", e)
            try:
                self._load_spotter_from_db(force=force, rebuild=False)
            except Exception as e:
                log.debug("MessageViewer: spotter load failed: %s", e)
            try:
                self._load_sitrep_from_local(force=force, rebuild=False)
            except Exception as e:
                log.debug("MessageViewer: sitrep load failed: %s", e)
            if rebuild:
                self._populate_messages_table(force=force)

    def _refresh_sitrep_messages(self, force: bool = False, rebuild: bool = True) -> None:
        if self._is_shutting_down:
            return
        try:
            self._load_sitrep_from_local(force=force, rebuild=False)
        except Exception as e:
            log.debug("MessageViewer: sitrep refresh failed: %s", e)
        if rebuild:
            self._populate_messages_table(force=force)

    def _update_fldigi_senders(self, records: Dict[str, List[FileRecord]]) -> None:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return
        def _base_callsign(val: str) -> str:
            cs_norm = (val or "").strip().upper()
            if not cs_norm:
                return ""
            cs_norm = re.sub(r"/(P|M|MM|QRP|SOTA|ROVER|[A-Z0-9]{1,4})$", "", cs_norm)
            match = re.search(r"\b[A-Z]{1,2}\d[A-Z0-9]{1,4}\b", cs_norm)
            if match:
                return match.group(0)
            return cs_norm
        rows: Dict[str, tuple[float, str]] = {}
        for origin in ("flmsg", "flamp"):
            for rec in records.get(origin, []):
                sender = self._extract_sender_from_file(rec)
                if not sender:
                    continue
                sender = _base_callsign(sender)
                if not sender:
                    continue
                ts_val = float(rec.mtime or 0.0)
                existing = rows.get(sender)
                if not existing or ts_val > existing[0]:
                    rows[sender] = (ts_val, origin)
        if not rows:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            for cs, (ts_val, origin) in rows.items():
                cur.execute(
                    """
                    INSERT INTO fldigi_file_senders (callsign, last_seen_ts, origin)
                    VALUES (?, ?, ?)
                    ON CONFLICT(callsign) DO UPDATE SET
                        last_seen_ts=excluded.last_seen_ts,
                        origin=excluded.origin
                    """,
                    (cs, float(ts_val), origin),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to update fldigi senders: %s", e)

    def _refresh_varac_messages(self, force: bool = False, rebuild: bool = True) -> None:
        if self._is_shutting_down:
            return
        now = time.time()
        should_ingest = bool(force) or (
            now - float(self._last_varac_ingest_ts) >= self._varac_ingest_interval_sec
        )
        if should_ingest:
            try:
                ingest_varac(self.settings)
                self._last_varac_ingest_ts = now
            except Exception as e:
                log.debug("MessageViewer: VarAC ingest failed: %s", e)
        try:
            self._load_varac_from_local(force=force, rebuild=False)
        except Exception as e:
            log.debug("MessageViewer: VarAC load failed: %s", e)
        if rebuild:
            self._populate_messages_table(force=force)

    def _cycle_flag_state(self, payload: object) -> None:
        current = int(getattr(payload, "flag_state", 0) or 0)
        next_state = 1 if current == 0 else (2 if current == 1 else 0)
        try:
            setattr(payload, "flag_state", next_state)
        except Exception:
            pass
        if isinstance(payload, JS8Message):
            self._set_js8_flag(payload.msg_id, next_state)
        elif isinstance(payload, SpotterMessage):
            self._set_spotter_flag(payload.spotter_id, next_state)
        elif isinstance(payload, VarACMessage):
            self._set_varac_flag(payload, next_state)
        elif isinstance(payload, FileRecord):
            self._set_file_flag(payload, next_state)
        self._populate_messages_table(force=True)

    def _set_js8_flag(self, msg_id: int, flag_state: int) -> None:
        db_path = self._local_js8_db()
        if not db_path or not Path(db_path).exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("UPDATE js8_messages SET flag_state=? WHERE id=?", (int(flag_state), int(msg_id)))
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to update JS8 flag: %s", e)

    def _set_spotter_flag(self, msg_id: int, flag_state: int) -> None:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("UPDATE spotter_traffic SET flag_state=? WHERE id=?", (int(flag_state), int(msg_id)))
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to update Spotter flag: %s", e)

    def _set_varac_flag(self, msg: VarACMessage, flag_state: int) -> None:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "UPDATE varac_messages SET flag_state=? WHERE source=? AND id=?",
                (int(flag_state), msg.source, int(msg.msg_id)),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to update VarAC flag: %s", e)

    def _set_file_flag(self, rec: FileRecord, flag_state: int) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        key = self._read_state_key(rec.origin, rec)
        status, read_ts, _ = self._read_state_map.get(key, ("NEW", 0.0, 0))
        self._read_state_map[key] = (status, read_ts, int(flag_state))
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT OR REPLACE INTO message_read_state
                    (origin, path, mtime, size, status, read_ts, flag_state)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rec.origin,
                    str(rec.path),
                    float(rec.mtime),
                    int(rec.size),
                    status,
                    float(read_ts),
                    int(flag_state),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to update file flag: %s", e)

    def _load_varac_from_local(self, force: bool = False, rebuild: bool = True) -> None:
        db_path = self._db_path()
        msgs: List[VarACMessage] = []
        if not db_path or not db_path.exists():
            self.varac_messages = msgs
            if rebuild:
                self._populate_messages_table(force=force)
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, guid, source, msg_type, from_call, to_call, subject, body,
                       ts, band, freq_hz, snr, read_status, folder, vmail_guid, is_deleted, flag_state
                FROM varac_messages
                WHERE COALESCE(is_deleted, 0) = 0
                ORDER BY ts DESC
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load varac messages: %s", e)
            rows = []
        for r in rows:
            msg_type = (r[3] or "")
            if msg_type.strip().upper() == "QSO":
                continue
            msg = VarACMessage(
                msg_id=int(r[0]),
                guid=(r[1] or ""),
                source=(r[2] or ""),
                msg_type=msg_type,
                from_call=(r[4] or "").strip().upper(),
                to_call=(r[5] or "").strip().upper(),
                subject=(r[6] or ""),
                body=(r[7] or ""),
                ts=float(r[8] or 0.0),
                band=(r[9] or ""),
                freq_hz=float(r[10]) if r[10] not in (None, "") else None,
                snr=float(r[11]) if r[11] not in (None, "") else None,
                read_status=int(r[12] or 0),
                folder=str(r[13] or ""),
                vmail_guid=(r[14] or ""),
                flag_state=int(r[16] or 0),
            )
            msgs.append(msg)
        self.varac_messages = msgs
        if rebuild:
            self._populate_messages_table(force=force)

    @staticmethod
    def _is_truthy(value: object, default: bool = False) -> bool:
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

    def _load_sitrep_from_local(self, force: bool = False, rebuild: bool = True) -> None:
        db_path = self._db_path()
        msgs: List[SitrepMessage] = []
        enabled = self._is_truthy(self.settings.get("sitrep_unified_messages_enabled", True), True)
        if not enabled:
            self.sitrep_messages = msgs
            if rebuild:
                self._populate_messages_table(force=force)
            return
        if not db_path or not db_path.exists():
            self.sitrep_messages = msgs
            if rebuild:
                self._populate_messages_table(force=force)
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, report_key, event_ts, event_ts_utc, from_call, target, grid, scope,
                       subtype, overall_status, power, water, medical, communications, internet,
                       travel, food, fuel, crime, civil_unrest, political,
                       source_first, source_last, source_count, sources_json, source_refs_json, raw_payload_json, updated_ts
                FROM sitrep_events
                ORDER BY event_ts DESC, id DESC
                LIMIT 5000
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load sitrep events: %s", e)
            rows = []
        for r in rows:
            msg = SitrepMessage(
                event_id=int(r[0] or 0),
                report_key=str(r[1] or ""),
                event_ts=float(r[2] or 0.0),
                event_ts_utc=str(r[3] or ""),
                from_call=str(r[4] or "").strip().upper(),
                target=str(r[5] or "").strip().upper(),
                grid=str(r[6] or "").strip().upper(),
                scope=str(r[7] or "").strip(),
                subtype=str(r[8] or "").strip().upper(),
                overall_status=str(r[9] or "").strip().lower(),
                power=str(r[10] or "").strip().lower(),
                water=str(r[11] or "").strip().lower(),
                medical=str(r[12] or "").strip().lower(),
                communications=str(r[13] or "").strip().lower(),
                internet=str(r[14] or "").strip().lower(),
                travel=str(r[15] or "").strip().lower(),
                food=str(r[16] or "").strip().lower(),
                fuel=str(r[17] or "").strip().lower(),
                crime=str(r[18] or "").strip().lower(),
                civil_unrest=str(r[19] or "").strip().lower(),
                political=str(r[20] or "").strip().lower(),
                source_first=str(r[21] or "").strip().upper(),
                source_last=str(r[22] or "").strip().upper(),
                source_count=int(r[23] or 0),
                sources_json=str(r[24] or ""),
                source_refs_json=str(r[25] or ""),
                raw_payload_json=str(r[26] or ""),
                updated_ts=float(r[27] or 0.0),
            )
            msgs.append(msg)
        self.sitrep_messages = msgs
        if rebuild:
            self._populate_messages_table(force=force)

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
        table_style = (
            f"QTableView {{ gridline-color: {grid}; }}"
            "QTableView::indicator {"
            " width: 14px; height: 14px;"
            f" border: 1px solid {theme['text_muted']};"
            f" background-color: {theme['surface']};"
            "}"
            "QTableView::indicator:checked {"
            f" background-color: {theme['accent']};"
            f" border: 1px solid {theme['accent']};"
            "}"
        )
        self.messages_table.setStyleSheet(table_style)
        self.pending_table.setStyleSheet(f"QTableWidget {{ gridline-color: {grid}; }}")
        if self._actions_delegate:
            self._actions_delegate._danger = QColor(theme["danger"])
        if self.loading_label:
            bg = theme.get("surface_alt", theme.get("surface", "#f2f2f2"))
            fg = theme.get("accent", theme.get("text", "#222"))
            border = theme.get("border", "#ccc")
            self.loading_label.setStyleSheet(
                f"padding: 2px 6px; border-radius: 4px; background: {bg}; color: {fg}; border: 1px solid {border};"
            )
        header = self.messages_table.horizontalHeader()
        if isinstance(header, MessageHeaderWithCheckbox):
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
        self._update_time_toggle_style(theme)
        self._update_clear_filters_style()
        self._update_mark_all_read_style()
        self._apply_accessibility_width_guards()
        self._update_pending_table()

    def shutdown(self) -> None:
        self._is_shutting_down = True
        try:
            if self._persist_timer and self._persist_timer.isActive():
                self._persist_timer.stop()
        except Exception:
            pass
        try:
            self._flush_persist_ops()
        except Exception:
            pass
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
            if self._bbs_auto_archive_timer:
                self._bbs_auto_archive_timer.stop()
        except Exception:
            pass
        try:
            if self._filter_timer:
                self._filter_timer.stop()
        except Exception:
            pass
        try:
            if self._file_scan_thread and self._file_scan_thread.isRunning():
                self._file_scan_thread.quit()
                self._file_scan_thread.wait(1000)
        except Exception:
            pass
        try:
            if self._rows_build_thread and self._rows_build_thread.isRunning():
                self._rows_build_thread.quit()
                self._rows_build_thread.wait(1000)
        except Exception:
            pass
        try:
            if self._signature_verify_thread and self._signature_verify_thread.isRunning():
                self._signature_verify_thread.quit()
                self._signature_verify_thread.wait(1000)
        except Exception:
            pass
        try:
            if self._bbs_auto_archive_thread and self._bbs_auto_archive_thread.isRunning():
                self._bbs_auto_archive_thread.quit()
                self._bbs_auto_archive_thread.wait(1000)
        except Exception:
            pass

    def on_settings_saved(self) -> None:
        prev_watch_sig = ""
        prev_auth_sig: tuple = tuple()
        try:
            prev_watch_sig = self._watch_dirs_signature(self._effective_watch_dirs())
            prev_auth_sig = self._auth_refresh_signature()
        except Exception:
            prev_watch_sig = ""
            prev_auth_sig = tuple()
        try:
            if hasattr(self.settings, "reload"):
                self.settings.reload()
        except Exception:
            pass
        watch_sig_changed = False
        auth_sig_changed = False
        try:
            watch_sig_changed = prev_watch_sig != self._watch_dirs_signature(self._effective_watch_dirs())
            auth_sig_changed = prev_auth_sig != self._auth_refresh_signature()
        except Exception:
            watch_sig_changed = False
            auth_sig_changed = False
        try:
            if watch_sig_changed:
                # Only rescan file watches when a watch-root-affecting setting changed.
                self._refresh_files(force=False)
        except Exception:
            if watch_sig_changed:
                try:
                    self._refresh_varac_messages(force=True)
                except Exception:
                    pass
        if auth_sig_changed:
            try:
                self._start_signature_verification(force=True)
            except Exception:
                pass
        try:
            if self._has_active_view:
                self._queue_bbs_auto_archive_check("settings_saved", delay_ms=1500)
        except Exception:
            pass
        self._update_pending_table()

    def _auth_refresh_signature(self) -> tuple:
        try:
            trusted_signers_raw = self.settings.get("gpg_trusted_signers", []) or []
            if not isinstance(trusted_signers_raw, list):
                trusted_signers_raw = []
            trusted_signers = tuple(sorted(normalize_fingerprints(str(v) for v in trusted_signers_raw)))
        except Exception:
            trusted_signers = tuple()
        try:
            inline_suffixes = tuple(self._inline_signature_name_suffixes())
        except Exception:
            inline_suffixes = tuple()
        return (
            bool(self.settings.get("gpg_verify_flamp_k2s_enabled", False)),
            bool(self.settings.get("hash_verify_flamp_k2s_enabled", True)),
            str(self.settings.get("gpg_executable_path", "") or "").strip(),
            trusted_signers,
            inline_suffixes,
            str(self._trusted_hash_set_signature() or ""),
        )

    def _queue_persist_op(self, op: str, payload: Tuple) -> None:
        self._pending_persist_ops.append((op, payload))
        if self._persist_timer is None:
            self._persist_timer = QTimer(self)
            self._persist_timer.setSingleShot(True)
            self._persist_timer.timeout.connect(self._flush_persist_ops)
        if not self._persist_timer.isActive():
            self._persist_timer.start(120)

    def _flush_persist_ops(self) -> None:
        if not self._pending_persist_ops:
            return
        ops = self._pending_persist_ops
        self._pending_persist_ops = []
        for op, payload in ops:
            try:
                if op == "file_read_state":
                    origin, path_txt, mtime, size, status, read_ts, flag_state = payload
                    self._persist_file_read_state(
                        str(origin),
                        str(path_txt),
                        float(mtime),
                        int(size),
                        str(status),
                        float(read_ts),
                        int(flag_state),
                    )
                elif op == "spotter_read":
                    spotter_id, read_ts = payload
                    self._persist_spotter_read(int(spotter_id), float(read_ts))
                elif op == "varac_read":
                    source, msg_id = payload
                    self._persist_varac_read(str(source), int(msg_id))
                elif op == "js8_read":
                    msg_id, utc_ts, read_ts, sync_flag = payload
                    self._persist_js8_read(int(msg_id), float(utc_ts), float(read_ts), bool(sync_flag))
            except Exception as e:
                log.debug("MessageViewer: deferred persist op failed (%s): %s", op, e)

    def _persist_file_read_state(
        self,
        origin: str,
        path_txt: str,
        mtime: float,
        size: int,
        status: str,
        read_ts: float,
        flag_state: int,
    ) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT OR REPLACE INTO message_read_state
                    (origin, path, mtime, size, status, read_ts, flag_state)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (origin, path_txt, float(mtime), int(size), status, float(read_ts), int(flag_state)),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to persist read state: %s", e)

    def _persist_spotter_read(self, spotter_id: int, read_ts: float) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "UPDATE spotter_traffic SET state='READ', read_ts=? WHERE id=?",
                (float(read_ts), int(spotter_id)),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to update spotter read state: %s", e)

    def _persist_varac_read(self, source: str, msg_id: int) -> None:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "UPDATE varac_messages SET read_status=1 WHERE source=? AND id=?",
                (source, int(msg_id)),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    def _persist_js8_read(self, msg_id: int, utc_ts: float, read_ts: float, sync_flag: bool) -> None:
        try:
            self._save_js8_state(msg_id, "READ", utc_ts, read_ts=read_ts)
            self._update_local_read(msg_id, read_ts)
        except Exception as e:
            log.debug("MessageViewer: failed to persist JS8 READ state: %s", e)
        if sync_flag:
            try:
                ok = self._mark_js8call_inbox_read_by_id(msg_id)
                if not ok:
                    log.debug(
                        "MessageViewer: JS8Call inbox mark READ failed (msg_id=%s)",
                        msg_id,
                    )
            except Exception:
                pass

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

    def _minutes_to_next_change(self) -> Optional[int]:
        if suspend_active(self.settings) or not scheduler_enabled(self.settings):
            return None
        win = self.window()
        sched = getattr(win, "scheduler", None) if win is not None else None
        dt = getattr(sched, "next_change_utc", None) if sched is not None else None
        if not dt:
            return None
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        delta = (dt - now_utc).total_seconds()
        if delta <= 0:
            return None
        return int((delta + 59) // 60)

    def _on_pending_get(self, callsign: str, msg_id: str) -> None:
        if not callsign or not msg_id:
            return
        mycall = self._my_callsign()
        if not mycall:
            QMessageBox.warning(self, "Missing Callsign", "Configure your callsign in the Settings tab.")
            return
        minutes = self._minutes_to_next_change()
        if minutes is not None and minutes <= 5:
            resp = QMessageBox.question(
                self,
                "Frequency Change",
                f"Frequency Change in {minutes} minutes. Continue?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if resp != QMessageBox.Yes:
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

    def _current_time_mode(self) -> str:
        if self._time_mode_override:
            return self._time_mode_override
        return "LOCAL" if self._show_local_time else "UTC"

    def _current_timezone_label(self) -> tuple[str, str]:
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        now = datetime.datetime.now(tz)
        fallback = now.tzname() or tz_name
        abbr = self._ui_tz_abbr(tz_name, fallback)
        if abbr and len(abbr) > 5:
            abbr = self._ui_tz_abbr(tz_name, abbr)
        return str(tz_name), str(abbr)

    def _ui_tz_abbr(self, tz_name: str, fallback: str) -> str:
        mapping = {
            "UTC": "UTC",
            "America/New_York": "ET",
            "America/Chicago": "CT",
            "America/Denver": "MT",
            "America/Los_Angeles": "PT",
            "Mountain Standard Time": "MST",
            "Central Standard Time": "CST",
            "Eastern Standard Time": "EST",
            "Pacific Standard Time": "PST",
        }
        return mapping.get(tz_name, fallback)

    def _fmt_ts(self, ts: float) -> str:
        if not ts:
            return ""
        try:
            mode = self._current_time_mode()
            if mode == "UTC":
                return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            tz = get_timezone(tz_name)
            return datetime.datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""

    def _format_rcv_display(self, ts: float, utc_str: str | None) -> str:
        mode = self._current_time_mode()
        if mode == "UTC":
            if utc_str:
                return utc_str
            return self._fmt_ts(ts)
        if ts:
            return self._fmt_ts(ts)
        if utc_str:
            try:
                dt = datetime.datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
                tz_name = self.settings.get("timezone", "UTC") or "UTC"
                tz = get_timezone(tz_name)
                return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return utc_str
        return ""

    def _update_time_ui(self) -> None:
        mode = self._current_time_mode()
        tz_name, tz_abbr = self._current_timezone_label()
        if mode == "UTC":
            self.time_toggle_btn.setText("Showing: UTC")
            self._messages_model.set_time_header("RCV_DT (UTC)")
            self.pending_table.setHorizontalHeaderLabels(
                ["Callsign", "Msg ID", "Last Seen (UTC)", "Status", "Actions"]
            )
        else:
            self.time_toggle_btn.setText("Showing: Local")
            self._messages_model.set_time_header(f"RCV_DT ({tz_abbr})")
            self.pending_table.setHorizontalHeaderLabels(
                ["Callsign", "Msg ID", f"Last Seen ({tz_abbr})", "Status", "Actions"]
            )
        self._update_time_toggle_style()

    def _update_time_toggle_style(self, theme: Optional[Dict[str, str]] = None) -> None:
        if theme is None:
            theme = resolve_theme(self.settings)
        role = "info" if self._current_time_mode() == "UTC" else "muted"
        self.time_toggle_btn.setStyleSheet(button_style(role, theme))

    def _toggle_time_view(self) -> None:
        mode = self._current_time_mode()
        self._time_mode_override = "UTC" if mode == "LOCAL" else "LOCAL"
        if (self._time_mode_override == "LOCAL" and self._show_local_time) or (
            self._time_mode_override == "UTC" and not self._show_local_time
        ):
            self._time_mode_override = None
        self._update_time_ui()
        self._populate_messages_table(force=True)

    def _populate_messages_table(self, force: bool = False):
        with perf_span(
            "messages.populate_table",
            settings=self.settings,
            meta={"force": bool(force)},
            min_ms=5.0,
        ):
            if self._freeze_messages_table and not force:
                self._deferred_refresh = True
                log.debug("MessageViewer: table refresh deferred (freeze active)")
                return
            self._deferred_refresh = False
            self._start_rows_build(force=force)

    def _snapshot_for_rows_build(self) -> Dict[str, object]:
        form_ids: set[str] = set()
        for msg in self.js8_messages:
            if isinstance(msg.msg_type, str) and msg.msg_type.startswith("F!"):
                form_ids.add(msg.msg_type[2:].strip())
        for msg in self.spotter_messages:
            mtype = str(msg.msg_type or "")
            if mtype.startswith("F!"):
                form_ids.add(mtype[2:].strip())
        form_titles: Dict[str, str] = {}
        for form_id in sorted({f for f in form_ids if f}):
            form_titles[form_id] = self._load_form_title(form_id)
        signature_map: Dict[tuple, Dict[str, object]] = {}
        if self._is_any_auth_verification_enabled():
            current_sig_keys: set[tuple] = set()
            for rec in self.files.get("flamp", []):
                if not self._is_flamp_auth_file(rec):
                    continue
                current_sig_keys.add(self._signature_cache_key(rec))
            for key in current_sig_keys:
                state = self._signature_state_map.get(key)
                if not isinstance(key, tuple) or len(key) != 4:
                    continue
                if not isinstance(state, FileSignatureState):
                    continue
                ui_state, ui_detail, ui_trusted = self._derive_auth_ui(state)
                signature_map[key] = {
                    "status": ui_state,
                    "detail": ui_detail,
                    "trusted": ui_trusted,
                }
        return {
            "js8_messages": list(self.js8_messages),
            "spotter_messages": list(self.spotter_messages),
            "varac_messages": list(self.varac_messages),
            "sitrep_messages": list(self.sitrep_messages),
            "files": {k: list(v) for k, v in self.files.items()},
            "read_state_map": dict(self._read_state_map),
            "signature_state_map": signature_map,
            "sender_cache_seed": dict(self._sender_cache),
            "form_titles": form_titles,
            "show_local_time": self._current_time_mode() != "UTC",
            "tz_name": str(self.settings.get("timezone", "UTC") or "UTC"),
            "sitrep_dedupe_enabled": self._is_truthy(
                self.settings.get("sitrep_messages_dedupe_enabled", True), True
            ),
            "sitrep_show_raw_duplicates": self._is_truthy(
                self.settings.get("sitrep_messages_show_raw_duplicates", False), False
            ),
        }

    def _start_rows_build(self, force: bool = False) -> None:
        if self._is_shutting_down:
            return
        if self._rows_build_thread:
            try:
                if self._rows_build_thread.isRunning():
                    self._rows_build_pending = True
                    self._rows_build_pending_force = bool(self._rows_build_pending_force or force)
                    return
            except RuntimeError:
                self._rows_build_thread = None
                self._rows_build_worker = None
        snapshot = self._snapshot_for_rows_build()
        self._rows_build_generation += 1
        generation = int(self._rows_build_generation)
        self._rows_build_pending = False
        self._rows_build_pending_force = False
        self._rows_build_thread = QThread(self)
        self._rows_build_worker = _RowsBuildWorker(
            js8_messages=snapshot.get("js8_messages", []),  # type: ignore[arg-type]
            spotter_messages=snapshot.get("spotter_messages", []),  # type: ignore[arg-type]
            varac_messages=snapshot.get("varac_messages", []),  # type: ignore[arg-type]
            sitrep_messages=snapshot.get("sitrep_messages", []),  # type: ignore[arg-type]
            files=snapshot.get("files", {}),  # type: ignore[arg-type]
            read_state_map=snapshot.get("read_state_map", {}),  # type: ignore[arg-type]
            signature_state_map=snapshot.get("signature_state_map", {}),  # type: ignore[arg-type]
            sender_cache_seed=snapshot.get("sender_cache_seed", {}),  # type: ignore[arg-type]
            form_titles=snapshot.get("form_titles", {}),  # type: ignore[arg-type]
            show_local_time=bool(snapshot.get("show_local_time", False)),
            tz_name=str(snapshot.get("tz_name", "UTC") or "UTC"),
            sitrep_dedupe_enabled=bool(snapshot.get("sitrep_dedupe_enabled", True)),
            sitrep_show_raw_duplicates=bool(snapshot.get("sitrep_show_raw_duplicates", False)),
            force=bool(force),
            generation=generation,
        )
        self._rows_build_worker.moveToThread(self._rows_build_thread)
        self._rows_build_thread.started.connect(self._rows_build_worker.run)
        self._rows_build_worker.finished.connect(self._on_rows_build_finished)
        self._rows_build_worker.finished.connect(self._rows_build_thread.quit)
        self._rows_build_worker.finished.connect(self._rows_build_worker.deleteLater)
        self._rows_build_thread.finished.connect(self._on_rows_build_thread_finished)
        self._rows_build_thread.finished.connect(self._rows_build_thread.deleteLater)
        self._rows_build_thread.start()

    def _on_rows_build_thread_finished(self) -> None:
        self._rows_build_thread = None
        self._rows_build_worker = None
        if self._rows_build_pending and not self._is_shutting_down:
            pending_force = bool(self._rows_build_pending_force)
            self._rows_build_pending = False
            self._rows_build_pending_force = False
            self._start_rows_build(force=pending_force)

    def _on_rows_build_finished(self, payload: object) -> None:
        if self._is_shutting_down:
            return
        data = payload if isinstance(payload, dict) else {}
        try:
            generation = int(data.get("generation", 0) or 0)
        except Exception:
            generation = 0
        if generation and generation != self._rows_build_generation:
            return
        rows = data.get("rows", [])
        if not isinstance(rows, list):
            rows = []
        self._message_rows = rows
        sender_updates = data.get("sender_cache_updates", {})
        if isinstance(sender_updates, dict):
            self._sender_cache.update(sender_updates)
            self._prune_cache(self._sender_cache, self._cache_max_sender_entries)
        try:
            build_ms = float(data.get("elapsed_ms", 0.0) or 0.0)
        except Exception:
            build_ms = 0.0
        if build_ms > 0:
            emit_span(
                "messages.build_rows",
                build_ms,
                settings=self.settings,
                meta={"rows": len(rows), "force": bool(data.get("force", False))},
                min_ms=5.0,
            )
        if self._freeze_messages_table and not bool(data.get("force", False)):
            self._deferred_refresh = True
            return
        with perf_span("messages.refresh_filters", settings=self.settings, min_ms=5.0):
            self._refresh_message_filters(rows)
        with perf_span("messages.apply_filters", settings=self.settings, min_ms=5.0):
            self._apply_message_filters()
        self._start_signature_verification(force=bool(data.get("force", False)))
        log.debug("MessageViewer: built %d unified messages", len(rows))

    def _refresh_message_filters(self, rows: List[UnifiedMessage]) -> None:
        type_vals = sorted({r.msg_type for r in rows if r.msg_type})
        status_vals = sorted({r.status for r in rows if r.status})
        from_vals = sorted({r.from_call for r in rows if r.from_call})
        to_vals = sorted({r.to_call for r in rows if r.to_call})
        spotter_forms = sorted({t for t in type_vals if re.match(r"^F!\d+$", t)})
        base_types = sorted([t for t in type_vals if t not in spotter_forms])
        sitrep_subtypes = sorted(
            {
                str(getattr(r.payload, "subtype", "") or "").strip().upper()
                for r in rows
                if r.msg_type == "SitRep"
            }
        )
        sitrep_subtypes = [s for s in sitrep_subtypes if s]
        if spotter_forms:
            type_vals = base_types + ["Spotter"] + spotter_forms
        else:
            type_vals = base_types
        if sitrep_subtypes:
            if "SitRep" in type_vals:
                idx = type_vals.index("SitRep") + 1
            else:
                type_vals.append("SitRep")
                idx = len(type_vals)
            sitrep_filters = [f"SitRep/{s}" for s in sitrep_subtypes]
            type_vals[idx:idx] = sitrep_filters
        if any(getattr(r.payload, "flag_state", 0) == 1 for r in rows):
            if "Action Needed" not in status_vals:
                status_vals.append("Action Needed")

        current_type = self.type_filter.currentText() if hasattr(self, "type_filter") else "ALL"
        current_status = self.status_filter.currentText() if hasattr(self, "status_filter") else "ALL"
        current_from = self.from_filter.currentText() if hasattr(self, "from_filter") else ""
        current_to = self.to_filter.currentText() if hasattr(self, "to_filter") else ""
        if not current_type:
            current_type = "ALL"
        if not current_status:
            current_status = "ALL"
        if not current_from:
            current_from = ""
        if not current_to:
            current_to = ""

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
        self.from_filter.addItems(from_vals)
        if current_from in from_vals:
            self.from_filter.setCurrentText(current_from)
        else:
            self.from_filter.setCurrentText("")
        self.from_filter.blockSignals(False)

        self.to_filter.blockSignals(True)
        self.to_filter.clear()
        self.to_filter.addItems(to_vals)
        if current_to in to_vals:
            self.to_filter.setCurrentText(current_to)
        else:
            self.to_filter.setCurrentText("")
        self.to_filter.blockSignals(False)
        self._filters_initialized = True
        self._fit_filter_combo_popup(self.type_filter)
        self._fit_filter_combo_popup(self.status_filter)
        self._fit_filter_combo_popup(self.from_filter)
        self._fit_filter_combo_popup(self.to_filter)

    def _apply_message_filters(self) -> None:
        rows = self._message_rows
        type_sel = self.type_filter.currentText() if hasattr(self, "type_filter") else "MSG Type..."
        status_sel = self.status_filter.currentText() if hasattr(self, "status_filter") else "Status..."
        from_sel = self.from_filter.currentText() if hasattr(self, "from_filter") else ""
        to_sel = self.to_filter.currentText() if hasattr(self, "to_filter") else ""
        rcv_query = (self.rcv_search.text() if hasattr(self, "rcv_search") else "").strip().lower()

        filtered = []
        for row in rows:
            if type_sel == "Spotter":
                if not (row.msg_type or "").startswith("F!"):
                    continue
            elif type_sel == "SitRep":
                if row.msg_type != "SitRep":
                    continue
            elif type_sel.startswith("SitRep/"):
                subtype = type_sel.split("/", 1)[1].strip().upper()
                if row.msg_type != "SitRep":
                    continue
                row_subtype = str(getattr(row.payload, "subtype", "") or "").strip().upper()
                if row_subtype != subtype:
                    continue
            elif type_sel != "MSG Type..." and row.msg_type != type_sel:
                continue
            if status_sel != "Status...":
                if status_sel == "Action Needed":
                    if getattr(row.payload, "flag_state", 0) != 1:
                        continue
                elif row.status != status_sel:
                    continue
            if from_sel and row.from_call != from_sel:
                continue
            if to_sel and row.to_call != to_sel:
                continue
            if rcv_query:
                hay = row.search_text
                if not hay:
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
                    row.search_text = hay
                if rcv_query not in hay:
                    continue
            filtered.append(row)
        if (
            type_sel == "BBS"
            and self._sort_column == self._default_sort_column
            and self._sort_order == self._default_sort_order
        ):
            filtered = sorted(filtered, key=lambda r: r.rcv_ts or 0.0)
        else:
            filtered = self._sort_rows(filtered)
        self._render_messages_table(filtered)
        self._update_clear_filters_style()
        self._update_mark_all_read_style()
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
        from_sel = self.from_filter.currentText() if hasattr(self, "from_filter") else ""
        to_sel = self.to_filter.currentText() if hasattr(self, "to_filter") else ""
        if type_sel not in ("", "MSG Type..."):
            return True
        if status_sel not in ("", "Status..."):
            return True
        if from_sel:
            return True
        if to_sel:
            return True
        if (self.rcv_search.text() if hasattr(self, "rcv_search") else "").strip():
            return True
        if (
            self._sort_column != self._default_sort_column
            or self._sort_order != self._default_sort_order
        ):
            return True
        return False

    def _requires_full_refresh_after_read(self) -> bool:
        status_sel = self.status_filter.currentText() if hasattr(self, "status_filter") else "Status..."
        if status_sel not in ("", "Status..."):
            return True
        # Sorting by status can change row ordering when NEW -> READ.
        if self._sort_column == 2:
            return True
        # Search text may include status terms and would require recompute.
        query = (self.rcv_search.text() if hasattr(self, "rcv_search") else "").strip().lower()
        if query:
            return True
        return False

    def _is_filter_active(self) -> bool:
        type_sel = self.type_filter.currentText() if hasattr(self, "type_filter") else "MSG Type..."
        status_sel = self.status_filter.currentText() if hasattr(self, "status_filter") else "Status..."
        from_sel = self.from_filter.currentText() if hasattr(self, "from_filter") else ""
        to_sel = self.to_filter.currentText() if hasattr(self, "to_filter") else ""
        if type_sel not in ("", "MSG Type..."):
            return True
        if status_sel not in ("", "Status..."):
            return True
        if from_sel:
            return True
        if to_sel:
            return True
        if (self.rcv_search.text() if hasattr(self, "rcv_search") else "").strip():
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
                idx = self._messages_model.index(i, 2)
                self._messages_model.dataChanged.emit(
                    idx, idx, [Qt.DisplayRole, Qt.ForegroundRole]
                )

    def _refresh_table_after_read(self, match_fn, row_ref: Optional[UnifiedMessage] = None) -> None:
        updated = False
        if row_ref is not None:
            row_ref.status = "READ"
            updated = True
        else:
            for row in self._message_rows:
                if match_fn(row):
                    row.status = "READ"
                    updated = True
        if not updated:
            return
        if self._requires_full_refresh_after_read():
            self._refresh_message_filters(self._message_rows)
            self._apply_message_filters_preserve_scroll()
        else:
            if row_ref is not None and hasattr(self, "_messages_model"):
                if not self._messages_model.mark_row_read(row_ref):
                    self._update_rendered_status(match_fn)
            else:
                self._update_rendered_status(match_fn)
            self._update_mark_all_read_style()

    def _clear_filters(self) -> None:
        self._unfreeze_table()
        if (
            self.type_filter.currentText() in ("", "MSG Type...")
            and self.status_filter.currentText() in ("", "Status...")
            and self.from_filter.currentText() in ("",)
            and self.to_filter.currentText() in ("",)
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
        self.from_filter.setCurrentText("")
        self.to_filter.setCurrentText("")
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
        self._update_bulk_delete_buttons()
        self._update_mark_all_read_style()

    def _update_bulk_delete_buttons(self) -> None:
        theme = resolve_theme(self.settings)
        count = len(self._messages_model.selected_rows())
        has_selection = count > 0
        if hasattr(self, "delete_selected_btn"):
            self.delete_selected_btn.setEnabled(has_selection)
            role = "eligible_danger" if has_selection else "muted"
            self.delete_selected_btn.setStyleSheet(button_style(role, theme))
        self._sync_select_all_checkbox()
        self._update_mark_all_read_style()

    def _delete_selected_messages(self) -> None:
        rows = self._messages_model.selected_rows()
        deletable = self._collect_deletable_rows(rows)
        if not deletable:
            QMessageBox.information(self, "Delete Selected", "No deletable messages selected.")
            return
        if not self._confirm_bulk_delete(deletable, "Delete selected messages?"):
            return
        self._bulk_delete_rows(deletable)

    @staticmethod
    def _collect_deletable_rows(rows: List[UnifiedMessage]) -> List[UnifiedMessage]:
        out: List[UnifiedMessage] = []
        for row in rows:
            if MessageTableModel._row_key(row) is not None:
                out.append(row)
        return out

    @staticmethod
    def _summarize_types(rows: List[UnifiedMessage]) -> str:
        counts: Dict[str, int] = {}
        for row in rows:
            label = (row.msg_type or row.origin or "").strip() or "Unknown"
            counts[label] = counts.get(label, 0) + 1
        parts = [f"{k}: {counts[k]}" for k in sorted(counts)]
        return ", ".join(parts)

    def _confirm_bulk_delete(self, rows: List[UnifiedMessage], prompt: str) -> bool:
        summary = self._summarize_types(rows)
        msg = f"{prompt}\n\n{len(rows)} messages\n{summary}"
        resp = QMessageBox.question(self, "Delete Messages", msg, QMessageBox.Yes | QMessageBox.No)
        return resp == QMessageBox.Yes

    def _bulk_delete_rows(self, rows: List[UnifiedMessage]) -> None:
        deleted = 0
        failed = 0
        skipped = 0
        for row in rows:
            payload = row.payload
            if isinstance(payload, JS8Message):
                msg_id = int(getattr(payload, "msg_id", 0) or 0)
                if msg_id <= 0:
                    skipped += 1
                    continue
                if not self._delete_js8_inbox_row(msg_id):
                    failed += 1
                    continue
                self._delete_js8_local_row(msg_id)
                self.js8_messages = [m for m in self.js8_messages if m.msg_id != msg_id]
                if self.current_js8 and self.current_js8.msg_id == msg_id:
                    self.current_js8 = None
                    self._has_active_view = False
                    self.info_label.setText("No message selected")
                    self.viewer.clear()
                deleted += 1
            elif isinstance(payload, VarACMessage):
                msg_id = int(getattr(payload, "msg_id", 0) or 0)
                if msg_id <= 0:
                    skipped += 1
                    continue
                if not self._soft_delete_varac_row(payload):
                    failed += 1
                    continue
                self._delete_varac_local_row(payload)
                self.varac_messages = [
                    m for m in self.varac_messages if m.msg_id != msg_id or m.source != payload.source
                ]
                deleted += 1
            elif isinstance(payload, SpotterMessage):
                msg_id = int(getattr(payload, "spotter_id", 0) or 0)
                if msg_id <= 0:
                    skipped += 1
                    continue
                if not self._delete_spotter_row(msg_id):
                    failed += 1
                    continue
                self.spotter_messages = [m for m in self.spotter_messages if m.spotter_id != msg_id]
                if self.current_js8 is None and self.current_record is None:
                    self._has_active_view = False
                    self.info_label.setText("No message selected")
                    self.viewer.clear()
                deleted += 1
            elif isinstance(payload, FileRecord):
                if not payload.path.exists():
                    skipped += 1
                    continue
                ok = self._send_to_recycle_bin(payload.path)
                if not ok:
                    failed += 1
                    continue
                self._remove_file_record(payload)
                deleted += 1
            else:
                skipped += 1
        self._messages_model.clear_selection()
        self._unfreeze_table()
        self._populate_messages_table(force=True)
        summary = self._summarize_types(rows)
        details = f"Deleted {deleted} messages.\n{summary}"
        if skipped:
            details = f"{details}\nSkipped: {skipped}"
        if failed:
            details = f"{details}\nFailed: {failed}"
        QMessageBox.information(self, "Delete Messages", details)

    def _mark_rows_read_bulk(self, rows: List[UnifiedMessage]) -> int:
        ts = time.time()
        js8_rows: List[JS8Message] = []
        spotter_rows: List[SpotterMessage] = []
        varac_rows: List[VarACMessage] = []
        file_rows: List[FileRecord] = []
        for row in rows:
            payload = row.payload
            if isinstance(payload, JS8Message) and payload.msg_id > 0 and payload.state.upper() != "READ":
                js8_rows.append(payload)
            elif isinstance(payload, SpotterMessage) and payload.spotter_id > 0 and payload.state.upper() != "READ":
                spotter_rows.append(payload)
            elif isinstance(payload, VarACMessage) and payload.msg_id > 0 and int(payload.read_status or 0) == 0:
                varac_rows.append(payload)
            elif isinstance(payload, FileRecord):
                key = self._read_state_key(payload.origin, payload)
                status = (self._read_state_map.get(key, ("NEW", 0.0, 0))[0] or "").upper()
                if status != "READ":
                    file_rows.append(payload)

        if js8_rows:
            self._mark_js8_rows_read_bulk(js8_rows, ts)
        if spotter_rows:
            self._mark_spotter_rows_read_bulk(spotter_rows, ts)
        if varac_rows:
            self._mark_varac_rows_read_bulk(varac_rows)
        if file_rows:
            self._set_read_state_bulk(file_rows, "READ", ts)

        changed = 0
        for row in rows:
            prev = (row.status or "").upper()
            payload = row.payload
            now_read = False
            if isinstance(payload, JS8Message):
                now_read = payload.state.upper() == "READ"
            elif isinstance(payload, SpotterMessage):
                now_read = payload.state.upper() == "READ"
            elif isinstance(payload, VarACMessage):
                now_read = int(payload.read_status or 0) == 1
            elif isinstance(payload, FileRecord):
                key = self._read_state_key(payload.origin, payload)
                now_read = ((self._read_state_map.get(key, ("NEW", 0.0, 0))[0] or "").upper() == "READ")
            if now_read:
                row.status = "READ"
                if prev != "READ":
                    changed += 1
        return changed

    def _mark_js8_rows_read_bulk(self, msgs: List[JS8Message], read_ts: float) -> None:
        dedup: Dict[int, JS8Message] = {}
        for msg in msgs:
            if msg.msg_id > 0:
                dedup[int(msg.msg_id)] = msg
        if not dedup:
            return
        pairs = [(mid, dedup[mid].utc_ts or 0.0) for mid in sorted(dedup.keys())]
        self._save_js8_state_bulk(pairs, read_ts)
        self._update_local_read_bulk(sorted(dedup.keys()), read_ts)
        if self.settings.get("js8_inbox_mark_retrieved_sync", False):
            updated = self._mark_js8call_inbox_read_by_ids(sorted(dedup.keys()))
            log.debug(
                "MessageViewer: mark-all JS8 inbox sync updated %s/%s rows",
                updated,
                len(dedup),
            )
        for msg in dedup.values():
            msg.state = "READ"
            msg.read_ts = read_ts

    def _mark_spotter_rows_read_bulk(self, msgs: List[SpotterMessage], read_ts: float) -> None:
        ids = sorted({int(msg.spotter_id) for msg in msgs if int(msg.spotter_id or 0) > 0})
        if not ids:
            return
        db_path = self._db_path()
        if db_path and db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                for start in range(0, len(ids), 250):
                    chunk = ids[start : start + 250]
                    placeholders = ",".join("?" for _ in chunk)
                    cur.execute(
                        f"UPDATE spotter_traffic SET state='READ', read_ts=? WHERE id IN ({placeholders})",
                        (float(read_ts), *chunk),
                    )
                conn.commit()
                conn.close()
            except Exception as e:
                log.debug("MessageViewer: bulk spotter read update failed: %s", e)
        for msg in msgs:
            msg.state = "READ"
            msg.read_ts = read_ts

    def _mark_varac_rows_read_bulk(self, msgs: List[VarACMessage]) -> None:
        pairs = sorted({(str(msg.source or ""), int(msg.msg_id)) for msg in msgs if int(msg.msg_id or 0) > 0})
        if not pairs:
            return
        db_path = self._db_path()
        if db_path and db_path.exists():
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.executemany(
                    "UPDATE varac_messages SET read_status=1 WHERE source=? AND id=?",
                    pairs,
                )
                conn.commit()
                conn.close()
            except Exception as e:
                log.debug("MessageViewer: bulk VarAC read update failed: %s", e)
        for msg in msgs:
            msg.read_status = 1

    def _set_read_state_bulk(self, recs: List[FileRecord], status: str, read_ts: float) -> None:
        status = (status or "READ").upper()
        if status != "READ":
            return
        dedup: Dict[tuple, FileRecord] = {}
        for rec in recs:
            dedup[self._read_state_key(rec.origin, rec)] = rec
        if not dedup:
            return
        rows: List[Tuple[str, str, float, int, str, float, int]] = []
        for key, rec in dedup.items():
            flag_state = self._get_flag_state(rec)
            self._read_state_map[key] = ("READ", float(read_ts), int(flag_state))
            rows.append(
                (
                    rec.origin,
                    str(rec.path),
                    float(rec.mtime),
                    int(rec.size),
                    "READ",
                    float(read_ts),
                    int(flag_state),
                )
            )
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT OR REPLACE INTO message_read_state
                    (origin, path, mtime, size, status, read_ts, flag_state)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: bulk file read-state update failed: %s", e)

    def _save_js8_state_bulk(self, rows: List[Tuple[int, float]], read_ts: float) -> None:
        db_path = self._local_js8_db()
        if not db_path:
            return
        if not rows:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL, read_ts REAL, last_ingested_id INTEGER)"
            )
            cur.executemany(
                "INSERT INTO js8_inbox_state (id, state, last_seen, read_ts) VALUES (?, 'READ', ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET state='READ', last_seen=excluded.last_seen, read_ts=excluded.read_ts",
                [(int(msg_id), float(last_seen or 0.0), float(read_ts)) for msg_id, last_seen in rows],
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: bulk js8 state save failed: %s", e)

    def _update_local_read_bulk(self, msg_ids: List[int], read_ts: float) -> None:
        db_path = self._local_js8_db()
        if not db_path or not Path(db_path).exists():
            return
        ids = [int(i) for i in msg_ids if int(i) > 0]
        if not ids:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.executemany(
                "UPDATE js8_messages SET state='READ', read_ts=? WHERE id=?",
                [(float(read_ts), int(msg_id)) for msg_id in ids],
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: bulk local js8 read update failed: %s", e)

    def _mark_js8call_inbox_read_by_ids(self, row_ids: List[int]) -> int:
        inbox_path = self._inbox_path()
        if not inbox_path or not inbox_path.exists():
            return 0
        pending = {int(rid) for rid in row_ids if int(rid) > 0}
        if not pending:
            return 0
        updated_count = 0
        candidates = [
            ("inbox_v1", "blob"),
            ("inbox_v1", "json"),
            ("inbox_v1", "message"),
            ("inbox", "blob"),
            ("inbox", "json"),
            ("inbox", "message"),
        ]
        try:
            conn = sqlite3.connect(inbox_path, timeout=1.5)
            cur = conn.cursor()
            cur.execute("PRAGMA busy_timeout = 1500")
            for table, col in candidates:
                if not pending:
                    break
                has_type_col = self._table_has_column(conn, table, "type")
                for id_expr in ("id", "rowid"):
                    if not pending:
                        break
                    remaining = sorted(pending)
                    table_usable = True
                    for start in range(0, len(remaining), 250):
                        chunk = remaining[start : start + 250]
                        placeholders = ",".join("?" for _ in chunk)
                        try:
                            cur.execute(
                                f"SELECT {id_expr} as id, {col} FROM {table} WHERE {id_expr} IN ({placeholders})",
                                chunk,
                            )
                            rows = cur.fetchall()
                        except Exception:
                            table_usable = False
                            break
                        if not rows:
                            continue
                        updates_blob: List[Tuple[str, str, int]] = []
                        updates_blob_only: List[Tuple[str, int]] = []
                        seen_ids: List[int] = []
                        for row_id, blob in rows:
                            rid = int(row_id or 0)
                            if rid <= 0:
                                continue
                            try:
                                parsed = json.loads(blob or "{}")
                            except Exception:
                                continue
                            if not isinstance(parsed, dict):
                                continue
                            current_type = str(parsed.get("type", "") or "").strip().upper()
                            if current_type == "DELIVERED":
                                seen_ids.append(rid)
                                continue
                            if current_type != "READ":
                                parsed["type"] = "READ"
                                new_blob = json.dumps(parsed, separators=(",", ":"))
                                if has_type_col:
                                    updates_blob.append((new_blob, "READ", rid))
                                else:
                                    updates_blob_only.append((new_blob, rid))
                                updated_count += 1
                            seen_ids.append(rid)
                        if updates_blob:
                            cur.executemany(
                                f"UPDATE {table} SET {col}=?, type=? WHERE {id_expr}=?",
                                updates_blob,
                            )
                        if updates_blob_only:
                            cur.executemany(
                                f"UPDATE {table} SET {col}=? WHERE {id_expr}=?",
                                updates_blob_only,
                            )
                        for rid in seen_ids:
                            pending.discard(rid)
                    if table_usable and not pending:
                        break
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: bulk JS8Call inbox mark READ failed: %s", e)
        return updated_count

    def _build_messages_header(self) -> None:
        while self.messages_header_layout.count():
            item = self.messages_header_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self._header_cells = []
        select_hdr = self._make_header_spacer()
        type_hdr = self._make_filter_cell(self.type_filter)
        status_hdr = self._make_filter_cell(self.status_filter)
        from_hdr = self._make_filter_cell(self.from_filter)
        to_hdr = self._make_filter_cell(self.to_filter)
        rcv_hdr = self._make_search_filter_cell(self.rcv_search)
        title_hdr = self._make_header_spacer()
        self.messages_header_layout.addWidget(select_hdr)
        self.messages_header_layout.addWidget(type_hdr)
        self.messages_header_layout.addWidget(status_hdr)
        self.messages_header_layout.addWidget(from_hdr)
        self.messages_header_layout.addWidget(to_hdr)
        self.messages_header_layout.addWidget(rcv_hdr)
        self.messages_header_layout.addWidget(title_hdr, 1)
        self._header_cells.extend([select_hdr, type_hdr, status_hdr, from_hdr, to_hdr, rcv_hdr, title_hdr])
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
        self._update_mark_all_read_style()
        self._sync_header_widths()
        header = self.messages_table.horizontalHeader()
        if not self._messages_header_sync_connected:
            header.sectionResized.connect(self._sync_header_widths)
            # Catch first-show geometry recalcs that do not emit sectionResized.
            header.geometriesChanged.connect(self._sync_header_widths)
            self._messages_header_sync_connected = True
        self.messages_header.setMinimumHeight(self.messages_header.sizeHint().height())
        # Perform a few deferred sync passes so first open matches post-resize alignment.
        QTimer.singleShot(0, self._sync_header_widths)
        QTimer.singleShot(25, self._sync_header_widths)
        QTimer.singleShot(100, self._sync_header_widths)
        QTimer.singleShot(0, self._sync_select_all_checkbox)

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
        try:
            min_w = int(combo.fontMetrics().horizontalAdvance("MSG Type...") + 44)
        except Exception:
            min_w = 110
        combo.setMinimumWidth(max(110, min_w))
        combo.setMinimumContentsLength(6)
        combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._fit_filter_combo_popup(combo)
        layout.addWidget(combo)
        return container

    def _make_combo_searchable(self, combo: QComboBox, placeholder: str) -> None:
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.NoInsert)
        combo.setStyleSheet("QComboBox { padding-right: 20px; } QComboBox::drop-down { width: 18px; }")
        completer = QCompleter(combo.model(), combo)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.activated.connect(lambda _: self._on_filter_changed())
        combo.setCompleter(completer)
        edit = combo.lineEdit()
        if edit is not None:
            edit.setPlaceholderText(placeholder)
            edit.editingFinished.connect(self._on_filter_changed)
            edit.textEdited.connect(lambda _: combo.completer().complete())
        self._fit_filter_combo_popup(combo)

    def _make_search_filter_cell(self, edit: QLineEdit) -> QWidget:
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(4, 0, 4, 0)
        layout.setSpacing(2)
        try:
            min_w = int(edit.fontMetrics().horizontalAdvance("Search...") + 136)
        except Exception:
            min_w = 200
        edit.setMinimumWidth(max(200, min_w))
        edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        layout.addWidget(edit)
        return container

    def _fit_filter_combo_popup(self, combo: QComboBox) -> None:
        try:
            fm = combo.fontMetrics()
            text_w = 0
            for i in range(combo.count()):
                text_w = max(text_w, int(fm.horizontalAdvance(combo.itemText(i))))
            if combo.isEditable() and combo.lineEdit() is not None:
                ph = combo.lineEdit().placeholderText() or ""
                if ph:
                    text_w = max(text_w, int(fm.horizontalAdvance(ph)))
            popup_w = max(int(combo.width()), text_w + 52)
            popup_w = min(520, popup_w)
            view = combo.view()
            if view is not None:
                view.setMinimumWidth(popup_w)
        except Exception:
            pass

    def _apply_accessibility_width_guards(self) -> None:
        # Prevent control label clipping at larger text sizes.
        max_w = 360
        buttons = [
            getattr(self, "refresh_btn", None),
            getattr(self, "export_btn", None),
            getattr(self, "delete_selected_btn", None),
            getattr(self, "mark_all_read_btn", None),
            getattr(self, "time_toggle_btn", None),
            getattr(self, "clear_filters_btn", None),
            getattr(self, "open_external_btn", None),
        ]
        for btn in buttons:
            if btn is None:
                continue
            try:
                txt = str(btn.text() or "").strip()
            except Exception:
                txt = ""
            if not txt:
                continue
            try:
                needed = int(btn.fontMetrics().horizontalAdvance(txt.replace("&", "")) + 30)
            except Exception:
                continue
            try:
                current_min = int(btn.minimumWidth() or 0)
            except Exception:
                current_min = 0
            base = btn.property("_fio_base_min_width")
            try:
                base_w = int(base)
            except Exception:
                base_w = current_min
                try:
                    btn.setProperty("_fio_base_min_width", base_w)
                except Exception:
                    pass
            target = max(base_w, min(max_w, needed))
            try:
                btn.setMinimumWidth(target)
            except Exception:
                pass

        try:
            progress_w = int(self.loading_label.fontMetrics().horizontalAdvance("Getting messages...") + 64)
            self._loading_progress.setFixedWidth(max(140, min(260, progress_w)))
        except Exception:
            pass

        for combo in (self.type_filter, self.status_filter, self.from_filter, self.to_filter):
            if combo is None:
                continue
            try:
                needed = int(combo.fontMetrics().horizontalAdvance("Status...") + 52)
            except Exception:
                continue
            try:
                current_min = int(combo.minimumWidth() or 0)
                base = combo.property("_fio_base_min_width")
                try:
                    base_w = int(base)
                except Exception:
                    base_w = current_min
                    combo.setProperty("_fio_base_min_width", base_w)
                combo.setMinimumWidth(max(base_w, min(220, needed)))
            except Exception:
                pass
            self._fit_filter_combo_popup(combo)
        try:
            search_needed = int(self.rcv_search.fontMetrics().horizontalAdvance("Search...") + 136)
            current_min = int(self.rcv_search.minimumWidth() or 0)
            base = self.rcv_search.property("_fio_base_min_width")
            try:
                base_w = int(base)
            except Exception:
                base_w = current_min
                self.rcv_search.setProperty("_fio_base_min_width", base_w)
            self.rcv_search.setMinimumWidth(max(base_w, min(320, search_needed)))
        except Exception:
            pass
        try:
            self._sync_header_widths()
        except Exception:
            pass

    @staticmethod
    def _make_header_spacer() -> QWidget:
        spacer = QWidget()
        return spacer

    def _sync_header_widths(self) -> None:
        if not hasattr(self, "messages_table"):
            return
        if not self._header_cells:
            return
        header = self.messages_table.horizontalHeader()
        fallback_widths = {
            0: 32,
            1: 100,
            2: 96,
            3: 122,
            4: 122,
            5: 162,
            6: 120,
            7: 210,
        }
        for idx, widget in enumerate(self._header_cells):
            if widget is None:
                continue
            width = header.sectionSize(idx)
            if int(width) <= 1:
                try:
                    width = int(self.messages_table.columnWidth(idx))
                except Exception:
                    width = 0
            if int(width) <= 1:
                width = fallback_widths.get(idx, 60)
            if idx == 0:
                min_width = 30
            elif idx == 7:
                min_width = 210
            elif idx in (3, 4):
                min_width = 96
            else:
                min_width = 60
            widget.setFixedWidth(max(min_width, width))

    def _filters_active(self) -> bool:
        return (
            self.type_filter.currentText() not in ("", "MSG Type...")
            or self.status_filter.currentText() not in ("", "Status...")
            or self.from_filter.currentText() not in ("",)
            or self.to_filter.currentText() not in ("",)
            or bool(self.rcv_search.text().strip())
        )

    def _update_clear_filters_style(self) -> None:
        theme = resolve_theme(self.settings)
        role = "eligible_warning" if self._filters_active() else "muted"
        self.clear_filters_btn.setStyleSheet(button_style(role, theme))

    def _mark_all_read_eligibility(self) -> tuple[bool, str]:
        type_sel = self.type_filter.currentText() if hasattr(self, "type_filter") else "MSG Type..."
        if type_sel in ("", "MSG Type..."):
            return False, "Select a Message Type filter first."
        rows = self._messages_model.rows()
        if not rows:
            return False, "No messages in current filtered view."
        if type_sel == "Spotter":
            if any(not re.match(r"^F!\d+$", (r.msg_type or "")) for r in rows):
                return False, "Filtered rows are not scoped to one message type."
        else:
            if any((r.msg_type or "") != type_sel for r in rows):
                return False, "Filtered rows are not scoped to one message type."
        unread = sum(1 for r in rows if (r.status or "").upper() != "READ")
        if unread <= 0:
            return False, "No unread rows in current filtered view."
        return True, ""

    def _update_mark_all_read_style(self) -> None:
        theme = resolve_theme(self.settings)
        enabled, reason = self._mark_all_read_eligibility()
        self.mark_all_read_btn.setEnabled(enabled)
        self.mark_all_read_btn.setToolTip(reason if not enabled else "Mark all visible rows as READ.")
        role = "eligible_warning" if enabled else "muted"
        self.mark_all_read_btn.setStyleSheet(button_style(role, theme))

    def _mark_all_filtered_read(self) -> None:
        enabled, reason = self._mark_all_read_eligibility()
        if not enabled:
            QMessageBox.information(self, "Mark All as Read", reason or "Nothing to mark.")
            return
        type_sel = self.type_filter.currentText()
        rows = self._messages_model.rows()
        unread_rows = [r for r in rows if (r.status or "").upper() != "READ"]
        if not unread_rows:
            QMessageBox.information(self, "Mark All as Read", "No unread rows in current filtered view.")
            return
        prompt = (
            f"Mark {len(unread_rows)} unread {type_sel} message(s) as READ "
            f"in the current filtered view?"
        )
        if QMessageBox.question(self, "Mark All as Read", prompt, QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
        changed = self._mark_rows_read_bulk(unread_rows)
        if changed <= 0:
            QMessageBox.information(self, "Mark All as Read", "No rows were updated.")
            return
        self._apply_message_filters_preserve_scroll()
        QMessageBox.information(self, "Mark All as Read", f"Marked {changed} message(s) as READ.")

    def _on_sort_clicked(self, section: int) -> None:
        if section == 0 or section >= 7:
            return
        if section == self._sort_column:
            self._sort_order = (
                Qt.AscendingOrder if self._sort_order == Qt.DescendingOrder else Qt.DescendingOrder
            )
        else:
            self._sort_column = section
            self._sort_order = Qt.AscendingOrder
        self._apply_message_filters()

    def _sync_select_all_checkbox(self) -> None:
        header = self.messages_table.horizontalHeader() if hasattr(self, "messages_table") else None
        if not isinstance(header, MessageHeaderWithCheckbox):
            return
        rows = self._messages_model.rows()
        keys = [MessageTableModel._row_key(r) for r in rows]
        keys = [k for k in keys if k is not None]
        enabled = self._is_filter_active() and bool(keys)
        if not keys:
            header.set_checkbox_state(Qt.Unchecked, enabled=False)
            return
        selected = 0
        for k in keys:
            if k in self._messages_model._selected_keys:
                selected += 1
        if selected == 0:
            header.set_checkbox_state(Qt.Unchecked, enabled=enabled)
        elif selected == len(keys):
            header.set_checkbox_state(Qt.Checked, enabled=enabled)
        else:
            header.set_checkbox_state(Qt.PartiallyChecked, enabled=enabled)

    def _on_header_checkbox_toggled(self, state: int) -> None:
        if not self._is_filter_active():
            self._sync_select_all_checkbox()
            return
        rows = self._messages_model.rows()
        state_val = int(getattr(state, "value", state))
        if state_val == Qt.PartiallyChecked.value:
            return
        target = state_val == Qt.Checked.value
        self._messages_model.set_selected_for_rows(rows, target)
        self._update_bulk_delete_buttons()

    def _sort_rows(self, rows: List[UnifiedMessage]) -> List[UnifiedMessage]:
        reverse = self._sort_order == Qt.DescendingOrder
        col = self._sort_column

        def key(row: UnifiedMessage):
            if col == 1:
                return row.msg_type or ""
            if col == 2:
                return row.status or ""
            if col == 3:
                return row.from_call or ""
            if col == 4:
                return row.to_call or ""
            if col == 5:
                return row.rcv_ts or 0.0
            if col == 6:
                return row.title or ""
            return row.rcv_ts or 0.0

        return sorted(rows, key=key, reverse=reverse)


    # ---------- Selection / Viewing ----------

    def _build_message_rows(self) -> List[UnifiedMessage]:
        rows: List[UnifiedMessage] = []
        dedupe_raw_spotter = bool(
            self._is_truthy(self.settings.get("sitrep_messages_dedupe_enabled", True), True)
            and not self._is_truthy(self.settings.get("sitrep_messages_show_raw_duplicates", False), False)
        )
        sitrep_report_keys: set[str] = set()
        sitrep_render_keys: set[str] = set()
        if dedupe_raw_spotter:
            for sitrep in self.sitrep_messages:
                key = _RowsBuildWorker._sitrep_message_semantic_key(sitrep, bucket_seconds=60)
                if key:
                    sitrep_report_keys.add(key)

        for msg in self.js8_messages:
            msg_type = msg.msg_type if msg.msg_type.startswith("F!") else "JS8 MSG"
            status = "READ" if msg.state.upper() == "READ" else "NEW"
            rcv_ts = msg.utc_ts or 0.0
            rcv_display = self._format_rcv_display(rcv_ts, msg.utc_str)
            title = ""
            if msg.msg_type.startswith("F!"):
                form_id = msg.msg_type[2:].strip()
                title = self._load_form_title(form_id)
            if not title:
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

        for msg in self.spotter_messages:
            if dedupe_raw_spotter:
                spotter_key = _RowsBuildWorker._spotter_message_report_key(msg)
                if spotter_key and spotter_key in sitrep_report_keys:
                    continue
            msg_type = msg.msg_type or "F!"
            status = "READ" if msg.state.upper() == "READ" else "NEW"
            rcv_ts = msg.utc_ts or 0.0
            rcv_display = self._format_rcv_display(rcv_ts, msg.utc_str)
            title = ""
            if msg_type.startswith("F!"):
                form_id = msg_type[2:].strip()
                title = self._load_form_title(form_id)
            if not title:
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
                    origin="spotter",
                    payload=msg,
                )
            )

        for msg in self.varac_messages:
            msg_type = "VarAC"
            status = "NEW" if (msg.read_status == 0 and msg.msg_type.upper() != "QSO") else "READ"
            rcv_ts = msg.ts or 0.0
            rcv_display = self._format_rcv_display(rcv_ts, None)
            if (msg.msg_type or "").upper() == "VMAIL":
                title_base = (msg.subject or "").strip()
            else:
                title_base = (msg.subject or msg.body or "").strip()
            title = f"{msg.msg_type}: {title_base}" if title_base else (msg.msg_type or "VarAC")
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
                    origin="varac",
                    payload=msg,
                )
            )

        for msg in self.sitrep_messages:
            if dedupe_raw_spotter:
                ui_key = _RowsBuildWorker._sitrep_message_semantic_key(msg, bucket_seconds=1)
                if ui_key and ui_key in sitrep_render_keys:
                    continue
                if ui_key:
                    sitrep_render_keys.add(ui_key)
            rcv_ts = msg.event_ts or 0.0
            rcv_display = self._format_rcv_display(rcv_ts, msg.event_ts_utc)
            overall = (msg.overall_status or "").strip().lower()
            scope = (msg.scope or "").strip()
            title_parts = [msg.subtype]
            if scope:
                title_parts.append(scope)
            if overall:
                title_parts.append(overall.upper())
            title = " | ".join([p for p in title_parts if p]) or "SitRep"
            if len(title) > 60:
                title = title[:57].rstrip() + "..."
            rows.append(
                UnifiedMessage(
                    msg_type="SitRep",
                    status="INFO",
                    from_call=(msg.from_call or "").strip().upper(),
                    to_call=(msg.target or "").strip().upper(),
                    rcv_ts=rcv_ts,
                    rcv_display=rcv_display,
                    title=title,
                    origin="sitrep",
                    payload=msg,
                )
            )

        for origin, recs in self.files.items():
            for rec in recs:
                status = self._get_read_state(rec)
                is_image = self._is_image_file(rec.path)
                from_call = "" if is_image else self._extract_sender_from_file(rec)
                title = "Image Received" if is_image else rec.path.name
                rcv_ts = rec.mtime or 0.0
                rcv_display = self._format_rcv_display(rcv_ts, None)
                msg_type = origin.upper() if origin != "varac" else "VarAC"
                if origin == "flmsg":
                    msg_type = "FLMSG"
                elif origin == "bbs":
                    msg_type = "BBS"
                auth_state = ""
                auth_detail = ""
                auth_trusted = False
                if self._is_flamp_auth_file(rec):
                    sig_state = self._signature_state_for_record(rec)
                    auth_state, auth_detail, auth_trusted = self._derive_auth_ui(sig_state)
                rows.append(
                    UnifiedMessage(
                        msg_type=msg_type,
                        status=status,
                        from_call=from_call,
                        to_call="",
                        rcv_ts=rcv_ts,
                        rcv_display=rcv_display,
                        title=title,
                        origin=origin,
                        payload=rec,
                        auth_state=auth_state,
                        auth_detail=auth_detail,
                        auth_trusted=auth_trusted,
                    )
                )

        for row in rows:
            if not row.search_text:
                row.search_text = " ".join(
                    [
                        row.msg_type or "",
                        row.status or "",
                        row.from_call or "",
                        row.to_call or "",
                        row.rcv_display or "",
                        row.title or "",
                    ]
                ).lower()
        rows.sort(key=lambda r: r.rcv_ts, reverse=True)
        return rows

    def _on_view_message(self, row: UnifiedMessage) -> None:
        with perf_span(
            "messages.view_message",
            settings=self.settings,
            meta={"msg_type": row.msg_type, "origin": row.origin},
            min_ms=5.0,
        ):
            log.debug(
                "MessageViewer: view requested type=%s origin=%s title=%s",
                row.msg_type,
                row.origin,
                row.title,
            )
            self._has_active_view = True
            self._freeze_messages_table = True
            self._set_open_external_path(None)
            if isinstance(row.payload, JS8Message):
                self.current_record = None
                self.current_js8 = row.payload
                self._load_js8_content(row.payload)
                self._mark_js8_read(row.payload, row_ref=row)
            elif isinstance(row.payload, SpotterMessage):
                self.current_record = None
                self.current_js8 = None
                self._load_js8_content(row.payload)
                self._mark_spotter_read(row.payload, row_ref=row)
            elif isinstance(row.payload, FileRecord):
                self.current_js8 = None
                self.current_record = row.payload
                self._load_content(row.payload)
                self._set_read_state(row.payload, "READ", row_ref=row)
            elif isinstance(row.payload, VarACMessage):
                self.current_js8 = None
                self.current_record = None
                self._load_varac_content(row.payload, row_ref=row)
            elif isinstance(row.payload, SitrepMessage):
                self.current_js8 = None
                self.current_record = None
                self._load_sitrep_content(row.payload)

    def _read_file_head(self, path: Path, limit: int = 4096) -> str:
        try:
            with path.open("rb") as fh:
                raw = fh.read(limit)
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return ""

    def _extract_sender_from_file(self, rec: FileRecord) -> str:
        cache_key = (
            str(rec.path),
            float(rec.mtime or 0.0),
            int(rec.size or 0),
        )
        cached = self._sender_cache.get(cache_key)
        if cached is not None:
            return cached
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
                        match = re.search(r"\b[A-Z]{1,2}\d[A-Z0-9]{1,4}\b", nxt.upper())
                        if match:
                            sender = match.group(0)
                            log.debug(
                                "MessageViewer: sender parsed via %s for %s => %s",
                                marker,
                                rec.path.name,
                                sender,
                            )
                            self._sender_cache[cache_key] = sender
                            self._prune_cache(self._sender_cache, self._cache_max_sender_entries)
                            return sender
                    break
        tokens = re.split(r"[-_\\s]+", rec.path.stem)
        for tok in tokens:
            up = tok.strip().upper()
            if re.fullmatch(r"[A-Z]{1,2}\d[A-Z0-9]{1,4}", up):
                log.debug("MessageViewer: sender fallback from filename %s => %s", rec.path.name, up)
                self._sender_cache[cache_key] = up
                self._prune_cache(self._sender_cache, self._cache_max_sender_entries)
                return up
        log.debug("MessageViewer: sender not found for %s", rec.path.name)
        self._sender_cache[cache_key] = ""
        self._prune_cache(self._sender_cache, self._cache_max_sender_entries)
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
    def _is_image_file(path: Path) -> bool:
        return path.suffix.lower() in IMAGE_EXTS

    @staticmethod
    def _can_preview_image(path: Path) -> bool:
        return path.suffix.lower() in IMAGE_PREVIEW_EXTS

    def _set_open_external_path(self, path: Path | None, *, label: str = "Open Image") -> None:
        self._open_external_path = path
        if hasattr(self, "open_external_btn"):
            if path:
                self.open_external_btn.setText(label)
                self.open_external_btn.setVisible(True)
                self.open_external_btn.setEnabled(True)
            else:
                self.open_external_btn.setVisible(False)
                self.open_external_btn.setEnabled(False)

    def _open_external_file(self) -> None:
        path = self._open_external_path
        if not path:
            return
        try:
            if platform.system() == "Darwin":
                subprocess.Popen(["open", str(path)])
            elif os.name == "nt":
                os.startfile(str(path))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as e:
            QMessageBox.critical(self, "Open File", f"Could not open file:\n{e}")

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

    def _archive_file_record(self, rec: FileRecord) -> None:
        if not rec or (rec.origin or "").strip().lower() != "bbs":
            return
        if not rec.path.exists():
            QMessageBox.warning(self, "Archive BBS File", "The selected file no longer exists.")
            return
        archive_dir_txt = (self.settings.get("varac_bbs_archive_dir", "") or "").strip()
        if not archive_dir_txt:
            QMessageBox.warning(
                self,
                "Archive BBS File",
                "Set VarAC BBS Archive in Settings before archiving files.",
            )
            return
        archive_dir = Path(archive_dir_txt)
        if not archive_dir.exists() or not archive_dir.is_dir():
            QMessageBox.warning(
                self,
                "Archive BBS File",
                "Configured BBS Archive is not a valid directory.",
            )
            return
        details = (
            f"Archive this BBS file?\n\n"
            f"{rec.path}\n"
            f"Destination: {archive_dir}"
        )
        resp = QMessageBox.question(self, "Archive BBS File", details, QMessageBox.Yes | QMessageBox.No)
        if resp != QMessageBox.Yes:
            return
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        dst = archive_dir / rec.path.name
        if dst.exists():
            dst = archive_dir / f"{rec.path.stem}_{stamp}{rec.path.suffix}"
            attempt = 2
            while dst.exists():
                dst = archive_dir / f"{rec.path.stem}_{stamp}_{attempt}{rec.path.suffix}"
                attempt += 1
        try:
            shutil.move(str(rec.path), str(dst))
        except Exception as e:
            QMessageBox.warning(self, "Archive BBS File", f"Failed to archive file:\n{e}")
            return
        log.info("MessageViewer: archived BBS file %s -> %s", rec.path, dst)
        self._remove_file_record(rec)
        self._unfreeze_table()
        self._populate_messages_table(force=True)

    def _can_copy_row_to_varac_bbs(self, row: UnifiedMessage | None) -> bool:
        if row is None:
            return False
        bbs_dir_txt = (self.settings.get("varac_bbs_dir", "") or "").strip()
        if not bbs_dir_txt:
            return False
        msg_type = str(getattr(row, "msg_type", "") or "").strip().upper()
        if msg_type not in {"FLMSG", "FLAMP", "VARAC"}:
            return False
        payload = getattr(row, "payload", None)
        return isinstance(payload, FileRecord)

    @staticmethod
    def _bbs_copy_session_key_for_record(rec: FileRecord | None) -> tuple[str, float, int] | None:
        if not isinstance(rec, FileRecord):
            return None
        try:
            path_txt = str(rec.path.resolve())
        except Exception:
            path_txt = str(rec.path)
        path_key = os.path.normcase(os.path.normpath(path_txt))
        try:
            mtime_key = round(float(rec.mtime or 0.0), 6)
        except Exception:
            mtime_key = 0.0
        try:
            size_key = int(rec.size or 0)
        except Exception:
            size_key = 0
        return (path_key, mtime_key, size_key)

    def _bbs_copy_session_key_for_row(self, row: UnifiedMessage | None) -> tuple[str, float, int] | None:
        payload = getattr(row, "payload", None) if row is not None else None
        return self._bbs_copy_session_key_for_record(payload if isinstance(payload, FileRecord) else None)

    def _is_row_bbs_copy_action_enabled(self, row: UnifiedMessage | None) -> bool:
        if not self._can_copy_row_to_varac_bbs(row):
            return False
        key = self._bbs_copy_session_key_for_row(row)
        if key is None:
            return True
        return key not in self._bbs_copied_session_keys

    def _mark_row_copied_to_varac_bbs_session(self, row: UnifiedMessage | None) -> None:
        key = self._bbs_copy_session_key_for_row(row)
        if key is None:
            return
        self._bbs_copied_session_keys.add(key)

    def _copy_row_to_varac_bbs(self, row: UnifiedMessage | None) -> None:
        if row is None or not self._can_copy_row_to_varac_bbs(row):
            return
        if not self._is_row_bbs_copy_action_enabled(row):
            return
        payload = getattr(row, "payload", None)
        if not isinstance(payload, FileRecord):
            return
        src = payload.path
        if not src.exists() or not src.is_file():
            QMessageBox.warning(self, "Copy to VarAC BBS", "The selected source file no longer exists.")
            return
        bbs_dir_txt = (self.settings.get("varac_bbs_dir", "") or "").strip()
        if not bbs_dir_txt:
            QMessageBox.warning(self, "Copy to VarAC BBS", "Configure VarAC BBS directory in Settings first.")
            return
        bbs_dir = Path(bbs_dir_txt)
        if not bbs_dir.exists() or not bbs_dir.is_dir():
            QMessageBox.warning(self, "Copy to VarAC BBS", "Configured VarAC BBS directory is not valid.")
            return
        dst = bbs_dir / src.name
        try:
            if src.resolve() == dst.resolve():
                QMessageBox.information(self, "Copy to VarAC BBS", "File is already in the VarAC BBS folder.")
                return
        except Exception:
            pass
        if dst.exists():
            resp = QMessageBox.question(
                self,
                "Copy to VarAC BBS",
                f"File already exists in BBS folder:\n{dst}\n\nOverwrite existing file?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if resp != QMessageBox.Yes:
                return
        try:
            shutil.copy2(str(src), str(dst))
        except Exception as e:
            QMessageBox.warning(self, "Copy to VarAC BBS", f"Copy failed:\n{e}")
            return
        self._mark_row_copied_to_varac_bbs_session(row)
        QMessageBox.information(
            self,
            "Copy to VarAC BBS",
            f"Copied file to VarAC BBS folder:\n{dst}",
        )
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

    def _delete_js8_message(self, msg: JS8Message) -> None:
        if not msg:
            return
        msg_id = int(getattr(msg, "msg_id", 0) or 0)
        if msg_id <= 0:
            return
        deleted = self._delete_js8_inbox_row(msg_id)
        if not deleted:
            QMessageBox.warning(self, "Delete Message", f"Failed to delete Message {msg_id}.")
            return
        self._delete_js8_local_row(msg_id)
        self.js8_messages = [m for m in self.js8_messages if m.msg_id != msg_id]
        if self.current_js8 and self.current_js8.msg_id == msg_id:
            self.current_js8 = None
            self._has_active_view = False
            self.info_label.setText("No message selected")
            self.viewer.clear()
        self._unfreeze_table()
        self._populate_messages_table(force=True)
        QMessageBox.information(self, "Delete Message", f"Message {msg_id} Deleted")

    def _delete_varac_message(self, msg: VarACMessage) -> None:
        if not msg:
            return
        msg_id = int(getattr(msg, "msg_id", 0) or 0)
        if msg_id <= 0:
            return
        resp = QMessageBox.question(
            self,
            "Delete Message",
            f"Delete VarAC message {msg_id}?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return
        if not self._soft_delete_varac_row(msg):
            QMessageBox.warning(self, "Delete Message", f"Failed to delete Message {msg_id}.")
            return
        self._delete_varac_local_row(msg)
        self.varac_messages = [m for m in self.varac_messages if m.msg_id != msg_id or m.source != msg.source]
        if self.current_record is None and self.current_js8 is None:
            self._has_active_view = False
            self.info_label.setText("No message selected")
            self.viewer.clear()
        self._unfreeze_table()
        self._populate_messages_table(force=True)
        QMessageBox.information(self, "Delete Message", f"Message {msg_id} Deleted")

    def _delete_spotter_message(self, msg: SpotterMessage) -> None:
        if not msg:
            return
        msg_id = int(getattr(msg, "spotter_id", 0) or 0)
        if msg_id <= 0:
            return
        resp = QMessageBox.question(
            self,
            "Delete Message",
            f"Delete Spotter message {msg_id}?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return
        if not self._delete_spotter_row(msg_id):
            QMessageBox.warning(self, "Delete Message", f"Failed to delete Message {msg_id}.")
            return
        self.spotter_messages = [m for m in self.spotter_messages if m.spotter_id != msg_id]
        if self.current_js8 is None and self.current_record is None:
            self._has_active_view = False
            self.info_label.setText("No message selected")
            self.viewer.clear()
        self._unfreeze_table()
        self._populate_messages_table(force=True)
        QMessageBox.information(self, "Delete Message", f"Message {msg_id} Deleted")

    def _resolve_varac_db_path(self) -> Path | None:
        raw_db = (self.settings.get("varac_db_path", "") or "").strip()
        raw_install = (self.settings.get("varac_path", "") or "").strip()
        for raw in (raw_db, raw_install):
            if not raw:
                continue
            try:
                p = Path(raw)
                if p.is_dir():
                    return p / "VarAC.db"
                if p.is_file():
                    return p
            except Exception:
                continue
        return None

    def _soft_delete_varac_row(self, msg: VarACMessage) -> bool:
        db_path = self._resolve_varac_db_path()
        if not db_path or not db_path.exists():
            return False
        table = msg.source
        if table not in {"qso", "vmail", "broadcast"}:
            return False
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(f"UPDATE {table} SET is_deleted=1 WHERE id=?", (int(msg.msg_id),))
            if table == "vmail" and msg.vmail_guid:
                try:
                    cur.execute("UPDATE vmail_attachment SET is_deleted=1 WHERE vmail_guid=?", (msg.vmail_guid,))
                except Exception:
                    pass
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            log.debug("MessageViewer: failed to soft delete VarAC row %s/%s: %s", table, msg.msg_id, e)
            return False

    def _delete_varac_local_row(self, msg: VarACMessage) -> None:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM varac_messages WHERE source=? AND id=?",
                (msg.source, int(msg.msg_id)),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to delete local varac row %s/%s: %s", msg.source, msg.msg_id, e)

    def _delete_spotter_row(self, msg_id: int) -> bool:
        db_path = self._db_path()
        if not db_path or not db_path.exists():
            return False
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM spotter_traffic WHERE id=?", (int(msg_id),))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            log.debug("MessageViewer: failed to delete spotter row %s: %s", msg_id, e)
            return False

    def _delete_js8_inbox_row(self, msg_id: int) -> bool:
        inbox_path = self._inbox_path()
        if not inbox_path or not inbox_path.exists():
            return False
        try:
            conn = sqlite3.connect(inbox_path, timeout=1.0)
            cur = conn.cursor()
            cur.execute("PRAGMA busy_timeout = 1000")
            tables = ["inbox_v1", "inbox"]
            deleted = False
            for table in tables:
                try:
                    cur.execute("PRAGMA table_info(%s)" % table)
                    cols = {str(r[1]).lower() for r in cur.fetchall()}
                except Exception:
                    continue
                try:
                    if "id" in cols:
                        cur.execute(f"DELETE FROM {table} WHERE id=?", (int(msg_id),))
                        if cur.rowcount:
                            deleted = True
                    else:
                        cur.execute(f"DELETE FROM {table} WHERE rowid=?", (int(msg_id),))
                        if cur.rowcount:
                            deleted = True
                except Exception:
                    continue
            conn.commit()
            conn.close()
            return deleted
        except Exception as e:
            log.debug("MessageViewer: failed to delete inbox row %s: %s", msg_id, e)
            return False

    def _delete_js8_local_row(self, msg_id: int) -> None:
        db_path = self._local_js8_db()
        if not db_path or not Path(db_path).exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM js8_messages WHERE id=?", (int(msg_id),))
            cur.execute("DELETE FROM js8_inbox_state WHERE id=?", (int(msg_id),))
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to delete local js8 row %s: %s", msg_id, e)

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
        with perf_span(
            "messages.load_content",
            settings=self.settings,
            meta={"origin": rec.origin, "ext": rec.path.suffix.lower()},
            min_ms=5.0,
        ):
            view_key = (
                str(rec.path),
                float(rec.mtime or 0.0),
                int(rec.size or 0),
            )
            cached_view = self._file_view_cache.get(view_key)
            if cached_view is not None:
                is_html, content, info, open_path, open_label = cached_view
                self._set_open_external_path(open_path, label=open_label)
                self.info_label.setText(self._compose_info_with_signature(rec, info))
                if is_html:
                    self.viewer.setAcceptRichText(True)
                    self.viewer.setHtml(content)
                else:
                    self.viewer.setAcceptRichText(False)
                    self.viewer.setPlainText(content)
                return
            log.debug("MessageViewer: loading file %s", rec.path)
            if self._is_image_file(rec.path):
                ext = rec.path.suffix.lower()
                self._set_open_external_path(rec.path, label="Open Image")
                info = f"Image Received - {rec.path.name} - {rec.size} bytes - {self._fmt_mtime(rec.mtime)}"
                self.info_label.setText(self._compose_info_with_signature(rec, info))
                if self._can_preview_image(rec.path) and rec.path.exists():
                    try:
                        uri = rec.path.resolve().as_uri()
                    except Exception:
                        uri = ""
                    html_out = [
                        "<div style='font-family: sans-serif;'>",
                        f"<div><b>File:</b> {html.escape(rec.path.name)}</div>",
                    ]
                    if uri:
                        html_out.append(
                            f"<div style='margin-top:8px;'><img src='{uri}' "
                            "style='max-width: 100%; height: auto;'></div>"
                        )
                    else:
                        html_out.append("<div>Preview unavailable for this image.</div>")
                    html_out.append("</div>")
                    self.viewer.setAcceptRichText(True)
                    rendered = "".join(html_out)
                    self.viewer.setHtml(rendered)
                    self._file_view_cache[view_key] = (True, rendered, info, rec.path, "Open Image")
                    self._prune_cache(self._file_view_cache, self._cache_max_view_entries)
                else:
                    self.viewer.setAcceptRichText(False)
                    rendered = (
                        f"Image file: {rec.path.name}\n\nPreview is not available for {ext}.\n"
                        "Use 'Open Image' to view it in an external application."
                    )
                    self.viewer.setPlainText(rendered)
                    self._file_view_cache[view_key] = (False, rendered, info, rec.path, "Open Image")
                    self._prune_cache(self._file_view_cache, self._cache_max_view_entries)
                return
            self._set_open_external_path(None)
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
            self.info_label.setText(self._compose_info_with_signature(rec, info))
            if is_html:
                self.viewer.setAcceptRichText(True)
                self.viewer.setHtml(content)
            else:
                self.viewer.setAcceptRichText(False)
                self.viewer.setPlainText(content)
            self._file_view_cache[view_key] = (bool(is_html), str(content), info, None, "Open Image")
            self._prune_cache(self._file_view_cache, self._cache_max_view_entries)

    @staticmethod
    def _prune_cache(cache: Dict, max_size: int) -> None:
        if max_size <= 0:
            cache.clear()
            return
        while len(cache) > max_size:
            try:
                first_key = next(iter(cache))
            except StopIteration:
                return
            cache.pop(first_key, None)

    def _load_varac_content(self, msg: VarACMessage, row_ref: Optional[UnifiedMessage] = None) -> None:
        with perf_span(
            "messages.load_varac_content",
            settings=self.settings,
            meta={"msg_type": msg.msg_type},
            min_ms=2.0,
        ):
            header = [
                f"TYPE: {msg.msg_type}",
                f"FROM: {msg.from_call}",
                f"TO:   {msg.to_call}",
                f"TIME: {self._fmt_ts(msg.ts)}",
            ]
            if msg.band:
                header.append(f"BAND: {msg.band}")
            if msg.freq_hz:
                header.append(f"FREQ: {float(msg.freq_hz) / 1_000_000.0:.3f}")
            if msg.snr is not None:
                header.append(f"SNR:  {msg.snr}")
            header.append("")
            if (msg.msg_type or "").upper() == "VMAIL":
                body = msg.body or ""
                if not body:
                    body = msg.subject or ""
            else:
                body = msg.body or msg.subject or ""
            self.info_label.setText(f"VarAC {msg.msg_type} {msg.from_call} -> {msg.to_call}")
            self.viewer.setAcceptRichText(False)
            self.viewer.setPlainText("\n".join(header + [body]))
            self._mark_varac_read(msg, row_ref=row_ref)

    @staticmethod
    def _pretty_sitrep_value(value: str) -> str:
        txt = str(value or "").strip().lower()
        mapping = {
            "red": "Red",
            "yellow": "Yellow",
            "green": "Green",
            "unknown": "Unknown",
            "not_reported": "Not Reported",
        }
        return mapping.get(txt, txt or "Not Reported")

    @staticmethod
    def _safe_json_pretty(text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return "{}"
        try:
            obj = json.loads(raw)
            return json.dumps(obj, indent=2, ensure_ascii=True)
        except Exception:
            return raw

    def _load_sitrep_content(self, msg: SitrepMessage) -> None:
        with perf_span(
            "messages.load_sitrep_content",
            settings=self.settings,
            meta={"subtype": msg.subtype},
            min_ms=2.0,
        ):
            mode = self._current_time_mode()
            label = "UTC" if mode == "UTC" else "Local"
            ts_display = self._format_rcv_display(msg.event_ts or 0.0, msg.event_ts_utc)
            source_list = self._safe_json_pretty(msg.sources_json)
            source_refs = self._safe_json_pretty(msg.source_refs_json)
            raw_payload = self._safe_json_pretty(msg.raw_payload_json)
            lines = [
                "Normalized SitRep",
                "",
                f"CALLSIGN: {msg.from_call}",
                f"TO:       {msg.target}",
                f"GRID:     {msg.grid}",
                f"SCOPE:    {msg.scope or 'My Location'}",
                f"SUBTYPE:  {msg.subtype}",
                f"{label}:  {ts_display}",
                f"REPORT:   {msg.report_key}",
                f"SOURCES:  {msg.source_count}",
                "",
                "Status Fields",
                f"  Overall:        {self._pretty_sitrep_value(msg.overall_status)}",
                f"  Power:          {self._pretty_sitrep_value(msg.power)}",
                f"  Water:          {self._pretty_sitrep_value(msg.water)}",
                f"  Medical:        {self._pretty_sitrep_value(msg.medical)}",
                f"  Communications: {self._pretty_sitrep_value(msg.communications)}",
                f"  Internet:       {self._pretty_sitrep_value(msg.internet)}",
                f"  Travel:         {self._pretty_sitrep_value(msg.travel)}",
                f"  Food:           {self._pretty_sitrep_value(msg.food)}",
                f"  Fuel:           {self._pretty_sitrep_value(msg.fuel)}",
                f"  Crime:          {self._pretty_sitrep_value(msg.crime)}",
                f"  Civil Unrest:   {self._pretty_sitrep_value(msg.civil_unrest)}",
                f"  Political:      {self._pretty_sitrep_value(msg.political)}",
                "",
                "Source Metadata",
                f"  First Source: {msg.source_first or 'Unknown'}",
                f"  Last Source:  {msg.source_last or 'Unknown'}",
                "  Source List JSON:",
                source_list,
                "",
                "Source Refs JSON:",
                source_refs,
                "",
                "Raw Payload JSON:",
                raw_payload,
            ]
            self.info_label.setText(f"SitRep {msg.subtype} {msg.from_call} -> {msg.target}")
            self.viewer.setAcceptRichText(False)
            self.viewer.setPlainText("\n".join(lines))

    def _mark_varac_read(self, msg: VarACMessage, row_ref: Optional[UnifiedMessage] = None) -> None:
        if not msg or msg.read_status:
            return
        msg.read_status = 1
        self._queue_persist_op("varac_read", (str(msg.source or ""), int(msg.msg_id)))
        self._refresh_table_after_read(
            lambda row: isinstance(row.payload, VarACMessage)
            and int(getattr(row.payload, "msg_id", 0) or 0) == int(msg.msg_id)
            and str(getattr(row.payload, "source", "") or "") == str(msg.source or ""),
            row_ref=row_ref,
        )

    def _load_js8_content(self, msg: JS8Message | SpotterMessage):
        with perf_span(
            "messages.load_js8_content",
            settings=self.settings,
            meta={"msg_type": msg.msg_type},
            min_ms=2.0,
        ):
            mode = self._current_time_mode()
            label = "UTC" if mode == "UTC" else "Local"
            ts_display = self._format_rcv_display(msg.utc_ts or 0.0, msg.utc_str)
            header = [
                f"FROM: {msg.from_call}",
                f"TO:   {msg.to_call}",
                f"TYPE: {msg.msg_type}",
                f"{label}:  {ts_display}",
                "",
            ]
            relay_via = getattr(msg, "relay_via", "") or ""
            if relay_via:
                header.insert(4, f"RELAY VIA: {relay_via}")
            body = msg.decoded_text or msg.raw_text
            self.info_label.setText(f"{msg.msg_type} {msg.from_call} -> {msg.to_call}")
            self.viewer.setAcceptRichText(False)
            self.viewer.setPlainText("\n".join(header + [body]))

    def _fmt_mtime(self, mtime: float) -> str:
        if not mtime:
            return ""
        try:
            mode = self._current_time_mode()
            if mode == "UTC":
                return datetime.datetime.fromtimestamp(mtime, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            tz = get_timezone(tz_name)
            return datetime.datetime.fromtimestamp(mtime, tz=tz).strftime("%Y-%m-%d %H:%M")
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

    def _mark_js8_read(self, msg: JS8Message, row_ref: Optional[UnifiedMessage] = None):
        if msg.state.upper() == "READ":
            return
        ts = time.time()
        self._queue_persist_op(
            "js8_read",
            (
                int(msg.msg_id),
                float(msg.utc_ts or 0.0),
                float(ts),
                bool(self.settings.get("js8_inbox_mark_retrieved_sync", False)),
            ),
        )
        msg.state = "READ"
        msg.read_ts = ts
        self._refresh_table_after_read(
            lambda row: isinstance(row.payload, JS8Message) and row.payload.msg_id == msg.msg_id,
            row_ref=row_ref,
        )

    def _mark_spotter_read(self, msg: SpotterMessage, row_ref: Optional[UnifiedMessage] = None) -> None:
        if msg.state.upper() == "READ":
            return
        ts = time.time()
        self._queue_persist_op("spotter_read", (int(msg.spotter_id), float(ts)))
        msg.state = "READ"
        msg.read_ts = ts
        self._refresh_table_after_read(
            lambda row: isinstance(row.payload, SpotterMessage)
            and row.payload.spotter_id == msg.spotter_id,
            row_ref=row_ref,
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
        self._prune_cache(self._form_cache, self._cache_max_form_entries)
        return questions

    def _load_form_title(self, form_id: str) -> str:
        if form_id in self._form_title_cache:
            return self._form_title_cache[form_id]
        forms_dir = (self.settings.get("js8_forms_path", self.forms_path) or "").strip()
        if not forms_dir:
            return ""
        path = Path(forms_dir) / f"MCF{form_id}.txt"
        if not path.exists():
            return ""
        title = ""
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    if "|" in line:
                        title = line.split("|", 1)[0].strip()
                    else:
                        title = line.strip()
                    break
        except Exception:
            title = ""
        self._form_title_cache[form_id] = title
        self._prune_cache(self._form_title_cache, self._cache_max_form_title_entries)
        return title

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
                read_ts REAL,
                flag_state INTEGER DEFAULT 0
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
            cur.execute("ALTER TABLE js8_messages ADD COLUMN flag_state INTEGER DEFAULT 0")
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

    def _load_js8_from_local(self, force: bool = False, rebuild: bool = True) -> None:
        self._ensure_local_js8_tables()
        db_path = self._local_js8_db()
        msgs: List[JS8Message] = []
        if not db_path or not Path(db_path).exists():
            self.js8_messages = msgs
            if rebuild:
                self._populate_messages_table(force=force)
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text, state, read_ts, flag_state
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
                flag_state=int(r[10] or 0),
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
        if rebuild:
            self._populate_messages_table(force=force)

    def _load_spotter_from_db(self, force: bool = False, rebuild: bool = True) -> None:
        db_path = self._db_path()
        msgs: List[SpotterMessage] = []
        if not db_path or not Path(db_path).exists():
            self.spotter_messages = msgs
            if rebuild:
                self._populate_messages_table(force=force)
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, utc_str, utc_ts, from_call, to_call, form_id, spotter_token,
                       raw_text, decoded_text, state, read_ts, flag_state, relay_via
                FROM spotter_traffic
                ORDER BY utc_ts DESC, id DESC
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("MessageViewer: failed to load spotter traffic: %s", e)
            rows = []
        for r in rows:
            msg = SpotterMessage(
                spotter_id=int(r[0]),
                utc_str=(r[1] or ""),
                utc_ts=float(r[2] or 0.0),
                from_call=(r[3] or "").strip().upper(),
                to_call=(r[4] or "").strip().upper(),
                msg_type=f"F!{r[5]}" if r[5] and not str(r[5]).startswith("F!") else (r[5] or ""),
                raw_text=(r[7] or ""),
                decoded_text=(r[8] or ""),
                state=(r[9] or "UNREAD").upper(),
                read_ts=float(r[10] or 0.0),
                relay_via=(r[12] or "").strip().upper(),
                flag_state=int(r[11] or 0),
            )
            msgs.append(msg)
        self.spotter_messages = msgs
        if rebuild:
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

    def _spotter_offset_key(self) -> str:
        return "spotter_directed_offset"

    def _resolve_directed_path(self) -> Optional[Path]:
        directed = (self.settings.get("js8_directed_path", "") or "").strip()
        if not directed:
            return None
        return Path(directed)

    def _spotter_exists(self, from_call: str, form_id: str, token: str, raw_text: str) -> bool:
        db_path = self._db_path()
        if not db_path:
            return False
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            if token:
                cur.execute(
                    """
                    SELECT 1 FROM spotter_traffic
                    WHERE from_call=? AND form_id=? AND spotter_token=?
                    LIMIT 1
                    """,
                    (from_call, form_id, token),
                )
            else:
                cur.execute(
                    """
                    SELECT 1 FROM spotter_traffic
                    WHERE from_call=? AND form_id=? AND raw_text=?
                    LIMIT 1
                    """,
                    (from_call, form_id, raw_text),
                )
            exists = cur.fetchone() is not None
            conn.close()
            return exists
        except Exception:
            return False

    def _parse_directed_spotter_line(self, line: str) -> Optional[Dict[str, str | float]]:
        if not line:
            return None
        if not line.rstrip().endswith("\u2662"):
            return None
        parts = [p for p in line.strip().split("\t") if p]
        if len(parts) < 5:
            parts = re.split(r"\s+", line.strip(), maxsplit=4)
        if len(parts) < 5:
            return None
        dt_str, _freq_txt, _shift, _snr_txt, msg = parts[0], parts[1], parts[2], parts[3], parts[4]
        if ":" not in msg:
            return None
        msg_upper = msg.upper()
        if "?" in msg_upper or "E?" in msg_upper:
            return None
        if "..." in msg:
            return None
        if re.search(r"\bMSG\b", msg_upper):
            return None
        form_match = re.search(r"F!(\d{3})", msg_upper)
        if not form_match:
            return None
        try:
            ts = datetime.datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=datetime.timezone.utc
            )
        except Exception:
            return None
        relay_via, rest = msg.split(":", 1)
        relay_via = relay_via.strip().upper()
        rest = rest.strip()
        dest_token = (rest.split() or [""])[0]
        dest = dest_token.split(">")[0].strip().strip(",").upper()
        if not dest:
            return None
        de_match = re.search(r"\*DE\*\s*([A-Z0-9/]+)", msg_upper)
        from_call = de_match.group(1) if de_match else relay_via
        form_start = msg_upper.find("F!")
        if form_start < 0:
            return None
        raw_form = msg[form_start:].strip()
        raw_form = re.split(r"\*DE\*", raw_form, 1, flags=re.IGNORECASE)[0].strip()
        if raw_form.endswith("\u2662"):
            raw_form = raw_form[:-1].rstrip()
        token_match = re.search(r"(#[A-Z0-9]{3,})", raw_form.upper())
        token = token_match.group(1) if token_match else ""
        form_id = form_match.group(1)
        return {
            "utc_ts": ts.timestamp(),
            "utc_str": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "from_call": from_call.strip().upper(),
            "to_call": dest.strip().upper(),
            "form_id": form_id,
            "spotter_token": token,
            "raw_form": raw_form,
            "relay_via": relay_via,
        }

    def _ingest_spotter_from_directed(self) -> None:
        directed_path = self._resolve_directed_path()
        if not directed_path or not directed_path.exists():
            return
        self._ensure_spotter_table()
        try:
            offset = int(self.settings.get(self._spotter_offset_key(), 0) or 0)
        except Exception:
            offset = 0
        try:
            size_now = directed_path.stat().st_size
            if offset < 0 or offset > size_now:
                offset = 0
            with directed_path.open("r", encoding="utf-8", errors="ignore") as fh:
                if offset:
                    fh.seek(offset)
                last_pos = fh.tell()
                while True:
                    line = fh.readline()
                    if not line:
                        break
                    last_pos = fh.tell()
                    parsed = self._parse_directed_spotter_line(line)
                    if not parsed:
                        continue
                    form_id = str(parsed.get("form_id") or "").strip()
                    raw_form = str(parsed.get("raw_form") or "").strip()
                    if not form_id or not raw_form:
                        continue
                    from_call = str(parsed.get("from_call") or "").strip().upper()
                    token = str(parsed.get("spotter_token") or "").strip().upper()
                    if not from_call:
                        continue
                    if self._spotter_exists(from_call, form_id, token, raw_form):
                        continue
                    _, resp, comment = self._parse_form_parts(raw_form)
                    decoded = self._decode_form(form_id, resp, comment, raw=raw_form)
                    db_path = self._db_path()
                    if not db_path:
                        continue
                    conn = sqlite3.connect(db_path)
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO spotter_traffic
                            (utc_ts, utc_str, from_call, to_call, form_id, spotter_token,
                             raw_text, decoded_text, state, read_ts, relay_via, ingested_ts)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'UNREAD', 0, ?, ?)
                        """,
                        (
                            float(parsed.get("utc_ts") or 0.0),
                            str(parsed.get("utc_str") or ""),
                            from_call,
                            str(parsed.get("to_call") or "").strip().upper(),
                            form_id,
                            token,
                            raw_form,
                            decoded or raw_form,
                            str(parsed.get("relay_via") or "").strip().upper(),
                            float(time.time()),
                        ),
                    )
                    conn.commit()
                    conn.close()
                self.settings.set(self._spotter_offset_key(), int(last_pos))
                if hasattr(self.settings, "save"):
                    self.settings.save()
        except Exception as e:
            log.debug("MessageViewer: spotter ingest failed reading DIRECTED.TXT: %s", e)

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
