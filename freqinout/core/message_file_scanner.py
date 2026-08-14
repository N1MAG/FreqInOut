from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp", ".raw"}
AUTH_FILE_EXTS = {".b2s", ".k2s", ".sig", ".asc", ".gpg"}
SUPPORTED_EXT = {
    ".b2s",
    ".k2s",
    ".sig",
    ".asc",
    ".gpg",
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
    "flamp": {".txt", ".rtf", *AUTH_FILE_EXTS},
    "varac": {".txt", ".html", ".htm", *AUTH_FILE_EXTS, *IMAGE_EXTS},
    "bbs": set(SUPPORTED_EXT),
}
FLAMP_AUTH_EXTS = set(AUTH_FILE_EXTS)
BBS_HELPER_FILE_PREFIXES = (
    "BBS MSG - ",
    "00 READ FIRST -",
    "00 NOTICE -",
    "01 COMMANDS -",
    "BBS_QUEUE_LIST",
    "BBS_BLOCK_LIST",
)


@dataclass
class FileRecord:
    path: Path
    origin: str
    size: int = 0
    mtime: float = 0.0
    source_id: str = ""
    source_label: str = ""

    def display_name(self) -> str:
        return self.path.name

    def info_line(self) -> str:
        return f"{self.display_name()} - {self.size} bytes"


def is_fio_bbs_helper_file_name(name: object) -> bool:
    clean = Path(str(name or "").strip()).name.upper()
    return any(clean.startswith(prefix.upper()) for prefix in BBS_HELPER_FILE_PREFIXES) or bool(
        re.match(r"^\d{2} TYPE .+\.TXT$", clean)
    )


class MessageFileScanner:
    def __init__(
        self,
        watch_dirs: List[Dict],
        *,
        force: bool,
        base_records: Optional[Dict[str, List[FileRecord]]] = None,
        base_dir_mtimes: Optional[Dict[str, float]] = None,
    ) -> None:
        self._watch_dirs = list(watch_dirs)
        self._force = bool(force)
        self._base_records = base_records or {}
        self._base_dir_mtimes: Dict[str, float] = {}
        for key, value in (base_dir_mtimes or {}).items():
            try:
                self._base_dir_mtimes[self._norm_path(key)] = float(value)
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
        return path_norm == root_norm or path_norm.startswith(root_norm + os.sep)

    def _is_under_any(self, path_norm: str, roots: set[str] | List[str]) -> bool:
        return any(self._is_under(path_norm, root_norm) for root_norm in roots)

    @staticmethod
    def _empty_result() -> Dict[str, Dict[str, FileRecord]]:
        return {"varac": {}, "flmsg": {}, "flamp": {}, "bbs": {}}

    def _full_scan_recursive(
        self,
        base: Path,
        origin: str,
        allowed_exts: Optional[set[str]],
        out_map: Dict[str, Dict[str, FileRecord]],
        dir_mtimes: Dict[str, float],
        *,
        source_id: str = "",
        source_label: str = "",
    ) -> None:
        try:
            dir_mtimes[self._norm_path(base)] = float(base.stat().st_mtime)
        except OSError:
            return
        try:
            with os.scandir(base) as entries:
                for dent in entries:
                    try:
                        if dent.is_dir(follow_symlinks=False):
                            self._full_scan_recursive(
                                Path(dent.path),
                                origin,
                                allowed_exts,
                                out_map,
                                dir_mtimes,
                                source_id=source_id,
                                source_label=source_label,
                            )
                            continue
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        if is_fio_bbs_helper_file_name(dent.name):
                            continue
                        suffix = Path(dent.name).suffix.lower()
                        if suffix not in SUPPORTED_EXT:
                            continue
                        if allowed_exts and suffix not in allowed_exts:
                            continue
                        stat = dent.stat()
                        rec = FileRecord(
                            path=Path(dent.path),
                            origin=origin,
                            size=stat.st_size,
                            mtime=stat.st_mtime,
                            source_id=source_id,
                            source_label=source_label,
                        )
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
        *,
        source_id: str = "",
        source_label: str = "",
    ) -> None:
        try:
            dir_mtimes[self._norm_path(base)] = float(base.stat().st_mtime)
        except OSError:
            return
        try:
            with os.scandir(base) as entries:
                for dent in entries:
                    try:
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        if is_fio_bbs_helper_file_name(dent.name):
                            continue
                        suffix = Path(dent.name).suffix.lower()
                        if suffix not in SUPPORTED_EXT:
                            continue
                        stat = dent.stat()
                        rec = FileRecord(
                            path=Path(dent.path),
                            origin="bbs",
                            size=stat.st_size,
                            mtime=stat.st_mtime,
                            source_id=source_id,
                            source_label=source_label,
                        )
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
        *,
        source_id: str = "",
        source_label: str = "",
    ) -> None:
        base_norm = self._norm_path(base)
        try:
            current_mtime = float(base.stat().st_mtime)
        except OSError:
            return
        dir_mtimes[base_norm] = current_mtime
        previous_mtime = self._base_dir_mtimes.get(base_norm)
        if previous_mtime is not None and abs(previous_mtime - current_mtime) < 1e-6:
            reused_dirs[origin].add(base_norm)
            return
        changed_dirs[origin].add(base_norm)
        try:
            with os.scandir(base) as entries:
                for dent in entries:
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
                                source_id=source_id,
                                source_label=source_label,
                            )
                            continue
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        if is_fio_bbs_helper_file_name(dent.name):
                            continue
                        suffix = Path(dent.name).suffix.lower()
                        if suffix not in SUPPORTED_EXT:
                            continue
                        if allowed_exts and suffix not in allowed_exts:
                            continue
                        stat = dent.stat()
                        rec = FileRecord(
                            path=Path(dent.path),
                            origin=origin,
                            size=stat.st_size,
                            mtime=stat.st_mtime,
                            source_id=source_id,
                            source_label=source_label,
                        )
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
        *,
        source_id: str = "",
        source_label: str = "",
    ) -> None:
        base_norm = self._norm_path(base)
        try:
            current_mtime = float(base.stat().st_mtime)
        except OSError:
            return
        dir_mtimes[base_norm] = current_mtime
        previous_mtime = self._base_dir_mtimes.get(base_norm)
        if previous_mtime is not None and abs(previous_mtime - current_mtime) < 1e-6:
            reused_dirs["bbs"].add(base_norm)
            return
        changed_dirs["bbs"].add(base_norm)
        try:
            with os.scandir(base) as entries:
                for dent in entries:
                    try:
                        if not dent.is_file(follow_symlinks=False):
                            continue
                        if is_fio_bbs_helper_file_name(dent.name):
                            continue
                        suffix = Path(dent.name).suffix.lower()
                        if suffix not in SUPPORTED_EXT:
                            continue
                        stat = dent.stat()
                        rec = FileRecord(
                            path=Path(dent.path),
                            origin="bbs",
                            size=stat.st_size,
                            mtime=stat.st_mtime,
                            source_id=source_id,
                            source_label=source_label,
                        )
                        key = self._norm_path(rec.path)
                        out_map["bbs"][key] = rec
                        seen_files["bbs"].add(key)
                    except OSError:
                        continue
        except OSError:
            return

    @staticmethod
    def _finalize_maps(records_map: Dict[str, Dict[str, FileRecord]]) -> Dict[str, List[FileRecord]]:
        out: Dict[str, List[FileRecord]] = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
        for origin in out:
            out[origin] = sorted(records_map.get(origin, {}).values(), key=lambda item: item.mtime, reverse=True)
        return out

    def _run_full(self) -> tuple[Dict[str, List[FileRecord]], Dict[str, float]]:
        records_map = self._empty_result()
        dir_mtimes: Dict[str, float] = {}
        for entry in self._watch_dirs:
            origin = str(entry.get("origin", "") or "").strip().lower()
            if origin not in records_map:
                continue
            path = str(entry.get("path", "") or "").strip()
            if not path:
                continue
            source_id = str(entry.get("source_id", "") or "").strip()
            source_label = str(entry.get("source_label", "") or "").strip()
            base = Path(path)
            if not base.exists():
                continue
            if origin == "bbs":
                self._full_scan_bbs(base, records_map, dir_mtimes, source_id=source_id, source_label=source_label)
            else:
                self._full_scan_recursive(
                    base,
                    origin,
                    ORIGIN_EXTS.get(origin),
                    records_map,
                    dir_mtimes,
                    source_id=source_id,
                    source_label=source_label,
                )
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
                    source_id=str(getattr(rec, "source_id", "") or ""),
                    source_label=str(getattr(rec, "source_label", "") or ""),
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
            path = str(entry.get("path", "") or "").strip()
            if not path:
                continue
            source_id = str(entry.get("source_id", "") or "").strip()
            source_label = str(entry.get("source_label", "") or "").strip()
            base = Path(path)
            base_norm = self._norm_path(base)
            if not base.exists():
                missing_roots[origin].add(base_norm)
                continue
            if origin == "bbs":
                self._scan_changed_bbs(
                    base,
                    records_map,
                    dir_mtimes,
                    seen_files,
                    changed_dirs,
                    reused_dirs,
                    source_id=source_id,
                    source_label=source_label,
                )
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
                    source_id=source_id,
                    source_label=source_label,
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

    def scan(self) -> tuple[Dict[str, List[FileRecord]], Dict[str, float], str]:
        have_base = any(bool(value) for value in (self._base_records or {}).values())
        try:
            if self._force or not have_base:
                records, dir_mtimes = self._run_full()
                return records, dir_mtimes, "full"
            records, dir_mtimes = self._run_incremental()
            return records, dir_mtimes, "incremental"
        except Exception:
            records, dir_mtimes = self._run_full()
            return records, dir_mtimes, "fallback"
