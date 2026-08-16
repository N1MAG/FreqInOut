from __future__ import annotations

import configparser
import os
import platform
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence

from freqinout.core.config_autodiscovery import (
    JS8CALL_APP_NAMES,
    JS8CALL_COMMAND_NAMES,
    discover_js8call_file_profiles,
    select_js8call_file_profile,
)


@dataclass(frozen=True)
class PathDetectionResult:
    key: str
    label: str
    path: str
    confidence: str
    reason: str
    exists: bool
    target_type: str


class SoftwarePathDetector:
    def __init__(self, settings) -> None:
        self.settings = settings
        self.system = platform.system()
        self.home = Path.home()

    def detect_fast_light(self) -> Dict[str, PathDetectionResult]:
        results: Dict[str, PathDetectionResult] = {}
        results["path_flrig"] = self._detect_program_path(
            key="path_flrig",
            label="FLRig launch path",
            tokens=("flrig", "FLRig"),
            bundle_names=("flrig", "FLRig"),
            windows_files=(
                Path(os.environ.get("ProgramFiles", "")) / "flrig" / "flrig.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "flrig" / "flrig.exe",
                self.home / "AppData" / "Local" / "flrig" / "flrig.exe",
            ),
            linux_files=(Path("/usr/bin/flrig"), Path("/usr/local/bin/flrig"), Path("/opt/flrig/flrig")),
        )
        results["path_fldigi"] = self._detect_program_path(
            key="path_fldigi",
            label="FLDigi launch path",
            tokens=("fldigi", "FLDigi"),
            bundle_names=("fldigi", "FLDigi"),
            windows_files=(
                Path(os.environ.get("ProgramFiles", "")) / "fldigi" / "fldigi.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "fldigi" / "fldigi.exe",
                self.home / "AppData" / "Local" / "fldigi" / "fldigi.exe",
            ),
            linux_files=(Path("/usr/bin/fldigi"), Path("/usr/local/bin/fldigi"), Path("/opt/fldigi/fldigi")),
        )
        results["path_flmsg"] = self._detect_program_path(
            key="path_flmsg",
            label="FLMsg launch path",
            tokens=("flmsg", "FLMsg"),
            bundle_names=("flmsg", "FLMsg"),
            windows_files=(
                Path(os.environ.get("ProgramFiles", "")) / "flmsg" / "flmsg.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "flmsg" / "flmsg.exe",
                self.home / "AppData" / "Local" / "flmsg" / "flmsg.exe",
            ),
            linux_files=(Path("/usr/bin/flmsg"), Path("/usr/local/bin/flmsg"), Path("/opt/flmsg/flmsg")),
        )
        results["path_flamp"] = self._detect_program_path(
            key="path_flamp",
            label="FLAmp launch path",
            tokens=("flamp", "FLAmp"),
            bundle_names=("flamp", "FLAmp"),
            windows_files=(
                Path(os.environ.get("ProgramFiles", "")) / "flamp" / "flamp.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "flamp" / "flamp.exe",
                self.home / "AppData" / "Local" / "flamp" / "flamp.exe",
            ),
            linux_files=(Path("/usr/bin/flamp"), Path("/usr/local/bin/flamp"), Path("/opt/flamp/flamp")),
        )
        results["fldigi_log_path"] = self._detect_fldigi_log_dir()
        results["message_paths.flmsg"] = self._detect_flmsg_messages_dir()
        results["message_paths.flamp"] = self._detect_flamp_messages_dir()
        return results

    def detect_js8(self) -> Dict[str, PathDetectionResult]:
        results: Dict[str, PathDetectionResult] = {}
        results["path_js8call"] = self._detect_install_target(
            key="path_js8call",
            label="JS8Call install folder",
            tokens=JS8CALL_COMMAND_NAMES,
            bundle_names=JS8CALL_APP_NAMES,
            windows_files=(
                Path(os.environ.get("ProgramFiles", "")) / "JS8Call" / "js8call.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "JS8Call" / "js8call.exe",
                self.home / "AppData" / "Local" / "JS8Call" / "js8call.exe",
                Path(os.environ.get("ProgramFiles", "")) / "JS8Call-improved" / "js8call.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "JS8Call-improved" / "js8call.exe",
                self.home / "AppData" / "Local" / "JS8Call-improved" / "js8call.exe",
                Path(os.environ.get("ProgramFiles", "")) / "JS8Call Subspace" / "js8call.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "JS8Call Subspace" / "js8call.exe",
                self.home / "AppData" / "Local" / "JS8Call Subspace" / "js8call.exe",
            ),
            linux_files=(
                Path("/usr/bin/js8call"),
                Path("/usr/local/bin/js8call"),
                Path("/opt/js8call/js8call"),
                Path("/usr/bin/js8call-improved"),
                Path("/usr/local/bin/js8call-improved"),
                Path("/opt/js8call-improved/js8call"),
                Path("/opt/js8call-subspace/js8call"),
            ),
            prefer_bundle_dir=True,
        )
        results["js8_directed_path"] = self._detect_js8_directed_path()
        results["js8_forms_path"] = self._detect_js8_forms_path()
        results["path_js8spotter"] = self._detect_program_path(
            key="path_js8spotter",
            label="JS8Spotter launch path",
            tokens=("js8spotter", "JS8Spotter"),
            bundle_names=("JS8Spotter", "js8spotter"),
            windows_files=(
                Path(os.environ.get("ProgramFiles", "")) / "JS8Spotter" / "JS8Spotter.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "JS8Spotter" / "JS8Spotter.exe",
                self.home / "AppData" / "Local" / "Programs" / "JS8Spotter" / "JS8Spotter.exe",
            ),
            linux_files=(
                Path("/usr/bin/js8spotter"),
                Path("/usr/local/bin/js8spotter"),
                self.home / ".local" / "bin" / "js8spotter",
                self.home / "bin" / "js8spotter",
            ),
        )
        results["path_commstat"] = self._detect_program_path(
            key="path_commstat",
            label="CommStat launch path",
            tokens=("commstat", "CommStat"),
            bundle_names=("CommStat", "commstat"),
            windows_files=(
                Path(os.environ.get("ProgramFiles", "")) / "CommStat" / "CommStat.exe",
                Path(os.environ.get("ProgramFiles(x86)", "")) / "CommStat" / "CommStat.exe",
                self.home / "AppData" / "Local" / "Programs" / "CommStat" / "CommStat.exe",
            ),
            linux_files=(
                Path("/usr/bin/commstat"),
                Path("/usr/local/bin/commstat"),
                self.home / ".local" / "bin" / "commstat",
                self.home / "bin" / "commstat",
            ),
        )
        return results

    def detect_varac(self) -> Dict[str, PathDetectionResult]:
        results: Dict[str, PathDetectionResult] = {}
        install = self._detect_varac_install_dir()
        results["varac_path"] = install
        install_dir = Path(install.path) if install.path else None
        results["varac_db_path"] = self._detect_varac_db_file(install_dir)
        results["varac_ini_path"] = self._detect_varac_ini_file(install_dir)
        results["message_paths.varac"] = self._detect_varac_incoming_dir(install_dir)
        results["varac_outbox_dir"] = self._detect_varac_outbox_dir(install_dir)
        results["varac_bbs_dir"] = self._detect_varac_bbs_dir(install_dir)
        results["varac_bbs_archive_dir"] = self._detect_varac_bbs_archive_dir(install_dir, results["varac_bbs_dir"])
        return results

    def _detect_program_path(
        self,
        *,
        key: str,
        label: str,
        tokens: Sequence[str],
        bundle_names: Sequence[str],
        windows_files: Sequence[Path],
        linux_files: Sequence[Path],
    ) -> PathDetectionResult:
        if self.system == "Darwin":
            for bundle_name in bundle_names:
                bundle = self._existing_paths(self._macos_bundle_candidates(bundle_name))
                if bundle:
                    executable = self._macos_bundle_executable(bundle[0], tokens)
                    if executable is not None:
                        return self._result(key, label, executable, "verified", "Found macOS app bundle executable", "file")
            for token in tokens:
                resolved = shutil.which(token)
                if resolved:
                    return self._result(key, label, Path(resolved), "verified", "Found on PATH", "file")
            return self._not_found(key, label, "No installed app bundle or PATH command found", "file")

        for token in tokens:
            resolved = shutil.which(token)
            if resolved:
                return self._result(key, label, Path(resolved), "verified", "Found on PATH", "file")

        candidates = windows_files if self.system == "Windows" else linux_files
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(key, label, existing[0], "high", "Found in standard install location", "file")
        return self._not_found(key, label, "No installed executable found in standard locations", "file")

    def _detect_install_target(
        self,
        *,
        key: str,
        label: str,
        tokens: Sequence[str],
        bundle_names: Sequence[str],
        windows_files: Sequence[Path],
        linux_files: Sequence[Path],
        prefer_bundle_dir: bool,
    ) -> PathDetectionResult:
        if self.system == "Darwin":
            for bundle_name in bundle_names:
                bundle = self._existing_paths(self._macos_bundle_candidates(bundle_name))
                if bundle:
                    target = bundle[0] if prefer_bundle_dir else self._macos_bundle_executable(bundle[0], tokens)
                    if target is not None:
                        target_type = "app_bundle" if target.suffix.lower() == ".app" else "directory"
                        return self._result(key, label, target, "verified", "Found macOS application bundle", target_type)
            for token in tokens:
                resolved = shutil.which(token)
                if resolved:
                    return self._result(key, label, Path(resolved).parent, "high", "Derived install folder from PATH command", "directory")
            return self._not_found(key, label, "No installed app bundle or PATH command found", "directory")

        for token in tokens:
            resolved = shutil.which(token)
            if resolved:
                return self._result(key, label, Path(resolved).parent, "high", "Derived install folder from PATH command", "directory")

        candidates = windows_files if self.system == "Windows" else linux_files
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(key, label, existing[0].parent, "verified", "Found install folder from standard location", "directory")
        return self._not_found(key, label, "No installed folder found in standard locations", "directory")

    def _detect_fldigi_log_dir(self) -> PathDetectionResult:
        candidates: List[Path] = []
        if self.system == "Windows":
            candidates.extend(
                [
                    self.home / "fldigi.files",
                    Path(os.environ.get("APPDATA", "")) / "fldigi",
                    Path(os.environ.get("LOCALAPPDATA", "")) / "fldigi",
                ]
            )
        elif self.system == "Darwin":
            candidates.extend(
                [
                    self.home / "Library" / "Application Support" / "fldigi",
                    self.home / ".fldigi",
                ]
            )
        else:
            candidates.extend([self.home / ".fldigi", self.home / ".local" / "share" / "fldigi"])
        for candidate in self._unique_paths(candidates):
            if candidate.is_dir() and (list(candidate.glob("fldigi*.log")) or (candidate / "images").exists()):
                return self._result(
                    "fldigi_log_path",
                    "FLDigi log path",
                    candidate,
                    "verified",
                    "Found FLDigi logs or images directory",
                    "directory",
                )
        return self._not_found("fldigi_log_path", "FLDigi log path", "No FLDigi log directory found", "directory")

    def _detect_flmsg_messages_dir(self) -> PathDetectionResult:
        candidates = [root / "ICS" / "messages" for root in self._nbems_roots()]
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(
                "message_paths.flmsg",
                "FLMsg ICS/Messages path",
                existing[0],
                "verified",
                "Found NBEMS ICS/messages directory",
                "directory",
            )
        return self._not_found(
            "message_paths.flmsg",
            "FLMsg ICS/Messages path",
            "No NBEMS ICS/messages directory found",
            "directory",
        )

    def _detect_flamp_messages_dir(self) -> PathDetectionResult:
        candidates: List[Path] = []
        for root in self._nbems_roots():
            candidates.append(root / "FLAMP" / "rx")
            candidates.append(root / "FLAMP")
        for candidate in self._unique_paths(candidates):
            if candidate.is_dir():
                reason = "Found NBEMS FLAMP/rx directory" if candidate.name.lower() == "rx" else "Found NBEMS FLAMP directory"
                return self._result(
                    "message_paths.flamp",
                    "FLAmp message path",
                    candidate,
                    "verified",
                    reason,
                    "directory",
                )
        return self._not_found("message_paths.flamp", "FLAmp message path", "No NBEMS FLAMP directory found", "directory")

    def _detect_js8_directed_path(self) -> PathDetectionResult:
        file_profiles = discover_js8call_file_profiles(platform=self.system, home=self.home)
        selected_profile = select_js8call_file_profile(file_profiles)
        if selected_profile is not None:
            return self._result(
                "js8_directed_path",
                "JS8Call DIRECTED.TXT path",
                Path(selected_profile.directed_path),
                selected_profile.confidence,
                selected_profile.reason,
                "file",
            )
        if sum(1 for profile in file_profiles if profile.directed_path) > 1:
            return self._not_found(
                "js8_directed_path",
                "JS8Call DIRECTED.TXT path",
                "Multiple JS8Call profiles have DIRECTED.TXT; select a radio/profile before Auto-Fill.",
                "file",
            )
        for base in self._js8_data_roots():
            directed = base / "DIRECTED.TXT"
            if directed.is_file():
                return self._result(
                    "js8_directed_path",
                    "JS8Call DIRECTED.TXT path",
                    directed,
                    "verified",
                    "Found DIRECTED.TXT in JS8Call data directory",
                    "file",
                )
        existing_root = self._first_existing_dir(self._js8_data_roots())
        if existing_root is not None:
            return self._not_found(
                "js8_directed_path",
                "JS8Call DIRECTED.TXT path",
                f"JS8Call data directory exists at {existing_root}, but DIRECTED.TXT was not found",
                "file",
            )
        return self._not_found(
            "js8_directed_path",
            "JS8Call DIRECTED.TXT path",
            "No JS8Call data directory with DIRECTED.TXT found",
            "file",
        )

    def _detect_js8_forms_path(self) -> PathDetectionResult:
        candidates: List[Path] = []
        custom_override = str(self.settings.get("nbems_custom_forms_path", "") or "").strip()
        if custom_override:
            candidates.append(Path(custom_override))
        for raw in self._message_path_values().values():
            if not raw:
                continue
            p = Path(raw)
            candidates.extend([p / "CUSTOM", *[parent / "CUSTOM" for parent in p.parents]])
        for root in self._nbems_roots():
            candidates.append(root / "CUSTOM")
        for candidate in self._unique_paths(candidates):
            if candidate.is_dir() and list(candidate.glob("MCF*.txt")):
                return self._result(
                    "js8_forms_path",
                    "JS8Spotter forms path",
                    candidate,
                    "verified",
                    "Found forms directory containing MCF*.txt files",
                    "directory",
                )
        return self._not_found(
            "js8_forms_path",
            "JS8Spotter forms path",
            "No forms directory containing MCF*.txt files found",
            "directory",
        )

    def _detect_varac_install_dir(self) -> PathDetectionResult:
        candidates: List[Path] = []
        if self.system == "Windows":
            candidates.extend(
                [
                    Path(os.environ.get("ProgramFiles", "")) / "VarAC",
                    Path(os.environ.get("ProgramFiles(x86)", "")) / "VarAC",
                    Path(os.environ.get("LOCALAPPDATA", "")) / "VarAC",
                    self.home / "RadioTools" / "Programs" / "VarAC_files",
                    self.home / "AppData" / "Local" / "VarAC",
                ]
            )
        elif self.system == "Darwin":
            candidates.extend(
                [
                    self.home / ".wine" / "drive_c" / "VarAC",
                    self.home / ".wine" / "drive_c" / "Program Files" / "VarAC",
                    self.home / ".wine" / "drive_c" / "Program Files (x86)" / "VarAC",
                    self.home / "RadioTools" / "Programs" / "VarAC_files",
                    self.home / "Applications" / "VarAC.app",
                    Path("/Applications/VarAC.app"),
                ]
            )
        else:
            candidates.extend(
                [
                    self.home / ".wine" / "drive_c" / "VarAC",
                    self.home / ".wine" / "drive_c" / "Program Files" / "VarAC",
                    self.home / ".wine" / "drive_c" / "Program Files (x86)" / "VarAC",
                    self.home / "RadioTools" / "Programs" / "VarAC_files",
                    self.home / ".varac",
                ]
            )
        for candidate in self._unique_paths(candidates):
            if candidate.suffix.lower() == ".app" and candidate.exists():
                return self._result("varac_path", "VarAC install folder", candidate, "high", "Found VarAC application bundle", "app_bundle")
            exe = candidate / "VarAC.exe"
            db = candidate / "VarAC.db"
            if candidate.is_dir() and (exe.exists() or db.exists()):
                return self._result(
                    "varac_path",
                    "VarAC install folder",
                    candidate,
                    "verified",
                    "Found VarAC folder with VarAC.exe or VarAC.db",
                    "directory",
                )
        return self._not_found("varac_path", "VarAC install folder", "No VarAC install folder found", "directory")

    def _detect_varac_incoming_dir(self, install_dir: Path | None) -> PathDetectionResult:
        candidates: List[Path] = []
        if install_dir is not None:
            ini_path = self._first_existing_path([install_dir / "VarAC.ini", install_dir / "varac.ini"])
            ini_incoming = self._varac_ini_existing_path(ini_path, "FILES", "IncomingFilesDir")
            if ini_incoming is not None:
                candidates.append(ini_incoming)
            candidates.extend(
                [
                    install_dir / "INCOMING",
                    install_dir / "Received Files",
                    install_dir / "ReceivedFiles",
                    install_dir / "Incoming",
                    install_dir / "incoming",
                    install_dir / "INBOX",
                    install_dir / "Inbox",
                ]
            )
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(
                "message_paths.varac",
                "VarAC incoming files path",
                existing[0],
                "verified",
                "Found conventional VarAC incoming-files directory",
                "directory",
            )
        return self._not_found(
            "message_paths.varac",
            "VarAC incoming files path",
            "No conventional VarAC incoming-files directory found",
            "directory",
        )

    def _detect_varac_db_file(self, install_dir: Path | None) -> PathDetectionResult:
        candidates: List[Path] = []
        if install_dir is not None:
            candidates.extend([install_dir / "VarAC.db", install_dir / "varac.db"])
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(
                "varac_db_path",
                "VarAC database",
                existing[0],
                "verified",
                "Found conventional VarAC database file",
                "file",
            )
        return self._not_found("varac_db_path", "VarAC database", "No conventional VarAC database found", "file")

    def _detect_varac_ini_file(self, install_dir: Path | None) -> PathDetectionResult:
        candidates: List[Path] = []
        if install_dir is not None:
            candidates.extend([install_dir / "VarAC.ini", install_dir / "varac.ini"])
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(
                "varac_ini_path",
                "VarAC INI file",
                existing[0],
                "verified",
                "Found conventional VarAC INI file",
                "file",
            )
        return self._not_found("varac_ini_path", "VarAC INI file", "No conventional VarAC INI file found", "file")

    def _detect_varac_bbs_dir(self, install_dir: Path | None) -> PathDetectionResult:
        candidates: List[Path] = []
        if install_dir is not None:
            ini_path = self._first_existing_path([install_dir / "VarAC.ini", install_dir / "varac.ini"])
            ini_bbs = self._varac_ini_existing_path(ini_path, "BBS", "BBSDirectory")
            if ini_bbs is not None:
                candidates.append(ini_bbs)
            candidates.extend([install_dir / "BBS", install_dir / "bbs", install_dir / "BBS Files"])
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(
                "varac_bbs_dir",
                "VarAC BBS directory",
                existing[0],
                "verified",
                "Found conventional VarAC BBS directory",
                "directory",
            )
        return self._not_found("varac_bbs_dir", "VarAC BBS directory", "No conventional VarAC BBS directory found", "directory")

    def _detect_varac_outbox_dir(self, install_dir: Path | None) -> PathDetectionResult:
        candidates: List[Path] = []
        if install_dir is not None:
            candidates.extend(
                [
                    install_dir / "OUTGOING",
                    install_dir / "Outbox",
                    install_dir / "OUTBOX",
                    install_dir / "outbox",
                    install_dir / "Outgoing",
                    install_dir / "Outgoing Files",
                    install_dir / "OutgoingFiles",
                ]
            )
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(
                "varac_outbox_dir",
                "VarAC Outbox directory",
                existing[0],
                "verified",
                "Found conventional VarAC outbox directory",
                "directory",
            )
        return self._not_found(
            "varac_outbox_dir",
            "VarAC Outbox directory",
            "No conventional VarAC outbox directory found",
            "directory",
        )

    def _detect_varac_bbs_archive_dir(
        self,
        install_dir: Path | None,
        bbs_result: PathDetectionResult,
    ) -> PathDetectionResult:
        candidates: List[Path] = []
        if bbs_result.path:
            bbs_dir = Path(bbs_result.path)
            candidates.extend([bbs_dir / "Archive", bbs_dir / "archive"])
        if install_dir is not None:
            candidates.extend([install_dir / "BBS" / "Archive", install_dir / "Archive"])
        existing = self._existing_paths(candidates)
        if existing:
            return self._result(
                "varac_bbs_archive_dir",
                "VarAC BBS archive directory",
                existing[0],
                "verified",
                "Found conventional VarAC BBS archive directory",
                "directory",
            )
        return self._not_found(
            "varac_bbs_archive_dir",
            "VarAC BBS archive directory",
            "No conventional VarAC BBS archive directory found",
            "directory",
        )

    def _varac_ini_existing_path(self, ini_path: Path | None, section: str, option: str) -> Path | None:
        if ini_path is None or not ini_path.is_file():
            return None
        parser = configparser.ConfigParser(interpolation=None)
        try:
            parser.read(ini_path, encoding="utf-8")
            value = str(parser.get(section, option, fallback="") or "").strip()
        except Exception:
            return None
        if not value:
            return None
        path = Path(os.path.expandvars(os.path.expanduser(value)))
        return path if path.exists() else None

    @staticmethod
    def _first_existing_path(candidates: Sequence[Path]) -> Path | None:
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _js8_data_roots(self) -> List[Path]:
        if self.system == "Windows":
            return self._unique_paths(
                [
                    Path(os.environ.get("LOCALAPPDATA", "")) / "JS8Call",
                    Path(os.environ.get("APPDATA", "")) / "JS8Call",
                    self.home / "AppData" / "Local" / "JS8Call",
                ]
            )
        if self.system == "Darwin":
            return self._unique_paths([self.home / "Library" / "Application Support" / "JS8Call"])
        return self._unique_paths(
            [
                self.home / ".local" / "share" / "JS8Call",
                self.home / ".config" / "JS8Call",
                self.home / ".var" / "app" / "org.js8call.JS8Call" / "data" / "JS8Call",
            ]
        )

    def _nbems_roots(self) -> List[Path]:
        roots = [self.home / "NBEMS.files", self.home / ".nbems", self.home / "Documents" / "NBEMS.files"]
        if self.system == "Darwin":
            roots.append(self.home / "Library" / "Application Support" / "NBEMS.files")
        return self._unique_paths(roots)

    def _message_path_values(self) -> Mapping[str, str]:
        raw = self.settings.get("message_paths", {}) or {}
        if not isinstance(raw, dict):
            return {}
        out: Dict[str, str] = {}
        for key, value in raw.items():
            txt = str(value or "").strip()
            if txt:
                out[str(key)] = txt
        return out

    def _macos_bundle_candidates(self, app_name: str) -> List[Path]:
        normalized = app_name if app_name.lower().endswith(".app") else f"{app_name}.app"
        roots = [
            Path("/Applications"),
            Path("/Applications") / "RadioApps",
            self.home / "Applications",
            self.home / "Applications" / "RadioApps",
            self.home / "RadioTools" / "Programs",
        ]
        candidates: List[Path] = [root / normalized for root in roots]
        stem = Path(normalized).stem
        for root in roots:
            if not root.is_dir():
                continue
            candidates.extend(sorted(root.glob(f"{stem}-*.app")))
            candidates.extend(sorted(root.glob(f"{stem.upper()}-*.app")))
            candidates.extend(sorted(root.glob(f"{stem.lower()}-*.app")))
        return self._unique_paths(candidates)

    def _macos_bundle_executable(self, bundle: Path, names: Sequence[str]) -> Path | None:
        candidates: List[Path] = []
        for name in names:
            stem = Path(name).stem
            candidates.append(bundle / "Contents" / "MacOS" / stem)
            candidates.append(bundle / "Contents" / "MacOS" / stem.lower())
            candidates.append(bundle / "Contents" / "MacOS" / stem.upper())
        existing = self._existing_paths(candidates)
        if existing:
            return existing[0]
        return None

    def _result(self, key: str, label: str, path: Path, confidence: str, reason: str, target_type: str) -> PathDetectionResult:
        return PathDetectionResult(
            key=key,
            label=label,
            path=str(path),
            confidence=confidence,
            reason=reason,
            exists=path.exists(),
            target_type=target_type,
        )

    def _not_found(self, key: str, label: str, reason: str, target_type: str) -> PathDetectionResult:
        return PathDetectionResult(
            key=key,
            label=label,
            path="",
            confidence="not_found",
            reason=reason,
            exists=False,
            target_type=target_type,
        )

    def _first_existing_dir(self, paths: Iterable[Path]) -> Path | None:
        for path in self._unique_paths(paths):
            if path.exists() and path.is_dir():
                return path
        return None

    def _existing_paths(self, paths: Iterable[Path]) -> List[Path]:
        return [path for path in self._unique_paths(paths) if path.exists()]

    def _unique_paths(self, paths: Iterable[Path]) -> List[Path]:
        out: List[Path] = []
        seen: set[str] = set()
        for raw in paths:
            txt = str(raw or "").strip()
            if not txt:
                continue
            expanded = Path(os.path.expandvars(os.path.expanduser(txt)))
            key = os.path.normcase(os.path.normpath(str(expanded)))
            if key in seen:
                continue
            seen.add(key)
            out.append(expanded)
        return out
