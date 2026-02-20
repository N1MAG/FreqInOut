from __future__ import annotations

import os
import platform
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PySide6.QtCore import QObject, QTimer, Signal

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService


LAUNCH_APP_ORDER: List[str] = [
    "FLRig",
    "FLDigi",
    "FLAmp",
    "FLMsg",
    "VarAC",
    "JS8Call",
    "JS8Spotter",
    "CommStat",
]


LAUNCH_APP_META: Dict[str, Dict[str, Any]] = {
    "FLRig": {
        "path_key": "path_flrig",
        "legacy_autostart_key": "autostart_flrig",
        "fallback_cmds": ["flrig", "FLRig"],
        "folder_candidates": ["flrig.exe", "FLRig.exe", "flrig", "FLRig"],
    },
    "FLDigi": {
        "path_key": "path_fldigi",
        "legacy_autostart_key": "autostart_fldigi",
        "fallback_cmds": ["fldigi", "FLDigi"],
        "folder_candidates": ["fldigi.exe", "FLDigi.exe", "fldigi", "FLDigi"],
    },
    "FLAmp": {
        "path_key": "path_flamp",
        "legacy_autostart_key": "autostart_flamp",
        "fallback_cmds": ["flamp", "FLAmp"],
        "folder_candidates": ["flamp.exe", "FLAmp.exe", "flamp", "FLAmp"],
    },
    "FLMsg": {
        "path_key": "path_flmsg",
        "legacy_autostart_key": "autostart_flmsg",
        "fallback_cmds": ["flmsg", "FLMsg"],
        "folder_candidates": ["flmsg.exe", "FLMsg.exe", "flmsg", "FLMsg"],
    },
    "VarAC": {
        "path_key": "varac_path",
        "legacy_autostart_key": None,
        "fallback_cmds": ["VarAC", "varac"],
        "folder_candidates": ["VarAC.exe", "varac.exe", "VarAC", "varac"],
    },
    "JS8Call": {
        "path_key": "path_js8call",
        "legacy_autostart_key": "autostart_js8call",
        "fallback_cmds": ["js8call", "JS8Call"],
        "folder_candidates": ["JS8Call.exe", "js8call.exe", "JS8Call", "js8call"],
    },
    "JS8Spotter": {
        "path_key": "path_js8spotter",
        "legacy_autostart_key": None,
        "fallback_cmds": ["js8spotter", "JS8Spotter"],
        "folder_candidates": ["js8spotter.exe", "JS8Spotter.exe", "js8spotter.py", "js8spotter", "JS8Spotter"],
    },
    "CommStat": {
        "path_key": "path_commstat",
        "legacy_autostart_key": None,
        "fallback_cmds": ["commstat", "CommStat"],
        "folder_candidates": ["commstat.exe", "CommStat.exe", "commstat.py", "commstat", "CommStat"],
    },
}


class LaunchOrchestrator(QObject):
    sequence_started = Signal(object)
    sequence_progress = Signal(object)
    sequence_finished = Signal(object)

    def __init__(self, settings: SettingsManager, parent: QObject | None = None):
        super().__init__(parent)
        self.settings = settings
        self.status = SoftwareStatusService(settings)
        self._active = False
        self._cancel_requested = False
        self._trigger = ""
        self._queue: List[str] = []
        self._index = 0
        self._results: List[Dict[str, Any]] = []
        self._current_name: Optional[str] = None
        self._current_cmd: Optional[List[str]] = None
        self._current_started_monotonic = 0.0
        self._wait_timeout_sec = 30
        self._poll_timer = QTimer(self)
        self._poll_timer.setInterval(500)
        self._poll_timer.timeout.connect(self._poll_current_readiness)
        self._migrate_if_needed()

    @staticmethod
    def is_truthy(val: Any) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        if isinstance(val, str):
            return val.strip().lower() in {"1", "true", "yes", "on"}
        return False

    def build_default_items(self, existing: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        existing_map: Dict[str, Dict[str, Any]] = {}
        existing_order: List[str] = []
        for item in existing or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if name:
                existing_map[name] = item
                existing_order.append(name)
        ordered_names: List[str] = []
        seen: set[str] = set()
        for name in existing_order:
            if name not in LAUNCH_APP_ORDER or name in seen:
                continue
            ordered_names.append(name)
            seen.add(name)
        for name in LAUNCH_APP_ORDER:
            if name in seen:
                continue
            ordered_names.append(name)
            seen.add(name)
        out: List[Dict[str, Any]] = []
        for name in ordered_names:
            prev = existing_map.get(name, {})
            default_startup = False
            legacy_key = LAUNCH_APP_META.get(name, {}).get("legacy_autostart_key")
            if legacy_key:
                default_startup = self.is_truthy(self.settings.get(str(legacy_key), False))
            out.append(
                {
                    "name": name,
                    "enabled": bool(prev.get("enabled", True)),
                    "startup": bool(prev.get("startup", default_startup)),
                }
            )
        return out

    def get_launch_items(self) -> List[Dict[str, Any]]:
        self._migrate_if_needed()
        raw = self.settings.get("launch_control_items", [])
        if not isinstance(raw, list):
            raw = []
        normalized = self.build_default_items(raw)
        return normalized

    def set_launch_items(self, items: List[Dict[str, Any]], launch_all_with_startup: bool) -> None:
        normalized = self.build_default_items(items)
        batch: Dict[str, Any] = {
            "launch_control_items": normalized,
            "launch_control_enabled": bool(launch_all_with_startup),
            "launch_control_migrated_v1": True,
            "launch_readiness_timeout_sec": int(self.settings.get("launch_readiness_timeout_sec", 30) or 30),
        }
        for item in normalized:
            name = str(item.get("name", "")).strip()
            startup = bool(item.get("startup", False))
            legacy_key = LAUNCH_APP_META.get(name, {}).get("legacy_autostart_key")
            if legacy_key:
                batch[str(legacy_key)] = startup
        if hasattr(self.settings, "set_many"):
            self.settings.set_many(batch, save=True)  # type: ignore[attr-defined]
        else:
            for key, val in batch.items():
                self.settings.set(key, val)

    def start_startup_sequence(self) -> bool:
        if self._active:
            return False
        launch_all = self.is_truthy(self.settings.get("launch_control_enabled", True))
        if not launch_all:
            return False
        items = self.get_launch_items()
        queue = self._build_queue(items, startup_only=True)
        if not queue:
            return False
        return self._start_sequence("startup", queue)

    def start_manual_sequence(self, items: Optional[List[Dict[str, Any]]] = None) -> bool:
        if self._active:
            return False
        base_items = self.build_default_items(items) if items is not None else self.get_launch_items()
        queue = self._build_queue(base_items, startup_only=False)
        if not queue:
            return False
        return self._start_sequence("manual", queue)

    def stop_sequence(self) -> None:
        if not self._active:
            return
        self._cancel_requested = True

    def is_active(self) -> bool:
        return self._active

    def app_path_key(self, name: str) -> str:
        return str(LAUNCH_APP_META.get(name, {}).get("path_key", "") or "")

    def is_configured(self, name: str) -> bool:
        path_key = self.app_path_key(name)
        if not path_key:
            return False
        raw = self.settings.get(path_key, "")
        return bool(str(raw or "").strip())

    def _migrate_if_needed(self) -> None:
        migrated = self.is_truthy(self.settings.get("launch_control_migrated_v1", False))
        raw_items = self.settings.get("launch_control_items", None)
        if migrated and isinstance(raw_items, list):
            return
        defaults = self.build_default_items(raw_items if isinstance(raw_items, list) else None)
        batch = {
            "launch_control_items": defaults,
            "launch_control_enabled": bool(self.settings.get("launch_control_enabled", True)),
            "launch_control_migrated_v1": True,
            "launch_readiness_timeout_sec": int(self.settings.get("launch_readiness_timeout_sec", 30) or 30),
        }
        for item in defaults:
            name = str(item.get("name", "")).strip()
            legacy_key = LAUNCH_APP_META.get(name, {}).get("legacy_autostart_key")
            if legacy_key:
                batch[str(legacy_key)] = bool(item.get("startup", False))
        if hasattr(self.settings, "set_many"):
            self.settings.set_many(batch, save=True)  # type: ignore[attr-defined]
        else:
            for key, val in batch.items():
                self.settings.set(key, val)

    def _build_queue(self, items: List[Dict[str, Any]], startup_only: bool) -> List[str]:
        queue: List[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if name not in LAUNCH_APP_META:
                continue
            if not bool(item.get("enabled", False)):
                continue
            if startup_only and not bool(item.get("startup", False)):
                continue
            if not self.is_configured(name):
                continue
            queue.append(name)
        return queue

    def _start_sequence(self, trigger: str, queue: List[str]) -> bool:
        self._active = True
        self._cancel_requested = False
        self._trigger = trigger
        self._queue = queue
        self._index = 0
        self._results = []
        self._current_name = None
        self._current_cmd = None
        self._current_started_monotonic = 0.0
        try:
            self._wait_timeout_sec = int(self.settings.get("launch_readiness_timeout_sec", 30) or 30)
        except Exception:
            self._wait_timeout_sec = 30
        self.sequence_started.emit({"trigger": trigger, "queue": list(queue)})
        QTimer.singleShot(0, self._advance_queue)
        return True

    def _advance_queue(self) -> None:
        if not self._active:
            return
        if self._cancel_requested:
            self._finish_sequence(cancelled=True)
            return
        if self._index >= len(self._queue):
            self._finish_sequence(cancelled=False)
            return
        name = self._queue[self._index]
        self._index += 1
        if self._program_running(name):
            result = {"name": name, "status": "already_running", "detail": "already running"}
            self._results.append(result)
            self.sequence_progress.emit(result)
            QTimer.singleShot(0, self._advance_queue)
            return
        cmd, cmd_desc = self._resolve_launch_command(name)
        if not cmd:
            result = {"name": name, "status": "failed", "detail": "no launch command"}
            self._results.append(result)
            self.sequence_progress.emit(result)
            QTimer.singleShot(0, self._advance_queue)
            return
        if self._is_self_launch_command(cmd):
            result = {"name": name, "status": "blocked_self", "detail": "blocked self-launch target"}
            self._results.append(result)
            self.sequence_progress.emit(result)
            log.warning("LaunchOrchestrator: blocked self-launch target for %s via %r", name, cmd)
            QTimer.singleShot(0, self._advance_queue)
            return
        try:
            creationflags = 0
            if platform.system() == "Windows":
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
            cwd = self._infer_launch_cwd(cmd, cmd_desc)
            subprocess.Popen(cmd, shell=False, creationflags=creationflags, cwd=cwd)
            if cwd:
                log.info("LaunchOrchestrator: launched %s via %s (cwd=%s)", name, cmd_desc, cwd)
            else:
                log.info("LaunchOrchestrator: launched %s via %s", name, cmd_desc)
            self._current_name = name
            self._current_cmd = cmd
            self._current_started_monotonic = time.monotonic()
            self._poll_timer.start()
        except Exception as e:
            log.error("LaunchOrchestrator: failed launching %s via %s: %s", name, cmd_desc, e)
            result = {"name": name, "status": "failed", "detail": str(e)}
            self._results.append(result)
            self.sequence_progress.emit(result)
            QTimer.singleShot(0, self._advance_queue)

    def _poll_current_readiness(self) -> None:
        if not self._active:
            self._poll_timer.stop()
            return
        if self._cancel_requested:
            self._poll_timer.stop()
            self._finish_sequence(cancelled=True)
            return
        name = self._current_name
        if not name:
            self._poll_timer.stop()
            QTimer.singleShot(0, self._advance_queue)
            return
        elapsed = max(0.0, time.monotonic() - self._current_started_monotonic)
        if self._program_running(name):
            self._poll_timer.stop()
            result = {"name": name, "status": "launched", "detail": f"ready in {elapsed:.1f}s"}
            self._results.append(result)
            self.sequence_progress.emit(result)
            self._current_name = None
            self._current_cmd = None
            QTimer.singleShot(0, self._advance_queue)
            return
        if elapsed >= float(self._wait_timeout_sec):
            self._poll_timer.stop()
            result = {"name": name, "status": "timeout", "detail": f"not ready after {self._wait_timeout_sec}s"}
            self._results.append(result)
            self.sequence_progress.emit(result)
            self._current_name = None
            self._current_cmd = None
            QTimer.singleShot(0, self._advance_queue)

    def _program_running(self, name: str) -> bool:
        try:
            return bool(self.status.program_is_running(name))
        except Exception:
            return False

    def _resolve_launch_command(self, name: str) -> Tuple[Optional[List[str]], str]:
        meta = LAUNCH_APP_META.get(name, {})
        path_key = str(meta.get("path_key", "") or "")
        raw = str(self.settings.get(path_key, "") or "").strip() if path_key else ""
        if raw:
            cmd = self._command_from_config_path(name, raw)
            if cmd:
                return cmd, "configured path"
            cmd = self._command_from_freeform(raw)
            if cmd:
                return cmd, "configured command"
        fallback = self._fallback_cmd(name)
        if fallback:
            return fallback, "fallback command"
        return None, "none"

    def _command_from_config_path(self, name: str, raw: str) -> Optional[List[str]]:
        p = Path(raw)
        if p.exists() and p.is_dir():
            for cand in LAUNCH_APP_META.get(name, {}).get("folder_candidates", []):
                fp = p / str(cand)
                if fp.exists() and fp.is_file():
                    cmd = self._command_for_file(fp)
                    if cmd:
                        return cmd
            return None
        if p.exists() and p.is_file():
            return self._command_for_file(p)
        return None

    def _command_from_freeform(self, raw: str) -> Optional[List[str]]:
        try:
            parts = shlex.split(raw, posix=platform.system() != "Windows")
            if not parts:
                return None
            return [str(p) for p in parts]
        except Exception:
            return None

    def _command_for_file(self, path: Path) -> Optional[List[str]]:
        suffix = path.suffix.lower()
        if suffix == ".desktop" and platform.system() != "Windows":
            if shutil.which("xdg-open"):
                return ["xdg-open", str(path)]
            return [str(path)]
        if suffix == ".py":
            py_cmd = "python" if platform.system() == "Windows" else "python3"
            return [py_cmd, str(path)]
        return [str(path)]

    def _is_self_launch_command(self, cmd: List[str]) -> bool:
        """
        Prevent recursive launch-control self-launch loops.
        """
        parts = [str(p or "").strip() for p in cmd if str(p or "").strip()]
        if not parts:
            return False
        parts_lower = [p.lower() for p in parts]
        joined = " ".join(parts_lower)
        if "freqinout.main" in joined:
            return True
        if any("freqinout.exe" in p for p in parts_lower):
            return True
        if any(p.endswith("freqinout/main.py") or p.endswith("freqinout\\main.py") for p in parts_lower):
            return True

        # Python module/script invocation patterns.
        if len(parts_lower) >= 3 and parts_lower[1] == "-m" and parts_lower[2] == "freqinout.main":
            return True
        if len(parts_lower) >= 2 and (
            parts_lower[1].endswith("freqinout/main.py") or parts_lower[1].endswith("freqinout\\main.py")
        ):
            return True

        # Direct executable equivalence with current process executable/script.
        try:
            current_exec = os.path.basename(sys.executable).strip().lower()
        except Exception:
            current_exec = ""
        try:
            current_argv0 = os.path.basename(sys.argv[0]).strip().lower()
        except Exception:
            current_argv0 = ""
        first_base = os.path.basename(parts_lower[0]).strip().lower()
        if first_base and first_base in {current_exec, current_argv0}:
            for token in parts_lower[1:4]:
                if "freqinout.main" in token:
                    return True
                if token.endswith("freqinout/main.py") or token.endswith("freqinout\\main.py"):
                    return True
        return False

    def _fallback_cmd(self, name: str) -> Optional[List[str]]:
        for cand in LAUNCH_APP_META.get(name, {}).get("fallback_cmds", []):
            cand_s = str(cand).strip()
            if not cand_s:
                continue
            return [cand_s]
        return None

    def _infer_launch_cwd(self, cmd: List[str], cmd_desc: str) -> Optional[str]:
        """
        For configured paths, launch from the app/script directory so relative
        resources resolve the same as direct desktop launch.
        """
        if cmd_desc != "configured path" or not cmd:
            return None
        try:
            first = Path(str(cmd[0])).expanduser()
            if first.exists() and first.is_file():
                return str(first.parent)
            if len(cmd) >= 2:
                second = Path(str(cmd[1])).expanduser()
                if second.exists() and second.is_file():
                    # Covers commands like: python C:\\path\\app.py
                    first_name = os.path.basename(str(cmd[0])).lower()
                    if first_name.startswith("python") or second.suffix.lower() == ".py":
                        return str(second.parent)
        except Exception:
            return None
        return None

    def _finish_sequence(self, cancelled: bool) -> None:
        self._poll_timer.stop()
        if cancelled and self._current_name:
            self._results.append(
                {
                    "name": self._current_name,
                    "status": "cancelled",
                    "detail": "sequence cancelled before readiness check completed",
                }
            )
        summary = self._build_summary(cancelled=cancelled)
        self._active = False
        self._cancel_requested = False
        self._trigger = ""
        self._queue = []
        self._index = 0
        self._current_name = None
        self._current_cmd = None
        self._current_started_monotonic = 0.0
        self.sequence_finished.emit(summary)

    def _build_summary(self, cancelled: bool) -> Dict[str, Any]:
        launched = sum(1 for r in self._results if r.get("status") == "launched")
        already_running = sum(1 for r in self._results if r.get("status") == "already_running")
        failed = sum(1 for r in self._results if r.get("status") == "failed")
        timeout = sum(1 for r in self._results if r.get("status") == "timeout")
        blocked_self = sum(1 for r in self._results if r.get("status") == "blocked_self")
        cancelled_count = sum(1 for r in self._results if r.get("status") == "cancelled")
        return {
            "trigger": self._trigger,
            "cancelled": cancelled,
            "launched": launched,
            "already_running": already_running,
            "failed": failed,
            "timeout": timeout,
            "blocked_self": blocked_self,
            "cancelled_count": cancelled_count,
            "results": list(self._results),
        }
