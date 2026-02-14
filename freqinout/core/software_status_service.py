from __future__ import annotations

import os
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import psutil


PROGRAM_TOKENS: Dict[str, Sequence[str]] = {
    "FLRig": ("flrig", "flrig.exe"),
    "FLDigi": ("fldigi", "fldigi.exe"),
    "FLMsg": ("flmsg", "flmsg.exe"),
    "FLAmp": ("flamp", "flamp.exe"),
    "VarAC": ("varac", "varac.exe"),
    "JS8Call": ("js8call", "js8call.exe"),
    "JS8Spotter": ("js8spotter", "js8spotter.exe", "js8spotter.py"),
    "CommStat": ("commstat", "commstat.exe", "commstat.py"),
}

PROGRAM_PATH_KEYS: Dict[str, str] = {
    "FLRig": "path_flrig",
    "FLDigi": "path_fldigi",
    "FLMsg": "path_flmsg",
    "FLAmp": "path_flamp",
    "JS8Call": "path_js8call",
    "JS8Spotter": "path_js8spotter",
    "CommStat": "path_commstat",
}

STATUS_KEYS: Sequence[str] = (
    "JS8Call_API",
    "FLRig",
    "FLDigi",
    "FLMsg",
    "FLAmp",
    "VarAC",
    "JS8Spotter",
    "CommStat",
)


class SoftwareStatusService:
    """
    Shared process and API status probe used by UI surfaces.

    This class is intentionally side-effect free aside from local process/socket
    inspection so tabs can refresh status consistently.
    """

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self._snapshot_ttl_sec: float = 2.0

    def _refresh_process_snapshot(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._proc_snapshot_ts) < self._snapshot_ttl_sec:
            return
        snap: List[str] = []
        for proc in psutil.process_iter(attrs=["name", "exe", "cmdline"]):
            try:
                name = (proc.info.get("name") or "").strip().lower()
                exe = os.path.basename(proc.info.get("exe") or "").strip().lower()
                cmdline = proc.info.get("cmdline") or []
                first_arg = os.path.basename(cmdline[0]).strip().lower() if cmdline else ""
                second_arg = os.path.basename(cmdline[1]).strip().lower() if len(cmdline) > 1 else ""
                for token in (name, exe, first_arg, second_arg):
                    if token:
                        snap.append(token)
            except Exception:
                continue
        self._proc_snapshot = snap
        self._proc_snapshot_ts = now

    def _configured_tokens(self, program_name: str) -> List[str]:
        key = PROGRAM_PATH_KEYS.get(program_name)
        if not key:
            return []
        try:
            path_txt = (self.settings.get(key, "") or "").strip()
        except Exception:
            path_txt = ""
        if not path_txt:
            return []
        try:
            p = Path(path_txt)
            name = p.name.strip().lower()
            return [name] if name else []
        except Exception:
            return []

    def _target_tokens(self, program_name: str) -> List[str]:
        defaults = [t.lower() for t in PROGRAM_TOKENS.get(program_name, ())]
        return list(dict.fromkeys(defaults + self._configured_tokens(program_name)))

    def program_is_running(self, program_name: str) -> bool:
        self._refresh_process_snapshot()
        targets = set(self._target_tokens(program_name))
        if not targets:
            targets = {program_name.strip().lower(), f"{program_name.strip().lower()}.exe"}
        return any(token in targets for token in self._proc_snapshot)

    def find_process_exe(self, program_name: str) -> Optional[str]:
        targets = set(self._target_tokens(program_name))
        if not targets:
            targets = {program_name.strip().lower(), f"{program_name.strip().lower()}.exe"}
        for proc in psutil.process_iter(attrs=["name", "exe", "cmdline"]):
            try:
                name = (proc.info.get("name") or "").strip().lower()
                exe = (proc.info.get("exe") or "").strip()
                exe_base = os.path.basename(exe).strip().lower()
                cmdline = proc.info.get("cmdline") or []
                first_arg = os.path.basename(cmdline[0]).strip().lower() if cmdline else ""
                second_arg = os.path.basename(cmdline[1]).strip().lower() if len(cmdline) > 1 else ""
                if any(token in targets for token in (name, exe_base, first_arg, second_arg)):
                    if exe:
                        return exe
                    if cmdline:
                        return cmdline[0]
                    return None
            except Exception:
                continue
        return None

    def js8_api_reachable(self, *, port_override: Optional[int] = None) -> bool:
        if port_override is not None:
            port = int(port_override)
        else:
            try:
                port = int(self.settings.get("js8_port", 2442) or 2442)
            except Exception:
                port = 2442

        hosts: List[str] = []
        try:
            host_cfg = (self.settings.get("js8_host", "") or "").strip()
            if host_cfg:
                hosts.append(host_cfg)
        except Exception:
            pass
        hosts.extend(["127.0.0.1", "localhost", "::1"])

        for host in hosts:
            try:
                with socket.create_connection((host, port), timeout=1.5):
                    return True
            except Exception:
                continue

        # Fallback probe through existing control client.
        try:
            from freqinout.radio_interface.js8_status import JS8ControlClient

            client = JS8ControlClient()
            return client.get_frequency() is not None
        except Exception:
            return False

    def status_snapshot(self, *, port_override: Optional[int] = None) -> Dict[str, Dict[str, object]]:
        running_js8 = self.program_is_running("JS8Call")
        api_ok = self.js8_api_reachable(port_override=port_override)

        out: Dict[str, Dict[str, object]] = {}
        out["JS8Call_API"] = {
            "state": "ok" if api_ok else "warn" if running_js8 else "idle",
            "tooltip": "API reachable" if api_ok else "Process running, API unreachable" if running_js8 else "Not running",
            "running": bool(running_js8),
        }

        for key in STATUS_KEYS:
            if key == "JS8Call_API":
                continue
            if key == "JS8Call":
                out[key] = {
                    "state": "ok" if api_ok else "warn" if running_js8 else "idle",
                    "tooltip": "API reachable" if api_ok else "Process running, API unreachable" if running_js8 else "Not running",
                    "running": bool(running_js8),
                }
                continue
            running = self.program_is_running(key)
            tooltip = "Running" if running else "Not running"
            if key == "VarAC" and running:
                exe = self.find_process_exe("VarAC")
                if exe:
                    tooltip = f"Running: {exe}"
            out[key] = {
                "state": "ok" if running else "idle",
                "tooltip": tooltip,
                "running": bool(running),
            }
        return out

