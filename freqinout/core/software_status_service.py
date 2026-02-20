from __future__ import annotations

import os
import shlex
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

    # Shared caches across all service instances to avoid duplicated polling work.
    _shared_proc_snapshot: List[str] = []
    _shared_proc_snapshot_ts: float = 0.0
    _shared_js8_api_cache: Dict[tuple[str, int, bool], tuple[float, bool]] = {}

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self._snapshot_ttl_sec: float = 2.0
        self._js8_api_cache_key: tuple[str, int, bool] | None = None
        self._js8_api_cache_ok: bool = False
        self._js8_api_cache_ts: float = 0.0
        self._js8_api_cache_ttl_sec: float = 4.0

    def _refresh_process_snapshot(self, *, force: bool = False) -> None:
        cls = type(self)
        now = time.monotonic()
        if not force and (now - float(cls._shared_proc_snapshot_ts or 0.0)) < self._snapshot_ttl_sec:
            self._proc_snapshot = list(cls._shared_proc_snapshot)
            self._proc_snapshot_ts = float(cls._shared_proc_snapshot_ts or now)
            return
        snap: List[str] = []
        for proc in psutil.process_iter(attrs=["name", "exe", "cmdline"]):
            try:
                name = (proc.info.get("name") or "").strip().lower()
                exe = os.path.basename(proc.info.get("exe") or "").strip().lower()
                cmdline = proc.info.get("cmdline") or []
                cmd_tokens: List[str] = []
                for arg in cmdline[:6]:
                    try:
                        token = os.path.basename(str(arg or "")).strip().lower()
                    except Exception:
                        token = ""
                    if token:
                        cmd_tokens.append(token)
                for token in (name, exe, *cmd_tokens):
                    if token:
                        snap.append(token)
            except Exception:
                continue
        cls._shared_proc_snapshot = list(snap)
        cls._shared_proc_snapshot_ts = now
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
        out: List[str] = []
        try:
            p = Path(path_txt)
            name = p.name.strip().lower()
            if name:
                out.append(name)
        except Exception:
            pass
        try:
            parts = shlex.split(path_txt, posix=os.name != "nt")
        except Exception:
            parts = []
        for part in parts[:6]:
            try:
                token = os.path.basename(str(part or "")).strip().lower()
            except Exception:
                token = ""
            if token:
                out.append(token)
        return list(dict.fromkeys(out))

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

    def js8_api_reachable(
        self,
        *,
        port_override: Optional[int] = None,
        allow_fallback: bool = True,
        force: bool = False,
    ) -> bool:
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
        cache_host = hosts[0].strip().lower() if hosts else ""
        cache_key = (cache_host, int(port), bool(allow_fallback))
        now = time.monotonic()
        cls = type(self)
        cache_entry = cls._shared_js8_api_cache.get(cache_key)
        if not force and cache_entry:
            cached_ts, cached_ok = cache_entry
            if (now - float(cached_ts or 0.0)) < float(self._js8_api_cache_ttl_sec):
                self._js8_api_cache_key = cache_key
                self._js8_api_cache_ok = bool(cached_ok)
                self._js8_api_cache_ts = float(cached_ts or now)
                return bool(cached_ok)

        hosts.extend(["127.0.0.1", "localhost", "::1"])
        reachable = False
        for host in hosts:
            try:
                with socket.create_connection((host, port), timeout=0.35):
                    reachable = True
                    break
            except Exception:
                continue

        if not reachable and allow_fallback:
            # Optional fallback probe through existing control client.
            try:
                from freqinout.radio_interface.js8_status import JS8ControlClient

                client = JS8ControlClient()
                reachable = client.get_frequency() is not None
            except Exception:
                reachable = False

        self._js8_api_cache_key = cache_key
        self._js8_api_cache_ok = bool(reachable)
        self._js8_api_cache_ts = time.monotonic()
        cls._shared_js8_api_cache[cache_key] = (self._js8_api_cache_ts, self._js8_api_cache_ok)
        return bool(reachable)

    def status_snapshot(self, *, port_override: Optional[int] = None) -> Dict[str, Dict[str, object]]:
        running_js8 = self.program_is_running("JS8Call")
        api_ok = self.js8_api_reachable(
            port_override=port_override,
            allow_fallback=False,
        ) if running_js8 else False

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
