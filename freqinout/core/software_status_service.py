from __future__ import annotations

import os
import shlex
import socket
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import psutil


PROGRAM_TOKENS: Dict[str, Sequence[str]] = {
    "FLRig": ("flrig", "flrig.exe"),
    "RigCtlD": ("rigctld", "rigctld.exe"),
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
    "RigCtlD",
    "FLDigi",
    "FLMsg",
    "FLAmp",
    "VarAC",
    "JS8Spotter",
    "CommStat",
)

JS8_DEFAULT_HOST = "127.0.0.1"
JS8_DEFAULT_PORT = 2442
FLRIG_DEFAULT_HOST = "127.0.0.1"
FLRIG_DEFAULT_PORT = 12345
RIGCTLD_DEFAULT_HOST = "127.0.0.1"
RIGCTLD_DEFAULT_PORT = 4532
FLDIGI_DEFAULT_HOST = "127.0.0.1"
FLDIGI_DEFAULT_PORT = 7362


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
    _shared_service_probe_cache: Dict[tuple[str, str, int], tuple[float, bool]] = {}

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self._snapshot_ttl_sec: float = 2.0
        self._js8_api_cache_key: tuple[str, int, bool] | None = None
        self._js8_api_cache_ok: bool = False
        self._js8_api_cache_ts: float = 0.0
        self._js8_api_cache_ttl_sec: float = 4.0
        self._service_probe_ttl_sec: float = 4.0

    def _settings_text(self, key: str, default: str = "") -> str:
        try:
            return str(self.settings.get(key, default) or "").strip()
        except Exception:
            return str(default or "").strip()

    def _settings_int(self, key: str, default: int) -> int:
        try:
            value = self.settings.get(key, default)
            return int(value if value not in (None, "") else default)
        except Exception:
            return int(default)

    def _resolved_fldigi_host(self, host_override: Optional[str] = None) -> str:
        override = str(host_override or "").strip()
        if override:
            return override
        host = self._settings_text("fldigi_host", "")
        if host:
            return host
        host = self._settings_text("flrig_host", FLDIGI_DEFAULT_HOST)
        return host or FLDIGI_DEFAULT_HOST

    @staticmethod
    def _format_endpoint(host: str, port: int) -> str:
        return f"{host}:{int(port)}"

    @staticmethod
    def _is_loopback_host(host: str) -> bool:
        host_norm = str(host or "").strip().lower()
        return host_norm in {"", "127.0.0.1", "localhost", "::1", "[::1]"}

    def _cached_service_probe(
        self,
        service_name: str,
        host: str,
        port: int,
        *,
        force: bool = False,
        probe: Callable[[], bool],
    ) -> bool:
        cls = type(self)
        cache_key = (service_name.strip().upper(), str(host or "").strip().lower(), int(port))
        now = time.monotonic()
        cached = cls._shared_service_probe_cache.get(cache_key)
        if not force and cached:
            cached_ts, cached_ok = cached
            if (now - float(cached_ts or 0.0)) < float(self._service_probe_ttl_sec):
                return bool(cached_ok)
        try:
            ok = bool(probe())
        except Exception:
            ok = False
        cls._shared_service_probe_cache[cache_key] = (now, ok)
        return ok

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
        host_override: Optional[str] = None,
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
        host_cfg = (host_override or "").strip()
        if not host_cfg:
            try:
                host_cfg = (self.settings.get("js8_host", "") or "").strip()
            except Exception:
                host_cfg = ""
        if host_cfg:
            hosts.append(host_cfg)
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

        primary_host = hosts[0] if hosts else None
        if not hosts:
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

                client = JS8ControlClient(host=primary_host, port=port, settings=self.settings)
                reachable = client.get_frequency() is not None
            except Exception:
                reachable = False

        self._js8_api_cache_key = cache_key
        self._js8_api_cache_ok = bool(reachable)
        self._js8_api_cache_ts = time.monotonic()
        cls._shared_js8_api_cache[cache_key] = (self._js8_api_cache_ts, self._js8_api_cache_ok)
        return bool(reachable)

    def flrig_api_reachable(
        self,
        *,
        port_override: Optional[int] = None,
        host_override: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        host = (host_override or "").strip() or self._settings_text("flrig_host", FLRIG_DEFAULT_HOST) or FLRIG_DEFAULT_HOST
        port = int(port_override) if port_override is not None else self._settings_int("flrig_port", FLRIG_DEFAULT_PORT)

        def _probe() -> bool:
            from freqinout.radio_interface.rigctl_client import FLRigClient

            client = FLRigClient(host=host, port=port, timeout=0.35)
            return bool(client.is_available())

        return self._cached_service_probe("FLRIG", host, port, force=force, probe=_probe)

    def fldigi_api_reachable(
        self,
        *,
        port_override: Optional[int] = None,
        host_override: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        host = self._resolved_fldigi_host(host_override)
        port = int(port_override) if port_override is not None else self._settings_int("fldigi_port", FLDIGI_DEFAULT_PORT)
        flrig_host = self._settings_text("flrig_host", FLRIG_DEFAULT_HOST) or FLRIG_DEFAULT_HOST
        flrig_port = self._settings_int("flrig_port", FLRIG_DEFAULT_PORT)

        def _probe() -> bool:
            from freqinout.radio_interface.rigctl_client import FLRigClient

            client = FLRigClient(
                host=flrig_host,
                port=flrig_port,
                fldigi_host=host,
                fldigi_port=port,
                timeout=0.35,
            )
            return bool(client.is_fldigi_available())

        return self._cached_service_probe("FLDIGI", host, port, force=force, probe=_probe)

    def rigctld_api_reachable(
        self,
        *,
        port_override: Optional[int] = None,
        host_override: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        host = (host_override or "").strip() or self._settings_text("rig_host", RIGCTLD_DEFAULT_HOST) or RIGCTLD_DEFAULT_HOST
        port = int(port_override) if port_override is not None else self._settings_int("rig_port", RIGCTLD_DEFAULT_PORT)

        def _probe() -> bool:
            from freqinout.radio_interface.rigctl_client import RigctldClient

            client = RigctldClient(host=host, port=port, timeout=0.35)
            return bool(client.is_available())

        return self._cached_service_probe("RIGCTLD", host, port, force=force, probe=_probe)

    def _endpoint_status(
        self,
        *,
        endpoint_label: str,
        host: str,
        port: int,
        reachable: bool,
        process_running: bool,
    ) -> Dict[str, object]:
        endpoint = self._format_endpoint(host, port)
        if reachable:
            return {
                "state": "ok",
                "tooltip": f"Configured {endpoint_label} reachable at {endpoint}",
                "running": True,
                "reachable": True,
                "endpoint": endpoint,
            }
        if process_running:
            return {
                "state": "warn",
                "tooltip": (
                    f"Process running, configured {endpoint_label} unreachable at {endpoint} "
                    "(possible instance/port mismatch)"
                ),
                "running": True,
                "reachable": False,
                "endpoint": endpoint,
            }
        tooltip = (
            f"Not running at configured {endpoint_label} {endpoint}"
            if self._is_loopback_host(host)
            else f"Configured {endpoint_label} unreachable at {endpoint}"
        )
        return {
            "state": "idle",
            "tooltip": tooltip,
            "running": False,
            "reachable": False,
            "endpoint": endpoint,
        }

    def status_snapshot(
        self,
        *,
        force: bool = False,
        port_override: Optional[int] = None,
        host_override: Optional[str] = None,
        flrig_port_override: Optional[int] = None,
        flrig_host_override: Optional[str] = None,
        rigctld_port_override: Optional[int] = None,
        rigctld_host_override: Optional[str] = None,
        fldigi_port_override: Optional[int] = None,
        fldigi_host_override: Optional[str] = None,
    ) -> Dict[str, Dict[str, object]]:
        running_js8 = self.program_is_running("JS8Call")
        js8_host = (host_override or "").strip() or self._settings_text("js8_host", JS8_DEFAULT_HOST) or JS8_DEFAULT_HOST
        js8_port = int(port_override) if port_override is not None else self._settings_int("js8_port", JS8_DEFAULT_PORT)
        js8_api_ok = self.js8_api_reachable(
            port_override=port_override,
            host_override=host_override,
            allow_fallback=False,
            force=force,
        )
        running_flrig = self.program_is_running("FLRig")
        flrig_host = (flrig_host_override or "").strip() or self._settings_text("flrig_host", FLRIG_DEFAULT_HOST) or FLRIG_DEFAULT_HOST
        flrig_port = (
            int(flrig_port_override)
            if flrig_port_override is not None
            else self._settings_int("flrig_port", FLRIG_DEFAULT_PORT)
        )
        flrig_api_ok = self.flrig_api_reachable(
            port_override=flrig_port_override,
            host_override=flrig_host_override,
            force=force,
        )
        active_control_via = self._settings_text("control_via", "FLRig").strip().upper()
        rigctld_active = active_control_via == "RIGCTLD" or rigctld_host_override is not None or rigctld_port_override is not None
        running_rigctld = self.program_is_running("RigCtlD")
        rigctld_host = (
            (rigctld_host_override or "").strip()
            or self._settings_text("rig_host", RIGCTLD_DEFAULT_HOST)
            or RIGCTLD_DEFAULT_HOST
        )
        rigctld_port = (
            int(rigctld_port_override)
            if rigctld_port_override is not None
            else self._settings_int("rig_port", RIGCTLD_DEFAULT_PORT)
        )
        rigctld_api_ok = (
            self.rigctld_api_reachable(
                port_override=rigctld_port_override,
                host_override=rigctld_host_override,
                force=force,
            )
            if rigctld_active
            else False
        )
        running_fldigi = self.program_is_running("FLDigi")
        fldigi_host = self._resolved_fldigi_host(fldigi_host_override)
        fldigi_port = (
            int(fldigi_port_override)
            if fldigi_port_override is not None
            else self._settings_int("fldigi_port", FLDIGI_DEFAULT_PORT)
        )
        fldigi_api_ok = self.fldigi_api_reachable(
            port_override=fldigi_port_override,
            host_override=fldigi_host_override,
            force=force,
        )

        out: Dict[str, Dict[str, object]] = {}
        js8_info = self._endpoint_status(
            endpoint_label="TCP API",
            host=js8_host,
            port=js8_port,
            reachable=js8_api_ok,
            process_running=running_js8,
        )
        out["JS8Call_API"] = dict(js8_info)
        out["JS8Call"] = dict(js8_info)

        for key in STATUS_KEYS:
            if key == "JS8Call_API":
                continue
            if key == "FLRig":
                out[key] = self._endpoint_status(
                    endpoint_label="XML-RPC",
                    host=flrig_host,
                    port=flrig_port,
                    reachable=flrig_api_ok,
                    process_running=running_flrig,
                )
                continue
            if key == "RigCtlD":
                if not rigctld_active:
                    out[key] = {
                        "state": "idle",
                        "tooltip": "Inactive backend",
                        "running": False,
                        "reachable": False,
                    }
                else:
                    out[key] = self._endpoint_status(
                        endpoint_label="TCP",
                        host=rigctld_host,
                        port=rigctld_port,
                        reachable=rigctld_api_ok,
                        process_running=running_rigctld,
                    )
                continue
            if key == "FLDigi":
                out[key] = self._endpoint_status(
                    endpoint_label="XML-RPC",
                    host=fldigi_host,
                    port=fldigi_port,
                    reachable=fldigi_api_ok,
                    process_running=running_fldigi,
                )
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
