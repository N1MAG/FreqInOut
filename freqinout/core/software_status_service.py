from __future__ import annotations

import os
import shlex
import socket
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import psutil

from freqinout.core.dependency_health import get_dependency_health_registry
from freqinout.core.logger import log
from freqinout.radio_interface.js8_api_client import JS8ApiClient, JS8ApiEndpoint


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
    _shared_proc_records: List[Dict[str, object]] = []
    _shared_proc_snapshot_ts: float = 0.0
    _shared_proc_lock = threading.Lock()
    _shared_js8_api_cache: Dict[tuple[str, int, bool], tuple[float, bool]] = {}
    _shared_js8_capability_cache: Dict[tuple[str, int], tuple[float, Dict[str, object]]] = {}
    _shared_service_probe_cache: Dict[tuple[str, ...], tuple[float, bool]] = {}

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self._proc_snapshot: List[str] = []
        self._proc_records: List[Dict[str, object]] = []
        self._proc_snapshot_ts: float = 0.0
        self._snapshot_ttl_sec: float = 5.0
        self._js8_api_cache_key: tuple[str, int, bool] | None = None
        self._js8_api_cache_ok: bool = False
        self._js8_api_cache_ts: float = 0.0
        self._api_success_ttl_sec: float = 15.0
        self._api_failure_ttl_sec: float = 30.0
        self._js8_capability_success_ttl_sec: float = 60.0
        self._js8_capability_failure_ttl_sec: float = 120.0
        self._service_probe_success_ttl_sec: float = 15.0
        self._service_probe_failure_ttl_sec: float = 30.0
        self._health = get_dependency_health_registry()

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
        cache_key: tuple[str, ...],
        *,
        force: bool = False,
        probe: Callable[[], bool],
    ) -> bool:
        cls = type(self)
        now = time.monotonic()
        cached = cls._shared_service_probe_cache.get(cache_key)
        if not force and cached:
            cached_ts, cached_ok = cached
            ttl = self._service_probe_success_ttl_sec if cached_ok else self._service_probe_failure_ttl_sec
            if (now - float(cached_ts or 0.0)) < float(ttl):
                return bool(cached_ok)
        health_key = self._health_key(cache_key)
        allowed, _health = self._health.may_run(health_key, owner="SoftwareStatusService", force=force)
        if not allowed and cached:
            return bool(cached[1])
        if not allowed:
            return False
        started = time.monotonic()
        try:
            ok = bool(probe())
            elapsed_ms = (time.monotonic() - started) * 1000.0
        except Exception as exc:
            ok = False
            elapsed_ms = (time.monotonic() - started) * 1000.0
            self._health.record_failure(
                health_key,
                owner="SoftwareStatusService",
                error=str(exc or "probe failed"),
                duration_ms=elapsed_ms,
            )
        else:
            if ok:
                self._health.record_success(health_key, owner="SoftwareStatusService", duration_ms=elapsed_ms)
            else:
                self._health.record_failure(
                    health_key,
                    owner="SoftwareStatusService",
                    error="unreachable",
                    duration_ms=elapsed_ms,
                )
        cls._shared_service_probe_cache[cache_key] = (now, ok)
        return ok

    @staticmethod
    def _health_key(cache_key: tuple[object, ...]) -> str:
        parts = [str(part or "").strip().lower() for part in cache_key]
        return ":".join(part for part in parts if part) or "software-status"

    def _health_snapshot_for(self, cache_key: tuple[object, ...]) -> Dict[str, object]:
        return self._health.snapshot(self._health_key(cache_key))

    @staticmethod
    def _basename_token(value: object) -> str:
        try:
            text = str(value or "").strip().replace("\\", "/")
            return text.rsplit("/", 1)[-1].strip().lower()
        except Exception:
            return ""

    def _refresh_process_snapshot(self, *, force: bool = False) -> None:
        cls = type(self)
        with cls._shared_proc_lock:
            now = time.monotonic()
            if not force and (now - float(cls._shared_proc_snapshot_ts or 0.0)) < self._snapshot_ttl_sec:
                self._proc_snapshot = cls._shared_proc_snapshot
                self._proc_records = cls._shared_proc_records
                self._proc_snapshot_ts = float(cls._shared_proc_snapshot_ts or now)
                return
            started = time.perf_counter()
            snap: List[str] = []
            records: List[Dict[str, object]] = []
            for proc in psutil.process_iter(attrs=["name", "exe", "cmdline"]):
                try:
                    name = (proc.info.get("name") or "").strip().lower()
                    exe_path = (proc.info.get("exe") or "").strip()
                    exe = self._basename_token(exe_path)
                    cmdline = proc.info.get("cmdline") or []
                    cmd_paths: List[str] = []
                    cmd_tokens: List[str] = []
                    for arg in cmdline[:6]:
                        try:
                            path = str(arg or "").strip()
                            token = self._basename_token(path)
                        except Exception:
                            path = ""
                            token = ""
                        if token:
                            cmd_paths.append(path)
                            cmd_tokens.append(token)
                    for token in (name, exe, *cmd_tokens):
                        if token:
                            snap.append(token)
                    records.append(
                        {
                            "name": name,
                            "exe": exe,
                            "exe_path": exe_path,
                            "cmd_tokens": tuple(cmd_tokens),
                            "cmd_paths": tuple(cmd_paths),
                        }
                    )
                except Exception:
                    continue
            cls._shared_proc_snapshot = snap
            cls._shared_proc_records = records
            cls._shared_proc_snapshot_ts = now
            self._proc_snapshot = cls._shared_proc_snapshot
            self._proc_records = cls._shared_proc_records
            self._proc_snapshot_ts = now
        log.debug(
            "PROCESS_INVENTORY|refreshed|processes=%s|tokens=%s|duration_ms=%.1f",
            len(records),
            len(snap),
            (time.perf_counter() - started) * 1000.0,
        )

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
            name = self._basename_token(p.name)
            if name:
                out.append(name)
        except Exception:
            pass
        try:
            parts = shlex.split(path_txt, posix=os.name != "nt")
        except Exception:
            parts = []
        for part in parts[:6]:
            token = self._basename_token(part)
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
        self._refresh_process_snapshot()
        targets = set(self._target_tokens(program_name))
        if not targets:
            targets = {program_name.strip().lower(), f"{program_name.strip().lower()}.exe"}
        for record in self._proc_records:
            try:
                cmd_tokens = tuple(str(token or "") for token in record.get("cmd_tokens", ()))
                cmd_paths = tuple(str(path or "") for path in record.get("cmd_paths", ()))
                for token, path in zip(cmd_tokens, cmd_paths):
                    if token in targets and path:
                        return path
                process_tokens = {str(record.get("name") or ""), str(record.get("exe") or "")}
                if not process_tokens.intersection(targets):
                    continue
                exe_path = str(record.get("exe_path") or "").strip()
                if exe_path:
                    return exe_path
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
            ttl = self._api_success_ttl_sec if cached_ok else self._api_failure_ttl_sec
            if (now - float(cached_ts or 0.0)) < float(ttl):
                self._js8_api_cache_key = cache_key
                self._js8_api_cache_ok = bool(cached_ok)
                self._js8_api_cache_ts = float(cached_ts or now)
                return bool(cached_ok)
        health_key = self._health_key(("JS8CALL", cache_host or "loopback", int(port), bool(allow_fallback)))
        allowed, _health = self._health.may_run(health_key, owner="SoftwareStatusService", force=force)
        if not allowed and cache_entry:
            return bool(cache_entry[1])
        if not allowed:
            return False

        primary_host = hosts[0] if hosts else None
        if not hosts:
            hosts.extend(["127.0.0.1", "localhost", "::1"])
        started = time.monotonic()
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

                client = JS8ControlClient(host=primary_host)
                reachable = client.get_frequency() is not None
            except Exception:
                reachable = False

        self._js8_api_cache_key = cache_key
        self._js8_api_cache_ok = bool(reachable)
        self._js8_api_cache_ts = time.monotonic()
        cls._shared_js8_api_cache[cache_key] = (self._js8_api_cache_ts, self._js8_api_cache_ok)
        elapsed_ms = (time.monotonic() - started) * 1000.0
        if reachable:
            self._health.record_success(health_key, owner="SoftwareStatusService", duration_ms=elapsed_ms)
        else:
            self._health.record_failure(
                health_key,
                owner="SoftwareStatusService",
                error="unreachable",
                duration_ms=elapsed_ms,
            )
        return bool(reachable)

    def js8_api_capability_status(
        self,
        *,
        port_override: Optional[int] = None,
        host_override: Optional[str] = None,
        process_running: Optional[bool] = None,
        force: bool = False,
    ) -> Dict[str, object]:
        host = (host_override or "").strip() or self._settings_text("js8_host", JS8_DEFAULT_HOST) or JS8_DEFAULT_HOST
        port = int(port_override) if port_override is not None else self._settings_int("js8_port", JS8_DEFAULT_PORT)
        endpoint = JS8ApiEndpoint(host, port).normalized()
        cache_key = endpoint.key
        health_key = self._health_key(("JS8CALL", endpoint.host.lower(), int(endpoint.port), "capability"))
        running = bool(process_running) if process_running is not None else self.program_is_running("JS8Call")
        now = time.monotonic()
        cached = type(self)._shared_js8_capability_cache.get(cache_key)
        if not running and self._is_loopback_host(endpoint.host):
            status = self._js8_capability_offline_status(
                endpoint,
                last_error="JS8Call is not running",
            )
            type(self)._shared_js8_capability_cache[cache_key] = (now, dict(status))
            return status
        if not force and cached:
            cached_ts, cached_status = cached
            connected = bool(cached_status.get("connected"))
            ttl = self._js8_capability_success_ttl_sec if connected else self._js8_capability_failure_ttl_sec
            if (now - float(cached_ts or 0.0)) < ttl:
                return dict(cached_status)
        allowed, _health = self._health.may_run(health_key, owner="SoftwareStatusService", force=force)
        if not allowed and cached:
            return dict(cached[1])
        if not allowed:
            return self._js8_capability_offline_status(
                endpoint,
                last_error="JS8Call API capability check is waiting for cooldown",
            )
        started = time.monotonic()
        status = self._js8_capability_offline_status(endpoint)
        client = JS8ApiClient(endpoint, timeout_s=0.4, auto_reconnect=False)
        try:
            if client.start():
                snapshot = client.probe_capabilities(timeout_s=0.4)
                status.update(
                    {
                        "connected": bool(snapshot.connected),
                        "mode": str(snapshot.mode or "offline"),
                        "version": str(snapshot.version or ""),
                        "supported": dict(snapshot.supported),
                        "errors": dict(snapshot.errors),
                        "last_error": client.last_error,
                    }
                )
            else:
                status["last_error"] = client.last_error or "JS8Call TCP API not reachable"
        except Exception as exc:
            status["last_error"] = str(exc or "JS8Call capability probe failed")
        finally:
            client.stop()
        elapsed_ms = (time.monotonic() - started) * 1000.0
        metadata = {
            "capability_mode": status.get("mode", "offline"),
            "version": status.get("version", ""),
            "endpoint": status.get("endpoint", ""),
            "action": self._js8_capability_action(status, running=running),
        }
        if bool(status.get("connected")):
            self._health.record_success(
                health_key,
                owner="SoftwareStatusService",
                duration_ms=elapsed_ms,
                metadata=metadata,
            )
        elif running:
            self._health.record_failure(
                health_key,
                owner="SoftwareStatusService",
                error=str(status.get("last_error") or "JS8Call TCP API not reachable"),
                duration_ms=elapsed_ms,
                metadata=metadata,
            )
        type(self)._shared_js8_capability_cache[cache_key] = (time.monotonic(), dict(status))
        return status

    def _js8_capability_offline_status(
        self,
        endpoint: JS8ApiEndpoint,
        *,
        last_error: str = "",
    ) -> Dict[str, object]:
        normalized = endpoint.normalized()
        return {
            "connected": False,
            "mode": "offline",
            "version": "",
            "endpoint": self._format_endpoint(normalized.host, normalized.port),
            "supported": {},
            "errors": {},
            "last_error": str(last_error or ""),
        }

    @staticmethod
    def _js8_capability_action(status: Dict[str, object], *, running: bool) -> str:
        endpoint = str(status.get("endpoint", "") or "").strip()
        version = str(status.get("version", "") or "").strip()
        mode = str(status.get("mode", "offline") or "offline").strip()
        version_part = f" Version: {version}." if version else ""
        if mode == "api_full":
            return f"JS8Call API is ready for native FIO diagnostics at {endpoint}.{version_part}"
        if mode == "api_basic":
            return f"JS8Call API is reachable at {endpoint}; FIO will use basic API features and keep fallbacks available.{version_part}"
        if mode == "file_fallback":
            return f"JS8Call is reachable at {endpoint}, but native API support is limited; FIO will keep using log/database fallbacks.{version_part}"
        if running:
            return f"JS8Call appears to be running, but FIO could not verify the TCP API at {endpoint}."
        return "JS8Call is not running; no JS8 API check is needed right now."

    def flrig_api_reachable(
        self,
        *,
        port_override: Optional[int] = None,
        host_override: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        host = (host_override or "").strip() or self._settings_text("flrig_host", FLRIG_DEFAULT_HOST) or FLRIG_DEFAULT_HOST
        port = int(port_override) if port_override is not None else self._settings_int("flrig_port", FLRIG_DEFAULT_PORT)
        cache_key = ("FLRIG", host.strip().lower(), str(int(port)))

        def _probe() -> bool:
            from freqinout.radio_interface.rigctl_client import FLRigClient

            client = FLRigClient(host=host, port=port, timeout=0.35)
            return bool(client.is_available())

        return self._cached_service_probe(cache_key, force=force, probe=_probe)

    def fldigi_api_reachable(
        self,
        *,
        port_override: Optional[int] = None,
        host_override: Optional[str] = None,
        flrig_port_override: Optional[int] = None,
        flrig_host_override: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        host = self._resolved_fldigi_host(host_override)
        port = int(port_override) if port_override is not None else self._settings_int("fldigi_port", FLDIGI_DEFAULT_PORT)
        flrig_host = (flrig_host_override or "").strip() or self._settings_text("flrig_host", FLRIG_DEFAULT_HOST) or FLRIG_DEFAULT_HOST
        flrig_port = (
            int(flrig_port_override)
            if flrig_port_override is not None
            else self._settings_int("flrig_port", FLRIG_DEFAULT_PORT)
        )
        cache_key = (
            "FLDIGI",
            host.strip().lower(),
            str(int(port)),
            flrig_host.strip().lower(),
            str(int(flrig_port)),
        )

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

        return self._cached_service_probe(cache_key, force=force, probe=_probe)

    def rigctld_api_reachable(
        self,
        *,
        port_override: Optional[int] = None,
        host_override: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        host = (host_override or "").strip() or self._settings_text("rig_host", RIGCTLD_DEFAULT_HOST) or RIGCTLD_DEFAULT_HOST
        port = int(port_override) if port_override is not None else self._settings_int("rig_port", RIGCTLD_DEFAULT_PORT)
        cache_key = ("RIGCTLD", host.strip().lower(), str(int(port)))

        def _probe() -> bool:
            from freqinout.radio_interface.rigctl_client import RigctldClient

            client = RigctldClient(host=host, port=port, timeout=0.35)
            return bool(client.is_available())

        return self._cached_service_probe(cache_key, force=force, probe=_probe)

    def tcp_endpoint_reachable(
        self,
        *,
        service_name: str,
        host: str,
        port: int,
        force: bool = False,
    ) -> bool:
        host_value = str(host or "").strip()
        port_value = int(port or 0)
        if not host_value or port_value <= 0:
            return False
        cache_key = (str(service_name or "TCP").strip().upper() or "TCP", host_value.lower(), str(port_value))

        def _probe() -> bool:
            with socket.create_connection((host_value, port_value), timeout=0.35):
                return True

        return self._cached_service_probe(cache_key, force=force, probe=_probe)

    def _endpoint_status(
        self,
        *,
        endpoint_label: str,
        host: str,
        port: int,
        reachable: bool,
        process_running: bool,
        health: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        endpoint = self._format_endpoint(host, port)
        degraded = bool((health or {}).get("degraded"))
        cooldown = float((health or {}).get("cooldown_remaining_sec") or 0.0)
        reason = str((health or {}).get("last_error") or "").strip()
        suffix = ""
        if degraded and cooldown > 0:
            suffix = f" FIO is backing off for {cooldown:.0f}s to keep the UI responsive."
        elif degraded and reason:
            suffix = f" Last issue: {reason}."
        if reachable:
            return {
                "state": "ok",
                "tooltip": f"Configured {endpoint_label} reachable at {endpoint}{suffix}",
                "running": True,
                "reachable": True,
                "endpoint": endpoint,
                "degraded": degraded,
                "stale": bool(cooldown > 0),
                "health": health or {},
            }
        if process_running:
            return {
                "state": "warn",
                "tooltip": (
                    f"Process running, configured {endpoint_label} unreachable at {endpoint} "
                    f"(possible instance/port mismatch).{suffix}"
                ),
                "running": True,
                "reachable": False,
                "endpoint": endpoint,
                "degraded": degraded,
                "stale": bool(cooldown > 0),
                "health": health or {},
            }
        tooltip = (
            f"Not running at configured {endpoint_label} {endpoint}"
            if self._is_loopback_host(host)
            else f"Configured {endpoint_label} unreachable at {endpoint}"
        )
        if suffix:
            tooltip = f"{tooltip}.{suffix}"
        return {
            "state": "idle",
            "tooltip": tooltip,
            "running": False,
            "reachable": False,
            "endpoint": endpoint,
            "degraded": degraded,
            "stale": bool(cooldown > 0),
            "health": health or {},
        }

    def generic_endpoint_status(
        self,
        *,
        service_name: str,
        endpoint_label: str,
        host: str,
        port: int,
        force: bool = False,
    ) -> Dict[str, object]:
        host_value = str(host or "").strip()
        port_value = int(port or 0)
        if not host_value or port_value <= 0:
            return {
                "state": "idle",
                "tooltip": f"{endpoint_label} not configured",
                "running": False,
                "reachable": False,
                "endpoint": "",
            }
        reachable = self.tcp_endpoint_reachable(
            service_name=service_name,
            host=host_value,
            port=port_value,
            force=force,
        )
        return self._endpoint_status(
            endpoint_label=endpoint_label,
            host=host_value,
            port=port_value,
            reachable=reachable,
            process_running=False,
            health=self._health_snapshot_for(
                (str(service_name or "TCP").strip().upper() or "TCP", host_value.lower(), str(port_value))
            ),
        )

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
        if force:
            self._refresh_process_snapshot(force=True)
        running_js8 = self.program_is_running("JS8Call")
        js8_host = (host_override or "").strip() or self._settings_text("js8_host", JS8_DEFAULT_HOST) or JS8_DEFAULT_HOST
        js8_port = int(port_override) if port_override is not None else self._settings_int("js8_port", JS8_DEFAULT_PORT)
        js8_cache_host = str(js8_host or "").strip().lower()
        js8_health = self._health.snapshot(
            self._health_key(("JS8CALL", js8_cache_host or "loopback", int(js8_port), False))
        )
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
        flrig_key = ("FLRIG", flrig_host.strip().lower(), str(int(flrig_port)))
        flrig_api_ok = self.flrig_api_reachable(
            port_override=flrig_port_override,
            host_override=flrig_host_override,
            force=force,
        )
        active_control_via = self._settings_text("control_via", "FLRig").strip().upper()
        rigctld_active = active_control_via == "RIGCTLD" or rigctld_host_override is not None or rigctld_port_override is not None
        running_rigctld = self.program_is_running("RigCtlD") if rigctld_active else False
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
        rigctld_key = ("RIGCTLD", rigctld_host.strip().lower(), str(int(rigctld_port)))
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
        fldigi_key = (
            "FLDIGI",
            fldigi_host.strip().lower(),
            str(int(fldigi_port)),
            flrig_host.strip().lower(),
            str(int(flrig_port)),
        )
        fldigi_api_ok = self.fldigi_api_reachable(
            port_override=fldigi_port_override,
            host_override=fldigi_host_override,
            flrig_port_override=flrig_port_override,
            flrig_host_override=flrig_host_override,
            force=force,
        )

        out: Dict[str, Dict[str, object]] = {}
        out["JS8Call_API"] = self._endpoint_status(
            endpoint_label="TCP API",
            host=js8_host,
            port=js8_port,
            reachable=js8_api_ok,
            process_running=running_js8,
            health=js8_health,
        )

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
                    health=self._health_snapshot_for(flrig_key),
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
                        health=self._health_snapshot_for(rigctld_key),
                    )
                continue
            if key == "FLDigi":
                out[key] = self._endpoint_status(
                    endpoint_label="XML-RPC",
                    host=fldigi_host,
                    port=fldigi_port,
                    reachable=fldigi_api_ok,
                    process_running=running_fldigi,
                    health=self._health_snapshot_for(fldigi_key),
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
