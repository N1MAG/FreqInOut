from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from freqinout.core.logger import log
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.radio_interface.js8_status import JS8ControlClient, VarACStatusClient
from freqinout.radio_interface.rigctl_client import RigControlClient, rig_control_client_from_settings


CONTROL_STATUS_KEYS: Dict[str, Optional[str]] = {
    "flrig": "FLRig",
    "rigctld": "RigCtlD",
    "js8call": "JS8Call_API",
    "manual": None,
}


def _row_bool(value: object, default: bool) -> bool:
    if value in (None, ""):
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(default)


def _device_endpoint_summary(profile: Mapping[str, Any]) -> str:
    backend = str(profile.get("control_backend", "") or "").strip().lower()
    if backend == "flrig":
        host = str(profile.get("flrig_host", "") or "").strip() or "127.0.0.1"
        port = int(profile.get("flrig_port", 12345) or 12345)
        return f"FLRig {host}:{port}"
    if backend == "js8call":
        host = str(profile.get("js8_host", "") or "").strip() or "127.0.0.1"
        port = int(profile.get("js8_port", 2442) or 2442)
        return f"JS8Call {host}:{port}"
    if backend == "rigctld":
        host = str(profile.get("rig_host", "") or "").strip() or "127.0.0.1"
        port = int(profile.get("rig_port", 4532) or 4532)
        return f"rigctld {host}:{port}"
    host = str(profile.get("flrig_host", "") or "").strip() or str(profile.get("js8_host", "") or "").strip()
    if host:
        return f"Manual via {host}"
    return "Manual / no endpoint"


def _legacy_control_via(control_backend: str) -> str:
    backend = str(control_backend or "manual").strip().lower()
    if backend == "flrig":
        return "FLRig"
    if backend == "js8call":
        return "JS8Call"
    if backend == "rigctld":
        return "RIGCTLD"
    return "Manual"


class DeviceSettingsProxy:
    def __init__(self, profile: Mapping[str, Any], fallback_settings: Optional[object] = None) -> None:
        self.profile = dict(profile)
        self._fallback = fallback_settings
        self._data = self._build_data(self.profile)

    @staticmethod
    def _build_data(profile: Mapping[str, Any]) -> Dict[str, Any]:
        backend = str(profile.get("control_backend", "manual") or "manual").strip().lower() or "manual"
        data: Dict[str, Any] = {
            "control_via": _legacy_control_via(backend),
            "rig_host": str(profile.get("rig_host", "") or "").strip(),
            "rig_port": profile.get("rig_port", 4532) or 4532,
            "flrig_host": str(profile.get("flrig_host", "") or "127.0.0.1").strip() or "127.0.0.1",
            "flrig_port": profile.get("flrig_port", 12345) or 12345,
            "fldigi_host": str(profile.get("fldigi_host", "") or profile.get("flrig_host", "") or "127.0.0.1").strip()
            or "127.0.0.1",
            "fldigi_port": profile.get("fldigi_port", 7362) or 7362,
            "fldigi_log_path": str(profile.get("fldigi_log_path", "") or "").strip(),
            "js8_host": str(profile.get("js8_host", "") or "127.0.0.1").strip() or "127.0.0.1",
            "js8_port": profile.get("js8_port", 2442) or 2442,
            "js8_profile_path": str(profile.get("js8_profile_path", "") or "").strip(),
            "varac_path": str(profile.get("varac_install_path", "") or "").strip(),
            "varac_db_path": str(profile.get("varac_db_path", "") or "").strip(),
            "varac_ini_path": str(profile.get("varac_ini_path", "") or "").strip(),
            "varac_launch_cmd": str(profile.get("launch_cmd", "") or "").strip(),
            "launch_control_enabled": bool(int(profile.get("launch_enabled", 1) or 0)),
        }
        launch_path = str(profile.get("launch_path", "") or "").strip()
        if backend == "flrig":
            data["path_flrig"] = launch_path
        elif backend == "js8call":
            data["path_js8call"] = launch_path
        return data

    def get(self, key: str, default: Any = None) -> Any:
        if key in self._data:
            return self._data.get(key, default)
        fallback_get = getattr(self._fallback, "get", None)
        if callable(fallback_get):
            try:
                return fallback_get(key, default)
            except Exception:
                return default
        return default

    def all(self) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        fallback_all = getattr(self._fallback, "all", None)
        if callable(fallback_all):
            try:
                raw = fallback_all()
                if isinstance(raw, dict):
                    merged.update(raw)
            except Exception:
                pass
        merged.update(self._data)
        return merged


@dataclass
class DeviceRuntimeSnapshot:
    device_profile_id: int
    name: str
    control_backend: str
    deployment_mode: str
    runtime_active: bool
    runtime_primary: bool
    scheduler_owner: bool
    endpoint_summary: str
    assigned_operating_profile_id: Optional[int]
    assigned_operating_profile_name: str
    assignment_state: str
    scheduler_enabled: bool
    scheduler_mode: str
    use_messages: bool
    use_map: bool
    use_background_ingest: bool
    use_launch_control: bool
    use_net_control_tabs: bool
    control_ready: bool
    overall_state: str
    status_summary: str
    warning_text: str
    service_states: Dict[str, Dict[str, object]]


class DeviceRuntime:
    def __init__(
        self,
        profile: Mapping[str, Any],
        *,
        is_primary: bool,
        fallback_settings: Optional[object] = None,
        assignment: Optional[Mapping[str, Any]] = None,
        operating_profile: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.fallback_settings = fallback_settings
        self.profile: Dict[str, Any] = {}
        self.is_primary = False
        self.assignment: Dict[str, Any] = {}
        self.operating_profile: Dict[str, Any] = {}
        self.settings_proxy: DeviceSettingsProxy | None = None
        self.status_service: SoftwareStatusService | None = None
        self.rig_client: Optional[RigControlClient] = None
        self.js8_control_client: Optional[JS8ControlClient] = None
        self.varac_status_client: Optional[VarACStatusClient] = None
        self._config_signature: tuple[object, ...] = tuple()
        self.update(
            profile,
            is_primary=is_primary,
            assignment=assignment,
            operating_profile=operating_profile,
        )

    @staticmethod
    def _signature_for(
        profile: Mapping[str, Any],
        *,
        is_primary: bool,
    ) -> tuple[object, ...]:
        profile_items = tuple(sorted((str(key), profile.get(key)) for key in profile.keys()))
        return (bool(is_primary), profile_items)

    def update(
        self,
        profile: Mapping[str, Any],
        *,
        is_primary: bool,
        assignment: Optional[Mapping[str, Any]] = None,
        operating_profile: Optional[Mapping[str, Any]] = None,
    ) -> None:
        signature = self._signature_for(
            profile,
            is_primary=is_primary,
        )
        if signature == self._config_signature:
            self.profile = dict(profile)
            self.is_primary = bool(is_primary)
            self.assignment = dict(assignment or {})
            self.operating_profile = dict(operating_profile or {})
            return
        self.stop()
        self.profile = dict(profile)
        self.is_primary = bool(is_primary)
        self.assignment = dict(assignment or {})
        self.operating_profile = dict(operating_profile or {})
        self.settings_proxy = DeviceSettingsProxy(self.profile, self.fallback_settings)
        self.status_service = SoftwareStatusService(self.settings_proxy)
        backend = str(self.profile.get("control_backend", "") or "").strip().lower()
        if backend in {"flrig", "rigctld"}:
            try:
                self.rig_client = rig_control_client_from_settings(self.settings_proxy)
            except Exception as exc:
                log.debug("DeviceRuntime: failed building rig client for %s: %s", self.profile.get("name", ""), exc)
                self.rig_client = None
        else:
            self.rig_client = None

        if self.is_primary:
            try:
                host = str(self.settings_proxy.get("js8_host", "127.0.0.1") or "127.0.0.1").strip() or "127.0.0.1"
                port = int(self.settings_proxy.get("js8_port", 2442) or 2442)
                self.js8_control_client = JS8ControlClient(host=host, port=port, settings=self.settings_proxy)
            except Exception as exc:
                log.debug("DeviceRuntime: failed building JS8 client for %s: %s", self.profile.get("name", ""), exc)
                self.js8_control_client = None
            try:
                self.varac_status_client = VarACStatusClient(settings=self.settings_proxy)
            except Exception as exc:
                log.debug("DeviceRuntime: failed building VarAC client for %s: %s", self.profile.get("name", ""), exc)
                self.varac_status_client = None
        self._config_signature = signature

    def stop(self) -> None:
        if self.js8_control_client is not None:
            try:
                self.js8_control_client.stop()
            except Exception:
                pass
        self.rig_client = None
        self.js8_control_client = None
        self.varac_status_client = None

    def primary_signature(self) -> tuple[object, ...]:
        profile = self.profile
        if not profile:
            return tuple()
        return (
            int(profile.get("id", 0) or 0),
            str(profile.get("control_backend", "") or "").strip().lower(),
            str(profile.get("deployment_mode", "") or "").strip().lower(),
            str(profile.get("flrig_host", "") or "").strip(),
            int(profile.get("flrig_port", 12345) or 12345),
            str(profile.get("rig_host", "") or "").strip(),
            int(profile.get("rig_port", 4532) or 4532),
            str(profile.get("js8_host", "") or "").strip(),
            int(profile.get("js8_port", 2442) or 2442),
            bool(self.is_primary),
        )

    def operating_policy(self) -> Dict[str, object]:
        operating = self.operating_profile if isinstance(self.operating_profile, dict) else {}
        assignment = self.assignment if isinstance(self.assignment, dict) else {}
        scheduler_mode = str(operating.get("scheduler_mode", "full") or "full").strip().lower() or "full"
        if scheduler_mode not in {"full", "simple"}:
            scheduler_mode = "full"
        return {
            "operating_profile_name": str(operating.get("name", "") or "").strip(),
            "assignment_state": str(assignment.get("assignment_state", "") or "").strip().lower() or "unassigned",
            "scheduler_enabled": _row_bool(operating.get("scheduler_enabled", 1), True),
            "scheduler_mode": scheduler_mode,
            "use_messages": _row_bool(operating.get("use_messages", 1), True),
            "use_map": _row_bool(operating.get("use_map", 1), True),
            "use_background_ingest": _row_bool(operating.get("use_background_ingest", 1), True),
            "use_launch_control": _row_bool(operating.get("use_launch_control", 1), True),
            "use_net_control_tabs": _row_bool(operating.get("use_net_control_tabs", 1), True),
        }

    def shell_policy_signature(self) -> tuple[object, ...]:
        policy = self.operating_policy()
        return (
            str(policy.get("operating_profile_name", "") or "").strip(),
            str(policy.get("assignment_state", "") or "").strip().lower(),
            bool(policy.get("scheduler_enabled", True)),
            str(policy.get("scheduler_mode", "full") or "full").strip().lower(),
            bool(policy.get("use_messages", True)),
            bool(policy.get("use_map", True)),
            bool(policy.get("use_background_ingest", True)),
            bool(policy.get("use_launch_control", True)),
            bool(policy.get("use_net_control_tabs", True)),
        )

    def snapshot(self, *, force: bool = False) -> DeviceRuntimeSnapshot:
        service_states: Dict[str, Dict[str, object]] = {}
        if self.status_service is not None:
            service_states = self.status_service.status_snapshot(
                force=force,
                host_override=str(self.profile.get("js8_host", "") or "").strip() or None,
                port_override=int(self.profile.get("js8_port", 2442) or 2442),
                flrig_host_override=str(self.profile.get("flrig_host", "") or "").strip() or None,
                flrig_port_override=int(self.profile.get("flrig_port", 12345) or 12345),
                rigctld_host_override=str(self.profile.get("rig_host", "") or "").strip() or None,
                rigctld_port_override=int(self.profile.get("rig_port", 4532) or 4532),
                fldigi_host_override=str(self.profile.get("fldigi_host", "") or "").strip() or None,
                fldigi_port_override=int(self.profile.get("fldigi_port", 7362) or 7362),
            )

        backend = str(self.profile.get("control_backend", "manual") or "manual").strip().lower() or "manual"
        control_key = CONTROL_STATUS_KEYS.get(backend)
        control_info = dict(service_states.get(control_key, {})) if control_key else {}
        control_state = str(control_info.get("state", "ok" if backend == "manual" else "idle") or "idle").strip().lower()
        if backend == "manual":
            control_ready = True
            overall_state = "ok"
            status_summary = "Manual control"
        else:
            control_ready = control_state == "ok"
            overall_state = control_state if control_state in {"ok", "warn", "error"} else "idle"
            if self.is_primary and not control_ready and overall_state == "idle":
                overall_state = "warn"
            status_summary = str(control_info.get("tooltip", "") or "").strip()
            if not status_summary:
                if control_ready:
                    status_summary = f"{backend.upper()} control reachable"
                else:
                    status_summary = f"{backend.upper()} control unavailable"

        warning_text = ""
        if not self.is_primary:
            warning_text = "Current control tabs remain bound to the primary compatibility device."
            if backend == "js8call":
                warning_text = (
                    "Secondary JS8 devices are monitored, but current JS8 tab workflows remain bound "
                    "to the primary compatibility device."
                )

        assigned_name = str(self.operating_profile.get("name", "") or "").strip()
        policy = self.operating_policy()
        return DeviceRuntimeSnapshot(
            device_profile_id=int(self.profile.get("id", 0) or 0),
            name=str(self.profile.get("name", "") or "").strip(),
            control_backend=backend,
            deployment_mode=str(self.profile.get("deployment_mode", "full") or "full").strip().lower() or "full",
            runtime_active=bool(int(self.profile.get("runtime_active", 0) or 0)),
            runtime_primary=bool(self.is_primary),
            scheduler_owner=bool(self.is_primary),
            endpoint_summary=_device_endpoint_summary(self.profile),
            assigned_operating_profile_id=(
                int(self.assignment.get("operating_profile_id", 0) or 0) if self.assignment.get("operating_profile_id") is not None else None
            ),
            assigned_operating_profile_name=assigned_name,
            assignment_state=str(policy.get("assignment_state", "") or "").strip().lower(),
            scheduler_enabled=bool(policy.get("scheduler_enabled", True)),
            scheduler_mode=str(policy.get("scheduler_mode", "full") or "full").strip().lower(),
            use_messages=bool(policy.get("use_messages", True)),
            use_map=bool(policy.get("use_map", True)),
            use_background_ingest=bool(policy.get("use_background_ingest", True)),
            use_launch_control=bool(policy.get("use_launch_control", True)),
            use_net_control_tabs=bool(policy.get("use_net_control_tabs", True)),
            control_ready=bool(control_ready),
            overall_state=overall_state,
            status_summary=status_summary,
            warning_text=warning_text,
            service_states=service_states,
        )


class StationRuntimeManager:
    def __init__(self, store: Optional[MultiRadioStore] = None, settings: Optional[object] = None) -> None:
        self.store = store or MultiRadioStore()
        self.settings = settings
        self._runtimes: Dict[int, DeviceRuntime] = {}
        self._primary_device_id: Optional[int] = None

    def sync_with_store(self) -> None:
        settings_reload = getattr(self.settings, "reload", None)
        if callable(settings_reload):
            try:
                settings_reload()
            except Exception:
                pass

        active_profiles = list(self.store.list_runtime_active_device_profiles())
        primary_profile = self.store.get_runtime_primary_device_profile()
        primary_id = int(primary_profile.get("id", 0) or 0) if isinstance(primary_profile, dict) else None

        assignments = {
            int(row.get("device_profile_id", 0) or 0): dict(row)
            for row in self.store.list_effective_assignments()
            if isinstance(row, dict)
        }
        operating_profiles = {
            int(row.get("id", 0) or 0): dict(row)
            for row in self.store.list_operating_profiles()
            if isinstance(row, dict)
        }

        active_ids = {int(profile.get("id", 0) or 0) for profile in active_profiles}
        for stale_id in [runtime_id for runtime_id in self._runtimes if runtime_id not in active_ids]:
            runtime = self._runtimes.pop(stale_id, None)
            if runtime is not None:
                runtime.stop()

        for profile in active_profiles:
            device_id = int(profile.get("id", 0) or 0)
            assignment = assignments.get(device_id)
            operating_profile = operating_profiles.get(int(assignment.get("operating_profile_id", 0) or 0)) if assignment else None
            runtime = self._runtimes.get(device_id)
            if runtime is None:
                runtime = DeviceRuntime(
                    profile,
                    is_primary=device_id == primary_id,
                    fallback_settings=self.settings,
                    assignment=assignment,
                    operating_profile=operating_profile,
                )
                self._runtimes[device_id] = runtime
            else:
                runtime.update(
                    profile,
                    is_primary=device_id == primary_id,
                    assignment=assignment,
                    operating_profile=operating_profile,
                )

        self._primary_device_id = primary_id

    def stop(self) -> None:
        for runtime in list(self._runtimes.values()):
            runtime.stop()
        self._runtimes.clear()
        self._primary_device_id = None

    def get_runtime_snapshots(self, *, force: bool = False) -> List[DeviceRuntimeSnapshot]:
        snapshots = [runtime.snapshot(force=force) for runtime in self._runtimes.values()]
        snapshots.sort(key=lambda snap: (0 if snap.runtime_primary else 1, snap.name.lower(), snap.device_profile_id))
        return snapshots

    def get_primary_runtime(self) -> Optional[DeviceRuntime]:
        if self._primary_device_id is None:
            return None
        return self._runtimes.get(int(self._primary_device_id))

    def get_runtime_primary_device_profile(self) -> Optional[Dict[str, Any]]:
        runtime = self.get_primary_runtime()
        if runtime is None:
            return None
        return dict(runtime.profile)

    def primary_runtime_signature(self) -> tuple[object, ...]:
        runtime = self.get_primary_runtime()
        if runtime is None:
            return tuple()
        return runtime.primary_signature()

    def primary_runtime_policy(self) -> Dict[str, object]:
        runtime = self.get_primary_runtime()
        if runtime is None:
            return {
                "operating_profile_name": "",
                "assignment_state": "unassigned",
                "scheduler_enabled": True,
                "scheduler_mode": "full",
                "use_messages": True,
                "use_map": True,
                "use_background_ingest": True,
                "use_launch_control": True,
                "use_net_control_tabs": True,
            }
        return runtime.operating_policy()

    def primary_runtime_policy_signature(self) -> tuple[object, ...]:
        runtime = self.get_primary_runtime()
        if runtime is None:
            return tuple()
        return runtime.shell_policy_signature()
