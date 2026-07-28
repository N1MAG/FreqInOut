from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence, Tuple

from freqinout.core.config_autodiscovery import LOCALHOST, RadioInstanceProposal


@dataclass(frozen=True)
class ManagedAppProfilePlan:
    app_id: str
    instance_name: str
    executable_path: str
    config_dir: Path
    data_dirs: Tuple[Path, ...]
    launch_args: Tuple[str, ...]
    expected_host: str
    expected_port: int
    settings: Mapping[str, str] = field(default_factory=dict)
    notes: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def display_name(self) -> str:
        return {"flrig": "FLRig", "fldigi": "FLDigi"}.get(self.app_id, self.app_id)


def build_flrig_fldigi_managed_profile_plans(
    proposal: RadioInstanceProposal,
    *,
    config_root: Path,
    app_paths: Mapping[str, str] | None = None,
) -> Tuple[ManagedAppProfilePlan, ...]:
    root = Path(config_root) / "managed-instances" / proposal.instance_name
    paths = dict(app_paths or {})
    ports = _ports_by_service(proposal)
    plans = []
    if "flrig" in proposal.enabled_apps:
        flrig_config_dir = root / "flrig"
        flrig_port = ports.get("flrig", 12345)
        plans.append(
            ManagedAppProfilePlan(
                app_id="flrig",
                instance_name=proposal.instance_name,
                executable_path=paths.get("flrig", ""),
                config_dir=flrig_config_dir,
                data_dirs=(flrig_config_dir,),
                launch_args=("--config-dir", str(flrig_config_dir)),
                expected_host=LOCALHOST,
                expected_port=flrig_port,
                settings={
                    "xmlrpc_host": LOCALHOST,
                    "xmlrpc_port": str(flrig_port),
                },
                notes=(
                    "FLRig launch argument support must be verified against the installed binary before launch control uses it.",
                    "XML-RPC port is retained as managed profile settings for the later config writer.",
                ),
            )
        )
    if "fldigi" in proposal.enabled_apps:
        fldigi_config_dir = root / "fldigi"
        fldigi_logs_dir = fldigi_config_dir / "logs"
        fldigi_checkins_dir = fldigi_config_dir / "checkins"
        fldigi_port = ports.get("fldigi", 7362)
        flrig_port = ports.get("flrig", 12345)
        plans.append(
            ManagedAppProfilePlan(
                app_id="fldigi",
                instance_name=proposal.instance_name,
                executable_path=paths.get("fldigi", ""),
                config_dir=fldigi_config_dir,
                data_dirs=(fldigi_config_dir, fldigi_logs_dir, fldigi_checkins_dir),
                launch_args=(
                    "--config-dir",
                    str(fldigi_config_dir),
                    "--xmlrpc-server-address",
                    LOCALHOST,
                    "--xmlrpc-server-port",
                    str(fldigi_port),
                ),
                expected_host=LOCALHOST,
                expected_port=fldigi_port,
                settings={
                    "xmlrpc_host": LOCALHOST,
                    "xmlrpc_port": str(fldigi_port),
                    "flrig_host": LOCALHOST,
                    "flrig_port": str(flrig_port),
                    "log_dir": str(fldigi_logs_dir),
                    "checkin_dir": str(fldigi_checkins_dir),
                },
                notes=(
                    "FLDigi --config-dir and --xmlrpc-server-* arguments are supported by the local FLDigi manual.",
                    "FLDigi-to-FLRig client settings are retained for the later config writer.",
                ),
            )
        )
    return tuple(plans)


def create_managed_profile_directories(plans: Sequence[ManagedAppProfilePlan]) -> Tuple[Path, ...]:
    created_or_ready = []
    seen = set()
    for plan in plans:
        for path in (plan.config_dir, *plan.data_dirs):
            resolved = Path(path)
            key = str(resolved)
            if key in seen:
                continue
            resolved.mkdir(parents=True, exist_ok=True)
            seen.add(key)
            created_or_ready.append(resolved)
    return tuple(created_or_ready)


def _ports_by_service(proposal: RadioInstanceProposal) -> Mapping[str, int]:
    return {assignment.service: int(assignment.assigned_port) for assignment in proposal.ports}
