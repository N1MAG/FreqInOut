from __future__ import annotations

import configparser
import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence, Tuple

from freqinout.core.config_autodiscovery import LOCALHOST, RadioInstanceProposal


@dataclass(frozen=True)
class JS8CallManagedProfilePlan:
    profile_name: str
    instance_name: str
    executable_path: str
    config_dir: Path
    save_dir: Path
    forms_dir: Path
    directed_path: Path
    flrig_host: str
    flrig_port: int
    tcp_host: str
    tcp_port: int
    udp_port: int
    control_route: str = "flrig"
    rig_summary: str = ""
    settings: Mapping[str, str] = field(default_factory=dict)


def build_js8call_managed_profile_plans(
    proposals: Sequence[RadioInstanceProposal],
    *,
    config_root: Path,
    js8call_path: str = "",
    callsign: str = "",
    grid: str = "",
    control_route: str = "flrig",
    radio_label: str = "",
) -> Tuple[JS8CallManagedProfilePlan, ...]:
    plans = []
    route_key = str(control_route or "flrig").strip().lower()
    for proposal in proposals:
        if "js8call" not in proposal.enabled_apps:
            continue
        ports = _ports_by_service(proposal)
        profile_root = Path(config_root) / "managed-instances" / proposal.instance_name / "js8call"
        save_dir = profile_root / "save"
        forms_dir = profile_root / "forms"
        directed_path = profile_root / "DIRECTED.TXT"
        flrig_port = ports.get("flrig", 12345)
        tcp_port = ports.get("js8call", 2442)
        udp_port = ports.get("js8call_udp", 2242)
        settings = {
            "TCPEnabled": "true",
            "TCPServer": LOCALHOST,
            "TCPServerPort": str(tcp_port),
            "TCPMaxConnections": "2",
            "UDPEnabled": "true",
            "UDPServerPort": str(udp_port),
            "SaveDir": str(save_dir),
        }
        rig_summary = "JS8Call radio/CAT selection requires operator review."
        if route_key == "flrig":
            settings["Rig"] = "FLRig FLRig"
            settings["CATNetworkPort"] = f"{LOCALHOST}:{flrig_port}"
            rig_summary = f"FLRig {LOCALHOST}:{flrig_port}"
        elif route_key == "js8call":
            rig_text = str(radio_label or proposal.name or "").strip()
            if rig_text:
                rig_summary = f"JS8Call controls {rig_text}; confirm the radio in JS8Call."
            else:
                rig_summary = "JS8Call controls the radio; confirm the radio in JS8Call."
        elif route_key in {"none", "manual", "later"}:
            rig_summary = "No FIO-managed JS8Call frequency control."
        if callsign.strip():
            settings["MyCall"] = callsign.strip().upper()
        if grid.strip():
            settings["MyGrid"] = grid.strip().upper()
        plans.append(
            JS8CallManagedProfilePlan(
                profile_name=proposal.instance_name,
                instance_name=proposal.instance_name,
                executable_path=js8call_path,
                config_dir=profile_root,
                save_dir=save_dir,
                forms_dir=forms_dir,
                directed_path=directed_path,
                flrig_host=LOCALHOST,
                flrig_port=flrig_port,
                tcp_host=LOCALHOST,
                tcp_port=tcp_port,
                udp_port=udp_port,
                control_route=route_key,
                rig_summary=rig_summary,
                settings=settings,
            )
        )
    return tuple(plans)


def create_js8call_managed_directories(plans: Sequence[JS8CallManagedProfilePlan]) -> Tuple[Path, ...]:
    created_or_ready = []
    seen = set()
    for plan in plans:
        for path in (plan.config_dir, plan.save_dir, plan.forms_dir):
            key = str(path)
            if key in seen:
                continue
            path.mkdir(parents=True, exist_ok=True)
            seen.add(key)
            created_or_ready.append(path)
    return tuple(created_or_ready)


def render_js8call_multisettings_ini(
    existing_ini_text: str,
    plans: Sequence[JS8CallManagedProfilePlan],
) -> str:
    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    if existing_ini_text.strip():
        parser.read_string(existing_ini_text)
    for plan in plans:
        section = f"MultiSettings/{plan.profile_name}"
        if not parser.has_section(section):
            parser.add_section(section)
        for key, value in plan.settings.items():
            parser.set(section, key, str(value))
    output = io.StringIO()
    parser.write(output)
    return output.getvalue()


def _ports_by_service(proposal: RadioInstanceProposal) -> Mapping[str, int]:
    return {assignment.service: int(assignment.assigned_port) for assignment in proposal.ports}
