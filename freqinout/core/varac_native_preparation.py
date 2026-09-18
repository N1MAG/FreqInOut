"""Bounded discovery and immutable preparation for native VarAC clusters.

The GUI supplies reviewed intent and durable FIO snapshots.  This module may
read only the explicitly selected VarAC/VARA sources and produces either one
immutable writer plan or a presentation explaining why native apply is not
safe.  It never writes native files or FIO state.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from freqinout.core.config_varac_managed import (
    DEFAULT_SUPPORTED_VARAC_VERSIONS,
    VarACIniSource,
    VarACMemberInput,
    VarACNativeClusterPlan,
    VarACNativeClusterRequest,
    VarACNativeConfigurationError,
    VarARuntimeInput,
    build_varac_native_cluster_plan,
    parse_vara_ini_bytes,
    parse_varac_ini_bytes,
    snapshot_target_state,
    snapshot_vara_runtime_files,
)
from freqinout.core.varac_bbs_config import varac_path_to_host_path


@dataclass(frozen=True)
class VarACNativePreparationResult:
    state: str
    presentation: Mapping[str, Any]
    draft_fingerprint: str
    generation: int
    plan: VarACNativeClusterPlan | None = None
    error: str = ""

    @property
    def ready(self) -> bool:
        return self.state == "ready" and self.plan is not None


def native_draft_fingerprint(draft: Mapping[str, Any]) -> str:
    """Fingerprint operator intent while excluding host-generated native facts.

    Managed create/join preparation owns these targets.  Publishing them into
    the canonical assistant draft must not make the just-prepared plan stale;
    changing source/topology/policy evidence still changes the fingerprint.
    """

    generated_fields = {
        "configuration_path",
        "storage_path",
        "secondary_storage_path",
        "outbox_path",
        "working_directory",
        "launch_command",
        "vara_runtime_path",
        "vara_ini_path",
        "port",
        "secondary_port",
        "udp_port",
    }
    clean = {
        str(key): value
        for key, value in dict(draft or {}).items()
        if str(key)
        not in {
            "varac_native_presentation",
            "varac_native_generation",
            *generated_fields,
        }
    }
    encoded = json.dumps(clean, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def prepare_varac_native_configuration(
    draft: Mapping[str, Any],
    *,
    varac_nodes: Sequence[Mapping[str, Any]],
    device_profiles: Sequence[Mapping[str, Any]],
    varac_clusters: Sequence[Mapping[str, Any]],
    varac_members: Sequence[Mapping[str, Any]],
    managed_root: Path,
    generation: int,
    platform_override: str = "",
    process_running: bool = False,
) -> VarACNativePreparationResult:
    """Prepare the exact supported create/join plan from reviewed evidence."""

    intent = dict(draft or {})
    fingerprint = native_draft_fingerprint(intent)
    arrangement = str(intent.get("cluster_path") or "standalone").strip().lower()
    if str(intent.get("family_key") or "").strip().lower() != "varac":
        return _result("manual setup required", "This preparation route is only for VarAC.", intent, fingerprint, generation)
    if str(intent.get("mode") or "").strip().lower() != "managed":
        return _result("manual setup required", "Choose a new FIO-managed VarAC instance to use native configuration.", intent, fingerprint, generation)
    if arrangement not in {"create_cluster", "join_cluster"}:
        return _result("manual setup required", "Choose Create cluster or Join cluster before preparing native VarAC.", intent, fingerprint, generation)
    if process_running:
        return _result("stop required", "Close VarAC and VARA before preparing or applying their configuration.", intent, fingerprint, generation)

    platform_key = _platform_key(platform_override)
    if platform_key not in {"windows", "linux-wine"}:
        return _result(
            "manual setup required",
            "Native VarAC writing is qualified only for Windows and Linux/Wine in this release.",
            intent,
            fingerprint,
            generation,
            platform=platform_key,
        )
    try:
        plan = _build_plan(
            intent,
            nodes=tuple(dict(row) for row in varac_nodes),
            profiles=tuple(dict(row) for row in device_profiles),
            clusters=tuple(dict(row) for row in varac_clusters),
            memberships=tuple(dict(row) for row in varac_members),
            managed_root=Path(managed_root).expanduser(),
            platform_key=platform_key,
        )
    except (OSError, ValueError, VarACNativeConfigurationError) as exc:
        detail = str(exc).strip() or "The selected native VarAC evidence is incomplete or ambiguous."
        state = "stop required" if "running" in detail.casefold() else "needs attention"
        if "unsupported" in detail.casefold() or "not qualified" in detail.casefold():
            state = "manual setup required"
        return _result(state, detail, intent, fingerprint, generation, platform=platform_key, error=detail)

    sender = plan.email_gateway_sender_member_id or "No email gateway"
    member_numbers = ", ".join(
        f"{member.member_id}: {member.changes['VARAC_CLUSTER']['InstanceNumber']}"
        for member in plan.members
    )
    new_member = plan.members[-1]
    prepared_application_path = str(intent.get("application_path") or "").strip()
    if not prepared_application_path and new_member.launch_command:
        prepared_application_path = str(
            new_member.launch_command[1]
            if plan.platform == "linux-wine" and len(new_member.launch_command) > 1
            else new_member.launch_command[0]
        )
    member_root = new_member.target_path.parent
    vara_values = new_member.changes.get("VARAHF_CONFIG", {})
    presentation = {
        "state": "ready",
        "why": "Exact VarAC 13.2.7 source, paths, ports, runtime copies, and target state are qualified and ready for transactional apply.",
        "arrangement": arrangement,
        "affected_radios": tuple(_affected_radio_names(intent, plan, device_profiles)),
        "shared_database_summary": plan.shared_db_path,
        "member_numbers_summary": member_numbers,
        "ptt_lock_summary": "Enabled" if plan.ptt_lock_enabled else "Disabled",
        "email_gateway_sender_summary": sender,
        "writer_version": plan.version,
        "writer_platform": plan.platform,
        "writer_operation": plan.operation,
        "writer_qualified": True,
        "application_path": prepared_application_path,
        "varac_ini_path": str(new_member.target_path),
        "configuration_path": str(new_member.target_path),
        "storage_path": str(plan.shared_db_path),
        "secondary_storage_path": str(
            intent.get("secondary_storage_path") or member_root / "incoming"
        ),
        "outbox_path": str(intent.get("outbox_path") or member_root / "outbox"),
        "working_directory": str(new_member.working_directory),
        "vara_runtime_path": str(new_member.vara_target_runtime_folder),
        "vara_ini_path": str(new_member.vara_target_path),
        "launch_command": _display_argv(new_member.launch_command),
        "port": int(vara_values.get("VarahfMainPort") or 0),
        "secondary_port": int(vara_values.get("VarahfMainKissPort") or 0),
        "ports_summary": _ports_summary(plan),
        "fingerprints_summary": plan.plan_fingerprint,
        "generation": int(generation),
        "draft_fingerprint": fingerprint,
        "plan_fingerprint": plan.plan_fingerprint,
    }
    return VarACNativePreparationResult(
        state="ready",
        presentation=presentation,
        draft_fingerprint=fingerprint,
        generation=int(generation),
        plan=plan,
    )


def _build_plan(
    draft: Mapping[str, Any],
    *,
    nodes: Sequence[Mapping[str, Any]],
    profiles: Sequence[Mapping[str, Any]],
    clusters: Sequence[Mapping[str, Any]],
    memberships: Sequence[Mapping[str, Any]],
    managed_root: Path,
    platform_key: str,
) -> VarACNativeClusterPlan:
    arrangement = str(draft.get("cluster_path") or "").strip().lower()
    new_key = str(draft.get("draft_instance_key") or draft.get("owner_draft_key") or draft.get("instance_name") or "new-varac").strip()
    new_label = str(draft.get("instance_name") or draft.get("owner_label") or "New VarAC").strip()
    new_slug = _slug(new_label or new_key)
    managed_member_root = managed_root / new_slug / "varac-native"
    new_ini_target = managed_member_root / "VarAC.ini"
    new_vara_target = managed_member_root / "VARA"

    existing_node: Mapping[str, Any] | None = None
    existing_profile: Mapping[str, Any] | None = None
    cluster: Mapping[str, Any] | None = None
    operation = "create-member"
    if arrangement == "create_cluster":
        node_id = _positive_int(draft.get("existing_standalone_node_id"))
        existing_node = _unique(nodes, "id", node_id, "existing standalone VarAC node")
        existing_profile = _profile_for_node(profiles, node_id)
        shared_db = str(draft.get("cluster_shared_database") or existing_node.get("db_path") or "").strip()
        if not shared_db:
            raise ValueError("The existing standalone VarAC database is unknown; choose a shared database before preparing.")
        operation = "convert-standalone"
    else:
        cluster_token = str(draft.get("cluster_id") or "").strip()
        cluster = _cluster_by_token(clusters, cluster_token)
        shared_db = str(cluster.get("shared_db_path") or "").strip()
        if not shared_db:
            raise ValueError("The selected cluster has no verified shared VarAC database.")
        member_rows = [row for row in memberships if int(row.get("cluster_id") or 0) == int(cluster.get("id") or 0) and int(row.get("enabled", 1) or 0) == 1]
        if not member_rows:
            raise ValueError("The selected cluster has no enabled member that can provide qualified source evidence.")
        seed_profile_id = int(member_rows[0].get("device_profile_id") or 0)
        existing_profile = _unique(profiles, "id", seed_profile_id, "existing cluster member")
        existing_node = _unique(nodes, "id", int(existing_profile.get("varac_node_id") or 0), "existing cluster VarAC node")

    source_ini_path = _required_file(existing_node.get("ini_path"), "existing VarAC.ini")
    source_ini = parse_varac_ini_bytes(source_ini_path, source_ini_path.read_bytes())
    wine_prefix = _wine_prefix(source_ini.path) if platform_key == "linux-wine" else ""
    source_executable = _resolve_varac_executable(
        draft.get("application_path") or existing_node.get("install_path")
    )
    version = _qualified_version(
        str(draft.get("version") or "").strip(),
        str(existing_node.get("native_writer_key") or "").strip(),
        source_executable,
    )
    source_vara_root = _resolve_vara_runtime(existing_node, source_ini)
    source_vara_ini = _required_file(source_vara_root / "VARA.ini", "source VARA.ini")
    source_vara = parse_vara_ini_bytes(source_vara_ini, source_vara_ini.read_bytes())
    source_files = snapshot_vara_runtime_files(source_vara_root)

    occupied = _occupied_ports(nodes)
    source_ports = _source_vara_ports(source_ini, source_vara)
    occupied.update(source_ports)
    new_ports = _allocate_port_bundle(occupied, start=max(8300, source_ports[0] + 10))
    new_member_number = _positive_int(draft.get("cluster_instance_number"))
    if not new_member_number:
        raise ValueError("Choose a positive VarAC cluster member number before preparing.")
    members: list[VarACMemberInput] = []
    if arrangement == "create_cluster":
        existing_number = _positive_int(draft.get("existing_standalone_member_number")) or 1
        if existing_number == new_member_number:
            raise ValueError("The existing and new VarAC members need different member numbers.")
        existing_member_id = f"node:{int(existing_node.get('id') or 0)}"
        existing_target_root = managed_root / _slug(str(existing_profile.get("name") or existing_node.get("name") or existing_member_id)) / "varac-native" / "VARA"
        members.append(
            _member(
                member_id=existing_member_id,
                source_ini=source_ini,
                target_ini=source_ini_path,
                member_number=existing_number,
                executable=_resolve_varac_executable(existing_node.get("install_path")),
                source_vara_root=source_vara_root,
                source_vara=source_vara,
                source_files=source_files,
                target_vara_root=existing_target_root,
                ports=source_ports,
                platform_key=platform_key,
            )
        )
    members.append(
        _member(
            member_id=new_key,
            source_ini=source_ini,
            target_ini=new_ini_target,
            member_number=new_member_number,
            executable=source_executable,
            source_vara_root=source_vara_root,
            source_vara=source_vara,
            source_files=source_files,
            target_vara_root=new_vara_target,
            ports=new_ports,
            platform_key=platform_key,
        )
    )
    sender_choice = str(draft.get("email_gateway_sender_choice") or "none").strip().lower()
    sender = ""
    if sender_choice == "existing_member" and arrangement == "create_cluster":
        sender = members[0].member_id
    elif sender_choice == "new_member":
        sender = new_key
    elif sender_choice not in {"", "none"}:
        raise ValueError("Choose No email gateway, existing member, or new member before preparing.")

    roots = _minimal_roots(
        managed_root,
        source_ini_path.parent,
        source_vara_root,
        Path(shared_db).expanduser().parent,
    )
    return build_varac_native_cluster_plan(
        VarACNativeClusterRequest(
            version=version,
            platform=platform_key,
            operation=operation,
            members=tuple(members),
            shared_db_path=str(Path(shared_db).expanduser()),
            native_shared_db_path=_native_varac_path(
                Path(shared_db).expanduser(),
                platform_key=platform_key,
                wine_prefix=wine_prefix,
            ),
            allowed_roots=roots,
            ptt_lock_enabled=bool(draft.get("cluster_ptt_lock", False)),
            email_gateway_sender_member_id=sender,
            wine_executable="wine",
        )
    )


def _member(
    *,
    member_id: str,
    source_ini: VarACIniSource,
    target_ini: Path,
    member_number: int,
    executable: Path,
    source_vara_root: Path,
    source_vara: VarACIniSource,
    source_files: tuple[Any, ...],
    target_vara_root: Path,
    ports: tuple[int, int, int],
    platform_key: str,
) -> VarACMemberInput:
    command, kiss, monitor = ports
    target_exe = target_vara_root / "VARA.exe"
    target_vara_ini = target_vara_root / "VARA.ini"
    wine_prefix = _wine_prefix(source_ini.path) if platform_key == "linux-wine" else ""
    configured_target_exe = _native_varac_path(
        target_exe,
        platform_key=platform_key,
        wine_prefix=wine_prefix,
    )
    return VarACMemberInput(
        member_id=member_id,
        source=source_ini,
        target=snapshot_target_state(target_ini),
        member_number=member_number,
        vara_settings={
            "VarahfMainPath": configured_target_exe,
            "VarahfMainPort": str(command),
            "VarahfMainHost": "127.0.0.1",
            "VarahfEnableKissInterface": "ON",
            "VarahfMainKissPort": str(kiss),
            "VarahfMonitorPath": configured_target_exe,
            "VarahfMonitorPort": str(monitor),
            "VarahfLaunchOnModemConnect": "OFF",
        },
        executable_path=str(executable),
        vara_runtime=VarARuntimeInput(
            source_runtime_folder=source_vara_root,
            target_runtime_folder=target_vara_root,
            source=source_vara,
            target=snapshot_target_state(target_vara_ini),
            settings={
                "Setup": {
                    "TCP Command Port": str(command),
                    "Enable KISS": "1",
                    "KISS Port": str(kiss),
                },
                "Monitor": {"Monitor Mode": "1"},
            },
            files=source_files,
            configured_main_executable_path=configured_target_exe,
            configured_monitor_executable_path=configured_target_exe,
        ),
        working_directory=str(target_ini.parent),
        wine_prefix=wine_prefix,
        launch_ini_path=_native_varac_path(
            target_ini,
            platform_key=platform_key,
            wine_prefix=wine_prefix,
        ),
    )


def _source_vara_ports(varac: VarACIniSource, vara: VarACIniSource) -> tuple[int, int, int]:
    section = _section(varac.values, "VARAHF_CONFIG")
    setup = _section(vara.values, "Setup")
    monitor_section = _section(vara.values, "Monitor")
    command = _port(_value(section, "VarahfMainPort") or _value(setup, "TCP Command Port"), "VARA command port")
    kiss = _port(_value(section, "VarahfMainKissPort") or _value(setup, "KISS Port"), "VARA KISS port")
    monitor = _port(_value(section, "VarahfMonitorPort") or _value(monitor_section, "Monitor Port") or command + 3, "VARA monitor port")
    return command, kiss, monitor


def _occupied_ports(nodes: Sequence[Mapping[str, Any]]) -> set[int]:
    ports: set[int] = set()
    for node in nodes:
        path = Path(str(node.get("ini_path") or "")).expanduser()
        if not path.is_file() or path.is_symlink():
            continue
        try:
            source = parse_varac_ini_bytes(path, path.read_bytes())
            command, kiss, monitor = _source_vara_ports(source, source)
        except Exception:
            continue
        ports.update((command, command + 1, kiss, monitor))
    return ports


def _allocate_port_bundle(occupied: set[int], *, start: int) -> tuple[int, int, int]:
    command = max(1024, int(start))
    while command <= 65520:
        proposed = (command, command + 1, command + 2, command + 3)
        if not any(port in occupied for port in proposed):
            return command, command + 2, command + 3
        command += 10
    raise ValueError("FIO could not allocate a non-conflicting local VARA port bundle.")


def _qualified_version(hint: str, writer_key: str, executable: Path) -> str:
    candidates = [hint]
    candidates.extend(re.findall(r"(?<!\d)(\d+\.\d+\.\d+)(?!\d)", writer_key))
    for candidate in candidates:
        if candidate in DEFAULT_SUPPORTED_VARAC_VERSIONS:
            return candidate
    with executable.open("rb") as handle:
        raw = handle.read(64 * 1024 * 1024)
    for version in DEFAULT_SUPPORTED_VARAC_VERSIONS:
        if version.encode("ascii") in raw or version.encode("utf-16le") in raw:
            return version
    raise ValueError("The detected VarAC version is not qualified for native writing; configure this cluster manually.")


def _resolve_varac_executable(value: Any) -> Path:
    path = Path(str(value or "")).expanduser()
    candidates = (path, path / "VarAC.exe", path / "varac.exe") if path.suffix else (path / "VarAC.exe", path / "varac.exe", path)
    for candidate in candidates:
        if candidate.is_file() and not candidate.is_symlink():
            return candidate
    raise ValueError("VarAC.exe was not found in the selected application location.")


def _resolve_vara_runtime(node: Mapping[str, Any], source: VarACIniSource) -> Path:
    explicit = str(node.get("vara_runtime_path") or "").strip()
    if explicit:
        candidate = Path(explicit).expanduser()
    else:
        configured = _value(_section(source.values, "VARAHF_CONFIG"), "VarahfMainPath")
        candidate = Path(varac_path_to_host_path(configured, ini_path=source.path)).expanduser().parent
    if not candidate.is_dir() or candidate.is_symlink():
        raise ValueError("The source VARA runtime folder could not be verified.")
    return candidate


def _platform_key(override: str) -> str:
    value = str(override or "").strip().lower()
    if value:
        return value
    if os.name == "nt":
        return "windows"
    if sys.platform.startswith("linux"):
        return "linux-wine"
    return "macos-wine" if sys.platform == "darwin" else sys.platform


def _result(state: str, why: str, draft: Mapping[str, Any], fingerprint: str, generation: int, *, platform: str = "", error: str = "") -> VarACNativePreparationResult:
    presentation = {
        "state": state,
        "why": why,
        "arrangement": str(draft.get("cluster_path") or ""),
        "affected_radios": tuple(filter(None, (str(draft.get("owner_label") or draft.get("instance_name") or "").strip(),))),
        "writer_platform": platform,
        "writer_qualified": False,
        "generation": int(generation),
        "draft_fingerprint": fingerprint,
    }
    return VarACNativePreparationResult(state, presentation, fingerprint, int(generation), error=error)


def _section(values: Mapping[str, Mapping[str, str]], wanted: str) -> Mapping[str, str]:
    for name, section in values.items():
        if str(name).casefold() == wanted.casefold():
            return section
    return {}


def _value(values: Mapping[str, Any], wanted: str) -> str:
    for key, value in values.items():
        if str(key).casefold() == wanted.casefold():
            return str(value or "").strip()
    return ""


def _port(value: Any, label: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} is unavailable or invalid.") from exc
    if not 1 <= number <= 65535:
        raise ValueError(f"{label} is outside the valid port range.")
    return number


def _positive_int(value: Any) -> int:
    try:
        number = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def _unique(rows: Sequence[Mapping[str, Any]], key: str, value: Any, label: str) -> Mapping[str, Any]:
    matches = [row for row in rows if str(row.get(key) or "") == str(value or "")]
    if len(matches) != 1:
        raise ValueError(f"The {label} is missing or ambiguous; refresh discovery and choose it again.")
    return matches[0]


def _profile_for_node(profiles: Sequence[Mapping[str, Any]], node_id: int) -> Mapping[str, Any]:
    matches = [row for row in profiles if int(row.get("varac_node_id") or 0) == int(node_id)]
    if len(matches) != 1:
        raise ValueError("The existing standalone VarAC node must belong to exactly one radio.")
    return matches[0]


def _cluster_by_token(clusters: Sequence[Mapping[str, Any]], token: str) -> Mapping[str, Any]:
    matches = [row for row in clusters if token and token.casefold() in {str(row.get("id") or "").casefold(), str(row.get("cluster_id") or "").casefold()}]
    if len(matches) != 1:
        raise ValueError("The existing VarAC cluster is missing or ambiguous; choose it again.")
    return matches[0]


def _required_file(value: Any, label: str) -> Path:
    path = Path(str(value or "")).expanduser()
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"The {label} could not be verified.")
    return path


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return slug or "varac-member"


def _minimal_roots(*paths: Path) -> tuple[Path, ...]:
    roots: list[Path] = []
    for path in paths:
        candidate = Path(path).expanduser()
        if candidate not in roots:
            roots.append(candidate)
    return tuple(roots)


def _wine_prefix(ini_path: Path) -> str:
    parts = ini_path.expanduser().parts
    for index, part in enumerate(parts):
        if re.fullmatch(r"drive_[a-zA-Z]", part):
            return str(Path(*parts[:index]))
    return str(Path(os.environ.get("WINEPREFIX", "~/.wine")).expanduser())


def _native_varac_path(path: Path, *, platform_key: str, wine_prefix: str) -> str:
    """Return the path spelling the Windows VarAC process can consume."""

    candidate = Path(path).expanduser().absolute()
    if platform_key != "linux-wine":
        return str(candidate)
    prefix = Path(wine_prefix).expanduser().absolute()
    try:
        relative = candidate.relative_to(prefix)
    except ValueError:
        relative = None
    if relative is not None and relative.parts:
        drive_match = re.fullmatch(r"drive_([a-zA-Z])", relative.parts[0])
        if drive_match:
            suffix = "\\".join(relative.parts[1:])
            return f"{drive_match.group(1).upper()}:\\{suffix}" if suffix else f"{drive_match.group(1).upper()}:\\"
    # Wine's default Z: mapping exposes the host root.  This keeps generated
    # managed paths usable even when the FIO configuration root is outside the
    # selected prefix's drive_c directory.
    return "Z:\\" + "\\".join(candidate.parts[1:])


def _display_argv(argv: Iterable[str]) -> str:
    return " ".join(f'"{item}"' if any(ch.isspace() for ch in item) else item for item in argv)


def _ports_summary(plan: VarACNativeClusterPlan) -> str:
    return "; ".join(
        f"{member.member_id}: command {member.vara_changes['Setup']['TCP Command Port']}, KISS {member.vara_changes['Setup']['KISS Port']}"
        for member in plan.members
    )


def _affected_radio_names(draft: Mapping[str, Any], plan: VarACNativeClusterPlan, profiles: Sequence[Mapping[str, Any]]) -> Iterable[str]:
    by_node = {int(row.get("varac_node_id") or 0): str(row.get("name") or "").strip() for row in profiles}
    for member in plan.members:
        if member.member_id.startswith("node:"):
            yield by_node.get(int(member.member_id.split(":", 1)[1]), member.member_id)
        else:
            yield str(draft.get("owner_label") or draft.get("instance_name") or member.member_id).strip()


__all__ = [
    "VarACNativePreparationResult",
    "native_draft_fingerprint",
    "prepare_varac_native_configuration",
]
