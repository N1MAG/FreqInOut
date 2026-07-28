from __future__ import annotations

import configparser
import os
import platform as platform_module
import shutil
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple


LOCALHOST = "127.0.0.1"

APP_DISPLAY_NAMES: Mapping[str, str] = {
    "flrig": "FLRig",
    "fldigi": "FLDigi",
    "flmsg": "FLMsg",
    "flamp": "FLAmp",
    "js8call": "JS8Call",
    "js8spotter": "JS8Spotter",
    "commstat": "CommStat",
    "varac": "VarAC",
}

DEFAULT_RADIO_INSTANCE_NAMES: Tuple[str, ...] = ("fio-a", "fio-b", "fio-c", "fio-d")

DEFAULT_PORT_PLAN: Mapping[str, Tuple[int, ...]] = {
    "rigctld": (4532, 4533, 4534, 4535),
    "flrig": (12345, 12346, 12347, 12348),
    "fldigi": (7362, 7363, 7364, 7365),
    "js8call": (2442, 2443, 2444, 2445),
    "js8call_udp": (2242, 2243, 2244, 2245),
    "wsjtx_udp": (2237, 2238, 2239, 2240),
}


@dataclass(frozen=True)
class AppCandidate:
    app_id: str
    display_name: str
    path: str
    source: str
    confidence: str
    exists: bool
    executable: bool
    target_type: str
    notes: Tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class PortAssignment:
    service: str
    host: str
    preferred_port: int
    assigned_port: int
    conflict: bool
    protocol: str = "tcp"
    conflict_checked: bool = True
    note: str = ""


@dataclass(frozen=True)
class RadioInstanceProposal:
    name: str
    instance_name: str
    index: int
    enabled_apps: Tuple[str, ...]
    ports: Tuple[PortAssignment, ...]
    varac_enabled: bool = False
    notes: Tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class JS8CallConfigProfile:
    name: str
    settings: Mapping[str, str]


@dataclass(frozen=True)
class AutoconfigProposal:
    platform: str
    candidates: Tuple[AppCandidate, ...]
    radios: Tuple[RadioInstanceProposal, ...]
    warnings: Tuple[str, ...]
    missing_apps: Tuple[str, ...]
    js8_profiles: Tuple[JS8CallConfigProfile, ...] = field(default_factory=tuple)


def normalize_platform(value: Optional[str] = None) -> str:
    raw = (value or platform_module.system() or "").strip().lower()
    if raw in {"darwin", "mac", "macos", "osx"}:
        return "Darwin"
    if raw in {"windows", "win32", "win"}:
        return "Windows"
    if raw in {"linux", "gnu/linux"}:
        return "Linux"
    return value or platform_module.system() or "Unknown"


def default_app_search_paths(
    *,
    platform: Optional[str] = None,
    home: Optional[Path] = None,
) -> Mapping[str, Tuple[Path, ...]]:
    system = normalize_platform(platform)
    user_home = Path(home) if home is not None else Path.home()

    if system == "Darwin":
        base_dirs = (
            Path("/Applications"),
            Path("/Applications/RadioApps"),
            user_home / "Applications",
            user_home / "RadioTools" / "bin",
            user_home / "RadioTools" / "Programs",
        )
        return {
            "flrig": _mac_app_candidates(base_dirs, ("FLRig", "flrig")),
            "fldigi": _mac_app_candidates(base_dirs, ("FLDigi", "fldigi")),
            "flmsg": _mac_app_candidates(base_dirs, ("FLMsg", "flmsg")),
            "flamp": _mac_app_candidates(base_dirs, ("FLAmp", "flamp")),
            "js8call": _mac_app_candidates(base_dirs, ("JS8Call", "js8call")),
            "js8spotter": _mac_app_candidates(base_dirs, ("JS8Spotter", "js8spotter")),
            "commstat": _mac_app_candidates(base_dirs, ("CommStat", "commstat")),
            "varac": _mac_app_candidates(base_dirs, ("VarAC", "varac"))
            + (
                user_home / ".wine" / "drive_c" / "VarAC",
                user_home / ".wine" / "drive_c" / "Program Files" / "VarAC",
                user_home / ".wine" / "drive_c" / "Program Files (x86)" / "VarAC",
            ),
        }

    if system == "Windows":
        program_files = Path(os.environ.get("ProgramFiles", "C:/Program Files"))
        program_files_x86 = Path(os.environ.get("ProgramFiles(x86)", "C:/Program Files (x86)"))
        local_app_data = Path(os.environ.get("LOCALAPPDATA", str(user_home / "AppData" / "Local")))
        return {
            "flrig": (
                program_files / "flrig" / "flrig.exe",
                program_files_x86 / "flrig" / "flrig.exe",
                local_app_data / "flrig" / "flrig.exe",
            ),
            "fldigi": (
                program_files / "fldigi" / "fldigi.exe",
                program_files_x86 / "fldigi" / "fldigi.exe",
                local_app_data / "fldigi" / "fldigi.exe",
            ),
            "flmsg": (
                program_files / "flmsg" / "flmsg.exe",
                program_files_x86 / "flmsg" / "flmsg.exe",
                local_app_data / "flmsg" / "flmsg.exe",
            ),
            "flamp": (
                program_files / "flamp" / "flamp.exe",
                program_files_x86 / "flamp" / "flamp.exe",
                local_app_data / "flamp" / "flamp.exe",
            ),
            "js8call": (
                program_files / "JS8Call" / "js8call.exe",
                program_files_x86 / "JS8Call" / "js8call.exe",
                local_app_data / "JS8Call" / "js8call.exe",
            ),
            "js8spotter": (
                program_files / "JS8Spotter" / "JS8Spotter.exe",
                program_files_x86 / "JS8Spotter" / "JS8Spotter.exe",
                local_app_data / "Programs" / "JS8Spotter" / "JS8Spotter.exe",
            ),
            "commstat": (
                program_files / "CommStat" / "CommStat.exe",
                program_files_x86 / "CommStat" / "CommStat.exe",
                local_app_data / "Programs" / "CommStat" / "CommStat.exe",
            ),
            "varac": (
                program_files / "VarAC",
                program_files_x86 / "VarAC",
                local_app_data / "VarAC",
                user_home / "AppData" / "Local" / "VarAC",
            ),
        }

    return {
        "flrig": (Path("/usr/bin/flrig"), Path("/usr/local/bin/flrig"), Path("/opt/flrig/flrig")),
        "fldigi": (Path("/usr/bin/fldigi"), Path("/usr/local/bin/fldigi"), Path("/opt/fldigi/fldigi")),
        "flmsg": (Path("/usr/bin/flmsg"), Path("/usr/local/bin/flmsg"), Path("/opt/flmsg/flmsg")),
        "flamp": (Path("/usr/bin/flamp"), Path("/usr/local/bin/flamp"), Path("/opt/flamp/flamp")),
        "js8call": (Path("/usr/bin/js8call"), Path("/usr/local/bin/js8call"), Path("/opt/js8call/js8call")),
        "js8spotter": (
            Path("/usr/bin/js8spotter"),
            Path("/usr/local/bin/js8spotter"),
            user_home / ".local" / "bin" / "js8spotter",
            user_home / "bin" / "js8spotter",
        ),
        "commstat": (
            Path("/usr/bin/commstat"),
            Path("/usr/local/bin/commstat"),
            user_home / ".local" / "bin" / "commstat",
            user_home / "bin" / "commstat",
        ),
        "varac": (
            user_home / ".wine" / "drive_c" / "VarAC",
            user_home / ".wine" / "drive_c" / "Program Files" / "VarAC",
            user_home / ".wine" / "drive_c" / "Program Files (x86)" / "VarAC",
            user_home / ".varac",
        ),
    }


def find_app_candidates(
    *,
    apps: Sequence[str] = ("flrig", "fldigi", "flmsg", "flamp", "js8call", "js8spotter", "commstat", "varac"),
    platform: Optional[str] = None,
    home: Optional[Path] = None,
    extra_paths: Sequence[Path] = (),
    app_search_paths: Optional[Mapping[str, Sequence[Path]]] = None,
) -> Tuple[AppCandidate, ...]:
    system = normalize_platform(platform)
    search_paths = app_search_paths if app_search_paths is not None else default_app_search_paths(platform=system, home=home)
    candidates = []
    for app_id in apps:
        normalized_app = app_id.strip().lower()
        seen_paths = set()
        scoped_extra_paths = [path for path in extra_paths if _path_name_matches_app(normalized_app, Path(path))]
        raw_paths = list(search_paths.get(normalized_app, ())) + scoped_extra_paths
        for raw in _unique_paths(raw_paths):
            candidate = _candidate_from_path(normalized_app, raw, system=system, source="known_path")
            key = os.path.normcase(os.path.normpath(candidate.path))
            if key in seen_paths:
                continue
            seen_paths.add(key)
            if candidate.exists:
                candidates.append(candidate)

        if any(candidate.app_id == normalized_app and candidate.executable for candidate in candidates):
            continue

        for command in _command_names(normalized_app):
            resolved = shutil.which(command)
            if not resolved:
                continue
            candidate = _candidate_from_path(normalized_app, Path(resolved), system=system, source="path")
            candidates.append(candidate)
            break

    return tuple(candidates)


def is_local_tcp_port_listening(host: str, port: int, timeout: float = 0.15) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def assign_default_ports(
    *,
    radio_count: int = 3,
    host: str = LOCALHOST,
    busy_checker: Callable[[str, int], bool] = is_local_tcp_port_listening,
) -> Tuple[Tuple[PortAssignment, ...], ...]:
    assignments = []
    used_ports = set()
    for index in range(max(0, int(radio_count))):
        radio_ports = []
        for service, defaults in DEFAULT_PORT_PLAN.items():
            protocol = _service_protocol(service)
            preferred = _default_port_for_index(defaults, index)
            assigned = preferred
            conflict_checked = protocol == "tcp"
            conflict = bool((conflict_checked and busy_checker(host, assigned)) or assigned in used_ports)
            if conflict:
                assigned = _first_available_port(preferred + 10, host, used_ports, busy_checker)
            used_ports.add(assigned)
            radio_ports.append(
                PortAssignment(
                    service=service,
                    host=host,
                    preferred_port=preferred,
                    assigned_port=assigned,
                    conflict=conflict,
                    protocol=protocol,
                    conflict_checked=conflict_checked,
                    note=_port_note(service, preferred, assigned, conflict, conflict_checked=conflict_checked),
                )
            )
        assignments.append(tuple(radio_ports))
    return tuple(assignments)


def build_lab_radio_proposals(
    *,
    radio_count: int = 3,
    enabled_apps: Sequence[str] = ("flrig", "fldigi", "js8call"),
    include_varac: bool = False,
    host: str = LOCALHOST,
    busy_checker: Callable[[str, int], bool] = is_local_tcp_port_listening,
) -> Tuple[RadioInstanceProposal, ...]:
    port_sets = assign_default_ports(radio_count=radio_count, host=host, busy_checker=busy_checker)
    radios = []
    clean_enabled_apps = tuple(app.strip().lower() for app in enabled_apps if str(app).strip())
    for index, ports in enumerate(port_sets):
        instance_name = _instance_name(index)
        radios.append(
            RadioInstanceProposal(
                name=f"Radio {chr(ord('A') + index)}",
                instance_name=instance_name,
                index=index,
                enabled_apps=clean_enabled_apps,
                ports=ports,
                varac_enabled=include_varac,
                notes=(f"Lab-safe managed instance {instance_name}.",),
            )
        )
    return tuple(radios)


def read_js8call_multisettings(ini_path: Path) -> Tuple[JS8CallConfigProfile, ...]:
    path = Path(ini_path)
    if not path.is_file():
        return ()

    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    try:
        parser.read(path, encoding="utf-8")
    except configparser.Error:
        return ()

    profiles = []
    root_settings = _selected_js8_settings(parser.defaults())
    if parser.has_section("Configuration"):
        root_settings.update(_selected_js8_settings(dict(parser.items("Configuration"))))
    if root_settings:
        profiles.append(JS8CallConfigProfile(name="Default", settings=root_settings))

    for section in parser.sections():
        normalized = section.strip()
        if not normalized.lower().startswith("multisettings/"):
            continue
        name = normalized.split("/", 1)[1].strip() or normalized
        settings = _selected_js8_settings(dict(parser.items(section)))
        if settings:
            profiles.append(JS8CallConfigProfile(name=name, settings=settings))
    return tuple(profiles)


def build_autoconfig_proposal(
    *,
    radio_count: int = 1,
    platform: Optional[str] = None,
    home: Optional[Path] = None,
    include_varac: bool = False,
    extra_app_paths: Sequence[Path] = (),
    app_search_paths: Optional[Mapping[str, Sequence[Path]]] = None,
    js8_ini_path: Optional[Path] = None,
    busy_checker: Callable[[str, int], bool] = is_local_tcp_port_listening,
) -> AutoconfigProposal:
    system = normalize_platform(platform)
    candidates = find_app_candidates(
        platform=system,
        home=home,
        extra_paths=extra_app_paths,
        app_search_paths=app_search_paths,
    )
    found_app_ids = {candidate.app_id for candidate in candidates if candidate.executable}
    desired_apps = ("flrig", "fldigi", "js8call")
    missing_apps = tuple(app_id for app_id in desired_apps if app_id not in found_app_ids)
    warnings = []
    if missing_apps:
        labels = ", ".join(APP_DISPLAY_NAMES.get(app_id, app_id) for app_id in missing_apps)
        warnings.append(f"FIO could not find {labels}; the user can choose paths, search again, or skip those apps.")
    if not include_varac:
        warnings.append("VarAC is optional and remains disabled unless the operator enables it.")

    radios = build_lab_radio_proposals(
        radio_count=radio_count,
        include_varac=include_varac,
        busy_checker=busy_checker,
    )
    js8_profiles = read_js8call_multisettings(js8_ini_path) if js8_ini_path is not None else ()
    return AutoconfigProposal(
        platform=system,
        candidates=candidates,
        radios=radios,
        warnings=tuple(warnings),
        missing_apps=missing_apps,
        js8_profiles=js8_profiles,
    )


def _mac_app_candidates(base_dirs: Iterable[Path], names: Sequence[str]) -> Tuple[Path, ...]:
    candidates = []
    for base in base_dirs:
        base_path = Path(base)
        for name in names:
            app_name = name if name.lower().endswith(".app") else f"{name}.app"
            candidates.append(base_path / app_name)
            candidates.extend(_versioned_macos_bundle_candidates(base_path, name))
    return tuple(_unique_paths(candidates))


def _candidate_from_path(app_id: str, path: Path, *, system: str, source: str) -> AppCandidate:
    target = Path(os.path.expandvars(os.path.expanduser(str(path))))
    target_type = _target_type(target)
    executable = _path_is_executable_target(app_id, target, system)
    confidence = "verified" if executable else "high" if target.exists() else "not_found"
    notes = ()
    if target_type == "app_bundle" and executable:
        notes = ("Found macOS app bundle.",)
    elif target.exists() and not executable:
        notes = ("Found path, but executable entry point was not verified.",)
    return AppCandidate(
        app_id=app_id,
        display_name=APP_DISPLAY_NAMES.get(app_id, app_id),
        path=str(target),
        source=source,
        confidence=confidence,
        exists=target.exists(),
        executable=executable,
        target_type=target_type,
        notes=notes,
    )


def _target_type(path: Path) -> str:
    if path.suffix.lower() == ".app":
        return "app_bundle"
    if path.suffix.lower() in {".exe", ".bat", ".cmd", ".sh"}:
        return "file"
    if path.is_dir():
        return "directory"
    return "file" if path.name and "." in path.name else "directory"


def _path_is_executable_target(app_id: str, path: Path, system: str) -> bool:
    if not _path_name_matches_app(app_id, path):
        return False
    if path.suffix.lower() == ".app":
        return _macos_bundle_executable(app_id, path) is not None
    if path.is_file():
        return os.access(path, os.X_OK) or system == "Windows"
    if app_id == "varac" and path.is_dir():
        return (path / "VarAC.exe").exists() or (path / "VarAC.db").exists()
    return False


def _macos_bundle_executable(app_id: str, bundle: Path) -> Optional[Path]:
    for name in _command_names(app_id):
        stem = Path(name).stem
        for candidate_name in (stem, stem.lower(), stem.upper()):
            candidate = bundle / "Contents" / "MacOS" / candidate_name
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return candidate
    return None


def _command_names(app_id: str) -> Tuple[str, ...]:
    if app_id == "js8call":
        return ("JS8Call", "js8call")
    if app_id == "js8spotter":
        return ("JS8Spotter", "js8spotter")
    if app_id == "commstat":
        return ("CommStat", "commstat")
    if app_id == "varac":
        return ("VarAC", "varac", "VarAC.exe")
    return (APP_DISPLAY_NAMES.get(app_id, app_id), app_id)


def _default_port_for_index(defaults: Sequence[int], index: int) -> int:
    if index < len(defaults):
        return int(defaults[index])
    return int(defaults[-1]) + (index - len(defaults) + 1)


def _first_available_port(
    start_port: int,
    host: str,
    used_ports: set,
    busy_checker: Callable[[str, int], bool],
) -> int:
    candidate = int(start_port)
    while candidate < 65535:
        if candidate not in used_ports and not busy_checker(host, candidate):
            return candidate
        candidate += 1
    raise RuntimeError("No available localhost port could be proposed.")


def _port_note(service: str, preferred: int, assigned: int, conflict: bool, *, conflict_checked: bool = True) -> str:
    if not conflict_checked:
        return f"{service} uses default UDP port {preferred}; UDP conflict probing is deferred."
    if conflict:
        return f"{service} preferred port {preferred} is busy; propose {assigned}."
    return f"{service} uses default port {preferred}."


def _service_protocol(service: str) -> str:
    return "udp" if service.endswith("_udp") else "tcp"


def _versioned_macos_bundle_candidates(base_dir: Path, name: str) -> Tuple[Path, ...]:
    if not base_dir.is_dir():
        return ()
    stem = Path(name).stem.strip().lower()
    matches = []
    try:
        children = tuple(base_dir.iterdir())
    except OSError:
        return ()
    for child in children:
        child_name = child.name.lower()
        if child.suffix.lower() == ".app" and child_name.startswith(stem) and child.is_dir():
            matches.append(child)
    return tuple(matches)


def _path_name_matches_app(app_id: str, path: Path) -> bool:
    if app_id == "varac":
        lowered = path.name.lower()
        return lowered.startswith("varac") or (path / "VarAC.exe").exists() or (path / "VarAC.db").exists()
    allowed_names = {Path(name).stem.lower() for name in _command_names(app_id)}
    stem = path.stem.lower()
    return any(stem == allowed or stem.startswith(f"{allowed}-") for allowed in allowed_names)


def _instance_name(index: int) -> str:
    if index < len(DEFAULT_RADIO_INSTANCE_NAMES):
        return DEFAULT_RADIO_INSTANCE_NAMES[index]
    return f"fio-{index + 1}"


def _selected_js8_settings(settings: Mapping[str, str]) -> Dict[str, str]:
    interesting = {
        "Rig",
        "CATNetworkPort",
        "TCPEnabled",
        "TCPServer",
        "TCPServerPort",
        "TCPMaxConnections",
        "UDPEnabled",
        "UDPServerPort",
        "SaveDir",
        "MyCall",
        "MyGrid",
    }
    return {key: str(value) for key, value in settings.items() if key in interesting and str(value).strip()}


def _unique_paths(paths: Iterable[Path]) -> Tuple[Path, ...]:
    out = []
    seen = set()
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
    return tuple(out)
