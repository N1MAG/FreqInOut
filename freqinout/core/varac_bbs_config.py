from __future__ import annotations

import configparser
import json
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import List, Mapping, Optional


def normalize_callsign(value: object) -> str:
    clean = str(value or "").strip().upper()
    clean = re.sub(r"^[^A-Z0-9/]+|[^A-Z0-9/]+$", "", clean)
    return clean


def base_callsign(value: object) -> str:
    return normalize_callsign(value).split("/", 1)[0]


def callsigns_match(candidate: object, expected: object) -> bool:
    left = normalize_callsign(candidate)
    right = normalize_callsign(expected)
    if not left or not right:
        return False
    return left == right or base_callsign(left) == base_callsign(right)


def parse_varac_bool(value: object, default: bool = False) -> bool:
    txt = str(value or "").strip().upper()
    if txt in {"ON", "TRUE", "YES", "1"}:
        return True
    if txt in {"OFF", "FALSE", "NO", "0"}:
        return False
    return bool(default)


def parse_callsign_list(value: object) -> List[str]:
    parts: List[str] = []
    if isinstance(value, (list, tuple, set)):
        tokens = [str(token or "") for token in value]
    else:
        raw = str(value or "")
        tokens = raw.replace("\n", ",").replace(";", ",").split(",")
    for token in tokens:
        clean = normalize_callsign(token)
        if clean:
            parts.append(clean)
    seen: set[str] = set()
    ordered: List[str] = []
    for callsign in parts:
        if callsign in seen:
            continue
        seen.add(callsign)
        ordered.append(callsign)
    return ordered


def format_callsign_list(value: object) -> str:
    return ",".join(parse_callsign_list(value))


def locate_varac_ini_path(*candidates: object) -> str:
    checked: List[Path] = []
    for candidate in candidates:
        txt = str(candidate or "").strip()
        if not txt:
            continue
        path = Path(txt).expanduser()
        checked.append(path)
        if path.is_file():
            return str(path)
        if path.is_dir():
            for name in ("VarAC.ini", "varac.ini"):
                maybe = path / name
                checked.append(maybe)
                if maybe.is_file():
                    return str(maybe)
    return ""


def _resolve_section_name(parser: configparser.ConfigParser, wanted: str) -> str | None:
    target = str(wanted or "").strip().lower()
    if not target:
        return None
    for section_name in parser.sections():
        if section_name.lower() == target:
            return section_name
    return None


def _get_section_value(section: configparser.SectionProxy, key: str, default: object = "") -> object:
    target = str(key or "").strip().lower()
    if not target:
        return default
    for existing_key, value in section.items():
        if str(existing_key or "").strip().lower() == target:
            return value
    return default


def load_varac_bbs_config(ini_path: object) -> Mapping[str, object]:
    resolved = locate_varac_ini_path(ini_path)
    if not resolved:
        raise FileNotFoundError("VarAC.ini not found")
    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    with Path(resolved).open("r", encoding="utf-8", errors="replace") as handle:
        parser.read_file(handle)
    section_name = _resolve_section_name(parser, "BBS")
    if section_name is None:
        raise KeyError("VarAC.ini does not contain a [BBS] section")

    section = parser[section_name]
    return {
        "ini_path": resolved,
        "enable_bbs": parse_varac_bool(_get_section_value(section, "EnableBBS", ""), False),
        "bbs_directory": str(_get_section_value(section, "BBSDirectory", "") or "").strip(),
        "limit_access": parse_varac_bool(_get_section_value(section, "LimitAccessToCallsigns", ""), False),
        "allowed_callsigns": parse_callsign_list(_get_section_value(section, "LimitAccessToCallsignsList", "")),
        "announce": parse_varac_bool(_get_section_value(section, "Announce", ""), False),
    }


def bbs_summary_text(values: Mapping[str, object]) -> str:
    enabled = parse_varac_bool(values.get("enable_bbs"), False)
    limited = parse_varac_bool(values.get("limit_access"), False)
    announce = parse_varac_bool(values.get("announce"), False)
    allowed = parse_callsign_list(values.get("allowed_callsigns", ""))
    return (
        f"BBS {'on' if enabled else 'off'}, "
        f"Access {'limited' if limited else 'open'}, "
        f"{len(allowed)} allowed, "
        f"Announce {'on' if announce else 'off'}"
    )


@dataclass(frozen=True)
class VaracIniSyncState:
    path: str
    size: int
    mtime_ns: int
    digest: str


def get_varac_ini_sync_state(ini_path: object) -> VaracIniSyncState:
    resolved = locate_varac_ini_path(ini_path)
    if not resolved:
        raise FileNotFoundError("VarAC.ini not found")
    path = Path(resolved)
    stat = path.stat()
    content = path.read_bytes()
    return VaracIniSyncState(
        path=str(path),
        size=int(stat.st_size),
        mtime_ns=int(stat.st_mtime_ns),
        digest=sha256(content).hexdigest(),
    )


def varac_ini_sync_state_to_json(state: Optional[VaracIniSyncState]) -> str:
    if not state:
        return ""
    try:
        return json.dumps(
            {
                "path": state.path,
                "size": state.size,
                "mtime_ns": state.mtime_ns,
                "digest": state.digest,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    except Exception:
        return ""


def varac_ini_sync_state_from_json(value: object) -> Optional[VaracIniSyncState]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    path = str(data.get("path", "") or "").strip()
    digest = str(data.get("digest", "") or "").strip()
    if not path or not digest:
        return None
    try:
        size = int(data.get("size", 0) or 0)
    except Exception:
        size = 0
    try:
        mtime_ns = int(data.get("mtime_ns", 0) or 0)
    except Exception:
        mtime_ns = 0
    return VaracIniSyncState(path=path, size=size, mtime_ns=mtime_ns, digest=digest)


def varac_ini_sync_state_matches(known: object, current: object) -> bool:
    known_state = known if isinstance(known, VaracIniSyncState) else varac_ini_sync_state_from_json(known)
    current_state = current if isinstance(current, VaracIniSyncState) else varac_ini_sync_state_from_json(current)
    if known_state is None or current_state is None:
        return False
    return (
        known_state.path == current_state.path
        and known_state.size == current_state.size
        and known_state.mtime_ns == current_state.mtime_ns
        and known_state.digest == current_state.digest
    )


def write_varac_bbs_config(
    ini_path: object,
    *,
    enable_bbs: bool,
    bbs_directory: str,
    limit_access: bool,
    allowed_callsigns: object,
    announce: bool,
    expected_sync_state: object = None,
) -> VaracIniSyncState:
    resolved = locate_varac_ini_path(ini_path)
    if not resolved:
        raise FileNotFoundError("VarAC.ini not found")

    path = Path(resolved)
    current_state = get_varac_ini_sync_state(path)
    known_state = expected_sync_state if isinstance(expected_sync_state, VaracIniSyncState) else varac_ini_sync_state_from_json(expected_sync_state)
    if known_state is not None and not varac_ini_sync_state_matches(known_state, current_state):
        raise RuntimeError("VarAC.ini changed since it was loaded")

    raw_text = path.read_text(encoding="utf-8", errors="replace")
    lines = raw_text.splitlines()
    bbs_lines = [
        "[BBS]",
        f"EnableBBS={'ON' if bool(enable_bbs) else 'OFF'}",
        f"BBSDirectory={str(bbs_directory or '').strip()}",
        f"LimitAccessToCallsigns={'ON' if bool(limit_access) else 'OFF'}",
        f"LimitAccessToCallsignsList={format_callsign_list(allowed_callsigns)}",
        f"Announce={'ON' if bool(announce) else 'OFF'}",
    ]
    replacement = "\n".join(bbs_lines)
    section_start = None
    section_end = None
    for idx, line in enumerate(lines):
        if line.strip().lower() == "[bbs]":
            section_start = idx
            for j in range(idx + 1, len(lines)):
                nxt = lines[j].strip()
                if nxt.startswith("[") and nxt.endswith("]"):
                    section_end = j
                    break
            if section_end is None:
                section_end = len(lines)
            break
    if section_start is None:
        if raw_text and not raw_text.endswith(("\n", "\r")):
            raw_text += "\n"
        if raw_text and not raw_text.endswith("\n\n"):
            raw_text += "\n"
        updated_text = raw_text + replacement + "\n"
    else:
        before = "\n".join(lines[:section_start]).rstrip()
        after = "\n".join(lines[section_end:]).lstrip()
        parts: List[str] = []
        if before:
            parts.append(before)
        parts.append(replacement)
        if after:
            parts.append(after)
        updated_text = "\n\n".join(parts).rstrip() + "\n"
    path.write_text(updated_text, encoding="utf-8")
    return get_varac_ini_sync_state(path)
