from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from pathlib import PureWindowsPath
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from freqinout.core.logger import log
from freqinout.core.nbems_compose import safe_varac_bbs_filename
from freqinout.core.varac_log_parser import parse_varac_event_timestamp_to_epoch
from freqinout.core.varac_bbs_config import parse_callsign_list
from freqinout.core.varac_guard import resolve_varac_traffic_log_paths

DEFAULT_ACCESS_CODE_ITERATIONS = 310_000
DEFAULT_FAILED_ATTEMPT_LIMIT = 3
DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS = 15 * 60
DEFAULT_COOLDOWN_SECONDS = 30 * 60
DEFAULT_IDLE_TIMEOUT_SECONDS = 10 * 60
DEFAULT_TRIGGER_MODE = "Command prefix"
DEFAULT_RETURN_MODE = "On disconnect"
DEFAULT_LOCATION_ID = "default"
DEFAULT_LOCATION_NAME = "Default"
DEFAULT_GLOBAL_CODE_POLICY = "Require for non-default locations"
DEFAULT_VIEW_MODE = "root"
MAX_PROCESSED_EVENT_KEYS = 256
MAX_MANIFEST_FILENAME_LENGTH = 180
MAX_DB_ROWS_PER_SCAN = 256
DEFAULT_FLAMP_QUEUE_HELPER_NAME = "BBS_QUEUE_LIST.txt"
DEFAULT_FLAMP_BLOCK_PREFIX = "BBS_BLOCK_LIST"
DEFAULT_FLAMP_FILE_PREFIX = "BBS"

EVENT_TS_RE = re.compile(r"^(?P<stamp>\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\s+-\s+(?P<body>.*)$")
COMMAND_RE = re.compile(r"\bBBS\s+OPEN\s+([A-Z0-9][A-Z0-9_.:/+\-]{0,63})\b", re.IGNORECASE)
DISCONNECT_RE = re.compile(r"\bDISCONNECTED(?:\s+(?:FROM|BY|TO)\s+[A-Z0-9/+\-]+)?\b", re.IGNORECASE)
CALLSIGN_PATTERNS = (
    r"\bFROM\b\s*[:=]?\s*([A-Z0-9/]{3,15})",
    r"\bDE\b\s*([A-Z0-9/]{3,15})",
    r"\bSENDER\b\s*[:=]?\s*([A-Z0-9/]{3,15})",
    r"\bCALLSIGN\b\s*[:=]?\s*([A-Z0-9/]{3,15})",
)
CALLSIGN_RE = re.compile(r"\b([A-Z0-9/]{3,15})\b")
IGNORED_CALLSIGN_TOKENS = {
    "BBS",
    "OPEN",
    "DISCONNECTED",
    "CONNECTION",
    "CLOSED",
    "SESSION",
    "FROM",
    "DE",
    "CALLSIGN",
    "SENDER",
}
ROOT_CMD_RE = re.compile(r"^(ROOT|LOCK|EXIT|BACK)\s*$", re.IGNORECASE)
LIST_Q_RE = re.compile(r"^LIST\s+Q\s*$", re.IGNORECASE)
LIST_BLOCKS_RE = re.compile(r"^(?:LIST\s+BLKS|LIST\s+BLOCKS|BLKS\?)\s+([A-F0-9]{4})\s*$", re.IGNORECASE)
BLOCK_REQUEST_RE = re.compile(r"^(?:REQ\s+)?BLK\s+([0-9,\s]+)\s+([A-F0-9]{4})\s*$", re.IGNORECASE)
ALIAS_RE = re.compile(r"^[A-Z0-9][A-Z0-9_.:/+\-]{0,31}$")


@dataclass(frozen=True)
class VaultLocation:
    id: str
    name: str
    source_dir: str
    enabled: bool = True
    inherit_global_allowed_callsigns: bool = True
    allowed_callsigns: Tuple[str, ...] = ()
    access_code_hash: str = ""
    access_code_salt: str = ""
    access_code_iterations: int = DEFAULT_ACCESS_CODE_ITERATIONS
    alias: str = ""
    description: str = ""
    list_in_root_menu: bool = True
    visibility_rule: str = "Public"
    open_rule: str = "Public"


@dataclass(frozen=True)
class VaultRuntimeState:
    current_location_id: str = DEFAULT_LOCATION_ID
    current_session_callsign: str = ""
    current_session_qso_guid: str = ""
    current_view_mode: str = DEFAULT_VIEW_MODE
    current_view_label: str = DEFAULT_LOCATION_NAME
    previous_location_id: str = DEFAULT_LOCATION_ID
    previous_view_mode: str = DEFAULT_VIEW_MODE
    previous_view_label: str = DEFAULT_LOCATION_NAME
    current_overlay_file: str = ""
    processed_event_keys: Tuple[str, ...] = ()
    cooldowns: Mapping[str, Mapping[str, float]] = field(default_factory=dict)
    failed_attempts: Mapping[str, Sequence[float]] = field(default_factory=dict)
    last_publish_manifest_path: str = ""
    last_publish_ts: float = 0.0
    last_action: str = ""
    last_request_ts: float = 0.0
    last_error: str = ""
    unmanaged_live_files: Tuple[str, ...] = ()
    last_datastream_id: int = 0


@dataclass(frozen=True)
class VaultPublishManifestEntry:
    source_name: str
    live_name: str
    size: int
    mtime_ns: int
    sha256: str = ""


@dataclass(frozen=True)
class VaultPublishResult:
    changed: bool
    published_count: int
    removed_count: int
    unmanaged_live_files: Tuple[str, ...]
    manifest_path: str
    ignored_directories: int = 0


@dataclass(frozen=True)
class VaultLogEvent:
    timestamp_utc: float
    kind: str
    sender: str
    body: str
    code_text: str = ""
    alias: str = ""
    raw_line: str = ""
    log_path: str = ""


@dataclass(frozen=True)
class VaultDbEvent:
    row_id: int
    timestamp_utc: float
    qso_guid: str
    remote_callsign: str
    my_callsign: str
    entry_callsign: str
    entry_text: str
    kind: str
    alias: str = ""
    code_text: str = ""
    queue_id: str = ""
    block_numbers: Tuple[int, ...] = ()


@dataclass(frozen=True)
class VaultActionResult:
    action: str
    success: bool
    summary: str
    runtime_state: VaultRuntimeState
    publish_result: Optional[VaultPublishResult] = None


@dataclass(frozen=True)
class VaracBbsVaultRunResult:
    enabled: bool
    scanned_events: int
    processed_events: int
    published: bool
    active_location_id: str
    current_session_callsign: str
    summary: str


@dataclass(frozen=True)
class _VirtualFile:
    name: str
    content: str


def _normalize_callsign(value: object) -> str:
    return str(value or "").strip().upper()


def _clean_location_name(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_location_alias(value: object, fallback_name: object = "") -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        raw = re.sub(r"[^A-Z0-9]+", "", str(fallback_name or "").strip().upper())
    raw = re.sub(r"[^A-Z0-9_.:/+\-]+", "", raw)
    if raw in {"ROOT", "LOCK", "EXIT", "BACK", "LIST", "BLK", "REQ", "BBS", "OPEN"}:
        raw = f"{raw}1"
    return raw[:32]


def _location_id_from_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "-", str(name or "").strip().lower()).strip("-")
    return cleaned or DEFAULT_LOCATION_ID


def _ensure_location_id(existing_ids: Iterable[str], preferred_name: str, raw_id: object = "") -> str:
    existing = {str(item or "").strip() for item in existing_ids if str(item or "").strip()}
    base = str(raw_id or "").strip() or _location_id_from_name(preferred_name)
    if base not in existing:
        return base
    counter = 2
    while f"{base}-{counter}" in existing:
        counter += 1
    return f"{base}-{counter}"


def _resolve_path(value: object) -> Optional[Path]:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        win_match = re.match(r"^([A-Za-z]):[\\/](.*)$", txt)
        if win_match and os.name != "nt":
            drive = win_match.group(1).lower()
            rest = PureWindowsPath(txt).parts[1:]
            wine_prefix = Path(os.environ.get("WINEPREFIX", "~/.wine")).expanduser()
            return wine_prefix / f"drive_{drive}" / Path(*rest)
        return Path(txt).expanduser()
    except Exception:
        return None


def _json_safe_load(value: object, default):
    if isinstance(value, (dict, list)):
        return value
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        parsed = json.loads(raw)
    except Exception:
        return default
    return parsed


def _state_key(event: VaultLogEvent) -> str:
    return "|".join(
        [
            event.log_path,
            str(int(event.timestamp_utc or 0.0)),
            event.kind,
            event.sender,
            event.alias.upper(),
            event.code_text.upper(),
        ]
    )


def _read_tail(path: Path, max_bytes: int = 65536) -> str:
    try:
        with path.open("rb") as handle:
            try:
                handle.seek(-max_bytes, os.SEEK_END)
            except OSError:
                handle.seek(0)
            return handle.read().decode("utf-8", errors="replace")
    except Exception as exc:
        log.debug("varac_bbs_vault: failed to tail %s: %s", path, exc)
        return ""


def _parse_timestamp(stamp: str) -> float:
    return parse_varac_event_timestamp_to_epoch(stamp)


def _parse_db_timestamp(value: object) -> float:
    txt = str(value or "").strip()
    if not txt:
        return 0.0
    txt = txt.replace("T", " ").replace("Z", "")
    if "." in txt:
        head, frac = txt.split(".", 1)
        digits = "".join(ch for ch in frac if ch.isdigit())
        if digits:
            txt = f"{head}.{digits[:6].ljust(6, '0')}"
        else:
            txt = head
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(txt, fmt).replace(tzinfo=dt.timezone.utc).timestamp()
        except Exception:
            continue
    return 0.0


def _split_log_events(text: str) -> List[str]:
    events: List[str] = []
    current: List[str] = []
    for raw_line in str(text or "").splitlines():
        if EVENT_TS_RE.match(raw_line):
            if current:
                events.append("\n".join(current))
            current = [raw_line]
        elif current:
            current.append(raw_line)
    if current:
        events.append("\n".join(current))
    return events


def _extract_sender(body: str) -> str:
    upper = str(body or "").upper()
    for pattern in CALLSIGN_PATTERNS:
        match = re.search(pattern, upper)
        if match:
            return _normalize_callsign(match.group(1))
    tokens = [tok for tok in CALLSIGN_RE.findall(upper) if tok not in IGNORED_CALLSIGN_TOKENS]
    if tokens:
        return _normalize_callsign(tokens[0])
    return ""


def _extract_log_message_text(body: str) -> str:
    payloads: List[str] = []
    for line in str(body or "").splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.search(r"\b[A-Z0-9/]{3,15}>\s*(.*)$", line, re.IGNORECASE)
        payloads.append(str(match.group(1) if match else line).strip())
    return " ".join(payloads).strip()


def parse_vault_log_events(
    text: str,
    *,
    trigger_mode: str = DEFAULT_TRIGGER_MODE,
    log_path: str = "",
    alias_map: Optional[Mapping[str, str]] = None,
) -> List[VaultLogEvent]:
    events: List[VaultLogEvent] = []
    exact_mode = str(trigger_mode or DEFAULT_TRIGGER_MODE).strip().lower() == "exact code only"
    aliases = alias_map or {}
    for block in _split_log_events(text):
        lines = [line for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        match = EVENT_TS_RE.match(lines[0])
        if not match:
            continue
        body = "\n".join([match.group("body")] + lines[1:]).strip()
        sender = _extract_sender(body)
        message_text = _extract_log_message_text(body)
        code_text = ""
        alias = ""
        kind = ""
        alias, code_text = _extract_alias_request(message_text, aliases)
        if alias:
            kind = "open_alias"
        else:
            command_match = COMMAND_RE.search(body)
            if command_match:
                code_text = str(command_match.group(1) or "").strip()
                kind = "unlock"
        if not kind and ROOT_CMD_RE.match(message_text):
            kind = "root_return"
        if not kind and exact_mode:
            stripped = " ".join(message_text.split())
            alias, code_text = _extract_alias_request(stripped, aliases)
            if alias:
                kind = "open_alias"
            elif ROOT_CMD_RE.match(stripped):
                kind = "root_return"
            elif stripped:
                code_text = stripped
                kind = "unlock"
        elif not kind and DISCONNECT_RE.search(body):
            kind = "disconnect"
        if not kind:
            continue
        events.append(
            VaultLogEvent(
                timestamp_utc=_parse_timestamp(match.group("stamp")),
                kind=kind,
                sender=sender,
                body=body,
                code_text=code_text,
                alias=alias,
                raw_line=block,
                log_path=log_path,
            )
        )
    return events


def hash_access_code(code: str, *, salt: Optional[str] = None, iterations: int = DEFAULT_ACCESS_CODE_ITERATIONS) -> Dict[str, object]:
    clean = str(code or "").strip()
    if not clean:
        return {
            "access_code_hash": "",
            "access_code_salt": "",
            "access_code_iterations": int(iterations),
        }
    raw_salt = bytes.fromhex(salt) if salt else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", clean.encode("utf-8"), raw_salt, int(iterations))
    return {
        "access_code_hash": digest.hex(),
        "access_code_salt": raw_salt.hex(),
        "access_code_iterations": int(iterations),
    }


def verify_access_code(code: str, *, access_code_hash: str, access_code_salt: str, access_code_iterations: int) -> bool:
    clean = str(code or "").strip()
    if not clean or not access_code_hash or not access_code_salt:
        return False
    try:
        expected = bytes.fromhex(str(access_code_hash).strip())
        salt = bytes.fromhex(str(access_code_salt).strip())
        iterations = int(access_code_iterations or DEFAULT_ACCESS_CODE_ITERATIONS)
    except Exception:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", clean.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def load_vault_locations(value: object) -> List[VaultLocation]:
    parsed = _json_safe_load(value, [])
    if not isinstance(parsed, list):
        return []
    locations: List[VaultLocation] = []
    existing_ids: List[str] = []
    for row in parsed:
        if not isinstance(row, dict):
            continue
        name = _clean_location_name(row.get("name", ""))
        if not name:
            continue
        location_id = _ensure_location_id(existing_ids, name, row.get("id", ""))
        existing_ids.append(location_id)
        locations.append(
            VaultLocation(
                id=location_id,
                name=name,
                source_dir=str(row.get("source_dir", "") or "").strip(),
                enabled=bool(row.get("enabled", True)),
                inherit_global_allowed_callsigns=bool(row.get("inherit_global_allowed_callsigns", True)),
                allowed_callsigns=tuple(parse_callsign_list(row.get("allowed_callsigns", []))),
                access_code_hash=str(row.get("access_code_hash", "") or "").strip(),
                access_code_salt=str(row.get("access_code_salt", "") or "").strip(),
                access_code_iterations=int(row.get("access_code_iterations", DEFAULT_ACCESS_CODE_ITERATIONS) or DEFAULT_ACCESS_CODE_ITERATIONS),
                alias=normalize_location_alias(row.get("alias", ""), name),
                description=str(row.get("description", "") or "").strip(),
                list_in_root_menu=bool(row.get("list_in_root_menu", True)),
                visibility_rule=str(row.get("visibility_rule", "Public") or "Public").strip() or "Public",
                open_rule=str(row.get("open_rule", "Public") or "Public").strip() or "Public",
            )
        )
    return locations


def vault_locations_to_data(locations: Sequence[VaultLocation]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for location in locations:
        out.append(
            {
                "id": location.id,
                "name": location.name,
                "source_dir": location.source_dir,
                "enabled": bool(location.enabled),
                "inherit_global_allowed_callsigns": bool(location.inherit_global_allowed_callsigns),
                "allowed_callsigns": list(location.allowed_callsigns),
                "access_code_hash": location.access_code_hash,
                "access_code_salt": location.access_code_salt,
                "access_code_iterations": int(location.access_code_iterations or DEFAULT_ACCESS_CODE_ITERATIONS),
                "alias": location.alias,
                "description": location.description,
                "list_in_root_menu": bool(location.list_in_root_menu),
                "visibility_rule": location.visibility_rule,
                "open_rule": location.open_rule,
            }
        )
    return out


def load_vault_runtime_state(value: object) -> VaultRuntimeState:
    parsed = _json_safe_load(value, {})
    if not isinstance(parsed, dict):
        return VaultRuntimeState()
    cooldowns = parsed.get("cooldowns", {})
    if not isinstance(cooldowns, dict):
        cooldowns = {}
    failed_attempts = parsed.get("failed_attempts", {})
    if not isinstance(failed_attempts, dict):
        failed_attempts = {}
    return VaultRuntimeState(
        current_location_id=str(parsed.get("current_location_id", DEFAULT_LOCATION_ID) or DEFAULT_LOCATION_ID).strip() or DEFAULT_LOCATION_ID,
        current_session_callsign=_normalize_callsign(parsed.get("current_session_callsign", "")),
        current_session_qso_guid=str(parsed.get("current_session_qso_guid", "") or "").strip(),
        current_view_mode=str(parsed.get("current_view_mode", DEFAULT_VIEW_MODE) or DEFAULT_VIEW_MODE).strip() or DEFAULT_VIEW_MODE,
        current_view_label=str(parsed.get("current_view_label", DEFAULT_LOCATION_NAME) or DEFAULT_LOCATION_NAME).strip() or DEFAULT_LOCATION_NAME,
        previous_location_id=str(parsed.get("previous_location_id", DEFAULT_LOCATION_ID) or DEFAULT_LOCATION_ID).strip() or DEFAULT_LOCATION_ID,
        previous_view_mode=str(parsed.get("previous_view_mode", DEFAULT_VIEW_MODE) or DEFAULT_VIEW_MODE).strip() or DEFAULT_VIEW_MODE,
        previous_view_label=str(parsed.get("previous_view_label", DEFAULT_LOCATION_NAME) or DEFAULT_LOCATION_NAME).strip() or DEFAULT_LOCATION_NAME,
        current_overlay_file=str(parsed.get("current_overlay_file", "") or "").strip(),
        processed_event_keys=tuple(
            str(item or "").strip()
            for item in parsed.get("processed_event_keys", [])
            if str(item or "").strip()
        ),
        cooldowns={str(key or "").strip(): dict(value) for key, value in cooldowns.items() if str(key or "").strip() and isinstance(value, dict)},
        failed_attempts={
            str(key or "").strip(): [float(ts) for ts in value if isinstance(ts, (int, float))]
            for key, value in failed_attempts.items()
            if str(key or "").strip() and isinstance(value, list)
        },
        last_publish_manifest_path=str(parsed.get("last_publish_manifest_path", "") or "").strip(),
        last_publish_ts=float(parsed.get("last_publish_ts", 0.0) or 0.0),
        last_action=str(parsed.get("last_action", "") or "").strip(),
        last_request_ts=float(parsed.get("last_request_ts", 0.0) or 0.0),
        last_error=str(parsed.get("last_error", "") or "").strip(),
        unmanaged_live_files=tuple(
            str(item or "").strip()
            for item in parsed.get("unmanaged_live_files", [])
            if str(item or "").strip()
        ),
        last_datastream_id=int(parsed.get("last_datastream_id", 0) or 0),
    )


def vault_runtime_state_to_data(state: VaultRuntimeState) -> Dict[str, object]:
    return {
        "current_location_id": state.current_location_id,
        "current_session_callsign": state.current_session_callsign,
        "current_session_qso_guid": state.current_session_qso_guid,
        "current_view_mode": state.current_view_mode,
        "current_view_label": state.current_view_label,
        "previous_location_id": state.previous_location_id,
        "previous_view_mode": state.previous_view_mode,
        "previous_view_label": state.previous_view_label,
        "current_overlay_file": state.current_overlay_file,
        "processed_event_keys": list(state.processed_event_keys),
        "cooldowns": {str(key): dict(value) for key, value in state.cooldowns.items()},
        "failed_attempts": {str(key): list(value) for key, value in state.failed_attempts.items()},
        "last_publish_manifest_path": state.last_publish_manifest_path,
        "last_publish_ts": float(state.last_publish_ts or 0.0),
        "last_action": state.last_action,
        "last_request_ts": float(state.last_request_ts or 0.0),
        "last_error": state.last_error,
        "unmanaged_live_files": list(state.unmanaged_live_files),
        "last_datastream_id": int(state.last_datastream_id or 0),
    }


def compute_default_managed_root(live_bbs_dir: object) -> str:
    live_path = _resolve_path(live_bbs_dir)
    if live_path is None:
        return ""
    return str(live_path.parent / "FIO_BBS_Vault")


def _managed_root_paths(managed_root: object) -> Dict[str, Path]:
    root = _resolve_path(managed_root)
    if root is None:
        raise ValueError("Managed root path is required")
    return {
        "root": root,
        "locations": root / "locations",
        "default": root / "locations" / DEFAULT_LOCATION_NAME,
        "runtime": root / "runtime",
        "manifests": root / "runtime" / "manifests",
        "tmp": root / "runtime" / "tmp",
        "logs": root / "runtime" / "logs",
    }


def initialize_managed_root(managed_root: object) -> Dict[str, str]:
    paths = _managed_root_paths(managed_root)
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return {name: str(path) for name, path in paths.items()}


def import_live_bbs_to_default_location(live_bbs_dir: object, default_location_dir: object) -> int:
    live_dir = _resolve_path(live_bbs_dir)
    default_dir = _resolve_path(default_location_dir)
    if live_dir is None or default_dir is None:
        return 0
    if not live_dir.exists() or not live_dir.is_dir():
        return 0
    default_dir.mkdir(parents=True, exist_ok=True)
    imported = 0
    for child in sorted(live_dir.iterdir(), key=lambda item: item.name.lower()):
        if not child.is_file():
            continue
        dst = default_dir / child.name
        if dst.exists():
            stem = dst.stem
            suffix = dst.suffix
            counter = 2
            while dst.exists():
                dst = default_dir / f"{stem}-{counter}{suffix}"
                counter += 1
        shutil.copy2(child, dst)
        imported += 1
    return imported


def _manifest_path_for(managed_root: object) -> Path:
    paths = _managed_root_paths(managed_root)
    return paths["manifests"] / "current_publish_manifest.json"


def _audit_log_path_for(managed_root: object) -> Path:
    paths = _managed_root_paths(managed_root)
    return paths["logs"] / "vault_audit.jsonl"


def read_publish_manifest(path: object) -> List[VaultPublishManifestEntry]:
    manifest_path = _resolve_path(path)
    if manifest_path is None or not manifest_path.exists():
        return []
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8") or "[]")
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    entries: List[VaultPublishManifestEntry] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        live_name = str(item.get("live_name", "") or "").strip()
        source_name = str(item.get("source_name", "") or "").strip()
        if not live_name or not source_name:
            continue
        try:
            size = int(item.get("size", 0) or 0)
        except Exception:
            size = 0
        try:
            mtime_ns = int(item.get("mtime_ns", 0) or 0)
        except Exception:
            mtime_ns = 0
        entries.append(
            VaultPublishManifestEntry(
                source_name=source_name,
                live_name=live_name,
                size=size,
                mtime_ns=mtime_ns,
                sha256=str(item.get("sha256", "") or "").strip(),
            )
        )
    return entries


def write_publish_manifest(path: object, entries: Sequence[VaultPublishManifestEntry]) -> str:
    manifest_path = _resolve_path(path)
    if manifest_path is None:
        raise ValueError("Manifest path is required")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(entry) for entry in entries]
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return str(manifest_path)


def scan_location_files(source_dir: object) -> Tuple[List[Path], int]:
    source_path = _resolve_path(source_dir)
    if source_path is None or not source_path.exists() or not source_path.is_dir():
        return [], 0
    files: List[Path] = []
    ignored_dirs = 0
    for child in sorted(source_path.iterdir(), key=lambda item: item.name.lower()):
        if child.is_dir():
            ignored_dirs += 1
            continue
        if child.is_file():
            files.append(child)
    return files, ignored_dirs


def _location_folder_candidates(location: VaultLocation, managed_root: object) -> List[Path]:
    try:
        locations_root = _managed_root_paths(managed_root)["locations"]
    except Exception:
        return []
    names = [
        str(location.name or "").strip(),
        str(location.alias or "").strip(),
        str(location.id or "").strip(),
    ]
    candidates: List[Path] = []
    seen: set[str] = set()
    for name in names:
        if not name:
            continue
        candidate = locations_root / name
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(candidate)
    return candidates


def _effective_location_source_dir(location: VaultLocation, managed_root: object) -> object:
    configured_files, _ignored = scan_location_files(location.source_dir)
    if configured_files:
        return location.source_dir
    for candidate in _location_folder_candidates(location, managed_root):
        candidate_files, _candidate_ignored = scan_location_files(candidate)
        if candidate_files:
            return candidate
    return location.source_dir


def _with_filesystem_location_fallbacks(
    locations: Sequence[VaultLocation],
    managed_root: object,
    *,
    default_location_id: str,
) -> List[VaultLocation]:
    merged = list(locations)
    try:
        locations_root = _managed_root_paths(managed_root)["locations"]
    except Exception:
        return merged
    if not locations_root.exists() or not locations_root.is_dir():
        return merged

    existing_ids = [location.id for location in merged]
    existing_aliases = {normalize_location_alias(location.alias, location.name) for location in merged}
    existing_aliases.add("ROOT")
    for child in sorted(locations_root.iterdir(), key=lambda item: item.name.lower()):
        if not child.is_dir() or child.name == DEFAULT_LOCATION_NAME:
            continue
        alias = normalize_location_alias("", child.name)
        if not alias or alias in existing_aliases:
            continue
        files, _ignored = scan_location_files(child)
        if not files:
            continue
        location_id = _ensure_location_id(existing_ids, child.name, f"fs-{_location_id_from_name(child.name)}")
        existing_ids.append(location_id)
        existing_aliases.add(alias)
        merged.append(
            VaultLocation(
                id=location_id,
                name=child.name,
                source_dir=str(child),
                enabled=True,
                inherit_global_allowed_callsigns=True,
                allowed_callsigns=(),
                access_code_hash="",
                access_code_salt="",
                access_code_iterations=DEFAULT_ACCESS_CODE_ITERATIONS,
                alias=alias,
                description=f"Open {child.name}",
                list_in_root_menu=True,
                visibility_rule="Public",
                open_rule="Public",
            )
        )
    return merged


def build_publish_manifest(source_dir: object, *, virtual_files: Sequence[_VirtualFile] = ()) -> Tuple[List[VaultPublishManifestEntry], int]:
    files, ignored_dirs = scan_location_files(source_dir)
    manifest: List[VaultPublishManifestEntry] = []
    used_names: set[str] = set()

    def _unique_name(raw_name: str) -> str:
        safe_name = safe_varac_bbs_filename(raw_name, max_len=MAX_MANIFEST_FILENAME_LENGTH)
        if safe_name not in used_names:
            used_names.add(safe_name)
            return safe_name
        stem = Path(safe_name).stem
        suffix = Path(safe_name).suffix
        counter = 2
        candidate = f"{stem}-{counter}{suffix}"
        while candidate in used_names:
            counter += 1
            candidate = f"{stem}-{counter}{suffix}"
        used_names.add(candidate)
        return candidate

    for child in files:
        try:
            st = child.stat()
        except OSError:
            continue
        live_name = _unique_name(child.name)
        manifest.append(
            VaultPublishManifestEntry(
                source_name=child.name,
                live_name=live_name,
                size=int(st.st_size or 0),
                mtime_ns=int(st.st_mtime_ns or 0),
            )
        )
    for virtual in virtual_files:
        content = str(virtual.content or "")
        live_name = _unique_name(virtual.name)
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        manifest.append(
            VaultPublishManifestEntry(
                source_name=f"@virtual/{virtual.name}",
                live_name=live_name,
                size=len(content.encode("utf-8")),
                mtime_ns=0,
                sha256=digest,
            )
        )
    return manifest, ignored_dirs


def _hash_file(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _entry_map(entries: Sequence[VaultPublishManifestEntry]) -> Dict[str, VaultPublishManifestEntry]:
    return {entry.live_name: entry for entry in entries}


def _entries_equal(a: VaultPublishManifestEntry, b: VaultPublishManifestEntry, *, src_root: Optional[Path] = None, dst_root: Optional[Path] = None) -> bool:
    if a.size != b.size or a.mtime_ns != b.mtime_ns or a.source_name != b.source_name:
        return False
    if a.sha256 and b.sha256:
        return a.sha256 == b.sha256
    if a.source_name.startswith("@virtual/") or b.source_name.startswith("@virtual/"):
        return bool(a.sha256 and b.sha256 and a.sha256 == b.sha256)
    if src_root is None or dst_root is None:
        return True
    try:
        src_hash = _hash_file(src_root / a.source_name)
        dst_hash = _hash_file(dst_root / a.live_name)
    except Exception:
        return False
    return src_hash == dst_hash


def _publish_manifest_entries(
    entries: Sequence[VaultPublishManifestEntry],
    *,
    source_dir: object,
    live_bbs_dir: object,
    managed_root: object,
    virtual_files: Sequence[_VirtualFile] = (),
) -> VaultPublishResult:
    live_dir = _resolve_path(live_bbs_dir)
    if live_dir is None:
        raise ValueError("Live BBS directory is required")
    live_dir.mkdir(parents=True, exist_ok=True)
    src_root = _resolve_path(source_dir)
    manifest_path = _manifest_path_for(managed_root)
    previous_manifest = read_publish_manifest(manifest_path)
    previous_map = _entry_map(previous_manifest)
    next_map = _entry_map(entries)
    virtual_map = {f"@virtual/{entry.name}": entry for entry in virtual_files}

    published = 0
    for entry in entries:
        dst = live_dir / entry.live_name
        previous = previous_map.get(entry.live_name)
        if previous is not None and dst.exists() and _entries_equal(entry, previous, src_root=src_root, dst_root=live_dir):
            continue
        tmp_name = f".fio-vault-{uuid.uuid4().hex}.tmp"
        tmp_path = live_dir / tmp_name
        if entry.source_name.startswith("@virtual/"):
            virtual = virtual_map.get(entry.source_name)
            if virtual is None:
                continue
            tmp_path.write_text(str(virtual.content or ""), encoding="utf-8")
        else:
            if src_root is None:
                raise ValueError("Location source directory is required")
            shutil.copy2(src_root / entry.source_name, tmp_path)
        os.replace(tmp_path, dst)
        published += 1

    removed = 0
    for live_name, previous in previous_map.items():
        if live_name in next_map:
            continue
        target = live_dir / live_name
        if target.exists():
            try:
                target.unlink()
                removed += 1
            except OSError:
                pass

    write_publish_manifest(manifest_path, entries)

    unmanaged: List[str] = []
    tracked = set(next_map.keys())
    for child in sorted(live_dir.iterdir(), key=lambda item: item.name.lower()):
        if not child.is_file():
            continue
        if child.name.startswith(".fio-vault-"):
            continue
        if child.name not in tracked:
            unmanaged.append(child.name)

    changed = bool(published or removed or len(entries) != len(previous_manifest))
    return VaultPublishResult(
        changed=changed,
        published_count=published,
        removed_count=removed,
        unmanaged_live_files=tuple(unmanaged),
        manifest_path=str(manifest_path),
        ignored_directories=0,
    )


def publish_location(location: VaultLocation, *, live_bbs_dir: object, managed_root: object) -> VaultPublishResult:
    source_dir = _effective_location_source_dir(location, managed_root)
    entries, ignored_dirs = build_publish_manifest(source_dir)
    result = _publish_manifest_entries(
        entries,
        source_dir=source_dir,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
    )
    return VaultPublishResult(
        changed=result.changed,
        published_count=result.published_count,
        removed_count=result.removed_count,
        unmanaged_live_files=result.unmanaged_live_files,
        manifest_path=result.manifest_path,
        ignored_directories=ignored_dirs,
    )


def _append_audit_event(managed_root: object, payload: Mapping[str, object]) -> None:
    try:
        audit_path = _audit_log_path_for(managed_root)
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")
    except Exception as exc:
        log.debug("varac_bbs_vault: failed to append audit event: %s", exc)


def _sender_globally_allowed(sender: str, *, global_allowed_callsigns: Sequence[str], limit_access_enabled: bool) -> bool:
    normalized_sender = _normalize_callsign(sender)
    if not normalized_sender:
        return False
    if not limit_access_enabled:
        return True
    global_allowed = {item for item in parse_callsign_list(global_allowed_callsigns)}
    return not global_allowed or normalized_sender in global_allowed


def _location_allowed_subset(sender: str, location: VaultLocation) -> bool:
    normalized_sender = _normalize_callsign(sender)
    if not normalized_sender:
        return False
    if location.inherit_global_allowed_callsigns:
        return True
    allowed = {item for item in parse_callsign_list(location.allowed_callsigns)}
    return not allowed or normalized_sender in allowed


def _location_sender_allowed(
    sender: str,
    *,
    location: VaultLocation,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
) -> bool:
    return _sender_globally_allowed(
        sender,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
    ) and _location_allowed_subset(sender, location)


def _location_requires_code(location: VaultLocation, *, default_location_id: str, global_code_policy: str) -> bool:
    if location.id == default_location_id:
        return False
    open_rule = str(location.open_rule or "Public").strip()
    if open_rule == "Public":
        return False
    policy = str(global_code_policy or DEFAULT_GLOBAL_CODE_POLICY).strip()
    if open_rule == "Allowed callsigns + access code":
        return True
    if policy == "Require for non-default locations":
        return True
    if policy == "Require for all restricted locations" and open_rule != "Public":
        return True
    return False


def _location_visible_in_root(
    location: VaultLocation,
    *,
    sender: str,
    default_location_id: str,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    global_code_policy: str,
) -> bool:
    if not location.enabled or location.id == default_location_id or not location.list_in_root_menu:
        return False
    rule = str(location.visibility_rule or "Public").strip()
    open_rule = str(location.open_rule or "Public").strip()
    if rule == "Hidden":
        return False
    if rule == "Allowed callsigns only":
        return _location_sender_allowed(
            sender,
            location=location,
            global_allowed_callsigns=global_allowed_callsigns,
            limit_access_enabled=limit_access_enabled,
        )
    if rule == "Public":
        return True
    if open_rule == "Public":
        return True
    if _location_requires_code(location, default_location_id=default_location_id, global_code_policy=global_code_policy):
        return _sender_globally_allowed(
            sender,
            global_allowed_callsigns=global_allowed_callsigns,
            limit_access_enabled=limit_access_enabled,
        )
    return _sender_globally_allowed(
        sender,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
    )


def _location_by_id(locations: Sequence[VaultLocation], location_id: str) -> Optional[VaultLocation]:
    return next((loc for loc in locations if loc.id == location_id), None)


def _location_by_alias(locations: Sequence[VaultLocation], alias: str) -> Optional[VaultLocation]:
    target = normalize_location_alias(alias)
    return next((loc for loc in locations if normalize_location_alias(loc.alias, loc.name) == target), None)


def _extract_alias_request(text: str, alias_map: Mapping[str, str]) -> Tuple[str, str]:
    if not alias_map:
        return "", ""
    parts = " ".join(str(text or "").upper().split()).split()
    if not parts:
        return "", ""
    starts: List[int] = [0]
    if parts[0] in {"OPEN", "MSG", "TYPE"}:
        starts.append(1)
    if parts[0] == "BBS":
        starts.append(1)
        if len(parts) > 1 and parts[1] in {"OPEN", "MSG"}:
            starts.append(2)
        if len(parts) > 2 and parts[1] == "MSG" and parts[2] == "-":
            starts.append(3)
    for start in starts:
        while start < len(parts) and parts[start] in {"-", "TYPE"}:
            start += 1
        if start >= len(parts):
            continue
        alias = normalize_location_alias(parts[start])
        if alias and alias in alias_map:
            return alias, " ".join(parts[start + 1 :]).strip()
    return "", ""


def _menu_instruction_entry(text: str) -> _VirtualFile:
    return _VirtualFile(name=f"{text}.txt", content=text + "\n")


def _root_virtual_files(
    *,
    sender: str,
    locations: Sequence[VaultLocation],
    default_location_id: str,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    global_code_policy: str,
    flamp_enabled: bool,
    include_enabled_fallback: bool = False,
) -> List[_VirtualFile]:
    entries: List[_VirtualFile] = []
    for location in locations:
        if not _location_visible_in_root(
            location,
            sender=sender,
            default_location_id=default_location_id,
            global_allowed_callsigns=global_allowed_callsigns,
            limit_access_enabled=limit_access_enabled,
            global_code_policy=global_code_policy,
        ):
            continue
        alias = normalize_location_alias(location.alias, location.name)
        description = str(location.description or "").strip() or f"Open {location.name}"
        if _location_requires_code(location, default_location_id=default_location_id, global_code_policy=global_code_policy):
            text = f"BBS MSG - Type {alias} <code> {description} then refresh BBS"
        else:
            text = f"BBS MSG - Type {alias} {description} then refresh BBS"
        entries.append(_menu_instruction_entry(text))
    if not entries and include_enabled_fallback:
        for location in locations:
            if not location.enabled or location.id == default_location_id:
                continue
            if str(location.visibility_rule or "Public").strip() == "Hidden":
                continue
            alias = normalize_location_alias(location.alias, location.name)
            description = str(location.description or "").strip() or f"Open {location.name}"
            if _location_requires_code(location, default_location_id=default_location_id, global_code_policy=global_code_policy):
                text = f"BBS MSG - Type {alias} <code> {description} then refresh BBS"
            else:
                text = f"BBS MSG - Type {alias} {description} then refresh BBS"
            entries.append(_menu_instruction_entry(text))
    if flamp_enabled:
        entries.append(
            _menu_instruction_entry(
                "BBS MSG - FLAMP CMDS LIST Q LIST BLKS QNUM BLK 10 QNUM BLK 10,11 QNUM ThenRefresh"
            )
        )
    return entries


def _filesystem_location_virtual_files(managed_root: object) -> List[_VirtualFile]:
    try:
        locations_root = _managed_root_paths(managed_root)["locations"]
    except Exception:
        return []
    if not locations_root.exists() or not locations_root.is_dir():
        return []
    entries: List[_VirtualFile] = []
    seen_aliases = {"ROOT"}
    for child in sorted(locations_root.iterdir(), key=lambda item: item.name.lower()):
        if not child.is_dir():
            continue
        if child.name == DEFAULT_LOCATION_NAME:
            continue
        alias = normalize_location_alias("", child.name)
        if not alias or alias in seen_aliases:
            continue
        seen_aliases.add(alias)
        entries.append(_menu_instruction_entry(f"BBS MSG - Type {alias} Open {child.name} then refresh BBS"))
    return entries


def _location_virtual_files(*, include_root: bool = True) -> List[_VirtualFile]:
    entries: List[_VirtualFile] = []
    if include_root:
        entries.append(_menu_instruction_entry("BBS MSG - Type ROOT to return to main menu then refresh BBS"))
    return entries


def _location_access_prompt_virtual_files(location: VaultLocation, *, reason: str = "code_required") -> List[_VirtualFile]:
    alias = normalize_location_alias(location.alias, location.name)
    if reason == "callsign_restricted":
        lines = [
            f"BBS MSG - {location.name} is restricted to allowed callsigns",
            "BBS MSG - Type ROOT to return to main menu then refresh BBS",
        ]
    elif reason == "cooldown":
        lines = [
            f"BBS MSG - {location.name} access is temporarily locked after failed attempts",
            "BBS MSG - Type ROOT to return to main menu then refresh BBS",
        ]
    else:
        lines = [
            f"BBS MSG - {location.name} requires an access code",
            f"BBS MSG - Type {alias} _code_ then refresh BBS",
            "BBS MSG - Type ROOT to return to main menu then refresh BBS",
        ]
    return [_menu_instruction_entry(line) for line in lines]


def publish_location_access_prompt_view(
    location: VaultLocation,
    *,
    live_bbs_dir: object,
    managed_root: object,
    reason: str = "code_required",
) -> VaultPublishResult:
    virtual_files = _location_access_prompt_virtual_files(location, reason=reason)
    manifest, ignored_dirs = build_publish_manifest("", virtual_files=virtual_files)
    result = _publish_manifest_entries(
        manifest,
        source_dir="",
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        virtual_files=virtual_files,
    )
    return VaultPublishResult(
        changed=result.changed,
        published_count=result.published_count,
        removed_count=result.removed_count,
        unmanaged_live_files=result.unmanaged_live_files,
        manifest_path=result.manifest_path,
        ignored_directories=ignored_dirs,
    )


def publish_root_view(
    *,
    sender: str,
    locations: Sequence[VaultLocation],
    default_location_id: str,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    global_code_policy: str,
    live_bbs_dir: object,
    managed_root: object,
    flamp_enabled: bool = False,
    include_enabled_fallback: bool = False,
) -> VaultPublishResult:
    default_location = _location_by_id(locations, default_location_id)
    if default_location is None:
        raise ValueError("Managed Vault default location is missing")
    virtual_files = _root_virtual_files(
        sender=sender,
        locations=locations,
        default_location_id=default_location_id,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
        global_code_policy=global_code_policy,
        flamp_enabled=flamp_enabled,
        include_enabled_fallback=include_enabled_fallback,
    )
    if not virtual_files and include_enabled_fallback:
        virtual_files = _filesystem_location_virtual_files(managed_root)
    manifest, ignored_dirs = build_publish_manifest(default_location.source_dir, virtual_files=virtual_files)
    result = _publish_manifest_entries(
        manifest,
        source_dir=default_location.source_dir,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        virtual_files=virtual_files,
    )
    return VaultPublishResult(
        changed=result.changed,
        published_count=result.published_count,
        removed_count=result.removed_count,
        unmanaged_live_files=result.unmanaged_live_files,
        manifest_path=result.manifest_path,
        ignored_directories=ignored_dirs,
    )


def publish_location_view(
    location: VaultLocation,
    *,
    live_bbs_dir: object,
    managed_root: object,
) -> VaultPublishResult:
    virtual_files = _location_virtual_files(include_root=True)
    source_dir = _effective_location_source_dir(location, managed_root)
    manifest, ignored_dirs = build_publish_manifest(source_dir, virtual_files=virtual_files)
    result = _publish_manifest_entries(
        manifest,
        source_dir=source_dir,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        virtual_files=virtual_files,
    )
    return VaultPublishResult(
        changed=result.changed,
        published_count=result.published_count,
        removed_count=result.removed_count,
        unmanaged_live_files=result.unmanaged_live_files,
        manifest_path=result.manifest_path,
        ignored_directories=ignored_dirs,
    )


class FlampRelayStore:
    PROG_RE = re.compile(r"<PROG.*?\{([A-F0-9]+)\}", re.IGNORECASE)
    SIZE_RE = re.compile(r"<SIZE\s+[^>]*>\{([A-F0-9]+)\}(\d+)\s+(\d+)\s+(\d+)", re.IGNORECASE)
    BLOCK_RE = re.compile(r"\{([A-F0-9]+):(\d+)\}", re.IGNORECASE)
    VALID_Q_RE = re.compile(r"^[A-F0-9]{4}$", re.IGNORECASE)

    def __init__(self, relay_dir: object):
        self.relay_dir = _resolve_path(relay_dir)

    def relay_files(self) -> List[Path]:
        relay_dir = self.relay_dir
        if relay_dir is None or not relay_dir.exists():
            return []
        files: List[Path] = []
        for pattern in ("*.b2s", "*.k2s", "*.relay", "*.txt", "*.dat"):
            files.extend(relay_dir.glob(pattern))
        return [path for path in files if path.is_file()]

    def queue_index(self) -> Dict[str, Path]:
        index: Dict[str, Path] = {}
        for path in self.relay_files():
            name = path.name
            if len(name) < 4:
                continue
            queue_id = name[:4].upper()
            if not self.VALID_Q_RE.match(queue_id):
                continue
            previous = index.get(queue_id)
            if previous is None or path.stat().st_mtime > previous.stat().st_mtime:
                index[queue_id] = path
        return index

    def parse_queue(self, queue_id: str) -> Optional[Dict[str, object]]:
        queue_id = str(queue_id or "").strip().upper()
        if not self.VALID_Q_RE.match(queue_id):
            return None
        path = self.queue_index().get(queue_id)
        if not path:
            return None
        return self.parse_file(path)

    def parse_file(self, file_path: Path) -> Optional[Dict[str, object]]:
        file_id = None
        total_blocks = None
        file_size = None
        block_len = None
        blocks: Dict[int, str] = {}
        header_lines: List[str] = []
        try:
            with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
                for raw_line in handle:
                    line = raw_line.rstrip("\r\n")
                    if len(header_lines) < 4 and line.startswith("<"):
                        header_lines.append(line)
                    if not line.strip():
                        continue
                    match_prog = self.PROG_RE.search(line)
                    if match_prog and not file_id:
                        file_id = match_prog.group(1).upper()
                    match_size = self.SIZE_RE.search(line)
                    if match_size:
                        file_id = file_id or match_size.group(1).upper()
                        file_size = int(match_size.group(2))
                        total_blocks = int(match_size.group(3))
                        block_len = int(match_size.group(4))
                    match_block = self.BLOCK_RE.search(line)
                    if match_block:
                        fid, block_num = match_block.groups()
                        fid = fid.upper()
                        if not file_id:
                            file_id = fid
                        blocks[int(block_num)] = line
        except Exception:
            return None
        if not blocks:
            return None
        return {
            "path": str(file_path),
            "name": file_path.name,
            "file_id": (file_id or file_path.name[:4]).upper(),
            "total_blocks": total_blocks,
            "file_size": file_size,
            "block_len": block_len,
            "blocks": blocks,
            "header": header_lines,
        }

    def available_blocks_text(self, relay_info: Mapping[str, object]) -> Tuple[List[int], List[int]]:
        blocks = sorted(int(item) for item in dict(relay_info.get("blocks", {})).keys())
        total = int(relay_info.get("total_blocks") or (blocks[-1] if blocks else 0))
        missing = [num for num in range(1, total + 1) if num not in blocks]
        return blocks, missing


def _flamp_queue_files(store: FlampRelayStore) -> List[_VirtualFile]:
    entries: List[_VirtualFile] = []
    for queue_id, path in sorted(store.queue_index().items()):
        entries.append(
            _menu_instruction_entry(
                f"BBS MSG - Type LIST BLKS {queue_id} for {path.name} then refresh BBS"
            )
        )
    entries.append(_menu_instruction_entry("BBS MSG - Type ROOT to return to main menu then refresh BBS"))
    return entries


def publish_flamp_queue_list_view(
    store: FlampRelayStore,
    *,
    base_source_dir: object,
    live_bbs_dir: object,
    managed_root: object,
) -> VaultPublishResult:
    queues = store.queue_index()
    body_lines = [f"{queue_id} {path.name}" for queue_id, path in sorted(queues.items())]
    virtual_files = [
        _VirtualFile(name=DEFAULT_FLAMP_QUEUE_HELPER_NAME, content="\n".join(body_lines) + ("\n" if body_lines else "")),
        *_flamp_queue_files(store),
    ]
    manifest, ignored_dirs = build_publish_manifest(base_source_dir, virtual_files=virtual_files)
    result = _publish_manifest_entries(
        manifest,
        source_dir=base_source_dir,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        virtual_files=virtual_files,
    )
    return VaultPublishResult(
        changed=result.changed,
        published_count=result.published_count,
        removed_count=result.removed_count,
        unmanaged_live_files=result.unmanaged_live_files,
        manifest_path=result.manifest_path,
        ignored_directories=ignored_dirs,
    )


def publish_flamp_block_list_view(
    store: FlampRelayStore,
    queue_id: str,
    *,
    base_source_dir: object,
    live_bbs_dir: object,
    managed_root: object,
) -> VaultPublishResult:
    relay_info = store.parse_queue(queue_id)
    if not relay_info:
        raise ValueError(f"FLAMP queue {queue_id} not found")
    blocks, missing = store.available_blocks_text(relay_info)
    lines = [
        f"QUEUE {relay_info['file_id']}",
        f"FILE {relay_info['name']}",
        "AVAILABLE " + (",".join(map(str, blocks)) if blocks else "NONE"),
        "MISSING " + (",".join(map(str, missing)) if missing else "NONE"),
    ]
    virtual_files = [
        _VirtualFile(
            name=f"{DEFAULT_FLAMP_BLOCK_PREFIX}_{relay_info['file_id']}.txt",
            content="\n".join(lines) + "\n",
        ),
        _menu_instruction_entry("BBS MSG - Type ROOT to return to main menu then refresh BBS"),
    ]
    manifest, ignored_dirs = build_publish_manifest(base_source_dir, virtual_files=virtual_files)
    result = _publish_manifest_entries(
        manifest,
        source_dir=base_source_dir,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        virtual_files=virtual_files,
    )
    return VaultPublishResult(
        changed=result.changed,
        published_count=result.published_count,
        removed_count=result.removed_count,
        unmanaged_live_files=result.unmanaged_live_files,
        manifest_path=result.manifest_path,
        ignored_directories=ignored_dirs,
    )


def publish_flamp_block_overlay_view(
    store: FlampRelayStore,
    queue_id: str,
    block_numbers: Sequence[int],
    *,
    live_bbs_dir: object,
    managed_root: object,
) -> Tuple[VaultPublishResult, str]:
    relay_info = store.parse_queue(queue_id)
    if not relay_info:
        raise ValueError(f"FLAMP queue {queue_id} not found")
    delivered: List[str] = []
    missing: List[str] = []
    combined_blocks: List[str] = []
    for block_num in block_numbers:
        if int(block_num) == 0:
            header = list(relay_info.get("header", []))
            if header:
                combined_blocks.extend(header)
                delivered.append("0")
            else:
                missing.append("0")
            continue
        payload = dict(relay_info["blocks"]).get(int(block_num))
        if not payload:
            missing.append(str(block_num))
            continue
        combined_blocks.append(str(payload))
        delivered.append(str(block_num))
    if not delivered:
        raise ValueError(f"Requested block(s) not present for queue {queue_id}: {', '.join(missing)}")
    overlay_name = f"{DEFAULT_FLAMP_FILE_PREFIX}_{relay_info['file_id']}_BLK_{'_'.join(delivered)}.txt"
    virtual_files = [
        _VirtualFile(name=overlay_name, content="\n".join(combined_blocks) + "\n"),
        _menu_instruction_entry("BBS MSG - Type ROOT to return to main menu then refresh BBS"),
    ]
    manifest, _ = build_publish_manifest("", virtual_files=virtual_files)
    result = _publish_manifest_entries(
        manifest,
        source_dir="",
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        virtual_files=virtual_files,
    )
    return result, overlay_name


def _prune_attempts(attempts: Sequence[float], *, now_ts: float, window_seconds: int) -> List[float]:
    lower = float(now_ts) - max(1, int(window_seconds))
    return [float(ts) for ts in attempts if float(ts) >= lower]


def _resolve_varac_db_path(settings) -> Optional[Path]:
    raw_db = str(settings.get("varac_db_path", "") or "").strip() if settings is not None else ""
    raw_install = str(settings.get("varac_path", "") or "").strip() if settings is not None else ""
    for raw in (raw_db, raw_install):
        if not raw:
            continue
        try:
            path = Path(raw)
            if path.is_dir():
                candidate = path / "VarAC.db"
                if candidate.exists():
                    return candidate
            elif path.is_file():
                return path
        except Exception:
            continue
    return None


def _load_db_events(varac_db_path: Path, *, last_datastream_id: int, alias_map: Mapping[str, str]) -> List[VaultDbEvent]:
    events: List[VaultDbEvent] = []
    try:
        conn = sqlite3.connect(str(varac_db_path), timeout=1.5)
        conn.row_factory = sqlite3.Row
    except Exception as exc:
        log.debug("varac_bbs_vault: could not open VarAC.db %s: %s", varac_db_path, exc)
        return events
    try:
        rows = conn.execute(
            """
            SELECT
                ds.id,
                COALESCE(ds.qso_guid, '') AS qso_guid,
                COALESCE(ds.callsign, '') AS entry_callsign,
                COALESCE(ds.entry, '') AS entry_text,
                COALESCE(ds.creation_time, '') AS creation_time,
                COALESCE(ds.datastream_entry_type_id, 0) AS entry_type_id,
                COALESCE(q.callsign, '') AS remote_callsign,
                COALESCE(q.my_callsign, '') AS my_callsign
            FROM datastream ds
            LEFT JOIN qso q ON q.guid = ds.qso_guid
            WHERE ds.id > ?
            ORDER BY ds.id ASC
            LIMIT ?
            """,
            (int(last_datastream_id or 0), int(MAX_DB_ROWS_PER_SCAN)),
        ).fetchall()
    except Exception as exc:
        log.debug("varac_bbs_vault: could not query VarAC.db datastream: %s", exc)
        conn.close()
        return events
    finally:
        try:
            conn.close()
        except Exception:
            pass

    for row in rows:
        row_id = int(row["id"] or 0)
        qso_guid = str(row["qso_guid"] or "").strip()
        remote_callsign = _normalize_callsign(row["remote_callsign"])
        my_callsign = _normalize_callsign(row["my_callsign"])
        entry_callsign = _normalize_callsign(row["entry_callsign"])
        entry_text = str(row["entry_text"] or "").strip()
        if not qso_guid or not entry_text:
            continue
        timestamp_utc = _parse_db_timestamp(row["creation_time"])
        upper = " ".join(entry_text.split()).upper()

        if DISCONNECT_RE.search(upper):
            events.append(
                VaultDbEvent(
                    row_id=row_id,
                    timestamp_utc=timestamp_utc,
                    qso_guid=qso_guid,
                    remote_callsign=remote_callsign,
                    my_callsign=my_callsign,
                    entry_callsign=entry_callsign,
                    entry_text=entry_text,
                    kind="disconnect",
                )
            )
            continue

        if not remote_callsign or entry_callsign != remote_callsign:
            continue

        if upper == "<BLR>":
            events.append(
                VaultDbEvent(row_id, timestamp_utc, qso_guid, remote_callsign, my_callsign, entry_callsign, entry_text, "root_request")
            )
            continue
        if ROOT_CMD_RE.match(upper):
            events.append(
                VaultDbEvent(row_id, timestamp_utc, qso_guid, remote_callsign, my_callsign, entry_callsign, entry_text, "root_return")
            )
            continue
        match = LIST_Q_RE.match(upper)
        if match:
            events.append(
                VaultDbEvent(row_id, timestamp_utc, qso_guid, remote_callsign, my_callsign, entry_callsign, entry_text, "flamp_list_q")
            )
            continue
        match = LIST_BLOCKS_RE.match(upper)
        if match:
            events.append(
                VaultDbEvent(
                    row_id,
                    timestamp_utc,
                    qso_guid,
                    remote_callsign,
                    my_callsign,
                    entry_callsign,
                    entry_text,
                    "flamp_list_blocks",
                    queue_id=str(match.group(1) or "").upper(),
                )
            )
            continue
        match = BLOCK_REQUEST_RE.match(upper)
        if match:
            nums = [int(part.strip()) for part in str(match.group(1) or "").split(",") if part.strip().isdigit()]
            events.append(
                VaultDbEvent(
                    row_id,
                    timestamp_utc,
                    qso_guid,
                    remote_callsign,
                    my_callsign,
                    entry_callsign,
                    entry_text,
                    "flamp_block_request",
                    queue_id=str(match.group(2) or "").upper(),
                    block_numbers=tuple(nums),
                )
            )
            continue
        alias, code_text = _extract_alias_request(upper, alias_map)
        if alias:
            events.append(
                VaultDbEvent(
                    row_id,
                    timestamp_utc,
                    qso_guid,
                    remote_callsign,
                    my_callsign,
                    entry_callsign,
                    entry_text,
                    "open_alias",
                    alias=alias,
                    code_text=code_text,
                )
            )
            continue

        match = COMMAND_RE.search(upper)
        if match:
            events.append(
                VaultDbEvent(
                    row_id,
                    timestamp_utc,
                    qso_guid,
                    remote_callsign,
                    my_callsign,
                    entry_callsign,
                    entry_text,
                    "legacy_code_open",
                    code_text=str(match.group(1) or "").strip(),
                )
            )
    return events


def _summary_location_name(locations: Sequence[VaultLocation], location_id: str) -> str:
    location = _location_by_id(locations, location_id)
    return location.name if location is not None else location_id or DEFAULT_LOCATION_NAME


def _persist_runtime_state(settings, state: VaultRuntimeState, summary: str) -> None:
    if settings is None:
        return
    try:
        settings.set("varac_bbs_vault_runtime_state_v1", vault_runtime_state_to_data(state))
        settings.set("varac_bbs_vault_last_summary", summary)
    except Exception as exc:
        log.debug("varac_bbs_vault: failed to persist runtime state: %s", exc)


def _update_state(
    state: VaultRuntimeState,
    **updates,
) -> VaultRuntimeState:
    payload = vault_runtime_state_to_data(state)
    payload.update(updates)
    return VaultRuntimeState(**payload)


def _publish_root_action(
    *,
    sender: str,
    qso_guid: str,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    default_location_id: str,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    global_code_policy: str,
    runtime_state: VaultRuntimeState,
    now_ts: float,
    flamp_enabled: bool,
    reason: str,
) -> VaultActionResult:
    publish_result = publish_root_view(
        sender=sender,
        locations=locations,
        default_location_id=default_location_id,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
        global_code_policy=global_code_policy,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        flamp_enabled=flamp_enabled,
        include_enabled_fallback=True,
    )
    summary = f"Managed Vault published root menu for {sender or 'public'}."
    next_state = _update_state(
        runtime_state,
        current_location_id=default_location_id,
        current_session_callsign=_normalize_callsign(sender),
        current_session_qso_guid=str(qso_guid or "").strip(),
        current_view_mode="root",
        current_view_label=_summary_location_name(locations, default_location_id),
        current_overlay_file="",
        last_publish_manifest_path=publish_result.manifest_path,
        last_publish_ts=now_ts,
        last_action=summary,
        last_request_ts=now_ts,
        last_error="",
        unmanaged_live_files=list(publish_result.unmanaged_live_files),
    )
    _append_audit_event(
        managed_root,
        {
            "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "action": reason,
            "sender": _normalize_callsign(sender),
            "qso_guid": str(qso_guid or "").strip(),
            "location_id": default_location_id,
        },
    )
    return VaultActionResult(reason, True, summary, next_state, publish_result=publish_result)


def _publish_refresh_action(
    *,
    sender: str,
    qso_guid: str,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    default_location_id: str,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    global_code_policy: str,
    runtime_state: VaultRuntimeState,
    now_ts: float,
    flamp_enabled: bool,
    reason: str,
) -> VaultActionResult:
    state = load_vault_runtime_state(vault_runtime_state_to_data(runtime_state))
    sender_norm = _normalize_callsign(sender)
    qso_guid_text = str(qso_guid or "").strip()
    same_session_location_refresh = (
        state.current_view_mode in {"location", "access-prompt"}
        and bool(state.current_session_qso_guid)
        and state.current_session_qso_guid == qso_guid_text
        and (not state.current_session_callsign or state.current_session_callsign == sender_norm)
    )
    if not same_session_location_refresh:
        return _publish_root_action(
            sender=sender_norm,
            qso_guid=qso_guid_text,
            locations=locations,
            live_bbs_dir=live_bbs_dir,
            managed_root=managed_root,
            default_location_id=default_location_id,
            global_allowed_callsigns=global_allowed_callsigns,
            limit_access_enabled=limit_access_enabled,
            global_code_policy=global_code_policy,
            runtime_state=state,
            now_ts=now_ts,
            flamp_enabled=flamp_enabled,
            reason=reason,
        )
    current_location = _location_by_id(locations, state.current_location_id)
    if current_location is None or not current_location.enabled:
        return _publish_root_action(
            sender=sender_norm,
            qso_guid=qso_guid_text,
            locations=locations,
            live_bbs_dir=live_bbs_dir,
            managed_root=managed_root,
            default_location_id=default_location_id,
            global_allowed_callsigns=global_allowed_callsigns,
            limit_access_enabled=limit_access_enabled,
            global_code_policy=global_code_policy,
            runtime_state=state,
            now_ts=now_ts,
            flamp_enabled=flamp_enabled,
            reason=reason,
        )
    if state.current_view_mode == "access-prompt":
        publish_result = publish_location_access_prompt_view(
            current_location,
            live_bbs_dir=live_bbs_dir,
            managed_root=managed_root,
            reason="code_required",
        )
        summary = f"Managed Vault refreshed access prompt for {current_location.name}."
        next_state = _update_state(
            state,
            current_location_id=current_location.id,
            current_session_callsign=sender_norm,
            current_session_qso_guid=qso_guid_text,
            current_view_mode="access-prompt",
            current_view_label=f"{current_location.name} access",
            current_overlay_file="",
            last_publish_manifest_path=publish_result.manifest_path,
            last_publish_ts=now_ts,
            last_action=summary,
            last_request_ts=now_ts,
            last_error="",
            unmanaged_live_files=list(publish_result.unmanaged_live_files),
        )
        _append_audit_event(
            managed_root,
            {
                "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "action": "refresh_access_prompt",
                "sender": sender_norm,
                "qso_guid": qso_guid_text,
                "location_id": current_location.id,
            },
        )
        return VaultActionResult("refresh_access_prompt", True, summary, next_state, publish_result=publish_result)
    publish_result = publish_location_view(current_location, live_bbs_dir=live_bbs_dir, managed_root=managed_root)
    summary = f"Managed Vault refreshed {current_location.name} for {sender_norm or 'public'}."
    next_state = _update_state(
        state,
        current_location_id=current_location.id,
        current_session_callsign=sender_norm,
        current_session_qso_guid=qso_guid_text,
        current_view_mode="location",
        current_view_label=current_location.name,
        current_overlay_file="",
        last_publish_manifest_path=publish_result.manifest_path,
        last_publish_ts=now_ts,
        last_action=summary,
        last_request_ts=now_ts,
        last_error="",
        unmanaged_live_files=list(publish_result.unmanaged_live_files),
    )
    _append_audit_event(
        managed_root,
        {
            "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "action": "refresh_current_view",
            "sender": sender_norm,
            "qso_guid": qso_guid_text,
            "location_id": current_location.id,
        },
    )
    return VaultActionResult("refresh_current_view", True, summary, next_state, publish_result=publish_result)


def apply_unlock_request(
    sender: str,
    code_text: str,
    *,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    default_location_id: str,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    runtime_state: VaultRuntimeState,
    now_ts: Optional[float] = None,
    failed_attempt_limit: int = DEFAULT_FAILED_ATTEMPT_LIMIT,
    failed_attempt_window_seconds: int = DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS,
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
) -> VaultActionResult:
    return _apply_open_request(
        sender=sender,
        qso_guid=runtime_state.current_session_qso_guid,
        requested_location=None,
        alias_text="",
        code_text=code_text,
        locations=locations,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        default_location_id=default_location_id,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
        runtime_state=runtime_state,
        now_ts=now_ts,
        failed_attempt_limit=failed_attempt_limit,
        failed_attempt_window_seconds=failed_attempt_window_seconds,
        cooldown_seconds=cooldown_seconds,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        action_reason="legacy_code_open",
    )


def _access_prompt_result(
    *,
    state: VaultRuntimeState,
    location: VaultLocation,
    sender: str,
    qso_guid: str,
    alias_text: str,
    live_bbs_dir: object,
    managed_root: object,
    now_ts: float,
    summary: str,
    action: str,
    reason: str,
    cooldowns: Mapping[str, Mapping[str, float]],
    failed_attempts: Mapping[str, Sequence[float]],
) -> VaultActionResult:
    publish_result = publish_location_access_prompt_view(
        location,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        reason=reason,
    )
    next_state = _update_state(
        state,
        current_location_id=location.id,
        current_session_callsign=sender,
        current_session_qso_guid=str(qso_guid or "").strip(),
        current_view_mode="access-prompt",
        current_view_label=f"{location.name} access",
        cooldowns=cooldowns,
        failed_attempts=failed_attempts,
        current_overlay_file="",
        last_publish_manifest_path=publish_result.manifest_path,
        last_publish_ts=now_ts,
        last_action=summary,
        last_request_ts=now_ts,
        last_error=action,
        unmanaged_live_files=list(publish_result.unmanaged_live_files),
    )
    _append_audit_event(
        managed_root,
        {
            "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "action": action,
            "sender": sender,
            "location_id": location.id,
            "location_name": location.name,
            "alias": alias_text,
            "prompt_reason": reason,
        },
    )
    return VaultActionResult(action, False, summary, next_state, publish_result=publish_result)


def _apply_open_request(
    *,
    sender: str,
    qso_guid: str,
    requested_location: Optional[VaultLocation],
    alias_text: str,
    code_text: str,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    default_location_id: str,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    runtime_state: VaultRuntimeState,
    now_ts: Optional[float],
    failed_attempt_limit: int,
    failed_attempt_window_seconds: int,
    cooldown_seconds: int,
    global_code_policy: str,
    action_reason: str,
) -> VaultActionResult:
    now_ts = float(now_ts if now_ts is not None else time.time())
    sender = _normalize_callsign(sender)
    code_text = str(code_text or "").strip()
    state = load_vault_runtime_state(vault_runtime_state_to_data(runtime_state))
    if not sender:
        summary = "Managed Vault ignored request with no identifiable callsign."
        return VaultActionResult("ignored_no_sender", False, summary, state)

    if state.current_session_qso_guid and qso_guid and state.current_session_qso_guid != qso_guid and state.current_session_callsign:
        summary = f"Managed Vault session is locked to {state.current_session_callsign}."
        return VaultActionResult("session_locked", False, summary, _update_state(state, last_action=summary, last_error="session_locked"))

    cooldowns = {str(key): dict(value) for key, value in state.cooldowns.items()}
    current_cooldown = cooldowns.get(sender, {})
    until_ts = float(current_cooldown.get("until_ts", 0.0) or 0.0)
    if until_ts > now_ts:
        remaining = int(max(1.0, until_ts - now_ts))
        summary = f"Managed Vault cooldown active for {sender} ({remaining}s remaining)."
        if requested_location is not None:
            return _access_prompt_result(
                state=state,
                location=requested_location,
                sender=sender,
                qso_guid=qso_guid,
                alias_text=alias_text,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                now_ts=now_ts,
                summary=summary,
                action="cooldown_active",
                reason="cooldown",
                cooldowns=cooldowns,
                failed_attempts={str(key): list(value) for key, value in state.failed_attempts.items()},
            )
        return VaultActionResult("cooldown_active", False, summary, _update_state(state, last_action=summary, last_error="cooldown_active"))

    matched_location = requested_location
    if matched_location is None:
        for location in locations:
            if not location.enabled or location.id == default_location_id:
                continue
            if not location.access_code_hash or not location.access_code_salt:
                continue
            if verify_access_code(
                code_text,
                access_code_hash=location.access_code_hash,
                access_code_salt=location.access_code_salt,
                access_code_iterations=location.access_code_iterations,
            ):
                matched_location = location
                break

    failed_attempts = {str(key): list(value) for key, value in state.failed_attempts.items()}
    sender_attempts = _prune_attempts(
        failed_attempts.get(sender, []),
        now_ts=now_ts,
        window_seconds=failed_attempt_window_seconds,
    )

    if matched_location is None:
        sender_attempts.append(now_ts)
        failed_attempts[sender] = sender_attempts
        action = "invalid_code"
        summary = f"Managed Vault rejected access request from {sender}."
        if len(sender_attempts) >= max(1, int(failed_attempt_limit)):
            cooldowns[sender] = {
                "until_ts": now_ts + max(1, int(cooldown_seconds)),
                "reason": "failed_attempt_limit",
            }
            action = "cooldown_applied"
            summary = f"Managed Vault rejected access request from {sender}; cooldown applied."
        next_state = _update_state(
            state,
            cooldowns=cooldowns,
            failed_attempts=failed_attempts,
            last_action=summary,
            last_error=action,
        )
        _append_audit_event(
            managed_root,
            {
                "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "action": action,
                "sender": sender,
                "alias": alias_text,
            },
        )
        return VaultActionResult(action, False, summary, next_state)

    open_rule = str(matched_location.open_rule or "Public").strip()
    if open_rule != "Public" and not _location_sender_allowed(
        sender,
        location=matched_location,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
    ):
        summary = f"Managed Vault rejected {sender}; callsign is not allowed for {matched_location.name}."
        return _access_prompt_result(
            state=state,
            location=matched_location,
            sender=sender,
            qso_guid=qso_guid,
            alias_text=alias_text,
            live_bbs_dir=live_bbs_dir,
            managed_root=managed_root,
            now_ts=now_ts,
            summary=summary,
            action="rejected_callsign",
            reason="callsign_restricted",
            cooldowns=cooldowns,
            failed_attempts=failed_attempts,
        )

    if _location_requires_code(matched_location, default_location_id=default_location_id, global_code_policy=global_code_policy):
        if not verify_access_code(
            code_text,
            access_code_hash=matched_location.access_code_hash,
            access_code_salt=matched_location.access_code_salt,
            access_code_iterations=matched_location.access_code_iterations,
        ):
            sender_attempts.append(now_ts)
            failed_attempts[sender] = sender_attempts
            summary = f"Managed Vault rejected access code for {matched_location.name} from {sender}."
            action = "invalid_code"
            if len(sender_attempts) >= max(1, int(failed_attempt_limit)):
                cooldowns[sender] = {
                    "until_ts": now_ts + max(1, int(cooldown_seconds)),
                    "reason": "failed_attempt_limit",
                }
                action = "cooldown_applied"
                summary = f"Managed Vault rejected access code for {matched_location.name} from {sender}; cooldown applied."
            return _access_prompt_result(
                state=state,
                location=matched_location,
                sender=sender,
                qso_guid=qso_guid,
                alias_text=alias_text,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                now_ts=now_ts,
                summary=summary,
                action=action,
                reason="cooldown" if action == "cooldown_applied" else "code_required",
                cooldowns=cooldowns,
                failed_attempts=failed_attempts,
            )

    publish_result = publish_location_view(matched_location, live_bbs_dir=live_bbs_dir, managed_root=managed_root)
    failed_attempts.pop(sender, None)
    cooldowns.pop(sender, None)
    summary = f"Managed Vault published {matched_location.name} for {sender}."
    next_state = _update_state(
        state,
        current_location_id=matched_location.id,
        current_session_callsign=sender,
        current_session_qso_guid=str(qso_guid or "").strip(),
        current_view_mode="location",
        current_view_label=matched_location.name,
        cooldowns=cooldowns,
        failed_attempts=failed_attempts,
        current_overlay_file="",
        last_publish_manifest_path=publish_result.manifest_path,
        last_publish_ts=now_ts,
        last_action=summary,
        last_request_ts=now_ts,
        last_error="",
        unmanaged_live_files=list(publish_result.unmanaged_live_files),
    )
    _append_audit_event(
        managed_root,
        {
            "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "action": action_reason,
            "sender": sender,
            "location_id": matched_location.id,
            "location_name": matched_location.name,
            "alias": alias_text,
        },
    )
    return VaultActionResult(action_reason, True, summary, next_state, publish_result=publish_result)


def reset_to_default_location(
    *,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    default_location_id: str,
    runtime_state: VaultRuntimeState,
    global_allowed_callsigns: Sequence[str] = (),
    limit_access_enabled: bool = False,
    global_code_policy: str = DEFAULT_GLOBAL_CODE_POLICY,
    flamp_enabled: bool = False,
    reason: str = "manual_reset",
    now_ts: Optional[float] = None,
) -> VaultActionResult:
    now_ts = float(now_ts if now_ts is not None else time.time())
    state = load_vault_runtime_state(vault_runtime_state_to_data(runtime_state))
    publish_result = publish_root_view(
        sender="",
        locations=locations,
        default_location_id=default_location_id,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
        global_code_policy=global_code_policy,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        flamp_enabled=flamp_enabled,
        include_enabled_fallback=True,
    )
    summary = f"Managed Vault returned to {DEFAULT_LOCATION_NAME}."
    next_state = _update_state(
        state,
        current_location_id=default_location_id,
        current_session_callsign="",
        current_session_qso_guid="",
        current_view_mode="root",
        current_view_label=_summary_location_name(locations, default_location_id),
        current_overlay_file="",
        last_publish_manifest_path=publish_result.manifest_path,
        last_publish_ts=now_ts,
        last_action=summary,
        last_request_ts=0.0,
        last_error="",
        unmanaged_live_files=list(publish_result.unmanaged_live_files),
    )
    _append_audit_event(
        managed_root,
        {
            "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "action": "returned_to_default",
            "reason": reason,
            "location_id": default_location_id,
        },
    )
    return VaultActionResult("returned_to_default", True, summary, next_state, publish_result=publish_result)


def _restore_previous_view(
    *,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    default_location_id: str,
    runtime_state: VaultRuntimeState,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    global_code_policy: str,
    flamp_enabled: bool,
    now_ts: Optional[float] = None,
    reason: str = "overlay_restore",
) -> VaultActionResult:
    now_ts = float(now_ts if now_ts is not None else time.time())
    if runtime_state.previous_view_mode == "location":
        location = _location_by_id(locations, runtime_state.previous_location_id)
        if location is None:
            return reset_to_default_location(
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                default_location_id=default_location_id,
                runtime_state=runtime_state,
                global_allowed_callsigns=global_allowed_callsigns,
                limit_access_enabled=limit_access_enabled,
                global_code_policy=global_code_policy,
                flamp_enabled=flamp_enabled,
                reason=reason,
                now_ts=now_ts,
            )
        publish_result = publish_location_view(location, live_bbs_dir=live_bbs_dir, managed_root=managed_root)
        summary = f"Managed Vault restored {location.name} after FLAMP overlay."
        next_state = _update_state(
            runtime_state,
            current_location_id=location.id,
            current_view_mode="location",
            current_view_label=location.name,
            current_overlay_file="",
            previous_location_id=default_location_id,
            previous_view_mode="root",
            previous_view_label=_summary_location_name(locations, default_location_id),
            last_publish_manifest_path=publish_result.manifest_path,
            last_publish_ts=now_ts,
            last_action=summary,
            last_request_ts=now_ts,
            last_error="",
            unmanaged_live_files=list(publish_result.unmanaged_live_files),
        )
        return VaultActionResult("overlay_restore", True, summary, next_state, publish_result=publish_result)
    return _publish_root_action(
        sender=runtime_state.current_session_callsign,
        qso_guid=runtime_state.current_session_qso_guid,
        locations=locations,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        default_location_id=default_location_id,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
        global_code_policy=global_code_policy,
        runtime_state=runtime_state,
        now_ts=now_ts,
        flamp_enabled=flamp_enabled,
        reason=reason,
    )


def _reconcile_current_location(
    settings,
    *,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    runtime_state: VaultRuntimeState,
    default_location_id: str,
    global_allowed_callsigns: Sequence[str],
    limit_access_enabled: bool,
    global_code_policy: str,
    flamp_enabled: bool,
) -> Tuple[VaultRuntimeState, bool]:
    try:
        if runtime_state.current_view_mode == "location":
            current_location = _location_by_id(locations, runtime_state.current_location_id)
            if current_location is None or not current_location.enabled:
                return runtime_state, False
            publish_result = publish_location_view(current_location, live_bbs_dir=live_bbs_dir, managed_root=managed_root)
        elif runtime_state.current_view_mode == "access-prompt":
            current_location = _location_by_id(locations, runtime_state.current_location_id)
            if current_location is None or not current_location.enabled:
                return runtime_state, False
            publish_result = publish_location_access_prompt_view(
                current_location,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                reason="code_required",
            )
        elif runtime_state.current_view_mode == "flamp-block-overlay":
            overlay_name = str(runtime_state.current_overlay_file or "").strip()
            if overlay_name:
                live_dir = _resolve_path(live_bbs_dir)
                if live_dir is not None and not (live_dir / overlay_name).exists():
                    restored = _restore_previous_view(
                        locations=locations,
                        live_bbs_dir=live_bbs_dir,
                        managed_root=managed_root,
                        default_location_id=default_location_id,
                        runtime_state=runtime_state,
                        global_allowed_callsigns=global_allowed_callsigns,
                        limit_access_enabled=limit_access_enabled,
                        global_code_policy=global_code_policy,
                        flamp_enabled=flamp_enabled,
                    )
                    return restored.runtime_state, bool(restored.publish_result and restored.publish_result.changed)
            return runtime_state, False
        else:
            publish_result = publish_root_view(
                sender=runtime_state.current_session_callsign,
                locations=locations,
                default_location_id=default_location_id,
                global_allowed_callsigns=global_allowed_callsigns,
                limit_access_enabled=limit_access_enabled,
                global_code_policy=global_code_policy,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                flamp_enabled=flamp_enabled,
                include_enabled_fallback=True,
            )
    except Exception as exc:
        summary = f"Managed Vault degraded: {exc}"
        next_state = _update_state(runtime_state, last_action=summary, last_error=str(exc))
        return next_state, False
    next_state = _update_state(
        runtime_state,
        last_publish_manifest_path=publish_result.manifest_path,
        last_publish_ts=time.time(),
        unmanaged_live_files=list(publish_result.unmanaged_live_files),
    )
    return next_state, bool(publish_result.changed)


def _summary_text(runtime_state: VaultRuntimeState) -> str:
    summary = f"Managed Vault {runtime_state.current_view_label or DEFAULT_LOCATION_NAME}"
    if runtime_state.current_session_callsign:
        summary += f" | Session {runtime_state.current_session_callsign}"
    if runtime_state.current_view_mode == "flamp-block-overlay":
        summary += " | FLAMP overlay"
    elif runtime_state.current_view_mode.startswith("flamp"):
        summary += " | FLAMP relay"
    if runtime_state.unmanaged_live_files:
        summary += f" | Unmanaged live files: {len(runtime_state.unmanaged_live_files)}"
    if runtime_state.last_error:
        summary += f" | {runtime_state.last_action or runtime_state.last_error}"
    elif runtime_state.last_action:
        summary += f" | {runtime_state.last_action}"
    return summary


def run_varac_bbs_vault(settings) -> VaracBbsVaultRunResult:
    enabled = bool(settings.get("varac_bbs_vault_enabled", False) if settings is not None else False)
    if not enabled:
        return VaracBbsVaultRunResult(False, 0, 0, False, DEFAULT_LOCATION_ID, "", "Managed Vault disabled")

    live_bbs_dir = str(settings.get("varac_bbs_dir", "") or "").strip() if settings is not None else ""
    managed_root = compute_default_managed_root(live_bbs_dir)
    default_location_id = str(settings.get("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID) or DEFAULT_LOCATION_ID).strip() or DEFAULT_LOCATION_ID
    trigger_mode = str(settings.get("varac_bbs_vault_trigger_mode", DEFAULT_TRIGGER_MODE) or DEFAULT_TRIGGER_MODE).strip() or DEFAULT_TRIGGER_MODE
    return_mode = str(settings.get("varac_bbs_vault_return_mode", DEFAULT_RETURN_MODE) or DEFAULT_RETURN_MODE).strip() or DEFAULT_RETURN_MODE
    failed_attempt_limit = int(settings.get("varac_bbs_vault_failed_attempt_limit", DEFAULT_FAILED_ATTEMPT_LIMIT) or DEFAULT_FAILED_ATTEMPT_LIMIT)
    failed_attempt_window_seconds = int(settings.get("varac_bbs_vault_failed_attempt_window_seconds", DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS) or DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS)
    cooldown_seconds = int(settings.get("varac_bbs_vault_cooldown_seconds", DEFAULT_COOLDOWN_SECONDS) or DEFAULT_COOLDOWN_SECONDS)
    idle_timeout_seconds = int(settings.get("varac_bbs_vault_idle_timeout_seconds", DEFAULT_IDLE_TIMEOUT_SECONDS) or DEFAULT_IDLE_TIMEOUT_SECONDS)
    global_code_policy = str(settings.get("varac_bbs_vault_global_code_policy", DEFAULT_GLOBAL_CODE_POLICY) or DEFAULT_GLOBAL_CODE_POLICY).strip() or DEFAULT_GLOBAL_CODE_POLICY
    flamp_enabled = bool(settings.get("varac_bbs_vault_flamp_enabled", False) if settings is not None else False)
    flamp_relay_dir = str(settings.get("varac_bbs_vault_flamp_relay_dir", "") or "").strip() if settings is not None else ""
    locations = load_vault_locations(settings.get("varac_bbs_vault_locations_v1", []))
    locations = _with_filesystem_location_fallbacks(locations, managed_root, default_location_id=default_location_id)
    runtime_state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    global_allowed = parse_callsign_list(settings.get("varac_bbs_allowed_callsigns", "") if settings is not None else "")
    limit_access_enabled = bool(settings.get("varac_bbs_limit_access_enabled", False) if settings is not None else False)

    if not live_bbs_dir or not managed_root or not locations:
        summary = "Managed Vault needs setup before it can run."
        _persist_runtime_state(settings, runtime_state, summary)
        return VaracBbsVaultRunResult(True, 0, 0, False, runtime_state.current_location_id, runtime_state.current_session_callsign, summary)

    try:
        initialize_managed_root(managed_root)
    except Exception as exc:
        summary = f"Managed Vault initialization failed: {exc}"
        error_state = _update_state(runtime_state, last_action=summary, last_error=str(exc))
        _persist_runtime_state(settings, error_state, summary)
        return VaracBbsVaultRunResult(True, 0, 0, False, error_state.current_location_id, error_state.current_session_callsign, summary)

    alias_map = {
        normalize_location_alias(location.alias, location.name): location.id
        for location in locations
        if location.id != default_location_id
    }
    varac_db_path = _resolve_varac_db_path(settings)
    scanned = 0
    processed = 0
    published = False
    now_ts = time.time()

    if not runtime_state.last_publish_manifest_path:
        runtime_state = _update_state(runtime_state, last_publish_manifest_path=str(_manifest_path_for(managed_root)))

    if runtime_state.current_view_mode == "flamp-block-overlay" and runtime_state.current_overlay_file:
        live_dir = _resolve_path(live_bbs_dir)
        if live_dir is not None and not (live_dir / runtime_state.current_overlay_file).exists():
            restored = _restore_previous_view(
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                default_location_id=default_location_id,
                runtime_state=runtime_state,
                global_allowed_callsigns=global_allowed,
                limit_access_enabled=limit_access_enabled,
                global_code_policy=global_code_policy,
                flamp_enabled=flamp_enabled,
                now_ts=now_ts,
                reason="overlay_restore",
            )
            runtime_state = restored.runtime_state
            published = published or bool(restored.publish_result and restored.publish_result.changed)

    events: List[VaultDbEvent] = []
    if varac_db_path is not None:
        events = _load_db_events(varac_db_path, last_datastream_id=runtime_state.last_datastream_id, alias_map=alias_map)

    for event in events:
        scanned += 1
        runtime_state = _update_state(runtime_state, last_datastream_id=event.row_id)
        if event.kind == "disconnect":
            if runtime_state.current_session_qso_guid and event.qso_guid == runtime_state.current_session_qso_guid:
                if return_mode != "Manual operator reset only":
                    result = reset_to_default_location(
                        locations=locations,
                        live_bbs_dir=live_bbs_dir,
                        managed_root=managed_root,
                        default_location_id=default_location_id,
                        runtime_state=runtime_state,
                        global_allowed_callsigns=global_allowed,
                        limit_access_enabled=limit_access_enabled,
                        global_code_policy=global_code_policy,
                        flamp_enabled=flamp_enabled,
                        reason="disconnect",
                        now_ts=event.timestamp_utc or now_ts,
                    )
                    runtime_state = result.runtime_state
                    published = published or bool(result.publish_result and result.publish_result.changed)
                else:
                    runtime_state = _update_state(
                        runtime_state,
                        current_session_callsign="",
                        current_session_qso_guid="",
                        last_action="Managed Vault session disconnected.",
                    )
                processed += 1
            continue
        if runtime_state.current_session_qso_guid and runtime_state.current_session_qso_guid != event.qso_guid:
            continue
        if event.kind == "root_request":
            result = _publish_refresh_action(
                sender=event.remote_callsign,
                qso_guid=event.qso_guid,
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                default_location_id=default_location_id,
                global_allowed_callsigns=global_allowed,
                limit_access_enabled=limit_access_enabled,
                global_code_policy=global_code_policy,
                runtime_state=runtime_state,
                now_ts=event.timestamp_utc or now_ts,
                flamp_enabled=flamp_enabled,
                reason="root_request",
            )
            runtime_state = result.runtime_state
            published = published or bool(result.publish_result and result.publish_result.changed)
            processed += 1
            continue
        if event.kind == "root_return":
            result = _publish_root_action(
                sender=event.remote_callsign,
                qso_guid=event.qso_guid,
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                default_location_id=default_location_id,
                global_allowed_callsigns=global_allowed,
                limit_access_enabled=limit_access_enabled,
                global_code_policy=global_code_policy,
                runtime_state=runtime_state,
                now_ts=event.timestamp_utc or now_ts,
                flamp_enabled=flamp_enabled,
                reason="root_return",
            )
            runtime_state = result.runtime_state
            published = published or bool(result.publish_result and result.publish_result.changed)
            processed += 1
            continue
        if event.kind == "legacy_code_open":
            result = _apply_open_request(
                sender=event.remote_callsign,
                qso_guid=event.qso_guid,
                requested_location=None,
                alias_text="",
                code_text=event.code_text,
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                default_location_id=default_location_id,
                global_allowed_callsigns=global_allowed,
                limit_access_enabled=limit_access_enabled,
                runtime_state=runtime_state,
                now_ts=event.timestamp_utc or now_ts,
                failed_attempt_limit=failed_attempt_limit,
                failed_attempt_window_seconds=failed_attempt_window_seconds,
                cooldown_seconds=cooldown_seconds,
                global_code_policy=global_code_policy,
                action_reason="legacy_code_open",
            )
            runtime_state = result.runtime_state
            published = published or bool(result.publish_result and result.publish_result.changed)
            processed += 1
            continue
        if event.kind == "open_alias":
            requested = _location_by_alias(locations, event.alias)
            result = _apply_open_request(
                sender=event.remote_callsign,
                qso_guid=event.qso_guid,
                requested_location=requested,
                alias_text=event.alias,
                code_text=event.code_text,
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                default_location_id=default_location_id,
                global_allowed_callsigns=global_allowed,
                limit_access_enabled=limit_access_enabled,
                runtime_state=runtime_state,
                now_ts=event.timestamp_utc or now_ts,
                failed_attempt_limit=failed_attempt_limit,
                failed_attempt_window_seconds=failed_attempt_window_seconds,
                cooldown_seconds=cooldown_seconds,
                global_code_policy=global_code_policy,
                action_reason="open_alias",
            )
            runtime_state = result.runtime_state
            published = published or bool(result.publish_result and result.publish_result.changed)
            processed += 1
            continue
        if event.kind == "flamp_list_q" and flamp_enabled and flamp_relay_dir:
            store = FlampRelayStore(flamp_relay_dir)
            base_location = _location_by_id(locations, runtime_state.current_location_id) or _location_by_id(locations, default_location_id)
            if base_location is not None:
                publish_result = publish_flamp_queue_list_view(
                    store,
                    base_source_dir=base_location.source_dir,
                    live_bbs_dir=live_bbs_dir,
                    managed_root=managed_root,
                )
                runtime_state = _update_state(
                    runtime_state,
                    current_session_callsign=event.remote_callsign,
                    current_session_qso_guid=event.qso_guid,
                    current_view_mode="flamp-list",
                    current_view_label=f"FLAMP {DEFAULT_LOCATION_NAME}",
                    last_publish_manifest_path=publish_result.manifest_path,
                    last_publish_ts=event.timestamp_utc or now_ts,
                    last_action=f"Managed Vault published FLAMP queue list for {event.remote_callsign}.",
                    last_request_ts=event.timestamp_utc or now_ts,
                    last_error="",
                    unmanaged_live_files=list(publish_result.unmanaged_live_files),
                )
                published = published or bool(publish_result.changed)
                processed += 1
            continue
        if event.kind == "flamp_list_blocks" and flamp_enabled and flamp_relay_dir:
            store = FlampRelayStore(flamp_relay_dir)
            base_location = _location_by_id(locations, runtime_state.current_location_id) or _location_by_id(locations, default_location_id)
            if base_location is not None:
                try:
                    publish_result = publish_flamp_block_list_view(
                        store,
                        event.queue_id,
                        base_source_dir=base_location.source_dir,
                        live_bbs_dir=live_bbs_dir,
                        managed_root=managed_root,
                    )
                    runtime_state = _update_state(
                        runtime_state,
                        current_session_callsign=event.remote_callsign,
                        current_session_qso_guid=event.qso_guid,
                        current_view_mode="flamp-block-list",
                        current_view_label=f"FLAMP {event.queue_id}",
                        last_publish_manifest_path=publish_result.manifest_path,
                        last_publish_ts=event.timestamp_utc or now_ts,
                        last_action=f"Managed Vault published FLAMP block list {event.queue_id} for {event.remote_callsign}.",
                        last_request_ts=event.timestamp_utc or now_ts,
                        last_error="",
                        unmanaged_live_files=list(publish_result.unmanaged_live_files),
                    )
                    published = published or bool(publish_result.changed)
                    processed += 1
                except Exception as exc:
                    runtime_state = _update_state(runtime_state, last_action=f"FLAMP block list failed: {exc}", last_error=str(exc))
            continue
        if event.kind == "flamp_block_request" and flamp_enabled and flamp_relay_dir:
            store = FlampRelayStore(flamp_relay_dir)
            try:
                publish_result, overlay_name = publish_flamp_block_overlay_view(
                    store,
                    event.queue_id,
                    event.block_numbers,
                    live_bbs_dir=live_bbs_dir,
                    managed_root=managed_root,
                )
                runtime_state = _update_state(
                    runtime_state,
                    previous_location_id=runtime_state.current_location_id,
                    previous_view_mode=runtime_state.current_view_mode or DEFAULT_VIEW_MODE,
                    previous_view_label=runtime_state.current_view_label or DEFAULT_LOCATION_NAME,
                    current_session_callsign=event.remote_callsign,
                    current_session_qso_guid=event.qso_guid,
                    current_view_mode="flamp-block-overlay",
                    current_view_label=f"FLAMP {event.queue_id}",
                    current_overlay_file=overlay_name,
                    last_publish_manifest_path=publish_result.manifest_path,
                    last_publish_ts=event.timestamp_utc or now_ts,
                    last_action=f"Managed Vault published FLAMP overlay {overlay_name} for {event.remote_callsign}.",
                    last_request_ts=event.timestamp_utc or now_ts,
                    last_error="",
                    unmanaged_live_files=list(publish_result.unmanaged_live_files),
                )
                published = published or bool(publish_result.changed)
                processed += 1
            except Exception as exc:
                runtime_state = _update_state(runtime_state, last_action=f"FLAMP block request failed: {exc}", last_error=str(exc))
            continue

    log_paths = resolve_varac_traffic_log_paths(settings)
    seen_keys = set(runtime_state.processed_event_keys)
    for log_path in log_paths:
        log_events = parse_vault_log_events(
            _read_tail(log_path),
            trigger_mode=trigger_mode,
            log_path=str(log_path),
            alias_map=alias_map,
        )
        for event in log_events:
            scanned += 1
            key = _state_key(event)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            if event.kind == "unlock":
                result = _apply_open_request(
                    sender=event.sender,
                    qso_guid=runtime_state.current_session_qso_guid,
                    requested_location=None,
                    alias_text="",
                    code_text=event.code_text,
                    locations=locations,
                    live_bbs_dir=live_bbs_dir,
                    managed_root=managed_root,
                    default_location_id=default_location_id,
                    global_allowed_callsigns=global_allowed,
                    limit_access_enabled=limit_access_enabled,
                    runtime_state=runtime_state,
                    now_ts=event.timestamp_utc or now_ts,
                    failed_attempt_limit=failed_attempt_limit,
                    failed_attempt_window_seconds=failed_attempt_window_seconds,
                    cooldown_seconds=cooldown_seconds,
                    global_code_policy=global_code_policy,
                    action_reason="legacy_code_open",
                )
                runtime_state = result.runtime_state
                published = published or bool(result.publish_result and result.publish_result.changed)
                processed += 1
            elif event.kind == "open_alias":
                requested = _location_by_alias(locations, event.alias)
                result = _apply_open_request(
                    sender=event.sender,
                    qso_guid=runtime_state.current_session_qso_guid,
                    requested_location=requested,
                    alias_text=event.alias,
                    code_text=event.code_text,
                    locations=locations,
                    live_bbs_dir=live_bbs_dir,
                    managed_root=managed_root,
                    default_location_id=default_location_id,
                    global_allowed_callsigns=global_allowed,
                    limit_access_enabled=limit_access_enabled,
                    runtime_state=runtime_state,
                    now_ts=event.timestamp_utc or now_ts,
                    failed_attempt_limit=failed_attempt_limit,
                    failed_attempt_window_seconds=failed_attempt_window_seconds,
                    cooldown_seconds=cooldown_seconds,
                    global_code_policy=global_code_policy,
                    action_reason="log_open_alias",
                )
                runtime_state = result.runtime_state
                published = published or bool(result.publish_result and result.publish_result.changed)
                processed += 1
            elif event.kind == "root_return":
                result = _publish_root_action(
                    sender=event.sender,
                    qso_guid=runtime_state.current_session_qso_guid,
                    locations=locations,
                    live_bbs_dir=live_bbs_dir,
                    managed_root=managed_root,
                    default_location_id=default_location_id,
                    global_allowed_callsigns=global_allowed,
                    limit_access_enabled=limit_access_enabled,
                    global_code_policy=global_code_policy,
                    runtime_state=runtime_state,
                    now_ts=event.timestamp_utc or now_ts,
                    flamp_enabled=flamp_enabled,
                    reason="log_root_return",
                )
                runtime_state = result.runtime_state
                published = published or bool(result.publish_result and result.publish_result.changed)
                processed += 1
            elif event.kind == "disconnect" and runtime_state.current_session_callsign:
                if return_mode != "Manual operator reset only":
                    result = reset_to_default_location(
                        locations=locations,
                        live_bbs_dir=live_bbs_dir,
                        managed_root=managed_root,
                        default_location_id=default_location_id,
                        runtime_state=runtime_state,
                        reason="disconnect",
                        now_ts=event.timestamp_utc or now_ts,
                    )
                    runtime_state = result.runtime_state
                    published = published or bool(result.publish_result and result.publish_result.changed)
                processed += 1
    runtime_state = _update_state(runtime_state, processed_event_keys=list(tuple(list(seen_keys)[-MAX_PROCESSED_EVENT_KEYS:])))

    if processed == 0 and runtime_state.current_session_callsign and return_mode != "Manual operator reset only":
        last_request_ts = float(runtime_state.last_request_ts or 0.0)
        if last_request_ts and (now_ts - last_request_ts) >= max(60, int(idle_timeout_seconds)):
            result = reset_to_default_location(
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                default_location_id=default_location_id,
                runtime_state=runtime_state,
                global_allowed_callsigns=global_allowed,
                limit_access_enabled=limit_access_enabled,
                global_code_policy=global_code_policy,
                flamp_enabled=flamp_enabled,
                reason="idle_timeout",
                now_ts=now_ts,
            )
            runtime_state = result.runtime_state
            published = published or bool(result.publish_result and result.publish_result.changed)

    runtime_state, reconciled = _reconcile_current_location(
        settings,
        locations=locations,
        live_bbs_dir=live_bbs_dir,
        managed_root=managed_root,
        runtime_state=runtime_state,
        default_location_id=default_location_id,
        global_allowed_callsigns=global_allowed,
        limit_access_enabled=limit_access_enabled,
        global_code_policy=global_code_policy,
        flamp_enabled=flamp_enabled,
    )
    published = published or reconciled

    summary = _summary_text(runtime_state)
    _persist_runtime_state(settings, runtime_state, summary)
    return VaracBbsVaultRunResult(
        enabled=True,
        scanned_events=scanned,
        processed_events=processed,
        published=published,
        active_location_id=runtime_state.current_location_id,
        current_session_callsign=runtime_state.current_session_callsign,
        summary=summary,
    )
