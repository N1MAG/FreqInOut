from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from freqinout.core.logger import log
from freqinout.core.nbems_compose import safe_varac_bbs_filename
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
MAX_PROCESSED_EVENT_KEYS = 256
MAX_MANIFEST_FILENAME_LENGTH = 180

EVENT_TS_RE = re.compile(r"^(?P<stamp>\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\s+-\s+(?P<body>.*)$")
COMMAND_RE = re.compile(r"\bBBS\s+OPEN\s+([A-Z0-9][A-Z0-9_.:/+\-]{0,63})\b", re.IGNORECASE)
DISCONNECT_RE = re.compile(r"\b(DISCONNECTED|CONNECTION\s+CLOSED|SESSION\s+CLOSED)\b", re.IGNORECASE)
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


@dataclass(frozen=True)
class VaultRuntimeState:
    current_location_id: str = DEFAULT_LOCATION_ID
    current_session_callsign: str = ""
    processed_event_keys: Tuple[str, ...] = ()
    cooldowns: Mapping[str, Mapping[str, float]] = field(default_factory=dict)
    failed_attempts: Mapping[str, Sequence[float]] = field(default_factory=dict)
    last_publish_manifest_path: str = ""
    last_publish_ts: float = 0.0
    last_action: str = ""
    last_request_ts: float = 0.0
    last_error: str = ""
    unmanaged_live_files: Tuple[str, ...] = ()


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
    raw_line: str = ""
    log_path: str = ""


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


def _normalize_callsign(value: object) -> str:
    return str(value or "").strip().upper()


def _clean_location_name(value: object) -> str:
    return " ".join(str(value or "").strip().split())


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
    try:
        return dt.datetime.strptime(stamp, "%m/%d/%Y %H:%M:%S").replace(tzinfo=dt.timezone.utc).timestamp()
    except Exception:
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


def parse_vault_log_events(text: str, *, trigger_mode: str = DEFAULT_TRIGGER_MODE, log_path: str = "") -> List[VaultLogEvent]:
    events: List[VaultLogEvent] = []
    exact_mode = str(trigger_mode or DEFAULT_TRIGGER_MODE).strip().lower() == "exact code only"
    for block in _split_log_events(text):
        lines = [line for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        match = EVENT_TS_RE.match(lines[0])
        if not match:
            continue
        body = "\n".join([match.group("body")] + lines[1:]).strip()
        sender = _extract_sender(body)
        code_text = ""
        kind = ""
        command_match = COMMAND_RE.search(body)
        if command_match:
            code_text = str(command_match.group(1) or "").strip()
            kind = "unlock"
        elif exact_mode:
            stripped = " ".join(body.split())
            if stripped:
                code_text = stripped
                kind = "unlock"
        elif DISCONNECT_RE.search(body):
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
    )


def vault_runtime_state_to_data(state: VaultRuntimeState) -> Dict[str, object]:
    return {
        "current_location_id": state.current_location_id,
        "current_session_callsign": state.current_session_callsign,
        "processed_event_keys": list(state.processed_event_keys),
        "cooldowns": {str(key): dict(value) for key, value in state.cooldowns.items()},
        "failed_attempts": {str(key): list(value) for key, value in state.failed_attempts.items()},
        "last_publish_manifest_path": state.last_publish_manifest_path,
        "last_publish_ts": float(state.last_publish_ts or 0.0),
        "last_action": state.last_action,
        "last_request_ts": float(state.last_request_ts or 0.0),
        "last_error": state.last_error,
        "unmanaged_live_files": list(state.unmanaged_live_files),
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


def build_publish_manifest(source_dir: object) -> Tuple[List[VaultPublishManifestEntry], int]:
    files, ignored_dirs = scan_location_files(source_dir)
    manifest: List[VaultPublishManifestEntry] = []
    used_names: set[str] = set()
    for child in files:
        try:
            st = child.stat()
        except OSError:
            continue
        safe_name = safe_varac_bbs_filename(child.name, max_len=MAX_MANIFEST_FILENAME_LENGTH)
        if safe_name in used_names:
            stem = Path(safe_name).stem
            suffix = Path(safe_name).suffix
            counter = 2
            candidate = f"{stem}-{counter}{suffix}"
            while candidate in used_names:
                counter += 1
                candidate = f"{stem}-{counter}{suffix}"
            safe_name = candidate
        used_names.add(safe_name)
        manifest.append(
            VaultPublishManifestEntry(
                source_name=child.name,
                live_name=safe_name,
                size=int(st.st_size or 0),
                mtime_ns=int(st.st_mtime_ns or 0),
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
    if src_root is None or dst_root is None:
        return True
    try:
        src_hash = _hash_file(src_root / a.source_name)
        dst_hash = _hash_file(dst_root / a.live_name)
    except Exception:
        return False
    return src_hash == dst_hash


def publish_location(location: VaultLocation, *, live_bbs_dir: object, managed_root: object) -> VaultPublishResult:
    live_dir = _resolve_path(live_bbs_dir)
    if live_dir is None:
        raise ValueError("Live BBS directory is required")
    live_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = _manifest_path_for(managed_root)
    previous_manifest = read_publish_manifest(manifest_path)
    previous_map = _entry_map(previous_manifest)
    next_manifest, ignored_dirs = build_publish_manifest(location.source_dir)
    next_map = _entry_map(next_manifest)
    src_root = _resolve_path(location.source_dir)
    if src_root is None:
        raise ValueError("Location source directory is required")

    published = 0
    for entry in next_manifest:
        src = src_root / entry.source_name
        dst = live_dir / entry.live_name
        previous = previous_map.get(entry.live_name)
        if previous is not None and dst.exists() and _entries_equal(entry, previous, src_root=src_root, dst_root=live_dir):
            continue
        tmp_name = f".fio-vault-{uuid.uuid4().hex}.tmp"
        tmp_path = live_dir / tmp_name
        shutil.copy2(src, tmp_path)
        os.replace(tmp_path, dst)
        published += 1

    removed = 0
    for live_name, entry in previous_map.items():
        if live_name in next_map:
            continue
        target = live_dir / live_name
        if target.exists():
            try:
                target.unlink()
                removed += 1
            except OSError:
                pass

    write_publish_manifest(manifest_path, next_manifest)

    unmanaged: List[str] = []
    tracked = set(next_map.keys())
    for child in sorted(live_dir.iterdir(), key=lambda item: item.name.lower()):
        if not child.is_file():
            continue
        if child.name.startswith(".fio-vault-"):
            continue
        if child.name not in tracked:
            unmanaged.append(child.name)

    changed = bool(published or removed or len(next_manifest) != len(previous_manifest))
    return VaultPublishResult(
        changed=changed,
        published_count=published,
        removed_count=removed,
        unmanaged_live_files=tuple(unmanaged),
        manifest_path=str(manifest_path),
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


def _location_allowed(sender: str, *, location: VaultLocation, global_allowed_callsigns: Sequence[str], limit_access_enabled: bool) -> bool:
    if not limit_access_enabled:
        return True
    normalized_sender = _normalize_callsign(sender)
    if not normalized_sender:
        return False
    global_allowed = {item for item in parse_callsign_list(global_allowed_callsigns)}
    if global_allowed and normalized_sender not in global_allowed:
        return False
    if location.inherit_global_allowed_callsigns:
        return True
    location_allowed = {item for item in parse_callsign_list(location.allowed_callsigns)}
    return not location_allowed or normalized_sender in location_allowed


def _prune_attempts(attempts: Sequence[float], *, now_ts: float, window_seconds: int) -> List[float]:
    lower = float(now_ts) - max(1, int(window_seconds))
    return [float(ts) for ts in attempts if float(ts) >= lower]


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
    now_ts = float(now_ts if now_ts is not None else time.time())
    sender = _normalize_callsign(sender)
    code_text = str(code_text or "").strip()
    state = load_vault_runtime_state(vault_runtime_state_to_data(runtime_state))
    if not sender:
        summary = "Managed Vault ignored request with no identifiable callsign."
        return VaultActionResult("ignored_no_sender", False, summary, state)

    cooldowns = {str(key): dict(value) for key, value in state.cooldowns.items()}
    current_cooldown = cooldowns.get(sender, {})
    until_ts = float(current_cooldown.get("until_ts", 0.0) or 0.0)
    if until_ts > now_ts:
        remaining = int(max(1.0, until_ts - now_ts))
        summary = f"Managed Vault cooldown active for {sender} ({remaining}s remaining)."
        next_state = VaultRuntimeState(
            **{
                **vault_runtime_state_to_data(state),
                "last_action": summary,
                "last_error": "cooldown_active",
            }
        )
        return VaultActionResult("cooldown_active", False, summary, next_state)

    default_location = next((loc for loc in locations if loc.id == default_location_id), None)
    if default_location is None:
        summary = "Managed Vault default location is missing."
        next_state = VaultRuntimeState(
            **{
                **vault_runtime_state_to_data(state),
                "last_action": summary,
                "last_error": "default_location_missing",
            }
        )
        return VaultActionResult("default_location_missing", False, summary, next_state)

    if state.current_session_callsign and state.current_session_callsign != sender and state.current_location_id != default_location_id:
        summary = f"Managed Vault session is locked to {state.current_session_callsign}."
        next_state = VaultRuntimeState(
            **{
                **vault_runtime_state_to_data(state),
                "last_action": summary,
                "last_error": "session_locked",
            }
        )
        return VaultActionResult("session_locked", False, summary, next_state)

    matched_location: Optional[VaultLocation] = None
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
        if len(sender_attempts) >= max(1, int(failed_attempt_limit)):
            cooldowns[sender] = {
                "until_ts": now_ts + max(1, int(cooldown_seconds)),
                "reason": "failed_attempt_limit",
            }
            summary = f"Managed Vault rejected access code from {sender}; cooldown applied."
            action = "cooldown_applied"
        else:
            summary = f"Managed Vault rejected access code from {sender}."
            action = "invalid_code"
        next_state = VaultRuntimeState(
            current_location_id=state.current_location_id,
            current_session_callsign=state.current_session_callsign,
            processed_event_keys=state.processed_event_keys,
            cooldowns=cooldowns,
            failed_attempts=failed_attempts,
            last_publish_manifest_path=state.last_publish_manifest_path,
            last_publish_ts=state.last_publish_ts,
            last_action=summary,
            last_request_ts=state.last_request_ts,
            last_error=action,
            unmanaged_live_files=state.unmanaged_live_files,
        )
        _append_audit_event(
            managed_root,
            {
                "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "action": action,
                "sender": sender,
                "location_id": "",
            },
        )
        return VaultActionResult(action, False, summary, next_state)

    if not _location_allowed(
        sender,
        location=matched_location,
        global_allowed_callsigns=global_allowed_callsigns,
        limit_access_enabled=limit_access_enabled,
    ):
        summary = f"Managed Vault rejected {sender}; callsign is not allowed for {matched_location.name}."
        next_state = VaultRuntimeState(
            current_location_id=state.current_location_id,
            current_session_callsign=state.current_session_callsign,
            processed_event_keys=state.processed_event_keys,
            cooldowns=cooldowns,
            failed_attempts=failed_attempts,
            last_publish_manifest_path=state.last_publish_manifest_path,
            last_publish_ts=state.last_publish_ts,
            last_action=summary,
            last_request_ts=state.last_request_ts,
            last_error="rejected_callsign",
            unmanaged_live_files=state.unmanaged_live_files,
        )
        _append_audit_event(
            managed_root,
            {
                "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "action": "rejected_callsign",
                "sender": sender,
                "location_id": matched_location.id,
            },
        )
        return VaultActionResult("rejected_callsign", False, summary, next_state)

    publish_result = publish_location(matched_location, live_bbs_dir=live_bbs_dir, managed_root=managed_root)
    failed_attempts.pop(sender, None)
    cooldowns.pop(sender, None)
    summary = f"Managed Vault published {matched_location.name} for {sender}."
    next_state = VaultRuntimeState(
        current_location_id=matched_location.id,
        current_session_callsign=sender,
        processed_event_keys=state.processed_event_keys,
        cooldowns=cooldowns,
        failed_attempts=failed_attempts,
        last_publish_manifest_path=publish_result.manifest_path,
        last_publish_ts=now_ts,
        last_action=summary,
        last_request_ts=now_ts,
        last_error="",
        unmanaged_live_files=publish_result.unmanaged_live_files,
    )
    _append_audit_event(
        managed_root,
        {
            "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "action": "authorized_open",
            "sender": sender,
            "location_id": matched_location.id,
            "location_name": matched_location.name,
        },
    )
    return VaultActionResult("authorized_open", True, summary, next_state, publish_result=publish_result)


def reset_to_default_location(
    *,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    default_location_id: str,
    runtime_state: VaultRuntimeState,
    reason: str = "manual_reset",
    now_ts: Optional[float] = None,
) -> VaultActionResult:
    now_ts = float(now_ts if now_ts is not None else time.time())
    default_location = next((loc for loc in locations if loc.id == default_location_id), None)
    state = load_vault_runtime_state(vault_runtime_state_to_data(runtime_state))
    if default_location is None:
        summary = "Managed Vault default location is missing."
        next_state = VaultRuntimeState(
            **{
                **vault_runtime_state_to_data(state),
                "last_action": summary,
                "last_error": "default_location_missing",
            }
        )
        return VaultActionResult("default_location_missing", False, summary, next_state)
    publish_result = publish_location(default_location, live_bbs_dir=live_bbs_dir, managed_root=managed_root)
    summary = f"Managed Vault returned to {default_location.name}."
    next_state = VaultRuntimeState(
        current_location_id=default_location.id,
        current_session_callsign="",
        processed_event_keys=state.processed_event_keys,
        cooldowns=state.cooldowns,
        failed_attempts=state.failed_attempts,
        last_publish_manifest_path=publish_result.manifest_path,
        last_publish_ts=now_ts,
        last_action=summary,
        last_request_ts=0.0,
        last_error="",
        unmanaged_live_files=publish_result.unmanaged_live_files,
    )
    _append_audit_event(
        managed_root,
        {
            "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "action": "returned_to_default",
            "reason": reason,
            "location_id": default_location.id,
        },
    )
    return VaultActionResult("returned_to_default", True, summary, next_state, publish_result=publish_result)


def _summary_location_name(locations: Sequence[VaultLocation], location_id: str) -> str:
    location = next((loc for loc in locations if loc.id == location_id), None)
    return location.name if location is not None else location_id or DEFAULT_LOCATION_NAME


def _persist_runtime_state(settings, state: VaultRuntimeState, summary: str) -> None:
    if settings is None:
        return
    try:
        settings.set("varac_bbs_vault_runtime_state_v1", vault_runtime_state_to_data(state))
        settings.set("varac_bbs_vault_last_summary", summary)
    except Exception as exc:
        log.debug("varac_bbs_vault: failed to persist runtime state: %s", exc)


def _reconcile_current_location(
    settings,
    *,
    locations: Sequence[VaultLocation],
    live_bbs_dir: object,
    managed_root: object,
    runtime_state: VaultRuntimeState,
) -> Tuple[VaultRuntimeState, bool]:
    current_location = next((loc for loc in locations if loc.id == runtime_state.current_location_id), None)
    if current_location is None or not current_location.enabled:
        return runtime_state, False
    try:
        publish_result = publish_location(current_location, live_bbs_dir=live_bbs_dir, managed_root=managed_root)
    except Exception as exc:
        summary = f"Managed Vault degraded: {exc}"
        next_state = VaultRuntimeState(
            **{
                **vault_runtime_state_to_data(runtime_state),
                "last_action": summary,
                "last_error": str(exc),
            }
        )
        return next_state, False
    next_state = VaultRuntimeState(
        **{
            **vault_runtime_state_to_data(runtime_state),
            "last_publish_manifest_path": publish_result.manifest_path,
            "last_publish_ts": time.time(),
            "unmanaged_live_files": list(publish_result.unmanaged_live_files),
        }
    )
    return next_state, bool(publish_result.changed)


def run_varac_bbs_vault(settings) -> VaracBbsVaultRunResult:
    enabled = bool(settings.get("varac_bbs_vault_enabled", False) if settings is not None else False)
    if not enabled:
        return VaracBbsVaultRunResult(False, 0, 0, False, DEFAULT_LOCATION_ID, "", "Managed Vault disabled")

    live_bbs_dir = str(settings.get("varac_bbs_dir", "") or "").strip() if settings is not None else ""
    managed_root = str(settings.get("varac_bbs_vault_managed_root", "") or "").strip() if settings is not None else ""
    default_location_id = str(settings.get("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID) or DEFAULT_LOCATION_ID).strip() or DEFAULT_LOCATION_ID
    trigger_mode = str(settings.get("varac_bbs_vault_trigger_mode", DEFAULT_TRIGGER_MODE) or DEFAULT_TRIGGER_MODE).strip() or DEFAULT_TRIGGER_MODE
    return_mode = str(settings.get("varac_bbs_vault_return_mode", DEFAULT_RETURN_MODE) or DEFAULT_RETURN_MODE).strip() or DEFAULT_RETURN_MODE
    failed_attempt_limit = int(settings.get("varac_bbs_vault_failed_attempt_limit", DEFAULT_FAILED_ATTEMPT_LIMIT) or DEFAULT_FAILED_ATTEMPT_LIMIT)
    failed_attempt_window_seconds = int(settings.get("varac_bbs_vault_failed_attempt_window_seconds", DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS) or DEFAULT_FAILED_ATTEMPT_WINDOW_SECONDS)
    cooldown_seconds = int(settings.get("varac_bbs_vault_cooldown_seconds", DEFAULT_COOLDOWN_SECONDS) or DEFAULT_COOLDOWN_SECONDS)
    idle_timeout_seconds = int(settings.get("varac_bbs_vault_idle_timeout_seconds", DEFAULT_IDLE_TIMEOUT_SECONDS) or DEFAULT_IDLE_TIMEOUT_SECONDS)
    locations = load_vault_locations(settings.get("varac_bbs_vault_locations_v1", []))
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
        error_state = VaultRuntimeState(
            **{
                **vault_runtime_state_to_data(runtime_state),
                "last_action": summary,
                "last_error": str(exc),
            }
        )
        _persist_runtime_state(settings, error_state, summary)
        return VaracBbsVaultRunResult(True, 0, 0, False, error_state.current_location_id, error_state.current_session_callsign, summary)

    now_ts = time.time()
    processed = 0
    scanned = 0
    published = False

    if not runtime_state.last_publish_manifest_path:
        runtime_state = VaultRuntimeState(
            **{
                **vault_runtime_state_to_data(runtime_state),
                "last_publish_manifest_path": str(_manifest_path_for(managed_root)),
            }
        )

    log_paths = resolve_varac_traffic_log_paths(settings)
    seen_keys = set(runtime_state.processed_event_keys)

    for log_path in log_paths:
        events = parse_vault_log_events(_read_tail(log_path), trigger_mode=trigger_mode, log_path=str(log_path))
        for event in events:
            scanned += 1
            key = _state_key(event)
            if key in seen_keys:
                continue
            should_mark = False
            if event.kind == "unlock":
                result = apply_unlock_request(
                    event.sender,
                    event.code_text,
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
                )
                runtime_state = result.runtime_state
                published = published or bool(result.publish_result and result.publish_result.changed)
                should_mark = True
                processed += 1
            elif event.kind == "disconnect" and runtime_state.current_session_callsign:
                event_sender = _normalize_callsign(event.sender)
                if not event_sender or event_sender == runtime_state.current_session_callsign:
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
                    should_mark = True
                    processed += 1
            if should_mark:
                seen_keys.add(key)

    processed_keys = tuple(list(seen_keys)[-MAX_PROCESSED_EVENT_KEYS:])
    runtime_state = VaultRuntimeState(
        **{
            **vault_runtime_state_to_data(runtime_state),
            "processed_event_keys": list(processed_keys),
        }
    )

    if runtime_state.current_location_id != default_location_id and return_mode != "Manual operator reset only":
        last_request_ts = float(runtime_state.last_request_ts or 0.0)
        if last_request_ts and (now_ts - last_request_ts) >= max(60, int(idle_timeout_seconds)):
            result = reset_to_default_location(
                locations=locations,
                live_bbs_dir=live_bbs_dir,
                managed_root=managed_root,
                default_location_id=default_location_id,
                runtime_state=runtime_state,
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
    )
    published = published or reconciled

    current_location_name = _summary_location_name(locations, runtime_state.current_location_id)
    summary = f"Managed Vault {current_location_name}"
    if runtime_state.current_session_callsign:
        summary += f" | Session {runtime_state.current_session_callsign}"
    if runtime_state.unmanaged_live_files:
        summary += f" | Unmanaged live files: {len(runtime_state.unmanaged_live_files)}"
    if runtime_state.last_error:
        summary += f" | {runtime_state.last_action or runtime_state.last_error}"
    elif runtime_state.last_action:
        summary += f" | {runtime_state.last_action}"

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
