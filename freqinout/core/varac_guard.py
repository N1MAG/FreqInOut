from __future__ import annotations

import datetime as dt
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from freqinout.core.logger import log
from freqinout.core.varac_log_parser import parse_varac_event_timestamp_to_epoch
from freqinout.core.varac_bbs_config import parse_callsign_list
from freqinout.core.varac_file_action import delete_file, quarantine_file, VaracFileActionResult


EVENT_TS_RE = re.compile(r"^(?P<stamp>\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\s+-\s+(?P<body>.*)$")
CALLSIGN_RE = re.compile(r"\b([A-Z0-9/]{3,15})\b")
FILENAME_RE = re.compile(r"(?:FILE|FILENAME|NAME|AS)\s*[:=]\s*([^\r\n]+)", re.IGNORECASE)
QUOTED_FILE_RE = re.compile(r'"([^"]+\.(?:b2s|k2s|txt|rtf|html?|sig|asc|gpg)(?:\.[A-Za-z0-9]+)?)"', re.IGNORECASE)
TRAILING_FILE_RE = re.compile(r"([A-Za-z0-9_.\- ]+\.(?:b2s|k2s|txt|rtf|html?|sig|asc|gpg))(?:\s|$)", re.IGNORECASE)
FIO_HELPER_FILE_PREFIXES = (
    "BBS MSG - ",
    "BBS_QUEUE_LIST",
    "BBS_BLOCK_LIST",
)


@dataclass(frozen=True)
class VaracTransferEvent:
    timestamp_utc: float
    sender: str
    filename: str
    raw_line: str
    log_path: str


@dataclass(frozen=True)
class VaracGuardDecision:
    action: str
    reason: str
    sender: str = ""
    filename: str = ""
    source_path: str = ""
    destination_path: str = ""
    log_path: str = ""


@dataclass(frozen=True)
class VaracGuardRunResult:
    scanned_events: int
    processed_events: int
    allowed_events: int
    unauthorized_events: int
    deleted_files: int
    quarantined_files: int
    pending_events: int
    skipped_events: int
    summary: str


def _normalize_call(value: object) -> str:
    return str(value or "").strip().upper()


def _resolve_varac_base_paths(settings) -> List[Path]:
    bases: List[Path] = []
    for raw in (
        settings.get("varac_path", "") if settings is not None else "",
        settings.get("varac_db_path", "") if settings is not None else "",
    ):
        txt = str(raw or "").strip()
        if not txt:
            continue
        try:
            path = Path(txt).expanduser()
        except Exception:
            continue
        if path.is_file():
            bases.append(path.parent)
        else:
            bases.append(path)
    message_paths = settings.get("message_paths", {}) if settings is not None else {}
    incoming = str((message_paths or {}).get("varac", "") or "").strip()
    if incoming:
        try:
            path = Path(incoming).expanduser()
            if path.exists():
                bases.append(path.parent)
        except Exception:
            pass
    uniq: List[Path] = []
    seen: set[str] = set()
    for base in bases:
        key = str(base).lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(base)
    return uniq


def resolve_varac_traffic_log_paths(settings) -> List[Path]:
    out: List[Path] = []
    for base in _resolve_varac_base_paths(settings):
        for name in ("VarAC_traffic.log", "VarAC.log", "varalog.log"):
            path = base / name
            if path.exists():
                out.append(path)
    return out


def _read_tail(path: Path, max_bytes: int = 32768) -> str:
    try:
        with path.open("rb") as handle:
            try:
                handle.seek(-max_bytes, os.SEEK_END)
            except OSError:
                handle.seek(0)
            return handle.read().decode("utf-8", errors="replace")
    except Exception as exc:
        log.debug("varac_guard: failed to tail %s: %s", path, exc)
        return ""


def _parse_timestamp(stamp: str) -> float:
    return parse_varac_event_timestamp_to_epoch(stamp)


def _split_events(text: str) -> List[str]:
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
    patterns = [
        r"\bFROM\b\s*[:=]?\s*([A-Z0-9/]{3,15})",
        r"\bDE\b\s*([A-Z0-9/]{3,15})",
        r"\bSENDER\b\s*[:=]?\s*([A-Z0-9/]{3,15})",
        r"\bCALLSIGN\b\s*[:=]?\s*([A-Z0-9/]{3,15})",
    ]
    upper = body.upper()
    for pattern in patterns:
        match = re.search(pattern, upper)
        if match:
            return _normalize_call(match.group(1))
    if "FILE SUCCESSFULLY RECEIVED" in upper:
        tokens = [tok for tok in CALLSIGN_RE.findall(upper) if tok not in {"FILE", "SUCCESSFULLY", "RECEIVED", "FROM", "SENDER", "CALLSIGN"}]
        if tokens:
            return _normalize_call(tokens[0])
    return ""


def _extract_filename(body: str) -> str:
    for regex in (FILENAME_RE, QUOTED_FILE_RE, TRAILING_FILE_RE):
        match = regex.search(body)
        if match:
            candidate = str(match.group(1) or "").strip().strip(".")
            if candidate:
                return candidate
    return ""


def parse_varac_transfer_events(text: str, *, log_path: str = "") -> List[VaracTransferEvent]:
    events: List[VaracTransferEvent] = []
    for block in _split_events(text):
        lines = [line for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        match = EVENT_TS_RE.match(lines[0])
        if not match:
            continue
        body = "\n".join([match.group("body")] + lines[1:]).strip()
        upper = body.upper()
        if "FILE SUCCESSFULLY RECEIVED" not in upper:
            continue
        sender = _extract_sender(body)
        filename = _extract_filename(body)
        events.append(
            VaracTransferEvent(
                timestamp_utc=_parse_timestamp(match.group("stamp")),
                sender=sender,
                filename=filename,
                raw_line=block,
                log_path=log_path,
            )
        )
    return events


def _load_guard_state(settings) -> dict:
    raw = settings.get("varac_guard_state_v1", {}) if settings is not None else {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _save_guard_state(settings, state: dict) -> None:
    if settings is None:
        return
    try:
        settings.set("varac_guard_state_v1", state)
    except Exception as exc:
        log.debug("varac_guard: failed to persist state: %s", exc)


def _state_key(event: VaracTransferEvent) -> str:
    return "|".join(
        [
            event.log_path,
            str(int(event.timestamp_utc or 0.0)),
            event.sender,
            event.filename.lower(),
        ]
    )


def _candidate_files(incoming_dir: Path, filename: str) -> List[Path]:
    if not filename:
        return []
    candidates: List[Path] = []
    exact = incoming_dir / filename
    if exact.exists():
        candidates.append(exact)
    wanted = filename.lower()
    for child in incoming_dir.iterdir():
        if not child.is_file():
            continue
        if child.name.lower() == wanted:
            candidates.append(child)
    return candidates


def _latest_recent_file(incoming_dir: Path, *, since_ts: float, retry_seconds: int) -> Optional[Path]:
    newest: Optional[Path] = None
    newest_mtime = 0.0
    deadline = since_ts - max(5, int(retry_seconds))
    for child in incoming_dir.iterdir():
        if not child.is_file():
            continue
        try:
            st = child.stat()
        except Exception:
            continue
        mtime = float(st.st_mtime or 0.0)
        if mtime < deadline:
            continue
        if mtime > newest_mtime:
            newest = child
            newest_mtime = mtime
    return newest


def _resolve_incoming_dir(settings) -> Optional[Path]:
    raw = ""
    if settings is not None:
        raw = str(settings.get("varac_incoming_path", "") or "").strip()
        if not raw:
            message_paths = settings.get("message_paths", {}) or {}
            raw = str(message_paths.get("varac", "") or "").strip()
    if not raw:
        return None
    try:
        path = Path(raw).expanduser()
    except Exception:
        return None
    if not path.exists() or not path.is_dir():
        return None
    return path


def _resolve_quarantine_dir(settings, incoming_dir: Path) -> Path:
    raw = str(settings.get("varac_guard_quarantine_dir", "") or "").strip() if settings is not None else ""
    if raw:
        try:
            path = Path(raw).expanduser()
            return path
        except Exception:
            pass
    managed_root = str(settings.get("varac_bbs_vault_managed_root", "") or "").strip() if settings is not None else ""
    if managed_root:
        return Path(managed_root).expanduser() / "quarantine"
    bbs_dir = str(settings.get("varac_bbs_dir", "") or "").strip() if settings is not None else ""
    if bbs_dir:
        bbs_path = Path(bbs_dir).expanduser()
        return bbs_path.parent / "FIO_BBS_Vault" / "quarantine"
    return incoming_dir.parent / "FIO_BBS_Vault" / "quarantine"


def _is_fio_generated_helper_file(filename: object) -> bool:
    clean = Path(str(filename or "").strip()).name
    upper = clean.upper()
    return any(upper.startswith(prefix.upper()) for prefix in FIO_HELPER_FILE_PREFIXES)


def evaluate_varac_guard_event(
    event: VaracTransferEvent,
    *,
    settings,
    state: Optional[dict] = None,
    now_utc: Optional[float] = None,
    retry_seconds: int = 120,
) -> tuple[VaracGuardDecision, Optional[VaracFileActionResult], bool]:
    incoming_dir = _resolve_incoming_dir(settings)
    if incoming_dir is None:
        return (
            VaracGuardDecision(action="skip", reason="incoming_folder_missing", sender=event.sender, filename=event.filename, log_path=event.log_path),
            None,
            False,
        )

    allowed = set(parse_callsign_list(settings.get("varac_bbs_allowed_callsigns", "") if settings is not None else ""))
    sender = _normalize_call(event.sender)
    if sender and sender in allowed:
        return (
            VaracGuardDecision(action="allow", reason="authorized_sender", sender=sender, filename=event.filename, log_path=event.log_path),
            None,
            True,
        )

    mode = str(settings.get("varac_guard_mode", "Log only") or "Log only").strip().lower() if settings is not None else "log only"
    state = state or {}
    filename = str(event.filename or "").strip()
    if _is_fio_generated_helper_file(filename):
        return (
            VaracGuardDecision(action="skip", reason="fio_helper_file", sender=sender, filename=filename, log_path=event.log_path),
            None,
            True,
        )

    candidates = _candidate_files(incoming_dir, filename)
    if not candidates and event.timestamp_utc > 0:
        fallback = _latest_recent_file(incoming_dir, since_ts=event.timestamp_utc, retry_seconds=retry_seconds)
        if fallback is not None:
            candidates = [fallback]

    if not candidates:
        age = 0.0
        if now_utc is not None and event.timestamp_utc > 0:
            age = max(0.0, float(now_utc) - float(event.timestamp_utc))
        if age < float(retry_seconds):
            return (
                VaracGuardDecision(action="pending", reason="waiting_for_delayed_file_write", sender=sender, filename=filename, log_path=event.log_path),
                None,
                False,
            )
        return (
            VaracGuardDecision(action="skip", reason="file_not_found", sender=sender, filename=filename, log_path=event.log_path),
            None,
            True,
        )

    src = candidates[0]
    if _is_fio_generated_helper_file(src.name):
        return (
            VaracGuardDecision(action="skip", reason="fio_helper_file", sender=sender, filename=src.name, source_path=str(src), log_path=event.log_path),
            None,
            True,
        )
    try:
        st = src.stat()
    except Exception:
        return (
            VaracGuardDecision(action="pending", reason="file_stat_unavailable", sender=sender, filename=filename, source_path=str(src), log_path=event.log_path),
            None,
            False,
        )

    if event.timestamp_utc > 0 and float(st.st_mtime or 0.0) < (float(event.timestamp_utc) - 30.0):
        return (
            VaracGuardDecision(action="skip", reason="preexisting_file", sender=sender, filename=filename, source_path=str(src), log_path=event.log_path),
            None,
            True,
        )

    if mode == "log only":
        return (
            VaracGuardDecision(action="log_only", reason="unauthorized_sender", sender=sender, filename=filename, source_path=str(src), log_path=event.log_path),
            None,
            True,
        )

    if mode == "delete unauthorized files":
        result = delete_file(src)
        return (
            VaracGuardDecision(action="delete", reason="unauthorized_sender", sender=sender, filename=filename, source_path=str(src), log_path=event.log_path),
            result,
            True,
        )

    if mode == "quarantine unauthorized files":
        quarantine_dir = _resolve_quarantine_dir(settings, incoming_dir)
        result = quarantine_file(src, quarantine_dir)
        return (
            VaracGuardDecision(action="quarantine", reason="unauthorized_sender", sender=sender, filename=filename, source_path=str(src), destination_path=result.destination, log_path=event.log_path),
            result,
            True,
        )

    return (
        VaracGuardDecision(action="skip", reason="unknown_mode", sender=sender, filename=filename, source_path=str(src), log_path=event.log_path),
        None,
        True,
    )


def run_varac_guard(settings, *, retry_seconds: Optional[int] = None) -> VaracGuardRunResult:
    if not bool(settings.get("varac_guard_enabled", False) if settings is not None else False):
        return VaracGuardRunResult(0, 0, 0, 0, 0, 0, 0, 0, "VGuard disabled")

    log_paths = resolve_varac_traffic_log_paths(settings)
    if not log_paths:
        summary = "VGuard enabled, but no VarAC traffic log was found"
        try:
            settings.set("varac_guard_last_summary", summary)
        except Exception:
            pass
        return VaracGuardRunResult(0, 0, 0, 0, 0, 0, 0, 0, summary)

    retry_seconds = int(retry_seconds if retry_seconds is not None else settings.get("varac_guard_retry_seconds", 120) if settings is not None else 120)
    state = _load_guard_state(settings)
    seen = set(str(x) for x in state.get("processed_event_keys", []) if str(x).strip())
    processed_keys: List[str] = list(state.get("processed_event_keys", [])) if isinstance(state.get("processed_event_keys", []), list) else []
    now_utc = dt.datetime.now(dt.timezone.utc).timestamp()

    scanned = 0
    processed = 0
    allowed = 0
    unauthorized = 0
    deleted = 0
    quarantined = 0
    pending = 0
    skipped = 0

    for log_path in log_paths:
        events = parse_varac_transfer_events(_read_tail(log_path), log_path=str(log_path))
        for event in events:
            scanned += 1
            key = _state_key(event)
            if key in seen:
                skipped += 1
                continue
            decision, action_result, should_mark = evaluate_varac_guard_event(
                event,
                settings=settings,
                state=state,
                now_utc=now_utc,
                retry_seconds=retry_seconds,
            )
            if decision.action == "allow":
                allowed += 1
            elif decision.action == "pending":
                pending += 1
            elif decision.action == "skip":
                skipped += 1
            else:
                processed += 1
                unauthorized += 1
                if decision.action == "delete":
                    deleted += 1
                elif decision.action == "quarantine":
                    quarantined += 1
            if should_mark and decision.action != "pending":
                seen.add(key)
                processed_keys.append(key)
                if len(processed_keys) > 256:
                    processed_keys = processed_keys[-256:]
            if action_result is not None:
                log.debug("varac_guard: %s", action_result)

    state["processed_event_keys"] = processed_keys
    state["last_run_utc"] = now_utc
    state["last_summary"] = {
        "scanned_events": scanned,
        "processed_events": processed,
        "allowed_events": allowed,
        "unauthorized_events": unauthorized,
        "deleted_files": deleted,
        "quarantined_files": quarantined,
        "pending_events": pending,
        "skipped_events": skipped,
    }
    _save_guard_state(settings, state)

    summary = (
        f"VGuard {str(settings.get('varac_guard_mode', 'Log only') or 'Log only')} | "
        f"scanned {scanned}, processed {processed}, allowed {allowed}, unauthorized {unauthorized}, "
        f"deleted {deleted}, quarantined {quarantined}, pending {pending}, skipped {skipped}"
    )
    try:
        settings.set("varac_guard_last_summary", summary)
    except Exception:
        pass
    return VaracGuardRunResult(
        scanned_events=scanned,
        processed_events=processed,
        allowed_events=allowed,
        unauthorized_events=unauthorized,
        deleted_files=deleted,
        quarantined_files=quarantined,
        pending_events=pending,
        skipped_events=skipped,
        summary=summary,
    )
