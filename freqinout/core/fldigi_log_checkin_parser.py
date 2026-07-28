from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional


LOG_LINE_RE = re.compile(
    r"^(?P<direction>RX|TX)\s+\d+\s*:\s*FLDIGI\s*\((?P<ts>[^)]+)\):\s*(?P<payload>.*)$",
    re.IGNORECASE,
)
CALLSIGN_RE = re.compile(r"^[A-Z][A-Z0-9]{2,}(?:/[A-Z0-9]+)?$")
STATE_RE = re.compile(r"^[A-Z]{2}$")
QRU_RE = re.compile(r"\b(?:QRU|NO\s+TFC|NO\s+TRAFFIC|NO\s+MSG|NO\s+MESSAGES)\b", re.IGNORECASE)


@dataclass(frozen=True)
class FldigiLogCheckinCandidate:
    callsign: str
    name: str
    state: str
    traffic: str
    bucket: str
    confidence: str
    rx: bool = True
    timestamp_utc: Optional[datetime.datetime] = None
    tx_context: str = ""
    raw_payload: str = ""

    def completeness_score(self) -> int:
        return int(bool(self.callsign)) + int(bool(self.name)) + int(bool(self.state))


def _parse_ts(value: object) -> Optional[datetime.datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%MZ", "%Y-%m-%d %H:%M:%SZ", "%Y-%m-%d %H:%M"):
        try:
            return datetime.datetime.strptime(text, fmt).replace(tzinfo=datetime.timezone.utc)
        except Exception:
            continue
    try:
        parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def _normalize_traffic(value: object) -> tuple[str, str]:
    text = str(value or "").strip()
    if not text or QRU_RE.search(text):
        return "QRU", "QRU"
    return text.upper(), "TFC"


def _identity_lookup(
    callsign: str,
    lookup_identity: Optional[Callable[[str], dict[str, str]]],
) -> dict[str, str]:
    if lookup_identity is None:
        return {}
    try:
        result = lookup_identity(callsign)
    except Exception:
        return {}
    return result if isinstance(result, dict) else {}


def parse_fldigi_log_payload(
    payload: str,
    *,
    timestamp_utc: Optional[datetime.datetime] = None,
    tx_context: str = "",
    lookup_identity: Optional[Callable[[str], dict[str, str]]] = None,
) -> Optional[FldigiLogCheckinCandidate]:
    text = str(payload or "").strip()
    if not text:
        return None
    parts = [part.strip() for part in text.split("/") if part.strip()]
    if not parts:
        return None
    callsign = parts[0].split()[0].strip().upper()
    if not CALLSIGN_RE.match(callsign):
        return None

    name = ""
    state = ""
    traffic = ""
    if len(parts) >= 4:
        name, state, traffic = parts[1], parts[2].split()[0], parts[3]
    elif len(parts) == 3:
        second = parts[1].split()[0].strip().upper()
        if STATE_RE.match(second):
            state, traffic = second, parts[2]
        else:
            name, state = parts[1], parts[2].split()[0]
    elif len(parts) == 2:
        second = parts[1].strip()
        token = second.split()[0].upper() if second.split() else ""
        if QRU_RE.search(second) or re.search(r"\d", second):
            traffic = second
        elif STATE_RE.match(token):
            state = token
        else:
            name = second

    identity = _identity_lookup(callsign, lookup_identity)
    name = name.strip() or str(identity.get("name", "") or "").strip()
    state = state.strip().upper() or str(identity.get("state", "") or "").strip().upper()
    traffic, bucket = _normalize_traffic(traffic)
    complete = bool(name and state)
    if not complete and bucket == "TFC":
        bucket = "REVIEW"
    confidence = "high" if complete else "low"
    return FldigiLogCheckinCandidate(
        callsign=callsign,
        name=name,
        state=state,
        traffic=traffic,
        bucket=bucket,
        confidence=confidence,
        rx=True,
        timestamp_utc=timestamp_utc,
        tx_context=str(tx_context or "").strip(),
        raw_payload=text,
    )


def _dedupe_key(candidate: FldigiLogCheckinCandidate) -> str:
    return "|".join(
        [
            candidate.callsign,
            candidate.name.strip().upper(),
            candidate.state,
            candidate.traffic.strip().upper(),
            candidate.bucket,
        ]
    )


def _iter_new_log_lines(path: Path, start_offset: int) -> tuple[Iterable[str], int]:
    if not path.exists():
        return [], 0
    start = max(0, int(start_offset or 0))
    size = path.stat().st_size
    if start > size:
        start = 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(start)
        text = handle.read()
        return text.splitlines(), size


def scan_fldigi_log_file(
    path: str | Path,
    *,
    start_offset: int = 0,
    session_start_utc: Optional[datetime.datetime] = None,
    seen_normalized: Optional[set[str]] = None,
    last_tx_context: str = "",
    include_tx_context: bool = True,
    lookup_identity: Optional[Callable[[str], dict[str, str]]] = None,
) -> tuple[list[FldigiLogCheckinCandidate], int, str]:
    log_path = Path(path)
    lines, new_offset = _iter_new_log_lines(log_path, start_offset)
    seen = seen_normalized if seen_normalized is not None else set()
    candidates: list[FldigiLogCheckinCandidate] = []
    tx_context = str(last_tx_context or "").strip() if include_tx_context else ""
    session_start = session_start_utc
    if session_start is not None and session_start.tzinfo is None:
        session_start = session_start.replace(tzinfo=datetime.timezone.utc)
    for line in lines:
        match = LOG_LINE_RE.match(str(line or "").strip())
        if not match:
            continue
        direction = match.group("direction").upper()
        payload = match.group("payload").strip()
        ts = _parse_ts(match.group("ts"))
        if session_start is not None and ts is not None and ts < session_start:
            continue
        if direction == "TX":
            if include_tx_context and payload:
                tx_context = payload
            continue
        candidate = parse_fldigi_log_payload(
            payload,
            timestamp_utc=ts,
            tx_context=tx_context if include_tx_context else "",
            lookup_identity=lookup_identity,
        )
        if candidate is None:
            continue
        key = _dedupe_key(candidate)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(candidate)
    return candidates, int(new_offset), tx_context if include_tx_context else ""
