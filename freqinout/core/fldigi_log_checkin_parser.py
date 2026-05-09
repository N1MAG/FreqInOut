from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from freqinout.core.checkins_db import lookup_operator_identity

_LOG_LINE_RE = re.compile(
    r"^(RX|TX)\s+\d+\s+:\s+.+?\((\d{4}-\d{2}-\d{2} \d{2}:\d{2})Z\):\s*(.*)$"
)
_TRAFFIC_RE = re.compile(r"\b([1-9]\d*)\s*(RR|PP|QST)\b", re.IGNORECASE)
_NO_TRAFFIC_RE = re.compile(r"\bNO\s+(?:TFC|TRAFFIC)\b", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[A-Za-z0-9/']+")
_CALLSIGN_RE = re.compile(r"^[A-Z0-9]{2,}[A-Z0-9/]{1,}$")
_STATE_RE = re.compile(r"^[A-Z]{2}$")
_TRAILING_CALL_NOISE_RE = re.compile(r"[^A-Z0-9/]+$")
_PORTABLE_SUFFIX_RE = re.compile(r"/(P|M|MM|QRP|SOTA|ROVER|[A-Z0-9]{1,4})$")


@dataclass(slots=True)
class FldigiLogCheckinCandidate:
    raw_line: str
    normalized_line: str
    callsign: str
    name: str
    state: str
    traffic: str
    bucket: str
    confidence: str
    timestamp_utc: Optional[datetime.datetime] = None
    rx: bool = True
    tx_context: str = ""
    review_reason: str = ""
    enriched: bool = False

    def completeness_score(self) -> int:
        score = 0
        if self.callsign:
            score += 1
        if self.name:
            score += 1
        if self.state:
            score += 1
        if self.traffic:
            score += 1
        return score



def _normalize_callsign(value: object) -> str:
    cs = str(value or "").strip().upper()
    if not cs:
        return ""
    cs = _TRAILING_CALL_NOISE_RE.sub("", cs)
    if not cs:
        return ""
    return _PORTABLE_SUFFIX_RE.sub("", cs)



def _normalize_text(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip().upper()
    text = text.replace(" / ", "/")
    text = text.replace("/ ", "/").replace(" /", "/")
    return text


def _normalize_tx_context(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()



def _timestamp_from_log_text(ts_text: str) -> Optional[datetime.datetime]:
    try:
        ts_val = datetime.datetime.strptime(str(ts_text), "%Y-%m-%d %H:%M")
    except Exception:
        return None
    return ts_val.replace(tzinfo=datetime.timezone.utc)



def _is_state_token(token: str) -> bool:
    return bool(_STATE_RE.match(str(token or "").strip().upper()))



def _extract_traffic(payload: str) -> tuple[str, str]:
    if _NO_TRAFFIC_RE.search(payload):
        return "QRU", "No TFC / No Traffic"
    match = _TRAFFIC_RE.search(payload)
    if not match:
        return "", ""
    count = int(match.group(1))
    suffix = match.group(2).upper()
    return f"{count}{suffix}", ""



def _clean_tokens(payload: str) -> list[str]:
    tokens = []
    for raw_token in str(payload or "").split():
        token = raw_token.strip().strip(",;:!?()[]{}<>")
        if not token:
            continue
        if token.upper() == "DE" and not tokens:
            continue
        tokens.append(token)
    return tokens



def _strip_traffic_tokens(payload: str) -> str:
    text = _NO_TRAFFIC_RE.sub(" ", str(payload or ""))
    text = _TRAFFIC_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()



def _parse_identity(tokens: list[str], *, traffic_token_present: bool) -> tuple[str, str, str]:
    if not tokens:
        return "", "", ""
    callsign = _normalize_callsign(tokens[0])
    if not callsign or not _CALLSIGN_RE.match(callsign):
        return "", "", ""
    if len(tokens) == 1:
        return callsign, "", ""

    name_tokens = tokens[1:]
    state = ""
    if traffic_token_present and len(name_tokens) >= 2:
        maybe_state = name_tokens[-1].upper()
        if _is_state_token(maybe_state):
            state = maybe_state
            name_tokens = name_tokens[:-1]
    elif len(name_tokens) >= 2:
        maybe_state = name_tokens[-1].upper()
        if _is_state_token(maybe_state):
            state = maybe_state
            name_tokens = name_tokens[:-1]

    name = " ".join(name_tokens).strip()
    if name.upper() == callsign:
        name = ""
    return callsign, name, state



def _parse_slash_payload(payload: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in str(payload or "").split("/")]
    if not parts or not parts[0].strip():
        return "", "", ""
    callsign = _normalize_callsign(parts[0].split()[0])
    if not callsign or not _CALLSIGN_RE.match(callsign):
        return "", "", ""
    name = parts[1].strip() if len(parts) > 1 else ""
    state = ""
    if len(parts) > 2 and parts[2].strip():
        maybe_state = parts[2].split()[0].strip().upper()
        if _is_state_token(maybe_state):
            state = maybe_state
    if name.upper() == callsign:
        name = ""
    return callsign, name, state



def _format_candidate_line(callsign: str, name: str, state: str, traffic: str) -> str:
    parts = [p for p in (callsign.strip().upper(), name.strip(), state.strip().upper(), traffic.strip().upper()) if p]
    return " / ".join(parts)



def _classify_candidate(
    callsign: str,
    name: str,
    state: str,
    traffic: str,
    *,
    enriched: bool,
) -> tuple[str, str]:
    if not callsign:
        return "", ""
    if traffic == "QRU" and name and state:
        return "QRU", "high"
    if traffic and traffic != "QRU" and name and state:
        return "TFC", "high"
    if traffic and traffic != "QRU" and enriched and name and state:
        return "TFC", "high"
    if traffic == "QRU" and enriched and name and state:
        return "QRU", "high"
    if traffic:
        return "REVIEW", "medium" if (name or state) else "low"
    if name or state:
        return "REVIEW", "medium"
    return "REVIEW", "low"



def parse_fldigi_log_payload(
    payload: str,
    *,
    timestamp_utc: Optional[datetime.datetime] = None,
    tx_context: str = "",
    lookup_identity: Optional[Callable[[str], tuple[str, str] | dict[str, str] | None]] = None,
) -> Optional[FldigiLogCheckinCandidate]:
    raw = str(payload or "").strip()
    if not raw:
        return None

    traffic, traffic_reason = _extract_traffic(raw)
    identity_source = _strip_traffic_tokens(raw)
    if " / " in identity_source or identity_source.count("/") >= 2 or identity_source.rstrip().endswith("/"):
        callsign, name, state = _parse_slash_payload(identity_source)
    else:
        tokens = _clean_tokens(identity_source)
        callsign, name, state = _parse_identity(tokens, traffic_token_present=bool(traffic))
    if not callsign:
        return None

    enriched = False
    if lookup_identity is None:
        lookup_identity = lookup_operator_identity
    if (not name or not state) and lookup_identity is not None:
        try:
            lookup = lookup_identity(callsign)
        except Exception:
            lookup = None
        looked_name = ""
        looked_state = ""
        if isinstance(lookup, dict):
            looked_name = str(lookup.get("name") or "").strip()
            looked_state = str(lookup.get("state") or "").strip().upper()
        elif isinstance(lookup, tuple) and len(lookup) >= 2:
            looked_name = str(lookup[0] or "").strip()
            looked_state = str(lookup[1] or "").strip().upper()
        if looked_name and not name:
            name = looked_name
            enriched = True
        if looked_state and not state:
            state = looked_state
            enriched = True

    bucket, confidence = _classify_candidate(callsign, name, state, traffic, enriched=enriched)
    if not bucket:
        return None

    normalized_line = _normalize_text(raw)
    review_reason = traffic_reason
    if bucket == "REVIEW":
        if not traffic:
            review_reason = "missing traffic"
        elif not (name and state):
            review_reason = "partial decode"
    elif not review_reason:
        review_reason = ""

    return FldigiLogCheckinCandidate(
        raw_line=raw,
        normalized_line=normalized_line,
        callsign=callsign,
        name=name,
        state=state,
        traffic=traffic,
        bucket=bucket,
        confidence=confidence,
        timestamp_utc=timestamp_utc,
        rx=True,
        tx_context=_normalize_tx_context(tx_context),
        review_reason=review_reason,
        enriched=enriched,
    )



def _timestamp_is_within_session(
    line_ts: Optional[datetime.datetime],
    session_start_utc: Optional[datetime.datetime],
) -> bool:
    if line_ts is None or session_start_utc is None:
        return True
    try:
        if session_start_utc.tzinfo is None:
            session_start_utc = session_start_utc.replace(tzinfo=datetime.timezone.utc)
        else:
            session_start_utc = session_start_utc.astimezone(datetime.timezone.utc)
    except Exception:
        return True
    try:
        return (line_ts + datetime.timedelta(seconds=59)) >= session_start_utc
    except Exception:
        return True



def resolve_fldigi_log_path(raw_path: str) -> Optional[Path]:
    raw = str(raw_path or "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if path.is_dir():
        candidates = sorted(path.glob("fldigi*.log"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
        return candidates[-1] if candidates else None
    if path.is_file():
        try:
            latest = sorted(path.parent.glob("fldigi*.log"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
        except Exception:
            latest = []
        if latest and path.exists():
            try:
                if latest[-1].stat().st_mtime >= path.stat().st_mtime:
                    return latest[-1]
            except Exception:
                pass
        return path
    if path.parent.exists() and path.parent.is_dir():
        candidates = sorted(path.parent.glob("fldigi*.log"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
        return candidates[-1] if candidates else None
    return None



def scan_fldigi_log_file(
    path: Path,
    *,
    start_offset: int = 0,
    session_start_utc: Optional[datetime.datetime] = None,
    last_tx_context: str = "",
    include_tx_context: bool = True,
    lookup_identity: Optional[Callable[[str], tuple[str, str] | dict[str, str] | None]] = None,
    seen_normalized: Optional[set[str]] = None,
) -> tuple[list[FldigiLogCheckinCandidate], int, str]:
    candidates: list[FldigiLogCheckinCandidate] = []
    if path is None:
        return candidates, 0, _normalize_tx_context(last_tx_context) if include_tx_context else ""

    try:
        stat = path.stat()
    except Exception:
        return candidates, 0, _normalize_tx_context(last_tx_context) if include_tx_context else ""

    offset = max(0, int(start_offset or 0))
    if stat.st_size < offset:
        offset = 0

    current_tx_context = _normalize_tx_context(last_tx_context) if include_tx_context else ""

    try:
        with path.open("rb") as fh:
            fh.seek(offset)
            data = fh.read()
            new_offset = fh.tell()
    except Exception:
        return candidates, offset, current_tx_context

    if not data:
        return candidates, new_offset, current_tx_context

    text = data.decode("utf-8", errors="replace")
    for line in text.splitlines():
        match = _LOG_LINE_RE.match(line.strip())
        if not match:
            continue
        line_kind, ts_text, payload = match.groups()
        timestamp_utc = _timestamp_from_log_text(ts_text)
        if not _timestamp_is_within_session(timestamp_utc, session_start_utc):
            continue
        if line_kind == "TX":
            if include_tx_context:
                current_tx_context = _normalize_tx_context(payload)
            else:
                current_tx_context = ""
            continue
        candidate = parse_fldigi_log_payload(
            payload,
            timestamp_utc=timestamp_utc,
            tx_context=current_tx_context,
            lookup_identity=lookup_identity,
        )
        if candidate is None:
            continue
        if seen_normalized is not None:
            if candidate.normalized_line in seen_normalized:
                continue
            seen_normalized.add(candidate.normalized_line)
        candidates.append(candidate)
    return candidates, new_offset, current_tx_context
