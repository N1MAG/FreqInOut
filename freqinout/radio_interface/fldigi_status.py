from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager


_RX_LINE_RE = re.compile(r"^RX\s+\d+\s+:\s+.+?\((\d{4}-\d{2}-\d{2} \d{2}:\d{2})Z\):\s*(.*)$")
_CALLSIGN_RE = re.compile(r"\bDE\s+[A-Z0-9/]{3,}\b", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[A-Za-z']+")
_PRINTABLE_RE = re.compile(r"[ -~]")

_FLMSG_MARKERS = (
    "[WRAP:beg]",
    "[WRAP:fn",
    "<flmsg>",
    ":hdr_",
    ":mg:",
)
_FLAMP_PREFIXES = (
    "<PROG",
    "<FILE",
    "<ID",
    "<SIZE",
    "<DATA",
)

_COMMON_WORDS = {
    "a", "about", "after", "all", "also", "an", "and", "any", "are", "as", "at", "be", "been",
    "but", "by", "can", "do", "for", "from", "get", "had", "has", "have", "he", "her", "here",
    "his", "how", "i", "if", "in", "into", "is", "it", "just", "like", "me", "more", "my", "no",
    "not", "of", "on", "one", "or", "our", "out", "please", "radio", "report", "net", "msg",
    "schedule", "status", "station", "system", "that", "the", "their", "them", "then", "there",
    "this", "time", "to", "up", "us", "we", "were", "what", "when", "which", "who", "will",
    "with", "you", "your", "thanks", "thank", "hello", "hi", "copy", "de", "73"
}


@dataclass
class FldigiLogStatus:
    busy: bool
    reason: Optional[str]
    last_valid_age_s: Optional[float]


class FldigiLogStatusClient:
    """
    Determine FLDIGI activity by parsing fldigi*.log.
    This is intentionally log-only (no XML-RPC).
    """

    def __init__(
        self,
        *,
        hold_seconds: int = 90,
        flmsg_hold_seconds: int = 120,
        flamp_hold_seconds: int = 120,
        gibberish_grace_seconds: int = 90,
        gibberish_max_seconds: int = 120,
        gibberish_threshold: float = 0.55,
        min_words: int = 3,
        stale_mtime_seconds: int = 90,
    ) -> None:
        self.settings = SettingsManager()
        self.hold_seconds = int(hold_seconds)
        self.flmsg_hold_seconds = int(flmsg_hold_seconds)
        self.flamp_hold_seconds = int(flamp_hold_seconds)
        self.gibberish_grace_seconds = int(gibberish_grace_seconds)
        self.gibberish_max_seconds = int(gibberish_max_seconds)
        self.gibberish_threshold = float(gibberish_threshold)
        self.min_words = int(min_words)
        self.stale_mtime_seconds = int(stale_mtime_seconds)
        self._last_path: Optional[Path] = None
        self._last_offset: int = 0
        self._last_valid_ts: Optional[datetime.datetime] = None
        self._last_valid_reason: Optional[str] = None
        self._last_gibberish_ts: Optional[datetime.datetime] = None
        self._gibberish_run_start: Optional[datetime.datetime] = None

    def _resolve_log_path(self) -> Optional[Path]:
        raw = (self.settings.get("fldigi_log_path", "") or "").strip()
        if not raw:
            return None
        path = Path(raw)
        if path.is_dir():
            return self._latest_log_in_dir(path)
        if path.is_file():
            latest = self._latest_log_in_dir(path.parent)
            if latest and latest.stat().st_mtime >= path.stat().st_mtime:
                return latest
            return path
        # If path points to a non-existent file, try its parent dir
        if path.parent.exists() and path.parent.is_dir():
            return self._latest_log_in_dir(path.parent)
        return None

    def _latest_log_in_dir(self, base: Path) -> Optional[Path]:
        try:
            candidates = list(base.glob("fldigi*.log"))
            if not candidates:
                return None
            candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return candidates[0]
        except Exception:
            return None

    def _token_stats(self, token: str) -> Tuple[bool, float]:
        t = token.lower()
        if len(t) < 2:
            return False, 0.0
        if t in _COMMON_WORDS:
            return True, 1.0
        if not t.isalpha():
            return False, 0.0
        vowels = sum(1 for c in t if c in "aeiouy")
        if vowels == 0:
            return False, 0.0
        vowel_ratio = vowels / len(t)
        if vowel_ratio < 0.2 or vowel_ratio > 0.75:
            return False, 0.1
        run = 0
        max_run = 0
        for c in t:
            if c in "aeiouy":
                run = 0
            else:
                run += 1
                max_run = max(max_run, run)
        if max_run >= 5:
            return False, 0.1
        return True, 0.6

    def _gibberish_score(self, text: str) -> Tuple[float, int]:
        tokens = _TOKEN_RE.findall(text)
        if not tokens:
            return 1.0, 0
        valid = 0
        score = 0.0
        for tok in tokens:
            ok, s = self._token_stats(tok)
            if ok:
                valid += 1
            score += (1.0 - s)
        avg_noise = score / max(1, len(tokens))
        valid_ratio = valid / max(1, len(tokens))
        printable = len(_PRINTABLE_RE.findall(text))
        non_printable_ratio = 1.0 - (printable / max(1, len(text)))
        gib = 0.6 * avg_noise + 0.3 * (1.0 - valid_ratio) + 0.1 * non_printable_ratio
        return min(max(gib, 0.0), 1.0), len(tokens)

    def _classify_payload(self, payload: str) -> Tuple[bool, Optional[str]]:
        raw = payload.strip()
        if not raw:
            return False, None
        upper = raw.upper()
        for marker in _FLMSG_MARKERS:
            if marker.lower() in raw.lower():
                return True, "flmsg"
        if _CALLSIGN_RE.search(raw):
            return True, "callsign"
        if "MODE CHANGE TO" in upper:
            return True, "mode_change"
        for prefix in _FLAMP_PREFIXES:
            if raw.lstrip().startswith(prefix):
                return True, "flamp"
        gib_score, token_count = self._gibberish_score(raw)
        if token_count >= self.min_words and gib_score < self.gibberish_threshold:
            return True, "text"
        return False, None

    def _is_gibberish(self, payload: str) -> bool:
        raw = payload.strip()
        if not raw:
            return False
        gib_score, token_count = self._gibberish_score(raw)
        if token_count == 0:
            return False
        return gib_score >= self.gibberish_threshold

    def _update_from_log(self, path: Path) -> None:
        try:
            stat = path.stat()
        except Exception:
            return
        if self._last_path != path:
            self._last_path = path
            self._last_offset = 0
        if stat.st_size < self._last_offset:
            # log rotation/truncate
            self._last_offset = 0
        try:
            with path.open("rb") as fh:
                fh.seek(self._last_offset)
                data = fh.read()
                self._last_offset = fh.tell()
        except Exception:
            return
        if not data:
            return
        text = data.decode("utf-8", errors="replace")
        for line in text.splitlines():
            m = _RX_LINE_RE.match(line.strip())
            if not m:
                continue
            ts_str, payload = m.group(1), m.group(2)
            try:
                ts_val = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
                ts_val = ts_val.replace(tzinfo=datetime.timezone.utc)
            except Exception:
                continue
            valid, reason = self._classify_payload(payload)
            if valid:
                self._last_valid_ts = ts_val
                self._last_valid_reason = reason
                self._last_gibberish_ts = None
                self._gibberish_run_start = None
            elif self._is_gibberish(payload):
                self._last_gibberish_ts = ts_val
                if self._gibberish_run_start is None:
                    self._gibberish_run_start = ts_val

    def get_status(self) -> FldigiLogStatus:
        path = self._resolve_log_path()
        if not path or not path.exists():
            return FldigiLogStatus(busy=False, reason=None, last_valid_age_s=None)
        try:
            mtime = datetime.datetime.fromtimestamp(path.stat().st_mtime, tz=datetime.timezone.utc)
        except Exception:
            mtime = None
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if mtime and (now_utc - mtime).total_seconds() > self.stale_mtime_seconds:
            return FldigiLogStatus(busy=False, reason=None, last_valid_age_s=None)
        self._update_from_log(path)
        if not self._last_valid_ts:
            return FldigiLogStatus(busy=False, reason=None, last_valid_age_s=None)
        age = (now_utc - self._last_valid_ts).total_seconds()
        hold = self.hold_seconds
        if self._last_valid_reason == "flmsg":
            hold = self.flmsg_hold_seconds
        elif self._last_valid_reason == "flamp":
            hold = self.flamp_hold_seconds
        gibberish_run_age = None
        if self._gibberish_run_start:
            gibberish_run_age = (now_utc - self._gibberish_run_start).total_seconds()
        gibberish_stale = (
            gibberish_run_age is not None
            and gibberish_run_age > self.gibberish_max_seconds
        )
        busy = age <= hold and not gibberish_stale
        reason = self._last_valid_reason
        if busy and self._last_gibberish_ts:
            gib_age = (now_utc - self._last_gibberish_ts).total_seconds()
            if gib_age <= self.gibberish_grace_seconds:
                reason = "gibberish"
        return FldigiLogStatus(
            busy=bool(busy),
            reason=reason,
            last_valid_age_s=age,
        )

    def is_busy(self) -> bool:
        return bool(self.get_status().busy)
