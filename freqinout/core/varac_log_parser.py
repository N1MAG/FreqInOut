from __future__ import annotations

import datetime as dt
import locale
import os
import threading
import time
from typing import Optional


_DAY_FIRST_CACHE_LOCK = threading.Lock()
_DAY_FIRST_CACHE_VALUE: Optional[bool] = None
_DAY_FIRST_CACHE_TS: float = 0.0
_DAY_FIRST_CACHE_TTL_S = 300.0


def _local_timezone() -> dt.tzinfo:
    try:
        return dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
    except Exception:
        return dt.timezone.utc


def _env_day_first_override() -> Optional[bool]:
    raw = str(os.environ.get("FREQINOUT_VARAC_DAY_FIRST", "") or "").strip().lower()
    if raw in {"1", "true", "yes", "on", "day-first", "dmy", "dd/mm"}:
        return True
    if raw in {"0", "false", "no", "off", "month-first", "mdy", "mm/dd"}:
        return False
    return None


def _locale_prefers_day_first() -> bool:
    env_override = _env_day_first_override()
    if env_override is not None:
        return env_override
    now = time.monotonic()
    with _DAY_FIRST_CACHE_LOCK:
        global _DAY_FIRST_CACHE_VALUE, _DAY_FIRST_CACHE_TS
        if (
            _DAY_FIRST_CACHE_VALUE is not None
            and (now - float(_DAY_FIRST_CACHE_TS or 0.0)) < _DAY_FIRST_CACHE_TTL_S
        ):
            return bool(_DAY_FIRST_CACHE_VALUE)

        preferred = False
        try:
            d_fmt = locale.nl_langinfo(locale.D_FMT)
        except Exception:
            d_fmt = ""
        if d_fmt:
            day_idx = d_fmt.find("%d")
            month_idx = d_fmt.find("%m")
            if day_idx >= 0 and month_idx >= 0:
                preferred = day_idx < month_idx
                _DAY_FIRST_CACHE_VALUE = preferred
                _DAY_FIRST_CACHE_TS = now
                return preferred

        locale_candidates = []
        try:
            locale_candidates.extend([part for part in locale.getlocale() if part])
        except Exception:
            pass
        try:
            default_locale = locale.getdefaultlocale()
            locale_candidates.extend([part for part in default_locale if part])
        except Exception:
            pass

        for candidate in locale_candidates:
            text = str(candidate or "").strip().lower()
            if not text:
                continue
            if text.startswith("en_us"):
                preferred = False
                break
            if any(
                marker in text
                for marker in (
                    "_gb",
                    "_au",
                    "_nz",
                    "_ie",
                    "_in",
                    "_za",
                    "_fr",
                    "_de",
                    "_es",
                    "_it",
                    "_nl",
                    "_se",
                    "_no",
                    "_dk",
                )
            ):
                preferred = True
                break

        _DAY_FIRST_CACHE_VALUE = preferred
        _DAY_FIRST_CACHE_TS = now
        return preferred


def parse_varac_event_timestamp(
    stamp: str,
    *,
    prefer_day_first: Optional[bool] = None,
    tzinfo: Optional[dt.tzinfo] = None,
) -> Optional[dt.datetime]:
    raw = str(stamp or "").strip()
    if len(raw) < 19:
        return None
    raw = raw[:19]
    try:
        left = int(raw[0:2])
        right = int(raw[3:5])
        year = int(raw[6:10])
        hour = int(raw[11:13])
        minute = int(raw[14:16])
        second = int(raw[17:19])
    except Exception:
        return None

    if left > 12 and right <= 12:
        day_first = True
    elif right > 12 and left <= 12:
        day_first = False
    elif prefer_day_first is not None:
        day_first = bool(prefer_day_first)
    else:
        day_first = _locale_prefers_day_first()

    day = left if day_first else right
    month = right if day_first else left
    zone = tzinfo or _local_timezone()
    try:
        return dt.datetime(year, month, day, hour, minute, second, tzinfo=zone)
    except Exception:
        return None


def parse_varac_event_timestamp_to_epoch(
    stamp: str,
    *,
    prefer_day_first: Optional[bool] = None,
    tzinfo: Optional[dt.tzinfo] = None,
) -> float:
    parsed = parse_varac_event_timestamp(
        stamp,
        prefer_day_first=prefer_day_first,
        tzinfo=tzinfo,
    )
    if parsed is None:
        return 0.0
    try:
        return float(parsed.timestamp())
    except Exception:
        return 0.0
