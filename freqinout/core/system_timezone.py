from __future__ import annotations

import datetime
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Optional
from zoneinfo import ZoneInfo

SUPPORTED_TIMEZONE_CHOICES = (
    "UTC",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/Anchorage",
    "Pacific/Honolulu",
    "America/Toronto",
    "America/Vancouver",
    "Europe/London",
    "Europe/Berlin",
    "Europe/Madrid",
    "Europe/Rome",
    "Europe/Athens",
    "Europe/Helsinki",
    "Europe/Moscow",
    "Asia/Tokyo",
    "Asia/Shanghai",
    "Asia/Singapore",
    "Asia/Seoul",
    "Asia/Kolkata",
    "Australia/Sydney",
    "Australia/Melbourne",
    "Pacific/Auckland",
)

WINDOWS_TO_IANA = {
    "UTC": "UTC",
    "Eastern Standard Time": "America/New_York",
    "Central Standard Time": "America/Chicago",
    "Mountain Standard Time": "America/Denver",
    "US Mountain Standard Time": "America/Denver",
    "Pacific Standard Time": "America/Los_Angeles",
}

ALIASES_TO_IANA = {
    "GMT": "UTC",
    "UTC": "UTC",
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "EST5EDT": "America/New_York",
    "CST6CDT": "America/Chicago",
    "MST7MDT": "America/Denver",
    "PST8PDT": "America/Los_Angeles",
    "US/EASTERN": "America/New_York",
    "US/CENTRAL": "America/Chicago",
    "US/MOUNTAIN": "America/Denver",
    "US/PACIFIC": "America/Los_Angeles",
}

_IANA_VALIDATION_CACHE: Dict[str, bool] = {}
_DETECT_CACHE: Dict[str, object] = {
    "value": None,
    "ts": 0.0,
    "env": ("", ""),
    "fallback": "UTC",
}


def _is_valid_iana_timezone(value: str) -> bool:
    txt = str(value or "").strip()
    if not txt:
        return False
    cached = _IANA_VALIDATION_CACHE.get(txt)
    if cached is not None:
        return cached
    try:
        ZoneInfo(txt)
        _IANA_VALIDATION_CACHE[txt] = True
        return True
    except Exception:
        _IANA_VALIDATION_CACHE[txt] = False
        return False


def normalize_supported_timezone_name(value: object) -> Optional[str]:
    txt = str(value or "").strip()
    if not txt:
        return None
    if txt in SUPPORTED_TIMEZONE_CHOICES:
        return txt
    if txt in WINDOWS_TO_IANA:
        return WINDOWS_TO_IANA[txt]

    normalized = txt.replace("\\", "/").strip()
    if normalized in SUPPORTED_TIMEZONE_CHOICES:
        return normalized

    upper = normalized.upper()
    if upper in ALIASES_TO_IANA:
        return ALIASES_TO_IANA[upper]
    if _is_valid_iana_timezone(normalized):
        return normalized
    return None


def _read_text_if_present(path: str) -> Optional[str]:
    try:
        txt = Path(path).read_text(encoding="utf-8").strip()
    except Exception:
        return None
    return txt or None


def _timezone_from_localtime_symlink() -> Optional[str]:
    try:
        resolved = str(Path("/etc/localtime").resolve())
    except Exception:
        return None
    marker = "zoneinfo/"
    if marker not in resolved:
        return None
    candidate = resolved.split(marker, 1)[1].strip("/\\")
    return candidate.replace("\\", "/") or None


def _command_output(args: list[str]) -> Optional[str]:
    run_kwargs = {}
    if os.name == "nt":
        try:
            creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0) or 0)
            if creationflags:
                run_kwargs["creationflags"] = creationflags
        except Exception:
            pass
        try:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
            startupinfo.wShowWindow = 0
            run_kwargs["startupinfo"] = startupinfo
        except Exception:
            pass
    try:
        completed = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=1.5,
            **run_kwargs,
        )
    except Exception:
        return None
    txt = (completed.stdout or completed.stderr or "").strip()
    return txt or None


def _platform_timezone_candidates() -> Iterable[str]:
    if sys.platform.startswith("darwin"):
        txt = _command_output(["/usr/sbin/systemsetup", "-gettimezone"])
        if txt and ":" in txt:
            yield txt.split(":", 1)[1].strip()
    elif sys.platform.startswith("win"):
        txt = _command_output(["tzutil", "/g"])
        if txt:
            yield txt

    for path in ("/etc/timezone",):
        txt = _read_text_if_present(path)
        if txt:
            yield txt

    symlink_tz = _timezone_from_localtime_symlink()
    if symlink_tz:
        yield symlink_tz

    for candidate in time.tzname:
        if candidate:
            yield candidate


def detect_system_timezone_name(fallback: str = "UTC") -> str:
    normalized_fallback = normalize_supported_timezone_name(fallback) or "UTC"
    env_sig = (
        str(os.environ.get("FREQINOUT_TZ", "") or ""),
        str(os.environ.get("TZ", "") or ""),
    )
    now = time.monotonic()
    cached_value = _DETECT_CACHE.get("value")
    if (
        cached_value
        and _DETECT_CACHE.get("env") == env_sig
        and _DETECT_CACHE.get("fallback") == normalized_fallback
        and (now - float(_DETECT_CACHE.get("ts") or 0.0)) < 300.0
    ):
        return str(cached_value)

    for candidate in (
        env_sig[0],
        env_sig[1],
    ):
        normalized = normalize_supported_timezone_name(candidate)
        if normalized:
            _DETECT_CACHE.update({"value": normalized, "ts": now, "env": env_sig, "fallback": normalized_fallback})
            return normalized

    try:
        now_local = datetime.datetime.now().astimezone()
        tzinfo = now_local.tzinfo
        for candidate in (
            getattr(tzinfo, "key", None),
            getattr(tzinfo, "zone", None),
            now_local.tzname(),
        ):
            normalized = normalize_supported_timezone_name(candidate)
            if normalized:
                _DETECT_CACHE.update({"value": normalized, "ts": now, "env": env_sig, "fallback": normalized_fallback})
                return normalized
    except Exception:
        pass

    for candidate in _platform_timezone_candidates():
        normalized = normalize_supported_timezone_name(candidate)
        if normalized:
            _DETECT_CACHE.update({"value": normalized, "ts": now, "env": env_sig, "fallback": normalized_fallback})
            return normalized

    _DETECT_CACHE.update({"value": normalized_fallback, "ts": now, "env": env_sig, "fallback": normalized_fallback})
    return normalized_fallback
