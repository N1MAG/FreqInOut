from __future__ import annotations

import datetime
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, Optional

SUPPORTED_TIMEZONE_CHOICES = (
    "UTC",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
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
    try:
        completed = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=1.5,
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

    for candidate in (
        os.environ.get("FREQINOUT_TZ"),
        os.environ.get("TZ"),
    ):
        normalized = normalize_supported_timezone_name(candidate)
        if normalized:
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
                return normalized
    except Exception:
        pass

    for candidate in _platform_timezone_candidates():
        normalized = normalize_supported_timezone_name(candidate)
        if normalized:
            return normalized

    return normalized_fallback
