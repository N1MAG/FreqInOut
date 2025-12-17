# freqinout/utils/__init__.py
from __future__ import annotations

import datetime
from typing import Optional

from freqinout.core.settings_manager import SettingsManager
from freqinout.utils.timezones import get_timezone as _core_get_timezone


def get_timezone(tz_name: Optional[str] = None) -> datetime.tzinfo:
    """
    Return a tzinfo for the given timezone name, using the shared
    implementation in freqinout.utils.timezones.

    If tz_name is None, it is read from Settings ('timezone', default 'UTC').
    """
    if tz_name is None:
        settings = SettingsManager()
        tz_name = settings.get("timezone", "UTC") or "UTC"
    return _core_get_timezone(tz_name)


def get_utc_time() -> str:
    """
    Return a formatted UTC time string used by all tabs.

    Format: 'YYMMDD HH:MM:SS Z'
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    return now_utc.strftime("%y%m%d %H:%M:%S Z")


def get_local_time(tz_name: Optional[str] = None) -> str:
    """
    Return a formatted Local time string using the configured timezone
    (or an explicit tz_name if provided).

    Format: 'YYMMDD HH:MM:SS <TZABBR or name>'
    """
    tz = get_timezone(tz_name)

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_local = now_utc.astimezone(tz)

    abbr = now_local.tzname() or (tz_name or "UTC")
    return now_local.strftime(f"%y%m%d %H:%M:%S {abbr}")
