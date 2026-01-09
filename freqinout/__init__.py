
"""FreqInOut package."""

from freqinout.version import __version__  # central version string
import datetime
import os

import pytz

from freqinout.core.settings_manager import SettingsManager


def get_timezone():
    """
    Return the user's configured timezone.

    Priority:
      1. Timezone saved in FreqInOut settings ("timezone" key)
      2. Environment variable FREQINOUT_TZ (optional override)
      3. UTC as a safe default
    """
    tz_name = "UTC"

    # 1) Read from FreqInOut settings (what user picked in Settings tab)
    try:
        sm = SettingsManager()
        saved_tz = sm.get("timezone")
        if saved_tz:
            tz_name = saved_tz
    except Exception:
        # If anything goes wrong, continue with default
        pass

    # 2) Optional environment override
    env_tz = os.environ.get("FREQINOUT_TZ")
    if env_tz:
        tz_name = env_tz

    # 3) Resolve to a pytz timezone
    try:
        return pytz.timezone(tz_name)
    except Exception:
        # If invalid, fall back to UTC
        return pytz.UTC


def get_utc_time():
    """
    Return UTC time as 'YYMMDD HH:MM:SS Z'
    """
    now = datetime.datetime.utcnow()
    return now.strftime("%y%m%d %H:%M:%S Z")


def get_local_time():
    """
    Return local time using the configured timezone as:
      'YYMMDD HH:MM:SS <TZ_ABBR>'
    """
    tz = get_timezone()
    now = datetime.datetime.now(tz)
    abbr = now.tzname() or "LT"
    return now.strftime(f"%y%m%d %H:%M:%S {abbr}")
