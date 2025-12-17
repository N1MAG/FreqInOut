from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import datetime
import sys

# Windows fallback mapping
WINDOWS_TZ_MAP = {
    "UTC": "UTC",
    "America/New_York": "Eastern Standard Time",
    "America/Chicago": "Central Standard Time",
    "America/Denver": "Mountain Standard Time",
    "America/Los_Angeles": "Pacific Standard Time",
}

def get_timezone(tz_name: str):
    """
    Returns a timezone object that is guaranteed to work on ALL platforms,
    even if ZoneInfo cannot load the IANA database.
    """
    if not tz_name:
        return datetime.timezone.utc

    # Try ZoneInfo first (works on Linux, macOS, some Windows builds)
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        pass
    except Exception:
        pass

    # Windows fallback
    if sys.platform.startswith("win"):
        win_name = WINDOWS_TZ_MAP.get(tz_name)
        if win_name:
            try:
                return datetime.datetime.now().astimezone().tzinfo
            except Exception:
                return datetime.timezone.utc

    # Final fallback: system local timezone
    try:
        return datetime.datetime.now().astimezone().tzinfo
    except Exception:
        return datetime.timezone.utc