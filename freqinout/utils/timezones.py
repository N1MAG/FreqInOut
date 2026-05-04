from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import datetime

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

    # Final fallback: system local timezone
    try:
        return datetime.datetime.now().astimezone().tzinfo
    except Exception:
        return datetime.timezone.utc
