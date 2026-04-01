
"""FreqInOut package."""

from freqinout.version import __version__  # central version string
from freqinout.utils import (
    get_local_time as _shared_get_local_time,
    get_timezone as _shared_get_timezone,
    get_utc_time as _shared_get_utc_time,
)


def get_timezone():
    return _shared_get_timezone()


def get_utc_time():
    return _shared_get_utc_time()


def get_local_time():
    return _shared_get_local_time()
