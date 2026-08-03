from __future__ import annotations

import random
from typing import Callable, Iterable, Optional


JS8_OFFSET_MIN_HZ = 1800
JS8_OFFSET_MAX_HZ = 2700
JS8_OFFSET_STEP_HZ = 25


def js8_default_offset_choices() -> range:
    return range(JS8_OFFSET_MIN_HZ, JS8_OFFSET_MAX_HZ + 1, JS8_OFFSET_STEP_HZ)


def random_default_js8_offset_hz(
    *,
    chooser: Optional[Callable[[Iterable[int]], int]] = None,
) -> int:
    pick = chooser or random.choice
    return int(pick(js8_default_offset_choices()))


def coerce_js8_offset_hz(value: object, *, default_when_blank: bool = True) -> int:
    try:
        offset = int(str(value if value is not None else "").strip())
    except Exception:
        offset = 0
    if offset > 0:
        return offset
    return random_default_js8_offset_hz() if default_when_blank else 0
