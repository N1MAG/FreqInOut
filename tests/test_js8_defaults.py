from __future__ import annotations

from freqinout.core.js8_defaults import (
    JS8_OFFSET_MAX_HZ,
    JS8_OFFSET_MIN_HZ,
    JS8_OFFSET_STEP_HZ,
    coerce_js8_offset_hz,
    js8_default_offset_choices,
    random_default_js8_offset_hz,
)


def test_js8_default_offset_choices_match_operator_requested_range() -> None:
    choices = list(js8_default_offset_choices())

    assert choices[0] == JS8_OFFSET_MIN_HZ == 1800
    assert choices[-1] == JS8_OFFSET_MAX_HZ == 2700
    assert all((value - JS8_OFFSET_MIN_HZ) % JS8_OFFSET_STEP_HZ == 0 for value in choices)


def test_random_js8_default_offset_uses_choice_range() -> None:
    seen = []

    def chooser(values):
        seen.extend(values)
        return 2250

    assert random_default_js8_offset_hz(chooser=chooser) == 2250
    assert seen[0] == 1800
    assert seen[-1] == 2700


def test_coerce_js8_offset_preserves_existing_positive_value_and_defaults_blank() -> None:
    assert coerce_js8_offset_hz("2050") == 2050

    generated = coerce_js8_offset_hz("")

    assert 1800 <= generated <= 2700
    assert (generated - 1800) % 25 == 0
