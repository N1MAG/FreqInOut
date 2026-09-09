"""Contract tests for the Qt-free JS8 inbox projection policy.

These tests deliberately exercise the payload policy without a database or a
running radio.  RF observations may still be retained by the link/map
pipeline; this policy only decides whether a payload belongs in Messages and
provides the display-safe canonical text.
"""

import pytest

from freqinout.core.js8_message_policy import (
    canonicalize_js8_payload,
    classify_js8_payload,
    directed_js8_payload,
)


def _classification(text: str):
    result = classify_js8_payload(text)
    assert hasattr(result, "inbox_visible")
    assert hasattr(result, "reason")
    assert hasattr(result, "canonical_text")
    return result


@pytest.mark.parametrize(
    "payload",
    [
        "",
        "   ",
        "HB",
        "HEARTBEAT",
        "HB SNR -12",
        "HEARTBEAT SNR -12 dB",
        "SNR?",
        "SNR? SNR?",
        "SNR -12",
        "SNR -12 dB",
        "ACK",
        "ACK ACK",
        "NACK",
        "QUERY MSGS",
        "QUERY MSGS QUERY MSGS",
        "QUERY MSG 1234",
        "QUERY CALL K7ETC",
        "QUERY CALLSIGN K7ETC",
        "GRID?",
        "GRID EM75RJ",
        "INFO?",
        "STATUS?",
        "HEARING?",
        "E? Q 970F",
        "E? Q 970F E? Q 970F",
    ],
)
def test_protocol_and_control_payloads_are_not_inbox_messages(payload: str) -> None:
    result = _classification(payload)

    assert result.inbox_visible is False
    assert result.reason


@pytest.mark.parametrize(
    "payload",
    [
        "Can you query the team about the ACK from yesterday?",
        "Please acknowledge receipt of the report.",
        "GOOD AFTERNOON N1MAG",
        "The query is answered in the attached report.",
        "ACK is the name of the new exercise.",
        "Status update: all operations normal; no noteworthy activity.",
        "GRID EM75RJ is where the event is staged.",
    ],
)
def test_natural_language_is_retained_when_control_words_are_not_the_payload(payload: str) -> None:
    result = _classification(payload)

    assert result.inbox_visible is True
    assert result.canonical_text == payload


def test_exact_duplicate_payload_is_canonicalized_once() -> None:
    payload = "GOOD AFTERNOON N1MAG GOOD AFTERNOON N1MAG"

    assert canonicalize_js8_payload(payload) == "GOOD AFTERNOON N1MAG"
    result = _classification(payload)
    assert result.inbox_visible is True
    assert result.canonical_text == "GOOD AFTERNOON N1MAG"


@pytest.mark.parametrize(
    "payload",
    [
        "SNR? SNR?",
        "QUERY MSGS QUERY MSGS",
        "E? Q 970F E? Q 970F",
    ],
)
def test_exact_duplicate_control_payload_is_canonicalized_before_classification(payload: str) -> None:
    result = _classification(payload)

    assert result.inbox_visible is False
    assert result.canonical_text != payload


def test_repeated_single_word_is_not_treated_as_a_duplicated_message() -> None:
    payload = "HELLO HELLO"

    assert canonicalize_js8_payload(payload) == payload
    result = _classification(payload)
    assert result.inbox_visible is True
    assert result.canonical_text == payload


def test_whitespace_is_normalized_without_changing_message_words() -> None:
    payload = "  GOOD   AFTERNOON   N1MAG  "

    assert canonicalize_js8_payload(payload) == "GOOD AFTERNOON N1MAG"
    result = _classification(payload)
    assert result.inbox_visible is True
    assert result.canonical_text == "GOOD AFTERNOON N1MAG"


def test_directed_envelope_exposes_only_the_payload() -> None:
    assert directed_js8_payload("K1AAA: @MAGNET NEED WATER ♢") == "NEED WATER"


def test_third_party_relay_frame_is_not_an_inbox_payload() -> None:
    payload = directed_js8_payload("W7MOE: N1MAG> K7RIE ACK ♢")

    assert classify_js8_payload(payload).inbox_visible is False
