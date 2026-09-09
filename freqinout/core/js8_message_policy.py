"""Qt-free policy for deciding which JS8 payloads belong in Messages.

The RF/link index intentionally does not use this policy.  Protocol traffic is
valuable evidence for station presence and propagation even when it is not an
operator-facing message.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


JS8_MESSAGE_POLICY_VERSION = 1

_SPACE_RE = re.compile(r"\s+")
_HEARTBEAT_RE = re.compile(
    r"^(?:HB|HEARTBEAT)(?:\s+(?:ACK|SNR(?:\s+[-+]?\d+(?:\.\d+)?(?:\s*DB)?)))?$",
    re.IGNORECASE,
)
_SNR_RE = re.compile(r"^SNR(?:\?|\s+[-+]?\d+(?:\.\d+)?(?:\s*DB)?)$", re.IGNORECASE)
_ACK_RE = re.compile(r"^(?:ACK|NACK)(?:\s+(?:ACK|NACK))*$", re.IGNORECASE)
_QUERY_RE = re.compile(
    r"^QUERY\s+(?:"
    r"MSGS?|"
    r"MSG\s+\S+|"
    r"CALL(?:SIGN)?\s+[A-Z0-9/@._-]+\??|"
    r"(?:GRID|SNR)(?:\s+\S+)?\??|"
    r"INFO\??|STATUS\??|HEARING\??"
    r")$",
    re.IGNORECASE,
)
_SIMPLE_QUERY_RE = re.compile(r"^(?:GRID|INFO|STATUS|HEARING)\?$", re.IGNORECASE)
_GRID_REPORT_RE = re.compile(r"^GRID\s+[A-R]{2}\d{2}(?:[A-X]{2})?$", re.IGNORECASE)
_EXPECT_RE = re.compile(r"^E\?\s+\S+(?:\s+\S+)*$", re.IGNORECASE)
_DIRECTED_ENVELOPE_RE = re.compile(
    r"^[A-Z0-9/._-]+:\s+[@A-Z0-9/._-]+(?:>[A-Z0-9/._-]+)?\s+(?P<payload>.+?)\s*$",
    re.IGNORECASE,
)
_RELAY_FRAME_RE = re.compile(
    r"^[A-Z0-9/._-]+:\s+[@A-Z0-9/._-]+>\s*[A-Z0-9/._-]+(?:\s+.*)?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class JS8MessageDecision:
    inbox_visible: bool
    reason: str
    canonical_text: str
    classification_version: int = JS8_MESSAGE_POLICY_VERSION


def canonicalize_js8_payload(text: object) -> str:
    """Normalize display text and collapse an exact repeated multi-word half.

    JS8Call has been observed returning some payloads twice.  Only an exact
    repeated half containing at least two words is collapsed; ordinary human
    emphasis such as ``HELLO HELLO`` is retained.
    """

    canonical = _SPACE_RE.sub(" ", str(text or "").strip().rstrip("\u2662").rstrip())
    tokens = canonical.split()
    if (
        len(tokens) == 2
        and tokens[0].casefold() == tokens[1].casefold()
        and tokens[0].upper() in {"ACK", "NACK", "SNR?"}
    ):
        return tokens[0]
    if len(tokens) >= 4 and len(tokens) % 2 == 0:
        middle = len(tokens) // 2
        if [token.casefold() for token in tokens[:middle]] == [
            token.casefold() for token in tokens[middle:]
        ]:
            canonical = " ".join(tokens[:middle])
    return canonical


def classify_js8_payload(text: object) -> JS8MessageDecision:
    """Return the Messages projection decision for one payload.

    Patterns are deliberately anchored.  Natural-language messages containing
    words such as ``ack`` or ``query`` must not be mistaken for protocol frames.
    """

    canonical = canonicalize_js8_payload(text)
    if not canonical:
        return JS8MessageDecision(False, "empty", canonical)
    if _EXPECT_RE.fullmatch(canonical):
        return JS8MessageDecision(False, "expect_control", canonical)
    if _HEARTBEAT_RE.fullmatch(canonical):
        return JS8MessageDecision(False, "heartbeat", canonical)
    if _SNR_RE.fullmatch(canonical):
        return JS8MessageDecision(False, "signal_report", canonical)
    if _ACK_RE.fullmatch(canonical):
        return JS8MessageDecision(False, "acknowledgement", canonical)
    if _QUERY_RE.fullmatch(canonical) or _SIMPLE_QUERY_RE.fullmatch(canonical):
        return JS8MessageDecision(False, "protocol_query", canonical)
    if _GRID_REPORT_RE.fullmatch(canonical):
        return JS8MessageDecision(False, "link_telemetry", canonical)
    return JS8MessageDecision(True, "operator_message", canonical)


def directed_js8_payload(text: object) -> str:
    """Return the payload portion when JS8 includes a directed envelope."""

    raw = str(text or "").strip().rstrip("\u2662").rstrip()
    if _RELAY_FRAME_RE.fullmatch(raw):
        return ""
    match = _DIRECTED_ENVELOPE_RE.fullmatch(raw)
    return match.group("payload") if match else raw


def unique_js8_analysis_text(raw_text: object, decoded_text: object) -> str:
    """Build analysis input without repeating semantically identical text."""

    raw = canonicalize_js8_payload(raw_text)
    decoded = canonicalize_js8_payload(decoded_text)
    if not raw:
        return decoded
    if not decoded or raw.casefold() == decoded.casefold():
        return raw
    return f"{raw}\n{decoded}"
