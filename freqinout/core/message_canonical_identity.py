"""Source-independent identities for the station message library.

External receipts keep their source-specific identities in
``message_external_refs``.  This module supplies the separate identity of the
station-owned message those receipts describe.  The helpers are deliberately
conservative: only matching protocol identities or matching event envelopes
within one message family collapse to one presentation.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from freqinout.core.message_projection_store import stable_message_id


CANONICAL_MESSAGE_IDENTITY_VERSION = 1
_SPACE_RE = re.compile(r"\s+")


def normalized_message_text(value: object) -> str:
    """Return a stable comparison spelling without changing message meaning."""

    return _SPACE_RE.sub(" ", str(value or "").strip()).casefold()


def canonical_station_message_id(
    family: object,
    *,
    event_ts: object = 0.0,
    from_call: object = "",
    to_call: object = "",
    payload: object = "",
    message_type: object = "",
    durable_id: object = "",
    payload_digest: object = "",
) -> str:
    """Identify one message independently of the receipt source.

    ``family`` remains part of the identity because a protocol artifact and a
    higher-level fused operational report can legitimately describe the same
    traffic while having different lifecycle and action semantics.  Within a
    family, radio/profile/path/API identities are intentionally excluded.
    """

    family_key = normalized_message_text(family) or "message"
    durable_key = normalized_message_text(durable_id)
    payload_key = normalized_message_text(payload_digest)
    if not payload_key:
        payload_key = hashlib.sha256(
            normalized_message_text(payload).encode("utf-8", "replace")
        ).hexdigest()
    type_key = normalized_message_text(message_type) if not normalized_message_text(payload) else ""
    if durable_key:
        return stable_message_id(
            "station-message",
            CANONICAL_MESSAGE_IDENTITY_VERSION,
            family_key,
            "durable",
            durable_key,
            payload_key,
        )
    try:
        # JS8Call and several native file/database formats expose only whole
        # seconds.  Normalizing here lets a live API receipt and its on-disk
        # DIRECTED.TXT receipt refer to the same RF event.
        second = int(float(event_ts or 0.0))
    except Exception:
        second = 0
    return stable_message_id(
        "station-message",
        CANONICAL_MESSAGE_IDENTITY_VERSION,
        family_key,
        "event",
        second,
        normalized_message_text(from_call).upper(),
        normalized_message_text(to_call).upper(),
        type_key,
        payload_key,
    )


def canonical_message_key(family: object, message_id: object) -> str:
    return f"station:{normalized_message_text(family) or 'message'}:{str(message_id or '').strip()}"


def file_content_digest(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Hash a completed native message file without retaining its contents."""

    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(max(4096, int(chunk_size or 0)))
                if not chunk:
                    break
                digest.update(chunk)
    except (OSError, ValueError):
        return ""
    return digest.hexdigest()
