from __future__ import annotations

"""Pure, source-neutral presentation semantics for projected message rows.

The helpers in this module operate only on values already present in an Inbox
row/payload.  They deliberately have no Qt, filesystem, SQLite, or network
dependencies so table rendering, column fitting, and reader action state remain
cache-only.
"""

from dataclasses import dataclass
import re

from freqinout.core.message_summary import message_source_label, normalize_message_source_family


_RRSR_RE = re.compile(
    r"(?:^|[\s:])RRSR\s+([A-Z0-9/]{3,12}),([A-Z0-9]{1,8})\.?\s*(?:$|[\u2662])",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class CommStatStatusReceipt:
    report_callsign: str
    report_id: str


def commstat_status_receipt(*values: object) -> CommStatStatusReceipt | None:
    """Return the acknowledged report identity for one cached RRSR value."""

    for value in values:
        text = " ".join(str(value or "").split())
        if not text:
            continue
        match = _RRSR_RE.search(text)
        if match is not None:
            return CommStatStatusReceipt(
                report_callsign=match.group(1).strip().upper(),
                report_id=match.group(2).strip().upper(),
            )
    return None


def message_row_source_label(row: object) -> str:
    """Return a concise operator-facing source aligned with Inbox Focus."""

    payload = getattr(row, "payload", None)
    display_type = str(
        getattr(payload, "display_type", "") or getattr(row, "display_type", "") or ""
    ).strip().lower()
    if display_type == "commstat":
        return "CommStat"
    summary = getattr(row, "summary", None)
    label = str(getattr(summary, "source_label", "") or "").strip()
    if label:
        return label
    family = (
        getattr(summary, "source_family", "")
        or getattr(row, "origin", "")
        or getattr(payload, "source_family", "")
    )
    return message_source_label(normalize_message_source_family(family))


def message_row_kind_label(row: object) -> str:
    """Return semantic content kind without conflating it with source."""

    payload = getattr(row, "payload", None)
    receipt = commstat_status_receipt(
        getattr(payload, "body_preview", ""),
        getattr(payload, "summary", ""),
        getattr(payload, "decoded_text", ""),
        getattr(payload, "raw_text", ""),
        getattr(row, "title", ""),
    )
    if receipt is not None:
        return "Status receipt"

    family = normalize_message_source_family(
        getattr(payload, "source_family", "") or getattr(row, "origin", "")
    )
    display_type = str(
        getattr(payload, "display_type", "") or getattr(row, "display_type", "") or ""
    ).strip()
    message_type = str(
        getattr(payload, "message_type", "") or getattr(row, "msg_type", "") or ""
    ).strip()
    artifact_kind = str(getattr(payload, "artifact_kind", "") or "").strip()

    if family == "spotter" or message_type.upper().startswith("F!"):
        return f"MCF {message_type}" if message_type.upper().startswith("F!") else (display_type or "MCF form")
    if family == "commstat" or display_type.lower() == "commstat":
        value = artifact_kind or message_type
        value = re.sub(r"^CommStat[/\s·:-]*", "", value, flags=re.IGNORECASE).strip()
        return value.replace("_", " ").title() if value else "CommStat report"
    if family == "js8":
        if message_type.upper() in {"MSG", "JS8 MSG", "JS8"}:
            return "JS8 message"
        return message_type or "Directed traffic"
    if family == "varac":
        value = str(getattr(payload, "msg_type", "") or display_type or "").strip()
        return value or "VarAC message"
    if family == "bbs":
        return display_type or message_type or "BBS item"
    if family in {"flmsg", "flamp"}:
        return display_type or message_type or f"{message_source_label(family)} form"
    if family in {"meshcore", "meshtastic", "mesh"}:
        return display_type or message_type or "Mesh message"
    return display_type or message_type or "Message"


def message_row_narrative_label(row: object) -> str:
    """Combine semantic kind and summary once for the default scan surface."""

    kind = message_row_kind_label(row)
    summary = str(getattr(row, "title", "") or "").strip()
    if not summary:
        payload = getattr(row, "payload", None)
        summary = str(
            getattr(payload, "summary", "")
            or getattr(payload, "subject", "")
            or getattr(payload, "body_preview", "")
            or ""
        ).strip()
    if not summary:
        return kind
    if summary.casefold().startswith(kind.casefold()):
        return summary
    return f"{kind} · {summary}"


def status_receipt_summary(*, sender: object, receipt: CommStatStatusReceipt) -> str:
    actor = str(sender or "").strip().upper()
    prefix = f"{actor} acknowledged" if actor else "Acknowledged"
    return f"{prefix} {receipt.report_callsign} status report {receipt.report_id}"
