from __future__ import annotations


def normalize_group_name(value: object) -> str:
    """Return FIO's canonical group identity for storage, filters, and display."""
    try:
        text = str(value or "").strip().upper()
    except Exception:
        return ""
    while text.startswith("@"):
        text = text[1:].strip()
    return text.strip()
