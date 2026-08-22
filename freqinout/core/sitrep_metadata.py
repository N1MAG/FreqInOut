from __future__ import annotations

from typing import Dict, Iterable, List


_STATUS_ORDER = {
    "red": 5,
    "yellow": 4,
    "green": 3,
    "unknown": 2,
    "not_reported": 1,
}

_SOURCE_FAMILY_LABELS = {
    "COMMSTAT": "CommStat",
    "JS8SPOTTER": "JS8Spotter",
    "CONDITION_ALERT": "Condition Alert",
    "MANUAL": "Manual",
    "FUSED": "Fused",
    "UNKNOWN": "Unknown",
}

_SOURCE_SHORT_LABELS = {
    "COMMSTAT": "CS",
    "JS8SPOTTER": "SPT",
    "CONDITION_ALERT": "ALRT",
    "MANUAL": "MAN",
    "FUSED": "FUS",
    "UNKNOWN": "UNK",
}

_SUBTYPE_LABELS = {
    "COMMSTAT_12": "CommStat",
    "COMMSTAT_FWD": "CommStat",
    "SPOTTER_301": "F!301",
    "SPOTTER_304": "F!304",
    "SPOTTER_104": "F!104",
}


def normalize_transport_mode(value: object) -> str:
    txt = str(value or "").strip().lower()
    if not txt:
        return "unknown"
    if txt in {"rf", "js8"}:
        return "js8"
    if txt in {"internet"}:
        return "internet"
    if txt in {"rf+internet", "internet+rf", "js8+internet", "internet+js8"}:
        return "js8+internet"
    return "unknown"


def merge_transport_modes(*values: object) -> str:
    modes = {normalize_transport_mode(v) for v in values if normalize_transport_mode(v) != "unknown"}
    if not modes:
        return "unknown"
    if "js8+internet" in modes:
        return "js8+internet"
    if {"js8", "internet"}.issubset(modes):
        return "js8+internet"
    if "js8" in modes:
        return "js8"
    if "internet" in modes:
        return "internet"
    return "unknown"


def transport_label(value: object) -> str:
    mode = normalize_transport_mode(value)
    if mode == "js8":
        return "JS8"
    if mode == "internet":
        return "Internet"
    if mode == "js8+internet":
        return "JS8 + Internet"
    return "Unknown"


def source_family_key(source: object) -> str:
    src = str(source or "").strip().upper()
    if not src:
        return "UNKNOWN"
    if src in {"COMMSTAT3", "COMMSTAT23", "COMMSTAT", "COMMSTAT_12", "COMMSTAT_FWD"}:
        return "COMMSTAT"
    if src in {"JS8SPOTTER", "JS8SPOTTER_IMPORT", "SPOTTER"}:
        return "JS8SPOTTER"
    if src in {"CONDITION_ALERT", "CONDITIONALERT", "ALERT"}:
        return "CONDITION_ALERT"
    if src == "MANUAL":
        return "MANUAL"
    if src == "FUSED":
        return "FUSED"
    return src


def source_family_label(source: object) -> str:
    family = source_family_key(source)
    return _SOURCE_FAMILY_LABELS.get(family, family.title() if family else "Unknown")


def source_short_label(source: object) -> str:
    family = source_family_key(source)
    if family in _SOURCE_SHORT_LABELS:
        return _SOURCE_SHORT_LABELS[family]
    if len(family) <= 4:
        return family
    return family[:4]


def source_families_from_sources(sources: Iterable[object]) -> List[str]:
    families = {source_family_key(src) for src in sources if str(src or "").strip()}
    ordered = []
    for key in ("COMMSTAT", "JS8SPOTTER", "MANUAL", "FUSED", "UNKNOWN"):
        if key in families:
            ordered.append(key)
            families.discard(key)
    ordered.extend(sorted(families))
    return ordered


def source_family_display_label(sources: Iterable[object]) -> str:
    families = source_families_from_sources(sources)
    if not families:
        return ""
    if len(families) > 1:
        return "Mixed"
    return source_family_label(families[0])


def source_summary_by_family(summary: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw_source, raw_status in (summary or {}).items():
        family = source_family_key(raw_source)
        status = _normalize_status(raw_status)
        existing = out.get(family, "not_reported")
        if _STATUS_ORDER.get(status, 0) > _STATUS_ORDER.get(existing, 0):
            out[family] = status
    return out


def subtype_label(subtype: object) -> str:
    key = str(subtype or "").strip().upper()
    return _SUBTYPE_LABELS.get(key, key or "SitRep")


def subtype_filter_label(subtype: object) -> str:
    return f"SitRep/{subtype_label(subtype)}"


def parse_filter_subtype_label(value: object) -> str:
    label = str(value or "").strip()
    if not label:
        return ""
    if label.upper().startswith("SITREP/"):
        label = label.split("/", 1)[1].strip()
    normalized = label.lower()
    for key, pretty in _SUBTYPE_LABELS.items():
        if normalized == pretty.lower():
            return key
    return label.strip().upper()


def _normalize_status(value: object) -> str:
    txt = str(value or "").strip().lower()
    if txt in _STATUS_ORDER:
        return txt
    return "unknown"
