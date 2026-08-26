from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Tuple

from freqinout.core.group_utils import normalize_group_name


COMMSTAT_SCOPE_MAP = {
    "1": "My Location",
    "2": "My Community",
    "3": "My County",
    "4": "My Region",
    "5": "Other Location",
}

VALID_STATE_CODES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
    "AB",
    "BC",
    "MB",
    "NB",
    "NL",
    "NS",
    "NT",
    "NU",
    "ON",
    "PE",
    "QC",
    "SK",
    "YT",
}

STATE_NAME_TO_CODE = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}

_BREVITY_RE = re.compile(r"\b([1-5][A-Z]{5})\b")
_STANDARD_MARKERS = (
    ("{&%3}", "{&%}"),
    ("{F%3}", "{F%}"),
    ("{%%3}", "{%%}"),
    ("{^%3}", "{^%}"),
)

_AMBIGUOUS_STATE_WORDS = {"IN", "OR"}
_NON_LOCATION_PRECEDERS = {
    "BACK",
    "BEEN",
    "CHECK",
    "CONFIRMED",
    "DOWN",
    "FOUND",
    "HEARD",
    "ISSUE",
    "ISSUES",
    "LOST",
    "NEEDED",
    "OPEN",
    "OUT",
    "PENDING",
    "REPORTED",
    "REPORTS",
    "RESTORED",
    "RUNNING",
    "SEEN",
    "SHOWING",
    "SHOWN",
    "STARTED",
    "UPDATED",
}


def normalize_commstat_text(text: object) -> str:
    out = str(text or "").strip()
    for src, dest in _STANDARD_MARKERS:
        out = out.replace(src, dest)
    return out


def _positive_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except Exception:
        return 0


def transport_mode_for_source(source_value: object, raw_message: object = "", *, global_id: object = 0) -> str:
    txt = str(raw_message or "").upper()
    if any(marker in txt for marker in ("{&%3}", "{F%3}", "{%%3}", "{^%3}")):
        return "internet"
    src = _positive_int(source_value)
    if src == 1:
        if _positive_int(global_id) > 0:
            return "js8+internet"
        return "js8"
    if src in {2, 3}:
        return "internet"
    return "unknown"


def commstat_origin_path(source_value: object) -> str:
    src = _positive_int(source_value)
    if src == 1:
        return "rf"
    if src == 2:
        return "commstat_server"
    if src == 3:
        return "internet_only"
    return "unknown"


def commstat_reach_mode(source_value: object, *, global_id: object = 0, raw_message: object = "") -> str:
    src = _positive_int(source_value)
    txt = str(raw_message or "").upper()
    has_server_marker = any(marker in txt for marker in ("{&%3}", "{F%3}", "{%%3}", "{^%3}"))
    if src == 1:
        if _positive_int(global_id) > 0:
            return "maximum_reach"
        return "rf_observed"
    if src == 2:
        return "maximum_reach_relay"
    if src == 3 or has_server_marker:
        return "internet_only"
    return "unknown"


def commstat_reach_label(value: object) -> str:
    mode = str(value or "").strip().lower()
    if mode == "rf_observed":
        return "Limited Reach (RF only)"
    if mode == "maximum_reach":
        return "Maximum Reach (RF + Internet)"
    if mode == "maximum_reach_relay":
        return "Maximum Reach relay"
    if mode == "internet_only":
        return "Internet only"
    return "Unknown reach"


def commstat_transport_label(value: object) -> str:
    mode = str(value or "").strip().lower()
    if mode == "js8":
        return "JS8/RF"
    if mode == "js8+internet":
        return "JS8/RF + Internet"
    if mode == "internet":
        return "Internet"
    return "Unknown transport"


def report_group_for_target(target: object) -> str:
    value = str(target or "").strip()
    if value.startswith("@"):
        return normalize_group_name(value)
    return ""


def expand_plus_shorthand(status_code: object) -> str:
    value = str(status_code or "").strip()
    return "111111111111" if value == "+" else value


def extract_brevity_code(remarks: object) -> str:
    text = str(remarks or "").upper()
    matches = _BREVITY_RE.findall(text)
    if not matches:
        return ""
    return str(matches[-1] or "").strip().upper()


def infer_state_and_geo(grid: object, remarks: object) -> Tuple[str, str, str]:
    grid_txt = str(grid or "").strip().upper()
    state_code, state_confidence = _state_code_from_remarks(remarks)
    if len(grid_txt) >= 6:
        if state_code:
            return state_code, state_confidence, "grid6"
        return "", "unknown", "grid6"
    if len(grid_txt) == 4:
        if state_code:
            return state_code, "grid4_remarks" if state_confidence == "explicit" else state_confidence, "grid4_state"
        return "", "unknown", "unknown"
    if state_code:
        return state_code, state_confidence, "state_only"
    return "", "unknown", "unknown"


def decode_brevity_summary(code: object, asset_dir: Optional[Path]) -> str:
    brevity_code = str(code or "").strip().upper()
    if not re.fullmatch(r"[1-5][A-Z]{5}", brevity_code):
        return ""
    if asset_dir is None:
        return ""
    list_id = brevity_code[0]
    assets = _load_brevity_assets(str(asset_dir))
    positions = assets.get(list_id)
    if not isinstance(positions, dict):
        return ""

    emergency_name = _lookup_named_code(positions.get("emergency_type"), brevity_code[1])
    status_name = _lookup_named_code(positions.get("status_codes"), brevity_code[2])
    impact_name = _lookup_named_code(positions.get("shared_impacts"), brevity_code[3])
    response_name = _lookup_named_code(positions.get("public_reaction"), brevity_code[4])
    station_name = _lookup_named_code(positions.get("station_response"), brevity_code[5])
    parts = [p for p in (emergency_name, status_name, impact_name, response_name, station_name) if p]
    if not parts:
        return ""
    return f"{brevity_code}: " + " | ".join(parts)


def parse_commstat_message(
    message_text: object,
    *,
    target_hint: object = "",
    source_value: object = "",
    global_id: object = 0,
    asset_dir: Optional[Path] = None,
) -> Optional[Dict[str, object]]:
    raw_text = str(message_text or "")
    text = normalize_commstat_text(raw_text)
    if not text:
        return None
    parsed = _parse_standard_statrep_message(
        text,
        raw_message=raw_text,
        target_hint=target_hint,
        source_value=source_value,
        global_id=global_id,
        asset_dir=asset_dir,
    )
    if parsed is not None:
        return parsed
    return _parse_fcode_message(
        text,
        raw_message=raw_text,
        target_hint=target_hint,
        source_value=source_value,
        global_id=global_id,
        asset_dir=asset_dir,
    )


def _parse_standard_statrep_message(
    text: str,
    *,
    raw_message: str,
    target_hint: object,
    source_value: object,
    global_id: object,
    asset_dir: Optional[Path],
) -> Optional[Dict[str, object]]:
    is_forwarded = "{F%}" in text
    marker = "{F%}" if is_forwarded else "{&%}"
    if marker not in text:
        return None
    match = re.search(r",(.+?)" + re.escape(marker), text, re.IGNORECASE)
    if not match:
        return None
    fields = match.group(1).split(",")
    if len(fields) < 4:
        return None

    grid = str(fields[0] or "").strip().upper()
    scope = COMMSTAT_SCOPE_MAP.get(str(fields[1] or "").strip(), str(fields[1] or "").strip())
    report_id = str(fields[2] or "").strip()
    status_code = expand_plus_shorthand(fields[3])
    if len(status_code) < 12 or not str(status_code[:12]).isdigit():
        return None

    remarks_text = ",".join(field for field in fields[4:] if str(field or "").strip()).strip()
    report_group = report_group_for_target(target_hint)
    state_code, state_confidence, geo_confidence = infer_state_and_geo(grid, remarks_text)
    brevity_code = extract_brevity_code(remarks_text)
    brevity_summary = decode_brevity_summary(brevity_code, asset_dir)

    return {
        "subtype": "COMMSTAT_FWD" if is_forwarded else "COMMSTAT_12",
        "grid": grid,
        "scope": scope,
        "status_payload": {
            "status": status_code[:12],
            "scope": scope,
        },
        "metadata": {
            "report_group": report_group,
            "transport_mode": transport_mode_for_source(source_value, raw_message or text, global_id=global_id),
            "origin_path": commstat_origin_path(source_value),
            "reach_mode": commstat_reach_mode(source_value, global_id=global_id, raw_message=raw_message or text),
            "remarks_text": remarks_text,
            "brevity_code": brevity_code,
            "brevity_summary": brevity_summary,
            "state_code": state_code,
            "state_confidence": state_confidence,
            "geo_confidence": geo_confidence,
        },
        "raw_payload": {
            "sr_id": report_id,
            "message": text,
            "remarks": remarks_text,
            "forwarded": bool(is_forwarded),
            "origin_path": commstat_origin_path(source_value),
            "reach_mode": commstat_reach_mode(source_value, global_id=global_id, raw_message=raw_message or text),
            "global_id": _positive_int(global_id),
        },
    }


def _parse_fcode_message(
    text: str,
    *,
    raw_message: str,
    target_hint: object,
    source_value: object,
    global_id: object,
    asset_dir: Optional[Path],
) -> Optional[Dict[str, object]]:
    parsed = _match_fcode(text, "F!304", 8)
    if parsed is None:
        parsed = _match_fcode(text, "F!301", 9)
    if parsed is None:
        return None

    form_id, responses, remarks_text = parsed
    report_group = report_group_for_target(target_hint)
    state_code, state_confidence, geo_confidence = infer_state_and_geo("", remarks_text)
    brevity_code = extract_brevity_code(remarks_text)
    brevity_summary = decode_brevity_summary(brevity_code, asset_dir)
    scope = COMMSTAT_SCOPE_MAP.get(responses[:1], "") if form_id == "F!301" and responses else ""

    return {
        "subtype": "SPOTTER_301" if form_id == "F!301" else "SPOTTER_304",
        "grid": "",
        "scope": scope,
        "status_payload": {
            "responses": responses,
        },
        "metadata": {
            "report_group": report_group,
            "transport_mode": transport_mode_for_source(source_value, raw_message or text, global_id=global_id),
            "origin_path": commstat_origin_path(source_value),
            "reach_mode": commstat_reach_mode(source_value, global_id=global_id, raw_message=raw_message or text),
            "remarks_text": remarks_text,
            "brevity_code": brevity_code,
            "brevity_summary": brevity_summary,
            "state_code": state_code,
            "state_confidence": state_confidence,
            "geo_confidence": geo_confidence,
        },
        "raw_payload": {
            "message": text,
            "remarks": remarks_text,
            "form_id": form_id,
            "origin_path": commstat_origin_path(source_value),
            "reach_mode": commstat_reach_mode(source_value, global_id=global_id, raw_message=raw_message or text),
            "global_id": _positive_int(global_id),
        },
    }


def _match_fcode(text: str, form_id: str, digit_count: int) -> Optional[Tuple[str, str, str]]:
    pattern = rf"{re.escape(form_id)}\s+(\d{{{digit_count}}})\s*(.*?)(?:>])?$"
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return None
    responses = str(match.group(1) or "").strip()
    remarks_text = str(match.group(2) or "").strip()
    return form_id, responses, remarks_text


def _leading_state_code(remarks: object) -> str:
    tokens = re.findall(r"[A-Z]{2,}", str(remarks or "").upper())
    if not tokens:
        return ""
    first = str(tokens[0] or "").strip().upper()
    if first in VALID_STATE_CODES:
        return first
    if first == "NTR" and len(tokens) > 1:
        second = str(tokens[1] or "").strip().upper()
        if second in VALID_STATE_CODES:
            return second
    return ""


def _state_code_from_remarks(remarks: object) -> Tuple[str, str]:
    leading = _leading_state_code(remarks)
    if leading:
        return leading, "explicit"
    text = str(remarks or "").upper()
    if not text:
        return "", "unknown"
    for name, abbr in sorted(STATE_NAME_TO_CODE.items(), key=lambda item: -len(item[0])):
        for match in re.finditer(rf"\b{re.escape(name)}\b", text):
            trailing = text[match.end() : match.end() + 12]
            if re.match(r"\s+(?:ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|LN|LANE|BLVD)\b", trailing):
                continue
            return abbr, "remarks"
    patterns = (
        r"^\s*([A-Z]{2})\s*[:;-]",
        r"\b([A-Z]{2})\s*/\s*[A-R]{2}\d{2}(?:[A-X]{2})?\b",
        r"\b(?:STATE|ST|LOC|LOCATION|AREA)\s*[:=]?\s*([A-Z]{2})\b",
        r"\b[A-Z][A-Z .'-]{2,40}\s+([A-Z]{2})\s+\d{5}(?:-\d{4})?\b",
        r"\b[A-Z][A-Z .'-]{2,40}\s+([A-Z]{2})\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            abbr = str(match.group(1) or "").strip().upper()
            if abbr in VALID_STATE_CODES and _state_abbr_context_is_location(text, match):
                return abbr, "remarks"
    return "", "unknown"


def _state_abbr_context_is_location(text: str, match: re.Match[str]) -> bool:
    abbr = str(match.group(1) or "").strip().upper()
    if abbr not in _AMBIGUOUS_STATE_WORDS:
        return True
    prefix = text[: match.start(1)]
    words = re.findall(r"[A-Z]+", prefix)
    prev = words[-1] if words else ""
    if prev in _NON_LOCATION_PRECEDERS:
        return False
    return True


def _lookup_named_code(section: object, code: str) -> str:
    if not isinstance(section, dict):
        return ""
    entry = section.get(code)
    if isinstance(entry, dict):
        return str(entry.get("name") or "").strip()
    return ""


@lru_cache(maxsize=16)
def _load_brevity_assets(asset_dir: str) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    base = Path(asset_dir)
    if not base.exists():
        return out
    for path in sorted(base.glob("[0-9]-*.json")):
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue
        if not _valid_brevity_data(data):
            continue
        list_id = path.name[:1]
        out[list_id] = data
    return out


def _valid_brevity_data(data: object) -> bool:
    if not isinstance(data, dict):
        return False
    required = {
        "emergency_type",
        "status_codes",
        "public_reaction",
        "station_response",
        "shared_impacts",
    }
    if not required.issubset(set(data.keys())):
        return False
    return True
