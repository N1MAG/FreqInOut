from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence


MAPPER_SETTINGS_KEY = "js8_spotter_form_mappings"

PURPOSE_IGNORE = "Ignore"
PURPOSE_GENERIC = "Generic Message"
PURPOSE_NET_CHECKIN = "Net Check-in"
PURPOSE_NET_NOTIFICATION = "Net Notification"
PURPOSE_SITREP = "SitRep / StatRep"
PURPOSE_WEATHER = "Weather / Storm"
PURPOSE_HAZARD = "Hazard / Early Warning"
PURPOSE_INTEL = "Intel / RFI"
PURPOSE_SUPPLY = "Supply / Area Assessment"
PURPOSE_MEDICAL = "Medical / Hospital"
PURPOSE_STATION = "Station Capability"
PURPOSE_INFRASTRUCTURE = "Infrastructure Status"
PURPOSE_CUSTOM = "Custom"

PURPOSE_OPTIONS: Sequence[str] = (
    PURPOSE_IGNORE,
    PURPOSE_GENERIC,
    PURPOSE_NET_CHECKIN,
    PURPOSE_NET_NOTIFICATION,
    PURPOSE_SITREP,
    PURPOSE_WEATHER,
    PURPOSE_HAZARD,
    PURPOSE_INTEL,
    PURPOSE_SUPPLY,
    PURPOSE_MEDICAL,
    PURPOSE_STATION,
    PURPOSE_INFRASTRUCTURE,
    PURPOSE_CUSTOM,
)

FORM_TOKEN_RE = re.compile(r"\bF![0-9]{3}[A-Z]?\b", re.IGNORECASE)
FORM_FILE_RE = re.compile(r"^MCF([0-9]{3}[A-Z]?)$", re.IGNORECASE)


@dataclass(frozen=True)
class SpotterFormDefinition:
    form_code: str
    title: str
    path: str = ""


@dataclass(frozen=True)
class SpotterFormField:
    key: str
    label: str
    options: tuple[tuple[str, str], ...] = ()


def normalize_form_code(value: object) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text.startswith("MCF"):
        text = "F!" + text[3:]
    elif text.startswith("F!"):
        pass
    elif text.startswith("F"):
        text = "F!" + text[1:]
    elif text[0].isdigit():
        text = f"F!{text}"
    match = FORM_TOKEN_RE.search(text)
    return match.group(0).upper() if match else ""


def extract_form_codes(text: object) -> List[str]:
    found = [normalize_form_code(match.group(0)) for match in FORM_TOKEN_RE.finditer(str(text or ""))]
    return list(dict.fromkeys(code for code in found if code))


def _read_form_title(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                clean = line.strip()
                if not clean:
                    continue
                return clean.split("|", 1)[0].strip() if "|" in clean else clean
    except Exception:
        return ""
    return ""


def discover_spotter_forms(forms_dir: object) -> List[SpotterFormDefinition]:
    try:
        root = Path(str(forms_dir or "")).expanduser()
    except Exception:
        return []
    if not root.exists() or not root.is_dir():
        return []
    out: List[SpotterFormDefinition] = []
    for path in sorted(root.glob("MCF*.txt"), key=lambda item: item.name.upper()):
        match = FORM_FILE_RE.match(path.stem)
        if not match:
            continue
        form_code = f"F!{match.group(1).upper()}"
        out.append(SpotterFormDefinition(form_code=form_code, title=_read_form_title(path), path=str(path)))
    return out


def parse_spotter_form_fields(text: object) -> List[SpotterFormField]:
    """Parse JS8Spotter MCForms question/option rows into editable field metadata."""
    fields: List[SpotterFormField] = []
    current_label = ""
    current_options: List[tuple[str, str]] = []

    def flush() -> None:
        nonlocal current_label, current_options
        label = current_label.strip()
        if not label:
            current_options = []
            return
        key_base = re.sub(r"[^A-Za-z0-9]+", "_", label.upper()).strip("_") or f"FIELD_{len(fields) + 1}"
        key = key_base
        suffix = 2
        existing = {field.key for field in fields}
        while key in existing:
            key = f"{key_base}_{suffix}"
            suffix += 1
        fields.append(SpotterFormField(key=key, label=label, options=tuple(current_options)))
        current_label = ""
        current_options = []

    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("?"):
            flush()
            current_label = line[1:].strip().strip(":")
            continue
        if line.startswith("@") and current_label:
            parts = line[1:].strip().split(maxsplit=1)
            if not parts:
                continue
            token = parts[0].strip()
            label = parts[1].strip() if len(parts) > 1 else token
            if token:
                current_options.append((token, label))
            continue
    flush()
    return fields


def factory_mapping_for_form(form_code: object, title: object = "") -> Dict[str, object]:
    code = normalize_form_code(form_code)
    title_upper = str(title or "").upper()
    purpose = PURPOSE_GENERIC
    messages = True
    map_use = False
    alert = False
    net = False
    status = False

    if code in {"F!103", "F!700", "F!702A"} or "CHECKIN" in title_upper or "CHECK-IN" in title_upper:
        purpose = PURPOSE_NET_CHECKIN
        net = True
        status = code == "F!104"
    elif (
        code in {"F!104", "F!300", "F!301", "F!304", "F!701", "F!701A", "F!701B"}
        or "SITREP" in title_upper
        or "STATREP" in title_upper
        or "STAT-REP" in title_upper
        or "STATUS REPORT" in title_upper
        or "SITUATION REPORT" in title_upper
    ):
        purpose = PURPOSE_SITREP
        map_use = True
        status = code in {"F!104", "F!301", "F!304"}
    elif code in {"F!106", "F!108"} or "NET NOTICE" in title_upper or "NOTIFICATION" in title_upper:
        purpose = PURPOSE_NET_NOTIFICATION
        alert = True
        net = True
    elif code in {"F!305"} or "EARLY WARNING" in title_upper or "ALERT" in title_upper:
        purpose = PURPOSE_HAZARD
        map_use = True
        alert = True
    elif code in {"F!307", "F!504"} or "WEATHER" in title_upper or "WILDFIRE" in title_upper or "STORM" in title_upper:
        purpose = PURPOSE_WEATHER
        map_use = True
        alert = code == "F!307"
    elif code in {"F!105", "F!107", "F!308", "F!701"} or "RFI" in title_upper or "INTEL" in title_upper or "OBSERVATION" in title_upper:
        purpose = PURPOSE_INTEL
        map_use = code in {"F!701"}
        alert = code in {"F!107"}
    elif code in {"F!500", "F!505"} or "SUPPLY" in title_upper or "AREA ASSESSMENT" in title_upper:
        purpose = PURPOSE_SUPPLY
        map_use = True
    elif code in {"F!302", "F!303", "F!703"} or "HOSPITAL" in title_upper or "MEDIVAC" in title_upper or "MEDICAL" in title_upper:
        purpose = PURPOSE_MEDICAL
        map_use = True
        alert = True
    elif code in {"F!100", "F!101", "F!102", "F!702"} or "STATION" in title_upper or "EQUIPMENT" in title_upper:
        purpose = PURPOSE_STATION
    elif code in {"F!306"} or "POWER" in title_upper or "INFRASTRUCTURE" in title_upper:
        purpose = PURPOSE_INFRASTRUCTURE
        map_use = True
        status = True

    return {
        "form_code": code,
        "title": str(title or "").strip(),
        "purpose": purpose,
        "messages": messages,
        "map": map_use,
        "alert": alert,
        "net": net,
        "status": status,
    }


def normalize_mapping_row(row: Mapping[str, object], *, title: str = "") -> Dict[str, object]:
    code = normalize_form_code(row.get("form_code") or row.get("form") or row.get("code"))
    base = factory_mapping_for_form(code, title or row.get("title", ""))
    purpose = str(row.get("purpose", base["purpose"]) or base["purpose"]).strip()
    if purpose not in PURPOSE_OPTIONS:
        purpose = str(base["purpose"])
    base.update(
        {
            "form_code": code,
            "title": str(row.get("title", title or base.get("title", "")) or "").strip(),
            "purpose": purpose,
            "messages": bool(row.get("messages", base["messages"])),
            "map": bool(row.get("map", base["map"])),
            "alert": bool(row.get("alert", base["alert"])),
            "net": bool(row.get("net", base["net"])),
            "status": bool(row.get("status", base["status"])),
        }
    )
    if purpose == PURPOSE_IGNORE:
        base.update({"messages": False, "map": False, "alert": False, "net": False, "status": False})
    return base


def normalize_mapping_rows(rows: object) -> List[Dict[str, object]]:
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        norm = normalize_mapping_row(row)
        code = str(norm.get("form_code") or "")
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(norm)
    return out


def effective_mapping_rows(settings, forms_dir: object = None) -> List[Dict[str, object]]:
    configured = {}
    try:
        raw_rows = settings.get(MAPPER_SETTINGS_KEY, []) if settings is not None else []
    except Exception:
        raw_rows = []
    for row in normalize_mapping_rows(raw_rows):
        configured[str(row.get("form_code") or "")] = row

    if forms_dir is None and settings is not None:
        try:
            forms_dir = settings.get("js8_forms_path", "")
        except Exception:
            forms_dir = ""

    out: List[Dict[str, object]] = []
    seen: set[str] = set()
    for definition in discover_spotter_forms(forms_dir):
        base = factory_mapping_for_form(definition.form_code, definition.title)
        row = dict(configured.get(definition.form_code, base))
        row["form_code"] = definition.form_code
        row["title"] = str(row.get("title") or definition.title or base.get("title", "") or "").strip()
        out.append(normalize_mapping_row(row, title=definition.title))
        seen.add(definition.form_code)

    for code, row in configured.items():
        if code not in seen:
            out.append(normalize_mapping_row(row))
    return out


def forms_enabled_for(settings, *, purpose: str = "", flag: str = "") -> set[str]:
    rows = effective_mapping_rows(settings)
    out: set[str] = set()
    for row in rows:
        if purpose and str(row.get("purpose") or "") != purpose:
            continue
        if flag and not bool(row.get(flag, False)):
            continue
        code = str(row.get("form_code") or "").strip().upper()
        if code:
            out.add(code)
    return out


def custom_mapper_configured(settings) -> bool:
    try:
        raw_rows = settings.get(MAPPER_SETTINGS_KEY, []) if settings is not None else []
    except Exception:
        return False
    return isinstance(raw_rows, list) and bool(raw_rows)


def form_codes_enabled_for(settings, *, flag: str) -> set[str] | None:
    if not custom_mapper_configured(settings):
        return None
    return forms_enabled_for(settings, flag=flag)


def form_id_enabled(form_id: object, enabled_codes: set[str] | None) -> bool:
    if enabled_codes is None:
        return True
    code = normalize_form_code(form_id)
    return bool(code and code in enabled_codes)


def legacy_default_forms_for(*, purpose: str = "", flag: str = "") -> set[str]:
    defaults = [
        factory_mapping_for_form("F!103", "Net Checkin"),
        factory_mapping_for_form("F!104", "@SITREP Basic Check-in"),
        factory_mapping_for_form("F!106", "Impromptu Net Notice"),
    ]
    out: set[str] = set()
    for row in defaults:
        if purpose and str(row.get("purpose") or "") != purpose:
            continue
        if flag and not bool(row.get(flag, False)):
            continue
        out.add(str(row["form_code"]))
    return out
