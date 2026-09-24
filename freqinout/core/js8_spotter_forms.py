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

ALPHABETIC_FORM_IDS = frozenset({"BDN"})
FORM_ID_PATTERN = rf"(?:[0-9]{{3}}[A-Z]?|{'|'.join(sorted(ALPHABETIC_FORM_IDS))})"
FORM_TOKEN_RE = re.compile(rf"\bF!{FORM_ID_PATTERN}\b", re.IGNORECASE)
FORM_FILE_RE = re.compile(rf"^MCF({FORM_ID_PATTERN})$", re.IGNORECASE)


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
    kind: str = "choice"
    default_value: str = ""
    description: str = ""


SPOTTER_COMMENTS_KEY = "COMMENTS"
SPOTTER_COMMENTS_MAX_LENGTH = 50

# These are intentionally explicit form semantics.  A generic label match can
# incorrectly put the operator's QTH into an incident, affected-area, or
# "other area" field.  Expand this table only after reviewing the form text.
_OPERATOR_AUTOFILL_FIELDS: Mapping[str, Mapping[str, str]] = {
    "F!104": {"ST": "state", "GR": "grid"},
    "F!105": {"CS": "callsign", "ST": "state", "GR": "grid"},
    "F!108": {"ST": "state", "GR": "grid"},
    "F!306": {"ST": "state", "GR": "grid"},
    "F!500": {"ST": "state", "GR": "grid"},
    "F!504": {"ST": "state", "GR": "grid"},
    "F!701A": {"FR": "callsign"},
    "F!701C": {"ST": "state", "GR": "grid"},
    "F!BDN": {"GR": "grid"},
}


def bundled_spotter_forms_dir() -> Path:
    """Return FIO's packaged station-level Spotter form catalog."""

    return Path(__file__).resolve().parent.parent / "resources" / "spotter_forms"


def resolve_spotter_forms_dir(configured_dir: object = None) -> Path:
    """Resolve an optional advanced custom catalog, then the bundled catalog.

    The normal guided path deliberately supplies no per-radio folder.  A
    configured custom catalog remains an advanced station-level override, but
    an empty, missing, or invalid override always falls back to FIO's catalog.
    """

    configured = str(configured_dir or "").strip()
    if configured:
        try:
            candidate = Path(configured).expanduser()
            if candidate.exists() and candidate.is_dir():
                return candidate
        except (OSError, TypeError, ValueError):
            pass
    return bundled_spotter_forms_dir()


def normalize_form_code(value: object) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text.startswith("MCF"):
        text = "F!" + text[3:]
    elif text.startswith("F!"):
        pass
    elif re.fullmatch(r"F[0-9]{3}[A-Z]?", text):
        text = "F!" + text[1:]
    elif text[0].isdigit():
        text = f"F!{text}"
    elif text in ALPHABETIC_FORM_IDS:
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
        root = resolve_spotter_forms_dir(forms_dir)
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
    """Parse the editable portion of a JS8Spotter MCForms definition.

    Choice questions, their explicit ``*`` defaults, and ``[XX]`` structured
    prompts are retained in source order.  Heading/instruction text immediately
    before a field is attached as guidance instead of being silently discarded.
    The FIO-provided universal Comments field is added by the Compose surface,
    not by this catalog parser.
    """
    fields: List[SpotterFormField] = []
    current_label = ""
    current_options: List[tuple[str, str]] = []
    current_default = ""
    pending_guidance: List[str] = []
    current_guidance: List[str] = []

    def flush() -> None:
        nonlocal current_label, current_options, current_default, current_guidance
        label = current_label.strip()
        if not label:
            current_options = []
            current_default = ""
            current_guidance = []
            return
        key_base = re.sub(r"[^A-Za-z0-9]+", "_", label.upper()).strip("_") or f"FIELD_{len(fields) + 1}"
        key = key_base
        suffix = 2
        existing = {field.key for field in fields}
        while key in existing:
            key = f"{key_base}_{suffix}"
            suffix += 1
        fields.append(
            SpotterFormField(
                key=key,
                label=label,
                options=tuple(current_options),
                kind="choice",
                default_value=current_default,
                description=" ".join(part for part in current_guidance if part).strip(),
            )
        )
        current_label = ""
        current_options = []
        current_default = ""
        current_guidance = []

    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("?"):
            flush()
            current_label = line[1:].strip().strip(":")
            current_guidance = list(pending_guidance)
            pending_guidance = []
            continue
        if line.startswith("@") and current_label:
            parts = line[1:].strip().split(maxsplit=1)
            if not parts:
                continue
            token = parts[0].strip()
            label = parts[1].strip() if len(parts) > 1 else token
            selected = label.startswith("*")
            if selected:
                label = label[1:].lstrip()
            if token:
                current_options.append((token, label))
                if selected:
                    current_default = token
            continue
        prompt_match = re.match(r"^\[([A-Z0-9]{2})\]\s*(.*?)\s*$", line, flags=re.IGNORECASE)
        if prompt_match:
            flush()
            key = prompt_match.group(1).upper()
            label = prompt_match.group(2).strip().rstrip(":") or key
            fields.append(
                SpotterFormField(
                    key=key,
                    label=label,
                    kind="prompt",
                    description=" ".join(part for part in pending_guidance if part).strip(),
                )
            )
            pending_guidance = []
            continue
        if line.startswith(("!", ".")):
            guidance = line.lstrip("!.").strip()
            if guidance:
                pending_guidance.append(guidance)
            continue
    flush()
    return fields


def parse_spotter_form_guidance(text: object) -> tuple[str, ...]:
    """Return concise operator-facing headings/instructions in source order."""
    guidance: List[str] = []
    for raw_line in str(text or "").splitlines()[1:]:
        line = raw_line.strip()
        if not line or line.startswith("#") or not line.startswith(("!", ".")):
            continue
        clean = line.lstrip("!.").strip()
        if not clean or set(clean) <= {"-", "_"}:
            continue
        lower = clean.lower()
        if lower.startswith("version") or lower.startswith("date:") or lower.startswith("more info"):
            continue
        if clean not in guidance:
            guidance.append(clean)
    return tuple(guidance)


def spotter_operator_autofill_kind(form_code: object, field_key: object) -> str:
    """Return the reviewed operator-identity default for a form field.

    An empty result is a deliberate deny.  In particular, affected-area,
    incident, assessment, destination, wildfire, medivac, and "other area"
    fields must stay operator-entered.
    """
    code = normalize_form_code(form_code)
    key = str(field_key or "").strip().upper()
    return str(_OPERATOR_AUTOFILL_FIELDS.get(code, {}).get(key, "") or "")


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
        status = code in {"F!104", "F!701C"}
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
        status = code in {"F!104", "F!301", "F!304", "F!701B", "F!701C"}
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
    elif code in {"F!500", "F!505", "F!BDN"} or "SUPPLY" in title_upper or "AREA ASSESSMENT" in title_upper or "PRICE SURVEY" in title_upper:
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
