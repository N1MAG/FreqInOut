from __future__ import annotations

import datetime as dt
import html
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


NBEMS_FLMSG_VERSION = "4.0.24"
FORM_FAMILY_ORDER: Tuple[str, ...] = ("CUSTOM", "ICS", "CSV", "FLAMP", "TRANSFERS", "WRAP", "ARQ", "ARC")
FORM_TEMPLATE_SUFFIXES: Tuple[str, ...] = (".html", ".htm")
VARAC_BBS_SAFE_SUFFIXES: Tuple[str, ...] = (
    ".k2s.sig",
    ".b2s.sig",
    ".k2s.asc",
    ".b2s.asc",
    ".k2s.gpg",
    ".b2s.gpg",
    ".k2s",
    ".b2s",
    ".txt",
    ".rtf",
    ".html",
    ".htm",
    ".sig",
    ".asc",
    ".gpg",
)
FASTLIGHT_FILENAME_DELIMITER_OPTIONS = {"group_default", "underscore", "hyphen", "custom"}
FASTLIGHT_SIGNED_SUFFIX_OPTIONS = {"group_default", "dot_sig", "dash_sig"}
FASTLIGHT_UNSIGNED_SUFFIX_OPTIONS = {"group_default", "k2s", "b2s"}
FASTLIGHT_FORM_FAMILY_DEFAULT = "group_default"


@dataclass(frozen=True)
class ComposeFormFamily:
    key: str
    label: str
    path: Path


@dataclass(frozen=True)
class ComposeFormTemplate:
    family_key: str
    family_label: str
    path: Path
    display_name: str


@dataclass(frozen=True)
class ComposeFieldOption:
    value: str
    label: str
    selected: bool = False


@dataclass(frozen=True)
class ComposeFieldDefinition:
    key: str
    label: str
    description: str = ""
    field_type: str = "text"
    placeholder: str = ""
    options: Tuple[ComposeFieldOption, ...] = ()
    allow_custom: bool = False
    rows: int = 0


@dataclass(frozen=True)
class ComposeDestinationPlan:
    key: str
    label: str
    requested: bool
    ready: bool
    directory: str
    path: str
    note: str = ""


@dataclass(frozen=True)
class ComposeMessageFolderOption:
    label: str
    relative_path: str
    path: Path


@dataclass(frozen=True)
class FastLightFilenamePolicy:
    delimiter: str = "-"
    signed_marker: str = "-sig"
    unsigned_extension: str = ""
    source_group: str = ""
    source_label: str = "Default"


def _normalize_fastlight_group_name(value: object) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").strip().upper().lstrip("@"))


def _fastlight_group_default_policy(group_name: object = "") -> FastLightFilenamePolicy:
    normalized = _normalize_fastlight_group_name(group_name)
    if normalized.startswith("MAGNET") or re.fullmatch(r"MR\d{1,2}[A-Z0-9]*", normalized or ""):
        return FastLightFilenamePolicy(
            delimiter="_",
            signed_marker=".sig",
            source_group=str(group_name or "").strip().upper().lstrip("@"),
            source_label="MagNet default",
        )
    if normalized.startswith("AMRRON"):
        return FastLightFilenamePolicy(
            delimiter="-",
            signed_marker="-sig",
            source_group=str(group_name or "").strip().upper().lstrip("@"),
            source_label="AMRRON default",
        )
    return FastLightFilenamePolicy(
        delimiter="-",
        signed_marker="-sig",
        source_group=str(group_name or "").strip().upper().lstrip("@"),
        source_label="FIO default",
    )


def normalize_fastlight_filename_policy(
    values: Mapping[str, object] | FastLightFilenamePolicy | None = None,
    *,
    group_name: object = "",
) -> FastLightFilenamePolicy:
    """Resolve a FastLight compose filename policy with group-aware defaults."""
    if isinstance(values, FastLightFilenamePolicy):
        return values
    base = _fastlight_group_default_policy(group_name)
    data = values if isinstance(values, Mapping) else {}

    delimiter_mode = str(data.get("fastlight_filename_delimiter", "group_default") or "group_default").strip().lower()
    custom_delimiter = str(data.get("fastlight_custom_delimiter", "") or "").strip()
    if delimiter_mode == "underscore":
        delimiter = "_"
    elif delimiter_mode == "hyphen":
        delimiter = "-"
    elif delimiter_mode == "custom" and custom_delimiter:
        delimiter = sanitize_filename_component(custom_delimiter, replacement="-")[:3] or base.delimiter
    else:
        delimiter = base.delimiter
    if delimiter not in {"-", "_", "."} and not re.fullmatch(r"[A-Za-z0-9_.-]{1,3}", delimiter):
        delimiter = base.delimiter

    signed_mode = str(data.get("fastlight_signed_suffix", "group_default") or "group_default").strip().lower()
    if signed_mode in {"dot_sig", ".sig", "sig_dot"}:
        signed_marker = ".sig"
    elif signed_mode in {"dash_sig", "-sig", "sig_dash"}:
        signed_marker = "-sig"
    else:
        signed_marker = base.signed_marker

    unsigned_mode = str(data.get("fastlight_unsigned_suffix", "group_default") or "group_default").strip().lower()
    if unsigned_mode in {"k2s", ".k2s"}:
        unsigned_extension = ".k2s"
    elif unsigned_mode in {"b2s", ".b2s"}:
        unsigned_extension = ".b2s"
    else:
        unsigned_extension = ""

    group_txt = str(group_name or data.get("group") or data.get("group_name") or "").strip().upper().lstrip("@")
    return FastLightFilenamePolicy(
        delimiter=delimiter,
        signed_marker=signed_marker,
        unsigned_extension=unsigned_extension,
        source_group=group_txt,
        source_label=base.source_label if delimiter_mode == "group_default" and signed_mode == "group_default" else "Configured",
    )


def resolve_fastlight_filename_policy(
    operating_groups: Iterable[Mapping[str, object]] | None,
    target_group: object = "",
) -> FastLightFilenamePolicy:
    """Resolve filename policy for a target group from saved HF Operating Groups."""
    target_norm = _normalize_fastlight_group_name(target_group)
    matched: Mapping[str, object] | None = None
    if target_norm and operating_groups is not None:
        for row in operating_groups:
            if not isinstance(row, Mapping):
                continue
            row_group = row.get("group") or row.get("group_name") or row.get("name") or ""
            if _normalize_fastlight_group_name(row_group) == target_norm:
                matched = row
                break
    return normalize_fastlight_filename_policy(matched, group_name=target_group)


def resolve_fastlight_form_family(
    operating_groups: Iterable[Mapping[str, object]] | None,
    target_group: object = "",
) -> str:
    """Resolve the preferred custom form family for a target operating group."""
    target_norm = _normalize_fastlight_group_name(target_group)
    if not target_norm or operating_groups is None:
        return ""
    for row in operating_groups:
        if not isinstance(row, Mapping):
            continue
        row_group = row.get("group") or row.get("group_name") or row.get("name") or ""
        if _normalize_fastlight_group_name(row_group) != target_norm:
            continue
        value = str(row.get("fastlight_form_family", "") or "").strip().upper()
        if not value or value == FASTLIGHT_FORM_FAMILY_DEFAULT.upper():
            return ""
        return value
    return ""


def resolve_flamp_transmit_dir(path_text: str) -> str:
    """Derive an outbound FLAmp staging folder from the configured FLAmp receive path."""
    raw = str(path_text or "").strip()
    if not raw:
        return ""
    path = Path(raw).expanduser()
    name = path.name.lower()
    if name == "rx":
        return str(path.with_name("tx"))
    if name == "flamp":
        return str(path / "tx")
    if path.parent.name.lower() == "rx":
        return str(path.parent.with_name("tx"))
    return str(path)


def compose_message_relative_path(root: Path, folder: Path, *, max_depth: int = 2) -> Optional[str]:
    root_path = Path(root).expanduser()
    folder_path = Path(folder).expanduser()
    try:
        root_resolved = root_path.resolve(strict=False)
        folder_resolved = folder_path.resolve(strict=False)
        rel = folder_resolved.relative_to(root_resolved)
    except Exception:
        return None
    if rel == Path("."):
        return ""
    parts = rel.parts
    if len(parts) > int(max_depth):
        return None
    if any(part in {"", ".", ".."} or part.startswith(".") for part in parts):
        return None
    return Path(*parts).as_posix() if parts else ""


def resolve_compose_message_folder(root: str | Path, relative_path: str, *, max_depth: int = 2) -> Optional[Path]:
    root_path = Path(root).expanduser()
    rel_text = str(relative_path or "").strip().replace("\\", "/")
    if not rel_text:
        return root_path
    rel_path = Path(rel_text)
    if rel_path.is_absolute():
        return None
    target = root_path / rel_path
    rel = compose_message_relative_path(root_path, target, max_depth=max_depth)
    if rel is None or rel != rel_path.as_posix():
        return None
    return target


def discover_compose_message_folders(root: str | Path, *, max_depth: int = 2) -> List[ComposeMessageFolderOption]:
    root_path = Path(root).expanduser()
    if not root_path.exists() or not root_path.is_dir():
        return []
    options = [ComposeMessageFolderOption(label="Messages", relative_path="", path=root_path)]
    found: List[Tuple[str, Path]] = []

    def visit(parent: Path, depth: int) -> None:
        if depth > int(max_depth):
            return
        try:
            children = sorted(
                [child for child in parent.iterdir() if child.is_dir() and not child.name.startswith(".")],
                key=lambda item: item.name.casefold(),
            )
        except Exception:
            return
        for child in children:
            if child.is_symlink():
                continue
            rel = compose_message_relative_path(root_path, child, max_depth=max_depth)
            if rel is None:
                continue
            found.append((rel, child))
            visit(child, depth + 1)

    visit(root_path, 1)
    for rel, path in sorted(found, key=lambda item: item[0].casefold()):
        options.append(ComposeMessageFolderOption(label=rel, relative_path=rel, path=path))
    return options


def resolve_nbems_root(
    settings,
    *,
    custom_override_key: str = "nbems_custom_forms_path",
) -> Optional[Path]:
    override = str(settings.get(custom_override_key, "") or "").strip()
    if override:
        override_path = Path(override).expanduser()
        if override_path.exists():
            if override_path.is_dir() and override_path.name.upper() == "CUSTOM":
                return override_path.parent
            return override_path

    msg_paths = settings.get("message_paths", {}) or {}
    for origin in ("flmsg", "flamp"):
        base = str(msg_paths.get(origin, "") or "").strip()
        if not base:
            continue
        base_path = Path(base).expanduser()
        for parent in (base_path, *base_path.parents):
            if parent.name.lower() in {"nbems.files", ".nbems"} and parent.exists():
                return parent
    return None


def discover_form_families(settings) -> List[ComposeFormFamily]:
    root = resolve_nbems_root(settings)
    if root is None:
        return []
    out: List[ComposeFormFamily] = []
    for key in FORM_FAMILY_ORDER:
        path = root / key
        if not path.exists() or not path.is_dir():
            continue
        label = "ICS" if key == "ICS" else key.title()
        out.append(ComposeFormFamily(key=key, label=label, path=path))
    return out


def discover_forms_for_family(family: ComposeFormFamily) -> List[ComposeFormTemplate]:
    out: List[ComposeFormTemplate] = []
    for child in sorted(family.path.iterdir(), key=lambda p: p.name.lower()):
        if not child.is_file():
            continue
        if child.suffix.lower() not in FORM_TEMPLATE_SUFFIXES:
            continue
        out.append(
            ComposeFormTemplate(
                family_key=family.key,
                family_label=family.label,
                path=child,
                display_name=child.stem.replace("_", " ").strip() or child.name,
            )
        )
    return out


def _strip_html(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"(?i)<br\s*/?>", "\n", text)
    cleaned = re.sub(r"(?i)</(?:p|div|td|tr|li|label|section|table|tbody|thead|span|em|strong|font)>", "\n", cleaned)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"[ \t\r\f\v]+", " ", cleaned)
    cleaned = re.sub(r"\n\s*\n+", "\n", cleaned)
    return cleaned.strip()


def _parse_tag_attributes(text: str) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    if not text:
        return attrs
    for name, dq, sq, bare in re.findall(
        r'([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))',
        text,
    ):
        attrs[name.lower()] = dq or sq or bare
    return attrs


def _normalize_field_label(text: str, *, fallback: str = "") -> str:
    label = str(text or "").strip()
    label = re.sub(r"^\s*(?:\[\s*\d+[A-Za-z]?\s*\]|\d+[A-Za-z]?[.)]?)\s*", "", label)
    label = re.sub(r"\s+", " ", label).strip(" :")
    return label or fallback


def _split_field_label_and_description(text: str, *, fallback: str = "") -> Tuple[str, str]:
    cleaned = _strip_html(text)
    if not cleaned:
        return fallback, ""
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    match = re.match(r"^(.*?)(?:\s*\(([^()]*)\))?\s*$", cleaned)
    if match:
        label = _normalize_field_label(match.group(1), fallback=fallback)
        description = re.sub(r"\s+", " ", str(match.group(2) or "")).strip()
        return label, description
    return _normalize_field_label(cleaned, fallback=fallback), ""


def extract_compose_template_title(template: str) -> str:
    if not template:
        return ""
    match = re.search(r"<title>(.*?)</title>", template, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return _strip_html(match.group(1))


def extract_compose_menu_item(template: str) -> str:
    if not template:
        return ""
    match = re.search(
        r'<meta[^>]+name=["\']menu_item["\'][^>]+content=["\']([^"\']+)["\']',
        template,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    return _strip_html(match.group(1))


def _extract_field_label_and_description(template: str, field_pos: int, field_key: str) -> Tuple[str, str]:
    if not template:
        return field_key, ""
    window = template[max(0, field_pos - 1800) : field_pos]
    label_match = None
    for candidate in re.finditer(r"<label[^>]*>(.*?)</label>", window, flags=re.IGNORECASE | re.DOTALL):
        label_match = candidate
    if label_match is not None:
        label_html = label_match.group(1)
        desc_parts = [
            _strip_html(group)
            for group in re.findall(r"<(?:span|em|small|i)[^>]*>(.*?)</(?:span|em|small|i)>", label_html, flags=re.I | re.S)
        ]
        label_only = re.sub(
            r"<(?:span|em|small|i)[^>]*>.*?</(?:span|em|small|i)>",
            " ",
            label_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        label = _normalize_field_label(_strip_html(label_only), fallback=field_key)
        description = re.sub(r"\s+", " ", " ".join(part for part in desc_parts if part)).strip()
        return label, description

    container_pos = -1
    for token in ("<td", "<div", "<p", "<section", "<tr"):
        container_pos = max(container_pos, window.lower().rfind(token))
    if container_pos == -1:
        container_pos = window.lower().rfind("<span")
    if container_pos == -1:
        return field_key, ""
    container_text = _strip_html(window[container_pos:])
    lines = [line.strip() for line in container_text.splitlines() if line.strip()]
    if not lines:
        return field_key, ""
    return _split_field_label_and_description(" ".join(lines), fallback=field_key)


def _parse_option_block(block: str) -> Tuple[ComposeFieldOption, ...]:
    options: List[ComposeFieldOption] = []
    if not block:
        return tuple(options)
    for match in re.finditer(r"<option\b(?P<attrs>[^>]*)>(?P<body>.*?)</option>", block, flags=re.IGNORECASE | re.DOTALL):
        attrs = _parse_tag_attributes(match.group(0))
        raw_tag = match.group(0)
        label = _strip_html(match.group("body"))
        value = attrs.get("value", label)
        selected = "selected" in raw_tag.lower()
        options.append(ComposeFieldOption(value=str(value or ""), label=str(label or value or ""), selected=selected))
    return tuple(options)


def _parse_datalist_options(template: str, list_id: str) -> Tuple[ComposeFieldOption, ...]:
    if not template or not list_id:
        return ()
    match = re.search(
        rf"<datalist\b[^>]*\bid=[\"']{re.escape(list_id)}[\"'][^>]*>(.*?)</datalist>",
        template,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ()
    return _parse_option_block(match.group(1))


def parse_compose_template_fields(template: str) -> List[ComposeFieldDefinition]:
    if not template:
        return []

    matches: List[Tuple[int, str, re.Match[str]]] = []
    select_re = re.compile(
        r"<select\b(?P<attrs>[^>]*)\bname=(?P<q>[\"'])(?P<name>L\d{1,2}[A-Za-z]?)\2[^>]*>(?P<body>.*?)</select>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    textarea_re = re.compile(
        r"<textarea\b(?P<attrs>[^>]*)\bname=(?P<q>[\"'])(?P<name>L\d{1,2}[A-Za-z]?)\2[^>]*>(?P<body>.*?)</textarea>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    input_re = re.compile(
        r"<input\b(?P<attrs>[^>]*)\bname=(?P<q>[\"'])(?P<name>L\d{1,2}[A-Za-z]?)\2[^>]*>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in select_re.finditer(template):
        matches.append((match.start(), "select", match))
    for match in textarea_re.finditer(template):
        matches.append((match.start(), "textarea", match))
    for match in input_re.finditer(template):
        matches.append((match.start(), "input", match))
    matches.sort(key=lambda item: item[0])

    out: List[ComposeFieldDefinition] = []
    seen: set[str] = set()
    for field_pos, field_type, match in matches:
        key = str(match.group("name") or "").strip().upper()
        if not key or key in seen:
            continue
        attrs = _parse_tag_attributes(match.group(0))
        input_kind = str(attrs.get("type", "text") or "text").strip().lower()
        if field_type == "input" and input_kind in {"hidden", "button", "submit", "reset", "image"}:
            continue
        label, description = _extract_field_label_and_description(template, field_pos, key)
        placeholder = str(attrs.get("placeholder", "") or "").strip()
        allow_custom = False
        options: Tuple[ComposeFieldOption, ...] = ()
        rows = 0
        resolved_type = "text"
        if field_type == "select":
            resolved_type = "select"
            options = _parse_option_block(match.group("body"))
        elif field_type == "textarea":
            resolved_type = "textarea"
            try:
                rows = int(str(attrs.get("rows", "0") or "0"))
            except Exception:
                rows = 0
        else:
            list_id = str(attrs.get("list", "") or "").strip()
            if list_id:
                resolved_type = "select"
                allow_custom = True
                options = _parse_datalist_options(template, list_id)
        out.append(
            ComposeFieldDefinition(
                key=key,
                label=label,
                description=description,
                field_type=resolved_type,
                placeholder=placeholder,
                options=options,
                allow_custom=allow_custom,
                rows=rows,
            )
        )
        seen.add(key)
    return out


def standard_blank_field_definitions() -> List[ComposeFieldDefinition]:
    return [
        ComposeFieldDefinition(key="TO", label="To", description="Recipient or group"),
        ComposeFieldDefinition(key="SUBJECT", label="Subject", description="Short description of the report"),
        ComposeFieldDefinition(
            key="MESSAGE",
            label="Message",
            description="Enter the message body",
            field_type="textarea",
            rows=10,
        ),
    ]


def _match_option_value(options: Sequence[ComposeFieldOption], candidates: Sequence[str]) -> str:
    wanted = {str(candidate or "").strip().upper() for candidate in candidates if str(candidate or "").strip()}
    if not wanted:
        return ""
    for option in options:
        if str(option.value or "").strip().upper() in wanted:
            return str(option.value or "")
        if str(option.label or "").strip().upper() in wanted:
            return str(option.value or "")
    return ""


def _selected_option_value(options: Sequence[ComposeFieldOption]) -> str:
    for option in options:
        if option.selected:
            return str(option.value or "")
    return ""


def _infer_form_recipient(*, family_key: str = "", template_name: str = "", template_title: str = "", menu_item: str = "") -> str:
    text = " ".join(str(part or "") for part in (family_key, template_name, template_title, menu_item)).lower()
    if "magnet" in text:
        return "MAGNET"
    if "amrron" in text:
        return "AMRRON"
    return ""


def format_flmsg_header_timestamp(when_utc: dt.datetime) -> str:
    stamp = _coerce_utc(when_utc)
    return stamp.strftime("%Y%d%m%H%M%S")


def format_compose_zulu(when_utc: dt.datetime) -> str:
    stamp = _coerce_utc(when_utc)
    return stamp.strftime("%y%m%d-%H%Mz").lower()


def format_compose_filename_stamp(when_utc: dt.datetime) -> str:
    stamp = _coerce_utc(when_utc)
    return stamp.strftime("%Y%m%d-%H%Mz").lower()


def sanitize_filename_component(value: str, *, replacement: str = "_") -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    chars: List[str] = []
    for ch in text:
        if ch.isalnum():
            chars.append(ch)
        elif ch in {" ", "-", "_", "."}:
            chars.append(ch)
        else:
            chars.append(replacement)
    text = "".join(chars)
    text = re.sub(r"\s+", "_", text.strip())
    text = re.sub(r"_+", "_", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("._-") or "Report"


def sanitize_report_name(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    cleaned = [ch for ch in text if ch.isalnum()]
    report = "".join(cleaned).strip()
    return report or "Report"


def build_compose_filename(
    callsign: str,
    state: str,
    priority: str,
    when_utc: dt.datetime,
    report_title: str,
    *,
    extension: str = ".k2s",
    filename_policy: Mapping[str, object] | FastLightFilenamePolicy | None = None,
    operating_group: str = "",
) -> str:
    policy = normalize_fastlight_filename_policy(filename_policy, group_name=operating_group)
    call = sanitize_filename_component(str(callsign or "").strip().upper(), replacement="")
    state_norm = sanitize_filename_component(str(state or "").strip().upper(), replacement="")
    priority_norm = sanitize_filename_component(str(priority or "").strip().upper(), replacement="")
    if priority_norm not in {"RR", "PP"}:
        priority_norm = "RR"
    title = sanitize_report_name(report_title)
    ext = str(extension or policy.unsigned_extension or ".k2s").strip()
    if not ext.startswith("."):
        ext = f".{ext}"
    sep = policy.delimiter or "-"
    return sep.join(
        [
            call or "CALLSIGN",
            state_norm or "STATE",
            priority_norm,
            format_compose_filename_stamp(when_utc),
            title,
        ]
    ) + ext


def build_signed_filename(
    name: str,
    *,
    filename_policy: Mapping[str, object] | FastLightFilenamePolicy | None = None,
    operating_group: str = "",
) -> str:
    policy = normalize_fastlight_filename_policy(filename_policy, group_name=operating_group)
    text = str(name or "").strip()
    lower = text.lower()
    for suffix in (".k2s", ".b2s"):
        if lower.endswith(suffix):
            return f"{text[:-len(suffix)]}{policy.signed_marker}{text[-len(suffix):]}"
    stem, suffix = os.path.splitext(text)
    return f"{stem}{policy.signed_marker}{suffix}"


def _flmsg_field_length(value: str) -> int:
    return len(str(value or "").encode("utf-8"))


def serialize_custom_form_message(
    template_name: str,
    field_pairs: Sequence[Tuple[str, str]],
    *,
    callsign: str,
    created_utc: dt.datetime,
    edited_utc: Optional[dt.datetime] = None,
    flmsg_version: str = NBEMS_FLMSG_VERSION,
) -> str:
    created_stamp = format_flmsg_header_timestamp(created_utc)
    edited_stamp = format_flmsg_header_timestamp(edited_utc or created_utc)
    callsign_txt = str(callsign or "").strip().upper() or "UNKNOWN"
    hdr_fm = f"{callsign_txt} {created_stamp}"
    hdr_ed = f"{callsign_txt} {edited_stamp}"
    payload_lines = [f"CUSTOM_FORM,{Path(str(template_name or '')).name}"]
    for key, value in field_pairs:
        field_key = str(key or "").strip().upper()
        if not field_key:
            continue
        payload_lines.append(f"{field_key},{_normalize_field_value(value)}")
    mg_value = "\n".join(payload_lines) + "\n"
    header_lines = [
        f"<flmsg>{flmsg_version}",
        f":hdr_fm:{_flmsg_field_length(hdr_fm)} ",
        hdr_fm,
        f":hdr_ed:{_flmsg_field_length(hdr_ed)} ",
        hdr_ed,
        "<customform>",
    ]
    return "\n".join(header_lines) + "\n" + f":mg:{_flmsg_field_length(mg_value)} {mg_value}"


def serialize_standard_blank_message(
    *,
    callsign: str,
    created_utc: dt.datetime,
    edited_utc: Optional[dt.datetime] = None,
    subject: str = "",
    message: str = "",
    to_name: str = "",
    from_name: str = "",
    precedence: str = "",
    dtg: str = "",
    flmsg_version: str = NBEMS_FLMSG_VERSION,
) -> str:
    created_stamp = format_flmsg_header_timestamp(created_utc)
    edited_stamp = format_flmsg_header_timestamp(edited_utc or created_utc)
    callsign_txt = str(callsign or "").strip().upper() or "UNKNOWN"
    hdr_fm = f"{callsign_txt} {created_stamp}"
    hdr_ed = f"{callsign_txt} {edited_stamp}"
    body_lines: List[str] = []
    subject_txt = str(subject or "").strip()
    if subject_txt:
        body_lines.append(subject_txt)
    if dtg:
        body_lines.append(f"DTG: {str(dtg).strip()}")
    if to_name:
        body_lines.append(f"TO: {str(to_name).strip()}")
    if from_name:
        body_lines.append(f"FROM: {str(from_name).strip()}")
    if precedence:
        body_lines.append(f"PREC: {str(precedence).strip().upper()}")
    if body_lines and message:
        body_lines.append("")
    if message:
        body_lines.append(str(message).replace("\r\n", "\n").replace("\r", "\n").strip())
    mg_value = "\n".join(body_lines).strip()
    lines = [
        f"<flmsg>{flmsg_version}",
        f":hdr_fm:{_flmsg_field_length(hdr_fm)} ",
        hdr_fm,
        f":hdr_ed:{_flmsg_field_length(hdr_ed)} ",
        hdr_ed,
        "<blankform>",
        f":mg:{_flmsg_field_length(mg_value)} {mg_value}",
    ]
    return "\n".join(lines) + "\n"


def suggest_field_value(
    field_key: str,
    label: str,
    *,
    description: str = "",
    placeholder: str = "",
    options: Sequence[ComposeFieldOption] = (),
    family_key: str = "",
    template_name: str = "",
    template_title: str = "",
    menu_item: str = "",
    callsign: str = "",
    state: str = "",
    grid: str = "",
    zulu_timestamp: str = "",
    report_title: str = "",
    priority_code: str = "",
) -> str:
    key_txt = str(field_key or "").strip().upper()
    label_txt = str(label or "").strip().upper()
    desc_txt = str(description or "").strip().upper()
    placeholder_txt = str(placeholder or "").strip().upper()
    combined_txt = " ".join(part for part in (label_txt, desc_txt, placeholder_txt) if part)
    label_words = {word for word in re.split(r"[^A-Z0-9]+", combined_txt) if word}
    recipient_default = _infer_form_recipient(
        family_key=family_key,
        template_name=template_name,
        template_title=template_title,
        menu_item=menu_item,
    )

    if {"TO", "RECIPIENT"}.intersection(label_words) and recipient_default:
        matched = _match_option_value(options, [recipient_default])
        return matched or (recipient_default if not options else "")
    if "GRID" in label_words and grid:
        return str(grid).strip().upper()
    if {"STATE", "ST"}.intersection(label_words) and state:
        return str(state).strip().upper()
    if "SUBJECT" in label_words and report_title:
        return str(report_title).strip()
    if (
        {"FROM", "SENDER"}.intersection(label_words)
        or ({"CALLSIGN", "CALL"}.intersection(label_words) and not {"TO", "RECIPIENT"}.intersection(label_words))
    ) and callsign:
        return str(callsign).strip().upper()
    if "PRECEDENCE" in label_words:
        priority_norm = str(priority_code or "").strip().upper()
        if priority_norm == "PP":
            matched = _match_option_value(options, ["P", "PRIORITY"])
            return matched or "Priority"
        matched = _match_option_value(options, ["R", "ROUTINE"])
        return matched or "Routine"
    if {"EXPIRATION", "EXPIRES", "EXPIRE", "EXPIRY"}.intersection(label_words):
        return ""
    if {"UTC", "ZULU"}.intersection(label_words) or "DTG" in label_words or ("DATE" in label_words and "TIME" in label_words):
        return str(zulu_timestamp or "").strip()
    if key_txt in {"L02", "L03", "L04", "L07"} and "DTG" in label_words and zulu_timestamp:
        return str(zulu_timestamp).strip()
    if key_txt in {"L02", "L03"} and callsign and {"FROM", "SENDER"}.intersection(label_words):
        return str(callsign).strip().upper()
    if key_txt in {"L09"} and state:
        return str(state).strip().upper()
    if key_txt in {"L11"} and grid:
        return str(grid).strip().upper()
    if key_txt in {"L14"} and zulu_timestamp:
        return str(zulu_timestamp).strip()
    selected = _selected_option_value(options)
    if selected:
        return selected
    return ""


def split_varac_bbs_safe_suffix(name: str) -> Tuple[str, str]:
    text = str(name or "")
    lower = text.lower()
    for suffix in VARAC_BBS_SAFE_SUFFIXES:
        if lower.endswith(suffix):
            return text[:-len(suffix)], text[-len(suffix):]
    stem, suffix = os.path.splitext(text)
    return stem, suffix


def safe_varac_bbs_filename(name: str, *, max_len: int = 180) -> str:
    text = unicodedata.normalize("NFKC", str(name or ""))
    cleaned_chars: List[str] = []
    for ch in text:
        if ch.isspace():
            cleaned_chars.append(" ")
        elif ord(ch) < 32 or ord(ch) == 127 or ch in '/\\:*?"<>|':
            cleaned_chars.append("_")
        else:
            cleaned_chars.append(ch)
    text = re.sub(r"\s+", " ", "".join(cleaned_chars)).strip().rstrip(".")
    stem, suffix = split_varac_bbs_safe_suffix(text)
    stem = stem.strip().rstrip(".")
    if not stem:
        stem = "message"
    suffix = suffix.strip().rstrip(".")
    candidate = f"{stem}{suffix}" if suffix else stem
    candidate = candidate.strip().rstrip(".")
    if not candidate:
        candidate = "message"
    if len(candidate) <= max_len:
        return candidate
    stem, suffix = split_varac_bbs_safe_suffix(candidate)
    suffix_len = len(suffix)
    room = max(1, int(max_len) - suffix_len)
    return f"{stem[:room].rstrip() or 'message'}{suffix}"


def unique_destination(dst: Path) -> Optional[Path]:
    if not dst.exists():
        return dst
    stem, suffix = os.path.splitext(dst.name)
    if suffix.lower() in {".sig", ".asc", ".gpg"} and "." in stem:
        base_stem, base_suffix = os.path.splitext(stem)
        stem = f"{base_stem}{base_suffix}"
        suffix = suffix
    for attempt in range(2, 1000):
        candidate = dst.with_name(f"{stem}-{attempt}{suffix}")
        if not candidate.exists():
            return candidate
    return None


def plan_compose_destinations(
    base_filename: str,
    *,
    send_target: str,
    varac_target: str,
    flmsg_dir: str = "",
    flamp_dir: str = "",
    varac_outbox_dir: str = "",
    varac_bbs_dir: str = "",
    sign_flamp_copy: bool = False,
    filename_policy: Mapping[str, object] | FastLightFilenamePolicy | None = None,
    operating_group: str = "",
) -> List[ComposeDestinationPlan]:
    send_mode = str(send_target or "").strip().lower()
    varac_mode = str(varac_target or "").strip().lower()
    requested: List[Tuple[str, str, str, str, bool]] = []
    if send_mode in {"flmsg", "both"}:
        requested.append(("flmsg", "FLMsg", flmsg_dir, base_filename, False))
    if send_mode in {"flamp", "both"}:
        flamp_name = (
            build_signed_filename(
                base_filename,
                filename_policy=filename_policy,
                operating_group=operating_group,
            )
            if sign_flamp_copy
            else base_filename
        )
        requested.append(("flamp", "FLAmp", flamp_dir, flamp_name, False))
    if varac_mode in {"outbox", "both"}:
        requested.append(("varac_outbox", "VarAC Outbox", varac_outbox_dir, base_filename, False))
    if varac_mode in {"bbs", "both"}:
        requested.append(("varac_bbs", "VarAC BBS", varac_bbs_dir, base_filename, True))

    plans: List[ComposeDestinationPlan] = []
    for key, label, directory_txt, name, use_bbs_safe_name in requested:
        directory = str(directory_txt or "").strip()
        if not directory:
            plans.append(
                ComposeDestinationPlan(
                    key=key,
                    label=label,
                    requested=True,
                    ready=False,
                    directory="",
                    path="",
                    note=f"{label} directory is not configured.",
                )
            )
            continue
        path_dir = Path(directory).expanduser()
        if not path_dir.exists() or not path_dir.is_dir():
            plans.append(
                ComposeDestinationPlan(
                    key=key,
                    label=label,
                    requested=True,
                    ready=False,
                    directory=str(path_dir),
                    path="",
                    note=f"{label} directory is missing.",
                )
            )
            continue
        filename = safe_varac_bbs_filename(name) if use_bbs_safe_name else name
        base_path = path_dir / filename
        resolved = unique_destination(base_path)
        if resolved is None:
            plans.append(
                ComposeDestinationPlan(
                    key=key,
                    label=label,
                    requested=True,
                    ready=False,
                    directory=str(path_dir),
                    path="",
                    note=f"Could not create a unique {label} filename.",
                )
            )
            continue
        note = ""
        if resolved.name != base_path.name:
            note = f"Will stage as {resolved.name}"
        elif use_bbs_safe_name and filename != name:
            note = f"Will stage as {resolved.name}"
        plans.append(
            ComposeDestinationPlan(
                key=key,
                label=label,
                requested=True,
                ready=True,
                directory=str(path_dir),
                path=str(resolved),
                note=note,
            )
        )
    return plans


def _coerce_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _normalize_field_value(value: str) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text.replace("\n", "\\n")
