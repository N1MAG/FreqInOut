from __future__ import annotations

from datetime import datetime
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import unquote

_MACRO_HEADER_RE = re.compile(r"^\s*//\s*Macro\s*#\s*(\d+)\s*$")
_MACRO_META_RE = re.compile(r"^\s*/\$\s*(\d+)\s*(.*)$")
_STRUCTURED_FILE_REF_RE = re.compile(r"<FILE:\s*(?P<path>[^>]+?)\s*>", re.IGNORECASE)
_PATH_REF_RE = re.compile(
    r"(?<!\w)(?:[A-Za-z]:\\[^\s<>\"']+\.[A-Za-z0-9]{1,8}|/[^\s<>\"']+\.[A-Za-z0-9]{1,8})"
)
_TRAILING_REF_PUNCTUATION = ".,;:)]}>'\""
_LOCAL_POSIX_ROOTS = {
    "Applications",
    "Library",
    "Network",
    "System",
    "Users",
    "Volumes",
    "bin",
    "etc",
    "home",
    "opt",
    "private",
    "tmp",
    "usr",
    "var",
}


@dataclass(slots=True)
class DetectedMacro:
    macro_index: int
    macro_id: str
    macro_label: str
    raw_text: str
    detected_files: List[str]
    review_files: List[str]
    confidence: str

    def as_dict(self) -> Dict[str, object]:
        return {
            "macro_index": self.macro_index,
            "macro_id": self.macro_id,
            "macro_label": self.macro_label,
            "raw_text": self.raw_text,
            "detected_files": list(self.detected_files),
            "review_files": list(self.review_files),
            "confidence": self.confidence,
        }


@dataclass(slots=True)
class MacroProfileScan:
    profile_path: str
    profile_name: str
    last_scanned_mtime: Optional[float]
    file_size: Optional[int]
    detected_macros: List[DetectedMacro]

    def as_dict(self) -> Dict[str, object]:
        return {
            "profile_path": self.profile_path,
            "profile_name": self.profile_name,
            "last_scanned_mtime": self.last_scanned_mtime,
            "file_size": self.file_size,
            "detected_macros": [macro.as_dict() for macro in self.detected_macros],
        }


def _normalize_path_value(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"(?:\\n|\\r)+$", "", text)
    text = unquote(text.rstrip(_TRAILING_REF_PUNCTUATION).strip())
    if text.lower().startswith("file://"):
        text = text[7:]
        if re.match(r"^[A-Za-z]:[\\/]", text):
            return text
        if not text.startswith("/"):
            text = "/" + text
    if text.startswith("//") and not text.startswith("///"):
        first_segment = text.lstrip("/").split("/", 1)[0]
        if first_segment in _LOCAL_POSIX_ROOTS:
            text = "/" + text.lstrip("/")
    elif text.startswith("///"):
        text = "/" + text.lstrip("/")
    return text


def _dedupe_preserve(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _extract_file_references(raw_text: str) -> tuple[List[str], List[str], str]:
    structured = [
        _normalize_path_value(match.group("path"))
        for match in _STRUCTURED_FILE_REF_RE.finditer(raw_text or "")
    ]
    structured = _dedupe_preserve([path for path in structured if path])

    fallback = [
        _normalize_path_value(match.group(0))
        for match in _PATH_REF_RE.finditer(raw_text or "")
    ]
    fallback = _dedupe_preserve([path for path in fallback if path and path not in structured])

    if structured:
        return structured, fallback, "high"
    if fallback:
        return [], fallback, "review"

    return [], [], "low"


def _macro_identity_from_meta_line(header_index: int, meta_line: str) -> tuple[int, str, str]:
    macro_index = header_index
    macro_id = f"slot_{header_index + 1:02d}"
    macro_label = f"Macro {header_index + 1}"

    meta_match = _MACRO_META_RE.match(meta_line or "")
    if not meta_match:
        return macro_index, macro_id, macro_label

    try:
        macro_index = int(meta_match.group(1))
    except Exception:
        macro_index = header_index
    macro_id = f"slot_{macro_index + 1:02d}"
    label = meta_match.group(2).strip()
    if label:
        macro_label = label
    return macro_index, macro_id, macro_label


def _macro_id_to_index(macro_id: object) -> Optional[int]:
    text = str(macro_id or "").strip().casefold()
    match = re.fullmatch(r"slot_(\d{1,2})", text)
    if not match:
        return None
    try:
        return max(0, int(match.group(1)) - 1)
    except Exception:
        return None


def _path_compare_key(path: object) -> str:
    return _normalize_path_value(str(path or "")).replace("\\", "/").rstrip("/").casefold()


def rewrite_macro_file_reference_text(
    text: str,
    *,
    macro_id: str,
    old_path: str,
    new_path: str,
) -> tuple[str, int]:
    """Rewrite one structured <FILE:...> reference inside one macro slot."""
    target_index = _macro_id_to_index(macro_id)
    old_key = _path_compare_key(old_path)
    replacement = str(new_path or "").strip()
    if target_index is None or not old_key or not replacement:
        return text, 0

    lines = (text or "").splitlines(keepends=True)
    output: List[str] = []
    replacements = 0
    i = 0

    while i < len(lines):
        header_match = _MACRO_HEADER_RE.match(lines[i])
        if not header_match:
            output.append(lines[i])
            i += 1
            continue

        start = i
        header_index = int(header_match.group(1))
        j = i + 1
        while j < len(lines) and not _MACRO_HEADER_RE.match(lines[j]):
            j += 1

        block_lines = lines[start:j]
        block_index = header_index
        for candidate in block_lines[1:]:
            meta_match = _MACRO_META_RE.match(candidate)
            if meta_match:
                try:
                    block_index = int(meta_match.group(1))
                except Exception:
                    block_index = header_index
                break
            if candidate.strip():
                break

        block_text = "".join(block_lines)
        if block_index != target_index:
            output.append(block_text)
            i = j
            continue

        def replace_file_ref(match: re.Match[str]) -> str:
            nonlocal replacements
            found_path = _normalize_path_value(match.group("path"))
            if _path_compare_key(found_path) != old_key:
                return match.group(0)
            replacements += 1
            return f"<FILE:{replacement}>"

        output.append(_STRUCTURED_FILE_REF_RE.sub(replace_file_ref, block_text))
        i = j

    return "".join(output), replacements


def rewrite_macro_profile_file_reference(
    profile_path: str,
    *,
    macro_id: str,
    old_path: str,
    new_path: str,
) -> Dict[str, object]:
    path = Path(str(profile_path or "").strip()).expanduser()
    if not path.exists():
        return {"ok": False, "error": "profile_not_found", "replacements": 0, "backup_path": ""}

    original = path.read_text(encoding="utf-8", errors="ignore")
    updated, replacements = rewrite_macro_file_reference_text(
        original,
        macro_id=macro_id,
        old_path=old_path,
        new_path=new_path,
    )
    if replacements <= 0 or updated == original:
        return {"ok": False, "error": "reference_not_found", "replacements": 0, "backup_path": ""}

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = path.with_name(f"{path.name}.fio-backup-{timestamp}")
    shutil.copy2(path, backup_path)
    path.write_text(updated, encoding="utf-8")
    return {"ok": True, "error": "", "replacements": replacements, "backup_path": str(backup_path)}


def parse_macro_profile_text(text: str, *, profile_path: str = "", profile_name: str = "") -> MacroProfileScan:
    lines = (text or "").splitlines()
    detected_macros: List[DetectedMacro] = []
    i = 0

    while i < len(lines):
        header_match = _MACRO_HEADER_RE.match(lines[i])
        if not header_match:
            i += 1
            continue

        header_index = int(header_match.group(1))
        macro_index = header_index
        macro_id = f"slot_{header_index + 1:02d}"
        macro_label = f"Macro {header_index + 1}"

        meta_line = ""
        meta_line_index: Optional[int] = None
        j = i + 1
        while j < len(lines):
            candidate = lines[j]
            if _MACRO_META_RE.match(candidate):
                meta_line = candidate
                meta_line_index = j
                break
            if candidate.strip():
                break
            j += 1

        body_start = i + 1
        if meta_line:
            macro_index, macro_id, macro_label = _macro_identity_from_meta_line(header_index, meta_line)
            if meta_line_index is not None:
                body_start = meta_line_index + 1

        body_lines: List[str] = []
        k = body_start
        while k < len(lines):
            if _MACRO_HEADER_RE.match(lines[k]):
                break
            body_lines.append(lines[k])
            k += 1

        raw_text = "\n".join(body_lines).rstrip()
        structured_files, review_files, confidence = _extract_file_references(raw_text)
        detected_macros.append(
            DetectedMacro(
                macro_index=macro_index,
                macro_id=macro_id,
                macro_label=macro_label,
                raw_text=raw_text,
                detected_files=structured_files,
                review_files=review_files,
                confidence=confidence,
            )
        )
        i = k

    canonical_path = str(Path(profile_path).expanduser()) if profile_path else ""
    display_name = profile_name or (Path(canonical_path).stem if canonical_path else "")
    return MacroProfileScan(
        profile_path=canonical_path,
        profile_name=display_name,
        last_scanned_mtime=None,
        file_size=None,
        detected_macros=detected_macros,
    )


def scan_macro_profile(path: str) -> Dict[str, object]:
    canonical = str(Path(path).expanduser()) if path else ""
    p = Path(canonical) if canonical else None
    if not p or not p.exists():
        return {
            "profile_path": canonical,
            "profile_name": p.stem if p else "",
            "last_scanned_mtime": None,
            "file_size": None,
            "detected_macros": [],
            "scan_error": "file_not_found",
        }

    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return {
            "profile_path": canonical,
            "profile_name": p.stem or p.name,
            "last_scanned_mtime": None,
            "file_size": None,
            "detected_macros": [],
            "scan_error": f"read_error:{exc}",
        }

    scan = parse_macro_profile_text(text, profile_path=canonical, profile_name=p.stem or p.name)
    try:
        stat = p.stat()
        scan.last_scanned_mtime = stat.st_mtime
        scan.file_size = stat.st_size
    except Exception:
        pass
    return scan.as_dict()


def count_detected_file_references(scan_result: Dict[str, object]) -> int:
    macros = scan_result.get("detected_macros", [])
    if not isinstance(macros, list):
        return 0
    total = 0
    for macro in macros:
        if isinstance(macro, dict):
            total += len([path for path in macro.get("detected_files", []) if str(path).strip()])
    return total
