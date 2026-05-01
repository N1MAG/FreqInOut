from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

_MACRO_HEADER_RE = re.compile(r"^\s*//\s*Macro\s*#\s*(\d+)\s*$")
_MACRO_META_RE = re.compile(r"^\s*/\$\s*(\d+)\s*(.*)$")
_STRUCTURED_FILE_REF_RE = re.compile(r"<FILE:\s*(?P<path>[^>]+?)\s*>", re.IGNORECASE)
_PATH_REF_RE = re.compile(
    r"(?<!\w)(?:[A-Za-z]:\\[^\s<>\"']+\.[A-Za-z0-9]{1,8}|/[^\s<>\"']+\.[A-Za-z0-9]{1,8})"
)
_TRAILING_REF_PUNCTUATION = ".,;:)]}>'\""


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
    return text.rstrip(_TRAILING_REF_PUNCTUATION).strip()


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
