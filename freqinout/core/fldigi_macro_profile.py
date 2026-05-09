from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import re

from freqinout.core.fldigi_macro_parser import count_detected_file_references, scan_macro_profile


@dataclass(slots=True)
class MacroMapping:
    scope: str
    function: str
    custom_name: str = ""
    macro_id: str = ""
    macro_label: str = ""
    source_file: str = ""
    read_only: bool = False
    enabled: bool = False

    def as_dict(self) -> Dict[str, object]:
        return {
            "scope": self.scope,
            "function": self.function,
            "custom_name": self.custom_name,
            "macro_id": self.macro_id,
            "macro_label": self.macro_label,
            "source_file": self.source_file,
            "read_only": self.read_only,
            "enabled": self.enabled,
        }


@dataclass(slots=True)
class MacroProfileRecord:
    profile_path: str
    profile_name: str
    last_scanned_mtime: Optional[float] = None
    file_size: Optional[int] = None
    detected_macros: List[Dict[str, object]] = field(default_factory=list)
    mappings: List[Dict[str, object]] = field(default_factory=list)
    scan_error: Optional[str] = None

    def as_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "profile_path": self.profile_path,
            "profile_name": self.profile_name,
            "last_scanned_mtime": self.last_scanned_mtime,
            "file_size": self.file_size,
            "detected_macros": list(self.detected_macros),
            "mappings": list(self.mappings),
        }
        if self.scan_error:
            payload["scan_error"] = self.scan_error
        return payload


class FldigiMacroProfileStore:
    def __init__(self, settings) -> None:
        self.settings = settings

    def _store_key(self) -> str:
        return "fldigi_macro_profiles_v1"

    def _selected_key(self) -> str:
        return "fldigi_selected_macro_profile"

    def _normalize_path(self, path: str) -> str:
        text = str(path or "").strip()
        if not text:
            return ""
        try:
            return str(Path(text).expanduser().resolve())
        except Exception:
            return str(Path(text).expanduser().absolute())

    def normalize_path(self, path: str) -> str:
        return self._normalize_path(path)

    def _store(self) -> Dict[str, Dict[str, object]]:
        data = self.settings.all()
        raw_store = data.get(self._store_key(), {})
        return raw_store if isinstance(raw_store, dict) else {}

    def canonical_store(self) -> Dict[str, Dict[str, object]]:
        store = self._store()
        normalized: Dict[str, Dict[str, object]] = {}
        changed = False
        for raw_path, record in store.items():
            canonical = self._normalize_path(raw_path)
            if not canonical:
                continue
            if canonical != raw_path:
                changed = True
            normalized[canonical] = dict(record) if isinstance(record, dict) else {}
        if changed:
            self.settings.set(self._store_key(), normalized)
        return normalized

    def get_selected_profile_path(self) -> str:
        return str(self.settings.get(self._selected_key(), "") or "").strip()

    def get_record(self, path: str) -> Dict[str, object]:
        canonical = self._normalize_path(path)
        if not canonical:
            return {}
        return dict(self.canonical_store().get(canonical, {}))

    def profile_mode(self, path: Optional[str] = None) -> str:
        selected = self._normalize_path(path or self.get_selected_profile_path())
        if not selected:
            return "legacy"
        if not Path(selected).exists():
            return "legacy"
        record = self.get_record(selected)
        if not record:
            return "legacy"
        return "mapped" if self.has_enabled_mappings(record) else "legacy"

    @staticmethod
    def _mapping_is_complete(mapping: Dict[str, object]) -> bool:
        if not isinstance(mapping, dict):
            return False
        if not mapping.get("enabled"):
            return False
        if bool(mapping.get("read_only")):
            return False
        scope = str(mapping.get("scope", "") or "").strip()
        function = str(mapping.get("function", "") or "").strip()
        source_file = str(mapping.get("source_file", "") or "").strip()
        macro_id = str(mapping.get("macro_id", "") or "").strip()
        if not scope or not function:
            return False
        if not source_file and not macro_id:
            return False
        return True

    def mapping_is_complete(self, mapping: Dict[str, object]) -> bool:
        return self._mapping_is_complete(mapping)

    @staticmethod
    def mapping_snapshot(mapping: Dict[str, object]) -> Dict[str, object]:
        if not isinstance(mapping, dict):
            mapping = {}
        return {
            "confidence": str(mapping.get("confidence", "") or "").strip(),
            "scope": str(mapping.get("scope", "") or "").strip(),
            "function": str(mapping.get("function", "") or "").strip(),
            "custom_name": str(mapping.get("custom_name", "") or "").strip(),
            "macro_id": str(mapping.get("macro_id", "") or "").strip(),
            "macro_label": str(mapping.get("macro_label", "") or "").strip(),
            "source_file": str(mapping.get("source_file", "") or "").strip(),
            "read_only": bool(mapping.get("read_only", False)),
            "enabled": bool(mapping.get("enabled", False)),
        }

    def mapping_should_persist(
        self,
        mapping: Dict[str, object],
        original: Optional[Dict[str, object]] = None,
        *,
        origin: str = "discovered",
    ) -> bool:
        if origin == "saved":
            return True
        current = self.mapping_snapshot(mapping)
        baseline = self.mapping_snapshot(original or {})
        if current == baseline:
            return False
        return any(
            [
                current["scope"],
                current["function"],
                current["custom_name"],
                current["macro_id"],
                current["source_file"],
                current["enabled"],
                current["read_only"],
            ]
        )

    def has_enabled_mappings(self, record: Dict[str, object]) -> bool:
        mappings = record.get("mappings")
        if not isinstance(mappings, list):
            return False
        return any(self._mapping_is_complete(mapping) for mapping in mappings)

    def complete_mappings(self, record: Dict[str, object]) -> List[Dict[str, object]]:
        mappings = record.get("mappings")
        if not isinstance(mappings, list):
            return []
        return [mapping for mapping in mappings if self._mapping_is_complete(mapping)]

    def scan_profile(self, path: str) -> MacroProfileRecord:
        canonical = self._normalize_path(path)
        scan = scan_macro_profile(canonical)
        if canonical:
            fallback_name = Path(canonical).stem
        else:
            fallback_name = ""
        record = MacroProfileRecord(
            profile_path=str(scan.get("profile_path") or canonical),
            profile_name=str(scan.get("profile_name") or fallback_name),
            last_scanned_mtime=scan.get("last_scanned_mtime"),
            file_size=scan.get("file_size"),
            detected_macros=list(scan.get("detected_macros") or []),
            mappings=list(self.get_record(canonical).get("mappings") or []),
            scan_error=scan.get("scan_error"),
        )
        return record

    def save_scan(self, path: str) -> Dict[str, object]:
        record = self.scan_profile(path)
        canonical = self._normalize_path(path)
        store = self.canonical_store()
        existing = dict(store.get(canonical, {}))
        existing.update(record.as_dict())
        store[canonical] = existing
        self.settings.set(self._store_key(), store)
        self.settings.set(self._selected_key(), canonical)
        return existing

    def upsert_mappings(self, path: str, mappings: List[Dict[str, object]]) -> Dict[str, object]:
        canonical = self._normalize_path(path)
        store = self.canonical_store()
        record = dict(store.get(canonical, {}))
        record["mappings"] = list(mappings)
        if not record.get("profile_name"):
            record["profile_name"] = Path(canonical).stem if canonical else ""
        store[canonical] = record
        self.settings.set(self._store_key(), store)
        self.settings.set(self._selected_key(), canonical)
        return record

    @staticmethod
    def next_custom_name(mappings: List[Dict[str, object]]) -> str:
        used_numbers = set()
        for mapping in mappings or []:
            if not isinstance(mapping, dict):
                continue
            if str(mapping.get("function", "") or "").strip().upper() != "CUSTOM":
                continue
            custom_name = str(mapping.get("custom_name", "") or "").strip().upper()
            match = re.fullmatch(r"CUSTOM_(\d+)", custom_name)
            if match:
                try:
                    used_numbers.add(int(match.group(1)))
                except Exception:
                    pass
        candidate = 1
        while candidate in used_numbers:
            candidate += 1
        return f"CUSTOM_{candidate}"

    @staticmethod
    def normalize_function_name(value: str) -> str:
        return str(value or "").strip().upper()

    @staticmethod
    def summary_text(record: Dict[str, object]) -> str:
        detected_macros = record.get("detected_macros", [])
        mappings = record.get("mappings", [])
        file_refs = count_detected_file_references(record) if isinstance(record, dict) else 0
        total_macros = len(detected_macros) if isinstance(detected_macros, list) else 0
        mapping_count = 0
        if isinstance(mappings, list):
            mapping_count = len([mapping for mapping in mappings if FldigiMacroProfileStore._mapping_is_complete(mapping)])
        return f"Scanned {total_macros} macros, found {file_refs} file-backed references; {mapping_count} complete mappings saved"
