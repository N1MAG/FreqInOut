from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from pathlib import Path
from typing import Iterable, Mapping, Optional

from freqinout.core.logger import log


@dataclass(frozen=True)
class VarACCallsignTagSyncResult:
    path: Path
    changed: bool
    managed_count: int
    added: int
    updated: int
    removed: int
    deduplicated: int
    preserved_unmanaged: int


@dataclass(frozen=True)
class _ManagedEntry:
    callsign: str
    name: str
    state: str


def _normalize_callsign(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_name(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_state(value: object) -> str:
    return str(value or "").strip().upper()


def resolve_varac_callsign_tags_path(settings) -> Optional[Path]:
    raw_install = str(settings.get("varac_path", "") or "").strip()
    if not raw_install:
        return None
    install_path = Path(raw_install).expanduser()
    is_executable_like = install_path.suffix.lower() in {".exe", ".bat", ".cmd", ".ps1"}
    if install_path.exists():
        folder = install_path.parent if install_path.is_file() else install_path
    else:
        folder = install_path.parent if is_executable_like else install_path
    return folder / "VarAC_callsign_tags.conf"


def _format_managed_entry(entry: _ManagedEntry) -> str:
    return f'"{entry.callsign} / {entry.name} / {entry.state}"'


def _parse_managed_entry(line: str) -> Optional[_ManagedEntry]:
    stripped = str(line or "").strip()
    if not stripped:
        return None
    if stripped.startswith('"') and stripped.endswith('"') and len(stripped) >= 2:
        stripped = stripped[1:-1].strip()
    parts = [part.strip() for part in stripped.split("/")]
    if len(parts) != 3:
        return None
    callsign = _normalize_callsign(parts[0])
    name = _normalize_name(parts[1])
    state = _normalize_state(parts[2])
    if not callsign or not name or not state:
        return None
    return _ManagedEntry(callsign=callsign, name=name, state=state)


def _build_managed_entries(rows: Iterable[Mapping[str, object]]) -> dict[str, _ManagedEntry]:
    entries: dict[str, _ManagedEntry] = {}
    for row in rows:
        callsign = _normalize_callsign(row.get("callsign"))
        name = _normalize_name(row.get("name"))
        state = _normalize_state(row.get("state"))
        if not callsign or not name or not state:
            continue
        entries[callsign] = _ManagedEntry(callsign=callsign, name=name, state=state)
    return entries


def reconcile_varac_callsign_tags(
    existing_lines: Iterable[str],
    operator_rows: Iterable[Mapping[str, object]],
) -> tuple[list[str], VarACCallsignTagSyncResult]:
    desired = _build_managed_entries(operator_rows)
    output_lines: list[str] = []
    used_callsigns: set[str] = set()
    seen_existing: set[str] = set()
    added = 0
    updated = 0
    removed = 0
    deduplicated = 0
    preserved_unmanaged = 0

    existing_list = list(existing_lines)
    for line in existing_list:
        parsed = _parse_managed_entry(line)
        if parsed is None:
            output_lines.append(line)
            preserved_unmanaged += 1
            continue
        if parsed.callsign in seen_existing:
            deduplicated += 1
            continue
        seen_existing.add(parsed.callsign)
        desired_entry = desired.get(parsed.callsign)
        if desired_entry is None:
            removed += 1
            continue
        if desired_entry != parsed:
            updated += 1
        output_lines.append(_format_managed_entry(desired_entry))
        used_callsigns.add(parsed.callsign)

    for callsign in sorted(desired.keys()):
        if callsign in used_callsigns:
            continue
        output_lines.append(_format_managed_entry(desired[callsign]))
        added += 1

    new_text = "\n".join(output_lines).rstrip()
    old_text = "\n".join(existing_list).rstrip()
    result = VarACCallsignTagSyncResult(
        path=Path("."),
        changed=new_text != old_text,
        managed_count=len(desired),
        added=added,
        updated=updated,
        removed=removed,
        deduplicated=deduplicated,
        preserved_unmanaged=preserved_unmanaged,
    )
    return output_lines, result


def sync_varac_callsign_tags_file(
    file_path: Path,
    operator_rows: Iterable[Mapping[str, object]],
) -> VarACCallsignTagSyncResult:
    file_path = Path(file_path)
    parent = file_path.parent
    if not parent.exists():
        raise FileNotFoundError(f"VarAC install folder not found: {parent}")

    existing_lines: list[str] = []
    if file_path.exists():
        existing_lines = file_path.read_text(encoding="utf-8").splitlines()

    output_lines, raw_result = reconcile_varac_callsign_tags(existing_lines, operator_rows)
    result = VarACCallsignTagSyncResult(
        path=file_path,
        changed=raw_result.changed,
        managed_count=raw_result.managed_count,
        added=raw_result.added,
        updated=raw_result.updated,
        removed=raw_result.removed,
        deduplicated=raw_result.deduplicated,
        preserved_unmanaged=raw_result.preserved_unmanaged,
    )
    if result.changed or not file_path.exists():
        text = "\n".join(output_lines).rstrip()
        if text:
            text += "\n"
        file_path.write_text(text, encoding="utf-8")
        log.info(
            "varac_callsign_tags: synced %s (managed=%s added=%s updated=%s removed=%s deduped=%s)",
            file_path,
            result.managed_count,
            result.added,
            result.updated,
            result.removed,
            result.deduplicated,
        )
    return result


def sync_varac_callsign_tags_from_db(db_path: Path, settings) -> VarACCallsignTagSyncResult:
    target = resolve_varac_callsign_tags_path(settings)
    if target is None:
        raise ValueError("VarAC Install Folder is not configured.")
    conn = sqlite3.connect(str(db_path))
    try:
        rows = [
            {"callsign": callsign, "name": name, "state": state}
            for callsign, name, state in conn.execute(
                """
                SELECT callsign, name, state
                FROM operator_checkins
                ORDER BY callsign COLLATE NOCASE
                """
            ).fetchall()
        ]
    finally:
        conn.close()
    return sync_varac_callsign_tags_file(target, rows)
