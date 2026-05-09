from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from freqinout.core.varac_bbs_vault import DEFAULT_LOCATION_ID, load_vault_locations, load_vault_runtime_state


@dataclass(frozen=True)
class BbsLocationInventory:
    id: str
    name: str
    alias: str
    source_dir: str
    enabled: bool
    exists: bool
    file_count: int
    due_now_count: int
    due_soon_count: int
    subfolder_count: int
    is_default: bool = False
    is_current: bool = False


@dataclass(frozen=True)
class BbsInventory:
    bbs_enabled: bool
    live_dir: str
    live_exists: bool
    live_file_count: int
    live_due_now_count: int
    live_due_soon_count: int
    live_subfolder_count: int
    archive_enabled: bool
    archive_days: int
    vault_enabled: bool
    default_location_id: str
    current_location_id: str
    locations: Sequence[BbsLocationInventory]

    @property
    def enabled_location_count(self) -> int:
        return sum(1 for loc in self.locations if loc.enabled)

    @property
    def total_location_count(self) -> int:
        return len(self.locations)

    @property
    def managed_file_count(self) -> int:
        return sum(loc.file_count for loc in self.locations if loc.enabled)

    @property
    def managed_due_now_count(self) -> int:
        return sum(loc.due_now_count for loc in self.locations if loc.enabled)

    @property
    def managed_due_soon_count(self) -> int:
        return sum(loc.due_soon_count for loc in self.locations if loc.enabled)


def _truthy(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _int_setting(value: object, default: int) -> int:
    try:
        return max(1, int(value or default))
    except Exception:
        return default


def _allowed_suffixes(allowed_exts: Optional[Iterable[str]]) -> set[str]:
    return {
        str(ext or "").strip().lower()
        for ext in (allowed_exts or [])
        if str(ext or "").strip()
    }


def _count_files(
    root_txt: object,
    *,
    allowed_exts: Optional[Iterable[str]],
    recursive: bool,
    now_ts: float,
    archive_days: int,
) -> tuple[bool, int, int, int, int]:
    root = Path(str(root_txt or "").strip()) if str(root_txt or "").strip() else None
    if root is None or not root.exists() or not root.is_dir():
        return False, 0, 0, 0, 0
    allowed = _allowed_suffixes(allowed_exts)
    cutoff_ts = now_ts - (float(archive_days) * 86400.0)
    soon_lower_ts = now_ts - (float(max(0, archive_days - 1)) * 86400.0)
    file_count = 0
    due_now_count = 0
    due_soon_count = 0
    subfolder_count = 0
    try:
        iterator = root.rglob("*") if recursive else root.iterdir()
        for child in iterator:
            try:
                if child.is_dir():
                    if child != root:
                        subfolder_count += 1
                    continue
                if not child.is_file():
                    continue
                if allowed and child.suffix.lower() not in allowed:
                    continue
                file_count += 1
                try:
                    mtime = float(child.stat().st_mtime)
                except OSError:
                    continue
                if mtime <= cutoff_ts:
                    due_now_count += 1
                elif mtime <= soon_lower_ts:
                    due_soon_count += 1
            except OSError:
                continue
    except OSError:
        return True, file_count, due_now_count, due_soon_count, subfolder_count
    return True, file_count, due_now_count, due_soon_count, subfolder_count


def build_bbs_inventory(settings, *, allowed_exts: Optional[Iterable[str]] = None, now_ts: Optional[float] = None) -> BbsInventory:
    now_val = float(now_ts if now_ts is not None else time.time())
    archive_days = _int_setting(settings.get("varac_bbs_auto_archive_days", 14), 14)
    archive_enabled = _truthy(settings.get("varac_bbs_auto_archive_enabled", False), False)
    bbs_enabled = _truthy(settings.get("varac_bbs_enabled", False), False)
    live_dir = str(settings.get("varac_bbs_dir", "") or "").strip()
    live_exists, live_count, live_due_now, live_due_soon, live_subfolders = _count_files(
        live_dir,
        allowed_exts=allowed_exts,
        recursive=False,
        now_ts=now_val,
        archive_days=archive_days,
    )
    vault_enabled = _truthy(settings.get("varac_bbs_vault_enabled", False), False)
    default_location_id = str(settings.get("varac_bbs_vault_default_location_id", DEFAULT_LOCATION_ID) or DEFAULT_LOCATION_ID).strip() or DEFAULT_LOCATION_ID
    runtime_state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    current_location_id = str(runtime_state.current_location_id or default_location_id).strip() or default_location_id
    locations: List[BbsLocationInventory] = []
    for loc in load_vault_locations(settings.get("varac_bbs_vault_locations_v1", [])):
        exists, count, due_now, due_soon, subfolders = _count_files(
            loc.source_dir,
            allowed_exts=allowed_exts,
            recursive=True,
            now_ts=now_val,
            archive_days=archive_days,
        )
        locations.append(
            BbsLocationInventory(
                id=loc.id,
                name=loc.name,
                alias=loc.alias,
                source_dir=loc.source_dir,
                enabled=bool(loc.enabled),
                exists=exists,
                file_count=count,
                due_now_count=due_now,
                due_soon_count=due_soon,
                subfolder_count=subfolders,
                is_default=loc.id == default_location_id,
                is_current=loc.id == current_location_id,
            )
        )
    return BbsInventory(
        bbs_enabled=bbs_enabled,
        live_dir=live_dir,
        live_exists=live_exists,
        live_file_count=live_count,
        live_due_now_count=live_due_now,
        live_due_soon_count=live_due_soon,
        live_subfolder_count=live_subfolders,
        archive_enabled=archive_enabled,
        archive_days=archive_days,
        vault_enabled=vault_enabled,
        default_location_id=default_location_id,
        current_location_id=current_location_id,
        locations=tuple(locations),
    )


def format_bbs_inventory_detail(inventory: BbsInventory, *, max_locations: int = 3) -> str:
    if not inventory.bbs_enabled:
        return "Disabled"
    if not inventory.live_dir:
        return "Not configured"
    if not inventory.live_exists:
        return "Missing directory"
    parts: List[str] = []
    if inventory.archive_enabled:
        parts.append(f"Due now: {inventory.live_due_now_count}")
        parts.append(f"Due soon: {inventory.live_due_soon_count}")
    else:
        parts.append("Archive off")
    if inventory.vault_enabled:
        parts.append(f"Locations: {inventory.enabled_location_count} enabled / {inventory.total_location_count} total")
        enabled_locations = [loc for loc in inventory.locations if loc.enabled]
        for loc in enabled_locations[:max(0, int(max_locations))]:
            marker = " *" if loc.is_current else ""
            missing = " missing" if not loc.exists else ""
            parts.append(f"{loc.name}{marker}: {loc.file_count}{missing}")
        extra = len(enabled_locations) - max(0, int(max_locations))
        if extra > 0:
            parts.append(f"+{extra} more")
    return " | ".join(parts) if parts else "-"
