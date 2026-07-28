from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence, Tuple

from freqinout.core.config_paths import get_config_dir


@dataclass(frozen=True)
class ConfigBackupItem:
    original_path: str
    backup_path: str
    kind: str
    status: str
    error: str = ""


@dataclass(frozen=True)
class ConfigBackupResult:
    backup_dir: str
    reason: str
    created_at: str
    items: Tuple[ConfigBackupItem, ...]
    manifest_path: str


def create_config_backup(
    paths: Iterable[Path],
    *,
    reason: str = "pre-multirig",
    backup_root: Optional[Path] = None,
    now: Callable[[], datetime] = datetime.now,
) -> ConfigBackupResult:
    created_at = now().strftime("%Y%m%d-%H%M%S")
    safe_reason = _safe_backup_reason(reason)
    root = Path(backup_root) if backup_root is not None else get_config_dir() / "backups"
    backup_dir = _create_unique_backup_dir(root, f"{safe_reason}-{created_at}")
    resolved_root = _safe_resolve(root)

    items = []
    used_names = set()
    for raw_path in paths:
        source = Path(raw_path).expanduser()
        target_name = _unique_backup_name(source, used_names)
        target = backup_dir / target_name
        if not source.exists():
            items.append(
                ConfigBackupItem(
                    original_path=str(source),
                    backup_path="",
                    kind="missing",
                    status="missing",
                    error="Path does not exist.",
                )
            )
            continue
        try:
            if source.is_dir():
                if _same_path(source, resolved_root):
                    raise OSError("Backup root cannot be backed up into itself.")
                shutil.copytree(
                    source,
                    target,
                    symlinks=True,
                    ignore=_ignore_nested_backup_root(source, resolved_root),
                )
                kind = "directory"
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target, follow_symlinks=False)
                kind = "file"
            items.append(
                ConfigBackupItem(
                    original_path=str(source),
                    backup_path=str(target),
                    kind=kind,
                    status="backed_up",
                )
            )
        except OSError as exc:
            items.append(
                ConfigBackupItem(
                    original_path=str(source),
                    backup_path=str(target),
                    kind="directory" if source.is_dir() else "file",
                    status="failed",
                    error=str(exc),
                )
            )

    result = ConfigBackupResult(
        backup_dir=str(backup_dir),
        reason=safe_reason,
        created_at=created_at,
        items=tuple(items),
        manifest_path=str(backup_dir / "manifest.json"),
    )
    _write_manifest(result)
    return result


def _write_manifest(result: ConfigBackupResult) -> None:
    manifest = {
        "backup_dir": result.backup_dir,
        "reason": result.reason,
        "created_at": result.created_at,
        "items": [asdict(item) for item in result.items],
    }
    Path(result.manifest_path).write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def _safe_backup_reason(reason: str) -> str:
    cleaned = []
    for char in str(reason or "").strip().lower():
        if char.isalnum():
            cleaned.append(char)
        elif char in {"-", "_", " "}:
            cleaned.append("-")
    out = "".join(cleaned).strip("-")
    while "--" in out:
        out = out.replace("--", "-")
    return out or "backup"


def _create_unique_backup_dir(root: Path, base_name: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    candidate = root / base_name
    index = 2
    while True:
        try:
            candidate.mkdir()
            return candidate
        except FileExistsError:
            candidate = root / f"{base_name}-{index}"
            index += 1


def _ignore_nested_backup_root(source: Path, backup_root: Path):
    resolved_source = _safe_resolve(source)
    resolved_backup_root = _safe_resolve(backup_root)
    if not _relative_parts(resolved_backup_root, resolved_source):
        return None
    ignored_parent = resolved_backup_root.parent
    ignored_name = resolved_backup_root.name

    def ignore(current_dir: str, names: Sequence[str]):
        if _same_path(Path(current_dir), ignored_parent):
            return {ignored_name}
        return set()

    return ignore


def _relative_parts(child: Path, parent: Path) -> Tuple[str, ...]:
    try:
        return _safe_resolve(child).relative_to(_safe_resolve(parent)).parts
    except ValueError:
        return ()


def _same_path(left: Path, right: Path) -> bool:
    return _safe_resolve(left) == _safe_resolve(right)


def _safe_resolve(path: Path) -> Path:
    try:
        return Path(path).expanduser().resolve()
    except OSError:
        return Path(path).expanduser().absolute()


def _unique_backup_name(source: Path, used_names: set) -> str:
    name = source.name or "root"
    candidate = name
    index = 2
    while candidate in used_names:
        candidate = f"{source.stem}-{index}{source.suffix}"
        index += 1
    used_names.add(candidate)
    return candidate
