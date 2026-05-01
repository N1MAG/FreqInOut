from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from freqinout.core.logger import log


@dataclass(frozen=True)
class VaracFileActionResult:
    action: str
    source: str
    destination: str
    reason: str = ""


def _split_name_and_suffix(path: Path) -> tuple[str, str]:
    name = path.name
    suffix = "".join(path.suffixes)
    if suffix and name.lower().endswith(suffix.lower()):
        stem = name[: -len(suffix)]
    else:
        stem = path.stem
    return stem, suffix


def unique_destination(dst: Path) -> Path:
    dst = Path(dst)
    if not dst.exists():
        return dst
    stem, suffix = _split_name_and_suffix(dst)
    parent = dst.parent
    counter = 2
    while True:
        candidate = parent / f"{stem}-{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def delete_file(src: Path) -> VaracFileActionResult:
    src = Path(src)
    if not src.exists():
        return VaracFileActionResult(action="delete", source=str(src), destination="", reason="missing")
    try:
        src.unlink()
        log.info("varac_file_action: deleted unauthorized file %s", src)
        return VaracFileActionResult(action="delete", source=str(src), destination="", reason="deleted")
    except Exception as exc:
        log.error("varac_file_action: failed to delete %s: %s", src, exc)
        raise


def quarantine_file(src: Path, quarantine_dir: Path) -> VaracFileActionResult:
    src = Path(src)
    quarantine_dir = Path(quarantine_dir)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    dst = unique_destination(quarantine_dir / src.name)
    shutil.move(str(src), str(dst))
    log.info("varac_file_action: quarantined unauthorized file %s -> %s", src, dst)
    return VaracFileActionResult(action="quarantine", source=str(src), destination=str(dst), reason="quarantined")
