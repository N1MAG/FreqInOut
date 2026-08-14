from __future__ import annotations

import configparser
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class CommStatGroupState:
    configured_groups: frozenset[str]
    active_groups: frozenset[str]
    unchecked_groups: frozenset[str]
    show_other_groups: bool
    db_path: Path | None = None
    config_path: Path | None = None


def load_commstat_group_state(settings: Any) -> CommStatGroupState:
    """Read CommStat's configured and active group state without importing CommStat."""

    db_path = _resolve_commstat_db_path(settings)
    configured_groups, db_active_groups = _read_commstat_groups(db_path)
    config_path = _resolve_commstat_config_path(settings, db_path)
    show_other_groups, unchecked_groups = _read_commstat_filter_config(config_path)

    if unchecked_groups:
        active_groups = configured_groups - unchecked_groups
    elif db_active_groups:
        active_groups = db_active_groups & configured_groups
    else:
        active_groups = configured_groups

    return CommStatGroupState(
        configured_groups=frozenset(configured_groups),
        active_groups=frozenset(active_groups),
        unchecked_groups=frozenset(unchecked_groups),
        show_other_groups=show_other_groups,
        db_path=db_path,
        config_path=config_path,
    )


def _resolve_commstat_db_path(settings: Any) -> Path | None:
    candidates: list[Path] = []
    for key in ("commstat3_db_path", "commstat_db_path", "commstat23_db_path", "path_commstat"):
        candidates.extend(_candidate_db_paths(_settings_get(settings, key, ""), "traffic.db3"))
    return _pick_existing_file(candidates)


def _resolve_commstat_config_path(settings: Any, db_path: Path | None) -> Path | None:
    candidates: list[Path] = []
    explicit = _settings_get(settings, "commstat_config_path", "")
    if explicit:
        candidates.extend(_candidate_config_paths(explicit))
    for key in ("path_commstat", "commstat3_db_path", "commstat_db_path", "commstat23_db_path"):
        candidates.extend(_candidate_config_paths(_settings_get(settings, key, "")))
    if db_path is not None:
        candidates.append(db_path.parent / "config.ini")
    return _pick_existing_file(candidates)


def _read_commstat_groups(db_path: Path | None) -> tuple[set[str], set[str]]:
    if db_path is None:
        return set(), set()
    try:
        uri = f"file:{db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=0.35)
        try:
            conn.execute("PRAGMA busy_timeout=350")
        except Exception:
            pass
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='groups' LIMIT 1"
            ).fetchone()
            if not table:
                return set(), set()
            cols = {str(row[1] or "").lower() for row in conn.execute("PRAGMA table_info(groups)").fetchall()}
            configured: set[str] = set()
            active: set[str] = set()
            if "is_active" in cols:
                rows = conn.execute("SELECT name, is_active FROM groups ORDER BY name").fetchall()
                for name, is_active in rows:
                    group = _normalize_group_name(name)
                    if not group:
                        continue
                    configured.add(group)
                    if bool(is_active):
                        active.add(group)
            else:
                rows = conn.execute("SELECT name FROM groups ORDER BY name").fetchall()
                configured = {_normalize_group_name(row[0]) for row in rows}
                configured.discard("")
            return configured, active
        finally:
            conn.close()
    except Exception:
        return set(), set()


def _read_commstat_filter_config(config_path: Path | None) -> tuple[bool, set[str]]:
    if config_path is None:
        return False, set()
    try:
        parser = configparser.ConfigParser()
        parser.read(config_path)
        if not parser.has_section("DIRECTEDCONFIG"):
            return False, set()
        show_other = parser.getboolean("DIRECTEDCONFIG", "show_every_group", fallback=False)
        unchecked_raw = parser.get("DIRECTEDCONFIG", "unchecked_groups", fallback="")
        unchecked = {_normalize_group_name(item) for item in unchecked_raw.split(",")}
        unchecked.discard("")
        return show_other, unchecked
    except Exception:
        return False, set()


def _candidate_db_paths(raw: object, default_db_name: str) -> Iterable[Path]:
    txt = str(raw or "").strip()
    if not txt:
        return []
    p = Path(txt).expanduser()
    out: list[Path] = []
    suffix = p.suffix.lower()
    if suffix in {".db", ".db3", ".sqlite", ".sqlite3"}:
        out.append(p)
        out.append(p.parent / default_db_name)
    elif suffix:
        out.append(p.parent / default_db_name)
    else:
        out.append(p / default_db_name)
    return out


def _candidate_config_paths(raw: object) -> Iterable[Path]:
    txt = str(raw or "").strip()
    if not txt:
        return []
    p = Path(txt).expanduser()
    if p.suffix.lower() == ".ini":
        return [p]
    if p.suffix:
        return [p.parent / "config.ini"]
    return [p / "config.ini"]


def _pick_existing_file(candidates: Iterable[Path]) -> Path | None:
    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            if resolved.exists() and resolved.is_file():
                return resolved
        except Exception:
            continue
    return None


def _settings_get(settings: Any, key: str, default: object = "") -> object:
    try:
        if hasattr(settings, "get"):
            return settings.get(key, default)
    except Exception:
        pass
    try:
        if isinstance(settings, dict):
            return settings.get(key, default)
    except Exception:
        pass
    return default


def _normalize_group_name(value: object) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("@"):
        text = text[1:]
    return " ".join(text.split())
