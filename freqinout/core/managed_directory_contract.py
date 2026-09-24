"""Typed ownership contract for directories created by managed app recipes.

The canonical launch recipe may also contain executable, INI, database, log,
and message-file paths.  Those values must never be passed to ``mkdir``.  This
module therefore accepts only the recipe's explicit ``managed_directories``
field, with a narrow compatibility projection for recipes saved before that
field existed.
"""

from __future__ import annotations

import ntpath
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


_DIRECTORY_COMPONENTS = {"flrig", "fldigi", "flmsg", "flamp", "js8call"}


def parent_directory_text(value: object) -> str:
    """Return a path's parent without confusing Windows paths on POSIX."""

    text = str(value or "").strip()
    if not text:
        return ""
    if "\\" in text and (not "/" in text or ntpath.splitdrive(text)[0]):
        return ntpath.dirname(text)
    return str(Path(text).parent)


def managed_directories_from_component(
    component: Mapping[str, Any],
    *,
    component_key: str = "",
) -> tuple[str, ...]:
    """Return only directory targets the canonical recipe authorizes FIO to create.

    New recipes carry an explicit list.  The fallback is intentionally
    app-specific so existing saved Fast Light and JS8Call recipes can be
    repaired without treating JS8Call's INI file as a directory.  VarAC uses
    its qualified transactional writer, and shared FLAmp paths remain outside
    this generic creator.
    """

    key = str(component_key or component.get("component_key") or "").strip().lower()
    if "managed_directories" in component:
        raw = component.get("managed_directories", ())
        values = raw if isinstance(raw, (list, tuple)) else ()
    elif key in {"flrig", "fldigi", "flmsg"}:
        values = (
            *tuple(component.get("configuration_roots", ()) or ()),
            *tuple(component.get("data_roots", ()) or ()),
            str(component.get("working_directory", "") or ""),
        )
    elif key == "js8call":
        configuration_roots = tuple(component.get("configuration_roots", ()) or ())
        settings_parent = parent_directory_text(configuration_roots[0]) if configuration_roots else ""
        values = (settings_parent, *tuple(component.get("data_roots", ()) or ()))
    else:
        values = ()

    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return tuple(out)


def managed_directories_from_recipe(recipe: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    """Return ``(component_key, path)`` pairs from one canonical recipe."""

    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for component in recipe.get("components", ()) or ():
        if not isinstance(component, Mapping):
            continue
        key = str(component.get("component_key", "") or "").strip().lower()
        if key not in _DIRECTORY_COMPONENTS:
            continue
        for path in managed_directories_from_component(component, component_key=key):
            normalized = os.path.normcase(os.path.normpath(os.path.expanduser(path)))
            if normalized in seen:
                continue
            seen.add(normalized)
            out.append((key, path))
    return tuple(out)


def materialize_managed_directories(paths: Sequence[object] | Iterable[object]) -> tuple[Path, ...]:
    """Create reviewed FIO-owned directories idempotently and verify each target.

    Existing directories are left untouched.  Existing files fail closed.
    Filesystem roots and the home directory itself are never valid managed
    application targets, even if a damaged database claims otherwise.
    """

    ready: list[Path] = []
    seen: set[str] = set()
    home = Path.home().expanduser().resolve(strict=False)
    for raw in paths:
        text = str(raw or "").strip()
        if not text:
            continue
        target = Path(text).expanduser()
        resolved = target.resolve(strict=False)
        key = os.path.normcase(str(resolved))
        if key in seen:
            continue
        seen.add(key)
        anchor = Path(resolved.anchor) if resolved.anchor else None
        if (anchor is not None and resolved == anchor) or resolved == home:
            raise ValueError(f"Refusing broad managed directory target: {target}")
        if target.exists():
            if not target.is_dir():
                raise FileExistsError(f"Managed directory target is an existing file: {target}")
        else:
            target.mkdir(parents=True, exist_ok=False)
        if not target.is_dir():
            raise OSError(f"Managed directory target is not a directory: {target}")
        ready.append(target)
    return tuple(ready)


__all__ = [
    "managed_directories_from_component",
    "managed_directories_from_recipe",
    "materialize_managed_directories",
    "parent_directory_text",
]
