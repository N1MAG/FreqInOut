"""Exact VarAC launch recipes for legacy and canonical radio identities.

VarAC's INI selector is a Windows path even when the process is hosted by
Wine.  Keep that selector as one argv element; converting an older free-form
command with ``shlex`` destroys its backslashes on POSIX systems.
"""

from __future__ import annotations

import re
from pathlib import Path, PureWindowsPath
from typing import Any, Mapping


def _platform_key(value: str) -> str:
    token = str(value or "").strip().casefold()
    if token in {"windows", "win32", "nt"}:
        return "windows"
    if token in {"linux", "linux-wine"}:
        return "linux-wine"
    if token in {"darwin", "macos", "macos-wine"}:
        return "macos-wine"
    return token


def _windows_executable(install_path: str) -> str:
    path = PureWindowsPath(str(install_path or "").strip())
    if path.name.casefold() == "varac.exe":
        return str(path)
    return str(path / "VarAC.exe")


def _wine_identity(path_value: str) -> tuple[str, str] | None:
    """Return ``(WINEPREFIX, native Windows path)`` for a Wine host path."""

    path = Path(str(path_value or "").strip()).expanduser().absolute()
    parts = path.parts
    for index, part in enumerate(parts):
        match = re.fullmatch(r"drive_([a-zA-Z])", part)
        if not match:
            continue
        prefix = str(Path(*parts[:index]))
        suffix = "\\".join(parts[index + 1 :])
        drive = match.group(1).upper()
        native = f"{drive}:\\{suffix}" if suffix else f"{drive}:\\"
        return prefix, native
    return None


def legacy_varac_structured_launch(
    node: Mapping[str, Any],
    *,
    platform_name: str,
    wine_executable: str = "wine",
) -> Mapping[str, Any] | None:
    """Project a safe structured launch recipe from one persisted VarAC node.

    This is deliberately a read-only compatibility projection.  It neither
    changes third-party files nor claims that a legacy node is FIO-managed.
    """

    install_path = str(node.get("install_path") or "").strip()
    ini_path = str(node.get("ini_path") or "").strip()
    if not install_path or not ini_path:
        return None

    platform_key = _platform_key(platform_name)
    if platform_key == "windows":
        executable = _windows_executable(install_path)
        return {
            "executable": executable,
            "launch_arguments": [ini_path],
            "working_directory": str(PureWindowsPath(executable).parent),
            "environment": {},
        }

    if platform_key not in {"linux-wine", "macos-wine"}:
        return None
    identity = _wine_identity(ini_path)
    if identity is None:
        # A legacy Wine node without a concrete drive_<letter> identity cannot
        # safely select an INI.  Never manufacture a host-root Z: projection.
        return None
    wine_prefix, native_ini = identity
    install = Path(install_path).expanduser()
    executable = install if install.name.casefold() == "varac.exe" else install / "VarAC.exe"
    return {
        "executable": str(wine_executable or "wine"),
        "launch_arguments": [str(executable), native_ini],
        "working_directory": str(executable.parent),
        "environment": {"WINEPREFIX": wine_prefix},
    }


__all__ = ["legacy_varac_structured_launch"]
