"""Isolate frozen builds from host runtimes and expose bundled Qt resources."""

from __future__ import annotations

import os
import sys
from pathlib import Path


_EXTERNAL_RUNTIME_ENV = (
    "QT_PLUGIN_PATH",
    "QT_QPA_PLATFORM_PLUGIN_PATH",
    "QML2_IMPORT_PATH",
    "QML_IMPORT_PATH",
    "QTWEBENGINEPROCESS_PATH",
    "PYTHONHOME",
    "PYTHONPATH",
)


def _sanitize_frozen_runtime_env() -> None:
    """Prevent a packaged FIO from importing another Python/Qt installation."""

    if os.environ.get("FREQINOUT_ALLOW_EXTERNAL_RUNTIME_ENV") == "1":
        return

    removed: list[str] = []
    for key in _EXTERNAL_RUNTIME_ENV:
        if os.environ.pop(key, None):
            removed.append(key)
    if removed:
        os.environ["FREQINOUT_SANITIZED_ENV_VARS"] = ",".join(sorted(removed))

    if sys.platform == "win32":
        os.environ.setdefault("QT_OPENGL", "software")
        os.environ.setdefault("QT_QUICK_BACKEND", "software")
        flags = os.environ.get("QTWEBENGINE_CHROMIUM_FLAGS", "").strip().split()
        if "--disable-gpu" not in flags:
            flags.append("--disable-gpu")
        os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = " ".join(flags)


def _prepend_existing(env_name: str, path: Path) -> None:
    if not path.exists():
        return
    current = [item for item in os.environ.get(env_name, "").split(os.pathsep) if item]
    rendered = str(path)
    if rendered not in current:
        current.insert(0, rendered)
    os.environ[env_name] = os.pathsep.join(current)


_sanitize_frozen_runtime_env()
bundle_root = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
qt_root = bundle_root / "PySide6" / "Qt"
_prepend_existing("QML2_IMPORT_PATH", qt_root / "qml")
_prepend_existing("QT_PLUGIN_PATH", qt_root / "plugins")
