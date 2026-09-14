"""Preserve Qt's bundled QML and plugin search roots in frozen builds."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _prepend_existing(env_name: str, path: Path) -> None:
    if not path.exists():
        return
    current = [item for item in os.environ.get(env_name, "").split(os.pathsep) if item]
    rendered = str(path)
    if rendered not in current:
        current.insert(0, rendered)
    os.environ[env_name] = os.pathsep.join(current)


bundle_root = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
qt_root = bundle_root / "PySide6" / "Qt"
_prepend_existing("QML2_IMPORT_PATH", qt_root / "qml")
_prepend_existing("QT_PLUGIN_PATH", qt_root / "plugins")
