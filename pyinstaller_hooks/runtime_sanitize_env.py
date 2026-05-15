from __future__ import annotations

import os
import sys


def _sanitize_frozen_runtime_env() -> None:
    if os.environ.get("FREQINOUT_ALLOW_EXTERNAL_RUNTIME_ENV") == "1":
        return

    removed: list[str] = []
    for key in (
        "QT_PLUGIN_PATH",
        "QT_QPA_PLATFORM_PLUGIN_PATH",
        "QML2_IMPORT_PATH",
        "QML_IMPORT_PATH",
        "QTWEBENGINEPROCESS_PATH",
        "PYTHONHOME",
        "PYTHONPATH",
    ):
        if os.environ.pop(key, None):
            removed.append(key)

    if removed:
        os.environ["FREQINOUT_SANITIZED_ENV_VARS"] = ",".join(sorted(removed))

    if sys.platform == "win32":
        os.environ.setdefault("QT_OPENGL", "software")
        os.environ.setdefault("QT_QUICK_BACKEND", "software")
        existing_flags = os.environ.get("QTWEBENGINE_CHROMIUM_FLAGS", "").strip()
        disable_gpu = "--disable-gpu"
        if disable_gpu not in existing_flags.split():
            os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = (
                f"{existing_flags} {disable_gpu}".strip()
            )


_sanitize_frozen_runtime_env()
