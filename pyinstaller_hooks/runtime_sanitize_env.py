from __future__ import annotations

import os


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


_sanitize_frozen_runtime_env()
