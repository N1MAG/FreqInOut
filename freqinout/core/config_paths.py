"""
Shared helpers for locating user-writable config/data directories.
"""
from __future__ import annotations

import os
from pathlib import Path


APP_NAME = "FreqInOut"


def get_config_dir() -> Path:
    """
    Return a user-writable config directory.
    Priority:
      1) FREQINOUT_CONFIG_DIR env var (user override)
      2) Windows: LOCALAPPDATA/APPDATA/FreqInOut
         Others: ~/.freqinout
      3) CWD/freqinout_config (fallback if creation fails)
    """
    env_cfg = os.environ.get("FREQINOUT_CONFIG_DIR")
    if env_cfg:
        return Path(env_cfg)

    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA") or Path.home()) / APP_NAME
    else:
        base = Path.home() / f".{APP_NAME.lower()}"

    try:
        base.mkdir(parents=True, exist_ok=True)
        return base
    except Exception:
        fallback = Path.cwd() / "freqinout_config"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback
