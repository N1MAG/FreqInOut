from __future__ import annotations

import os
import subprocess
from typing import Any, Dict


def noninteractive_subprocess_kwargs() -> Dict[str, Any]:
    """Return platform kwargs for an internal command that must not show UI.

    This policy is deliberately opt-in. It is for short, noninteractive FIO
    probes such as ``tzutil`` and ``rigctl -l``; user-launched radio
    applications must continue to use their normal launch paths.
    """

    if os.name != "nt":
        return {}

    kwargs: Dict[str, Any] = {}
    creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0) or 0)
    if creationflags:
        kwargs["creationflags"] = creationflags

    try:
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= int(getattr(subprocess, "STARTF_USESHOWWINDOW", 0) or 0)
        startupinfo.wShowWindow = int(getattr(subprocess, "SW_HIDE", 0) or 0)
        kwargs["startupinfo"] = startupinfo
    except (AttributeError, OSError):
        pass
    return kwargs
