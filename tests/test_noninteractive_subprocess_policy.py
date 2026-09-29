from __future__ import annotations

import subprocess

from freqinout.core import radio_catalog, subprocess_utils, system_timezone


class _FakeStartupInfo:
    def __init__(self) -> None:
        self.dwFlags = 0
        self.wShowWindow = -1


def test_noninteractive_subprocess_policy_is_empty_off_windows(monkeypatch) -> None:
    monkeypatch.setattr(subprocess_utils.os, "name", "posix")

    assert subprocess_utils.noninteractive_subprocess_kwargs() == {}


def test_noninteractive_subprocess_policy_hides_windows_helpers(monkeypatch) -> None:
    monkeypatch.setattr(subprocess_utils.os, "name", "nt")
    monkeypatch.setattr(subprocess_utils.subprocess, "CREATE_NO_WINDOW", 0x08000000, raising=False)
    monkeypatch.setattr(subprocess_utils.subprocess, "STARTF_USESHOWWINDOW", 0x00000001, raising=False)
    monkeypatch.setattr(subprocess_utils.subprocess, "SW_HIDE", 0, raising=False)
    monkeypatch.setattr(subprocess_utils.subprocess, "STARTUPINFO", _FakeStartupInfo, raising=False)

    kwargs = subprocess_utils.noninteractive_subprocess_kwargs()

    assert kwargs["creationflags"] == 0x08000000
    assert kwargs["startupinfo"].dwFlags == 0x00000001
    assert kwargs["startupinfo"].wShowWindow == 0


def test_timezone_probe_applies_noninteractive_policy(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        system_timezone,
        "noninteractive_subprocess_kwargs",
        lambda: {"creationflags": 123},
    )

    def _fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, stdout="Mountain Standard Time\n", stderr="")

    monkeypatch.setattr(system_timezone.subprocess, "run", _fake_run)

    assert system_timezone._command_output(["tzutil", "/g"]) == "Mountain Standard Time"
    assert calls == [
        (
            ["tzutil", "/g"],
            {
                "check": False,
                "capture_output": True,
                "text": True,
                "timeout": 1.5,
                "creationflags": 123,
            },
        )
    ]


def test_radio_catalog_probe_applies_noninteractive_policy(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        radio_catalog,
        "noninteractive_subprocess_kwargs",
        lambda: {"creationflags": 456},
    )

    def _fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(radio_catalog.subprocess, "run", _fake_run)

    assert radio_catalog._load_from_rigctl() == []
    assert calls == [
        (
            ["rigctl", "-l"],
            {
                "check": True,
                "capture_output": True,
                "text": True,
                "timeout": radio_catalog._RIGCTL_TIMEOUT_SECONDS,
                "creationflags": 456,
            },
        )
    ]
