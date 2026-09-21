from __future__ import annotations

import freqinout.core.process_window_title as title_module


def test_window_title_dispatch_is_pid_scoped_by_platform(monkeypatch) -> None:
    calls: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        title_module,
        "_set_windows_title",
        lambda pid, title: calls.append(("windows", pid, title)) or True,
    )
    monkeypatch.setattr(
        title_module,
        "_set_x11_title",
        lambda pid, title: calls.append(("linux", pid, title)) or True,
    )

    assert title_module.set_process_window_title(
        41, "VarAC — FT-710", platform_name="Windows"
    )
    assert title_module.set_process_window_title(
        42, "VarAC — FTDX-10", platform_name="Linux"
    )
    assert calls == [
        ("windows", 41, "VarAC — FT-710"),
        ("linux", 42, "VarAC — FTDX-10"),
    ]


def test_window_title_rejects_ambiguous_or_unsupported_targets() -> None:
    assert not title_module.set_process_window_title(0, "VarAC — FT-710")
    assert not title_module.set_process_window_title(41, "")
    assert not title_module.set_process_window_title(
        41, "VarAC — FT-710", platform_name="Darwin"
    )


def test_x11_window_title_is_optional_without_a_display(monkeypatch) -> None:
    monkeypatch.delenv("DISPLAY", raising=False)
    assert not title_module.set_process_window_title(
        41, "VarAC — FT-710", platform_name="Linux"
    )
