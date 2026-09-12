from __future__ import annotations

from types import MethodType, SimpleNamespace

from freqinout.gui.main_window import MainWindow


class _Stack:
    def __init__(self, current_index: int = 0) -> None:
        self.current_index = current_index

    def currentIndex(self) -> int:  # noqa: N802 - Qt-compatible test double
        return self.current_index


class _Settings:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int | None]] = []

    def show_settings_context(
        self,
        context: str,
        *,
        health_key: str,
        radio_id: int | None,
    ) -> None:
        self.calls.append((context, health_key, radio_id))


def _host() -> SimpleNamespace:
    host = SimpleNamespace(
        _screen_index_by_label={"Settings": 2},
        _settings_nav_context="main",
        _navigation_epoch=4,
        _shutting_down=False,
        stack=_Stack(),
        settings_tab=_Settings(),
    )

    def set_screen(self, index: int) -> None:
        self._navigation_epoch += 1
        self.stack.current_index = index

    host._set_screen = MethodType(set_screen, host)
    host._run_if_screen_current = MethodType(MainWindow._run_if_screen_current, host)
    return host


def test_deferred_settings_context_runs_for_current_navigation(monkeypatch):
    pending: list[object] = []
    monkeypatch.setattr(
        "freqinout.gui.main_window.QTimer.singleShot",
        lambda _delay, callback: pending.append(callback),
    )
    host = _host()

    MainWindow.open_settings_section(
        host,
        "software_administration",
        radio_id=7,
        settings_nav_context="software",
    )

    assert len(pending) == 1
    pending.pop()()
    assert host.settings_tab.calls == [("software", "software_administration", 7)]


def test_stale_settings_context_cannot_mutate_hidden_page(monkeypatch):
    pending: list[object] = []
    monkeypatch.setattr(
        "freqinout.gui.main_window.QTimer.singleShot",
        lambda _delay, callback: pending.append(callback),
    )
    host = _host()

    MainWindow.open_settings_section(
        host,
        "software_administration",
        settings_nav_context="software",
    )
    host._navigation_epoch += 1
    host.stack.current_index = 0

    assert len(pending) == 1
    pending.pop()()
    assert host.settings_tab.calls == []


def test_current_screen_label_uses_the_main_stack():
    host = SimpleNamespace(
        stack=_Stack(1),
        _screens=(("Ops", object()), ("Settings", object())),
    )

    assert MainWindow._current_screen_label(host) == "Settings"

