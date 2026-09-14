from __future__ import annotations

from types import SimpleNamespace

from PySide6.QtCore import Qt

from freqinout.gui.main_window import MainWindow


class _Timer:
    def __init__(self) -> None:
        self.active = False
        self.starts = 0
        self.stops = 0

    def start(self) -> None:
        self.active = True
        self.starts += 1

    def stop(self) -> None:
        self.active = False
        self.stops += 1


def _host() -> SimpleNamespace:
    child_states: list[bool] = []
    pauses: list[str] = []
    dirty: list[str] = []
    host = SimpleNamespace(
        _shutting_down=False,
        _app_active=True,
        _observed_application_state=Qt.ApplicationActive,
        _ui_inactive_pending=False,
        _ui_resume_pending=False,
        _ui_inactive_settle_timer=_Timer(),
        _ui_resume_settle_timer=_Timer(),
        _pause_noncritical_ui_timers=lambda: pauses.append("pause"),
        _set_child_app_active=lambda active: child_states.append(bool(active)),
        _mark_ui_refresh_dirty=lambda reason: dirty.append(str(reason)),
        child_states=child_states,
        pauses=pauses,
        dirty=dirty,
    )
    host._on_ui_inactive_settled = lambda: MainWindow._on_ui_inactive_settled(host)
    return host


def test_transient_webengine_inactive_cycle_does_not_pause_or_refresh_children() -> None:
    host = _host()

    MainWindow._on_application_state_changed(host, Qt.ApplicationInactive)

    assert host._app_active is True
    assert host._ui_inactive_pending is True
    assert host._ui_inactive_settle_timer.starts == 1
    assert host.child_states == []
    assert host.pauses == []

    MainWindow._on_application_state_changed(host, Qt.ApplicationActive)

    assert host._app_active is True
    assert host._ui_inactive_pending is False
    assert host.child_states == []
    assert host.pauses == []
    assert host._ui_resume_settle_timer.starts == 0


def test_sustained_inactivity_still_pauses_after_grace_period() -> None:
    host = _host()

    MainWindow._on_application_state_changed(host, Qt.ApplicationInactive)
    MainWindow._on_ui_inactive_settled(host)

    assert host._app_active is False
    assert host._ui_inactive_pending is False
    assert host.pauses == ["pause"]
    assert host.child_states == [False]
    assert host.dirty == ["app_inactive"]

    MainWindow._on_application_state_changed(host, Qt.ApplicationActive)

    assert host._app_active is True
    assert host._ui_resume_pending is True
    assert host._ui_resume_settle_timer.starts == 1
    assert host.child_states == [False, False]


def test_hidden_application_pauses_immediately_without_grace_delay() -> None:
    host = _host()

    MainWindow._on_application_state_changed(host, Qt.ApplicationHidden)

    assert host._app_active is False
    assert host._ui_inactive_pending is False
    assert host._ui_inactive_settle_timer.starts == 0
    assert host.pauses == ["pause"]
    assert host.child_states == [False]
    assert host.dirty == ["app_inactive"]
