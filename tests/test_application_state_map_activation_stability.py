from __future__ import annotations

from types import SimpleNamespace

from PySide6.QtCore import Qt

import freqinout.gui.main_window as main_window_module
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
    resumes: list[str] = []
    dirty: list[str] = []
    scheduler_resumes: list[str] = []
    visible_refreshes: list[str] = []
    host = SimpleNamespace(
        _shutting_down=False,
        _app_active=True,
        _observed_application_state=Qt.ApplicationActive,
        _ui_inactive_pending=False,
        _ui_resume_pending=False,
        _ui_scheduler_resume_required=False,
        _ui_inactive_settle_timer=_Timer(),
        _ui_resume_settle_timer=_Timer(),
        _pause_noncritical_ui_timers=lambda: pauses.append("pause"),
        _resume_noncritical_ui_timers=lambda: resumes.append("resume"),
        _set_child_app_active=lambda active: child_states.append(bool(active)),
        _mark_ui_refresh_dirty=lambda reason: dirty.append(str(reason)),
        _flush_visible_ui_refresh=lambda reason: visible_refreshes.append(str(reason)),
        scheduler=SimpleNamespace(handle_resume=lambda: scheduler_resumes.append("resume")),
        child_states=child_states,
        pauses=pauses,
        resumes=resumes,
        dirty=dirty,
        scheduler_resumes=scheduler_resumes,
        visible_refreshes=visible_refreshes,
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
    assert host._ui_scheduler_resume_required is False

    MainWindow._on_application_state_changed(host, Qt.ApplicationActive)

    assert host._app_active is True
    assert host._ui_resume_pending is True
    assert host._ui_resume_settle_timer.starts == 1
    assert host.child_states == [False, False]

    MainWindow._on_ui_resume_settled(host)

    assert host.scheduler_resumes == []
    assert host._ui_scheduler_resume_required is False


def test_hidden_application_pauses_immediately_without_grace_delay() -> None:
    host = _host()

    MainWindow._on_application_state_changed(host, Qt.ApplicationHidden)

    assert host._app_active is False
    assert host._ui_inactive_pending is False
    assert host._ui_inactive_settle_timer.starts == 0
    assert host.pauses == ["pause"]
    assert host.child_states == [False]
    assert host.dirty == ["app_inactive"]
    assert host._ui_scheduler_resume_required is True


def test_initial_activation_does_not_run_scheduler_resume_recovery() -> None:
    """Native launch presentation must not erase the first applied schedule state."""

    host = _host()
    # QApplication may be inactive while the native launch window is being
    # created, without this process ever committing an inactive transition.
    host._app_active = False
    host._observed_application_state = Qt.ApplicationInactive

    MainWindow._on_application_state_changed(host, Qt.ApplicationActive)

    assert host._ui_resume_pending is True
    assert host._ui_scheduler_resume_required is False
    assert host._ui_resume_settle_timer.starts == 1

    MainWindow._on_ui_resume_settled(host)

    assert host.scheduler_resumes == []
    assert host.resumes == ["resume"]
    assert host.child_states == [False, True]
    assert host.visible_refreshes == ["app_resume"]


def test_unchanged_appearance_never_reapplies_application_stylesheet(monkeypatch) -> None:
    theme = {"bg": "#101010", "text": "#eeeeee"}
    signature = (
        tuple(sorted((str(key), repr(value)) for key, value in theme.items())),
        1.0,
    )
    applied: list[bool] = []
    host = SimpleNamespace(
        settings=SimpleNamespace(reload=lambda: None),
        _applied_appearance_signature=signature,
    )
    monkeypatch.setattr(main_window_module, "resolve_theme", lambda _settings: dict(theme))
    monkeypatch.setattr(main_window_module, "resolve_ui_text_scale", lambda _settings: 1.0)
    monkeypatch.setattr(main_window_module, "apply_app_theme", lambda *_args, **_kwargs: applied.append(True))

    MainWindow._apply_app_theme(host)

    assert applied == []


def test_operating_group_change_refreshes_only_domain_consumers() -> None:
    refreshed: list[str] = []
    lazy_refreshes: list[str] = []
    scheduler_refreshes: list[str] = []

    def _tab(label: str) -> SimpleNamespace:
        return SimpleNamespace(on_settings_saved=lambda: refreshed.append(label))

    host = SimpleNamespace(
        hf_schedule_tab=_tab("hf"),
        net_tab=_tab("net"),
        fldigi_tab=_tab("fldigi"),
        js8_tab=_tab("js8"),
        scheduler=SimpleNamespace(force_refresh=lambda: scheduler_refreshes.append("scheduler")),
        _on_settings_saved_for_lazy_tabs=lambda: lazy_refreshes.append("lazy"),
        _run_timed_ui_refresh=lambda _label, callback: callback(),
    )

    MainWindow._on_operating_groups_changed(host)

    assert refreshed == ["hf", "net", "fldigi", "js8"]
    assert lazy_refreshes == ["lazy"]
    assert scheduler_refreshes == ["scheduler"]
