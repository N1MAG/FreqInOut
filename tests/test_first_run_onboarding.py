from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core.multi_radio_store import (
    FIRST_RUN_ONBOARDING_ACK_KEY,
    detect_existing_fio_usage,
    ensure_multi_radio_settings_schema,
)
from freqinout.core.multi_rig_runtime_status import (
    STARTUP_DEFERRED,
    STARTUP_EXISTING_UNMIGRATED,
    STARTUP_FRESH_DEFAULT_READY,
    STARTUP_MIGRATED,
    STARTUP_MIGRATION_ERROR,
    should_present_first_run_onboarding,
)


ROOT = Path(__file__).resolve().parents[1]


def _status(mode: str = STARTUP_FRESH_DEFAULT_READY, version: int = 7):
    return SimpleNamespace(startup_mode=mode, migration_version=version)


def _fresh_settings(*, acknowledged: object = False, include_marker: bool = True):
    values = {}
    if include_marker:
        values["multi_rig_migration_summary_v7"] = {"fresh_install_blank_slate": True}
    if acknowledged is not False:
        values[FIRST_RUN_ONBOARDING_ACK_KEY] = acknowledged
    return values


def test_fresh_blank_station_is_eligible_until_user_acknowledges() -> None:
    status = _status()

    assert should_present_first_run_onboarding(
        status,
        settings_values=_fresh_settings(),
        has_device_profiles=False,
    )
    assert not should_present_first_run_onboarding(
        status,
        settings_values=_fresh_settings(acknowledged=True),
        has_device_profiles=False,
    )
    assert not should_present_first_run_onboarding(
        status,
        settings_values=_fresh_settings(acknowledged="true"),
        has_device_profiles=False,
    )


@pytest.mark.parametrize(
    "mode",
    (
        STARTUP_MIGRATED,
        STARTUP_EXISTING_UNMIGRATED,
        STARTUP_DEFERRED,
        STARTUP_MIGRATION_ERROR,
    ),
)
def test_upgrade_and_migrated_modes_are_never_fresh_onboarding(mode: str) -> None:
    assert not should_present_first_run_onboarding(
        _status(mode),
        settings_values=_fresh_settings(),
        has_device_profiles=False,
    )


def test_fresh_onboarding_requires_blank_slate_marker_and_no_saved_radio() -> None:
    assert not should_present_first_run_onboarding(
        _status(),
        settings_values=_fresh_settings(include_marker=False),
        has_device_profiles=False,
    )
    assert not should_present_first_run_onboarding(
        _status(),
        settings_values=_fresh_settings(),
        has_device_profiles=True,
    )


def test_onboarding_acknowledgement_does_not_count_as_legacy_usage() -> None:
    with sqlite3.connect(":memory:") as conn:
        ensure_multi_radio_settings_schema(conn)
        assert not detect_existing_fio_usage(
            conn,
            {FIRST_RUN_ONBOARDING_ACK_KEY: True},
        )


def test_public_guided_add_radio_seam_selects_radios_then_opens_existing_flow() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    events = []
    host = SimpleNamespace(
        show_settings_context=lambda context, **kwargs: events.append((context, kwargs)),
        _add_device_profile=lambda: events.append("guided-add-radio"),
    )

    SettingsTab.start_guided_add_radio(host)

    assert events == [
        ("radios", {"health_key": "radio_profiles"}),
        "guided-add-radio",
    ]


def test_ops_center_no_radio_action_routes_directly_to_guided_add_radio() -> None:
    from freqinout.gui.controlfreq_tab import ControlFreqTab

    events = []
    window = SimpleNamespace(open_guided_add_radio=lambda: events.append("guided-add-radio"))
    report = SimpleNamespace(first_actionable_issue=lambda: pytest.fail("fallback route used"))
    host = SimpleNamespace(
        _current_readiness_report=lambda: report,
        _readiness_has_device_profiles=False,
        window=lambda: window,
    )

    ControlFreqTab._review_readiness_now(host)

    assert events == ["guided-add-radio"]


def test_welcome_primary_action_acknowledges_and_queues_guided_add_radio(monkeypatch) -> None:
    import freqinout.gui.main_window as main_window_module

    queued = []
    monkeypatch.setattr(main_window_module.QTimer, "singleShot", lambda _delay, callback: queued.append(callback))
    primary = object()
    dialog = SimpleNamespace(clickedButton=lambda: primary)
    writes = []
    host = SimpleNamespace(
        _shutting_down=False,
        settings=SimpleNamespace(set=lambda key, value: writes.append((key, value))),
        open_guided_add_radio=lambda: None,
    )

    main_window_module.MainWindow._finish_first_run_onboarding(host, dialog, primary)

    assert writes == [(FIRST_RUN_ONBOARDING_ACK_KEY, True)]
    assert queued == [host.open_guided_add_radio]


def test_welcome_close_acknowledges_without_opening_setup(monkeypatch) -> None:
    import freqinout.gui.main_window as main_window_module

    queued = []
    monkeypatch.setattr(main_window_module.QTimer, "singleShot", lambda _delay, callback: queued.append(callback))
    writes = []
    host = SimpleNamespace(
        _shutting_down=False,
        settings=SimpleNamespace(set=lambda key, value: writes.append((key, value))),
        open_guided_add_radio=lambda: None,
    )

    main_window_module.MainWindow._finish_first_run_onboarding(
        host,
        SimpleNamespace(clickedButton=lambda: None),
        object(),
    )

    assert writes == [(FIRST_RUN_ONBOARDING_ACK_KEY, True)]
    assert queued == []


def test_onboarding_is_scheduled_only_after_shell_startup_is_complete() -> None:
    source = (ROOT / "freqinout" / "main.py").read_text(encoding="utf-8")
    main_block = source[source.index("def main") :]

    startup_complete = main_block.index('surface_trace.stop(stage="startup_complete")')
    schedule_onboarding = main_block.index("QTimer.singleShot(0, win.present_first_run_onboarding)")
    construct_window = main_block.index("win = MainWindow(")

    assert construct_window < startup_complete < schedule_onboarding
    assert 'if not args.smoke_test and hasattr(win, "present_first_run_onboarding")' in main_block


def test_no_radio_readiness_surface_keeps_setup_action_visible() -> None:
    source = (ROOT / "freqinout" / "gui" / "controlfreq_tab.py").read_text(encoding="utf-8")
    update = source[source.index("def _update_readiness_review_banner") : source.index("def on_condition_levels_changed")]

    assert 'self.readiness_review_now_btn.setText("Set Up First Radio…")' in update
    assert "self.readiness_review_dismiss_btn.setVisible(False)" in update
    assert "self.readiness_review_suppress_btn.setVisible(False)" in update
    assert "not requires_first_radio and (" in update
