from __future__ import annotations

from types import SimpleNamespace

import pytest

from tools.gui_slice0_soak import Slice0SoakController, _qt_message_is_hard_failure, _safe_nav_targets


class _FakeButton:
    def __init__(self) -> None:
        self.clicks = 0

    def click(self) -> None:
        self.clicks += 1


class _FakeApp:
    def __init__(self) -> None:
        self.process_events = 0
        self.quit_calls = 0
        self.quit_on_last_window_closed = None

    def processEvents(self, *_args, **_kwargs) -> None:
        self.process_events += 1

    def quit(self) -> None:
        self.quit_calls += 1

    def setQuitOnLastWindowClosed(self, value: bool) -> None:
        self.quit_on_last_window_closed = value


class _FakeWindow:
    def __init__(self) -> None:
        self._nav_specs = (
            ("Ops", "ControlFreq"),
            ("Inbox", "Messages"),
            ("Main", "Settings"),
            ("Plans", "Plan Builder"),
            ("SOP Builder", "SOP"),
            ("Help", "Help"),
            ("Net Ctrl", "NCS"),
            ("Map", "Map"),
        )
        self.nav_collapse_btn = _FakeButton()
        self.visible = False
        self.closed = False
        self.resize_calls: list[tuple[int, int]] = []
        self.navigation_calls: list[tuple[str, str]] = []

    def show(self) -> None:
        self.visible = True

    def resize(self, width: int, height: int) -> None:
        self.resize_calls.append((int(width), int(height)))

    def close(self) -> None:
        self.closed = True

    def _activate_navigation_item(self, button_label: str, screen_label: str) -> None:
        self.navigation_calls.append((button_label, screen_label))


def test_safe_nav_targets_skips_map_and_ncs() -> None:
    window = _FakeWindow()

    assert _safe_nav_targets(window) == [
        ("Ops", "ControlFreq"),
        ("Main", "Settings"),
        ("SOP Builder", "SOP"),
        ("Help", "Help"),
    ]


def test_soak_controller_records_normal_show_interactions_and_shutdown() -> None:
    clock_value = {"now": 100.0}

    def _clock() -> float:
        return clock_value["now"]

    app = _FakeApp()
    window = _FakeWindow()
    controller = Slice0SoakController(
        app,
        window,
        duration_sec=5.0,
        sample_period_sec=0.10,
        interaction_period_sec=0.20,
        lag_budget_ms=50.0,
        first_usable_budget_ms=10_000.0,
        warmup_sec=0.0,
        startup_started_at=100.0,
        construct_finished_at=100.05,
        clock=_clock,
        fail_fast=True,
    )

    controller.show()
    assert controller.result.construction_ms == pytest.approx(50.0, abs=0.01)
    assert controller.result.first_usable_shell_ms == pytest.approx(0.0, abs=0.01)
    assert app.process_events == 1
    assert window.visible is True

    clock_value["now"] = 100.10
    controller.tick(now=100.10)
    clock_value["now"] = 100.26
    controller.tick(now=100.26)
    assert controller.result.samples == 1
    assert controller.result.interactions == 1
    assert window.resize_calls == []
    assert window.navigation_calls == [("Ops", "ControlFreq")]
    assert window.nav_collapse_btn.clicks == 0
    assert controller.result.max_event_loop_lag_ms == pytest.approx(60.0, abs=0.01)
    assert controller.result.failures[0].kind == "event_loop_lag"
    assert window.closed is True

    clock_value["now"] = 100.51
    controller.complete_shutdown()
    assert controller.result.shutdown_ms == pytest.approx(250.0, abs=0.01)


def test_normal_shutdown_reason_is_not_treated_as_failure() -> None:
    clock_value = {"now": 10.0}

    def _clock() -> float:
        return clock_value["now"]

    app = _FakeApp()
    window = _FakeWindow()
    controller = Slice0SoakController(
        app,
        window,
        duration_sec=1.0,
        sample_period_sec=0.10,
        interaction_period_sec=0.20,
        lag_budget_ms=50.0,
        first_usable_budget_ms=10_000.0,
        warmup_sec=0.0,
        startup_started_at=10.0,
        construct_finished_at=10.0,
        clock=_clock,
        fail_fast=False,
    )

    controller.request_shutdown("duration reached")
    clock_value["now"] = 10.50
    controller.complete_shutdown()

    assert controller.result.shutdown_reason == "duration reached"
    assert [failure.kind for failure in controller.result.failures] == []
    assert controller.result.shutdown_ms == pytest.approx(500.0, abs=0.01)


def test_qt_message_hard_failure_filter_matches_shutdown_warnings() -> None:
    assert _qt_message_is_hard_failure("QObject::killTimer: Timers cannot be stopped from another thread")
    assert _qt_message_is_hard_failure("QObject::~QObject: Timers cannot be stopped from another thread")
    assert _qt_message_is_hard_failure("QThread: Destroyed while thread is still running")
    assert not _qt_message_is_hard_failure("This plugin does not support propagateSizeHints()")
