#!/usr/bin/env python3
"""Bounded Slice 0 Qt soak/acceptance runner for FreqInOut.

The harness launches the real ``MainWindow`` in an isolated config directory,
cycles only safe UI navigation/layout interactions, watches event-loop lag on a
practical timer cadence, and records first-usable-shell plus shutdown timing.

Use ``--duration-sec`` to shorten the run for CI or local validation. The
default target remains 1800 seconds.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


SAFE_SCREEN_LABELS = {
    "ControlFreq",
    "SOP",
    "Settings",
    "Help",
}

MINIMAL_SOAK_PROFILE_NAME = "Slice 0 Soak Minimal"
QT_HARD_FAILURE_MARKERS = (
    "QObject::killTimer",
    "QObject::~QObject",
    "QThread: Destroyed while thread is still running",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config-dir",
        default="",
        help="Required isolated FREQINOUT_CONFIG_DIR to use for the run.",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=1800.0,
        help="Total soak duration in seconds. Use a shorter value for CI or local smoke.",
    )
    parser.add_argument(
        "--sample-period-sec",
        type=float,
        default=0.10,
        help="Event-loop lag sampling cadence in seconds.",
    )
    parser.add_argument(
        "--interaction-period-sec",
        type=float,
        default=2.0,
        help="How often to run a safe navigation/layout interaction batch.",
    )
    parser.add_argument(
        "--lag-budget-ms",
        type=float,
        default=50.0,
        help="Maximum acceptable event-loop lag per sample.",
    )
    parser.add_argument(
        "--warmup-sec",
        type=float,
        default=6.0,
        help="Monitoring grace period after the first usable shell before lag sampling begins.",
    )
    parser.add_argument(
        "--first-usable-budget-ms",
        type=float,
        default=10_000.0,
        help="Maximum allowed construction-to-first-usable-shell time.",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional path to write a JSON report.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=True,
        help="Stop the soak on the first gate failure and begin shutdown.",
    )
    parser.add_argument(
        "--no-fail-fast",
        action="store_false",
        dest="fail_fast",
        help="Keep running after the first gate failure until duration expires.",
    )
    return parser.parse_args()


def _prepare_env(config_dir: Path) -> Path:
    cfg_root = config_dir.expanduser().resolve()
    cfg_root.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    os.environ.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")
    os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu --disable-software-rasterizer")
    os.environ.setdefault("FREQINOUT_BLOCKING_INFO_DIALOGS", "1")
    os.environ.setdefault("FREQINOUT_PERF_METRICS", "1")
    os.environ["FREQINOUT_CONFIG_DIR"] = str(cfg_root)
    return cfg_root


def _seed_minimal_runtime_profile(config_dir: Path) -> None:
    from freqinout.core.multi_radio_store import MultiRadioStore

    store = MultiRadioStore(config_dir / "config" / "freqinout.db")
    try:
        existing = store.list_device_profiles()
    except Exception:
        existing = []
    if existing:
        return
    profile = store.save_device_profile(
        {
            "name": MINIMAL_SOAK_PROFILE_NAME,
            "radio_manufacturer": "FreqInOut",
            "radio_model": "Slice 0 Soak",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 1,
            "display_order": 10,
            "device_class": "tx_rx",
            "deployment_mode": "minimal",
            "control_backend": "manual",
            "use_flrig": 0,
            "use_fldigi": 0,
            "use_js8call": 0,
            "use_flmsg": 0,
            "use_flamp": 0,
            "use_js8spotter": 0,
            "use_commstat": 0,
            "use_varac": 0,
            "scheduler_enabled": 0,
            "use_messages": 0,
            "use_map": 0,
            "use_background_ingest": 0,
            "use_launch_control": 0,
            "use_net_control_tabs": 0,
        }
    )
    profile_id = int(profile.get("id", 0) or 0)
    if profile_id > 0:
        store.set_device_profile_runtime_active(profile_id, True)
        store.set_runtime_primary_device_profile(profile_id)


@dataclass
class SoakFailure:
    kind: str
    detail: str


@dataclass
class SoakResult:
    config_dir: str
    duration_sec: float
    sample_period_sec: float
    interaction_period_sec: float
    lag_budget_ms: float
    first_usable_budget_ms: float
    warmup_sec: float
    shutdown_reason: str = ""
    first_usable_shell_ms: float | None = None
    construction_ms: float | None = None
    shutdown_ms: float | None = None
    max_event_loop_lag_ms: float = 0.0
    samples: int = 0
    interactions: int = 0
    resize_cycles: int = 0
    nav_switches: int = 0
    failures: list[SoakFailure] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures


def _short_error(exc: BaseException) -> str:
    return f"{exc.__class__.__name__}: {exc}"


def _safe_nav_targets(window: Any, *, include_map: bool = False) -> list[tuple[str, str]]:
    specs = list(getattr(window, "_nav_specs", ()) or ())
    targets: list[tuple[str, str]] = []
    seen_screens: set[str] = set()
    for spec in specs:
        if len(spec) < 2:
            continue
        button_label = str(spec[0] or "").strip()
        screen_label = str(spec[1] or "").strip()
        if not button_label or not screen_label:
            continue
        if screen_label == "Map" and not include_map:
            continue
        if screen_label not in SAFE_SCREEN_LABELS and screen_label != "Map":
            continue
        if screen_label in seen_screens:
            continue
        seen_screens.add(screen_label)
        targets.append((button_label, screen_label))
    return targets


def _soak_resize_sequence() -> tuple[tuple[int, int], ...]:
    return (
        (1280, 900),
        (1440, 900),
        (1024, 768),
        (1360, 880),
    )


class Slice0SoakController:
    def __init__(
        self,
        app: Any,
        window: Any,
        *,
        duration_sec: float,
        sample_period_sec: float,
        interaction_period_sec: float,
        lag_budget_ms: float,
        first_usable_budget_ms: float,
        warmup_sec: float,
        startup_started_at: float,
        construct_finished_at: float,
        clock: Callable[[], float] = time.perf_counter,
        fail_fast: bool = True,
    ) -> None:
        self.app = app
        self.window = window
        self.duration_sec = max(0.0, float(duration_sec))
        self.sample_period_sec = max(0.01, float(sample_period_sec))
        self.interaction_period_sec = max(self.sample_period_sec, float(interaction_period_sec))
        self.lag_budget_ms = max(0.0, float(lag_budget_ms))
        self.first_usable_budget_ms = max(0.0, float(first_usable_budget_ms))
        self.warmup_sec = max(0.0, float(warmup_sec))
        self.clock = clock
        self.fail_fast = bool(fail_fast)
        self.result = SoakResult(
            config_dir=str(Path(os.environ.get("FREQINOUT_CONFIG_DIR", "")).resolve()) if os.environ.get("FREQINOUT_CONFIG_DIR") else "",
            duration_sec=self.duration_sec,
            sample_period_sec=self.sample_period_sec,
            interaction_period_sec=self.interaction_period_sec,
            lag_budget_ms=self.lag_budget_ms,
            first_usable_budget_ms=self.first_usable_budget_ms,
            warmup_sec=self.warmup_sec,
        )
        self._start = float(startup_started_at)
        self._construct_finished_at = float(construct_finished_at)
        self.result.construction_ms = round(max(0.0, (self._construct_finished_at - self._start) * 1000.0), 3)
        self._shutdown_started: float | None = None
        self._monitoring_start: float | None = None
        self._last_sample_at: float | None = None
        self._next_interaction: float | None = None
        self._next_resize_index = 0
        self._nav_targets = _safe_nav_targets(window)
        self._nav_index = 0
        self._interaction_kind_index = 0
        self._closing = False

    def show(self) -> None:
        self.window.show()
        try:
            self.app.processEvents()
        except Exception:
            pass
        self.result.first_usable_shell_ms = round(max(0.0, (self.clock() - self._start) * 1000.0), 3)
        self._monitoring_start = self.clock() + self.warmup_sec
        self._next_interaction = self._monitoring_start + self.interaction_period_sec
        if self.result.first_usable_shell_ms > self.first_usable_budget_ms:
            self._fail(
                "first_usable_budget",
                f"first usable shell took {self.result.first_usable_shell_ms:.1f} ms, over {self.first_usable_budget_ms:.1f} ms budget",
            )

    def tick(self, now: float | None = None) -> None:
        if self._closing:
            return
        current = self.clock() if now is None else float(now)
        if self._monitoring_start is None or self._next_interaction is None:
            return
        if current < self._monitoring_start:
            if current - self._start >= self.duration_sec:
                self.request_shutdown("duration reached")
            return
        self._sample_lag(current)
        self._maybe_interact(current)
        if current - self._start >= self.duration_sec:
            self.request_shutdown("duration reached")

    def _sample_lag(self, now: float) -> None:
        previous = self._last_sample_at
        self._last_sample_at = now
        if previous is None:
            return
        # Compare adjacent timer deliveries instead of an absolute phase. Qt
        # timers may fire a fraction early; an absolute expected timestamp then
        # skips that sample and falsely reports one full interval of lag on the
        # following tick.
        lag_ms = round(max(0.0, (now - previous - self.sample_period_sec) * 1000.0), 3)
        self.result.samples += 1
        self.result.max_event_loop_lag_ms = max(self.result.max_event_loop_lag_ms, lag_ms)
        if lag_ms > self.lag_budget_ms:
            self._fail(
                "event_loop_lag",
                f"observed {lag_ms:.1f} ms lag, over {self.lag_budget_ms:.1f} ms budget",
            )

    def _maybe_interact(self, now: float) -> None:
        if now < self._next_interaction:
            return
        self.result.interactions += 1
        # Model operator-paced actions. Combining several resizes, page changes,
        # and collapse toggles in one timer callback manufactures event-loop lag
        # that a user could not produce with one click.
        actions = (
            self._run_safe_navigation,
            self._run_safe_resize,
            self._run_safe_navigation,
            self._run_nav_collapse_toggle,
        )
        action = actions[self._interaction_kind_index % len(actions)]
        self._interaction_kind_index += 1
        action()
        self._next_interaction = max(self._next_interaction + self.interaction_period_sec, now + self.interaction_period_sec)

    def _run_safe_resize(self) -> None:
        sizes = _soak_resize_sequence()
        width, height = sizes[self._next_resize_index % len(sizes)]
        self._next_resize_index += 1
        try:
            self.window.resize(int(width), int(height))
            self.result.resize_cycles += 1
        except Exception as exc:
            self._fail("resize", _short_error(exc))
        try:
            self.app.processEvents()
        except Exception:
            pass

    def _run_safe_navigation(self) -> None:
        if not self._nav_targets:
            return
        button_label, screen_label = self._nav_targets[self._nav_index % len(self._nav_targets)]
        self._nav_index += 1
        try:
            if hasattr(self.window, "_activate_navigation_item"):
                self.window._activate_navigation_item(button_label, screen_label)
            else:
                nav_buttons = list(getattr(self.window, "nav_buttons", ()) or ())
                nav_specs = list(getattr(self.window, "_nav_specs", ()) or ())
                for spec, button in zip(nav_specs, nav_buttons):
                    if tuple(spec) == (button_label, screen_label):
                        button.click()
                        break
            self.result.nav_switches += 1
        except Exception as exc:
            self._fail("navigation", _short_error(exc))
        try:
            self.app.processEvents()
        except Exception:
            pass

    def _run_nav_collapse_toggle(self) -> None:
        btn = getattr(self.window, "nav_collapse_btn", None)
        if btn is None:
            return
        try:
            btn.click()
        except Exception as exc:
            self._fail("navigation_collapse", _short_error(exc))
        try:
            self.app.processEvents()
        except Exception:
            pass

    def _fail(self, kind: str, detail: str) -> None:
        self.result.failures.append(SoakFailure(kind=kind, detail=detail))
        if self.fail_fast and not self._closing:
            self.request_shutdown(kind)

    def request_shutdown(self, reason: str) -> None:
        if self._closing:
            return
        self._closing = True
        self._shutdown_started = self.clock()
        self.result.shutdown_reason = str(reason or "")
        try:
            self.window.close()
        except Exception as exc:
            self.result.failures.append(SoakFailure(kind="close", detail=_short_error(exc)))
            try:
                self.app.quit()
            except Exception:
                pass

    def complete_shutdown(self) -> None:
        if self._shutdown_started is None:
            return
        self.result.shutdown_ms = round(max(0.0, (self.clock() - self._shutdown_started) * 1000.0), 3)
        if self.result.shutdown_ms > 3000.0:
            self.result.failures.append(
                SoakFailure(
                    kind="shutdown_budget",
                    detail=f"shutdown took {self.result.shutdown_ms:.1f} ms, over 3000.0 ms budget",
                )
            )

    def should_continue(self) -> bool:
        return not self._closing


def _write_report(path: str, result: SoakResult) -> None:
    report_path = Path(path).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(result)
    payload["passed"] = result.passed
    payload["failures"] = [asdict(item) for item in result.failures]
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _print_report(result: SoakResult) -> None:
    print("Slice 0 Qt soak report")
    print(f"config_dir: {result.config_dir}")
    print(f"duration_sec: {result.duration_sec:.1f}")
    print(f"sample_period_sec: {result.sample_period_sec:.2f}")
    print(f"interaction_period_sec: {result.interaction_period_sec:.2f}")
    print(f"lag_budget_ms: {result.lag_budget_ms:.1f}")
    print(f"first_usable_budget_ms: {result.first_usable_budget_ms:.1f}")
    print(f"first_usable_shell_ms: {result.first_usable_shell_ms:.1f}" if result.first_usable_shell_ms is not None else "first_usable_shell_ms: n/a")
    print(f"construction_ms: {result.construction_ms:.1f}" if result.construction_ms is not None else "construction_ms: n/a")
    print(f"shutdown_ms: {result.shutdown_ms:.1f}" if result.shutdown_ms is not None else "shutdown_ms: n/a")
    print(f"max_event_loop_lag_ms: {result.max_event_loop_lag_ms:.1f}")
    print(f"samples: {result.samples}")
    print(f"interactions: {result.interactions}")
    print(f"resize_cycles: {result.resize_cycles}")
    print(f"nav_switches: {result.nav_switches}")
    print(f"result: {'PASS' if result.passed else 'FAIL'}")
    if result.failures:
        print("failures:")
        for failure in result.failures:
            print(f"  - {failure.kind}: {failure.detail}")


def _qt_message_is_hard_failure(message: str) -> bool:
    text = str(message or "")
    return any(marker in text for marker in QT_HARD_FAILURE_MARKERS)


def run_soak(args: argparse.Namespace) -> SoakResult:
    from contextlib import ExitStack
    from unittest.mock import patch

    cfg_arg = str(args.config_dir or "").strip()
    if not cfg_arg and not str(os.environ.get("FREQINOUT_CONFIG_DIR", "")).strip():
        raise SystemExit("Slice 0 soak requires an explicit --config-dir or FREQINOUT_CONFIG_DIR.")
    config_dir = Path(cfg_arg) if cfg_arg else Path(os.environ["FREQINOUT_CONFIG_DIR"])
    # Qt reads platform/plugin environment while its modules are imported, so
    # establish the isolated runtime before importing any PySide object.
    cfg_root = _prepare_env(config_dir)

    from PySide6.QtCore import QEventLoop, Qt, QTimer, qInstallMessageHandler
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.dialog_notifications import install_auto_closing_information_dialogs
    from freqinout.gui.main_window import MainWindow
    from freqinout.core.background_ingest import BackgroundIngestController
    from freqinout.core.scheduler_engine import SchedulerEngine

    _seed_minimal_runtime_profile(cfg_root)
    install_auto_closing_information_dialogs()

    app = QApplication.instance() or QApplication(["freqinout-gui-soak"])
    app.setQuitOnLastWindowClosed(False)

    qt_messages: list[str] = []

    def _capture_qt_message(_mode: object, _context: object, message: str) -> None:
        text = str(message or "")
        qt_messages.append(text)

    previous_message_handler = qInstallMessageHandler(_capture_qt_message)

    # A soak must never tune a real radio, launch applications, connect Mesh,
    # or ingest the operator's live files. It exercises the real shell and Qt
    # lifecycle while replacing only those external side-effect entry points.
    runtime_suppression = ExitStack()
    runtime_suppression.enter_context(patch.object(SchedulerEngine, "start", lambda _self: None))
    runtime_suppression.enter_context(
        patch.object(BackgroundIngestController, "start", lambda _self, **_kwargs: None)
    )
    runtime_suppression.enter_context(
        patch.object(MainWindow, "_start_mesh_runtime_if_enabled", lambda _self: None)
    )
    runtime_suppression.enter_context(
        patch.object(MainWindow, "_start_launch_control_startup", lambda _self: None)
    )

    startup_started = time.perf_counter()
    window = MainWindow(startup_status=None)
    window_construct_finished = time.perf_counter()

    controller = Slice0SoakController(
        app,
        window,
        duration_sec=float(args.duration_sec),
        sample_period_sec=float(args.sample_period_sec),
        interaction_period_sec=float(args.interaction_period_sec),
        lag_budget_ms=float(args.lag_budget_ms),
        first_usable_budget_ms=float(args.first_usable_budget_ms),
        warmup_sec=float(args.warmup_sec),
        startup_started_at=startup_started,
        construct_finished_at=window_construct_finished,
        fail_fast=bool(args.fail_fast),
    )
    controller.result.config_dir = str(cfg_root)
    controller.show()

    if qt_messages:
        for text in qt_messages:
            if _qt_message_is_hard_failure(text):
                controller.result.failures.append(SoakFailure(kind="qt_message", detail=text))
                if controller.fail_fast and not controller._closing:
                    controller.request_shutdown("qt warning")
                    break
        qt_messages.clear()

    timer = QTimer()
    timer.setSingleShot(False)
    timer.setTimerType(Qt.PreciseTimer)
    timer.setInterval(max(1, int(round(controller.sample_period_sec * 1000.0))))

    def _on_tick() -> None:
        controller.tick()
        if not controller.should_continue():
            timer.stop()

    timer.timeout.connect(_on_tick)
    timer.start()

    deadline_timer = QTimer()
    deadline_timer.setSingleShot(True)

    def _on_deadline() -> None:
        if controller.should_continue():
            controller.request_shutdown("duration reached")

    deadline_timer.timeout.connect(_on_deadline)
    deadline_timer.start(max(1, int(round(controller.duration_sec * 1000.0))))

    try:
        app.exec()
    finally:
        controller.complete_shutdown()
        if qt_messages:
            for text in qt_messages:
                if _qt_message_is_hard_failure(text):
                    controller.result.failures.append(SoakFailure(kind="qt_message", detail=text))
        try:
            qInstallMessageHandler(previous_message_handler)
        except Exception:
            pass
        try:
            timer.stop()
        except Exception:
            pass
        try:
            deadline_timer.stop()
        except Exception:
            pass
        try:
            window.deleteLater()
            app.processEvents(QEventLoop.ExcludeUserInputEvents)
        except Exception:
            pass
        runtime_suppression.close()
    return controller.result


def main() -> int:
    args = _parse_args()
    result = run_soak(args)
    if args.json_out:
        _write_report(args.json_out, result)
    _print_report(result)
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
