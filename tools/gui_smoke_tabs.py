from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _default_config_dir() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return Path(".benchmarks") / "gui-smoke" / stamp


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offscreen GUI smoke sweep for all MainWindow tabs.")
    parser.add_argument(
        "--config-dir",
        default="",
        help="Isolated config root. Defaults to .benchmarks/gui-smoke/<timestamp>.",
    )
    parser.add_argument(
        "--keep-config",
        action="store_true",
        help="Keep generated isolated config directory after run.",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional JSON report output path.",
    )
    parser.add_argument(
        "--sweep-mode",
        choices=["basic", "exhaustive"],
        default="basic",
        help="basic: quick alternate-index sweep; exhaustive: cycle all subsection indices and exclusive groups.",
    )
    parser.add_argument(
        "--exhaustive",
        action="store_true",
        help="Shortcut for --sweep-mode exhaustive.",
    )
    parser.add_argument(
        "--max-controls",
        type=int,
        default=24,
        help="Max controls per control-type sweep in each tab. Use 0 for no per-type limit.",
    )
    parser.add_argument(
        "--event-cycles",
        type=int,
        default=4,
        help="Event loop pump cycles per action.",
    )
    return parser.parse_args()


def _prepare_env(args: argparse.Namespace) -> Path:
    cfg_root = Path(args.config_dir).resolve() if args.config_dir else _default_config_dir().resolve()
    cfg_root.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    os.environ.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")
    os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu --disable-software-rasterizer")
    os.environ["FREQINOUT_CONFIG_DIR"] = str(cfg_root)
    return cfg_root


@dataclass
class Failure:
    operation: str
    error: str


@dataclass
class TabResult:
    index: int
    label: str
    widget_type: str
    status: str
    metrics: dict[str, int] = field(default_factory=dict)
    failures: list[Failure] = field(default_factory=list)


def _install_optional_dependency_shims() -> list[str]:
    """
    Install minimal runtime shims for optional packages that may not be present
    in local dev environments but are not required for broad GUI smoke.
    """
    installed: list[str] = []
    try:
        import reportlab  # noqa: F401
    except Exception:
        import types

        reportlab = types.ModuleType("reportlab")
        reportlab_lib = types.ModuleType("reportlab.lib")
        reportlab_pagesizes = types.ModuleType("reportlab.lib.pagesizes")
        reportlab_pagesizes.letter = (612.0, 792.0)

        reportlab_pdfgen = types.ModuleType("reportlab.pdfgen")
        reportlab_canvas = types.ModuleType("reportlab.pdfgen.canvas")

        class _Canvas:
            def __init__(self, *_args, **_kwargs) -> None:
                pass

            def setFont(self, *_args, **_kwargs) -> None:
                pass

            def drawString(self, *_args, **_kwargs) -> None:
                pass

            def showPage(self, *_args, **_kwargs) -> None:
                pass

            def save(self, *_args, **_kwargs) -> None:
                pass

        reportlab_canvas.Canvas = _Canvas

        sys.modules["reportlab"] = reportlab
        sys.modules["reportlab.lib"] = reportlab_lib
        sys.modules["reportlab.lib.pagesizes"] = reportlab_pagesizes
        sys.modules["reportlab.pdfgen"] = reportlab_pdfgen
        sys.modules["reportlab.pdfgen.canvas"] = reportlab_canvas
        installed.append("reportlab")
    return installed


def _short_error(exc: BaseException) -> str:
    return f"{exc.__class__.__name__}: {exc}"


def _patch_dialogs() -> None:
    from PySide6.QtWidgets import QFileDialog, QInputDialog, QMessageBox

    def _ok(*_args, **_kwargs) -> int:
        return int(QMessageBox.Ok)

    def _yes(*_args, **_kwargs) -> int:
        return int(QMessageBox.Yes)

    def _get_text(*_args, **kwargs) -> tuple[str, bool]:
        default = str(kwargs.get("text", "") or "")
        return default, False

    def _get_item(*_args, **kwargs) -> tuple[str, bool]:
        items = kwargs.get("items") or []
        val = str(items[0]) if items else ""
        return val, False

    def _get_open_file_name(*_args, **_kwargs) -> tuple[str, str]:
        return "", ""

    def _get_save_file_name(*_args, **_kwargs) -> tuple[str, str]:
        return "", ""

    QMessageBox.information = staticmethod(_ok)  # type: ignore[assignment]
    QMessageBox.warning = staticmethod(_ok)  # type: ignore[assignment]
    QMessageBox.critical = staticmethod(_ok)  # type: ignore[assignment]
    QMessageBox.question = staticmethod(_yes)  # type: ignore[assignment]
    QMessageBox.exec = lambda self: int(QMessageBox.Ok)  # type: ignore[assignment]

    QInputDialog.getText = staticmethod(_get_text)  # type: ignore[assignment]
    QInputDialog.getItem = staticmethod(_get_item)  # type: ignore[assignment]
    QFileDialog.getOpenFileName = staticmethod(_get_open_file_name)  # type: ignore[assignment]
    QFileDialog.getSaveFileName = staticmethod(_get_save_file_name)  # type: ignore[assignment]


def _pump_events(app: Any, cycles: int, sleep_s: float = 0.01) -> None:
    for _ in range(max(1, int(cycles))):
        app.processEvents()
        time.sleep(max(0.0, float(sleep_s)))
        app.processEvents()


def _run_step(
    *,
    operation: str,
    func: Callable[[], None],
    failures: list[Failure],
    app: Any,
    event_cycles: int,
) -> bool:
    try:
        func()
        _pump_events(app, event_cycles)
        return True
    except Exception as exc:
        failures.append(Failure(operation=operation, error=_short_error(exc)))
        return False


def _limit_controls(items: list[Any], max_controls: int) -> list[Any]:
    if int(max_controls) <= 0:
        return items
    return items[: int(max_controls)]


def _target_indices(count: int, original: int, exhaustive: bool) -> list[int]:
    if int(count) <= 1:
        return []
    if not exhaustive:
        target = 0 if int(original) != 0 else 1
        target = max(0, min(target, int(count) - 1))
        return [target] if target != int(original) else []
    return [i for i in range(int(count)) if i != int(original)]


def _toggle_controls(
    widget: Any,
    app: Any,
    failures: list[Failure],
    event_cycles: int,
    max_controls: int,
    sweep_mode: str,
) -> dict[str, int]:
    from PySide6.QtWidgets import QAbstractButton, QCheckBox, QComboBox, QListWidget, QStackedWidget, QTabWidget, QToolBox

    exhaustive = str(sweep_mode).strip().lower() == "exhaustive"

    metrics: dict[str, int] = {
        "combos_toggled": 0,
        "checkboxes_toggled": 0,
        "buttons_toggled": 0,
        "exclusive_buttons_switched": 0,
        "stacked_switched": 0,
        "tabwidgets_switched": 0,
        "toolboxes_switched": 0,
        "lists_switched": 0,
        "subsection_steps": 0,
    }

    combos = _limit_controls([c for c in widget.findChildren(QComboBox) if c.isEnabled() and c.count() > 1], max_controls)
    for idx, combo in enumerate(combos):
        original = int(combo.currentIndex())
        targets = _target_indices(combo.count(), original, exhaustive)
        touched = False
        for target in targets:

            def _flip_combo(c=combo, orig=original, tgt=target) -> None:
                c.setCurrentIndex(tgt)
                c.setCurrentIndex(orig)

            ok = _run_step(
                operation=f"combo_toggle[{idx}:{target}]",
                func=_flip_combo,
                failures=failures,
                app=app,
                event_cycles=event_cycles,
            )
            if ok:
                touched = True
                metrics["subsection_steps"] += 1
        if touched:
            metrics["combos_toggled"] += 1

    checkboxes = _limit_controls([cb for cb in widget.findChildren(QCheckBox) if cb.isEnabled()], max_controls)
    for idx, cb in enumerate(checkboxes):
        original = bool(cb.isChecked())

        def _flip_checkbox(box=cb, orig=original) -> None:
            box.setChecked(not orig)
            box.setChecked(orig)

        ok = _run_step(
            operation=f"checkbox_toggle[{idx}]",
            func=_flip_checkbox,
            failures=failures,
            app=app,
            event_cycles=event_cycles,
        )
        if ok:
            metrics["checkboxes_toggled"] += 1
            metrics["subsection_steps"] += 1

    buttons = [
        b
        for b in widget.findChildren(QAbstractButton)
        if b.isEnabled() and b.isCheckable() and not isinstance(b, QCheckBox) and not bool(b.autoExclusive())
    ]
    buttons = _limit_controls(buttons, max_controls)
    for idx, btn in enumerate(buttons):
        original = bool(btn.isChecked())

        def _flip_button(button=btn, orig=original) -> None:
            button.setChecked(not orig)
            button.setChecked(orig)

        ok = _run_step(
            operation=f"checkable_button_toggle[{idx}]",
            func=_flip_button,
            failures=failures,
            app=app,
            event_cycles=event_cycles,
        )
        if ok:
            metrics["buttons_toggled"] += 1
            metrics["subsection_steps"] += 1

    if exhaustive:
        exclusive_buttons = [
            b
            for b in widget.findChildren(QAbstractButton)
            if b.isEnabled() and b.isCheckable() and not isinstance(b, QCheckBox) and bool(b.autoExclusive())
        ]
        exclusive_buttons = _limit_controls(exclusive_buttons, max_controls)
        for idx, btn in enumerate(exclusive_buttons):
            parent = btn.parent()
            siblings = [s for s in exclusive_buttons if s.parent() is parent]
            original = next((s for s in siblings if bool(s.isChecked())), None)
            if original is None or original is btn:
                continue

            def _flip_exclusive(button=btn, orig=original) -> None:
                button.click()
                if orig.isEnabled():
                    orig.click()

            ok = _run_step(
                operation=f"exclusive_button_switch[{idx}]",
                func=_flip_exclusive,
                failures=failures,
                app=app,
                event_cycles=event_cycles,
            )
            if ok:
                metrics["exclusive_buttons_switched"] += 1
                metrics["subsection_steps"] += 1

    stacks = _limit_controls([s for s in widget.findChildren(QStackedWidget) if s.count() > 1], max_controls)
    for idx, stack in enumerate(stacks):
        original = int(stack.currentIndex())
        targets = _target_indices(stack.count(), original, exhaustive)
        touched = False
        for target in targets:

            def _flip_stack(sw=stack, orig=original, tgt=target) -> None:
                sw.setCurrentIndex(tgt)
                sw.setCurrentIndex(orig)

            ok = _run_step(
                operation=f"stacked_switch[{idx}:{target}]",
                func=_flip_stack,
                failures=failures,
                app=app,
                event_cycles=event_cycles,
            )
            if ok:
                touched = True
                metrics["subsection_steps"] += 1
        if touched:
            metrics["stacked_switched"] += 1

    tabs = _limit_controls([t for t in widget.findChildren(QTabWidget) if t.count() > 1], max_controls)
    for idx, tabw in enumerate(tabs):
        original = int(tabw.currentIndex())
        targets = _target_indices(tabw.count(), original, exhaustive)
        touched = False
        for target in targets:

            def _flip_tab(t=tabw, orig=original, tgt=target) -> None:
                t.setCurrentIndex(tgt)
                t.setCurrentIndex(orig)

            ok = _run_step(
                operation=f"tabwidget_switch[{idx}:{target}]",
                func=_flip_tab,
                failures=failures,
                app=app,
                event_cycles=event_cycles,
            )
            if ok:
                touched = True
                metrics["subsection_steps"] += 1
        if touched:
            metrics["tabwidgets_switched"] += 1

    toolboxes = _limit_controls([tb for tb in widget.findChildren(QToolBox) if tb.isEnabled() and tb.count() > 1], max_controls)
    for idx, box in enumerate(toolboxes):
        original = int(box.currentIndex())
        targets = _target_indices(box.count(), original, exhaustive)
        touched = False
        for target in targets:

            def _flip_toolbox(tb=box, orig=original, tgt=target) -> None:
                tb.setCurrentIndex(tgt)
                tb.setCurrentIndex(orig)

            ok = _run_step(
                operation=f"toolbox_switch[{idx}:{target}]",
                func=_flip_toolbox,
                failures=failures,
                app=app,
                event_cycles=event_cycles,
            )
            if ok:
                touched = True
                metrics["subsection_steps"] += 1
        if touched:
            metrics["toolboxes_switched"] += 1

    lists = _limit_controls([lw for lw in widget.findChildren(QListWidget) if lw.isEnabled() and lw.count() > 1], max_controls)
    for idx, lw in enumerate(lists):
        original = int(lw.currentRow())
        if original < 0:
            original = 0
            try:
                lw.setCurrentRow(original)
            except Exception:
                continue
        targets = _target_indices(lw.count(), original, exhaustive)
        touched = False
        for target in targets:

            def _flip_list(listw=lw, orig=original, tgt=target) -> None:
                listw.setCurrentRow(tgt)
                listw.setCurrentRow(orig)

            ok = _run_step(
                operation=f"list_switch[{idx}:{target}]",
                func=_flip_list,
                failures=failures,
                app=app,
                event_cycles=event_cycles,
            )
            if ok:
                touched = True
                metrics["subsection_steps"] += 1
        if touched:
            metrics["lists_switched"] += 1

    return metrics


def _smoke_one_tab(
    *,
    window: Any,
    index: int,
    label: str,
    app: Any,
    event_cycles: int,
    max_controls: int,
    sweep_mode: str,
) -> TabResult:
    failures: list[Failure] = []

    _run_step(
        operation="activate_screen",
        func=lambda: window._set_screen(index),
        failures=failures,
        app=app,
        event_cycles=event_cycles,
    )
    widget = window.stack.widget(index)
    widget_name = type(widget).__name__ if widget is not None else "NoneType"

    if widget is None:
        return TabResult(
            index=index,
            label=label,
            widget_type=widget_name,
            status="FAIL",
            metrics={},
            failures=failures + [Failure(operation="resolve_widget", error="RuntimeError: widget not found")],
        )

    if hasattr(widget, "set_tab_active"):
        _run_step(
            operation="set_tab_active(True)",
            func=lambda: widget.set_tab_active(True),
            failures=failures,
            app=app,
            event_cycles=event_cycles,
        )
    if hasattr(widget, "on_tab_activated"):
        _run_step(
            operation="on_tab_activated()",
            func=lambda: widget.on_tab_activated(),
            failures=failures,
            app=app,
            event_cycles=event_cycles,
        )
    if hasattr(widget, "apply_theme"):
        _run_step(
            operation="apply_theme()",
            func=lambda: widget.apply_theme(),
            failures=failures,
            app=app,
            event_cycles=event_cycles,
        )

    metrics = _toggle_controls(
        widget=widget,
        app=app,
        failures=failures,
        event_cycles=event_cycles,
        max_controls=max_controls,
        sweep_mode=sweep_mode,
    )
    status = "PASS" if not failures else "FAIL"
    return TabResult(
        index=index,
        label=label,
        widget_type=widget_name,
        status=status,
        metrics=metrics,
        failures=failures,
    )


def main() -> int:
    args = _parse_args()
    if bool(getattr(args, "exhaustive", False)):
        args.sweep_mode = "exhaustive"
    cfg_root = _prepare_env(args)

    unhandled: list[str] = []
    original_excepthook = sys.excepthook

    def _hook(exc_type: type[BaseException], exc: BaseException, tb: Any) -> None:
        text = "".join(traceback.format_exception(exc_type, exc, tb))
        unhandled.append(text)

    sys.excepthook = _hook

    start = time.perf_counter()
    report: dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "config_dir": str(cfg_root),
        "dependency_shims": [],
        "tabs": [],
        "summary": {},
    }

    exit_code = 0
    window = None
    app = None

    try:
        _patch_dialogs()
        report["dependency_shims"] = _install_optional_dependency_shims()

        from PySide6.QtWidgets import QApplication
        from freqinout.gui.main_window import MainWindow

        # Keep launch-control from spawning external applications in smoke runs.
        MainWindow._start_launch_control_startup = lambda self: None  # type: ignore[assignment]

        app = QApplication.instance() or QApplication([])
        window = MainWindow()
        window.show()
        _pump_events(app, args.event_cycles + 2)

        tab_results: list[TabResult] = []
        for idx, (label, _widget) in enumerate(getattr(window, "_screens", [])):
            tab_results.append(
                _smoke_one_tab(
                    window=window,
                    index=idx,
                    label=str(label),
                    app=app,
                    event_cycles=args.event_cycles,
                    max_controls=max(0, int(args.max_controls)),
                    sweep_mode=str(args.sweep_mode),
                )
            )

        if hasattr(window, "_on_app_about_to_quit"):
            try:
                window._on_app_about_to_quit()
            except Exception:
                pass
        try:
            window.close()
        except Exception:
            pass
        _pump_events(app, args.event_cycles)

        failed_tabs = [r for r in tab_results if r.status != "PASS"]
        report["tabs"] = [asdict(r) for r in tab_results]
        report["summary"] = {
            "total_tabs": len(tab_results),
            "passed_tabs": len(tab_results) - len(failed_tabs),
            "failed_tabs": len(failed_tabs),
            "unhandled_exceptions": len(unhandled),
            "duration_ms": round((time.perf_counter() - start) * 1000, 2),
            "sweep_mode": str(args.sweep_mode),
            "max_controls": int(args.max_controls),
            "event_cycles": int(args.event_cycles),
        }

        print("GUI smoke sweep")
        print(f"Config dir: {cfg_root}")
        print(f"Sweep mode: {args.sweep_mode}")
        print(f"Tabs: {report['summary']['total_tabs']} | Failed: {report['summary']['failed_tabs']}")
        for r in tab_results:
            metrics = ", ".join(f"{k}={v}" for k, v in sorted(r.metrics.items()))
            print(f"[{r.status}] #{r.index:02d} {r.label} ({r.widget_type}) :: {metrics}")
            for f in r.failures:
                print(f"  - {f.operation}: {f.error}")

        if unhandled:
            print(f"Unhandled exceptions captured: {len(unhandled)}")
            for idx, text in enumerate(unhandled, start=1):
                head = text.strip().splitlines()[-1] if text.strip() else "Unknown exception"
                print(f"  - [{idx}] {head}")

        if failed_tabs or unhandled:
            exit_code = 1
    except Exception as exc:
        exit_code = 2
        report["summary"] = {
            "total_tabs": 0,
            "passed_tabs": 0,
            "failed_tabs": 0,
            "unhandled_exceptions": len(unhandled),
            "duration_ms": round((time.perf_counter() - start) * 1000, 2),
            "fatal_error": _short_error(exc),
        }
        print(f"Fatal smoke runner failure: {_short_error(exc)}")
    finally:
        try:
            sys.excepthook = original_excepthook
        except Exception:
            pass
        if args.json_out:
            out_path = Path(args.json_out).resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            print(f"Wrote report: {out_path}")
        if not args.keep_config and not args.config_dir:
            try:
                shutil.rmtree(cfg_root, ignore_errors=True)
            except Exception:
                pass
        if app is not None:
            try:
                app.quit()
            except Exception:
                pass

    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
