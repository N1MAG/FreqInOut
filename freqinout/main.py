
import sys
import os
import argparse
import tempfile
import time
import traceback
from pathlib import Path

from PySide6.QtCore import QEventLoop, QLockFile, QTimer
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtGui import QIcon
from freqinout.core import db_initializer
from freqinout.core.logger import log
from freqinout.core.perf_metrics import emit_span, shutdown_perf_metrics
from freqinout.core import updater
from freqinout.core.config_paths import get_config_dir
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.multi_rig_runtime_status import (
    STARTUP_DEFERRED,
    STARTUP_EXISTING_UNMIGRATED,
    STARTUP_MIGRATION_ERROR,
    build_multi_rig_runtime_status,
)
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.startup_lock import try_acquire_single_instance_lock
from freqinout.gui.dialog_notifications import install_auto_closing_information_dialogs
from freqinout.gui.startup_splash import StartupSplash
from freqinout.gui.startup_surface_trace import install_windows_startup_surface_trace
from freqinout.gui.theme import apply_app_theme, resolve_theme, resolve_ui_text_scale
from freqinout.version import __version__


def _write_fatal_startup_log(exc: BaseException) -> Path:
    """Persist fatal packaged-startup diagnostics even without a console."""

    candidates: list[Path] = []
    try:
        candidates.append(get_config_dir())
    except Exception:
        pass
    candidates.append(Path(tempfile.gettempdir()) / "FreqInOut")
    detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    for base in candidates:
        try:
            base.mkdir(parents=True, exist_ok=True)
            path = base / "startup-error.log"
            path.write_text(detail, encoding="utf-8")
            return path
        except Exception:
            continue
    return Path("startup-error.log")


def _set_windows_app_user_model_id() -> None:
    if sys.platform != "win32":
        return
    try:
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("N1MAG.FreqInOut")
    except Exception:
        pass


def _apply_application_identity(app: QApplication) -> None:
    """Set shell identity before the splash creates FIO's first window."""

    app.setApplicationName("FreqInOut")
    app.setApplicationDisplayName("FreqInOut")
    app.setOrganizationName("N1MAG")
    if sys.platform.startswith("linux"):
        # Matches ~/.local/share/applications/freqinout.desktop. Cinnamon,
        # GNOME, KDE, and Wayland shells use this association instead of
        # falling back to a generic Python/application gear.
        app.setDesktopFileName("freqinout")


def _app_icon_candidate_paths() -> list[Path]:
    names = (
        ("FreqInOut.ico", "FreqInOut-desktop.png")
        if sys.platform == "win32"
        else ("FreqInOut-desktop.png", "FreqInOut.ico")
    )
    roots: list[Path] = []
    bundle_root = str(getattr(sys, "_MEIPASS", "") or "").strip()
    if bundle_root:
        roots.append(Path(bundle_root) / "assets")
    roots.append(Path(__file__).resolve().parents[1] / "assets")
    candidates: list[Path] = []
    for root in roots:
        for name in names:
            candidate = root / name
            if candidate not in candidates:
                candidates.append(candidate)
    return candidates


def _load_app_icon() -> QIcon:
    for icon_path in _app_icon_candidate_paths():
        try:
            if not icon_path.exists():
                continue
            icon = QIcon(str(icon_path))
            if not icon.isNull():
                return icon
        except Exception:
            continue
    return QIcon()


def _apply_startup_theme(app: QApplication) -> None:
    try:
        settings = SettingsManager()
        apply_app_theme(app, resolve_theme(settings), ui_text_scale=resolve_ui_text_scale(settings))
    except Exception as e:
        log.debug("Startup theme/text-size application failed: %s", e)


def _emit_startup_stage(name: str, start: float, *, app_start: float | None = None) -> float:
    now = time.perf_counter()
    meta = {}
    if app_start is not None:
        meta["since_start_ms"] = round((now - app_start) * 1000.0, 3)
    emit_span(f"startup.{name}", (now - start) * 1000.0, meta=meta)
    return now


def _requires_existing_station_upgrade(startup_mode: str) -> bool:
    return startup_mode in {
        STARTUP_EXISTING_UNMIGRATED,
        STARTUP_DEFERRED,
        STARTUP_MIGRATION_ERROR,
    }


def _run_existing_station_upgrade_gate(*, before_dialog=None) -> bool:
    """Finish or decline a legacy station upgrade before MainWindow exists."""

    settings = SettingsManager()
    try:
        status = build_multi_rig_runtime_status(
            MultiRadioStore(),
            settings_values=dict(settings.all()),
        )
    finally:
        settings.close()
    if not _requires_existing_station_upgrade(status.startup_mode):
        return True
    if callable(before_dialog):
        before_dialog()

    from freqinout.gui.settings_tab import SettingsTab

    gate = SettingsTab(None, defer_initial_load=True)
    gate.set_multi_rig_runtime_status(status)
    try:
        upgraded = bool(gate._start_multi_rig_setup())
    finally:
        gate.shutdown()
        try:
            gate.settings.close()
        except Exception:
            pass
        gate.deleteLater()
    if not upgraded:
        log.info("Existing station upgrade was not completed; FIO will exit.")
    return upgraded


def _mark_packaged_smoke_test_ready(app: QApplication) -> None:
    """Publish packaging readiness after the usable shell has initialized."""

    marker = get_config_dir() / "packaged-smoke-test.ok"
    marker.write_text(f"FreqInOut {__version__} ready\n", encoding="utf-8")
    log.info("FreqInOut packaged smoke test passed: %s", marker)
    for handler in log.handlers:
        try:
            handler.flush()
        except Exception:
            pass
    # The Windows workflow owns test-process termination after observing the
    # marker. This keeps Qt teardown behavior separate from startup evidence.
    if sys.platform != "win32":
        app.quit()


def main():
    startup_started = time.perf_counter()
    parser = argparse.ArgumentParser(description="FreqInOut HF controller")
    parser.add_argument("--update", action="store_true", help="Check for and apply updates, then exit.")
    parser.add_argument("--smoke-test", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.update:
        updater.run_interactive_update()
        return

    _set_windows_app_user_model_id()
    stage_started = time.perf_counter()
    app = QApplication(sys.argv)
    _apply_application_identity(app)
    surface_trace = install_windows_startup_surface_trace(
        app,
        started_at=startup_started,
        platform=sys.platform,
    )
    _emit_startup_stage("qt_app_created", stage_started, app_start=startup_started)

    stage_started = time.perf_counter()
    if surface_trace is not None:
        surface_trace.set_stage("apply_startup_theme")
    _apply_startup_theme(app)
    _emit_startup_stage("apply_startup_theme", stage_started, app_start=startup_started)

    app_icon = _load_app_icon()
    if not app_icon.isNull():
        app.setWindowIcon(app_icon)

    stage_started = time.perf_counter()
    lock_path = get_config_dir() / "freqinout.lock"
    lockfile = QLockFile(str(lock_path))
    lockfile.setStaleLockTime(60_000)
    if not try_acquire_single_instance_lock(lockfile):
        _emit_startup_stage("single_instance_lock", stage_started, app_start=startup_started)
        QMessageBox.information(None, "FreqInOut", "FreqInOut is already running.")
        if surface_trace is not None:
            surface_trace.stop(stage="already_running")
        return
    _emit_startup_stage("single_instance_lock", stage_started, app_start=startup_started)

    splash = None
    stage_started = time.perf_counter()
    try:
        if surface_trace is not None:
            surface_trace.set_stage("splash_show")
        splash = StartupSplash(app, version=f"v{__version__}")
        splash.show("Checking database...")
        _emit_startup_stage("splash_visible", stage_started, app_start=startup_started)
    except Exception as e:
        splash = None
        log.warning("Startup splash could not be shown: %s", e)

    # Ensure SQLite schema is present while the operator can see startup progress.
    stage_started = time.perf_counter()
    try:
        if surface_trace is not None:
            surface_trace.set_stage("database_init")
        db_initializer.ensure_all_tables()
    except Exception as e:
        log.error("Database initialization failed: %s", e)
        if splash is not None:
            splash.update_status("Database check had a problem. Opening FIO...")
    finally:
        _emit_startup_stage("database_init", stage_started, app_start=startup_started)

    install_auto_closing_information_dialogs()
    app._single_instance = lockfile  # type: ignore[attr-defined]

    try:
        if surface_trace is not None:
            surface_trace.set_stage("existing_station_upgrade_gate")
        upgrade_ready = _run_existing_station_upgrade_gate(
            before_dialog=(splash.close if splash is not None else None),
        )
    except Exception as exc:
        log.exception("Unable to run the existing-station upgrade gate.")
        if surface_trace is not None:
            surface_trace.stop(stage="upgrade_gate_failed")
        if splash is not None:
            splash.close()
        QMessageBox.critical(
            None,
            "Upgrade Existing Station",
            "FIO could not verify the station upgrade state. No runtime services were started.\n\n"
            f"{exc}",
        )
        lockfile.unlock()
        return
    if not upgrade_ready:
        if splash is not None:
            splash.close()
        lockfile.unlock()
        if surface_trace is not None:
            surface_trace.stop(stage="upgrade_not_completed")
        return
    if splash is not None:
        splash.show("Preparing main window...")

    win = None
    try:
        if splash is not None:
            splash.update_status("Preparing main window...")
        stage_started = time.perf_counter()
        from freqinout.gui.main_window import MainWindow

        # MainWindow deliberately queues database/UI work for later event-loop
        # ticks.  Its progress callback must not pump the global event queue or
        # those timers run before the shell exists.
        def _report_startup_status(message: str) -> None:
            if surface_trace is not None:
                surface_trace.set_stage(message)
            if splash is not None:
                splash.update_status_without_event_pump(message)

        if surface_trace is not None:
            surface_trace.set_stage("main_window_construct")
        win = MainWindow(startup_status=_report_startup_status)
        _emit_startup_stage("main_window_construct", stage_started, app_start=startup_started)

        if splash is not None:
            splash.update_status("Opening FIO...")
        if surface_trace is not None:
            surface_trace.set_stage("main_window_show")
        stage_started = time.perf_counter()
        if hasattr(win, "release_startup_surface_shield"):
            win.release_startup_surface_shield()
        win.show()
        app.processEvents(QEventLoop.ExcludeUserInputEvents)
        _emit_startup_stage("main_window_show", stage_started, app_start=startup_started)
        _emit_startup_stage("first_usable_shell", startup_started)
        if splash is not None:
            splash.finish(win)
        if surface_trace is not None:
            surface_trace.stop(stage="startup_complete")
        _emit_startup_stage("startup_complete", startup_started)
        # Source listeners and projection catch-up deliberately begin only
        # after the first usable shell has been painted. The required legacy
        # station upgrade gate ran before MainWindow construction.
        if hasattr(win, "start_post_shell_services"):
            QTimer.singleShot(0, win.start_post_shell_services)
        if not args.smoke_test and hasattr(win, "present_first_run_onboarding"):
            QTimer.singleShot(0, win.present_first_run_onboarding)
        log.info("FreqInOut started.")
        if args.smoke_test:
            log.info("FreqInOut packaged smoke test started.")
            QTimer.singleShot(1000, lambda: _mark_packaged_smoke_test_ready(app))
    except Exception as e:
        log.exception("FreqInOut failed during startup: %s", e)
        if surface_trace is not None:
            surface_trace.stop(stage="startup_failed")
        if splash is not None:
            try:
                splash.update_status("FIO could not finish opening.")
                splash.close()
            except Exception:
                pass
        QMessageBox.critical(None, "FreqInOut", f"FIO could not finish opening.\n\n{e}")
        try:
            lockfile.unlock()
        except Exception:
            pass
        sys.exit(1)

    exit_code = app.exec()
    try:
        if win is not None:
            win.deleteLater()
        app.processEvents(QEventLoop.ExcludeUserInputEvents)
    except Exception:
        pass
    try:
        lockfile.unlock()
    except Exception:
        pass
    # Linux may use an intentional hard exit after Qt teardown; flush the
    # buffered telemetry lane explicitly because ``os._exit`` skips atexit.
    shutdown_perf_metrics(timeout=1.0)
    hard_exit = os.environ.get("FREQINOUT_HARD_EXIT")
    if hard_exit is None:
        hard_exit = "1" if sys.platform.startswith("linux") else "0"
    if hard_exit == "1":
        os._exit(exit_code)
    sys.exit(exit_code)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        try:
            log.exception("Fatal startup error")
        except Exception:
            pass
        path = _write_fatal_startup_log(exc)
        try:
            app = QApplication.instance() or QApplication(sys.argv)
            QMessageBox.critical(
                None,
                "FreqInOut Startup Error",
                f"FreqInOut could not start.\n\nDetails were written to:\n{path}",
            )
        except Exception:
            pass
        raise
