
import sys
import os
import argparse
import time
from pathlib import Path

from PySide6.QtCore import QEventLoop, QLockFile
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtGui import QIcon
from freqinout.core import db_initializer
from freqinout.core.logger import log
from freqinout.core.perf_metrics import emit_span
from freqinout.core import updater
from freqinout.core.config_paths import get_config_dir
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.startup_lock import try_acquire_single_instance_lock
from freqinout.gui.dialog_notifications import install_auto_closing_information_dialogs
from freqinout.gui.startup_splash import StartupSplash
from freqinout.gui.theme import apply_app_theme, resolve_theme, resolve_ui_text_scale
from freqinout.version import __version__


def _set_windows_app_user_model_id() -> None:
    if sys.platform != "win32":
        return
    try:
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("N1MAG.FreqInOut")
    except Exception:
        pass


def _load_app_icon() -> QIcon:
    assets_dir = Path(__file__).resolve().parents[1] / "assets"
    icon_candidates = [assets_dir / "FreqInOut.ico", assets_dir / "FreqInOut-desktop.png"]
    for icon_path in icon_candidates:
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


def main():
    startup_started = time.perf_counter()
    parser = argparse.ArgumentParser(description="FreqInOut HF controller")
    parser.add_argument("--update", action="store_true", help="Check for and apply updates, then exit.")
    args = parser.parse_args()

    if args.update:
        updater.run_interactive_update()
        return

    _set_windows_app_user_model_id()
    stage_started = time.perf_counter()
    app = QApplication(sys.argv)
    _emit_startup_stage("qt_app_created", stage_started, app_start=startup_started)

    stage_started = time.perf_counter()
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
        return
    _emit_startup_stage("single_instance_lock", stage_started, app_start=startup_started)

    splash = None
    stage_started = time.perf_counter()
    try:
        splash = StartupSplash(app, version=f"v{__version__}")
        splash.show("Checking database...")
        _emit_startup_stage("splash_visible", stage_started, app_start=startup_started)
    except Exception as e:
        splash = None
        log.warning("Startup splash could not be shown: %s", e)

    # Ensure SQLite schema is present while the operator can see startup progress.
    stage_started = time.perf_counter()
    try:
        db_initializer.ensure_all_tables()
    except Exception as e:
        log.error("Database initialization failed: %s", e)
        if splash is not None:
            splash.update_status("Database check had a problem. Opening FIO...")
    finally:
        _emit_startup_stage("database_init", stage_started, app_start=startup_started)

    install_auto_closing_information_dialogs()
    app._single_instance = lockfile  # type: ignore[attr-defined]

    win = None
    try:
        if splash is not None:
            splash.update_status("Preparing main window...")
        stage_started = time.perf_counter()
        from freqinout.gui.main_window import MainWindow

        win = MainWindow(startup_status=splash.update_status if splash is not None else None)
        _emit_startup_stage("main_window_construct", stage_started, app_start=startup_started)

        if splash is not None:
            splash.update_status("Opening FIO...")
        stage_started = time.perf_counter()
        win.show()
        app.processEvents(QEventLoop.ExcludeUserInputEvents)
        _emit_startup_stage("main_window_show", stage_started, app_start=startup_started)
        if splash is not None:
            splash.finish(win)
        _emit_startup_stage("startup_complete", startup_started)
        log.info("FreqInOut started.")
    except Exception as e:
        log.exception("FreqInOut failed during startup: %s", e)
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
    hard_exit = os.environ.get("FREQINOUT_HARD_EXIT")
    if hard_exit is None:
        hard_exit = "1" if sys.platform.startswith("linux") else "0"
    if hard_exit == "1":
        os._exit(exit_code)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
