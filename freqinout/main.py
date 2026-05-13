
import sys
import os
import argparse
import tempfile
import traceback
from pathlib import Path

from PySide6.QtCore import QLockFile, QTimer
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtGui import QIcon
from freqinout.core.logger import log
from freqinout.core.config_paths import get_config_dir


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


def _write_fatal_startup_log(exc: BaseException) -> Path:
    candidates = []
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


def main():
    parser = argparse.ArgumentParser(description="FreqInOut HF controller")
    parser.add_argument("--update", action="store_true", help="Check for and apply updates, then exit.")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    if args.update:
        from freqinout.core import updater

        updater.run_interactive_update()
        return

    # Ensure SQLite schema is present before the UI starts
    try:
        from freqinout.core import db_initializer

        db_initializer.ensure_all_tables()
    except Exception as e:
        log.error("Database initialization failed: %s", e)

    from freqinout.core.startup_lock import try_acquire_single_instance_lock
    from freqinout.gui.dialog_notifications import install_auto_closing_information_dialogs
    from freqinout.gui.main_window import MainWindow

    _set_windows_app_user_model_id()
    app = QApplication(sys.argv)
    app_icon = _load_app_icon()
    if not app_icon.isNull():
        app.setWindowIcon(app_icon)
    lock_path = get_config_dir() / "freqinout.lock"
    lockfile = QLockFile(str(lock_path))
    lockfile.setStaleLockTime(60_000)
    if not try_acquire_single_instance_lock(lockfile):
        QMessageBox.information(None, "FreqInOut", "FreqInOut is already running.")
        return
    install_auto_closing_information_dialogs()
    app._single_instance = lockfile  # type: ignore[attr-defined]
    win = MainWindow()
    win.show()
    log.info("FreqInOut started.")
    if args.smoke_test:
        log.info("FreqInOut smoke test started.")
        QTimer.singleShot(1000, app.quit)
    exit_code = app.exec()
    try:
        win.deleteLater()
        app.processEvents()
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
