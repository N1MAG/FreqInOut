
import sys
import os
import argparse
from pathlib import Path

from PySide6.QtCore import QLockFile
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtGui import QIcon
from freqinout.gui.main_window import MainWindow
from freqinout.core import db_initializer
from freqinout.core.logger import log
from freqinout.core import updater
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


def main():
    parser = argparse.ArgumentParser(description="FreqInOut HF controller")
    parser.add_argument("--update", action="store_true", help="Check for and apply updates, then exit.")
    args = parser.parse_args()

    if args.update:
        updater.run_interactive_update()
        return

    # Ensure SQLite schema is present before the UI starts
    try:
        db_initializer.ensure_all_tables()
    except Exception as e:
        log.error("Database initialization failed: %s", e)

    _set_windows_app_user_model_id()
    app = QApplication(sys.argv)
    app_icon = _load_app_icon()
    if not app_icon.isNull():
        app.setWindowIcon(app_icon)
    lock_path = get_config_dir() / "freqinout.lock"
    lockfile = QLockFile(str(lock_path))
    lockfile.setStaleLockTime(60_000)
    if not lockfile.isLocked():
        if not lockfile.tryLock(0):
            QMessageBox.information(None, "FreqInOut", "FreqInOut is already running.")
            return
    if not lockfile.isLocked():
        QMessageBox.information(None, "FreqInOut", "FreqInOut is already running.")
        return
    app._single_instance = lockfile  # type: ignore[attr-defined]
    win = MainWindow()
    win.show()
    log.info("FreqInOut started.")
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
    main()
