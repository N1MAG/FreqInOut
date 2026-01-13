
import sys
import argparse

from PySide6.QtCore import QSharedMemory
from PySide6.QtWidgets import QApplication, QMessageBox
from freqinout.gui.main_window import MainWindow
from freqinout.core import db_initializer
from freqinout.core.logger import log
from freqinout.core import updater

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

    app = QApplication(sys.argv)
    single = QSharedMemory("FreqInOut_single_instance")
    if not single.create(1):
        QMessageBox.information(None, "FreqInOut", "FreqInOut is already running.")
        return
    app._single_instance = single  # type: ignore[attr-defined]
    win = MainWindow()
    win.show()
    log.info("FreqInOut started.")
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
