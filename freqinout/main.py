
import sys
import argparse

from PySide6.QtWidgets import QApplication
from freqinout.gui.main_window import MainWindow
from freqinout.core.logger import log
from freqinout.core import updater

def main():
    parser = argparse.ArgumentParser(description="FreqInOut HF controller")
    parser.add_argument("--update", action="store_true", help="Check for and apply updates, then exit.")
    args = parser.parse_args()

    if args.update:
        updater.run_interactive_update()
        return

    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    log.info("FreqInOut started.")
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
