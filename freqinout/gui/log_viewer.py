from __future__ import annotations

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTextEdit,
    QPushButton,
    QLabel,
    QComboBox,
    QSpinBox,
    QMessageBox,
    QInputDialog,
)
from PySide6.QtCore import QTimer, Signal
from PySide6.QtGui import QTextCursor

from freqinout.core.logger import _get_log_file, set_log_level, get_log_level
from freqinout.core.settings_manager import SettingsManager
from freqinout.gui.theme import resolve_theme, button_style


class LogViewerTab(QWidget):
    REFRESH_INTERVAL_MS = 1500
    log_level_changed = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self.log_file = _get_log_file()

        self._build_ui()
        self._apply_theme()
        self._apply_saved_level()
        self._refresh()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._refresh)
        self.timer.start(self.REFRESH_INTERVAL_MS)

    def _build_ui(self):
        layout = QVBoxLayout(self)
        toolbar = QHBoxLayout()

        self.refresh_btn = QPushButton("Refresh")
        self.clear_btn = QPushButton("Clear")
        self.search_btn = QPushButton("Search")
        self.open_btn = QPushButton("Open Log")

        toolbar.addWidget(self.refresh_btn)
        toolbar.addWidget(self.clear_btn)
        toolbar.addWidget(self.search_btn)
        toolbar.addWidget(self.open_btn)

        toolbar.addSpacing(20)
        font_group = QWidget()
        font_layout = QHBoxLayout(font_group)
        font_layout.setContentsMargins(0, 0, 0, 0)
        font_layout.setSpacing(4)
        font_layout.addWidget(QLabel("Font:"))
        self.font_spin = QSpinBox()
        self.font_spin.setRange(8, 20)
        self.font_spin.setValue(10)
        font_layout.addWidget(self.font_spin)
        toolbar.addWidget(font_group)

        toolbar.addSpacing(20)
        level_group = QWidget()
        level_layout = QHBoxLayout(level_group)
        level_layout.setContentsMargins(0, 0, 0, 0)
        level_layout.setSpacing(4)
        level_layout.addWidget(QLabel("Level:"))
        self.level_combo = QComboBox()
        self.level_combo.addItems(["DISABLED", "ERROR", "WARNING", "INFO", "DEBUG", "ALL"])
        level_layout.addWidget(self.level_combo)
        toolbar.addWidget(level_group)

        layout.addLayout(toolbar)

        self.text = QTextEdit()
        self.text.setReadOnly(True)
        layout.addWidget(self.text)

        self.status_label = QLabel(f"Log file: {self.log_file}")
        layout.addWidget(self.status_label)

        # connections
        self.refresh_btn.clicked.connect(self._refresh)
        self.clear_btn.clicked.connect(lambda: self.text.clear())
        self.search_btn.clicked.connect(self._search)
        self.open_btn.clicked.connect(self._open_file)
        self.font_spin.valueChanged.connect(self._update_font)
        self.level_combo.currentTextChanged.connect(self._on_level_changed)

        self._update_font()
        self._apply_theme()

    def _update_font(self):
        size = self.font_spin.value()
        theme = getattr(self, "_theme", resolve_theme(self.settings))
        self.text.setStyleSheet(
            f"background-color: {theme['surface']}; color: {theme['text']}; "
            f"font-family: monospace; font-size: {size}pt;"
        )

    def _apply_saved_level(self):
        saved = (self.settings.get("log_level", "") or "INFO").upper()
        idx = self.level_combo.findText(saved)
        if idx >= 0:
            self.level_combo.setCurrentIndex(idx)
        else:
            self.level_combo.setCurrentIndex(self.level_combo.findText("INFO"))
        if saved != "ALL":
            set_log_level(saved)

    def _read_log_tail(self, max_lines=800):
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()[-max_lines:]
            return lines
        except FileNotFoundError:
            # Create an empty file so future writes succeed
            try:
                from pathlib import Path

                Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)
                Path(self.log_file).touch()
            except Exception:
                pass
            return ["No log file yet. Use FreqInOut a bit first.\n"]
        except Exception as e:
            return [f"Error reading log: {e}\n"]

    def _filter_lines(self, lines):
        level = self.level_combo.currentText()
        if level == "ALL":
            return lines
        token1 = f"[{level}]"
        token2 = f" {level} "
        return [l for l in lines if (token1 in l or token2 in l)]

    def _color_for_line(self, line: str) -> str:
        theme = getattr(self, "_theme", resolve_theme(self.settings))
        if " ERROR " in line or " CRITICAL " in line:
            return theme["danger"]
        if " WARNING " in line:
            return theme["warning"]
        if " DEBUG " in line:
            return theme["info"]
        return theme["text_muted"]

    def apply_theme(self):
        self._apply_theme()

    def _apply_theme(self):
        self._theme = resolve_theme(self.settings)
        self.refresh_btn.setStyleSheet(button_style("primary", self._theme))
        self.clear_btn.setStyleSheet(button_style("muted", self._theme))
        self.search_btn.setStyleSheet(button_style("info", self._theme))
        self.open_btn.setStyleSheet(button_style("primary", self._theme))
        self._update_font()

    def _refresh(self):
        lines = self._filter_lines(self._read_log_tail())
        self.text.clear()
        for line in lines:
            color = self._color_for_line(line)
            html_line = f'<span style="color:{color}">{line.rstrip()}</span>'
            self.text.append(html_line)
        self.text.moveCursor(QTextCursor.End)

    def _on_level_changed(self, level: str):
        level = (level or "").upper()
        if level and level != "ALL":
            set_log_level(level)
            try:
                self.settings.set("log_level", level)
            except Exception:
                pass
            try:
                self.log_level_changed.emit(level)
            except Exception:
                pass
        self._refresh()

    def _search(self):
        term, ok = QInputDialog.getText(self, "Search Logs", "Enter keyword:")
        if not ok or not term:
            return
        lines = self._read_log_tail(1000)
        matches = [l for l in lines if term.lower() in l.lower()]
        if not matches:
            QMessageBox.information(self, "Search", f"No matches for '{term}'.")
            return
        self.text.clear()
        for line in matches:
            color = self._color_for_line(line)
            html_line = f'<span style="color:{color}">{line.rstrip()}</span>'
            self.text.append(html_line)
        self.text.moveCursor(QTextCursor.End)

    def _open_file(self):
        try:
            import os
            import sys
            import subprocess

            if sys.platform.startswith("win"):
                os.startfile(self.log_file)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", self.log_file])
            else:
                subprocess.Popen(["xdg-open", self.log_file])
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not open log file:\n{e}")
