from __future__ import annotations

from typing import Optional

from PySide6.QtCore import QEventLoop, Qt
from PySide6.QtGui import QColor, QFont, QPainter, QPixmap
from PySide6.QtWidgets import QApplication, QSplashScreen, QWidget


class StartupSplash:
    """Small, early startup status surface for slow first-window builds."""

    def __init__(self, app: QApplication, *, version: str = "") -> None:
        self._app = app
        self._splash = QSplashScreen(self._build_pixmap(version))
        self._splash.setWindowFlag(Qt.WindowStaysOnTopHint, True)
        self._splash.setWindowFlag(Qt.FramelessWindowHint, True)
        self._last_message = ""

    @staticmethod
    def _build_pixmap(version: str) -> QPixmap:
        pixmap = QPixmap(460, 190)
        pixmap.fill(QColor("#f4f7fb"))

        painter = QPainter(pixmap)
        try:
            painter.setRenderHint(QPainter.Antialiasing, True)
            painter.setPen(QColor("#2f3a45"))
            title_font = QFont()
            title_font.setPointSize(24)
            title_font.setBold(True)
            painter.setFont(title_font)
            painter.drawText(28, 54, "Starting FIO")

            subtitle_font = QFont()
            subtitle_font.setPointSize(10)
            painter.setFont(subtitle_font)
            painter.setPen(QColor("#5a6673"))
            subtitle = "FreqInOut"
            if version:
                subtitle = f"{subtitle} {version}"
            painter.drawText(30, 82, subtitle)

            painter.setPen(QColor("#c8d2dc"))
            painter.drawLine(30, 105, 430, 105)
        finally:
            painter.end()
        return pixmap

    def show(self, message: str = "Starting FIO...") -> None:
        self._splash.show()
        self.update_status(message)

    def update_status(self, message: str) -> None:
        text = str(message or "").strip() or "Starting FIO..."
        self._last_message = text
        self._splash.showMessage(
            text,
            int(Qt.AlignLeft | Qt.AlignBottom),
            QColor("#24313d"),
        )
        self._process_events()

    def finish(self, widget: Optional[QWidget]) -> None:
        try:
            if widget is not None:
                self._splash.finish(widget)
            else:
                self._splash.close()
        finally:
            self._process_events()

    def close(self) -> None:
        self._splash.close()
        self._process_events()

    def _process_events(self) -> None:
        self._app.processEvents(QEventLoop.ExcludeUserInputEvents)
