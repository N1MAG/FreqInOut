from __future__ import annotations

import time
from typing import Optional

from PySide6.QtCore import QEvent, QObject, Qt
from PySide6.QtGui import QWindow
from PySide6.QtWidgets import QApplication, QWidget

from freqinout.core.logger import log


class StartupSurfaceTrace(QObject):
    """Log native-capable top-level surfaces during the Windows startup gate.

    The trace is deliberately observational: it does not hide, reparent, resize,
    or otherwise alter a widget.  Keeping it installed only until the usable
    shell is presented makes the field log small while still identifying any
    transient windows Windows maps behind the startup splash.
    """

    _EVENT_NAMES = {
        QEvent.Type.Show: "show",
        QEvent.Type.ShowToParent: "show_to_parent",
        QEvent.Type.Hide: "hide",
        QEvent.Type.HideToParent: "hide_to_parent",
        QEvent.Type.Close: "close",
        QEvent.Type.WinIdChange: "win_id_change",
        QEvent.Type.PlatformSurface: "platform_surface",
    }

    def __init__(self, app: QApplication, *, started_at: float) -> None:
        super().__init__(app)
        self._app = app
        self._started_at = float(started_at)
        self._stage = "trace_installed"
        self._sequence = 0
        self._active = True
        app.installEventFilter(self)
        log.info("STARTUP_SURFACE_TRACE action=begin stage=%s", self._stage)

    def set_stage(self, stage: str) -> None:
        if not self._active:
            return
        normalized = self._clean(stage) or "unspecified"
        if normalized == self._stage:
            return
        self._stage = normalized
        log.info(
            "STARTUP_SURFACE_TRACE action=stage elapsed_ms=%.1f stage=%s",
            self._elapsed_ms(),
            self._stage,
        )

    def stop(self, *, stage: str = "startup_complete") -> None:
        if not self._active:
            return
        self.set_stage(stage)
        self._active = False
        try:
            self._app.removeEventFilter(self)
        except Exception:
            pass
        log.info(
            "STARTUP_SURFACE_TRACE action=end elapsed_ms=%.1f events=%d stage=%s",
            self._elapsed_ms(),
            self._sequence,
            self._stage,
        )

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:  # noqa: N802
        if not self._active:
            return False
        event_name = self._EVENT_NAMES.get(event.type())
        if event_name is None:
            return False
        try:
            details = self._surface_details(watched)
            if details is None:
                return False
            self._sequence += 1
            log.info(
                "STARTUP_SURFACE_TRACE action=surface seq=%d elapsed_ms=%.1f "
                "stage=%s event=%s %s",
                self._sequence,
                self._elapsed_ms(),
                self._stage,
                event_name,
                details,
            )
        except Exception as exc:
            # Diagnostics must never jeopardize startup.
            log.debug("Startup surface trace could not describe an event: %s", exc)
        return False

    def _surface_details(self, watched: QObject) -> Optional[str]:
        if isinstance(watched, QWidget):
            if not watched.isWindow():
                return None
            geometry = watched.geometry()
            parent = watched.parentWidget()
            return (
                f"kind=QWidget class={self._clean(type(watched).__name__)} "
                f"object={self._clean(watched.objectName()) or '-'} "
                f"title={self._clean(watched.windowTitle()) or '-'} "
                f"geometry={geometry.x()},{geometry.y()},{geometry.width()},{geometry.height()} "
                f"visible={int(watched.isVisible())} hidden={int(watched.isHidden())} "
                f"dont_show={int(watched.testAttribute(Qt.WidgetAttribute.WA_DontShowOnScreen))} "
                f"parent={self._clean(type(parent).__name__) if parent is not None else '-'}"
            )
        if isinstance(watched, QWindow):
            geometry = watched.geometry()
            parent = watched.parent()
            return (
                f"kind=QWindow class={self._clean(type(watched).__name__)} "
                f"object={self._clean(watched.objectName()) or '-'} "
                f"title={self._clean(watched.title()) or '-'} "
                f"geometry={geometry.x()},{geometry.y()},{geometry.width()},{geometry.height()} "
                f"visible={int(watched.isVisible())} "
                f"parent={self._clean(type(parent).__name__) if parent is not None else '-'}"
            )
        return None

    def _elapsed_ms(self) -> float:
        return max(0.0, (time.perf_counter() - self._started_at) * 1000.0)

    @staticmethod
    def _clean(value: object) -> str:
        return " ".join(str(value or "").replace("|", "/").split())


def install_windows_startup_surface_trace(
    app: QApplication,
    *,
    started_at: float,
    platform: str,
) -> Optional[StartupSurfaceTrace]:
    """Install the bounded trace only for the affected native platform."""

    if platform != "win32":
        return None
    return StartupSurfaceTrace(app, started_at=started_at)
