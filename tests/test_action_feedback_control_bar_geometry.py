"""Regression coverage for transient action feedback and the station shell.

These tests deliberately use layout geometry rather than screenshots.  The
reported Linux failure occurs when a short-lived settings guardrail message is
inserted above the Station Control Bar and then removed while the window is
fullscreen.
"""

from __future__ import annotations

from PySide6.QtCore import QEvent, QObject, QTimer
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.shared_state import ActionFeedbackEvent
from freqinout.gui.main_window import MainWindow


class _LayoutRequestCounter(QObject):
    def __init__(self, watched: QObject) -> None:
        super().__init__()
        self.count = 0
        watched.installEventFilter(self)

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if event.type() == QEvent.LayoutRequest:
            self.count += 1
        return super().eventFilter(watched, event)


def _feedback_shell() -> tuple[QWidget, QWidget, QFrame, QFrame]:
    """Build only the right-side shell needed by the production handlers."""
    shell = QWidget()
    shell.settings = {}
    right = QWidget(shell)
    right_layout = QVBoxLayout(right)
    right_layout.setContentsMargins(0, 0, 0, 0)
    right_layout.setSpacing(10)

    banner = QFrame(right)
    banner.setVisible(False)
    feedback_layout = QHBoxLayout(banner)
    feedback_layout.setContentsMargins(10, 6, 8, 6)
    feedback_layout.addWidget(QLabel("", banner), 1)
    history = QToolButton(banner)
    dismiss = QToolButton(banner)
    feedback_layout.addWidget(history)
    feedback_layout.addWidget(dismiss)
    right_layout.addWidget(banner, 0)

    bar = QFrame(right)
    bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
    bar.setMinimumHeight(52)
    bar.setMaximumHeight(180)
    bar.setLayout(QHBoxLayout())
    bar.layout().addWidget(QLabel("Station Control Bar", bar))
    right_layout.addWidget(bar, 0)
    right_layout.addWidget(QLabel("Workspace", right), 1)
    right.resize(1600, 900)
    right.show()

    shell.action_feedback_banner = banner
    shell.action_feedback_label = feedback_layout.itemAt(0).widget()
    shell._action_feedback_clear_timer = QTimer(shell)
    shell._action_feedback_clear_timer.setSingleShot(True)
    shell._action_feedback_clear_timer.timeout.connect(
        lambda: MainWindow._hide_action_feedback_banner(shell)
    )
    shell._action_feedback_geometry_pending = False
    shell._right_shell_container = right
    shell._right_shell_layout = right_layout
    shell.station_command_bar = bar
    shell._station_command_layout_signature = ("wide", False)
    shell._apply_station_command_bar_layout = lambda *, force=False: None
    shell._action_feedback_banner_role = MainWindow._action_feedback_banner_role
    shell._action_feedback_banner_style = MainWindow._action_feedback_banner_style.__get__(shell)
    shell._action_feedback_banner_scopes = MainWindow._action_feedback_banner_scopes
    shell._action_feedback_display_ms = MainWindow._action_feedback_display_ms
    shell._schedule_action_feedback_geometry_sync = (
        MainWindow._schedule_action_feedback_geometry_sync.__get__(shell)
    )
    shell._flush_action_feedback_geometry_sync = (
        MainWindow._flush_action_feedback_geometry_sync.__get__(shell)
    )
    return shell, right, banner, bar


def test_settings_saved_warning_auto_hide_restores_fullscreen_control_bar_geometry(
    monkeypatch,
) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])
    shell, right, banner, bar = _feedback_shell()
    counter = _LayoutRequestCounter(right)
    event = ActionFeedbackEvent(
        id="feedback-1",
        timestamp_utc="2026-09-13T20:06:52Z",
        scope="settings",
        action_type="save_guardrails",
        status="partial",
        summary="Settings saved, but 1 multi-rig guardrail warning needs review.",
        detail="Review the selected radio configuration.",
        source_surface="settings",
    )
    try:
        app.processEvents()
        baseline_height = bar.height()
        assert baseline_height >= bar.minimumHeight()
        assert banner.isHidden()

        MainWindow._on_action_feedback_event(shell, event)
        app.processEvents()
        assert not banner.isHidden()
        assert shell.action_feedback_label.text() == event.summary
        assert not bar.isHidden()
        assert bar.height() >= bar.minimumHeight()

        # Use a short timer only for the test; production uses the same
        # single-shot path and partial warnings normally remain for 12 seconds.
        shell._action_feedback_clear_timer.start(1)
        QTimer.singleShot(20, app.quit)
        app.exec()
        app.processEvents()

        assert banner.isHidden()
        assert not bar.isHidden()
        assert bar.height() >= bar.minimumHeight()
        assert abs(bar.height() - baseline_height) <= 2

        # A transient sibling must settle after one hide/show cycle rather than
        # continuously enqueueing layout work (the Linux repaint symptom).
        settled = counter.count
        app.processEvents()
        assert counter.count == settled
    finally:
        shell.deleteLater()
        app.processEvents()


def test_settings_feedback_ignores_station_command_source_without_touching_bar(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])
    shell, _right, banner, bar = _feedback_shell()
    try:
        app.processEvents()
        before = bar.geometry()
        event = ActionFeedbackEvent(
            id="feedback-2",
            timestamp_utc="2026-09-13T20:06:52Z",
            scope="settings",
            action_type="save",
            status="succeeded",
            summary="Settings saved.",
            source_surface="station_command_bar",
        )
        MainWindow._on_action_feedback_event(shell, event)
        app.processEvents()
        assert banner.isHidden()
        assert bar.geometry() == before
    finally:
        shell.deleteLater()
        app.processEvents()


def test_repeated_feedback_visibility_keeps_bar_stable_at_fullscreen_and_compact_sizes(
    monkeypatch,
) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])
    shell, right, banner, bar = _feedback_shell()
    event = ActionFeedbackEvent(
        id="feedback-repeat",
        timestamp_utc="2026-09-13T20:06:52Z",
        scope="settings",
        action_type="save_guardrails",
        status="partial",
        summary="Settings saved, but one item needs review.",
        source_surface="settings",
    )
    try:
        large_font = right.font()
        large_font.setPointSize(max(18, large_font.pointSize() + 6))
        for width, height, use_large_font in (
            (1920, 1080, False),
            (900, 700, False),
            (1920, 1080, True),
        ):
            right.setFont(large_font if use_large_font else QApplication.font())
            right.resize(width, height)
            app.processEvents()
            baseline_height = bar.height()
            for _ in range(10):
                MainWindow._on_action_feedback_event(shell, event)
                app.processEvents()
                assert not banner.isHidden()
                assert bar.height() >= bar.minimumHeight()
                MainWindow._hide_action_feedback_banner(shell)
                app.processEvents()
                app.processEvents()
                assert banner.isHidden()
                assert bar.height() >= bar.minimumHeight()
                assert abs(bar.height() - baseline_height) <= 2
    finally:
        shell.deleteLater()
        app.processEvents()
