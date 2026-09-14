from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QHBoxLayout, QLayout, QMainWindow, QSizePolicy, QWidget

from freqinout.gui.current_page_stack import CurrentPageStack


def _app() -> QApplication:
    app = QApplication.instance()
    return app if isinstance(app, QApplication) else QApplication([])


def test_switching_to_a_large_active_page_does_not_resize_the_shown_top_level() -> None:
    """A page switch must not let its hint replace the user's window size."""
    app = _app()
    window = QMainWindow()
    central = QWidget(window)
    layout = QHBoxLayout(central)
    layout.setSizeConstraint(QLayout.SetNoConstraint)
    stack = CurrentPageStack(central)
    # Primary shell pages own internal overflow; their hints must not replace
    # the window size chosen by the operator/window manager.
    stack.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Ignored)
    compact = QWidget(stack)
    large = QWidget(stack)
    large.setMinimumSize(2400, 1800)
    stack.addWidget(compact)
    stack.addWidget(large)
    layout.addWidget(stack)
    window.setCentralWidget(central)
    window.setMinimumSize(0, 0)
    window.resize(900, 560)

    try:
        window.show()
        app.processEvents()
        before = window.size()

        stack.setCurrentWidget(large)
        app.processEvents()

        assert window.size() == before
    finally:
        window.close()
        window.deleteLater()
        app.processEvents()
