from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QLabel, QSizePolicy, QVBoxLayout, QWidget

from freqinout.gui.current_page_stack import CurrentPageStack
from freqinout.gui.software_administration_workspace import SoftwareAdministrationWorkspace
from freqinout.gui.software_instance_assistant import SoftwareInstanceAssistant


def _app() -> QApplication:
    app = QApplication.instance()
    return app if isinstance(app, QApplication) else QApplication([])


def test_current_page_stack_ignores_hidden_page_hints() -> None:
    _app()
    stack = CurrentPageStack()
    compact = QLabel("Current")
    huge = QWidget()
    huge.setMinimumSize(2400, 3200)
    stack.addWidget(compact)
    stack.addWidget(huge)
    stack.setCurrentWidget(compact)

    assert stack.sizeHint().height() < 3200
    assert stack.minimumSizeHint().height() < 3200

    stack.setCurrentWidget(huge)
    assert stack.sizeHint().height() >= 3200
    stack.deleteLater()


def test_software_surfaces_remain_contained_at_small_viewport_after_switches() -> None:
    app = _app()
    workspace = SoftwareAdministrationWorkspace()
    assistant = SoftwareInstanceAssistant("js8call", radios=())
    try:
        workspace.resize(900, 600)
        workspace.show()
        workspace.editor_stack.addWidget(assistant)
        workspace.editor_stack.setCurrentWidget(assistant)
        app.processEvents()

        host_rect = workspace.editor_host.rect()
        assert host_rect.contains(workspace.editor_stack.geometry().topLeft())
        assert host_rect.contains(workspace.editor_stack.geometry().bottomRight())
        assert workspace.window() is workspace
        assert assistant.window() is workspace
        assert not [window for window in app.topLevelWidgets() if window is assistant]

        # Repeated deferred-like current changes must not collapse the active
        # assistant or leave a stale oversized editor bound behind.
        placeholder = workspace.editor_placeholder
        workspace.editor_stack.setCurrentWidget(placeholder)
        workspace.editor_stack.setCurrentWidget(assistant)
        workspace.resize(900, 600)
        app.processEvents()
        assert assistant.height() > 0
        assert host_rect.contains(workspace.editor_stack.geometry().bottomRight())
    finally:
        workspace.deleteLater()
        assistant.deleteLater()
        app.processEvents()


def test_current_page_stack_preserves_expanding_policy_without_forced_bounds() -> None:
    _app()
    stack = CurrentPageStack()
    stack.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Expanding)
    page = QWidget()
    QVBoxLayout(page).addWidget(QLabel("Page"))
    stack.addWidget(page)
    assert stack.minimumHeight() == 0
    assert stack.maximumHeight() == 16777215
    stack.deleteLater()
