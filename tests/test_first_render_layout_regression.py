"""Bounded first-render and deferred-page geometry regressions.

These tests deliberately avoid constructing the production shell for every
case.  The source checks pin the two production stacks that must be active-page
aware; the widget tests exercise the shared primitive and MainWindow's lazy
replacement/activation contract with small in-memory pages.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QLabel, QWidget

from freqinout.gui.current_page_stack import CurrentPageStack
from freqinout.gui.main_window import MainWindow


_APP: QApplication | None = None


def _app() -> QApplication:
    global _APP
    _APP = QApplication.instance() or QApplication([])
    return _APP


def test_main_shell_and_messages_mode_use_active_page_stacks() -> None:
    """Hidden Compose and deferred screens must not inflate first Inbox paint."""

    main_source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    messages_source = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert "from freqinout.gui.current_page_stack import CurrentPageStack" in main_source
    assert "self.stack = CurrentPageStack()" in main_source
    assert "from freqinout.gui.current_page_stack import CurrentPageStack" in messages_source
    assert "self.messages_mode_stack = CurrentPageStack()" in messages_source


def test_windows_webengine_warmup_uses_the_proven_offscreen_child_view() -> None:
    """Windows keeps the established hidden-view warmup without touching shell geometry."""

    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    block = source[
        source.index("    def _prewarm_webengine") : source.index("    def _prewarm_next_lazy_tab")
    ]

    assert "from PySide6.QtWebEngineWidgets import QWebEngineView" in block
    assert "Qt.WA_DontShowOnScreen" in block
    assert "web.resize(4, 4)" in block
    assert "web.show()" in block
    for mutation in ("showMaximized(", "showFullScreen(", "setWindowState(", ".move("):
        assert mutation not in block


def test_webengine_prewarm_defaults_on_for_windows_only() -> None:
    host = SimpleNamespace(
        settings={},
        _platform_needs_webengine_prewarm=MainWindow._platform_needs_webengine_prewarm,
        _truthy_flag=MainWindow._truthy_flag,
    )

    with patch("freqinout.gui.main_window.sys.platform", "darwin"):
        assert MainWindow._should_prewarm_webengine_at_startup(host) is False
    with patch("freqinout.gui.main_window.sys.platform", "win32"):
        assert MainWindow._should_prewarm_webengine_at_startup(host) is True
    with patch("freqinout.gui.main_window.sys.platform", "linux"):
        assert MainWindow._should_prewarm_webengine_at_startup(host) is False

    host.settings["map_webengine_startup_prewarm"] = False
    with patch("freqinout.gui.main_window.sys.platform", "darwin"):
        assert MainWindow._should_prewarm_webengine_at_startup(host) is False


def test_current_page_stack_ignores_hidden_compose_sized_hint() -> None:
    _app()
    stack = CurrentPageStack()
    inbox = QLabel("Inbox")
    compose = QWidget()
    compose.setMinimumSize(1400, 1800)
    stack.addWidget(inbox)
    stack.addWidget(compose)
    stack.setCurrentWidget(inbox)

    assert stack.sizeHint().height() < 1800
    assert stack.minimumSizeHint().height() < 1800

    stack.setCurrentWidget(compose)
    assert stack.sizeHint().height() >= 1800
    assert stack.minimumSizeHint().height() >= 1800
    stack.deleteLater()


class _TrackedPage(QWidget):
    def __init__(self, name: str, activated: list[tuple[str, object]]) -> None:
        super().__init__()
        self.name = name
        self._activated = activated

    def on_tab_activated(self) -> None:
        host = self.parentWidget()
        self._activated.append((self.name, host.currentWidget() if host is not None else None))


def test_lazy_replacement_preserves_count_and_active_page() -> None:
    """Replacing a hidden deferred page must not disturb the visible page."""

    _app()
    stack = CurrentPageStack()
    first = QLabel("First")
    placeholder = QLabel("Loading Deferred...")
    last = QLabel("Last")
    stack.addWidget(first)
    stack.addWidget(placeholder)
    stack.addWidget(last)
    stack.setCurrentWidget(last)
    created: list[QWidget] = []

    shell = SimpleNamespace(
        settings={},
        stack=stack,
        _lazy_placeholders={"Deferred": placeholder},
        _lazy_factories={"Deferred": lambda: created.append(QLabel("Ready")) or created[-1]},
        _screens=[("First", first), ("Deferred", placeholder), ("Last", last)],
    )
    shell._get_tab_by_label = MethodType(MainWindow._get_tab_by_label, shell)

    MainWindow._ensure_lazy_tab_loaded(shell, "Deferred", 1)

    assert len(created) == 1
    assert stack.count() == 3
    assert shell._screens[1][1] is created[0]
    assert stack.widget(1) is created[0]
    assert stack.currentWidget() is last
    assert stack.currentIndex() == 2

    stack.deleteLater()


def test_lazy_replacement_never_exposes_adjacent_page_when_placeholder_is_current() -> None:
    """A visible deferred target must transition directly to its real page."""

    _app()
    stack = CurrentPageStack()
    first = QLabel("First")
    placeholder = QLabel("Loading Deferred...")
    last = QLabel("Last")
    for page in (first, placeholder, last):
        stack.addWidget(page)
    stack.setCurrentWidget(placeholder)
    transitions: list[QWidget | None] = []
    stack.currentChanged.connect(lambda _index: transitions.append(stack.currentWidget()))

    created: list[QWidget] = []

    def factory() -> QWidget:
        page = QLabel("Ready")
        created.append(page)
        return page

    shell = SimpleNamespace(
        settings={},
        stack=stack,
        _lazy_placeholders={"Deferred": placeholder},
        _lazy_factories={"Deferred": factory},
        _screens=[("First", first), ("Deferred", placeholder), ("Last", last)],
    )
    shell._get_tab_by_label = MethodType(MainWindow._get_tab_by_label, shell)

    MainWindow._ensure_lazy_tab_loaded(shell, "Deferred", 1)

    assert len(created) == 1
    assert stack.currentWidget() is created[0]
    assert all(page in {placeholder, created[0]} for page in transitions)
    assert last not in transitions
    stack.deleteLater()


def test_first_visible_activation_is_queued_after_lazy_page_becomes_current() -> None:
    """The deferred activation callback observes the replacement page as current."""

    app = _app()
    activated: list[tuple[str, object]] = []
    stack = CurrentPageStack()
    placeholder = QLabel("Loading Deferred...")
    stack.addWidget(placeholder)

    shell = SimpleNamespace(
        settings={},
        stack=stack,
        _screens=[("Deferred", placeholder)],
        _lazy_placeholders={"Deferred": placeholder},
        _lazy_factories={"Deferred": lambda: _TrackedPage("Deferred", activated)},
        _active_tab_index=None,
        _navigation_epoch=0,
        _screen_is_runtime_suppressed=lambda _label: False,
        _pending_map_switch_index=None,
        _help_dialog_settle_until=0.0,
        _queue_map_switch_after_webengine_warmup=lambda _index: False,
        _nav_screen_index_map={},
        _suppress_initial_nav_group_auto_expand=True,
        _expand_nav_group_for_screen=lambda _label: None,
        nav_buttons=[],
        _sync_compact_navigation_selection=lambda _label: None,
        _update_map_filters_visibility=lambda _index: None,
        _update_ncs_nav_button_styles=lambda: None,
        _schedule_status_refresh=lambda: None,
    )
    shell._get_tab_by_label = MethodType(MainWindow._get_tab_by_label, shell)
    shell._ensure_lazy_tab_loaded = MethodType(MainWindow._ensure_lazy_tab_loaded, shell)
    shell._current_screen_label = MethodType(MainWindow._current_screen_label, shell)
    shell._run_if_screen_current = MethodType(MainWindow._run_if_screen_current, shell)
    shell._settle_active_screen_layout = MethodType(MainWindow._settle_active_screen_layout, shell)

    MainWindow._set_screen(shell, 0)
    assert activated == []
    assert stack.currentIndex() == 0
    assert isinstance(stack.currentWidget(), _TrackedPage)

    app.processEvents()

    assert activated == [("Deferred", stack.currentWidget())]
    stack.deleteLater()
    app.processEvents()
