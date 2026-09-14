from __future__ import annotations

import os
from types import MethodType, SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QStackedWidget, QWidget


_APP: QApplication | None = None


def _app() -> QApplication:
    global _APP
    _APP = QApplication.instance() or QApplication([])
    return _APP


def test_deferred_screen_replaces_its_stable_placeholder_only_once(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_PERF_METRICS", "0")
    _app()

    from freqinout.gui.main_window import MainWindow

    placeholder = QWidget()
    stack = QStackedWidget()
    stack.addWidget(placeholder)
    created: list[QWidget] = []

    shell = SimpleNamespace(
        settings=SimpleNamespace(get=lambda *_args, **_kwargs: 0),
        stack=stack,
        _lazy_placeholders={"Deferred": placeholder},
        _screens=[("Deferred", placeholder)],
    )

    def _factory() -> QWidget:
        widget = QWidget()
        created.append(widget)
        return widget

    shell._lazy_factories = {"Deferred": _factory}
    shell._get_tab_by_label = lambda label: MainWindow._get_tab_by_label(shell, label)

    MainWindow._ensure_lazy_tab_loaded(shell, "Deferred", 0)
    MainWindow._ensure_lazy_tab_loaded(shell, "Deferred", 0)

    assert len(created) == 1
    assert shell._screens[0][1] is created[0]
    assert stack.widget(0) is created[0]

    stack.deleteLater()
    placeholder.deleteLater()
    created[0].deleteLater()


def test_deferred_screen_factory_failure_keeps_placeholder_and_is_logged(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_PERF_METRICS", "0")
    _app()

    from freqinout.gui import main_window
    from freqinout.gui.main_window import MainWindow

    placeholder = QWidget()
    stack = QStackedWidget()
    stack.addWidget(placeholder)
    failures: list[tuple[str, str]] = []

    shell = SimpleNamespace(
        settings=SimpleNamespace(get=lambda *_args, **_kwargs: 0),
        stack=stack,
        _lazy_placeholders={"Deferred": placeholder},
        _screens=[("Deferred", placeholder)],
        _lazy_factories={"Deferred": lambda: (_ for _ in ()).throw(AttributeError("missing callback"))},
    )
    shell._get_tab_by_label = lambda label: MainWindow._get_tab_by_label(shell, label)
    monkeypatch.setattr(
        main_window.log,
        "exception",
        lambda message, label: failures.append((message, label)),
    )

    MainWindow._ensure_lazy_tab_loaded(shell, "Deferred", 0)

    assert shell._screens == [("Deferred", placeholder)]
    assert stack.widget(0) is placeholder
    assert failures == [("MainWindow: failed to create deferred screen %r", "Deferred")]

    stack.deleteLater()
    placeholder.deleteLater()


def test_hf_operator_factory_treats_settings_callback_as_optional(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_PERF_METRICS", "0")
    _app()

    from freqinout.gui import main_window
    from freqinout.gui.main_window import MainWindow

    class _OperatorTabWithoutSettingsCallback(QWidget):
        def __init__(self, _parent=None):
            super().__init__()

    connections: list[tuple[str, object, object]] = []
    shell = SimpleNamespace(
        settings=SimpleNamespace(get=lambda *_args, **_kwargs: 0),
        settings_tab=SimpleNamespace(settings_saved=object()),
        operator_history_tab=None,
        _on_operator_history_local_update=lambda: None,
        _connect_lazy_screen_signal=lambda key, signal, slot: connections.append((key, signal, slot)),
    )
    monkeypatch.setattr(main_window, "OperatorHistoryTab", _OperatorTabWithoutSettingsCallback)

    tab = MainWindow._create_operator_history_tab(shell)

    assert tab is shell.operator_history_tab
    assert connections[0] == ("settings_saved.operator_history", shell.settings_tab.settings_saved, None)
    tab.deleteLater()


def test_callbackless_hf_operator_factory_replaces_deferred_placeholder(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_PERF_METRICS", "0")
    _app()

    from freqinout.gui import main_window
    from freqinout.gui.main_window import MainWindow

    class _OperatorTabWithoutSettingsCallback(QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)

    shell = QWidget()
    placeholder = QWidget()
    stack = QStackedWidget()
    stack.addWidget(placeholder)
    shell.settings = SimpleNamespace(get=lambda *_args, **_kwargs: 0)
    shell.settings_tab = SimpleNamespace(settings_saved=object())
    shell.operator_history_tab = None
    shell._on_operator_history_local_update = lambda: None
    shell._connect_lazy_screen_signal = lambda *_args: None
    shell.stack = stack
    shell._lazy_placeholders = {"HF Operators": placeholder}
    shell._screens = [("HF Operators", placeholder)]
    shell._get_tab_by_label = MethodType(MainWindow._get_tab_by_label, shell)
    shell._lazy_factories = {
        "HF Operators": lambda: MainWindow._create_operator_history_tab(shell),
    }
    monkeypatch.setattr(main_window, "OperatorHistoryTab", _OperatorTabWithoutSettingsCallback)

    MainWindow._ensure_lazy_tab_loaded(shell, "HF Operators", 0)

    assert shell._screens[0] == ("HF Operators", shell.operator_history_tab)
    assert stack.widget(0) is shell.operator_history_tab

    stack.deleteLater()
    placeholder.deleteLater()
    shell.operator_history_tab.deleteLater()
    shell.deleteLater()


def test_deferred_screen_prewarm_is_opt_in() -> None:
    from freqinout.gui.main_window import MainWindow

    disabled = SimpleNamespace(settings=SimpleNamespace(get=lambda *_args, **_kwargs: None))
    enabled = SimpleNamespace(settings=SimpleNamespace(get=lambda *_args, **_kwargs: "yes"))

    assert MainWindow._should_prewarm_deferred_screens_at_startup(disabled) is False
    assert MainWindow._should_prewarm_deferred_screens_at_startup(enabled) is True


def test_pending_map_focus_survives_deferred_map_construction(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()

    from freqinout.gui.main_window import MainWindow

    captured: list[dict[str, str]] = []
    tab = SimpleNamespace(focus_hf_reports=lambda **context: captured.append(context))
    shell = SimpleNamespace(
        _pending_map_focus=(
            "spotter",
            {"group_filter": "MAGNET", "topic_filter": "", "query_filter": "N1MAG", "state_filter": "", "grid_filter": ""},
        ),
        stations_map_tab=tab,
        _sync_map_filters_from_tab=lambda: None,
    )

    MainWindow._apply_pending_map_focus(shell)

    assert captured == [
        {"group_filter": "MAGNET", "topic_filter": "", "query_filter": "N1MAG", "state_filter": "", "grid_filter": ""}
    ]
    assert shell._pending_map_focus is None


def test_shell_defers_non_initial_workspaces_without_deferring_ops_or_settings() -> None:
    source = open("freqinout/gui/main_window.py", encoding="utf-8").read()

    assert '("ControlFreq", self.controlfreq_tab)' in source
    assert '("Settings", self.settings_tab)' in source
    for label in (
        "Station Overview",
        "Station Health",
        "HF Operators",
        "Local Operators",
        "Local Reports",
        "Peer Schedules",
        "Help",
        "HF Schedule",
        "Net Schedule",
        "NCS-FLDigi/SSB",
        "NCS-JS8",
        "NCS-Local",
    ):
        assert f'("{label}", self._placeholder_widget("{label}"))' in source
        assert f'"{label}": self._create_' in source
    assert '("Map", self._placeholder_widget("Map"))' in source
    assert '"Map": self._create_' not in source
    assert 'if label == "Map":' in source
    assert "self._open_map_window()" in source
