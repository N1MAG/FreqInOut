from __future__ import annotations

from typing import Mapping, Optional

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QColor, QBrush
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_health_summary import ScopeResolver, summarize_station_health
from freqinout.gui.help_registry import resolve_help_host
from freqinout.gui.theme import button_style, resolve_theme


class StationHealthTab(QWidget):
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.settings = SettingsManager()
        self._tab_active = False
        self._last_summary: dict[str, object] = {"issue_count": 0, "severity": "ok", "items": []}
        self._scope_resolver: Optional[ScopeResolver] = None
        self._refresh_timer = QTimer(self)
        self._refresh_timer.setInterval(10000)
        self._refresh_timer.timeout.connect(self.refresh_from_registry)
        self._build_ui()
        self.refresh_from_registry()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel("Station Health")
        title.setStyleSheet("font-size: 16px; font-weight: 700;")
        header.addWidget(title, 1)
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh_from_registry)
        self.help_btn = QPushButton("Help")
        self.help_btn.setToolTip("Open Station Health help.")
        self.help_btn.clicked.connect(lambda: self._open_context_help("tab.station-health"))
        header.addWidget(self.refresh_btn, 0, Qt.AlignRight)
        header.addWidget(self.help_btn, 0, Qt.AlignRight)
        layout.addLayout(header)

        self.summary_label = QLabel("")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.note_label = QLabel(
            "This view shows external software responsiveness only. Traffic busy states are handled separately."
        )
        self.note_label.setWordWrap(True)
        layout.addWidget(self.note_label)

        self.table = QTableWidget(0, 9, self)
        self.table.setHorizontalHeaderLabels(
            [
                "Scope",
                "Dependency",
                "State",
                "What FIO Is Doing",
                "Last Issue",
                "Issue Since",
                "Cooldown",
                "Last Check",
                "Last Duration",
            ]
        )
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setAlternatingRowColors(True)
        self.table.setWordWrap(True)
        self.table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        header_view = self.table.horizontalHeader()
        header_view.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(3, QHeaderView.Stretch)
        header_view.setSectionResizeMode(4, QHeaderView.Stretch)
        header_view.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(8, QHeaderView.ResizeToContents)
        layout.addWidget(self.table, 1)
        self.apply_theme()

    def set_scope_resolver(self, resolver: Optional[ScopeResolver]) -> None:
        self._scope_resolver = resolver
        self.refresh_from_registry()

    def set_tab_active(self, active: bool) -> None:
        self._tab_active = bool(active)
        if active:
            self.refresh_from_registry()
            if not self._refresh_timer.isActive():
                self._refresh_timer.start()
        else:
            self._refresh_timer.stop()

    def current_issue_count(self) -> int:
        try:
            return int(self._last_summary.get("issue_count", 0) or 0)
        except Exception:
            return 0

    def current_severity(self) -> str:
        return str(self._last_summary.get("severity", "ok") or "ok")

    def refresh_from_registry(self) -> None:
        self._last_summary = summarize_station_health(scope_resolver=self._scope_resolver)
        self._render_summary()
        self._render_table()

    def apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.refresh_btn.setStyleSheet(button_style("muted", theme))
        self.help_btn.setStyleSheet(button_style("secondary", theme))
        self.note_label.setStyleSheet(f"color: {theme.get('text_muted', '#666')};")
        self._render_summary()

    def _open_context_help(self, context_key: str) -> None:
        host = resolve_help_host(self)
        if host is not None and hasattr(host, "open_context_help"):
            try:
                host.open_context_help(context_key)
            except Exception:
                pass

    def _render_summary(self) -> None:
        theme = resolve_theme(self.settings)
        issue_count = self.current_issue_count()
        severity = self.current_severity()
        if issue_count <= 0:
            text = "No external software responsiveness issues are known."
            color = theme.get("success", "#2E7D32")
        elif severity == "danger":
            text = (
                f"{issue_count} station responsiveness issue{'s' if issue_count != 1 else ''}. "
                "FIO is backing off from at least one slow or unreachable dependency."
            )
            color = theme.get("danger", "#C62828")
        else:
            text = f"{issue_count} station responsiveness issue{'s' if issue_count != 1 else ''}."
            color = theme.get("warning", "#C99700")
        self.summary_label.setText(text)
        self.summary_label.setStyleSheet(f"color: {color}; font-weight: 600;")

    def _item(self, text: object, *, severity: str = "") -> QTableWidgetItem:
        item = QTableWidgetItem(str(text or ""))
        item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
        if severity:
            theme = resolve_theme(self.settings)
            if severity == "danger":
                item.setForeground(QBrush(QColor(theme.get("danger", "#C62828"))))
            elif severity == "warning":
                item.setForeground(QBrush(QColor(theme.get("warning", "#C99700"))))
            elif severity == "ok":
                item.setForeground(QBrush(QColor(theme.get("success", "#2E7D32"))))
        return item

    def _render_table(self) -> None:
        items = list(self._last_summary.get("items", []) or [])
        self.table.setRowCount(len(items))
        for row, item in enumerate(items):
            if not isinstance(item, Mapping):
                continue
            severity = str(item.get("severity", "") or "")
            values = [
                item.get("scope", ""),
                item.get("dependency", ""),
                item.get("state", ""),
                item.get("action", ""),
                item.get("last_issue", ""),
                item.get("issue_since", ""),
                item.get("cooldown", ""),
                item.get("last_check", ""),
                item.get("last_duration", ""),
            ]
            for col, value in enumerate(values):
                self.table.setItem(row, col, self._item(value, severity=severity if col == 2 else ""))
        self.table.resizeRowsToContents()
