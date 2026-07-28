from __future__ import annotations

import datetime
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
        self._pending_focus_scope = ""
        self._pending_focus_radio_id: Optional[int] = None
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
        header.addWidget(self.help_btn, 0, Qt.AlignRight)
        layout.addLayout(header)

        self.summary_label = QLabel("")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        action_row.addStretch(1)
        action_row.addWidget(self.refresh_btn, 0, Qt.AlignRight)
        layout.addLayout(action_row)

        self.note_label = QLabel(
            "This view shows external software responsiveness and scheduler holds based on what FIO has observed."
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
        self.health_empty_label = QLabel("No station health dependency issues are currently being tracked.")
        self.health_empty_label.setObjectName("stationHealthEmptyState")
        self.health_empty_label.setWordWrap(True)
        self.health_empty_label.setVisible(False)
        layout.addWidget(self.health_empty_label, 0)
        layout.addWidget(self.table, 1)
        self._update_health_table_empty_state()

        recent_label = QLabel("Latest Scheduler Success and Issue Log")
        recent_label.setStyleSheet("font-size: 14px; font-weight: 700;")
        layout.addWidget(recent_label)

        self.scheduler_table = QTableWidget(0, 6, self)
        self.scheduler_table.setHorizontalHeaderLabels(
            ["UTC Time", "Decision", "Source", "What FIO Did", "Detail", "Target"]
        )
        self.scheduler_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.scheduler_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.scheduler_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.scheduler_table.setAlternatingRowColors(True)
        self.scheduler_table.setWordWrap(True)
        self.scheduler_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        sched_header = self.scheduler_table.horizontalHeader()
        sched_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        sched_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        sched_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        sched_header.setSectionResizeMode(3, QHeaderView.Stretch)
        sched_header.setSectionResizeMode(4, QHeaderView.Stretch)
        sched_header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.scheduler_empty_label = QLabel("No scheduler success or issue events have been recorded yet.")
        self.scheduler_empty_label.setObjectName("stationHealthSchedulerEmptyState")
        self.scheduler_empty_label.setWordWrap(True)
        self.scheduler_empty_label.setVisible(False)
        layout.addWidget(self.scheduler_empty_label, 0)
        layout.addWidget(self.scheduler_table, 1)
        self._update_scheduler_table_empty_state()
        self.apply_theme()

    def set_scope_resolver(self, resolver: Optional[ScopeResolver]) -> None:
        self._scope_resolver = resolver
        self.refresh_from_registry()

    def focus_scope(self, *, device_profile_id: Optional[int] = None, scope_name: str = "") -> None:
        self._pending_focus_radio_id = device_profile_id if device_profile_id not in (None, 0) else None
        self._pending_focus_scope = str(scope_name or "").strip()
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
        self._last_summary = summarize_station_health(
            include_scheduler_events=True,
            scope_resolver=self._scope_resolver,
        )
        self._render_summary()
        self._render_table()
        self._render_scheduler_events()

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
        self._update_health_table_empty_state()
        self._apply_pending_focus_scope()

    def _update_health_table_empty_state(self) -> None:
        if not hasattr(self, "health_empty_label") or not hasattr(self, "table"):
            return
        has_rows = self.table.rowCount() > 0
        self.health_empty_label.setVisible(not has_rows)
        self.table.setVisible(has_rows)

    def _apply_pending_focus_scope(self) -> None:
        scope_name = str(getattr(self, "_pending_focus_scope", "") or "").strip()
        radio_id = getattr(self, "_pending_focus_radio_id", None)
        candidates = {scope_name.lower()} if scope_name else set()
        if radio_id not in (None, 0, ""):
            candidates.add(f"radio {int(radio_id)}".lower())
        if not candidates:
            return
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            text = str(item.text() if item is not None else "").strip().lower()
            if text and text in candidates:
                self.table.selectRow(row)
                self.table.scrollToItem(item)
                self._clear_pending_focus_scope()
                return
        self._clear_pending_focus_scope()

    def _clear_pending_focus_scope(self) -> None:
        self._pending_focus_scope = ""
        self._pending_focus_radio_id = None

    @staticmethod
    def _format_scheduler_ts(value: object) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            dt = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc).strftime("%H:%M:%SZ")
        except Exception:
            return text

    @staticmethod
    def _format_scheduler_target(item: Mapping[str, object]) -> str:
        freq = item.get("frequency_hz")
        parts = []
        try:
            if freq not in (None, ""):
                parts.append(f"{int(freq) / 1_000_000.0:.3f} MHz")
        except Exception:
            pass
        for key in ("band", "mode", "vfo"):
            text = str(item.get(key, "") or "").strip()
            if text:
                parts.append(text)
        return " / ".join(parts)

    def _scheduler_event_severity(self, item: Mapping[str, object]) -> str:
        kind = str(item.get("_station_health_kind", "") or "")
        if kind == "latest_success":
            return "ok"
        code = str(item.get("code", "") or "")
        event_type = str(item.get("event_type", "") or "")
        if event_type in {"failed"} or "failed" in code:
            return "danger"
        if event_type in {"hold", "skip", "watchdog", "breakaway"}:
            return "warning"
        if event_type in {"applied", "resume"}:
            return "ok"
        return ""

    def _render_scheduler_events(self) -> None:
        events = list(self._last_summary.get("recent_scheduler_events", []) or [])
        self.scheduler_table.setRowCount(len(events))
        for row, item in enumerate(events):
            if not isinstance(item, Mapping):
                continue
            severity = self._scheduler_event_severity(item)
            kind = str(item.get("_station_health_kind", "") or "")
            decision = str(item.get("code", "") or "")
            if kind == "latest_success":
                decision = f"latest success: {decision}"
            values = [
                self._format_scheduler_ts(item.get("ts_utc", "")),
                decision,
                item.get("source", ""),
                item.get("action", ""),
                item.get("detail", ""),
                self._format_scheduler_target(item),
            ]
            for col, value in enumerate(values):
                self.scheduler_table.setItem(row, col, self._item(value, severity=severity if col == 1 else ""))
        self.scheduler_table.resizeRowsToContents()
        self._update_scheduler_table_empty_state()

    def _update_scheduler_table_empty_state(self) -> None:
        if not hasattr(self, "scheduler_empty_label") or not hasattr(self, "scheduler_table"):
            return
        has_rows = self.scheduler_table.rowCount() > 0
        self.scheduler_empty_label.setVisible(not has_rows)
        self.scheduler_table.setVisible(has_rows)
