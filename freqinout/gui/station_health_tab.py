from __future__ import annotations

import datetime
from typing import Callable, Iterable, Mapping, Optional

from PySide6.QtCore import Qt, QTimer, Signal
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
    related_view_requested = Signal(object)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.settings = SettingsManager()
        self._tab_active = False
        self._last_summary: dict[str, object] = {"issue_count": 0, "severity": "ok", "items": []}
        self._scope_resolver: Optional[ScopeResolver] = None
        self._runtime_item_provider: Optional[Callable[[], Iterable[Mapping[str, object]]]] = None
        self._runtime_source_provider: Optional[Callable[[], Iterable[Mapping[str, object]]]] = None
        self._runtime_source_rows: list[Mapping[str, object]] = []
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

        runtime_label = QLabel("Runtime Sources")
        runtime_label.setStyleSheet("font-size: 14px; font-weight: 700;")
        layout.addWidget(runtime_label)

        self.runtime_sources_table = QTableWidget(0, 7, self)
        self.runtime_sources_table.setHorizontalHeaderLabels(
            [
                "Source",
                "State",
                "Kind",
                "Radio/App",
                "Path or Endpoint",
                "Projected Data",
                "Suggested Fix",
            ]
        )
        self.runtime_sources_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.runtime_sources_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.runtime_sources_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.runtime_sources_table.setAlternatingRowColors(True)
        self.runtime_sources_table.setWordWrap(True)
        self.runtime_sources_table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.runtime_sources_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.runtime_sources_table.itemSelectionChanged.connect(self._render_runtime_source_detail)
        runtime_header = self.runtime_sources_table.horizontalHeader()
        runtime_header.setStretchLastSection(False)
        runtime_header.setSectionResizeMode(0, QHeaderView.Interactive)
        runtime_header.setSectionResizeMode(1, QHeaderView.Interactive)
        runtime_header.setSectionResizeMode(2, QHeaderView.Interactive)
        runtime_header.setSectionResizeMode(3, QHeaderView.Interactive)
        runtime_header.setSectionResizeMode(4, QHeaderView.Interactive)
        runtime_header.setSectionResizeMode(5, QHeaderView.Interactive)
        runtime_header.setSectionResizeMode(6, QHeaderView.Interactive)
        self.runtime_sources_table.setColumnWidth(0, 210)
        self.runtime_sources_table.setColumnWidth(1, 130)
        self.runtime_sources_table.setColumnWidth(2, 170)
        self.runtime_sources_table.setColumnWidth(3, 150)
        self.runtime_sources_table.setColumnWidth(4, 260)
        self.runtime_sources_table.setColumnWidth(5, 150)
        self.runtime_sources_table.setColumnWidth(6, 230)
        self.runtime_sources_empty_label = QLabel("No runtime ingest sources are currently configured or active.")
        self.runtime_sources_empty_label.setObjectName("stationHealthRuntimeSourcesEmptyState")
        self.runtime_sources_empty_label.setWordWrap(True)
        self.runtime_sources_empty_label.setVisible(False)
        layout.addWidget(self.runtime_sources_empty_label, 0)
        layout.addWidget(self.runtime_sources_table, 0)
        self.runtime_source_detail_label = QLabel("Select a runtime source to review details.")
        self.runtime_source_detail_label.setObjectName("stationHealthRuntimeSourceDetail")
        self.runtime_source_detail_label.setWordWrap(True)
        layout.addWidget(self.runtime_source_detail_label, 0)
        runtime_source_action_row = QHBoxLayout()
        runtime_source_action_row.setSpacing(8)
        runtime_source_action_row.addStretch(1)
        self.runtime_source_open_related_btn = QPushButton("Open Related View")
        self.runtime_source_open_related_btn.setObjectName("stationHealthOpenRuntimeSourceRelated")
        self.runtime_source_open_related_btn.setToolTip("Open the settings area related to the selected runtime source.")
        self.runtime_source_open_related_btn.setEnabled(False)
        self.runtime_source_open_related_btn.clicked.connect(self._on_open_related_runtime_source)
        runtime_source_action_row.addWidget(self.runtime_source_open_related_btn, 0, Qt.AlignRight)
        layout.addLayout(runtime_source_action_row)
        self._update_runtime_sources_empty_state()

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
        self.table.cellClicked.connect(self._on_health_table_cell_clicked)
        self.table.itemSelectionChanged.connect(self._on_health_table_selection_changed)
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
        health_action_row = QHBoxLayout()
        health_action_row.setSpacing(8)
        health_action_row.addStretch(1)
        self.health_open_related_btn = QPushButton("Open Related View")
        self.health_open_related_btn.setObjectName("stationHealthOpenRelated")
        self.health_open_related_btn.setToolTip("Open the settings or operational view related to the selected health item.")
        self.health_open_related_btn.setEnabled(False)
        self.health_open_related_btn.clicked.connect(self._on_open_related_health_item)
        health_action_row.addWidget(self.health_open_related_btn, 0, Qt.AlignRight)
        layout.addLayout(health_action_row)
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

    def set_runtime_item_provider(self, provider: Optional[Callable[[], Iterable[Mapping[str, object]]]]) -> None:
        self._runtime_item_provider = provider
        self.refresh_from_registry()

    def set_runtime_source_provider(self, provider: Optional[Callable[[], Iterable[Mapping[str, object]]]]) -> None:
        self._runtime_source_provider = provider
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
        runtime_items = []
        if self._runtime_item_provider is not None:
            try:
                runtime_items = list(self._runtime_item_provider() or [])
            except Exception:
                runtime_items = []
        self._runtime_source_rows = []
        if self._runtime_source_provider is not None:
            try:
                self._runtime_source_rows = [
                    row for row in list(self._runtime_source_provider() or []) if isinstance(row, Mapping)
                ]
            except Exception:
                self._runtime_source_rows = []
        self._last_summary = summarize_station_health(
            include_scheduler_events=True,
            scope_resolver=self._scope_resolver,
            extra_items=runtime_items,
        )
        self._render_summary()
        self._render_runtime_sources()
        self._render_table()
        self._render_scheduler_events()

    def apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.refresh_btn.setStyleSheet(button_style("muted", theme))
        self.help_btn.setStyleSheet(button_style("secondary", theme))
        if hasattr(self, "health_open_related_btn"):
            self.health_open_related_btn.setStyleSheet(button_style("secondary", theme))
        if hasattr(self, "runtime_source_open_related_btn"):
            self.runtime_source_open_related_btn.setStyleSheet(button_style("secondary", theme))
        self.note_label.setStyleSheet(f"color: {theme.get('text_muted', '#666')};")
        self._render_runtime_sources()
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
        current_row = self.table.currentRow() if hasattr(self, "table") else -1
        self._set_open_related_health_item_enabled(bool(self._health_row_related_payload(current_row)))

    def _on_health_table_cell_clicked(self, row: int, _column: int) -> None:
        self._focus_runtime_sources_from_health_row(row)

    def _on_health_table_selection_changed(self) -> None:
        row = self.table.currentRow() if hasattr(self, "table") else -1
        self._set_open_related_health_item_enabled(bool(self._health_row_related_payload(row)))
        self._focus_runtime_sources_from_health_row(row)

    def _focus_runtime_sources_from_health_row(self, row: int) -> None:
        if row < 0 or not hasattr(self, "table"):
            return
        dependency_item = self.table.item(row, 1)
        dependency = str(dependency_item.text() if dependency_item is not None else "").strip()
        if dependency != "Runtime ingest sources":
            return
        self.focus_runtime_sources()

    def focus_runtime_sources(self) -> None:
        if not hasattr(self, "runtime_sources_table") or self.runtime_sources_table.rowCount() <= 0:
            return
        target_row = 0
        for row in range(self.runtime_sources_table.rowCount()):
            state_item = self.runtime_sources_table.item(row, 1)
            state = str(state_item.text() if state_item is not None else "").strip().lower()
            if state in {"missing", "shared endpoint", "needs attention"}:
                target_row = row
                break
        self.runtime_sources_table.selectRow(target_row)
        item = self.runtime_sources_table.item(target_row, 0)
        if item is not None:
            self.runtime_sources_table.scrollToItem(item)
        self.runtime_sources_table.setFocus(Qt.OtherFocusReason)

    def _runtime_source_cached_traffic(self, item: Mapping[str, object]) -> str:
        count = 0
        try:
            count = int(item.get("projection_count", 0) or 0)
        except Exception:
            count = 0
        metadata = item.get("metadata", {})
        if not isinstance(metadata, Mapping):
            metadata = {}
        link_summary = metadata.get("link_projection_summary")
        if isinstance(link_summary, Mapping):
            try:
                links = int(link_summary.get("total", 0) or 0)
            except Exception:
                links = 0
            try:
                pairs = int(link_summary.get("station_pairs", 0) or 0)
            except Exception:
                pairs = 0
            if links > 0 and pairs > 0:
                return f"{links} links / {pairs} pairs"
            if links > 0:
                return f"{links} links"
        if count > 0:
            kind = str(item.get("source_kind", "") or "").strip().lower()
            if "commstat" in kind:
                noun = "artifact" if count == 1 else "artifacts"
            elif "link" in kind:
                noun = "link" if count == 1 else "links"
            else:
                noun = "message" if count == 1 else "messages"
            return f"{count} {noun}"
        return ""

    def _render_runtime_sources(self) -> None:
        if not hasattr(self, "runtime_sources_table"):
            return
        rows = list(getattr(self, "_runtime_source_rows", []) or [])
        self.runtime_sources_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            severity = str(row.get("severity", "") or "")
            radio_app = " / ".join(
                part
                for part in (
                    str(row.get("radio_id", "") or "").strip(),
                    str(row.get("app_instance_id", "") or "").strip(),
                )
                if part
            )
            values = [
                row.get("title", "") or row.get("source_id", ""),
                row.get("state_label", "") or row.get("state", ""),
                row.get("source_kind", ""),
                radio_app,
                row.get("location", ""),
                self._runtime_source_cached_traffic(row),
                row.get("action_hint", ""),
            ]
            for col, value in enumerate(values):
                self.runtime_sources_table.setItem(
                    row_idx,
                    col,
                    self._item(value, severity=severity if col == 1 else ""),
                )
        self.runtime_sources_table.resizeRowsToContents()
        self._update_runtime_sources_empty_state()
        self._render_runtime_source_detail()

    def _update_runtime_sources_empty_state(self) -> None:
        if not hasattr(self, "runtime_sources_empty_label") or not hasattr(self, "runtime_sources_table"):
            return
        has_rows = self.runtime_sources_table.rowCount() > 0
        self.runtime_sources_empty_label.setVisible(not has_rows)
        self.runtime_sources_table.setVisible(has_rows)
        if hasattr(self, "runtime_source_detail_label"):
            self.runtime_source_detail_label.setVisible(has_rows)
        if hasattr(self, "runtime_source_open_related_btn"):
            self.runtime_source_open_related_btn.setVisible(has_rows)
            if not has_rows:
                self.runtime_source_open_related_btn.setEnabled(False)

    def _runtime_source_detail_text(self, row: Mapping[str, object]) -> str:
        title = str(row.get("title", "") or row.get("source_id", "") or "Runtime source").strip()
        state = str(row.get("state_label", "") or row.get("state", "") or "Observed").strip()
        kind = str(row.get("source_kind", "") or "").strip()
        location = str(row.get("location", "") or "").strip()
        detail = str(row.get("detail", "") or "").strip()
        action = str(row.get("action_hint", "") or "").strip()
        last_activity = str(row.get("last_activity_label", "") or "").strip()
        metadata = row.get("metadata", {})
        if not isinstance(metadata, Mapping):
            metadata = {}
        freshness = self._runtime_source_freshness_text(metadata)
        radio_app = " / ".join(
            part
            for part in (
                str(row.get("radio_id", "") or "").strip(),
                str(row.get("app_instance_id", "") or "").strip(),
            )
            if part
        )
        parts = [f"{title}: {state}"]
        if kind:
            parts.append(f"Kind: {kind}")
        if radio_app:
            parts.append(f"Radio/App: {radio_app}")
        if location:
            parts.append(f"Path/Endpoint: {location}")
        cached = self._runtime_source_cached_traffic(row)
        if cached:
            parts.append(f"Projected Data: {cached}")
        if last_activity:
            parts.append(f"Last Source Decision: {last_activity}")
        if freshness:
            parts.append(f"Freshness: {freshness}")
        if detail and detail != cached:
            parts.append(f"Detail: {detail}")
        if action:
            parts.append(f"Suggested Fix: {action}")
        return "\n".join(parts)

    @staticmethod
    def _runtime_source_freshness_text(metadata: Mapping[str, object]) -> str:
        label = str(metadata.get("freshness_label", "") or "").strip()
        if not label:
            return ""
        try:
            age = float(metadata.get("freshness_age_sec", 0.0) or 0.0)
        except Exception:
            age = 0.0
        try:
            stale_after = float(metadata.get("freshness_stale_sec", 0.0) or 0.0)
        except Exception:
            stale_after = 0.0
        parts = [label]
        if age > 0:
            parts.append(f"checked {StationHealthTab._runtime_source_duration_label(age)} ago")
        if stale_after > 0 and str(metadata.get("freshness_state", "") or "").strip().lower() != "stale":
            parts.append(f"stale after {StationHealthTab._runtime_source_duration_label(stale_after)}")
        return " - ".join(parts)

    @staticmethod
    def _runtime_source_duration_label(seconds: float) -> str:
        seconds = max(0.0, float(seconds or 0.0))
        if seconds < 60:
            return f"{int(round(seconds))}s"
        minutes = seconds / 60.0
        if minutes < 60:
            return f"{int(round(minutes))} min"
        hours = minutes / 60.0
        if hours < 24:
            return f"{hours:.1f} h"
        days = hours / 24.0
        return f"{int(round(days))} days"

    def _render_runtime_source_detail(self) -> None:
        if not hasattr(self, "runtime_source_detail_label") or not hasattr(self, "runtime_sources_table"):
            return
        rows = list(getattr(self, "_runtime_source_rows", []) or [])
        current = self.runtime_sources_table.currentRow()
        if current < 0 or current >= len(rows):
            self.runtime_source_detail_label.setText("Select a runtime source to review details.")
            self._set_open_related_runtime_source_enabled(False)
            return
        row = rows[current]
        if not isinstance(row, Mapping):
            self.runtime_source_detail_label.setText("Select a runtime source to review details.")
            self._set_open_related_runtime_source_enabled(False)
            return
        self.runtime_source_detail_label.setText(self._runtime_source_detail_text(row))
        self._set_open_related_runtime_source_enabled(bool(self._runtime_source_related_payload(row)))

    def _set_open_related_runtime_source_enabled(self, enabled: bool) -> None:
        btn = getattr(self, "runtime_source_open_related_btn", None)
        if btn is not None:
            btn.setEnabled(bool(enabled))

    def _set_open_related_health_item_enabled(self, enabled: bool) -> None:
        btn = getattr(self, "health_open_related_btn", None)
        if btn is not None:
            btn.setEnabled(bool(enabled))

    def _selected_health_item(self, row: int | None = None) -> Mapping[str, object] | None:
        if not hasattr(self, "table"):
            return None
        if row is None:
            row = self.table.currentRow()
        try:
            idx = int(row)
        except Exception:
            idx = -1
        items = list(self._last_summary.get("items", []) or [])
        if idx < 0 or idx >= len(items):
            return None
        item = items[idx]
        return item if isinstance(item, Mapping) else None

    def _health_row_related_payload(self, row: int | None = None) -> dict[str, object]:
        item = self._selected_health_item(row)
        if item is None:
            return {}
        dependency = str(item.get("dependency", "") or "").strip()
        scope = str(item.get("scope", "") or "").strip()
        if dependency == "Schedule Assignment RF Guard":
            return {
                "target": "settings",
                "settings_nav_context": "radios",
                "health_key": "schedule_assignments",
                "scope": scope,
                "dependency": dependency,
            }
        if dependency == "RF Guard":
            return {
                "target": "settings",
                "settings_nav_context": "radios",
                "health_key": "radio_profiles",
                "scope": scope,
                "dependency": dependency,
            }
        return {}

    def _on_open_related_health_item(self) -> None:
        payload = self._health_row_related_payload()
        if payload:
            self.related_view_requested.emit(payload)

    def _selected_runtime_source_row(self) -> Mapping[str, object] | None:
        if not hasattr(self, "runtime_sources_table"):
            return None
        rows = list(getattr(self, "_runtime_source_rows", []) or [])
        current = self.runtime_sources_table.currentRow()
        if current < 0 or current >= len(rows):
            return None
        row = rows[current]
        return row if isinstance(row, Mapping) else None

    @staticmethod
    def _runtime_source_related_payload(row: Mapping[str, object]) -> dict[str, object]:
        source_kind = str(row.get("source_kind", "") or "").strip()
        text = " ".join(
            str(row.get(key, "") or "").strip().lower()
            for key in ("source_kind", "title", "source_id", "app_instance_id")
        )
        health_key = "radio_profiles"
        context = "radios"
        if any(token in text for token in ("js8", "spotter", "commstat", "directed.txt", "all.txt")):
            health_key = "js8call"
        elif any(token in text for token in ("flmsg", "flamp", "fldigi", "fast light", "nbems")):
            health_key = "fast_light"
        elif "varac" in text:
            health_key = "varac"
        elif any(token in text for token in ("message auth", "msgauth", "gpg", "key")):
            context = "main"
            health_key = "message_auth"
        return {
            "target": "settings",
            "settings_nav_context": context,
            "health_key": health_key,
            "source_id": str(row.get("source_id", "") or ""),
            "source_kind": source_kind,
            "title": str(row.get("title", "") or ""),
            "radio_id": row.get("radio_id", "") or "",
            "app_instance_id": row.get("app_instance_id", "") or "",
            "location": row.get("location", "") or "",
            "state": row.get("state", "") or "",
            "state_label": row.get("state_label", "") or "",
        }

    def _on_open_related_runtime_source(self) -> None:
        row = self._selected_runtime_source_row()
        if row is None:
            return
        payload = self._runtime_source_related_payload(row)
        if payload:
            self.related_view_requested.emit(payload)

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
