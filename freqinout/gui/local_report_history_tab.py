from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.local_ops_store import list_local_reports
from freqinout.core.logger import log
from freqinout.core.message_intelligence import TOPIC_TAXONOMY
from freqinout.core.settings_manager import SettingsManager
from freqinout.gui.theme import button_style, resolve_theme


class LocalReportHistoryTab(QWidget):
    """
    Local report history for voice/local-network observations.

    This intentionally stays separate from HF Operator History. HF history is a
    callsign/activity model; this view is a local field-report review surface.
    """

    COL_STATUS = 0
    COL_AGE = 1
    COL_FROM = 2
    COL_SOURCE = 3
    COL_TOPICS = 4
    COL_SUBJECT = 5
    COL_LOCATION = 6

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._rows: List[Dict[str, Any]] = []
        self._row_by_id: Dict[int, Dict[str, Any]] = {}
        self._build_ui()
        self.refresh_reports()
        self.apply_theme()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Local Report History</h3>"))
        header.addStretch()
        self.refresh_btn = QPushButton("Refresh")
        header.addWidget(self.refresh_btn)
        layout.addLayout(header)

        filter_row = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search reports: callsign, topic, subject, location, keyword...")
        self.search_edit.setToolTip("Search local reports by callsign, operator name, subject, body text, city/county/state, or grid.")
        filter_row.addWidget(self.search_edit, stretch=1)
        self.callsign_edit = QLineEdit()
        self.callsign_edit.setPlaceholderText("Callsign")
        filter_row.addWidget(self.callsign_edit)
        self.topic_combo = QComboBox()
        self.topic_combo.addItem("All Topics", "")
        for topic in TOPIC_TAXONOMY:
            self.topic_combo.addItem(topic, topic)
        filter_row.addWidget(self.topic_combo)
        self.status_combo = QComboBox()
        self.status_combo.addItem("All Status", "")
        for status in ("INFO", "WATCH", "PRIORITY", "EMERGENCY"):
            self.status_combo.addItem(status, status)
        filter_row.addWidget(self.status_combo)
        self.clear_filters_btn = QPushButton("Clear Filters")
        filter_row.addWidget(self.clear_filters_btn)
        layout.addLayout(filter_row)

        summary_row = QHBoxLayout()
        self.summary_total_label = QLabel("Reports: 0")
        self.summary_priority_label = QLabel("Priority/Emergency: 0")
        self.summary_newest_label = QLabel("Newest: --")
        self.summary_filters_label = QLabel("Filters: none")
        self.summary_filters_label.setWordWrap(True)
        summary_row.addWidget(self.summary_total_label)
        summary_row.addWidget(self.summary_priority_label)
        summary_row.addWidget(self.summary_newest_label)
        summary_row.addWidget(self.summary_filters_label, stretch=1)
        layout.addLayout(summary_row)

        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(
            ["Status", "Age", "From", "Source", "Topics", "Subject", "Location"]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSortingEnabled(True)
        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(self.COL_STATUS, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_AGE, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_FROM, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_SOURCE, QHeaderView.ResizeToContents)
        hv.setSectionResizeMode(self.COL_TOPICS, QHeaderView.Stretch)
        hv.setSectionResizeMode(self.COL_SUBJECT, QHeaderView.Stretch)
        hv.setSectionResizeMode(self.COL_LOCATION, QHeaderView.ResizeToContents)
        layout.addWidget(self.table, stretch=2)

        self.detail_title = QLabel("Select a report")
        layout.addWidget(self.detail_title)
        self.detail_text = QTextEdit()
        self.detail_text.setReadOnly(True)
        self.detail_text.setMinimumHeight(150)
        layout.addWidget(self.detail_text, stretch=1)

        self.refresh_btn.clicked.connect(self.refresh_reports)
        self.clear_filters_btn.clicked.connect(self._clear_filters)
        self.search_edit.textChanged.connect(self.refresh_reports)
        self.callsign_edit.textChanged.connect(self.refresh_reports)
        self.topic_combo.currentIndexChanged.connect(self.refresh_reports)
        self.status_combo.currentIndexChanged.connect(self.refresh_reports)
        self.table.itemSelectionChanged.connect(self._on_selection_changed)

    def apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.refresh_btn.setStyleSheet(button_style("primary", theme))
        self.clear_filters_btn.setStyleSheet(button_style("muted", theme))

    def on_settings_saved(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        self.apply_theme()

    def show_callsign(self, callsign: str) -> None:
        self.callsign_edit.setText(str(callsign or "").strip().upper())
        self.refresh_reports()

    def refresh_reports(self, *_args) -> None:
        try:
            rows = list_local_reports(
                limit=500,
                callsign=self.callsign_edit.text().strip(),
                topic=str(self.topic_combo.currentData() or ""),
                status=str(self.status_combo.currentData() or ""),
                query=self.search_edit.text().strip(),
            )
        except Exception as e:
            log.error("LocalReportHistoryTab: failed to load local reports: %s", e)
            rows = []
        self._rows = rows
        self._row_by_id = {int(row.get("id", 0) or 0): row for row in rows if int(row.get("id", 0) or 0) > 0}
        self._update_summary(rows)
        self._populate_table(rows)

    def _clear_filters(self) -> None:
        self.search_edit.clear()
        self.callsign_edit.clear()
        self.topic_combo.setCurrentIndex(0)
        self.status_combo.setCurrentIndex(0)
        self.refresh_reports()

    def _populate_table(self, rows: List[Dict[str, Any]]) -> None:
        sorting_enabled = self.table.isSortingEnabled()
        self.table.setSortingEnabled(False)
        try:
            self.table.setRowCount(0)
            for row in rows:
                report_id = int(row.get("id", 0) or 0)
                r = self.table.rowCount()
                self.table.insertRow(r)
                values = [
                    str(row.get("status", "")),
                    self._age_text(str(row.get("created_utc", ""))),
                    str(row.get("callsign", "")),
                    self._source_text(row),
                    self._topics_text(row),
                    str(row.get("subject", "")) or self._body_preview(row),
                    self._location_text(row),
                ]
                for c, value in enumerate(values):
                    item = QTableWidgetItem(value)
                    if c == self.COL_STATUS:
                        item.setData(Qt.UserRole, report_id)
                    item.setToolTip(value)
                    self.table.setItem(r, c, item)
            if self.table.rowCount() > 0:
                self.table.selectRow(0)
            else:
                self.detail_title.setText("No local reports")
                self.detail_text.clear()
        finally:
            self.table.setSortingEnabled(sorting_enabled)

    def _selected_report(self) -> Optional[Dict[str, Any]]:
        rows = self.table.selectionModel().selectedRows() if self.table.selectionModel() else []
        if not rows:
            return None
        item = self.table.item(rows[0].row(), self.COL_STATUS)
        report_id = int(item.data(Qt.UserRole) or 0) if item is not None else 0
        return self._row_by_id.get(report_id)

    def _update_summary(self, rows: List[Dict[str, Any]]) -> None:
        total = len(rows)
        priority_count = sum(
            1
            for row in rows
            if str(row.get("status", "")).strip().upper() in {"PRIORITY", "EMERGENCY"}
        )
        newest = self._age_text(str(rows[0].get("created_utc", ""))) if rows else "--"
        filters = self._active_filter_text()
        self.summary_total_label.setText(f"Reports: {total}")
        self.summary_priority_label.setText(f"Priority/Emergency: {priority_count}")
        self.summary_newest_label.setText(f"Newest: {newest}")
        self.summary_filters_label.setText(f"Filters: {filters}")

    def _active_filter_text(self) -> str:
        filters: List[str] = []
        query = self.search_edit.text().strip()
        callsign = self.callsign_edit.text().strip().upper()
        topic = str(self.topic_combo.currentData() or "").strip()
        status = str(self.status_combo.currentData() or "").strip()
        if query:
            filters.append(f"search {query}")
        if callsign:
            filters.append(f"from {callsign}")
        if topic:
            filters.append(f"topic {topic}")
        if status:
            filters.append(f"status {status}")
        return ", ".join(filters) if filters else "none"

    def _on_selection_changed(self) -> None:
        row = self._selected_report()
        if not row:
            self.detail_title.setText("Select a report")
            self.detail_text.clear()
            return
        title = str(row.get("subject", "")).strip() or self._body_preview(row)
        self.detail_title.setText(
            f"{row.get('status', 'INFO')} | {row.get('callsign', '')} | {title or 'Local report'}"
        )
        lines = [
            f"From: {row.get('callsign', '') or row.get('from_name', '')}",
            f"To: {self._target_text(row) or 'Local'}",
            f"Source: {self._source_text(row)}",
            f"When: {self._age_text(str(row.get('created_utc', '')))} ({row.get('created_utc', '')})",
            f"Status: {row.get('status', '')}",
            f"Confidence: {self._confirmation_text(row)}",
            f"Topics: {self._topics_text(row) or 'None'}",
        ]
        location = self._location_text(row)
        if location:
            lines.append(f"Location: {location}")
        subject = str(row.get("subject", "")).strip()
        if subject:
            lines.extend(["", f"Subject: {subject}"])
        body = str(row.get("body", "")).strip()
        if body:
            lines.extend(["", body])
        self.detail_text.setPlainText("\n".join(lines))

    @staticmethod
    def _parse_utc(value: str) -> Optional[dt.datetime]:
        txt = str(value or "").strip()
        if not txt:
            return None
        try:
            parsed = dt.datetime.fromisoformat(txt.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)

    @classmethod
    def _age_text(cls, value: str) -> str:
        parsed = cls._parse_utc(value)
        if parsed is None:
            return ""
        seconds = max(0, int((dt.datetime.now(dt.timezone.utc) - parsed).total_seconds()))
        if seconds < 60:
            return f"{seconds}s"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes} min"
        hours = minutes // 60
        if hours < 24:
            mins = minutes % 60
            return f"{hours}:{mins:02d} h"
        days = hours // 24
        return f"{days} days"

    @staticmethod
    def _body_preview(row: Dict[str, Any]) -> str:
        body = str(row.get("body", "")).strip().replace("\n", " ")
        return body if len(body) <= 80 else body[:77] + "..."

    @staticmethod
    def _topics_text(row: Dict[str, Any]) -> str:
        return ", ".join(str(topic) for topic in row.get("topics", []) if str(topic).strip())

    @staticmethod
    def _source_text(row: Dict[str, Any]) -> str:
        source = str(row.get("source_kind", "")).strip().upper()
        channel = str(row.get("source_channel", "")).strip()
        return f"{source} {channel}".strip()

    @staticmethod
    def _target_text(row: Dict[str, Any]) -> str:
        session = str(row.get("net_session_id", "")).strip()
        return session

    @staticmethod
    def _location_text(row: Dict[str, Any]) -> str:
        city = str(row.get("city", "")).strip()
        county = str(row.get("county", "")).strip()
        state = str(row.get("state", "")).strip().upper()
        grid = str(row.get("grid", "")).strip().upper()
        place = ", ".join(part for part in (city, county, state) if part)
        if place and grid:
            return f"{place} / {grid}"
        return place or grid

    @staticmethod
    def _confirmation_text(row: Dict[str, Any]) -> str:
        value = str(row.get("confirmed_state", "")).strip().replace("_", " ").title()
        return value or "Unconfirmed"
