"""Reusable bounded resource picker and presentation helpers.

The picker is deliberately a catalog reader.  It does not create schema,
perform imports, or make runtime/radio decisions.
"""

from __future__ import annotations

from typing import Callable, Iterable

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.resource_catalog_models import FrequencyResource, NetDirectorySession
from freqinout.core.resource_catalog_store import MAX_RESULTS, ResourceCatalogStore


def frequency_where_text(resource: FrequencyResource) -> str:
    """Return a concise display value; keys and Hz remain the comparison API."""
    if resource.resource_kind == "band_range":
        return f"{resource.lower_hz or 0:,}–{resource.upper_hz or 0:,} Hz"
    receive = resource.receive_hz or resource.center_hz
    transmit = resource.transmit_hz
    if receive is None:
        return "Frequency unavailable"
    if transmit is not None and transmit != receive:
        return f"RX {receive:,} Hz · TX {transmit:,} Hz"
    return f"{receive:,} Hz"


def resource_status_text(resource: FrequencyResource) -> str:
    if resource.retired:
        return "Retired"
    if not resource.active:
        return "Inactive"
    return "Active"


def session_when_text(session: NetDirectorySession) -> str:
    parts = [part for part in (session.recurrence, session.local_start_time, session.timezone) if part]
    return " · ".join(parts) or "Timing not published"


class ResourcePicker(QDialog):
    """Modal, bounded picker usable by Local Nets and HF subscription flows."""

    frequency_selected = Signal(object)

    def __init__(self, store: ResourceCatalogStore, parent: QWidget | None = None, *, service: str | None = None) -> None:
        super().__init__(parent)
        self.store = store
        self.service = service
        self.selected_resource: FrequencyResource | None = None
        self.setWindowTitle("Choose Frequency Resource")
        self.setMinimumSize(620, 420)
        self._build_ui()
        self.refresh_results()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        heading = QLabel("Choose a reusable frequency resource")
        heading.setStyleSheet("font-weight: 700; font-size: 16px;")
        layout.addWidget(heading)
        copy = QLabel("Search the station catalog. Selecting a resource does not tune a radio or evaluate transmit eligibility.")
        copy.setWordWrap(True)
        layout.addWidget(copy)
        search_row = QHBoxLayout()
        self.search_edit = QLineEdit(self)
        self.search_edit.setPlaceholderText("Search label, channel, frequency, locality, or coverage")
        self.search_edit.setAccessibleName("Search frequency catalog")
        self.search_edit.returnPressed.connect(self.refresh_results)
        search_row.addWidget(self.search_edit, 1)
        refresh = QPushButton("Search", self)
        refresh.clicked.connect(self.refresh_results)
        search_row.addWidget(refresh)
        layout.addLayout(search_row)
        self.table = QTableWidget(0, 4, self)
        self.table.setHorizontalHeaderLabels(["Label", "Service", "Where", "Status"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.table.itemDoubleClicked.connect(lambda *_: self._accept_selected())
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table, 1)
        self.status_label = QLabel("")
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)
        buttons = QDialogButtonBox(QDialogButtonBox.Cancel, self)
        choose = buttons.addButton("Choose", QDialogButtonBox.AcceptRole)
        choose.clicked.connect(self._accept_selected)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def refresh_results(self) -> None:
        rows = self.store.list_frequencies(search=self.search_edit.text(), service=self.service, limit=MAX_RESULTS)
        self.table.setRowCount(len(rows))
        for row_index, resource in enumerate(rows):
            values = (resource.label, resource.service, frequency_where_text(resource), resource_status_text(resource))
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.UserRole, resource)
                self.table.setItem(row_index, column, item)
        self.status_label.setText(f"Showing {len(rows)} result{'s' if len(rows) != 1 else ''}; catalog results are limited to {MAX_RESULTS}.")
        if rows:
            self.table.selectRow(0)

    def _accept_selected(self) -> None:
        selected = self.table.selectedItems()
        if not selected:
            self.status_label.setText("Choose one frequency resource first.")
            return
        resource = self.table.item(selected[0].row(), 0).data(Qt.UserRole)
        if not isinstance(resource, FrequencyResource):
            return
        self.selected_resource = resource
        self.frequency_selected.emit(resource)
        self.accept()


def choose_frequency_resource(store: ResourceCatalogStore, parent: QWidget | None = None, *, service: str | None = None) -> FrequencyResource | None:
    picker = ResourcePicker(store, parent, service=service)
    return picker.selected_resource if picker.exec() == QDialog.Accepted else None


__all__ = [
    "ResourcePicker",
    "choose_frequency_resource",
    "frequency_where_text",
    "resource_status_text",
    "session_when_text",
]
