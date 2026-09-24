"""Source-first, non-mutating HF Net subscription review dialog.

The dialog only creates a draft.  ``NetScheduleTab`` remains responsible for
its existing validation, RF Guard, plan reprojection, and scheduler refresh
when the operator explicitly saves the named HF Net schedule.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from PySide6.QtCore import Qt, QEvent
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QScrollArea,
    QBoxLayout,
)

from freqinout.core.resource_catalog_models import CatalogValidationError, FrequencyResource, NetDirectoryEntry, NetDirectorySession, ReadOnlyResourceError
from freqinout.gui.frequency_catalog_view import hz_from_text, new_frequency_resource_key
from freqinout.gui.net_directory_view import new_net_entry_key, new_net_session_key
from freqinout.core.resource_catalog_store import MAX_RESULTS, ResourceCatalogStore
from freqinout.core.schedule_source_sets import HF_NET_SOURCE_CATEGORY, HF_NET_SOURCE_SETS_KEY, source_sets_for_category
from freqinout.gui.resource_picker import frequency_where_text, session_when_text
from freqinout.gui.theme import active_app_theme, button_height_for_font, button_style, control_height_for_font, font_derived_widget_height, label_style


@dataclass(frozen=True, slots=True)
class HfNetSubscriptionSelection:
    """UI-only selection; canonical schedule drafts come from the core service."""

    destination_id: str
    destination_name: str
    session_keys: tuple[str, ...]


def named_hf_net_destinations(settings: object) -> tuple[tuple[str, str], ...]:
    """Return only saved/named HF Net destinations; live schedule is excluded."""
    rows = source_sets_for_category(settings, HF_NET_SOURCE_SETS_KEY, HF_NET_SOURCE_CATEGORY)
    return tuple((str(row.get("id") or "").strip(), str(row.get("name") or "").strip()) for row in rows if str(row.get("id") or "").strip() and str(row.get("name") or "").strip())


class HfNetSubscriptionDialog(QDialog):
    """Choose one or more published sessions and a named HF schedule target."""

    def __init__(self, store: ResourceCatalogStore, settings: object, parent: QWidget | None = None, *, initial_session_keys: Iterable[str] = ()) -> None:
        super().__init__(parent)
        self.store = store
        self.settings = settings
        self.initial_session_keys = tuple(dict.fromkeys(str(key).strip() for key in initial_session_keys if str(key).strip()))
        self.selection: HfNetSubscriptionSelection | None = None
        self._entries: dict[str, NetDirectoryEntry] = {}
        self._build_ui()
        self._load_destinations()
        self._load_entries()
        self._select_initial_sessions()

    def _build_ui(self) -> None:
        self.setWindowTitle("Add HF Net")
        layout = QVBoxLayout(self)
        title = QLabel("Add HF Net from Net Directory")
        self.title_label = title
        layout.addWidget(title)
        copy = QLabel("Select published sessions, choose the named HF Net schedule that will receive them, then review the draft. Saving remains an explicit HF Nets action.")
        copy.setWordWrap(True)
        layout.addWidget(copy)
        destination_row = QHBoxLayout()
        self.destination_row = destination_row
        destination_row.addWidget(QLabel("Destination schedule:"))
        self.destination_combo = QComboBox(self)
        self.destination_combo.setAccessibleName("Named HF Net schedule destination")
        destination_row.addWidget(self.destination_combo, 1)
        layout.addLayout(destination_row)
        search_row = QHBoxLayout()
        self.search_row = search_row
        self.search_edit = QLineEdit(self)
        self.search_edit.setPlaceholderText("Search known nets")
        self.search_edit.returnPressed.connect(self._load_entries)
        search_row.addWidget(self.search_edit, 1)
        search = QPushButton("Search", self)
        search.clicked.connect(self._load_entries)
        search_row.addWidget(search)
        self.create_net_btn = QPushButton("Create New Net…", self)
        self.create_net_btn.clicked.connect(lambda: self._create_net(private_one_time=False))
        self.private_net_btn = QPushButton("Station-private one-time…", self)
        self.private_net_btn.clicked.connect(lambda: self._create_net(private_one_time=True))
        search_row.addWidget(self.create_net_btn)
        search_row.addWidget(self.private_net_btn)
        layout.addLayout(search_row)
        self.entry_table = QTableWidget(0, 3, self)
        self.entry_table.setHorizontalHeaderLabels(["Net", "Scope", "Source"])
        self.entry_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.entry_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.entry_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.entry_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.entry_table.horizontalHeader().setStretchLastSection(True)
        self.entry_table.itemSelectionChanged.connect(self._load_selected_entry_sessions)
        layout.addWidget(self.entry_table, 1)
        self.session_table = QTableWidget(0, 5, self)
        self.session_table.setHorizontalHeaderLabels(["Use", "When", "Service", "Where", "Status"])
        self.session_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.session_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.session_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.session_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.session_table.horizontalHeader().setStretchLastSection(True)
        self.session_table.itemChanged.connect(lambda *_args: self._update_review_copy())
        layout.addWidget(self.session_table, 1)
        self.review_label = QLabel("")
        self.review_label.setWordWrap(True)
        layout.addWidget(self.review_label)
        buttons = QDialogButtonBox(QDialogButtonBox.Cancel, self)
        self.review_btn = buttons.addButton("Review & Add Draft", QDialogButtonBox.AcceptRole)
        self.review_btn.clicked.connect(self._accept_draft)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.apply_theme()

    def apply_theme(self) -> None:
        theme = active_app_theme()
        self.title_label.setStyleSheet(label_style("text", theme, weight=700))
        self.review_label.setStyleSheet(label_style("muted", theme))
        self.entry_table.setStyleSheet(f"QTableWidget {{ gridline-color: {theme['border']}; }}")
        self.session_table.setStyleSheet(f"QTableWidget {{ gridline-color: {theme['border']}; }}")
        for control in (self.destination_combo, self.search_edit):
            control.setMinimumHeight(control_height_for_font(control))
        for button in (self.review_btn, self.create_net_btn, self.private_net_btn):
            button.setMinimumHeight(button_height_for_font(button))
        self.review_btn.setStyleSheet(button_style("primary", theme))
        self.create_net_btn.setStyleSheet(button_style("secondary", theme))
        self.private_net_btn.setStyleSheet(button_style("muted", theme))
        for table in (self.entry_table, self.session_table):
            table.verticalHeader().setDefaultSectionSize(font_derived_widget_height(table))
        self._apply_responsive_layout()

    def _apply_responsive_layout(self) -> None:
        compact = self.width() > 0 and self.width() < max(900, self.fontMetrics().horizontalAdvance("Add HF Net") * 24)
        direction = QBoxLayout.TopToBottom if compact else QBoxLayout.LeftToRight
        self.destination_row.setDirection(direction)
        self.search_row.setDirection(direction)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._apply_responsive_layout()

    def changeEvent(self, event) -> None:
        super().changeEvent(event)
        if event.type() == QEvent.FontChange:
            self.apply_theme()

    def _load_destinations(self) -> None:
        self.destination_combo.clear()
        for destination_id, name in named_hf_net_destinations(self.settings):
            self.destination_combo.addItem(name, destination_id)
        if not self.destination_combo.count():
            self.destination_combo.addItem("No named HF Net schedule available", "")
            self.review_btn.setEnabled(False)
            self.review_label.setText("Create and save a named HF Net schedule before subscribing to directory sessions.")

    def _load_entries(self) -> None:
        entries = self.store.list_net_entries(search=self.search_edit.text(), active=True, limit=MAX_RESULTS)
        self._entries = {entry.net_entry_key: entry for entry in entries}
        self.entry_table.setRowCount(len(entries))
        for row_index, entry in enumerate(entries):
            for column, value in enumerate((entry.name, entry.scope or "—", entry.source_key)):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.UserRole, entry.net_entry_key)
                self.entry_table.setItem(row_index, column, item)
        if entries:
            self.entry_table.selectRow(0)

    def _load_selected_entry_sessions(self) -> None:
        items = self.entry_table.selectedItems()
        if not items:
            return
        entry_key = self.entry_table.item(items[0].row(), 0).data(Qt.UserRole)
        sessions = self.store.list_sessions(net_entry_key=str(entry_key or ""), active=True, limit=MAX_RESULTS)
        self.session_table.setRowCount(len(sessions))
        for row_index, session in enumerate(sessions):
            frequency = self.store.get_frequency(session.frequency_resource_key) if session.frequency_resource_key else None
            checkbox = QTableWidgetItem()
            checkbox.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            checkbox.setCheckState(Qt.Checked if session.net_session_key in self.initial_session_keys else Qt.Unchecked)
            checkbox.setData(Qt.UserRole, session)
            self.session_table.setItem(row_index, 0, checkbox)
            values = (session_when_text(session), session.service, frequency_where_text(frequency) if frequency else "Frequency unavailable", "Retired" if session.retired else "Published")
            for column, value in enumerate(values, start=1):
                self.session_table.setItem(row_index, column, QTableWidgetItem(value))
        self._update_review_copy()

    def _select_initial_sessions(self) -> None:
        if not self.initial_session_keys:
            return
        first = self.store.get_session(self.initial_session_keys[0])
        if first:
            for row in range(self.entry_table.rowCount()):
                if self.entry_table.item(row, 0).data(Qt.UserRole) == first.net_entry_key:
                    self.entry_table.selectRow(row)
                    break

    def _selected_sessions(self) -> tuple[NetDirectorySession, ...]:
        selected = []
        for row in range(self.session_table.rowCount()):
            item = self.session_table.item(row, 0)
            if item and item.checkState() == Qt.Checked:
                session = item.data(Qt.UserRole)
                if isinstance(session, NetDirectorySession):
                    selected.append(session)
        return tuple(selected)

    def _update_review_copy(self) -> None:
        count = len(self._selected_sessions())
        destination = self.destination_combo.currentText() or "no destination"
        self.review_label.setText(f"Review: {count} published net meeting(s) will be added as drafts to '{destination}'. Confirm recurrence/time, target/radio, early check-in, mode, frequency, and conflict policy in HF Nets before Save Schedule.")

    def _accept_draft(self) -> None:
        sessions = self._selected_sessions()
        destination_id = str(self.destination_combo.currentData() or "").strip()
        if not destination_id:
            self.review_label.setText("A named HF Net schedule destination is required.")
            return
        if not sessions:
            self.review_label.setText("Select at least one published session to add.")
            return
        self.selection = HfNetSubscriptionSelection(destination_id, self.destination_combo.currentText(), tuple(session.net_session_key for session in sessions))
        self.accept()

    def _create_net(self, *, private_one_time: bool) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle("Station-private one-time net" if private_one_time else "Create Net Directory Entry")
        dialog_layout = QVBoxLayout(dialog)
        form_scroll = QScrollArea(dialog)
        form_scroll.setWidgetResizable(True)
        form_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        form_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        form_body = QWidget(form_scroll)
        form = QFormLayout(form_body)
        form_scroll.setWidget(form_body)
        dialog_layout.addWidget(form_scroll)
        name = QLineEdit(dialog); name.setPlaceholderText("Net name")
        frequency_key = QLineEdit(dialog); frequency_key.setPlaceholderText("Existing frequency resource key (optional)")
        frequency_hz = QLineEdit(dialog); frequency_hz.setPlaceholderText("Create station frequency in integer Hz if no key")
        day = QLineEdit(dialog); day.setPlaceholderText("Monday or ALL")
        start = QLineEdit(dialog); start.setPlaceholderText("HH:MM")
        timezone = QLineEdit(dialog); timezone.setText("UTC")
        for label, field in (("Net name", name), ("Frequency key", frequency_key), ("Frequency Hz", frequency_hz), ("Day (UTC)", day), ("Start", start), ("Timezone", timezone)):
            form.addRow(label, field)
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel, dialog)
        buttons.accepted.connect(dialog.accept); buttons.rejected.connect(dialog.reject); form.addRow(buttons)
        if dialog.exec() != QDialog.Accepted:
            return
        try:
            source_key = self.store.ensure_station_source().source_key
            key = frequency_key.text().strip()
            if not key:
                center_hz = hz_from_text(frequency_hz.text())
                if center_hz is None:
                    raise CatalogValidationError("Provide an existing frequency key or integer frequency Hz.")
                frequency = self.store.create_frequency(FrequencyResource(new_frequency_resource_key(), source_key, "simplex", "AMATEUR", f"{name.text().strip()} frequency", center_hz=center_hz))
                key = frequency.frequency_resource_key
            entry = self.store.create_net_entry(NetDirectoryEntry(new_net_entry_key(), source_key, name.text(), scope="Station private" if private_one_time else "Station", description="Station-private one-time schedule" if private_one_time else None))
            session = self.store.create_session(NetDirectorySession(new_net_session_key(), entry.net_entry_key, source_key, "AMATEUR", key, recurrence="One-time" if private_one_time else "Weekly", day_utc=day.text().strip() or None, local_start_time=start.text().strip() or None, duration_minutes=60, timezone=timezone.text().strip() or "UTC"))
        except (CatalogValidationError, ReadOnlyResourceError, ValueError) as exc:
            self.review_label.setText(f"Cannot create Net Directory meeting: {exc}")
            return
        self.initial_session_keys = (session.net_session_key,)
        self.search_edit.setText(entry.name)
        self._load_entries()
        self._select_initial_sessions()
        self.review_label.setText("Created a station-owned directory session. Review it and the named HF destination before adding the draft.")


__all__ = ["HfNetSubscriptionDialog", "HfNetSubscriptionSelection", "named_hf_net_destinations"]
