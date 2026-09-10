"""Bounded Frequency Catalog workspace for Tools & Resources."""

from __future__ import annotations

from dataclasses import replace
import uuid

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.resource_catalog_models import CatalogValidationError, FrequencyResource, ReadOnlyResourceError, ReferencedResourceError
from freqinout.core.resource_catalog_store import (
    MAX_RESULTS,
    STATION_MANUAL_SOURCE_KEY,
    ResourceCatalogStore,
)
from freqinout.gui.resource_picker import (
    frequency_where_text,
    populate_source_combo,
    resource_status_text,
    source_display_label,
    source_display_labels,
)


def new_frequency_resource_key() -> str:
    return f"frequency_{uuid.uuid4()}"


def hz_from_text(value: str) -> int | None:
    """Parse an editor Hz field without accepting display-MHz comparison keys."""
    text = str(value or "").strip().replace(",", "")
    return int(text) if text else None


class FrequencyCatalogView(QWidget):
    """Frequency catalog browser/editor. All reads are bounded store calls."""

    def __init__(self, store: ResourceCatalogStore, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.store = store
        self._selected: FrequencyResource | None = None
        self._editing_key: str | None = None
        self._build_ui()
        self.refresh_results()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        title = QLabel("Frequency Catalog")
        title.setStyleSheet("font-weight: 700; font-size: 17px;")
        layout.addWidget(title)
        why = QLabel("Reusable station and reference frequency data. Reference guidance is advisory; it does not assess transmit authorization.")
        why.setWordWrap(True)
        layout.addWidget(why)
        filters = QHBoxLayout()
        self.search_edit = QLineEdit(self)
        self.search_edit.setPlaceholderText("Search label, channel, frequency, locality, or coverage")
        self.search_edit.setAccessibleName("Frequency catalog search")
        self.search_edit.returnPressed.connect(self.refresh_results)
        filters.addWidget(self.search_edit, 1)
        self.service_filter = QComboBox(self)
        self.service_filter.addItem("Service: All", None)
        self.service_filter.addItem("Amateur", "AMATEUR")
        self.service_filter.addItem("GMRS", "GMRS")
        self.service_filter.currentIndexChanged.connect(self.refresh_results)
        filters.addWidget(self.service_filter)
        self.status_filter = QComboBox(self)
        self.status_filter.addItem("Listing: Listed", True)
        self.status_filter.addItem("Retired", False)
        self.status_filter.addItem("All listings", None)
        self.status_filter.currentIndexChanged.connect(self.refresh_results)
        filters.addWidget(self.status_filter)
        refresh = QPushButton("Refresh", self)
        refresh.clicked.connect(self.refresh_results)
        filters.addWidget(refresh)
        layout.addLayout(filters)
        self.splitter = QSplitter(Qt.Horizontal, self)
        self.splitter.setChildrenCollapsible(False)
        layout.addWidget(self.splitter, 1)
        self.table = QTableWidget(0, 5, self.splitter)
        self.table.setHorizontalHeaderLabels(["Label", "Service", "Frequency", "Catalog source", "Listing"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.table.itemSelectionChanged.connect(self._selection_changed)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        detail = QWidget(self.splitter)
        detail_layout = QVBoxLayout(detail)
        detail_layout.setContentsMargins(8, 0, 0, 0)
        self.detail_label = QLabel("Select a frequency resource to review its catalog source and station impact.")
        self.detail_label.setWordWrap(True)
        self.detail_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        detail_layout.addWidget(self.detail_label)
        self.usage_label = QLabel("Used by: —")
        self.usage_label.setWordWrap(True)
        detail_layout.addWidget(self.usage_label)
        actions = QGridLayout()
        actions.setHorizontalSpacing(6)
        actions.setVerticalSpacing(6)
        self.new_btn = QPushButton("New", detail)
        self.edit_btn = QPushButton("Edit", detail)
        self.clone_btn = QPushButton("Clone", detail)
        self.retire_btn = QPushButton("Retire", detail)
        self.delete_btn = QPushButton("Delete", detail)
        for index, button in enumerate((self.new_btn, self.edit_btn, self.clone_btn, self.retire_btn, self.delete_btn)):
            actions.addWidget(button, index // 3, index % 3)
        for column in range(3):
            actions.setColumnStretch(column, 1)
        detail_layout.addLayout(actions)
        editor_frame = QFrame(detail)
        editor_frame.setFrameShape(QFrame.StyledPanel)
        editor_layout = QFormLayout(editor_frame)
        self.key_edit = QLineEdit(editor_frame)
        self.source_edit = QComboBox(editor_frame)
        self.source_edit.setAccessibleName("Catalog source")
        self.label_edit = QLineEdit(editor_frame)
        self.kind_edit = QComboBox(editor_frame)
        self.kind_edit.addItems(["simplex", "repeater", "channel", "band_range"])
        self.editor_service = QComboBox(editor_frame)
        self.editor_service.addItems(["AMATEUR", "GMRS"])
        self.center_hz_edit = QLineEdit(editor_frame)
        self.lower_hz_edit = QLineEdit(editor_frame)
        self.upper_hz_edit = QLineEdit(editor_frame)
        self.receive_hz_edit = QLineEdit(editor_frame)
        self.transmit_hz_edit = QLineEdit(editor_frame)
        self.band_edit = QLineEdit(editor_frame)
        self.channel_edit = QLineEdit(editor_frame)
        self.mode_edit = QLineEdit(editor_frame)
        self.notes_edit = QLineEdit(editor_frame)
        self.key_edit.setVisible(False)
        for label, widget in (("Catalog source", self.source_edit), ("Label", self.label_edit), ("Kind", self.kind_edit), ("Service", self.editor_service), ("Center frequency (Hz)", self.center_hz_edit), ("Lower frequency (Hz)", self.lower_hz_edit), ("Upper frequency (Hz)", self.upper_hz_edit), ("Receive frequency (Hz)", self.receive_hz_edit), ("Transmit frequency (Hz)", self.transmit_hz_edit), ("Band", self.band_edit), ("Channel", self.channel_edit), ("Mode", self.mode_edit), ("Notes", self.notes_edit)):
            editor_layout.addRow(label, widget)
        editor_actions = QHBoxLayout()
        self.save_btn = QPushButton("Save", editor_frame)
        self.cancel_btn = QPushButton("Cancel", editor_frame)
        editor_actions.addWidget(self.save_btn)
        editor_actions.addWidget(self.cancel_btn)
        editor_layout.addRow(editor_actions)
        detail_layout.addWidget(editor_frame)
        self.editor_frame = editor_frame
        self.status_label = QLabel("")
        self.status_label.setWordWrap(True)
        detail_layout.addWidget(self.status_label)
        detail_layout.addStretch(1)
        self.new_btn.clicked.connect(self.begin_new)
        self.edit_btn.clicked.connect(self.begin_edit)
        self.clone_btn.clicked.connect(self.begin_clone)
        self.retire_btn.clicked.connect(self.retire_selected)
        self.delete_btn.clicked.connect(self.delete_selected)
        self.save_btn.clicked.connect(self.save_editor)
        self.cancel_btn.clicked.connect(self.cancel_editor)
        self.editor_frame.setVisible(False)
        self._set_action_state()

    def refresh_results(self) -> None:
        rows = self.store.list_frequencies(
            search=self.search_edit.text(), service=self.service_filter.currentData(),
            active=self.status_filter.currentData(), limit=MAX_RESULTS,
        )
        self.table.setRowCount(len(rows))
        source_labels = source_display_labels(self.store, (row.source_key for row in rows))
        for row_index, resource in enumerate(rows):
            values = (
                resource.label,
                resource.service,
                frequency_where_text(resource),
                source_labels[resource.source_key],
                resource_status_text(resource),
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.UserRole, resource)
                self.table.setItem(row_index, column, item)
        self.status_label.setText(f"Showing {len(rows)} bounded result{'s' if len(rows) != 1 else ''} (maximum {MAX_RESULTS}).")
        if rows:
            self.table.selectRow(0)
        else:
            self._selected = None
            self.detail_label.setText("No matching frequency resources. Create a station-owned resource when a new local reference is needed.")
            self.usage_label.setText("Used by: —")
            self._set_action_state()

    def _selection_changed(self) -> None:
        selected = self.table.selectedItems()
        if not selected:
            return
        resource = self.table.item(selected[0].row(), 0).data(Qt.UserRole)
        if isinstance(resource, FrequencyResource):
            self._selected = resource
            usage = self.store.frequency_usage(resource.frequency_resource_key)
            fields = [
                f"{resource.label} · {resource.service} · {resource.resource_kind}",
                frequency_where_text(resource),
                f"Catalog source: {source_display_label(self.store, resource.source_key)} · {resource_status_text(resource)}",
                f"Mode: {resource.mode or '—'} · Tone: {resource.tone or '—'}",
                f"Notes: {resource.notes or '—'}",
            ]
            self.detail_label.setText("\n".join(fields))
            impacts = ", ".join(f"{name.replace('_', ' ')} {count}" for name, count in usage.by_kind.items()) or "none"
            self.usage_label.setText(f"Used by: {usage.total_references} ({impacts})")
            self._set_action_state()

    def _set_action_state(self) -> None:
        has_selection = self._selected is not None
        for button in (self.edit_btn, self.clone_btn, self.retire_btn, self.delete_btn):
            button.setEnabled(has_selection)

    def begin_new(self) -> None:
        self._editing_key = None
        self._fill_editor(None)
        self.key_edit.setText(new_frequency_resource_key())
        self.editor_frame.setVisible(True)
        self.key_edit.setFocus()

    def begin_edit(self) -> None:
        if not self._selected:
            return
        self._editing_key = self._selected.frequency_resource_key
        self._fill_editor(self._selected)
        self.editor_frame.setVisible(True)

    def begin_clone(self) -> None:
        if not self._selected:
            return
        self._editing_key = None
        self._fill_editor(self._selected)
        self.key_edit.setText(new_frequency_resource_key())
        populate_source_combo(self.source_edit, self.store)
        self.editor_frame.setVisible(True)
        self.status_label.setText("Clone creates a new station-owned resource. The catalog source defaults to Station Resources.")

    def _fill_editor(self, resource: FrequencyResource | None) -> None:
        self.key_edit.setReadOnly(False)
        if resource is None:
            for field in (self.key_edit, self.label_edit, self.center_hz_edit, self.lower_hz_edit, self.upper_hz_edit, self.receive_hz_edit, self.transmit_hz_edit, self.band_edit, self.channel_edit, self.mode_edit, self.notes_edit):
                field.clear()
            populate_source_combo(self.source_edit, self.store)
            self.kind_edit.setCurrentText("simplex")
            self.editor_service.setCurrentText("AMATEUR")
            return
        self.key_edit.setText(resource.frequency_resource_key)
        self.key_edit.setReadOnly(self._editing_key is not None)
        populate_source_combo(self.source_edit, self.store, resource.source_key)
        self.label_edit.setText(resource.label)
        self.kind_edit.setCurrentText(resource.resource_kind)
        self.editor_service.setCurrentText(resource.service)
        self.center_hz_edit.setText(str(resource.center_hz or ""))
        self.lower_hz_edit.setText(str(resource.lower_hz or ""))
        self.upper_hz_edit.setText(str(resource.upper_hz or ""))
        self.receive_hz_edit.setText(str(resource.receive_hz or ""))
        self.transmit_hz_edit.setText(str(resource.transmit_hz or ""))
        self.band_edit.setText(resource.band or "")
        self.channel_edit.setText(resource.channel or "")
        self.mode_edit.setText(resource.mode or "")
        self.notes_edit.setText(resource.notes or "")

    def _editor_resource(self) -> FrequencyResource:
        existing = self._selected if self._editing_key else None
        return FrequencyResource(
            frequency_resource_key=self.key_edit.text(), source_key=str(self.source_edit.currentData() or ""), resource_kind=self.kind_edit.currentText(),
            service=self.editor_service.currentText(), label=self.label_edit.text(), center_hz=hz_from_text(self.center_hz_edit.text()),
            lower_hz=hz_from_text(self.lower_hz_edit.text()), upper_hz=hz_from_text(self.upper_hz_edit.text()),
            receive_hz=hz_from_text(self.receive_hz_edit.text()), transmit_hz=hz_from_text(self.transmit_hz_edit.text()),
            band=self.band_edit.text().strip() or None, channel=self.channel_edit.text().strip() or None,
            mode=self.mode_edit.text().strip() or None, notes=self.notes_edit.text().strip() or None,
            active=existing.active if existing else True, retired=existing.retired if existing else False,
            replacement_frequency_resource_key=existing.replacement_frequency_resource_key if existing else None,
        )

    def save_editor(self) -> None:
        try:
            if self.source_edit.currentData() == STATION_MANUAL_SOURCE_KEY:
                self.store.ensure_station_source()
            resource = self._editor_resource()
            saved = self.store.update_frequency(resource) if self._editing_key else self.store.create_frequency(resource)
        except (CatalogValidationError, ReadOnlyResourceError, ValueError) as exc:
            self.status_label.setText(f"Cannot save frequency resource: {exc}")
            return
        self.editor_frame.setVisible(False)
        self._selected = saved
        self.status_label.setText("Frequency resource saved.")
        self.refresh_results()

    def cancel_editor(self) -> None:
        self.editor_frame.setVisible(False)
        self._editing_key = None

    def retire_selected(self) -> None:
        if not self._selected:
            return
        try:
            retired = self.store.retire_frequency(self._selected.frequency_resource_key)
        except (CatalogValidationError, ReadOnlyResourceError) as exc:
            self.status_label.setText(f"Cannot retire frequency resource: {exc}")
            return
        self.status_label.setText(f"Retired {retired.label}; existing references remain intact.")
        self.refresh_results()

    def delete_selected(self) -> None:
        if not self._selected:
            return
        usage = self.store.frequency_usage(self._selected.frequency_resource_key)
        if usage.is_referenced:
            self.status_label.setText(f"Cannot delete: used by {usage.total_references} record(s). Retire it instead.")
            return
        answer = QMessageBox.question(self, "Delete frequency resource", f"Delete '{self._selected.label}'? This cannot be undone.")
        if answer != QMessageBox.Yes:
            return
        try:
            self.store.delete_frequency_if_unreferenced(self._selected.frequency_resource_key)
        except (ReferencedResourceError, ReadOnlyResourceError) as exc:
            self.status_label.setText(f"Cannot delete frequency resource: {exc}")
            return
        self._selected = None
        self.status_label.setText("Frequency resource deleted.")
        self.refresh_results()


__all__ = ["FrequencyCatalogView", "hz_from_text", "new_frequency_resource_key"]
